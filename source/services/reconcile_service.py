from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

import httpx

from core.contracts import KubernetesService, ProxmoxService, StateRepository, VMStateRecord
from core.models import GroupConfig, Settings, VMInfo
from core.vm_state_machine import (
    EVENT_BECAME_ACTIVE,
    EVENT_BECAME_PENDING,
    EVENT_INFRA_MISSING,
    EVENT_ISO_DONE,
    EVENT_ISO_RETRY,
    EVENT_NODE_DONE,
    EVENT_NODE_RETRY,
    EVENT_VM_DONE,
    EVENT_VM_RETRY,
    STATE_ACTIVE,
    STATE_COMPLETED,
    STATE_DELETING_ISO,
    STATE_DELETING_NODE,
    STATE_DELETING_VM,
    STATE_FAILED,
    STATE_PENDING,
    is_delete_state,
    is_lifecycle_state,
    transition_state,
)
from infra.seed import make_cidata_iso_bytes, render_seed, seed_iso_name
from infra.utils import parse_tags
from .group_context import GroupContext
from .scaling_service import ScalingService

LOG = logging.getLogger("proxmox-ca-externalgrpc")


@dataclass(frozen=True)
class DeleteStepOutcome:
    event: str
    last_error: str | None
    cleanup_storage: str | None = None
    cleanup_volume: str | None = None


class ReconcileService:
    def __init__(
        self,
        *,
        settings: Settings,
        context: GroupContext,
        proxmox: ProxmoxService,
        kube: KubernetesService,
        state: StateRepository,
        scaling: ScalingService,
        pending_vm_timeout_seconds: int,
    ):
        self.settings = settings
        self.context = context
        self.proxmox = proxmox
        self.kube = kube
        self.state = state
        self.scaling = scaling
        self.pending_vm_timeout_seconds = max(120, int(pending_vm_timeout_seconds))

    async def bootstrap_group(self, group: GroupConfig) -> None:
        group_vms = await self.context.group_vms(group)
        await self._reconcile_missing_vm_records(group, {vm.vmid for vm in group_vms})

        managed_count = 0
        for vm in group_vms:
            state = await self.context.ensure_vm_state(group, vm)
            if state in {STATE_ACTIVE, STATE_PENDING}:
                managed_count += 1
        await self.scaling.ensure_desired_size_initialized(group, observed_size=managed_count)

    async def reconcile_group(self, group: GroupConfig) -> None:
        now = int(time.time())
        try:
            kube_nodes = await self.kube.list_nodes()
        except Exception as exc:
            LOG.warning("Failed listing Kubernetes nodes for state reconcile group=%s: %s", group.id, exc)
            kube_nodes = []

        group_vms = await self.context.group_vms(group)
        vm_by_id = {vm.vmid: vm for vm in group_vms}

        await self._reconcile_missing_vm_records(group, set(vm_by_id.keys()))

        # Progress explicit delete state machine first.
        for record in await self.state.list_group_vm_states(group.id):
            if not is_delete_state(record.state):
                continue
            await self._progress_delete_state(group, record, vm_by_id.get(record.vmid))

        managed: list[tuple[VMInfo, str]] = []
        for vm in group_vms:
            state = await self.context.ensure_vm_state(group, vm)
            if is_delete_state(state):
                continue

            if state == STATE_FAILED:
                await self.scaling.request_vm_deletion(group, vm)
                continue

            if state == STATE_ACTIVE and vm.status != "running":
                state = transition_state(state, EVENT_BECAME_PENDING)
                await self.context.set_vm_state(group, vm, state=state, pending_since=now, last_error="vm not running")

            if state == STATE_PENDING:
                pending_since = await self.context.vm_pending_since(vm.vmid)
                if pending_since is None:
                    pending_since = now
                    await self.context.set_vm_state(group, vm, state=STATE_PENDING, pending_since=pending_since)

                age_s = max(0, now - int(pending_since))
                if vm.status == "running" and self._is_kube_node_ready_for_vm(group, vm, kube_nodes):
                    state = transition_state(state, EVENT_BECAME_ACTIVE)
                    await self.context.set_vm_state(group, vm, state=state, pending_since=None, last_error=None)
                    LOG.info("Promoted VM to active vmid=%s name=%s group=%s", vm.vmid, vm.name, group.id)
                elif age_s >= self.pending_vm_timeout_seconds:
                    LOG.warning(
                        "Pending VM exceeded timeout vmid=%s name=%s group=%s age=%ss timeout=%ss; deleting",
                        vm.vmid,
                        vm.name,
                        group.id,
                        age_s,
                        self.pending_vm_timeout_seconds,
                    )
                    await self.scaling.request_vm_deletion(group, vm)
                    continue

            if state in {STATE_ACTIVE, STATE_PENDING}:
                managed.append((vm, state))

        await self._prune_stale_kube_nodes_for_group(group)

        desired = await self.scaling.ensure_desired_size_initialized(group, observed_size=len(managed))
        if desired < group.min_size:
            desired = group.min_size
            await self.state.set_desired_size(group.id, desired)
        if desired > group.max_size:
            desired = group.max_size
            await self.state.set_desired_size(group.id, desired)

        if len(managed) < desired:
            for _ in range(desired - len(managed)):
                vm = await self._create_vm(group)
                await self.context.set_vm_state(group, vm, state=STATE_PENDING, pending_since=int(time.time()))
        elif len(managed) > desired:
            await self.scaling.shrink_to_desired(group, managed, desired)

    async def _progress_delete_state(self, group: GroupConfig, record: VMStateRecord, vm: VMInfo | None) -> None:
        outcome = await self._run_delete_step(record, vm)
        next_state = transition_state(record.state, outcome.event)
        if next_state == STATE_COMPLETED:
            await self.state.delete_vm_state(record.vmid)
            return
        await self._persist_delete_state(
            group,
            record,
            state=next_state,
            last_error=outcome.last_error,
            cleanup_storage=outcome.cleanup_storage,
            cleanup_volume=outcome.cleanup_volume,
        )

    async def _run_delete_step(self, record: VMStateRecord, vm: VMInfo | None) -> DeleteStepOutcome:
        if record.state == STATE_DELETING_VM:
            return await self._step_delete_vm(record, vm)
        if record.state == STATE_DELETING_ISO:
            return await self._step_delete_iso(record)
        if record.state == STATE_DELETING_NODE:
            return await self._step_delete_node(record)
        return DeleteStepOutcome(event=EVENT_NODE_RETRY, last_error=f"unknown delete state: {record.state}")

    async def _step_delete_vm(self, record: VMStateRecord, vm: VMInfo | None) -> DeleteStepOutcome:
        cleanup_storage = record.cleanup_storage
        cleanup_volume = record.cleanup_volume
        if (not cleanup_storage or not cleanup_volume) and vm is not None:
            try:
                seed_ref = await self.proxmox.attached_seed_iso(record.vmid)
                if seed_ref is not None:
                    cleanup_storage, cleanup_volume = seed_ref
            except Exception as exc:
                LOG.warning("Failed reading attached seed ISO during delete vmid=%s: %s", record.vmid, exc)

        if vm is not None:
            try:
                await self.proxmox.stop_and_delete_vm(record.vmid)
            except Exception as exc:
                return DeleteStepOutcome(
                    event=EVENT_VM_RETRY,
                    last_error=f"delete vm failed: {exc}",
                    cleanup_storage=cleanup_storage,
                    cleanup_volume=cleanup_volume,
                )

        return DeleteStepOutcome(
            event=EVENT_VM_DONE,
            last_error=None,
            cleanup_storage=cleanup_storage,
            cleanup_volume=cleanup_volume,
        )

    async def _step_delete_iso(self, record: VMStateRecord) -> DeleteStepOutcome:
        storage = record.cleanup_storage
        volume = record.cleanup_volume
        if storage and volume:
            try:
                await self.proxmox.delete_storage_volume(storage, volume)
            except httpx.HTTPStatusError as exc:
                code = exc.response.status_code
                if code != 404:
                    return DeleteStepOutcome(
                        event=EVENT_ISO_RETRY,
                        last_error=f"delete iso failed: {exc}",
                        cleanup_storage=storage,
                        cleanup_volume=volume,
                    )
            except Exception as exc:
                return DeleteStepOutcome(
                    event=EVENT_ISO_RETRY,
                    last_error=f"delete iso failed: {exc}",
                    cleanup_storage=storage,
                    cleanup_volume=volume,
                )

        return DeleteStepOutcome(
            event=EVENT_ISO_DONE,
            last_error=None,
            cleanup_storage=storage,
            cleanup_volume=volume,
        )

    async def _step_delete_node(self, record: VMStateRecord) -> DeleteStepOutcome:
        try:
            await self.kube.delete_node(record.vm_name)
        except Exception as exc:
            return DeleteStepOutcome(
                event=EVENT_NODE_RETRY,
                last_error=f"delete node failed: {exc}",
                cleanup_storage=record.cleanup_storage,
                cleanup_volume=record.cleanup_volume,
            )
        return DeleteStepOutcome(
            event=EVENT_NODE_DONE,
            last_error=None,
            cleanup_storage=record.cleanup_storage,
            cleanup_volume=record.cleanup_volume,
        )

    async def _persist_delete_state(
        self,
        group: GroupConfig,
        record: VMStateRecord,
        *,
        state: str,
        last_error: str | None,
        cleanup_storage: str | None = None,
        cleanup_volume: str | None = None,
    ) -> None:
        await self.state.upsert_vm_state(
            vmid=record.vmid,
            group_id=group.id,
            vm_name=record.vm_name,
            state=state,
            pending_since=None,
            last_error=last_error,
            cleanup_storage=record.cleanup_storage if cleanup_storage is None else cleanup_storage,
            cleanup_volume=record.cleanup_volume if cleanup_volume is None else cleanup_volume,
        )

    async def _reconcile_missing_vm_records(self, group: GroupConfig, existing_vmids: set[int]) -> None:
        for record in await self.state.list_group_vm_states(group.id):
            if record.vmid in existing_vmids:
                continue
            if not is_lifecycle_state(record.state):
                await self.state.delete_vm_state(record.vmid)
                continue
            next_state = transition_state(record.state, EVENT_INFRA_MISSING)
            if next_state == STATE_COMPLETED:
                await self.state.delete_vm_state(record.vmid)
                continue
            await self._persist_delete_state(group, record, state=next_state, last_error=None)

    async def _create_vm(self, group: GroupConfig) -> VMInfo:
        vmid = await self.proxmox.nextid()
        vm_name = f"{group.vm_name_prefix}-{vmid}"
        node_labels = list(group.labels)
        node_labels.append(f"autoscaler.proxmox/group={group.id}")
        node_labels.append(f"autoscaler.proxmox/vmid={vmid}")
        meta, user = render_seed(
            k3s=self.settings.k3s,
            hostname=vm_name,
            node_labels=node_labels,
            node_taints=group.taints,
        )
        iso_name = seed_iso_name(vm_name=vm_name, meta=meta, user=user)
        if not await self.proxmox.iso_exists(iso_name):
            iso_bytes = make_cidata_iso_bytes(meta_data=meta, user_data=user)
            await self.proxmox.upload(
                storage=self.settings.proxmox.iso_storage,
                filename=iso_name,
                content="iso",
                file_bytes=iso_bytes,
            )

        tags = ";".join([self.settings.vm_tag_prefix, "ca-managed", self.context.group_tag(group)])
        vmid = await self.proxmox.create_vm_from_image(
            vmid=vmid,
            name=vm_name,
            cores=group.cores,
            memory_mb=group.memory_mb,
            balloon_mb=group.balloon_mb,
            disk_size=group.disk_size,
            tags=tags,
            iso_name=iso_name,
        )
        LOG.info("Created VM vmid=%s name=%s group=%s", vmid, vm_name, group.id)
        return VMInfo(vmid=vmid, name=vm_name, status="running", tags=parse_tags(tags))

    def _is_node_ready(self, item: dict[str, Any]) -> bool:
        status = item.get("status") if isinstance(item, dict) else {}
        conditions = status.get("conditions") if isinstance(status, dict) else []
        if not isinstance(conditions, list):
            return False
        for condition in conditions:
            if not isinstance(condition, dict):
                continue
            if str(condition.get("type") or "").strip() != "Ready":
                continue
            if str(condition.get("status") or "").strip().lower() == "true":
                return True
        return False

    def _is_kube_node_ready_for_vm(self, group: GroupConfig, vm: VMInfo, kube_nodes: list[dict[str, Any]]) -> bool:
        vmid = str(vm.vmid)
        for item in kube_nodes:
            meta = item.get("metadata") if isinstance(item, dict) else {}
            labels = meta.get("labels") if isinstance(meta, dict) else {}
            name = str(meta.get("name") or "").strip() if isinstance(meta, dict) else ""
            if not isinstance(labels, dict):
                labels = {}
            same_group = str(labels.get("autoscaler.proxmox/group") or "").strip() == group.id
            same_vmid = str(labels.get("autoscaler.proxmox/vmid") or "").strip() == vmid
            same_name = name == vm.name
            if not same_group:
                continue
            if not (same_vmid or same_name):
                continue
            if self._is_node_ready(item):
                return True
        return False

    async def _prune_stale_kube_nodes_for_group(self, group: GroupConfig) -> None:
        try:
            nodes = await self.kube.list_nodes()
        except Exception as exc:
            LOG.warning("Failed listing Kubernetes nodes for stale-node prune group=%s: %s", group.id, exc)
            return

        vm_names = {vm.name for vm in await self.context.active_group_vms(group)}
        for item in nodes:
            meta = item.get("metadata") or {}
            labels = meta.get("labels") or {}
            if labels.get("autoscaler.proxmox/group", "").strip() != group.id:
                continue
            node_name = str(meta.get("name") or "").strip()
            if not node_name:
                continue
            if node_name in vm_names:
                continue
            try:
                await self.kube.delete_node(node_name)
            except Exception as exc:
                LOG.warning("Failed deleting stale Kubernetes node name=%s group=%s error=%s", node_name, group.id, exc)
