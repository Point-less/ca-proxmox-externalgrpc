from __future__ import annotations

import logging
import time
from typing import Any

from .contracts import KubernetesService, ProxmoxService, StateRepository
from .group_context import GroupContext, STATE_ACTIVE, STATE_FAILED, STATE_PENDING
from .models import GroupConfig, Settings, VMInfo
from .scaling_service import ScalingService
from .seed import make_cidata_iso_bytes, render_seed, seed_iso_name
from .utils import parse_tags

LOG = logging.getLogger("proxmox-ca-externalgrpc")


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
        vms = await self.context.group_vms(group)
        keep_vmids = {vm.vmid for vm in vms}
        await self.state.delete_missing_group_vm_states(group.id, keep_vmids)
        managed_count = 0
        for vm in vms:
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
        keep_vmids = {vm.vmid for vm in group_vms}
        await self.state.delete_missing_group_vm_states(group.id, keep_vmids)

        managed: list[tuple[VMInfo, str]] = []
        for vm in group_vms:
            state = await self.context.ensure_vm_state(group, vm)
            if state == STATE_FAILED:
                await self._cleanup_failed_vm(group, vm)
                continue

            if state == STATE_ACTIVE and vm.status != "running":
                await self.context.set_vm_state(group, vm, state=STATE_PENDING, pending_since=now, last_error="vm not running")
                state = STATE_PENDING

            if state == STATE_PENDING:
                pending_since = await self.context.vm_pending_since(vm.vmid)
                if pending_since is None:
                    pending_since = now
                    await self.context.set_vm_state(group, vm, state=STATE_PENDING, pending_since=pending_since)

                age_s = max(0, now - int(pending_since))
                if vm.status == "running" and self._is_kube_node_ready_for_vm(group, vm, kube_nodes):
                    await self.context.set_vm_state(group, vm, state=STATE_ACTIVE, pending_since=None, last_error=None)
                    state = STATE_ACTIVE
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
                    await self._delete_vm(vm)
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

    async def _cleanup_failed_vm(self, group: GroupConfig, vm: VMInfo) -> None:
        try:
            await self._delete_vm(vm)
            LOG.warning("Cleaned failed VM vmid=%s name=%s group=%s", vm.vmid, vm.name, group.id)
        except Exception:
            LOG.exception("Failed deleting failed-state VM vmid=%s group=%s", vm.vmid, group.id)

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

    async def _delete_vm(self, vm: VMInfo) -> None:
        await self.proxmox.stop_and_delete(vm.vmid)
        await self.state.delete_vm_state(vm.vmid)
        await self.kube.delete_node(vm.name)

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
