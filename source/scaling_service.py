from __future__ import annotations

import logging
from typing import Iterable

from .contracts import KubernetesService, StateRepository
from .errors import FailedPreconditionError, InvalidArgumentError, NotFoundError
from .group_context import GroupContext, ManagedNode, STATE_PENDING
from .models import GroupConfig, Settings, VMInfo

LOG = logging.getLogger("proxmox-ca-externalgrpc")


class ScalingService:
    def __init__(
        self,
        *,
        settings: Settings,
        context: GroupContext,
        kube: KubernetesService,
        state: StateRepository,
    ):
        self.settings = settings
        self.context = context
        self.kube = kube
        self.state = state

    async def ensure_desired_size_initialized(self, group: GroupConfig, observed_size: int | None = None) -> int:
        if observed_size is None:
            observed_size = len(await self.context.managed_group_vms(group))
        baseline = max(group.min_size, observed_size)
        await self.state.set_desired_size_if_missing(group.id, baseline)
        desired = await self.state.get_desired_size(group.id)
        if desired is None:
            await self.state.set_desired_size(group.id, baseline)
            return baseline
        return max(group.min_size, min(group.max_size, int(desired)))

    async def node_group_for_node(self, node: ManagedNode) -> GroupConfig | None:
        label_group = (node.labels or {}).get("autoscaler.proxmox/group", "").strip()
        if label_group in self.settings.groups:
            return self.settings.groups[label_group]
        for group in self.settings.groups.values():
            vm = await self.context.find_vm_for_node(group, node)
            if vm is not None:
                return group
        return None

    async def node_group_target_size(self, group_id: str) -> int:
        group = self.context.group(group_id)
        return await self.ensure_desired_size_initialized(group)

    async def node_group_increase_size(self, group_id: str, delta: int) -> None:
        group = self.context.group(group_id)
        if delta <= 0:
            raise InvalidArgumentError("delta must be > 0")
        desired = await self.ensure_desired_size_initialized(group)
        new_desired = desired + int(delta)
        if new_desired > group.max_size:
            raise FailedPreconditionError(
                f"scale would exceed max size for {group.id}: current={desired} delta={delta} max={group.max_size}"
            )
        await self.state.set_desired_size(group.id, new_desired)

    async def node_group_decrease_target_size(self, group_id: str, delta: int) -> None:
        group = self.context.group(group_id)
        if delta >= 0:
            raise InvalidArgumentError("delta must be < 0")
        desired = await self.ensure_desired_size_initialized(group)
        new_desired = desired + int(delta)
        if new_desired < group.min_size:
            raise FailedPreconditionError(
                f"scale would exceed min size for {group.id}: current={desired} delta={delta} min={group.min_size}"
            )
        await self.state.set_desired_size(group.id, new_desired)

    async def node_group_delete_nodes(self, group_id: str, nodes: Iterable[ManagedNode]) -> None:
        group = self.context.group(group_id)
        entries = list(nodes)
        if not entries:
            return
        deleted = 0
        for node in entries:
            vm = await self.context.find_vm_for_node(group, node)
            if vm is None:
                raise NotFoundError(f"node not in group {group.id}: {node.name} {node.provider_id}")
            await self._delete_vm(vm)
            deleted += 1
        desired = await self.ensure_desired_size_initialized(group)
        await self.state.set_desired_size(group.id, max(group.min_size, desired - deleted))

    async def node_group_nodes(self, group_id: str) -> list[VMInfo]:
        group = self.context.group(group_id)
        return await self.context.active_group_vms(group)

    async def shrink_to_desired(self, group: GroupConfig, candidates: list[tuple[VMInfo, str]], desired: int) -> None:
        if len(candidates) <= desired:
            return
        remove_count = len(candidates) - desired
        ordered = sorted(candidates, key=lambda item: (0 if item[1] == STATE_PENDING else 1, -item[0].vmid))
        for vm, _state in ordered[:remove_count]:
            await self._delete_vm(vm)

    async def _delete_vm(self, vm: VMInfo) -> None:
        await self.context.proxmox.stop_and_delete(vm.vmid)
        await self.state.delete_vm_state(vm.vmid)
        await self.kube.delete_node(vm.name)
        LOG.info("Deleted VM vmid=%s name=%s", vm.vmid, vm.name)
