from __future__ import annotations

import time
from dataclasses import dataclass

from core.contracts import ProxmoxService, StateRepository
from core.errors import GroupNotFoundError
from core.models import GroupConfig, Settings, VMInfo
from core.vm_state_machine import (
    STATE_ACTIVE,
    STATE_PENDING,
    is_lifecycle_state,
)
from infra.utils import parse_tags, vmid_from_provider_id


@dataclass(frozen=True)
class ManagedNode:
    provider_id: str
    name: str
    labels: dict[str, str]


class GroupContext:
    def __init__(self, *, settings: Settings, proxmox: ProxmoxService, state: StateRepository):
        self.settings = settings
        self.proxmox = proxmox
        self.state = state

    def group(self, group_id: str) -> GroupConfig:
        group = self.settings.groups.get(group_id)
        if group is None:
            raise GroupNotFoundError(f"unknown node group: {group_id}")
        return group

    def group_tag(self, group: GroupConfig) -> str:
        return f"ca-group-{group.id}"

    async def group_vms(self, group: GroupConfig) -> list[VMInfo]:
        out: list[VMInfo] = []
        want = self.group_tag(group)
        for vm in await self.proxmox.list_vms():
            try:
                vmid = int(vm.get("vmid"))
            except Exception:
                continue
            name = str(vm.get("name") or "")
            status = str(vm.get("status") or "")
            tags = parse_tags(vm.get("tags"))
            if not tags:
                try:
                    tags = parse_tags((await self.proxmox.vm_config(vmid)).get("tags"))
                except Exception:
                    tags = []
            if want in tags:
                out.append(VMInfo(vmid=vmid, name=name, status=status, tags=tags))
        return sorted(out, key=lambda x: x.vmid)

    async def ensure_vm_state(self, group: GroupConfig, vm: VMInfo) -> str:
        record = await self.state.get_vm_state(vm.vmid)
        if record is not None and record.group_id == group.id and is_lifecycle_state(record.state):
            return record.state

        state = STATE_ACTIVE if vm.status == "running" else STATE_PENDING
        pending_since = None if state == STATE_ACTIVE else int(time.time())

        await self.state.upsert_vm_state(
            vmid=vm.vmid,
            group_id=group.id,
            vm_name=vm.name,
            state=state,
            pending_since=pending_since,
        )
        return state

    async def set_vm_state(
        self,
        group: GroupConfig,
        vm: VMInfo,
        *,
        state: str,
        pending_since: int | None = None,
        last_error: str | None = None,
        cleanup_storage: str | None = None,
        cleanup_volume: str | None = None,
    ) -> None:
        await self.state.upsert_vm_state(
            vmid=vm.vmid,
            group_id=group.id,
            vm_name=vm.name,
            state=state,
            pending_since=pending_since,
            last_error=last_error,
            cleanup_storage=cleanup_storage,
            cleanup_volume=cleanup_volume,
        )

    async def vm_pending_since(self, vmid: int) -> int | None:
        record = await self.state.get_vm_state(vmid)
        return record.pending_since if record is not None else None

    async def active_group_vms(self, group: GroupConfig) -> list[VMInfo]:
        out: list[VMInfo] = []
        for vm in await self.group_vms(group):
            if vm.status != "running":
                continue
            if await self.ensure_vm_state(group, vm) != STATE_ACTIVE:
                continue
            out.append(vm)
        return out

    async def managed_group_vms(self, group: GroupConfig) -> list[tuple[VMInfo, str]]:
        out: list[tuple[VMInfo, str]] = []
        for vm in await self.group_vms(group):
            state = await self.ensure_vm_state(group, vm)
            if state in {STATE_ACTIVE, STATE_PENDING}:
                out.append((vm, state))
        return out

    async def find_vm_for_node(self, group: GroupConfig, node: ManagedNode) -> VMInfo | None:
        vms = await self.group_vms(group)
        by_vmid = {v.vmid: v for v in vms}
        vmid = vmid_from_provider_id(node.provider_id)
        if vmid is not None and vmid in by_vmid:
            return by_vmid[vmid]
        node_name = (node.name or "").strip()
        if node_name:
            for vm in vms:
                if vm.name == node_name:
                    return vm
        return None
