from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class VMStateRecord:
    vmid: int
    group_id: str
    vm_name: str
    state: str
    pending_since: int | None
    updated_at: int
    last_error: str | None


class StateRepository(Protocol):
    async def init(self) -> None: ...

    async def upsert_vm_state(
        self,
        *,
        vmid: int,
        group_id: str,
        vm_name: str,
        state: str,
        pending_since: int | None,
        last_error: str | None = None,
    ) -> None: ...

    async def get_vm_state(self, vmid: int) -> VMStateRecord | None: ...

    async def delete_vm_state(self, vmid: int) -> None: ...

    async def delete_missing_group_vm_states(self, group_id: str, keep_vmids: set[int]) -> int: ...

    async def count_group_vm_states(self, group_id: str, states: set[str]) -> int: ...

    async def get_desired_size(self, group_id: str) -> int | None: ...

    async def set_desired_size_if_missing(self, group_id: str, desired_size: int) -> None: ...

    async def set_desired_size(self, group_id: str, desired_size: int) -> None: ...


class ProxmoxService(Protocol):
    async def list_vms(self) -> list[dict[str, Any]]: ...

    async def vm_config(self, vmid: int) -> dict[str, Any]: ...

    async def nextid(self) -> int: ...

    async def iso_exists(self, iso_name: str) -> bool: ...

    async def upload(self, *, storage: str, filename: str, content: str, file_bytes: bytes) -> Any: ...

    async def create_vm_from_image(
        self,
        *,
        vmid: int,
        name: str,
        cores: int,
        memory_mb: int,
        balloon_mb: int,
        disk_size: str,
        tags: str,
        iso_name: str,
    ) -> int: ...

    async def stop_and_delete(self, vmid: int) -> None: ...


class KubernetesService(Protocol):
    async def list_nodes(self) -> list[dict[str, Any]]: ...

    async def delete_node(self, node_name: str) -> None: ...

    async def get_node(self, node_name: str) -> dict[str, Any]: ...

    async def build_template_node_bytes(self, payload: dict[str, Any]) -> bytes: ...
