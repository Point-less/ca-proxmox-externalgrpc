from __future__ import annotations

import asyncio
import logging
from typing import Any

from core.contracts import KubernetesService, ProxmoxService, StateRepository
from core.errors import FailedPreconditionError, GroupNotFoundError, InvalidArgumentError, NotFoundError
from core.models import GroupConfig, Settings, VMInfo
from .group_context import GroupContext, ManagedNode
from .reconcile_service import ReconcileService
from .scaling_service import ScalingService
from .template_service import TemplateService

LOG = logging.getLogger("proxmox-ca-externalgrpc")


class ProvisioningOrchestrator:
    def __init__(
        self,
        *,
        settings: Settings,
        proxmox: ProxmoxService,
        kube: KubernetesService,
        state: StateRepository,
        pending_vm_timeout_seconds: int = 900,
        reconcile_interval_seconds: int = 20,
    ):
        self.settings = settings
        self.context = GroupContext(settings=settings, proxmox=proxmox, state=state)
        self.scaling = ScalingService(settings=settings, context=self.context, state=state)
        self.reconcile = ReconcileService(
            settings=settings,
            context=self.context,
            proxmox=proxmox,
            kube=kube,
            state=state,
            scaling=self.scaling,
            pending_vm_timeout_seconds=pending_vm_timeout_seconds,
        )
        self.template = TemplateService(context=self.context, kube=kube)
        self.reconcile_interval_seconds = max(5, int(reconcile_interval_seconds))

        self._started = False
        self._start_lock = asyncio.Lock()
        self._group_locks = {group_id: asyncio.Lock() for group_id in settings.groups.keys()}
        self._tasks: list[asyncio.Task[Any]] = []

    async def start(self) -> None:
        async with self._start_lock:
            if self._started:
                return
            await self.context.state.init()
            for group in self.settings.groups.values():
                lock = self._group_locks[group.id]
                async with lock:
                    await self.reconcile.bootstrap_group(group)
            self._tasks.append(asyncio.create_task(self._reconcile_loop()))
            self._started = True

    async def stop(self) -> None:
        async with self._start_lock:
            if not self._started:
                return
            tasks = list(self._tasks)
            self._tasks.clear()
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self._started = False

    async def _with_group_lock(self, group_id: str, fn):
        lock = self._group_locks.get(group_id)
        if lock is None:
            raise GroupNotFoundError(f"unknown node group: {group_id}")
        async with lock:
            return await fn()

    async def _reconcile_loop(self) -> None:
        while True:
            try:
                for group in self.settings.groups.values():
                    lock = self._group_locks[group.id]
                    async with lock:
                        await self.reconcile.reconcile_group(group)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.exception("Background reconcile loop failed")
            await asyncio.sleep(self.reconcile_interval_seconds)

    async def node_group_for_node(self, node: ManagedNode) -> GroupConfig | None:
        return await self.scaling.node_group_for_node(node)

    async def node_group_target_size(self, group_id: str) -> int:
        return int(await self._with_group_lock(group_id, lambda: self.scaling.node_group_target_size(group_id)))

    async def node_group_increase_size(self, group_id: str, delta: int) -> None:
        await self._with_group_lock(group_id, lambda: self.scaling.node_group_increase_size(group_id, int(delta)))

    async def node_group_delete_nodes(self, group_id: str, nodes: list[ManagedNode]) -> None:
        await self._with_group_lock(group_id, lambda: self.scaling.node_group_delete_nodes(group_id, nodes))

    async def node_group_decrease_target_size(self, group_id: str, delta: int) -> None:
        await self._with_group_lock(group_id, lambda: self.scaling.node_group_decrease_target_size(group_id, int(delta)))

    async def node_group_nodes(self, group_id: str) -> list[VMInfo]:
        return await self._with_group_lock(group_id, lambda: self.scaling.node_group_nodes(group_id))

    async def node_group_template_node_bytes(self, group_id: str) -> bytes:
        return await self._with_group_lock(group_id, lambda: self.template.node_group_template_node_bytes(group_id))


__all__ = [
    "ProvisioningOrchestrator",
    "ManagedNode",
    "GroupNotFoundError",
    "InvalidArgumentError",
    "FailedPreconditionError",
    "NotFoundError",
]
