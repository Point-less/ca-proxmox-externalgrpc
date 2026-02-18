from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Awaitable, Callable, TypeVar

import grpc

from core.errors import FailedPreconditionError, GroupNotFoundError, InvalidArgumentError, NotFoundError
from core.models import GroupConfig, Settings
from infra.adapters import AsyncKubernetesAdapter, AsyncProxmoxAdapter
from infra.proto_stubs import pb, pb_grpc
from infra.pve import PveClient
from infra.state_store import StateStore
from services.orchestrator import (
    ManagedNode,
    ProvisioningOrchestrator,
)

LOG = logging.getLogger("proxmox-ca-externalgrpc")
_T = TypeVar("_T")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


class CloudProvider(pb_grpc.CloudProviderServicer):
    def __init__(self, settings: Settings):
        state_db_path = Path(os.getenv("PROVIDER_STATE_DB", "/tmp/proxmox-ca-externalgrpc-state.db"))
        pending_vm_timeout_seconds = max(120, _env_int("PENDING_VM_TIMEOUT_SECONDS", 900))
        reconcile_interval_seconds = max(5, _env_int("RECONCILE_INTERVAL_SECONDS", 20))

        self.settings = settings
        self.proxmox = AsyncProxmoxAdapter(PveClient(settings.proxmox))
        self.kube = AsyncKubernetesAdapter()
        self.orchestrator = ProvisioningOrchestrator(
            settings=settings,
            proxmox=self.proxmox,
            kube=self.kube,
            state=StateStore(state_db_path),
            pending_vm_timeout_seconds=pending_vm_timeout_seconds,
            reconcile_interval_seconds=reconcile_interval_seconds,
        )

    async def start(self) -> None:
        await self.orchestrator.start()

    async def stop(self) -> None:
        stop = getattr(self.orchestrator, "stop", None)
        if stop is not None:
            await stop()
        await self.kube.close()
        await self.proxmox.close()

    async def _group(self, group_id: str, context: grpc.ServicerContext) -> GroupConfig:
        group = self.settings.groups.get(group_id)
        if group is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"unknown node group: {group_id}")
        return group

    async def _map_error(self, context: grpc.ServicerContext, exc: Exception) -> None:
        if isinstance(exc, GroupNotFoundError):
            await context.abort(grpc.StatusCode.NOT_FOUND, str(exc))
        if isinstance(exc, InvalidArgumentError):
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        if isinstance(exc, FailedPreconditionError):
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(exc))
        if isinstance(exc, NotFoundError):
            await context.abort(grpc.StatusCode.NOT_FOUND, str(exc))
        await context.abort(grpc.StatusCode.UNAVAILABLE, str(exc))

    def _managed_node(self, node: pb.ExternalGrpcNode) -> ManagedNode:
        return ManagedNode(
            provider_id=str(node.providerID or ""),
            name=str(node.name or ""),
            labels={str(k): str(v) for k, v in dict(node.labels or {}).items()},
        )

    async def _call_or_abort(self, context: grpc.ServicerContext, fn: Callable[[], Awaitable[_T]]) -> _T:
        try:
            return await fn()
        except Exception as exc:
            await self._map_error(context, exc)
            raise AssertionError("unreachable")

    async def NodeGroups(self, request: pb.NodeGroupsRequest, context: grpc.ServicerContext) -> pb.NodeGroupsResponse:
        groups = [
            pb.NodeGroup(
                id=g.id,
                minSize=g.min_size,
                maxSize=g.max_size,
                debug=f"group={g.id} prefix={g.vm_name_prefix}",
            )
            for g in self.settings.groups.values()
        ]
        return pb.NodeGroupsResponse(nodeGroups=groups)

    async def NodeGroupForNode(
        self, request: pb.NodeGroupForNodeRequest, context: grpc.ServicerContext
    ) -> pb.NodeGroupForNodeResponse:
        group = await self._call_or_abort(
            context,
            lambda: self.orchestrator.node_group_for_node(self._managed_node(request.node)),
        )
        if group is None:
            return pb.NodeGroupForNodeResponse(nodeGroup=pb.NodeGroup(id=""))
        return pb.NodeGroupForNodeResponse(
            nodeGroup=pb.NodeGroup(id=group.id, minSize=group.min_size, maxSize=group.max_size)
        )

    async def GPULabel(self, request: pb.GPULabelRequest, context: grpc.ServicerContext) -> pb.GPULabelResponse:
        return pb.GPULabelResponse(label="")

    async def GetAvailableGPUTypes(
        self,
        request: pb.GetAvailableGPUTypesRequest,
        context: grpc.ServicerContext,
    ) -> pb.GetAvailableGPUTypesResponse:
        return pb.GetAvailableGPUTypesResponse(gpuTypes={})

    async def Cleanup(self, request: pb.CleanupRequest, context: grpc.ServicerContext) -> pb.CleanupResponse:
        return pb.CleanupResponse()

    async def Refresh(self, request: pb.RefreshRequest, context: grpc.ServicerContext) -> pb.RefreshResponse:
        return pb.RefreshResponse()

    async def NodeGroupTargetSize(
        self,
        request: pb.NodeGroupTargetSizeRequest,
        context: grpc.ServicerContext,
    ) -> pb.NodeGroupTargetSizeResponse:
        target_size = int(await self._call_or_abort(context, lambda: self.orchestrator.node_group_target_size(request.id)))
        return pb.NodeGroupTargetSizeResponse(targetSize=target_size)

    async def NodeGroupIncreaseSize(
        self,
        request: pb.NodeGroupIncreaseSizeRequest,
        context: grpc.ServicerContext,
    ) -> pb.NodeGroupIncreaseSizeResponse:
        await self._call_or_abort(context, lambda: self.orchestrator.node_group_increase_size(request.id, int(request.delta)))
        return pb.NodeGroupIncreaseSizeResponse()

    async def NodeGroupDeleteNodes(
        self,
        request: pb.NodeGroupDeleteNodesRequest,
        context: grpc.ServicerContext,
    ) -> pb.NodeGroupDeleteNodesResponse:
        nodes = [self._managed_node(node) for node in request.nodes]
        await self._call_or_abort(context, lambda: self.orchestrator.node_group_delete_nodes(request.id, nodes))
        return pb.NodeGroupDeleteNodesResponse()

    async def NodeGroupDecreaseTargetSize(
        self,
        request: pb.NodeGroupDecreaseTargetSizeRequest,
        context: grpc.ServicerContext,
    ) -> pb.NodeGroupDecreaseTargetSizeResponse:
        await self._call_or_abort(
            context,
            lambda: self.orchestrator.node_group_decrease_target_size(request.id, int(request.delta)),
        )
        return pb.NodeGroupDecreaseTargetSizeResponse()

    async def NodeGroupNodes(
        self, request: pb.NodeGroupNodesRequest, context: grpc.ServicerContext
    ) -> pb.NodeGroupNodesResponse:
        vms = await self._call_or_abort(context, lambda: self.orchestrator.node_group_nodes(request.id))
        instances: list[pb.Instance] = []
        for vm in vms:
            state = pb.InstanceStatus.instanceRunning if vm.status == "running" else pb.InstanceStatus.unspecified
            instances.append(pb.Instance(id=f"k3s://{vm.name}", status=pb.InstanceStatus(instanceState=state)))
        return pb.NodeGroupNodesResponse(instances=instances)

    async def NodeGroupTemplateNodeInfo(
        self,
        request: pb.NodeGroupTemplateNodeInfoRequest,
        context: grpc.ServicerContext,
    ) -> pb.NodeGroupTemplateNodeInfoResponse:
        node_bytes = await self._call_or_abort(context, lambda: self.orchestrator.node_group_template_node_bytes(request.id))
        return pb.NodeGroupTemplateNodeInfoResponse(nodeBytes=node_bytes)

    async def NodeGroupGetOptions(
        self,
        request: pb.NodeGroupAutoscalingOptionsRequest,
        context: grpc.ServicerContext,
    ) -> pb.NodeGroupAutoscalingOptionsResponse:
        _ = await self._group(request.id, context)
        return pb.NodeGroupAutoscalingOptionsResponse(nodeGroupAutoscalingOptions=request.defaults)
