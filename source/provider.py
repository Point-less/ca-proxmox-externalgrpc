from __future__ import annotations

import logging
import os
from pathlib import Path

import grpc
import requests

from .adapters import AsyncKubernetesAdapter, AsyncProxmoxAdapter
from .models import GroupConfig, Settings
from .orchestrator import (
    FailedPreconditionError,
    GroupNotFoundError,
    InvalidArgumentError,
    ManagedNode,
    NotFoundError,
    ProvisioningOrchestrator,
)
from .proto_stubs import pb, pb_grpc
from .pve import PveClient
from .state_store import StateStore

LOG = logging.getLogger("proxmox-ca-externalgrpc")


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
        self.orchestrator = ProvisioningOrchestrator(
            settings=settings,
            proxmox=AsyncProxmoxAdapter(PveClient(settings.proxmox)),
            kube=AsyncKubernetesAdapter(requests.Session()),
            state=StateStore(state_db_path),
            pending_vm_timeout_seconds=pending_vm_timeout_seconds,
            reconcile_interval_seconds=reconcile_interval_seconds,
        )

    async def start(self) -> None:
        await self.orchestrator.start()

    async def stop(self) -> None:
        stop = getattr(self.orchestrator, "stop", None)
        if stop is None:
            return
        await stop()

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
        node = ManagedNode(
            provider_id=str(request.node.providerID or ""),
            name=str(request.node.name or ""),
            labels={str(k): str(v) for k, v in dict(request.node.labels or {}).items()},
        )
        try:
            group = await self.orchestrator.node_group_for_node(node)
        except Exception as exc:
            await self._map_error(context, exc)
            raise AssertionError("unreachable")
        if group is None:
            return pb.NodeGroupForNodeResponse(nodeGroup=pb.NodeGroup(id=""))
        return pb.NodeGroupForNodeResponse(nodeGroup=pb.NodeGroup(id=group.id, minSize=group.min_size, maxSize=group.max_size))

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
        try:
            target_size = int(await self.orchestrator.node_group_target_size(request.id))
        except Exception as exc:
            await self._map_error(context, exc)
            raise AssertionError("unreachable")
        return pb.NodeGroupTargetSizeResponse(targetSize=target_size)

    async def NodeGroupIncreaseSize(
        self,
        request: pb.NodeGroupIncreaseSizeRequest,
        context: grpc.ServicerContext,
    ) -> pb.NodeGroupIncreaseSizeResponse:
        try:
            await self.orchestrator.node_group_increase_size(request.id, int(request.delta))
        except Exception as exc:
            await self._map_error(context, exc)
            raise AssertionError("unreachable")
        return pb.NodeGroupIncreaseSizeResponse()

    async def NodeGroupDeleteNodes(
        self,
        request: pb.NodeGroupDeleteNodesRequest,
        context: grpc.ServicerContext,
    ) -> pb.NodeGroupDeleteNodesResponse:
        nodes = [
            ManagedNode(
                provider_id=str(node.providerID or ""),
                name=str(node.name or ""),
                labels={str(k): str(v) for k, v in dict(node.labels or {}).items()},
            )
            for node in request.nodes
        ]
        try:
            await self.orchestrator.node_group_delete_nodes(request.id, nodes)
        except Exception as exc:
            await self._map_error(context, exc)
            raise AssertionError("unreachable")
        return pb.NodeGroupDeleteNodesResponse()

    async def NodeGroupDecreaseTargetSize(
        self,
        request: pb.NodeGroupDecreaseTargetSizeRequest,
        context: grpc.ServicerContext,
    ) -> pb.NodeGroupDecreaseTargetSizeResponse:
        try:
            await self.orchestrator.node_group_decrease_target_size(request.id, int(request.delta))
        except Exception as exc:
            await self._map_error(context, exc)
            raise AssertionError("unreachable")
        return pb.NodeGroupDecreaseTargetSizeResponse()

    async def NodeGroupNodes(
        self, request: pb.NodeGroupNodesRequest, context: grpc.ServicerContext
    ) -> pb.NodeGroupNodesResponse:
        try:
            vms = await self.orchestrator.node_group_nodes(request.id)
        except Exception as exc:
            await self._map_error(context, exc)
            raise AssertionError("unreachable")
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
        try:
            node_bytes = await self.orchestrator.node_group_template_node_bytes(request.id)
        except Exception as exc:
            await self._map_error(context, exc)
            raise AssertionError("unreachable")
        return pb.NodeGroupTemplateNodeInfoResponse(nodeBytes=node_bytes)

    async def NodeGroupGetOptions(
        self,
        request: pb.NodeGroupAutoscalingOptionsRequest,
        context: grpc.ServicerContext,
    ) -> pb.NodeGroupAutoscalingOptionsResponse:
        _ = await self._group(request.id, context)
        return pb.NodeGroupAutoscalingOptionsResponse(nodeGroupAutoscalingOptions=request.defaults)
