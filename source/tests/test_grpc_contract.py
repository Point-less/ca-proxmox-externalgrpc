from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import grpc

BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BASE_DIR))
subprocess.run([sys.executable, str(BASE_DIR / "source" / "scripts" / "generate-proto.py")], check=True)

from source.models import GroupConfig, K3sConfig, ProxmoxConfig, Settings, VMInfo  # noqa: E402
from source.orchestrator import FailedPreconditionError, GroupNotFoundError, InvalidArgumentError, NotFoundError  # noqa: E402
from source.provider import CloudProvider  # noqa: E402
from source.proto_stubs import pb, pb_grpc  # noqa: E402


def _settings(groups: dict[str, GroupConfig]) -> Settings:
    return Settings(
        proxmox=ProxmoxConfig(
            api_url="https://pm.example.invalid",
            node="pve",
            token_id="tokenid",
            token_secret="tokensecret",
            tls_insecure=True,
            import_storage="local",
            iso_storage="local",
            vm_storage="local-lvm",
            bridge="vmbr0",
            cloud_image_url="https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img",
            verify_certificates=False,
        ),
        k3s=K3sConfig(
            version="v1.34.4+k3s1",
            server_url="https://10.0.0.1:6443",
            cluster_token="token",
            ssh_public_key="ssh-ed25519 AAAA",
            registries_yaml="",
        ),
        vm_tag_prefix="proxmox_test2",
        groups=groups,
    )


def _group(group_id: str) -> GroupConfig:
    return GroupConfig(
        id=group_id,
        vm_name_prefix=f"ca-{group_id}",
        min_size=0,
        max_size=5,
        cores=2,
        memory_mb=4096,
        balloon_mb=2048,
        disk_size="20G",
        labels=[],
        taints=[],
    )


class _FakeOrchestrator:
    last_instance = None

    def __init__(self, **kwargs):
        self.settings = kwargs["settings"]
        self.behavior = {}
        _FakeOrchestrator.last_instance = self

    async def start(self):
        return None

    async def node_group_for_node(self, node):
        value = self.behavior.get("node_group_for_node", None)
        if isinstance(value, Exception):
            raise value
        return value

    async def node_group_target_size(self, group_id):
        value = self.behavior.get("node_group_target_size", 0)
        if isinstance(value, Exception):
            raise value
        return value

    async def node_group_increase_size(self, group_id, delta):
        value = self.behavior.get("node_group_increase_size", None)
        if isinstance(value, Exception):
            raise value
        return None

    async def node_group_delete_nodes(self, group_id, nodes):
        value = self.behavior.get("node_group_delete_nodes", None)
        if isinstance(value, Exception):
            raise value
        return None

    async def node_group_decrease_target_size(self, group_id, delta):
        value = self.behavior.get("node_group_decrease_target_size", None)
        if isinstance(value, Exception):
            raise value
        return None

    async def node_group_nodes(self, group_id):
        value = self.behavior.get("node_group_nodes", [])
        if isinstance(value, Exception):
            raise value
        return value

    async def node_group_template_node_bytes(self, group_id):
        value = self.behavior.get("node_group_template_node_bytes", b"")
        if isinstance(value, Exception):
            raise value
        return value


class GrpcContractTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.groups = {"general": _group("general")}
        self.settings = _settings(self.groups)
        self.patcher = patch("source.provider.ProvisioningOrchestrator", _FakeOrchestrator)
        self.patcher.start()

        self.servicer = CloudProvider(self.settings)
        await self.servicer.start()
        self.orch = _FakeOrchestrator.last_instance

        self.server = grpc.aio.server()
        pb_grpc.add_CloudProviderServicer_to_server(self.servicer, self.server)
        port = self.server.add_insecure_port("127.0.0.1:0")
        await self.server.start()

        self.channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
        self.stub = pb_grpc.CloudProviderStub(self.channel)

    async def asyncTearDown(self):
        await self.channel.close()
        await self.server.stop(None)
        await self.servicer.stop()
        self.patcher.stop()

    async def _assert_rpc_code(self, call, code: grpc.StatusCode):
        with self.assertRaises(grpc.aio.AioRpcError) as ctx:
            await call()
        self.assertEqual(ctx.exception.code(), code)

    async def test_node_groups(self):
        out = await self.stub.NodeGroups(pb.NodeGroupsRequest(), timeout=5)
        self.assertEqual(len(out.nodeGroups), 1)
        self.assertEqual(out.nodeGroups[0].id, "general")
        self.assertEqual(out.nodeGroups[0].minSize, 0)
        self.assertEqual(out.nodeGroups[0].maxSize, 5)

    async def test_node_group_for_node(self):
        self.orch.behavior["node_group_for_node"] = self.groups["general"]
        out = await self.stub.NodeGroupForNode(
            pb.NodeGroupForNodeRequest(node=pb.ExternalGrpcNode(name="n1", providerID="k3s://n1")),
            timeout=5,
        )
        self.assertEqual(out.nodeGroup.id, "general")

    async def test_node_group_for_node_empty(self):
        out = await self.stub.NodeGroupForNode(
            pb.NodeGroupForNodeRequest(node=pb.ExternalGrpcNode(name="n1", providerID="k3s://n1")),
            timeout=5,
        )
        self.assertEqual(out.nodeGroup.id, "")

    async def test_node_group_target_size(self):
        self.orch.behavior["node_group_target_size"] = 3
        out = await self.stub.NodeGroupTargetSize(pb.NodeGroupTargetSizeRequest(id="general"), timeout=5)
        self.assertEqual(out.targetSize, 3)

    async def test_node_group_increase_size_success(self):
        out = await self.stub.NodeGroupIncreaseSize(pb.NodeGroupIncreaseSizeRequest(id="general", delta=1), timeout=5)
        self.assertIsInstance(out, pb.NodeGroupIncreaseSizeResponse)

    async def test_node_group_increase_size_error_mapping(self):
        self.orch.behavior["node_group_increase_size"] = InvalidArgumentError("bad delta")
        await self._assert_rpc_code(
            lambda: self.stub.NodeGroupIncreaseSize(pb.NodeGroupIncreaseSizeRequest(id="general", delta=0), timeout=5),
            grpc.StatusCode.INVALID_ARGUMENT,
        )

        self.orch.behavior["node_group_increase_size"] = FailedPreconditionError("max exceeded")
        await self._assert_rpc_code(
            lambda: self.stub.NodeGroupIncreaseSize(pb.NodeGroupIncreaseSizeRequest(id="general", delta=10), timeout=5),
            grpc.StatusCode.FAILED_PRECONDITION,
        )

    async def test_node_group_delete_nodes_success(self):
        req = pb.NodeGroupDeleteNodesRequest(id="general", nodes=[pb.ExternalGrpcNode(name="n1", providerID="k3s://n1")])
        out = await self.stub.NodeGroupDeleteNodes(req, timeout=5)
        self.assertIsInstance(out, pb.NodeGroupDeleteNodesResponse)

    async def test_node_group_delete_nodes_not_found(self):
        self.orch.behavior["node_group_delete_nodes"] = NotFoundError("not found")
        req = pb.NodeGroupDeleteNodesRequest(id="general", nodes=[pb.ExternalGrpcNode(name="n1", providerID="k3s://n1")])
        await self._assert_rpc_code(lambda: self.stub.NodeGroupDeleteNodes(req, timeout=5), grpc.StatusCode.NOT_FOUND)

    async def test_node_group_decrease_target_size(self):
        out = await self.stub.NodeGroupDecreaseTargetSize(pb.NodeGroupDecreaseTargetSizeRequest(id="general", delta=-1), timeout=5)
        self.assertIsInstance(out, pb.NodeGroupDecreaseTargetSizeResponse)

    async def test_node_group_decrease_target_size_error_mapping(self):
        self.orch.behavior["node_group_decrease_target_size"] = InvalidArgumentError("delta must be < 0")
        await self._assert_rpc_code(
            lambda: self.stub.NodeGroupDecreaseTargetSize(pb.NodeGroupDecreaseTargetSizeRequest(id="general", delta=1), timeout=5),
            grpc.StatusCode.INVALID_ARGUMENT,
        )
        self.orch.behavior["node_group_decrease_target_size"] = FailedPreconditionError("cannot remove")
        await self._assert_rpc_code(
            lambda: self.stub.NodeGroupDecreaseTargetSize(pb.NodeGroupDecreaseTargetSizeRequest(id="general", delta=-10), timeout=5),
            grpc.StatusCode.FAILED_PRECONDITION,
        )

    async def test_node_group_nodes(self):
        self.orch.behavior["node_group_nodes"] = [
            VMInfo(vmid=101, name="ca-general-101", status="running", tags=[]),
            VMInfo(vmid=102, name="ca-general-102", status="stopped", tags=[]),
        ]
        out = await self.stub.NodeGroupNodes(pb.NodeGroupNodesRequest(id="general"), timeout=5)
        self.assertEqual([item.id for item in out.instances], ["k3s://ca-general-101", "k3s://ca-general-102"])
        self.assertEqual(out.instances[0].status.instanceState, pb.InstanceStatus.instanceRunning)
        self.assertEqual(out.instances[1].status.instanceState, pb.InstanceStatus.unspecified)

    async def test_template_node_info(self):
        self.orch.behavior["node_group_template_node_bytes"] = b"\x00\x01"
        out = await self.stub.NodeGroupTemplateNodeInfo(pb.NodeGroupTemplateNodeInfoRequest(id="general"), timeout=5)
        self.assertEqual(out.nodeBytes, b"\x00\x01")

    async def test_template_node_info_unavailable(self):
        self.orch.behavior["node_group_template_node_bytes"] = RuntimeError("backend error")
        await self._assert_rpc_code(
            lambda: self.stub.NodeGroupTemplateNodeInfo(pb.NodeGroupTemplateNodeInfoRequest(id="general"), timeout=5),
            grpc.StatusCode.UNAVAILABLE,
        )

    async def test_group_not_found_mapping(self):
        self.orch.behavior["node_group_target_size"] = GroupNotFoundError("unknown group")
        await self._assert_rpc_code(
            lambda: self.stub.NodeGroupTargetSize(pb.NodeGroupTargetSizeRequest(id="missing"), timeout=5),
            grpc.StatusCode.NOT_FOUND,
        )

    async def test_node_group_get_options(self):
        defaults = pb.NodeGroupAutoscalingOptions(zeroOrMaxNodeScaling=True, ignoreDaemonSetsUtilization=True)
        out = await self.stub.NodeGroupGetOptions(
            pb.NodeGroupAutoscalingOptionsRequest(id="general", defaults=defaults),
            timeout=5,
        )
        self.assertTrue(out.nodeGroupAutoscalingOptions.zeroOrMaxNodeScaling)
        self.assertTrue(out.nodeGroupAutoscalingOptions.ignoreDaemonSetsUtilization)

    async def test_node_group_get_options_unknown_group(self):
        defaults = pb.NodeGroupAutoscalingOptions(zeroOrMaxNodeScaling=True)
        await self._assert_rpc_code(
            lambda: self.stub.NodeGroupGetOptions(
                pb.NodeGroupAutoscalingOptionsRequest(id="missing", defaults=defaults),
                timeout=5,
            ),
            grpc.StatusCode.NOT_FOUND,
        )

    async def test_misc_rpcs(self):
        self.assertEqual((await self.stub.GPULabel(pb.GPULabelRequest(), timeout=5)).label, "")
        gpu_types = await self.stub.GetAvailableGPUTypes(pb.GetAvailableGPUTypesRequest(), timeout=5)
        self.assertEqual(dict(gpu_types.gpuTypes), {})
        self.assertIsInstance(await self.stub.Cleanup(pb.CleanupRequest(), timeout=5), pb.CleanupResponse)
        self.assertIsInstance(await self.stub.Refresh(pb.RefreshRequest(), timeout=5), pb.RefreshResponse)


if __name__ == "__main__":
    unittest.main()
