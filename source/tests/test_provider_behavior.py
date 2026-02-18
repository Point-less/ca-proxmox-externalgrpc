from __future__ import annotations

import subprocess
import sys
import threading
import unittest
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BASE_DIR))
subprocess.run([sys.executable, str(BASE_DIR / "source" / "scripts" / "generate-proto.py")], check=True)

from source import externalgrpc_pb2 as pb  # noqa: E402
from source.models import GroupConfig, K3sConfig, ProxmoxConfig, Settings, VMInfo  # noqa: E402
from source.provider import CloudProvider  # noqa: E402
from source.utils import parse_tags  # noqa: E402


class _DummyContext:
    def abort(self, code, message):  # pragma: no cover - only used by failure paths
        raise RuntimeError(f"{code}: {message}")


class _FakePve:
    def __init__(self):
        self._vms = []
        self._cfg = {}

    def list_vms(self):
        return self._vms

    def vm_config(self, vmid):
        return self._cfg.get(vmid, {})


def _group(
    group_id: str,
    labels: list[str] | None = None,
    taints: list[str] | None = None,
) -> GroupConfig:
    return GroupConfig(
        id=group_id,
        vm_name_prefix=f"ca-{group_id}",
        min_size=0,
        max_size=5,
        cores=2,
        memory_mb=4096,
        balloon_mb=2048,
        disk_size="20G",
        labels=labels or [],
        taints=taints or [],
    )


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


class ProviderBehaviorTests(unittest.TestCase):
    def _provider(self, groups: dict[str, GroupConfig]) -> CloudProvider:
        provider = object.__new__(CloudProvider)
        provider.settings = _settings(groups)
        provider.pve = _FakePve()
        provider._pending_creates = {}
        provider._pending_lock = threading.Lock()
        return provider

    def test_parse_tags_deduplicates_and_handles_separators(self):
        self.assertEqual(parse_tags("a;b,a;;b;c"), ["a", "b", "c"])
        self.assertEqual(parse_tags(None), [])

    def test_group_vms_filters_by_group_tag_and_falls_back_to_vm_config_tags(self):
        group = _group("general")
        provider = self._provider({"general": group})
        provider.pve._vms = [
            {"vmid": 101, "name": "ca-general-101", "status": "running", "tags": "ca-group-general"},
            {"vmid": 102, "name": "ca-general-102", "status": "running", "tags": ""},
            {"vmid": 201, "name": "ca-other-201", "status": "running", "tags": "ca-group-other"},
        ]
        provider.pve._cfg = {
            102: {"tags": "foo;ca-group-general"},
        }

        out = provider._group_vms(group)
        self.assertEqual([vm.vmid for vm in out], [101, 102])
        self.assertEqual([vm.name for vm in out], ["ca-general-101", "ca-general-102"])

    def test_node_group_target_size_includes_pending_creates(self):
        group = _group("general")
        provider = self._provider({"general": group})
        provider._group = lambda group_id, context: group
        provider._group_vms = lambda g: [VMInfo(vmid=1, name="n1", status="running", tags=[])]
        provider._pending_count = lambda group_id: 2

        out = provider.NodeGroupTargetSize(pb.NodeGroupTargetSizeRequest(id="general"), _DummyContext())
        self.assertEqual(out.targetSize, 3)

    def test_node_group_nodes_uses_k3s_provider_ids(self):
        group = _group("general")
        provider = self._provider({"general": group})
        provider._group = lambda group_id, context: group
        provider._group_vms = lambda g: [VMInfo(vmid=123, name="ca-general-123", status="running", tags=[])]
        provider._prune_stale_kube_nodes_for_group = lambda g: None

        out = provider.NodeGroupNodes(pb.NodeGroupNodesRequest(id="general"), _DummyContext())
        self.assertEqual(len(out.instances), 1)
        self.assertEqual(out.instances[0].id, "k3s://ca-general-123")
        self.assertEqual(out.instances[0].status.instanceState, pb.InstanceStatus.instanceRunning)

    def test_template_node_payload_contains_group_labels_taints_and_resources(self):
        group = _group(
            "gitea-runners",
            labels=["workload-tier=gitea-runner", "autoscaled=true", "invalidlabel"],
            taints=["dedicated=gitea:NoSchedule", "custom-taint"],
        )
        provider = self._provider({"gitea-runners": group})
        provider._pick_template_node_name = lambda g: ""

        payload = provider._template_node_payload(group)
        labels = payload["metadata"]["labels"]
        self.assertEqual(labels["autoscaler.proxmox/group"], "gitea-runners")
        self.assertEqual(labels["workload-tier"], "gitea-runner")
        self.assertEqual(labels["autoscaled"], "true")

        taints = payload["spec"]["taints"]
        self.assertIn({"key": "dedicated", "value": "gitea", "effect": "NoSchedule"}, taints)
        self.assertIn({"key": "custom-taint", "effect": "NoSchedule"}, taints)

        capacity = payload["status"]["capacity"]
        allocatable = payload["status"]["allocatable"]
        self.assertEqual(capacity["cpu"], "2")
        self.assertEqual(capacity["memory"], "4096Mi")
        self.assertEqual(allocatable["cpu"], "2")
        self.assertEqual(allocatable["memory"], "4096Mi")


if __name__ == "__main__":
    unittest.main()
