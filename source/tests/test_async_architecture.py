from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BASE_DIR))
subprocess.run([sys.executable, str(BASE_DIR / "source" / "scripts" / "generate-proto.py")], check=True)

from source.group_context import ManagedNode, STATE_ACTIVE, STATE_PENDING  # noqa: E402
from source.models import GroupConfig, K3sConfig, ProxmoxConfig, Settings  # noqa: E402
from source.orchestrator import ProvisioningOrchestrator  # noqa: E402
from source.state_store import StateStore  # noqa: E402
from source.utils import parse_tags  # noqa: E402


class _FakeProxmox:
    def __init__(self):
        self.vms: list[dict[str, Any]] = []
        self.cfgs: dict[int, dict[str, Any]] = {}
        self.deleted: list[int] = []
        self.next_vmid = 100

    async def list_vms(self) -> list[dict[str, Any]]:
        return list(self.vms)

    async def vm_config(self, vmid: int) -> dict[str, Any]:
        return dict(self.cfgs.get(vmid, {}))

    async def nextid(self) -> int:
        self.next_vmid += 1
        return self.next_vmid

    async def iso_exists(self, iso_name: str) -> bool:
        return True

    async def upload(self, *, storage: str, filename: str, content: str, file_bytes: bytes) -> Any:
        return "ok"

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
    ) -> int:
        self.vms.append({"vmid": vmid, "name": name, "status": "running", "tags": tags})
        return vmid

    async def stop_and_delete(self, vmid: int) -> None:
        self.deleted.append(vmid)
        self.vms = [vm for vm in self.vms if int(vm.get("vmid")) != int(vmid)]


class _FakeKube:
    def __init__(self):
        self.nodes: list[dict[str, Any]] = []
        self.deleted: list[str] = []

    async def list_nodes(self) -> list[dict[str, Any]]:
        return list(self.nodes)

    async def delete_node(self, node_name: str) -> None:
        if node_name:
            self.deleted.append(node_name)

    async def get_node(self, node_name: str) -> dict[str, Any]:
        for node in self.nodes:
            if str((node.get("metadata") or {}).get("name") or "") == node_name:
                return node
        return {}

    async def build_template_node_bytes(self, payload: dict[str, Any]) -> bytes:
        return b"k8s\x00dummy"


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


class AsyncArchitectureTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        db_path = Path(self.tmpdir.name) / "state.db"
        self.state = StateStore(db_path)
        await self.state.init()
        self.proxmox = _FakeProxmox()
        self.kube = _FakeKube()
        self.group = _group("general")
        self.orch = ProvisioningOrchestrator(
            settings=_settings({"general": self.group}),
            proxmox=self.proxmox,
            kube=self.kube,
            state=self.state,
            pending_vm_timeout_seconds=900,
            reconcile_interval_seconds=20,
        )

    async def asyncTearDown(self):
        await self.orch.stop()
        self.tmpdir.cleanup()

    async def test_desired_size_round_trip(self):
        await self.orch.start()
        self.assertEqual(await self.orch.node_group_target_size("general"), 0)
        await self.orch.node_group_increase_size("general", 2)
        self.assertEqual(await self.orch.node_group_target_size("general"), 2)
        await self.orch.node_group_decrease_target_size("general", -1)
        self.assertEqual(await self.orch.node_group_target_size("general"), 1)

    async def test_reconcile_creates_to_match_desired_size(self):
        await self.state.set_desired_size("general", 2)
        await self.orch.reconcile.reconcile_group(self.group)
        self.assertEqual(len(self.proxmox.vms), 2)
        self.assertEqual(await self.state.count_group_vm_states("general", {STATE_PENDING}), 2)

    async def test_reconcile_promotes_ready_pending_vm_to_active(self):
        self.proxmox.vms = [
            {"vmid": 101, "name": "ca-general-101", "status": "running", "tags": "ca-group-general"},
        ]
        await self.state.upsert_vm_state(
            vmid=101,
            group_id="general",
            vm_name="ca-general-101",
            state=STATE_PENDING,
            pending_since=1,
        )
        self.kube.nodes = [
            {
                "metadata": {
                    "name": "ca-general-101",
                    "labels": {"autoscaler.proxmox/group": "general", "autoscaler.proxmox/vmid": "101"},
                },
                "status": {"conditions": [{"type": "Ready", "status": "True"}]},
            }
        ]
        await self.orch.reconcile.reconcile_group(self.group)
        state = await self.state.get_vm_state(101)
        self.assertIsNotNone(state)
        self.assertEqual(state.state, STATE_ACTIVE)

    async def test_delete_nodes_reduces_desired_size(self):
        await self.orch.start()
        await self.state.set_desired_size("general", 2)
        self.proxmox.vms = [
            {"vmid": 101, "name": "ca-general-101", "status": "running", "tags": "ca-group-general"},
            {"vmid": 102, "name": "ca-general-102", "status": "running", "tags": "ca-group-general"},
        ]
        await self.state.upsert_vm_state(vmid=101, group_id="general", vm_name="ca-general-101", state=STATE_ACTIVE, pending_since=None)
        await self.state.upsert_vm_state(vmid=102, group_id="general", vm_name="ca-general-102", state=STATE_ACTIVE, pending_since=None)

        await self.orch.node_group_delete_nodes(
            "general",
            [
                ManagedNode(provider_id="k3s://ca-general-101", name="ca-general-101", labels={}),
            ],
        )
        self.assertEqual(await self.orch.node_group_target_size("general"), 1)
        self.assertEqual(self.proxmox.deleted, [101])

    async def test_target_size_not_counting_vm_health(self):
        await self.orch.start()
        await self.orch.node_group_increase_size("general", 1)
        self.assertEqual(await self.orch.node_group_target_size("general"), 1)

    def test_parse_tags_deduplicates(self):
        self.assertEqual(parse_tags("a;b,a;;b;c"), ["a", "b", "c"])
        self.assertEqual(parse_tags(None), [])


if __name__ == "__main__":
    unittest.main()
