from __future__ import annotations

import unittest
from typing import Any

from helpers import bootstrap_tests, make_proxmox_config

bootstrap_tests()

from core.models import ProxmoxConfig
from infra.pve import PveClient


def _cfg() -> ProxmoxConfig:
    return make_proxmox_config()


class _MockPveClient(PveClient):
    def __init__(self, responses: dict[tuple[str, str], Any]):
        super().__init__(_cfg())
        self.responses = responses
        self.calls: list[tuple[str, str]] = []
        self.waited: list[str] = []

    async def json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> Any:
        self.calls.append((method.upper(), path))
        key = (method.upper(), path)
        if key not in self.responses:
            raise AssertionError(f"Unexpected call: {key}")
        return self.responses[key]

    async def _wait_upid(self, upid: Any) -> None:
        if isinstance(upid, str):
            self.waited.append(upid)

    async def close(self) -> None:
        return None


class PveSeedCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_attached_seed_iso_parses_only_seed_iso(self):
        client = _MockPveClient(
            {
                ("GET", "/nodes/pve/qemu/101/config"): {"ide2": "local:iso/seed-ca-general-101-abc123def456.iso,media=cdrom"},
                ("GET", "/nodes/pve/qemu/202/config"): {"ide2": "local:iso/ubuntu-live.iso,media=cdrom"},
            }
        )

        self.assertEqual(
            await client.attached_seed_iso(101),
            ("local", "iso/seed-ca-general-101-abc123def456.iso"),
        )
        self.assertIsNone(await client.attached_seed_iso(202))

    async def test_stop_and_delete_vm_only_vm_resources(self):
        client = _MockPveClient(
            {
                ("POST", "/nodes/pve/qemu/101/status/stop"): "UPID:stop",
                ("DELETE", "/nodes/pve/qemu/101"): "UPID:del-vm",
            }
        )

        await client.stop_and_delete_vm(101)

        self.assertEqual(
            client.calls,
            [
                ("POST", "/nodes/pve/qemu/101/status/stop"),
                ("DELETE", "/nodes/pve/qemu/101"),
            ],
        )
        self.assertEqual(client.waited, ["UPID:stop", "UPID:del-vm"])

    async def test_delete_storage_volume(self):
        client = _MockPveClient(
            {
                ("DELETE", "/nodes/pve/storage/local/content/iso%2Fseed-ca-general-101-abc123def456.iso"): "UPID:del-iso",
            }
        )

        await client.delete_storage_volume("local", "iso/seed-ca-general-101-abc123def456.iso")

        self.assertEqual(
            client.calls,
            [("DELETE", "/nodes/pve/storage/local/content/iso%2Fseed-ca-general-101-abc123def456.iso")],
        )
        self.assertEqual(client.waited, ["UPID:del-iso"])


if __name__ == "__main__":
    unittest.main()
