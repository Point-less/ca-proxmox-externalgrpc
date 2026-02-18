from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_PROTO_READY = False


def _resolve_layout() -> tuple[Path, Path, Path]:
    for parent in Path(__file__).resolve().parents:
        repo_script = parent / "source" / "scripts" / "generate-proto.py"
        if repo_script.exists():
            return parent, parent / "source", repo_script
        flat_script = parent / "scripts" / "generate-proto.py"
        if flat_script.exists() and (parent / "core").exists():
            return parent, parent, flat_script
    raise RuntimeError("could not locate generate-proto.py")


def bootstrap_tests() -> None:
    global _PROTO_READY
    _base_dir, package_dir, proto_script = _resolve_layout()
    if str(package_dir) not in sys.path:
        sys.path.insert(0, str(package_dir))

    if _PROTO_READY:
        return
    subprocess.run([sys.executable, str(proto_script)], check=True)
    _PROTO_READY = True


def make_group(group_id: str):
    from core.models import GroupConfig

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


def make_proxmox_config():
    from core.models import ProxmoxConfig

    return ProxmoxConfig(
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
    )


def make_settings(groups):
    from core.models import K3sConfig, Settings

    return Settings(
        proxmox=make_proxmox_config(),
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
