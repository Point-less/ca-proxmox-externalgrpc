from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from core.models import GroupConfig, K3sConfig, ProxmoxConfig, Settings
from .utils import as_bool, normalize_pm_api_base, read_optional


def load_settings(config_path: Path) -> Settings:
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}

    proxmox_raw = raw.get("proxmox", {})
    k3s_raw = raw.get("k3s", {})

    def env_or(name: str, default: Any) -> Any:
        value = os.getenv(name)
        return value if (value is not None and str(value).strip() != "") else default

    proxmox = ProxmoxConfig(
        api_url=normalize_pm_api_base(str(env_or("PM_API_URL", proxmox_raw.get("api_url", "")))),
        node=str(env_or("PM_NODE", proxmox_raw.get("node", ""))),
        token_id=str(env_or("PM_SERVICE_TOKEN_ID", proxmox_raw.get("token_id", ""))),
        token_secret=str(env_or("PM_SERVICE_TOKEN_SECRET", proxmox_raw.get("token_secret", ""))),
        tls_insecure=as_bool(env_or("PM_TLS_INSECURE", proxmox_raw.get("tls_insecure", True))),
        import_storage=str(env_or("IMPORT_STORAGE", proxmox_raw.get("import_storage", "local"))),
        iso_storage=str(env_or("ISO_STORAGE", proxmox_raw.get("iso_storage", "local"))),
        vm_storage=str(env_or("VM_STORAGE", proxmox_raw.get("vm_storage", "local-lvm"))),
        bridge=str(env_or("BRIDGE", proxmox_raw.get("bridge", "vmbr0"))),
        cloud_image_url=str(env_or("CLOUD_IMAGE_URL", proxmox_raw.get("cloud_image_url", ""))),
        verify_certificates=as_bool(
            env_or("PM_VERIFY_CERTIFICATES", proxmox_raw.get("verify_certificates", False))
        ),
    )

    registries_yaml = str(k3s_raw.get("registries_yaml", ""))
    registries_yaml_file = str(k3s_raw.get("registries_yaml_file", "")).strip()
    if not registries_yaml and registries_yaml_file:
        registries_yaml = read_optional(Path(registries_yaml_file))

    k3s = K3sConfig(
        version=str(env_or("K3S_VERSION", k3s_raw.get("version", "v1.34.4+k3s1"))),
        server_url=str(env_or("K3S_SERVER_URL", k3s_raw.get("server_url", ""))),
        cluster_token=str(env_or("K3S_CLUSTER_TOKEN", k3s_raw.get("cluster_token", ""))),
        ssh_public_key=str(env_or("SSH_PUBLIC_KEY", k3s_raw.get("ssh_public_key", ""))).strip(),
        registries_yaml=registries_yaml,
    )

    groups: dict[str, GroupConfig] = {}
    for group in raw.get("node_groups", []) or []:
        item = GroupConfig(
            id=str(group["id"]),
            vm_name_prefix=str(group.get("vm_name_prefix") or f"ca-{group['id']}"),
            min_size=int(group.get("min_size", 0)),
            max_size=int(group.get("max_size", 10)),
            cores=int(group.get("cores", 2)),
            memory_mb=int(group.get("memory_mb", 2048)),
            balloon_mb=int(group.get("balloon_mb", 0)),
            disk_size=str(group.get("disk_size", "20G")),
            labels=[str(x) for x in (group.get("labels", []) or [])],
            taints=[str(x) for x in (group.get("taints", []) or [])],
        )
        groups[item.id] = item

    settings = Settings(
        proxmox=proxmox,
        k3s=k3s,
        vm_tag_prefix=str(raw.get("defaults", {}).get("vm_tag_prefix", "proxmox_test2")),
        groups=groups,
    )

    required = {
        "PM_API_URL": settings.proxmox.api_url,
        "PM_NODE": settings.proxmox.node,
        "PM_SERVICE_TOKEN_ID": settings.proxmox.token_id,
        "PM_SERVICE_TOKEN_SECRET": settings.proxmox.token_secret,
        "CLOUD_IMAGE_URL": settings.proxmox.cloud_image_url,
        "K3S_SERVER_URL": settings.k3s.server_url,
        "K3S_CLUSTER_TOKEN": settings.k3s.cluster_token,
        "SSH_PUBLIC_KEY": settings.k3s.ssh_public_key,
    }
    missing = [key for key, value in required.items() if not str(value).strip()]
    if missing:
        raise SystemExit(f"Missing required settings: {', '.join(missing)}")
    if not settings.groups:
        raise SystemExit("No node_groups configured")
    return settings
