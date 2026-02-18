from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ProxmoxConfig:
    api_url: str
    node: str
    token_id: str
    token_secret: str
    tls_insecure: bool
    import_storage: str
    iso_storage: str
    vm_storage: str
    bridge: str
    cloud_image_url: str
    verify_certificates: bool


@dataclass(frozen=True)
class K3sConfig:
    version: str
    server_url: str
    cluster_token: str
    ssh_public_key: str
    registries_yaml: str = ""


@dataclass(frozen=True)
class GroupConfig:
    id: str
    vm_name_prefix: str
    min_size: int
    max_size: int
    cores: int
    memory_mb: int
    balloon_mb: int
    disk_size: str
    labels: list[str] = field(default_factory=list)
    taints: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Settings:
    proxmox: ProxmoxConfig
    k3s: K3sConfig
    vm_tag_prefix: str
    groups: dict[str, GroupConfig]


@dataclass(frozen=True)
class VMInfo:
    vmid: int
    name: str
    status: str
    tags: list[str]

