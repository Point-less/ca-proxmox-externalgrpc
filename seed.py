from __future__ import annotations

import hashlib
import io
from pathlib import Path

import pycdlib
from jinja2 import Environment, FileSystemLoader, StrictUndefined

from models import K3sConfig

BASE_DIR = Path(__file__).resolve().parent


def render_seed(*, k3s: K3sConfig, hostname: str, node_labels: list[str], node_taints: list[str]) -> tuple[str, str]:
    env = Environment(
        loader=FileSystemLoader(str(BASE_DIR / "cidata")),
        undefined=StrictUndefined,
        autoescape=False,
        keep_trailing_newline=True,
    )
    meta = env.get_template("meta-data.yml.j2").render(hostname=hostname)
    user = env.get_template("user-data.agent.yml.j2").render(
        role="agent",
        ssh_public_key=k3s.ssh_public_key,
        k3s_version=k3s.version,
        cluster_token=k3s.cluster_token,
        server_url=k3s.server_url,
        node_labels=node_labels,
        node_taints=node_taints,
        registries_yaml=k3s.registries_yaml,
    )
    return meta, user


def make_cidata_iso_bytes(*, meta_data: str, user_data: str) -> bytes:
    iso = pycdlib.PyCdlib()
    iso.new(interchange_level=3, vol_ident="CIDATA", joliet=3, rock_ridge="1.09")

    def add(name: str, content: str) -> None:
        payload = content.encode("utf-8")
        iso.add_fp(
            io.BytesIO(payload),
            len(payload),
            iso_path=f"/{name.replace('-', '_').upper()};1",
            joliet_path=f"/{name}",
            rr_name=name,
        )

    add("meta-data", meta_data)
    add("user-data", user_data)

    out = io.BytesIO()
    iso.write_fp(out)
    iso.close()
    return out.getvalue()


def seed_iso_name(*, vm_name: str, meta: str, user: str) -> str:
    digest = hashlib.sha256((meta + "\n" + user).encode("utf-8")).hexdigest()[:12]
    return f"seed-{vm_name}-{digest}.iso"

