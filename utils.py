from __future__ import annotations

import re
from pathlib import Path
from typing import Any


def normalize_pm_api_base(url: str) -> str:
    out = (url or "").rstrip("/")
    if out.endswith("/api2/json"):
        out = out[: -len("/api2/json")]
    return out


def parse_tags(raw: str | None) -> list[str]:
    if not raw:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for part in str(raw).replace(",", ";").split(";"):
        tag = part.strip()
        if not tag or tag in seen:
            continue
        seen.add(tag)
        out.append(tag)
    return out


def _read_varint(buf: bytes, pos: int) -> tuple[int, int]:
    shift = 0
    value = 0
    i = pos
    while i < len(buf):
        b = buf[i]
        value |= (b & 0x7F) << shift
        i += 1
        if not (b & 0x80):
            return value, i
        shift += 7
        if shift >= 64:
            break
    raise ValueError("invalid protobuf varint")


def unwrap_k8s_protobuf(payload: bytes) -> bytes:
    """
    Kubernetes API protobuf responses are wrapped with 'k8s\\0' + runtime.Unknown.
    Cluster Autoscaler externalgrpc expects raw v1.Node#Marshal() bytes.
    """
    if not payload.startswith(b"k8s\x00"):
        return payload

    data = payload[4:]
    i = 0
    raw: bytes | None = None
    while i < len(data):
        key, i = _read_varint(data, i)
        field_no = key >> 3
        wire_type = key & 0x7

        if wire_type == 0:
            _, i = _read_varint(data, i)
            continue
        if wire_type == 1:
            i += 8
            continue
        if wire_type == 5:
            i += 4
            continue
        if wire_type == 2:
            length, i = _read_varint(data, i)
            end = i + length
            if end > len(data):
                raise ValueError("invalid protobuf length-delimited field")
            value = data[i:end]
            i = end
            if field_no == 2:
                raw = value
            continue
        raise ValueError(f"unsupported protobuf wire type: {wire_type}")

    if raw is None:
        raise ValueError("runtime.Unknown payload missing raw field")
    return raw


def vmid_from_provider_id(provider_id: str) -> int | None:
    m = re.search(r"(\d+)$", provider_id or "")
    if not m:
        return None
    return int(m.group(1))


def as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on", "y"}


def read_optional(path: Path) -> str:
    if not path.exists():
        return ""
    content = path.read_text(encoding="utf-8")
    for raw in content.splitlines():
        if raw.strip() and not raw.strip().startswith("#"):
            return content
    return ""

