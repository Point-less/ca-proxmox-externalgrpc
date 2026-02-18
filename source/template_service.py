from __future__ import annotations

import logging
from typing import Any

from .contracts import KubernetesService
from .group_context import GroupContext
from .models import GroupConfig

LOG = logging.getLogger("proxmox-ca-externalgrpc")


class TemplateService:
    def __init__(self, *, context: GroupContext, kube: KubernetesService):
        self.context = context
        self.kube = kube

    async def node_group_template_node_bytes(self, group_id: str) -> bytes:
        group = self.context.group(group_id)
        payload = await self._template_node_payload(group)
        return await self.kube.build_template_node_bytes(payload)

    async def _pick_template_node_name(self, group: GroupConfig) -> str:
        items = await self.kube.list_nodes()
        for item in items:
            labels = (item.get("metadata") or {}).get("labels") or {}
            if labels.get("autoscaler.proxmox/group", "").strip() == group.id:
                name = ((item.get("metadata") or {}).get("name") or "").strip()
                if name:
                    return name
        for item in items:
            labels = (item.get("metadata") or {}).get("labels") or {}
            is_control_plane = any(
                (
                    labels.get("node-role.kubernetes.io/control-plane") is not None,
                    labels.get("node-role.kubernetes.io/master") is not None,
                )
            )
            if not is_control_plane:
                name = ((item.get("metadata") or {}).get("name") or "").strip()
                if name:
                    return name
        if items:
            return str(((items[0].get("metadata") or {}).get("name") or "")).strip()
        return ""

    def _parse_group_taint(self, raw: str) -> dict[str, str] | None:
        value = (raw or "").strip()
        if not value:
            return None
        effect = "NoSchedule"
        key_value = value
        if ":" in value:
            key_value, effect = value.rsplit(":", 1)
        key_value = key_value.strip()
        effect = effect.strip() or "NoSchedule"
        if not key_value:
            return None
        if "=" in key_value:
            key, taint_value = key_value.split("=", 1)
            key = key.strip()
            taint_value = taint_value.strip()
            if not key:
                return None
            out: dict[str, str] = {"key": key, "effect": effect}
            if taint_value:
                out["value"] = taint_value
            return out
        return {"key": key_value, "effect": effect}

    def _parse_group_label(self, raw: str) -> tuple[str, str] | None:
        value = (raw or "").strip()
        if not value or "=" not in value:
            return None
        key, label_value = value.split("=", 1)
        key = key.strip()
        label_value = label_value.strip()
        if not key:
            return None
        return key, label_value

    async def _template_node_payload(self, group: GroupConfig) -> dict[str, Any]:
        node_name = await self._pick_template_node_name(group)
        base_labels: dict[str, str] = {}
        base_capacity: dict[str, str] = {}
        base_allocatable: dict[str, str] = {}
        if node_name:
            try:
                payload = await self.kube.get_node(node_name)
                meta = payload.get("metadata", {}) if isinstance(payload, dict) else {}
                status = payload.get("status", {}) if isinstance(payload, dict) else {}
                labels = meta.get("labels", {}) if isinstance(meta, dict) else {}
                capacity = status.get("capacity", {}) if isinstance(status, dict) else {}
                allocatable = status.get("allocatable", {}) if isinstance(status, dict) else {}
                if isinstance(labels, dict):
                    for key in (
                        "kubernetes.io/arch",
                        "kubernetes.io/os",
                        "topology.kubernetes.io/region",
                        "topology.kubernetes.io/zone",
                    ):
                        value = str(labels.get(key, "")).strip()
                        if value:
                            base_labels[key] = value
                if isinstance(capacity, dict):
                    base_capacity = {str(k): str(v) for k, v in capacity.items()}
                if isinstance(allocatable, dict):
                    base_allocatable = {str(k): str(v) for k, v in allocatable.items()}
            except Exception as exc:
                LOG.warning("Failed to read base node for template group=%s: %s", group.id, exc)

        labels = dict(base_labels)
        labels["autoscaler.proxmox/group"] = group.id
        labels["autoscaled"] = "true"
        for raw in group.labels:
            parsed = self._parse_group_label(raw)
            if parsed is not None:
                key, value = parsed
                labels[key] = value

        taints: list[dict[str, str]] = []
        for raw in group.taints:
            parsed = self._parse_group_taint(raw)
            if parsed is not None:
                taints.append(parsed)

        capacity = dict(base_capacity)
        allocatable = dict(base_allocatable)
        capacity["cpu"] = str(max(1, group.cores))
        capacity["memory"] = f"{max(256, group.memory_mb)}Mi"
        capacity["pods"] = str(max(int(capacity.get("pods", "110") or 110), 32))
        allocatable["cpu"] = capacity["cpu"]
        allocatable["memory"] = capacity["memory"]
        allocatable["pods"] = capacity["pods"]

        return {
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": f"proxmox-ca-template-{group.id}", "labels": labels},
            "spec": {"taints": taints},
            "status": {"capacity": capacity, "allocatable": allocatable},
        }
