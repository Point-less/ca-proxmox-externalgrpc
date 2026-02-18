from __future__ import annotations

import logging
import threading
import time
import urllib.parse
from concurrent import futures
from pathlib import Path
from typing import Any

import grpc
import requests

from models import GroupConfig, Settings, VMInfo
from proto_stubs import pb, pb_grpc
from pve import PveClient
from seed import make_cidata_iso_bytes, render_seed, seed_iso_name
from utils import parse_tags, unwrap_k8s_protobuf, vmid_from_provider_id

LOG = logging.getLogger("proxmox-ca-externalgrpc")
SA_TOKEN_PATH = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
SA_CA_CRT_PATH = Path("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
INCLUSTER_API_URL = "https://kubernetes.default.svc"


class CloudProvider(pb_grpc.CloudProviderServicer):
    def __init__(self, settings: Settings):
        self.settings = settings
        self.pve = PveClient(settings.proxmox)
        self._kube = requests.Session()
        self._pending_lock = threading.Lock()
        self._pending_creates: dict[str, int] = {}
        self._create_executor = futures.ThreadPoolExecutor(max_workers=1)
        self._reconciler = threading.Thread(target=self._reconcile_loop, daemon=True)
        self._reconciler.start()

    def _group(self, group_id: str, context: grpc.ServicerContext) -> GroupConfig:
        group = self.settings.groups.get(group_id)
        if group is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"unknown node group: {group_id}")
        return group

    def _group_tag(self, group: GroupConfig) -> str:
        return f"ca-group-{group.id}"

    def _group_vms(self, group: GroupConfig) -> list[VMInfo]:
        out: list[VMInfo] = []
        want = self._group_tag(group)
        for vm in self.pve.list_vms():
            try:
                vmid = int(vm.get("vmid"))
            except Exception:
                continue
            name = str(vm.get("name") or "")
            status = str(vm.get("status") or "")
            tags = parse_tags(vm.get("tags"))
            if not tags:
                try:
                    tags = parse_tags(self.pve.vm_config(vmid).get("tags"))
                except requests.HTTPError:
                    tags = []
            if want in tags:
                out.append(VMInfo(vmid=vmid, name=name, status=status, tags=tags))
        return sorted(out, key=lambda x: x.vmid)

    def _find_vm_for_node(self, group: GroupConfig, node: pb.ExternalGrpcNode) -> VMInfo | None:
        vms = self._group_vms(group)
        by_vmid = {v.vmid: v for v in vms}

        vmid = vmid_from_provider_id(node.providerID)
        if vmid is not None and vmid in by_vmid:
            return by_vmid[vmid]

        node_name = (node.name or "").strip()
        if node_name:
            for vm in vms:
                if vm.name == node_name:
                    return vm
        return None

    def _create_vm(self, group: GroupConfig) -> VMInfo:
        vmid = self.pve.nextid()
        vm_name = f"{group.vm_name_prefix}-{vmid}"
        node_labels = list(group.labels)
        node_labels.append(f"autoscaler.proxmox/group={group.id}")
        node_labels.append(f"autoscaler.proxmox/vmid={vmid}")
        meta, user = render_seed(
            k3s=self.settings.k3s,
            hostname=vm_name,
            node_labels=node_labels,
            node_taints=group.taints,
        )
        iso_name = seed_iso_name(vm_name=vm_name, meta=meta, user=user)
        if not self.pve.iso_exists(iso_name):
            iso_bytes = make_cidata_iso_bytes(meta_data=meta, user_data=user)
            self.pve.upload(storage=self.settings.proxmox.iso_storage, filename=iso_name, content="iso", file_bytes=iso_bytes)

        tags = ";".join(
            [
                self.settings.vm_tag_prefix,
                "ca-managed",
                self._group_tag(group),
            ]
        )
        vmid = self.pve.create_vm_from_image(
            vmid=vmid,
            name=vm_name,
            cores=group.cores,
            memory_mb=group.memory_mb,
            balloon_mb=group.balloon_mb,
            disk_size=group.disk_size,
            tags=tags,
            iso_name=iso_name,
        )
        LOG.info("Created VM vmid=%s name=%s group=%s", vmid, vm_name, group.id)
        return VMInfo(vmid=vmid, name=vm_name, status="running", tags=parse_tags(tags))

    def _pending_count(self, group_id: str) -> int:
        with self._pending_lock:
            return int(self._pending_creates.get(group_id, 0))

    def _add_pending(self, group_id: str, delta: int) -> None:
        with self._pending_lock:
            next_value = int(self._pending_creates.get(group_id, 0)) + int(delta)
            self._pending_creates[group_id] = max(0, next_value)

    def _enqueue_vm_create(self, group: GroupConfig) -> None:
        self._add_pending(group.id, 1)

        def _job() -> None:
            try:
                self._create_vm(group)
            except Exception:
                LOG.exception("Asynchronous VM create failed for group=%s", group.id)
            finally:
                self._add_pending(group.id, -1)

        self._create_executor.submit(_job)

    def _kube_headers(
        self,
        *,
        accept: str = "application/json",
        content_type: str | None = None,
    ) -> dict[str, str]:
        if not SA_TOKEN_PATH.exists():
            raise RuntimeError(f"Missing service account token: {SA_TOKEN_PATH}")
        token = SA_TOKEN_PATH.read_text(encoding="utf-8").strip()
        if not token:
            raise RuntimeError("Service account token is empty")
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": accept,
        }
        if content_type:
            headers["Content-Type"] = content_type
        return headers

    def _kube_request(
        self,
        method: str,
        path: str,
        *,
        accept: str = "application/json",
        content_type: str | None = None,
        data: Any | None = None,
        json_body: Any | None = None,
    ) -> requests.Response:
        if not SA_CA_CRT_PATH.exists():
            raise RuntimeError(f"Missing service account CA certificate: {SA_CA_CRT_PATH}")
        resp = self._kube.request(
            method=method.upper(),
            url=f"{INCLUSTER_API_URL}{path}",
            headers=self._kube_headers(accept=accept, content_type=content_type),
            data=data,
            json=json_body,
            verify=str(SA_CA_CRT_PATH),
            timeout=20,
        )
        resp.raise_for_status()
        return resp

    def _delete_kube_node(self, node_name: str) -> None:
        if not node_name:
            return
        node_path = f"/api/v1/nodes/{urllib.parse.quote(node_name, safe='')}"
        try:
            self._kube_request("DELETE", node_path)
            LOG.info("Deleted Kubernetes node object name=%s", node_name)
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else None
            if code == 404:
                return
            LOG.warning("Failed to delete Kubernetes node object name=%s status=%s error=%s", node_name, code, exc)

    def _prune_stale_kube_nodes_for_group(self, group: GroupConfig) -> None:
        vm_names = {vm.name for vm in self._group_vms(group)}
        try:
            response = self._kube_request("GET", "/api/v1/nodes")
        except Exception as exc:
            LOG.warning("Failed listing Kubernetes nodes for stale-node prune group=%s: %s", group.id, exc)
            return

        payload = response.json() if response.content else {}
        items = payload.get("items", []) if isinstance(payload, dict) else []
        for item in items:
            meta = item.get("metadata") or {}
            labels = meta.get("labels") or {}
            if labels.get("autoscaler.proxmox/group", "").strip() != group.id:
                continue
            node_name = str(meta.get("name") or "").strip()
            if not node_name:
                continue
            if node_name in vm_names:
                continue
            self._delete_kube_node(node_name)

    def _reconcile_loop(self) -> None:
        while True:
            try:
                for group in self.settings.groups.values():
                    self._prune_stale_kube_nodes_for_group(group)
            except Exception:
                LOG.exception("Background reconcile loop failed")
            time.sleep(20)

    def _pick_template_node_name(self, group: GroupConfig) -> str:
        response = self._kube_request("GET", "/api/v1/nodes")
        payload = response.json() if response.content else {}
        items = payload.get("items", []) if isinstance(payload, dict) else []

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

    def _template_node_payload(self, group: GroupConfig) -> dict[str, Any]:
        node_name = self._pick_template_node_name(group)
        base_labels: dict[str, str] = {}
        base_capacity: dict[str, str] = {}
        base_allocatable: dict[str, str] = {}
        if node_name:
            try:
                node_path = f"/api/v1/nodes/{urllib.parse.quote(node_name, safe='')}"
                node_resp = self._kube_request("GET", node_path)
                payload = node_resp.json() if node_resp.content else {}
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
            if parsed is None:
                continue
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
            "metadata": {
                "name": f"proxmox-ca-template-{group.id}",
                "labels": labels,
            },
            "spec": {
                "taints": taints,
            },
            "status": {
                "capacity": capacity,
                "allocatable": allocatable,
            },
        }

    def _template_node_bytes(self, group: GroupConfig) -> bytes:
        node_resp = self._kube_request(
            "POST",
            "/api/v1/nodes?dryRun=All",
            accept="application/vnd.kubernetes.protobuf",
            content_type="application/json",
            json_body=self._template_node_payload(group),
        )
        if not node_resp.content:
            raise RuntimeError(f"empty template node payload for group {group.id}")
        return unwrap_k8s_protobuf(node_resp.content)

    def NodeGroups(self, request: pb.NodeGroupsRequest, context: grpc.ServicerContext) -> pb.NodeGroupsResponse:
        groups = [
            pb.NodeGroup(
                id=g.id,
                minSize=g.min_size,
                maxSize=g.max_size,
                debug=f"group={g.id} prefix={g.vm_name_prefix}",
            )
            for g in self.settings.groups.values()
        ]
        return pb.NodeGroupsResponse(nodeGroups=groups)

    def NodeGroupForNode(
        self, request: pb.NodeGroupForNodeRequest, context: grpc.ServicerContext
    ) -> pb.NodeGroupForNodeResponse:
        label_group = (request.node.labels or {}).get("autoscaler.proxmox/group", "").strip()
        if label_group in self.settings.groups:
            g = self.settings.groups[label_group]
            return pb.NodeGroupForNodeResponse(nodeGroup=pb.NodeGroup(id=g.id, minSize=g.min_size, maxSize=g.max_size))

        for group in self.settings.groups.values():
            vm = self._find_vm_for_node(group, request.node)
            if vm is not None:
                return pb.NodeGroupForNodeResponse(
                    nodeGroup=pb.NodeGroup(id=group.id, minSize=group.min_size, maxSize=group.max_size)
                )
        return pb.NodeGroupForNodeResponse(nodeGroup=pb.NodeGroup(id=""))

    def GPULabel(self, request: pb.GPULabelRequest, context: grpc.ServicerContext) -> pb.GPULabelResponse:
        return pb.GPULabelResponse(label="")

    def GetAvailableGPUTypes(
        self,
        request: pb.GetAvailableGPUTypesRequest,
        context: grpc.ServicerContext,
    ) -> pb.GetAvailableGPUTypesResponse:
        return pb.GetAvailableGPUTypesResponse(gpuTypes={})

    def Cleanup(self, request: pb.CleanupRequest, context: grpc.ServicerContext) -> pb.CleanupResponse:
        return pb.CleanupResponse()

    def Refresh(self, request: pb.RefreshRequest, context: grpc.ServicerContext) -> pb.RefreshResponse:
        return pb.RefreshResponse()

    def NodeGroupTargetSize(
        self,
        request: pb.NodeGroupTargetSizeRequest,
        context: grpc.ServicerContext,
    ) -> pb.NodeGroupTargetSizeResponse:
        group = self._group(request.id, context)
        target_size = len(self._group_vms(group)) + self._pending_count(group.id)
        return pb.NodeGroupTargetSizeResponse(targetSize=target_size)

    def NodeGroupIncreaseSize(
        self,
        request: pb.NodeGroupIncreaseSizeRequest,
        context: grpc.ServicerContext,
    ) -> pb.NodeGroupIncreaseSizeResponse:
        group = self._group(request.id, context)
        delta = int(request.delta)
        if delta <= 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "delta must be > 0")

        current = len(self._group_vms(group)) + self._pending_count(group.id)
        if current + delta > group.max_size:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"scale would exceed max size for {group.id}: current={current} delta={delta} max={group.max_size}",
            )

        for _ in range(delta):
            self._enqueue_vm_create(group)
        return pb.NodeGroupIncreaseSizeResponse()

    def NodeGroupDeleteNodes(
        self,
        request: pb.NodeGroupDeleteNodesRequest,
        context: grpc.ServicerContext,
    ) -> pb.NodeGroupDeleteNodesResponse:
        group = self._group(request.id, context)
        if not request.nodes:
            return pb.NodeGroupDeleteNodesResponse()

        for node in request.nodes:
            vm = self._find_vm_for_node(group, node)
            if vm is None:
                context.abort(grpc.StatusCode.NOT_FOUND, f"node not in group {group.id}: {node.name} {node.providerID}")
            self.pve.stop_and_delete(vm.vmid)
            self._delete_kube_node((node.name or "").strip() or vm.name)
            LOG.info("Deleted VM vmid=%s name=%s group=%s", vm.vmid, vm.name, group.id)

        return pb.NodeGroupDeleteNodesResponse()

    def NodeGroupDecreaseTargetSize(
        self,
        request: pb.NodeGroupDecreaseTargetSizeRequest,
        context: grpc.ServicerContext,
    ) -> pb.NodeGroupDecreaseTargetSizeResponse:
        group = self._group(request.id, context)
        delta = int(request.delta)
        if delta >= 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "delta must be < 0")

        count = abs(delta)
        vms = self._group_vms(group)
        if count > len(vms):
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, f"cannot remove {count}; group has only {len(vms)}")

        for vm in sorted(vms, key=lambda x: x.vmid, reverse=True)[:count]:
            self.pve.stop_and_delete(vm.vmid)
            self._delete_kube_node(vm.name)
            LOG.info("Deleted VM vmid=%s name=%s group=%s (decrease target)", vm.vmid, vm.name, group.id)

        return pb.NodeGroupDecreaseTargetSizeResponse()

    def NodeGroupNodes(self, request: pb.NodeGroupNodesRequest, context: grpc.ServicerContext) -> pb.NodeGroupNodesResponse:
        group = self._group(request.id, context)
        self._prune_stale_kube_nodes_for_group(group)
        instances: list[pb.Instance] = []
        for vm in self._group_vms(group):
            state = pb.InstanceStatus.instanceRunning if vm.status == "running" else pb.InstanceStatus.unspecified
            instances.append(
                pb.Instance(
                    id=f"k3s://{vm.name}",
                    status=pb.InstanceStatus(instanceState=state),
                )
            )
        return pb.NodeGroupNodesResponse(instances=instances)

    def NodeGroupTemplateNodeInfo(
        self,
        request: pb.NodeGroupTemplateNodeInfoRequest,
        context: grpc.ServicerContext,
    ) -> pb.NodeGroupTemplateNodeInfoResponse:
        group = self._group(request.id, context)
        try:
            return pb.NodeGroupTemplateNodeInfoResponse(nodeBytes=self._template_node_bytes(group))
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else "n/a"
            context.abort(
                grpc.StatusCode.UNAVAILABLE,
                f"failed to fetch template node for group {group.id} (HTTP {status_code}): {exc}",
            )
        except Exception as exc:
            context.abort(grpc.StatusCode.UNAVAILABLE, f"failed to fetch template node for group {group.id}: {exc}")

    def NodeGroupGetOptions(
        self,
        request: pb.NodeGroupAutoscalingOptionsRequest,
        context: grpc.ServicerContext,
    ) -> pb.NodeGroupAutoscalingOptionsResponse:
        _ = self._group(request.id, context)
        return pb.NodeGroupAutoscalingOptionsResponse(nodeGroupAutoscalingOptions=request.defaults)

