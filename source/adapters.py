from __future__ import annotations

import asyncio
import urllib.parse
from pathlib import Path
from typing import Any

import requests

from .pve import PveClient
from .utils import unwrap_k8s_protobuf

SA_TOKEN_PATH = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
SA_CA_CRT_PATH = Path("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
INCLUSTER_API_URL = "https://kubernetes.default.svc"


class AsyncProxmoxAdapter:
    def __init__(self, client: PveClient):
        self._client = client

    async def list_vms(self) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._client.list_vms)

    async def vm_config(self, vmid: int) -> dict[str, Any]:
        return await asyncio.to_thread(self._client.vm_config, int(vmid))

    async def nextid(self) -> int:
        return await asyncio.to_thread(self._client.nextid)

    async def iso_exists(self, iso_name: str) -> bool:
        return await asyncio.to_thread(self._client.iso_exists, str(iso_name))

    async def upload(self, *, storage: str, filename: str, content: str, file_bytes: bytes) -> Any:
        return await asyncio.to_thread(
            self._client.upload,
            storage=storage,
            filename=filename,
            content=content,
            file_bytes=file_bytes,
        )

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
        return await asyncio.to_thread(
            self._client.create_vm_from_image,
            vmid=vmid,
            name=name,
            cores=cores,
            memory_mb=memory_mb,
            balloon_mb=balloon_mb,
            disk_size=disk_size,
            tags=tags,
            iso_name=iso_name,
        )

    async def stop_and_delete(self, vmid: int) -> None:
        await asyncio.to_thread(self._client.stop_and_delete, int(vmid))


class AsyncKubernetesAdapter:
    def __init__(self, session: requests.Session | None = None):
        self._session = session or requests.Session()

    def _headers(self, *, accept: str, content_type: str | None = None) -> dict[str, str]:
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

    def _request_sync(
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
        resp = self._session.request(
            method=method.upper(),
            url=f"{INCLUSTER_API_URL}{path}",
            headers=self._headers(accept=accept, content_type=content_type),
            data=data,
            json=json_body,
            verify=str(SA_CA_CRT_PATH),
            timeout=20,
        )
        resp.raise_for_status()
        return resp

    async def list_nodes(self) -> list[dict[str, Any]]:
        response = await asyncio.to_thread(self._request_sync, "GET", "/api/v1/nodes")
        payload = response.json() if response.content else {}
        if not isinstance(payload, dict):
            return []
        items = payload.get("items", [])
        return items if isinstance(items, list) else []

    async def delete_node(self, node_name: str) -> None:
        if not node_name:
            return
        node_path = f"/api/v1/nodes/{urllib.parse.quote(node_name, safe='')}"
        try:
            await asyncio.to_thread(self._request_sync, "DELETE", node_path)
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else None
            if code == 404:
                return
            raise

    async def get_node(self, node_name: str) -> dict[str, Any]:
        if not node_name:
            return {}
        node_path = f"/api/v1/nodes/{urllib.parse.quote(node_name, safe='')}"
        response = await asyncio.to_thread(self._request_sync, "GET", node_path)
        payload = response.json() if response.content else {}
        return payload if isinstance(payload, dict) else {}

    async def build_template_node_bytes(self, payload: dict[str, Any]) -> bytes:
        response = await asyncio.to_thread(
            self._request_sync,
            "POST",
            "/api/v1/nodes?dryRun=All",
            accept="application/vnd.kubernetes.protobuf",
            content_type="application/json",
            json_body=payload,
        )
        if not response.content:
            raise RuntimeError("empty template node payload")
        return unwrap_k8s_protobuf(response.content)
