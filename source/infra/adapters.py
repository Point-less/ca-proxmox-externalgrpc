from __future__ import annotations

import urllib.parse
import ssl
from pathlib import Path
from typing import Any

import httpx

from .pve import PveClient
from .utils import unwrap_k8s_protobuf

SA_TOKEN_PATH = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
SA_CA_CRT_PATH = Path("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
INCLUSTER_API_URL = "https://kubernetes.default.svc"


class AsyncProxmoxAdapter:
    def __init__(self, client: PveClient):
        self._client = client

    async def close(self) -> None:
        await self._client.close()

    async def list_vms(self) -> list[dict[str, Any]]:
        return await self._client.list_vms()

    async def vm_config(self, vmid: int) -> dict[str, Any]:
        return await self._client.vm_config(int(vmid))

    async def nextid(self) -> int:
        return await self._client.nextid()

    async def iso_exists(self, iso_name: str) -> bool:
        return await self._client.iso_exists(str(iso_name))

    async def upload(self, *, storage: str, filename: str, content: str, file_bytes: bytes) -> Any:
        return await self._client.upload(
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
        return await self._client.create_vm_from_image(
            vmid=vmid,
            name=name,
            cores=cores,
            memory_mb=memory_mb,
            balloon_mb=balloon_mb,
            disk_size=disk_size,
            tags=tags,
            iso_name=iso_name,
        )

    async def attached_seed_iso(self, vmid: int) -> tuple[str, str] | None:
        return await self._client.attached_seed_iso(int(vmid))

    async def stop_and_delete_vm(self, vmid: int) -> None:
        await self._client.stop_and_delete_vm(int(vmid))

    async def delete_storage_volume(self, storage: str, volume: str) -> None:
        await self._client.delete_storage_volume(str(storage), str(volume))


class AsyncKubernetesAdapter:
    def __init__(self, client: httpx.AsyncClient | None = None):
        self._client = client
        self._owns_client = client is None

    async def close(self) -> None:
        if self._client is None:
            return
        if self._owns_client:
            await self._client.aclose()
        self._client = None

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

    async def _request(
        self,
        method: str,
        path: str,
        *,
        accept: str = "application/json",
        content_type: str | None = None,
        data: Any | None = None,
        json_body: Any | None = None,
    ) -> httpx.Response:
        if not SA_CA_CRT_PATH.exists():
            raise RuntimeError(f"Missing service account CA certificate: {SA_CA_CRT_PATH}")
        if self._client is None:
            ssl_ctx = ssl.create_default_context(cafile=str(SA_CA_CRT_PATH))
            self._client = httpx.AsyncClient(timeout=20, verify=ssl_ctx)
        response = await self._client.request(
            method=method.upper(),
            url=f"{INCLUSTER_API_URL}{path}",
            headers=self._headers(accept=accept, content_type=content_type),
            data=data,
            json=json_body,
        )
        response.raise_for_status()
        return response

    async def list_nodes(self) -> list[dict[str, Any]]:
        response = await self._request("GET", "/api/v1/nodes")
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
            await self._request("DELETE", node_path)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return
            raise

    async def get_node(self, node_name: str) -> dict[str, Any]:
        if not node_name:
            return {}
        node_path = f"/api/v1/nodes/{urllib.parse.quote(node_name, safe='')}"
        response = await self._request("GET", node_path)
        payload = response.json() if response.content else {}
        return payload if isinstance(payload, dict) else {}

    async def build_template_node_bytes(self, payload: dict[str, Any]) -> bytes:
        response = await self._request(
            "POST",
            "/api/v1/nodes?dryRun=All",
            accept="application/vnd.kubernetes.protobuf",
            content_type="application/json",
            json_body=payload,
        )
        if not response.content:
            raise RuntimeError("empty template node payload")
        return unwrap_k8s_protobuf(response.content)
