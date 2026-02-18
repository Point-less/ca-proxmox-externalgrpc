from __future__ import annotations

import io
import logging
import os
import time
import urllib.parse
from pathlib import Path
from typing import Any

import requests
import urllib3

from models import ProxmoxConfig

LOG = logging.getLogger("proxmox-ca-externalgrpc")


class PveClient:
    def __init__(self, cfg: ProxmoxConfig, timeout_s: int = 120):
        self.cfg = cfg
        self.timeout_s = timeout_s
        self.s = requests.Session()
        if self.cfg.tls_insecure:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"PVEAPIToken={self.cfg.token_id}={self.cfg.token_secret}"}

    def json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> Any:
        base = self.cfg.api_url.rstrip("/")
        resp = self.s.request(
            method.upper(),
            f"{base}/api2/json{path}",
            headers=self._headers(),
            params=params or {},
            data=data or {},
            verify=not self.cfg.tls_insecure,
            timeout=self.timeout_s,
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("data")

    def upload(self, *, storage: str, filename: str, content: str, file_bytes: bytes) -> Any:
        files = {"filename": (filename, io.BytesIO(file_bytes), "application/octet-stream")}
        data = {"content": content}
        resp = self.s.post(
            f"{self.cfg.api_url.rstrip('/')}/api2/json/nodes/{self.cfg.node}/storage/{storage}/upload",
            headers=self._headers(),
            data=data,
            files=files,
            verify=not self.cfg.tls_insecure,
            timeout=600,
        )
        resp.raise_for_status()
        return resp.json().get("data")

    def wait_task(self, upid: str, timeout_s: int = 1800, poll_s: int = 2) -> None:
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            status = self.json("GET", f"/nodes/{self.cfg.node}/tasks/{urllib.parse.quote(upid, safe='')}/status")
            if isinstance(status, dict) and status.get("status") == "stopped":
                exitstatus = status.get("exitstatus")
                if exitstatus and exitstatus != "OK":
                    raise RuntimeError(f"Task failed: {upid} exitstatus={exitstatus}")
                return
            time.sleep(poll_s)
        raise TimeoutError(f"Timed out waiting for task: {upid}")

    def _wait_upid(self, upid: Any) -> None:
        if isinstance(upid, str) and upid.startswith("UPID:"):
            self.wait_task(upid)

    def nextid(self) -> int:
        return int(self.json("GET", "/cluster/nextid"))

    def list_vms(self) -> list[dict[str, Any]]:
        out = self.json("GET", f"/nodes/{self.cfg.node}/qemu")
        return out if isinstance(out, list) else []

    def vm_config(self, vmid: int) -> dict[str, Any]:
        out = self.json("GET", f"/nodes/{self.cfg.node}/qemu/{vmid}/config")
        return out if isinstance(out, dict) else {}

    def ensure_import_image(self) -> str:
        image_url = self.cfg.cloud_image_url
        filename = os.path.basename(urllib.parse.urlparse(image_url).path)
        if not filename:
            raise RuntimeError(f"Bad cloud image URL (no filename): {image_url}")
        if not filename.endswith(".qcow2"):
            filename = f"{Path(filename).stem}.qcow2"

        items = self.json("GET", f"/nodes/{self.cfg.node}/storage/{self.cfg.import_storage}/content")
        want = f"import/{filename}"
        for item in items if isinstance(items, list) else []:
            if str((item or {}).get("volid") or "").endswith(want):
                return filename

        LOG.info("Importing cloud image %s into %s", image_url, self.cfg.import_storage)
        upid = self.json(
            "POST",
            f"/nodes/{self.cfg.node}/storage/{self.cfg.import_storage}/download-url",
            data={
                "content": "import",
                "filename": filename,
                "url": image_url,
                "verify-certificates": "1" if self.cfg.verify_certificates else "0",
            },
        )
        self._wait_upid(upid)
        return filename

    def iso_exists(self, iso_name: str) -> bool:
        items = self.json("GET", f"/nodes/{self.cfg.node}/storage/{self.cfg.iso_storage}/content")
        want = f"iso/{iso_name}"
        for item in items if isinstance(items, list) else []:
            if str((item or {}).get("volid") or "").endswith(want):
                return True
        return False

    def create_vm_from_image(
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
        image_filename = self.ensure_import_image()

        scsi0 = (
            f"{self.cfg.vm_storage}:0,"
            f"import-from={self.cfg.import_storage}:import/{image_filename},"
            "discard=on"
        )
        upid = self.json(
            "POST",
            f"/nodes/{self.cfg.node}/qemu",
            data={
                "vmid": str(vmid),
                "name": name,
                "agent": "1",
                "memory": str(memory_mb),
                "cores": str(cores),
                "balloon": str(max(0, balloon_mb)),
                "net0": f"virtio,bridge={self.cfg.bridge}",
                "ipconfig0": "ip=dhcp",
                "scsihw": "virtio-scsi-pci",
                "serial0": "socket",
                "vga": "serial0",
                "ostype": "l26",
                "scsi0": scsi0,
                "boot": "order=scsi0",
                "tags": tags,
                "ide2": f"{self.cfg.iso_storage}:iso/{iso_name},media=cdrom",
            },
        )
        self._wait_upid(upid)

        if disk_size:
            try:
                resize_upid = self.json(
                    "PUT",
                    f"/nodes/{self.cfg.node}/qemu/{vmid}/resize",
                    data={"disk": "scsi0", "size": disk_size},
                )
                self._wait_upid(resize_upid)
            except requests.HTTPError:
                pass

        start_upid = self.json("POST", f"/nodes/{self.cfg.node}/qemu/{vmid}/status/start")
        self._wait_upid(start_upid)
        return vmid

    def stop_and_delete(self, vmid: int) -> None:
        try:
            stop_upid = self.json("POST", f"/nodes/{self.cfg.node}/qemu/{vmid}/status/stop")
            self._wait_upid(stop_upid)
        except requests.HTTPError:
            pass
        delete_upid = self.json(
            "DELETE",
            f"/nodes/{self.cfg.node}/qemu/{vmid}",
            params={"purge": "1", "destroy-unreferenced-disks": "1"},
        )
        self._wait_upid(delete_upid)

