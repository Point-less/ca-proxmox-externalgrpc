#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os
from concurrent import futures
from pathlib import Path

import grpc

from .proto_stubs import pb_grpc
from .provider import CloudProvider
from .settings import load_settings


def serve(*, config_path: Path, bind: str, port: int) -> None:
    settings = load_settings(config_path)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    pb_grpc.add_CloudProviderServicer_to_server(CloudProvider(settings), server)
    server.add_insecure_port(f"{bind}:{port}")
    logging.getLogger("proxmox-ca-externalgrpc").info("Starting provider on %s:%s", bind, port)
    server.start()
    server.wait_for_termination()


def main() -> int:
    parser = argparse.ArgumentParser(description="Proxmox externalgrpc cloud provider for Cluster Autoscaler")
    parser.add_argument("--config", default=os.getenv("PROVIDER_CONFIG", "/config/provider-config.yaml"))
    parser.add_argument("--bind", default=os.getenv("BIND_ADDRESS", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "50051")))
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    serve(config_path=Path(args.config), bind=args.bind, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
