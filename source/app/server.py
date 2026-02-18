#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import logging
import os
from pathlib import Path

import grpc

from infra.proto_stubs import pb_grpc
from infra.settings import load_settings
from .provider import CloudProvider


async def serve(*, config_path: Path, bind: str, port: int) -> None:
    settings = load_settings(config_path)
    provider = CloudProvider(settings)
    await provider.start()

    server = grpc.aio.server()
    pb_grpc.add_CloudProviderServicer_to_server(provider, server)
    server.add_insecure_port(f"{bind}:{port}")
    logging.getLogger("proxmox-ca-externalgrpc").info("Starting provider on %s:%s", bind, port)
    await server.start()
    try:
        await server.wait_for_termination()
    finally:
        await provider.stop()


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
    asyncio.run(serve(config_path=Path(args.config), bind=args.bind, port=args.port))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
