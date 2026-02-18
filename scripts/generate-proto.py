#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path

import grpc_tools
from grpc_tools import protoc


def main() -> int:
    base_dir = Path(__file__).resolve().parents[1]
    include_dir = Path(grpc_tools.__file__).resolve().parent / "_proto"
    proto = base_dir / "externalgrpc.proto"
    if not proto.exists():
        raise SystemExit(f"missing proto file: {proto}")

    rc = protoc.main(
        [
            "grpc_tools.protoc",
            f"-I{base_dir}",
            f"-I{include_dir}",
            f"--python_out={base_dir}",
            f"--grpc_python_out={base_dir}",
            str(proto),
        ]
    )
    if rc != 0:
        raise SystemExit(rc)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
