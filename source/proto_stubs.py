from __future__ import annotations

import sys
from pathlib import Path

MODULE_DIR = Path(__file__).resolve().parent
if str(MODULE_DIR) not in sys.path:
    sys.path.insert(0, str(MODULE_DIR))

try:
    import externalgrpc_pb2 as pb
    import externalgrpc_pb2_grpc as pb_grpc
except ModuleNotFoundError as exc:  # pragma: no cover - startup guard
    raise SystemExit(
        "Missing generated gRPC stubs. Run `python source/scripts/generate-proto.py` before starting the provider."
    ) from exc
