from __future__ import annotations

try:
    import externalgrpc_pb2 as pb
    import externalgrpc_pb2_grpc as pb_grpc
except ModuleNotFoundError as exc:  # pragma: no cover - startup guard
    raise SystemExit(
        "Missing generated gRPC stubs. Run `python scripts/generate-proto.py` before starting the provider."
    ) from exc

