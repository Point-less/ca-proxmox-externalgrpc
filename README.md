# Proxmox External gRPC Provider (Cluster Autoscaler)

Minimal Python implementation of Cluster Autoscaler's `externalgrpc` cloud provider.

## Model

- No CAPI / CAPMOX.
- No VM templates required for autoscaled nodes.
- Each scale-up creates a VM from a remote cloud image URL (`import-from` flow).
- Each VM gets a generated cloud-init ISO (`meta-data` + `user-data`) with k3s join logic.

## What it does

- Implements required gRPC methods used by Cluster Autoscaler:
  - `NodeGroups`
  - `NodeGroupForNode`
  - `NodeGroupTargetSize`
  - `NodeGroupIncreaseSize`
  - `NodeGroupDeleteNodes`
  - `NodeGroupDecreaseTargetSize`
  - `NodeGroupNodes`
  - `Refresh`, `Cleanup`
- Adds node identity labels during join:
  - `autoscaler.proxmox/group=<group-id>`
  - `autoscaler.proxmox/vmid=<vmid>`

## Build

```bash
docker build -t proxmox-ca-externalgrpc:dev .
```

The image build generates gRPC python files from `source/externalgrpc.proto`.

## Execution Policy

- NEVER run this service directly on the host machine.
- Run it only from the Docker image/container.

## Test (Container)

```bash
docker run --rm --entrypoint python proxmox-ca-externalgrpc:dev -m unittest discover -s /app/tests -v
```

## Code Layout

- `source/app/`: service entrypoints and gRPC servicer (`server.py`, `provider.py`)
- `source/services/`: orchestration and lifecycle services
- `source/core/`: domain contracts, models, errors, lifecycle state machine
- `source/infra/`: adapters, Proxmox client, settings loader, sqlite store, utility helpers
- `source/scripts/generate-proto.py`: protobuf generation script
- `source/tests/`: async architecture tests

Generated files `source/externalgrpc_pb2.py` and `source/externalgrpc_pb2_grpc.py` are intentionally not tracked in git.

## Config

Use `config.example.yaml` as a base.

Required inputs:
- Proxmox API token credentials
- `CLOUD_IMAGE_URL`
- k3s server URL + cluster token + SSH public key
- at least one `node_group`

Environment variables can override key settings:
- `PM_API_URL`, `PM_NODE`, `PM_SERVICE_TOKEN_ID`, `PM_SERVICE_TOKEN_SECRET`
- `PM_TLS_INSECURE`, `PM_VERIFY_CERTIFICATES`
- `IMPORT_STORAGE`, `ISO_STORAGE`, `VM_STORAGE`, `BRIDGE`, `CLOUD_IMAGE_URL`
- `K3S_VERSION`, `K3S_SERVER_URL`, `K3S_CLUSTER_TOKEN`, `SSH_PUBLIC_KEY`
- `PROVIDER_STATE_DB` (SQLite path; default `/tmp/proxmox-ca-externalgrpc-state.db`)
- `PENDING_VM_TIMEOUT_SECONDS` (max age for pending VMs before cleanup; default `900`)
- `RECONCILE_INTERVAL_SECONDS` (state reconcile interval; default `20`)

## Run

```bash
docker run --rm -p 50051:50051 \
  -v /path/to/provider-config.yaml:/config/provider-config.yaml:ro \
  proxmox-ca-externalgrpc:dev
```

## Notes

- This implementation is intentionally minimal.
- It uses VM tags to map nodes to autoscaler groups (`ca-group-<group-id>`).
- It uses SQLite-backed state for desired group size + VM lifecycle tracking.
- VM lifecycle is modeled with `python-statemachine` transitions (`pending`, `active`, `failed`, `deleting_vm`, `deleting_iso`, `deleting_node`).
- For reliable node mapping, keep hostnames unique and deterministic.
