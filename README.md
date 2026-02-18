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

## Local Dev

Regenerate protobuf stubs:

```bash
python source/scripts/generate-proto.py
```

Run tests:

```bash
python -m unittest discover -s source/tests -v
```

## Code Layout

- `source/server.py`: thin entrypoint (arg parsing, logging, gRPC server bootstrap)
- `source/provider.py`: async CloudProvider gRPC implementation
- `source/orchestrator.py`: facade over scaling/reconcile/template services
- `source/group_context.py`: shared VM/group discovery and state helpers
- `source/scaling_service.py`: desired-size management and scale APIs
- `source/reconcile_service.py`: VM lifecycle reconciliation loop
- `source/template_service.py`: template-node payload generation
- `source/state_store.py`: SQLAlchemy-backed SQLite state repository
- `source/adapters.py`: async adapters for Proxmox and Kubernetes APIs
- `source/pve.py`: Proxmox API client and VM lifecycle operations
- `source/settings.py`: config loading + env override logic
- `source/seed.py`: cloud-init rendering and seed ISO creation
- `source/utils.py`: shared parsing/protobuf helpers
- `source/models.py`: dataclasses for settings and VM/group state
- `source/proto_stubs.py`: guarded import of generated protobuf modules
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
python -m source.server --config /config/provider-config.yaml --port 50051
```

## Notes

- This implementation is intentionally minimal.
- It uses VM tags to map nodes to autoscaler groups (`ca-group-<group-id>`).
- It uses SQLite-backed state for desired group size + VM lifecycle tracking.
- For reliable node mapping, keep hostnames unique and deterministic.
