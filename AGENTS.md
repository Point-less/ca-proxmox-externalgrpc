# Project Quick Reference

> [!IMPORTANT]
> Operational ritual: after **every** user message, first read `AGENTS_CRITICAL.md` from the host repo root.

## Toolbox Workflow

- Host commands limited to `docker`, `docker compose`, and `git`.
- Everything else runs in toolbox:
  `docker compose up -d && docker compose exec toolbox bash`

## Repo Layout

- Code: `./source` (mounted to `/app`)
- State/secrets: `./state` (mounted to `/state`)

## Core Ops

- Proxmox bootstrap: `python3 /app/ops/proxmox-bootstrap.py`
- Server VM up: `python3 /app/ops/proxmox-vm.py server`
- Agent VM up: `python3 /app/ops/proxmox-vm.py agent --count 1`
- Cleanup tagged VMs: `python3 /app/ops/proxmox-cleanup.py --dry-run` then `python3 /app/ops/proxmox-cleanup.py`

## Runbooks

Runbooks are under `runbooks/`.

Agent rule: open only the runbook(s) needed for the current task. Do not bulk-read `runbooks/`.

- Index: `runbooks/README.md`
- Toolbox: `runbooks/00-toolbox.md`
- Proxmox bootstrap: `runbooks/10-proxmox-bootstrap.md`
- Server: `runbooks/20-cluster-server.md`
- Agent: `runbooks/30-cluster-agent.md`
- Manifests: `runbooks/40-bootstrap-manifests.md`
- Autoscaler (externalgrpc): `runbooks/50-autoscaler-externalgrpc.md`
- Cleanup: `runbooks/90-cleanup.md`
