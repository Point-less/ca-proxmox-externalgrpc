# Project-Specific Notes

This repository is the Proxmox external gRPC provider for Cluster Autoscaler.

## Execution Policy

- NEVER execute this project directly on the host machine.
- All runtime, tests, and operational checks must run inside the Docker container built from this repository.

## Layout

- Service code: `source/`
- App layer: `source/app/`
- Domain/core: `source/core/`
- Infra/integrations: `source/infra/`
- Service orchestration: `source/services/`
- Proto generation script: `source/scripts/generate-proto.py`
- Unit tests: `source/tests/`
- Dependencies: `requirements.txt` (repo root)

## Common Commands

- Build image: `docker build -t proxmox-ca-externalgrpc:dev .`
- Run tests (in container): `docker run --rm --entrypoint python proxmox-ca-externalgrpc:dev -m unittest discover -s /app/tests -v`
- Run server (in container): `docker run --rm -p 50051:50051 -v /path/to/provider-config.yaml:/config/provider-config.yaml:ro proxmox-ca-externalgrpc:dev`

## Container Build Rules

- Use Alpine Python base image.
- Copy only required files into the image:
  - `requirements.txt`
  - `source/`
