# Project-Specific Notes

This repository is the Proxmox external gRPC provider for Cluster Autoscaler.

## Layout

- Service code: `source/`
- Proto generation script: `source/scripts/generate-proto.py`
- Unit tests: `source/tests/`
- Dependencies: `requirements.txt` (repo root)

## Common Commands

- Generate protobuf stubs: `python3 source/scripts/generate-proto.py`
- Run tests: `python3 -m unittest discover -s source/tests -v`
- Build image: `docker build -t proxmox-ca-externalgrpc:dev .`
- Run server: `python3 -m source.server --config /config/provider-config.yaml --port 50051`

## Container Build Rules

- Use Alpine Python base image.
- Copy only required files into the image:
  - `requirements.txt`
  - `source/`
