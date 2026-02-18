# AGENTS_CRITICAL

- Host: use only `docker`/`docker compose`/`git`.
- App/tool commands: run inside the toolbox via `docker compose exec toolbox ...`.
- Source is bind-mounted at `/app`; state/secrets at `/state`.
- Do not commit secrets; keep them under `state/` or local `source/.env`.

