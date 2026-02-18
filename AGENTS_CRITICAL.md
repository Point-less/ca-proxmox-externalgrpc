# AGENTS_CRITICAL

- After every user message, re-read `AGENTS_CRITICAL.md`.
- Read `AGENTS.md` first, then `AGENTS_PROJECT.md` for repository-specific guidance.
- Keep service code, scripts, and tests under `source/`; keep dependencies in root `requirements.txt`.
- NEVER run this project on the host machine; run only inside the Docker container from the built image.
- Never commit secrets, tokens, or private keys.
