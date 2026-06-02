# Stackblaze fork — `postgres-dba-mcp`

Fork of [crystaldba/postgres-mcp](https://github.com/crystaldba/postgres-mcp) (kept as the `upstream` remote — pull fixes with `git fetch upstream && git merge upstream/main`).

This is the **database-administration** MCP that kubero attaches per add-on when a Postgres (CloudNativePG) add-on is selected in the chat context. It is **day-2 DB admin only** — it never touches Kubernetes/the operator (deploy/scale/failover stay in the kubero DevOps workflow).

## What the fork changes

### Added discrete DBA tools (`src/postgres_mcp/server.py`)
Upstream exposes analysis tools + a single `execute_sql`. We add explicit, validated buttons so the agent doesn't hand-write DDL:

- **Read (always available):** `list_roles`, `list_databases`, `list_active_sessions`
- **Write (UNRESTRICTED mode only):** `create_role`, `drop_role`, `terminate_session`

Identifiers (role/db names) are strictly validated (`^[A-Za-z_][A-Za-z0-9_]{0,62}$`) before interpolation into DDL; pids and values use bind params. More to come (`grant`/`revoke`, `create_database` — needs autocommit, `alter_role_password`, `logical_backup`).

### What did NOT need a code change (driven by the kubero broker at spawn time)
- **Connection from context** — the broker resolves the selected add-on's connection (reusing kubero's `db-browser` resolver) and sets `DATABASE_URI` when it spawns this MCP. The MCP carries no hardcoded DB.
- **Phase-based safety** — the broker passes `--access-mode restricted` for `production` add-ons (read/analyse only — write tools aren't even registered) and `--access-mode unrestricted` for dev/review.
- **Transport** — `stdio` (works with both the port-forward-child and in-cluster-pod-exec bridges).

## How kubero spawns it (per selected add-on)
```
DATABASE_URI=postgres://<superuser>:<pw>@<host>:5432/<db> \
  postgres-dba-mcp --access-mode <restricted|unrestricted> --transport stdio
```
The broker re-exposes the tools namespaced (`pg__*`) and deferred, so they only enter the agent context while that add-on is selected.

## CI (mirrors `stackblaze/kubero-mcp`, self-hosted `kubero` runner)
The upstream DockerHub/`uv` test workflows are replaced with kubero-mcp's chain — all jobs `runs-on: [self-hosted, kubero]` (the private runner):

- **`auto-tag-on-main.yaml`** — every merge to `main` auto-increments `vX.Y.Z-sb.NN` (the fork's release line; the `-sb.NN` suffix never collides with upstream crystaldba tags merged in from `upstream`).
- **`docker-release.yaml`** — on a `v*.*.*` tag, build the multi-arch image, push to **`ghcr.io/stackblaze/postgres-dba-mcp:<tag>`**, and publish a GitHub Release.
- **`docker-pr.yaml`** — PR-only single-arch build (no push) to validate the Dockerfile.

**No `deploy.yaml`** (intentional divergence from kubero-mcp): this MCP is not a standing Deployment. The kubero broker pulls the GHCR image and spawns it on-demand (one short-lived container per selected add-on), so "release" just means "image published to GHCR".

**Seeding:** `auto-tag` needs a base `v*.*.*-sb.*` tag to count from — seeded once as `v0.3.0-sb.0` (matching the upstream `0.3.0` base in `pyproject.toml`). To bump the base later, push e.g. `v0.4.0-sb.0` manually once.
