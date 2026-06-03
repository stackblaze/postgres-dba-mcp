# Stackblaze fork — `postgres-dba-mcp`

Fork of [crystaldba/postgres-mcp](https://github.com/crystaldba/postgres-mcp) (kept as the `upstream` remote — pull fixes with `git fetch upstream && git merge upstream/main`).

This is the **database-administration** MCP the kubero chat attaches when a Postgres (CloudNativePG) add-on is selected in the chat context. It is **day-2 DB admin only** — it never touches Kubernetes/the operator (deploy/scale/failover stay in the kubero DevOps workflow).

It runs as **one long-lived per-cluster Deployment** that serves **every** Postgres add-on in that cluster — the connection target arrives **per request**, not at boot (see connection-from-request below). The kubero **server** is the broker: on add-on selection it resolves the connection, picks the access mode by phase, port-forwards to this Service over the workload cluster's apiserver, and merges the tools into the chat namespaced as `pg__*` (only while that add-on is selected).

## What the fork changes

### Added discrete DBA tools (`src/postgres_mcp/server.py`)
Upstream exposes analysis tools + a single `execute_sql`. We add explicit, validated buttons so the agent doesn't hand-write DDL:

- **Read:** `list_roles`, `list_databases`, `list_active_sessions`
- **Write (refused under restricted access):** `create_role`, `drop_role`, `terminate_session`

Identifiers (role/db names) are strictly validated (`^[A-Za-z_][A-Za-z0-9_]{0,62}$`) before interpolation into DDL; pids and values use bind params. More to come (`grant`/`revoke`, `create_database` — needs autocommit, `alter_role_password`, `logical_backup`).

### Connection-from-request (`--connection-from-request`)
So one instance can serve many add-on DBs, the target is read **per request from HTTP headers** instead of a boot `DATABASE_URI`:

- `X-Kubero-DB-URI` → the Postgres URI; a per-URI `psycopg` pool is created lazily and idle-evicted (`_ConnRegistry`).
- `X-Kubero-Access-Mode` → `restricted` (read/analyse only — `SafeSqlDriver`, and the write tools refuse) or `unrestricted`. **Missing/invalid ⇒ restricted** (fail safe — a production add-on never silently gets write access).

`get_sql_driver()` resolves the pool + mode from the request via the lowlevel `request_ctx` contextvar, so no per-tool `Context` plumbing is needed. Requires an HTTP transport (`streamable-http`). Without the flag the server keeps upstream's single-DB behaviour (boot `DATABASE_URI`, write tools registered only in unrestricted mode) — handy for local/standalone use.

## How kubero runs it
Deployed by `deploy/` + `deploy.yaml` as a ClusterIP Service on each workload cluster:
```
postgres-mcp --transport streamable-http --connection-from-request \
  --streamable-http-host 0.0.0.0 --streamable-http-port 3010
```
The broker (kubero server) sends each request with `X-Kubero-DB-URI` + `X-Kubero-Access-Mode` over a port-forward; never exposed publicly.

## CI (mirrors `stackblaze/kubero-mcp`, self-hosted `kubero` runner)
The upstream DockerHub/`uv` test workflows are replaced with kubero-mcp's chain — all jobs `runs-on: [self-hosted, kubero]` (the private runner):

- **`auto-tag-on-main.yaml`** — every merge to `main` auto-increments `vX.Y.Z-sb.NN` (the fork's release line; the `-sb.NN` suffix never collides with upstream crystaldba tags merged in from `upstream`).
- **`docker-release.yaml`** — on a `v*.*.*` tag, build the amd64 image, push to **`ghcr.io/stackblaze/postgres-dba-mcp:<tag>`**, and publish a GitHub Release.
- **`docker-pr.yaml`** — PR-only build (no push) to validate the Dockerfile.
- **`deploy.yaml`** — on the published Release, roll the long-lived Deployment on the workload cluster(s) (`KUBECONFIG_B64_<region>`). ClusterIP only; no ingress (the broker reaches it via apiserver port-forward).

`CHAIN_PAT` (repo secret) lets auto-tag's tag auto-fire docker-release and the Release fire deploy.

**Seeding:** `auto-tag` needs a base `v*.*.*-sb.*` tag to count from — seeded once as `v0.3.0-sb.0` (matching the upstream `0.3.0` base in `pyproject.toml`). To bump the base later, push e.g. `v0.4.0-sb.0` manually once.
