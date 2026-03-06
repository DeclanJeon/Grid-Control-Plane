# Grid Control Plane

Operator-side control-plane service for presence, trust, sessions, ledger rollups, and backend readiness in the Broker Peer Grid.

## Project Title

- `Grid Control Plane`

## Project Description

- `Operator-side control-plane service for presence, trust, sessions, ledger rollups, and backend readiness in the Broker Peer Grid.`

## What It Does

`apps/control-plane` is the central operator service that sits beside the desktop clients and supports multi-node coordination.

It provides:

- presence registration and snapshot APIs
- file registration and listing APIs
- session creation and recovery-state tracking
- trust attestation and file trust scoring
- ledger event ingestion and rollup summaries
- backend status and production-readiness inspection

In the current release model, this service is deployed separately from the desktop app.

## Key Documents

- deployment readiness report: `docs/deployment-readiness-report.md`
- operator runbook: `docs/operator-runbook.md`

## Runtime Model

The service starts as a Node.js HTTP server from `server.mjs`.

Default local mode:

- storage: `file`
- queue: `inline`
- host: `127.0.0.1`
- port: `19090`

Production-style mode:

- storage: `postgres`
- queue: `redis_bullmq`

## API Highlights

Health and admin:

- `GET /health`
- `GET /v1/admin/runtime/backends`
- `GET /v1/admin/runtime/readiness`
- `GET /v1/admin/ledger/rollups`
- `GET /v1/admin/audit`

Presence and transfer coordination:

- `POST /v1/presence/sync`
- `GET /v1/presence/snapshot`
- `POST /v1/files/register`
- `GET /v1/files`
- `POST /v1/sessions/create`
- `GET /v1/sessions`

Trust and ledger:

- `POST /v1/trust/attest`
- `GET /v1/trust/file/:file_id`
- `POST /v1/ledger/events`
- `POST /v1/ledger/events/batch`
- `GET /v1/ledger/summary`

## Local Development

Install and run:

```bash
cd apps/control-plane
npm install
CONTROL_AUTH_TOKEN=dev-local-token npm run start
```

Health check:

```bash
curl http://127.0.0.1:19090/health
```

Readiness check:

```bash
curl \
  -H "Authorization: Bearer dev-local-token" \
  -H "x-grid-role: admin" \
  http://127.0.0.1:19090/v1/admin/runtime/readiness
```

Expected local result:

- server is healthy
- `ready_for_production` may still be `false` in file/inline mode

## Operator Deployment

For an operator server, use externalized backends.

```bash
cd apps/control-plane
npm install

export CONTROL_HOST=0.0.0.0
export CONTROL_PORT=19090
export CONTROL_AUTH_TOKEN=replace-with-strong-token
export CONTROL_STORAGE_BACKEND=postgres
export CONTROL_PG_DSN='postgres://USER:PASSWORD@POSTGRES_HOST:5432/DBNAME'
export CONTROL_PG_TABLE=control_plane_state
export CONTROL_QUEUE_BACKEND=redis_bullmq
export CONTROL_REDIS_URL='redis://REDIS_HOST:6379'
export CONTROL_BULLMQ_QUEUE=grid-ledger-events

npm run start
```

Recommended operator verification:

```bash
curl http://SERVER_IP:19090/health
curl \
  -H "Authorization: Bearer replace-with-strong-token" \
  -H "x-grid-role: admin" \
  http://SERVER_IP:19090/v1/admin/runtime/readiness
```

Expected production-style result:

- `ready_for_production: true`
- no `storage_not_externalized`
- no `queue_not_externalized`

## CI

GitHub Actions workflow:

- `.github/workflows/ci.yml`

It runs:

- `npm ci`
- `npm test`

on both pushes to `main` and pull requests.

## Docker Deployment

This repository includes a production-oriented `Dockerfile` for the control-plane service.

Build the image:

```bash
docker build -t grid-control-plane:latest .
```

Run the container:

```bash
docker run --rm -p 19090:19090 \
  -e CONTROL_AUTH_TOKEN=replace-with-strong-token \
  -e CONTROL_STORAGE_BACKEND=postgres \
  -e CONTROL_PG_DSN='postgres://USER:PASSWORD@POSTGRES_HOST:5432/DBNAME' \
  -e CONTROL_QUEUE_BACKEND=redis_bullmq \
  -e CONTROL_REDIS_URL='redis://REDIS_HOST:6379' \
  grid-control-plane:latest
```

Use `.env.example` as the baseline for the runtime environment passed into the container.

## Docker Compose Deployment

This repository also includes `docker-compose.yml` for a full local operator stack:

- `control-plane`
- `postgres`
- `redis`

Quick start:

```bash
# optional: customize values first
cp .env.example .env
docker compose up -d --build
```

If `.env` is not present, `docker-compose.yml` still works with its built-in defaults.

## Git Clone to Run

For an operator server, the shortest path is:

```bash
git clone https://github.com/DeclanJeon/Grid-Control-Plane.git
cd Grid-Control-Plane
./scripts/preflight.sh
./scripts/deploy-compose.sh
```

What the script does:

- creates `.env` from `.env.example` on first run
- generates a random `CONTROL_AUTH_TOKEN`
- generates a random `POSTGRES_PASSWORD` and matching `CONTROL_PG_DSN` on a truly fresh bootstrap
- runs `docker compose up -d --build`
- waits for `/health` and `/v1/admin/runtime/readiness`
- exits only when `ready_for_production: true`

If an existing PostgreSQL volume is already present, the script keeps the database credentials in `.env` compatible with that volume and prints a warning if the template password is still in use.

The script also locks down `.env` permissions with `chmod 600`.

If an existing PostgreSQL volume is present and `.env` still uses the template database password, the script now fails closed by default. To intentionally continue with those credentials, set:

```bash
ALLOW_INSECURE_DEFAULT_DB_CREDS=1 ./scripts/deploy-compose.sh
```

Useful follow-up commands:

```bash
# stop the stack
./scripts/compose-down.sh

# stop and remove volumes
./scripts/compose-down.sh -v

# inspect generated secrets
cat .env
```

## Server Preflight

Run this before the first deployment on an operator server:

```bash
./scripts/preflight.sh
```

It checks:

- Docker CLI and Docker daemon access
- Docker Compose availability
- `python3`, `curl`, and `systemctl`
- required deployment files and executable scripts
- whether the target TCP port is already in use
- whether an existing `.env` file has secure permissions

## Deployment Rehearsal

Recommended rehearsal on a new server:

```bash
git clone https://github.com/DeclanJeon/Grid-Control-Plane.git
cd Grid-Control-Plane
./scripts/preflight.sh
./scripts/deploy-compose.sh

CONTROL_AUTH_TOKEN=$(python3 - <<'PY'
from pathlib import Path
for line in Path('.env').read_text(encoding='utf-8').splitlines():
    if line.startswith('CONTROL_AUTH_TOKEN='):
        print(line.split('=', 1)[1])
        break
PY
)

curl http://127.0.0.1:19090/health
curl \
  -H "Authorization: Bearer ${CONTROL_AUTH_TOKEN}" \
  -H "x-grid-role: admin" \
  http://127.0.0.1:19090/v1/admin/runtime/readiness
./scripts/install-systemd.sh --print-only
```

Expected rehearsal result:

- preflight reports no blocking issues
- deployment reaches `ready_for_production: true`
- systemd unit renders cleanly

Verify the stack:

```bash
CONTROL_AUTH_TOKEN=$(python3 - <<'PY'
from pathlib import Path
for line in Path('.env').read_text(encoding='utf-8').splitlines():
    if line.startswith('CONTROL_AUTH_TOKEN='):
        print(line.split('=', 1)[1])
        break
PY
)

curl http://127.0.0.1:19090/health
curl \
  -H "Authorization: Bearer ${CONTROL_AUTH_TOKEN}" \
  -H "x-grid-role: admin" \
  http://127.0.0.1:19090/v1/admin/runtime/readiness
```

Stop the stack:

```bash
docker compose down
```

Remove the stack with persisted volumes:

```bash
docker compose down -v
```

## Systemd Auto-Start

If you want the stack to restart automatically after server reboot:

```bash
./scripts/install-systemd.sh
```

This installs `grid-control-plane-compose.service` into `/etc/systemd/system/`, enables it, and starts it immediately.

Useful options:

```bash
# preview the rendered unit without installing it
./scripts/install-systemd.sh --print-only

# install for a different service user or checkout path
SERVICE_USER=ubuntu WORKING_DIRECTORY=/opt/Grid-Control-Plane ./scripts/install-systemd.sh
```

Manual systemd checks:

```bash
sudo systemctl status grid-control-plane-compose.service
sudo journalctl -u grid-control-plane-compose.service -n 200 --no-pager
```

## Environment Variables

Use `.env.example` as the starting template for operator deployment.

Core settings:

- `CONTROL_HOST`
- `CONTROL_PORT`
- `CONTROL_AUTH_TOKEN`
- `CONTROL_DATA_FILE`

Storage backend:

- `CONTROL_STORAGE_BACKEND`
- `CONTROL_PG_DSN`
- `CONTROL_PG_TABLE`

Queue backend:

- `CONTROL_QUEUE_BACKEND`
- `CONTROL_QUEUE_JOURNAL_FILE`
- `CONTROL_REDIS_URL`
- `CONTROL_BULLMQ_QUEUE`
- `CONTROL_BULLMQ_WORKER_CONCURRENCY`

Tuning:

- `PRESENCE_TTL_SECONDS`
- `RATE_LIMIT_WINDOW_MS`
- `RATE_LIMIT_MAX`
- `LEDGER_DECAY_ALPHA`
- `MESSAGE_COOLDOWN_MS`
- `SESSION_PATH_EMA_ALPHA`

## Test Commands

Basic tests:

```bash
npm test
```

Real-backend verification examples:

```bash
npm run verify:real-backends
npm run verify:real-backends:load
npm run verify:real-backends:faults
npm run verify:real-backends:wan
npm run verify:real-backends:wan-loss
npm run verify:real-backends:queue-state
npm run verify:resilience:marathon
```

## Current Deployment Position

- desktop app users install a single Tauri app
- operators separately deploy `Grid Control Plane`
- two-laptop testing should connect both desktops to this service running on the operator server

## Related Documents

- `../../report/2603061720_integrated_test_plan_cases_scenarios.md`
- `../../report/2603061805_local_test_execution_report.md`
- `../../report/2603061823_release_composition_and_deployment_runbook.md`
