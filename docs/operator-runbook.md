# Grid Control Plane Operator Runbook

## Purpose

This guide is the practical execution handbook for running `Grid Control Plane` on an operator server.

Use it when you want to:

- deploy from GitHub with `git clone`
- verify server readiness before startup
- start the full stack with Docker Compose
- verify that the service is production-ready
- enable automatic restart after reboot
- stop, inspect, or recover the service stack

## Target Environment

Recommended baseline:

- Linux server
- Docker installed and daemon reachable by the operator user
- Docker Compose plugin available
- `python3`, `curl`, `systemctl` available

## Quick Start

```bash
git clone https://github.com/DeclanJeon/Grid-Control-Plane.git
cd Grid-Control-Plane
./scripts/preflight.sh
./scripts/deploy-compose.sh
```

If deployment succeeds, the service should reach `ready_for_production: true`.

## Step 1. Clone the Repository

```bash
git clone https://github.com/DeclanJeon/Grid-Control-Plane.git
cd Grid-Control-Plane
```

## Step 2. Run Server Preflight

```bash
./scripts/preflight.sh
```

Expected result:

- no blocking failures
- Docker and Compose available
- deployment scripts executable
- target port not unexpectedly occupied

If preflight fails, fix the reported issue before continuing.

## Step 3. Bootstrap and Start the Stack

```bash
./scripts/deploy-compose.sh
```

What happens on first run:

- `.env` is created from `.env.example`
- `CONTROL_AUTH_TOKEN` is randomized
- if this is a truly fresh Postgres volume, `POSTGRES_PASSWORD` is randomized
- `CONTROL_PG_DSN` is rewritten to match the generated DB password
- `.env` permissions are set to `600`
- the compose stack is built and started
- the script waits until the readiness endpoint returns `ready_for_production: true`

## Step 4. Verify Service Health

Load the auth token from `.env` and check the service:

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

Expected readiness result:

- `ready_for_production: true`
- `blocker_count: 0`

## Step 5. Enable Auto-Start After Reboot

```bash
./scripts/install-systemd.sh
```

This installs and enables `grid-control-plane-compose.service`.

Check status:

```bash
sudo systemctl status grid-control-plane-compose.service
sudo journalctl -u grid-control-plane-compose.service -n 200 --no-pager
```

Preview only:

```bash
./scripts/install-systemd.sh --print-only
```

## Common Operations

### Stop the stack

```bash
./scripts/compose-down.sh
```

### Stop and remove volumes

```bash
./scripts/compose-down.sh -v
```

### Re-run deployment after a code update

```bash
git pull
./scripts/preflight.sh
./scripts/deploy-compose.sh
```

### Inspect generated configuration

```bash
cat .env
ls -l .env
```

## Failure Handling

### Case A. Preflight fails

Typical causes:

- Docker daemon not reachable
- Docker Compose missing
- required files absent
- target port already in use

Action:

1. fix the reported issue
2. rerun `./scripts/preflight.sh`
3. only continue after the script passes

### Case B. Deploy script refuses to start because of default DB credentials

This means:

- an existing Postgres volume already exists
- `.env` still contains the template DB password

Recommended action:

1. rotate `POSTGRES_PASSWORD`
2. update `CONTROL_PG_DSN` to match
3. rerun `./scripts/deploy-compose.sh`

Override only if you intentionally accept the risk:

```bash
ALLOW_INSECURE_DEFAULT_DB_CREDS=1 ./scripts/deploy-compose.sh
```

### Case C. Stack starts but readiness is not true

Check:

```bash
docker compose ps
docker compose logs control-plane --tail=200
docker compose logs postgres --tail=200
docker compose logs redis --tail=200
```

Look for:

- Postgres authentication failures
- Redis connection failures
- port binding conflicts
- readiness blockers returned by `/v1/admin/runtime/readiness`

### Case D. Auto-start does not work after reboot

Check:

```bash
sudo systemctl status grid-control-plane-compose.service
sudo journalctl -u grid-control-plane-compose.service -b --no-pager
```

Confirm:

- Docker starts on boot
- the checkout path still exists
- the service user still has Docker access

## Recommended Deployment Rehearsal

Before switching to live operator use, run this exact rehearsal:

```bash
git clone https://github.com/DeclanJeon/Grid-Control-Plane.git
cd Grid-Control-Plane
./scripts/preflight.sh
./scripts/deploy-compose.sh
./scripts/install-systemd.sh --print-only
```

Successful rehearsal criteria:

- preflight passes with no blocking issues
- deploy completes successfully
- readiness reports `ready_for_production: true`
- systemd unit renders cleanly

## Handoff to Two-Laptop Test

Once the operator server is live and healthy:

1. keep the stack running
2. retain the generated `CONTROL_AUTH_TOKEN`
3. verify network reachability to the operator host
4. point the desktop clients at the operator-hosted control-plane
5. begin the two-laptop connection and transfer scenarios
