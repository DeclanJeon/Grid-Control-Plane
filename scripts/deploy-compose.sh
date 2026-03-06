#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
ENV_EXAMPLE="$ROOT_DIR/.env.example"
ENV_FILE="$ROOT_DIR/.env"

random_value() {
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 16
    return
  fi

  if command -v python3 >/dev/null 2>&1; then
    python3 - <<'PY'
import secrets
print(secrets.token_hex(16))
PY
    return
  fi

  node -e "console.log(require('node:crypto').randomBytes(16).toString('hex'))"
}

replace_env_value() {
  local key="$1"
  local value="$2"
  python3 - "$ENV_FILE" "$key" "$value" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
key = sys.argv[2]
value = sys.argv[3]
lines = path.read_text(encoding='utf-8').splitlines()
prefix = f"{key}="
for index, line in enumerate(lines):
    if line.startswith(prefix):
        lines[index] = f"{prefix}{value}"
        break
else:
    lines.append(f"{prefix}{value}")
path.write_text("\n".join(lines) + "\n", encoding='utf-8')
PY
}

if [ ! -f "$ENV_FILE" ]; then
  cp "$ENV_EXAMPLE" "$ENV_FILE"

  auth_token=$(random_value)

  replace_env_value "CONTROL_AUTH_TOKEN" "$auth_token"
fi

cd "$ROOT_DIR"
docker compose up -d --build

auth_token=$(python3 - <<'PY'
from pathlib import Path
for line in Path('.env').read_text(encoding='utf-8').splitlines():
    if line.startswith('CONTROL_AUTH_TOKEN='):
        print(line.split('=', 1)[1])
        break
PY
)

for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:${CONTROL_PORT:-19090}/health" >/dev/null 2>&1; then
    readiness_json=$(curl -fsS \
      -H "Authorization: Bearer ${auth_token}" \
      -H "x-grid-role: admin" \
      "http://127.0.0.1:${CONTROL_PORT:-19090}/v1/admin/runtime/readiness")

    if python3 - "$readiness_json" <<'PY'
import json
import sys
payload = json.loads(sys.argv[1])
raise SystemExit(0 if payload.get('ready_for_production') is True else 1)
PY
    then
      printf 'Deployment ready\n'
      printf '%s\n' "$readiness_json"
      exit 0
    fi
  fi
  sleep 2
done

printf 'Deployment did not become ready in time\n' >&2
docker compose ps >&2 || true
exit 1
