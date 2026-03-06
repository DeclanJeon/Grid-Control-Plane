#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
PORT=${CONTROL_PORT:-19090}
FAILURES=0

pass() {
  printf 'OK: %s\n' "$1"
}

warn() {
  printf 'WARN: %s\n' "$1"
}

fail() {
  printf 'FAIL: %s\n' "$1" >&2
  FAILURES=$((FAILURES + 1))
}

require_cmd() {
  local cmd="$1"
  if command -v "$cmd" >/dev/null 2>&1; then
    pass "command '$cmd' available"
  else
    fail "command '$cmd' missing"
  fi
}

check_file() {
  local path="$1"
  if [ -e "$path" ]; then
    pass "found $(realpath "$path")"
  else
    fail "missing $path"
  fi
}

check_executable() {
  local path="$1"
  if [ -x "$path" ]; then
    pass "executable $path"
  else
    fail "not executable: $path"
  fi
}

check_port() {
  if command -v ss >/dev/null 2>&1; then
    if ss -ltn | grep -q ":${PORT} "; then
      warn "TCP port ${PORT} is already listening; deploy may fail or replace an existing service"
    else
      pass "TCP port ${PORT} appears free"
    fi
    return
  fi

  if command -v netstat >/dev/null 2>&1; then
    if netstat -ltn 2>/dev/null | grep -q ":${PORT}[[:space:]]"; then
      warn "TCP port ${PORT} is already listening; deploy may fail or replace an existing service"
    else
      pass "TCP port ${PORT} appears free"
    fi
    return
  fi

  warn "port check skipped because neither ss nor netstat is available"
}

check_docker_access() {
  if docker info >/dev/null 2>&1; then
    pass "docker daemon reachable"
  else
    fail "docker daemon is not reachable for the current user"
  fi
}

check_systemd() {
  if command -v systemctl >/dev/null 2>&1; then
    pass "systemctl available"
  else
    warn "systemctl not available; auto-start setup will be unavailable"
  fi
}

cd "$ROOT_DIR"

require_cmd docker
require_cmd python3
require_cmd curl
check_docker_access

if docker compose version >/dev/null 2>&1; then
  pass "docker compose available"
else
  fail "docker compose plugin not available"
fi

check_systemd
check_file "$ROOT_DIR/.env.example"
check_file "$ROOT_DIR/docker-compose.yml"
check_file "$ROOT_DIR/Dockerfile"
check_file "$ROOT_DIR/systemd/grid-control-plane-compose.service"
check_executable "$ROOT_DIR/scripts/deploy-compose.sh"
check_executable "$ROOT_DIR/scripts/compose-down.sh"
check_executable "$ROOT_DIR/scripts/install-systemd.sh"
check_port

if [ -f "$ROOT_DIR/.env" ]; then
  pass "existing .env found"
  if [ "$(stat -c '%a' "$ROOT_DIR/.env" 2>/dev/null || printf 'unknown')" = "600" ]; then
    pass ".env permissions are 600"
  else
    warn ".env permissions are not 600"
  fi
else
  warn ".env not found yet; deploy script will create it on first run"
fi

if [ "$FAILURES" -gt 0 ]; then
  printf 'Preflight failed with %s blocking issue(s).\n' "$FAILURES" >&2
  exit 1
fi

printf 'Preflight passed with no blocking issues.\n'
