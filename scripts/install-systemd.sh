#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
TEMPLATE_FILE="$ROOT_DIR/systemd/grid-control-plane-compose.service"
TARGET_FILE="/etc/systemd/system/grid-control-plane-compose.service"
SERVICE_USER=${SERVICE_USER:-$(id -un)}
WORKING_DIRECTORY=${WORKING_DIRECTORY:-$ROOT_DIR}
PRINT_ONLY=0

while [ $# -gt 0 ]; do
  case "$1" in
    --print-only)
      PRINT_ONLY=1
      ;;
    --user)
      shift
      SERVICE_USER="$1"
      ;;
    --working-directory)
      shift
      WORKING_DIRECTORY="$1"
      ;;
    *)
      printf 'Unknown option: %s\n' "$1" >&2
      exit 1
      ;;
  esac
  shift
done

rendered_unit=$(python3 - "$TEMPLATE_FILE" "$SERVICE_USER" "$WORKING_DIRECTORY" <<'PY'
from pathlib import Path
import sys

template = Path(sys.argv[1]).read_text(encoding='utf-8')
service_user = sys.argv[2]
working_directory = sys.argv[3]

print(
    template
    .replace('__SERVICE_USER__', service_user)
    .replace('__WORKING_DIRECTORY__', working_directory)
)
PY
)

if [ "$PRINT_ONLY" = "1" ]; then
  printf '%s\n' "$rendered_unit"
  exit 0
fi

printf '%s\n' "$rendered_unit" | sudo tee "$TARGET_FILE" >/dev/null
sudo systemctl daemon-reload
sudo systemctl enable --now grid-control-plane-compose.service
sudo systemctl status --no-pager grid-control-plane-compose.service
