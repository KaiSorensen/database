#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMMAND_FILE="${1:-$ROOT_DIR/repl_bulk_commands_chunked.txt}"

if [[ ! -f "$COMMAND_FILE" ]]; then
  echo "Command file not found: $COMMAND_FILE" >&2
  exit 1
fi

cd "$ROOT_DIR"

# Preload the REPL with a batch of commands, then continue forwarding live stdin.
{
  cat "$COMMAND_FILE"
  cat
} | bash ./main.sh
