#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$ROOT_DIR/scripts/validation/lib.sh"

main() {
  export_default_env
  require_bin cargo

  # Prefer release binary if present, else debug
  if command -v cqlite >/dev/null 2>&1; then
    ok "Using cqlite from PATH"
  elif [[ -x "$ROOT_DIR/target/release/cqlite" ]]; then
    export PATH="$ROOT_DIR/target/release:$PATH"
  elif [[ -x "$ROOT_DIR/target/debug/cqlite" ]]; then
    export PATH="$ROOT_DIR/target/debug:$PATH"
  else
    warn "cqlite not found, attempting quick build"
    "$ROOT_DIR/scripts/validation/build_cqlite.sh" >/dev/null
    export PATH="$ROOT_DIR/target/debug:$PATH"
  fi

  require_bin cqlite

  # Help and version
  run "cqlite --help >/dev/null"
  run "cqlite --version >/dev/null"

  # One-shot basic query (table)
  run "cqlite --schema \"$CQLITE_SCHEMA\" --data-dir \"$CQLITE_DATA_DIR\" -e \"SELECT * FROM ks.users LIMIT 3\" --out table >/dev/null"

  # One-shot JSON
  run "cqlite --schema \"$CQLITE_SCHEMA\" --data-dir \"$CQLITE_DATA_DIR\" -e \"SELECT id, name FROM ks.users LIMIT 2\" --out json >/dev/null"

  ok "Smoke CLI tests passed"
}

main "$@"


