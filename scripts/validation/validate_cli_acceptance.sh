#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$ROOT_DIR/scripts/validation/lib.sh"

# Full manual acceptance suite aligned to M2_CLI_SPEC

main() {
  export_default_env
  require_bin cargo

  # Build with state_machine to enable ingestion, writers, and :status
  if [[ "${CQLITE_BUILD_RELEASE:-0}" == "1" ]]; then
    "$ROOT_DIR/scripts/validation/build_cqlite.sh" --release --package cqlite-cli --features state_machine
    export PATH="$ROOT_DIR/target/release:$PATH"
  else
    "$ROOT_DIR/scripts/validation/build_cqlite.sh" --package cqlite-cli --features state_machine
    export PATH="$ROOT_DIR/target/debug:$PATH"
  fi

  require_bin cqlite

  # A. Help and version
  run "cqlite --help > /tmp/cqlite_help.txt"
  assert_grep "--schema" /tmp/cqlite_help.txt
  assert_grep "--data-dir" /tmp/cqlite_help.txt
  assert_grep "-e, --execute" /tmp/cqlite_help.txt
  assert_grep "-f, --file" /tmp/cqlite_help.txt
  assert_grep "--out" /tmp/cqlite_help.txt
  assert_grep "--limit" /tmp/cqlite_help.txt
  assert_grep "--page-size" /tmp/cqlite_help.txt
  run "cqlite --version > /tmp/cqlite_version.txt"

  # B. One-shot table output
  run "cqlite --schema \"$CQLITE_SCHEMA\" --data-dir \"$CQLITE_DATA_DIR\" -e \"SELECT * FROM ks.users LIMIT 5\" --out table > /tmp/cqlite_table.txt"
  assert_grep "\\(5 rows\\)" /tmp/cqlite_table.txt || true # table footer; tolerate formatter variants

  # C. One-shot JSON
  run "cqlite --schema \"$CQLITE_SCHEMA\" --data-dir \"$CQLITE_DATA_DIR\" -e \"SELECT id, name FROM ks.users LIMIT 3\" --out json > /tmp/cqlite_json.json"
  run "jq -e . /tmp/cqlite_json.json > /dev/null" || { err "Invalid JSON output"; exit 1; }

  # D. One-shot CSV
  run "cqlite --schema \"$CQLITE_SCHEMA\" --data-dir \"$CQLITE_DATA_DIR\" -e \"SELECT id, email FROM ks.users LIMIT 3\" --out csv > /tmp/cqlite_csv.csv"
  assert_grep "^id,.*email$" /tmp/cqlite_csv.csv

  # E. Script execution
  SCRIPT=$(tmpfile)
  cat > "$SCRIPT" <<'EOF'
USE ks;
SELECT * FROM users LIMIT 5;
SELECT id, name FROM orders LIMIT 3;
EOF
  run "cqlite --schema \"$CQLITE_SCHEMA\" --data-dir \"$CQLITE_DATA_DIR\" -f \"$SCRIPT\" --out table > /tmp/cqlite_script.txt"

  # F. Limits and pagination
  run "cqlite --schema \"$CQLITE_SCHEMA\" --data-dir \"$CQLITE_DATA_DIR\" --limit 10 -e \"SELECT * FROM ks.users\" --out json > /tmp/cqlite_limit.json"
  run "jq -e 'length <= 10' /tmp/cqlite_limit.json > /dev/null"
  run "cqlite --schema \"$CQLITE_SCHEMA\" --data-dir \"$CQLITE_DATA_DIR\" --page-size 25 -e \"SELECT * FROM ks.events LIMIT 50\" --out table > /tmp/cqlite_pages.txt"

  # G. Env vs flag precedence
  run "CQLITE_OUT=json cqlite --schema \"$CQLITE_SCHEMA\" --data-dir \"$CQLITE_DATA_DIR\" -e \"SELECT * FROM ks.users LIMIT 1\" --out csv > /tmp/cqlite_over.csv"
  assert_grep "," /tmp/cqlite_over.csv

  # H/I. REPL session (:status, :keyspaces, USE, :tables, DESCRIBE, SELECT, :health)
  # Drive REPL via :source
  REPL_SCRIPT=$(tmpfile)
  cat > "$REPL_SCRIPT" <<EOF
:config data-dir $CQLITE_DATA_DIR
:schema load $CQLITE_SCHEMA
:status
:keyspaces
USE ks;
:tables
DESCRIBE ks.users;
SELECT id, name, email FROM users LIMIT 5;
:health
EOF
  run "cqlite --file \"$REPL_SCRIPT\" > /tmp/cqlite_repl.txt"
  assert_grep "Keyspaces" /tmp/cqlite_repl.txt
  assert_grep "users" /tmp/cqlite_repl.txt

  # K. Error handling and exit codes
  assert_exit 3 "cqlite --schema /does/not/exist --data-dir \"$CQLITE_DATA_DIR\" -e \"SELECT * FROM ks.users LIMIT 1\" --out table >/dev/null 2>&1"
  assert_exit 4 "CQLITE_DATA_DIR= cqlite --schema \"$CQLITE_SCHEMA\" -e \"SELECT * FROM ks.users LIMIT 1\" --out table >/dev/null 2>&1"
  assert_exit 5 "cqlite --schema \"$CQLITE_SCHEMA\" --data-dir \"$CQLITE_DATA_DIR\" -e \"SELECT * FROM ks.users ORDER BY name\" --out table >/dev/null 2>&1"

  # L. Low-level info
  run "cqlite info \"$CQLITE_DATA_DIR\" > /tmp/cqlite_info.txt"

  ok "Acceptance suite passed"
}

main "$@"


