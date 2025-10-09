#!/usr/bin/env bash
set -euo pipefail

# Shared helpers for CLI validation scripts

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export PATH="$ROOT_DIR/target/debug:$ROOT_DIR/target/release:$PATH"

ok() { printf "\033[32m✔ %s\033[0m\n" "$*"; }
warn() { printf "\033[33m⚠ %s\033[0m\n" "$*"; }
err() { printf "\033[31m✘ %s\033[0m\n" "$*"; }

require_bin() {
  if ! command -v "$1" >/dev/null 2>&1; then
    err "Missing binary: $1"
    exit 127
  fi
}

run() {
  echo "> $*"
  if ! eval "$*"; then
    err "Command failed: $*"
    return 1
  fi
}

assert_exit() {
  local expected=$1; shift
  set +e
  eval "$@"
  local code=$?
  set -e
  if [[ $code -ne $expected ]]; then
    err "Expected exit $expected, got $code: $*"
    return 1
  fi
}

assert_grep() {
  local pattern=$1; shift
  local file=$1; shift
  if ! grep -E "$pattern" "$file" >/dev/null 2>&1; then
    err "Missing expected pattern: $pattern"
    return 1
  fi
}

tmpfile() {
  mktemp "/tmp/cqlite.XXXXXX"
}

export_default_env() {
  export CQLITE_DATA_DIR=${CQLITE_DATA_DIR:-"$ROOT_DIR/test-data/datasets"}
  export CQLITE_SCHEMA=${CQLITE_SCHEMA:-"$ROOT_DIR/test-data/schemas"}
}


