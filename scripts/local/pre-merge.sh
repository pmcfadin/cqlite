#!/usr/bin/env bash
#
# Local pre-merge validation entrypoint for CQLite.
#
# Modes:
#   fast     formatting, cqlite-core clippy hard gate, all-feature build, unit tests
#   core     fast + doc tests + deterministic M1 parser integration smoke
#   storage  core + pinned dataset/provenance checks + focused SSTable parity smoke
#   bindings core + Linux-only Python/Node binding smoke when local toolchains exist
#   full     storage + bindings + broader local CI parity checks

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

MODE="${1:-fast}"
if [ "$#" -gt 1 ]; then
  echo "error: too many arguments" >&2
  echo "Usage: scripts/local/pre-merge.sh [fast|core|storage|bindings|full]" >&2
  exit 2
fi

DATASET_TAG="${DATASET_TAG:-datasets-v3}"
DATASET_ASSET="${DATASET_ASSET:-cassandra5-small-full-v3.5.tar.gz}"
DATASET_SHA256="${DATASET_SHA256:-13d8da00743d9780c7ee89478649c280f9d91519a4561f6909cc4ce3bb7a3631}"
export DATASET_TAG DATASET_ASSET DATASET_SHA256

export CARGO_TERM_COLOR="${CARGO_TERM_COLOR:-always}"
export RUST_BACKTRACE="${RUST_BACKTRACE:-1}"
export CQLITE_DATASETS_ROOT="${CQLITE_DATASETS_ROOT:-${REPO_ROOT}/test-data/datasets}"

FAST_DONE=0
CORE_DONE=0
STORAGE_DONE=0
BINDINGS_DONE=0
FULL_DONE=0
OPTIONAL_SKIPS=0

usage() {
  cat <<'USAGE'
Usage: scripts/local/pre-merge.sh [fast|core|storage|bindings|full]

Modes:
  fast      Required pre-merge Rust gate: fmt, cqlite-core clippy -D warnings,
            all-feature cqlite-core build, and cqlite-core unit tests.
  core      fast plus cqlite-core doctests and deterministic M1 parser smoke.
  storage   core plus dataset fetch/provenance checks and focused SSTable parity.
  bindings  core plus Linux-only Python and Node smoke checks when local
            toolchains are already installed.
  full      storage plus bindings plus broader local CI parity checks.
USAGE
}

run_step() {
  local label="$1"
  shift

  echo
  echo "==> ${label}"
  printf '+'
  printf ' %q' "$@"
  printf '\n'
  "$@"
}

run_shell_step() {
  local label="$1"
  local command="$2"

  echo
  echo "==> ${label}"
  echo "+ ${command}"
  bash -lc "$command"
}

fail() {
  echo
  echo "error: $*" >&2
  exit 1
}

skip_optional() {
  local label="$1"
  local reason="$2"
  local follow_up="$3"

  OPTIONAL_SKIPS=$((OPTIONAL_SKIPS + 1))
  echo
  echo "==> ${label}"
  echo "SKIP optional check: ${reason}"
  echo "Follow-up command: ${follow_up}"
}

is_linux() {
  [ "$(uname -s)" = "Linux" ]
}

docker_available() {
  command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1
}

verify_dataset_pin() {
  local pin_file="${CQLITE_DATASETS_ROOT}/.dataset-pin"

  [ -f "$pin_file" ] || fail "dataset pin missing: ${pin_file}"
  grep -qx "tag=${DATASET_TAG}" "$pin_file" \
    || fail "dataset pin tag mismatch in ${pin_file}; expected tag=${DATASET_TAG}"
  grep -qx "asset=${DATASET_ASSET}" "$pin_file" \
    || fail "dataset pin asset mismatch in ${pin_file}; expected asset=${DATASET_ASSET}"
  grep -qx "sha256=${DATASET_SHA256}" "$pin_file" \
    || fail "dataset pin sha256 mismatch in ${pin_file}; expected sha256=${DATASET_SHA256}"
}

verify_dataset_content() {
  local data_db_count

  [ -f "${CQLITE_DATASETS_ROOT}/metadata.yml" ] \
    || fail "dataset metadata missing: ${CQLITE_DATASETS_ROOT}/metadata.yml"
  [ -f "${CQLITE_DATASETS_ROOT}/references.yml" ] \
    || fail "dataset references missing: ${CQLITE_DATASETS_ROOT}/references.yml"

  data_db_count="$(find "$CQLITE_DATASETS_ROOT" -name '*-Data.db' 2>/dev/null | wc -l | tr -d ' ')"
  [ "$data_db_count" -gt 0 ] \
    || fail "dataset contains no Data.db files after fetch: ${CQLITE_DATASETS_ROOT}"

  echo "Dataset provenance verified: ${DATASET_ASSET} (${DATASET_TAG})"
  echo "Dataset root: ${CQLITE_DATASETS_ROOT}"
  echo "Data.db files present: ${data_db_count}"
}

run_fast() {
  [ "$FAST_DONE" -eq 0 ] || return 0

  echo "CQLite local pre-merge validation: fast"
  echo "Repository: ${REPO_ROOT}"
  echo "Datasets: ${CQLITE_DATASETS_ROOT}"

  cd "$REPO_ROOT"

  run_step "Format check" \
    cargo fmt --all -- --check

  run_step "cqlite-core clippy hard gate" \
    cargo clippy --package cqlite-core --all-targets --all-features -- -D warnings

  run_step "cqlite-core all-feature build" \
    cargo build --package cqlite-core --all-features

  # Broad lib smoke: run the all-feature unit suite while excluding known
  # legacy write-path tests that still call Database::put/flush paths removed
  # in Issue #175. This keeps fast deterministic without silently accepting
  # unrelated core regressions.
  run_step "cqlite-core fast unit tests" \
    cargo test --package cqlite-core --lib --all-features --no-fail-fast -- \
      --skip query::engine::plan_cache_tests \
      --skip query::select_integration_tests \
      --skip storage::tests::test_batch_operations \
      --skip tests::test_database

  FAST_DONE=1
}

run_core() {
  [ "$CORE_DONE" -eq 0 ] || return 0

  run_fast

  run_step "cqlite-core documentation tests" \
    cargo test --package cqlite-core --doc --all-features --no-fail-fast

  # These are the legacy M1 local parser/format smoke tests. They do not require
  # fetched SSTable binaries, so they keep core mode deterministic and local.
  run_step "M1 parser and format integration smoke" \
    cargo test --package cqlite-core --all-features --no-fail-fast \
      --test P0_4_modern_format_rejection_tests \
      --test parser_abstraction_tests \
      --test parsing_improvements_test

  CORE_DONE=1
}

run_storage() {
  [ "$STORAGE_DONE" -eq 0 ] || return 0

  run_core

  export CQLITE_PARITY_REQUIRE_DATASETS=1

  run_step "Fetch or verify pinned Cassandra datasets" \
    bash test-data/scripts/fetch-datasets.sh

  verify_dataset_pin
  verify_dataset_content

  run_step "Dataset manifest completeness check" \
    bash test-data/scripts/check-dataset-manifest.sh "$CQLITE_DATASETS_ROOT"

  run_step "SSTable Statistics.db parity smoke" \
    cargo test --package cqlite-core --features write-support \
      --test sstabledump_parity_statistics \
      test_statistics_db_parity_comprehensive -- --nocapture

  run_step "SSTable Summary.db parity smoke" \
    cargo test --package cqlite-core --features write-support \
      --test sstabledump_parity_summary \
      test_summary_db_parity_comprehensive -- --nocapture

  STORAGE_DONE=1
}

run_python_binding_smoke() {
  local follow_up
  follow_up="cd bindings/python && python3 -m venv .venv && . .venv/bin/activate && python -m pip install -e '.[dev]' && python -m pytest tests/test_basic.py -q"

  if ! is_linux; then
    skip_optional "Python binding smoke" \
      "Linux-only binding smoke; current host is $(uname -s)" \
      "Run on Linux: ${follow_up}"
    return 0
  fi

  if ! command -v python3 >/dev/null 2>&1; then
    skip_optional "Python binding smoke" \
      "python3 is not installed" \
      "$follow_up"
    return 0
  fi

  if ! command -v maturin >/dev/null 2>&1; then
    skip_optional "Python binding smoke" \
      "maturin is not installed" \
      "$follow_up"
    return 0
  fi

  if ! python3 - <<'PY'
import sys
raise SystemExit(0 if sys.prefix != sys.base_prefix else 1)
PY
  then
    skip_optional "Python binding smoke" \
      "no active Python virtual environment for maturin develop" \
      "$follow_up"
    return 0
  fi

  if ! python3 - <<'PY'
import pytest  # noqa: F401
PY
  then
    skip_optional "Python binding smoke" \
      "pytest is not installed in the active Python environment" \
      "$follow_up"
    return 0
  fi

  run_shell_step "Python binding build and smoke" \
    "cd bindings/python && maturin develop --quiet && python3 -m pytest tests/test_basic.py -q"
}

run_node_binding_smoke() {
  local follow_up
  follow_up="cd bindings/node && npm ci && npm run build && npm test -- --runTestsByPath __test__/smoke.test.js --runInBand"

  if ! is_linux; then
    skip_optional "Node binding smoke" \
      "Linux-only binding smoke; current host is $(uname -s)" \
      "Run on Linux: ${follow_up}"
    return 0
  fi

  if ! command -v node >/dev/null 2>&1 || ! command -v npm >/dev/null 2>&1; then
    skip_optional "Node binding smoke" \
      "node and npm are not both installed" \
      "$follow_up"
    return 0
  fi

  if ! node -e "const major = Number(process.versions.node.split('.')[0]); process.exit(major >= 18 ? 0 : 1)"; then
    skip_optional "Node binding smoke" \
      "Node.js 18 or newer is required" \
      "$follow_up"
    return 0
  fi

  if [ ! -x "bindings/node/node_modules/.bin/jest" ] || [ ! -x "bindings/node/node_modules/.bin/napi" ]; then
    skip_optional "Node binding smoke" \
      "bindings/node dependencies are not installed" \
      "$follow_up"
    return 0
  fi

  run_shell_step "Node binding build and smoke" \
    "cd bindings/node && npm run build && npm test -- --runTestsByPath __test__/smoke.test.js --runInBand"
}

run_bindings() {
  [ "$BINDINGS_DONE" -eq 0 ] || return 0

  run_core
  run_python_binding_smoke
  run_node_binding_smoke

  BINDINGS_DONE=1
}

run_full() {
  [ "$FULL_DONE" -eq 0 ] || return 0

  run_storage
  run_bindings

  run_step "cqlite-cli clippy broad check" \
    cargo clippy --package cqlite-cli --all-targets --all-features -- -D warnings

  run_step "cqlite-core all-feature test suite" \
    cargo test --package cqlite-core --all-features --no-fail-fast -- \
      --skip query::engine::plan_cache_tests \
      --skip query::select_integration_tests \
      --skip storage::tests::test_batch_operations \
      --skip tests::test_database

  if docker_available; then
    run_step "SSTableLoader Docker integration" \
      scripts/local/run-sstableloader-tests.sh
  else
    skip_optional "SSTableLoader Docker integration" \
      "Docker CLI or daemon is not available" \
      "scripts/local/run-sstableloader-tests.sh"
  fi

  FULL_DONE=1
}

case "$MODE" in
  fast)
    run_fast
    ;;
  core)
    run_core
    ;;
  storage)
    run_storage
    ;;
  bindings)
    run_bindings
    ;;
  full)
    run_full
    ;;
  -h|--help|help)
    usage
    exit 0
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac

echo
echo "pre-merge '${MODE}' validation passed."
if [ "$OPTIONAL_SKIPS" -gt 0 ]; then
  echo "Optional checks skipped: ${OPTIONAL_SKIPS} (see follow-up command(s) above)."
fi
