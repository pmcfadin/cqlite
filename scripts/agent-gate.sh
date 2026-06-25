#!/usr/bin/env bash
# Canonical agent gate (issue #719).
#
# This script IS the gate. A builder claiming "the gate passed" must have run
# this script and pasted its summary block verbatim; ad-hoc cargo invocations
# do not count. It exists because epic #646 shipped three false-green reports
# rooted in "which commands count as the gate" ambiguity (feature-gated tests
# silently skipping, filtered runs reported as full runs).
#
# Components mirror the enforced CI gates (.github/workflows/ci.yml,
# ci-minimal-features.yml, python-ci.yml) plus the local smoke suite:
#   fmt                cargo fmt --all --check
#   clippy             RUSTFLAGS="-D warnings" clippy --workspace --all-targets --all-features
#   core-tests         cargo test -p cqlite-core --features cli-helpers (CI skip-list applied)
#   integration-tests  cargo test -p cqlite-integration-tests: compile ALL targets
#                      (--no-run, whole package) then run the seven CI-enforced ones
#   format-compat      cargo test -p format-compatibility-tests (the 'oa' format crate;
#                      issue #865 folded it into the workspace so fmt/clippy reach it)
#   write-tests        cargo test -p cqlite-core --features write-support (lib + roundtrip + compaction)
#   cli-tests          cargo test -p cqlite-cli --test unit_tests
#   python-bindings    maturin develop + pytest bindings/python/tests in a throwaway
#                      venv; SKIPs (never silently PASSes) if python3 is unavailable.
#                      Set RUN_SLOW_TESTS=1 to also run the CLI-parity suite.
#   minimal-build      cargo build -p cqlite-core --no-default-features --features all-compression
#   smoke              bash test-data/scripts/smoke-test-all-tables.sh
#
# The integration-tests --no-run sweep, the format-compat component, and the
# python-bindings component close the three blind spots from issue #865: a
# compile break in a non-enumerated test target, a fmt/compile break in the
# (previously workspace-excluded) format-compatibility crate, and Python-only
# regressions (LIMIT 0, SET<TEXT> validation) that shipped "gate PASS".
#
# All components run even after a failure so one run reports everything.
# Exit code 0 iff every component passes. Machine-checkable output: the
# summary block between the AGENT-GATE SUMMARY markers, ending in
# "RESULT: PASS" or "RESULT: FAIL".
#
# Usage:
#   scripts/agent-gate.sh             # full gate (the only run that counts)
#   scripts/agent-gate.sh --list      # list components without running
#   scripts/agent-gate.sh --only fmt,clippy   # debugging aid; output is
#                                     # marked PARTIAL and never counts as the gate
set -uo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT="$PWD"

# Agent sandboxes often run with a minimal PATH; pick up rustup's cargo.
if ! command -v cargo >/dev/null 2>&1 && [ -d "$HOME/.cargo/bin" ]; then
  export PATH="$HOME/.cargo/bin:$PATH"
fi
export CQLITE_DATASETS_ROOT="${CQLITE_DATASETS_ROOT:-$REPO_ROOT/test-data/datasets}"

COMPONENTS=(fmt clippy core-tests tombstones-scan integration-tests format-compat write-tests cli-tests python-bindings minimal-build smoke)
ONLY=""
case "${1:-}" in
  --list) printf '%s\n' "${COMPONENTS[@]}"; exit 0 ;;
  --only) ONLY="${2:?--only needs a comma-separated component list}" ;;
  "") ;;
  *) echo "unknown argument: $1" >&2; exit 2 ;;
esac

LOG_DIR=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate.XXXXXX")
declare -a NAMES=() STATUSES=() TIMES=()
OVERALL=PASS

run_component() { # run_component <name> <cmd...>
  local name="$1"; shift
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local log="$LOG_DIR/$name.log"
  local start end status
  echo ">>> [$name] $*"
  start=$(date +%s)
  if "$@" >"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    OVERALL=FAIL
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("$((end - start))s")
  echo ">>> [$name] $status ($((end - start))s)"
}

# python-bindings: build the extension with maturin and run pytest. Unlike the
# Rust components this is SKIP-aware: if there is no usable python3 the component
# records SKIP (loudly, never silently PASS) so a missing toolchain can't mask a
# real Python regression the way it did pre-#865. Anything else (venv/build/test
# failure) is a hard FAIL.
run_python_bindings() {
  local name=python-bindings
  if [ -n "$ONLY" ] && ! grep -qw "$name" <<<"${ONLY//,/ }"; then
    return 0
  fi
  local log="$LOG_DIR/$name.log"
  local start end status
  start=$(date +%s)
  if ! command -v python3 >/dev/null 2>&1; then
    status=SKIP
    echo ">>> [$name] SKIP (no python3 on PATH)"
    NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("0s")
    return 0
  fi
  # Persistent venv under target/ so repeat runs skip the maturin/pytest install.
  local venv="$REPO_ROOT/target/agent-gate-venv"
  echo ">>> [$name] maturin develop + pytest (venv: $venv, RUN_SLOW_TESTS=${RUN_SLOW_TESTS:-0})"
  if RUN_SLOW_TESTS="${RUN_SLOW_TESTS:-0}" bash -c '
      set -euo pipefail
      venv="'"$venv"'"
      [ -x "$venv/bin/python" ] || python3 -m venv "$venv"
      . "$venv/bin/activate"
      pip install --quiet --upgrade pip >/dev/null
      pip install --quiet maturin pytest
      maturin develop -m bindings/python/Cargo.toml
      pytest bindings/python/tests -q' >"$log" 2>&1; then
    status=PASS
  else
    status=FAIL
    OVERALL=FAIL
    echo "--- [$name] FAILED; last 40 lines of $log ---"
    tail -40 "$log"
    echo "--- end of $name output ---"
  fi
  end=$(date +%s)
  NAMES+=("$name"); STATUSES+=("$status"); TIMES+=("$((end - start))s")
  echo ">>> [$name] $status ($((end - start))s)"
}

# Dataset preflight: dataset-dependent components must FAIL loudly when data is
# missing, never silently pass on a skipped suite (the #646 failure mode).
DATA_COUNT=$(find "$CQLITE_DATASETS_ROOT/sstables" -name "*-Data.db" 2>/dev/null | wc -l | tr -d ' ')
if [ "$DATA_COUNT" -eq 0 ]; then
  echo "agent-gate: no Data.db files under $CQLITE_DATASETS_ROOT/sstables" >&2
  echo "agent-gate: fetch them first: bash test-data/scripts/fetch-datasets.sh" >&2
  exit 1
fi

# CI dataset pins, for the CI-parity check (issue #719): local validation must
# target the same asset CI uses.
PIN_FILE=".github/workflows/sstabledump-parity-gate.yml"
PINS=$(grep -E 'DATASET_(TAG|ASSET|SHA256):' "$PIN_FILE" 2>/dev/null | sed 's/^ *//' | tr '\n' ' ' || echo "unavailable")

run_component fmt cargo fmt --all --check
run_component clippy env RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features
run_component core-tests cargo test --package cqlite-core --features cli-helpers -- \
  --skip test_legacy_format_allows_blob_fallback_with_feature
# Issue #1085: the row-collapse bug lived in the `tombstones`-feature scan path,
# which the default gate run (cli-helpers) never compiles. Run the full-scan
# regression test under `tombstones` so a re-introduction can't ship green.
run_component tombstones-scan cargo test --package cqlite-core \
  --features write-support,cli-helpers,tombstones \
  --test issue_1085_tombstones_full_scan_parity
# Compile EVERY target in the package first (--no-run, whole package) so a
# new/edited test file that doesn't compile can't hide behind the enumerated
# run-list (issue #865); then execute the seven CI-enforced targets.
run_component integration-tests bash -c '
  cargo test --package cqlite-integration-tests --no-run &&
  cargo test --package cqlite-integration-tests \
    --test chunked_data_reader_direct_test \
    --test comprehensive_component_integration_tests \
    --test fixture_specific_integration_tests \
    --test golden_path_get_operations_tests \
    --test golden_path_partition_lookup_tests \
    --test golden_path_scan_operations_tests \
    --test golden_path_summary_index_integration_tests'
# format-compatibility-tests is now a workspace member (issue #865) so fmt/clippy
# reach it; run its 'oa' format compliance tests here too.
run_component format-compat cargo test --package format-compatibility-tests
run_component write-tests bash -c '
  cargo test --package cqlite-core --features write-support --lib &&
  cargo test --package cqlite-core --features write-support --test write_read_roundtrip &&
  cargo test --package cqlite-core --features write-support --test compaction_integration'
run_component cli-tests cargo test --package cqlite-cli --test unit_tests
run_python_bindings
run_component minimal-build cargo build --package cqlite-core --no-default-features --features all-compression
# Pin smoke to a binary built from THIS tree. Left to its own devices the
# smoke script prefers any existing target/release/cqlite, however stale —
# the first full gate run caught a May binary failing all test_oa tables
# that current code reads fine.
run_component smoke bash -c '
  cargo build --package cqlite-cli --bin cqlite &&
  CQLITE_CLI="$PWD/target/debug/cqlite" bash test-data/scripts/smoke-test-all-tables.sh'

echo
echo "==== AGENT-GATE SUMMARY ===="
echo "commit: $(git rev-parse --short HEAD) branch: $(git rev-parse --abbrev-ref HEAD) dirty: $(test -n "$(git status --porcelain)" && echo yes || echo no)"
echo "datasets: $DATA_COUNT Data.db files under $CQLITE_DATASETS_ROOT"
echo "ci-pins: $PINS"
if [ -n "$ONLY" ]; then
  echo "mode: PARTIAL (--only $ONLY) - does NOT count as the gate"
  [ "$OVERALL" = "PASS" ] && OVERALL=PARTIAL
fi
for i in "${!NAMES[@]}"; do
  printf '%-18s %s (%s)\n' "${NAMES[$i]}:" "${STATUSES[$i]}" "${TIMES[$i]}"
done
echo "logs: $LOG_DIR"
echo "RESULT: $OVERALL"
echo "==== END AGENT-GATE SUMMARY ===="

# Exit 0 only for a full-gate PASS; PARTIAL runs exit 3 so they can never be
# scripted into a green gate claim.
case "$OVERALL" in
  PASS) exit 0 ;;
  PARTIAL) exit 3 ;;
  *) exit 1 ;;
esac
