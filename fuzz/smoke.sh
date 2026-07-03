#!/usr/bin/env bash
# Local bounded smoke run of every CQLite fuzz target (issue #1614).
#
# REQUIRES: nightly Rust + cargo-fuzz (`rustup toolchain install nightly` and
# `cargo install cargo-fuzz`). Fuzzing uses libFuzzer, which needs nightly.
#
# This is a developer convenience mirroring the PR smoke lane's bounded flags.
# It is deliberately NOT wired into scripts/agent-gate.sh — the stable gate runs
# on stable Rust and must not depend on nightly/cargo-fuzz.
#
# Usage:
#   fuzz/smoke.sh                 # ~20s per target
#   MAX_TOTAL_TIME=45 fuzz/smoke.sh
#
# The block-emit target only exercises the full path when the simple_table
# fixture is reachable; export CQLITE_DATASETS_ROOT to the test-data datasets
# dir first (otherwise that target no-ops but still proves never-panic).
set -euo pipefail

cd "$(dirname "$0")"

MAX_TOTAL_TIME="${MAX_TOTAL_TIME:-20}"
RSS_LIMIT_MB="${RSS_LIMIT_MB:-2048}"
TIMEOUT="${TIMEOUT:-25}"

TARGETS=(
  fuzz_vint
  fuzz_value_decode
  fuzz_block_emit
  fuzz_bti
  fuzz_schema_parse
)

if ! cargo fuzz --version >/dev/null 2>&1; then
  echo "ERROR: cargo-fuzz not installed. Run: cargo install cargo-fuzz" >&2
  exit 2
fi

failures=0
for target in "${TARGETS[@]}"; do
  echo "=== fuzz: ${target} (max_total_time=${MAX_TOTAL_TIME}s) ==="
  if cargo +nightly fuzz run "${target}" -- \
      -max_total_time="${MAX_TOTAL_TIME}" \
      -rss_limit_mb="${RSS_LIMIT_MB}" \
      -timeout="${TIMEOUT}"; then
    echo "PASS: ${target}"
  else
    echo "CRASH/FAIL: ${target} (see fuzz/artifacts/${target}/)" >&2
    failures=$((failures + 1))
  fi
done

if [ "${failures}" -ne 0 ]; then
  echo "${failures} fuzz target(s) reported a crash/failure" >&2
  exit 1
fi
echo "All fuzz targets passed the bounded smoke run."
