#!/usr/bin/env bash
# Regression test for issue #2039: the full gate's `cli-tests` component must
# ENUMERATE every cqlite-cli/tests/*.rs integration-test target, not run a
# hardcoded 3-target allowlist.
#
# Root cause of the false-green (#2039, observed on #1483): the component ran a
# fixed list of exactly three targets —
#     cargo test -p cqlite-cli --test unit_tests
#     cargo test -p cqlite-cli --features write-support --test write_readback_content_tests
#     cargo test -p cqlite-cli --features write-support --test graceful_shutdown_tests
# so any NEW cqlite-cli integration test file was invisible to the full gate: a
# red integration test the gate never ran could still merge (read_sstable_stdout_tests
# was failing while the gate reported cli-tests PASS).
#
# The fix runs `cargo test --package cqlite-cli --features write-support --tests`,
# which cargo expands to EVERY test target whose required-features are satisfied
# (auto-skipping the deliberately-excluded delta-export/duckdb-tests/dhat-heap
# targets), so a new tests/*.rs is automatically covered and a deliberately-failing
# one propagates cargo's non-zero exit → the FULL gate FAILs (acceptance #2).
#
# Fast + hermetic by design: it asserts the SHAPE of the cli-tests command in
# agent-gate.sh (enumeration, no allowlist, fail-closed guard, dataset-guarded)
# and behaviorally exercises the fail-closed zero-file guard. It NEVER runs cargo.
#
# Run standalone:   bash scripts/tests/test_agent_gate_cli_tests_enum.sh
# Or via the gate:  scripts/agent-gate.sh runs it as part of `tooling-tests`.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

# Extract the `cli-tests) run_component ... ;;` dispatch block from the gate.
CLI_BLOCK=$(awk '/cli-tests\) run_component cli-tests bash -c/{f=1} f{print} f&&/;;$/{exit}' "$GATE")

if [ -z "$CLI_BLOCK" ]; then
  bad "could not locate the cli-tests dispatch block in $GATE"
else
  ok "located the cli-tests dispatch block"
fi

# 1. Enumeration form: must run `cargo test ... --tests` (cargo enumerates every
#    integration test target) rather than naming individual targets.
if grep -qE 'cargo test --package cqlite-cli --features write-support --tests' <<<"$CLI_BLOCK"; then
  ok "cli-tests enumerates all targets via 'cargo test --package cqlite-cli --features write-support --tests'"
else
  bad "cli-tests does NOT use the enumerating '--tests' form (regressed to an allowlist?)"
fi

# 2. Regression guard: the exact #2039 false-green shape (a hardcoded per-target
#    allowlist) must NOT return.
if grep -qE -- '--test (unit_tests|write_readback_content_tests|graceful_shutdown_tests)' <<<"$CLI_BLOCK"; then
  bad "cli-tests reintroduced the hardcoded 3-target allowlist (#2039 false-green)"
else
  ok "cli-tests carries no hardcoded per-target allowlist (#2039 fixed)"
fi

# 3. Fail-closed guard: zero enumerable test files must be a visible error, never
#    a silent pass.
if grep -q 'FAIL-CLOSED' <<<"$CLI_BLOCK" && grep -q 'cli_test_count' <<<"$CLI_BLOCK"; then
  ok "cli-tests fails closed when no cqlite-cli/tests/*.rs files are found"
else
  bad "cli-tests lacks a fail-closed zero-file guard (#2039 hard rule)"
fi

# 4. cli-tests now reads real Data.db (real-data CLI integration tests are in the
#    enumerated set), so it must join DATASET_COMPONENTS to be guarded by the
#    dataset preflight (the #646 hazard otherwise).
DS_LINE=$(grep -E '^DATASET_COMPONENTS=' "$GATE")
if grep -qw 'cli-tests' <<<"$DS_LINE"; then
  ok "cli-tests is dataset-guarded (present in DATASET_COMPONENTS)"
else
  bad "cli-tests missing from DATASET_COMPONENTS — dataset preflight would not guard it (#646 hazard)"
fi

# 5. Behavioral check of the fail-closed guard logic itself, in isolation. Uses the
#    SAME snippet shape the gate runs; proves it fails on an empty tests/ dir and
#    passes on a populated one, quoted safely.
tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-cli-enum-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

guard() {
  # mirrors the gate's guard (find + count + fail-closed) against $1/tests
  local root="$1" cli_test_count
  cli_test_count=$(find "$root/tests" -maxdepth 1 -name "*.rs" 2>/dev/null | wc -l | tr -d " ")
  if [ "${cli_test_count:-0}" -eq 0 ]; then
    echo "FAIL-CLOSED" >&2
    return 1
  fi
  echo "$cli_test_count"
  return 0
}

mkdir -p "$tmp/empty/tests"
if guard "$tmp/empty" >/dev/null 2>&1; then
  bad "fail-closed guard did NOT fail on an empty tests/ directory"
else
  ok "fail-closed guard fails on an empty tests/ directory"
fi

mkdir -p "$tmp/populated/tests"
: >"$tmp/populated/tests/some_new_integration_test.rs"
: >"$tmp/populated/tests/another_test.rs"
count=$(guard "$tmp/populated" 2>/dev/null)
if [ "$count" = "2" ]; then
  ok "guard counts enumerable test files (a NEW tests/*.rs is discovered: got $count)"
else
  bad "guard miscounted enumerable test files (expected 2, got '${count:-<none>}')"
fi

echo
echo "cli-tests-enum self-test: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
