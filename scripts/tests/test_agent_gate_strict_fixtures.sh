#!/usr/bin/env bash
# Regression coverage for issue #4230: the full gate's core-tests, write-tests, and
# cli-tests children must see CQLITE_REQUIRE_FIXTURES=1. --only/--lite probes and
# AGENT_GATE_ALLOW_MISSING_FIXTURES=1 remain explicitly non-certifying, while any strict
# fixture policy supplied by the caller remains inherited by those modes.
#
# This extracts the shipped run_component/run_core_tests/dispatch_component bodies and
# drives their real child commands through a cargo stub. It therefore observes the actual
# nextest, doctest, write-support, and two CLI pass environments without compiling anything.
# A missing-fixture stub failure also proves the strict value is enforcement, not merely a
# census annotation. The final fmt probe proves the scoped environment does not leak.
#
# Run standalone: bash scripts/tests/test_agent_gate_strict_fixtures.sh
# The full gate runs this from the tooling-tests component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1" >&2; FAIL=$((FAIL + 1)); }

if grep -qF 'bash "$REPO_ROOT/scripts/tests/test_agent_gate_strict_fixtures.sh"' "$GATE"; then
  ok "#4230 regression suite is wired into tooling-tests"
else
  bad "#4230 regression suite is not wired into tooling-tests"
fi

extract_function() {
  local fn="$1"
  awk -v fn="$fn" '
    !found && $0 ~ ("^" fn "\\(\\) \\{") { found = 1 }
    found { print }
    found && $0 ~ /^\}$/ { exit }
  ' "$GATE"
}

TMP=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-strict-fixtures.XXXXXX") || {
  bad "could not create a private temporary directory"
  exit 1
}
trap 'rm -rf "$TMP"' EXIT INT TERM HUP

LOG_DIR="$TMP/log"
TRACE="$TMP/cargo.trace"
RESULTS="$TMP/results"
mkdir -p "$LOG_DIR" "$TMP/bin"
export LOG_DIR TRACE RESULTS

# The extracted production functions depend on these small gate-owned interfaces. The
# command bodies and environment boundary stay production code; only result persistence and
# feature-matrix observation are stubbed so no real cargo is reachable.
record_result() {
  printf '%s\t%s\n' "$1" "$2" >>"$RESULTS"
  RECORDED_STATUS="$2"
}

_fm_observe_child() { :; }

for fn in run_component run_core_tests dispatch_component _ansi_stripped_log check_no_unexpected_zero_tests; do
  src=$(extract_function "$fn")
  if [ -z "$src" ]; then
    bad "could not extract $fn() from $GATE — the test would otherwise exercise no production code"
  else
    if eval "$src"; then
      ok "extracted $fn() from the shipped gate"
    else
      bad "extracted $fn() does not parse"
    fi
  fi
done

export -f _fm_observe_child _ansi_stripped_log check_no_unexpected_zero_tests

# The stub models a corpus-gated command. Strict children fail before doing work when the
# fixture is absent; lenient children return the ordinary green test output. Every invocation
# records the exact inherited value, so the assertions cover each child rather than a parent
# shell's assignment.
cat >"$TMP/bin/cargo" <<'EOF'
#!/bin/sh
printf 'args=%s require=%s parity=%s fixture=%s\n' "$*" "${CQLITE_REQUIRE_FIXTURES-<unset>}" "${CQLITE_PARITY_REQUIRE_DATASETS-<unset>}" "${FIXTURE_PRESENT-<absent>}" >>"$TRACE"
if [ "${FIXTURE_PRESENT:-0}" != 1 ] \
  && { [ "${CQLITE_REQUIRE_FIXTURES:-0}" = 1 ] || [ "${CQLITE_PARITY_REQUIRE_DATASETS:-0}" = 1 ]; }; then
  echo 'fixture probe: missing fixture; strict child must fail' >&2
  exit 71
fi
if [ "${1:-}" = nextest ]; then
  printf '1 tests run: 1 passed, 0 failed\n'
else
  printf '%s\n' '     Running tests/fake.rs (target/debug/deps/fake-abc)' \
    'running 1 test' \
    'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s'
fi
exit 0
EOF
chmod +x "$TMP/bin/cargo"
export PATH="$TMP/bin:$PATH"

ONLY=""
LITE=0
NEXTEST=1
CQLITE_SKIP_DOCKER_TESTS=1
GATE_TEST_THREADS=1
AGENT_GATE_FM_COMPONENT=""
unset CQLITE_REQUIRE_FIXTURES CQLITE_PARITY_REQUIRE_DATASETS AGENT_GATE_ALLOW_MISSING_FIXTURES FIXTURE_PRESENT

reset_case() {
  : >"$TRACE"
  : >"$RESULTS"
  rm -f "$LOG_DIR"/*
}

last_status() {
  sed -n "s/^$1\t//p" "$RESULTS" | tail -1
}

assert_status() {
  local component="$1" expected="$2" got
  got=$(last_status "$component")
  if [ "$got" = "$expected" ]; then
    ok "$component records $expected"
  else
    bad "$component recorded '${got:-<none>}' instead of $expected"
  fi
}

assert_trace_count() {
  local expected="$1" label="$2" count
  count=$(wc -l <"$TRACE" | tr -d ' ')
  if [ "$count" -eq "$expected" ]; then
    ok "$label: observed exactly $expected cargo child(s)"
  else
    bad "$label: expected exactly $expected cargo child(s), got $count: $(cat "$TRACE")"
  fi
}

assert_all_trace_field() {
  local field="$1" expected="$2" label="$3" count bad_lines
  count=$(wc -l <"$TRACE" | tr -d ' ')
  bad_lines=$(awk -v wanted="$field=$expected" '
    {
      found = 0
      for (i = 1; i <= NF; i++) {
        if ($i == wanted) {
          found = 1
          break
        }
      }
      if (!found) print
    }
  ' "$TRACE")
  if [ "$count" -gt 0 ] && [ -z "$bad_lines" ]; then
    ok "$label: all $count observed cargo children received exact $field=$expected"
  else
    bad "$label: expected non-empty trace with exact $field=$expected; got: $(cat "$TRACE")"
  fi
}

assert_exact_trace_line() {
  local args="$1" label="$2" expected matches
  expected="args=$args require=1 parity=<unset> fixture=1"
  matches=$(grep -Fxc -- "$expected" "$TRACE" || true)
  if [ "$matches" -eq 1 ]; then
    ok "$label: observed the expected strict child command"
  else
    bad "$label: expected one exact line '$expected', got: $(cat "$TRACE")"
  fi
}

# Full-gate positive controls. These are the actual production command bodies: core-tests
# exercises both nextest and the separate doctest pass; write-tests runs all write-support
# cargo passes; cli-tests runs both its default and write-support passes.
FIXTURE_PRESENT=1
export FIXTURE_PRESENT
reset_case
run_core_tests >"$TMP/core-positive.out" 2>&1
assert_status core-tests PASS
assert_trace_count 2 "core-tests nextest/doctest positive"
assert_all_trace_field require 1 "core-tests nextest/doctest"
if grep -qF 'args=nextest run --package cqlite-core --features cli-helpers' "$TRACE" \
  && grep -qF 'args=test --doc --package cqlite-core --features cli-helpers' "$TRACE"; then
  ok "core-tests nextest and doctest children received CQLITE_REQUIRE_FIXTURES=1"
else
  bad "core-tests nextest/doctest trace did not show strict fixture export: $(cat "$TRACE")"
fi

reset_case
dispatch_component write-tests >"$TMP/write-positive.out" 2>&1
assert_status write-tests PASS
assert_trace_count 11 "write-tests positive"
assert_all_trace_field require 1 "write-tests"
assert_exact_trace_line 'test --package cqlite-core --features write-support --lib' 'write-tests lib'
assert_exact_trace_line 'test --package cqlite-core --features write-support --test write_read_roundtrip' 'write-tests write_read_roundtrip'
assert_exact_trace_line 'test --package cqlite-core --features write-support --test compaction_integration' 'write-tests compaction_integration'
assert_exact_trace_line 'test --package cqlite-core --features write-support --test issue_4196_salvage_healthy_parity' 'write-tests issue_4196_salvage_healthy_parity'
assert_exact_trace_line 'test --package cqlite-core --features write-support --test issue_4196_salvage_corruption_corpus' 'write-tests issue_4196_salvage_corruption_corpus'
assert_exact_trace_line 'test --package cqlite-core --features write-support --test issue_4196_salvage_oom_bounds' 'write-tests issue_4196_salvage_oom_bounds'
assert_exact_trace_line 'test --package cqlite-core --features write-support --test issue_4196_salvage_round15_bounds' 'write-tests issue_4196_salvage_round15_bounds'
assert_exact_trace_line 'test --package cqlite-core --features write-support --test issue_4196_round16_chunk_size_ceiling' 'write-tests issue_4196_round16_chunk_size_ceiling'
assert_exact_trace_line 'test --package cqlite-core --features write-support --test issue_4196_salvage_partition_atomicity' 'write-tests issue_4196_salvage_partition_atomicity'
assert_exact_trace_line 'test --package cqlite-core --features write-support --test issue_4196_salvage_effective_schema' 'write-tests issue_4196_salvage_effective_schema'
assert_exact_trace_line 'test --package cqlite-core --features write-support --test issue_4196_salvage_output_input_contracts' 'write-tests issue_4196_salvage_output_input_contracts'

reset_case
dispatch_component cli-tests >"$TMP/cli-positive.out" 2>&1
assert_status cli-tests PASS
cli_count=$(wc -l <"$TRACE" | tr -d ' ')
cli_default_count=$(awk '/^args=test / && index($0, "--features write-support") == 0 { count++ } END { print count + 0 }' "$TRACE")
cli_write_count=$(awk '/^args=test / && index($0, "--features write-support") > 0 { count++ } END { print count + 0 }' "$TRACE")
assert_trace_count 2 "cli-tests positive"
assert_all_trace_field require 1 "cli-tests default/write-support children"
if [ "$cli_count" -eq 2 ] && [ "$cli_default_count" -eq 1 ] && [ "$cli_write_count" -eq 1 ]; then
  ok "cli-tests default and write-support children received CQLITE_REQUIRE_FIXTURES=1"
else
  bad "cli-tests did not observe exactly one strict child for each feature pass: $(cat "$TRACE")"
fi

# Plain cargo fallback is a separate production path and must also be strict. It is tested
# independently because a nextest-only assertion could leave the fallback unprotected.
NEXTEST=0
FIXTURE_PRESENT=1
reset_case
run_core_tests >"$TMP/core-fallback-positive.out" 2>&1
assert_status core-tests PASS
assert_trace_count 1 "core-tests cargo fallback positive"
assert_all_trace_field require 1 "core-tests cargo fallback"
if grep -qF 'args=test --package cqlite-core --features cli-helpers -- --test-threads 1' "$TRACE"; then
  ok "core-tests cargo fallback child received CQLITE_REQUIRE_FIXTURES=1"
else
  bad "core-tests cargo fallback command was not observed: $(cat "$TRACE")"
fi

unset FIXTURE_PRESENT
reset_case
run_core_tests >"$TMP/core-fallback-negative.out" 2>&1
assert_status core-tests FAIL
assert_trace_count 1 "core-tests cargo fallback missing-fixture"
assert_all_trace_field require 1 "core-tests cargo fallback missing-fixture"
if grep -qF 'fixture probe: missing fixture; strict child must fail' "$LOG_DIR/core-tests.log"; then
  ok "core-tests cargo fallback fails hard on an absent fixture"
else
  bad "core-tests cargo fallback lost the absent-fixture failure evidence"
fi
NEXTEST=1

# Full-gate negative controls. The same stub has no fixture, so each component must record a
# hard FAIL after its first strict child sees CQLITE_REQUIRE_FIXTURES=1.
unset FIXTURE_PRESENT
for component in core-tests write-tests cli-tests; do
  reset_case
  case "$component" in
    core-tests) run_core_tests >"$TMP/$component-negative.out" 2>&1 ;;
    *) dispatch_component "$component" >"$TMP/$component-negative.out" 2>&1 ;;
  esac
  assert_status "$component" FAIL
  if grep -qF 'fixture probe: missing fixture; strict child must fail' "$LOG_DIR/$component.log"; then
    ok "$component fails hard on an absent fixture under the full-gate strict export"
  else
    bad "$component did not preserve the strict absent-fixture failure evidence"
  fi
  assert_all_trace_field require 1 "$component absent-fixture child"
done

# Documented opt-out: absent fixtures may complete only as an explicitly non-certifying run,
# and with no caller policy every child remains lenient. This covers nextest/doc, write, and
# both CLI passes through the same real bodies above.
AGENT_GATE_ALLOW_MISSING_FIXTURES=1
unset CQLITE_REQUIRE_FIXTURES CQLITE_PARITY_REQUIRE_DATASETS
for component in core-tests write-tests cli-tests; do
  reset_case
  case "$component" in
    core-tests) run_core_tests >"$TMP/$component-optout.out" 2>&1 ;;
    *) dispatch_component "$component" >"$TMP/$component-optout.out" 2>&1 ;;
  esac
  assert_status "$component" PASS
  assert_all_trace_field require '<unset>' "$component opt-out require"
  assert_all_trace_field parity '<unset>' "$component opt-out parity"
done
unset AGENT_GATE_ALLOW_MISSING_FIXTURES

# An explicit caller policy remains authoritative in the non-certifying opt-out. This guards
# against changing run_component to scrub the environment merely because it did not add the
# full-gate assignment.
AGENT_GATE_ALLOW_MISSING_FIXTURES=1
CQLITE_REQUIRE_FIXTURES=1
CQLITE_PARITY_REQUIRE_DATASETS=1
export AGENT_GATE_ALLOW_MISSING_FIXTURES CQLITE_REQUIRE_FIXTURES CQLITE_PARITY_REQUIRE_DATASETS
reset_case
run_core_tests >"$TMP/optout-inherited.out" 2>&1
assert_status core-tests FAIL
assert_all_trace_field require 1 "opt-out inherited require policy"
assert_all_trace_field parity 1 "opt-out inherited parity policy"
unset AGENT_GATE_ALLOW_MISSING_FIXTURES CQLITE_REQUIRE_FIXTURES CQLITE_PARITY_REQUIRE_DATASETS

# Probe modes are not certification runs, so with no caller policy they remain lenient. Explicit
# caller strictness is tested separately below and must still be inherited by each probe child.
ONLY=core-tests
unset CQLITE_REQUIRE_FIXTURES CQLITE_PARITY_REQUIRE_DATASETS
reset_case
run_core_tests >"$TMP/only-probe.out" 2>&1
assert_status core-tests PASS
assert_all_trace_field require '<unset>' '--only core-tests probe require'
assert_all_trace_field parity '<unset>' '--only core-tests probe parity'

ONLY=""
LITE=1
reset_case
dispatch_component write-tests >"$TMP/lite-probe.out" 2>&1
assert_status write-tests PASS
assert_all_trace_field require '<unset>' '--lite write-tests probe require'
assert_all_trace_field parity '<unset>' '--lite write-tests probe parity'

CQLITE_REQUIRE_FIXTURES=1
CQLITE_PARITY_REQUIRE_DATASETS=1
export CQLITE_REQUIRE_FIXTURES CQLITE_PARITY_REQUIRE_DATASETS
ONLY=core-tests
reset_case
run_core_tests >"$TMP/only-inherited.out" 2>&1
assert_status core-tests FAIL
assert_all_trace_field require 1 '--only core-tests inherited require'
assert_all_trace_field parity 1 '--only core-tests inherited parity'

ONLY=""
LITE=1
reset_case
dispatch_component write-tests >"$TMP/lite-inherited.out" 2>&1
assert_status write-tests FAIL
assert_all_trace_field require 1 '--lite write-tests inherited require'
assert_all_trace_field parity 1 '--lite write-tests inherited parity'

LITE=0
unset CQLITE_REQUIRE_FIXTURES CQLITE_PARITY_REQUIRE_DATASETS
FIXTURE_PRESENT=1
export FIXTURE_PRESENT
reset_case
run_core_tests >"$TMP/leak-core.out" 2>&1
PROBE="$TMP/bin/non-dataset-probe"
cat >"$PROBE" <<'EOF'
#!/bin/sh
printf 'fmt-require=%s\n' "${CQLITE_REQUIRE_FIXTURES-<unset>}" >>"$TRACE"
[ "${CQLITE_REQUIRE_FIXTURES-}" != 1 ]
EOF
chmod +x "$PROBE"
run_component fmt "$PROBE" >"$TMP/leak-fmt.out" 2>&1
if grep -qF 'fmt-require=<unset>' "$TRACE" && [ "$(last_status fmt)" = PASS ]; then
  ok "strict fixture export does not leak into the following dataset-free component"
else
  bad "strict fixture export leaked into fmt or the probe did not run: $(cat "$TRACE")"
fi

printf 'PASS=%s FAIL=%s\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
