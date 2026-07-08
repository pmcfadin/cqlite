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
# The fix runs TWO feature-correct passes, both derived off the tests/*.rs glob:
#   PASS 1  cargo test --package cqlite-cli --test <default...>  (default/read-only)
#           — the glob MINUS required-features targets MINUS a documented QUARANTINE
#             of pre-existing-red targets. Auto-covers current AND future read-only
#             files.
#   PASS 2  cargo test --package cqlite-cli --features write-support --test <ws...>
#           — the write-support-gated targets, DERIVED (not hardcoded) from
#             cqlite-cli/Cargo.toml required-features + the two self-gated
#             ground-truth targets, run explicitly (NOT `--tests`, which would
#             re-run read-only-only targets like cli_schema_validation_tests under
#             a write-capable binary and fail them).
#
# Fast + hermetic by design: it asserts the SHAPE of the cli-tests command in
# agent-gate.sh (two derived passes, no unit_tests allowlist, documented quarantine,
# fail-closed guards, dataset-guarded) and behaviorally exercises the fail-closed
# zero-file guard, the write-support target-derivation awk, and the Pass-1
# set-subtraction (glob - required-features - quarantine). It NEVER runs cargo.
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

# 1. PASS 1 — default-feature enumeration: derives its target set from the tests/*.rs
#    glob (all_targets) minus required-features minus quarantine (def_flags), run
#    under default features (no --features flag on the first cargo invocation).
if grep -q 'all_targets=' <<<"$CLI_BLOCK" && grep -q 'def_flags' <<<"$CLI_BLOCK" \
   && grep -qE 'cargo test --package cqlite-cli "\$\{def_flags\[@\]\}"' <<<"$CLI_BLOCK"; then
  ok "PASS 1 runs a glob-derived default-feature target set (def_flags), not a hardcoded list"
else
  bad "PASS 1 does NOT enumerate a glob-derived default target set (regressed to an allowlist?)"
fi

# 2. PASS 2 — write-support pass exists and is DERIVED, not hardcoded.
if grep -qE 'cargo test --package cqlite-cli --features write-support' <<<"$CLI_BLOCK"; then
  ok "PASS 2 runs the write-support pass under --features write-support"
else
  bad "PASS 2 (write-support) is missing"
fi
if grep -q 'ws_targets' <<<"$CLI_BLOCK" && grep -q 'cqlite-cli/Cargo.toml' <<<"$CLI_BLOCK"; then
  ok "PASS 2 derives its write-support target set from cqlite-cli/Cargo.toml (not hardcoded)"
else
  bad "PASS 2 does not derive its write-support targets from Cargo.toml (#2039 hardcoding risk)"
fi

# 3. Regression guard: the exact #2039 false-green tell — naming unit_tests as an
#    explicit `--test` target (the old allowlist form) — must NOT return. unit_tests
#    is now covered by the PASS 1 glob-derived set and is never named literally.
if grep -qE -- '--test unit_tests' <<<"$CLI_BLOCK"; then
  bad "cli-tests reintroduced the hardcoded '--test unit_tests' allowlist (#2039 false-green)"
else
  ok "cli-tests no longer names unit_tests explicitly (covered by the PASS 1 glob; #2039 fixed)"
fi

# 4. Quarantine: pre-existing-red targets must be EXCLUDED loudly + tracked, never
#    silently unrun. Assert the QUARANTINE var + the loud runtime notice exist.
if grep -q 'QUARANTINE=' <<<"$CLI_BLOCK" && grep -q 'QUARANTINED pre-existing-red' <<<"$CLI_BLOCK"; then
  ok "cli-tests declares a documented QUARANTINE and prints it loudly at runtime"
else
  bad "cli-tests lacks a documented + loudly-printed QUARANTINE (#2039 honesty rule)"
fi

# 5. Honesty: required-features targets excluded from BOTH passes (delta-export/
#    duckdb-tests/dhat-heap) must also be named loudly at runtime, not left to live
#    only in a source comment (roborev finding, #2039 "silent coverage cap" class).
if grep -q 'excluded_both=' <<<"$CLI_BLOCK" && grep -q 'EXCLUDED from BOTH passes' <<<"$CLI_BLOCK"; then
  ok "cli-tests loudly names required-features targets excluded from BOTH passes"
else
  bad "cli-tests silently drops required-features targets that run in neither pass (#2039 honesty rule)"
fi

# 6. Fail-closed guards: zero test files AND an empty derived default/write-support
#    set must each be a visible error, never a silent pass.
if grep -q 'FAIL-CLOSED' <<<"$CLI_BLOCK" && grep -q 'cli_test_count' <<<"$CLI_BLOCK"; then
  ok "cli-tests fails closed when no cqlite-cli/tests/*.rs files are found"
else
  bad "cli-tests lacks a fail-closed zero-file guard (#2039 hard rule)"
fi
if grep -q 'derived zero default' <<<"$CLI_BLOCK" && grep -q 'derived zero write-support targets' <<<"$CLI_BLOCK"; then
  ok "cli-tests fails closed when either derived target set is empty"
else
  bad "cli-tests lacks a fail-closed empty-target-set guard (#2039)"
fi

# 7. cli-tests now reads real Data.db (real-data CLI integration tests are in the
#    enumerated set), so it must join DATASET_COMPONENTS to be guarded by the
#    dataset preflight (the #646 hazard otherwise).
DS_LINE=$(grep -E '^DATASET_COMPONENTS=' "$GATE")
if grep -qw 'cli-tests' <<<"$DS_LINE"; then
  ok "cli-tests is dataset-guarded (present in DATASET_COMPONENTS)"
else
  bad "cli-tests missing from DATASET_COMPONENTS — dataset preflight would not guard it (#646 hazard)"
fi

# 8. CWD-independence: the enumeration must anchor to REPO_ROOT (roborev finding,
#    #2039), not bare relative `cqlite-cli/tests` / `cqlite-cli/Cargo.toml` paths —
#    unlike the CWD-independent `cargo test --package cqlite-cli` invocations, a
#    relative read would silently break if this component ran from another CWD.
if grep -q 'cli_tests_dir=' <<<"$CLI_BLOCK" && grep -q 'cli_cargo_toml=' <<<"$CLI_BLOCK" \
   && grep -q 'REPO_ROOT' <<<"$CLI_BLOCK"; then
  ok "cli-tests anchors its tests-dir/Cargo.toml reads to REPO_ROOT (CWD-independent)"
else
  bad "cli-tests reads cqlite-cli/tests or Cargo.toml via a bare relative path (#2039 CWD fragility)"
fi

# 9. Behavioral check of the fail-closed zero-file guard logic, in isolation. Uses
#    the SAME snippet shape the gate runs; proves it fails on an empty tests/ dir
#    and counts a populated one, quoted safely.
tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-cli-enum-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

guard() {
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

# 10. Behavioral check of the write-support target-derivation awk against a synthetic
#    Cargo.toml — proves it extracts exactly the targets whose required-features name
#    write-support (and ignores delta-export/duckdb-tests/dhat-heap targets and the
#    [package]/[[bin]] name lines), then UNIONs the self-gated ground-truth targets.
cat >"$tmp/Cargo.toml" <<'EOF'
[package]
name = "cqlite-cli"

[[bin]]
name = "cqlite"

[[test]]
name = "delta_export_tests"
required-features = ["delta-export", "duckdb-tests"]

[[test]]
name = "cli_dml_integration_tests"
required-features = ["write-support"]

[[test]]
name = "issue_1581_query_stream_memory"
required-features = ["dhat-heap"]

[[test]]
name = "new_write_target"
path = "tests/new_write_target.rs"
required-features = ["write-support"]
EOF

derive() {
  { awk -F\" "/^[[:space:]]*name[[:space:]]*=/{cur=\$2} /^[[:space:]]*required-features[[:space:]]*=/ && /write-support/{print cur}" "$1"; \
    printf "%s\n" write_readback_content_tests graceful_shutdown_tests; } | sort -u
}

got=$(derive "$tmp/Cargo.toml" | tr '\n' ' ' | sed 's/ *$//')
want="cli_dml_integration_tests graceful_shutdown_tests new_write_target write_readback_content_tests"
if [ "$got" = "$want" ]; then
  ok "write-support derivation extracts declared write-support targets + ground truth (a NEW write-support file is auto-covered)"
else
  bad "write-support derivation wrong: got '$got' want '$want'"
fi

# The derivation must NOT pick up delta-export/duckdb-tests/dhat-heap-only targets.
if grep -qw 'delta_export_tests' <<<"$got" || grep -qw 'issue_1581_query_stream_memory' <<<"$got"; then
  bad "write-support derivation wrongly included a delta/duckdb/dhat-only target"
else
  ok "write-support derivation excludes delta-export/duckdb-tests/dhat-heap-only targets"
fi

# 11. Behavioral check of the Pass-1 default set subtraction (glob - required-features
#    - quarantine). Build a synthetic tests/ + reuse the synthetic Cargo.toml above,
#    then reproduce the gate's derivation and assert the excluded targets are gone and
#    a plain new read-only file is INCLUDED.
mkdir -p "$tmp/pass1/tests"
for n in unit_tests read_sstable_stdout_tests a_new_readonly_test \
         delta_export_tests cli_dml_integration_tests issue_1581_query_stream_memory \
         new_write_target comprehensive_select_test table_snapshot_tests; do
  : >"$tmp/pass1/tests/$n.rs"
done
QUARANTINE_T="comprehensive_select_test table_snapshot_tests"
all_t=$(for f in "$tmp"/pass1/tests/*.rs; do basename "$f" .rs; done | sort -u)
rf_t=$(awk -F\" "/^[[:space:]]*name[[:space:]]*=/{cur=\$2} /^[[:space:]]*required-features[[:space:]]*=/{if(cur!=\"\")print cur}" "$tmp/Cargo.toml" | sort -u)
default_t=$(printf "%s\n" "$all_t" | grep -vxF -f <(printf "%s\n" $rf_t $QUARANTINE_T))

# Required-features targets and quarantined targets must be EXCLUDED from Pass 1.
excluded_ok=1
for n in delta_export_tests cli_dml_integration_tests issue_1581_query_stream_memory new_write_target comprehensive_select_test table_snapshot_tests; do
  grep -qx "$n" <<<"$default_t" && excluded_ok=0
done
if [ "$excluded_ok" -eq 1 ]; then
  ok "Pass-1 subtraction excludes required-features + quarantined targets"
else
  bad "Pass-1 subtraction leaked a required-features or quarantined target into the default set"
fi
# A plain new read-only file must be INCLUDED (auto-coverage of future files).
if grep -qx 'a_new_readonly_test' <<<"$default_t" && grep -qx 'read_sstable_stdout_tests' <<<"$default_t"; then
  ok "Pass-1 subtraction includes read-only targets incl. a NEW file (auto-coverage)"
else
  bad "Pass-1 subtraction dropped a read-only target that should run"
fi

# 12. Behavioral check of the zero-tests guard (roborev finding on #2039): a target
#     whose body is entirely `#[cfg(feature = "write-support")]`-gated but which does
#     NOT declare required-features would compile+run 0 tests in Pass 1 and never
#     appear in the derived Pass-2 set unless it happens to be one of the two
#     hardcoded ground-truth names — silently zero coverage forever for a THIRD such
#     file. Extract check_no_unexpected_zero_tests() VERBATIM from the gate (source
#     of truth, no re-typed copy to drift) and drive it against synthetic cargo-style
#     "Running tests/<name>.rs" / "test result:" log text.
FUNC_SRC=$(awk '/^  check_no_unexpected_zero_tests\(\) \{/{f=1} f{print} f&&/^  \}$/{exit}' "$GATE")
if [ -z "$FUNC_SRC" ]; then
  bad "could not extract check_no_unexpected_zero_tests() from $GATE"
else
  ok "extracted check_no_unexpected_zero_tests() from the gate for behavioral testing"
  eval "$FUNC_SRC"

  mkdir -p "$tmp/zg"
  cat >"$tmp/zg/pass1_ok.log" <<'LOG'
     Running tests/unit_tests.rs (target/debug/deps/unit_tests-abc)

running 5 tests
test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.10s

     Running tests/write_readback_content_tests.rs (target/debug/deps/write_readback_content_tests-abc)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
LOG
  if check_no_unexpected_zero_tests "Pass 1 (default)" "$tmp/zg/pass1_ok.log" write_readback_content_tests graceful_shutdown_tests; then
    ok "zero-tests guard: known-0 ground-truth target does NOT false-positive in Pass 1"
  else
    bad "zero-tests guard: false-positived on the known-0 Pass-1 ground-truth target"
  fi

  cat >"$tmp/zg/pass1_bad.log" <<'LOG'
     Running tests/unit_tests.rs (target/debug/deps/unit_tests-abc)

running 5 tests
test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.10s

     Running tests/sneaky_write_support_test.rs (target/debug/deps/sneaky_write_support_test-abc)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
LOG
  if check_no_unexpected_zero_tests "Pass 1 (default)" "$tmp/zg/pass1_bad.log" write_readback_content_tests graceful_shutdown_tests; then
    bad "zero-tests guard: did NOT catch a THIRD write-support-#[cfg]-gated target running 0 tests"
  else
    ok "zero-tests guard: catches a THIRD unexpected 0-test target (the exact gap roborev found)"
  fi

  cat >"$tmp/zg/pass2_bad.log" <<'LOG'
     Running tests/cli_dml_integration_tests.rs (target/debug/deps/cli_dml_integration_tests-abc)

running 12 tests
test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.34s

     Running tests/write_readback_content_tests.rs (target/debug/deps/write_readback_content_tests-abc)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
LOG
  if check_no_unexpected_zero_tests "Pass 2 (write-support)" "$tmp/zg/pass2_bad.log"; then
    bad "zero-tests guard: Pass 2 did NOT fail on a 0-test target (nothing is allowed 0 there)"
  else
    ok "zero-tests guard: Pass 2 fails on ANY 0-test target (no allowed-zero exceptions there)"
  fi

  cat >"$tmp/zg/pass2_ok.log" <<'LOG'
     Running tests/cli_dml_integration_tests.rs (target/debug/deps/cli_dml_integration_tests-abc)

running 12 tests
test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 1.34s

     Running tests/write_readback_content_tests.rs (target/debug/deps/write_readback_content_tests-abc)

running 2 tests
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.56s
LOG
  if check_no_unexpected_zero_tests "Pass 2 (write-support)" "$tmp/zg/pass2_ok.log"; then
    ok "zero-tests guard: an all-green Pass 2 passes cleanly (no false-positive)"
  else
    bad "zero-tests guard: false-positived on an all-green Pass 2"
  fi

  # Scenario E (roborev finding, #2039): a target whose tests are ALL #[ignore]d
  # ALSO reports "0 passed; 0 failed" — a legitimate, unrelated shape (a future
  # manual/slow test suite) that the guard must NEVER fault, since it is not the
  # write-support-#[cfg]-gated-with-no-required-features shape it exists to catch.
  cat >"$tmp/zg/pass1_all_ignored.log" <<'LOG'
     Running tests/unit_tests.rs (target/debug/deps/unit_tests-abc)

running 5 tests
test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.10s

     Running tests/manual_slow_suite.rs (target/debug/deps/manual_slow_suite-abc)

running 3 tests
test result: ok. 0 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out; finished in 0.00s
LOG
  if check_no_unexpected_zero_tests "Pass 1 (default)" "$tmp/zg/pass1_all_ignored.log" write_readback_content_tests graceful_shutdown_tests; then
    ok "zero-tests guard: an all-#[ignore]d target does NOT trip the guard (distinguished from a truly-empty run)"
  else
    bad "zero-tests guard: false-positived on an all-#[ignore]d target (roborev regression)"
  fi
fi

echo
echo "cli-tests-enum self-test: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
