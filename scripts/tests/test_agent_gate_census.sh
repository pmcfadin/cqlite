#!/usr/bin/env bash
# test_agent_gate_census.sh — the #3625 component-census guard.
#
# SUBJECT: the census subsystem in scripts/agent-gate.sh, exercised AS CODE. Every
# function under test is EXTRACTED FROM THE SHIPPED GATE and run — never re-implemented
# here, because a test that re-implements its subject can only prove that the copy works
# (the idiom of test_cargo_output_parsers.sh and test_agent_gate_feature_matrix_annotation.sh).
#
# WHY IT EXISTS. A component line reading `PASS (0s)` is indistinguishable, in the SUMMARY
# block a closer pastes, from a component that did nothing. #3625 replaces the proxy (a
# duration) with the thing itself (a count), and couples the verdict: a PASS whose measured
# subject count is ZERO is recorded as VACUOUS.
#
# THE FOUR ACCEPTANCE CRITERIA AND WHERE THEY ARE ASSERTED:
#   AC1 every 0s-capable component's line carries an affirmative count  -> (A), (D), (F)
#   AC2 a component that verified nothing cannot report PASS            -> (E), (F)
#   AC3 a PLANT shows the new state fires AND NAMES the component       -> (F)
#   AC4 the two-run comparison is explained                             -> (C) pins the
#       measured oracle it rests on: `Executable ` survives a WARM `--no-run` and
#       `test result:` survives a WARM `cargo test`, so a 0s lane really did re-verify.
#
# A BARE RED IS NOT EVIDENCE. Section (F) requires the emitted block to NAME the component
# that verified nothing — an unrelated breakage produces an identical exit code — and it
# carries a POSITIVE CONTROL on the SAME lane, differing in ONE property (the shim's
# output), so the red proves the census and not the harness.
#
# NO TEST-ONLY SEAM IS ADDED TO THE GATE. Section (F) drives the REAL `agent-gate.sh
# --only <component>` with a PATH shim `cargo`, the established idiom here; a settable
# env seam in the gate is one more thing a real invoker can set (#3312 job 27's corollary).
#
# Hermetic: temp dir only. No datasets, no network, no gh, no real cargo build.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GATE="$REPO_ROOT/scripts/agent-gate.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

tmp="$(mktemp -d)"
CENSUS_REACHED_END=0
_on_exit() {
  local rc=$?
  rm -rf "$tmp"
  if [ "$CENSUS_REACHED_END" -ne 1 ]; then
    printf 'FAIL - the guard ABORTED before its terminal tally (set -u, a syntax error, or a missing tool). Whatever the exit status was, this run certified nothing.\n' >&2
    exit 1
  fi
  exit "$rc"
}
trap _on_exit EXIT

[ -r "$GATE" ] || { echo "FAIL - cannot read $GATE"; exit 1; }

# ---------------------------------------------------------------------------
# EXTRACTION. Fail-closed: an unextractable function is a FAIL, never a skip, or this
# guard could pass having tested nothing.
# ---------------------------------------------------------------------------
for fn in _ansi_stripped_log _census_sidecar _census_kind _census_write _census_read \
          _census_declare _census_libtest_tally _census_compile_tally \
          _census_driver_tally _census_measure_kind _census_measure \
          _census_scoped_record _python_tier_ran \
          _census_status_for _census_finalize _census_record _census_annotate \
          census_summary_line _status_is_nonfailing; do
  src=$(sed -n "/^$fn() {/,/^}$/p" "$GATE")
  if [ -z "$src" ]; then
    echo "FAIL - could not extract $fn from $GATE — renamed or reshaped; this guard must not pass having tested nothing (#3625)" >&2
    exit 1
  fi
  eval "$src" || { echo "FAIL - extracted $fn does not parse" >&2; exit 1; }
done

# ---------------------------------------------------------------------------
# (A) COMPLETENESS — every name that can reach a SUMMARY component line resolves to a
#     declared census kind, and the kind is a member of the CLOSED set.
#
#     THE NAME DOMAIN IS DERIVED FROM THE EMIT PATH, not from COMPONENTS alone: the
#     dynamic `NAMES+=("<literal>")` appends in the run_delta_* helpers (node-tests,
#     shell-selftests, scoped-tests) also render component lines, and enumerating only
#     COMPONENTS is how #3453's job-277-F2 defect escaped one directory over.
# ---------------------------------------------------------------------------
components_line=$(grep -m1 '^COMPONENTS=(' "$GATE")
if [ -z "$components_line" ]; then
  bad "A0: could not find COMPONENTS=( in $GATE"
  comps_arr=()
else
  comps=${components_line#COMPONENTS=(}
  comps=${comps%)}
  # shellcheck disable=SC2206  # deliberate word-split of the source array literal
  comps_arr=($comps)
  if [ "${#comps_arr[@]}" -lt 30 ]; then
    bad "A0: parsed only ${#comps_arr[@]} components out of COMPONENTS — the parse looks wrong, so the completeness census below would prove almost nothing"
  else
    ok "A0: parsed ${#comps_arr[@]} components from the gate's COMPONENTS array"
  fi
fi
# Two derived sources, because the emit path has two of them and enumerating only one is
# how #3453's job-277-F2 escaped one directory over — MEASURED here, not theorised: the
# `record_result "<literal>"` half was missing from the first draft of this file, and the
# #2926 `tree-selftest` hook (which records a verdict under a name no static set carries)
# went undeclared and rendered its row FAIL in a real self-test block.
#   (1) the run_delta_* helpers' `NAMES+=("<literal>")` appends;
#   (2) any `record_result "<literal>"` call, from a NON-COMMENT line.
# `NAMES+=("$var")` / `record_result "$var"` sites are the COMPONENTS-driven paths already
# covered by comps_arr, so nothing is silently dropped.
dyn_names=$( { grep -oE 'NAMES\+=\("[a-z0-9][a-z0-9-]*"\)' "$GATE" | sed -E 's/.*\("(.*)"\)/\1/'
               grep -E '^[^#]*record_result "[a-z0-9][a-z0-9-]*"' "$GATE" \
                 | sed -E 's/.*record_result "([a-z0-9][a-z0-9-]*)".*/\1/'; } | sort -u)
dyn_n=$(printf '%s\n' "$dyn_names" | grep -c . || true)
if [ "${dyn_n:-0}" -lt 4 ]; then
  bad "A0b: derivation of the dynamic summary-name set from $GATE yielded ${dyn_n:-0} names (expected at least the 4 known: node-tests, scoped-tests, shell-selftests, tree-selftest) — the domain would silently shrink toward COMPONENTS, which is exactly how an undeclared name reaches a block"
else
  ok "A0b: dynamic summary-name set DERIVED from the emit path: $dyn_n name(s) [$(printf '%s' "$dyn_names" | tr '\n' ' ')]"
fi

undeclared=()
badkind=()
n_libtest=0; n_compile=0; n_both=0; n_self=0; n_indirect=0; n_runtime=0; n_gap=0
for c in ${comps_arr[@]+"${comps_arr[@]}"} $dyn_names; do
  if k=$(_census_kind "$c"); then
    case "$k" in
      libtest)  n_libtest=$((n_libtest + 1)) ;;
      compile)  n_compile=$((n_compile + 1)) ;;
      both)     n_both=$((n_both + 1)) ;;
      self:?*)     n_self=$((n_self + 1)) ;;
      indirect:?*) n_indirect=$((n_indirect + 1)) ;;
      runtime:?*)  n_runtime=$((n_runtime + 1)) ;;
      gap:?*)      n_gap=$((n_gap + 1)) ;;
      *)        badkind+=("$c=$k") ;;
    esac
  else
    undeclared+=("$c")
  fi
done
if [ "${#undeclared[@]}" -eq 0 ]; then
  ok "A1: every COMPONENTS name + every DERIVED dynamic summary name resolves to a declared census kind"
else
  bad "A1: undeclared in _census_kind — a component cannot join the gate with a blank census (#3625): ${undeclared[*]}"
fi
if [ "${#badkind[@]}" -eq 0 ]; then
  ok "A2: every declared kind is in the CLOSED set (libtest=$n_libtest compile=$n_compile both=$n_both self=$n_self indirect=$n_indirect runtime=$n_runtime gap=$n_gap)"
else
  bad "A2: declared with a kind outside the closed set {libtest,compile,both,self:<unit>,indirect:<driver>,runtime:<why>,gap:<reason>}: ${badkind[*]}"
fi
# The subject the ISSUE names must actually be measured, not gapped away. These are the
# components the two-run comparison caught at 0s, plus the lane doctrine already records
# as "reports PASS (0s) warm, so presence proves nothing".
issue_subjects="tombstones-scan arrow-parity-guard format-compat integration-tests query-semantics-oracle feature-iso-parquet"
gapped=()
for c in $issue_subjects; do
  k=$(_census_kind "$c") || k="(undeclared)"
  case "$k" in libtest|compile|both) ;; *) gapped+=("$c=$k") ;; esac
done
if [ "${#gapped[@]}" -eq 0 ]; then
  ok "A3: every component named in issue #3625's two-run table has a MEASURED kind, not a declared gap"
else
  bad "A3: the issue's own subjects are gapped rather than measured: ${gapped[*]}"
fi
# An UNKNOWN name must be REFUSED, never guessed at.
if _census_kind a-component-that-does-not-exist >/dev/null 2>&1; then
  bad "A4: _census_kind invented a kind for an unknown component instead of refusing"
else
  ok "A4: an unknown component name is a fail-closed refusal from _census_kind"
fi

# ---------------------------------------------------------------------------
# (B) NEVER BLANK. The annotation contract, same as _fm_annotate's: every name renders
#     SOMETHING, and a name with no record renders a NAMED state rather than a silence.
# ---------------------------------------------------------------------------
LOG_DIR="$tmp/emptylogs"; mkdir -p "$LOG_DIR"
blank=()
notbrace=()
for c in ${comps_arr[@]+"${comps_arr[@]}"} $dyn_names a-component-that-does-not-exist; do
  a=$(_census_annotate "$c")
  [ -n "$a" ] || blank+=("$c")
  case "$a" in '{'*'}') ;; *) notbrace+=("$c=$a") ;; esac
done
if [ "${#blank[@]}" -eq 0 ] && [ "${#notbrace[@]}" -eq 0 ]; then
  ok "B1: no name renders a BLANK or malformed census annotation (including an undeclared one)"
else
  bad "B1: blank: ${blank[*]:-(none)}; malformed: ${notbrace[*]:-(none)}"
fi
case "$(_census_annotate a-component-that-does-not-exist)" in
  *'UNDECLARED'*'a-component-that-does-not-exist'*)
    ok "B2: an undeclared component's annotation NAMES the component, rather than reading as an ordinary gap" ;;
  *) bad "B2: got '$(_census_annotate a-component-that-does-not-exist)'" ;;
esac
# A declared gap must PRINT its reason on every run — a gap nobody sees is a silence.
case "$(_census_annotate fmt)" in
  *'no census'*'fmt --all --check'*)
    ok "B3: a DECLARED GAP renders its reason, so the reduction in coverage is visible in the block" ;;
  *) bad "B3: the fmt gap does not print its declared reason: $(_census_annotate fmt)" ;;
esac

# ---------------------------------------------------------------------------
# (C) THE PARSERS ARE COLOUR-IMMUNE AT THE PARSE SITE (#3400) — and this section is
#     ALSO where AC4's oracle is pinned.
#
#     FIXTURE PROVENANCE. Every escape below is a REAL ESC byte injected via
#     `printf '\033'`, transcribed from a `cat -v` capture of real cargo output under
#     CARGO_TERM_COLOR=always REDIRECTED TO A FILE (measured for #3625 on 2026-09-01,
#     matching the #3400 capture): the reset lands BETWEEN the status word and the
#     payload — `Executable<ESC>[0m unittests src/lib.rs` — so `Executable ` followed by
#     a payload never appears, while `$1 == "Executable"` after a strip does.
#
#     THE TWO DIRECTIONS DIFFER AND BOTH MATTER HERE:
#       * `Executable ` is a CARGO STATUS WORD and IS coloured. Parsed raw, the count is
#         0 — which for this subsystem means a MEASURED ZERO, i.e. a legitimately green
#         `--no-run` lane rendered VACUOUS. A FALSE RED, and the worst failure this
#         mechanism can have.
#       * `test result:` and `Summary [...] N tests run:` are the HARNESS's own text and
#         carry no escapes, measured — but that is a property of cargo's plumbing, not of
#         this code, which is exactly the coupling that left the cli-tests zero-tests
#         guard inert for months. Both are normalised anyway.
# ---------------------------------------------------------------------------
ESC="$(printf '\033')"
mk_log() { # mk_log <colour|plain> <outfile>
  local mode="$1" out="$2" pre="" post=""
  if [ "$mode" = colour ]; then pre="${ESC}[1m${ESC}[92m"; post="${ESC}[0m"; fi
  {
    printf '%s    Finished%s `test` profile [unoptimized + debuginfo] target(s) in 0.01s\n' "$pre" "$post"
    printf '%s  Executable%s unittests src/lib.rs (target/debug/deps/dw-e20e08564b117d3a)\n' "$pre" "$post"
    printf '%s  Executable%s tests/foo.rs (target/debug/deps/foo-1d5149cf0c5ed499)\n' "$pre" "$post"
    printf '%s     Running%s tests/foo.rs (target/debug/deps/foo-1d5149cf0c5ed499)\n' "$pre" "$post"
    printf '\nrunning 5 tests\n\n'
    printf 'test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n\n'
    printf '%s   Doc-tests%s dw\n' "$pre" "$post"
    printf '\nrunning 0 tests\n\n'
    printf 'test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n'
  } >"$out"
}
mk_log colour "$tmp/c.log"
mk_log plain  "$tmp/p.log"
esc_c=$(LC_ALL=C tr -cd '\033' <"$tmp/c.log" | wc -c | tr -d ' ')
esc_p=$(LC_ALL=C tr -cd '\033' <"$tmp/p.log" | wc -c | tr -d ' ')
if [ "$esc_c" -gt 0 ] && [ "$esc_p" -eq 0 ]; then
  ok "C0: fixture provenance — the coloured log carries $esc_c real ESC bytes and the plain one 0; without this the rest of (C) would be vacuous"
else
  bad "C0: fixture provenance — expected ESC>0 coloured / 0 plain, got $esc_c / $esc_p"
fi
if grep -q 'Executable unittests' "$tmp/p.log" && ! grep -q 'Executable unittests' "$tmp/c.log"; then
  ok "C0b: the literal 'Executable unittests' is present in the plain log and ABSENT in the coloured one — the reset really lands between status word and payload"
else
  bad "C0b: the fixture does not reproduce the colour placement, so C1's RED means nothing"
fi

raw_bins=$(_census_compile_tally "$tmp/c.log")
if [ "$raw_bins" = 0 ]; then
  ok "C1 (RED, pinned defect): parsing the COLOURED log RAW counts 0 test binaries — the measured-ZERO that would render a healthy --no-run lane VACUOUS"
else
  bad "C1 (RED): the coloured log parsed raw already yields $raw_bins — the fixture no longer reproduces the #3400 hazard, so C2 proves nothing"
fi
src=$(_ansi_stripped_log "$tmp/c.log") || src=""
if [ -n "$src" ] && [ "$(_census_compile_tally "$src")" = 2 ]; then
  ok "C2 (GREEN): routed through _ansi_stripped_log the SAME coloured log counts 2 test binaries — the strip carries the correctness"
else
  bad "C2 (GREEN): expected 2 test binaries after the strip, got '$(_census_compile_tally "${src:-/dev/null}")'"
fi
if [ "$(_census_libtest_tally "$tmp/p.log")" = "5 2" ]; then
  ok "C3: the libtest tally SUMS every result line (5 + a 0-passed doc-test line = 5 across 2 lines) — a normal 0-passed doc-test cannot by itself make a lane read vacuous"
else
  bad "C3: expected '5 2' from the libtest tally, got '$(_census_libtest_tally "$tmp/p.log")'"
fi
# cargo-nextest reports a SUMMARY, not per-binary `test result:` lines. core-tests' nextest
# branch runs nextest for the unit suite AND `cargo test --doc` for the doc-tests, so both
# shapes appear in one log and both must count.
printf '     Summary [   4.567s] 3562 tests run: 3562 passed, 2 skipped\ntest result: ok. 12 passed; 0 failed; 0 ignored\n' > "$tmp/nx.log"
if [ "$(_census_libtest_tally "$tmp/nx.log")" = "3574 2" ]; then
  ok "C4: cargo-nextest's 'N tests run:' summary is counted alongside libtest's 'test result:' (3562 + 12 = 3574) — core-tests' nextest branch is not silently uncounted"
else
  bad "C4: expected '3574 2' from a nextest+doctest log, got '$(_census_libtest_tally "$tmp/nx.log")'"
fi
# AC4's ORACLE, pinned as a fixture rather than as prose: a WARM `cargo test --no-run`
# still prints an `Executable ` line per binary, and a WARM `cargo test` still prints
# `test result:`. That is what makes a 0s lane's re-verification measurable at all.
printf '    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.01s\n  Executable unittests src/lib.rs (target/debug/deps/dw-e20e08564b117d3a)\n' > "$tmp/warm-norun.log"
printf '     Running unittests src/lib.rs (target/debug/deps/dw-e20e08564b117d3a)\ntest result: ok. 3 passed; 0 failed; 0 ignored\n' > "$tmp/warm-run.log"
if [ "$(_census_compile_tally "$tmp/warm-norun.log")" = 1 ] \
   && [ "$(_census_libtest_tally "$tmp/warm-run.log")" = "3 1" ]; then
  ok "C5 (AC4 oracle): a WARM --no-run log still yields 1 binary and a WARM run log still yields 3 tests — cargo caches COMPILATION, never EXECUTION, so a 0s lane has something affirmative to say"
else
  bad "C5 (AC4 oracle): warm shapes did not measure (norun='$(_census_compile_tally "$tmp/warm-norun.log")' run='$(_census_libtest_tally "$tmp/warm-run.log")')"
fi

# ---------------------------------------------------------------------------
# (D) THE MEASURER'S STATE MACHINE, driven through the REAL _census_measure over
#     synthetic component logs in a scratch LOG_DIR.
# ---------------------------------------------------------------------------
LOG_DIR="$tmp/logs"; mkdir -p "$LOG_DIR"
state_of() { printf '%s' "${1%% *}"; }
# tombstones-scan is `libtest`; feature-iso-parquet is `compile`; integration-tests is `both`.
cp "$tmp/c.log" "$LOG_DIR/tombstones-scan.log"
got=$(_census_measure tombstones-scan PASS)
case "$got" in
  'COUNT 5 tests passed'*) ok "D1: libtest over a COLOURED log -> COUNT 5 tests passed (the measurer routes through the strip, not just the unit-tested parser)" ;;
  *) bad "D1: got '$got'" ;;
esac
printf 'running 0 tests\ntest result: ok. 0 passed; 0 failed; 0 ignored\n' > "$LOG_DIR/tombstones-scan.log"
got=$(_census_measure tombstones-scan PASS)
case "$got" in
  'ZERO tests'*'every one of them reporting 0 passed'*) ok "D2: libtest whose only result line reports 0 passed -> ZERO, naming what was measured" ;;
  *) bad "D2: got '$got'" ;;
esac
printf 'nothing that looks like a test run at all\n' > "$LOG_DIR/tombstones-scan.log"
got=$(_census_measure tombstones-scan PASS)
case "$got" in
  'ZERO tests'*'no libtest or nextest result line'*) ok "D3: a libtest lane whose log carries NO result line at all -> ZERO, distinguishing 'no binary reported a tally' from 'a binary reported zero'" ;;
  *) bad "D3: got '$got'" ;;
esac
cp "$tmp/c.log" "$LOG_DIR/feature-iso-parquet.log"
got=$(_census_measure feature-iso-parquet PASS)
case "$got" in
  'COUNT 2 test binaries built/verified') ok "D4: compile -> COUNT 2 test binaries built/verified (the honest subject of a --no-run lane)" ;;
  *) bad "D4: got '$got'" ;;
esac
printf '    Finished `test` profile in 0.01s\n' > "$LOG_DIR/feature-iso-parquet.log"
got=$(_census_measure feature-iso-parquet PASS)
case "$got" in
  'ZERO test binaries'*) ok "D5: a compile lane with no Executable status line -> ZERO test binaries" ;;
  *) bad "D5: got '$got'" ;;
esac
cp "$tmp/c.log" "$LOG_DIR/integration-tests.log"
got=$(_census_measure integration-tests PASS)
case "$got" in
  'COUNT 5 tests passed and 2 test binaries built/verified') ok "D6: both -> reports BOTH subjects" ;;
  *) bad "D6: got '$got'" ;;
esac
printf 'Finished in 0.01s\n' > "$LOG_DIR/integration-tests.log"
got=$(_census_measure integration-tests PASS)
case "$got" in
  'ZERO tests and test binaries'*) ok "D7: both is ZERO only when BOTH subjects measure zero — one non-zero subject is enough to affirm the lane" ;;
  *) bad "D7: got '$got'" ;;
esac
# A LEGITIMATELY-GREEN both-lane that only compiled must NOT read vacuous (scoped-tests
# degrades to --no-run for a test-only crate with no changed --test target).
printf '  Executable tests/foo.rs (target/debug/deps/foo-1)\n' > "$LOG_DIR/integration-tests.log"
got=$(_census_measure integration-tests PASS)
case "$got" in
  'COUNT 0 tests passed and 1 test binaries built/verified') ok "D8: a both-lane that only COMPILED still affirms its binaries — a --no-run-only pass is not vacuous" ;;
  *) bad "D8: got '$got'" ;;
esac
# NOT-MEASURED: an unreadable log. NEVER a fallback to a count, and never fatal.
rm -f "$LOG_DIR/format-compat.log"
got=$(_census_measure format-compat PASS)
case "$got" in
  'NOT-MEASURED could not read or ANSI-normalise'*) ok "D9: an unreadable component log -> NOT-MEASURED naming the cause, never a count and never a silent zero" ;;
  *) bad "D9: got '$got'" ;;
esac
# A non-PASS component has no PASS to affirm.
printf 'test result: ok. 5 passed\n' > "$LOG_DIR/format-compat.log"
got=$(_census_measure format-compat SKIP)
case "$got" in
  'NOT-APPLICABLE component ended SKIP'*) ok "D10: a SKIPped component records NOT-APPLICABLE — the census makes no claim about a PASS that does not exist" ;;
  *) bad "D10: got '$got'" ;;
esac
# A declared gap prints its reason and is never measured from a log.
got=$(_census_measure clippy PASS)
case "$got" in
  'GAP cargo clippy emits a per-crate tally only COLD'*) ok "D11: a declared gap records GAP + its reason, regardless of what its log happens to contain" ;;
  *) bad "D11: got '$got'" ;;
esac
# A `self:` component that recorded nothing is a RECORDING GAP, named — never a count.
got=$(_census_measure node-tests PASS)
case "$got" in
  'NOT-MEASURED'*"'node-tests' records its own subject count and recorded none"*) ok "D12: a self: component with no record is NOT-MEASURED naming itself, not a licence to claim a count" ;;
  *) bad "D12: got '$got'" ;;
esac
_census_declare node-tests 4 'changed jest test file(s)'
got=$(_census_measure node-tests PASS)
case "$got" in
  'COUNT 4 changed jest test file(s)') ok "D13: _census_declare records a self: component's own affirmative count" ;;
  *) bad "D13: got '$got'" ;;
esac
_census_declare node-tests 0 'changed jest test file(s)'
got=$(_census_measure node-tests PASS)
case "$got" in
  'ZERO changed jest test file(s)') ok "D14: _census_declare records a self-reported 0 as ZERO — an affirmative measurement of nothing is still nothing" ;;
  *) bad "D14: got '$got'" ;;
esac
_census_declare node-tests "not-a-number" 'changed jest test file(s)'
got=$(_census_measure node-tests PASS)
case "$got" in
  'NOT-MEASURED the component offered a non-numeric subject count') ok "D15: a non-numeric self-reported count is NOT-MEASURED, never coerced to 0 (which would be a fabricated vacuity) and never to a pass" ;;
  *) bad "D15: got '$got'" ;;
esac
got=$(_census_measure a-component-that-does-not-exist PASS)
case "$got" in
  'UNDECLARED no census kind is declared'*) ok "D16: an undeclared component is refused BY NAME at measurement time" ;;
  *) bad "D16: got '$got'" ;;
esac
# ---- indirect:<driver>. python-bindings is the component the ISSUE itself holds up as
# the contrast that already answered the question, so its tally is lifted into the block.
printf 'Compiling cqlite-py v0.1.0\n....ss..x.\n576 passed, 61 skipped, 1 xfailed in 62.30s\n' > "$LOG_DIR/python-bindings.log"
got=$(_census_measure python-bindings PASS)
case "$got" in
  'COUNT 576 pytest tests passed') ok "D17: indirect:pytest lifts the driver's own tally (576 passed) out of the component log and into the census" ;;
  *) bad "D17: got '$got'" ;;
esac
printf 'no tests ran in 0.01s\n' > "$LOG_DIR/python-bindings.log"
got=$(_census_measure python-bindings PASS)
case "$got" in
  'ZERO pytest tests'*) ok "D18: pytest's own affirmative 'no tests ran' is ZERO — matched explicitly, not inferred from a missing count" ;;
  *) bad "D18: got '$got'" ;;
esac
printf 'maturin build output and nothing that looks like a pytest summary\n' > "$LOG_DIR/python-bindings.log"
got=$(_census_measure python-bindings PASS)
case "$got" in
  'NOT-MEASURED no pytest tally found'*) ok "D19: an ABSENT driver tally is NOT-MEASURED, never ZERO — a third-party output-format change must not red a healthy lane (the one rule that differs from the cargo kinds)" ;;
  *) bad "D19: got '$got'" ;;
esac
printf 'Test Suites: 27 passed, 27 total\nTests:       1 skipped, 122 passed, 123 total\n' > "$LOG_DIR/node-bindings.log"
got=$(_census_measure node-bindings PASS)
case "$got" in
  'COUNT 122 jest tests passed') ok "D20: indirect:jest reads the 'Tests:' summary line (122 passed), not the suite line above it" ;;
  *) bad "D20: got '$got'" ;;
esac
printf 'Tests:       0 total\n' > "$LOG_DIR/node-bindings.log"
got=$(_census_measure node-bindings PASS)
case "$got" in
  'ZERO jest tests'*) ok "D21: a jest 'Tests:' line PRESENT with no passing count is a present-and-zero tally -> ZERO" ;;
  *) bad "D21: got '$got'" ;;
esac
# ---- PRESENT-AND-ZERO IN EVERY SPELLING THE DRIVER USES (roborev job 360, finding 1).
# The first version keyed on the word `passed`, so every pytest terminal summary that
# reports zero passed WITHOUT that word — skipped-only, xfailed-only, deselected-only,
# errors-only — fell into the ABSENT branch and was therefore NOT-MEASURED, which
# PRESERVES PASS. A suite whose every test was skipped is exactly the vacuous pass this
# subsystem exists to catch. Driven through _census_measure AND _census_status_for, so the
# VERDICT COUPLING is asserted too and not merely the parser.
zero_spellings='61 skipped in 1.20s|1 xfailed in 0.10s|2 deselected in 0.02s|3 errors in 0.40s|no tests ran in 0.01s'
z_bad=()
z_n=0
old_ifs=$IFS
IFS='|'
for spelling in $zero_spellings; do
  IFS=$old_ifs
  z_n=$((z_n + 1))
  printf 'Compiling cqlite-py v0.1.0\n%s\n' "$spelling" > "$LOG_DIR/python-bindings.log"
  g=$(_census_measure python-bindings PASS)
  st=$(_census_status_for PASS "$g")
  case "$g|$st" in
    'ZERO pytest tests'*'|VACUOUS') ;;
    *) z_bad+=("[$spelling]->$g/$st") ;;
  esac
  IFS='|'
done
IFS=$old_ifs
if [ "$z_n" -ne 5 ]; then
  bad "D23: only $z_n of the 5 zero-passed pytest spellings were exercised — the loop is not iterating, so a green here would certify nothing"
elif [ "${#z_bad[@]}" -eq 0 ]; then
  ok "D23 (roborev job 360 F1): all 5 pytest terminal summaries reporting ZERO passed — skipped-only, xfailed-only, deselected-only, errors-only, 'no tests ran' — measure ZERO and couple to VACUOUS, not to a PASS-preserving NOT-MEASURED"
else
  bad "D23: a present-and-zero pytest summary did not reach ZERO/VACUOUS: ${z_bad[*]}"
fi
# ...and the RECOGNISER must stay OFF other harnesses' output, or D23's widening would
# start attributing rust tests (or a cargo build's duration) to pytest. Both lines below
# carry a ` in <n>s` tail, which is exactly why the recogniser requires an outcome pair
# TOO and excludes libtest's line by name.
n_bad=()
printf '    Finished `dev` profile [unoptimized + debuginfo] target(s) in 41.05s\n' > "$LOG_DIR/python-bindings.log"
case "$(_census_measure python-bindings PASS)" in 'NOT-MEASURED no pytest tally'*) ;; *) n_bad+=("cargo-Finished-line-recognised-as-a-pytest-summary") ;; esac
printf 'test result: ok. 5 passed; 0 failed; 0 ignored; finished in 0.00s\n' > "$LOG_DIR/python-bindings.log"
case "$(_census_measure python-bindings PASS)" in 'NOT-MEASURED no pytest tally'*) ;; *) n_bad+=("libtest-test-result-line-counted-as-pytest") ;; esac
printf '    Finished `dev` in 41.05s\ntest result: ok. 9 passed; finished in 0.0s\n576 passed, 61 skipped in 62.30s\n' > "$LOG_DIR/python-bindings.log"
case "$(_census_measure python-bindings PASS)" in 'COUNT 576 pytest tests passed') ;; *) n_bad+=("mixed-log-did-not-yield-the-pytest-count-alone") ;; esac
printf '576 passed in 302.30s (0:05:02)\n' > "$LOG_DIR/python-bindings.log"
case "$(_census_measure python-bindings PASS)" in 'COUNT 576 pytest tests passed') ;; *) n_bad+=("pytest-7-long-duration-form-unrecognised") ;; esac
if [ "${#n_bad[@]}" -eq 0 ]; then
  ok "D24: the widened recogniser stays OFF cargo's 'Finished … in 41.05s' and libtest's 'test result: … finished in 0.00s' (both carry a duration), and still reads the pytest tally out of a log holding all three"
else
  bad "D24: ${n_bad[*]}"
fi
# The jest arm ALREADY had finding 1's property — it keys on the `Tests:` line's PRESENCE,
# not on the word `passed` — but it was true by accident and pinned by nothing. jest reports
# a suite whose every test is individually skipped as a PASSED suite (CLAUDE.md, #3522
# roborev F1), which is the same vacuous shape, so both spellings are pinned here.
j_bad=()
for jline in 'Tests:       27 skipped, 27 total' 'Tests:       27 skipped, 0 passed, 27 total'; do
  printf 'Test Suites: 27 passed, 27 total\n%s\n' "$jline" > "$LOG_DIR/node-bindings.log"
  g=$(_census_measure node-bindings PASS)
  st=$(_census_status_for PASS "$g")
  case "$g|$st" in 'ZERO jest tests'*'|VACUOUS') ;; *) j_bad+=("[$jline]->$g/$st") ;; esac
done
if [ "${#j_bad[@]}" -eq 0 ]; then
  ok "D25 (finding 1's sibling): an ALL-SKIPPED jest run — which jest reports as a passing suite — measures ZERO and couples to VACUOUS, in both spellings (with and without an explicit '0 passed')"
else
  bad "D25: ${j_bad[*]}"
fi
# The derived `<log>.ansi-stripped` sibling is a full COPY of the component log; leaving
# one per component would silently double the retained `logs:` bundle.
cp "$tmp/c.log" "$LOG_DIR/tombstones-scan.log"
rm -f "$LOG_DIR/tombstones-scan.log.ansi-stripped"
_census_measure tombstones-scan PASS >/dev/null
if [ ! -e "$LOG_DIR/tombstones-scan.log.ansi-stripped" ]; then
  ok "D22: the derived .ansi-stripped sibling is removed after the tally — the census does not double the size of the retained logs bundle"
else
  bad "D22: _census_measure left $LOG_DIR/tombstones-scan.log.ansi-stripped behind"
fi

# ---------------------------------------------------------------------------
# (E) THE VERDICT COUPLING IS AFFIRMATIVE (AC2). A positive verdict requires an
#     affirmative measurement: the permissive branch is keyed on the GOOD values, never
#     on `!= <bad>`, so an unrecognised or empty record FAILs rather than passing.
# ---------------------------------------------------------------------------
declare -a e_cases=(
  'PASS|COUNT 12 tests passed|PASS'
  'PASS|GAP nothing derivable here|PASS'
  'PASS|NOT-MEASURED could not read the log|PASS'
  'PASS|ZERO tests|VACUOUS'
  'PASS|UNDECLARED no census kind|FAIL'
  'PASS|WHATEVER a token nobody planned|FAIL'
  'PASS||FAIL'
  'FAIL|COUNT 12 tests passed|FAIL'
  'SKIP|NOT-APPLICABLE component ended SKIP|SKIP'
)
e_bad=()
for case_spec in "${e_cases[@]}"; do
  in_st=${case_spec%%|*}; rest=${case_spec#*|}
  rec=${rest%|*}; want=${rest##*|}
  gotst=$(_census_status_for "$in_st" "$rec")
  [ "$gotst" = "$want" ] || e_bad+=("($in_st,'$rec')->$gotst want $want")
done
if [ "${#e_bad[@]}" -eq 0 ]; then
  ok "E1: _census_status_for maps all ${#e_cases[@]} states affirmatively — ZERO->VACUOUS, and an UNRECOGNISED or EMPTY record FAILs rather than taking the permissive branch"
else
  bad "E1: ${e_bad[*]}"
fi
# _status_is_nonfailing is the CLOSED SET the aggregations key on. This is the #3625
# fail-open fix: every aggregation used to ask `[ "$st" = FAIL ]`, so VACUOUS — and an
# empty result file, and any future token — sailed through as non-failing.
s_bad=()
for s in PASS SKIP; do _status_is_nonfailing "$s" || s_bad+=("$s should be non-failing"); done
for s in FAIL VACUOUS "" pass Pass PASSED UNKNOWN; do
  _status_is_nonfailing "$s" && s_bad+=("'$s' was treated as non-failing")
done
if [ "${#s_bad[@]}" -eq 0 ]; then
  ok "E2: _status_is_nonfailing admits exactly PASS and SKIP; VACUOUS, the empty string and every unplanned spelling FAIL the run"
else
  bad "E2: ${s_bad[*]}"
fi
# Every aggregation site must ASK that function. A surviving `= FAIL ] && OVERALL=FAIL`
# is the fail-open shape returning, and it is invisible to any behavioural case that does
# not happen to seed the mode it lives in.
leftover=$(grep -nE '^\s*\[ "\$[A-Za-z_{}\[\]$#@]*" = FAIL \] && OVERALL=FAIL' "$GATE" || true)
if [ -z "$leftover" ]; then
  ok "E3: no aggregation in the gate still keys OVERALL on the single literal FAIL token"
else
  bad "E3: a '!= <bad>' aggregation survives: $leftover"
fi
n_agg=$(grep -c '_status_is_nonfailing ' "$GATE")
if [ "$n_agg" -ge 6 ]; then
  ok "E4: $n_agg call sites ask _status_is_nonfailing (>= the 5 aggregation/notification sites plus its definition)"
else
  bad "E4: only $n_agg _status_is_nonfailing reference(s) — a mode's aggregation is not routed through the closed set"
fi

# ---------------------------------------------------------------------------
# (F) AC3 — THE PLANT, END TO END, AND ITS POSITIVE CONTROL.
#
#     A no-op is planted in ONE component by putting a `cargo` on PATH that exits 0 and
#     compiles/prints nothing, then running the REAL gate with `--only` on that
#     component. The gate must NOT report PASS, and the emitted block must NAME the
#     component that verified nothing.
#
#     THE CONTROL DIFFERS IN EXACTLY ONE PROPERTY — the shim's stdout — so the red is
#     attributable to the census and not to the harness, the shim, `--only`, or the
#     component's own body. A bare non-zero exit would prove neither.
# ---------------------------------------------------------------------------
PLANT_COMPONENT=tombstones-scan
shim_noop="$tmp/shim-noop"; mkdir -p "$shim_noop"
cat > "$shim_noop/cargo" <<'SHIM'
#!/usr/bin/env bash
# THE PLANT: a cargo that succeeds and verifies nothing. No compile, no test, no output.
exit 0
SHIM
shim_real="$tmp/shim-real"; mkdir -p "$shim_real"
cat > "$shim_real/cargo" <<'SHIM'
#!/usr/bin/env bash
# THE CONTROL: identical, except it emits the tally a real warm `cargo test` emits.
printf '    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.02s\n'
printf '     Running tests/issue_1085_tombstones_full_scan_parity.rs (target/debug/deps/x-1)\n'
printf '\nrunning 7 tests\n\n'
printf 'test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n'
exit 0
SHIM
chmod +x "$shim_noop/cargo" "$shim_real/cargo"

run_only() { # run_only <shim-dir> <summary-out> ; echoes the gate's exit status
  local shim="$1" out="$2" rc=0
  AGENT_GATE_SUMMARY_FILE="$out" \
  AGENT_GATE_ALLOW_MISSING_FIXTURES=1 \
  PATH="$shim:$PATH" \
    bash "$GATE" --only "$PLANT_COMPONENT" >"$out.stdout" 2>&1 || rc=$?
  printf '%s' "$rc"
}
plant_sum="$tmp/plant.txt"
plant_rc=$(run_only "$shim_noop" "$plant_sum")
ctrl_sum="$tmp/ctrl.txt"
ctrl_rc=$(run_only "$shim_real" "$ctrl_sum")

plant_line=$(grep -E "^$PLANT_COMPONENT: " "$plant_sum" 2>/dev/null | head -1 || true)
ctrl_line=$(grep -E "^$PLANT_COMPONENT: " "$ctrl_sum" 2>/dev/null | head -1 || true)

if [ -z "$plant_line" ] || [ -z "$ctrl_line" ]; then
  bad "F0: the gate emitted no '$PLANT_COMPONENT:' component line (plant='$plant_line' control='$ctrl_line') — neither arm can be judged, so (F) certifies nothing"
else
  ok "F0: both arms emitted a '$PLANT_COMPONENT:' component line, so the differential has a subject"
  case "$plant_line" in
    "$PLANT_COMPONENT: "*VACUOUS*"verified NOTHING"*)
      ok "F1 (AC3): the planted no-op reports VACUOUS and the line NAMES '$PLANT_COMPONENT' as having verified nothing — not a bare red" ;;
    *) bad "F1 (AC3): the planted no-op did not report a NAMED VACUOUS state: $plant_line" ;;
  esac
  case "$plant_line" in
    *' PASS '*) bad "F2 (AC2): the planted component still reports PASS — a component that verified nothing must not pass: $plant_line" ;;
    *) ok "F2 (AC2): the planted component does NOT report PASS" ;;
  esac
  # `%-18s` pads the name field, so the status is matched after arbitrary spaces rather
  # than assumed adjacent — an assertion that depends on the padding width would red on a
  # rename of any longer component.
  case "$ctrl_line" in
    "$PLANT_COMPONENT: "*" PASS "*'{verified: 7 tests passed'*)
      ok "F3 (positive control): the SAME lane with a real tally reports PASS and states its count — the red is the census, not the harness" ;;
    *) bad "F3 (positive control): the control arm did not report PASS with its count: $ctrl_line" ;;
  esac
fi
# The verdict must PROPAGATE: a VACUOUS component fails the run, and the control must not.
if grep -q '^RESULT: FAIL' "$plant_sum" 2>/dev/null && [ "$plant_rc" != 0 ]; then
  ok "F4: the planted run terminates RESULT: FAIL with a non-zero exit — the VACUOUS token reaches the aggregation instead of sailing through it"
else
  bad "F4: expected RESULT: FAIL + non-zero exit from the planted run (rc=$plant_rc, result=$(grep -m1 '^RESULT:' "$plant_sum" 2>/dev/null))"
fi
# `--only` promotes a clean PASS to PARTIAL, which is the control's expected terminal state.
if grep -qE '^RESULT: (PASS|PARTIAL)' "$ctrl_sum" 2>/dev/null; then
  ok "F5 (control): the same run with a real tally terminates non-FAIL — the plant is DISCRIMINATING, not a lane that reds on correct input"
else
  bad "F5 (control): the control run did not terminate PASS/PARTIAL (rc=$ctrl_rc, result=$(grep -m1 '^RESULT:' "$ctrl_sum" 2>/dev/null))"
fi
# ATTRIBUTION: the block must not blame a component the plant did not touch.
other_vac=$(grep -E '^[a-z][a-z-]*: +VACUOUS' "$plant_sum" 2>/dev/null | grep -vE "^$PLANT_COMPONENT:" || true)
if [ -z "$other_vac" ]; then
  ok "F6: only the planted component is reported VACUOUS — the state is attributed, not sprayed across the block"
else
  bad "F6: a component the plant did not touch is also VACUOUS: $other_vac"
fi

# ---------------------------------------------------------------------------
# (G) THE AGGREGATE `census:` LINE, and the VACUOUS token reaching a MODE'S aggregation.
# ---------------------------------------------------------------------------
LOG_DIR="$tmp/agglogs"; mkdir -p "$LOG_DIR"
_census_write alpha 'COUNT 9 tests passed'
_census_write beta  'ZERO tests'
NAMES=(alpha beta fmt format-compat)   # fmt = declared gap; format-compat = no record
agg=$(census_summary_line "${NAMES[@]}")
case "$agg" in
  'census: 1/4 components AFFIRMED a count; 1 DECLARED-GAP (RECOGNISED); 1 NOT-MEASURED (RECOGNISED); 1 VACUOUS (RECOGNISED);'*'NON-EXHAUSTIVE'*)
    ok "G1: the aggregate line counts each class separately and reports every non-affirmed class as 'N RECOGNISED'" ;;
  *) bad "G1: got '$agg'" ;;
esac
case "$agg" in
  *'the gap set is CURATED'*'UNMEASURED, never verified'*)
    ok "G2: the aggregate line DECLARES its own non-exhaustiveness, so it cannot be read as a verified all-clear" ;;
  *) bad "G2: the aggregate line does not declare its non-exhaustiveness: $agg" ;;
esac
NAMES=(fmt)
agg0=$(census_summary_line "${NAMES[@]}")
case "$agg0" in
  *'0 NOT-MEASURED (RECOGNISED)'*'0 VACUOUS (RECOGNISED)'*)
    ok "G3: a zero class renders '0 ... (RECOGNISED)', never a bare 0 — a bare zero in a gate log reads as a verified all-clear from a scan that is documented as incomplete" ;;
  *) bad "G3: got '$agg0'" ;;
esac
# The lite aggregation must FAIL on a VACUOUS component. Driven through the gate's own
# --lite-aggregate-selftest hook, which seeds statuses and runs the REAL
# aggregate_lite_components. Under the pre-#3625 `= FAIL` test this was a GREEN run.
lite_sum="$tmp/lite-vac.txt"; lite_rc=0
AGENT_GATE_SUMMARY_FILE="$lite_sum" \
AGENT_GATE_TEST_LITE_RESULTS="file-size:PASS fmt:VACUOUS clippy:PASS" \
AGENT_GATE_TEST_LITE_SCOPED=PASS \
  bash "$GATE" --lite-aggregate-selftest >/dev/null 2>&1 || lite_rc=$?
if grep -q '^RESULT: FAIL' "$lite_sum" 2>/dev/null && [ "$lite_rc" -ne 0 ]; then
  ok "G4: --lite's aggregation FAILs on a VACUOUS component — the new token is not silently non-failing in a second mode"
else
  bad "G4: --lite aggregated a VACUOUS component to $(grep -m1 '^RESULT:' "$lite_sum" 2>/dev/null) with rc=$lite_rc"
fi
lite_ok="$tmp/lite-ok.txt"; lite_ok_rc=0
AGENT_GATE_SUMMARY_FILE="$lite_ok" \
AGENT_GATE_TEST_LITE_RESULTS="file-size:PASS fmt:PASS clippy:PASS" \
AGENT_GATE_TEST_LITE_SCOPED=PASS \
  bash "$GATE" --lite-aggregate-selftest >/dev/null 2>&1 || lite_ok_rc=$?
if grep -q '^RESULT: PASS' "$lite_ok" 2>/dev/null && [ "$lite_ok_rc" -eq 0 ]; then
  ok "G5 (control): the same aggregation with no VACUOUS component still PASSes — G4 is the token, not a broken hook"
else
  bad "G5 (control): the all-PASS aggregation did not pass (rc=$lite_ok_rc)"
fi

# ---------------------------------------------------------------------------
# (H) STRUCTURAL: the wiring a behavioural case cannot see, because a mode this file does
#     not drive would silently lose the census.
# ---------------------------------------------------------------------------
h_bad=()
grep -q '_census_finalize "\$1" "\$2"' <<<"$(sed -n '/^record_result() {/,/^}$/p' "$GATE")" \
  || h_bad+=("record_result-does-not-call-_census_finalize")
grep -q '_census_annotate' <<<"$(sed -n '/^_fm_summary_line() {/,/^}$/p' "$GATE")" \
  || h_bad+=("_fm_summary_line-does-not-append-the-census")
# SEVEN emit sites: full, lite, 2x delta, lite-agg selftest, emit-summary-selftest, and
# the tree-integrity BOUNDARY printer (#3625, roborev job 360 finding 2 — a mode that
# rendered a component table carrying NEITHER the feature matrix nor the census, missed
# because the emit-site set had been taken from a COUNT written down in a report rather
# than re-derived from the code).
n_census_agg=$(grep -cE '^[^#]*census_summary_line ' "$GATE")
[ "$n_census_agg" -ge 7 ] || h_bad+=("only-$n_census_agg-census_summary_line-emit-sites-expected->=7")
# The boundary printer specifically: BOTH contracts, asserted on the function BODY rather
# than on the file, because a call anywhere else would satisfy a whole-file grep.
boundary_body=$(sed -n '/^_tree_boundary_meta_lines() {/,/^}$/p' "$GATE")
if [ -z "$boundary_body" ]; then
  h_bad+=("_tree_boundary_meta_lines-not-found-renamed-or-reshaped")
else
  grep -q '_fm_summary_line "' <<<"$boundary_body" \
    || h_bad+=("boundary-printer-does-not-route-rows-through-_fm_summary_line")
  grep -q 'census_summary_line ' <<<"$boundary_body" \
    || h_bad+=("boundary-printer-emits-no-aggregate-census-line")
  grep -qE "^[^#]*printf '%-18s" <<<"$boundary_body" \
    && h_bad+=("boundary-printer-still-has-a-raw-component-row-printf")
fi
# The measurer must go through the strip, from a NON-COMMENT line: a comment naming the
# helper would otherwise satisfy a bare substring test (#3312's shape, and the reason
# test_cargo_output_parsers.sh's own extraction is comment-blind).
# The strip lives in the MEASURING CORE, which _census_measure delegates to; assert BOTH
# links, or a future refactor could leave the core routed and nothing calling it.
sed -n '/^_census_measure_kind() {/,/^}$/p' "$GATE" | grep -v '^[[:space:]]*#' | grep -q '_ansi_stripped_log' \
  || h_bad+=("_census_measure_kind-has-no-non-comment-call-to-_ansi_stripped_log")
sed -n '/^_census_measure() {/,/^}$/p' "$GATE" | grep -v '^[[:space:]]*#' | grep -q '_census_measure_kind ' \
  || h_bad+=("_census_measure-does-not-delegate-to-the-measuring-core")
# Both parsers must read by REDIRECTION. A pipe into awk is survivable for a counter, but
# a pipe into a `while read` accumulator is the #3400 silent-verdict shape, and the rule
# is applied uniformly rather than case by case.
for fn in _census_libtest_tally _census_compile_tally; do
  body=$(sed -n "/^$fn() {/,/^}$/p" "$GATE")
  grep -q "< \"\\\$1\"" <<<"$body" || h_bad+=("$fn-does-not-read-by-redirection")
  grep -qE '\|[[:space:]]*(awk|while)' <<<"$body" && h_bad+=("$fn-pipes-into-a-parser")
done
if [ "${#h_bad[@]}" -eq 0 ]; then
  ok "H1: the census is wired at record_result, at the ONE renderer, at $n_census_agg emit sites (the tree-integrity boundary printer included), through _ansi_stripped_log, and both parsers read by redirection"
else
  bad "H1: ${h_bad[*]}"
fi
# The guard must be REGISTERED in the gate, or it can be silently dropped.
if grep -q 'test_agent_gate_census.sh' "$GATE"; then
  ok "H2: this guard is registered in agent-gate.sh (tooling-tests), so it cannot be quietly unrun"
else
  bad "H2: scripts/tests/test_agent_gate_census.sh is not invoked by agent-gate.sh — an unregistered guard runs nowhere"
fi

# ---------------------------------------------------------------------------
# (K) THE RUN-TIME CENSUS FOR `scoped-tests` — #3625 census audit BLOCKER 1.
#
# THE DEFECT, which was a HIGH: `scoped-tests` was declared `both`, but a diff confined to
# bindings/python/** dispatches NO cargo at all — classify_scoped_plan diverts cqlite-py
# and sets the python-tier flag, and the cqlite-core fallback is deliberately guarded on
# `python_diff -eq 0` — so the lane's log holds only maturin + pytest output. `both`
# requires BOTH subjects to be zero before it says ZERO, and both were: no `test result:`,
# no `N tests run:`, no `Executable`. Result: ZERO -> VACUOUS -> OVERALL=FAIL on a CORRECT
# `--lite` fix round, and on a CORRECT `--delta`, which is a CERTIFYING mode. "A lane that
# reds on correct input is the lane agents learn to waive" — this file's own subject.
#
# THE FIX IS DERIVED, NOT A RE-DECLARATION: the lane chooses its census from what it
# actually dispatched. So every ROUTE gets a case — a suite that only exercised the cargo
# branch would prove nothing about a routing bug.
# ---------------------------------------------------------------------------
LOG_DIR="$tmp/scopedlogs"; mkdir -p "$LOG_DIR"
k=$(_census_kind scoped-tests) || k="(undeclared)"
case "$k" in
  runtime:*) ok "K0: scoped-tests is declared 'runtime:' — it has no statically correct kind, because its subject depends on what the diff routed to" ;;
  *) bad "K0: scoped-tests is declared '$k'; a static kind cannot be right for a lane whose subject depends on routing (this is how BLOCKER 1 happened)" ;;
esac
# ---- ROUTE 1: rust packages dispatched -> measure `both`, as before.
printf '     Running tests/x.rs (target/debug/deps/x-1)\ntest result: ok. 41 passed; 0 failed; 0 ignored\n' > "$LOG_DIR/scoped-tests.log"
_census_scoped_record scoped-tests 1 0 ""
got=$(_census_measure scoped-tests PASS); st=$(_census_status_for PASS "$got")
case "$got|$st" in
  'COUNT 41 tests passed and 0 test binaries built/verified|PASS') ok "K1 (route: rust): a diff that dispatched cargo measures 'both' and affirms its count" ;;
  *) bad "K1: got '$got' / '$st'" ;;
esac
# ---- ROUTE 2: python tier ONLY and it RAN -> the pytest tally in the SAME log is the
# subject, measured through the same indirect:pytest path python-bindings uses.
printf 'Compiling cqlite-py v0.1.0\n    Finished `dev` profile in 41.05s\n576 passed, 61 skipped in 62.30s\n' > "$LOG_DIR/scoped-tests.log"
_census_scoped_record scoped-tests 0 1 "python-tier: PASS (maturin develop … && pytest …)"
got=$(_census_measure scoped-tests PASS); st=$(_census_status_for PASS "$got")
case "$got|$st" in
  'COUNT 576 pytest tests passed|PASS') ok "K2 (route: python tier RAN): the pytest tally in the same log is the affirmative subject — the lane is measured, not gapped" ;;
  *) bad "K2: got '$got' / '$st'" ;;
esac
# ...and it inherits the corrected present-and-zero rule from batch 1: an all-skipped
# pytest run through THIS route is still ZERO, hence VACUOUS. The routing fix must not
# have bought back the vacuous pass it was fixing somewhere else.
printf 'Compiling cqlite-py v0.1.0\n61 skipped in 1.20s\n' > "$LOG_DIR/scoped-tests.log"
_census_scoped_record scoped-tests 0 1 "python-tier: PASS (…)"
got=$(_census_measure scoped-tests PASS); st=$(_census_status_for PASS "$got")
case "$got|$st" in
  'ZERO pytest tests'*'|VACUOUS') ok "K3 (route: python tier RAN, all skipped): still ZERO -> VACUOUS — the routing fix did not buy back a vacuous pass" ;;
  *) bad "K3: got '$got' / '$st'" ;;
esac
# ---- ROUTE 3: THE BLOCKER ITSELF. Python tier in scope but SKIPPED, and the
# nothing-dispatched case. Both must PRESERVE PASS with an affirmative record NAMING that
# there was no executable subject — never VACUOUS, which is what red a correct run.
r3_bad=()
r3_n=0
printf 'maturin build output only, no pytest summary anywhere\n' > "$LOG_DIR/scoped-tests.log"
for spec in \
  '0|1|python-tier: SKIPPED (no python3 on PATH) — python-binding diff NOT validated by this lite run' \
  '0|1|python-tier: SKIPPED (toolchain: cargo/rustc absent) — …' \
  '0|1|' \
  '0|0|'; do
  r3_n=$((r3_n + 1))
  np=${spec%%|*}; rest=${spec#*|}; pd=${rest%%|*}; nt=${rest#*|}
  rm -f "$(_census_sidecar scoped-tests)"
  _census_scoped_record scoped-tests "$np" "$pd" "$nt"
  g=$(_census_measure scoped-tests PASS); s2=$(_census_status_for PASS "$g")
  case "$g|$s2" in
    'NOT-APPLICABLE the diff routed'*'no executable subject|PASS') ;;
    *) r3_bad+=("[np=$np pd=$pd note='${nt:-(empty)}']->$g/$s2") ;;
  esac
done
if [ "$r3_n" -ne 4 ]; then
  bad "K4: only $r3_n of the 4 no-executable-subject routes were exercised — the loop is not iterating"
elif [ "${#r3_bad[@]}" -eq 0 ]; then
  ok "K4 (BLOCKER 1, the regression): all 4 routes that dispatch NO executable subject — python tier SKIPPED (x2), an empty tier note, and nothing routed at all — record an affirmative NOT-APPLICABLE and PRESERVE PASS. Declared 'both', every one of these measured ZERO and reddened a correct --lite/--delta"
else
  bad "K4: a no-executable-subject route did not preserve PASS: ${r3_bad[*]}"
fi
# _python_tier_ran is the ONE discrimination both this recorder and _delta_python_tier_gap
# depend on, and it must be AFFIRMATIVE: only the two ran-states may answer yes.
p_bad=()
for n in 'python-tier: PASS (cmd)' 'python-tier: FAIL (pytest failure — a real code failure)'; do
  _python_tier_ran "$n" || p_bad+=("ran-state '$n' read as not-run")
done
for n in 'python-tier: SKIPPED (no python3 on PATH)' 'python-tier: SKIPPED (toolchain: venv/pip setup failed — offline?)' '' 'python-tier: something new' 'PASS'; do
  _python_tier_ran "$n" && p_bad+=("non-ran state '$n' read as RAN")
done
if [ "${#p_bad[@]}" -eq 0 ]; then
  ok "K5: _python_tier_ran admits exactly the two ran-states (PASS/FAIL); every SKIPPED spelling, an empty note and an unplanned one answer 'did not run' — a could-not-tell never takes the permissive branch"
else
  bad "K5: ${p_bad[*]}"
fi
# ONE definition, two readers. A second spelling of this discrimination in
# _delta_python_tier_gap is a second thing to drift.
if grep -q '_python_tier_ran "$note"' <<<"$(sed -n '/^_delta_python_tier_gap() {/,/^}$/p' "$GATE")"; then
  ok "K6: _delta_python_tier_gap asks the SAME _python_tier_ran predicate rather than re-spelling the PASS/FAIL test"
else
  bad "K6: _delta_python_tier_gap carries its own copy of the ran-state test — two spellings, one concept"
fi
# The WIRING: run_scoped_tests must call the recorder with the ROUTING variables, and
# before it finalizes. A recorder nothing calls is the same defect one layer out.
scoped_body=$(sed -n '/^run_scoped_tests() {/,/^}$/p' "$GATE")
if [ -z "$scoped_body" ]; then
  bad "K7: run_scoped_tests not found — renamed or reshaped; the wiring cannot be judged"
elif grep -q '_census_scoped_record "$name" "${#pkgs\[@\]}" "$python_diff" "$PYTHON_TIER_NOTE"' <<<"$scoped_body"; then
  ok "K7: run_scoped_tests records its run-time census from the SAME routing variables the dispatch was made from (pkgs[], python_diff, PYTHON_TIER_NOTE)"
else
  bad "K7: run_scoped_tests does not call _census_scoped_record with the routing variables — the runtime: kind would render a recording gap on every run"
fi

# ---------------------------------------------------------------------------
# (L) THE `self:` LANES ARE COUPLED TO AC2 — #3625 census audit BLOCKER 2.
#
# THE DEFECT: run_delta_node_tests and run_delta_shell_selftests called _census_declare and
# then pushed the RAW $status, never routing through _census_status_for. A ZERO there
# rendered `{verified NOTHING: …}` beside a PASS, was counted as VACUOUS on the aggregate
# line, and the run stayed GREEN. Unreachable today only because both early-return on an
# empty target set — the coupling was ABSENT and something unrelated was holding the line,
# which is precisely CLAUDE.md's "ask of every key what fails the run if THIS key alone
# goes bad".
#
# DRIVEN THROUGH THE REAL FUNCTION, with ONE declared substitution. The production path
# cannot currently emit a ZERO (an empty target set returns before _census_declare), so the
# ZERO is INJECTED by substituting `_census_declare` alone — the collaborator whose value
# is under test — while the function's own status handling, the real _census_finalize, the
# real _census_status_for and the real _status_is_nonfailing all run unmodified. That is
# the wiring the defect was in, and it is stated as a substitution rather than dressed up
# as an end-to-end run.
# ---------------------------------------------------------------------------
l_bad=()
l_n=0
for fn in run_delta_node_tests run_delta_shell_selftests; do
  body=$(sed -n "/^$fn() {/,/^}$/p" "$GATE")
  if [ -z "$body" ]; then l_bad+=("$fn-not-found"); continue; fi
  l_n=$((l_n + 1))
  # STRUCTURAL: finalize + the closed-set OVERALL flip must both precede the STATUSES push.
  grep -q 'status=$(_census_finalize ' <<<"$body" || l_bad+=("$fn-does-not-route-its-status-through-_census_finalize")
  grep -q '_status_is_nonfailing "$status" || OVERALL=FAIL' <<<"$body" || l_bad+=("$fn-does-not-flip-OVERALL-on-a-non-passing-census")
done
[ "$l_n" -eq 2 ] || l_bad+=("only-$l_n-of-2-self-lanes-inspected")
if [ "${#l_bad[@]}" -eq 0 ]; then
  ok "L1: both self: lanes route their status through _census_finalize AND flip OVERALL through the closed set — they own their own OVERALL bookkeeping, so the coupling has to be local"
else
  bad "L1: ${l_bad[*]}"
fi
# BEHAVIOURAL: the composition the wiring performs. A ZERO census must turn a PASS into
# VACUOUS and a VACUOUS must be a failing status — asserted on the REAL functions.
LOG_DIR="$tmp/selflogs"; mkdir -p "$LOG_DIR"
_census_declare node-tests 0 'changed jest test file(s)'
zst=$(_census_finalize node-tests PASS)
if [ "$zst" = VACUOUS ] && ! _status_is_nonfailing "$zst"; then
  ok "L2: a self: lane's ZERO census turns its PASS into VACUOUS, and VACUOUS is a FAILING status — so the flip the wiring performs really fails the run"
else
  bad "L2: a ZERO self: census yielded '$zst' (want VACUOUS, and it must be failing)"
fi
_census_declare node-tests 4 'changed jest test file(s)'
nst=$(_census_finalize node-tests PASS)
if [ "$nst" = PASS ]; then
  ok "L3 (control): a self: lane with a real count keeps its PASS — L2 is the ZERO, not the coupling reddening everything"
else
  bad "L3: a COUNT self: census yielded '$nst' (want PASS)"
fi

# ---------------------------------------------------------------------------
# (M) TRUTH IN THE AGGREGATE LINE — #3625 census audit LOW 1 and LOW 2.
# ---------------------------------------------------------------------------
LOG_DIR="$tmp/truthlogs"; mkdir -p "$LOG_DIR"
# LOW 1: the status check must sit ABOVE the kind dispatch. A FAILing gap component used to
# render its gap reason and be counted under DECLARED-GAP rather than not-applicable. No
# verdict changed — but a miscounted census line is what stops the next person looking.
m_bad=()
for spec in 'fmt|FAIL' 'fmt|SKIP' 'node-tests|FAIL' 'scoped-tests|SKIP'; do
  c=${spec%%|*}; st=${spec##*|}
  rm -f "$(_census_sidecar "$c")"
  g=$(_census_measure "$c" "$st")
  case "$g" in "NOT-APPLICABLE component ended $st"*) ;; *) m_bad+=("[$c/$st]->$g") ;; esac
done
if [ "${#m_bad[@]}" -eq 0 ]; then
  ok "M1 (LOW 1): a gap:, self: or runtime: component that did NOT pass records NOT-APPLICABLE, not its declared reason — the aggregate line counts it under not-applicable (SKIP/FAIL) where it belongs"
else
  bad "M1: ${m_bad[*]}"
fi
# ...and the control: the same components on a PASS still render their declared state.
if [ "$(_census_measure fmt PASS)" != "${_census_measure_fmt_pass:-}" ] \
   && case "$(_census_measure fmt PASS)" in 'GAP cargo fmt'*) true ;; *) false ;; esac; then
  ok "M2 (control): the same component on a PASS still records its declared GAP — M1 is the status check, not the gap arm being lost"
else
  bad "M2: a PASSing gap component no longer records GAP: $(_census_measure fmt PASS)"
fi
# LOW 2: nextest's `N tests run: X passed, Y failed` has N = X + Y, so summing N under a
# `COUNT %d tests passed` label is a FALSE LABEL. Only reachable on a PASS today, where the
# two are equal — which is exactly why it would have decayed unnoticed.
printf '     Summary [   4.567s] 10 tests run: 8 passed, 2 failed\n' > "$tmp/nx2.log"
if [ "$(_census_libtest_tally "$tmp/nx2.log")" = "8 1" ]; then
  ok "M3 (LOW 2): the nextest arm counts tests PASSED (8), not tests RUN (10) — the label 'N tests passed' is true rather than merely equal on the happy path"
else
  bad "M3: expected '8 1' from a nextest summary with failures, got '$(_census_libtest_tally "$tmp/nx2.log")'"
fi
printf '     Summary [   4.567s] 3562 tests run: 3562 passed, 2 skipped\n' > "$tmp/nx3.log"
if [ "$(_census_libtest_tally "$tmp/nx3.log")" = "3562 1" ]; then
  ok "M4 (control): an all-passing nextest summary is unchanged at 3562 — M3 narrowed the label, it did not change the happy-path number"
else
  bad "M4: got '$(_census_libtest_tally "$tmp/nx3.log")'"
fi
# LOW 3: `cargo test -q` suppresses `Executable` but NOT `test result:` (measured). A `-q`
# lane is therefore safe as `libtest` and can never be `compile`/`both`. kit-dashboard-drift
# is the only -q lane; this pins the pairing so a future --no-run pass cannot silently make
# it measure ZERO test binaries.
kd=$(_census_kind kit-dashboard-drift) || kd="(undeclared)"
q_doc=$(sed -n '/^# _census_compile_tally <stripped-log>/,/^_census_compile_tally() {/p' "$GATE")
q_ok=0
if grep -qi 'suppresses' <<<"$q_doc" && grep -q 'Executable' <<<"$q_doc" && grep -q 'test result:' <<<"$q_doc"; then
  q_ok=1
fi
if [ "$kd" = libtest ] && [ "$q_ok" -eq 1 ]; then
  ok "M5 (LOW 3): the only 'cargo test -q' lane is declared libtest, and the -q trap (which suppresses Executable while leaving test result: intact) is recorded at the compile parser"
else
  bad "M5: kit-dashboard-drift is '$kd' (want libtest) and/or the -q trap is not recorded at _census_compile_tally (q_ok=$q_ok)"
fi
echo
echo "component census guard: $PASS passed, $FAIL failed"
# A COUNT FLOOR beside the abort trap (the idiom of test_agent_gate_summary.sh and
# test_agent_gate_component_set.sh): a section that stops contributing verdicts — an
# extraction that broke, a subshell dying quietly — shrinks the subject set WITHOUT
# aborting, and "failed: 0" over a shrunken set is the vacuous pass this whole file is
# about. Set just below the full-host figure so it reds on a structural loss.
CENSUS_CASE_FLOOR=52
CENSUS_REACHED_END=1
if [ $((PASS + FAIL)) -lt "$CENSUS_CASE_FLOOR" ]; then
  printf 'FAIL - only %s verdicts were produced (floor %s): sections are being skipped or dying silently, and a "0 failed" over a shrunken subject set certifies nothing.\n' \
    "$((PASS + FAIL))" "$CENSUS_CASE_FLOOR" >&2
  exit 1
fi
[ "$FAIL" -eq 0 ] || exit 1
exit 0
