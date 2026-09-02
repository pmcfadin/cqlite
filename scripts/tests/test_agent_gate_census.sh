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
          _census_status_for _census_finalize _census_classify _census_record _census_annotate \
          _status_is_nonfailing run_delta_node_tests \
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
# EVERY STATUS, not just PASS (#3625/job 379): the annotation is a function of (component,
# status) now, so a status whose arm forgot to render would be invisible to a PASS-only loop.
for c in ${comps_arr[@]+"${comps_arr[@]}"} $dyn_names a-component-that-does-not-exist; do
  for b_st in PASS FAIL SKIP VACUOUS; do
    a=$(_census_annotate "$c" "$b_st")
    [ -n "$a" ] || blank+=("$c/$b_st")
    case "$a" in '{'*'}') ;; *) notbrace+=("$c/$b_st=$a") ;; esac
  done
done
if [ "${#blank[@]}" -eq 0 ] && [ "${#notbrace[@]}" -eq 0 ]; then
  ok "B1: no (name x status) pair renders a BLANK or malformed census annotation — every component at PASS/FAIL/SKIP/VACUOUS, an undeclared name included"
else
  bad "B1: blank: ${blank[*]:-(none)}; malformed: ${notbrace[*]:-(none)}"
fi
case "$(_census_annotate a-component-that-does-not-exist PASS)" in
  *'UNDECLARED'*'a-component-that-does-not-exist'*)
    ok "B2: an undeclared component's annotation NAMES the component, rather than reading as an ordinary gap" ;;
  *) bad "B2: got '$(_census_annotate a-component-that-does-not-exist PASS)'" ;;
esac
# A declared gap must PRINT its reason on every run — a gap nobody sees is a silence.
case "$(_census_annotate fmt PASS)" in
  *'no census'*'fmt --all --check'*)
    ok "B3: a DECLARED GAP renders its reason, so the reduction in coverage is visible in the block" ;;
  *) bad "B3: the fmt gap does not print its declared reason: $(_census_annotate fmt PASS)" ;;
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

# The tally is TWO-FIELD since #3625/job 368: "<Executable lines> <cargo status lines>".
# BOTH fields are colour-sensitive — every cargo status word carries the same escapes —
# which makes the raw parse doubly wrong: it under-counts the binaries AND reports the log
# as carrying no cargo output at all.
raw_bins=$(_census_compile_tally "$tmp/c.log")
if [ "$raw_bins" = "0 0" ]; then
  ok "C1 (RED, pinned defect): parsing the COLOURED log RAW yields '0 0' — 0 test binaries AND 0 cargo status lines, so an unrouted parse would report a healthy --no-run lane as carrying no cargo output whatsoever"
else
  bad "C1 (RED): the coloured log parsed raw already yields '$raw_bins' — the fixture no longer reproduces the #3400 hazard, so C2 proves nothing"
fi
src=$(_ansi_stripped_log "$tmp/c.log") || src=""
if [ -n "$src" ] && [ "$(_census_compile_tally "$src")" = "2 4" ]; then
  ok "C2 (GREEN): routed through _ansi_stripped_log the SAME coloured log counts 2 test binaries across 4 cargo status lines — the strip carries the correctness for BOTH fields"
else
  bad "C2 (GREEN): expected '2 4' after the strip, got '$(_census_compile_tally "${src:-/dev/null}")'"
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
if [ "$(_census_compile_tally "$tmp/warm-norun.log")" = "1 2" ] \
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
got=$(_census_measure shell-selftests PASS)
case "$got" in
  'NOT-MEASURED'*"'shell-selftests' records its own subject count and recorded none"*) ok "D12: a self: component with no record is NOT-MEASURED naming itself, not a licence to claim a count" ;;
  *) bad "D12: got '$got'" ;;
esac
_census_declare shell-selftests 4 'changed scripts/tests/*.sh executed'
got=$(_census_measure shell-selftests PASS)
case "$got" in
  'COUNT 4 changed scripts/tests/*.sh executed') ok "D13: _census_declare records a self: component's own affirmative count" ;;
  *) bad "D13: got '$got'" ;;
esac
_census_declare shell-selftests 0 'changed scripts/tests/*.sh executed'
got=$(_census_measure shell-selftests PASS)
case "$got" in
  'ZERO changed scripts/tests/*.sh executed') ok "D14: _census_declare records a self-reported 0 as ZERO — an affirmative measurement of nothing is still nothing" ;;
  *) bad "D14: got '$got'" ;;
esac
_census_declare shell-selftests "not-a-number" 'changed scripts/tests/*.sh executed'
got=$(_census_measure shell-selftests PASS)
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
  # THE STATUS FIELD, not a scan for the word. A bare `* PASS *` was the wrong instrument
  # and this suite's own RED arm proved it: with the VACUOUS arm removed the row reads
  # `… {no census: component ended VACUOUS, so there is no PASS to affirm}`, whose PROSE
  # contains " PASS ", so F2 fired for a reason unrelated to the status. Same lesson as
  # Q1's status-claim check — a word scan over a line that legitimately names other
  # statuses is a guard that reds on correct input.
  plant_st=$(printf '%s' "$plant_line" | awk '{print $2}')
  if [ "$plant_st" = PASS ]; then
    bad "F2 (AC2): the planted component still reports PASS — a component that verified nothing must not pass: $plant_line"
  else
    ok "F2 (AC2): the planted component's STATUS FIELD is '$plant_st', not PASS"
  fi
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
# REAL declared components (#3625/job 379): the classifier resolves the DECLARATION before
# anything else, so a made-up name is correctly UNDECLARED and can no longer stand in for a
# component that has a census. tombstones-scan = a count; feature-iso-parquet = a measured
# zero (hence the VACUOUS status); fmt = a declared gap; format-compat = no record at all.
_census_write tombstones-scan     'COUNT 9 tests passed'
_census_write feature-iso-parquet 'ZERO test binaries'
# name/STATUS pairs since #3625/job 371 — see section (Q) for why every status-naming
# qualifier has to come from the status rather than from the census state.
agg=$(census_summary_line tombstones-scan PASS feature-iso-parquet VACUOUS fmt PASS format-compat PASS)
case "$agg" in
  'census: 1/4 components AFFIRMED a count; 1 DECLARED-GAP (RECOGNISED); 1 NOT-MEASURED (RECOGNISED); 1 measured-ZERO (RECOGNISED);'*'1 row(s) carry a VACUOUS status.'*'NON-EXHAUSTIVE'*)
    ok "G1: the aggregate line counts each class separately, reports every non-affirmed class as 'N RECOGNISED', and reports the VACUOUS count from the STATUS beside the measured-ZERO state" ;;
  *) bad "G1: got '$agg'" ;;
esac
case "$agg" in
  # …and it must name an OPEN issue for the residual. #3625 is the ORIGIN (closed
  # NOT_PLANNED, absorbed into #3162); a printed pointer that resolves only to a dead ticket
  # leaves the residual belonging to nobody, which is the same defect as the gap strings.
  *'the gap set is CURATED'*'UNMEASURED, never verified'*'tracked in #3162'*)
    ok "G2: the aggregate line DECLARES its own non-exhaustiveness AND points the residual at an OPEN issue, so it cannot be read as a verified all-clear or followed to a dead ticket" ;;
  *) bad "G2: the aggregate line does not declare its non-exhaustiveness: $agg" ;;
esac
agg0=$(census_summary_line fmt PASS)
case "$agg0" in
  *'0 NOT-MEASURED (RECOGNISED)'*'0 measured-ZERO (RECOGNISED)'*)
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
_census_declare shell-selftests 0 'changed scripts/tests/*.sh executed'
zst=$(_census_finalize shell-selftests PASS)
if [ "$zst" = VACUOUS ] && ! _status_is_nonfailing "$zst"; then
  ok "L2: a self: lane's ZERO census turns its PASS into VACUOUS, and VACUOUS is a FAILING status — so the flip the wiring performs really fails the run"
else
  bad "L2: a ZERO self: census yielded '$zst' (want VACUOUS, and it must be failing)"
fi
_census_declare shell-selftests 4 'changed scripts/tests/*.sh executed'
nst=$(_census_finalize shell-selftests PASS)
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
# ---------------------------------------------------------------------------
# (N) QUIET IS #3400's SECOND DIMENSION — roborev job 368, blocker 1.
#
# #3400 is about a cargo-output parse keyed on a PRESENTATION property. Colour was one
# instance; QUIET is another, and the `Executable` anchor was colour-immune and still
# presentation-dependent in that second dimension. `-q` on the command line was already
# noted (census audit LOW 3), but `CARGO_TERM_QUIET=true` in the ENVIRONMENT and
# `[term] quiet = true` in ANY `.cargo/config.toml` are invisible at the call site, so a box
# carrying either would have made `feature-iso-parquet` and `minimal-build` measure ZERO and
# read VACUOUS on EVERY gate — reddening correct input, fleet-wide.
#
# FIXTURE PROVENANCE, measured 2026-09-01 against real cargo, BOTH mechanisms:
#   * quiet suppresses EVERY cargo status line — `Compiling`, `Finished`, `Running`,
#     `Executable` — so `cargo test --lib --no-run` under quiet emits a COMPLETELY EMPTY
#     log. There is no partial state to misread, which is what makes a presence probe sound.
#   * libtest's `running N tests` / `test result:` are UNAFFECTED by either mechanism.
# Both facts are asserted below as fixture properties, so a future cargo that changed either
# would red HERE rather than silently invalidate the probe.
# ---------------------------------------------------------------------------
LOG_DIR="$tmp/quietlogs"; mkdir -p "$LOG_DIR"
: > "$tmp/quiet-norun.log"                       # what quiet really produces: nothing
printf '    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.01s\n  Executable unittests src/lib.rs (target/debug/deps/dw-1)\n  Executable tests/foo.rs (target/debug/deps/foo-1)\n' > "$tmp/loud-norun.log"
if [ "$(_census_compile_tally "$tmp/quiet-norun.log")" = "0 0" ] \
   && [ "$(_census_compile_tally "$tmp/loud-norun.log")" = "2 3" ]; then
  ok "N0: the compile tally is TWO-FIELD — a quiet --no-run log measures '0 executables, 0 cargo status lines' while the same run non-quiet measures '2 executables, 3 status lines'. Zero-executables and no-cargo-output-at-all are now different facts"
else
  bad "N0: expected '0 0' quiet and '2 3' loud, got '$(_census_compile_tally "$tmp/quiet-norun.log")' / '$(_census_compile_tally "$tmp/loud-norun.log")'"
fi
# THE VERDICT SPLIT, driven through the real measurer on a real `compile` component.
cp "$tmp/quiet-norun.log" "$LOG_DIR/feature-iso-parquet.log"
got=$(_census_measure feature-iso-parquet PASS); st=$(_census_status_for PASS "$got")
case "$got|$st" in
  'NOT-MEASURED cargo status output is SUPPRESSED'*'|PASS')
    ok "N1 (BLOCKER 1): a quiet-suppressed compile log is NOT-MEASURED and PRESERVES PASS — 'could not measure' does not take the branch reserved for 'measured, and it is zero'" ;;
  *) bad "N1: got '$got' / '$st'" ;;
esac
case "$got" in
  *'NOT a measured zero'*'Remedy: unset the quiet setting'*)
    ok "N2: the NOT-MEASURED text NAMES the cause (CARGO_TERM_QUIET / [term] quiet / -q), says it is NOT a measured zero, and carries the remedy — the coverage loss is DECLARED rather than silent" ;;
  *) bad "N2: the suppression record does not name its cause and remedy: $got" ;;
esac
# ...and the POSITIVE CONTROL: the same lane, non-quiet, measures its binaries.
cp "$tmp/loud-norun.log" "$LOG_DIR/feature-iso-parquet.log"
got=$(_census_measure feature-iso-parquet PASS)
case "$got" in
  'COUNT 2 test binaries built/verified') ok "N3 (positive control): the SAME lane on a non-quiet log measures 2 test binaries — N1 is the suppression, not the probe disabling the census" ;;
  *) bad "N3: got '$got'" ;;
esac
# ...and a GENUINE zero is still fatal: cargo status output PRESENT, no Executable line.
printf '   Compiling dw v0.1.0 (/tmp/dw)\n    Finished `test` profile in 1.32s\n' > "$LOG_DIR/feature-iso-parquet.log"
got=$(_census_measure feature-iso-parquet PASS); st=$(_census_status_for PASS "$got")
case "$got|$st" in
  'ZERO test binaries'*'carries cargo status output but no'*"'Executable'"*'|VACUOUS')
    ok "N4: a log that DEMONSTRABLY carries cargo status output and no 'Executable' line is still a real ZERO -> VACUOUS. The fix narrowed what counts as a measured zero; it did not remove the state" ;;
  *) bad "N4: got '$got' / '$st'" ;;
esac
# `both` probes its two subjects INDEPENDENTLY: libtest survives quiet, cargo status does
# not, so a quiet box must not turn a lane's measurable half into a claim about the other.
printf '\nrunning 6 tests\n\ntest result: ok. 6 passed; 0 failed; 0 ignored\n' > "$LOG_DIR/integration-tests.log"
got=$(_census_measure integration-tests PASS); st=$(_census_status_for PASS "$got")
case "$got|$st" in
  'COUNT 6 tests passed (test binaries NOT MEASURED:'*'|PASS')
    ok "N5: a quiet 'both' lane reports its 6 measured tests AND names the binary count as NOT MEASURED — it neither claims '0 binaries' (false) nor discards the half it could measure" ;;
  *) bad "N5: got '$got' / '$st'" ;;
esac
: > "$LOG_DIR/integration-tests.log"
got=$(_census_measure integration-tests PASS); st=$(_census_status_for PASS "$got")
case "$got|$st" in
  'NOT-MEASURED'*'no libtest tally'*'SUPPRESSED'*'|PASS')
    ok "N6: a 'both' lane with NEITHER subject readable under quiet is NOT-MEASURED, not ZERO — under quiet an empty log is exactly what a healthy --no-run-only route produces" ;;
  *) bad "N6: got '$got' / '$st'" ;;
esac
# The libtest kind needs no equivalent probe, and this is the measured reason why: quiet
# does not touch libtest's own output. Pinned as a FIXTURE PROPERTY so a cargo that changed
# it reds here instead of silently invalidating the asymmetry the design rests on.
printf '\nrunning 3 tests\n...\ntest result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n' > "$LOG_DIR/tombstones-scan.log"
got=$(_census_measure tombstones-scan PASS)
case "$got" in
  'COUNT 3 tests passed'*) ok "N7: a QUIET log — no cargo status line anywhere — still measures its libtest tally, because quiet suppresses cargo's status output and not the harness's. That asymmetry is why only the compile half needed a presence probe" ;;
  *) bad "N7: got '$got'" ;;
esac
# STRUCTURAL: the probe must be anchored on the STATUS WORD ALONE ($1), never on
# `<status> <payload>` — the #3400 rule, which applies to the new probe exactly as it does
# to the Executable anchor it sits beside.
compile_body=$(sed -n '/^_census_compile_tally() {/,/^}$/p' "$GATE")
if grep -q '\$1 == "Finished"' <<<"$compile_body" && ! grep -qE '\$0 ~ /(Finished|Executable) ' <<<"$compile_body"; then
  ok "N8: the suppression probe anchors on the cargo STATUS WORD alone (\$1), never on '<status> <payload>' — colour puts a reset between the two (#3400)"
else
  bad "N8: the suppression probe is not status-word-anchored"
fi

# ---------------------------------------------------------------------------
# (O) `UNDECLARED` IS FATAL AT ANY STATUS — roborev job 368, blocker 2.
#
# _census_status_for returned every non-PASS status without inspecting the census state, so
# the fail-closed state that makes "a new component cannot join the gate with a blank
# census" TRUE was not fatal when the component SKIPped — the completeness guarantee
# failing exactly where it is least likely to be noticed, on a NEW component that SKIPs on
# the box where it is first run. The standing question — what fails the run if THIS key
# alone goes bad? — had the answer "nothing, on a SKIP".
# ---------------------------------------------------------------------------
o_bad=()
o_n=0
for spec in \
  'PASS|UNDECLARED no census kind is declared|FAIL' \
  'SKIP|UNDECLARED no census kind is declared|FAIL' \
  'FAIL|UNDECLARED no census kind is declared|FAIL' \
  'PASS|WHATEVER an unplanned token|FAIL' \
  'SKIP|WHATEVER an unplanned token|FAIL' \
  'SKIP||FAIL' \
  'SKIP|NOT-APPLICABLE component ended SKIP|SKIP' \
  'SKIP|GAP nothing derivable|SKIP' \
  'FAIL|COUNT 12 tests passed|FAIL' \
  'PASS|ZERO tests|VACUOUS' \
  'PASS|COUNT 12 tests passed|PASS'; do
  o_n=$((o_n + 1))
  in_st=${spec%%|*}; rest=${spec#*|}; rec=${rest%|*}; want=${rest##*|}
  g=$(_census_status_for "$in_st" "$rec")
  [ "$g" = "$want" ] || o_bad+=("($in_st,'$rec')->$g want $want")
done
if [ "$o_n" -ne 11 ]; then
  bad "O1: only $o_n of 11 status/record pairs were exercised"
elif [ "${#o_bad[@]}" -eq 0 ]; then
  ok "O1 (BLOCKER 2): an UNDECLARED or unrecognised census record FAILs the run at EVERY status — SKIP included — while a sound record still lets a non-PASS status pass through unchanged"
else
  bad "O1: ${o_bad[*]}"
fi
# End to end: an undeclared component that SKIPs. _census_measure resolves the DECLARATION
# before the status precisely so the record is UNDECLARED rather than NOT-APPLICABLE, and
# the coupling then fails the run.
LOG_DIR="$tmp/undeclogs"; mkdir -p "$LOG_DIR"
g=$(_census_measure a-brand-new-component SKIP)
st=$(_census_status_for SKIP "$g")
case "$g|$st" in
  'UNDECLARED no census kind is declared'*"'a-brand-new-component'"*'|FAIL')
    ok "O2 (BLOCKER 2, end to end): a NEW component that SKIPs is still refused BY NAME and FAILs — the completeness guarantee does not depend on the component happening to run" ;;
  *) bad "O2: got '$g' / '$st'" ;;
esac
if ! _status_is_nonfailing "$(_census_status_for SKIP "$g")"; then
  ok "O3: …and that FAIL reaches the aggregation's closed set, so the run really fails rather than merely printing FAIL on one row"
else
  bad "O3: the undeclared-SKIP status was treated as non-failing by _status_is_nonfailing"
fi

# ---------------------------------------------------------------------------
# (P) THE PROGRESS LINE MUST NOT LIE — roborev job 368, low.
#
# record_result can turn a PASS into VACUOUS or FAIL, and every caller printed its OWN
# unchanged local `$status` afterwards, so a no-op component wrote `>>> [x] PASS` to the run
# log while the SUMMARY reported failure. A gate log that makes an affirmatively false
# statement is worse than silence — it is the first thing a human reads when triaging.
# ---------------------------------------------------------------------------
if grep -q 'RECORDED_STATUS="$_rr_status"' <<<"$(sed -n '/^record_result() {/,/^}$/p' "$GATE")"; then
  ok "P1: record_result publishes the FINALIZED status for its caller's progress line"
else
  bad "P1: record_result does not publish the finalized status — every caller's progress line would keep printing the pre-census one"
fi
# EVERY caller, not just run_component. The two legitimate exceptions are run_scoped_tests'
# own terminal paths, which never reach record_result and reassign `$status` from
# _census_finalize themselves — so they are excluded BY FUNCTION, not by count.
# FAIL CLOSED WITHOUT python3, found while answering the case-floor question (round 14).
# This is the suite's ONLY environment dependency, and it was the vacuous shape this whole
# file polices: with python3 absent the derivation printed nothing, `$p_raw` was empty, and
# P2 reported "no progress line prints the pre-census status" having examined NOTHING. The
# case count did not change, which is why a floor could never have caught it. `tooling-tests`
# SKIPs this component without python3 so the gate never saw it, but a direct run on a lean
# box did.
p_rc=0
p_raw=$(python3 - "$GATE" <<'PYX'
import sys
lines = open(sys.argv[1], encoding='utf-8').read().split('\n')
start = next(i for i, l in enumerate(lines) if l == 'run_scoped_tests() {')
end = next(i for i in range(start, len(lines)) if lines[i] == '}')
bad = [i + 1 for i, l in enumerate(lines)
       if '>>> [$name] $status (' in l and not (start <= i <= end)]
print(' '.join(str(b) for b in bad))
PYX
) || p_rc=$?
if [ "$p_rc" -ne 0 ]; then
  bad "P2: the derivation could not run (python3 exited $p_rc, or is absent), so NO progress line was examined — an empty result here is 'not measured', never 'none found'"
elif [ -z "$p_raw" ]; then
  ok "P2: no progress line outside run_scoped_tests prints the pre-census \$status — all ~115 print the finalized RECORDED_STATUS"
else
  bad "P2: progress line(s) still printing the PRE-census status at line(s): $p_raw"
fi
p_fin=$(grep -c 'RECORDED_STATUS (\$((end - start))s)' "$GATE")
if [ "$p_fin" -ge 100 ]; then
  ok "P3: $p_fin progress lines print the finalized status — the fix reached the bespoke runners, not only run_component"
else
  bad "P3: only $p_fin progress line(s) print RECORDED_STATUS; the rewrite did not reach every caller"
fi
# ---------------------------------------------------------------------------
# (Q) THE LABEL SWEEP — roborev job 371, and the class it belongs to.
#
# FOUR findings across three rounds were ONE shape: a census label or count asserting
# something the component row contradicts.
#   1. the progress line printed PASS while the SUMMARY said VACUOUS               (job 368)
#   2. a FAILing gap: component counted under DECLARED-GAP, not not-applicable  (audit LOW 1)
#   3. NOT-APPLICABLE labelled `(SKIP/FAIL)` on a row that PASSes                 (job 371)
#   4. the ZERO state counted under the heading `VACUOUS` — a STATUS word derived from a
#      STATE. NOT cited: found by sweeping for siblings of 3, and reproduced in a SHIPPING
#      mode (`--lite-aggregate-selftest` emitted `fmt: VACUOUS (0s)` beside
#      `0 VACUOUS (RECOGNISED)`).
#
# THE RULE: a label may name a STATUS only if it was DERIVED from the observed status.
# `census_summary_line` therefore takes name/STATUS PAIRS; before job 371 it took names
# alone, so every status word in it was necessarily an assumption about which statuses
# reach a given state.
#
# THIS SECTION IS THE SWEEP AS AN EXECUTABLE TABLE. It drives EVERY (status x census-state)
# pair through the real aggregate and the real per-row renderer, and asserts of each that
# no rendered word contradicts the status. That is what makes the class closed mechanically
# rather than by my reading of it — a NEW state or a NEW label joins the table or it fails
# the count floor below.
# ---------------------------------------------------------------------------
LOG_DIR="$tmp/sweeplogs"; mkdir -p "$LOG_DIR"
q_bad=()
q_n=0
# <component>|<status>|<sidecar record, or '-' for none>|<expected aggregate field>
#
# THE SUBJECT IS CHOSEN PER CELL (#3625/job 379): the classifier resolves the DECLARATION and
# the STATUS before it looks at the sidecar, so a cell must use a component whose KIND makes
# its state REACHABLE — one made-up subject with a planted record no longer stands in for
# every kind, and pretending otherwise would test a state machine that does not exist.
for spec in \
  'tombstones-scan|PASS|COUNT 12 tests passed|AFFIRMED' \
  'fmt|PASS|-|DECLARED-GAP' \
  'tombstones-scan|PASS|-|NOT-MEASURED' \
  'scoped-tests|PASS|NOT-APPLICABLE the diff routed to no rust package and no python tier|no-subject' \
  'feature-iso-parquet|VACUOUS|ZERO test binaries|measured-ZERO' \
  'tombstones-scan|VACUOUS|-|NOT-MEASURED' \
  'fmt|FAIL|-|not-applicable' \
  'fmt|SKIP|-|not-applicable' \
  'tombstones-scan|FAIL|COUNT 12 tests passed|not-applicable' \
  'a-brand-new-component|FAIL|-|UNDECLARED' \
  'a-brand-new-component|SKIP|-|UNDECLARED' \
  'a-brand-new-component|PASS|-|UNDECLARED' \
  'tombstones-scan|PASS|WHATEVER an unplanned token|unrecognised' \
  ; do
  q_n=$((q_n + 1))
  q_comp=${spec%%|*}; q_rest=${spec#*|}
  q_st=${q_rest%%|*}; q_rest=${q_rest#*|}
  q_rec=${q_rest%|*}; q_want=${q_rest##*|}
  rm -f "$(_census_sidecar "$q_comp")"
  [ "$q_rec" = '-' ] || _census_write "$q_comp" "$q_rec"
  q_line=$(census_summary_line "$q_comp" "$q_st")
  # The expected bucket must hold exactly 1, and the OTHER status-derived buckets must
  # hold 0 — a label that fires for two different pairs is the defect, not the count.
  case "$q_want" in
    AFFIRMED)       q_got=$(printf '%s' "$q_line" | sed -n 's/^census: \([0-9]*\)\/1 .*/\1/p') ;;
    DECLARED-GAP)   q_got=$(printf '%s' "$q_line" | sed -n 's/.*; \([0-9]*\) DECLARED-GAP .*/\1/p') ;;
    NOT-MEASURED)   q_got=$(printf '%s' "$q_line" | sed -n 's/.*; \([0-9]*\) NOT-MEASURED (RECOGNISED).*/\1/p') ;;
    measured-ZERO)  q_got=$(printf '%s' "$q_line" | sed -n 's/.*; \([0-9]*\) measured-ZERO .*/\1/p') ;;
    not-applicable) q_got=$(printf '%s' "$q_line" | sed -n 's/.*; \([0-9]*\) not-applicable (component did not PASS).*/\1/p') ;;
    no-subject)     q_got=$(printf '%s' "$q_line" | sed -n 's/.*; \([0-9]*\) no-subject (PASSed.*/\1/p') ;;
    UNDECLARED)     q_got=$(printf '%s' "$q_line" | sed -n 's/.*; \([0-9]*\) UNDECLARED;.*/\1/p') ;;
    unrecognised)   q_got=$(printf '%s' "$q_line" | sed -n 's/.*; \([0-9]*\) unrecognised;.*/\1/p') ;;
    *)              q_got="(no extractor for $q_want)" ;;
  esac
  [ "$q_got" = 1 ] || q_bad+=("[$q_comp $q_st / ${q_rec%% *}] expected 1 in '$q_want', got '$q_got'")
  # THE STATUS-DERIVED FIGURES must track the STATUS, never the state.
  q_vac=$(printf '%s' "$q_line" | sed -n 's/.*; \([0-9]*\) row(s) carry a VACUOUS status.*/\1/p')
  if [ "$q_st" = VACUOUS ]; then
    [ "$q_vac" = 1 ] || q_bad+=("[$q_comp $q_st / ${q_rec%% *}] a VACUOUS row was counted as '$q_vac' VACUOUS")
  else
    [ "$q_vac" = 0 ] || q_bad+=("[$q_comp $q_st / ${q_rec%% *}] a non-VACUOUS row was counted as '$q_vac' VACUOUS")
  fi
  # …and no rendered word may assert the WRONG status. The two directions that bit:
  q_np=$(printf '%s' "$q_line" | sed -n 's/.*; \([0-9]*\) not-applicable (component did not PASS).*/\1/p')
  q_ns=$(printf '%s' "$q_line" | sed -n 's/.*; \([0-9]*\) no-subject (PASSed.*/\1/p')
  if [ "$q_st" = PASS ] && [ "$q_np" != 0 ]; then
    q_bad+=("[$q_comp $q_st / ${q_rec%% *}] a PASSing row was counted as 'did not PASS'")
  fi
  if [ "$q_st" != PASS ] && [ "$q_ns" != 0 ]; then
    q_bad+=("[$q_comp $q_st / ${q_rec%% *}] a non-PASSing row was counted as 'PASSed'")
  fi
  # EVERY ROW LANDS IN EXACTLY ONE STATE BUCKET: the seven must sum to N, or a row is
  # being double-counted or silently dropped. Each field is extracted on its OWN — a single
  # whole-line regex would fail as a unit on any wording change and report "could not
  # extract" for all seven, which says nothing about which one moved.
  # EXPLICIT per-field extractors, not a regex built from the label text: escaping a
  # label containing `(` into a sed program is a second grammar to get wrong, and it was
  # (`Unmatched \(`). One hand-written pattern per field, each naming its own field.
  q_tot=0
  q_missing=""
  _qf() { # _qf <label> <sed-program>
    local v
    v=$(printf '%s' "$q_line" | sed -n "$2")
    case "$v" in
      ''|*[!0-9]*) q_missing="$q_missing '$1'" ;;
      *)           q_tot=$(( q_tot + v )) ;;
    esac
  }
  _qf 'AFFIRMED'       's/^census: \([0-9][0-9]*\)\/.*/\1/p'
  _qf 'DECLARED-GAP'   's/.*; \([0-9][0-9]*\) DECLARED-GAP .*/\1/p'
  _qf 'NOT-MEASURED'   's/.*; \([0-9][0-9]*\) NOT-MEASURED (RECOGNISED).*/\1/p'
  _qf 'measured-ZERO'  's/.*; \([0-9][0-9]*\) measured-ZERO .*/\1/p'
  _qf 'not-applicable' 's/.*; \([0-9][0-9]*\) not-applicable (component did not PASS).*/\1/p'
  _qf 'no-subject'     's/.*; \([0-9][0-9]*\) no-subject (PASSed.*/\1/p'
  _qf 'UNDECLARED'     's/.*; \([0-9][0-9]*\) UNDECLARED;.*/\1/p'
  _qf 'unrecognised'   's/.*; \([0-9][0-9]*\) unrecognised;.*/\1/p'
  if [ -n "$q_missing" ]; then
    q_bad+=("[$q_comp $q_st / ${q_rec%% *}] these aggregate fields could not be read, so the bucket sum measured nothing:$q_missing")
  elif [ "$q_tot" != 1 ]; then
    q_bad+=("[$q_comp $q_st / ${q_rec%% *}] the state buckets sum to $q_tot, not 1 — a row is double-counted or dropped")
  fi
  # THE PER-ROW SUFFIX MUST NOT ASSERT A STATUS THE ROW DOES NOT HAVE. A bare scan for
  # status WORDS is the wrong instrument and was tried: `{no census: component ended FAIL,
  # so there is no PASS to affirm}` legitimately contains "PASS", and a guard that reds on
  # correct prose is the guard agents learn to waive. What IS checkable is the one place
  # the suffix makes a status CLAIM — `component ended <X>` — which must name the row's own
  # status, because it is derived from it.
  q_ann=$(_census_annotate "$q_comp" "$q_st")
  case "$q_ann" in
    *'component ended '*)
      q_claim=$(printf '%s' "$q_ann" | sed -n 's/.*component ended \([A-Z][A-Z-]*\).*/\1/p')
      [ "$q_claim" = "$q_st" ] || q_bad+=("[$q_comp $q_st / ${q_rec%% *}] the row suffix CLAIMS 'component ended $q_claim' on a $q_st row") ;;
  esac
done
if [ "$q_n" -ne 13 ]; then
  bad "Q1: only $q_n of the 13 (component x status x census-state) cells were exercised — the sweep table is not iterating, so a green here would certify nothing"
elif [ "${#q_bad[@]}" -eq 0 ]; then
  ok "Q1 (the sweep): all $q_n (component x status x census-state) cells — including the NO-SIDECAR fallback for a gap:, a log-measured and an undeclared kind (#3625/job 379) — land in exactly ONE state bucket, the buckets sum to N, every status-naming qualifier tracks the OBSERVED status, and no per-row suffix names a foreign status"
else
  bad "Q1: ${q_bad[*]}"
fi
# `_census_kind` must not have grown a state the sweep does not cover. DERIVED from the
# renderer, so a new census state cannot join without joining this table.
q_states=$(sed -n '/^_census_annotate() {/,/^}$/p' "$GATE" \
  | sed -n 's/^[[:space:]]*\(COUNT\|ZERO\|NOT-MEASURED\|GAP\|NOT-APPLICABLE\|UNDECLARED\)).*/\1/p' | sort -u | grep -c .)
if [ "$q_states" -eq 6 ]; then
  ok "Q2: the renderer knows exactly the 6 census states this sweep enumerates (COUNT, ZERO, NOT-MEASURED, GAP, NOT-APPLICABLE, UNDECLARED) plus its unrecognised catch-all"
else
  bad "Q2: _census_annotate renders $q_states named states, not the 6 the sweep covers — a state was added or removed without joining the table"
fi
# THE AGGREGATE TAKES THE STATUS. Structural, because a call site that reverted to
# names-only would silently make every status qualifier an assumption again — and the
# ODD-argument refusal is what turns such a site into a loud failure rather than a
# dropped row.
q_sites=$(grep -cE '^[^#]*census_summary_line ' "$GATE")
q_zips=$(grep -cF 'for _ci in "${!' "$GATE")
if [ "$q_sites" -ge 7 ] && [ "$q_zips" -ge 6 ]; then
  ok "Q3: $q_sites aggregate emit sites, $q_zips of them zipping name/STATUS pairs (the boundary printer collects its pairs inline) — no site passes names alone"
else
  bad "Q3: $q_sites emit site(s) and $q_zips zip(s) — a site may be passing names without statuses"
fi
case "$(census_summary_line lonely-name)" in
  'census: MALFORMED'*'ODD argument count (1)'*)
    ok "Q4: an ODD argument count is a NAMED refusal — a call site that forgot to zip its statuses fails loudly instead of emitting a line that silently omits a row" ;;
  *) bad "Q4: got '$(census_summary_line lonely-name)'" ;;
esac
# The KEYS-expansion guard, pinned because getting it wrong is SILENT AND CATASTROPHIC and
# I reproduced it in this very change: `"${!arr[@]+...}"` is read by bash as INDIRECT
# expansion ("invalid variable name") and ABANDONS the enclosing block — written that way,
# `--emit-summary-selftest` fell straight through into a REAL 37-component gate. The
# repository already documented this at run_delta's own keys loop.
if grep -qF 'for _ci in "${!' "$GATE" && ! grep -qE '^[^#]*for _ci in "\$\{!(NAMES|DN)\[@\]\+' "$GATE"; then
  ok "Q5: every census zip guards its KEYS expansion with a count check, never the '+' idiom (which bash reads as indirect expansion and which silently abandons the enclosing block)"
else
  bad "Q5: a census zip uses the '+' guard on a KEYS expansion — that aborts the block it is in"
fi
# ---------------------------------------------------------------------------
# (R) THE FOURTH STATUS TOKEN REACHES EVERY STATUS-SET LITERAL — roborev job 376, finding 2
#     and the sweep it prompted.
#
# #3625 added VACUOUS to a vocabulary that was PASS/FAIL/SKIP, so every hard-coded
# three-token alternation became WRONG the moment it landed — and wrong in the direction
# that is hardest to notice, because such a pattern stops SEEING exactly the rows that
# report a component verified nothing. Measured at the time of the sweep, THREE sites had
# it, and only one was cited:
#   * test_agent_gate_tree_provenance.sh's boundary `n_rows` — CITED. Reds on CORRECT
#     input: a legitimate VACUOUS boundary row went uncounted there while the annotation
#     count beside it (added with the census) counted it, so the two disagreed and the
#     consistency assert failed on a healthy block.
#   * test_agent_gate_summary.sh's 3453-annot-b UNDECLARED/UNCLASSIFIED screen — blind to
#     VACUOUS rows, i.e. blind on the rows most worth screening.
#   * test_agent_gate_summary.sh's 3453-annot-c RESULT:-embedding screen — same.
#
# THE GUARD IS THE EXACT THREE-TOKEN GROUP, not "a line mentioning PASS". That distinction
# is what keeps it off the roborev review-block grammar, whose verdict vocabulary
# legitimately begins with those three and then continues with NOTICE, UNAVAILABLE and the
# rest — a different vocabulary for a different artifact, and reddening it would be a guard
# that fires on correct input.
#
# THE NEEDLE IS SPLIT so this guard cannot match its OWN source. It did on the first run:
# a self-matching grep is a guard that is always red, which is the guard nobody keeps. For
# the same reason the group is never written out in this file's prose.
# ---------------------------------------------------------------------------
r_needle="(PASS|FAIL|""SKIP)"
r_hits=$(grep -rlF -- "$r_needle" "$REPO_ROOT/scripts" 2>/dev/null | sort || true)
if [ -z "$r_hits" ]; then
  ok "R1 (job 376 F2 + sweep): no script enumerates the component-status set as a bare three-token PASS/FAIL/SKIP alternation — VACUOUS is a status now, and a pattern that omits it stops seeing the rows that report a component verified nothing"
else
  bad "R1: three-token component-status alternation(s) survive (VACUOUS omitted) in: $(printf '%s' "$r_hits" | tr '\n' ' ') — remedy: add VACUOUS to each, or (if the site is a different artifact's vocabulary) extend that alternation so it is not the bare three"
fi
# The guard must be DISCRIMINATING and must not fire on the roborev block's own, longer
# vocabulary — asserted both ways so a future narrowing or widening of R1 is caught.
# Line 1 is the DEFECT shape (the bare three), line 2 is the roborev block's LONGER
# vocabulary, which begins with the same three tokens and must NOT match. Both are composed
# from fragments so neither this source nor the probe file can be confused for the other.
r_open="(PASS|FAIL|"
r_probe="$tmp/r-probe.txt"
printf '%s\n' "grep -cE '^[a-z-]*: +${r_open}SKIP) '"                > "$r_probe"
printf '%s\n' "grep -qE '${r_open}SKIP|NOTICE|UNAVAILABLE)'"        >> "$r_probe"
r_bad=$(grep -cF -- "$r_needle" "$r_probe" || true)
if [ "$r_bad" = 1 ]; then
  ok "R2: the R1 needle matches the bare three-token group and NOT the roborev block's longer verdict vocabulary — it cannot red a correct artifact that happens to name the same first three tokens"
else
  bad "R2: the R1 needle matched $r_bad of 2 probe lines (want exactly 1) — it is either blind to the defect or firing on the roborev grammar"
fi
# …and the two suites that carry the component-row patterns must positively RECOGNISE a
# VACUOUS row, not merely have had the literal edited.
r_missing=()
grep -qE 'PASS\|FAIL\|SKIP\|VACUOUS\) \\\(\[0-9\]\+s' "$REPO_ROOT/scripts/tests/test_agent_gate_tree_provenance.sh" \
  || r_missing+=("tree-provenance-boundary-row-count")
grep -qE 'PASS\|FAIL\|SKIP\|VACUOUS\)\.\*\\\[\(UNDECLARED' "$REPO_ROOT/scripts/tests/test_agent_gate_summary.sh" \
  || r_missing+=("summary-3453-annot-b-undeclared-screen")
if [ "${#r_missing[@]}" -eq 0 ]; then
  ok "R3: the boundary row count and the UNDECLARED screen both recognise a VACUOUS component row"
else
  bad "R3: still blind to VACUOUS rows: ${r_missing[*]}"
fi
# ---------------------------------------------------------------------------
# (S) THE TWO PATHS CONVERGE, AND THEIR AGREEMENT IS TESTED — roborev job 379.
#
# THE DEFECT: `_census_measure` (verdict time) and `_census_record` (render time) answer the
# SAME question — the truthful census state for (component, status) — and answered it
# DIFFERENTLY for five rounds, because they were two implementations of it. The batch-2 LOW
# fix ("a component that did not PASS has no PASS to affirm, whatever its kind") landed in
# the measurer and COULD NOT land in the fallback, because the fallback was not given the
# status: it dispatched on kind alone, so a gap-declared component that CRASHED before
# record_result rendered its GAP reason and was counted as DECLARED-GAP.
#
# Same structural root as job 371 one function over — *a function required to reason about
# status that is not handed the status* — which is why the fix is a CONVERGENCE (both paths
# now call `_census_classify`) rather than a sixth label patch.
#
# THE ONE SURVIVING ASYMMETRY, declared rather than assumed: the measurer may read the
# component log and write a sidecar; the renderer runs in the PARENT after the component's
# lane is gone and must do neither. So the classifier returns `MEASURE <kind>` for the ONE
# cell that genuinely needs the log — PASS x a log-measured kind — and the paths may differ
# only there. Everything else must be byte-identical, and this section drives BOTH over the
# same matrix to prove it, because a second implementation's agreement is only knowable by
# testing it.
# ---------------------------------------------------------------------------
LOG_DIR="$tmp/converge"; mkdir -p "$LOG_DIR"
s_bad=()
s_cells=0
s_measure_cells=0
# One representative component per declared kind, DERIVED so a new kind cannot skip the
# matrix: every kind _census_kind can return must have a subject here.
s_subjects='fmt:gap tombstones-scan:libtest feature-iso-parquet:compile integration-tests:both python-bindings:indirect shell-selftests:self scoped-tests:runtime a-brand-new-component:undeclared'
for s_pair in $s_subjects; do
  s_comp=${s_pair%%:*}; s_kindname=${s_pair##*:}
  for s_st in PASS FAIL SKIP VACUOUS; do
    for s_side in absent present; do
      s_cells=$((s_cells + 1))
      rm -f "$(_census_sidecar "$s_comp")"
      [ "$s_side" = present ] && _census_write "$s_comp" "COUNT 7 tests passed (a pre-existing sidecar)"
      # The RENDER path (may not measure) and the CLASSIFIER the measure path uses, over the
      # identical inputs. Comparing the classifier rather than _census_measure itself keeps
      # the measure path's log read — which the renderer structurally cannot do — out of the
      # comparison, which is exactly the declared asymmetry.
      s_rec=$(_census_read "$s_comp") || s_rec=""
      s_measure=$(_census_classify "$s_comp" "$s_st" "$s_rec" 1)
      s_render=$(_census_record "$s_comp" "$s_st")
      case "$s_measure" in
        MEASURE\ *)
          s_measure_cells=$((s_measure_cells + 1))
          # The ONLY cell allowed to differ, and it must be the DECLARED one.
          if [ "$s_st" != PASS ]; then
            s_bad+=("[$s_comp/$s_kindname $s_st $s_side] the measure path wants the LOG on a non-PASS row — the status check is not above the kind dispatch")
          fi
          case "$s_kindname" in
            libtest|compile|both|indirect) ;;
            *) s_bad+=("[$s_comp/$s_kindname $s_st $s_side] a NON-log-measured kind asked to read the log") ;;
          esac ;;
        *)
          [ "$s_measure" = "$s_render" ] \
            || s_bad+=("[$s_comp/$s_kindname $s_st $s_side] DIVERGENCE — measure='$s_measure' render='$s_render'") ;;
      esac
    done
  done
done
rm -f "$(_census_sidecar fmt)" "$(_census_sidecar tombstones-scan)" "$(_census_sidecar python-bindings)"
if [ "$s_cells" -ne 64 ]; then
  bad "S1: only $s_cells of the 64 (kind x status x sidecar) cells were exercised — the matrix is not iterating, so a green here would certify nothing"
elif [ "${#s_bad[@]}" -eq 0 ]; then
  ok "S1 (job 379, the convergence): across all $s_cells (kind x status x sidecar) cells the verdict-time and render-time paths produce IDENTICAL state, except the $s_measure_cells cells that are the declared asymmetry (PASS x a log-measured kind, where only the measurer may read the log)"
else
  bad "S1: ${s_bad[*]}"
fi
# THE CITED CELL, asserted by name on both paths — a gap-declared component that CRASHED
# before record_result, so there is no sidecar and the status is a synthetic FAIL.
rm -f "$(_census_sidecar fmt)"
s_crash=$(_census_record fmt FAIL)
case "$s_crash" in
  'NOT-APPLICABLE component ended FAIL'*)
    ok "S2 (the cited finding): a gap-declared component that crashed before record_result renders NOT-APPLICABLE, not its GAP reason — the aggregate no longer counts a crashed component as DECLARED-GAP" ;;
  *) bad "S2: got '$s_crash'" ;;
esac
case "$(census_summary_line fmt FAIL)" in
  *'0 DECLARED-GAP (RECOGNISED)'*'1 not-applicable (component did not PASS)'*)
    ok "S2b: …and the aggregate counts it under not-applicable, with DECLARED-GAP at zero" ;;
  *) bad "S2b: got '$(census_summary_line fmt FAIL)'" ;;
esac
case "$(_census_record fmt PASS)" in
  'GAP cargo fmt'*) ok "S3 (control): the same component on a PASS still renders its declared GAP — S2 is the status check, not the gap arm being lost" ;;
  *) bad "S3: got '$(_census_record fmt PASS)'" ;;
esac
# STRUCTURAL: neither path may regrow its own copy of the classification. A second
# implementation is what these five rounds have been paying for.
s_struct=()
grep -q '_census_classify ' <<<"$(sed -n '/^_census_measure() {/,/^}$/p' "$GATE")" \
  || s_struct+=("_census_measure-does-not-delegate-to-_census_classify")
grep -q '_census_classify ' <<<"$(sed -n '/^_census_record() {/,/^}$/p' "$GATE")" \
  || s_struct+=("_census_record-does-not-delegate-to-_census_classify")
# …and the renderer must never be handed permission to measure.
grep -q '_census_classify "$1" "${2:-}" "$rec" 0' <<<"$(sed -n '/^_census_record() {/,/^}$/p' "$GATE")" \
  || s_struct+=("_census_record-does-not-pass-may-measure=0")
# The status has to REACH both: the ONE renderer must forward it to the annotation.
grep -q '_census_annotate "$1" "$2"' <<<"$(sed -n '/^_fm_summary_line() {/,/^}$/p' "$GATE")" \
  || s_struct+=("_fm_summary_line-does-not-forward-the-status-to-the-census-annotation")
if [ "${#s_struct[@]}" -eq 0 ]; then
  ok "S4: both paths delegate to the ONE classifier, the renderer is denied permission to measure, and the status reaches the per-row annotation"
else
  bad "S4: ${s_struct[*]}"
fi
# ---------------------------------------------------------------------------
# (T) node-tests CENSUSES THE WORK, NOT THE INPUTS — roborev job 383.
#
# THE DEFECT, and it is this issue's own thesis violated inside its fix. #3625 says "a
# duration is a PROXY for work; a COUNT is the work" — and `node-tests` was censusing
# `n_targets`, THE NUMBER OF CHANGED FILES THE LANE SELECTED, which is simply a better
# proxy. It was wrong in BOTH directions at once:
#   * jest EXITS 0 when every selected test is skipped, so an all-skipped run of many files
#     reported a confident `COUNT n` and kept its PASS — the vacuous run this whole
#     subsystem exists to catch, waved through by the detector;
#   * a changed HELPER (a non-`*.test.js` file) runs the WHOLE suite and was censused as
#     ONE "jest test file".
#
# THE FIX REUSES THE EXISTING TALLY rather than adding a second parser: the lane is now
# `indirect:jest`, the same path `node-bindings` takes, so there is ONE implementation of
# "what did jest report" — the rounds before this one were all about two implementations of
# one question drifting. The old `self:` rationale ("it deletes its log, so no log-reading
# measurer could census it") was an IMPLEMENTATION CHOICE, not a constraint; the lane writes
# to $LOG_DIR now and keeps the log.
#
# DRIVEN THROUGH THE REAL FUNCTION. `_delta_node_targets` is stubbed (it is the diff
# classifier, not the subject) and `node` is a PATH shim emitting a chosen jest summary —
# which is what makes the two arms differ in exactly ONE property, the tally jest reports.
# Everything else runs: the real filter derivation, the real `bash -c` body, the real
# `_census_finalize`, the real `_status_is_nonfailing` OVERALL flip.
# ---------------------------------------------------------------------------
t_root="$tmp/nodedelta"; mkdir -p "$t_root/bindings/node/scripts"
: > "$t_root/bindings/node/scripts/generate-loader.mjs"
t_shimdir="$tmp/nodeshim"; mkdir -p "$t_shimdir"
cat > "$t_shimdir/node" <<'NODESHIM'
#!/usr/bin/env bash
# Stands in for BOTH `node scripts/generate-loader.mjs` (silent, rc 0) and
# `node --expose-gc ./node_modules/jest/bin/jest.js …`, which prints the summary the case
# chose. jest exits 0 for an all-skipped run, so this shim does too — that is the property
# under test, not an accident of the double.
case "$*" in
  *generate-loader*) exit 0 ;;
esac
printf '%s\n' "${CQLITE_TEST_JEST_SUMMARY:?}"
exit 0
NODESHIM
chmod +x "$t_shimdir/node"

# t_delta_node <jest-summary> ; echoes "<pushed-status>|<OVERALL>|<census state>"
t_delta_node() {
  (
    _delta_node_targets() { printf '%s\n' 'bindings/node/__test__/a.test.js'; printf '%s\n' 'bindings/node/__test__/b.test.js'; }
    REPO_ROOT="$t_root"
    LOG_DIR="$tmp/nodedelta-logs"; mkdir -p "$LOG_DIR"
    rm -f "$LOG_DIR/node-tests.log" "$LOG_DIR/node-tests.census"
    CQLITE_DATASETS_ROOT=""
    DELTA_EXECUTORS=""
    OVERALL=PASS
    NAMES=(); STATUSES=(); TIMES=()
    CQLITE_TEST_JEST_SUMMARY="$1"
    export CQLITE_TEST_JEST_SUMMARY
    PATH="$t_shimdir:$PATH"
    run_delta_node_tests 'bindings/node/__test__/a.test.js' >/dev/null 2>&1
    printf '%s|%s|%s' "${STATUSES[0]:-(none)}" "$OVERALL" "$(_census_record node-tests "${STATUSES[0]:-}")"
  )
}
t_skipped=$(t_delta_node 'Test Suites: 2 skipped, 2 total
Tests:       12 skipped, 12 total')
case "$t_skipped" in
  'VACUOUS|FAIL|ZERO jest tests'*)
    ok "T1 (job 383): an ALL-SKIPPED delta node run — which jest exits 0 for — measures ZERO, is pushed as VACUOUS and FAILs the run. Censusing the 2 changed files it selected would have reported 'COUNT 2' and kept the PASS" ;;
  *) bad "T1: got '$t_skipped' (want VACUOUS|FAIL|ZERO …)" ;;
esac
t_real=$(t_delta_node 'Test Suites: 2 passed, 2 total
Tests:       1 skipped, 41 passed, 42 total')
case "$t_real" in
  'PASS|PASS|COUNT 41 jest tests passed')
    ok "T2 (positive control): the SAME lane, differing in ONE property — the tally jest reports — measures 41 tests passed and keeps its PASS. T1 is the census, not the harness" ;;
  *) bad "T2: got '$t_real' (want PASS|PASS|COUNT 41 jest tests passed)" ;;
esac
# THE SECOND DIRECTION of the same defect: a changed HELPER runs the WHOLE suite. The census
# must report what jest ran, not the one file that triggered it.
t_helper=$(
  (
    _delta_node_targets() { printf '%s\n' 'bindings/node/__test__/setup.js'; }
    REPO_ROOT="$t_root"
    LOG_DIR="$tmp/nodedelta-logs2"; mkdir -p "$LOG_DIR"
    CQLITE_DATASETS_ROOT=""; DELTA_EXECUTORS=""; OVERALL=PASS
    NAMES=(); STATUSES=(); TIMES=()
    CQLITE_TEST_JEST_SUMMARY='Tests:       0 skipped, 137 passed, 137 total'
    export CQLITE_TEST_JEST_SUMMARY
    PATH="$t_shimdir:$PATH"
    run_delta_node_tests 'bindings/node/__test__/setup.js' >/dev/null 2>&1
    printf '%s' "$(_census_record node-tests "${STATUSES[0]:-}")"
  )
)
case "$t_helper" in
  'COUNT 137 jest tests passed')
    ok "T3: ONE changed helper runs the WHOLE suite, and the census says 137 tests — not the '1 jest test file' the input count would have claimed. The proxy was wrong in both directions; the tally is wrong in neither" ;;
  *) bad "T3: got '$t_helper' (want COUNT 137 jest tests passed)" ;;
esac
# The declaration must be the SHARED jest path, not a second parser.
t_kind=$(_census_kind node-tests) || t_kind='(undeclared)'
if [ "$t_kind" = 'indirect:jest' ] && [ "$(_census_kind node-bindings)" = 'indirect:jest' ]; then
  ok "T4: node-tests and node-bindings both declare indirect:jest — ONE implementation of 'what did jest report', which is what the previous rounds were spent on"
else
  bad "T4: node-tests is '$t_kind' and node-bindings is '$(_census_kind node-bindings)' — the two jest lanes do not share a tally"
fi
# …and the lane must KEEP its log, or the measurer has nothing to read.
t_body=$(sed -n '/^run_delta_node_tests() {/,/^}$/p' "$GATE")
t_bad=()
grep -q 'log="$LOG_DIR/node-tests.log"' <<<"$t_body" || t_bad+=("does-not-log-into-LOG_DIR")
grep -qE '^[[:space:]]*rm -f "\$log"' <<<"$t_body" && t_bad+=("still-deletes-its-log-before-the-census-can-read-it")
# COMMENT-BLIND: the body now carries a comment EXPLAINING why the declare is gone, and a
# bare substring test would read that explanation as the thing it forbids — the artifact
# describing the rule becoming a violation of it (#3312's shape, and the same reason this
# repo's other structural scans strip comments first).
grep -vE '^[[:space:]]*#' <<<"$t_body" | grep -q '_census_declare' \
  && t_bad+=("still-declares-an-input-count-instead-of-measuring-the-work")
if [ "${#t_bad[@]}" -eq 0 ]; then
  ok "T5: the lane writes its jest output to \$LOG_DIR and keeps it, and no longer self-declares an input count"
else
  bad "T5: ${t_bad[*]}"
fi

# ---------------------------------------------------------------------------
# (U) THE shell-selftests RULING — stated, not assumed (roborev job 383).
#
# The other `self:` lane counts SCRIPTS EXECUTED, and the question "is that the same defect"
# deserves an answer that lives somewhere, not a seventh reviewer asking it. The position,
# and the two facts it rests on:
#   (1) SELECTED == EXECUTED. `_run_shell_selftest_files` invokes every file it is handed,
#       unconditionally — no skip layer, no filter. That is the fact that distinguishes it
#       from node-tests, whose count was of SELECTIONS while jest decided separately how many
#       to run.
#   (2) There is no uniform per-script assertion tally to prefer instead; deriving one across
#       heterogeneous shell guards would be the curation this census refuses.
# So the subject is genuinely the script. DECLARED RESIDUAL, recorded rather than hidden: a
# script that runs and asserts nothing is invisible to this count — the census records a
# COUNT, not a TRUTH.
# ---------------------------------------------------------------------------
u_runner=$(sed -n '/^_run_shell_selftest_files() {/,/^}$/p' "$GATE")
if [ -z "$u_runner" ]; then
  bad "U1: _run_shell_selftest_files not found — the ruling's first premise cannot be checked"
elif grep -qE '^[[:space:]]*(if )?bash "\$REPO_ROOT/\$f"' <<<"$u_runner" \
     && ! grep -qiE 'skip|filter|continue[[:space:]]*$' <<<"$(grep -v '\[ -n "\$f" \]' <<<"$u_runner")"; then
  ok "U1 (the ruling's premise, MEASURED not assumed): _run_shell_selftest_files invokes every file it is handed with no skip or filter layer — so for this lane selected == executed, which is exactly what was NOT true of node-tests"
else
  bad "U1: _run_shell_selftest_files has grown a skip/filter path — selected != executed, so 'scripts executed' is now an input count and this lane needs node-tests' treatment"
fi
u_decl=$(sed -n '/^_census_kind() {/,/^}$/p' "$GATE")
if grep -q 'SELECTED == EXECUTED' <<<"$u_decl" && grep -q 'DECLARED RESIDUAL' <<<"$u_decl"; then
  ok "U2: the ruling — why this lane's subject IS the script, and what it does not cover — is recorded AT THE DECLARATION, where the next reader of _census_kind will find it"
else
  bad "U2: the shell-selftests ruling is not recorded at _census_kind, so the question has no answer in the code"
fi
# ---------------------------------------------------------------------------
# (W) A LOST SIDECAR WRITE MUST NOT BUY A GREEN — roborev job 400.
#
# `_census_write` is deliberately BEST-EFFORT, inherited from `_fm_note`, whose comment
# argues it correctly: "a failed append must never fail the component whose matrix it
# describes — the consequence of a lost append is a visibly incomplete annotation, never a
# wrong one." THAT REASONING WAS TRUE FOR THE FEATURE MATRIX AND IS FALSE FOR THE CENSUS,
# because the census now drives a VERDICT: the `self:`/`runtime:` paths computed a record,
# threw the value away, and finalized by RE-READING the sidecar — so a failed write turned a
# computed ZERO into NOT-MEASURED, and NOT-MEASURED preserves PASS. A filesystem hiccup
# bought a false green in a merge gate.
#
# It is CLAUDE.md's recorded shape one directory over: a fail-closed argument holds only for
# the consumers that existed when it was written, and a NEW consumer for which the permissive
# direction is unsafe inverts it SILENTLY.
#
# The write is made to fail the way a real one would — the sidecar PATH is occupied by a
# DIRECTORY, so the `printf >` redirect cannot create it — rather than by stubbing
# `_census_write`, which would test a double instead of the shipped best-effort helper.
# ---------------------------------------------------------------------------
LOG_DIR="$tmp/writefail"; mkdir -p "$LOG_DIR"
w_side="$(_census_sidecar shell-selftests)"
rm -rf "$w_side"; mkdir -p "$w_side"
# The sabotage must actually bite, or everything below proves nothing.
if _census_write shell-selftests 'probe' && [ ! -f "$w_side" ] && ! _census_read shell-selftests >/dev/null 2>&1; then
  ok "W0 (control): the sidecar write is genuinely failing and unreadable at '$w_side' — the case below exercises the lost-write path"
else
  bad "W0: the sabotage did not take effect — _census_write could still persist a record, so W1/W2 would prove nothing"
fi
w_rec=$(_census_declare shell-selftests 0 'changed scripts/tests/*.sh executed')
if [ "$w_rec" = 'ZERO changed scripts/tests/*.sh executed' ]; then
  ok "W1: the producer RETURNS the record it computed, so the value survives a write it could not persist"
else
  bad "W1: _census_declare returned '$w_rec' (want the computed ZERO record) — the value is still being thrown away"
fi
w_st=$(_census_finalize shell-selftests PASS "$w_rec")
if [ "$w_st" = VACUOUS ] && ! _status_is_nonfailing "$w_st"; then
  ok "W2 (job 400): a computed ZERO still becomes VACUOUS when the sidecar write FAILED — the verdict comes from the value, not from a re-read of a best-effort file"
else
  bad "W2: got '$w_st' (want VACUOUS, and it must be failing) — a lost write is still converting a computed ZERO into a PASS"
fi
# RED control, inline: the PRE-FIX call shape — finalize WITHOUT the computed value, so it
# re-reads the sidecar that does not exist. It must NOT yield VACUOUS, or W2 is passing for
# a reason unrelated to the fix.
w_old=$(_census_finalize shell-selftests PASS)
if [ "$w_old" = VACUOUS ]; then
  bad "W2 RED: the pre-fix shape (re-reading the sidecar) ALSO returned VACUOUS, so this case does not demonstrate that finalizing from the value is what fixes it"
else
  ok "W2 RED: the pre-fix shape (re-read, no computed value) returns '$w_old' on the same lost write — PASS-preserving, which is exactly the false green job 400 found"
fi
# …and the CONTROL that the fix did not simply hard-wire VACUOUS: a real COUNT still passes
# through the same lost-write path.
w_rec2=$(_census_declare shell-selftests 4 'changed scripts/tests/*.sh executed')
w_st2=$(_census_finalize shell-selftests PASS "$w_rec2")
if [ "$w_st2" = PASS ]; then
  ok "W3 (control): a real COUNT through the SAME failed-write path still PASSes — W2 is the ZERO, not the lost write reddening everything"
else
  bad "W3: got '$w_st2' (want PASS)"
fi
rm -rf "$w_side"
# The runtime: producer must return its record too — same defect, same fix, different lane.
LOG_DIR="$tmp/writefail2"; mkdir -p "$LOG_DIR"
w_side2="$(_census_sidecar scoped-tests)"
rm -rf "$w_side2"; mkdir -p "$w_side2"
printf 'Compiling cqlite-py v0.1.0\n61 skipped in 1.20s\n' > "$LOG_DIR/scoped-tests.log"
w_rec3=$(_census_scoped_record scoped-tests 0 1 'python-tier: PASS (…)')
w_st3=$(_census_finalize scoped-tests PASS "$w_rec3")
case "$w_rec3|$w_st3" in
  'ZERO pytest tests'*'|VACUOUS')
    ok "W4: the runtime: producer also RETURNS its record, so an all-skipped python tier is still VACUOUS when its sidecar write failed" ;;
  *) bad "W4: got record '$w_rec3' / status '$w_st3' (want a ZERO record and VACUOUS)" ;;
esac
rm -rf "$w_side2"
# STRUCTURAL: no verdict path may finalize by re-reading what it just computed.
w_struct=()
grep -q '_census_finalize "$name" "$status" "$_cen_rec"' <<<"$(sed -n '/^run_scoped_tests() {/,/^}$/p' "$GATE")" \
  || w_struct+=("run_scoped_tests-finalizes-without-its-computed-record")
grep -q '_census_finalize shell-selftests "$status" "$_cen_rec"' <<<"$(sed -n '/^run_delta_shell_selftests() {/,/^}$/p' "$GATE")" \
  || w_struct+=("shell-selftests-finalizes-without-its-computed-record")
# …and the producers must PRINT what they wrote, or the callers have nothing to pass.
grep -qE '^[^#]*printf .%s. "\$line"' <<<"$(sed -n '/^_census_declare() {/,/^}$/p' "$GATE")" \
  || w_struct+=("_census_declare-does-not-return-its-record")
grep -qE '^[^#]*printf .%s. "\$line"' <<<"$(sed -n '/^_census_scoped_record() {/,/^}$/p' "$GATE")" \
  || w_struct+=("_census_scoped_record-does-not-return-its-record")
if [ "${#w_struct[@]}" -eq 0 ]; then
  ok "W5: both self:/runtime: producers return their computed record and both callers finalize from it — the sidecar is rendering-only on the verdict path"
else
  bad "W5: ${w_struct[*]}"
fi
# ---------------------------------------------------------------------------
# (X) THE RECORD-CONSTRUCTION TEXTS — the r4 sweep's scope, widened (roborev job 402).
#
# Round 4 swept "every label and counter on the aggregate `census:` line and in
# `_fm_annotate`'s census suffix" and I recorded the family as CLOSED. It was not: the texts
# that BUILD the record — inside `_census_measure_kind`, `_census_classify` and the
# `self:`/`runtime:` recorders — feed the record, which feeds the annotation, so they were in
# the family's blast radius and outside the sweep's stated scope. Job 402 is the fifth
# instance and it lives there: the `both` arm keyed its "no libtest tally" wording on
# `total == 0`, which is ALSO true of a tally that is PRESENT and reports zero.
#
# THIS IS CLAIM-VS-OBSERVATION, NOT A WORD SCAN. Each row supplies a log whose contents are
# known, then asserts the text CONTAINS what those contents justify and does NOT contain the
# specific claim they falsify. A blanket scan for words was tried in r4 and removed — the
# text `{no census: component ended FAIL, so there is no PASS to affirm}` legitimately
# contains "PASS" — and a guard that reds on correct prose is the guard agents learn to
# waive. The forbidden string here is derived from the fixture, one per row.
# ---------------------------------------------------------------------------
LOG_DIR="$tmp/recordtext"; mkdir -p "$LOG_DIR"
x_bad=()
x_n=0
x_LOUD='    Finished `test` profile in 0.01s\n'
x_ZEROTALLY='\nrunning 0 tests\n\ntest result: ok. 0 passed; 0 failed; 0 ignored\n'
x_REALTALLY='\nrunning 5 tests\n\ntest result: ok. 5 passed; 0 failed; 0 ignored\n'
x_EXEC='  Executable unittests src/lib.rs (target/debug/deps/x-1)\n'
# <component>|<kind>|<log body>|<must contain>|<must NOT contain, or '-'>
x_rows='
integration-tests|both|'"$x_ZEROTALLY"'|libtest result line(s), all reporting 0 passed|no libtest tally
integration-tests|both||no libtest tally|libtest result line(s), all reporting 0 passed
integration-tests|both|'"$x_LOUD$x_ZEROTALLY"'|libtest result line(s), all reporting 0 passed|no libtest tally
integration-tests|both|'"$x_LOUD"'|no libtest tally|reporting 0 passed
integration-tests|both|'"$x_LOUD$x_REALTALLY$x_EXEC"'|COUNT 5 tests passed and 1 test binaries|NOT MEASURED
tombstones-scan|libtest|'"$x_ZEROTALLY"'|every one of them reporting 0 passed|carries no libtest or nextest result line
tombstones-scan|libtest||carries no libtest or nextest result line|reporting 0 passed
feature-iso-parquet|compile||SUPPRESSED|nothing was built or verified fresh
feature-iso-parquet|compile|'"$x_LOUD"'|carries cargo status output but no '"'"'Executable'"'"' line|SUPPRESSED
python-bindings|indirect:pytest|61 skipped in 1.20s\n|reports none|no pytest tally found
python-bindings|indirect:pytest|maturin noise only\n|no pytest tally found|reports none
'
old_ifs=$IFS
while IFS='|' read -r x_comp x_kind x_body x_want x_forbid; do
  [ -n "$x_comp" ] || continue
  x_n=$((x_n + 1))
  rm -f "$(_census_sidecar "$x_comp")"
  printf '%b' "$x_body" > "$LOG_DIR/$x_comp.log"
  x_got=$(_census_measure_kind "$x_comp" "$x_kind")
  case "$x_got" in
    *"$x_want"*) ;;
    *) x_bad+=("[$x_comp/$x_kind] missing '$x_want' in: $x_got") ;;
  esac
  if [ "$x_forbid" != '-' ]; then
    case "$x_got" in
      *"$x_forbid"*) x_bad+=("[$x_comp/$x_kind] FALSE CLAIM '$x_forbid' — the log does not justify it: $x_got") ;;
    esac
  fi
done <<X_EOF
$x_rows
X_EOF
IFS=$old_ifs
if [ "$x_n" -ne 11 ]; then
  bad "X1: only $x_n of the 11 record-construction cells were exercised — the table is not iterating, so a green here would certify nothing"
elif [ "${#x_bad[@]}" -eq 0 ]; then
  ok "X1 (job 402 + the widened r4 sweep): all $x_n record-construction texts say only what their log justifies — in particular a PRESENT tally reporting zero is never described as an ABSENT tally, in either the libtest or the both arm, quiet or loud"
else
  bad "X1: ${x_bad[*]}"
fi
# The specific regression, called out by name so a failure points at job 402 directly.
rm -f "$(_census_sidecar integration-tests)"
printf '%b' "$x_ZEROTALLY" > "$LOG_DIR/integration-tests.log"
x_j402=$(_census_measure_kind integration-tests both)
case "$x_j402" in
  *'no libtest tally'*) bad "X2 (job 402): the both arm still reports 'no libtest tally' for a log whose tally is PRESENT and reports 0 passed: $x_j402" ;;
  *'1 libtest result line(s), all reporting 0 passed'*) ok "X2 (job 402): the both arm describes a present-but-zero tally from \$seen, not from \$total — the information was already in hand since job 389 and this branch was the one not consuming it" ;;
  *) bad "X2: got '$x_j402'" ;;
esac
echo
echo "component census guard: $PASS passed, $FAIL failed"
# A COUNT FLOOR beside the abort trap (the idiom of test_agent_gate_summary.sh and
# test_agent_gate_component_set.sh): a section that stops contributing verdicts — an
# extraction that broke, a subshell dying quietly — shrinks the subject set WITHOUT
# aborting, and "failed: 0" over a shrunken set is the vacuous pass this whole file is
# about. Set just below the full-host figure so it reds on a structural loss.
# MEASURED, and the reason is recorded here so it is not re-litigated (roborev job 400's
# side question). 117 cases on a fully-equipped host — and the count is ENVIRONMENT-INVARIANT:
# this suite has NO `skip` path, NO `command -v` guard and NO corpus/node/cargo dependency
# (verified by grep; the only external tool is python3 at the P2 derivation, which now FAILS
# CLOSED rather than dropping a case). So there is no lean-host figure to sit below.
#
# The earlier value of 88 was NOT an environment measurement — it was set by subtracting from
# the count when the `emitted` section was removed, which is exactly the move that hides a
# future shrink, and the honest answer to the question is that it gave away headroom for no
# reason. Raised to sit just under the measured count; the 7-case margin covers a lean host
# I have NOT measured (bash 3.2 on macOS, which this repo supports), not a known drop.
CENSUS_CASE_FLOOR=112
CENSUS_REACHED_END=1
if [ $((PASS + FAIL)) -lt "$CENSUS_CASE_FLOOR" ]; then
  printf 'FAIL - only %s verdicts were produced (floor %s): sections are being skipped or dying silently, and a "0 failed" over a shrunken subject set certifies nothing.\n' \
    "$((PASS + FAIL))" "$CENSUS_CASE_FLOOR" >&2
  exit 1
fi
[ "$FAIL" -eq 0 ] || exit 1
exit 0
