#!/usr/bin/env bash
#
# Regression tests for scripts/flow/premerge-assert.sh (issues #2668, #3465).
#
# `gh` is shimmed by a PATH-prepended mock that emits the two-token line the
# script's `--jq '.headRefOid + " " + .state'` expression would produce (or a
# failure), driven by env vars — no network, no GitHub.
#
# FAST + HERMETIC, AND THAT CLAIM IS AGAIN TRUE (#3650 review R5 F2). It was not:
# `run()` invoked the SHIPPED premerge-assert.sh, which invokes the SHIPPED
# base-staleness.sh, which read the AMBIENT checkout — so 13 success-path cases
# ran repeated repository-dependent scans whose cost scales with how far this
# lane's base is behind, and on a stock macOS (no GNU coreutils) they ran with the
# bound DISCARDED by the runner shim below. A suite header saying "hermetic" while
# every green case measured the surrounding repository is the same defect class as
# a contract stating an absolute the code violates.
#
# TWO CHANGES REMOVE IT, AND NO CASE IS LOST:
#   * `run()` now invokes a SCRATCH COPY of premerge-assert.sh beside an
#     IMMEDIATE advisory stub. Those cases are about refusals, provenance and the
#     scope disclaimer — the advisory only has to be invoked and reported, never
#     to scan anything.
#   * the ONE wiring case (the only case whose subject IS "the shipped script
#     really invokes the shipped advisory") runs the shipped artifacts against a
#     3-commit SYNTHETIC repository built here, with the certified sha being that
#     repository's own HEAD. It is bounded by construction, independent of the
#     ambient checkout, and — unlike the ambient version, which could only pin the
#     advisory's PREFIX — it pins MEASURED values (`behind 1 commits`,
#     `blast-radius 1 RECOGNISED`, `verdict STALE-RECOGNISED`), because a
#     synthetic fixture's staleness IS a property of this suite.
# Nothing here reads the surrounding repository any more; every case that pins
# advisory CONTENT substitutes a stub artifact in a scratch tree, and Case 41d
# asserts BOTH halves of that — behaviourally (an ordinary case carries the
# neutral stub's line and NOT the shipped advisory's `NON-EXHAUSTIVE` block) and
# STRUCTURALLY (exactly ONE invocation of the shipped artifact exists in this
# file), because the F2 defect was invisible: 161 assertions were green while 13
# cases scanned the surrounding repository and nothing said so.
#
# THE RUNTIME CLAIM, MEASURED RATHER THAN ASSERTED (this lane, git 2.43.0): one
# shipped-advisory scan costs ~0.03s on a freshly-rebased base (behind 0) and
# 0.43s on a base 110 commits behind. So the ambient dependency cost the suite
# 13 x that — ~0.4s rebased but ~5.6s on an 8-day-old base, growing with the
# lane's staleness, and UNBOUNDED on a stock macOS where the runner shim
# discarded the bound. That is why the suite's runtime used to be a function of
# the checkout it happened to sit in; it is now a constant.
#
# #3465 adds the gate-of-record half: the assert now takes a THIRD, REQUIRED
# argument naming the FULL gate's summary file, and refuses to merge without a
# `==== AGENT-GATE SUMMARY ====` block carrying RESULT: PASS, tree-integrity:
# PASS, and provenance matching the certified sha. Those cases refuse BEFORE the
# `gh` call, so most of them do not need the mock at all — but they run with it on
# PATH anyway, so a case that wrongly reaches `gh` cannot pass by accident.
#
# Run standalone:   bash scripts/tests/test_premerge_assert.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ASSERT="$SCRIPT_DIR/../flow/premerge-assert.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

# THE SCRATCH DIR IS VALIDATED BEFORE ANY PATH IS BUILT FROM IT (#3650 review
# B5). An unchecked `mktemp` leaves `$T` EMPTY, after which every `"$T/..."` in
# this suite resolves to an ABSOLUTE path at the ROOT — `/all-output.txt` and
# synthetic git repos directly under `/` — which a privileged run would really
# create. Aborting here, BEFORE the trap is installed, also keeps the trap from
# ever running `rm -rf ""`.
if ! T=$(mktemp -d "${TMPDIR:-/tmp}/premerge-assert-test.XXXXXX" 2>/dev/null) ||
  [ -z "$T" ] || [ ! -d "$T" ]; then
  printf 'FAIL - could not create a scratch directory under %s: refusing to run, because\n' \
    "${TMPDIR:-/tmp}" >&2
  printf 'FAIL - every path in this suite would resolve under / instead.\n' >&2
  exit 1
fi
trap 'rm -rf "$T"' EXIT

# --- gh mock -----------------------------------------------------------------
# A fake `gh` on PATH. Since the script parses via gh's built-in `--jq`, the
# mock stands in for gh-plus-jq and emits the already-extracted two-token line
# "<headRefOid> <state>". Two env vars drive it per-case:
#   MOCK_GH_OUT    the exact stdout to emit (the two-token --jq result)
#   MOCK_GH_FAIL   if "1", exit non-zero without output (simulates auth/network)
BIN="$T/bin"
mkdir -p "$BIN"
cat >"$BIN/gh" <<'MOCK'
#!/usr/bin/env bash
if [ "${MOCK_GH_FAIL:-0}" = "1" ]; then
  echo "gh: could not connect" >&2
  exit 1
fi
printf '%s\n' "${MOCK_GH_OUT:-}"
exit 0
MOCK
chmod +x "$BIN/gh"

# REAL_TO — the HOST's own supported timeout runner, resolved by the SAME
# algorithm the script under test uses (`timeout` THEN `gtimeout`, each PROBED
# for `--kill-after`, since BusyBox and older implementations reject the flag).
# Resolving `timeout` alone here would answer a different question than the
# script asks and would red on a supported gtimeout-only host (#3650 review R2).
REAL_TO=""
for _rt_cand in timeout gtimeout; do
  _rt_path=$(command -v "$_rt_cand" 2>/dev/null) || _rt_path=""
  [ -n "$_rt_path" ] || continue
  if "$_rt_path" --kill-after=1 1 true >/dev/null 2>&1; then
    REAL_TO="$_rt_path"
    break
  fi
done
unset _rt_cand _rt_path

# $BIN/timeout — A TEST-ONLY RUNNER SHIM, AND WHY THE WHOLE SUITE NEEDS ONE
# (#3650 review R3). The script under test requires a runner supporting
# `--kill-after` and SKIPS the advisory when there is none, which is correct
# behaviour — so every case asserting advisory CONTENT through `run`/`run_copy`
# was implicitly asserting that the HOST has GNU coreutils. On a stock macOS
# without it, 10 assertions demanded advisory output the shipped script is
# documented not to produce: the suite contradicted its own documented
# behaviour. The content cases are about the REPORT, not about the bound — the
# bound is owned, decisively and on both configurations, by Case 41b's four
# paths, which construct their own PATHs and are unaffected by this shim.
#
# It DELEGATES to the host's real runner when there is one, so a real bound
# still applies wherever one is available, and runs the command directly only
# when the host has none. It deliberately does NOT implement a bound in bash: a
# second timing implementation is the kind of mechanism #3650's scope discipline
# forbids.
#
# THE DISCARD BRANCH IS SAFE ONLY BECAUSE EVERY COMMAND IT CAN RUN IS BOUNDED BY
# CONSTRUCTION, AND THAT WAS NOT TRUE UNTIL #3650 review R5 F2. It used to be
# reached by 13 success-path cases running the SHIPPED advisory against the
# AMBIENT checkout — repeated repository-dependent scans whose runtime scales
# with how far this lane's base is behind, with no bound at all on a stock macOS.
# Now the only commands that reach this shim are (a) IMMEDIATE advisory stubs
# that print a line and exit, and (b) the ONE wiring case, whose advisory scans a
# 3-commit SYNTHETIC repository. Both are bounded by their own construction, not
# by the runner, on every host shape.
if [ -n "$REAL_TO" ]; then
  cat >"$BIN/timeout" <<SHIM
#!/usr/bin/env bash
exec "$REAL_TO" "\$@"
SHIM
else
  cat >"$BIN/timeout" <<'SHIM'
#!/usr/bin/env bash
# No real runner on this host: accept and discard the bound, then run directly.
case "$1" in --kill-after=*) shift ;; esac
shift   # the bound in seconds
exec "$@"
SHIM
fi
chmod +x "$BIN/timeout"

# --- THE NEUTRAL SCRATCH COPY (#3650 review R5 F2) --------------------------
# `run()` invokes THIS, not the shipped artifact, so no ordinary case reads the
# ambient checkout. It is a byte copy of premerge-assert.sh beside an IMMEDIATE
# advisory stub: the script resolves `base-staleness.sh` from its OWN directory
# with no env override (#3312's enforcer rule), so substituting the ARTIFACT is
# the only way to change which advisory runs — never a path variable, which would
# be one more seam a real invoker could set.
#
# The stub is DELIBERATELY not silent and not failing: `PREMERGE: ADVISORY`
# reporting is a property several of these cases inspect in passing, and a silent
# or erroring advisory has its own dedicated cases in Case 41. It exits 0 with a
# no-staleness verdict — the boring shape — so nothing here can pass because of an
# unusual advisory result.
#
# Copy failure ABORTS the suite rather than falling back to $ASSERT: a silent
# fallback would restore the ambient-scan behaviour this exists to remove, and it
# would do so invisibly.
NEUTRAL_ADV='#!/usr/bin/env bash
printf "BASE-STALENESS: neutral immediate stub — this case is not about the advisory\n"
printf "BASE-STALENESS: verdict NO-STALENESS-RECOGNISED\n"
exit 0'
NEUTRAL_DIR="$T/neutral-flow"
mkdir -p "$NEUTRAL_DIR"
if ! cp "$ASSERT" "$NEUTRAL_DIR/premerge-assert.sh"; then
  printf 'FAIL - could not build the neutral scratch copy of premerge-assert.sh: refusing
' >&2
  printf 'FAIL - to run, because every case would fall back to scanning the ambient repo.
' >&2
  exit 1
fi
printf '%s\n' "$NEUTRAL_ADV" >"$NEUTRAL_DIR/base-staleness.sh"
chmod +x "$NEUTRAL_DIR/base-staleness.sh"
# review-stage.sh travels with the copy (#3751). premerge-assert resolves it from
# its OWN directory with no env override (#3312's enforcer rule), so a scratch copy
# that omits it turns every `--c-verdict AUTO` case into a TOOL-FAILURE about a
# missing artifact instead of the state the case is about. The REAL script is
# copied, not a stub: it needs nothing but git and a worktree, so the end-to-end
# cases below exercise the shipped grammar rather than a second implementation of
# it.
if ! cp "$SCRIPT_DIR/../flow/review-stage.sh" "$NEUTRAL_DIR/review-stage.sh"; then
  printf 'FAIL - could not copy review-stage.sh into the neutral scratch copy\n' >&2
  exit 1
fi
NEUTRAL_ASSERT="$NEUTRAL_DIR/premerge-assert.sh"

# run <expected-exit> <description> <args...> — invokes the NEUTRAL COPY of the
# assert with the gh mock on PATH, captures combined output + exit code. Sets
# $OUT and $RC. The shipped artifact is exercised by the wiring case alone.
# c_inject_into <args...> — publish C_INJ, the caller's args plus
# `--c-verdict $C_PASS_FILE` unless the case already names the flag or sets
# NO_C_INJECT=1 (#3751).
#
# IT PUBLISHES AN ARRAY RATHER THAN PRINTING. A print-and-read-back version lost a
# TRAILING EMPTY argument — command substitution strips trailing newlines, so
# `--c-verdict ""` arrived as `--c-verdict` with no value and the empty-value case
# passed for the wrong reason (it asserted the wrong diagnostic). An argument
# vector cannot be round-tripped through a newline-delimited string, so it is not.
#
# WHY AN INJECTION RATHER THAN 90 EDITED CALL SITES. `--c-verdict` is REQUIRED, so
# every pre-#3751 call would exit 3 — turning a suite about gate-of-record
# provenance into a suite about one new flag, and hiding any real regression behind
# a uniform usage error. The injected value is an EXPLICIT PATH holding a PASS
# verdict line, so no routing is measured and those cases keep asserting exactly
# what they asserted before. The cases that are ABOUT the flag (omission,
# routing, AUTO) name it or opt out, so the injection can never mask them: Case 44
# below sets NO_C_INJECT=1 for the omission case, which is the one place a silently
# injected flag would produce a false pass.
C_INJ=()
c_inject_into() {
  local a inject=1
  if [ "${NO_C_INJECT:-0}" = 1 ]; then
    inject=0
  else
    for a in "$@"; do
      case "$a" in --c-verdict | --c-verdict=*) inject=0 ;; esac
    done
  fi
  C_INJ=(${1+"$@"})
  if [ "$inject" -eq 1 ]; then
    C_INJ=(${C_INJ[@]+"${C_INJ[@]}"} --c-verdict "$C_PASS_FILE")
  fi
}

run() {
  local want="$1" desc="$2"
  shift 2
  c_inject_into ${1+"$@"}
  set -- ${C_INJ[@]+"${C_INJ[@]}"}
  OUT=$(PATH="$BIN:$PATH" bash "$NEUTRAL_ASSERT" ${1+"$@"} 2>&1)
  RC=$?
  if [ "$RC" -ne "$want" ]; then
    bad "$desc (exit $RC, wanted $want)"
    printf '     output: %s\n' "$OUT"
    return 1
  fi
  return 0
}

CERTIFIED="da9a7cb2abc00000000000000000000000000000"   # full 40-char hex
STALE="ca8eb016def11111111111111111111111111111"       # full 40-char hex

# SR_REPORT <repo> <issue> <kind> — THE REPORT THE STAGE RECORD NAMES, derived the way
# review-stage.sh derives it (#3751 round 6, K2). The report name carries a per-open NONCE, so it
# is UNPREDICTABLE and must be READ from `report-nonce:`, never reconstructed from a shape; with no
# record, or a record with no nonce, it is the LEGACY bare `<kind>.md`.
SR_REPORT() {
  local d="$1/.review-stage/issue-$2" n
  n="$(LC_ALL=C sed -n 's/^report-nonce:[[:space:]]*//p' "$d/$3.stage" 2>/dev/null | LC_ALL=C head -1 || true)"
  if [ -n "$n" ]; then printf '%s/%s.%s.md\n' "$d" "$3" "$n"; else printf '%s/%s.md\n' "$d" "$3"; fi
}

# THE INJECTED C VERDICT (#3751) — a captured `review-stage.sh verdict` PASS line,
# in the shape review-stage.sh emits it. Every case that is not ABOUT the C flag
# gets this, so those cases assert what they always asserted. It is a FILE (an
# explicit path), never AUTO, so no case reads the ambient repository's openspec
# routing — the same hermeticity rule the neutral advisory copy exists for.
C_PASS_FILE="$T/c-verdict-pass.txt"
printf 'REVIEW-STAGE: c RESULT: PASS elapsed=42 deadline=1800 agent=spec-auditor report=%s\n' \
  "$T/injected-c-report.md" >"$C_PASS_FILE"

# The two abbreviations the gate ACTUALLY writes for $CERTIFIED: `commit:` is
# `printf '%.7s'` and `tree-start:` is `_tree_short` = `printf '%.12s'`, both of
# the same verified capture (scripts/agent-gate.sh). Two independent widths.
C7="da9a7cb"
C12="da9a7cb2abc0"

# --- summary fixture builders -------------------------------------------------
# emit_summary_block <start-marker> <end-marker> <mode-line|-> <commit-val|-> \
#                    <tree-start-val|-> <tree-integrity-val|-> <result-val|->
# A "-" omits that line entirely. Line SHAPES are copied from a real full-gate
# summary (/tmp/cqlite-gates/**/full-gate.txt), trailing fields included, so the
# parser is exercised against the grammar the gate emits and not a simplification.
#
# The 8th parameter is the `dirty:` VALUE on the `commit:` line (#3648), which is
# the field the assert now enforces. Defaults to `no` (via `${8-no}`, so an
# EMPTY string is a value and not "unset"). Rendering is delegated to
# `dirty_field`/`dirty_tree_start`, so a case can build the three shapes the
# enforcement must refuse — absent field, present-but-empty value, unrecognised
# value — as well as the two it must decide on.
#
# `tree-start:` MIRRORS the value only when it is `yes`/`no`; for the sentinel
# shapes it stays a clean `dirty: no`. That disagreement is deliberate: it pins
# that the check reads the value from the `commit:` line (which is where the
# parser takes it from) and does NOT fall back to the clean-looking one below it.

# dirty_field <value> — the ` dirty: <value>` suffix the gate writes on `commit:`.
# "-" omits the field entirely; an EMPTY value renders the bare key.
dirty_field() {
  case "$1" in
    -)  : ;;
    '') printf ' dirty:' ;;
    *)  printf ' dirty: %s' "$1" ;;
  esac
}

# dirty_tree_start <value> — see the note above: mirror yes/no, else `no`.
dirty_tree_start() {
  case "$1" in
    yes|no) printf '%s' "$1" ;;
    *)      printf 'no' ;;
  esac
}

emit_summary_block() {
  local start="$1" end="$2" mode="$3" commit="$4" tstart="$5" ti="$6" result="$7" \
        dirty="${8-no}" tsdirty="${9-__mirror__}"
  # 9th param: tree-start:/tree-end:'s OWN dirty value, allowed to DIVERGE from
  # commit:'s. Defaults to mirroring so every pre-existing caller is unchanged.
  # The divergence is REAL, not synthetic: commit: renders the END capture, so a
  # lockfile-settled run legally reads tree-start dirty:yes + commit dirty:no
  # (#3648 roborev round 4).
  [ "$tsdirty" = "__mirror__" ] && tsdirty="$dirty"
  printf '%s\n' "$start"
  printf 'run-id: /tmp/agent-gate.9cIQgX\n'
  [ "$mode" = "-" ] || printf '%s\n' "$mode"
  [ "$commit" = "-" ] || printf 'commit: %s branch: issue-3465-require-gate-of-record%s\n' \
    "$commit" "$(dirty_field "$dirty")"
  printf 'datasets: 144 Data.db files under /data/datasets\n'
  printf 'accelerators: sccache=on nextest=on lanes=on mold=absent perf=paranoid-4\n'
  [ "$tstart" = "-" ] || printf 'tree-start: %s dirty: %s digest: 671a6275687c\n' \
    "$tstart" "$(dirty_tree_start "$tsdirty")"
  printf 'tree-end: %s dirty: %s digest: 671a6275687c\n' "$tstart" "$(dirty_tree_start "$dirty")"
  [ "$ti" = "-" ] || printf 'tree-integrity: %s\n' "$ti"
  printf 'file-size:         PASS (0s)\n'
  printf 'smoke:             PASS (193s)\n'
  printf 'logs: /tmp/agent-gate.9cIQgX\n'
  [ "$result" = "-" ] || printf 'RESULT: %s\n' "$result"
  printf '%s\n' "$end"
}

FULL_S="==== AGENT-GATE SUMMARY ===="
FULL_E="==== END AGENT-GATE SUMMARY ===="
LITE_S="==== AGENT-GATE LITE SUMMARY ===="
LITE_E="==== END AGENT-GATE LITE SUMMARY ===="
DELTA_S="==== AGENT-GATE DELTA SUMMARY ===="
DELTA_E="==== END AGENT-GATE DELTA SUMMARY ===="

# full_block [commit] [tree-start] [tree-integrity] [result] [dirty] -> STDOUT.
# Separate from full_summary because composing two blocks into one file cannot go
# through a `>"$f"` helper: `full_summary /dev/stdout` inside a `{ } > file` group
# TRUNCATES the file, which silently produced a ONE-block fixture for a
# two-block case (found while writing these tests — the case passed vacuously).
full_block() {
  emit_summary_block "$FULL_S" "$FULL_E" "-" \
    "${1:-$C7}" "${2:-$C12}" "${3:-PASS}" "${4:-PASS}" "${5-no}" "${6-__mirror__}"
}

# full_summary <file> [commit] [tree-start] [tree-integrity] [result] [dirty]
full_summary() {
  local f="$1"
  shift
  full_block "$@" >"$f"
}

GOOD="$T/full-pass.txt"
full_summary "$GOOD"

# refused <description> <summary-file> [needle] — every gate-of-record refusal is
# exit 2 with the NO-GATE-OF-RECORD verdict; an optional needle pins the cause so
# a case cannot pass by refusing for the WRONG reason.
refused() {
  local desc="$1" f="$2" needle="${3:-}"
  if run 2 "$desc" 2421 "$CERTIFIED" "$f"; then
    case "$OUT" in
      *"PREMERGE: NO-GATE-OF-RECORD"*) ;;
      *) bad "$desc: missing NO-GATE-OF-RECORD verdict (got: $OUT)"; return 1 ;;
    esac
    if [ -n "$needle" ] && [ "${OUT#*"$needle"}" = "$OUT" ]; then
      bad "$desc: refusal does not name the cause '$needle' (got: $OUT)"
      return 1
    fi
    ok "$desc"
  fi
}

export MOCK_GH_FAIL=0
export MOCK_GH_OUT="$CERTIFIED OPEN"

# =============================================================================
# Pre-#3465 behaviour, now carrying the third argument
# =============================================================================

# --- Case 1: match -> exit 0 --------------------------------------------------
if run 0 "match: OPEN + head==certified + gate of record -> exit 0" 2421 "$CERTIFIED" "$GOOD"; then
  case "$OUT" in
    *"PREMERGE: OK $CERTIFIED"*) ok "match: prints PREMERGE: OK <sha>" ;;
    *) bad "match: missing 'PREMERGE: OK <sha>' (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: GATE-OF-RECORD commit: $C7 tree-start: $C12 tree-integrity: PASS dirty: no"*)
      ok "match: prints the GATE-OF-RECORD evidence line (commit/tree-start/integrity/dirty)" ;;
    *) bad "match: missing GATE-OF-RECORD evidence line (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"summary: $GOOD"*) ok "match: evidence line names the summary file" ;;
    *) bad "match: evidence line must name the summary file (got: $OUT)" ;;
  esac
fi

# --- Case 2: mismatch -> exit 2, message names BOTH SHAs -----------------------
export MOCK_GH_OUT="$STALE OPEN"
if run 2 "mismatch: head moved -> exit 2" 2421 "$CERTIFIED" "$GOOD"; then
  if [ "${OUT#*"$CERTIFIED"}" != "$OUT" ] && [ "${OUT#*"$STALE"}" != "$OUT" ]; then
    ok "mismatch: message contains BOTH SHAs"
  else
    bad "mismatch: message must contain both certified + actual SHA (got: $OUT)"
  fi
  case "$OUT" in
    *"re-certify before merge"*) ok "mismatch: message says re-certify before merge" ;;
    *) bad "mismatch: missing re-certify guidance (got: $OUT)" ;;
  esac
fi

# --- Case 3: merged/closed PR -> exit 2 ---------------------------------------
export MOCK_GH_OUT="$CERTIFIED MERGED"
if run 2 "merged PR -> exit 2" 2421 "$CERTIFIED" "$GOOD"; then
  case "$OUT" in
    *"NOT-OPEN"*|*"closed or merged"*) ok "merged: distinct not-open refusal message" ;;
    *) bad "merged: missing not-open message (got: $OUT)" ;;
  esac
fi

# --- Case 4: gh/network failure -> exit 3 (fail closed) -----------------------
export MOCK_GH_FAIL=1
export MOCK_GH_OUT=""
if run 3 "gh failure -> exit 3 (fail closed)" 2421 "$CERTIFIED" "$GOOD"; then
  case "$OUT" in
    *"GH-FAILURE"*) ok "gh-failure: distinct fail-closed message" ;;
    *) bad "gh-failure: missing GH-FAILURE message (got: $OUT)" ;;
  esac
fi
export MOCK_GH_FAIL=0
export MOCK_GH_OUT="$CERTIFIED OPEN"

# --- Case 5: usage guard -> exit 3 --------------------------------------------
if run 3 "missing args -> exit 3" 2421; then
  ok "usage: too few args fails closed (exit 3)"
fi

# --- Case 6: malformed --jq output (missing state token) -> exit 3 ------------
export MOCK_GH_OUT="$CERTIFIED"   # only one token, no state
if run 3 "malformed --jq output -> exit 3" 2421 "$CERTIFIED" "$GOOD"; then
  ok "malformed: fails closed (exit 3)"
fi
export MOCK_GH_OUT="$CERTIFIED OPEN"

# --- Case 7: certified SHA passed UPPERCASE -> still matches (normalization) --
CERT_UPPER=$(printf '%s' "$CERTIFIED" | tr '[:lower:]' '[:upper:]')
if run 0 "uppercase certified SHA -> normalized match -> exit 0" 2421 "$CERT_UPPER" "$GOOD"; then
  ok "normalization: uppercase certified SHA still matches lowercase head + summary"
fi

# --- Case 8: certified SHA wrong length -> exit 3 -----------------------------
if run 3 "short certified SHA -> exit 3" 2421 "da9a7cb2" "$GOOD"; then
  ok "validation: abbreviated (non-40-char) SHA fails closed (exit 3)"
fi

# =============================================================================
# #3465 — the gate of record is REQUIRED
# =============================================================================

# --- Case 9: the third argument is REQUIRED, not optional ---------------------
# An OPTIONAL argument would leave the honour system exactly where it is, so the
# pre-#3465 two-argument invocation must FAIL LOUDLY (usage, exit 3).
if run 3 "pre-#3465 two-arg invocation -> exit 3 (usage)" 2421 "$CERTIFIED"; then
  case "$OUT" in
    *"gate-of-record-summary"*) ok "usage: two-arg call fails closed and names <gate-of-record-summary>" ;;
    *) bad "usage: two-arg refusal must name the missing argument (got: $OUT)" ;;
  esac
fi
if run 3 "empty third argument -> exit 3 (usage)" 2421 "$CERTIFIED" ""; then
  ok "usage: an EMPTY summary path is a usage failure, not an absent-file refusal"
fi

# --- Case 10: summary file absent / empty -------------------------------------
refused "summary file absent -> refuse" "$T/does-not-exist.txt" "does not exist"
: >"$T/empty.txt"
refused "summary file EMPTY -> refuse" "$T/empty.txt" "EMPTY"

# --- Case 11: LITE-only summary -> refuse (this is literally the #3465 case) --
# PR #3408 merged on 22 lite PASSes and no full gate. --lite emits DISTINCT
# markers plus a MODE line, so a lite summary carries ZERO full-gate blocks.
emit_summary_block "$LITE_S" "$LITE_E" \
  "MODE: lite (FAST ITERATION — NOT the gate of record; full agent-gate.sh must PASS once before merge)" \
  "$C7" "$C12" PASS PASS >"$T/lite-only.txt"
refused "LITE-only summary (the #3408 case) -> refuse" "$T/lite-only.txt" "ZERO full-gate blocks"
if [ "${OUT#*"found 1 lite"}" != "$OUT" ]; then
  ok "lite-only: refusal NAMES the lite block it found (actionable diagnostic)"
else
  bad "lite-only: refusal should name the lite block it found (got: $OUT)"
fi

# --- Case 12: DELTA-only summary -> refuse ------------------------------------
emit_summary_block "$DELTA_S" "$DELTA_E" \
  "MODE: delta (TEST/DOCS-ONLY RE-CERTIFICATION — NOT the gate of record; gate of record = the full agent-gate.sh PASS at anchor abc1234)" \
  "$C7" "$C12" PASS PASS >"$T/delta-only.txt"
refused "DELTA-only summary -> refuse" "$T/delta-only.txt" "ZERO full-gate blocks"
if [ "${OUT#*"0 lite, 1 delta"}" != "$OUT" ]; then
  ok "delta-only: refusal NAMES the delta block it found"
else
  bad "delta-only: refusal should name the delta block it found (got: $OUT)"
fi

# --- Case 13: the #3041 launch SENTINEL alone -> refuse -----------------------
# The gate writes this at LAUNCH (before the #1825 slot is even granted): a
# FULL-header block with run-id + tree-start + `RESULT: INCOMPLETE (gate did not
# finish)` and NO tree-integrity/commit. It is a liveness placeholder, NOT a
# verdict — accepting it would certify a still-queued or dead gate.
{
  printf '%s\n' "$FULL_S"
  printf 'run-id: /tmp/agent-gate.9cIQgX\n'
  printf 'tree-start: %s dirty: no digest: 671a6275687c\n' "$C12"
  printf 'RESULT: INCOMPLETE (gate did not finish)\n'
  printf '%s\n' "$FULL_E"
} >"$T/sentinel.txt"
refused "launch sentinel (RESULT: INCOMPLETE) alone -> refuse" "$T/sentinel.txt" "INCOMPLETE"

# --- Case 14: RESULT: FAIL -> refuse -----------------------------------------
full_summary "$T/result-fail.txt" "$C7" "$C12" PASS FAIL
refused "RESULT: FAIL -> refuse" "$T/result-fail.txt" "RESULT verdict token in the full-gate block is 'FAIL'"

# --- Case 15: near-miss RESULT verdict tokens -> refuse (token-exact) --------
# A `PASS*` prefix test would check a SPELLING rather than a STATE.
for near in PASSthisNeverRan PASS-MEASUREMENT-DID-NOT-HAPPEN PASSED; do
  full_summary "$T/near-$near.txt" "$C7" "$C12" PASS "$near"
  refused "near-miss RESULT token '$near' -> refuse (token-exact, not a prefix)" \
    "$T/near-$near.txt" "RESULT verdict token in the full-gate block is '$near'"
done

# --- Case 16: tree-integrity FAIL / PENDING / SKIP -> refuse -----------------
# A run whose worktree mutated mid-run is not a certification (#2926); PENDING
# means it never reached the terminal emit; SKIP means the check never ran.
full_summary "$T/ti-fail.txt" "$C7" "$C12" \
  "FAIL (tree-mutated-midrun; head da9a7cb->ca8eb01; changed: src/lib.rs)" PASS
refused "tree-integrity: FAIL -> refuse" "$T/ti-fail.txt" "tree-integrity verdict token in the full-gate block is 'FAIL'"
full_summary "$T/ti-pending.txt" "$C7" "$C12" PENDING PASS
refused "tree-integrity: PENDING -> refuse" "$T/ti-pending.txt" "token in the full-gate block is 'PENDING'"
full_summary "$T/ti-skip.txt" "$C7" "$C12" "SKIP (no capture)" PASS
refused "tree-integrity: SKIP -> refuse" "$T/ti-skip.txt" "token in the full-gate block is 'SKIP'"

# --- Case 17: missing RESULT / tree-integrity lines -> refuse ----------------
full_summary "$T/no-result.txt" "$C7" "$C12" PASS "-"
refused "no RESULT: line -> refuse" "$T/no-result.txt" "no 'RESULT:' line"
full_summary "$T/no-ti.txt" "$C7" "$C12" "-" PASS
refused "no tree-integrity: line -> refuse" "$T/no-ti.txt" "no 'tree-integrity:' line"

# --- Case 18: commit: / tree-start: mismatch -> refuse (the #3616 class) -----
# This is the OTHER motivating escape, distinct from #3408's "no gate at all":
# #3616 = "a real gate, someone else's". A closer located its run dir by RECENCY
# (`ls -t /tmp/agent-gate.*`), read a PEER LANE's dir, saw 33/37 components PASS
# and was about to merge #3616 on PR #3580's verdict — the count, the dir and the
# timestamps were all real, and only the `run-id:` line (read by a human) exposed
# it. premerge-assert cannot verify `run-id:` (it did not launch the gate), so the
# sha binding is what makes a cross-lane verdict a mechanical refusal.
# Each abbreviation is compared at ITS OWN width (7 and 12), against a DIFFERENT
# sha here.
#
# First the composite #3616 shape: a fully well-formed FULL block, RESULT: PASS,
# tree-integrity: PASS, everything a genuine gate of record has — except that its
# provenance names a peer PR's head throughout.
full_summary "$T/peer-lane.txt" "ca8eb01" "ca8eb016def1" PASS PASS
refused "#3616: a valid full-gate PASS naming a PEER LANE's sha -> refuse" \
  "$T/peer-lane.txt" "does not match the certified sha"
# ...and the single-field variants, which is where the two independent widths pay.
full_summary "$T/commit-mismatch.txt" "ca8eb01" "$C12" PASS PASS
refused "commit: mismatch (7 hex of a different sha) -> refuse" \
  "$T/commit-mismatch.txt" "'commit:' value 'ca8eb01' in the full-gate block does not match the certified sha at 7 chars"
full_summary "$T/tstart-mismatch.txt" "$C7" "ca8eb016def1" PASS PASS
refused "tree-start: mismatch (12 hex of a different sha) -> refuse" \
  "$T/tstart-mismatch.txt" "'tree-start:' value 'ca8eb016def1' in the full-gate block does not match the certified sha at 12 chars"
# A tree-start that agrees on the FIRST 7 chars but diverges at 8..12 is exactly
# what the second, wider compare buys over the 7-hex one.
full_summary "$T/tstart-wide.txt" "$C7" "da9a7cbffff0" PASS PASS
refused "tree-start: diverges only beyond 7 chars -> refuse (the wider compare pays)" \
  "$T/tstart-wide.txt" "'tree-start:' value 'da9a7cbffff0' in the full-gate block does not match"
# ...and one that is NARROWER than the 12 the gate emits still has to match at
# its OWN width, which it does — proving the compare uses the VALUE's length, not
# a fixed assumed width. 8 hex, not 4: 4 would be BELOW the floor (next case), and
# pinning the loosest accepted width would make that leniency a requirement.
full_summary "$T/tstart-short.txt" "$C7" "da9a7cb2" PASS PASS
if run 0 "8-hex tree-start that prefixes the certified sha -> accepted at ITS width" \
  2421 "$CERTIFIED" "$T/tstart-short.txt"; then
  ok "width: the compare uses the VALUE's own length (8 hex accepted, not padded/globbed)"
fi
# The FLOOR is 7 — the narrowest abbreviation the gate ever emits (commit: is
# printf '%.7s'). A 4-hex value that DOES prefix the certified sha is refused
# anyway: accepted at its own width it would be a 1-in-65536 accidental
# cross-lane match, i.e. the #3616 class this compare exists to refuse.
full_summary "$T/tstart-4hex.txt" "$C7" "da9a" PASS PASS
refused "4-hex tree-start (below the 7 floor) -> refuse even though it prefixes" \
  "$T/tstart-4hex.txt" "is 4 hex chars — outside the 7..40 range"

# --- Case 19: non-hex commit:/tree-start: -> refuse, never skipped -----------
# The gate writes these placeholders when its capture failed or there was no git
# worktree. "Nothing was recorded" must REFUSE, not be waved through.
full_summary "$T/commit-unverified.txt" "unverified" "$C12" PASS PASS
refused "commit: unverified (capture failed) -> refuse" \
  "$T/commit-unverified.txt" "is not lowercase hex"
full_summary "$T/tstart-selftest.txt" "$C7" "selftest" PASS PASS
refused "tree-start: selftest -> refuse" "$T/tstart-selftest.txt" "is not lowercase hex"
# `tree-start: (not captured)` — the awk token is the literal "(not".
full_summary "$T/tstart-notcap.txt" "$C7" "(not" PASS PASS
refused "tree-start: (not captured) -> refuse" "$T/tstart-notcap.txt" "is not lowercase hex"
# An UPPERCASE hex abbreviation is not something git emits; refuse rather than
# silently normalize a value whose provenance we cannot explain.
full_summary "$T/commit-upper.txt" "DA9A7CB" "$C12" PASS PASS
refused "commit: UPPERCASE hex -> refuse" "$T/commit-upper.txt" "is not lowercase hex"
# Missing lines entirely.
full_summary "$T/no-commit.txt" "-" "$C12" PASS PASS
refused "no commit: line -> refuse" "$T/no-commit.txt" "no 'commit:' line"
full_summary "$T/no-tstart.txt" "$C7" "-" PASS PASS
refused "no tree-start: line -> refuse" "$T/no-tstart.txt" "no 'tree-start:' line"

# --- Case 20: TWO full-gate blocks -> refuse (ambiguous, never "take the last") --
{ full_block "$C7" "$C12" PASS FAIL
  full_block "$C7" "$C12" PASS PASS
} >"$T/two-blocks.txt"
if [ "$(grep -c -x -F "$FULL_S" "$T/two-blocks.txt")" -eq 2 ]; then
  ok "two-blocks fixture: the file really does hold TWO full-gate start markers"
else
  bad "two-blocks fixture: expected 2 start markers in the fixture"
fi
refused "two full-gate blocks -> refuse as AMBIGUOUS" "$T/two-blocks.txt" "2 full-gate blocks"
if [ "${OUT#*"take the last block"}" != "$OUT" ]; then
  ok "two-blocks: refusal explains why 'take the last one' is unsafe"
else
  bad "two-blocks: expected the rationale for refusing rather than picking (got: $OUT)"
fi

# --- Case 21: an UNTERMINATED full block -> refuse ---------------------------
# A killed/truncated gate leaves a start marker with no exact end marker.
full_summary "$T/unterminated.txt"
grep -v -x -F "$FULL_E" "$T/unterminated.txt" >"$T/unterminated2.txt"
refused "unterminated full block (truncated file) -> refuse" \
  "$T/unterminated2.txt" "UNTERMINATED"

# --- Case 22: a MODE: line inside a FULL-header block -> refuse --------------
# Belt for the header separation: the FULL gate emits NO MODE: line.
emit_summary_block "$FULL_S" "$FULL_E" \
  "MODE: lite (FAST ITERATION — NOT the gate of record)" \
  "$C7" "$C12" PASS PASS >"$T/full-with-mode.txt"
refused "MODE: line inside a FULL-header block -> refuse" \
  "$T/full-with-mode.txt" "carries a MODE: line"

# --- Case 23: prose QUOTING the markers is not a block ----------------------
# Whole-line-exact anchoring defends against PROSE copies of a marker —
# indented, `>`-quoted, fenced or mid-sentence — which CLAUDE.md, issue bodies,
# PR comments and the doctrine files this change edits all contain, and against
# a TRUNCATED pattern such as `AGENT-GATE SUMMARY ====`, which matches all four
# markers (full/lite start and end). Note the end marker does NOT contain the
# start marker as a substring: `END ` sits between `====` and `AGENT-GATE`.
{
  printf 'The gate emits a "%s" block whose RESULT: PASS is the verdict.\n' "$FULL_S"
  printf 'Its terminator is %s and it is not a block opener.\n' "$FULL_E"
  printf '  %s\n' "$FULL_S"      # indented -> not whole-line-exact
  printf 'RESULT: PASS\n'
  printf 'tree-integrity: PASS\n'
} >"$T/prose-only.txt"
refused "prose quoting the markers -> counted as ZERO blocks" \
  "$T/prose-only.txt" "ZERO full-gate blocks"

# A real block AFTER prose that quotes the markers still parses as exactly one.
{ cat "$T/prose-only.txt"; full_block; } >"$T/prose-then-block.txt"
if [ "$(grep -c -x -F "$FULL_S" "$T/prose-then-block.txt")" -eq 1 ]; then
  ok "prose+block fixture: exactly one whole-line-exact start marker, plus quoted ones"
else
  bad "prose+block fixture: expected exactly 1 whole-line-exact start marker"
fi
if run 0 "prose quoting the markers + ONE real block -> exit 0" \
  2421 "$CERTIFIED" "$T/prose-then-block.txt"; then
  ok "anchoring: quoted/indented markers are inert; the one real block certifies"
fi

# --- Case 24: ANSI-coloured summary still parses (#3400) --------------------
# BELT, stated precisely: the summary FILE's block lines are `echo`s of computed
# strings (scripts/agent-gate.sh emit_summary), so they are NOT coloured —
# CARGO_TERM_COLOR colours cargo output inside gate.log, not the block. What the
# strip covers is a block RECOVERED FROM A COLOURED CAPTURE rather than from the
# summary file, and colour does survive redirection (#3400). Non-vacuous either
# way: without the strip this fixture refuses a perfectly good gate of record.
ESC=$(printf '\033')
sed -e "s/RESULT: PASS/${ESC}[32mRESULT${ESC}[0m: ${ESC}[1;32mPASS${ESC}[0m/" \
    -e "s/tree-integrity: PASS/tree-integrity: ${ESC}[32mPASS${ESC}[0m/" \
    -e "s/^${FULL_S}\$/${ESC}[1m${FULL_S}${ESC}[0m/" \
    "$GOOD" >"$T/coloured.txt"
if grep -q "$ESC" "$T/coloured.txt"; then
  ok "colour fixture: the fixture really does contain ANSI escapes"
else
  bad "colour fixture: expected ANSI escapes in the fixture"
fi
if run 0 "ANSI-coloured full summary -> still parsed -> exit 0" \
  2421 "$CERTIFIED" "$T/coloured.txt"; then
  ok "ansi: escapes are stripped before marker/verdict matching (#3400)"
fi

# --- Case 25: dirty: is REPORTED **AND ENFORCED** (#3648) --------------------
# A gate that ran with `dirty: yes` certified sha PLUS uncommitted NON-IGNORED
# content (tracked edits and/or non-ignored untracked files)
# (the gate's capture is --exclude-standard, so never a gitignored log), and
# `commit:`/`tree-start:` name the same sha either way — so this is the one
# property of that hazard the sha binding provably cannot see. It was REPORTED
# and not enforced until #3648; these cases pin the enforcement.
#
# The POSITIVE CONTROL comes first on purpose: without it a suite that refused
# everything would look green while proving nothing about the accepted shape.

# 25(a) POSITIVE CONTROL — `dirty: no` still passes, and is still reported.
full_summary "$T/dirty-no.txt" "$C7" "$C12" PASS PASS no
if run 0 "dirty: no -> exit 0 (the clean tree is still accepted)" \
  2421 "$CERTIFIED" "$T/dirty-no.txt"; then
  case "$OUT" in
    *"tree-integrity: PASS dirty: no"*)
      ok "dirty: a clean gate of record is still REPORTED in the evidence line" ;;
    *) bad "dirty: the evidence line must still report dirty: no (got: $OUT)" ;;
  esac
fi

# 25(b) THE AC'S NAMED TEST — a full PASS identical BUT FOR `dirty: yes`.
# Built from the same builder as 25(a) with only that one field changed, so the
# case cannot pass by differing somewhere else.
full_summary "$T/dirty-yes.txt" "$C7" "$C12" PASS PASS yes
if diff <(full_block "$C7" "$C12" PASS PASS no | grep -v 'dirty:') \
        <(full_block "$C7" "$C12" PASS PASS yes | grep -v 'dirty:') >/dev/null; then
  ok "dirty fixture: the yes/no fixtures differ ONLY in their dirty: fields"
else
  bad "dirty fixture: the yes/no fixtures differ somewhere other than dirty:"
fi
refused "dirty: yes -> REFUSE (the run certified sha + uncommitted edits, #3648)" \
  "$T/dirty-yes.txt" "records 'dirty: yes'"
case "$OUT" in
  *"commit the edits (or discard them), then re-run the FULL gate"*)
    ok "dirty: the refusal names the REMEDY (commit or discard, then re-gate)" ;;
  *) bad "dirty: the refusal must name the remedy (got: $OUT)" ;;
esac
case "$OUT" in
  *"NO opt-out"*)
    ok "dirty: the refusal states there is no opt-out (a dirty tree is re-gateable)" ;;
  *) bad "dirty: the refusal must state that there is no opt-out (got: $OUT)" ;;
esac
# 25(b2) THE DIAGNOSTIC MUST DESCRIBE THE GATE'S ACTUAL DIRTY IDENTITY (#3648
# roborev round 1, finding 2). It previously said "uncommitted TRACKED edits",
# which is FALSE: agent-gate.sh pairs the tracked-side diff with
# `git ls-files --others --exclude-standard`, so a non-ignored UNTRACKED file makes
# the tree dirty too. An operator told "tracked" looks for a modified file, finds
# none, and concludes the gate is broken.
case "$OUT" in
  *"NON-IGNORED content"*) ok "dirty: the refusal describes NON-IGNORED content, not tracked-only" ;;
  *) bad "dirty: the refusal must not narrow the dirty identity to tracked files (got: $OUT)" ;;
esac
case "$OUT" in
  *"UNTRACKED files"*) ok "dirty: the refusal names UNTRACKED files as a dirty cause" ;;
  *) bad "dirty: the refusal must name untracked files as a dirty cause (got: $OUT)" ;;
esac

# 25(c) ABSENT `dirty:` field -> REFUSE. Never skipped, never read as clean:
# the same discipline as a non-hex commit:/tree-start: placeholder.
full_summary "$T/dirty-absent.txt" "$C7" "$C12" PASS PASS -
if grep -q '^commit: .* dirty:' "$T/dirty-absent.txt"; then
  bad "dirty fixture: the absent-field fixture still carries a dirty: on commit:"
else
  ok "dirty fixture: the absent-field fixture really omits dirty: from commit:"
fi
refused "commit: line with NO dirty: field -> refuse (nothing was measured)" \
  "$T/dirty-absent.txt" "records NO 'dirty:' value"

# 25(c2) DUPLICATED `dirty:` FIELD -> REFUSE, and refuse on the AMBIGUITY rather
# than on either value (#3648 roborev round 2, finding 1 -- Medium). The parser
# used to assign on EVERY `dirty:` token, so the LAST one won: a commit: line
# reading `dirty: yes dirty: no` reduced to `no` and CERTIFIED a dirty run. That
# is precisely the "last one wins" reading assert_single_key refuses for whole
# keys, one field down, and it is a false PASS in a merge gate. The refusal must
# fire BEFORE the `= no` compare, so a trailing clean value cannot short-circuit.
full_summary "$T/dirty-dup.txt"
sed -i 's/^\(commit: .* dirty: no\)$/\1 dirty: yes/' "$T/dirty-dup.txt"
if grep -qE '^commit: .* dirty: no dirty: yes$' "$T/dirty-dup.txt"; then
  ok "dirty fixture: the duplicate-field fixture really carries TWO dirty: tokens"
else
  bad "dirty fixture: expected two dirty: tokens on the commit: line"
fi
refused "commit: line with TWO dirty: fields -> refuse (AMBIGUOUS, not last-wins)" \
  "$T/dirty-dup.txt" "AMBIGUOUS"
# And the mirror image: a clean value LAST must not rescue a dirty value first.
full_summary "$T/dirty-dup-clean-last.txt" "$C7" "$C12" PASS PASS yes
sed -i 's/^\(commit: .* dirty: yes\)$/\1 dirty: no/' "$T/dirty-dup-clean-last.txt"
if grep -qE '^commit: .* dirty: yes dirty: no$' "$T/dirty-dup-clean-last.txt"; then
  ok "dirty fixture: the clean-last fixture is dirty FIRST, clean LAST"
else
  bad "dirty fixture: expected 'dirty: yes dirty: no' on the commit: line"
fi
refused "commit: dirty: yes then dirty: no -> refuse (a trailing clean value cannot rescue it)" \
  "$T/dirty-dup-clean-last.txt" "AMBIGUOUS"

# 25(c3) THE LOCKFILE-SETTLED HOLE (#3648 roborev round 4, Medium). `commit:`
# renders the END capture (agent-gate.sh:8810), so reading cleanliness from it
# ALONE is not enough: a gate that STARTED against a dirty Cargo.lock and
# finished clean emits `tree-start: ... dirty: yes` + `commit: ... dirty: no`,
# and `tree-integrity:` is a NON-FATAL `PASS (lockfile-settled: ...)`
# (agent-gate.sh:8754) -- a legal, real PASS. Checking commit: only would certify
# a run that executed against uncommitted content, which is this issue's own
# defect one capture down. BOTH captures must read clean.
full_summary "$T/dirty-start-only.txt" "$C7" "$C12" \
  "PASS (lockfile-settled: Cargo.lock)" PASS no yes
if grep -qE '^tree-start: .* dirty: yes ' "$T/dirty-start-only.txt" \
   && grep -qE '^commit: .* dirty: no$' "$T/dirty-start-only.txt"; then
  ok "dirty fixture: the lockfile-settled fixture is dirty at START, clean at commit:"
else
  bad "dirty fixture: expected tree-start dirty: yes with commit: dirty: no"
fi
if grep -q 'tree-integrity: PASS (lockfile-settled' "$T/dirty-start-only.txt"; then
  ok "dirty fixture: the lockfile-settled fixture carries the NON-FATAL integrity PASS"
else
  bad "dirty fixture: expected a lockfile-settled tree-integrity PASS"
fi
refused "tree-start: dirty: yes with a clean commit: -> refuse (lockfile-settled run)" \
  "$T/dirty-start-only.txt" "tree-start:"

# 25(d) PRESENT KEY, EMPTY VALUE -> REFUSE. Distinct from an absent field: the
# gate said something and it reduced to nothing.
full_summary "$T/dirty-empty.txt" "$C7" "$C12" PASS PASS ""
if grep -q '^commit: .* dirty:$' "$T/dirty-empty.txt"; then
  ok "dirty fixture: the empty-value fixture ends its commit: line at the bare key"
else
  bad "dirty fixture: expected a bare trailing 'dirty:' on the commit: line"
fi
refused "commit: dirty: with an EMPTY value -> refuse" \
  "$T/dirty-empty.txt" "records NO 'dirty:' value"

# 25(e) UNRECOGNISED values -> REFUSE. `unknown` is pinned by name because it is
# the literal this script used to substitute for a missing value while `dirty:`
# was merely reported. `YES`/`No` are pinned because the compare is TOKEN-EXACT
# and case-sensitive: a near-miss spelling is an unestablished state, not a clean
# one. (`unverified` is a REAL emitted value and gets its own case below.)
for _d in maybe unknown YES No; do
  full_summary "$T/dirty-$_d.txt" "$C7" "$C12" PASS PASS "$_d"
  refused "dirty: $_d (unrecognised) -> refuse, never read as clean" \
    "$T/dirty-$_d.txt" "records 'dirty: $_d'"
done
unset _d

# 25(g) `dirty: unverified` — A REAL EMITTED VALUE, refused as DEFENCE IN DEPTH.
# scripts/agent-gate.sh:8814 writes `commit: unverified branch: <b> dirty:
# unverified` deliberately, when no validated tree capture exists (the start
# capture failed, or there is no worktree at the terminal emit): the run is
# already fail-closed there and must not name a sha nothing verified. So this arm
# closes NO live hole — a real such block is refused by `RESULT: FAIL` and by the
# non-hex `commit:` placeholder as well. It is tested BY NAME anyway, and the
# fixture deliberately keeps RESULT: PASS and a VALID hex commit: so that the
# refusal can only be the `dirty:` one: a value meaning "never measured" must not
# survive on the strength of a neighbouring key, and an untested correct
# behaviour is one refactor away from an untested wrong one.
full_summary "$T/dirty-unverified.txt" "$C7" "$C12" PASS PASS unverified
refused "dirty: unverified (the gate's own not-measured value) -> refuse on its OWN merit" \
  "$T/dirty-unverified.txt" "records 'dirty: unverified'"

# 25(h) A FOLLOWING KEY IS NOT A VALUE. scripts/agent-gate.sh renders these lines
# by unquoted interpolation of variables initialised to the empty string
# (TREE_START_DIRTY/TREE_END_DIRTY), so a block really can carry a `dirty:` key
# with nothing of its own after it — and on a space-joined line the NEXT KEY then
# sits where the value would be. Reading `digest:` as the dirty state would be a
# two-valued read of a multi-state signal one layer down: it is neither `no` nor
# absent, so it must REFUSE, naming what it actually found.
full_summary "$T/dirty-next-key.txt" "$C7" "$C12" PASS PASS "digest: a7743efe8d80"
if grep -q "^commit: .* dirty: digest: a7743efe8d80" "$T/dirty-next-key.txt"; then
  ok "dirty fixture: the following-key fixture puts another key where the value goes"
else
  bad "dirty fixture: expected 'dirty: digest: ...' on the commit: line"
fi
refused "dirty: followed by another KEY -> refuse (a key is not a value)" \
  "$T/dirty-next-key.txt" "records 'dirty: digest:'"

# 25(i) A SELFTEST-SHAPED block cannot certify a merge. The gate's selftest emits
# `commit: selftest branch: selftest dirty: no`, `tree-start: selftest ... digest:
# selftest` and `tree-integrity: PASS (selftest)` (scripts/agent-gate.sh:7826,
# :8921). Token-exactly that block reads `tree-integrity: PASS`, `RESULT: PASS`
# and `dirty: no` — so the ONE key refusing it is the hex check on
# `commit:`/`tree-start:`. That is correct and sufficient, and it is ONE key
# holding the door; pointing this script at a selftest summary is a plausible
# accident, so pin the property rather than leave it incidental. UNLIKE every
# other case in this group it is green BEFORE and AFTER #3648 — it characterises
# behaviour that already existed, and is recorded as such rather than counted as
# evidence for the new enforcement.
full_summary "$T/selftest-block.txt" selftest selftest "PASS (selftest)" PASS no
if grep -q '^commit: selftest ' "$T/selftest-block.txt" &&
   grep -q '^tree-integrity: PASS (selftest)' "$T/selftest-block.txt" &&
   grep -q '^RESULT: PASS' "$T/selftest-block.txt"; then
  ok "selftest fixture: the block really is a PASS with the selftest placeholders"
else
  bad "selftest fixture: expected a selftest-shaped PASS block"
fi
refused "a SELFTEST-shaped full block -> refuse (it certifies the gate, not this PR)" \
  "$T/selftest-block.txt" "is not lowercase hex"

# 25(f) The check reads the `commit:` line, NOT the clean-looking `tree-start:`
# one below it. The 25(b) fixture disagrees between the two on purpose.
if grep -q '^tree-start: .* dirty: yes' "$T/dirty-yes.txt"; then
  ok "dirty: the refused fixture's tree-start: mirrors yes (the gate's real shape)"
else
  bad "dirty: expected the yes fixture's tree-start: to mirror the value"
fi
if grep -q '^tree-start: .* dirty: no' "$T/dirty-maybe.txt" &&
   grep -q "^commit: .* dirty: maybe" "$T/dirty-maybe.txt"; then
  ok "dirty: an unrecognised commit: value refuses despite a clean tree-start: line"
else
  bad "dirty: the sentinel fixture should disagree between commit: and tree-start:"
fi

# --- Case 26: the mutated-path commit: parenthetical parses ------------------
# On the #2926 mutation path `commit:` carries a trailing parenthetical; token
# extraction must be robust to it. (tree-integrity is FAIL there, so this fixture
# keeps integrity PASS and only exercises the LINE SHAPE.)
{
  printf '%s\n' "$FULL_S"
  printf 'commit: %s branch: main dirty: no (VERIFIED START — the identity this run executed against)\n' "$C7"
  printf 'tree-start: %s dirty: no digest: 671a6275687c\n' "$C12"
  printf 'tree-integrity: PASS\n'
  printf 'RESULT: PASS\n'
  printf '%s\n' "$FULL_E"
} >"$T/commit-paren.txt"
if run 0 "commit: with a trailing parenthetical -> token extracted -> exit 0" \
  2421 "$CERTIFIED" "$T/commit-paren.txt"; then
  ok "shape: commit: token extraction survives a trailing parenthetical"
fi

# --- Case 27: the gate-of-record check runs BEFORE any gh call ---------------
# It is offline and cheap, and "you have no gate of record" must be reportable
# without a network round trip. Proven by a gh mock that FAILS: a missing gate of
# record must still yield the NO-GATE-OF-RECORD refusal (exit 2), not GH-FAILURE.
export MOCK_GH_FAIL=1
if run 2 "no gate of record + gh DOWN -> NO-GATE-OF-RECORD (offline check first)" \
  2421 "$CERTIFIED" "$T/lite-only.txt"; then
  case "$OUT" in
    *"NO-GATE-OF-RECORD"*)
      case "$OUT" in
        *"GH-FAILURE"*) bad "ordering: reached gh before validating the summary (got: $OUT)" ;;
        *) ok "ordering: the summary is validated BEFORE any gh call" ;;
      esac
      ;;
    *) bad "ordering: expected NO-GATE-OF-RECORD with gh down (got: $OUT)" ;;
  esac
fi
export MOCK_GH_FAIL=0

# =============================================================================
# #3465 review — the ANCHORED DELTA PAIR (optional 4th argument)
# =============================================================================
# CLAUDE.md's #1892 rule MANDATES that a test/docs-only diff on top of a full
# PASS at anchor X re-certifies with `--delta X` and "never a repeat full gate",
# and that the PR record BOTH blocks. So the merged head legitimately differs
# from the gate of record's sha, and a 3-arg-only guard red on correct,
# doctrine-mandated input.
ANCHOR="ab12cd34ef560000000000000000000000000000"   # the full gate's sha (X)
A7="ab12cd3"
A12="ab12cd34ef56"
if [ "${#ANCHOR}" -eq 40 ] && [ "${ANCHOR:0:7}" = "$A7" ] && [ "${ANCHOR:0:12}" = "$A12" ]; then
  ok "anchor fixture: ANCHOR is 40 hex and A7/A12 are its own 7/12-char prefixes"
else
  bad "anchor fixture: ANCHOR/A7/A12 are inconsistent"
fi

DELTA_MODE="MODE: delta (TEST/DOCS-ONLY RE-CERTIFICATION — NOT the gate of record; gate of record = the full agent-gate.sh PASS at anchor $ANCHOR)"

# delta_block [anchor] [commit] [tree-start] [tree-integrity] [result] [mode] \
#             [anchor-parenthetical] [dirty] -> STDOUT.  "-" omits a line entirely.
# The 8th parameter is the delta run's OWN `dirty:` value (#3648), rendered by the
# same helpers as the full block's.
# Line SHAPES are copied from scripts/agent-gate.sh's delta emit site
# (anchor_meta + SUMMARY_MODE_LINE), trailing fields included.
delta_block() {
  local anchor="${1:-$ANCHOR}" commit="${2:-$C7}" tstart="${3:-$C12}" \
        ti="${4:-PASS}" result="${5:-PASS}" mode="${6:-$DELTA_MODE}" \
        paren="${7:-(full-gate PASS commit)}" dirty="${8-no}" \
        tsdirty="${9-__mirror__}"
  # 9th param, mirroring by default: the delta block's tree-start: dirty value,
  # allowed to DIVERGE from its commit: one for the lockfile-settled case
  # (#3648 roborev round 5 -- without it the delta tree-start: assertion was
  # uncovered, since every delta fixture mirrored a single value into both).
  [ "$tsdirty" = "__mirror__" ] && tsdirty="$dirty"
  printf '%s\n' "$DELTA_S"
  printf 'run-id: /tmp/agent-gate.dLt4Qx\n'
  [ "$mode" = "-" ] || printf '%s\n' "$mode"
  [ "$commit" = "-" ] || printf 'commit: %s branch: issue-3465-require-gate-of-record%s\n' \
    "$commit" "$(dirty_field "$dirty")"
  [ "$anchor" = "-" ] || printf 'delta-anchor: %s %s\n' "$anchor" "$paren"
  printf 'delta-anchor-run-id: /tmp/agent-gate.9cIQgX\n'
  printf 'gate-of-record: full agent-gate.sh run at %s (this DELTA re-certifies a test/docs-only diff; it is NOT a substitute for the full gate)\n' "$anchor"
  printf 'delta-executors: cargo test -p cqlite-core --test issue_3465 (3), docs (2)\n'
  [ "$tstart" = "-" ] || printf 'tree-start: %s dirty: %s digest: 671a6275687c\n' \
    "$tstart" "$(dirty_tree_start "$tsdirty")"
  printf 'tree-end: %s dirty: %s digest: 671a6275687c\n' "$tstart" "$(dirty_tree_start "$dirty")"
  [ "$ti" = "-" ] || printf 'tree-integrity: %s\n' "$ti"
  printf 'logs: /tmp/agent-gate.dLt4Qx\n'
  [ "$result" = "-" ] || printf 'RESULT: %s\n' "$result"
  printf '%s\n' "$DELTA_E"
}
delta_summary() { local f="$1"; shift; delta_block "$@" >"$f"; }

# refused_pair <desc> <full-file> <delta-file> [needle] — a 4-arg refusal.
refused_pair() {
  local desc="$1" f="$2" d="$3" needle="${4:-}"
  if run 2 "$desc" 2421 "$CERTIFIED" "$f" "$d"; then
    case "$OUT" in
      *"PREMERGE: NO-GATE-OF-RECORD"*) ;;
      *) bad "$desc: missing NO-GATE-OF-RECORD verdict (got: $OUT)"; return 1 ;;
    esac
    if [ -n "$needle" ] && [ "${OUT#*"$needle"}" = "$OUT" ]; then
      bad "$desc: refusal does not name the cause '$needle' (got: $OUT)"
      return 1
    fi
    ok "$desc"
  fi
}

ANCHORFULL="$T/anchor-full.txt"
full_summary "$ANCHORFULL" "$A7" "$A12" PASS PASS
GOODDELTA="$T/good-delta.txt"
delta_summary "$GOODDELTA"

# --- Case 28(a): 3-arg call with an ANCHOR-only summary still refuses ---------
# The red half of the blocker: without the fourth argument the anchor's full
# PASS names a different tree than the head being merged, and that is exactly
# the #3616 shape — indistinguishable from a peer lane's summary.
refused "anchor-only full summary at a MOVED head (3 args) -> refuse" \
  "$ANCHORFULL" "does not match the certified sha"

# --- Case 28(b): the anchored delta PAIR at that same moved head -> accept ---
if run 0 "anchored delta pair (full PASS at X + delta at Y) -> exit 0" \
  2421 "$CERTIFIED" "$ANCHORFULL" "$GOODDELTA"; then
  case "$OUT" in
    *"PREMERGE: OK $CERTIFIED"*) ok "delta pair: prints PREMERGE: OK <sha>" ;;
    *) bad "delta pair: missing PREMERGE: OK (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: GATE-OF-RECORD commit: $A7 tree-start: $A12"*)
      ok "delta pair: the GATE-OF-RECORD line names the ANCHOR's provenance" ;;
    *) bad "delta pair: GATE-OF-RECORD line must name the anchor (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: DELTA-RECERT anchor: $ANCHOR commit: $C7 tree-start: $C12"*)
      ok "delta pair: a DISTINCT DELTA-RECERT line names the anchor + the merged tree" ;;
    *) bad "delta pair: missing the DELTA-RECERT evidence line (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"summary: $GOODDELTA"*) ok "delta pair: the DELTA-RECERT line names the delta summary file" ;;
    *) bad "delta pair: DELTA-RECERT must name the delta summary file (got: $OUT)" ;;
  esac
fi

# --- Case 28(c): delta-anchor: naming a DIFFERENT sha -> refuse --------------
# This is the property that makes the pair a CHAIN rather than two unrelated
# blocks: without it, ANY full PASS could anchor ANY delta run.
delta_summary "$T/delta-wrong-anchor.txt" "$STALE"
refused_pair "delta-anchor: names a DIFFERENT sha than the full block -> refuse" \
  "$ANCHORFULL" "$T/delta-wrong-anchor.txt" \
  "in the full-gate block does not match the delta block's anchor sha"

# --- Case 28(d): the delta run's OWN provenance must cover the merged tree ---
delta_summary "$T/delta-wrong-commit.txt" "$ANCHOR" "ca8eb01" "$C12"
refused_pair "delta commit: does not match the certified sha -> refuse" \
  "$ANCHORFULL" "$T/delta-wrong-commit.txt" \
  "'commit:' value 'ca8eb01' in the delta block does not match the certified sha"
delta_summary "$T/delta-wrong-tstart.txt" "$ANCHOR" "$C7" "ca8eb016def1"
refused_pair "delta tree-start: does not match the certified sha -> refuse" \
  "$ANCHORFULL" "$T/delta-wrong-tstart.txt" \
  "'tree-start:' value 'ca8eb016def1' in the delta block does not match the certified sha"

# --- Case 28(e): delta-anchor: (UNRESOLVED) -> refuse ------------------------
# scripts/agent-gate.sh emits this on its ERROR path when the --delta anchor does
# not resolve to a commit. Tested at RESULT: PASS so the needle pins the ANCHOR
# check rather than the RESULT check...
delta_summary "$T/delta-unresolved.txt" "$ANCHOR" "$C7" "$C12" PASS PASS "$DELTA_MODE" "(UNRESOLVED)"
refused_pair "delta-anchor: (UNRESOLVED) -> refuse" \
  "$ANCHORFULL" "$T/delta-unresolved.txt" "(UNRESOLVED)"
# ...and once in the shape the gate really emits it (RESULT: ERROR, a ref name
# rather than a resolved sha), which must also refuse.
delta_summary "$T/delta-unresolved-real.txt" "HEAD~3" "$C7" "$C12" PASS ERROR \
  "$DELTA_MODE" "(UNRESOLVED)"
refused_pair "delta-anchor: (UNRESOLVED) in its real RESULT: ERROR shape -> refuse" \
  "$ANCHORFULL" "$T/delta-unresolved-real.txt"

# --- Case 29: the delta-anchor value's own shape -----------------------------
delta_summary "$T/delta-no-anchor.txt" "-"
refused_pair "delta block with NO delta-anchor: line -> refuse" \
  "$ANCHORFULL" "$T/delta-no-anchor.txt" "has no 'delta-anchor:' line"
delta_summary "$T/delta-anchor-short.txt" "$A12"
refused_pair "delta-anchor: abbreviated (12 hex, not the resolved 40) -> refuse" \
  "$ANCHORFULL" "$T/delta-anchor-short.txt" "is 12 hex chars,"
delta_summary "$T/delta-anchor-nonhex.txt" "unverified"
refused_pair "delta-anchor: non-hex -> refuse" \
  "$ANCHORFULL" "$T/delta-anchor-nonhex.txt" "is not lowercase hex"

# --- Case 30: the fourth argument must BE a delta block ----------------------
refused_pair "a FULL summary passed as the fourth argument -> refuse" \
  "$ANCHORFULL" "$GOOD" "holds ZERO delta blocks"
if [ "${OUT#*"found 1 full"}" != "$OUT" ]; then
  ok "fourth-arg: refusal NAMES the full block it found instead of a delta one"
else
  bad "fourth-arg: refusal should name what it found (got: $OUT)"
fi
refused_pair "a LITE summary passed as the fourth argument -> refuse" \
  "$ANCHORFULL" "$T/lite-only.txt" "holds ZERO delta blocks"
{ delta_block; delta_block; } >"$T/two-deltas.txt"
if [ "$(grep -c -x -F "$DELTA_S" "$T/two-deltas.txt")" -eq 2 ]; then
  ok "two-deltas fixture: the file really does hold TWO delta start markers"
else
  bad "two-deltas fixture: expected 2 delta start markers"
fi
refused_pair "TWO delta blocks in the fourth argument -> refuse as AMBIGUOUS" \
  "$ANCHORFULL" "$T/two-deltas.txt" "holds 2 delta blocks"
refused_pair "fourth-argument file absent -> refuse" \
  "$ANCHORFULL" "$T/no-such-delta.txt" "delta summary file does not exist"
: >"$T/empty-delta.txt"
refused_pair "fourth-argument file EMPTY -> refuse" \
  "$ANCHORFULL" "$T/empty-delta.txt" "delta summary file is EMPTY"
if run 3 "EMPTY fourth argument -> exit 3 (usage, never a silent 3-arg downgrade)" \
  2421 "$CERTIFIED" "$ANCHORFULL" ""; then
  ok "usage: an empty fourth argument is a usage failure, not '3-arg mode'"
fi
if run 3 "five arguments -> exit 3 (usage)" 2421 "$CERTIFIED" "$ANCHORFULL" "$GOODDELTA" extra; then
  ok "usage: a fifth argument fails closed rather than being ignored"
fi

# --- Case 31: MODE: delta is REQUIRED in the delta block (the INVERSE belt) --
# In the FULL block a MODE: line is a refusal; in the DELTA block its ABSENCE
# is, and the token is asserted AFFIRMATIVELY rather than merely tolerated.
delta_summary "$T/delta-no-mode.txt" "$ANCHOR" "$C7" "$C12" PASS PASS "-"
refused_pair "delta block with NO MODE: line -> refuse (presence is REQUIRED here)" \
  "$ANCHORFULL" "$T/delta-no-mode.txt" "has no 'MODE:' line"
delta_summary "$T/delta-mode-lite.txt" "$ANCHOR" "$C7" "$C12" PASS PASS \
  "MODE: lite (FAST ITERATION — NOT the gate of record)"
refused_pair "delta header + MODE: lite -> refuse (token asserted, not presence)" \
  "$ANCHORFULL" "$T/delta-mode-lite.txt" "MODE token is 'lite', not 'delta'"

# --- Case 32: the delta block's own verdicts ---------------------------------
delta_summary "$T/delta-result-fail.txt" "$ANCHOR" "$C7" "$C12" PASS FAIL
refused_pair "delta RESULT: FAIL -> refuse" \
  "$ANCHORFULL" "$T/delta-result-fail.txt" "RESULT verdict token in the delta block is 'FAIL'"
delta_summary "$T/delta-ti-fail.txt" "$ANCHOR" "$C7" "$C12" \
  "FAIL (tree-mutated-midrun; head da9a7cb->ca8eb01; changed: docs/x.md)" PASS
refused_pair "delta tree-integrity: FAIL -> refuse" \
  "$ANCHORFULL" "$T/delta-ti-fail.txt" "tree-integrity verdict token in the delta block is 'FAIL'"
delta_block >"$T/delta-unterminated.txt.full"
grep -v -x -F "$DELTA_E" "$T/delta-unterminated.txt.full" >"$T/delta-unterminated.txt"
refused_pair "UNTERMINATED delta block -> refuse" \
  "$ANCHORFULL" "$T/delta-unterminated.txt" "UNTERMINATED"

# --- Case 33: the ANCHOR block is still held to the full-gate contract -------
# Case B relaxes ONLY the anchor's sha binding. Everything else it must satisfy
# in Case A it must still satisfy here.
{ full_block "$A7" "$A12" PASS FAIL; } >"$T/anchor-fail.txt"
refused_pair "anchor block RESULT: FAIL -> refuse even with a valid delta" \
  "$T/anchor-fail.txt" "$GOODDELTA" "RESULT verdict token in the full-gate block is 'FAIL'"
refused_pair "a LITE-only file as the ANCHOR -> refuse (the #3408 case, 4-arg form)" \
  "$T/lite-only.txt" "$GOODDELTA" "ZERO full-gate blocks"
full_summary "$T/anchor-nonhex.txt" "unverified" "$A12" PASS PASS
refused_pair "anchor commit: non-hex -> refuse (a real sha is still required)" \
  "$T/anchor-nonhex.txt" "$GOODDELTA" "is not lowercase hex"

# --- Case 33(x): `dirty: no` is required of BOTH blocks in Case B (#3648) ----
# The delta run is the one that covers the tree being MERGED, and the anchor is
# the full PASS the whole chain hangs from. A dirty tree in either place breaks
# the same property, so both are held to the requirement.

# 33(x)(a) POSITIVE CONTROL — both clean -> exit 0, both evidence lines report it.
if run 0 "delta pair with BOTH blocks dirty: no -> exit 0" \
  2421 "$CERTIFIED" "$ANCHORFULL" "$GOODDELTA"; then
  case "$OUT" in
    *"GATE-OF-RECORD commit: $A7"*"dirty: no"*"DELTA-RECERT anchor:"*"dirty: no"*)
      ok "dirty (case B): BOTH evidence lines report the clean tree" ;;
    *) bad "dirty (case B): both evidence lines must report dirty: no (got: $OUT)" ;;
  esac
fi

# 33(x)(b) the DELTA block dirty -> refuse, naming the delta block.
delta_summary "$T/delta-dirty-yes.txt" "$ANCHOR" "$C7" "$C12" PASS PASS \
  "$DELTA_MODE" "(full-gate PASS commit)" yes
refused_pair "delta block dirty: yes -> refuse (it covers the MERGED tree)" \
  "$ANCHORFULL" "$T/delta-dirty-yes.txt" "The delta block records 'dirty: yes'"
# 33(x)(b2) THE REMEDY IS PER-ARTIFACT (#3648 roborev round 1, finding 1). A dirty
# DELTA block must send the operator back to the `--delta` re-certification, NOT to
# a repeat full gate: #1892 mandates `--delta` for a test/docs-only diff on top of a
# full PASS and forbids re-running the full gate. A refusal naming the wrong remedy
# routes a correct operator down a path doctrine forbids.
case "$OUT" in
  *"re-run the --delta re-certification on the clean tree"*)
    ok "dirty (delta): the refusal names the DELTA remedy, not a repeat full gate" ;;
  *) bad "dirty (delta): the refusal must name the --delta remedy (got: $OUT)" ;;
esac
case "$OUT" in
  *"re-run the FULL gate"*)
    bad "dirty (delta): the delta refusal must NOT tell the operator to re-run the FULL gate (got: $OUT)" ;;
  *) ok "dirty (delta): the delta refusal does NOT misdirect to a repeat full gate" ;;
esac
case "$OUT" in
  *"anchor's own full-gate PASS is unaffected"*)
    ok "dirty (delta): the refusal states the anchor's full PASS still stands" ;;
  *) bad "dirty (delta): the refusal should say the anchor PASS is unaffected (got: $OUT)" ;;
esac

# 33(x)(c) the ANCHOR (full) block dirty -> refuse, naming the full-gate block.
# The delta here is the GOOD one, so the refusal cannot be the delta's.
full_summary "$T/anchor-dirty-yes.txt" "$A7" "$A12" PASS PASS yes
refused_pair "anchor block dirty: yes -> refuse even with a clean delta re-cert" \
  "$T/anchor-dirty-yes.txt" "$GOODDELTA" "The full-gate block records 'dirty: yes'"

# 33(x)(c2) THE DELTA HALF OF THE LOCKFILE-SETTLED CASE (#3648 roborev round 5).
# 33(x)(b)/(c) mirror ONE dirty value into both commit: and tree-start:, so they
# would still pass with the delta-specific tree-start: assertion deleted -- i.e.
# that assertion was uncovered. (My own round-4 RED-verify showed it: removing
# BOTH tree-start assertions red only ONE case.) These two pin each block's
# tree-start: independently, with commit: clean so the refusal cannot be its.
delta_summary "$T/delta-start-only.txt" "$ANCHOR" "$C7" "$C12" \
  "PASS (lockfile-settled: Cargo.lock)" PASS "$DELTA_MODE" "(full-gate PASS commit)" no yes
if grep -qE '^tree-start: .* dirty: yes ' "$T/delta-start-only.txt" \
   && grep -qE '^commit: .* dirty: no$' "$T/delta-start-only.txt"; then
  ok "dirty fixture (delta): dirty at START, clean at commit: -- values are INDEPENDENT"
else
  bad "dirty fixture (delta): expected tree-start dirty: yes with commit: dirty: no"
fi
refused_pair "delta tree-start: dirty: yes with a clean commit: -> refuse" \
  "$ANCHORFULL" "$T/delta-start-only.txt" "tree-start:"

# And the ANCHOR's own tree-start:, with a wholly clean delta beside it.
full_summary "$T/anchor-start-only.txt" "$A7" "$A12" \
  "PASS (lockfile-settled: Cargo.lock)" PASS no yes
refused_pair "anchor tree-start: dirty: yes with a clean commit: and a clean delta -> refuse" \
  "$T/anchor-start-only.txt" "$GOODDELTA" "tree-start:"

# 33(x)(d) an ABSENT dirty: field in either block -> refuse (never read as clean).
delta_summary "$T/delta-dirty-absent.txt" "$ANCHOR" "$C7" "$C12" PASS PASS \
  "$DELTA_MODE" "(full-gate PASS commit)" -
refused_pair "delta block with NO dirty: field -> refuse" \
  "$ANCHORFULL" "$T/delta-dirty-absent.txt" "The delta block records NO 'dirty:' value"
full_summary "$T/anchor-dirty-absent.txt" "$A7" "$A12" PASS PASS -
refused_pair "anchor block with NO dirty: field -> refuse" \
  "$T/anchor-dirty-absent.txt" "$GOODDELTA" "The full-gate block records NO 'dirty:' value"

# =============================================================================
# #3465 review — the remaining refusal branches
# =============================================================================

# --- Case 34: DUPLICATE keys inside one block -> refuse ----------------------
# "Last one wins" would let a doctored line override the real verdict. Without
# these fixtures the >1 branch of assert_single_key was dead code.
{
  printf '%s\n' "$FULL_S"
  printf 'commit: %s branch: main dirty: no\n' "$C7"
  printf 'tree-start: %s dirty: no digest: 671a6275687c\n' "$C12"
  printf 'tree-integrity: PASS\n'
  printf 'RESULT: FAIL\n'
  printf 'RESULT: PASS\n'
  printf '%s\n' "$FULL_E"
} >"$T/dup-result.txt"
refused "TWO RESULT: lines in one block -> refuse as AMBIGUOUS" \
  "$T/dup-result.txt" "has 2 'RESULT:' lines"
{
  printf '%s\n' "$FULL_S"
  printf 'commit: ca8eb01 branch: main dirty: no\n'
  printf 'commit: %s branch: main dirty: no\n' "$C7"
  printf 'tree-start: %s dirty: no digest: 671a6275687c\n' "$C12"
  printf 'tree-integrity: PASS\n'
  printf 'RESULT: PASS\n'
  printf '%s\n' "$FULL_E"
} >"$T/dup-commit.txt"
refused "TWO commit: lines in one block -> refuse as AMBIGUOUS" \
  "$T/dup-commit.txt" "has 2 'commit:' lines"

# --- Case 35: an UNREADABLE summary file -> refuse ---------------------------
# Distinct from absent and from empty. Skipped when running as root, which
# ignores the mode bit.
if [ "$(id -u)" = "0" ]; then
  ok "unreadable summary: SKIPPED (running as root ignores the mode bit)"
else
  cp "$GOOD" "$T/unreadable.txt"
  chmod 000 "$T/unreadable.txt"
  refused "summary file present but UNREADABLE -> refuse" \
    "$T/unreadable.txt" "is not readable"
  chmod 644 "$T/unreadable.txt"
fi

# --- Case 36: the PARSER TOOL failing is exit 3, not exit 2 ------------------
# A missing/ENOMEM-ing awk is a broken BOX, not a verdict about the gate of
# record — telling that caller to re-run a 45-minute gate would be the wrong
# remedy, so this one path exits 3 and NAMES the tool.
BADBIN="$T/badbin"
mkdir -p "$BADBIN"
printf '#!/bin/sh\necho "awk: cannot allocate" >&2\nexit 1\n' >"$BADBIN/awk"
chmod +x "$BADBIN/awk"
# The NEUTRAL copy (#3650 review R5 F2), like every non-advisory case: this one
# refuses at exit 3 before the advisory is reached, but running the shipped
# artifact here would leave a second path that could regress into an ambient scan.
OUT=$(PATH="$BADBIN:$BIN:$PATH" bash "$NEUTRAL_ASSERT" 2421 "$CERTIFIED" "$GOOD" \
  --c-verdict "$C_PASS_FILE" 2>&1)
RC=$?
if [ "$RC" -eq 3 ]; then
  case "$OUT" in
    *"TOOL-FAILURE"*awk*) ok "tool failure: a failing awk is exit 3 and NAMES the tool" ;;
    *) bad "tool failure: exit 3 but the message must name the tool (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"NO-GATE-OF-RECORD"*) bad "tool failure: must NOT be reported as NO-GATE-OF-RECORD (got: $OUT)" ;;
    *) ok "tool failure: not misreported as a missing gate of record" ;;
  esac
else
  bad "tool failure: wanted exit 3, got $RC (output: $OUT)"
fi

# --- Case 37: the REAL --only shape -> refused by RESULT, not by the belt ----
# scripts/agent-gate.sh emits, for --only, the FULL markers, RESULT: PARTIAL and
# a LOWERCASE `mode: PARTIAL (--only …)` line. The MODE: belt is case-sensitive
# ($1 == "MODE:") and deliberately does NOT catch it: the property that matters
# is the RESULT compare. This is the most plausible real mis-invocation.
{
  printf '%s\n' "$FULL_S"
  printf 'run-id: /tmp/agent-gate.9cIQgX\n'
  printf 'commit: %s branch: issue-3465-require-gate-of-record dirty: no\n' "$C7"
  printf 'tree-start: %s dirty: no digest: 671a6275687c\n' "$C12"
  printf 'tree-end: %s dirty: no digest: 671a6275687c\n' "$C12"
  printf 'tree-integrity: PASS\n'
  printf 'mode: PARTIAL (--only file-size) - does NOT count as the gate\n'
  printf 'file-size:         PASS (0s)\n'
  printf 'RESULT: PARTIAL\n'
  printf '%s\n' "$FULL_E"
} >"$T/only-partial.txt"
if grep -q -x -F 'mode: PARTIAL (--only file-size) - does NOT count as the gate' "$T/only-partial.txt"; then
  ok "--only fixture: carries the LOWERCASE mode: PARTIAL line the gate really emits"
else
  bad "--only fixture: expected the verbatim lowercase mode: PARTIAL line"
fi
refused "a real --only summary (RESULT: PARTIAL) -> refuse" \
  "$T/only-partial.txt" "RESULT verdict token in the full-gate block is 'PARTIAL'"
case "$OUT" in
  *"carries a MODE: line"*)
    bad "--only: refused by the case-sensitive MODE: belt, not by RESULT (got: $OUT)" ;;
  *) ok "--only: the case-sensitive MODE: belt is deliberately NOT what catches it" ;;
esac

# --- Case 38: a NESTED sub-gate block -> refuse ------------------------------
# #2874: a gate spawned by an enclosing gate stamps `nested-under: <parent>` and
# emits the FULL markers at the SAME tree, so the sha binding provably cannot
# distinguish it. One affirmative line closes the only wrong-file class the sha
# compare cannot see.
{
  printf '%s\n' "$FULL_S"
  printf 'run-id: /tmp/agent-gate.nested1\n'
  printf 'nested-under: /tmp/agent-gate.9cIQgX\n'
  printf 'commit: %s branch: main dirty: no\n' "$C7"
  printf 'tree-start: %s dirty: no digest: 671a6275687c\n' "$C12"
  printf 'tree-integrity: PASS\n'
  printf 'RESULT: PASS\n'
  printf '%s\n' "$FULL_E"
} >"$T/nested.txt"
refused "nested sub-gate (nested-under:) at the RIGHT sha -> refuse" \
  "$T/nested.txt" "nested-under"
delta_summary "$T/delta-plain.txt"
{ grep -v -x -F "$DELTA_E" "$T/delta-plain.txt"
  printf 'nested-under: /tmp/agent-gate.9cIQgX\n'
  printf '%s\n' "$DELTA_E"
} >"$T/delta-nested.txt"
refused_pair "nested sub-gate in the DELTA block -> refuse" \
  "$ANCHORFULL" "$T/delta-nested.txt" "nested-under"

# --- Case 39: the success path DISCLAIMS what it did not prove (#3650) ------
# A squash-merge composes this diff with main's CURRENT tip, so the tree
# certified here and the tree that lands are different objects for any PR whose
# base is behind main. `PREMERGE: GATE-OF-RECORD` must not be readable as full
# certification.
if run 0 "success path prints the SCOPE disclaimer" 2421 "$CERTIFIED" "$GOOD"; then
  case "$OUT" in
    *"PREMERGE: SCOPE"*) ok "scope: the success path prints a PREMERGE: SCOPE clause" ;;
    *) bad "scope: missing the PREMERGE: SCOPE clause (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"#3650"*) ok "scope: the clause names the follow-up issue (#3650)" ;;
    *) bad "scope: the clause must name #3650 (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"does NOT prove"*) ok "scope: the clause states what was NOT proven" ;;
    *) bad "scope: the clause must state what was NOT proven (got: $OUT)" ;;
  esac
fi
if run 0 "success path prints SCOPE in the anchored-delta case too" \
  2421 "$CERTIFIED" "$ANCHORFULL" "$GOODDELTA"; then
  case "$OUT" in
    *"PREMERGE: SCOPE"*) ok "scope: the delta pair carries the same disclaimer" ;;
    *) bad "scope: the delta pair must carry the SCOPE clause too (got: $OUT)" ;;
  esac
fi
# #3650 SLICE 1 EXTENSION. Slice 1 ships the base-staleness ADVISORY, which does
# NOT close the merge-result gap, so all THREE original SCOPE lines are RETAINED
# verbatim and only ONE line is added pointing at the advisory. Pinning the three
# by their own text (not just by the marker) is what makes "retained" checkable:
# a reword that quietly dropped one would otherwise still satisfy Case 39 above.
SCOPE1="this proves a full gate PASSed on THIS tree"
SCOPE2="the tree was certified against current main (#3650)"
SCOPE3="composes this diff with main tip, which no gate here has executed"
for shape in direct delta; do
  if [ "$shape" = direct ]; then
    run 0 "SCOPE retained (direct)" 2421 "$CERTIFIED" "$GOOD" || continue
  else
    run 0 "SCOPE retained (anchored delta)" 2421 "$CERTIFIED" "$ANCHORFULL" "$GOODDELTA" || continue
  fi
  missing=""
  for needle in "$SCOPE1" "$SCOPE2" "$SCOPE3"; do
    [ "${OUT#*"$needle"}" = "$OUT" ] && missing="$missing | $needle"
  done
  if [ -z "$missing" ]; then
    ok "scope($shape): all THREE #3465 SCOPE lines are retained verbatim (#3650 slice 1)"
  else
    bad "scope($shape): a SCOPE line was dropped:$missing"
  fi
  case "$OUT" in
    *"PREMERGE: SCOPE the PREMERGE: ADVISORY lines below measure that gap"*)
      ok "scope($shape): the added SCOPE line points at the advisory" ;;
    *) bad "scope($shape): the added SCOPE line must point at the advisory (got: $OUT)" ;;
  esac
done

# --- Case 41: the base-staleness ADVISORY (#3650 slice 1) -------------------
# Slice 1's whole contract is that it changes NO verdict. The advisory is
# resolved from premerge-assert.sh's OWN directory with no env override
# (#3312's enforcer rule), so a case needing a different advisory SUBSTITUTES
# THE ARTIFACT in a scratch copy of the tree — never a path variable, which
# would be one more seam a real invoker could set.
#
# flow_copy <name> <advisory-body|ABSENT> — builds the scratch copy and sets the
# global $COPY. It sets a GLOBAL rather than printing a path on purpose: a
# command-substitution form runs in a SUBSHELL, so a bad() inside it would
# increment a counter that dies with the subshell, and a failed copy would make
# the caller's `&&` chain skip the case SILENTLY — a vacuous pass. The `local`
# declarations are split too: `local a="$1" d="$T/$a/x"` reads $a before the
# assignment takes effect and dies under `set -u`.
flow_copy() {
  local name="$1" body="$2"
  local d="$T/$name/flow"
  COPY=""
  mkdir -p "$d"
  if ! cp "$ASSERT" "$d/premerge-assert.sh"; then
    bad "flow_copy($name): could not copy premerge-assert.sh into the scratch tree"
    return 1
  fi
  if [ "$body" != ABSENT ]; then
    printf '%s\n' "$body" >"$d/base-staleness.sh"
    chmod +x "$d/base-staleness.sh"
  fi
  if [ ! -f "$d/premerge-assert.sh" ]; then
    bad "flow_copy($name): the scratch copy is missing after cp"
    return 1
  fi
  COPY="$d/premerge-assert.sh"
  return 0
}

# run_copy <expected-exit> <desc> <copied-assert> <args...>
run_copy() {
  local want="$1" desc="$2" script="$3"
  shift 3
  c_inject_into ${1+"$@"}
  set -- ${C_INJ[@]+"${C_INJ[@]}"}
  OUT=$(PATH="$BIN:$PATH" bash "$script" ${1+"$@"} 2>&1)
  RC=$?
  if [ "$RC" -ne "$want" ]; then
    bad "$desc (exit $RC, wanted $want)"
    printf '     output: %s\n' "$OUT"
    return 1
  fi
  return 0
}

STALE_ADV='#!/usr/bin/env bash
printf "BASE-STALENESS: behind 107 commits (on origin/main, not reachable from the merge-base)\n"
printf "BASE-STALENESS: blast-radius 22 RECOGNISED of 107 commits behind\n"
printf "BASE-STALENESS: verdict STALE-RECOGNISED\n"
exit 4'
if flow_copy adv-stale "$STALE_ADV" &&
  run_copy 0 "a STALE-RECOGNISED advisory still merges (slice 1 changes no verdict)" \
    "$COPY" 2421 "$CERTIFIED" "$GOOD"; then
  case "$OUT" in
    *"PREMERGE: OK $CERTIFIED"*) ok "advisory: a stale base still reaches PREMERGE: OK" ;;
    *) bad "advisory: the verdict must be unchanged by the advisory (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: ADVISORY BASE-STALENESS: verdict STALE-RECOGNISED"*)
      ok "advisory: the finding is printed on PREMERGE: ADVISORY lines" ;;
    *) bad "advisory: STALE-RECOGNISED must be reported on ADVISORY lines (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: ADVISORY BASE-STALENESS: behind 107 commits"*)
      ok "advisory: every line of the advisory's report is carried through" ;;
    *) bad "advisory: the behind-count line must be carried through (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: SCOPE"*"#3650"*) ok "advisory: the SCOPE disclaimer is STILL printed alongside it" ;;
    *) bad "advisory: the SCOPE lines must survive (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"treat exit 5 /"*"UNMEASURED as STALE"*)
      ok "advisory: the UNMEASURED-is-stale consumer contract is stated at the merge point" ;;
    *) bad "advisory: the UNMEASURED-is-stale contract must be stated (got: $OUT)" ;;
  esac
fi

# NON-VACUITY for the four stub cases below: the STALE stub above genuinely
# carries the shape the case claims (exit 4 + the verdict token), so a case
# asserting "a stale advisory does not change the verdict" is not testing a stub
# that silently exits 0.
if [ "${STALE_ADV#*exit 4}" != "$STALE_ADV" ] &&
  [ "${STALE_ADV#*STALE-RECOGNISED}" != "$STALE_ADV" ]; then
  ok "advisory fixture: the stub really exits 4 and really reports STALE-RECOGNISED"
else
  bad "advisory fixture: the stale stub does not have the shape the case claims"
fi

for pair in "5:an UNMEASURED (exit 5) advisory" "9:a BROKEN (exit 9) advisory" \
  "0:a NO-STALENESS (exit 0) advisory"; do
  code="${pair%%:*}"
  what="${pair#*:}"
  body="#!/usr/bin/env bash
printf 'BASE-STALENESS: verdict from a stub exiting %s\n' $code
exit $code"
  if flow_copy "adv-exit-$code" "$body" &&
    run_copy 0 "$what cannot change the exit code" "$COPY" 2421 "$CERTIFIED" "$GOOD"; then
    case "$OUT" in
      *"PREMERGE: ADVISORY"*"exit $code"*)
        ok "advisory: $what is REPORTED with its exit code, and is not fatal" ;;
      *) bad "advisory: $what must be reported with exit $code (got: $OUT)" ;;
    esac
  fi
done

# An ABSENT advisory is reported, not fatal — the artifact is simply not copied.
if flow_copy adv-absent ABSENT &&
  run_copy 0 "an ABSENT advisory cannot fail the assert" "$COPY" 2421 "$CERTIFIED" "$GOOD"; then
  case "$OUT" in
    *"PREMERGE: ADVISORY base-staleness.sh is ABSENT"*)
      ok "advisory: an absent advisory is named on an ADVISORY line, and is not fatal" ;;
    *) bad "advisory: an absent advisory must be reported (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: OK $CERTIFIED"*) ok "advisory: the verdict is unchanged when the advisory is absent" ;;
    *) bad "advisory: an absent advisory must not change the verdict (got: $OUT)" ;;
  esac
fi

# The advisory must measure the CERTIFIED SHA, not this checkout's HEAD (#3650
# review F1). Invoked with no rev the advisory defaults to HEAD — the LOCAL head,
# which is exactly what the surrounding assert exists because it can differ from
# the sha being approved. A report about a different diff than the one being
# merged is the "satisfied and wrong" shape. The stub ECHOES its own argv, so the
# case observes the argument rather than reasoning about it.
ARGV_ADV='#!/usr/bin/env bash
printf "BASE-STALENESS: argv-count %s\n" "$#"
printf "BASE-STALENESS: argv1 %s\n" "${1-NO-REV-PASSED}"
exit 0'
# NON-VACUITY: the stub genuinely reports its argv, so `argv1 <sha>` cannot be a
# constant it prints regardless. Run it with NO argument and require the
# no-rev marker — otherwise the case below would pass against a stub that always
# printed the sha.
printf '%s\n' "$ARGV_ADV" >"$T/argv-probe.sh"
if [ "$(bash "$T/argv-probe.sh" | grep -c 'argv1 NO-REV-PASSED')" -eq 1 ] &&
  [ "$(bash "$T/argv-probe.sh" whatever | grep -c 'argv1 whatever')" -eq 1 ]; then
  ok "advisory fixture: the argv stub really echoes its own argument list"
else
  bad "advisory fixture: the argv stub does not report its argv (the case would be vacuous)"
fi
if flow_copy adv-argv "$ARGV_ADV" &&
  run_copy 0 "the advisory is invoked with the CERTIFIED sha, not the local HEAD" \
    "$COPY" 2421 "$CERTIFIED" "$GOOD"; then
  case "$OUT" in
    *"PREMERGE: ADVISORY BASE-STALENESS: argv1 $CERTIFIED"*)
      ok "advisory: the certified sha is passed to the advisory as its subject rev (#3650 F1)" ;;
    *) bad "advisory: the advisory must receive the certified sha as \$1 (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: ADVISORY BASE-STALENESS: argv-count 1"*)
      ok "advisory: exactly ONE argument is passed (a second positional is a usage error there)" ;;
    *) bad "advisory: exactly one argument must be passed (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"argv1 NO-REV-PASSED"*)
      bad "advisory: the advisory was invoked with NO rev — it would measure the local HEAD" ;;
    *) ok "advisory: the advisory is never invoked bare (which would default to local HEAD)" ;;
  esac
fi

# An advisory printing NOTHING is reported too (an empty report is not a clean one).
SILENT_ADV='#!/usr/bin/env bash
exit 0'
if flow_copy adv-silent "$SILENT_ADV" &&
  run_copy 0 "a SILENT advisory is reported, not read as clean" "$COPY" 2421 "$CERTIFIED" "$GOOD"; then
  case "$OUT" in
    *"produced NO output"*) ok "advisory: an empty report is named rather than read as a finding" ;;
    *) bad "advisory: an empty report must be named (got: $OUT)" ;;
  esac
fi

# --- Case 41b: the 60s BOUND is never silently dropped (#3650 review B1) -----
# Two paths, one HANGING stub, and the stub TOUCHES A MARKER FILE before it
# sleeps — so "was it executed?" is answered by an artifact on disk rather than
# by reading elapsed time, which would be a wall-clock assert in a correctness
# test. The stub sleeps a BOUNDED 20s (not forever) so a REGRESSION of either
# path fails the case instead of hanging the suite, and it redirects the sleep's
# stdout to /dev/null: a surviving grandchild holding the command-substitution
# pipe would block the parent for the full sleep even after the bounded child was
# killed, which is a way for the timeout to "work" and the caller to hang anyway.
hang_stub() {
  cat <<STUB
#!/usr/bin/env bash
printf 'BASE-STALENESS: hanging stub REACHED\\n'
: >"$1"
sleep 20 >/dev/null 2>&1
printf 'BASE-STALENESS: hanging stub FINISHED - the bound did not apply\\n'
exit 0
STUB
}

# Path 1 — a supported runner PRESENT. A shim stands in for it (the suite cannot
# wait 60s for the real bound): it RECORDS what the script requested — the kill
# grace AND the bound — then applies a short one of its own and reports 124 the
# way `timeout` does. So the case asserts every half of the contract: the
# constants really reach the runner, `--kill-after` really is passed, and a
# hanging advisory really is cut off without touching the exit code.
#
# THE SHIM MUST ALSO ANSWER THE CAPABILITY PROBE (#3650 review R1/R2): the script
# now resolves its runner by PROBING `--kill-after=1 1 true`, and a shim that
# failed that probe would make the script take the SKIP path — the bounded case
# would then pass for the wrong reason, asserting nothing about the bound. The
# probe is answered by delegating a plain `<secs> <cmd...>` invocation.
SHIMD="$T/timeout-shim-bin"
mkdir -p "$SHIMD"
cat >"$SHIMD/timeout" <<'SHIM'
#!/usr/bin/env bash
grace=none
case "$1" in
  --kill-after=*) grace="${1#--kill-after=}"; shift ;;
esac
printf 'BASE-STALENESS: bound-shim requested %s grace %s\n' "$1" "$grace"
shift
# The capability probe (`--kill-after=1 1 true`) must SUCCEED, or the script
# concludes there is no supported runner and skips the advisory entirely.
if [ "$#" -eq 1 ] && [ "$1" = true ]; then
  exit 0
fi
"$@" &
child=$!
( sleep 2; kill -9 "$child" 2>/dev/null ) &
killer=$!
wait "$child"
rc=$?
kill "$killer" 2>/dev/null
[ "$rc" -ge 128 ] && rc=124
exit "$rc"
SHIM
chmod +x "$SHIMD/timeout"

MARK_A="$T/hang-marker-bounded"
rm -f "$MARK_A"
if flow_copy adv-hang-bounded "$(hang_stub "$MARK_A")"; then
  OUT=$(PATH="$SHIMD:$BIN:$PATH" bash "$COPY" 2421 "$CERTIFIED" "$GOOD" \
    --c-verdict "$C_PASS_FILE" 2>&1)
  RC=$?
  if [ "$RC" -ne 0 ]; then
    bad "advisory bound: a hanging advisory must not change the exit code (exit $RC, wanted 0)"
  else
    ok "advisory bound: a HANGING advisory still reaches exit 0 (the bound applied)"
  fi
  # NON-VACUITY: the stub really ran, so "not FINISHED" below is a bound and not
  # a stub that never started.
  if [ -f "$MARK_A" ]; then
    ok "advisory bound: the hanging stub really executed (the case is not vacuous)"
  else
    bad "advisory bound: the hanging stub never ran — the bounded case proves nothing"
  fi
  case "$OUT" in
    *"PREMERGE: ADVISORY BASE-STALENESS: bound-shim requested 60 grace 5"*)
      ok "advisory bound: the 60s bound AND a 5s kill grace are what reach the runner" ;;
    *"bound-shim requested 60 grace none"*)
      bad "advisory bound: the runner was invoked WITHOUT --kill-after — a SIGTERM-only bound a child can ignore (#3650 R1)" ;;
    *) bad "advisory bound: the advisory must be invoked through \`<runner> --kill-after=<grace> <secs>\` (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"hanging stub FINISHED"*)
      bad "advisory bound: the advisory was AWAITED to completion — the bound did not apply" ;;
    *) ok "advisory bound: the hanging advisory was cut off before completing" ;;
  esac
  case "$OUT" in
    *"PREMERGE: ADVISORY"*"exit 124"*)
      ok "advisory bound: the timeout is REPORTED with its exit code, and is not fatal" ;;
    *) bad "advisory bound: a timed-out advisory must be reported as exit 124 (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: OK $CERTIFIED"*) ok "advisory bound: the verdict survives a timed-out advisory" ;;
    *) bad "advisory bound: the verdict must be unchanged by a timed-out advisory (got: $OUT)" ;;
  esac
fi

# Path 2 — `timeout` ABSENT (stock macOS). The advisory must NOT be executed
# unbounded; it must be reported unavailable, and the exit code must be
# untouched. `timeout` cannot be hidden by shadowing (a non-executable file is
# skipped by PATH lookup, which then finds the real one), so PATH is rebuilt from
# an explicit symlink set holding every tool the assert needs and NOT `timeout`.
# `sleep` IS included deliberately: with it present a regression genuinely hangs
# for the stub's 20s and is caught by the marker, rather than dying instantly for
# an unrelated reason and passing for the wrong one.
NOBIN="$T/no-timeout-bin"
mkdir -p "$NOBIN"
nobin_ok=1
for tool in bash awk tr basename dirname sleep; do
  tp=$(command -v "$tool" 2>/dev/null) || tp=""
  if [ -z "$tp" ]; then
    bad "advisory bound: cannot build the no-timeout PATH — \`$tool\` is not on PATH"
    nobin_ok=0
    continue
  fi
  ln -sf "$tp" "$NOBIN/$tool"
done
cp "$BIN/gh" "$NOBIN/gh"
chmod +x "$NOBIN/gh"
# Vacuity guard over BOTH candidate names: the script resolves `timeout` THEN
# `gtimeout` (#3650 review R2), so leaving `gtimeout` reachable would let it
# escape the constructed PATH and take the BOUNDED path instead.
for cand in timeout gtimeout; do
  if [ "$(PATH="$NOBIN" command -v "$cand" 2>/dev/null)" != "" ]; then
    bad "advisory bound: the no-timeout PATH still resolves \`$cand\` (the case would be vacuous)"
    nobin_ok=0
  fi
done

MARK_B="$T/hang-marker-unbounded"
rm -f "$MARK_B"
if [ "$nobin_ok" -eq 1 ] && flow_copy adv-hang-no-timeout "$(hang_stub "$MARK_B")"; then
  OUT=$(PATH="$NOBIN" bash "$COPY" 2421 "$CERTIFIED" "$GOOD" \
    --c-verdict "$C_PASS_FILE" 2>&1)
  RC=$?
  if [ "$RC" -ne 0 ]; then
    bad "advisory bound: an unavailable bound must not change the exit code (exit $RC, wanted 0)"
  else
    ok "advisory bound: no \`timeout\` on PATH still reaches exit 0"
  fi
  if [ -f "$MARK_B" ]; then
    bad "advisory bound: the advisory RAN UNBOUNDED with no \`timeout\` available (#3650 B1)"
  else
    ok "advisory bound: with no \`timeout\`, the advisory is NOT executed at all"
  fi
  case "$OUT" in
    *"PREMERGE: ADVISORY"*"NOT RUN"*"gtimeout"*)
      ok "advisory bound: the missing bound is NAMED on an ADVISORY line, naming BOTH candidates" ;;
    *) bad "advisory bound: the unavailable bound must be reported, naming what the code accepts (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"hanging stub REACHED"*)
      bad "advisory bound: the advisory's output appears — it was executed unbounded" ;;
    *) ok "advisory bound: no advisory report is produced when it cannot be bounded" ;;
  esac
  case "$OUT" in
    *"PREMERGE: OK $CERTIFIED"*) ok "advisory bound: the verdict survives an unavailable bound" ;;
    *) bad "advisory bound: the verdict must be unchanged (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: SCOPE"*"#3650"*) ok "advisory bound: the SCOPE lines survive an unavailable bound" ;;
    *) bad "advisory bound: the SCOPE lines must survive (got: $OUT)" ;;
  esac
fi

# Path 2b — a `gtimeout`-ONLY PATH: the macOS-with-coreutils shape (#3650 review
# R2). GNU coreutils installs its timeout as `gtimeout`, and the code used to
# resolve `timeout` alone — so on the EXACT configuration the skip diagnostic
# recommends, the advisory still skipped. This fleet is LINUX (D5's own
# measurement is git 2.43.0 on the lanes), so the rationale is NOT "the fleet is
# macOS": it is that the repo SUPPORTS stock macOS, which makes the
# gtimeout-only shape a supported configuration rather than a hypothetical.
# (Same reasoning, same shape, as
# scripts/tests/test_bootstrap_agent_machine.sh's gtimeout-only credential case.)
#
# `timeout` cannot be hidden by shadowing, so PATH is rebuilt from an explicit
# symlink set — the recording shim installed under the name `gtimeout` and NO
# plain `timeout` anywhere.
GTD="$T/gtimeout-only-bin"
mkdir -p "$GTD"
gtd_ok=1
for tool in bash awk tr basename dirname sleep; do
  tp=$(command -v "$tool" 2>/dev/null) || tp=""
  if [ -z "$tp" ]; then
    bad "advisory bound: cannot build the gtimeout-only PATH — \`$tool\` is not on PATH"
    gtd_ok=0
    continue
  fi
  ln -sf "$tp" "$GTD/$tool"
done
cp "$BIN/gh" "$GTD/gh"
chmod +x "$GTD/gh"
cp "$SHIMD/timeout" "$GTD/gtimeout"
chmod +x "$GTD/gtimeout"
if [ "$(PATH="$GTD" command -v timeout 2>/dev/null)" != "" ]; then
  bad "advisory bound: the gtimeout-only PATH still resolves plain \`timeout\` (the case would be vacuous)"
  gtd_ok=0
fi
MARK_G="$T/hang-marker-gtimeout"
rm -f "$MARK_G"
if [ "$gtd_ok" -eq 1 ] && flow_copy adv-hang-gtimeout "$(hang_stub "$MARK_G")"; then
  OUT=$(PATH="$GTD" bash "$COPY" 2421 "$CERTIFIED" "$GOOD" \
    --c-verdict "$C_PASS_FILE" 2>&1)
  RC=$?
  if [ "$RC" -ne 0 ]; then
    bad "advisory bound: a gtimeout-only host must not change the exit code (exit $RC, wanted 0)"
  else
    ok "advisory bound: a gtimeout-only (macOS-shaped) host still reaches exit 0"
  fi
  if [ -f "$MARK_G" ]; then
    ok "advisory bound: on a gtimeout-only host the advisory IS RUN (bounded), not skipped (#3650 R2)"
  else
    bad "advisory bound: the advisory was SKIPPED on a gtimeout-only host — the exact configuration the diagnostic recommends (#3650 R2)"
  fi
  case "$OUT" in
    *"bound-shim requested 60 grace 5"*)
      ok "advisory bound: \`gtimeout\` is invoked with the same bound and kill grace" ;;
    *) bad "advisory bound: the advisory must be bounded through \`gtimeout\` (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"NOT RUN"*)
      bad "advisory bound: a gtimeout-only host reported the bound UNAVAILABLE (got: $OUT)" ;;
    *) ok "advisory bound: no unavailable-bound report is printed when \`gtimeout\` exists" ;;
  esac
  case "$OUT" in
    *"PREMERGE: OK $CERTIFIED"*) ok "advisory bound: the verdict survives the gtimeout-only path" ;;
    *) bad "advisory bound: the verdict must be unchanged (got: $OUT)" ;;
  esac
fi

# Path 3 — a child that IGNORES SIGTERM (#3650 review R1). `timeout <secs>` sends
# SIGTERM and then WAITS, so a stub containing `trap '' TERM` outlives the bound
# entirely and the merge-critical path stays blocked: the advertised bound bounds
# nothing. `--kill-after=<grace>` follows with SIGKILL, which cannot be trapped.
#
# THE DELEGATING SHIM IS THE ONLY HONEST WAY TO TEST THIS. A hand-written shim
# would have to REIMPLEMENT the escalation, and would then be asserting its own
# signal handling rather than the script's invocation; so this shim delegates to
# the REAL runner with short values and reflects FAITHFULLY whether the script
# passed `--kill-after`. A regression that drops the flag therefore gets a real
# SIGTERM-only bound and is caught by the observable difference measured on this
# repo's own boxes: with the flag, rc=137 and the stub never reaches its FINISHED
# line; without it, rc=124 and FINISHED is printed after the stub's own sleep.
# Both assertions below are OBSERVABLE OUTPUT, never elapsed time — a wall-clock
# threshold in a correctness test is the #2642 defect class.
#
# THE CASE BOUNDS ITSELF: the stub sleeps 20s, not forever, so even a total
# regression of both the flag and the resolution ends the case rather than
# hanging the suite, and the sleep's stdout goes to /dev/null so a surviving
# grandchild cannot hold the command-substitution pipe open (the way a bound can
# "work" and the caller hang anyway).
term_ignoring_stub() {
  cat <<STUB
#!/usr/bin/env bash
trap '' TERM
printf 'BASE-STALENESS: TERM-ignoring stub REACHED\\n'
: >"$1"
sleep 20 >/dev/null 2>&1
printf 'BASE-STALENESS: TERM-ignoring stub FINISHED - the bound was ESCAPED\\n'
exit 0
STUB
}

# REAL_TO is resolved once near the top of the suite, by the same algorithm the
# script uses. This path needs a REAL runner (it delegates to it rather than
# reimplementing the escalation it is testing), so it skips where there is none.
if [ -z "$REAL_TO" ]; then
  echo "skip - advisory bound: the TERM-ignoring case needs a real timeout/gtimeout supporting --kill-after"
else
  KILLD="$T/killafter-shim-bin"
  mkdir -p "$KILLD"
  cat >"$KILLD/timeout" <<SHIM
#!/usr/bin/env bash
# Delegates to the REAL runner with short values, reflecting faithfully whether
# the caller asked for a kill grace. Never reimplements the escalation.
grace=none
case "\$1" in
  --kill-after=*) grace="\${1#--kill-after=}"; shift ;;
esac
shift   # the bound the script asked for; a short one is substituted below
printf 'BASE-STALENESS: killafter-shim grace %s\\n' "\$grace"
# Answer the script's capability probe (\`--kill-after=1 1 true\`) affirmatively,
# or it concludes there is no supported runner and skips the advisory.
if [ "\$#" -eq 1 ] && [ "\$1" = true ]; then exit 0; fi
if [ "\$grace" = none ]; then
  exec "$REAL_TO" 2 "\$@"
fi
exec "$REAL_TO" --kill-after=1 2 "\$@"
SHIM
  chmod +x "$KILLD/timeout"
  MARK_C="$T/hang-marker-term-ignoring"
  rm -f "$MARK_C"
  if flow_copy adv-term-ignoring "$(term_ignoring_stub "$MARK_C")"; then
    OUT=$(PATH="$KILLD:$BIN:$PATH" bash "$COPY" 2421 "$CERTIFIED" "$GOOD" \
    --c-verdict "$C_PASS_FILE" 2>&1)
    RC=$?
    if [ "$RC" -ne 0 ]; then
      bad "advisory bound: a TERM-ignoring advisory must not change the exit code (exit $RC, wanted 0)"
    else
      ok "advisory bound: a TERM-IGNORING advisory still reaches exit 0"
    fi
    if [ -f "$MARK_C" ]; then
      ok "advisory bound: the TERM-ignoring stub really executed (the case is not vacuous)"
    else
      bad "advisory bound: the TERM-ignoring stub never ran — the case proves nothing"
    fi
    case "$OUT" in
      *"killafter-shim grace 5"*)
        ok "advisory bound: the 5s kill grace reaches the runner on the TERM-ignoring path" ;;
      *"killafter-shim grace none"*)
        bad "advisory bound: no --kill-after was passed — a TERM-ignoring child escapes the bound (#3650 R1)" ;;
      *) bad "advisory bound: the runner was not invoked as expected (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"TERM-ignoring stub FINISHED"*)
        bad "advisory bound: the TERM-ignoring advisory ran to COMPLETION — the bound was escaped" ;;
      *) ok "advisory bound: the TERM-ignoring advisory was KILLED, not awaited" ;;
    esac
    case "$OUT" in
      *"PREMERGE: ADVISORY"*"exit 137"*)
        ok "advisory bound: the SIGKILL escalation is REPORTED (exit 137), and is not fatal" ;;
      *"exit 124"*)
        bad "advisory bound: exit 124 means TERM-only — the child was awaited, not killed (got: $OUT)" ;;
      *) bad "advisory bound: a killed advisory must be reported with its exit code (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"PREMERGE: OK $CERTIFIED"*) ok "advisory bound: the verdict survives a TERM-ignoring advisory" ;;
      *) bad "advisory bound: the verdict must be unchanged (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"PREMERGE: SCOPE"*"#3650"*) ok "advisory bound: the SCOPE lines survive a TERM-ignoring advisory" ;;
      *) bad "advisory bound: the SCOPE lines must survive (got: $OUT)" ;;
    esac
  fi
fi

# A REFUSAL is unaffected: the advisory runs only on the success path, so a
# refusing invocation carries no ADVISORY lines and still exits 2.
if flow_copy adv-refuse "$STALE_ADV" &&
  run_copy 2 "a refusal is unaffected by the advisory" "$COPY" 2421 "$CERTIFIED" "$T/lite-only.txt"; then
  case "$OUT" in
    *"PREMERGE: ADVISORY"*) bad "advisory: a refusal must not carry ADVISORY lines (got: $OUT)" ;;
    *) ok "advisory: a refusal path prints no ADVISORY lines and still exits 2" ;;
  esac
fi

# --- Case 41c: THE WIRING CASE — shipped artifacts, SYNTHETIC repository -----
#
# NON-VACUITY for the wiring itself: every stub case above would pass if the real
# script invoked some OTHER path. This is the ONE case whose subject is "the
# shipped premerge-assert really runs scripts/flow/base-staleness.sh", so it is
# the ONE case that runs the shipped artifacts.
#
# IT NO LONGER READS THE AMBIENT CHECKOUT (#3650 review R5 F2). It used to run
# with the suite's own cwd, so the shipped advisory scanned the surrounding
# repository — repository-dependent, ambient-staleness-dependent runtime, and on a
# stock macOS with the runner shim's discard branch it ran UNBOUNDED. It now runs
# against a 3-commit SYNTHETIC repository built right here, which is bounded by
# CONSTRUCTION rather than by a runner and is identical on every host.
#
# THAT MAKES THE CASE STRONGER, NOT WEAKER. The ambient version could only pin the
# advisory's PREFIX — a measured line would have red on a correct run, since the
# lane's own base staleness is not a property of this suite. A synthetic
# fixture's staleness IS, so this pins MEASURED values: the fixture is stale by
# exactly one commit, which stales via the gate-global `.config/nextest.toml`
# while the diff touches only a src path. A stub could not produce those numbers
# from this fixture by accident.
#
# THE CERTIFIED SHA IS THE FIXTURE'S OWN HEAD, which is what lets the advisory
# resolve its subject rev and actually measure: premerge-assert passes the
# CERTIFIED sha to the advisory (#3650 F1), not the local HEAD, so a fabricated
# sha would make the advisory report UNMEASURED and the numbers would be
# unassertable. The gate summary fixture therefore carries this sha's own two
# abbreviations, the same two widths the gate writes.
#
# A supported runner is still guaranteed (the $BIN shim), so the expectation
# below is the only correct one on every host — Case 41b Path 2 owns the
# no-runner configuration and asserts the documented skip.
WIRE_REPO="$T/wiring-repo"
wire_shape=0
mkdir -p "$WIRE_REPO"
if git init -q -b mainline "$WIRE_REPO" >/dev/null 2>&1; then
  git -C "$WIRE_REPO" config user.email t@t
  git -C "$WIRE_REPO" config user.name t
  wire_commit() {
    local msg="$1" path="$2"
    mkdir -p "$WIRE_REPO/$(dirname "$path")"
    printf 'content for %s at %s\n' "$path" "$msg" >>"$WIRE_REPO/$path"
    git -C "$WIRE_REPO" add -- "$path" >/dev/null &&
      git -C "$WIRE_REPO" commit -q -m "$msg" >/dev/null
  }
  wire_commit "c0 initial" "README.md" &&
    git -C "$WIRE_REPO" checkout -q -b feature &&
    wire_commit "the PR: a src path only" "cqlite-core/src/storage/sstable/mod.rs" &&
    git -C "$WIRE_REPO" checkout -q mainline &&
    wire_commit "behind: a gate-global commit" ".config/nextest.toml" &&
    git -C "$WIRE_REPO" update-ref refs/remotes/origin/main mainline &&
    git -C "$WIRE_REPO" checkout -q feature &&
    wire_shape=1
fi
WIRE_SHA=""
if [ "$wire_shape" -eq 1 ]; then
  WIRE_SHA=$(git -C "$WIRE_REPO" rev-parse HEAD 2>/dev/null) || WIRE_SHA=""
fi
# FIXTURE SELF-CONSISTENCY: the sha must be a real 40-hex, and the fixture must
# genuinely be behind by exactly one commit that touches a gate-global path — or
# the measured expectations below would be asserting the wrong thing.
wire_behind=""
if [ -n "$WIRE_SHA" ]; then
  wire_behind=$(git -C "$WIRE_REPO" rev-list --count \
    "$(git -C "$WIRE_REPO" merge-base refs/remotes/origin/main HEAD)..refs/remotes/origin/main" \
    2>/dev/null) || wire_behind=""
fi
case "$WIRE_SHA" in
  [0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]) ;;
  *) wire_shape=0 ;;
esac
[ "$wire_behind" = 1 ] || wire_shape=0
if [ "$wire_shape" -eq 1 ]; then
  ok "wiring fixture: the synthetic repo is behind by exactly 1 gate-global commit at a real 40-hex HEAD"
else
  bad "wiring fixture: the synthetic repo does not have the shape the case claims (sha '$WIRE_SHA', behind '$wire_behind') — the case would be vacuous"
fi

if [ "$wire_shape" -eq 1 ]; then
  WIREGOOD="$T/wiring-full-pass.txt"
  emit_summary_block "$FULL_S" "$FULL_E" "-" \
    "$(printf '%.7s' "$WIRE_SHA")" "$(printf '%.12s' "$WIRE_SHA")" PASS PASS >"$WIREGOOD"
  WIRE_OUT=$(cd "$WIRE_REPO" &&
    PATH="$BIN:$PATH" MOCK_GH_OUT="$WIRE_SHA OPEN" MOCK_GH_FAIL=0 \
    bash "$ASSERT" 2421 "$WIRE_SHA" "$WIREGOOD" --c-verdict "$C_PASS_FILE" 2>&1)
  WIRE_RC=$?
  if [ "$WIRE_RC" -ne 0 ]; then
    bad "the SHIPPED script invokes the SHIPPED advisory (exit $WIRE_RC, wanted 0)"
    printf '     output: %s\n' "$WIRE_OUT"
  else
    ok "the SHIPPED script reaches PREMERGE: OK against the synthetic repository"
    OUT="$WIRE_OUT"
    case "$OUT" in
      *"PREMERGE: ADVISORY BASE-STALENESS:"*)
        ok "advisory: the shipped premerge-assert really runs scripts/flow/base-staleness.sh" ;;
      *) bad "advisory: the shipped script must invoke the shipped advisory (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"PREMERGE: ADVISORY"*"NON-EXHAUSTIVE"*)
        ok "advisory: the advisory's own non-exhaustiveness travels to the merge point" ;;
      *) bad "advisory: the NON-EXHAUSTIVE lines must travel with the report (got: $OUT)" ;;
    esac
    # THE MEASURED HALF — unassertable while this case read the ambient checkout.
    case "$OUT" in
      *"PREMERGE: ADVISORY BASE-STALENESS: behind 1 commits"*)
        ok "advisory: the shipped advisory really MEASURED the synthetic fixture (behind 1)" ;;
      *) bad "advisory: the shipped advisory must report the fixture's own behind count (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"PREMERGE: ADVISORY BASE-STALENESS: blast-radius 1 RECOGNISED of 1 commits behind"*)
        ok "advisory: the gate-global half of the blast radius fired on the shipped path" ;;
      *) bad "advisory: the shipped advisory must report blast-radius 1 RECOGNISED (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"PREMERGE: ADVISORY BASE-STALENESS: verdict STALE-RECOGNISED"*)
        ok "advisory: a STALE-RECOGNISED shipped advisory still reaches PREMERGE: OK" ;;
      *) bad "advisory: the shipped advisory's verdict must be STALE-RECOGNISED here (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"PREMERGE: ADVISORY"*"exit 4"*)
        ok "advisory: the shipped advisory's exit 4 is reported and is not fatal" ;;
      *) bad "advisory: exit 4 must be reported on an ADVISORY line (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"NOT RUN"*)
        bad "advisory: a runner IS on PATH here, so the bound must not be reported unavailable (got: $OUT)" ;;
      *) ok "advisory: with a supported runner on PATH the advisory is not reported unavailable" ;;
    esac
    case "$OUT" in
      *"PREMERGE: SCOPE"*"#3650"*)
        ok "advisory: the SCOPE lines and the literal #3650 print on the shipped path" ;;
      *) bad "advisory: the SCOPE lines must print on the shipped path (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"PREMERGE: OK $WIRE_SHA"*)
        ok "advisory: the verdict names the certified sha on the shipped path" ;;
      *) bad "advisory: PREMERGE: OK must name the certified sha (got: $OUT)" ;;
    esac
  fi
fi


# --- Case 41d: the AMBIENT SCAN CANNOT COME BACK (#3650 review R5 F2) --------
#
# Two assertions, because the F2 defect was INVISIBLE: every case was green while
# 13 of them scanned the surrounding repository, and nothing in the suite said so.
#
# (1) BEHAVIOURAL. An ordinary success case must carry the NEUTRAL stub's own line
#     and must NOT carry the shipped advisory's `NON-EXHAUSTIVE` block — which
#     every real run of base-staleness.sh prints, measured or unmeasured. So its
#     absence is direct evidence that no scan of any repository happened.
# (2) STRUCTURAL. Exactly ONE invocation of the shipped artifact may exist in this
#     file: the wiring case. A behavioural check can only speak for the cases
#     someone remembered to check; a new case added later would slip past it. The
#     needle is SPLIT so this guard cannot match its own line.
if run 0 "an ordinary success case runs the NEUTRAL advisory, not the shipped one" \
  2421 "$CERTIFIED" "$GOOD"; then
  case "$OUT" in
    *"neutral immediate stub"*)
      ok "no-ambient-scan: an ordinary case really reaches the neutral immediate stub" ;;
    *) bad "no-ambient-scan: the neutral stub did not run — the case proves nothing (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"NON-EXHAUSTIVE"*)
      bad "no-ambient-scan: the SHIPPED advisory ran — an ordinary case is scanning a repository again (#3650 R5 F2)" ;;
    *) ok "no-ambient-scan: no shipped-advisory report appears, so nothing scanned a repository" ;;
  esac
fi
SUITE_SELF="${BASH_SOURCE[0]}"
# Assembled from two halves so this guard cannot match its own line.
_shipped_a='bash "$AS'
_shipped_b='SERT"'
SHIPPED_NEEDLE="$_shipped_a$_shipped_b"
if [ -r "$SUITE_SELF" ]; then
  shipped_calls=$(grep -c -F -- "$SHIPPED_NEEDLE" "$SUITE_SELF" | tr -d ' ')
  if [ "$shipped_calls" = 1 ]; then
    ok "no-ambient-scan: exactly ONE invocation of the shipped assert exists (the wiring case)"
  else
    bad "no-ambient-scan: $shipped_calls invocations of the shipped assert — only the wiring case may run it against a synthetic repo (#3650 R5 F2)"
  fi
else
  bad "no-ambient-scan: could not read this suite's own source ($SUITE_SELF) to check the invocation count"
fi

# --- Case 40: the three exit-3 causes are DISTINGUISHABLE (nit 8) ------------
# Exit 3 covers a usage error, a tool failure and a gh failure. The CODES are
# unchanged (documented), so the caller tells them apart by the MARKER — a
# forgotten third argument must never be escalated upward as a GitHub outage.
if run 3 "usage error prints PREMERGE: USAGE" 2421 "$CERTIFIED"; then
  case "$OUT" in
    *"PREMERGE: USAGE"*) ok "exit 3: a usage error carries the PREMERGE: USAGE marker" ;;
    *) bad "exit 3: usage error must carry PREMERGE: USAGE (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"GH-FAILURE"*|*"TOOL-FAILURE"*)
      bad "exit 3: a usage error must not read as a gh/tool failure (got: $OUT)" ;;
    *) ok "exit 3: a usage error is NOT reported as a gh failure" ;;
  esac
fi
export MOCK_GH_FAIL=1
if run 3 "gh failure prints PREMERGE: GH-FAILURE only" 2421 "$CERTIFIED" "$GOOD"; then
  case "$OUT" in
    *"PREMERGE: USAGE"*) bad "exit 3: a gh failure must not read as a usage error (got: $OUT)" ;;
    *) ok "exit 3: a gh failure is NOT reported as a usage error" ;;
  esac
fi
export MOCK_GH_FAIL=0

# --- Case 43: the advisory is MEASURED BEFORE the head/state check -----------
# REGRESSION GUARD (roborev job 250, Medium -> promoted to blocker). The advisory
# is bounded at 60s + a 5s kill grace. When it ran AFTER the final `gh pr view`
# head/state check, up to 65s separated "the head equals the certified sha" from
# `PREMERGE: OK` -- and the caller's very next action is `gh pr merge --auto`,
# which merges whatever head is current when `required` goes green. So a push in
# that window turned a stale OK into a merge of an UNCERTIFIED head: the escape
# #2456/#3465 exist to refuse, reintroduced by the advisory call site itself.
#
# The property asserted is ORDER, not duration: a duration assert here would be a
# wall-clock race in the correctness path (the very thing roborev-lints forbids).
# Both the advisory and `gh` append a token to one log; the advisory's must come
# FIRST. That is observable, deterministic, and fails against the old ordering.
ORDER_LOG="$T/order-43.log"
ORDBIN="$T/bin-order-43"
ORDFLOW="$T/flow-order-43"
mkdir -p "$ORDBIN" "$ORDFLOW"
# gh stub that RECORDS its invocation, then behaves like the standard mock.
cat >"$ORDBIN/gh" <<'ORDGH'
#!/usr/bin/env bash
printf 'GH\n' >>"$PREMERGE_ORDER_LOG"
if [ "${MOCK_GH_FAIL:-0}" = "1" ]; then echo "gh: could not connect" >&2; exit 1; fi
printf '%s\n' "${MOCK_GH_OUT:-}"
exit 0
ORDGH
chmod +x "$ORDBIN/gh"
if ! cp "$ASSERT" "$ORDFLOW/premerge-assert.sh"; then
  bad "order: could not build the scratch copy of premerge-assert.sh"
else
  cat >"$ORDFLOW/base-staleness.sh" <<'ORDADV'
#!/usr/bin/env bash
printf 'ADV\n' >>"$PREMERGE_ORDER_LOG"
printf "BASE-STALENESS: ordering-case stub\n"
printf "BASE-STALENESS: verdict NO-STALENESS-RECOGNISED\n"
exit 0
ORDADV
  chmod +x "$ORDFLOW/base-staleness.sh"
  : >"$ORDER_LOG"
  ORDOUT=$(PATH="$ORDBIN:$BIN:$PATH" PREMERGE_ORDER_LOG="$ORDER_LOG" \
    MOCK_GH_OUT="$CERTIFIED OPEN" MOCK_GH_FAIL=0 \
    bash "$ORDFLOW/premerge-assert.sh" 2421 "$CERTIFIED" "$GOOD" \
    --c-verdict "$C_PASS_FILE" 2>&1)
  ORDRC=$?
  if [ "$ORDRC" -ne 0 ]; then
    bad "order: the success-path case did not exit 0 (rc=$ORDRC, got: $ORDOUT)"
  else
    ok "order: the ordering case runs the success path"
    # Non-vacuity: BOTH tokens must be present, or an absent stub would "pass".
    if ! grep -qx 'ADV' "$ORDER_LOG" || ! grep -qx 'GH' "$ORDER_LOG"; then
      bad "order: NON-VACUITY -- expected both ADV and GH to be recorded (got: $(tr '\n' ',' <"$ORDER_LOG"))"
    else
      ok "order: non-vacuity -- both the advisory and gh were actually invoked"
      if [ "$(head -n 1 "$ORDER_LOG")" = "ADV" ]; then
        ok "order: the advisory is MEASURED BEFORE the gh head/state check (job 250)"
      else
        bad "order: the advisory ran AFTER the gh head check -- up to 65s of staleness sits between the head check and PREMERGE: OK (got: $(tr '\n' ',' <"$ORDER_LOG"))"
      fi
    fi
    # The report must still be printed BELOW the SCOPE clause that promises it.
    case "$ORDOUT" in
      *"ADVISORY lines below measure that gap"*"BASE-STALENESS: ordering-case stub"*)
        ok "order: the report is still PRINTED below the SCOPE clause that promises it" ;;
      *) bad "order: capturing the advisory must not move its output (got: $ORDOUT)" ;;
    esac
  fi
fi


# =============================================================================
# Case 44 — THE C (INTENT AUDIT) VERDICT AT THE MERGE POINT (#3751)
# =============================================================================
# The consumer half of `scripts/flow/review-stage.sh`. A delegated review stage
# used to write NOTHING, so its reader had only ABSENCE to reason from. Every
# measured instance was recorded as not-run by its own lane and nothing REQUIRED
# it; no false certification has occurred yet. These cases pin that
# an absent C cannot reach a merge, that routing is MEASURED rather than asserted
# by the caller, and that the disclosed substitute keeps its own token.
#
# THE END-TO-END WIRES LIVE HERE, NOT IN test_review_stage.sh, ON PURPOSE. The
# spec asks for two paired assertions — "premerge-assert REFUSES on a
# sentinel-only stage" (AC1) and "the merge assert PROCEEDS on a real PASS" (the
# positive control) — and both need a gh mock, a gate-of-record summary fixture
# and the neutral scratch copy of the assert. All three live in THIS file;
# test_review_stage.sh has none of them and would have to grow a second copy of
# each, which is a second implementation of a fixture and one more thing to drift.
# What test_review_stage.sh owns is the PRODUCER's grammar; what this file owns is
# what the merge point DOES with it.

# c_verdict_file <name> <line...> — a captured-verdict fixture. Written with
# printf so a case can plant a shape review-stage.sh would never emit (an indented
# copy, two lines, an invented token) — which is the point: this parser must be
# correct about text it did not produce.
c_verdict_file() {
  local f="$T/cv-$1.txt"
  shift
  : >"$f"
  while [ "$#" -gt 0 ]; do
    printf '%s\n' "$1" >>"$f"
    shift
  done
  printf '%s\n' "$f"
}

cv_line() {
  printf 'REVIEW-STAGE: c RESULT: %s elapsed=7 deadline=1800 agent=spec-auditor report=%s\n' \
    "$1" "$T/planted-c-report.md"
}

# c_refused <desc> <c-verdict-arg> <needle> — the C refusal is exit 2 with the
# NO-C-VERDICT verdict; the needle pins the CAUSE, so a case cannot pass by
# refusing for the wrong reason (the same contract as `refused` above).
c_refused() {
  local desc="$1" arg="$2" needle="$3"
  if run 2 "$desc" 2421 "$CERTIFIED" "$GOOD" --c-verdict "$arg"; then
    case "$OUT" in
      *"PREMERGE: NO-C-VERDICT"*) ;;
      *) bad "$desc: missing NO-C-VERDICT verdict (got: $OUT)"; return 1 ;;
    esac
    if [ "${OUT#*"$needle"}" = "$OUT" ]; then
      bad "$desc: refusal does not name the cause '$needle' (got: $OUT)"
      return 1
    fi
    ok "$desc"
  fi
}

# --- 44a: the flag is REQUIRED (item 2.1) ------------------------------------
# NO_C_INJECT is the ONE place the suite's convenience injection is disabled: if
# it leaked here this case would pass for the wrong reason, so it is also asserted
# that the message NAMES the flag rather than merely exiting 3.
if NO_C_INJECT=1 run 3 "--c-verdict omitted entirely -> exit 3 (usage), never a silent skip" \
  2421 "$CERTIFIED" "$GOOD"; then
  case "$OUT" in
    *"PREMERGE: USAGE"*"MISSING REQUIRED FLAG(S): --c-verdict"*)
      ok "omission: the usage failure NAMES the missing flag (#3465's break-loudly precedent)" ;;
    *) bad "omission: exit 3 must name --c-verdict, not merely fail (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"reproduce, inside the enforcer, the exact defect"*)
      ok "omission: the message says WHY there is no default" ;;
    *) bad "omission: the refusal must explain why 'not required' is not a default (got: $OUT)" ;;
  esac
fi

if NO_C_INJECT=1 run 3 "--c-verdict with NO value -> exit 3 (usage)" \
  2421 "$CERTIFIED" "$GOOD" --c-verdict; then
  case "$OUT" in
    *"--c-verdict requires a value"*) ok "no value: the usage failure names the missing value" ;;
    *) bad "no value: must name the missing value (got: $OUT)" ;;
  esac
fi

if run 3 "--c-verdict with an EMPTY value -> exit 3 (never AUTO, never skip)" \
  2421 "$CERTIFIED" "$GOOD" --c-verdict ""; then
  case "$OUT" in
    *"EMPTY value"*) ok "empty value: named as a caller bug, not defaulted" ;;
    *) bad "empty value: must be named (got: $OUT)" ;;
  esac
fi

if run 3 "an unknown option -> exit 3 (usage), not silently ignored" \
  2421 "$CERTIFIED" "$GOOD" --c-verdicts AUTO; then
  case "$OUT" in
    *"unknown option '--c-verdicts'"*) ok "unknown option: named verbatim" ;;
    *) bad "unknown option: must be named (got: $OUT)" ;;
  esac
fi

# The `--flag=value` spelling is accepted, because a caller that writes it must not
# get "unknown option" for a flag that exists.
if run 0 "--c-verdict=<path> (equals form) -> accepted" \
  2421 "$CERTIFIED" "$GOOD" "--c-verdict=$C_PASS_FILE"; then
  case "$OUT" in
    *"PREMERGE: C-VERDICT PASS"*) ok "equals form: parsed as the same value" ;;
    *) bad "equals form: must reach the same verdict (got: $OUT)" ;;
  esac
fi

# --- 44b: an explicit verdict path is held to the closed grammar -------------
c_refused "C verdict path ABSENT -> refuse" "$T/no-such-c-verdict.txt" "does not exist"
: >"$T/cv-empty-file.txt"
c_refused "C verdict path EMPTY -> refuse (the shape a redirect leaves when nothing ran)" \
  "$T/cv-empty-file.txt" "EMPTY"
c_refused "C verdict path with no verdict line -> refuse" \
  "$(c_verdict_file noline 'some prose about a review that happened, allegedly')" \
  "holds NO verdict line"
# COLUMN-ZERO ANCHORING (#3312). An indented or quoted copy of a verdict line is
# DATA — this repository's docs and PR comments are full of such copies — so it
# must NOT satisfy the check. awk's `$1 ==` would have accepted it.
c_refused "an INDENTED copy of a verdict line -> counted as ZERO (column-zero anchored)" \
  "$(c_verdict_file indented "  $(cv_line PASS)" '> more quoted prose')" \
  "holds NO verdict line"
c_refused "TWO verdict lines -> refuse as AMBIGUOUS, never last-wins" \
  "$(c_verdict_file two "$(cv_line NOT-RUN)" "$(cv_line PASS)")" \
  "2 verdict lines"
c_refused "verdict NOT-RUN -> refuse, naming the stage and that no verdict was recorded" \
  "$(c_verdict_file notrun "$(cv_line 'NOT-RUN (no report written)')")" \
  "reports NOT-RUN"
c_refused "verdict FINDINGS -> refuse (a blocking intent-audit gap)" \
  "$(c_verdict_file findings "$(cv_line FINDINGS)")" \
  "reports FINDINGS"
# THE ESCAPE HATCH THIS ISSUE EXISTS TO REMOVE: a caller cannot spell
# "not applicable" into a verdict. Inapplicability is reachable ONLY from AUTO's
# measurement of the branch.
c_refused "a hand-written NOT-APPLICABLE token -> refuse (no caller-supplied exemption)" \
  "$(c_verdict_file na "$(cv_line NOT-APPLICABLE)")" \
  "not in the closed set"
c_refused "PASS-BUT-UNMEASURED -> refuse (token-exact, never a PASS* prefix, #3544)" \
  "$(c_verdict_file pbu "$(cv_line PASS-BUT-UNMEASURED)")" \
  "not in the closed set"
c_refused "an empty token after RESULT: -> refuse" \
  "$(c_verdict_file notok 'REVIEW-STAGE: c RESULT:')" \
  "NO token after"

if run 0 "verdict PASS from an explicit path -> the merge assert PROCEEDS" \
  2421 "$CERTIFIED" "$GOOD" --c-verdict "$C_PASS_FILE"; then
  case "$OUT" in
    *"PREMERGE: C-VERDICT PASS stage: c source: file"*)
      ok "PASS: reported on its own PREMERGE: C-VERDICT line, naming its source" ;;
    *) bad "PASS: the C verdict must be reported on the success path (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"report: $T/injected-c-report.md"*)
      ok "PASS: the line names WHICH stage report answered (the declared residual's mitigation)" ;;
    *) bad "PASS: the report path must travel to the success line (got: $OUT)" ;;
  esac
fi

# --- 44b(iii): THE EMIT BOUNDARY — a verdict this script did not produce cannot
#               carry control characters into its output (round 5, J3) ---------
# The verdict line, its token and its `report=` field come from a file a DELEGATED AGENT wrote, and
# they are printed verbatim into the refusal block and (on the success path) into a line that gets
# pasted into a PR comment. `_c_verdict_awk` strips ANSI CSI sequences as PARSE hygiene (#3400),
# which is neither complete — a bare ESC, BEL, backspace, VT/FF, DEL or an OSC `ESC ] … BEL`
# sequence survives it — nor a display guarantee. Asserted as a BYTE CENSUS over the whole output,
# for the same reason as in test_review_stage.sh: a case that checked one byte would pass on a fix
# that handled that byte alone.
CV_CTRL_CAUSE="$(printf 'NOT-RUN (ran out \033[2Jof \007context \010here \177and \013more)')"
if run 2 "controls: a control-bearing verdict cause still REFUSES" \
  2421 "$CERTIFIED" "$GOOD" --c-verdict "$(c_verdict_file ctrl "$(cv_line "$CV_CTRL_CAUSE")")"; then
  CV_LEFT="$(printf '%s' "$OUT" | LC_ALL=C tr -dc '\001-\010\013\014\016-\037\177' | LC_ALL=C wc -c | LC_ALL=C tr -d ' ')"
  if [ "$CV_LEFT" = "0" ]; then
    ok "controls: NO C0 or DEL byte reaches the refusal block (ESC, BEL, backspace, VT and DEL all planted)"
  else
    bad "controls: $CV_LEFT control byte(s) reached the refusal block"
  fi
  # BOTH ENDS of the planted cause, so a fix that TRUNCATED at the first control byte fails here.
  # Note the tail reads `and  more`: the VT became a space and this boundary deliberately does NOT
  # squeeze runs (the refusal block's own detail lines are aligned with leading spaces), so the
  # assertion matches the two words separately rather than pinning a space count.
  case "$OUT" in
    *"ran out"*) ok "controls: the readable prose BEFORE the first control byte is preserved" ;;
    *) bad "controls: the cause was dropped rather than neutralised (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"more)"*) ok "controls: and the prose AFTER the last control byte too — not truncated at the first one" ;;
    *) bad "controls: the cause was truncated at a control byte (got: $OUT)" ;;
  esac
fi
# THE SUCCESS PATH TOO: a PASS line is the one that gets pasted into a PR comment, and its
# `report=` field is equally agent-written. Neutralised there as well, and the token has already
# been compared by string equality by then, so this is display-only.
if run 0 "controls CONTROL: a PASS whose report= carries a control byte still PROCEEDS" \
  2421 "$CERTIFIED" "$GOOD" --c-verdict "$(c_verdict_file ctrlpass \
    "$(printf 'REVIEW-STAGE: c RESULT: PASS elapsed=7 deadline=1800 agent=spec-auditor report=%s/plan\007ted.md' "$T")")"; then
  CV_LEFT2="$(printf '%s' "$OUT" | LC_ALL=C tr -dc '\001-\010\013\014\016-\037\177' | LC_ALL=C wc -c | LC_ALL=C tr -d ' ')"
  if [ "$CV_LEFT2" = "0" ]; then
    ok "controls: no control byte reaches the PREMERGE: C-VERDICT success line either"
  else
    bad "controls: $CV_LEFT2 control byte(s) reached the success line"
  fi
  case "$OUT" in
    *"PREMERGE: C-VERDICT PASS stage: c source: file"*)
      ok "controls CONTROL: the neutralisation did not change the verdict — a display boundary decides nothing" ;;
    *) bad "controls CONTROL: the PASS must still be reported (got: $OUT)" ;;
  esac
fi

# --- 44b(iv): THE `--c-verdict` FILENAME reaches the SUCCESS block too (round 7, L1) ---
# `C_SOURCE` is the THIRD site of round 5's J3 class to be found ONE AT A TIME: it renders
# `file <the --c-verdict argument>` on the `PREMERGE: C-VERDICT` line of a SUCCESSFUL run — the
# one block a human reads as the merge verdict and pastes into a PR comment. A filename carrying
# a NEWLINE therefore emitted a second line with no `PREMERGE: ` prefix, and a filename shaped
# like `…\nPREMERGE: OK <sha>` forged a verdict line in that block. Two properties are asserted
# here, and the second is the one a byte census alone would miss: the OUTPUT MUST STILL CARRY
# EXACTLY ONE column-zero `PREMERGE: OK ` line.
#
# A filename is not only invoker text, which is why this is a defect and not a recorded residual:
# on the AUTO path the same field carries `C_ROUTING_DETAIL`, which interpolates an
# `openspec/changes/<slug>` path measured out of the certified tree — and git PERMITS NEWLINES IN
# PATHS. The filename is simply the reachable half to test hermetically.
# `$'\n'` AND NOT `$(printf '\n')`: command substitution STRIPS trailing newlines, so the
# obvious spelling builds a name with NO newline in it and the case passes having planted
# nothing — measured while writing this case, which is why the name is CHECKED below before it
# is used. A harness that never reached the code is this repo's own recurring test defect.
CV_FORGE_NAME="$T/cv-forge"$'\n'"PREMERGE: OK 0000000000000000000000000000000000000000"$'\007'"x.txt"
printf '%s\n' "$(cv_line PASS)" >"$CV_FORGE_NAME" 2>/dev/null || true
case "$CV_FORGE_NAME" in *$'\n'*) ;; *) CV_FORGE_NAME="" ;; esac
if [ -z "$CV_FORGE_NAME" ] || [ ! -f "$CV_FORGE_NAME" ]; then
  # A name this filesystem cannot hold, or a name that lost its newline, cannot host the case.
  # REPORTED, never skipped silently — three `bad`s, matching the three assertions the case owes,
  # so the tally cannot quietly shrink (the case-floor lesson, #3544).
  bad "forge: no control-bearing --c-verdict filename could be planted under $T — the L1 case did NOT run (1/3)"
  bad "forge: the same absence (2/3)"
  bad "forge: the same absence (3/3)"
else
  if run 0 "forge: a control-bearing --c-verdict FILENAME still PROCEEDS" \
    2421 "$CERTIFIED" "$GOOD" --c-verdict "$CV_FORGE_NAME"; then
    CV_FLEFT="$(printf '%s' "$OUT" | LC_ALL=C tr -dc '\001-\010\013\014\016-\037\177' | LC_ALL=C wc -c | LC_ALL=C tr -d ' ')"
    if [ "$CV_FLEFT" = "0" ]; then
      ok "forge: no C0 or DEL byte from the FILENAME reaches the success block"
    else
      bad "forge: $CV_FLEFT control byte(s) from the --c-verdict filename reached the success block"
    fi
    # THE FORGERY ASSERTION. `grep -c` on a column-zero anchor, because that is what a reader
    # (and premerge-assert's own consumers) match on. Two would mean the filename produced one.
    CV_FOK="$(printf '%s\n' "$OUT" | LC_ALL=C grep -c '^PREMERGE: OK ' || true)"
    if [ "$CV_FOK" = "1" ]; then
      ok "forge: EXACTLY ONE column-zero 'PREMERGE: OK ' line survives — the filename forged none"
    else
      bad "forge: $CV_FOK column-zero 'PREMERGE: OK ' lines — a filename forged a verdict line"
    fi
    case "$OUT" in
      *"PREMERGE: C-VERDICT PASS stage: c source: file "*)
        ok "forge CONTROL: the verdict is still reported, and the source is still named" ;;
      *) bad "forge CONTROL: the C-VERDICT line lost its source (got: $OUT)" ;;
    esac
  fi
fi

# --- 44b(ii): the FULL grammar is validated, kind included (F2) --------------
# A parser that accepted any column-zero `REVIEW-STAGE: ` line containing
# `RESULT: PASS` would let a SIBLING stage certify C: this very branch's diff
# produced a `code-review` stage whose PASS line satisfied `--c-verdict`. So the
# whole documented line is validated —
#   REVIEW-STAGE: <kind> RESULT: <token> elapsed=<n> deadline=<n> agent=<t> report=<abs>
# — with the kind compared by STRING EQUALITY against the expected stage and each
# mandatory key required EXACTLY ONCE.
#
# THE ACCEPTED SHAPE IS DERIVED FROM A REAL EMITTED LINE, NOT FROM THIS FILE'S
# IDEA OF ONE. The pin below runs the SHIPPED review-stage.sh and captures what it
# actually prints, so the parser cannot drift from the emitter: a change to the
# emitted line reds this case instead of silently making every verdict
# ungrammatical.
C_REAL_REPO="$T/c-real-line"
C_REAL_FILE="$T/c-real-verdict.txt"
C_REAL_OK=""
mkdir -p "$C_REAL_REPO"
if git init -q "$C_REAL_REPO" >/dev/null 2>&1 &&
  printf '.review-stage/\n' >"$C_REAL_REPO/.gitignore" &&
  (cd "$C_REAL_REPO" && bash "$SCRIPT_DIR/../flow/review-stage.sh" open c --issue 3751 \
    --agent spec-auditor >/dev/null 2>&1) &&
  printf 'result: PASS\n\n## Findings\n\nnone.\n' \
    >"$(SR_REPORT "$C_REAL_REPO" 3751 c)" &&
  (cd "$C_REAL_REPO" && bash "$SCRIPT_DIR/../flow/review-stage.sh" verdict c --issue 3751 \
    >"$C_REAL_FILE" 2>/dev/null) &&
  [ -s "$C_REAL_FILE" ]; then
  C_REAL_OK=1
  ok "real line: captured a verdict line from the SHIPPED review-stage.sh"
else
  bad "real line: could not capture a real verdict line — the grammar pin would be vacuous"
fi
if [ -n "$C_REAL_OK" ]; then
  # PINNED VERBATIM. If review-stage.sh's emitted line changes shape, this equality
  # fails HERE, in one named place, rather than turning every real verdict into
  # `ungrammatical` at a merge point.
  C_REAL_LINE=$(cat "$C_REAL_FILE")
  case "$C_REAL_LINE" in
    "REVIEW-STAGE: c RESULT: PASS elapsed="*" deadline="*" agent=spec-auditor report="*"/.review-stage/issue-3751/c."*".md")
      ok "real line: the emitted shape is <kind> RESULT: <token> elapsed= deadline= agent= report=" ;;
    *) bad "real line: the emitter's shape moved — update the parser WITH it (got: $C_REAL_LINE)" ;;
  esac
  if run 0 "real line: a verdict captured from the real emitter is ACCEPTED" \
    2421 "$CERTIFIED" "$GOOD" --c-verdict "$C_REAL_FILE"; then
    case "$OUT" in
      *"PREMERGE: C-VERDICT PASS"*) ok "real line: reaches C-VERDICT PASS" ;;
      *) bad "real line: a real emitted PASS must be accepted (got: $OUT)" ;;
    esac
  fi
fi

# A SIBLING STAGE'S PASS IS NOT A C PASS. This is the live instance: the kind field
# is compared by string equality against the expected stage kind.
c_refused "a 'rust-review' stage's PASS line -> refuse (wrong stage KIND)" \
  "$(c_verdict_file wrongkind \
    "REVIEW-STAGE: rust-review RESULT: PASS elapsed=7 deadline=1800 agent=rust-reviewer report=$T/planted-c-report.md")" \
  "stage kind"
c_refused "a kind that merely CONTAINS the expected one -> refuse (string equality, not a prefix)" \
  "$(c_verdict_file kindprefix \
    "REVIEW-STAGE: c-review RESULT: PASS elapsed=7 deadline=1800 agent=spec-auditor report=$T/planted-c-report.md")" \
  "stage kind"

# A TRUNCATED LINE IS NOT A VERDICT. Every mandatory key is required, and the
# refusal NAMES which are absent, because "your line is wrong" is not an operator
# action.
c_refused "a truncated line with NO key=value fields -> refuse, naming them" \
  "$(c_verdict_file truncated 'REVIEW-STAGE: c RESULT: PASS')" \
  "elapsed="
c_refused "a line missing only agent= -> refuse (each key is mandatory)" \
  "$(c_verdict_file noagent \
    "REVIEW-STAGE: c RESULT: PASS elapsed=7 deadline=1800 report=$T/planted-c-report.md")" \
  "agent="
c_refused "a line missing only report= -> refuse" \
  "$(c_verdict_file noreport 'REVIEW-STAGE: c RESULT: PASS elapsed=7 deadline=1800 agent=spec-auditor')" \
  "report="

# EXACTLY ONCE, not at least once: a duplicated key is two answers to one question,
# and a scanning consumer would read whichever it met first.
c_refused "a DUPLICATED elapsed= -> refuse (exactly once, never first-wins)" \
  "$(c_verdict_file dupelapsed \
    "REVIEW-STAGE: c RESULT: PASS elapsed=7 elapsed=99999 deadline=1800 agent=spec-auditor report=$T/planted-c-report.md")" \
  "elapsed="
c_refused "a DUPLICATED agent= -> refuse" \
  "$(c_verdict_file dupagent \
    "REVIEW-STAGE: c RESULT: PASS elapsed=7 deadline=1800 agent=spec-auditor agent=peer report=$T/planted-c-report.md")" \
  "agent="

# `RESULT:` MUST BE THE THIRD FIELD. Anywhere-on-the-line matching is what let a
# non-verdict line supply a token.
c_refused "RESULT: not in its documented position -> refuse" \
  "$(c_verdict_file resultpos \
    "REVIEW-STAGE: c verdict RESULT: PASS elapsed=7 deadline=1800 agent=spec-auditor report=$T/planted-c-report.md")" \
  "RESULT:"

# --- 44b(v): THE MANDATORY FIELDS' VALUES, NOT ONLY THEIR NAMES (round 7, L3) ---
# THE FINDING. Round 1's F2 made each of `elapsed=` / `deadline=` / `agent=` / `report=` mandatory
# and EXACTLY ONCE — by COUNTING them. It never looked at what they carried, so a `PASS` line ending
# in a BARE `report=`, or carrying an empty `elapsed=`, `deadline=` or `agent=`, was ACCEPTED and
# certified a merge. "Counted, not measured" is this repository's own recurring shape: a count is an
# affirmative measurement of PRESENCE and of nothing else.
#
# THE PERMITTED SET IS DERIVED FROM THE EMITTER, WHICH IS THE ONLY WAY THIS DOES NOT RED ON CORRECT
# INPUT. Part 1 below RUNS the SHIPPED review-stage.sh through EVERY state it has and requires each
# captured line to be accepted — including round 6's K1 state, which legitimately emits
# `elapsed=unknown deadline=unknown agent=unknown report=unresolved`, and `--deadline-secs 0`, which
# legitimately emits `deadline=0`. A validator written from what "looks reasonable" would red on
# both. Part 2 then pins the refusals.
RSSH="$SCRIPT_DIR/../flow/review-stage.sh"

# c_capture_state <state> — drive the SHIPPED emitter into <state> in a throwaway repository and
# print the path of a file holding its verdict line. Prints nothing on failure.
c_capture_state() {
  local st="$1" d f rp
  d="$T/l3-$st"
  rm -rf "$d" 2>/dev/null || true
  mkdir -p "$d" 2>/dev/null || return 1
  git init -q "$d" >/dev/null 2>&1 || return 1
  printf '.review-stage/\n' >"$d/.gitignore" 2>/dev/null || return 1
  if [ "$st" != never-opened ]; then
    if [ "$st" = deadline-zero ]; then
      (cd "$d" && bash "$RSSH" open c --issue 700 --agent spec-auditor --deadline-secs 0 >/dev/null 2>&1) || return 1
    else
      (cd "$d" && bash "$RSSH" open c --issue 700 --agent spec-auditor >/dev/null 2>&1) || return 1
    fi
  fi
  rp="$(SR_REPORT "$d" 700 c)"
  case "$st" in
    pass | deadline-zero) printf 'result: PASS\n' >"$rp" 2>/dev/null || return 1 ;;
    findings) printf 'result: FINDINGS\n' >"$rp" 2>/dev/null || return 1 ;;
    sentinel | never-opened) : ;;
    report-absent) rm -f "$rp" 2>/dev/null || true ;;
    report-empty) : >"$rp" 2>/dev/null || return 1 ;;
    report-ungrammatical) printf 'result: MAYBE\n' >"$rp" 2>/dev/null || return 1 ;;
    report-unreadable) printf 'result: PASS\n' >"$rp" 2>/dev/null || return 1; chmod 000 "$rp" 2>/dev/null || true ;;
    self-reported) printf 'result: NOT-RUN (the corpus root was absent)\n' >"$rp" 2>/dev/null || return 1 ;;
    record-unreadable) chmod 000 "$d/.review-stage/issue-700/c.stage" 2>/dev/null || true ;;
    author-performed)
      (cd "$d" && bash "$RSSH" record-author-performed c --issue 700 \
        --reason no-independent-auditor-available \
        --evidence docs/round-artifacts/issue-3751-l3.md --performed-by author >/dev/null 2>&1) || return 1 ;;
  esac
  f="$T/l3-cap-$st.txt"
  (cd "$d" && bash "$RSSH" verdict c --issue 700 >"$f" 2>/dev/null) || true
  chmod 644 "$d/.review-stage/issue-700/c.stage" 2>/dev/null || true
  [ -s "$f" ] || return 1
  printf '%s\n' "$f"
}

# PART 1 — every state the emitter can produce is ACCEPTED by the value validator.
# The EXIT CODE varies by token (PASS/AUTHOR-PERFORMED proceed, everything else refuses), so the
# assertion is on the CAUSE: no state may be refused for an unusable field value.
for L3_ST in pass findings sentinel report-absent report-empty report-ungrammatical \
  report-unreadable self-reported record-unreadable author-performed deadline-zero; do
  L3_F="$(c_capture_state "$L3_ST")" || L3_F=""
  if [ -n "$L3_F" ] && [ -s "$L3_F" ]; then
    ok "l3/states: captured a REAL verdict line from the shipped emitter for state '$L3_ST'"
    L3_OUT="$(PATH="$BIN:$PATH" bash "$NEUTRAL_ASSERT" 2421 "$CERTIFIED" "$GOOD" \
      --c-verdict "$L3_F" 2>&1)" || true
    case "$L3_OUT" in
      *"NO USABLE VALUE"*)
        bad "l3/states: state '$L3_ST' was refused for an unusable field value — the validator REDS ON CORRECT EMITTER OUTPUT (line: $(cat "$L3_F"))" ;;
      *) ok "l3/states: state '$L3_ST' is NOT refused for an unusable field value" ;;
    esac
  else
    bad "l3/states: could not capture a verdict line for state '$L3_ST' (1/2) — the assertion below would be vacuous"
    bad "l3/states: the same absence for state '$L3_ST' (2/2)"
  fi
done
# AND THE SWEEP IS NOT SATISFIED BY REFUSING EVERYTHING: the two proceeding tokens must still
# proceed, from the captured lines.
for L3_ST in pass author-performed; do
  L3_F="$T/l3-cap-$L3_ST.txt"
  if [ -s "$L3_F" ] && run 0 "l3/states CONTROL: the captured '$L3_ST' line still PROCEEDS" \
    2421 "$CERTIFIED" "$GOOD" --c-verdict "$L3_F"; then
    ok "l3/states CONTROL: and reports its token ('$L3_ST')"
  elif [ ! -s "$L3_F" ]; then
    bad "l3/states CONTROL: no captured line for '$L3_ST'"
  fi
done

# PART 2 — the refusals. Each line is WELL-FORMED except for ONE field's value, so the case cannot
# pass by refusing for a neighbouring reason: the assertion names the field.
c_value_refused() {
  local desc="$1" line="$2" needle="$3" f
  f="$(c_verdict_file "l3v-$(printf '%s' "$desc" | LC_ALL=C tr -c 'A-Za-z0-9' '-')" "$line")"
  if run 2 "$desc" 2421 "$CERTIFIED" "$GOOD" --c-verdict "$f"; then
    case "$OUT" in
      *"NO USABLE VALUE"*) ok "$desc: refused as a MANDATORY FIELD WITH NO USABLE VALUE" ;;
      *) bad "$desc: refused for the WRONG reason (got: $OUT)"; return 1 ;;
    esac
    case "$OUT" in
      *"$needle"*) ok "$desc: and the refusal NAMES the field and what was wrong" ;;
      *) bad "$desc: the refusal does not name '$needle' (got: $OUT)" ;;
    esac
  fi
}
c_value_refused "l3/bare-report" \
  "REVIEW-STAGE: c RESULT: PASS elapsed=7 deadline=1800 agent=spec-auditor report=" \
  "report= is EMPTY"
c_value_refused "l3/empty-agent" \
  "REVIEW-STAGE: c RESULT: PASS elapsed=7 deadline=1800 agent= report=$T/x.md" \
  "agent= is EMPTY"
c_value_refused "l3/empty-elapsed" \
  "REVIEW-STAGE: c RESULT: PASS elapsed= deadline=1800 agent=spec-auditor report=$T/x.md" \
  "elapsed=''"
c_value_refused "l3/empty-deadline" \
  "REVIEW-STAGE: c RESULT: PASS elapsed=7 deadline= agent=spec-auditor report=$T/x.md" \
  "deadline=''"
c_value_refused "l3/nonnumeric-elapsed" \
  "REVIEW-STAGE: c RESULT: PASS elapsed=abc deadline=1800 agent=spec-auditor report=$T/x.md" \
  "elapsed='abc'"
c_value_refused "l3/suffixed-deadline" \
  "REVIEW-STAGE: c RESULT: PASS elapsed=7 deadline=1800s agent=spec-auditor report=$T/x.md" \
  "deadline='1800s'"
# THE `unknown` SENTINELS ARE ACCEPTED — round 6's K1 emits them, and they are not a passing verdict
# on their own (their token is NOT-RUN). Asserted against a PASS token deliberately: the value
# validator must not be the thing that catches a hand-forged token, and a `NOT-RUN` line would be
# refused by the token switch before this validator's acceptance could be observed.
if run 0 "l3/unknown-sentinels: elapsed=unknown deadline=unknown agent=unknown is ACCEPTED" \
  2421 "$CERTIFIED" "$GOOD" --c-verdict "$(c_verdict_file l3unk \
    "REVIEW-STAGE: c RESULT: PASS elapsed=unknown deadline=unknown agent=unknown report=unresolved")"; then
  case "$OUT" in
    *"NO USABLE VALUE"*) bad "l3/unknown-sentinels: the emitter's own not-measured values were refused" ;;
    *) ok "l3/unknown-sentinels: and 'unresolved' as the report, which round 6's K1 emits" ;;
  esac
fi
if run 0 "l3/zero: elapsed=0 deadline=0 is ACCEPTED — a zero is a measurement" \
  2421 "$CERTIFIED" "$GOOD" --c-verdict "$(c_verdict_file l3zero \
    "REVIEW-STAGE: c RESULT: PASS elapsed=0 deadline=0 agent=spec-auditor report=$T/x.md")"; then
  ok "l3/zero: (--deadline-secs 0 is a legal open, so this line is reachable from the emitter)"
fi

# --- 44c: AUTHOR-PERFORMED keeps its own token (item 2.4) -------------------
if run 0 "verdict AUTHOR-PERFORMED -> proceeds, under its OWN token" \
  2421 "$CERTIFIED" "$GOOD" \
  --c-verdict "$(c_verdict_file ap "$(cv_line AUTHOR-PERFORMED)")"; then
  case "$OUT" in
    *"PREMERGE: C-VERDICT AUTHOR-PERFORMED"*)
      ok "AUTHOR-PERFORMED: printed under its own token on a PREMERGE: C-VERDICT line" ;;
    *) bad "AUTHOR-PERFORMED: must appear under its own token (got: $OUT)" ;;
  esac
  # NEVER FOLDED INTO `OK`, and this is the assertion that says so: a reader
  # grepping the PASSING token must NOT match a substitute. Same reason the roborev
  # wrapper's WAIVED is textually distinct from PASS.
  case "$OUT" in
    *"PREMERGE: C-VERDICT PASS"*)
      bad "AUTHOR-PERFORMED: a substitute matched the PASSING token — it was folded into a pass" ;;
    *) ok "AUTHOR-PERFORMED: a reader grepping 'C-VERDICT PASS' does NOT match a substitute" ;;
  esac
  case "$OUT" in
    *"PREMERGE: OK $CERTIFIED"*)
      ok "AUTHOR-PERFORMED: the merge still proceeds (it is the SANCTIONED fallback)" ;;
    *) bad "AUTHOR-PERFORMED: must not block the merge (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: C-VERDICT-NOTE"*"not an independent"*)
      ok "AUTHOR-PERFORMED: the disclosure travels to the merge point" ;;
    *) bad "AUTHOR-PERFORMED: the disclosure must be printed (got: $OUT)" ;;
  esac
fi

# --- 44d: AUTO MEASURES the routing (items 2.2/2.3), end to end -------------
# SYNTHETIC REPOSITORIES, so the routing answer is a property of THIS SUITE and
# not of the lane it happens to run in. Each carries `refs/remotes/origin/main`
# (the merge-base the measurement is taken against) and a `.gitignore` for
# `.review-stage/`, because review-stage.sh verifies its write path is ignored
# fail-closed (#2926) and would otherwise refuse to open a stage at all.
#
# The NEUTRAL copy of the assert is used, so the base-staleness advisory is the
# immediate stub and nothing scans a repository — but review-stage.sh beside it is
# the SHIPPED artifact, so these cases exercise the real producer grammar.

# c_repo <name> <mode> — build a synthetic repo; print its path, or nothing on
# failure. mode: oracle (no openspec) | design (an openspec change) |
# archive (only openspec/changes/archive/**).
c_repo() {
  # The `local` declarations are SPLIT: `local name="$1" d="$T/c-repo-$name"` reads
  # $name before the assignment takes effect and dies under `set -u` — the same
  # trap flow_copy above documents, and it cost a round here too.
  local name="$1" mode="$2"
  local d="$T/c-repo-$name"
  local p
  mkdir -p "$d"
  git init -q -b mainline "$d" >/dev/null 2>&1 || return 1
  git -C "$d" config user.email t@t
  git -C "$d" config user.name t
  printf '.review-stage/\n' >"$d/.gitignore"
  printf 'seed\n' >"$d/README.md"
  git -C "$d" add -A >/dev/null 2>&1 || return 1
  git -C "$d" commit -q -m seed >/dev/null 2>&1 || return 1
  git -C "$d" update-ref refs/remotes/origin/main mainline || return 1
  git -C "$d" checkout -q -b feature || return 1
  case "$mode" in
    oracle)  p="cqlite-core/src/storage/sstable/reader.rs" ;;
    design)  p="openspec/changes/a-design-routed-slug/proposal.md" ;;
    archive) p="openspec/changes/archive/2026-01-01-old/proposal.md" ;;
    *) return 1 ;;
  esac
  mkdir -p "$d/$(dirname "$p")"
  printf 'the PR content\n' >"$d/$p"
  git -C "$d" add -A >/dev/null 2>&1 || return 1
  git -C "$d" commit -q -m "the PR" >/dev/null 2>&1 || return 1
  printf '%s\n' "$d"
}

# run_in_repo <dir> <expected-exit> <desc> <args...> — `run`, from inside a
# synthetic repository, with that repository's own HEAD as the certified sha and a
# gate-summary fixture carrying its two abbreviations. Sets $OUT/$RC.
run_in_repo() {
  local d="$1" want="$2" desc="$3"
  shift 3
  local sha f
  sha=$(git -C "$d" rev-parse HEAD 2>/dev/null) || sha=""
  if [ -z "$sha" ]; then
    bad "$desc: could not resolve the synthetic repository's HEAD"
    return 1
  fi
  f="$d/../gate-$(basename "$d").txt"
  emit_summary_block "$FULL_S" "$FULL_E" "-" \
    "$(printf '%.7s' "$sha")" "$(printf '%.12s' "$sha")" PASS PASS >"$f"
  OUT=$(cd "$d" && PATH="$BIN:$PATH" MOCK_GH_OUT="$sha OPEN" MOCK_GH_FAIL=0 \
    bash "$NEUTRAL_ASSERT" 2421 "$sha" "$f" "$@" 2>&1)
  RC=$?
  C_REPO_SHA="$sha"
  if [ "$RC" -ne "$want" ]; then
    bad "$desc (exit $RC, wanted $want)"
    printf '     output: %s\n' "$OUT"
    return 1
  fi
  return 0
}

C_ORACLE=$(c_repo oracle oracle) || C_ORACLE=""
C_DESIGN=$(c_repo design design) || C_DESIGN=""
C_ARCHIVE=$(c_repo archive archive) || C_ARCHIVE=""
if [ -n "$C_ORACLE" ] && [ -n "$C_DESIGN" ] && [ -n "$C_ARCHIVE" ]; then
  ok "AUTO fixtures: three synthetic repositories built (oracle / design / archive-only)"
else
  bad "AUTO fixtures: the synthetic repositories could not be built — the AUTO cases would be vacuous"
fi

# NOT-APPLICABLE IS A MEASUREMENT, NOT AN ASSERTION. This repository carries NO
# openspec change on its branch, and the script says so AFFIRMATIVELY, naming what
# it measured — it does not merely stay quiet.
if [ -n "$C_ORACLE" ] &&
  run_in_repo "$C_ORACLE" 0 "AUTO on an ORACLE-routed branch -> NOT-APPLICABLE, measured" \
    --c-verdict AUTO; then
  case "$OUT" in
    *"PREMERGE: C-VERDICT NOT-APPLICABLE"*"no openspec change on branch"*)
      ok "AUTO/oracle: reported affirmatively, naming what was measured" ;;
    *) bad "AUTO/oracle: must report NOT-APPLICABLE (no openspec change on branch) (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: OK"*) ok "AUTO/oracle: the merge proceeds without a C verdict" ;;
    *) bad "AUTO/oracle: an oracle-driven branch must not need C (got: $OUT)" ;;
  esac
fi

# ARCHIVING IS NOT A ROUTING SIGNAL. `openspec/changes/archive/**` is what
# flow-finalize writes AFTER a merge, so a PR that only archives is not
# design-routed — and `archive` is present on `origin/main` permanently, which is
# why the measurement is a DIFF and not a listing.
if [ -n "$C_ARCHIVE" ] &&
  run_in_repo "$C_ARCHIVE" 0 "AUTO on an ARCHIVE-only branch -> NOT-APPLICABLE" \
    --c-verdict AUTO; then
  case "$OUT" in
    *"no openspec change on branch"*) ok "AUTO/archive: archive/** is excluded from the routing measure" ;;
    *) bad "AUTO/archive: openspec/changes/archive/** must not route to C (got: $OUT)" ;;
  esac
fi

# A REAL `openspec archive` MOVE IS NOT DESIGN-ROUTED EITHER (F4). Rename detection is
# pinned OFF deliberately, so a live -> archive move appears as a DELETION from
# `openspec/changes/<slug>/` plus an ADDITION under `archive/`: the addition is
# excluded and the deletion used to be COUNTED, so an archive-only finalize PR was
# design-routed and REFUSED for want of a C verdict. That is a false refusal on
# correct, doctrine-mandated input — the guard agents learn to waive. The fixture
# performs the ACTUAL move (`git mv`) rather than simulating its shape.
c_repo_archive_move() {
  local d="$T/c-repo-archive-move"
  mkdir -p "$d"
  git init -q -b mainline "$d" >/dev/null 2>&1 || return 1
  git -C "$d" config user.email t@t
  git -C "$d" config user.name t
  printf '.review-stage/\n' >"$d/.gitignore"
  printf 'seed\n' >"$d/README.md"
  # The LIVE change exists on the merge-base, which is what makes the move a DELETION
  # there — the whole shape of the finding.
  mkdir -p "$d/openspec/changes/a-finished-slug"
  printf 'the proposal\n' >"$d/openspec/changes/a-finished-slug/proposal.md"
  mkdir -p "$d/openspec/changes/a-finished-slug/specs/thing"
  printf 'the spec delta\n' >"$d/openspec/changes/a-finished-slug/specs/thing/spec.md"
  git -C "$d" add -A >/dev/null 2>&1 || return 1
  git -C "$d" commit -q -m seed >/dev/null 2>&1 || return 1
  git -C "$d" update-ref refs/remotes/origin/main mainline || return 1
  git -C "$d" checkout -q -b finalize || return 1
  mkdir -p "$d/openspec/changes/archive"
  git -C "$d" mv openspec/changes/a-finished-slug \
    openspec/changes/archive/a-finished-slug >/dev/null 2>&1 || return 1
  git -C "$d" commit -q -m "archive the completed change" >/dev/null 2>&1 || return 1
  printf '%s\n' "$d"
}
C_ARCHIVE_MOVE=$(c_repo_archive_move) || C_ARCHIVE_MOVE=""
if [ -n "$C_ARCHIVE_MOVE" ]; then
  ok "archive-move fixture: a live openspec change was really git mv'd into archive/"
else
  bad "archive-move fixture: could not build it — the case would be vacuous"
fi
if [ -n "$C_ARCHIVE_MOVE" ] &&
  run_in_repo "$C_ARCHIVE_MOVE" 0 \
    "AUTO on a real live->archive MOVE -> NOT-APPLICABLE (deletions are not a routing signal)" \
    --c-verdict AUTO; then
  case "$OUT" in
    *"PREMERGE: C-VERDICT NOT-APPLICABLE"*"no openspec change on branch"*)
      ok "archive-move: reported NOT-APPLICABLE, so a finalize PR is not falsely design-routed" ;;
    *) bad "archive-move: must report NOT-APPLICABLE (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: OK"*) ok "archive-move: the merge proceeds without a C verdict" ;;
    *) bad "archive-move: an archiving PR must not need C (got: $OUT)" ;;
  esac
fi
# THE OTHER DIRECTION STAYS FAIL-CLOSED: an ADDITION or a MODIFICATION under a live
# `openspec/changes/<slug>/` still routes to C. Without this, `--diff-filter=d` could
# have been widened to exclude everything and every case above would still pass.
C_MODIFY=""
if [ -n "$C_ARCHIVE_MOVE" ]; then
  C_MODIFY="$T/c-repo-modify"
  if cp -R "$C_ARCHIVE_MOVE" "$C_MODIFY" >/dev/null 2>&1 &&
    git -C "$C_MODIFY" checkout -q -b modify mainline &&
    printf 'an EDITED spec delta\n' \
      >"$C_MODIFY/openspec/changes/a-finished-slug/specs/thing/spec.md" &&
    git -C "$C_MODIFY" add -A >/dev/null 2>&1 &&
    git -C "$C_MODIFY" commit -q -m "edit the live spec delta" >/dev/null 2>&1; then
    ok "modify fixture: a branch that MODIFIES a live spec delta was built"
  else
    bad "modify fixture: could not build it — the fail-closed direction would be untested"
    C_MODIFY=""
  fi
fi
if [ -n "$C_MODIFY" ] &&
  run_in_repo "$C_MODIFY" 2 \
    "AUTO on a MODIFIED live spec delta -> still C REQUIRED (the fail-closed direction)" \
    --c-verdict AUTO; then
  case "$OUT" in
    *"routing: REQUIRED"*"openspec/changes/a-finished-slug"*)
      ok "modify: a modification still routes to C, naming the change it found" ;;
    *) bad "modify: a modified spec delta must route to C (got: $OUT)" ;;
  esac
fi

# A DESIGN-ROUTED BRANCH WITH NO STAGE EVER OPENED. This is the state
# review-stage.sh names `NOT-RUN (stage never opened)`, and it must REFUSE.
if [ -n "$C_DESIGN" ] &&
  run_in_repo "$C_DESIGN" 2 "AUTO on a DESIGN-routed branch with no stage -> REFUSE" \
    --c-verdict AUTO; then
  case "$OUT" in
    *"PREMERGE: NO-C-VERDICT"*) ok "AUTO/design: refused at the merge point" ;;
    *) bad "AUTO/design: must refuse with NO-C-VERDICT (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"routing: REQUIRED"*"openspec/changes/a-design-routed-slug"*)
      ok "AUTO/design: the routing measurement NAMES the change it found" ;;
    *) bad "AUTO/design: the refusal must name the measured routing (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"No 'c' stage was ever OPENED"*) ok "AUTO/design: the cause is named (stage never opened)" ;;
    *) bad "AUTO/design: the refusal must name the cause (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"review-stage.sh open c --issue"*) ok "AUTO/design: the refusal prints the remedy" ;;
    *) bad "AUTO/design: the refusal must name the remedy (got: $OUT)" ;;
  esac
fi

# THE AC1 END-TO-END WIRE: open a stage, spawn nothing, and the merge assert
# REFUSES on the sentinel. This is the whole issue in one case — the state that
# was previously indistinguishable from a clean review.
C_STAGED=""
if [ -n "$C_DESIGN" ]; then
  if (cd "$C_DESIGN" && bash "$NEUTRAL_DIR/review-stage.sh" open c --issue 3751 \
    --agent spec-auditor >/dev/null 2>&1); then
    C_STAGED=1
    ok "AC1 wire: a stage was opened in the synthetic repository (sentinel pre-stamped)"
  else
    bad "AC1 wire: review-stage.sh open failed in the synthetic repository — the wire is vacuous"
  fi
fi
if [ -n "$C_STAGED" ] &&
  run_in_repo "$C_DESIGN" 2 "AC1 wire: a SENTINEL-ONLY stage -> the merge assert REFUSES" \
    --c-verdict AUTO; then
  case "$OUT" in
    *"reports NOT-RUN"*) ok "AC1 wire: reported as NOT-RUN, never as clean and never as empty findings" ;;
    *) bad "AC1 wire: the sentinel must be reported as NOT-RUN (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"no report written"*) ok "AC1 wire: the NOT-RUN CAUSE travels to the merge point" ;;
    *) bad "AC1 wire: the cause must travel (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"elapsed="*) ok "AC1 wire: the elapsed time travels with the refusal" ;;
    *) bad "AC1 wire: the verdict line's elapsed= must reach the operator (got: $OUT)" ;;
  esac
fi

# THE POSITIVE CONTROL, END TO END (the spec's "the merge assert proceeds"). An
# implementation that answered NOT-RUN for every input would pass every case above
# and FAIL this one — which is the whole reason it exists.
if [ -n "$C_STAGED" ]; then
  if (cd "$C_DESIGN" && printf 'result: PASS\n\n## Findings\n\nnone.\n' \
    >"$(SR_REPORT "$C_DESIGN" 3751 c)"); then
    ok "positive control: a real report was written over the sentinel"
  else
    bad "positive control: could not write the report — the control is vacuous"
  fi
  if run_in_repo "$C_DESIGN" 0 \
    "POSITIVE CONTROL: a real C report -> the merge assert PROCEEDS (exit 0)" \
    --c-verdict AUTO; then
    case "$OUT" in
      *"PREMERGE: C-VERDICT PASS"*"source: AUTO issue=3751"*)
        ok "positive control: the verdict is PASS and names the stage it read" ;;
      *) bad "positive control: must report C-VERDICT PASS from AUTO (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"PREMERGE: OK $C_REPO_SHA"*) ok "positive control: the merge point reaches OK" ;;
      *) bad "positive control: must reach PREMERGE: OK (got: $OUT)" ;;
    esac
  fi
fi

# ROUTING THAT CANNOT BE MEASURED IS TREATED AS REQUIRED. The certified sha here
# is the suite's synthetic $CERTIFIED, which exists in NO repository, so the
# measurement fails — and a failure to measure must never take the permissive
# branch (the standing rule against deriving a pass from the absence of a signal).
if [ -n "$C_ORACLE" ]; then
  UNM_OUT=$(cd "$C_ORACLE" && PATH="$BIN:$PATH" MOCK_GH_OUT="$CERTIFIED OPEN" MOCK_GH_FAIL=0 \
    bash "$NEUTRAL_ASSERT" 2421 "$CERTIFIED" "$GOOD" --c-verdict AUTO 2>&1)
  UNM_RC=$?
  if [ "$UNM_RC" -ne 2 ]; then
    bad "AUTO/unmeasured: an unmeasurable routing must REFUSE (exit $UNM_RC, wanted 2)"
    printf '     output: %s\n' "$UNM_OUT"
  else
    ok "AUTO/unmeasured: an unmeasurable routing REFUSES rather than passing"
    case "$UNM_OUT" in
      *"routing: UNMEASURED"*"is not present in this checkout"*)
        ok "AUTO/unmeasured: the cause is named, not collapsed onto NOT-APPLICABLE" ;;
      *) bad "AUTO/unmeasured: the refusal must name why it could not measure (got: $UNM_OUT)" ;;
    esac
    case "$UNM_OUT" in
      *"NOT-APPLICABLE"*)
        bad "AUTO/unmeasured: an unmeasured routing was reported as NOT-APPLICABLE — the permissive branch" ;;
      *) ok "AUTO/unmeasured: UNMEASURED is never rendered as NOT-APPLICABLE" ;;
    esac
  fi
fi

# TWO STAGE RECORDS IN ONE WORKTREE IS AMBIGUOUS. 1:1:1:1 puts exactly one issue
# in a worktree, so two records mean the caller is not where it thinks it is —
# the #3616 wrong-run-dir class, one directory over.
if [ -n "$C_STAGED" ]; then
  if (cd "$C_DESIGN" && bash "$NEUTRAL_DIR/review-stage.sh" open c --issue 4242 \
    --agent spec-auditor >/dev/null 2>&1); then
    if run_in_repo "$C_DESIGN" 2 "AUTO with TWO c stage records -> refuse as AMBIGUOUS" \
      --c-verdict AUTO; then
      case "$OUT" in
        *"stage records exist"*"AMBIGUOUS"*)
          ok "AUTO/ambiguous: two stages refuse rather than one being picked" ;;
        *) bad "AUTO/ambiguous: must refuse naming the ambiguity (got: $OUT)" ;;
      esac
      # AND NOT THE WRONG DIAGNOSTIC. If the ambiguity ever degrades to an advisory print, the
      # run continues with an EMPTY issue and reports "No 'c' stage was ever OPENED" — the
      # wrong next action for a caller standing in two lanes' worth of stage records.
      case "$OUT" in
        *"was ever OPENED"*)
          bad "AUTO/ambiguous: the refusal must not ALSO print the never-opened diagnostic (got: $OUT)" ;;
        *) ok "AUTO/ambiguous: the never-opened diagnostic is NOT printed — the ambiguity is the verdict" ;;
      esac
    fi
  else
    bad "AUTO/ambiguous: could not open a second stage — the case is vacuous"
  fi
fi

# THE AMBIGUITY REFUSAL IS RAISED AT THE CALL SITE, NOT INSIDE A COMMAND SUBSTITUTION
# (#3751 round 2, S3). STRUCTURAL, AND LABELLED AS SUCH: the behaviour above is correct with
# `set -e` ON, which is the only configuration this script ships, so no behavioural case can
# distinguish "refuses because the caller checked" from "refuses because a failed assignment
# tripped `set -e`". What IS decidable from source is that `c_auto_locate_issue` — whose only
# caller is a command substitution, where `exit` terminates the SUBSHELL — raises no refusal of
# its own, and that its caller checks the status.
C_LOC_BODY=$(LC_ALL=C awk '
  /^c_auto_locate_issue\(\) \{/ { inf = 1 }
  inf { print }
  inf && /^\}/ { exit }
' "$ASSERT")
case "$C_LOC_BODY" in
  "") bad "S3-structural: could not extract c_auto_locate_issue from the shipped script — the assert has no subject" ;;
  *) ok "S3-structural: c_auto_locate_issue was located in the shipped script" ;;
esac
case "$C_LOC_BODY" in
  *refuse_no_c_verdict* | *refuse_tool_failure* | *"exit "*)
    bad "S3-structural: c_auto_locate_issue still refuses/exits INSIDE itself; its only caller is a command substitution, so that exit terminates the subshell and reaching the top level depends on set -e (body: $C_LOC_BODY)" ;;
  *) ok "S3-structural: c_auto_locate_issue raises no refusal and no exit of its own — it REPORTS (a value plus a status)" ;;
esac
if printf '%s\n' "$C_LOC_BODY" | LC_ALL=C grep -q 'return 3'; then
  ok "S3-structural: it reports the ambiguity as a STATUS (return 3) the caller can check"
else
  bad "S3-structural: c_auto_locate_issue does not return a distinct status for the ambiguous case (body: $C_LOC_BODY)"
fi
if LC_ALL=C grep -q 'issue=$(c_auto_locate_issue) || arc=$?' "$ASSERT"; then
  ok "S3-structural: the CALL SITE captures the status with the correct idiom (\`cmd || rc=\$?\`, never \`if ! cmd; then rc=\$?\`, which reads 0)"
else
  bad "S3-structural: the call site does not capture c_auto_locate_issue's status, so the ambiguity cannot be refused explicitly"
fi

# --- 44e: AUTO's locally-located stage must be BOUND to the certified tree ----
# THE #3616 PEER-ARTIFACT CLASS, one directory over. On this fleet every lane is a
# worktree of ONE shared `.git`, so a PEER lane's certified commit RESOLVES from
# any lane — resolvability proves nothing about whose `.review-stage/` this is.
# AUTO locates the stage in the CURRENT worktree, so without a binding a lane
# could certify a merge with a stage that belongs to a different branch entirely.
# The binding is HEAD-equality: rule 1 asserts the PR's headRefOid == the
# certified sha, so HEAD == certified binds the local artifact to THIS PR
# transitively.
#
# The fixture is that exact shape: a design-routed repo whose HEAD carries a
# PASSING c stage, and a SECOND commit (on another branch) that is resolvable here
# but is NOT HEAD. Certifying the second commit must REFUSE.
C_PEER=$(c_repo peer design) || C_PEER=""
C_PEER_SHA=""
if [ -n "$C_PEER" ]; then
  # A second commit, resolvable in this repository, NOT on the checked-out branch —
  # exactly what a peer lane's push looks like through a shared object store. It
  # also touches openspec/changes/, so its OWN routing measures REQUIRED and the
  # refusal cannot be an artifact of routing.
  if git -C "$C_PEER" checkout -q -b peer-lane mainline &&
    mkdir -p "$C_PEER/openspec/changes/a-peer-lanes-slug" &&
    printf 'a peer lane content\n' >"$C_PEER/openspec/changes/a-peer-lanes-slug/proposal.md" &&
    git -C "$C_PEER" add -A >/dev/null 2>&1 &&
    git -C "$C_PEER" commit -q -m "the PEER lane's PR" >/dev/null 2>&1 &&
    C_PEER_SHA=$(git -C "$C_PEER" rev-parse HEAD 2>/dev/null) &&
    git -C "$C_PEER" checkout -q feature &&
    (cd "$C_PEER" && bash "$NEUTRAL_DIR/review-stage.sh" open c --issue 3751 \
      --agent spec-auditor >/dev/null 2>&1) &&
    printf 'result: PASS\n\n## Findings\n\nnone.\n' \
      >"$(SR_REPORT "$C_PEER" 3751 c)"; then
    ok "binding fixture: a PASSING local stage plus a resolvable NON-HEAD commit"
  else
    bad "binding fixture: could not build the peer-commit repository — the case would be vacuous"
    C_PEER_SHA=""
  fi
fi
if [ -n "$C_PEER_SHA" ]; then
  # POSITIVE CONTROL FIRST: at the repo's OWN HEAD the same stage certifies. Without
  # this, a binding that refused EVERYTHING would satisfy the case below.
  if run_in_repo "$C_PEER" 0 \
    "binding control: AUTO at the worktree's OWN head -> the local stage certifies" \
    --c-verdict AUTO; then
    case "$OUT" in
      *"PREMERGE: C-VERDICT PASS"*) ok "binding control: HEAD == certified proceeds" ;;
      *) bad "binding control: must report C-VERDICT PASS (got: $OUT)" ;;
    esac
  fi
  BIND_F="$T/gate-peer-bind.txt"
  emit_summary_block "$FULL_S" "$FULL_E" "-" \
    "$(printf '%.7s' "$C_PEER_SHA")" "$(printf '%.12s' "$C_PEER_SHA")" PASS PASS >"$BIND_F"
  BIND_OUT=$(cd "$C_PEER" && PATH="$BIN:$PATH" MOCK_GH_OUT="$C_PEER_SHA OPEN" MOCK_GH_FAIL=0 \
    bash "$NEUTRAL_ASSERT" 2421 "$C_PEER_SHA" "$BIND_F" --c-verdict AUTO 2>&1)
  BIND_RC=$?
  if [ "$BIND_RC" -eq 2 ]; then
    ok "binding: a stage located in a worktree whose HEAD is NOT the certified commit REFUSES"
  else
    bad "binding: a FOREIGN certified sha was certified by this lane's stage (exit $BIND_RC, wanted 2)"
    printf '     output: %s\n' "$BIND_OUT"
  fi
  case "$BIND_OUT" in
    *"PREMERGE: NO-C-VERDICT"*) ok "binding: refused under the NO-C-VERDICT verdict" ;;
    *) bad "binding: must refuse with NO-C-VERDICT (got: $BIND_OUT)" ;;
  esac
  case "$BIND_OUT" in
    *"HEAD"*"is not the certified commit"*)
      ok "binding: the refusal NAMES the head/certified divergence" ;;
    *) bad "binding: the refusal must name the divergence (got: $BIND_OUT)" ;;
  esac
  case "$BIND_OUT" in
    *"shared"*) ok "binding: the refusal says WHY resolvability is not provenance" ;;
    *) bad "binding: the refusal must explain the shared-object-store reason (got: $BIND_OUT)" ;;
  esac
  case "$BIND_OUT" in
    *"PREMERGE: C-VERDICT PASS"*)
      bad "binding: a foreign-sha run reported C-VERDICT PASS — the peer artifact certified it" ;;
    *) ok "binding: no PASS token is emitted for a foreign certified sha" ;;
  esac
fi

# --- 44f: the STAGE ARTIFACT must be bound to the certified sha too (G1) ------
# 44e binds the WORKTREE (HEAD == certified). It does NOT bind the ARTIFACT, and the two
# are different questions: the stage record carried no commit identity, so a `result: PASS`
# recorded BEFORE a further commit, an amend or a rebase persisted in `.review-stage/` and
# certified the NEW tree — and the HEAD check was satisfied BY CONSTRUCTION, because the
# lane is standing at the very commit it is certifying. Open the stage, get a PASS, commit
# again, and the stale PASS still read clean.
#
# FAIL-CLOSED IS DELIBERATE HERE: a C audit of an older tree may not certify a newer one.
# That is the gate-of-record rule ("any src change after the gate INVALIDATES it") applied
# to the intent audit, so a missing or unparsable `head-sha:` REFUSES by name rather than
# being skipped — an older record predating the field must not be readable as certifying.
#
# `sr_field` rewrites the stage record in place. The record is a file this tool writes, so a
# case may plant a shape review-stage.sh would never emit (no field, two fields, a
# non-sha value) — which is exactly what the refusals are for.
STALE_REPO=$(c_repo stale design) || STALE_REPO=""
STALE_A=""
STALE_B=""
if [ -n "$STALE_REPO" ]; then
  if (cd "$STALE_REPO" && bash "$NEUTRAL_DIR/review-stage.sh" open c --issue 3751 \
    --agent spec-auditor >/dev/null 2>&1) &&
    printf 'result: PASS\n\n## Findings\n\nnone.\n' \
      >"$(SR_REPORT "$STALE_REPO" 3751 c)" &&
    STALE_A=$(git -C "$STALE_REPO" rev-parse HEAD 2>/dev/null); then
    ok "stale fixture: a PASSING c stage was opened at the branch head (sha A)"
  else
    bad "stale fixture: could not open the stage — the case would be vacuous"
    STALE_A=""
  fi
fi
STALE_RECORD="$STALE_REPO/.review-stage/issue-3751/c.stage"
if [ -n "$STALE_A" ]; then
  # The recorded binding must really name A, or every assertion below is about nothing.
  if LC_ALL=C grep -q "^head-sha: $STALE_A\$" "$STALE_RECORD" 2>/dev/null; then
    ok "stale fixture: the stage record really records the commit it was opened at (head-sha: A)"
  else
    bad "stale fixture: the stage record does not record head-sha: A — the binding cannot be asserted (record: $(cat "$STALE_RECORD" 2>/dev/null))"
  fi
  # THE POSITIVE CONTROL FIRST: at A the stage certifies. Without it a binding that refused
  # everything would satisfy every negative case below.
  if run_in_repo "$STALE_REPO" 0 \
    "stale control: at the SAME commit the stage was opened at, it certifies" \
    --c-verdict AUTO; then
    case "$OUT" in
      *"PREMERGE: C-VERDICT PASS"*) ok "stale control: head-sha == certified proceeds" ;;
      *) bad "stale control: must report C-VERDICT PASS (got: $OUT)" ;;
    esac
  fi
  # NOW THE DEFECT: a further commit. HEAD moves to B, the certified sha IS B (so 44e's
  # worktree binding passes), and the stage record still says A.
  if printf 'a later change nobody audited\n' >"$STALE_REPO/openspec/changes/a-design-routed-slug/design.md" &&
    git -C "$STALE_REPO" add -A >/dev/null 2>&1 &&
    git -C "$STALE_REPO" commit -q -m "a further commit after the C audit" >/dev/null 2>&1 &&
    STALE_B=$(git -C "$STALE_REPO" rev-parse HEAD 2>/dev/null) &&
    [ "$STALE_A" != "$STALE_B" ]; then
    ok "stale fixture: a FURTHER commit was made (sha B != A), the stage untouched"
  else
    bad "stale fixture: could not make the further commit — the defect cannot be reproduced"
    STALE_B=""
  fi
fi
if [ -n "$STALE_B" ]; then
  if run_in_repo "$STALE_REPO" 2 \
    "stale: a PASS recorded at A must NOT certify B (the whole G1 defect)" \
    --c-verdict AUTO; then
    case "$OUT" in
      *"PREMERGE: NO-C-VERDICT"*) ok "stale: refused under the NO-C-VERDICT verdict" ;;
      *) bad "stale: must refuse with NO-C-VERDICT (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"$STALE_A"*) ok "stale: the refusal NAMES the commit the stage was opened at" ;;
      *) bad "stale: the refusal must name the recorded sha (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"PREMERGE: C-VERDICT PASS"*)
        bad "stale: a stale PASS certified a newer tree — the G1 defect is live" ;;
      *) ok "stale: no PASS token is emitted for a stage opened at another commit" ;;
    esac
  fi
  # AND THE REMEDY IS RE-OPENING THE STAGE: --force re-stamps head-sha (deliberately unlike
  # spawned-at), so a re-opened, re-audited stage certifies B. Without this the refusal above
  # would be a dead end, and a guard with no way past it is the guard agents learn to waive.
  # THE RE-AUDIT GOES INTO THE PATH THE RE-OPEN PRINTS, NOT THE OLD ONE (#3751 round 5 J1, round
  # 6 K2). A forced re-open publishes a report under a FRESH NONCE, so the file the PREVIOUS agent
  # holds is not read any more. Taking the path from the tool's own printed line is also the
  # property under test one level up: the clause a lane pastes into the re-spawn prompt has to name
  # the file that counts.
  STALE_RP_OLD="$(SR_REPORT "$STALE_REPO" 3751 c)"
  STALE_REOPEN_OUT="$(cd "$STALE_REPO" && bash "$NEUTRAL_DIR/review-stage.sh" open c --issue 3751 \
    --agent spec-auditor --force 2>/dev/null || true)"
  STALE_RP="$(printf '%s\n' "$STALE_REOPEN_OUT" | LC_ALL=C sed -n 's|^\(/.*\.md\)$|\1|p' | LC_ALL=C head -1)"
  if [ -n "$STALE_RP" ] && [ "$STALE_RP" != "$STALE_RP_OLD" ]; then
    ok "stale remedy: the re-open printed a FRESH report path (a resumed agent's old path cannot certify)"
  else
    bad "stale remedy: the re-open printed '$STALE_RP', which is not a fresh report path"
  fi
  if [ -n "$STALE_RP" ] &&
    printf 'result: PASS\n\n## Findings\n\nre-audited at B.\n' >"$STALE_RP"; then
    if LC_ALL=C grep -q "^head-sha: $STALE_B\$" "$STALE_RECORD" 2>/dev/null; then
      ok "stale remedy: --force RE-STAMPS head-sha to the current commit (B)"
    else
      bad "stale remedy: --force did not re-stamp head-sha to B (record: $(cat "$STALE_RECORD" 2>/dev/null))"
    fi
    if run_in_repo "$STALE_REPO" 0 \
      "stale remedy: a re-opened, re-audited stage certifies B" --c-verdict AUTO; then
      case "$OUT" in
        *"PREMERGE: C-VERDICT PASS"*) ok "stale remedy: the merge proceeds after a real re-audit" ;;
        *) bad "stale remedy: must reach C-VERDICT PASS (got: $OUT)" ;;
      esac
    fi
    # --force PRESERVES THE CLOCK while re-stamping the sha — the two fields answer different
    # questions and a fix that re-stamped both would hide how long the stage has been open.
    if LC_ALL=C grep -q '^reopen-count: 1$' "$STALE_RECORD" 2>/dev/null; then
      ok "stale remedy: the re-open was recorded as a re-open (reopen-count: 1)"
    else
      bad "stale remedy: the re-open was not recorded (record: $(cat "$STALE_RECORD" 2>/dev/null))"
    fi
  else
    bad "stale remedy: could not re-open the stage — the remedy is unasserted"
  fi
fi
# THE THREE UNPARSABLE SHAPES. Each is a NAMED refusal, never a skip: a record that cannot
# state which tree it audited certifies nothing, and "cannot tell" must not take the
# permissive branch. Rebuilt from a fresh fixture so the cases are independent of the
# sequence above.
sr_plant() {
  # sr_plant <repo> <awk-program> — RESTORE the pristine stage record, then rewrite it through
  # awk. A helper rather than `sed -i`, whose in-place flag is GNU/BSD incompatible on this
  # fleet. The RESTORE is load-bearing and was measured: without it case (a) (which DELETES the
  # head-sha line) left case (b) with ONE planted line instead of two, so (b) exercised the
  # stale-value branch while its label claimed the ambiguity branch — a case passing for a
  # reason that was not its own, which is this suite's own recorded failure mode.
  # The `local` declarations are SPLIT: `local repo="$1" rec="$repo/..."` reads $repo before
  # the assignment takes effect and dies under `set -u` — the same trap `c_repo` above
  # documents, measured again here (it exited 127 and the whole section short-circuited
  # SILENTLY, which is the case-floor's own subject one function over).
  local repo="$1"
  local prog="$2"
  local rec="$repo/.review-stage/issue-3751/c.stage"
  [ -f "$rec.pristine" ] || cp "$rec" "$rec.pristine" || return 1
  LC_ALL=C awk "$prog" "$rec.pristine" >"$rec.new" 2>/dev/null || return 1
  mv -f "$rec.new" "$rec" || return 1
}
SHAPE_REPO=$(c_repo shape design) || SHAPE_REPO=""
if [ -n "$SHAPE_REPO" ]; then
  if (cd "$SHAPE_REPO" && bash "$NEUTRAL_DIR/review-stage.sh" open c --issue 3751 \
    --agent spec-auditor >/dev/null 2>&1) &&
    printf 'result: PASS\n\n## Findings\n\nnone.\n' \
      >"$(SR_REPORT "$SHAPE_REPO" 3751 c)"; then
    ok "shape fixture: a PASSING c stage was opened for the unparsable-record cases"
  else
    bad "shape fixture: could not open the stage — the cases would be vacuous"
    SHAPE_REPO=""
  fi
fi
if [ -n "$SHAPE_REPO" ]; then
  # (a) NO head-sha AT ALL — the shape a record written before this field existed has. It must
  #     refuse, not be waved through: that is the whole "an older record must not certify" half.
  if sr_plant "$SHAPE_REPO" '!/^head-sha:/ { print }' &&
    run_in_repo "$SHAPE_REPO" 2 \
      "record-shape: a stage record with NO head-sha REFUSES (an older record cannot certify)" \
      --c-verdict AUTO; then
    case "$OUT" in
      *"head-sha"*) ok "record-shape/absent: the refusal names the missing field" ;;
      *) bad "record-shape/absent: the refusal must name head-sha (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"--force"*) ok "record-shape/absent: the refusal names the remedy (re-open the stage)" ;;
      *) bad "record-shape/absent: the refusal must name the remedy (got: $OUT)" ;;
    esac
  fi
  # (b) TWO head-sha lines — two answers to one question. Picking one is the first-wins rule
  #     this file refuses everywhere else. The record's OWN, VALID sha stays FIRST and the
  #     planted one is appended, so a first-wins reader would have PASSED this case: the
  #     refusal has to come from the COUNT, not from the value it happened to read.
  if sr_plant "$SHAPE_REPO" '{ print } END { print "head-sha: 0000000000000000000000000000000000000000" }' &&
    run_in_repo "$SHAPE_REPO" 2 \
      "record-shape: TWO head-sha lines is AMBIGUOUS and refuses" --c-verdict AUTO; then
    case "$OUT" in
      *AMBIGUOUS*) ok "record-shape/several: refused as AMBIGUOUS rather than first-wins" ;;
      *) bad "record-shape/several: must refuse as AMBIGUOUS (got: $OUT)" ;;
    esac
  fi
  # (c) A NON-SHA VALUE — `unresolved` is what `open` records where HEAD is unborn, and it is
  #     an honest non-measurement, not a binding.
  if sr_plant "$SHAPE_REPO" '!/^head-sha:/ { print } END { print "head-sha: unresolved" }' &&
    run_in_repo "$SHAPE_REPO" 2 \
      "record-shape: a head-sha that is not a 40-hex sha REFUSES by name" --c-verdict AUTO; then
    case "$OUT" in
      *unresolved*) ok "record-shape/unparsable: the refusal quotes the value it could not read" ;;
      *) bad "record-shape/unparsable: the refusal must quote the value (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"PREMERGE: C-VERDICT PASS"*)
        bad "record-shape/unparsable: an unreadable binding certified the merge" ;;
      *) ok "record-shape/unparsable: no PASS token is emitted for an unreadable binding" ;;
    esac
  fi
  # (d) AN UNREADABLE `report-nonce:` — the field that names WHICH REPORT this stage's
  #     verdict lives in (#3751 round 5, J1). The AUTO path does not read the report itself; it
  #     asks `review-stage.sh verdict`, which refuses to derive a path it cannot compute. So the
  #     refusal arrives as a NON-PASSING TOKEN rather than as a stage-binding failure — asserted
  #     here because this is the seam between the two scripts, and a fallback to the bare name
  #     would have let a superseded report certify a stage that has moved on.
  if sr_plant "$SHAPE_REPO" '{ sub(/^report-nonce:.*/, "report-nonce: ../../nope"); print }' &&
    run_in_repo "$SHAPE_REPO" 2 \
      "record-shape: an unreadable report-nonce REFUSES (the report cannot be located)" \
      --c-verdict AUTO; then
    case "$OUT" in
      *"stage record unreadable"*) ok "record-shape/nonce: the refusal names the STAGE RECORD as the unreadable half" ;;
      *) bad "record-shape/nonce: the refusal must name the record defect (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"PREMERGE: C-VERDICT PASS"*)
        bad "record-shape/nonce: a stage whose report could not be located certified the merge" ;;
      *) ok "record-shape/nonce: no PASS token is emitted when the report cannot be located" ;;
    esac
    case "$OUT" in
      *"review-stage.sh open c --issue"*) ok "record-shape/nonce: and the refusal names a concrete remedy" ;;
      *) bad "record-shape/nonce: the refusal must name a remedy (got: $OUT)" ;;
    esac
  fi
fi

# --- 44g: THE DIFFERENTIAL — two readers, one shared table (round 3, G2) ------
# TWO READERS READ THE SAME SHAPE. `review-stage.sh`'s `classify_report` locates the report's
# `result:` record; `premerge-assert.sh`'s `_c_verdict_awk` + `c_parse_verdict` locates the
# verdict stream's `REVIEW-STAGE:` record. Neither reads the other's file, but BOTH answer the
# same three questions: is the candidate line at COLUMN ZERO, is there EXACTLY ONE of them, and
# is the token in the CLOSED set.
#
# THEY DIVERGED, TWICE IN TWO ROUNDS, and each time a reviewer named one side: round 2 (B1) that
# `classify_report` was not column-zero anchored while `_c_verdict_awk` was; round 3 (G2) that
# `_c_verdict_awk` COUNTED its anchored lines and refused several while `classify_report`
# first-won. Patching whichever side the reviewer happened to name is what let the second
# divergence exist, so the rule is now MECHANICALLY CHECKED rather than maintained by care:
# one table, both readers, agreement asserted per row.
#
# THE DISPOSITION IS THE LOCATION VERDICT, NOT THE MERGE OUTCOME — three values, because
# "accept/refuse" would fuse two different questions and the readers legitimately differ on the
# second. NO-RECORD: the reader found zero, or several, column-zero candidates. TOKEN-REJECTED:
# it located exactly one and the token is not in the closed set. READ: it located one and the
# token is in the set. An output matching none of those is UNCLASSIFIED and is a FAILURE — a
# positive verdict requires an affirmative measurement, so a case that cannot be read must not
# be counted as agreement.
#
# EXPECTED IS ASSERTED BESIDE AGREEMENT. Agreement alone is satisfiable by BOTH readers being
# wrong in the same way, which is exactly the state this section exists to detect.
diff_disp_a() {
  case "$1" in
    *"RESULT: PASS "* | *"RESULT: FINDINGS "*)       printf 'READ\n' ;;
    *"column-zero 'result:' lines"*)                 printf 'NO-RECORD\n' ;;
    *"no 'result:' line"*)                           printf 'NO-RECORD\n' ;;
    *"unrecognised result token"*)                   printf 'TOKEN-REJECTED\n' ;;
    *)                                               printf 'UNCLASSIFIED\n' ;;
  esac
}
diff_disp_b() {
  if [ "${2:-1}" -eq 0 ]; then printf 'READ\n'; return 0; fi
  case "$1" in
    *"holds NO verdict line"*)              printf 'NO-RECORD\n' ;;
    *"verdict lines"*AMBIGUOUS*)            printf 'NO-RECORD\n' ;;
    *"verdict token:"*)                     printf 'TOKEN-REJECTED\n' ;;
    # THE ESCAPE REFUSAL IS A TOKEN REJECTION (round 15, U2): the line was LOCATED and the
    # complaint is that the bytes it carries are not the token they would be read as. Mapped
    # here rather than left UNCLASSIFIED so this section can assert AGREEMENT with reader A,
    # which reports the same shape as `unrecognised result token 'PA?[31mSS'`.
    *"contains an ANSI ESCAPE SEQUENCE"*)   printf 'TOKEN-REJECTED\n' ;;
    *)                                      printf 'UNCLASSIFIED\n' ;;
  esac
}

# THE TABLE. One row per adversarial shape, each spelled for BOTH readers by
# `diff_row_body_a` / `diff_row_line_b` — the SAME shape, in each reader's own grammar. Written
# with printf so a row can plant something neither producer would ever emit, which is the point.
DIFF_ROWS="plain indented several zero crlf ansi token-junk fenced globish"
DIFF_EXPECT_plain=READ
DIFF_EXPECT_indented=NO-RECORD
DIFF_EXPECT_several=NO-RECORD
DIFF_EXPECT_zero=NO-RECORD
DIFF_EXPECT_crlf=READ
# THE ROW THE TWO READERS ACTUALLY DISAGREED ON (round 15, U2). Measured on the shipped code
# BEFORE the fix: `classify_report` reported
# `NOT-RUN (report ungrammatical: unrecognised result token 'PA?[31mSS')` — fail-closed, because
# `one_line` renders the ESC as `?` — while `_c_verdict_awk` DELETED the CSI first and published
# `PASS`, so the merge gate certified a token the file does not contain. TOKEN-REJECTED is the
# agreed answer, and reaching it is what makes the two readers agree about this shape.
#
# NOTE WHAT THIS ROW IS *NOT*: the `crlf` row above stays READ at BOTH readers, deliberately. A
# trailing CR is separator WHITESPACE — a `\r$` strip removes one byte at end of line, where
# nothing follows, so it can SEPARATE but never JOIN — exactly as a trailing TAB or trailing
# SPACES do (measured: all three report `RESULT: PASS` from `classify_report`). A CSI deletion
# removes bytes from the MIDDLE and JOINS two runs the file keeps apart. Refusing the CR here
# would have been a UNILATERAL change to one of two readers of one shape, which is the very
# divergence this section exists to detect.
DIFF_EXPECT_ansi=TOKEN-REJECTED
DIFF_EXPECT_token_junk=TOKEN-REJECTED
DIFF_EXPECT_fenced=NO-RECORD
DIFF_EXPECT_globish=TOKEN-REJECTED
diff_expect() {
  # Row names carry '-', which is not a shell identifier character, so the lookup translates it.
  local key
  key="DIFF_EXPECT_$(printf '%s' "$1" | tr '-' '_')"
  eval "printf '%s\n' \"\${$key}\""
}
diff_row_body_a() {
  case "$1" in
    plain)      printf 'result: PASS\n\nreviewed the whole diff.\n' ;;
    indented)   printf '  result: PASS\n\nreviewed the whole diff.\n' ;;
    several)    printf 'result: PASS\n\nan earlier round.\n\nresult: FINDINGS\n\na later one.\n' ;;
    zero)       printf '# a report with prose only\n\nnothing recordable here.\n' ;;
    crlf)       printf 'result: PASS\r\n\r\nreviewed the whole diff.\r\n' ;;
    ansi)       printf 'result: PA\033[31mSS\n\na token spliced by a colour escape.\n' ;;
    token-junk) printf 'result: PASSNOW\n\nan invented token.\n' ;;
    fenced)     printf '```\nresult: PASS\n```\n\nresult: PASS\n' ;;
    globish)    printf 'result: *\n\na glob where a token belongs.\n' ;;
  esac
}
diff_row_line_b() {
  local pfx='REVIEW-STAGE: c RESULT:' sfx='elapsed=1 deadline=1800 agent=spec-auditor report=/p/c.md'
  case "$1" in
    plain)      printf '%s PASS %s\n' "$pfx" "$sfx" ;;
    indented)   printf '  %s PASS %s\n' "$pfx" "$sfx" ;;
    several)    printf '%s PASS %s\n%s FINDINGS %s\n' "$pfx" "$sfx" "$pfx" "$sfx" ;;
    zero)       printf 'a capture holding prose only, with no anchored line.\n' ;;
    crlf)       printf '%s PASS %s\r\n' "$pfx" "$sfx" ;;
    ansi)       printf '%s PA\033[31mSS %s\n' "$pfx" "$sfx" ;;
    token-junk) printf '%s PASSNOW %s\n' "$pfx" "$sfx" ;;
    fenced)     printf '```\n%s PASS %s\n```\n%s PASS %s\n' "$pfx" "$sfx" "$pfx" "$sfx" ;;
    globish)    printf '%s * %s\n' "$pfx" "$sfx" ;;
  esac
}

# A CASE FLOOR ON THE TABLE ITSELF (#3544's lesson): an emptied or shrunken table yields a loop
# that runs fewer times and reports `failed: 0`, which is a green tally over a shrunken suite.
DIFF_ROW_COUNT=$(printf '%s\n' $DIFF_ROWS | grep -c .)
if [ "$DIFF_ROW_COUNT" -ge 9 ]; then
  ok "differential: the shared table holds $DIFF_ROW_COUNT adversarial shapes (floor 9)"
else
  bad "differential: the shared table holds only $DIFF_ROW_COUNT rows, below the floor of 9 — a row was lost and agreement over an empty table is not agreement"
fi

DIFF_REPO="$T/diff-readers"
DIFF_OK=""
if mkdir -p "$DIFF_REPO" && git init -q "$DIFF_REPO" >/dev/null 2>&1 &&
  printf '.review-stage/\n' >"$DIFF_REPO/.gitignore"; then
  DIFF_OK=1
  ok "differential: a scratch worktree for the report reader was built"
else
  bad "differential: could not build the scratch worktree — the section would be vacuous"
fi

if [ -n "$DIFF_OK" ]; then
  DIFF_N=900
  for DROW in $DIFF_ROWS; do
    DIFF_N=$((DIFF_N + 1))
    DEXP=$(diff_expect "$DROW")
    # READER A — review-stage.sh's classify_report, through the shipped subcommand.
    if ! (cd "$DIFF_REPO" && bash "$NEUTRAL_DIR/review-stage.sh" open c --issue "$DIFF_N" \
      --agent spec-auditor >/dev/null 2>&1); then
      bad "differential/$DROW: could not open the stage — this row is vacuous"
      continue
    fi
    diff_row_body_a "$DROW" >"$(SR_REPORT "$DIFF_REPO" "$DIFF_N" c)"
    DOUT_A=$(cd "$DIFF_REPO" && bash "$NEUTRAL_DIR/review-stage.sh" verdict c \
      --issue "$DIFF_N" 2>&1) || true
    DA=$(diff_disp_a "$DOUT_A")
    # READER B — premerge-assert.sh's _c_verdict_awk + c_parse_verdict, through --c-verdict.
    DFILE="$T/diff-verdict-$DROW.txt"
    diff_row_line_b "$DROW" >"$DFILE"
    DOUT_B=$(PATH="$BIN:$PATH" bash "$NEUTRAL_ASSERT" 2421 "$CERTIFIED" "$GOOD" \
      --c-verdict "$DFILE" 2>&1)
    DRC_B=$?
    DB=$(diff_disp_b "$DOUT_B" "$DRC_B")
    if [ "$DA" = "$DB" ]; then
      ok "differential/$DROW: the two readers AGREE ($DA)"
    else
      bad "differential/$DROW: the readers DISAGREE — classify_report says $DA, _c_verdict_awk says $DB. Two readers of one shape must not hold two opinions (A: $DOUT_A) (B: $DOUT_B)"
    fi
    if [ "$DA" = "$DEXP" ] && [ "$DB" = "$DEXP" ]; then
      ok "differential/$DROW: and both reach the EXPECTED disposition ($DEXP)"
    else
      bad "differential/$DROW: expected $DEXP, got A=$DA B=$DB — agreement on a WRONG answer is not correctness (A: $DOUT_A) (B: $DOUT_B)"
    fi
  done
fi

# --- 44i: AUTO must rest on ONE OBSERVATION of the stage record (round 9, N2) ---
# THE FINDING (roborev job 382, N2). AUTO validated `head-sha` from the stage record
# (`c_assert_stage_binds_certified`, round 3's G1) and then invoked `review-stage.sh verdict`,
# which RE-READS that record to find which report is current (the nonce, round 6's K2). Two reads
# of one record are TWO DIFFERENT FACTS: an atomic replacement in between makes the ACCEPTED
# verdict come from a different GENERATION of the stage — and potentially a different commit —
# than the `head-sha` that was validated, which defeats G1 and the nonce in combination, and that
# pair is what stops a stale audit certifying a new tree.
#
# THE INTERLEAVING IS SIMULATED, NOT RACED: one line is injected into a SCRATCH COPY of the assert
# immediately before it invokes `review-stage.sh verdict`, so the replacement is deterministic and
# cannot flake. The ARTIFACT is substituted (#3312's corollary for tests); there is no seam.
N2_DIR="$T/n2/flow"
mkdir -p "$N2_DIR"
n2_ok=1
cp "$ASSERT" "$N2_DIR/premerge-assert.sh" 2>/dev/null || n2_ok=0
cp "$SCRIPT_DIR/../flow/review-stage.sh" "$N2_DIR/review-stage.sh" 2>/dev/null || n2_ok=0
printf '%s\n' "$NEUTRAL_ADV" >"$N2_DIR/base-staleness.sh" 2>/dev/null || n2_ok=0
chmod +x "$N2_DIR/base-staleness.sh" 2>/dev/null || true
# Every list travels through ENVIRON: `awk -v` performs ESCAPE PROCESSING on its value, which
# round 7 measured turning a `\n` in an injected line into a real newline.
N2_ANCHOR='out=$(bash "$rs" verdict "$C_STAGE_KIND" --issue "$issue"'
n2_build() {
  local dest="$1" inj="$2"
  [ "$n2_ok" -eq 1 ] || return 1
  N2_A="$N2_ANCHOR" N2_I="$inj" LC_ALL=C awk '
    BEGIN { a = ENVIRON["N2_A"]; inj = ENVIRON["N2_I"]; done = 0 }
    index($0, a) > 0 && done == 0 { print inj; done = 1 }
    { print }
  ' "$N2_DIR/premerge-assert.sh" >"$dest" 2>/dev/null || return 1
  [ -s "$dest" ] || return 1
  LC_ALL=C grep -q 'N2_INTERLEAVE' "$dest" || return 1
  return 0
}
# n2_run <script> <repo> <want> <desc> — `run_in_repo`, against a named scratch assert.
n2_run() {
  local script="$1" d="$2" want="$3" desc="$4" sha f
  sha=$(git -C "$d" rev-parse HEAD 2>/dev/null) || sha=""
  if [ -z "$sha" ]; then bad "$desc: could not resolve the fixture HEAD"; return 1; fi
  f="$d/../gate-n2-$(basename "$script").txt"
  emit_summary_block "$FULL_S" "$FULL_E" "-" \
    "$(printf '%.7s' "$sha")" "$(printf '%.12s' "$sha")" PASS PASS >"$f"
  OUT=$(cd "$d" && PATH="$BIN:$PATH" MOCK_GH_OUT="$sha OPEN" MOCK_GH_FAIL=0 \
    bash "$script" 2421 "$sha" "$f" --c-verdict AUTO 2>&1)
  RC=$?
  if [ "$RC" -ne "$want" ]; then
    bad "$desc (exit $RC, wanted $want)"
    printf '     output: %s\n' "$OUT"
    return 1
  fi
  return 0
}

# n2_restore — put the stage back to "opened at HEAD, report records PASS". EVERY case starts
# from it, because a case that leaves the DECOY installed makes the next one refuse at the
# head-sha BINDING (the decoy's sha is forty zeros) and never reach the read the case is about —
# which is exactly the cross-case leakage round 3 recorded in `sr_plant`, one section over.
n2_restore() {
  (cd "$N2_REPO" && bash "$N2_DIR/review-stage.sh" open c --issue 3751 \
    --agent spec-auditor --force >/dev/null 2>&1) || return 1
  printf 'result: PASS\n\n## Findings\n\nnone.\n' >"$(SR_REPORT "$N2_REPO" 3751 c)" || return 1
  return 0
}

N2_REPO=$(c_repo n2 design) || N2_REPO=""
N2_REC="$N2_REPO/.review-stage/issue-3751/c.stage"
N2_DECOY="$N2_REPO/.review-stage/issue-3751/c.decoy"
N2_ZERO=0000000000000000000000000000000000000000
if [ -n "$N2_REPO" ] && [ "$n2_ok" -eq 1 ] &&
  (cd "$N2_REPO" && bash "$N2_DIR/review-stage.sh" open c --issue 3751 \
    --agent spec-auditor >/dev/null 2>&1) &&
  printf 'result: PASS\n\n## Findings\n\nnone.\n' >"$(SR_REPORT "$N2_REPO" 3751 c)"; then
  ok "n2 fixture: a PASSING c stage was opened at the fixture head"
else
  bad "n2 fixture: could not open the stage — every case below would be vacuous"
  N2_REPO=""
fi
if [ -n "$N2_REPO" ]; then
  # THE DECOY IS A SECOND GENERATION OF THE SAME STAGE: another nonce, another report, and a
  # head-sha that is NOT the certified commit. It is what an `open --force` (or a hand edit)
  # leaves behind, and it is the bait the second read would follow.
  LC_ALL=C sed -e "s|^head-sha:.*|head-sha: $N2_ZERO|" -e 's|^report-nonce:.*|report-nonce: decoygenerationB|' \
    "$N2_REC" >"$N2_DECOY" 2>/dev/null || true
  printf 'result: PASS\n\n## Findings\n\nan audit of a DIFFERENT tree.\n' \
    >"$N2_REPO/.review-stage/issue-3751/c.decoygenerationB.md" 2>/dev/null || true
  # THE BAIT MUST BE VALID, or a refusal below could come from the decoy being broken rather than
  # from the interleaving being caught.
  if LC_ALL=C grep -q "^head-sha: $N2_ZERO\$" "$N2_DECOY" 2>/dev/null &&
    LC_ALL=C grep -q '^report-nonce: decoygenerationB$' "$N2_DECOY" 2>/dev/null &&
    LC_ALL=C grep -q '^result: PASS$' "$N2_REPO/.review-stage/issue-3751/c.decoygenerationB.md" 2>/dev/null; then
    ok "n2 fixture: the decoy generation is VALID BAIT (its own nonce, its own PASSING report, a head-sha that is not the certified commit)"
  else
    bad "n2 fixture: the decoy generation is not valid bait, so the cases below prove nothing"
  fi

  # (a) THE DEFECT: the record is REPLACED between the validated read and the verdict read.
  if n2_restore; then
    ok "n2/swap: the stage was restored to opened-at-HEAD before the case (no leakage from a sibling)"
  else
    bad "n2/swap: the stage could not be restored, so this case starts from an unknown state"
  fi
  # THE PLANT READS THE ROOT FROM THE GLOBAL, exactly as the shipped code does (#3751 round 18,
  # X1). `c_stage_root` no longer PRINTS — it assigns `C_STAGE_ROOT` — so a `$(c_stage_root)`
  # here would expand to the EMPTY string, `cp` would fail into its `|| true`, and this case
  # would report a green over a plant that never landed. That is the harness-that-never-reached-
  # the-code class the fixture assert below exists for; it fired for real when the resolver
  # changed, which is the assert paying out.
  if n2_build "$N2_DIR/swap.sh" '    c_stage_root; cp "$C_STAGE_ROOT/.review-stage/issue-$issue/$C_STAGE_KIND.decoy" "$C_STAGE_ROOT/.review-stage/issue-$issue/$C_STAGE_KIND.stage" 2>/dev/null || true   # N2_INTERLEAVE'; then
    ok "n2/swap: the interleaving plant landed in the scratch assert (asserted, not assumed)"
  else
    bad "n2/swap: the plant did NOT land, so this case proves nothing"
  fi
  if n2_run "$N2_DIR/swap.sh" "$N2_REPO" 2 \
    "n2/swap: a verdict read from a DIFFERENT generation of the record must NOT certify"; then
    case "$OUT" in
      *"PREMERGE: NO-C-VERDICT"*) ok "n2/swap: refused under the NO-C-VERDICT verdict" ;;
      *) bad "n2/swap: must refuse with NO-C-VERDICT (got: $OUT)" ;;
    esac
    case "$OUT" in
      *CHANGED*) ok "n2/swap: the refusal names the interleaving, not a downstream symptom" ;;
      *) bad "n2/swap: the refusal must name the record change (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"PREMERGE: C-VERDICT PASS"*)
        bad "n2/swap: a verdict from an unvalidated generation certified the merge — the N2 defect is live" ;;
      *) ok "n2/swap: no PASS token is emitted for a verdict read from an unvalidated record" ;;
    esac
  fi

  # (b) THE RECORD VANISHES in the same window. This refuses either way — with the record gone
  #     `verdict` reports NOT-RUN — so what is asserted is the CAUSE: the refusal must name the
  #     interleaving it observed, not the downstream symptom, because the operator action differs
  #     ("a peer replaced your stage" is not "your auditor produced nothing").
  if n2_restore; then
    ok "n2/unlink: the stage was restored before the case"
  else
    bad "n2/unlink: the stage could not be restored"
  fi
  if n2_build "$N2_DIR/unlink.sh" '    c_stage_root; rm -f "$C_STAGE_ROOT/.review-stage/issue-$issue/$C_STAGE_KIND.stage" 2>/dev/null || true   # N2_INTERLEAVE'; then
    ok "n2/unlink: the plant landed"
  else
    bad "n2/unlink: the plant did NOT land"
  fi
  if n2_run "$N2_DIR/unlink.sh" "$N2_REPO" 2 \
    "n2/unlink: a record REMOVED in the window refuses"; then
    case "$OUT" in
      *CHANGED*) ok "n2/unlink: and it names the interleaving rather than the NOT-RUN it produced" ;;
      *) bad "n2/unlink: the refusal must name the record change (got: $OUT)" ;;
    esac
  fi
  if n2_restore; then
    ok "n2/CONTROL: the stage was restored before the control"
  else
    bad "n2/CONTROL: the stage could not be restored"
  fi
  # (c) CONTROL — THE SAME SCRATCH MACHINERY WITH A NO-OP INJECTION STILL CERTIFIES. Without it
  #     both refusals are satisfiable by a scratch copy that is simply broken, or by a check that
  #     refuses every AUTO run.
  if n2_build "$N2_DIR/noop.sh" '    : N2_INTERLEAVE no-op'; then
    ok "n2/CONTROL: the no-op plant landed at the same anchor"
  else
    bad "n2/CONTROL: the no-op plant did NOT land"
  fi
  if n2_run "$N2_DIR/noop.sh" "$N2_REPO" 0 \
    "n2/CONTROL: an UNDISTURBED record still certifies (the refusals come from the interleaving)"; then
    case "$OUT" in
      *"PREMERGE: C-VERDICT PASS"*) ok "n2/CONTROL: and it still reports C-VERDICT PASS" ;;
      *) bad "n2/CONTROL: must report C-VERDICT PASS (got: $OUT)" ;;
    esac
    case "$OUT" in
      *CHANGED*) bad "n2/CONTROL: it claimed an interleaving that did not happen (got: $OUT)" ;;
      *) ok "n2/CONTROL: and claims no interleaving that did not happen" ;;
    esac
  fi
fi
# (d) STRUCTURAL — THE RE-OBSERVATION IS INSIDE THE WINDOW IT CERTIFIES: after the verdict is
#     obtained and BEFORE it is parsed into a token, so an unvalidated observation never produces
#     a token at all. A check placed after `c_parse_verdict` could only report.
N2_VERDICT_LN="$(LC_ALL=C grep -n 'out=\$(bash "\$rs" verdict' "$ASSERT" | LC_ALL=C head -1 | cut -d: -f1)"
N2_PARSE_LN="$(LC_ALL=C grep -n 'c_parse_verdict text "\$out"' "$ASSERT" | LC_ALL=C head -1 | cut -d: -f1)"
N2_RECHECK_LN="$(LC_ALL=C grep -n 'c_assert_stage_record_unchanged' "$ASSERT" | LC_ALL=C tail -1 | cut -d: -f1)"
if [ -n "$N2_VERDICT_LN" ] && [ -n "$N2_PARSE_LN" ] && [ -n "$N2_RECHECK_LN" ] &&
  [ "$N2_RECHECK_LN" -gt "$N2_VERDICT_LN" ] && [ "$N2_RECHECK_LN" -lt "$N2_PARSE_LN" ]; then
  ok "n2/structural: the re-observation sits between the verdict read and the token parse (lines $N2_VERDICT_LN < $N2_RECHECK_LN < $N2_PARSE_LN)"
else
  bad "n2/structural: the re-observation is NOT between the verdict read and the token parse (verdict=$N2_VERDICT_LN recheck=$N2_RECHECK_LN parse=$N2_PARSE_LN)"
fi
# AND THE VALIDATED OBSERVATION IS THE ONE THE head-sha WAS TAKEN FROM — one read of the file,
# every question asked of that value. A second `<"$sfile"` read for the head-sha would make the
# comparison below compare something the binding never saw.
# NAME-AGNOSTIC ON PURPOSE (#3751 round 10). It used to name `_c_stage_head_awk` literally, so
# RENAMING that function — which round 10 did, when it grew a second field — would have made this
# guard match nothing and pass VACUOUSLY. The pattern is the SHAPE (`<parser> <"$sfile"`), which a
# rename cannot escape.
if [ "$(LC_ALL=C grep -cE '_awk[[:space:]]*<[[:space:]]*"\$sfile"' "$ASSERT" || true)" -eq 0 ] &&
  LC_ALL=C grep -q 'C_STAGE_RECORD=' "$ASSERT"; then
  ok "n2/structural: head-sha is parsed from the CAPTURED observation, not from a second read of the file"
else
  bad "n2/structural: the head-sha is still parsed by re-reading the record, so the captured observation is a different fact"
fi

# --- 44j: an ABA replacement must not defeat the byte comparison (round 10, P2) ---
# THE FINDING (roborev job 384, P2, premerge-assert.sh:1320). Round 9's N2 captures the stage
# record ONCE, validates `head-sha` from that capture, lets `review-stage.sh verdict` re-read the
# record to pick which report is current, and then re-compares the record's BYTES. An ABA
# replacement defeats that comparison: the record goes from the validated generation A to a
# foreign generation B while `verdict` reads B, and back to A before the comparison. Both
# observations are byte-identical, the check passes, and the ACCEPTED verdict came from B —
# possibly stale, possibly another lane's, possibly bound to a different commit. Equality of two
# observations is not identity of the thing observed at a third instant.
#
# THE INTERLEAVE IS SIMULATED, NOT RACED. Two lines are injected into a SCRATCH COPY of the
# assert — one immediately BEFORE it invokes `review-stage.sh verdict`, one immediately AFTER —
# so the A->B->A sequence is deterministic, cannot flake, and makes no claim about timing. The
# ARTIFACT is substituted (#3312's corollary for tests); there is no settable seam. Section 44i's
# single-injection builder cannot express this case: the restore has to land after the callee ran.
P2_DIR="$T/p2/flow"
mkdir -p "$P2_DIR"
p2_ok=1
cp "$ASSERT" "$P2_DIR/premerge-assert.sh" 2>/dev/null || p2_ok=0
cp "$SCRIPT_DIR/../flow/review-stage.sh" "$P2_DIR/review-stage.sh" 2>/dev/null || p2_ok=0
printf '%s\n' "$NEUTRAL_ADV" >"$P2_DIR/base-staleness.sh" 2>/dev/null || p2_ok=0
chmod +x "$P2_DIR/base-staleness.sh" 2>/dev/null || true
# Every injected line travels through ENVIRON, never `awk -v`, which performs ESCAPE PROCESSING
# on its value (round 7 measured a `\n` in an injected line becoming a real newline).
P2_ANCHOR='out=$(bash "$rs" verdict "$C_STAGE_KIND" --issue "$issue"'
# FROM THE GLOBAL, NOT A CAPTURE (#3751 round 18, X1): `c_stage_root` assigns and prints
# nothing, so `$(c_stage_root)` would inject an EMPTY root and the plant would silently not land.
# Each injected line calls the resolver first; see `P2_SD_SET`.
P2_SD_SET='c_stage_root; '
P2_SD='"$C_STAGE_ROOT/.review-stage/issue-$issue/$C_STAGE_KIND'
# p2_build <dest> <pre> <post> — the same anchor as 44i, but a line on EACH side of it.
p2_build() {
  local dest="$1" pre="$2" post="$3"
  [ "$p2_ok" -eq 1 ] || return 1
  P2_A="$P2_ANCHOR" P2_PRE="$pre" P2_POST="$post" LC_ALL=C awk '
    BEGIN { a = ENVIRON["P2_A"]; pre = ENVIRON["P2_PRE"]; post = ENVIRON["P2_POST"]; done = 0 }
    index($0, a) > 0 && done == 0 { print pre; print $0; print post; done = 1; next }
    { print }
  ' "$P2_DIR/premerge-assert.sh" >"$dest" 2>/dev/null || return 1
  [ -s "$dest" ] || return 1
  LC_ALL=C grep -q 'P2_ABA_PRE' "$dest" || return 1
  LC_ALL=C grep -q 'P2_ABA_POST' "$dest" || return 1
  return 0
}
# p2_run <script> <repo> <want> <desc> — `run_in_repo` against a named scratch assert.
p2_run() {
  local script="$1" d="$2" want="$3" desc="$4" sha f
  sha=$(git -C "$d" rev-parse HEAD 2>/dev/null) || sha=""
  if [ -z "$sha" ]; then bad "$desc: could not resolve the fixture HEAD"; return 1; fi
  f="$d/../gate-p2-$(basename "$script").txt"
  emit_summary_block "$FULL_S" "$FULL_E" "-" \
    "$(printf '%.7s' "$sha")" "$(printf '%.12s' "$sha")" PASS PASS >"$f"
  OUT=$(cd "$d" && PATH="$BIN:$PATH" MOCK_GH_OUT="$sha OPEN" MOCK_GH_FAIL=0 \
    bash "$script" 2421 "$sha" "$f" --c-verdict AUTO 2>&1)
  RC=$?
  if [ "$RC" -ne "$want" ]; then
    bad "$desc (exit $RC, wanted $want)"
    printf '     output: %s\n' "$OUT"
    return 1
  fi
  return 0
}
# p2_restore — back to "opened at HEAD, current report records PASS", asserted per case. A case
# that leaves the DECOY installed makes the next one refuse at the head-sha BINDING and never
# reach the read it is about — the cross-case leakage round 3 recorded in `sr_plant`.
p2_restore() {
  (cd "$P2_REPO" && bash "$P2_DIR/review-stage.sh" open c --issue 3751 \
    --agent spec-auditor --force >/dev/null 2>&1) || return 1
  printf 'result: PASS\n\n## Findings\n\nnone.\n' >"$(SR_REPORT "$P2_REPO" 3751 c)" || return 1
  rm -f "$P2_REPO/.review-stage/issue-3751/c.md" 2>/dev/null || true
  return 0
}
P2_REPO=$(c_repo p2 design) || P2_REPO=""
P2_REC="$P2_REPO/.review-stage/issue-3751/c.stage"
P2_DECOY="$P2_REPO/.review-stage/issue-3751/c.decoy"
P2_ZERO=0000000000000000000000000000000000000000
if [ -n "$P2_REPO" ] && [ "$p2_ok" -eq 1 ] && p2_restore; then
  ok "p2 fixture: a PASSING c stage was opened at the fixture head"
else
  bad "p2 fixture: could not open the stage — every case below would be vacuous"
  P2_REPO=""
fi
if [ -n "$P2_REPO" ]; then
  # THE DECOY IS A SECOND GENERATION: its own nonce, its own PASSING report, and a head-sha that
  # is NOT the certified commit. It is what an `open --force` (or a hand edit) leaves behind, and
  # it is the bait the second read follows.
  LC_ALL=C sed -e "s|^head-sha:.*|head-sha: $P2_ZERO|" \
    -e 's|^report-nonce:.*|report-nonce: decoygenerationB|' \
    "$P2_REC" >"$P2_DECOY" 2>/dev/null || true
  printf 'result: PASS\n\n## Findings\n\nan audit of a DIFFERENT tree.\n' \
    >"$P2_REPO/.review-stage/issue-3751/c.decoygenerationB.md" 2>/dev/null || true
  if LC_ALL=C grep -q "^head-sha: $P2_ZERO\$" "$P2_DECOY" 2>/dev/null &&
    LC_ALL=C grep -q '^report-nonce: decoygenerationB$' "$P2_DECOY" 2>/dev/null &&
    LC_ALL=C grep -q '^result: PASS$' "$P2_REPO/.review-stage/issue-3751/c.decoygenerationB.md" 2>/dev/null; then
    ok "p2 fixture: the decoy generation is VALID BAIT (its own nonce, its own PASSING report, a head-sha that is not the certified commit)"
  else
    bad "p2 fixture: the decoy generation is not valid bait, so the cases below prove nothing"
  fi

  # (a) THE DEFECT: A -> B -> A. The record is replaced for exactly the span in which
  #     `review-stage.sh verdict` reads it, and restored before the byte comparison.
  if p2_restore; then
    ok "p2/aba: the stage was restored to opened-at-HEAD before the case (no leakage from a sibling)"
  else
    bad "p2/aba: the stage could not be restored, so this case starts from an unknown state"
  fi
  if p2_build "$P2_DIR/aba.sh" \
    "    $P2_SD_SET cp $P2_SD.stage\" $P2_SD.genA\" 2>/dev/null && cp $P2_SD.decoy\" $P2_SD.stage\" 2>/dev/null || true   # P2_ABA_PRE" \
    "    $P2_SD_SET cp $P2_SD.genA\" $P2_SD.stage\" 2>/dev/null || true   # P2_ABA_POST"; then
    ok "p2/aba: both halves of the A->B->A plant landed in the scratch assert (asserted, not assumed)"
  else
    bad "p2/aba: the plant did NOT land, so this case proves nothing"
  fi
  if p2_run "$P2_DIR/aba.sh" "$P2_REPO" 2 \
    "p2/aba: a verdict read from a generation swapped in and BACK OUT must NOT certify"; then
    case "$OUT" in
      *"PREMERGE: NO-C-VERDICT"*) ok "p2/aba: refused under the NO-C-VERDICT verdict" ;;
      *) bad "p2/aba: must refuse with NO-C-VERDICT (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"DIFFERENT generation"*) ok "p2/aba: the refusal names the GENERATION mismatch" ;;
      *) bad "p2/aba: the refusal must name the generation mismatch (got: $OUT)" ;;
    esac
    case "$OUT" in
      *decoygenerationB*) ok "p2/aba: and it names the generation the verdict actually came from" ;;
      *) bad "p2/aba: the refusal must name the foreign generation (got: $OUT)" ;;
    esac
    # THE DISCRIMINATION THAT MAKES THIS CASE ITS OWN: the byte comparison MUST have passed. If
    # the refusal said the record CHANGED, this would be section 44i's case again and the ABA
    # premise would be untested.
    case "$OUT" in
      *CHANGED*) bad "p2/aba: the byte comparison fired, so the A->B->A restore did not happen and this case is 44i's (got: $OUT)" ;;
      *) ok "p2/aba: the byte comparison did NOT fire — the two observations were identical, which is the finding" ;;
    esac
    case "$OUT" in
      *"PREMERGE: C-VERDICT PASS"*)
        bad "p2/aba: a verdict from an unvalidated generation certified the merge — the P2 defect is live" ;;
      *) ok "p2/aba: no PASS token is emitted for a verdict read from an unvalidated generation" ;;
    esac
  fi

  # (b) CONTROL — THE SAME TWO-POINT MACHINERY WITH NO-OP INJECTIONS STILL CERTIFIES. Without it
  #     the refusal above is satisfiable by a scratch copy that is simply broken, or by a check
  #     that refuses every AUTO run.
  if p2_restore; then
    ok "p2/CONTROL: the stage was restored before the control"
  else
    bad "p2/CONTROL: the stage could not be restored"
  fi
  if p2_build "$P2_DIR/noop.sh" '    : P2_ABA_PRE no-op' '    : P2_ABA_POST no-op'; then
    ok "p2/CONTROL: both no-op plants landed at the same anchor"
  else
    bad "p2/CONTROL: the no-op plants did NOT land"
  fi
  if p2_run "$P2_DIR/noop.sh" "$P2_REPO" 0 \
    "p2/CONTROL: an UNDISTURBED generation still certifies (the refusal comes from the interleave)"; then
    case "$OUT" in
      *"PREMERGE: C-VERDICT PASS"*) ok "p2/CONTROL: and it still reports C-VERDICT PASS" ;;
      *) bad "p2/CONTROL: must report C-VERDICT PASS (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"DIFFERENT generation"*) bad "p2/CONTROL: it claimed a generation mismatch that did not happen (got: $OUT)" ;;
      *) ok "p2/CONTROL: and claims no generation mismatch that did not happen" ;;
    esac
  fi

  # (c) THE LEGACY PRE-NONCE RECORD — a state that cannot be bound, and it PASSED. Strip
  #     `report-nonce:` and provide the bare `<kind>.md` report review-stage.sh still READS: the
  #     token is a genuine PASS, the head-sha binding holds, the bytes never change, and nothing
  #     names which generation answered. An unbindable audit must refuse, naming the state.
  if p2_restore &&
    printf 'result: PASS\n\n## Findings\n\nthe LEGACY bare report.\n' \
      >"$P2_REPO/.review-stage/issue-3751/c.md" &&
    LC_ALL=C awk '!/^report-nonce:/ { print }' "$P2_REC" >"$P2_REC.new" &&
    mv -f "$P2_REC.new" "$P2_REC" &&
    ! LC_ALL=C grep -q '^report-nonce:' "$P2_REC" &&
    LC_ALL=C grep -q '^head-sha: ' "$P2_REC"; then
    ok "p2/legacy: the fixture is a record with NO report-nonce and a PASSING bare report (head-sha intact)"
    if p2_run "$P2_DIR/noop.sh" "$P2_REPO" 2 \
      "p2/legacy: a PASS whose generation the record does not name must NOT certify"; then
      case "$OUT" in
        *"report-nonce"*) ok "p2/legacy: the refusal names the field that is missing" ;;
        *) bad "p2/legacy: the refusal must name report-nonce (got: $OUT)" ;;
      esac
      case "$OUT" in
        *"--force"*) ok "p2/legacy: and it names the remedy (re-open, which publishes a fresh nonce)" ;;
        *) bad "p2/legacy: the refusal must name the remedy (got: $OUT)" ;;
      esac
      case "$OUT" in
        *"PREMERGE: C-VERDICT PASS"*)
          bad "p2/legacy: an unnameable generation certified the merge" ;;
        *) ok "p2/legacy: no PASS token is emitted for a generation the record does not name" ;;
      esac
    fi
  else
    bad "p2/legacy: the fixture could not be built, so the case proves nothing"
  fi

  # (d) A DRIFTED OR FOREIGN CALLEE — the affirmative match tested WITHOUT the injection
  #     machinery. `review-stage.sh` is substituted by a STUB emitting one grammatical verdict
  #     line, so the `report=` value is chosen directly. This is what makes the check's own
  #     property visible: it is a comparison against the VALIDATED nonce, not a coupling to
  #     whatever the shipped callee happens to print. The `report=unresolved` arm is reachable
  #     ONLY this way — the shipped emitter forces NOT-RUN whenever it cannot derive a path —
  #     so without a substituted callee that arm would be untested code.
  P2_STUB_DIR="$T/p2/stub"
  mkdir -p "$P2_STUB_DIR"
  p2_stub_ok=1
  cp "$ASSERT" "$P2_STUB_DIR/premerge-assert.sh" 2>/dev/null || p2_stub_ok=0
  printf '%s\n' "$NEUTRAL_ADV" >"$P2_STUB_DIR/base-staleness.sh" 2>/dev/null || p2_stub_ok=0
  chmod +x "$P2_STUB_DIR/base-staleness.sh" 2>/dev/null || true
  # p2_stub <report-value> — a callee that reports PASS with exactly that `report=` field.
  p2_stub() {
    printf '#!/usr/bin/env bash\nprintf "REVIEW-STAGE: c RESULT: PASS elapsed=1 deadline=3600 agent=spec-auditor report=%%s\\n" %s\nexit 0\n' \
      "'$1'" >"$P2_STUB_DIR/review-stage.sh" 2>/dev/null || return 1
    [ -s "$P2_STUB_DIR/review-stage.sh" ] || return 1
    return 0
  }
  if p2_restore; then
    ok "p2/stub: the stage was restored before the substituted-callee cases"
  else
    bad "p2/stub: the stage could not be restored"
  fi
  P2_NONCE="$(LC_ALL=C sed -n 's/^report-nonce:[[:space:]]*//p' "$P2_REC" 2>/dev/null | LC_ALL=C head -1 || true)"
  if [ "$p2_stub_ok" -eq 1 ] && [ -n "$P2_NONCE" ]; then
    ok "p2/stub: the validated generation's nonce was read from the record ($P2_NONCE)"
  else
    bad "p2/stub: the scratch callee or the record's nonce is unavailable, so the cases below prove nothing"
    p2_stub_ok=0
  fi
  if [ "$p2_stub_ok" -eq 1 ]; then
    # (d1) CONTROL FIRST: the stub naming the VALIDATED generation still certifies. Without it
    #      the two refusals below are satisfiable by a check that refuses every stubbed callee.
    if p2_stub "$P2_REPO/.review-stage/issue-3751/c.$P2_NONCE.md" &&
      p2_run "$P2_STUB_DIR/premerge-assert.sh" "$P2_REPO" 0 \
        "p2/stub CONTROL: a callee naming the VALIDATED generation certifies"; then
      case "$OUT" in
        *"PREMERGE: C-VERDICT PASS"*) ok "p2/stub CONTROL: and it reports C-VERDICT PASS" ;;
        *) bad "p2/stub CONTROL: must report C-VERDICT PASS (got: $OUT)" ;;
      esac
    fi
    # (d2) A WELL-FORMED PATH WITH A FOREIGN NONCE — the match is on the GENERATION, not on the
    #      path being shaped like a report.
    if p2_stub "$P2_REPO/.review-stage/issue-3751/c.forgedgeneration9.md" &&
      p2_run "$P2_STUB_DIR/premerge-assert.sh" "$P2_REPO" 2 \
        "p2/stub forged: a well-formed report path with a FOREIGN nonce refuses"; then
      case "$OUT" in
        *"DIFFERENT generation"*) ok "p2/stub forged: the refusal names the generation mismatch" ;;
        *) bad "p2/stub forged: the refusal must name the generation mismatch (got: $OUT)" ;;
      esac
      case "$OUT" in
        *forgedgeneration9*) ok "p2/stub forged: and quotes the value it was given" ;;
        *) bad "p2/stub forged: the refusal must quote the reported path (got: $OUT)" ;;
      esac
    fi
    # (d3) `report=unresolved` BESIDE AN ACCEPTING TOKEN — review-stage.sh's honest
    #      non-measurement. It is its OWN named state, not folded into the mismatch, because the
    #      operator action differs (repair the record, not "a peer replaced your stage").
    if p2_stub unresolved &&
      p2_run "$P2_STUB_DIR/premerge-assert.sh" "$P2_REPO" 2 \
        "p2/stub unresolved: report=unresolved beside a PASS token refuses"; then
      case "$OUT" in
        *"report=unresolved"*) ok "p2/stub unresolved: the refusal names the non-measurement as its own state" ;;
        *) bad "p2/stub unresolved: the refusal must name report=unresolved (got: $OUT)" ;;
      esac
      case "$OUT" in
        *"PREMERGE: C-VERDICT PASS"*)
          bad "p2/stub unresolved: a verdict naming no generation certified the merge" ;;
        *) ok "p2/stub unresolved: no PASS token is emitted where no generation was named" ;;
      esac
    fi
  fi
fi
# (e) STRUCTURAL — WHERE THE BINDING SITS, WHAT IT READS, AND WHAT IT DOES NOT PASS.
P2_VERDICT_LN="$(LC_ALL=C grep -n 'out=\$(bash "\$rs" verdict' "$ASSERT" | LC_ALL=C head -1 | cut -d: -f1)"
P2_BIND_LN="$(LC_ALL=C grep -n 'c_assert_verdict_from_validated_generation "\$issue"' "$ASSERT" | LC_ALL=C head -1 | cut -d: -f1)"
P2_ACCEPT_LN="$(LC_ALL=C grep -n '^  case "\$C_TOKEN" in' "$ASSERT" | LC_ALL=C head -1 | cut -d: -f1)"
if [ -n "$P2_VERDICT_LN" ] && [ -n "$P2_BIND_LN" ] && [ -n "$P2_ACCEPT_LN" ] &&
  [ "$P2_BIND_LN" -gt "$P2_VERDICT_LN" ] && [ "$P2_BIND_LN" -lt "$P2_ACCEPT_LN" ]; then
  ok "p2/structural: the generation binding sits between the verdict read and the closed-grammar acceptance (lines $P2_VERDICT_LN < $P2_BIND_LN < $P2_ACCEPT_LN)"
else
  bad "p2/structural: the generation binding is NOT between the verdict read and the acceptance (verdict=$P2_VERDICT_LN bind=$P2_BIND_LN accept=$P2_ACCEPT_LN)"
fi
# THE EXPECTED NONCE COMES FROM THE ONE CAPTURE, AND `report-nonce:` IS PARSED IN EXACTLY ONE
# PLACE. A second reader of that field would be a second fact, which is the defect one level down.
# The pattern matches the awk RULE (a `/^report-nonce:` at the start of a line, comments excluded
# — a `#` line cannot match), so a second PARSER of the field reds this and a second mention of it
# in prose does not.
if [ "$(LC_ALL=C grep -cE '^[[:space:]]*/\^report-nonce:' "$ASSERT" || true)" -eq 1 ] &&
  LC_ALL=C grep -q 'C_STAGE_NONCE="\$nonce"' "$ASSERT"; then
  ok "p2/structural: report-nonce: is anchored and parsed in exactly ONE place, and published from that parse"
else
  bad "p2/structural: report-nonce: is read in more than one place, or is not published from the capture's parse"
fi
# NOTHING IS PASSED INTO review-stage.sh. Round 4 (H2) deleted `--report` so that no caller can
# name which file holds a verdict; this fix reads a value OUT of the verdict line, and an inbound
# path or nonce argument would rebuild that channel from the other end. Asserted on the ARGV.
P2_INVOKE="$(LC_ALL=C grep -h 'bash "\$rs" verdict' "$ASSERT" | LC_ALL=C head -1)"
if [ -n "$P2_INVOKE" ] &&
  [ "$P2_INVOKE" = '    out=$(bash "$rs" verdict "$C_STAGE_KIND" --issue "$issue" 2>/dev/null) || rc=$?' ]; then
  ok "p2/structural: the callee is invoked with kind and issue ONLY — no report path, no nonce, no third channel"
else
  bad "p2/structural: the verdict invocation is not the pinned argv (got: $P2_INVOKE)"
fi
# AND THE BYTE COMPARISON IS KEPT AS DEFENCE IN DEPTH, not replaced by the nonce match: it catches
# what the nonce cannot (a spawned-at/agent/deadline edit under the SAME nonce, and the record
# vanishing), and the nonce catches what it cannot. Neither contains the other.
if LC_ALL=C grep -q 'c_assert_stage_record_unchanged "\$issue"' "$ASSERT" &&
  [ "$(LC_ALL=C grep -c 'c_assert_stage_record_unchanged()' "$ASSERT" || true)" -eq 1 ]; then
  ok "p2/structural: round 9's byte re-comparison is still called (defence in depth, not superseded)"
else
  bad "p2/structural: the byte re-comparison was removed or is no longer called"
fi

# --- 44k: the ROUTING PATHSPEC must be repository-root-anchored (round 11, Q1) ---
# `git diff … -- openspec/changes/` interprets its pathspec RELATIVE TO THE CALLER'S
# CWD. Invoked from a repository SUBDIRECTORY the diff therefore came back EMPTY, a
# genuinely design-routed branch measured `NOT-APPLICABLE`, and the merge proceeded
# with NO C verdict at all — the exact escape `--c-verdict` exists to close, reached
# by nothing more exotic than the caller's working directory.
#
# `diff.relative=false` DOES NOT FIX IT, and believing it did is what left the hole:
# that option controls the OUTPUT PATH PREFIX, not how a PATHSPEC is INTERPRETED.
# Measured, in a scratch repository, from a subdirectory:
#     git diff --name-only A B -- openspec/changes/                      -> (empty)
#     git -c diff.relative=false diff … -- openspec/changes/             -> (empty)
#     git -c diff.relative=false diff … -- ':(top)openspec/changes/'     -> openspec/changes/foo/spec.md
# So BOTH are needed and neither substitutes for the other: `:(top)` anchors the
# pathspec at the root, `diff.relative=false` keeps the emitted paths root-relative
# (which the `archive/` prefix test and the slug extraction below both depend on).
#
# A FIXTURE OF ITS OWN, never 44d's, so this section cannot inherit or leak a stage
# record (the round-3 sr_plant lesson). It is design-routed AND carries a
# subdirectory to invoke from.
c_repo_subdir() {
  local d="$T/c-repo-q1-subdir"
  mkdir -p "$d"
  git init -q -b mainline "$d" >/dev/null 2>&1 || return 1
  git -C "$d" config user.email t@t
  git -C "$d" config user.name t
  printf '.review-stage/\n' >"$d/.gitignore"
  printf 'seed\n' >"$d/README.md"
  mkdir -p "$d/cqlite-core/src/storage"
  printf 'fn seed() {}\n' >"$d/cqlite-core/src/storage/mod.rs"
  git -C "$d" add -A >/dev/null 2>&1 || return 1
  git -C "$d" commit -q -m seed >/dev/null 2>&1 || return 1
  git -C "$d" update-ref refs/remotes/origin/main mainline || return 1
  git -C "$d" checkout -q -b feature || return 1
  mkdir -p "$d/openspec/changes/a-subdir-routed-slug"
  printf 'the design proposal\n' >"$d/openspec/changes/a-subdir-routed-slug/proposal.md"
  git -C "$d" add -A >/dev/null 2>&1 || return 1
  git -C "$d" commit -q -m "the PR" >/dev/null 2>&1 || return 1
  printf '%s\n' "$d"
}

# run_in_subdir <repo> <subdir-relative-path> <expected-exit> <desc> <args...> —
# `run_in_repo`, but the invocation cwd is a SUBDIRECTORY of the repository rather
# than its root. Sets $OUT/$RC/$C_REPO_SHA exactly as run_in_repo does.
run_in_subdir() {
  local d="$1" sub="$2" want="$3" desc="$4"
  shift 4
  local sha f
  sha=$(git -C "$d" rev-parse HEAD 2>/dev/null) || sha=""
  if [ -z "$sha" ]; then
    bad "$desc: could not resolve the synthetic repository's HEAD"
    return 1
  fi
  if [ ! -d "$d/$sub" ]; then
    bad "$desc: the subdirectory $sub does not exist — the case would be vacuous"
    return 1
  fi
  f="$d/../gate-$(basename "$d")-sub.txt"
  emit_summary_block "$FULL_S" "$FULL_E" "-" \
    "$(printf '%.7s' "$sha")" "$(printf '%.12s' "$sha")" PASS PASS >"$f"
  OUT=$(cd "$d/$sub" && PATH="$BIN:$PATH" MOCK_GH_OUT="$sha OPEN" MOCK_GH_FAIL=0 \
    bash "$NEUTRAL_ASSERT" 2421 "$sha" "$f" "$@" 2>&1)
  RC=$?
  C_REPO_SHA="$sha"
  if [ "$RC" -ne "$want" ]; then
    bad "$desc (exit $RC, wanted $want)"
    printf '     output: %s\n' "$OUT"
    return 1
  fi
  return 0
}

Q1_REPO=$(c_repo_subdir) || Q1_REPO=""
if [ -n "$Q1_REPO" ]; then
  ok "q1 fixture: a design-routed repository with a subdirectory to invoke from was built"
else
  bad "q1 fixture: could not build it — the subdirectory cases would be vacuous"
fi

# THE CONTROL FIRST, so the RED below is attributable to the CWD and not to the
# fixture: from the repository ROOT the same branch measures REQUIRED and refuses.
if [ -n "$Q1_REPO" ] &&
  run_in_repo "$Q1_REPO" 2 "q1/control: from the repository ROOT the branch measures REQUIRED" \
    --c-verdict AUTO; then
  case "$OUT" in
    *"routing: REQUIRED"*"openspec/changes/a-subdir-routed-slug"*)
      ok "q1/control: the root invocation names the change it found" ;;
    *) bad "q1/control: the root invocation must measure REQUIRED (got: $OUT)" ;;
  esac
fi

# THE FINDING. Same repository, same certified sha, same argv — only the cwd differs.
# A subdirectory invocation must reach the SAME routing verdict, and must therefore
# still REFUSE (this branch has no stage opened).
if [ -n "$Q1_REPO" ] &&
  run_in_subdir "$Q1_REPO" cqlite-core/src/storage 2 \
    "q1/subdir: a SUBDIRECTORY invocation on a design-routed branch still REFUSES" \
    --c-verdict AUTO; then
  case "$OUT" in
    *"routing: REQUIRED"*"openspec/changes/a-subdir-routed-slug"*)
      ok "q1/subdir: the routing is measured from the repository ROOT, not from the cwd" ;;
    *) bad "q1/subdir: a design-routed branch must measure REQUIRED from a subdirectory (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"NOT-APPLICABLE"*)
      bad "q1/subdir: the cwd made a design-routed branch read NOT-APPLICABLE — C is escaped by chdir" ;;
    *) ok "q1/subdir: NOT-APPLICABLE is never reached by changing directory" ;;
  esac
fi

# TWO LEVELS DOWN, and from a subdirectory of `openspec/` ITSELF — the shape where a
# cwd-relative pathspec is not merely empty but could match a DIFFERENT tree
# (`openspec/changes/openspec/changes/…`). Both must land on the same answer.
if [ -n "$Q1_REPO" ] &&
  run_in_subdir "$Q1_REPO" openspec/changes 2 \
    "q1/subdir-openspec: invoked from inside openspec/changes itself -> still REQUIRED" \
    --c-verdict AUTO; then
  case "$OUT" in
    *"routing: REQUIRED"*"openspec/changes/a-subdir-routed-slug"*)
      ok "q1/subdir-openspec: the slug is still extracted from a ROOT-relative path" ;;
    *) bad "q1/subdir-openspec: must measure REQUIRED with the slug named (got: $OUT)" ;;
  esac
fi

# THE OTHER DIRECTION STAYS TRUE FROM A SUBDIRECTORY TOO: anchoring the pathspec must
# not turn an ORACLE-routed branch into a design-routed one. Without this, `:(top)`
# could have been widened to match everything and every case above would still pass.
Q1_ORACLE=""
if [ -n "$C_ORACLE" ]; then
  Q1_ORACLE="$T/c-repo-q1-oracle"
  if cp -R "$C_ORACLE" "$Q1_ORACLE" >/dev/null 2>&1 &&
    mkdir -p "$Q1_ORACLE/cqlite-core/src/storage"; then
    ok "q1 oracle fixture: an oracle-routed copy with a subdirectory was built"
  else
    bad "q1 oracle fixture: could not build it — the fail-open direction would be untested"
    Q1_ORACLE=""
  fi
fi
if [ -n "$Q1_ORACLE" ] &&
  run_in_subdir "$Q1_ORACLE" cqlite-core/src/storage 0 \
    "q1/oracle-subdir: an ORACLE-routed branch is still NOT-APPLICABLE from a subdirectory" \
    --c-verdict AUTO; then
  case "$OUT" in
    *"PREMERGE: C-VERDICT NOT-APPLICABLE"*"no openspec change on branch"*)
      ok "q1/oracle-subdir: the anchored pathspec did not widen the measure" ;;
    *) bad "q1/oracle-subdir: must still report NOT-APPLICABLE (got: $OUT)" ;;
  esac
fi

# STRUCTURAL: the pathspec carries the `:(top)` magic prefix, and BOTH pins stay.
# Behavioural cases only cover the cwds someone thought of; this pins the mechanism.
if LC_ALL=C grep -q "':(top)openspec/changes/'" "$ASSERT"; then
  ok "q1/structural: the routing pathspec carries the :(top) root anchor"
else
  bad "q1/structural: the routing pathspec is not root-anchored — a subdirectory invocation escapes C"
fi
if [ "$(LC_ALL=C grep -c -- '-- openspec/changes/ 2' "$ASSERT" || true)" -eq 0 ]; then
  ok "q1/structural: no unanchored openspec/changes/ pathspec survives in the routing diff"
else
  bad "q1/structural: a cwd-relative openspec/changes/ pathspec is still passed to git diff"
fi
if LC_ALL=C grep -q 'diff.renames=false -c diff.relative=false' "$ASSERT"; then
  ok "q1/structural: diff.renames=false and diff.relative=false are BOTH still pinned"
else
  bad "q1/structural: a routing pin was dropped — renames off (F4) and root-relative output are both load-bearing"
fi

# --- 44l: a `report=` value containing a SPACE must arrive WHOLE (round 11, Q3) ---
# THE FINDING (roborev job 385, Q3). `report=` carries an absolute PATH, and a path may
# legitimately contain whitespace — a checkout at `/tmp/work tree`, and this repository itself
# tracks 40 space-bearing paths under `docs/`. `_c_verdict_awk` took the value as ONE
# whitespace-delimited FIELD, so it truncated at the first space and round 10's nonce match then
# REFUSED an otherwise VALID verdict: a false refusal on correct input, which is the guard agents
# learn to waive. Measured on the SHIPPED artifacts before the fix, in a checkout named
# `…/q3/work tree`, where the stage's real generation IS the one the verdict names:
#
#   PREMERGE: NO-C-VERDICT — REFUSING TO MERGE
#     validated generation: c.wDYumb5BHk.md (from the report-nonce: … the head-sha binding read)
#     verdict reported:     /tmp/…/q3/work            <-- truncated at the space
#
# THE FIX IS THE REMAINDER OF THE LINE, and it is sound ONLY because `report=` is emitted LAST —
# which is pinned structurally below against the shipped emitter, so a field appended after it
# reds this suite instead of silently truncating verdicts again.
#
# A FIXTURE WHOSE DIRECTORY NAME CONTAINS A SPACE, end to end on the real producer: nothing short
# of that exercises the path this defect lives on.
c_repo_spacey() {
  local d="$T/c-repo q3 spacey"
  mkdir -p "$d" || return 1
  git init -q -b mainline "$d" >/dev/null 2>&1 || return 1
  git -C "$d" config user.email t@t
  git -C "$d" config user.name t
  printf '.review-stage/\n' >"$d/.gitignore"
  printf 'seed\n' >"$d/README.md"
  git -C "$d" add -A >/dev/null 2>&1 || return 1
  git -C "$d" commit -q -m seed >/dev/null 2>&1 || return 1
  git -C "$d" update-ref refs/remotes/origin/main mainline || return 1
  git -C "$d" checkout -q -b feature || return 1
  mkdir -p "$d/openspec/changes/a-spacey-slug"
  printf 'the design proposal\n' >"$d/openspec/changes/a-spacey-slug/proposal.md"
  git -C "$d" add -A >/dev/null 2>&1 || return 1
  git -C "$d" commit -q -m "the PR" >/dev/null 2>&1 || return 1
  printf '%s\n' "$d"
}
Q3_REPO=$(c_repo_spacey) || Q3_REPO=""
Q3_OK=0
if [ -n "$Q3_REPO" ] && [ -d "$Q3_REPO" ]; then
  case "$Q3_REPO" in
    *" "*) Q3_OK=1; ok "q3 fixture: a design-routed repository whose PATH contains a space was built" ;;
    *) bad "q3 fixture: the fixture path carries no space, so every case below would be vacuous" ;;
  esac
else
  bad "q3 fixture: could not build it — every case below would be vacuous"
fi
# THE PRODUCER IS THE SHIPPED ARTIFACT, and its own emitted line is asserted to CARRY the space:
# if `review-stage.sh` flattened the path instead, this section would prove nothing about the
# reader.
Q3_NONCE=""
if [ "$Q3_OK" -eq 1 ]; then
  if (cd "$Q3_REPO" && bash "$NEUTRAL_DIR/review-stage.sh" open c --issue 3751 \
    --agent spec-auditor >/dev/null 2>&1) &&
    printf 'result: PASS\n\n## Findings\n\nnone.\n' >"$(SR_REPORT "$Q3_REPO" 3751 c)"; then
    ok "q3: a PASSING c stage was opened in the space-bearing checkout"
  else
    bad "q3: the stage could not be opened there — the cases below would be vacuous"
    Q3_OK=0
  fi
fi
if [ "$Q3_OK" -eq 1 ]; then
  Q3_LINE=$(cd "$Q3_REPO" && bash "$NEUTRAL_DIR/review-stage.sh" verdict c --issue 3751 2>/dev/null) || true
  case "$Q3_LINE" in
    *"report=$Q3_REPO/"*) ok "q3/producer: the emitted report= carries the space-bearing path verbatim" ;;
    *) bad "q3/producer: the emitter did not put the space-bearing path on the line (got: $Q3_LINE)" ;;
  esac
  Q3_NONCE="$(LC_ALL=C sed -n 's/^report-nonce:[[:space:]]*//p' \
    "$Q3_REPO/.review-stage/issue-3751/c.stage" 2>/dev/null | LC_ALL=C head -1 || true)"
  if [ -n "$Q3_NONCE" ]; then
    ok "q3: the validated generation's nonce was read from the record ($Q3_NONCE)"
  else
    bad "q3: the record's report-nonce: is unavailable, so the acceptance case proves nothing"
  fi
fi

# (a) THE FINDING, END TO END: a CORRECT verdict in a space-bearing checkout is ACCEPTED. Before
#     the fix this exited 2 with `verdict reported: <path truncated at the space>` beside a
#     `validated generation:` that was exactly the one the verdict named.
if [ "$Q3_OK" -eq 1 ] &&
  run_in_repo "$Q3_REPO" 0 \
    "q3/accept: a CORRECT verdict whose report path contains a SPACE is ACCEPTED" \
    --c-verdict AUTO; then
  case "$OUT" in
    *"PREMERGE: C-VERDICT PASS"*"source: AUTO issue=3751"*)
      ok "q3/accept: the verdict is PASS and names the stage it read" ;;
    *) bad "q3/accept: must report C-VERDICT PASS from AUTO (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"DIFFERENT generation"*)
      bad "q3/accept: a correct verdict was refused as a generation mismatch — the value truncated at the space" ;;
    *) ok "q3/accept: no generation mismatch is reported for a verdict that names the right generation" ;;
  esac
  # AND THE WHOLE PATH REACHES THE OPERATOR, not a prefix: the success line's `report:` field is
  # the only place a human sees WHICH report answered.
  if [ -n "$Q3_NONCE" ]; then
    case "$OUT" in
      *"report: $Q3_REPO/.review-stage/issue-3751/c.$Q3_NONCE.md"*)
        ok "q3/accept: the success line reports the WHOLE space-bearing path" ;;
      *) bad "q3/accept: the success line must report the whole path (got: $OUT)" ;;
    esac
  fi
fi

# (b) THE CONTROL, IN THE SAME SPACE-BEARING CHECKOUT: a FOREIGN nonce is STILL REFUSED. Without
#     it, (a) is satisfiable by a reader that stopped comparing generations at all — which is
#     exactly the round-10 property this must not weaken. A SUBSTITUTED CALLEE (#3312's corollary
#     for tests; there is no settable seam), because the shipped emitter cannot be made to name a
#     generation its own record does not hold.
Q3_STUB_DIR="$T/q3stub/flow"
Q3_STUB_OK=0
if [ "$Q3_OK" -eq 1 ] && [ -n "$Q3_NONCE" ] && mkdir -p "$Q3_STUB_DIR" &&
  cp "$ASSERT" "$Q3_STUB_DIR/premerge-assert.sh" 2>/dev/null &&
  printf '%s\n' "$NEUTRAL_ADV" >"$Q3_STUB_DIR/base-staleness.sh" 2>/dev/null; then
  chmod +x "$Q3_STUB_DIR/base-staleness.sh" 2>/dev/null || true
  Q3_STUB_OK=1
  ok "q3/stub: a scratch assert plus a substitutable callee was built"
else
  bad "q3/stub: the scratch callee could not be built, so the control below proves nothing"
fi
# q3_stub <report-value> — a callee reporting PASS with exactly that `report=` field.
q3_stub() {
  printf '#!/usr/bin/env bash\nprintf "REVIEW-STAGE: c RESULT: PASS elapsed=1 deadline=3600 agent=spec-auditor report=%%s\\n" %s\nexit 0\n' \
    "'$1'" >"$Q3_STUB_DIR/review-stage.sh" 2>/dev/null || return 1
  [ -s "$Q3_STUB_DIR/review-stage.sh" ] || return 1
  return 0
}
# q3_run <repo> <want> <desc> — run the scratch assert from inside <repo> under AUTO.
q3_run() {
  local d="$1" want="$2" desc="$3" sha f
  sha=$(git -C "$d" rev-parse HEAD 2>/dev/null) || sha=""
  if [ -z "$sha" ]; then bad "$desc: could not resolve the fixture HEAD"; return 1; fi
  f="$T/gate-q3-stub.txt"
  emit_summary_block "$FULL_S" "$FULL_E" "-" \
    "$(printf '%.7s' "$sha")" "$(printf '%.12s' "$sha")" PASS PASS >"$f"
  OUT=$(cd "$d" && PATH="$BIN:$PATH" MOCK_GH_OUT="$sha OPEN" MOCK_GH_FAIL=0 \
    bash "$Q3_STUB_DIR/premerge-assert.sh" 2421 "$sha" "$f" --c-verdict AUTO 2>&1)
  RC=$?
  if [ "$RC" -ne "$want" ]; then
    bad "$desc (exit $RC, wanted $want)"
    printf '     output: %s\n' "$OUT"
    return 1
  fi
  return 0
}
if [ "$Q3_STUB_OK" -eq 1 ]; then
  # CONTROL FIRST, so the refusal below is attributable to the NONCE and not to the substitution:
  # the stub naming the VALIDATED generation, with the space, still certifies.
  if q3_stub "$Q3_REPO/.review-stage/issue-3751/c.$Q3_NONCE.md" &&
    q3_run "$Q3_REPO" 0 "q3/stub CONTROL: a space-bearing path naming the VALIDATED generation certifies"; then
    case "$OUT" in
      *"PREMERGE: C-VERDICT PASS"*) ok "q3/stub CONTROL: and it reports C-VERDICT PASS" ;;
      *) bad "q3/stub CONTROL: must report C-VERDICT PASS (got: $OUT)" ;;
    esac
  fi
  # THE REFUSAL: same space-bearing directory, FOREIGN nonce.
  if q3_stub "$Q3_REPO/.review-stage/issue-3751/c.forgedgeneration9.md" &&
    q3_run "$Q3_REPO" 2 "q3/stub forged: a space-bearing path with a FOREIGN nonce still REFUSES"; then
    case "$OUT" in
      *"DIFFERENT generation"*) ok "q3/stub forged: the refusal names the generation mismatch" ;;
      *) bad "q3/stub forged: the refusal must name the generation mismatch (got: $OUT)" ;;
    esac
    # ASSERTED ON THE `verdict reported:` DETAIL LINE, not anywhere in the output: the block also
    # echoes the RAW `verdict line:`, which carries the whole path whether the READER truncated it
    # or not — so an `$OUT`-wide match would pass in the pre-fix state too (measured: it did).
    case "$OUT" in
      *"verdict reported:     $Q3_REPO/.review-stage/issue-3751/c.forgedgeneration9.md"*)
        ok "q3/stub forged: and the parsed value it names is the WHOLE path, not a prefix of it" ;;
      *) bad "q3/stub forged: the refusal must name the whole PARSED path (got: $OUT)" ;;
    esac
  fi
fi

# (c) THE `--c-verdict <path>` BRANCH READS THE SAME LINE, so the grammar/value validation (round
#     7's L3) must not refuse a space-bearing path either. That branch runs no nonce check, so
#     this isolates `c_parse_verdict` from round 10's binding.
if run 0 "q3/explicit: an explicit verdict whose report path contains a SPACE is ACCEPTED" \
  2421 "$CERTIFIED" "$GOOD" --c-verdict "$(c_verdict_file q3sp \
    "REVIEW-STAGE: c RESULT: PASS elapsed=7 deadline=1800 agent=spec-auditor report=$T/a work dir/.review-stage/issue-1/c.abc.md")"; then
  case "$OUT" in
    *"NO USABLE VALUE"*)
      bad "q3/explicit: a space-bearing report path was refused as a mandatory field with no usable value" ;;
    *) ok "q3/explicit: the value validator does not red on a space-bearing path" ;;
  esac
  case "$OUT" in
    *"report: $T/a work dir/.review-stage/issue-1/c.abc.md"*)
      ok "q3/explicit: and the success line carries the whole value" ;;
    *) bad "q3/explicit: the success line must carry the whole value (got: $OUT)" ;;
  esac
fi
# AND THE FIELD CENSUS IS UNCHANGED: a BARE `report=` is still refused, so taking the remainder
# did not turn "empty" into "the rest of the line".
if run 2 "q3/explicit: a BARE report= is still refused (the remainder of nothing is nothing)" \
  2421 "$CERTIFIED" "$GOOD" --c-verdict "$(c_verdict_file q3bare \
    "REVIEW-STAGE: c RESULT: PASS elapsed=7 deadline=1800 agent=spec-auditor report=")"; then
  case "$OUT" in
    *"report= is EMPTY"*) ok "q3/explicit: and it is named as EMPTY, not silently widened" ;;
    *) bad "q3/explicit: a bare report= must be named EMPTY (got: $OUT)" ;;
  esac
fi

# (d) STRUCTURAL — THE PARSER'S ASSUMPTION IS ENFORCED, NOT ASSUMED. Taking the remainder of the
#     line is correct ONLY while `report=` is the LAST field the emitter can produce. The states
#     are DERIVED by RUNNING the shipped emitter (the same `c_capture_state` section 44b(v)
#     uses), never listed by hand, and the property asserted is that NO mandatory key follows
#     `report=` on any line it emits — so appending a field after it reds this suite.
Q3_STATES="pass findings sentinel report-absent report-empty report-ungrammatical
report-unreadable self-reported record-unreadable author-performed deadline-zero"
Q3_LAST_BAD=0
Q3_LAST_N=0
for Q3_ST in $Q3_STATES; do
  Q3_F="$(c_capture_state "$Q3_ST")" || Q3_F=""
  if [ -z "$Q3_F" ] || [ ! -s "$Q3_F" ]; then
    bad "q3/last: could not capture a verdict line for state '$Q3_ST' — the assertion below would be vacuous"
    Q3_LAST_BAD=1
    continue
  fi
  Q3_LAST_N=$((Q3_LAST_N + 1))
  # The TAIL after `report=`. `sed` takes the FIRST occurrence, which is the field the parser
  # reads; a second one is a duplicate the census refuses independently.
  Q3_TAIL="$(LC_ALL=C sed -n 's/^REVIEW-STAGE: .* report=//p' "$Q3_F" 2>/dev/null | LC_ALL=C head -1 || true)"
  case "$Q3_TAIL" in
    *" elapsed="* | *" deadline="* | *" agent="* | *" report="*)
      bad "q3/last: state '$Q3_ST' emits a mandatory key AFTER report=, so the remainder rule truncates it (tail: $Q3_TAIL)"
      Q3_LAST_BAD=1 ;;
    *) ;;
  esac
done
if [ "$Q3_LAST_N" -ge 11 ]; then
  ok "q3/last: $Q3_LAST_N emitter states were derived and captured (>= the 11 known ones)"
else
  bad "q3/last: only $Q3_LAST_N emitter state(s) captured — the derivation broke and this guard is vacuous"
fi
if [ "$Q3_LAST_BAD" -eq 0 ]; then
  ok "q3/last: report= is the LAST field on every verdict line the shipped emitter can produce"
else
  bad "q3/last: report= is not last on some emitted state — the parser's remainder rule is unsound there"
fi
# THE EMITTER HAS EXACTLY ONE VERDICT-LINE SITE, and `report=` is written last on it. Asserted on
# the SOURCE as well as on the output, because the behavioural sweep above can only cover the
# states this suite knows how to reach.
Q3_EMIT="$(LC_ALL=C grep -h 'RESULT: \$rendered' "$NEUTRAL_DIR/review-stage.sh" 2>/dev/null || true)"
if [ "$(printf '%s\n' "$Q3_EMIT" | LC_ALL=C grep -c 'RESULT: \$rendered' || true)" -eq 1 ]; then
  ok "q3/last-structural: the verdict line is emitted from exactly ONE site"
else
  bad "q3/last-structural: the verdict line is emitted from more than one site — the remainder rule would need checking at each"
fi
# THE LITERAL PINS THE BOUNDARY AS WELL AS THE POSITION (#3751 round 16, V2). `report=` is the ONE
# field EXEMPT from the '='->'~' map, through `remainder_value` — a repository root may legally
# contain '=', and mapping it published a path that DOES NOT EXIST. That exemption is sound ONLY
# because of the property this subsection asserts: the field is LAST and is read as the line
# REMAINDER, so an '=' inside it cannot create an ambiguous field. So the two facts are pinned
# TOGETHER, in one match: a change that appends a field after `report=` OR routes it back through
# the '='-mapping boundary reds here, because either one alone makes the other wrong.
case "$Q3_EMIT" in
  *'report=$(remainder_value "${STAGE_REPORT:-unresolved}")"')
    ok "q3/last-structural: that site ends with report= through the remainder-exempt boundary, so nothing follows the remainder" ;;
  *) bad "q3/last-structural: report= is no longer the LAST thing on the emitted line, or no longer exempt from the '=' map (got: $Q3_EMIT)" ;;
esac
# AND THE EXEMPTION IS NOT A BLANKET ONE: the OTHER mandatory fields on this line must still go
# through the '='-mapping boundary, or a hand-edited record could forge a `report=` pair AHEAD of
# the measured one and the remainder rule above would take the FORGED value (the reader takes the
# FIRST occurrence). The behavioural counterpart is section 29(b) of test_review_stage.sh.
case "$Q3_EMIT" in
  *'deadline=$(field_value "$STAGE_DEADLINE") agent=$(field_value "$STAGE_AGENT")'*)
    ok "q3/last-structural: and deadline=/agent= STILL route through the '='-mapping boundary, so neither can forge a report= pair" ;;
  *) bad "q3/last-structural: a mandatory field ahead of report= no longer maps '=' — a record value could forge the pair the remainder rule reads (got: $Q3_EMIT)" ;;
esac

# (e) STRUCTURAL — THE READER TAKES THE REMAINDER, not one field. Behavioural cases only cover the
#     paths someone thought of; this pins the mechanism against a "simplification" back to a field.
if LC_ALL=C grep -q 'rep = substr($0, fs + 7)' "$ASSERT"; then
  ok "q3/reader-structural: report= is read as the REMAINDER of the line"
else
  bad "q3/reader-structural: report= is no longer read as the remainder — a space-bearing path truncates again"
fi
if [ "$(LC_ALL=C grep -c 'rep = substr($i, 8)' "$ASSERT" || true)" -eq 0 ]; then
  ok "q3/reader-structural: no field-truncating extraction of report= survives"
else
  bad "q3/reader-structural: the field-truncating extraction is still present"
fi

# --- 44m: a CAPTURE must not MANUFACTURE the token it validates (round 13, S2) ---
# THE FINDING'S SIBLING, one script over (roborev job 387, S2). Bash SILENTLY DISCARDS NUL bytes in
# a command substitution, so a capture does not merely lose information — it can CREATE grammar its
# source does not contain. Both of this script's reads of untrusted content are captures:
# `_c_verdict_awk`'s OUTPUT (awk passes a NUL through; the capture then removes it) and
# `c_record_bytes`' read of the stage record. Measured with the shipped awk:
#
#   $ printf 'REVIEW-STAGE: c RESULT: PA\0SS …\n' > cv.txt
#   $ awk '/^REVIEW-STAGE: /{printf "token=%s\n", $4}' cv.txt | od -c   ->  t o k e n = P A \0 S S
#   $ o="$(awk … cv.txt)"; printf %s "$o" | od -c                        ->  t o k e n = P A S S
#
# so a file whose token is `PA<NUL>SS` — a token that is NOT `PASS`, and which this script's own
# closed grammar must therefore refuse — reached the merge point as `PASS`. This is the MERGE GATE,
# and the token is the whole subject of the flag.
#
# THE FIX IS THE SAME ONE, FOR THE SAME REASON: the ONE read maps NUL to SOH in the stream, so the
# byte count is preserved, nothing is lost, and the transformed token fails the closed-set match by
# STRING EQUALITY exactly as `PASSthisNeverRan` does. A separate probe of the same path would be a
# SECOND observation, one direction of whose disagreement is a false PASS.
C_NUL_TOKEN="$T/c-verdict-nul-token.txt"
printf 'REVIEW-STAGE: c RESULT: PA\000SS elapsed=42 deadline=1800 agent=spec-auditor report=%s\n' \
  "$T/injected-c-report.md" >"$C_NUL_TOKEN"
# THE PREMISE IS MEASURED ON THE FILE: it really does not carry the token `PASS`.
if LC_ALL=C grep -q 'RESULT: PASS' "$C_NUL_TOKEN" 2>/dev/null; then
  bad "s2/pm PREMISE: the fixture DOES carry the literal token PASS, so the case below proves nothing"
else
  ok "s2/pm PREMISE: the fixture carries NO literal 'RESULT: PASS' (MEASURED with grep on the FILE)"
fi
c_refused "s2/pm: a NUL-bearing token is REFUSED, not read as the PASS the capture manufactured" \
  "$C_NUL_TOKEN" "token"
# CONTROL: the same line WITHOUT the NUL is accepted, so the refusal is about the byte and not
# about the fixture being malformed some other way.
if run 0 "s2/pm CONTROL: the same verdict line without the NUL still certifies (exit 0)" \
  2421 "$CERTIFIED" "$GOOD" --c-verdict "$C_PASS_FILE"; then
  case "$OUT" in
    *"PREMERGE: OK"*) ok "s2/pm CONTROL: and reports PREMERGE: OK, so the guard does not red on correct input" ;;
    *) bad "s2/pm CONTROL: the clean verdict did not report PREMERGE: OK (got: $OUT)" ;;
  esac
fi
# STRUCTURAL: ONE MAPPING, ONE LITERAL, and every capture of untrusted content routed through it —
# the same three properties test_review_stage.sh section 26 pins for the sibling script. The two
# scripts deliberately do NOT share an implementation (no agreement between them is required: each
# read is used within ONE process, over a DIFFERENT file), so each carries its own, and each
# carries its own guard.
if LC_ALL=C grep -q 'c_capture_map_nul() {' "$ASSERT"; then
  ok "s2/pm/structural: c_capture_map_nul() is the ONE mapping implementation in this script"
else
  bad "s2/pm/structural: could not locate c_capture_map_nul() — the assertions below would be vacuous"
fi
if [ "$(LC_ALL=C grep -c "tr '\\\\000'" "$ASSERT" || true)" -eq 1 ]; then
  ok "s2/pm/structural: the NUL translation appears EXACTLY ONCE, so no reader can drift from it"
else
  bad "s2/pm/structural: the NUL translation appears $(LC_ALL=C grep -c "tr '\\\\000'" "$ASSERT" || true) time(s) — a second copy is a second place for the mapper and the detector to disagree"
fi
if LC_ALL=C grep -q 'C_CAPTURE_NUL_BYTE="\$(printf' "$ASSERT"; then
  ok "s2/pm/structural: the marker BYTE is DERIVED from the tr spelling, not written a second time"
else
  bad "s2/pm/structural: the marker byte is spelled independently of the translation, so the detector can look for a byte the mapper never writes"
fi
if [ "$(LC_ALL=C grep -c 'cat -- "\$' "$ASSERT" || true)" -eq 0 ]; then
  ok "s2/pm/structural: no raw file capture bypasses the mapping"
else
  bad "s2/pm/structural: $(LC_ALL=C grep -c 'cat -- "\$' "$ASSERT" || true) raw file capture(s) remain, and a capture that normalises its input cannot be the thing that validates it"
fi
if LC_ALL=C grep -q '_c_verdict_awk <(c_capture_map_nul' "$ASSERT" ||
  LC_ALL=C grep -q 'c_capture_map_nul "$value" | _c_verdict_awk' "$ASSERT"; then
  ok "s2/pm/structural: the c-verdict FILE read goes through the mapping before awk sees it"
else
  bad "s2/pm/structural: the c-verdict file is still fed to awk raw, so a NUL in the token survives to a capture that removes it"
fi

# --- 44q: the ANSI STRIP may LOCATE a line, never SUPPLY a value (round 15, U2) ---
# THE FINDING (roborev job 391, U2). `_c_verdict_awk` deleted every CSI sequence BEFORE the closed
# grammar was applied to the fields that deletion produced, so a token spelt `PA<ESC>[31mSS` was
# NORMALISED INTO `PASS` and a malformed explicit verdict artifact CERTIFIED A MERGE. That is round
# 13's S2 rule at a different byte — *a transform that normalises its input cannot be the thing
# that validates it* — and it is the same shape as the NUL: the read did not lose information, it
# MANUFACTURED grammar the file does not contain. Measured on the shipped parser before the fix:
#
#   $ printf 'REVIEW-STAGE: c RESULT: PA\033[31mSS elapsed=42 …\n' > cv.txt
#   $ LC_ALL=C grep -c 'RESULT: PASS' cv.txt                       -> 0   (rc 1)
#   $ awk '{gsub(/\033\[[0-9;]*[a-zA-Z]/,"")} /^REVIEW-STAGE: /{print "token=" $4}' cv.txt
#     token=PASS
#
# THE STRIP IS NOT GRATUITOUS, so it is SPLIT rather than deleted. It exists for #3400 (colour
# SURVIVES redirection to a file), and without it a reader anchored on a marker line reports "no
# verdict line" for a document that has one — Case 24 above pins that a coloured GATE SUMMARY still
# certifies. So each of this script's three awk readers now keeps TWO readings of every line: one
# with each CSI DELETED, used to LOCATE and to parse, and one with each CSI replaced by a SINGLE
# SPACE, used for ONE question — did the deletion JOIN two runs the file keeps apart? A CSI that
# BRACKETS a token leaves it a whole field of the second reading; a CSI INSIDE one splits it, so the
# token the first reading shows appears in the second NOWHERE. Separate versus join, measured.
#
# THE TWO READERS OF REVIEW-STAGE.SH's OWN ARTIFACTS TAKE THE STRICT FORM (every field of the
# anchored line must survive), because those artifacts have ONE producer and it emits no colour at
# all. `_gate_awk` takes the VALUE-ONLY form, because a coloured gate-summary capture is
# documented-legitimate input and real colouring brackets the KEY as readily as the value
# (`<ESC>[32mRESULT<ESC>[0m:`), which the strict form would red on.
U2_ESC=$(printf '\033')
# (a) THE C-VERDICT TOKEN — the reported site.
U2_CV="$T/u2-c-verdict-spliced.txt"
printf 'REVIEW-STAGE: c RESULT: PA%s[31mSS elapsed=42 deadline=1800 agent=spec-auditor report=%s\n' \
  "$U2_ESC" "$T/injected-c-report.md" >"$U2_CV"
if LC_ALL=C grep -q 'RESULT: PASS' "$U2_CV" 2>/dev/null; then
  bad "u2/cv PREMISE: the fixture DOES carry the literal token PASS, so the case below proves nothing"
else
  ok "u2/cv PREMISE: the fixture carries NO literal 'RESULT: PASS' (MEASURED with grep on the FILE)"
fi
c_refused "u2/cv: an ESCAPE-SPLICED token is REFUSED, not read as the PASS the strip manufactured" \
  "$U2_CV" "ANSI ESCAPE SEQUENCE"
# AND THE DIAGNOSTIC SHOWS WHAT THE FILE HOLDS. Printing the NORMALISED line beside "this line
# contains an escape" would show the operator a clean `RESULT: PASS` and contradict its own
# sentence — a misleading rationale is worse than none (round 2, B7). The ESC renders as `?`,
# which is the same rendering review-stage.sh's own classifier reports the shape under.
if run 2 "u2/cv: (re-run to inspect the diagnostic)" 2421 "$CERTIFIED" "$GOOD" --c-verdict "$U2_CV"; then
  case "$OUT" in
    *"RESULT: PA?[31mSS"*) ok "u2/cv: the diagnostic prints the RAW line (PA?[31mSS), not the clean PASS the strip produced" ;;
    *) bad "u2/cv: the diagnostic does not show the raw spliced token (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"verdict line: REVIEW-STAGE: c RESULT: PASS "*)
      bad "u2/cv: the diagnostic printed the NORMALISED line, asserting a clean PASS beside a refusal about an escape" ;;
    *) ok "u2/cv: and it never prints the normalised 'RESULT: PASS' beside that refusal" ;;
  esac
fi
# (b) THE GATE SUMMARY's RESULT: value — the sibling site, found by sweeping the class. Nothing
#     else here would catch it: unlike the c-verdict token, that value has no mandatory-field
#     census standing behind it.
U2_G="$T/u2-gate-spliced.txt"
{
  printf '==== AGENT-GATE SUMMARY ====\n'
  printf 'RESULT: PA%s[31mSS\n' "$U2_ESC"
  printf 'tree-integrity: PASS\n'
  printf 'commit: %s dirty: no\n' "$C7"
  printf 'tree-start: %s dirty: no\n' "$C12"
  printf '==== END AGENT-GATE SUMMARY ====\n'
} >"$U2_G"
if LC_ALL=C grep -q 'RESULT: PASS' "$U2_G" 2>/dev/null; then
  bad "u2/gate PREMISE: the fixture DOES carry the literal 'RESULT: PASS', so the case below proves nothing"
else
  ok "u2/gate PREMISE: the fixture carries NO literal 'RESULT: PASS' (MEASURED with grep on the FILE)"
fi
if run 2 "u2/gate: an ESCAPE-SPLICED gate RESULT: is REFUSED, not read as PASS" \
  2421 "$CERTIFIED" "$U2_G" --c-verdict "$C_PASS_FILE"; then
  case "$OUT" in
    *"PREMERGE: NO-GATE-OF-RECORD"*) ok "u2/gate: under the gate-of-record refusal" ;;
    *) bad "u2/gate: expected NO-GATE-OF-RECORD (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"ANSI ESCAPE SEQUENCE"*) ok "u2/gate: and the refusal NAMES the escape rather than complaining about a token nobody wrote" ;;
    *) bad "u2/gate: the refusal must name the escape (got: $OUT)" ;;
  esac
fi
# (c) THE CONTROL THAT MATTERS MOST — colour that BRACKETS the key and the value, which is what a
#     colouring tool actually emits and what #3400 makes legitimate input. It must still certify:
#     a guard that reds on correct input is the guard agents learn to waive. Distinct from Case 24
#     above, which colours the same fixture: this one asserts the property under THIS section's
#     rule, so deleting the value-only form here reds locally instead of two thousand lines away.
U2_GC="$T/u2-gate-bracketed.txt"
LC_ALL=C sed \
  -e "s/RESULT: PASS/${U2_ESC}[32mRESULT${U2_ESC}[0m: ${U2_ESC}[1;32mPASS${U2_ESC}[0m/" \
  -e "s/tree-integrity: PASS/tree-integrity: ${U2_ESC}[32mPASS${U2_ESC}[0m/" \
  "$GOOD" >"$U2_GC"
if LC_ALL=C grep -q "$U2_ESC" "$U2_GC" 2>/dev/null; then
  ok "u2/gate CONTROL: the bracketed-colour fixture really does carry ANSI escapes"
else
  bad "u2/gate CONTROL: the fixture carries no escapes — the control below is vacuous"
fi
if run 0 "u2/gate CONTROL: colour BRACKETING the key and the value still certifies (exit 0)" \
  2421 "$CERTIFIED" "$U2_GC" --c-verdict "$C_PASS_FILE"; then
  case "$OUT" in
    *"PREMERGE: OK"*) ok "u2/gate CONTROL: and reports PREMERGE: OK — the value survives as a whole field, so nothing was joined" ;;
    *) bad "u2/gate CONTROL: the bracketed capture did not certify (got: $OUT)" ;;
  esac
fi
# (d) THE STAGE RECORD's head-sha — the third site. A spliced sha normalises into a clean 40-hex
#     value and would BIND the stage to a tree the record does not name, which is the one thing
#     round 3's G1 exists to prevent.
U2_REPO=$(c_repo u2rec design) || U2_REPO=""
if [ -n "$U2_REPO" ]; then
  if (cd "$U2_REPO" && bash "$NEUTRAL_DIR/review-stage.sh" open c --issue 3751 \
    --agent spec-auditor >/dev/null 2>&1) &&
    printf 'result: PASS\n\n## Findings\n\nnone.\n' >"$(SR_REPORT "$U2_REPO" 3751 c)"; then
    ok "u2/record fixture: a PASSING c stage was opened"
  else
    bad "u2/record fixture: could not open the stage — the cases would be vacuous"
    U2_REPO=""
  fi
fi
if [ -n "$U2_REPO" ]; then
  U2_REC="$U2_REPO/.review-stage/issue-3751/c.stage"
  cp "$U2_REC" "$U2_REC.pristine" 2>/dev/null || true
  # THE CONTROL FIRST, so the section proves the fixture certifies before it is spliced. The
  # record's own head-sha is the certified commit, so AUTO must reach C-VERDICT PASS.
  if run_in_repo "$U2_REPO" 0 "u2/record CONTROL: the intact record certifies under AUTO" \
    --c-verdict AUTO; then
    case "$OUT" in
      *"PREMERGE: C-VERDICT PASS"*) ok "u2/record CONTROL: reaching C-VERDICT PASS, so the splice below is the only difference" ;;
      *) bad "u2/record CONTROL: expected C-VERDICT PASS (got: $OUT)" ;;
    esac
  fi
  # THE SPLICE, planted through ENVIRON rather than `awk -v`: `-v` PERFORMS ESCAPE PROCESSING on
  # its value, so a plant carrying `\033` would arrive as something else entirely (round 7's
  # measured harness defect).
  if U2_E="$U2_ESC" LC_ALL=C awk '
      BEGIN { e = ENVIRON["U2_E"] }
      /^head-sha: / { printf "head-sha: %s%s[32m%s\n", substr($2, 1, 3), e, substr($2, 4); next }
      { print }
    ' "$U2_REC.pristine" >"$U2_REC.new" 2>/dev/null && mv -f "$U2_REC.new" "$U2_REC"; then
    ok "u2/record: the escape was planted INSIDE the head-sha value"
  else
    bad "u2/record: could not plant the escape — the case below is vacuous"
  fi
  if LC_ALL=C grep -q "^head-sha: $CERTIFIED\$" "$U2_REC" 2>/dev/null; then
    bad "u2/record PREMISE: the record still carries the certified sha literally, so the case proves nothing"
  else
    ok "u2/record PREMISE: the record no longer carries the certified sha as literal bytes (MEASURED with grep on the FILE)"
  fi
  if run_in_repo "$U2_REPO" 2 \
    "u2/record: an ESCAPE-SPLICED head-sha REFUSES, not binds the stage to a tree the record does not name" \
    --c-verdict AUTO; then
    # The needle is the phrase that cannot be split by the diagnostic's own line wrapping:
    # the full sentence "ANSI ESCAPE SEQUENCE (0x1b)" is emitted across two lines here.
    case "$OUT" in
      *"ANSI ESCAPE"*) ok "u2/record: and the refusal NAMES the escape" ;;
      *) bad "u2/record: the refusal must name the escape (got: $OUT)" ;;
    esac
  fi
fi
# (e) STRUCTURAL — THE SPLIT IS PRESENT IN ALL THREE READERS, AND THE COUNTS ARE PINNED. A reader
#     that lost its separating reading would go back to validating a value the deletion invented,
#     while every behavioural case above still passed for its own site.
U2_DEL=$(LC_ALL=C grep -c 'gsub(/\\033\\\[\[0-9;\]\*\[a-zA-Z\]/, "",' "$ASSERT" || true)
U2_SEP=$(LC_ALL=C grep -c 'gsub(/\\033\\\[\[0-9;\]\*\[a-zA-Z\]/, " ",' "$ASSERT" || true)
if [ "$U2_DEL" = "3" ]; then
  ok "u2/structural: all THREE awk readers keep a DELETING reading (for locating): $U2_DEL"
else
  bad "u2/structural: $U2_DEL deleting CSI strips, want 3 (one per awk reader) — a reader lost or gained one"
fi
if [ "$U2_SEP" = "3" ]; then
  ok "u2/structural: and all THREE keep a SEPARATING reading (for the join test): $U2_SEP"
else
  bad "u2/structural: $U2_SEP separating CSI substitutions, want 3 — a reader validates values the deletion invented"
fi
if [ "$(LC_ALL=C grep -c 'gsub(/\\033\\\[\[0-9;\]\*\[a-zA-Z\]/, "")' "$ASSERT" || true)" = "0" ]; then
  ok "u2/structural: no reader strips in place with a bare two-argument gsub, which would leave no raw line to compare against"
else
  bad "u2/structural: $(LC_ALL=C grep -n 'gsub(/\\033\\\[\[0-9;\]\*\[a-zA-Z\]/, "")' "$ASSERT" | LC_ALL=C tr '\n' ' ')"
fi
# THE DECISION IS TAKEN BEFORE THE GRAMMAR IT PROTECTS. A check placed after the asserts that read
# the normalised fields could only report — for `PA<ESC>[31mSS` it would report nothing at all,
# because those asserts would have PASSED and the run would have certified.
U2_CVESC_LN=$(LC_ALL=C grep -n 'case "\$CV_ESC" in' "$ASSERT" | LC_ALL=C head -1 | cut -d: -f1)
U2_CVKIND_LN=$(LC_ALL=C grep -n 'if \[ "\$CV_KIND" != "\$C_STAGE_KIND" \]' "$ASSERT" | LC_ALL=C head -1 | cut -d: -f1)
if [ -n "$U2_CVESC_LN" ] && [ -n "$U2_CVKIND_LN" ] && [ "$U2_CVESC_LN" -lt "$U2_CVKIND_LN" ]; then
  ok "u2/structural: the c-verdict escape decision precedes the first grammar assert (lines $U2_CVESC_LN < $U2_CVKIND_LN)"
else
  bad "u2/structural: the c-verdict escape decision is NOT before the grammar asserts (esc=$U2_CVESC_LN kind=$U2_CVKIND_LN)"
fi
U2_GPESC_LN=$(LC_ALL=C grep -n 'case "\$GP_esc" in' "$ASSERT" | LC_ALL=C head -1 | cut -d: -f1)
U2_GPCOUNT_LN=$(LC_ALL=C grep -n 'for gp_k in blocks full lite delta unterminated' "$ASSERT" | LC_ALL=C head -1 | cut -d: -f1)
if [ -n "$U2_GPESC_LN" ] && [ -n "$U2_GPCOUNT_LN" ] && [ "$U2_GPESC_LN" -lt "$U2_GPCOUNT_LN" ]; then
  ok "u2/structural: the gate-summary escape decision precedes its count validation (lines $U2_GPESC_LN < $U2_GPCOUNT_LN)"
else
  bad "u2/structural: the gate-summary escape decision is NOT before the count validation (esc=$U2_GPESC_LN counts=$U2_GPCOUNT_LN)"
fi
# AND EVERY ESCAPE BRANCH SPELLS ITS PERMISSIVE SET AFFIRMATIVELY. `0` and nothing else may
# proceed; an unparseable or absent measurement takes the fail-closed arm, so a flag this parser
# stops publishing cannot arrive as "no escape found".
U2_AFFIRM=0
for U2_V in CV_ESC GP_esc; do
  if LC_ALL=C awk -v v="$U2_V" '
      index($0, "case \"$" v "\" in") > 0 { seen = 1; next }
      seen == 1 && $0 ~ /^[[:space:]]*0\)/ { hit = 1; seen = 0 }
      END { exit(hit ? 0 : 1) }
    ' "$ASSERT"; then
    U2_AFFIRM=$((U2_AFFIRM + 1))
  fi
done
if [ "$U2_AFFIRM" = "2" ]; then
  ok "u2/structural: both escape branches key their permissive arm on the AFFIRMATIVE 0, never on != 1"
else
  bad "u2/structural: only $U2_AFFIRM of 2 escape branches spell the permissive set affirmatively"
fi
# THE TRAILING CR IS DELIBERATELY STILL TOLERATED, AND THE RULING IS PINNED IN THE SOURCE. A CR is
# separator WHITESPACE — the `\r$` strip removes one byte where nothing follows, so it can separate
# but never join — and the sibling reader `classify_report` reads a CRLF line as its token too
# (measured, alongside a trailing TAB and trailing SPACES). Refusing it here would have been a
# unilateral change to one of two readers of one shape, which is what section 44g detects. The
# `crlf` row of that table asserts the AGREEMENT; this asserts the reason is written down.
if LC_ALL=C grep -q 'SEPARATE VERSUS JOIN' "$ASSERT"; then
  ok "u2/structural: the separate-versus-join ruling that keeps the trailing CR tolerated is stated in the source"
else
  bad "u2/structural: nothing in the source states why an escape is refused while a trailing CR is not"
fi

# --- 44r: THE C VERDICT MUST BE RE-VALIDATED INSIDE THE WINDOW IT CERTIFIES (round 16, V1) ---
# THE FINDING (roborev job 393, V1, premerge-assert.sh:2717). `c_evaluate` ran ONCE, and then the
# base-staleness advisory (bounded at ADVISORY_TIMEOUT_SECS + ADVISORY_KILL_GRACE = 65s) and the
# `gh pr view` network call happened, and NOTHING re-validated C before `PREMERGE: OK` was emitted.
# A concurrent `review-stage.sh open --force` landing in that window supersedes the validated PASS
# with a fresh NOT-RUN generation, and the script still printed `PREMERGE: OK` on the strength of
# the stale PASS.
#
# THIS REPOSITORY ALREADY HAD THE RULING (CLAUDE.md, roborev job 290, on the gate's own
# component-set pre-flight): a check must be INSIDE the window it certifies — not before it, not
# after the harm — and the remedy applied there was to REPEAT the check inside the window while
# KEEPING the earlier one, because the early call is what stops an uncertifiable run from doing the
# expensive work at all. Both halves are asserted below: the repeat, and the retention.
#
# THE INTERLEAVE IS SIMULATED, NOT RACED. One line is injected into a SCRATCH COPY of the assert
# immediately after its FIRST `c_evaluate`, so the supersede is deterministic, cannot flake, and
# makes no claim about timing. The ARTIFACT is substituted (#3312's corollary for tests); there is
# no settable seam. Section 44j's two-point builder cannot express this case — the plant has to
# land between the two EVALUATIONS, not either side of one callee invocation.
V1_DIR="$T/v1/flow"
mkdir -p "$V1_DIR"
v1_ok=1
cp "$ASSERT" "$V1_DIR/premerge-assert.sh" 2>/dev/null || v1_ok=0
cp "$SCRIPT_DIR/../flow/review-stage.sh" "$V1_DIR/review-stage.sh" 2>/dev/null || v1_ok=0
printf '%s\n' "$NEUTRAL_ADV" >"$V1_DIR/base-staleness.sh" 2>/dev/null || v1_ok=0
chmod +x "$V1_DIR/base-staleness.sh" 2>/dev/null || true
V1_REPO=$(c_repo v1 design) || V1_REPO=""
# v1_restore — back to "opened at HEAD, current report records PASS", re-asserted per case: a case
# that leaves a superseded generation installed makes the next one refuse for the PREVIOUS case's
# reason, which is the cross-case leakage section 44j records.
v1_restore() {
  [ -n "$V1_REPO" ] || return 1
  (cd "$V1_REPO" && bash "$V1_DIR/review-stage.sh" open c --issue 3751 \
    --agent spec-auditor --force >/dev/null 2>&1) || return 1
  printf 'result: PASS\n\n## Findings\n\nnone.\n' >"$(SR_REPORT "$V1_REPO" 3751 c)" || return 1
  return 0
}
if [ -n "$V1_REPO" ] && [ "$v1_ok" -eq 1 ] && v1_restore; then
  ok "v1 fixture: a design-routed repository with a PASSING c stage opened at its head was built"
else
  bad "v1 fixture: could not build it — every case below would be vacuous"
  V1_REPO=""
fi
# Every injected line travels through ENVIRON, never `awk -v`, which performs ESCAPE PROCESSING on
# its value (round 7 measured a `\n` in an injected line becoming a real newline).
# v1_build <dest> <line> — copy the scratch assert with <line> inserted immediately AFTER its
# first bare `c_evaluate` call, i.e. inside the window the finding is about.
v1_build() {
  local dest="$1" post="$2"
  [ "$v1_ok" -eq 1 ] || return 1
  V1_POST="$post" LC_ALL=C awk '
    BEGIN { post = ENVIRON["V1_POST"]; done = 0 }
    $0 == "c_evaluate" && done == 0 { print $0; print post; done = 1; next }
    { print }
  ' "$V1_DIR/premerge-assert.sh" >"$dest" 2>/dev/null || return 1
  [ -s "$dest" ] || return 1
  LC_ALL=C grep -q 'V1_SUPERSEDE' "$dest" || return 1
  return 0
}
# v1_run <script> <repo> <want> <desc> — `run_in_repo` against a named scratch assert.
v1_run() {
  local script="$1" d="$2" want="$3" desc="$4" sha f
  sha=$(git -C "$d" rev-parse HEAD 2>/dev/null) || sha=""
  if [ -z "$sha" ]; then bad "$desc: could not resolve the fixture HEAD"; return 1; fi
  f="$T/gate-v1-$(basename "$script").txt"
  emit_summary_block "$FULL_S" "$FULL_E" "-" \
    "$(printf '%.7s' "$sha")" "$(printf '%.12s' "$sha")" PASS PASS >"$f"
  OUT=$(cd "$d" && PATH="$BIN:$PATH" MOCK_GH_OUT="$sha OPEN" MOCK_GH_FAIL=0 \
    bash "$script" 2421 "$sha" "$f" --c-verdict AUTO 2>&1)
  RC=$?
  if [ "$RC" -ne "$want" ]; then
    bad "$desc (exit $RC, wanted $want)"
    printf '     output: %s\n' "$OUT"
    return 1
  fi
  return 0
}
V1_RS="$V1_DIR/review-stage.sh"
# THE SUPERSEDE, in the two shapes a real one takes. `open --force` re-stamps `head-sha` to THIS
# worktree's HEAD — which in the fixture IS the certified sha — so neither plant is caught by the
# head binding: what changes is the GENERATION, which is the property under test.
V1_SUP_NOTRUN="    bash \"$V1_RS\" open c --issue 3751 --agent spec-auditor --force >/dev/null 2>&1 || true   # V1_SUPERSEDE"
V1_SUP_PASS="    V1P=\$(bash \"$V1_RS\" open c --issue 3751 --agent spec-auditor --force 2>/dev/null | LC_ALL=C sed -n 2p) && printf 'result: PASS\\n' >\"\$V1P\" || true   # V1_SUPERSEDE"

if [ -n "$V1_REPO" ]; then
  # (a) THE CONTROL, FIRST AND UNDISTURBED: no plant at all, so the scratch copy must still reach
  #     `PREMERGE: OK`. A guard that reds on correct input is the guard agents learn to waive, and
  #     without this case the refusals below are satisfiable by a re-check that refuses always.
  if v1_restore; then
    ok "v1/control: the stage was restored to opened-at-HEAD with a PASSING report"
  else
    bad "v1/control: the stage could not be restored, so this case starts from an unknown state"
  fi
  if v1_run "$V1_DIR/premerge-assert.sh" "$V1_REPO" 0 \
    "v1/control: an UNDISTURBED run still certifies (the re-check must not red on correct input)"; then
    case "$OUT" in
      *"PREMERGE: OK"*) ok "v1/control: and it emits PREMERGE: OK" ;;
      *) bad "v1/control: must emit PREMERGE: OK (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"PREMERGE: C-VERDICT PASS"*) ok "v1/control: and it reports C-VERDICT PASS" ;;
      *) bad "v1/control: must report C-VERDICT PASS (got: $OUT)" ;;
    esac
  fi

  # (b) THE FINDING: the validated PASS is SUPERSEDED by a fresh NOT-RUN generation inside the
  #     window. Before the fix this exited 0 and printed `PREMERGE: OK` on the strength of a
  #     verdict that no longer existed.
  if v1_restore; then
    ok "v1/notrun: the stage was restored before the case (no leakage from the control)"
  else
    bad "v1/notrun: the stage could not be restored, so this case starts from an unknown state"
  fi
  if v1_build "$V1_DIR/sup-notrun.sh" "$V1_SUP_NOTRUN"; then
    ok "v1/notrun: the supersede plant landed in the scratch assert (asserted, not assumed)"
  else
    bad "v1/notrun: the plant did NOT land, so this case proves nothing"
  fi
  if v1_run "$V1_DIR/sup-notrun.sh" "$V1_REPO" 2 \
    "v1/notrun: a PASS SUPERSEDED by a fresh NOT-RUN generation mid-window must NOT certify"; then
    case "$OUT" in
      *"PREMERGE: NO-C-VERDICT"*) ok "v1/notrun: refused under the NO-C-VERDICT verdict" ;;
      *) bad "v1/notrun: must refuse with NO-C-VERDICT (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"revalidation"*) ok "v1/notrun: and the refusal names the RE-VALIDATION as the window it was raised in" ;;
      *) bad "v1/notrun: the refusal must name the re-validation window (got: $OUT)" ;;
    esac
    case "$OUT" in
      *NOT-RUN*) ok "v1/notrun: and it names the state the stage is now in" ;;
      *) bad "v1/notrun: the refusal must name the NOT-RUN state (got: $OUT)" ;;
    esac
    # THE CERTIFICATION MUST NEVER HAVE BEEN PRINTED. Exit 2 with an `OK` line already on stdout
    # would be a merge armed by a reader that stopped at the first line.
    case "$OUT" in
      *"PREMERGE: OK"*) bad "v1/notrun: PREMERGE: OK was emitted on a refusing run — a reader takes that as certification" ;;
      *) ok "v1/notrun: and no PREMERGE: OK line was emitted at all" ;;
    esac
  fi

  # (c) THE DISCRIMINATION THAT MAKES THIS SECTION MORE THAN "RUN IT TWICE": the supersede installs
  #     a generation that ITSELF PASSES, at the same head. A second evaluation alone therefore
  #     returns PASS and would certify; only a COMPARISON against the first observation sees that
  #     the audit that answered is not the audit that was validated.
  if v1_restore; then
    ok "v1/freshpass: the stage was restored before the case (no leakage from (b))"
  else
    bad "v1/freshpass: the stage could not be restored, so this case starts from an unknown state"
  fi
  if v1_build "$V1_DIR/sup-pass.sh" "$V1_SUP_PASS"; then
    ok "v1/freshpass: the fresh-PASS supersede plant landed in the scratch assert"
  else
    bad "v1/freshpass: the plant did NOT land, so this case proves nothing"
  fi
  if v1_run "$V1_DIR/sup-pass.sh" "$V1_REPO" 2 \
    "v1/freshpass: a DIFFERENT generation that also PASSES must NOT certify either"; then
    case "$OUT" in
      *"PREMERGE: NO-C-VERDICT"*) ok "v1/freshpass: refused under the NO-C-VERDICT verdict" ;;
      *) bad "v1/freshpass: must refuse with NO-C-VERDICT (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"CHANGED between"*) ok "v1/freshpass: the refusal says the audit CHANGED between the two evaluations" ;;
      *) bad "v1/freshpass: the refusal must say the audit changed between the evaluations (got: $OUT)" ;;
    esac
    case "$OUT" in
      *report-nonce*) ok "v1/freshpass: and it NAMES the field that changed (the generation), not merely that something did" ;;
      *) bad "v1/freshpass: the refusal must name the changed field (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"PREMERGE: OK"*) bad "v1/freshpass: PREMERGE: OK was emitted on a refusing run" ;;
      *) ok "v1/freshpass: and no PREMERGE: OK line was emitted" ;;
    esac
  fi
  v1_restore || true
fi

# (d) STRUCTURAL — THE REPEAT EXISTS, THE EARLY CALL IS KEPT, AND THE ORDER IS THE ONE THE RULING
#     REQUIRES. Behavioural cases only cover the interleaves someone thought of; these pin the
#     arrangement, which is the thing job 290's ruling is actually about.
if LC_ALL=C grep -q '^c_revalidate$' "$ASSERT"; then
  ok "v1/structural: the C verdict is re-validated by a second, explicit call"
else
  bad "v1/structural: no re-validation call survives — the C verdict is validated outside the window it certifies"
fi
if [ "$(LC_ALL=C grep -c '^c_evaluate$' "$ASSERT" || true)" -eq 1 ]; then
  ok "v1/structural: the EARLY evaluation is RETAINED (it fails fast before the advisory and the network call)"
else
  bad "v1/structural: the early c_evaluate call is not present exactly once — job 290's remedy keeps it"
fi
V1_LN_FIRST="$(LC_ALL=C grep -n '^c_evaluate$' "$ASSERT" | LC_ALL=C head -1 | LC_ALL=C cut -d: -f1)"
V1_LN_GH="$(LC_ALL=C grep -n 'gh pr view "\$pr"' "$ASSERT" | LC_ALL=C head -1 | LC_ALL=C cut -d: -f1)"
V1_LN_REVAL="$(LC_ALL=C grep -n '^c_revalidate$' "$ASSERT" | LC_ALL=C head -1 | LC_ALL=C cut -d: -f1)"
V1_LN_OK="$(LC_ALL=C grep -n "^printf 'PREMERGE: OK" "$ASSERT" | LC_ALL=C head -1 | LC_ALL=C cut -d: -f1)"
if [ -n "$V1_LN_FIRST" ] && [ -n "$V1_LN_GH" ] && [ -n "$V1_LN_REVAL" ] && [ -n "$V1_LN_OK" ]; then
  ok "v1/structural: all four ordering anchors were located (the assertion below is not vacuous)"
  if [ "$V1_LN_FIRST" -lt "$V1_LN_GH" ] && [ "$V1_LN_GH" -lt "$V1_LN_REVAL" ] &&
    [ "$V1_LN_REVAL" -lt "$V1_LN_OK" ]; then
    ok "v1/structural: the re-validation sits AFTER the gh call and BEFORE any line a reader takes as certification"
  else
    bad "v1/structural: the order is wrong (early=$V1_LN_FIRST gh=$V1_LN_GH reval=$V1_LN_REVAL ok=$V1_LN_OK)"
  fi
else
  bad "v1/structural: an ordering anchor could not be located (early=$V1_LN_FIRST gh=$V1_LN_GH reval=$V1_LN_REVAL ok=$V1_LN_OK)"
  bad "v1/structural: so the ordering property is UNMEASURED, which is never read as satisfied"
fi
# THE SECOND EVALUATION MUST MEASURE, NOT INHERIT. Round 9's byte comparison and round 10's nonce
# match both fail CLOSED on an empty capture, so a re-validation that left the FIRST observation in
# place would compare the record against a capture taken before the window — a different property,
# and one that reads as satisfied while the second evaluation never captured anything.
V1_FNBODY="$(LC_ALL=C awk '
  $0 == "c_revalidate() {" { inf = 1 }
  inf { print }
  inf && $0 == "}" { exit }
' "$ASSERT" 2>/dev/null || true)"
if [ -n "$V1_FNBODY" ]; then
  ok "v1/structural: the re-validation function body was extracted (the assertion below is not vacuous)"
else
  bad "v1/structural: the re-validation function body could not be extracted, so the reset is UNMEASURED"
fi
# ASSERTED INSIDE THE FUNCTION, not file-wide: the C_* globals are initialised to "" at the top of
# the script, so a whole-file grep for the reset PASSES on the pre-fix artifact — measured, it did,
# while no re-validation existed at all. A structural assert satisfiable by the code it is meant to
# require is not an assert.
V1_RESET=0
for V1_G in C_TOKEN C_TOKEN_REPORT C_STAGE_HEAD C_STAGE_RECORD C_STAGE_NONCE C_STAGE_NONCE_N; do
  case "$V1_FNBODY" in
    *"$V1_G="'""'*) V1_RESET=$((V1_RESET + 1)) ;;
  esac
done
if [ "$V1_RESET" -eq 6 ]; then
  ok "v1/structural: all 6 captured-observation fields are RESET inside the re-validation, so it measures afresh"
else
  bad "v1/structural: only $V1_RESET of 6 captured-observation fields are reset inside the re-validation — it inherits the first observation"
fi
# AND THE SECOND EVALUATION IS THE SHIPPED ONE, not a re-implementation of the binding: a shortcut
# past `c_evaluate` would be a second copy of round 3/9/10's asserts and a second place to drift.
case "$V1_FNBODY" in
  *"  c_evaluate"*) ok "v1/structural: the re-validation runs the SAME evaluation, not a second implementation of the binding" ;;
  *) bad "v1/structural: the re-validation does not call c_evaluate — the binding is implemented twice" ;;
esac

# --- 44s: A CAPTURED PATH IS NOT THE PATH (round 18, X1) ----------------------
# THE FINDING (roborev job 397, premerge-assert.sh:1316). `c_stage_root` captured
# `git rev-parse --show-toplevel` in a command substitution, and every one of its four callers
# captured `c_stage_root` in a SECOND one — so a trailing newline was stripped TWICE. A checkout
# whose DIRECTORY NAME ends in an LF therefore resolved to a DIFFERENT, EXISTING SIBLING path, and
# the AUTO path located, bound and read THAT directory's stage records. It is #3616's
# peer-artifact class reached through a lossy capture instead of a recency scan, and
# `c_assert_head_binds_certified` cannot see it: HEAD is read in the CWD — the real lane, so it
# binds — while the ARTIFACT comes from the sibling.
#
# ROUND 13 (S2) ENUMERATED TRAILING-NEWLINE STRIPPING AND DECLARED IT HARMLESS. That was right
# about the stage RECORD's CONTENT (every grammar here is per-line and column-zero anchored) and
# wrong about a PATH, whose stripped bytes are part of its identity. The rule this section pins: a
# lossy-capture conclusion must be RE-DERIVED PER CONSUMER, never carried.
#
# HOW THE ROOT IS MADE OBSERVABLE. `No 'c' stage was ever OPENED` prints no root, so a bare
# before/after on the exit code would prove nothing about WHICH directory was searched. So the
# sibling is given TWO stage records: the AMBIGUOUS refusal is the ONE branch that PRINTS the root
# it enumerated. With the fix the real root is searched (0 records, `never OPENED`); without it the
# sibling is (2 records, AMBIGUOUS, naming the sibling path). Textually distinct, and the text
# names the directory.
X1_D="$T/x1"
mkdir -p "$X1_D"
X1_FLOW="$X1_D/flow"
mkdir -p "$X1_FLOW"
x1_ok=1
cp "$ASSERT" "$X1_FLOW/premerge-assert.sh" 2>/dev/null || x1_ok=0
cp "$SCRIPT_DIR/../flow/review-stage.sh" "$X1_FLOW/review-stage.sh" 2>/dev/null || x1_ok=0
printf '%s\n' "$NEUTRAL_ADV" >"$X1_FLOW/base-staleness.sh" 2>/dev/null || x1_ok=0
chmod +x "$X1_FLOW/base-staleness.sh" 2>/dev/null || true

# x1_repo <outvar> <dirname> — a synthetic DESIGN-ROUTED repository at a LITERAL directory name,
# ASSIGNED to <outvar>. `c_repo` above PRINTS its path, so it carries the very defect under test
# and cannot build this fixture: a trailing-LF name would arrive as its sibling and the case would
# run in an ordinary directory, passing for the wrong reason. `printf -v` is byte-faithful.
x1_repo() {
  local out="$1" d="$X1_D/$2"
  printf -v "$out" '%s' '' 2>/dev/null || return 1
  mkdir -p "$d" 2>/dev/null || return 1
  git init -q -b mainline "$d" >/dev/null 2>&1 || return 1
  git -C "$d" config user.email t@t >/dev/null 2>&1 || return 1
  git -C "$d" config user.name t >/dev/null 2>&1 || return 1
  printf '.review-stage/\n' >"$d/.gitignore" 2>/dev/null || return 1
  printf 'seed\n' >"$d/README.md" 2>/dev/null || return 1
  git -C "$d" add -A >/dev/null 2>&1 || return 1
  git -C "$d" commit -q -m seed >/dev/null 2>&1 || return 1
  git -C "$d" update-ref refs/remotes/origin/main mainline >/dev/null 2>&1 || return 1
  git -C "$d" checkout -q -b feature >/dev/null 2>&1 || return 1
  mkdir -p "$d/openspec/changes/a-design-routed-slug" 2>/dev/null || return 1
  printf 'the PR content\n' >"$d/openspec/changes/a-design-routed-slug/proposal.md" 2>/dev/null || return 1
  git -C "$d" add -A >/dev/null 2>&1 || return 1
  git -C "$d" commit -q -m "the PR" >/dev/null 2>&1 || return 1
  printf -v "$out" '%s' "$d"
  return 0
}

# x1_run <script> <repo> <want> <desc> — `run_in_repo` with the repository passed as a VALUE
# rather than resolved from a substitution, and against a named scratch assert.
x1_run() {
  local script="$1" d="$2" want="$3" desc="$4" sha f
  sha=$(git -C "$d" rev-parse HEAD 2>/dev/null) || sha=""
  if [ -z "$sha" ]; then bad "$desc: could not resolve the fixture HEAD"; return 1; fi
  f="$X1_D/gate-$(basename "$script").txt"
  emit_summary_block "$FULL_S" "$FULL_E" "-" \
    "$(printf '%.7s' "$sha")" "$(printf '%.12s' "$sha")" PASS PASS >"$f"
  OUT=$(cd "$d" && PATH="$BIN:$PATH" MOCK_GH_OUT="$sha OPEN" MOCK_GH_FAIL=0 \
    bash "$script" 2421 "$sha" "$f" --c-verdict AUTO 2>&1)
  RC=$?
  if [ "$RC" -ne "$want" ]; then
    bad "$desc (exit $RC, wanted $want)"
    printf '     output: %s\n' "$OUT"
    return 1
  fi
  return 0
}

X1_TR=""      # the real lane: a checkout whose directory name ENDS in an LF
X1_SIB=""     # the sibling the stripped capture names
X1_TR_NAME="lanetrail
"
if [ "$x1_ok" -eq 1 ] && x1_repo X1_TR "$X1_TR_NAME" && [ -n "$X1_TR" ] &&
  case "$X1_TR" in *"
") true ;; *) false ;; esac; then
  ok "x1 fixture: a design-routed checkout whose path ENDS in an LF was built (asserted on the path, not assumed)"
else
  bad "x1 fixture: the trailing-LF checkout could not be built — every case below is UNMEASURED"
  X1_TR=""
fi
# THE SIBLING GETS TWO STAGE RECORDS, so enumerating it is AMBIGUOUS and therefore PRINTS the root.
# `$'\n'` and NOT `"$(printf '\n')"`: the latter is itself a trailing-newline-stripping capture and
# expands to the EMPTY string, so the suffix removal would remove nothing — this section's own
# subject, one line deep.
if [ -n "$X1_TR" ] && x1_repo X1_SIB "lanetrail" && [ -n "$X1_SIB" ] &&
  [ "$X1_SIB" = "${X1_TR%$'\n'}" ]; then
  X1_SHA=$(git -C "$X1_SIB" rev-parse HEAD 2>/dev/null) || X1_SHA=""
  mkdir -p "$X1_SIB/.review-stage/issue-3751" "$X1_SIB/.review-stage/issue-9999" 2>/dev/null || true
  for X1_I in 3751 9999; do
    printf 'kind: c\nissue: %s\nagent: spec-auditor\nspawned-at: 2026-09-01T00:00:00Z\nspawned-epoch: 1\ndeadline-secs: 1800\nhead-sha: %s\nreport-nonce: peergenA%s\nreopen-count: 0\n' \
      "$X1_I" "$X1_SHA" "$X1_I" >"$X1_SIB/.review-stage/issue-$X1_I/c.stage" 2>/dev/null || true
    printf 'result: PASS\n\n## Findings\n\nnone.\n' \
      >"$X1_SIB/.review-stage/issue-$X1_I/c.peergenA$X1_I.md" 2>/dev/null || true
  done
  if [ -f "$X1_SIB/.review-stage/issue-3751/c.stage" ] &&
    [ -f "$X1_SIB/.review-stage/issue-9999/c.stage" ]; then
    ok "x1 fixture: the SIBLING holds TWO peer stage records (so enumerating it is AMBIGUOUS, the one refusal that PRINTS the root)"
  else
    bad "x1 fixture: the sibling's peer records were not written — the before/after below cannot distinguish the two roots"
    X1_TR=""
  fi
else
  bad "x1 fixture: the sibling is missing or is not the stripped form of the real path — the peer-read case proves nothing"
  X1_TR=""
fi

# THE RED CONTROL, BUILT BY SUBSTITUTING THE ARTIFACT (#3312's corollary for tests — never a
# settable seam). A scratch copy of the assert gets the PRE-FIX lossy resolver appended after the
# real one, so the later definition wins while the call sites are untouched. A green above proves
# nothing unless this control REDS in the pre-fix direction and NAMES the sibling.
# BESIDE `review-stage.sh`, not in the parent: this script locates the enforcer at
# `$self_dir/review-stage.sh`, so a scratch assert one directory up refuses as TOOL-FAILURE
# (exit 3) and the control would measure the harness rather than the resolver. Caught by the
# expected-exit assert in `x1_run`, which is why that assert names the code it wanted.
X1_LOSSY="$X1_FLOW/lossy.sh"
X1_LOSSY_OK=0
if [ "$x1_ok" -eq 1 ]; then
  X1_OVERRIDE='c_stage_root() { local r; r=$(git rev-parse --show-toplevel 2>/dev/null) || r=""; [ -n "$r" ] || r="$PWD"; C_STAGE_ROOT="$r"; }   # X1_LOSSY_PLANT'
  X1_ANCHOR='# c_record_bytes <path> — ONE OBSERVATION OF THE STAGE RECORD'
  X1_A="$X1_ANCHOR" X1_O="$X1_OVERRIDE" LC_ALL=C awk '
    BEGIN { a = ENVIRON["X1_A"]; o = ENVIRON["X1_O"]; done = 0 }
    index($0, a) > 0 && done == 0 { print o; print ""; print $0; done = 1; next }
    { print }
  ' "$X1_FLOW/premerge-assert.sh" >"$X1_LOSSY" 2>/dev/null || true
  if [ -s "$X1_LOSSY" ] && LC_ALL=C grep -q 'X1_LOSSY_PLANT' "$X1_LOSSY"; then
    X1_LOSSY_OK=1
    ok "x1 control: the pre-fix lossy resolver was planted into a scratch assert (asserted, not assumed)"
  else
    bad "x1 control: the plant did NOT land, so the green case below is unattributable"
  fi
else
  bad "x1 control: the scratch assert could not be prepared"
fi

if [ -n "$X1_TR" ] && [ "$X1_LOSSY_OK" -eq 1 ]; then
  # (a) RED: with the lossy capture, AUTO enumerates the SIBLING and says so.
  if x1_run "$X1_LOSSY" "$X1_TR" 2 \
    "x1/red: the pre-fix lossy capture refuses — but for the WRONG reason, having read another lane"; then
    case "$OUT" in
      *"stage records exist under $X1_SIB/.review-stage/"*)
        ok "x1/red: it enumerated the SIBLING lane, and the refusal NAMES that directory — the peer-artifact defect, reproduced" ;;
      *) bad "x1/red: the lossy capture did not name the sibling, so the control does not reproduce the finding (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"No 'c' stage was ever OPENED"*)
        bad "x1/red: the control reported the REAL root's answer, so it is not reproducing the pre-fix behaviour" ;;
      *) ok "x1/red: and it did NOT report the real root's answer, so the two directions are textually distinguishable" ;;
    esac
  else
    bad "x1/red: the control did not refuse at all (2/3)"
    bad "x1/red: the control did not refuse at all (3/3)"
  fi
  # (b) GREEN: the shipped resolver searches the REAL root, finds nothing there, and never names
  #     the sibling. `never OPENED` is the correct answer for this lane: the trailing-LF checkout
  #     cannot open a stage at all, because `review-stage.sh` REFUSES such a root outright.
  if x1_run "$X1_FLOW/premerge-assert.sh" "$X1_TR" 2 \
    "x1/green: the shipped resolver refuses — naming THIS lane's absence, not a peer's records"; then
    case "$OUT" in
      *"No 'c' stage was ever OPENED"*)
        ok "x1/green: the AUTO locator searched the REAL root (the trailing LF survived the resolution)" ;;
      *) bad "x1/green: the refusal does not name this lane's own absence (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"$X1_SIB/.review-stage/"*)
        bad "x1/green: the sibling's directory is still named — a peer lane's stage is still reachable" ;;
      *) ok "x1/green: and the SIBLING lane is never named, so no peer artifact was enumerated" ;;
    esac
    case "$OUT" in
      *"PREMERGE: OK"* | *"C-VERDICT PASS"* | *"C-VERDICT AUTHOR-PERFORMED"*)
        bad "x1/green: a merge-proceeding verdict was emitted off a checkout whose root could not be carried" ;;
      *) ok "x1/green: and NO merge-proceeding verdict is emitted" ;;
    esac
  else
    bad "x1/green: the shipped assert did not refuse (2/4)"
    bad "x1/green: the shipped assert did not refuse (3/4)"
    bad "x1/green: the shipped assert did not refuse (4/4)"
  fi
  # (c) The peer's records are untouched: this lane neither read them as its own nor wrote to them.
  if [ -f "$X1_SIB/.review-stage/issue-3751/c.stage" ] &&
    [ -f "$X1_SIB/.review-stage/issue-9999/c.stage" ] &&
    [ ! -e "$X1_SIB/.review-stage/issue-2421" ]; then
    ok "x1/green: the peer lane's stage records are intact and no new directory was created in it"
  else
    bad "x1/green: the peer lane's .review-stage was modified"
  fi
else
  bad "x1: UNMEASURED (1/8) — fixture or control absent"
  bad "x1: UNMEASURED (2/8)"
  bad "x1: UNMEASURED (3/8)"
  bad "x1: UNMEASURED (4/8)"
  bad "x1: UNMEASURED (5/8)"
  bad "x1: UNMEASURED (6/8)"
  bad "x1: UNMEASURED (7/8)"
  bad "x1: UNMEASURED (8/8)"
fi

# (d) STRUCTURAL — THE CHANNEL IS REMOVED, NOT MADE FAITHFUL. `c_stage_root` assigns a global and
#     prints nothing, so there is nothing for a fifth call site to capture. That is stronger than
#     one more careful capture: it makes the defect UNEXPRESSIBLE at the call sites.
X1_SRC="$SCRIPT_DIR/../flow/premerge-assert.sh"
# THE CODE IS READ FROM A FILE, AND EVERY PIN GREPS THAT FILE — NEVER `printf … | grep -q`.
# Under this suite's `pipefail`, a MATCHING `grep -q` exits immediately, printf takes SIGPIPE, and
# the PIPELINE status becomes 141 — so a pin over a >64KiB body FAILED exactly when its needle was
# PRESENT, and only intermittently, since a body that fits the pipe buffer completes first. Two of
# these pins flaked that way before the read moved to a file. It is the same family as #3400's
# "read by redirection, never a pipe", one shell feature over.
X1_CODE_F="$T/x1-shipped-code.txt"
LC_ALL=C sed -e 's/^[[:space:]]*#.*$//' "$X1_SRC" >"$X1_CODE_F" 2>/dev/null || : >"$X1_CODE_F"
X1_CODE=""
[ -s "$X1_CODE_F" ] && X1_CODE=present
if [ -n "$X1_CODE" ]; then
  ok "x1/structural: the shipped assert's code was extracted (the pins below are not vacuous)"
else
  bad "x1/structural: the shipped assert could not be read — the pins below are UNMEASURED"
fi
# Needle SPLIT so this guard cannot match its own source line.
X1_CAP="\$(c_stage""_root)"
if [ -n "$X1_CODE" ] && ! LC_ALL=C grep -qF "$X1_CAP" "$X1_CODE_F"; then
  ok "x1/structural: the root is never CAPTURED — no call site can strip a byte off it"
else
  bad "x1/structural: a command substitution of the resolver survives, which strips the trailing newline back off the path"
fi
if LC_ALL=C grep -q 'C_STAGE_ROOT="\$root"' "$X1_CODE_F"; then
  ok "x1/structural: the resolver publishes the root by ASSIGNMENT to the shared global"
else
  bad "x1/structural: the resolver does not assign C_STAGE_ROOT — the one-resolution property is gone"
fi
# The four consumers must READ the global. Counted, so a new consumer that resolves for itself is
# visible as a count that no longer matches its call sites.
X1_READS="$(LC_ALL=C grep -c '"\$C_STAGE_ROOT' "$X1_CODE_F" || true)"
X1_CALLS="$(LC_ALL=C grep -c '^[[:space:]]*c_stage_root$' "$X1_CODE_F" || true)"
if [ "$X1_READS" -eq 4 ] && [ "$X1_CALLS" -eq 4 ]; then
  ok "x1/structural: all 4 consumers CALL the resolver and READ the global (4 calls, 4 reads)"
else
  bad "x1/structural: $X1_CALLS resolver calls against $X1_READS global reads (want 4 and 4) — a consumer resolves or captures for itself"
fi
# THE SENTINEL IS WHAT MAKES THE CAPTURE FAITHFUL, and its completeness is asserted by TWO signals
# (round 13's lesson): the sentinel AND the status, the latter captured with `|| rc=$?` rather than
# an `if ! …` that can only ever read zero.
X1_FN="$(LC_ALL=C sed -n '/^c_stage_root() {$/,/^}$/p' "$X1_SRC" 2>/dev/null || true)"
case "$X1_FN" in
  *"printf 'E'; } )\" || rc=\$?"*)
    ok "x1/structural: the resolver's capture carries the sentinel AND captures git's status with || rc=\$?" ;;
  *) bad "x1/structural: the resolver's capture is not the two-signal form (got: $X1_FN)" ;;
esac
case "$X1_FN" in
  *'root="${raw%$'*)
    ok "x1/structural: and it removes EXACTLY ONE trailing newline — git's own terminator, never the directory name's" ;;
  *) bad "x1/structural: the resolver does not strip exactly one trailing newline" ;;
esac
# AND THE SAME CLASS AT THE SECOND SITE THIS SWEEP FOUND: the script's own directory, which is how
# `review-stage.sh` — the ENFORCER of the verdict this script refuses to merge without — is
# located. `${BASH_SOURCE[0]%/*}` is pure parameter expansion and cannot lose a byte.
X1_DN="\$(dir""name \"\${BASH_SOURCE[0]}\")"
if [ -n "$X1_CODE" ] && ! LC_ALL=C grep -qF "$X1_DN" "$X1_CODE_F"; then
  ok "x1/structural: the script's own directory is no longer resolved through a dirname substitution"
else
  bad "x1/structural: self_dir still goes through \$(dirname …), which strips a trailing newline and names a SIBLING"
fi
# BOTH HALVES, because either alone is satisfiable without the property: the source has to be
# bound by expansion AND the directory taken by expansion. The first draft pinned a spelling the
# code does not use (`${BASH_SOURCE[0]%/*}` in one step) and red on a correct fix — the guard that
# reds on correct input is the guard agents learn to waive.
if LC_ALL=C grep -qF 'self_src="${BASH_SOURCE[0]:-}"' "$X1_CODE_F" &&
  LC_ALL=C grep -qF 'self_dir_rel="${self_src%/*}"' "$X1_CODE_F"; then
  ok "x1/structural: it takes the source AND its directory by parameter expansion, which is byte-faithful by construction"
else
  bad "x1/structural: the byte-faithful expansion is absent, so the enforcer's location can still be mislocated"
fi

# --- 44t: A FOREIGN VERDICT REACHED THROUGH A SYMLINKED PATH COMPONENT (round 20, Z1) ---
# THE HARM, AT THE MERGE POINT. `--c-verdict AUTO` locates this worktree's C stage by globbing
# `<root>/.review-stage/issue-*/c.stage` and then reads its verdict with `review-stage.sh verdict`.
# Round 19 gave that reader a `[ -L ]` test on the LEAF of each artifact and NOTHING on the
# components above it, so a symlink at `.review-stage/` (or at `issue-<N>/`) supplied this lane
# with ANOTHER TREE's stage — record, nonce and clean report together — and every leaf test
# answered about the far end of the link. The glob traverses it, the head-sha binding is satisfied
# by a peer standing at the same commit, and `PREMERGE: OK` follows.
#
# IT NEEDS NO RACE. The link is planted at any earlier moment and simply followed; the TOCTOU
# window between a check and its open is #3929's and is not this.
#
# THIS CASE IS WHY THE FIX BELONGS IN `review-stage.sh` AND NOT HERE: premerge-assert consumes the
# verdict LINE, so the protection is INHERITED — but "should be inherited" is a prediction, and the
# whole point of this issue is that a prediction is not a measurement. Asserted end to end.
Z1_LANE=$(c_repo z1lane design) || Z1_LANE=""
Z1_PEER=$(c_repo z1peer design) || Z1_PEER=""
Z1_LANE_SHA=""
if [ -n "$Z1_LANE" ] && [ -n "$Z1_PEER" ]; then
  Z1_LANE_SHA=$(git -C "$Z1_LANE" rev-parse HEAD 2>/dev/null) || Z1_LANE_SHA=""
fi
# A FIXTURE THAT COULD NOT BE BUILT IS A RED, NOT SILENCE: without this the whole section would
# emit ZERO assertions and the suite would report `failed: 0` over a block that never ran — this
# suite's own case-floor subject, one section down.
if [ -z "$Z1_LANE_SHA" ]; then
  bad "z1/premerge fixture: could not build the two synthetic repositories — the whole section would be vacuous"
fi
if [ -n "$Z1_LANE_SHA" ]; then
  # THE PEER'S STAGE IS A REAL ONE, built with the shipped tool: a hand-made record would be
  # refused for a reason that is not this case's, and the case would pass vacuously.
  if (cd "$Z1_PEER" && bash "$NEUTRAL_DIR/review-stage.sh" open c --issue 3751 \
      --agent spec-auditor >/dev/null 2>&1) &&
    printf 'result: PASS\n\n## Findings\n\nanother lane audit.\n' \
      >"$(SR_REPORT "$Z1_PEER" 3751 c)"; then
    ok "z1/premerge fixture: the PEER tree holds a real, PASSING c stage"
  else
    bad "z1/premerge fixture: could not build the peer stage — the case would be vacuous"
    Z1_LANE_SHA=""
  fi
fi
Z1_PEER_REC="$Z1_PEER/.review-stage/issue-3751/c.stage"
if [ -n "$Z1_LANE_SHA" ]; then
  # THE PEER'S RECORD IS RE-STAMPED TO THE LANE'S CERTIFIED SHA, so the head-sha binding (44f) is
  # SATISFIED and cannot be what refuses. Without this the case would go green pre-fix and prove
  # nothing about the read path — it is the shape of a real peer lane standing at the same commit,
  # which on this fleet is the ordinary case (every lane is a worktree of one repository).
  if LC_ALL=C awk -v s="$Z1_LANE_SHA" '/^head-sha: /{print "head-sha: " s; next}{print}' \
      "$Z1_PEER_REC" >"$Z1_PEER_REC.new" 2>/dev/null &&
    mv -f "$Z1_PEER_REC.new" "$Z1_PEER_REC" &&
    LC_ALL=C grep -q "^head-sha: $Z1_LANE_SHA\$" "$Z1_PEER_REC"; then
    ok "z1/premerge fixture: the peer record now names the LANE's certified sha, so the stage binding cannot be what refuses"
  else
    bad "z1/premerge fixture: could not re-stamp the peer record — the refusal below could not be attributed to the read path"
    Z1_LANE_SHA=""
  fi
fi
if [ -n "$Z1_LANE_SHA" ]; then
  # THE POSITIVE CONTROL FIRST: the peer's stage, read IN THE PEER'S OWN TREE, certifies. So the
  # refusal below is about the LINK and not about this artifact, this sha or this fixture.
  if run_in_repo "$Z1_PEER" 0 \
    "z1/premerge control: in its OWN tree the peer's stage certifies (so the bait is live)" \
    --c-verdict AUTO; then
    case "$OUT" in
      *"PREMERGE: C-VERDICT PASS"*) ok "z1/premerge control: reported C-VERDICT PASS" ;;
      *) bad "z1/premerge control: must report C-VERDICT PASS (got: $OUT)" ;;
    esac
  fi
  # NOW THE DEFECT: the LANE never opened a stage, and a link at `.review-stage/` supplies the
  # peer's. This is the realistic shape — nothing to overwrite, nothing to notice.
  if ln -s "$Z1_PEER/.review-stage" "$Z1_LANE/.review-stage" 2>/dev/null \
     && [ -L "$Z1_LANE/.review-stage" ]; then
    ok "z1/premerge: PREMISE — the lane's .review-stage/ is a symlink at the peer tree's stage root"
  else
    bad "z1/premerge: PREMISE — could not plant the link; the assertions below would be vacuous"
    Z1_LANE_SHA=""
  fi
fi
if [ -n "$Z1_LANE_SHA" ]; then
  if run_in_repo "$Z1_LANE" 2 \
    "z1/premerge: a FOREIGN PASS reached through a symlinked PATH COMPONENT is REFUSED at the merge point" \
    --c-verdict AUTO; then
    case "$OUT" in
      *"PREMERGE: NO-C-VERDICT"*) ok "z1/premerge: refused under the NO-C-VERDICT verdict" ;;
      *) bad "z1/premerge: must refuse with NO-C-VERDICT (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"PREMERGE: C-VERDICT PASS"*)
        bad "z1/premerge: a FOREIGN tree's PASS certified this lane's merge — the Z1 defect is live at the merge point" ;;
      *) ok "z1/premerge: no PASS token is emitted for a verdict read through a symlinked component" ;;
    esac
    case "$OUT" in
      *"PREMERGE: OK"*) bad "z1/premerge: the merge was cleared over a foreign verdict" ;;
      *) ok "z1/premerge: and PREMERGE: OK is never reached" ;;
    esac
    # THE INHERITED CAUSE IS THE ONE THIS SCRIPT PRINTS, so an operator reads the LEVEL from the
    # merge-point output and does not have to re-run the stage tool to find out which link is wrong.
    case "$OUT" in
      *"stage record path has a symlinked parent directory"*)
        ok "z1/premerge: and review-stage.sh's own cause is carried through, naming the LEVEL (a parent component, not the leaf)" ;;
      *) bad "z1/premerge: the refusal does not carry the component-level cause (got: $OUT)" ;;
    esac
  fi
fi

# --- 44h: THE STRUCTURAL EMIT-BOUNDARY GUARD (round 7, L1b) -------------------
# The mirror of test_review_stage.sh section 18, for this script. See
# scripts/tests/lib/emit-boundary-scan.sh for why the guard exists (the boundary was bypassed at a
# NEW site in three consecutive rounds, so the answer is a mechanism rather than a fourth patch) and
# for the scope it DECLARES on every run. The positive control is the requirement, not the clean
# run: a scanner that flagged nothing would exit 0 exactly as this one does.
EBS="$SCRIPT_DIR/lib/emit-boundary-scan.sh"
if [ ! -f "$EBS" ]; then
  # TEN, matching the ten assertions the else-branch emits, so the case floor is unaffected by
  # which branch runs (round 9 added the four compound-statement control assertions below).
  bad "emit-guard: $EBS is missing — the structural guard did not run (1/10)"
  bad "emit-guard: the same absence (2/10)"
  bad "emit-guard: the same absence (3/10)"
  bad "emit-guard: the same absence (4/10)"
  bad "emit-guard: the same absence (5/10)"
  bad "emit-guard: the same absence (6/10)"
  bad "emit-guard: the same absence (7/10)"
  bad "emit-guard: the same absence (8/10)"
  bad "emit-guard: the same absence (9/10)"
  bad "emit-guard: the same absence (10/10)"
else
  EBS_OUT="$(bash "$EBS" "$ASSERT" 2>&1)"; EBS_RC=$?
  if [ "$EBS_RC" -eq 0 ]; then
    ok "emit-guard: the SHIPPED premerge-assert.sh is CLEAN — every value on an emitted line is routed or allowlisted"
  else
    bad "emit-guard: the shipped premerge-assert.sh has an emit-boundary BYPASS: $EBS_OUT"
  fi
  case "$EBS_OUT" in
    *"NOT COVERED"*) ok "emit-guard: the scan DECLARES what it does not cover, on every run" ;;
    *) bad "emit-guard: the scan did not declare its scope (got: $EBS_OUT)" ;;
  esac
  case "$EBS_OUT" in
    *"in-scope emit site(s)"*) ok "emit-guard: and it reports HOW MANY sites it examined — a count, not an adjective" ;;
    *) bad "emit-guard: the scan did not report its subject count (got: $EBS_OUT)" ;;
  esac
  EBS_D="$T/ebs"; mkdir -p "$EBS_D"
  # The plant goes on the `PREMERGE: OK` line — the emitted line this whole check is about.
  # SINGLE-QUOTED sed, so `$PLANTED_BYPASS_VALUE` is LITERAL text in the planted line rather than
  # being expanded by this suite's own shell; the address uses `.` for the quote character.
  LC_ALL=C sed -e '/^printf .PREMERGE: OK /s/$/ "$PLANTED_BYPASS_VALUE"/' \
    "$ASSERT" >"$EBS_D/premerge-assert.sh" 2>/dev/null || true
  if [ -f "$EBS_D/premerge-assert.sh" ] && LC_ALL=C grep -q 'PLANTED_BYPASS_VALUE' "$EBS_D/premerge-assert.sh"; then
    ok "emit-guard/control: the plant landed in the scratch copy (asserted, not assumed)"
  else
    bad "emit-guard/control: the plant did NOT land, so the control below proves nothing"
  fi
  EBS_POUT="$(bash "$EBS" "$EBS_D/premerge-assert.sh" 2>&1)"; EBS_PRC=$?
  if [ "$EBS_PRC" -ne 0 ]; then
    ok "emit-guard/control: the guard REDS on a planted bypass"
  else
    bad "emit-guard/control: the guard reported CLEAN on a planted bypass — it proves nothing (got: $EBS_POUT)"
  fi
  case "$EBS_POUT" in
    *PLANTED_BYPASS_VALUE*) ok "emit-guard/control: and it NAMES the offending value, so the red is attributable" ;;
    *) bad "emit-guard/control: the guard red without naming the planted value (got: $EBS_POUT)" ;;
  esac
  # (b) THE COMPOUND-STATEMENT POSITIVE CONTROL, REPRODUCING THE INSTANCE THE GUARD MISSED (#3751
  #     round 9, N3). The plant above is at the START of a line, which the FIRST version of this
  #     guard could see; its blind spot was every COMPOUND statement, because its scope was anchored
  #     `^[[:space:]]*(printf|echo)[[:space:]]`. The measured consequence was RIGHT HERE: the
  #     NO-GATE-OF-RECORD block printed the caller-supplied `$delta_file` unrouted from a line
  #     beginning `[ -n "$delta_file" ] &&`, and the guard reported this script CLEAN. So this
  #     control puts that instance back: the routing is stripped from that exact line and the
  #     planted name put in its place.
  EBS_C="$T/ebs-compound"; mkdir -p "$EBS_C"
  # The address narrows to the one refusal line; the substitution swaps the ROUTING call for the
  # planted name, leaving the compound `[ -n … ] &&` prefix intact — which is the whole point.
  LC_ALL=C sed -e '/delta summary file/s|"\$(c_safe_display "\$delta_file")"|"\$PLANTED_COMPOUND_BYPASS"|' \
    "$ASSERT" >"$EBS_C/premerge-assert.sh" 2>/dev/null || true
  EBS_CLINE="$(LC_ALL=C grep -n 'PLANTED_COMPOUND_BYPASS' "$EBS_C/premerge-assert.sh" 2>/dev/null | LC_ALL=C head -1 || true)"
  if [ -n "$EBS_CLINE" ]; then
    ok "emit-guard/compound: the compound plant landed in the scratch copy (asserted, not assumed)"
  else
    bad "emit-guard/compound: the compound plant did NOT land, so this control proves nothing"
  fi
  # THE PLANT MUST REALLY BE COMPOUND, or this control is a duplicate of (a). Measured from the
  # planted line itself: its first word must not be the output command.
  case "$(printf '%s\n' "${EBS_CLINE#*:}" | LC_ALL=C sed -e 's/^[[:space:]]*//' -e 's/[[:space:]].*//')" in
    printf | echo)
      bad "emit-guard/compound: the planted statement BEGINS its line, so a line-anchored scope would have seen it too — this control does not test compound recognition (line: $EBS_CLINE)" ;;
    "")
      bad "emit-guard/compound: could not read the planted line's first word" ;;
    *)
      ok "emit-guard/compound: the planted statement does NOT begin its line (it is behind a [ … ] &&), which is exactly what the line-anchored scope could not see" ;;
  esac
  EBS_COUT="$(bash "$EBS" "$EBS_C/premerge-assert.sh" 2>&1)"; EBS_CRC=$?
  if [ "$EBS_CRC" -ne 0 ]; then
    ok "emit-guard/compound: the guard REDS on a bypass inside a COMPOUND statement"
  else
    bad "emit-guard/compound: the guard reported CLEAN on a compound-statement bypass — the round-7 blind spot is back (got: $EBS_COUT)"
  fi
  case "$EBS_COUT" in
    *PLANTED_COMPOUND_BYPASS*) ok "emit-guard/compound: and it NAMES the offending value, so the red is attributable" ;;
    *) bad "emit-guard/compound: the guard red without naming the planted value (got: $EBS_COUT)" ;;
  esac
fi

# --- 44n: THE OUTPUT PRIMITIVE MUST BE A LITERAL PRINTER (round 14, T2) --------
# The mirror of test_review_stage.sh section 27, for this script. Section 44h asserts that every
# VALUE on an emitted line is routed through `c_safe_display`; this one asserts that the printing
# COMMAND is a literal printer. A routed value is no protection if the primitive re-interprets what
# the boundary just neutralised — under the bash option `xpg_echo`, set by an INHERITED environment
# and never by this script, `echo` performs BACKSLASH ESCAPE PROCESSING on its argument, so a `\n`
# splits a line, a `\033` injects terminal control, a `\c` truncates, and octal `\075` manufactures
# a REAL `=`. Measured on `review-stage.sh`, whose `emit` had this exact shape: one legal directory
# name produced a SECOND column-zero `REVIEW-STAGE: … RESULT: PASS` line carrying real `key=` pairs.
#
# THIS SCRIPT USES NO `echo` TODAY — measured, zero occurrences — so the check is here to keep it
# that way, and the POSITIVE CONTROL is the requirement rather than the clean run. Its format
# arguments are all literals too; the second control plants a data-derived one, which is the same
# channel one step in (`%` and `\` in a format are interpreted).
if [ ! -f "$EBS" ]; then
  # TEN, matching the ten assertions the else-branch emits, so the case floor is unaffected by
  # which branch runs.
  bad "primitive-guard: $EBS is missing — the structural guard did not run (1/10)"
  bad "primitive-guard: the same absence (2/10)"
  bad "primitive-guard: the same absence (3/10)"
  bad "primitive-guard: the same absence (4/10)"
  bad "primitive-guard: the same absence (5/10)"
  bad "primitive-guard: the same absence (6/10)"
  bad "primitive-guard: the same absence (7/10)"
  bad "primitive-guard: the same absence (8/10)"
  bad "primitive-guard: the same absence (9/10)"
  bad "primitive-guard: the same absence (10/10)"
else
  PG_OUT="$(bash "$EBS" "$ASSERT" 2>&1)"; PG_RC=$?
  if [ "$PG_RC" -eq 0 ]; then
    ok "primitive-guard: the SHIPPED premerge-assert.sh is CLEAN — no echo, and every printf FORMAT is a literal"
  else
    bad "primitive-guard: the shipped premerge-assert.sh FAILS the guard: $PG_OUT"
  fi
  case "$PG_OUT" in
    *"printf statement(s)"*) ok "primitive-guard: the check REPORTS how many printf statements it examined — a count, not an adjective" ;;
    *) bad "primitive-guard: the primitive check reported no subject count, so it may not have run at all (got: $PG_OUT)" ;;
  esac
  case "$PG_OUT" in
    *"NOT COVERED (output primitive)"*) ok "primitive-guard: and it DECLARES what the primitive check does not cover, on every run" ;;
    *) bad "primitive-guard: the primitive check did not declare its own scope (got: $PG_OUT)" ;;
  esac
  # PLANT 1: an `echo` on the `PREMERGE: OK ` line — the emitted line this whole file is about —
  # and deliberately COMPOUND, so the control also proves the primitive walker is POSITIONAL and
  # not line-anchored (round 9's N3 blind spot, in the check added this round).
  PG_ED="$T/pg-echo"; mkdir -p "$PG_ED"
  LC_ALL=C sed -e '/^printf .PREMERGE: OK /s|.*|[ -n "$certified" ] \&\& echo "PREMERGE: OK $PLANTED_ECHO_PRIMITIVE"|' \
    "$ASSERT" >"$PG_ED/premerge-assert.sh" 2>/dev/null || true
  PG_ELINE="$(LC_ALL=C grep -n 'PLANTED_ECHO_PRIMITIVE' "$PG_ED/premerge-assert.sh" 2>/dev/null | LC_ALL=C head -1 || true)"
  if [ -n "$PG_ELINE" ]; then
    ok "primitive-guard/control: the echo plant landed in the scratch copy (asserted, not assumed)"
  else
    bad "primitive-guard/control: the echo plant did NOT land, so the control below proves nothing"
  fi
  case "$(printf '%s\n' "${PG_ELINE#*:}" | LC_ALL=C sed -e 's/^[[:space:]]*//' -e 's/[[:space:]].*//')" in
    echo)
      bad "primitive-guard/control: the planted echo begins its line, so this control does not test positional recognition (line: $PG_ELINE)" ;;
    "")
      bad "primitive-guard/control: could not read the planted line's first word" ;;
    *)
      ok "primitive-guard/control: the planted echo does NOT begin its line (it is behind a [ … ] &&), so the control also tests POSITIONAL recognition" ;;
  esac
  PG_EOUT="$(bash "$EBS" "$PG_ED/premerge-assert.sh" 2>&1)"; PG_ERC=$?
  if [ "$PG_ERC" -ne 0 ]; then
    ok "primitive-guard/control: the guard REDS on a planted echo"
  else
    bad "primitive-guard/control: the guard reported CLEAN on a planted echo — it proves nothing (got: $PG_EOUT)"
  fi
  case "$PG_EOUT" in
    *"output-primitive bypass"*) ok "primitive-guard/control: and it NAMES the check that failed, so the red is attributable to the primitive rather than to a value" ;;
    *) bad "primitive-guard/control: the guard red without naming the output-primitive check (got: $PG_EOUT)" ;;
  esac
  # PLANT 2: a printf whose FORMAT comes from a variable.
  PG_FD="$T/pg-fmt"; mkdir -p "$PG_FD"
  LC_ALL=C sed -e '/^printf .PREMERGE: OK /s|.*|printf "$PLANTED_FORMAT_PRIMITIVE" "$certified"|' \
    "$ASSERT" >"$PG_FD/premerge-assert.sh" 2>/dev/null || true
  if LC_ALL=C grep -q 'PLANTED_FORMAT_PRIMITIVE' "$PG_FD/premerge-assert.sh" 2>/dev/null; then
    ok "primitive-guard/format: the data-derived-format plant landed in the scratch copy"
  else
    bad "primitive-guard/format: the format plant did NOT land, so the control below proves nothing"
  fi
  PG_FOUT="$(bash "$EBS" "$PG_FD/premerge-assert.sh" 2>&1)"; PG_FRC=$?
  if [ "$PG_FRC" -ne 0 ]; then
    ok "primitive-guard/format: the guard REDS on a printf whose FORMAT came from a variable"
  else
    bad "primitive-guard/format: the guard reported CLEAN on a data-derived printf format (got: $PG_FOUT)"
  fi
  case "$PG_FOUT" in
    *PLANTED_FORMAT_PRIMITIVE*) ok "primitive-guard/format: and it NAMES the offending format, so the red is attributable" ;;
    *) bad "primitive-guard/format: the guard red without naming the planted format (got: $PG_FOUT)" ;;
  esac
fi

# --- 44o: THE GATE-OF-RECORD READ MUST GO THROUGH THE CAPTURE BOUNDARY (round 14, T1) ---
# THE THIRD SITE OF ROUND 13's DEFECT, in the OTHER half of this same script (roborev job 388, T1
# names the sibling site in `review-stage.sh`; this one the audit found while sweeping the class).
# Section 44m routed `c_parse_verdict`'s file read and `c_record_bytes` through `c_capture_map_nul`
# and left `_gate_awk` — the parser of the GATE OF RECORD, the artifact #3465 exists to require —
# reading its file RAW with `awk … <"$1"`. Measured on the shipped script:
#
#   $ printf '…\nRESULT: PA\0SS\n…' > g.txt
#   $ _gate_awk g.txt full | od -c              ->  v _ r e s u l t = P A \0 S S
#   $ out=$(_gate_awk g.txt full); …            ->  v_result=PASS
#
# gawk passes the NUL through the field and the CAPTURE in `gate_parse_file` removes it, so this
# script read `PASS` from a summary that does not contain it. Same shape, same round-13 lesson: a
# capture that normalises its input cannot be the thing that validates it.
#
# THE MAPPING IS APPLIED AT THE READ, in `gate_parse_file`, because the parser is a pure stdin
# filter and that caller is the one place a FILE is opened; `_gate_awk` lost its file operand with
# the redirection, since a parameter nothing reads is a parameter a later caller passes wrongly.
GATE_NUL="$T/full-pass-nul-result.txt"
LC_ALL=C sed -e 's/^RESULT: PASS$/RESULT: PA\x00SS/' "$GOOD" >"$GATE_NUL" 2>/dev/null ||
  cp "$GOOD" "$GATE_NUL"
# THE PREMISE IS MEASURED ON THE FILE — twice, because both halves matter: the fixture must NOT
# carry the literal certifying token, and it must really carry the byte. A refusal from a fixture
# that is merely malformed some other way would prove nothing.
if LC_ALL=C grep -q '^RESULT: PASS$' "$GATE_NUL" 2>/dev/null; then
  bad "t1/pm PREMISE: the fixture still carries a literal column-zero 'RESULT: PASS', so the case below proves nothing"
else
  ok "t1/pm PREMISE: the fixture carries NO literal column-zero 'RESULT: PASS' (MEASURED with grep on the FILE)"
fi
if LC_ALL=C tr -d '\000' <"$GATE_NUL" 2>/dev/null | LC_ALL=C cmp -s - "$GATE_NUL"; then
  bad "t1/pm PREMISE: the fixture holds NO NUL byte — this host's sed did not plant it, so the case below is not about the byte"
else
  ok "t1/pm PREMISE: the fixture really holds a NUL byte (MEASURED by deleting it and comparing)"
fi
refused "t1/pm: a NUL-bearing gate-of-record RESULT is REFUSED, not read as the PASS the capture manufactured" \
  "$GATE_NUL"
# CONTROL: the SAME summary without the byte still certifies, so the refusal is about the byte and
# not about the fixture being unparsable for some other reason.
if run 0 "t1/pm CONTROL: the same summary without the NUL still certifies (exit 0)" \
  2421 "$CERTIFIED" "$GOOD" --c-verdict "$C_PASS_FILE"; then
  case "$OUT" in
    *"PREMERGE: OK"*) ok "t1/pm CONTROL: and reports PREMERGE: OK, so the guard does not red on correct input" ;;
    *) bad "t1/pm CONTROL: the clean summary did not report PREMERGE: OK (got: $OUT)" ;;
  esac
fi
# STRUCTURAL: the read is routed, and the parser can no longer be handed a file at all.
if LC_ALL=C grep -q 'c_capture_map_nul "$1" | _gate_awk' "$ASSERT"; then
  ok "t1/pm/structural: the gate-summary FILE read goes through the mapping before awk sees it"
else
  bad "t1/pm/structural: the gate summary is still fed to awk raw, so a NUL in a field survives to a capture that removes it"
fi
if [ "$(LC_ALL=C grep -c "' <\"\\\$1\"" "$ASSERT" || true)" -eq 0 ]; then
  ok "t1/pm/structural: no awk program takes its input by redirection from a value any more"
else
  bad "t1/pm/structural: $(LC_ALL=C grep -c "' <\"\\\$1\"" "$ASSERT" || true) awk program(s) still read a value-named file directly"
fi
if LC_ALL=C grep -q 'awk -v WANT="\$1"' "$ASSERT"; then
  ok "t1/pm/structural: _gate_awk takes WANT as its ONLY argument — a file operand nothing reads is one a later caller passes wrongly"
else
  bad "t1/pm/structural: _gate_awk still declares a second parameter, so a caller can pass it a file that is never read"
fi

# --- 44p: THE STRUCTURAL READ-BOUNDARY GUARD (round 14, T1) -------------------
# THREE CONSECUTIVE ROUNDS HAVE FOUND "a boundary exists and one path bypasses it" — round 7's emit
# sites, round 13's record reads, and round 14's two remaining file reads — which is this
# repository's standing signal to mechanize rather than carve the same place a fourth time. Round
# 13's own structural asserts could NOT see either site: they check that the mapping appears exactly
# ONCE, which is a property of the boundary and not of its CALLERS.
#
# `scripts/tests/lib/read-boundary-scan.sh` is the caller-side guard, the mirror of section 44h's
# emit-boundary scanner. The POSITIVE CONTROL is the requirement, not the clean run — and here it is
# more than a formality: written without an assignment-prefix stripper the scanner reported CLEAN on
# the very defect it exists for, because every text call in these scripts is spelled
# `LC_ALL=C grep …` and the text before the command word therefore ends in `C`.
RBS="$SCRIPT_DIR/lib/read-boundary-scan.sh"
if [ ! -f "$RBS" ]; then
  # SEVEN, matching the seven assertions the else-branch emits, so the case floor is unaffected by
  # which branch runs.
  bad "read-guard: $RBS is missing — the structural guard did not run (1/7)"
  bad "read-guard: the same absence (2/7)"
  bad "read-guard: the same absence (3/7)"
  bad "read-guard: the same absence (4/7)"
  bad "read-guard: the same absence (5/7)"
  bad "read-guard: the same absence (6/7)"
  bad "read-guard: the same absence (7/7)"
else
  RBS_OUT="$(bash "$RBS" "$ASSERT" 2>&1)"; RBS_RC=$?
  if [ "$RBS_RC" -eq 0 ]; then
    ok "read-guard: the SHIPPED premerge-assert.sh is CLEAN — every read of file content is routed or declared"
  else
    bad "read-guard: the shipped premerge-assert.sh has a read-boundary BYPASS: $RBS_OUT"
  fi
  case "$RBS_OUT" in
    *"NOT COVERED"*) ok "read-guard: the scan DECLARES what it does not cover, on every run" ;;
    *) bad "read-guard: the scan did not declare its scope (got: $RBS_OUT)" ;;
  esac
  case "$RBS_OUT" in
    *"recogniser hit(s)"*) ok "read-guard: and it reports HOW MANY statements it examined — a count, not an adjective" ;;
    *) bad "read-guard: the scan did not report its subject count (got: $RBS_OUT)" ;;
  esac
  # THE POSITIVE CONTROL: the round-13 shape, an awk program fed the file by REDIRECTION, planted in
  # a THROWAWAY COPY (the artifact is substituted, never a settable seam — #3312's corollary).
  RBS_D="$T/rbs"; mkdir -p "$RBS_D"
  LC_ALL=C sed -e "/awk -v WANT=/s|.*|  awk -v WANT=\"\$1\" '{ print }' <\"\$PLANTED_RAW_READ\"|" \
    "$ASSERT" >"$RBS_D/premerge-assert.sh" 2>/dev/null || true
  if LC_ALL=C grep -q 'PLANTED_RAW_READ' "$RBS_D/premerge-assert.sh" 2>/dev/null; then
    ok "read-guard/control: the raw-read plant landed in the scratch copy (asserted, not assumed)"
  else
    bad "read-guard/control: the raw-read plant did NOT land, so the control below proves nothing"
  fi
  RBS_POUT="$(bash "$RBS" "$RBS_D/premerge-assert.sh" 2>&1)"; RBS_PRC=$?
  if [ "$RBS_PRC" -ne 0 ]; then
    ok "read-guard/control: the guard REDS on a planted raw read"
  else
    bash "$RBS" "$RBS_D/premerge-assert.sh" >/dev/null 2>&1
    bad "read-guard/control: the guard reported CLEAN on a planted raw read — it proves nothing (got: $RBS_POUT)"
  fi
  case "$RBS_POUT" in
    *"input REDIRECTION reads a file named by a value"*) ok "read-guard/control: and it NAMES the recogniser that fired, so the red is attributable" ;;
    *) bad "read-guard/control: the guard red without naming the redirection recogniser (got: $RBS_POUT)" ;;
  esac
  # AND A STALE ALLOWLIST ENTRY IS ITS OWN FAILURE. An entry that matches nothing excuses nothing —
  # and it is the signal that the read it described has CHANGED, which is the whole reason entries
  # are matched on source text rather than by line number.
  RBS_S="$T/rbs-stale"; mkdir -p "$RBS_S"
  LC_ALL=C sed -e '/awk -v WANT=/s|.*|  awk -v WANT="$1" -v UNUSED=1 '"'"'|' \
    "$ASSERT" >"$RBS_S/premerge-assert.sh" 2>/dev/null || true
  RBS_SOUT="$(bash "$RBS" "$RBS_S/premerge-assert.sh" 2>&1)"
  case "$RBS_SOUT" in
    *"STALE allowlist entry"*) ok "read-guard/stale: an allowlist entry whose source text has CHANGED is reported STALE by name, not silently kept" ;;
    *) bad "read-guard/stale: the guard did not report a stale allowlist entry when the declared read was reworded (got: $RBS_SOUT)" ;;
  esac
fi

# --- case floor (#3544) ------------------------------------------------------
# A span-replacing edit once silently deleted FOUR cases from a suite in this repo
# that then reported `failed: 0` at 102 instead of 105 — a green tally over a
# shrunken suite, which is #3751's own subject inside a test file. This suite grew
# a large section in one edit (Case 44), which is exactly the shape that loses
# cases quietly.
#
# THE FLOOR CARRIES A SMALL MARGIN, AND THAT IS DERIVED, NOT DEFENSIVE. Unlike
# test_review_stage.sh (bash + git only, no host-conditional assertion, so an
# EXACT floor is correct there), this suite's Case 41 bound cases branch on the
# HOST: whether `timeout`/`gtimeout` exist and accept `--kill-after` decides which
# assertions execute, so an exact floor would red on a legitimately-configured
# machine — the guard agents learn to waive. The margin is the size of that
# displacement and nothing more: the ONE host-gated block is the TERM-ignoring
# escalation case, which `skip -`s where no real `timeout`/`gtimeout` supporting
# `--kill-after` exists (it DELEGATES to a real runner rather than reimplementing
# the escalation it tests) and carries at most 6 assertions — measured by counting
# the `ok`/`bad` calls inside it, not guessed. Every other case runs on every host.
# 273 executed here minus that 6 is the floor. Adding cases never reds it (it is a
# lower bound); deleting a section does, which is the point. Move it consciously,
# in the same diff as the shrink it accounts for.
#
# ROUND 2 ADDED 5 HOST-INDEPENDENT ASSERTIONS (268 -> 273: one behavioural, that the AMBIGUOUS
# refusal does not ALSO print the never-opened diagnostic, plus the four S3 structural asserts),
# so the floor moves by the SAME 5 and the derived 6-assertion margin is PRESERVED. It is
# deliberately NOT raised to the exact 273: the margin accounts for a real host-gated block, and
# a floor that reds on a legitimately-configured machine is the guard agents learn to waive.
#
# ROUND 3 ADDED 34 MORE, ALSO ALL HOST-INDEPENDENT (273 -> 307): section 44f's 16 (the stage
# ARTIFACT bound to the certified sha — the stale-PASS case, the three unparsable record shapes,
# and the --force re-open remedy) and section 44g's 18 (the shared-table DIFFERENTIAL over the two
# `result:`/`REVIEW-STAGE:` readers, 2 per row plus the table floor and the fixture check). Both
# sections need only git and bash, the same as every other non-Case-41 case, so the floor moves by
# the SAME 34 and the derived 6-assertion margin for the ONE host-gated block (Case 41's
# TERM-ignoring escalation, which needs a real `timeout`/`gtimeout` supporting `--kill-after`) is
# PRESERVED UNCHANGED. It is still deliberately NOT the exact 307, for that reason.
#
# ROUND 5 (J1) ADDS 4, ALSO HOST-INDEPENDENT (307 -> 311): section 44f's stale-remedy case gains
# one assertion (a forced re-open publishes a NEW report GENERATION and PRINTS its path, so the
# re-audit is written where a reader looks — the old path is the one the resumed agent holds), and
# a fourth unparsable-record shape is added (an unreadable `report-nonce:`, which arrives
# here as a non-passing TOKEN because the seam between the two scripts is
# `review-stage.sh verdict`, plus its remedy assertion). Both need only git and bash, so the floor
# moves by the SAME 4 and the derived 6-assertion margin for the ONE host-gated block is PRESERVED
# UNCHANGED — still deliberately not the exact 311, for the reason recorded above.
#
# ROUND 5's SECOND ITEM (J3) ADDS 5 MORE, ALSO HOST-INDEPENDENT (311 -> 316): section 44b(iii)'s
# control-character census over the refusal block AND over the PASS success line, with the
# prose-preserved assertions either side of a planted control byte and the CONTROL that the
# neutralisation did not change the verdict. So the floor moves by the SAME 5 and the derived
# 6-assertion margin is PRESERVED UNCHANGED.
#
# ROUND 7 ADDS 47, ALL HOST-INDEPENDENT (310 -> 357): section 44b(iv)'s 4 (a control-bearing
# `--c-verdict` FILENAME reaches the SUCCESS block through `C_SOURCE` — the byte census plus the
# assertion a byte census alone would miss, that EXACTLY ONE column-zero `PREMERGE: OK ` line
# survives), section 44b(v)'s 37 (the mandatory fields' VALUES: the shipped emitter driven through
# ALL ELEVEN of its states with each captured line required to be ACCEPTED — the only way this
# validator does not red on correct input — plus six refusals each naming its field, and the
# `unknown`/`unresolved`/`0` acceptance cases), and section 44h's 6 (the structural emit-boundary
# guard and its positive control). Every one needs only bash, git and coreutils, the same as every
# other non-Case-41 case, so the floor moves by the SAME 47 and the derived 6-assertion margin for
# the ONE host-gated block (Case 41's TERM-ignoring escalation, which needs a real
# `timeout`/`gtimeout` supporting `--kill-after`) is PRESERVED UNCHANGED — still deliberately not
# the exact 363, for the reason recorded above.
#
# ROUND 9 (N2) ADDS 16, ALL HOST-INDEPENDENT (363 -> 379): section 44i's 16 — AUTO must rest on ONE
# observation of the stage record. The `head-sha` binding was validated on one read and
# `review-stage.sh verdict` then RE-READ the record to pick which report is current, so a
# replacement in between handed back a verdict from a different GENERATION (measured: the success
# line named `stage-head=<validated>` beside `report: …/c.decoygenerationB.md`, whose own head-sha
# was forty zeros). The interleaving is SIMULATED by one line injected into a scratch copy of the
# assert immediately before it invokes `verdict`: a decoy-generation SWAP, an UNLINK (which refuses
# either way, so what is asserted is that the CAUSE names the interleaving and not the downstream
# NOT-RUN), a per-case RESTORE so no case inherits a sibling's plant, a valid-BAIT assertion so a
# refusal cannot come from a broken decoy, a NO-OP-injection CONTROL that still certifies, and two
# STRUCTURAL pins (the re-observation sits between the verdict read and the token parse; the
# head-sha is parsed from the CAPTURED observation rather than a second read of the file). All need
# only bash, git and coreutils, so the floor moves by the SAME 16 and the derived 6-assertion
# margin for the ONE host-gated block is PRESERVED UNCHANGED — still deliberately not the exact
# 379, for the reason recorded above.
#
# ROUND 9's N3 ADDS 4 MORE, ALSO HOST-INDEPENDENT (379 -> 383): section 44h's COMPOUND-STATEMENT
# positive control, which reproduces the instance the guard MISSED — this script printed the
# caller-supplied `$delta_file` unrouted from a line beginning `[ -n "$delta_file" ] &&`, and the
# line-anchored scope reported the file CLEAN. The control strips the routing from that exact line,
# requires the guard to red AND to NAME the planted symbol (a bare red is not evidence: measured,
# the OLD scanner reds on this same plant for an UNRELATED reason and never names it), and asserts
# the planted statement does not begin its line. The fallback arm is ten bads to match. So the
# floor moves by the SAME 4 and the derived 6-assertion margin is PRESERVED UNCHANGED.
#
# ROUND 10 (P2) ADDS 28, ALL HOST-INDEPENDENT (383 -> 411): section 44j's 28 — an ABA replacement
# must not defeat round 9's byte comparison. A record swapped to a foreign generation for exactly
# the span in which `review-stage.sh verdict` reads it, and swapped BACK before the comparison,
# leaves two byte-identical observations while the ACCEPTED verdict came from the foreign one
# (measured: `C-VERDICT PASS … stage-head=<validated>` beside `report: …/c.decoygenerationB.md`,
# with NO record-changed refusal, because the bytes genuinely matched). The interleave is SIMULATED
# by TWO lines injected either side of the verdict invocation in a scratch copy of the assert
# (44i's single-injection builder cannot express it: the restore has to land after the callee ran):
# the A->B->A case with its four content assertions — including the DISCRIMINATION that the byte
# comparison must NOT have fired, without which this is 44i's case again — a valid-BAIT assertion,
# per-case RESTOREs, a two-point NO-OP CONTROL that still certifies, the LEGACY pre-nonce record
# (a genuine PASS from a bare `<kind>.md` report whose generation nothing names), three cases
# against a SUBSTITUTED callee (a control naming the validated generation, a well-formed path with
# a foreign nonce, and `report=unresolved` beside an accepting token — the last reachable ONLY this
# way, since the shipped emitter forces NOT-RUN whenever it cannot derive a path), and four
# STRUCTURAL pins (the binding sits between the verdict read and the closed-grammar acceptance;
# `report-nonce:` is parsed in exactly ONE place and published from the capture's parse; the callee
# is invoked with kind and issue ONLY, so H2's deleted `--report` channel is not rebuilt from the
# other end; and round 9's byte comparison is still called, as defence in depth rather than
# superseded). All need only bash, git and coreutils, so the floor moves by the SAME 28 and the
# derived 6-assertion margin for the ONE host-gated block is PRESERVED UNCHANGED.
#
# ROUND 11 (Q1) ADDS 10, ALL HOST-INDEPENDENT (411 -> 421): section 44k's 10 — the routing pathspec
# must be repository-ROOT-anchored. `git diff … -- openspec/changes/` interprets its pathspec
# relative to the CALLER'S CWD, so invoked from a subdirectory the diff came back EMPTY, a
# design-routed branch measured NOT-APPLICABLE and the merge PROCEEDED with no C verdict at all
# (measured: `PREMERGE: OK 95052a5e…` from `cqlite-core/src/storage`, exit 0 where the root
# invocation on the SAME repository, sha and argv refuses with `routing: REQUIRED`). Its own
# fixture, a ROOT-invocation control so the red is attributable to the cwd, two subdirectory depths
# (including inside `openspec/changes` itself, where a cwd-relative pathspec would look for
# `openspec/changes/openspec/changes/…`), the fail-OPEN direction (an oracle-routed branch must stay
# NOT-APPLICABLE, without which `:(top)` could have been widened to match everything), and three
# structural pins — the `:(top)` anchor is PRESENT, no unanchored `openspec/changes/` pathspec
# survives, and BOTH `diff.renames=false` and `diff.relative=false` are still pinned (they are
# different axes: `:(top)` anchors what is SELECTED, `diff.relative` only what is PRINTED). All need
# only bash, git and coreutils, so the floor moves by the SAME 10 and the derived 6-assertion margin
# for the ONE host-gated block is PRESERVED UNCHANGED.
#
# ROUND 11 (Q3) ADDS 20, ALL HOST-INDEPENDENT (421 -> 441): section 44l's 20 — a `report=` value
# containing a SPACE must arrive WHOLE. `report=` carries a PATH, `_c_verdict_awk` took it as one
# whitespace-delimited FIELD, so it truncated at the first space and round 10's nonce match then
# REFUSED an otherwise VALID verdict — a false refusal on correct input (measured on the SHIPPED
# artifacts in a checkout named `…/work tree`: `verdict reported: /tmp/…/work` beside a
# `validated generation:` that was exactly the one the verdict named). A fixture whose DIRECTORY
# NAME contains a space, driven end to end through the real producer (whose own emitted line is
# asserted to carry the space, or the section would prove nothing about the reader); the
# acceptance case plus a whole-path assertion on the SUCCESS line; a substituted-callee CONTROL
# (a space-bearing path naming the VALIDATED generation certifies) and its counterpart (a
# space-bearing path with a FOREIGN nonce still REFUSES, asserted on the `verdict reported:`
# DETAIL line, because the block also echoes the RAW verdict line and an `$OUT`-wide match passed
# in the pre-fix state); the `--c-verdict <path>` branch, which reads the same line under no nonce
# check, so round 7's L3 value validation is isolated; a BARE `report=` still refused as EMPTY, so
# taking the remainder did not turn "empty" into "the rest of the line"; and the assumption the
# remainder rule RESTS on made ENFORCED rather than assumed — the 11 emitter states DERIVED by
# running the shipped `review-stage.sh` (`c_capture_state`, section 44b(v)'s helper) with a
# committed count floor, no mandatory key permitted to follow `report=` on any line it produces,
# the single emit site pinned structurally, and the reader pinned to the remainder form with the
# field-truncating form required ABSENT. All need only bash, git and coreutils, so the floor moves
# by the SAME 20 and the derived 6-assertion margin for the ONE host-gated block is PRESERVED
# UNCHANGED — still deliberately not the exact 441, for the reason recorded above.
#
# ROUND 13 (S2) ADDS 8, ALL HOST-INDEPENDENT (441 -> 449): section 44m's 8 — a capture must not
# MANUFACTURE the token it validates. Bash SILENTLY DISCARDS NUL bytes in a command substitution,
# and gawk passes a NUL through a field, so a `--c-verdict` file whose token is `PA\0SS` — a token
# that is NOT `PASS` and which the closed-set match must refuse — reached the merge point as `PASS`
# and this script reported `PREMERGE: OK` at exit 0. Measured on the shipped artifacts. The case
# asserts the premise ON THE FILE (grep finds no literal `RESULT: PASS`), the refusal, a CONTROL
# that the same line without the NUL still certifies (a guard that reds on correct input is the
# guard agents learn to waive), and five STRUCTURAL pins: ONE mapping implementation
# (`c_capture_map_nul`), the NUL translation appearing EXACTLY ONCE, the marker byte DERIVED from
# the `tr` spelling rather than written twice, no raw file capture left, and the c-verdict FILE read
# routed through the mapping before awk sees it. All need only bash, git and coreutils, so the floor
# moves by the SAME 8 and the derived 6-assertion margin for the ONE host-gated block is PRESERVED
# UNCHANGED.
#
# ROUND 14's T2 ADDS 10, ALL HOST-INDEPENDENT (443 -> 453): section 44n's 10 — the output primitive
# must be a LITERAL PRINTER. Section 44h asserts every VALUE on an emitted line is routed; a routed
# value is no protection if the printing COMMAND re-interprets what the boundary neutralised, and
# under the bash option `xpg_echo` (set by an INHERITED environment, never by this script) `echo`
# processes BACKSLASH ESCAPES in its argument — so a `\n` splits a line, a `\033` injects terminal
# control and octal `\075` manufactures a REAL `=`. Measured on `review-stage.sh`, whose `emit` had
# exactly this shape. This script uses no `echo` today, so the value is the POSITIVE CONTROL: an
# `echo` plant (deliberately COMPOUND, so the control also tests positional recognition) and a
# data-derived-`printf`-FORMAT plant, each required to red AND to name what failed. The fallback arm
# is ten bads to match. All need only bash and coreutils, so the floor moves by the SAME 10 and the
# derived 6-assertion margin for the ONE host-gated block is PRESERVED UNCHANGED.
#
# ROUND 14's T1 ADDS 14 MORE, ALL HOST-INDEPENDENT (453 -> 467): section 44o's 7 — the GATE-OF-RECORD
# read must go through the capture boundary. Section 44m routed the c-verdict read and the stage
# record and left `_gate_awk` reading its summary RAW, so gawk passed a NUL through `v_result` and
# the capture in `gate_parse_file` removed it: `RESULT: PA<NUL>SS` read as `PASS` at the merge gate.
# Two PREMISE assertions measure the fixture ON THE FILE (no literal `RESULT: PASS`, and the byte
# really planted), the refusal, a CONTROL that the same summary without the byte still certifies, and
# three structural pins (the read is piped through the mapping; no awk program takes a value-named
# file by redirection; `_gate_awk` has no file parameter left). Plus section 44p's 7 — the STRUCTURAL
# read-boundary guard `scripts/tests/lib/read-boundary-scan.sh`, the caller-side mirror of 44h:
# round 13's asserts check the mapping appears exactly ONCE, which is a property of the boundary and
# not of its callers, so neither round-14 site was visible to them. Its positive controls plant a
# raw redirection read and a REWORDED declared read, requiring the guard to red AND to name the
# recogniser / the STALE allowlist entry — and that control is not a formality: written without an
# assignment-prefix stripper the scanner reported CLEAN on the very defect it exists for, because
# every text call in these scripts is spelled `LC_ALL=C grep …`. The fallback arm is seven bads to
# match. All need only bash and coreutils, so the floor moves by the SAME 14 and the derived
# 6-assertion margin for the ONE host-gated block is PRESERVED UNCHANGED.
#
# ROUND 15 (U2) ADDS 23, ALL HOST-INDEPENDENT (473 -> 496; floor 467 -> 490, the documented
# 6-assertion host-gated margin PRESERVED UNCHANGED — every added case needs only bash, git,
# coreutils and awk): section 44q's 22 (the ANSI strip may LOCATE a line and may not SUPPLY a
# value — the reported c-verdict site plus the two siblings the class sweep found, the gate
# summary's `RESULT:` and the stage record's `head-sha:`; each with its premise MEASURED on the
# file, a control that the intact artifact still certifies, and the control that matters most,
# a #3400-shaped capture whose colour BRACKETS the key and the value and which must still reach
# `PREMERGE: OK`; plus eight structural pins over the split readings, the decision order and the
# affirmative permissive arms), and section 44g's 1 (a new `ansi` row: the two readers of one
# shape ACTUALLY disagreed there — `classify_report` reported `unrecognised result token
# 'PA?[31mSS'` while `_c_verdict_awk` published `PASS` — so reaching TOKEN-REJECTED at both is
# a consolidation, and the table's floor moves 8 -> 9 with it).
#
# ROUND 16 (V1) ADDS 23, ALL HOST-INDEPENDENT (496 -> 519; floor 490 -> 513, the documented
# 6-assertion host-gated margin PRESERVED UNCHANGED — every added case needs only bash, git,
# coreutils and awk): section 44r's 23 — the C verdict must be re-validated INSIDE the window it
# certifies. `c_evaluate` ran ONCE and was then followed by the base-staleness advisory (bounded at
# 65s) and the `gh pr view` round trip with NOTHING re-checking C before `PREMERGE: OK`, so a
# concurrent `review-stage.sh open --force` superseded the validated PASS and the script still
# certified (measured on the shipped artifact with the supersede planted immediately after the
# single evaluation: `PREMERGE: OK b5f49d60aae4…` at exit 0). The interleave is SIMULATED by ONE
# line injected after the first evaluation in a scratch copy of the assert. An UNDISTURBED CONTROL
# first (a guard that reds on correct input is the guard agents learn to waive), then the
# supersede-to-NOT-RUN case, then the case that makes this a COMPARISON rather than a repeat — a
# supersede to a DIFFERENT generation that itself PASSES at the same head, which a second
# evaluation alone would certify — each with per-case RESTOREs, plant-landed assertions, and the
# requirement that NO `PREMERGE: OK` line is emitted at all on a refusing run (which caught this
# fix's own first draft: the refusal PROSE carried the literal success marker, so a reader grepping
# for it saw certification in a refusal — #3312's rule that a diagnostic may not print the token it
# describes, one directory over). Plus seven STRUCTURAL pins: the re-validation call EXISTS, the
# EARLY call is RETAINED exactly once (job 290's remedy keeps it), the four-anchor ORDER is derived
# by line number (early < gh < re-validation < the success emit) with its own not-vacuous
# assertion, the captured observation is RESET inside the FUNCTION BODY — extracted, not
# file-wide, because the globals are initialised to "" at the top and a whole-file grep PASSES on
# the pre-fix artifact — and the second evaluation calls the SHIPPED `c_evaluate` rather than
# re-implementing the binding.
#
# ROUND 16 (V2) ADDS 1 MORE, HOST-INDEPENDENT (519 -> 520; floor 513 -> 514, the documented
# 6-assertion host-gated margin PRESERVED UNCHANGED): section 44l(d)'s existing emitter-literal pin
# is retargeted at `remainder_value` — `report=` is the ONE field EXEMPT from the `=`->`~` map,
# because a repository root may legally contain `=` and mapping it published a path that DOES NOT
# EXIST — and pinned TOGETHER with the position, since the exemption is sound only while the field
# is last and read as the remainder, so either change alone makes the other wrong. The ADDED
# assertion is the other half of that coupling: `deadline=`/`agent=` must STILL route through the
# `=`-mapping boundary, or a hand-edited record could forge a `report=` pair AHEAD of the measured
# one and the remainder rule would read the FORGED value (the reader takes the FIRST occurrence).
# Its behavioural counterpart is section 29(b) of test_review_stage.sh.
#
# ROUND 18's X1 MOVES IT TO 531 (+17, of which 537 execute — the documented 6-assertion host-gated
# margin is preserved). The new section 44s: a CAPTURED PATH IS NOT THE PATH. `c_stage_root`
# captured `git rev-parse --show-toplevel`, and each of its four callers captured `c_stage_root`
# again, so a trailing newline was stripped TWICE and a checkout whose DIRECTORY NAME ends in an
# LF resolved to an EXISTING SIBLING — whose stage records the AUTO path then located, bound and
# read. `c_assert_head_binds_certified` cannot see it: HEAD is read in the CWD (the real lane, so
# it binds) while the ARTIFACT comes from the sibling. The channel is REMOVED rather than made
# faithful — the resolver assigns a global and prints nothing — so a fifth call site cannot
# reintroduce it. The root is made OBSERVABLE by giving the sibling TWO stage records, because
# AMBIGUOUS is the one refusal that PRINTS the directory it enumerated: the RED CONTROL plants the
# pre-fix lossy resolver into a scratch copy of the assert (the ARTIFACT substituted, never a
# settable seam) and must NAME the sibling, while the shipped one must name THIS lane's own
# absence and never the sibling, emit no merge-proceeding verdict, and leave the peer's records
# intact. EIGHT structural pins: no capture of the resolver anywhere (needle SPLIT so the guard
# cannot match its own line), assignment to the shared global, 4 calls against 4 global reads, the
# two-signal completeness form, exactly-one-trailing-newline removal, and — the SECOND instance of
# the class, found by sweeping rather than by fixing the site the finding named — the script's own
# directory no longer resolved through `$(dirname "${BASH_SOURCE[0]}")`, which is how
# `review-stage.sh`, the ENFORCER of the verdict this script refuses to merge without, is located.
# Two of these pins initially FLAKED: `printf … | grep -q` short-circuits on a match, printf takes
# SIGPIPE, and under `pipefail` the pipeline status is 141 — so a pin over a >64KiB body failed
# exactly when its needle was PRESENT. They read a FILE now (#3400's "by redirection, never a
# pipe", one shell feature over). Every one needs only bash, git and coreutils.
#
# ROUND 20's Z1 (HALF A) ADDS 8, ALL HOST-INDEPENDENT (537 -> 545; floor 531 -> 539, the documented
# 6-assertion host-gated margin PRESERVED UNCHANGED): section 44t — a FOREIGN verdict reached
# through a symlinked PATH COMPONENT. `--c-verdict AUTO` globs `<root>/.review-stage/issue-*/c.stage`
# and reads the verdict with `review-stage.sh verdict`; round 19 gave that reader a leaf `[ -L ]`
# test and nothing above it, so a link at `.review-stage/` supplied this lane with a PEER TREE's
# stage — record, nonce and clean report together. Measured on the pre-fix reader with everything
# else intact: `PREMERGE: OK` plus `PREMERGE: C-VERDICT PASS … source: AUTO issue=3751`, i.e. another
# lane's audit clearing this merge. The peer record is RE-STAMPED to the lane's certified sha on
# purpose, so the head-sha binding (44f) cannot be what refuses and the refusal is attributable to
# the read path; the POSITIVE CONTROL reads the same stage in the peer's OWN tree and still
# certifies. The case exists because the fix lives in `review-stage.sh` and the inheritance was a
# PREDICTION — this issue is about the difference between a prediction and a measurement. Every
# added assertion needs only bash, git, coreutils and `ln -s`, and a fixture that cannot be built
# calls `bad`, so a displaced count can never be a silent green.
ASSERT_FLOOR=539
EXECUTED=$((PASS + FAIL))
if [ "$EXECUTED" -lt "$ASSERT_FLOOR" ]; then
  bad "CASE FLOOR: only $EXECUTED assertions executed, below the committed floor of $ASSERT_FLOOR — a section died silently, and 'failed: 0' over a shrunken suite is not a pass"
fi

# --- summary -----------------------------------------------------------------
printf '\n=== premerge-assert: %d passed, %d failed (executed %d, floor %d) ===\n' \
  "$PASS" "$FAIL" "$((PASS + FAIL))" "$ASSERT_FLOOR"
[ "$FAIL" -eq 0 ]
