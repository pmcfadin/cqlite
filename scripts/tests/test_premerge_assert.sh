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
    *)                                      printf 'UNCLASSIFIED\n' ;;
  esac
}

# THE TABLE. One row per adversarial shape, each spelled for BOTH readers by
# `diff_row_body_a` / `diff_row_line_b` — the SAME shape, in each reader's own grammar. Written
# with printf so a row can plant something neither producer would ever emit, which is the point.
DIFF_ROWS="plain indented several zero crlf token-junk fenced globish"
DIFF_EXPECT_plain=READ
DIFF_EXPECT_indented=NO-RECORD
DIFF_EXPECT_several=NO-RECORD
DIFF_EXPECT_zero=NO-RECORD
DIFF_EXPECT_crlf=READ
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
    token-junk) printf '%s PASSNOW %s\n' "$pfx" "$sfx" ;;
    fenced)     printf '```\n%s PASS %s\n```\n%s PASS %s\n' "$pfx" "$sfx" "$pfx" "$sfx" ;;
    globish)    printf '%s * %s\n' "$pfx" "$sfx" ;;
  esac
}

# A CASE FLOOR ON THE TABLE ITSELF (#3544's lesson): an emptied or shrunken table yields a loop
# that runs fewer times and reports `failed: 0`, which is a green tally over a shrunken suite.
DIFF_ROW_COUNT=$(printf '%s\n' $DIFF_ROWS | grep -c .)
if [ "$DIFF_ROW_COUNT" -ge 8 ]; then
  ok "differential: the shared table holds $DIFF_ROW_COUNT adversarial shapes (floor 8)"
else
  bad "differential: the shared table holds only $DIFF_ROW_COUNT rows, below the floor of 8 — a row was lost and agreement over an empty table is not agreement"
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

# --- 44h: THE STRUCTURAL EMIT-BOUNDARY GUARD (round 7, L1b) -------------------
# The mirror of test_review_stage.sh section 18, for this script. See
# scripts/tests/lib/emit-boundary-scan.sh for why the guard exists (the boundary was bypassed at a
# NEW site in three consecutive rounds, so the answer is a mechanism rather than a fourth patch) and
# for the scope it DECLARES on every run. The positive control is the requirement, not the clean
# run: a scanner that flagged nothing would exit 0 exactly as this one does.
EBS="$SCRIPT_DIR/lib/emit-boundary-scan.sh"
if [ ! -f "$EBS" ]; then
  bad "emit-guard: $EBS is missing — the structural guard did not run (1/6)"
  bad "emit-guard: the same absence (2/6)"
  bad "emit-guard: the same absence (3/6)"
  bad "emit-guard: the same absence (4/6)"
  bad "emit-guard: the same absence (5/6)"
  bad "emit-guard: the same absence (6/6)"
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
ASSERT_FLOOR=310
EXECUTED=$((PASS + FAIL))
if [ "$EXECUTED" -lt "$ASSERT_FLOOR" ]; then
  bad "CASE FLOOR: only $EXECUTED assertions executed, below the committed floor of $ASSERT_FLOOR — a section died silently, and 'failed: 0' over a shrunken suite is not a pass"
fi

# --- summary -----------------------------------------------------------------
printf '\n=== premerge-assert: %d passed, %d failed (executed %d, floor %d) ===\n' \
  "$PASS" "$FAIL" "$((PASS + FAIL))" "$ASSERT_FLOOR"
[ "$FAIL" -eq 0 ]
