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
NEUTRAL_ASSERT="$NEUTRAL_DIR/premerge-assert.sh"

# run <expected-exit> <description> <args...> — invokes the NEUTRAL COPY of the
# assert with the gh mock on PATH, captures combined output + exit code. Sets
# $OUT and $RC. The shipped artifact is exercised by the wiring case alone.
run() {
  local want="$1" desc="$2"
  shift 2
  OUT=$(PATH="$BIN:$PATH" bash "$NEUTRAL_ASSERT" "$@" 2>&1)
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
        dirty="${8-no}"
  printf '%s\n' "$start"
  printf 'run-id: /tmp/agent-gate.9cIQgX\n'
  [ "$mode" = "-" ] || printf '%s\n' "$mode"
  [ "$commit" = "-" ] || printf 'commit: %s branch: issue-3465-require-gate-of-record%s\n' \
    "$commit" "$(dirty_field "$dirty")"
  printf 'datasets: 144 Data.db files under /data/datasets\n'
  printf 'accelerators: sccache=on nextest=on lanes=on mold=absent perf=paranoid-4\n'
  [ "$tstart" = "-" ] || printf 'tree-start: %s dirty: %s digest: 671a6275687c\n' \
    "$tstart" "$(dirty_tree_start "$dirty")"
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
    "${1:-$C7}" "${2:-$C12}" "${3:-PASS}" "${4:-PASS}" "${5-no}"
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
# A gate that ran with `dirty: yes` certified sha PLUS uncommitted TRACKED edits
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
# was merely reported; `unverified` is what the gate itself writes when its tree
# capture failed. An unestablished state is not a clean one.
for _d in maybe unknown unverified YES No; do
  full_summary "$T/dirty-$_d.txt" "$C7" "$C12" PASS PASS "$_d"
  refused "dirty: $_d (unrecognised) -> refuse, never read as clean" \
    "$T/dirty-$_d.txt" "records 'dirty: $_d'"
done
unset _d

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
# The 9th parameter is the delta run's OWN `dirty:` value (#3648), rendered by the
# same helpers as the full block's.
# Line SHAPES are copied from scripts/agent-gate.sh's delta emit site
# (anchor_meta + SUMMARY_MODE_LINE), trailing fields included.
delta_block() {
  local anchor="${1:-$ANCHOR}" commit="${2:-$C7}" tstart="${3:-$C12}" \
        ti="${4:-PASS}" result="${5:-PASS}" mode="${6:-$DELTA_MODE}" \
        paren="${7:-(full-gate PASS commit)}" dirty="${8-no}"
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
    "$tstart" "$(dirty_tree_start "$dirty")"
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

# 33(x)(c) the ANCHOR (full) block dirty -> refuse, naming the full-gate block.
# The delta here is the GOOD one, so the refusal cannot be the delta's.
full_summary "$T/anchor-dirty-yes.txt" "$A7" "$A12" PASS PASS yes
refused_pair "anchor block dirty: yes -> refuse even with a clean delta re-cert" \
  "$T/anchor-dirty-yes.txt" "$GOODDELTA" "The full-gate block records 'dirty: yes'"

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
OUT=$(PATH="$BADBIN:$BIN:$PATH" bash "$NEUTRAL_ASSERT" 2421 "$CERTIFIED" "$GOOD" 2>&1)
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
  OUT=$(PATH="$BIN:$PATH" bash "$script" "$@" 2>&1)
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
  OUT=$(PATH="$SHIMD:$BIN:$PATH" bash "$COPY" 2421 "$CERTIFIED" "$GOOD" 2>&1)
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
  OUT=$(PATH="$NOBIN" bash "$COPY" 2421 "$CERTIFIED" "$GOOD" 2>&1)
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
  OUT=$(PATH="$GTD" bash "$COPY" 2421 "$CERTIFIED" "$GOOD" 2>&1)
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
    OUT=$(PATH="$KILLD:$BIN:$PATH" bash "$COPY" 2421 "$CERTIFIED" "$GOOD" 2>&1)
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
    bash "$ASSERT" 2421 "$WIRE_SHA" "$WIREGOOD" 2>&1)
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
    bash "$ORDFLOW/premerge-assert.sh" 2421 "$CERTIFIED" "$GOOD" 2>&1)
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

# --- summary -----------------------------------------------------------------
printf '\n=== premerge-assert: %d passed, %d failed ===\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
