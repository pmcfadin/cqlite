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
# NOTHING IN THIS SUITE KILLS ANY PROCESS (roborev job 388). The box-wide sweep
# that used to live here is DELETED, and for the same reason as every other
# removal in this apparatus: it killed from a STALE `ps` snapshot, so a pid that
# had exited and been RECYCLED between the snapshot and the signal got SIGKILLed —
# on a shared box, most likely a peer lane's process. It was a hazard introduced
# while removing a different one.
#
# NARROWING IT WAS NOT AVAILABLE. Holding an ownership handle is impossible here:
# the shim is spawned by the bounded runner through `timeout`, so it is NOT this
# suite's direct child and there is no pid this suite legitimately owns.
#
# SO THE RESOURCE WAS MADE SELF-LIMITING AND THE CODE WENT AWAY WITH THE HAZARD.
# Every process the arms can leak now terminates ON ITS OWN within ~120s — the
# sentinel is a bounded fractional sleep, and the TERM-ignoring shim counts
# bounded 1s sleeps. Nothing needs hunting, nothing is signalled, and the whole
# recycled-pid class is GONE rather than reduced. 120s is comfortably longer than
# the arms' 2s+1s bound, so it cannot mask a bound that failed to fire, and short
# enough in absolute terms that a leak is self-clearing.
#
# The run-unique sentinel argv is KEPT — for diagnostics and for the structural
# pin at 44(m) — but NOTHING MAY KILL BASED ON IT.
TOSENTINEL=""

# THE SUITE'S OWN `rm -rf "$T"` HAS THE SAME RACE, AND IS DELIBERATELY KEPT
# (roborev job 390, finding 2 — DECLARED AND ACCEPTED, not removed). A same-UID
# peer can swap `$T` between the mktemp and this delete, exactly as it could for
# the shipped scratch. Four reasons this is not the same decision:
#   1. cleaning one's own `mktemp` root is universal test practice;
#   2. the same-UID fleet means NO boundary exists for a test suite either, so
#      removal would not buy isolation — it would only move the garbage;
#   3. the consequence is bounded: a test scratch, not a merge verdict. The
#      shipped delete could damage a concurrent lane while certifying a merge;
#   4. never cleaning ACCUMULATES every run, which R68 measured as a real cost
#      (240 stray directories) — and this delete is what contains those, since
#      TMPDIR points here.
# Consistency with the production path would be cargo-culting that rule past the
# reason for it.
suite_cleanup() {
  rm -rf "$T"
}

# EVERY CHILD GETS $T AS ITS TMPDIR (found while measuring job 388). The shipped
# assert creates its scratch under TMPDIR and, since the delete was removed, LEAVES
# IT THERE. This suite exercises Case B dozens of times per run and runs in the
# gate's tooling-tests, so with the ambient /tmp it accumulated scratch dirs
# without bound — 240 measured on this box after a round of development, not the
# "one per Case B merge" the removal's trade was stated against.
#
# Pointing TMPDIR at $T contains them WITHOUT reinstating any per-path delete in
# the shipped script: the suite already owns $T and removes it WHOLESALE, which is
# safe precisely because it is this run's own mktemp directory rather than a
# peer-mutable path. Exported after $T is validated, so the suite's own mktemp
# above still used the ambient TMPDIR.
export TMPDIR="$T"

trap 'suite_cleanup' EXIT
trap 'suite_cleanup; trap - INT;  kill -INT  $$' INT
trap 'suite_cleanup; trap - TERM; kill -TERM $$' TERM
trap 'suite_cleanup; trap - HUP;  kill -HUP  $$' HUP

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
# The #3752 legs ask gh for STRUCTURED payloads, not the two-token head/state
# line, so the mock answers by CALL SHAPE. Only the shipped-wiring case reaches
# these branches (every other case runs against the neutral binding stub), and
# each is driven by its own env var so a case can substitute a payload.
case "$*" in
  *closingIssuesReferences*)
    printf '%s\n' "${MOCK_GH_HOLD_JSON:-{\"body\":\"\",\"comments\":[],\"closingIssuesReferences\":[]\}}"
    exit 0 ;;
  *baseRefName*)
    printf '%s\n' "${MOCK_GH_PR_JSON:-{\"baseRefName\":\"main\",\"body\":\"\",\"comments\":[]\}}"
    exit 0 ;;
esac
case "$1" in
  api) printf '%s\n' "${MOCK_GH_TIMELINE_JSON:-[]}"; exit 0 ;;
esac
printf '%s\n' "${MOCK_GH_OUT:-}"
exit 0
MOCK
chmod +x "$BIN/gh"

# A `roborev` mock: the #3752 review-binding leg derives the reviewed head from
# the JOB RECORD, never from stdout prose, so this emits the real payload shape
# (the job row NESTED under a "job" key, measured in issue #2964).
cat >"$BIN/roborev" <<'RBMOCK'
#!/usr/bin/env bash
[ -n "${MOCK_ROBOREV_JSON:-}" ] || exit 1
case "$1" in
  show | list) printf '%s\n' "$MOCK_ROBOREV_JSON" ;;
  *) exit 1 ;;
esac
RBMOCK
chmod +x "$BIN/roborev"

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

# THE #3752 REVIEW-BINDING / HOLD LEGS ALSO NEED A NEUTRAL STUB. They are
# resolved from the assert's OWN directory with no override (#3312's enforcer
# rule), so substituting the ARTIFACT is the only way to keep these cases about
# what they are about. An ABSENT helper is a TOOL-FAILURE by design — which is
# exactly what every success-path case here would become without this stub.
# Their own subject is owned by scripts/tests/test_premerge_review_binding.sh.
NEUTRAL_BINDING='#!/usr/bin/env bash
case "$1" in
  review-binding) printf "PREMERGE: REVIEW-BINDING neutral immediate stub\n"
                  printf "PREMERGE: REVIEW-BINDING verdict NOT-APPLICABLE\n" ;;
  hold-check)     printf "PREMERGE: HOLD-CHECK neutral immediate stub\n"
                  printf "PREMERGE: HOLD-CHECK verdict NO-HOLD-RECOGNISED\n" ;;
  *) exit 3 ;;
esac
exit 0'
printf '%s\n' "$NEUTRAL_BINDING" >"$NEUTRAL_DIR/premerge-review-binding.sh"
chmod +x "$NEUTRAL_DIR/premerge-review-binding.sh"
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

# =============================================================================
# THE ANCESTRY FIXTURE (#3653) — A REAL SYNTHETIC REPOSITORY
# =============================================================================
# Case B's #3653 binding asks a REPOSITORY a question (`git merge-base
# --is-ancestor <anchor> <certified>`), and it asks it of the CURRENT WORKING
# DIRECTORY's repository with no env override (#3312's enforcer rule). So every
# Case B case that reaches that check needs REAL shas in a REAL repository —
# fabricated 40-hex constants are absent objects and land, correctly, on
# UNVERIFIABLE. One fixture serves BOTH arms:
#
#     c0 ─── c1 (ANCHOR) ─── c2 (CERTIFIED head)        [branch feature]
#      └──── cF (FOREIGN anchor: NOT an ancestor of c2)  [branch other]
#
# Built with `git init` in the scratch dir, so it is hermetic, bounded by
# construction and identical on every host — the same discipline (and the same
# fixture-self-consistency assertions) as the wiring case at the end of this file.
ANC_REPO="$T/ancestry-repo"
anc_shape=0
R_ANCHOR=""; R_CERT=""; R_FOREIGN=""
mkdir -p "$ANC_REPO"
if git init -q -b mainline "$ANC_REPO" >/dev/null 2>&1; then
  git -C "$ANC_REPO" config user.email t@t
  git -C "$ANC_REPO" config user.name t
  anc_commit() {
    printf 'content %s\n' "$1" >>"$ANC_REPO/$1.txt"
    git -C "$ANC_REPO" add -- "$1.txt" >/dev/null &&
      git -C "$ANC_REPO" commit -q -m "$1" >/dev/null
  }
  anc_commit c0 &&
    git -C "$ANC_REPO" checkout -q -b other &&
    anc_commit cF &&
    R_FOREIGN=$(git -C "$ANC_REPO" rev-parse HEAD) &&
    git -C "$ANC_REPO" checkout -q -b feature mainline &&
    anc_commit c1 &&
    R_ANCHOR=$(git -C "$ANC_REPO" rev-parse HEAD) &&
    anc_commit c2 &&
    R_CERT=$(git -C "$ANC_REPO" rev-parse HEAD) &&
    anc_shape=1
fi
# FIXTURE SELF-CONSISTENCY. Every property the cases below rely on is asserted
# here, because a fixture that is not the shape the case claims makes the case
# vacuous — green while proving nothing.
anc_is_hex40() { case "$1" in *[!0-9a-f]*|'') return 1 ;; esac; [ "${#1}" -eq 40 ]; }
if [ "$anc_shape" -eq 1 ]; then
  anc_is_hex40 "$R_ANCHOR" && anc_is_hex40 "$R_CERT" && anc_is_hex40 "$R_FOREIGN" || anc_shape=0
fi
if [ "$anc_shape" -eq 1 ]; then
  git -C "$ANC_REPO" merge-base --is-ancestor "$R_ANCHOR" "$R_CERT" || anc_shape=0
  git -C "$ANC_REPO" merge-base --is-ancestor "$R_FOREIGN" "$R_CERT" && anc_shape=0
  [ "$(git -C "$ANC_REPO" rev-parse --is-shallow-repository 2>/dev/null)" = false ] || anc_shape=0
fi
if [ "$anc_shape" -eq 1 ]; then
  ok "ancestry fixture: c1 IS an ancestor of c2, cF is NOT, and the repo is proven complete"
else
  bad "ancestry fixture: the synthetic repo is not the shape the #3653 cases claim (anchor '$R_ANCHOR', certified '$R_CERT', foreign '$R_FOREIGN') — those cases would be vacuous"
fi

# The two abbreviations the gate writes for each fixture sha (`%.7s` / `%.12s`).
RA7=$(printf '%.7s' "$R_ANCHOR");  RA12=$(printf '%.12s' "$R_ANCHOR")
RC7=$(printf '%.7s' "$R_CERT");    RC12=$(printf '%.12s' "$R_CERT")

# run_anc <expected-exit> <desc> <args...> — run() with the CURRENT DIRECTORY
# inside the ancestry fixture, which is the repository the #3653 binding reads.
# Still the NEUTRAL scratch copy of the assert (so no case scans the ambient
# checkout) and still the gh mock, answering with the fixture's own head.
run_anc() {
  local want="$1" desc="$2"
  shift 2
  OUT=$(cd "$ANC_REPO" && PATH="$BIN:$PATH" MOCK_GH_FAIL=0 MOCK_GH_OUT="$R_CERT OPEN" \
    bash "$NEUTRAL_ASSERT" "$@" 2>&1)
  RC=$?
  if [ "$RC" -ne "$want" ]; then
    bad "$desc (exit $RC, wanted $want)"
    printf '     output: %s\n' "$OUT"
    return 1
  fi
  return 0
}

# anc_pair <tag> <anchor-sha> — build the whole Case B PAIR for one anchor: the
# ANCHOR's full-gate block (whose own commit:/tree-start: ARE that anchor, which
# is what the earlier checks require) plus a delta block naming it and covering
# the fixture's certified head. Files land at $T/anc-full-<tag>.txt and
# $T/anc-delta-<tag>.txt.
#
# ONE builder, ONE varying input — the anchor sha. That is what lets the accept
# arm and the RED arm below differ in EXACTLY ONE PROPERTY, verifiably: mapping
# the foreign sha (and its two abbreviations) onto the anchor's must reproduce
# the accepted pair byte for byte.
anc_pair() {
  local tag="$1" a="$2"
  full_summary "$T/anc-full-$tag.txt" "$(printf '%.7s' "$a")" "$(printf '%.12s' "$a")" PASS PASS
  delta_summary "$T/anc-delta-$tag.txt" "$a" "$RC7" "$RC12" PASS PASS \
    "MODE: delta (TEST/DOCS-ONLY RE-CERTIFICATION — NOT the gate of record; gate of record = the full agent-gate.sh PASS at anchor $a)"
}
# anc_same_but_anchor <foreign-sha> <foreign-file> <anchor-file> — TRUE when the
# only difference between the two files is the anchor sha, in every width the
# gate writes it (40, 12, 7). Widest first, so a narrower substitution cannot
# eat part of an already-rewritten value.
anc_same_but_anchor() {
  sed -e "s/$1/$R_ANCHOR/g" \
      -e "s/$(printf '%.12s' "$1")/$RA12/g" \
      -e "s/$(printf '%.7s' "$1")/$RA7/g" "$2" | cmp -s - "$3"
}
anc_pair good "$R_ANCHOR"
anc_pair foreign "$R_FOREIGN"
RANCFULL="$T/anc-full-good.txt"
RGOODDELTA="$T/anc-delta-good.txt"
RFORFULL="$T/anc-full-foreign.txt"
RFORDELTA="$T/anc-delta-foreign.txt"

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
# MIGRATED to the REAL ancestry fixture (#3653): the fabricated ANCHOR/CERTIFIED
# constants are absent objects, so this case now lands on ANCHOR-UNVERIFIABLE
# unless it runs in a repository that HOLDS the two shas. Everything it asserted
# is preserved, at the fixture's own shas, plus the new affirmative token.
if run_anc 0 "anchored delta pair (full PASS at X + delta at Y) -> exit 0" \
  2421 "$R_CERT" "$RANCFULL" "$RGOODDELTA"; then
  case "$OUT" in
    *"PREMERGE: OK $R_CERT"*) ok "delta pair: prints PREMERGE: OK <sha>" ;;
    *) bad "delta pair: missing PREMERGE: OK (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: GATE-OF-RECORD commit: $RA7 tree-start: $RA12"*)
      ok "delta pair: the GATE-OF-RECORD line names the ANCHOR's provenance" ;;
    *) bad "delta pair: GATE-OF-RECORD line must name the anchor (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: DELTA-RECERT anchor: $R_ANCHOR anchor-ancestry: BOUND anchor-reads: "*"commit: $RC7 tree-start: $RC12"*)
      ok "delta pair: a DISTINCT DELTA-RECERT line names the anchor + the merged tree" ;;
    *) bad "delta pair: missing the DELTA-RECERT evidence line (got: $OUT)" ;;
  esac
  # `anchor-reads:` must be AFFIRMATIVE, never silent (job 358). The suite always
  # puts a `--kill-after`-capable runner on PATH via the $BIN shim, so the bounded
  # value is the only correct one here; the UNBOUNDED spelling is asserted by its
  # own arm below, which constructs a PATH without one.
  case "$OUT" in
    *"anchor-reads: bounded-"*)
      ok "delta pair: the evidence line AFFIRMS the reads were bounded" ;;
    *) bad "delta pair: expected an affirmative bounded anchor-reads: token (got: $OUT)" ;;
  esac
  # THE TOKEN MUST NOT CLAIM MORE THAN IS BOUNDED (job 382). `_anchor_canon`
  # (`cd`+`pwd -P`) and the object-dir `[ -d … ]` probe are SHELL BUILTINS with no
  # process to bound, so a bare `bounded-<n>s+<g>s` overclaimed. The token now
  # names both halves, and this arm pins BOTH — a reworded token that quietly
  # dropped the unbounded half would restore the overclaim.
  case "$OUT" in
    *"anchor-reads: bounded-"*"external:git,mktemp,sh"*)
      ok "delta pair: the token names WHAT is bounded (the external commands)" ;;
    *) bad "delta pair: the anchor-reads token must name the bounded externals (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"UNBOUNDED:command-v+pwd-builtins"*)
      ok "delta pair: the token also names what is NOT bounded — no overclaim" ;;
    *) bad "delta pair: the anchor-reads token must declare the UNBOUNDED builtin probes (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"summary: $RGOODDELTA"*) ok "delta pair: the DELTA-RECERT line names the delta summary file" ;;
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
if run_anc 0 "delta pair with BOTH blocks dirty: no -> exit 0" \
  2421 "$R_CERT" "$RANCFULL" "$RGOODDELTA"; then
  case "$OUT" in
    *"GATE-OF-RECORD commit: $RA7"*"dirty: no"*"DELTA-RECERT anchor:"*"dirty: no"*)
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
# Case 44: THE CASE B ANCHOR IS BOUND TO THE CERTIFIED SHA'S HISTORY (#3653)
# =============================================================================
# The fixture (built above) is c0 -> c1 (ANCHOR) -> c2 (CERTIFIED), with cF a
# sibling of c0 that is NOT an ancestor of c2. The accept arm ran at Case 28(b);
# these are the refusing arms, the load-bearing proof, and the UNVERIFIABLE
# family.

# --- 44(a): the accept arm's AFFIRMATIVE record ------------------------------
# A silent pass is indistinguishable from a check that never ran, so the passing
# verdict must appear in the output. Pinned separately from 28(b)'s composite
# needle so a reword of the surrounding line cannot take this assertion with it.
if run_anc 0 "ancestry BOUND: anchor c1 IS an ancestor of the certified head -> exit 0" \
  2421 "$R_CERT" "$RANCFULL" "$RGOODDELTA"; then
  case "$OUT" in
    *"anchor-ancestry: BOUND"*)
      ok "ancestry: the success path RECORDS the binding affirmatively (anchor-ancestry: BOUND)" ;;
    *) bad "ancestry: the success path must record anchor-ancestry: BOUND (got: $OUT)" ;;
  esac
fi

# --- 44(b): THE RED ARM — one property different, and only one ---------------
# The two delta summaries come from ONE builder (`anc_delta`) whose only varying
# input is the anchor sha, so substituting the foreign sha for the anchor sha in
# the foreign file must reproduce the accepted file BYTE FOR BYTE. Asserted, not
# assumed: a RED arm that differs in some second property proves nothing about
# the property under test (MEMORY.md: "a RED arm must differ in one property").
# THE FOREIGN PAIR IS THE REALISTIC #3616 SHAPE, and it has to be: the checks
# ABOVE the ancestry binding require the full block's own provenance to BE the
# delta's anchor, so a foreign anchor pinned against the LOCAL full block refuses
# earlier, for a different reason. What survives those checks — and what #3653
# closes — is a peer lane's genuine, internally consistent full-gate PASS plus a
# delta naming it. That is exactly `anc_pair foreign`.
if anc_same_but_anchor "$R_FOREIGN" "$RFORDELTA" "$RGOODDELTA" &&
   anc_same_but_anchor "$R_FOREIGN" "$RFORFULL" "$RANCFULL"; then
  ok "ancestry RED arm: the foreign pair differs from the accepted pair in EXACTLY ONE property (the anchor sha)"
else
  bad "ancestry RED arm: the foreign and accepted pairs differ in more than the anchor sha — the arm would prove nothing"
fi
if ! cmp -s "$RFORDELTA" "$RGOODDELTA" && ! cmp -s "$RFORFULL" "$RANCFULL"; then
  ok "ancestry RED arm: non-vacuity — both halves of the pair are genuinely different files"
else
  bad "ancestry RED arm: a half of the foreign pair is IDENTICAL to the accepted one — the arm is vacuous"
fi
if run_anc 2 "ancestry NOT-ANCESTOR: a FOREIGN anchor pair -> refuse (exit 2)" \
  2421 "$R_CERT" "$RFORFULL" "$RFORDELTA"; then
  case "$OUT" in
    *"PREMERGE: NO-GATE-OF-RECORD"*)
      ok "ancestry: a foreign anchor is refused under the NO-GATE-OF-RECORD verdict" ;;
    *) bad "ancestry: a foreign anchor must carry the NO-GATE-OF-RECORD verdict (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"is NOT on the certified sha's history"*)
      ok "ancestry: the refusal NAMES the cause (the anchor is not on this PR's history)" ;;
    *) bad "ancestry: the refusal must name the cause (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"$R_FOREIGN"*) ok "ancestry: the refusal names the ANCHOR sha it rejected" ;;
    *) bad "ancestry: the refusal must name the anchor sha (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"$R_CERT"*) ok "ancestry: the refusal names the CERTIFIED sha it compared against" ;;
    *) bad "ancestry: the refusal must name the certified sha (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"#3616"*) ok "ancestry: the refusal names the class it closes (#3616 cross-lane)" ;;
    *) bad "ancestry: the refusal should name the #3616 class (got: $OUT)" ;;
  esac
fi

# --- 44(c): THE MUTATION ARM — the check is LOAD-BEARING ---------------------
# 44(b) reds today; that alone does not prove the ANCESTRY CHECK is what refuses
# it, because some other check might be doing the work. So: substitute the
# ARTIFACT in a scratch tree (the `flow_copy` idiom — never a path variable or a
# test-only seam, which would be one more thing a real invoker could set), NEUTER
# the ancestry call there, and require the SAME foreign-anchor call to reach
# exit 0. If it does not, 44(b) proves nothing.
MUTDIR="$T/flow-anc-mutant"
mkdir -p "$MUTDIR"
mut_ok=0
if ! cp "$ASSERT" "$MUTDIR/premerge-assert.sh"; then
  bad "ancestry mutation: could not copy premerge-assert.sh into the scratch tree"
else
  printf '%s\n' "$NEUTRAL_ADV" >"$MUTDIR/base-staleness.sh"
  chmod +x "$MUTDIR/base-staleness.sh"
  # The call site, replaced by the assignment it would have made on success.
  # shellcheck disable=SC2016  # a LITERAL line of another script; it must not expand here
  MUT_FROM='  assert_anchor_on_history "$delta_anchor" "$certified"'
  MUT_TO='  ANCHOR_ANCESTRY=BOUND   # MUTANT: ancestry check removed'
  if [ "$(grep -c -x -F -- "$MUT_FROM" "$MUTDIR/premerge-assert.sh" | tr -d ' ')" = 1 ]; then
    ok "ancestry mutation: the shipped script has exactly ONE ancestry call site to neuter"
    # awk rather than sed: the needle contains `$` and `"`, and a literal
    # whole-line match is what the count above verified.
    awk -v from="$MUT_FROM" -v to="$MUT_TO" '{ if ($0 == from) print to; else print }' \
      "$MUTDIR/premerge-assert.sh" >"$MUTDIR/mutated.sh" &&
      mv "$MUTDIR/mutated.sh" "$MUTDIR/premerge-assert.sh" && mut_ok=1
  else
    bad "ancestry mutation: expected exactly one ancestry call site in the shipped script (found $(grep -c -x -F -- "$MUT_FROM" "$MUTDIR/premerge-assert.sh" | tr -d ' '))"
  fi
fi
if [ "$mut_ok" -eq 1 ]; then
  # ASSERT THE MUTATION TOOK. A mutation that silently failed to apply would make
  # this whole arm vacuously green — the exact failure mode it exists to catch.
  if [ "$(grep -c -x -F -- "$MUT_FROM" "$MUTDIR/premerge-assert.sh" | tr -d ' ')" = 0 ] &&
     [ "$(grep -c -x -F -- "$MUT_TO" "$MUTDIR/premerge-assert.sh" | tr -d ' ')" = 1 ]; then
    ok "ancestry mutation: the scratch copy really differs from the shipped one in the expected way"
  else
    bad "ancestry mutation: the mutation did not apply — the arm would be vacuous"
    mut_ok=0
  fi
fi
if [ "$mut_ok" -eq 1 ]; then
  MUTOUT=$(cd "$ANC_REPO" && PATH="$BIN:$PATH" MOCK_GH_FAIL=0 MOCK_GH_OUT="$R_CERT OPEN" \
    bash "$MUTDIR/premerge-assert.sh" 2421 "$R_CERT" "$RFORFULL" "$RFORDELTA" 2>&1)
  MUTRC=$?
  if [ "$MUTRC" -eq 0 ]; then
    ok "ancestry mutation: WITHOUT the check the foreign anchor reaches exit 0 — the check is load-bearing"
  else
    bad "ancestry mutation: the foreign-anchor call still fails (exit $MUTRC) with the check removed, so 44(b) proves nothing about it (got: $MUTOUT)"
  fi
fi

# --- 44(d): UNVERIFIABLE — an absent object is UNKNOWN, never ok -------------
# A pass may never be derived from the ABSENCE of a bad signal, so a sha this
# repository does not hold is exit 3 under its OWN marker, not a silent accept
# and not exit 2 (which would tell the operator their chain is wrong when the
# real fault is the checkout).
ABSENT_SHA="0123456789abcdef0123456789abcdef01234567"
if git -C "$ANC_REPO" cat-file -e "$ABSENT_SHA^{commit}" >/dev/null 2>&1; then
  bad "ancestry UNVERIFIABLE fixture: the 'absent' sha is present in the fixture repo"
else
  ok "ancestry UNVERIFIABLE fixture: the chosen 40-hex sha really is absent from the fixture repo"
fi
anc_pair absent "$ABSENT_SHA"
if run_anc 3 "ancestry UNVERIFIABLE: an ANCHOR sha absent from this repository -> exit 3" \
  2421 "$R_CERT" "$T/anc-full-absent.txt" "$T/anc-delta-absent.txt"; then
  case "$OUT" in
    *"PREMERGE: ANCHOR-UNVERIFIABLE"*)
      ok "ancestry: an absent anchor object carries the ANCHOR-UNVERIFIABLE marker" ;;
    *) bad "ancestry: an absent anchor object must carry ANCHOR-UNVERIFIABLE (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"the ANCHOR commit is not present in this repository"*)
      ok "ancestry: the UNVERIFIABLE refusal names WHICH object is absent" ;;
    *) bad "ancestry: the UNVERIFIABLE refusal must name the absent object (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"git fetch origin"*) ok "ancestry: the absent-object cause carries its own remedy (fetch the branch)" ;;
    *) bad "ancestry: the absent-object cause must carry a fetch remedy (got: $OUT)" ;;
  esac
  # THE MARKER MUST BE TEXTUALLY DISTINCT from the other two verdicts, because
  # exit 3 and exit 2 name DIFFERENT operator actions and the codes alone cannot
  # carry that (the same reason nit 8 split USAGE from GH-FAILURE).
  case "$OUT" in
    *"PREMERGE: NO-GATE-OF-RECORD"*)
      bad "ancestry: UNVERIFIABLE must NOT read as NO-GATE-OF-RECORD (got: $OUT)" ;;
    *) ok "ancestry: UNVERIFIABLE is textually DISTINCT from NO-GATE-OF-RECORD" ;;
  esac
  case "$OUT" in
    *"PREMERGE: TOOL-FAILURE"*)
      bad "ancestry: UNVERIFIABLE must NOT read as TOOL-FAILURE (got: $OUT)" ;;
    *) ok "ancestry: UNVERIFIABLE is textually DISTINCT from TOOL-FAILURE" ;;
  esac
fi

# The CERTIFIED sha absent is its own cause with its own remedy (the two are
# different operator actions in practice: a rebased-away anchor vs a PR branch
# that was never fetched).
NOREPO="$T/anc-not-a-repo"
mkdir -p "$NOREPO"
if (cd "$NOREPO" && git rev-parse --git-dir >/dev/null 2>&1); then
  bad "ancestry UNVERIFIABLE fixture: $NOREPO is inside a git work tree — the no-work-tree case would not fire"
else
  ok "ancestry UNVERIFIABLE fixture: the scratch dir really is outside any git work tree"
  OUT=$(cd "$NOREPO" && PATH="$BIN:$PATH" MOCK_GH_FAIL=0 MOCK_GH_OUT="$R_CERT OPEN" \
    bash "$NEUTRAL_ASSERT" 2421 "$R_CERT" "$RANCFULL" "$RGOODDELTA" 2>&1)
  RC=$?
  if [ "$RC" -ne 3 ]; then
    bad "ancestry UNVERIFIABLE: outside a work tree must be exit 3 (got $RC: $OUT)"
  else
    ok "ancestry UNVERIFIABLE: run outside a git work tree -> exit 3"
    case "$OUT" in
      *"PREMERGE: ANCHOR-UNVERIFIABLE"*)
        ok "ancestry: the no-work-tree case carries the ANCHOR-UNVERIFIABLE marker" ;;
      *) bad "ancestry: the no-work-tree case must carry ANCHOR-UNVERIFIABLE (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"not inside a git work tree"*)
        ok "ancestry: the no-work-tree case names its OWN cause, not the absent-object one" ;;
      *) bad "ancestry: the no-work-tree case must name its own cause (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"run this assert from the ISSUE'S WORKTREE"*)
        ok "ancestry: the no-work-tree cause carries its own remedy" ;;
      *) bad "ancestry: the no-work-tree cause must carry its own remedy (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"PREMERGE: NO-GATE-OF-RECORD"*|*"PREMERGE: TOOL-FAILURE"*)
        bad "ancestry: the no-work-tree case must not read as another verdict (got: $OUT)" ;;
      *) ok "ancestry: the no-work-tree marker is distinct from NO-GATE-OF-RECORD and TOOL-FAILURE" ;;
    esac
  fi
fi

# The CERTIFIED object absent is the OTHER half of the presence probe, and it has
# its own cause and its own remedy — a rebased-away anchor and a PR branch that
# was never fetched are different operator actions. Probed separately so a single
# combined message could not pass for both.
FAKE_CERT="fedcba9876543210fedcba9876543210fedcba98"
FC7=$(printf '%.7s' "$FAKE_CERT"); FC12=$(printf '%.12s' "$FAKE_CERT")
if git -C "$ANC_REPO" cat-file -e "$FAKE_CERT^{commit}" >/dev/null 2>&1; then
  bad "ancestry UNVERIFIABLE fixture: the 'absent certified' sha is present in the fixture repo"
else
  ok "ancestry UNVERIFIABLE fixture: the chosen certified sha really is absent from the fixture repo"
fi
delta_summary "$T/anc-delta-fakecert.txt" "$R_ANCHOR" "$FC7" "$FC12" PASS PASS \
  "MODE: delta (TEST/DOCS-ONLY RE-CERTIFICATION — NOT the gate of record; gate of record = the full agent-gate.sh PASS at anchor $R_ANCHOR)"
if run_anc 3 "ancestry UNVERIFIABLE: the CERTIFIED sha absent from this repository -> exit 3" \
  2421 "$FAKE_CERT" "$RANCFULL" "$T/anc-delta-fakecert.txt"; then
  case "$OUT" in
    *"the CERTIFIED commit is not present in this repository"*)
      ok "ancestry: the absent-CERTIFIED cause is named separately from the absent-ANCHOR one" ;;
    *) bad "ancestry: expected the absent-CERTIFIED cause (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: ANCHOR-UNVERIFIABLE"*)
      ok "ancestry: an absent certified object also carries the ANCHOR-UNVERIFIABLE marker" ;;
    *) bad "ancestry: expected ANCHOR-UNVERIFIABLE (got: $OUT)" ;;
  esac
fi

# git ABSENT is a cause too, and it must not be mistaken for "the anchor is not
# an ancestor". A PATH holding only what the parser needs (awk, tr) plus the gh
# mock — and NOT git — is the whole fixture; the check must refuse before it ever
# calls git. NON-VACUITY: the fixture PATH is asserted to really lack git.
NOGIT="$T/bin-nogit"
mkdir -p "$NOGIT"
nogit_ok=1
for _tool in awk tr; do
  _tp=$(command -v "$_tool" 2>/dev/null) || _tp=""
  if [ -n "$_tp" ]; then ln -sf "$_tp" "$NOGIT/$_tool"; else nogit_ok=0; fi
done
cp "$BIN/gh" "$NOGIT/gh" 2>/dev/null || nogit_ok=0
if [ "$nogit_ok" -eq 1 ] && PATH="$NOGIT" command -v git >/dev/null 2>&1; then
  nogit_ok=0
fi
if [ "$nogit_ok" -ne 1 ]; then
  bad "ancestry no-git fixture: could not build a PATH that has awk/tr/gh but NOT git — the arm did NOT run"
else
  ok "ancestry no-git fixture: the fixture PATH really has no git on it"
  # `bash` is invoked by ABSOLUTE path: the fixture PATH deliberately holds only
  # awk/tr/gh, so a bare `bash` would be "command not found" — a 127 that looks
  # like a refusal and proves nothing about the check.
  OUT=$(cd "$ANC_REPO" && PATH="$NOGIT" MOCK_GH_FAIL=0 MOCK_GH_OUT="$R_CERT OPEN" \
    "${BASH:-/bin/bash}" "$NEUTRAL_ASSERT" 2421 "$R_CERT" "$RANCFULL" "$RGOODDELTA" 2>&1)
  RC=$?
  if [ "$RC" -ne 3 ]; then
    bad "ancestry no-git: git absent must be exit 3, never a pass (got $RC: $OUT)"
  else
    ok "ancestry no-git: git absent from PATH -> exit 3, never a silent accept"
    case "$OUT" in
      *"PREMERGE: ANCHOR-UNVERIFIABLE"*"git is not on PATH"*)
        ok "ancestry no-git: the marker and the cause both name what could not be measured" ;;
      *) bad "ancestry no-git: expected ANCHOR-UNVERIFIABLE naming the absent git (got: $OUT)" ;;
    esac
  fi
fi

# --- 44(e): rc 1 IN A SHALLOW CLONE IS NOT A VERDICT (rebuilt, roborev job 410) -
#
# The subtlest arm, and the reason the check is three-valued at all (#3544):
# `--is-ancestor` exits 1 both for "not an ancestor" AND for "the connecting
# history is absent".
#
# THE PREVIOUS VERSION OF THIS ARM WAS VACUOUS, and that is what job 410 caught.
# It compared `$R_FOREIGN`, which is genuinely NOT an ancestor even in the
# complete repository — so rc 1 was the CORRECT answer and the arm never
# exercised the ambiguous case at all. It passed without testing its own subject,
# which is the class this whole issue treats as must-fix: a green that proves
# nothing is worse than a missing test.
#
# WHAT THE CASE ACTUALLY REQUIRES is a TRUE ancestor that returns rc 1 SOLELY
# because the connecting commits are gone. So: a 6-commit history, a shallow
# clone holding only the tip, then a `--depth 1` fetch of a branch at the OLDEST
# commit — which brings that endpoint in as its own shallow boundary while
# c1..c4 stay absent. Both endpoints present, the path between them missing.
#
# AND THE ASSERTION IS THE DIFFERENTIAL, twice over, because either half alone
# proves nothing:
#   git level   : complete repo -> rc 0 (TRUE ancestor) | shallow -> rc 1
#   guard level : complete repo -> exit 0 + BOUND       | shallow -> exit 3
# The second pair is the one that matters: the SAME anchor/certified pair must be
# BOUND where the history is complete and UNVERIFIABLE where it is not.
SH_REPO="$T/shallow-src"
SH_CLONE="$T/shallow-clone"
shallow_shape=0
SH_A=""; SH_C=""
mkdir -p "$SH_REPO"
if git init -q -b main "$SH_REPO" >/dev/null 2>&1; then
  git -C "$SH_REPO" config user.email t@t
  git -C "$SH_REPO" config user.name t
  _sh_ok=1
  for _i in 0 1 2 3 4 5; do
    printf 'c%s\n' "$_i" >"$SH_REPO/f$_i"
    git -C "$SH_REPO" add -- "f$_i" >/dev/null 2>&1 &&
      git -C "$SH_REPO" commit -q -m "c$_i" >/dev/null 2>&1 || _sh_ok=0
  done
  if [ "$_sh_ok" -eq 1 ]; then
    SH_A=$(git -C "$SH_REPO" rev-parse main~5 2>/dev/null) || SH_A=""
    SH_C=$(git -C "$SH_REPO" rev-parse main 2>/dev/null) || SH_C=""
    # a branch at the OLDEST commit, so the endpoint can be fetched at depth 1
    # without needing uploadpack.allowAnySHA1InWant on the source.
    [ -n "$SH_A" ] && git -C "$SH_REPO" branch old "$SH_A" >/dev/null 2>&1
  fi
fi
if [ -n "$SH_A" ] && [ -n "$SH_C" ] && [ "$SH_A" != "$SH_C" ] &&
   git clone -q --depth 1 --no-local --branch main "file://$SH_REPO" "$SH_CLONE" >/dev/null 2>&1 &&
   git -C "$SH_CLONE" fetch -q --depth 1 origin old:refs/remotes/origin/old >/dev/null 2>&1; then
  # FIXTURE SELF-CONSISTENCY: shallow, BOTH endpoints present, the CONNECTING
  # commit absent. Without the third condition the fixture is not the shape the
  # case claims, which is exactly how the previous version went vacuous.
  _sh_mid=$(git -C "$SH_REPO" rev-parse main~2 2>/dev/null) || _sh_mid=""
  if [ "$(git -C "$SH_CLONE" rev-parse --is-shallow-repository 2>/dev/null)" = true ] &&
     git -C "$SH_CLONE" cat-file -e "$SH_A^{commit}" >/dev/null 2>&1 &&
     git -C "$SH_CLONE" cat-file -e "$SH_C^{commit}" >/dev/null 2>&1 &&
     [ -n "$_sh_mid" ] &&
     ! git -C "$SH_CLONE" cat-file -e "$_sh_mid^{commit}" >/dev/null 2>&1; then
    shallow_shape=1
  fi
fi
if [ "$shallow_shape" -eq 1 ]; then
  ok "shallow fixture: shallow clone holds BOTH endpoints and NOT the connecting commit"
  # THE GIT-LEVEL DIFFERENTIAL, which is what makes rc 1 ambiguous rather than a
  # verdict. Both halves asserted: either alone would prove nothing.
  if git -C "$SH_REPO" merge-base --is-ancestor "$SH_A" "$SH_C" >/dev/null 2>&1; then
    ok "shallow differential (git): in the COMPLETE repo the pair IS an ancestor (rc 0)"
  else
    bad "shallow differential (git): the pair is not an ancestor even in the complete repo — the fixture cannot exercise the ambiguous case (this was the previous arm's defect)"
    shallow_shape=0
  fi
  if git -C "$SH_CLONE" merge-base --is-ancestor "$SH_A" "$SH_C" >/dev/null 2>&1; then
    bad "shallow differential (git): the SHALLOW clone still reports the pair as an ancestor — rc 1 is not being produced, so the arm proves nothing"
    shallow_shape=0
  else
    ok "shallow differential (git): in the SHALLOW clone the SAME pair returns rc 1 — history absent, not 'not an ancestor'"
  fi
fi
if [ "$shallow_shape" -eq 1 ]; then
  SHFULL="$T/shallow-full.txt"
  SHDELTA="$T/shallow-delta.txt"
  full_summary "$SHFULL" "$(printf '%.7s' "$SH_A")" "$(printf '%.12s' "$SH_A")" PASS PASS
  delta_summary "$SHDELTA" "$SH_A" "$(printf '%.7s' "$SH_C")" "$(printf '%.12s' "$SH_C")" \
    PASS PASS "MODE: delta (TEST/DOCS-ONLY RE-CERTIFICATION — anchor $SH_A)"
  # GUARD LEVEL, half 1: the COMPLETE repository must BIND the same pair. Without
  # this the shallow refusal below could be any unrelated refusal.
  OUT=$(cd "$SH_REPO" && PATH="$BIN:$PATH" MOCK_GH_FAIL=0 MOCK_GH_OUT="$SH_C OPEN" \
    bash "$NEUTRAL_ASSERT" 2421 "$SH_C" "$SHFULL" "$SHDELTA" 2>&1)
  RC=$?
  if [ "$RC" -eq 0 ] && [ "${OUT#*anchor-ancestry: BOUND}" != "$OUT" ]; then
    ok "shallow differential (guard): the COMPLETE repo BINDS the pair (exit 0, anchor-ancestry: BOUND)"
  else
    bad "shallow differential (guard): the complete repo did not BIND the pair (exit $RC) — the shallow half below would prove nothing (got: $OUT)"
  fi
  # GUARD LEVEL, half 2: the SHALLOW clone must refuse UNVERIFIABLE, NOT exit 2.
  OUT=$(cd "$SH_CLONE" && PATH="$BIN:$PATH" MOCK_GH_FAIL=0 MOCK_GH_OUT="$SH_C OPEN" \
    bash "$NEUTRAL_ASSERT" 2421 "$SH_C" "$SHFULL" "$SHDELTA" 2>&1)
  RC=$?
  if [ "$RC" -ne 3 ]; then
    bad "shallow (guard): a TRUE ancestor whose history is absent must be exit 3, never the exit-2 verdict (got $RC: $OUT)"
  else
    ok "shallow (guard): the SAME pair in a shallow clone is exit 3, never the exit-2 refusal"
    case "$OUT" in
      *"PREMERGE: ANCHOR-UNVERIFIABLE"*)
        ok "shallow (guard): carries the ANCHOR-UNVERIFIABLE marker" ;;
      *) bad "shallow (guard): expected the ANCHOR-UNVERIFIABLE marker (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"NOT PROVEN COMPLETE"*)
        ok "shallow (guard): the refusal names the incomplete history, not 'not an ancestor'" ;;
      *) bad "shallow (guard): the refusal must name the incomplete history (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"is NOT on the certified sha's history"*)
        bad "shallow (guard): a TRUE ancestor was reported as NOT on the history — the exact false verdict this arm exists to prevent (got: $OUT)" ;;
      *) ok "shallow (guard): a TRUE ancestor is NOT reported as 'not on the history'" ;;
    esac
    case "$OUT" in
      *"git fetch --unshallow"*)
        ok "shallow (guard): the shallow cause carries its own remedy (git fetch --unshallow)" ;;
      *) bad "shallow (guard): the shallow cause must carry the unshallow remedy (got: $OUT)" ;;
    esac
  fi
else
  # DECLARED, not silently skipped, and non-fatal: a host whose git cannot build a
  # `--depth` clone over `file://` plus a `--depth 1` branch fetch is a HOST
  # property, not a defect in the tree under test. A lane that reds on correct
  # input is the lane agents learn to waive.
  printf 'ARM NOT TAKEN: shallow rc-1-ambiguity (job 410) — this host could not build the fixture: a\n'
  printf 'ARM NOT TAKEN: 6-commit history, a --depth 1 clone over file://, and a --depth 1 fetch of a\n'
  printf 'ARM NOT TAKEN: branch at the OLDEST commit, yielding a shallow repo that holds BOTH endpoints\n'
  printf 'ARM NOT TAKEN: and NOT the connecting commits. Without it rc 1 cannot be made ambiguous, so\n'
  printf 'ARM NOT TAKEN: the shallow branch of assert_anchor_on_history is UNEXERCISED on this run.\n'
  ok "shallow: SKIPPED (differential fixture unbuildable — arm UNEXERCISED, declared not silent)"
fi

# --- 44(e2): AN INCOMPLETE OBJECT STORE WITHOUT A SHALLOW MARKER (job 412) ----
#
# THE VERDICT DEFECT this arm exists for: `--is-shallow-repository = false` says
# there is no shallow MARKER, not that the history is COMPLETE. A missing
# intermediate COMMIT object in a non-shallow repository produces the same rc 1
# from the ancestry walk — and the script used to call that NOT-ANCESTOR at
# exit 2, i.e. "your chain is wrong", when the truth was "this box could not
# measure it". A false DEFINITIVE verdict, which is the bad direction.
#
# THE DIFFERENTIAL IS THE PROOF, exactly as for the shallow arm: the same pair
# must be BOUND in the INTACT copy and UNVERIFIABLE once one intermediate commit
# object is removed. Without the intact half, the refusal could be anything.
BR_SRC="$T/broken-src"
BR_INTACT="$T/broken-intact"
broken_shape=0
BR_A=""; BR_C=""; BR_MID=""
mkdir -p "$BR_SRC"
if git init -q -b main "$BR_SRC" >/dev/null 2>&1; then
  git -C "$BR_SRC" config user.email t@t
  git -C "$BR_SRC" config user.name t
  _br_ok=1
  for _i in 0 1 2 3 4 5; do
    printf 'c%s\n' "$_i" >"$BR_SRC/f$_i"
    git -C "$BR_SRC" add -- "f$_i" >/dev/null 2>&1 &&
      git -C "$BR_SRC" commit -q -m "c$_i" >/dev/null 2>&1 || _br_ok=0
  done
  if [ "$_br_ok" -eq 1 ]; then
    BR_A=$(git -C "$BR_SRC" rev-parse main~5 2>/dev/null) || BR_A=""
    BR_C=$(git -C "$BR_SRC" rev-parse main 2>/dev/null) || BR_C=""
    BR_MID=$(git -C "$BR_SRC" rev-parse main~2 2>/dev/null) || BR_MID=""
  fi
fi
# The INTACT copy is the differential's other half; the SOURCE is the one broken.
if [ -n "$BR_A" ] && [ -n "$BR_C" ] && [ -n "$BR_MID" ] &&
   cp -a "$BR_SRC" "$BR_INTACT" 2>/dev/null; then
  # Objects are loose here (no repack was run), so removing the intermediate
  # COMMIT object is a single unlink. Trees and blobs are untouched: the point is
  # that the COMMIT graph — what `--is-ancestor` traverses — is broken.
  _br_obj="$BR_SRC/.git/objects/$(printf '%s' "$BR_MID" | cut -c1-2)/$(printf '%s' "$BR_MID" | cut -c3-)"
  if [ -f "$_br_obj" ] && rm -f "$_br_obj"; then
    # FIXTURE SELF-CONSISTENCY, four conditions. The third is the whole subject
    # (no shallow marker) and the fourth is what makes the case ambiguous.
    if [ "$(git -C "$BR_SRC" rev-parse --is-shallow-repository 2>/dev/null)" = false ] &&
       ! git -C "$BR_SRC" cat-file -e "$BR_MID^{commit}" >/dev/null 2>&1 &&
       git -C "$BR_SRC" cat-file -e "$BR_A^{commit}" >/dev/null 2>&1 &&
       git -C "$BR_SRC" cat-file -e "$BR_C^{commit}" >/dev/null 2>&1; then
      broken_shape=1
    fi
  fi
fi
if [ "$broken_shape" -eq 1 ]; then
  ok "incomplete-store fixture: NO shallow marker, both endpoints present, an INTERMEDIATE commit removed"
  # THE GIT-LEVEL DIFFERENTIAL: intact -> rc 0, broken -> rc 1 with no shallow
  # marker. That second line is the exact input that produced the false verdict.
  if git -C "$BR_INTACT" merge-base --is-ancestor "$BR_A" "$BR_C" >/dev/null 2>&1; then
    ok "incomplete-store differential (git): the INTACT copy reports the pair as an ancestor (rc 0)"
  else
    bad "incomplete-store differential (git): the pair is not an ancestor even intact — the fixture cannot exercise the case"
    broken_shape=0
  fi
  if git -C "$BR_SRC" merge-base --is-ancestor "$BR_A" "$BR_C" >/dev/null 2>&1; then
    bad "incomplete-store differential (git): the BROKEN copy still reports an ancestor — rc 1 is not being produced, so the arm proves nothing"
    broken_shape=0
  else
    ok "incomplete-store differential (git): the BROKEN copy returns rc 1 with NO shallow marker — the false-verdict input"
  fi
fi
if [ "$broken_shape" -eq 1 ]; then
  BRFULL="$T/broken-full.txt"
  BRDELTA="$T/broken-delta.txt"
  full_summary "$BRFULL" "$(printf '%.7s' "$BR_A")" "$(printf '%.12s' "$BR_A")" PASS PASS
  delta_summary "$BRDELTA" "$BR_A" "$(printf '%.7s' "$BR_C")" "$(printf '%.12s' "$BR_C")" \
    PASS PASS "MODE: delta (TEST/DOCS-ONLY RE-CERTIFICATION — anchor $BR_A)"
  # GUARD LEVEL, half 1 — the INTACT copy must BIND, or half 2 proves nothing.
  OUT=$(cd "$BR_INTACT" && PATH="$BIN:$PATH" MOCK_GH_FAIL=0 MOCK_GH_OUT="$BR_C OPEN" \
    bash "$NEUTRAL_ASSERT" 2421 "$BR_C" "$BRFULL" "$BRDELTA" 2>&1)
  RC=$?
  if [ "$RC" -eq 0 ] && [ "${OUT#*anchor-ancestry: BOUND}" != "$OUT" ]; then
    ok "incomplete-store differential (guard): the INTACT copy BINDS the pair (exit 0, anchor-ancestry: BOUND)"
  else
    bad "incomplete-store differential (guard): the intact copy did not BIND (exit $RC) — the broken half proves nothing (got: $OUT)"
  fi
  # GUARD LEVEL, half 2 — the BROKEN copy must be UNVERIFIABLE, never exit 2.
  OUT=$(cd "$BR_SRC" && PATH="$BIN:$PATH" MOCK_GH_FAIL=0 MOCK_GH_OUT="$BR_C OPEN" \
    bash "$NEUTRAL_ASSERT" 2421 "$BR_C" "$BRFULL" "$BRDELTA" 2>&1)
  RC=$?
  if [ "$RC" -eq 2 ]; then
    bad "incomplete-store (guard): a VALID anchor was refused at exit 2 as NOT-ANCESTOR — the false definitive verdict this arm exists to prevent (got: $OUT)"
  elif [ "$RC" -ne 3 ]; then
    bad "incomplete-store (guard): expected exit 3 (got $RC: $OUT)"
  else
    ok "incomplete-store (guard): an incomplete store is exit 3, NEVER the exit-2 NOT-ANCESTOR verdict"
    case "$OUT" in
      *"PREMERGE: ANCHOR-UNVERIFIABLE"*)
        ok "incomplete-store (guard): carries the ANCHOR-UNVERIFIABLE marker" ;;
      *) bad "incomplete-store (guard): expected the ANCHOR-UNVERIFIABLE marker (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"reachable history is INCOMPLETE"*)
        ok "incomplete-store (guard): the cause names the incomplete history" ;;
      *) bad "incomplete-store (guard): the cause must name the incomplete history (got: $OUT)" ;;
    esac
    # THE THREE REASONS rc 1 IS NOT A VERDICT MUST BE DISTINGUISHABLE. This one is
    # neither the shallow cause nor a timeout, and it must not read as either.
    case "$OUT" in
      *"NOT PROVEN COMPLETE"*)
        bad "incomplete-store (guard): reported as the SHALLOW cause — the operator would run git fetch --unshallow on a repo with no shallow marker (got: $OUT)" ;;
      *) ok "incomplete-store (guard): NOT reported as the shallow cause" ;;
    esac
    case "$OUT" in
      *"timed out after"*)
        bad "incomplete-store (guard): reported as a TIMEOUT (got: $OUT)" ;;
      *) ok "incomplete-store (guard): NOT reported as a timeout" ;;
    esac
    case "$OUT" in
      *"git fsck"*)
        ok "incomplete-store (guard): the cause carries its own remedy (git fsck / git fetch)" ;;
      *) bad "incomplete-store (guard): the cause must carry a repair remedy (got: $OUT)" ;;
    esac
  fi
else
  printf 'ARM NOT TAKEN: incomplete object store (job 412) — this host could not build the fixture: a\n'
  printf 'ARM NOT TAKEN: 6-commit history with an INTERMEDIATE loose commit object removed, no shallow\n'
  printf 'ARM NOT TAKEN: marker, both endpoints intact, plus an untouched copy for the differential.\n'
  printf 'ARM NOT TAKEN: Without it the connectivity branch of assert_anchor_on_history is UNEXERCISED.\n'
  ok "incomplete-store: SKIPPED (differential fixture unbuildable — arm UNEXERCISED, declared not silent)"
fi

# --- 44(f): A GRAFT MUST NOT BE ABLE TO MANUFACTURE `BOUND` (roborev job 355) -
#
# `$GIT_DIR/info/grafts` rewrites parentage and **survives
# `--no-replace-objects`** (CLAUDE.md, #3544 job 285: measured `no -> YES -> YES`
# across before-graft / plain / `--no-replace-objects`). Every lane here is a
# worktree of ONE shared `.git` and grafts live in the COMMON git dir, so the
# planter is a PEER LANE — a non-invoker route — and a leftover debugging graft
# is the accident route. Either alone makes it a defect, which is why the walk
# was MOVED into an isolated scratch repository rather than the graft flagged.
#
# TWO ARMS OVER ONE FIXTURE, AND ARM 1 IS A POSITIVE CONTROL (CLAUDE.md's job-264
# rule: assert UNREACHABILITY with a control proving the attack executes in a
# plain repository, or the green means nothing).
#   1. plain `git merge-base --is-ancestor` in the grafted repository answers 0
#      — the attack works here, on this host's git;
#   2. premerge-assert.sh on that SAME repository, with that SAME graft, still
#      refuses (exit 2, NOT-ANCESTOR).
# If arm 1 does not reproduce, arm 2 proves nothing and the case says so as a
# declared not-taken arm rather than passing quietly.
GRAFT_REPO="$T/ancestry-graft-repo"
graft_shape=0
if [ "$anc_shape" -eq 1 ] && cp -a "$ANC_REPO" "$GRAFT_REPO" 2>/dev/null; then
  # The graft makes the FOREIGN commit a parent of the CERTIFIED head, which is
  # precisely "make a foreign anchor look ancestral".
  if printf '%s %s\n' "$R_CERT" "$R_FOREIGN" >"$GRAFT_REPO/.git/info/grafts" 2>/dev/null; then
    graft_shape=1
  fi
fi
graft_attack=0
if [ "$graft_shape" -eq 1 ]; then
  # NON-VACUITY, both directions: the UNGRAFTED copy must still answer "no", or
  # a fixture that was ancestral all along would make arm 1 meaningless.
  if git -C "$ANC_REPO" merge-base --is-ancestor "$R_FOREIGN" "$R_CERT" >/dev/null 2>&1; then
    bad "graft control: the UNGRAFTED fixture already reports the foreign anchor as ancestral — the arm would be meaningless"
    graft_shape=0
  elif git -C "$GRAFT_REPO" merge-base --is-ancestor "$R_FOREIGN" "$R_CERT" >/dev/null 2>&1; then
    graft_attack=1
  fi
fi
if [ "$graft_shape" -eq 1 ] && [ "$graft_attack" -eq 1 ]; then
  ok "graft POSITIVE CONTROL: plain merge-base --is-ancestor in the grafted repo answers 0 — the attack really executes on this host"
  # And it survives --no-replace-objects, which is the half that makes the pins
  # insufficient and the move necessary.
  if git --no-replace-objects -C "$GRAFT_REPO" merge-base --is-ancestor "$R_FOREIGN" "$R_CERT" >/dev/null 2>&1; then
    ok "graft POSITIVE CONTROL: the graft SURVIVES --no-replace-objects (so the pins alone cannot close it)"
  else
    bad "graft POSITIVE CONTROL: --no-replace-objects defeated the graft on this host — CLAUDE.md's #3544 job-285 measurement does not reproduce, so the stated rationale needs re-checking"
  fi
  OUT=$(cd "$GRAFT_REPO" && PATH="$BIN:$PATH" MOCK_GH_FAIL=0 MOCK_GH_OUT="$R_CERT OPEN" \
    bash "$NEUTRAL_ASSERT" 2421 "$R_CERT" "$RFORFULL" "$RFORDELTA" 2>&1)
  RC=$?
  if [ "$RC" -ne 2 ]; then
    bad "graft: a planted graft manufactured a non-refusal (exit $RC, wanted 2) — the ancestry walk is reading the live repository again (job 355) (got: $OUT)"
  else
    ok "graft: the planted graft does NOT manufacture BOUND — the walk is isolated from it (exit 2)"
    case "$OUT" in
      *"is NOT on the certified sha's history"*)
        ok "graft: the refusal is the NOT-ANCESTOR verdict, not some other refusal firing first" ;;
      *) bad "graft: expected the NOT-ANCESTOR cause (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"anchor-ancestry: BOUND"*)
        bad "graft: the output claims BOUND — the graft was honoured (got: $OUT)" ;;
      *) ok "graft: no BOUND token appears anywhere in the grafted run's output" ;;
    esac
  fi
  # The ACCEPT direction must still work in the same grafted repository: the
  # isolation must not have broken ordinary ancestry, or arm 2 would "pass" for
  # the trivial reason that nothing is ever BOUND any more.
  #
  # IT IS ALSO A SECOND, INDEPENDENT DETECTOR OF THE REGRESSION, which is worth
  # knowing before reading its failure message. This graft REPLACES the certified
  # head's parent list, so under it the foreign commit becomes ancestral AND the
  # REAL anchor stops being so. A walk that reads the live repository therefore
  # fails BOTH arms — verified by reverting the walk: the refusal arm reported
  # exit 0 and this one reported the genuine anchor refused at exit 2.
  OUT=$(cd "$GRAFT_REPO" && PATH="$BIN:$PATH" MOCK_GH_FAIL=0 MOCK_GH_OUT="$R_CERT OPEN" \
    bash "$NEUTRAL_ASSERT" 2421 "$R_CERT" "$RANCFULL" "$RGOODDELTA" 2>&1)
  RC=$?
  if [ "$RC" -eq 0 ] && [ "${OUT#*anchor-ancestry: BOUND}" != "$OUT" ]; then
    ok "graft: NON-VACUITY — a genuine ancestor is still BOUND in the same grafted repository"
  else
    bad "graft: the genuine ancestor is no longer BOUND (exit $RC) — either the isolation broke ordinary ancestry, or the walk is reading the live repository and the graft masked the real parent (job 355). Arm 2 proves nothing either way (got: $OUT)"
  fi
else
  # DECLARED, not silent, and non-fatal — the same treatment the shallow arm gets.
  printf 'ARM NOT TAKEN: graft isolation (roborev job 355) — this host could not build the fixture\n'
  printf 'ARM NOT TAKEN: (a copy of the ancestry repo with $GIT_DIR/info/grafts making the foreign\n'
  printf 'ARM NOT TAKEN: commit a parent of the certified head) or the graft did not take effect, so\n'
  printf 'ARM NOT TAKEN: the POSITIVE CONTROL did not fire. Without it a passing refusal would prove\n'
  printf 'ARM NOT TAKEN: nothing, so BOTH arms are skipped and the graft-isolation property is\n'
  printf 'ARM NOT TAKEN: UNEXERCISED on this run. Non-fatal by design: git support for grafts is\n'
  printf 'ARM NOT TAKEN: deprecated and may be removed, which is a host property, not a defect here.\n'
  ok "graft: SKIPPED (positive control did not fire — see the ARM NOT TAKEN lines; arm UNEXERCISED, declared not silent)"
fi

# --- 44(g): THE SCRATCH'S ENVIRONMENT IS LOAD-BEARING (roborev job 358) ------
#
# The scratch isolates the walk from the LANE's repository state — but only if
# the environment cannot point git back at a grafted repository, or seed a graft
# into the scratch as it is created. Two routes, each with a POSITIVE CONTROL
# proving the attack executes (CLAUDE.md job 264: assert unreachability, with a
# control, or the green means nothing).
#
# Both arms reuse Case 44(f)'s GRAFT_REPO, so they need its fixture.

# --- 44(g)(1): an inherited GIT_DIR OVERRIDES `-C` --------------------------
if [ "$graft_shape" -eq 1 ] && [ "$graft_attack" -eq 1 ]; then
  # CONTROL: under this GIT_DIR a plain git call, given `-C` pointing at the
  # CLEAN repository, still answers about the GRAFTED one — and the grafted
  # ancestry answer comes with it.
  gd_control=0
  if [ "$(GIT_DIR="$GRAFT_REPO/.git" git -C "$ANC_REPO" rev-parse --git-dir 2>/dev/null)" = "$GRAFT_REPO/.git" ] &&
     GIT_DIR="$GRAFT_REPO/.git" git -C "$ANC_REPO" merge-base --is-ancestor "$R_FOREIGN" "$R_CERT" >/dev/null 2>&1; then
    gd_control=1
  fi
  if [ "$gd_control" -eq 1 ]; then
    ok "GIT_DIR POSITIVE CONTROL: an inherited GIT_DIR overrides -C and carries the grafted ancestry answer"
    OUT=$(cd "$ANC_REPO" && PATH="$BIN:$PATH" GIT_DIR="$GRAFT_REPO/.git" \
      MOCK_GH_FAIL=0 MOCK_GH_OUT="$R_CERT OPEN" \
      bash "$NEUTRAL_ASSERT" 2421 "$R_CERT" "$RFORFULL" "$RFORDELTA" 2>&1)
    RC=$?
    if [ "$RC" -ne 2 ]; then
      bad "GIT_DIR: an inherited GIT_DIR redirected the guard (exit $RC, wanted 2) — the env allowlist is not reaching every git call (job 358) (got: $OUT)"
    else
      ok "GIT_DIR: an inherited GIT_DIR cannot redirect the guard — still refused (exit 2)"
      case "$OUT" in
        *"anchor-ancestry: BOUND"*) bad "GIT_DIR: the run claims BOUND (got: $OUT)" ;;
        *) ok "GIT_DIR: no BOUND token appears in the redirected run's output" ;;
      esac
    fi
    # NON-VACUITY: a genuine ancestor must still be BOUND with that GIT_DIR set,
    # or the arm above would "pass" because nothing is ever BOUND any more.
    OUT=$(cd "$ANC_REPO" && PATH="$BIN:$PATH" GIT_DIR="$GRAFT_REPO/.git" \
      MOCK_GH_FAIL=0 MOCK_GH_OUT="$R_CERT OPEN" \
      bash "$NEUTRAL_ASSERT" 2421 "$R_CERT" "$RANCFULL" "$RGOODDELTA" 2>&1)
    RC=$?
    if [ "$RC" -eq 0 ] && [ "${OUT#*anchor-ancestry: BOUND}" != "$OUT" ]; then
      ok "GIT_DIR: NON-VACUITY — a genuine ancestor is still BOUND with GIT_DIR set"
    else
      bad "GIT_DIR: the genuine ancestor stopped being BOUND (exit $RC) — the refusal above proves nothing (got: $OUT)"
    fi
  else
    printf 'ARM NOT TAKEN: GIT_DIR redirect (job 358) — the control did not fire: this git did not let\n'
    printf 'ARM NOT TAKEN: an inherited GIT_DIR override -C, so a passing refusal would prove nothing.\n'
    ok "GIT_DIR: SKIPPED (positive control did not fire — arm UNEXERCISED, declared not silent)"
  fi
else
  printf 'ARM NOT TAKEN: GIT_DIR redirect (job 358) — depends on the graft fixture of 44(f), which was\n'
  printf 'ARM NOT TAKEN: not built on this host.\n'
  ok "GIT_DIR: SKIPPED (graft fixture unavailable — arm UNEXERCISED, declared not silent)"
fi

# --- 44(g)(2): a TEMPLATE seeds info/grafts INTO the scratch ----------------
# `git init --template=<dir>` and `GIT_TEMPLATE_DIR=<dir> git init` both copy a
# planted `info/grafts` into the NEW repository — so without an empty
# `--template=` and a cleared environment, the scratch is born grafted and the
# isolation buys nothing.
#
# WHAT THIS ARM CAN AND CANNOT SEPARATE, so nobody reads a green here as proof
# that both closures are individually necessary. The two are REDUNDANT against
# the route this arm constructs: removing ONLY the empty `--template=` still
# refuses (the allowlist cleared GIT_TEMPLATE_DIR), removing ONLY the `env -i`
# still refuses (the explicit flag beats the inherited variable), and removing
# BOTH lands the attack at exit 0 — which is what this arm detects. The flag's
# unique contribution, a config-FILE `init.templateDir`, is NOT exercised here:
# constructing it would mean writing to /etc/gitconfig.
TPL="$T/anchor-template"
tpl_shape=0
if [ "$anc_shape" -eq 1 ] && mkdir -p "$TPL/info" 2>/dev/null &&
   printf '%s %s\n' "$R_CERT" "$R_FOREIGN" >"$TPL/info/grafts" 2>/dev/null; then
  tpl_shape=1
fi
# CONTROL: build a scratch the NAIVE way (inherited GIT_TEMPLATE_DIR, no empty
# --template=) over the lane's objects, and show the walk there answers 0. This
# is the strongest available control: it proves the attack reaches the SCRATCH,
# not merely that templates copy files.
tpl_control=0
if [ "$tpl_shape" -eq 1 ]; then
  NAIVE="$T/anchor-naive-scratch"
  if GIT_TEMPLATE_DIR="$TPL" git init -q "$NAIVE/repo" >/dev/null 2>&1 &&
     [ -f "$NAIVE/repo/.git/info/grafts" ] &&
     GIT_ALTERNATE_OBJECT_DIRECTORIES="$ANC_REPO/.git/objects" \
       git -C "$NAIVE/repo" merge-base --is-ancestor "$R_FOREIGN" "$R_CERT" >/dev/null 2>&1; then
    tpl_control=1
  fi
fi
if [ "$tpl_control" -eq 1 ]; then
  ok "TEMPLATE POSITIVE CONTROL: GIT_TEMPLATE_DIR seeds info/grafts into a naive scratch AND the walk there answers 0"
  OUT=$(cd "$ANC_REPO" && PATH="$BIN:$PATH" GIT_TEMPLATE_DIR="$TPL" \
    MOCK_GH_FAIL=0 MOCK_GH_OUT="$R_CERT OPEN" \
    bash "$NEUTRAL_ASSERT" 2421 "$R_CERT" "$RFORFULL" "$RFORDELTA" 2>&1)
  RC=$?
  if [ "$RC" -ne 2 ]; then
    bad "TEMPLATE: a templated graft reached the scratch (exit $RC, wanted 2) — the empty --template= and/or the env allowlist is missing (job 358) (got: $OUT)"
  else
    ok "TEMPLATE: a templated graft cannot reach the scratch — still refused (exit 2)"
    case "$OUT" in
      *"anchor-ancestry: BOUND"*) bad "TEMPLATE: the run claims BOUND (got: $OUT)" ;;
      *) ok "TEMPLATE: no BOUND token appears in the templated run's output" ;;
    esac
  fi
  OUT=$(cd "$ANC_REPO" && PATH="$BIN:$PATH" GIT_TEMPLATE_DIR="$TPL" \
    MOCK_GH_FAIL=0 MOCK_GH_OUT="$R_CERT OPEN" \
    bash "$NEUTRAL_ASSERT" 2421 "$R_CERT" "$RANCFULL" "$RGOODDELTA" 2>&1)
  RC=$?
  if [ "$RC" -eq 0 ] && [ "${OUT#*anchor-ancestry: BOUND}" != "$OUT" ]; then
    ok "TEMPLATE: NON-VACUITY — a genuine ancestor is still BOUND with GIT_TEMPLATE_DIR set"
  else
    bad "TEMPLATE: the genuine ancestor stopped being BOUND (exit $RC) — the refusal above proves nothing (got: $OUT)"
  fi
else
  printf 'ARM NOT TAKEN: template-seeded graft (job 358) — this host could not build the fixture, or a\n'
  printf 'ARM NOT TAKEN: naive scratch built with GIT_TEMPLATE_DIR did NOT inherit the graft, so the\n'
  printf 'ARM NOT TAKEN: positive control did not fire and a passing refusal would prove nothing.\n'
  ok "TEMPLATE: SKIPPED (positive control did not fire — arm UNEXERCISED, declared not silent)"
fi

# --- 44(h): NO BOUNDED RUNNER -> REFUSE (job 370 finding 1, REVERSING job 358) --
#
# THIS ARM ASSERTS THE OPPOSITE OF WHAT IT USED TO, and the reversal is recorded
# here as well as in the shipped header, because the old expectation was
# reasonable and wrong. It used to require that a box with no `timeout`/`gtimeout`
# still MERGED, with the degradation affirmed on the evidence line, on the ground
# that a hang is a liveness failure and cannot manufacture a false pass.
#
# What that missed: A HANG IN THIS GUARD BLOCKS THE MERGE ANYWAY. The real
# comparison is hang-forever-with-no-diagnosis vs refuse-now-with-a-named-remedy —
# same outcome for the merge, and the refusal strictly dominates. So the arm now
# requires exit 3 with `ANCHOR-UNVERIFIABLE`, AND requires the CAUSE to name the
# absent runner: a refusal for some other UNVERIFIABLE reason (an absent object,
# a shallow history) would satisfy the exit code while proving nothing about
# bounding.
NOTO="$T/bin-no-timeout"
mkdir -p "$NOTO"
noto_ok=1
# `dirname`/`basename` are the script's own helpers (advisory-path resolution and
# the usage banner), not the ancestry check's — they are here so the run reaches
# the check at all rather than dying at line 1.
# `bash` is here for the gh MOCK's own `#!/usr/bin/env bash` shebang, not for the
# script under test; without it the mock cannot start and the case fails on a
# GH-FAILURE that says nothing about bounding.
for _tool in git awk tr env mktemp rm dirname basename bash; do
  _tp=$(command -v "$_tool" 2>/dev/null) || _tp=""
  if [ -n "$_tp" ]; then ln -sf "$_tp" "$NOTO/$_tool"; else noto_ok=0; fi
done
cp "$BIN/gh" "$NOTO/gh" 2>/dev/null || noto_ok=0
# NON-VACUITY: the fixture PATH must really have NEITHER runner on it.
if [ "$noto_ok" -eq 1 ]; then
  for _t in timeout gtimeout; do
    if PATH="$NOTO" command -v "$_t" >/dev/null 2>&1; then noto_ok=0; fi
  done
fi
if [ "$noto_ok" -ne 1 ]; then
  printf 'ARM NOT TAKEN: no-bounded-runner refusal (job 370) — could not build a PATH holding git/awk/\n'
  printf 'ARM NOT TAKEN: tr/env/mktemp/rm/gh but NEITHER timeout nor gtimeout.\n'
  ok "no-runner: SKIPPED (fixture unbuildable — arm UNEXERCISED, declared not silent)"
else
  ok "no-runner fixture: the fixture PATH has git but NEITHER timeout nor gtimeout"
  OUT=$(cd "$ANC_REPO" && PATH="$NOTO" MOCK_GH_FAIL=0 MOCK_GH_OUT="$R_CERT OPEN" \
    "${BASH:-/bin/bash}" "$NEUTRAL_ASSERT" 2421 "$R_CERT" "$RANCFULL" "$RGOODDELTA" 2>&1)
  RC=$?
  if [ "$RC" -ne 3 ]; then
    bad "no-runner: with no bounded runner the check must REFUSE at exit 3, not run unbounded (exit $RC) (got: $OUT)"
  else
    ok "no-runner: with no bounded runner the check REFUSES at exit 3 rather than running unbounded"
    case "$OUT" in
      *"PREMERGE: ANCHOR-UNVERIFIABLE"*)
        ok "no-runner: the refusal carries the ANCHOR-UNVERIFIABLE marker" ;;
      *) bad "no-runner: expected the ANCHOR-UNVERIFIABLE marker (got: $OUT)" ;;
    esac
    # THE REASON MUST BE THE ABSENT RUNNER. Without this the arm would pass on any
    # other UNVERIFIABLE cause and prove nothing about bounding.
    case "$OUT" in
      *"cannot be BOUNDED"*)
        ok "no-runner: the cause names the ABSENT RUNNER, not some other UNVERIFIABLE reason" ;;
      *) bad "no-runner: the cause must name the unbounded-reads problem (got: $OUT)" ;;
    esac
    case "$OUT" in
      *"is not present in this repository"* | *"NOT PROVEN COMPLETE"* | *"not inside a git work tree"* | *"timed out after"*)
        bad "no-runner: refused for a DIFFERENT cause than the missing runner — the arm proves nothing (got: $OUT)" ;;
      *) ok "no-runner: the refusal is NOT an absent-object, shallow, work-tree or timeout cause" ;;
    esac
    case "$OUT" in
      *"install GNU coreutils"*)
        ok "no-runner: the refusal names a ONE-COMMAND remedy (install coreutils / gtimeout)" ;;
      *) bad "no-runner: the refusal must name the remedy (got: $OUT)" ;;
    esac
    # And it must not claim a bound, nor emit an ancestry verdict it never reached.
    case "$OUT" in
      *"anchor-reads: bounded-"*)
        bad "no-runner: the output claims the reads were bounded on a box with no runner (got: $OUT)" ;;
      *) ok "no-runner: the output does NOT claim a bound it did not have" ;;
    esac
    case "$OUT" in
      *"anchor-ancestry: BOUND"*)
        bad "no-runner: the output claims an ancestry verdict it refused before reaching (got: $OUT)" ;;
      *) ok "no-runner: no ancestry verdict is claimed — the check refused before the walk" ;;
    esac
  fi
fi

# --- 44(i): A FORGED commit-graph (roborev job 361) --------------------------
#
# `objects/info/commit-graph` is reachable through the alternate, is NOT
# content-addressed, and git TRUSTS its recorded parent edges — so it is a
# parent-edge source the scratch isolation does not by itself remove. The control
# is `-c core.commitGraph=false` on every isolated read.
#
# WHAT THIS ARM CAN HONESTLY CLAIM, WHICH IS LESS THAN THE OTHER TWO. Measured on
# this suite's own fixture (see the forge below): the graph IS consulted and IS
# trusted — `rev-list --parents` reports the FORGED parent and `-c
# core.commitGraph=false` reports the real one — but `merge-base --is-ancestor`,
# which is the call this guard makes, answered "no" in BOTH cases on git 2.43.0.
# So the EXPLOIT AGAINST THIS CALL does not reproduce here, and an arm shaped like
# 44(f)/44(g) ("plain merge-base answers 0, the guard still refuses") CANNOT be
# built on this host. Pretending otherwise would be a green that proves nothing.
#
# What is asserted instead, split by what each half can actually establish:
#   (1) BEHAVIOURAL, and it is a real positive control for the MECHANISM: the
#       forged graph changes `rev-list`'s answer, and the flag changes it back.
#       That proves the fixture is genuinely forged AND that git trusts the graph.
#   (2) BEHAVIOURAL: the guard refuses the foreign pair in the forged repository,
#       and still BINDs a genuine ancestor there. NOT load-bearing on this git —
#       it would pass without the flag — and labelled as such.
#   (3) STRUCTURAL, labelled: the isolated reads carry `-c core.commitGraph=false`
#       from the single options array. This is the assertion that actually pins
#       the control on a host where the behavioural one cannot, and it is declared
#       structural rather than dressed up as behavioural (the convention CLAUDE.md
#       records for roborev job 279's ownership invariant).
CG_REPO="$T/ancestry-cg-repo"
cg_shape=0
cg_forged=0
if [ "$anc_shape" -eq 1 ] && cp -a "$ANC_REPO" "$CG_REPO" 2>/dev/null &&
   git -C "$CG_REPO" commit-graph write --reachable >/dev/null 2>&1 &&
   [ -f "$CG_REPO/.git/objects/info/commit-graph" ]; then
  cg_shape=1
fi
if [ "$cg_shape" -eq 1 ]; then
  # THE FORGE: patch the CDAT record's first parent slot for the CERTIFIED commit
  # to name the FOREIGN commit, then recompute the file's trailing checksum. git
  # writes the graph read-only, which is not a control (a peer runs as the same
  # user), so it is chmod'd first — exactly as a planter would.
  chmod u+w "$CG_REPO/.git/objects/info/commit-graph" 2>/dev/null
  if python3 - "$CG_REPO/.git/objects/info/commit-graph" "$R_CERT" "$R_FOREIGN" <<'FORGE' >/dev/null 2>&1
import sys, struct, hashlib
path, victim, newparent = sys.argv[1], sys.argv[2], sys.argv[3]
d = bytearray(open(path, 'rb').read())
if d[0:4] != b'CGPH' or d[4] != 1 or d[5] != 1:
    raise SystemExit(1)                      # not a v1/SHA-1 graph: unsupported here
nchunks = d[6]
off, entries = 8, []
for _ in range(nchunks + 1):
    entries.append((bytes(d[off:off + 4]), struct.unpack('>Q', d[off + 4:off + 12])[0]))
    off += 12
chunks = {entries[i][0]: (entries[i][1], entries[i + 1][1]) for i in range(nchunks)}
if b'OIDL' not in chunks or b'CDAT' not in chunks:
    raise SystemExit(1)
oidl, cdat = chunks[b'OIDL'], chunks[b'CDAT']
n = (oidl[1] - oidl[0]) // 20
oids = [d[oidl[0] + i * 20:oidl[0] + i * 20 + 20].hex() for i in range(n)]
if victim not in oids or newparent not in oids:
    raise SystemExit(1)
rec = cdat[0] + oids.index(victim) * 36
d[rec + 20:rec + 24] = struct.pack('>I', oids.index(newparent))
d[len(d) - 20:] = hashlib.sha1(bytes(d[:len(d) - 20])).digest()
open(path, 'wb').write(bytes(d))
FORGE
  then
    cg_forged=1
  fi
fi
# (1) THE MECHANISM CONTROL — the forge must actually change what git believes.
cg_control=0
if [ "$cg_forged" -eq 1 ]; then
  cg_default=$(git -C "$CG_REPO" rev-list --parents -1 "$R_CERT" 2>/dev/null)
  cg_off=$(git -C "$CG_REPO" -c core.commitGraph=false rev-list --parents -1 "$R_CERT" 2>/dev/null)
  if [ "${cg_default#*"$R_FOREIGN"}" != "$cg_default" ] &&
     [ "${cg_off#*"$R_FOREIGN"}" = "$cg_off" ]; then
    cg_control=1
  fi
fi
if [ "$cg_control" -eq 1 ]; then
  ok "commit-graph POSITIVE CONTROL: the forged graph makes git report a FOREIGN parent, and -c core.commitGraph=false reports the real one"
  # The reachability half of the same control, which is the property an ancestry
  # walk would consume.
  if [ "$(git -C "$CG_REPO" rev-list "$R_CERT" | grep -c "$R_FOREIGN")" = 1 ] &&
     [ "$(git -C "$CG_REPO" -c core.commitGraph=false rev-list "$R_CERT" | grep -c "$R_FOREIGN")" = 0 ]; then
    ok "commit-graph POSITIVE CONTROL: the forged graph makes the FOREIGN commit reachable from the certified head (and the flag removes it)"
  else
    bad "commit-graph POSITIVE CONTROL: the forge did not change reachability — the fixture is not the shape this arm claims"
  fi
  # (2) The guard's behaviour in that repository. Declared NOT load-bearing on a
  # git whose --is-ancestor ignores the forged edge; it is here so a git that
  # DOES honour it would red this arm rather than merge.
  OUT=$(cd "$CG_REPO" && PATH="$BIN:$PATH" MOCK_GH_FAIL=0 MOCK_GH_OUT="$R_CERT OPEN" \
    bash "$NEUTRAL_ASSERT" 2421 "$R_CERT" "$RFORFULL" "$RFORDELTA" 2>&1)
  RC=$?
  if [ "$RC" -ne 2 ]; then
    bad "commit-graph: a forged commit-graph produced a non-refusal (exit $RC, wanted 2) — this git's --is-ancestor DOES honour the forged edge and the control is not reaching it (job 361) (got: $OUT)"
  else
    ok "commit-graph: the guard refuses the foreign pair in the forged repository (exit 2)"
  fi
  OUT=$(cd "$CG_REPO" && PATH="$BIN:$PATH" MOCK_GH_FAIL=0 MOCK_GH_OUT="$R_CERT OPEN" \
    bash "$NEUTRAL_ASSERT" 2421 "$R_CERT" "$RANCFULL" "$RGOODDELTA" 2>&1)
  RC=$?
  if [ "$RC" -eq 0 ] && [ "${OUT#*anchor-ancestry: BOUND}" != "$OUT" ]; then
    ok "commit-graph: NON-VACUITY — a genuine ancestor is still BOUND in the forged repository"
  else
    bad "commit-graph: the genuine ancestor stopped being BOUND (exit $RC) — the refusal above proves nothing (got: $OUT)"
  fi
  printf 'ARM NOT TAKEN: commit-graph EXPLOIT arm (job 361) — the mechanism control above FIRED, but on\n'
  printf 'ARM NOT TAKEN: this git `merge-base --is-ancestor` answered "no" for the forged edge WITH and\n'
  printf 'ARM NOT TAKEN: WITHOUT the flag, so no arm on this host can show the flag CHANGING this\n'
  printf 'ARM NOT TAKEN: guard-s verdict. The control is pinned STRUCTURALLY below instead.\n'
else
  printf 'ARM NOT TAKEN: forged commit-graph (job 361) — this host could not build the fixture (no\n'
  printf 'ARM NOT TAKEN: commit-graph support, a non-v1/SHA-1 graph format, or the forge did not change\n'
  printf 'ARM NOT TAKEN: what git believes), so no behavioural claim is made about the graph route.\n'
  printf 'ARM NOT TAKEN: The structural assertion below still applies.\n'
  ok "commit-graph: SKIPPED (mechanism control did not fire — arm UNEXERCISED, declared not silent)"
fi

# (3) THE STRUCTURAL ASSERTION, labelled as such. On a git whose --is-ancestor
# ignores a forged graph this is the ONLY thing pinning the control, so it is not
# optional decoration: it asserts the shipped script disables the graph, from the
# SINGLE options array, so a future option cannot reach some call sites and miss
# others (roborev job 276's failure mode).
if grep -q -F -- 'ANCHOR_GIT_OPTS=(--no-replace-objects -c core.commitGraph=false)' "$ASSERT"; then
  ok "commit-graph (STRUCTURAL): the shipped script disables core.commitGraph in the ONE options array"
else
  bad "commit-graph (STRUCTURAL): the shipped script no longer disables core.commitGraph in ANCHOR_GIT_OPTS (job 361)"
fi
# ...and that every git invocation in the check consumes that array rather than
# calling `git` bare. Counted, not eyeballed: this is the invariant that job 276
# says has to reach the sites a later change adds.
# Scans `_anchor_bounded` TOO (job 382): it is now where the raw command actually
# executes, so a git call added there would bypass the options array. COMMENTS are
# skipped — the first version matched the phrase "not inside a git work tree" in a
# comment and reported a bypass that did not exist.
anchor_bare=$(awk '
  /^_anchor_(run|git|bounded)\(\) \{/ { inf = 1 }
  inf && /^\}/ { inf = 0 }
  inf && /^[[:space:]]*#/ { next }
  inf && /(^|[^_[:alnum:]])git / && !/ANCHOR_GIT_OPTS/ { c++ }
  END { print c + 0 }
' "$ASSERT")
if [ "$anchor_bare" = 0 ]; then
  ok "commit-graph (STRUCTURAL): every git invocation in the isolated wrappers passes ANCHOR_GIT_OPTS"
else
  bad "commit-graph (STRUCTURAL): $anchor_bare git invocation(s) in the isolated wrappers bypass ANCHOR_GIT_OPTS — an option would reach some sites and miss others (job 276)"
fi

# --- 44(j): THE BOUNDARY IS DECLARED ON EVERY SUCCESS LINE (job 361) ---------
# Three rounds found three routes into one mechanism, so the guard now DECLARES
# what it cannot prove instead of implying a closure it does not deliver: the
# ancestry verdict is derived from a shared object store that is TRUSTED, not
# VERIFIED, and that hazard belongs to #3746.
if run_anc 0 "Case B success declares the ancestry provenance boundary" \
  2421 "$R_CERT" "$RANCFULL" "$RGOODDELTA"; then
  case "$OUT" in
    *"TRUSTED, not verified (#3746)"*)
      ok "boundary: the DELTA-RECERT line declares the store is TRUSTED, not verified, naming #3746" ;;
    *) bad "boundary: the success line must declare the trusted-store boundary (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"SHARED object store"*)
      ok "boundary: the declaration names WHAT is trusted (this box's shared object store)" ;;
    *) bad "boundary: the declaration must name the shared object store (got: $OUT)" ;;
  esac
  # WIDENED AT job 390: the SCRATCH namespace is trusted for the SAME reason — a
  # same-UID peer can write into it between `git init` and the walk, reproducing
  # round 1's graft attack inside the thing built to prevent it. There is no
  # permission boundary on a one-user fleet, so the claim is narrowed rather than
  # the hole patched, and this arm stops the scratch half being quietly dropped.
  case "$OUT" in
    *"SCRATCH namespace"*)
      ok "boundary: the declaration ALSO names the scratch namespace (job 390)" ;;
    *) bad "boundary: the declaration must name the SCRATCH namespace too (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"NOT a same-UID peer"*)
      ok "boundary: the declaration states what it does NOT close (a same-UID peer)" ;;
    *) bad "boundary: the declaration must say it does not close a same-UID peer (got: $OUT)" ;;
  esac
  # It must not have displaced the tokens a reader greps for.
  case "$OUT" in
    *"anchor-ancestry: BOUND"*) ok "boundary: the ancestry token is unchanged beside the declaration" ;;
    *) bad "boundary: the declaration must not displace anchor-ancestry: BOUND (got: $OUT)" ;;
  esac
fi
# ONE renderer, never per-arm: two spellings of a boundary drift, and a drifted
# boundary is worse than none. Asserted structurally, for the same reason (3) is.
boundary_sites=$(grep -c -F -- 'TRUSTED, not verified (#3746)' "$ASSERT")
if [ "$boundary_sites" = 1 ]; then
  ok "boundary (STRUCTURAL): the declaration text exists in exactly ONE place in the shipped script"
else
  bad "boundary (STRUCTURAL): the declaration appears $boundary_sites times — it must be ONE constant consumed by the ONE renderer"
fi

# --- 44(k): AN ACTUALLY HUNG READ (roborev job 364) --------------------------
#
# The arms above verify runner SELECTION and the `anchor-reads:` label; none of
# them ever hangs a read, so the timeout handling — and specifically the exit
# 124 / 137 recognition — could regress with the suite still green. These two
# arms hang the ancestry call for real.
#
# CONSTRUCTION, and the two couplings a future reader could break:
#   * A PATH-SHIM `git` that matches on ARGV and blocks ONLY on
#     `merge-base --is-ancestor`, passing everything else through to the real
#     binary. Discovery reads and the object reads must keep working or the
#     fixture never reaches the code under test.
#   * THE SHIM IS REACHABLE ONLY BECAUSE `PATH` IS IN THE `env -i` ALLOWLIST.
#     That is a non-obvious coupling: tightening the allowlist to drop PATH
#     would make these arms silently test nothing (git would not be found at
#     all, so they would fail loudly — but a future variant that hard-coded an
#     absolute git path would make them vacuous instead).
#   * THE TIMEOUT IS SHORTENED BY SUBSTITUTING THE ARTIFACT, never a seam: a
#     scratch copy of the shipped script with ONLY the two constants rewritten
#     (60s/5s -> 2s/1s), asserted to have taken. A settable timeout would be one
#     more thing a real invoker could set (#3312), and the runner is deliberately
#     NOT refactored to make this easier to test.
#
# SAFETY, because a test that leaks processes is its own hazard (CLAUDE.md job
# 279: a bounded runner's ownership ends at REAP, not at exit, and a
# pattern-based kill on this fleet has already killed a peer lane's gate). Each
# arm asserts NO stray process survives, matched on an EXACT argv sentinel that
# no other process on the box can produce — never a `pkill -f` pattern.
TOFLOW="$T/flow-timeout"
# THE SENTINEL MUST BE UNIQUE PER RUN, AND A FIXED STRING WAS A REAL BLOCKER.
# It used to be a FIXED `sleep <constant>` argv, while the leak census and the
# cleanup scanned the WHOLE BOX (`ps -eo …`). Measured, both directions: a suite run
# CONCURRENTLY with another run of itself reported `293 passed, 1 FAILED —
# 1 stray process(es) matching the sentinel survived`, and the same suite run
# alone immediately after reported `294 passed, 0 failed` with no strays on the
# box. Two consequences, the second far worse:
#   1. FALSE RED ACROSS LANES. This suite runs in `tooling-tests`, i.e. in EVERY
#      full gate, and this fleet reports `max-concurrency=3` — so two concurrent
#      gates red each other. A flaky merge gate, fleet-wide.
#   2. THE CLEANUP KILLED THE PEER'S PROCESS. The kill site iterated every
#      process on the box and SIGKILLed any whose argv equalled the sentinel.
#      Its comment claimed exactness made that safe "because it cannot be a peer
#      lane's gate" — true, and worthless: it is exactly a peer lane's copy of
#      THIS SUITE, because the argv was not unique per run. Exact-but-SHARED is
#      job 279's incident wearing a safer-looking comment.
# `$T` is already run-unique (`mktemp -d …premerge-assert-test.XXXXXX`), so the
# sentinel is derived from it: the census can then only ever match this run's own
# processes, and the cleanup can only ever kill its own.
#
# WHY A DURATION AND NOT A SUFFIX: `sleep` takes numeric operands only, so a
# textual tag cannot be appended to its argv. The uniqueness therefore lives in
# the NUMBER. `printf '9%09d'` keeps it a fixed 10 digits with a floor of ~285
# years, so a small token can never yield a sleep that exits during the run and
# makes the leak check pass for the wrong reason.
#
# THE TOKEN IS THE PID, AND THAT IS A GUARANTEE RATHER THAN A PROBABILITY. The
# first version hashed `$T` with `cksum … % 1000000000`, which is LOSSY: two
# concurrent runs could collide, and a collision hands the whole-box cleanup a
# peer's process to SIGKILL. A PID IS UNIQUE AMONG LIVE PROCESSES, and live
# processes are exactly what the census inspects — so two simultaneous runs
# cannot collide, by construction. The cksum path is GONE rather than kept as a
# fallback: a second derivation is a second thing to be wrong, and this one needs
# no external tool.
#
# The residual, stated: a stale sleep left by a DEAD earlier run whose pid was
# later recycled to this one would be seen as this run's leak. That direction is
# benign — the stray really is garbage and killing it really is correct cleanup —
# and it can only arise if an earlier run leaked, which these arms now assert
# against.
to_derive_sentinel() {
  local tok="$1"
  case "$tok" in
    ''|*[!0-9]*) printf '%s\n' "INVALID-TOKEN"; return 0 ;;
  esac
  printf 'sleep 120.%s\n' "$(printf '%09d' "$(( tok % 1000000000 ))")"
}
TOSENTINEL=$(to_derive_sentinel "$$")
to_shape=0
# A REAL bounding runner is REQUIRED for these arms. Without one the $BIN shim
# discards the bound (see its own comment) and a hung read would hang FOREVER —
# so the absence of a runner is a declared not-taken, never an attempt.
if [ -n "$REAL_TO" ] && mkdir -p "$TOFLOW" && cp "$ASSERT" "$TOFLOW/premerge-assert.sh"; then
  printf '%s\n' "$NEUTRAL_ADV" >"$TOFLOW/base-staleness.sh"
  chmod +x "$TOFLOW/base-staleness.sh"
  # ONLY the two constants change. sed on the exact assignment lines, then verify.
  sed -e 's/^ADVISORY_TIMEOUT_SECS=60$/ADVISORY_TIMEOUT_SECS=2/' \
      -e 's/^ADVISORY_KILL_GRACE=5$/ADVISORY_KILL_GRACE=1/' \
      "$TOFLOW/premerge-assert.sh" >"$TOFLOW/x" && mv "$TOFLOW/x" "$TOFLOW/premerge-assert.sh"
  if grep -q -x -F 'ADVISORY_TIMEOUT_SECS=2' "$TOFLOW/premerge-assert.sh" &&
     grep -q -x -F 'ADVISORY_KILL_GRACE=1' "$TOFLOW/premerge-assert.sh" &&
     ! grep -q -x -F 'ADVISORY_TIMEOUT_SECS=60' "$TOFLOW/premerge-assert.sh" &&
     ! grep -q -x -F 'ADVISORY_KILL_GRACE=5' "$TOFLOW/premerge-assert.sh"; then
    to_shape=1
  fi
fi
if [ "$to_shape" -eq 1 ]; then
  ok "hung-read fixture: a scratch copy with the bound shortened to 2s+1s (constants only, mutation verified)"
else
  printf 'ARM NOT TAKEN: hung ancestry read (job 364) — no real timeout/gtimeout runner on this host, or\n'
  printf 'ARM NOT TAKEN: the shortened-bound scratch copy could not be built. BOTH arms are skipped\n'
  printf 'ARM NOT TAKEN: DELIBERATELY: without a real runner the $BIN shim discards the bound and a hung\n'
  printf 'ARM NOT TAKEN: read would hang forever, so attempting the arm is worse than declaring it. The\n'
  printf 'ARM NOT TAKEN: exit 124/137 recognition is UNEXERCISED on this run.\n'
  ok "hung-read: SKIPPED (no bounding runner / fixture unbuildable — arms UNEXERCISED, declared not silent)"
fi

# THE LEAK ADJUDICATION USED TO LIVE HERE (`to_leaks`, `to_pid_verdict`,
# `to_check_pid`) AND IS DELETED — see the cleanup block at the top of this file
# for why. What the arms below still assert is the SHIPPED behaviour rounds 4 and
# 7 asked for: exit 3, the ANCHOR-UNVERIFIABLE marker, and a TIMEOUT cause
# distinct from every other cause, each with a runner-exit control. What they no
# longer assert is WHO leaked and WHY.

# to_run_arm <label> <shim-dir> <expected-runner-rc>
#
# THE THIRD ARGUMENT IS WHAT KEEPS THE TWO ARMS DISTINCT. `_anchor_timed_out`
# accepts BOTH 124 and 137, so without a control each arm would pass whichever
# path it actually took — and "we tested the escalation" would be a claim nothing
# established. So each shim is first run UNDER THE RUNNER DIRECTLY and its exit
# code asserted: 124 for the TERM shim, 137 for the TERM-ignoring one.
# Params 5 and 6 were added for roborev job 374: the same arms must be able to
# hang a DISCOVERY call, whose expected CAUSE differs from the ancestry walk's.
# Parameterising the proven runner is deliberate reuse — a second construction is
# where every defect in this scaffolding has come from.
#   $5 want_cause   the timeout cause substring this arm must see
#                   (default: the ancestry walk's)
#   $6 control_args the argv the CONTROL invokes, which must be the one the shim
#                   triggers on (default: the ancestry walk's)
#   $7 control_bin  the BINARY the control invokes (default `git`). Added for job
#                   395, whose shim intercepts `sh` — the canonicalisation helper
#                   runs `sh -c 'cd -- "$1" && pwd -P'`, not git. Parameterising
#                   the proven runner beats a second construction, which is where
#                   every defect in this scaffolding came from.
to_run_arm() {
  local label="$1" shimdir="$2" want_rc="$3" pidfile="$4"
  local want_cause="${5:-the ancestry walk timed out after}"
  local control_args="${6:-merge-base --is-ancestor $R_CERT $R_CERT}"
  local control_bin="${7:-git}"
  local out rc leaks crc=0
  rm -f "$pidfile"
  # shellcheck disable=SC2086  # intentional word-split of the control argv list
  PATH="$shimdir:$PATH" "$REAL_TO" --kill-after=1 2 \
    "$control_bin" $control_args >/dev/null 2>&1 || crc=$?
  if [ "$crc" = "$want_rc" ]; then
    ok "hung-read ($label): CONTROL — the shim really produces runner exit $want_rc"
  else
    bad "hung-read ($label): CONTROL — expected runner exit $want_rc, got $crc; this arm is not exercising the path it names"
  fi
  out=$(cd "$ANC_REPO" && PATH="$shimdir:$BIN:$PATH" MOCK_GH_FAIL=0 MOCK_GH_OUT="$R_CERT OPEN" \
    bash "$TOFLOW/premerge-assert.sh" 2421 "$R_CERT" "$RANCFULL" "$RGOODDELTA" 2>&1)
  rc=$?
  if [ "$rc" -ne 3 ]; then
    bad "hung-read ($label): a hung bounded call must be exit 3 (got $rc: $out)"
  else
    ok "hung-read ($label): a hung bounded call is exit 3, not a hang and not a pass"
    case "$out" in
      *"PREMERGE: ANCHOR-UNVERIFIABLE"*)
        ok "hung-read ($label): carries the ANCHOR-UNVERIFIABLE marker" ;;
      *) bad "hung-read ($label): expected the ANCHOR-UNVERIFIABLE marker (got: $out)" ;;
    esac
    case "$out" in
      *"$want_cause"*)
        ok "hung-read ($label): the cause names the TIMEOUT of the call that hung, distinctly from the other UNVERIFIABLE causes" ;;
      *) bad "hung-read ($label): the cause must name '$want_cause' (got: $out)" ;;
    esac
    # Distinctness in both directions: a timeout must never BORROW another
    # cause's remedy — that is job 374's whole subject. The forbidden set covers
    # every non-timeout UNVERIFIABLE cause this script can print.
    case "$out" in
      *"is not present in this repository"* | *"NOT PROVEN COMPLETE"* \
        | *"is NOT on the certified sha's history"* | *"not inside a git work tree"* \
        | *"could not be resolved"* | *"scratch root"* | *"could not initialise"*)
        bad "hung-read ($label): a timeout was misreported as another cause — the operator would get the WRONG remedy (got: $out)" ;;
      *) ok "hung-read ($label): a timeout borrows NO other cause's remedy (not work-tree, TMPDIR, absent-object, shallow or NOT-ANCESTOR)" ;;
    esac
    # AND THE TIMEOUT MESSAGE MUST NAME ITS OWN SUBJECT (roborev job 410). The
    # shared diagnostic used to assert "this is NOT about your TMPDIR" and send the
    # operator to the OBJECT STORE — while being the destination for timeouts that
    # occurred canonicalising TMPDIR, running `mktemp`, or canonicalising the
    # scratch. Both halves are asserted here rather than per-arm, so every timeout
    # arm covers the class: a SUBJECT line must be present, and the old
    # object-store-only remedy must be gone.
    case "$out" in
      *"SUBJECT: the timed-out operation was acting on: "*)
        ok "hung-read ($label): the timeout names the SUBJECT it was acting on" ;;
      *) bad "hung-read ($label): the timeout must name its subject, or a TMPDIR-side hang reads as an object-store problem (got: $out)" ;;
    esac
    case "$out" in
      *"check the object store under this repository"*)
        bad "hung-read ($label): the refusal still sends the operator to the OBJECT STORE unconditionally (job 410) (got: $out)" ;;
      *) ok "hung-read ($label): the remedy points at the named subject, not unconditionally at the object store" ;;
    esac
  fi
  # NO LEAK ASSERTION HERE ANY MORE (job 381). Reaping is covered by the
  # unconditional run-scoped sweep at suite exit, which makes no claim about it.
  # DECLARED, not buried: see the DECLARED LOSS notice printed below this arm.
}

if [ "$to_shape" -eq 1 ]; then
  REALGIT=$(command -v git 2>/dev/null) || REALGIT=""
  if [ -z "$REALGIT" ]; then
    printf 'ARM NOT TAKEN: hung ancestry read (job 364) — the real git binary could not be resolved, so a\n'
    printf 'ARM NOT TAKEN: pass-through shim cannot be built.\n'
    ok "hung-read: SKIPPED (real git unresolvable — arms UNEXERCISED, declared not silent)"
  else
    # ARM 1 — the TERM path (exit 124). The shim `exec`s into the sentinel sleep,
    # so it BECOMES the sleep and dies on the runner's SIGTERM. (Without `exec`,
    # bash defers the trap until its foreground child finishes, which would test
    # the escalation path by accident.)
    TSH="$T/bin-git-hang-term"
    mkdir -p "$TSH"
    TPID="$T/hang-term.pid"
    # The pid is recorded BEFORE the exec, and `exec` PRESERVES the pid — so the
    # recorded value names the sentinel sleep itself, not a vanished parent.
    cat >"$TSH/git" <<TERMSHIM
#!/usr/bin/env bash
for a in "\$@"; do
  if [ "\$a" = "--is-ancestor" ]; then
    printf '%s\n' "\$\$" >"$TPID"
    exec $TOSENTINEL
  fi
done
exec "$REALGIT" "\$@"
TERMSHIM
    chmod +x "$TSH/git"
    to_run_arm "TERM path / exit 124" "$TSH" 124 "$TPID"

    # ARM 2 — the KILL-after-grace path (exit 137). The shim IGNORES TERM, so the
    # runner must escalate to SIGKILL.
    #
    # NO FIFO ANY MORE (roborev job 388). It used to block on opening a writer-less
    # FIFO: uninterruptible, which was the point, but also UNBOUNDED — a leaked
    # shim blocked forever and needed hunting, which is exactly what the deleted
    # sweep existed for. It now counts BOUNDED 1s sleeps instead. The shim itself
    # still ignores TERM (so the escalation fires), each `sleep 1` child is
    # trivially short, and the whole thing self-terminates in ~120s if leaked.
    # Measured on this box: runner rc 137 in 3s, the shim survives TERM, and a
    # leaked one goes away on its own. The mkfifo not-taken branch went with the
    # FIFO — one fewer host-dependent skip.
    KSH="$T/bin-git-hang-kill"
    mkdir -p "$KSH"
    KPID="$T/hang-kill.pid"
    cat >"$KSH/git" <<KILLSHIM
#!/usr/bin/env bash
for a in "\$@"; do
  if [ "\$a" = "--is-ancestor" ]; then
    printf '%s\\n' "\$\$" >"$KPID"
    trap '' TERM
    _n=0
    while [ "\$_n" -lt 120 ]; do sleep 1; _n=\$((_n + 1)); done
    exit 0
  fi
done
exec "$REALGIT" "\$@"
KILLSHIM
    chmod +x "$KSH/git"
    to_run_arm "KILL after grace / exit 137" "$KSH" 137 "$KPID"

    # ARM 3 — A HUNG *DISCOVERY* CALL (roborev job 374). Several bounded calls
    # used to discard their status via `|| true` / an empty-value fallback /
    # `if !`, so a timeout during repository discovery was reported as "not
    # inside a git work tree" or an unusable TMPDIR — sending the operator to
    # check their cwd when the real answer is a stalled filesystem. The arms
    # above only ever hung `merge-base`, so nothing caught it.
    #
    # SAME TEMPLATE AS ARM 1, different trigger argv: `--show-toplevel`, which
    # `_anchor_build_scratch` calls. `--git-dir` runs BEFORE it and passes
    # through to the real binary, so the fixture proves the status is propagated
    # from a call that is not the walk.
    DSH="$T/bin-git-hang-discovery"
    mkdir -p "$DSH"
    DPID="$T/hang-discovery.pid"
    cat >"$DSH/git" <<DISCSHIM
#!/usr/bin/env bash
for a in "\$@"; do
  if [ "\$a" = "--show-toplevel" ]; then
    printf '%s\n' "\$\$" >"$DPID"
    exec $TOSENTINEL
  fi
done
exec "$REALGIT" "\$@"
DISCSHIM
    chmod +x "$DSH/git"
    to_run_arm "DISCOVERY path / exit 124" "$DSH" 124 "$DPID" \
      "the work-tree root probe (rev-parse --show-toplevel) timed out after" \
      "rev-parse --show-toplevel"

    # ARM 5 — A HUNG *CANONICALISATION* (roborev job 395). `_anchor_canon` used to
    # end in `|| true`, so a timeout there was reported as an unresolvable path and
    # the operator got the stalled-mount cause's remedy from a completely different
    # cause. It was the one site job 374's status propagation did not reach,
    # because its own suppression hid the status.
    #
    # SAME TEMPLATE AS ARM 1, different INTERCEPTED BINARY: the helper runs
    # `sh -c 'cd -- "$1" && pwd -P'`, so the shim is `sh`, and it `exec`s the real
    # sh for everything else. The FIRST canonicalisation reached is the work-tree
    # root, so that is the cause this arm expects.
    #
    # IT TRIGGERS ON `pwd`, NOT ON `cd -- `, and that is forced by the control
    # rather than a preference: `to_run_arm`'s control argv is WORD-SPLIT, so a
    # quoted `-c` body cannot survive it — `cd -- "$1" && pwd -P` arrives as
    # separate words and no single arg contains `cd -- `, which made the control
    # exit 2 instead of the runner's 124. `pwd` is one word, appears in the real
    # helper's body, and the shim is only on PATH for this arm — where the
    # assert's ONLY use of `sh` is that canonicalisation.
    CSH="$T/bin-sh-hang-canon"
    mkdir -p "$CSH"
    CPID="$T/hang-canon.pid"
    REALSH=$(command -v sh 2>/dev/null) || REALSH=""
    if [ -z "$REALSH" ]; then
      printf 'ARM NOT TAKEN: hung canonicalisation (job 395) — the real `sh` could not be resolved, so a\n'
      printf 'ARM NOT TAKEN: pass-through shim cannot be built. The canonicalisation TIMEOUT cause is\n'
      printf 'ARM NOT TAKEN: UNEXERCISED on this run.\n'
      ok "hung-read (CANON path): SKIPPED (real sh unresolvable — arm UNEXERCISED, declared not silent)"
    else
      cat >"$CSH/sh" <<CANONSHIM
#!/usr/bin/env bash
for a in "\$@"; do
  case "\$a" in
    *pwd*) printf '%s\\n' "\$\$" >"$CPID"; exec $TOSENTINEL ;;
  esac
done
exec "$REALSH" "\$@"
CANONSHIM
      chmod +x "$CSH/sh"
      to_run_arm "CANON path / exit 124" "$CSH" 124 "$CPID" \
        "canonicalising the work-tree root (cd + pwd -P) timed out after" \
        "-c pwd" \
        sh
    fi

    # ARM 4 — A DISCOVERY CALL THAT *FAILS* (not hangs). Found while RED-verifying
    # arm 3, and it is the more serious half: `cd ""` SUCCEEDS in bash and leaves
    # the shell where it is, so `(cd "$x" && pwd -P)` with an EMPTY $x printed the
    # CURRENT DIRECTORY. A discovery call that failed therefore yielded cwd, every
    # `[ -z … ]` guard was unreachable, and the check ran on a plausible-looking
    # wrong value — arm 3's RED control exited 0 with `PREMERGE: OK`, a FALSE PASS.
    # Status propagation cannot close this route, because here the value really IS
    # empty; only refusing an empty argument can.
    #
    # No timeout involved, so this runs the NEUTRAL assert (real bound, real
    # constants) rather than the shortened scratch copy.
    FSH="$T/bin-git-fail-discovery"
    mkdir -p "$FSH"
    cat >"$FSH/git" <<FAILSHIM
#!/usr/bin/env bash
for a in "\$@"; do
  if [ "\$a" = "--show-toplevel" ]; then exit 1; fi
done
exec "$REALGIT" "\$@"
FAILSHIM
    chmod +x "$FSH/git"
    # NON-VACUITY: the shim must really make that one call fail while others work.
    if PATH="$FSH:$PATH" git rev-parse --show-toplevel >/dev/null 2>&1; then
      bad "discovery-fail fixture: the shim did not make rev-parse --show-toplevel fail"
    elif ! PATH="$FSH:$PATH" git rev-parse --git-dir >/dev/null 2>&1; then
      bad "discovery-fail fixture: the shim broke OTHER git calls too — the arm would not reach the code under test"
    else
      ok "discovery-fail fixture: --show-toplevel fails while other git calls pass through"
      OUT=$(cd "$ANC_REPO" && PATH="$FSH:$BIN:$PATH" MOCK_GH_FAIL=0 MOCK_GH_OUT="$R_CERT OPEN" \
        bash "$NEUTRAL_ASSERT" 2421 "$R_CERT" "$RANCFULL" "$RGOODDELTA" 2>&1)
      RC=$?
      if [ "$RC" -eq 0 ]; then
        bad "discovery-fail: a FAILED work-tree-root probe produced PREMERGE: OK — an empty canonicalisation is being read as the current directory (got: $OUT)"
      elif [ "$RC" -ne 3 ]; then
        bad "discovery-fail: expected exit 3 (got $RC: $OUT)"
      else
        ok "discovery-fail: a FAILED discovery probe refuses at exit 3, never a false PREMERGE: OK"
        case "$OUT" in
          *"could not be resolved"*)
            ok "discovery-fail: the refusal names the unresolvable root — the guard is REACHABLE at last" ;;
          *) bad "discovery-fail: expected the unresolvable-root cause (got: $OUT)" ;;
        esac
        case "$OUT" in
          *"timed out after"*)
            bad "discovery-fail: a plain failure must NOT be reported as a timeout (got: $OUT)" ;;
          *) ok "discovery-fail: a plain failure is not misreported as a timeout" ;;
        esac
      fi
    fi
  fi
fi

# --- 44(l) REMOVED: the leak self-test and its decoy (roborev job 381) -------
#
# This case existed to prove the leak adjudication could say LEAK. The
# adjudication is gone, so the proof has nothing to prove — and the decoy was the
# only fixture in this suite that deliberately spawned a live process in order to
# INSPECT it, which is where every process-lifetime defect in this apparatus
# originated (an orphaning child, an unregistered pid, a kill without a reap).
#
# DECLARED LOSS, printed at run time rather than left in a commit message: the
# shipped runner's reap-its-own-child property (job 279) is now asserted by
# NOTHING in this suite. The hung-read arms still prove the bound FIRES (exit 3,
# the ANCHOR-UNVERIFIABLE marker, a distinct TIMEOUT cause, and a runner-exit
# control per arm); they no longer prove the runner left no process behind.
printf 'DECLARED LOSS: leak reaping is NO LONGER ASSERTED by this suite (job 381). The hung-read\n'
printf 'DECLARED LOSS: arms prove the bound fires and names its own cause; they do NOT prove the\n'
printf 'DECLARED LOSS: bounded runner reaped its child. AND SINCE job 388 nothing sweeps either:\n'
printf 'DECLARED LOSS: killing from a stale ps snapshot could SIGKILL a RECYCLED pid, so instead\n'
printf 'DECLARED LOSS: every process these arms can leak is SELF-LIMITING (~120s) and nothing is\n'
printf 'DECLARED LOSS: signalled at all. A leak is SELF-CLEARING, not detected.\n'

# --- 44(m): THE SENTINEL IS UNIQUE PER RUN (cross-lane blocker) --------------
#
# A single run cannot demonstrate cross-run isolation, so the property is pinned
# two ways. Both matter: the derivation assert is what stops a future edit
# quietly restoring a shared constant, and the shape assert is what stops the
# derivation degrading into something a peer could also produce.
#
# (1) DERIVATION — two different scratch roots must yield two different
#     sentinels. This is the actual property: `$T` is run-unique, so a peer
#     lane's suite derives a different argv and neither census can see the other.
sent_a=$(to_derive_sentinel 111)
sent_b=$(to_derive_sentinel 222)
if [ -n "$sent_a" ] && [ "$sent_a" != "$sent_b" ]; then
  ok "sentinel: two different pids derive DIFFERENT sentinels — a concurrent run cannot collide with this one"
else
  bad "sentinel: two pids derived the SAME sentinel ('$sent_a') — the census would count a peer run's process as this run's leak, and the cleanup would SIGKILL it"
fi
# (1b) The derivation must be INJECTIVE over the pid range, not merely different
#      for two samples: `% 1000000000` is applied, so a pid could in principle
#      alias. Asserted over the whole plausible pid space rather than argued —
#      `/proc/sys/kernel/pid_max` where readable, else the 4194304 Linux default.
pid_max=$(cat /proc/sys/kernel/pid_max 2>/dev/null) || pid_max=""
case "$pid_max" in ''|*[!0-9]*) pid_max=4194304 ;; esac
if [ "$pid_max" -lt 1000000000 ]; then
  ok "sentinel: pid_max ($pid_max) is below the modulus, so the pid -> sentinel map is INJECTIVE — no aliasing is possible"
else
  bad "sentinel: pid_max ($pid_max) reaches the modulus, so two live pids could alias to one sentinel"
fi
# (2) THIS RUN's sentinel really is the derived one, not a constant someone
#     reintroduced beside it.
if [ "$TOSENTINEL" = "$(to_derive_sentinel "$$")" ]; then
  ok "sentinel: the live TOSENTINEL is the value derived from this run's own pid"
else
  bad "sentinel: TOSENTINEL ('$TOSENTINEL') is not the value derived from \$\$ — something is overriding the derivation"
fi
# (2b) A non-numeric token must be REFUSED loudly, never silently turned into a
#      shared default — that is how a fixed sentinel would sneak back.
if [ "$(to_derive_sentinel "not-a-pid")" = "INVALID-TOKEN" ]; then
  ok "sentinel: a non-numeric token yields INVALID-TOKEN, never a silent shared default"
else
  bad "sentinel: a non-numeric token did not refuse — a bad token must not degrade into a shared value"
fi
# (3) SHAPE — `sleep ` plus a fixed 10 digits. The width floor is not cosmetic:
#     a short duration could EXPIRE mid-run and make the leak check pass because
#     the sleep ended, not because the runner reaped it.
case "$TOSENTINEL" in
  "sleep 120."[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9])
    ok "sentinel: the derived value has the expected shape (sleep 120.<9 digits> — BOUNDED and run-unique)" ;;
  *)
    bad "sentinel: unexpected shape '$TOSENTINEL' — it must be a BOUNDED (self-limiting) and run-unique duration" ;;
esac
# (4) The historical SHARED constants must not be hard-coded anywhere in this
#     file any more — not in a shim, not in a census, not in a comment (a comment
#     carrying them would make this guard pass for the wrong reason if someone
#     later grepped for them).
# The needle is ASSEMBLED FROM TWO HALVES so this guard cannot match its own line
# (the idiom Case 41d already uses), and it is the `sleep `-prefixed form so the
# 40-hex FAKE_CERT fixture — which happens to contain the same digits — is not a
# false positive.
_sent_lit_a='sleep 9876'
_sent_lit_b='5432'
# `grep -c` PRINTS 0 AND EXITS 1 on no-match, so `$(grep -c … || printf 0)`
# captures BOTH and yields "0\n0" — which is not `0` and reds a clean tree. (That
# is exactly what the first version of this guard did.) The rc is read
# THREE-VALUED: 0/1 mean the count is trustworthy, >=2 means grep FAILED and the
# scan could not be made — which is `bad`, never a pass derived from an
# unmeasurable read.
shared_lits=$(grep -c -F -- "$_sent_lit_a$_sent_lit_b" "${BASH_SOURCE[0]}" 2>/dev/null)
_sent_rc=$?
if [ "$_sent_rc" -ge 2 ]; then
  bad "sentinel: the shared-literal scan could not be made (grep exit $_sent_rc) — UNMEASURED, not clean"
elif [ "$shared_lits" = 0 ]; then
  ok "sentinel: no hard-coded shared sentinel literal survives anywhere in this suite"
else
  bad "sentinel: $shared_lits occurrence(s) of a hard-coded shared sentinel literal remain — a fixed argv is exactly the cross-lane blocker"
fi

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
if run_anc 0 "success path prints SCOPE in the anchored-delta case too" \
  2421 "$R_CERT" "$RANCFULL" "$RGOODDELTA"; then
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
    run_anc 0 "SCOPE retained (anchored delta)" 2421 "$R_CERT" "$RANCFULL" "$RGOODDELTA" || continue
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
  # The #3752 legs get the neutral stub in EVERY scratch copy: their absence is
  # a TOOL-FAILURE by design, and these cases are about the advisory.
  printf '%s\n' "$NEUTRAL_BINDING" >"$d/premerge-review-binding.sh"
  chmod +x "$d/premerge-review-binding.sh"
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
  # The shipped #3752 legs run for real here too, so the case is given the
  # payloads they need: a PR body recording a roborev block, and a job record
  # whose git_ref head IS this fixture's certified sha. That makes this the one
  # case where BOTH shipped helpers are exercised end to end.
  WIRE_BASE=$(git -C "$WIRE_REPO" rev-parse refs/remotes/origin/main 2>/dev/null)
  WIRE_OUT=$(cd "$WIRE_REPO" &&
    PATH="$BIN:$PATH" MOCK_GH_OUT="$WIRE_SHA OPEN" MOCK_GH_FAIL=0 \
    MOCK_GH_PR_JSON="{\"baseRefName\":\"mainline\",\"body\":\"==== ROBOREV REVIEW SUMMARY ====\\njob: 7\\n==== END ROBOREV REVIEW SUMMARY ====\",\"comments\":[]}" \
    MOCK_ROBOREV_JSON="{\"id\":7,\"job\":{\"id\":7,\"git_ref\":\"$WIRE_BASE..$WIRE_SHA\",\"status\":\"done\"}}" \
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
  printf '%s\n' "$NEUTRAL_BINDING" >"$ORDFLOW/premerge-review-binding.sh"
  chmod +x "$ORDFLOW/premerge-review-binding.sh"
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
