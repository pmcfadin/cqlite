#!/usr/bin/env bash
# gate-component-verdict.sh — answer "did THIS COMPONENT pass?" from a gate summary,
# as an assertion SEPARATE from "did the run finish?" (issue #3750).
#
# THE DEFECT THIS CLOSES
# ----------------------
# `--only <component>` demotes a successful run to `RESULT: PARTIAL` (agent-gate.sh, on
# purpose — a component probe must never be pastable as the gate of record), so the
# mandated #3041 completion probe `grep -qE 'RESULT: (PASS|FAIL)'` terminated on failure
# and SPUN FOREVER ON SUCCESS. A lane spun 8+ minutes past a terminal PASS and then
# re-ran an 18-minute component that had already passed.
#
# The first fix for that was to widen the completion grammar to accept `PARTIAL`. That
# removes the hang and introduces a WORSE bug the moment anything reads success out of
# it, because:
#
#     `PARTIAL` says THE RUN ENDED. It does not say MY COMPONENT PASSED.
#
# A component probe therefore needs TWO assertions, and this script is the second one:
#
#   1. COMPLETION — did the run end? PRIMARY signal: the process EXIT STATUS, where the
#      poller can observe it (observing an exit status at all means the run ended; for
#      `--only`, agent-gate.sh exits 3). FALLBACK, for a detached run whose exit status
#      the poller never sees: the anchored, token-terminated text grammar for that MODE
#      (see --help). `scripts/gate-liveness.sh` is the shared reader for it.
#   2. VERDICT — did the component pass? THIS script, read from the component's OWN
#      line. A completed run whose component SKIPped or is ABSENT is NOT a pass: a SKIP
#      means the check never ran, which is the vacuous pass itself.
#
# EVERY POSITIVE VERDICT IS AN AFFIRMATIVE MEASUREMENT (CLAUDE.md, #3229)
# ----------------------------------------------------------------------
# `PASS` requires a COMPLETE terminal block for THIS run carrying EXACTLY ONE line for
# the requested component whose status token is EXACTLY `PASS`. Nothing else reaches it.
# Everything unmeasurable — an absent or unreadable summary, a non-terminal block, a
# truncated block, a foreign run-id, an unrecognised status token, two lines for one
# component — is `COULD-NOT-MEASURE` with a NAMED cause, and is NEVER read as a pass.
# `NOT-PASS` is likewise an affirmative reading: the component's line was found and its
# status is not `PASS`, or the block is complete and AFFIRMATIVELY does not name it.
#
#   VERDICT             exit  meaning
#   PASS                  0   this component's own line reads PASS in a complete run
#   NOT-PASS              1   read affirmatively, and it is not a pass (FAIL/SKIP/absent)
#   COULD-NOT-MEASURE     4   cannot tell; the printed cause says what was unmeasurable
#   USAGE                64   the request itself is malformed or is not this tool's to serve
#
# WHAT THIS IS NOT. It is not a gate-of-record verdict and cannot be made into one:
# `--mode record` is a NAMED REFUSAL naming `scripts/flow/premerge-assert.sh`, which
# already owns that grammar (it binds the certified sha, requires exactly one full block,
# and refuses `PARTIAL` token-exactly — pinned by scripts/tests/test_premerge_assert.sh).
# Re-implementing it here would be a second place for it to drift.
#
# Every line this script prints — stdout and stderr — begins `gate-verdict: `, and NO
# output ever contains a `RESULT: <TOKEN>` form or an `==== AGENT-GATE` marker. Run
# context is rendered `run-result=<TOKEN>` for exactly that reason: this tool's output
# gets pasted, and a line reading `run RESULT: PASS` would MATCH the documented
# gate-of-record completion probe — the artifact becoming the credential (#3312).
#
# Usage:
#   bash scripts/gate-component-verdict.sh <summary-file> --mode only \
#        --component <name> [--run-id <id>] [--heartbeat <path>]
#
# --run-id BINDS THE ANSWER TO A RUN (#2874). Pass it whenever you know it: without it a
# block left by a CONCURRENT PEER in the same checkout answers about the peer's gate.
#
# THE TWO DOCUMENTED TEXT-COMPLETION GRAMMARS (fallback only; prefer the exit status):
#   record (full / --lite / --delta):  grep -qE '^RESULT: (PASS|FAIL)([[:space:]]|$)'
#   only   (--only <component>):       grep -qE '^RESULT: (PASS|FAIL|PARTIAL)([[:space:]]|$)'
# Both ANCHORED and token-terminated. Unanchored, the first matches `RESULT: PASSENGER`
# and the second `RESULT: PARTIALLY` — a spelling check masquerading as a state check.
# The record grammar must keep REFUSING `PARTIAL`, and it does.
set -uo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
LIVENESS="$HERE/gate-liveness.sh"

SUMMARY=""; MODE=""; COMPONENT=""; WANT_RUN_ID=""; HB=""

# Every emit goes through these two, so the anchor and the no-RESULT-token rule hold for
# every path rather than at each printf site (CLAUDE.md #3312: an invariant over OUTPUT
# needs a check on the OUTPUT PATH). Neutralised DISPLAY-ONLY: every decision above is
# made on the raw value.
_safe() {  # <text> -> the same text, pastable gate tokens defused and controls stripped
  printf '%s' "$1" \
    | sed -e 's/RESULT:/RESULT(defused)/g' -e 's/==== AGENT-GATE/====(defused) AGENT-GATE/g' \
    | tr '\n\r\t' '   ' | tr -d '\000-\010\013\014\016-\037\177'
}
say()  { printf 'gate-verdict: %s\n' "$(_safe "$1")"; }
sayerr(){ printf 'gate-verdict: %s\n' "$(_safe "$1")" >&2; }

# verdict <TOKEN> <exit> <text> — the single terminal emit.
verdict() {
  local tok="$1" rc="$2" txt="$3"
  say "$tok $txt"
  [ -n "$SUMMARY" ] && say "summary: $SUMMARY"
  exit "$rc"
}
usage_refusal() { sayerr "USAGE $1"; exit 64; }

while [ $# -gt 0 ]; do
  case "$1" in
    --mode)      MODE="${2:?--mode needs a value}"; shift 2 ;;
    --component) COMPONENT="${2:?--component needs a value}"; shift 2 ;;
    --run-id)    WANT_RUN_ID="${2:?--run-id needs a value}"; shift 2 ;;
    --heartbeat) HB="${2:?--heartbeat needs a path}"; shift 2 ;;
    -h|--help)   awk 'NR>1 { if ($0 !~ /^#/) exit; print }' "$0"; exit 0 ;;
    -*)          usage_refusal "unknown option '$1'" ;;
    *)           if [ -n "$SUMMARY" ]; then usage_refusal "unexpected extra argument '$1'"; fi
                 SUMMARY="$1"; shift ;;
  esac
done

[ -n "$SUMMARY" ] || usage_refusal "a summary-file path is required; see --help"

# THE ACCEPTED-VERDICT SET IS A PARAMETER OF THE RUN MODE (#3750), never implicit and
# never one grammar serving both. The modes this tool does not serve are refusals that
# NAME their authority, so a caller is routed rather than left to improvise — which is
# how the record grammar got improvised as `RESULT: PASS` prefix greps in the first place.
case "$MODE" in
  only) ;;
  "")   usage_refusal "--mode is required (only|record|lite). The accepted-verdict set is a parameter of the run MODE, so it is never implicit; see --help" ;;
  record)
        usage_refusal "--mode record is not this tool's verdict to give. The gate-of-record grammar is owned by scripts/flow/premerge-assert.sh (it binds the certified sha, requires exactly one full block, and refuses a PARTIAL verdict token). A component line is NOT a certification" ;;
  lite)
        usage_refusal "--mode lite is a different claim entirely: a lite PASS is silent about 32 of the full gate's components, so a lite block's verdict token answers only for LITE_COMPONENTS. Read the LITE block, and never treat it as the gate of record" ;;
  delta)
        usage_refusal "--mode delta is not this tool's verdict to give: a delta re-certification is bound to its anchor's full PASS, which scripts/flow/premerge-assert.sh checks (Case B)" ;;
  *)    usage_refusal "unknown --mode '$MODE'; the closed set is only|record|lite|delta and only 'only' is served here" ;;
esac

[ -n "$COMPONENT" ] || usage_refusal "--mode only requires --component <name>"
# CLOSED NAME GRAMMAR, matching scripts/agent-gate.components' own: a name is
# [A-Za-z0-9._-]+ and may not start with `-`. Refusing anything else is what keeps the
# name out of the regex as metacharacters, so the pattern below cannot be steered by it.
case "$COMPONENT" in
  -*) usage_refusal "a component name may not start with '-' (got '$COMPONENT')" ;;
esac
if ! printf '%s' "$COMPONENT" | grep -qE '^[A-Za-z0-9][A-Za-z0-9._-]*$'; then
  usage_refusal "component name '$COMPONENT' is outside the closed grammar [A-Za-z0-9._-]+ (see scripts/agent-gate.components)"
fi
# `.` is the one accepted character that is also a regex metacharacter. Escape it rather
# than trusting it to match itself.
COMP_RE=$(printf '%s' "$COMPONENT" | sed 's/\./[.]/g')

[ -n "$HB" ] || HB="$SUMMARY.heartbeat"

# ---------------------------------------------------------------------------
# ONE READ, ONE SNAPSHOT, BOTH ASSERTIONS.
#
# The summary is a SHARED path that agent-gate.sh rewrites in place with `>`, so a
# per-question re-open can sample two different versions of the file — one run's
# `run-id:` combined with another run's component line. Both the completion question
# (delegated to gate-liveness.sh) and the verdict question below are answered from the
# SAME snapshot, so the two can never disagree about which run they read.
#
# ANSI is stripped at the parse site (#3400). The gate writes this block with plain
# printf and never colours it, so this is defence rather than a fix — but the rule is
# stated at the parse site, not at the emitter, and the strip is applied to the one
# snapshot both assertions read. A FAILED strip is a refusal, never "use the original":
# handing back unnormalised text converts a normalisation failure into a vacuous read.
# ---------------------------------------------------------------------------
SNAPDIR=$(mktemp -d "${TMPDIR:-/tmp}/gate-component-verdict.XXXXXX") || {
  sayerr "COULD-NOT-MEASURE $COMPONENT (cannot create a scratch directory for the snapshot)"; exit 4; }
trap 'rm -rf "$SNAPDIR"' EXIT
SNAP="$SNAPDIR/summary"

if [ ! -e "$SUMMARY" ]; then
  verdict COULD-NOT-MEASURE 4 "$COMPONENT (summary-absent; no file at that path — the run may not have started, or the path is wrong)"
fi
if [ ! -f "$SUMMARY" ] || [ ! -r "$SUMMARY" ]; then
  verdict COULD-NOT-MEASURE 4 "$COMPONENT (summary-unreadable; not a readable regular file)"
fi
_esc=$(printf '\033')
if ! sed -E "s/${_esc}\\[[0-9;]*[A-Za-z]//g" "$SUMMARY" > "$SNAP" 2>/dev/null; then
  verdict COULD-NOT-MEASURE 4 "$COMPONENT (summary-unreadable; could not snapshot/normalise the summary)"
fi

# ---------------------------------------------------------------------------
# ASSERTION 1 — COMPLETION, by ASKING the shared reader (roborev job 172: one
# implementation, one grammar). gate-liveness.sh already enumerates the terminal set
# from agent-gate.sh, requires the block's END marker (a truncated artifact is
# permanent and must never be believed), and enforces the #2874 run-id binding. A
# second grep here would be a second place for all three to drift.
#
# It is pointed at the SNAPSHOT with the REAL heartbeat path, so it reads exactly the
# bytes the verdict below is read from. `--no-wait` because a non-terminal block is
# COULD-NOT-MEASURE whichever non-terminal state it is in, so the stall-confirmation
# sleep would buy nothing and would block a poller.
# ---------------------------------------------------------------------------
if [ ! -r "$LIVENESS" ]; then
  verdict COULD-NOT-MEASURE 4 "$COMPONENT (reader-absent; the shared completion reader is not readable at $LIVENESS, so completion cannot be established — and this script deliberately re-implements neither the terminal grammar nor the run-id binding)"
fi
declare -a _gl_args=("$SNAP" --heartbeat "$HB" --no-wait)
[ -n "$WANT_RUN_ID" ] && _gl_args+=(--run-id "$WANT_RUN_ID")
GL_OUT=$(bash "$LIVENESS" "${_gl_args[@]}" 2>&1); GL_RC=$?
if [ "$GL_RC" -ne 0 ]; then
  # Reduce the reader's answer to its own first line, and name the SUMMARY rather than
  # the snapshot path the reader was handed.
  _gl_first=$(printf '%s\n' "$GL_OUT" | head -1)
  _gl_first="${_gl_first//"$SNAP"/"$SUMMARY"}"
  _gl_first="${_gl_first//"$SNAPDIR"/(snapshot)}"
  verdict COULD-NOT-MEASURE 4 "$COMPONENT (run-not-complete; the run has not published a terminal verdict for this request, so no component verdict exists yet — gate-liveness.sh: ${_gl_first:-no answer} [rc=$GL_RC])"
fi

# Run context ONLY (never the verdict): the terminal token, rendered so it can never be
# grepped as a gate verdict.
RUN_TOKEN=$(grep -m1 '^RESULT: ' "$SNAP" | sed -e 's/^RESULT: //' -e 's/ .*//')
[ -n "$RUN_TOKEN" ] || RUN_TOKEN="unreadable"

# ---------------------------------------------------------------------------
# ASSERTION 2 — THE VERDICT, from the component's OWN line.
#
# The line shape is agent-gate.sh's `_fm_summary_line`:
#   printf '%-18s %s (%s)  %s' "<name>:" "<STATUS>" "<secs>s" "<annotation>"
# so a component line is `^<name>: +<STATUS> (<digits>s)`. The `(<digits>s)` field is
# what STRUCTURALLY distinguishes a component line from a META line: `tree-integrity:
# PASS` and `component-set: PASS (36/36 vs …)` carry no duration, so a mistyped or
# non-component name cannot return a confident PASS. Derived from the emitter's format
# rather than from a second list of component names, which would be a second thing to
# drift.
#
# Read by REDIRECTION into a variable, never a `while read` pipeline whose verdict would
# be discarded in a subshell (#3400).
# ---------------------------------------------------------------------------
COMP_LINES=$(grep -E "^${COMP_RE}: +[A-Za-z][A-Za-z-]* \([0-9]+s\)" "$SNAP" || true)
COMP_N=0
[ -n "$COMP_LINES" ] && COMP_N=$(printf '%s\n' "$COMP_LINES" | grep -c '^' )

if [ "$COMP_N" -gt 1 ]; then
  # AMBIGUOUS, and ambiguity is never resolved in favour of PASS.
  verdict COULD-NOT-MEASURE 4 "$COMPONENT (ambiguous-component-line; the block carries $COMP_N lines for this component, so which one is the verdict cannot be established; run-result=$RUN_TOKEN)"
fi

if [ "$COMP_N" -eq 0 ]; then
  # AFFIRMATIVELY ABSENT from a block we have established is complete: the component was
  # not selected, or it crashed before recording. Either way the check did not pass, and
  # this must never soften to "probably fine".
  _hint=""
  if grep -qE "^${COMP_RE}: " "$SNAP"; then
    _hint=" (a non-component line with that prefix exists — a META line carries no (Ns) duration field and is not a verdict)"
  fi
  verdict NOT-PASS 1 "$COMPONENT (component-absent; the run completed and its block does not name this component as a component${_hint}; run-result=$RUN_TOKEN)"
fi

COMP_STATUS=$(printf '%s' "$COMP_LINES" | sed -E "s/^${COMP_RE}: +([A-Za-z][A-Za-z-]*) \([0-9]+s\).*/\1/")
# CLOSED STATUS GRAMMAR, matched EXACTLY as a token (#3229 / the `PASS*` accepts
# `PASSthisNeverRan` defect this repo has now made twice). An unrecognised token is
# COULD-NOT-MEASURE — if a future gate adds a fourth status, a lane asks a human rather
# than this reader guessing.
case "$COMP_STATUS" in
  PASS)
    verdict PASS 0 "$COMPONENT (its own component line reads PASS in a completed run; run-result=$RUN_TOKEN. THIS IS ONE COMPONENT, NOT THE GATE OF RECORD)" ;;
  FAIL)
    verdict NOT-PASS 1 "$COMPONENT (its own component line reads FAIL; run-result=$RUN_TOKEN)" ;;
  SKIP)
    verdict NOT-PASS 1 "$COMPONENT (its own component line reads SKIP — the check NEVER RAN, which is the vacuous pass itself, so this is not a pass; run-result=$RUN_TOKEN)" ;;
  *)
    verdict COULD-NOT-MEASURE 4 "$COMPONENT (unrecognised-status '$COMP_STATUS'; the closed set is PASS|FAIL|SKIP and a token outside it is never read as a pass; run-result=$RUN_TOKEN)" ;;
esac
