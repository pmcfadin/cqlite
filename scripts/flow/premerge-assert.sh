#!/usr/bin/env bash
#
# premerge-assert.sh — the #2456 pre-merge SHA guard + the #3465 gate-of-record
# guard, as a script (issues #2668, #3465).
#
# WHY THIS EXISTS
# ---------------
# The flow-closer certifies a SPECIFIC SHA: the exact tree the full gate of
# record and the final roborev pass actually ran on. If the PR's head has
# moved since that certification (a foreign push, a stale un-pushed rebase,
# someone else's commit), then `gh pr merge` would squash a DIFFERENT tree than
# the one the gate covered. That is the 2026-07-14 stale-merge escape on
# #2299/PR #2421: the closer certified a rebased-and-fixed tip locally but never
# pushed it, and `gh pr merge` squashed the PR's stale pre-fix head, transiently
# landing a known data-loss blocker on main. The GitHub required check re-runs
# on push but CANNOT catch a "merge of an old green head" — this SHA assertion
# is the real guard.
#
# #3465 adds the OTHER half. Verifying that the PR head equals a *claimed*
# certified sha never verified that a certified sha EXISTS. The gate-of-record
# convention was honour-system doctrine; this script is the one point every merge
# passes through, so the convention becomes a mechanism here: a summary file
# carrying a FULL-gate block with `RESULT: PASS`, `tree-integrity: PASS`,
# `dirty: no` (#3648), and provenance (`commit:` + `tree-start:`) covering the
# certified sha is REQUIRED.
#
# TWO DISTINCT ESCAPES, ONE MECHANISM
#   * #3408 — NO GATE AT ALL. That PR merged on 22 `--lite` PASSes and not one
#     `scripts/agent-gate.sh` run, because nothing in the merge path ever asked
#     for the full `==== AGENT-GATE SUMMARY ====` block. Refused here by the
#     block/marker/RESULT checks: lite and delta emit DISTINCT headers.
#   * #3616 — A REAL GATE, SOMEONE ELSE'S. A closer located its gate run dir by
#     RECENCY (`ls -t /tmp/agent-gate.*`), read a PEER LANE's run dir, saw 33 of
#     37 components PASS, and was about to merge #3616 on PR #3580's verdict. The
#     count was real, the dir was real, the timestamps were plausible; only the
#     `run-id:` line exposed it, and only because a human read it. With
#     14000-27000 stale run dirs per box and up to 4 concurrent gates, recency
#     picks a peer ROUTINELY. This is what the `commit:` + `tree-start:` binding
#     below refuses: a peer's summary for a different PR carries THAT PR's branch
#     head, so requiring both abbreviations to match the certified sha turns a
#     cross-lane verdict from "a human might notice the run-id line" into a
#     mechanical refusal at the merge point. The sha comparison is therefore not
#     bookkeeping — it is the guard for the #3616 class.
#
# The gate-of-record argument is deliberately REQUIRED, not optional: an optional
# argument would leave the honour system exactly where it is. Omitting it is a
# usage failure (exit 3), which breaks pre-#3465 callers loudly and on purpose.
#
# TWO ACCEPTED SHAPES (#3465 review blocker): DIRECT, or ANCHORED DELTA
# --------------------------------------------------------------------
# CLAUDE.md's #1892 post-gate-polish rule MANDATES that a test/docs-only diff on
# top of a full PASS at anchor `X` re-certifies with `scripts/agent-gate.sh
# --delta X` and "never a repeat full gate", and that the PR record BOTH the
# delta block AND the anchor's full SUMMARY. So the merged head `Y` legitimately
# differs from the gate of record's `X`, and a guard that accepted only the
# 3-argument shape red on correct, doctrine-mandated input — the guard agents
# learn to waive. Hence the OPTIONAL fourth argument:
#
#   CASE A (3 args) — DIRECT. The full block's `commit:`/`tree-start:` must
#     cover the certified sha. The gate of record ran on the merged tree itself.
#   CASE B (4 args) — ANCHORED DELTA. The full block is the ANCHOR (its sha need
#     NOT be the certified sha), and the fourth argument must be a `--delta`
#     block that (i) is a PASS with an intact tree, (ii) names that exact anchor
#     in `delta-anchor:`, and (iii) whose OWN `commit:`/`tree-start:` cover the
#     certified sha. The chain is therefore closed end to end: full PASS at X →
#     delta re-cert anchored at X → delta ran on Y → Y is the PR head.
#
# In BOTH cases a full-gate PASS must EXIST, and the merged tree is covered
# either directly (A) or by an anchored delta re-cert on top of it (B). What is
# never accepted is a delta or lite block ALONE — the #3408 escape.
#
# We parse gh with gh's built-in `--jq` (jq expression run inside gh), so gh's
# JSON serialization is NOT load-bearing — we never read raw JSON with
# sed/regex. The gate summary is parsed by whole-line-anchored marker matching,
# after an ANSI strip that is BELT rather than the load-bearing part: the summary
# FILE's block lines are `echo`s of computed strings (scripts/agent-gate.sh
# emit_summary), so they are not coloured; `CARGO_TERM_COLOR` colours cargo
# output inside `gate.log`, not the block. The strip covers the case where the
# block was recovered from a coloured CAPTURE rather than from the summary file
# (#3400: colour survives redirection).
#
# TWO RESIDUALS, STATED RATHER THAN FAKED
# ---------------------------------------
#  1. `run-id:` CANNOT be verified here. The #2874 reader contract says a reader
#     must confirm the summary's `run-id:` matches the run IT launched — this
#     script did not launch the gate, so it has nothing to compare against. It
#     therefore does not look at `run-id:` at all rather than pretend to. That is
#     precisely why the `commit:`/`tree-start:` binding carries the weight for the
#     #3616 cross-lane class above: it is the only property of a peer's summary
#     this script CAN falsify without having launched the run.
#  2. This assert proves a summary EXISTS claiming a full-gate PASS covering this
#     sha with an intact tree. It cannot prove that summary was produced by a
#     genuine gate run rather than hand-written. A HOSTILE INVOKER IS OUT OF THE
#     THREAT MODEL — whoever runs this script controls the process and could
#     edit the script, fake the file, or skip the script entirely; no check
#     inside a process defends against the party that controls the process. What
#     this guard defends is ACCIDENT AND DRIFT, which is the observed failure
#     mode: a diligent worker with no step in its path telling it the gate of
#     record was never run.
#  3. THE CERTIFIED TREE IS NOT THE MERGED TREE (#3650). A squash-merge composes
#     this diff with main's CURRENT tip, not with the base the branch was written
#     against, so for any PR whose base is behind main the tree this script
#     certifies and the tree that lands are DIFFERENT OBJECTS. Measured on
#     #3358/PR #3362: base 2bde26a7c with main 10 commits ahead, whose head gate
#     FAILed `core-tests` only because the fix for a known flake (5e08db201,
#     #3514) was on main and absent from that base — the benign direction. The
#     MALIGN direction is a PASS at a stale head hiding an interaction with
#     something that landed in between: this assert would accept it, and the
#     merge would compose two things never tested together. So this script
#     proves FACT 1 — the diff is unchanged since certification and a full gate
#     of record PASSed on that exact tree — and it explicitly does NOT prove
#     FACT 2 — that the diff was certified against the main it will join. Fact 2
#     is a gate on the MERGE RESULT and is STILL NOT implemented here: it is
#     #3650's SLICE 2, filed separately. The success path SAYS so
#     (`PREMERGE: SCOPE`), because an enforcement that certifies the wrong tree
#     while CLAIMING to close #3465 would be the vacuous-pass shape one level up
#     — worse than the gap it replaces, which is at least visible.
#
#     WHAT SLICE 1 ADDED, AND WHAT IT DELIBERATELY DID NOT. This script now runs
#     `scripts/flow/base-staleness.sh` (resolved from its OWN directory, with no
#     env override — #3312's enforcer rule) and reports its finding on
#     `PREMERGE: ADVISORY` lines: `N` commits behind the merge-base and `M` of
#     those touching this diff's blast radius (paths the diff touches + a
#     hard-coded gate-global set). That is INFORMATION, not enforcement:
#     **the advisory can never change this script's exit code.** An advisory that
#     is absent, fails, or reports `UNMEASURED` is REPORTED and is not fatal in
#     slice 1. Two properties of it a reader must carry:
#       * `UNMEASURED` MUST be treated as STALE by any consumer, never as fresh
#         (#3650 D3) — the standing rule against deriving a pass from the absence
#         of a bad signal. Slice 2 is the consumer that will act on it.
#       * the blast radius is NOT a dependency closure. A commit changing an item
#         this diff CALLS, touching neither this diff's paths nor a gate-global
#         path, is reported as NOT staling. The advisory declares that on every
#         run; it is a real false-negative class, filed, not closed.
#     So the three `PREMERGE: SCOPE` lines are RETAINED: slice 1 does not close
#     the gap they disclose, and removing them would be exactly the overclaim
#     this residual exists to prevent.
#
# USAGE
#   scripts/flow/premerge-assert.sh <pr-number> <certified-sha> \
#       <gate-of-record-summary> [<delta-summary>]
#
# ENVIRONMENT
#   GH_REPO   the target repo (default: pmcfadin/cqlite). `gh` honors GH_REPO
#             natively; we pass --repo explicitly too so the default applies.
#
# EXIT CODES
#   0   gate of record verified + head matches + PR OPEN
#       — prints "PREMERGE: OK <sha>", "PREMERGE: SCOPE ..." (what was and was
#         NOT proven, #3650), "PREMERGE: ADVISORY ..." (the non-blocking
#         base-staleness report, #3650 slice 1 — it NEVER changes this exit
#         code) and "PREMERGE: GATE-OF-RECORD ..."
#         (plus "PREMERGE: DELTA-RECERT ..." in Case B)
#   2   no/invalid gate of record, OR head moved (mismatch), OR PR closed/merged
#       — LOUD multi-line refusal
#   3   gh/network failure, a required TOOL failing, or a usage error — fail
#       closed, never merge on uncertainty. The three are distinguished by the
#       printed marker, NOT by the code: `PREMERGE: USAGE` (you called it wrong),
#       `PREMERGE: TOOL-FAILURE` (a broken box — fix the box, do NOT re-run the
#       gate), `PREMERGE: GH-FAILURE` (auth/network/no-such-PR).
#
# macOS bash 3.2 compatible, shellcheck-clean.
set -euo pipefail

repo="${GH_REPO:-pmcfadin/cqlite}"

usage() {
  # A distinct grepable marker (#3465 review nit 8): exit 3 covers a USAGE error,
  # a TOOL failure and a GH failure, and a caller must be able to tell them apart
  # — "you called me wrong" is not "GitHub is down". The exit CODES are unchanged.
  printf 'PREMERGE: USAGE — the call is wrong (this is NOT a gh/network failure)\n' >&2
  printf 'usage: %s <pr-number> <certified-sha> <gate-of-record-summary> [<delta-summary>]\n' \
    "$(basename "$0")" >&2
  printf '       <gate-of-record-summary> is REQUIRED: the AGENT_GATE_SUMMARY_FILE of the\n' >&2
  printf '       FULL gate (a "==== AGENT-GATE SUMMARY ====" block with RESULT: PASS and\n' >&2
  printf '       tree-integrity: PASS). With 3 args it must be AT the certified sha.\n' >&2
  printf '       <delta-summary> is OPTIONAL: an "==== AGENT-GATE DELTA SUMMARY ====" block\n' >&2
  printf '       whose delta-anchor: is the full block above and whose own commit:/\n' >&2
  printf '       tree-start: are AT the certified sha (the #1892 post-gate-polish route).\n' >&2
  printf '       See #3465.\n' >&2
}

if [ "$#" -ne 3 ] && [ "$#" -ne 4 ]; then
  usage
  exit 3
fi

pr="$1"
certified="$2"
summary_file="$3"
delta_file="${4:-}"

if [ -z "$pr" ] || [ -z "$certified" ] || [ -z "$summary_file" ]; then
  usage
  exit 3
fi
# An EMPTY fourth argument is a usage failure, not "3-arg mode": a caller whose
# variable expanded to nothing must be told, never silently downgraded.
if [ "$#" -eq 4 ] && [ -z "$delta_file" ]; then
  usage
  exit 3
fi

# Normalize the certified SHA to lowercase and require a full 40-char hex SHA —
# an abbreviated or malformed value can never be safely compared to headRefOid.
certified=$(printf '%s' "$certified" | tr '[:upper:]' '[:lower:]')
case "$certified" in
  *[!0-9a-f]* | "")
    printf 'error: certified SHA must be 40 hex chars (got: %s)\n' "$2" >&2
    usage
    exit 3
    ;;
esac
if [ "${#certified}" -ne 40 ]; then
  printf 'error: certified SHA must be a full 40-char hex SHA (got %d chars: %s)\n' \
    "${#certified}" "$2" >&2
  usage
  exit 3
fi

# ---------------------------------------------------------------------------
# GATE OF RECORD (#3465) — checked FIRST, before any `gh` call. It is offline
# and cheap, and "you have no gate of record" must be reportable without a
# network round trip.
# ---------------------------------------------------------------------------

refuse_no_gate() {
  printf '========================================================\n' >&2
  printf 'PREMERGE: NO-GATE-OF-RECORD — REFUSING TO MERGE\n' >&2
  printf '  summary file: %s\n' "$summary_file" >&2
  [ -n "$delta_file" ] && printf '  delta summary file: %s\n' "$delta_file" >&2
  printf '  certified sha: %s\n' "$certified" >&2
  while [ "$#" -gt 0 ]; do
    printf '  %s\n' "$1" >&2
    shift
  done
  printf '  The FULL gate is the only run that counts (#719). Run it once,\n' >&2
  printf '  immediately pre-merge, with the mandated redirect:\n' >&2
  printf '    AGENT_GATE_SUMMARY_FILE=<path> bash scripts/agent-gate.sh > gate.log 2>&1\n' >&2
  printf '  then pass <path> as the third argument.\n' >&2
  printf '  If the ONLY diff since a full PASS at anchor X is test/docs-only, the\n' >&2
  printf '  sanctioned route is the ANCHORED DELTA PAIR (#1892) — not a repeat full\n' >&2
  printf '  gate: scripts/agent-gate.sh --delta X --anchor-run-id <id>, then pass BOTH\n' >&2
  printf '  summaries: <anchor-full-summary> <delta-summary>. See #3465.\n' >&2
  printf '========================================================\n' >&2
  exit 2
}

# A required TOOL failing is NOT "no gate of record" and must NOT be answered
# with "go run a 45-minute gate" (#3465 review nit 6). The header reserves exit 3
# for tool/usage failure; route it there, naming the tool.
refuse_tool_failure() {
  printf '========================================================\n' >&2
  printf 'PREMERGE: TOOL-FAILURE\n' >&2
  printf '  %s failed while parsing %s.\n' "$1" "$2" >&2
  printf '  This is a broken/absent tool on THIS box (missing, ENOMEM, bad PATH),\n' >&2
  printf '  not a verdict about the gate of record. Fix the box and re-run this\n' >&2
  printf '  assert — do NOT re-run the gate. Refusing to merge (fail closed).\n' >&2
  printf '========================================================\n' >&2
  exit 3
}

# ---------------------------------------------------------------------------
# THE BASE-STALENESS ADVISORY (#3650 slice 1) — INFORMATION, NEVER A VERDICT
# ---------------------------------------------------------------------------
# Resolved from THIS script's own directory, with NO env override and no
# `${...:-...}` fallback: #3312's second rule is that the constrained party must
# not choose its own enforcer, and "which paths stale my certification" is
# exactly what a lane wanting to skip a re-gate would redirect. A test needing a
# different advisory substitutes the ARTIFACT in a scratch copy of the tree.
#
# NOTHING here may alter this script's exit code. Every failure mode — absent,
# not executable, non-zero, empty output, UNMEASURED — is REPORTED on a
# `PREMERGE: ADVISORY` line and then ignored. That is slice 1's whole contract:
# an enforcement built on an information source nobody has read yet would be the
# vacuous-pass shape one level up.
#
# IT MEASURES THE CERTIFIED SHA, NOT THIS CHECKOUT'S HEAD (#3650 review F1)
# ------------------------------------------------------------------------
# The advisory is invoked with `"$certified"` EXPLICITLY. Invoked with no rev it
# defaults to `HEAD`, which is the LOCAL CHECKOUT's head — and the whole point of
# the surrounding assert is that the local head and the sha being approved can
# differ (a foreign push, a stale un-pushed rebase). A report about a DIFFERENT
# diff than the one being merged is the "satisfied and wrong" shape this issue
# exists to remove, and slice 2 will CONSUME this report. If the certified commit
# is not present in this checkout the advisory reports UNMEASURED — correct, and
# non-fatal here by the paragraph above.
# Resolved WITHOUT letting `set -e` kill the run: this executes before argument
# validation, so an unreadable script directory would exit 1 — a code outside the
# documented 0/2/3 set, from a line that only feeds a NON-BLOCKING advisory. An
# unresolvable directory degrades to the ABSENT branch below, which is reported
# and not fatal, exactly like a deleted advisory.
self_dir=""
if ! self_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" 2>/dev/null && pwd)"; then
  self_dir=""
fi
if [ -n "$self_dir" ]; then
  advisory_script="$self_dir/base-staleness.sh"
else
  advisory_script="<unresolvable script directory>/base-staleness.sh"
fi

# The advisory is BOUNDED (60s). It sits on the merge critical path, its cost
# grows with how far the base is behind, and an unbounded child of the merge
# gate is a hang the closer cannot distinguish from a slow gh call. A timeout is
# just another non-zero exit here: REPORTED on an ADVISORY line (`exit 124`) and
# ignored, per the paragraph above.
#
# AN UNAVAILABLE BOUND SKIPS THE ADVISORY — IT DOES NOT DEGRADE TO AN UNBOUNDED
# CALL (#3650 review B1). `timeout` is not POSIX and is absent on a stock macOS,
# which this repo supports, and the earlier code took the UNBOUNDED branch there
# and said so in this comment as though it were a considered trade. It was not:
# an unbounded child on the MERGE CRITICAL PATH is precisely the hang the bound
# exists to prevent, and a rationale written down is what stops the next reader
# questioning it. Skipping keeps BOTH invariants — the bound is never silently
# dropped, and the advisory still cannot touch this script's exit code, because
# the unavailability is REPORTED on a `PREMERGE: ADVISORY` line naming the
# missing mechanism, exactly like an absent artifact.
#
# TWO THINGS THE FIRST VERSION OF THAT BOUND GOT WRONG (#3650 review R1/R2):
#
#  R1 — `timeout <secs>` SENDS SIGTERM AND THEN WAITS, so a child that traps or
#  ignores TERM runs on indefinitely and the advertised bound bounds NOTHING.
#  The escalation `--kill-after=<grace>` follows with SIGKILL, which cannot be
#  trapped, and it IS the bound. This is the same finding, with the same
#  measurement, that scripts/lib/gate-notify.sh records for the gate's notify
#  path and scripts/bootstrap-agent-machine.sh for its network probes.
#
#  R2 — only `timeout` was resolved, while GNU coreutils installs its timeout as
#  `gtimeout` on stock macOS. The skip diagnostic below TOLD the reader to
#  install coreutils for `gtimeout`, and the code then never looked for it: on
#  the exact configuration the message recommends, the advisory still skipped.
#
# So the resolution follows the repository's existing convention verbatim
# (`_gate_notify_bounded_timeout`, scripts/lib/gate-notify.sh): try `timeout`
# then `gtimeout`, PROBE each for `--kill-after` rather than assuming it (BusyBox
# and older implementations reject the flag, and a non-GNU `timeout` earlier on
# PATH must not win a first-match-wins lookup), and treat a candidate that
# rejects it as NO bounding tool at all. That last part is the B1 rule applied
# one level down: an escapable bound is not a bound, and running behind one
# would be the silent degrade B1 forbids.
ADVISORY_TIMEOUT_SECS=60
# The grace is ADDITIVE wall-clock: the true worst case of the advisory call is
# ADVISORY_TIMEOUT_SECS + ADVISORY_KILL_GRACE. 5s is ample for a well-behaved
# child to finish its own cleanup after TERM.
ADVISORY_KILL_GRACE=5

# resolve_advisory_timeout — print a timeout(1) that supports `--kill-after`, or
# return 1 when no such runner exists. Capability is PROBED, never assumed; see
# the R1/R2 paragraphs above.
resolve_advisory_timeout() {
  local c
  for c in timeout gtimeout; do
    command -v "$c" >/dev/null 2>&1 || continue
    "$c" --kill-after=1 1 true >/dev/null 2>&1 || continue
    printf '%s\n' "$c"
    return 0
  done
  return 1
}

print_base_staleness_advisory() {
  local adv_out adv_rc=0 line adv_to
  if [ ! -f "$advisory_script" ]; then
    printf 'PREMERGE: ADVISORY base-staleness.sh is ABSENT at %s — the base-staleness\n' \
      "$advisory_script"
    printf 'PREMERGE: ADVISORY report could not be produced. NOT fatal in #3650 slice 1: the\n'
    printf 'PREMERGE: ADVISORY advisory changes no verdict, so its absence changes none either.\n'
    return 0
  fi
  if ! adv_to=$(resolve_advisory_timeout); then
    printf 'PREMERGE: ADVISORY base-staleness.sh was NOT RUN: no `timeout`/`gtimeout` on PATH\n'
    printf 'PREMERGE: ADVISORY supporting `--kill-after`, so the %ss bound could not be applied\n' \
      "$ADVISORY_TIMEOUT_SECS"
    printf 'PREMERGE: ADVISORY with a SIGKILL escalation, and the bound is not droppable (#3650\n'
    printf 'PREMERGE: ADVISORY review B1/R1) — an UNBOUNDED child here, or one behind a\n'
    printf 'PREMERGE: ADVISORY SIGTERM-only bound a child can ignore, is the merge-path hang the\n'
    printf 'PREMERGE: ADVISORY bound exists to prevent, so the advisory is SKIPPED.\n'
    printf 'PREMERGE: ADVISORY NOT fatal in #3650 slice 1: the advisory changes no verdict, so\n'
    printf 'PREMERGE: ADVISORY its absence changes none either. Install GNU coreutils (its\n'
    printf 'PREMERGE: ADVISORY timeout is `gtimeout` on macOS, and IS accepted here) to get the\n'
    printf 'PREMERGE: ADVISORY report back.\n'
    return 0
  fi
  adv_out=$("$adv_to" --kill-after="$ADVISORY_KILL_GRACE" "$ADVISORY_TIMEOUT_SECS" \
    bash "$advisory_script" "$certified" 2>&1) || adv_rc=$?
  if [ -z "$adv_out" ]; then
    printf 'PREMERGE: ADVISORY base-staleness.sh produced NO output (exit %s) — reported, and\n' \
      "$adv_rc"
    printf 'PREMERGE: ADVISORY not fatal in #3650 slice 1.\n'
    return 0
  fi
  while IFS= read -r line; do
    printf 'PREMERGE: ADVISORY %s\n' "$line"
  done <<EOF
$adv_out
EOF
  printf 'PREMERGE: ADVISORY exit %s — advisory ONLY (#3650 slice 1): it did NOT affect this\n' \
    "$adv_rc"
  printf 'PREMERGE: ADVISORY assert. A CONSUMER of the advisory (slice 2) must treat exit 5 /\n'
  printf 'PREMERGE: ADVISORY UNMEASURED as STALE, never as fresh.\n'
  return 0
}

# assert_readable_summary <file> <what> — the three file-level preconditions.
assert_readable_summary() {
  if [ ! -f "$1" ]; then
    refuse_no_gate "The $2 file does not exist (or is not a regular file)."
  fi
  if [ ! -r "$1" ]; then
    refuse_no_gate "The $2 file exists but is not readable."
  fi
  if [ ! -s "$1" ]; then
    refuse_no_gate "The $2 file is EMPTY — nothing was certified."
  fi
}

# Parse the summary by REDIRECTION, never a pipe (#3400: a piped `while read`
# runs in a subshell and its verdict is discarded). One awk pass:
#   * strips ANSI escapes and a trailing CR before matching anything (belt — see
#     the header: the summary file's own block lines are not coloured)
#   * counts blocks by WHOLE-LINE-EXACT marker equality, never substring. That
#     anchoring defends against (a) PROSE copies of a marker — indented,
#     `>`-quoted, fenced, or mid-sentence — which CLAUDE.md, issue bodies, PR
#     comments and the very doctrine files this change edits all contain, and
#     (b) a TRUNCATED pattern such as `AGENT-GATE SUMMARY ====`, which matches
#     ALL FOUR markers (full/lite start and end). Note the end marker does NOT
#     contain the start marker as a substring — `END ` sits between `====` and
#     `AGENT-GATE` — so substring matching would fail for the reasons above,
#     not for that one.
#   * counts all three block families so a refusal can NAME what it found
#     (the headers are distinct by construction: scripts/agent-gate.sh)
#   * emits key=value lines with per-key occurrence COUNTS, so a duplicated key
#     inside one block is refusable rather than silently last-wins
# WANT selects which family is "the block": full (default) or delta.
_gate_awk() {
  awk -v WANT="$2" '
  BEGIN {
    FULL_S  = "==== AGENT-GATE SUMMARY ===="
    FULL_E  = "==== END AGENT-GATE SUMMARY ===="
    LITE_S  = "==== AGENT-GATE LITE SUMMARY ===="
    DELTA_S = "==== AGENT-GATE DELTA SUMMARY ===="
    DELTA_E = "==== END AGENT-GATE DELTA SUMMARY ===="
    if (WANT == "delta") { S = DELTA_S; E = DELTA_E } else { S = FULL_S; E = FULL_E }
    blocks = 0; full = 0; lite = 0; delta = 0; open = 0; unterminated = 0
    n_result = 0; n_ti = 0; n_commit = 0; n_ts = 0; n_mode = 0
    n_anchor = 0; n_nested = 0; anchor_unresolved = 0; n_dirty = 0
    v_result = ""; v_ti = ""; v_commit = ""; v_ts = ""; v_dirty = ""
    v_mode = ""; v_anchor = ""
  }
  {
    gsub(/\033\[[0-9;]*[a-zA-Z]/, "")
    sub(/\r$/, "")
  }
  $0 == FULL_S  { full++;  if (S == FULL_S)  { blocks++; if (open == 1) unterminated = 1; open = 1 } next }
  $0 == DELTA_S { delta++; if (S == DELTA_S) { blocks++; if (open == 1) unterminated = 1; open = 1 } next }
  $0 == LITE_S  { lite++;  next }
  $0 == E       { if (open == 1) open = 0; next }
  open == 1 {
    if ($1 == "MODE:")                { n_mode++;   v_mode = $2 }
    else if ($1 == "RESULT:")         { n_result++; v_result = $2 }
    else if ($1 == "tree-integrity:") { n_ti++;     v_ti = $2 }
    else if ($1 == "tree-start:")     { n_ts++;     v_ts = $2 }
    else if ($1 == "nested-under:")   { n_nested++ }
    else if ($1 == "delta-anchor:") {
      n_anchor++; v_anchor = $2
      for (i = 2; i <= NF; i++) if ($i == "(UNRESOLVED)") anchor_unresolved = 1
    }
    else if ($1 == "commit:") {
      n_commit++; v_commit = $2
      # COUNT every `dirty:` token and keep the FIRST value, never the last.
      # The old loop assigned on every match, so `dirty: yes dirty: no` reduced
      # to `no` and certified a dirty run (#3648 roborev round 2). That is the
      # "last one wins" rule assert_single_key exists to refuse, one field down.
      # Scanned to NF (not NF-1) so a BARE trailing `dirty:` still COUNTS as an
      # occurrence; its value stays empty and refuses on the empty-value path.
      for (i = 2; i <= NF; i++) if ($i == "dirty:") {
        n_dirty++
        if (n_dirty == 1 && i < NF) v_dirty = $(i + 1)
      }
    }
    next
  }
  END {
    if (open == 1) unterminated = 1
    print "blocks=" blocks
    print "full=" full
    print "lite=" lite
    print "delta=" delta
    print "unterminated=" unterminated
    print "n_mode=" n_mode
    print "n_result=" n_result
    print "n_ti=" n_ti
    print "n_commit=" n_commit
    print "n_ts=" n_ts
    print "n_anchor=" n_anchor
    print "n_nested=" n_nested
    print "n_dirty=" n_dirty
    print "anchor_unresolved=" anchor_unresolved
    print "v_result=" v_result
    print "v_ti=" v_ti
    print "v_commit=" v_commit
    print "v_ts=" v_ts
    print "v_dirty=" v_dirty
    print "v_mode=" v_mode
    print "v_anchor=" v_anchor
  }
' <"$1"
}

# gate_parse_file <file> <want> <what> — run the parse and publish its fields as
# GP_* globals (bash 3.2: no namerefs, no associative arrays). Every COUNT is
# validated as a non-negative integer here, keyed on its AFFIRMATIVE value: an
# unparseable/absent count is refused, never treated as "no problem found".
gate_parse_file() {
  local gp_out gp_k gp_v
  gp_out=$(_gate_awk "$1" "$2") || refuse_tool_failure awk "$3"
  GP_blocks=""; GP_full=""; GP_lite=""; GP_delta=""; GP_unterminated=""
  GP_n_mode=""; GP_n_result=""; GP_n_ti=""; GP_n_commit=""; GP_n_ts=""
  GP_n_anchor=""; GP_n_nested=""; GP_anchor_unresolved=""; GP_n_dirty=""
  GP_v_result=""; GP_v_ti=""; GP_v_commit=""; GP_v_ts=""; GP_v_dirty=""
  GP_v_mode=""; GP_v_anchor=""
  while IFS='=' read -r gp_k gp_v; do
    case "$gp_k" in
      blocks)       GP_blocks="$gp_v" ;;
      full)         GP_full="$gp_v" ;;
      lite)         GP_lite="$gp_v" ;;
      delta)        GP_delta="$gp_v" ;;
      unterminated) GP_unterminated="$gp_v" ;;
      n_mode)       GP_n_mode="$gp_v" ;;
      n_result)     GP_n_result="$gp_v" ;;
      n_ti)         GP_n_ti="$gp_v" ;;
      n_commit)     GP_n_commit="$gp_v" ;;
      n_ts)         GP_n_ts="$gp_v" ;;
      n_anchor)     GP_n_anchor="$gp_v" ;;
      n_nested)     GP_n_nested="$gp_v" ;;
      n_dirty)      GP_n_dirty="$gp_v" ;;
      anchor_unresolved) GP_anchor_unresolved="$gp_v" ;;
      v_result)     GP_v_result="$gp_v" ;;
      v_ti)         GP_v_ti="$gp_v" ;;
      v_commit)     GP_v_commit="$gp_v" ;;
      v_ts)         GP_v_ts="$gp_v" ;;
      v_dirty)      GP_v_dirty="$gp_v" ;;
      v_mode)       GP_v_mode="$gp_v" ;;
      v_anchor)     GP_v_anchor="$gp_v" ;;
    esac
  done <<GATE_PARSE
$gp_out
GATE_PARSE
  for gp_k in blocks full lite delta unterminated n_mode n_result n_ti n_commit \
              n_ts n_anchor n_nested anchor_unresolved n_dirty; do
    eval "gp_v=\${GP_$gp_k}"
    case "$gp_v" in
      ''|*[!0-9]*)
        refuse_no_gate "Gate summary parse produced no usable '$gp_k' count for the $3 — refusing (fail closed)."
        ;;
    esac
  done
}

# assert_single_key <count> <label> <what>: the key must appear EXACTLY once.
# Zero certifies nothing; more than one is ambiguous and a "last one wins" rule
# would let a doctored line override the real verdict. Asserted per key,
# immediately before that key is USED, so the diagnostic names the first thing
# that is wrong — e.g. the #3041 launch sentinel (a FULL-header block carrying
# `tree-start:` and `RESULT: INCOMPLETE`, with no `tree-integrity:`/`commit:`
# yet) is reported as the INCOMPLETE verdict it is, not as a missing
# tree-integrity line.
assert_single_key() {
  if [ "$1" -eq 0 ]; then
    refuse_no_gate "The $3 has no '$2:' line — it cannot certify anything."
  fi
  if [ "$1" -gt 1 ]; then
    refuse_no_gate "The $3 has $1 '$2:' lines — AMBIGUOUS, refusing."
  fi
}

# assert_hex_abbrev <label> <value> <what>: the value must be a lowercase-hex
# abbreviation of SOME sha. A non-hex value ("(not captured)", "(capture
# unavailable — no git worktree)", "selftest", "unverified") REFUSES — it is
# never skipped.
assert_hex_abbrev() {
  local n
  case "$2" in
    ''|*[!0-9a-f]*)
      refuse_no_gate \
        "'$1:' value '$2' in the $3 is not lowercase hex — nothing verifiable was recorded." \
        "The gate writes a non-hex placeholder when its capture failed or there was no" \
        "git worktree; such a run proves nothing about which tree it executed against."
      ;;
  esac
  n=${#2}
  # FLOOR 7, not 4 (#3465 review nit 5): 7 is the NARROWEST abbreviation the gate
  # ever emits (`commit:` is `printf '%.7s'`; `tree-start:` is `%.12s`), and a
  # 4-hex value accepted at its own width is a 1-in-65536 accidental cross-lane
  # match — precisely the #3616 class this compare exists to refuse. Accepting a
  # width the gate cannot produce buys nothing and weakens the binding.
  if [ "$n" -lt 7 ] || [ "$n" -gt 40 ]; then
    refuse_no_gate \
      "'$1:' value '$2' in the $3 is $n hex chars — outside the 7..40 range." \
      "The gate emits 7 (commit:) and 12 (tree-start:) hex; a narrower value cannot" \
      "bind a run to a tree (a 4-hex 'match' is 1-in-65536 by accident)."
  fi
}

# assert_covers <label> <value> <full-40-sha> <what> <subject>: the abbreviation
# must be a prefix of the full sha AT ITS OWN EXACT WIDTH.
#
# `commit:` carries a 7-char abbreviation and `tree-start:` a 12-char one (both
# `printf '%.Ns'` of the same VERIFIED capture in scripts/agent-gate.sh), so
# "matches the certified sha" cannot be string equality against the 40-hex sha.
# Compare each value at ITS OWN width, using the value's own length — never a
# glob, never `case $x in $y*)`, never a fixed assumed width. BOTH must match:
# two independent widths off one verified capture is materially stronger than one
# 7-hex compare — and this pair is what refuses the #3616 cross-lane class (a
# peer lane's perfectly valid summary, recovered by recency, naming a DIFFERENT
# PR's head).
assert_covers() {
  local label="$1" val="$2" full="$3" what="$4" subject="$5" n
  assert_hex_abbrev "$label" "$val" "$what"
  n=${#val}
  if [ "${full:0:n}" != "$val" ]; then
    refuse_no_gate \
      "'$label:' value '$val' in the $what does not match the $subject at $n chars." \
      "$subject: $full" \
      "That run executed against a DIFFERENT tree than the one it must cover." \
      "If the only diff since the gate's anchor is test/docs-only, the route is the" \
      "ANCHORED DELTA PAIR below — a fourth argument, not a repeat full gate."
  fi
}

# assert_clean_tree <what> <value>: the run that block records must have executed
# against a COMMITTED tree (#3648).
#
# WHY THIS IS ENFORCED AND NOT MERELY REPORTED. A gate that ran with `dirty: yes`
# certified sha PLUS uncommitted non-ignored content — not the sha. The gate's tree
# capture pairs a tracked-side diff with `git ls-files --others --exclude-standard`,
# so `dirty: yes` is real uncommitted NON-IGNORED content — tracked edits AND/OR
# untracked files the ignore rules do not exclude — never a gitignored log. The escape is then ordinary: a full gate PASSes
# at HEAD X with edits in the worktree, the edits are discarded or simply never
# committed, X is pushed and merged — and the gate of record covered a tree that
# is NOT the one that lands. `commit:`/`tree-start:` cannot see it: both name X in
# exactly that run.
#
# HOW OFTEN THIS FIRES — MEASURED, WITH ITS POPULATION AND ITS LIMITS (2026-09-01,
# #3648). Census of one box's `/tmp/agent-gate.*` summaries, restricted to blocks
# that could ever BE a gate of record: FULL-gate blocks, deduplicated by `run-id`,
# NOT `nested-under:`, and carrying a canonical `RESULT` token — n=2395, of which
# 1608 are `RESULT: PASS`. Of those 1608 PASSes an affirmative `dirty: no` match
# refuses 26 (~1.6%), broken down by cause so the figure can be re-derived rather
# than inherited: 19 `dirty: yes`, 7 carrying NO `dirty:` field at all, and 0
# `unverified` (all 40 `unverified` blocks in the population are already FAIL).
# So the absent-field arm below is not hypothetical — it is 7 of the 26.
# LIMITS, stated because a number in a comment decays like any other claim: this
# is a SINGLE-BOX `/tmp` census over run dirs of unknown age, blind to runs that
# were pruned, and the fixture exclusion (canonical `RESULT` + no `nested-under:`)
# is a heuristic that removes this repo's own planted near-miss summaries, not a
# guarantee that none survive. Percentages taken over the UNfiltered population
# are unstable for exactly that reason; the absolute counts are not.
#
# The compare is AFFIRMATIVE — `= no`, never `!= yes` — for the same reason the
# RESULT/tree-integrity token compares are: a `!= yes` test is a two-valued
# predicate over a multi-state signal and would hand every unmeasured state
# (`unverified`, an absent field, a future value) the PERMISSIVE branch. An absent
# or unrecognised value therefore REFUSES; it is never skipped and never read as
# clean, exactly as a non-hex `commit:`/`tree-start:` placeholder refuses rather
# than being skipped.
#
# `dirty: unverified` IS A REAL EMITTED VALUE, AND THIS ARM IS DEFENCE IN DEPTH —
# NOT A HOLE THIS CHANGE CLOSES. scripts/agent-gate.sh:8814 emits
# `commit: unverified branch: <b> dirty: unverified` deliberately, when no
# validated tree capture exists (the start capture failed, or there is no worktree
# at the terminal emit) — the run is ALREADY fail-closed there and must not name a
# sha nothing verified. Such a block is therefore refused THREE times over: by
# `RESULT: FAIL`, by the non-hex `commit:` placeholder, and now here. The
# redundancy is deliberate: each of those three is a separate key, and a value
# that means "the state was never measured" must not survive on the strength of
# one neighbouring check (the standing rule that no key may delegate its failure
# to its neighbour).
#
# THERE IS NO ENV OPT-OUT AND NONE MAY BE ADDED. A dirty tree is always
# re-gateable — commit or discard, then re-run — so an escape hatch could only
# buy a vacuous green, which is the shape this whole script exists to refuse.
assert_clean_tree() {
  local what="$1" val="$2" kind="$3"
  # The REMEDY is per-artifact, because the two artifacts are re-produced by
  # DIFFERENT runs (#3648 roborev round 1, finding 1). Telling the operator to
  # "re-run the FULL gate" over a dirty DELTA block contradicts #1892, which
  # mandates `--delta` — never a repeat full gate — for a test/docs-only diff on
  # top of a full PASS. A refusal naming the wrong remedy sends a correct operator
  # down a route doctrine forbids, which is worse than naming none.
  #
  # `kind` is REQUIRED and an unrecognised value REFUSES. It deliberately takes no
  # `${3:-full}` default: a permissive default is how a new call site silently
  # inherits the wrong remedy, and this file's whole discipline is that an
  # unestablished value is never given the benign branch.
  local rerun
  case "$kind" in
    full)  rerun="re-run the FULL gate on the clean tree and pass that summary" ;;
    delta) rerun="re-run the --delta re-certification on the clean tree and pass that summary (the anchor's own full-gate PASS is unaffected)" ;;
    *)
      refuse_no_gate \
        "INTERNAL: assert_clean_tree was called with remedy kind '$kind', which is not" \
        "'full' or 'delta'. Refusing rather than guessing a remedy (#3648)."
      ;;
  esac
  # AMBIGUITY BEFORE VALUE: more than one `dirty:` on the commit: line means the
  # block states the tree's cleanliness twice, and no reading of it is
  # authoritative. Refused BEFORE the `= no` compare, or `dirty: yes dirty: no`
  # would return 0 here on the second token's value (#3648 roborev round 2).
  if [ "${4:-1}" -gt 1 ] 2>/dev/null; then
    refuse_no_gate \
      "The $what has ${4} 'dirty:' fields on its 'commit:' line — AMBIGUOUS, refusing." \
      "A block that states its tree state twice authorises nothing: a 'last one wins'" \
      "reading would let a trailing 'dirty: no' override the real value."
  fi
  if [ "$val" = no ]; then
    return 0
  fi
  if [ -z "$val" ]; then
    refuse_no_gate \
      "The $what records NO 'dirty:' value on its 'commit:' line — nothing was measured" \
      "about whether that run executed against a committed tree, so it cannot certify one." \
      "REMEDY: $rerun."
  fi
  refuse_no_gate \
    "The $what records 'dirty: $val' — the gate of record must be 'dirty: no' (#3648)." \
    "'yes' means that run certified the sha PLUS uncommitted NON-IGNORED content — both" \
    "modified TRACKED files and UNTRACKED files the repo's ignore rules do not exclude," \
    "since the gate's capture pairs a tracked-side diff with" \
    "\`git ls-files --others --exclude-standard\` (so this is never a gitignored log, and it" \
    "is not tracked-only either). Anything that is" \
    "neither 'yes' nor 'no' means the state was never established — and an unestablished" \
    "state is not a clean one. Either way the tree that was gated is not provably the tree" \
    "that will merge: commit:/tree-start: name the same sha in both cases and cannot see it." \
    "REMEDY: commit the edits (or discard them), then $rerun." \
    "There is deliberately NO opt-out: a dirty tree is always" \
    "re-gateable, so an override could only buy a vacuous green."
}

# assert_pass_block <what>: the verdict half every accepted block must satisfy —
# terminated, RESULT: PASS, tree-integrity: PASS, and not a nested sub-gate.
assert_pass_block() {
  local what="$1"
  if [ "$GP_unterminated" != 0 ]; then
    refuse_no_gate \
      "A block in the $what is UNTERMINATED (no exact end marker)." \
      "A truncated summary certifies nothing — the gate may still be running or have died."
  fi

  assert_single_key "$GP_n_result" RESULT "$what"
  # Verdict TOKENS are compared EXACTLY, never by prefix (#3229): a `PASS*` glob
  # accepts `PASSthisNeverRan` and `PASS-MEASUREMENT-DID-NOT-HAPPEN`, i.e. it would
  # check a SPELLING rather than a STATE. awk already gave us the first
  # whitespace-delimited token after the key, so this is a token-exact compare.
  if [ "$GP_v_result" != PASS ]; then
    refuse_no_gate \
      "RESULT verdict token in the $what is '$GP_v_result', not PASS." \
      "INCOMPLETE is the launch-time liveness SENTINEL, not a verdict (#3041): it is" \
      "written when the gate starts (before the slot is even granted) and overwritten" \
      "only at the terminal emit. Such a summary means still running, queued, or died." \
      "PARTIAL is an --only run, which does NOT count as the gate."
  fi

  assert_single_key "$GP_n_ti" tree-integrity "$what"
  if [ "$GP_v_ti" != PASS ]; then
    refuse_no_gate \
      "tree-integrity verdict token in the $what is '$GP_v_ti', not PASS." \
      "A run whose worktree mutated mid-run cannot certify (#2926); PENDING means the" \
      "run never reached its terminal emit, and SKIP means the check never ran."
  fi

  # A NESTED sub-gate (#2874: launched by an enclosing gate, stamped
  # `nested-under: <parent-run-id>`) emits the SAME markers at the SAME tree, so
  # the sha binding provably cannot distinguish it from the real thing — this
  # one affirmative line closes the only wrong-file class the sha compare cannot
  # see. A self-test/sub-gate verdict is about the gate's own machinery, never
  # about this PR.
  if [ "$GP_n_nested" -ne 0 ]; then
    refuse_no_gate \
      "The $what carries a 'nested-under:' line — it is a NESTED sub-gate (#2874)." \
      "A sub-gate spawned by an enclosing gate runs at the SAME tree, so the sha" \
      "binding cannot tell it apart; it certifies the gate's machinery, not this PR."
  fi
}

# --- the FULL gate of record (arg 3) -----------------------------------------
assert_readable_summary "$summary_file" "gate summary"
gate_parse_file "$summary_file" full "gate summary"

if [ "$GP_blocks" -eq 0 ]; then
  refuse_no_gate \
    "The file contains ZERO full-gate blocks (found $GP_lite lite, $GP_delta delta)." \
    "--lite and --delta emit DISTINCT headers; NEITHER is the gate of record:" \
    "  --lite  is fast iteration and is never acceptable here." \
    "  --delta re-certifies a post-full-PASS test/docs-only round — pass the ANCHOR's" \
    "          FULL summary as argument 3 and the delta summary as argument 4." \
    "This is the #3408 failure exactly: many lite PASSes, no full gate."
fi

if [ "$GP_blocks" -gt 1 ]; then
  refuse_no_gate \
    "The file contains $GP_blocks full-gate blocks — AMBIGUOUS." \
    "Refusing rather than picking one (a 'take the last block' rule would let a" \
    "stale or foreign run certify this merge). Point at ONE run's summary file."
fi

# Belt for the header separation above: the FULL gate emits NO `MODE:` line;
# --lite and --delta each emit one naming themselves. (An `--only` run emits the
# FULL markers with a LOWERCASE `mode: PARTIAL (--only …)` line, which this
# case-sensitive check deliberately does NOT catch — that run is refused by the
# `RESULT: PARTIAL` compare above, which is the property that matters.)
if [ "$GP_n_mode" -ne 0 ]; then
  refuse_no_gate \
    "The full-gate block carries a MODE: line — the FULL gate emits none." \
    "This block was produced by (or doctored from) a lite/delta run."
fi

assert_pass_block "full-gate block"

assert_single_key "$GP_n_commit" commit "full-gate block"
assert_single_key "$GP_n_ts" tree-start "full-gate block"
full_commit="$GP_v_commit"
full_ts="$GP_v_ts"
full_dirty="$GP_v_dirty"
full_ndirty="$GP_n_dirty"

if [ -z "$delta_file" ]; then
  # CASE A — DIRECT: the gate of record ran on the merged tree itself.
  assert_covers commit "$full_commit" "$certified" "full-gate block" "certified sha"
  assert_covers tree-start "$full_ts" "$certified" "full-gate block" "certified sha"
else
  # CASE B — ANCHORED DELTA (#1892). The full block is the ANCHOR: its sha need
  # not be the certified sha, but it must still be a real, verifiable sha, and
  # the delta block must name exactly it.
  assert_hex_abbrev commit "$full_commit" "full-gate block"
  assert_hex_abbrev tree-start "$full_ts" "full-gate block"

  assert_readable_summary "$delta_file" "delta summary"
  gate_parse_file "$delta_file" delta "delta summary"

  if [ "$GP_blocks" -eq 0 ]; then
    refuse_no_gate \
      "The fourth argument holds ZERO delta blocks (found $GP_full full, $GP_lite lite)." \
      "It must be the AGENT_GATE_SUMMARY_FILE of a 'scripts/agent-gate.sh --delta' run" \
      "('==== AGENT-GATE DELTA SUMMARY ====' — a DISTINCT header, by construction)."
  fi
  if [ "$GP_blocks" -gt 1 ]; then
    refuse_no_gate \
      "The fourth argument holds $GP_blocks delta blocks — AMBIGUOUS." \
      "Point at ONE run's summary file; picking one would let a stale run re-certify."
  fi

  # The INVERSE of the full block's belt: here a `MODE: delta` line is REQUIRED
  # and asserted AFFIRMATIVELY. A delta block always carries it
  # (scripts/agent-gate.sh SUMMARY_MODE_LINE), so its absence means the block was
  # doctored or is not what its header claims.
  assert_single_key "$GP_n_mode" MODE "delta block"
  if [ "$GP_v_mode" != delta ]; then
    refuse_no_gate \
      "The delta block's MODE token is '$GP_v_mode', not 'delta'." \
      "A --delta run stamps 'MODE: delta (TEST/DOCS-ONLY RE-CERTIFICATION …)'; anything" \
      "else is a different mode wearing the delta header."
  fi

  assert_pass_block "delta block"

  # delta-anchor: must name the FULL block above. The gate emits
  # 'delta-anchor: <40-hex> (full-gate PASS commit)' from `git rev-parse
  # --verify`, and 'delta-anchor: <ref> (UNRESOLVED)' on the ERROR path — the
  # latter MUST refuse (it certifies nothing about any tree).
  assert_single_key "$GP_n_anchor" delta-anchor "delta block"
  if [ "$GP_anchor_unresolved" -ne 0 ]; then
    refuse_no_gate \
      "The delta block's 'delta-anchor:' is (UNRESOLVED) — the anchor did not resolve" \
      "to a commit, so that run re-certified nothing against the gate of record."
  fi
  case "$GP_v_anchor" in
    ''|*[!0-9a-f]*)
      refuse_no_gate \
        "The delta block's 'delta-anchor:' value '$GP_v_anchor' is not lowercase hex."
      ;;
  esac
  if [ "${#GP_v_anchor}" -ne 40 ]; then
    refuse_no_gate \
      "The delta block's 'delta-anchor:' value '$GP_v_anchor' is ${#GP_v_anchor} hex chars," \
      "not the full 40 the gate resolves it to (git rev-parse --verify of the anchor)."
  fi
  delta_anchor="$GP_v_anchor"
  # Both of the anchor block's independent widths must prefix that anchor sha.
  assert_covers commit "$full_commit" "$delta_anchor" "full-gate block" "delta block's anchor sha"
  assert_covers tree-start "$full_ts" "$delta_anchor" "full-gate block" "delta block's anchor sha"

  # ...and the delta run's OWN provenance must cover the tree being merged.
  assert_single_key "$GP_n_commit" commit "delta block"
  assert_single_key "$GP_n_ts" tree-start "delta block"
  assert_covers commit "$GP_v_commit" "$certified" "delta block" "certified sha"
  assert_covers tree-start "$GP_v_ts" "$certified" "delta block" "certified sha"
  delta_commit="$GP_v_commit"
  delta_ts="$GP_v_ts"
  delta_dirty="$GP_v_dirty"
  delta_ndirty="$GP_n_dirty"
  # The delta run's OWN tree must be clean too: it is the run that covers the
  # tree being merged, so a dirty delta re-cert certifies edits that are not in
  # the PR exactly as a dirty full gate does.
  assert_clean_tree "delta block" "$delta_dirty" delta "$delta_ndirty"
fi

# `dirty:` is REPORTED **AND ENFORCED** (#3648, replacing the deferral note this
# line used to carry). In CASE B this is the ANCHOR's own tree: a full PASS taken
# on a dirty tree anchors the whole chain on a tree nobody can reconstruct, so
# both blocks are held to the same requirement. The evidence line below still
# prints the value — after this call it can only ever read `dirty: no`, which is
# the affirmative record that the check RAN.
assert_clean_tree "full-gate block" "$full_dirty" full "$full_ndirty"

# ---------------------------------------------------------------------------
# THE ADVISORY IS MEASURED **BEFORE** THE HEAD CHECK (#3650, roborev job 250)
# ---------------------------------------------------------------------------
# The advisory is bounded at ADVISORY_TIMEOUT_SECS + ADVISORY_KILL_GRACE (65s).
# Running it AFTER the `gh pr view` head/state check would leave up to 65s
# between the instant the head was verified and the instant `PREMERGE: OK` is
# emitted -- so a push inside that window would leave this script emitting OK for
# a sha that is no longer the PR head, which is precisely the stale-head merge
# #2456 exists to refuse. The fix is ordering, not a re-check: the advisory is
# MEASURED here and PRINTED later in its original position, so the gh head/state
# check remains the LAST thing that happens before OK.
#
# Capturing changes no output: every line of the report is written to stdout by
# `print_base_staleness_advisory`, and printing it at the original call site keeps
# the order identical -- which matters, because the `PREMERGE: SCOPE ... ADVISORY
# lines below` clause asserts the advisory appears BELOW it.
#
# Cost, accepted: on a refusal path the 65s is already spent. Correctness of the
# approval beats latency of a refusal, and nothing is printed on those paths.
advisory_out=$(print_base_staleness_advisory)

# ---------------------------------------------------------------------------
# PR HEAD + STATE (#2456)
# ---------------------------------------------------------------------------

# Fetch head + state in ONE call, extracted by gh's built-in jq into two
# whitespace-separated tokens: "<headRefOid> <state>". Because gh runs the jq
# expression, its JSON serialization (compact vs pretty) is irrelevant. On any
# gh/network failure -> exit 3 (fail closed).
if ! out=$(gh pr view "$pr" --repo "$repo" --json headRefOid,state \
  --jq '.headRefOid + " " + .state' 2>/dev/null); then
  printf '========================================================\n' >&2
  printf 'PREMERGE: GH-FAILURE\n' >&2
  printf '  gh pr view %s --repo %s failed (auth/network/no-such-PR).\n' "$pr" "$repo" >&2
  printf '  Cannot verify the PR head — refusing to merge (fail closed).\n' >&2
  printf '========================================================\n' >&2
  exit 3
fi

# Split the two tokens. Empty or malformed --jq output -> exit 3 (fail closed).
actual=$(printf '%s' "$out" | awk '{print $1}')
state=$(printf '%s' "$out" | awk '{print $2}')
actual=$(printf '%s' "$actual" | tr '[:upper:]' '[:lower:]')

if [ -z "$actual" ] || [ -z "$state" ]; then
  printf '========================================================\n' >&2
  printf 'PREMERGE: GH-FAILURE\n' >&2
  printf '  Could not parse headRefOid/state from gh --jq output.\n' >&2
  printf '  Refusing to merge (fail closed).\n' >&2
  printf '========================================================\n' >&2
  exit 3
fi

if [ "$state" != "OPEN" ]; then
  printf '========================================================\n' >&2
  printf 'PREMERGE: NOT-OPEN\n' >&2
  printf '  PR #%s state is "%s" (expected OPEN).\n' "$pr" "$state" >&2
  printf '  The PR is already closed or merged — do NOT merge again.\n' >&2
  printf '========================================================\n' >&2
  exit 2
fi

if [ "$actual" != "$certified" ]; then
  printf '========================================================\n' >&2
  printf 'PREMERGE: STALE-HEAD — REFUSING TO MERGE\n' >&2
  printf '  certified SHA: %s\n' "$certified" >&2
  printf '  actual   head: %s\n' "$actual" >&2
  printf '  head moved since certification — the gate of record no longer\n' >&2
  printf '  covers this PR; re-certify before merge.\n' >&2
  printf '========================================================\n' >&2
  exit 2
fi

printf 'PREMERGE: OK %s\n' "$certified"
# Scope clause (#3650) — printed on EVERY success so `GATE-OF-RECORD` can never be
# read as "certified against main". See residual 3 in the header.
printf 'PREMERGE: SCOPE this proves a full gate PASSed on THIS tree (%s); it does NOT prove\n' \
  "$certified"
printf 'PREMERGE: SCOPE the tree was certified against current main (#3650) — a squash-merge\n'
printf 'PREMERGE: SCOPE composes this diff with main tip, which no gate here has executed.\n'
# One added SCOPE line pointing at the advisory (#3650 slice 1). The three lines
# above are RETAINED verbatim: slice 1 ships INFORMATION, not the merge-result
# gate, so the disclaimer they carry is still true.
printf 'PREMERGE: SCOPE the PREMERGE: ADVISORY lines below measure that gap (non-blocking, #3650 slice 1).\n'
# Printed here, MEASURED earlier (see the note above the head check): the
# advisory's 65s bound must not sit between the head check and OK.
if [ -n "$advisory_out" ]; then
  printf '%s\n' "$advisory_out"
fi
printf 'PREMERGE: GATE-OF-RECORD commit: %s tree-start: %s tree-integrity: PASS dirty: %s summary: %s\n' \
  "$full_commit" "$full_ts" "$full_dirty" "$summary_file"
if [ -n "$delta_file" ]; then
  printf 'PREMERGE: DELTA-RECERT anchor: %s commit: %s tree-start: %s tree-integrity: PASS dirty: %s summary: %s\n' \
    "$delta_anchor" "$delta_commit" "$delta_ts" "$delta_dirty" "$delta_file"
fi
exit 0
