#!/usr/bin/env bash
# gate-liveness.sh — answer "is my gate running, or was it reaped?" from artifacts
# alone, with no access to the box's process table (issue #3473 AC4).
#
# THE DEFECT THIS CLOSES
# ----------------------
# `RESULT: INCOMPLETE (gate did not finish)` is written into the summary file ONCE, at
# launch, before the #1825 slot is even granted (#3041). It is therefore the artifact
# of THREE states at once — queued, running, killed — and the correct completion probe
# (`grep -qE 'RESULT: (PASS|FAIL)'`) says "not finished" for all three. A lane whose
# gate was reaped at the #3473 ceiling and a lane whose gate is 30 minutes from a PASS
# read IDENTICAL text. Resolving them required a human running `ps` on the box, which
# is exactly what made the coordination lead the fleet's only gate-runner.
#
# A one-shot placeholder cannot carry liveness — nothing about it decays. So the gate
# now also runs scripts/lib/gate-heartbeat.sh, which rewrites a heartbeat file every
# `interval` seconds for as long as the gate process lives. This script reads the two
# artifacts together and reports ONE of four states.
#
# EVERY POSITIVE VERDICT IS AN AFFIRMATIVE MEASUREMENT (CLAUDE.md, #3229)
# ----------------------------------------------------------------------
# `RUNNING` is never inferred from the ABSENCE of a bad signal — it requires a beat
# that is present, carries THIS run's run-id, and is fresh. `STALLED` likewise requires a
# present, run-id-matching, STALE beat. Everything unmeasurable — no heartbeat at all, a
# foreign run-id, an unparseable beat, a beat dated in the future — is `UNKNOWN` with a
# NAMED cause, never folded into either real answer. In particular "no heartbeat file" is
# NOT reported as STALLED: a gate predating this mechanism, or one whose summary path is
# unwritable, produces the same absence, and reporting those as a stall would be the
# fail-open shape one level down.
#
#   STATUS     exit  meaning
#   COMPLETE     0   the summary carries a terminal verdict for this run
#   RUNNING      2   no verdict yet, and this run beat within the freshness window
#   STALLED      3   no verdict, and this run has published no liveness for a while
#   UNKNOWN      4   cannot tell; the printed cause says what was unmeasurable
#
# STALLED is deliberately NOT "the gate is dead" — see the long note where it is returned.
# It is "no liveness published", which is what two local files can actually establish.
#   (usage)     64
#
# The exit code is a convenience for scripts; the `gate-liveness:` line is the answer.
# Never read this through a pipe expecting $? — a pipeline's status is its last stage.
#
# Usage:
#   bash scripts/gate-liveness.sh <summary-file> [--run-id <id>] [--heartbeat <path>]
#
# --run-id BINDS THE ANSWER TO A RUN. Pass it whenever you know it (the gate prints it
# and every SUMMARY block carries it): without it, a block or beat left by a CONCURRENT
# PEER in the same checkout answers about the peer's gate, not yours — the same reader
# hazard #2874 documents for the summary file.
set -uo pipefail

SUMMARY=""; WANT_RUN_ID=""; HB=""
while [ $# -gt 0 ]; do
  case "$1" in
    --run-id)    WANT_RUN_ID="${2:?--run-id needs a value}"; shift 2 ;;
    --heartbeat) HB="${2:?--heartbeat needs a path}"; shift 2 ;;
    -h|--help)   sed -n '2,50p' "$0"; exit 0 ;;
    -*)          echo "gate-liveness: unknown option '$1'" >&2; exit 64 ;;
    *)           if [ -n "$SUMMARY" ]; then
                   echo "gate-liveness: unexpected extra argument '$1'" >&2; exit 64
                 fi
                 SUMMARY="$1"; shift ;;
  esac
done
if [ -z "$SUMMARY" ]; then
  echo "gate-liveness: a summary-file path is required" >&2
  echo "usage: bash scripts/gate-liveness.sh <summary-file> [--run-id <id>] [--heartbeat <path>]" >&2
  exit 64
fi
# The heartbeat lives beside the summary under a fixed suffix, so a caller that chose
# the summary path in advance knows the heartbeat path in advance too — the same
# contract that makes the summary file recoverable (#1175).
[ -n "$HB" ] || HB="$SUMMARY.heartbeat"

# BOTH artifacts are read ONCE, into a snapshot, and every field is parsed from that
# snapshot — never by re-opening the file per field (roborev job 155, Medium).
#
# WHY: these are SHARED paths that peers replace ATOMICALLY (the beater writes a sibling
# temp and renames; emit_summary rewrites the summary). A per-field re-open therefore
# samples a possibly DIFFERENT version of the file for each field, so one run's `run-id:`
# could be combined with another run's `RESULT:` or a fresher `beat-epoch:` — producing a
# confident COMPLETE or RUNNING about a run that never had that state. That is precisely
# the cross-run confusion the #2874 reader contract exists to prevent, reintroduced by the
# I/O pattern rather than by the logic.
#
# HOW STRONG THIS ACTUALLY IS, stated precisely, because the first version over-claimed
# (roborev job 160, Medium). A single `cat` is a single `open()`, so it reads one inode
# start to finish. That gives a genuinely atomic snapshot ONLY for a writer that publishes
# by RENAME — which the heartbeat does (sibling temp + `mv`), so a rename landing mid-read
# swaps the NAME, not our open file.
#
# The SUMMARY is NOT published that way: agent-gate.sh writes it in place with `>`, i.e.
# O_TRUNC followed by sequential writes. So a reader can legitimately observe a PREFIX of a
# block being written. It cannot observe a blend of two versions (O_TRUNC resets the length
# and the new content is written forward), and that is the property that makes this
# tractable: a partial block is missing its tail, so the mandatory end-marker check on the
# COMPLETE path (below) rejects it. A torn read therefore degrades to UNKNOWN, never to a
# wrong COMPLETE.
#
# `_slurp_settled` then converts the COMMON case of a torn read — we caught a write in
# progress — into a correct answer, by re-reading once when the framing is incomplete.
# Genuinely truncated artifacts (a killed gate, ENOSPC) still land on UNKNOWN, because a
# second read of a permanently short file is identical to the first.
#
# Making the summary write itself atomic is the root fix and is deliberately NOT done here:
# emit_summary is load-bearing for #1175's write-failure detection and #2874's no-clobber
# contract, and changing its publish mechanism to temp+rename is a change to the gate of
# record that deserves its own issue rather than a ride-along in this one.
#
# _slurp <file> — the file's contents, or empty when unreadable.
_slurp() { cat -- "$1" 2>/dev/null; }

# _slurp_settled <file> — like _slurp, but if the text does not look like a COMPLETE
# summary block, read it once more and prefer the completed version. Bounded to exactly one
# retry: this distinguishes "caught mid-write" (the retry completes) from "permanently
# truncated" (the retry is identical), and cannot loop.
_slurp_settled() {
  local t
  t=$(_slurp "$1")
  if ! printf '%s\n' "$t" | grep -qE '^==== END AGENT-GATE( LITE| DELTA)? SUMMARY ====$'; then
    sleep 0.2
    local t2
    t2=$(_slurp "$1")
    if printf '%s\n' "$t2" | grep -qE '^==== END AGENT-GATE( LITE| DELTA)? SUMMARY ====$'; then
      t="$t2"
    fi
  fi
  printf '%s' "$t"
}

# _field <text> <key> — first "key: value" line's value in <text>, or empty.
_field() {
  local text="$1" k="$2" line
  line=$(printf '%s\n' "$text" | grep -m1 "^$k: ") || return 0
  printf '%s' "${line#"$k": }"
}

verdict() { # verdict <STATUS> <exit> <detail>
  echo "gate-liveness: $1 ($3)"
  echo "summary: $SUMMARY"
  echo "heartbeat: $HB"
  [ -n "$WANT_RUN_ID" ] && echo "expected-run-id: $WANT_RUN_ID"
  exit "$2"
}

# ---- the summary side: is there a verdict at all? -------------------------------
# NOTE on the file predicates below (CLAUDE.md's predicate-family rule): every `[ -f ]` /
# `[ -r ]` is TWO-valued, so it must collapse "cannot tell" (an unsearchable parent
# directory, a transient FS error) onto one of its two answers. Every one here collapses
# onto **UNKNOWN**, the conservative side — never onto a verdict. The rule warns against
# collapsing onto the PERMISSIVE answer; this is the other direction, deliberately.
# The cause text is worded to match: "not readable as a file" states what was observed,
# not "nothing has been written", which would assert a fact the predicate cannot establish.
if [ ! -f "$SUMMARY" ]; then
  verdict UNKNOWN 4 "no-summary-artifact; $SUMMARY is not readable as a regular file (never written, or its location is not reachable from here)"
fi
if [ ! -r "$SUMMARY" ]; then
  verdict UNKNOWN 4 "summary-unreadable; $SUMMARY exists but cannot be read"
fi
SUM_TEXT=$(_slurp_settled "$SUMMARY")
SUM_RUN_ID=$(_field "$SUM_TEXT" run-id)
# #2874 reader contract: a block bearing a FOREIGN run-id is a peer's, and that holds
# for a PASS just as much as for an INCOMPLETE. Refuse to answer about someone else's
# gate rather than report their state as ours.
#
# When the caller NAMED a run, a MISSING `run-id:` is a refusal, not a pass (roborev job
# 155, Medium). The earlier form also required `-n "$SUM_RUN_ID"` before comparing, so a
# summary with no run-id line skipped validation entirely and its terminal verdict was
# attributed to the requested run — a permissive branch keyed on the ABSENCE of the bad
# signal, which is the exact shape CLAUDE.md forbids (#3229). The binding is only a
# guarantee if it is unconditional: caller named a run ⇒ the artifact must AFFIRMATIVELY
# say it is that run.
if [ -n "$WANT_RUN_ID" ]; then
  if [ -z "$SUM_RUN_ID" ]; then
    verdict UNKNOWN 4 "summary-no-run-id; $SUMMARY carries no 'run-id:' line, so it cannot be attributed to run '$WANT_RUN_ID'"
  fi
  if [ "$SUM_RUN_ID" != "$WANT_RUN_ID" ]; then
    verdict UNKNOWN 4 "summary-run-id-mismatch; $SUMMARY carries run-id '$SUM_RUN_ID', not '$WANT_RUN_ID' — a live peer owns that path"
  fi
fi
RESULT_LINE=$(printf '%s\n' "$SUM_TEXT" | grep -m1 '^RESULT: ' || true)
# The TERMINAL verdict set, enumerated from agent-gate.sh rather than assumed to be
# two values. `PARTIAL` (an --only run), `ERROR` and `REFUSED` (a --delta entry
# refusal) are every bit as terminal as PASS/FAIL: the gate reached a decision and
# stopped, so no amount of waiting will change the artifact. This reader answers "is
# there a verdict", NOT "is it green" — a caller that needs green reads the summary.
#
# INCOMPLETE is the ONLY non-terminal value, and it is the entire reason this script
# exists. It also has a `(foreign)` variant (#2874), which is likewise not a verdict.
#
# Closed grammar (#3229): an unrecognised value is UNKNOWN, never assumed benign and
# never assumed terminal. If a future gate adds a sixth verdict, a lane reads UNKNOWN
# and asks a human — the safe direction — instead of this reader guessing.
#
# The value is reduced to its VERDICT TOKEN (up to the first space) and matched EXACTLY
# (roborev job 155, Low). A prefix glob — `'RESULT: PASS'*` — accepts `RESULT: PASSENGER`
# and `RESULT: FAILURE`, i.e. it checks a SPELLING rather than a STATE, so the "closed
# grammar" would have been open at exactly the place it claimed to be shut. CLAUDE.md
# records this same defect in the roborev wrapper's own verdict scan (`PASS*` accepting
# `PASSthisNeverRan`); this was that mistake reproduced one layer down.
if [ -z "$RESULT_LINE" ]; then
  verdict UNKNOWN 4 "no-result-line; $SUMMARY has no 'RESULT:' line (truncated or not a gate summary)"
fi
RESULT_VALUE="${RESULT_LINE#RESULT: }"
RESULT_TOKEN="${RESULT_VALUE%% *}"
# A TERMINAL verdict is only believable if the block that carries it is COMPLETE
# (roborev job 157, Medium). emit_summary writes the block with a single `>` redirection
# and then verifies its own end marker precisely because that write can be cut short — by
# ENOSPC, or by the gate being killed between the `RESULT:` line and the closing marker.
# A truncated artifact is PERMANENT: nothing will ever finish it, so a reader that
# accepted it would report a verdict the gate never actually published.
#
# Asymmetric on purpose: this is required only on the COMPLETE path. An INCOMPLETE
# summary already falls through to the heartbeat, which is the conservative direction, and
# a block truncated before its `RESULT:` line has no result to misread — it lands on
# `no-result-line` above.
_has_line() { printf '%s\n' "$SUM_TEXT" | grep -qE "$1"; }
_START_RE='^==== AGENT-GATE( LITE| DELTA)? SUMMARY ====$'
_END_RE='^==== END AGENT-GATE( LITE| DELTA)? SUMMARY ====$'
case "$RESULT_TOKEN" in
  PASS|FAIL|PARTIAL|ERROR|REFUSED)
    if ! _has_line "$_START_RE"; then
      verdict UNKNOWN 4 "summary-no-start-marker; '$RESULT_LINE' is present but the block has no '==== AGENT-GATE … SUMMARY ====' opener — this is not a complete gate summary"
    fi
    if ! _has_line "$_END_RE"; then
      verdict UNKNOWN 4 "summary-truncated; '$RESULT_LINE' is present but the closing '==== END AGENT-GATE … SUMMARY ====' marker is not — the write was cut short (kill or ENOSPC) and will never complete, so this verdict was never published"
    fi
    verdict COMPLETE 0 "the summary carries a terminal verdict — $RESULT_VALUE" ;;
  INCOMPLETE)
    : ;;  # the interesting case: fall through to the heartbeat
  *)
    verdict UNKNOWN 4 "unrecognised-result; verdict token '$RESULT_TOKEN' (from '$RESULT_LINE') is not a value this reader knows" ;;
esac

# ---- the heartbeat side: affirmative liveness, or an affirmative death ----------
if [ ! -f "$HB" ]; then
  verdict UNKNOWN 4 "no-heartbeat-artifact; the summary is INCOMPLETE and no beat exists at $HB (a gate predating the heartbeat, or an unwritable path) — absence is NOT evidence of death"
fi
if [ ! -r "$HB" ]; then
  verdict UNKNOWN 4 "heartbeat-unreadable; $HB exists but cannot be read"
fi
HB_TEXT=$(_slurp "$HB")
HB_RUN_ID=$(_field "$HB_TEXT" run-id)
if [ -z "$HB_RUN_ID" ]; then
  verdict UNKNOWN 4 "heartbeat-no-run-id; $HB carries no 'run-id:' line, so it cannot be attributed to any run"
fi
if [ -n "$WANT_RUN_ID" ]; then
  if [ "$HB_RUN_ID" != "$WANT_RUN_ID" ]; then
    verdict UNKNOWN 4 "heartbeat-run-id-mismatch; the beat at $HB is run '$HB_RUN_ID', not '$WANT_RUN_ID'"
  fi
elif [ -n "$SUM_RUN_ID" ] && [ "$HB_RUN_ID" != "$SUM_RUN_ID" ]; then
  # No caller-supplied id, but the two artifacts disagree: they describe different
  # runs, so neither can be read as evidence about the other.
  verdict UNKNOWN 4 "heartbeat-summary-run-id-disagree; summary is run '$SUM_RUN_ID' but the beat is run '$HB_RUN_ID'"
fi

HB_EPOCH=$(_field "$HB_TEXT" beat-epoch)
case "$HB_EPOCH" in
  ''|*[!0-9]*) verdict UNKNOWN 4 "heartbeat-unparseable-epoch; 'beat-epoch: $HB_EPOCH' is not an integer" ;;
esac
HB_INTERVAL=$(_field "$HB_TEXT" interval)
case "$HB_INTERVAL" in
  ''|*[!0-9]*) verdict UNKNOWN 4 "heartbeat-unparseable-interval; 'interval: $HB_INTERVAL' is not an integer" ;;
esac
[ "$HB_INTERVAL" -ge 1 ] || verdict UNKNOWN 4 "heartbeat-bad-interval; 'interval: $HB_INTERVAL' must be >= 1"

# The staleness window is derived from the beat's OWN declared interval, so this
# reader holds no duplicate of the gate's beat period and cannot drift from it. Three
# missed beats, with a 90s floor: a gate on a loaded box (the #1825 cap admits one
# gate, but --lite runs and cargo take no slot) can be descheduled for a while, and a
# a false STALLED is the more expensive error — it would send a lane off to re-run a gate
# that was about to PASS.
STALE_AFTER=$(( HB_INTERVAL * 3 ))
[ "$STALE_AFTER" -ge 90 ] || STALE_AFTER=90

NOW=$(date +%s)
AGE=$(( NOW - HB_EPOCH ))
# A beat dated in the FUTURE is unmeasurable, not fresh — otherwise a clock step (or a
# hand-edited artifact) would read as RUNNING indefinitely. One interval of slop
# absorbs ordinary clock jitter between the beater's write and this read.
if [ "$AGE" -lt $(( -HB_INTERVAL )) ]; then
  verdict UNKNOWN 4 "heartbeat-in-the-future; beat-epoch $HB_EPOCH is $(( -AGE ))s ahead of this host's clock"
fi
[ "$AGE" -ge 0 ] || AGE=0

HB_PID=$(_field "$HB_TEXT" gate-pid)
HB_SEQ=$(_field "$HB_TEXT" beat-seq)
HB_CHECK=$(_field "$HB_TEXT" parent-check)
_where="run-id $HB_RUN_ID, gate-pid ${HB_PID:-unknown}, beat ${HB_SEQ:-?}, age ${AGE}s, window ${STALE_AFTER}s"
# parent-check declares HOW the beater verifies its gate on the gate's own host. Surfaced as
# a DIAGNOSTIC only: no verdict here depends on it (see the descope note below).
[ -n "$HB_CHECK" ] && _where="$_where, parent-check $HB_CHECK"

if [ "$AGE" -le "$STALE_AFTER" ]; then
  verdict RUNNING 2 "the gate beat ${AGE}s ago — it is alive and has not reached a verdict yet; $_where"
fi

# ---- a stale beat means NO LIVENESS, and that is ALL it is claimed to mean -----------
# DESCOPED DELIBERATELY (#3473). This used to report `REAPED` — a positive claim that the
# gate PROCESS IS DEAD — and four review rounds each found another way that claim was
# unsound: a stale beat alone (a beater can die under a live gate); inspecting the reader's
# own /proc without proving it was the gate's host (a live remote gate on shared storage
# read as dead); and matching hostnames not proving machine identity (two boxes can share a
# hostname, so a differing boot-id was misread as a reboot). Each fix was correct about the
# case in front of it, and the list did not close — because proving a process is dead means
# proving a negative about a machine you may not even be on.
#
# So the claim is removed rather than defended a fourth time. `STALLED` says exactly what
# the artifacts support and no more: **this run has published no liveness for N seconds.**
# It asserts nothing about whether the process exists, and needs no pid, no /proc, no host
# identity and no boot identity — so it is correct on every host, including the macOS/BSD
# gate hosts where /proc does not exist and where the previous version's REAPED cases had
# already become a deterministic test failure.
#
# The lane's real question is "should I keep waiting?", and STALLED answers it. What is lost
# is "definitely dead, re-run now"; what replaces it needs no process inspection at all — the
# gate relaunches its beater at every component boundary, so a live gate whose beater alone
# died RECOVERS to RUNNING within one component. Re-read before acting; if it is still
# STALLED after a component's worth of time, treat the gate as gone.
verdict STALLED 3 "no liveness for ${AGE}s (window ${STALE_AFTER}s) — this run has stopped publishing beats. This is NOT a claim that the process is dead: a beater can die under a live gate, and the gate relaunches it at the next component boundary. Re-read shortly; if it is still STALLED after a component's worth of time (the longest component is ~850s), treat the gate as gone and relaunch it. $_where"
