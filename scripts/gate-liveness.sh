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
# that is present, carries THIS run's run-id, and is fresh. `REAPED` likewise requires
# a present, run-id-matching, STALE beat. Everything unmeasurable — no heartbeat at
# all, a foreign run-id, an unparseable beat, a beat dated in the future — is
# `UNKNOWN` with a NAMED cause, never folded into either real answer. In particular
# "no heartbeat file" is NOT reported as REAPED: a gate predating this mechanism, or
# one whose summary path is unwritable, produces the same absence, and a watchdog that
# declared those dead would be the fail-open shape one level down.
#
#   STATUS     exit  meaning
#   COMPLETE     0   the summary carries a real verdict (PASS or FAIL) for this run
#   RUNNING      2   no verdict yet, and the gate beat within the freshness window
#   REAPED       3   no verdict, and the gate stopped beating — it is not coming back
#   UNKNOWN      4   cannot tell; the printed cause says what was unmeasurable
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
# false REAPED is the more expensive error — it would send a lane off to re-run a gate
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
# parent-check declares HOW the beater verified its gate: 'starttime' is reuse-proof,
# 'kill0' is not. Surfaced so a reader is told which guarantee it is getting (#3229).
[ -n "$HB_CHECK" ] && _where="$_where, parent-check $HB_CHECK"

if [ "$AGE" -le "$STALE_AFTER" ]; then
  verdict RUNNING 2 "the gate beat ${AGE}s ago — it is alive and has not reached a verdict yet; $_where"
fi

# ---- a STALE beat is not by itself evidence of death (roborev job 157, Medium) ------
# The beater is supervised only at COMPONENT BOUNDARIES, and components run for minutes
# (tooling-tests: 687-849s). If the beater alone dies mid-component — a stray signal, an
# OOM reap of the smallest process — the beat goes stale under a PERFECTLY LIVE gate, and
# reporting REAPED there is a FALSE DEATH: the caller re-runs a gate that was about to
# PASS, which is the expensive direction and precisely what this script exists to prevent.
#
# So REAPED, like RUNNING, must be an AFFIRMATIVE measurement: the gate process itself has
# to be shown gone. The beat names `gate-pid`, and `gate-starttime` pins that pid's
# identity so a RECYCLED pid cannot pose as the living gate.
#
# This corroboration is only possible on the gate's OWN host. Where it cannot be done the
# answer is UNKNOWN with the reason named — never REAPED on a guess, and never RUNNING
# either.
HB_STARTTIME=$(_field "$HB_TEXT" gate-starttime)
# HOST GATE on the /proc corroboration (roborev job 160, Medium). A pid means nothing off
# the machine that owns it, and these artifacts may be read across a shared filesystem — so
# inspecting OUR /proc for the gate's pid could report a live REMOTE gate as REAPED, or
# "corroborate" against an unrelated local process holding that pid.
#
# The earlier version's comment asserted "only possible on the gate's OWN host" and then
# never CHECKED the host — a rule stated rather than enforced, which is the same defect
# class as the rest of this issue.
HB_HOST=$(_field "$HB_TEXT" host)
HB_BOOT=$(_field "$HB_TEXT" boot-id)
MY_HOST=$(uname -n 2>/dev/null || echo unknown)
MY_BOOT=$(cat /proc/sys/kernel/random/boot_id 2>/dev/null || echo "")
_same_machine=no
_reboot_since_beat=no
if [ -n "$HB_BOOT" ] && [ "$HB_BOOT" != unavailable ] && [ -n "$MY_BOOT" ]; then
  if [ "$HB_BOOT" = "$MY_BOOT" ]; then
    _same_machine=yes
  elif [ -n "$HB_HOST" ] && [ "$HB_HOST" = "$MY_HOST" ]; then
    # Same hostname, DIFFERENT kernel boot: this machine rebooted since the beat, so every
    # process from the previous boot is gone. That is affirmative evidence, not a puzzle.
    _reboot_since_beat=yes
  fi
fi
if [ "$_reboot_since_beat" = yes ]; then
  verdict REAPED 3 "the gate stopped beating ${AGE}s ago and this host has REBOOTED since (boot-id $MY_BOOT != $HB_BOOT) — every process from that boot is gone; re-run it. $_where"
fi
if [ "$_same_machine" != yes ]; then
  verdict UNKNOWN 4 "heartbeat-foreign-host; the beat is ${AGE}s stale but was written on host '${HB_HOST:-unknown}' (boot-id ${HB_BOOT:-absent}) and this is '$MY_HOST' (boot-id ${MY_BOOT:-unavailable}) — a pid cannot be inspected across machines, so the gate's death cannot be confirmed from here. $_where"
fi
_proc_starttime() {
  local pid="$1" raw rest
  raw=$(cat "/proc/$pid/stat" 2>/dev/null) || return 1
  rest="${raw##*) }"
  # shellcheck disable=SC2086
  set -- $rest
  [ $# -ge 20 ] || return 1
  printf '%s' "${20}"
}
case "$HB_PID" in
  ''|*[!0-9]*)
    verdict UNKNOWN 4 "heartbeat-no-gate-pid; the beat is ${AGE}s stale but names no usable 'gate-pid:', so the gate's death cannot be confirmed. $_where" ;;
esac
if [ -d /proc/1 ] && [ -n "$HB_STARTTIME" ]; then
  _now_st=$(_proc_starttime "$HB_PID" || true)
  if [ -z "$_now_st" ]; then
    verdict REAPED 3 "the gate stopped beating ${AGE}s ago AND pid $HB_PID no longer exists — it was killed and will never write a verdict; re-run it. $_where"
  fi
  if [ "$_now_st" != "$HB_STARTTIME" ]; then
    verdict REAPED 3 "the gate stopped beating ${AGE}s ago AND pid $HB_PID has been RECYCLED by a different process (start time $_now_st != $HB_STARTTIME) — the gate is gone; re-run it. $_where"
  fi
  verdict UNKNOWN 4 "beater-died-gate-alive; the beat is ${AGE}s stale (window ${STALE_AFTER}s) but gate pid $HB_PID IS STILL ALIVE and is the same process — the LIVENESS SIGNAL died, not the gate. Do NOT re-run: wait, and re-read at the next component boundary, where the gate relaunches its beater. $_where"
fi
# Same machine (proven above), but no /proc or no published start time: `kill -0` still
# distinguishes "pid gone" from "pid present", it just cannot rule out reuse. Absence is
# affirmative enough for REAPED; presence is not affirmative enough for anything.
if kill -0 "$HB_PID" 2>/dev/null; then
  verdict UNKNOWN 4 "beater-died-gate-maybe-alive; the beat is ${AGE}s stale but pid $HB_PID still exists and this host cannot pin its identity (no /proc, or the beat carries no 'gate-starttime:'), so it may be the gate or a recycled pid. Not re-running on a guess. $_where"
fi
verdict REAPED 3 "the gate stopped beating ${AGE}s ago AND pid $HB_PID no longer exists — it was killed and will never write a verdict; re-run it. $_where"
