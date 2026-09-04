#!/usr/bin/env bash
#
# lib/process-liveness.sh — the THREE-VALUED process-liveness primitives shared by
# scripts/flow/claim-heartbeat.sh and scripts/flow/drive-issue-state.sh (issue #3822).
#
# WHY THIS FILE EXISTS
# --------------------
# These five functions are the accumulated result of ~8 review rounds on #3393, and
# every one encodes a specific way a two-valued liveness probe reports a LIVE process
# as DEAD (or the reverse). #3822 needs the same question answered — "is the session
# that stamped this marker still running?" — and a SECOND implementation would be a
# second place for those rounds to be re-lost: a second implementation's correctness is
# only knowable by differential testing against the first, so there is ONE definition
# and both callers source it.
#
# They were MOVED here VERBATIM from claim-heartbeat.sh, comments included, because the
# comments ARE the contract: each names the review round and the false verdict it closes.
#
# CONTRACT FOR CALLERS
#   Every function is THREE-VALUED (or returns EMPTY for "could not tell"). A caller must
#   never fold the unknown answer onto the permissive one — a positive verdict requires an
#   affirmative measurement.
#
#     ps_usable                    exit 0 iff `ps` can answer an existence question here
#     signal_probe_class   <pid>   present | absent | denied | unknown
#     process_presence     <pid>   present | absent | unknown
#     process_state_class  <pid>   zombie  | running | unreadable
#     process_start_window <pid>   "<earliest> <latest>" epoch secs, or EMPTY
#
# CONSTRAINTS
#   macOS bash 3.2 compatible. SOURCED, never executed: it defines functions and nothing
#   else — no `set -e`, no side effects, no output at source time — so it cannot change a
#   sourcing script's shell options or emit an unanchored line into its output.
#
#   EVERY external tool here has its stderr SUPPRESSED (#3822, roborev job 26 F2). Both callers
#   publish a strictly ANCHORED output (`DRIVE-STATE: ` / claim-heartbeat's own prefix), and a
#   native `date:`/`tr:`/`cut:` diagnostic from inside a sourced function is a line with no
#   prefix on the caller's stream — breaking an anchor these functions cannot see.

# process_start_window <pid> — echo `<earliest> <latest>` epoch seconds bracketing when <pid>
# started, or EMPTY when it cannot be determined. Empty is a THIRD answer and is never folded
# onto "consistent".
#
# DERIVED FROM ELAPSED TIME, NOT A WALL-CLOCK STRING (roborev round 3, Medium). The first cut
# read `ps -o lstart=` and parsed it with `date -u`, but `lstart` is LOCAL wall time with no
# zone in it, so on any non-UTC host the epoch came out shifted by the offset — MEASURED: the
# same lstart parses 19,800s apart between UTC and Asia/Kolkata, far past the tolerance, which
# would falsely declare a live supervisor DEAD-PID-REUSED. Elapsed seconds carry no timezone at
# all, so the whole class is gone rather than corrected.
#
# AN INTERVAL, NOT A POINT (roborev round 15, Medium). `start = now - elapsed` needs `now` and
# `elapsed` to refer to the same instant, and they cannot: one is read before the other. The
# first cut sampled `now` BEFORE running `ps`, so a slow `ps` shifted the computed start
# BACKWARD — and a start that looks earlier than it is makes a REUSED pid look like it predates
# the claim, i.e. a false ALIVE. That delay is most likely on exactly the resource-exhausted
# hosts this command exists for. So the query is bracketed and both bounds are returned; the
# caller must decide UNKNOWN when the interval straddles its decision boundary.
process_start_window() {
  local pid="$1" secs t0 t1
  t0="$(date -u +%s 2>/dev/null)"
  secs="$(ps -o etimes= -p "$pid" 2>/dev/null | tr -d ' ' 2>/dev/null)"
  case "$secs" in
    '' | *[!0-9]*) secs="" ;;
  esac
  if [ -z "$secs" ]; then
    # Fall back to `etime` ([[DD-]HH:]MM:SS), which POSIX ps provides where `etimes` is
    # absent. Still elapsed, still timezone-free.
    local et d hms h m sec
    et="$(ps -o etime= -p "$pid" 2>/dev/null | tr -d ' ' 2>/dev/null)"
    [ -n "$et" ] || return 0
    case "$et" in
      *-*) d="${et%%-*}"; hms="${et#*-}" ;;
      *)   d=0;           hms="$et" ;;
    esac
    case "$hms" in
      *:*:*) h="${hms%%:*}"; m="$(printf '%s' "$hms" 2>/dev/null | cut -d: -f2 2>/dev/null)"; sec="${hms##*:}" ;;
      *:*)   h=0;            m="${hms%%:*}";                          sec="${hms##*:}" ;;
      *)     return 0 ;;
    esac
    case "$d$h$m$sec" in
      *[!0-9]*) return 0 ;;
    esac
    secs=$(( (10#$d * 86400) + (10#$h * 3600) + (10#$m * 60) + 10#$sec ))
  fi
  t1="$(date -u +%s 2>/dev/null)"
  # The elapsed reading was taken at some instant in [t0, t1], so the start lies in
  # [t0 - secs, t1 - secs]. Earliest first.
  printf '%s %s\n' "$((t0 - secs))" "$((t1 - secs))"
}

# ps_usable — exit 0 iff `ps` can be trusted to answer an existence question here.
#
# SELF-VALIDATING, because "nonzero" is not the same as "absent" (roborev round 8, Medium).
# A `ps` that is missing, unsupported, or simply unable to run would otherwise turn every
# claim into DEAD-NO-PROCESS and exit 3 — a fleet-wide false DEAD. That failure mode is not
# hypothetical on the boxes this issue is about: under the memory exhaustion #3393 records,
# a process that cannot fork cannot run `ps` either, so the ONE moment the report matters
# most is when the probe is most likely to fail. So the tool is validated against a pid
# that is certainly present — our own — before any of its answers are believed. This is
# necessary but NOT sufficient; see `process_presence` for why a per-TARGET vote is also
# needed (round 9).
ps_usable() {
  ps -p "$$" >/dev/null 2>&1
}

# signal_probe_class <pid> — echo `present` | `absent` | `denied` | `unknown`.
#
# `kill -0` is the ONE probe here that is not visibility-based, which is why its failure mode
# has to be decoded rather than abstained on (roborev round 10, Medium). EPERM means the
# process EXISTS and is simply not ours; ESRCH means it is gone. Treating both as "no
# opinion" made every remaining voter a VISIBILITY probe — `ps` and `/proc/<pid>` are
# correlated, both hidden by `hidepid=2` — so a different user's live process was unanimously
# "absent" and reported DEAD.
#
# `LC_ALL=C` is load-bearing: the distinction is drawn from the error text, so the message
# has to be in a known language. An unrecognised message is `unknown`, never folded onto
# either answer.
signal_probe_class() {
  local pid="$1" err
  if kill -0 "$pid" 2>/dev/null; then
    printf 'present\n'
    return 0
  fi
  err="$(LC_ALL=C kill -0 "$pid" 2>&1 || true)"
  case "$err" in
    *"not permitted"* | *"Not permitted"* | *"Operation not permitted"*) printf 'denied\n' ;;
    *"No such process"* | *"no such process"*)                           printf 'absent\n' ;;
    *)                                                                   printf 'unknown\n' ;;
  esac
}

# process_presence <pid> — echo `present` | `absent` | `unknown`.
#
# BUILT FROM AGREEING VOTES, because a NEGATIVE answer from one probe is not proof of
# absence (roborev round 9, Medium). `ps -p` exiting nonzero can mean the process is gone,
# but it can equally mean a transient failure under load or that the target is not visible
# to us — and reading that as absence reports a LIVE supervisor as DEAD. Validating `ps`
# against our OWN pid (round 8) was necessary but not sufficient: it proves the tool runs,
# not that it can see THIS target.
#
# THE VOTERS MUST NOT ALL MEASURE THE SAME THING (round 10). `ps -p` and `/proc/<pid>` are
# both VISIBILITY probes and are hidden together by `hidepid=2`, so on their own they can be
# unanimously and confidently wrong about a live process owned by another user. The signal
# probe is the independent one, and its EPERM answer is affirmative evidence of EXISTENCE —
# which is exactly the case the other two get wrong.
#
# Unanimous present => present. Unanimous absent => absent. DISAGREEMENT => unknown: our view
# of the process table is not self-consistent, so nothing is claimed either way.
process_presence() {
  local pid="$1" yes=0 no=0 sig
  if ps -p "$pid" >/dev/null 2>&1; then yes=$((yes + 1)); else no=$((no + 1)); fi
  if [ -d /proc ]; then
    if [ -e "/proc/$pid" ]; then yes=$((yes + 1)); else no=$((no + 1)); fi
  fi
  sig="$(signal_probe_class "$pid")"
  case "$sig" in
    present | denied) yes=$((yes + 1)) ;;   # denied == EPERM == it exists
    absent)           no=$((no + 1)) ;;
    unknown)          : ;;                  # genuinely no opinion; abstains
  esac

  if [ "$yes" -gt 0 ] && [ "$no" -eq 0 ]; then
    printf 'present\n'
  elif [ "$yes" -eq 0 ] && [ "$no" -gt 0 ] && [ "$sig" = absent ]; then
    # ABSENCE REQUIRES THE INDEPENDENT PROBE TO SAY SO (roborev round 15, Medium). Round 10
    # fixed the case where the signal probe answered `denied`, but when it answers `unknown`
    # the only remaining voters are `ps` and `/proc` — which are BOTH visibility probes,
    # hidden together by `hidepid=2`. They can then be unanimously and confidently wrong
    # about a live process, and this function would call it absent: a false DEAD. So a
    # declaration of absence needs the one non-visibility probe to have affirmed it.
    printf 'absent\n'
  else
    printf 'unknown\n'
  fi
}

# process_state_class <pid> — echo `zombie` | `running` | `unreadable`.
#
# THREE-VALUED, and that is the whole point (roborev round 7, Medium). The first cut was a
# two-valued `process_is_zombie` that returned "not a zombie" when the state could not be
# read — after which a readable start time produced ALIVE and exit 0. So an unreadable
# state became a CLEAN result, which is the same "unknown folded onto the permissive
# answer" shape this command exists to avoid, reintroduced one level down in the fix for
# the previous round. The unreadable case must be neither: not `zombie` (a false DEAD on a
# healthy fleet is how a monitor gets ignored) and not `running` (that is the false-clean).
process_state_class() {
  local st
  st="$(ps -o stat= -p "$1" 2>/dev/null | tr -d ' ' 2>/dev/null)"
  case "$st" in
    '') printf 'unreadable\n' ;;
    Z*) printf 'zombie\n' ;;
    *)  printf 'running\n' ;;
  esac
}
