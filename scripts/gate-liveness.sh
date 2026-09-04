#!/usr/bin/env bash
# gate-liveness.sh — answer "is my gate running, or was it reaped?" from artifacts
# alone, with no access to the box's process table (issue #3473 AC4).
#
# THE DEFECT THIS CLOSES
# ----------------------
# `RESULT: INCOMPLETE (gate did not finish)` is written into the summary file ONCE, at
# launch, before the #1825 slot is even granted (#3041). It is therefore the artifact
# of THREE states at once — queued, running, killed — and the correct completion probe (the
# RECORD grammar, `grep -qE '^RESULT: (PASS|FAIL)([[:space:]]|$)'`) says "not finished" for all
# three. An `--only` run needs the ONLY grammar instead (`…|PARTIAL)…`) because it demotes success
# to `RESULT: PARTIAL`, and its component VERDICT is a separate assertion —
# scripts/gate-component-verdict.sh, which delegates ITS completion question to this reader (#3750).
# A lane whose
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
# Captured BEFORE parsing so the post-wait re-decision can re-exec this script with the caller's exact
# request (roborev job 231). Parsing shifts "$@" away, and a re-exec is what lets the re-decision reuse
# the ENTIRE summary grammar instead of a second, narrower copy of it.
GL_ORIG_ARGS=("$@")
NO_WAIT=""
# INITIALISED HERE, before any summary handling (roborev job 238). Several summary-side paths — a
# missing summary, an unsnapshotable one — now DEFER to the heartbeat side and break out of the summary
# section before line 474 assigns this. The post-wait re-decision then expanded it, and under `set -u`
# even `[ -n "$_SUM_SNAP" ]` ABORTS on an unset variable: a gate that completed during the wait produced
# NO verdict at all, which the launcher reads as unmonitorable. Reproduced: "line 908: _SUM_SNAP:
# unbound variable", exit 1.
_SUM_SNAP=""
# SUM_RUN_ID for the same reason, and the honest status is: NOT reachable today. The precondition audit
# (which of my earlier fixes weakened a guarantee later code relies on?) flagged it as assigned only
# inside the summary section yet referenced after it. Reproduction showed that reference is an `elif`
# evaluated only on an UNBOUND read, while deferral requires `_beat_matches=yes` which requires a named
# run — so on that path the summary section always completed. Kept anyway: the reachability argument is
# NON-LOCAL, and anyone making the funnel defer on an unbound read turns this into the same abort, which
# yields NO verdict and makes the launcher stop a healthy gate. One line against that.
SUM_RUN_ID=""
while [ $# -gt 0 ]; do
  case "$1" in
    --run-id)    WANT_RUN_ID="${2:?--run-id needs a value}"; shift 2 ;;
    # Skip the stall-confirmation sleep. A caller that cannot afford to block (the launcher's bounded
    # verification phase) or one that has already sampled (the post-wait re-decision) uses this. It can
    # only ever WEAKEN a verdict: without a second sample STALLED is unprovable, so the answer becomes
    # UNKNOWN, never a stronger claim.
    --no-wait)   NO_WAIT=1; shift ;;
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
# HOW STRONG THIS ACTUALLY IS. Two earlier revisions over-claimed here and both were wrong
# (roborev jobs 160 and 164), so the guarantee is now stated with its residual attached.
#
# A single `cat` is a single `open()`, so it reads one inode start to finish. That is a
# genuinely atomic snapshot ONLY for a writer that publishes by RENAME — which the heartbeat
# does (sibling temp + `mv`), so a rename landing mid-read swaps the NAME, not our open file.
#
# The SUMMARY is NOT published that way: agent-gate.sh writes it in place with `>`, i.e.
# O_TRUNC then sequential writes. Two consequences, and the second was denied by the previous
# revision of this comment:
#
#   1. a reader can observe a PREFIX of a block being written. That is handled: a partial
#      block is missing its tail, so the mandatory end-marker check rejects it.
#
#   2. a reader CAN observe a BLEND of two writes. The previous comment asserted this was
#      impossible "because O_TRUNC resets the length and content is written forward". FALSE:
#      two writers hold INDEPENDENT file offsets, so if writer B truncates while writer A is
#      mid-block, A's next write lands at ITS old offset and the file becomes B's opener, a
#      sparse hole, then A's tail. Verified directly, not reasoned about. A reader could then
#      pair one run's `run-id:` with another run's `RESULT:` and end marker — a FALSE
#      COMPLETE, the most dangerous verdict this script can give.
#
# The blend has a detectable signature: the sparse hole reads back as NUL bytes, which a
# legitimate summary never contains. `_has_nul` rejects it. Combined with the single-block
# structure check (exactly one opener, run-id, RESULT and end marker, in that order), the
# realistic interleavings are caught.
#
# RESIDUAL, stated rather than papered over: a blend that happens to land on NO hole AND to
# produce a structurally well-formed single block is indistinguishable from a genuine one by
# any reader of the file alone. Nothing here closes that; what closes it is the
# single-writer discipline #2874 already mandates (concurrent gates in one checkout MUST use
# distinct summary paths, and the gate de-exports its summary path so no child inherits it),
# plus making the write atomic at the source — which is a change to how the GATE OF RECORD
# publishes its verdict and belongs in its own issue, not as a ride-along here.
#
# A bounded settle-retry then converts the COMMON case of a torn read — we caught a write in
# progress — into a correct answer, by re-snapshotting once when the framing is incomplete.
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

# SNAPSHOT DISCIPLINE (roborev job 178, Medium). The NUL check and the parse must see the SAME
# bytes. The previous version opened each artifact TWICE — once for `_has_nul`, once for the
# parse — so an interleaved write arriving between them was missed entirely, and because `$( )`
# strips NUL bytes the parse could then accept the blended block as COMPLETE. That is a
# regression of the FIRST review round's finding (per-field re-opens), reintroduced by a check
# added to fix a different problem.
#
# So each artifact is copied ONCE into a private snapshot and every later read — NUL detection
# included — is of that immutable copy. `cp` is a single open of the original, and nothing else
# can write our snapshot.
SNAP_DIR=""
# shellcheck disable=SC2317,SC2329  # invoked indirectly, by the EXIT trap below
_cleanup_snaps() { [ -n "$SNAP_DIR" ] && rm -rf "$SNAP_DIR" 2>/dev/null; return 0; }
trap _cleanup_snaps EXIT
# _ensure_snap_dir — create the private snapshot directory, IN THE CALLING SHELL.
#
# This is deliberately separate from _snap_of, and the reason is a bug this split fixes: the
# first version created the directory inside _snap_of, which is invoked as `$(_snap_of …)` — a
# COMMAND SUBSTITUTION, i.e. a subshell. So `SNAP_DIR=` was assigned in the subshell and never
# reached the parent, which meant (a) the EXIT trap saw an empty SNAP_DIR and cleaned nothing,
# and (b) every call created ANOTHER directory. Measured: 868 leaked `gate-liveness-snap.*`
# directories after the suites had run. Assignments do not escape `$( )`.
_ensure_snap_dir() {
  [ -n "$SNAP_DIR" ] && return 0
  SNAP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/gate-liveness-snap.XXXXXX" 2>/dev/null) || return 1
  return 0
}

# _snap_of <file> <tag> -> path to an immutable copy on stdout; non-zero on failure.
# Requires _ensure_snap_dir to have run in the calling shell first.
_snap_of() {
  [ -n "$SNAP_DIR" ] || return 1
  local dst="$SNAP_DIR/$2"
  cp -- "$1" "$dst" 2>/dev/null || return 1
  printf '%s' "$dst"
}

# (defined HERE, with the other artifact helpers, because the STARTUP probe below calls it before
#  the heartbeat section is reached — bash defines functions as it reads the file.)
# ONE validator for a beat, applied to EVERY snapshot of it (roborev job 190, Medium). The first
# version validated only the FIRST read: the confirmation re-read was accepted on a matching
# run-id plus a merely "different, non-empty" beat-seq, so a malformed or truncated SECOND
# snapshot could still carry a RUNNING verdict. Two reads of one artifact must clear the same bar,
# and the only way to guarantee that is for a single piece of code to state it.
#
# Returns 0 when <text> is a single, coherent, identity-bearing beat; otherwise non-zero with a
# named reason in BEAT_ERR. It decides only whether the fields may be TRUSTED; the caller reads
# values with _field.
BEAT_ERR=""
_beat_valid() {
 local t="$1" n_start n_end open_ln close_ln pc iv ep sq ln
  n_start=$(grep -cxF '==== AGENT-GATE HEARTBEAT ====' <<<"$t")
  n_end=$(grep -cxF '==== END AGENT-GATE HEARTBEAT ====' <<<"$t")
  if [ "$n_start" -ne 1 ] || [ "$n_end" -ne 1 ]; then
    BEAT_ERR="heartbeat-not-a-single-block; found $n_start opener(s) and $n_end closer(s) — a beat is published by atomic rename, so anything but exactly one of each means this is not one coherent beat"
    return 1
  fi
  open_ln=$(grep -nxF '==== AGENT-GATE HEARTBEAT ====' <<<"$t" | head -1 | cut -d: -f1)
  close_ln=$(grep -nxF '==== END AGENT-GATE HEARTBEAT ====' <<<"$t" | head -1 | cut -d: -f1)
  if [ "$open_ln" -ge "$close_ln" ]; then
    BEAT_ERR="heartbeat-out-of-order; the closer (line $close_ln) does not follow the opener (line $open_ln)"
    return 1
  fi
  # EVERY field that decides a verdict must appear EXACTLY ONCE and INSIDE the framing (roborev
  # job 193, Medium). The first version checked only run-id/beat-seq/beat-epoch — but
  # `parent-check` decides whether any RUNNING is supportable, `interval` sets the staleness window
  # and the confirmation wait, `host` decides whether the clock may be trusted, and `beater-pid`
  # decides whether a restart counts as progress. A duplicate or out-of-block copy of any of them
  # would be read as the first occurrence, letting an ambiguous beat produce RUNNING.
  #
  # "Exactly once", not "at most once": a beat missing any of these cannot support a verdict.
  # `beater-pid` is the one exception — it is absent from beats written by an older gate, and its
  # absence only forfeits restart detection rather than enabling a wrong answer, so it is checked
  # for uniqueness/placement ONLY IF present.
  # REQUIRED vs OPTIONAL-BUT-UNIQUE, and the line between them is whether ABSENCE would make a
  # verdict unsound or merely narrower:
  #   required  — run-id, beat-seq, beat-epoch, interval, parent-check. Without any of these a
  #               verdict cannot be computed at all (or, for parent-check, cannot be trusted).
  #   optional  — host, beater-pid. Their absence DEGRADES SAFELY and is already handled: no host
  #               means the clock domain is unproven, so progression decides; no beater-pid means
  #               restart detection is forfeited. Requiring them would reject beats that the reader
  #               can answer about perfectly well — which it briefly did, until 11g.9 caught it.
  # Either way a DUPLICATE is fatal for both groups, because the first occurrence would be trusted.
  local f cnt
  for f in run-id beat-seq beat-epoch interval parent-check; do
    cnt=$(grep -c "^$f: " <<<"$t")
    if [ "$cnt" -ne 1 ]; then
      BEAT_ERR="heartbeat-field-count; '$f' appears $cnt time(s) and must appear exactly once — no value is attributable to a single beat otherwise"
      return 1
    fi
  done
  # `beater-pid`, WHEN PRESENT, must be a plausible pid (roborev job 223, Low). It is optional — an
  # older gate's beats omit it — but it is NOT inert: a CHANGED value between two samples counts as a
  # beater relaunch and therefore as PROGRESS, which yields RUNNING. Checking only uniqueness and
  # placement meant two DIFFERENT malformed values read as a restart, so a pair of invalid beats could
  # produce a RUNNING verdict. Absent stays safe (progression decides); present-but-nonsense must not.
  # EVERY refusal NAMES ITS CAUSE — the invariant this whole file is built on, and the first version of
  # this validation broke it (roborev job 226): the branches returned 1 without setting BEAT_ERR, so
  # the diagnostic degraded to a literal `gate-liveness: UNKNOWN ()` or carried an unrelated earlier
  # reason. My own test for it asserted only the exit code, so it passed while the cause was EMPTY —
  # the "assert the message, not just the code" lesson from 4b.76, not propagated to a test written one
  # round later.
  _bp=$(sed -n 's/^beater-pid: //p' <<<"$1" | head -1)
  if [ -n "$_bp" ]; then
    case "$_bp" in
      *[!0-9]*)
        BEAT_ERR="heartbeat-beater-pid-not-a-pid; 'beater-pid: $_bp' is not a decimal pid — the field is optional, but when present a CHANGED value counts as a beater relaunch and therefore as PROGRESS, so a value that cannot be a pid must not be compared"
        return 1 ;;
      0)
        BEAT_ERR="heartbeat-beater-pid-zero; 'beater-pid: 0' is never a real process — see above: this field moves a verdict when it changes, so it may not hold a placeholder"
        return 1 ;;
    esac
    if [ "${#_bp}" -gt 7 ]; then
      BEAT_ERR="heartbeat-beater-pid-out-of-range; 'beater-pid: $_bp' has ${#_bp} digits and no pid namespace goes past 7, so this cannot be a pid this reader may compare"
      return 1
    fi
  fi
  for f in host beater-pid; do
    cnt=$(grep -c "^$f: " <<<"$t")
    if [ "$cnt" -gt 1 ]; then
      BEAT_ERR="heartbeat-field-count; '$f' appears $cnt times and must appear at most once — the first occurrence would be trusted"
      return 1
    fi
  done
  for f in run-id beat-seq beat-epoch interval parent-check host beater-pid; do
    grep -q "^$f: " <<<"$t" || continue
    ln=$(grep -n "^$f: " <<<"$t" | head -1 | cut -d: -f1)
    if [ "$ln" -lt "$open_ln" ] || [ "$ln" -gt "$close_ln" ]; then
      BEAT_ERR="heartbeat-field-outside-block; '$f' (line $ln) lies outside the block (lines $open_ln..$close_ln)"
      return 1
    fi
  done
  # A DIGIT STRING IS NOT YET A NUMBER (roborev job 192, Low). Bash arithmetic reads a leading zero
  # as OCTAL, so `interval: 08` is a syntax error that ABORTS the shell — the reader would die
  # instead of returning its documented UNKNOWN — and an unbounded digit string can overflow a
  # comparison. So each field is length-bounded (rejecting absurd values outright) and every later
  # use goes through base-10 arithmetic.
  ep=$(grep -m1 '^beat-epoch: ' <<<"$t"); ep="${ep#beat-epoch: }"
  case "$ep" in ''|*[!0-9]*) BEAT_ERR="heartbeat-unparseable-epoch; 'beat-epoch: $ep' is not an integer"; return 1 ;; esac
  [ "${#ep}" -le 12 ] || { BEAT_ERR="heartbeat-epoch-out-of-range; 'beat-epoch: $ep' has ${#ep} digits, which is not a plausible unix time"; return 1; }
  sq=$(grep -m1 '^beat-seq: ' <<<"$t"); sq="${sq#beat-seq: }"
  case "$sq" in ''|*[!0-9]*) BEAT_ERR="heartbeat-unparseable-seq; 'beat-seq: $sq' is not an integer"; return 1 ;; esac
  [ "${#sq}" -le 12 ] || { BEAT_ERR="heartbeat-seq-out-of-range; 'beat-seq: $sq' has ${#sq} digits"; return 1; }
  iv=$(grep -m1 '^interval: ' <<<"$t"); iv="${iv#interval: }"
  case "$iv" in ''|*[!0-9]*) BEAT_ERR="heartbeat-unparseable-interval; 'interval: $iv' is not an integer"; return 1 ;; esac
  [ "${#iv}" -le 6 ] || { BEAT_ERR="heartbeat-interval-out-of-range; 'interval: $iv' has ${#iv} digits"; return 1; }
  # Normalise to base 10 NOW, so no later comparison can trip over an octal reading.
  ep=$((10#$ep)); sq=$((10#$sq)); iv=$((10#$iv))
  [ "$iv" -ge 1 ] || { BEAT_ERR="heartbeat-bad-interval; 'interval: $iv' must be >= 1"; return 1; }
  if [ "$iv" -gt 60 ]; then
    BEAT_ERR="heartbeat-interval-too-long; 'interval: ${iv}s' exceeds the 60s this reader can observe (its confirmation window is capped at 65s to bound a hostile artifact), so a live beat might not advance inside it and STALLED would be a false death"
    return 1
  fi
  pc=$(grep -m1 '^parent-check: ' <<<"$t"); pc="${pc#parent-check: }"
  case "$pc" in
    starttime|lstart) : ;;
    kill0) BEAT_ERR="heartbeat-no-gate-identity; the beater reports 'parent-check: kill0', meaning it could NOT establish any identity for its gate — so a recycled pid would keep it publishing for an unrelated process. Counter progression would only prove the BEATER is alive, not the gate, so no RUNNING claim is supportable from this beat"; return 1 ;;
    '')    BEAT_ERR="heartbeat-no-parent-check; the beat declares no 'parent-check:' field, so it is unknown whether the beater can identify its gate at all"; return 1 ;;
    *)     BEAT_ERR="heartbeat-unknown-parent-check; 'parent-check: $pc' is not a value this reader knows (expected starttime, lstart or kill0)"; return 1 ;;
  esac
  return 0
}


# _has_nul <file> — true when the file contains a NUL byte. Checked on the FILE, never on a
# slurped string: `$( )` silently strips NULs, so the evidence is gone by the time the text
# reaches a variable. A NUL is the fingerprint of the sparse hole an interleaved O_TRUNC
# write leaves behind (see the note above); no legitimate gate artifact contains one.
# shellcheck disable=SC2094  # reads $1 twice (redirect + cmp arg) and writes it never
_has_nul() {
  LC_ALL=C tr -d '\000' < "$1" 2>/dev/null | cmp -s - "$1" 2>/dev/null && return 1
  return 0
}


# _field <text> <key> — first "key: value" line's value in <text>, or empty.
_field() {
  local text="$1" k="$2" line
  line=$(grep -m1 "^$k: " <<<"$text") || return 0
  printf '%s' "${line#"$k": }"
}

verdict() { # verdict <STATUS> <exit> <detail>
  echo "gate-liveness: $1 ($3)"
  # RUNNING MEANS ALIVE, NOT PROGRESSING — and the caveat is emitted HERE, once, rather than
  # appended to each of the five `verdict RUNNING` sites, so a sixth site inherits it and there is
  # no list to forget. Measured the hard way: a gate WAITING for the #1825 slot beats exactly like
  # one compiling, so this reader answered RUNNING for 4.9 hours about a run that had done nothing,
  # and both a lane and its coordination lead read that as progress. STALLED's own text already
  # carries its limitation ("NOT a claim that the process is dead"); RUNNING carried none, and the
  # asymmetry is the defect: the verdict that over-claims DEATH was disclosed carefully and the one
  # that over-claims PROGRESS was not disclosed at all. A caveat that lives only in the source is
  # not a disclosure to whoever reads the artifact.
  if [ "$1" = RUNNING ]; then
    echo "note: RUNNING means this run is ALIVE, not that it is PROGRESSING — a gate queued for the"
    echo "      gate slot beats identically to one doing work."
    echo "      Do NOT judge progress from /proc/<gate-pid>/stat: utime+stime+cutime+cstime counts"
    echo "      REAPED children only, and a gate's work lives in cargo/maturin children that are"
    echo "      still ALIVE, so the parent reads near 0.1 cores while the run consumes GB/min."
    echo "      MEASURED on a healthy 37/37 run: 0.15 cores at 89s and 0.09 at 905s, while it was"
    echo "      producing ~5.7 GB/min of build output with 60 pids turning over per 20s."
    echo "      Sound signals: PID-SET TURNOVER in the run's cgroup (processes entering and leaving"
    echo "      = advancing), and GROWTH of the build target/ (a build's output evidences a build)."
    echo "      Sample either more than once: component load swings 0 -> 2+ cores, and one spot check"
    echo "      under-reads a bursty writer as surely as it over-reads a lull."
  fi
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
# STARTUP: a NAMED run with a MATCHING BEAT is alive, even before its summary exists.
#
# This is the launcher's own workflow (roborev job 196, Medium). The beater deliberately starts
# before the tree-identity capture, and gate-detached.sh accepts a gate on the strength of that
# beat — then prints a run-bound poll command. But this reader used to reject a missing summary
# outright, so the advertised command answered UNKNOWN for a healthy, accepted, actively-beating
# gate for the whole duration of the capture. The launcher and the reader disagreed about what
# "accepted" means, which is worse than either being wrong alone.
#
# Only for a CALLER-NAMED run: the caller has told us which run they mean, and a fresh beat bearing
# that run-id is affirmative evidence it is alive. Without --run-id there is nothing to match
# against, so the summary remains the only anchor and its absence is still UNKNOWN.
_startup_beat=no
_beat_matches=no
# The snapshot directory must exist before _snap_of can work; it is normally created just before the
# summary is read, which is AFTER this probe. Creating it here made the difference between this
# check working and silently never firing.
if [ -n "$WANT_RUN_ID" ] && [ -f "$HB" ] && [ -r "$HB" ] && _ensure_snap_dir; then
  _sb_snap=$(_snap_of "$HB" startupbeat 2>/dev/null) || _sb_snap=""
  if [ -n "$_sb_snap" ] && ! _has_nul "$_sb_snap"; then
    _sb_text=$(_slurp "$_sb_snap")
    # FRESHNESS IS PART OF THE TEST (roborev job 197, Medium). The first version accepted any
    # structurally valid, run-id-matching beat — so a gate that died after its FIRST beat but before
    # writing its summary would report RUNNING forever from that one stale beat. A false RUNNING is
    # the worst verdict this script can give: the caller waits indefinitely on a gate that is gone.
    #
    # Only a beat that is BOTH matching AND fresh in a proven shared clock domain takes the startup
    # shortcut. A stale one simply does NOT take it, and the normal summary handling below then
    # answers (UNKNOWN, naming the missing or superseded summary) — the pre-shortcut behaviour, which
    # was safe. That keeps the whole progression apparatus in one place instead of duplicating it.
    if _beat_valid "$_sb_text" && [ "$(_field "$_sb_text" run-id)" = "$WANT_RUN_ID" ]; then
      # SEPARATE FROM FRESHNESS, and deliberately set BEFORE the host and age checks (roborev job
      # 216): the beat is structurally valid and names the run the caller asked about. That alone
      # makes the HEARTBEAT SIDE the right authority whenever the summary is unusable, because a
      # STALE matching beat must reach the two-sample confirmation and be answered STALLED rather
      # than pre-empted by a complaint about the summary. Without it, a gate reaped during the
      # pre-sentinel tree capture could NEVER be reported STALLED — exactly the startup interval that
      # moving the beater before the tree capture exists to cover.
      _beat_matches=yes
      # A FAILED LOOKUP MUST NOT COMPARE EQUAL TO ANOTHER FAILED LOOKUP (roborev job 221). Both this
      # reader and the beater used `uname -n || echo unknown`, so when BOTH lookups failed the two
      # literal `unknown`s matched and were accepted as proof of a SHARED CLOCK DOMAIN — which then
      # licenses judging freshness by epoch and could report RUNNING for a dead cross-host beat.
      # That is absence of measurement read as a positive match, the shape this file refuses
      # everywhere else. An unverified host is now EMPTY, and empty never matches.
      _sb_host=$(_field "$_sb_text" host)
      _sb_myhost=$(uname -n 2>/dev/null || true)
      case "$_sb_host" in unknown) _sb_host="" ;; esac
      case "$_sb_myhost" in unknown) _sb_myhost="" ;; esac
      if [ -n "$_sb_host" ] && [ -n "$_sb_myhost" ] && [ "$_sb_host" = "$_sb_myhost" ]; then
        _sb_iv=$(_field "$_sb_text" interval); _sb_iv=$((10#${_sb_iv:-0}))
        _sb_ep=$(_field "$_sb_text" beat-epoch); _sb_ep=$((10#${_sb_ep:-0}))
        _sb_win=$(( _sb_iv * 3 )); [ "$_sb_win" -ge 90 ] || _sb_win=90
        _sb_age=$(( $(date +%s) - _sb_ep ))
        if [ "$_sb_age" -ge 0 ] && [ "$_sb_age" -le "$_sb_win" ]; then
          _startup_beat=yes
          _sb_note="beat ${_sb_age}s old, window ${_sb_win}s"
        fi
      fi
    fi
  fi
fi

# ONE DECISION POINT FOR EVERY SUMMARY-SIDE REFUSAL (roborev job 209). Each refusal below is about
# the SUMMARY — unreadable, NUL-bearing, not a single block, no run-id, no opener, mismatched
# dialect, out-of-order. NONE of them says anything about the run the CALLER NAMED. So when that run
# has a valid, matching, FRESH beat, answering UNKNOWN discards evidence we already hold.
#
# The harm was concrete and not hypothetical: `gate-detached.sh` STOPS the unit when liveness cannot
# answer within 20s, so a previous run's malformed leftover summary made the launcher KILL A HEALTHY
# GATE whose tree capture outran the window.
#
# A valid TERMINAL summary for the requested run still wins, and that is why this is a deferral at
# the refusal sites rather than a short-circuit before parsing: we must attempt the parse to find a
# terminal verdict, and only fall back when the parse cannot yield one.
#
# It is ONE helper rather than a guard per site because the first attempt at this fix guarded FOUR
# refusals and left FOUR others, and the behavioural probe still killed the gate through
# `summary-no-run-id-at-all`. A single funnel closes the class, and the test suite asserts no
# summary refusal bypasses it — so a refusal added later cannot silently reopen this.
_summary_refusal() {  # <what-is-wrong-with-the-summary> <full-cause-for-UNKNOWN>
  if [ "$_startup_beat" = yes ]; then
    verdict RUNNING 2 "run '$WANT_RUN_ID' is beating ($_sb_note); $1, which says nothing about this run — it may be a previous run's leftover or a write in progress"
  fi
  # A VALID beat naming the requested run, but not fresh: the heartbeat side is the authority, and it
  # owns the two-sample confirmation that distinguishes STALLED from an unmeasurable stall. Returning
  # 1 defers to it (the caller `|| break`s out of the summary section) rather than duplicating that
  # logic here — this change has already removed two duplicated grammars (jobs 172, 198) and a third
  # would be the same mistake.
  [ "$_beat_matches" = yes ] && return 1
  verdict UNKNOWN 4 "$2"
}
# THE SECOND POLICY, AND WHY THERE ARE TWO (roborev job 220). `_summary_refusal_or_defer` exists for
# summaries that say NOTHING about whether the run terminated — absent, unreadable, unsnapshotable, no
# RESULT line. For those, a matching beat is the better authority.
#
# An UNRECOGNISED RESULT TOKEN is different in kind, and job 218 wrongly treated it as the same: a
# well-formed summary for the requested run bearing `RESULT: <something new>` tells us the gate
# TERMINATED — we simply cannot name the verdict. Deferring to the beat then converts "I do not
# understand this verdict" into "the gate seems dead", and a stale beat here is a CONSEQUENCE of that
# termination rather than evidence of a stall. A caller told STALLED may relaunch a run that already
# finished. So this path never defers, whatever the heartbeat says.
#
# It is a NAMED helper rather than a bare `verdict` so the structural guard can stay property-shaped:
# every UNKNOWN in the summary region goes through one of exactly two helpers, each documenting its
# own deferral policy, and an ad-hoc bare UNKNOWN is still a violation.
_summary_terminal_unknown() {  # <full-cause>: the summary indicates termination; never defer
  verdict UNKNOWN 4 "$1"
}
# MOVED to enclose EVERY summary-side path (roborev job 218). Three paths previously sat ABOVE this
# wrapper and emitted a bare `verdict UNKNOWN`: `no-summary-artifact`, `no-snapshot-dir` and
# `no-result-line`. None of them starts with `summary-`, which is exactly why the structural guard
# missed them — it checked a NAME PREFIX rather than the PROPERTY, the same mistake this file
# documents elsewhere. One of those paths also called `_summary_refusal` with no `|| break`, so a
# deferral there fell through into the next check instead of reaching the heartbeat side.
# The summary section sits in a `while :; do ... break; done` so a refusal can FALL THROUGH to the
# heartbeat side below instead of ending the script (roborev job 216). A `while` loop is not a
# subshell, so every variable this section sets is still visible afterwards; bash has no forward goto
# and this is the idiomatic substitute. `_summary_refusal_or_defer` breaks out when the beat is the
# better authority, and otherwise never returns at all.
_summary_refusal_or_defer() { _summary_refusal "$@"; }
while :; do
if [ ! -f "$SUMMARY" ]; then
  if [ "$_startup_beat" = yes ]; then
    verdict RUNNING 2 "run '$WANT_RUN_ID' is beating ($_sb_note) but has not written its summary yet — normal during startup, because the gate publishes liveness BEFORE its tree-identity capture and sentinel. summary: not yet at $SUMMARY"
  fi
  _summary_refusal_or_defer "the summary has not been written at this path" "no-summary-artifact; $SUMMARY is not readable as a regular file (never written, or its location is not reachable from here)" || break
fi
if [ ! -r "$SUMMARY" ]; then
  _summary_refusal_or_defer "the summary at this path cannot be read" "summary-unreadable; $SUMMARY exists but cannot be read" || break
fi
_ensure_snap_dir || _summary_refusal_or_defer "the summary could not be snapshotted for a consistent read" "no-snapshot-dir; could not create a private temp directory under ${TMPDIR:-/tmp} to read the artifacts consistently" || break
_SUM_SNAP=$(_snap_of "$SUMMARY" summary) || _SUM_SNAP=""
if [ -z "$_SUM_SNAP" ]; then
  _summary_refusal_or_defer "the summary at this path could not be snapshotted" "summary-unsnapshotable; could not take a private copy of $SUMMARY to read it consistently (no writable temp dir, or the file vanished)" || break
fi
if _has_nul "$_SUM_SNAP"; then
  _summary_refusal_or_defer "the summary at this path contains NUL bytes" "summary-contains-nul; $SUMMARY holds NUL bytes, the signature of two writers interleaving on one path (#2874 requires concurrent gates to use distinct summary paths) — its fields cannot be attributed to a single run" || break
fi
SUM_TEXT=$(_slurp "$_SUM_SNAP")
# Settle-retry: incomplete framing may mean we caught a write in progress. Re-SNAPSHOT once and
# prefer the completed copy — re-snapshotting rather than re-reading is what keeps the NUL check
# and the parse on the same bytes.
if ! grep -qE '^==== END AGENT-GATE( LITE| DELTA)? SUMMARY ====$' <<<"$SUM_TEXT"; then
  sleep 0.2
  _SUM_SNAP2=$(_snap_of "$SUMMARY" summary2) || _SUM_SNAP2=""
  if [ -n "$_SUM_SNAP2" ] && ! _has_nul "$_SUM_SNAP2"; then
    _t2=$(_slurp "$_SUM_SNAP2")
    if grep -qE '^==== END AGENT-GATE( LITE| DELTA)? SUMMARY ====$' <<<"$_t2"; then
      SUM_TEXT="$_t2"
    fi
  fi
fi
# EXACTLY ONE of each framing element. Checked HERE — before any field is read — not on the
# COMPLETE path only, which is where it first lived: a file whose first RESULT happens to be
# `INCOMPLETE` would then have skipped the check entirely and been dispatched on a field it
# had no right to trust. More than one of any element means the file holds fragments of more
# than one write, and then NOTHING in it can be attributed to a single run — including the
# run-id used moments later to decide whether this artifact is even ours.
_n_start=$(grep -cE '^==== AGENT-GATE( LITE| DELTA)? SUMMARY ====$' <<<"$SUM_TEXT")
_n_end=$(grep -cE '^==== END AGENT-GATE( LITE| DELTA)? SUMMARY ====$' <<<"$SUM_TEXT")
_n_res=$(grep -c '^RESULT: ' <<<"$SUM_TEXT")
_n_rid=$(grep -c '^run-id: ' <<<"$SUM_TEXT")
if [ "$_n_start" -gt 1 ] || [ "$_n_end" -gt 1 ] || [ "$_n_res" -gt 1 ] || [ "$_n_rid" -gt 1 ]; then
  _summary_refusal_or_defer "the summary at this path is not one single block" "summary-not-a-single-block; found $_n_start openers / $_n_rid run-id / $_n_res RESULT / $_n_end closers — more than one of any means the file holds fragments of more than one write, so no field can be attributed to a single run" || break
fi
# EXACTLY ONE run-id, whether or not the caller named a run (roborev job 199, Medium). Duplicates
# were already fatal; ZERO was not, so an unbound reader accepted a terminal verdict from a block
# attributable to NO run. Every legitimate summary carries a run-id — the gate writes it immediately
# after the opener — so its absence means this is not a whole block, and a verdict read from it
# cannot be assigned to anything. (My own audit of what backs COMPLETE said "run-id bound", which
# was only true when --run-id was supplied; this makes the claim unconditional.)
if [ "$_n_rid" -eq 0 ]; then
  _summary_refusal_or_defer "the summary at this path carries no run-id" "summary-no-run-id-at-all; $SUMMARY carries no 'run-id:' line, so nothing in it can be attributed to a run — every summary the gate writes has one" || break
fi
# The closer must MATCH THE OPENER'S DIALECT, and the elements must be IN ORDER (roborev job
# 172, Medium). Counting "some opener" and "some closer" independently accepted a LITE opener
# closed by a DELTA marker, and imposed no ordering — so a `RESULT:` line sitting BEFORE the
# opener also passed. Both were verified reporting COMPLETE before this check existed, and both
# are exactly what an interleaved write produces, which is the case these checks are for.
#
# The three dialects are the gate's own (full / --lite / --delta) and CLAUDE.md keeps them
# DISTINCT so no block can ever be pasted as another; a reader that accepts a mismatched pair
# throws that distinction away.
_open_line=$(grep -nE '^==== AGENT-GATE( LITE| DELTA)? SUMMARY ====$' <<<"$SUM_TEXT" | head -1)
_open_ln="${_open_line%%:*}"
_open_txt="${_open_line#*:}"
_dialect="${_open_txt#'==== AGENT-GATE'}"
_dialect="${_dialect%' SUMMARY ===='}"
_want_close="==== END AGENT-GATE${_dialect} SUMMARY ===="
_close_ln=$(grep -nxF "$_want_close" <<<"$SUM_TEXT" | head -1 | cut -d: -f1)
# ENFORCED HERE, for every path (roborev job 176, Medium): a valid OPENER must exist and the
# fields must be ORDERED, whatever the RESULT says. The previous split enforced neither on the
# INCOMPLETE path, and that is not merely untidy — when the caller omits `--run-id` this reader
# takes the run-id FROM THE SUMMARY and uses it to decide whether the heartbeat is ours. An
# interleaved summary can therefore hand over a FOREIGN fragment's run-id, and the reader would
# then validate a peer's beat and report RUNNING about somebody else's gate.
#
# Only the CLOSER stays COMPLETE-path-only, which is the precise shape of the legitimate
# exception: a mid-write read is missing its TAIL. A missing closer is truncation and still
# falls through to the heartbeat; a MISMATCHED closer or out-of-order fields are not truncation,
# they are two writes spliced, and those are refused everywhere.
_res_ln=$(grep -n '^RESULT: ' <<<"$SUM_TEXT" | head -1 | cut -d: -f1)
_rid_ln=$(grep -n '^run-id: ' <<<"$SUM_TEXT" | head -1 | cut -d: -f1)
# Ordering: opener first, closer last, RESULT and (when present) run-id between them. A missing
# run-id is tolerated here and judged on its own terms further down.
# The closer may legitimately be ABSENT (a truncated mid-write read), so every comparison
# against it is guarded. Everything else is checked unconditionally.
_order_bad=""
if [ -n "$_open_ln" ] && [ -n "$_res_ln" ]; then
  if [ "$_res_ln" -lt "$_open_ln" ]; then
    _order_bad="RESULT (line $_res_ln) precedes the opener (line $_open_ln)"
  elif [ -n "$_close_ln" ] && [ "$_res_ln" -gt "$_close_ln" ]; then
    _order_bad="RESULT (line $_res_ln) follows the closer (line $_close_ln)"
  fi
fi
if [ -z "$_order_bad" ] && [ -n "$_open_ln" ] && [ -n "$_rid_ln" ]; then
  if [ "$_rid_ln" -lt "$_open_ln" ]; then
    _order_bad="run-id (line $_rid_ln) precedes the opener (line $_open_ln)"
  elif [ -n "$_close_ln" ] && [ "$_rid_ln" -gt "$_close_ln" ]; then
    _order_bad="run-id (line $_rid_ln) follows the closer (line $_close_ln)"
  fi
fi
# ...and run-id must precede RESULT (roborev job 191, Medium). The previous version checked only
# that each field was INSIDE the markers, not their RELATIVE order — so a block with `RESULT: PASS`
# ahead of a matching `run-id` was accepted as COMPLETE while claiming to validate an ordered block.
# The gate always writes run-id immediately after the opener and RESULT last, so any other order is
# fragments of more than one write.
if [ -z "$_order_bad" ] && [ -n "$_rid_ln" ] && [ -n "$_res_ln" ] && [ "$_rid_ln" -gt "$_res_ln" ]; then
  _order_bad="run-id (line $_rid_ln) comes AFTER RESULT (line $_res_ln); the gate writes run-id first and RESULT last"
fi

if [ -z "$_open_ln" ]; then
  _summary_refusal_or_defer "the summary at this path is not a readable gate block" "summary-no-opener; $SUMMARY has no '==== AGENT-GATE … SUMMARY ====' opener, so it is not a gate summary block and none of its fields can be attributed to a run" || break
fi
if [ "$_n_end" -gt 0 ] && [ -z "$_close_ln" ]; then
  _summary_refusal_or_defer "the summary at this path splices markers from two modes" "summary-marker-dialect-mismatch; the block opens with '$_open_txt' but its closer is a DIFFERENT mode (no matching '$_want_close') — an opener and closer from different modes are fragments of two writes, not one block" || break
fi
if [ -n "$_order_bad" ]; then
  _summary_refusal_or_defer "the summary at this path has out-of-order fields" "summary-out-of-order; $_order_bad — these are not one ordered block, so no field can be attributed to a single write" || break
fi

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
if [ -n "$WANT_RUN_ID" ] && [ "$_startup_beat" = yes ] && [ -n "$SUM_RUN_ID" ] \
   && [ "$SUM_RUN_ID" != "$WANT_RUN_ID" ]; then
  # The summary at this path still belongs to a PREVIOUS run while ours is starting up — the same
  # startup window as above, one step later (job 196). The caller named a run and its beat is
  # valid and matching, so answer about THAT run instead of refusing.
  verdict RUNNING 2 "run '$WANT_RUN_ID' is beating ($_sb_note); the summary at this path still belongs to run '$SUM_RUN_ID' and has not been replaced yet — normal during startup, because liveness is published before the sentinel"
fi
if [ -n "$WANT_RUN_ID" ]; then
  if [ -z "$SUM_RUN_ID" ]; then
    _summary_refusal_or_defer "the summary at this path carries no run-id" "summary-no-run-id; $SUMMARY carries no 'run-id:' line, so it cannot be attributed to run '$WANT_RUN_ID'" || break
  fi
  if [ "$SUM_RUN_ID" != "$WANT_RUN_ID" ]; then
    _summary_refusal_or_defer "the summary at this path belongs to a different run" "summary-run-id-mismatch; $SUMMARY carries run-id '$SUM_RUN_ID', not '$WANT_RUN_ID' — a live peer owns that path" || break
  fi
fi
RESULT_LINE=$(grep -m1 '^RESULT: ' <<<"$SUM_TEXT" || true)
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
  _summary_refusal_or_defer "the summary has no RESULT line" "no-result-line; $SUMMARY has no 'RESULT:' line (truncated or not a gate summary)" || break
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
_END_RE='^==== END AGENT-GATE( LITE| DELTA)? SUMMARY ====$'
case "$RESULT_TOKEN" in
  PASS|FAIL|PARTIAL|ERROR|REFUSED)
    # RECONCILE WITH THE HEARTBEAT before believing this verdict (roborev job 192, Medium). During
    # startup a NEW run publishes its beat BEFORE it replaces the previous run's summary — the beater
    # now starts before the tree capture, which widened that window on purpose. So an unbound reader
    # (no --run-id) could read the PREVIOUS run's PASS as the completion of the run that is starting
    # right now. If a readable beat names a DIFFERENT run, the two artifacts describe different runs
    # and neither can be reported as the other's outcome.
    if [ -z "$WANT_RUN_ID" ] && [ -n "$SUM_RUN_ID" ] && [ -f "$HB" ] && [ -r "$HB" ]; then
      _hb_peek=$(_snap_of "$HB" hbpeek 2>/dev/null) || _hb_peek=""
      if [ -n "$_hb_peek" ] && ! _has_nul "$_hb_peek"; then
        _hb_peek_text=$(_slurp "$_hb_peek")
        _hb_rid_peek=$(_field "$_hb_peek_text" run-id)
        if [ -n "$_hb_rid_peek" ] && [ "$_hb_rid_peek" != "$SUM_RUN_ID" ]; then
          # A DIFFERING unbound run-id is UNKNOWN, FULL STOP — and the road here is worth recording,
          # because the middle version was WORSE than either end.
          #
          # Job 206 was right that this arm's diagnostic was wrong: it called any readable foreign
          # beat "a live heartbeat … a NEWER run is starting", asserting a liveness it never measured
          # and an ordering it could not know. Heartbeats outlive their runs, so the foreign beat may
          # be an older leftover.
          #
          # The fix attempted for that added a permissive branch — a PROVABLY STALE foreign beat was
          # treated as an older leftover and ignored, reporting the summary's verdict. Job 208 (High)
          # showed that is unsound: **staleness establishes no ordering.** If run A completed, run B
          # published its early beat and then DIED before replacing A's summary, B's beat is stale
          # and NEWER than the summary — and ignoring it reports A's old PASS as the current run's
          # outcome. A false COMPLETE certifies a gate that never finished. The two shapes,
          #     summary=B(terminal) + beat=A(stale)   [beat older]
          #     summary=A(terminal) + beat=B(stale)   [beat newer]
          # are INDISTINGUISHABLE by age, and "stale" was being read as "predates the summary".
          # That is this file's own rule broken in its own favour: a positive verdict derived from a
          # reading that does not support it.
          #
          # So there is no age branch. Any differing unbound run-id is UNKNOWN, the honest answer
          # from two files that describe different runs, and `--run-id` is the way to ask a question
          # that HAS an answer. Job 206's real defect — the overclaiming diagnostic — is fixed by
          # saying what is actually known and nothing more. An ordering field could rescue the
          # permissive branch, but that is a new artifact contract, not a bug fix.
          _summary_refusal_or_defer "the summary and the heartbeat name different runs" "summary-foreign-run; the summary carries a terminal verdict for run '$SUM_RUN_ID' while the heartbeat beside it names run '$_hb_rid_peek'. These two files describe DIFFERENT runs, and nothing in either says which is current: a heartbeat outlives the run that wrote it, so the foreign beat may be a newer run that has not replaced the summary yet OR an older leftover, and its age cannot tell those apart. Refusing to report one run's verdict as another's. Pass --run-id to name the run you mean." || break
        fi
      fi
    fi
    # Distinguish the two shapes rather than blaming the wrong one: NO closer at all is a
    # truncated write; a closer of a DIFFERENT dialect is two fragments spliced together.
    if [ "$_n_end" -eq 0 ]; then
      _summary_refusal_or_defer "the summary at this path was cut short mid-write" "summary-truncated; '$RESULT_LINE' is present but the closing '==== END AGENT-GATE … SUMMARY ====' marker is not — the write was cut short (kill or ENOSPC) and will never complete, so this verdict was never published" || break
    fi
    # (a mismatched dialect and out-of-order fields were already refused above, for every
    # path; only truncation is specific to believing a terminal verdict.)
    verdict COMPLETE 0 "the summary carries a terminal verdict — $RESULT_VALUE" ;;
  INCOMPLETE)
    : ;;  # the interesting case: fall through to the heartbeat
  *)
    # TERMINATION IS ONLY PROVEN BY A COMPLETE BLOCK (roborev job 221). Job 220 established that an
    # unrecognised RESULT token means the gate terminated, so it must not defer to a beat. But that
    # is true only of a summary that was actually FINISHED: a truncated or interrupted write can
    # contain a partial `RESULT:` line and no closer, and treating that as termination bypasses the
    # heartbeat confirmation exactly when a stale matching beat should establish STALLED. The
    # recognised-terminal path above already required the closer for this reason; this branch was
    # added without it.
    if [ "$_n_end" -eq 0 ]; then
      _summary_refusal_or_defer "the summary at this path was cut short mid-write" "summary-truncated; '$RESULT_LINE' is present but the closing '==== END AGENT-GATE … SUMMARY ====' marker is not — the write was cut short (kill or ENOSPC), so nothing in it proves the gate terminated" || break
    fi
    _summary_terminal_unknown "unrecognised-result; verdict token '$RESULT_TOKEN' (from '$RESULT_LINE') is not a value this reader knows — the summary is COMPLETE and names this run, so the gate TERMINATED; this reader cannot name the verdict, and a heartbeat cannot answer for it" ;;
esac

  break
done

# ---- the heartbeat side: affirmative liveness, or an affirmative death ----------
if [ ! -f "$HB" ]; then
  verdict UNKNOWN 4 "no-heartbeat-artifact; the summary is INCOMPLETE and no beat exists at $HB (a gate predating the heartbeat, or an unwritable path) — absence is NOT evidence of death"
fi
if [ ! -r "$HB" ]; then
  verdict UNKNOWN 4 "heartbeat-unreadable; $HB exists but cannot be read"
fi
_HB_SNAP=$(_snap_of "$HB" heartbeat) || _HB_SNAP=""
if [ -z "$_HB_SNAP" ]; then
  verdict UNKNOWN 4 "heartbeat-unsnapshotable; could not take a private copy of $HB to read it consistently"
fi
if _has_nul "$_HB_SNAP"; then
  verdict UNKNOWN 4 "heartbeat-contains-nul; $HB holds NUL bytes, so it is not a single coherent beat"
fi
HB_TEXT=$(_slurp "$_HB_SNAP")

_beat_valid "$HB_TEXT" || verdict UNKNOWN 4 "$BEAT_ERR"

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

HB_EPOCH=$(_field "$HB_TEXT" beat-epoch); HB_EPOCH=$((10#$HB_EPOCH))
case "$HB_EPOCH" in
  ''|*[!0-9]*) verdict UNKNOWN 4 "heartbeat-unparseable-epoch; 'beat-epoch: $HB_EPOCH' is not an integer" ;;
esac
HB_INTERVAL=$(_field "$HB_TEXT" interval); HB_INTERVAL=$((10#$HB_INTERVAL))
case "$HB_INTERVAL" in
  ''|*[!0-9]*) verdict UNKNOWN 4 "heartbeat-unparseable-interval; 'interval: $HB_INTERVAL' is not an integer" ;;
esac
[ "$HB_INTERVAL" -ge 1 ] || verdict UNKNOWN 4 "heartbeat-bad-interval; 'interval: $HB_INTERVAL' must be >= 1"
# An interval the confirmation window cannot span would be reported STALLED for a perfectly LIVE
# gate (roborev job 189, Medium): the window is capped at 65s to stop a hostile or misconfigured
# artifact stretching the wait, so a beat declaring an interval above 60s may legitimately not
# advance within it. Refusing to answer is correct; guessing STALLED would send a lane to re-run a
# healthy gate. The gate's own interval is a fixed 20s, so this can only be a foreign or
# hand-made beat.
if [ "$HB_INTERVAL" -gt 60 ]; then
  verdict UNKNOWN 4 "heartbeat-interval-too-long; 'interval: ${HB_INTERVAL}s' exceeds the 60s this reader can observe (its confirmation window is capped at 65s to bound a hostile artifact), so a live beat might not advance inside it and STALLED would be a false death"
fi

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
# A future epoch is only MEANINGFUL inside a proven shared clock domain (roborev job 178).
# Rejecting it first meant a LIVE beat from another host whose clock runs ahead returned UNKNOWN
# without ever reaching the counter-progression check that exists precisely for that case. Off a
# shared clock the epoch carries no information in EITHER direction, so it must not produce a
# verdict of its own; the clock domain is resolved below, and this rejection is applied there.
# RAW_AGE keeps the signed value: the future-epoch check happens LATER (once the clock domain
# is known), and clamping before it would hide exactly the anomaly it looks for — measured, as a
# future-dated beat reporting RUNNING with "age 0s".
RAW_AGE="$AGE"
[ "$AGE" -ge 0 ] || AGE=0

HB_PID=$(_field "$HB_TEXT" gate-pid)
HB_SEQ=$(_field "$HB_TEXT" beat-seq); HB_SEQ=$((10#${HB_SEQ:-0}))
HB_CHECK=$(_field "$HB_TEXT" parent-check)
_where="run-id $HB_RUN_ID, gate-pid ${HB_PID:-unknown}, beat ${HB_SEQ:-?}, age ${AGE}s, window ${STALE_AFTER}s"
# parent-check declares HOW the beater verifies its gate on the gate's own host. Surfaced as
# a DIAGNOSTIC only: no verdict here depends on it (see the descope note below).
[ -n "$HB_CHECK" ] && _where="$_where, parent-check $HB_CHECK"

# CLOCK DOMAIN (roborev job 169, Medium). Round 6 made STALLED clock-independent and left
# RUNNING comparing clocks — an incomplete fix, and the reviewer was right that it is exploitable
# in the other direction: a DEAD beat written by a host whose clock ran AHEAD later falls inside
# the freshness window and reads RUNNING, with no sequence advancing, so a lane waits forever on
# a gate that is gone. (A beat that is ahead *right now* is already caught as
# `heartbeat-in-the-future`; the problem is the same beat re-read later.)
#
# So the epoch may only decide anything when the writer and reader demonstrably share a clock:
# the beat names its `host:`, and if that is THIS host the timestamps are commensurable. This is
# a SCOPE test, not the evidence-for-death that was descoped — and its residual is stated: two
# boxes sharing a hostname and a filesystem would be treated as one clock domain. The
# consequence there is a possibly-wrong RUNNING/STALLED, never a claim that a process is dead.
#
# Outside a proven shared clock domain, BOTH answers come from counter progression below, which
# compares no clocks at all.
HB_HOST=$(_field "$HB_TEXT" host)
# Unverified means EMPTY, never the literal `unknown` (roborev job 221) — see the startup probe above.
MY_HOST=$(uname -n 2>/dev/null || true)
case "$MY_HOST" in unknown) MY_HOST="" ;; esac
_shared_clock=no
case "$HB_HOST" in unknown) HB_HOST="" ;; esac
if [ -n "$HB_HOST" ] && [ -n "$MY_HOST" ] && [ "$HB_HOST" = "$MY_HOST" ]; then
  _shared_clock=yes
  _where="$_where, clock-domain shared ($MY_HOST)"
else
  _where="$_where, clock-domain UNPROVEN (beat host '${HB_HOST:-absent}' vs '$MY_HOST')"
fi
if [ "$_shared_clock" = yes ] && [ "$RAW_AGE" -lt $(( -HB_INTERVAL )) ]; then
  # Same clock, yet the beat is dated in the future: that is a genuine anomaly (a clock step, or
  # a hand-edited artifact), and it must not read as fresh forever.
  verdict UNKNOWN 4 "heartbeat-in-the-future; beat-epoch $HB_EPOCH is $(( -RAW_AGE ))s ahead of this host's clock, and the beat claims THIS host — so the timestamp is not trustworthy. $_where"
fi
# A beat whose beater could not establish ANY identity for its gate (`parent-check: kill0`)
# cannot support an affirmative RUNNING from the timestamp alone (roborev job 185): after a reap
# the beater may be publishing for a RECYCLED pid, so the beat proves the beater is alive, not
# that the gate is. Counter progression below is the same evidence and no weaker, so such a beat
# simply takes that path. With the portable `lstart` fallback in place this is now rare — it
# needs a host with neither /proc nor a working `ps -o lstart=`.
# CLOSED grammar for parent-check, and `kill0` cannot earn RUNNING at all (job 189, Medium+Low).
#
# The previous version merely pushed a `kill0` beat onto the counter-progression path. That was
# not enough: progression proves the BEATER is alive, not that its GATE is — and `kill0` means the
# beater could not identify its gate, so after a pid recycle it may be beating happily for a
# stranger while the original gate is long gone. There is no evidence in the artifact that can
# rescue a RUNNING claim there, so the honest verdict is UNKNOWN.
#
# This costs almost nothing now: `kill0` requires a host with neither /proc NOR a working
# `ps -o lstart=`, which the tiered identity makes vanishingly rare. An unrecognised value is
# likewise UNKNOWN rather than assumed benign.
case "$HB_CHECK" in
  starttime|lstart) : ;;
  kill0)
    verdict UNKNOWN 4 "heartbeat-no-gate-identity; the beater reports 'parent-check: kill0', meaning it could NOT establish any identity for its gate — so a recycled pid would keep it publishing for an unrelated process. Counter progression would only prove the BEATER is alive, not the gate, so no RUNNING claim is supportable from this beat. $_where" ;;
  '')
    verdict UNKNOWN 4 "heartbeat-no-parent-check; the beat declares no 'parent-check:' field, so it is unknown whether the beater can identify its gate at all" ;;
  *)
    verdict UNKNOWN 4 "heartbeat-unknown-parent-check; 'parent-check: $HB_CHECK' is not a value this reader knows (expected starttime, lstart or kill0)" ;;
esac
if [ "$_shared_clock" = yes ] && [ "$AGE" -le "$STALE_AFTER" ]; then
  verdict RUNNING 2 "this run beat ${AGE}s ago on this host — it is alive and has not reached a verdict yet; $_where"
fi

# ---- decide by COUNTER PROGRESSION: no clocks compared ------------------------------
# Reached when the beat looks stale, OR when the clock domain is unproven (so the epoch may
# not be trusted in EITHER direction).
# `AGE` compares the WRITER's self-reported `beat-epoch` against the READER's clock, and
# nothing guarantees those clocks agree (roborev job 166, Medium). A gate host running more
# than one window behind would have EVERY fresh beat reported STALLED — and the documented
# response to a persistent STALLED is "relaunch", so a clock skew could cause a DUPLICATE
# gate launch. Comparing two clocks is the same class of cross-machine assumption this script
# has already been burned by twice, so the fix REMOVES the assumption rather than
# special-casing it.
#
# `beat-seq` is a counter the writer increments. Watching it advance over an interval THIS
# process times uses only the reader's clock for the wait and only the writer's counter for
# progress — the two are never compared. If it advances, the writer is alive whatever its
# clock says.
#
# The cost lands only on the verdict that is expensive to get wrong: a genuinely fresh beat
# already returned RUNNING above without waiting at all. The wait is bounded by the beat's own
# declared interval and hard-capped, so a misconfigured or hostile artifact cannot stretch it.
_confirm_wait=$(( HB_INTERVAL + 5 ))
[ "$_confirm_wait" -le 65 ] || _confirm_wait=65
if [ -n "$NO_WAIT" ]; then
  verdict UNKNOWN 4 "confirmation-skipped; this beat is not fresh and --no-wait forbids the second sample that would distinguish a stalled run from a slow one, so no STALLED claim is supportable. run-id ${HB_RUN_ID:-?}, beat ${HB_SEQ:-?}, age ${AGE}s, window ${STALE_AFTER}s"
fi
sleep "$_confirm_wait"
# THE GATE MAY HAVE FINISHED DURING THE WAIT (roborev job 228), AND THE RE-DECISION MUST USE THE REAL
# GRAMMAR (roborev job 231). If the gate completed and stopped its beater before another beat, the
# counter cannot advance and this code would report STALLED while a TERMINAL SUMMARY now exists.
#
# Job 228's fix parsed the fresh summary HERE, with a deliberately narrow check, and I argued that being
# "promote-only" made the small overlap with the main grammar safe. **That argument was wrong.** It
# counted openers and closers but never checked dialect match, field ordering, or duplicate
# `RESULT`/`run-id`, so a SPLICED summary could be promoted to COMPLETE — and promoting on a malformed
# artifact IS a false certification, the worst verdict this script can give. It also sent a valid block
# carrying an unrecognised token to STALLED instead of UNKNOWN, contradicting job 220.
#
# So there is no second parser. If the summary CHANGED during the wait, re-exec this script with the
# caller's original request plus `--no-wait`: the whole framing grammar, the run-id binding, the
# terminal dispatch and `_summary_terminal_unknown` all apply exactly as they do on a first read, and
# `--no-wait` guarantees termination by forbidding a second sleep.
if [ -z "$NO_WAIT" ]; then
  _post_snap=$(_snap_of "$SUMMARY" postwait 2>/dev/null) || _post_snap=""
  # A SNAPSHOT FAILURE IS NOT "NO CHANGE" (roborev job 241). If the summary was readable before the wait
  # and cannot be snapshotted after it — deleted, replaced by a directory, permissions changed — then
  # whether the gate completed during the wait is UNKNOWABLE, and continuing from the stale INCOMPLETE
  # snapshot would report STALLED on evidence that no longer exists. Absence of a measurement read as
  # absence of change: the same shape as `unknown` hostnames matching, and as an unrecognised
  # `ActiveState` reading as stopped.
  #
  # Present-then-missing is the only failing case: if the summary was ALREADY absent, a failed snapshot
  # is consistent with that and the heartbeat side answers as before.
  if [ -n "$_SUM_SNAP" ] && [ -z "$_post_snap" ]; then
    verdict UNKNOWN 4 "summary-unreadable-after-wait; $SUMMARY was readable before the confirmation wait and cannot be read after it, so whether the gate reached a verdict during the wait is unknowable — reporting STALLED from the pre-wait snapshot would rest on evidence that no longer exists"
  fi
  # A summary that did not exist before and exists NOW is the most important change of all — it is the
  # gate finishing — and the first version could not see it, because it required the INITIAL snapshot to
  # be non-empty before comparing. Absent-then-present must count.
  if [ -n "$_post_snap" ] && { [ -z "$_SUM_SNAP" ] || ! cmp -s "$_post_snap" "$_SUM_SNAP" 2>/dev/null; }; then
    # `exec` REPLACES this process, so the EXIT trap never runs and the private snapshot directory
    # leaks (roborev job 238). This is a regression of a known class here: an earlier revision leaked
    # 868 of them by assigning SNAP_DIR inside a subshell. Clean up explicitly before handing over.
    [ -n "${SNAP_DIR:-}" ] && rm -rf "$SNAP_DIR" 2>/dev/null
    # Same bash-3.2 + `set -u` empty-array rule as gate-detached.sh (roborev job 319). NOT reachable
    # with an empty array today — a no-arg run exits at the usage check long before here — so this is
    # uniformity, not a live defect. The safe form costs nothing and does not depend on that staying
    # true, which is the argument this file already makes for its other defensive expansions.
    exec bash "$0" ${GL_ORIG_ARGS[@]+"${GL_ORIG_ARGS[@]}"} --no-wait
  fi
fi
_hb2_snap=$(_snap_of "$HB" heartbeat2) || _hb2_snap=""
_hb2=""
[ -n "$_hb2_snap" ] && ! _has_nul "$_hb2_snap" && _hb2=$(_slurp "$_hb2_snap")
# The SECOND snapshot clears the SAME bar as the first (roborev job 190). Accepting it on a
# matching run-id plus a "different" beat-seq let a malformed or truncated re-read carry a RUNNING
# verdict — and "different" is itself too weak: a peer's smaller counter differs too. Progress
# means STRICTLY GREATER.
# WHY A CHANGED beater-pid ALSO COUNTS AS PROGRESS (roborev job 191, Medium).
#
# Round 15 tightened this from "the counter differs" to "the counter is strictly greater", to stop
# a peer's SMALLER counter passing as progress. That opened the opposite hole: every replacement
# beater starts its counter at 1, and the gate respawns its beater at component boundaries
# (_hb_ensure). A restart inside this confirmation window therefore produces a LOWER second
# sequence — and a live gate would be reported STALLED. That is the precise false-death this whole
# script is built to avoid, introduced by my own fix for the previous hole.
#
# The resolution needs no new field: a CHANGED `beater-pid` under the SAME run-id is itself
# affirmative evidence the gate is alive, because the only thing that starts a new beater for a
# run is that run's own gate reaching a component boundary. So progress is either the counter
# advancing within one beater incarnation, or the incarnation changing.
#
# The run-id equality check is what keeps this sound: a different run-id is a peer, and a peer's
# beater pid tells us nothing about our gate.
# STALLED IS A POSITIVE VERDICT AND NEEDS TWO VALID SAMPLES (roborev job 198, Medium). The first
# version left `_advanced=no` whenever the second snapshot could not be copied, held NULs, failed
# validation, or belonged to another run — and then reported STALLED. That collapses "I could not
# measure" into "I measured no progress", which is the one thing this script's own header forbids:
# never derive a positive verdict from the absence of a bad signal. A transient read failure or a
# concurrent replacement would have stalled a perfectly live gate.
_advanced=no
_adv_why=""
_conf_err=""
if [ -z "$_hb2_snap" ]; then
  _conf_err="the confirmation snapshot of $HB could not be taken"
elif [ -z "$_hb2" ]; then
  _conf_err="the confirmation snapshot of $HB held NUL bytes, so it is not one coherent beat"
elif ! _beat_valid "$_hb2"; then
  _conf_err="the confirmation snapshot did not validate: $BEAT_ERR"
elif [ "$(_field "$_hb2" run-id)" != "$HB_RUN_ID" ]; then
  _conf_err="the confirmation snapshot belongs to run '$(_field "$_hb2" run-id)', not '$HB_RUN_ID'"
fi
if [ -n "$_conf_err" ]; then
  verdict UNKNOWN 4 "confirmation-unmeasurable; $_conf_err — so no SECOND valid sample was observed and progression could not be tested. This is NOT a stall: STALLED requires two valid samples of the SAME run showing no progress. Re-read. $_where"
fi
if [ -n "$_hb2" ] && _beat_valid "$_hb2"; then
    # Explicit base 10 on both sides. NOT a bug fix: `[ x -gt y ]` in bash parses with base 10
    # (verified — `[ 011 -gt 08 ]` is true, i.e. 11 > 8, and `08` raises no error), unlike `$(( ))`
    # which rejects `08` as invalid octal. So the leading-zero hazard job 194 flagged does not
    # actually reach this comparison. The normalisation stays anyway, because the difference between
    # `[ -gt ]` and `(( ))` is exactly the kind of thing a later edit changes without noticing —
    # and it makes the intent legible instead of resting on which comparison primitive is in use.
    _seq2=$(_field "$_hb2" beat-seq); _seq2=$((10#${_seq2:-0}))
    _rid2=$(_field "$_hb2" run-id)
  _bpid1=$(_field "$HB_TEXT" beater-pid)
  _bpid2=$(_field "$_hb2" beater-pid)
  if [ "$_rid2" = "$HB_RUN_ID" ]; then
    if [ "$_seq2" -gt "$HB_SEQ" ] 2>/dev/null; then
      _advanced=yes; _adv_why="beat-seq advanced $HB_SEQ->$_seq2"
    elif [ -n "$_bpid1" ] && [ -n "$_bpid2" ] && [ "$_bpid1" != "$_bpid2" ]; then
      # A new incarnation: the gate relaunched its beater, which only a live gate does.
      _advanced=yes; _adv_why="the beater was RELAUNCHED (beater-pid $_bpid1->$_bpid2), which only this run's own live gate does at a component boundary"
    fi
  fi
fi
if [ "$_advanced" = yes ]; then
  verdict RUNNING 2 "$_adv_why over a ${_confirm_wait}s window timed on THIS host — the writer is alive. Decided by counter progression, comparing no clocks (the epoch read ${AGE}s old here, which is not trusted for this run). $_where"
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
# gate relaunches its beater at every component boundary AND while queued for the #1825 slot
# (job 322), so a live gate whose beater alone died RECOVERS to RUNNING within one component.
# Re-read before acting; if it is still STALLED after longer than the LONGEST COMPONENT OF THAT
# RUN, treat the gate as gone.
#
# THE EMITTED TEXT MUST NOT CARRY A FIXED DURATION (roborev job 323). It used to say "the longest
# component is ~850s", and this change corrected that figure in four DOCUMENTATION sites while
# leaving the one an operator actually reads -- the reader's own output -- untouched. Correcting a
# claim at the sites you happen to notice is not correcting it; the disclosure path needs its own
# census. The number was understated 2.4x (measured: tooling-tests at 2073s), and under-waiting
# relaunches a LIVE gate, which is the two-gates-on-one-summary outcome this issue exists to stop.
#
# AND THE RETIRED FIGURE STAYS OUT OF THE EMITTED STRING (caught by 4b.202 on my own fix). The first
# version of this fix explained the history INSIDE the operator-facing text — so the output still
# printed "~850s", as a warning rather than as advice, which an operator skimming for a number would
# read as the number. A diagnostic must not carry the value it warns against; that history belongs
# here, in the comment, where only someone editing the code will read it.
verdict STALLED 3 "no liveness (staleness window ${STALE_AFTER}s): beat-seq did NOT advance over a ${_confirm_wait}s window timed on this host (and the beat reads ${AGE}s old against this clock). This is NOT a claim that the process is dead: a beater can die under a live gate, and the gate relaunches it at the next component boundary AND while it is queued for the gate slot. Re-read shortly; if it is still STALLED after LONGER THAN THE LONGEST COMPONENT OF THIS RUN, treat the gate as gone and relaunch it -- read that duration off the component table in the summary block (\`<name>: PASS (<n>s)\`) rather than from any fixed figure, because it grows as components are added. $_where"
