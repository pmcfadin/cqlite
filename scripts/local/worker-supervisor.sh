#!/usr/bin/env bash
# scripts/local/worker-supervisor.sh — unattended worker recycle loop (issue #2090).
#
# One issue, one worker session, one process exit (context economy). This
# supervisor is what turns that into a safe *overnight loop*: single-instance
# lock, fail-closed preflight (wait, never spin), a crash-loop breaker, hard
# issue/wall-clock budgets, and one journal line + notification per iteration.
# The worker itself is opaque to this script — it is judged ONLY by its exit
# code and the ITERATION MARKER it leaves behind.
#
# ============================================================================
# ITERATION MARKER CONTRACT (mirrored in
# docs/scratch/agentic-workflow-audit/doc-deltas-supervisor.md — the worker
# skill MUST implement exactly this contract)
# ============================================================================
# The worker's LAST act, whatever else happened, is to write JSON to
# $MARKER_FILE (default: <repo-root>/.worker-last-iteration.json):
#
#   {"outcome":"finalized|no-work|blocked","issue":<int|null>,
#    "pr":"<url>|null","duration_s":<int>,"reason":"<string, required if blocked>"}
#
#   finalized — claimed an issue, drove it through gate/review/merge-on-green,
#               flow-finalized it. issue + pr MUST be set (both non-null,
#               non-missing) — a "finalized" marker with a null/missing issue
#               or pr is judged ABNORMAL (counts toward BREAKER_N, does NOT
#               count toward MAX_ISSUES) rather than trusted at face value.
#               A well-formed "finalized" is still NOT trusted on field shape
#               alone (issue #2670): the supervisor VERIFIES the claimed PR is
#               actually merged via `gh pr view <pr> --json state,mergedAt,
#               autoMergeRequest` before crediting the iteration. Verdicts, in the
#               journal's `verified:` field:
#                 verified=merged           — gh reports state MERGED. Counts
#                                             toward MAX_ISSUES, resets the breaker
#                                             (the ONLY path that credits an issue).
#                 verified=pending-automerge — OPEN with auto-merge armed (the
#                                             closer's documented auto-merge path).
#                                             The PR WILL land — a legitimate PENDING
#                                             state, not a false finalize: does NOT
#                                             count toward MAX_ISSUES yet, a
#                                             default-priority page, breaker-NEUTRAL.
#                                             Outcome `finalized-pending-automerge`
#                                             (roborev 1813).
#                 verified=mismatch:<STATE> — a STABLE non-MERGED state without
#                                             auto-merge armed, confirmed across
#                                             MISMATCH_RETRIES grace reads (absorbing
#                                             read-after-merge lag). Judged ABNORMAL:
#                                             does NOT count toward MAX_ISSUES, counts
#                                             toward the breaker, HIGH page naming the
#                                             discrepancy.
#                 verified=mismatch:UNRESOLVED — the marker's `pr` is FORGED: a
#                                             non-numeric ref, a URL that is not
#                                             EXACTLY a pmcfadin/cqlite PR URL, gh
#                                             could not resolve it (stderr signature),
#                                             or a cleanly-PARSED response lacking the
#                                             PR state. Same ESCALATION as mismatch.
#                 verified=unverified       — a TOOLING/TRANSPORT gap, NEVER forgery:
#                                             gh binary missing, network/auth/rate-limit
#                                             error, OR no JSON parser present / the
#                                             payload failed to parse (a tooling gap
#                                             must never read as forgery — roborev 1813).
#                                             FAIL-INFORMATIVE: logged + default-priority
#                                             page, does NOT count as a completed issue,
#                                             NEUTRAL to the breaker (neither trips nor
#                                             resets). Outcome `finalized-unverified`. A
#                                             PERSISTENT outage is bounded: UNVERIFIED_MAX
#                                             consecutive unverified finalizes STOP the
#                                             loop (verify-unavailable, exit 1, HIGH page)
#                                             so the MAX_ISSUES ceiling can't drift.
#               Bounded holds: a leftover-process preflight hold that never clears
#               STOPS the loop loudly — naming the surviving PIDs — and every hold pass
#               re-checks the stop-file and the wall-clock budget (issue #2670, roborev
#               1810): a hold can never latch the supervisor silently. The two leftover
#               families are bounded SEPARATELY (roborev 1839): the non-self-clearing
#               worker-CLI orphan trips the TIGHT LEFTOVER_HOLD_MAX; the self-clearing
#               build/gate family gets the LOOSE BUILD_HOLD_MAX so a legitimate
#               concurrent full gate is waited out, never mistaken for a stuck orphan.
#   no-work   — rehydrated from the board, nothing Ready (or nothing to
#               resume). issue/pr may be null. Does NOT count toward
#               MAX_ISSUES; triggers a BACKOFF_NOWORK_SECS sleep so the loop
#               doesn't hot-poll an empty board. Resets the breaker.
#   blocked   — made progress but stopped short of merge for a reason needing
#               the owner (design-call finding, scope question, HOLD order,
#               unmet requirement). "reason" MUST be set. Does NOT count
#               toward MAX_ISSUES. Resets the breaker; the issue is
#               remembered (LAST_BLOCKED_ISSUE). If the SAME issue reports
#               "blocked" on two consecutive iterations, the supervisor treats
#               the queue as head-blocked and STOPS cleanly (high-priority
#               notify, exit 0) rather than looping until MAX_HOURS — it is
#               retried exactly once, never auto-retried indefinitely.
#
#               Two "blocked" reasons are a distinct CLEAN PARK (issue #2666,
#               park-and-resume) rather than an owner-escalation:
#                 reason=seam1-approval  — the worker hit Seam 1 (an unapproved
#                                          design spec) in an unattended session.
#                 reason=needs-decision  — a genuine mid-run owner decision.
#               For either, the worker's contract is: post ONE structured
#               question comment on the issue (rendered options + recommendation
#               + default), add the `needs-decision` label, write this marker
#               (optionally an extra "question" field = one-line question
#               summary for the page), and EXIT — releasing the machine. NEVER
#               wait, NEVER call AskUserQuestion unattended. The supervisor
#               judges these NORMAL: verdict `parked-on-owner`, never toward the
#               breaker, and normally does NOT head-block the queue (the labeled
#               issue is excluded from the worker's next pickup until the owner
#               answers and the label clears), fires ONE high-priority page, and
#               moves on to the next Ready issue. Safety valve (mirrors the
#               blocked-path F2 guard): if the SAME issue parks on two
#               consecutive iterations — the label evidently never applied, so
#               the pickup exclusion is not holding — the supervisor pages the
#               owner (head-blocked-on-decision) and STOPS cleanly rather than
#               re-asking one question until MAX_ISSUES.
#
# stuck-on-question (mid-iteration, no marker) — a worker WEDGED on an
#               interactive prompt (AskUserQuestion / permission prompt / menu)
#               in an unattended session never writes a marker; it just burns
#               MAX_ITER_SECS and would look "abnormal". A watchdog classifies
#               this on POSITIVE WEDGE EVIDENCE, not a bare substring match (the
#               Claude CLI routinely prints tool names like `AskUserQuestion` in
#               normal trace, so a whole-log substring match would misclassify
#               ordinary crashes as stuck and permanently defeat the breaker —
#               roborev 1773). Every STUCK_POLL_SECS the watchdog scans, and
#               declares `stuck-on-question` ONLY when ALL hold across TWO
#               consecutive scans: (a) the process is alive, (b) a prompt
#               signature is in the LAST ~20 log lines (tail, not whole file),
#               and (c) the log has not grown between the scans (a wedged prompt
#               emits nothing). It then pages the owner and records the verdict
#               when the worker later exits without a clean marker. NOT abnormal,
#               never toward BREAKER_N. A marker-less exit whose only signature is
#               a stray scrollback match (fails tail-locality or no-growth) stays
#               ABNORMAL and counts toward the breaker.
#
# Any other outcome value, a marker missing required fields, a nonzero worker
# exit code, OR no marker file present when the worker process exits => the
# iteration is judged ABNORMAL and counts toward BREAKER_N. The supervisor
# removes any pre-existing marker before every spawn — a marker left behind
# by a prior (possibly crashed) invocation must never be re-judged as this
# iteration's outcome.
# ============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# ---------------------------------------------------------------------------
# Config (env-overridable; all defaults per issue #2090)
# ---------------------------------------------------------------------------
MAX_ISSUES="${MAX_ISSUES:-4}"
MAX_HOURS="${MAX_HOURS:-8}"
# MAX_HOURS_SECS is DERIVED from MAX_HOURS in validate_numeric_knobs (the first thing
# main() does, before any preflight_wait path reads it — roborev 1819) rather than here,
# so the `$((MAX_HOURS * 3600))` integer arithmetic runs only AFTER MAX_HOURS is confirmed
# a valid integer (roborev 1842): a malformed MAX_HOURS pages a FATAL instead of silently
# deriving 0 (which would exit budget-wallclock rc=0) or crashing under `set -e`. An
# explicit MAX_HOURS_SECS override is honored (and integer-validated) there too.
MAX_HOURS_SECS="${MAX_HOURS_SECS:-}"
DISK_FLOOR_GB="${DISK_FLOOR_GB:-40}"
BREAKER_N="${BREAKER_N:-3}"
BACKOFF_NOWORK_SECS="${BACKOFF_NOWORK_SECS:-900}"
HOLD_POLL_SECS="${HOLD_POLL_SECS:-300}"
MAX_ITER_SECS="${MAX_ITER_SECS:-7200}"
# Mid-iteration stuck-on-question watchdog (issue #2666): how often to tail the
# live worker log for an interactive-prompt signature, and the signatures to
# match. Both env-overridable (tests tighten the poll interval).
STUCK_POLL_SECS="${STUCK_POLL_SECS:-30}"
PROMPT_SIGNATURE_RE="${PROMPT_SIGNATURE_RE:-AskUserQuestion|Do you want to|waiting for input|❯}"
# issue #2670: bound the ways verdict-integrity can otherwise latch the loop. The
# two leftover-process families are bounded SEPARATELY (roborev 1839 HIGH) because
# they behave differently:
# LEFTOVER_HOLD_MAX — consecutive preflight holds on the NON-self-clearing worker
#   family (`leftover-worker`: an orphaned `claude --agent worker` CLI from a prior
#   iteration that never dies) before the supervisor STOPS loudly. This must be a
#   TIGHT bound — a wedged worker CLI is a stuck-on-question hazard, not something to
#   wait out. Default 3 holds (3 x HOLD_POLL_SECS = 15 min at defaults).
# BUILD_HOLD_MAX — consecutive preflight holds on the SELF-CLEARING build/gate
#   family (`leftover-build`: cargo/nextest/gate_slot_daemon) before stopping. This
#   is a LOOSE bound: a legitimate concurrent full gate runs 15-25 min (per CLAUDE.md),
#   so cap x HOLD_POLL_SECS must comfortably exceed it or the loop would false-stop a
#   healthy box that merely has a gate running. Default 12 holds (= 1 h at 300 s poll).
#   A value <=0 DISABLES this bound (wait indefinitely for the build to clear — still
#   backstopped by the wall-clock budget), same <=0-disables semantics as
#   MISMATCH_GRACE_CAP_SECS.
# UNVERIFIED_MAX — consecutive finalized markers gh could not verify before the
#   supervisor STOPS. A persistent verification outage is an operator problem and
#   would otherwise let the MAX_ISSUES ceiling drift (uncounted-forever iterations).
LEFTOVER_HOLD_MAX="${LEFTOVER_HOLD_MAX:-3}"
BUILD_HOLD_MAX="${BUILD_HOLD_MAX:-12}"
UNVERIFIED_MAX="${UNVERIFIED_MAX:-2}"
# issue #3749: the SHARED git object store on this box (every lane here is a worktree of
# ONE `.git`) is rehashed with `git fsck` on a THROTTLED cadence from the per-iteration
# preflight path. This is the recurring half of #3749's control; the one-shot half is
# scripts/bootstrap-agent-machine.sh section 5d. Both go through the SAME script,
# scripts/check-object-store-integrity.sh — a second implementation would be a second
# place for the verdict to drift.
#
# OBJ_SWEEP_INTERVAL_HOURS — minimum hours between sweeps, throttled by a stamp file.
#   MEASURED COST IS A RANGE WITH CONDITIONS, NOT A NUMBER (#3749 review): on this
#   fleet's shared store (366M, git 2.43.0) two independent measurement sets give 13-24s
#   warm and 47-80s cold or under concurrent gates — the sweep is I/O-bound, so cache
#   state and box load dominate. An earlier revision of this comment quoted a single
#   "19.83s" from one warm run and was wrong by 2.5-4x. A box runs up to 4 lanes, so an
#   UNTHROTTLED sweep would burn ~50-320s of every iteration cycle across the box for a
#   property that changes on the timescale of disk faults, not minutes.
#   A value of 0 DISABLES the sweep (same `<=0 disables` semantics as BUILD_HOLD_MAX),
#   which is ANNOUNCED ONCE in the journal rather than left silent: a disabled hygiene
#   probe must be visible in the log, not inferred from the absence of its lines. It buys
#   no green anywhere — this sweep certifies nothing, it only refuses to run workers over
#   a store known to be damaged.
# OBJ_SWEEP_TIMEOUT_SECS — passed through as the sweep's own `--timeout`, and it is the
#   bound on EACH of the sweep's walks, of which there can be MAX_SWEEP_WALKS (declared
#   in the sweep script: the sweep, the reproduction discriminator when walk 1 is not
#   clean, and the `--no-reflogs` reachability-cause discriminator when both walks report
#   ERROR_REACHABLE). Worst-case wall time is therefore MAX_SWEEP_WALKS x this value. The
#   bound exists to stop a HANG, not to police duration, and a bound that expires on a
#   healthy-but-busy box yields UNMEASURED noise nobody acts on.
#   THE SUPERVISOR'S DEFAULT IS THE SCRIPT'S BOUND DIVIDED BY MAX_SWEEP_WALKS (200 vs
#   600/3), AND THAT IS THIS CALLER'S LATENCY BUDGET RATHER THAN A DIFFERENT VIEW OF THE
#   STORE (#3749 review round 3, item 3; re-derived in round 4 when the third walk was
#   added). The sweep runs inside a CHILD process, so nothing here can check the stop
#   file or the wall-clock budget BETWEEN its walks; the only lever this caller has is
#   how long they may take in total. At 200 the worst case stays 600s — one walk at the
#   script's own bound — which is the property round 3 fixed and round 4 must not undo by
#   adding a walk. 200s is ~2.5x the observed COLD worst case (47-80s on this fleet's
#   366M store) and ~12x the warm one; the third walk is the CHEAPEST of the three
#   (measured 12.5s vs 24s on the live store, because excluding the reflogs is what it
#   does). The other caller, scripts/bootstrap-agent-machine.sh, keeps 600: machine
#   onboarding is a one-shot step where nobody is waiting on a stop file.
#   THE TRADE, STATED: this is stop latency bought with UNMEASURED headroom. A walk that
#   runs past 200s under load pages UNMEASURED here where it would have completed at 300.
#   Nothing on this fleet has been observed above 80s, and nothing monitors it either.
#   STRICTLY POSITIVE: the sweep rejects 0 as a usage error, which would turn the probe
#   into a permanent UNMEASURED — a silently self-disabling bound, the shape
#   validate_numeric_knobs exists for.
OBJ_SWEEP_INTERVAL_HOURS="${OBJ_SWEEP_INTERVAL_HOURS:-6}"
OBJ_SWEEP_TIMEOUT_SECS="${OBJ_SWEEP_TIMEOUT_SECS:-200}"
# Empty => derived per SHARED STORE in obj_sweep_stamp_path (below), so lanes sharing one
# object store share one throttle. It also names the STOP latch (`<stamp>.STOP`), so
# pinning it relocates both files together.
OBJ_SWEEP_STAMP="${OBJ_SWEEP_STAMP:-}"
# issue #2670 (roborev 1813): before escalating a non-merged PR state to a
# mismatch, re-read gh a few times to absorb read-after-merge lag. Env-tunable so
# the tooling tests set the wait to 0 (nothing sleeps in the suite).
MISMATCH_RETRIES="${MISMATCH_RETRIES:-3}"
MISMATCH_RETRY_WAIT_SECS="${MISMATCH_RETRY_WAIT_SECS:-10}"
# Hard ceiling on TOTAL mismatch-grace wall-clock regardless of retries*wait
# (roborev 1819 MED): even a misconfigured MISMATCH_RETRIES/WAIT can never make a
# single verification block longer than this. EXPLICIT semantics (roborev 1821):
# a value >0 is the wall-clock ceiling; a value <=0 DISABLES the wall-clock cap
# entirely, leaving grace bounded solely by the retry count (MISMATCH_RETRIES).
MISMATCH_GRACE_CAP_SECS="${MISMATCH_GRACE_CAP_SECS:-120}"
# issue #2670 (roborev 1819/1839): auto-merge-stuck detection is PER-PR. Under the
# documented `gh pr merge --auto` closer path, a finalize that verifies as OPEN+armed
# is the NORMAL healthy state, and a fast fleet legitimately has several DISTINCT PRs
# pending at once — so a distinct armed PR is NOT evidence that the same PR is stuck.
# The supervisor re-verifies each still-pending PR on later iterations, retroactively
# CREDITS ones that reach MERGED toward MAX_ISSUES, and only STOPS loudly when the
# SAME PR is observed still-unmerged across this many consecutive iterations (a PR
# whose auto-merge genuinely never lands).
PENDING_AUTOMERGE_MAX="${PENDING_AUTOMERGE_MAX:-3}"
# PENDING_AUTOMERGE_MIN_SECS (roborev 1840): the observation count alone is not enough
# to declare a PR stuck — a burst of fast no-progress iterations (e.g. several quick
# owner-decision parks, which have no backoff) could rack up PENDING_AUTOMERGE_MAX
# observations of a PR whose CI simply hasn't finished yet. So automerge-stuck ALSO
# requires the PR to have been pending at least this long (wall-clock, comfortably above
# CI time). Default 20 min.
PENDING_AUTOMERGE_MIN_SECS="${PENDING_AUTOMERGE_MIN_SECS:-1200}"

# ONE SUPERVISOR PER LANE, NOT PER MACHINE (roborev round 19, Medium). The default lock was
# machine-global, so a SECOND lane started with the documented default invocation exited during lock
# acquisition — which makes the per-lane claim refs this change adds unreachable in production with
# defaults, and N-lanes-per-box is the standing model (#3393 retracted #1930's one-worker-per-machine
# reading). This is that retracted invariant surviving in a SECOND mechanism: fixing the ref namespace
# left the flock still enforcing the assumption the namespace change exists to remove.
#
# Scoped by the LANE's checkout root, which is what "one instance" should have meant all along — the
# lock still prevents two supervisors in the SAME worktree, which is the failure it was built for. The
# basename is kept readable so an operator can tell which lane owns which lock, and a cksum of the
# full path disambiguates two lanes whose directories share a basename.
# Resolved LAZILY and with BUILTINS ONLY, never at source time (#3464 family 2, reintroduced and
# caught here). The first cut computed the lane discriminator with `tr` and `cksum` on the line
# below, at SOURCE time — which is exactly the mechanism that broke the placeholder token earlier in
# this change: several tests SOURCE this file with a deliberately stripped PATH to prove the no-jq /
# no-python3 paths, and an external tool in a source-time assignment fails there and silently yields
# an empty value. Two suite cases went red on it immediately. So: empty by default, filled in by
# `supervisor_lock_path` at acquisition, using only bash substitution and arithmetic.
SUPERVISOR_LOCK="${SUPERVISOR_LOCK:-}"
# `SUPERVISOR_LOCK` NAMES WHERE *OUR* LOCK LIVES, AND THAT IS ALL IT HAS EVER LEGITIMATELY DONE
# (#3549, lead ruling 2026-08-30 — AC4 REMOVED AS UNSOUND).
#
# #3549's AC4 said "an explicit `SUPERVISOR_LOCK` override skips the legacy check entirely". It is
# REMOVED, not descoped, and the proof is one sentence: an explicit `SUPERVISOR_LOCK` renames OUR lock,
# while a pre-#3467 supervisor uses the machine-global path REGARDLESS — it has never heard of this
# variable — so the skip disabled the check in a case where the collision is still LIVE. It conflated a
# naming choice with an isolation guarantee.
#
# CONSEQUENCE, RECORDED SO NOBODY REINTRODUCES A SKIP AS A CONVENIENCE: `supervisor_legacy_lock_guard`
# runs UNCONDITIONALLY. There is no opt-out, no provenance record and nothing for a caller to set to
# make the check not happen. The tri-state provenance (`SUPERVISOR_LOCK_DERIVED`), the pinned path
# beside it (`SUPERVISOR_LOCK_RESOLVED`) and every early return they fed were deleted with the skip:
# four review rounds (jobs 209 F1, 214, 217 F1, 218) were each a new route to bypassing the check
# through that record, and with no record there is no route. Do not add one back.
STOP_FILE="${STOP_FILE:-$REPO_ROOT/.worker-stop}"
MARKER_FILE="${MARKER_FILE:-$REPO_ROOT/.worker-last-iteration.json}"
LOG_DIR="${LOG_DIR:-$REPO_ROOT/logs/worker-supervisor}"

# ---------------------------------------------------------------------------
# Supervisor-authored git-ref claim (issue #2655 / #2499 design).
#
# The supervisor — a long-lived, LANE-scoped process — stamps a
# refs/lane-claims/<machine>/<lane-id> liveness ref (issue+PID+ts) at every worker spawn
# and clears it when it exits cleanly, so claim liveness is MECHANISM-driven and no
# longer depends on the worker LLM remembering to `beat`. The PID recorded is
# the SUPERVISOR's own ($$): it is the stable per-LANE anchor that outlives
# each recycled worker, so a same-machine reaper's process-liveness check tracks
# "is this lane's supervisor still running" rather than a transient worker
# subprocess. PER LANE, not per machine, since #3393: a box runs several lanes at once, and
# a single per-machine ref was force-updated by each of them, so siblings overwrote one
# another and at most one was observable. `refs/machine-claims/<machine>` is legacy and
# read-only — still drained, never written. When the supervisor dies, nothing refreshes the ref and it goes
# stale within the reap threshold.
#
# CLAIM_CMD is the claim-heartbeat.sh entrypoint; overridable so the tests can
# substitute a hermetic stub (no origin/network). Set CLAIM_CMD="" to disable
# claim stamping entirely (e.g. a machine with no origin push rights, or the
# tooling tests, which must never touch origin/gh).
#
# Use `${VAR-default}` (NO colon), NOT `${VAR:-default}`: the colon form
# substitutes the default for an EMPTY string too, which silently re-enabled the
# real claim-heartbeat.sh (git push / gh pr list — network ops) whenever a caller
# set CLAIM_CMD="" to disable it. That defeated the documented "set to empty to
# disable" contract and let a slow/contended origin push or `gh pr list` WEDGE the
# supervisor (issue #2849 — non-deterministic tooling-tests hang: the tests set
# CLAIM_CMD="" but hit the real network path anyway). The colonless form preserves
# an explicitly-empty override so disabling truly disables.
CLAIM_CMD="${CLAIM_CMD-bash $REPO_ROOT/scripts/flow/claim-heartbeat.sh}"

# LOCK_CMD is the `claim.sh` entrypoint — the PER-ISSUE LOCK, a different script and a different
# namespace from `CLAIM_CMD`'s per-lane liveness ref. The supervisor had no claim.sh seam at all, which
# is why round 34's finding 1 needed one. Colonless default like `CLAIM_CMD`, so `LOCK_CMD=""` genuinely
# DISABLES it (a `:-` form would silently re-enable the real network path — the #3464 trap that seam
# already fell into once).
LOCK_CMD="${LOCK_CMD-bash $REPO_ROOT/scripts/flow/claim.sh}"

# The actor every pre-upgrade claim was stamped with: `claim.sh`'s own fallback when nothing sets one.
LEGACY_CLAIM_ACTOR="flow"
# The machine identity the claim ref is scoped to — must match what the reaper
# clears. Defaults to claim-heartbeat.sh's own default (`hostname -s`), honoring
# HEARTBEAT_MACHINE when the fleet overrides it.
CLAIM_MACHINE="${HEARTBEAT_MACHINE:-$(hostname -s 2>/dev/null || echo unknown)}"
# Best-known issue for the CURRENT iteration's spawn stamp. Empty/unknown on the
# first iteration (the worker picks from the board); learned from each marker and
# reused as the next spawn's stamp. "0" is stamped when still unknown — the
# reaper treats a non-issue as "no open-PR guard applicable" and governs on age.
CLAIM_ISSUE="${CLAIM_ISSUE:-}"
# The issue this supervisor LAST STAMPED a lane claim for (#3393). Distinct from CLAIM_ISSUE, which
# is the next spawn's issue and is cleared on `finalized`; this one must survive that so clean exit
# can delete the ref it actually created. "0" is a legitimate value — a stamp with an unknown issue
# still creates a real ref that must be cleared.
CLAIM_STAMPED_ISSUE=""
# The SHA this supervisor last stamped for CLAIM_STAMPED_ISSUE, used as the `reap` LEASE so a delete
# can never remove a ref another supervisor has since refreshed (roborev round 19, Medium). Empty
# means "not known" — a lease is then not passed, and that is logged rather than silently skipped.
CLAIM_STAMPED_SHA=""
# Whether the work this supervisor last claimed has CONCLUDED — finalized, or released to someone else
# (an owner park). 1 also covers "nothing has been claimed yet", so a supervisor that exits before its
# first iteration deletes nothing it did not create (roborev round 23, Medium).
CLAIM_WORK_CONCLUDED=1
# A per-INVOCATION token for the placeholder lane id (#3393, roborev round 3). `p$$` alone is unique
# only among CURRENTLY RUNNING processes: after a crash or reboot, pid reuse lets a new supervisor
# write the SAME placeholder ref as a dead lane's, overwriting it with a fresh timestamp and a live
# pid — masking the dead lane, which is the exact failure per-lane refs exist to prevent.
#
# BUILT FROM BUILTINS, AND LAZILY. The first cut computed it at SOURCE time with `tr`/`cut`, and the
# suite caught why that is wrong: `test_parser_absent_is_unverified` SOURCES this file under a
# minimal PATH (bash mktemp cat rm grep dirname date) to unit-test one function, so an external
# command at source time aborted the source and the test read an empty verdict. Sourcing this file
# must cost nothing and require nothing. `$RANDOM`, `$EPOCHSECONDS` and `printf` are all bash
# builtins, and the value is computed on first use rather than at load.
# SETS the variable rather than echoing it, and the difference is load-bearing. Called as
# `$(claim_lane_token)` the cache assignment happens in a SUBSHELL and is lost, so every stamp got a
# FRESH token — which made each iteration look like a lane transition, reaped the previous ref, and
# defeated the per-invocation uniqueness this exists for. Caught by the assertion that a
# supervisor's stamps all name the SAME placeholder.
# Lane refs whose cleanup has not yet succeeded (#3393, roborev round 5). Retried on every stamp and
# on clean exit; a placeholder that stays here is a permanent stale claim, because automated reaping
# refuses placeholders by design.
CLAIM_PENDING_CLEANUP=""
CLAIM_LANE_TOKEN=""
# claim_drain_pending_cleanup: retry every lane ref whose deletion has not yet succeeded. Keeps the
# ones that still fail, so a transient push error is retried rather than lost.
# claim_drain_pending_cleanup [protect_id]: retry every lane ref whose deletion has not yet
# succeeded, NEVER touching <protect_id> — the lane currently stamped.
#
# THE PROTECTION IS THE WHOLE POINT (roborev round 6, Medium). Without it the queue can delete the
# ref it just stamped: if cleaning placeholder P fails during P -> issue, P stays queued; after
# finalization the next issue -> P transition REFRESHES P and then drains the queue, deleting that
# fresh CURRENT ref and leaving the running lane unobservable. That is the failure this whole change
# exists to prevent, produced by the retry logic added to fix a leak.
claim_drain_pending_cleanup() {
  [[ -n "$CLAIM_CMD" ]] || return 0
  [[ -n "${CLAIM_PENDING_CLEANUP// /}" ]] || return 0
  local protect="${1:-}" still="" entry id lease rc
  for entry in $CLAIM_PENDING_CLEANUP; do
    [[ -n "$entry" ]] || continue
    # `<lane-id>:<sha>`; a bare id (no colon) means no lease is known, which stays supported so an
    # entry queued by an older process is still drained rather than silently dropped.
    id="${entry%%:*}"
    lease="${entry#*:}"
    [[ "$lease" == "$entry" ]] && lease=""
    [[ -n "$id" ]] || continue
    if [[ -n "$protect" && "$id" == "$protect" ]]; then
      log "pending cleanup of ${id} dropped: it is the lane currently stamped, not a stale ref"
      continue
    fi
    # NO LEASE => NO DELETE (roborev round 32). Also covers a bare `<id>` entry queued by an older
    # process, where no lease was ever recorded: the entry is dropped from the queue rather than retried
    # forever, because retrying cannot acquire a lease that was never captured.
    if [[ -z "$lease" ]]; then
      log "pending cleanup of ${id} DROPPED: no lease was recorded for it, and an unleased delete could remove a successor's live claim — the CI reaper collects it with its own lease"
      continue
    fi
    rc=0
    HEARTBEAT_MACHINE="$CLAIM_MACHINE" $CLAIM_CMD reap "$CLAIM_MACHINE" "$id" "$lease" >/dev/null 2>&1 || rc=$?
    if [[ "$rc" == 0 ]]; then
      log "stale lane ref ${id} cleared${lease:+ (lease held at ${lease})}"
    elif [[ "$rc" == 4 ]]; then
      # LEASE NOT HELD => OWNERSHIP TRANSFERRED, so DROP rather than retry (roborev round 19). The
      # ref moved after we stamped it, which means another supervisor now owns this lane id. Retrying
      # could only ever delete THAT supervisor's live claim, and the whole point of the lease is to
      # refuse. Retaining it would retry forever against a ref that is no longer ours.
      log "pending cleanup of ${id} dropped: the lease at ${lease:-<none>} is no longer held, so another supervisor owns this lane now"
    else
      still="${still} ${id}:${lease}"
      log "WARN: lane ref ${id} still not cleared (rc=${rc}: open PR, unreadable message or push error) — retained for retry"
    fi
  done
  CLAIM_PENDING_CLEANUP="$still"
}

claim_lane_token() {
  [[ -n "$CLAIM_LANE_TOKEN" ]] || CLAIM_LANE_TOKEN="$(printf '%04x%04x%x' "$RANDOM" "$RANDOM" "${EPOCHSECONDS:-0}")"
}

detect_ncpu() {
  if command -v nproc >/dev/null 2>&1; then nproc
  elif command -v getconf >/dev/null 2>&1 && getconf _NPROCESSORS_ONLN >/dev/null 2>&1; then getconf _NPROCESSORS_ONLN
  elif command -v sysctl >/dev/null 2>&1; then sysctl -n hw.ncpu
  else echo 4
  fi
}
LOAD_MAX="${LOAD_MAX:-$(detect_ncpu)}"

# Real invocation (documented default; every real fleet run should pin this
# explicitly rather than rely on the default prompt text drifting). Validated
# headless launch (issue #2841) — each flag fixes a distinct spawn failure:
#   -p (--print)                    non-interactive; runs the prompt to completion
#                                   and exits (matches the one-shot-per-iteration
#                                   model). WITHOUT it `claude` opens an interactive
#                                   TUI and blocks on keyboard input forever.
#   --output-format stream-json     with `-p`, emit a LIVE event stream to stdout
#     --verbose                     (required by the CLI for stream-json under -p)
#                                   so the supervisor's `>"$logfile"` redirect
#                                   captures non-empty worker activity and the
#                                   stuck-on-question watchdog (detect_prompt_signature
#                                   /log_size on iter-N.log) keeps working. Without a
#                                   stream flag `-p` writes only to the session
#                                   transcript and iter-N.log stays 0 bytes (design
#                                   decision A; see openspec change).
#   --dangerously-skip-permissions  a supervisor-spawned session has no human to
#                                   approve per-command prompts, so gh project / gh
#                                   auth / git worktree / git -C would be auto-denied.
#   --agent flow-lead               the registered orchestrator agent. `worker` is a
#                                   /-command/skill (.claude/commands/worker.md), NOT
#                                   a registered agent type — `--agent worker` exits 1.
#   '/worker'                       the prompt: invoke the worker skill (single-issue
#                                   session mode, #2090). The skill body carries the
#                                   full contract, so the inline prompt stays minimal.
if [[ -z "${WORKER_CMD:-}" ]]; then
  WORKER_CMD="claude -p --output-format stream-json --verbose --dangerously-skip-permissions --agent flow-lead '/worker'"
fi

# NOTE: default probe commands are assigned via explicit if/then blocks, not a
# compact "${VAR:-default}" one-liner — the deeply nested/escaped quoting that
# form requires corrupts the ALREADY-SET value at bash's parse stage (verified
# empirically: a real override got mangled even though the default clause is
# never evaluated when the var is set). Plain assignment has no such hazard.
if [[ -z "${LOAD_PROBE_CMD:-}" ]]; then
  # shellcheck disable=SC2016  # literal $1 for the later `bash -c` eval, not expanded now.
  LOAD_PROBE_CMD='if [ -r /proc/loadavg ]; then cut -d" " -f1 /proc/loadavg; else sysctl -n vm.loadavg | tr -d "{}" | awk "{print \$1}"; fi'
fi
if [[ -z "${DISK_PROBE_CMD:-}" ]]; then
  # shellcheck disable=SC2016  # single-quoted $4 is intentional: literal text for the later `bash -c` eval, not expanded now.
  DISK_PROBE_CMD="df -Pk \"$REPO_ROOT\" | awk 'NR==2{print int(\$4/1024/1024)}'"
fi
# Single source of the leftover-process match patterns (roborev 1821): BOTH the
# per-family count probes (PROC_PROBE_WORKER/BUILD_CMD) and list probes
# (PROC_LIST_WORKER/BUILD_CMD) DERIVE from these, so the "what counts as a leftover" set
# and the "what we name in the page" set can never drift apart. `[c]argo`/`[c]laude` are
# the bracket-trick forms (see below).
PROC_MATCH_BUILD='[c]argo |[n]extest|[g]ate_slot_daemon'
PROC_MATCH_WORKER='[c]laude.* (-p|--print)( |$).*--agent flow-lead'
# Leftover-process probes (issue #2670): two families of prior-iteration debris block
# the next spawn (HOLD-and-poll, same as load/disk), bounded SEPARATELY (roborev 1839)
# so each needs its OWN count probe:
#   (1) build/gate processes — cargo/nextest/gate_slot_daemon — SELF-CLEARING; a
#       legitimate concurrent full gate is one of these, so it gets the LOOSE
#       BUILD_HOLD_MAX (reason `leftover-build`).
#   (2) an orphaned worker Claude CLI from a prior iteration (a SIGTERM'd-wrapper
#       survivor and stuck-on-question hazard) — NON-self-clearing; gets the TIGHT
#       LEFTOVER_HOLD_MAX (reason `leftover-worker`).
# The Claude match is keyed on the supervisor's OWN spawn shape — the unattended
# headless launch `claude … -p … --agent flow-lead` (see WORKER_CMD, issue #2841) —
# NOT a bare `claude`. The print-mode token (`-p` or `--print`) is load-bearing and is
# matched as a WHITESPACE-DELIMITED token ` (-p|--print)( |$)` (roborev #2841): an
# UNANCHORED `-p` would also match the `-p` inside `--dangerously-skip-permissions`
# (ski-P-ermissions), misclassifying an interactive `claude --dangerously-skip-permissions
# --agent flow-lead` lead as a leftover worker. A plain interactive `claude` REPL has
# neither the print token nor `--agent flow-lead`, and a deliberate INTERACTIVE lead
# session (`claude --agent flow-lead`, no `-p`/`--print`) is likewise excluded, so a
# legitimate hand-run lead/REPL on the box is not misdetected as a leftover worker. Only
# the print-mode unattended spawn carries both the print token and `--agent flow-lead`. LIMIT: an
# operator who deliberately runs the full `claude -p … --agent flow-lead` shape by hand
# WILL be matched (correctly — by the one-worker-per-machine rule #1930). The current
# iteration's own worker has already exited before preflight runs, so any print-mode
# `--agent flow-lead` process seen here is from a prior iteration. The WORKER_CMD default
# and this pattern are coupled: test_worker_supervisor.sh asserts PROC_MATCH_WORKER
# actually matches the resolved default WORKER_CMD (anti-drift, roborev #2841).
#
# SELF-MATCH DEFUSED (roborev 1813, MED-HIGH): a live `bash -c` wrapper's OWN argv
# contains these pattern strings, so a naive pattern would match the wrapper and report
# a phantom leftover at EVERY boot. The classic pgrep bracket trick (`[c]argo`,
# `[c]laude`) makes each regex still match the real process (`cargo`, `claude`) while
# the bracketed pattern TEXT in the wrapper's argv (`[c]argo`) no longer contains the
# literal substring, so the wrapper cannot match itself.
#
# SELF-EXCLUSION (roborev 1819, LOW): additionally drop the supervisor's OWN pid ($$)
# and parent ($PPID) — expanded HERE at definition time so they are the supervisor's,
# not the probe subshell's. RESIDUAL LIMIT: the match is still an argv substring test,
# so a `claude --agent worker` process ELSEWHERE in the ancestry, or an unrelated
# process carrying the literal substring, could still be counted.
# LANE ATTRIBUTION (roborev round 35, High) — THE WORKER FAMILY IS PER-LANE, NOT PER-MACHINE.
#
# This probe counted EVERY matching worker on the box. That was right while one worker per machine was
# the model; it is wrong now, and it is wrong in the direction that defeats this whole change: with
# several lanes running, each supervisor sees its SIBLINGS' healthy workers, counts them as leftover
# debris, holds, and after LEFTOVER_HOLD_MAX polls STOPS. So per-lane claim refs would have shipped
# while multi-lane operation stayed serialized by a DIFFERENT machine-global mechanism — the FOURTH
# carrier of the retracted #1930 invariant found in this PR (after the supervisor lock, `worker.md`'s
# hard rule, and the shared claim actor).
#
# A pid is attributed to THIS lane by its working directory being REPO_ROOT or below. That catches the
# case a recorded pid cannot — an orphan from a PRIOR RUN of this lane, whose pid this process never
# knew — which is exactly what the probe exists for.
#
# AFFIRMATIVE ATTRIBUTION: a pid is counted only when its cwd is READ and matches. An unreadable cwd
# (the process exited mid-probe, a permission boundary, or a platform without /proc) is NOT counted.
# That is deliberate in both directions: the positive verdict here is "there IS leftover debris in my
# lane", so it needs a positive measurement, and the failure mode of guessing YES is the false STOP
# this finding is about. The cost is stated rather than hidden: on a platform without /proc, this
# probe stops detecting cross-run orphans instead of falsely stopping healthy lanes. Override
# PROC_PROBE_WORKER_CMD to restore machine-wide counting on such a host.
#
# The BUILD family is deliberately NOT lane-scoped: one gate at a time per MACHINE is a resource bound
# that survived #1930's retraction, so a sibling's cargo/nextest is genuinely this lane's business.
LANE_PID_FILTER="while read -r p; do c=\$(readlink \"/proc/\$p/cwd\" 2>/dev/null) || continue; case \"\$c\" in '$REPO_ROOT' | '$REPO_ROOT'/*) printf '%s\\n' \"\$p\" ;; esac; done"
# CAPTURED BEFORE THE DEFAULT IS APPLIED: `supervisor_resolve_lane_id` needs to know whether the
# OPERATOR chose this probe, and after the `:-` below every value looks chosen. An operator who
# supplied their own probe has taken responsibility for its attribution, so the
# lane-attribution-impossible refusal yields to them; the DEFAULT probe cannot make that claim.
PROC_PROBE_WORKER_CMD_OVERRIDDEN=""
[[ -n "${PROC_PROBE_WORKER_CMD:-}" ]] && PROC_PROBE_WORKER_CMD_OVERRIDDEN=yes
if [[ -z "${PROC_PROBE_WORKER_CMD:-}" ]]; then
  PROC_PROBE_WORKER_CMD="pgrep -f '$PROC_MATCH_WORKER' 2>/dev/null | grep -vxF -e '$$' -e '$PPID' | $LANE_PID_FILTER | wc -l | tr -d ' '"
fi
if [[ -z "${PROC_PROBE_BUILD_CMD:-}" ]]; then
  PROC_PROBE_BUILD_CMD="pgrep -f '$PROC_MATCH_BUILD' 2>/dev/null | grep -vxF -e '$$' -e '$PPID' | wc -l | tr -d ' '"
fi
# Companion PER-FAMILY list probes (roborev 1839/1821): a leftover-worker / leftover-build
# STOP names ONLY its OWN family's surviving PIDs (never unrelated PIDs from the other
# family). DERIVED from the SAME $PROC_MATCH_* patterns as the count probes, list-from-
# count-set so they can't drift, with the same $$/$PPID self-exclusion. Best-effort — an
# empty result yields "<unavailable>" in the page.
if [[ -z "${PROC_LIST_WORKER_CMD:-}" ]]; then
  # LIST-FROM-COUNT-SET still holds (roborev 1839/1821): the same pgrep, the same self-exclusion and the
  # same $LANE_PID_FILTER, so the set named in a page cannot drift from the set that triggered it. Only
  # the rendering differs — pids are mapped to command lines at the end rather than up front, because the
  # attribution filter needs bare pids.
  PROC_LIST_WORKER_CMD="pgrep -f '$PROC_MATCH_WORKER' 2>/dev/null | grep -vxF -e '$$' -e '$PPID' | $LANE_PID_FILTER | { ids=\$(tr '\\n' ' '); [ -n \"\$ids\" ] && ps -o pid=,args= -p \$(printf '%s' \"\$ids\" | tr ' ' ',' | sed 's/,\$//') 2>/dev/null; }"
fi
if [[ -z "${PROC_LIST_BUILD_CMD:-}" ]]; then
  PROC_LIST_BUILD_CMD="pgrep -lf '$PROC_MATCH_BUILD' 2>/dev/null | grep -vE '^($$|$PPID) '"
fi

# GH verification of a "finalized" marker's PR (issue #2670). $1 (passed by the
# later `bash -c ... _ "$prnum"`) is the PR number. Emits the raw `gh pr view` JSON
# on stdout. `autoMergeRequest` is queried so the verifier can recognize the
# closer's auto-armed path (OPEN with auto-merge enabled), a legitimate pending
# state rather than a false-finalize mismatch (roborev 1813). Overridable so the
# tooling tests can stub GitHub with a PATH-free command string.
if [[ -z "${GH_VERIFY_CMD:-}" ]]; then
  # shellcheck disable=SC2016  # literal $1 for the later `bash -c` eval, not expanded now.
  GH_VERIFY_CMD='gh pr view "$1" --repo pmcfadin/cqlite --json state,mergedAt,autoMergeRequest'
fi

# Notification command (issue #3119). The default is the REPO-OWNED notify
# contract, not the out-of-band `agent-notify` binary: upstream v1.1.0 has no
# `--category` arm, so the flag this function used to pass was swallowed — the
# title became the literal flag name, the message became the category value, and
# a `high` page published as ntfy priority 3 with a green check. The wrapper owns
# the payload and publishes to the ntfy server ROOT; `agent-notify` survives only
# as its optional, bounded, positional local desktop/sound adjunct.
# CONVENTION: the notify command takes THREE positional args — <severity> <title>
# <message>. The DEFAULT is built as an ARRAY (never a word-split string) so a
# REPO_ROOT containing a space cannot silently degrade every page to a WARN; an
# externally supplied NOTIFY_CMD string remains word-split, as the test seam and
# $CLAIM_CMD both are.
NOOP_NOTIFY_MARKER="__noop_notify__"
if [[ -z "${NOTIFY_CMD:-}" ]]; then
  if [[ -r "$REPO_ROOT/scripts/lib/gate-notify.sh" ]]; then
    NOTIFY_ARGV=(bash "$REPO_ROOT/scripts/lib/gate-notify.sh" --publish)
    NOTIFY_CMD="${NOTIFY_ARGV[*]}"   # display/marker value only, never re-split
  else
    NOTIFY_CMD="$NOOP_NOTIFY_MARKER"
    NOTIFY_ARGV=()
  fi
else
  # shellcheck disable=SC2206  # deliberate word-split of a caller-supplied string
  NOTIFY_ARGV=($NOTIFY_CMD)
fi

# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
WARNED_NOOP_NOTIFY=0
log() { printf '[worker-supervisor] %s\n' "$*" >&2; }

notify() {
  local priority="$1" title="$2" message="$3" category="completion"
  [[ "$priority" == "high" ]] && category="error"
  if [[ "$NOTIFY_CMD" == "$NOOP_NOTIFY_MARKER" ]]; then
    if [[ "$WARNED_NOOP_NOTIFY" -eq 0 ]]; then
      log "WARN: no notify command resolved (scripts/lib/gate-notify.sh unreadable); notifications are no-ops for this run"
      WARNED_NOOP_NOTIFY=1
    fi
    return 0
  fi
  # Positional args only — never a flag the notifier might swallow. NOTIFY_ARGV is
  # a properly quoted array, so a path with a space is safe.
  "${NOTIFY_ARGV[@]}" "$category" "$title" "$message" || log "WARN: notify command failed (non-fatal)"
}

is_gt() { awk -v a="$1" -v b="$2" 'BEGIN{ if ((a+0)>(b+0)) exit 0; exit 1 }'; }
is_lt() { awk -v a="$1" -v b="$2" 'BEGIN{ if ((a+0)<(b+0)) exit 0; exit 1 }'; }

# stamp_claim <issue>: refresh this lane's claim ref (refs/lane-claims/<machine>/<issue> since
# #3393) with issue+PID+ts (issue
# #2655). PID stamped is the SUPERVISOR's ($$) — the stable per-machine anchor,
# not a transient worker subprocess. A non-numeric/empty issue is stamped as "0"
# (unknown): the reaper then governs purely on age + open-PR (a "0" issue can
# have no open PR). Non-fatal on failure (a claim-push hiccup must never crash
# the supervisor) — logs a WARN and continues; the ref simply won't refresh this
# iteration and ages toward the reap threshold, which is fail-safe.
stamp_claim() {
  local issue="$1"
  [[ -n "$CLAIM_CMD" ]] || return 0
  # AN UNKNOWN ISSUE GETS A UNIQUE PER-SUPERVISOR LANE ID, NOT A SHARED PLACEHOLDER (roborev
  # round 1, High). The old behaviour normalised it to "0", which under per-lane refs means every
  # supervisor with a not-yet-known issue writes the SAME ref `refs/lane-claims/<machine>/0` — so
  # concurrent first iterations overwrite each other and a live sibling can mask an OOM-killed lane.
  # That is exactly the false clean ruling A removes, reintroduced through the placeholder.
  #
  # I first tried simply NOT stamping when the issue is unknown, and the suite caught why that is
  # wrong: `CLAIM_ISSUE` is CLEARED on `finalized`, so a supervisor finalising issue after issue
  # never knows its issue at spawn time and would stamp nothing at all — trading a collision for
  # zero liveness coverage. Measured as stamps=0 where 2 were expected.
  #
  # So the lane id is `p<pid>-<per-invocation token>`, prefixed `p` to keep it out of the numeric
  # issue space: unique per supervisor INVOCATION (the token defeats pid reuse across a crash or
  # reboot — roborev round 3), and unmistakable for an issue by any consumer that requires digits.
  # Automated reaping declines it outright, because an id that names no issue cannot be checked
  # against an open PR.
  local lane_id
  case "$issue" in
    '' | *[!0-9]* | 0) claim_lane_token; lane_id="p$$-${CLAIM_LANE_TOKEN}" ;;
    *)                 lane_id="$issue" ;;
  esac
  # A lane TRANSITION must not leak the previous ref (roborev round 1, Medium), and the ORDER
  # matters (roborev round 2, Medium). Deleting the old ref first leaves the lane with NO claim if
  # the replacement stamp then fails — invisible to dead-lanes and the reaper for the whole
  # iteration, which is a liveness GAP introduced by the leak fix. So: stamp the new ref, record it,
  # and only then best-effort delete the old one. Both refs existing briefly is harmless — the old
  # one names this same live supervisor pid, so it reads ALIVE — and is strictly better than a gap.
  local prev_lane_id="$CLAIM_STAMPED_ISSUE" prev_sha="$CLAIM_STAMPED_SHA" stamp_sha=""
  # `stamp` prints the sha it wrote on STDOUT (one line, a field). Captured rather than parsed out of
  # the log note: a sha read from a sentence is #3464 family 5 in the subsystem where a misread
  # message deletes someone's lease.
  if stamp_sha="$(HEARTBEAT_MACHINE="$CLAIM_MACHINE" $CLAIM_CMD stamp "$lane_id" "$$" 2>/dev/null)"; then
    stamp_sha="${stamp_sha%%$'\n'*}"
    case "$stamp_sha" in *[!0-9a-f]* | '') stamp_sha="" ;; esac
    # What was ACTUALLY stamped, which is not `CLAIM_ISSUE` (#3393). `CLAIM_ISSUE` is the NEXT
    # spawn's issue and is deliberately cleared on `finalized`, so by clean-exit time it is empty
    # and cannot name the ref this supervisor wrote. Recording it here is the only place that knows.
    CLAIM_STAMPED_ISSUE="$lane_id"
    CLAIM_STAMPED_SHA="$stamp_sha"
    # NO SHA MEANS NO AUTOMATED DELETE, NOT A DELETE WITHOUT A LEASE (roborev round 32, Medium). The
    # round-19 lease work logged a WARN here and then carried on, so an empty lease reached `reap` and
    # `delete_ref_guarded` performed an UNCONDITIONAL delete — the lease protection silently degrading to
    # none, in exactly the case it exists for. The reachable path is not exotic: `CLAIM_CMD` names a
    # script, so during ANY rollout where the invoked `claim-heartbeat.sh` predates the sha-on-stdout
    # contract this change introduces, EVERY stamp yields an empty lease and EVERY reap is unleased.
    #
    # Fail closed on the safe side: without a lease this supervisor does not delete that lane at all.
    # A retained ref is collectable — the CI reaper supplies its own lease from the listing — whereas a
    # successor's live claim deleted by an unleased push is not recoverable.
    [[ -n "$stamp_sha" ]] || log "WARN: stamp reported no sha for lane $lane_id — automated deletion of that lane is DISABLED for this supervisor (an unleased delete could remove a successor's live claim); the CI reaper will collect it with its own lease"
    # The replacement exists, so the old ref can go. Best-effort by design: a failure here leaves a
    # stale ref that dead-lanes will report and the reaper will collect, which is far better than the
    # gap that deleting first could open.
    # A FAILED CLEANUP IS RETAINED AND RETRIED, not forgotten (roborev round 5, Medium). Best-effort
    # was right about ordering — never delete before the replacement lands — but forgetting the ref
    # on failure is not recoverable for a PLACEHOLDER: automated reaping deliberately REFUSES a `p…`
    # lane (it names no issue, so an open PR cannot be ruled out), so a transient push failure would
    # leave a permanent stale claim that dead-lanes then reports as a dead lane forever. Carried in a
    # pending list and retried on every later stamp and on clean exit.
    # QUEUED WITH ITS LEASE, as `<lane-id>:<sha>` (roborev round 19). Without the sha, a retry that
    # lands after another supervisor has taken over this lane id deletes THAT supervisor's live claim.
    #
    # A NUMERIC PREDECESSOR WHOSE WORK HAS NOT CONCLUDED IS NOT QUEUED (roborev round 31, Medium). The
    # round-29 fix stopped the SHUTDOWN path from clearing such a ref, and left the TRANSITION path — the
    # same guard, a second route. A technical block on issue 88 followed by a `no-work` leaves the work
    # unconcluded but releases `CLAIM_ISSUE`, so the next iteration stamps a placeholder and this line
    # queued 88 for immediate reaping: the unresolved issue's liveness signal deleted, and its board item
    # potentially pinned with nothing left to reap. A `finalized` marker that fails its F5/verify checks
    # reaches the same transition, which is why the guard is here at the QUEUEING SITE and keyed on the
    # property — it covers every transition, including ones nobody has enumerated.
    #
    # `CLAIM_WORK_CONCLUDED` still holds the PREVIOUS iteration's verdict at this point (run_iteration
    # resets it to 0 immediately AFTER this stamp), which is exactly the value needed to judge whether the
    # PREVIOUS ref may be collected.
    #
    # A PLACEHOLDER predecessor is always queued, concluded or not: it names no issue, so deleting it
    # destroys no issue-linked signal, and NOT queueing it is the permanent leak round 5 fixed.
    if [[ -n "$prev_lane_id" && "$prev_lane_id" != "$lane_id" ]]; then
      if [[ "$prev_lane_id" == p* || "$CLAIM_WORK_CONCLUDED" == 1 ]]; then
        CLAIM_PENDING_CLEANUP="${CLAIM_PENDING_CLEANUP} ${prev_lane_id}:${prev_sha}"
      else
        log "claim cleanup of lane $prev_lane_id SKIPPED: its work has not concluded, so the issue-linked liveness signal stays (#2499)"
      fi
    fi
    claim_drain_pending_cleanup "$lane_id"
    log "claim stamped: machine=$CLAIM_MACHINE issue=$lane_id pid=$$"
  else
    log "WARN: claim stamp failed (machine=$CLAIM_MACHINE issue=$lane_id) — ref not refreshed this iteration (non-fatal)"
  fi
}

# clear_claim: delete this LANE's claim ref on a CLEAN supervisor exit (issue #2655; per-lane
# since #3393). `reap` refuses to delete a ref whose issue still has an open PR, so a supervisor
# that stops with an unfinished endgame leaves the claim in place for adoption rather than
# orphaning it. Non-fatal on failure.
#
# THE ISSUE MUST BE PASSED, and getting this wrong is silent (#3393). `reap <machine>` with no
# issue now targets the LEGACY per-machine ref, so omitting it would leave this lane's real ref
# behind while cheerfully reporting a clean clear — and a claim ref nothing deletes pins its board
# item at In Progress indefinitely. When the issue is still unknown, `CLAIM_ISSUE` is empty and
# nothing was stamped, so there is no ref to clear.
# clear_claim [clean] — release this lane's claim ref. `clean` is 1 only when the supervisor is
# stopping deliberately (finalize_exit code 0).
#
# A PLACEHOLDER IS NOT CLEARED ON AN ABNORMAL EXIT (roborev round 18, Medium). `finalize_exit` runs
# this on EVERY exit — `breaker`, `leftover-worker`, `leftover-build`, `automerge-stuck`,
# `verify-unavailable` included — while its own comment said "on a clean stop", and the whole
# safeguard rests on `reap` refusing when the issue has an open PR. For a NUMERIC lane id that guard
# runs and the clear is safe on any exit. For a `p<pid>-…` PLACEHOLDER there is no issue to consult,
# so `reap` deletes unconditionally (by design: the owner knows it finished, a reaper cannot) — and
# on an abnormal exit the owner does NOT know that. A worker can have claimed an issue and opened a
# PR before the supervisor ever received the marker, so clearing there destroys the only liveness
# signal of a lane with an unfinished endgame: the exact #2499 case, reached from the other side.
#
# THE EXIT CODE WAS THE WRONG DISCRIMINATOR, AND SO WAS THE LANE ID'S SHAPE (roborev round 23, Medium).
# The previous rule preserved only a PLACEHOLDER on an abnormal exit, reasoning that a numeric lane id
# is safe because `reap`'s open-PR guard runs. It is not: **an absence of an open PR is a correct answer
# to the wrong question.** Pre-PR work — an issue claimed, implementation under way, no PR opened yet —
# has no open PR, so the guard passes and the ref is deleted. A breaker or a `head-blocked` stop then
# erases the ONLY liveness signal for unfinished work: `dead-lanes` cannot report it (no ref), and the
# CI reaper cannot flip its board item back to Ready (nothing to reap), so the item is pinned at In
# Progress indefinitely. That is the same shape as round 21's `issue_has_open_pr` finding, in a
# different caller — which is why the discriminator is now the PROPERTY rather than any proxy for it.
#
# The property: has the work this supervisor claimed CONCLUDED? Only `finalized` and an owner park
# answer yes. A clean stop mid-issue (stop-file, budget) answers NO and deliberately leaves the ref —
# that is the #2499 intent, an unfinished lane stays owned for adoption, and the CI reaper collects it
# on age + no-open-PR + dead-pid once it is genuinely stale.
# conclusion_matches_stamped_lane <marker-issue> — true when a terminal marker may conclude THIS lane.
#
# A CONCLUSION IS ABOUT AN ISSUE, AND THE FLAG IS ABOUT A LANE (roborev round 33, High). Both accept
# points set `CLAIM_WORK_CONCLUDED=1` from the MARKER, without checking that the marker's issue is the
# one this supervisor actually stamped. `CLAIM_ISSUE` is carried forward across iterations, so a
# supervisor holding lane 88 whose worker finalises or parks issue 99 concluded 88 — and the shutdown
# then deleted 88's liveness ref while its work was unresolved.
#
# A PLACEHOLDER or unstamped lane has no issue-linked ref to protect, so any conclusion is fine there.
# A NUMERIC lane requires an exact match; a mismatch leaves the work unconcluded and the ref in place.
conclusion_matches_stamped_lane() {
  local marker_issue="${1:-}"
  case "${CLAIM_STAMPED_ISSUE:-}" in
    '' | p*) return 0 ;;
    *) [[ "$marker_issue" == "$CLAIM_STAMPED_ISSUE" ]] ;;
  esac
}

clear_claim() {
  [[ -n "$CLAIM_CMD" ]] || return 0
  local concluded="${1:-}"
  # Retry anything a transition could not clean up, before clearing this lane's own ref. Nothing is
  # protected here: on a clean exit this lane's own ref is being removed too.
  claim_drain_pending_cleanup
  local issue="$CLAIM_STAMPED_ISSUE"
  # May be a `p<pid>` placeholder as well as an issue number; both are real refs to clear.
  case "$issue" in '' | p) issue="" ;; esac
  # AN ENDGAME IN FLIGHT KEEPS THE REF, AND A PENDING AUTO-MERGE PR IS ONE (roborev round 28, Medium;
  # owner ruling (b) on #2499 semantics). `concluded` reflects only the LATEST iteration, so after a
  # pending-automerge finalize a subsequent `no-work`, merged finalize or owner park could set it to 1
  # and the shutdown would delete this lane's ref while an EARLIER auto-merge PR is still open. Ruling
  # (b) says exactly that state must keep its ref — so "concluded" is necessary and not sufficient:
  # there must also be nothing pending.
  if [[ -n "$issue" && -n "${PENDING_PR_LIST// /}" ]]; then
    # A PLACEHOLDER CANNOT CARRY THIS PROTECTION PAST OUR OWN EXIT (roborev round 36, Medium).
    #
    # Keeping the ref is right — ruling (b) on #2499 says an endgame in flight stays owned. But when
    # the stamped lane is a `p<pid>` PLACEHOLDER, keeping it is a TRAP of my own making: `should-reap`
    # permanently refuses placeholders (round 3, because an id naming no issue cannot be checked
    # against an open PR), so once this supervisor exits NOTHING can ever clear that ref — not the CI
    # reaper, not a later merge of the very PR it is protecting. The result is a stale ref and a
    # permanent false dead-lane report needing manual cleanup.
    #
    # So TRANSFER the protection to ISSUE-numbered refs, which the reaper CAN evaluate: an issue lane
    # has an open PR (KEEP) until the PR merges, and is then collectable on the next sweep. The
    # per-lane namespace makes this natural — an issue IS a lane id.
    #
    # ALL-OR-NOTHING, and the direction is deliberate: the placeholder is cleared ONLY if every
    # pending issue was stamped. A partial transfer would drop protection for the unstamped ones,
    # which is worse than the stale ref this fixes. A pending PR with no recorded issue is
    # untransferable, so it keeps the placeholder — a stale ref beats an unprotected endgame.
    if [[ "$issue" == p* ]]; then
      # FIELDS ARE CUT, NOT `read`-SPLIT, AND THAT IS NOT STYLE. TAB IS AN IFS *WHITESPACE*
      # CHARACTER, so `IFS=$'\t' read -r pr iss rest` COLLAPSES an empty field: an entry whose
      # issue is empty — exactly the untransferable case this branch exists to refuse — parsed as
      # pr=3467 iss=1 (the COUNT), so it would have stamped a BOGUS lane 1 and cleared the
      # placeholder. That is worse than the stale ref being fixed, and my own "untransferable" case
      # is what caught it. Parameter expansion does not collapse, so an empty field stays empty.
      local pr_e iss_e rest_e entry transferred=0 untransferable=0
      while IFS= read -r entry; do
        [[ -n "$entry" ]] || continue
        pr_e="${entry%%$'\t'*}"
        rest_e="${entry#*$'\t'}"
        iss_e="${rest_e%%$'\t'*}"
        # A line with no tab at all yields pr_e == entry and iss_e == entry; neither is a usable
        # pairing, so it must not be read as an issue.
        [[ "$entry" == *$'\t'* ]] || { untransferable=$((untransferable + 1)); continue; }
        [[ -n "$pr_e" ]] || continue
        case "$iss_e" in
          '' | *[!0-9]* | 0) untransferable=$((untransferable + 1)); continue ;;
        esac
        if HEARTBEAT_MACHINE="$CLAIM_MACHINE" $CLAIM_CMD stamp "$iss_e" "$$" >/dev/null 2>&1; then
          transferred=$((transferred + 1))
          log "endgame protection transferred off the placeholder: lane $iss_e stamped for pending PR $pr_e, so the reaper can collect it once that PR lands"
        else
          untransferable=$((untransferable + 1))
          log "WARN: could not stamp lane $iss_e for pending PR $pr_e; the placeholder is kept instead"
        fi
      done <<<"$PENDING_PR_LIST"
      if [[ "$untransferable" -eq 0 && "$transferred" -gt 0 ]]; then
        log "placeholder lane $issue is now clearable: all $transferred pending endgame(s) are protected by issue-numbered refs"
      else
        log "claim clear DECLINED: placeholder lane $issue kept — $untransferable pending endgame(s) could not be transferred to an issue-numbered ref (a stale ref beats an unprotected endgame)"
        return 0
      fi
    else
      log "claim clear DECLINED: lane $issue kept because an auto-merge PR is still pending — an endgame in flight stays owned for adoption (#2499)"
      return 0
    fi
  fi
  if [[ -n "$issue" && "$concluded" != 1 ]]; then
    log "claim clear DECLINED: the work on lane $issue has not concluded (no finalize, no owner park), so this ref is the only signal that the lane held it — left for dead-lanes to report and the reaper to collect (#2499)"
    return 0
  fi
  if [[ -z "$issue" ]]; then
    log "claim clear skipped: this supervisor never stamped a lane claim, so there is no ref to clear"
    return 0
  fi
  # UNDER THE LEASE THIS SUPERVISOR STAMPED (roborev round 19). A late exit — the breaker firing while
  # a successor has already taken this lane id — would otherwise delete the successor's LIVE claim on
  # its way out, making that lane unobservable. rc=4 means exactly that case and is reported as a
  # transfer, not a failure.
  if [[ -z "$CLAIM_STAMPED_SHA" ]]; then
    log "claim clear DECLINED for lane $issue: no lease was recorded, and an unleased delete could remove a successor's live claim — left for the CI reaper, which supplies its own lease (roborev round 32)"
    return 0
  fi
  local rc=0
  HEARTBEAT_MACHINE="$CLAIM_MACHINE" $CLAIM_CMD reap "$CLAIM_MACHINE" "$issue" "$CLAIM_STAMPED_SHA" >/dev/null 2>&1 || rc=$?
  if [[ "$rc" == 0 ]]; then
    log "claim cleared (work concluded): machine=$CLAIM_MACHINE issue=$issue${CLAIM_STAMPED_SHA:+ (lease held at $CLAIM_STAMPED_SHA)}"
  elif [[ "$rc" == 4 ]]; then
    log "claim clear declined for machine=$CLAIM_MACHINE issue=$issue: the lease at ${CLAIM_STAMPED_SHA:-<none>} is no longer held, so another supervisor owns this lane — its live claim is left alone"
  else
    log "WARN: claim clear declined/failed for machine=$CLAIM_MACHINE issue=$issue (rc=$rc: open PR, unreadable message or push error) — left for adoption (non-fatal)"
  fi
}

# detect_prompt_signature <logfile>: true (exit 0) when the LAST ~20 lines of the
# worker's live log show an interactive-prompt signature — a wedged
# AskUserQuestion, a permission prompt, a `❯` menu block, or a "waiting for input"
# line (issue #2666). Deliberately a TAIL scan, not a whole-log scan: the Claude
# CLI routinely prints tool names like `AskUserQuestion` in its normal trace, so
# a bare whole-file substring match would misclassify ordinary crashes as stuck
# and permanently defeat the breaker (roborev 1773). A stray match in old
# scrollback is not evidence of a live wedge; only a signature still resident in
# the last frames (paired with no-growth — see the supervise loop) is.
STUCK_TAIL_LINES="${STUCK_TAIL_LINES:-20}"
detect_prompt_signature() {
  local logfile="$1"
  [[ -f "$logfile" ]] || return 1
  tail -n "$STUCK_TAIL_LINES" "$logfile" 2>/dev/null | grep -qE "$PROMPT_SIGNATURE_RE"
}

# captured_question <logfile>: the matching prompt line(s) from the same tail
# window, collapsed to a single ≤300-char line for the ntfy body. Empty when
# nothing matched.
captured_question() {
  local logfile="$1"
  [[ -f "$logfile" ]] || return 0
  tail -n "$STUCK_TAIL_LINES" "$logfile" 2>/dev/null | grep -E "$PROMPT_SIGNATURE_RE" 2>/dev/null \
    | head -n 3 | tr '\n' ' ' | cut -c1-300
}

# log_size <logfile>: byte size of the file, `0` when absent, or `-1` when it COULD NOT BE MEASURED. A
# wedged interactive prompt produces NO further output, so a frozen byte size across two consecutive
# scans is the positive evidence that distinguishes a genuine wedge from a busy worker that merely
# printed a tool name and kept writing.
#
# A FAILED MEASUREMENT IS NOT A VALUE (#3601). This used to return the EMPTY STRING when `wc` failed,
# and empty collapses to `0` in the caller's `-eq` comparison — so two UNMEASURABLE reads compared
# EQUAL and a healthy worker's log read as frozen. `-1` is the same "no measurement" sentinel the
# caller's `prev_size` already starts at, so an unmeasurable read can never satisfy the caller's
# `-ge 0` guards and can never be mistaken for a real byte count.
log_size() {
  local f="$1" n
  [[ -f "$f" ]] || { printf '0'; return 0; }
  # `|| n=''` IS LOAD-BEARING UNDER `set -e`, NOT DEFENSIVE PADDING. `wc` failing makes the pipeline
  # non-zero (`pipefail`), and a non-zero command substitution in an ASSIGNMENT aborts an errexit shell
  # before the classification below ever runs — so without this the probe would not return a sentinel,
  # it would kill its caller. Measured: the bare call `log_size "$f"` from a `set -e` shell produced NO
  # output at all. The pre-#3601 form was safe only by accident, because its one caller sits inside a
  # `set +e` region; a probe's correctness must not depend on that.
  n="$( { wc -c <"$f"; } 2>/dev/null | tr -d ' ')" || n=''
  # The digits are enumerated rather than given as a `[0-9]` RANGE, whose members are decided by the
  # caller's collation.
  case "$n" in
    '' | *[!0123456789]*)
      printf '%s' '-1'
      return 0
      ;;
  esac
  printf '%s' "$n"
}

marker_field() {
  local field="$1"
  [[ -f "$MARKER_FILE" ]] || return 0
  if command -v jq >/dev/null 2>&1; then
    jq -r --arg f "$field" '.[$f] // empty' "$MARKER_FILE" 2>/dev/null
  else
    python3 -c '
import json, sys
try:
    d = json.load(open(sys.argv[1]))
except Exception:
    sys.exit(0)
v = d.get(sys.argv[2])
print(v if v is not None else "")
' "$MARKER_FILE" "$field" 2>/dev/null || true
  fi
}

# verify_finalized_pr <pr>: check the claimed PR is actually merged (issue #2670).
# Echoes exactly one verdict token on stdout (never returns nonzero — the caller's
# `case` handles every path explicitly):
#   merged              — gh reported state MERGED. The ONLY path that credits an issue.
#   pending-automerge   — OPEN with `autoMergeRequest` armed: the closer's documented
#                         auto-merge path, so the PR WILL land. Legitimate pending
#                         state — uncounted (for now), default-priority page,
#                         breaker-NEUTRAL — not an escalation (roborev 1813).
#   mismatch:<STATE>    — a STABLE non-MERGED state without auto-merge armed, after
#                         MISMATCH_RETRIES grace reads (absorbing read-after-merge lag).
#   mismatch:UNRESOLVED — the marker's `pr` is FORGED/GARBAGE: a non-numeric ref, a
#                         URL that is not EXACTLY a pmcfadin/cqlite PR URL (finding 4),
#                         gh could not resolve it (resolve-failure stderr signature),
#                         or a cleanly-PARSED response that simply lacks a PR state. An
#                         ESCALATION (abnormal, high page, breaker-counting).
#   unverified          — a TOOLING/TRANSPORT gap, never forgery (finding 2): gh binary
#                         missing, a network/auth/rate-limit error, OR no JSON parser
#                         present / the payload failed to parse. Fail-informative;
#                         neutral to the breaker and uncounted.
verify_finalized_pr() {
  local pr="$1" prnum out err rc parser_ok state automerge errfile pyout attempt=0
  # Finding 4 (URL shape): a URL-form pr must be EXACTLY a pmcfadin/cqlite PR URL;
  # any other host/repo is a forged/foreign reference. A bare number (optional
  # leading '#') is accepted; anything else is forged.
  if [[ "$pr" =~ ^[a-zA-Z][a-zA-Z0-9+.-]*:// ]]; then
    if [[ "$pr" =~ ^https://github\.com/pmcfadin/cqlite/pull/([0-9]+)$ ]]; then
      prnum="${BASH_REMATCH[1]}"
    else
      printf 'mismatch:UNRESOLVED'
      return 0
    fi
  else
    prnum="${pr#\#}"
    if [[ ! "$prnum" =~ ^[0-9]+$ ]]; then
      printf 'mismatch:UNRESOLVED'
      return 0
    fi
  fi

  # Total mismatch-grace wall-clock ceiling. The natural budget is retries*wait;
  # MISMATCH_GRACE_CAP_SECS>0 tightens it to at most the cap, while a cap <=0
  # DISABLES the ceiling (grace then bounded solely by the retry count — roborev
  # 1821 explicit semantics). The guard below only binds when grace_budget>0, so a
  # disabled cap / a wait=0 config leaves retries count-bounded, never blocked.
  local grace_start grace_budget
  grace_start=$(date +%s)
  grace_budget=$((MISMATCH_RETRIES * MISMATCH_RETRY_WAIT_SECS))
  if [[ "$MISMATCH_GRACE_CAP_SECS" -gt 0 && "$grace_budget" -gt "$MISMATCH_GRACE_CAP_SECS" ]]; then
    grace_budget="$MISMATCH_GRACE_CAP_SECS"
  fi

  while :; do
    errfile="$(mktemp "${TMPDIR:-/tmp}/gh-verify.XXXXXX")"
    # Context-independent capture (roborev 1810): capture rc explicitly via the
    # `&& rc=0 || rc=$?` list, not the errexit-in-assignment quirk.
    out="$(bash -c "$GH_VERIFY_CMD" _ "$prnum" 2>"$errfile")" && rc=0 || rc=$?
    err="$(cat "$errfile" 2>/dev/null || true)"
    rm -f "$errfile"

    # gh binary missing (exec failure) is transport class, never a forged PR.
    if [[ "$rc" -eq 127 ]]; then
      printf 'unverified'
      return 0
    fi
    # gh RAN and could not RESOLVE the ref — a forged reference. Finding 2 (roborev
    # 1819): match gh's ACTUAL resolve-failure signature ONLY, anchored and
    # case-tolerant on the leading letter — `Could not resolve to a PullRequest|Issue`
    # and the `no pull requests found` form. Everything else nonzero is a transport
    # error (`dial tcp ... no such host`, DNS/proxy 404) → unverified, NEVER forgery.
    # The bare `not found` substring is deliberately GONE.
    if printf '%s' "$err" | grep -qE '[Cc]ould not resolve to a (PullRequest|Issue)|[Nn]o pull requests found'; then
      printf 'mismatch:UNRESOLVED'
      return 0
    fi

    # Parse. parser_ok = a parser is present AND the payload parsed as JSON.
    # Finding 2: a tooling gap (no parser / parse error) is transport class
    # (unverified), NEVER forgery. mismatch:UNRESOLVED is reserved for a
    # SUCCESSFULLY-parsed response that simply lacks the PR state. Finding 3 (roborev
    # 1819): jq is tried first, but a jq PARSE FAILURE falls THROUGH to python3 before
    # giving up (jq present but broken must not defeat a valid response).
    parser_ok=0
    state=""
    automerge=""
    if command -v jq >/dev/null 2>&1 && printf '%s' "$out" | jq -e . >/dev/null 2>&1; then
      parser_ok=1
      state="$(printf '%s' "$out" | jq -r '.state // empty' 2>/dev/null || true)"
      # Normalize the auto-merge sentinel (roborev 1821): both parsers emit the
      # SAME token — "armed" when autoMergeRequest is present, "" otherwise — so the
      # downstream `-n "$automerge"` test can never diverge between jq and python3
      # (jq's raw object dump vs python's boolean string).
      automerge="$(printf '%s' "$out" | jq -r 'if .autoMergeRequest then "armed" else "" end' 2>/dev/null || true)"
    fi
    if [[ "$parser_ok" -eq 0 ]] && command -v python3 >/dev/null 2>&1; then
      if pyout="$(printf '%s' "$out" | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(2)
print(d.get("state") or "")
print("armed" if d.get("autoMergeRequest") else "")
' 2>/dev/null)"; then
        parser_ok=1
        state="$(printf '%s\n' "$pyout" | sed -n 1p)"
        automerge="$(printf '%s\n' "$pyout" | sed -n 2p)"
      fi
    fi

    if [[ "$parser_ok" -eq 0 ]]; then
      # No parser, or the payload failed to parse — our tooling/transport gap, not
      # the worker's forgery.
      printf 'unverified'
      return 0
    fi
    if [[ -z "$state" ]]; then
      # Parsed cleanly but no PR state — a genuine unresolved/garbage response.
      printf 'mismatch:UNRESOLVED'
      return 0
    fi
    if [[ "$state" == "MERGED" ]]; then
      printf 'merged'
      return 0
    fi
    # Finding 3: OPEN with auto-merge armed is a LEGITIMATE closer path (the PR will
    # land automatically) — a distinct, non-escalating verdict, not a mismatch.
    if [[ "$state" == "OPEN" && -n "$automerge" ]]; then
      printf 'pending-automerge'
      return 0
    fi
    # A resolved, non-merged state without auto-merge. Grace: read-after-merge lag
    # can briefly show OPEN just after a merge — retry a few times before escalating.
    # The grace loop is BOUNDED (roborev 1819): the retry COUNT (MISMATCH_RETRIES) is
    # the primary bound; it ALSO stops if the stop-file appears (a requested shutdown
    # must not wait out the grace) or the total grace wall-clock budget is spent. The
    # wall-clock guard only binds when there is an actual per-retry wait — with
    # wait=0 the budget is 0, so instant retries stay count-bounded (not blocked).
    attempt=$((attempt + 1))
    if [[ "$attempt" -lt "$MISMATCH_RETRIES" ]]; then
      # Retries remain, but the grace loop is being CUT SHORT rather than by observing
      # a stable non-merged state MISMATCH_RETRIES times. The PR state was never allowed
      # to settle, so reporting it as a forged finalize (abnormal + HIGH page + breaker)
      # would falsely accuse a worker whose endgame was merely interrupted (roborev 1837).
      #  - stop-file present  → `aborted`: a NEUTRAL non-verdict (the supervisor is
      #    shutting down; it is neither a forged mismatch NOR a verification outage, so it
      #    must not accumulate the unverified-outage streak and trip verify-unavailable
      #    during an ordinary shutdown).
      #  - wall-clock budget spent (no shutdown) → `unverified` (transport-class): we
      #    genuinely could not confirm the state in the allotted grace.
      if [[ -f "$STOP_FILE" ]]; then
        printf 'aborted'
        return 0
      fi
      if [[ "$grace_budget" -gt 0 ]] && [[ $(( $(date +%s) - grace_start )) -ge "$grace_budget" ]]; then
        printf 'unverified'
        return 0
      fi
      sleep "$MISMATCH_RETRY_WAIT_SECS"
      continue
    fi
    # Retries exhausted. Re-check the stop-file here too: with MISMATCH_RETRIES=1 the
    # loop never reaches the mid-loop guard above, and even with more retries a shutdown
    # can arrive during this FINAL read — either way a requested shutdown must never be
    # reported as a forged finalize (roborev 1838), so emit the neutral `aborted`.
    if [[ -f "$STOP_FILE" ]]; then
      printf 'aborted'
      return 0
    fi
    # A stable, resolved, non-merged state that outlasted the grace — a genuine mismatch.
    printf 'mismatch:%s' "$state"
    return 0
  done
}

# json_or_null <value>: quotes+escapes a string field for embedding into the
# JSONL journal, or emits a bare `null` when empty. A quote/backslash/newline
# in an untrusted field (pr URL, blocked "reason" text) must never be allowed
# to corrupt journal JSON — printf '"%s"' alone does not escape those chars.
# Preferred path: python3 json.dumps (already a project dependency, same as
# marker_field's jq-absent fallback). Degraded fallback (python3 somehow
# absent): strip to a conservative safe charset so the line still parses,
# rather than emitting broken JSON.
json_or_null() {
  local v="$1"
  [[ -n "$v" ]] || { printf 'null'; return 0; }
  if command -v python3 >/dev/null 2>&1; then
    python3 -c 'import json, sys; print(json.dumps(sys.argv[1]))' "$v" 2>/dev/null && return 0
  fi
  printf '"%s"' "$(printf '%s' "$v" | tr -cd 'A-Za-z0-9:/._#?=&-')"
}
num_or_null() { [[ "$1" =~ ^[0-9]+$ ]] && printf '%s' "$1" || printf 'null'; }

# journal_line <iter> <outcome> <issue> <pr> <duration_s> <exit_code> [reason] [verified]
# `reason` (only "blocked"/park iterations) and `verified` (only the finalized
# family, issue #2670) are optional; both `pr` and `reason` are free-form
# worker-controlled text and go through json_or_null so a stray quote/backslash/
# newline can never corrupt the JSONL line. `verified` is appended only when set,
# so every pre-existing 6/7-arg caller emits an unchanged line.
journal_line() {
  mkdir -p "$LOG_DIR"
  local jf="${JOURNAL_FILE:-$LOG_DIR/journal-$(date -u +%Y-%m-%d).jsonl}"
  local verified_json=""
  [[ -n "${8:-}" ]] && verified_json=",\"verified\":$(json_or_null "$8")"
  printf '{"ts":"%s","iter":%d,"outcome":"%s","issue":%s,"pr":%s,"duration_s":%d,"exit_code":%d,"reason":%s%s}\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$1" "$2" "$(num_or_null "$3")" "$(json_or_null "$4")" "$5" "$6" "$(json_or_null "${7:-}")" "$verified_json" >>"$jf"
}

# ---------------------------------------------------------------------------
# Single-instance lock. macOS ships no flock(1); an atomic mkdir + pid-liveness
# check gives the same "only one supervisor per LANE" guarantee portably.
#
# PER LANE, NOT PER MACHINE (roborev round 19, Medium). The default was machine-global, so a second
# lane started with the documented default invocation exited during lock acquisition — making the
# per-lane claim refs this change adds unreachable in production with defaults, while N lanes per box
# is the standing model (#3393 retracted #1930). That is the retracted invariant surviving in a
# SECOND mechanism: fixing the ref namespace left the lock still enforcing the assumption the
# namespace change exists to remove. Scoped by the lane's own checkout root, which is what "one
# instance" should always have meant — it still refuses two supervisors in the SAME worktree, the
# failure it was built for.
# ---------------------------------------------------------------------------
# supervisor_lock_path: set SUPERVISOR_LOCK if the caller did not. Builtins only — no `tr`, `cksum`,
# `awk` or `sed` — because this must also work under the stripped PATH the parser tests source with.
# The basename stays readable so an operator can tell which lane owns which lock; the arithmetic hash
# of the FULL path disambiguates two lanes whose directories share a basename.
# supervisor_lane_id — echo a stable, lane-unique token derived from this lane's checkout root, using
# BUILTINS ONLY (no `tr`/`cksum`/`awk`: several suite cases source this file under a stripped PATH, and a
# source-time external tool broke them twice — #3464 family 2).
# THE HASH COMES FIRST AND THE SLUG IS BOUNDED (roborev round 34, Medium). The first cut appended the
# hash AFTER an unbounded directory basename, and `claim.sh`'s `sanitize_field` caps a field at 120
# characters — so a lane directory with a long basename truncated the HASH AWAY and two distinct lanes
# collapsed onto one actor. That is the very aliasing this identifier exists to prevent, reintroduced by
# the shape of the identifier itself. Hash first makes truncation cost only READABILITY; bounding the
# slug means the whole token is ~40 chars, so it also cannot approach a filesystem NAME_MAX in the lock
# filename. Ordering is doing real work here, not cosmetics.
supervisor_lane_id() {
  local slug="${REPO_ROOT##*/}" full="$REPO_ROOT" i h=0 c
  slug="${slug//[^A-Za-z0-9._-]/_}"
  [[ -n "$slug" ]] || slug=lane
  for ((i = 0; i < ${#full}; i++)); do
    printf -v c '%d' "'${full:i:1}"
    h=$(((h * 31 + c) & 0x7fffffff))
  done
  printf '%s-%s\n' "$h" "${slug:0:24}"
}

# LANE IDENTITY IS GIVEN, NOT DEDUCED (roborev round 36; lead ruling B + C, 2026-08-30).
#
# Every per-lane mechanism here — the single-instance lock, the claim actor, the legacy-claim
# migration, the worker-orphan attribution — needs to know WHICH LANE THIS IS. It used to answer that
# from `REPO_ROOT`, which is `$(dirname "${BASH_SOURCE[0]}")/../..`: **"where is my script" standing in
# for "which lane am I".** On this fleet that happens to land on the lane, because every lane worktree
# carries a full `scripts/` tree — so the mechanisms worked BY COINCIDENCE.
#
# THE RULING REJECTED RATIFYING THAT COINCIDENCE, and the reason is the defect class this whole change
# has been removing: a cheap observable substituted for the property, invisible to the code that
# depends on it. Launched from the root checkout instead, all four mechanisms silently degrade — the
# "per-lane" lock becomes machine-global, the actor is shared, the migration finds no issue branch, and
# the orphan probe attributes ZERO, which is a probe that returns the good answer unconditionally. That
# last one was MY round-35 fail-open: I fixed a false-STOP and replaced it with a never-STOP.
#
# So: `LANE_ID` is the identity, and it is a value the program is GIVEN.
#
# `LANE_ID` unset is still supported, because nothing on the fleet sets it yet — but the fallback must
# PROVE it landed in a lane, and REFUSE LOUDLY otherwise. That refusal is what converts "an old
# launcher silently gets today's behaviour" into a startup error naming the remedy, which was the whole
# objection to B. A fallback that silently degrades is the same defect one level down.
#
# The proof is STRUCTURAL — git answers it, and no lane-directory naming convention is assumed
# anywhere. A linked worktree has `--git-dir` != `--git-common-dir`; the main worktree has them equal.
# Assuming a layout is exactly what made this issue's AC3 unimplementable, so the check for it must not
# reintroduce that assumption.
LANE_ID="${LANE_ID:-}"

# lane_worktree_ok — rc 0 iff REPO_ROOT is a LINKED worktree (i.e. a lane, not the root checkout).
# Echoes nothing; callers phrase their own refusal.
lane_worktree_ok() {
  local gd cd_
  gd="$(git -C "$REPO_ROOT" rev-parse --absolute-git-dir 2>/dev/null)" || return 1
  cd_="$(git -C "$REPO_ROOT" rev-parse --git-common-dir 2>/dev/null)" || return 1
  case "$cd_" in
    /*) : ;;
    *) cd_="$(cd "$REPO_ROOT/$cd_" 2>/dev/null && pwd)" || return 1 ;;
  esac
  [[ "$gd" != "$cd_" ]]
}

# supervisor_resolve_lane_id — establish the lane identity, or refuse to start.
#
# TWO NAMED REFUSALS, each with its remedy, because they are different failures:
#   * lane-identity-unprovable — `LANE_ID` unset AND `REPO_ROOT` is not a lane worktree, so there is
#     nothing to derive an identity FROM.
#   * lane-attribution-impossible — the worker-orphan probe attributes by working directory under
#     `REPO_ROOT`; if that is the main worktree, the lane's workers live in OTHER worktrees and the
#     probe can only ever attribute zero. Setting `LANE_ID` does NOT fix this, because an identity
#     token is not a directory — which is why this check is independent of the first.
#     An operator who has overridden `PROC_PROBE_WORKER_CMD` has taken that responsibility explicitly,
#     so the refusal yields to them.
supervisor_resolve_lane_id() {
  if [[ -z "$LANE_ID" ]]; then
    if lane_worktree_ok; then
      LANE_ID="$(supervisor_lane_id)"
      log "LANE_ID unset; derived '$LANE_ID' from this lane's worktree ($REPO_ROOT). Set LANE_ID explicitly to make the lane identity a given rather than an inference (#3393)."
    else
      log "FATAL: lane-identity-unprovable — LANE_ID is unset and REPO_ROOT ($REPO_ROOT) is NOT a lane worktree, so no per-lane identity can be derived. Every per-lane mechanism (single-instance lock, claim actor, legacy-claim migration, worker-orphan attribution) would silently share one identity across all lanes on this machine. Remedy: export LANE_ID=<stable per-lane token>, or launch inside the lane's own worktree."
      notify "high" "worker-supervisor: lane identity unprovable" "LANE_ID unset and REPO_ROOT is not a lane worktree; refusing to start (#3393). Set LANE_ID."
      exit 2
    fi
  else
    LANE_ID="$(printf '%s' "$LANE_ID" | tr -c 'A-Za-z0-9._-' '_')"
    LANE_ID="${LANE_ID:0:40}"
    if [[ "${#LANE_ID}" -lt 3 ]]; then
      log "FATAL: lane-identity-unusable — LANE_ID sanitises to '$LANE_ID', fewer than 3 recordable characters. claim.sh refuses such an actor, so the claim lock would fail closed on every call. Remedy: give LANE_ID at least 3 characters from [A-Za-z0-9._-]."
      notify "high" "worker-supervisor: bad LANE_ID" "LANE_ID='$LANE_ID' is too short after sanitisation; refusing to start (#3393)."
      exit 2
    fi
    log "lane identity given explicitly: LANE_ID='$LANE_ID'"
  fi
  export LANE_ID
  # INDEPENDENT of the above: an identity token is not a directory.
  if ! lane_worktree_ok && [[ -z "${PROC_PROBE_WORKER_CMD_OVERRIDDEN:-}" ]]; then
    log "FATAL: lane-attribution-impossible — REPO_ROOT ($REPO_ROOT) is the MAIN worktree, so the worker-orphan probe attributes by a working directory that this lane's workers never occupy: it can only ever count ZERO, reporting 'no leftover debris' unconditionally. LANE_ID does not fix this — an identity token is not a directory. Remedy: launch inside the lane's own worktree, or set PROC_PROBE_WORKER_CMD to a probe appropriate to this layout."
    notify "high" "worker-supervisor: lane attribution impossible" "REPO_ROOT is the main worktree; the worker-orphan probe would always count zero. Refusing to start (#3393)."
    exit 2
  fi
}

# supervisor_claim_actor: EXPORT a lane-unique `CLAIM_ACTOR` unless the operator set one.
#
# THE CLAIM LOCK'S HOLDER IDENTITY IS machine+actor, AND THE DEFAULT ACTOR IS SHARED (roborev round 33,
# High). `claim.sh` documents the hazard for the machine half — "two machines share one identity and each
# treats the other's claim as its own (false re-entrancy / cross-release)" — and the actor exists precisely
# so distinct roles on ONE box do not alias. Nothing set it: every lane defaulted to `flow`.
#
# That was harmless while a machine-global lock made a second lane impossible to start. THIS CHANGE made
# the lock per-LANE, so two default lanes can now run — and with one holder identity between them,
# `verify` can report that lane A holds lane B's claim and `release` can delete it. **Removing a coarse
# guard exposed a finer defect the guard had been masking**, which is this change's own doing and so its
# own responsibility.
#
# EXPORTED, not just set: `claim.sh` is invoked by the WORKER this supervisor spawns, which inherits the
# environment. Setting it locally would leave the worker on the shared default.
#
# `claim.sh` sanitises the actor to one token and REFUSES fewer than 3 recordable characters, so the value
# is prefixed `flow-` and carries the lane id — recognisable in a claim message and never degenerate.
supervisor_claim_actor() {
  [[ -n "${CLAIM_ACTOR:-}" ]] && return 0
  # FROM THE GIVEN IDENTITY, not from a second inference of it (lead ruling B). Two derivations of
  # "which lane am I" is two things to keep in step, and the one that drifts is found in production.
  CLAIM_ACTOR="flow-${LANE_ID}"
  export CLAIM_ACTOR
  log "claim actor defaulted to lane-unique '$CLAIM_ACTOR' (the claim lock's holder identity is machine+actor; a shared default would alias two lanes on one box)"
}

# ONE construction, reused (roborev round 34, Medium). This duplicated `supervisor_lane_id`'s body
# verbatim, so the bound added there would have silently not applied here — two spellings of one identity
# is the drift shape #3464 records. A shell function call is not an external tool, so the builtins-only
# constraint still holds.
# supervisor_migrate_legacy_claim — CAS-adopt THIS LANE'S OWN pre-upgrade claim to the lane actor.
#
# WHY THIS EXISTS (roborev round 34, finding 1). The claim lock's holder identity is machine+actor. Every
# claim stamped before the lane-actor change carries `actor=flow`, so once a lane resolves a lane-unique
# actor its OWN claim reads as FOREIGN: measured against a live claim, `claim.sh verify` answers
# VERIFY-FAIL with `holder-machine` and `wanted-machine` IDENTICAL and only the actor differing. Left
# alone, every lane in flight at the moment that change lands is stranded — it can neither verify nor
# non-forcibly release its own lock. That is this change's own doing, so it is this change's to migrate.
#
# WHAT IDENTIFIES "MY" CLAIM, and why it is not the ref. On a four-lane box all four legacy claims read
# `machine+flow` — IDENTICAL — so the ref carries nothing that distinguishes them. Only the LANE'S OWN
# BRANCH does. So the issue is taken from this worktree's branch and nothing else: a lane can never
# reach a sibling's claim, which would be the aliasing bug with extra steps.
#
# AFFIRMATIVE MEASUREMENT, not absence of a bad signal (#3229's rule). The adopt runs ONLY when the
# status read AFFIRMATIVELY shows this machine AND exactly the legacy actor. An unreadable status, a
# missing ref, a different machine, or any other actor => DO NOTHING. The cost of doing nothing is a
# DIAGNOSED refusal on the worker's next `verify` (the documented `adopt` procedure then applies), which
# is strictly better than a guess that could take a claim that is not ours.
#
# CAS, never a bare write: `--expect <sha>` means a live peer that moved the ref wins (ADOPT-LOST), and
# the adopt commit records who took it and why.
# NEVER PERMANENTLY FOREIGN FOR THE RUN (roborev round 35, Medium). The first cut read the claim once
# and returned silently on failure — so a transient outage at startup left the lane running under an
# actor its own claim does not recognise for the WHOLE run: precisely the stranding this migration
# exists to prevent, merely rarer and harder to see.
#
# Retried on two timescales: a bounded burst here for a blip, and again at the top of EVERY iteration
# until it settles, so a longer outage self-heals the moment the remote comes back.
#
# DELIBERATE DEVIATION from the review's suggested fix, stated rather than glossed. The reviewer asked
# for a preflight FAILURE until migration succeeds. That trades a transient failure for a permanent
# one — a supervisor that refuses to start whenever the remote is unreachable. Re-attempting every
# iteration removes "permanently foreign" at no availability cost, and a lane cannot do useful work
# without the remote anyway. The WARN names the state on every attempt, so a persistent outage is loud.
CLAIM_MIGRATION_SETTLED=0
CLAIM_MIGRATION_RETRIES="${CLAIM_MIGRATION_RETRIES:-3}"
supervisor_migrate_legacy_claim() {
  # SETTLED = migrated, or affirmatively nothing to migrate. A cheap no-op on every later call.
  [[ "$CLAIM_MIGRATION_SETTLED" == 1 ]] && return 0
  [[ -n "$LOCK_CMD" ]] || { CLAIM_MIGRATION_SETTLED=1; return 0; }
  # An operator-pinned actor is not ours to migrate away from — SETTLED, not merely skipped.
  [[ "${CLAIM_ACTOR:-}" == "$LEGACY_CLAIM_ACTOR" || -z "${CLAIM_ACTOR:-}" ]] && { CLAIM_MIGRATION_SETTLED=1; return 0; }
  local branch n st sha holder_machine holder_actor attempt
  # `symbolic-ref --short`, not `rev-parse --abbrev-ref`: it asks the question actually being asked —
  # "which BRANCH is this worktree on" — and answers it without resolving a commit, so it also works on
  # an unborn HEAD. `rev-parse --abbrev-ref HEAD` fails outright there ("ambiguous argument 'HEAD'"),
  # which is how the positive control for this function first came back with no call at all. A DETACHED
  # worktree makes `symbolic-ref` exit non-zero, which is the right answer: it names no issue.
  branch="$(git -C "$REPO_ROOT" symbolic-ref --short HEAD 2>/dev/null)" || { CLAIM_MIGRATION_SETTLED=1; return 0; }
  case "$branch" in
    issue-[0-9]*) n="${branch#issue-}"; n="${n%%-*}" ;;
    *) CLAIM_MIGRATION_SETTLED=1; return 0 ;;
  esac
  case "$n" in
    '' | *[!0-9]* | 0*) CLAIM_MIGRATION_SETTLED=1; return 0 ;;
  esac
  # A BOUNDED BURST for a blip. Deliberately NOT settled when every attempt fails: the loop re-enters.
  st=""
  for ((attempt = 1; attempt <= CLAIM_MIGRATION_RETRIES; attempt++)); do
    if st="$($LOCK_CMD status "$n" 2>/dev/null)"; then break; fi
    st=""
    [[ "$attempt" -lt "$CLAIM_MIGRATION_RETRIES" ]] && sleep 2
  done
  if [[ -z "$st" ]]; then
    log "WARN: could not read issue $n's claim after $CLAIM_MIGRATION_RETRIES attempts; this lane runs as '$CLAIM_ACTOR' while a pre-upgrade claim is held by '$LEGACY_CLAIM_ACTOR', so its own lock may read as FOREIGN until this settles. Retrying next iteration."
    return 0
  fi
  sha="$(supervisor_msg_token "$st" sha)"
  holder_machine="$(supervisor_msg_token "$st" machine)"
  holder_actor="$(supervisor_msg_token "$st" actor)"
  # EVERY field must be affirmatively right. A missing token is not a match.
  # 40 OR 64 lowercase hex (roborev round 35, Low): a SHA-256 repository's object ids are 64 chars, and
  # `claim.sh` imposes NO length check of its own — it hands `--expect` straight to git — so a hard 40
  # here was STRICTER THAN THE THING IT FEEDS and would silently skip every valid claim on such a repo.
  # Today's reachability is zero (this repo is SHA-1), so this is consistency rather than a live bug;
  # the asymmetry is what deserved removing, because a needlessly narrow guard reads as intentional.
  [[ -n "$sha" && "$sha" != *[!0-9a-f]* ]] || return 0
  case "${#sha}" in
    40 | 64) : ;;
    *) return 0 ;;
  esac
  [[ "$holder_machine" == "$CLAIM_MACHINE" ]] || return 0
  [[ "$holder_actor" == "$LEGACY_CLAIM_ACTOR" ]] || return 0
  log "migrating issue $n's pre-upgrade claim (actor=$LEGACY_CLAIM_ACTOR) to this lane's actor '$CLAIM_ACTOR' via compare-and-swap on $sha"
  if $LOCK_CMD adopt "$n" --expect "$sha" \
    --reason "upgrade-lane-actor:pre-upgrade-claim-held-by-${LEGACY_CLAIM_ACTOR}-on-this-machine-adopted-by-lane-actor" \
    >/dev/null 2>&1; then
    log "claim for issue $n adopted under '$CLAIM_ACTOR'"
    CLAIM_MIGRATION_SETTLED=1
  else
    # NOT fatal, and deliberately not retried: a peer may have moved the ref (ADOPT-LOST is the CAS
    # working), or the remote may be unreachable. Either way the claim is untouched and the worker's
    # next verify gives the documented refusal.
    log "WARN: could not adopt issue $n's pre-upgrade claim (a peer moved the ref, or the remote is unreachable); the claim is untouched and the next verify will diagnose it"
  fi
}

# supervisor_msg_token <msg> <field> — the WHOLE token for an EXACT key, never a substring (#3464
# family 6: `notissue=42` must not answer `issue`). Builtins only.
supervisor_msg_token() {
  local msg="$1" field="$2" tok
  for tok in $msg; do
    case "$tok" in
      "$field"=?*) printf '%s\n' "${tok#*=}"; return 0 ;;
    esac
  done
  return 0
}

supervisor_lock_path() {
  # If the caller named a path, that is where OUR lock goes; otherwise derive the per-lane default.
  # NOTHING ELSE IS RECORDED HERE, AND NOTHING DOWNSTREAM ASKS WHICH BRANCH RAN (#3549, lead ruling —
  # AC4 removed as unsound; see the note beside `SUPERVISOR_LOCK`'s initialisation). The legacy-lock
  # guard runs either way, so the provenance this function used to keep has no consumer left: it
  # existed ONLY to switch that check off, and switching it off was the defect.
  if [[ -n "$SUPERVISOR_LOCK" ]]; then
    return 0
  fi
  # FROM THE GIVEN IDENTITY (lead ruling B). `LANE_ID` is resolved before this is called.
  SUPERVISOR_LOCK="${TMPDIR:-/tmp}/cqlite-worker-supervisor-${LANE_ID}.lock"
}

# ---------------------------------------------------------------------------
# LEGACY GLOBAL LOCK COMPATIBILITY (#3549; the defect #3467 introduced).
#
# Before #3467 the single-instance lock was ONE MACHINE-GLOBAL path,
# `${TMPDIR:-/tmp}/cqlite-worker-supervisor.lock`. After #3467 the derived default is per LANE, which
# is the correct end state (#3393: N lanes per box). But the two paths are invisible to each other, so
# a supervisor launched from a PRE-#3467 checkout holds a lock this one never looks at — and both then
# run, in the same worktree, sharing markers, branch, logs and `.worker-last-iteration.json`. The
# window opens during any rolling update of the fleet's checkouts.
#
# REMOVAL CONDITION (AC6). Delete `supervisor_legacy_lock_presence`, `supervisor_legacy_lock_refuse`,
# `supervisor_legacy_lock_guard`, their call site in `acquire_lock`, and their suite cases once EVERY
# checkout that can launch a supervisor on this fleet is at or past #3467 — i.e. once no pre-#3467
# `worker-supervisor.sh` can run again. How to check it: on each box,
#   git -C <checkout> merge-base --is-ancestor f33f726c4 HEAD   # f33f726c4 = the #3467 commit
# for every checkout that a launcher can reach, and confirm no legacy lock is present
# (`ls -d "${TMPDIR:-/tmp}"/cqlite-worker-supervisor.lock`). Both halves are required: an old checkout
# that is merely idle can still be launched. Until then this guard is load-bearing.
# THAT `ls` ANSWERS FOR THE `TMPDIR` OF THE SHELL YOU RUN IT IN, AND FOR NO OTHER (#3549, job 222 F1):
# a launcher with a different `TMPDIR`, or one that named `SUPERVISOR_LOCK` itself, resolves a different
# absolute path, so run it under each launcher's own environment — and treat the ANCESTRY half as the
# one that actually closes the condition, since it holds for every path a launcher could pick. Same
# scope limit as the guard's own check; see the SPATIAL GAP in the RESIDUAL block below.
# (`supervisor_shell_quote` and `supervisor_one_line` are NOT part of that removal — see the note at
# each; they are general emitters.)
#
# The legacy lock's ON-DISK SHAPE is a DIRECTORY containing a single file `pid` (atomic `mkdir` +
# pid-liveness, never flock) — read out of the pre-#3467 file, not assumed. IT IS RECORDED HERE FOR
# CONTEXT AND NOTHING READS IT: per the lead's second ruling the guard tests for EXISTENCE only, so no
# behaviour of this script depends on that shape being what it says. Do not reintroduce a shape check.
# ---------------------------------------------------------------------------

# The two bytes that break the one-physical-line contract, held as VALUES rather than spelled as `$'…'`
# literals at the use sites: a `case` pattern and a `${var//…}` pattern both need them expanded, and
# inline they make the substitutions unreadable. Assigned BEFORE the function that reads them, so no
# call can see them unset under `set -u`.
SUPERVISOR_LF='
'
SUPERVISOR_CR=$'\r'
# ESC joins them (#3549, roborev job 201 F1 class sweep): `ESC[G` moves the cursor to column 1 and
# `ESC[2K` erases the line, so an escape sequence forges an apparently-unprefixed line WITHOUT a
# newline or a carriage return anywhere in the bytes — the same defeat of the one-bare-line contract by
# a third mechanism. Modern bash's `%q` already escapes it; bash 3.2's may not, which is why it joins
# the verified-and-repaired set below rather than being assumed away.
SUPERVISOR_ESC=$'\033'

# supervisor_shell_quote <string> — render a string so a PRINTED command is PASTE-SAFE and stays on ONE
# PHYSICAL LINE.
#
# The refusal below prints a command an operator is expected to RUN, and the path in it is derived from
# `TMPDIR` — i.e. from the environment, not from a literal. A path containing a space, a quote or a
# newline naively interpolated into `rm -f '<path>/pid'` produces a command that either fails or acts
# on a DIFFERENT path than the one we diagnosed, which is the worst kind of operator-facing text: it
# looks precise and is wrong.
#
# ONE PHYSICAL LINE IS PART OF THE CONTRACT, NOT A NICETY (#3549, roborev job 198 F4). The refusal's
# contract is "select one bare line and paste it": diagnostics carry a `worker-supervisor:` prefix and
# the runnable command is the one BARE line, which is what makes it identifiable without parsing prose.
# A newline in `TMPDIR` survives SINGLE QUOTING LITERALLY — `'a<LF>b'` is one shell word spanning two
# physical lines — so the previous single-quote-only form split the command across lines and left
# fragments that are indistinguishable from command lines. The same is true of the DIAGNOSTIC paths, which
# is why they are rendered through here too: an unquoted path with a newline turns one prose line into
# two, and the second has no prefix.
#
# `%q` IS THE RENDERING, AND ITS RESULT IS CHECKED RATHER THAN ASSUMED. Bash's `printf '%q'` renders a
# control character as an ANSI-C `$'\n'` escape, which keeps the word on one line. But %q's treatment of
# non-printing characters has CHANGED ACROSS BASH VERSIONS, and this file deliberately supports the
# bash 3.2 macOS ships (see the `read -d ''` loop note elsewhere), where a newline may instead come back
# as a literal backslash-newline — still two physical lines. So the one-line property is VERIFIED here
# and repaired if the builtin did not deliver it: an affirmative check, not a version assumption.
# `printf -v` is used rather than `$(…)`, because command substitution STRIPS trailing newlines and would
# silently truncate the rendering of a path that ends in one.
#
# THE REPAIR IS VERSION-INDEPENDENT: the single-quote form, with each newline/carriage return
# re-expressed as `'$'\n''` — closing the quote, an ANSI-C quoted escape, reopening — which every bash
# concatenates back into the original bytes on ONE line.
#
# THE OUTPUT IS BASH-SPECIFIC, and the remedy is documented as bash-pasteable: `$'…'` is a bash (and
# ksh/zsh) construct, not POSIX `sh`. That is the same assumption the script itself makes — it runs under
# `#!/usr/bin/env bash` and its operators paste into a bash shell.
#
# IT IS NOT OPTION-SAFETY, AND THE CALLER MUST NOT READ IT AS SUCH (#3549, roborev job 192 F2). Quoting
# controls how the SHELL splits a word; option parsing happens inside the COMMAND, on bytes the shell
# has already finished with. `rm -f '-scratch/pid'` is one quoted operand and still an "invalid option".
# A printed command whose operands are derived from the environment therefore needs `--` as well.
supervisor_shell_quote() {
  local q=""
  printf -v q '%q' "$1"
  case "$q" in
    *"$SUPERVISOR_LF"* | *"$SUPERVISOR_CR"* | *"$SUPERVISOR_ESC"*) ;;
    *) printf '%s' "$q"; return 0 ;;
  esac
  # This bash rendered a control character literally. Rebuild from the ORIGINAL string (the partially
  # escaped `$q` is not a safe base — it may end in a dangling backslash).
  q="'${1//\'/\'\\\'\'}'"
  q="${q//"$SUPERVISOR_LF"/\'\$\'\\n\'\'}"
  q="${q//"$SUPERVISOR_CR"/\'\$\'\\r\'\'}"
  q="${q//"$SUPERVISOR_ESC"/\'\$\'\\033\'\'}"
  printf '%s' "$q"
}

# supervisor_one_line <string> — render PROSE onto ONE PHYSICAL LINE, without quoting it.
#
# THE THIRD INSTANCE OF ONE CLASS, SO IT IS A CHOKE POINT AND NOT A THIRD SPOT FIX (#3549, roborev job
# 201 F1). The class is "a dynamic value reaches emitted text unrendered". Instance 1 was an em dash on
# the runnable command line (job 185 F2); instance 2 a newline in the diagnostic PATHS (job 198 F4);
# instance 3 a newline inside a STATE DESCRIPTION, which reaches the emitter as part of `$detail`. Each
# fix was correct and each left the next call site raw — the signal that per-call-site correctness is
# the wrong shape. So `supervisor_legacy_lock_refuse` renders EVERY prose argument it is handed, and no
# caller can emit a raw multi-line value whatever it interpolates.
#
# WHY NOT `supervisor_shell_quote` FOR PROSE. That renders a VALUE for PASTING: `%q` backslash-escapes
# every space, so a sentence comes back as `recorded\ pid\ 12\ is\ ACTIVE` — unreadable, and the prose
# channel is never pasted. This renderer therefore leaves printable bytes alone and rewrites only the
# three that break the contract; each has its own mechanism, so covering one is not covering the class:
#   LF  — splits the line, and the second half carries no `worker-supervisor:` prefix, which is exactly
#         the marker that identifies the ONE bare line as the runnable command;
#   CR  — returns the cursor to column 0, so following text OVERWRITES the prefix and the same forged
#         bare line appears with no newline in the bytes at all;
#   ESC — repositions the cursor or erases the line, producing that effect a third way.
# It is a DISPLAY rendering, NOT a reversible encoding: a literal two-character `\n` in the input comes
# out indistinguishable from a rendered newline. That is fine here (an operator reads this text, nothing
# parses it back) and is why the VALUE channel keeps the quoting renderer instead.
supervisor_one_line() {
  local s="$1"
  s="${s//"$SUPERVISOR_LF"/\\n}"
  s="${s//"$SUPERVISOR_CR"/\\r}"
  s="${s//"$SUPERVISOR_ESC"/\\e}"
  printf '%s' "$s"
}
# supervisor_legacy_lock_presence <path> — echo EXACTLY one of
#   verified-absent | present | could-not-tell <cause>
#
# THE GUARD DETECTS THE LEGACY LOCK AND STOPS THERE — IT DOES NOT PARSE IT (#3549, lead ruling).
#
# WHAT WAS DELETED AND WHY, so nobody rebuilds it as an improvement. There used to be a classifier
# (`supervisor_legacy_lock_state`) that opened the lock directory, enumerated it, parsed its `pid` file
# and measured the recorded pid's liveness, producing `live <pid>` / `stale <pid>` / `unknown <cause>`;
# it needed a pid-liveness probe (`supervisor_pid_liveness`, deleted with it — this was its only
# caller; #3601 later rebuilt the CAPABILITY, under a name that says whose question it answers, for the
# PER-LANE lock, where the verdict does change the decision — see the probes above `acquire_lock`), a
# platform pid bound read from `/proc/sys/kernel/pid_max`, a NUL probe, a single-line parse,
# a collation-free digit test, and a wholesale neutralisation of the caller's inherited glob/match state
# (`GLOBIGNORE`, `dotglob`, `nullglob`, `failglob`, `noglob`, `nocasematch`, …) with per-option
# verification and an order-sensitive restore — because the enumeration's correctness depended on all of
# it.
#
# THE ARGUMENT THAT ENDED IT: SINCE THE RECLAIM WAS REMOVED, `live`, `stale` AND `unknown` ALL REFUSE.
# The classification therefore CANNOT CHANGE THE DECISION. Its only outputs were the wording of the
# refusal and which remedy line printed — and the remedy it licensed was a DELETION, which is the one
# thing a guard that mutates nothing has no business recommending about an object it does not own.
# Machinery whose output cannot change the decision is not a guard; it is a description generator
# sitting on the decision path. Every one of those parts had also been the subject of its own review
# round (a false `{pid}` shape forged by `GLOBIGNORE`, a NUL silently discarded by `read`, an
# out-of-range pid read as dead, a locale-dependent digit test, a `ps` fallback that called a live
# BusyBox pid 1 dead) — one defect class, reached through a new door each time, all of it in service of
# a distinction with no consumer.
#
# SO THE QUESTION IS NARROWED TO THE ONE THE DECISION ACTUALLY NEEDS: IS SOMETHING THERE?
#   present         => refuse. Name the path. Claim NOTHING about staleness, about what the object is,
#                      or about whether removing it is safe.
#   verified-absent => proceed.
#   could-not-tell  => refuse, with a cause that says THE PROBE FAILED — never that a lock exists.
#
# ABSENCE MUST BE VERIFIED ABSENCE, WHICH IS WHY THIS IS THREE-VALUED AT ALL. Every `test`/`[[ ]]` file
# predicate is TWO-VALUED, so it must fold "cannot tell" onto one of its answers and always picks the
# permissive one (named doctrine in CLAUDE.md). Existence of a known child name is decided by `lstat(2)`
# on that name, which needs SEARCH (`-x`) on the container and nothing else — we never enumerate the
# container, so `-r` on it is NOT required and demanding it would be a false STOP on a legitimate
# write-and-search-only `TMPDIR` (mode 0311/0711). An UNSEARCHABLE container is the case that matters:
# `[[ -e ]]` then answers "not there" for a lock that IS there, which is the permissive collapse this
# probe exists to avoid, so it is `could-not-tell` and refuses.
#
# A SYMLINK COUNTS AS PRESENT, INCLUDING A DANGLING ONE. The existence test is `-e || -L`: `-e` follows
# the link and is FALSE for a broken one, so `-e` alone would report a dangling symlink at the legacy
# path as absence. No supervisor at any version creates that path as a symlink, so there is nothing
# there to be compatible with — but something IS at the name, our own `mkdir` of a per-lane path is
# unaffected, and a pre-#3467 supervisor's `mkdir` of THIS name would fail against it, so the honest
# answer is `present` and the operator is told to look.
#
# WHAT THIS PROBE CAN AND CANNOT DISTINGUISH — RECORDED, NOT PAPERED OVER. Bash's file tests expose no
# errno. `[[ -e X ]]` is false when `lstat(X)` fails for ANY reason, so an `lstat` that fails with EIO
# (failing disk), ELOOP, ENOMEM or an ENOTCONN stale network mount is INDISTINGUISHABLE FROM ENOENT to
# this program. Concretely:
#   DECIDABLE, and each is measured by the suite:
#     * the container is missing or not searchable => `could-not-tell` (refuse). This is the dominant
#       real cause of a blind existence test, and it is the one the two-valued form got wrong.
#     * `lstat` of the child SUCCEEDS => `present` (refuse), for every object type including a symlink.
#     * container searchable AND `lstat` of the child reports ENOENT => `verified-absent` (proceed).
#   NOT DECIDABLE, therefore reported as `verified-absent`:
#     * an `lstat` of the CHILD that fails for a reason other than ENOENT, on a searchable container.
#     * a divergence between `access(2)` (what `-x` asks) and actual traversal — a MAC layer
#       (SELinux/AppArmor) or an NFS ACL can deny a traversal that `-x` permits; and as root, `-x` is
#       false on a mode-0000 directory that root can nevertheless traverse (that direction lands on
#       `could-not-tell`, i.e. the refusing side).
# NO PROBE HERE CLOSES THE FIRST GAP, AND ONE IS DELIBERATELY NOT INVENTED. Distinguishing EIO from
# ENOENT needs the errno, which means an external command (`stat`, `test` from coreutils) — PATH
# exposure and an output format to parse, in a function that today executes NO external command and
# whose verdicts therefore cannot be changed by the caller's `PATH`. Trading that for a rarer failure
# mode would buy the appearance of completeness, which is worse than a written-down gap: an `lstat`
# failing with EIO is a box with a broken filesystem, where a supervisor refusing to start is not the
# problem anyone has. So the residual is stated here and left open.
supervisor_legacy_lock_presence() {
  local legacy="$1" dir
  dir="${legacy%/*}"
  # No slash at all => the current directory; a single LEADING slash (a `TMPDIR` of `/`) strips to the
  # empty string, which is not a path and would misreport an undeterminable container.
  # `if`, NOT `[[ … ]] && x`: both tests are FALSE in the ordinary case, so each statement's own value
  # is 1, and the same line as a function's LAST statement would return 1 and abort an errexit caller.
  # Writing them as `if` removes a correctness property that depends on where the line happens to sit.
  if [[ "$dir" == "$legacy" ]]; then dir="."; fi
  if [[ -z "$dir" ]]; then dir="/"; fi
  if [[ ! -d "$dir" || ! -x "$dir" ]]; then
    printf 'could-not-tell container-not-searchable:%s\n' "$(supervisor_shell_quote "$dir")"
    return 0
  fi
  # `-e || -L`, in that order and both required: see the symlink note above.
  if [[ -e "$legacy" || -L "$legacy" ]]; then
    printf 'present\n'
    return 0
  fi
  printf 'verified-absent\n'
}

# supervisor_legacy_lock_refuse <path> <detail> [remedy-prose] [remedy-command] — LOUD, DIAGNOSTIC, and
# TEXTUALLY DISTINCT from the per-lane "another instance is already running" refusal, so an operator
# (and a test) can tell which of the two locks stopped the start.
#
# A RUNNABLE COMMAND IS A SEPARATE ARGUMENT AND GETS A LINE OF ITS OWN — NEVER INLINED IN PROSE (#3549,
# roborev job 185 F2). It was inlined once, with a trailing em dash and an explanatory clause after it,
# and the consequence is worse than untidiness. Pasted verbatim, the prose becomes extra `rmdir`
# OPERANDS and the command ALWAYS fails; what it leaves behind depends on the prose, and both measured
# outcomes are bad (`scripts/tests/test_worker_supervisor.sh` runs them):
#   - with the shipped em dash, `rm -f` succeeds and `rmdir` removes the directory AND THEN errors on
#     each prose word ("failed to remove 'the'", …), so the operator gets a non-zero exit and three
#     alarming messages with no way to tell whether the lock was actually cleared;
#   - with any prose carrying an option-shaped token (`-x`), the outcome depends on whether the command
#     terminates option parsing. The line now emits `--` (job 192 F2), so such a token is an OPERAND and
#     the failure is the one above; on the PRE-F2 line `rmdir` read it as an OPTION and rejected the
#     whole invocation before removing anything — leaving a PID-LESS lock directory, precisely the shape
#     a pre-#3467 supervisor reads as stale and reclaims, i.e. the remedy manufacturing the hazard this
#     guard exists to prevent. Both spellings are still MEASURED in the suite, so this note cannot
#     outlive the behaviour it describes.
# Do not re-inline it, and do not append punctuation: the command line must be the WHOLE line, and it is
# printed BARE (no `worker-supervisor:` prefix) so that selecting the line is enough to paste it.
#
# EVERY LINE IS EMITTED WITH `printf '%s\n'`, NEVER `echo` — THE EMITTER MUST NOT UNDO THE RENDERERS
# (#3549, roborev job 205 F2). `supervisor_one_line` and `supervisor_shell_quote` encode a control
# character AS A BACKSLASH SEQUENCE, which is precisely what `echo` under bash's `xpg_echo` option
# INTERPRETS: MEASURED, `shopt -s xpg_echo; echo 'a\nb'` emits TWO PHYSICAL LINES, so the renderer's
# guarantee is thrown away at the last step and the second half of a prose line arrives with no
# `worker-supervisor:` prefix — the forged bare line the whole one-line contract exists to prevent. It
# is INHERITED STATE, not a local choice: `env BASHOPTS=xpg_echo` is imported by bash (measured), so
# nothing in this file needs to have run `shopt` for it to be on.
#
# `printf` is a bash BUILTIN, so this introduces no PATH exposure, and it takes NO option that changes
# how it treats its payload; the format is the LITERAL `'%s\n'`, so a payload that begins with `-` or
# contains a `%` is data. Do not reintroduce `echo` here for any reason, including brevity.
supervisor_legacy_lock_refuse() {
  local legacy="$1" detail="$2" remedy="${3:-}" remedy_cmd="${4:-}"
  # EVERY PROSE ARGUMENT IS RENDERED HERE, ONCE — A CHOKE POINT, NOT N CORRECT CALL SITES (#3549,
  # roborev job 201 F1). `$detail` and `$remedy` carry values the callers interpolate from the
  # environment and from disk (a `TMPDIR`-derived container path inside a state description, a pid
  # file's bytes), and a raw newline in any of them splits a prose line so the second half has no
  # `worker-supervisor:` prefix — indistinguishable from the ONE bare line an operator is told to select
  # and paste. Rendering at the EMITTER makes that unreachable for every caller, present and future,
  # rather than relying on each interpolation site being remembered: this class had already been fixed
  # twice at call sites (jobs 185 F2, 198 F4) and re-appeared at a third.
  # `|| true` ON THE RENDERER CAPTURES: this function's ONE job is to print a refusal, and an
  # assignment-from-substitution is a genuine errexit abort site (#3549, roborev job 201 F3 class
  # sweep — the `shopt -p` shape). The renderers return 0 on every path they take, so this is
  # unreachable; if it ever fires, a rendering that could not be made costs THAT FIELD and never the
  # refusal, and the one-physical-line contract still holds because an empty field cannot break it.
  detail="$(supervisor_one_line "$detail")" || true
  remedy="$(supervisor_one_line "$remedy")" || true
  # THE COMMAND CHANNEL CANNOT BE RENDERED — a rendered command is not executable — SO IT IS VERIFIED,
  # and a command that is not one physical line is DEMOTED TO PROSE rather than printed bare. The
  # shipped callers build their commands through `supervisor_shell_quote`, which guarantees the
  # property, so this branch is unreachable today; it exists so the emitted-output contract holds by the
  # EMITTER's rules and not by a caller's discipline. Printing such a command is the worst outcome
  # available here: several bare lines, each a fragment, where the contract promises exactly one.
  local cmd_broken=""
  case "$remedy_cmd" in
    *"$SUPERVISOR_LF"* | *"$SUPERVISOR_CR"* | *"$SUPERVISOR_ESC"*)
      cmd_broken="$(supervisor_one_line "$remedy_cmd")" || true
      remedy_cmd=""
      ;;
  esac
  # THE DIAGNOSTIC PATHS ARE RENDERED, NOT INTERPOLATED RAW (#3549, roborev job 198 F4). Both paths come
  # from the environment, and a newline in `TMPDIR` interpolated raw SPLITS a prose line in two — the
  # second half carrying no `worker-supervisor:` prefix, which is precisely the marker that identifies
  # the one BARE line as the runnable command. So an unrendered diagnostic path does not merely wrap: it
  # manufactures text that an operator (and this file's own structural test) cannot tell from a command.
  # One physical line per emitted line is the contract; `supervisor_shell_quote` is what holds it.
  local legacy_shown="" own_shown=""
  legacy_shown="$(supervisor_shell_quote "$legacy")" || true
  own_shown="$(supervisor_shell_quote "$SUPERVISOR_LOCK")" || true
  printf '%s\n' "worker-supervisor: refusing to start — LEGACY GLOBAL supervisor lock $legacy_shown: $detail" >&2
  # THIS LINE DESCRIBES TWO RESOLVED PATHS; IT ASSERTS NO RELATIONSHIP IT HAS NOT ESTABLISHED (#3549,
  # roborev job 222 F2). It used to say our own lock "is PER LANE" unconditionally and conclude that the
  # two are therefore invisible to each other. Per-lane is only the DEFAULT: a caller may name our lock
  # path itself, and that name may be machine-global — it may even BE the legacy path — in which case
  # both halves of the old sentence were false at once. It also said "the" machine-global lock, as if
  # one such path existed; the name is `TMPDIR`-derived, so it is the machine-global path only for a
  # launcher whose environment resolves it the same way ours does (see the SPATIAL GAP in the RESIDUAL
  # block above). So: print both paths as RESOLVED, and make the consequence conditional on them
  # differing, which is the fact that actually produces the co-run. Naming the override VARIABLE here is
  # separately forbidden (job 208 F1: a refused operator must not be handed something that reads as an
  # escape hatch), which is why it describes the LAUNCHER rather than naming the variable.
  printf '%s\n' "worker-supervisor: that path is the pre-#3467 machine-global single-instance lock name as THIS process's \${TMPDIR:-/tmp} resolves it; this run's own lock is at $own_shown (per lane by default, or wherever this run's launcher named it) — so unless those two are the SAME path, neither supervisor can see the other's lock and both would run in one worktree (#3549)." >&2
  # A CASE-SPECIFIC remedy, when there is one. The two refusing states differ in what an operator should
  # DO — a PRESENT lock means "something is at that path; find out what it is"; a probe that COULD NOT
  # ANSWER means "the question was undecidable here; make it decidable" — so the generic line below is
  # not sufficient on its own.
  [[ -z "$remedy" ]] || printf '%s\n' "worker-supervisor: remedy for THIS state — $remedy" >&2
  [[ -z "$cmd_broken" ]] || printf '%s\n' "worker-supervisor: a remedy command for this state was built with an embedded control character, so it is NOT printed as a runnable line (it could not be one); rendered for reading only: $cmd_broken" >&2
  [[ -z "$remedy_cmd" ]] || printf '%s\n' "$remedy_cmd" >&2
  # NO REFUSAL MENTIONS `SUPERVISOR_LOCK`, AND THE ABSENCE IS DELIBERATE (#3549, roborev job 208 F1).
  # This line used to end with "…, or set SUPERVISOR_LOCK explicitly to opt out of this compatibility
  # check", and it was printed by EVERY refusal. Read as an operator reads it, that was an instruction
  # that CAUSED the harm this guard exists to prevent: naming the lock skipped the check, so a refused
  # run started anyway and the two supervisors shared one worktree, invisible to each other. It
  # survived fourteen review rounds of the surrounding code because it reads as helpful.
  #
  # THERE IS NOW NO SUCH ESCAPE HATCH AT ALL (lead ruling 2026-08-30, AC4 removed as unsound): setting
  # `SUPERVISOR_LOCK` chooses where OUR lock lives and does not affect this check. So nothing may be
  # printed here that even IMPLIES a way past the refusal — there is none, and inventing one in prose
  # would send an operator looking for a knob that does not exist. The remedies are the two real ones:
  # stop the pre-#3467 supervisor, or upgrade that checkout past #3467.
  printf '%s\n' "worker-supervisor: remedy — stop the pre-#3467 supervisor on this box, or upgrade that checkout to #3467+." >&2
  exit 1
}

# THIS GUARD DETECTS AND REFUSES. IT NEVER MUTATES THE FOREIGN LOCK (#3549, lead ruling).
#
# Nothing on this path touches an object this run does not own — no rename-aside, no delete, no
# adoption, no re-creation, and since the lead's second ruling NO READ OF ITS CONTENTS EITHER: the
# guard tests for EXISTENCE and stops. Anything other than a verified absence stops the start and names
# the path. That is a deliberate REDUCTION in capability, twice over, and the reason is recorded here
# because the deleted version was written three times and produced the same harm each time.
#
# WHY RECLAIMING WAS REMOVED. Reclaiming a stale lock means renaming it aside, and every abort path
# after that rename must put it BACK — so a reclaim cannot be safer than its restore. Restoring a
# DIRECTORY-WITH-CONTENTS is not expressible atomically with the primitives available here: the
# formulation that was shipped, `mkdir "$legacy"` followed by `mv "$aside/pid" "$legacy/pid"`, is TWO
# steps, and between them the lock is observable WITHOUT its pid file. A pre-#3467 supervisor that
# looks in that window reads a pid-less directory as STALE, reclaims it, and our `mv` then writes into
# ITS lock — corrupting a live holder's lock and leaving the real holder effectively lockless. That is
# strictly worse than the check-then-act it replaced, which at least failed toward refusal. Three
# successive fixes each produced that same harm, which is the signal that the shape, not the code, was
# wrong.
#
# THE ATOMIC FORMULATION THAT WOULD WORK, recorded so nobody rediscovers it as novel: build the lock
# COMPLETE in a private staging directory (`mkdir "$staging"`; write `"$staging/pid"`) and move it into
# place with ONE `rename(2)`. The directory is then never observable without its pid, and the rename
# FAILS against an existing non-empty target instead of nesting inside it. It needs true no-replace
# rename semantics — `mv -T` / `RENAME_NOREPLACE` — which are GNU-ONLY, and this file deliberately
# supports macOS (its own header records that macOS ships no `flock(1)`). So the atomic form was
# AVAILABLE AND DECLINED: that portability cost, plus the lead's ruling that refusal is the default,
# is why this guard refuses instead.
#
# AC2 OF #3549 ("reclaimed rather than blocking forever") IS SATISFIED BY THE REFUSAL, per the lead's
# ruling: a LOUD refusal that names the legacy path, the precondition (stop or upgrade the legacy
# launcher FIRST) and a READ-ONLY inspection line of its own is not "blocking forever" — an operator
# who has established that no pre-#3467 supervisor can run on the box removes the path themselves, and
# the alternative is a supervisor that silently mutates another supervisor's lock.
#
# AND THE PRINTED LINE IS AN INSPECTION, NOT A DELETION — WHICH IS WHAT THE SECOND RULING CHANGED
# (#3549). While the classifier existed, an enumerated exactly-`{pid}` shape was what LICENSED printing
# `rm -f -- <dir>/pid && rmdir -- <dir>`. With no inspection there is no such licence, and the deletion
# line is not merely unjustified but measurably DESTRUCTIVE on a shape we no longer look at: MEASURED,
# with a SYMLINK at the legacy path pointing at a foreign directory, `rm -f -- <legacy>/pid` follows the
# link and DELETES THAT DIRECTORY'S `pid` FILE (rc=0), after which `rmdir -- <legacy>` fails with "Not a
# directory" — so the operator destroys a file this run never examined and the lock is still there.
# See the `present` branch for the line that is printed instead, and why printing one at all is right.

# RESIDUAL (#3549, roborev job 178) — THIS GUARD REDUCES THE COLLISION WINDOW; IT DOES NOT ELIMINATE
# IT. Read plainly: it is a STARTUP CHECK, not machine-wide mutual exclusion.
#
# 1. THE UNCLOSED WINDOW. A pre-#3467 supervisor that starts AFTER this check completes is not stopped
#    by anything here. It consults ONLY the legacy global path and knows nothing of our per-lane lock,
#    so once we have passed the check — or once an operator has removed a lock we refused over — it can
#    take the legacy name and run alongside us in the same worktree. The check is a single existence
#    test: the window is "after that test", and nothing here can shrink it further. Tracked as #3596.
# 2. WHY IT IS NOT CLOSED HERE. The only construction that closes it is for THIS supervisor to acquire
#    and HOLD the legacy global lock for its whole lifetime — which reimposes MACHINE-GLOBAL exclusion
#    and would make the second post-#3467 lane on a box refuse to start. That is precisely what #3467
#    removed and what #3393's owner ruling forbids (N lanes per box is the standing model; "one worker
#    per machine" is RETRACTED). Closing this window would reverse a standing owner ruling, which a
#    bug-fix does not get to do.
# 3. WHY THAT IS TOLERABLE. The activation condition is a TRANSIENT ROLLING-UPDATE window, and its
#    precondition is currently measurably EMPTY: the #3549 census found zero `refs/machine-claims/*`
#    and zero `refs/lane-claims/*` fleet-wide and zero production supervisors running. It closes
#    PERMANENTLY once every checkout a launcher can reach is at or past #3467 — the same condition as
#    this guard's own REMOVAL CONDITION above.
# 4. SO: do not read the guard as complete mutual exclusion. It refuses a start under a lock that is
#    ALREADY PRESENT, whatever it is (the case #3467 regressed), and MUTATES NOTHING — it cannot destroy or
#    corrupt any holder's lock, live or dead, because it never writes to the legacy path at all; a
#    supervisor that starts later from an old checkout remains a documented, bounded risk. Per #3549's own doctrine: a
#    deferred defect whose activation condition is unwritten is a landmine; one whose condition is
#    written is a documented risk.
#
# AND THE SECOND GAP, WHICH IS SPATIAL WHERE #3596 IS TEMPORAL (#3549, roborev job 222 F1; lead ruling
# 2026-08-31 — DECLARE IT, change no detection logic). Read the two together: #3596 above is a gap in
# TIME (a pre-#3467 supervisor that starts AFTER our check); this is a gap in SPACE (a pre-#3467
# supervisor whose lock is not at the path we tested).
#
# 5. WHAT IS DETECTED, EXACTLY. One path: `${TMPDIR:-/tmp}/cqlite-worker-supervisor.lock`, as THIS
#    process's own environment resolves it at guard time. Nothing else is looked at, in any state.
# 6. WHAT IS THEREFORE NOT DETECTED. A pre-#3467 supervisor launched with a DIFFERENT `TMPDIR` — its
#    machine-global default then resolves to a different absolute path — or one launched with an
#    explicit `SUPERVISOR_LOCK`: the pre-#3467 script honours that variable too — read out of the
#    pre-image, `git show f33f726c4^:scripts/local/worker-supervisor.sh` line 215 is
#    `SUPERVISOR_LOCK="${SUPERVISOR_LOCK:-${TMPDIR:-/tmp}/cqlite-worker-supervisor.lock}"` — so its lock
#    sits wherever its own launcher put it. In either case that supervisor holds a lock at a
#    path this guard never stats, and our `verified-absent` is a true statement about the path we tested
#    and says nothing about the one it holds. This is why the proceed-path line and the refusal's
#    relationship line both state which path was resolved and by whose environment: a bare absence
#    invites the falsely reassuring reading, and an unqualified "no legacy lock" would be a claim this
#    check cannot support.
# 7. WHY IT CANNOT BE CLOSED. Another process's environment is UNKNOWABLE from here — we cannot read the
#    `TMPDIR` a supervisor we have never seen was launched with, nor a lock path its launcher chose —
#    the missing input is not on this side of the process boundary, so no amount of care here produces
#    it. And "fail closed when the path cannot be established" degenerates: since the path
#    can NEVER be established for an arbitrary launcher, fail-closed would mean REFUSE ALWAYS, and a
#    guard that never permits a start is broken, not fail-closed. The verdict stays scoped to the path
#    it can name, and the SCOPE is declared instead.
# 8. WHY EXTRA PROBES WERE REJECTED, and not merely "not done" (lead ruling). Probing further candidate
#    paths (other plausible `TMPDIR`s, a glob of lock-shaped names) would each add a place where a
#    STALE directory permanently refuses every lane with NO REMEDY: since the classifier was deleted
#    (see `supervisor_legacy_lock_presence`) this guard cannot tell a stale lock from a live one, so
#    every additional probed path buys detection of a rarer live collision at the price of a permanent
#    false refusal — which inverts the trade the refusal is worth making at ONE canonical path an
#    operator can reason about. More probes would also manufacture exactly the appearance of
#    completeness this block exists to deny.
# 9. WHEN IT CLOSES: the SAME condition as #3596 and as this guard's own REMOVAL CONDITION above —
#    every checkout a launcher can reach is at or past #3467, at which point no pre-#3467 supervisor can
#    run under ANY `TMPDIR` or lock name. All THREE retire together; do not close one and leave the
#    others' records standing.
supervisor_legacy_lock_guard() {
  # THIS GUARD ALWAYS RUNS. THERE IS NO SKIP AND NO OPT-OUT (#3549, lead ruling 2026-08-30 — AC4
  # REMOVED AS UNSOUND).
  #
  # AC4 used to exempt a run whose `SUPERVISOR_LOCK` was named by the caller, on the reasoning that
  # such an operator had taken the placement decision. The reasoning does not hold: an explicit
  # `SUPERVISOR_LOCK` renames OUR lock, and a pre-#3467 supervisor uses the machine-global path
  # REGARDLESS — it has never heard of the variable — so the exemption skipped the check in exactly the
  # case where the collision is still LIVE. A naming choice is not an isolation guarantee. Deleted with
  # it: the tri-state provenance, its pinned path partner, and every early return here that read them.
  #
  # AND THE SUBJECT IS READ HERE, AT GUARD TIME, IN THE SAME BREATH AS THE DECISION IT FEEDS (#3549,
  # roborev job 218 — unexpressible rather than patched). That finding was that the per-lane lock path
  # was PINNED at first resolution while the legacy path was recomputed from the CURRENT `TMPDIR`, so a
  # wrapper that changed `TMPDIR` between the two moments made the recorded provenance describe one
  # path while the check looked at another. There is no longer a record, a pinned partner, or a second
  # moment: this line is the only place a legacy path is computed, and its value cannot disagree with a
  # decision taken elsewhere because no decision is taken elsewhere. `TMPDIR` at guard time is also the
  # RIGHT reading on its own terms — a pre-#3467 supervisor derives the machine-global name from the
  # environment it starts in, which our own resolution moment says nothing about.
  local legacy="${TMPDIR:-/tmp}/cqlite-worker-supervisor.lock" state shown=""
  # A PROBE THAT COULD NOT ANSWER IS A REFUSAL, NEVER A SILENT EXIT (#3549, roborev job 201 F3).
  # The probe returns 0 on every path it takes, but under a caller with `inherit_errexit` any
  # unforeseen non-zero inside the `$( )` makes THIS ASSIGNMENT non-zero, and with the script's own
  # `set -e` that ends the whole supervisor with no message at all — a start neither permitted nor
  # explained. This is the fail-closed backstop, and it lands in the `could-not-tell` branch, which
  # refuses.
  state="$(supervisor_legacy_lock_presence "$legacy")" || state="could-not-tell presence-probe-exited-nonzero"
  case "$state" in
    verified-absent)
      # THE PROCEED PATH STATES WHAT WAS CHECKED, BECAUSE A BARE ABSENCE INVITES A FALSELY REASSURING
      # NEGATIVE (#3549, roborev job 222 F1 — lead ruling: DECLARE THE SCOPE, change no detection
      # logic). This branch used to be silent. Silence is not itself a false claim, but the guard runs
      # on every start and an operator reading a clean start reasonably concludes "no pre-#3467
      # supervisor can be holding a lock here" — which is NOT what was established. What WAS
      # established is narrower, and the line says exactly it: ONE path was tested, and that path was
      # derived from THIS process's `TMPDIR`. A pre-#3467 supervisor launched with a different `TMPDIR`,
      # or with its own lock path named by its launcher, holds a path this check never looks at. That is
      # the SPATIAL gap recorded in the RESIDUAL block above; it is stated here, in the operator's own
      # output, because the RESIDUAL block is read by whoever edits this file and this line is read by
      # whoever runs it.
      #
      # ONE LINE, at the level `log` already uses for startup facts, and only here — the refusing
      # branches already NAME the path they found, so they assert no completeness that needs qualifying
      # beyond the relationship line in the emitter. The path is rendered so a `TMPDIR` containing a
      # control character cannot split this into two physical lines (the same contract the refusal
      # emitter holds; `log` is `printf`-based, so `xpg_echo` cannot reinterpret the escape either).
      # `|| true` because an assignment-from-substitution is an errexit abort site and a rendering that
      # could not be made must cost the FIELD, never the line.
      #
      # IT NAMES NO VARIABLE. Job 208 F1's rule is about refusals, but the reason generalises: a line
      # printed on every successful start is the most-read text this guard has, and there is no opt-out
      # to advertise in it either (AC4 removed as unsound). The launcher, not the variable, is what an
      # operator can act on.
      shown="$(supervisor_shell_quote "$legacy")" || true
      log "legacy-lock check: nothing at $shown, which is the ONLY path this check tested — it is the pre-#3467 machine-global lock name as THIS process's \${TMPDIR:-/tmp} resolves it. A pre-#3467 supervisor started under a different TMPDIR, or with its lock path named by its own launcher, would hold a path this check does not see; and one that starts after this moment is not stopped by it either (#3549 spatial gap, #3596 later-start gap)."
      return 0
      ;;
    present)
      # THE REFUSAL CLAIMS NOTHING ABOUT WHAT IS THERE (#3549, lead ruling). It does not say the holder
      # is dead, it does not say the holder is alive, it does not say the object is a lock, and it does
      # not promise that removing it will succeed — because this guard did not look. Every sentence
      # below is a statement about what WE did, or an instruction whose precondition the OPERATOR must
      # establish. The previous version's `live`/`stale` wordings asserted a process's fate from a bare
      # pid, and each such assertion cost a review round.
      #
      # WHY A COMMAND IS PRINTED AT ALL, AND WHY IT IS READ-ONLY. The refusal's entire content is
      # "something is at this path and we deliberately did not look inside", so the operator's next
      # question is necessarily "what IS it?" — and that is precisely the question a read-only line can
      # answer without this guard making a single claim. It is also the only shape whose mis-paste is
      # harmless: `ls` cannot destroy anything, so the worst outcome is an uninformative line. (Same
      # reasoning that kept the read-only `ps -p` line in the deleted `live` branch, and the exact
      # opposite of the deletion line, which was measurably destructive on a symlink — see above.)
      # `ls -ldn` names the OBJECT without following a symlink (link vs directory vs plain file);
      # `ls -lna` then shows the CONTENTS when it is a directory. Measured rc=0 on all three shapes, so
      # the line is not noise on any of them. `--` because the path is `TMPDIR`-derived and an
      # option-shaped one would otherwise be parsed as flags (quoting is not option-safety); paths are
      # rendered through `supervisor_shell_quote` so the line is paste-safe and stays on ONE physical
      # line; and it is the FOURTH argument, so the emitter prints it BARE on its own line rather than
      # inlined in prose where pasting it would append prose words as operands.
      #
      # NO REMOVAL COMMAND IS PRINTED, DELIBERATELY. A removal that is correct depends on what the
      # object is, which is the thing we chose not to determine; and printing one would be an implicit
      # claim that removal is safe now. The prose says removal is the operator's call once they have
      # established the precondition, and says what makes it safe when they do (non-recursive, so it
      # cannot silently delete contents nobody examined).
      supervisor_legacy_lock_refuse "$legacy" "a path EXISTS there (this guard tests for EXISTENCE ONLY — it does not open, read, enumerate or inspect that path, so it makes NO claim about what the object is, whether any holder is alive, or whether removing it would be safe)" \
        "PRECONDITION FIRST — stop the pre-#3467 supervisor(s) on this box, or upgrade that checkout past #3467. That path becomes removable only once YOU have established that no pre-#3467 supervisor can run here; this guard has established no such thing, and it has NOT verified that any removal will succeed. Removing it before the precondition frees the legacy name for a pre-#3467 supervisor to take at once, which is the concurrency this refusal exists to prevent. To see WHAT is actually there, run the next line, on its own, exactly as printed — it only reads. If you then remove the path, use a NON-RECURSIVE removal (rmdir refuses a non-empty directory) so nothing you have not examined is deleted" \
        "ls -ldn -- $(supervisor_shell_quote "$legacy") && ls -lna -- $(supervisor_shell_quote "$legacy")"
      ;;
    *)
      # THE CAUSE SAYS THE PROBE FAILED — IT DOES NOT SAY A LOCK EXISTS (#3549, lead ruling). "Cannot
      # tell" and "something is there" are different facts with different remedies, and reporting the
      # first as the second would send an operator hunting for a lock that may not exist. The refusal
      # is identical in force, and only in force.
      supervisor_legacy_lock_refuse "$legacy" "THE EXISTENCE PROBE FAILED (${state#could-not-tell }) — this is NOT a report that a legacy lock exists; it is a report that this run could not decide whether one does, and 'cannot tell' is a refusal here, never a green light" \
        "make the question decidable and re-run: the container directory named in the cause must exist and be SEARCHABLE by this user (mode +x — the probe stats a known child name and never enumerates, so read permission is not needed). Until then a legacy lock sitting at that path would be invisible to this check"
      ;;
  esac
}

# ---------------------------------------------------------------------------
# THE PER-LANE LOCK'S HOLDER PROBES (#3601) — WHY THEY EXIST AND WHY THEY ARE SHAPED LIKE THIS
#
# `acquire_lock` below claims a per-lane lock with `mkdir`, and when the name is already taken it has
# to answer ONE question: MAY WE TAKE IT? Before #3601 it answered with `cat` + `kill -0`, which is
# two-valued in three separate ways at once, and every one of them collapsed onto the PERMISSIVE
# answer — RECLAIM:
#
#   * the recorded pid was used UNPARSED, so an empty, garbled or multi-line `pid` file made `kill -0`
#     fail, and that failure read as "the holder is dead";
#   * `kill -0` fails with EPERM as well as ESRCH, so a LIVE holder owned by another user read as dead;
#   * `mkdir` followed by `echo $$ >…/pid` leaves a window in which the lock exists with NO pid file,
#     and a peer arriving in that window read a STARTING holder as a dead one.
#
# In all three the outcome is identical and it is the worst one available: the lock is taken from a LIVE
# holder and two supervisors run in one worktree — the concurrency the lock exists to prevent.
#
# THE PROBES ARE THREE-VALUED AND "CANNOT TELL" REFUSES. That is the standing rule (CLAUDE.md; #3549
# lead ruling 1), and it is the whole content of this fix: `dead` is the ONLY verdict that licenses a
# reclaim, and it must be AFFIRMATIVE — evidence the process is GONE, never absence of evidence that it
# is there.
#
# AND A REFUSAL IS NOT A DEAD END — RULING 2, WHICH IS THE HARD HALF. "Cannot tell ⇒ refuse" applied
# without an answer to "then how does a stale lock EVER get cleared, and by whom?" produces a lane
# blocked forever by a directory, and a guard that never permits work is broken, not fail-closed. The
# answer, in full:
#
#   1. NORMAL EXIT — the holder's own EXIT trap (`supervisor_lock_release`) removes the lock. Nobody
#      else is involved and nothing has to be cleared.
#   2. HOLDER KILLED (-9, OOM, reboot) — the lock survives WITH a well-formed pid. The next start reads
#      it, `supervisor_lock_holder_liveness` returns an AFFIRMATIVE `dead`, and the lock is RECLAIMED
#      AUTOMATICALLY, exactly as before #3601. This is the stale case that actually happens on this
#      fleet; it needs no operator, and nothing in this change narrows it.
#   3. THE UNDECIDABLE REMAINDER — a pid file that is PERSISTENTLY absent or unparseable, or a liveness
#      probe that could not answer. Only here do we refuse, and every such refusal PRINTS THE PATH AND
#      A COMMAND THAT CLEARS IT, so the operator is the mechanism and the refusal is the instruction.
#      A refusal naming no remedy is what turns "fail closed" into "wedged", which is itself a defect.
#
# WHY (3) IS A VANISHINGLY SMALL SET, BY CONSTRUCTION RATHER THAN BY HOPE. The pid is published by
# RENAME (`supervisor_lock_publish`), so the `pid` NAME never exists holding partial content: a reader
# either does not see the name, or sees the complete value. The only undecidable shapes left are a lock
# whose holder died inside the one-rename window after `mkdir`, and a `pid` file corrupted by something
# outside this script. Neither is a state a healthy supervisor passes through.
#
# WHAT THESE PROBES DO NOT DEFEND AGAINST, STATED. Anything that can WRITE our lock directory can write
# any pid it likes into it, and no parse can tell a forged pid from a recorded one. That is not a hole
# these probes could close: whoever can write `$SUPERVISOR_LOCK` can also `rmdir` it. The parse's job is
# to reject content no legitimate writer produces (crash residue, a partial write, a truncated file),
# not to authenticate the writer.
#
# WHY NOT `supervisor_pid_liveness`, THE NAME AC3 ASKED FOR: that symbol was DELETED in #3549's final
# form together with the legacy classifier that was its only caller (see the note above
# `supervisor_legacy_lock_presence`), and the deletion is asserted by this file's suite. It is not
# revived here, because the argument that removed it — a verdict that cannot change the decision is a
# description generator on the decision path — is TRUE of the legacy guard and FALSE here: on this path
# the verdict selects between refusing and reclaiming. So the capability is rebuilt under a name that
# says WHOSE question it answers, and the legacy path is asserted (as a PROPERTY, not a name) to
# measure no pid liveness at all.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# THE PER-LANE LOCK'S CONSTANTS — DELIBERATELY NOT KNOBS (#3601, roborev job 231 B6).
#
# Every one of these is an UNCONDITIONAL assignment, not the file's `${VAR:-default}` knob form, and
# that is the point: a `${VAR:-…}` here would be settable from the environment, and CLAUDE.md's #3312
# ruling is that a test-only seam is one more thing a real invoker can set — while this suite's own
# doctrine (`test_worker_supervisor.sh`, the inherited-state drives) says a knob added so tests can go
# fast ends up testing the knob instead of the code. An earlier cut of this change DID expose
# `SUPERVISOR_LOCK_PID_TRIES`/`_WAIT` as knobs for exactly that reason; they are constants now and the
# cases drive the shipped values.
#
# The re-read window is sized for PRODUCTION, not for the tests: the state it waits out is ONE rename
# wide (microseconds), so 1s is four orders of magnitude of margin and survives a heavily loaded box,
# and it is paid ONLY on a start that finds a peer's lock pid-less — never on an ordinary start.
# 20 x 0.05s = 1s.
SUPERVISOR_LOCK_PID_TRIES=20
SUPERVISOR_LOCK_PID_WAIT=0.05
# How far into the pid file the NUL scan looks. Any legitimate pid file is a handful of bytes, so this is
# four orders of magnitude of headroom; beyond it the scan reports `could-not-measure`, never `nul-free`.
SUPERVISOR_PID_NUL_SCAN=4096
# supervisor_pid_space_ceiling — echo EXACTLY one of
#   authoritative <inclusive-max>   the largest pid this platform can ISSUE, from its own metadata
#   unknown <cause>                 this platform publishes no bound we can read
#
# WHY A VALUE BOUND REPLACED A DIGIT-COUNT BOUND (#3601, roborev job 231 B3). The digit count was 10 and
# every real pid space is at most 7 digits, so a 10-digit corruption was ACCEPTED, cast to `pid_t` by
# `kill`, reliably reported ESRCH, became an affirmative `dead` and RECLAIMED THE LOCK — while a 15-digit
# corruption of the same kind refused. One defect class, two widths, opposite outcomes, and the accepting
# width is the direction #3601 exists to close.
#
# THE VALUE IS EXCLUSIVE AND IS CONVERTED (#3601, roborev job 231 B10). `proc(5)` on
# `/proc/sys/kernel/pid_max`: "This file specifies the value at which PIDs wrap around (i.e., the value
# in this file is one greater than the maximum PID)." So `pid_max` itself is NOT an issuable pid, and
# accepting a holder pid equal to it was an off-by-one that let exactly one malformed value through. The
# inclusive maximum is `pid_max - 1`, and that is what this echoes. A suite case pinned the pre-fix
# boundary as correct and was fixed with the code, not around it.
#
# AND THERE IS NO CROSS-PLATFORM FALLBACK CONSTANT ANY MORE, WHICH IS THE POINT OF THE THIRD VALUE. An
# earlier cut of this change substituted Linux's own `PID_MAX_LIMIT` (4194304) wherever `/proc` was
# absent. On macOS, whose real ceiling is 99999, that accepted values up to 42x the platform limit — so
# malformed pids passed there — and, worse, it was a GUESS ABOUT A PLATFORM WE HAD NOT MEASURED
# presented as a bound, which is the no-heuristics violation this repository forbids outright
# (CLAUDE.md #28: authoritative metadata only, never inference). An unmeasurable ceiling is not licence
# to accept anything, and it is equally not licence to invent a number.
#
# SO WHAT DOES `unknown` DO? It makes the platform check NOT APPLY, and — this is the half that matters —
# the parser then does not CLAIM it applied one. It cannot mean "refuse every pid": that would wedge
# every non-Linux box permanently, which is the lead's ruling 2 (a guard that never permits work is
# broken, not fail-closed). It is sound for this specific check, and only because of what the check IS:
# a REJECTION-ONLY filter. It can turn `accept` into `refuse` and never the reverse, so its absence
# cannot cause a wrong reclaim that was not already possible — it only fails to tighten. That reasoning
# does NOT generalise to the liveness or NUL probes, whose verdicts SELECT the reclaim; theirs is
# `cannot tell => refuse`. The residual with no platform bound is stated at the call site.
#
# THIS IS NOT THE DELETED `pid_max` READ COMING BACK. #3549 removed a `/proc/sys/kernel/pid_max` read
# from the LEGACY guard's classifier because that classifier's verdict could not change the decision.
# Here it does. The suite asserts the deletion as a PROPERTY over the legacy functions rather than as a
# name ban over the file, for that reason.
supervisor_pid_space_ceiling() {
  local b=''
  if [[ ! -r /proc/sys/kernel/pid_max ]]; then
    printf '%s' 'unknown no-platform-pid-bound-published'
    return 0
  fi
  { IFS= read -r b; } 2>/dev/null </proc/sys/kernel/pid_max || b=''
  # Validated the way a holder pid is: enumerated digits (collation-free, no `[0-9]` range), and short
  # enough that the arithmetic below cannot overflow a 64-bit shell integer.
  case "$b" in
    '' | *[!0123456789]*)
      printf '%s' 'unknown platform-pid-bound-unparseable'
      return 0
      ;;
  esac
  if [[ "${#b}" -gt 18 ]]; then
    printf '%s' 'unknown platform-pid-bound-out-of-arithmetic-range'
    return 0
  fi
  # A wrap point below 2 cannot issue a usable pid at all, so it is not a bound we can convert; saying so
  # beats echoing `authoritative 0`, which would refuse every pid on this box.
  if [[ "$b" -lt 2 ]]; then
    printf '%s' 'unknown platform-pid-bound-implausible'
    return 0
  fi
  printf 'authoritative %s' "$((b - 1))"
}

# supervisor_lock_pid_nul_free <file> — echo EXACTLY one of
#   nul-free | contains-nul | could-not-measure <cause>
#
# WHY THIS EXISTS AT ALL (#3601, roborev job 231). A NUL byte is invisible to every check that runs on a
# shell VARIABLE: bash cannot hold one, and `read` discards it silently. MEASURED on bash 5.2.21 — a pid
# file whose bytes are `4242 NUL LF` reads back as the string `4242`, length 4: non-empty, single line,
# all decimal digits, non-zero. It therefore passed every gate in `supervisor_lock_pid_read`, the
# liveness probe said `dead`, and the lock was RECLAIMED — precisely the AC1 defect this parser exists to
# close, from precisely the input AC1 is about, since a crash mid-write can leave allocated-but-zeroed
# bytes. So the check must observe the FILE'S BYTES, not the string the shell was able to hold.
#
# BASH'S OWN WARNING IS NOT A USABLE SIGNAL, recorded because it looks like one. `$(cat …)` on such a
# file does emit `warning: … ignored null byte in input` — on STDERR, which the `2>/dev/null` in use at
# every one of these sites already suppresses; and keying correctness on bash's message TEXT would be
# the same defect class as a gate parser keyed on cargo's literal status words (CLAUDE.md #3400).
#
# THE PRIMARY DETECTOR IS A BUILTIN, AND THAT CHOICE IS LOAD-BEARING, NOT STYLISTIC. `read -d ''` sets
# the delimiter to NUL, so read STOPS AT the first NUL byte and its stop condition is a direct
# observation of the raw bytes — no fork, no `PATH` dependency, and nothing for the shell to have
# discarded first. The obvious alternative, comparing `wc -c` against `tr -d '\000' | wc -c`, was BUILT
# AND MEASURED AND REJECTED AS PRIMARY: it makes this parser — which `supervisor_lock_publish` calls on
# EVERY start, for the read-back — depend on two external commands, and with `wc` unavailable a
# supervisor could not start AT ALL, on a FRESH uncontended lock, refusing forever with a diagnostic
# about a read-back that never happened. Turning a missing coreutils tool into "this lane can never
# start" is the permanent-refusal harm this whole change exists to remove (#3549 lead ruling 2).
#
# IT SURVIVES AS THE FALLBACK, chosen by CAPABILITY rather than by guess: a `read` that rejects `-d` (or
# `-n`) exits >1, which is distinguishable from both "found a NUL" (0) and "reached EOF" (1), and only
# then is the byte-count form used. This is deliberately a capability probe against the real file and
# not a version test. Two implementations of one predicate is a cost — CLAUDE.md's ruling is that a
# second implementation's correctness is only knowable by DIFFERENTIAL TESTING — so both paths are
# driven over the same NUL / clean / unmeasurable inputs by the suite, with the fallback forced through
# a shipped-derived override rather than assumed unreachable.
#
# A FAILED MEASUREMENT IS NOT "NO NULS FOUND", in either path — the third value, for the same reason
# `log_size` grew one in this change. In the fallback the two counts are validated as DIGIT STRINGS and
# compared as STRINGS, never with `-eq`, because `-eq` reads an empty operand as 0 and two failed
# measurements would then compare EQUAL and report `nul-free`.
#
# Both paths open the file by REDIRECTION, never as an argument, so an option-shaped path (#3601 AC7)
# needs no `--` and no quoting here.
supervisor_lock_pid_nul_free() {
  local f="$1" probe='' rrc=0 opened=0
  # `opened=1` is the FIRST command inside the group, so a FAILED REDIRECTION — which never executes the
  # group's body — is distinguishable from a `read` that ran and reported EOF. Both return 1, and
  # folding "could not open" onto "no NUL found" would be the permissive collapse this function is for.
  # `2>/dev/null` precedes `<"$f"`: redirections are applied LEFT TO RIGHT, so a failed open is only
  # silent if stderr is already redirected when it is attempted.
  { opened=1; IFS= read -r -d '' -n "$SUPERVISOR_PID_NUL_SCAN" probe; } 2>/dev/null <"$f" || rrc=$?
  if [[ "$opened" -eq 0 ]]; then
    printf '%s' 'could-not-measure pid-file-unreadable-by-the-nul-scan'
    return 0
  fi
  if [[ "$rrc" -eq 0 ]]; then
    # read returned SUCCESS, which means it stopped for one of two reasons and they are told apart by
    # how much it consumed: fewer characters than the scan bound means it hit the NUL DELIMITER; exactly
    # the bound means it hit the character LIMIT, and a NUL beyond that point is unobserved — which is
    # `could-not-measure`, never `nul-free`.
    if [[ "${#probe}" -lt "$SUPERVISOR_PID_NUL_SCAN" ]]; then
      printf '%s' 'contains-nul'
    else
      printf '%s' 'could-not-measure pid-file-longer-than-the-nul-scan-bound'
    fi
    return 0
  fi
  if [[ "$rrc" -eq 1 ]]; then
    # EOF with no NUL delimiter anywhere in the file: AFFIRMATIVELY NUL-free. This is the only branch
    # that permits acceptance, and it depends on `-d ''` alone — never on `-n`, whose only job is to
    # bound memory, so an ignored `-n` cannot manufacture a false `nul-free`.
    printf '%s' 'nul-free'
    return 0
  fi
  # rrc > 1: `read` refused the options, so THIS SHELL cannot run the builtin probe. Fall back.
  supervisor_lock_pid_nul_free_bytes "$f"
}

# supervisor_lock_pid_nul_free_bytes <file> — the FALLBACK, same three values, for a shell whose `read`
# does not support `-d`/`-n`. Compares the file's byte count against its NUL-stripped byte count; `wc -c`
# and `tr -d '\000'` are both POSIX (no GNU-only flag, no `grep -P`, no `od` parsing, no bash 4).
#
# TWO OPENS, so a file rewritten between them yields mismatched counts and is reported `contains-nul` —
# a REFUSAL. The ambiguity costs a start and can never cost a live holder its lock, which is the
# direction every branch here is biased in. `wc`'s leading whitespace differs across platforms, so both
# counts are whitespace-stripped before they are compared.
supervisor_lock_pid_nul_free_bytes() {
  local f="$1" raw='' stripped=''
  raw="$( { wc -c <"$f"; } 2>/dev/null | tr -d '[:space:]')" || raw=''
  stripped="$( { tr -d '\000' <"$f"; } 2>/dev/null | wc -c | tr -d '[:space:]')" || stripped=''
  case "$raw" in
    '' | *[!0123456789]*)
      printf '%s' 'could-not-measure raw-byte-count-unmeasurable'
      return 0
      ;;
  esac
  case "$stripped" in
    '' | *[!0123456789]*)
      printf '%s' 'could-not-measure nul-stripped-byte-count-unmeasurable'
      return 0
      ;;
  esac
  if [[ "$raw" == "$stripped" ]]; then
    printf '%s' 'nul-free'
  else
    printf '%s' 'contains-nul'
  fi
  return 0
}

# supervisor_lock_pid_read <pid-file> — echo EXACTLY one of
#   pid <digits>          a single, canonical, non-zero decimal pid
#   unparseable <cause>   nothing usable is there, for a NAMED reason
#
# Every failure is a NAMED cause and never a bare empty string, because the caller must be able to tell
# "no pid" from "pid 0" from "unreadable" — collapsing them is the original defect.
#
# THE STRUCTURAL PARSE USES NO EXTERNAL COMMAND — `read` and `[[ ]]` only, so none of those verdicts
# depends on `PATH` — and the file is opened by REDIRECTION, which parses no options, so an
# option-shaped path (#3601 AC7) needs no `--` here. The digit test enumerates the ten digits rather
# than using a `[0-9]` RANGE, whose members are decided by the caller's collation (a locale-dependent
# digit test cost #3549 a review round).
#
# THE NUL CHECK IS ALSO BUILTIN-ONLY ON THE PATH THAT RUNS (`supervisor_lock_pid_nul_free`, whose
# primary detector is `read -d ''`), so the whole parser forks nothing and no verdict depends on `PATH`.
# Its `wc`/`tr` FALLBACK is reached only by a shell that rejects `read -d ''`, and even then the probe
# is THREE-valued: an unavailable tool makes this parser REFUSE with a named cause rather than accept an
# unverified pid. The NUL check runs LAST, after the cheap structural gates, because by then the content
# is known to be a short run of decimal digits and a NUL is the one remaining thing that could make
# those digits a different number than the file records.
supervisor_lock_pid_read() {
  local f="$1" first='' line='' n=0 readrc=0
  if [[ ! -e "$f" && ! -L "$f" ]]; then
    printf '%s' 'unparseable pid-file-absent'
    return 0
  fi
  if [[ ! -f "$f" ]]; then
    printf '%s' 'unparseable pid-name-is-not-a-regular-file'
    return 0
  fi
  # ONE redirection over the whole loop, so both lines come from ONE open. A failed open makes the
  # group return non-zero, which is CAPTURED (never folded into "empty"): unreadable and empty are
  # different facts with different remedies. `|| [[ -n "$line" ]]` keeps a final line that has no
  # trailing newline, which `read` reports as failure while still assigning it.
  { while IFS= read -r line || [[ -n "$line" ]]; do
      n=$((n + 1))
      if [[ "$n" -eq 1 ]]; then first="$line"; fi
      if [[ "$n" -ge 2 ]]; then break; fi
      line=''
    done; } 2>/dev/null <"$f" || readrc=$?
  if [[ "$readrc" -ne 0 ]]; then
    printf '%s' 'unparseable pid-file-unreadable'
    return 0
  fi
  if [[ "$n" -eq 0 ]]; then
    printf '%s' 'unparseable pid-file-empty'
    return 0
  fi
  if [[ "$n" -gt 1 ]]; then
    printf '%s' 'unparseable pid-file-has-more-than-one-line'
    return 0
  fi
  case "$first" in
    '' | *[!0123456789]*)
      printf '%s' 'unparseable pid-not-all-decimal-digits'
      return 0
      ;;
    # `0*` is BOTH halves of "non-zero and canonical" in one pattern: it rejects `0` itself (a pid 0
    # target means the whole process GROUP to `kill`, which is the most dangerous operand available
    # here) and rejects a leading-zero spelling, which no legitimate writer of `$$` produces.
    0*)
      printf '%s' 'unparseable pid-zero-or-leading-zero'
      return 0
      ;;
  esac
  # TWO BOUNDS, IN THIS ORDER, AND THE ORDER IS REQUIRED. The digit count is checked FIRST purely so the
  # value comparison below cannot overflow a 64-bit shell integer (19 digits can; 18 cannot). It is not
  # the correctness bound and is deliberately far too loose to be one.
  if [[ "${#first}" -gt 18 ]]; then
    printf '%s' 'unparseable pid-digit-count-out-of-well-formedness-bound'
    return 0
  fi
  # ...and then the bound that actually decides: a pid this platform CANNOT ISSUE is malformed content,
  # not an unusual pid (#3601, roborev job 231 B3). The previous 10-digit rule accepted a corruption that
  # `kill` then reported ESRCH for, which became an affirmative `dead` and RECLAIMED the lock — while a
  # wider corruption of the same kind refused.
  # THE PLATFORM BOUND, WHEN THE PLATFORM PUBLISHES ONE — and nothing pretending to be one when it does
  # not (#3601, roborev job 231 B10). `unknown` leaves this gate UNAPPLIED, so on a platform with no
  # published pid space the parser is exactly as strong here as it was before the bound existed: the
  # structural gates and the 18-digit arithmetic guard above still reject, and a plausible-looking
  # corruption within them is still accepted, probed, and reclaimed if the kernel says no such process.
  # That residual is REAL and is stated rather than papered over with a guessed constant.
  local pidceil=''
  pidceil="$(supervisor_pid_space_ceiling)" || pidceil='unknown ceiling-probe-aborted'
  case "$pidceil" in
    'authoritative '*)
      if [[ "$first" -gt "${pidceil#authoritative }" ]]; then
        printf '%s' 'unparseable pid-above-the-platform-pid-space'
        return 0
      fi
      ;;
  esac
  # LAST GATE BEFORE ACCEPTANCE: the FILE'S BYTES, not the string the shell was able to hold (#3601,
  # roborev job 231). Everything above ran on a value `read` had already stripped NULs out of, so a
  # `<digits> NUL LF` file satisfied all of it. It is deliberately last: it is the only gate that forks,
  # and by here the content is known to be a short run of decimal digits, so a NUL is the one remaining
  # thing that could make those digits a different number than the file records.
  #
  # A file that carries BOTH a NUL and other junk is refused ABOVE, under whichever gate it trips
  # first, and that cause is also true of it — the dangerous shape, the one that reaches this line, is
  # the file whose NUL-stripped content is a clean plausible pid.
  local nul=''
  nul="$(supervisor_lock_pid_nul_free "$f")" || nul='could-not-measure nul-probe-aborted'
  case "$nul" in
    nul-free) ;;
    contains-nul)
      printf '%s' 'unparseable pid-file-contains-nul'
      return 0
      ;;
    *)
      printf '%s' "unparseable pid-file-nul-check-${nul}"
      return 0
      ;;
  esac
  printf 'pid %s' "$first"
}

# supervisor_lock_holder_liveness <pid> — echo EXACTLY one of
#   live | dead | unknown <cause>
#
# `dead` REQUIRES AFFIRMATIVE EVIDENCE OF ABSENCE. `kill -0` returning non-zero is not that evidence: it
# fails with EPERM (the process EXISTS and we may not signal it — another user's, or one this uid cannot
# reach) exactly as it fails with ESRCH, and reading the first as the second is what reclaimed live
# holders' locks.
#
# THE TWO WITNESSES, AND THE DIRECTION EACH IS USED IN:
#   * procfs — `/proc/<pid>` existing is a LOCALE-FREE witness that the process EXISTS. It is used in
#     that ONE direction. ABSENCE of `/proc/<pid>` is NOT absence of the process (`hidepid=2` hides
#     another user's pid entirely), so it never yields `dead`. `/proc/self` is checked first so that a
#     host with no procfs at all (macOS) is distinguished from a host whose procfs answered "no".
#   * the errno text — bash renders `kill`'s errno, so the two cases are distinguishable by message.
#     It is read under a `C` locale so the patterns are deterministic; a message matching NEITHER
#     pattern yields `unknown`, never `dead`.
#
# The locale is set by ASSIGNMENT INSIDE the subshell, not as a prefix to the builtin: a temporary
# assignment to a regular builtin is not guaranteed to reach `setlocale`, and this probe's correctness
# would then depend on the caller's environment.
supervisor_lock_holder_liveness() {
  local pid="$1" msg='' rc=0
  msg="$( LC_ALL=C; LC_MESSAGES=C; export LC_ALL LC_MESSAGES; kill -0 "$pid" 2>&1 )" || rc=$?
  if [[ "$rc" -eq 0 ]]; then
    printf '%s' 'live'
    return 0
  fi
  if [[ -d /proc/self && -d "/proc/$pid" ]]; then
    printf '%s' 'live'
    return 0
  fi
  case "$msg" in
    *'No such process'*)
      printf '%s' 'dead'
      return 0
      ;;
    *'Operation not permitted'*)
      printf '%s' 'live'
      return 0
      ;;
  esac
  printf '%s' 'unknown kill-0-verdict-unrecognised'
}

# Where `supervisor_lock_take` reports WHICH step failed. A plain variable rather than an echoed value
# because `take` installs the EXIT trap, and a function whose output is captured runs in a subshell where
# a trap would be discarded. Assigned before any call can read it, so `set -u` is satisfied.
SUPERVISOR_LOCK_TAKE_CAUSE=''

# supervisor_lock_publish — the `mkdir` has already succeeded, so the NAME is ours. Publish our pid and
# then VERIFY WE OWN THE LOCK by reading it back. Returns 0 only on verified ownership.
#
# THIS IS WHERE THE TUPLE IS MADE UNNECESSARY (#3549 lead ruling 5). The question "do we hold this
# lock?" was previously answered by inference from three separate values (a parsed pid, its liveness,
# its identity), and pinning them one at a time is what cost that issue four rounds. It is answered here
# by ONE observation: the lock's `pid` file reads back as OUR pid.
#
# THE PUBLISH IS A RENAME, WHICH IS WHAT CLOSES THE PARTIAL-PID HALF OF AC2. `echo $$ >…/pid` creates
# the `pid` NAME and then writes it, so a peer can observe the name holding nothing. A rename makes the
# name appear only with its complete content: a reader sees the name absent, or sees the whole value.
# The residual window is therefore "the `pid` name is not there yet", one rename wide, and the caller
# resolves that by measuring persistence rather than by assuming death.
#
# WHAT THE READ-BACK DOES AND DOES NOT PROVE. It proves the publish landed and was not clobbered by a
# racer between the `mkdir` and this read — the case that matters, because a peer running PRE-#3601 code
# reclaims a pid-less lock and would rename ours aside mid-startup. It is a point-in-time verification
# and does not prove nobody steals the lock later; `supervisor_lock_release` re-checks ownership at exit
# for exactly that reason.
#
# IT ECHOES WHICH STEP FAILED, AND THAT IS NOT COSMETIC (#3601, roborev job 231 B1). A single "publish
# failed" collapsed three different facts — the write never happened, the rename never happened, the
# lock is not ours — onto one caller verdict, and the refusal built from it then ASSERTED all three
# steps had run: it told the operator "our pid was published into it, and reading it back did not return
# our pid" for an ENOSPC that never wrote a byte. On a fleet that hits ENOSPC routinely that is a
# diagnostic pointing at a race that did not occur. Distinguishing them also decides whether the
# directory we just created is ours to remove again, which is the difference between recovering and
# manufacturing the undecidable state ourselves.
supervisor_lock_publish() {
  local tmpf="$SUPERVISOR_LOCK/pid.tmp.$$" state=''
  # THE GROUP FORM IS REQUIRED, NOT TIDINESS: a redirection that fails is reported by the SHELL, and
  # `2>/dev/null` attached to the command does not suppress that — measured, it printed the raw
  # `TMPDIR`-derived path to stderr, which is both noise and the unrendered-path class (#3549 job 201
  # F1). Redirecting the group's stderr FIRST covers the redirection setup itself.
  if ! { printf '%s\n' "$$" >"$tmpf"; } 2>/dev/null; then
    printf '%s' 'write-failed'
    return 1
  fi
  # DECLINE RATHER THAN OVERWRITE (#3601, roborev job 231 F3/B11; race tracked as #3683). Publication is
  # NOT bound to the directory instance this process created: a peer can rename our pid-less directory
  # away, create and publish its OWN lock at the same name, and our `mv -f` then overwrites THAT peer's
  # ownership record — destroying the evidence of who holds the lane, which is strictly worse than
  # failing to start. In the legitimate case the directory was created microseconds ago by our own
  # `mkdir`, so `pid` CANNOT exist and this branch is never taken; if it does exist the directory is not
  # the one we made, and declining costs us a start we were not entitled to anyway.
  #
  # NARROWED, NOT CLOSED — DO NOT READ THIS AS A GUARANTEE. Test-then-move is NOT equivalent to an atomic
  # create-exclusive: a peer that publishes between this test and the rename below is still overwritten.
  # The window shrinks from "the whole publish" to "between these two lines" and no further, because
  # closing it needs serialisation across the complete claim-and-publication operation — the same
  # primitive as the reclaim ABA above and the read-then-remove in `supervisor_lock_release`, all three
  # tracked as ONE follow-up (#3683). `mv -n` was considered and NOT used: it is outside POSIX, and GNU
  # `mv -n` exits 0 WITHOUT MOVING, so its success proves nothing about publication and a platform
  # lacking `-n` would fail every publish outright — a wedge in exchange for a narrower window that
  # #3683 closes properly.
  if [[ -e "$SUPERVISOR_LOCK/pid" || -L "$SUPERVISOR_LOCK/pid" ]]; then
    rm -f -- "$tmpf" 2>/dev/null || true
    state="$(supervisor_lock_pid_read "$SUPERVISOR_LOCK/pid")" || state='unparseable read-back-aborted'
    printf '%s' "declined $state"
    return 1
  fi
  # `--` because both operands are `TMPDIR`-derived and may be option-shaped (#3601 AC7); quoting stops
  # word-splitting and globbing and does nothing about option parsing.
  if ! mv -f -- "$tmpf" "$SUPERVISOR_LOCK/pid" 2>/dev/null; then
    rm -f -- "$tmpf" 2>/dev/null || true
    # A RACE AND AN I/O ERROR SHARE THIS FAILURE, AND THEY WANT OPPOSITE ACTIONS (#3601, job 244 sweep).
    # The rename fails with ENOENT when a peer has removed the directory under us — contention, where the
    # answer is "re-run" — and with EACCES/ENOSPC when the filesystem is broken, where the answer is "fix
    # the box" and re-running loops forever. They are told apart by whether the directory is still there.
    if [[ ! -d "$SUPERVISOR_LOCK" ]]; then
      printf '%s' 'rename-failed-lock-gone'
    else
      printf '%s' 'rename-failed'
    fi
    return 1
  fi
  state="$(supervisor_lock_pid_read "$SUPERVISOR_LOCK/pid")" || state='unparseable read-back-aborted'
  if [[ "$state" == "pid $$" ]]; then
    printf '%s' 'ok'
    return 0
  fi
  printf '%s' "not-owned $state"
  return 1
}

# supervisor_lock_release — the EXIT trap. Remove the lock ONLY while it is still OURS.
#
# THE PRE-#3601 TRAP WAS AN UNCONDITIONAL `rm -rf "$SUPERVISOR_LOCK"`, which is the same defect class as
# the reclaim it sat next to, pointing the other way: by the time we exit, the directory at that name
# may be a DIFFERENT holder's lock (a peer reclaimed ours, or an operator cleared it and a new
# supervisor started), and deleting it hands the lane to a third process while the second one believes
# it holds exclusion. Ownership is decided by the same one observation the publish used, so the two
# cannot disagree. A lock that is NOT ours is left exactly as found and the fact is logged — never
# silently, because "my lock vanished" is otherwise unattributable.
#
# THIS CLOSES ONE HALF OF ITS PROBLEM AND IS RECORDED AS PARTIAL, DELIBERATELY (#3601, roborev job 231).
# CLOSED: the UNCONDITIONAL DELETE. Before this change the trap was `rm -rf "$SUPERVISOR_LOCK"` with no
# ownership test at all, so a run that had lost its lock deleted the new holder's on the way out.
# NOT CLOSED: the read and the removal are two operations, so a lock that becomes someone else's between
# them is still removed. Closing that needs serialization across classify -> reclaim -> claim, which is
# the same root as the reclaim ABA race at the rename below, and it is tracked as ONE follow-up (#3683)
# than patched here: the primitive it needs is a lock protecting a lock, whose own staleness reopens
# "how does a stale instance get cleared, and by whom?" one level down. Left as an explicit partial
# because dropping the ownership test to avoid a half-fix would REGRESS to the unconditional delete.
supervisor_lock_release() {
  local state=''
  state="$(supervisor_lock_pid_read "$SUPERVISOR_LOCK/pid")" || state='unparseable read-aborted'
  if [[ "$state" == "pid $$" ]]; then
    rm -rf -- "$SUPERVISOR_LOCK" 2>/dev/null || true
    if [[ ! -e "$SUPERVISOR_LOCK" && ! -L "$SUPERVISOR_LOCK" ]]; then
      return 0
    fi
    # THE SURVIVING LOCK IS RE-READ, BECAUSE `rm -rf` CAN SUCCEED IN PART (#3601, roborev job 240 B16).
    #
    # `rm -rf` recurses: it unlinks the CONTENTS first and the directory last, and those need permission
    # on DIFFERENT directories — unlinking `<lock>/pid` needs write on `<lock>`, unlinking `<lock>` needs
    # write on its PARENT. So an unwritable parent produces a partial removal, and MEASURED it does
    # exactly that: `pid` is gone and the directory remains. The previous wording asserted the pid record
    # was still there and promised the next start would reclaim automatically; in that state the next
    # start sees a PID-LESS lock, which is undecidable, and REFUSES INDEFINITELY. So the promise was not
    # merely inaccurate, it pointed away from a wedge — and our own read-only-parent suite case reaches
    # this state, which is how it was found.
    #
    # THIS REPORTS THE WEDGE; IT DOES NOT PREVENT IT, and that distinction is the honest one. The
    # alternative fix — rename the directory aside before deleting it, so the NAME is freed atomically —
    # CANNOT prevent this wedge, and that is measured rather than argued: `mv <lock> <lock>.aside` needs
    # write on the same PARENT that just refused the unlink, and fails with the same EACCES. Restoring the
    # pid record instead would need a test-then-write against a directory that may by then be a peer's,
    # i.e. the clobber hazard B11/B12 exist to remove, on the one path that runs at exit with no ability
    # to report the outcome. So this path stays DIAGNOSIS ONLY: no new operation, no new window, and no
    # new primitive — serialising the whole release is #3683.
    #
    # WHAT IT BUYS, precisely: the state was already reachable before this change (the pre-#3601 trap was
    # the same unconditional `rm -rf`, silently), and it was undiagnosed. It is now named at the moment it
    # happens, with the cause an operator must fix — the parent's permissions — because the remedy printed
    # by a later start's refusal (`rmdir`) needs that same permission and fails without it.
    local after=''
    after="$(supervisor_lock_pid_read "$SUPERVISOR_LOCK/pid")" || after='unparseable post-removal-probe-aborted'
    case "$after" in
      "pid $$")
        log "$(supervisor_one_line "exit: FAILED to remove our own lock $(supervisor_shell_quote "$SUPERVISOR_LOCK") — it still exists and STILL records this process's pid ($$), verified after the attempt. The next start will find that holder affirmatively dead and reclaim it automatically; no action is needed unless starts keep refusing (#3601).")"
        ;;
      'pid '*)
        log "$(supervisor_one_line "exit: did NOT remove $(supervisor_shell_quote "$SUPERVISOR_LOCK") — it still exists and now records holder pid ${after#pid }, which is not this process ($$). Something took that name during our exit; its record is intact and this run did not overwrite it. The next start will read that holder and report it by pid (#3601).")"
        ;;
      *)
        log "$(supervisor_one_line "exit: PARTIALLY removed our own lock $(supervisor_shell_quote "$SUPERVISOR_LOCK") — the pid record is GONE and the directory REMAINS (its pid file now reads [$after]). This will NOT clear itself: a pid-less lock is undecidable, so the next start REFUSES over it rather than reclaiming it. ACTION IS NEEDED. \`rm -rf\` removes a directory's contents before the directory itself, and those need write permission on DIFFERENT directories, so the usual cause is that this lock's PARENT directory is not writable by this user — check that first, because the removal a refusing start prints needs the same permission and will fail without it (#3601).")"
        ;;
    esac
    return 0
  fi
  # NOT OURS — AND "NOT OURS" IS NOT THE SAME FACT AS "SOMEONE ELSE'S" (#3601, roborev job 240 B17).
  # Every state other than our own pid used to be reported as "Something else owns that name now",
  # including absent, unreadable, malformed and NUL-bearing records. Those establish only that ownership
  # CANNOT BE VERIFIED; attributing them to another process names a holder that may not exist, and sends
  # an operator looking for it. The decision is the same either way — we remove nothing — so only the
  # wording differs, which is exactly why it has to be the wording that is true.
  #
  # Rendered, not interpolated raw (#3549 job 201 F1 class, at a new site — #3601 roborev job 231 B7):
  # the path comes from the environment, and a newline or an ESC in it would split this line or forge an
  # unprefixed one. `supervisor_shell_quote` renders the path as a paste-safe value; `supervisor_one_line`
  # guarantees the composed message is ONE physical line whatever it interpolated.
  case "$state" in
    'pid '*)
      log "$(supervisor_one_line "exit: NOT removing $(supervisor_shell_quote "$SUPERVISOR_LOCK") — it records holder pid ${state#pid }, not this process ($$). Another holder owns that name, so removing it would break a holder that is not us (#3601).")"
      ;;
    *)
      log "$(supervisor_one_line "exit: NOT removing $(supervisor_shell_quote "$SUPERVISOR_LOCK") — this run could not VERIFY that it owns it: its pid file reads [$state], which is an absent, unreadable or malformed record. That does NOT establish that another process holds this lock, and it is not a reason to delete one either — an unverifiable record is left exactly as found (#3601).")"
      ;;
  esac
  return 0
}

# supervisor_lock_refuse <headline> <detail> <remedy-prose> [<remedy-command>] — the per-lane lock's
# refusal channel, and the counterpart of ruling 2: NO REFUSAL LEAVES WITHOUT A REMEDY.
#
# `printf`, never `echo`: `echo` inherits `xpg_echo` and would interpret a backslash sequence in a
# `TMPDIR`-derived path (this was the FOURTH SHAPE recorded as a residual at this call site under #3549;
# it is closed for these lines by this emitter). Prose is rendered through `supervisor_one_line` so no
# interpolated path can split a line or forge an unprefixed one, and a printed COMMAND is built by the
# caller through `supervisor_shell_quote` so it is paste-safe and stays on one physical line — the same
# contract the legacy guard's emitter holds, for the same reasons, stated at length there.
supervisor_lock_refuse() {
  local headline="$1" detail="$2" remedy="$3" remedy_cmd="${4:-}" shown=''
  headline="$(supervisor_one_line "$headline")" || true
  detail="$(supervisor_one_line "$detail")" || true
  remedy="$(supervisor_one_line "$remedy")" || true
  shown="$(supervisor_shell_quote "$SUPERVISOR_LOCK")" || true
  printf '%s\n' "worker-supervisor: $headline (lock $shown)" >&2
  printf '%s\n' "worker-supervisor: $detail" >&2
  printf '%s\n' "worker-supervisor: remedy — $remedy" >&2
  # A command that is not one physical line is DEMOTED TO PROSE rather than printed bare, so the
  # one-bare-line-is-the-command contract holds by the EMITTER's rules and not by a caller's discipline.
  case "$remedy_cmd" in
    '') ;;
    *"$SUPERVISOR_LF"* | *"$SUPERVISOR_CR"* | *"$SUPERVISOR_ESC"*)
      printf '%s\n' "worker-supervisor: a remedy command for this state carries an embedded control character, so it is NOT printed as a runnable line; rendered for reading only: $(supervisor_one_line "$remedy_cmd")" >&2
      ;;
    *)
      printf '%s\n' "$remedy_cmd" >&2
      ;;
  esac
  exit 1
}

# supervisor_lock_take — claim the lock NAME and prove we own it. Returns
#   0 = the lock is ours, published and read back, EXIT trap installed
#   1 = the name was not free (or could not be created) — nothing was written
#   2 = the name became ours but the published pid did NOT read back as ours
#
# Return 2 is a genuine state, not paranoia: a peer running PRE-#3601 code reclaims a pid-less lock, so
# it can rename ours aside inside our own publish window. Distinguishing it from 1 matters because the
# two have different remedies (1 = someone holds it; 2 = someone took it FROM us mid-startup).
#
# The trap is installed only AFTER ownership is verified. A crash in the window before that leaves an
# ordinary reclaimable stale lock (case 2 of the clearing story above), which is the benign residue.
supervisor_lock_take() {
  SUPERVISOR_LOCK_TAKE_CAUSE=''
  # `--` because the operand is `TMPDIR`-derived and may be option-shaped (#3601 AC7).
  mkdir -- "$SUPERVISOR_LOCK" 2>/dev/null || return 1
  local marker="$SUPERVISOR_LOCK/own.$$" pub=''
  # THE OWNERSHIP MARKER, WRITTEN BEFORE ANYTHING ELSE (#3601, roborev job 236 B12).
  #
  # WHY IT EXISTS. The un-create below used to be an UNCONDITIONAL `rmdir`, which is not bound to the
  # directory this process created: a legacy peer can rename ours aside and `mkdir` its OWN pid-less lock
  # at the same name, and a non-recursive `rmdir` then SUCCEEDS against the peer's empty startup
  # directory — deleting a live peer's lock and freeing the name for a third claimant. That is a
  # regression the previous round introduced, not a pre-existing defect: it traded "we wedge our own lane
  # with an empty lock" for "we can delete a peer's startup lock", which is the worse of the two. The
  # marker makes the removal conditional on the directory still being the instance we made.
  #
  # WHY A STALE MARKER CANNOT ALIAS US. This path is reached only when OUR `mkdir` succeeded, which means
  # the directory did not exist a moment ago — so it cannot already contain an `own.<pid>` left by a dead
  # process that happened to have our pid. Pid reuse therefore cannot forge this token for this decision.
  #
  # NARROWED, NOT CLOSED — AND HERE IS THE BOUND, BECAUSE THE MARKER IS NOT AIRTIGHT. Creating it is a
  # SECOND step after `mkdir`, so there is a window in which the directory exists without it. A peer that
  # replaces the directory inside THAT window receives our marker in its own directory, and the un-create
  # would then remove the peer's directory exactly as before. What makes this worth doing anyway is the
  # window's SIZE: it is two adjacent syscalls with no intervening I/O, where the window it replaces
  # spanned the whole publish attempt (a write, a decline test and a rename). Ordering the marker before
  # the publish is what makes it the narrowest window available to this code, and nothing here can make it
  # zero — that needs serialisation of the complete claim-and-publication operation, which is #3683,
  # together with the reclaim ABA and the release read-then-remove. Do not read the marker as a guarantee.
  # SAME PREDICATE AS THE RENAME BELOW (#3601, job 244 sweep): this write fails with ENOENT when a peer
  # has already removed the directory we just created — contention — and with EACCES/ENOSPC when the
  # filesystem cannot take a zero-byte file. Reporting the first as the second sends an operator to check
  # a disk that is fine.
  # ONE OUTCOME VARIABLE, SO EACH BRANCH HAS A LINE OF ITS OWN. An earlier cut folded the write and the
  # existence re-test into two `if`s whose conditions were indistinguishable from the un-create's, which
  # made a mutant unable to name either uniquely — `sv_mutant_override` correctly refused it.
  local marker_written=0
  if { : >"$marker"; } 2>/dev/null; then marker_written=1; fi
  if [[ "$marker_written" -eq 0 ]]; then
    # NO OWNERSHIP EVIDENCE, SO NO REMOVAL. We know we created the directory and we cannot PROVE the one
    # at that name now is still ours, so it is left in place and reported. That risks the empty-lock wedge
    # B1 removed — but the cause is a filesystem that cannot take a zero-byte file, the same failure the
    # remedy names, and guessing our way to a deletion is how a peer's lock gets destroyed.
    if [[ ! -d "$SUPERVISOR_LOCK" ]]; then
      SUPERVISOR_LOCK_TAKE_CAUSE='marker-failed-lock-gone cleanup-declined-lock-already-gone'
    else
      SUPERVISOR_LOCK_TAKE_CAUSE='marker-failed cleanup-declined-no-ownership-evidence'
    fi
    return 3
  fi
  # `supervisor_lock_publish` runs in a command substitution, which is a SUBSHELL — fine, because it
  # installs no trap. The `trap` below must NOT be inside one, which is why the cause travels back
  # through a variable instead of this function echoing it.
  pub="$(supervisor_lock_publish)" || true
  if [[ "$pub" == 'ok' ]]; then
    # The marker's job is done the moment ownership is verified, and it must not outlive it: a held lock
    # carrying an extra file makes the non-recursive clear command an operator is handed elsewhere refuse.
    if ! rm -f -- "$marker" 2>/dev/null; then
      log "$(supervisor_one_line "startup: could not remove our own ownership marker $(supervisor_shell_quote "$marker") after acquiring the lock. Harmless to this run — the lock is held and its release removes the directory wholesale — but a non-recursive manual clear of that lock would refuse until the marker is gone (#3601).")"
    fi
    # THE CLAIM IS RELEASED BEFORE THE LOCK, AND IT IS REGISTERED HERE RATHER THAN AT ITS
    # CREATE SITE (#3749 review round 5, item 2; CLAUDE.md's roborev-job-282 ruling that a
    # fix which ADDS a resource inherits that resource's lifetime bugs). The trap is
    # installed before any sweep can run, so the handler exists before the resource does,
    # and the resource's identity travels in OBJ_SWEEP_CLAIM_OWNED — empty until a claim is
    # created, cleared the moment it is released, so this is a no-op on every other path.
    # KNOWN AND NOT CLOSED HERE: this file installs no INT/TERM handlers, so a SIGNALLED
    # supervisor releases neither its claim nor its lock. For the claim that is a delay and
    # not a wedge — a peer recovers it once it ages past the staleness bound — and adding
    # signal handlers to this process is a change to the LOCK's lifetime too, which is
    # #3683's subject and not this one's.
    trap 'obj_sweep_claim_release "$OBJ_SWEEP_CLAIM_OWNED"; supervisor_lock_release' EXIT
    return 0
  fi
  SUPERVISOR_LOCK_TAKE_CAUSE="$pub"
  # `declined` joins `not-owned` (#3601 B11): both mean the directory at that name is not ours, so both
  # LEAVE IT ALONE. The difference is only whether we wrote into it, and the refusal says which. Our own
  # marker is removed either way — it is unambiguously ours by name, and on the `declined` path it may be
  # sitting in the PEER's directory, where leaving it would be litter in someone else's lock.
  if [[ "$pub" == 'not-owned'* || "$pub" == 'declined'* ]]; then
    rm -f -- "$marker" 2>/dev/null || true
    return 2
  fi
  # WE CREATED THE DIRECTORY AND NEVER PUBLISHED INTO IT (#3601, roborev job 231 B1). Leaving it would
  # MANUFACTURE the pid-less lock that every other branch here refuses to reclaim — this run would wedge
  # its own lane until an operator cleared it, which is the lead's ruling 2 inverted: we would have
  # created the undecidable state ourselves and then refused over it.
  #
  # SO THE REMOVAL IS BOUND TO THE INSTANCE WE CREATED, not merely non-recursive (#3601 B12). The
  # non-recursive `rmdir` protects a peer that has already PUBLISHED — the directory is then non-empty and
  # the removal fails harmlessly — and protects nothing at all against a peer that has `mkdir`'d and not
  # yet published, which is an EMPTY directory a `rmdir` succeeds against. The marker is what distinguishes
  # those: if it is gone, the directory at that name is not the one we made, and we remove NOTHING.
  if [[ ! -e "$marker" && ! -L "$marker" ]]; then
    local foreign=''
    foreign="$(supervisor_lock_pid_read "$SUPERVISOR_LOCK/pid")" || foreign='unparseable residual-probe-aborted'
    case "$foreign" in
      'pid '*) SUPERVISOR_LOCK_TAKE_CAUSE="$pub cleanup-declined-foreign-holder $foreign" ;;
      *)       SUPERVISOR_LOCK_TAKE_CAUSE="$pub cleanup-declined-foreign-instance $foreign" ;;
    esac
    return 3
  fi
  rm -f -- "$marker" 2>/dev/null || true
  rm -f -- "$SUPERVISOR_LOCK/pid.tmp.$$" 2>/dev/null || true
  rmdir -- "$SUPERVISOR_LOCK" 2>/dev/null || true
  # ...AND THE OUTCOME IS VERIFIED, NOT ASSUMED (#3601, roborev job 231 B9). Both removals above ignore
  # their exit status — deliberately, because a peer populating the directory is a legitimate reason for
  # `rmdir` to refuse — so NOTHING here knows whether the lock is gone until it looks. The refusal built
  # from this cause used to state unconditionally that the directory "has been REMOVED AGAIN": with a
  # peer's record inside, or a filesystem that refuses the removal, the lock REMAINED while the operator
  # was told there was nothing to clear. Same defect family as the refusal that claimed a read-back it
  # never made (B1) and the mutant comment that claimed an isolation it did not have (B4) — an artifact
  # asserting a step that did not run, which is worse than no artifact because it is what stops the next
  # reader looking. So the verdict carries what was OBSERVED after the attempt, and the cases are
  # distinguished because their remedies differ: nothing to do, a foreign holder to leave alone, or a
  # residual to clear.
  if [[ ! -e "$SUPERVISOR_LOCK" && ! -L "$SUPERVISOR_LOCK" ]]; then
    SUPERVISOR_LOCK_TAKE_CAUSE="$pub cleanup-verified-absent"
    return 3
  fi
  local residual=''
  residual="$(supervisor_lock_pid_read "$SUPERVISOR_LOCK/pid")" || residual='unparseable residual-probe-aborted'
  case "$residual" in
    'pid '*) SUPERVISOR_LOCK_TAKE_CAUSE="$pub cleanup-failed-foreign-holder $residual" ;;
    *)       SUPERVISOR_LOCK_TAKE_CAUSE="$pub cleanup-failed-residual $residual" ;;
  esac
  return 3
}

# supervisor_lock_refuse_unowned — the name became ours and the read-back said it is not.
#
# TWO REACHING PATHS, TWO DIFFERENT TRUTHS, AND THE TEXT MUST NOT MIX THEM UP (#3601, B9 family). The
# `declined` path never wrote anything — a `pid` file already existed, so the publish refused to
# overwrite it — while the `not-owned` path DID publish and then read back someone else's pid. Saying
# "our pid was published into it" on the declined path would assert a step that did not run, which is the
# defect class B1, B4 and the reclaim comment were all instances of.
supervisor_lock_refuse_unowned() {
  local cause="${1:-not-owned unrecorded}" detail=''
  case "$cause" in
    'declined'*)
      # AND IT DOES NOT NAME A CAUSE IT DID NOT OBSERVE (#3601, roborev job 242 B19). It used to conclude
      # "the directory at that name is therefore not the one this process created: a peer renamed ours
      # aside and published its own between our two steps". Finding a `pid` record proves only that
      # publication must be DECLINED. It does not identify how the record got there: a peer renaming ours
      # aside and publishing is one way, and something writing a `pid` file straight into the directory we
      # created — an external writer, an operator, a stray tool — is another, and this code cannot tell
      # them apart. Directory-instance identity is not verifiable on this path and per #3683 cannot be
      # made so here, so the specific race is a story, not an observation.
      #
      # WHAT THIS PATH MAY CLAIM, NARROWED TO WHAT IS TRUE (#3601, roborev job 238 B15 — the SEVENTH
      # instance of the alibi family, and it is in the text written to FIX the family's second instance).
      # It used to say the run "wrote NOTHING" and that "NOTHING at that path has been modified", and
      # both are false in detail: `supervisor_lock_publish` writes its staging file `pid.tmp.$$` BEFORE it
      # tests for an existing `pid`, and `supervisor_lock_take` may then remove an ownership marker from
      # the very directory being described. The load-bearing claim — the only one an operator needs and
      # the only one this path establishes — is that THE EXISTING HOLDER RECORD WAS NOT OVERWRITTEN OR
      # PUBLISHED OVER. That is what it says now, and the test asserts PRESERVATION OF THAT RECORD rather
      # than an absence of modification the code never provided.
      detail="the lock directory was created by this process and a holder record then APPEARED in the lock before this run could publish its own, so it declined to publish over it; that record reads [${cause#declined }] and is INTACT — it was neither overwritten nor replaced. HOW that record got there is NOT established by this run: another supervisor may have taken the name, or something may have written a pid file into the directory directly, and nothing here can tell those apart. This run did write and then remove its own scratch entries in that directory (a staging file, and an ownership marker), so it is not true that nothing there was touched — what is true, and what matters, is that the holder record itself was left exactly as found"
      ;;
    *)
      # This path DID write: `pid` at that name currently holds OUR pid, over whatever was there. Saying
      # "nothing has been modified" here would be false, and it was — for about an hour, until the B9
      # class sweep read it back (#3601, roborev job 231 B9).
      detail="the lock directory was created by this process and our pid WAS published into it; reading it back did not return our pid ($$) — the read-back said [${cause#not-owned }] — so between those two steps something else took the lock name, and our published pid may have overwritten that holder's record (the race #3683 closes)"
      ;;
  esac
  supervisor_lock_refuse \
    "refusing to start — this run CREATED the lock and then could not verify it OWNS it" \
    "$detail. The most likely cause is a supervisor running PRE-#3601 code on this box: it reads a lock whose pid file is not yet present as STALE and renames it aside, which is the defect #3601 fixes. Starting now would put two supervisors in one worktree, so this run stops instead" \
    "re-run this supervisor: if the other holder is a real one, the next start will say so and name its pid; if it was a pre-#3601 reclaim, upgrade every checkout on this box that can launch a supervisor past #3601 so no peer reclaims a pid-less lock again"
}

# supervisor_lock_refuse_publish_failed <cause> — the lock NAME became ours and we could not record
# ourselves in it (#3601, roborev job 231 B1).
#
# THIS IS A SEPARATE REFUSAL BECAUSE IT IS A SEPARATE FACT, and conflating it cost a false diagnostic:
# an ENOSPC or a read-only filesystem never writes a byte, so telling the operator that "our pid was
# published and read back wrong" describes a race that did not happen and hides the one that did. It also
# reports that the directory we created was REMOVED AGAIN, because an operator who reads "could not
# start" and then finds a lock sitting there will otherwise go looking for a holder.
supervisor_lock_refuse_publish_failed() {
  local cause="$1" pub="$1" cleanup='' what='' aftermath='' remedy='' cmd=''
  pub="${cause%% *}"
  case "$cause" in
    *' '*) cleanup="${cause#* }" ;;
  esac
  # THE STEP THAT FAILED IS REPORTED; THE *CAUSE* IS NOT CLAIMED (#3601, roborev job 245 B21). An earlier
  # cut carried a `nature` per cause — `filesystem` for a failed write or rename, `contention` when the
  # directory had vanished — derived from a post-failure `-d` test. That test cannot decide it in either
  # direction (see `supervisor_lock_nature_unestablished`), and `write-failed` claimed a filesystem fault
  # with no test at all. So each branch now states only WHAT it observed, and the shared ambiguity text
  # states what could have caused it, with the order to check.
  local what='' nature_line='' first_action=''
  case "$pub" in
    marker-failed)
      what='writing our OWNERSHIP MARKER into the lock FAILED — this run got the lock name and could not record, even provisionally, that the directory is its own; the directory was still there when this run looked'
      ;;
    marker-failed-lock-gone)
      what='writing our OWNERSHIP MARKER into the lock FAILED, and the lock directory this run had just created was GONE when this run looked'
      ;;
    write-failed)
      what='writing our pid into the lock FAILED — no byte of it was written'
      ;;
    rename-failed)
      what='publishing our pid into the lock FAILED at the rename — the staging file was written and could not be moved into place; the lock directory was still there when this run looked'
      ;;
    rename-failed-lock-gone)
      what='publishing our pid into the lock FAILED at the rename, and the lock directory this run had created was GONE when this run looked'
      ;;
    *)
      what="recording our pid in the lock FAILED ([$pub])"
      ;;
  esac
  nature_line="$(supervisor_lock_nature_unestablished)"
  first_action="$(supervisor_lock_nature_actions)"
  # EVERY SENTENCE BELOW IS BOUND TO AN OBSERVATION `supervisor_lock_take` ACTUALLY MADE (#3601 B9).
  case "$cleanup" in
    cleanup-verified-absent)
      aftermath='The directory this run created has been removed again and its absence was VERIFIED after the removal, so this failure leaves no pid-less lock behind for the next start to have to refuse over'
      remedy="$first_action. Nothing needs clearing by hand"
      ;;
    'cleanup-failed-foreign-holder '*)
      aftermath="This run then tried to remove the directory it had created and it REMAINS — and it now holds a holder record that is NOT ours ([${cleanup#cleanup-failed-foreign-holder }]). A peer published into it while our publish was failing; the removal is NON-RECURSIVE precisely so that it cannot delete that record, and nothing of that peer's has been touched"
      remedy="nothing to clear: the path named above belongs to another holder, which this run left intact. $first_action; the next start will read that holder and report it by pid"
      ;;
    cleanup-declined-lock-already-gone)
      aftermath='There was nothing for this run to remove: the directory it created was already gone when it looked. Nothing was removed and nothing is left behind by this run'
      remedy="$first_action"
      ;;
    cleanup-declined-no-ownership-evidence)
      aftermath='This run therefore did NOT attempt to remove the directory it created: with no marker written it cannot prove the directory now at that name is still its own, and a removal on that guess is how a peer that took the name in the meantime loses its lock. A lock MAY be sitting at that path — this run makes no claim either way'
      remedy="$first_action. If starts keep refusing over a lock at that path, inspect it and clear it with the next line, which is NON-RECURSIVE and refuses if anything is inside that you have not examined$(supervisor_lock_shape_note)"
      cmd="$(supervisor_lock_clear_command)"
      ;;
    'cleanup-declined-foreign-holder '*)
      aftermath="This run then found that the directory at that name is NOT the one it created — its ownership marker is gone from it — and that it holds a holder record ([${cleanup#cleanup-declined-foreign-holder }]). Another supervisor took the name while our publish was failing, so this run removed NOTHING"
      remedy="nothing to clear: the path named above belongs to another holder, which this run left intact. $first_action; the next start will read that holder and report it by pid"
      ;;
    'cleanup-declined-foreign-instance '*)
      aftermath="This run then found that the directory at that name is NOT the one it created — its ownership marker is gone from it — and that it carries no holder record yet ([${cleanup#cleanup-declined-foreign-instance }]). That is another supervisor's start in progress, which is exactly the lock a blind removal would have destroyed, so this run removed NOTHING"
      remedy="nothing to clear: another supervisor is starting at that path and this run left it alone. $first_action; the next start will read that holder and report it by pid"
      ;;
    'cleanup-failed-residual '*)
      aftermath="This run then tried to remove the directory it had created and it REMAINS ([${cleanup#cleanup-failed-residual }]) — the removal did NOT succeed, so a lock DOES sit at that path and the next start will refuse over it until it is cleared"
      remedy="$first_action. Then clear the leftover lock with the next line, on its own, exactly as printed — it is NON-RECURSIVE, so it refuses if anything is inside that you have not examined$(supervisor_lock_shape_note)"
      cmd="$(supervisor_lock_clear_command)"
      ;;
    *)
      aftermath='The removal outcome for the directory this run created was NOT ESTABLISHED, so this refusal makes no claim about whether a lock remains at that path'
      remedy="$first_action. Then inspect the path named above before re-running: this run could not establish whether it left a lock there"
      ;;
  esac
  supervisor_lock_refuse \
    "refusing to start — the lock name became ours and this run could not record itself in it" \
    "$what. $nature_line. $aftermath" \
    "$remedy" \
    "$cmd"
}

# supervisor_lock_refuse_undecidable <cause> <what-was-observed> — the "cannot tell" refusal, and the
# only place ruling 2's remedy obligation actually bites: this is the branch that would otherwise leave a
# lane blocked by a directory with nothing to do about it.
#
# THE PRINTED COMMAND IS `rmdir`, AND THE CHOICE IS THE SAFETY ARGUMENT. `rmdir` is NON-RECURSIVE and
# REFUSES a non-empty directory, so a mis-paste cannot delete a holder's pid record: the worst outcome is
# "Directory not empty", which is itself the signal to go and look. `rm -rf` would be the opposite — it
# would silently destroy the evidence of the very state we could not decide. `--` terminates option
# parsing because the path is `TMPDIR`-derived, and the path is rendered by `supervisor_shell_quote` so
# the line is paste-safe on one physical line.
supervisor_lock_refuse_undecidable() {
  local cause="$1" observed="$2"
  supervisor_lock_refuse \
    "refusing to start — the lock is HELD and this run could NOT DECIDE whether its holder is alive ($cause)" \
    "$observed. THIS IS NOT A REPORT THAT THE HOLDER IS DEAD, and it is not a report that it is alive: it is a report that the question was undecidable here. A reclaim needs AFFIRMATIVE evidence that the recorded holder is gone, and there is none, so this run stops rather than take a lock that may belong to a live supervisor (#3601). An ordinary stale lock — one left by a holder that was killed — carries a well-formed pid, is decided automatically, and never reaches this message" \
    "PRECONDITION FIRST — establish that no supervisor is running for this lane (bash scripts/flow/claim-heartbeat.sh dead-lanes, and check for a live worker-supervisor process for this lane). Once you have, clear the lock by running the next line, on its own, exactly as printed. It is NON-RECURSIVE on purpose: it REFUSES if anything is still inside, so it cannot delete a holder's record you have not examined — if it refuses, list the contents and decide before removing anything. Removing a lock also needs WRITE permission on its PARENT directory, which reading one does not: if the line below fails with a permission error, that is why, and it is also how a supervisor's own exit can leave a pid-less lock here (#3601)$(supervisor_lock_shape_note)" \
    "$(supervisor_lock_clear_command)"
}

# supervisor_lock_clear_command / supervisor_lock_shape_note — THE REMEDY MUST MATCH WHAT IS ACTUALLY AT
# THAT NAME (#3601, roborev job 231 B2).
#
# A refusal that prints a command which CANNOT WORK is worse than one that prints none: it spends the
# operator's trust and then fails, and they have no way to tell whether the failure means "wrong command"
# or "something is seriously wrong with this lock". `rmdir` is right for a DIRECTORY and wrong for every
# other shape, and the other shapes are reachable — a lock NAME that is a regular file or a dangling
# symlink refuses with `pid-file-absent`, because `<file>/pid` does not exist.
#
# `-L` IS TESTED BEFORE `-d`, and that order is the whole correctness of it: `[[ -d ]]` FOLLOWS a symlink,
# so a symlink-to-a-directory would otherwise be handed `rmdir`, which fails with ENOTDIR. `rm -f` on a
# symlink removes the LINK and never touches the target, so it is the safe answer for both link shapes.
# Every command emitted here is NON-RECURSIVE, so none of them can delete contents nobody examined.
supervisor_lock_clear_command() {
  local shown=''
  shown="$(supervisor_shell_quote "$SUPERVISOR_LOCK")" || true
  if [[ -L "$SUPERVISOR_LOCK" ]]; then
    printf 'rm -f -- %s' "$shown"
  elif [[ -d "$SUPERVISOR_LOCK" ]]; then
    printf 'rmdir -- %s' "$shown"
  elif [[ -e "$SUPERVISOR_LOCK" ]]; then
    printf 'rm -f -- %s' "$shown"
  fi
}

supervisor_lock_shape_note() {
  if [[ -L "$SUPERVISOR_LOCK" ]]; then
    printf '%s' '. NOTE: that name is a SYMLINK, not a lock directory — the line below removes the LINK and does not touch whatever it points at, which is why it is not an rmdir'
  elif [[ -d "$SUPERVISOR_LOCK" ]]; then
    # THE ONE CASE WHERE A CORRECT `rmdir` STILL REFUSES, NAMED RATHER THAN LEFT TO SURPRISE (#3601 B12).
    # This is NOT the B2 defect of printing a command that could never work for the shape at that name:
    # `rmdir` is the right command for a directory, and it refuses only when there is content the
    # operator must look at first, which is the safety property it was chosen for. A supervisor killed
    # between creating its lock and recording its pid leaves an ownership marker behind — a state this
    # change introduced — so the note says so and says what to do when the line refuses.
    printf '%s' '. NOTE: the line below is NON-RECURSIVE and will REFUSE if anything is inside — including an `own.<pid>` ownership marker or a `pid.tmp.<pid>` staging file left by a supervisor killed between creating this lock and recording its pid. If it refuses, list the contents (ls -lna on the path above) and decide before removing anything'
  elif [[ -e "$SUPERVISOR_LOCK" ]]; then
    printf '%s' '. NOTE: that name is NOT a directory, so it is not a lock this script could ever have written — the line below removes that one file, non-recursively'
  else
    printf '%s' '. NOTE: nothing exists at that name any more, so there is nothing to clear and no command is printed below'
  fi
}

# supervisor_lock_refuse_lost_race — a dead holder's lock was cleared and the name was taken again
# before we could claim it. This is the loud exit the pre-#3601 code reached as "failed to acquire lock",
# kept loud and given the cause it always had: we lost a race, we are not co-running.
supervisor_lock_refuse_lost_race() {
  supervisor_lock_refuse \
    "refusing to start — the lock name was taken again before this run could claim it" \
    "a stale lock left by a dead holder is gone from that name and the name was claimed again before this run could take it, so this run did NOT get the lock. WHO cleared it and WHO holds it now are both unestablished — this run reached its own claim and found the name taken, which is all it observed; it does not know that its own rename is what cleared the lock, and it has not read the new holder. The loser stops here rather than co-running" \
    "nothing is wrong: re-run this supervisor. If it keeps losing, the winner is a live supervisor for this lane and the next start will name its pid"
}

# supervisor_lock_nature_unestablished / supervisor_lock_nature_actions — THE ONE THING EVERY FAILURE
# SITE IN THIS FILE MAY SAY ABOUT *WHY* AN OPERATION FAILED (#3601, roborev job 245 B21).
#
# WHY THERE IS NO CONFIDENT VERDICT LEFT. Every one of these sites tried to tell contention apart from a
# filesystem fault by looking at whether the lock path exists AFTER the failure. That is unsound in BOTH
# directions, and the two directions were introduced two rounds apart:
#   * job 244 (B20) fixed "contention reported as a disk problem": `mkdir` fails with EEXIST, the name is
#     present, and the code called it a path failure.
#   * job 245 (B21) is the inverse it created: contender A moves the old lock aside, B's operation fails
#     while the name is ABSENT, A republishes before B reaches the check — and B reports a filesystem
#     fault and sends an operator to repair permissions on a box that is fine.
# A post-failure existence test cannot decide this, because a peer can remove or recreate that name
# between the failure and the check, either way round. The failing call's errno is the only thing that
# could decide it and this script cannot see it.
#
# AND IT IS NOT RECOVERED BY PARSING `mv`/`mkdir` STDERR. Message text is locale-dependent and is not a
# contract — that is the cargo-output-parse defect class this repo has already paid for (CLAUDE.md
# #3400), and a locale-sensitive guess dressed as a verdict is worse than an honest ambiguity.
#
# SO THE AMBIGUITY IS PRESERVED — AND MADE ACTIONABLE, which is the whole point. The sweep's value was
# telling "retry" apart from "fix the box", and going ambiguous gives that up unless the text says both
# possibilities WITH what to check for each and in what order. An honest "it is one of these two, here is
# how to tell" beats a confident wrong nature and it also beats a shrug.
#
# THE SOUND ALTERNATIVE, NOT BUILT HERE, RECORDED SO IT IS NOT REDISCOVERED: measure the filesystem
# AFFIRMATIVELY instead of inferring it — try to create and unlink a scratch file in the lock's PARENT, so
# a success proves the filesystem is fine (hence contention) and a failure demonstrates the fault. That is
# a real answer rather than an inference, and it is deliberately NOT added here: it puts a new write on
# every failure path, and a mechanism that produces one new instance per fix is one to remove rather than
# iterate. It belongs with #3683/#3697.
#
# APPLIED AT ALL FIVE SITES, not the three the finding named. The first `take`'s uncreatable refusal and
# the reclaim rename's refusal made the same claim from the same kind of test, and `write-failed` claimed
# a filesystem fault with no test at all; leaving those confident while fixing the others would have been
# the same defect with a smaller blast radius.
supervisor_lock_nature_unestablished() {
  printf '%s' "WHETHER THIS WAS CONTENTION OR A FILESYSTEM FAULT IS NOT ESTABLISHED: this script cannot see the failing call's error code, and the state of that path afterwards cannot decide it either, because a peer can remove or recreate the name between the failure and any check made here — in either direction"
}

supervisor_lock_nature_actions() {
  printf '%s' "IT IS ONE OF TWO THINGS AND THIS IS THE ORDER TO TELL THEM APART. (1) A FILESYSTEM FAULT: check the PARENT directory of the path named above — it must exist, be a directory, be writable by this user, and its filesystem must have free space and not be mounted read-only (ls -ld on the parent, df, mount). If any of those is wrong, that is the cause, and re-running changes nothing until it is fixed. (2) CONTENTION: if all of those are clean, another supervisor was taking or releasing this lock at the same moment, nothing is wrong with this box, and re-running is sufficient"
}

# supervisor_lock_refuse_uncreatable [<phase-note>] — `mkdir` failed AND nothing is at that name, so the
# failure is about the PATH and not about a holder (#3601 AC7 addendum; reused post-reclaim by job 244 B20).
#
# TWO CALL SITES, ONE TEXT, because the operator's problem is identical in both: the lock path cannot be
# created. The optional phase note says WHERE in the start we were, which is the only thing that differs.
supervisor_lock_refuse_uncreatable() {
  local phase="${1:-}"
  supervisor_lock_refuse \
    "refusing to start — the lock directory could NOT BE CREATED, and nothing exists at that path" \
    "\`mkdir\` failed and NOTHING is at that name now${phase:+ ($phase)}. $(supervisor_lock_nature_unestablished)" \
    "$(supervisor_lock_nature_actions). The path is \${TMPDIR:-/tmp}-derived unless this run's launcher named it, so a TMPDIR pointing somewhere absent, read-only or full is the usual first cause to check"
}

acquire_lock() {
  # IDENTITY FIRST: both the lock path and the claim actor are derived FROM it, so resolving it after
  # them would leave them on a stale inference.
  supervisor_resolve_lane_id
  supervisor_lock_path
  # BEFORE any side effect (#3549). A refusal must leave nothing behind it: `supervisor_claim_actor`
  # EXPORTS an actor into the environment and `supervisor_migrate_legacy_claim` performs a CAS ADOPT
  # that pushes a ref to origin — neither is something a supervisor that is about to refuse to start
  # should have done. It runs after `supervisor_lock_path` only so the refusal can NAME our own lock
  # path in its diagnostic; the guard's VERDICT depends on nothing that call produces.
  supervisor_legacy_lock_guard
  supervisor_claim_actor
  supervisor_migrate_legacy_claim

  local takerc=0
  supervisor_lock_take || takerc=$?
  if [[ "$takerc" -eq 0 ]]; then
    return 0
  fi
  if [[ "$takerc" -eq 2 ]]; then
    supervisor_lock_refuse_unowned "$SUPERVISOR_LOCK_TAKE_CAUSE"
  fi
  if [[ "$takerc" -eq 3 ]]; then
    supervisor_lock_refuse_publish_failed "$SUPERVISOR_LOCK_TAKE_CAUSE"
  fi

  # `mkdir` FAILING IS NOT YET EVIDENCE OF CONTENTION, and conflating the two is the diagnostic defect
  # #3601's addendum measured: with an option-shaped `TMPDIR` the pre-fix code printed "reclaiming stale
  # lock" and then "failed to acquire lock", sending an operator to hunt a stale lock that did not exist.
  # `mkdir` also fails when the parent is missing, unwritable, or not a directory. So before attributing
  # the failure to a holder, check that something IS at that name.
  if [[ ! -e "$SUPERVISOR_LOCK" && ! -L "$SUPERVISOR_LOCK" ]]; then
    supervisor_lock_refuse_uncreatable ''
  fi

  # THE NAME IS TAKEN. WHO HOLDS IT? Three-valued, and only an AFFIRMATIVE `dead` reclaims.
  local holder='' holder_pid='' liveness='' tries=0
  # THE BOUNDS ARE CONSTANTS, NOT KNOBS (#3601, roborev job 231 B5/B6). An earlier cut of this change
  # read them from the environment so the cases could go fast, which is both a seam a real invoker can
  # set (CLAUDE.md #3312) and a knob the tests would then be testing instead of the code — and its
  # validation covered only the try count, leaving `sleep "$wait_secs"` to be handed anything at all
  # (`999999` makes the window this code calls bounded unbounded; `--help` puts sleep's usage on our
  # stdout). Being literals, there is nothing to validate and nothing to override.

  # THE PROBE ASSIGNMENTS CARRY A FAIL-CLOSED FALLBACK (#3549 job 201 F3, same shape). Both probes
  # return 0 on every path they take, so these are unreachable today — but a non-zero inside a `$( )`
  # makes the ASSIGNMENT non-zero, and under this script's own errexit that would abort the start
  # silently instead of refusing. The fallbacks land on the REFUSING verdict of each probe, so an
  # unforeseen internal failure costs a start and can never cost a live holder its lock.
  holder="$(supervisor_lock_pid_read "$SUPERVISOR_LOCK/pid")" || holder='unparseable pid-probe-aborted'
  # THE PID-LESS WINDOW IS RESOLVED BY MEASURING PERSISTENCE, WHICH IS NOT A TIMING HEURISTIC — and it
  # will read like one, so here is the difference. A heuristic infers WHAT is happening from HOW LONG it
  # has been happening. This measures a different property: a pid-less lock is a state a STARTING holder
  # passes through in ONE rename, and a state a lock abandoned inside that window stays in FOREVER.
  # Transient and persistent are distinguishable by observation alone, and re-reading is how you observe
  # it. Both outcomes are affirmative: either a pid APPEARS — a real answer, and the wait ends early —
  # or it PERSISTENTLY does not, which is itself the answer "this is not a holder that is starting". The
  # second branch still refuses; it does not conclude death. The bound exists because an unbounded wait
  # is a hang, and it is short because the state it waits out is one rename wide.
  #
  # ONLY THE TWO TRANSIENT CAUSES ARE RE-READ. `pid-file-absent` and `pid-file-empty` are the two states
  # a writer passes through (the name not yet renamed into place; a pre-#3601 peer's `echo $$ >…/pid`
  # between create and write). A garbled, multi-line or non-decimal pid is not a startup artifact of any
  # writer, so re-reading it would only add delay to a refusal that is already decided.
  while [[ "$tries" -lt "$SUPERVISOR_LOCK_PID_TRIES" ]]; do
    case "$holder" in
      'unparseable pid-file-absent' | 'unparseable pid-file-empty') ;;
      *) break ;;
    esac
    tries=$((tries + 1))
    # A `sleep` that cannot take a fractional argument (some minimal builds) fails harmlessly: the loop
    # then spins its bounded number of iterations instead of pausing, which measures the same property
    # with less patience.
    sleep "$SUPERVISOR_LOCK_PID_WAIT" 2>/dev/null || true
    holder="$(supervisor_lock_pid_read "$SUPERVISOR_LOCK/pid")" || holder='unparseable pid-probe-aborted'
  done

  case "$holder" in
    'pid '*)
      holder_pid="${holder#pid }"
      ;;
    *)
      supervisor_lock_refuse_undecidable "${holder#unparseable }" \
        "the lock exists and this run could not read a usable holder pid out of it after $((tries + 1)) read(s) over a bounded window; the pid probe's verdict was [$holder]"
      ;;
  esac

  liveness="$(supervisor_lock_holder_liveness "$holder_pid")" || liveness='unknown liveness-probe-aborted'
  case "$liveness" in
    live)
      # AC4: THE REFUSAL SAYS WHAT IT KNOWS AND SAYS WHAT IT DID NOT CHECK. It has established that pid
      # $holder_pid EXISTS; it has NOT established that the process is a supervisor. Pids are reused, so
      # a lock left by a dead holder can name a number the kernel has since given to something else.
      #
      # WHY NO IDENTITY PROBE IS RUN, rather than one being added: the verdict could not change the
      # decision. `live` refuses whatever the process turns out to be, because reclaiming on an identity
      # GUESS is precisely how a live holder's lock gets stolen — and #3549's ruling on exactly this
      # shape is that machinery whose output cannot change the decision is not a guard but a description
      # generator on the decision path. So the scope is DECLARED in the text an operator reads instead,
      # which is AC4's own second option.
      supervisor_lock_refuse \
        "another instance is already running (pid $holder_pid)" \
        "pid $holder_pid is recorded as this lane's lock holder and that process EXISTS (verified: it is signallable, or the kernel reports it exists but is not ours to signal). ITS IDENTITY IS NOT VERIFIED — this run did not check that pid $holder_pid is a worker-supervisor, and pids are REUSED, so a lock abandoned by a dead holder can name a number that now belongs to an unrelated process" \
        "if that pid IS the supervisor for this lane, wait for it or stop it. If it is NOT — check with: ps -p $holder_pid -o pid,ppid,user,lstart,args — then the lock is stale in a way this run cannot prove, and clearing it is the operator's call once no supervisor is running for this lane"
      ;;
    dead) ;;
    *)
      supervisor_lock_refuse_undecidable "holder-liveness-${liveness#unknown }" \
        "the lock records holder pid $holder_pid, and the liveness probe could not decide whether that process exists (verdict [$liveness])"
      ;;
  esac

  # THE ONLY RECLAIMING PATH, AND IT IS REACHED ONLY FROM AN AFFIRMATIVE `dead`.
  log "$(supervisor_one_line "reclaiming stale lock $(supervisor_shell_quote "$SUPERVISOR_LOCK") (holder pid $holder_pid is affirmatively DEAD: the kernel reports no such process)")"
  # Reclaim by rename-then-remove, not rm-then-mkdir: the rename is a single atomic operation, so a
  # racer that loses it does not get a window in which the lock name simply does not exist.
  #
  # WHAT THIS DOES **NOT** DO, STATED HERE BECAUSE THE COMMENT THAT USED TO SIT ON THIS LINE CLAIMED
  # OTHERWISE (#3601, roborev job 231 F1; tracked as #3683). The prior wording asserted that "only ONE
  # racer's mv can succeed" and that the loser therefore "refuses loudly instead of silently
  # co-running". Both are FALSE, and the interleaving that falsifies them is this one:
  #
  #   A and B both read holder pid P and both get an affirmative `dead`.
  #   A renames the stale lock aside, removes it, takes the name, publishes pid A -- A is now LIVE.
  #   B's rename now runs. The name EXISTS again (it is A's fresh, live lock), so B's `mv` SUCCEEDS,
  #   moving A's lock aside; B removes it and takes the name. A and B now BOTH believe they hold the
  #   lane, and nothing printed a refusal.
  #
  # So the rename serialises the case where the loser arrives BEFORE the winner republishes, and it does
  # not close the ABA window where the loser arrives AFTER. Closing that needs serialisation across
  # classify -> reclaim -> claim (the same root as the read-then-remove in `supervisor_lock_release`),
  # which is one follow-up (#3683) and not a widening of this change: the primitive it needs is a lock
  # protecting a lock, whose own staleness reopens "how does a stale instance get cleared, and by whom?"
  # one level down. Two shortcuts are already measured closed -- `mkdir`+`mv` is not atomic (it leaves a
  # pid-less window), and `mv -T` is NOT `RENAME_NOREPLACE`: it refuses a non-empty target but SUCCEEDS
  # against an EMPTY one, which is exactly a peer's pid-less window, besides being GNU-only.
  #
  # NARROWED, NOT CLOSED: this path is now reached ONLY from an affirmative `dead`, where the pre-#3601
  # code reclaimed on ANY non-live answer (unparseable pid, EPERM, pid-less window), so the set of states
  # that can reach the race is much smaller than it was -- but it is not empty. Do not read the paragraph
  # above as a guarantee; it is a bound.
  #
  # `--` on both operands: `TMPDIR`-derived (#3601 AC7).
  if mv -f -- "$SUPERVISOR_LOCK" "$SUPERVISOR_LOCK.stale.$$" 2>/dev/null; then
    rm -rf -- "$SUPERVISOR_LOCK.stale.$$" 2>/dev/null || true
  elif [[ -e "$SUPERVISOR_LOCK" || -L "$SUPERVISOR_LOCK" ]]; then
    # THE RENAME FAILED AND THE LOCK IS STILL THERE (#3601, roborev job 231 B2). Ignoring this was a
    # MISDIAGNOSIS, not just a missing branch: the `take` below then failed too and the run printed the
    # lost-race refusal — "a stale lock was cleared and the name was immediately claimed by someone
    # else" — which had not happened, together with "re-run this supervisor", which loops forever. Same
    # family as the AC7 addendum: a message that sends the operator after a problem that is not there.
    # The realistic cause is the parent directory becoming unwritable after the lock appeared.
    #
    # AND THE IDENTITY WE MEASURED IS STALE BY THE TIME WE GET HERE (#3601, roborev job 242 B18). The
    # liveness verdict above describes the pid file as it was read BEFORE the rename was attempted; the
    # path can be replaced in between, so the object now at that name may belong to a LIVE holder. The
    # previous wording declared "this lock IS stale and this run is entitled to it" and printed the
    # NON-RECURSIVE REMOVAL COMMAND for it.
    #
    # THAT IS WHY THIS ONE IS NOT MERELY ANOTHER OVERCLAIM. The code destroys nothing here; the printed
    # line does, in the operator's hands, and remedies in this file exist precisely to be pasted. It is
    # the sibling of "a remedy that cannot work is worse than none" — a remedy that WORKS, aimed at the
    # wrong target. So this branch now re-reads the lock, reports what is ACTUALLY there, declares
    # nothing stale or removable, and prints only a READ-ONLY inspection: the one command shape that is
    # safe under the assumption that the lock is live. `ls -ldn` names the object without following a
    # symlink and `ls -lna` shows a directory's contents (both measured rc=0 on all three shapes under
    # #3549), `--` because the path is `TMPDIR`-derived, and the path is rendered paste-safe.
    local after_mv='' identity=''
    after_mv="$(supervisor_lock_pid_read "$SUPERVISOR_LOCK/pid")" || after_mv='unparseable post-rename-probe-aborted'
    case "$after_mv" in
      "pid $holder_pid")
        identity="it still records pid $holder_pid, the holder this run measured as dead. That is consistent with the same lock still being there, but it is NOT proof: this run cannot establish that the object now at that name is the same one it measured, so it does not declare the lock stale"
        ;;
      'pid '*)
        identity="it now records a DIFFERENT holder, pid ${after_mv#pid }, and not the pid $holder_pid this run measured as dead. Something took that name while the rename was failing, and that holder may be ALIVE — the dead-holder measurement above no longer describes what is at that path"
        ;;
      *)
        identity="its pid file no longer reads as a usable holder record ([$after_mv]), so this run cannot tell whether the object now at that name is the stale lock it measured or something else that has since taken the name"
        ;;
    esac
    supervisor_lock_refuse \
      "refusing to start — a lock this run measured as stale could not be cleared" \
      "renaming the lock aside FAILED, so nothing was cleared and nothing was claimed; nothing at that path has been modified by this run. $(supervisor_lock_nature_unestablished). AND THE IDENTITY OF WHAT IS THERE IS NOW UNESTABLISHED TOO — $identity" \
      "$(supervisor_lock_nature_actions). Either way, RE-RUN rather than clearing anything by hand: the next start re-measures the holder from scratch and will reclaim the lock if it is genuinely stale, or name its live holder. Do NOT remove anything on the strength of this message — this run measured a dead holder BEFORE its rename failed and cannot vouch for what is at that path now. To see what is actually there, run the next line, on its own, exactly as printed; it only reads" \
      "ls -ldn -- $(supervisor_shell_quote "$SUPERVISOR_LOCK") && ls -lna -- $(supervisor_shell_quote "$SUPERVISOR_LOCK")"
  fi
  takerc=0
  supervisor_lock_take || takerc=$?
  if [[ "$takerc" -eq 0 ]]; then
    return 0
  fi
  if [[ "$takerc" -eq 2 ]]; then
    supervisor_lock_refuse_unowned "$SUPERVISOR_LOCK_TAKE_CAUSE"
  fi
  if [[ "$takerc" -eq 3 ]]; then
    supervisor_lock_refuse_publish_failed "$SUPERVISOR_LOCK_TAKE_CAUSE"
  fi
  # STATUS 1 IS `mkdir` FAILING, AND THAT IS TWO DIFFERENT FACTS WITH OPPOSITE REMEDIES (#3601, roborev
  # job 244 B20). EEXIST means someone else took the name — a race, and re-running is exactly right.
  # EACCES/ENOSPC/ENOENT mean the path cannot hold a lock — and then "a stale lock was cleared and the
  # name was claimed by someone else, re-run this supervisor" tells an operator that nothing is wrong and
  # sends them into a retry loop over a state that cannot resolve until permissions or disk are fixed.
  #
  # THIS IS #3601'S OWN HEADLINE DEFECT, ONE BRANCH OVER: the AC7 addendum exists because the pre-fix code
  # "blames the lock rather than the path shape, and an operator goes looking for a stale lock that isn't
  # there". Shipping this issue with a fresh instance of its own rationale is not defensible. The FIRST
  # take already disambiguates exactly this way, which is what makes the omission here an oversight rather
  # than a design choice — so the same existence test decides it.
  if [[ -e "$SUPERVISOR_LOCK" || -L "$SUPERVISOR_LOCK" ]]; then
    supervisor_lock_refuse_lost_race
  fi
  supervisor_lock_refuse_uncreatable 'after a stale lock was successfully cleared, so the name was free a moment ago'
}

# ---------------------------------------------------------------------------
# Shared-object-store integrity sweep (issue #3749) — THROTTLED, and deliberately
# NOT a hold reason.
#
# WHY IT IS HERE. Every lane on this box reads ONE shared `.git`, and git does not
# rehash an object against the id it was asked for on an ordinary read. A corrupt
# shared store can therefore change ANY gate's verdict on this box, so no worker
# should be allowed to certify against it. #3749's owner ruling scopes this to
# ACCIDENTAL corruption (bit rot, a torn pack write, a SIGKILLed gc); DELIBERATE
# peer forgery is invoker-class and out of model per the #3312 triage rule.
#
# WHY `CORRUPT` STOPS THE LOOP RATHER THAN HOLDING IT. Corruption is
# NON-SELF-CLEARING: a HOLD-and-repoll loop would spin until the wall-clock budget
# taking no useful action, which is precisely the latch #2670 bounded the leftover
# families to avoid. So it uses the established "stop loudly" idiom — notify high,
# journal, finalize_exit with its own reason — the same shape as leftover-worker
# exceeding LEFTOVER_HOLD_MAX.
# ---------------------------------------------------------------------------
OBJ_SWEEP_ANNOUNCED=0
OBJ_SWEEP_UNMEASURED_NOTIFIED=0
OBJ_SWEEP_NOSTAMP_NOTIFIED=0
OBJ_SWEEP_CLAIM_NOTIFIED=0
# The in-progress claim this process currently owns, or empty. Read by the EXIT trap, so
# it is assigned before anything can create a claim (`set -u`), and it is what makes the
# claim a REGISTERED resource rather than one whose lifetime nobody owns (CLAUDE.md's
# roborev-job-282 ruling: a fix that adds a resource inherits that resource's lifetime
# bugs, so register it the moment it exists and clear it the moment it is released).
OBJ_SWEEP_CLAIM_OWNED=""
# THE OWNERSHIP TOKEN THIS PROCESS WRITES INTO ITS OWN CLAIM (#3749 review round 11, item
# 2). It is what makes a release ASK before deleting: the path alone cannot tell this
# lane's claim from a SUCCESSOR's claim at the same path. Set once, on the line after the
# claim is created; empty until then, and an empty token can never match anything.
OBJ_SWEEP_CLAIM_TOKEN=""
OBJ_SWEEP_CLAIM_STATE=""
# SLACK on the claim-staleness bound: what a sweep spends OUTSIDE its bounded fsck walks —
# process startup, the `--print-store` resolution, output capture, and the notify/journal
# writes. It is additive to a DERIVED product (see obj_sweep_claim_stale_secs) rather than
# a bound in its own right, which is why it is a small constant and not a measurement.
OBJ_SWEEP_CLAIM_SLACK_SECS=60

# obj_sweep_stamp_path <sweep-script> — the THROTTLE stamp: WHEN this box last SPENT a
# sweep. Keyed on the SHARED OBJECT STORE, not on the lane, so four lanes of one box
# share one cadence instead of sweeping four times.
#
# IT HOLDS A TIMESTAMP AND NOTHING ELSE, AND THAT IS THE DESIGN, NOT AN OMISSION (#3749
# review round 2, BLOCKER 1). The CORRUPT verdict lives in a SEPARATE, CREATE-ONLY file
# — see obj_sweep_set_latch below for the reasoning. A timestamp is a cell it is
# HARMLESS to move backwards: the worst a losing writer can do is buy the box one extra
# sweep. A VERDICT is not, which is why it is not kept here.
#
# THE STORE IS RESOLVED BY THE SWEEP SCRIPT ITSELF (`--print-store`), NOT BY A `git`
# CALL IN THIS FILE, AND THAT IS THE REVIEW'S OTHER CORRECTION (BLOCKER 2). This
# function used to run a BARE `git -C "$REPO_ROOT" rev-parse --git-common-dir`,
# inheriting the caller's environment, while the sweep it keys runs EVERY git call under
# `env -i` + one allowlist. An inherited GIT_DIR/GIT_COMMON_DIR therefore selected
# ANOTHER repository's stamp — so the real store's sweep is throttled away, or its
# verdict recorded under the wrong key. That is the same defect class the sweep had just
# closed, at a site the same round left behind, which is CLAUDE.md's roborev-job-276
# ruling verbatim ("the migrated object reads ran under a bare `env` … the round-13 hole
# re-opened at the NEW sites, not a new route"), and its remedy is the same: ONE
# resolver, in the file that owns the allowlist. A future git call cannot be added
# un-isolated HERE because there is no git call here at all — the un-isolated shape is
# unavailable rather than discouraged.
#
# THE DIRECTORY IS DERIVED, NOT TAKEN FROM THE CALLER. Both facts this pair of files
# records — "this box swept recently" and "this box's shared store is damaged" — are
# BOX-wide facts about a store every lane reads, so a per-lane `TMPDIR` would silently
# make them per-lane: peers would keep their own cadence and, worse, never see a peer's
# CORRUPT. `/tmp` when it is a writable directory, `${TMPDIR:-/tmp}` only as a fallback
# for a host without one. OBJ_SWEEP_STAMP still overrides both (the self-suite pins it
# per case), and the latch follows it, so a pinned stamp relocates BOTH files together.
#
# AN UNRESOLVABLE STORE — OR A HOST WITH NO DIGEST TOOL — DOES NOT THROTTLE AT ALL: this
# prints the empty string, and the caller then neither reads nor writes a stamp (and says
# so once in the journal). The old fallback collapsed every unresolvable store on a box
# onto ONE name, so two different checkouts shared a 6-hour throttle and one suppressed
# the other's sweep: a MISSED sweep, not an extra one. Not throttling costs nothing in the
# unresolvable case, because the sweep resolves the same store itself and fails FAST (a
# sub-second UNMEASURED) in exactly those states; on a host with no digest tool it costs
# an un-throttled sweep per iteration, which is loud and visible rather than a key two
# stores can share.
obj_sweep_stamp_path() {
  local script="$1" line key dir
  if [[ -n "$OBJ_SWEEP_STAMP" ]]; then
    printf '%s' "$OBJ_SWEEP_STAMP"
    return 0
  fi
  [[ -n "$script" && -r "$script" ]] || return 0
  # `{ grep || true; }` for the reason spelled out in object_store_sweep below: this
  # file runs under `set -euo pipefail`, so a grep that matches nothing (an older or
  # stubbed sweep script that has no --print-store mode) would take the supervisor down.
  line="$(bash "$script" --repo "$REPO_ROOT" --print-store 2>/dev/null |
    { grep '^OBJECT-STORE: store-key ' || true; } | head -1)"
  key="${line#OBJECT-STORE: store-key }"
  # Both halves are required: an empty line yields an empty value, and a line the prefix
  # did NOT strip is not the answer to this question.
  [[ -n "$key" && "$key" != "$line" ]] || return 0
  # THE KEY IS DERIVED BY THE SWEEP SCRIPT, FROM THE RAW CANONICAL PATH — NOT HERE, AND NOT
  # FROM THE `store` LINE (#3749 review round 5, item 3). Round 4 made this key injective
  # over the FLATTENING (`/tmp/a/b/objects` and `/tmp/a_b/objects` collapse to one name) by
  # digesting the path — and digested the value from the resolver's `store` line, which is
  # passed through that script's `sane()` display encoding. That encoding is LOSSY: a path
  # holding a real newline and one holding the literal characters `\n` print identically, so
  # two stores shared a throttle stamp AND a CORRUPT latch. Either direction is a defect —
  # one store suppresses the other's sweep, or one store's damage stops every lane working
  # on the other.
  #
  # So the identity now arrives already computed, from the only process that has the raw
  # bytes, and there is no lossy value left here to digest. See `store_key` in
  # check-object-store-integrity.sh for the tail/digest split and the three digest tools.
  #
  # IT IS STILL VALIDATED, because it is another process's stdout and not a fact: the shape
  # is checked rather than assumed, and anything else yields the EMPTY path — no throttle,
  # no latch, announced once by the caller — instead of a filename built out of whatever
  # arrived. A host with no digest tool prints no key line at all and lands here too.
  [[ "$key" =~ ^[A-Za-z0-9._-]{1,64}\.[0-9a-f]{16}$ ]] || return 0
  dir="/tmp"
  [[ -d "$dir" && -w "$dir" ]] || dir="${TMPDIR:-/tmp}"
  printf '%s' "${dir}/cqlite-object-store-sweep.${key}.stamp"
}

# obj_sweep_latch_path <stamp-path> — the STOP latch that belongs to that stamp. Derived
# from the stamp so the two always live in the same box-wide directory and a pinned
# OBJ_SWEEP_STAMP relocates both.
#
# ONE FILE, ONE QUESTION: "is this box stopped?" — AND ITS NAME CLAIMS NO CAUSE (#3749
# review round 10, item 1). It was `<stamp>.CORRUPT`, which was accurate while damage was
# the only stopping verdict. It is not any more: a sweep that RAN and reproducibly DIED
# without finishing also stops the box, and it establishes NO cause — so a file called
# `.CORRUPT` would have been a confidently-wrong claim in the one artifact an operator
# finds on disk, and every message naming that path would have sent them to the re-clone
# remedy for a store nothing observed damage in. That is round 9's defect (a printed
# remedy that does not match what was measured) arriving through a file name.
#
# WHICH verdict was recorded is therefore CONTENT, read affirmatively by
# obj_sweep_latch_verdict, and it is safe as content for a reason the throttle stamp's
# verdict field was not: this file is CREATE-ONLY, so its bytes are written once by the
# lane that wins the create and no later writer can move them (see obj_sweep_set_latch).
# TWO FILES WERE THE OTHER OPTION and were rejected: the latch question is answered in
# four values already, and asking it of two paths multiplies those states (present-here,
# present-there, both, one-unreadable) for no gain — a lane reading either must stop.
obj_sweep_latch_path() {
  local stamp="$1"
  [[ -n "$stamp" ]] || return 0
  printf '%s.STOP' "$stamp"
}

# obj_sweep_latch_state <latch-path> — the latch question, answered in FOUR values on
# stdout: `present`, `absent`, `unknown` or `unkeyed`.
#
# WHY IT IS NOT A FILE PREDICATE (CLAUDE.md's standing rule, #3749 review round 5 item 1).
# Every `test`/`[` file predicate is TWO-valued, so it has to collapse "cannot tell" onto
# one of its two answers — and it always picks the permissive one. `[[ -e "$latch" ]]` is
# FALSE both for a latch that is genuinely absent and for a latch this process cannot look
# at, and reading the second as the first is a fail-open on the one file whose entire job
# is to stop this box.
#
#   present — something is at that path. `-e || -L`, because a DANGLING symlink is
#             `-e`-false and the fail-safe reading of "something is at the latch path" is
#             LATCHED. Same idiom, same reason, as the supervisor lock's existence test.
#   absent  — nothing is there AND the absence was ESTABLISHED THROUGH SEARCHABLE
#             ANCESTORS, so it is a MEASUREMENT and not a failure to look.
#             SEARCH (`-x`) and not READ (`-r`) is the right test, deliberately: `-e` on a
#             named path needs only search permission on the parent, so a searchable but
#             unlistable directory gives a TRUE answer and requiring `-r` as well would
#             refuse a probe that was in fact valid — a false stop bought for nothing.
#   unknown — a latch may be sitting there unread, because some directory on the way to it
#             exists and this process cannot search it. NEVER folded into `absent`.
#
# AND `-d` IS ITSELF A TWO-VALUED PREDICATE, WHICH IS THE SAME TRAP ONE LEVEL UP (#3749
# review round 10, item 2). The first version answered `absent` whenever `[[ ! -d "$dir" ]]`
# — reading "the holding directory does not exist" off a test that is ALSO false when an
# ANCESTOR of that directory is not searchable. On a box where `/tmp/x` is mode 000 and the
# latch would live at `/tmp/x/y/stamp.STOP`, `-d /tmp/x/y` is false, and a latch that may
# well be sitting there was reported as an affirmative `absent` — bypassing the fail-closed
# `unknown` state built for exactly this. So absence is established by WALKING UP to the
# deepest ancestor this process can actually stat, and it counts only when that ancestor is
# SEARCHABLE: with a searchable ancestor, a stat of its child gives a true answer (and so
# does each stat below it, by induction), so a component that fails `-d` beneath one
# genuinely is not there. With an UNSEARCHABLE one, nothing below it is observable and the
# answer is `unknown`.
#   unkeyed — there is no latch PATH at all, because the box-wide key could not be
#             derived. A different statement from all three above: it is not that the file
#             is unreadable, it is that no file was ever named. The caller decides, and
#             says why at the branch.
obj_sweep_latch_state() {
  local latch="$1" dir probe parent
  [[ -n "$latch" ]] || {
    printf 'unkeyed'
    return 0
  }
  if [[ -e "$latch" || -L "$latch" ]]; then
    printf 'present'
    return 0
  fi
  dir="${latch%/*}"
  # No slash at all: the holding directory is the CWD, which is what a bare `-e` would
  # have resolved the name against.
  [[ "$dir" == "$latch" ]] && dir="."
  # A latch directly under the root: `${latch%/*}` strips to the empty string.
  [[ -n "$dir" ]] || dir="/"
  # Walk up to the deepest ancestor that can be STATTED. Each step strictly shortens the
  # path or breaks, so this terminates; `.` is the implicit parent of a relative
  # single-component path, and `/` is its own parent, which is where the walk ends.
  probe="$dir"
  while [[ ! -d "$probe" ]]; do
    parent="${probe%/*}"
    [[ -n "$parent" ]] || parent="/"
    [[ "$parent" == "$probe" ]] && parent="."
    [[ "$parent" != "$probe" ]] || break
    probe="$parent"
  done
  # Not even the end of that walk is a statable directory: nothing was established, so the
  # answer is `unknown` and not an absence.
  if [[ ! -d "$probe" ]]; then
    printf 'unknown'
    return 0
  fi
  if [[ -x "$probe" ]]; then
    printf 'absent'
    return 0
  fi
  printf 'unknown'
}

# obj_sweep_latch_present <latch-path> — is the latch in place? The affirmative half of
# obj_sweep_latch_state, kept as its own name because obj_sweep_set_latch asks exactly
# this question (did the file end up there?) and nothing else.
obj_sweep_latch_present() {
  local latch="$1"
  [[ "$(obj_sweep_latch_state "$latch")" == present ]]
}

# obj_sweep_write_stamp <path> — record WHEN this box last swept. One line, the epoch.
#
# `2>/dev/null` PRECEDES the output redirection deliberately: bash applies redirections
# LEFT TO RIGHT, so with the old order a failed `>"$stamp"` printed bash's own
# UNANCHORED error before the suppression took effect, in addition to the intended log
# line.
obj_sweep_write_stamp() {
  local path="$1"
  [[ -n "$path" ]] || return 0
  printf '%s\n' "$(date +%s)" 2>/dev/null >"$path" ||
    log "object-store sweep: could not write the throttle stamp $path — the sweep will re-run next iteration"
  return 0
}

# obj_sweep_force_stamp_stale <path> — make the throttle stamp read as EXPIRED, so no lane
# on this box can throttle past a CORRUPT verdict that could NOT be latched (#3749 review
# round 9, item 1).
#
# THE DEFECT IT EXISTS FOR. The CORRUPT branch wrote the throttle stamp unconditionally,
# INCLUDING on the path where the latch could not be persisted (a stamp that is writable
# inside a directory that is not — so the create of `<stamp>.CORRUPT` fails while the
# rewrite of `<stamp>` succeeds). The detecting lane stopped, correctly; its peers then
# read that FRESH stamp, skipped their own sweep for the whole interval, and kept spawning
# workers over a store already confirmed damaged. That is round 1's harm surviving in the
# one branch that never got round 1's treatment: the latch is what a peer must not
# throttle past, and where there is no latch the ONLY thing that stops a peer is a stamp
# it reads as expired.
#
# WHY IT FORCES RATHER THAN MERELY LEAVING THE STAMP ALONE. Not writing would be enough
# for the stamp THIS lane read as stale — but a peer whose own sweep started earlier can
# finish DURING this one and write a fresh timestamp, and then "leave it alone" throttles
# every lane for the full interval on that peer's stamp. `0` is an affirmative statement
# that this box's last sweep is to be treated as expired, and obj_sweep_stamp_is_fresh
# reads it as expired for any plausible clock.
#
# IT CANNOT MANUFACTURE A FALSE CLEAN. Its only effect is MORE sweeping: every lane
# re-measures the store, reproduces the damage and stops on its own finding. The cost is a
# repeated sweep per lane per iteration until an operator repairs the store, which is the
# correct price for a corruption verdict that has nowhere durable to live.
obj_sweep_force_stamp_stale() {
  local path="$1"
  [[ -n "$path" ]] || return 0
  if printf '0\n' 2>/dev/null >"$path"; then
    log "object-store: the throttle stamp $path has been FORCED STALE (epoch 0) because the CORRUPT verdict could not be latched — every lane on this box will re-sweep, reproduce the damage and stop on its own finding rather than throttling past it (#3749)."
  else
    log "object-store: the throttle stamp $path could NEITHER be advanced nor forced stale, so peer lanes will throttle on whatever it already holds. If it is fresh they will skip their own sweep for up to ${OBJ_SWEEP_INTERVAL_HOURS}h over a store found DAMAGED: stop every lane on this box by hand (#3749)."
  fi
  return 0
}

# ---------------------------------------------------------------------------
# THE IN-PROGRESS CLAIM: ONE SWEEP PER BOX AT A TIME (#3749 review round 5, item 2 —
# raised as a Low in round 1, recorded as "not disposed of" in round 2, returned as a
# Medium with a consequence attached in round 5).
#
# THE DEFECT. The throttle read and the sweep invocation are unsynchronised, and the stamp
# is written at the END of a sweep — so when the interval expires EVERY lane on the box
# reads the same stale stamp and starts its own full-store `git fsck`. On a four-lane box
# that is four concurrent rehashes of one 366M store, and the consequence is not merely
# CPU: the walks are I/O-bound (17-19s of user time inside an 80s wall), so contention
# pushes them toward the per-walk bound — and an expired bound is UNMEASURED. That is a
# CORRELATED loss of the measurement on every lane at once, which is the state this whole
# feature exists to prevent.
#
# THE CLAIM IS A DIRECTORY CREATED WITH `mkdir`: the same kernel-arbitrated create-only
# primitive as the CORRUPT latch, for the same reason — exactly one caller can win and
# there is no read-modify-write to serialise. NOT `flock`: this file's single-instance lock
# is an atomic mkdir + pid-liveness precisely because MACOS SHIPS NO `flock(1)` (see the
# SUPERVISOR_LOCK header), and a second locking mechanism in one file is a second set of
# failure modes.
#
# A LOSER DOES NOT WAIT. It skips this iteration's sweep and carries on, after the usual
# latch read: the sweep is a 6-hourly hygiene probe, so "a peer is doing it right now" is a
# complete answer, and waiting would put the box's whole spawn path behind one fsck.
# ---------------------------------------------------------------------------

# obj_sweep_claim_path <stamp-path> — the claim that belongs to that stamp. Derived from
# the stamp, so it is keyed on the SHARED STORE (the resource being contended) and a pinned
# OBJ_SWEEP_STAMP relocates the stamp, the latch and the claim together.
obj_sweep_claim_path() {
  local stamp="$1"
  [[ -n "$stamp" ]] || return 0
  printf '%s.sweeping' "$stamp"
}

# obj_sweep_claim_stale_secs <sweep-script> — the age at which a claim may be TAKEN OVER;
# nothing (exit 1) when it cannot be derived.
#
# DERIVED, NOT A CONSTANT, BECAUSE THE HAZARD IS REAL IN BOTH DIRECTIONS. A lane killed
# mid-sweep leaves its claim behind, and with no recovery no lane on the box ever sweeps
# again — strictly worse than the herd. But a threshold shorter than a legitimate sweep
# would let a peer take over a claim whose sweep is still running, which is the herd back
# again with a stolen claim on top. So: one invocation spends at most MAX_SWEEP_WALKS
# bounded walks — DECLARED in the sweep script and READ FROM IT here rather than re-typed
# (round 4's lesson: a relation whose own constant is restated in its consumer is two magic
# numbers wearing a relation's clothes) — and each walk is bounded by the
# OBJ_SWEEP_TIMEOUT_SECS this supervisor passes. The earliest safe threshold is their
# PRODUCT, plus OBJ_SWEEP_CLAIM_SLACK_SECS for what is not inside a walk. With the shipped
# defaults that is 3 x 200 + 60 = 660s.
#
# IF IT CANNOT BE DERIVED, NO CLAIM IS TAKEN AT ALL and the caller announces it once. That
# is the non-wedging direction: without a bound a stale claim could never be recovered, and
# an unrecoverable claim is worse than the herd it prevents, so the mechanism degrades to
# exactly the previous behaviour instead of inventing a number nobody measured.
obj_sweep_claim_stale_secs() {
  local script="$1" walks
  [[ -n "$script" && -r "$script" ]] || return 1
  walks="$({ grep -m1 '^MAX_SWEEP_WALKS=' "$script" || true; } 2>/dev/null)"
  walks="${walks#MAX_SWEEP_WALKS=}"
  walks="${walks%%[!0-9]*}"
  [[ "$walks" =~ ^[0-9]+$ ]] || return 1
  [[ "$walks" -ge 1 ]] || return 1
  printf '%s' "$((walks * OBJ_SWEEP_TIMEOUT_SECS + OBJ_SWEEP_CLAIM_SLACK_SECS))"
}

# obj_sweep_claim_acquire <claim-dir> <stale-secs> — try to become the one lane sweeping
# this store. Sets OBJ_SWEEP_CLAIM_STATE to exactly one of:
#
#   acquired    — this lane created the claim and owns it
#   taken       — this lane RECOVERED a stale claim and owns it
#   held        — a peer lane is sweeping; this lane must skip
#   unavailable — no claim could be taken at all (no key, or no derivable recovery bound),
#                 so the caller sweeps unserialised exactly as it did before this existed
#
# IT SETS GLOBALS RATHER THAN PRINTING, deliberately: a command substitution would run it
# in a SUBSHELL, and OBJ_SWEEP_CLAIM_OWNED — the registration the EXIT trap reads — would
# be lost with that subshell. The registration must happen in THIS shell, on the line after
# the create, or a lane killed a moment later leaves a claim its own exit will not release.
obj_sweep_claim_acquire() {
  local claim="$1" stale="$2" now started age
  OBJ_SWEEP_CLAIM_STATE=unavailable
  [[ -n "$claim" ]] || return 0
  [[ "$stale" =~ ^[0-9]+$ ]] || return 0
  now="$(date +%s)"
  if mkdir "$claim" 2>/dev/null; then
    OBJ_SWEEP_CLAIM_OWNED="$claim"
    obj_sweep_claim_mark_owner "$claim"
    obj_sweep_claim_mark_started "$claim" "$now"
    OBJ_SWEEP_CLAIM_STATE=acquired
    return 0
  fi
  # Not ours, and not creatable. If there is no directory there at all, this is not
  # contention — it is a name we cannot use (an unwritable parent, a file in the way) — and
  # claiming nothing is the honest answer rather than reading it as a peer.
  [[ -d "$claim" ]] || return 0
  # THE AGE COMES FROM A FILE THIS CODE WROTE, NOT FROM mtime. `stat` is GNU-vs-BSD
  # incompatible (the same reason bootstrap verifies a mode with `find -perm`), and the
  # throttle stamp beside it already records an epoch this way.
  started=""
  { read -r started || true; } <"$claim/started" 2>/dev/null || true
  if [[ "$started" =~ ^[0-9]+$ ]] && [[ "$started" -le "$now" ]]; then
    age=$((now - started))
    if [[ "$age" -le "$stale" ]]; then
      OBJ_SWEEP_CLAIM_STATE=held
      return 0
    fi
  fi
  # NO PARSEABLE START TIME — OR ONE IN THE FUTURE — COUNTS AS STALE, and the direction is
  # deliberate. Its cause is a lane killed in the microseconds between the `mkdir` and the
  # `started` write (or a clock that moved), and the alternative reading — "keep it" —
  # wedges the box's sweep FOREVER on a file nobody can age. Cost of this direction: if a
  # peer is taken over inside that microsecond window, two lanes sweep once. One extra
  # sweep is recoverable; an unrecoverable claim is not. The throttle stamp treats a future
  # timestamp the same way, for the same reason.
  #
  # `mv` THEN `mkdir`: rename(2) is atomic, so of N lanes finding one stale claim exactly
  # ONE can move it aside — and the `mkdir` that follows is the arbiter of ownership
  # anyway, so a lane whose rename lost still ends up `held` rather than sweeping beside
  # the winner.
  mv "$claim" "$claim.stale.$$" 2>/dev/null || true
  rm -rf "$claim.stale.$$" 2>/dev/null || true
  if mkdir "$claim" 2>/dev/null; then
    OBJ_SWEEP_CLAIM_OWNED="$claim"
    obj_sweep_claim_mark_owner "$claim"
    obj_sweep_claim_mark_started "$claim" "$now"
    OBJ_SWEEP_CLAIM_STATE=taken
    return 0
  fi
  OBJ_SWEEP_CLAIM_STATE=held
  return 0
}

# obj_sweep_claim_mark_started <claim-dir> <epoch> — record WHEN the sweep this claim
# represents began, which is the only thing that makes the claim recoverable. A failed
# write is not fatal and is not silent: the claim still serialises, and a peer will read it
# as unageable and therefore stale, which is the recoverable direction.
obj_sweep_claim_mark_started() {
  local claim="$1" epoch="$2"
  printf '%s\n' "$epoch" 2>/dev/null >"$claim/started" ||
    log "object-store sweep: could not record a start time in the sweep claim $claim — a peer will read it as unageable and may sweep beside this lane (#3749)"
  return 0
}

# obj_sweep_claim_mark_owner <claim-dir> — write THIS process's ownership token into the
# claim it has just created, and remember it in OBJ_SWEEP_CLAIM_TOKEN.
#
# WHY THE CLAIM CARRIES AN OWNER AT ALL (#3749 review round 11, item 2). The release used
# to `rm -rf` the claim PATH unconditionally, and a path is not an identity: round 5 gave
# this claim a stale-recovery route, so the directory at that path may be a SUCCESSOR's
# claim by the time the original owner releases — a lane whose sweep overran the staleness
# bound, or one on the `unavailable` path that never owned the claim at all and still ran
# the release at the end of its own sweep. Deleting it then permits a second concurrent
# full-store fsck, which is exactly the herd the claim exists to prevent.
#
# THE TOKEN IS WRITTEN BEFORE `started`, and that ordering is the one property worth
# having: `started` is what makes a claim AGEABLE by a peer, so writing the owner first
# means a claim a peer can age is a claim that also names its owner. The only way to get
# `started` without `owner` is a write failure, which is reported.
#
# IT IS NOT A SECURITY BOUNDARY. `$$` plus the epoch plus `$RANDOM` distinguishes
# concurrent and successive lanes on one box, which is the entire question here; a peer
# that wanted to forge it could simply write the file, and a same-host peer able to do
# that is invoker-class (#3312's triage rule).
obj_sweep_claim_mark_owner() {
  local claim="$1"
  OBJ_SWEEP_CLAIM_TOKEN="$$.$(date +%s).${RANDOM}${RANDOM}"
  printf '%s\n' "$OBJ_SWEEP_CLAIM_TOKEN" 2>/dev/null >"$claim/owner" || {
    # THE TOKEN IS DROPPED, NOT KEPT, when it could not be recorded: keeping it would make
    # every later ownership test answer `unknown` for a claim this lane does own, and the
    # honest state is "this lane cannot prove ownership of that claim". The consequence is
    # named: this lane will not delete it, so peers wait out the staleness bound once.
    OBJ_SWEEP_CLAIM_TOKEN=""
    log "object-store sweep: could not record an ownership token in the sweep claim $claim — this lane will NOT remove that claim when it finishes (it cannot prove the claim is still its own), so peer lanes will skip their sweep until the claim ages past the recovery bound (#3749)"
  }
  return 0
}

# obj_sweep_claim_owner_state <claim-dir> — FOUR-VALUED, on stdout:
#   ours     — the token in that claim is the one THIS process wrote (affirmative)
#   other    — a token was read and it is NOT ours: a successor owns this claim
#   gone     — there is no claim at that path (nothing to release)
#   unknown  — the token could not be read while something IS at that path
# Only `ours` licenses a delete. `other`, `gone` and `unknown` are all non-deleting, so a
# probe that fails cannot destroy a peer's claim — the cost of that direction is a claim
# left behind, which round 5's stale recovery already handles.
obj_sweep_claim_owner_state() {
  local claim="$1" tok=""
  [[ -n "$claim" ]] || { printf 'gone'; return 0; }
  { read -r tok || true; } <"$claim/owner" 2>/dev/null || tok=""
  if [[ -n "$tok" && -n "$OBJ_SWEEP_CLAIM_TOKEN" && "$tok" == "$OBJ_SWEEP_CLAIM_TOKEN" ]]; then
    printf 'ours'
    return 0
  fi
  if [[ -n "$tok" ]]; then
    printf 'other'
    return 0
  fi
  # NO TOKEN READ. Distinguish "the claim is not there" from "it is there and its token
  # could not be read" — same answer (do not delete), different journal line, and the
  # unreadable case is the one worth seeing. `-e` is asked SECOND and only to choose the
  # message, so its own two-valuedness cannot make a delete happen.
  if [[ -e "$claim" || -L "$claim" ]]; then
    printf 'unknown'
  else
    printf 'gone'
  fi
  return 0
}

# obj_sweep_claim_release <claim-dir> — give the claim up, IF IT IS STILL OURS. Called BOTH
# from the sweep path (so the next interval is contended honestly) AND from the EXIT trap
# (so a lane that stops mid-iteration does not make its peers wait out the staleness
# bound).
#
# WHAT THIS GUARANTEES, AND WHAT IT DOES NOT (#3749 review round 11, item 2). It removes
# the claim only after reading an ownership token that is this process's own, so the
# "delete a successor's claim" route is closed for every case except one: the read and the
# `rm` are two operations, so a takeover landing BETWEEN them is still deleted. That window
# is microseconds wide and needs a claim to be taken over in it; the previous behaviour was
# to delete unconditionally, always. THIS IS NOT ATOMIC and is not claimed to be — a
# rename-then-verify would make the removal atomic but would move a successor's claim
# aside before it could be checked, and restoring it is not atomic either (and `mv` onto an
# existing directory moves INTO it). The harm bound is unchanged from the claim's own
# design: a wrongly-removed claim costs a second concurrent 6-hourly probe, never a wrong
# verdict.
obj_sweep_claim_release() {
  local claim="$1" state
  [[ -n "$claim" ]] || return 0
  state="$(obj_sweep_claim_owner_state "$claim")"
  case "$state" in
    ours)
      rm -rf -- "$claim" 2>/dev/null || true
      OBJ_SWEEP_CLAIM_TOKEN=""
      ;;
    other)
      log "object-store sweep: NOT removing the sweep claim $claim — it carries another lane's ownership token, so this claim was taken over after this lane acquired it (its sweep overran the recovery bound, or this lane never owned it). Removing it would permit a second concurrent full-store fsck (#3749)."
      ;;
    unknown)
      log "object-store sweep: NOT removing the sweep claim $claim — its ownership token could not be read, so this lane cannot tell its own claim from a successor's. Leaving it for the staleness bound to recover (#3749)."
      ;;
  esac
  # THE REGISTRATION IS CLEARED FOR EVERY STATE EXCEPT `unknown`, where the read itself may
  # have failed transiently and the EXIT trap gets one more attempt. `other` and `gone` are
  # settled answers: there is nothing at that path for this lane to release.
  if [[ "$state" != unknown ]]; then
    [[ "$OBJ_SWEEP_CLAIM_OWNED" != "$claim" ]] || OBJ_SWEEP_CLAIM_OWNED=""
  fi
  return 0
}

# obj_sweep_stamp_is_fresh <stamp-path> — 0 when this box swept inside the interval.
#
# ONE implementation, because the throttle is now read TWICE: once before contending for
# the claim, and again after winning it — the winner of a race may have finished sweeping
# and written the stamp while this lane was deciding, and re-reading is what stops the
# claim from converting a herd into a queue of redundant sweeps.
#
# A stamp in the FUTURE (clock skew, a hand-edited file, a restored snapshot) must not park
# the sweep forever: it reads as never-swept. It does NOT clear the CORRUPT latch, which is
# a fact about the store rather than about when it was last measured.
obj_sweep_stamp_is_fresh() {
  local stamp="$1" now last=0 ts=""
  [[ -n "$stamp" && -r "$stamp" ]] || return 1
  now="$(date +%s)"
  { read -r ts || true; } <"$stamp" 2>/dev/null || true
  [[ "$ts" =~ ^[0-9]+$ ]] && last="$ts"
  [[ "$last" -gt "$now" ]] && last=0
  [[ $((now - last)) -lt $((OBJ_SWEEP_INTERVAL_HOURS * 3600)) ]]
}

# obj_sweep_set_latch <latch-path> — record MONOTONICALLY that this box's shared object
# store has been found damaged. Exits 0 when the latch is in place AFTERWARDS (whether
# this call created it or found a peer's), 1 when it could not be persisted at all.
#
# WHY A SEPARATE CREATE-ONLY FILE AND NOT A VERDICT FIELD IN THE STAMP — THE DESIGN CALL
# (#3749 review round 2, BLOCKER 1). Round 1 put the verdict IN the throttle stamp, and
# the stamp is a mutable shared cell that every lane rewrites at the END of its own
# sweep. Stamp writes are unsynchronised, so a peer whose sweep STARTED before this lane
# detected corruption can finish AFTER it and overwrite `CORRUPT` with `VERIFIED` or
# `UNMEASURED`; the other lanes then throttle on that fresh non-corrupt stamp and keep
# working against a store already known to be damaged. That is round 1's own harm coming
# back through a different door, and it is the SECOND consecutive review finding in this
# one shared cell.
#
# A LOCK WAS THE OTHER OPTION AND WAS REJECTED. It makes the read-modify-write atomic,
# but the value stays a mutable cell any writer can move BACKWARDS, so "CORRUPT is
# sticky" would rest on every present and future writer remembering to honour it — plus
# a lock has its own could-not-acquire path, which must then not silently skip the
# latch. CLAUDE.md's standing ruling for this family is to REMOVE THE SHARED MUTABLE
# CHANNEL rather than to serialise access to it, so the latch is its own file and its
# only state transition is ABSENT -> PRESENT:
#
#   * `set -C` (noclobber) makes `>` FAIL on an existing path instead of truncating it,
#     and create with O_EXCL on a missing one. The kernel arbitrates; a losing writer
#     cannot overwrite the winner, and there is no read-modify-write to serialise.
#   * It is set in a SUBSHELL so the option cannot leak into the rest of this script.
#   * NOTHING HERE EVER REMOVES IT. Corruption is non-self-clearing, so it does not
#     expire either; the operator who repairs the store removes the file, and every
#     message that mentions the latch names that exact command.
#
# THE LATCH IS THE FILE'S EXISTENCE, ASKED AFTERWARDS — never this call's own exit
# status, which cannot tell "a peer got there first" (fine, the box is latched) from
# "the directory is unwritable" (not fine, and the caller must say so out loud).
#
# THE SECOND ARGUMENT IS THE STOPPING VERDICT, FROM A CLOSED SET OF TWO, AND AN
# UNRECOGNISED VALUE CREATES NOTHING (#3749 review round 10, item 1). A latch whose
# content a reader cannot recognise can only be reported as a cause-free stop, so writing
# one would record a fact nobody can act on; refusing instead makes the caller take its
# own could-not-persist path, which is journalled and forces the throttle stamp stale.
# Validated here rather than at the two call sites, so a third caller cannot invent a
# token, and asserted structurally by the suite.
obj_sweep_set_latch() {
  local latch="$1" verdict="${2:-}"
  [[ -n "$latch" ]] || return 1
  case "$verdict" in
    CORRUPT | UNSWEEPABLE) ;;
    *) return 1 ;;
  esac
  (
    set -C
    printf '%s\n%s\n' "$(date +%s)" "$verdict" >"$latch"
  ) 2>/dev/null || true
  obj_sweep_latch_present "$latch"
}

# obj_sweep_latch_verdict <latch-path> — WHICH stopping verdict the latch records, on
# stdout: `CORRUPT`, `UNSWEEPABLE`, or `UNRECOGNISED`.
#
# AFFIRMATIVE RECOGNITION, AND NOTHING ELSE (CLAUDE.md: a positive verdict requires an
# affirmative measurement). Only a second line that IS one of the two tokens is reported
# as that token. Anything else — an empty file, a dangling symlink, a latch written by a
# newer supervisor, a truncated write — is `UNRECOGNISED`, which its caller reports as a
# cause-free stop. It is never guessed at and never defaulted to CORRUPT: that would name
# a cause the box may not have, which is the whole reason the file is no longer called
# `.CORRUPT`.
obj_sweep_latch_verdict() {
  local latch="$1" line
  line="$(sed -n '2p' "$latch" 2>/dev/null || true)"
  case "$line" in
    CORRUPT) printf 'CORRUPT' ;;
    UNSWEEPABLE) printf 'UNSWEEPABLE' ;;
    *) printf 'UNRECOGNISED' ;;
  esac
}

# obj_sweep_stop_if_latched <latch-path> — ASK THE LATCH QUESTION AND STOP UNLESS THE
# ANSWER IS AN AFFIRMATIVE "no". Returns 0 (carry on) only for `absent` and `unkeyed`;
# never returns at all for `present` or `unknown`, because finalize_exit ends the process.
#
# IT IS CALLED AT THE TOP OF object_store_sweep, BEFORE EVERY BRANCH, AND AGAIN BEFORE
# EVERY RETURN THAT LEADS TO A SPAWN (#3749 review rounds 3 and 5, item 1). Round 3 found
# THREE returns that reached a spawn without re-reading the latch and fixed all three;
# round 5 found a FOURTH — the OBJ_SWEEP_INTERVAL_HOURS=0 opt-out, which returned before
# any latch read at all, so the documented way to disable the sweep also disabled an
# already-recorded CORRUPT verdict. That is ONE defect arriving through a new door each
# round, because the shape required every early return to independently remember the
# check. So the ENTRY read is hoisted above every branch in object_store_sweep: the
# invariant is now "this function cannot be ENTERED without a latch read", which holds for
# branches nobody has written yet, and the post-sweep reads stay because a PEER can latch
# the box WHILE this lane sweeps (up to MAX_SWEEP_WALKS fsck walks).
#
# `unknown` STOPS, AND HERE IS THE REASONING AT THE BRANCH. It means the directory holding
# the latch exists and this process cannot search it, so a CORRUPT verdict may be sitting
# there unread. The two directions are not symmetric: a false stop is ONE lane exiting with
# a named, actionable reason an operator fixes in seconds, while a false carry-on is
# workers certifying merges against a store already known to be damaged. It is also not a
# plausible self-DoS — that directory is the same box-wide `/tmp` this supervisor puts its
# own flock in, so a box on which it is unsearchable cannot get this far anyway. It is
# reported as its OWN reason (`object-store-latch-unreadable`), never as CORRUPT: nothing
# here observed damage, and claiming it would send an operator to re-clone a healthy store.
#
# `unkeyed` CARRIES ON, AND THAT IS THE ONE PERMISSIVE ANSWER (CLAUDE.md #3229: where a
# signal genuinely SHOULD be permissive, record the why at the branch). No key means no
# latch was ever NAMED — and the writer derives the name through the same resolver, so on
# the two box-wide causes (the store cannot be resolved; this host has no digest tool) no
# lane here could have recorded one either. It is announced once and it costs this lane its
# throttle, so it re-measures the store itself every iteration and would rediscover damage
# rather than inherit it. What it does NOT cover is a TRANSIENT resolver failure in THIS
# lane while a peer keyed and latched successfully: that lane misses the peer's verdict for
# one iteration. Stopping instead would make a missing or older `check-object-store-
# integrity.sh` — an ordinary state on any branch cut before #3749 merged — halt every lane
# on the box for want of a hygiene probe, which is the self-DoS CLAUDE.md rules out.
obj_sweep_stop_if_latched() {
  local latch="$1" latch_ts state latch_verdict stop_reason
  state="$(obj_sweep_latch_state "$latch")"
  case "$state" in
    absent)
      return 0
      ;;
    unkeyed)
      if [[ "$OBJ_SWEEP_NOSTAMP_NOTIFIED" -eq 0 ]]; then
        log "object-store sweep: no box-wide key for this store — the shared store could not be resolved, or this host has no sha256 digest tool. There is no throttle and NO LATCH: a CORRUPT verdict recorded by a peer cannot be read here, and one found here cannot be recorded for peers. The sweep runs every iteration instead (#3749)."
        OBJ_SWEEP_NOSTAMP_NOTIFIED=1
      fi
      return 0
      ;;
    unknown)
      notify "high" "worker-supervisor: object-store STOP latch UNREADABLE" \
        "the directory holding this box's object-store STOP latch ($latch) exists and cannot be searched, so a recorded stopping verdict may be sitting there unread. Stopping this lane rather than spawning a worker on an unknown store. This is NOT a corruption finding."
      log "object-store: the STOP latch state could NOT be read at $latch (its directory exists and is not searchable) — stopping. UNKNOWN is not clean: a peer's verdict may be unread. This is NOT a corruption finding; fix the directory's permissions (#3749)."
      finalize_exit "object-store-latch-unreadable" 1
      ;;
  esac
  latch_ts="$(head -1 "$latch" 2>/dev/null || true)"
  [[ "$latch_ts" =~ ^[0-9]+$ ]] || latch_ts="<unrecorded>"
  # WHICH STOPPING VERDICT WAS RECORDED DECIDES WHAT THIS LANE SAYS, AND IT IS READ
  # AFFIRMATIVELY (#3749 review round 10, item 1). This lane did NOT sweep — that is the
  # point of the latch — so everything it can say about the store comes from the file. A
  # latch recording that the store could not be SWEPT must not be reported as damage: the
  # detecting sweep established no cause, and repeating a claim it did not make would send
  # an operator to re-clone a store nothing observed damage in (round 9's defect class).
  # An unrecognised token is a cause-free stop, never a default to the damage text.
  latch_verdict="$(obj_sweep_latch_verdict "$latch")"
  case "$latch_verdict" in
    CORRUPT)
      notify "high" "worker-supervisor: SHARED OBJECT STORE CORRUPT (latched)" \
        "a sweep on this box recorded CORRUPT for the shared git object store every lane here reads. Stopping without re-sweeping. Repair the store, then re-run the sweep and require its affirmative verdict BEFORE removing $latch."
      log "object-store: CORRUPT (cached at $latch_ts by a sweep on this box) — stopping; no worker may certify against a damaged shared store (#3749)."
      stop_reason="object-store-corrupt"
      ;;
    UNSWEEPABLE)
      # THE LATCH RECORDS THE TOKEN AND NOTHING ELSE, so this text may only assert what is
      # true of EVERY cause of that token — and since #3749 review round 11 there are two
      # (git fsck ran and reproducibly died; or the store's own config sets fsck.* keys, so
      # no walk was run at all). The earlier wording named the first mechanism as though it
      # were the only one, which on the second cause is a confidently-wrong sentence in the
      # one place an operator reads when the box has stopped. What the reader knows is the
      # DISPOSITION, so that is all it says; the CAUSE is named by the sweep that recorded
      # it, in the journal of the lane that stopped.
      notify "high" "worker-supervisor: SHARED OBJECT STORE could not be SWEPT (latched)" \
        "a sweep on this box recorded that the shared git object store every lane here reads could NOT be swept to an affirmative verdict, so its integrity is UNKNOWN and NO damage was established. Stopping without re-sweeping. Run 'bash scripts/check-object-store-integrity.sh' by hand, act on what THAT run reports, and require its affirmative verdict BEFORE removing $latch."
      log "object-store: UNSWEEPABLE (cached at $latch_ts by a sweep on this box) — that sweep could not obtain an affirmative verdict for this store, so its object content is UNKNOWN, not clean. Stopping. NO damage was established: this is NOT a damage finding, and this reader knows only the verdict, not which of its causes fired (#3749)."
      stop_reason="object-store-unsweepable"
      ;;
    *)
      notify "high" "worker-supervisor: shared object store LATCHED (verdict unrecognised)" \
        "a sweep on this box latched the shared git object store with a stopping verdict this supervisor does not recognise (an older or newer check-object-store-integrity.sh, or a latch that could not be read). Stopping rather than guessing which verdict it was. Run 'bash scripts/check-object-store-integrity.sh' by hand and require its affirmative verdict BEFORE removing $latch."
      log "object-store: LATCHED with an UNRECOGNISED stopping verdict at $latch (cached at $latch_ts) — stopping. NO cause is claimed: this supervisor will not guess whether damage was found (#3749)."
      stop_reason="object-store-latch-unrecognised"
      ;;
  esac
  # THE REMEDY NAMES WHAT ACTUALLY REPAIRS THE STORE (#3749 review round 9, item 2). It
  # used to say "re-obtain the objects from the canonical remote", which read as
  # `git fetch --force origin` — the instruction the sweep itself used to print, and
  # measured to repair NOTHING: `--force` only permits non-fast-forward REF updates and
  # re-downloads no objects at all. This lane did NOT sweep (that is the point of the
  # latch), so it has no damage class and no fsck output to quote: for the damage verdict
  # it names the two things that are true of every damage class, for the others it names
  # nothing at all (see the branch), it points at the sweep for the class-specific text,
  # and it makes a successful sweep — not a belief — the condition for clearing the latch.
  if [[ "$latch_verdict" == CORRUPT ]]; then
    log "object-store: REMEDY — stop every lane on this box, then repair the shared store. 'git fetch --force origin' does NOT repair it (--force only permits non-fast-forward REF updates; it re-downloads no objects), and neither does a local 'git gc'/'git repack'. Run 'bash scripts/check-object-store-integrity.sh' for the measured remedy for the damage class it finds (#3749)."
  else
    # NO REPAIR INSTRUCTION WHERE NO CAUSE WAS ESTABLISHED. The two other verdicts this
    # latch can carry say the store could not be swept, or say something this supervisor
    # cannot recognise; naming a repair for either would be a second confidently-wrong
    # instruction, which is the class round 9 removed. The one thing that is true for all
    # of them is where to get the class-specific text: the sweep itself.
    log "object-store: REMEDY — stop every lane on this box, then run 'bash scripts/check-object-store-integrity.sh' by hand and act on what IT names. NO repair instruction is given here because the latched verdict established no cause, and a repair chosen for the wrong cause is worse than none (#3749)."
  fi
  log "object-store: CLEAR THE LATCH ONLY AFTER that sweep completes and reports its affirmative verdict — 'I think I fixed it' is not an exit condition: rm -f $latch"
  # PER-VERDICT, so the journal records WHICH stopping fact ended this lane rather than a
  # generic "it was latched" a reader would have to correlate by timestamp.
  finalize_exit "$stop_reason" 1
}

object_store_sweep() {
  local script stamp latch claim claim_stale rc out verdict stop_verdict
  # ---------------------------------------------------------------------------
  # THE ENTRY LATCH READ. IT IS ABOVE EVERY BRANCH IN THIS FUNCTION, AND THE LINES
  # BEFORE IT ARE ASSIGNMENTS ONLY — NO `if`, NO `return`, NOTHING THAT CAN LEAVE
  # (#3749 review round 5, item 1).
  #
  # THE HISTORY IS THE POINT. Round 3 found "a return path that reaches a spawn without
  # re-reading the latch" at THREE sites and fixed all three; round 5 found a FOURTH, the
  # OBJ_SWEEP_INTERVAL_HOURS=0 opt-out, which returned before any latch read — so the
  # documented way to switch the sweep OFF also switched off an already-recorded CORRUPT
  # verdict, contradicting "a latch ignores the interval". Patching the fourth site would
  # have left a fifth for the next reviewer, because the design required EVERY early
  # return to independently remember the check. The read is therefore hoisted here, ONCE,
  # at entry: "this function cannot be entered without a latch read" is a property of the
  # function rather than of a set of sites, and it holds for branches nobody has written
  # yet. Asserted STRUCTURALLY (test_worker_supervisor.sh, obj-sweep(entry-latch-read)):
  # the first control-flow line in this body must come AFTER this call, so a future early
  # return cannot be placed above it without reddening that case.
  #
  # THE POST-SWEEP READS STAY. They answer a DIFFERENT question — a peer can latch the box
  # WHILE this lane sweeps, which is up to MAX_SWEEP_WALKS fsck walks — so the entry read
  # does not subsume them (round 3, and its RED arm is still in the suite).
  #
  # THE COST IS ONE `--print-store` RESOLUTION PER ITERATION, MEASURED AT 5 ms on this
  # fleet's store (10 runs, 2026-09-02; round 2 measured 7 ms for the same call). The
  # opt-out path did not pay it before and now does. A supervisor iteration is minutes, so
  # this is not a trade worth making conditional: a cache would be one more piece of
  # process-scoped state on a question ("is this box latched?") whose whole value is that
  # it is asked FRESH.
  #
  # THE LATCH PATH CANNOT ALWAYS BE DERIVED, and that is a NAMED state rather than an
  # absent latch: obj_sweep_latch_state answers `unkeyed`, and obj_sweep_stop_if_latched
  # announces it once and carries on, with the reasoning — and the residual it leaves — at
  # that branch.
  # ---------------------------------------------------------------------------
  script="$REPO_ROOT/scripts/check-object-store-integrity.sh"
  stamp="$(obj_sweep_stamp_path "$script")"
  latch="$(obj_sweep_latch_path "$stamp")"
  obj_sweep_stop_if_latched "$latch"
  if [[ "$OBJ_SWEEP_INTERVAL_HOURS" -le 0 ]]; then
    # ANNOUNCED, never silent (the CLAIM_CMD-disabled precedent in main()): a hygiene
    # probe that is off must be visible in the journal rather than inferred from missing
    # lines.
    #
    # AND IT IS REACHED ONLY AFTER THE ENTRY LATCH READ ABOVE. Disabling the sweep means
    # "do not spend fsck walks on this box", never "ignore a verdict a sweep already
    # recorded": the latch is a fact about the store, not a schedule.
    if [[ "$OBJ_SWEEP_ANNOUNCED" -eq 0 ]]; then
      log "object-store sweep DISABLED (OBJ_SWEEP_INTERVAL_HOURS=0) — this box's SHARED git object store is NOT being rehashed this run; an existing CORRUPT latch is still honoured (#3749)"
      OBJ_SWEEP_ANNOUNCED=1
    fi
    return 0
  fi
  if [[ ! -r "$script" ]]; then
    # PERMISSIVE, AND HERE IS THE REASON, IN CODE (CLAUDE.md #3229: where a signal
    # genuinely SHOULD be permissive, record the why at the branch). The sweep is absent
    # from this checkout — an older branch, or a partial tree. Refusing to run any worker
    # because a hygiene probe is missing is a self-DoS on the whole fleet, and the probe
    # certifies nothing on the passing side either. Journalled once so it is visible.
    if [[ "$OBJ_SWEEP_ANNOUNCED" -eq 0 ]]; then
      log "object-store sweep UNAVAILABLE: no $script in this checkout — the shared store is NOT being rehashed (#3749). NOT treated as clean and NOT a stop reason."
      OBJ_SWEEP_ANNOUNCED=1
    fi
    return 0
  fi
  # THE STAMP AND LATCH WERE RESOLVED AT ENTRY, ABOVE, and the "no box-wide key" state is
  # announced there by obj_sweep_stop_if_latched's `unkeyed` branch — the one place that
  # can say it, because it is the branch that could not ask the latch question.
  # A LATCHED BOX STOPS THIS LANE WITHOUT RE-SWEEPING, AND IT IGNORES THE INTERVAL —
  # WHICH IS WHY THE READ THAT ENFORCES IT IS AT THE TOP OF THIS FUNCTION AND NOT HERE.
  #
  # THE DEFECT IT EXISTS FOR (#3749 review, BLOCKER A). The throttle is keyed on the
  # SHARED store and lives in a box-wide directory, so it is genuinely box-wide. When it
  # recorded only a timestamp, the lane that DETECTED corruption stopped — and its three
  # peers then saw a FRESH stamp, skipped their own sweep for the whole interval, and kept
  # spawning workers against a store known to be damaged. That is the exact harm this
  # feature exists to prevent, delivered by the throttle.
  #
  # THE LATCH IS A DIFFERENT FILE FROM THE STAMP, ON PURPOSE (round 2, BLOCKER 1): see
  # obj_sweep_set_latch for why a verdict field in the mutable stamp is not monotonic
  # under concurrency and why a lock was rejected. The consequence here is simply that the
  # test is an EXISTENCE test, which no concurrent writer can undo.
  #
  # IT DOES NOT EXPIRE, and it is CLEARED BY HAND. Corruption is non-self-clearing, so an
  # age-based expiry would resume workers over a still-damaged store; the operator who
  # repairs the store removes the file, and every message that names the latch names that
  # exact command, because a latch nobody can clear bricks the box after the repair.
  #
  # There is deliberately NO second read between the entry read and the throttle branch
  # below: nothing between them can change the latch except a peer, in the microseconds it
  # takes to stat one file, and the reads that DO cover a real window (this lane's own
  # multi-walk sweep) are the ones after it.
  if obj_sweep_stamp_is_fresh "$stamp"; then
    # RE-ASKED ON THE THROTTLED PATH. Cheap (one stat), and it keeps ONE rule —
    # "no return that leads to a spawn without a fresh latch read" — instead of a set of
    # paths someone has to remember to audit individually.
    obj_sweep_stop_if_latched "$latch"
    return 0
  fi

  # --- ONE SWEEP PER BOX AT A TIME (#3749 review round 5, item 2) -------------
  # The throttle alone cannot prevent the herd: the stamp is written at the END of a sweep,
  # so every lane reads the same stale stamp at the same moment and all of them start their
  # own full-store fsck. See the obj_sweep_claim_* header for the mechanism and for why the
  # loser skips instead of waiting.
  claim="$(obj_sweep_claim_path "$stamp")"
  claim_stale="$(obj_sweep_claim_stale_secs "$script" || true)"
  obj_sweep_claim_acquire "$claim" "$claim_stale"
  case "$OBJ_SWEEP_CLAIM_STATE" in
    held)
      log "object-store sweep: SKIPPED — a peer lane on this box holds the sweep claim ($claim), so the shared store is being rehashed by that lane right now. Not waiting, and not sweeping beside it: the sweep is a 6-hourly hygiene probe and one lane paying for it is the whole box's answer (#3749)."
      obj_sweep_stop_if_latched "$latch"
      return 0
      ;;
    taken)
      log "object-store sweep: RECOVERED a stale sweep claim ($claim, older than ${claim_stale}s = the sweep's own MAX_SWEEP_WALKS x this supervisor's per-walk bound + slack, so the sweep it represented cannot still be running) — the lane holding it was killed mid-sweep. Sweeping (#3749)."
      ;;
    unavailable)
      # ANNOUNCED ONCE, never inferred from missing lines: without a claim the herd is back,
      # and that is a property of the box (no box-wide key, or a sweep script whose
      # MAX_SWEEP_WALKS could not be read to derive a recovery bound), not of the iteration.
      if [[ "$OBJ_SWEEP_CLAIM_NOTIFIED" -eq 0 ]]; then
        log "object-store sweep: NO in-progress claim is being taken — there is no box-wide key for this store, or the sweep's own MAX_SWEEP_WALKS could not be read to derive a recovery bound. Every lane on this box may sweep at once when the interval expires (#3749)."
        OBJ_SWEEP_CLAIM_NOTIFIED=1
      fi
      ;;
  esac
  # RE-READ THE THROTTLE UNDER THE CLAIM, AND THAT SECOND READ IS WHAT MAKES THE CLAIM
  # WORTH TAKING. The winner of the race may have finished its sweep and written the stamp
  # between this lane's first read and its own acquire, so without this the claim would
  # merely convert N simultaneous sweeps into N sequential ones.
  if [[ "$OBJ_SWEEP_CLAIM_STATE" != unavailable ]] && obj_sweep_stamp_is_fresh "$stamp"; then
    log "object-store sweep: SKIPPED — a peer lane finished sweeping this store while this lane was taking the claim, so the box is inside the interval again (#3749)."
    obj_sweep_claim_release "$claim"
    obj_sweep_stop_if_latched "$latch"
    return 0
  fi
  rc=0
  out="$(bash "$script" --repo "$REPO_ROOT" --timeout "$OBJ_SWEEP_TIMEOUT_SECS" 2>&1)" || rc=$?
  # Read the verdict from the sweep's OWN anchored control line, never from loose text:
  # its output prints repository-controlled paths verbatim, so an unanchored match could
  # land on one. Both the line AND the exit status are required to AGREE for the two
  # actionable verdicts, and since #3749 review round 4 the CORRUPT branch really does
  # require both (it was `||`, while this comment already claimed the conjunction — the
  # false-rationale class, and the reason a reader would not have looked).
  # `{ grep || true; }` ON EVERY PIPELINE BELOW, AND IT IS LOAD-BEARING, NOT DEFENSIVE.
  # This file runs under `set -euo pipefail`: a `grep` that matches NOTHING exits 1, and
  # with `pipefail` that status becomes the pipeline's — so an ASSIGNMENT from such a
  # substitution, or a bare pipeline, KILLS THE SUPERVISOR with rc=1. TWO REACHABLE
  # CAUSES, and neither is a hypothetical (an earlier version of this comment named two
  # that are NOT reachable, which is worse than no comment because it is what stops the
  # next person looking):
  #   * `$script` IS WHATEVER THIS CHECKOUT HOLDS. An older, newer or stubbed sweep, or
  #     one killed from OUTSIDE this process (an operator, the OOM killer), prints no
  #     `verdict ` line and no `unmeasured-cause` line at all. This supervisor must not
  #     depend on the current output shape of a sibling script to stay alive.
  #   * `head -1`/`head -4`/`head -6` CLOSE THE PIPE EARLY, so grep dies of SIGPIPE (141)
  #     whenever there are more matching lines than the head takes — which is the ORDINARY
  #     case for the finding and cause pipelines. `|| true` INSIDE the brace group is what
  #     absorbs that status before `pipefail` can promote it.
  # Caught by scripts/tests/test_worker_supervisor.sh's obj-sweep(UNMEASURED) case, which
  # planted a verdict with no cause line and observed the whole loop exit 1.
  verdict="$(printf '%s\n' "$out" | { grep '^OBJECT-STORE: verdict ' || true; } | head -1)"
  verdict="${verdict#OBJECT-STORE: verdict }"
  verdict="${verdict%% *}"

  # THE CORRUPT BRANCH IS TESTED FIRST, AND IT CREATES THE LATCH BEFORE THE THROTTLE
  # STAMP IS WRITTEN (#3749 review round 3, item 1a).
  #
  # THE ORDERING DEFECT THIS FIXES. The stamp used to be written for every outcome
  # BEFORE this branch ran, so between that write and the latch create there was a
  # window in which a peer lane saw a FRESH, non-corrupt throttle stamp, skipped its own
  # sweep for the whole interval, and went on spawning workers over a store this lane
  # had already found damaged. The window was as long as it took to build the `findings`
  # string and create a file; it is now empty in that direction, because the latch — the
  # thing a peer must not throttle past — is in place before anything advertises that
  # this box has been swept.
  #
  # AND IT TAKES *BOTH*, WHICH IS ITEM 2 OF THE SAME REVIEW. With `||`, an exit 4 with no
  # verdict line — or a `CORRUPT` line under any other status — created a PERSISTENT,
  # BOX-WIDE latch that stops every lane on this box and can only be cleared by hand. An
  # unrecognised or incomplete sweep must not be able to do that: the blast radius of a
  # false latch (four lanes halted until someone notices) is worse than one more
  # iteration of a store whose damage, if real, the NEXT sweep reproduces and latches
  # properly. A disagreement therefore falls through to the UNMEASURED path below, which
  # is non-passing, journalled, and paged once — and it is named explicitly there, so the
  # signal is not swallowed on the way past.
  # ONE STOPPING PATH FOR BOTH STOPPING VERDICTS, AND THAT IS DELIBERATE (#3749 review
  # round 10, item 1). `UNSWEEPABLE` (the sweep RAN and reproducibly DIED without
  # finishing; see the sweep script's PASS 2 branch) has to stop this box for the same
  # reason CORRUPT does — a peer must not throttle past a durable finding — so it goes
  # through the SAME latch-then-stamp ordering rather than a second copy of it. Four
  # review rounds hardened that ordering (round 3's latch-before-stamp, round 4's
  # two-channel conjunction, round 9's stamp gated on a CONFIRMED latch); a copied branch
  # would inherit none of them, and the structural write-order assert reads THIS body.
  # Only the operator-facing text differs, and it differs where a `case` says so.
  stop_verdict=""
  if [[ "$rc" -eq 4 && "$verdict" == "CORRUPT" ]]; then
    stop_verdict=CORRUPT
  elif [[ "$rc" -eq 6 && "$verdict" == "UNSWEEPABLE" ]]; then
    stop_verdict=UNSWEEPABLE
  fi
  if [[ -n "$stop_verdict" ]]; then
    local findings latched=0 stop_headline stop_title stop_body stop_reason
    case "$stop_verdict" in
      CORRUPT)
        stop_headline="CORRUPT — stopping loudly; no worker may certify against a damaged shared store (#3749)."
        stop_title="worker-supervisor: SHARED OBJECT STORE CORRUPT"
        stop_body="git fsck reports damaged objects in this box's shared git object store — every lane here reads it, so NO gate verdict on this box can be trusted. Stopping."
        stop_reason="object-store-corrupt"
        ;;
      *)
        # ONE TOKEN, TWO CAUSES (#3749 review round 11, item 1): the fatal-death branch and
        # the refusal to walk a store whose own config sets fsck.* keys. This text is
        # derived from the TOKEN, so it asserts only the disposition; the sweep's own
        # verdict-detail lines are quoted verbatim below and they name the cause.
        stop_headline="UNSWEEPABLE — this box's shared store could NOT be swept to an affirmative verdict, so its object content is UNKNOWN, not clean. Stopping. NO damage was established: this is NOT a damage finding, and the sweep's own lines below say what it observed (#3749)."
        stop_title="worker-supervisor: SHARED OBJECT STORE could not be SWEPT"
        stop_body="the sweep could NOT obtain an affirmative verdict for this box's shared git object store — every lane here reads it, so NO gate verdict on this box can be trusted. Stopping. NO damage was established; act on what the sweep reported."
        stop_reason="object-store-unsweepable"
        ;;
    esac
    # LATCH FIRST, THEN EVERYTHING ELSE. A failure to persist it is REPORTED, never
    # silent: this lane stops either way, but without the latch the peers on this box
    # will re-run their own sweep and rediscover the damage rather than inheriting the
    # verdict, and an operator reading only this journal would otherwise never learn that.
    if [[ -z "$latch" ]]; then
      log "object-store: the $stop_verdict verdict could NOT be latched — this box's shared store could not be resolved, so there is no box-wide file to record it in. Peer lanes will re-sweep and rediscover it (#3749)."
    else
      obj_sweep_set_latch "$latch" "$stop_verdict" || true
      # CONFIRMED BY ASKING THE FILE AFTERWARDS, NEVER BY THE CREATE'S EXIT STATUS — the
      # same test obj_sweep_set_latch itself ends on, and for the same reason: a create's
      # status cannot tell "a peer got there first" (fine, the box IS latched) from "the
      # directory is unwritable" (not fine). One extra stat, and it makes the gate below
      # a property of the file rather than of a return value.
      if obj_sweep_latch_present "$latch"; then
        latched=1
      else
        log "object-store: the $stop_verdict verdict could NOT be latched at $latch (unwritable?) — peer lanes will re-sweep and rediscover it rather than inheriting this verdict (#3749)."
      fi
    fi
    # THE THROTTLE STAMP IS WRITTEN ONLY WHEN THE LATCH IS CONFIRMED IN PLACE (#3749
    # review round 9, item 1). A fresh stamp tells every peer on this box "somebody swept
    # recently, do not spend another fsck" — which is safe ONLY because the latch they
    # will also read stops them. With no latch there is nothing to stop them, so
    # advertising a swept box would buy six hours of silence over a store confirmed
    # damaged. See obj_sweep_force_stamp_stale for why the stamp is forced expired rather
    # than merely left alone.
    if [[ "$latched" -eq 1 ]]; then
      obj_sweep_write_stamp "$stamp"
    else
      obj_sweep_force_stamp_stale "$stamp"
    fi
    findings="$(printf '%s\n' "$out" | { grep -E '^OBJECT-STORE: (finding|object) ' || true; } | head -6 | tr '\n' ';' | cut -c1-600)"
    notify "high" "$stop_title" \
      "$stop_body ${findings:-<no findings captured>}"
    log "object-store: $stop_headline ${findings:-<no findings captured>}"
    # THE REMEDY IS QUOTED FROM THE SWEEP, NEVER RESTATED (#3749 review round 9, item 2).
    # This lane HAS the sweep's output, and the sweep knows which damage class it found —
    # the repair differs by class, and it was measured there. Restating it here is how the
    # wrong instruction ('git fetch --force origin', which re-downloads no objects and
    # repairs nothing) survived in three files at once.
    printf '%s\n' "$out" | { grep '^OBJECT-STORE: verdict-detail ' || true; } | head -40 | while IFS= read -r obj_line; do
      log "object-store: $obj_line"
    done
    # FAIL-CLOSED ON THE DIAGNOSTIC: an older, newer or stubbed sweep may print no
    # guidance, and a stopping lane must never leave an operator with none.
    if ! printf '%s\n' "$out" | grep -q '^OBJECT-STORE: verdict-detail REMEDY'; then
      log "object-store: REMEDY — this sweep printed no operator guidance (an older or stubbed check-object-store-integrity.sh). Stop every lane on this box and run 'bash scripts/check-object-store-integrity.sh' by hand for the measured remedy; 'git fetch --force origin' does NOT repair a damaged store and neither does a local 'git gc'/'git repack' (#3749)."
    fi
    # AND THE CLEARING INSTRUCTION IS PRINTED ONLY WHERE THERE IS SOMETHING TO CLEAR. On
    # the un-latchable path (above) no latch exists, and naming one would be a second
    # confidently-wrong instruction — the exact class item 2 removed — pointing an operator
    # at a file that is not there while the real state (peers will re-sweep) was logged
    # separately.
    if [[ "$latched" -eq 1 ]]; then
      log "object-store: CLEAR THE LATCH ONLY AFTER a re-run of that sweep reports its affirmative verdict — 'I think I fixed it' is not an exit condition: rm -f $latch"
    else
      log "object-store: there is NO latch to clear (this verdict could not be recorded box-wide) — repair the store, then re-run the sweep and require its affirmative verdict before resuming any lane (#3749)."
    fi
    finalize_exit "$stop_reason" 1
  fi

  # THE STAMP IS WRITTEN FOR EVERY OUTCOME. It bounds how often this box SPENDS the
  # sweep — a box that cannot measure (no timeout binary, say) must not re-attempt it
  # every iteration — and it says nothing about what was found. What a peer lane must
  # not throttle past is recorded separately, and monotonically, by the latch above.
  obj_sweep_write_stamp "$stamp"
  # AND ONLY THEN THE CLAIM, IN THAT ORDER: a peer that acquires the claim next reads the
  # stamp this lane just wrote and throttles, instead of paying for a sweep that has just
  # happened. The CORRUPT branch above does not reach this line — it ends the process — and
  # the EXIT trap releases the claim there, which is why the claim is registered in
  # OBJ_SWEEP_CLAIM_OWNED on the line after it is created rather than here.
  obj_sweep_claim_release "$claim"

  if [[ "$rc" -eq 0 && "$verdict" == "VERIFIED" ]]; then
    log "object-store: VERIFIED — $(printf '%s\n' "$out" | { grep '^OBJECT-STORE: measured ' || true; } | head -1)"
    # RE-ASKED AFTER THE SWEEP, BEFORE RETURNING TO THE SPAWN PATH. This lane's own walk
    # said VERIFIED, but it took up to MAX_SWEEP_WALKS fsck bounds to say it, and a PEER may have
    # latched the box in the meantime. The peer's finding is about the same store and is
    # strictly newer than this measurement, so it wins.
    obj_sweep_stop_if_latched "$latch"
    return 0
  fi
  # UNMEASURED (or no recognised verdict at all): REPORTED, and DELIBERATELY PERMISSIVE.
  # THE WHY, IN CODE (CLAUDE.md #3229). An UNMEASURED sweep is not clean — nothing here
  # reads it as clean, and it stops no worker from being spawned either, because refusing
  # to run any worker on this box because a HYGIENE PROBE could not run is a self-DoS: the
  # probe's failure modes are its own (no timeout binary, an unresolvable git dir, an
  # expired bound on a loaded box), none of which is evidence about the store. So it is
  # journalled every time and paged ONCE per run — loud enough to fix, never a silent
  # swallow and never a latch.
  # A DISAGREEMENT IS NAMED, NOT MERELY DEMOTED. One of the two channels said CORRUPT and
  # the other did not, so this run neither established damage nor ruled it out — and the
  # generic UNMEASURED line below would read as an ordinary "could not measure". Loud
  # enough to investigate, and still not a latch (see the CORRUPT branch for why).
  if [[ "$rc" -eq 4 || "$rc" -eq 6 || "$verdict" == "CORRUPT" || "$verdict" == "UNSWEEPABLE" ]]; then
    log "object-store: INCONSISTENT sweep result (rc=$rc verdict='${verdict:-<none>}') — exactly ONE of the exit status and the anchored verdict line reports a STOPPING verdict, or the two name different ones. Treated as UNMEASURED and NOT latched: an incomplete or unrecognised sweep must not stop every lane on this box (#3749). If the next sweep reproduces the finding it will latch it; investigate the sweep itself."
  fi
  log "object-store: UNMEASURED (rc=$rc verdict='${verdict:-<none>}') — the shared store was NOT rehashed, so its integrity is UNKNOWN, not clean. Continuing: a hygiene probe that cannot run must not stop the fleet (#3749)."
  printf '%s\n' "$out" | { grep '^OBJECT-STORE: unmeasured-cause ' || true; } | head -4 | while IFS= read -r obj_line; do
    log "object-store: $obj_line"
  done
  if [[ "$OBJ_SWEEP_UNMEASURED_NOTIFIED" -eq 0 ]]; then
    notify "high" "worker-supervisor: object-store sweep UNMEASURED" \
      "the shared git object store could not be rehashed on this box (rc=$rc) — integrity is UNKNOWN, not clean. The loop continues; fix the cause so the sweep can run."
    OBJ_SWEEP_UNMEASURED_NOTIFIED=1
  fi
  # Same re-check on the permissive path: UNMEASURED lets this lane carry on, so it is a
  # return that leads to a spawn and it gets a fresh latch read like every other one.
  obj_sweep_stop_if_latched "$latch"
  return 0
}

# THE RESIDUAL WINDOW, STATED PRECISELY BECAUSE IT IS NOT CLOSED (#3749 review round 3,
# item 1b — DELIBERATELY DEFERRED to its own issue, not papered over).
#
# WHAT THE ORDERING ABOVE BUYS: no lane advertises "this box has been swept" before the
# CORRUPT finding is durable, and no lane returns from this function toward a spawn
# without having re-read the latch. That strictly shrinks the window; it does not remove
# it.
#
# WHAT REMAINS RACY. The latch read and the worker spawn are two separate operations
# with no synchronisation between them, so a peer can create the latch in the gap
# between this lane's last `obj_sweep_stop_if_latched` and the moment its worker
# actually starts — and that gap is not small: after this function returns, the caller
# still runs the whole preflight hold loop (load, leftover processes, disk), which can
# poll for minutes. A peer's sweep completing anywhere inside that stretch is
# unobserved by this lane.
#
# THE WORST OUTCOME, NAMED: ONE worker is spawned on a box that was latched corrupt
# moments earlier. It is not silent and it is not durable — that lane's NEXT iteration
# begins with `object_store_sweep`, whose first act is the latch read at the top, so it
# stops there and its own worker exits at the end of its current issue. Nothing certifies
# a merge in that window without a full gate, and a full gate on a latched box is exactly
# what the next iteration refuses.
#
# WHY IT IS NOT CLOSED HERE. An atomic "no worker may start after any peer latches"
# guarantee needs per-store synchronisation shared by the sweep and the spawn decision
# (a lock held across both, or a spawn-time re-check inside whatever holds that lock) —
# a redesign of the boundary between this function and the spawn path, not an ordering
# change. It is split to its own issue. Do NOT describe the race as closed: it is
# narrowed, and the residual above is what a reader is entitled to know.

# ---------------------------------------------------------------------------
# Preflight: fail-closed, wait-don't-spin. Returns a hold reason on stdout, or
# empty when clear. stop-file / budgets are handled by the caller (clean exit,
# not a hold).
# ---------------------------------------------------------------------------
preflight_reason() {
  local load procs disk
  load="$(bash -c "$LOAD_PROBE_CMD" 2>/dev/null || echo 0)"
  if is_gt "$load" "$LOAD_MAX"; then
    echo "load"
    return 0
  fi
  # Two leftover families, bounded differently (roborev 1839). The worker orphan is a
  # NON-self-clearing hazard and takes PRIORITY when both are present (its tight bound
  # governs); the build/gate family is self-clearing with a loose bound.
  procs="$(bash -c "$PROC_PROBE_WORKER_CMD" 2>/dev/null || echo 0)"
  if [[ "$procs" =~ ^[0-9]+$ ]] && [[ "$procs" -gt 0 ]]; then
    echo "leftover-worker"
    return 0
  fi
  procs="$(bash -c "$PROC_PROBE_BUILD_CMD" 2>/dev/null || echo 0)"
  if [[ "$procs" =~ ^[0-9]+$ ]] && [[ "$procs" -gt 0 ]]; then
    echo "leftover-build"
    return 0
  fi
  disk="$(bash -c "$DISK_PROBE_CMD" 2>/dev/null || echo 999999)"
  if is_lt "$disk" "$DISK_FLOOR_GB"; then
    echo "disk"
    return 0
  fi
  echo ""
}

LAST_HOLD_REASON=""
# preflight_wait is BOUNDED (issue #2670, roborev 1810 HIGH): a hold loop must not
# be able to latch the supervisor silently. Every pass re-checks the stop-file AND
# the wall-clock budget, and a leftover hold that never clears stops the loop loudly
# rather than holding until MAX_HOURS with no page beyond the first HOLD. The two
# leftover families are bounded SEPARATELY (roborev 1839): a non-self-clearing worker
# orphan trips the TIGHT LEFTOVER_HOLD_MAX; a self-clearing build/gate process is
# allowed the LOOSE BUILD_HOLD_MAX so a legitimate concurrent full gate is waited out.
# preflight_stop_or_budget — the TWO clean-exit conditions, in one place so they can be
# asked wherever this file is about to commit to something long and uninterruptible.
# Never returns when either fires (finalize_exit ends the process).
preflight_stop_or_budget() {
  [[ -f "$STOP_FILE" ]] && finalize_exit "stop-file" 0
  [[ $(($(date +%s) - START_TS)) -ge "$MAX_HOURS_SECS" ]] && finalize_exit "budget-wallclock" 0
  return 0
}

preflight_wait() {
  local worker_holds=0 build_holds=0
  # ASKED IMMEDIATELY BEFORE THE SWEEP (#3749 review round 3, item 3). The sweep is the
  # longest uninterruptible step in an iteration, and it used to run BEFORE the hold
  # loop's stop-file and wall-clock checks — so a stop requested just after the outer
  # loop's own check was ignored for the whole sweep. An operator who touches the stop
  # file is entitled to have it read before this process spends minutes on hygiene.
  preflight_stop_or_budget
  # The throttled shared-object-store sweep (#3749) runs ONCE per iteration, HERE, before
  # the hold loop: it is per-iteration box hygiene, not a hold reason, and it must not be
  # re-run on every hold repoll. `CORRUPT` never returns — it stops the loop loudly from
  # inside (see object_store_sweep).
  #
  # THE IN-FLIGHT SWEEP IS STILL NOT INTERRUPTIBLE, AND THAT IS A DELIBERATE LIMIT, NOT
  # AN OVERSIGHT. The sweep's fsck walks (up to MAX_SWEEP_WALKS of them) happen inside a
  # CHILD PROCESS
  # (`bash scripts/check-object-store-integrity.sh`), so "check the stop file between the
  # passes" is not something this function can do: there is no point between them that
  # executes here. Making it interruptible means running the child in its own process
  # group, polling, and signalling that group — a bounded-runner redesign whose own
  # lifetime and process-ownership hazards this repo has already paid for once
  # (CLAUDE.md: never signal a process group you no longer own), for a stop-latency win.
  # So the exposure is BOUNDED IN WALL TIME INSTEAD: see OBJ_SWEEP_TIMEOUT_SECS, whose
  # supervisor default is the sweep script's own bound DIVIDED BY MAX_SWEEP_WALKS,
  # precisely so that every walk this caller can pay for still costs no more than ONE
  # walk at the script's bound. The stop file is re-read the moment
  # the sweep returns (the loop's first statement, below).
  object_store_sweep
  while true; do
    preflight_stop_or_budget
    local reason
    reason="$(preflight_reason)"
    if [[ -z "$reason" ]]; then
      LAST_HOLD_REASON=""
      return 0
    fi
    # Count each leftover family's holds CUMULATIVELY across the whole preflight_wait
    # invocation (roborev 1813) — a different hold reason (a transient load/disk blip,
    # or the OTHER leftover family) does NOT reset a family's tally. Otherwise an
    # orphan that never dies but is occasionally masked by a load spike would reset the
    # bound every time and hold forever. The wall-clock budget re-check at the top
    # bounds total hold time regardless of the reason mix.
    if [[ "$reason" == "leftover-worker" ]]; then
      worker_holds=$((worker_holds + 1))
      if [[ "$worker_holds" -ge "$LEFTOVER_HOLD_MAX" ]]; then
        local pids
        pids="$(bash -c "$PROC_LIST_WORKER_CMD" 2>/dev/null | tr '\n' ';' | cut -c1-300)"
        notify "high" "worker-supervisor: leftover worker CLI will not clear" \
          "held $worker_holds times on an orphaned worker CLI that never cleared — stopping loudly. Surviving: ${pids:-<unavailable>}"
        log "leftover-worker held $worker_holds times (>= LEFTOVER_HOLD_MAX=$LEFTOVER_HOLD_MAX) without clearing; stopping. Surviving: ${pids:-<unavailable>}"
        finalize_exit "leftover-worker" 1
      fi
    elif [[ "$reason" == "leftover-build" ]]; then
      # BUILD_HOLD_MAX<=0 DISABLES the build bound (wait indefinitely — still
      # backstopped by the wall-clock budget above).
      build_holds=$((build_holds + 1))
      if [[ "$BUILD_HOLD_MAX" -gt 0 ]] && [[ "$build_holds" -ge "$BUILD_HOLD_MAX" ]]; then
        local pids
        pids="$(bash -c "$PROC_LIST_BUILD_CMD" 2>/dev/null | tr '\n' ';' | cut -c1-300)"
        notify "high" "worker-supervisor: build/gate processes will not clear" \
          "held $build_holds times on build/gate processes that never cleared (well beyond a normal gate) — stopping loudly. Surviving: ${pids:-<unavailable>}"
        log "leftover-build held $build_holds times (>= BUILD_HOLD_MAX=$BUILD_HOLD_MAX) without clearing; stopping. Surviving: ${pids:-<unavailable>}"
        finalize_exit "leftover-build" 1
      fi
    fi
    if [[ "$reason" != "$LAST_HOLD_REASON" ]]; then
      notify "high" "worker-supervisor HOLD" "HOLD: $reason (repolling every ${HOLD_POLL_SECS}s, no spawn)"
      LAST_HOLD_REASON="$reason"
    fi
    log "HOLD: $reason; sleeping ${HOLD_POLL_SECS}s"
    sleep "$HOLD_POLL_SECS"
  done
}

# ---------------------------------------------------------------------------
# State + exit paths
# ---------------------------------------------------------------------------
ITER=0
ISSUES_DONE=0
CONSECUTIVE_ABNORMAL=0
CONSECUTIVE_UNVERIFIED=0
# Auto-merge-stuck is tracked PER-PR (roborev 1839/1840), not as a single across-PR
# streak: PENDING_PR_LIST holds one TAB-separated "pr<TAB>issue<TAB>observations<TAB>
# first-observed-epoch" record per still-pending armed PR. Each iteration re-verifies
# them (credit_merged_pending_prs): a PR that reached MERGED is credited toward
# MAX_ISSUES and dropped; a PR still pending has its observation count bumped and trips
# automerge-stuck only when the SAME PR has been seen unmerged PENDING_AUTOMERGE_MAX
# times AND has been pending at least PENDING_AUTOMERGE_MIN_SECS (a wall-clock floor so
# fast no-progress iterations can't burn the budget before CI could finish).
PENDING_PR_LIST=""
STUCK_SKIP_PR=""
LAST_BLOCKED_ISSUE=""
LAST_PARKED_ISSUE=""
START_TS=$(date +%s)
# MAX_HOURS_SECS is derived in validate_numeric_knobs (roborev 1842) — the FIRST
# statement of main(), before acquire_lock / preflight_wait / the main loop read it — so
# the derivation runs only after MAX_HOURS is confirmed a valid integer (roborev 1819's
# "budget defined before any preflight_wait read" guarantee is preserved). Do NOT move the
# derivation back to the config block: that reintroduces the silent `MAX_HOURS=abc → 0`
# budget hazard #1842 closed.

# report_pending_at_exit: a run that stops while armed PRs are still pending must not
# drop them silently (roborev 1841) — the PENDING_AUTOMERGE_MIN_SECS floor means a
# recently-armed PR can legitimately never have tripped automerge-stuck in this run. On
# exit, journal one `pending-at-exit` line per still-tracked PR (with its age) and fire a
# single summary notify, so an operator can follow up on any PR that hadn't landed yet.
report_pending_at_exit() {
  [[ -z "$PENDING_PR_LIST" ]] && return 0
  local pr issue count first_ts age n=0 summary=""
  while IFS=$'\t' read -r pr issue count first_ts; do
    [[ -z "$pr" ]] && continue
    # A currently-tripping stuck PR already got its own HIGH "auto-merge stuck" page —
    # don't ALSO announce it here as a generic still-pending PR (roborev 1842/1843).
    # STUCK_SKIP_PR is a newline-delimited list (all stuck PRs this pass).
    case $'\n'"$STUCK_SKIP_PR" in *$'\n'"$pr"$'\n'*) continue ;; esac
    age=$(( $(date +%s) - first_ts ))
    journal_line "$ITER" "pending-at-exit" "$issue" "$pr" 0 0 "pending_age_s=$age"
    n=$((n + 1))
    summary="${summary}${pr} (issue $issue, ${age}s); "
  done <<< "$PENDING_PR_LIST"
  [[ "$n" -eq 0 ]] && return 0
  notify "info" "worker-supervisor: $n armed PR(s) still pending at exit" \
    "these PRs were OPEN with auto-merge armed and had not yet landed when the run stopped — check they merge: ${summary}"
}

# NOTIFY BOUNDS ON THE EXIT PATH (#2666 / #3119). Issue #2666 pins a <15s
# supervisor exit latency (test_worker_supervisor.sh Test 22 — stubbed there, so it
# cannot observe the real notifier). finalize_exit fires up to TWO notifies
# (report_pending_at_exit + the stop page).
#
# BUDGET ARITHMETIC — count all THREE bounded steps, AND the SIGKILL grace on each.
# Each notify runs a payload ENCODER, then the PUBLISH, then the optional ADJUNCT,
# sequentially, so one notify's worst case is the SUM over steps of
# (bound + GATE_NOTIFY_KILL_GRACE). Two omissions have already been caught here, both
# by review, and both of the same shape — a term left out of this sum:
#
#   1. The ENCODER term. An early revision tightened only transport+adjunct and claimed
#      "2 x (4 + 2) = 12s"; the encoder's 4s bound was missing, so the truth was
#      2 x (4 + 4 + 2) = 20s — OVER the ceiling.
#   2. The KILL GRACE. `timeout` alone only SIGTERMs and a helper can ignore that
#      (measured: a `trap "" TERM` helper under `timeout 2` never returned), so
#      gate-notify.sh now escalates to SIGKILL via `--kill-after=<grace>`. That grace is
#      ADDITIVE WALL-CLOCK — `--kill-after=1 2` returns at 3s — so keeping 2/2/1 would
#      have made this 2 x (2+1 + 2+1 + 1+1) = 16s, again OVER the ceiling.
#
# So the bounds are retuned DOWN as part of adding the grace, never the ceiling up:
#
#     2 x [ (PAYLOAD+g) + (CURL+g) + (ADJUNCT+g) ]
#   = 2 x [ (1+1) + (1+1) + (1+1) ] = 12s  <  15s   (3s headroom, g = 1)
#
# Mechanized: scripts/tests/test_agent_gate_notify.sh reads these three values AND the
# grace out of the sources and measures two real notifies against SIGTERM-IGNORING
# wedges, so neither omission can recur silently. Steady-state notifies keep the
# roomier defaults; only the exit path is tightened, and it remains strictly better
# than the UNBOUNDED `curl -s` this replaced.
NOTIFY_EXIT_PAYLOAD_TIMEOUT="${NOTIFY_EXIT_PAYLOAD_TIMEOUT:-1}"
NOTIFY_EXIT_CURL_TIMEOUT="${NOTIFY_EXIT_CURL_TIMEOUT:-1}"
NOTIFY_EXIT_ADJUNCT_TIMEOUT="${NOTIFY_EXIT_ADJUNCT_TIMEOUT:-1}"

finalize_exit() {
  local reason="$1" code="$2"
  export GATE_NOTIFY_PAYLOAD_TIMEOUT="$NOTIFY_EXIT_PAYLOAD_TIMEOUT"
  export GATE_NOTIFY_CURL_TIMEOUT="$NOTIFY_EXIT_CURL_TIMEOUT"
  export GATE_NOTIFY_ADJUNCT_TIMEOUT="$NOTIFY_EXIT_ADJUNCT_TIMEOUT"
  # Release this lane's claim ref (issue #2655). `reap` refuses when the claim's issue still has an
  # open PR, so an unfinished endgame is preserved for adoption rather than orphaned. THIS RUNS ON
  # EVERY EXIT, not only a clean one — the comment here used to say "on a clean stop" and was simply
  # false (roborev round 18) — so the clean/abnormal distinction is passed down instead of assumed:
  # a placeholder lane id has no issue for that guard to consult and is kept on an abnormal exit.
  # Keyed on whether the CLAIMED WORK concluded, not on this exit's code (roborev round 23): a clean
  # stop mid-issue must keep the ref exactly as a breaker must, because in both cases the work is
  # unfinished and the ref is the only signal that this lane held it.
  clear_claim "$CLAIM_WORK_CONCLUDED"
  local elapsed=$(($(date +%s) - START_TS))
  mkdir -p "$LOG_DIR"
  report_pending_at_exit
  printf '{"ts":"%s","iter":%d,"outcome":"summary","reason":"%s","issues_done":%d,"elapsed_s":%d}\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$ITER" "$reason" "$ISSUES_DONE" "$elapsed" \
    >>"${JOURNAL_FILE:-$LOG_DIR/journal-$(date -u +%Y-%m-%d).jsonl}"
  local prio="info"
  [[ "$reason" == "breaker" ]] && prio="high"
  notify "$prio" "worker-supervisor stopped" "reason=$reason issues_done=$ISSUES_DONE elapsed_s=$elapsed"
  exit "$code"
}

# ---------------------------------------------------------------------------
# Per-PR auto-merge-stuck tracking (roborev 1839)
# ---------------------------------------------------------------------------
# remember_pending_pr <pr> <issue>: record an OPEN+armed PR for later re-verification,
# de-duped by PR. New PRs start at observation count 1 (this finalize is observation 1).
remember_pending_pr() {
  local pr="$1" issue="$2" line found=""
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    if [[ "${line%%$'\t'*}" == "$pr" ]]; then found=1; fi
  done <<< "$PENDING_PR_LIST"
  [[ -n "$found" ]] && return 0
  # Record: pr <TAB> issue <TAB> observation-count <TAB> first-observed-epoch.
  PENDING_PR_LIST="${PENDING_PR_LIST}${pr}"$'\t'"${issue}"$'\t'"1"$'\t'"$(date +%s)"$'\n'
}

# forget_pending_pr <pr>: drop a PR from the retroactive-credit set once its OWN
# finalized-marker iteration authoritatively resolves it (merged or mismatch), so it
# can never be re-verified/credited a second time by credit_merged_pending_prs.
forget_pending_pr() {
  local pr="$1" line new=""
  [[ -z "$PENDING_PR_LIST" ]] && return 0
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    [[ "${line%%$'\t'*}" == "$pr" ]] && continue
    new="${new}${line}"$'\n'
  done <<< "$PENDING_PR_LIST"
  PENDING_PR_LIST="$new"
}

# credit_merged_pending_prs: re-verify every still-pending armed PR. One that reached
# MERGED is CREDITED toward MAX_ISSUES (retroactive finalize credit) and dropped. One
# still pending has its observation count bumped and, if the SAME PR has now been seen
# unmerged PENDING_AUTOMERGE_MAX times, trips automerge-stuck (STOP loudly). One that is
# neither merged nor pending (closed/forged/gone) is dropped uncredited; an unverified/
# aborted transient is kept for a later retry. Called once per main-loop iteration.
credit_merged_pending_prs() {
  [[ -z "$PENDING_PR_LIST" ]] && return 0
  local new_list="" pr issue count first_ts verify age mstate any_stuck=""
  STUCK_SKIP_PR=""
  while IFS=$'\t' read -r pr issue count first_ts; do
    [[ -z "$pr" ]] && continue
    verify="$(verify_finalized_pr "$pr")"
    case "$verify" in
      merged)
        ISSUES_DONE=$((ISSUES_DONE + 1))
        journal_line "$ITER" "pending-credited" "$issue" "$pr" 0 0 "" "merged"
        notify "info" "worker-supervisor: pending PR landed" \
          "PR $pr (issue $issue) reached MERGED — retroactively credited toward MAX_ISSUES (issues_done=$ISSUES_DONE)"
        log "retroactive credit: PR $pr merged (issue $issue); ISSUES_DONE=$ISSUES_DONE"
        ;;
      pending-automerge)
        count=$((count + 1))
        age=$(( $(date +%s) - first_ts ))
        new_list="${new_list}${pr}"$'\t'"${issue}"$'\t'"${count}"$'\t'"${first_ts}"$'\n'
        # Auto-merge-stuck needs BOTH enough observations AND enough wall-clock (roborev
        # 1840): the time floor stops a burst of fast no-progress iterations (parks with
        # no backoff) from burning the observation budget before CI could plausibly finish.
        # Page/log/journal each stuck PR HERE at detection (no delay — roborev 1843), and
        # record it so report_pending_at_exit skips ALL stuck PRs (not just the first) and
        # the ACTUAL stop is deferred until after the list is rebuilt (roborev 1842).
        if [[ "$count" -ge "$PENDING_AUTOMERGE_MAX" ]] && [[ "$age" -ge "$PENDING_AUTOMERGE_MIN_SECS" ]]; then
          notify "high" "worker-supervisor: auto-merge stuck" \
            "PR $pr (issue $issue) has stayed OPEN pending auto-merge across $count observations over ${age}s — auto-merge is not landing; stopping so it isn't looped forever"
          log "PR $pr observed still-pending $count times over ${age}s (>= PENDING_AUTOMERGE_MAX=$PENDING_AUTOMERGE_MAX, >= PENDING_AUTOMERGE_MIN_SECS=$PENDING_AUTOMERGE_MIN_SECS); stopping (automerge-stuck)"
          journal_line "$ITER" "finalized-pending-automerge" "$issue" "$pr" 0 0 "pending_age_s=$age" "pending-automerge"
          any_stuck=1
          STUCK_SKIP_PR="${STUCK_SKIP_PR}${pr}"$'\n'
        fi
        ;;
      mismatch:*)
        # A tracked armed PR that ended NOT-merged (CLOSED-unmerged, auto-merge dropped,
        # or the ref forged/removed) is the very failure this feature exists to catch —
        # it must NOT be swallowed silently (roborev 1840). Page HIGH naming the PR/issue/
        # state and journal it abnormal-shaped, then drop it (the crash breaker is not
        # touched: the PR was legitimately armed earlier, so this is an escalation to the
        # owner, not a worker-crash signal).
        mstate="${verify#mismatch:}"
        journal_line "$ITER" "pending-dropped" "$issue" "$pr" 0 0 "" "$verify"
        notify "high" "worker-supervisor: armed PR did not land" \
          "PR $pr (issue $issue) was OPEN+armed but is now $mstate (not MERGED) — auto-merge did not land; needs owner attention"
        log "pending PR $pr resolved $verify (issue $issue); dropped from retroactive-credit set, paged high"
        ;;
      *)
        # unverified / aborted — a transient gh/transport gap or a shutdown; keep the PR
        # (count + first_ts unchanged) for a later retry rather than crediting or dropping.
        new_list="${new_list}${pr}"$'\t'"${issue}"$'\t'"${count}"$'\t'"${first_ts}"$'\n'
        ;;
    esac
  done <<< "$PENDING_PR_LIST"
  PENDING_PR_LIST="$new_list"
  # PENDING_PR_LIST now reflects this pass (resolved PRs removed, stuck ones kept but in
  # STUCK_SKIP_PR). Stop AFTER the rebuild so report_pending_at_exit reads a fresh list and
  # skips every already-paged stuck PR (roborev 1842/1843). Use an explicit `if` (not a
  # `&&` tail) so the function returns 0 under `set -e` when nothing is stuck.
  if [[ -n "$any_stuck" ]]; then
    finalize_exit "automerge-stuck" 1
  fi
  return 0
}

# ---------------------------------------------------------------------------
# One iteration: spawn, judge, journal.
# ---------------------------------------------------------------------------
run_iteration() {
  ITER=$((ITER + 1))
  rm -f "$MARKER_FILE"
  mkdir -p "$LOG_DIR"
  # Stamp/refresh the machine's claim ref BEFORE spawning (issue #2655): the
  # supervisor authors liveness mechanically, so a beat-then-crash worker can no
  # longer look alive for the whole threshold window. Uses the best-known issue
  # from the previous iteration's marker (empty/unknown -> "0").
  stamp_claim "$CLAIM_ISSUE"
  # UNCONCLUDED FROM THE MOMENT THE REF EXISTS (roborev round 24, High). The round-23 fix tracked
  # whether the claimed work had concluded but never RESET the flag, so it kept its initial `1` — or
  # inherited `1` from a previous finalize — and every path that returns before the outcome case
  # (a crash, the stuck watchdog, an early `finalize_exit`) left it saying "concluded". A breaker after
  # abnormal iterations then called `clear_claim 1` and deleted the current lane's only liveness ref:
  # THE EXACT FAILURE THE ROUND-23 FIX EXISTS TO PREVENT, REINTRODUCED BY THAT FIX.
  #
  # Set here, immediately after the stamp, so "in progress" is the state from the instant the ref
  # exists and every early exit inherits the SAFE value. Concluded is now something an outcome must
  # EARN, at the point its own validation accepts it — never a default and never a leftover.
  CLAIM_WORK_CONCLUDED=0
  local logfile="$LOG_DIR/iter-${ITER}.log"
  local stuck_flag="$LOG_DIR/.iter-${ITER}.stuck"
  rm -f "$stuck_flag"
  local t0 t1 rc=0
  t0=$(date +%s)
  # Spawn the worker in the background and supervise it with a split-cadence poll
  # loop (issue #2666, hardened per roborev 1773):
  #   * exit/deadline check every 1s — near-blocking completion latency and
  #     MAX_ITER_SECS enforced at 1s granularity (portably — no coreutils
  #     `timeout` on macOS).
  #   * WEDGE scan every STUCK_POLL_SECS — classify `stuck-on-question` ONLY on
  #     positive wedge evidence that holds across TWO consecutive scans: (a) the
  #     process is still alive, (b) a prompt signature is in the LAST ~20 log
  #     lines (tail scan, not whole-log), AND (c) the log has NOT GROWN between
  #     the two scans (byte size unchanged). A wedged prompt emits no further
  #     output, so its tail + size freeze; a busy worker that merely printed the
  #     tool name in its trace keeps writing (size grows) and/or scrolls the match
  #     out of the tail — either fails the test and is ignored. Tradeoff: a
  #     genuine wedge takes up to 2*STUCK_POLL_SECS to confirm (vs an instant but
  #     false-positive-prone substring match), and a marker-less abnormal exit
  #     with a stray scrollback match stays ABNORMAL (counts toward the breaker) —
  #     exactly the guarantee the substring approach would have defeated.
  set +e
  bash -c "$WORKER_CMD" >"$logfile" 2>&1 &
  local wpid=$!
  local deadline=$((t0 + MAX_ITER_SECS))
  local stuck_notified=0 g now
  local last_scan_ts=$t0 prev_size=-1 prev_sig=0 cur_size cur_sig
  # SWEPT AND LEFT TWO-VALUED, DELIBERATELY (#3601 call-site audit). `#3601` made the LOCK's liveness
  # probe three-valued because that pid comes off DISK, from a process we have never seen, possibly
  # owned by another user — so `kill -0` failing there could mean EPERM and reading it as death stole a
  # live holder's lock. None of that applies here: `$wpid` is the pid of a child THIS SHELL just forked,
  # same uid, same session, so EPERM is not reachable and a non-zero `kill -0` means exactly one thing.
  # Recorded so a later sweep of the `kill -0` sites does not "fix" a correct one — the three-valued
  # probe belongs where the pid's provenance is untrusted, not everywhere the primitive appears.
  while kill -0 "$wpid" 2>/dev/null; do
    now=$(date +%s)
    if [[ "$now" -ge "$deadline" ]]; then
      log "iteration $ITER exceeded MAX_ITER_SECS=${MAX_ITER_SECS}s; terminating worker"
      kill -TERM "$wpid" 2>/dev/null
      g=0
      while kill -0 "$wpid" 2>/dev/null && [[ "$g" -lt 5 ]]; do sleep 1; g=$((g + 1)); done
      kill -KILL "$wpid" 2>/dev/null
      break
    fi
    if [[ "$stuck_notified" -eq 0 ]] && [[ $((now - last_scan_ts)) -ge "$STUCK_POLL_SECS" ]]; then
      last_scan_ts="$now"
      cur_size="$(log_size "$logfile")"
      if detect_prompt_signature "$logfile"; then cur_sig=1; else cur_sig=0; fi
      # Wedge confirmed only when the signature is present in the tail at BOTH
      # this scan and the prior one AND the log did not grow between them (and
      # the process is still alive — the loop condition). prev_size<0 = no prior
      # scan yet, so the very first scan can never confirm.
      # AND BOTH SCANS MUST BE REAL MEASUREMENTS (#3601). `log_size` reports `-1` when it could not
      # measure; without `cur_size -ge 0` two FAILED reads compare equal and the log reads as frozen —
      # the "an empty probe is not a zero" shape. A `-1` also propagates into `prev_size` below, so the
      # next scan cannot confirm against it either.
      if [[ "$prev_sig" -eq 1 && "$cur_sig" -eq 1 && "$prev_size" -ge 0 && "$cur_size" -ge 0 && "$cur_size" -eq "$prev_size" ]]; then
        local qtext
        qtext="$(captured_question "$logfile")"
        printf '%s' "$qtext" >"$stuck_flag"
        notify "high" "worker-supervisor: stuck-on-question (iter $ITER)" \
          "worker appears wedged on an interactive prompt (frozen log + signature across 2 polls): ${qtext:-<no text captured>}"
        log "iteration $ITER: wedge confirmed (frozen log + tail signature x2 polls); paged owner (stuck-on-question)"
        stuck_notified=1
      fi
      prev_size="$cur_size"
      prev_sig="$cur_sig"
    fi
    sleep 1
  done
  wait "$wpid"
  rc=$?
  set -e
  t1=$(date +%s)
  local duration=$((t1 - t0))

  # The verification-outage streak (CONSECUTIVE_UNVERIFIED) measures CONSECUTIVE gh
  # verification FAILURES with no gh SUCCESS in between (roborev 1840): it is reset ONLY
  # by an outcome where gh actually RESOLVED the PR (merged / pending-automerge /
  # mismatch:* below), NOT by an intervening no-work/abnormal/parked iteration that
  # never exercised gh — otherwise a persistent gh outage interleaved with unrelated
  # outcomes (a common shape, since gh and worker failures share causes) would never
  # reach UNVERIFIED_MAX, restoring the MAX_ISSUES-drift hazard #2670 closed.

  # A live-but-prompt-blocked worker (detected mid-iteration) that then exits
  # without a trustworthy clean marker is a PARK-shaped stall, not a crash:
  # verdict `stuck-on-question`, owner already paged, NOT counted toward the
  # breaker (issue #2666). Guarded on "no clean marker" so a late false-positive
  # signature can never mask a real finalized/park outcome.
  if [[ -f "$stuck_flag" ]] && { [[ "$rc" -ne 0 ]] || [[ ! -f "$MARKER_FILE" ]]; }; then
    # NEUTRAL, not transparent: like every other non-abnormal verdict this
    # resets the consecutive-abnormal counter, so a real prior crash chain is
    # BROKEN by a stuck iteration rather than silently continuing across it
    # (roborev 1769: `abnormal → stuck → abnormal → abnormal` must not trip
    # BREAKER_N=3). The owner has already been paged.
    CONSECUTIVE_ABNORMAL=0
    journal_line "$ITER" "stuck-on-question" "" "" "$duration" "$rc" "$(cat "$stuck_flag" 2>/dev/null)"
    log "iteration $ITER stuck-on-question (owner paged; breaker chain reset, not counted)"
    return 0
  fi

  if [[ "$rc" -ne 0 ]] || [[ ! -f "$MARKER_FILE" ]]; then
    journal_line "$ITER" "abnormal" "" "" "$duration" "$rc"
    log "iteration $ITER abnormal (exit=$rc marker_present=$([[ -f "$MARKER_FILE" ]] && echo yes || echo no))"
    trip_breaker_or_continue
    return 0
  fi

  local outcome issue pr reason question
  outcome="$(marker_field outcome)"
  issue="$(marker_field issue)"
  pr="$(marker_field pr)"
  reason="$(marker_field reason)"
  question="$(marker_field question)"

  # Learn the issue this iteration worked so the NEXT spawn's claim stamp names it (issue #2655). A
  # recycled worker that resumes the same issue keeps the claim ref's open-PR reap guard accurate.
  #
  # CARRYING IT FORWARD IS THE UNSAFE DIRECTION, SO IT IS THE ONE THAT MUST BE EARNED (roborev round
  # 20, High). Only `finalized` used to release the issue, but a `blocked` park on
  # `seam1-approval`/`needs-decision` ALSO releases it: the worker posts its question, the issue is
  # excluded from the next pickup until the owner answers, and this supervisor moves on. Carrying that
  # issue forward stamped the NEXT worker under the RELEASED issue's ref — so another lane legitimately
  # resuming that issue overwrote the ref, and a dead supervisor behind it became invisible. That is
  # precisely the ref collision per-lane claims exist to remove, reached through the commonest park path.
  #
  # Keyed on the AFFIRMATIVE case, not on a list of releasing reasons (#3464 family 6: an enumeration
  # of reasons is a subject set that drifts the moment someone adds an exit). The issue is carried
  # forward ONLY when this iteration demonstrably continues working it — a `blocked` outcome that is NOT
  # an owner park. Every other outcome, including any park reason added later and any outcome nobody has
  # thought of yet, falls through to the SAFE branch: `CLAIM_ISSUE` is cleared and the next spawn stamps
  # its own unique `p<pid>-<token>` lane, which cannot collide with anything.
  local retains_issue=no
  case "$outcome" in
    blocked)
      case "$reason" in
        seam1-approval | needs-decision) : ;;   # owner park => the issue is RELEASED
        *) retains_issue=yes ;;                 # a technical block => this lane keeps working it
      esac
      ;;
  esac
  if [[ "$retains_issue" == yes && -n "$issue" ]]; then
    CLAIM_ISSUE="$issue"
  else
    CLAIM_ISSUE=""
  fi

  # NOTE: `CLAIM_WORK_CONCLUDED` is NOT classified here. It was, in round 23, and that read the
  # outcome string BEFORE the validation below judged it — so an untrusted `finalized` marker missing
  # its issue/pr, or one whose PR is not actually merged, marked the work concluded and got the lane's
  # ref deleted on exit (roborev round 24, High). It is set only where an outcome is ACCEPTED.

  case "$outcome" in
    finalized)
      if [[ -z "$issue" || -z "$pr" ]]; then
        # F5: contract requires BOTH issue and pr on "finalized" — a marker
        # claiming success with either missing/null is untrustworthy and must
        # not be counted as a done issue nor reset the breaker.
        journal_line "$ITER" "abnormal" "$issue" "$pr" "$duration" "$rc"
        log "iteration $ITER abnormal (finalized marker missing issue/pr: issue='$issue' pr='$pr')"
        trip_breaker_or_continue
      else
        # Issue #2670: a well-formed "finalized" is NOT trusted on field shape
        # alone — verify the claimed PR is actually merged on GitHub before
        # crediting the iteration. A worker that parked its endgame yet wrote
        # "finalized" (or a stale/forged marker) must never count as done nor
        # reset the crash breaker.
        local verify
        verify="$(verify_finalized_pr "$pr")"
        case "$verify" in
          merged)
            # EARNED HERE, and only here, for a finalize: the marker was well-formed AND the PR is
            # verified merged on GitHub. Anything less leaves the work unconcluded, so the lane keeps
            # its claim ref (roborev round 24). AND the marker's issue must be the lane this supervisor
            # stamped (roborev round 33) — concluding a DIFFERENT issue's work must not release this one.
            if conclusion_matches_stamped_lane "$issue"; then
              CLAIM_WORK_CONCLUDED=1
            else
              log "WARN: marker finalises issue $issue but this lane stamped ${CLAIM_STAMPED_ISSUE:-<none>} — NOT concluding; lane ${CLAIM_STAMPED_ISSUE} keeps its ref"
            fi
            CONSECUTIVE_ABNORMAL=0
            CONSECUTIVE_UNVERIFIED=0
            forget_pending_pr "$pr"
            ISSUES_DONE=$((ISSUES_DONE + 1))
            journal_line "$ITER" "finalized" "$issue" "$pr" "$duration" "$rc" "" "merged"
            notify "info" "worker-supervisor: finalized issue $issue" "pr=$pr duration_s=$duration verified=merged"
            ;;
          pending-automerge)
            # OPEN with auto-merge armed (roborev 1813): the closer's DOCUMENTED
            # auto-merge path — the PR WILL land on green. NOT counted toward
            # MAX_ISSUES yet (it isn't merged), a default-priority page, and
            # breaker-NEUTRAL (gh resolved fine — do NOT touch the crash breaker).
            # A DISTINCT armed PR is a healthy fresh finalize, NOT evidence any PR is
            # stuck (roborev 1839): record it for per-PR re-verification. It is credited
            # toward MAX_ISSUES retroactively once it reaches MERGED (credit_merged_
            # pending_prs), and only trips automerge-stuck when the SAME PR is seen
            # unmerged PENDING_AUTOMERGE_MAX times — never on N distinct healthy PRs.
            CONSECUTIVE_UNVERIFIED=0
            remember_pending_pr "$pr" "$issue"
            journal_line "$ITER" "finalized-pending-automerge" "$issue" "$pr" "$duration" "$rc" "" "pending-automerge"
            notify "info" "worker-supervisor: finalized PENDING AUTO-MERGE issue $issue" \
              "PR $pr is OPEN with auto-merge armed — not counted yet; it will land automatically on green"
            log "iteration $ITER finalized-pending-automerge (PR $pr OPEN, auto-merge armed; tracked for retroactive credit, breaker neutral)"
            ;;
          aborted)
            # The finalize verification was cut short by a requested shutdown mid-grace
            # (roborev 1837): the stop-file appeared before the PR state could settle.
            # This is NOT a verdict about the worker — it is neutral. Journal it, do NOT
            # count it toward MAX_ISSUES, do NOT increment the unverified-outage streak
            # (so an ordinary shutdown can't false-trip verify-unavailable), do NOT page,
            # and leave the breaker untouched. The main loop exits on the stop-file at its
            # next top.
            journal_line "$ITER" "finalized-aborted" "$issue" "$pr" "$duration" "$rc" "" "aborted"
            log "iteration $ITER finalized-aborted (verify interrupted by shutdown; neutral, not counted)"
            ;;
          mismatch:*)
            # The PR is NOT merged (OPEN / CLOSED-unmerged / UNRESOLVED forged ref)
            # — the worker claimed a finalize it did not actually land. Judged
            # ABNORMAL: uncounted, counts toward the breaker, and a HIGH page names
            # the discrepancy. gh clearly RESOLVED (or the ref was forged), so the
            # verification-outage streak is broken — reset it (a gh-SUCCESS outcome).
            CONSECUTIVE_UNVERIFIED=0
            forget_pending_pr "$pr"
            local mstate="${verify#mismatch:}"
            journal_line "$ITER" "abnormal" "$issue" "$pr" "$duration" "$rc" "" "$verify"
            notify "high" "worker-supervisor: finalized MISMATCH issue $issue" \
              "worker claimed finalized but PR $pr is $mstate (not MERGED) — not counted, breaker +1"
            log "iteration $ITER abnormal (finalized MISMATCH: PR $pr is $mstate, not MERGED)"
            trip_breaker_or_continue
            ;;
          *)
            # unverified — strictly a gh exec/transport failure. FAIL-INFORMATIVE:
            # log + default-priority page, do NOT count as done, and stay NEUTRAL
            # to the breaker (do not increment — a transient outage is not a crash;
            # do not reset — it must not mask a real prior crash chain). But a
            # PERSISTENT outage is an operator problem (roborev 1810 MED): after
            # UNVERIFIED_MAX consecutive unverified finalizes (with no gh-SUCCESS
            # resetting the streak in between) the supervisor STOPS loudly, so the
            # MAX_ISSUES ceiling can't drift on uncounted-forever runs.
            CONSECUTIVE_UNVERIFIED=$((CONSECUTIVE_UNVERIFIED + 1))
            journal_line "$ITER" "finalized-unverified" "$issue" "$pr" "$duration" "$rc" "" "unverified"
            notify "info" "worker-supervisor: finalized UNVERIFIED issue $issue" \
              "could not verify PR $pr merged (gh unavailable/network) — not counted, breaker unchanged"
            log "iteration $ITER finalized-unverified (gh could not confirm PR $pr; not counted, breaker neutral)"
            if [[ "$CONSECUTIVE_UNVERIFIED" -ge "$UNVERIFIED_MAX" ]]; then
              notify "high" "worker-supervisor: verification unavailable" \
                "$CONSECUTIVE_UNVERIFIED consecutive finalized markers could not be gh-verified — persistent verification outage; stopping so the MAX_ISSUES ceiling stays meaningful"
              log "iteration $ITER: $CONSECUTIVE_UNVERIFIED consecutive unverified (>= UNVERIFIED_MAX=$UNVERIFIED_MAX); stopping (verify-unavailable)"
              finalize_exit "verify-unavailable" 1
            fi
            ;;
        esac
      fi
      ;;
    no-work)
      CONSECUTIVE_ABNORMAL=0
      # NOTHING WAS CLAIMED, SO NOTHING IS IN PROGRESS (roborev round 25, Medium — a REGRESSION from
      # round 24). Round 24 correctly reset the flag to 0 at the stamp so early exits inherit the safe
      # value, but that made `no-work` permanently unconcluded: the Ready queue was empty, a placeholder
      # was stamped, and a later stop or wall-clock exit then PRESERVED that placeholder. Placeholders are
      # never automatically reaped, so every NORMAL IDLE SHUTDOWN leaked a stale ref that `dead-lanes`
      # then reported as a dead lane — turning the monitor this change ships into one an operator learns
      # to ignore.
      #
      # GUARDED ON THE STAMPED LANE, NOT ON THE MARKER'S ISSUE FIELD (roborev round 29, Medium — a
      # REGRESSION from round 25's own guard). Round 25 keyed on `[ -z "$issue" ]`, the marker's issue,
      # which is empty for every `no-work`. But the STAMPED ref can be a NUMERIC issue carried forward
      # from a prior technical block — so an empty no-work concluded work on an issue that is still
      # unresolved, and the shutdown then cleared that issue's only liveness signal.
      #
      # The question is not "did this iteration name an issue" but "does the ref this lane currently
      # holds name one". A placeholder (or nothing stamped) means nothing is in progress; a numeric lane
      # means an issue is still held and must keep its ref.
      case "${CLAIM_STAMPED_ISSUE:-}" in
        '' | p*) [[ -z "$issue" ]] && CLAIM_WORK_CONCLUDED=1 ;;
        *) : ;;   # a numeric lane is still held — no-work says nothing about that issue
      esac
      journal_line "$ITER" "no-work" "$issue" "$pr" "$duration" "$rc"
      log "no work available; backing off ${BACKOFF_NOWORK_SECS}s"
      sleep "$BACKOFF_NOWORK_SECS"
      ;;
    blocked)
      CONSECUTIVE_ABNORMAL=0
      case "$reason" in
        seam1-approval | needs-decision)
          # CLEAN PARK (issue #2666, park-and-resume). The worker hit Seam 1 (an
          # unapproved design spec) or a genuine mid-run owner decision: it posted
          # ONE structured question comment, added the `needs-decision` label, and
          # released the machine. Judged NORMAL — never abnormal, never toward the
          # breaker — and it does NOT head-block the queue: the labeled issue is
          # excluded from the worker's next pickup until the owner answers and the
          # label clears, so the loop moves straight to the next Ready issue. Fire
          # ONE high-priority page whose title carries the issue # and the first
          # line of the question (the marker's optional "question" field).
          # EARNED HERE for a park: the issue is RELEASED — excluded from the next pickup until the
          # owner answers — so this lane no longer holds it and its ref should go on a clean exit.
          # Same lane check as the finalize path (roborev round 33): a park of a DIFFERENT issue says
          # nothing about the issue this lane stamped.
          if conclusion_matches_stamped_lane "$issue"; then
            CLAIM_WORK_CONCLUDED=1
          else
            log "WARN: marker parks issue $issue but this lane stamped ${CLAIM_STAMPED_ISSUE:-<none>} — NOT concluding; lane ${CLAIM_STAMPED_ISSUE} keeps its ref"
          fi
          journal_line "$ITER" "parked-on-owner" "$issue" "$pr" "$duration" "$rc" "$reason"
          if [[ -n "$issue" && "$issue" == "$LAST_PARKED_ISSUE" ]]; then
            # Head-block-on-decision guard (mirrors the F2 blocked-path guard,
            # roborev 1769): the SAME issue parked on two consecutive iterations
            # means the worker keeps re-parking it — typically because the
            # `needs-decision` label never applied, so the pickup exclusion is not
            # holding and the loop would burn to MAX_ISSUES re-asking one question.
            # Page the owner and STOP cleanly instead of looping.
            notify "high" "worker-supervisor: issue $issue head-blocked on decision" "issue #$issue parked twice in a row (needs-decision) — queue head-blocked on an owner decision, needs owner"
            log "issue $issue parked on two consecutive iterations; head-blocked on decision, stopping"
            finalize_exit "head-blocked-decision" 0
          fi
          LAST_PARKED_ISSUE="$issue"
          local qline="${question:-$reason}"
          notify "high" "worker-supervisor: parked issue $issue — ${qline}" \
            "issue #$issue parked awaiting owner (${reason}). Answer the needs-decision question comment on the issue; the worker resumes on a newer owner reply."
          log "issue $issue parked-on-owner ($reason); moving to next Ready issue"
          ;;
        *)
          journal_line "$ITER" "blocked" "$issue" "$pr" "$duration" "$rc" "$reason"
          if [[ -n "$issue" && "$issue" == "$LAST_BLOCKED_ISSUE" ]]; then
            # F2: the SAME issue blocked on two consecutive iterations means the
            # queue is head-blocked — looping would just reset the breaker every
            # time and burn wall-clock budget until MAX_HOURS with no progress.
            # Stop cleanly and page the owner instead.
            notify "high" "worker-supervisor: issue $issue persistently blocked" "issue #$issue persistently blocked — queue is head-blocked, needs owner"
            log "issue $issue blocked on two consecutive iterations; queue is head-blocked, stopping"
            finalize_exit "head-blocked" 0
          fi
          LAST_BLOCKED_ISSUE="$issue"
          notify "info" "worker-supervisor: blocked on issue $issue" "${reason:-no reason given}"
          log "remembered blocked issue $LAST_BLOCKED_ISSUE (not auto-retried this run)"
          ;;
      esac
      ;;
    *)
      journal_line "$ITER" "abnormal" "$issue" "$pr" "$duration" "$rc"
      log "iteration $ITER abnormal (unrecognized marker outcome '$outcome')"
      trip_breaker_or_continue
      ;;
  esac
}

trip_breaker_or_continue() {
  CONSECUTIVE_ABNORMAL=$((CONSECUTIVE_ABNORMAL + 1))
  if [[ "$CONSECUTIVE_ABNORMAL" -ge "$BREAKER_N" ]]; then
    notify "high" "worker-supervisor BREAKER" "$BREAKER_N consecutive abnormal iterations — stopping, no hot respawn"
    finalize_exit "breaker" 1
  fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
# validate_numeric_knobs: every env-overridable numeric bound is used in arithmetic /
# `-gt`/`-ge` comparisons; a malformed value (an operator typo like `12h` in a plist)
# would otherwise evaluate to 0 and SILENTLY disable/misbehave a bound — the opposite of
# fail-closed (roborev 1840). Validate each against `^-?[0-9]+$` at startup and STOP loudly
# on a bad value rather than running with a silently-broken safety bound.
_bad_knob() {
  log "FATAL: numeric knob $1 has value '$2' — expected $3; refusing to start with a silently-broken bound"
  notify "high" "worker-supervisor: bad config" "$1='$2' is not $3; supervisor refused to start (fix the env/plist)"
  exit 2
}

validate_numeric_knobs() {
  local name val
  # FLOAT-tolerant knobs — compared via awk is_gt/is_lt (which coerce with `+0`). A
  # fractional value (e.g. DISK_FLOOR_GB=37.5, `.5`) is valid; a NON-numeric OR NEGATIVE
  # value would coerce/compare so the bound silently DISABLES (fail-open), so require a
  # NON-NEGATIVE decimal (roborev 1843). Accepts `40`, `40.5`, `.5`, `40.`.
  for name in LOAD_MAX DISK_FLOOR_GB; do
    val="${!name}"
    [[ "$val" =~ ^([0-9]+\.?[0-9]*|\.[0-9]+)$ ]] || _bad_knob "$name" "$val" "a non-negative number"
  done
  # NON-NEGATIVE integer knobs — counts/seconds where a negative is meaningless and would
  # silently break the bound (e.g. MAX_ISSUES=-1 ⇒ instant budget-issues rc=0, roborev
  # 1843). MAX_HOURS is here because its `$((MAX_HOURS * 3600))` derivation is integer
  # arithmetic (a bare-word MAX_HOURS would coerce to 0 → a broken wall-clock budget).
  # OBJ_SWEEP_INTERVAL_HOURS is here, not in the signed group: 0 is its documented
  # `disables` value and a NEGATIVE would be an operator typo whose meaning is undefined
  # (the `-le 0` guard would treat it as disabled, which is not what a `-1` was trying to
  # say). #3749.
  for name in MAX_HOURS MAX_ISSUES BREAKER_N BACKOFF_NOWORK_SECS HOLD_POLL_SECS \
              MAX_ITER_SECS STUCK_POLL_SECS STUCK_TAIL_LINES LEFTOVER_HOLD_MAX \
              UNVERIFIED_MAX MISMATCH_RETRIES MISMATCH_RETRY_WAIT_SECS \
              PENDING_AUTOMERGE_MAX PENDING_AUTOMERGE_MIN_SECS \
              OBJ_SWEEP_INTERVAL_HOURS; do
    val="${!name}"
    [[ "$val" =~ ^[0-9]+$ ]] || _bad_knob "$name" "$val" "a non-negative integer"
  done
  # STRICTLY POSITIVE integer knobs — a group of its own because for these, ZERO is not a lax bound
  # but a SILENT SKIP. `CLAIM_MIGRATION_RETRIES=0` makes the retry loop body never execute, so the
  # legacy-claim status is never read, the migration never happens, and the lane runs foreign to its
  # own lock with no error anywhere: exactly the fail-open shape this function's own docstring
  # describes ("evaluate to 0 and SILENTLY disable"). Found because a test harness left the knob
  # unset and the migration quietly did nothing — the same failure a plist typo would produce in
  # production, where nothing would have been watching.
  for name in CLAIM_MIGRATION_RETRIES; do
    val="${!name}"
    [[ "$val" =~ ^[0-9]+$ ]] || _bad_knob "$name" "$val" "a positive integer"
    [[ "$val" -ge 1 ]] || _bad_knob "$name" "$val" "a positive integer (0 would silently skip the migration entirely)"
  done
  # OBJ_SWEEP_TIMEOUT_SECS is the same class (#3749): it is passed straight through as the
  # sweep's `--timeout`, which REJECTS 0 as a usage error — so a 0 would make every sweep
  # UNMEASURED forever, i.e. a bound that silently disables the probe rather than loosening
  # it. Its own group because the message has to say that.
  for name in OBJ_SWEEP_TIMEOUT_SECS; do
    val="${!name}"
    [[ "$val" =~ ^[0-9]+$ ]] || _bad_knob "$name" "$val" "a positive integer"
    [[ "$val" -ge 1 ]] || _bad_knob "$name" "$val" "a positive integer (the sweep rejects 0, which would make every run UNMEASURED)"
  done
  # SIGNED integer knobs — the two with a documented `<=0 disables` contract.
  for name in BUILD_HOLD_MAX MISMATCH_GRACE_CAP_SECS; do
    val="${!name}"
    [[ "$val" =~ ^-?[0-9]+$ ]] || _bad_knob "$name" "$val" "an integer"
  done
  # Derive the wall-clock budget now that MAX_HOURS is a confirmed non-negative integer;
  # honor an explicit MAX_HOURS_SECS override (also validated).
  if [[ -z "$MAX_HOURS_SECS" ]]; then
    MAX_HOURS_SECS=$(( MAX_HOURS * 3600 ))
  else
    [[ "$MAX_HOURS_SECS" =~ ^[0-9]+$ ]] || _bad_knob MAX_HOURS_SECS "$MAX_HOURS_SECS" "a non-negative integer"
  fi
}

main() {
  validate_numeric_knobs
  acquire_lock
  log "started: MAX_ISSUES=$MAX_ISSUES MAX_HOURS=$MAX_HOURS LOAD_MAX=$LOAD_MAX DISK_FLOOR_GB=$DISK_FLOOR_GB BREAKER_N=$BREAKER_N"
  # #2655 machine-claim liveness is disabled when CLAIM_CMD is empty (a machine with
  # no origin push rights, or the hermetic tooling tests). Announce it ONCE so a
  # silently-unclaimed run is visible in the log rather than mistaken for a claim
  # that simply never refreshed (stamp_claim/clear_claim otherwise return with no line).
  [[ -n "$CLAIM_CMD" ]] || log "claim stamping DISABLED (CLAIM_CMD empty) — no machine-claim liveness ref this run (#2655)"
  while true; do
    [[ -f "$STOP_FILE" ]] && finalize_exit "stop-file" 0
    [[ $(($(date +%s) - START_TS)) -ge "$MAX_HOURS_SECS" ]] && finalize_exit "budget-wallclock" 0
    # Re-verify previously-pending armed PRs FIRST (roborev 1839): credit any that have
    # since reached MERGED toward MAX_ISSUES, and trip automerge-stuck if the SAME PR is
    # stuck — done before the ceiling check so a retroactive credit can satisfy it.
    credit_merged_pending_prs
    [[ "$ISSUES_DONE" -ge "$MAX_ISSUES" ]] && finalize_exit "budget-issues" 0

    # Re-attempt an UNSETTLED legacy-claim migration (roborev round 35, Medium): a transient outage at
    # startup must not leave this lane foreign to its own lock for the rest of the run. Once settled
    # this returns immediately, so it costs nothing.
    supervisor_migrate_legacy_claim

    preflight_wait

    run_iteration
  done
}

# Run the loop only when executed directly; when sourced (e.g. by the tooling
# tests to exercise detect_prompt_signature/captured_question in isolation) the
# functions are defined but the machine-guarding loop never starts.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  main "$@"
fi
