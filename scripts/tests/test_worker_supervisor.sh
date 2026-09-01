#!/usr/bin/env bash
# scripts/tests/test_worker_supervisor.sh — fast, self-contained tests for
# scripts/local/worker-supervisor.sh (issue #2090). No cargo, no gate, no
# network: every external probe/worker/notify is a stub script written to a
# per-test mktemp dir. Target: <30s total.
set -uo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# This file's own path: cases that extract a HARNESS function into a scratch driver read it from here,
# the same way the lock cases read the shipped supervisor out of `$SUPERVISOR` — the subject is always
# the shipped text, never a re-implementation.
SELF_FILE="$SELF_DIR/$(basename "${BASH_SOURCE[0]}")"
REPO_ROOT="$(cd "$SELF_DIR/../.." && pwd)"
SUPERVISOR="$REPO_ROOT/scripts/local/worker-supervisor.sh"

PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
pass() {
  PASS_COUNT=$((PASS_COUNT + 1))
  echo "PASS: $1"
}
fail() {
  FAIL_COUNT=$((FAIL_COUNT + 1))
  echo "FAIL: $1"
}

# t <test-fn> — run a top-level test. IT MUST EXIST, AND IT MUST RETURN ZERO.
#
# THE SUITE REPORTED GREEN THROUGH A TEST THAT DID NOT EXIST (roborev round 27, Medium).
# `test_claim_transition_survives_failed_replacement` was invoked at the bottom of this file and never
# defined. The harness runs under `set -uo pipefail` with NO errexit, so bash printed
# "command not found" to stderr, the status was discarded, and the summary still said
# "80 passed, 0 failed" — through ELEVEN gates. A suite that can report success while a named case never
# runs is the vacuity failure one level up from the individual asserts: every non-vacuity probe in here
# was guarding its own case while the HARNESS had no guard at all.
#
# Both halves are closed: an undefined name is a FAILURE rather than a silent no-op, and a test that
# returns non-zero without having called `fail` is also a failure — otherwise an early `return 1` inside a
# case would vanish the same way.
t() {
  local name="$1" rc=0
  if ! declare -F "$name" >/dev/null 2>&1; then
    fail "harness: test function '$name' is INVOKED but UNDEFINED — it has never run"
    return 0
  fi
  "$name" || rc=$?
  [[ "$rc" -eq 0 ]] || fail "harness: test function '$name' returned non-zero ($rc) without reporting a failure"
}

# skip: an ENVIRONMENTAL non-result (e.g. a live control process that never
# scheduled within the wait cap) — explicitly reported, never counted as failure.
skip() {
  SKIP_COUNT=$((SKIP_COUNT + 1))
  echo "SKIP: $1"
}

TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/cqlite-supervisor-test.XXXXXX")"
T_LOCKFN="$TMP_ROOT/lockfn"

# THE WHOLE SUITE GETS A PRIVATE `TMPDIR`, AND IT IS LOAD-BEARING, NOT HYGIENE (#3549, lead ruling
# 2026-08-30). `supervisor_legacy_lock_guard` now runs on EVERY start with no opt-out, and the path it
# tests is `${TMPDIR:-/tmp}/cqlite-worker-supervisor.lock`. Left on the real `/tmp`, every case in this
# file that starts a supervisor would consult a MACHINE-WIDE path this suite does not own: on a box
# where a pre-#3467 supervisor has ever run (or where a stale lock sits) the whole file would refuse,
# and on a box where it has not, the cases would pass for a reason that is nobody's property. Both are
# the same defect — a test whose verdict depends on state outside the checkout. Set here, ONCE, so it
# holds for cases that never call `common_env` too, and stable for the whole run so nothing inherits a
# removed case directory. Cases that need a chosen `TMPDIR` (the legacy-lock drives) still pass their
# own, and the three lane-id cases pin `TMPDIR=/tmp` explicitly; nothing here overrides those.
export TMPDIR="$TMP_ROOT/tmpdir"
mkdir -p "$TMPDIR"

# ---------------------------------------------------------------------------
# Background fixture processes: process-GROUP launch, OWNERSHIP-CHECKED reap (#3549, roborev jobs
# 196 F2 + 198 F2)
# ---------------------------------------------------------------------------
# THE LEAK THIS CLOSES (job 196 F2). Cases stage REAL processes — this suite's standing technique,
# because a staged string tests the RULE and not the PROBE — and several of those fixtures are a SHELL
# THAT FORKS A CHILD: `bash <script-whose-body-is-sleep-300>`, `sh -c 'sleep 300; :' …`. Killing the pid
# the case recorded kills only the SHELL. The `sleep 300` is NOT an exec replacement, so it is orphaned,
# reparented, and sits on the box for five minutes holding whatever descriptors it inherited — which is
# not merely untidy: THIS BOX RUNS FOUR LANES, so every run degraded a shared machine, and a held output
# descriptor can make a harness look hung.
#
# THE FIX IS STRUCTURAL, NOT PER CALL SITE. `fixture_bg` enables job control for exactly the duration of
# the spawn (`set -m`), which makes the background job a PROCESS GROUP LEADER whose pgid equals its pid.
# Everything it forks inherits that group, so ONE `kill -- -<pgid>` reaps the whole tree WHATEVER it
# turned out to spawn — including children a future fixture adds without anyone re-reading this comment.
# It is also why a fixture that DOES `exec` needs no special case: a group of one reaps identically.
#
# AND THE REAP MUST NEVER SIGNAL A GROUP IT NO LONGER OWNS (#3549, roborev job 198 F2). THE PREVIOUS
# VERSION OF THIS BLOCK WAS MORE DANGEROUS THAN THE LEAK IT FIXED. It kept one HISTORICAL list and sent
# TERM and then KILL to every pgid in it on every reap — including groups that had already been cleaned
# up per case, and including groups reaped by an earlier call. A pgid is a PID NUMBER and pid numbers are
# REUSED, so on this four-lane box the suite could deliver SIGKILL to an unrelated process GROUP: a
# sibling lane's supervisor, its gate, its worker. The old comment reasoned only about a false test
# FAILURE from a recycled number; the destructive direction is the one that matters.
#
# SO OWNERSHIP IS TRACKED, AND IT IS SURRENDERED AS SOON AS IT CAN BE:
#   * `FIXTURE_OWNED` holds one record per group this run CURRENTLY owns. A group is REMOVED from it the
#     moment we can prove we no longer own it, which happens BEFORE any later signal can reach it.
#   * AND OWNERSHIP IS SURRENDERED AT THE POINT OF REAPING, NOT AT TEARDOWN (#3549, roborev job 203 F2).
#     A `wait`ed fixture is dead; leaving it registered until the next probe means the next probe asks
#     `kill -0` about a NUMBER that may already have been recycled, gets a truthful `live` about someone
#     else, and signals them. So every reap goes through `fixture_wait`, which waits, clears the group
#     and UNREGISTERS — and `fixture_kill` routes through it, making that the ONE place a registered
#     fixture is reaped. No call site may `wait` a fixture directly; a structural case pins that.
#   * `fixture_group_state` is the proof, and it is THREE-VALUED for the same reason the code under test
#     is: `kill -0` on a negative pid fails with ESRCH (the group is gone — release it) and with EPERM
#     (the group EXISTS and we may not signal it, i.e. the number now belongs to somebody else — release
#     it, and NEVER signal it). Only an affirmatively `live` group is signalled at all.
#   * SIGNAL-TIME INCARNATION CHECK, where the host can answer it. `fixture_bg` records the group
#     leader's procfs start time; before signalling, if the leader pid is alive and its start time
#     DIFFERS from the recorded one, the pid number has been reused and the group is released
#     UNSIGNALLED (`FIXTURE_FOREIGN`), never killed. This is a REFUTATION-ONLY use: it can prove a group
#     is not ours, and it is not required to prove that it IS.
#
# WHY THE CHECK CANNOT BE A PRECONDITION FOR SIGNALLING. The leak this reaps is an ORPHANED CHILD whose
# leader — the shell — has ALREADY EXITED, so "the leader is alive and matches" is FALSE for exactly the
# case that matters most. Requiring it would refuse to reap the orphan. And on a host with no procfs
# (macOS) there is no start time at all. So the registry is the primary authority and the incarnation
# check only ever REMOVES a group from it. RESIDUAL, stated rather than implied: a group that dies
# between `fixture_group_state` and the `kill` a moment later, whose number is then immediately reused,
# is still signalled. That window is irreducible for any check-then-act on a pid number; what the
# registry removes is the LARGE window — a group reaped minutes ago and re-signalled at every later reap.
#
# WHY NOT `pgrep -f`, HERE OR IN THE ASSERT: this box runs sibling lanes staging the same fixture names,
# and `pgrep` also self-matches. A pid/pgid recorded at spawn time is the only identity that is ours.
FIXTURE_OWNED=()      # records "<pgid>|<leader-start-time>" for the groups this run CURRENTLY owns
FIXTURE_FOREIGN=()    # pgids released WITHOUT being signalled, because the number was proven reused
FIXTURE_STAGED=0      # monotone count of groups ever staged — the non-vacuity floor, since FIXTURE_OWNED shrinks
FIXTURE_LAST_PID=""
FIXTURE_WAIT_STATUS=0 # status of the last pid `fixture_wait` waited for (see why it is not an exit code)

# fixture_leader_ident <pid> — echo an incarnation token for a pid (procfs start time), or NOTHING when
# this host cannot answer (no procfs, e.g. macOS; an unreadable or unparseable `stat`). EMPTY MEANS
# UNMEASURED and is never compared as a value.
fixture_leader_ident() {
  local pid="$1" line="" rest=""
  local -a f=()
  [[ -r "/proc/$pid/stat" ]] || return 0
  IFS= read -r line <"/proc/$pid/stat" 2>/dev/null || return 0
  # `comm` (field 2) is parenthesised and may itself contain spaces and parentheses, so the fields are
  # taken from AFTER THE LAST `) ` — the documented way to parse this file. `rest` field 1 is then
  # `state` (stat field 3), so `starttime` (stat field 22) is `rest` field 20.
  rest="${line##*) }"
  [[ "$rest" != "$line" ]] || return 0
  f=($rest)
  [[ "${#f[@]}" -ge 20 ]] || return 0
  printf '%s' "${f[19]}"
}

# fixture_group_state <pgid> — echo `live`, `dead` or `foreign`. `foreign` = the group exists and we are
# NOT permitted to signal it, which on this box means the number has been recycled by another user's
# process group; it must never be signalled and must not be counted as a leak of ours.
fixture_group_state() {
  local pgid="$1" err=""
  if err="$(LC_ALL=C kill -0 "-$pgid" 2>&1)"; then
    printf 'live\n'
    return 0
  fi
  case "$err" in
    *'not permitted'*) printf 'foreign\n' ;;
    *) printf 'dead\n' ;;
  esac
}

# fixture_bg <cmd> [arg...] — start a background fixture in its OWN process group and register it as
# OWNED. Sets `FIXTURE_LAST_PID` (== the pgid); it CANNOT echo the pid, because `$(fixture_bg …)` would
# run the append to `FIXTURE_OWNED` in a subshell and lose the registration.
# Redirections belong at the CALL SITE (`fixture_bg tail -f "$f" >/dev/null 2>&1`): they are applied by
# this function's caller and inherited by the job, so nothing is forced on fixtures that need a tty-less
# stdin or a captured stream.
fixture_bg() {
  local had_m=off
  case "$-" in *m*) had_m=on ;; esac
  set -m
  "$@" &
  FIXTURE_LAST_PID=$!
  # Restore the previous monitor state rather than blindly clearing it.
  [[ "$had_m" == on ]] || set +m
  FIXTURE_OWNED+=("$FIXTURE_LAST_PID|$(fixture_leader_ident "$FIXTURE_LAST_PID")")
  FIXTURE_STAGED=$((FIXTURE_STAGED + 1))
}

# fixture_release_unowned — drop from `FIXTURE_OWNED` every group we can PROVE we no longer own (dead,
# or existing-but-unsignallable). Called before every signalling pass, so a released group can never be
# signalled by a later reap.
fixture_release_unowned() {
  local rec pgid
  local -a keep=()
  for rec in ${FIXTURE_OWNED[@]+"${FIXTURE_OWNED[@]}"}; do
    pgid="${rec%%|*}"
    case "$(fixture_group_state "$pgid")" in
      live) keep+=("$rec") ;;
      foreign) FIXTURE_FOREIGN+=("$pgid") ;;
      *) ;;  # dead: released, and never signalled again
    esac
  done
  FIXTURE_OWNED=(${keep[@]+"${keep[@]}"})
  return 0
}

# fixture_signal_owned <sig> [pgid...] — signal ONLY groups this run currently owns. With no pgid
# argument every owned group is signalled; with arguments, only those (per-case teardown). A group whose
# leader incarnation is refuted is released UNSIGNALLED.
fixture_signal_owned() {
  local sig="$1" rec pgid ident now want=""
  shift
  [[ "$#" -eq 0 ]] || want=" $* "
  local -a keep=()
  for rec in ${FIXTURE_OWNED[@]+"${FIXTURE_OWNED[@]}"}; do
    pgid="${rec%%|*}"; ident="${rec#*|}"
    if [[ -n "$want" && "$want" != *" $pgid "* ]]; then
      keep+=("$rec")
      continue
    fi
    case "$(fixture_group_state "$pgid")" in
      dead) continue ;;                                  # released
      foreign) FIXTURE_FOREIGN+=("$pgid"); continue ;;    # released, UNSIGNALLED
      *) ;;
    esac
    now="$(fixture_leader_ident "$pgid")"
    if [[ -n "$ident" && -n "$now" && "$ident" != "$now" ]]; then
      # The leader pid is alive and is NOT the incarnation staged here: the number was recycled, so this
      # group is not ours. Released without a signal — killing it would hit somebody else's tree.
      FIXTURE_FOREIGN+=("$pgid")
      continue
    fi
    kill "-$sig" "-$pgid" 2>/dev/null || true
    keep+=("$rec")
  done
  FIXTURE_OWNED=(${keep[@]+"${keep[@]}"})
  return 0
}

# fixture_unregister <pgid>... — SURRENDER OWNERSHIP of a group, unconditionally and WITHOUT signalling
# it. This is the only operation that removes a record on the strength of something WE did rather than
# something we PROVED about the group, and it is sound for exactly one caller: `fixture_wait`, which has
# already reaped the group. `fixture_release_unowned` cannot serve there, because its proof is
# `kill -0` on a pgid NUMBER — and after a reap the number is precisely what may have been recycled.
fixture_unregister() {
  local rec pgid drop
  local -a keep=()
  [[ "$#" -gt 0 ]] || return 0
  drop=" $* "
  for rec in ${FIXTURE_OWNED[@]+"${FIXTURE_OWNED[@]}"}; do
    pgid="${rec%%|*}"
    [[ "$drop" != *" $pgid "* ]] || continue
    keep+=("$rec")
  done
  FIXTURE_OWNED=(${keep[@]+"${keep[@]}"})
  return 0
}

# fixture_wait <pid>... — WAIT for a fixture, REAP WHATEVER REMAINS OF ITS GROUP, AND SURRENDER
# OWNERSHIP. THE ONLY SANCTIONED WAY TO WAIT FOR A REGISTERED FIXTURE.
#
# THE LEAK THIS CLOSES (#3549, roborev job 203 F2) — THE SAME DESTRUCTIVE-SIGNAL CLASS THE REGISTRY WAS
# INTRODUCED FOR, ONE STEP FURTHER IN. Job 198 F2 stopped the reap iterating a HISTORICAL list; what it
# left is that a fixture which has been `wait`ed is DEAD YET STILL REGISTERED. Nine cases staged a
# `sleep 0.1` for a genuinely-reaped pid and waited it DIRECTLY, so its pgid sat in `FIXTURE_OWNED` for
# the rest of the run. `fixture_release_unowned` cannot see the problem: its only evidence is `kill -0`
# on that NUMBER, and once the kernel hands the number to an unrelated same-user process GROUP the probe
# answers `live` — truthfully, about somebody else. The reap then delivers TERM and KILL to it. On this
# four-lane box that is plausibly a sibling lane's supervisor, gate or worker.
#
# WHY THE INCARNATION TOKEN IS NOT THE MECHANISM. It is procfs-only, so it does not exist on macOS, and
# it is refutation-only by design (`fixture_bg`'s comment): a reaped leader has no `/proc/<pid>/stat` at
# all, so there is nothing to compare and the recycled group is not refuted. Correctness therefore may
# not depend on it — it is a bonus that catches SOME recycles on Linux, never the guarantee.
#
# WHY A CHOKE POINT AND NOT N CORRECT CALL SITES. This class has now recurred twice (jobs 198, 203), and
# both times the previous fix was correct and left the NEXT site raw — the signal that per-call-site
# correctness is the wrong shape. So `fixture_kill` routes through here too, making this the single
# place a registered fixture is reaped: `wait`, clear the group, unregister. No path reaps without
# surrendering, and `test_fixture_wait_surrenders_ownership` pins that structurally as well.
#
# THE ORDER IS LOAD-BEARING. `wait` first (the leader is ours to reap and this shell must clear its job
# table), then signal the group WHILE OWNERSHIP IS STILL ESTABLISHED — a non-`exec` fixture's orphaned
# child can hold the group after its shell exits, which is the ORIGINAL leak (job 196 F2), so
# unregistering without that sweep would trade a destructive signal for a silent five-minute leak the
# end-of-run leak assert could no longer see. Only then unregister.
#
# STATUS IS RETURNED IN A VARIABLE, NOT AS AN EXIT CODE, DELIBERATELY: `t()` fails a test function that
# returns non-zero, so a `fixture_wait` on a TERMed fixture (status 143) as a case's LAST statement
# would fail the case. `FIXTURE_WAIT_STATUS` carries the last waited status for any caller that wants it.
fixture_wait() {
  local pid st
  FIXTURE_WAIT_STATUS=0
  for pid in "$@"; do
    [[ -n "$pid" ]] || continue
    st=0
    wait "$pid" 2>/dev/null || st=$?
    FIXTURE_WAIT_STATUS="$st"
    fixture_signal_owned TERM "$pid"
    # Only pay the settle for a group that is still holding the number after TERM. For the common
    # already-exited fixture `fixture_signal_owned` has already released it and this is a no-op.
    if [[ "$(fixture_group_state "$pid")" == live ]]; then
      sleep 0.2
      fixture_signal_owned KILL "$pid"
    fi
    fixture_unregister "$pid"
  done
  return 0
}

# fixture_kill <pid>... — per-case teardown: TERM the whole GROUP of each named fixture (only if still
# owned), then hand it to `fixture_wait`, which reaps the direct child (no zombie in this shell's job
# table), clears any survivor of the group and SURRENDERS OWNERSHIP. A group that is already gone stays
# gone: it is released, not re-signalled by the end-of-run reap.
fixture_kill() {
  local pid
  for pid in "$@"; do
    [[ -n "$pid" ]] || continue
    fixture_signal_owned TERM "$pid"
    fixture_wait "$pid"
  done
  fixture_release_unowned
  return 0
}

# fixture_live_groups — echo every group still OWNED and live after a release pass. Anything here after
# `fixture_reap` is a leak of ours: still alive, and still provably ours to reap.
# Residual, stated: a recycled pgid whose leader we cannot refute (no procfs) could read as live. That
# direction is a LOUD FALSE FAILURE, never a silent pass, which is the acceptable one for a leak check.
fixture_live_groups() {
  local rec
  fixture_release_unowned
  for rec in ${FIXTURE_OWNED[@]+"${FIXTURE_OWNED[@]}"}; do
    printf '%s\n' "${rec%%|*}"
  done
  return 0
}

# fixture_reap — TERM then KILL every group this run still OWNS, releasing each group the moment it can
# be proven gone (or proven not ours). Idempotent, and after the first call it signals NOTHING, because
# every group it reaped has been released. Called from the EXIT trap AND from the end-of-run assert.
#
# THE RELEASE PASS BEFORE EACH SIGNALLING PASS IS THE WHOLE POINT, AND IT IS NOT ABOUT TEST HYGIENE
# (#3549, roborev job 198 F2): a signal aimed at a pgid this run has already reaped is a signal aimed at
# whatever now holds that NUMBER, and on a four-lane box that is plausibly a sibling lane's supervisor,
# gate or worker being SIGKILLed by somebody else's test suite. `scripts/tests/` has no licence to
# signal a process it did not start. Do not "simplify" this back to iterating a historical list.
fixture_reap() {
  local sig
  for sig in TERM KILL; do
    fixture_release_unowned
    fixture_signal_owned "$sig"
    sleep 0.2
  done
  fixture_release_unowned
  return 0
}

# REAP BEFORE `rm -rf`: a fixture still running out of `$TMP_ROOT` holds descriptors into it.
cleanup() {
  fixture_reap
  rm -rf "$TMP_ROOT" 2>/dev/null || true
}
trap cleanup EXIT
# INT/TERM: cleanup, then exit with the conventional 128+signal status. The EXIT trap fires again on the
# way out and `cleanup` is idempotent.
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM

new_case_dir() {
  local d
  d="$(mktemp -d "$TMP_ROOT/case.XXXXXX")"
  mkdir -p "$d/bin" "$d/logs"
  echo "$d"
}

# ---------------------------------------------------------------------------
# Stub writers
# ---------------------------------------------------------------------------
write_notify_stub() {
  cat >"$1" <<'EOF'
#!/usr/bin/env bash
# $NOTIFY_CMD convention (issue #3119): THREE positional args, <severity>
# <title> <message>. The old `--category <cat>` flag form is gone — the real
# upstream agent-notify has no such arm and silently swallowed it.
printf '%s|%s|%s\n' "${1:-}" "${2:-}" "${3:-}" >>"${NOTIFY_LOG:?NOTIFY_LOG not set}"
EOF
  chmod +x "$1"
}

# Always finalizes; issue number = contents of $2, incremented each call.
# Optional $3 = seconds to sleep after writing the marker, before exit.
write_finalize_stub() {
  local path="$1" counter_file="$2" sleep_s="${3:-0}"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
n=0
[[ -f "$counter_file" ]] && n=\$(cat "$counter_file")
n=\$((n + 1))
echo "\$n" >"$counter_file"
cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":\$n,"pr":"https://github.com/pmcfadin/cqlite/pull/\$n","duration_s":1}
JSON
sleep "$sleep_s"
EOF
  chmod +x "$path"
}

# issue #2841 (design decision A): a HEALTHY worker that emits activity to stdout
# (as a real `claude -p --output-format stream-json --verbose` worker does) BEFORE
# writing its finalize marker — so the supervisor's `>"$logfile"` redirect captures a
# NON-EMPTY iter-N.log. Proves the watchdog has a live stream to scan under `-p`.
write_verbose_finalize_stub() {
  local path="$1" counter_file="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
n=0
[[ -f "$counter_file" ]] && n=\$(cat "$counter_file")
n=\$((n + 1))
echo "\$n" >"$counter_file"
# Stream-style activity to stdout (captured into iter-N.log by the supervisor).
echo '{"type":"system","subtype":"init"}'
echo '{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash"}]}}'
echo '{"type":"assistant","message":{"content":[{"type":"text","text":"dispatching subagent"}]}}'
cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":\$n,"pr":"https://github.com/pmcfadin/cqlite/pull/\$n","duration_s":1}
JSON
EOF
  chmod +x "$path"
}

# Finalize stub that always claims the SAME issue/PR (roborev 1839): used to exercise
# the per-PR auto-merge-stuck path (the same PR observed unmerged N times), distinct
# from write_finalize_stub's incrementing PR (used for the healthy distinct-PR case).
write_fixed_pr_finalize_stub() {
  local path="$1" counter_file="$2" pr="$3"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
n=0
[[ -f "$counter_file" ]] && n=\$(cat "$counter_file")
n=\$((n + 1))
echo "\$n" >"$counter_file"
cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":$pr,"pr":"https://github.com/pmcfadin/cqlite/pull/$pr","duration_s":1}
JSON
EOF
  chmod +x "$path"
}

# Always exits 1 without writing a marker (abnormal iteration).
write_abnormal_stub() {
  cat >"$1" <<'EOF'
#!/usr/bin/env bash
exit 1
EOF
  chmod +x "$1"
}

# First call: outcome=no-work. Every call after: outcome=finalized (issue
# counter separate from the call counter, so budget-vs-no-work is unambiguous).
write_nowork_then_finalize_stub() {
  local path="$1" call_ctr="$2" issue_ctr="$3"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"no-work","issue":null,"pr":null,"duration_s":1}
JSON
else
  n=0
  [[ -f "$issue_ctr" ]] && n=\$(cat "$issue_ctr")
  n=\$((n + 1))
  echo "\$n" >"$issue_ctr"
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":\$n,"pr":"https://github.com/pmcfadin/cqlite/pull/\$n","duration_s":1}
JSON
fi
EOF
  chmod +x "$path"
}

# F2 regression: writes outcome=blocked with a fixed issue number on every
# call (never finalizes) — used to prove the supervisor stops after the SAME
# issue reports blocked on two consecutive iterations, rather than looping.
write_blocked_same_issue_stub() {
  local path="$1" issue="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cat >"\$MARKER_FILE" <<JSON
{"outcome":"blocked","issue":$issue,"pr":null,"duration_s":1,"reason":"needs owner decision"}
JSON
EOF
  chmod +x "$path"
}

# Parks on the EXACT reason token the supervisor keys on (#3393 round 20). Note the sibling stub above
# uses free text ("needs owner decision"), which is deliberately NOT a park token — that is why it
# retains its issue and this one releases it.
write_park_stub() {
  local path="$1" issue="$2" reason="$3"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cat >"\$MARKER_FILE" <<JSON
{"outcome":"blocked","issue":$issue,"pr":null,"duration_s":1,"reason":"$reason"}
JSON
EOF
  chmod +x "$path"
}

# F5 regression: writes outcome=finalized with issue set but pr MISSING
# entirely (not just null) — the marker contract requires BOTH issue and pr
# on "finalized"; a marker missing either must be judged abnormal.
write_finalize_missing_pr_stub() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cat >"$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":42,"duration_s":1}
JSON
EOF
  chmod +x "$path"
}

# F3 regression: writes outcome=blocked with a "reason" containing a double
# quote and an embedded literal newline (via printf %b), to prove the journal
# line stays valid JSON end-to-end (marker_field's python3 read handles the
# marker side; journal_line's json_or_null handles the journal-write side).
write_blocked_nasty_reason_stub() {
  local path="$1"
  cat >"$path" <<'PYEOF'
#!/usr/bin/env bash
set -euo pipefail
python3 - "$MARKER_FILE" <<'PY'
import json, sys
d = {"outcome": "blocked", "issue": 55, "pr": None, "duration_s": 1,
     "reason": 'has a "quote" and\na newline'}
open(sys.argv[1], "w").write(json.dumps(d))
PY
PYEOF
  chmod +x "$path"
}

# Fails loudly (sentinel + exit 1) if the marker file is already present at
# start (proves the supervisor removed a stale marker before spawning).
write_stale_check_stub() {
  local path="$1" sentinel="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
if [[ -f "\$MARKER_FILE" ]]; then
  touch "$sentinel"
  exit 1
fi
cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":1,"pr":"https://github.com/pmcfadin/cqlite/pull/1","duration_s":1}
JSON
EOF
  chmod +x "$path"
}

# issue #2666 CLEAN PARK: first call writes a `blocked` marker with a park
# reason (seam1-approval | needs-decision) and an optional one-line question;
# every call after finalizes. Used to prove a park is judged parked-on-owner,
# fires a high page, never trips the breaker, and the loop advances.
write_park_then_finalize_stub() {
  local path="$1" call_ctr="$2" reason="$3" question="${4:-}"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"blocked","issue":77,"pr":null,"duration_s":1,"reason":"$reason","question":"$question"}
JSON
else
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":78,"pr":"https://github.com/pmcfadin/cqlite/pull/78","duration_s":1}
JSON
fi
EOF
  chmod +x "$path"
}

# issue #2666: writes a marker with an UNKNOWN outcome value — must be judged
# abnormal (counts toward the breaker), never silently trusted.
write_unknown_outcome_stub() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cat >"$MARKER_FILE" <<JSON
{"outcome":"weird-outcome","issue":9,"pr":null,"duration_s":1}
JSON
EOF
  chmod +x "$path"
}

# issue #2666 stuck-on-question: first call prints an interactive-prompt line
# then sleeps past MAX_ITER_SECS (so the watchdog detects + pages and it gets
# timeout-killed WITHOUT a marker); every call after finalizes so the test
# terminates at MAX_ISSUES.
write_stuck_then_finalize_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  echo "AskUserQuestion: Do you want to proceed with option A?"
  sleep 120
else
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":88,"pr":"https://github.com/pmcfadin/cqlite/pull/88","duration_s":1}
JSON
fi
EOF
  chmod +x "$path"
}

# issue #2666 / roborev 1769: abnormal → stuck → abnormal → abnormal → finalize.
# The stuck iteration (call 2) must RESET the consecutive-abnormal counter so the
# crash chain is broken and BREAKER_N=3 never trips; call 5 finalizes so the run
# terminates at MAX_ISSUES.
write_abnormal_stuck_abnormal_finalize_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
case \$calls in
  1|3|4) exit 1 ;;
  2)
    echo "AskUserQuestion: choose an option"
    sleep 120 ;;
  *)
    cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":91,"pr":"https://github.com/pmcfadin/cqlite/pull/91","duration_s":1}
JSON
    ;;
esac
EOF
  chmod +x "$path"
}

# issue #2666 / roborev 1769: parks the SAME issue with a park reason on EVERY
# call (never finalizes) — proves the park-path head-block guard stops after the
# same issue parks on two consecutive iterations.
write_park_same_issue_stub() {
  local path="$1" issue="$2" reason="${3:-needs-decision}"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cat >"\$MARKER_FILE" <<JSON
{"outcome":"blocked","issue":$issue,"pr":null,"duration_s":1,"reason":"$reason","question":"same question"}
JSON
EOF
  chmod +x "$path"
}

# issue #2666 / roborev 1769: parks issue 41, then a DIFFERENT issue 42, then
# finalizes — proves distinct-issue parks do NOT trip the head-block guard.
write_park_two_issues_then_finalize_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
case \$calls in
  1) cat >"\$MARKER_FILE" <<JSON
{"outcome":"blocked","issue":41,"pr":null,"duration_s":1,"reason":"needs-decision","question":"q41"}
JSON
    ;;
  2) cat >"\$MARKER_FILE" <<JSON
{"outcome":"blocked","issue":42,"pr":null,"duration_s":1,"reason":"needs-decision","question":"q42"}
JSON
    ;;
  *) cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":43,"pr":"https://github.com/pmcfadin/cqlite/pull/43","duration_s":1}
JSON
    ;;
esac
EOF
  chmod +x "$path"
}

# issue #2666 / roborev 1773 (case a): prints a stray signature line, then keeps
# WRITING many lines (log grows + signature scrolls out of the tail) and exits 1
# WITHOUT a marker. No wedge evidence → must stay ABNORMAL (counts to breaker),
# never misclassified as stuck.
write_crash_stray_signature_stub() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
echo "AskUserQuestion: stray tool-name printed in normal trace"
for i in $(seq 1 60); do
  echo "working on step $i ..."
  sleep 0.1
done
exit 1
EOF
  chmod +x "$path"
}

# issue #2666 / roborev 1773 (case c): a BUSY worker that prints the signature on
# EVERY line (so it is always in the tail) but keeps WRITING (log grows every
# scan) — the no-growth evidence fails, so it must NOT be classified stuck. Call 1
# runs until killed at the deadline (abnormal); call 2 finalizes to terminate.
write_busy_signature_then_finalize_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  while true; do
    echo "AskUserQuestion tick — still working, writing more output"
    sleep 0.3
  done
else
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":95,"pr":"https://github.com/pmcfadin/cqlite/pull/95","duration_s":1}
JSON
fi
EOF
  chmod +x "$path"
}

# issue #2670: a gh-verify stub that FAILS (exit 1, no output → unverified) on the
# first call and returns MERGED JSON on every call after. Used to prove an
# unverified finalize is not counted and does not trip the breaker, while still
# terminating the run (the second, verified-merged finalize hits MAX_ISSUES=1).
write_gh_flaky_then_merged_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  exit 1
fi
printf %s '{"state":"MERGED","mergedAt":"2026-01-01T00:00:00Z"}'
EOF
  chmod +x "$path"
}

# issue #2670 (roborev 1810): a gh-verify stub that ALWAYS fails transport (exit 1,
# NO stderr → unverified). Used to prove UNVERIFIED_MAX consecutive unverified
# finalizes stop the loop (verify-unavailable).
write_gh_transport_fail_stub() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
exit 1
EOF
  chmod +x "$path"
}

# issue #2670 (roborev 1810): a gh-verify stub that emulates `gh pr view` on a PR
# number that does NOT exist — a resolve failure (stderr signature + nonzero exit),
# distinct from a transport outage. verify_finalized_pr must classify this
# mismatch:UNRESOLVED (forged marker), not unverified.
write_gh_notfound_stub() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
echo "GraphQL: Could not resolve to a PullRequest with the number of $1. (repository.pullRequest)" >&2
exit 1
EOF
  chmod +x "$path"
}

# issue #2670 (roborev 1810): worker finalizes with a FORGED pr on every call —
# call 1 a not-found number (999999), call 2 a garbage non-numeric string — both
# must be judged abnormal mismatch:UNRESOLVED. Never finalizes cleanly.
write_finalize_forged_pr_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
set -euo pipefail
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":61,"pr":"999999","duration_s":1}
JSON
else
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":62,"pr":"not-a-real-pr","duration_s":1}
JSON
fi
EOF
  chmod +x "$path"
}

# issue #2670 (roborev 1813): gh-verify stub whose FIRST read reports OPEN and
# every read after reports MERGED — proves the mismatch-grace retry absorbs
# read-after-merge lag (ends up merged, never mismatch).
write_gh_open_then_merged_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  printf %s '{"state":"OPEN","mergedAt":null,"autoMergeRequest":null}'
else
  printf %s '{"state":"MERGED","mergedAt":"2026-01-01T00:00:00Z","autoMergeRequest":null}'
fi
EOF
  chmod +x "$path"
}

# issue #2670 (roborev 1813): gh-verify stub — FIRST read OPEN with auto-merge
# ARMED (pending-automerge verdict), every read after MERGED (so a following
# iteration terminates the run). Proves the auto-merge path is not a false mismatch.
write_gh_automerge_then_merged_stub() {
  local path="$1" call_ctr="$2"
  cat >"$path" <<EOF
#!/usr/bin/env bash
calls=0
[[ -f "$call_ctr" ]] && calls=\$(cat "$call_ctr")
calls=\$((calls + 1))
echo "\$calls" >"$call_ctr"
if [[ \$calls -eq 1 ]]; then
  printf %s '{"state":"OPEN","mergedAt":null,"autoMergeRequest":{"enabledAt":"2026-01-01T00:00:00Z"}}'
else
  printf %s '{"state":"MERGED","mergedAt":"2026-01-01T00:00:00Z","autoMergeRequest":null}'
fi
EOF
  chmod +x "$path"
}

# issue #2670 (roborev 1813, finding 4): worker finalizes with a FOREIGN-host PR
# URL (correct path shape, wrong host/repo) — must classify mismatch:UNRESOLVED,
# never merged. Never finalizes cleanly.
write_finalize_foreign_url_stub() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cat >"$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":71,"pr":"https://github.com/evil/other/pull/5","duration_s":1}
JSON
EOF
  chmod +x "$path"
}

# ---------------------------------------------------------------------------
# R7 (issue #3119): the DEFAULT notify path — the one production actually uses.
#
# Every other case in this file injects NOTIFY_CMD with a recording stub, so the
# DEFAULT resolution (NOTIFY_ARGV=(bash <repo>/scripts/lib/gate-notify.sh --publish))
# and the wrapper's `--publish` arm were exercised by NO test. Two mutations survived
# green: reverting the default to bare `agent-notify` — whose pristine 3-positional
# mode puts the SEVERITY in the title slot, i.e. the original defect of this issue,
# reintroduced silently — and breaking the `--publish` arm outright.
#
# So: NOTIFY_CMD UNSET, a curl-capture shim on PATH, and a pre-created stop-file so
# finalize_exit fires immediately. What is asserted is the PUBLISHED payload, which
# only the real default chain can produce.
# ---------------------------------------------------------------------------
test_default_notify_path_publishes() {
  local d curl_log rc title
  d="$(new_case_dir)"
  common_env "$d"
  # THE point of the case: no injected notify command.
  unset NOTIFY_CMD
  curl_log="$d/curl.log"; : >"$curl_log"
  cat >"$d/bin/curl" <<'CURLSHIM'
#!/usr/bin/env bash
body=""; prev=""
for a in "$@"; do
  [[ "$prev" == "-d" ]] && body="$a"
  prev="$a"
done
printf '%s\n' "$body" >>"$CURL_LOG"
CURLSHIM
  chmod +x "$d/bin/curl"
  export CURL_LOG="$curl_log"
  export CQLITE_NOTIFY_WEBHOOK="https://ntfy.invalid/r7-default-path"
  export CODEX_NOTIFY_WEBHOOK= CQLITE_NOTIFY_TOPIC= CODEX_NOTIFY_NTFY_TOPIC=
  export PATH="$d/bin:$PATH"
  touch "$STOP_FILE"   # stop at the first loop top -> finalize_exit -> notify
  timeout -s KILL 60 bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  # The stop page is an `info`, so the wrapper must publish priority 3 and put the
  # supervisor's own TITLE in the title field — not a severity token (the pristine
  # `agent-notify` positional bug) and not nothing (a broken --publish arm).
  title=$(python3 - "$curl_log" <<'PYP'
import json, sys
for line in open(sys.argv[1]):
    line = line.strip()
    if not line:
        continue
    try:
        d = json.loads(line)
    except Exception:
        continue
    print("%s|%s" % (d.get("title", ""), d.get("priority", "")))
    break
PYP
)
  unset CURL_LOG CQLITE_NOTIFY_WEBHOOK CODEX_NOTIFY_WEBHOOK CQLITE_NOTIFY_TOPIC CODEX_NOTIFY_NTFY_TOPIC
  if [[ "$rc" -eq 0 && "$title" == "worker-supervisor stopped|3" ]]; then
    pass "R7 default notify path: NOTIFY_CMD unset -> wrapper published title='worker-supervisor stopped' priority=3"
  else
    fail "R7 default notify path: rc=$rc published='$title' (expected 'worker-supervisor stopped|3'; see $curl_log)"
  fi
}

# ---------------------------------------------------------------------------
# Common env baseline: every test starts here, then overrides what it needs.
# Clear preflight (no holds), generous budgets, fast polling/backoff.
# ---------------------------------------------------------------------------
common_env() {
  local d="$1"
  export MARKER_FILE="$d/marker.json"
  export STOP_FILE="$d/stop"
  export LOG_DIR="$d/logs"
  export JOURNAL_FILE="$d/logs/journal.jsonl"
  export SUPERVISOR_LOCK="$d/lock"
  export NOTIFY_LOG="$d/notify.log"
  : >"$NOTIFY_LOG"
  write_notify_stub "$d/bin/notify.sh"
  export NOTIFY_CMD="$d/bin/notify.sh"
  # The per-issue LOCK seam off by default: `REPO_ROOT` is the REAL lane checkout in these cases, so its
  # branch genuinely names an issue and the legacy-claim migration would fire a network `claim.sh status`
  # in every one of them. Dedicated cases below supply a stub instead.
  export LOCK_CMD=""
  export LOAD_PROBE_CMD="echo 0"
  export DISK_PROBE_CMD="echo 999999"
  # The #3749 shared-object-store sweep OFF by default: `REPO_ROOT` is the REAL lane
  # checkout in these cases, so a sweep here would `git fsck` this box's 366M shared store
  # once per case — 13-24s warm and 47-80s cold or under concurrent gates (two independent
  # measurement sets), i.e. MANY minutes added to a MANDATORY gate component for a
  # property these cases are not about. The dedicated cases below override it and point the
  # supervisor at their own scratch tree with a stub sweep, so the branch coverage is real
  # rather than mocked away.
  export OBJ_SWEEP_INTERVAL_HOURS=0
  # roborev 1839: preflight bounds the two leftover families separately, so it reads
  # per-family probes (the old combined PROC_PROBE_CMD is gone). Default both to clear;
  # leftover-hold tests override the family they exercise.
  export PROC_PROBE_WORKER_CMD="echo 0"
  export PROC_PROBE_BUILD_CMD="echo 0"
  # issue #2670: every "finalized" marker is now GH-verified. Default the mock to
  # MERGED so all pre-existing finalize-based cases credit the issue as before;
  # verification tests override GH_VERIFY_CMD to exercise mismatch/unverified.
  export GH_VERIFY_CMD='printf %s "{\"state\":\"MERGED\",\"mergedAt\":\"2026-01-01T00:00:00Z\",\"autoMergeRequest\":null}"'
  # roborev 1813: mismatch grace re-reads gh a few times; keep the wait at 0 so no
  # test ever sleeps for it.
  export MISMATCH_RETRY_WAIT_SECS=0
  export LOAD_MAX=999999
  export MAX_ISSUES=100
  export MAX_HOURS=8
  export BREAKER_N=3
  export BACKOFF_NOWORK_SECS=1
  export HOLD_POLL_SECS=1
  export MAX_ITER_SECS=10
  export STUCK_POLL_SECS=1
  unset LOAD_CONTROL_FILE 2>/dev/null || true
  unset WORKER_CMD 2>/dev/null || true
  # Reset every knob a test may override but common_env does not explicitly re-set, so
  # one test's override (e.g. MAX_HOURS_SECS=3, PENDING_AUTOMERGE_*) cannot leak into the
  # next test in this shared shell and cause a spurious pass/fail.
  unset MAX_HOURS_SECS DISK_FLOOR_GB PENDING_AUTOMERGE_MAX PENDING_AUTOMERGE_MIN_SECS \
        BUILD_HOLD_MAX LEFTOVER_HOLD_MAX UNVERIFIED_MAX MISMATCH_RETRIES \
        MISMATCH_GRACE_CAP_SECS PROC_LIST_WORKER_CMD PROC_LIST_BUILD_CMD \
        OBJ_SWEEP_STAMP OBJ_SWEEP_TIMEOUT_SECS 2>/dev/null || true
  # Claim stamping (issue #2655) OFF by default so most tests stay focused; the
  # dedicated claim tests set a hermetic CLAIM_CMD stub that logs its args.
  export CLAIM_CMD=""
  unset HEARTBEAT_MACHINE 2>/dev/null || true
}

jline_count() { grep -c "$2" "$1" 2>/dev/null || true; }

# A hermetic claim-heartbeat.sh stand-in: append "<subcmd> <args...>" to
# $CLAIM_LOG on every call, always succeed. Lets a test assert the supervisor
# invoked `stamp`/`reap` with the right shape, without any origin/network.
write_claim_stub() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${CLAIM_LOG:?CLAIM_LOG not set}"
# `stamp` prints the sha it wrote on STDOUT (roborev round 19), which the supervisor captures and
# passes back as a `reap` LEASE. A stub that printed nothing would exercise the NO-lease path instead,
# and every lease assertion below would pass vacuously while proving the opposite of the property.
# Fixed and hex so assertions can name it exactly.
[ "${1:-}" = stamp ] && printf 'deadbeefdeadbeefdeadbeefdeadbeefdeadbeef\n'
exit 0
EOF
  chmod +x "$path"
}

# write_claim_stub_failing_issue_stamp <path> — logs every call like the normal stub but FAILS any
# `stamp <numeric-issue> ...`, i.e. the replacement stamp of a lane transition (#3393, roborev round
# 2). Used to prove the transition cannot open a liveness gap: the OLD ref must survive a failed
# replacement, because a lane with no claim ref at all is invisible to dead-lanes and the reaper.
write_claim_stub_failing_issue_stamp() {
  local path="$1"
  cat >"$path" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${CLAIM_LOG:?CLAIM_LOG not set}"
if [ "${1:-}" = "stamp" ]; then
  case "${2:-}" in
    p*) exit 0 ;;          # the placeholder stamp still succeeds
    *[!0-9]*) exit 0 ;;
    '') exit 0 ;;
    *) exit 1 ;;           # an ISSUE-named stamp fails: the replacement cannot land
  esac
fi
exit 0
EOF
  chmod +x "$path"
}

# ---------------------------------------------------------------------------
# Test 1: happy path — 2 finalized iterations, then MAX_ISSUES=2 budget stop.
# ---------------------------------------------------------------------------
test_happy_path_budget_stop() {
  local d counter jf rc fcount scount
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=2
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  scount=$(jline_count "$jf" '"outcome":"summary"')
  if [[ "$rc" -eq 0 && "$fcount" -eq 2 && "$scount" -eq 1 ]] && grep -q '"reason":"budget-issues"' "$jf"; then
    pass "happy path: 2 finalized + budget-issues summary stop"
  else
    fail "happy path: rc=$rc finalized=$fcount summary=$scount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 2: crash-loop breaker — N abnormal exits stop + alert, no hot respawn.
# ---------------------------------------------------------------------------
test_breaker_stops_on_abnormal() {
  local d jf rc acount ncount
  d="$(new_case_dir)"
  common_env "$d"
  write_abnormal_stub "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export BREAKER_N=3
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  ncount=$(grep -c '^error|.*BREAKER' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && "$acount" -eq 3 ]] && grep -q '"reason":"breaker"' "$jf" && [[ "$ncount" -ge 1 ]]; then
    pass "breaker: 3 consecutive abnormal exits stop with ALERT, no hot respawn"
  else
    fail "breaker: rc=$rc abnormal=$acount notify_breaker=$ncount (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 3: stop-file honored between iterations (clean exit, exactly 1 ran).
# ---------------------------------------------------------------------------
test_stop_file_honored() {
  local d counter jf sup_pid rc waited
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  # Sleep after the marker write so the test can create the stop-file while
  # this iteration is still "in flight" — guarantees the NEXT loop-top check
  # sees it, with no race on how fast the stub itself runs.
  write_finalize_stub "$d/bin/worker.sh" "$counter" 1
  export WORKER_CMD="$d/bin/worker.sh"
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 &
  sup_pid=$!
  waited=0
  while [[ ! -f "$counter" && "$waited" -lt 100 ]]; do
    sleep 0.1
    waited=$((waited + 1))
  done
  touch "$STOP_FILE"
  wait "$sup_pid"
  rc=$?
  local fcount
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  if [[ "$rc" -eq 0 && "$fcount" -eq 1 ]] && grep -q '"reason":"stop-file"' "$jf"; then
    pass "stop-file: honored between iterations, exactly 1 ran"
  else
    fail "stop-file: rc=$rc finalized=$fcount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 4: preflight hold — high load blocks the spawn until it clears.
# ---------------------------------------------------------------------------
test_preflight_load_hold() {
  local d counter sup_pid rc hold_notifies
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export LOAD_CONTROL_FILE="$d/load"
  echo 99 >"$LOAD_CONTROL_FILE"
  # shellcheck disable=SC2016  # deferred: expanded later by the supervisor's own `bash -c`, not here.
  export LOAD_PROBE_CMD='cat "$LOAD_CONTROL_FILE"'
  export LOAD_MAX=1

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 &
  sup_pid=$!
  # Load-proof: this suite is now gate-wired (agent-gate.sh tooling-tests), so a
  # fixed sleep-then-assert window flakes when the box is busy. POLL (hard-capped
  # at 30s) until the HOLD notify appears instead — the semantic is unchanged
  # (HOLD fires while load is high, spawn is deferred). Load stays pinned high
  # (LOAD_CONTROL_FILE=99) throughout, so the spawn cannot happen until we clear
  # it below; the counter must remain absent the whole time.
  local waited=0
  hold_notifies=0
  while [[ "$waited" -lt 300 ]]; do
    hold_notifies=$(grep -c '^error|worker-supervisor HOLD|HOLD: load' "$NOTIFY_LOG" 2>/dev/null || true)
    [[ "$hold_notifies" -ge 1 ]] && break
    sleep 0.1
    waited=$((waited + 1))
  done
  local invoked_while_high="no"
  [[ -f "$counter" ]] && invoked_while_high="yes"

  echo 0 >"$LOAD_CONTROL_FILE"
  waited=0
  while [[ ! -f "$counter" && "$waited" -lt 300 ]]; do
    sleep 0.1
    waited=$((waited + 1))
  done
  wait "$sup_pid"
  rc=$?

  if [[ "$invoked_while_high" == "no" && "$rc" -eq 0 && -f "$counter" && "$hold_notifies" -ge 1 ]]; then
    pass "preflight: high load holds the spawn (no invoke), then proceeds once clear (HOLD notify fired)"
  else
    fail "preflight: invoked_while_high=$invoked_while_high rc=$rc hold_notifies=$hold_notifies (see $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 5: no-work backoff does NOT count against MAX_ISSUES.
# ---------------------------------------------------------------------------
test_nowork_not_counted() {
  local d call_ctr issue_ctr jf rc
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  issue_ctr="$d/issues"
  common_env "$d"
  write_nowork_then_finalize_stub "$d/bin/worker.sh" "$call_ctr" "$issue_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  local nwcount fcount calls
  nwcount=$(jline_count "$jf" '"outcome":"no-work"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  calls=$(cat "$call_ctr" 2>/dev/null || echo -1)
  # MAX_ISSUES=1: if no-work counted, the supervisor would stop after the
  # very first (no-work) iteration and never reach a second, finalizing call.
  if [[ "$rc" -eq 0 && "$nwcount" -eq 1 && "$fcount" -eq 1 && "$calls" -eq 2 ]]; then
    pass "no-work: backoff sleeps but does not count toward MAX_ISSUES"
  else
    fail "no-work: rc=$rc no-work=$nwcount finalized=$fcount calls=$calls (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 6: single-instance lock — second concurrent invocation refuses loudly.
# ---------------------------------------------------------------------------
test_single_instance_lock() {
  local d counter pid_a rc_b stderr_b
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter" 3
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100

  bash "$SUPERVISOR" >"$d/stdout_a.log" 2>&1 &
  pid_a=$!
  local waited=0
  while [[ ! -f "$counter" && "$waited" -lt 50 ]]; do
    sleep 0.1
    waited=$((waited + 1))
  done

  bash "$SUPERVISOR" >"$d/stdout_b.log" 2>&1
  rc_b=$?
  stderr_b="$(cat "$d/stdout_b.log")"

  kill "$pid_a" 2>/dev/null || true
  wait "$pid_a" 2>/dev/null || true

  if [[ "$rc_b" -ne 0 ]] && echo "$stderr_b" | grep -q "already running" && echo "$stderr_b" | grep -q "pid $pid_a"; then
    pass "flock: second concurrent instance refuses loudly with holder pid"
  else
    fail "flock: rc_b=$rc_b pid_a=$pid_a stderr_b='$stderr_b'"
  fi
}

# ---------------------------------------------------------------------------
# Test 7: stale marker is removed before spawn (never re-judged).
# ---------------------------------------------------------------------------
test_stale_marker_removed() {
  local d sentinel jf rc
  d="$(new_case_dir)"
  sentinel="$d/stale-detected"
  common_env "$d"
  write_stale_check_stub "$d/bin/worker.sh" "$sentinel"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  jf="$JOURNAL_FILE"
  mkdir -p "$(dirname "$MARKER_FILE")"
  echo '{"outcome":"finalized","issue":999,"pr":"stale"}' >"$MARKER_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  if [[ "$rc" -eq 0 && ! -f "$sentinel" ]] && grep -q '"issue":1,' "$jf" && ! grep -q '"issue":999' "$jf"; then
    pass "stale marker: removed before spawn, fresh outcome judged (issue 1, not stale 999)"
  else
    fail "stale marker: rc=$rc sentinel_present=$([[ -f "$sentinel" ]] && echo yes || echo no) (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 8 (F2 regression): the SAME issue reports "blocked" on two consecutive
# iterations → supervisor stops after the second with a head-blocked notify,
# clean exit, and never reaches MAX_HOURS/MAX_ISSUES.
# ---------------------------------------------------------------------------
test_repeated_blocked_head_of_queue_stops() {
  local d jf rc bcount hb_notifies
  d="$(new_case_dir)"
  common_env "$d"
  write_blocked_same_issue_stub "$d/bin/worker.sh" 7
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  bcount=$(jline_count "$jf" '"outcome":"blocked"')
  hb_notifies=$(grep -c '^error|.*persistently blocked' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$bcount" -eq 2 ]] && grep -q '"reason":"head-blocked"' "$jf" && [[ "$hb_notifies" -ge 1 ]]; then
    pass "F2: same issue blocked twice in a row stops cleanly with head-blocked notify"
  else
    fail "F2: rc=$rc blocked_iters=$bcount head_blocked_notifies=$hb_notifies (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 9 (F5 regression): a "finalized" marker missing "pr" is judged
# abnormal — ISSUES_DONE must not advance, and it must count toward the
# crash-loop breaker (proven here by tripping BREAKER_N=1 immediately).
# ---------------------------------------------------------------------------
test_finalized_missing_pr_is_abnormal() {
  local d jf rc fcount acount
  d="$(new_case_dir)"
  common_env "$d"
  write_finalize_missing_pr_stub "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  if [[ "$rc" -ne 0 && "$fcount" -eq 0 && "$acount" -eq 1 ]] && grep -q '"reason":"breaker"' "$jf"; then
    pass "F5: finalized marker missing pr is judged abnormal, not counted done"
  else
    fail "F5: rc=$rc finalized=$fcount abnormal=$acount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 10 (F3 regression): a "reason" containing a double-quote and an
# embedded newline must still produce a journal line that parses as valid
# JSON — proves journal_line's json_or_null escaping (not just printf %s).
# ---------------------------------------------------------------------------
test_journal_escapes_nasty_reason() {
  local d jf rc line all_valid
  d="$(new_case_dir)"
  common_env "$d"
  write_blocked_nasty_reason_stub "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  if [[ ! -f "$jf" ]]; then
    fail "F3: no journal file written ($jf)"
    return
  fi
  all_valid="yes"
  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    printf '%s' "$line" | python3 -c 'import json,sys; json.loads(sys.stdin.read())' 2>/dev/null || all_valid="no"
  done <"$jf"
  if [[ "$rc" -eq 0 && "$all_valid" == "yes" ]] && grep -q '"outcome":"blocked"' "$jf"; then
    pass "F3: reason with embedded quote+newline still yields valid JSON journal lines"
  else
    fail "F3: rc=$rc all_valid=$all_valid (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 11 (#2666): blocked/seam1-approval is a CLEAN PARK → verdict
# parked-on-owner, ONE high-priority page, never abnormal, never trips the
# breaker (BREAKER_N=1 here would stop before finalizing if it did), and the
# loop advances to the next issue.
# ---------------------------------------------------------------------------
test_park_seam1_parked_on_owner() {
  local d call_ctr jf rc pcount fcount page
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_park_then_finalize_stub "$d/bin/worker.sh" "$call_ctr" "seam1-approval" ""
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  pcount=$(jline_count "$jf" '"outcome":"parked-on-owner"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  page=$(grep -c '^error|worker-supervisor: parked issue 77' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$pcount" -eq 1 && "$fcount" -eq 1 && "$page" -ge 1 ]] &&
     ! grep -q '"outcome":"abnormal"' "$jf" && ! grep -q '"reason":"breaker"' "$jf"; then
    pass "park(seam1-approval): parked-on-owner + high page, no breaker, loop advances"
  else
    fail "park(seam1): rc=$rc parked=$pcount finalized=$fcount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 12 (#2666): blocked/needs-decision parks the same way AND the page title
# carries the marker's one-line "question" field (issue # + first line).
# ---------------------------------------------------------------------------
test_park_needs_decision_question_in_title() {
  local d call_ctr jf rc pcount page
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_park_then_finalize_stub "$d/bin/worker.sh" "$call_ctr" "needs-decision" "Which compaction strategy for wide rows?"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  pcount=$(jline_count "$jf" '"outcome":"parked-on-owner"')
  page=$(grep -c 'parked issue 77 — Which compaction strategy for wide rows?' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$pcount" -eq 1 && "$page" -ge 1 ]] && grep -q '"reason":"needs-decision"' "$jf"; then
    pass "park(needs-decision): parked-on-owner + question text in the page title"
  else
    fail "park(needs-decision): rc=$rc parked=$pcount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 13 (#2666): a marker with an UNKNOWN outcome value is still judged
# abnormal (counts toward the breaker) — parks must not have widened the set of
# "trusted" outcomes.
# ---------------------------------------------------------------------------
test_unknown_outcome_is_abnormal() {
  local d jf rc acount pcount
  d="$(new_case_dir)"
  common_env "$d"
  write_unknown_outcome_stub "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  pcount=$(jline_count "$jf" '"outcome":"parked-on-owner"')
  if [[ "$rc" -ne 0 && "$acount" -eq 1 && "$pcount" -eq 0 ]] && grep -q '"reason":"breaker"' "$jf"; then
    pass "unknown outcome: judged abnormal, trips breaker (not parked/trusted)"
  else
    fail "unknown outcome: rc=$rc abnormal=$acount parked=$pcount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 14 (#2666): a worker WEDGED on an interactive prompt is detected
# mid-iteration → immediate high page + verdict stuck-on-question when it exits
# without a marker; NEVER abnormal, never trips the breaker (BREAKER_N=1 here).
# The second iteration finalizes so the run terminates at MAX_ISSUES.
# ---------------------------------------------------------------------------
test_stuck_on_question_detected() {
  local d call_ctr jf rc scount fcount page
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_stuck_then_finalize_stub "$d/bin/worker.sh" "$call_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  # Generous deadline headroom: the watchdog detects on its first poll (~1s in);
  # a large MAX_ITER_SECS keeps detection well ahead of the deadline-kill even
  # under a heavily loaded box (the wedged stub sleeps 120s, so it is always
  # killed by the deadline, never by exiting on its own).
  export MAX_ITER_SECS=10
  export STUCK_POLL_SECS=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  scount=$(jline_count "$jf" '"outcome":"stuck-on-question"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  page=$(grep -c '^error|worker-supervisor: stuck-on-question' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$scount" -eq 1 && "$fcount" -eq 1 && "$page" -ge 1 ]] &&
     ! grep -q '"outcome":"abnormal"' "$jf" && ! grep -q '"reason":"breaker"' "$jf" &&
     grep -q 'AskUserQuestion' "$NOTIFY_LOG"; then
    pass "stuck-on-question: detected mid-iteration, high page, no breaker, loop advances"
  else
    fail "stuck-on-question: rc=$rc stuck=$scount finalized=$fcount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 15 (#2666): unit-test the prompt-signature grep directly by SOURCING the
# supervisor (the source-guard keeps main() from running) and calling
# detect_prompt_signature/captured_question against fixture logs — fires on a
# menu block, stays silent on a clean log, and captures the question text.
# ---------------------------------------------------------------------------
test_prompt_signature_grep() {
  local d fixture clean out
  d="$(new_case_dir)"
  fixture="$d/iter.log"
  clean="$d/clean.log"
  printf 'building project...\nrunning tests\n\xe2\x9d\xaf 1. Yes\n  2. No\n' >"$fixture"
  printf 'building project...\nall tests green\nfinalized issue 5\n' >"$clean"

  out="$(SUPERVISOR="$SUPERVISOR" FIX="$fixture" CLN="$clean" bash -c '
    # shellcheck disable=SC1090
    source "$SUPERVISOR"
    set +e
    if detect_prompt_signature "$FIX"; then echo MATCH; else echo NOMATCH; fi
    if detect_prompt_signature "$CLN"; then echo CLEAN-MATCH; else echo CLEAN-NOMATCH; fi
    captured_question "$FIX"
  ' 2>/dev/null)"

  if echo "$out" | grep -q '^MATCH$' && echo "$out" | grep -q '^CLEAN-NOMATCH$' &&
     echo "$out" | grep -q '1. Yes'; then
    pass "prompt-signature grep: fires on menu block, silent on clean log, captures text"
  else
    fail "prompt-signature grep: out='$out'"
  fi
}

# ---------------------------------------------------------------------------
# Test 16 (#2666 / roborev 1769): a stuck-on-question iteration RESETS the
# consecutive-abnormal counter — the chain abnormal→stuck→abnormal→abnormal must
# NOT trip a BREAKER_N=3 breaker (the stuck iteration breaks the chain). Call 5
# finalizes so the run terminates at MAX_ISSUES=1.
# ---------------------------------------------------------------------------
test_stuck_breaks_abnormal_chain() {
  local d call_ctr jf rc scount acount fcount
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_abnormal_stuck_abnormal_finalize_stub "$d/bin/worker.sh" "$call_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=3
  # Headroom so the stuck iteration (call 2) is reliably DETECTED (not
  # deadline-killed before the first poll) even under load — the whole point of
  # this test is that a detected stuck iteration resets the abnormal chain.
  export MAX_ITER_SECS=10
  export STUCK_POLL_SECS=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  scount=$(jline_count "$jf" '"outcome":"stuck-on-question"')
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  if [[ "$rc" -eq 0 && "$scount" -eq 1 && "$acount" -eq 3 && "$fcount" -eq 1 ]] &&
     ! grep -q '"reason":"breaker"' "$jf"; then
    pass "stuck breaks abnormal chain: 3 abnormals split by a stuck iter never trip BREAKER_N=3"
  else
    fail "stuck-chain: rc=$rc stuck=$scount abnormal=$acount finalized=$fcount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 17 (#2666 / roborev 1769): the SAME issue parking on two consecutive
# iterations → head-block-on-decision page + clean stop (mirrors the F2
# blocked-path guard); never loops to MAX_ISSUES.
# ---------------------------------------------------------------------------
test_repeated_park_same_issue_stops() {
  local d jf rc pcount hb
  d="$(new_case_dir)"
  common_env "$d"
  write_park_same_issue_stub "$d/bin/worker.sh" 33 "needs-decision"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  pcount=$(jline_count "$jf" '"outcome":"parked-on-owner"')
  hb=$(grep -c '^error|worker-supervisor: issue 33 head-blocked on decision' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$pcount" -eq 2 && "$hb" -ge 1 ]] && grep -q '"reason":"head-blocked-decision"' "$jf"; then
    pass "repeated park (same issue): head-blocked-on-decision page + clean stop after 2"
  else
    fail "repeated-park: rc=$rc parked=$pcount head_block=$hb (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 18 (#2666 / roborev 1769): parks of DIFFERENT issues do NOT trip the
# head-block guard — issue 41 then 42 park, then a finalize terminates the run.
# ---------------------------------------------------------------------------
test_different_issue_parks_do_not_head_block() {
  local d call_ctr jf rc pcount fcount
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_park_two_issues_then_finalize_stub "$d/bin/worker.sh" "$call_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=100
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  pcount=$(jline_count "$jf" '"outcome":"parked-on-owner"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  if [[ "$rc" -eq 0 && "$pcount" -eq 2 && "$fcount" -eq 1 ]] &&
     ! grep -q '"reason":"head-blocked-decision"' "$jf" && ! grep -q 'head-blocked on decision' "$NOTIFY_LOG"; then
    pass "different-issue parks: no head-block, loop advances through both then finalizes"
  else
    fail "different-park: rc=$rc parked=$pcount finalized=$fcount (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 19 (#2666 / roborev 1773, case a): a crash whose ONLY signature is a stray
# match in scrollback (log grew + match scrolled out of the tail) must stay
# ABNORMAL and count toward the breaker — NOT be misclassified as stuck.
# ---------------------------------------------------------------------------
test_stray_signature_scrollback_is_abnormal() {
  local d jf rc acount scount
  d="$(new_case_dir)"
  common_env "$d"
  write_crash_stray_signature_stub "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=1
  export STUCK_POLL_SECS=1
  export MAX_ITER_SECS=20
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  scount=$(jline_count "$jf" '"outcome":"stuck-on-question"')
  if [[ "$rc" -ne 0 && "$acount" -eq 1 && "$scount" -eq 0 ]] && grep -q '"reason":"breaker"' "$jf"; then
    pass "stray scrollback signature: crash stays ABNORMAL (breaker), not stuck"
  else
    fail "stray-scrollback: rc=$rc abnormal=$acount stuck=$scount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 20 (#2666 / roborev 1773, case b): a GENUINE wedge — alive + signature in
# the tail + log frozen across two consecutive scans → stuck-on-question, high
# page, never toward the breaker. Call 2 finalizes so the run terminates.
# ---------------------------------------------------------------------------
test_genuine_wedge_frozen_is_stuck() {
  local d call_ctr jf rc scount fcount page
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_stuck_then_finalize_stub "$d/bin/worker.sh" "$call_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  export STUCK_POLL_SECS=1
  export MAX_ITER_SECS=10
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  scount=$(jline_count "$jf" '"outcome":"stuck-on-question"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  page=$(grep -c '^error|worker-supervisor: stuck-on-question' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$scount" -eq 1 && "$fcount" -eq 1 && "$page" -ge 1 ]] &&
     ! grep -q '"outcome":"abnormal"' "$jf" && ! grep -q '"reason":"breaker"' "$jf"; then
    pass "genuine wedge (frozen log + tail signature x2 polls): stuck, high page, no breaker"
  else
    fail "genuine-wedge: rc=$rc stuck=$scount finalized=$fcount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 21 (#2666 / roborev 1773, case c): a BUSY worker printing the signature on
# every line while STILL WRITING (log grows between scans) → no-growth evidence
# fails → NOT stuck (marker-less kill stays abnormal). Call 2 finalizes.
# ---------------------------------------------------------------------------
test_busy_writing_signature_not_stuck() {
  local d call_ctr jf rc scount acount fcount
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_busy_signature_then_finalize_stub "$d/bin/worker.sh" "$call_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=2
  export STUCK_POLL_SECS=1
  export MAX_ITER_SECS=5
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  scount=$(jline_count "$jf" '"outcome":"stuck-on-question"')
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  if [[ "$rc" -eq 0 && "$scount" -eq 0 && "$acount" -eq 1 && "$fcount" -eq 1 ]]; then
    pass "busy worker printing signature while writing: NOT stuck (growth defeats it)"
  else
    fail "busy-writing: rc=$rc stuck=$scount abnormal=$acount finalized=$fcount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 22 (#2666 / roborev 1773, case d): exit-latency — a fast-finalizing worker
# is judged on the ~1s exit cadence, NOT held until the 30s wedge-scan cadence.
# Loose, load-proof cap (well under STUCK_POLL_SECS=30).
# ---------------------------------------------------------------------------
test_fast_exit_latency() {
  local d counter t0 t1 elapsed rc fcount
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export STUCK_POLL_SECS=30
  export MAX_ITER_SECS=7200

  t0=$(date +%s)
  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  t1=$(date +%s)
  elapsed=$((t1 - t0))
  fcount=$(jline_count "$JOURNAL_FILE" '"outcome":"finalized"')
  if [[ "$rc" -eq 0 && "$fcount" -eq 1 && "$elapsed" -lt 15 ]]; then
    pass "exit-latency: fast finalize judged in ${elapsed}s (<15s, not the 30s scan cadence)"
  else
    fail "exit-latency: rc=$rc finalized=$fcount elapsed=${elapsed}s (see $JOURNAL_FILE)"
  fi
}

# ---------------------------------------------------------------------------
# Test 23 (#2670): a "finalized" marker whose PR gh-verifies as MERGED is
# credited normally — outcome finalized, journal `verified: merged`, counted
# toward MAX_ISSUES (proven by the budget-issues stop).
# ---------------------------------------------------------------------------
test_finalized_verified_merged_counts() {
  local d counter jf rc fcount
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  # explicit MERGED mock (common_env already defaults to this; pin it here so the
  # case is self-describing)
  export GH_VERIFY_CMD='printf %s "{\"state\":\"MERGED\",\"mergedAt\":\"2026-01-01T00:00:00Z\"}"'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  if [[ "$rc" -eq 0 && "$fcount" -eq 1 ]] &&
     grep -q '"outcome":"finalized".*"verified":"merged"' "$jf" &&
     grep -q '"reason":"budget-issues"' "$jf"; then
    pass "verify(merged): finalized credited, journal verified=merged, counts to budget"
  else
    fail "verify(merged): rc=$rc finalized=$fcount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 23-claim (#2655): the supervisor STAMPS refs/lane-claims/<machine>/<issue> before each
# spawn and CLEARS (reap) it on a clean exit — via CLAIM_CMD, mechanically, without the
# worker LLM. A hermetic CLAIM_CMD stub logs every invocation; we assert one
# `stamp <issue> <pid>` per iteration and exactly one `reap <machine>` at stop.
# ---------------------------------------------------------------------------
test_claim_stamp_each_iter_and_clear_on_exit() {
  local d counter jf rc stamps reaps
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=2
  export CLAIM_LOG="$d/claim.log"
  : >"$CLAIM_LOG"
  write_claim_stub "$d/bin/claim.sh"
  export CLAIM_CMD="bash $d/bin/claim.sh"
  export HEARTBEAT_MACHINE="testbox"
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  # 2 finalized iterations => 2 stamps; a clean budget stop => exactly 1 reap.
  stamps=$(grep -c '^stamp ' "$CLAIM_LOG" 2>/dev/null || true)
  reaps=$(grep -c '^reap testbox' "$CLAIM_LOG" 2>/dev/null || true)
  # Every stamp carries a LANE ID then the SUPERVISOR pid (#3393). The lane id is the issue number
  # when known, or `p<pid>` when it is not — which is the case here, because `CLAIM_ISSUE` is
  # cleared on `finalized`, so a supervisor finalising issue after issue never knows its issue at
  # spawn time. The placeholder MUST be unique per supervisor: the old shared "0" made every
  # unknown-issue supervisor on a machine write the same per-lane ref, re-creating the masking that
  # per-lane refs exist to remove.
  local well_formed="yes"
  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    [[ "$line" =~ ^stamp\ ([0-9]+|p[0-9]+-[0-9a-f]+)\ [0-9]+$ ]] || well_formed="no"
    [[ "$line" =~ ^stamp\ 0\  ]] && well_formed="no"   # the shared placeholder must be gone
  done < <(grep '^stamp ' "$CLAIM_LOG" 2>/dev/null)
  # ...and both stamps must name the SAME placeholder, since it is this supervisor's identity.
  local uniq_ids
  uniq_ids=$(grep '^stamp ' "$CLAIM_LOG" 2>/dev/null | awk '{print $2}' | sort -u | wc -l | tr -d ' ')
  [[ "$uniq_ids" == "1" ]] || well_formed="no"
  if [[ "$rc" -eq 0 && "$stamps" -eq 2 && "$reaps" -eq 1 && "$well_formed" == "yes" ]]; then
    pass "claim: stamp per iteration (unique p<pid> lane id + supervisor pid, never the shared 0) + one reap on clean exit"
  else
    fail "claim: rc=$rc stamps=$stamps reaps=$reaps well_formed=$well_formed (see $CLAIM_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 24 (#2670): a "finalized" marker whose PR gh-verifies as OPEN is judged
# ABNORMAL — a HIGH page names the discrepancy, ISSUES_DONE does NOT advance
# (the false finalize is not counted), and it counts toward the breaker (proven
# by tripping BREAKER_N=1 immediately).
# ---------------------------------------------------------------------------
test_finalized_mismatch_open_is_abnormal() {
  local d counter jf rc fcount acount page
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=1
  export GH_VERIFY_CMD='printf %s "{\"state\":\"OPEN\",\"mergedAt\":null}"'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  page=$(grep -c '^error|worker-supervisor: finalized MISMATCH' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && "$fcount" -eq 0 && "$acount" -eq 1 && "$page" -ge 1 ]] &&
     grep -q '"outcome":"abnormal".*"verified":"mismatch:OPEN"' "$jf" &&
     grep -q '"reason":"breaker"' "$jf"; then
    pass "verify(mismatch OPEN): abnormal + high page, not counted, trips breaker"
  else
    fail "verify(mismatch): rc=$rc finalized=$fcount abnormal=$acount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 25 (#2670): gh unavailable → UNVERIFIED — outcome finalized-unverified
# (journal `verified: unverified`), NOT counted toward MAX_ISSUES, and NEUTRAL to
# the breaker (BREAKER_N=1 here must NOT trip on it). The gh mock fails on call 1
# (unverified) then returns MERGED, so the second, verified-merged finalize hits
# MAX_ISSUES=1 — proving the unverified iteration was not counted (else the run
# would have stopped before it).
# ---------------------------------------------------------------------------
test_finalized_unverified_not_counted_no_breaker() {
  local d counter gh_ctr jf rc ucount fcount page
  d="$(new_case_dir)"
  counter="$d/counter"
  gh_ctr="$d/gh-calls"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  write_gh_flaky_then_merged_stub "$d/bin/gh.sh" "$gh_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  # shellcheck disable=SC2016  # $1 expanded later by the supervisor's own `bash -c`.
  export GH_VERIFY_CMD="$d/bin/gh.sh \"\$1\""
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  ucount=$(jline_count "$jf" '"outcome":"finalized-unverified"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  page=$(grep -c 'finalized UNVERIFIED' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$ucount" -eq 1 && "$fcount" -eq 1 && "$page" -ge 1 ]] &&
     grep -q '"outcome":"finalized-unverified".*"verified":"unverified"' "$jf" &&
     ! grep -q '"reason":"breaker"' "$jf"; then
    pass "verify(unverified): finalized-unverified, not counted, breaker neutral, loop continues"
  else
    fail "verify(unverified): rc=$rc unverified=$ucount finalized=$fcount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 26 (#2670 / #2841): PROC_PROBE discriminates the supervisor's OWN
# unattended worker spawn shape (`claude … -p … --agent flow-lead …`, issue
# #2841) from a legitimate INTERACTIVE `claude --agent flow-lead` lead session
# (no `-p`) and a plain `claude` REPL. Portable PROPERTY proof is a pure-string
# `grep -E` regex check (always runs, deterministic); the live-process PID check
# is a bonus that SKIPs (never fails) if the control process never schedules
# within the wait cap — an environmental non-result, not a property failure
# (roborev 1819 finding 7).
# ---------------------------------------------------------------------------
test_proc_probe_discriminates_worker_claude() {
  # Source the ACTUAL pattern from the script (anti-drift): a regex edit that
  # broke discrimination would break this test, not silently pass a stale copy.
  local pat
  pat="$(grep -E "^PROC_MATCH_WORKER=" "$SUPERVISOR" | head -1 | sed -E "s/^PROC_MATCH_WORKER='(.*)'$/\1/")"
  # Pure-string property proof (no live process): the pattern matches the
  # unattended `-p` (and long-form `--print`) worker argv shape and NOT an
  # interactive lead / plain REPL. The `-p` MUST be matched as a whitespace-
  # delimited token (roborev #2841): a `claude --dangerously-skip-permissions
  # --agent flow-lead` interactive lead has a `-p` INSIDE `ski-p-ermissions` but
  # NO real print flag, and must NOT match.
  if ! printf "claude -p --output-format stream-json --verbose --dangerously-skip-permissions --agent flow-lead '/worker'\n" | grep -qE "$pat" ||
       ! printf "claude --print --dangerously-skip-permissions --agent flow-lead '/worker'\n" | grep -qE "$pat" ||
       printf 'claude --dangerously-skip-permissions --agent flow-lead review the board\n' | grep -qE "$pat" ||
       printf 'claude --agent flow-lead review the board\n' | grep -qE "$pat" ||
       printf 'claude\n' | grep -qE "$pat"; then
    fail "proc-probe: pure-string regex does not discriminate -p/--print worker vs interactive lead (incl. skip-permissions) / REPL"
    return
  fi
  # Bonus live check: spawn the three argv-shaped stubs and confirm the same
  # discrimination against real PIDs.
  bash -c "exec -a 'claude -p --dangerously-skip-permissions --agent flow-lead /worker' sleep 30" &
  local wpid=$!
  bash -c 'exec -a "claude --agent flow-lead review the board" sleep 30' &
  local ipid=$!
  bash -c 'exec -a "claude" sleep 30' &
  local rpid=$!
  local waited=0 control_up="no"
  while [[ "$waited" -lt 50 ]]; do
    pgrep -f "$pat" | grep -qw "$wpid" && { control_up="yes"; break; }
    sleep 0.1
    waited=$((waited + 1))
  done
  if [[ "$control_up" == "no" ]]; then
    kill "$wpid" "$ipid" "$rpid" 2>/dev/null || true
    wait "$wpid" "$ipid" "$rpid" 2>/dev/null || true
    skip "proc-probe (live): control worker process never scheduled within cap — pure-string proof held"
    return
  fi
  local worker_matched="yes" interactive_matched="no" repl_matched="no"
  pgrep -f "$pat" | grep -qw "$ipid" && interactive_matched="yes"
  pgrep -f "$pat" | grep -qw "$rpid" && repl_matched="yes"
  kill "$wpid" "$ipid" "$rpid" 2>/dev/null || true
  wait "$wpid" "$ipid" "$rpid" 2>/dev/null || true
  if [[ "$worker_matched" == "yes" && "$interactive_matched" == "no" && "$repl_matched" == "no" ]]; then
    pass "proc-probe: matches -p --agent flow-lead worker, excludes interactive lead + plain REPL"
  else
    fail "proc-probe: worker=$worker_matched interactive=$interactive_matched repl=$repl_matched"
  fi
}

# ---------------------------------------------------------------------------
# Test 27 (#2670 / roborev 1810 HIGH, 1839): a `leftover-worker` preflight hold (an
# orphaned worker CLI) that NEVER clears must STOP the supervisor loudly
# (leftover-worker, exit 1, high page naming survivors) after the TIGHT
# LEFTOVER_HOLD_MAX passes — not latch it silently until MAX_HOURS.
# ---------------------------------------------------------------------------
test_leftover_hold_bounded_stops() {
  local d counter jf rc page
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  # Worker-family probe never clears (always reports an orphaned worker CLI); the
  # worker would finalize if ever spawned (it must not be).
  export PROC_PROBE_WORKER_CMD="echo 1"
  export PROC_LIST_WORKER_CMD="echo '12345 claude --agent worker orphan'"
  export LEFTOVER_HOLD_MAX=3
  export HOLD_POLL_SECS=1
  export MAX_HOURS=8
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  page=$(grep -c '^error|worker-supervisor: leftover worker CLI will not clear' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && ! -f "$counter" && "$page" -ge 1 ]] &&
     grep -q '"reason":"leftover-worker"' "$jf" &&
     grep -q '12345' "$NOTIFY_LOG"; then
    pass "leftover-worker bound: never-clearing worker orphan stops loudly (exit 1, survivors named), no spawn"
  else
    fail "leftover-worker: rc=$rc spawned=$([[ -f "$counter" ]] && echo yes || echo no) page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 28 (#2670 / roborev 1810 MED): UNVERIFIED_MAX consecutive unverified
# finalizes STOP the supervisor (verify-unavailable, exit 1, high page) — a
# persistent verification outage must not let uncounted-forever iterations drift
# past the MAX_ISSUES ceiling.
# ---------------------------------------------------------------------------
test_persistent_unverified_stops() {
  local d counter jf rc ucount page
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  write_gh_transport_fail_stub "$d/bin/gh.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  export UNVERIFIED_MAX=2
  # shellcheck disable=SC2016  # $1 expanded later by the supervisor's own `bash -c`.
  export GH_VERIFY_CMD="$d/bin/gh.sh \"\$1\""
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  ucount=$(jline_count "$jf" '"outcome":"finalized-unverified"')
  page=$(grep -c '^error|worker-supervisor: verification unavailable' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && "$ucount" -eq 2 && "$page" -ge 1 ]] &&
     grep -q '"reason":"verify-unavailable"' "$jf"; then
    pass "persistent unverified: 2 consecutive stop the loop (verify-unavailable, exit 1, high page)"
  else
    fail "persistent-unverified: rc=$rc unverified=$ucount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 29 (#2670 / roborev 1810 MED): a FORGED `pr` is an escalation, not a blip —
# a gh-not-found number (999999) and a garbage non-numeric string both classify
# mismatch:UNRESOLVED (abnormal, high MISMATCH page, breaker-counting), NEVER
# unverified. BREAKER_N=2 stops after the two forged finalizes.
# ---------------------------------------------------------------------------
test_forged_pr_is_unresolved_mismatch() {
  local d call_ctr jf rc acount ucount page
  d="$(new_case_dir)"
  call_ctr="$d/calls"
  common_env "$d"
  write_finalize_forged_pr_stub "$d/bin/worker.sh" "$call_ctr"
  write_gh_notfound_stub "$d/bin/gh.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=2
  # shellcheck disable=SC2016  # $1 expanded later by the supervisor's own `bash -c`.
  export GH_VERIFY_CMD="$d/bin/gh.sh \"\$1\""
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  ucount=$(jline_count "$jf" '"outcome":"finalized-unverified"')
  page=$(grep -c '^error|worker-supervisor: finalized MISMATCH' "$NOTIFY_LOG" 2>/dev/null || true)
  # both forged finalizes → mismatch:UNRESOLVED (one via gh not-found, one via
  # shape-check), never unverified; breaker trips at 2.
  if [[ "$rc" -ne 0 && "$acount" -eq 2 && "$ucount" -eq 0 && "$page" -ge 2 ]] &&
     [[ "$(jline_count "$jf" '"verified":"mismatch:UNRESOLVED"')" -eq 2 ]] &&
     grep -q '"reason":"breaker"' "$jf"; then
    pass "forged pr: not-found number + garbage string both mismatch:UNRESOLVED (escalation, not unverified)"
  else
    fail "forged-pr: rc=$rc abnormal=$acount unverified=$ucount mismatch_page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 30 (#2670 / roborev 1810 HIGH): the bounded hold loop re-checks exit
# conditions on EVERY pass — a stop-file created WHILE preflight is holding (a
# non-leftover reason, so the leftover cap is not what stops it) exits cleanly
# from inside the hold loop, never spawning. Deterministic (no timing race): load
# stays pinned high until the stop-file lands.
# ---------------------------------------------------------------------------
test_stop_file_honored_mid_hold() {
  local d counter sup_pid rc hold_notifies
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export LOAD_CONTROL_FILE="$d/load"
  echo 99 >"$LOAD_CONTROL_FILE"
  # shellcheck disable=SC2016  # deferred: expanded later by the supervisor's own `bash -c`.
  export LOAD_PROBE_CMD='cat "$LOAD_CONTROL_FILE"'
  export LOAD_MAX=1

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 &
  sup_pid=$!
  local waited=0
  hold_notifies=0
  while [[ "$waited" -lt 300 ]]; do
    hold_notifies=$(grep -c '^error|worker-supervisor HOLD|HOLD: load' "$NOTIFY_LOG" 2>/dev/null || true)
    [[ "$hold_notifies" -ge 1 ]] && break
    sleep 0.1
    waited=$((waited + 1))
  done
  # stop while still holding (load never cleared)
  touch "$STOP_FILE"
  wait "$sup_pid"
  rc=$?
  if [[ "$rc" -eq 0 && ! -f "$counter" && "$hold_notifies" -ge 1 ]] &&
     grep -q '"reason":"stop-file"' "$JOURNAL_FILE"; then
    pass "stop-file mid-hold: bounded hold loop exits cleanly from inside the hold, no spawn"
  else
    fail "stop-mid-hold: rc=$rc spawned=$([[ -f "$counter" ]] && echo yes || echo no) holds=$hold_notifies (see $JOURNAL_FILE)"
  fi
}

# ---------------------------------------------------------------------------
# Test 31 (#2670 / roborev 1813 MED-HIGH): the DEFAULT proc probe must not count
# its own brace-group `bash -c` wrapper (whose argv carries the pattern text). On
# Linux a naive pattern matches that wrapper → a phantom leftover at EVERY boot,
# hard-stopping every supervisor (macOS `pgrep -f` happens not to, but the bracket
# trick is the portable fix). Proven portably + deterministically: a process whose
# argv holds the LITERAL pattern text (as the wrapper's does) must NOT match the
# bracketed pattern, while a REAL `claude --agent worker` process MUST. Plus a
# sanity run of the verbatim default probe: well-formed, and its worker-Claude
# sub-probe is 0 with no worker running.
test_probe_no_self_match() {
  # Source the ACTUAL pattern from the script (anti-drift, roborev #2841): a naive
  # hardcoded copy would silently desync from an edit to PROC_MATCH_WORKER.
  local pat
  pat="$(env -u PROC_MATCH_WORKER SUP="$SUPERVISOR" bash -c 'source "$SUP"; printf %s "$PROC_MATCH_WORKER"' 2>/dev/null)"
  # PROPERTY proof (pure-string, always runs, deterministic): the bracketed pattern
  # matches a REAL unattended `-p` worker argv, and does NOT match a process whose
  # argv literally contains the bracketed PATTERN TEXT (exactly as the probe's own
  # `bash -c` wrapper does) — `[c]laude` = literal `c`+`laude`; the text `[c]laude`
  # has `c` followed by `]`, so no match. This is the self-exclusion property.
  if ! printf "claude -p --dangerously-skip-permissions --agent flow-lead '/worker'\n" | grep -qE "$pat" ||
       printf 'wrap %s probe\n' "$pat" | grep -qE "$pat"; then
    fail "probe self-match: pure-string regex fails the self-exclusion property"
    return
  fi
  # static sanity: the real DEFAULT per-family probe strings (the ones preflight
  # ACTUALLY executes, roborev 1840) each carry the bracket trick + the $$/$PPID
  # self-exclusion. Source with both family probes unset so a leaked test override can't
  # mask the default; assert the strings, don't execute them.
  local worker_probe build_probe defaulted="no"
  # shellcheck disable=SC2016  # $SUP/$PROC_* expand inside the sub-bash, not here.
  worker_probe="$(env -u PROC_PROBE_WORKER_CMD SUP="$SUPERVISOR" bash -c 'source "$SUP"; printf %s "$PROC_PROBE_WORKER_CMD"' 2>/dev/null)"
  # shellcheck disable=SC2016
  build_probe="$(env -u PROC_PROBE_BUILD_CMD SUP="$SUPERVISOR" bash -c 'source "$SUP"; printf %s "$PROC_PROBE_BUILD_CMD"' 2>/dev/null)"
  [[ "$worker_probe" == *"$pat"* && "$worker_probe" == *'grep -vxF'* &&
     "$build_probe" == *'[c]argo '* && "$build_probe" == *'grep -vxF'* ]] && defaulted="yes"
  if [[ "$defaulted" != "yes" ]]; then
    fail "probe self-match: default per-family probe strings missing bracket trick / self-exclusion: worker='${worker_probe:0:50}' build='${build_probe:0:50}'"
    return
  fi
  # Bonus live check: real `claude -p … --agent flow-lead` stub matches, wrapper-text
  # stub does not. SKIPs (never fails) if the control worker never schedules.
  bash -c "exec -a 'claude -p --dangerously-skip-permissions --agent flow-lead /worker' sleep 30" &
  local wpid=$!
  bash -c "exec -a 'wrap $pat probe' sleep 30" &
  local xpid=$!
  local waited=0 control_up="no"
  while [[ "$waited" -lt 50 ]]; do
    pgrep -f "$pat" | grep -qw "$wpid" && { control_up="yes"; break; }
    sleep 0.1
    waited=$((waited + 1))
  done
  if [[ "$control_up" == "no" ]]; then
    kill "$wpid" "$xpid" 2>/dev/null || true
    wait "$wpid" "$xpid" 2>/dev/null || true
    skip "probe self-match (live): control worker process never scheduled within cap — pure-string proof held"
    return
  fi
  local wrapper_matched="no"
  pgrep -f "$pat" | grep -qw "$xpid" && wrapper_matched="yes"
  kill "$wpid" "$xpid" 2>/dev/null || true
  wait "$wpid" "$xpid" 2>/dev/null || true
  if [[ "$wrapper_matched" == "no" ]]; then
    pass "probe self-match: bracket trick matches a real worker, excludes the wrapper-argv text"
  else
    fail "probe self-match: wrapper-argv text was matched (self-exclusion broken)"
  fi
}

# ---------------------------------------------------------------------------
# Test 32 (#2670 / roborev 1813 MED): a tooling gap (NO json parser present) on a
# VALID gh response must read as `unverified` (transport class), NEVER
# mismatch:UNRESOLVED — a missing parser is our problem, not the worker's forgery.
# Unit-tests verify_finalized_pr under a PATH with jq/python3 removed.
test_parser_absent_is_unverified() {
  local d bindir t src out
  d="$(new_case_dir)"
  bindir="$d/nobin"
  mkdir -p "$bindir"
  # symlink only the tools sourcing + verify_finalized_pr's unverified path need
  # (dirname/date are used at source time); jq AND python3 are deliberately absent.
  for t in bash mktemp cat rm grep dirname date; do
    src="$(command -v "$t" 2>/dev/null)" && ln -s "$src" "$bindir/$t"
  done
  # shellcheck disable=SC2016  # $1 expands inside the sub-bash (source target), not here.
  out="$(PATH="$bindir" \
        GH_VERIFY_CMD='printf %s "{\"state\":\"MERGED\",\"autoMergeRequest\":null}"' \
        MISMATCH_RETRIES=1 MISMATCH_RETRY_WAIT_SECS=0 \
        "$bindir/bash" -c 'source "$1"; verify_finalized_pr 42' _ "$SUPERVISOR" 2>/dev/null)"
  if [[ "$out" == "unverified" ]]; then
    pass "parser-absent: no jq/python3 on a valid response → unverified (tooling gap, not forgery)"
  else
    fail "parser-absent: got '$out' (expected unverified)"
  fi
}

# ---------------------------------------------------------------------------
# Test 33 (#2670 / roborev 1813 MED, 1839): OPEN with auto-merge ARMED is a legitimate
# pending state, not a false finalize — verdict finalized-pending-automerge: NOT
# counted toward MAX_ISSUES immediately, default-priority page, breaker-NEUTRAL. The
# PR is re-verified on the NEXT iteration and, now MERGED, RETROACTIVELY credited
# (pending-credited), reaching MAX_ISSUES=1 — proving the armed PR both wasn't
# double-counted and wasn't lost.
test_pending_automerge_verdict() {
  local d counter gh_ctr jf rc pcount ccount page
  d="$(new_case_dir)"
  counter="$d/counter"
  gh_ctr="$d/gh-calls"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  write_gh_automerge_then_merged_stub "$d/bin/gh.sh" "$gh_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  # shellcheck disable=SC2016  # $1 expanded later by the supervisor's own `bash -c`.
  export GH_VERIFY_CMD="$d/bin/gh.sh \"\$1\""
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  pcount=$(jline_count "$jf" '"outcome":"finalized-pending-automerge"')
  ccount=$(jline_count "$jf" '"outcome":"pending-credited"')
  page=$(grep -c 'finalized PENDING AUTO-MERGE' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$pcount" -eq 1 && "$ccount" -eq 1 && "$page" -ge 1 ]] &&
     grep -q '"outcome":"finalized-pending-automerge".*"verified":"pending-automerge"' "$jf" &&
     grep -q '"outcome":"pending-credited".*"verified":"merged"' "$jf" &&
     grep -q '"reason":"budget-issues"' "$jf" &&
     ! grep -q '"reason":"breaker"' "$jf"; then
    pass "pending-automerge: armed → not counted yet, then retroactively credited on MERGED (breaker-neutral)"
  else
    fail "pending-automerge: rc=$rc pending=$pcount credited=$ccount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 34 (#2670 / roborev 1813 MED): mismatch grace absorbs read-after-merge lag —
# gh reports OPEN on read 1, MERGED on read 2, so the verdict is `merged` (counted),
# never a spurious mismatch. Proves the retry re-reads gh (call counter reaches 2).
test_mismatch_grace_absorbs_lag() {
  local d counter gh_ctr jf rc fcount acount calls
  d="$(new_case_dir)"
  counter="$d/counter"
  gh_ctr="$d/gh-calls"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  write_gh_open_then_merged_stub "$d/bin/gh.sh" "$gh_ctr"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  export MISMATCH_RETRIES=3
  export MISMATCH_RETRY_WAIT_SECS=0
  # shellcheck disable=SC2016  # $1 expanded later by the supervisor's own `bash -c`.
  export GH_VERIFY_CMD="$d/bin/gh.sh \"\$1\""
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  calls=$(cat "$gh_ctr" 2>/dev/null || echo -1)
  if [[ "$rc" -eq 0 && "$fcount" -eq 1 && "$acount" -eq 0 && "$calls" -ge 2 ]] &&
     grep -q '"verified":"merged"' "$jf"; then
    pass "mismatch grace: OPEN-then-MERGED across a retry → merged, no spurious mismatch (gh read ${calls}x)"
  else
    fail "mismatch-grace: rc=$rc finalized=$fcount abnormal=$acount gh_calls=$calls (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 35 (#2670 / roborev 1813 finding 4): a foreign-host PR URL (right path
# shape, wrong host/repo) is a forged reference → mismatch:UNRESOLVED (abnormal,
# high page), never merged. BREAKER_N=1 stops on the single forged finalize.
test_foreign_url_is_unresolved() {
  local d jf rc acount page
  d="$(new_case_dir)"
  common_env "$d"
  write_finalize_foreign_url_stub "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  page=$(grep -c '^error|worker-supervisor: finalized MISMATCH' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && "$acount" -eq 1 && "$page" -ge 1 ]] &&
     grep -q '"verified":"mismatch:UNRESOLVED"' "$jf" &&
     grep -q '"reason":"breaker"' "$jf"; then
    pass "foreign URL: non-pmcfadin/cqlite PR URL → mismatch:UNRESOLVED (escalation), never merged"
  else
    fail "foreign-url: rc=$rc abnormal=$acount page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 36 (#2670 / roborev 1813 finding 5, 1839): leftover-worker holds are counted
# CUMULATIVELY across the invocation — a transient load blip interleaved between
# leftover holds must NOT reset the bound. Alternating load(high)/leftover holds
# still trip the leftover-worker bound and stop the loop. (With the pre-fix reset, the
# leftover tally would zero on each load pass and never trip.)
test_alternating_holds_still_bounded() {
  local d counter jf rc page
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  # load probe toggles high/low each poll via a counter file; worker probe always
  # reports a leftover. preflight checks load BEFORE procs, so odd polls hold on
  # `load`, even polls hold on `leftover-worker` — never clearing to a spawn.
  export LOAD_CONTROL_FILE="$d/loadctr"
  echo 0 >"$LOAD_CONTROL_FILE"
  # shellcheck disable=SC2016  # deferred: expanded later by the supervisor's own `bash -c`.
  export LOAD_PROBE_CMD='n=$(cat "$LOAD_CONTROL_FILE"); n=$((n+1)); echo "$n" >"$LOAD_CONTROL_FILE"; if [ $((n % 2)) -eq 1 ]; then echo 99; else echo 0; fi'
  export LOAD_MAX=1
  export PROC_PROBE_WORKER_CMD="echo 1"
  export PROC_LIST_WORKER_CMD="echo '999 claude --agent worker orphan'"
  export LEFTOVER_HOLD_MAX=2
  export HOLD_POLL_SECS=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  page=$(grep -c '^error|worker-supervisor: leftover worker CLI will not clear' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && ! -f "$counter" && "$page" -ge 1 ]] &&
     grep -q '"reason":"leftover-worker"' "$jf"; then
    pass "alternating holds: leftover-worker tally is cumulative across a load blip → still bounded (stops)"
  else
    fail "alternating-holds: rc=$rc spawned=$([[ -f "$counter" ]] && echo yes || echo no) page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 37 (#2670 / roborev 1819 HIGH, finding 1): a hold entered with ONLY MAX_HOURS
# set (MAX_HOURS_SECS derived, not passed) must NOT spuriously abort budget-wallclock
# from inside the hold loop — proves the derived budget is defined on the hold path.
# Load pinned high, then cleared; the run holds, then finalizes normally.
# ---------------------------------------------------------------------------
test_maxhours_only_hold_no_abort() {
  local d counter jf rc sup_pid waited hold_notifies
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export MAX_HOURS=8
  unset MAX_HOURS_SECS 2>/dev/null || true   # force derivation on the hold path
  export LOAD_CONTROL_FILE="$d/load"; echo 99 >"$LOAD_CONTROL_FILE"
  # shellcheck disable=SC2016  # deferred: expanded later by the supervisor's own `bash -c`.
  export LOAD_PROBE_CMD='cat "$LOAD_CONTROL_FILE"'
  export LOAD_MAX=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 &
  sup_pid=$!
  waited=0
  while [[ "$waited" -lt 300 ]]; do
    hold_notifies=$(grep -c '^error|worker-supervisor HOLD|HOLD: load' "$NOTIFY_LOG" 2>/dev/null || true)
    [[ "${hold_notifies:-0}" -ge 1 ]] && break
    sleep 0.1
    waited=$((waited + 1))
  done
  echo 0 >"$LOAD_CONTROL_FILE"
  waited=0
  while [[ ! -f "$counter" && "$waited" -lt 300 ]]; do sleep 0.1; waited=$((waited + 1)); done
  wait "$sup_pid"
  rc=$?
  if [[ "$rc" -eq 0 && -f "$counter" ]] &&
     grep -q '"outcome":"finalized"' "$jf" &&
     ! grep -q '"reason":"budget-wallclock"' "$jf"; then
    pass "maxhours-only hold: derived budget on hold path, no spurious budget-wallclock abort"
  else
    fail "maxhours-only: rc=$rc counter=$([[ -f "$counter" ]] && echo yes || echo no) (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 38 (#2670 / roborev 1819 HIGH, finding 2): a TRANSPORT error whose stderr
# merely contains "not found" (`dial tcp ... host not found`) must classify
# `unverified`, NOT mismatch:UNRESOLVED — the tightened classifier keys only on
# gh's actual resolve-failure signature, so a DNS/proxy 404 is never read as forgery.
# ---------------------------------------------------------------------------
test_transport_notfound_is_unverified() {
  local d counter jf rc ucount acount
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  export UNVERIFIED_MAX=1   # a single unverified stops the loop → deterministic end
  export GH_VERIFY_CMD='echo "dial tcp: lookup github.com: no such host: host not found" >&2; exit 1'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  ucount=$(jline_count "$jf" '"outcome":"finalized-unverified"')
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  if [[ "$ucount" -eq 1 && "$acount" -eq 0 ]] &&
     grep -q '"verified":"unverified"' "$jf" &&
     ! grep -q '"verified":"mismatch:UNRESOLVED"' "$jf" &&
     grep -q '"reason":"verify-unavailable"' "$jf"; then
    pass "transport not-found: DNS/host-not-found stderr → unverified, never forgery"
  else
    fail "transport-notfound: rc=$rc unverified=$ucount abnormal=$acount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 39 (#2670 / roborev 1819 MED, finding 3): with jq ABSENT but python3
# present, verify_finalized_pr falls through to the python3 parser and correctly
# classifies an OPEN+auto-merge-armed response as pending-automerge. Unit-tests the
# function under a PATH with jq removed (python3 kept).
# ---------------------------------------------------------------------------
test_python_only_parser_automerge() {
  local d bindir t src out
  d="$(new_case_dir)"
  bindir="$d/pybin"
  mkdir -p "$bindir"
  # symlink the tools the function needs PLUS python3; jq deliberately absent.
  for t in bash mktemp cat rm grep dirname date sed python3; do
    src="$(command -v "$t" 2>/dev/null)" && ln -s "$src" "$bindir/$t"
  done
  # shellcheck disable=SC2016  # $1 expands inside the sub-bash (source target), not here.
  out="$(PATH="$bindir" \
        GH_VERIFY_CMD='printf %s "{\"state\":\"OPEN\",\"autoMergeRequest\":{\"enabledAt\":\"x\"}}"' \
        MISMATCH_RETRIES=1 MISMATCH_RETRY_WAIT_SECS=0 STOP_FILE=/nonexistent \
        "$bindir/bash" -c 'source "$1"; verify_finalized_pr 42' _ "$SUPERVISOR" 2>/dev/null)"
  if [[ "$out" == "pending-automerge" ]]; then
    pass "python-only parser: jq absent, python3 parses OPEN+auto-merge → pending-automerge"
  else
    fail "python-only: got '$out' (expected pending-automerge)"
  fi
}

# ---------------------------------------------------------------------------
# Test 40 (#2670 / roborev 1819 MED, finding 4): the mismatch-grace retry loop
# honors the stop-file mid-grace — a shutdown request must not wait out the full
# grace. gh always returns OPEN (never merges), MISMATCH_RETRIES large with a 1s
# wait; the stop-file is created while grace is sleeping, and the supervisor exits
# cleanly (stop-file) well under the would-be full grace time.
# ---------------------------------------------------------------------------
test_stop_file_honored_mid_grace() {
  local d counter jf rc sup_pid t0 elapsed waited
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  export GH_VERIFY_CMD='printf %s "{\"state\":\"OPEN\",\"mergedAt\":null,\"autoMergeRequest\":null}"'
  export MISMATCH_RETRIES=100
  export MISMATCH_RETRY_WAIT_SECS=1   # would be ~100s of grace without the stop check
  jf="$JOURNAL_FILE"

  t0=$(date +%s)
  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 &
  sup_pid=$!
  # wait until the worker has run (counter written → we're into verify/grace)
  waited=0
  while [[ ! -f "$counter" && "$waited" -lt 100 ]]; do sleep 0.1; waited=$((waited + 1)); done
  sleep 1   # let grace enter its sleep
  touch "$STOP_FILE"
  wait "$sup_pid"
  rc=$?
  elapsed=$(( $(date +%s) - t0 ))
  if [[ "$rc" -eq 0 && "$elapsed" -lt 30 ]] && grep -q '"reason":"stop-file"' "$jf"; then
    pass "stop-file mid-grace: grace loop honors the stop-file, exits in ${elapsed}s (not full grace)"
  else
    fail "stop-mid-grace: rc=$rc elapsed=${elapsed}s (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 41 (#2670 / roborev 1819 MED, 1839): the SAME PR observed OPEN-with-auto-merge-
# armed across PENDING_AUTOMERGE_MAX consecutive observations is auto-merge-stuck — the
# supervisor pages high and STOPS (automerge-stuck, exit 1) rather than looping forever.
# The stub finalizes the SAME fixed PR each iteration; gh always returns OPEN+armed for
# it; PENDING_AUTOMERGE_MAX=2.
# ---------------------------------------------------------------------------
test_persistent_pending_automerge_stops() {
  local d counter jf rc page
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_fixed_pr_finalize_stub "$d/bin/worker.sh" "$counter" 7
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  export PENDING_AUTOMERGE_MAX=2
  export PENDING_AUTOMERGE_MIN_SECS=0   # count alone trips; the wall-clock floor is exercised by test 48
  export GH_VERIFY_CMD='printf %s "{\"state\":\"OPEN\",\"mergedAt\":null,\"autoMergeRequest\":{\"enabledAt\":\"x\"}}"'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  page=$(grep -c '^error|worker-supervisor: auto-merge stuck' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && "$page" -ge 1 ]] &&
     grep -q '"reason":"automerge-stuck"' "$jf" &&
     grep -q '/pull/7' "$NOTIFY_LOG"; then
    pass "persistent pending-automerge: SAME PR unmerged x2 stops the loop (automerge-stuck, exit 1, high page)"
  else
    fail "persistent-pending: rc=$rc page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 45 (#2670 / roborev 1839 HIGH): the HEALTHY case — a fast fleet finalizes N
# DISTINCT PRs that are each briefly OPEN+armed then land. Under the per-PR model this
# must NEVER trip automerge-stuck: each distinct PR is retroactively credited toward
# MAX_ISSUES once it reaches MERGED, and the run ends cleanly at budget-issues. (Under
# the old across-PR streak this false-tripped after PENDING_AUTOMERGE_MAX distinct PRs.)
# gh stub is per-PR: OPEN+armed on the FIRST view of a PR (its finalize), MERGED after.
# ---------------------------------------------------------------------------
test_healthy_multi_pr_no_false_stop() {
  local d counter jf rc scount ccount
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"   # distinct incrementing PRs
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=3
  export PENDING_AUTOMERGE_MAX=2   # would false-trip on 2 distinct PRs under the old model
  mkdir -p "$d/ghviews"
  # Per-PR view counter: OPEN+armed on first view, MERGED thereafter.
  # shellcheck disable=SC2016  # $1 expands inside the supervisor's own `bash -c`.
  export GH_VERIFY_CMD='n="${1##*/}"; f="'"$d"'/ghviews/$n"; c=0; [ -f "$f" ] && c=$(cat "$f"); c=$((c+1)); echo "$c">"$f"; if [ "$c" -le 1 ]; then printf %s "{\"state\":\"OPEN\",\"mergedAt\":null,\"autoMergeRequest\":{\"enabledAt\":\"x\"}}"; else printf %s "{\"state\":\"MERGED\",\"mergedAt\":\"x\",\"autoMergeRequest\":null}"; fi'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  ccount=$(jline_count "$jf" '"outcome":"pending-credited"')
  scount=$(grep -c '^error|worker-supervisor: auto-merge stuck' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$ccount" -ge 3 && "$scount" -eq 0 ]] &&
     grep -q '"reason":"budget-issues"' "$jf" &&
     ! grep -q '"reason":"automerge-stuck"' "$jf"; then
    pass "healthy multi-PR: N distinct armed PRs are credited on MERGED, never false-trip automerge-stuck"
  else
    fail "healthy-multi-pr: rc=$rc credited=$ccount stuck-page=$scount (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 46 (#2670 / roborev 1839 HIGH): the self-clearing build/gate family is bounded
# by the LOOSE BUILD_HOLD_MAX, NOT the tight LEFTOVER_HOLD_MAX — a legitimate concurrent
# gate must be waited out, not killed at 15 min. A `leftover-build` hold that never
# clears must survive past LEFTOVER_HOLD_MAX and only stop at BUILD_HOLD_MAX (as
# `leftover-build`, exit 1). LEFTOVER_HOLD_MAX=1 (would stop immediately if it governed
# builds); BUILD_HOLD_MAX=3.
# ---------------------------------------------------------------------------
test_build_hold_uses_loose_bound() {
  local d counter jf rc page holds
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export PROC_PROBE_BUILD_CMD="echo 1"   # a concurrent build/gate that never clears
  export PROC_LIST_BUILD_CMD="echo '4242 cargo test --workspace'"
  export LEFTOVER_HOLD_MAX=1             # tight worker bound — must NOT govern builds
  export BUILD_HOLD_MAX=3               # loose build bound governs
  export HOLD_POLL_SECS=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  page=$(grep -c '^error|worker-supervisor: build/gate processes will not clear' "$NOTIFY_LOG" 2>/dev/null || true)
  # It must have held on leftover-build MORE than LEFTOVER_HOLD_MAX(=1) times before
  # stopping — proving the tight worker bound did NOT govern the build family.
  holds=$(grep -c 'HOLD: leftover-build' "$d/stdout.log" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && ! -f "$counter" && "$page" -ge 1 && "$holds" -ge 2 ]] &&
     grep -q '"reason":"leftover-build"' "$jf" &&
     grep -q '4242' "$NOTIFY_LOG"; then
    pass "build hold: self-clearing family uses the LOOSE BUILD_HOLD_MAX (survives LEFTOVER_HOLD_MAX, stops at BUILD_HOLD_MAX)"
  else
    fail "build-hold-loose: rc=$rc spawned=$([[ -f "$counter" ]] && echo yes || echo no) page=$page holds=$holds (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 47 (#2670 / roborev 1839 HIGH): a concurrent build/gate that CLEARS after a few
# polls (a legitimate gate finishing) must be WAITED OUT — the supervisor then spawns
# the worker, which finalizes normally. Proves the loose build bound doesn't kill a run
# that merely had a gate running. Build probe reports busy for 2 polls then clears.
# ---------------------------------------------------------------------------
test_build_hold_clears_then_proceeds() {
  local d counter jf rc
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export BUILD_HOLD_MAX=12   # loose; the build clears well before this
  export HOLD_POLL_SECS=1
  echo 0 >"$d/buildctr"
  # shellcheck disable=SC2016  # expanded later by the supervisor's own `bash -c`.
  export PROC_PROBE_BUILD_CMD='n=$(cat "'"$d"'/buildctr"); n=$((n+1)); echo "$n">"'"$d"'/buildctr"; if [ "$n" -le 2 ]; then echo 1; else echo 0; fi'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  if [[ "$rc" -eq 0 && -f "$counter" ]] &&
     grep -q '"outcome":"finalized"' "$jf" &&
     grep -q '"reason":"budget-issues"' "$jf" &&
     ! grep -q '"reason":"leftover-build"' "$jf"; then
    pass "build hold clears: a concurrent gate that finishes is waited out, then the worker runs"
  else
    fail "build-hold-clears: rc=$rc spawned=$([[ -f "$counter" ]] && echo yes || echo no) (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 48 (#2670 / roborev 1840): the wall-clock floor — a burst of fast no-progress
# iterations must NOT trip automerge-stuck on a PR whose CI simply hasn't finished. The
# same PR is observed OPEN+armed well past PENDING_AUTOMERGE_MAX observations, but with
# PENDING_AUTOMERGE_MIN_SECS set high the run instead ends at MAX_ISSUES/wall-clock, never
# `automerge-stuck`. (Here MAX_ITER_SECS-independent: worker no-work after 1 finalize so
# iterations are instant; MAX_HOURS is the terminating budget.)
# ---------------------------------------------------------------------------
test_pending_time_floor_blocks_fast_stuck() {
  local d counter jf rc stuck
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_fixed_pr_finalize_stub "$d/bin/worker.sh" "$counter" 9
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  export PENDING_AUTOMERGE_MAX=2
  export PENDING_AUTOMERGE_MIN_SECS=100000   # far above the test's wall-clock — never met
  export MAX_HOURS_SECS=3                     # terminate cleanly on wall-clock instead
  export GH_VERIFY_CMD='printf %s "{\"state\":\"OPEN\",\"mergedAt\":null,\"autoMergeRequest\":{\"enabledAt\":\"x\"}}"'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  stuck=$(grep -c '"reason":"automerge-stuck"' "$jf" 2>/dev/null || true)
  if [[ "$rc" -eq 0 && "$stuck" -eq 0 ]] &&
     grep -q '"reason":"budget-wallclock"' "$jf"; then
    pass "pending time-floor: fast repeated observations do NOT trip automerge-stuck before PENDING_AUTOMERGE_MIN_SECS"
  else
    fail "pending-time-floor: rc=$rc stuck=$stuck (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 49 (#2670 / roborev 1840): a tracked armed PR that ends CLOSED-unmerged (auto-
# merge dropped / PR closed) must NOT be swallowed silently — it is the failure this
# feature catches. It re-verifies as a non-merged mismatch on the next iteration and
# fires a HIGH "armed PR did not land" page + a `pending-dropped` journal line. gh:
# PR 1 = OPEN+armed on first view then CLOSED; any later PR = MERGED (so iter2's finalize
# credits toward MAX_ISSUES=1 and the run exits budget-issues deterministically — NOT
# wallclock, so the credit re-verify always runs before the stop).
# ---------------------------------------------------------------------------
test_pending_pr_closed_pages_high() {
  local d counter jf rc page
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  mkdir -p "$d/ghviews"
  # shellcheck disable=SC2016  # $1 expands inside the supervisor's own `bash -c`.
  export GH_VERIFY_CMD='n="${1##*/}"; f="'"$d"'/ghviews/$n"; c=0; [ -f "$f" ] && c=$(cat "$f"); c=$((c+1)); echo "$c">"$f"; if [ "$n" != "1" ]; then printf %s "{\"state\":\"MERGED\",\"mergedAt\":\"x\",\"autoMergeRequest\":null}"; elif [ "$c" -le 1 ]; then printf %s "{\"state\":\"OPEN\",\"mergedAt\":null,\"autoMergeRequest\":{\"enabledAt\":\"x\"}}"; else printf %s "{\"state\":\"CLOSED\",\"mergedAt\":null,\"autoMergeRequest\":null}"; fi'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  page=$(grep -c 'armed PR did not land' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$page" -ge 1 ]] &&
     grep -q '"outcome":"pending-dropped".*"verified":"mismatch:CLOSED"' "$jf"; then
    pass "pending closed: an armed PR that ends CLOSED-unmerged pages HIGH (not silently swallowed)"
  else
    fail "pending-closed: rc=$rc page=$page (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 50 (#2670 / roborev 1840): the verification-outage streak is reset ONLY by a
# gh-SUCCESS outcome, NOT by an intervening abnormal/no-work iteration — otherwise a
# persistent gh outage interleaved with unrelated iterations would never trip. Sequence:
# unverified finalize → abnormal → unverified finalize must reach UNVERIFIED_MAX=2 and
# STOP (verify-unavailable). Worker alternates: finalize (odd), crash (even).
# ---------------------------------------------------------------------------
test_unverified_streak_survives_intervening_abnormal() {
  local d counter jf rc
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  # Odd calls write a finalized marker; even calls exit 1 with NO marker (abnormal).
  cat >"$d/bin/worker.sh" <<EOF
#!/usr/bin/env bash
n=0
[[ -f "$counter" ]] && n=\$(cat "$counter")
n=\$((n + 1))
echo "\$n" >"$counter"
if [[ \$((n % 2)) -eq 1 ]]; then
  cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":\$n,"pr":"https://github.com/pmcfadin/cqlite/pull/\$n","duration_s":1}
JSON
else
  exit 1
fi
EOF
  chmod +x "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100          # do not let the abnormal trip the crash breaker first
  export UNVERIFIED_MAX=2
  export GH_VERIFY_CMD='printf %s "GH DOWN"'   # unparseable ⇒ unverified (transport gap)
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  if [[ "$rc" -ne 0 ]] &&
     grep -q '"reason":"verify-unavailable"' "$jf" &&
     ! grep -q '"reason":"breaker"' "$jf"; then
    pass "unverified streak: an intervening abnormal does NOT reset it — persistent outage still trips verify-unavailable"
  else
    fail "unverified-streak: rc=$rc (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 53 (#2670 / roborev 1843): the deferred automerge-stuck stop. Two tracked PRs in
# the same credit pass — one that MERGES (credited) and one that is STUCK — must leave a
# clean exit report: the stuck PR gets its OWN `finalized-pending-automerge` + HIGH page
# and is NOT re-listed as a generic `pending-at-exit`; the merged PR (resolved earlier in
# the same pass) is likewise never announced as still-pending. gh stub is per-PR: PR 21
# arms once then MERGES; PR 22 stays OPEN+armed forever.
# ---------------------------------------------------------------------------
test_deferred_stuck_stop_clean_exit() {
  local d jf rc pae_stuck pae_merged fpa_stuck stuckpage
  d="$(new_case_dir)"
  common_env "$d"
  # Worker finalizes PR 21 then PR 22 (two distinct armed PRs), then no-work.
  cat >"$d/bin/worker.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
n=0; [[ -f "$d/counter" ]] && n=\$(cat "$d/counter"); n=\$((n+1)); echo "\$n">"$d/counter"
if [[ \$n -eq 1 ]]; then pr=21; elif [[ \$n -eq 2 ]]; then pr=22; else
  printf '{"outcome":"no-work"}' >"\$MARKER_FILE"; exit 0
fi
cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":\$pr,"pr":"https://github.com/pmcfadin/cqlite/pull/\$pr","duration_s":1}
JSON
EOF
  chmod +x "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  export PENDING_AUTOMERGE_MAX=2
  export PENDING_AUTOMERGE_MIN_SECS=0
  export BACKOFF_NOWORK_SECS=1
  export MAX_HOURS_SECS=20
  mkdir -p "$d/ghviews"
  # PR 21: OPEN+armed on first view, MERGED after. PR 22: always OPEN+armed (stuck).
  # shellcheck disable=SC2016  # $1 expands inside the supervisor's own `bash -c`.
  export GH_VERIFY_CMD='p="${1##*/}"; f="'"$d"'/ghviews/$p"; c=0; [ -f "$f" ] && c=$(cat "$f"); c=$((c+1)); echo "$c">"$f"; if [ "$p" = "21" ] && [ "$c" -ge 2 ]; then printf %s "{\"state\":\"MERGED\",\"mergedAt\":\"x\",\"autoMergeRequest\":null}"; else printf %s "{\"state\":\"OPEN\",\"mergedAt\":null,\"autoMergeRequest\":{\"enabledAt\":\"x\"}}"; fi'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  pae_stuck=$(grep -c '"outcome":"pending-at-exit".*/pull/22' "$jf" 2>/dev/null || true)
  pae_merged=$(grep -c '"outcome":"pending-at-exit".*/pull/21' "$jf" 2>/dev/null || true)
  fpa_stuck=$(grep -c '"outcome":"finalized-pending-automerge".*/pull/22' "$jf" 2>/dev/null || true)
  stuckpage=$(grep -c '^error|worker-supervisor: auto-merge stuck' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 0 && "$pae_stuck" -eq 0 && "$pae_merged" -eq 0 && "$fpa_stuck" -ge 1 && "$stuckpage" -ge 1 ]] &&
     grep -q '"reason":"automerge-stuck"' "$jf"; then
    pass "deferred stuck stop: stuck PR paged once (not re-listed at exit), merged PR not announced pending (clean exit report)"
  else
    fail "deferred-stuck: rc=$rc pae22=$pae_stuck pae21=$pae_merged fpa22=$fpa_stuck stuckpage=$stuckpage (see $jf, $NOTIFY_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 51 (#2670 / roborev 1841): the wall-clock floor is genuinely CROSSED — with
# PENDING_AUTOMERGE_MAX=2 and PENDING_AUTOMERGE_MIN_SECS=2, the same PR observed pending
# holds through the first observations (count reached quickly) and only trips
# automerge-stuck once ~2s of wall-clock have elapsed. Proves the AND actually binds on
# the time term (not just the two degenerate MIN_SECS=0 / huge extremes). Asserts the
# stop IS automerge-stuck and the run lasted >= 2s.
# ---------------------------------------------------------------------------
test_pending_time_floor_crossed_trips() {
  local d counter jf rc elapsed
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  # worker finalizes the same PR each time with a small sleep so the loop doesn't spin
  # thousands of times per second while the 2s floor elapses.
  cat >"$d/bin/worker.sh" <<EOF
#!/usr/bin/env bash
set -euo pipefail
n=0; [[ -f "$counter" ]] && n=\$(cat "$counter"); n=\$((n+1)); echo "\$n">"$counter"
cat >"\$MARKER_FILE" <<JSON
{"outcome":"finalized","issue":11,"pr":"https://github.com/pmcfadin/cqlite/pull/11","duration_s":1}
JSON
sleep 0.3
EOF
  chmod +x "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  export PENDING_AUTOMERGE_MAX=2
  export PENDING_AUTOMERGE_MIN_SECS=2
  export MAX_HOURS_SECS=30   # backstop so a broken floor can't hang the suite
  export GH_VERIFY_CMD='printf %s "{\"state\":\"OPEN\",\"mergedAt\":null,\"autoMergeRequest\":{\"enabledAt\":\"x\"}}"'
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  elapsed=$(grep -o '"reason":"automerge-stuck","issues_done":[0-9]*,"elapsed_s":[0-9]*' "$jf" 2>/dev/null | grep -o 'elapsed_s":[0-9]*' | grep -o '[0-9]*' | tail -1)
  local iters pcount
  iters=$(cat "$counter" 2>/dev/null || echo 0)
  pcount=$(jline_count "$jf" '"outcome":"finalized-pending-automerge"')
  # Trip only after the TIME term (elapsed >= 2s): a `||`-instead-of-`&&` bug (OR a
  # time-term-ignored bug) would trip on the 1st observation at ~0.35s → elapsed<2, caught
  # here. The COUNT term is pinned separately by test 47 (MIN_SECS=0, so only the count can
  # gate the trip). iters/pcount>=2 just confirm the run genuinely accumulated observations.
  if [[ "$rc" -ne 0 ]] &&
     grep -q '"reason":"automerge-stuck"' "$jf" &&
     [[ -n "$elapsed" && "$elapsed" -ge 2 && "$iters" -ge 2 && "$pcount" -ge 2 ]]; then
    pass "pending time-floor crossed: held through $pcount observations (${iters} iters), trips automerge-stuck only after MIN_SECS (elapsed ${elapsed}s)"
  else
    fail "pending-time-floor-crossed: rc=$rc elapsed=${elapsed:-?} iters=$iters pcount=$pcount (see $jf)"
  fi
}

# ---------------------------------------------------------------------------
# Test 52 (#2670 / roborev 1841/1842): numeric-knob validation is fail-CLOSED for a
# malformed INTEGER knob (a `MAX_HOURS=abc` typo must page + exit 2, never silently
# derive a 0 budget), but fail-OPEN-safe values are honored — a fractional DISK_FLOOR_GB
# (float-compared) is ACCEPTED and the supervisor runs normally.
# ---------------------------------------------------------------------------
test_numeric_knob_validation() {
  local d counter rc page
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  # (a) malformed integer knob → FATAL exit 2, no worker spawn, bad-config page.
  export MAX_HOURS="abc"
  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  page=$(grep -c 'bad config' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -ne 2 || -f "$counter" || "$page" -lt 1 ]]; then
    fail "knob-validation(bad-int): rc=$rc (want 2) spawned=$([[ -f "$counter" ]] && echo yes) page=$page"
    return
  fi
  # (b) fractional DISK_FLOOR_GB is a valid float — the run proceeds and finalizes.
  local d2 counter2
  d2="$(new_case_dir)"
  counter2="$d2/counter"
  common_env "$d2"
  write_finalize_stub "$d2/bin/worker.sh" "$counter2"
  export WORKER_CMD="$d2/bin/worker.sh"
  export MAX_ISSUES=1
  export DISK_FLOOR_GB="37.5"
  bash "$SUPERVISOR" >"$d2/stdout.log" 2>&1
  rc=$?
  if [[ "$rc" -eq 0 && -f "$counter2" ]] &&
     grep -q '"reason":"budget-issues"' "$JOURNAL_FILE"; then
    pass "knob validation: malformed MAX_HOURS fails closed (exit 2, paged); fractional DISK_FLOOR_GB accepted"
  else
    fail "knob-validation(float): rc=$rc spawned=$([[ -f "$counter2" ]] && echo yes || echo no) (see $d2)"
  fi
  # (c) ZERO is not a lax bound for CLAIM_MIGRATION_RETRIES, it is a SILENT SKIP (roborev round 35).
  # A 0 makes the retry loop body never execute, so the legacy claim is never read and the lane runs
  # foreign to its own lock with no error anywhere. It therefore belongs to a strictly-POSITIVE group,
  # unlike the count knobs where 0 is a meaningful value. Found because a harness left it unset — the
  # same failure a plist typo would produce in production, where nothing would be watching.
  local d3 counter3 rc3
  d3="$(new_case_dir)"; counter3="$d3/counter"
  common_env "$d3"
  write_finalize_stub "$d3/bin/worker.sh" "$counter3"
  export WORKER_CMD="$d3/bin/worker.sh"
  export CLAIM_MIGRATION_RETRIES=0
  bash "$SUPERVISOR" >"$d3/stdout.log" 2>&1
  rc3=$?
  if [[ "$rc3" -eq 2 && ! -f "$counter3" ]] &&
     grep -q "CLAIM_MIGRATION_RETRIES" "$d3/stdout.log"; then
    pass "knob validation: CLAIM_MIGRATION_RETRIES=0 fails closed and names the knob (0 would silently skip the migration)"
  else
    fail "knob-validation(zero-retries): rc=$rc3 (want 2) spawned=$([[ -f "$counter3" ]] && echo yes || echo no)"
  fi
  # NON-VACUITY: a positive value is accepted, so (c) is about ZERO and not about the knob being
  # rejected outright.
  local d4 counter4 rc4
  d4="$(new_case_dir)"; counter4="$d4/counter"
  common_env "$d4"
  write_finalize_stub "$d4/bin/worker.sh" "$counter4"
  export WORKER_CMD="$d4/bin/worker.sh"
  export MAX_ISSUES=1
  export CLAIM_MIGRATION_RETRIES=2
  bash "$SUPERVISOR" >"$d4/stdout.log" 2>&1
  rc4=$?
  unset CLAIM_MIGRATION_RETRIES
  if [[ "$rc4" -eq 0 && -f "$counter4" ]]; then
    pass "NON-VACUITY: CLAIM_MIGRATION_RETRIES=2 is accepted and the run proceeds"
  else
    fail "knob-validation(positive-retries): rc=$rc4 spawned=$([[ -f "$counter4" ]] && echo yes || echo no)"
  fi
}

# ---------------------------------------------------------------------------
# Test 42 (#2670 / roborev 1821, 1840): each family's count probe AND its list probe
# DERIVE from that family's shared match pattern (PROC_MATCH_BUILD / PROC_MATCH_WORKER)
# — the "what counts" set and the "what we name" set cannot drift, per family. Source
# with the family probes unset and assert each command string embeds its own pattern.
test_probe_list_derives_from_count_set() {
  local out build worker wprobe bprobe wlist blist
  # shellcheck disable=SC2016  # $SUP/$PROC_* expand inside the sub-bash, not here.
  out="$(env -u PROC_PROBE_WORKER_CMD -u PROC_PROBE_BUILD_CMD -u PROC_LIST_WORKER_CMD -u PROC_LIST_BUILD_CMD SUP="$SUPERVISOR" bash -c '
    # shellcheck disable=SC1090
    source "$SUP"
    printf "%s\n%s\n%s\n%s\n%s\n%s\n" "$PROC_MATCH_BUILD" "$PROC_MATCH_WORKER" "$PROC_PROBE_WORKER_CMD" "$PROC_PROBE_BUILD_CMD" "$PROC_LIST_WORKER_CMD" "$PROC_LIST_BUILD_CMD"' 2>/dev/null)"
  build="$(printf '%s' "$out" | sed -n 1p)"
  worker="$(printf '%s' "$out" | sed -n 2p)"
  wprobe="$(printf '%s' "$out" | sed -n 3p)"
  bprobe="$(printf '%s' "$out" | sed -n 4p)"
  wlist="$(printf '%s' "$out" | sed -n 5p)"
  blist="$(printf '%s' "$out" | sed -n 6p)"
  if [[ -n "$build" && -n "$worker" &&
        "$wprobe" == *"$worker"* && "$wlist" == *"$worker"* &&
        "$bprobe" == *"$build"* && "$blist" == *"$build"* ]]; then
    pass "probe derivation: each family's count + list probe derives from its own match pattern"
  else
    fail "probe-derivation: worker-ok=$([[ "$wprobe" == *"$worker"* && "$wlist" == *"$worker"* ]] && echo y) build-ok=$([[ "$bprobe" == *"$build"* && "$blist" == *"$build"* ]] && echo y)"
  fi
}

# ---------------------------------------------------------------------------
# Test 43 (#2670 / roborev 1821, finding b): MISMATCH_GRACE_CAP_SECS<=0 DISABLES
# the wall-clock cap — grace stays bounded solely by the retry count and must NOT
# be blocked. gh reports OPEN then MERGED; with cap=-1, retries=3, wait=0 the grace
# still retries and resolves `merged` (never a spurious mismatch). Unit-tests
# verify_finalized_pr directly.
test_grace_cap_disabled_semantics() {
  local d ctr out
  d="$(new_case_dir)"
  ctr="$d/gh-calls"
  cat >"$d/gh.sh" <<EOF
#!/usr/bin/env bash
n=0; [[ -f "$ctr" ]] && n=\$(cat "$ctr"); n=\$((n + 1)); echo "\$n" >"$ctr"
if [[ \$n -eq 1 ]]; then printf %s '{"state":"OPEN","autoMergeRequest":null}'
else printf %s '{"state":"MERGED","autoMergeRequest":null}'; fi
EOF
  chmod +x "$d/gh.sh"
  # shellcheck disable=SC2016  # $1 expands inside the sub-bash, not here.
  out="$(GH_VERIFY_CMD="$d/gh.sh \"\$1\"" \
        MISMATCH_RETRIES=3 MISMATCH_RETRY_WAIT_SECS=0 MISMATCH_GRACE_CAP_SECS=-1 STOP_FILE=/nonexistent \
        bash -c 'source "$1"; verify_finalized_pr 42' _ "$SUPERVISOR" 2>/dev/null)"
  if [[ "$out" == "merged" && "$(cat "$ctr" 2>/dev/null)" -ge 2 ]]; then
    pass "grace cap<=0: disabled ceiling, grace stays count-bounded (OPEN→MERGED resolves merged)"
  else
    fail "grace-cap-disabled: got '$out' gh_calls=$(cat "$ctr" 2>/dev/null)"
  fi
}

# ---------------------------------------------------------------------------
# Test 44 (#2670 / roborev 1837 MED): a grace loop CUT SHORT by the stop-file
# (a requested shutdown mid-grace) is NOT a confirmed mismatch — the PR state was
# never allowed to settle, so verify_finalized_pr must return the NEUTRAL `aborted`
# verdict, NEVER `mismatch:OPEN` (which the caller turns into an abnormal "worker
# forged a finalize" HIGH page + breaker+1). `aborted` is also distinct from
# `unverified` so an ordinary shutdown cannot accumulate the unverified-outage
# streak. Unit-tests verify_finalized_pr directly with an already-present stop-file.
# ---------------------------------------------------------------------------
test_mid_grace_stop_is_aborted() {
  local d out
  d="$(new_case_dir)"
  touch "$d/stop"
  # shellcheck disable=SC2016  # $1 expands inside the sub-bash, not here.
  local out1
  out="$(GH_VERIFY_CMD='printf %s "{\"state\":\"OPEN\",\"autoMergeRequest\":null}"' \
        MISMATCH_RETRIES=5 MISMATCH_RETRY_WAIT_SECS=0 STOP_FILE="$d/stop" \
        bash -c 'source "$1"; verify_finalized_pr 42' _ "$SUPERVISOR" 2>/dev/null)"
  # roborev 1838: also cover MISMATCH_RETRIES=1 — the loop never reaches the mid-loop
  # guard, so the final-read stop-file re-check is what defuses the forgery verdict.
  out1="$(GH_VERIFY_CMD='printf %s "{\"state\":\"OPEN\",\"autoMergeRequest\":null}"' \
        MISMATCH_RETRIES=1 MISMATCH_RETRY_WAIT_SECS=0 STOP_FILE="$d/stop" \
        bash -c 'source "$1"; verify_finalized_pr 42' _ "$SUPERVISOR" 2>/dev/null)"
  if [[ "$out" == "aborted" && "$out1" == "aborted" ]]; then
    pass "mid-grace stop: shutdown cuts grace short → aborted (neutral; retries=5 AND retries=1)"
  else
    fail "mid-grace-stop: got retries5='$out' retries1='$out1' (expected aborted, NOT mismatch:* / unverified)"
  fi
}

# ---------------------------------------------------------------------------
# Test 24-claim (#2655): the NEXT spawn's claim stamp carries the issue LEARNED from a
# non-finalized (blocked) marker — so the reaper's open-PR guard tracks the real
# issue. Iter 1 blocks issue 88; iter 2's stamp must name issue 88 (before the
# head-block guard stops the run on the 2nd consecutive block).
# ---------------------------------------------------------------------------
test_claim_issue_learned_from_marker() {
  local d rc second_stamp
  d="$(new_case_dir)"
  common_env "$d"
  write_blocked_same_issue_stub "$d/bin/worker.sh" 88
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  export CLAIM_LOG="$d/claim.log"
  : >"$CLAIM_LOG"
  write_claim_stub "$d/bin/claim.sh"
  export CLAIM_CMD="bash $d/bin/claim.sh"
  export HEARTBEAT_MACHINE="testbox"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  # Iter1 stamps the `p<pid>` placeholder (issue unknown — it is no longer the shared "0", #3393);
  # iter2 stamps issue 88, learned from iter1's blocked marker. Assert on the ISSUE-NAMED stamp
  # rather than on line position, which is what the property is actually about.
  second_stamp=$(grep -E '^stamp 88 [0-9]+$' "$CLAIM_LOG" 2>/dev/null | head -1)
  # ...and the placeholder it replaced must have been cleared, or the transition leaks a ref that
  # holds a dead pid and dead-lanes reports it as a dead lane forever.
  local placeholder_id placeholder_reaped
  placeholder_id=$(grep -E '^stamp p[0-9]+-[0-9a-f]+ [0-9]+$' "$CLAIM_LOG" 2>/dev/null | head -1 | awk '{print $2}')
  placeholder_reaped=no
  # WITH THE LEASE the stamp reported (roborev round 19): a reap of a lane ref must never run
  # unleased, or a retry landing after another supervisor took the lane id deletes ITS live claim.
  [[ -n "$placeholder_id" ]] && grep -qE "^reap testbox ${placeholder_id} deadbeefdeadbeefdeadbeefdeadbeefdeadbeef\$" "$CLAIM_LOG" 2>/dev/null && placeholder_reaped=yes
  if [[ "$rc" -eq 0 ]] && printf '%s' "$second_stamp" | grep -qE '^stamp 88 [0-9]+$' \
    && [[ "$placeholder_reaped" == "yes" ]]; then
    pass "claim: issue learned from a blocked marker names the next stamp (issue 88), and the p<pid> placeholder ref it replaced was cleared (no leaked ref)"
  else
    fail "claim-learn: rc=$rc second_stamp='$second_stamp' placeholder='$placeholder_id' reaped=$placeholder_reaped (see $CLAIM_LOG)"
  fi
}

# ---------------------------------------------------------------------------
# Test 25-claim (#3393, roborev round 2): a lane TRANSITION whose replacement stamp FAILS must not
# leave the lane with no claim ref. Deleting the old ref first would open exactly that gap — the
# worker still starts, but dead-lanes and the reaper cannot see it for the whole iteration. So the
# old ref must SURVIVE a failed replacement.
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Test 26-claim (#3393, roborev round 6): the pending-cleanup queue must NEVER delete the lane ref
# just stamped. If cleaning placeholder P fails during P -> issue, P stays queued; a later
# issue -> P transition REFRESHES P and then drains, which without protection deletes that fresh
# CURRENT ref and leaves the running lane unobservable — the failure this change exists to prevent,
# produced by the retry logic that was added to fix a leak.
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Test 27-claim (#3393, roborev round 18): clear_claim must NOT delete a PLACEHOLDER lane ref on an
# ABNORMAL exit. finalize_exit runs on every exit path (breaker, leftover-*, automerge-stuck,
# verify-unavailable), and a `p<pid>` id names no issue, so `reap` cannot consult the open-PR
# safeguard and deletes unconditionally — destroying the only liveness signal of a lane whose worker
# may have claimed an issue and opened a PR before the supervisor ever saw the marker (#2499 reached
# from the other side). A NUMERIC lane id is unaffected: there the guard runs inside reap.
# ---------------------------------------------------------------------------
test_clear_claim_keeps_placeholder_on_abnormal_exit() {
  local d out
  d="$(new_case_dir)"
  common_env "$d"
  export CLAIM_LOG="$d/claim.log"
  cat >"$d/bin/claim.sh" <<'STUBEOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${CLAIM_LOG:?CLAIM_LOG not set}"
[ "${1:-}" = stamp ] && printf 'deadbeefdeadbeefdeadbeefdeadbeefdeadbeef\n'
exit 0
STUBEOF
  chmod +x "$d/bin/claim.sh"
  # FOUR cases. The two NUMERIC ones are the round-23 correction: a numeric lane id used to be cleared
  # on any exit, on the reasoning that reap's open-PR guard makes it safe. It does not — PRE-PR work has
  # no open PR, so the guard passes and the ref is deleted, erasing the only signal that an unfinished
  # lane held that issue. "No open PR" is a correct answer to the wrong question.
  : >"$CLAIM_LOG"
  out="$(
    CLAIM_CMD="bash $d/bin/claim.sh" HEARTBEAT_MACHINE=testbox CLAIM_LOG="$CLAIM_LOG" \
    bash -c '
      source "$1"
      # A real supervisor always holds a lease unless the stamp reported no sha; round 32 makes an
      # empty lease refuse outright, so these legs supply one and the empty case is asserted below.
      CLAIM_STAMPED_SHA="feed0001"
      CLAIM_STAMPED_ISSUE="p777-dead1"; clear_claim 0
      CLAIM_STAMPED_ISSUE="p888-dead2"; clear_claim 1
      CLAIM_STAMPED_ISSUE="4242";       clear_claim 0
      CLAIM_STAMPED_ISSUE="5353";       clear_claim 1
      # ROUND 32: no lease => no automated delete, even when concluded.
      CLAIM_STAMPED_SHA=""
      CLAIM_STAMPED_ISSUE="6464";       clear_claim 1
    ' _ "$SUPERVISOR" 2>&1
  )"
  if printf '%s' "$out" | grep -q 'the work on lane p777-dead1 has not concluded' \
    && printf '%s' "$out" | grep -q 'the work on lane 4242 has not concluded' \
    && ! grep -qE '^reap testbox p777-dead1( |$)' "$CLAIM_LOG" \
    && ! grep -qE '^reap testbox 4242( |$)' "$CLAIM_LOG" \
    && grep -qE '^reap testbox p888-dead2( |$)' "$CLAIM_LOG" \
    && grep -qE '^reap testbox 5353( |$)' "$CLAIM_LOG" \
    && ! grep -qE '^reap testbox 6464' "$CLAIM_LOG" \
    && printf '%s' "$out" | grep -q 'DECLINED for lane 6464: no lease was recorded'; then
    pass "claim: an UNCONCLUDED lane survives regardless of its id shape (placeholder AND numeric), and a concluded one is cleared either way"
  else
    fail "clear-claim-concluded: out=[$out] log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
  # WIRING: finalize_exit must pass the WORK-CONCLUDED state, not a code-derived clean flag. The exit
  # code was the previous discriminator and it is exactly what this round falsified — a clean stop
  # mid-issue must keep the ref just as a breaker must.
  if grep -qE 'clear_claim "\$CLAIM_WORK_CONCLUDED"' "$SUPERVISOR" \
    && ! grep -qE 'clear_claim "\$clean_exit"' "$SUPERVISOR"; then
    pass "claim: finalize_exit passes CLAIM_WORK_CONCLUDED and no longer derives a clean flag from the exit code"
  else
    fail "clear-claim-wiring: finalize_exit must pass \$CLAIM_WORK_CONCLUDED, not an exit-code flag"
  fi
  # ...and the LIFECYCLE must hold, which the round-23 version of this case did not check. It asserted
  # that the shipped file contained particular `case` arms — i.e. it tested a MODEL of the code, and
  # when round 24 moved the assignment to the accept points the model went stale while the property it
  # was standing in for was never being measured at all. Replaced with behaviour.
  #
  # (a) UNCONCLUDED AT SPAWN. The flag must be reset where the ref is stamped, so every path that
  #     returns early — a crash, the stuck watchdog, an early finalize_exit — inherits the SAFE value.
  #     Round 24: it kept its initial 1, so a breaker after abnormal iterations deleted the live ref.
  local spawn_block
  spawn_block="$(sed -n '/^run_iteration()/,/^}/p' "$SUPERVISOR" | sed -n '1,/CLAIM_WORK_CONCLUDED=0/p')"
  if printf '%s' "$spawn_block" | grep -q 'stamp_claim' \
    && printf '%s' "$spawn_block" | grep -q 'CLAIM_WORK_CONCLUDED=0'; then
    pass "claim: run_iteration resets work-concluded to 0 at the stamp, so every early exit inherits the safe value"
  else
    fail "clear-claim-spawn-reset: run_iteration must set CLAIM_WORK_CONCLUDED=0 at/after stamp_claim"
  fi
  # (b) A MALFORMED `finalized` MARKER MUST NOT CONCLUDE THE WORK. Behavioural: the marker claims
  #     success with no pr, the supervisor judges it abnormal, and the lane's ref must SURVIVE.
  #     Round 24: the flag was set from the outcome STRING before that validation ran.
  local d2
  d2="$(new_case_dir)"
  common_env "$d2"
  export CLAIM_LOG="$d2/claim.log"
  : >"$CLAIM_LOG"
  write_finalize_missing_pr_stub "$d2/bin/worker.sh"
  export WORKER_CMD="$d2/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=1
  write_claim_stub "$d2/bin/claim.sh"
  export CLAIM_CMD="bash $d2/bin/claim.sh"
  export HEARTBEAT_MACHINE="testbox"
  bash "$SUPERVISOR" >"$d2/stdout.log" 2>&1 || true
  local stamped reaped_it
  stamped=$(grep -oE '^stamp [^ ]+' "$CLAIM_LOG" 2>/dev/null | head -1 | awk '{print $2}')
  reaped_it=no
  [[ -n "$stamped" ]] && grep -qE "^reap testbox ${stamped}( |$)" "$CLAIM_LOG" 2>/dev/null && reaped_it=yes
  if [[ -n "$stamped" && "$reaped_it" == no ]] \
    && grep -q 'has not concluded' "$d2/stdout.log"; then
    pass "claim: a malformed 'finalized' marker does NOT conclude the work — lane $stamped keeps its ref"
  else
    fail "clear-claim-untrusted-finalize: stamped='$stamped' reaped=$reaped_it log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
  # NON-VACUITY: the run really did stamp a lane and really did reach its exit path, so "no reap" is a
  # DECISION rather than an absence of activity.
  #
  # KEYED ON A SIGNAL THAT EXISTS IN BOTH DIRECTIONS. The first cut looked for the DECLINE message —
  # which only appears when the fix works, so under RED (fix removed) this probe failed too. A
  # non-vacuity check that can only pass when the assertion passes measures nothing; it has to be true
  # of the broken code as well. The journal's `summary` record is written by `finalize_exit` on every
  # exit path, whatever the claim decision was.
  local jf_summary=no
  grep -rqs '"outcome":"summary"' "$d2/logs" 2>/dev/null && jf_summary=yes
  if grep -qE '^stamp ' "$CLAIM_LOG" && [[ "$jf_summary" == yes ]]; then
    pass "NON-VACUITY: the run stamped a lane and journalled an exit summary, so the surviving ref is a decision"
  else
    fail "clear-claim-untrusted-finalize-nonvacuity: stamp=$(grep -cE '^stamp ' "$CLAIM_LOG") summary=$jf_summary"
  fi
}

# ---------------------------------------------------------------------------
# Test 28-claim (#3393, roborev round 19): the single-instance lock must be PER LANE. A
# machine-global default made a second lane exit during lock acquisition, so the per-lane claim refs
# this change adds were unreachable with the documented default invocation — the retracted #1930
# invariant surviving in a second mechanism.
# ---------------------------------------------------------------------------
test_supervisor_lock_is_per_lane() {
  local body a b same
  body="$T_LOCKFN/lockfn.sh"
  mkdir -p "$T_LOCKFN"
  # The functions alone, so the case does not depend on sourcing the whole supervisor. BOTH are needed:
  # `supervisor_lock_path` now BUILDS ON `supervisor_lane_id` (roborev round 34) rather than carrying a
  # second copy of its body, so extracting the lock function alone yields an undefined call and an EMPTY
  # path — which is how this case caught the change, loudly and in the right place.
  # DRIVEN BY `LANE_ID`, THE GIVEN IDENTITY (lead ruling B, 2026-08-30). This case used to drive the
  # lock by REPO_ROOT, because the lock inferred its own identity from the script's location — which is
  # exactly the coincidence the ruling rejected. The PROPERTY is unchanged (distinct lanes get distinct
  # locks; one lane is stable); only the SOURCE of the identity moved, from an inference to a value.
  {
    printf '%s\n' '#!/usr/bin/env bash'
    sed -n '/^supervisor_lane_id()/,/^}/p' "$SUPERVISOR"
    sed -n '/^supervisor_lock_path()/,/^}/p' "$SUPERVISOR"
    printf '%s\n' 'supervisor_lock_path; printf "%s\n" "$SUPERVISOR_LOCK"'
  } >"$body"
  a=$(SUPERVISOR_LOCK="" LANE_ID=lane-1111 REPO_ROOT=/data/lanes/lane-1111 TMPDIR=/tmp bash "$body")
  b=$(SUPERVISOR_LOCK="" LANE_ID=lane-2222 REPO_ROOT=/data/lanes/lane-2222 TMPDIR=/tmp bash "$body")
  same=$(SUPERVISOR_LOCK="" LANE_ID=lane-1111 REPO_ROOT=/data/lanes/lane-1111 TMPDIR=/tmp bash "$body")
  if [[ -n "$a" && -n "$b" && "$a" != "$b" && "$a" == "$same" ]]; then
    pass "claim: two lanes get DIFFERENT default locks and one lane is stable across runs ($a vs $b)"
  else
    fail "lock-per-lane: a=[$a] b=[$b] same=[$same] — two lanes must differ and one lane must be stable"
  fi
  # Two lanes whose directories share a BASENAME must still differ, or the readable half would alias
  # them onto one lock and reintroduce the machine-global failure for the common fleet layout.
  local c e
  c=$(SUPERVISOR_LOCK="" LANE_ID=boxA-lane REPO_ROOT=/data/boxA/lane TMPDIR=/tmp bash "$body")
  e=$(SUPERVISOR_LOCK="" LANE_ID=boxB-lane REPO_ROOT=/data/boxB/lane TMPDIR=/tmp bash "$body")
  if [[ "$c" != "$e" ]]; then
    pass "claim: two distinct LANE_IDs get different locks (the basename coincidence is no longer load-bearing)"
  else
    fail "lock-per-lane-basename: both resolved to [$c]"
  fi
  # An explicit SUPERVISOR_LOCK still wins — the fix must not take the override away.
  local ov
  ov=$(SUPERVISOR_LOCK=/tmp/explicit.lock LANE_ID=lane-1111 REPO_ROOT=/data/lanes/lane-1111 bash "$body")
  if [[ "$ov" == "/tmp/explicit.lock" ]]; then
    pass "claim: an explicit SUPERVISOR_LOCK is still honoured"
  else
    fail "lock-per-lane-override: got [$ov]"
  fi
  # ONE CONSTRUCTION (roborev round 34, Medium): the lock path must be built FROM `supervisor_lane_id`,
  # not from a second copy of its body — two spellings of one identity drift, and the bound added to one
  # would silently not apply to the other.
  # ONE IDENTITY, TWO CONSUMERS (lead ruling B): the lock and the claim actor must BOTH derive from
  # `LANE_ID`. Two independent derivations of "which lane am I" is two things to keep in step, and the
  # one that drifts is found in production. The earlier form of this assert required the lock to call
  # `supervisor_lane_id`; that was the same property when identity was inferred, and is the wrong
  # spelling of it now that identity is given.
  local lock_uses actor_uses
  lock_uses=$(sed -n '/^supervisor_lock_path()/,/^}/p' "$SUPERVISOR" | grep -c 'LANE_ID')
  actor_uses=$(sed -n '/^supervisor_claim_actor()/,/^}/p' "$SUPERVISOR" | grep -c 'LANE_ID')
  if [[ "$lock_uses" -ge 1 && "$actor_uses" -ge 1 ]]; then
    pass "identity: the lock AND the claim actor both derive from the given LANE_ID (one identity, two consumers)"
  else
    fail "identity-drift: lock refs LANE_ID $lock_uses time(s), actor $actor_uses — a consumer re-inferring its own lane identity will drift from the other"
  fi
  # BUILTINS ONLY (#3464 family 2, reintroduced in the first cut of this very fix). Several cases
  # SOURCE the supervisor under a stripped PATH to prove the no-jq/no-python3 paths, so an external
  # tool anywhere in this resolution breaks them. Driven by an EMPTY PATH.
  local stripped
  # `$BASH` is the ABSOLUTE path of the running shell. `PATH="" bash …` cannot find bash itself, so
  # the first cut of this case failed with "bash: No such file or directory" — and its NON-VACUITY
  # control PASSED for that same wrong reason, which is the shape this whole change keeps meeting.
  stripped=$(SUPERVISOR_LOCK="" LANE_ID=lane-1111 REPO_ROOT=/data/lanes/lane-1111 TMPDIR=/tmp PATH="" "$BASH" "$body" 2>&1)
  if [[ "$stripped" == "$a" ]]; then
    pass "claim: the lock path resolves with an EMPTY PATH — builtins only, no tr/cksum/awk"
  else
    fail "lock-per-lane-builtins: with PATH='' got [$stripped], expected [$a] — an external tool crept into the resolution"
  fi
  # NON-VACUITY: the same harness with a deliberately external-tool implementation DOES fail under
  # the stripped PATH, so the case above is a measurement rather than a tautology.
  local ext_body ext_out
  ext_body="$T_LOCKFN/ext.sh"
  {
    printf '%s\n' '#!/usr/bin/env bash'
    printf '%s\n' 'h="$(printf %s "$REPO_ROOT" | cksum | awk "{print \$1}")"'
    printf '%s\n' 'printf "%s\n" "/tmp/x-$h.lock"'
  } >"$ext_body"
  local ext_expected
  ext_expected="/tmp/x-$(printf %s /data/lanes/lane-1111 | cksum | awk '{print $1}').lock"
  # Sanity: WITH a normal PATH the control must produce that value, or the comparison below is
  # meaningless regardless of what the stripped run does.
  local ext_ok
  ext_ok=$(REPO_ROOT=/data/lanes/lane-1111 "$BASH" "$ext_body" 2>/dev/null)
  ext_out=$(REPO_ROOT=/data/lanes/lane-1111 PATH="" "$BASH" "$ext_body" 2>/dev/null)
  if [[ "$ext_ok" == "$ext_expected" && "$ext_out" != "$ext_expected" ]]; then
    pass "NON-VACUITY: an external-tool implementation of the same resolution DOES break under PATH='' (so the builtin case above measures something)"
  else
    fail "NON-VACUITY broken: control with PATH gave [$ext_ok] (expected [$ext_expected]) and with PATH='' gave [$ext_out] — the external-tool control must WORK normally and BREAK stripped, or the builtins assertion proves nothing"
  fi
}

# ---------------------------------------------------------------------------
# Test 29-claim (#3393, roborev round 19): a lane-ref reap must carry the LEASE this supervisor
# stamped, and a lease-not-held result (rc=4) means ownership TRANSFERRED — drop the entry rather
# than retry, because retrying can only delete the new owner's live claim.
# ---------------------------------------------------------------------------
test_claim_cleanup_uses_lease_and_drops_on_transfer() {
  local d out
  d="$(new_case_dir)"
  common_env "$d"
  export CLAIM_LOG="$d/claim.log"
  : >"$CLAIM_LOG"
  # A reap stub that reports rc=4 (lease not held) for lane 77, and success otherwise.
  cat >"$d/bin/claim.sh" <<'STUBEOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${CLAIM_LOG:?CLAIM_LOG not set}"
if [ "${1:-}" = reap ] && [ "${3:-}" = 77 ]; then exit 4; fi
[ "${1:-}" = stamp ] && printf 'deadbeefdeadbeefdeadbeefdeadbeefdeadbeef\n'
exit 0
STUBEOF
  chmod +x "$d/bin/claim.sh"
  out="$(
    CLAIM_CMD="bash $d/bin/claim.sh" HEARTBEAT_MACHINE=testbox CLAIM_LOG="$CLAIM_LOG" \
    bash -c '
      source "$1"
      CLAIM_PENDING_CLEANUP=" 77:cafe1234 88:beef5678 "
      claim_drain_pending_cleanup
      printf "PENDING_AFTER=[%s]\n" "$CLAIM_PENDING_CLEANUP"
    ' _ "$SUPERVISOR" 2>&1
  )"
  if grep -qE '^reap testbox 77 cafe1234$' "$CLAIM_LOG" \
    && grep -qE '^reap testbox 88 beef5678$' "$CLAIM_LOG" \
    && printf '%s' "$out" | grep -q 'pending cleanup of 77 dropped: the lease at cafe1234 is no longer held' \
    && printf '%s' "$out" | grep -q 'PENDING_AFTER=\[\]'; then
    pass "claim: the drain passes each entry's LEASE and DROPS the one whose lease transferred (never retries it)"
  else
    fail "claim-lease-drain: out=[$out] log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
  # CONTROL: an entry whose reap SUCCEEDS is also removed, so the drop above is attributable to the
  # rc=4 branch rather than to "the drain empties the queue regardless".
  : >"$CLAIM_LOG"
  out="$(
    CLAIM_CMD="bash $d/bin/claim.sh" HEARTBEAT_MACHINE=testbox CLAIM_LOG="$CLAIM_LOG" \
    bash -c '
      source "$1"
      CLAIM_PENDING_CLEANUP=" 99:aaa111 "
      claim_drain_pending_cleanup
      printf "PENDING_AFTER=[%s]\n" "$CLAIM_PENDING_CLEANUP"
    ' _ "$SUPERVISOR" 2>&1
  )"
  if grep -qE '^reap testbox 99 aaa111$' "$CLAIM_LOG" \
    && printf '%s' "$out" | grep -q 'stale lane ref 99 cleared (lease held at aaa111)' \
    && printf '%s' "$out" | grep -q 'PENDING_AFTER=\[\]'; then
    pass "claim: a successful leased reap clears the entry and names the lease it held"
  else
    fail "claim-lease-success: out=[$out] log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
  # A NON-TRANSFER FAILURE MUST STILL BE RETAINED — dropping every non-zero rc would turn the lease
  # fix into a ref leak, the mirror mistake (#3464 family 4, fail-shut).
  : >"$CLAIM_LOG"
  cat >"$d/bin/claim-fail.sh" <<'STUBEOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${CLAIM_LOG:?CLAIM_LOG not set}"
[ "${1:-}" = reap ] && exit 3
exit 0
STUBEOF
  chmod +x "$d/bin/claim-fail.sh"
  out="$(
    CLAIM_CMD="bash $d/bin/claim-fail.sh" HEARTBEAT_MACHINE=testbox CLAIM_LOG="$CLAIM_LOG" \
    bash -c '
      source "$1"
      CLAIM_PENDING_CLEANUP=" 55:bbb222 "
      claim_drain_pending_cleanup
      printf "PENDING_AFTER=[%s]\n" "$CLAIM_PENDING_CLEANUP"
    ' _ "$SUPERVISOR" 2>&1
  )"
  if printf '%s' "$out" | grep -q 'PENDING_AFTER=\[ 55:bbb222\]' \
    && printf '%s' "$out" | grep -q 'retained for retry'; then
    pass "claim: an open-PR refusal (rc=3) is RETAINED with its lease, not dropped — only a transfer drops"
  else
    fail "claim-lease-retain: a non-transfer failure must be retained: out=[$out]"
  fi
}

# ---------------------------------------------------------------------------
# Test 30-claim (#3393, roborev round 20, High): a park on `seam1-approval`/`needs-decision` RELEASES
# the issue — it is excluded from the next pickup until the owner answers — so the next spawn must NOT
# be stamped under that issue's ref. It was, which let another lane legitimately resuming the issue
# overwrite the ref and hide a dead supervisor behind it: the collision per-lane refs exist to remove.
# ---------------------------------------------------------------------------
test_park_releases_issue_so_next_lane_is_a_placeholder() {
  local d rc stamps placeholders named
  for reason in needs-decision seam1-approval; do
    d="$(new_case_dir)"
    common_env "$d"
    write_park_stub "$d/bin/worker.sh" 88 "$reason"
    export WORKER_CMD="$d/bin/worker.sh"
    export MAX_ISSUES=100
    export BREAKER_N=100
    export CLAIM_LOG="$d/claim.log"
    : >"$CLAIM_LOG"
    write_claim_stub "$d/bin/claim.sh"
    export CLAIM_CMD="bash $d/bin/claim.sh"
    export HEARTBEAT_MACHINE="testbox"
    bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
    rc=$?
    stamps=$(grep -cE '^stamp ' "$CLAIM_LOG" 2>/dev/null || true)
    placeholders=$(grep -cE '^stamp p[0-9]+-[0-9a-f]+ [0-9]+$' "$CLAIM_LOG" 2>/dev/null || true)
    named=$(grep -cE '^stamp 88 [0-9]+$' "$CLAIM_LOG" 2>/dev/null || true)
    # NON-VACUITY is built in: the run must actually have stamped more than once, or "no stamp names
    # 88" would hold trivially for a supervisor that never reached a second iteration.
    if [[ "$stamps" -ge 2 && "$named" -eq 0 && "$placeholders" -eq "$stamps" ]]; then
      pass "claim: a '$reason' park releases issue 88, so all $stamps stamps are unique placeholders and none names the released issue"
    else
      fail "park-releases-issue ($reason): stamps=$stamps placeholders=$placeholders named=$named rc=$rc log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
    fi
  done
  # CONTROL: a TECHNICAL block (free-text reason, not a park token) must still CARRY the issue forward,
  # or the fix would have thrown away the liveness accuracy it exists to protect. This is the existing
  # claim-learn behaviour, asserted here so the two directions sit side by side.
  d="$(new_case_dir)"
  common_env "$d"
  write_blocked_same_issue_stub "$d/bin/worker.sh" 88
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  export CLAIM_LOG="$d/claim.log"
  : >"$CLAIM_LOG"
  write_claim_stub "$d/bin/claim.sh"
  export CLAIM_CMD="bash $d/bin/claim.sh"
  export HEARTBEAT_MACHINE="testbox"
  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 || true
  if grep -qE '^stamp 88 [0-9]+$' "$CLAIM_LOG"; then
    pass "claim: CONTROL — a technical block still carries the issue forward (stamp names 88), so only the park path releases"
  else
    fail "park-releases-issue control: a technical block must still name the issue: log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
}

# ---------------------------------------------------------------------------
# Test 31-claim (#3393, roborev round 25, Medium — a REGRESSION from round 24): an idle shutdown must
# CLEAR the placeholder it stamped. Round 24 reset work-concluded to 0 at the stamp (correct, so early
# exits inherit the safe value) which left `no-work` permanently unconcluded — and placeholders are never
# automatically reaped, so every NORMAL idle shutdown leaked a stale ref that dead-lanes then reported as
# a dead lane. A monitor that fires falsely on every idle stop is one an operator learns to ignore.
# ---------------------------------------------------------------------------
test_no_work_shutdown_clears_its_placeholder() {
  local d out stamped
  d="$(new_case_dir)"
  common_env "$d"
  export CLAIM_LOG="$d/claim.log"
  : >"$CLAIM_LOG"
  # A worker that reports no-work AND asks the loop to stop, so the run is exactly one idle iteration
  # followed by the normal stop-file exit — the commonest shutdown shape on an empty Ready queue.
  cat >"$d/bin/worker.sh" <<'WEOF'
#!/usr/bin/env bash
set -euo pipefail
cat >"$MARKER_FILE" <<JSON
{"outcome":"no-work","issue":null,"pr":null,"duration_s":1}
JSON
: >"${STOP_FILE:?STOP_FILE not set}"
WEOF
  chmod +x "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export BACKOFF_NOWORK_SECS=0
  export MAX_ISSUES=5
  export BREAKER_N=5
  write_claim_stub "$d/bin/claim.sh"
  export CLAIM_CMD="bash $d/bin/claim.sh"
  export HEARTBEAT_MACHINE="testbox"
  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 || true
  stamped=$(grep -oE '^stamp p[0-9]+-[0-9a-f]+' "$CLAIM_LOG" 2>/dev/null | head -1 | awk '{print $2}')
  if [[ -n "$stamped" ]] && grep -qE "^reap testbox ${stamped}( |$)" "$CLAIM_LOG"; then
    pass "claim: a no-work idle shutdown CLEARS the placeholder it stamped ($stamped) — no leaked ref for dead-lanes to misreport"
  else
    fail "no-work-clears-placeholder: stamped='$stamped' log=[$(tr '\n' ';' <"$CLAIM_LOG")] out=[$(tail -5 "$d/stdout.log")]"
  fi
  # NON-VACUITY, true of the BROKEN code too: the run must have stamped a placeholder and journalled an
  # exit summary. Both hold whether or not the clear happens, so this establishes the run did the work
  # rather than that the fix fired.
  local jf_summary=no
  grep -rqs '"outcome":"summary"' "$d/logs" 2>/dev/null && jf_summary=yes
  if [[ -n "$stamped" && "$jf_summary" == yes ]] && grep -rqs '"outcome":"no-work"' "$d/logs"; then
    pass "NON-VACUITY: the run stamped a placeholder, journalled a no-work iteration and reached its exit summary"
  else
    fail "no-work-clears-placeholder-nonvacuity: stamped='$stamped' summary=$jf_summary"
  fi
  # ...and a no-work marker that DOES name an issue must NOT conclude it — a no-work carrying an issue is
  # not evidence that issue finished, so the ref stays.
  local d2 stamped2
  d2="$(new_case_dir)"
  common_env "$d2"
  export CLAIM_LOG="$d2/claim.log"
  : >"$CLAIM_LOG"
  cat >"$d2/bin/worker.sh" <<'WEOF'
#!/usr/bin/env bash
set -euo pipefail
cat >"$MARKER_FILE" <<JSON
{"outcome":"no-work","issue":777,"pr":null,"duration_s":1}
JSON
: >"${STOP_FILE:?STOP_FILE not set}"
WEOF
  chmod +x "$d2/bin/worker.sh"
  export WORKER_CMD="$d2/bin/worker.sh"
  export BACKOFF_NOWORK_SECS=0
  write_claim_stub "$d2/bin/claim.sh"
  export CLAIM_CMD="bash $d2/bin/claim.sh"
  export HEARTBEAT_MACHINE="testbox"
  bash "$SUPERVISOR" >"$d2/stdout.log" 2>&1 || true
  stamped2=$(grep -oE '^stamp [^ ]+' "$CLAIM_LOG" 2>/dev/null | head -1 | awk '{print $2}')
  if [[ -n "$stamped2" ]] && ! grep -qE "^reap testbox ${stamped2}( |$)" "$CLAIM_LOG"; then
    pass "claim: a no-work marker that NAMES an issue does not conclude it — lane $stamped2 keeps its ref"
  else
    fail "no-work-with-issue: lane '$stamped2' must not be cleared: log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
}

# ---------------------------------------------------------------------------
# Test 33-claim (#3393, roborev round 29, Medium — a REGRESSION from round 25's guard): a `no-work`
# iteration must conclude only a PLACEHOLDER lane. Round 25 keyed on the MARKER's issue field, which is
# empty for every no-work — but the STAMPED ref can be a NUMERIC issue carried forward from a prior
# technical block, and concluding that cleared the only liveness signal for a still-unresolved issue.
# ---------------------------------------------------------------------------
test_no_work_does_not_conclude_a_numeric_lane() {
  local d stamped_issue reaped
  d="$(new_case_dir)"
  common_env "$d"
  export CLAIM_LOG="$d/claim.log"
  : >"$CLAIM_LOG"
  # BEHAVIOURAL, not a model of the code. The first cut of this case COPIED the supervisor's `case` arms
  # into the test and classified with the copy — which is exactly the defect round 24 found and fixed
  # here: a test that validates a MODEL stays green when the shipped logic moves. Driven instead through
  # the real loop with a two-phase worker: iteration 1 blocks on issue 88 for a TECHNICAL reason (so the
  # issue is carried forward), iteration 2 reports no-work and asks the loop to stop.
  cat >"$d/bin/worker.sh" <<'WEOF'
#!/usr/bin/env bash
set -euo pipefail
n_file="${LOG_DIR:?LOG_DIR not set}/.phase"
n=0; [[ -f "$n_file" ]] && n=$(cat "$n_file")
n=$((n + 1)); printf '%s' "$n" >"$n_file"
if [[ "$n" -eq 1 ]]; then
  cat >"$MARKER_FILE" <<JSON
{"outcome":"blocked","issue":88,"pr":null,"duration_s":1,"reason":"a technical block, not an owner park"}
JSON
else
  cat >"$MARKER_FILE" <<JSON
{"outcome":"no-work","issue":null,"pr":null,"duration_s":1}
JSON
  : >"${STOP_FILE:?STOP_FILE not set}"
fi
WEOF
  chmod +x "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export BACKOFF_NOWORK_SECS=0
  export MAX_ISSUES=10
  export BREAKER_N=10
  write_claim_stub "$d/bin/claim.sh"
  export CLAIM_CMD="bash $d/bin/claim.sh"
  export HEARTBEAT_MACHINE="testbox"
  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 || true
  stamped_issue=$(grep -cE '^stamp 88 [0-9]+$' "$CLAIM_LOG" 2>/dev/null || true)
  reaped=no
  grep -qE '^reap testbox 88( |$)' "$CLAIM_LOG" 2>/dev/null && reaped=yes
  # The numeric lane must have been stamped (iteration 2 carried issue 88 forward) and must NOT be reaped:
  # a no-work says nothing about an issue this lane is still holding.
  if [[ "$stamped_issue" -ge 1 && "$reaped" == no ]] \
    && grep -q 'has not concluded' "$d/stdout.log"; then
    pass "claim: a no-work after a technical block does NOT conclude the numeric lane (88) it still holds"
  else
    fail "no-work-numeric-lane: stamp88=$stamped_issue reaped=$reaped log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
  # NON-VACUITY, true of the BROKEN code too: the run really did reach a second iteration and a shutdown.
  # Both hold whether or not the fix is present — under the old guard the same run reaps 88 instead.
  local phases jf_summary=no
  phases=$(cat "$d/logs/.phase" 2>/dev/null || echo 0)
  grep -rqs '"outcome":"summary"' "$d/logs" 2>/dev/null && jf_summary=yes
  if [[ "$phases" -ge 2 && "$jf_summary" == yes ]]; then
    pass "NON-VACUITY: the run reached iteration $phases and journalled an exit summary, so the surviving ref is a decision"
  else
    fail "no-work-numeric-lane-nonvacuity: phases=$phases summary=$jf_summary"
  fi
}

# ---------------------------------------------------------------------------
# Test 34-claim (#3393, roborev round 31, Medium): a lane TRANSITION must not queue an unconcluded
# NUMERIC predecessor for reaping. Round 29 protected the shutdown path and left this one — the same
# guard, a second route. Technical block on 88 -> no-work (unconcluded, but CLAIM_ISSUE released) -> the
# next stamp is a placeholder and the transition reaped 88, deleting an unresolved issue's only signal.
#
# THREE iterations are required, which is why this case did not exist before: the defect needs a
# transition AFTER the numeric lane, so a two-iteration run cannot reach it.
# ---------------------------------------------------------------------------
test_transition_keeps_an_unconcluded_numeric_lane() {
  local d reaped88 stamped88 placeholders
  d="$(new_case_dir)"
  common_env "$d"
  export CLAIM_LOG="$d/claim.log"
  : >"$CLAIM_LOG"
  cat >"$d/bin/worker.sh" <<'WEOF'
#!/usr/bin/env bash
set -euo pipefail
n_file="${LOG_DIR:?LOG_DIR not set}/.phase"
n=0; [[ -f "$n_file" ]] && n=$(cat "$n_file")
n=$((n + 1)); printf '%s' "$n" >"$n_file"
case "$n" in
  1)  # technical block: carries issue 88 forward
      cat >"$MARKER_FILE" <<JSON
{"outcome":"blocked","issue":88,"pr":null,"duration_s":1,"reason":"a technical block, not an owner park"}
JSON
      ;;
  2)  # no-work: leaves 88 UNCONCLUDED but releases CLAIM_ISSUE
      cat >"$MARKER_FILE" <<JSON
{"outcome":"no-work","issue":null,"pr":null,"duration_s":1}
JSON
      ;;
  *)  # a third iteration happens, stamping a placeholder — this is the transition under test
      cat >"$MARKER_FILE" <<JSON
{"outcome":"no-work","issue":null,"pr":null,"duration_s":1}
JSON
      : >"${STOP_FILE:?STOP_FILE not set}"
      ;;
esac
WEOF
  chmod +x "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh"
  export BACKOFF_NOWORK_SECS=0
  export MAX_ISSUES=10
  export BREAKER_N=10
  write_claim_stub "$d/bin/claim.sh"
  export CLAIM_CMD="bash $d/bin/claim.sh"
  export HEARTBEAT_MACHINE="testbox"
  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 || true
  stamped88=$(grep -cE '^stamp 88 [0-9]+$' "$CLAIM_LOG" 2>/dev/null || true)
  reaped88=no
  grep -qE '^reap testbox 88( |$)' "$CLAIM_LOG" 2>/dev/null && reaped88=yes
  if [[ "$stamped88" -ge 1 && "$reaped88" == no ]] \
    && grep -q 'SKIPPED: its work has not concluded' "$d/stdout.log"; then
    pass "claim: a transition past an UNCONCLUDED numeric lane (88) does not queue it for reaping"
  else
    fail "transition-keeps-numeric: stamp88=$stamped88 reaped88=$reaped88 log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
  # A PLACEHOLDER predecessor must still be collected — the exception exists so the round-5 leak stays
  # fixed, and getting it wrong in the other direction trades one leak for another.
  placeholders=$(grep -cE '^reap testbox p[0-9]+-[0-9a-f]+' "$CLAIM_LOG" 2>/dev/null || true)
  if [[ "$placeholders" -ge 1 ]]; then
    pass "claim: a PLACEHOLDER predecessor is still queued and reaped ($placeholders), so the round-5 leak stays fixed"
  else
    fail "transition-placeholder-still-reaped: no placeholder was reaped: log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
  # NON-VACUITY, true of the BROKEN code too: the run reached a THIRD iteration, which is what makes the
  # transition-after-numeric reachable at all. Under the old code the same run reaps 88 instead.
  local phases
  phases=$(cat "$d/logs/.phase" 2>/dev/null || echo 0)
  if [[ "$phases" -ge 3 ]]; then
    pass "NON-VACUITY: the run reached iteration $phases, so the transition after the numeric lane really occurred"
  else
    fail "transition-keeps-numeric-nonvacuity: only $phases iteration(s) — the transition under test never happened"
  fi
}

# ---------------------------------------------------------------------------
# Test 32-claim (#3393, roborev round 28, Medium): an ENDGAME IN FLIGHT keeps its ref. Owner ruling (b)
# on #2499 semantics — a pending auto-merge PR IS an open PR, and `delete_ref_guarded` already refuses to
# delete an issue-named ref in that state. But `CLAIM_WORK_CONCLUDED` reflects only the LATEST iteration,
# so after a pending-automerge finalize a later no-work/finalize/park set it to 1 and the shutdown cleared
# the lane's ref anyway. "Concluded" is necessary and NOT sufficient: nothing may be pending either.
# ---------------------------------------------------------------------------
test_pending_pr_keeps_the_claim() {
  local d out
  d="$(new_case_dir)"
  common_env "$d"
  export CLAIM_LOG="$d/claim.log"
  : >"$CLAIM_LOG"
  write_claim_stub "$d/bin/claim.sh"
  # Unit-tested deliberately: reaching this state end to end needs a pending-automerge finalize followed
  # by a concluding iteration AND a budget exit, which no existing stub sequences. The invariant is one
  # condition in one function, so it is exercised directly — the approach the parser tests take.
  #
  # THE STAMPED LANE IS AN ISSUE NUMBER, AND THAT NOW MATTERS (roborev round 36). This case originally
  # staged a `p999-abc` PLACEHOLDER, which was incidental to what it asserts — its stated invariant is
  # "a pending auto-merge PR keeps the lane ref", and that is what an ISSUE-numbered lane still does.
  # The PLACEHOLDER path deliberately behaves differently now: keeping a placeholder was a trap, because
  # `should-reap` permanently refuses placeholders, so after the supervisor exited NOTHING could ever
  # clear it. Its protection is transferred to an issue-numbered ref instead, and that path is pinned by
  # `test_placeholder_endgame_protection_transfers` below rather than by weakening this case.
  # Changed the PREMISE to keep the invariant honest — not the assertion to match new behaviour.
  out="$(
    CLAIM_CMD="bash $d/bin/claim.sh" HEARTBEAT_MACHINE=testbox CLAIM_LOG="$CLAIM_LOG" \
    bash -c '
      source "$1"
      CLAIM_STAMPED_ISSUE="88"
      CLAIM_STAMPED_SHA="feed0002"
      PENDING_PR_LIST="4242'$'\t''88'$'\t''1'$'\t''0"
      clear_claim 1          # CONCLUDED=1, but a PR is pending
      printf "AFTER_PENDING=%s\n" "$(grep -c "^reap" "$CLAIM_LOG" 2>/dev/null || echo 0)"
      PENDING_PR_LIST=""
      clear_claim 1          # concluded AND nothing pending => clears
      printf "AFTER_EMPTY=%s\n" "$(grep -c "^reap" "$CLAIM_LOG" 2>/dev/null || echo 0)"
    ' _ "$SUPERVISOR" 2>&1
  )"
  if printf '%s' "$out" | grep -q 'auto-merge PR is still pending' \
    && printf '%s' "$out" | grep -q 'AFTER_PENDING=0' \
    && printf '%s' "$out" | grep -q 'AFTER_EMPTY=1'; then
    pass "claim: a pending auto-merge PR KEEPS the lane ref even when concluded=1, and the same call clears once nothing is pending"
  else
    fail "pending-pr-keeps-claim: out=[$out] log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
  # NON-VACUITY, AND IT MUST HOLD ON THE BROKEN CODE TOO — which the first cut did not. It required
  # `AFTER_EMPTY=1` exactly, but with the fix removed BOTH calls reap, so the count becomes 2 and the
  # probe failed alongside the assertion it was meant to qualify. That is the round-24 rule violated by
  # the very case that cites it. Keyed on "at least one reap happened" instead, which is true whether or
  # not the pending-PR hold is present, so it establishes reachability rather than the fix.
  local reaps_seen
  reaps_seen=$(printf '%s' "$out" | sed -n 's/.*AFTER_EMPTY=\([0-9][0-9]*\).*/\1/p' | head -1)
  if [[ -n "$reaps_seen" && "$reaps_seen" -ge 1 ]]; then
    pass "NON-VACUITY: the reap path IS reachable in this harness (${reaps_seen} reap(s) seen), so AFTER_PENDING=0 is a refusal"
  else
    fail "pending-pr-nonvacuity: the reap path never fires here, so the refusal proves nothing: out=[$out]"
  fi
}

# ---------------------------------------------------------------------------
# Test 25-claim (#3393, roborev round 2, Medium): a lane TRANSITION must not open a liveness GAP. The
# replacement is stamped BEFORE the old ref is deleted, so if the replacement FAILS the OLD ref must
# SURVIVE — a lane with no claim ref at all is invisible to dead-lanes and to the reaper for the whole
# iteration, which is a gap introduced by the leak fix rather than by the leak.
#
# THIS FUNCTION WAS INVOKED AND NEVER DEFINED until roborev round 27 (Medium). The suite reported
# "80 passed, 0 failed" through eleven gates while this case never ran; the `t` wrapper above now makes an
# undefined invocation a failure. The regression it was meant to pin is finally pinned here.
# ---------------------------------------------------------------------------
test_claim_transition_survives_failed_replacement() {
  local d placeholder stamped_issue reaped
  d="$(new_case_dir)"
  common_env "$d"
  export CLAIM_LOG="$d/claim.log"
  : >"$CLAIM_LOG"
  # A technical block (free-text reason, NOT a park token) retains the issue, so iteration 2 attempts the
  # ISSUE-named replacement stamp — which this stub fails, while letting the placeholder stamp succeed.
  write_blocked_same_issue_stub "$d/bin/worker.sh" 88
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  write_claim_stub_failing_issue_stamp "$d/bin/claim.sh"
  export CLAIM_CMD="bash $d/bin/claim.sh"
  export HEARTBEAT_MACHINE="testbox"
  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 || true
  placeholder=$(grep -oE '^stamp p[0-9]+-[0-9a-f]+' "$CLAIM_LOG" 2>/dev/null | head -1 | awk '{print $2}')
  stamped_issue=$(grep -cE '^stamp 88 [0-9]+$' "$CLAIM_LOG" 2>/dev/null || true)
  reaped=no
  [[ -n "$placeholder" ]] && grep -qE "^reap testbox ${placeholder}( |$)" "$CLAIM_LOG" 2>/dev/null && reaped=yes
  # The failed replacement must have been ATTEMPTED (or the case proves nothing), and the old ref must
  # still be there.
  if [[ -n "$placeholder" && "$stamped_issue" -ge 1 && "$reaped" == no ]]; then
    pass "claim: a FAILED replacement stamp leaves the old ref ($placeholder) in place — no liveness gap"
  else
    fail "claim-transition-gap: placeholder='$placeholder' issue_stamp_attempts=$stamped_issue reaped=$reaped log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
  # NON-VACUITY / CONTROL: with a stub whose replacement SUCCEEDS, the old placeholder IS cleared. So the
  # survival above is caused by the failure, not by the transition never happening or by a reap that never
  # runs in this shape.
  local d2 ph2 reaped2
  d2="$(new_case_dir)"
  common_env "$d2"
  export CLAIM_LOG="$d2/claim.log"
  : >"$CLAIM_LOG"
  write_blocked_same_issue_stub "$d2/bin/worker.sh" 88
  export WORKER_CMD="$d2/bin/worker.sh"
  export MAX_ISSUES=100
  export BREAKER_N=100
  write_claim_stub "$d2/bin/claim.sh"
  export CLAIM_CMD="bash $d2/bin/claim.sh"
  export HEARTBEAT_MACHINE="testbox"
  bash "$SUPERVISOR" >"$d2/stdout.log" 2>&1 || true
  ph2=$(grep -oE '^stamp p[0-9]+-[0-9a-f]+' "$CLAIM_LOG" 2>/dev/null | head -1 | awk '{print $2}')
  reaped2=no
  [[ -n "$ph2" ]] && grep -qE "^reap testbox ${ph2}( |$)" "$CLAIM_LOG" 2>/dev/null && reaped2=yes
  if [[ -n "$ph2" && "$reaped2" == yes ]]; then
    pass "NON-VACUITY: when the replacement SUCCEEDS the old placeholder IS cleared, so the survival above is attributable to the failure"
  else
    fail "claim-transition-gap-control: ph2='$ph2' reaped2=$reaped2 — the control must clear the old ref"
  fi
}

test_claim_drain_never_deletes_current_lane() {
  local d out
  d="$(new_case_dir)"
  common_env "$d"
  export CLAIM_LOG="$d/claim.log"
  : >"$CLAIM_LOG"
  # A reap stub that always FAILS, so a queued cleanup stays queued and the drain keeps retrying it.
  cat >"$d/bin/claim.sh" <<'STUBEOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${CLAIM_LOG:?CLAIM_LOG not set}"
[ "${1:-}" = "reap" ] && exit 1
exit 0
STUBEOF
  chmod +x "$d/bin/claim.sh"

  # UNIT-TESTED, deliberately. Reaching the protected state end to end needs three stamps in the
  # order placeholder -> issue -> placeholder, which requires a worker that blocks on one iteration
  # and finalizes on the next; no existing stub alternates that way, and building one would test the
  # stub more than the invariant. The invariant itself is one function, so it is exercised directly —
  # the same approach the parser tests take with verify_finalized_pr.
  out="$(
    # HEARTBEAT_MACHINE, not CLAIM_MACHINE: sourcing the supervisor DERIVES CLAIM_MACHINE from it,
    # so presetting CLAIM_MACHINE is overwritten at source time and the reap lands on the real hostname.
    CLAIM_CMD="bash $d/bin/claim.sh" HEARTBEAT_MACHINE=testbox \
    CLAIM_LOG="$CLAIM_LOG" \
    bash -c '
      source "$1"
      CLAIM_PENDING_CLEANUP=" p123-abc:aaa111 88:bbb222 "
      # Draining while lane p123-abc is the CURRENT one must skip it and retry only 88.
      claim_drain_pending_cleanup "p123-abc"
      printf "PENDING_AFTER=[%s]\n" "$CLAIM_PENDING_CLEANUP"
      # ROUND 32: a BARE entry (no lease recorded) must be DROPPED, not drained. Round 19 deliberately
      # kept draining those "so an entry queued by an older process is still cleaned" — and that was
      # itself the defect: draining without a lease IS the unleased delete that can remove a
      # successor'"'"'s live claim.
      CLAIM_PENDING_CLEANUP=" 77 "
      claim_drain_pending_cleanup
      printf "BARE_AFTER=[%s]\n" "$CLAIM_PENDING_CLEANUP"
    ' _ "$SUPERVISOR" 2>&1
  )"
  # Three things must hold: the current lane is announced as skipped, it is NOT reaped, and the other
  # id IS retried and retained (its reap failed).
  if printf '%s' "$out" | grep -q 'pending cleanup of p123-abc dropped: it is the lane currently stamped' \
    && ! grep -qE '^reap testbox p123-abc( |$)' "$CLAIM_LOG" \
    && grep -qE '^reap testbox 88 bbb222$' "$CLAIM_LOG" \
    && ! grep -qE '^reap testbox 77' "$CLAIM_LOG" \
    && printf '%s' "$out" | grep -q 'DROPPED: no lease was recorded' \
    && printf '%s' "$out" | grep -q 'BARE_AFTER=\[\]' \
    && printf '%s' "$out" | grep -q 'PENDING_AFTER=\[ 88:bbb222\]'; then
    pass "claim: the drain SKIPS the current lane, retries the other and RETAINS it with its lease on failure, and DROPS a bare leaseless entry"
  else
    fail "claim-drain-current: protection did not hold. out=[$out] log=[$(tr '\n' ';' <"$CLAIM_LOG")]"
  fi
}

# ---------------------------------------------------------------------------
# F1 (stale-lock reclaim double-acquire race) note: the fix makes reclaim
# atomic via `mv "$LOCK" "$LOCK.stale.$$" && rm -rf "$LOCK.stale.$$"` instead
# of `rm -rf "$LOCK"; mkdir "$LOCK"`. Reliably reproducing the ORIGINAL race
# requires two processes hitting the reclaim window at the exact same instant
# — any test harness recreation of that is inherently sleep/timing-dependent
# and would be flaky by construction (the class of test this suite explicitly
# avoids per its <30s/no-sleep-loop design goal). Covered by code inspection
# instead: `mv` on the same filesystem is atomic (POSIX rename(2)), so of two
# racers only one `mv "$LOCK" "$LOCK.stale.$$"` can succeed against a given
# stale directory name; the loser's `mv` fails (source already gone) and it
# falls through to its own `mkdir "$LOCK"`, which fails against the winner's
# fresh lock, hitting the loud lost-race exit path.
# No test function here by design — see comment in acquire_lock() itself.
# UPDATED (#3601): that exit path is now `supervisor_lock_refuse_lost_race`, which names the cause
# instead of the pre-#3601 bare "failed to acquire lock", and it is reached only when the name is taken
# by someone else — a `mkdir` that fails because the PATH cannot hold a lock is diagnosed separately
# (`test_lane_lock_uncreatable_path_is_not_reported_as_contention`). The reclaim's operands also now
# terminate option parsing, which `test_lane_lock_option_shaped_tmpdir_starts_normally` drives. The
# same-instant two-racer interleaving remains untested for the reason above; the interleaving that IS
# reproducible — a peer holding a PID-LESS lock — is driven with a real competing process by
# `test_lane_lock_pidless_window_is_never_read_as_dead`.

# ---------------------------------------------------------------------------
# Test (#2841): the resolved DEFAULT WORKER_CMD (caller does not export one)
# is a headless-executable invocation — source the supervisor with WORKER_CMD
# unset (the source-guard keeps main() from running) and assert the resolved
# value carries `-p`, `--dangerously-skip-permissions`, and `--agent flow-lead`,
# and does NOT name the non-existent `--agent worker`. A future edit that drops
# any of these fails here rather than shipping a silently-broken default.
# ANTI-DRIFT (roborev #2841): also assert PROC_MATCH_WORKER actually MATCHES the
# resolved default WORKER_CMD — a flag reorder or regex edit that desynced the
# orphan-probe pattern from the spawn shape (the #2670 coupling) fails HERE.
# ---------------------------------------------------------------------------
test_default_worker_cmd_is_headless() {
  local resolved pat
  # shellcheck disable=SC2016  # $SUP/$WORKER_CMD expand inside the sub-bash, not here.
  resolved="$(env -u WORKER_CMD SUP="$SUPERVISOR" bash -c 'source "$SUP"; printf %s "$WORKER_CMD"' 2>/dev/null)"
  # shellcheck disable=SC2016  # $SUP/$PROC_MATCH_WORKER expand inside the sub-bash, not here.
  pat="$(env -u WORKER_CMD SUP="$SUPERVISOR" bash -c 'source "$SUP"; printf %s "$PROC_MATCH_WORKER"' 2>/dev/null)"
  if [[ "$resolved" == *' -p '* && "$resolved" == *'--dangerously-skip-permissions'* &&
        "$resolved" == *'--agent flow-lead'* && "$resolved" != *'--agent worker'* ]] &&
     printf '%s' "$resolved" | grep -qE "$pat"; then
    pass "default WORKER_CMD: headless (-p + skip-permissions + --agent flow-lead) AND matched by PROC_MATCH_WORKER"
  else
    fail "default WORKER_CMD: resolved='$resolved' pat='$pat' matched=$(printf '%s' "$resolved" | grep -qE "$pat" && echo yes || echo no)"
  fi
}

# ---------------------------------------------------------------------------
# Test (#2841 / design decision A, R3): a HEALTHY worker whose stub emits stream
# activity to stdout produces a NON-EMPTY iter-N.log (the redirect captures the
# `-p --output-format stream-json --verbose` event stream the watchdog scans),
# so the watchdog is not blinded under `-p`. The existing wedge classifier tests
# (test_genuine_wedge_frozen_is_stuck etc.) cover the frozen-log+signature side.
# ---------------------------------------------------------------------------
test_healthy_worker_iterlog_nonempty() {
  local d counter jf rc fcount logsize
  d="$(new_case_dir)"
  counter="$d/counter"
  common_env "$d"
  write_verbose_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  jf="$JOURNAL_FILE"

  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  logsize=0
  [[ -f "$LOG_DIR/iter-1.log" ]] && logsize=$(wc -c <"$LOG_DIR/iter-1.log" | tr -d ' ')
  if [[ "$rc" -eq 0 && "$fcount" -eq 1 && "$logsize" -gt 0 ]] &&
     grep -q 'tool_use' "$LOG_DIR/iter-1.log"; then
    pass "healthy worker: -p stream activity captured into non-empty iter-1.log ($logsize bytes)"
  else
    fail "healthy iter-log: rc=$rc finalized=$fcount logsize=$logsize (see $LOG_DIR/iter-1.log)"
  fi
}

# ---------------------------------------------------------------------------
# Test (#2849 REGRESSION): setting CLAIM_CMD="" MUST truly disable claim
# stamping — it must NOT be silently re-defaulted back to the real
# claim-heartbeat.sh (git push / gh pr list — network ops). The original defect
# used `${CLAIM_CMD:-default}` (colon), which substitutes the default for an
# EMPTY string too, so common_env's `export CLAIM_CMD=""` hit the real network
# path and a slow/contended origin push or `gh pr list` WEDGED the supervisor —
# the non-deterministic tooling-tests hang. Pinned three ways:
#   (a) sourced with CLAIM_CMD="", the resolved value stays empty. Guarded against
#       a VACUOUS pass (an aborted `source "$SUP"` under `set -euo pipefail` would
#       ALSO print nothing): the sub-bash prints a `MARK:` sentinel, so success is
#       the exact string `MARK:` (empty CLAIM_CMD) — never empty-because-aborted.
#   (b) the config line uses the colonless `${CLAIM_CMD-` form (source-level pin,
#       survives a refactor that moves the resolution).
#   (c) LIVE: a full nasty-reason iteration with CLAIM_CMD="" invokes NO claim
#       command on EITHER path — success (`claim stamped/cleared`) OR failure
#       (`claim stamp/clear failed|declined`, which is what a re-defaulted call
#       WOULD log in a no-push/hermetic env). The run is BOUNDED by a
#       background+poll+kill watchdog (macOS has no `timeout(1)`): a re-introduced
#       slow-network claim path is caught as a wedge (kill + FAIL), not a hang.
# ---------------------------------------------------------------------------
test_claim_cmd_empty_truly_disables_no_network() {
  local resolved cfg_line d jf rc invoked sup_pid waited finished
  # (a) NON-VACUOUS resolved pin: MARK: prefix distinguishes "CLAIM_CMD is empty"
  # from "source aborted and printed nothing" (mirrors the sibling anti-drift pins).
  # shellcheck disable=SC2016  # $SUP/$CLAIM_CMD expand inside the sub-bash, not here.
  resolved="$(env CLAIM_CMD="" SUP="$SUPERVISOR" bash -c 'source "$SUP"; printf "MARK:%s" "$CLAIM_CMD"' 2>/dev/null)"
  # (b) config line uses the colonless default form.
  cfg_line="$(grep -E '^CLAIM_CMD=' "$SUPERVISOR" | head -1)"
  # (c) LIVE, BOUNDED: with CLAIM_CMD="" the supervisor must invoke NO claim command
  # at all. Background it and poll for exit up to a 60s cap; a re-defaulted slow claim
  # path would exceed the cap → kill + FAIL (proving the no-hang property), never a
  # silent suite wedge. The nasty-reason marker still drives a bounded head-blocked stop.
  d="$(new_case_dir)"
  common_env "$d" # sets CLAIM_CMD=""
  write_blocked_nasty_reason_stub "$d/bin/worker.sh"
  export WORKER_CMD="$d/bin/worker.sh" MAX_ISSUES=1 BREAKER_N=1
  jf="$JOURNAL_FILE"
  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1 &
  sup_pid=$!
  waited=0
  finished="no"
  while [[ "$waited" -lt 600 ]]; do # 600 * 0.1s = 60s bound
    kill -0 "$sup_pid" 2>/dev/null || { finished="yes"; break; }
    sleep 0.1
    waited=$((waited + 1))
  done
  if [[ "$finished" != "yes" ]]; then
    kill -KILL "$sup_pid" 2>/dev/null || true
    wait "$sup_pid" 2>/dev/null || true
    fail "#2849: supervisor did NOT finish within 60s with CLAIM_CMD='' — a re-defaulted claim path is wedging it (see $d/stdout.log)"
    return
  fi
  wait "$sup_pid"
  rc=$?
  # Match a claim invocation on BOTH the success AND failure log paths: a real
  # claim-heartbeat.sh call in a hermetic/no-push env FAILS and logs a WARN
  # ("claim stamp failed" / "claim clear declined/failed"), which a success-only
  # grep would miss — letting a reintroduced `${CLAIM_CMD:-…}` pass unnoticed.
  invoked="no"
  grep -qiE 'claim (stamped|cleared)|claim (stamp|clear) (failed|declined)' "$d/stdout.log" && invoked="yes"
  if [[ "$resolved" == "MARK:" && "$cfg_line" == *'${CLAIM_CMD-'* && "$cfg_line" != *'${CLAIM_CMD:-'* &&
        "$rc" -eq 0 && "$invoked" == "no" ]] && grep -q '"outcome":"blocked"' "$jf"; then
    pass "#2849: CLAIM_CMD='' truly disables claim stamping (no network, no re-default); nasty run completes within 60s bound"
  else
    fail "#2849: resolved='$resolved' cfg='$cfg_line' rc=$rc claim_invoked=$invoked (see $d/stdout.log)"
  fi
}

# ---------------------------------------------------------------------------
# Test (#2849 HERMETICITY, documented + enforced): every REAL pgrep process-table
# scan in THIS suite matches the whole host, so on a dev box concurrently running
# Claude Code / a gate (cargo|nextest|gate_slot_daemon) it WILL match host
# processes. Each such line MUST therefore scope its assertion to the test's OWN
# spawned PID via `grep -qw "$...pid"` on the same line — never assert on a bare
# match count and never block on a host match. This meta-check fails if a future
# edit adds an un-PID-scoped real pgrep scan, re-introducing host contamination.
# The scan matches `pgrep` + any flag group containing `f` (`-f`, `-af`, `-fl`,
# `-lf`) in ANY position (`if pgrep`, `out="$(pgrep …)"`, `while ! pgrep`) on a
# NON-comment line, so it is not fooled by a form other than a line-leading
# `pgrep -f`. (Its own pass/fail text says "pgrep process scan" — no `-flag` —
# and the pattern literal has no whitespace after `pgrep`, so neither self-matches.)
# ---------------------------------------------------------------------------
test_real_pgrep_usages_are_pid_scoped() {
  local bad="" line
  # Strip comment lines (first non-blank char `#`), then flag any real pgrep scan
  # whose line does not PID-scope via `grep -qw`.
  # TWO acceptable scopings, and `pgrep-lint-allow` is not a blanket exemption — it asserts the SECOND:
  #   * `grep -qw $pid`      — pid-scoped: the scan can only match a pid this test owns.
  #   * a RUN-UNIQUE MARKER  — the pattern contains a token minted for this run ($$ + $RANDOM), so no
  #                            host process can carry it. Used by the probe two-direction control, which
  #                            must exercise the REAL pgrep pipeline and therefore cannot pid-scope it.
  # Both bound the scan to this test's own processes, which is the property #2849 is about.
  while IFS= read -r line; do
    [[ "$line" == *'grep -qw'* || "$line" == *'pgrep-lint-allow'* ]] || bad="${bad}${line}\n"
  done < <(grep -vE '^[[:space:]]*#' "${BASH_SOURCE[0]}" | grep -E 'pgrep[[:space:]]+-[a-zA-Z]*f')
  if [[ -z "$bad" ]]; then
    pass "#2849: every real pgrep process scan is PID-scoped (grep -qw \$pid) — hermetic vs host processes"
  else
    fail "#2849: un-PID-scoped real pgrep process scan(s) can match host processes:\n$(printf '%b' "$bad")"
  fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
echo "=== worker-supervisor test suite ==="
t test_happy_path_budget_stop
t test_breaker_stops_on_abnormal
t test_stop_file_honored
t test_preflight_load_hold
t test_nowork_not_counted
t test_single_instance_lock
t test_stale_marker_removed
t test_repeated_blocked_head_of_queue_stops
t test_finalized_missing_pr_is_abnormal
t test_journal_escapes_nasty_reason
t test_park_seam1_parked_on_owner
t test_park_needs_decision_question_in_title
t test_unknown_outcome_is_abnormal
t test_stuck_on_question_detected
t test_prompt_signature_grep
t test_stuck_breaks_abnormal_chain
t test_repeated_park_same_issue_stops
t test_different_issue_parks_do_not_head_block
t test_stray_signature_scrollback_is_abnormal
t test_genuine_wedge_frozen_is_stuck
t test_busy_writing_signature_not_stuck
t test_fast_exit_latency
t test_claim_stamp_each_iter_and_clear_on_exit
t test_claim_issue_learned_from_marker
t test_claim_transition_survives_failed_replacement
t test_claim_drain_never_deletes_current_lane
t test_clear_claim_keeps_placeholder_on_abnormal_exit
t test_supervisor_lock_is_per_lane
t test_claim_cleanup_uses_lease_and_drops_on_transfer
t test_park_releases_issue_so_next_lane_is_a_placeholder
t test_no_work_shutdown_clears_its_placeholder
t test_no_work_does_not_conclude_a_numeric_lane
t test_transition_keeps_an_unconcluded_numeric_lane
t test_pending_pr_keeps_the_claim
t test_finalized_verified_merged_counts
t test_finalized_mismatch_open_is_abnormal
t test_finalized_unverified_not_counted_no_breaker
t test_proc_probe_discriminates_worker_claude
t test_leftover_hold_bounded_stops
t test_persistent_unverified_stops
t test_forged_pr_is_unresolved_mismatch
t test_stop_file_honored_mid_hold
t test_probe_no_self_match
t test_parser_absent_is_unverified
t test_pending_automerge_verdict
t test_mismatch_grace_absorbs_lag
t test_foreign_url_is_unresolved
t test_alternating_holds_still_bounded
t test_maxhours_only_hold_no_abort
t test_transport_notfound_is_unverified
t test_python_only_parser_automerge
t test_stop_file_honored_mid_grace
t test_persistent_pending_automerge_stops
t test_healthy_multi_pr_no_false_stop
t test_pending_time_floor_blocks_fast_stuck
t test_pending_pr_closed_pages_high
t test_unverified_streak_survives_intervening_abnormal
t test_deferred_stuck_stop_clean_exit
t test_pending_time_floor_crossed_trips
t test_numeric_knob_validation
t test_build_hold_uses_loose_bound
t test_build_hold_clears_then_proceeds
t test_probe_list_derives_from_count_set
t test_grace_cap_disabled_semantics
t test_mid_grace_stop_is_aborted
t test_default_worker_cmd_is_headless
t test_healthy_worker_iterlog_nonempty
t test_claim_cmd_empty_truly_disables_no_network
t test_real_pgrep_usages_are_pid_scoped
t test_default_notify_path_publishes

# ---------------------------------------------------------------------------
# Test 29-claim (#3393, roborev round 33 High): the claim lock's holder identity is machine+ACTOR, and
# every lane defaulted to the shared actor `flow`. Harmless while a machine-global lock made a second
# lane impossible; THIS change made the lock per-lane, so two default lanes can now run and each would
# read the other's claim as its own (`verify` false-positive / `release` cross-delete). Removing the
# coarse guard exposed the finer defect it was masking.
# ---------------------------------------------------------------------------
test_claim_actor_is_lane_unique() {
  local body a b same c e
  body="$T_LOCKFN/actorfn.sh"
  mkdir -p "$T_LOCKFN"
  {
    printf '%s\n' '#!/usr/bin/env bash'
    printf '%s\n' 'log() { :; }'
    sed -n '/^supervisor_lane_id()/,/^}/p' "$SUPERVISOR"
    sed -n '/^supervisor_claim_actor()/,/^}/p' "$SUPERVISOR"
    # Read it back out of the ENVIRONMENT, not the shell variable: the worker that calls claim.sh is a
    # CHILD process, so a merely-set value would leave it on the shared default. `env` is the assertion.
    # THE PREMISE HAS TO BE **UNSET**, NOT EMPTY. `CLAIM_ACTOR="" cmd` marks the name EXPORTED in the
    # child's environment, so a later plain assignment propagates to grandchildren with no `export` at
    # all — the first cut of this case staged it that way and its own RED did not fire, because the
    # assert was true of the un-exported code too. `unset` is a builtin, so it survives PATH="".
    printf '%s\n' '[[ "${T_UNSET_ACTOR:-}" == 1 ]] && unset CLAIM_ACTOR'
    printf '%s\n' 'supervisor_claim_actor; "$BASH" -c '"'"'printf "%s\n" "${CLAIM_ACTOR:-UNSET-IN-CHILD}"'"'"''
  } >"$body"
  # FROM `LANE_ID` (lead ruling B): the actor no longer re-infers the lane from REPO_ROOT.
  a=$(T_UNSET_ACTOR=1 LANE_ID=lane-1111 "$BASH" "$body")
  b=$(T_UNSET_ACTOR=1 LANE_ID=lane-2222 "$BASH" "$body")
  same=$(T_UNSET_ACTOR=1 LANE_ID=lane-1111 "$BASH" "$body")
  if [[ "$a" != "UNSET-IN-CHILD" && "$a" != "$b" && "$a" == "$same" ]]; then
    pass "claim-actor: EXPORTED to the child, derived from the GIVEN LANE_ID, and stable ($a vs $b)"
  else
    fail "claim-actor: a=[$a] b=[$b] same=[$same] — must reach the child, differ per lane, and be stable"
  fi
  c=$(T_UNSET_ACTOR=1 LANE_ID=boxA-lane "$BASH" "$body")
  e=$(T_UNSET_ACTOR=1 LANE_ID=boxB-lane "$BASH" "$body")
  if [[ "$c" != "$e" ]]; then
    pass "claim-actor: two distinct LANE_IDs get different actors"
  else
    fail "claim-actor-basename: both resolved to [$c]"
  fi
  # claim.sh REFUSES an actor with fewer than 3 recordable characters, so a degenerate value would be a
  # fail-closed claim rather than an alias. Assert the shape the lock will actually accept.
  if [[ "${#a}" -ge 3 && "$a" == flow-* && "$a" != *[!A-Za-z0-9._-]* ]]; then
    pass "claim-actor: recordable single token >=3 chars, claim.sh-acceptable ($a)"
  else
    fail "claim-actor-shape: [$a] is not a recordable single token of >=3 chars"
  fi
  # THE BOUND AND THE ORDER ARE PROPERTIES OF THE FALLBACK DERIVATION, so they are tested THERE.
  # They used to be reached through the actor, because the actor inferred its own lane; after the
  # ruling the actor takes a GIVEN identity, and it is `supervisor_lane_id` — the fallback used when
  # `LANE_ID` is unset — that must stay bounded and hash-first. `claim.sh`'s `sanitize_field` caps a
  # field at 120 chars, so a hash placed LAST is truncatable and two lanes could collapse onto one.
  local lidbody long_a long_b
  lidbody="$T_LOCKFN/lidonly.sh"
  {
    printf '%s\n' '#!/usr/bin/env bash'
    sed -n '/^supervisor_lane_id()/,/^}/p' "$SUPERVISOR"
    printf '%s\n' 'supervisor_lane_id'
  } >"$lidbody"
  long_a=$(REPO_ROOT="/data/lanes/$(printf 'l%.0s' $(seq 1 200))" "$BASH" "$lidbody")
  long_b=$(REPO_ROOT="/data/other/$(printf 'l%.0s' $(seq 1 200))" "$BASH" "$lidbody")
  if [[ "${#long_a}" -le 60 && "$long_a" =~ ^[0-9]+- ]]; then
    pass "fallback-derivation: a 200-char basename yields a bounded, hash-FIRST id (${#long_a} chars) — truncation costs readability, never uniqueness"
  else
    fail "fallback-derivation-bound: [$long_a] is ${#long_a} chars and/or not hash-first"
  fi
  if [[ "$long_a" != "$long_b" ]]; then
    pass "fallback-derivation: two 200-char-basename lanes still derive DIFFERENT ids"
  else
    fail "fallback-derivation-alias: both long lanes derived [$long_a]"
  fi

  # An operator-set actor still wins — the fix must not seize the override.
  local ov
  ov=$(CLAIM_ACTOR=owner-run LANE_ID=lane-1111 "$BASH" "$body")
  if [[ "$ov" == "owner-run" ]]; then
    pass "claim-actor: an explicit CLAIM_ACTOR is still honoured"
  else
    fail "claim-actor-override: got [$ov]"
  fi
  # Builtins only, same reason as the lock path (#3464 family 2): cases source this file under a
  # stripped PATH. `$BASH` absolute, since PATH='' cannot find bash itself.
  local stripped
  stripped=$(T_UNSET_ACTOR=1 LANE_ID=lane-1111 PATH="" "$BASH" "$body" 2>&1)
  if [[ "$stripped" == "$a" ]]; then
    pass "claim-actor: resolves with an EMPTY PATH — builtins only"
  else
    fail "claim-actor-builtins: with PATH='' got [$stripped], expected [$a]"
  fi
  # WIRED, not merely defined: the resolution must run on the documented path before any worker spawn.
  # A helper nothing calls is #3464's check-whose-subject-never-ran family.
  if grep -qE '^[[:space:]]*supervisor_claim_actor$' "$SUPERVISOR"; then
    pass "claim-actor: supervisor_claim_actor is CALLED (not just defined)"
  else
    fail "claim-actor-unwired: supervisor_claim_actor is defined but never invoked"
  fi
}

# ---------------------------------------------------------------------------
# Test 30-claim (#3393, roborev round 33 High): A CONCLUSION IS ABOUT AN ISSUE, AND THE FLAG IS ABOUT A
# LANE. A marker concluding issue 99 set the global flag while the stamped lane was issue 88, so
# `clear_claim` could delete issue 88's liveness ref with its work unresolved.
# ---------------------------------------------------------------------------
test_conclusion_must_match_the_stamped_lane() {
  local body out
  body="$T_LOCKFN/conclfn.sh"
  mkdir -p "$T_LOCKFN"
  {
    printf '%s\n' '#!/usr/bin/env bash'
    sed -n '/^conclusion_matches_stamped_lane()/,/^}/p' "$SUPERVISOR"
    printf '%s\n' 'CLAIM_STAMPED_ISSUE="$1"; if conclusion_matches_stamped_lane "$2"; then echo MATCH; else echo MISMATCH; fi'
  } >"$body"
  # The defect's exact shape: stamped 88, marker concludes 99.
  out=$("$BASH" "$body" 88 99)
  if [[ "$out" == "MISMATCH" ]]; then
    pass "conclusion-lane: stamped 88 + marker concluding 99 is a MISMATCH (ref preserved)"
  else
    fail "conclusion-lane: stamped 88 / marker 99 reported [$out] — the round-33 defect"
  fi
  out=$("$BASH" "$body" 88 88)
  if [[ "$out" == "MATCH" ]]; then
    pass "conclusion-lane: the matching issue still concludes (the fix does not strand the normal path)"
  else
    fail "conclusion-lane: stamped 88 / marker 88 reported [$out] — over-tightened"
  fi
  # A PLACEHOLDER lane has no issue to match, and an EMPTY stamped value means no lease was recorded.
  # Both must stay permissive, or a placeholder iteration could never conclude and its ref would be
  # refused by automated reaping forever (the round-28/31 failure mode, in reverse).
  out=$("$BASH" "$body" p1234 77)
  if [[ "$out" == "MATCH" ]]; then
    pass "conclusion-lane: a PLACEHOLDER stamped lane still concludes (no issue to match)"
  else
    fail "conclusion-lane-placeholder: reported [$out]"
  fi
  out=$("$BASH" "$body" "" 77)
  if [[ "$out" == "MATCH" ]]; then
    pass "conclusion-lane: an EMPTY stamped lane still concludes"
  else
    fail "conclusion-lane-empty: reported [$out]"
  fi
  # WIRED at BOTH accept points. The predicate exists to guard them; guarding one is half a fix.
  local guarded
  guarded=$(grep -cE 'conclusion_matches_stamped_lane' "$SUPERVISOR")
  if [[ "$guarded" -ge 3 ]]; then
    pass "conclusion-lane: the predicate guards both accept points ($guarded references incl. definition)"
  else
    fail "conclusion-lane-unwired: only $guarded reference(s) — definition plus BOTH accept points expected"
  fi
}

# ---------------------------------------------------------------------------
# Test 31-claim (#3393, roborev round 33 High, second half): the RETRACTED #1930 invariant in the
# OPERATIVE worker contract. `.claude/commands/worker.md` is what a `/worker` session actually obeys, so
# leaving "Exactly ONE flow-lead worker runs per machine" there means the second lane STOPS in preflight
# and every mechanism this change adds is unreachable by the documented invocation. Third instance of
# #3464's retracted-invariant-in-a-second-carrier family.
# ---------------------------------------------------------------------------
test_worker_contract_does_not_assert_one_worker_per_machine() {
  local doc="$REPO_ROOT/.claude/commands/worker.md" bad=""
  if [[ ! -r "$doc" ]]; then
    fail "worker-contract: $doc is not readable — the carrier this case exists for is missing"
    return 0
  fi
  # The retracted claim, in the spellings the file used. Comment lines are not a concern: this is
  # markdown, all of it operative.
  while IFS= read -r line; do bad="${bad}${line}\n"; done < <(
    grep -nE 'Exactly ONE flow-lead worker|One worker per machine — you are the sole' "$doc" |
      grep -v 'RETRACTED'
  )
  if [[ -z "$bad" ]]; then
    pass "worker-contract: worker.md no longer asserts the retracted one-worker-per-machine invariant"
  else
    fail "worker-contract: retracted #1930 invariant still live in worker.md:\n$(printf '%b' "$bad")"
  fi
  # The retraction must be POSITIVE, not just an absence — a silent deletion leaves a reader with no
  # statement either way, and #1930 is cited across the fleet docs.
  if grep -q 'RETRACTED by #3393' "$doc"; then
    pass "worker-contract: the retraction is stated explicitly, citing #3393"
  else
    fail "worker-contract: nothing in worker.md records that #1930 was retracted"
  fi
  # AND THE TRUE PARTS MUST SURVIVE. The retraction is scoped to the worker-COUNT invariant; the
  # full-gate concurrency bound is a RESOURCE bound and still holds, and dropping it with the
  # retraction would trade one wrong doc for another.
  if grep -qE 'full-gate concurrency = \*\*1\*\*|full-gate concurrency = 1' "$doc"; then
    pass "worker-contract: the surviving resource bound (full-gate concurrency = 1) is retained"
  else
    fail "worker-contract: the full-gate concurrency bound was dropped along with the retraction"
  fi
  # The actor requirement is the thing that makes multi-lane SAFE, so the contract must name it.
  if grep -q 'CLAIM_ACTOR' "$doc"; then
    pass "worker-contract: worker.md names the per-lane CLAIM_ACTOR requirement"
  else
    fail "worker-contract: multi-lane is now permitted but CLAIM_ACTOR is unmentioned"
  fi
}

t test_claim_actor_is_lane_unique
t test_conclusion_must_match_the_stamped_lane
t test_worker_contract_does_not_assert_one_worker_per_machine

# ---------------------------------------------------------------------------
# Test 32-claim (#3393, roborev round 34 finding 1): the legacy-claim migration. Every claim stamped
# before the lane-actor change carries `actor=flow`, so a lane that resolves a lane-unique actor reads
# its OWN claim as foreign and can neither verify nor non-forcibly release its lock. The migration
# CAS-adopts it — but ONLY on an affirmative reading, and ONLY for the issue this lane's own branch
# names, because on a four-lane box all four legacy claims are textually identical.
# ---------------------------------------------------------------------------
mig_case() {
  # mig_case <status-line-or-FAIL> <actor> <branch-issue> -> echoes the stub's recorded call log
  local status_line="$1" actor="$2" branch_issue="$3" d body repo
  d="$(new_case_dir)"
  repo="$d/lane"
  mkdir -p "$repo" "$d/bin"
  git -C "$repo" init -q 2>/dev/null
  git -C "$repo" checkout -q -b "$branch_issue" 2>/dev/null
  # A REAL COMMIT, because a real lane always has one. The first cut left the repo unborn, and with the
  # old `rev-parse --abbrev-ref HEAD` probe that made the happy path resolve NO branch — so it made no
  # call, and all NINE refusal cases below passed VACUOUSLY (they assert the absence of an adopt, and
  # nothing was called at all). The positive control is the only reason that was visible.
  git -C "$repo" -c user.email=t@t -c user.name=t commit -q --allow-empty -m init 2>/dev/null
  # The stub records every invocation and answers `status` with the staged line.
  cat >"$d/bin/claim.sh" <<STUB
#!/usr/bin/env bash
printf '%s\n' "\$*" >>"$d/calls.log"
if [ "\$1" = status ]; then
  [ "$status_line" = FAIL ] && exit 1
  printf '%s\n' "$status_line"
fi
exit 0
STUB
  chmod +x "$d/bin/claim.sh"
  : >"$d/calls.log"
  {
    printf '%s\n' '#!/usr/bin/env bash'
    printf '%s\n' 'log() { :; }'
    # The KNOBS the function depends on, extracted too (roborev round 35). Leaving them out silently
    # unset CLAIM_MIGRATION_RETRIES, the retry loop body never ran, and the happy path made no call —
    # a green-looking harness hiding a disabled subject. It also revealed the production hazard:
    # the knob is now validated as strictly positive.
    sed -n '/^CLAIM_MIGRATION_SETTLED=/p' "$SUPERVISOR"
    sed -n '/^CLAIM_MIGRATION_RETRIES=/p' "$SUPERVISOR"
    sed -n '/^supervisor_msg_token()/,/^}/p' "$SUPERVISOR"
    sed -n '/^supervisor_migrate_legacy_claim()/,/^}/p' "$SUPERVISOR"
    printf '%s\n' 'supervisor_migrate_legacy_claim'
  } >"$d/mig.sh"
  LOCK_CMD="bash $d/bin/claim.sh" CLAIM_ACTOR="$actor" CLAIM_MACHINE=boxA \
    LEGACY_CLAIM_ACTOR=flow REPO_ROOT="$repo" bash "$d/mig.sh" >/dev/null 2>&1
  cat "$d/calls.log"
}

test_legacy_claim_migration() {
  local sha40 out
  sha40="1111111111111111111111111111111111111111"
  # (a) HAPPY PATH: this machine, legacy actor => CAS-adopt on the exact sha.
  out=$(mig_case "CLAIM: STATUS issue=88 sha=$sha40 machine=boxA actor=flow" flow-9-lane issue-88-x)
  if printf '%s\n' "$out" | grep -q "^adopt 88 --expect $sha40"; then
    pass "legacy-migration: a pre-upgrade claim on THIS machine is CAS-adopted on its exact sha"
  else
    fail "legacy-migration: expected 'adopt 88 --expect $sha40', got:
$out"
  fi
  # (b) A DIFFERENT MACHINE is not ours to take.
  out=$(mig_case "CLAIM: STATUS issue=88 sha=$sha40 machine=boxZ actor=flow" flow-9-lane issue-88-x)
  if ! printf '%s\n' "$out" | grep -q '^adopt'; then
    pass "legacy-migration: a claim held by ANOTHER machine is never adopted"
  else
    fail "legacy-migration-foreign-machine: adopted anyway:
$out"
  fi
  # (c) An actor that is ALREADY lane-scoped needs no migration.
  out=$(mig_case "CLAIM: STATUS issue=88 sha=$sha40 machine=boxA actor=flow-7-other" flow-9-lane issue-88-x)
  if ! printf '%s\n' "$out" | grep -q '^adopt'; then
    pass "legacy-migration: a claim already under a lane actor is left alone (no cross-lane grab)"
  else
    fail "legacy-migration-other-lane: adopted a sibling lane's claim:
$out"
  fi
  # (d) AN UNREADABLE STATUS IS NOT A LICENCE (#3229's affirmative-measurement rule). A failed probe
  # must not reach the adopt; doing nothing costs a diagnosed refusal, guessing costs someone's lock.
  out=$(mig_case FAIL flow-9-lane issue-88-x)
  if ! printf '%s\n' "$out" | grep -q '^adopt'; then
    pass "legacy-migration: an UNREADABLE status does not reach the adopt (affirmative measurement)"
  else
    fail "legacy-migration-unreadable: adopted on a failed probe:
$out"
  fi
  # (e) A branch that names no issue must not even ASK — there is no candidate to migrate.
  out=$(mig_case "CLAIM: STATUS issue=88 sha=$sha40 machine=boxA actor=flow" flow-9-lane main)
  if [[ -z "$out" ]]; then
    pass "legacy-migration: a branch naming no issue makes no claim.sh call at all"
  else
    fail "legacy-migration-no-issue-branch: called claim.sh anyway:
$out"
  fi
  # (f) An OPERATOR-PINNED legacy actor is not ours to migrate away from.
  out=$(mig_case "CLAIM: STATUS issue=88 sha=$sha40 machine=boxA actor=flow" flow issue-88-x)
  if [[ -z "$out" ]]; then
    pass "legacy-migration: an operator-pinned actor=flow is left exactly as the operator set it"
  else
    fail "legacy-migration-pinned: touched a pinned actor:
$out"
  fi
  # (g) A MALFORMED sha cannot be a CAS lease. Adopting on a short sha would either fail or, worse,
  # be interpreted; neither belongs in a lock path.
  out=$(mig_case "CLAIM: STATUS issue=88 sha=1111 machine=boxA actor=flow" flow-9-lane issue-88-x)
  if ! printf '%s\n' "$out" | grep -q '^adopt'; then
    pass "legacy-migration: a malformed sha is not used as a CAS lease"
  else
    fail "legacy-migration-badsha: adopted on a non-40-hex sha:
$out"
  fi
  # (h) A SUBSTRING KEY IS NOT A KEY (#3464 family 6): `notmachine=boxA` must not satisfy `machine`.
  # Staged so the ONLY `machine=` token is a foreign one, with a decoy that ends in our machine name.
  out=$(mig_case "CLAIM: STATUS issue=88 sha=$sha40 notmachine=boxA machine=boxZ actor=flow" flow-9-lane issue-88-x)
  if ! printf '%s\n' "$out" | grep -q '^adopt'; then
    pass "legacy-migration: a decoy 'notmachine=' token does not satisfy the machine match"
  else
    fail "legacy-migration-substring: a decoy key satisfied the machine match:
$out"
  fi
  # (i) THE SEAM GENUINELY DISABLES. `LOCK_CMD=""` must make no call — the colonless default exists
  # precisely so an empty value is not silently replaced by the real network path.
  local d2 repo2
  d2="$(new_case_dir)"; repo2="$d2/lane"; mkdir -p "$repo2"
  git -C "$repo2" init -q 2>/dev/null; git -C "$repo2" checkout -q -b issue-88-x 2>/dev/null
  {
    printf '%s\n' '#!/usr/bin/env bash'
    printf '%s\n' 'log() { :; }'
    sed -n '/^supervisor_msg_token()/,/^}/p' "$SUPERVISOR"
    sed -n '/^supervisor_migrate_legacy_claim()/,/^}/p' "$SUPERVISOR"
    printf '%s\n' 'supervisor_migrate_legacy_claim; echo RETURNED'
  } >"$d2/mig.sh"
  local dis
  dis=$(LOCK_CMD="" CLAIM_ACTOR=flow-9-lane CLAIM_MACHINE=boxA LEGACY_CLAIM_ACTOR=flow \
    REPO_ROOT="$repo2" bash "$d2/mig.sh" 2>&1)
  if [[ "$dis" == "RETURNED" ]]; then
    pass "legacy-migration: LOCK_CMD='' disables the migration and returns cleanly"
  else
    fail "legacy-migration-seam: LOCK_CMD='' produced [$dis]"
  fi
  # WIRED: a migration nothing calls is #3464's check-whose-subject-never-ran.
  if grep -qE '^[[:space:]]*supervisor_migrate_legacy_claim$' "$SUPERVISOR"; then
    pass "legacy-migration: supervisor_migrate_legacy_claim is CALLED (not just defined)"
  else
    fail "legacy-migration-unwired: defined but never invoked"
  fi
}

t test_legacy_claim_migration

# ---------------------------------------------------------------------------
# Test 33-claim (#3393, roborev round 35 High): the worker orphan probe must be attributed to THIS
# LANE. Counting every matching worker on the box made each supervisor read its SIBLINGS' healthy
# workers as leftover debris and stop after LEFTOVER_HOLD_MAX polls — so per-lane claim refs would
# have shipped while multi-lane operation stayed serialized by a different machine-global mechanism.
# ---------------------------------------------------------------------------
test_worker_probe_is_lane_attributed() {
  local d filt lane sib a b c out
  d="$(new_case_dir)"
  lane="$d/lane"; sib="$d/sibling"
  mkdir -p "$lane/sub" "$sib"
  filt="$d/filt.sh"
  {
    printf '%s\n' '#!/usr/bin/env bash'
    printf '%s\n' 'REPO_ROOT="$1"'
    # The SHIPPED filter definition, evaluated with this REPO_ROOT — not a reimplementation of it.
    printf '%s\n' 'eval "$(sed -n "/^LANE_PID_FILTER=/p" "$2")"'
    printf '%s\n' 'eval "$LANE_PID_FILTER"'
  } >"$filt"
  # Ordinary processes, distinguished ONLY by their working directory. No fake `claude` argv is needed:
  # the property under test is the ATTRIBUTION half, and driving it with real cwds keeps the case
  # hermetic and free of any dependence on the machine's actual process table.
  ( cd "$lane" && exec sleep 30 ) & a=$!
  ( cd "$sib" && exec sleep 30 ) & b=$!
  ( cd "$lane/sub" && exec sleep 30 ) & c=$!
  sleep 1
  out=$(printf '%s\n%s\n%s\n' "$a" "$b" "$c" | bash "$filt" "$lane" "$SUPERVISOR")
  if printf '%s\n' "$out" | grep -qxF "$a" && printf '%s\n' "$out" | grep -qxF "$c" \
    && ! printf '%s\n' "$out" | grep -qxF "$b"; then
    pass "worker-probe: lane root and lane SUBDIR are attributed to the lane; a SIBLING lane's process is not"
  else
    fail "worker-probe-attribution: lane=[$a] sub=[$c] sibling=[$b] but filter returned:
$out"
  fi
  # NON-VACUITY, and it must be true of the broken code too: the SAME three pids, filtered for the
  # SIBLING's root, must return the sibling and neither lane pid. Without this, an always-empty filter
  # would satisfy the case above.
  out=$(printf '%s\n%s\n%s\n' "$a" "$b" "$c" | bash "$filt" "$sib" "$SUPERVISOR")
  if printf '%s\n' "$out" | grep -qxF "$b" && ! printf '%s\n' "$out" | grep -qxF "$a"; then
    pass "NON-VACUITY: the same pids filtered for the SIBLING root return the sibling — the filter discriminates rather than returning nothing"
  else
    fail "worker-probe-nonvacuity: filtering for the sibling root returned:
$out"
  fi
  # A pid whose cwd cannot be read is attributed to NOBODY (affirmative attribution). Driven with a
  # pid that does not exist, which is the same unreadable condition as a process exiting mid-probe.
  out=$(printf '%s\n' 999999 | bash "$filt" "$lane" "$SUPERVISOR")
  if [[ -z "$out" ]]; then
    pass "worker-probe: an unreadable cwd is attributed to nobody — a positive verdict needs a positive measurement"
  else
    fail "worker-probe-unreadable: a nonexistent pid was attributed: [$out]"
  fi
  kill "$a" "$b" "$c" 2>/dev/null
  wait "$a" "$b" "$c" 2>/dev/null
  # The BUILD family must stay machine-wide: one gate at a time per MACHINE is a resource bound that
  # survived #1930's retraction, so a sibling's cargo IS this lane's business. Asserted structurally,
  # because the distinction is the whole point of scoping only one family.
  if sed -n '/^if \[\[ -z "${PROC_PROBE_BUILD_CMD:-}"/,/^fi/p' "$SUPERVISOR" | grep -q 'LANE_PID_FILTER'; then
    fail "worker-probe-build-scoped: the BUILD probe was lane-scoped too — a sibling's gate is still this lane's business"
  else
    pass "worker-probe: the BUILD family is deliberately NOT lane-scoped (machine-wide gate serialization survives)"
  fi
  # LIST-FROM-COUNT-SET (roborev 1839/1821) must survive the change: both probes must apply the filter.
  local cnt lst
  cnt=$(sed -n '/^if \[\[ -z "${PROC_PROBE_WORKER_CMD:-}"/,/^fi/p' "$SUPERVISOR" | grep -c 'LANE_PID_FILTER')
  lst=$(sed -n '/^if \[\[ -z "${PROC_LIST_WORKER_CMD:-}"/,/^fi/p' "$SUPERVISOR" | grep -c 'LANE_PID_FILTER')
  if [[ "$cnt" -ge 1 && "$lst" -ge 1 ]]; then
    pass "worker-probe: COUNT and LIST both derive from the same lane filter — the named set cannot drift from the triggering set"
  else
    fail "worker-probe-drift: count=$cnt list=$lst references to LANE_PID_FILTER — the two sets can diverge"
  fi
}

# ---------------------------------------------------------------------------
# Test 34-claim (#3393, roborev round 35 Medium/Low): the migration must never leave the lane
# permanently foreign to its own lock, and must accept a SHA-256 lease.
# ---------------------------------------------------------------------------
mig2_case() {
  # mig2_case <fail-first-N> <sha> ; echoes the recorded claim.sh calls
  local failn="$1" sha="$2" d repo
  d="$(new_case_dir)"; repo="$d/lane"; mkdir -p "$repo" "$d/bin"
  git -C "$repo" init -q 2>/dev/null
  git -C "$repo" checkout -q -b issue-88-x 2>/dev/null
  git -C "$repo" -c user.email=t@t -c user.name=t commit -q --allow-empty -m init 2>/dev/null
  cat >"$d/bin/claim.sh" <<STUB
#!/usr/bin/env bash
printf '%s\n' "\$*" >>"$d/calls.log"
if [ "\$1" = status ]; then
  n=\$(grep -c '^status' "$d/calls.log")
  if [ "\$n" -le "$failn" ]; then exit 1; fi
  printf '%s\n' "CLAIM: STATUS issue=88 sha=$sha machine=boxA actor=flow"
fi
exit 0
STUB
  chmod +x "$d/bin/claim.sh"; : >"$d/calls.log"
  {
    printf '%s\n' '#!/usr/bin/env bash' 'log() { :; }'
    sed -n '/^CLAIM_MIGRATION_SETTLED=/p' "$SUPERVISOR"
    sed -n '/^CLAIM_MIGRATION_RETRIES=/p' "$SUPERVISOR"
    sed -n '/^supervisor_msg_token()/,/^}/p' "$SUPERVISOR"
    sed -n '/^supervisor_migrate_legacy_claim()/,/^}/p' "$SUPERVISOR"
    printf '%s\n' 'supervisor_migrate_legacy_claim'
  } >"$d/mig.sh"
  LOCK_CMD="bash $d/bin/claim.sh" CLAIM_ACTOR=flow-9-lane CLAIM_MACHINE=boxA \
    LEGACY_CLAIM_ACTOR=flow CLAIM_MIGRATION_RETRIES=3 REPO_ROOT="$repo" \
    bash "$d/mig.sh" >/dev/null 2>&1
  cat "$d/calls.log"
}

test_migration_retries_and_sha256() {
  local sha40 sha64 out
  sha40="1111111111111111111111111111111111111111"
  sha64="$(printf '2%.0s' $(seq 1 64))"
  # A BLIP: the first two status reads fail, the third succeeds -> the adopt still happens.
  out=$(mig2_case 2 "$sha40")
  if printf '%s\n' "$out" | grep -q "^adopt 88 --expect $sha40"; then
    pass "migration-retry: two failed status reads are retried and the adopt still happens (a blip does not strand the lane)"
  else
    fail "migration-retry: expected an adopt after retries, got:
$out"
  fi
  # ALL attempts fail -> NO adopt (never guess), and the bounded burst really did retry rather than
  # giving up after one read. The count is the evidence the retry loop ran.
  out=$(mig2_case 99 "$sha40")
  local tries
  tries=$(printf '%s\n' "$out" | grep -c '^status')
  if [[ "$tries" -eq 3 ]] && ! printf '%s\n' "$out" | grep -q '^adopt'; then
    pass "migration-retry: a total outage is retried CLAIM_MIGRATION_RETRIES=3 times and never adopts on a guess"
  else
    fail "migration-retry-exhausted: status attempts=$tries (expected 3), calls:
$out"
  fi
  # A 64-hex SHA-256 object id is a valid CAS lease.
  out=$(mig2_case 0 "$sha64")
  if printf '%s\n' "$out" | grep -q "^adopt 88 --expect $sha64"; then
    pass "migration-sha256: a 64-hex object id is accepted as a CAS lease (claim.sh imposes no length check of its own)"
  else
    fail "migration-sha256: a 64-hex sha was skipped, calls:
$out"
  fi
  # A 41-hex value is neither, and must still be refused — widening to 64 must not become "any length".
  out=$(mig2_case 0 "${sha40}1")
  if ! printf '%s\n' "$out" | grep -q '^adopt'; then
    pass "migration-sha256: 41 hex is still refused — the widening is 40-OR-64, not 'any length'"
  else
    fail "migration-sha256-any: a 41-char sha was accepted:
$out"
  fi
  # THE RE-ENTRY IS WIRED: an unsettled migration must be re-attempted from the main loop, or a
  # transient outage is still permanent for the run.
  if grep -cE '^[[:space:]]*supervisor_migrate_legacy_claim$' "$SUPERVISOR" | grep -qE '^[2-9]'; then
    pass "migration-retry: the migration is invoked from BOTH lock acquisition and the iteration loop"
  else
    fail "migration-retry-unwired: only one call site — an unsettled migration would never be retried"
  fi
}

t test_worker_probe_is_lane_attributed
t test_migration_retries_and_sha256

# ---------------------------------------------------------------------------
# Test 35-claim (#3393, roborev round 36 Medium): a p<pid> PLACEHOLDER cannot carry endgame
# protection past our own exit. `should-reap` permanently refuses placeholders (round 3), so keeping
# one for a pending auto-merge PR meant NOTHING could ever clear it — not the CI reaper, not a later
# merge of the very PR it protected. The protection must move to issue-numbered refs.
# ---------------------------------------------------------------------------
clearclaim_case() {
  # clearclaim_case <stamped-lane> <pending-list> <stamp-rc> -> echoes the recorded claim-cmd calls
  local stamped="$1" pending="$2" stamp_rc="$3" d
  d="$(new_case_dir)"; mkdir -p "$d/bin"
  cat >"$d/bin/hb.sh" <<STUB
#!/usr/bin/env bash
printf '%s\n' "\$*" >>"$d/calls.log"
[ "\$1" = stamp ] && exit $stamp_rc
exit 0
STUB
  chmod +x "$d/bin/hb.sh"; : >"$d/calls.log"
  {
    printf '%s\n' '#!/usr/bin/env bash' 'log() { :; }' 'claim_drain_pending_cleanup() { :; }'
    sed -n '/^clear_claim()/,/^}/p' "$SUPERVISOR"
    printf '%s\n' 'clear_claim 1'
  } >"$d/cc.sh"
  CLAIM_CMD="bash $d/bin/hb.sh" CLAIM_MACHINE=boxA CLAIM_STAMPED_ISSUE="$stamped" \
    CLAIM_STAMPED_SHA=deadbeef PENDING_PR_LIST="$pending" bash "$d/cc.sh" >/dev/null 2>&1
  cat "$d/calls.log"
}

test_placeholder_endgame_protection_transfers() {
  local out nl
  nl=$'\n'
  # (a) A PLACEHOLDER with a pending PR naming issue 88: stamp lane 88, THEN clear the placeholder.
  out=$(clearclaim_case "p1234-abc" "3467${nl:0:0}"$'\t'"88"$'\t'"1"$'\t'"1000$nl" 0)
  if printf '%s\n' "$out" | grep -q '^stamp 88' && printf '%s\n' "$out" | grep -q '^reap boxA p1234-abc'; then
    pass "placeholder-transfer: the pending endgame is re-stamped as lane 88 and the placeholder is then cleared"
  else
    fail "placeholder-transfer: expected 'stamp 88' then 'reap boxA p1234-abc', got:
$out"
  fi
  # (b) IF THE TRANSFER FAILS the placeholder must be KEPT — a stale ref beats an unprotected
  # endgame. Driven by a stamp that exits non-zero.
  out=$(clearclaim_case "p1234-abc" "3467"$'\t'"88"$'\t'"1"$'\t'"1000$nl" 1)
  if printf '%s\n' "$out" | grep -q '^stamp 88' && ! printf '%s\n' "$out" | grep -q '^reap'; then
    pass "placeholder-transfer: a FAILED stamp keeps the placeholder (all-or-nothing — a stale ref beats an unprotected endgame)"
  else
    fail "placeholder-transfer-failed-stamp: the placeholder was cleared anyway:
$out"
  fi
  # (c) A pending PR with NO recorded issue is UNTRANSFERABLE, so the placeholder is kept. This is the
  # case that must not silently clear: there is nothing for the reaper to evaluate.
  out=$(clearclaim_case "p1234-abc" "3467"$'\t'""$'\t'"1"$'\t'"1000$nl" 0)
  if ! printf '%s\n' "$out" | grep -q '^reap'; then
    pass "placeholder-transfer: a pending PR with no issue is untransferable and keeps the placeholder"
  else
    fail "placeholder-transfer-no-issue: cleared the placeholder with an untransferable endgame:
$out"
  fi
  # (d) AN ISSUE-NUMBERED lane with a pending PR is unchanged — it keeps, as #2499 ruling (b) requires,
  # and must NOT be re-stamped. Without this the fix could have widened into the case that was correct.
  out=$(clearclaim_case "88" "3467"$'\t'"88"$'\t'"1"$'\t'"1000$nl" 0)
  if ! printf '%s\n' "$out" | grep -qE '^(reap|stamp)'; then
    pass "placeholder-transfer: an ISSUE-numbered lane with a pending PR still just KEEPS (#2499 ruling (b) untouched)"
  else
    fail "placeholder-transfer-issue-lane: an issue lane was altered:
$out"
  fi
  # (e) NON-VACUITY: with NO pending PR at all, a placeholder is cleared with no stamping — so the
  # transfer above is attributable to the pending endgame rather than to placeholders always clearing.
  out=$(clearclaim_case "p1234-abc" "" 0)
  if printf '%s\n' "$out" | grep -q '^reap boxA p1234-abc' && ! printf '%s\n' "$out" | grep -q '^stamp'; then
    pass "NON-VACUITY: with no pending PR the placeholder clears WITHOUT any stamp — the transfer is caused by the endgame"
  else
    fail "placeholder-transfer-nonvacuity: got:
$out"
  fi
}

t test_placeholder_endgame_protection_transfers

# ---------------------------------------------------------------------------
# Test 36-claim (#3393, roborev round 36; lead ruling B + C, 2026-08-30): lane identity is GIVEN, and a
# fallback that cannot prove it landed in a lane REFUSES rather than degrades. The earlier cut of this
# case asserted a WARN; the ruling replaced warning with refusal, because a warning still starts four
# silently-degraded mechanisms.
#
# TWO REFUSALS AND THEY ARE INDEPENDENT — the case that proves it is MAIN-worktree + LANE_ID given:
# identity is then fine and attribution is still impossible, because an identity token is not a
# directory.
# ---------------------------------------------------------------------------
lane_identity_case() {
  # lane_identity_case <linked|main> [env...] -> echoes the FATAL token, or the accepted-identity line
  local kind="$1"; shift
  local d root
  d="$(new_case_dir)"
  if [[ "$kind" == linked ]]; then
    root="$d/lanewt"
    mkdir -p "$d/main"; git -C "$d/main" init -q
    git -C "$d/main" -c user.email=t@t -c user.name=t commit -q --allow-empty -m init 2>/dev/null
    git -C "$d/main" worktree add -q -b issue-88-x "$root" 2>/dev/null
    [[ -e "$root/.git" ]] || { skip "lane-identity: host would not create a linked worktree — premise unstageable"; return 1; }
  else
    root="$d/mainwt"
    mkdir -p "$root"; git -C "$root" init -q
    git -C "$root" -c user.email=t@t -c user.name=t commit -q --allow-empty -m init 2>/dev/null
  fi
  mkdir -p "$root/scripts/local" "$root/scripts/lib"
  cp "$SUPERVISOR" "$root/scripts/local/worker-supervisor.sh"
  # scripts/lib is needed by the default notify path (learned the hard way — an incomplete scratch tree
  # produced an unattributable failure).
  cp "$REPO_ROOT/scripts/lib/gate-notify.sh" "$root/scripts/lib/" 2>/dev/null || true
  # A FATAL WINS OVER THE IDENTITY LINE, because identity is resolved and LOGGED first and the
  # attribution refusal fires after it. Taking `head -1` across both alternatives returned the
  # identity line and hid the refusal — the case reported "started fine" for a run that refused.
  local raw
  # `PROC_PROBE_WORKER_CMD=` FIRST, and this is the whole case. `common_env` EXPORTS that variable to
  # stub the probe, so it leaks into every later case — and the attribution refusal deliberately yields
  # to an operator who set it. The refusal was therefore ALWAYS yielding to a phantom override, and the
  # two cases below both passed for the same wrong reason. Cleared here; "$@" comes after, so a case
  # that genuinely wants the override still gets it (later `env` assignments win).
  raw=$(env PROC_PROBE_WORKER_CMD= "$@" NOTIFY_CMD=true STOP_FILE=/nonexistent LOCK_CMD="" CLAIM_CMD="" MAX_ISSUES=1 \
    timeout 30 bash "$root/scripts/local/worker-supervisor.sh" 2>&1)
  if printf '%s\n' "$raw" | grep -oE 'FATAL: lane-[a-z-]+' | head -1 | grep .; then
    return 0
  fi
  printf '%s\n' "$raw" | grep -oE 'lane identity given explicitly|LANE_ID unset; derived' | head -1
  return 0
}

test_lane_identity_is_given_or_refused() {
  local out
  # (a) a LANE worktree with LANE_ID unset: the fallback may derive, because it can PROVE it is a lane.
  out=$(lane_identity_case linked X=1) || return 0
  if [[ "$out" == "LANE_ID unset; derived" ]]; then
    pass "lane-identity: in a LANE worktree the fallback derives an identity (it can prove where it is)"
  else
    fail "lane-identity(linked-fallback): got [$out]"
  fi
  # (b) MAIN worktree, LANE_ID unset -> lane-identity-unprovable. Nothing to derive FROM.
  out=$(lane_identity_case main X=1) || return 0
  if [[ "$out" == "FATAL: lane-identity-unprovable" ]]; then
    pass "lane-identity: MAIN worktree + LANE_ID unset REFUSES (lane-identity-unprovable), rather than sharing one identity across lanes"
  else
    fail "lane-identity(main-unprovable): got [$out]"
  fi
  # (c) THE CASE THAT PROVES THE TWO REFUSALS ARE INDEPENDENT: MAIN worktree + LANE_ID GIVEN. Identity
  # is satisfied; attribution is still impossible, because an identity token is not a directory.
  out=$(lane_identity_case main LANE_ID=explicit-lane-x) || return 0
  if [[ "$out" == "FATAL: lane-attribution-impossible" ]]; then
    pass "lane-identity: LANE_ID satisfies IDENTITY but not ATTRIBUTION — the second refusal is independent (an identity token is not a directory)"
  else
    fail "lane-identity(main-attribution): got [$out] — expected the attribution refusal, since LANE_ID cannot supply a directory"
  fi
  # (d) an operator who overrode the probe has taken responsibility, so it starts.
  out=$(lane_identity_case main LANE_ID=explicit-lane-x PROC_PROBE_WORKER_CMD="echo 0") || return 0
  if [[ "$out" == "lane identity given explicitly" ]]; then
    pass "lane-identity: an explicit PROC_PROBE_WORKER_CMD yields the attribution refusal to the operator"
  else
    fail "lane-identity(probe-override): got [$out]"
  fi
  # (e) a LANE_ID that claim.sh would refuse is refused HERE, loudly, rather than failing every claim.
  out=$(lane_identity_case linked LANE_ID=ab) || return 0
  if [[ "$out" == "FATAL: lane-identity-unusable" ]]; then
    pass "lane-identity: a LANE_ID under 3 recordable chars is refused at startup (claim.sh would reject the actor on every call)"
  else
    fail "lane-identity(short): got [$out]"
  fi
  # NO LAYOUT HEURISTIC: the worktrees above are named `lanewt`/`mainwt`, matching no fleet convention,
  # and the implementation must contain no such pattern — that assumption is what made AC3 unimplementable.
  if sed -n '/^lane_worktree_ok()/,/^}/p' "$SUPERVISOR" | grep -qiE '/data/lanes|lane-\[0-9\]'; then
    fail "lane-identity-heuristic: lane_worktree_ok references a lane-directory naming convention"
  else
    pass "lane-identity: the proof is structural (git worktree), assuming NO directory naming convention"
  fi
}

t test_lane_identity_is_given_or_refused

# ---------------------------------------------------------------------------
# Test 37-claim (#3393, roborev round 36 row 4; lead condition 1): the worker-orphan probe needs a
# TWO-DIRECTION control, not a passing test. A probe whose subject set can be EMPTY passes vacuously
# when it is — the same shape as `--delta-classify`'s ALLOW on an empty subject set (#3480). So:
#   POSITIVE: a leftover IS in this lane  -> counted  (would STOP)
#   NEGATIVE: no leftover in this lane    -> zero     (would NOT stop)
# and the OLD machine-wide probe counted the sibling too, which is the false STOP being fixed.
#
# The marker must be IN THE ARGV, which took three wrong attempts worth recording: a `# comment` is
# stripped by bash before exec so it never reaches /proc/<pid>/cmdline; a pattern containing regex
# metacharacters MATCHES ITS OWN TEXT in the probe subshell (the bracket trick only defeats a LITERAL
# self-match, and the real probe's $$/$PPID exclusion is load-bearing); and `exec sleep` replaces the
# process image, discarding the marker. Hence: a marker-named SCRIPT that does not exec.
# ---------------------------------------------------------------------------
test_worker_probe_two_direction_control() {
  local d lane sib marker script match probe neg pos machine_wide
  d="$(new_case_dir)"
  lane="$d/lane"; sib="$d/sibling"
  mkdir -p "$lane" "$sib"
  marker="probe$$x$RANDOM"
  script="$d/${marker}-worker.sh"
  printf '%s\n%s\n' '#!/usr/bin/env bash' 'sleep 120' >"$script"
  chmod +x "$script"
  # LITERAL match only, plus the real probe's self-exclusion — both for the reasons in the header.
  match="[p]${marker#p}-worker"
  # REPO_ROOT MUST BE SET BEFORE THE EVAL. `LANE_PID_FILTER`'s literal contains `'$REPO_ROOT'`, which
  # expands AT EVAL TIME — so eval'ing it first and passing REPO_ROOT to the probe later bakes in the
  # TEST's own lane and the case measures the wrong directory. That is how the first cut of this case
  # reported POSITIVE=0 while the machine-wide count was 2: the marker matched, the attribution did not.
  # REPO_ROOT MUST BE SET IN *THIS* SHELL BEFORE THE EVAL. `LANE_PID_FILTER`'s literal contains
  # `'$REPO_ROOT'`, and `eval` expands it in the shell that RUNS the eval — so wrapping the command
  # substitution in a subshell that sets REPO_ROOT does nothing at all (my first fix was a no-op, and
  # the case still measured the test's own lane). A function-local shadow is what actually applies.
  local LANE_PID_FILTER REPO_ROOT="$lane"
  eval "$(sed -n '/^LANE_PID_FILTER=/p' "$SUPERVISOR")"
  probe="pgrep -f '$match' 2>/dev/null | grep -vxF -e \$\$ -e \$PPID | $LANE_PID_FILTER | wc -l | tr -d ' '" # pgrep-lint-allow: run-unique marker scoping
  # NEGATIVE first, before anything is spawned: if this is not 0, the harness is matching itself and
  # every later number is meaningless.
  neg=$(bash -c "$probe")
  # PRE-DATES #3549 AND LEAKED THE SAME WAY (roborev job 196 F2, class sweep): `$script`'s body is a
  # `sleep 120` that is NOT an exec replacement, and the `pkill -f <marker>` below matches the SHELL's
  # argv only — the child's argv is a bare `sleep 120`, so two of them survived every run of this suite
  # for two minutes. Group-launched and group-killed, the child goes with its parent.
  fixture_bg bash -c 'cd "$1" && exec bash "$2"' _ "$lane" "$script" >/dev/null 2>&1
  local pid_lane=$FIXTURE_LAST_PID
  fixture_bg bash -c 'cd "$1" && exec bash "$2"' _ "$sib" "$script" >/dev/null 2>&1
  local pid_sib=$FIXTURE_LAST_PID
  sleep 1
  pos=$(bash -c "$probe")
  machine_wide=$(bash -c "pgrep -f '$match' 2>/dev/null | grep -vxF -e \$\$ -e \$PPID | wc -l | tr -d ' '") # pgrep-lint-allow: run-unique marker scoping
  fixture_kill "$pid_lane" "$pid_sib"
  if [[ "$neg" == "0" ]]; then
    pass "probe-two-direction NEGATIVE: no leftover in this lane counts 0 (so the probe does not fire unconditionally)"
  else
    fail "probe-two-direction: NEGATIVE control counted $neg before anything was spawned — the harness is matching itself, so no later number means anything"
  fi
  if [[ "$pos" == "1" ]]; then
    pass "probe-two-direction POSITIVE: a leftover IN this lane counts 1 (so the probe DOES fire, and the negative above is a measurement)"
  else
    fail "probe-two-direction: POSITIVE control counted $pos (expected 1) — a probe that cannot count its subject passes vacuously"
  fi
  if [[ "$machine_wide" == "2" ]]; then
    pass "probe-two-direction: the OLD machine-wide probe counts 2 (lane + sibling) — the false STOP this fixes, measured rather than asserted"
  else
    fail "probe-two-direction: machine-wide counted $machine_wide (expected 2); the comparison that motivates lane scoping is not established"
  fi
}

t test_worker_probe_two_direction_control
# ---------------------------------------------------------------------------
# Tests 38..42-lock (#3549): PRE-#3467 LEGACY GLOBAL LOCK COMPATIBILITY.
#
# #3467 moved the derived default lock from ONE MACHINE-GLOBAL path to a PER-LANE one. The per-lane
# path is the correct end state (#3393), but nothing consulted the old path — so a supervisor from a
# pre-#3467 checkout holds a lock the new one never looks at and BOTH run in one worktree, sharing
# markers, branch, logs and `.worker-last-iteration.json`.
#
# THE CHECK RUNS ON EVERY START, WITH NO OPT-OUT (lead ruling 2026-08-30, AC4 removed as unsound), so
# what keeps the OTHER cases in this file out of it is not an exemption but a private `TMPDIR`: the
# suite exports one at load time (see beside `TMP_ROOT`), so the machine-global path the guard tests is
# inside this run's scratch tree and definitively absent. The cases below scope `TMPDIR` to their own
# case directory in addition, because they PLANT a legacy lock at that path and must never plant one
# where a sibling case would find it.
#
# THE PIDS ARE REAL: a genuinely running child, never a staged number, wherever a case needs a
# process to exist. The guard itself no longer reads any pid (the classifier is deleted).
# ---------------------------------------------------------------------------

# legacy_lock_drive <tmpdir> <lane-id> [explicit-lock] — run the REAL `acquire_lock`, read out of the
# shipped supervisor at run time (sourced, so the guard under test is the shipped code and never a
# re-implementation). Echoes stdout+stderr; the caller reads `$?`.
#
# `LOCK_CMD=""`/`CLAIM_CMD=""`: with `SUPERVISOR_LOCK` unset the run also does lane-identity
# resolution, claim-actor derivation and `supervisor_migrate_legacy_claim`, the last of which can fire
# a network `claim.sh status`. Empty disables both seams (the colonless `${VAR-default}` form in the
# supervisor preserves an explicitly-empty override), so these cases stay hermetic.
# ONE drive body, shared by both drivers below, so a case that varies the WORKING DIRECTORY cannot
# drift into exercising a different startup path than the ordinary case does.
SV_DRIVE_BODY='source "$1"; acquire_lock; printf "ACQUIRED=%s\n" "$SUPERVISOR_LOCK"; [[ -d "$SUPERVISOR_LOCK" ]] && printf "LOCKDIR=yes\n"; exit 0'

legacy_lock_drive() {
  local tmp="$1" lane="$2" explicit="${3:-}"
  if [[ -n "$explicit" ]]; then
    env TMPDIR="$tmp" SUPERVISOR_LOCK="$explicit" LANE_ID="$lane" LOCK_CMD="" CLAIM_CMD="" \
      bash -c "$SV_DRIVE_BODY" _ "$SUPERVISOR" 2>&1
  else
    env -u SUPERVISOR_LOCK TMPDIR="$tmp" LANE_ID="$lane" LOCK_CMD="" CLAIM_CMD="" \
      bash -c "$SV_DRIVE_BODY" _ "$SUPERVISOR" 2>&1
  fi
}

# ONE FUNCTION REPLACED, THE REST SHIPPED: `SV_DRIVE_BODY_OVERRIDE` is DERIVED from `SV_DRIVE_BODY` by
# inserting a second `source`, so a mutant case cannot drift into a different startup path than the
# ordinary case exercises. The override file holds exactly the pre-fix spelling of one function.
SV_DRIVE_BODY_OVERRIDE="${SV_DRIVE_BODY/'source "$1"; '/'source "$1"; source "$2"; '}"

# SV_MAIN_DRIVE_BODY — the WHOLE supervisor loop, not just `acquire_lock`, for the cases whose subject is
# inside `run_iteration`. ONE body used by both a control and its mutant, differing only in argument 2
# (the override file; empty for the control), because a contrast whose two sides differ in their ENTRY
# POINT does not isolate the property under test (#3601, roborev job 231 B4). No `|| true` and no
# `2>/dev/null` on the source: swallowing a startup failure would let a broken mutant read as a result.
# `main` is called explicitly because the supervisor's own guard runs it only when executed directly.
SV_MAIN_DRIVE_BODY='source "$1"; [[ -z "${2:-}" ]] || source "$2"; main'


# legacy_lock_drive_override <override-file> <tmp> <lane> — the ordinary drive with one shipped function
# replaced by the file's redefinition (sourced AFTER the supervisor).
legacy_lock_drive_override() {
  local override="$1" tmp="$2" lane="$3"
  env -u SUPERVISOR_LOCK TMPDIR="$tmp" LANE_ID="$lane" LOCK_CMD="" CLAIM_CMD="" \
    bash -c "$SV_DRIVE_BODY_OVERRIDE" _ "$SUPERVISOR" "$override" 2>&1
}

# legacy_lock_drive_env <tmp> <lane> [VAR=VALUE ...] — the ordinary drive with EXTRA INHERITED
# ENVIRONMENT (#3549, roborev job 205). Derived from the same `SV_DRIVE_BODY`, so a case that varies the
# inherited state cannot drift into exercising a different startup path than the ordinary case does.
#
# THE STATE IS SET HERE, IN THE DRIVER, AND NEVER IN THE SHIPPED SCRIPT. That is the whole point of an
# inherited-state case: the subject is what the shipped code does with state IT DID NOT CHOOSE, so a
# knob in production code would test the knob instead. `GLOBIGNORE`, `LC_ALL`, and — measured —
# `SHELLOPTS`/`BASHOPTS` are all imported by bash from the environment, so `env` is sufficient to put the
# shell into the state under test with no cooperation from the script.
legacy_lock_drive_env() {
  local tmp="$1" lane="$2"; shift 2
  env -u SUPERVISOR_LOCK TMPDIR="$tmp" LANE_ID="$lane" LOCK_CMD="" CLAIM_CMD="" "$@" \
    bash -c "$SV_DRIVE_BODY" _ "$SUPERVISOR" 2>&1
}

# legacy_lock_drive_env_override <override-file> <tmp> <lane> [VAR=VALUE ...] — the mutant drive with the
# same extra inherited environment, so a contrast varies ONE function and nothing else.
legacy_lock_drive_env_override() {
  local override="$1" tmp="$2" lane="$3"; shift 3
  env -u SUPERVISOR_LOCK TMPDIR="$tmp" LANE_ID="$lane" LOCK_CMD="" CLAIM_CMD="" "$@" \
    bash -c "$SV_DRIVE_BODY_OVERRIDE" _ "$SUPERVISOR" "$override" 2>&1
}





# legacy_lock_drive_in <cwd> <tmp> <lane> — the same drive from a chosen working directory, which is
# what makes a RELATIVE (and therefore possibly OPTION-SHAPED) `TMPDIR` testable at all.
legacy_lock_drive_in() {
  local cwd="$1" tmp="$2" lane="$3"
  ( cd "$cwd" && env -u SUPERVISOR_LOCK TMPDIR="$tmp" LANE_ID="$lane" LOCK_CMD="" CLAIM_CMD="" \
      bash -c "$SV_DRIVE_BODY" _ "$SUPERVISOR" 2>&1 )
}

# THE SUPERVISOR HAS TWO OUTPUT CHANNELS AND BOTH LABEL THEMSELVES: `log()` writes
# `[worker-supervisor] ...` and this guard's diagnostics write `worker-supervisor: ...`. A runnable
# command line carries NEITHER, which is what makes it identifiable without parsing prose. `SV_DIAG_RE`
# is that pair, in one place, so a case cannot accidentally test only one of them.
SV_DIAG_RE='^(\[worker-supervisor\]|worker-supervisor:) '
# A literal newline as a VALUE, for the cases that stage a `TMPDIR` containing one.
SV_LF='
'
# ...and a carriage return, for the cases that assert NO RAW CR reaches emitted text (#3549, job 201
# F1). A CR adds no LINE, so a bare-line count cannot see it: it returns the cursor to column 0 and the
# text after it overwrites the `worker-supervisor:` prefix, forging the same unprefixed line by another
# mechanism. It is therefore asserted as a BYTE.
SV_CR=$'\r'

# sv_q <string> — render a value for a DIAGNOSTIC so it stays on ONE PHYSICAL LINE, without `${var@Q}`
# (#3549, roborev job 217 F2). `@Q` is BASH 4.4+, and this suite deliberately supports the bash 3.2 macOS
# ships (the shipped script's `read -d ''` and `%q` notes say why). Worse than an unavailable feature: an
# unsupported parameter transformation is a PARSE error, so it fails the WHOLE FILE at load time — every
# case, including the file's own pre-4.4 `skip` handling, which never gets the chance to run. `printf -v`
# and `%q` both exist in 3.2, so the rendering is done with those.
#
# %q's treatment of control characters HAS CHANGED ACROSS BASH VERSIONS, so this does not trust it: any
# real newline or carriage return that survives is folded into a visible two-character escape. The
# guarantee owed to a diagnostic is one physical line — a fragment on a line of its own is
# indistinguishable from the harness's own output — not a byte-exact requoting.
sv_q() {
  local raw="$1" out=""
  printf -v out '%q' "$raw" 2>/dev/null || out="$raw"
  out="${out//"$SV_LF"/\\n}"
  out="${out//"$SV_CR"/\\r}"
  printf '%s' "$out"
}
# A COMMAND SIGNATURE IS A VERB PLUS AN OPERAND, not a bare mention. The prose legitimately NAMES the
# tools ("the rmdir is non-recursive"), and flagging that would make the sweep red on correct text — a
# check that reds on correct input is the check people learn to waive. So the patterns require the
# argument shape a paste would actually mangle.
SV_CMD_RE="rm -f |rm -rf |rmdir ['\"/]|rmdir -- |ps -p |kill -"

# remedy_lines_structural <label> <out> <expected-bare-lines> — the (a) half of Test 44-lock (see its
# comment block below) plus the bare-line count. IT LIVES HERE, WITH `SV_DIAG_RE`/`SV_CMD_RE`, AND NOT
# BESIDE THE CASES THAT CALL IT (#3549, roborev job 220): it is called from five cases spread across two
# `t` blocks, and while it sat between them the earlier block called a function that did not exist yet.
# Bash resolves a name at CALL time, so that call was `command not found`, its status was discarded (no
# `errexit`), `FAIL_COUNT` never moved, and the suite reported success with the assertion never run —
# the very defect `t()` was built for, reaching past `t()` because the callee is a helper, not a test.
remedy_lines_structural() {
  local label="$1" out="$2" want="$3" bare_count inlined
  bare_count="$(printf '%s\n' "$out" | grep -cvE "$SV_DIAG_RE" || true)"
  # A COUNT THAT IS NOT A NUMBER IS NOT A ZERO. An unset `SV_DIAG_RE` (these are top-level assignments,
  # and a `t` invocation placed above them would run first) makes the substitution come back EMPTY, and
  # `[[ "" == "0" ]]` is false — so the failure is at least visible — but the message would report a
  # count nobody measured. Named explicitly instead; this is the same rule the code under test follows.
  if [[ ! "$bare_count" =~ ^[0-9]+$ ]]; then
    fail "remedy-lines-count-unmeasurable ($label): the bare-line count came back [$bare_count]; the measurement did not happen"
    return 0
  fi
  if [[ "$bare_count" == "$want" ]]; then
    pass "remedy-lines ($label): exactly $want bare (unprefixed) line(s) — the runnable command, if any, is a line of its own"
  else
    fail "remedy-lines-bare-count ($label): $bare_count bare lines, expected $want; out=[$out]"
  fi
  # A command SIGNATURE on a prefixed line is the inlined-in-prose defect, whatever the prose says
  # around it: a line that mixes the two cannot be pasted, wherever in the line the command sits.
  inlined="$(printf '%s\n' "$out" | grep -E "$SV_DIAG_RE" | grep -nE "$SV_CMD_RE" || true)"
  if [[ -z "$inlined" ]]; then
    pass "remedy-lines ($label): no diagnostic line inlines a runnable command in prose"
  else
    fail "remedy-lines-inlined ($label): prose lines carrying a command: [$inlined]"
  fi
}



# The refusal must be the LEGACY one, not the per-lane "another instance is already running" — an
# operator and a test both have to be able to tell the two locks apart.
legacy_refusal_ok() {
  local out="$1"
  [[ "$out" == *"LEGACY GLOBAL supervisor lock"* ]] \
    && [[ "$out" == *"#3549"* ]] \
    && [[ "$out" != *"another instance is already running"* ]]
}


# ---------------------------------------------------------------------------
# Scratch drivers and mutants (#3549, roborev job 201) — TWO helpers, because both mechanisms had
# already drifted once each.
# ---------------------------------------------------------------------------

# sv_scratch_head — the head of a scratch driver that needs the SHIPPED classifier: it SOURCES THE WHOLE
# supervisor instead of extracting the two functions it happened to need.
#
# WHY, MEASURED: four cases built such a copy by `sed`-extracting `supervisor_pid_liveness` and
# `supervisor_legacy_lock_state`, which encodes a dependency list that is invisible at every one of the
# four sites. Job 201 F1 gave the classifier a THIRD dependency (`supervisor_shell_quote`, plus the
# `SUPERVISOR_LF`/`CR`/`ESC` values it reads) and all four copies broke at once with
# `supervisor_shell_quote: command not found` INSIDE a captured string — i.e. the failure surfaced as a
# nonsense state value, not as a missing symbol. Sourcing the file cannot go stale: a new dependency
# comes along with it. The supervisor defines its functions and never starts its loop when sourced
# (`BASH_SOURCE[0]` != `$0`), which is what every other drive in this file already relies on.
#
# THE OPTIONS ARE THE EXTRACTION-ERA ONES, DELIBERATELY: sourcing brings the supervisor's own
# `set -euo pipefail`, and these drivers run probes whose non-zero exit is NORMAL (`shopt -p` for a
# disabled option). Errexit EXPOSURE is measured by the dedicated `inherit_errexit` case below, where it
# is the subject; it must not arrive by accident in every scratch driver and change what they measure.
sv_scratch_head() {
  printf '%s\n' '#!/usr/bin/env bash'
  printf '%s\n' "source \"$SUPERVISOR\""
  printf '%s\n' 'set +e; set -uo pipefail'
}

# sv_mutant_override <outfile> <fn> <from> <to> [expected-hits] — APPEND to <outfile> the SHIPPED text of
# <fn> with ONE literal substitution applied, so a mutant is DERIVED from the shipped code and can never
# be a re-implementation that drifts. Sourced after the supervisor, the copy redefines the function.
#
# Every premise is checked and NAMED, because a mutant that silently failed to mutate produces a PASS
# from the shipped code and a contrast that measured nothing: the function text must be extractable, the
# `from` string must occur the expected number of times (default 1 — an unexpected count means the source
# moved), the substitution must actually change something, and the result must parse.
sv_mutant_override() {
  local out="$1" fn="$2" from="$3" to="$4" want="${5:-1}" body hits mutated
  body="$(sed -n "/^$fn()/,/^}/p" "$SUPERVISOR")"
  if [[ -z "$body" || "$body" != *"$fn() {"* ]]; then
    fail "mutant-premise ($fn): the shipped function text could not be extracted from $SUPERVISOR; the contrast would measure nothing"
    return 1
  fi
  hits="$(printf '%s\n' "$body" | grep -cF -- "$from" || true)"
  if [[ ! "$hits" =~ ^[0-9]+$ ]] || [[ "$hits" != "$want" ]]; then
    fail "mutant-premise ($fn): [$from] occurs [$hits] time(s) in the shipped function, expected $want — the source moved and the mutant below would not be the pre-fix form"
    return 1
  fi
  mutated="${body//"$from"/"$to"}"
  if [[ "$mutated" == "$body" ]]; then
    fail "mutant-premise ($fn): the substitution [$from]->[$to] changed nothing"
    return 1
  fi
  printf '%s\n' "$mutated" >>"$out"
  if ! bash -n "$out" 2>/dev/null; then
    fail "mutant-premise ($fn): the mutant override does not parse"
    return 1
  fi
  return 0
}



# THE ERREXIT DRIVES (#3549, roborev job 201 F3). `inherit_errexit` is what makes a caller's `set -e`
# reach INSIDE the `$( )` the guard wraps the classifier in — without it bash does not propagate errexit
# into a command substitution, which is the only reason a `shopt -p` on a disabled option (non-zero in
# the COMMON path) never showed. Both bodies are DERIVED from the shipped drive bodies by inserting one
# statement AFTER the source, so an errexit case cannot drift into exercising a different startup path,
# and the option is set after sourcing so a source-time substitution cannot abort instead.
SV_DRIVE_BODY_ERREXIT="${SV_DRIVE_BODY/'source "$1"; '/'source "$1"; set -e; shopt -s inherit_errexit 2>/dev/null || { printf "ERREXIT_UNAVAILABLE\n"; exit 97; }; '}"
SV_DRIVE_BODY_ERREXIT_OVERRIDE="${SV_DRIVE_BODY_OVERRIDE/'source "$2"; '/'source "$2"; set -e; shopt -s inherit_errexit 2>/dev/null || { printf "ERREXIT_UNAVAILABLE\n"; exit 97; }; '}"

legacy_lock_drive_errexit() {
  local tmp="$1" lane="$2"
  env -u SUPERVISOR_LOCK TMPDIR="$tmp" LANE_ID="$lane" LOCK_CMD="" CLAIM_CMD="" \
    bash -c "$SV_DRIVE_BODY_ERREXIT" _ "$SUPERVISOR" 2>&1
}

legacy_lock_drive_errexit_override() {
  local override="$1" tmp="$2" lane="$3"
  env -u SUPERVISOR_LOCK TMPDIR="$tmp" LANE_ID="$lane" LOCK_CMD="" CLAIM_CMD="" \
    bash -c "$SV_DRIVE_BODY_ERREXIT_OVERRIDE" _ "$SUPERVISOR" "$override" 2>&1
}

# legacy_presence_drive_errexit <legacy-path> [override-file] — call the PROBE from a shell with
# `set -e` AND `inherit_errexit`, through a BARE assignment with no `||` after it. That caller shape is
# the one that EXPOSES an abort: bash suppresses errexit for a command that is part of an `&&`/`||`
# list, and that suppression is inherited by the substitution — so the guard's own fail-closed
# `… || state=…` fallback makes the probe's internals unobservable from the guard. Reading the probe
# directly is therefore the only place a non-aborting internal can be measured.
# It also reports the caller's `dotglob`/`nullglob` AFTER the call: the DELETED classifier pinned and
# restored them, and the probe must not touch them at all.
legacy_presence_drive_errexit() {
  local legacy="$1" override="${2:-}"
  bash -c '
    source "$1"
    [[ -z "$3" ]] || source "$3"
    set -e
    shopt -s inherit_errexit 2>/dev/null || { printf "ERREXIT_UNAVAILABLE\n"; exit 97; }
    st="$(supervisor_legacy_lock_presence "$2")"
    printf "STATE=[%s]\n" "$st"
    printf "OPTS=[%s|%s]\n" "$(shopt -p dotglob || true)" "$(shopt -p nullglob || true)"
  ' _ "$SUPERVISOR" "$legacy" "$override" 2>&1
}

# ---------------------------------------------------------------------------
# (#3549, lead ruling) PRESENCE REFUSES, AND THE REFUSAL DOES NOT DEPEND ON WHAT IS THERE.
#
# The classifier that used to read the lock is DELETED, so the property under test changed shape: it is
# no longer "each state gets its own cause and its own remedy" but "EVERY object at that path produces
# the SAME refusal". That is the assertion the deletion is worth: a wording that varied with the lock's
# contents was the only consumer of the parsing, and any variation reappearing here means the parsing
# came back. So the shape list below is no longer a cause-token table — it is a UNIFORMITY table.
# ---------------------------------------------------------------------------
test_legacy_global_lock_refuses_a_present_lock() {
  local d tmp lane legacy live out rc control crc derived shape dead first_detail detail
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"; lane="lane3549present$$"
  mkdir -p "$tmp"
  legacy="$tmp/cqlite-worker-supervisor.lock"
  derived="$tmp/cqlite-worker-supervisor-$lane.lock"

  # NON-VACUITY 1: the collision this guard prevents is REAL — the derived per-lane path and the
  # legacy global path genuinely DIFFER for this LANE_ID, so a refusal cannot be coming from the
  # per-lane lock. (This is the whole reason the old lock is invisible to the new one.)
  if [[ "$derived" != "$legacy" ]]; then
    pass "legacy-lock NON-VACUITY: the derived per-lane path differs from the legacy global path (the collision is real, and no refusal below can come from the per-lane lock)"
  else
    fail "legacy-lock-nonvacuity: derived [$derived] == legacy [$legacy]; the two paths must differ or these cases measure nothing"
  fi

  # NON-VACUITY 2 (two-direction control): the SAME harness with NO legacy lock present must ACQUIRE.
  # Without this, a refusal could be any earlier failure — an unresolvable lane identity, a missing
  # stub — wearing the guard's clothes.
  control="$(legacy_lock_drive "$tmp" "$lane")"; crc=$?
  if [[ "$crc" -eq 0 && "$control" == *"LOCKDIR=yes"* && "$control" == *"ACQUIRED=$derived"* ]]; then
    pass "legacy-lock NON-VACUITY: with NO legacy lock the same harness ACQUIRES the per-lane lock (so it reaches the guard, and a refusal below is attributable to the guard)"
  else
    fail "legacy-lock-nonvacuity-control: rc=$crc out=[$control] — the harness must succeed when no legacy lock exists, or nothing below is attributable"
  fi
  rm -rf "$derived"

  # AC1, with a REAL live holder pid — the strongest form of "something is there".
  fixture_bg sleep 300
  live=$FIXTURE_LAST_PID
  mkdir -p "$legacy"
  printf '%s\n' "$live" >"$legacy/pid"
  out="$(legacy_lock_drive "$tmp" "$lane")"; rc=$?
  fixture_kill "$live"

  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" && [[ "$out" == *"EXISTS there"* ]]; then
    pass "legacy-lock AC1: a legacy lock left by a pre-#3467 supervisor (real live pid $live) refuses the start, loudly, naming the legacy lock — and NOT with the per-lane message"
  else
    fail "legacy-lock-present: rc=$rc (expected non-zero) out=[$out] — expected the LEGACY GLOBAL refusal saying a path EXISTS there"
  fi

  # THE REFUSAL CLAIMS NOTHING ABOUT THE HOLDER (lead ruling). Every one of these tokens was in the
  # DELETED classifier's vocabulary, and each was a claim this guard can no longer support: it does not
  # read the lock, so it cannot know a pid, its liveness, or whether the object is a lock at all.
  # Asserted as ABSENCES because that is what the ruling changed; the presence half is above.
  if [[ "$out" != *"$live"* ]] \
     && [[ "$out" != *"is ACTIVE"* ]] && [[ "$out" != *"recorded pid"* ]] \
     && [[ "$out" != *"stale"* ]] && [[ "$out" != *"LEFT BEHIND"* ]] \
     && [[ "$out" != *"affirmatively dead"* ]] && [[ "$out" != *"PID NUMBERS ARE REUSED"* ]]; then
    pass "legacy-lock AC1 (ruling): the refusal makes NO claim about the holder — it never names the recorded pid $live, never calls the lock live or stale, and never asserts a process's fate"
  else
    fail "legacy-lock-present-claims-staleness: out=[$out] — the refusal asserted something about the holder that this guard does not measure"
  fi

  # ...AND IT RECOMMENDS NO DELETION, in any state. While the classifier existed, an enumerated
  # exactly-`{pid}` shape LICENSED printing `rm -f … && rmdir …`; with no inspection there is no licence,
  # and the deletion line is measurably destructive on a shape we no longer look at (see the symlink
  # case, which reproduces it). So no refusal may carry a deletion command at all.
  if [[ "$out" != *"rm -f"* && "$out" != *"rm -rf"* && "$out" != *"rmdir --"* && "$out" != *"rmdir '"* ]]; then
    pass "legacy-lock AC1 (ruling): the refusal carries NO deletion command — the guard did not inspect the object, so it prints nothing that would destroy it"
  else
    fail "legacy-lock-present-deletion: out=[$out] — a guard that does not inspect the lock must not print a deletion for it"
  fi

  # A REFUSED START ACQUIRES NOTHING. Asserted on the RUN'S OUTPUT as well as the filesystem, because
  # the filesystem half alone is vacuous: a successful acquisition removes the per-lane lock again on
  # exit (the EXIT trap), so `! -e` is true either way — it passed under the guard-removed mutant.
  if [[ "$out" != *"ACQUIRED="* && ! -e "$derived" ]]; then
    pass "legacy-lock AC1: the refusal acquired NOTHING — no ACQUIRED line and no per-lane lock (a refused start leaves nothing behind)"
  else
    fail "legacy-lock-present-sideeffect: out=[$out] derived-exists=$([[ -e "$derived" ]] && echo yes || echo no) — a refusal must not acquire the per-lane lock"
  fi
  first_detail="$(printf '%s\n' "$out" | grep -F 'refusing to start' | head -1 | sed "s#$legacy##g")"
  rm -rf "$legacy" "$derived"

  # ---- THE UNIFORMITY TABLE. Each of these shapes made the DELETED classifier take a DIFFERENT branch
  # and print a DIFFERENT cause and remedy: a live pid, a confirmed-dead pid, a non-numeric pid, a padded
  # one, a NUL-bearing one, a multi-line one, an out-of-range one, a missing pid file, an extra entry, a
  # plain file instead of a directory. Every one of them is now simply PRESENT, so every one must produce
  # the SAME first diagnostic line (the path aside) — and that is asserted against the LIVE case's own
  # wording above, not against a copy of the expected string, so a reworded refusal cannot pass here
  # while the uniformity property silently breaks.
  fixture_bg sleep 0.1
  dead=$FIXTURE_LAST_PID
  fixture_wait "$dead"
  for shape in dead-pid non-numeric-pid padded-pid nul-pid oversized-pid second-line \
               no-pid-file extra-entry hidden-extra-entry plain-file empty-dir; do
    rm -rf "$legacy" "$derived"
    case "$shape" in
      dead-pid)          mkdir -p "$legacy"; printf '%s\n' "$dead" >"$legacy/pid" ;;
      non-numeric-pid)   mkdir -p "$legacy"; printf 'pid-1234\n' >"$legacy/pid" ;;
      padded-pid)        mkdir -p "$legacy"; printf ' %s \n' "$dead" >"$legacy/pid" ;;
      nul-pid)           mkdir -p "$legacy"; printf '%s\0\n' "$dead" >"$legacy/pid" ;;
      oversized-pid)     mkdir -p "$legacy"; printf '99999999999999999999\n' >"$legacy/pid" ;;
      second-line)       mkdir -p "$legacy"; printf '%s\nfoo\n' "$dead" >"$legacy/pid" ;;
      no-pid-file)       mkdir -p "$legacy" ;;
      extra-entry)       mkdir -p "$legacy"; printf '%s\n' "$dead" >"$legacy/pid"; printf 'x\n' >"$legacy/notes.txt" ;;
      hidden-extra-entry) mkdir -p "$legacy"; printf '%s\n' "$dead" >"$legacy/pid"; printf 'x\n' >"$legacy/.hidden" ;;
      plain-file)        printf 'not a lock dir\n' >"$legacy" ;;
      empty-dir)         mkdir -p "$legacy" ;;
    esac
    out="$(legacy_lock_drive "$tmp" "$lane")"; rc=$?
    detail="$(printf '%s\n' "$out" | grep -F 'refusing to start' | head -1 | sed "s#$legacy##g")"
    if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" && [[ ! -e "$derived" ]] \
       && [[ -n "$detail" && "$detail" == "$first_detail" ]]; then
      pass "legacy-lock AC1 uniformity ($shape): refuses with the IDENTICAL diagnostic the live holder got — the guard does not parse the lock, so its contents cannot change the wording"
    else
      fail "legacy-lock-uniformity($shape): rc=$rc derived-exists=$([[ -e "$derived" ]] && echo yes || echo no) detail=[$detail] expected=[$first_detail] out=[$out]"
    fi
    # NOTHING WAS MUTATED, for every shape: no deletion command in the output, and the object still
    # there. The guard never writes to the legacy path, which is the whole basis of the ruling.
    if [[ "$out" != *"rm -f"* && "$out" != *"rm -rf"* && "$out" != *"rmdir --"* ]] && [[ -e "$legacy" || -L "$legacy" ]]; then
      pass "legacy-lock AC1 uniformity ($shape): no deletion command printed and the object is STILL THERE after the refusal"
    else
      fail "legacy-lock-uniformity-mutated($shape): legacy-exists=$([[ -e "$legacy" ]] && echo yes || echo GONE) out=[$out]"
    fi
  done

  # NO RENAMED-ASIDE / STALE RESIDUE ANYWHERE under the case TMPDIR — that absence IS the property
  # "this guard never touches an object it does not own".
  local residue
  residue="$(find "$tmp" \( -name '*.aside.*' -o -name '*.stale.*' \) 2>/dev/null | wc -l | tr -d ' ')"
  if [[ "$residue" == "0" ]]; then
    pass "legacy-lock AC1 (ruling): no aside/stale scratch path exists anywhere under the case TMPDIR after $((11)) refusals"
  else
    fail "legacy-lock-residue: $residue aside/stale paths under $tmp — the guard mutated something it does not own"
  fi

  # ---- MUTANT (a): A GUARD THAT PROCEEDS ON PRESENCE MUST RED. The mutation is the smallest one that
  # expresses the defect: the `present` branch returns instead of refusing. Derived from the shipped
  # function, so it cannot drift into a re-implementation.
  local ovr mout mrc=0
  rm -rf "$legacy" "$derived"
  mkdir -p "$legacy"
  printf '%s\n' "$dead" >"$legacy/pid"
  ovr="$d/m-proceed.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" supervisor_legacy_lock_guard \
       '    present)' '    present)
      return 0
      ;;
    never-taken-present)'; then
    mout="$(legacy_lock_drive_override "$ovr" "$tmp" "$lane")" || mrc=$?
    if [[ "$mrc" -eq 0 && "$mout" == *"ACQUIRED=$derived"* && "$mout" != *"LEGACY GLOBAL supervisor lock"* ]]; then
      pass "legacy-lock MUTANT (a): a guard whose \`present\` branch PROCEEDS starts anyway with the legacy lock in place (ACQUIRED, no refusal) — so the refusal above is the shipped branch doing the work, not the harness"
    else
      fail "legacy-lock-mutant-proceed: rc=$mrc out=[$mout] — the proceeding mutant must start, or the presence refusal measures nothing"
    fi
  fi
  rm -rf "$legacy" "$derived"

  # ---- STRUCTURAL: THE DELETED MACHINERY MUST NOT CREEP BACK. A behavioural case cannot see a
  # reintroduced code path that nothing currently reaches, and every one of these names belonged to a
  # capability the two rulings removed: reclaim/restore/aside, and the classifier with its pid parsing,
  # liveness measurement and glob-state neutralisation.
  if ! grep -qE 'supervisor_legacy_lock_(reclaim_stale|restore_and_refuse)|SUPERVISOR_LEGACY_ASIDE_CHILD' "$SUPERVISOR"; then
    pass "legacy-lock (ruling 1): the reclaim/restore/aside machinery is GONE from the supervisor (not merely unreached)"
  else
    fail "legacy-lock-reclaim-resurrected: the supervisor still defines reclaim/restore/aside machinery the ruling deleted"
  fi
  local revived
  # The names are the DELETED symbols and the two inherited-state variables the deleted enumeration
  # depended on. `kill -0` is deliberately NOT in the list: the per-lane lock and the worker-liveness
  # loop both use it legitimately, and a pattern that reds on correct code is the pattern people learn
  # to waive.
  revived="$(grep -nE 'supervisor_legacy_lock_state|supervisor_pid_liveness|GLOBIGNORE' "$SUPERVISOR" | grep -vE '^[0-9]+:[[:space:]]*#' || true)"
  if [[ -z "$revived" ]]; then
    pass "legacy-lock (ruling 2): the classifier, the deleted pid-liveness SYMBOL and the glob-state neutralisation are GONE from the supervisor's code"
  else
    fail "legacy-lock-classifier-resurrected: [$revived] — machinery whose output cannot change the decision is back on the decision path"
  fi
  # ...AND THE PROPERTY THE NAME CHECK CANNOT SEE (#3601). A NAME BAN IS NOT A PROPERTY ASSERT: the
  # ruling deleted pid liveness from THIS decision path, not from the file, and #3601 legitimately
  # rebuilt the capability for the PER-LANE lock — where, unlike here, the verdict selects between
  # refusing and reclaiming, so it is a guard and not a description generator. Read as a file-wide ban
  # the assert above would have to red on that correct code, which is the assert people learn to waive;
  # read as a name it can be satisfied by a rename, which is the assert that sees nothing. So the
  # durable half is stated over the three functions that ARE the legacy decision: none of them may
  # measure a pid's liveness, by any spelling — no `kill`, and no call to the per-lane probes.
  #
  # `pid_max` MOVED OUT OF THE FILE-WIDE NAME LIST FOR THE SAME REASON (#3601, roborev job 231 B3). The
  # classifier's `/proc/sys/kernel/pid_max` read was deleted because ITS verdict could not change the
  # decision; the per-lane parser reads the same file to bound a holder pid, where the verdict selects
  # between refusing and reclaiming. A file-wide ban would have to red on that correct code — the ban
  # people learn to waive — so what is asserted is the property: the legacy path reads no pid bound.
  local legacy_fn legacy_body legacy_code liveness_on_legacy_path=""
  for legacy_fn in supervisor_legacy_lock_presence supervisor_legacy_lock_refuse supervisor_legacy_lock_guard; do
    legacy_body="$(sed -n "/^$legacy_fn()/,/^}/p" "$SUPERVISOR")"
    if [[ -z "$legacy_body" || "$legacy_body" != *"$legacy_fn() {"* ]]; then
      fail "legacy-lock-liveness-property-premise: could not extract $legacy_fn from $SUPERVISOR; the scan below has no subject"
      legacy_code=""
      break
    fi
    legacy_code="$(printf '%s\n' "$legacy_body" | grep -vE '^[[:space:]]*#' || true)"
    if [[ -z "$legacy_code" ]]; then
      fail "legacy-lock-liveness-property-premise: $legacy_fn has no non-comment line"
      break
    fi
    liveness_on_legacy_path+="$(printf '%s\n' "$legacy_code" | grep -nE 'kill[[:space:]]|supervisor_lock_holder_liveness|supervisor_lock_pid_read|pid_max|/proc/' || true)"
  done
  if [[ -n "$legacy_code" && -z "$liveness_on_legacy_path" ]]; then
    pass "legacy-lock (ruling 2, as a PROPERTY): no function on the legacy decision path measures a pid's liveness, parses a pid, or reads a platform pid bound, by any spelling — so the deletion holds even though those capabilities exist elsewhere in the file for a path where their verdict changes the decision"
  elif [[ -n "$legacy_code" ]]; then
    fail "legacy-lock-liveness-on-legacy-path: [$liveness_on_legacy_path] — the legacy guard tests for EXISTENCE and stops; measuring a holder's liveness or reading a pid bound there is the machinery the ruling removed, whatever it is called"
  fi
  # ...AND THE PROBE STILL EXECUTES NO EXTERNAL COMMAND, so none of its verdicts depends on `PATH`.
  # Comment lines are stripped first (the prose legitimately NAMES tools it does not use, and a check
  # that reds on correct text is the check people learn to waive); what remains is code.
  local body code ext
  body="$(sed -n '/^supervisor_legacy_lock_presence()/,/^}/p' "$SUPERVISOR")"
  code="$(printf '%s\n' "$body" | grep -vE '^[[:space:]]*#' || true)"
  ext="$(printf '%s\n' "$code" | grep -nEw 'od|wc|cmp|tr|dd|cat|sed|awk|grep|expr|stat|ls|find|ps|env|xargs|head|tail|cut|python3|perl' || true)"
  if [[ -n "$code" && "$code" == *"supervisor_legacy_lock_presence() {"* && -z "$ext" ]]; then
    pass "legacy-lock: the presence probe's code names no external tool — its verdicts cannot be changed by the caller's PATH"
  else
    fail "legacy-lock-probe-external-command: [$ext] — an external command in the probe puts its verdicts at the mercy of PATH; use a builtin"
  fi
}



# ---------------------------------------------------------------------------
# THE SYMLINK SHAPES (#3549, roborev job 180 Medium, carried forward through the lead's second ruling).
#
# `-e` FOLLOWS the link, so `-e` ALONE reports a DANGLING symlink at the legacy path as ABSENCE — the
# permissive collapse, and the reason the existence test is `-e || -L`. Something IS at that name: a
# pre-#3467 supervisor's `mkdir` of it fails, so it is not free, and reading it as absence lets this run
# proceed as if no legacy supervisor could exist.
#
# AND THE SYMLINK IS WHY NO DELETION IS PRINTED ANY MORE. While the classifier existed it followed the
# link, saw a textbook lock behind it, and handed the operator `rm -f -- <legacy>/pid && rmdir --
# <legacy>` — which MEASURABLY deletes a foreign directory's `pid` file. That measurement is reproduced
# below against the pre-ruling command rather than argued from the source.
# ---------------------------------------------------------------------------
test_legacy_global_lock_symlink_shapes_refuse() {
  local d tmp lane legacy derived out rc dead target link_before residue ovr mout mrc
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"; lane="lane3549sym$$"
  mkdir -p "$tmp"
  legacy="$tmp/cqlite-worker-supervisor.lock"
  derived="$tmp/cqlite-worker-supervisor-$lane.lock"

  fixture_bg sleep 0.1
  dead=$FIXTURE_LAST_PID
  fixture_wait "$dead"

  # ---- (a) a symlink to a directory that is NOT a lock but happens to hold a well-formed `pid`. ----
  rm -rf "$legacy" "$derived"
  target="$tmp/not-a-lock-at-all"
  mkdir -p "$target"
  printf '%s\n' "$dead" >"$target/pid"
  ln -s "$target" "$legacy"
  link_before="$(readlink "$legacy")"
  out="$(legacy_lock_drive "$tmp" "$lane")"; rc=$?
  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" && [[ "$out" == *"EXISTS there"* ]] && [[ ! -e "$derived" ]]; then
    pass "legacy-lock symlink(a): a symlink at the legacy path is PRESENT — it refuses the start and creates no per-lane lock"
  else
    fail "legacy-lock-symlink-a: rc=$rc derived-exists=$([[ -e "$derived" ]] && echo yes || echo no) out=[$out] — expected a presence refusal"
  fi
  # NOTHING MOVED, NOTHING DELETED, AND NOTHING PRINTED THAT WOULD DO EITHER.
  residue="$(find "$tmp" -maxdepth 1 -name '*.aside.*' 2>/dev/null | wc -l | tr -d ' ')"
  if [[ -L "$legacy" && "$(readlink "$legacy")" == "$link_before" ]] \
     && [[ -d "$target" && -f "$target/pid" && "$(cat "$target/pid")" == "$dead" ]] \
     && [[ "$residue" == "0" ]] && [[ "$out" != *"rm -f"* && "$out" != *"rmdir --"* ]]; then
    pass "legacy-lock symlink(a): the link, its TARGET directory and the target's pid file are all untouched, no aside residue was created, and the refusal printed no command that would touch them"
  else
    fail "legacy-lock-symlink-a-destroyed: link=$([[ -L "$legacy" ]] && readlink "$legacy" || echo GONE) target-dir=$([[ -d "$target" ]] && echo yes || echo GONE) target-pid=[$(cat "$target/pid" 2>/dev/null || echo GONE)] aside-residue=$residue out=[$out]"
  fi

  # ---- (a2) THE HARM OF THE PRE-RULING REMEDY, MEASURED ON THIS EXACT SHAPE. The deleted branch's
  # command, run against this staged symlink, follows the link and DELETES the foreign directory's `pid`
  # (rc=0) and then fails at the `rmdir` — so the operator destroys a file the guard never examined and
  # the legacy lock is still there. This is why the printed line is now read-only, and it is measured
  # here rather than asserted from the source comment.
  local del_rc=0
  eval "rm -f -- '$legacy/pid' && rmdir -- '$legacy'" >/dev/null 2>&1 || del_rc=$?
  if [[ "$del_rc" -ne 0 && ! -e "$target/pid" && -L "$legacy" ]]; then
    pass "legacy-lock symlink(a2): the PRE-RULING deletion remedy, run against this symlink, destroyed the FOREIGN directory's pid file and then failed (rc=$del_rc) leaving the lock in place — the measurement that makes a read-only remedy the only honest one"
  else
    fail "legacy-lock-symlink-a2: rc=$del_rc foreign-pid=$([[ -e "$target/pid" ]] && echo intact || echo deleted) link=$([[ -L "$legacy" ]] && echo yes || echo GONE) — the harm of the deleted remedy is not established, so the justification for dropping it is unmeasured"
  fi

  # ---- (b) a DANGLING symlink: `-e` is FALSE and `-L` is TRUE. -----------------------------------
  rm -rf "$legacy" "$derived"
  ln -s "$tmp/nothing-is-here" "$legacy"
  out="$(legacy_lock_drive "$tmp" "$lane")"; rc=$?
  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" && [[ ! -e "$derived" ]]; then
    pass "legacy-lock symlink(b): a DANGLING symlink is NOT verified absence — it refuses the start"
  else
    fail "legacy-lock-symlink-b: rc=$rc derived-exists=$([[ -e "$derived" ]] && echo yes || echo no) out=[$out]"
  fi
  if [[ -L "$legacy" ]]; then
    pass "legacy-lock symlink(b): the dangling link itself was not removed"
  else
    fail "legacy-lock-symlink-b-destroyed: the dangling link at $legacy is gone"
  fi

  # ---- MUTANT (a'): `-e || -L` reduced to `-e` alone. The dangling link then reads as VERIFIED
  # ABSENCE and the start PROCEEDS — the two-valued collapse, measured on the shipped probe with one
  # substitution.
  ovr="$d/m-eonly.sh"; : >"$ovr"
  mrc=0
  if sv_mutant_override "$ovr" supervisor_legacy_lock_presence \
       'if [[ -e "$legacy" || -L "$legacy" ]]; then' 'if [[ -e "$legacy" ]]; then'; then
    mout="$(legacy_lock_drive_override "$ovr" "$tmp" "$lane")" || mrc=$?
    if [[ "$mrc" -eq 0 && "$mout" == *"ACQUIRED=$derived"* && "$mout" != *"LEGACY GLOBAL supervisor lock"* ]]; then
      pass "legacy-lock symlink MUTANT (a'): with the \`-L\` half removed, the SAME dangling link is read as verified absence and the supervisor STARTS — so the \`|| -L\` is load-bearing, not decoration"
    else
      fail "legacy-lock-symlink-mutant: rc=$mrc out=[$mout] — the -e-only probe must proceed on a dangling link, or the assert above measures nothing"
    fi
    rm -rf "$derived"
  fi

  rm -rf "$legacy" "$derived"
}

# ---------------------------------------------------------------------------
# THE HEADLINE CASE OF THE NEW CONTRACT (#3549, lead ruling 2026-08-30): AN EXPLICIT
# `SUPERVISOR_LOCK` DOES **NOT** SKIP THE CHECK.
#
# WHAT WAS REMOVED AND WHY, because this case is the inverse of the one it replaces. AC4 said "an
# explicit `SUPERVISOR_LOCK` override skips the legacy check entirely", and the case here asserted
# exactly that, with a LIVE legacy holder present, and passed. The ruling removed AC4 as UNSOUND on a
# one-sentence proof: an explicit `SUPERVISOR_LOCK` renames OUR lock, while a pre-#3467 supervisor uses
# the machine-global path REGARDLESS — it has never heard of the variable — so the skip disabled the
# check in a case where the collision is still LIVE. A naming choice is not an isolation guarantee.
#
# So the property is now the OPPOSITE, and it is asserted the same way the removed one was: plant a
# legacy lock, name our own lock explicitly, and require the refusal to happen anyway. FOUR halves,
# because each can pass for the wrong reason on its own — the refusal (the contract), the CONTROL that
# proves the explicit path is otherwise perfectly usable (else the refusal might be about the path and
# not about the legacy lock), the still-MUTATES-NOTHING assertion (the one thing the removed case
# checked that is still true), and a MUTANT CONTRAST reintroducing the skip.
# ---------------------------------------------------------------------------
test_explicit_lock_does_not_skip_the_check() {
  local d tmp lane legacy explicit live out rc cout crc ovr mout code
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"; lane="lane3549noskip$$"
  mkdir -p "$tmp"
  legacy="$tmp/cqlite-worker-supervisor.lock"
  explicit="$d/explicit.lock"

  # ---- (0) CONTROL, FIRST: with NO legacy lock present, the explicit path is acquired normally. Run
  # before anything is planted, so the refusal below cannot be explained by the explicit path itself
  # being unusable — which is the way this case would otherwise pass for the wrong reason.
  cout="$(legacy_lock_drive "$tmp" "$lane" "$explicit")"; crc=$?
  if [[ "$crc" -eq 0 && "$cout" == *"ACQUIRED=$explicit"* && "$cout" == *"LOCKDIR=yes"* ]]; then
    pass "no-skip CONTROL: with no legacy lock present, an explicit SUPERVISOR_LOCK is honoured and acquired — so the refusal below is caused by the legacy lock, not by the explicit path"
  else
    fail "no-skip-control: rc=$crc out=[$cout] — an explicit lock with no legacy lock present must acquire"
  fi
  rm -rf "$explicit"

  # ---- (1) THE CONTRACT. A LIVE legacy holder is present — the strongest form, and the exact staging
  # the deleted AC4 case used to prove the skip — and the start is REFUSED anyway.
  fixture_bg sleep 300
  live=$FIXTURE_LAST_PID
  mkdir -p "$legacy"
  printf '%s\n' "$live" >"$legacy/pid"
  out="$(legacy_lock_drive "$tmp" "$lane" "$explicit")"; rc=$?

  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" && [[ "$out" == *"EXISTS there"* ]] \
     && [[ "$out" != *"ACQUIRED="* ]] && [[ ! -e "$explicit" ]]; then
    pass "no-skip CONTRACT: an explicit SUPERVISOR_LOCK does NOT skip the legacy check — the start is refused over the present legacy lock and NO lock is created at the operator's path (#3549, AC4 removed as unsound)"
  else
    fail "no-skip-contract: rc=$rc explicit-exists=$([[ -e "$explicit" ]] && echo yes || echo no) out=[$out] — naming our own lock must not disable the compatibility check"
  fi

  # ---- (2) AND IT STILL MUTATES NOTHING. The one assertion the removed case made that survives the
  # ruling: whatever the verdict, the foreign lock is not reclaimed, rewritten or removed.
  if [[ -d "$legacy" && -f "$legacy/pid" && "$(cat "$legacy/pid")" == "$live" ]]; then
    pass "no-skip: the legacy lock is UNTOUCHED by the refused run (not reclaimed, not rewritten, not removed)"
  else
    fail "no-skip-touched: the legacy lock at $legacy was modified by a run that only had to refuse"
  fi

  # ---- (3) MUTANT CONTRAST: the skip, reintroduced at the top of the guard in its most direct form —
  # keyed on the explicit lock itself, which is what AC4's provenance record was a proxy for. The SAME
  # drive then starts alongside the live legacy holder, which is the harm the ruling removed.
  ovr="$d/m-noskip.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" supervisor_legacy_lock_guard \
       '  local legacy="${TMPDIR:-/tmp}/cqlite-worker-supervisor.lock" state' \
       '  case "$SUPERVISOR_LOCK" in ?*) return 0 ;; esac
  local legacy="${TMPDIR:-/tmp}/cqlite-worker-supervisor.lock" state'; then
    mout="$(env TMPDIR="$tmp" SUPERVISOR_LOCK="$explicit" LANE_ID="$lane" LOCK_CMD="" CLAIM_CMD="" \
      bash -c "$SV_DRIVE_BODY_OVERRIDE" _ "$SUPERVISOR" "$ovr" 2>&1)" || true
    if [[ "$mout" == *"ACQUIRED=$explicit"* ]] && [[ "$mout" != *"LEGACY GLOBAL supervisor lock"* ]]; then
      pass "no-skip MUTANT CONTRAST: with the AC4 skip restored, the same run starts alongside a LIVE pre-#3467 holder and never mentions the legacy lock — the unsound behaviour, measured, so the assert above has teeth"
    else
      fail "no-skip-mutant: out=[$mout] — the removed skip must be shown to bypass the guard, or the case above measures nothing"
    fi
    rm -rf "$explicit"
  fi

  fixture_kill "$live"

  # ---- (4) STRUCTURAL: the guard's CODE names `SUPERVISOR_LOCK` NOWHERE, in any spelling. There is
  # therefore no observable in it that a skip could be keyed on, for states no behavioural case stages.
  # Full-line comments are stripped first — the guard's prose necessarily names the variable to record
  # why the skip is gone.
  code="$(sed -n '/^supervisor_legacy_lock_guard()/,/^}/p' "$SUPERVISOR" | grep -vE '^[[:space:]]*#' || true)"
  if [[ -z "$code" ]]; then
    fail "no-skip-structural-premise: supervisor_legacy_lock_guard has no non-comment line; the scan has no subject"
  elif [[ "$code" != *"SUPERVISOR_LOCK"* ]]; then
    pass "no-skip STRUCTURAL: the guard's code mentions SUPERVISOR_LOCK in no spelling at all — it consults no provenance, no pinned path and no lock name, so there is nothing left for a skip to key on"
  else
    fail "no-skip-structural: the guard's code references SUPERVISOR_LOCK; code=[$code] — the check must not depend on where our own lock lives (#3549, AC4 removed as unsound)"
  fi

  rm -rf "$legacy" "$explicit"
}

test_legacy_global_lock_residual_recorded() {
  local block
  # The guard REDUCES, and does not eliminate, the collision window (roborev job 178, Half B: a
  # pre-#3467 supervisor STARTING AFTER our check cannot be stopped without machine-global exclusion,
  # which #3393's owner ruling forbids). That is a documented risk only if it is actually written down
  # where the guard is read. Pins the RECORD, not its prose.
  block="$(sed -n '/^# RESIDUAL (#3549/,/^supervisor_legacy_lock_guard()/p' "$SUPERVISOR")"
  if [[ -n "$block" && "$block" == *"#3393"* && "$block" == *"REDUCES"* ]]; then
    pass "legacy-lock RESIDUAL: the guard records the unclosed later-start window, why #3393 forbids closing it, and that the guard reduces rather than eliminates the collision window"
  else
    fail "legacy-lock-residual: no RESIDUAL block naming #3393 and the reduction is recorded at the guard"
  fi
}

test_legacy_global_lock_removal_condition_recorded() {
  local block
  # AC6: the check is REMOVABLE, and the condition under which it may be dropped is RECORDED IN THE
  # CODE — not in a commit message that nobody reads at deletion time. Light on purpose: this pins the
  # RECORD, not its prose.
  block="$(sed -n '/LEGACY GLOBAL LOCK COMPATIBILITY/,/^supervisor_legacy_lock_guard()/p' "$SUPERVISOR")"
  if [[ "$block" == *"REMOVAL CONDITION"* && "$block" == *"#3467"* && "$block" == *"#3549"* ]]; then
    pass "legacy-lock AC6: the guard records its own removal condition (every checkout at or past #3467) with both issue numbers"
  else
    fail "legacy-lock-removal-condition: the guard does not record a removal condition naming #3467 and #3549"
  fi
  # AND THE RECORD OF THE OTHER REMOVAL: AC4's skip is gone, and WHY it went is written where the guard
  # is read — so nobody reintroduces a skip as a convenience. Pins the RECORD, not its prose; the
  # BEHAVIOUR is `test_explicit_lock_does_not_skip_the_check`.
  local guard_body
  guard_body="$(sed -n '/^supervisor_legacy_lock_guard()/,/^}/p' "$SUPERVISOR")"
  if [[ "$guard_body" == *"ALWAYS RUNS"* ]] && [[ "$guard_body" == *"AC4"* ]] \
     && [[ "$guard_body" == *"unsound"* || "$guard_body" == *"UNSOUND"* ]]; then
    pass "legacy-lock: the guard records that it ALWAYS runs and that AC4's skip was removed as unsound, at the guard itself"
  else
    fail "legacy-lock-ac4-removal-record: supervisor_legacy_lock_guard does not record that it always runs and that AC4 was removed as unsound"
  fi
}

# ---------------------------------------------------------------------------
# THE CONTAINER GATE (#3549, roborev job 182 F3 + the lead's second ruling) — BOTH DIRECTIONS.
#
# The probe decides the legacy path's existence with `[[ -e ]]`/`[[ -L ]]` on a KNOWN NAME and never
# enumerates the container, so `lstat(2)` needs the execute/search bit and nothing else:
#   * REQUIRING `-r` on `${TMPDIR}` is a FALSE STOP — a legitimate write-and-search-only TMPDIR
#     (mode 0311) refused every start even though the legacy path is definitively absent AND our own
#     per-lane lock can be created there.
#   * NOT REQUIRING `-x` is the PERMISSIVE COLLAPSE — on an unsearchable container `[[ -e ]]` answers
#     "not there" for a lock that IS there, and the start proceeds as if no legacy supervisor existed.
# Both are measured here, each against a one-substitution mutant of the shipped probe.
# ---------------------------------------------------------------------------
test_legacy_lock_container_needs_search_not_read() {
  local d tmp lane legacy derived out rc body ans ovr mout mrc
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/searchonly-tmp"; lane="lane3549srch$$"
  mkdir -p "$tmp"
  legacy="$tmp/cqlite-worker-supervisor.lock"
  derived="$tmp/cqlite-worker-supervisor-$lane.lock"

  # ---- DIRECTION 1 (root-independent): a container that DOES NOT EXIST is not an absence — it is an
  # UNDECIDABLE existence question, so it refuses, and the cause must say THE PROBE FAILED rather than
  # that a lock is there. This half needs no permission bits, so it runs as any user.
  local missing="$d/no-such-container"
  out="$(legacy_lock_drive "$missing" "$lane")"; rc=$?
  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" \
     && [[ "$out" == *"EXISTENCE PROBE FAILED"* ]] && [[ "$out" == *"container-not-searchable:"* ]] \
     && [[ "$out" != *"EXISTS there"* ]]; then
    pass "legacy-lock container: a NON-EXISTENT container refuses with a cause that says the PROBE FAILED — never that a legacy lock exists (the two are different facts with different remedies)"
  else
    fail "legacy-lock-container-missing: rc=$rc out=[$out] — expected an 'EXISTENCE PROBE FAILED' refusal naming container-not-searchable"
  fi
  # ...and it prints NO runnable line: there is nothing to inspect, so there is no command to give.
  remedy_lines_structural "could-not-tell-container" "$out" 0

  if [[ "$(id -u)" == "0" ]]; then
    # Under root every permission bit is advisory: `-r` is true on a 0311 directory and a 0600 one is
    # still searchable, so neither remaining direction is stageable and a green result would mean
    # nothing. SKIPped explicitly rather than passed vacuously.
    skip "legacy-lock container: running as root, where permission bits are advisory — neither the false-STOP (-r) nor the permissive-collapse (-x) direction can be measured"
    return 0
  fi

  # ---- DIRECTION 2: WRITE + SEARCH, NO READ (0311), legacy path ABSENT. The per-lane lock must still
  # be creatable (it is a `mkdir` in this directory), which is what makes the false STOP a real cost.
  chmod 0311 "$tmp"
  out="$(legacy_lock_drive "$tmp" "$lane")"; rc=$?
  chmod 0711 "$tmp"
  if [[ "$rc" -eq 0 && "$out" == *"ACQUIRED=$derived"* && "$out" != *"LEGACY GLOBAL supervisor lock"* ]]; then
    pass "legacy-lock container: a write-and-search-only TMPDIR (0311) with the legacy path ABSENT PROCEEDS — verified absence needs search, not read"
  else
    fail "legacy-lock-container-search: rc=$rc out=[$out] — a search-only container with no legacy lock must not refuse"
  fi
  rm -rf "$derived"

  # MUTANT (false STOP): restore the `-r` gate and the very same directory refuses. Without this, the
  # pass above could be true of any probe — including one that ignores the container entirely.
  ovr="$d/m-rgate.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" supervisor_legacy_lock_presence \
       'if [[ ! -d "$dir" || ! -x "$dir" ]]; then' 'if [[ ! -d "$dir" || ! -r "$dir" || ! -x "$dir" ]]; then'; then
    chmod 0311 "$tmp"
    mout="$(legacy_lock_drive_override "$ovr" "$tmp" "$lane")" || true
    chmod 0711 "$tmp"
    if [[ "$mout" == *"container-not-searchable:"* ]]; then
      pass "legacy-lock container MUTANT (false STOP): the same probe WITH the -r gate restored refuses the identical search-only container — the false STOP is measured, not argued"
    else
      fail "legacy-lock-container-rgate-mutant: out=[$mout] — the -r-restored probe must refuse, so the fix's effect is not established"
    fi
    rm -rf "$derived"
  fi

  # ---- DIRECTION 3: THE PERMISSIVE COLLAPSE. An UNSEARCHABLE container (0600 — readable, NOT
  # searchable) holding a REAL legacy lock. The shipped probe cannot see the lock, so it must say so;
  # a probe without the `-x` gate reports VERIFIED ABSENCE for a lock that is right there.
  mkdir -p "$legacy"
  printf '1\n' >"$legacy/pid"
  chmod 0600 "$tmp"
  if [[ -d "$tmp" && ! -x "$tmp" ]]; then
    pass "legacy-lock container PREMISE: an UNSEARCHABLE container holding a real legacy lock was staged, so direction 3 measures the real shape"
  else
    fail "legacy-lock-container-premise: searchable=$([[ -x "$tmp" ]] && echo yes || echo no) — the unsearchable state could not be staged"
  fi
  out="$(legacy_lock_drive "$tmp" "$lane")"; rc=$?
  chmod 0711 "$tmp"
  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" && [[ "$out" == *"EXISTENCE PROBE FAILED"* ]]; then
    pass "legacy-lock container: an UNSEARCHABLE container holding a REAL legacy lock refuses — 'cannot tell' never collapses onto the permissive answer"
  else
    fail "legacy-lock-container-unsearchable: rc=$rc out=[$out] — expected a probe-failed refusal"
  fi

  # MUTANT (b): THE PERMISSIVE COLLAPSE ITSELF — the container gate deleted. `[[ -e ]]` then answers
  # false for the lock it cannot stat, the probe reports `verified-absent`, and the supervisor STARTS
  # alongside a pre-#3467 holder. This is the mutant the three-valued design exists for.
  ovr="$d/m-nogate.sh"; : >"$ovr"
  mrc=0
  if sv_mutant_override "$ovr" supervisor_legacy_lock_presence \
       'if [[ ! -d "$dir" || ! -x "$dir" ]]; then' 'if false; then'; then
    # The per-lane lock lives in the same container, so give the mutant a searchable place to acquire in
    # by pointing the probe at the unsearchable one through TMPDIR only — impossible; instead assert on
    # the probe's own answer, which is the state the guard branches on, read from the SHIPPED text.
    body="$T_LOCKFN/presencefn.sh"
    mkdir -p "$T_LOCKFN"
    {
      sv_scratch_head
      printf '%s\n' 'source "$2"'
      printf '%s\n' 'supervisor_legacy_lock_presence "$1"'
    } >"$body"
    chmod 0600 "$tmp"
    ans="$(bash "$body" "$legacy" "$ovr" 2>&1 || true)"
    chmod 0711 "$tmp"
    if [[ "$ans" == verified-absent ]]; then
      pass "legacy-lock container MUTANT (b): with the container gate removed, the probe answers 'verified-absent' for a legacy lock that IS PRESENT behind an unsearchable container — the permissive collapse, measured"
    else
      fail "legacy-lock-container-nogate-mutant: the gate-less probe answered [$ans]; expected verified-absent, or the value of the -x gate is not established"
    fi
    # ...and the SHIPPED probe, same driver, same directory, answers could-not-tell.
    {
      sv_scratch_head
      printf '%s\n' 'supervisor_legacy_lock_presence "$1"'
    } >"$body"
    chmod 0600 "$tmp"
    ans="$(bash "$body" "$legacy" 2>&1 || true)"
    chmod 0711 "$tmp"
    if [[ "$ans" == could-not-tell\ container-not-searchable:* ]]; then
      pass "legacy-lock container: the SHIPPED probe answers [$ans] for that same directory — read out of the supervisor at run time with no substitution at all"
    else
      fail "legacy-lock-container-shipped: the shipped probe answered [$ans]; expected 'could-not-tell container-not-searchable:…'"
    fi
  fi

  # ...and the shipped probe answers `verified-absent` for the SEARCH-ONLY container the -r gate rejected.
  rm -rf "$legacy"
  {
    sv_scratch_head
    printf '%s\n' 'supervisor_legacy_lock_presence "$1"'
  } >"$T_LOCKFN/shipped-presence.sh"
  chmod 0311 "$tmp"
  ans="$(bash "$T_LOCKFN/shipped-presence.sh" "$legacy" 2>&1 || true)"
  chmod 0711 "$tmp"
  if [[ "$ans" == verified-absent ]]; then
    pass "legacy-lock container: the shipped probe answers 'verified-absent' (a VERIFIED absence) for the search-only container the -r gate rejected"
  else
    fail "legacy-lock-container-searchonly-shipped: the shipped probe answered [$ans] for a search-only container with the legacy path absent; expected verified-absent"
  fi
}

t test_legacy_global_lock_refuses_a_present_lock
t test_legacy_global_lock_symlink_shapes_refuse
t test_explicit_lock_does_not_skip_the_check
t test_legacy_global_lock_removal_condition_recorded
# ---------------------------------------------------------------------------
# Test 47k (#3549, roborev job 222 F1; lead ruling 2026-08-31): THE CHECK'S SCOPE IS DECLARED WHERE AN
# OPERATOR READS IT.
#
# THE FINDING. The guard probes ONE path, derived from THIS process's `TMPDIR`. A pre-#3467 supervisor
# launched with a different `TMPDIR`, or with an explicit `SUPERVISOR_LOCK`, holds a path we never stat.
# The ruling was NOT to probe more paths (with the classifier deleted there is no stale/live
# discrimination, so each extra path is a permanent false refusal with no remedy) and NOT to fail closed
# (the path can never be established for an arbitrary launcher, so fail-closed means refuse ALWAYS).
# The ruling was to DECLARE THE SCOPE and change no detection logic. So the property under test is a
# STATEMENT, and the risk to it is that the statement quietly disappears — which is what this pins.
#
# WHY BOTH HALVES. Structural alone passes on a comment nobody prints; behavioural alone passes on a
# line whose wording drifts free of the record at the guard. And the behavioural half carries a MUTANT
# CONTRAST — the pre-fix silent `verified-absent) return 0` — because "the output contains a
# qualification" is exactly the assert that passes for free if the drive is emitting anything at all.
# Pins the RECORD and the EMISSION, not the prose: the tokens asserted are the ones that carry the
# CLAIM (the path, that it is the ONLY one, and whose environment resolved it).
# ---------------------------------------------------------------------------
test_legacy_lock_scope_is_declared() {
  local d tmp lane legacy derived out rc line ovr mout mrc block doc explicit

  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"; lane="lane3549scope$$"
  mkdir -p "$tmp"
  legacy="$tmp/cqlite-worker-supervisor.lock"
  derived="$tmp/cqlite-worker-supervisor-$lane.lock"

  # ---- (1) BEHAVIOURAL, on the PROCEED path — the one an operator sees on every clean start. It must
  # name the path actually tested and say that path is ALL that was tested; a bare absence is the
  # falsely reassuring negative this ruling is about.
  out="$(legacy_lock_drive "$tmp" "$lane")"; rc=$?
  if [[ "$rc" -eq 0 && "$out" == *"ACQUIRED=$derived"* ]]; then
    pass "scope-declared PREMISE: with no legacy lock the drive reaches the proceed path and acquires (so the line below is the guard's, not an error path's)"
  else
    fail "scope-declared-premise: rc=$rc out=[$out] — the proceed path must be reached, or nothing below is attributable"
  fi
  line="$(printf '%s\n' "$out" | grep -F 'legacy-lock check' | head -1)"
  if [[ -n "$line" ]] && [[ "$line" == *"$legacy"* ]] \
     && [[ "$line" == *"ONLY path"* ]] && [[ "$line" == *"TMPDIR"* ]]; then
    pass "scope-declared: the proceed path STATES ITS REACH — it names the one path it tested ($legacy), says that is the only one, and says whose TMPDIR resolved it"
  else
    fail "scope-declared-proceed: line=[$line] out=[$out] — a clean start must state which path was checked and that it was the only one (#3549 job 222 F1)"
  fi
  # ...and it is a DIAGNOSTIC line, not a bare one: a bare line in this file's contract is a runnable
  # command, and this is prose.
  if [[ "$line" =~ ^\[worker-supervisor\]\  ]]; then
    pass "scope-declared: the declaration is a prefixed diagnostic line, so it cannot be mistaken for the one bare runnable line the refusals print"
  else
    fail "scope-declared-prefix: line=[$line] — the proceed-path declaration must carry the log prefix"
  fi
  rm -rf "$derived"

  # ---- (2) MUTANT CONTRAST: the pre-fix branch, which returned 0 in silence. The start still succeeds
  # — which is the point: silence is INDISTINGUISHABLE from a checked all-clear, and that is the defect.
  ovr="$d/m-scope.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" supervisor_legacy_lock_guard \
       '    verified-absent)' \
       '    verified-absent)
      return 0'; then
    mrc=0
    mout="$(legacy_lock_drive_override "$ovr" "$tmp" "$lane")" || mrc=$?
    if [[ "$mrc" -eq 0 && "$mout" == *"ACQUIRED=$derived"* ]] && [[ "$mout" != *"legacy-lock check"* ]]; then
      pass "scope-declared MUTANT CONTRAST: with the pre-fix early return restored, the identical drive starts with NO statement of what was checked — an operator cannot tell a one-path check from a whole-box all-clear, so the assert above has teeth"
    else
      fail "scope-declared-mutant: rc=$mrc out=[$mout] — the silent branch must be shown to emit no declaration, or (1) measures nothing"
    fi
    rm -rf "$derived"
  fi

  # ---- (3) BEHAVIOURAL, on the REFUSAL path (job 222 F2): the relationship line must not assert that
  # our lock is PER LANE when it is not. Staged in exactly the state where the old wording was FALSE —
  # an explicitly named lock, which may be global and is not per-lane — with a legacy lock present.
  explicit="$tmp/operator-named-global.lock"
  mkdir -p "$legacy"
  printf '%s\n' "1" >"$legacy/pid"
  out="$(legacy_lock_drive "$tmp" "$lane" "$explicit")"; rc=$?
  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out"; then
    pass "scope-declared PREMISE (refusal): an explicitly-named lock is still refused over the present legacy lock, which is the state where 'our lock is PER LANE' was false"
  else
    fail "scope-declared-premise-refusal: rc=$rc out=[$out]"
  fi
  if [[ "$out" != *"is PER LANE"* ]] && [[ "$out" == *"$explicit"* ]]; then
    pass "scope-declared (F2): the refusal describes the RESOLVED lock path ($explicit) instead of asserting it is per-lane — a claim that is false for exactly this run"
  else
    fail "scope-declared-per-lane: out=[$out] — the refusal must not claim our lock is PER LANE when the caller named it (#3549 job 222 F2)"
  fi
  rm -rf "$legacy" "$explicit" "$derived"

  # ---- (4) STRUCTURAL, at the guard: the SPATIAL gap is recorded beside the temporal one, with why it
  # cannot be closed and why extra probes were rejected. Same shape as the RESIDUAL pin above — the
  # RECORD, not its prose — because a declaration that lives only in emitted text is one refactor away
  # from being deleted as a stray log line.
  block="$(sed -n '/^# RESIDUAL (#3549/,/^supervisor_legacy_lock_guard()/p' "$SUPERVISOR")"
  if [[ -n "$block" ]] && [[ "$block" == *"SPATIAL"* ]] && [[ "$block" == *"#3596"* ]] \
     && [[ "$block" == *"UNKNOWABLE"* || "$block" == *"unknowable"* ]] \
     && [[ "$block" == *"REJECTED"* ]]; then
    pass "scope-declared STRUCTURAL (code): the RESIDUAL block records the SPATIAL gap beside the temporal #3596 one, that another process's environment is unknowable, and that extra probes were REJECTED"
  else
    fail "scope-declared-residual: the RESIDUAL block does not record the spatial gap, its unknowable input and the rejection of extra probes (#3549 job 222 F1)"
  fi

  # ---- (5) STRUCTURAL, in the runbook: the same declaration in operator language, AND TIED TO THE CODE
  # by quoting the emitted fragment. That tie is the point: a doc that paraphrases can drift silently,
  # while a doc that quotes the emitted text fails here the moment the line is reworded — which is the
  # failure mode this issue has already hit three times.
  #
  # THE EMITTED FRAGMENT IS MATCHED CASE-EXACTLY AND THE ARGUMENT TOKENS ARE NOT, deliberately: the
  # first is a QUOTATION and any difference from the shipped bytes is exactly what this half exists to
  # catch, while the last two are CLAIMS whose capitalisation is prose (measured: the runbook writes
  # "**rejected**" in a sentence and "REJECTED" nowhere, so a case-exact token here would have red on
  # correct text — the shape this file elsewhere calls the check people learn to waive).
  doc="$REPO_ROOT/docs/development/fleet-runbook.md"
  if [[ ! -r "$doc" ]]; then
    fail "scope-declared-runbook-premise: $doc is not readable; the scan has no subject"
    return 0
  fi
  if grep -qF 'legacy-lock check: nothing at' "$doc" \
     && grep -qiF "the check's reach" "$doc" \
     && grep -qiF 'rejected' "$doc"; then
    pass "scope-declared STRUCTURAL (runbook): the runbook declares the check's reach, quotes the emitted line verbatim (so a reworded line reds this), and records that extra probes were rejected"
  else
    fail "scope-declared-runbook: docs/development/fleet-runbook.md does not carry the reach declaration tied to the emitted line (#3549 job 222 F1)"
  fi

  rm -rf "$tmp"
}

t test_legacy_global_lock_residual_recorded
t test_legacy_lock_container_needs_search_not_read
t test_legacy_lock_scope_is_declared




# ---------------------------------------------------------------------------
# Test 44-lock (#3549, roborev job 185 F2, carried through the lead's second ruling): THE ONE
# OPERATOR-FACING LINE IS EXECUTABLE AS PRINTED, and no diagnostic line mixes a runnable command with
# prose.
#
# The original defect: `rm -f <dir>/pid && rmdir <dir> — the shape was verified ...` was printed as ONE
# line. Pasted, the em dash and the clause after it become extra operands, so `rm -f` SUCCEEDS and
# `rmdir` FAILS, leaving a PID-LESS lock directory — precisely the shape a pre-#3467 supervisor reads as
# stale and reclaims. The remedy would have manufactured the hazard the guard exists to prevent, which
# is why the property is executability of the RAW LINE and not the presence of a command somewhere in
# the text.
#
# WHAT THE RULING CHANGED HERE: the line is now a READ-ONLY INSPECTION
# (`ls -ldn -- <p> && ls -lna -- <p>`), not a deletion, because the guard no longer inspects the object
# and so cannot license destroying it. The emission-layer properties are UNCHANGED and still the subject:
#   (a) no `worker-supervisor:`-prefixed line contains a command token — a command inside prose is by
#       construction not pasteable, wherever it appears;
#   (b) the command is the WHOLE of exactly one bare line, and running that line verbatim does what the
#       prose says — here: reports what is at the path, and changes nothing.
# The state with no command at all (the probe could not answer) must print NO bare line — asserted too,
# so "zero bare lines" cannot be confused with "the command went missing".
# ---------------------------------------------------------------------------

# `remedy_lines_structural` — the (a) half plus the bare-line count — is DEFINED WITH THE CONSTANTS IT
# CONSUMES (beside `SV_CMD_RE`), not here (#3549, roborev job 220). It is called from five cases across
# TWO `t` blocks, and one of those blocks runs ABOVE this point: a definition here had been reached only
# by the later block, so the earlier call was a `command not found` whose status bash discards — the
# exact vacuous-pass shape `t()` exists to catch, walking past `t()` because the callee is a HELPER and
# not a test. A shared helper therefore lives above every `t` invocation that can reach it.

test_legacy_lock_remedy_lines_are_executable_as_printed() {
  local d tmp lane legacy derived out rc bare dead
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"; lane="lane3549remedy$$"
  mkdir -p "$tmp"
  legacy="$tmp/cqlite-worker-supervisor.lock"
  derived="$tmp/cqlite-worker-supervisor-$lane.lock"

  fixture_bg sleep 0.1
  dead=$FIXTURE_LAST_PID
  fixture_wait "$dead"

  # ---- (1) PRESENT: exactly ONE bare line, and it is the whole command.
  mkdir -p "$legacy"
  printf '%s\n' "$dead" >"$legacy/pid"
  out="$(legacy_lock_drive "$tmp" "$lane")"; rc=$?
  remedy_lines_structural "present" "$out" 1
  bare="$(printf '%s\n' "$out" | grep -vE "$SV_DIAG_RE" | head -1)"
  local qp
  printf -v qp '%q' "$legacy"
  if [[ "$bare" == "ls -ldn -- $qp && ls -lna -- $qp" ]]; then
    pass "remedy-lines (present): the bare line is EXACTLY the read-only inspection command, with no trailing prose or punctuation appended"
  else
    fail "remedy-lines-present-not-exact: line=[$bare], expected exactly [ls -ldn -- $qp && ls -lna -- $qp]"
  fi
  # EXECUTED AS PRINTED — the whole line, nothing stripped — AND IT MUTATES NOTHING. Both halves matter:
  # a read-only remedy that fails is useless, and one that changes state is the thing the ruling forbids.
  local vrc=0
  eval "$bare" >/dev/null 2>&1 || vrc=$?
  if [[ "$vrc" -eq 0 && -d "$legacy" && -f "$legacy/pid" && "$(cat "$legacy/pid")" == "$dead" ]]; then
    pass "remedy-lines (present): the bare line runs VERBATIM (rc=0) and the legacy lock is byte-identical afterwards — the printed remedy only READS"
  else
    fail "remedy-lines-present-not-runnable: rc=$vrc dir=$([[ -d "$legacy" ]] && echo yes || echo GONE) pid=[$(cat "$legacy/pid" 2>/dev/null || echo GONE)] line=[$bare]"
  fi
  # ...AND IT IS NOT A DELETION. Asserted by name, because a deletion is what used to be here and what
  # the symlink case measures as destructive on an uninspected object.
  if [[ "$bare" != *"rm "* && "$bare" != *"rmdir"* && "$bare" != *"mv "* && "$bare" != *">"* ]]; then
    pass "remedy-lines (present): the printed line carries no rm, rmdir, mv or redirection — a mis-paste can only ever be uninformative"
  else
    fail "remedy-lines-present-destructive: line=[$bare] — the printed line can change state"
  fi
  # An em dash on the command line is the exact byte that broke the paste, so it is asserted by name.
  if [[ "$bare" != *"—"* && "$bare" != *"#"* && "$bare" == "ls -ldn -- "* ]]; then
    pass "remedy-lines (present): the bare line begins with the command and carries no em dash and no comment marker"
  else
    fail "remedy-lines-present-line-shape: line=[$bare]"
  fi
  # THE PRECONDITION IS FIRST (#3549, roborev job 185 F1) and the run makes NO safety claim.
  if [[ "$out" == *"PRECONDITION FIRST"* ]] \
     && [[ "$out" == *"has established no such thing"* ]] \
     && [[ "$out" != *"fails loudly if anything unexpected is present"* ]]; then
    pass "remedy-lines (present, F1): the precondition is stated FIRST and the guard states it has established nothing about the object"
  else
    fail "remedy-lines-present-precondition: out=[$out] — expected the precondition first and an explicit no-claim statement"
  fi

  # NON-VACUITY, AND THE INLINED-PROSE FINDING REPRODUCED — MEASURED. The pre-fix form appended prose to
  # this same line; pasted, the prose becomes extra `ls` OPERANDS and the command FAILS naming them, so
  # the operator cannot tell what was reported about what. Read-only either way, which is the point: the
  # failure mode is now MISINFORMATION rather than destruction, and it is still a failure.
  local emdash_rc=0 emdash_err=""
  emdash_err="$(eval "$bare — the shape was verified to be exactly that one file" 2>&1 >/dev/null)" || emdash_rc=$?
  if [[ "$emdash_rc" -ne 0 && "$emdash_err" == *"shape"* ]]; then
    pass "remedy-lines NON-VACUITY (present, em dash): the pre-fix INLINED form, pasted, FAILS (rc=$emdash_rc) naming the prose words — so the whole-line contract is load-bearing"
  else
    fail "remedy-lines-oldform-emdash: rc=$emdash_rc err=[$emdash_err] — the pre-fix form must be shown to break, or the assert above measures nothing"
  fi
  rm -rf "$legacy" "$derived"

  # ---- (2) OPTION-SHAPED, RELATIVE `TMPDIR` (#3549, roborev job 192 F2). QUOTING IS NOT OPTION-SAFETY:
  # `supervisor_shell_quote` stops word-splitting, globbing and metacharacters, and does nothing about
  # option parsing, because `ls` reads a leading `-` in an operand as flags whatever quoting produced it.
  # `TMPDIR=-scratch` is a legitimate (if exotic) configuration and the guard itself handles it — every
  # internal operand it builds reaches `[[ ]]`, which does not parse options — so the only thing that
  # breaks is the line the operator was told to run.
  local optdir="$d/optshaped" opttmp="-scratch" optlegacy
  mkdir -p "$optdir/$opttmp"
  optlegacy="$optdir/$opttmp/cqlite-worker-supervisor.lock"
  mkdir -p "$optlegacy"
  printf '%s\n' "$dead" >"$optlegacy/pid"
  out="$(legacy_lock_drive_in "$optdir" "$opttmp" "$lane")"; rc=$?
  remedy_lines_structural "present-option-shaped-tmpdir" "$out" 1
  bare="$(printf '%s\n' "$out" | grep -vE "$SV_DIAG_RE" | head -1)"
  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out"; then
    pass "remedy-lines (option-shaped TMPDIR): the guard still detects and REFUSES with a relative, option-shaped TMPDIR — its own internal operands are option-safe"
  else
    fail "remedy-lines-optshaped-no-refusal: rc=$rc out=[$out] — the guard must work for a relative option-shaped TMPDIR, not only its printed remedy"
  fi
  local opt_rc=0 opt_out=""
  opt_out="$( cd "$optdir" && eval "$bare" 2>/dev/null )" || opt_rc=$?
  if [[ "$opt_rc" -eq 0 && "$opt_out" == *"cqlite-worker-supervisor.lock"* ]]; then
    pass "remedy-lines (option-shaped TMPDIR): the bare line runs VERBATIM from that directory and reports the path (line=[$bare])"
  else
    fail "remedy-lines-optshaped-not-runnable: rc=$opt_rc out=[$opt_out] line=[$bare] — the printed remedy is not executable as printed for an option-shaped TMPDIR"
  fi
  # NON-VACUITY: the SAME line with `--` stripped — the pre-F2 emission — must FAIL on this very case,
  # or the assert above would pass for a path shape that never exercised option parsing.
  local optbare_nodash="${bare//-- /}" optnodash_rc=0 optnodash_err=""
  optnodash_err="$( cd "$optdir" && eval "$optbare_nodash" 2>&1 >/dev/null )" || optnodash_rc=$?
  if [[ "$optnodash_rc" -ne 0 && "$optnodash_err" == *option* ]]; then
    pass "remedy-lines NON-VACUITY (option-shaped TMPDIR): with \`--\` stripped the identical line FAILS on an invalid option (err=[$optnodash_err]) — so the \`--\` in the shipped line is load-bearing, not decoration"
  else
    fail "remedy-lines-optshaped-nodashdash: rc=$optnodash_rc err=[$optnodash_err] — the pre-F2 form must be shown to break here, or the assert above measures nothing"
  fi
  rm -rf "$optdir"

  # ---- (3) NEWLINE-CONTAINING `TMPDIR` (#3549, roborev job 198 F4). A newline survives SINGLE QUOTING
  # LITERALLY, so the pre-fix rendering split the printed command across two physical lines — and split
  # the DIAGNOSTIC lines too, leaving prose fragments with no `worker-supervisor:` prefix that are
  # indistinguishable from the one bare line an operator is told to select and paste.
  local nltmp="$d/nl${SV_LF}dir" nllegacy
  mkdir -p "$nltmp"
  nllegacy="$nltmp/cqlite-worker-supervisor.lock"
  mkdir -p "$nllegacy"
  printf '%s\n' "$dead" >"$nllegacy/pid"
  if [[ -d "$nltmp" && "$nltmp" == *"$SV_LF"* ]]; then
    pass "remedy-lines PREMISE (newline TMPDIR): a directory whose name contains a newline was staged, so the case below measures the real shape"
  else
    fail "remedy-lines-newline-premise: could not stage a newline-containing TMPDIR on this host; the case below would measure nothing"
    rm -rf "$nltmp"
    return 0
  fi
  out="$(legacy_lock_drive "$nltmp" "$lane")"; rc=$?
  remedy_lines_structural "present-newline-tmpdir" "$out" 1
  bare="$(printf '%s\n' "$out" | grep -vE "$SV_DIAG_RE" | head -1)"
  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out"; then
    pass "remedy-lines (newline TMPDIR): the guard still detects and REFUSES with a newline in TMPDIR"
  else
    fail "remedy-lines-newline-no-refusal: rc=$rc out=[$out]"
  fi
  local nl_rc=0 nl_out=""
  nl_out="$(eval "$bare" 2>/dev/null)" || nl_rc=$?
  if [[ "$nl_rc" -eq 0 && "$nl_out" == *"cqlite-worker-supervisor.lock"* ]]; then
    pass "remedy-lines (newline TMPDIR): the ONE bare line runs VERBATIM and reports the path (line=[$bare])"
  else
    fail "remedy-lines-newline-not-runnable: rc=$nl_rc out=[$nl_out] line=[$bare]"
  fi
  # MUTANT CONTRAST: the SAME drive with `supervisor_shell_quote` restored to the pre-fix
  # single-quote-only form — one function replaced, everything else shipped. The newline then survives
  # literally in the command AND in the diagnostics, so the output carries MORE than one bare line and
  # the line an operator would select does not do the job.
  local ovr="$d/quote-override.sh" mout mbare mbare_count mrc=0
  cat >"$ovr" <<'OVERRIDE'
# The pre-#3549-job-198-F4 rendering: single quotes only. A newline is preserved LITERALLY.
supervisor_shell_quote() {
  local s="${1//\'/\'\\\'\'}"
  printf "'%s'" "$s"
}
OVERRIDE
  if [[ "$SV_DRIVE_BODY_OVERRIDE" != "$SV_DRIVE_BODY" && "$SV_DRIVE_BODY_OVERRIDE" == *'source "$2"'* ]]; then
    pass "remedy-lines PREMISE (mutant drive): the override drive body is the shipped body plus exactly one extra source, so the mutant differs from the ordinary drive in ONE function"
  else
    fail "remedy-lines-newline-mutant-drive: the override drive body was not derived from the shipped one ([$SV_DRIVE_BODY_OVERRIDE])"
  fi
  mout="$(legacy_lock_drive_override "$ovr" "$nltmp" "$lane")" || true
  mbare_count="$(printf '%s\n' "$mout" | grep -cvE "$SV_DIAG_RE" || true)"
  mbare="$(printf '%s\n' "$mout" | grep -vE "$SV_DIAG_RE" | head -1)"
  eval "$mbare" >/dev/null 2>&1 || mrc=$?
  if [[ "$mbare_count" =~ ^[0-9]+$ ]] && [[ "$mbare_count" -gt 1 ]] && [[ "$mrc" -ne 0 ]]; then
    pass "remedy-lines MUTANT CONTRAST (newline TMPDIR): the single-quote-only rendering emits $mbare_count bare lines instead of 1 — the newline split the command AND the diagnostic paths — and the line an operator would select FAILS (rc=$mrc, line=[$mbare])"
  else
    fail "remedy-lines-newline-mutant: bare=$mbare_count rc=$mrc line=[$mbare] out=[$mout] — the pre-fix form must be shown to break, or the assert above measures nothing"
  fi
  rm -rf "$nltmp"

  # ---- (4) THE PROBE COULD NOT ANSWER: no inspection command, so no bare line either. Staged with a
  # container that does not exist, which needs no permission bits and therefore works as any user.
  out="$(legacy_lock_drive "$d/no-such-container" "$lane")"; rc=$?
  remedy_lines_structural "could-not-tell" "$out" 0
  if [[ "$rc" -ne 0 ]] && [[ "$out" == *"EXISTENCE PROBE FAILED"* ]]; then
    pass "remedy-lines (could-not-tell): the probe-failed refusal prints NO bare line — there is nothing to inspect, so no command is offered"
  else
    fail "remedy-lines-couldnottell: rc=$rc out=[$out]"
  fi
  rm -rf "$derived"
}




# ---------------------------------------------------------------------------
# Test 48-lock (#3549, roborev job 201 F1): EVERY DYNAMIC VALUE THAT REACHES EMITTED TEXT IS RENDERED,
# AND THE **EMITTER** IS WHAT GUARANTEES IT.
#
# THE CLASS, THIRD INSTANCE. The refusal's contract is "select the ONE bare line and paste it":
# diagnostics carry a `worker-supervisor:` prefix, the runnable command carries none. A control
# character in a dynamic value breaks that by three different mechanisms — LF splits the line and the
# second half has no prefix; CR returns the cursor to column 0 so the text after it OVERWRITES the
# prefix; ESC (`ESC[G`) repositions the cursor with no newline in the bytes at all — and each forges
# something an operator cannot tell from the command they were told to run. It was fixed at the command
# line (job 185 F2), then at the diagnostic PATHS (job 198 F4), and re-appeared inside a STATE
# DESCRIPTION, which reaches the emitter as `$detail`.
#
# THE SUBJECT SET SHRANK WITH THE CLASSIFIER (#3549, lead ruling), AND ONE MEMBER IS LEFT. The pid
# file's bytes and an unexpected entry's NAME used to reach emitted text too, and each had its own
# rendered channel and its own case here; the guard no longer READS the lock, so neither value exists
# any more and both cases went with the parsing. What remains is the value that is still
# operator-controlled and still interpolated: the `TMPDIR`-derived PATH, staged below with a newline AND
# an ESC in it, on the one branch that reports a container (`container-not-searchable`).
#
# THE MUTANTS ARE THREE, NOT ONE, because there are now TWO layers and a single mutant cannot tell a
# redundant layer from a load-bearing one: the pre-fix WORLD (both reverted) must break, and each layer
# ALONE must hold. That distinction is the whole claim of the fix — the emitter renders, so no caller
# can break the contract — and asserting it costs two extra drives.
# ---------------------------------------------------------------------------
test_legacy_lock_emitted_dynamic_values_are_rendered() {
  local d lane out rc dead esc nltmp ovr n
  d="$(new_case_dir)"
  common_env "$d"
  lane="lane3549rend$$"
  esc=$'\033'

  # A REAL reaped pid: the staged lock is the ordinary shape, so nothing below refuses for an unrelated
  # reason. Its VALUE no longer reaches any diagnostic (the guard does not read the lock), which is why
  # the only remaining dynamic value in emitted text is the `TMPDIR`-derived path.
  fixture_bg sleep 0.1
  dead=$FIXTURE_LAST_PID
  fixture_wait "$dead"

  # ---- (a) THE UNSEARCHABLE CONTAINER, with a NEWLINE **and** an ESC in `TMPDIR`.
  if [[ "$(id -u)" == "0" ]]; then
    # As root a 0000 directory is still searchable, so this state cannot be staged and a green result
    # would measure nothing. Explicitly a non-result, never a pass.
    skip "legacy-lock render (a): running as root, where a 0000 directory is still searchable — the unsearchable-container state cannot be staged"
  else
    nltmp="$d/rend${SV_LF}${esc}[Gdir"
    mkdir -p "$nltmp/cqlite-worker-supervisor.lock"
    printf '%s\n' "$dead" >"$nltmp/cqlite-worker-supervisor.lock/pid"
    chmod 000 "$nltmp"
    if [[ -d "$nltmp" && ! -x "$nltmp" && "$nltmp" == *"$SV_LF"* && "$nltmp" == *"$esc"* ]]; then
      pass "legacy-lock render PREMISE (a): an UNSEARCHABLE container whose name carries a newline AND an ESC was staged, so the case below measures the real shape"
    else
      fail "legacy-lock-render-premise-a: dir=$([[ -d "$nltmp" ]] && echo yes || echo no) searchable=$([[ -x "$nltmp" ]] && echo yes || echo no) — the container state cannot be staged, so nothing below is attributable"
    fi
    out="$(legacy_lock_drive "$nltmp" "$lane")"; rc=$?
    remedy_lines_structural "could-not-tell-container-not-searchable" "$out" 0
    if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" && [[ "$out" == *"container-not-searchable:"* ]]; then
      pass "legacy-lock render (a): an unsearchable container REFUSES and names the cause (container-not-searchable), with a newline and an ESC in the path"
    else
      fail "legacy-lock-render-a: rc=$rc out=[$out] — expected a legacy refusal naming container-not-searchable"
    fi
    # The bare-line count above covers LF (a raw one manufactures an unprefixed line). CR and ESC do not
    # add a LINE, they forge one on a TERMINAL, so they are asserted as BYTES: neither may survive into
    # emitted text.
    if [[ "$out" != *"$esc"* && "$out" != *"$SV_CR"* ]]; then
      pass "legacy-lock render (a): no raw ESC and no raw CR survives into the emitted refusal — the cursor cannot be moved by a TMPDIR"
    else
      fail "legacy-lock-render-a-controls: a raw control byte survived into the output: [$out]"
    fi
    chmod 0755 "$nltmp"

    # ---- M1: THE PRE-FIX WORLD — the state string interpolates the container path RAW **and** the
    # emitter does not render its prose. Both, in one override, because either layer alone holds.
    ovr="$d/m-prefix-world.sh"; : >"$ovr"
    if sv_mutant_override "$ovr" supervisor_legacy_lock_presence '$(supervisor_shell_quote "$dir")' '$dir' \
       && sv_mutant_override "$ovr" supervisor_legacy_lock_refuse 'detail="$(supervisor_one_line "$detail")" || true' 'detail="$detail"'; then
      chmod 000 "$nltmp"
      out="$(legacy_lock_drive_override "$ovr" "$nltmp" "$lane")" || true
      chmod 0755 "$nltmp"
      n="$(printf '%s\n' "$out" | grep -cvE "$SV_DIAG_RE" || true)"
      if [[ "$n" =~ ^[0-9]+$ ]] && [[ "$n" -gt 0 ]]; then
        pass "legacy-lock render MUTANT CONTRAST (a, PRE-FIX WORLD): with the state value raw AND the emitter not rendering, the refusal emits $n unprefixed line(s) — fragments an operator cannot tell from the runnable command"
      else
        fail "legacy-lock-render-mutant-world: $n bare lines out=[$out] — the pre-fix form must be shown to break, or the assert above measures nothing"
      fi
    fi

    # ---- M2: the STATE VALUE raw, emitter rendering INTACT. The contract must still hold — this is the
    # structural claim: a caller that interpolates raw text cannot break the emitted output.
    ovr="$d/m-state-only.sh"; : >"$ovr"
    if sv_mutant_override "$ovr" supervisor_legacy_lock_presence '$(supervisor_shell_quote "$dir")' '$dir'; then
      chmod 000 "$nltmp"
      out="$(legacy_lock_drive_override "$ovr" "$nltmp" "$lane")" || true
      chmod 0755 "$nltmp"
      n="$(printf '%s\n' "$out" | grep -cvE "$SV_DIAG_RE" || true)"
      if [[ "$n" == 0 ]]; then
        pass "legacy-lock render (a) CHOKE POINT: with the state value interpolated RAW, the EMITTER still holds the one-line contract (0 unprefixed lines) — no caller can break it"
      else
        fail "legacy-lock-render-chokepoint: $n bare lines with only the state value reverted; the emitter must render whatever it is handed. out=[$out]"
      fi
    fi

    # ---- M3: the emitter's rendering removed, the STATE VALUE rendered. Also holds — so the two are
    # genuinely independent layers and neither assert above is passing on the other's account.
    ovr="$d/m-emitter-only.sh"; : >"$ovr"
    if sv_mutant_override "$ovr" supervisor_legacy_lock_refuse 'detail="$(supervisor_one_line "$detail")" || true' 'detail="$detail"'; then
      chmod 000 "$nltmp"
      out="$(legacy_lock_drive_override "$ovr" "$nltmp" "$lane")" || true
      chmod 0755 "$nltmp"
      n="$(printf '%s\n' "$out" | grep -cvE "$SV_DIAG_RE" || true)"
      if [[ "$n" == 0 ]]; then
        pass "legacy-lock render (a) SITE RENDER: with the emitter's rendering removed, the state string's own rendering still holds the contract (0 unprefixed lines) — the two layers are independent"
      else
        fail "legacy-lock-render-site: $n bare lines with only the emitter reverted. out=[$out]"
      fi
    fi
    rm -rf "$nltmp"
  fi
}


# ---------------------------------------------------------------------------
# Test 50-lock (#3549, roborev job 201 F3): THE PROBE SURVIVES A CALLER THAT PROPAGATES ERREXIT, AND THE
# GUARD'S FAIL-CLOSED FALLBACK IS LOAD-BEARING.
#
# `inherit_errexit` is what makes a caller's `set -e` reach INSIDE the `$( )` the guard wraps the probe
# in. The original defect was a `shopt -p` capture (non-zero whenever an option is disabled, i.e. every
# run) aborting BETWEEN the mutation and the restore: the caller's globbing was left CHANGED and no
# refusal was printed at all — a supervisor that neither started nor said why. The `shopt` block is
# DELETED with the classifier, so the probe now has no abort site by construction. That is a weaker
# claim than "the abort is fixed", and only it is true — so what is measured here is the property that
# still has teeth: whatever goes wrong inside the probe, the guard REFUSES rather than dying silently.
#
# TWO LEVELS, because they answer different questions:
#   - THE PROBE, called directly through a BARE assignment under `set -e` + `inherit_errexit`: it
#     ANSWERS, and it leaves the caller's shell as it found it.
#   - THE GUARD: a probe forced to exit non-zero produces the `could-not-tell` REFUSAL, and with the
#     fallback removed the same mutant kills the supervisor silently. That contrast is the fallback's
#     entire value.
# ---------------------------------------------------------------------------
test_legacy_lock_classifier_survives_inherit_errexit() {
  local d lane legacy out rc dead ovr got
  d="$(new_case_dir)"
  common_env "$d"
  lane="lane3549errx$$"
  legacy="$d/tmp/cqlite-worker-supervisor.lock"

  fixture_bg sleep 0.1
  dead=$FIXTURE_LAST_PID
  fixture_wait "$dead"
  mkdir -p "$legacy"
  printf '%s\n' "$dead" >"$legacy/pid"

  # PREMISE: the drive bodies are DERIVED from the shipped ones, and this bash can set the option. A
  # host that cannot is an environmental non-result, not a pass.
  if [[ "$SV_DRIVE_BODY_ERREXIT" != "$SV_DRIVE_BODY" && "$SV_DRIVE_BODY_ERREXIT" == *inherit_errexit* ]]; then
    pass "legacy-lock errexit PREMISE: the errexit drive body is the shipped body plus exactly one statement, so it exercises the same startup path"
  else
    fail "legacy-lock-errexit-premise: the errexit drive body was not derived from the shipped one ([$SV_DRIVE_BODY_ERREXIT])"
  fi
  out="$(legacy_lock_drive_errexit "$d/tmp" "$lane")"; rc=$?
  if [[ "$out" == *ERREXIT_UNAVAILABLE* ]]; then
    skip "legacy-lock errexit: this bash cannot set inherit_errexit (pre-4.4), so an errexit-propagating caller cannot be staged"
    return 0
  fi

  # ---- THE GUARD, under errexit: the presence refusal is the ordinary one, command line and all.
  remedy_lines_structural "errexit-present" "$out" 1
  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" && [[ "$out" == *"EXISTS there"* ]]; then
    pass "legacy-lock errexit: with the caller propagating errexit into the probe's substitution, the guard still detects and prints the full presence refusal with its ONE inspection line"
  else
    fail "legacy-lock-errexit-guard: rc=$rc out=[$out] — an errexit-propagating caller must get the same refusal"
  fi

  # ---- THE PROBE, directly, bare assignment: it answers, and it leaves the caller's shell as it found
  # it. `dotglob`/`nullglob` are reported because the DELETED classifier changed them; the probe must
  # not, and reporting them keeps that regression visible if anything ever pins an option here again.
  got="$(legacy_presence_drive_errexit "$legacy")"
  if [[ "$got" == *"STATE=[present]"* ]] \
     && [[ "$got" == *"OPTS=[shopt -u dotglob|shopt -u nullglob]"* ]]; then
    pass "legacy-lock errexit: called directly from a shell with set -e AND inherit_errexit, the probe ANSWERS (present) and leaves dotglob/nullglob at the caller's values"
  else
    fail "legacy-lock-errexit-direct: got=[$got] — expected STATE=[present] and both options unchanged"
  fi

  # ---- THE FALLBACK, BOTH DIRECTIONS. A probe forced to exit non-zero is the shape the fallback exists
  # for; nothing in the shipped probe can do that today, so it is INJECTED, and the injection is stated
  # rather than disguised as a natural failure.
  ovr="$d/m-probe-fails.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" supervisor_legacy_lock_presence \
       "  printf 'present\\n'" "  printf 'present\\n'; return 1"; then
    out="$(legacy_lock_drive_errexit_override "$ovr" "$d/tmp" "$lane")"; rc=$?
    if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" && [[ "$out" == *"presence-probe-exited-nonzero"* ]]; then
      pass "legacy-lock errexit (fail-closed): a probe that exits non-zero produces the could-not-tell REFUSAL naming presence-probe-exited-nonzero — never a silent non-start and never a start"
    else
      fail "legacy-lock-errexit-fallback: rc=$rc out=[$out] — expected the fail-closed refusal"
    fi

    # ...AND WITH THE FALLBACK REMOVED the same mutant kills the supervisor with no message at all —
    # the silent death the fallback converts into a refusal.
    ovr="$d/m-probe-fails-nofallback.sh"; : >"$ovr"
    if sv_mutant_override "$ovr" supervisor_legacy_lock_presence \
         "  printf 'present\\n'" "  printf 'present\\n'; return 1" \
       && sv_mutant_override "$ovr" supervisor_legacy_lock_guard \
            'state="$(supervisor_legacy_lock_presence "$legacy")" || state="could-not-tell presence-probe-exited-nonzero"' \
            'state="$(supervisor_legacy_lock_presence "$legacy")"'; then
      out="$(legacy_lock_drive_errexit_override "$ovr" "$d/tmp" "$lane")"; rc=$?
      if [[ "$rc" -ne 0 ]] && [[ "$out" != *"LEGACY GLOBAL supervisor lock"* ]] && [[ "$out" != *ACQUIRED=* ]]; then
        pass "legacy-lock errexit MUTANT CONTRAST (fallback): with the guard's fallback removed, the same failing probe ends the supervisor rc=$rc having printed no refusal — the silent death the fallback prevents"
      else
        fail "legacy-lock-errexit-nofallback: rc=$rc out=[$out] — expected a silent non-start, or the fallback's value is not established"
      fi
    fi
  fi

  rm -rf "$d/tmp"
}

t test_legacy_lock_remedy_lines_are_executable_as_printed
t test_legacy_lock_emitted_dynamic_values_are_rendered
t test_legacy_lock_classifier_survives_inherit_errexit












# ---------------------------------------------------------------------------
# Test 47g (#3549, roborev job 205 F2): THE EMITTER MUST NOT UNDO THE RENDERERS.
#
# THE DEFECT. `supervisor_one_line` and `supervisor_shell_quote` encode a control character AS A
# BACKSLASH SEQUENCE — that is what keeps a diagnostic on ONE PHYSICAL LINE and the runnable command
# identifiable as the ONE bare (unprefixed) line. `echo` under bash's `xpg_echo` option INTERPRETS
# backslash sequences, so the last step turns every `\n` the renderers produced back into a real
# newline: the renderers are correct and their guarantee is thrown away at the emitter. The result is
# prose fragments with no `worker-supervisor:` prefix — indistinguishable from the command line an
# operator is told to select and paste.
#
# IT IS INHERITED STATE: `env BASHOPTS=xpg_echo` is imported by bash (measured), so nothing in this file
# needs to have run `shopt` for the option to be on. Set in the DRIVER; the shipped script gets no knob.
# ---------------------------------------------------------------------------
test_legacy_lock_diagnostics_survive_xpg_echo() {
  local d nltmp lane legacy dead out rc bare ovr mout mbare mbare_count mrc=0
  d="$(new_case_dir)"
  common_env "$d"
  lane="lane3549xpg$$"
  # A newline in `TMPDIR` is what makes the renderers actually PRODUCE a backslash sequence: with an
  # ordinary path there is nothing to interpret and the case would be vacuous under either emitter.
  nltmp="$d/nl${SV_LF}dir"
  mkdir -p "$nltmp"
  legacy="$nltmp/cqlite-worker-supervisor.lock"
  if [[ -d "$nltmp" && "$nltmp" == *"$SV_LF"* ]]; then
    pass "xpg_echo PREMISE: a TMPDIR containing a newline was staged, so the renderers emit a backslash sequence for the emitter to (mis)interpret"
  else
    fail "xpg_echo-premise: could not stage a newline-containing TMPDIR on this host; the case below would measure nothing"
    return 0
  fi
  # The option really is inherited from the environment — measured here rather than assumed, because an
  # inert BASHOPTS would make every assertion below pass against the UNFIXED emitter.
  local xpg_probe
  xpg_probe="$(env BASHOPTS=xpg_echo bash -c "shopt -p xpg_echo" 2>&1)"
  if [[ "$xpg_probe" == "shopt -s xpg_echo" ]]; then
    pass "xpg_echo PREMISE: \`env BASHOPTS=xpg_echo\` is imported by bash ([$xpg_probe]), so the drives below really do run with the option on"
  else
    fail "xpg_echo-premise-import: BASHOPTS=xpg_echo did not reach the shell ([$xpg_probe]) — the case below would measure nothing"
    return 0
  fi

  fixture_bg sleep 0.1
  dead=$FIXTURE_LAST_PID
  fixture_wait "$dead"
  mkdir -p "$legacy"
  printf '%s\n' "$dead" >"$legacy/pid"
  out="$(legacy_lock_drive_env "$nltmp" "$lane" BASHOPTS=xpg_echo)"; rc=$?
  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out"; then
    pass "xpg_echo: the guard still classifies and REFUSES with xpg_echo inherited"
  else
    fail "xpg_echo-no-refusal: rc=$rc out=[$out]"
  fi
  remedy_lines_structural "xpg_echo-present" "$out" 1
  bare="$(printf '%s\n' "$out" | grep -vE "$SV_DIAG_RE" | head -1)"
  local nl_rc=0 nl_out=""
  nl_out="$(eval "$bare" 2>/dev/null)" || nl_rc=$?
  if [[ "$nl_rc" -eq 0 && "$nl_out" == *"cqlite-worker-supervisor.lock"* ]]; then
    pass "xpg_echo: the ONE bare line still runs VERBATIM under an inherited xpg_echo and reports the path (line=[$bare])"
  else
    fail "xpg_echo-not-runnable: rc=$nl_rc out=[$nl_out] line=[$bare]"
  fi

  # MUTANT CONTRAST: the shipped refusal with all five `printf '%s\n'` emissions restored to `echo` —
  # ONE function replaced, everything else shipped, and the SAME inherited option. The rendered `\n`
  # sequences are then interpreted back into physical newlines, so the output carries MORE bare lines
  # than the one the contract promises.
  ovr="$d/m-xpg-echo.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" supervisor_legacy_lock_refuse \
       "printf '%s\\n' \"worker-supervisor" 'echo "worker-supervisor' 5; then
    mout="$(legacy_lock_drive_env_override "$ovr" "$nltmp" "$lane" BASHOPTS=xpg_echo)" || true
    mbare_count="$(printf '%s\n' "$mout" | grep -cvE "$SV_DIAG_RE" || true)"
    mbare="$(printf '%s\n' "$mout" | grep -vE "$SV_DIAG_RE" | head -1)"
    eval "$mbare" >/dev/null 2>&1 || mrc=$?
    if [[ "$mbare_count" =~ ^[0-9]+$ ]] && [[ "$mbare_count" -gt 1 ]] && [[ "$mrc" -ne 0 ]]; then
      pass "xpg_echo MUTANT CONTRAST: with \`echo\` restored, the identical drive emits $mbare_count bare lines instead of 1 — the emitter interpreted the renderers' escapes back into newlines — and the line an operator would select FAILS (rc=$mrc, line=[$mbare])"
    else
      fail "xpg_echo-mutant: bare=$mbare_count rc=$mrc line=[$mbare] out=[$mout] — the pre-fix emitter must be shown to break, or the assert above measures nothing"
    fi
  fi
  # ...and the same mutant is INVISIBLE without the inherited option, which is what makes this an
  # inherited-state defect rather than a rendering one: `echo` and `printf '%s\n'` agree when xpg_echo
  # is off, so no ordinary run could ever have exposed it.
  mout="$(legacy_lock_drive_override "$ovr" "$nltmp" "$lane")" || true
  mbare_count="$(printf '%s\n' "$mout" | grep -cvE "$SV_DIAG_RE" || true)"
  if [[ "$mbare_count" == 1 ]]; then
    pass "xpg_echo MUTANT CONTRAST (option off): the SAME \`echo\` mutant emits exactly 1 bare line with xpg_echo off — the defect is reachable ONLY through inherited state, which is why no ordinary run exposed it"
  else
    fail "xpg_echo-mutant-optionoff: $mbare_count bare lines with the option off, expected 1 — the two channels differ for some other reason and the contrast above is not about xpg_echo"
  fi

  rm -rf "$nltmp"
}

t test_legacy_lock_diagnostics_survive_xpg_echo

# ---------------------------------------------------------------------------
# Test 47b (#3549, roborev job 198 F2): THE REAP NEVER SIGNALS A GROUP IT NO LONGER OWNS.
#
# THE DEFECT, and it is the most dangerous thing this issue produced. The previous reap kept one
# HISTORICAL list of every pgid ever staged and sent TERM then KILL to all of them on every call —
# including groups already cleaned up per case, and including groups an earlier reap had already killed.
# A pgid is a PID NUMBER; pid numbers are REUSED; THIS BOX RUNS FOUR LANES. So the suite could deliver
# SIGKILL to an unrelated process GROUP — a sibling lane's supervisor, gate or worker. The leak fix had
# introduced something worse than the leak.
#
# HOW IT IS MEASURED WITHOUT SIGNALLING ANYTHING. The harness functions are extracted from THIS FILE at
# run time into a scratch driver (the technique used throughout this suite: the subject is the shipped
# code, never a re-implementation) and `kill` is overridden by a shell FUNCTION that LOGS its arguments
# instead of delivering a signal. So the property under test — WHICH GROUPS GET SIGNALLED — is read
# directly, and no process anywhere is touched. The pgids are synthetic numbers; the state each one
# reports is supplied by the stub.
#
# NO PROCFS DEPENDENCY (#3549, roborev job 198 F3): `fixture_leader_ident` returns EMPTY for these
# synthetic pgids on any host — on Linux because no such `/proc/<pid>/stat` exists, on a host without
# procfs because there is no procfs — so this case exercises the REGISTRY path, which is the one the
# finding is about, and it behaves identically on macOS.
# ---------------------------------------------------------------------------
test_fixture_reap_never_signals_a_disowned_group() {
  local d drv log out g1=2999901 g2=2999902 g3=2999903
  d="$(new_case_dir)"
  drv="$d/reapdrv.sh"
  log="$d/kill.log"
  {
    printf '%s\n' '#!/usr/bin/env bash'
    printf '%s\n' 'set -uo pipefail'
    printf '%s\n' 'KILL_LOG="$1"; DEAD_GROUPS="${2:-}"'
    # The stub: every invocation is logged; a `-0` probe answers from DEAD_GROUPS with the errno text
    # `fixture_group_state` reads, so a released group is released for the shipped reason.
    printf '%s\n' 'kill() {'
    printf '%s\n' '  printf "%s\n" "$*" >>"$KILL_LOG"'
    printf '%s\n' '  if [[ "${1:-}" == "-0" ]]; then'
    printf '%s\n' '    case " $DEAD_GROUPS " in *" ${2#-} "*) printf "%s\n" "bash: kill: (${2}) - No such process" >&2; return 1 ;; esac'
    printf '%s\n' '  fi'
    printf '%s\n' '  return 0'
    printf '%s\n' '}'
    printf '%s\n' 'sleep() { :; }'
    sed -n '/^fixture_leader_ident()/,/^}/p' "$SELF_FILE"
    sed -n '/^fixture_group_state()/,/^}/p' "$SELF_FILE"
    sed -n '/^fixture_release_unowned()/,/^}/p' "$SELF_FILE"
    sed -n '/^fixture_signal_owned()/,/^}/p' "$SELF_FILE"
    sed -n '/^fixture_unregister()/,/^}/p' "$SELF_FILE"
    sed -n '/^fixture_wait()/,/^}/p' "$SELF_FILE"
    sed -n '/^fixture_kill()/,/^}/p' "$SELF_FILE"
    sed -n '/^fixture_reap()/,/^}/p' "$SELF_FILE"
    printf '%s\n' 'FIXTURE_OWNED=("2999901|" "2999902|" "2999903|")'
    printf '%s\n' 'FIXTURE_FOREIGN=()'
    printf '%s\n' 'wait() { :; }'
    # (1) g1 is ALREADY GONE when the first reap runs; g2 and g3 are live.
    printf '%s\n' 'fixture_reap'
    # (2) g3 dies, and is THEN cleaned up per case, the shipped way. `fixture_kill` must signal nothing:
    #     the group is provably gone, so its number is no longer ours to signal.
    printf '%s\n' 'DEAD_GROUPS="$DEAD_GROUPS 2999903"'
    printf '%s\n' 'echo "--- PER-CASE KILL ---" >>"$KILL_LOG"'
    printf '%s\n' 'fixture_kill 2999903'
    # (3) ...and now EVERYTHING is gone. A later reap must signal NOTHING: this is the pgid-reuse
    #     window, where each of those numbers may already belong to another lane.
    printf '%s\n' 'DEAD_GROUPS="$DEAD_GROUPS 2999902"'
    printf '%s\n' 'echo "--- LATER REAP ---" >>"$KILL_LOG"'
    printf '%s\n' 'fixture_reap'
  } >"$drv"
  # THE HARNESS ITSELF MUST BE EXTRACTABLE, or the driver would test an empty file and pass vacuously.
  local fns
  fns="$(grep -c '^fixture_\(leader_ident\|group_state\|release_unowned\|signal_owned\|unregister\|wait\|kill\|reap\)() {$' "$drv" || true)"
  if [[ "$fns" == "8" ]]; then
    pass "reap-ownership PREMISE: all 8 harness functions were extracted from this file into the driver (the subject is the shipped harness, not a re-implementation)"
  else
    fail "reap-ownership-premise: extracted $fns/8 harness functions into the driver; the case below would measure nothing"
    return 0
  fi

  # ---- THE FIXED FORM. g1 was dead before the first reap, so it is RELEASED and never signalled.
  : >"$log"
  bash "$drv" "$log" "$g1" >/dev/null 2>&1 || true
  out="$(tr '\n' ';' <"$log")"
  if [[ "$out" != *"-TERM -$g1"* && "$out" != *"-KILL -$g1"* ]]; then
    pass "reap-ownership: a group that was ALREADY GONE at reap time is released and NEVER signalled — its pgid may already belong to another lane (log=[$out])"
  else
    fail "reap-ownership-dead-signalled: the reap signalled pgid $g1, which it had proven dead; log=[$out]"
  fi
  # NON-VACUITY: it is not simply signalling nothing. A live owned group DOES get TERM and then KILL.
  if [[ "$out" == *"-TERM -$g2"* && "$out" == *"-KILL -$g2"* ]]; then
    pass "reap-ownership NON-VACUITY: a LIVE owned group is still TERMed and KILLed, so the silence above is a decision and not a broken reap"
  else
    fail "reap-ownership-live-unsignalled: the reap did not signal the live owned pgid $g2; log=[$out]"
  fi
  # THE FINDING'S EXACT CASE: after everything has been reaped or cleaned up, a LATER reap signals
  # nothing at all. That is the window in which a recycled pgid belongs to somebody else.
  local later="${out##*--- LATER REAP ---;}"
  if [[ "$later" != *"-TERM -"* && "$later" != *"-KILL -"* ]]; then
    pass "reap-ownership: a LATER reap, after every group has been reaped or per-case cleaned, delivers NO signal to ANY historical pgid (later=[$later])"
  else
    fail "reap-ownership-later-signalled: a later reap signalled a group it no longer owns; later=[$later]"
  fi
  # And the per-case teardown path releases too: `fixture_kill` on a group that has already died signals
  # NOTHING — the probe runs, the group is released, and no signal is delivered to a number that may
  # already have been recycled. (The window between the two markers is that call and nothing else.)
  local percase="${out##*--- PER-CASE KILL ---;}"
  percase="${percase%%--- LATER REAP ---*}"
  if [[ "$percase" == *"-0 -$g3"* && "$percase" != *"-TERM -$g3"* && "$percase" != *"-KILL -$g3"* ]]; then
    pass "reap-ownership: fixture_kill PROBES a group that has since died and then signals NOTHING — the per-case teardown path releases as well (window=[$percase])"
  else
    fail "reap-ownership-percase: the per-case teardown window was [$percase]; expected a liveness probe for $g3 and no signal to it"
  fi

  # ---- THE SIGNAL-TIME INCARNATION REFUTATION, both directions. This is the defence-in-depth half:
  # a recorded leader that is ALIVE with a DIFFERENT procfs start time proves the pid NUMBER was reused,
  # so the group is released UNSIGNALLED rather than killed. It is refutation-only — it can prove a group
  # is not ours and is never required to prove that it is — because the orphan this reaps has a leader
  # that already exited.
  #
  # PROCFS IS THE ORACLE, SO A HOST WITHOUT PROCFS IS AN EXPLICIT SKIP, NOT A SILENT PASS (#3549, roborev
  # job 198 F3). macOS has no `/proc/<pid>/stat`, `fixture_leader_ident` correctly returns nothing there,
  # and the registry asserts above are what carry the property on such a host.
  if [[ ! -r "/proc/$$/stat" ]]; then
    skip "reap-ownership: the leader-incarnation refutation needs /proc/<pid>/stat and this host has no procfs — the registry-only asserts above are what hold the property here"
  else
    local idrv="$d/identdrv.sh" iout ipid ilog
    ilog="$d/ident-kill.log"
    {
      printf '%s\n' '#!/usr/bin/env bash'
      printf '%s\n' 'set -uo pipefail'
      printf '%s\n' 'KILL_LOG="$1"; MODE="$2"'
      # Every group reads LIVE here, so the ONLY thing that can stop a signal is the incarnation check.
      printf '%s\n' 'kill() { printf "%s\n" "$*" >>"$KILL_LOG"; return 0; }'
      printf '%s\n' 'sleep() { :; }'
      sed -n '/^fixture_leader_ident()/,/^}/p' "$SELF_FILE"
      sed -n '/^fixture_group_state()/,/^}/p' "$SELF_FILE"
      sed -n '/^fixture_signal_owned()/,/^}/p' "$SELF_FILE"
      printf '%s\n' 'FIXTURE_FOREIGN=()'
      printf '%s\n' 'me=$$'
      printf '%s\n' 'if [[ "$MODE" == match ]]; then FIXTURE_OWNED=("$me|$(fixture_leader_ident "$me")"); else FIXTURE_OWNED=("$me|1"); fi'
      printf '%s\n' 'fixture_signal_owned TERM'
      printf '%s\n' 'printf "PID=%s FOREIGN=[%s] OWNED=[%s]\n" "$me" "${FIXTURE_FOREIGN[*]:-}" "${FIXTURE_OWNED[*]:-}"'
    } >"$idrv"
    # (a) MISMATCHED recorded incarnation: the number was reused, so no signal is delivered.
    : >"$ilog"
    iout="$(bash "$idrv" "$ilog" mismatch 2>&1)"
    ipid="${iout#PID=}"; ipid="${ipid%% *}"
    if [[ "$iout" == *"FOREIGN=[$ipid]"* ]] && ! grep -q -- "-TERM -$ipid" "$ilog"; then
      pass "reap-ownership (incarnation): a live leader whose recorded start time does NOT match is released UNSIGNALLED — the pgid was recycled, so killing it would hit another tree ($iout)"
    else
      fail "reap-ownership-incarnation-mismatch: out=[$iout] log=[$(tr '\n' ';' <"$ilog")] — a refuted incarnation must be released without a signal"
    fi
    # (b) NON-VACUITY: with the incarnation MATCHED, the same group is signalled — the check refutes, it
    #     does not veto everything.
    : >"$ilog"
    iout="$(bash "$idrv" "$ilog" match 2>&1)"
    ipid="${iout#PID=}"; ipid="${ipid%% *}"
    if [[ "$iout" == *"FOREIGN=[]"* ]] && grep -q -- "-TERM -$ipid" "$ilog"; then
      pass "reap-ownership (incarnation) NON-VACUITY: with the recorded start time MATCHING, that same group IS signalled — the check is a refutation, not a blanket veto ($iout)"
    else
      fail "reap-ownership-incarnation-match: out=[$iout] log=[$(tr '\n' ';' <"$ilog")] — a matching incarnation must still be signalled"
    fi
  fi

  # ---- MUTANT CONTRAST: the HISTORICAL-LIST reap this replaced. Same driver, same stub, with
  # `fixture_reap`/`fixture_signal_owned` replaced by the form that iterates every pgid ever staged. It
  # must be shown to signal a group it no longer owns, or the asserts above prove nothing.
  local mut="$d/reapdrv-mutant.sh" mout
  {
    sed -e '/^fixture_signal_owned() {$/,/^}$/d' -e '/^fixture_reap() {$/,/^}$/d' "$drv"
  } >"$mut.body"
  {
    # The historical list is captured at definition time, exactly as the pre-fix harness kept it.
    printf '%s\n' 'FIXTURE_HISTORICAL=("2999901" "2999902" "2999903")'
    printf '%s\n' 'fixture_signal_owned() { local sig="$1" pgid; shift; for pgid in "${FIXTURE_HISTORICAL[@]}"; do kill "-$sig" "-$pgid" 2>/dev/null || true; done; }'
    printf '%s\n' 'fixture_reap() { local sig pgid; for sig in TERM KILL; do for pgid in "${FIXTURE_HISTORICAL[@]}"; do kill "-$sig" "-$pgid" 2>/dev/null || true; done; done; return 0; }'
  } >"$mut.defs"
  # The overrides are appended AFTER the extracted definitions and BEFORE the driver's first call, which
  # is the `fixture_reap` line — so the mutant differs from the driver above in those two functions only.
  awk 'BEGIN{ins=0} /^fixture_reap$/ && ins==0 {while ((getline l < DEFS) > 0) print l; ins=1} {print}' \
    DEFS="$mut.defs" "$mut.body" >"$mut"
  : >"$log"
  bash "$mut" "$log" "$g1" >/dev/null 2>&1 || true
  mout="$(tr '\n' ';' <"$log")"
  local mlater="${mout##*--- LATER REAP ---;}"
  if [[ "$mout" == *"-KILL -$g1"* ]] && [[ "$mlater" == *"-KILL -$g2"* ]]; then
    pass "reap-ownership MUTANT CONTRAST: the historical-list reap SIGKILLs pgid $g1 (proven dead before it ever ran) and re-SIGKILLs $g2 in a later reap — signals to groups it no longer owns, on a box where those numbers may have been recycled"
  else
    fail "reap-ownership-mutant: the historical-list form did not signal a disowned group (log=[$mout]); the contrast proves nothing"
  fi
}

# ---------------------------------------------------------------------------
# (#3549, roborev job 203 F2): A `wait`ED FIXTURE IS SURRENDERED AT THE REAP — NO LATER PASS MAY SIGNAL
# ITS PGID.
#
# THE SAME DESTRUCTIVE-SIGNAL CLASS AS JOB 198 F2, ONE STEP FURTHER IN. That fix stopped the reap
# iterating a HISTORICAL list; what it left is that a fixture which has been `wait`ed is DEAD YET STILL
# REGISTERED. Nine cases waited a short-lived `sleep 0.1` DIRECTLY, and its pgid then sat in
# `FIXTURE_OWNED` for the rest of the run. `fixture_release_unowned` cannot see the problem, because its
# only evidence is `kill -0` on that NUMBER: once the kernel hands the number to an unrelated same-user
# process GROUP the probe answers `live` — truthfully, about somebody ELSE — and the reap delivers TERM
# and then KILL to it. On this four-lane box that is plausibly a sibling lane's supervisor, gate or
# worker being SIGKILLed by this test suite.
#
# THE FIX IS A CHOKE POINT (`fixture_wait`), so the property is pinned TWO ways:
#   (a) STRUCTURALLY — no call site `wait`s a fixture-registered pid directly. The variable set is
#       DERIVED from this file (every name assigned `$FIXTURE_LAST_PID`), so a NEW fixture variable is
#       covered without editing this case, which is the whole reason per-call-site correctness failed
#       twice;
#   (b) BEHAVIOURALLY — with `kill` overridden by a logging function, a fixture that has exited and been
#       waited receives NO signal, and a LATER reap in which its number READS LIVE (the recycle) sends
#       nothing to it either. The MUTANT is the pre-fix call site — a bare `wait` with no registry
#       interaction — and it must be SHOWN delivering TERM and KILL to that recycled number.
#
# NOTHING IS SIGNALLED FOR REAL AND THERE IS NO PROCFS DEPENDENCY: the pgids are synthetic, the state
# each reports is supplied by the stub, and `fixture_leader_ident` returns EMPTY for them on every host
# — so this exercises the REGISTRY path (the one the finding is about) identically on macOS.
# ---------------------------------------------------------------------------
test_fixture_wait_surrenders_ownership() {
  local d drv mut log out win later vars v offenders="" g1=2999911 g2=2999912
  d="$(new_case_dir)"
  drv="$d/waitdrv.sh"
  mut="$d/waitdrv-mutant.sh"
  log="$d/kill.log"

  # ---- (a) STRUCTURAL. The subject set is derived, not listed.
  vars="$(grep -oE '^[[:space:]]*(local +)?[A-Za-z_][A-Za-z0-9_]*=\$FIXTURE_LAST_PID' "$SELF_FILE" \
          | sed -E 's/^[[:space:]]*(local +)?//; s/=\$FIXTURE_LAST_PID$//' | sort -u | tr '\n' ' ')"
  if [[ -n "${vars// /}" ]]; then
    pass "fixture-wait (a) PREMISE: the fixture-pid variable set was DERIVED from this file: [${vars% }]"
  else
    fail "fixture-wait-premise: no variable is assigned from FIXTURE_LAST_PID, so the structural check below has no subject and would pass vacuously"
    return 0
  fi
  for v in $vars FIXTURE_LAST_PID; do
    if grep -nE "^[[:space:]]*wait[[:space:]]+\"\\\$$v\"" "$SELF_FILE" >/dev/null; then
      offenders="$offenders $v"
    fi
  done
  if [[ -z "$offenders" ]]; then
    pass "fixture-wait (a): no call site waits a fixture-registered pid directly — every reap goes through fixture_wait, which unregisters"
  else
    fail "fixture-wait-structural: these fixture pids are \`wait\`ed directly, leaving them registered for a later reap to signal:$offenders — use fixture_wait"
  fi

  # ---- (b) BEHAVIOURAL, against the SHIPPED harness extracted from this file.
  {
    printf '%s\n' '#!/usr/bin/env bash'
    printf '%s\n' 'set -uo pipefail'
    printf '%s\n' 'KILL_LOG="$1"; DEAD_GROUPS="${2:-}"'
    printf '%s\n' 'kill() {'
    printf '%s\n' '  printf "%s\n" "$*" >>"$KILL_LOG"'
    printf '%s\n' '  if [[ "${1:-}" == "-0" ]]; then'
    printf '%s\n' '    case " $DEAD_GROUPS " in *" ${2#-} "*) printf "%s\n" "bash: kill: (${2}) - No such process" >&2; return 1 ;; esac'
    printf '%s\n' '  fi'
    printf '%s\n' '  return 0'
    printf '%s\n' '}'
    printf '%s\n' 'sleep() { :; }'
    printf '%s\n' 'wait() { :; }'
    sed -n '/^fixture_leader_ident()/,/^}/p' "$SELF_FILE"
    sed -n '/^fixture_group_state()/,/^}/p' "$SELF_FILE"
    sed -n '/^fixture_release_unowned()/,/^}/p' "$SELF_FILE"
    sed -n '/^fixture_signal_owned()/,/^}/p' "$SELF_FILE"
    sed -n '/^fixture_unregister()/,/^}/p' "$SELF_FILE"
    sed -n '/^fixture_wait()/,/^}/p' "$SELF_FILE"
    sed -n '/^fixture_reap()/,/^}/p' "$SELF_FILE"
    # g1 is the short-lived fixture: it has EXITED, which is the honest state at the moment a case
    # waits it. g2 is a live owned group and is the non-vacuity control.
    printf '%s\n' "FIXTURE_OWNED=(\"$g1|\" \"$g2|\")"
    printf '%s\n' 'FIXTURE_FOREIGN=()'
    printf '%s\n' 'echo "--- WAIT ---" >>"$KILL_LOG"'
    printf '%s\n' "fixture_wait $g1"
    # THE RECYCLE: the number is handed to an unrelated process group, so it now READS LIVE. This is the
    # window the finding is about, and it is minutes wide in a real run.
    # `DEAD_GROUPS=""`, not a substring removal: g1 is the ONLY dead group here, and a `${var// x/}`
    # form silently matched nothing (the driver's argument carries no leading space), leaving the
    # recycle unstaged and the contrast measuring the un-recycled case.
    printf '%s\n' 'DEAD_GROUPS=""'
    printf '%s\n' 'echo "--- LATER REAP ---" >>"$KILL_LOG"'
    printf '%s\n' 'fixture_reap'
  } >"$drv"
  local fns
  fns="$(grep -c '^fixture_\(leader_ident\|group_state\|release_unowned\|signal_owned\|unregister\|wait\|reap\)() {$' "$drv" || true)"
  if [[ "$fns" == "7" ]] && bash -n "$drv" 2>/dev/null; then
    pass "fixture-wait PREMISE: all 7 harness functions were extracted from this file into the driver, which parses (the subject is the shipped harness, not a re-implementation)"
  else
    fail "fixture-wait-premise-extract: extracted $fns/7 harness functions, or the driver does not parse; nothing below would be attributable"
    return 0
  fi

  : >"$log"
  bash "$drv" "$log" "$g1" >/dev/null 2>&1 || true
  out="$(tr '\n' ';' <"$log")"
  win="${out##*--- WAIT ---;}"; win="${win%%--- LATER REAP ---*}"
  later="${out##*--- LATER REAP ---;}"
  if [[ "$win" != *"-TERM -$g1"* && "$win" != *"-KILL -$g1"* ]]; then
    pass "fixture-wait (b): waiting a fixture that has already exited delivers NO signal — the group is proven gone and released (window=[$win])"
  else
    fail "fixture-wait-signalled-at-wait: the wait window signalled pgid $g1, which it had proven dead; window=[$win]"
  fi
  if [[ "$later" != *"-TERM -$g1"* && "$later" != *"-KILL -$g1"* ]]; then
    pass "fixture-wait (b) THE FINDING'S CASE: a LATER reap, with pgid $g1 now READING LIVE because the number was recycled, sends it NOTHING — ownership was surrendered at the reap (later=[$later])"
  else
    fail "fixture-wait-recycled-signalled: a later reap signalled recycled pgid $g1 — it is still registered after being waited; later=[$later]"
  fi
  if [[ "$later" == *"-TERM -$g2"* && "$later" == *"-KILL -$g2"* ]]; then
    pass "fixture-wait NON-VACUITY: the same later reap DOES TERM and KILL the still-owned live group $g2, so the silence above is a decision and not a broken reap"
  else
    fail "fixture-wait-nonvacuity: the later reap did not signal still-owned live pgid $g2; later=[$later]"
  fi

  # ---- THE MUTANT: the pre-fix call site. `fixture_wait` replaced by the bare
  # `wait "$pid" 2>/dev/null || true` those nine cases used, which touches the registry not at all.
  # Appended AFTER the extracted definitions so it redefines the function, and the driver body is the
  # SAME text, so the contrast is attributable to this one substitution.
  python3 - "$drv" "$mut" <<'PYEOF'
import sys
src, dst = sys.argv[1], sys.argv[2]
s = open(src).read()
# Anchored on the DRIVER BODY's first marker, not on a `FIXTURE_OWNED=(` assignment: that spelling also
# occurs inside `fixture_release_unowned`, so the insertion would land mid-function.
anchor = '\necho "--- WAIT ---"'
if s.count(anchor) != 1:
    sys.exit("the driver's WAIT marker is not unique; the insertion point moved")
i = s.index(anchor) + 1
pre = '''fixture_wait() {
  local pid
  for pid in "$@"; do
    wait "$pid" 2>/dev/null || true
  done
  return 0
}
'''
open(dst, 'w').write(s[:i] + pre + s[i:])
PYEOF
  if [[ -s "$mut" ]] && bash -n "$mut" 2>/dev/null \
     && [[ "$(grep -c '^fixture_wait() {$' "$mut")" == "2" ]]; then
    pass "fixture-wait MUTANT PREMISE: the pre-fix bare-wait form was appended over the shipped fixture_wait, and the mutant parses"
  else
    fail "fixture-wait-mutant-build: the mutant could not be built (definitions=[$(grep -c '^fixture_wait() {$' "$mut" 2>/dev/null || echo 0)])"
    return 0
  fi
  : >"$log"
  bash "$mut" "$log" "$g1" >/dev/null 2>&1 || true
  out="$(tr '\n' ';' <"$log")"
  later="${out##*--- LATER REAP ---;}"
  if [[ "$later" == *"-TERM -$g1"* && "$later" == *"-KILL -$g1"* ]]; then
    pass "fixture-wait MUTANT CONTRAST: with the pre-fix bare wait, the later reap delivers TERM AND KILL to recycled pgid $g1 — an unrelated process group, plausibly a sibling lane's, killed by this suite (later=[$later])"
  else
    fail "fixture-wait-mutant: the pre-fix form sent [$later] to recycled pgid $g1; expected TERM and KILL, or the contrast proves nothing"
  fi
}

t test_fixture_reap_never_signals_a_disowned_group
t test_fixture_wait_surrenders_ownership

# ---------------------------------------------------------------------------
# Test 47j (#3549, roborev job 208 F1): NO REFUSAL ADVERTISES THE ESCAPE HATCH.
#
# THE DEFECT. The generic remedy line printed by EVERY refusal used to end "…, or set SUPERVISOR_LOCK
# explicitly to opt out of this compatibility check". Naming the lock DID skip this guard at the time,
# so that sentence told an operator whose start had just been refused how to start anyway, and the two
# supervisors then shared one worktree with no lock in common. The advice CAUSED the harm the guard
# exists to prevent, and it survived fourteen review rounds of the surrounding code because it reads as
# helpful.
#
# THE PROPERTY IS NOW STRICTLY STRONGER, because the skip itself is GONE (lead ruling 2026-08-30, AC4
# removed as unsound): there is no opt-out to advertise, so no refusal may print anything that even
# implies one. `SUPERVISOR_LOCK` chooses where OUR lock lives, nothing more, and that is documented in
# `docs/development/fleet-runbook.md` — not in a refusal, whose reader is by definition looking for a
# way past it.
#
# TWO HALVES, because either alone can pass for the wrong reason. BEHAVIOURAL over both refusing states
# (`present` and the probe-failed cause) — the emitted bytes are what an operator reads. STRUCTURAL over
# the two functions that build that text, counting a BARE `SUPERVISOR_LOCK` and NOT a `$SUPERVISOR_LOCK`
# expansion: the refusal legitimately prints the per-lane lock's PATH (that is the fact that explains
# why the two locks are invisible to each other), and a check that reds on correct text is the check
# people learn to waive.
# ---------------------------------------------------------------------------
sv_refusal_has_no_override_advice() {
  local label="$1" out="$2"
  if [[ "$out" != *"SUPERVISOR_LOCK"* ]]; then
    pass "no-override-advice ($label): the refusal names no way to opt out of the check — its remedies are stop the legacy supervisor, or upgrade that checkout"
  else
    fail "no-override-advice-$label: the refusal text mentions SUPERVISOR_LOCK; out=[$out] — advertising the override to a refused operator is advice to run two supervisors in one worktree (#3549 job 208 F1)"
  fi
}

test_legacy_lock_refusals_never_advertise_the_override() {
  local d tmp lane legacy out rc live ovr mout code probe fn body
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"; lane="lane3549adv$$"
  mkdir -p "$tmp"
  legacy="$tmp/cqlite-worker-supervisor.lock"

  # ---- (1) PRESENT — the state where the advice was actively harmful: an operator refused BECAUSE
  # another supervisor may be running, told how to skip the check.
  fixture_bg sleep 300
  live=$FIXTURE_LAST_PID
  mkdir -p "$legacy"
  printf '%s\n' "$live" >"$legacy/pid"
  out="$(legacy_lock_drive "$tmp" "$lane")"; rc=$?
  if [[ "$rc" -ne 0 ]] && legacy_refusal_ok "$out" && [[ "$out" == *"EXISTS there"* ]]; then
    pass "no-override-advice PREMISE (present): the drive refused over a present legacy lock, so the text below is the presence refusal"
  else
    fail "no-override-advice-premise-present: rc=$rc out=[$out] — the presence refusal was not reached and the assert below has no subject"
  fi
  sv_refusal_has_no_override_advice present "$out"
  # ...and the two remedies that DO belong there are present, so the removal did not leave the operator
  # with no guidance at all.
  if [[ "$out" == *"stop the pre-#3467 supervisor on this box, or upgrade that checkout to #3467+"* ]]; then
    pass "no-override-advice (present): the generic remedy still names both legitimate actions — stop the legacy supervisor, or upgrade that checkout past #3467"
  else
    fail "no-override-advice-present-remedy-lost: out=[$out] — removing the override clause must not remove the remedy"
  fi

  # ---- (2) MUTANT CONTRAST: the pre-fix line, restored in the emitter, on the SAME state. The assert
  # above must be shown to have teeth — a green over text that never contained the string would measure
  # nothing.
  ovr="$d/m-advice.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" supervisor_legacy_lock_refuse \
       'stop the pre-#3467 supervisor on this box, or upgrade that checkout to #3467+.' \
       'stop the pre-#3467 supervisor, or set SUPERVISOR_LOCK explicitly to opt out of this compatibility check.'; then
    mout="$(legacy_lock_drive_override "$ovr" "$tmp" "$lane")" || true
    if [[ "$mout" == *"EXISTS there"* ]] && [[ "$mout" == *"set SUPERVISOR_LOCK explicitly to opt out"* ]]; then
      pass "no-override-advice MUTANT CONTRAST: with the pre-fix line restored, the refusal hands the operator the one instruction that puts a second supervisor in this worktree — the assert above reds on exactly this text"
    else
      fail "no-override-advice-mutant: out=[$mout] — the pre-fix form must be shown to break, or the assert measures nothing"
    fi
  fi
  fixture_kill "$live"
  rm -rf "$legacy"

  # ---- (3) THE OTHER REFUSING STATE: the probe could not answer. Both states, since each builds its own
  # detail and remedy text. Staged with a container that does not exist, so it needs no permission bits.
  out="$(legacy_lock_drive "$d/no-such-container" "$lane")"; rc=$?
  if [[ "$rc" -ne 0 ]] && [[ "$out" == *"EXISTENCE PROBE FAILED"* ]]; then
    pass "no-override-advice PREMISE (could-not-tell): the drive reached the probe-failed refusal"
  else
    fail "no-override-advice-premise-couldnottell: rc=$rc out=[$out]"
  fi
  sv_refusal_has_no_override_advice could-not-tell "$out"

  # ---- (4) STRUCTURAL, over the two functions that BUILD refusal text: the emitter and the guard that
  # passes it the per-state detail and remedy. Full-line comments are stripped first — this file's own
  # prose necessarily NAMES the variable to explain why it is absent — and what remains is code, where a
  # BARE `SUPERVISOR_LOCK` (not a `$SUPERVISOR_LOCK` expansion) can only be prose about the override.
  # This half holds for states no behavioural case stages.
  probe=""
  for fn in supervisor_legacy_lock_refuse supervisor_legacy_lock_guard; do
    body="$(sed -n "/^$fn()/,/^}/p" "$SUPERVISOR")"
    if [[ -z "$body" || "$body" != *"$fn() {"* ]]; then
      fail "no-override-advice-structural-premise: could not extract $fn from $SUPERVISOR"
      return 0
    fi
    code="$(printf '%s\n' "$body" | grep -vE '^[[:space:]]*#' || true)"
    if [[ -z "$code" ]]; then
      fail "no-override-advice-structural-premise: $fn has no non-comment line; the scan has no subject"
      return 0
    fi
    probe+="$(printf '%s\n' "$code" | grep -n 'SUPERVISOR_LOCK' | grep -v '[$]SUPERVISOR_LOCK' || true)"
  done
  if [[ -z "$probe" ]]; then
    pass "no-override-advice STRUCTURAL: neither the refusal emitter nor the guard carries a bare SUPERVISOR_LOCK in code — the only occurrences are the \$SUPERVISOR_LOCK path expansion, so no state can print the escape hatch"
  else
    fail "no-override-advice-structural: [$probe] — a refusal path names SUPERVISOR_LOCK as prose; the override is documented in docs/development/fleet-runbook.md, not in a refusal (#3549 job 208 F1)"
  fi

  rm -rf "$tmp"
}

t test_legacy_lock_refusals_never_advertise_the_override





# ===========================================================================
# THE PER-LANE LOCK (#3601) — `acquire_lock`'s OWN lock, the one every lane executes on every start.
#
# The cases below are the LIVE-PATH half of #3549's defect family. #3549 hardened the legacy
# COMPATIBILITY guard, whose activation precondition was measurably empty, and recorded these four at
# the site as out of scope. They are:
#
#   1. the holder pid used UNPARSED — an empty, garbled or multi-line `pid` file made `kill -0` fail,
#      which read as "dead", and the lock was RECLAIMED FROM A LIVE HOLDER;
#   2. the `mkdir`-then-populate window — a lock observable with NO pid file, which a peer read as a
#      dead holder;
#   3. two-valued liveness — `kill -0` fails with EPERM as well as ESRCH, so a live holder owned by
#      another user read as dead;
#   4. an "already running (pid N)" refusal asserting an identity it never checked;
#   5. (addendum) every `TMPDIR`-derived operand missing a `--`, so an option-shaped `TMPDIR` stopped
#      the lane from starting at all, with a diagnostic that blamed a stale lock instead of the path.
#
# MEASURED PRE-FIX, against `origin/main` at `674cffa9d`, so none of this is inferred:
#   pid-less lock      -> `reclaiming stale lock …(holder pid  not alive)` + ACQUIRED
#   pid file `not-a-pid` -> `reclaiming stale lock …(holder pid not-a-pid not alive)` + ACQUIRED
#   pid file `1` (live, EPERM for a non-root uid) -> `…(holder pid 1 not alive)` + ACQUIRED
#   TMPDIR=-scratch    -> `reclaiming stale lock -scratch/…` then `failed to acquire lock -scratch/…`
#
# EVERY CASE CARRIES A MUTANT CONTRAST, and the mutants are DERIVED from the shipped functions by
# `sv_mutant_override` (one literal substitution, premises checked), so a mutant can never drift into a
# re-implementation and a green assert can never be a green over code that was never the pre-fix form.
# ===========================================================================

# lane_lock_drive_at <cwd> <override|-> <tmp> <lane> [VAR=VAL ...] — ONE driver for every per-lane-lock
# case, derived from the SAME `SV_DRIVE_BODY`/`SV_DRIVE_BODY_OVERRIDE` the legacy-guard cases use, so a
# case that varies the working directory, the environment, or one function cannot drift into exercising
# a different startup path than the ordinary case does. `-` for no override.
#
# A working directory is a parameter because a RELATIVE — and therefore possibly OPTION-SHAPED — `TMPDIR`
# is only testable from a chosen cwd. `REPO_ROOT` is resolved from the SUPERVISOR's own location, not
# from cwd, so moving cwd does not change any other resolution (verified by the pre-existing
# `legacy_lock_drive_in` cases, which do the same thing).
lane_lock_drive_at() {
  local cwd="$1" override="$2" tmp="$3" lane="$4"
  shift 4
  if [[ "$override" == '-' ]]; then
    ( cd "$cwd" && env -u SUPERVISOR_LOCK TMPDIR="$tmp" LANE_ID="$lane" LOCK_CMD="" CLAIM_CMD="" "$@" \
        bash -c "$SV_DRIVE_BODY" _ "$SUPERVISOR" 2>&1 )
  else
    ( cd "$cwd" && env -u SUPERVISOR_LOCK TMPDIR="$tmp" LANE_ID="$lane" LOCK_CMD="" CLAIM_CMD="" "$@" \
        bash -c "$SV_DRIVE_BODY_OVERRIDE" _ "$SUPERVISOR" "$override" 2>&1 )
  fi
}

# A per-lane-lock refusal, told apart from the LEGACY one — an operator and a test both have to be able
# to tell the two locks apart, which is why `legacy_refusal_ok` asserts the converse.
lane_refusal_ok() {
  local out="$1"
  [[ "$out" == *"worker-supervisor: "* ]] \
    && [[ "$out" != *"LEGACY GLOBAL supervisor lock"* ]] \
    && [[ "$out" != *"ACQUIRED="* ]]
}

# The undecidable refusal must carry ITS REMEDY, and the remedy must be RUNNABLE AS PRINTED. That is
# #3549 lead ruling 2 in mechanized form: "cannot tell ⇒ refuse" with no way out is a lane blocked by a
# directory forever, which is broken rather than fail-closed. Asserted per case, not once, because each
# refusing state builds its own text.
lane_lock_remedy_ok() {
  local label="$1" out="$2" lock="$3" bare
  bare="$(printf '%s\n' "$out" | grep -vE "$SV_DIAG_RE" | grep -v '^$' | head -1)"
  if [[ "$out" == *"worker-supervisor: remedy — "* ]] && [[ "$bare" == "rmdir -- "* ]]; then
    pass "lane-lock remedy ($label): the refusal names a remedy and prints exactly one BARE runnable line, and it is the NON-RECURSIVE removal (line=[$bare]) — a refusal with no way out is a permanently blocked lane, not fail-closed"
  else
    fail "lane-lock-remedy-missing ($label): bare=[$bare] out=[$out] — every undecidable refusal must print the command that clears the lock"
    return 0
  fi
  # ...AND IT MUST BE RUNNABLE, VERBATIM, AGAINST THE REAL PATH. A remedy that is only prose is what
  # ruling 2 calls a permanent refusal with extra words.
  local run_rc=0
  eval "$bare" 2>/dev/null || run_rc=$?
  if [[ "$run_rc" -eq 0 && ! -d "$lock" ]]; then
    pass "lane-lock remedy ($label): the printed line runs VERBATIM and clears the lock — so this refusal is an instruction, not a dead end"
  else
    fail "lane-lock-remedy-not-runnable ($label): rc=$run_rc line=[$bare] lock still present=[$([[ -d "$lock" ]] && echo yes || echo no)]"
  fi
}


# ---------------------------------------------------------------------------
# AC1 — THE HOLDER PID IS PARSED BEFORE IT IS USED, AND AN UNPARSEABLE PID REFUSES.
# ---------------------------------------------------------------------------
test_lane_lock_holder_pid_is_parsed_before_use() {
  local d tmp lane lock out rc dead shape ovr mout mrc
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"
  lane="lane3601parse$$"
  mkdir -p "$tmp"
  lock="$tmp/cqlite-worker-supervisor-$lane.lock"

  fixture_bg sleep 0.1
  dead=$FIXTURE_LAST_PID
  fixture_wait "$dead"

  # ---- (0) THE NON-VACUITY FLOOR, FIRST, BECAUSE EVERY ASSERT BELOW IS A REFUSAL. A suite of
  # refusals is satisfied by code that refuses unconditionally, and that code is BROKEN (ruling 2: a
  # guard that never permits work is not fail-closed). So: a WELL-FORMED pid whose process is
  # affirmatively gone must still be RECLAIMED, automatically, with no operator. This is the stale case
  # that actually happens on this fleet — a holder killed by -9, an OOM, a reboot.
  rm -rf "$lock"
  mkdir -p "$lock"
  printf '%s\n' "$dead" >"$lock/pid"
  out="$(lane_lock_drive_at "$d" - "$tmp" "$lane")"; rc=$?
  if [[ "$rc" -eq 0 && "$out" == *"ACQUIRED=$lock"* && "$out" == *"affirmatively DEAD"* ]]; then
    pass "lane-lock AC1 NON-VACUITY: a well-formed pid whose process is affirmatively gone is still RECLAIMED automatically — the refusals below are decisions, not a guard that always says no"
  else
    fail "lane-lock-reclaim-lost: rc=$rc out=[$out] — an affirmatively dead holder must still be reclaimed, or this change has broken the only automated way a stale lock clears"
  fi
  rm -rf "$lock"

  # ---- (1) EVERY UNPARSEABLE SHAPE REFUSES, AND LEAVES THE LOCK EXACTLY AS FOUND. The shapes are the
  # ones a crash, a truncation or a foreign writer produces — not a curated list of things that happen
  # to fail, but one per rejection reason in `supervisor_lock_pid_read`.
  local staged
  for shape in EMPTY GARBLED TWOLINE TRAILING ZERO LEADINGZERO OVERLONG NOTAFILE NUL NULMID; do
    rm -rf "$lock"
    mkdir -p "$lock"
    case "$shape" in
      EMPTY)       : >"$lock/pid" ;;
      GARBLED)     printf 'not-a-pid\n' >"$lock/pid" ;;
      TWOLINE)     printf '%s\n%s\n' "$dead" "$dead" >"$lock/pid" ;;
      TRAILING)    printf '%s x\n' "$dead" >"$lock/pid" ;;
      ZERO)        printf '0\n' >"$lock/pid" ;;
      LEADINGZERO) printf '0%s\n' "$dead" >"$lock/pid" ;;
      # 19 DIGITS, AND THE WIDTH IS LOAD-BEARING (#3601, roborev job 236 B14). This fixture was 15
      # digits, which only exceeds a bound the platform PUBLISHES: where none is published the ceiling
      # gate does not apply and the parser's own platform-independent guard allows up to 18, so a
      # 15-digit value is ACCEPTED and this shape's required refusal failed — on macOS/bash 3.2, a
      # platform this file explicitly supports. 19 digits is past the arithmetic-length guard, which is
      # a property of the shell's integer width rather than of any platform's pid space, so this shape
      # refuses everywhere. Same class as B13, and surfaced BY the B13 fix: making the no-bound branch
      # measured on this host instead of platform-conditional is what made this sibling's assumption
      # visible.
      OVERLONG)    printf '1234567890123456789\n' >"$lock/pid" ;;
      NOTAFILE)    mkdir -p "$lock/pid" ;;
      # REAL NUL BYTES, WRITTEN AS BYTES (#3601, roborev job 231) — not a stand-in, because the entire
      # defect is that a NUL is INVISIBLE to every check that runs on a shell variable. `NUL` is the
      # dangerous shape: `<dead-pid> NUL LF`, whose NUL-stripped content is a clean, plausible, DEAD pid,
      # so it passed non-empty + single-line + all-digits + non-zero and the lock was reclaimed. `NULMID`
      # is the same hazard with the NUL INSIDE the digits, where the value the shell sees is a DIFFERENT
      # number than the file records.
      NUL)         printf '%s\000\n' "$dead" >"$lock/pid" ;;
      NULMID)      printf '%s\000%s\n' "${dead%?}" "${dead#${dead%?}}" >"$lock/pid" ;;
    esac
    staged="$(ls -A "$lock" | tr '\n' ' ')"
    out="$(lane_lock_drive_at "$d" - "$tmp" "$lane")"; rc=$?
    if [[ "$rc" -ne 0 ]] && lane_refusal_ok "$out" \
       && [[ "$out" == *"could NOT DECIDE"* ]] && [[ "$out" != *"reclaiming stale lock"* ]] \
       && [[ -d "$lock" ]] && [[ "$(ls -A "$lock" | tr '\n' ' ')" == "$staged" ]]; then
      pass "lane-lock AC1 ($shape): an unparseable holder pid REFUSES, names the parse verdict, reclaims nothing and leaves the lock byte-for-byte as found"
    else
      fail "lane-lock-unparsed-pid ($shape): rc=$rc out=[$out] lock-contents=[$(ls -A "$lock" 2>/dev/null | tr '\n' ' ')] — an unparseable pid must never be read as a dead holder"
    fi
    # The cause must NAME the parse verdict, not merely refuse: "cannot tell" and "the holder is there"
    # are different facts with different remedies (#3549 lead ruling 1).
    if [[ "$out" == *"unparseable pid-"* ]]; then
      pass "lane-lock AC1 ($shape): the refusal names WHICH parse rejected the content, so the cause is the probe's verdict and not a bare 'refused'"
    else
      fail "lane-lock-unparsed-cause ($shape): out=[$out] — the refusal must name the parse verdict"
    fi
    # AND THE WAY OUT (ruling 2). Checked for the shapes whose lock a non-recursive removal can clear —
    # for the two shapes that leave content behind, `rmdir` correctly REFUSES, which is the safety
    # property the command was chosen for and is asserted separately below.
    case "$shape" in
      EMPTY | GARBLED | TWOLINE | TRAILING | ZERO | LEADINGZERO | OVERLONG | NUL | NULMID)
        rm -f -- "$lock/pid"
        lane_lock_remedy_ok "$shape" "$out" "$lock"
        ;;
    esac
  done

  # ---- (1b) THE `OVERLONG` WIDTH IS PINNED MECHANICALLY, NOT BY THE COMMENT ABOVE IT (#3601 B14). A
  # comment saying "19 digits, because 15 only beats a published bound" does not stop the next edit
  # shrinking it, and the shrink is INVISIBLE on this host — it reds only on a platform without
  # `/proc/sys/kernel/pid_max`, which nobody runs the suite on. So the same fixture content is driven
  # through the parser with the ceiling FORCED to `unknown`, which is the macOS shape, and must still
  # refuse. This is the mechanism-not-care point: a human catches instance N, a test catches the class.
  local ovr_unk over_verdict
  ovr_unk="$d/f-unknown-ceiling.sh"; : >"$ovr_unk"
  if sv_mutant_override "$ovr_unk" supervisor_pid_space_ceiling \
       "  printf 'authoritative %s' \"\$((b - 1))\"" \
       "  printf '%s' 'unknown forced-for-test'; return 0"; then
    rm -rf "$lock"
    mkdir -p "$lock"
    printf '1234567890123456789\n' >"$lock/pid"
    over_verdict="$(env SUP="$SUPERVISOR" OVR="$ovr_unk" F="$lock/pid" bash -c 'source "$SUP"; source "$OVR"; printf "%s" "$(supervisor_lock_pid_read "$F")"' 2>/dev/null || true)"
    if [[ "$over_verdict" == 'unparseable pid-digit-count-out-of-well-formedness-bound' ]]; then
      pass "lane-lock AC1 (OVERLONG, platform-independent): the fixture is refused by the ARITHMETIC-LENGTH guard even with no platform pid bound published [$over_verdict] — so this shape's refusal does not depend on the host publishing a ceiling"
    else
      fail "lane-lock-overlong-platform-dependent: verdict=[$over_verdict] — this fixture must be wide enough to be refused with no published ceiling, or the shape reds on macOS (the B14 defect); 15 digits was not"
    fi
    rm -rf "$lock"
  fi

  # ---- (2) THE PRINTED REMEDY IS NON-RECURSIVE, AND THAT IS THE SAFETY PROPERTY. With content still in
  # the lock the printed line must FAIL rather than delete something nobody examined — the opposite of
  # an `rm -rf`, which would destroy the evidence of the state we could not decide.
  rm -rf "$lock"
  mkdir -p "$lock"
  printf 'not-a-pid\n' >"$lock/pid"
  out="$(lane_lock_drive_at "$d" - "$tmp" "$lane")" || true
  local bare rmrc=0
  bare="$(printf '%s\n' "$out" | grep -vE "$SV_DIAG_RE" | grep -v '^$' | head -1)"
  eval "$bare" 2>/dev/null || rmrc=$?
  if [[ "$rmrc" -ne 0 && -f "$lock/pid" ]]; then
    pass "lane-lock AC1 (remedy safety): with content still inside the lock the printed removal REFUSES (rc=$rmrc) and the pid file survives — a mis-paste cannot destroy a holder's record"
  else
    fail "lane-lock-remedy-destructive: rc=$rmrc line=[$bare] — the printed remedy deleted contents nobody examined"
  fi

  # ---- (3) MUTANT CONTRAST: the PRE-FIX semantics, which is exactly "an unparseable pid is a holder
  # pid, and `kill -0` failing on it means dead". Expressed as ONE substitution in the shipped parse: the
  # non-digit rejection returns a (known-dead) pid instead of a named refusal. With that, the shipped
  # decision path reclaims the lock — which is the measured pre-fix behaviour, reproduced from shipped
  # code rather than from a copy of it.
  rm -rf "$lock"
  mkdir -p "$lock"
  printf 'not-a-pid\n' >"$lock/pid"
  ovr="$d/m-parse.sh"; : >"$ovr"
  mrc=0
  if sv_mutant_override "$ovr" supervisor_lock_pid_read \
       "      printf '%s' 'unparseable pid-not-all-decimal-digits'" \
       "      printf '%s' 'pid $dead'"; then
    mout="$(lane_lock_drive_at "$d" "$ovr" "$tmp" "$lane")" || mrc=$?
    if [[ "$mrc" -eq 0 && "$mout" == *"ACQUIRED=$lock"* && "$mout" == *"reclaiming stale lock"* ]]; then
      pass "lane-lock AC1 MUTANT: with the parse's non-digit rejection turned back into a holder pid, the SAME drive RECLAIMS the lock (the measured pre-fix behaviour) — so the refusals above are the parse doing the work, not the harness"
    else
      fail "lane-lock-mutant-parse: rc=$mrc out=[$mout] — the pre-fix form must be shown to reclaim, or every AC1 assert measures nothing"
    fi
  fi
  rm -rf "$lock" "$tmp"
}

t test_lane_lock_holder_pid_is_parsed_before_use


# ---------------------------------------------------------------------------
# AC2 / AC5 — THE PID-LESS WINDOW, DRIVEN WITH A REAL COMPETING PROCESS AND A FORCED INTERLEAVING.
#
# The window is `mkdir` (the lock now exists) … `pid` published. A peer arriving inside it saw a lock
# with no pid file and reclaimed it from a holder that was alive and starting. This case stages that
# interleaving with an actual second process rather than by argument, in both of its shapes:
#
#   (i)  THE HOLDER IS STARTING — pid-less now, published shortly. The arriving run must end up
#        refusing over the REAL holder pid: the window resolves into a fact, and the fact is "held".
#   (ii) THE HOLDER DIED INSIDE THE WINDOW — pid-less forever. The arriving run must REFUSE (never
#        reclaim), because "pid-less" is not evidence of death, and must print the way out.
#
# THE DISCRIMINATOR IS PERSISTENCE, NOT DURATION, and that is why (i) and (ii) are BOTH here: a timing
# heuristic would have to pick a number that is right for one of them and wrong for the other. Case (i)
# uses a delay well inside the bounded window and case (ii) never publishes at all, so the two are
# separated by whether the state RESOLVES, not by how long the test waited.
# ---------------------------------------------------------------------------
test_lane_lock_pidless_window_is_never_read_as_dead() {
  local d tmp lane lock out rc peer peerpid ovr mout mrc
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"
  lane="lane3601window$$"
  mkdir -p "$tmp"
  lock="$tmp/cqlite-worker-supervisor-$lane.lock"

  # ---- (i) A REAL PEER HOLDING A PID-LESS LOCK THAT IT THEN PUBLISHES.
  #
  # THE INTERLEAVING IS SEQUENCED BY OBSERVABLE EVENTS, NOT BY A TUNED `sleep` (see the header note on
  # this component's latency). A `sleep 0.4` in the peer would be exactly the load-dependent shape that
  # flakes a serial gate component: under co-scheduled load the peer publishes late, the arriving run
  # exhausts its window, and a correct implementation reds. So the two processes BLOCK ON EACH OTHER:
  #
  #   * the PEER creates the lock, then waits for a sentinel file before publishing;
  #   * the sentinel is created by the ARRIVING RUN's own first read of the pid file — an override that
  #     ADDS a `touch` to the shipped parse and changes nothing else, derived by `sv_mutant_override` so
  #     it cannot drift into a re-implementation;
  #   * the arriving run then re-reads until the pid appears, which is what it does in production.
  #
  # So the ordering is PROVEN rather than hoped: the peer CANNOT publish before the arriving run has
  # entered the window, and the arriving run does not leave the window until the peer publishes. Load
  # changes how long the case takes (normally milliseconds) and cannot change what it measures. The two
  # bounds present are generous safety stops, not tuned values, and each FAILS LOUDLY if reached.
  rm -rf "$lock"
  local probe_seen="$d/first-read-happened"
  rm -f "$probe_seen"
  ovr="$d/m-observe.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" supervisor_lock_pid_read \
       "  local f=\"\$1\" first='' line='' n=0 readrc=0" \
       "  local f=\"\$1\" first='' line='' n=0 readrc=0
  : >>\"$probe_seen\""; then
    pass "lane-lock AC2 PREMISE (i): the arriving run's first pid read is observable — the peer's publish is sequenced on it, so the interleaving needs no tuned delay"
  else
    fail "lane-lock-window-observer: the observing override could not be derived; the interleaving below would be timing-dependent, so the case stops rather than becoming a flake"
    return 0
  fi
  # The peer's wait is bounded so a failed handshake ends the case instead of hanging the suite; it
  # publishes anyway on expiry, which makes the assert below FAIL loudly rather than block.
  fixture_bg bash -c 'mkdir -p "$1"; i=0; while [[ ! -e "$2" && "$i" -lt 1500 ]]; do sleep 0.02; i=$((i + 1)); done; printf "%s\n" "$$" >"$1/pid"; sleep 60' _ "$lock" "$probe_seen"
  peer=$FIXTURE_LAST_PID
  # Precondition, polled on an OBSERVABLE state change with a generous bound: the lock must exist and be
  # PID-LESS before the arriving run starts, or the window was never staged.
  local waited=0
  while [[ ! -d "$lock" && "$waited" -lt 1500 ]]; do sleep 0.02; waited=$((waited + 1)); done
  if [[ -d "$lock" && ! -f "$lock/pid" ]] && [[ ! -e "$probe_seen" ]]; then
    pass "lane-lock AC2 PREMISE (i): the arriving run starts with the peer's lock present and PID-LESS, and the peer is blocked until that run reads it — the interleaving is staged, not assumed"
  else
    fail "lane-lock-window-premise: lock present=[$([[ -d "$lock" ]] && echo yes || echo no)] pid present=[$([[ -f "$lock/pid" ]] && echo yes || echo no)] sentinel=[$([[ -e "$probe_seen" ]] && echo yes || echo no)] — the window was not staged and the assert below has no subject"
  fi
  out="$(lane_lock_drive_at "$d" "$ovr" "$tmp" "$lane")"; rc=$?
  peerpid="$(cat "$lock/pid" 2>/dev/null || true)"
  if [[ "$rc" -ne 0 ]] && lane_refusal_ok "$out" && [[ "$out" != *"reclaiming stale lock"* ]] \
     && [[ -d "$lock" ]] && [[ -n "$peerpid" ]] && [[ "$out" == *"already running (pid $peerpid)"* ]]; then
    pass "lane-lock AC2 (i): a run arriving inside a real peer's PID-LESS window waits the window out, reads the peer's PUBLISHED pid and refuses over it — the peer's lock is untouched and nothing was reclaimed"
  else
    fail "lane-lock-window-starting: rc=$rc peerpid=[$peerpid] lock=[$([[ -d "$lock" ]] && echo present || echo GONE)] out=[$out] — a starting holder must never be read as a dead one"
  fi
  fixture_kill "$peer"
  rm -rf "$lock"

  # ---- (ii) THE PID-LESS LOCK THAT NEVER RESOLVES — a holder killed inside its own window. Persistent,
  # so the verdict is "undecidable", and undecidable REFUSES. The bound is small here on purpose: the
  # measurement is of PERSISTENCE, so a short bound is sufficient to establish it and the case does not
  # pay for the long window (i) needs.
  mkdir -p "$lock"
  out="$(lane_lock_drive_at "$d" - "$tmp" "$lane")"; rc=$?
  if [[ "$rc" -ne 0 ]] && lane_refusal_ok "$out" && [[ "$out" == *"pid-file-absent"* ]] \
     && [[ "$out" != *"reclaiming stale lock"* ]] && [[ -d "$lock" ]]; then
    pass "lane-lock AC2 (ii): a PERSISTENTLY pid-less lock is UNDECIDABLE and refuses — it is never read as stale, and the lock is left as found"
  else
    fail "lane-lock-window-persistent: rc=$rc out=[$out] lock=[$([[ -d "$lock" ]] && echo present || echo GONE)]"
  fi
  # It must also say how many reads it took, so the refusal reports a MEASUREMENT and not an assumption.
  if [[ "$out" == *"read(s) over a bounded window"* ]]; then
    pass "lane-lock AC2 (ii): the refusal states that the verdict came from repeated reads over a bounded window — a measurement of persistence, reported as one"
  else
    fail "lane-lock-window-not-reported: out=[$out]"
  fi
  lane_lock_remedy_ok "pid-file-absent" "$out" "$lock"

  # ---- (iii) NO `.stale.*` RESIDUE ANYWHERE. A refusal must leave nothing behind it: the reclaim path
  # renames the lock aside, so a refusal that had touched it would leave that name in the tree.
  local residue
  residue="$(find "$tmp" -name '*.stale.*' 2>/dev/null | wc -l | tr -d ' ')"
  if [[ "$residue" == "0" ]]; then
    pass "lane-lock AC2: no rename-aside residue exists anywhere under the case TMPDIR after the refusals — a refusing run mutated nothing"
  else
    fail "lane-lock-stale-residue: $residue path(s) — a refusal touched a lock it does not own"
  fi

  # ---- (iv) MUTANT CONTRAST: pid-less read as a dead holder, which IS the pre-fix behaviour (measured:
  # `reclaiming stale lock … (holder pid  not alive)` + ACQUIRED). One substitution, in the shipped
  # parse's absent-file branch.
  rm -rf "$lock"
  mkdir -p "$lock"
  local deadpid
  fixture_bg sleep 0.1
  deadpid=$FIXTURE_LAST_PID
  fixture_wait "$deadpid"
  ovr="$d/m-window.sh"; : >"$ovr"
  mrc=0
  if sv_mutant_override "$ovr" supervisor_lock_pid_read \
       "    printf '%s' 'unparseable pid-file-absent'" \
       "    printf '%s' 'pid $deadpid'"; then
    mout="$(lane_lock_drive_at "$d" "$ovr" "$tmp" "$lane")" || mrc=$?
    if [[ "$mrc" -eq 0 && "$mout" == *"ACQUIRED=$lock"* && "$mout" == *"reclaiming stale lock"* ]]; then
      pass "lane-lock AC2 MUTANT: with a pid-less lock reported as a (dead) holder pid, the SAME drive RECLAIMS a lock whose holder it never established — the measured pre-fix behaviour, so the refusals above are the probe doing the work"
    else
      fail "lane-lock-mutant-window: rc=$mrc out=[$mout] — the pre-fix form must be shown to reclaim"
    fi
  fi
  rm -rf "$lock" "$tmp"
}

t test_lane_lock_pidless_window_is_never_read_as_dead


# ---------------------------------------------------------------------------
# AC3 — LIVENESS IS THREE-VALUED, AND ONLY AN AFFIRMATIVE `dead` RECLAIMS.
#
# THE CASE IS REAL EPERM, NOT A SIMULATION OF IT. `kill -0 1` from a non-root uid fails with EPERM: pid 1
# EXISTS and is not ours to signal. Pre-fix that failure read as "not alive" and the lock was reclaimed
# (measured). This is the same errno a live holder owned by ANOTHER USER produces, which is the fleet
# case the two-valued test could not see.
# ---------------------------------------------------------------------------
test_lane_lock_liveness_is_three_valued() {
  local d tmp lane lock out rc ovr mout mrc live
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"
  lane="lane3601live$$"
  mkdir -p "$tmp"
  lock="$tmp/cqlite-worker-supervisor-$lane.lock"

  # ---- (1) A LIVE HOLDER WE CAN SIGNAL: refuse. The ordinary case, kept as the floor.
  rm -rf "$lock"; mkdir -p "$lock"
  fixture_bg sleep 30
  live=$FIXTURE_LAST_PID
  printf '%s\n' "$live" >"$lock/pid"
  out="$(lane_lock_drive_at "$d" - "$tmp" "$lane")"; rc=$?
  if [[ "$rc" -ne 0 && "$out" == *"already running (pid $live)"* && "$out" != *"reclaiming stale lock"* ]]; then
    pass "lane-lock AC3 (live, signallable): a live holder refuses and is named"
  else
    fail "lane-lock-live-signallable: rc=$rc out=[$out]"
  fi
  fixture_kill "$live"

  # ---- (2) A LIVE HOLDER WE CANNOT SIGNAL — REAL EPERM. This is the assert #3601 exists for.
  if kill -0 1 2>/dev/null; then
    skip "lane-lock AC3 (EPERM): this uid CAN signal pid 1 (running as root), so no EPERM case is stageable here — the property is unmeasurable on this host rather than passing vacuously"
  else
    rm -rf "$lock"; mkdir -p "$lock"
    printf '1\n' >"$lock/pid"
    out="$(lane_lock_drive_at "$d" - "$tmp" "$lane")"; rc=$?
    if [[ "$rc" -ne 0 ]] && [[ "$out" == *"already running (pid 1)"* ]] \
       && [[ "$out" != *"reclaiming stale lock"* ]] && [[ -f "$lock/pid" ]]; then
      pass "lane-lock AC3 (EPERM): a holder that EXISTS but is not ours to signal refuses — the errno that a live holder owned by another user produces is no longer read as death"
    else
      fail "lane-lock-eperm-reclaimed: rc=$rc out=[$out] — a live, unsignallable holder was read as dead; this is the measured pre-fix defect"
    fi

    # ---- MUTANT CONTRAST for (2): the pre-fix two-valued test, restored by ONE substitution — any
    # non-zero `kill -0` means dead. Both of the shipped probe's existence witnesses (procfs, and the
    # EPERM message) are bypassed by it, so the mutant is the pre-fix decision exactly.
    ovr="$d/m-live.sh"; : >"$ovr"
    mrc=0
    if sv_mutant_override "$ovr" supervisor_lock_holder_liveness \
         '  if [[ -d /proc/self && -d "/proc/$pid" ]]; then' \
         '  if [[ "$rc" -ne 0 ]]; then printf "%s" dead; return 0; fi
  if [[ -d /proc/self && -d "/proc/$pid" ]]; then'; then
      mout="$(lane_lock_drive_at "$d" "$ovr" "$tmp" "$lane")" || mrc=$?
      if [[ "$mrc" -eq 0 && "$mout" == *"ACQUIRED=$lock"* && "$mout" == *"reclaiming stale lock"* ]]; then
        pass "lane-lock AC3 MUTANT: with liveness collapsed back to the two-valued \`kill -0\`, the SAME drive RECLAIMS pid 1's lock — a LIVE process's lock, which is the pre-fix behaviour and the harm #3601 fixes"
      else
        fail "lane-lock-mutant-live: rc=$mrc out=[$mout] — the two-valued form must be shown to reclaim a live holder's lock"
      fi
    fi
  fi

  # ---- (3) THE PROBE'S THIRD VALUE EXISTS AND REFUSES. A pid `kill` cannot even interpret is neither
  # live nor dead; `unknown` must refuse, never reclaim. Reached through the probe directly, because the
  # parse (correctly) rejects such content before `acquire_lock` ever calls the probe — asserting it here
  # is what stops the third value being unreachable dead code that a later edit deletes as unused.
  local verdict
  verdict="$(env SUP="$SUPERVISOR" bash -c 'source "$SUP"; supervisor_lock_holder_liveness "not-a-pid"' 2>/dev/null || true)"
  if [[ "$verdict" == unknown* ]]; then
    pass "lane-lock AC3 (third value): a pid the kernel cannot be asked about yields \`$verdict\` — not \`dead\`, so nothing downstream can read an unanswerable probe as a licence to reclaim"
  else
    fail "lane-lock-liveness-not-three-valued: verdict=[$verdict] — a probe that cannot answer must say so"
  fi
  # ...and the two answerable values, from the same probe, so the three-valued claim is measured whole.
  verdict="$(env SUP="$SUPERVISOR" bash -c 'source "$SUP"; supervisor_lock_holder_liveness "$$"' 2>/dev/null || true)"
  if [[ "$verdict" == live ]]; then
    pass "lane-lock AC3: the probe reports \`live\` for a process that demonstrably exists (its own pid)"
  else
    fail "lane-lock-liveness-live: verdict=[$verdict]"
  fi
  local dead2
  fixture_bg sleep 0.1
  dead2=$FIXTURE_LAST_PID
  fixture_wait "$dead2"
  verdict="$(env SUP="$SUPERVISOR" DP="$dead2" bash -c 'source "$SUP"; supervisor_lock_holder_liveness "$DP"' 2>/dev/null || true)"
  if [[ "$verdict" == dead ]]; then
    pass "lane-lock AC3: the probe reports \`dead\` for a reaped process — the affirmative verdict the reclaim requires is reachable, so the fix has not made every lock permanent"
  else
    fail "lane-lock-liveness-dead: verdict=[$verdict] — without a reachable \`dead\` nothing ever clears a stale lock automatically"
  fi

  rm -rf "$lock" "$tmp"
}

t test_lane_lock_liveness_is_three_valued


# ---------------------------------------------------------------------------
# AC4 — THE "ALREADY RUNNING" REFUSAL DOES NOT ASSERT AN IDENTITY IT NEVER CHECKED.
#
# It has established that the recorded pid EXISTS. It has NOT established that the process is a
# supervisor, and pids are REUSED — so a lock abandoned by a dead holder can name a number the kernel
# has since given to something unrelated. AC4 allows either corroborating the identity or declaring the
# gap; the gap is declared, because the verdict could not change the decision (`live` refuses whatever
# the process is) and #3549's ruling on exactly that shape is that such machinery is a description
# generator on the decision path, not a guard.
# ---------------------------------------------------------------------------
test_lane_lock_running_refusal_declares_unverified_identity() {
  local d tmp lane lock out rc live ovr mout
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"
  lane="lane3601ident$$"
  mkdir -p "$tmp"
  lock="$tmp/cqlite-worker-supervisor-$lane.lock"
  mkdir -p "$lock"
  fixture_bg sleep 30
  live=$FIXTURE_LAST_PID
  printf '%s\n' "$live" >"$lock/pid"

  out="$(lane_lock_drive_at "$d" - "$tmp" "$lane")"; rc=$?
  if [[ "$rc" -ne 0 && "$out" == *"already running (pid $live)"* ]]; then
    pass "lane-lock AC4 PREMISE: the running refusal was reached, so the text below has a subject"
  else
    fail "lane-lock-ac4-premise: rc=$rc out=[$out]"
  fi
  if [[ "$out" == *"IDENTITY IS NOT VERIFIED"* ]] && [[ "$out" == *"pids are REUSED"* ]]; then
    pass "lane-lock AC4: the refusal states plainly that it did NOT verify the pid is a supervisor and that pids are reused — the scope is declared in the text an operator reads, which is what stops the message asserting a fact it never established"
  else
    fail "lane-lock-ac4-unqualified-identity: out=[$out] — 'another instance is already running (pid N)' with no scope statement asserts an identity the run never checked"
  fi
  # The refusal must also hand the operator the read-only line that ANSWERS the question it left open.
  if [[ "$out" == *"ps -p $live -o pid,ppid,user,lstart,args"* ]]; then
    pass "lane-lock AC4: it names the read-only command that settles the identity it did not check — the declared gap comes with the way to close it"
  else
    fail "lane-lock-ac4-no-identity-probe-offered: out=[$out]"
  fi

  # ---- MUTANT CONTRAST: the pre-fix wording, restored by one substitution. The assert above must be
  # shown to red on exactly that text, or it is a green over a string that was never absent.
  ovr="$d/m-ident.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" acquire_lock \
       '        "pid $holder_pid is recorded as this lane'"'"'s lock holder and that process EXISTS (verified: it is signallable, or the kernel reports it exists but is not ours to signal). ITS IDENTITY IS NOT VERIFIED — this run did not check that pid $holder_pid is a worker-supervisor, and pids are REUSED, so a lock abandoned by a dead holder can name a number that now belongs to an unrelated process" \' \
       '        "another instance is already running (pid $holder_pid)" \'; then
    mout="$(lane_lock_drive_at "$d" "$ovr" "$tmp" "$lane")" || true
    if [[ "$mout" == *"already running (pid $live)"* ]] && [[ "$mout" != *"IDENTITY IS NOT VERIFIED"* ]]; then
      pass "lane-lock AC4 MUTANT: with the scope statement removed the refusal still fires and now claims 'another instance' with nothing behind it — so the assert above reds on exactly the pre-fix text"
    else
      fail "lane-lock-mutant-ident: out=[$mout] — the pre-fix wording must be shown to lose the scope statement"
    fi
  fi
  fixture_kill "$live"
  rm -rf "$lock" "$tmp"
}

t test_lane_lock_running_refusal_declares_unverified_identity


# ---------------------------------------------------------------------------
# AC7 — AN OPTION-SHAPED `TMPDIR` MUST NOT STOP THE LANE FROM STARTING.
#
# QUOTING IS NOT OPTION-SAFETY. `"$SUPERVISOR_LOCK"` stops word-splitting and globbing and does nothing
# about option parsing: `mkdir`, `mv` and `rm` read a leading `-` in an operand as flags whatever quoting
# produced it. MEASURED pre-fix with `TMPDIR=-scratch`: `reclaiming stale lock -scratch/…` followed by
# `failed to acquire lock -scratch/…` — the lane cannot start, and the diagnostic blames a stale lock
# that does not exist, which is the expensive half.
#
# BOTH PATHS ARE DRIVEN, because they use different operands: the fresh-claim path (`mkdir`, and the
# publish's `mv`) and the reclaim path (`mv` aside, `rm -rf`). The pid is read by REDIRECTION, which
# parses no options at all, so the `cat` the addendum named is gone rather than terminated.
# ---------------------------------------------------------------------------
test_lane_lock_option_shaped_tmpdir_starts_normally() {
  local d optdir opttmp lane lock out rc dead ovr mout mrc
  d="$(new_case_dir)"
  common_env "$d"
  optdir="$d/optshaped"
  opttmp="-scratch"
  lane="lane3601opt$$"
  mkdir -p "$optdir/$opttmp"
  lock="$optdir/$opttmp/cqlite-worker-supervisor-$lane.lock"

  # ---- (1) THE FRESH CLAIM.
  rm -rf "$lock"
  out="$(lane_lock_drive_at "$optdir" - "$opttmp" "$lane")"; rc=$?
  if [[ "$rc" -eq 0 ]] && [[ "$out" == *"ACQUIRED=$opttmp/cqlite-worker-supervisor-$lane.lock"* ]] \
     && [[ "$out" == *"LOCKDIR=yes"* ]] && [[ "$out" != *"reclaiming stale lock"* ]] \
     && [[ "$out" != *"failed to acquire"* ]]; then
    pass "lane-lock AC7 (fresh claim): with a relative, option-shaped TMPDIR the lock is acquired NORMALLY — no invented stale lock, no refusal"
  else
    fail "lane-lock-optshaped-fresh: rc=$rc out=[$out] — an option-shaped TMPDIR must not stop the lane from starting"
  fi

  # ---- (2) THE RECLAIM PATH, same TMPDIR shape: `mv` aside and `rm -rf` both take the option-shaped
  # operand, and neither is exercised by (1).
  fixture_bg sleep 0.1
  dead=$FIXTURE_LAST_PID
  fixture_wait "$dead"
  rm -rf "$lock"
  mkdir -p "$lock"
  printf '%s\n' "$dead" >"$lock/pid"
  out="$(lane_lock_drive_at "$optdir" - "$opttmp" "$lane")"; rc=$?
  if [[ "$rc" -eq 0 && "$out" == *"reclaiming stale lock"* && "$out" == *"LOCKDIR=yes"* ]]; then
    pass "lane-lock AC7 (reclaim): the rename-aside and removal operands are option-safe too — a genuinely stale lock under an option-shaped TMPDIR is still cleared automatically"
  else
    fail "lane-lock-optshaped-reclaim: rc=$rc out=[$out]"
  fi
  local residue
  residue="$(find "$optdir" -name '*.stale.*' 2>/dev/null | wc -l | tr -d ' ')"
  if [[ "$residue" == "0" ]]; then
    pass "lane-lock AC7 (reclaim): the renamed-aside copy was removed — the `rm -rf` operand parsed as a path, not as flags"
  else
    fail "lane-lock-optshaped-aside-leak: $residue leftover aside path(s) under $optdir — the removal's operand was eaten as an option"
  fi

  # ---- (3) MUTANT CONTRAST: strip the `--` from the claim's `mkdir`, which is the pre-fix spelling.
  # Without it the identical case must FAIL, or the `--` in the shipped line is decoration.
  rm -rf "$lock"
  ovr="$d/m-opt.sh"; : >"$ovr"
  mrc=0
  if sv_mutant_override "$ovr" supervisor_lock_take \
       '  mkdir -- "$SUPERVISOR_LOCK" 2>/dev/null || return 1' \
       '  mkdir "$SUPERVISOR_LOCK" 2>/dev/null || return 1'; then
    mout="$(lane_lock_drive_at "$optdir" "$ovr" "$opttmp" "$lane")" || mrc=$?
    if [[ "$mrc" -ne 0 && "$mout" != *"ACQUIRED="* ]]; then
      pass "lane-lock AC7 MUTANT: with \`--\` stripped from the claim's mkdir the SAME case FAILS to start (rc=$mrc) — so the terminator is load-bearing, not decoration"
    else
      fail "lane-lock-mutant-opt: rc=$mrc out=[$mout] — the pre-fix spelling must be shown to break here"
    fi
  fi
  # ...and the same for the reclaim's `mv`, whose operand (1) and (3) never reach.
  rm -rf "$lock"
  mkdir -p "$lock"
  printf '%s\n' "$dead" >"$lock/pid"
  ovr="$d/m-opt-mv.sh"; : >"$ovr"
  mrc=0
  if sv_mutant_override "$ovr" acquire_lock \
       '  if mv -f -- "$SUPERVISOR_LOCK" "$SUPERVISOR_LOCK.stale.$$" 2>/dev/null; then' \
       '  if mv -f "$SUPERVISOR_LOCK" "$SUPERVISOR_LOCK.stale.$$" 2>/dev/null; then'; then
    mout="$(lane_lock_drive_at "$optdir" "$ovr" "$opttmp" "$lane")" || mrc=$?
    if [[ "$mrc" -ne 0 && "$mout" != *"ACQUIRED="* ]]; then
      pass "lane-lock AC7 MUTANT (reclaim mv): with \`--\` stripped from the rename-aside the SAME reclaim FAILS (rc=$mrc) — the reclaim path's terminator is load-bearing on its own"
    else
      fail "lane-lock-mutant-opt-mv: rc=$mrc out=[$mout]"
    fi
  fi

  rm -rf "$optdir"
}

t test_lane_lock_option_shaped_tmpdir_starts_normally


# ---------------------------------------------------------------------------
# THE OWNERSHIP HALF, FOUND BY THE CALL-SITE SWEEP THIS ISSUE MANDATES (#3601).
#
# AC1-AC3 stop us TAKING a lock we do not own. The same defect class points the other way at the same
# call site and had no acceptance criterion: the EXIT trap was an unconditional
# `rm -rf "$SUPERVISOR_LOCK"`, so a run that had LOST its lock (a peer running pre-#3601 code reclaims a
# pid-less lock, which ours is for one rename at startup) deleted the NEW holder's lock on the way out,
# handing the lane to a third process while the second believed it held exclusion.
#
# Both halves are decided by the same ONE observation — does the lock's pid file read back as OUR pid —
# which is #3549 lead ruling 5 applied: the question "do we hold this lock?" is answered by a single
# reading rather than by a tuple of pid, liveness and identity agreeing.
# ---------------------------------------------------------------------------
test_lane_lock_release_only_removes_a_lock_we_still_own() {
  local d tmp lane lock out rc ovr mout foreign
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"
  lane="lane3601own$$"
  mkdir -p "$tmp"
  lock="$tmp/cqlite-worker-supervisor-$lane.lock"
  foreign=424242

  # A drive that acquires the lock and then has the lock STOLEN (its pid file replaced) before exiting,
  # so the EXIT trap runs against a lock that is no longer ours. Derived from the shipped drive body by
  # inserting one statement before its exit, so it cannot drift into a different startup path.
  local body_steal="${SV_DRIVE_BODY/'exit 0'/'printf "%s\n" "$2" >"$SUPERVISOR_LOCK/pid"; exit 0'}"
  if [[ "$body_steal" != "$SV_DRIVE_BODY" && "$body_steal" == *'"$SUPERVISOR_LOCK/pid"'* ]]; then
    pass "lane-lock ownership PREMISE: the steal drive is derived from the shipped drive body by one insertion"
  else
    fail "lane-lock-steal-drive-premise: [$body_steal]"
    return 0
  fi

  rm -rf "$lock"
  out="$( cd "$d" && env -u SUPERVISOR_LOCK TMPDIR="$tmp" LANE_ID="$lane" LOCK_CMD="" CLAIM_CMD="" \
            bash -c "$body_steal" _ "$SUPERVISOR" "$foreign" 2>&1 )"; rc=$?
  if [[ "$rc" -eq 0 && "$out" == *"ACQUIRED=$lock"* ]]; then
    pass "lane-lock ownership PREMISE: the run acquired the lock, then the lock was taken from it"
  else
    fail "lane-lock-ownership-premise: rc=$rc out=[$out]"
  fi
  if [[ -d "$lock" ]] && [[ "$(cat "$lock/pid" 2>/dev/null || true)" == "$foreign" ]]; then
    pass "lane-lock ownership: the EXIT trap left the lock ALONE because it no longer held our pid — a run that lost its lock does not delete the new holder's"
  else
    fail "lane-lock-release-deleted-foreign: lock=[$([[ -d "$lock" ]] && echo present || echo GONE)] pid=[$(cat "$lock/pid" 2>/dev/null || true)] — the exit path removed a lock this run did not own"
  fi
  # The fixture writes a PARSED foreign pid, so B17's foreign branch is the one that must fire — matched
  # on the wording that branch now uses (#3601, roborev job 240 B17 reworded this: "no longer holds this
  # process's pid" was said for unparseable states too, where no holder had been read).
  if [[ "$out" == *"NOT removing"* && "$out" == *"records holder pid $foreign, not this process"* ]]; then
    pass "lane-lock ownership: and it SAYS so, naming the holder it read — 'my lock vanished' is otherwise unattributable, so the non-removal is logged rather than silent"
  else
    fail "lane-lock-release-silent: out=[$out]"
  fi

  # ---- MUTANT CONTRAST: the pre-fix unconditional removal, by one substitution in the shipped release.
  rm -rf "$lock"
  ovr="$d/m-release.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" supervisor_lock_release \
       '  if [[ "$state" == "pid $$" ]]; then' \
       '  if true; then'; then
    local body_steal_ovr="${SV_DRIVE_BODY_OVERRIDE/'exit 0'/'printf "%s\n" "$3" >"$SUPERVISOR_LOCK/pid"; exit 0'}"
    mout="$( cd "$d" && env -u SUPERVISOR_LOCK TMPDIR="$tmp" LANE_ID="$lane" LOCK_CMD="" CLAIM_CMD="" \
               bash -c "$body_steal_ovr" _ "$SUPERVISOR" "$ovr" "$foreign" 2>&1 )" || true
    if [[ "$mout" == *"ACQUIRED=$lock"* ]] && [[ ! -d "$lock" ]]; then
      pass "lane-lock ownership MUTANT: with the ownership test removed the identical drive DELETES the new holder's lock on exit — so the test above is what protects it"
    else
      fail "lane-lock-mutant-release: out=[$mout] lock=[$([[ -d "$lock" ]] && echo present || echo GONE)] — the unconditional removal must be shown to destroy a foreign lock"
    fi
  fi

  rm -rf "$lock" "$tmp"
}

t test_lane_lock_release_only_removes_a_lock_we_still_own


# ---------------------------------------------------------------------------
# THE PUBLISH IS VERIFIED, NOT ASSUMED (#3601, AC2's other half).
#
# `mkdir` makes the NAME ours; it does not make the lock ours, because a peer running pre-#3601 code
# reclaims a pid-less lock and our lock is pid-less for exactly one rename. So the publish READS BACK
# what it wrote and requires our own pid. The pair of mutants below is what shows the read-back is the
# thing refusing, and not something else: the first forges a foreign pid into the publish (the shipped
# read-back must catch it), the second forges the SAME pid and blinds the read-back (it must not).
# ---------------------------------------------------------------------------
test_lane_lock_publish_is_read_back_before_it_is_trusted() {
  local d tmp lane lock ovr out rc
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"
  lane="lane3601pub$$"
  mkdir -p "$tmp"
  lock="$tmp/cqlite-worker-supervisor-$lane.lock"

  # ---- (1) A clean claim publishes OUR pid and leaves no temporary behind: the rename is what makes the
  # `pid` NAME appear only with complete content, so a `pid.tmp.*` survivor would mean the publish is
  # back to create-then-write.
  rm -rf "$lock"
  local body_keep="${SV_DRIVE_BODY/'exit 0'/'printf "KEPT=%s\n" "$(ls -A "$SUPERVISOR_LOCK" | tr "\n" " ")"; printf "PID=%s\n" "$(<"$SUPERVISOR_LOCK/pid")"; trap - EXIT; exit 0'}"
  out="$( cd "$d" && env -u SUPERVISOR_LOCK TMPDIR="$tmp" LANE_ID="$lane" LOCK_CMD="" CLAIM_CMD="" \
            bash -c "$body_keep" _ "$SUPERVISOR" 2>&1 )"; rc=$?
  if [[ "$rc" -eq 0 && "$out" == *"KEPT=pid "* && "$out" != *"pid.tmp."* ]]; then
    pass "lane-lock publish: the acquired lock holds exactly one file, \`pid\`, with no staging temporary left behind — the name is published by rename, so a reader never sees it holding partial content"
  else
    fail "lane-lock-publish-residue: rc=$rc out=[$out] — a leftover staging file means the publish is not a rename"
  fi
  rm -rf "$lock"

  # ---- (2) FORGE A FOREIGN PID INTO THE PUBLISH: the read-back must refuse.
  ovr="$d/m-pub-forge.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" supervisor_lock_publish \
       '  if ! { printf '"'"'%s\n'"'"' "$$" >"$tmpf"; } 2>/dev/null; then' \
       '  if ! { printf '"'"'%s\n'"'"' 424243 >"$tmpf"; } 2>/dev/null; then'; then
    out="$(lane_lock_drive_at "$d" "$ovr" "$tmp" "$lane")"; rc=$?
    if [[ "$rc" -ne 0 && "$out" != *"ACQUIRED="* && "$out" == *"could not verify it OWNS it"* ]]; then
      pass "lane-lock publish: when the lock does not read back as ours the run REFUSES and says which step failed — the read-back has teeth"
    else
      fail "lane-lock-publish-unverified: rc=$rc out=[$out] — a lock that does not hold our pid must never be treated as acquired"
    fi
  fi
  rm -rf "$lock"

  # ---- (3) NON-VACUITY: the same forgery with the read-back BLINDED must acquire. Without this, (2)
  # could be passing because of the forgery rather than because of the check.
  ovr="$d/m-pub-blind.sh"; : >"$ovr"
  # THE TARGET IS THE COMPARISON, NOT THE READ ITSELF: B11 added a second, identical read-back line in
  # the decline branch, so a substitution on the read would no longer be unique — `sv_mutant_override`
  # correctly refused it as a moved source. The comparison occurs once and expresses the same mutation
  # (forge a foreign pid, then accept regardless of what came back).
  if sv_mutant_override "$ovr" supervisor_lock_publish \
       '  if [[ "$state" == "pid $$" ]]; then' \
       '  printf '"'"'%s\n'"'"' 424243 >"$SUPERVISOR_LOCK/pid" 2>/dev/null || true
  if true; then'; then
    out="$(lane_lock_drive_at "$d" "$ovr" "$tmp" "$lane")"; rc=$?
    if [[ "$rc" -eq 0 && "$out" == *"ACQUIRED=$lock"* ]]; then
      pass "lane-lock publish MUTANT: with the read-back blinded the SAME forged publish is accepted and the run starts holding a lock whose pid is someone else's — so (2) is the read-back refusing, not the forgery failing"
    else
      fail "lane-lock-mutant-publish: rc=$rc out=[$out] — the blinded form must be shown to accept a foreign lock"
    fi
  fi

  rm -rf "$lock" "$tmp"
}

t test_lane_lock_publish_is_read_back_before_it_is_trusted


# ---------------------------------------------------------------------------
# A FAILED `mkdir` IS NOT EVIDENCE OF CONTENTION (#3601).
#
# The addendum's expensive half was the DIAGNOSIS, not the failure: pre-fix, an option-shaped `TMPDIR`
# produced `reclaiming stale lock …` then `failed to acquire lock …`, sending an operator to hunt a
# stale lock that did not exist. `mkdir` also fails when the parent is missing, is not a directory, or
# is not writable — so the cause is checked before it is attributed to a holder.
# ---------------------------------------------------------------------------
test_lane_lock_uncreatable_path_is_not_reported_as_contention() {
  local d lane out rc ro
  d="$(new_case_dir)"
  common_env "$d"
  lane="lane3601nopath$$"
  # THE `TMPDIR` MUST EXIST AND BE SEARCHABLE, AND ONLY THEN UNWRITABLE. An absent container is refused
  # EARLIER, by the legacy guard's existence probe ("cannot tell" — measured), so it never reaches
  # `mkdir` and would test the wrong refusal. Read-and-search but not write (0555) is the shape that
  # reaches the claim and fails there.
  ro="$d/readonly-tmp"
  mkdir -p "$ro"
  chmod 0555 "$ro" 2>/dev/null || true
  if ( : >"$ro/.probe" ) 2>/dev/null; then
    rm -f "$ro/.probe" 2>/dev/null || true
    chmod 0755 "$ro" 2>/dev/null || true
    skip "lane-lock (uncreatable path): this uid can write a 0555 directory (running as root), so an uncreatable lock path is not stageable here — unmeasurable on this host rather than passing vacuously"
    return 0
  fi
  out="$(lane_lock_drive_at "$d" - "$ro" "$lane")"; rc=$?
  chmod 0755 "$ro" 2>/dev/null || true
  # B21 removed the confident "NOT contention": a post-failure existence test cannot establish the cause.
  # What must survive is the OBSERVATION (nothing is at that name) and the operator's first check (the
  # parent), which is the half of #3601's AC7 fix that has operator value.
  if [[ "$rc" -ne 0 ]] && [[ "$out" == *"NOTHING is at that name now"* ]] \
     && [[ "$out" == *"check the PARENT directory"* ]] && [[ "$out" == *"NOT ESTABLISHED"* ]] \
     && [[ "$out" != *"reclaiming stale lock"* ]] \
     && [[ "$out" != *"already running"* ]]; then
    pass "lane-lock (uncreatable path): a lock that cannot be created is reported as a PATH problem and explicitly not as contention — no invented stale lock, no invented holder"
  else
    fail "lane-lock-uncreatable-misdiagnosed: rc=$rc out=[$out] — a failed mkdir must not be attributed to a holder"
  fi
}

t test_lane_lock_uncreatable_path_is_not_reported_as_contention




# ---------------------------------------------------------------------------
# `log_size`: A FAILED MEASUREMENT IS NOT A VALUE (#3601, the issue's fourth item).
#
# `log_size` returned the EMPTY STRING when `wc` failed, and empty collapses to `0` in the caller's
# `-eq` comparison — so two UNMEASURABLE reads compared EQUAL and the wedge detector read a healthy,
# growing log as FROZEN. That is the "an empty probe is not a zero" shape: a measurement that did not
# happen became the value 0. It was mitigated, not prevented, by the conjoined prompt-signature
# requirement, which is why it is fixed with the rest of this family rather than left as a landmine.
#
# BOTH LEVELS ARE DRIVEN: the probe's own three answers, and the CONSUMER — because a probe returning a
# sentinel nobody checks is not a fix.
# ---------------------------------------------------------------------------
test_log_size_unmeasurable_is_not_zero() {
  local d shadow out absent present unmeasurable
  d="$(new_case_dir)"
  common_env "$d"
  printf '12345' >"$d/some.log"

  absent="$(env SUP="$SUPERVISOR" F="$d/no-such.log" bash -c 'source "$SUP"; log_size "$F"' 2>/dev/null || true)"
  present="$(env SUP="$SUPERVISOR" F="$d/some.log" bash -c 'source "$SUP"; log_size "$F"' 2>/dev/null || true)"
  # `wc` FAILS FOR REAL — a shadow earlier on `PATH`, not a stubbed function — because the defect is
  # what happens when the external tool this probe depends on cannot answer.
  shadow="$d/shadow"
  mkdir -p "$shadow"
  printf '%s\n' '#!/usr/bin/env bash' 'exit 1' >"$shadow/wc"
  chmod +x "$shadow/wc"
  # TWO CALLER SHAPES, AND THE DIFFERENCE IS THE POINT. The BARE call runs the probe in the caller's own
  # shell, which inherits the supervisor's `set -euo pipefail`; the WRAPPED call runs it inside a command
  # substitution, where errexit is NOT inherited. A failing `wc` makes an internal pipeline non-zero, and
  # in the bare shape that aborts the caller BEFORE the probe classifies anything — measured: the bare
  # call produced NO OUTPUT AT ALL, no sentinel and no error, i.e. the probe killed its caller instead of
  # answering. The pre-#3601 form was safe only because its one caller sits inside a `set +e` region, and
  # a probe whose correctness depends on that is one edit away from taking the supervisor down.
  local unmeasurable_bare=''
  unmeasurable_bare="$(env SUP="$SUPERVISOR" F="$d/some.log" PATH="$shadow:$PATH" bash -c 'source "$SUP"; log_size "$F"' 2>/dev/null || true)"
  unmeasurable="$(env SUP="$SUPERVISOR" F="$d/some.log" PATH="$shadow:$PATH" bash -c 'source "$SUP"; printf "%s" "$(log_size "$F")"' 2>/dev/null || true)"

  if [[ "$absent" == "0" && "$present" == "5" && "$unmeasurable" == "-1" ]]; then
    pass "log_size: absent=[0] (a real zero), present=[5] (a real count), unmeasurable=[-1] (a NAMED non-value) — three answers, so a failed measurement can never be mistaken for a byte count"
  else
    fail "log-size-collapses: absent=[$absent] present=[$present] unmeasurable=[$unmeasurable] — an unmeasurable read must not come back as an empty string (which \`-eq\` reads as 0) or as any real count"
  fi
  if [[ "$unmeasurable_bare" == "-1" ]]; then
    pass "log_size (errexit caller): the BARE call from a shell carrying the supervisor's own \`set -euo pipefail\` still returns the sentinel — the probe answers rather than aborting its caller"
  else
    fail "log-size-aborts-errexit-caller: bare=[$unmeasurable_bare] — a failing internal pipeline aborted the caller before the probe could classify anything, so it returns no sentinel at all"
  fi

  # ---- THE CONSUMER, BEHAVIOURALLY. Same wedge scenario as the genuine-wedge case (a worker that
  # prints the prompt signature and then stops), with `wc` shadowed so the size is UNMEASURABLE on every
  # scan. The wedge must NOT be confirmed: two failed reads are not evidence of a frozen log. Pre-fix
  # this reported `stuck-on-question` and paged the owner, from a measurement that never happened.
  local d2 call_ctr jf rc scount acount fcount
  d2="$(new_case_dir)"
  common_env "$d2"
  call_ctr="$d2/calls"
  write_stuck_then_finalize_stub "$d2/bin/worker.sh" "$call_ctr"
  export WORKER_CMD="$d2/bin/worker.sh"
  export MAX_ISSUES=1
  export BREAKER_N=2
  export STUCK_POLL_SECS=1
  export MAX_ITER_SECS=5
  jf="$JOURNAL_FILE"
  mkdir -p "$d2/shadow"
  printf '%s\n' '#!/usr/bin/env bash' 'exit 1' >"$d2/shadow/wc"
  chmod +x "$d2/shadow/wc"

  env PATH="$d2/shadow:$PATH" bash -c "$SV_MAIN_DRIVE_BODY" _ "$SUPERVISOR" '' >"$d2/stdout.log" 2>&1
  rc=$?
  scount=$(jline_count "$jf" '"outcome":"stuck-on-question"')
  acount=$(jline_count "$jf" '"outcome":"abnormal"')
  fcount=$(jline_count "$jf" '"outcome":"finalized"')
  if [[ "$rc" -eq 0 && "$scount" -eq 0 && "$acount" -eq 1 && "$fcount" -eq 1 ]]; then
    pass "log_size CONSUMER: with the byte size UNMEASURABLE on every scan the wedge is NOT confirmed (stuck=0) and the iteration is judged on what IS known (abnormal, then finalize) — two failed reads are not a frozen log"
  else
    fail "log-size-consumer-false-wedge: rc=$rc stuck=$scount abnormal=$acount finalized=$fcount (see $jf) — an unmeasurable size must not confirm a wedge"
  fi

  # ---- MUTANT CONTRAST: the pre-fix probe, restored by one substitution — an unmeasurable read comes
  # back EMPTY. `-eq` then reads it as 0, both scans agree, and the same run reports a wedge that never
  # happened. Driven through the probe AND through the consumer, because the empty value is harmless
  # until something compares it.
  local ovr mval
  ovr="$d/m-logsize.sh"; : >"$ovr"
  # THE PREMISE THIS CONTRAST NEEDS, ASSERTED RATHER THAN COMMENTED: control and mutant reach the
  # supervisor's `main` through ONE body, so the only difference between the two runs is the override.
  if [[ "$SV_MAIN_DRIVE_BODY" == *'source "$1"'* && "$SV_MAIN_DRIVE_BODY" == *'main'* \
        && "$SV_MAIN_DRIVE_BODY" != *'2>/dev/null'* ]]; then
    pass "log_size CONTRAST PREMISE: control and mutant share one entry body that sources the shipped supervisor and calls its own \`main\`, with no error suppression around the source — so the contrast below isolates the probe"
  else
    fail "log-size-contrast-premise: [$SV_MAIN_DRIVE_BODY] — control and mutant must differ only in the override file"
  fi
  if sv_mutant_override "$ovr" log_size \
       "      printf '%s' '-1'" \
       "      printf '%s' ''"; then
    mval="$(env SUP="$SUPERVISOR" OVR="$ovr" F="$d/some.log" PATH="$shadow:$PATH" \
             bash -c 'source "$SUP"; source "$OVR"; printf "[%s]" "$(log_size "$F")"' 2>/dev/null || true)"
    if [[ "$mval" == "[]" ]]; then
      pass "log_size MUTANT: the pre-fix probe returns the EMPTY STRING for an unmeasurable read (mval=$mval) — which \`[[ x -eq y ]]\` reads as 0, so two failures compared equal; the assert above reds on exactly that"
    else
      fail "log-size-mutant: mval=[$mval] — the pre-fix form must be shown to return empty"
    fi
    # ...and the consumer, with that same pre-fix probe, DOES confirm the false wedge. This is the harm,
    # measured rather than argued.
    local d3 jf3 mscount
    d3="$(new_case_dir)"
    common_env "$d3"
    write_stuck_then_finalize_stub "$d3/bin/worker.sh" "$d3/calls"
    export WORKER_CMD="$d3/bin/worker.sh"
    export MAX_ISSUES=1
    export BREAKER_N=2
    export STUCK_POLL_SECS=1
    export MAX_ITER_SECS=5
    jf3="$JOURNAL_FILE"
    mkdir -p "$d3/shadow"
    printf '%s\n' '#!/usr/bin/env bash' 'exit 1' >"$d3/shadow/wc"
    chmod +x "$d3/shadow/wc"
    # THE SAME ENTRY POINT AS THE CONTROL, WHICH IS THE ONLY THING THAT MAKES THIS A CONTRAST (#3601,
    # roborev job 231 B4). An earlier cut ran the control as `bash "$SUPERVISOR"` and the mutant as
    # `bash -c 'source …; main "$@"'` with a `2>/dev/null || true` on the source that additionally
    # swallowed errexit during startup — two differences besides the probe, under a comment that claimed
    # there were none. A mutant IS the evidence for its property, so a comment asserting an isolation the
    # code does not provide is a false claim in the artifact a reviewer is asked to trust. Both sides now
    # run `$SV_MAIN_DRIVE_BODY` and differ in ONE argument: the override file, empty for the control.
    env PATH="$d3/shadow:$PATH" bash -c "$SV_MAIN_DRIVE_BODY" _ "$SUPERVISOR" "$ovr" >"$d3/stdout.log" 2>&1 || true
    mscount=$(jline_count "$jf3" '"outcome":"stuck-on-question"')
    if [[ "$mscount" -ge 1 ]]; then
      pass "log_size CONSUMER MUTANT: with the pre-fix probe the identical run reports a wedge (stuck=$mscount) from a measurement that never happened — so the consumer assert above is the sentinel doing the work"
    else
      fail "log-size-consumer-mutant: stuck=$mscount (see $jf3) — the pre-fix form must be shown to produce the false wedge, or the consumer assert measures nothing"
    fi
  fi
  unset WORKER_CMD
}

t test_log_size_unmeasurable_is_not_zero




# ---------------------------------------------------------------------------
# NUL BYTES IN THE PID FILE (#3601, roborev job 231) — the hole in AC1's own guarantee.
#
# THE DEFECT, MEASURED BEFORE THE FIX: bash cannot hold a NUL and `read` discards it silently, so a pid
# file whose BYTES are `<dead-pid> NUL LF` reads back as a clean `<dead-pid>` — non-empty, single line,
# all decimal digits, non-zero. Every gate in `supervisor_lock_pid_read` passed it, the liveness probe
# then said `dead`, and the lock was RECLAIMED. That is exactly the outcome AC1 exists to prevent, from
# exactly the input AC1 is about: a partially-written pid file, which is where NULs come from, because a
# crash mid-write can leave allocated-but-zeroed bytes.
#
# SO THE CHECK CANNOT LIVE IN THE SHELL, and this case asserts that premise rather than assuming it: it
# first demonstrates that the shell's own view of the file is a clean all-digit pid, and only then that
# the parser refuses anyway. Without the first half a green here would not show that anything hard was
# happening.
#
# NOT VIA BASH'S WARNING, EITHER. `$(cat …)` on such a file emits `warning: … ignored null byte in
# input` — on STDERR, which the `2>/dev/null` in use at every one of these sites already suppresses; and
# keying correctness on bash's message TEXT would be the cargo-status-word defect class (CLAUDE.md
# #3400). The shipped check measures bytes.
# ---------------------------------------------------------------------------
test_lane_lock_nul_bearing_pid_file_refuses() {
  local d tmp lane lock out rc dead verdict shell_view ovr mout mrc shadow
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"
  lane="lane3601nul$$"
  mkdir -p "$tmp"
  lock="$tmp/cqlite-worker-supervisor-$lane.lock"

  fixture_bg sleep 0.1
  dead=$FIXTURE_LAST_PID
  fixture_wait "$dead"

  mkdir -p "$lock"
  # REAL BYTES. `printf '%s\000\n'` writes an actual NUL; nothing here simulates one, because a simulated
  # NUL would be visible to the shell and would therefore not reproduce the defect at all.
  printf '%s\000\n' "$dead" >"$lock/pid"
  local raw_bytes stripped_bytes
  raw_bytes="$(wc -c <"$lock/pid" | tr -d '[:space:]')"
  stripped_bytes="$(tr -d '\000' <"$lock/pid" | wc -c | tr -d '[:space:]')"
  if [[ "$raw_bytes" -eq $((stripped_bytes + 1)) ]]; then
    pass "lane-lock NUL PREMISE: the staged pid file really carries a NUL byte — $raw_bytes bytes on disk, $stripped_bytes with NULs stripped"
  else
    fail "lane-lock-nul-premise: raw=$raw_bytes stripped=$stripped_bytes — the fixture does not contain a NUL, so this case measures nothing"
    return 0
  fi
  # ...AND THE SHELL CANNOT SEE IT. This is the half that shows why a byte-level check is required.
  shell_view="$(env F="$lock/pid" bash -c 'IFS= read -r v <"$F"; printf "%s|%s" "$v" "${#v}"' 2>/dev/null || true)"
  if [[ "$shell_view" == "$dead|${#dead}" ]]; then
    pass "lane-lock NUL PREMISE: \`read\` hands the shell a CLEAN all-digit pid [$shell_view] — non-empty, single line, all digits, non-zero — so no check running on that value can possibly reject this file"
  else
    fail "lane-lock-nul-shell-view: [$shell_view] expected [$dead|${#dead}] — the premise of this case is that the NUL is invisible to the shell"
  fi

  # ---- (1) THE PARSER REFUSES, WITH ITS OWN NAMED CAUSE, consistent with the other gates.
  verdict="$(env SUP="$SUPERVISOR" F="$lock/pid" bash -c 'source "$SUP"; printf "%s" "$(supervisor_lock_pid_read "$F")"' 2>/dev/null || true)"
  if [[ "$verdict" == 'unparseable pid-file-contains-nul' ]]; then
    pass "lane-lock NUL: the parser reads the FILE'S BYTES and refuses with its own cause [$verdict] — it is \`unparseable\`, never \`dead\`"
  else
    fail "lane-lock-nul-accepted: verdict=[$verdict] — a NUL-bearing pid file must not parse as a pid"
  fi

  # ---- (2) END TO END: refuse, reclaim nothing, leave the lock exactly as found, print the way out.
  out="$(lane_lock_drive_at "$d" - "$tmp" "$lane")"; rc=$?
  if [[ "$rc" -ne 0 ]] && lane_refusal_ok "$out" && [[ "$out" == *"pid-file-contains-nul"* ]] \
     && [[ "$out" != *"reclaiming stale lock"* ]] && [[ -f "$lock/pid" ]] \
     && [[ "$(wc -c <"$lock/pid" | tr -d '[:space:]')" == "$raw_bytes" ]]; then
    pass "lane-lock NUL: the start REFUSES over a NUL-bearing lock, names the cause, reclaims nothing and leaves the file byte-for-byte as found"
  else
    fail "lane-lock-nul-reclaimed: rc=$rc out=[$out] — this is the reclaim-from-a-live-holder path the NUL defect reopened"
  fi
  rm -f -- "$lock/pid"
  lane_lock_remedy_ok "pid-file-contains-nul" "$out" "$lock"

  # ---- (3) THE DETECTOR IS FORK-FREE, WHICH IS THE PROPERTY, NOT AN OPTIMISATION. An earlier cut made
  # the byte-count form the primary and this parser then depended on `wc`/`tr` — and because
  # `supervisor_lock_publish` calls it on EVERY start for its read-back, a box without `wc` could not
  # start a supervisor at all. So: with `wc` unavailable the verdict must be UNCHANGED, in both
  # directions. (The byte-count form's own three-valued failure handling is asserted where that fallback
  # is forced to run, in the differential case below.)
  shadow="$d/shadow"
  mkdir -p "$shadow"
  printf '%s\n' '#!/usr/bin/env bash' 'exit 1' >"$shadow/wc"
  chmod +x "$shadow/wc"
  printf '%s\n' "$dead" >"$d/clean-pid"
  # Re-staged at a path of its own: the remedy step above deliberately CLEARED the lock, so `$lock/pid`
  # no longer exists by here and reading it would assert `pid-file-absent` instead of the NUL verdict.
  printf '%s\000\n' "$dead" >"$d/nul-pid"
  local v_nul_nowc v_clean_nowc
  v_nul_nowc="$(env SUP="$SUPERVISOR" F="$d/nul-pid" PATH="$shadow:$PATH" bash -c 'source "$SUP"; printf "%s" "$(supervisor_lock_pid_read "$F")"' 2>/dev/null || true)"
  v_clean_nowc="$(env SUP="$SUPERVISOR" F="$d/clean-pid" PATH="$shadow:$PATH" bash -c 'source "$SUP"; printf "%s" "$(supervisor_lock_pid_read "$F")"' 2>/dev/null || true)"
  if [[ "$v_clean_nowc" == "pid $dead" ]]; then
    pass "lane-lock NUL (fork-free): a clean pid file still parses as [$v_clean_nowc] with \`wc\` unavailable — the detector is a builtin, so a missing coreutils tool cannot stop this lane starting"
  else
    fail "lane-lock-nul-probe-needs-wc: verdict=[$v_clean_nowc] — the primary detector must not depend on an external command"
  fi
  # ...and the refusing direction is equally unaffected: a broken `wc` must not turn a NUL-bearing file
  # into an accepted pid either.
  if [[ "$v_nul_nowc" == 'unparseable pid-file-contains-nul' ]]; then
    pass "lane-lock NUL (fork-free): and the NUL-bearing file is STILL refused with \`wc\` unavailable [$v_nul_nowc] — being fork-free did not cost the detection"
  else
    fail "lane-lock-nul-nowc-accepted: verdict=[$v_nul_nowc]"
  fi
  # ---- THE PRIMARY'S OWN THIRD VALUE, at the one state it can actually reach: content longer than the
  # bounded scan. It is unreachable through `supervisor_lock_pid_read` (the structural gates reject any
  # such file first), so it is driven at the probe, which is what stops it being dead code a later edit
  # deletes as unused. `could-not-measure`, never `nul-free`.
  local big
  big="$d/big-pid"
  : >"$big"
  local i=0
  while [[ "$i" -lt 130 ]]; do printf '%s' '0123456789012345678901234567890123456789' >>"$big"; i=$((i + 1)); done
  verdict="$(env SUP="$SUPERVISOR" F="$big" bash -c 'source "$SUP"; printf "%s" "$(supervisor_lock_pid_nul_free "$F")"' 2>/dev/null || true)"
  if [[ "$verdict" == 'could-not-measure pid-file-longer-than-the-nul-scan-bound' ]]; then
    pass "lane-lock NUL (third value): content past the bounded scan is [$verdict] — a NUL beyond the bound is UNOBSERVED, and unobserved is never reported as nul-free"
  else
    fail "lane-lock-nul-scan-bound: verdict=[$verdict] — a scan that could not see the whole file must say so"
  fi
  # ---- (4) MUTANT CONTRAST: the NUL gate removed by ONE literal substitution — its `contains-nul`
  # verdict joins the accepting branch. Everything else, including the byte measurement itself, is
  # shipped code, so the contrast isolates the GATE and not the probe.
  mkdir -p "$lock"
  printf '%s\000\n' "$dead" >"$lock/pid"
  ovr="$d/m-nul.sh"; : >"$ovr"
  mrc=0
  if sv_mutant_override "$ovr" supervisor_lock_pid_read \
       '    nul-free) ;;' \
       '    nul-free | contains-nul) ;;'; then
    mout="$(lane_lock_drive_at "$d" "$ovr" "$tmp" "$lane")" || mrc=$?
    if [[ "$mrc" -eq 0 && "$mout" == *"ACQUIRED=$lock"* && "$mout" == *"reclaiming stale lock"* ]]; then
      pass "lane-lock NUL MUTANT: with the NUL gate removed the SAME byte-for-byte file is ACCEPTED and the lock RECLAIMED — the measured pre-job-231 behaviour, so the refusals above are that gate doing the work"
    else
      fail "lane-lock-mutant-nul: rc=$mrc out=[$mout] — the ungated form must be shown to reclaim, or every NUL assert measures nothing"
    fi
  fi

  rm -rf "$lock" "$tmp"
}

t test_lane_lock_nul_bearing_pid_file_refuses




# ---------------------------------------------------------------------------
# THE LOCK NAME BECAME OURS AND WE COULD NOT RECORD OURSELVES IN IT (#3601, roborev job 231 B1).
#
# Three facts were collapsed onto one caller verdict — the write never happened, the rename never
# happened, the lock is not ours — and the refusal built from that collapse ASSERTED all three steps had
# run: it told the operator "our pid was published into it, and reading it back did not return our pid"
# for an ENOSPC that never wrote a byte. On a fleet that hits ENOSPC routinely that points the operator
# at a race that did not occur. Worse, the empty directory we had just created was LEFT BEHIND, so this
# run manufactured the pid-less lock every other branch here refuses to reclaim and wedged its own lane.
#
# THE FAULT IS INJECTED, AND THE CODE UNDER TEST IS SHIPPED. A read-only lock directory is a REAL
# filesystem failure for the publish (no mutation at all); the end-to-end half needs `mkdir` to succeed
# and only the write to fail, which no permission bit can stage, so the WRITE is failed by a
# shipped-derived override while `take`'s cleanup and the refusal — the things being asserted — stay
# shipped code. That is fault injection, not a mutant contrast; the contrast is the non-vacuity case.
# ---------------------------------------------------------------------------
test_lane_lock_publish_failure_is_named_and_leaves_nothing() {
  local d tmp lane lock out rc verdict ovr
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"
  lane="lane3601pubfail$$"
  mkdir -p "$tmp"
  lock="$tmp/cqlite-worker-supervisor-$lane.lock"

  # ---- (1) THE SHIPPED PUBLISH, against a REAL unwritable lock directory: it must say WHICH step failed.
  mkdir -p "$lock"
  chmod 0555 "$lock" 2>/dev/null || true
  if ( : >"$lock/.probe" ) 2>/dev/null; then
    rm -f "$lock/.probe" 2>/dev/null || true
    chmod 0755 "$lock" 2>/dev/null || true
    skip "lane-lock B1: this uid can write a 0555 directory (running as root), so a publish write failure is not stageable here — unmeasurable on this host rather than passing vacuously"
  else
    verdict="$(env SUP="$SUPERVISOR" L="$lock" bash -c 'source "$SUP"; SUPERVISOR_LOCK="$L"; printf "%s" "$(supervisor_lock_publish)"' 2>&1 || true)"
    chmod 0755 "$lock" 2>/dev/null || true
    if [[ "$verdict" == 'write-failed' ]]; then
      pass "lane-lock B1: the publish reports WHICH step failed [$verdict] — and reports it SILENTLY, with no shell redirection error leaking the raw path to stderr"
    else
      fail "lane-lock-publish-cause: verdict=[$verdict] — expected exactly 'write-failed'; a stray shell error here is also the unrendered-path class (#3549 job 201 F1)"
    fi
  fi
  rm -rf "$lock"

  # ---- (2) END TO END with the write failed: the refusal must name a FILESYSTEM failure, must NOT claim
  # the read-back happened, and the directory this run created must be GONE.
  ovr="$d/f-write.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" supervisor_lock_publish \
       '  if ! { printf '"'"'%s\n'"'"' "$$" >"$tmpf"; } 2>/dev/null; then' \
       '  if true; then'; then
    out="$(lane_lock_drive_at "$d" "$ovr" "$tmp" "$lane")"; rc=$?
    # B21 retired the confident "FILESYSTEM failure, not contention" here too: this code cannot know which
    # it was. What B1 is actually about survives unchanged — the refusal is its OWN refusal, it names the
    # step that failed, and it does not claim a read-back that never ran.
    if [[ "$rc" -ne 0 ]] && [[ "$out" == *"could not record itself in it"* ]] \
       && [[ "$out" == *"no byte of it was written"* ]] \
       && [[ "$out" == *"NOT ESTABLISHED"* ]] \
       && [[ "$out" != *"reading it back did not return our pid"* ]] \
       && [[ "$out" != *"ACQUIRED="* ]]; then
      pass "lane-lock B1/B21: a publish that never wrote produces its OWN refusal naming the step that failed, does NOT claim a read-back that never ran, and does not assert a cause it cannot establish"
    else
      fail "lane-lock-publish-misdiagnosed: rc=$rc out=[$out] — the refusal must not assert steps that did not run"
    fi
    if [[ ! -e "$lock" && ! -L "$lock" ]]; then
      pass "lane-lock B1: the directory this run created and never published into was REMOVED AGAIN — the run does not manufacture the pid-less lock it would then have to refuse over (ruling 2)"
    else
      fail "lane-lock-publish-residue: [$(ls -A "$lock" 2>/dev/null | tr '\n' ' ')] — an unpublished lock left behind wedges this lane until an operator clears it"
    fi
    # NON-VACUITY: the SAME drive with the shipped publish acquires normally, so (2) measured the
    # injected failure and not a broken fixture.
    out="$(lane_lock_drive_at "$d" - "$tmp" "$lane")"; rc=$?
    if [[ "$rc" -eq 0 && "$out" == *"ACQUIRED=$lock"* ]]; then
      pass "lane-lock B1 NON-VACUITY: the identical drive with the shipped publish ACQUIRES — so the refusal above is the injected write failure, not the harness"
    else
      fail "lane-lock-publish-nonvacuity: rc=$rc out=[$out]"
    fi
  fi

  # ---- (3) THE NON-RECURSIVE CLEANUP CANNOT EAT A PEER'S RECORD. If a peer reclaimed our pid-less
  # directory and wrote its own pid into it while our publish was failing, `rmdir` refuses and the peer's
  # record survives. Staged by failing the write AND pre-seeding a foreign pid into the directory the
  # moment it exists — which is what a pre-#3601 peer's reclaim looks like from here.
  rm -rf "$lock"
  ovr="$d/f-write-peer.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" supervisor_lock_publish \
       '  if ! { printf '"'"'%s\n'"'"' "$$" >"$tmpf"; } 2>/dev/null; then' \
       '  printf "%s\n" 515151 >"$SUPERVISOR_LOCK/pid" 2>/dev/null || true
  if true; then'; then
    out="$(lane_lock_drive_at "$d" "$ovr" "$tmp" "$lane")" || true
    if [[ -d "$lock" ]] && [[ "$(cat "$lock/pid" 2>/dev/null || true)" == "515151" ]]; then
      pass "lane-lock B1: with a foreign pid already in the directory the cleanup's \`rmdir\` REFUSES and the peer's record survives — the un-create can only ever remove the empty shell we made"
    else
      fail "lane-lock-cleanup-destructive: lock=[$([[ -d "$lock" ]] && echo present || echo GONE)] pid=[$(cat "$lock/pid" 2>/dev/null || true)] — the cleanup deleted a record it does not own"
    fi
  fi

  rm -rf "$lock" "$tmp"
}

t test_lane_lock_publish_failure_is_named_and_leaves_nothing


# ---------------------------------------------------------------------------
# A FAILED RECLAIM RENAME IS NAMED, NOT MISATTRIBUTED (#3601, roborev job 231 B2).
#
# `if mv …; then rm …; fi` with no `else` swallowed the failure, the following claim then failed too, and
# the run printed the LOST-RACE refusal: "a stale lock was cleared and the name was immediately claimed
# by someone else" — which had not happened — followed by "re-run this supervisor", which loops forever.
# Same family as the AC7 addendum this issue already fixed: a message that sends an operator after a
# problem that is not there.
#
# STAGED WITHOUT MUTATION: a lock holding a DEAD pid inside a directory that is readable and searchable
# but NOT WRITABLE. The holder is affirmatively dead, so the run is entitled to the lock and reaches the
# rename; the rename cannot succeed, because clearing a lock needs write permission on its PARENT and
# being able to read a lock does not imply that.
# ---------------------------------------------------------------------------
test_lane_lock_failed_reclaim_rename_is_named() {
  local d ro lane lock out rc dead ovr mout
  d="$(new_case_dir)"
  common_env "$d"
  ro="$d/ro-parent"
  lane="lane3601mvfail$$"
  mkdir -p "$ro"
  lock="$ro/cqlite-worker-supervisor-$lane.lock"

  fixture_bg sleep 0.1
  dead=$FIXTURE_LAST_PID
  fixture_wait "$dead"

  mkdir -p "$lock"
  printf '%s\n' "$dead" >"$lock/pid"
  chmod 0555 "$ro" 2>/dev/null || true
  if ( : >"$ro/.probe" ) 2>/dev/null; then
    rm -f "$ro/.probe" 2>/dev/null || true
    chmod 0755 "$ro" 2>/dev/null || true
    skip "lane-lock B2: this uid can write a 0555 directory (running as root), so a failed reclaim rename is not stageable here — unmeasurable on this host rather than passing vacuously"
    rm -rf "$ro"
    return 0
  fi
  out="$(lane_lock_drive_at "$d" - "$ro" "$lane")"; rc=$?
  chmod 0755 "$ro" 2>/dev/null || true

  # B21: "names a filesystem cause" was itself a claim beyond the evidence. What B2 is about survives — the
  # failure is reported rather than swallowed, and no race is invented.
  if [[ "$rc" -ne 0 ]] && [[ "$out" == *"could not be cleared"* ]] \
     && [[ "$out" == *"nothing was cleared and nothing was claimed"* ]] \
     && [[ "$out" == *"NOT ESTABLISHED"* ]] \
     && [[ "$out" != *"immediately claimed by someone else"* ]]; then
    pass "lane-lock B2/B21: a reclaim whose rename FAILS says so, states plainly that nothing was cleared or claimed, never invents a race, and does not assert a cause it cannot establish"
  else
    fail "lane-lock-mv-misattributed: rc=$rc out=[$out] — the pre-fix path printed the lost-race refusal plus 're-run this supervisor', which loops"
  fi
  # ---- B18: THE IDENTITY IS UNESTABLISHED BY NOW, AND THE PRINTED COMMAND MUST BE SAFE IF THE LOCK IS
  # LIVE (#3601, roborev job 242). The liveness verdict describes the pid file as it was BEFORE the
  # rename was attempted; the path can be replaced in between. The pre-B18 text declared "this lock IS
  # stale and this run is entitled to it" and printed the non-recursive REMOVAL for it — the code
  # destroys nothing, but the operator pasting that line does, and these remedies exist to be pasted.
  if [[ "$out" == *"IDENTITY OF WHAT IS THERE IS NOW UNESTABLISHED"* ]] \
     && [[ "$out" == *"does not declare the lock stale"* ]] \
     && [[ "$out" == *"Do NOT remove anything on the strength of this message"* ]]; then
    pass "lane-lock B18: after a failed rename the refusal reports the identity as UNESTABLISHED, declines to call the lock stale, and tells the operator not to remove anything on its word"
  else
    fail "lane-lock-b18-declares-stale: out=[$out] — a lock whose identity is unverified must not be declared stale or removable"
  fi
  local b18_bare b18_rc=0
  b18_bare="$(printf '%s\n' "$out" | grep -vE "$SV_DIAG_RE" | grep -v '^$' | head -1)"
  if [[ "$b18_bare" == 'ls -ldn -- '* ]] && [[ "$out" != *$'\n''rmdir -- '* ]] \
     && ! printf '%s\n' "$out" | grep -qE '^(rmdir|rm) '; then
    pass "lane-lock B18: the ONLY command printed on this branch is a READ-ONLY inspection and no removal appears anywhere in the refusal — the one command shape that is safe if the lock turns out to be live"
  else
    fail "lane-lock-b18-prints-removal: bare=[$b18_bare] — a paste-ready removal aimed at a possibly-live holder is harm via the operator's hands"
  fi
  # ...and it must RUN verbatim, or it is the "remedy that cannot work" defect in the other direction.
  eval "$b18_bare" >/dev/null 2>&1 || b18_rc=$?
  if [[ "$b18_rc" -eq 0 && -d "$lock" && -f "$lock/pid" ]]; then
    pass "lane-lock B18: that line runs verbatim (rc=0) and removes nothing — the lock and its record are untouched after the operator's inspection"
  else
    fail "lane-lock-b18-remedy-unrunnable: rc=$b18_rc lock=[$([[ -d "$lock" ]] && echo present || echo GONE)]"
  fi

  # It also must not have destroyed the dead holder's lock on the way out, and must not have claimed it.
  if [[ -d "$lock" && -f "$lock/pid" ]] && [[ "$out" != *"ACQUIRED="* ]]; then
    pass "lane-lock B2: the lock is left exactly as found and nothing was claimed"
  else
    fail "lane-lock-mv-residue: lock=[$([[ -d "$lock" ]] && echo present || echo GONE)] out=[$out]"
  fi

  # ---- MUTANT CONTRAST: the pre-fix swallow, restored by one substitution — the `elif` branch becomes
  # unreachable, and the SAME case then produces the lost-race misdiagnosis.
  ovr="$d/m-mv.sh"; : >"$ovr"
  chmod 0555 "$ro" 2>/dev/null || true
  if sv_mutant_override "$ovr" acquire_lock \
       '  elif [[ -e "$SUPERVISOR_LOCK" || -L "$SUPERVISOR_LOCK" ]]; then' \
       '  elif false; then'; then
    mout="$(lane_lock_drive_at "$d" "$ovr" "$ro" "$lane")" || true
    chmod 0755 "$ro" 2>/dev/null || true
    # The expected string is the CURRENT lost-race wording. It changed once already, in the B9 class
    # sweep, which removed the attribution this mutant is demonstrating — so match the surviving text.
    if [[ "$mout" == *"taken again before this run could claim it"* ]]; then
      pass "lane-lock B2 MUTANT: with the rename's failure swallowed the identical case falls through to the lost-race refusal — a claim contest that did not happen — so the assert above is the new branch doing the work"
    else
      fail "lane-lock-mutant-mv: out=[$mout] — the pre-fix swallow must be shown to misattribute"
    fi
  fi
  chmod 0755 "$ro" 2>/dev/null || true

  # ---- B18 (b): A DIFFERENT HOLDER APPEARS between the liveness verdict and the failed rename — the
  # interleaving the finding is about. Fault-injected on the liveness probe so it publishes a foreign pid
  # and THEN answers `dead`, which is exactly the ordering the real race produces; the branch under test
  # (`acquire_lock`'s post-rename reclassification) is shipped code.
  local ovrb mout
  ovrb="$d/f-swap-holder.sh"; : >"$ovrb"
  rm -rf "$lock"
  mkdir -p "$lock"
  printf '%s\n' "$dead" >"$lock/pid"
  chmod 0555 "$ro" 2>/dev/null || true
  if sv_mutant_override "$ovrb" supervisor_lock_holder_liveness \
       '  local pid="$1" msg='"''"' rc=0' \
       '  local pid="$1" msg='"''"' rc=0
  printf "%s\n" 987654 >"$SUPERVISOR_LOCK/pid" 2>/dev/null || true
  printf "%s" dead
  return 0'; then
    mout="$(lane_lock_drive_at "$d" "$ovrb" "$ro" "$lane")" || true
    chmod 0755 "$ro" 2>/dev/null || true
    if [[ "$mout" == *"now records a DIFFERENT holder, pid 987654"* ]] \
       && [[ "$mout" == *"may be ALIVE"* ]] \
       && ! printf '%s\n' "$mout" | grep -qE '^(rmdir|rm) '; then
      pass "lane-lock B18 (different holder): when the record changed under us the refusal names the NEW pid, says it may be alive, and still prints no removal — the case where the pre-B18 text would have aimed a paste-ready delete at a live holder"
    else
      fail "lane-lock-b18-different-holder: out=[$mout]"
    fi
  fi
  chmod 0755 "$ro" 2>/dev/null || true

  # ---- B18 MUTANT: the pre-B18 remedy restored — the removal command comes back, aimed at a lock whose
  # identity this run cannot vouch for.
  local ovrm2
  ovrm2="$d/m-b18.sh"; : >"$ovrm2"
  rm -rf "$lock"
  mkdir -p "$lock"
  printf '%s\n' "$dead" >"$lock/pid"
  chmod 0555 "$ro" 2>/dev/null || true
  if sv_mutant_override "$ovrm2" acquire_lock \
       '      "ls -ldn -- $(supervisor_shell_quote "$SUPERVISOR_LOCK") && ls -lna -- $(supervisor_shell_quote "$SUPERVISOR_LOCK")"' \
       '      "$(supervisor_lock_clear_command)"'; then
    mout="$(lane_lock_drive_at "$d" "$ovrm2" "$ro" "$lane")" || true
    chmod 0755 "$ro" 2>/dev/null || true
    if printf '%s\n' "$mout" | grep -qE '^rmdir -- '; then
      pass "lane-lock B18 MUTANT: with the pre-fix remedy restored the refusal prints a paste-ready REMOVAL for a lock it cannot vouch for — the harm, reproduced, so the read-only assert above is doing the work"
    else
      fail "lane-lock-mutant-b18: out=[$mout] — the pre-fix remedy must be shown to print a removal"
    fi
  fi
  chmod 0755 "$ro" 2>/dev/null || true
  rm -rf "$ro"
}

t test_lane_lock_failed_reclaim_rename_is_named


# ---------------------------------------------------------------------------
# THE PID BOUND IS THE PLATFORM'S PID SPACE, NOT A DIGIT COUNT (#3601, roborev job 231 B3).
#
# The bound was 10 digits and every real pid space is at most 7, so a 10-digit corruption was ACCEPTED,
# cast to `pid_t` by `kill`, reliably reported ESRCH, became an affirmative `dead` and RECLAIMED THE
# LOCK — while a 15-digit corruption of the same kind refused. One defect class, two widths, opposite
# outcomes, and the accepting one is the direction #3601 exists to close.
#
# THE ASSERT IS CONSISTENCY PLUS A REAL CEILING: both widths must refuse, a pid just above the platform
# ceiling must refuse, and a pid the platform CAN issue must still be accepted — because a bound that
# rejected real pids would turn live holders into "malformed" and wedge the lane, which is the same harm
# pointing the other way.
# ---------------------------------------------------------------------------
test_lane_lock_pid_bound_is_the_platform_pid_space() {
  local d verdict raw_max ceiling inclusive
  d="$(new_case_dir)"
  common_env "$d"

  probe_pid() {
    printf '%s\n' "$1" >"$d/pidfile"
    env SUP="$SUPERVISOR" F="$d/pidfile" bash -c 'source "$SUP"; printf "%s" "$(supervisor_lock_pid_read "$F")"' 2>/dev/null || true
  }

  ceiling="$(env SUP="$SUPERVISOR" bash -c 'source "$SUP"; supervisor_pid_space_ceiling' 2>/dev/null || true)"
  # THE PROBE IS THREE-VALUED, so the case must handle BOTH of its answers rather than requiring the one
  # this host happens to give (#3601, roborev job 231 B10). An earlier cut of this case required the
  # ceiling to parse as a bare number, which on a platform that publishes none would have failed a
  # CORRECT implementation — and, worse, it asserted a pid EQUAL to the ceiling was accepted, pinning the
  # off-by-one as correct. That is the #3559 shape (a test pinning wrong behaviour) inside this suite, so
  # it is fixed here together with the code rather than by moving the boundary.
  case "$ceiling" in
    'authoritative '*)
      inclusive="${ceiling#authoritative }"
      pass "lane-lock B3/B10: this platform publishes a pid bound; the probe converts it to the INCLUSIVE maximum [$inclusive]"
      ;;
    'unknown '*)
      pass "lane-lock B3/B10: this platform publishes no readable pid bound, so the probe says [$ceiling] — the gate does not apply and, critically, nothing invents a number in its place"
      ;;
    *)
      fail "lane-lock-pid-ceiling: [$ceiling] — the probe must answer 'authoritative <n>' or 'unknown <cause>' and nothing else"
      unset -f probe_pid
      return 0
      ;;
  esac

  # ---- (1) THE OFF-BY-ONE, MEASURED AGAINST THE PLATFORM'S OWN FILE. `proc(5)` says `pid_max` is the
  # value at which pids WRAP — one greater than the maximum pid — so `pid_max` itself is not issuable and
  # must REFUSE, while `pid_max - 1` is a pid a real process can hold and must be ACCEPTED. Both
  # directions, because a bound that rejects a legal pid would turn live holders into "malformed" and
  # wedge the lane, which is the same harm inverted.
  if [[ "$ceiling" == 'authoritative '* ]]; then
    raw_max="$(cat /proc/sys/kernel/pid_max 2>/dev/null || true)"
    if [[ "$raw_max" =~ ^[0-9]+$ ]] && [[ "$inclusive" == "$((raw_max - 1))" ]]; then
      pass "lane-lock B10: the inclusive maximum [$inclusive] is exactly the platform's exclusive wrap point [$raw_max] minus one, as proc(5) defines it"
    else
      fail "lane-lock-pid-ceiling-conversion: pid_max=[$raw_max] inclusive=[$inclusive] — the exclusive wrap point must be converted, not used as-is"
    fi
    verdict="$(probe_pid "$raw_max")"
    if [[ "$verdict" == 'unparseable pid-above-the-platform-pid-space' ]]; then
      pass "lane-lock B10: a pid EQUAL to the exclusive wrap point REFUSES — the value no process can hold no longer parses as a holder pid"
    else
      fail "lane-lock-pid-ceiling-offbyone: verdict=[$verdict] for pid_max=$raw_max — this is the one malformed value the pre-B10 boundary let through, and the case that used to assert it was ACCEPTED"
    fi
    verdict="$(probe_pid "$inclusive")"
    if [[ "$verdict" == "pid $inclusive" ]]; then
      pass "lane-lock B10 (the other direction): the largest pid a process CAN hold is still accepted — the corrected bound rejects nothing legal, so it cannot wedge the lane"
    else
      fail "lane-lock-pid-ceiling-rejects-legal: verdict=[$verdict] — rejecting an issuable pid is the same harm pointing the other way"
    fi
    verdict="$(probe_pid $((raw_max + 1)))"
    if [[ "$verdict" == 'unparseable pid-above-the-platform-pid-space' ]]; then
      pass "lane-lock B3: a pid past the wrap point refuses"
    else
      fail "lane-lock-pid-bound-above: [$verdict]"
    fi
  fi

  # ---- (2) THE WIDTH INCONSISTENCY THE BOUND EXISTS FOR. A 10-digit and a 15-digit corruption used to get
  # OPPOSITE verdicts — the narrower one accepted, probed, reported dead and RECLAIMED.
  #
  # THE COMPARISON IS BETWEEN ACCEPT/REJECT CLASSES, NOT BETWEEN VERDICT STRINGS (#3601, roborev job 236
  # B13). This case compared the two strings EXACTLY while its own comment claimed it "stays true on a
  # platform that publishes no bound … rather than pinning this host's answer". That claim was FALSE, and
  # falsifiable without leaving this file: where no bound is published the gate does not apply, both pids
  # are ACCEPTED, and the two strings then differ by construction (`pid 9999999999` vs
  # `pid 999999999999999`) — so the case RED on macOS/bash 3.2, a platform this file explicitly supports,
  # and the first person to run it there would have spent an hour deciding whether they had broken
  # something. It is the alibi family again — a comment describing a tolerance the code does not have —
  # and it is fixed here rather than downgraded, because a suite that reds on a supported platform is not
  # a nit. The property that actually matters is that BOTH WIDTHS LAND IN THE SAME CLASS, and the expected
  # class is then asserted EXPLICITLY per ceiling branch, which is stronger than either version.
  pid_verdict_class() {
    case "$1" in
      'pid '*)         printf '%s' accepted ;;
      'unparseable '*) printf '%s' refused ;;
      *)               printf '%s' unrecognised ;;
    esac
  }
  local ten fifteen ten_class fifteen_class want_class
  ten="$(probe_pid 9999999999)"
  fifteen="$(probe_pid 999999999999999)"
  ten_class="$(pid_verdict_class "$ten")"
  fifteen_class="$(pid_verdict_class "$fifteen")"
  if [[ "$ten_class" == "$fifteen_class" && "$ten_class" != unrecognised ]]; then
    pass "lane-lock B3: a 10-digit and a 15-digit corruption land in the SAME class [$ten_class] — the width-dependent inconsistency is gone, and the accepting half of it (which reclaimed) with it"
  else
    fail "lane-lock-pid-bound-inconsistent: ten=[$ten]($ten_class) fifteen=[$fifteen]($fifteen_class) — a corruption that reclaims at one width and refuses at another is the hole this closes"
  fi
  # ...and WHICH class it is, is decided by the ceiling branch, stated rather than left to the host.
  case "$ceiling" in
    'authoritative '*) want_class=refused ;;
    *)                 want_class=accepted ;;
  esac
  if [[ "$ten_class" == "$want_class" ]]; then
    pass "lane-lock B13: with the ceiling [${ceiling%% *}] the expected class for an out-of-range corruption is [$want_class], and that is what both widths got — the case asserts the platform's own consequence instead of this host's string"
  else
    fail "lane-lock-pid-bound-class: ceiling=[$ceiling] want=[$want_class] got=[$ten_class] — where a bound IS published an out-of-range pid must be refused; where none is, the gate does not apply and the pid is accepted"
  fi

  # ---- (2b) THE NO-BOUND BRANCH IS EXERCISED ON THIS HOST TOO, not left to a platform nobody runs the
  # suite on. `supervisor_pid_space_ceiling` is made to report `unknown` by a shipped-derived override, so
  # the macOS/no-`/proc` path is measured here: both widths must be ACCEPTED (the gate cannot apply) and
  # the parser must still reject on its own structural gates.
  local ovru u_ten u_wide
  ovru="$d/f-unknown-ceiling.sh"; : >"$ovru"
  if sv_mutant_override "$ovru" supervisor_pid_space_ceiling \
       "  printf 'authoritative %s' \"\$((b - 1))\"" \
       "  printf '%s' 'unknown forced-for-test'"; then
    probe_pid_ovr() {
      printf '%s\n' "$1" >"$d/pidfile"
      env SUP="$SUPERVISOR" OVR="$ovru" F="$d/pidfile" bash -c 'source "$SUP"; source "$OVR"; printf "%s" "$(supervisor_lock_pid_read "$F")"' 2>/dev/null || true
    }
    u_ten="$(pid_verdict_class "$(probe_pid_ovr 9999999999)")"
    u_wide="$(pid_verdict_class "$(probe_pid_ovr 999999999999999)")"
    if [[ "$u_ten" == accepted && "$u_wide" == accepted ]]; then
      pass "lane-lock B13: forced onto the no-published-bound path (the macOS shape) both widths are ACCEPTED and consistent — so the case above is correct on a platform without /proc, which is what its comment used to claim without being true"
    else
      fail "lane-lock-b13-unknown-branch: ten=[$u_ten] wide=[$u_wide] — with no bound published the gate cannot apply, so both must be accepted"
    fi
    # ...and the structural gates still bite on that path, so "the gate does not apply" is not "anything goes".
    if [[ "$(pid_verdict_class "$(probe_pid_ovr 9999999999999999999999)")" == refused ]]; then
      pass "lane-lock B13: on the same no-bound path a pid past the arithmetic guard is still REFUSED — an unapplied platform gate is not licence to accept anything"
    else
      fail "lane-lock-b13-unknown-unbounded: a 22-digit value must still be refused with no platform bound published"
    fi
    unset -f probe_pid_ovr
  fi

  # ---- (3) NO INVENTED CONSTANT SURVIVES. The rejected design substituted Linux's PID_MAX_LIMIT
  # wherever `/proc` was absent, which on macOS accepted values 42x the real platform limit and was a
  # guess about an unmeasured platform presented as a bound. Structural, because no behavioural case on a
  # Linux host can observe the non-Linux path.
  local invented
  invented="$(grep -nE 'SUPERVISOR_PID_MAX_FALLBACK|4194304' "$SUPERVISOR" | grep -vE '^[0-9]+:[[:space:]]*#' || true)"
  if [[ -z "$invented" ]]; then
    pass "lane-lock B10 STRUCTURAL: no cross-platform pid ceiling constant exists in the supervisor's code — an unreadable bound yields \`unknown\`, never a number nobody measured (CLAUDE.md #28)"
  else
    fail "lane-lock-pid-ceiling-invented: [$invented] — a guessed bound for an unmeasured platform is a no-heuristics violation, not a conservative default"
  fi

  # ---- (4) MUTANT CONTRAST for the conversion: use the exclusive value as if it were inclusive, which
  # is the pre-B10 spelling, and the wrap-point pid is accepted again.
  local ovr mverdict
  ovr="$d/m-ceiling.sh"; : >"$ovr"
  if [[ "$ceiling" == 'authoritative '* ]] && sv_mutant_override "$ovr" supervisor_pid_space_ceiling \
       "  printf 'authoritative %s' \"\$((b - 1))\"" \
       "  printf 'authoritative %s' \"\$b\""; then
    printf '%s\n' "$raw_max" >"$d/pidfile"
    mverdict="$(env SUP="$SUPERVISOR" OVR="$ovr" F="$d/pidfile" bash -c 'source "$SUP"; source "$OVR"; printf "%s" "$(supervisor_lock_pid_read "$F")"' 2>/dev/null || true)"
    if [[ "$mverdict" == "pid $raw_max" ]]; then
      pass "lane-lock B10 MUTANT: with the exclusive wrap point used as an inclusive maximum the unissuable value $raw_max is ACCEPTED again — so the conversion is what refuses it, and the assert above is not vacuous"
    else
      fail "lane-lock-mutant-ceiling: verdict=[$mverdict] — the pre-B10 spelling must be shown to accept the wrap point"
    fi
  fi

  unset -f probe_pid pid_verdict_class
}

t test_lane_lock_pid_bound_is_the_platform_pid_space


# ---------------------------------------------------------------------------
# THE NUL PROBE'S FALLBACK IS A SECOND IMPLEMENTATION, SO IT IS DIFFERENTIALLY TESTED (#3601).
#
# The primary detector is `read -d ''` — a builtin, so no fork and no `PATH` dependency. The `wc`/`tr`
# byte-count form survives as the FALLBACK for a shell whose `read` rejects `-d`/`-n`, chosen by
# capability (a `read` that refuses the options exits >1, distinguishable from both "found a NUL" and
# "reached EOF"). CLAUDE.md's ruling is that a second implementation's correctness is only knowable by
# differential testing against the original, so both are driven over the SAME inputs here rather than
# the fallback being assumed unreachable and left uncovered.
#
# WHY THE PRIMARY IS NOT THE BYTE-COUNT FORM, MEASURED: `supervisor_lock_publish` calls this parser on
# EVERY start for its read-back, so with `wc` unavailable a supervisor could not start AT ALL — on a
# FRESH, uncontended lock — and refused forever with a diagnostic about a read-back that never ran.
# Turning a missing coreutils tool into "this lane can never start" is the permanent-refusal harm this
# change exists to remove, so that case is asserted below too.
# ---------------------------------------------------------------------------
test_lane_lock_nul_probe_primary_and_fallback_agree() {
  local d shadow nulf cleanf ovr pv fv
  d="$(new_case_dir)"
  common_env "$d"
  nulf="$d/nul-pid"
  cleanf="$d/clean-pid"
  printf '4242\000\n' >"$nulf"
  printf '4242\n' >"$cleanf"
  shadow="$d/shadow"
  mkdir -p "$shadow"
  printf '%s\n' '#!/usr/bin/env bash' 'exit 1' >"$shadow/wc"
  chmod +x "$shadow/wc"

  # ---- (1) THE PRIMARY IS FORK-FREE: with BOTH `wc` and `tr` unavailable it still answers both ways.
  # This is the PROBE called directly, deliberately: `tr` is used all over the supervisor's own startup
  # (lane-identity sanitisation, the proc probes), so a whole-run drive with `tr` shadowed would fail for
  # reasons that have nothing to do with this probe and would measure nothing. The whole-run case below
  # shadows `wc` only, which is the tool the rejected byte-count-as-primary form actually wedged on.
  local shadow2="$d/shadow2"
  mkdir -p "$shadow2"
  printf '%s\n' '#!/usr/bin/env bash' 'exit 1' >"$shadow2/wc"
  printf '%s\n' '#!/usr/bin/env bash' 'exit 1' >"$shadow2/tr"
  chmod +x "$shadow2/wc" "$shadow2/tr"
  pv="$(env SUP="$SUPERVISOR" F="$nulf" PATH="$shadow2:$PATH" bash -c 'source "$SUP"; printf "%s" "$(supervisor_lock_pid_nul_free "$F")"' 2>/dev/null || true)"
  fv="$(env SUP="$SUPERVISOR" F="$cleanf" PATH="$shadow2:$PATH" bash -c 'source "$SUP"; printf "%s" "$(supervisor_lock_pid_nul_free "$F")"' 2>/dev/null || true)"
  if [[ "$pv" == 'contains-nul' && "$fv" == 'nul-free' ]]; then
    pass "lane-lock NUL probe: the primary detector answers correctly with \`wc\` AND \`tr\` both unavailable — it is a builtin, so no verdict here depends on PATH"
  else
    fail "lane-lock-nul-primary-forks: nul=[$pv] clean=[$fv] — the primary must not depend on an external command"
  fi

  # ---- (2) A FRESH START ON SUCH A BOX MUST WORK. This is the wedge the byte-count-as-primary form
  # measurably caused: the parser runs on every start, for the publish's read-back.
  local tmp lane lock out rc
  tmp="$d/tmp"; lane="lane3601nofork$$"
  mkdir -p "$tmp"
  lock="$tmp/cqlite-worker-supervisor-$lane.lock"
  out="$(lane_lock_drive_at "$d" - "$tmp" "$lane" PATH="$shadow:$PATH")"; rc=$?
  if [[ "$rc" -eq 0 && "$out" == *"ACQUIRED=$lock"* ]]; then
    pass "lane-lock NUL probe: a FRESH uncontended start still succeeds on a box with no \`wc\` — a missing coreutils tool cannot make this lane unable to start (ruling 2), which the byte-count-as-primary form measurably did"
  else
    fail "lane-lock-nul-probe-wedges-start: rc=$rc out=[$out] — this is the permanent-refusal harm the primary detector avoids"
  fi

  # ---- (3) THE FALLBACK, FORCED. `read` is made to look unsupported by a shipped-derived override that
  # returns >1 from the primary read — the same capability signal a shell without `-d` produces — so the
  # fallback runs for real rather than being assumed correct.
  ovr="$d/m-nulfallback.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" supervisor_lock_pid_nul_free \
       '  { opened=1; IFS= read -r -d '"'"''"'"' -n "$SUPERVISOR_PID_NUL_SCAN" probe; } 2>/dev/null <"$f" || rrc=$?' \
       '  opened=1; rrc=2'; then
    pv="$(env SUP="$SUPERVISOR" OVR="$ovr" F="$nulf" bash -c 'source "$SUP"; source "$OVR"; printf "%s" "$(supervisor_lock_pid_nul_free "$F")"' 2>/dev/null || true)"
    fv="$(env SUP="$SUPERVISOR" OVR="$ovr" F="$cleanf" bash -c 'source "$SUP"; source "$OVR"; printf "%s" "$(supervisor_lock_pid_nul_free "$F")"' 2>/dev/null || true)"
    if [[ "$pv" == 'contains-nul' && "$fv" == 'nul-free' ]]; then
      pass "lane-lock NUL probe DIFFERENTIAL: with the builtin reporting itself unsupported, the byte-count fallback returns the SAME two verdicts over the SAME two files — the second implementation agrees with the first where it can be compared"
    else
      fail "lane-lock-nul-fallback-diverges: nul=[$pv] clean=[$fv] — the fallback must agree with the primary, or one of them is wrong"
    fi
    # ...and the fallback's own third value: with `wc` gone it must refuse, never report `nul-free`.
    pv="$(env SUP="$SUPERVISOR" OVR="$ovr" F="$cleanf" PATH="$shadow:$PATH" bash -c 'source "$SUP"; source "$OVR"; printf "%s" "$(supervisor_lock_pid_nul_free "$F")"' 2>/dev/null || true)"
    if [[ "$pv" == 'could-not-measure'* ]]; then
      pass "lane-lock NUL probe DIFFERENTIAL: the fallback's failed measurement is [$pv] — three-valued like the primary, so an unmeasurable read is never 'nul-free' in either implementation"
    else
      fail "lane-lock-nul-fallback-permissive: [$pv]"
    fi
  fi

  rm -rf "$tmp"
}

t test_lane_lock_nul_probe_primary_and_fallback_agree




# ---------------------------------------------------------------------------
# THE PUBLISH DECLINES A PRE-EXISTING HOLDER RECORD RATHER THAN OVERWRITING IT (#3601, job 231 B11).
#
# THE DEFECT: publication was not tied to the directory instance this process created. A peer can rename
# our pid-less directory away, create and publish its OWN lock at the same name, and our forced `mv` then
# overwrote THAT peer's ownership record — destroying the evidence of who holds the lane, which is
# strictly worse than failing to start.
#
# WHAT IS FIXED HERE AND WHAT IS NOT: the OVERWRITE is refused, so we decline instead of corrupting. The
# RACE is not closed — test-then-move is not an atomic create-exclusive — and closing it needs
# serialisation across the whole claim-and-publication operation, which is #3683 together with the
# reclaim ABA and the release read-then-remove. The asserts below are about the non-destruction only, and
# the site comment says the same, because a comment claiming more than the code does is the B9 family.
# ---------------------------------------------------------------------------
test_lane_lock_publish_declines_instead_of_clobbering() {
  local d tmp lane lock verdict out rc ovr peer_pid
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"
  lane="lane3601decl$$"
  mkdir -p "$tmp"
  lock="$tmp/cqlite-worker-supervisor-$lane.lock"
  peer_pid=515151

  # ---- (1) THE SHIPPED PUBLISH against a directory that already carries a record.
  mkdir -p "$lock"
  printf '%s\n' "$peer_pid" >"$lock/pid"
  verdict="$(env SUP="$SUPERVISOR" L="$lock" bash -c 'source "$SUP"; SUPERVISOR_LOCK="$L"; printf "%s" "$(supervisor_lock_publish)"' 2>&1 || true)"
  if [[ "$verdict" == "declined pid $peer_pid" ]] && [[ "$(cat "$lock/pid" 2>/dev/null || true)" == "$peer_pid" ]]; then
    pass "lane-lock B11: the publish DECLINES when a holder record already exists [$verdict] and the peer's pid is untouched — it reports the record it found rather than replacing it"
  else
    fail "lane-lock-publish-clobbers: verdict=[$verdict] pid=[$(cat "$lock/pid" 2>/dev/null || true)] — a peer's ownership record must never be overwritten"
  fi
  # ...and the staging file it wrote on the way is cleaned up, so declining leaves no debris either.
  if [[ -z "$(ls -A "$lock" 2>/dev/null | grep 'pid\.tmp\.' || true)" ]]; then
    pass "lane-lock B11: the declined publish removed its own staging file — declining leaves nothing behind"
  else
    fail "lane-lock-publish-decline-residue: [$(ls -A "$lock" | tr '\n' ' ')]"
  fi

  # ---- (2) THE LEGITIMATE CASE IS UNCHANGED: a directory we just created has no `pid`, so the branch is
  # never taken and the publish succeeds. Without this the assert above is satisfied by code that never
  # publishes at all.
  rm -rf "$lock"
  mkdir -p "$lock"
  verdict="$(env SUP="$SUPERVISOR" L="$lock" bash -c 'source "$SUP"; SUPERVISOR_LOCK="$L"; printf "%s|%s" "$(supervisor_lock_publish)" "$(cat "$L/pid" 2>/dev/null)"' 2>&1 || true)"
  if [[ "$verdict" == "ok|"* ]]; then
    pass "lane-lock B11 NON-VACUITY: an empty lock directory still publishes normally [$verdict] — the non-overwrite costs the legitimate path nothing"
  else
    fail "lane-lock-publish-broken: verdict=[$verdict]"
  fi

  # ---- (3) END TO END: a peer publishes into the directory we created. The refusal must say we wrote
  # NOTHING, must say the path is untouched, and the peer's record must survive.
  rm -rf "$lock"
  ovr="$d/f-peerpub.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" supervisor_lock_publish \
       '  local tmpf="$SUPERVISOR_LOCK/pid.tmp.$$" state='"''" \
       '  local tmpf="$SUPERVISOR_LOCK/pid.tmp.$$" state='"''"'
  printf "%s\n" 626262 >"$SUPERVISOR_LOCK/pid" 2>/dev/null || true'; then
    out="$(lane_lock_drive_at "$d" "$ovr" "$tmp" "$lane")"; rc=$?
    # THE ASSERTED PROPERTY IS PRESERVATION OF THE HOLDER RECORD, not an absence of modification (#3601,
    # roborev job 238 B15). The old form required the refusal to say "wrote NOTHING" and "NOTHING at that
    # path has been modified" — and so it PINNED two claims that were false in detail: the publish writes
    # its staging file before it tests for an existing `pid`, and the un-create may remove an ownership
    # marker from that same directory. A test that requires a false claim keeps the claim alive, which is
    # how the alibi family survives being fixed; so this now asserts what the code actually guarantees.
    if [[ "$rc" -ne 0 ]] && [[ "$out" == *"declined to publish over it"* ]] \
       && [[ "$out" == *"is INTACT — it was neither overwritten nor replaced"* ]] \
       && [[ "$out" == *"[pid 626262]"* ]] && [[ "$out" != *"ACQUIRED"* ]]; then
      pass "lane-lock B11/B15: with a peer's record published into the directory we created, the run refuses, names the record it found, and claims exactly the preservation it provides — not an absence of modification it does not"
    else
      fail "lane-lock-b11-endtoend: rc=$rc out=[$out]"
    fi
    # ...and the claim is not merely WORDED correctly, it is TRUE: the record's bytes are unchanged.
    if [[ "$(cat "$lock/pid" 2>/dev/null || true)" == "626262" ]]; then
      pass "lane-lock B15: the holder record is byte-for-byte as the peer left it — the refusal's one load-bearing claim is measured, not asserted"
    else
      fail "lane-lock-b15-record-not-preserved: pid=[$(cat "$lock/pid" 2>/dev/null || true)]"
    fi
    # NEGATIVE CONTROL for B19: the refusal must not name a CAUSE it did not observe. Finding a `pid`
    # record proves publication must be declined; it does not identify how the record got there, and
    # directory-instance identity is unverifiable on this path (#3683). The pre-B19 text asserted a
    # specific race ("a peer renamed ours aside and published its own between our two steps").
    if [[ "$out" == *"APPEARED in the lock"* ]] \
       && [[ "$out" == *"HOW that record got there is NOT established"* ]] \
       && [[ "$out" != *"renamed ours aside"* ]]; then
      pass "lane-lock B19: the refusal reports that a holder record APPEARED and was preserved, and says outright that how it got there is not established — the invented race is gone"
    else
      fail "lane-lock-b19-invents-a-race: out=[$out] — naming one of several possible causes as the cause is a claim beyond the evidence"
    fi
    # NEGATIVE CONTROL for the wording: the refusal must NOT claim the path is untouched, because it is
    # not. This is the assert that stops the false claim being reintroduced by a well-meaning edit.
    if [[ "$out" != *"NOTHING at that path has been modified"* ]]; then
      pass "lane-lock B15: the refusal does NOT claim the path is untouched — it says plainly that it wrote and removed its own scratch entries, because it did"
    else
      fail "lane-lock-b15-overclaims: the refusal claims nothing at that path was modified, which is false: a staging file and an ownership marker are written there"
    fi
    if [[ "$(cat "$lock/pid" 2>/dev/null || true)" == "626262" ]]; then
      pass "lane-lock B11: and the peer's record survives the whole refusal path — nothing on the way out removed or rewrote it"
    else
      fail "lane-lock-b11-peer-record-lost: pid=[$(cat "$lock/pid" 2>/dev/null || true)]"
    fi
  fi

  # ---- (4) MUTANT CONTRAST: the pre-B11 spelling — the decline branch removed, so the forced rename
  # overwrites whatever is there. One literal substitution.
  rm -rf "$lock"
  mkdir -p "$lock"
  printf '%s\n' "$peer_pid" >"$lock/pid"
  ovr="$d/m-clobber.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" supervisor_lock_publish \
       '  if [[ -e "$SUPERVISOR_LOCK/pid" || -L "$SUPERVISOR_LOCK/pid" ]]; then' \
       '  if false; then'; then
    verdict="$(env SUP="$SUPERVISOR" OVR="$ovr" L="$lock" bash -c 'source "$SUP"; source "$OVR"; SUPERVISOR_LOCK="$L"; printf "%s" "$(supervisor_lock_publish)"' 2>&1 || true)"
    if [[ "$verdict" == 'ok' ]] && [[ "$(cat "$lock/pid" 2>/dev/null || true)" != "$peer_pid" ]]; then
      pass "lane-lock B11 MUTANT: with the decline removed the publish reports [$verdict] and the peer's record $peer_pid is GONE, replaced by ours — the measured pre-B11 behaviour, so the assert above is the non-overwrite doing the work"
    else
      fail "lane-lock-mutant-clobber: verdict=[$verdict] pid=[$(cat "$lock/pid" 2>/dev/null || true)] — the pre-fix form must be shown to destroy the peer's record"
    fi
  fi

  # ---- (5) THE RESIDUAL IS STATED AT THE SITE, IN THE NARROWED-NOT-CLOSED FORM, and does not claim
  # atomicity. Structural, because no behavioural case can observe a comment — and the comment is the
  # artifact a reviewer is asked to trust about a race that is still open.
  local body
  body="$(sed -n '/^supervisor_lock_publish()/,/^}/p' "$SUPERVISOR")"
  if [[ "$body" == *'NARROWED, NOT CLOSED'* ]] && [[ "$body" == *'#3683'* ]] \
     && [[ "$body" == *'NOT bound to the directory instance'* || "$body" == *'NOT bound to the directory'* ]] \
     && [[ "$body" == *'not equivalent to an atomic'* || "$body" == *'NOT equivalent to an atomic'* ]]; then
    pass "lane-lock B11: the site states that publication is not bound to the created directory instance, that test-then-move is not atomic, and that closing it is #3683 — the comment claims exactly what the code does"
  else
    fail "lane-lock-b11-comment-overclaims: the publish's comment must name the residual and the follow-up; a comment claiming more than the code does is the B9 family"
  fi

  rm -rf "$lock" "$tmp"
}

t test_lane_lock_publish_declines_instead_of_clobbering




# ---------------------------------------------------------------------------
# NO DIAGNOSTIC NAMES A STEP THAT DID NOT RUN (#3601, roborev job 231 B9 — and the CLASS, not just the
# reported instance).
#
# THE REPORTED INSTANCE: `supervisor_lock_take` ignores its two cleanup failures — deliberately, because a
# peer populating the directory is a legitimate reason for `rmdir` to refuse — and the refusal then stated
# unconditionally that the directory "has been REMOVED AGAIN". With a peer's record inside, or a
# filesystem that refuses the removal, the lock REMAINED while the operator was told there was nothing to
# clear.
#
# THE CLASS: four instances of it landed in this one diff — a refusal claiming a read-back that never ran
# (B1), a mutant comment claiming an isolation it did not have (B4), a code comment vouching for the race
# it sits on (fixed by hand), and this one. The lead's ruling is that such an artifact is worse than no
# artifact, because it is what stops the next reader looking. So the cases below assert the property for
# EVERY branch of the cleanup, including the two the reported finding did not name.
# ---------------------------------------------------------------------------
test_lane_lock_cleanup_outcome_is_verified_not_claimed() {
  local d tmp lane lock out rc ovr
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"
  lane="lane3601cln$$"
  mkdir -p "$tmp"
  lock="$tmp/cqlite-worker-supervisor-$lane.lock"

  # The publish's write is failed by a shipped-derived override in every case below; `take`'s cleanup and
  # the refusal — the code being asserted — stay shipped.
  ovr="$d/f-write.sh"; : >"$ovr"
  if ! sv_mutant_override "$ovr" supervisor_lock_publish \
       '  if ! { printf '"'"'%s\n'"'"' "$$" >"$tmpf"; } 2>/dev/null; then' \
       '  if true; then'; then
    return 0
  fi

  # ---- (a) THE CLEANUP SUCCEEDS: only here may the refusal say the directory is gone, and it says the
  # absence was VERIFIED rather than assumed.
  rm -rf "$lock"
  out="$(lane_lock_drive_at "$d" "$ovr" "$tmp" "$lane")"; rc=$?
  if [[ "$rc" -ne 0 ]] && [[ "$out" == *"absence was VERIFIED after the removal"* ]] \
     && [[ "$out" == *"Nothing needs clearing by hand"* ]] && [[ ! -e "$lock" ]]; then
    pass "lane-lock B9 (cleanup succeeded): the refusal claims the removal ONLY with the absence verified afterwards, and the lock really is gone"
  else
    fail "lane-lock-b9-verified: rc=$rc lock=[$([[ -e "$lock" ]] && echo present || echo gone)] out=[$out]"
  fi

  # ---- (b) A PEER'S RECORD BLOCKS THE `rmdir`: the lock REMAINS, and the refusal must say so, name the
  # foreign holder, and offer NO removal command — the record is not ours to delete.
  local ovrb
  ovrb="$d/f-write-peer.sh"; : >"$ovrb"
  rm -rf "$lock"
  if sv_mutant_override "$ovrb" supervisor_lock_publish \
       '  local tmpf="$SUPERVISOR_LOCK/pid.tmp.$$" state='"''" \
       '  local tmpf="$SUPERVISOR_LOCK/pid.tmp.$$" state='"''"'
  printf "%s\n" 717171 >"$SUPERVISOR_LOCK/pid" 2>/dev/null || true
  if true; then printf "%s" write-failed; return 1; fi'; then
    out="$(lane_lock_drive_at "$d" "$ovrb" "$tmp" "$lane")"; rc=$?
    local bare
    bare="$(printf '%s\n' "$out" | grep -vE "$SV_DIAG_RE" | grep -v '^$' | head -1)"
    if [[ "$rc" -ne 0 ]] && [[ "$out" == *"it REMAINS"* ]] && [[ "$out" == *"NOT ours ([pid 717171])"* ]] \
       && [[ "$out" != *"absence was VERIFIED"* ]] && [[ -z "$bare" ]]; then
      pass "lane-lock B9 (peer record blocks the cleanup): the refusal says the lock REMAINS, names the foreign holder, and prints NO removal command — a record we do not own is not ours to offer for deletion"
    else
      fail "lane-lock-b9-foreign: rc=$rc bare=[$bare] out=[$out]"
    fi
    if [[ "$(cat "$lock/pid" 2>/dev/null || true)" == "717171" ]]; then
      pass "lane-lock B9: and the peer's record survives — the cleanup's \`rmdir\` is non-recursive precisely so it cannot delete it"
    else
      fail "lane-lock-b9-foreign-destroyed: pid=[$(cat "$lock/pid" 2>/dev/null || true)]"
    fi
  fi

  # ---- (c) A RESIDUAL THAT IS NOT A HOLDER: the lock remains with unusable content, so the refusal says
  # it remains AND offers the non-recursive clear command, which the (b) case must not.
  local ovrc
  ovrc="$d/f-write-junk.sh"; : >"$ovrc"
  rm -rf "$lock"
  if sv_mutant_override "$ovrc" supervisor_lock_publish \
       '  local tmpf="$SUPERVISOR_LOCK/pid.tmp.$$" state='"''" \
       '  local tmpf="$SUPERVISOR_LOCK/pid.tmp.$$" state='"''"'
  printf "%s\n" not-a-pid >"$SUPERVISOR_LOCK/pid" 2>/dev/null || true
  if true; then printf "%s" write-failed; return 1; fi'; then
    out="$(lane_lock_drive_at "$d" "$ovrc" "$tmp" "$lane")"; rc=$?
    local barec
    barec="$(printf '%s\n' "$out" | grep -vE "$SV_DIAG_RE" | grep -v '^$' | head -1)"
    if [[ "$rc" -ne 0 ]] && [[ "$out" == *"it REMAINS"* ]] && [[ "$out" == *"unparseable pid-not-all-decimal-digits"* ]] \
       && [[ "$barec" == 'rmdir -- '* ]]; then
      pass "lane-lock B9 (residual, not a holder): the refusal says the lock REMAINS, names what is in it, and DOES offer the non-recursive clear — the three cleanup outcomes get three different remedies because the right action differs"
    else
      fail "lane-lock-b9-residual: rc=$rc bare=[$barec] out=[$out]"
    fi
  fi

  # ---- (d) MUTANT CONTRAST: the pre-B9 spelling — the cleanup verdict is asserted unconditionally, so
  # the operator is told the directory was removed while it demonstrably remains. Applied to the shipped
  # `take` by ONE substitution, driven over case (b)'s staging.
  local ovrd mout
  ovrd="$d/m-claim.sh"; : >"$ovrd"
  rm -rf "$lock"
  if sv_mutant_override "$ovrd" supervisor_lock_take \
       '  if [[ ! -e "$SUPERVISOR_LOCK" && ! -L "$SUPERVISOR_LOCK" ]]; then' \
       '  if true; then'; then
    cat "$ovrb" >>"$ovrd"
    mout="$(lane_lock_drive_at "$d" "$ovrd" "$tmp" "$lane")" || true
    if [[ "$mout" == *"absence was VERIFIED after the removal"* ]] && [[ -e "$lock" ]]; then
      pass "lane-lock B9 MUTANT: with the cleanup verdict asserted rather than observed, the refusal tells the operator the directory was removed WHILE IT IS STILL THERE — the exact false artifact, so the asserts above are the verification doing the work"
    else
      fail "lane-lock-mutant-b9: lock=[$([[ -e "$lock" ]] && echo present || echo gone)] out=[$mout] — the unconditional claim must be shown to lie"
    fi
  fi

  rm -rf "$lock" "$tmp"
}

t test_lane_lock_cleanup_outcome_is_verified_not_claimed




# ---------------------------------------------------------------------------
# THE OTHER TWO SITES THE B9 CLASS SWEEP FOUND (#3601, roborev job 231 B9).
#
# Neither was in the finding. Both are the same shape: an artifact whose text is not bound to what the run
# actually observed.
#
#   (1) THE LOST-RACE REFUSAL attributed the clearing and the new ownership to specific actors — "a stale
#       lock was cleared and the name was immediately claimed by someone else" — when all the run observed
#       is that its own claim found the name taken. It does not know its own rename is what cleared the
#       lock (a racer may have), and it never read the new holder.
#   (2) THE RELEASE'S REMOVAL made NO claim, silently: `rm -rf … || true`. The silence is the same problem
#       one step earlier — a lock left behind by a supervisor that exited cleanly is unattributable, and
#       the next start has to deal with it. It is self-healing, so it reports rather than escalates.
# ---------------------------------------------------------------------------
test_lane_lock_sweep_no_unobserved_attribution() {
  local d tmp lane lock out rc dead ovr
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"
  lane="lane3601sweep$$"
  mkdir -p "$tmp"
  lock="$tmp/cqlite-worker-supervisor-$lane.lock"

  fixture_bg sleep 0.1
  dead=$FIXTURE_LAST_PID
  fixture_wait "$dead"

  # ---- (1) THE LOST-RACE PATH, reached by failing every claim attempt while a genuinely dead holder's
  # lock is present: the reclaim runs, the claim that follows finds the name unavailable, and this is the
  # refusal. Fault-injected on `take`'s return, so the refusal text under test is shipped.
  mkdir -p "$lock"
  printf '%s\n' "$dead" >"$lock/pid"
  ovr="$d/f-take.sh"; : >"$ovr"
  # THE NAME MUST BE OCCUPIED FOR THIS TO BE A LOST RACE (#3601, roborev job 244 B20). This override used
  # to just `return 1`, which after the reclaim left NOTHING at the name — and B20 correctly reports that
  # as a path failure, not a race. So the fault now also holds the name, which is what a lost race means.
  if sv_mutant_override "$ovr" supervisor_lock_take \
       '  mkdir -- "$SUPERVISOR_LOCK" 2>/dev/null || return 1' \
       '  mkdir -p -- "$SUPERVISOR_LOCK" 2>/dev/null || true
  return 1'; then
    out="$(lane_lock_drive_at "$d" "$ovr" "$tmp" "$lane")"; rc=$?
    if [[ "$rc" -ne 0 ]] && [[ "$out" == *"taken again before this run could claim it"* ]] \
       && [[ "$out" == *"both unestablished"* ]] \
       && [[ "$out" != *"immediately claimed by someone else"* ]] \
       && [[ "$out" != *"Only one racer can win"* ]]; then
      pass "lane-lock B9 sweep (lost race): the refusal reports what the run OBSERVED — the name was taken again — and states that who cleared the lock and who holds it now are both unestablished, instead of attributing either"
    else
      fail "lane-lock-sweep-lostrace: rc=$rc out=[$out] — the pre-sweep wording attributed a clearing and an ownership this run never established"
    fi
  fi
  rm -rf "$lock"

  # ---- (2) THE RELEASE PATH, with its own removal made to FAIL for real: the lock's parent is made
  # unwritable after the lock is taken, so the EXIT trap cannot remove it. Derived from the shipped drive
  # body by one insertion, so the startup path is the ordinary one.
  local rop body_ro
  rop="$d/ro-parent"
  mkdir -p "$rop"
  body_ro="${SV_DRIVE_BODY/'exit 0'/'chmod 0555 "$2" 2>/dev/null || true; exit 0'}"
  if [[ "$body_ro" == "$SV_DRIVE_BODY" ]]; then
    fail "lane-lock-sweep-release-premise: the read-only drive body was not derived from the shipped one"
  elif ( : >"$rop/.probe" ) 2>/dev/null && { rm -f "$rop/.probe"; chmod 0555 "$rop" 2>/dev/null; ( : >"$rop/.probe2" ) 2>/dev/null; }; then
    rm -f "$rop/.probe2" 2>/dev/null || true
    chmod 0755 "$rop" 2>/dev/null || true
    skip "lane-lock B9 sweep (release): this uid can write a 0555 directory (running as root), so a failed self-removal is not stageable here — unmeasurable on this host rather than passing vacuously"
  else
    chmod 0755 "$rop" 2>/dev/null || true
    out="$( cd "$d" && env -u SUPERVISOR_LOCK TMPDIR="$rop" LANE_ID="$lane" LOCK_CMD="" CLAIM_CMD="" \
              bash -c "$body_ro" _ "$SUPERVISOR" "$rop" 2>&1 )"; rc=$?
    chmod 0755 "$rop" 2>/dev/null || true
    # WHAT THIS CASE ACTUALLY PRODUCES IS A PARTIAL REMOVAL, AND IT USED TO PIN THE FALSE CLAIM ABOUT IT
    # (#3601, roborev job 240 B16). `rm -rf` unlinks CONTENTS before the directory, and those need write
    # permission on DIFFERENT directories — `<lock>/pid` needs write on `<lock>`, `<lock>` needs write on
    # its PARENT — so an unwritable parent deletes the pid record and leaves the directory. This case
    # therefore reaches the pid-less state, and the assert here REQUIRED the log to promise that the next
    # start would "reclaim it automatically", which is false: a pid-less lock is undecidable and the next
    # start refuses over it indefinitely. A test that requires a false promise is what keeps the promise
    # alive, so the state is now measured on disk FIRST and the claim asserted against it.
    local surviving_pid='no' surviving_dir='no'
    [[ -d "$rop/cqlite-worker-supervisor-$lane.lock" ]] && surviving_dir='yes'
    [[ -f "$rop/cqlite-worker-supervisor-$lane.lock/pid" ]] && surviving_pid='yes'
    if [[ "$surviving_dir" == 'yes' && "$surviving_pid" == 'no' ]]; then
      pass "lane-lock B16 PREMISE: this case really does produce the PARTIAL removal — the lock directory survives and its pid record is GONE, so the claim below has the state it is about"
    else
      fail "lane-lock-b16-premise: dir=[$surviving_dir] pid=[$surviving_pid] — the partial-removal state was not staged, so the asserts below would measure something else"
    fi
    if [[ "$out" == *"ACQUIRED="* ]] && [[ "$out" == *"PARTIALLY removed our own lock"* ]] \
       && [[ "$out" == *"will NOT clear itself"* ]] && [[ "$out" == *"ACTION IS NEEDED"* ]] \
       && [[ "$out" == *"PARENT directory is not writable"* ]]; then
      pass "lane-lock B16: the partial removal is named as a partial removal, stated NOT to self-clear, flagged as needing action, and the cause an operator must fix (the parent's permissions) is named — because the removal a later refusal prints needs that same permission"
    else
      fail "lane-lock-b16-report: rc=$rc out=[$out] — a partial removal must be reported as the wedge it is"
    fi
    # NEGATIVE CONTROL: the pre-B16 promise must be ABSENT. This is the assert that stops it returning.
    if [[ "$out" != *"reclaim it automatically"* ]]; then
      pass "lane-lock B16: and it does NOT promise automatic reclaim — the promise that pointed away from this wedge is gone"
    else
      fail "lane-lock-b16-false-promise: the log still promises the next start will reclaim automatically, which is false for a pid-less lock"
    fi
    chmod 0755 "$rop" 2>/dev/null || true
    rm -rf "$rop" 2>/dev/null || true
  fi

  rm -rf "$tmp"
}

t test_lane_lock_sweep_no_unobserved_attribution


# ---------------------------------------------------------------------------
# "NOT OURS" IS NOT THE SAME FACT AS "SOMEONE ELSE'S" (#3601, roborev job 240 B17).
#
# The release reported EVERY state other than its own pid as "Something else owns that name now" —
# including absent, unreadable, malformed and NUL-bearing records. Those establish only that ownership
# CANNOT BE VERIFIED. Attributing them to another process names a holder that may not exist and sends an
# operator looking for it; the DECISION is identical either way (remove nothing), so the wording is the
# entire content of the finding, which is why it has to be the wording that is true.
# ---------------------------------------------------------------------------
test_lane_lock_release_distinguishes_foreign_from_unverifiable() {
  local d tmp lane out body_steal foreign
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"
  lane="lane3601b17$$"
  mkdir -p "$tmp"

  # One drive, derived from the shipped body by a single insertion, that replaces the lock's pid record
  # with a value the caller chooses just before exiting — so the EXIT trap runs against that state.
  body_steal="${SV_DRIVE_BODY/'exit 0'/'printf "%s\n" "$2" >"$SUPERVISOR_LOCK/pid"; exit 0'}"
  if [[ "$body_steal" == "$SV_DRIVE_BODY" ]]; then
    fail "lane-lock-b17-premise: the steal drive was not derived from the shipped body"
    return 0
  fi

  # ---- (1) A PARSED FOREIGN PID: naming another holder is justified, because one was read.
  foreign=909090
  out="$( cd "$d" && env -u SUPERVISOR_LOCK TMPDIR="$tmp" LANE_ID="$lane" LOCK_CMD="" CLAIM_CMD="" \
            bash -c "$body_steal" _ "$SUPERVISOR" "$foreign" 2>&1 )"
  if [[ "$out" == *"records holder pid $foreign, not this process"* ]] \
     && [[ "$out" == *"Another holder owns that name"* ]]; then
    pass "lane-lock B17 (parsed foreign pid): the refusal names the holder it actually read — an attribution backed by a parse is fine, and this is the only branch that may make one"
  else
    fail "lane-lock-b17-foreign: out=[$out]"
  fi
  rm -rf "$tmp"; mkdir -p "$tmp"

  # ---- (2) AN UNPARSEABLE RECORD: ownership is unverifiable, and that is all that may be said.
  out="$( cd "$d" && env -u SUPERVISOR_LOCK TMPDIR="$tmp" LANE_ID="$lane" LOCK_CMD="" CLAIM_CMD="" \
            bash -c "$body_steal" _ "$SUPERVISOR" 'not-a-pid' 2>&1 )"
  if [[ "$out" == *"could not VERIFY that it owns it"* ]] \
     && [[ "$out" == *"does NOT establish that another process holds this lock"* ]] \
     && [[ "$out" == *"left exactly as found"* ]]; then
    pass "lane-lock B17 (unparseable record): the refusal reports that ownership could not be VERIFIED and says explicitly that this does not establish another holder — the decision is unchanged, only the claim is now true"
  else
    fail "lane-lock-b17-unverifiable: out=[$out]"
  fi
  # NEGATIVE CONTROL, which is the whole finding: the unparseable branch must not attribute a holder.
  if [[ "$out" != *"Another holder owns that name"* ]] && [[ "$out" != *"Something else owns that name"* ]]; then
    pass "lane-lock B17: and it does NOT say another holder owns the name — the attribution that named a process nobody read is gone"
  else
    fail "lane-lock-b17-attributes: the unparseable branch still attributes the lock to another holder, which it never established"
  fi
  # ...and either way the record is untouched, so the wording change did not come with a behaviour change.
  if [[ "$(cat "$tmp/cqlite-worker-supervisor-$lane.lock/pid" 2>/dev/null || true)" == 'not-a-pid' ]]; then
    pass "lane-lock B17: the unverifiable record is left byte-for-byte as found — the decision was always to remove nothing, and it still is"
  else
    fail "lane-lock-b17-removed: the unverifiable record was modified or removed"
  fi

  rm -rf "$tmp"
}

t test_lane_lock_release_distinguishes_foreign_from_unverifiable




# ---------------------------------------------------------------------------
# THE UN-CREATE IS BOUND TO THE DIRECTORY INSTANCE THIS RUN CREATED (#3601, roborev job 236 B12).
#
# THE DEFECT, AND IT WAS A REGRESSION THIS DIFF INTRODUCED. B1 made a failed publish remove the directory
# it had created, so a run would stop manufacturing the pid-less lock every other branch refuses to
# reclaim. But the removal was an UNCONDITIONAL `rmdir`, not bound to the instance: a legacy peer can
# rename ours aside and `mkdir` its OWN pid-less lock at the same name, and a non-recursive `rmdir`
# SUCCEEDS against that empty startup directory. B1 therefore traded "we wedge our own lane with an empty
# lock" for "we can delete a peer's startup lock" — the worse of the two.
#
# WHY NON-RECURSIVE WAS NOT ENOUGH, which is the whole subtlety: `rmdir` protects a peer that has already
# PUBLISHED (non-empty directory, removal fails harmlessly) and protects nothing at all against a peer
# that has `mkdir`'d and not yet published. The dangerous case is precisely the empty one.
#
# WHAT IS FIXED AND WHAT IS NOT: the UNCONDITIONAL DESTRUCTION is gone. The race is not closed — creating
# the marker is a second step after `mkdir`, so a peer that replaces the directory inside THAT window
# still receives our marker — and the site says so with the bound (two adjacent syscalls, no intervening
# I/O, versus the whole publish attempt before). #3683 closes it.
# ---------------------------------------------------------------------------
test_lane_lock_uncreate_is_bound_to_the_created_instance() {
  local d tmp lane lock out rc ovr swap contents
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"
  lane="lane3601inst$$"
  mkdir -p "$tmp"
  lock="$tmp/cqlite-worker-supervisor-$lane.lock"

  # ---- (1) THE MARKER DOES NOT OUTLIVE THE ACQUIRE. A held lock carrying an extra file would make the
  # non-recursive clear command handed to operators elsewhere refuse, so this is a property of the fix and
  # not an incidental detail.
  rm -rf "$lock"
  local body_peek
  body_peek="${SV_DRIVE_BODY/'exit 0'/'printf "CONTENTS=[%s]\n" "$(ls -A "$SUPERVISOR_LOCK" | tr "\n" " ")"; trap - EXIT; exit 0'}"
  if [[ "$body_peek" == "$SV_DRIVE_BODY" ]]; then
    fail "lane-lock-b12-premise: the contents-peek drive body was not derived from the shipped one"
    return 0
  fi
  out="$( cd "$d" && env -u SUPERVISOR_LOCK TMPDIR="$tmp" LANE_ID="$lane" LOCK_CMD="" CLAIM_CMD="" \
            bash -c "$body_peek" _ "$SUPERVISOR" 2>&1 )"
  contents="$(printf '%s\n' "$out" | grep -o 'CONTENTS=\[[^]]*\]' || true)"
  if [[ "$contents" == 'CONTENTS=[pid ]' ]]; then
    pass "lane-lock B12: an acquired lock holds exactly \`pid\` — the ownership marker is removed the moment ownership is verified, so it cannot make a later manual clear refuse"
  else
    fail "lane-lock-b12-marker-outlives: $contents — the marker must not survive a successful acquire"
  fi
  rm -rf "$lock"

  # ---- (2) THE REPORTED INTERLEAVING, STAGED FOR REAL. The publish fails, and BEFORE it does, a peer
  # renames our directory aside and `mkdir`s its own PID-LESS lock at the same name — the exact case a
  # non-recursive `rmdir` succeeds against. Fault-injected on the publish; `take`'s un-create decision,
  # which is what is being asserted, is shipped code.
  swap='  local tmpf="$SUPERVISOR_LOCK/pid.tmp.$$" state='"''"'
  mv -- "$SUPERVISOR_LOCK" "$SUPERVISOR_LOCK.peeraside" 2>/dev/null || true
  mkdir -- "$SUPERVISOR_LOCK" 2>/dev/null || true
  if true; then printf "%s" write-failed; return 1; fi'
  ovr="$d/f-swap.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" supervisor_lock_publish \
       '  local tmpf="$SUPERVISOR_LOCK/pid.tmp.$$" state='"''" "$swap"; then
    out="$(lane_lock_drive_at "$d" "$ovr" "$tmp" "$lane")"; rc=$?
    if [[ "$rc" -ne 0 ]] && [[ -d "$lock" ]]; then
      pass "lane-lock B12: with a peer's PID-LESS lock at that name the run refuses and the peer's lock SURVIVES — the case a bare non-recursive removal destroys"
    else
      fail "lane-lock-b12-peer-lock-destroyed: rc=$rc lock=[$([[ -d "$lock" ]] && echo present || echo DESTROYED)] — this is the regression B1 introduced and B12 removes"
    fi
    if [[ "$out" == *"NOT the one it created"* ]] && [[ "$out" == *"removed NOTHING"* ]] \
       && [[ "$out" == *"start in progress"* ]] && [[ "$out" != *"absence was VERIFIED"* ]]; then
      pass "lane-lock B12: and the refusal says WHY it removed nothing — the marker is gone, so the directory is another run's start in progress — instead of claiming a removal (the B9 family)"
    else
      fail "lane-lock-b12-diagnostic: out=[$out]"
    fi
  fi
  rm -rf "$lock" "$lock.peeraside"

  # ---- (3) NON-VACUITY: with NO peer, the same failed publish still un-creates its own directory and the
  # absence is verified. Without this, (2) is satisfied by code that never removes anything at all — which
  # would reinstate the wedge B1 fixed.
  ovr="$d/f-write.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" supervisor_lock_publish \
       '  if ! { printf '"'"'%s\n'"'"' "$$" >"$tmpf"; } 2>/dev/null; then' \
       '  if true; then'; then
    out="$(lane_lock_drive_at "$d" "$ovr" "$tmp" "$lane")" || true
    if [[ "$out" == *"absence was VERIFIED after the removal"* ]] && [[ ! -e "$lock" ]]; then
      pass "lane-lock B12 NON-VACUITY: with no peer involved the run still removes the directory it created and verifies its absence — the instance binding costs the legitimate path nothing"
    else
      fail "lane-lock-b12-nonvacuity: lock=[$([[ -e "$lock" ]] && echo present || echo gone)] out=[$out] — declining to remove our OWN empty lock would reinstate the wedge B1 fixed"
    fi
  fi

  # ---- (4) MUTANT CONTRAST: the marker check disabled, which is the pre-B12 spelling. The identical
  # staging from (2) must then DESTROY the peer's lock.
  rm -rf "$lock" "$lock.peeraside"
  local ovrm mout
  ovrm="$d/m-uncond.sh"; : >"$ovrm"
  if sv_mutant_override "$ovrm" supervisor_lock_take \
       '  if [[ ! -e "$marker" && ! -L "$marker" ]]; then' \
       '  if false; then'; then
    # the same peer-swap fault, appended so ONE override carries both
    if sv_mutant_override "$ovrm" supervisor_lock_publish \
         '  local tmpf="$SUPERVISOR_LOCK/pid.tmp.$$" state='"''" "$swap"; then
      mout="$(lane_lock_drive_at "$d" "$ovrm" "$tmp" "$lane")" || true
      if [[ ! -d "$lock" ]] && [[ "$mout" == *"absence was VERIFIED"* ]]; then
        pass "lane-lock B12 MUTANT: with the instance binding removed the identical case DELETES the peer's pid-less lock and reports the absence as its own success — the regression, measured, so (2) is the marker doing the work"
      else
        fail "lane-lock-mutant-b12: lock=[$([[ -d "$lock" ]] && echo present || echo gone)] out=[$mout] — the unconditional form must be shown to destroy the peer's lock"
      fi
    fi
  fi
  rm -rf "$lock" "$lock.peeraside"

  # ---- (5) A MARKER THAT CANNOT BE WRITTEN IS NOT OWNERSHIP EVIDENCE, so nothing is removed. Staged with
  # a lock directory whose creation succeeds and whose contents cannot be written — the parent is
  # writable-and-searchable, the created directory is not writable, which `mkdir -m` cannot express, so
  # the marker write is faulted instead and the DECISION is shipped.
  local ovrk
  ovrk="$d/f-marker.sh"; : >"$ovrk"
  if sv_mutant_override "$ovrk" supervisor_lock_take \
       '  if [[ "$marker_written" -eq 0 ]]; then' \
       '  if true; then'; then
    out="$(lane_lock_drive_at "$d" "$ovrk" "$tmp" "$lane")"; rc=$?
    if [[ "$rc" -ne 0 ]] && [[ "$out" == *"did NOT attempt to remove"* ]] \
       && [[ "$out" == *"cannot prove"* ]] && [[ "$out" == *"makes no claim either way"* ]]; then
      pass "lane-lock B12: a marker that could not be written leaves the directory ALONE and says so — no ownership evidence means no removal, and the refusal claims nothing about what is at that path"
    else
      fail "lane-lock-b12-marker-failed: rc=$rc out=[$out] — with no marker the run must neither remove nor claim"
    fi
  fi
  rm -rf "$lock"

  # ---- (6) THE RESIDUAL IS STATED AT THE SITE, in the same NARROWED-NOT-CLOSED form as the other two,
  # and does not present the marker as airtight. The comment is the artifact a reviewer is asked to trust
  # about a window that is still open, which is the B9 family if it overclaims.
  local body
  body="$(sed -n '/^supervisor_lock_take()/,/^}/p' "$SUPERVISOR")"
  if [[ "$body" == *'NARROWED, NOT CLOSED'* ]] && [[ "$body" == *'#3683'* ]] \
     && [[ "$body" == *'two adjacent syscalls'* ]] \
     && [[ "$body" == *'not read the marker as a guarantee'* ]]; then
    pass "lane-lock B12: the site states the marker's OWN window, bounds it (two adjacent syscalls), names #3683 and says explicitly not to read it as a guarantee"
  else
    fail "lane-lock-b12-comment-overclaims: the marker's own absence window must be stated with its bound; presenting it as airtight is the alibi family"
  fi

  rm -rf "$tmp"
}

t test_lane_lock_uncreate_is_bound_to_the_created_instance




# ---------------------------------------------------------------------------
# A RACE AND AN I/O ERROR MUST NOT SHARE A DIAGNOSTIC (#3601, roborev job 244 B20 + class sweep).
#
# THE DEFECT: `supervisor_lock_take` returns 1 when its `mkdir` fails, and `mkdir` fails BOTH because
# someone else took the name (EEXIST — a race, where re-running is exactly right) and because the path
# cannot hold a lock (EACCES/ENOSPC/ENOENT — where re-running loops forever over a state that cannot
# resolve until permissions or disk are fixed). After a SUCCESSFUL rename-aside, every status 1 was
# reported as a lost race: "a stale lock was cleared and the name was claimed by someone else … re-run
# this supervisor". On a broken filesystem that tells an operator nothing is wrong and sends them into a
# retry loop.
#
# WHY THIS ONE IS NOT JUST ANOTHER OVERCLAIM: it is #3601'S OWN HEADLINE DEFECT, one branch over. The AC7
# addendum exists because the pre-fix code "blames the lock rather than the path shape, and an operator
# goes looking for a stale lock that isn't there". The two facts want OPPOSITE actions — retry versus fix
# the box — which is what makes conflating them a misdirection rather than an imprecision.
#
# THE SWEEP: three sites in this file shared that predicate and did not disambiguate (the post-reclaim
# take, the publish's rename, the marker write); two others already did (the FIRST take, and the reclaim
# rename), which is what made the omissions oversights rather than design. All five are asserted here.
# ---------------------------------------------------------------------------
test_lane_lock_race_and_io_error_are_not_conflated() {
  local d tmp lane lock out rc dead ovr

  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"
  lane="lane3601conflate$$"
  mkdir -p "$tmp"
  lock="$tmp/cqlite-worker-supervisor-$lane.lock"

  fixture_bg sleep 0.1
  dead=$FIXTURE_LAST_PID
  fixture_wait "$dead"

  # ---- (1) B20: the SECOND take's mkdir fails while NOTHING is at the name. Fault-injected so only the
  # second call fails, which is the interleaving the finding describes; the branch under test — the
  # existence check that picks the refusal — is shipped code.
  rm -rf "$lock"
  mkdir -p "$lock"
  printf '%s\n' "$dead" >"$lock/pid"
  ovr="$d/f-second-take.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" supervisor_lock_take \
       '  mkdir -- "$SUPERVISOR_LOCK" 2>/dev/null || return 1' \
       '  if [[ -n "${SV_SECOND_TAKE:-}" ]]; then return 1; fi
  export SV_SECOND_TAKE=1
  mkdir -- "$SUPERVISOR_LOCK" 2>/dev/null || return 1'; then
    out="$(lane_lock_drive_at "$d" "$ovr" "$tmp" "$lane")"; rc=$?
    if [[ "$rc" -ne 0 ]] && [[ "$out" == *"NOTHING is at that name now"* ]] \
       && [[ "$out" == *"after a stale lock was successfully cleared"* ]] \
       && [[ "$out" == *"ORDER TO TELL THEM APART"* ]] \
       && [[ "$out" != *"taken again before this run could claim it"* ]]; then
      pass "lane-lock B20/B21: a post-reclaim claim that fails with NOTHING at the name reports that observation and the phase it happened in, and hands the operator both candidate causes in order — never the lost-race story"
    else
      fail "lane-lock-b20-conflated: rc=$rc out=[$out] — reporting a filesystem failure as a lost race sends the operator into a retry loop over a state that cannot resolve"
    fi
  fi

  # ---- (2) NON-VACUITY: a real lost race must still be reported as one, or (1) is satisfied by code
  # that has simply stopped recognising contention. The name is occupied by a LIVE holder at the moment
  # the second take runs, so the existence check must select the race refusal.
  rm -rf "$lock"
  mkdir -p "$lock"
  printf '%s\n' "$dead" >"$lock/pid"
  ovr="$d/f-second-take-occupied.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" supervisor_lock_take \
       '  mkdir -- "$SUPERVISOR_LOCK" 2>/dev/null || return 1' \
       '  if [[ -n "${SV_SECOND_TAKE:-}" ]]; then mkdir -p -- "$SUPERVISOR_LOCK" 2>/dev/null || true; printf "%s\n" 313131 >"$SUPERVISOR_LOCK/pid" 2>/dev/null || true; return 1; fi
  export SV_SECOND_TAKE=1
  mkdir -- "$SUPERVISOR_LOCK" 2>/dev/null || return 1'; then
    out="$(lane_lock_drive_at "$d" "$ovr" "$tmp" "$lane")"; rc=$?
    if [[ "$rc" -ne 0 ]] && [[ "$out" == *"taken again before this run could claim it"* ]] \
       && [[ "$out" != *"NOTHING is at that name now"* ]]; then
      pass "lane-lock B20 NON-VACUITY: when the name IS occupied the same failure is still reported as a lost race — the existence test discriminates, it does not just relabel everything as a path failure"
    else
      fail "lane-lock-b20-nonvacuity: rc=$rc out=[$out] — a genuine race must still read as a race"
    fi
  fi

  # ---- (3) SWEEP SITE: the publish's RENAME fails because the lock directory vanished under it. That is
  # contention, and the pre-sweep text called every rename failure a FILESYSTEM failure and sent the
  # operator to check a disk that is fine.
  rm -rf "$lock"
  ovr="$d/f-rename-gone.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" supervisor_lock_publish \
       '  if ! mv -f -- "$tmpf" "$SUPERVISOR_LOCK/pid" 2>/dev/null; then' \
       '  rm -rf -- "$SUPERVISOR_LOCK"
  if true; then'; then
    out="$(lane_lock_drive_at "$d" "$ovr" "$tmp" "$lane")"; rc=$?
    # B21: the vanished directory is an OBSERVATION and may be reported; the CAUSE may not be claimed from
    # it, because a peer can remove or recreate that name between the failure and the check either way.
    if [[ "$rc" -ne 0 ]] && [[ "$out" == *"was GONE when this run looked"* ]] \
       && [[ "$out" == *"NOT ESTABLISHED"* ]] \
       && [[ "$out" == *"ORDER TO TELL THEM APART"* ]] \
       && [[ "$out" != *"This is CONTENTION"* ]] && [[ "$out" != *"This is a FILESYSTEM failure"* ]]; then
      pass "lane-lock sweep/B21 (publish rename): the vanished directory is reported as what was OBSERVED, the cause is explicitly not established, and the operator gets both candidates in order"
    else
      fail "lane-lock-sweep-rename: rc=$rc out=[$out]"
    fi
  fi

  # ---- (4) SWEEP SITE: the MARKER write fails because the directory vanished under it. Same predicate,
  # same two natures, one step earlier.
  rm -rf "$lock"
  ovr="$d/f-marker-gone.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" supervisor_lock_take \
       '  if { : >"$marker"; } 2>/dev/null; then marker_written=1; fi' \
       '  rm -rf -- "$SUPERVISOR_LOCK" 2>/dev/null || true'; then
    out="$(lane_lock_drive_at "$d" "$ovr" "$tmp" "$lane")"; rc=$?
    if [[ "$rc" -ne 0 ]] && [[ "$out" == *"was GONE when this run looked"* ]] \
       && [[ "$out" == *"NOT ESTABLISHED"* ]] \
       && [[ "$out" == *"already gone when it looked"* ]] \
       && [[ "$out" != *"This is CONTENTION"* ]] && [[ "$out" != *"This is a FILESYSTEM failure"* ]]; then
      pass "lane-lock sweep/B21 (marker write): same at the marker step — the observation is reported, the cause is not claimed, and it still states that nothing was left behind because nothing was there to remove"
    else
      fail "lane-lock-sweep-marker: rc=$rc out=[$out]"
    fi
  fi

  # ---- (5) NON-VACUITY for (3)/(4): a REAL filesystem failure must still read as one. The write fails
  # with the directory intact, so the nature must be `filesystem` and the action must be to check the box.
  rm -rf "$lock"
  ovr="$d/f-write-fs.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" supervisor_lock_publish \
       '  if ! { printf '"'"'%s\n'"'"' "$$" >"$tmpf"; } 2>/dev/null; then' \
       '  if true; then'; then
    out="$(lane_lock_drive_at "$d" "$ovr" "$tmp" "$lane")" || true
    # B21 RETIRED THIS CONTROL'S ORIGINAL PROPERTY, AND THE REPLACEMENT IS THE ONE THAT IS TRUE. It used
    # to require a genuine filesystem failure to be REPORTED as one — but this code cannot know that it
    # was one (no errno, and the path's later state cannot decide it), so requiring the confident verdict
    # was requiring a claim beyond the evidence: a test pinning an overclaim, the shape that kept this
    # family alive. What must hold instead: the step that failed is named, both candidate causes are
    # given with the order to check them, and NEITHER is asserted.
    if [[ "$out" == *"no byte of it was written"* ]] \
       && [[ "$out" == *"NOT ESTABLISHED"* ]] \
       && [[ "$out" == *"check the PARENT directory"* ]] \
       && [[ "$out" == *"CONTENTION: if all of those are clean"* ]] \
       && [[ "$out" != *"This is a FILESYSTEM failure"* ]] && [[ "$out" != *"This is CONTENTION,"* ]]; then
      pass "lane-lock B21: a failed write names the STEP that failed, states the cause is not established, and gives both candidates in order — the confident verdict is gone in BOTH directions, not swapped for the other one"
    else
      fail "lane-lock-sweep-nonvacuity: out=[$out]"
    fi
  fi

  # ---- (6) STRUCTURAL: every remedy branch in the publish-failure refusal takes its FIRST ACTION from
  # the failure's nature rather than hard-coding one. This is what stops a new cleanup branch being added
  # with "fix the filesystem problem" baked in, which is how sites 3 and 4 came to misdirect.
  local body hardcoded
  body="$(sed -n '/^supervisor_lock_refuse_publish_failed()/,/^}/p' "$SUPERVISOR")"
  if [[ -z "$body" || "$body" != *'supervisor_lock_refuse_publish_failed() {'* ]]; then
    fail "lane-lock-sweep-structural-premise: could not extract the refusal from $SUPERVISOR"
  else
    # Anchored on an INDENTED assignment, so the function's own `local … remedy='' …` declaration is not
    # matched — an earlier cut of this guard flagged that line and reported a defect that was not there,
    # which is this diff's own family showing up in a guard written for it.
    hardcoded="$(printf '%s\n' "$body" | grep -nE '^ +remedy=' | grep -v 'first_action' || true)"
    if [[ -z "$hardcoded" ]]; then
      pass "lane-lock sweep STRUCTURAL: every remedy branch derives its first action from the failure's NATURE — no branch hard-codes an action that could be aimed at the wrong cause"
    else
      fail "lane-lock-sweep-hardcoded-remedy: [$hardcoded] — a remedy that names an action without consulting the nature is how a race gets reported as a disk problem"
    fi
  fi

  rm -rf "$lock" "$tmp"
}

t test_lane_lock_race_and_io_error_are_not_conflated




# ---------------------------------------------------------------------------
# A FAILURE'S CAUSE IS NOT INFERRED FROM THE PATH'S LATER STATE (#3601, roborev job 245 B21).
#
# THE TWO DIRECTIONS, INTRODUCED TWO ROUNDS APART, BOTH FROM THE SAME PREDICATE:
#   * job 244 (B20): `mkdir` fails with EEXIST, the name is present, and the code called it a PATH failure
#     — contention reported as a disk problem, retry-able state reported as needing repair.
#   * job 245 (B21): contender A moves the old lock aside; B's operation fails while the name is ABSENT;
#     A republishes before B reaches the check. B reports a FILESYSTEM fault and sends an operator to
#     repair permissions on a box that is fine — a disk problem reported where there was contention.
#
# A post-failure existence test cannot decide this in either direction, because a peer can remove or
# recreate that name between the failure and the check. The failing call's errno is the only thing that
# could, and this script cannot see it; recovering it from `mv`/`mkdir` stderr is locale-dependent guessing
# dressed as a verdict (the cargo-output-parse class, CLAUDE.md #3400). So the ambiguity is PRESERVED —
# and made actionable, because "retry versus fix the box" was the sweep's whole value and a shrug would
# throw it away: the text names both candidates with what to check for each, in order.
#
# THE INTERLEAVING IS STAGED FOR REAL — the operation fails while the name is absent and the name is then
# recreated with a foreign holder record before the code looks, which is exactly A republishing.
# ---------------------------------------------------------------------------
test_lane_lock_failure_cause_is_not_inferred_from_later_state() {
  local d tmp lane lock out rc ovr
  d="$(new_case_dir)"
  common_env "$d"
  tmp="$d/tmp"
  lane="lane3601cause$$"
  mkdir -p "$tmp"
  lock="$tmp/cqlite-worker-supervisor-$lane.lock"

  # ---- (1) THE B21 INTERLEAVING: disappearance, then reappearance, before the check.
  rm -rf "$lock"
  ovr="$d/f-gone-then-back.sh"; : >"$ovr"
  if sv_mutant_override "$ovr" supervisor_lock_publish \
       '  if ! mv -f -- "$tmpf" "$SUPERVISOR_LOCK/pid" 2>/dev/null; then' \
       '  rm -rf -- "$SUPERVISOR_LOCK"
  mkdir -p -- "$SUPERVISOR_LOCK" 2>/dev/null || true
  printf "%s\n" 424242 >"$SUPERVISOR_LOCK/pid" 2>/dev/null || true
  if true; then'; then
    out="$(lane_lock_drive_at "$d" "$ovr" "$tmp" "$lane")"; rc=$?
    if [[ "$rc" -ne 0 ]] && [[ "$out" == *"NOT ESTABLISHED"* ]] \
       && [[ "$out" != *"This is a FILESYSTEM failure"* ]] \
       && [[ "$out" != *"This is CONTENTION,"* ]]; then
      pass "lane-lock B21: when the name vanishes and REAPPEARS before the check, the refusal asserts NEITHER cause — the interleaving that made the post-failure existence test report a disk fault over pure contention"
    else
      fail "lane-lock-b21-infers-cause: rc=$rc out=[$out] — a cause inferred from the path's later state is wrong in this interleaving whichever way it lands"
    fi
    # ...and it is still ACTIONABLE, which is the constraint that makes ambiguity acceptable here.
    if [[ "$out" == *"ORDER TO TELL THEM APART"* ]] \
       && [[ "$out" == *"A FILESYSTEM FAULT: check the PARENT directory"* ]] \
       && [[ "$out" == *"CONTENTION: if all of those are clean"* ]]; then
      pass "lane-lock B21: and the ambiguity is ACTIONABLE — both candidates are named, each with what to check, in the order to check them; an honest 'one of these two, here is how to tell' rather than a shrug"
    else
      fail "lane-lock-b21-unactionable: out=[$out] — going ambiguous without telling the operator how to discriminate gives up the sweep's whole value"
    fi
  fi

  # ---- (2) THE PATH IS STILL NAMED FIRST, so #3601's own AC7 value survives the ambiguity: an operator
  # facing an option-shaped or unwritable TMPDIR is still pointed at the parent before anything else.
  local ro
  ro="$d/ro-parent"
  mkdir -p "$ro"
  chmod 0555 "$ro" 2>/dev/null || true
  if ( : >"$ro/.probe" ) 2>/dev/null; then
    rm -f "$ro/.probe" 2>/dev/null || true
    chmod 0755 "$ro" 2>/dev/null || true
    skip "lane-lock B21 (2): this uid can write a 0555 directory (running as root), so an uncreatable lock path is not stageable here"
  else
    out="$(lane_lock_drive_at "$d" - "$ro" "$lane")"; rc=$?
    chmod 0755 "$ro" 2>/dev/null || true
    if [[ "$rc" -ne 0 ]] && [[ "$out" == *"NOTHING is at that name now"* ]] \
       && [[ "$out" == *"A FILESYSTEM FAULT: check the PARENT directory"* ]] \
       && [[ "$out" == *"TMPDIR"* ]]; then
      pass "lane-lock B21: the uncreatable-path refusal still reports what it OBSERVED and still puts the parent directory and TMPDIR first — the useful half of AC7's fix survives dropping the confident verdict"
    else
      fail "lane-lock-b21-lost-ac7-value: rc=$rc out=[$out] — dropping the false claim must not drop the operator's first check"
    fi
  fi

  # ---- (3) STRUCTURAL: no site may reintroduce a confident cause. The two emitters are the ONLY place a
  # cause is described, so a new failure branch cannot quietly assert one — which is how B20 and B21 each
  # arrived, one branch at a time.
  local confident
  confident="$(grep -nE "This is a FILESYSTEM failure|This is CONTENTION|not contention|nature='(filesystem|contention)'" "$SUPERVISOR" | grep -vE '^[0-9]+:[[:space:]]*#' || true)"
  if [[ -z "$confident" ]]; then
    pass "lane-lock B21 STRUCTURAL: no code line in the supervisor asserts a failure's cause as contention or filesystem — every site routes through the shared ambiguity emitters, so a new branch cannot claim one by accident"
  else
    fail "lane-lock-b21-confident-nature-returned: [$confident] — a cause asserted from a post-failure state check is unsound in both directions"
  fi
  # ...and both emitters must actually exist and be used, or (3) passes vacuously on a file that says
  # nothing at all about causes.
  local uses
  uses="$(grep -c 'supervisor_lock_nature_unestablished\|supervisor_lock_nature_actions' "$SUPERVISOR" || true)"
  if [[ "$uses" -ge 6 ]]; then
    pass "lane-lock B21 STRUCTURAL: the ambiguity emitters are defined and referenced $uses times — (3) is asserting an absence over a file that does discuss causes, not over silence"
  else
    fail "lane-lock-b21-emitters-missing: only $uses reference(s) — the absence assert above would be vacuous"
  fi

  rm -rf "$tmp"
}

t test_lane_lock_failure_cause_is_not_inferred_from_later_state


# ---------------------------------------------------------------------------
# Tests (#3749): the THROTTLED shared-object-store sweep in the preflight path.
#
# Every case runs the supervisor from a SCRATCH TREE carrying a STUB sweep script — the
# artifact is SUBSTITUTED rather than reached through a path variable, because a test-only
# seam is one more thing a real invoker can set (CLAUDE.md's #3312 corollary), and because
# the real sweep's own behaviour belongs to
# scripts/tests/test_check_object_store_integrity.sh. What these assert is the
# SUPERVISOR's half: which verdict stops the loop, which one does not, and that the
# throttle throttles.
#
# RED-ARM DISCIPLINE: the three verdict cases differ from one another in EXACTLY ONE
# property — the stub's verdict line and exit status — and each stub RECORDS its
# invocations, so a case cannot pass against a sweep that never ran.
# ---------------------------------------------------------------------------
# obj_sweep_tree <dir> <verdict-line> <exit> [call-log] -> scratch repo root
#
# The runs below pass `LANE_ID` EXPLICITLY (per invocation, never exported, so nothing
# leaks into a later case): the supervisor refuses to start with
# `FATAL: lane-identity-unprovable` when it cannot derive a per-lane identity, and a
# plain scratch repo is not a lane worktree. Supplying the identity is what the refusal
# itself recommends, and it keeps the fixture a one-`git init` tree.
obj_sweep_tree() {
  local d="$1" verdict="$2" rc="$3" calllog="${4:-}" root="$d/root"
  mkdir -p "$root/scripts/local" "$root/scripts/lib"
  git -C "$root" init -q 2>/dev/null
  git -C "$root" -c user.email=t@t -c user.name=t commit -q --allow-empty -m init 2>/dev/null
  cp "$SUPERVISOR" "$root/scripts/local/worker-supervisor.sh"
  # scripts/lib is needed by the default notify path (an incomplete scratch tree produces
  # an unattributable failure — learned by the lane-identity cases above).
  cp "$REPO_ROOT/scripts/lib/gate-notify.sh" "$root/scripts/lib/" 2>/dev/null || true
  {
    printf '#!/usr/bin/env bash\n'
    printf '# STUB sweep: record the invocation, print one anchored verdict, exit with its code.\n'
    [[ -n "$calllog" ]] && printf 'printf "called\\n" >>%s\n' "$(printf '%q' "$calllog")"
    printf 'printf "OBJECT-STORE: measured stub\\n"\n'
    printf 'printf "OBJECT-STORE: object deadbeefdeadbeefdeadbeefdeadbeefdeadbeef\\n"\n'
    printf 'printf "OBJECT-STORE: verdict %s\\n"\n' "$verdict"
    printf 'exit %s\n' "$rc"
  } >"$root/scripts/check-object-store-integrity.sh"
  chmod +x "$root/scripts/check-object-store-integrity.sh"
  printf '%s' "$root"
}

# obj_sweep_calls <file> -> the number of recorded invocations, ALWAYS one integer.
# `grep -c .` prints `0` AND EXITS 1 on an existing-but-empty file, so the idiom
# `n=$(grep -c . f || echo 0)` yields the two-line value `0\n0` and the `[[ -eq ]]`
# that follows ERRORS instead of comparing — a diagnostic garbled exactly on the
# failing path, and the two-valued collapse this repo lints for elsewhere.
obj_sweep_calls() {
  local n
  n="$(grep -c . "$1" 2>/dev/null || true)"
  [[ "$n" =~ ^[0-9]+$ ]] || n=0
  printf '%s' "$n"
}

test_object_store_sweep_verdicts() {
  local d root counter rc calls
  # (a) VERIFIED: the sweep runs, is journalled, and the iteration proceeds normally.
  d="$(new_case_dir)"; counter="$d/counter"; calls="$d/calls-verified"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export OBJ_SWEEP_INTERVAL_HOURS=6
  export OBJ_SWEEP_STAMP="$d/sweep.stamp"
  root="$(obj_sweep_tree "$d" VERIFIED 0 "$calls")"
  env LANE_ID=objsweep-test bash "$root/scripts/local/worker-supervisor.sh" >"$d/stdout.log" 2>&1
  rc=$?
  if [[ "$rc" -eq 0 && -s "$calls" && -f "$counter" ]] &&
     grep -q 'object-store: VERIFIED' "$d/stdout.log"; then
    pass "obj-sweep(VERIFIED): the sweep ran, was journalled, and the worker still ran"
  else
    fail "obj-sweep(VERIFIED): rc=$rc swept=$([[ -s "$calls" ]] && echo yes || echo no) spawned=$([[ -f "$counter" ]] && echo yes || echo no) (see $d)"
  fi
  # The stamp is written, which is what the throttle reads.
  if [[ -s "$OBJ_SWEEP_STAMP" ]] && grep -qE '^[0-9]+$' "$OBJ_SWEEP_STAMP"; then
    pass "obj-sweep(VERIFIED): the throttle stamp records an epoch"
  else
    fail "obj-sweep(stamp): no usable stamp at $OBJ_SWEEP_STAMP"
  fi

  # (b) CORRUPT: ONE property different from (a) — the stub's verdict. It must STOP the
  # loop (corruption is non-self-clearing, so a HOLD would spin to the budget) and NO
  # worker may run: a worker must never certify against a damaged shared store.
  d="$(new_case_dir)"; counter="$d/counter"; calls="$d/calls-corrupt"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export OBJ_SWEEP_INTERVAL_HOURS=6
  export OBJ_SWEEP_STAMP="$d/sweep.stamp"
  root="$(obj_sweep_tree "$d" CORRUPT 4 "$calls")"
  env LANE_ID=objsweep-test bash "$root/scripts/local/worker-supervisor.sh" >"$d/stdout.log" 2>&1
  rc=$?
  if [[ "$rc" -eq 1 && -s "$calls" && ! -f "$counter" ]] &&
     grep -q '"reason":"object-store-corrupt"' "$JOURNAL_FILE" &&
     grep -q 'object-store: CORRUPT' "$d/stdout.log"; then
    pass "obj-sweep(CORRUPT): stops the loop loudly (rc=1, own reason) and spawns NO worker"
  else
    fail "obj-sweep(CORRUPT): rc=$rc swept=$([[ -s "$calls" ]] && echo yes || echo no) spawned=$([[ -f "$counter" ]] && echo yes || echo no) (see $d)"
  fi
  if grep -q 'OBJECT STORE CORRUPT' "$NOTIFY_LOG" 2>/dev/null &&
     grep -q 'deadbeefdeadbeefdeadbeefdeadbeefdeadbeef' "$d/stdout.log"; then
    pass "obj-sweep(CORRUPT): pages high AND carries the sweep's own findings (object ids) into the journal"
  else
    fail "obj-sweep(CORRUPT): no high page, or the findings were dropped (see $NOTIFY_LOG)"
  fi
  # AND IT IS NOT A HOLD REASON. A HOLD would have logged `HOLD:` and repolled until the
  # wall-clock budget with no useful action — the latch #2670 bounded the leftover families
  # to avoid. Asserted directly, because "it stopped" alone cannot distinguish the two.
  if ! grep -q 'HOLD: object-store' "$d/stdout.log"; then
    pass "obj-sweep(CORRUPT): it is NOT a hold reason (non-self-clearing: a repoll loop would spin to the budget)"
  else
    fail "obj-sweep(CORRUPT): corruption was treated as a HOLD"
  fi

  # (c) UNMEASURED: again ONE property different — reported, paged once, and DELIBERATELY
  # NOT a stop: refusing to run any worker because a hygiene probe could not run is a
  # self-DoS. The permissive branch is asserted here so a future "tighten it" edit reds.
  d="$(new_case_dir)"; counter="$d/counter"; calls="$d/calls-unmeasured"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export OBJ_SWEEP_INTERVAL_HOURS=6
  export OBJ_SWEEP_STAMP="$d/sweep.stamp"
  root="$(obj_sweep_tree "$d" UNMEASURED 5 "$calls")"
  env LANE_ID=objsweep-test bash "$root/scripts/local/worker-supervisor.sh" >"$d/stdout.log" 2>&1
  rc=$?
  if [[ "$rc" -eq 0 && -s "$calls" && -f "$counter" ]] &&
     grep -q 'object-store: UNMEASURED' "$d/stdout.log" &&
     grep -q 'UNKNOWN, not clean' "$d/stdout.log"; then
    pass "obj-sweep(UNMEASURED): reported as UNKNOWN-not-clean, paged, and the loop CONTINUES (a hygiene probe must not stop the fleet)"
  else
    fail "obj-sweep(UNMEASURED): rc=$rc swept=$([[ -s "$calls" ]] && echo yes || echo no) spawned=$([[ -f "$counter" ]] && echo yes || echo no) (see $d)"
  fi
}

test_object_store_sweep_throttle_and_disable() {
  local d root counter calls rc n
  # (a) THE THROTTLE: two runs sharing one stamp sweep ONCE. The stub counts invocations,
  # so this measures the throttle rather than inferring it from absent log lines.
  d="$(new_case_dir)"; counter="$d/counter"; calls="$d/calls"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export OBJ_SWEEP_INTERVAL_HOURS=6
  export OBJ_SWEEP_STAMP="$d/sweep.stamp"
  root="$(obj_sweep_tree "$d" VERIFIED 0 "$calls")"
  env LANE_ID=objsweep-test bash "$root/scripts/local/worker-supervisor.sh" >"$d/run1.log" 2>&1
  env LANE_ID=objsweep-test bash "$root/scripts/local/worker-supervisor.sh" >"$d/run2.log" 2>&1
  n=$(obj_sweep_calls "$calls")
  if [[ "$n" -eq 1 ]]; then
    pass "obj-sweep(throttle): two runs inside the interval sweep exactly ONCE (measured invocations, not inferred)"
  else
    fail "obj-sweep(throttle): the sweep ran $n time(s) across two runs, wanted 1 (see $calls)"
  fi
  # (b) A STAMP IN THE FUTURE must not park the sweep forever (clock skew, a restored
  # snapshot, a hand-edited file). ONE property apart from (a): the stamp's value.
  printf '%s\n' "$(( $(date +%s) + 86400 ))" >"$OBJ_SWEEP_STAMP"
  env LANE_ID=objsweep-test bash "$root/scripts/local/worker-supervisor.sh" >"$d/run3.log" 2>&1
  n=$(obj_sweep_calls "$calls")
  if [[ "$n" -eq 2 ]]; then
    pass "obj-sweep(throttle): a stamp in the FUTURE is treated as never-swept, not as a permanent skip"
  else
    fail "obj-sweep(future-stamp): invocations=$n, wanted 2"
  fi
  # (c) DISABLED (interval 0) — announced in the journal, and the sweep really does not run.
  d="$(new_case_dir)"; counter="$d/counter"; calls="$d/calls-off"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export OBJ_SWEEP_INTERVAL_HOURS=0
  export OBJ_SWEEP_STAMP="$d/sweep.stamp"
  root="$(obj_sweep_tree "$d" VERIFIED 0 "$calls")"
  env LANE_ID=objsweep-test bash "$root/scripts/local/worker-supervisor.sh" >"$d/stdout.log" 2>&1
  rc=$?
  if [[ "$rc" -eq 0 && ! -s "$calls" ]] &&
     grep -q 'object-store sweep DISABLED' "$d/stdout.log"; then
    pass "obj-sweep(disabled): interval 0 skips the sweep and ANNOUNCES it (a disabled hygiene probe must be visible, not inferred)"
  else
    fail "obj-sweep(disabled): rc=$rc swept=$([[ -s "$calls" ]] && echo yes || echo no) (see $d)"
  fi
}

test_object_store_sweep_knobs() {
  local d rc page
  # A malformed interval must FAIL CLOSED like its siblings: a bare word would evaluate to
  # 0 in `-le 0` and SILENTLY DISABLE the sweep — the `MAX_HOURS=abc → 0` hazard, one knob
  # over.
  d="$(new_case_dir)"
  common_env "$d"
  export WORKER_CMD="$d/bin/worker.sh"
  write_finalize_stub "$d/bin/worker.sh" "$d/counter"
  export OBJ_SWEEP_INTERVAL_HOURS="abc"
  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  page=$(grep -c 'bad config' "$NOTIFY_LOG" 2>/dev/null || true)
  if [[ "$rc" -eq 2 && ! -f "$d/counter" && "$page" -ge 1 ]]; then
    pass "obj-sweep(knob): a malformed OBJ_SWEEP_INTERVAL_HOURS pages and exits 2 (never a silently-disabled sweep)"
  else
    fail "obj-sweep(knob-interval): rc=$rc page=$page spawned=$([[ -f "$d/counter" ]] && echo yes || echo no)"
  fi
  # ZERO is not a lax bound for the TIMEOUT: the sweep REJECTS 0 as a usage error, so a 0
  # would make every run UNMEASURED forever — a bound that disables the probe rather than
  # loosening it. Strictly positive, and the message has to say so.
  d="$(new_case_dir)"
  common_env "$d"
  export WORKER_CMD="$d/bin/worker.sh"
  write_finalize_stub "$d/bin/worker.sh" "$d/counter"
  export OBJ_SWEEP_TIMEOUT_SECS=0
  bash "$SUPERVISOR" >"$d/stdout.log" 2>&1
  rc=$?
  if [[ "$rc" -eq 2 && ! -f "$d/counter" ]] &&
     grep -q 'OBJ_SWEEP_TIMEOUT_SECS' "$d/stdout.log"; then
    pass "obj-sweep(knob): OBJ_SWEEP_TIMEOUT_SECS=0 fails closed naming the knob (0 would make every sweep UNMEASURED)"
  else
    fail "obj-sweep(knob-timeout): rc=$rc spawned=$([[ -f "$d/counter" ]] && echo yes || echo no) (see $d/stdout.log)"
  fi
}

t test_object_store_sweep_verdicts
t test_object_store_sweep_throttle_and_disable
t test_object_store_sweep_knobs

# THE THROTTLE MUST NOT SUPPRESS A PEER LANE'S CORRUPT (#3749 review, BLOCKER A).
#
# THE DEFECT: the stamp is keyed on the SHARED object store and lives in a box-wide
# directory, and it used to record only a TIMESTAMP — written for every outcome. So the
# lane that DETECTED corruption stopped, and its three peers then saw a fresh stamp,
# skipped their own sweep for the whole 6-hour interval, and kept spawning workers against
# a store known to be damaged: the exact harm the feature exists to prevent, delivered by
# its own throttle.
#
# The two halves are asserted separately, because either alone can pass while the box is
# still unprotected: (1) a CORRUPT run RECORDS the verdict, and (2) a later lane reading
# that record STOPS — without re-sweeping, and while the interval is still fresh.
test_object_store_corrupt_verdict_outlives_the_throttle() {
  local d root counter calls rc stamp
  # (1) A CORRUPT sweep records the VERDICT beside the timestamp.
  d="$(new_case_dir)"; counter="$d/counter"; calls="$d/calls-corrupt"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export OBJ_SWEEP_INTERVAL_HOURS=6
  export OBJ_SWEEP_STAMP="$d/sweep.stamp"
  stamp="$OBJ_SWEEP_STAMP"
  root="$(obj_sweep_tree "$d" CORRUPT 4 "$calls")"
  env LANE_ID=objsweep-test bash "$root/scripts/local/worker-supervisor.sh" >"$d/detect.log" 2>&1
  rc=$?
  if [[ "$rc" -eq 1 ]] && [[ "$(sed -n 2p "$stamp" 2>/dev/null)" == "CORRUPT" ]] &&
     grep -qE '^[0-9]+$' <(sed -n 1p "$stamp" 2>/dev/null); then
    pass "obj-sweep(corrupt-latch): the detecting lane RECORDS 'CORRUPT' in the shared stamp, not just an epoch"
  else
    fail "obj-sweep(corrupt-latch): rc=$rc stamp='$(tr '\n' '/' <"$stamp" 2>/dev/null)' (wanted an epoch then CORRUPT)"
  fi
  # (2) THE PEER LANE. Same box, same stamp, INSIDE the interval — and ONE property
  #     different from the run above: its sweep stub would report VERIFIED. It must still
  #     stop, and it must NOT re-sweep (the stub records its invocations, so this is
  #     measured, not inferred) and must name how to clear the latch.
  calls="$d/calls-peer"
  root="$(obj_sweep_tree "$d" VERIFIED 0 "$calls")"
  env LANE_ID=objsweep-test bash "$root/scripts/local/worker-supervisor.sh" >"$d/peer.log" 2>&1
  rc=$?
  if [[ "$rc" -eq 1 && ! -s "$calls" ]] &&
     grep -q '"reason":"object-store-corrupt"' "$JOURNAL_FILE" &&
     grep -q 'object-store: CORRUPT (cached' "$d/peer.log"; then
    pass "obj-sweep(corrupt-latch): a PEER lane inside the interval STOPS on the cached verdict instead of throttling past it — without re-sweeping"
  else
    fail "obj-sweep(corrupt-latch-peer): rc=$rc reswept=$([[ -s "$calls" ]] && echo yes || echo no) (see $d/peer.log)"
  fi
  if grep -q "rm -f $stamp" "$d/peer.log"; then
    pass "obj-sweep(corrupt-latch): the remedy NAMES the file to remove — a latch nobody can clear bricks the box after the repair"
  else
    fail "obj-sweep(corrupt-latch): the stop gives no way to clear the latch (see $d/peer.log)"
  fi
  # (3) THE CONTROL, one property apart from (2): the SAME fresh stamp carrying VERIFIED.
  #     The lane must proceed and, being inside the interval, must NOT sweep — so (2)'s
  #     stop is attributable to the recorded verdict and not to the stamp's mere presence.
  d="$(new_case_dir)"; counter="$d/counter"; calls="$d/calls-control"
  common_env "$d"
  write_finalize_stub "$d/bin/worker.sh" "$counter"
  export WORKER_CMD="$d/bin/worker.sh"
  export MAX_ISSUES=1
  export OBJ_SWEEP_INTERVAL_HOURS=6
  export OBJ_SWEEP_STAMP="$d/sweep.stamp"
  printf '%s\nVERIFIED\n' "$(date +%s)" >"$OBJ_SWEEP_STAMP"
  root="$(obj_sweep_tree "$d" VERIFIED 0 "$calls")"
  env LANE_ID=objsweep-test bash "$root/scripts/local/worker-supervisor.sh" >"$d/control.log" 2>&1
  rc=$?
  if [[ "$rc" -eq 0 && ! -s "$calls" && -f "$counter" ]]; then
    pass "obj-sweep(corrupt-latch): the SAME stamp carrying VERIFIED throttles normally and the worker runs — the stop above is the recorded verdict, not the file"
  else
    fail "obj-sweep(corrupt-latch-control): rc=$rc swept=$([[ -s "$calls" ]] && echo yes || echo no) spawned=$([[ -f "$counter" ]] && echo yes || echo no) (see $d)"
  fi
}

t test_object_store_corrupt_verdict_outlives_the_throttle

# ---------------------------------------------------------------------------
# Test 48 (#3549, roborev job 196 F2): THE SUITE LEAVES NO FIXTURE PROCESS BEHIND.
#
# This is the assert the leak had no equivalent of: the fixtures were reaped per case, by pid, and
# nothing ever CHECKED, so five-minute orphans accumulated invisibly behind a green summary on a
# four-lane box. Cleanup that is not asserted is a comment.
#
# WHAT IT MEASURES. Every `fixture_bg` registers its pgid as OWNED; this runs the same reap the EXIT trap
# runs and then asks each STILL-OWNED group whether any member is still alive (`kill -0` on a negative
# pid). A group is the right unit: it sees an ORPHANED CHILD whose parent shell is already gone, which is
# exactly the leak — a `sleep 300` inside `bash <script>`.
#
# THE COUNT AND THE SUBJECT SET ARE NOW DIFFERENT THINGS (#3549, roborev job 198 F2). `FIXTURE_OWNED`
# SHRINKS: a group is released the moment it is proven gone, which is what stops a later reap signalling
# a recycled pgid. So the non-vacuity floor is taken from `FIXTURE_STAGED`, the monotone count of groups
# ever staged, and the leak set is what remains OWNED after the reap. A group released as `foreign`
# (existing but unsignallable — the number now belongs to another user) is NOT a leak of ours and is
# reported separately if it ever happens, because calling it a leak would blame this suite for a pid
# number the kernel reassigned.
#
# IT MUST BE THE LAST `t`: a case running after it would register groups nobody checks.
#
# NON-VACUITY IS ASSERTED, NOT ASSUMED, IN BOTH HALVES. (1) An empty registry FAILS: a green "nothing
# leaked" over zero subjects is the vacuous pass this whole file is written against, and the floor is
# well below the count this suite stages so it does not become a maintenance tripwire. (2) The check is
# demonstrably CAPABLE of failing — with the group kill removed from `fixture_reap`, a scratch copy of
# this suite reports `fixture-leak:` and names the surviving groups (recorded here because a
# self-referential in-suite mutant would have to break the very reap it is testing).
#
# SCOPE, STATED: it covers the groups THIS SUITE REGISTERED. The `bash "$SUPERVISOR" &` launches are the
# SUBJECT UNDER TEST rather than fixtures (their lifecycle is what several cases assert) and are not
# registered; nor are the pre-existing `exec`-only fixtures, which replace their shell and so have no
# child to orphan. A whole-machine sweep is not available to it: this box runs sibling lanes staging the
# same names, and `pgrep -f` also self-matches, so a recorded pid is the only identity that is ours.
# ---------------------------------------------------------------------------
test_no_fixture_processes_leak() {
  local n leaked foreign
  n=$FIXTURE_STAGED
  if [[ "$n" -lt 10 ]]; then
    fail "fixture-leak-check-vacuous: only $n fixture process group(s) were staged this run — the check has no subject, so a green here would measure nothing"
    return 0
  fi
  fixture_reap
  leaked="$(fixture_live_groups | tr '\n' ' ')"
  leaked="${leaked% }"
  foreign="$(printf '%s ' ${FIXTURE_FOREIGN[@]+"${FIXTURE_FOREIGN[@]}"})"
  foreign="${foreign% }"
  if [[ -z "$leaked" ]]; then
    pass "fixtures: every one of the $n background fixture process GROUPS staged by this run is gone — nothing orphaned, children included (the reap is by group, so it covers a child whose parent shell already exited)${foreign:+; groups released UNSIGNALLED as recycled: [$foreign]}"
  else
    fail "fixture-leak: still-owned process group(s) [$leaked] have live members after the reap — a fixture child (a non-exec \`sleep\`) has been orphaned; see fixture_bg"
  fi
}

t test_no_fixture_processes_leak

echo "=== $PASS_COUNT passed, $FAIL_COUNT failed, $SKIP_COUNT skipped ==="
[[ "$FAIL_COUNT" -eq 0 ]]
