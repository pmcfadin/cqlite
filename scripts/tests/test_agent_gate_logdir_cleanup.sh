#!/usr/bin/env bash
# Regression test for issue #3637: the gate's per-run LOG_DIR must not leak.
#
# scripts/agent-gate.sh creates one `mktemp -d "$TMPDIR/agent-gate.XXXXXX"` per
# invocation — full, --lite, --delta, --only, and every NESTED gate the self-tests
# spawn — and used to remove NONE of them. Measured 5,697 on one lane box and
# ~61,000 fleet-wide in under three days. The cost is not only inodes: that
# population is what made an `ls -t`-style "find the newest run dir" habit
# routinely land on a PEER LANE's directory — the #3616 near-miss, where a closer
# read 33/37 components PASS out of another PR's run and nearly merged on it.
#
# Properties proven here (the issue's acceptance criteria):
#   AC1  a run ending RESULT: PASS leaves NO agent-gate.<its-run-id> directory;
#   AC2  a run ending RESULT: FAIL DOES leave its directory, and the SUMMARY's
#        `logs:` path still resolves to it;
#   AC3  a NESTED run (AGENT_GATE_PARENT_RUN_ID in env) leaves none on EITHER
#        verdict — with the design-A carve-out: a nested run that publishes its
#        summary INSIDE its own log dir (#2874's private default) RETAINS it, with
#        a NAMED reason, because removing it would delete the verdict block the
#        parent asserts on;
#   AC4  AGENT_GATE_KEEP_LOGS=1 suppresses BOTH the per-run removal and the sweep;
#   AC5  the startup sweep removes an AGED agent-gate.* dir WHOSE OWNER IS PROVABLY
#        GONE and leaves a fresh one (age SYNTHESISED with `touch`, never waited for),
#        never touches a directory that is not the gate's own creation shape, and
#        never touches one whose owner is LIVE or merely UNVERIFIABLE — age is not
#        proof of abandonment (roborev job 70 medium 1);
#   AC15 the sweep's work cap bounds removal ATTEMPTS, not successes — measured with
#        FAILING removals, which is the only way the two accountings differ (roborev
#        job 70 medium 2);
#   AC16 the owner token names the MACHINE-AND-BOOT-AND-PID-NAMESPACE, and every
#        weaker token the earlier form accepted (boot-only, hostname-only, another
#        namespace's) reads cannot-tell — containers share a boot id and a temp dir
#        while having separate pid namespaces (roborev job 111 medium 1);
#
# PLATFORM: the sweep's LIVENESS half is a LINUX-ONLY capability (boot_id,
# /proc/self/ns/pid, /proc/<pid>/stat), so AC5, AC15, AC16 and AC17 are gated on an
# AFFIRMATIVELY PROBED owner-marker capability — measured by planting a marker and
# reading it back through the gate's own probe, never inferred from `uname`. On Linux
# the capability is REQUIRED and its absence is a hard FAIL, so a regression in the
# shipped probe can never present as a green skip. Where it is genuinely absent
# (macOS) each of those cases skips BY NAME and asserts the SAFE DEGRADATION in its
# place: with no establishable owner token every candidate reads cannot-tell, the
# sweep attempts nothing and removes nothing (roborev job 114).
#   AC17 successive capped sweeps examine DIFFERENT, PREDICTED windows, so no
#        position in find's order is STRUCTURALLY privileged: measured with more
#        candidates than the cap and a deterministic failing prefix, the configuration
#        in which a fixed start position starves the tail forever (roborev job 111
#        medium 2). Every sweep is given an explicit simulated run-id and must examine
#        EXACTLY the window the SHIPPED offset function predicts for it — the earlier
#        form asserted properties of a $RANDOM sample, i.e. it could fail by chance
#        inside a registered tooling-tests case (roborev job 117 medium). WHAT IT DOES
#        NOT ESTABLISH, declared at the assertions themselves rather than left to the
#        doctrine: eventual or complete coverage of the population. The offset derives
#        from the run-id, which is a random mktemp suffix, so distinct run-ids CAN map
#        to one offset and a deferred candidate CAN be starved indefinitely — accepted
#        and declared, not tested away. The enumerated sequence below demonstrates one
#        covering run over five candidates and is evidence about THAT SEQUENCE only
#        (roborev job 118 medium);
#   AC18 the cap bounds the candidates EXAMINED, not the removals attempted, measured
#        over an ALL-MARKERLESS population where NOTHING is removable — the shape of
#        every pre-#3637 directory, and the only configuration in which the two
#        accountings differ (roborev job 116 medium). Successive run-ids must examine
#        exactly their PREDICTED, and therefore different, subsets, and every
#        markerless candidate must SURVIVE;
#   AC6  a run that KEEPS its directory is otherwise unchanged: summary-integrity,
#        tree-integrity and the `logs:` PATH FIELD are byte-identical, and the
#        summary-integrity no-clobber path (#2874) still retains its bundle. A
#        non-zero exit that never reached a terminal verdict keeps its bundle when
#        it holds diagnostic content, and leaves no husk when it does not;
#   AC7  `logs:` is a PATH-ONLY field and the disposition rides its OWN
#        `logdir-disposition:` key — proven against a $TMPDIR that itself contains
#        " (", the exact value the withdrawn shared-channel design truncated (#3312);
#   AC9  the SUMMARY's `logs:` and the heartbeat file's `logs:` are byte-identical:
#        ONE field name must not carry two grammars (STRUCTURAL — see the case);
#   AC21 a terminal PASS does NOT destroy the `file-size` OPT-OUT disclosure: the
#        component's own arm PINS the retention (its `OPT-OUT` token is NON-FAILING, so
#        `CQLITE_ALLOW_FILE_GROWTH=1` reaches `RESULT: PASS`, and #3402/#3401 put the
#        grown-file NAMES in `file-size.log` INSIDE the bundle) — roborev job 173 F1;
#   AC22 `--only <component>` RETAINS, by a reason that NAMES the mode: the entire product
#        of that diagnostic is the component log under `logs:`. `--lite` is deliberately
#        NOT exempted and its disposition states the removal AND the KEEP_LOGS remedy —
#        roborev job 173 F2;
#   AC23 ONE content predicate serves BOTH early-exit arms: a non-zero exit whose bundle
#        holds only this run's LAUNCH artifacts (incl. the #3755 admission family) leaves
#        no husk, and the same bundle plus one component log IS retained — job 173 F3;
#   AC24 the two NEW keys carry no $TMPDIR-derived control characters into the block: a
#        newline-bearing `$TMPDIR` adds only the 2 pre-existing DECLARED lines (`run-id:`,
#        `logs:`), and a value carrying the probe's reserved token is WITHHELD with its key
#        intact — roborev job 173 F4;
#   AC25 the opt-out's DISCLOSURE states the OBSERVED value: engagement stays LENIENT
#        (any set, non-empty, non-`0` value retains — this branch KEEPS data, so a typo
#        must not destroy the bundle), the three emitted strings render what was
#        actually set rather than a hard-coded `=1`, a set-but-not-`1` value is
#        ANNOUNCED as unconventional-but-honoured, set-but-EMPTY does NOT engage, and a
#        hostile value rides the same `_summary_block_value` boundary as AC24's keys —
#        roborev job 174 F1;
#   AC26 STRUCTURAL over the shipped script: the `cli-tests` comment no longer claims
#        `$LOG_DIR` "is retained deliberately", which a terminal PASS makes false, and
#        the reworded rationale names the #3637 disposition alongside the `_cli_tmp`
#        cleanup it explains — roborev job 174 F2;
#   AC8  an EARLY EXIT — one that never reaches the terminal emit — still gets a
#        disposition: the CQLITE_GATE_STUB_RUNDIR stub (exit 0) and an argv/usage
#        refusal (exit 2, empty bundle) both leave NOTHING, and `--list` creates no
#        directory at all. This is the roborev job-54 finding: _logdir_decide had ONE
#        call site, so every path that exited before it leaked.
#
# Hermetic: every case runs a gate in an ISOLATED fake checkout with its own
# scratch TMPDIR, through the no-cargo `--lite-aggregate-selftest` /
# `--emit-summary-selftest` / AGENT_GATE_INTEGRITY_SELFTEST paths. Nothing here
# reads or writes the real /tmp population, and the final case asserts this file's
# own runs leave the scratch TMPDIR empty.
#
# Run standalone:   bash scripts/tests/test_agent_gate_logdir_cleanup.sh
# Or via the gate:  scripts/agent-gate.sh runs it inside the `tooling-tests` component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"
# Scrub any inherited summary path (a standalone run must never clobber a caller's
# file) and disable the machine slot cap so nested gates never queue.
unset AGENT_GATE_SUMMARY_FILE
unset AGENT_GATE_KEEP_LOGS
export CQLITE_GATE_DISABLE_CAP=1

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

# FAIL-CLOSED before anything is derived from it. An unchecked `mktemp -d` leaves
# `tmp` EMPTY, after which every child path below becomes root-level — `/fakeroot`,
# `/td-pass`, `/paren (dir)/td` — and the cleanup trap `rm -rf ""` reclaims none of
# them; under a privileged run those are persistent files outside any scratch root.
# Same rule the gate's own `_logdir_rm_guarded` follows: never derive a path from an
# unchecked value. The check precedes the trap because the trap itself is one such
# derivation.
tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-logdir.XXXXXX") || tmp=""
if [ -z "$tmp" ] || [ ! -d "$tmp" ]; then
  printf 'FAIL - could not create a scratch dir under %s — refusing to run\n' "${TMPDIR:-/tmp}"
  exit 1
fi
trap 'rm -rf "$tmp"' EXIT INT TERM

# Fixture git must be ISOLATED from the invoker's environment, not merely given an
# identity: `git init`/`git commit` also read global/system `commit.gpgsign`,
# `core.hooksPath` and `init.templateDir`, any one of which turns the committed
# fixture below into a hard failure on someone's box — and this file is a registered
# `tooling-tests` case, so that failure would read as a log-cleanup regression.
# GIT_CONFIG_GLOBAL/SYSTEM=/dev/null kills all three vectors at once (the convention
# scripts/tests/test_agent_gate_file_size_log.sh and lib/perf-capability-test-lib.sh
# already use); GIT_CFG below is the belt-and-braces half and pins the branch name.
export GIT_CONFIG_GLOBAL=/dev/null
export GIT_CONFIG_SYSTEM=/dev/null
GIT_CFG=(-c user.email=gate@example.invalid -c user.name=gate-selftest
         -c init.defaultBranch=main -c commit.gpgsign=false -c core.hooksPath=/dev/null)


# Isolated fake checkout: only the gate script is needed by the hermetic modes below
# (they exit before the component-set pre-flight and before any component runs), and
# REPO_ROOT then resolves into $tmp so no real repo artifact is ever written.
fakeroot="$tmp/fakeroot"
mkdir -p "$fakeroot/scripts"
cp "$GATE" "$fakeroot/scripts/agent-gate.sh"
FAKE_GATE="$fakeroot/scripts/agent-gate.sh"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# logs_field <summary-file> -> the `logs:` line's value, VERBATIM.
#
# No stripping, and that is the property under test (#3637 finding 2): `logs:` is a
# PATH-ONLY field. A first draft appended the disposition to it and told consumers to
# cut at the first " (" — but $TMPDIR is environment-controlled and may contain " (",
# so `/tmp/build (scratch)/agent-gate.ABC123` truncated to `/tmp/build`. Reading it
# verbatim here means a re-introduced clause makes the [ -d "$d" ] assertions FAIL
# rather than being silently papered over by the test's own parser.
logs_field() {
  local line
  line=$(sed -n 's/^logs: //p' "$1" 2>/dev/null | tail -1)
  [ -n "$line" ] || return 1
  printf '%s' "$line"
}

# logs_disposition <summary-file> -> the value of the `logdir-disposition:` key.
# Its OWN line, so there is no delimiter to get wrong.
logs_disposition() {
  sed -n 's/^logdir-disposition: //p' "$1" 2>/dev/null | tail -1
}

# run_agg <scratch-tmpdir> <summary-file> <scoped-status> [env=value ...]
# Drive the hermetic --lite-aggregate-selftest path: a real terminal emit through
# _emit_terminal_summary/emit_summary with NO cargo, NO git and NO components.
# <scoped-status> PASS -> RESULT: PASS, FAIL -> RESULT: FAIL.
run_agg() {
  local td="$1" sf="$2" scoped="$3"; shift 3
  mkdir -p "$td"
  env -u AGENT_GATE_PARENT_RUN_ID \
      TMPDIR="$td" \
      AGENT_GATE_SUMMARY_FILE="$sf" \
      AGENT_GATE_TEST_LITE_SCOPED="$scoped" \
      AGENT_GATE_TEST_LITE_RESULTS="file-size:PASS fmt:PASS clippy:PASS" \
      "$@" \
      bash "$FAKE_GATE" --lite-aggregate-selftest >"$td.out" 2>&1
}

# count_logdirs <dir> -> number of agent-gate.* directories directly under <dir>
count_logdirs() {
  find "$1" -maxdepth 1 -type d -name 'agent-gate.*' 2>/dev/null | grep -c . || true
}

# one_logdir <dir> -> the FIRST agent-gate.* directory directly under <dir>.
# THREE-VALUED (#1699's find-tristate rule): rc 0 + the path when the scan ran and
# matched, rc 1 when it ran and matched nothing, rc 2 when the scan itself could not
# be trusted. A caller that cannot tell those apart reports a missing bundle for an
# unreadable directory.
one_logdir() {
  local listing rc
  listing=$(find "$1" -maxdepth 1 -type d -name 'agent-gate.*' 2>/dev/null); rc=$?
  [ "$rc" -eq 0 ] || return 2
  [ -n "$listing" ] || return 1
  printf '%s' "$listing" | head -1
}

# artifact_field <bundle-dir> <key> -> the value of <key> in the bundle's own
# `logdir-disposition.txt`. Read VERBATIM, exactly as logs_field() is: the artifact
# is keyed, one value per line, and a test that trims would hide a re-appended clause.
artifact_field() {
  sed -n "s/^$2: //p" "$1/logdir-disposition.txt" 2>/dev/null | tail -1
}

# block_lines <summary-file> -> the number of lines in that file's SUMMARY block, both
# markers included. Written for AC24, where a keyed line MUST stay ONE line: an injected
# newline is invisible to every per-field assertion (each key still parses) and shows up
# only in the block's own line count.
#
# The LITE markers, because every counting case here drives the hermetic
# `--lite-aggregate-selftest`. A file with no block yields 0, which every caller treats as
# a measurement failure rather than as a small block.
block_lines() {
  awk '/^==== AGENT-GATE LITE SUMMARY ====$/,/^==== END AGENT-GATE LITE SUMMARY ====$/' \
    "$1" 2>/dev/null | grep -c . || true
}

# run_agg_gate <gate-script> <scratch-tmpdir> <summary-file> <scoped-status> [env=value ...]
# -- run_agg against an ARBITRARY gate script, so a MUTANT copy can be driven through the
# identical fixture. A mutant can then differ from the real run by nothing but the mutation.
run_agg_gate() {
  local gate="$1" td="$2" sf="$3" scoped="$4"; shift 4
  mkdir -p "$td"
  env -u AGENT_GATE_PARENT_RUN_ID \
      TMPDIR="$td" \
      AGENT_GATE_SUMMARY_FILE="$sf" \
      AGENT_GATE_TEST_LITE_SCOPED="$scoped" \
      AGENT_GATE_TEST_LITE_RESULTS="file-size:PASS fmt:PASS clippy:PASS" \
      "$@" \
      bash "$gate" --lite-aggregate-selftest >"$td.out" 2>&1
}

# case_mark / case_floor <label> <min> — the PER-CASE FLOOR idiom this file already applies
# to its four driver-based cases (AC17/AC18/AC19/AC20), available to the INLINE cases too.
# Same lesson (#3544): a case that dies partway through would otherwise report a green
# SUBSET of itself, and the counters are the only thing that can see it. `case_mark` at the
# top of a case, `case_floor` at the bottom; the floor is itself a verdict, so a deleted
# floor is visible in the suite-wide floor at the end of this file.
CASE_MARK=0
case_mark() { CASE_MARK=$((PASS + FAIL)); }
case_floor() {
  local n=$((PASS + FAIL - CASE_MARK))
  if [ "$n" -ge "$2" ]; then
    ok "$1: the case reported all $n of its verdicts (case floor $2)"
  else
    bad "$1: the case reported only $n verdicts (case floor $2) — a truncated case, not a pass"
  fi
}

# _age_dir_apply <dir> — THE ONE mtime-synthesis mechanism in this file (#3637, roborev job
# 175 finding 3). rc 0 iff <dir> now reads as older than the sweep's 7-day floor. It prints
# NOTHING, so the same definition serves this file's `ok`/`bad` protocol AND the four
# driver scripts, each of which reports through its own `say OK|BAD` channel (it is
# `export -f`d for them, the same idiom `plant_owner_marker` already uses).
#
# WHY ONE HELPER AND NOT A LINE PER SITE: three fixtures — AC4's `aged_keep`, AC5's
# `notours` and `foreign` — synthesised their age with a bare
# `touch -d … 2>/dev/null || touch -t … 2>/dev/null` in which BOTH forms were allowed to
# fail SILENTLY. All three assert "must SURVIVE the sweep", so a fixture that was never
# aged survives TRIVIALLY and the case reports a pass having measured nothing. That is the
# vacuous green this file's own `plant_owner_marker` self-verification exists to prevent,
# one fixture over — and the per-site form means the NEXT fixture has to remember. One
# boundary cannot be forgotten.
#
# SELF-VERIFYING, not merely error-checked, for the same reason `plant_owner_marker` reads
# the state back through the gate's own probe: `touch` can SUCCEED while setting a time the
# sweep does not consider aged (a `touch -t` clamped by a filesystem with a narrow time
# range, a timestamp the kernel truncates). The verdict that decides whether the fixture is
# in the intended state is the SWEEP's, so the check is the sweep's own predicate —
# `find -mtime +${GATE_LOGDIR_SWEEP_AGE_DAYS}`, whose shipped floor is 7 days.
_age_dir_apply() {
  local d="${1:-}"
  [ -n "$d" ] && [ -e "$d" ] || return 1
  touch -d '30 days ago' "$d" 2>/dev/null || touch -t 202001010000 "$d" 2>/dev/null || return 1
  [ -n "$(find "$d" -maxdepth 0 -mtime +7 2>/dev/null)" ]
}
export -f _age_dir_apply

# age_dir <dir> <case-label> — `_age_dir_apply` plus THIS suite's reporting: a failure is a
# named `bad`, never a silent continue. Every main-scope aged fixture goes through it.
age_dir() {
  _age_dir_apply "$1" && return 0
  bad "${2:-mtime synthesis}: could not synthesise an aged mtime for '$1' — neither touch -d nor touch -t produced a time the sweep's own find -mtime +7 accepts, so the fixture is NOT aged and every assertion about it below would hold trivially"
  return 1
}

# The owner-marker machinery, EXTRACTED FROM THE SHIPPED GATE rather than
# reimplemented (the repo idiom — see test_agent_gate_jest_guards.sh and
# test_agent_gate_feature_matrix_annotation.sh). A second implementation of the
# machine token and the pid-start token would be a second place for them to diverge,
# and a fixture that plants a marker the gate would not recognise measures nothing.
owner_lib="$tmp/owner-lib.sh"
awk '/^GATE_LOGDIR_OWNER_BASENAME=/,/^# _logdir_sweep:/' "$GATE" | sed '$d' >"$owner_lib"
if grep -q '^_logdir_write_owner() {' "$owner_lib" \
   && grep -q '^_logdir_owner_state() {' "$owner_lib" \
   && grep -q "^GATE_LOGDIR_OWNER_BASENAME=" "$owner_lib"; then
  ok "AC5/AC15: the owner-marker helpers were extracted from the shipped gate"
else
  bad "AC5/AC15: could not extract the owner-marker helpers from the shipped gate — every liveness case below would measure nothing"
fi
# shellcheck disable=SC1090
. "$owner_lib"

# plant_owner_marker <dir> live|dead -> rc 0 iff the planted marker really READS as
# the requested state through the gate's own probe.
#
# SELF-VERIFYING on purpose: a fixture whose marker the gate classifies differently
# from what the case intends would make the assertion below pass or fail for a reason
# that has nothing to do with the sweep.
plant_owner_marker() {
  local d="$1" want="$2" expect victim
  case "$want" in
    dead) expect="verified-dead" ;;
    live) expect="live" ;;
    *) return 1 ;;
  esac
  GATE_LOGDIR_CREATED="$d"
  GATE_LOGDIR_OWNER_FILE="$d/$GATE_LOGDIR_OWNER_BASENAME"
  _logdir_write_owner
  [ -s "$GATE_LOGDIR_OWNER_FILE" ] || return 1
  if [ "$want" = dead ]; then
    # A REAPED pid. Every other field stays this machine's own, so the probe's
    # machine-and-boot check passes and the pid is what answers. Robust to the pid
    # being handed out again before the sweep runs: the recorded start token is this
    # test process's, so a reusing process mismatches it and still reads dead.
    sleep 0 & victim=$!
    wait "$victim" 2>/dev/null
    sed -i.bak "s/^pid=.*/pid=$victim/" "$GATE_LOGDIR_OWNER_FILE" || return 1
    rm -f "$GATE_LOGDIR_OWNER_FILE.bak"
  fi
  _logdir_owner_state "$d"
  [ "$GATE_LOGDIR_OWNER_STATE" = "$expect" ]
}

# ---------------------------------------------------------------------------
# The owner-marker CAPABILITY, probed AFFIRMATIVELY (never inferred from `uname`).
# ---------------------------------------------------------------------------
# THE LIVENESS HALF OF THE SWEEP IS A LINUX-ONLY CAPABILITY, and that is a declared
# platform residual rather than a defect. The owner token is
# `boot=<uuid>;pidns=<inode>`, read from /proc/sys/kernel/random/boot_id and
# /proc/self/ns/pid, and the pid-start token comes from /proc/<pid>/stat. A host that
# publishes none of those — macOS is the live instance, and docs/development/gate-ops.md
# carries Darwin-specific gate contracts, so this registered `tooling-tests` case really
# does run there — yields the EMPTY machine token BY DESIGN, so every candidate reads
# `cannot-tell` and the sweep removes NOTHING. That is the fail-safe direction the whole
# probe is built to take.
#
# So the three cases that need a PLANTED marker (AC5, AC15, AC17) are gated on this
# capability, and the capability is measured BY DOING THE THING THEY DEPEND ON: plant a
# verified-dead marker and a live one and require the gate's OWN probe to read both back.
# A `uname` test would assert a proxy for the dependency; this asserts the dependency.
#
# ON LINUX THE CAPABILITY IS REQUIRED, NOT OPTIONAL. The verdict below is a `bad` — a
# hard FAIL — when a Linux host cannot plant a marker, because a capability guard that
# also let the Linux cases skip would silently delete the coverage those cases exist to
# pin: a regression in the shipped probe would then read as a green skip.
owner_probe_dir="$tmp/owner-capability"
mkdir -p "$owner_probe_dir/reaped" "$owner_probe_dir/running"
if plant_owner_marker "$owner_probe_dir/reaped" dead \
   && plant_owner_marker "$owner_probe_dir/running" live; then
  OWNER_MARKER_CAPABLE=1
else
  OWNER_MARKER_CAPABLE=0
fi
rm -rf "$owner_probe_dir"
# `uname` decides only HOW STRICT the verdict is, never whether the capability exists.
host_kind=$(uname -s 2>/dev/null) || host_kind=""
[ -n "$host_kind" ] || host_kind="unknown"
if [ "$OWNER_MARKER_CAPABLE" = 1 ]; then
  ok "capability: owner markers plant AND read back through the gate's own probe on this host ($host_kind) — AC5/AC15/AC17 are EXERCISED below"
elif [ "$host_kind" = Linux ]; then
  bad "capability: owner markers do NOT plant on a LINUX host ($host_kind) — the liveness gate's inputs (/proc/sys/kernel/random/boot_id, /proc/self/ns/pid, /proc/<pid>/stat) all exist here, so this is a REGRESSION in the shipped probe and not a platform gap; AC5/AC15/AC17's liveness coverage must never skip on Linux"
else
  ok "capability: DECLARED ABSENT on this non-Linux host ($host_kind) — no boot id and/or no /proc pid namespace, so the owner token is EMPTY, every candidate reads cannot-tell and the sweep can remove NOTHING; AC5/AC15/AC17 skip by name and each asserts that SAFE DEGRADATION positively in place of the planted-marker fixture"
fi

# ---------------------------------------------------------------------------
# AC1: a PASS run removes its own log dir.
# ---------------------------------------------------------------------------
td1="$tmp/td-pass"; sf1="$tmp/pass-summary.txt"
run_agg "$td1" "$sf1" PASS
if grep -q '^RESULT: PASS' "$sf1" 2>/dev/null; then
  ok "AC1: the hermetic aggregate run really ended RESULT: PASS (precondition)"
else
  bad "AC1: precondition failed — the aggregate run did not end PASS; cannot measure removal"
  sed -n '1,40p' "$td1.out"
fi
d1=$(logs_field "$sf1") || d1=""
if [ -n "$d1" ]; then
  ok "AC1: the PASS block still publishes a parseable 'logs:' path field ($d1)"
else
  bad "AC1: the PASS block published no parseable 'logs:' path field"
fi
if [ -n "$d1" ] && [ ! -d "$d1" ]; then
  ok "AC1: a RESULT: PASS run left NO log directory behind"
else
  bad "AC1: a RESULT: PASS run LEFT its log directory behind ($d1)"
fi
if [ "$(count_logdirs "$td1")" = 0 ]; then
  ok "AC1: the run's whole scratch TMPDIR holds no agent-gate.* directory"
else
  bad "AC1: the run's scratch TMPDIR still holds $(count_logdirs "$td1") agent-gate.* dir(s)"
fi
disp1=$(logs_disposition "$sf1")
case "$disp1" in
  REMOVED*) ok "AC1/B: the logdir-disposition: line DECLARES the removal ($disp1)" ;;
  *) bad "AC1/B: the logdir-disposition: line does not declare a removal (got: '${disp1:-<none>}')" ;;
esac
case "$disp1" in
  *AGENT_GATE_KEEP_LOGS=1*) ok "AC1/B: the removal disposition names the opt-out" ;;
  *) bad "AC1/B: the removal disposition does not name AGENT_GATE_KEEP_LOGS=1" ;;
esac

# ---------------------------------------------------------------------------
# AC2: a FAIL run keeps its log dir, and the logs: path still resolves.
# ---------------------------------------------------------------------------
td2="$tmp/td-fail"; sf2="$tmp/fail-summary.txt"
run_agg "$td2" "$sf2" FAIL
if grep -q '^RESULT: FAIL' "$sf2" 2>/dev/null; then
  ok "AC2: the hermetic aggregate run really ended RESULT: FAIL (precondition)"
else
  bad "AC2: precondition failed — the aggregate run did not end FAIL; cannot measure retention"
  sed -n '1,40p' "$td2.out"
fi
d2=$(logs_field "$sf2") || d2=""
if [ -n "$d2" ] && [ -d "$d2" ]; then
  ok "AC2: a RESULT: FAIL run KEPT its log directory and the logs: path resolves ($d2)"
else
  bad "AC2: a RESULT: FAIL run did not leave a resolvable log directory ('${d2:-<none>}')"
fi
if [ -n "$d2" ] && [ -f "$d2/summary.txt" ]; then
  ok "AC2: the retained bundle still holds the archival summary copy"
else
  bad "AC2: the retained bundle lost its archival summary copy"
fi
disp2=$(logs_disposition "$sf2")
case "$disp2" in
  RETAINED*FAIL*) ok "AC2/B: the logdir-disposition: line names the retention reason ($disp2)" ;;
  *) bad "AC2/B: the logdir-disposition: line does not name a FAIL retention reason (got: '${disp2:-<none>}')" ;;
esac

# ---------------------------------------------------------------------------
# AC3: nested runs (AGENT_GATE_PARENT_RUN_ID set) leave nothing on either verdict.
# ---------------------------------------------------------------------------
for verdict in PASS FAIL; do
  tdn="$tmp/td-nested-$verdict"; sfn="$tmp/nested-$verdict.txt"
  mkdir -p "$tdn"
  env TMPDIR="$tdn" \
      AGENT_GATE_PARENT_RUN_ID="/tmp/agent-gate.PARENTFAKE" \
      AGENT_GATE_SUMMARY_FILE="$sfn" \
      AGENT_GATE_TEST_LITE_SCOPED="$verdict" \
      AGENT_GATE_TEST_LITE_RESULTS="file-size:PASS fmt:PASS clippy:PASS" \
      bash "$FAKE_GATE" --lite-aggregate-selftest >"$tdn.out" 2>&1
  if ! grep -q "^RESULT: $verdict" "$sfn" 2>/dev/null; then
    bad "AC3: precondition failed — nested $verdict run did not end RESULT: $verdict"
  fi
  if ! grep -q '^nested-under: ' "$sfn" 2>/dev/null; then
    bad "AC3: precondition failed — nested $verdict run did not stamp nested-under:"
  fi
  dn=$(logs_field "$sfn") || dn=""
  if [ -n "$dn" ] && [ ! -d "$dn" ]; then
    ok "AC3: a NESTED run ending $verdict left NO log directory"
  else
    bad "AC3: a NESTED run ending $verdict LEFT its log directory ('${dn:-<none>}')"
  fi
  if [ "$(count_logdirs "$tdn")" = 0 ]; then
    ok "AC3: the nested $verdict run's scratch TMPDIR is empty of agent-gate.* dirs"
  else
    bad "AC3: the nested $verdict run's scratch TMPDIR still holds $(count_logdirs "$tdn") dir(s)"
  fi
done

# AC3 / design-A carve-out: a nested run with NO explicit AGENT_GATE_SUMMARY_FILE
# publishes its summary to $LOG_DIR/summary-primary.txt (#2874). Removing that
# directory would delete the very verdict block the parent asserts on, so the run
# must RETAIN it and SAY SO. This is a fail-safe, never a silent exception.
tdc="$tmp/td-nested-inside"; mkdir -p "$tdc"
env -u AGENT_GATE_SUMMARY_FILE \
    TMPDIR="$tdc" \
    AGENT_GATE_PARENT_RUN_ID="/tmp/agent-gate.PARENTFAKE" \
    AGENT_GATE_TEST_LITE_SCOPED=PASS \
    AGENT_GATE_TEST_LITE_RESULTS="file-size:PASS fmt:PASS clippy:PASS" \
    bash "$FAKE_GATE" --lite-aggregate-selftest >"$tdc.out" 2>&1
inside_summary=$(sed -n 's/^summary-file:[[:space:]]*//p' "$tdc.out" | tail -1)
case "$inside_summary" in
  */agent-gate.*/summary-primary.txt)
    ok "AC3/A: precondition — the private nested run published inside its own log dir" ;;
  *)
    bad "AC3/A: precondition failed — nested private summary path was '${inside_summary:-<none>}'" ;;
esac
if [ -n "$inside_summary" ] && [ -f "$inside_summary" ]; then
  ok "AC3/A: the parent-readable summary-primary.txt SURVIVED (not deleted with the dir)"
else
  bad "AC3/A: summary-primary.txt was DELETED — the parent's verdict block is gone"
fi
dispc=$(logs_disposition "$tdc.out")
case "$dispc" in
  RETAINED*summary-inside-logdir*)
    ok "AC3/A: the retention is NAMED on the logdir-disposition: line ($dispc)" ;;
  *)
    bad "AC3/A: the summary-inside-logdir retention is not named (got: '${dispc:-<none>}')" ;;
esac

# AC3 / design-A carve-out under a RELATIVE $TMPDIR (#3637, roborev job 67 finding 1).
# The containment check above is only as good as its two operands being in the SAME
# FORM. A relative TMPDIR gave a relative LOG_DIR (and so a relative
# GATE_LOGDIR_CREATED), while the nested private summary path was later absolutised —
# so `case "$SUMMARY_FILE" in "$GATE_LOGDIR_CREATED"/*)` compared a relative prefix
# against an absolute path, concluded the summary was NOT inside the directory, and
# the nested terminal run DELETED ITS OWN PARENT-READABLE VERDICT. That is the exact
# failure AC6 forbids, inside the carve-out that exists to prevent it.
#
# The run's cwd is $fakeroot — the same directory the gate cd's into — so the two
# paths agree on their BASE and the only variable left is the FORM. RED against the
# unfixed script: summary-primary.txt is gone.
reltd="reltd-nested"; mkdir -p "$fakeroot/$reltd"
( cd "$fakeroot" && env -u AGENT_GATE_SUMMARY_FILE \
    TMPDIR="$reltd" \
    AGENT_GATE_PARENT_RUN_ID="/tmp/agent-gate.PARENTFAKE" \
    AGENT_GATE_TEST_LITE_SCOPED=PASS \
    AGENT_GATE_TEST_LITE_RESULTS="file-size:PASS fmt:PASS clippy:PASS" \
    bash "$FAKE_GATE" --lite-aggregate-selftest ) >"$tmp/rel-nested.out" 2>&1
rel_summary=$(sed -n 's/^summary-file:[[:space:]]*//p' "$tmp/rel-nested.out" | tail -1)
case "$rel_summary" in
  /*/agent-gate.*/summary-primary.txt)
    ok "AC3/C: precondition — the relative-TMPDIR nested run published an ABSOLUTE private summary path inside its own log dir" ;;
  *)
    bad "AC3/C: precondition failed — nested private summary path under a relative TMPDIR was '${rel_summary:-<none>}'" ;;
esac
if [ -n "$rel_summary" ] && [ -f "$rel_summary" ]; then
  ok "AC3/C: under a RELATIVE \$TMPDIR the parent-readable summary-primary.txt SURVIVED"
else
  bad "AC3/C: under a RELATIVE \$TMPDIR summary-primary.txt was DELETED — the containment check compared a relative log dir against an absolute summary path"
fi
disp_rel=$(logs_disposition "$tmp/rel-nested.out")
case "$disp_rel" in
  RETAINED*summary-inside-logdir*)
    ok "AC3/C: the relative-TMPDIR retention is NAMED on the logdir-disposition: line ($disp_rel)" ;;
  *)
    bad "AC3/C: the relative-TMPDIR run did not name a summary-inside-logdir retention (got: '${disp_rel:-<none>}')" ;;
esac
# The `logs:` field must be usable by a reader with a different cwd, which is the
# other half of the same normalisation: a relative path there is a path only the
# gate's own process could resolve.
logs_rel=$(logs_field "$tmp/rel-nested.out") || logs_rel=""
case "$logs_rel" in
  /*) ok "AC3/C: the logs: field is an ABSOLUTE path even under a relative \$TMPDIR ($logs_rel)" ;;
  *)  bad "AC3/C: the logs: field is not absolute under a relative \$TMPDIR (got: '${logs_rel:-<none>}') — unresolvable for any reader with another cwd" ;;
esac

# ---------------------------------------------------------------------------
# AC4: AGENT_GATE_KEEP_LOGS=1 suppresses the per-run removal AND the sweep.
# ---------------------------------------------------------------------------
td4="$tmp/td-keep"; sf4="$tmp/keep-summary.txt"
mkdir -p "$td4"
aged_keep="$td4/agent-gate.AGEDKP"
mkdir -p "$aged_keep"
age_dir "$aged_keep" AC4
run_agg "$td4" "$sf4" PASS AGENT_GATE_KEEP_LOGS=1
d4=$(logs_field "$sf4") || d4=""
if [ -n "$d4" ] && [ -d "$d4" ]; then
  ok "AC4: AGENT_GATE_KEEP_LOGS=1 kept a PASS run's own log directory"
else
  bad "AC4: AGENT_GATE_KEEP_LOGS=1 did NOT keep a PASS run's log directory ('${d4:-<none>}')"
fi
disp4=$(logs_disposition "$sf4")
case "$disp4" in
  RETAINED*AGENT_GATE_KEEP_LOGS=1*) ok "AC4: the retention names the opt-out ($disp4)" ;;
  *) bad "AC4: the KEEP_LOGS retention is not named on the logdir-disposition: line (got: '${disp4:-<none>}')" ;;
esac
if [ -d "$aged_keep" ]; then
  ok "AC4: AGENT_GATE_KEEP_LOGS=1 also suppressed the startup age sweep"
else
  bad "AC4: the startup sweep ran under AGENT_GATE_KEEP_LOGS=1 and removed an aged dir"
fi
if grep -q '^logdir-sweep: SKIPPED (AGENT_GATE_KEEP_LOGS=1)' "$sf4"; then
  ok "AC4: the SUMMARY reports the sweep as SKIPPED under the opt-out"
else
  bad "AC4: the SUMMARY does not report a SKIPPED sweep under the opt-out"
fi

# ---------------------------------------------------------------------------
# AC5: the startup sweep. Age is SYNTHESISED with `touch`; nothing waits.
# ---------------------------------------------------------------------------
td5="$tmp/td-sweep"; sf5="$tmp/sweep-summary.txt"
mkdir -p "$td5"
aged="$td5/agent-gate.AGED01"
fresh="$td5/agent-gate.FRESH1"
aged_live="$td5/agent-gate.AGEDLV"          # aged, owner LIVE: must survive
aged_unk="$td5/agent-gate.AGEDNM"           # aged, NO marker: unverifiable, must survive
notours="$td5/agent-gate-notadir-shape"     # wrong basename shape: must survive
foreign="$td5/some-other-tool.XXXXXX"       # not ours at all: must survive
mkdir -p "$aged" "$fresh" "$aged_live" "$aged_unk" "$notours" "$foreign"
printf 'evidence\n' > "$aged/marker"
# The sweep needs TWO independent conditions, so where the owner-marker capability is
# present the fixture supplies both: an aged mtime AND an owner marker naming a process
# that is provably gone. Where it is ABSENT (no /proc, i.e. no establishable owner
# token) there is no such marker to plant on any host, so the case measures the SAFE
# DEGRADATION instead — see the else arm.
ac5_degraded_survivor=""
if [ "$OWNER_MARKER_CAPABLE" = 1 ]; then
  plant_owner_marker "$aged" dead \
    && ok "AC5: precondition — the aged directory's planted marker reads verified-dead through the gate's own probe" \
    || bad "AC5: precondition failed — could not plant a verified-dead owner marker; the removal case below measures nothing"
  plant_owner_marker "$aged_live" live \
    && ok "AC5: precondition — the live-owner decoy's marker reads live through the gate's own probe" \
    || bad "AC5: precondition failed — could not plant a live owner marker; the live-decoy case below measures nothing"
else
  # SAFE DEGRADATION, ASSERTED POSITIVELY rather than left as an untested hole. The
  # marker written is the one the SHIPPED writer produces on this host (its `machine=`
  # field is `unknown`, recorded rather than omitted so it can never match by
  # omission), so the degradation is measured through the real artifact.
  GATE_LOGDIR_CREATED="$aged"
  GATE_LOGDIR_OWNER_FILE="$aged/$GATE_LOGDIR_OWNER_BASENAME"
  _logdir_write_owner
  GATE_LOGDIR_CREATED="$aged_live"
  GATE_LOGDIR_OWNER_FILE="$aged_live/$GATE_LOGDIR_OWNER_BASENAME"
  _logdir_write_owner
  _logdir_owner_state "$aged"
  if [ "$GATE_LOGDIR_OWNER_STATE" = cannot-tell ]; then
    ok "AC5: DEGRADED — with no establishable owner token this host's own shipped marker reads cannot-tell, so no candidate can ever reach verified-dead"
  else
    bad "AC5: DEGRADED — a marker written where the owner token is unestablishable reads '$GATE_LOGDIR_OWNER_STATE', not cannot-tell; the sweep could remove a bundle whose owner was never verified"
  fi
  ac5_degraded_survivor="$aged"
fi
for aged_path in "$aged" "$aged_live" "$aged_unk"; do
  age_dir "$aged_path" AC5
done
# THE SAME BOUNDARY for the two must-SURVIVE decoys (#3637, roborev job 175 finding 3):
# their age used to be synthesised by a bare `touch … || touch …` with both forms
# silenced, and "the sweep left it alone" is exactly the assertion an un-aged fixture
# satisfies for free.
age_dir "$notours" AC5
age_dir "$foreign" AC5
run_agg "$td5" "$sf5" PASS
if [ "$OWNER_MARKER_CAPABLE" = 1 ]; then
  if [ ! -d "$aged" ]; then
    ok "AC5: the startup sweep removed an AGED agent-gate.* directory whose owner is provably gone"
  else
    bad "AC5: the startup sweep did NOT remove an aged agent-gate.* directory whose owner is provably gone"
  fi
  # AGE IS NOT PROOF OF ABANDONMENT (roborev job 70 medium 1). Both of these are older
  # than the floor and neither may be taken: one has a LIVE owner, the other has no
  # marker at all, and `cannot-tell` must not take the permissive branch.
  if [ -d "$aged_live" ]; then
    ok "AC5: the sweep left an AGED directory whose owner is LIVE alone"
  else
    bad "AC5: the sweep removed an AGED directory whose owner is LIVE — a live peer's bundle destroyed"
  fi
else
  # The whole point of the degradation: with no establishable owner token NOTHING is
  # removable, so the aged directory that WOULD be swept on Linux survives here.
  if [ -d "$aged" ] && [ -d "$aged_live" ]; then
    ok "AC5: DEGRADED — the sweep removed NEITHER aged directory on a host with no establishable owner token (cannot-tell keeps everything)"
  else
    bad "AC5: DEGRADED — the sweep removed an aged directory whose owner could NOT be established; cannot-tell took the permissive branch"
  fi
fi
if [ -d "$aged_unk" ]; then
  ok "AC5: the sweep left an AGED directory with NO owner marker alone (cannot-tell is not permissive)"
else
  bad "AC5: the sweep removed an AGED directory whose owner could not be established"
fi
if [ -d "$fresh" ]; then
  ok "AC5: the startup sweep left a FRESH agent-gate.* directory alone"
else
  bad "AC5: the startup sweep removed a FRESH agent-gate.* directory"
fi
if [ -d "$notours" ] && [ -d "$foreign" ]; then
  ok "AC5: the sweep left an aged dir of a different basename shape untouched"
else
  bad "AC5: the sweep removed a directory that is not the gate's own creation shape"
fi
sweep5=$(sed -n 's/^logdir-sweep: //p' "$sf5" | tail -1)
if [ "$OWNER_MARKER_CAPABLE" = 1 ]; then
  case "$sweep5" in
    *"1 REMOVED"*"aged (>7d)"*) ok "AC5: the SUMMARY reports the sweep affirmatively ($sweep5)" ;;
    *) bad "AC5: the SUMMARY sweep line is missing or malformed (got: '${sweep5:-<none>}')" ;;
  esac
  # The owner census rides the same line, so a run that reclaimed less than it found
  # says WHY rather than leaving the reader to guess.
  case "$sweep5" in
    *"verified-dead 1"*"live 1"*"unverifiable 1"*)
      ok "AC5: the sweep line reports the owner census (verified-dead/live/unverifiable)" ;;
    *) bad "AC5: the sweep line does not report the owner census (got: '${sweep5:-<none>}')" ;;
  esac
else
  # `0 REMOVED of 3 aged`, and the census must attribute all three to `unverifiable` —
  # a degraded host has to SAY it verified nothing, not report a bare zero the reader
  # would read as an all-clear.
  case "$sweep5" in
    *"0 REMOVED of 3 aged (>7d)"*)
      ok "AC5: DEGRADED — the SUMMARY reports 0 REMOVED of 3 aged, affirmatively ($sweep5)" ;;
    *) bad "AC5: DEGRADED — the sweep line does not report 0 REMOVED of 3 aged (got: '${sweep5:-<none>}')" ;;
  esac
  case "$sweep5" in
    *"verified-dead 0"*"live 0"*"unverifiable 3"*)
      ok "AC5: DEGRADED — the census attributes every aged candidate to unverifiable, so the reason nothing was reclaimed is visible in the block" ;;
    *) bad "AC5: DEGRADED — the census does not attribute all 3 aged candidates to unverifiable (got: '${sweep5:-<none>}')" ;;
  esac
fi
# `0 REMOVED`, never a bare `0`: a bare zero in a gate log reads as a verified all-clear.
td5b="$tmp/td-sweep-empty"; sf5b="$tmp/sweep-empty.txt"
run_agg "$td5b" "$sf5b" PASS
sweep5b=$(sed -n 's/^logdir-sweep: //p' "$sf5b" | tail -1)
case "$sweep5b" in
  "0 REMOVED"*) ok "AC5: a no-op sweep reports '0 REMOVED', not a bare 0 ($sweep5b)" ;;
  *) bad "AC5: a no-op sweep does not report '0 REMOVED' (got: '${sweep5b:-<none>}')" ;;
esac

# ---------------------------------------------------------------------------
# AC6: a run that KEEPS its directory is otherwise unchanged.
# ---------------------------------------------------------------------------
# tree-integrity survives untouched on the retained FAIL run.
if grep -q '^tree-integrity: PASS' "$sf2"; then
  ok "AC6: the retained FAIL run's tree-integrity line is unchanged"
else
  bad "AC6: the retained FAIL run lost its tree-integrity line"
fi
# The logs: PATH FIELD is byte-identical to the directory the run created — the
# disposition is APPENDED, never woven into the path.
if [ -n "$d2" ] && [ -d "$d2" ] && [ -f "$d2/summary.txt" ]; then
  ok "AC6: the retained run's logs: path field addresses the real bundle byte-for-byte"
else
  bad "AC6: the retained run's logs: path field does not address the real bundle"
fi
# The #2874 summary-integrity no-clobber path must RETAIN its bundle: it publishes
# the verdict to a sibling and POINTS AT the logs bundle, so removing it would
# delete the artifact the block names.
td6="$tmp/td-integrity"; sf6="$tmp/integrity-summary.txt"
mkdir -p "$td6"
env -u AGENT_GATE_PARENT_RUN_ID \
    TMPDIR="$td6" \
    AGENT_GATE_SUMMARY_FILE="$sf6" \
    AGENT_GATE_INTEGRITY_SELFTEST=1 \
    bash "$FAKE_GATE" >"$td6.out" 2>&1
if grep -q '^summary-integrity: FAIL' "$td6.out"; then
  ok "AC6: precondition — the #2874 no-clobber path fired"
else
  bad "AC6: precondition failed — the #2874 no-clobber path did not fire"
  sed -n '1,30p' "$td6.out"
fi
d6=$(logs_field "$td6.out") || d6=""
if [ -n "$d6" ] && [ -d "$d6" ]; then
  ok "AC6: the summary-integrity no-clobber run RETAINED its logs bundle"
else
  bad "AC6: the summary-integrity no-clobber run lost its logs bundle ('${d6:-<none>}')"
fi
# A run that never reaches a terminal verdict (the RESULT: INCOMPLETE sentinel) is
# the post-mortem case — and the rule is CONTENT-BASED, so both directions are
# asserted here rather than one being assumed:
#   * non-zero early exit WITH diagnostic content -> RETAIN (there is a post-mortem);
#   * non-zero early exit with an EMPTY bundle     -> REMOVE (a husk informs nobody,
#     and husks are the population this issue exists to drain).
# The same gate mode drives both: the tree-selftest fixture refusal exits 2 after the
# INCOMPLETE sentinel, and whether `_tree_capture_start` managed to write
# tree-identity.start into the bundle is exactly what a git checkout adds.
gitroot="$tmp/gitroot"
mkdir -p "$gitroot/scripts"
cp "$GATE" "$gitroot/scripts/agent-gate.sh"
git_init_ok=0
if git -C "$gitroot" "${GIT_CFG[@]}" init -q . >/dev/null 2>&1 \
   && git -C "$gitroot" "${GIT_CFG[@]}" add -A >/dev/null 2>&1 \
   && git -C "$gitroot" "${GIT_CFG[@]}" commit -qm init >/dev/null 2>&1; then
  git_init_ok=1
fi
# d7 is the ONLY survivor variable assigned inside a conditional (every sibling is
# top-level), so under `set -u` a failed git fixture made the final survivor accounting
# expand an UNSET name and abort the run BEFORE it printed its report — the fixture
# failure was recorded and then never shown. Defined here, unconditionally, rather than
# guarded as `${d7:-}` at each use: the accounting has 13 call sites and the next one
# added would forget (roborev job 68).
d7=""
if [ "$git_init_ok" = 1 ]; then
  td7="$tmp/td-incomplete"; sf7="$tmp/incomplete-summary.txt"
  mkdir -p "$td7"
  env -u AGENT_GATE_PARENT_RUN_ID \
      TMPDIR="$td7" \
      AGENT_GATE_SUMMARY_FILE="$sf7" \
      AGENT_GATE_TREE_SELFTEST=boundary \
      bash "$gitroot/scripts/agent-gate.sh" >"$td7.out" 2>&1
  if grep -q '^RESULT: INCOMPLETE' "$sf7" 2>/dev/null; then
    ok "AC6: precondition — the refusal run left the INCOMPLETE sentinel, no terminal verdict"
  else
    bad "AC6: precondition failed — the refusal run did not leave an INCOMPLETE sentinel"
  fi
  # THREE-VALUED, both scans (#1699's find-tristate rule, which this repo LINTS for
  # by name): "the scan FAILED" is not "no match", and collapsing them here would
  # report a LOST bundle when the truth is that the directory could not be read —
  # a real failure and an unmeasurable one taking the same branch, needing different
  # remedies. Each scan captures its own status; `could not tell` gets its own bad().
  d7_listing=$(find "$td7" -maxdepth 1 -type d -name 'agent-gate.*' 2>/dev/null); d7_rc=$?
  d7=""
  [ "$d7_rc" -eq 0 ] && d7=$(printf '%s\n' "$d7_listing" | head -1)
  if [ "$d7_rc" -ne 0 ]; then
    bad "AC6: could not scan $td7 for the run's bundle (find rc=$d7_rc) — UNMEASURED, not 'no bundle'"
  elif [ -z "$d7" ]; then
    bad "AC6: a run that died before its terminal verdict left NO bundle under $td7"
  else
    b7_listing=$(find "$d7" -mindepth 1 -maxdepth 1 2>/dev/null); b7_rc=$?
    if [ "$b7_rc" -ne 0 ]; then
      bad "AC6: could not scan the retained bundle '$d7' (find rc=$b7_rc) — UNMEASURED, not 'empty'"
    elif [ -n "$b7_listing" ]; then
      ok "AC6: a run that died before its terminal verdict KEPT its non-empty bundle"
    else
      bad "AC6: a run that died before its terminal verdict left an EMPTY bundle ('$d7')"
    fi
  fi
else
  bad "AC6: could not build a git fixture root — the retain-with-content case did not run"
fi
# The EMPTY half: the same mode in the non-git fake checkout refuses BEFORE anything
# is written into the bundle, so there is nothing to post-mortem and no husk is left.
td7b="$tmp/td-incomplete-empty"; sf7b="$tmp/incomplete-empty.txt"
mkdir -p "$td7b"
env -u AGENT_GATE_PARENT_RUN_ID \
    TMPDIR="$td7b" \
    AGENT_GATE_SUMMARY_FILE="$sf7b" \
    AGENT_GATE_TREE_SELFTEST=boundary \
    bash "$FAKE_GATE" >"$td7b.out" 2>&1
if [ "$(count_logdirs "$td7b")" = 0 ]; then
  ok "AC6: a non-zero early exit with an EMPTY bundle left no husk"
else
  bad "AC6: a non-zero early exit left $(count_logdirs "$td7b") empty husk directory/ies"
fi

# ---------------------------------------------------------------------------
# AC7: `logs:` is PATH-ONLY; the disposition rides its own key.
#
# Driven through a $TMPDIR whose own name contains " (" — the sequence the withdrawn
# `logs: <path> (DISPOSITION)` design used as its delimiter. With the disposition on
# the logs: line, the documented `${line%% (*}` recovery truncated such a path to its
# first component and every consumer lost the bundle. Control and environment-
# controlled data must not share a channel (#3312).
# ---------------------------------------------------------------------------
td8="$tmp/paren (dir)/td"; sf8="$tmp/paren-summary.txt"
mkdir -p "$td8"
run_agg "$td8" "$sf8" FAIL
d8=$(logs_field "$sf8") || d8=""
if [ -n "$d8" ] && [ -d "$d8" ]; then
  ok "AC7: the logs: field is a resolvable path even when \$TMPDIR contains ' (' ($d8)"
else
  bad "AC7: the logs: field did not resolve under a ' ('-bearing TMPDIR ('${d8:-<none>}')"
fi
case "$d8" in
  *" ("*) ok "AC7: precondition — the run's log dir really does carry ' (' in its path" ;;
  *) bad "AC7: precondition failed — the scratch TMPDIR lost its ' (' ($d8)" ;;
esac
logs_line8=$(sed -n 's/^logs: //p' "$sf8" | tail -1)
if [ "$logs_line8" = "$d8" ] && [ -d "$logs_line8" ]; then
  ok "AC7: the logs: line carries the path and NOTHING else"
else
  bad "AC7: the logs: line is not path-only (got: '$logs_line8')"
fi
disp8=$(logs_disposition "$sf8")
case "$disp8" in
  RETAINED*FAIL*) ok "AC7: the disposition is on its own logdir-disposition: key ($disp8)" ;;
  *) bad "AC7: no logdir-disposition: key for the ' ('-TMPDIR run (got: '${disp8:-<none>}')" ;;
esac
# One key, one line: a second `logdir-disposition:` would mean two emit paths disagree.
n_disp8=$(grep -c '^logdir-disposition: ' "$sf8" 2>/dev/null || true)
if [ "$n_disp8" = 1 ]; then
  ok "AC7: exactly one logdir-disposition: line in the block"
else
  bad "AC7: expected exactly one logdir-disposition: line, found $n_disp8"
fi

# ---------------------------------------------------------------------------
# AC8: EARLY EXITS — paths that never reach the terminal emit.
#
# roborev job 54, finding 1. `_logdir_decide` had exactly ONE call site (the terminal
# emit), so anything that created a LOG_DIR and left before it retained the directory
# by default. Two windows existed and both are covered here: the test stub, which
# `exit 0`s after its sleep (6 dirs per gate-of-record run — test_gate_concurrency_cap.sh
# drives it 6x inside tooling-tests), and the argv/usage refusals, which sit between
# LOG_DIR creation and the composed trap arming ~2000 lines later and so ran no EXIT
# trap at all. The assertions below are on the SPECIFIC observable — the directory
# count under an isolated scratch TMPDIR — never on an exit code.
# ---------------------------------------------------------------------------
# (a) the CQLITE_GATE_STUB_RUNDIR stub, run NESTED exactly as tooling-tests runs it
#     (AGENT_GATE_PARENT_RUN_ID set, no explicit summary file, so #2874 puts the
#     private summary INSIDE the log dir — the shape that made the naive carve-out
#     retain it).
td9="$tmp/td-stub"; mkdir -p "$td9/rundir"
env TMPDIR="$td9" \
    AGENT_GATE_PARENT_RUN_ID="/tmp/agent-gate.PARENTFAKE" \
    CQLITE_GATE_DISABLE_CAP=1 \
    CQLITE_GATE_STUB_RUNDIR="$td9/rundir" \
    CQLITE_GATE_STUB_SLEEP=0 \
    bash "$FAKE_GATE" >"$td9.out" 2>&1
stub_rc=$?
if [ "$stub_rc" -eq 0 ]; then
  ok "AC8: precondition — the stub run completed normally (exit 0)"
else
  bad "AC8: precondition failed — the stub run exited $stub_rc"
  sed -n '1,20p' "$td9.out"
fi
if [ "$(count_logdirs "$td9")" = 0 ]; then
  ok "AC8: the CQLITE_GATE_STUB_RUNDIR early exit left NO log directory"
else
  bad "AC8: the stub early exit LEFT $(count_logdirs "$td9") log directory/ies"
fi
# The opt-out must still reach an early exit — otherwise AGENT_GATE_KEEP_LOGS=1 would
# be silently ineffective for exactly the runs this fix newly removes.
td9b="$tmp/td-stub-keep"; mkdir -p "$td9b/rundir"
env TMPDIR="$td9b" \
    AGENT_GATE_PARENT_RUN_ID="/tmp/agent-gate.PARENTFAKE" \
    AGENT_GATE_KEEP_LOGS=1 \
    CQLITE_GATE_DISABLE_CAP=1 \
    CQLITE_GATE_STUB_RUNDIR="$td9b/rundir" \
    CQLITE_GATE_STUB_SLEEP=0 \
    bash "$FAKE_GATE" >"$td9b.out" 2>&1
if [ "$(count_logdirs "$td9b")" -ge 1 ]; then
  ok "AC8: AGENT_GATE_KEEP_LOGS=1 still retains an early exit's log directory"
else
  bad "AC8: AGENT_GATE_KEEP_LOGS=1 did NOT retain the stub run's log directory"
fi
# Recorded for the end-of-file survivor accounting, which enumerates every expected
# survivor by the case that owns it.
d9b=$(one_logdir "$td9b") || d9b=""

# (b) an argv/usage refusal: a NON-ZERO early exit whose bundle is EMPTY. A husk
#     carries no post-mortem information, so it is removed rather than left behind.
td10="$tmp/td-refusal"; mkdir -p "$td10"
env -u AGENT_GATE_PARENT_RUN_ID -u AGENT_GATE_SUMMARY_FILE \
    TMPDIR="$td10" \
    CQLITE_GATE_DISABLE_CAP=1 \
    AGENT_GATE_INTEGRITY_SELFTEST=not-a-valid-selector \
    bash "$FAKE_GATE" >"$td10.out" 2>&1
refusal_rc=$?
if [ "$refusal_rc" -eq 2 ]; then
  ok "AC8: precondition — the usage refusal really exited 2 before any emit"
else
  bad "AC8: precondition failed — the usage refusal exited $refusal_rc (expected 2)"
fi
if [ "$(count_logdirs "$td10")" = 0 ]; then
  ok "AC8: a non-zero usage refusal with an EMPTY bundle left no husk directory"
else
  bad "AC8: the usage refusal left $(count_logdirs "$td10") empty husk directory/ies"
fi

# (c) --list: exits from the argv dispatch BEFORE the LOG_DIR is created at all, so
#     the correct observable is that no directory is ever made. Asserted rather than
#     assumed: it is the cheapest possible early exit and the one most likely to be
#     copied when a new hook is added.
td11="$tmp/td-list"; mkdir -p "$td11"
env TMPDIR="$td11" bash "$FAKE_GATE" --list >"$td11.out" 2>&1
if [ -s "$td11.out" ]; then
  ok "AC8: precondition — --list printed its component set"
else
  bad "AC8: precondition failed — --list printed nothing"
fi
if [ "$(count_logdirs "$td11")" = 0 ]; then
  ok "AC8: --list created no log directory"
else
  bad "AC8: --list created $(count_logdirs "$td11") log directory/ies"
fi

# ---------------------------------------------------------------------------
# AC10: A RETENTION IS ALWAYS NAMED — in an artifact THE RUN ITSELF OWNS.
#
# roborev job 59, finding 2. The disposition is decided inside the EXIT trap, and on
# an EARLY EXIT nothing ever published it: the `RESULT: INCOMPLETE` sentinel is
# written AT LAUNCH, long before any decision exists, so it carries neither `logs:`
# nor `logdir-disposition:` — and AC8 above only COUNTS directories, so it cannot see
# a retention with no stated reason. A bundle left on disk with nothing saying why is
# precisely the anonymous population this issue exists to drain: the reader who finds
# it is back to guessing, which is the habit behind the #3616 near-miss.
#
# So every RETAINING path writes `logdir-disposition.txt` INTO the bundle before
# cleanup — the post-mortem artifact a reader already has in hand, owned by the run,
# keyed with the SAME `logdir-disposition:` name the SUMMARY uses so one grep answers
# both, and carrying `run-id:` because a run dir is bound to a gate ONLY by that
# value (#3637): a bundle located by recency is a PEER's until its run-id says
# otherwise.
#
# Asserted in BOTH directions, because "always named" is a claim about every
# retaining path and only one of them has a SUMMARY to name it in:
#   * a RETAINED early exit (the opt-out arm, and the non-zero-with-content arm)
#     carries the artifact with its reason;
#   * a REMOVED early exit leaves NO artifact anywhere — it goes with the directory
#     and is never orphaned beside it.
# ---------------------------------------------------------------------------
# (a) RETAINED via the opt-out: the same stub early exit AC8 drove with
#     AGENT_GATE_KEEP_LOGS=1, which reaches no terminal emit and so has no SUMMARY.
d9b=$(one_logdir "$td9b"); one9b_rc=$?
if [ "$one9b_rc" -eq 2 ]; then
  bad "AC10: could not scan $td9b for the retained bundle — UNMEASURED, not 'no bundle'"
elif [ "$one9b_rc" -ne 0 ] || [ -z "$d9b" ]; then
  bad "AC10: precondition failed — the KEEP_LOGS early exit left no bundle to name"
else
  disp9b=$(artifact_field "$d9b" logdir-disposition)
  case "$disp9b" in
    *AGENT_GATE_KEEP_LOGS=1*)
      ok "AC10: the retained early exit NAMES its reason in its own bundle ($disp9b)" ;;
    "")
      bad "AC10: the retained early exit published NO logdir-disposition artifact — a silent retention is the anonymous-bundle population #3637 drains" ;;
    *)
      bad "AC10: the retained early exit named the wrong reason ('$disp9b')" ;;
  esac
  # Bindable to THIS run: recency is not identity (#3637/#3616).
  rid9b=$(artifact_field "$d9b" run-id)
  if [ -n "$rid9b" ] && [ "$rid9b" = "$d9b" ]; then
    ok "AC10: the artifact binds the bundle to its run by run-id, not by recency"
  else
    bad "AC10: the artifact carries no usable run-id ('${rid9b:-<none>}' vs '$d9b')"
  fi
  # ONE field name, ONE grammar: the artifact's logs: is path-only, exactly as the
  # SUMMARY's and the heartbeat's are (AC7/AC9). A third spelling would reopen #3312.
  algs9b=$(artifact_field "$d9b" logs)
  if [ "$algs9b" = "$d9b" ] && [ -d "$algs9b" ]; then
    ok "AC10: the artifact's logs: field is path-only and resolves to the bundle"
  else
    bad "AC10: the artifact's logs: field is not path-only (got: '${algs9b:-<none>}')"
  fi
fi

# (b) RETAINED via the non-zero-with-content arm: the AC6 git-fixture refusal, whose
#     summary file holds only the INCOMPLETE sentinel — the exact case that had no
#     named reason anywhere before this.
if [ "$git_init_ok" = 1 ] && [ -n "${d7:-}" ] && [ -d "${d7:-}" ]; then
  disp7=$(artifact_field "$d7" logdir-disposition)
  case "$disp7" in
    RETAINED:*)
      ok "AC10: the INCOMPLETE-sentinel run's retained bundle names its own reason ($disp7)" ;;
    "")
      bad "AC10: the INCOMPLETE-sentinel run retained a bundle with NO named reason — the sentinel carries no logdir-disposition: either, so the retention is silent" ;;
    *)
      bad "AC10: the INCOMPLETE-sentinel run's reason is not a RETAINED verdict ('$disp7')" ;;
  esac
  if grep -q '^logdir-disposition: ' "$sf7" 2>/dev/null; then
    bad "AC10: unexpected — the INCOMPLETE sentinel now carries a disposition; keep ONE owner for the field"
  else
    ok "AC10: precondition — the INCOMPLETE sentinel carries no disposition, so the bundle artifact is the only place the reason can live"
  fi
else
  bad "AC10: the git fixture did not produce a retained bundle — the named-reason case did not run"
fi

# (c) REMOVED: the artifact is part of the bundle, so it must vanish WITH it. A file
#     left beside a deleted directory would be a second, orphaned population.
for pair in "$td9:stub exit-0" "$td10:usage refusal"; do
  d="${pair%%:*}"; what="${pair#*:}"
  orphan_listing=$(find "$d" -maxdepth 2 -name 'logdir-disposition.txt' 2>/dev/null); orphan_rc=$?
  if [ "$orphan_rc" -ne 0 ]; then
    bad "AC10: could not scan $d for orphaned disposition artifacts (find rc=$orphan_rc) — UNMEASURED, not 'none'"
  elif [ -z "$orphan_listing" ]; then
    ok "AC10: the REMOVED early exit ($what) left no disposition artifact behind"
  else
    bad "AC10: the REMOVED early exit ($what) orphaned a disposition artifact ($orphan_listing)"
  fi
done

# ---------------------------------------------------------------------------
# AC9: ONE field name, ONE grammar — the SUMMARY's `logs:` and the HEARTBEAT's
# `logs:` must be byte-identical.
#
# `scripts/lib/gate-heartbeat.sh` emits its own `logs:` line into
# `<summary-file>.heartbeat` (the file `scripts/gate-liveness.sh` reads). It is a
# different file, but the SAME field name — so had the disposition been appended to
# the SUMMARY's `logs:` while the heartbeat's stayed path-only, one field name would
# have carried TWO grammars, which is the ambiguity #3637 removes rather than a
# second instance of it. Path-only makes them identical BY CONSTRUCTION, and that is
# what is pinned here.
#
# STRUCTURAL, and labelled as such. The behavioural route is not available: the
# hermetic no-cargo modes finish in well under one beat, so the beater is reaped
# before it writes its first file, and an "assert only if the heartbeat happens to
# exist" case is a conditional skip wearing an assertion's clothes. The three facts
# below are the whole of the invariant and are each a bounded read of shipped source.
# ---------------------------------------------------------------------------
BEATER="$SCRIPT_DIR/../lib/gate-heartbeat.sh"
if [ -f "$BEATER" ]; then
  ok "AC9: precondition — the beater script is where the gate looks for it"
else
  bad "AC9: the beater script is missing ($BEATER)"
fi
# (1) the SUMMARY renders the raw variable with NOTHING after it.
if grep -qF "printf 'logs: %s\\n' \"\$LOG_DIR\"" "$GATE"; then
  ok "AC9: the SUMMARY logs: line is an undecorated '%s' of \$LOG_DIR"
else
  bad "AC9: the SUMMARY logs: line is no longer an undecorated '%s' of \$LOG_DIR — if a clause was re-appended, the heartbeat's logs: now disagrees with it"
fi
# (2) the gate hands the beater that SAME variable, not a rendered line.
if grep -qF -- '--logs "$LOG_DIR"' "$GATE"; then
  ok "AC9: the gate passes the beater --logs \"\$LOG_DIR\" verbatim"
else
  bad "AC9: the gate no longer passes --logs \"\$LOG_DIR\" — the two logs: fields can drift"
fi
# (3) the beater prints it undecorated too.
if grep -qF 'echo "logs: $LOGS"' "$BEATER"; then
  ok "AC9: the heartbeat logs: line is an undecorated echo of \$LOGS"
else
  bad "AC9: the heartbeat logs: line is no longer an undecorated echo of \$LOGS"
fi
# (4) and the disposition is NOT duplicated into the HEARTBEAT. The two artifacts
# that may declare it (the SUMMARY, and the retained bundle's own
# logdir-disposition.txt — AC10) are both written ONCE, AFTER the decision exists.
# The heartbeat is written REPEATEDLY DURING the run, every 20s from before the
# first component to after the last, so any disposition it carried would be a guess
# about an exit that has not happened — a stale value in the one file a reader polls
# while the gate is still alive.
if grep -q 'logdir-disposition' "$BEATER"; then
  bad "AC9: the beater emits a logdir-disposition — a file written before the decision exists can only carry a guess"
else
  ok "AC9: the beater carries no disposition field (it is written before any decision exists)"
fi

# ---------------------------------------------------------------------------
# AC11: the removal is CLEARED only once the verdict is PUBLISHED, and a retention
# that contradicts an already-published claim CORRECTS ITSELF in the bundle.
#
# #3637 roborev job 61. `_logdir_decide` runs as the FIRST action of the terminal
# emit — deliberately, so the block it assembles can DECLARE what happens to the
# directory. Arming the removal there too meant a run that then FAILED to publish its
# verdict lost BOTH artifacts at once: no summary on the caller-known path, and no
# post-mortem bundle either. That is the reachable ENOSPC shape this issue cites as
# its own motivation (/dev/root holds every lane's summary file AND its LOG_DIR), and
# it is AC6's "never a way to lose a failed gate's evidence" being crossed by the
# cleanup itself. So the DECISION stays early and the CLEARANCE moves after the
# verification; between the two, every exit retains.
# ---------------------------------------------------------------------------

# wait_for_logdir <scratch-tmpdir> [tries] -> echoes the bundle path once it appears.
# Bounded, and rc 1 if it never does — never an unbounded wait and never a silent skip.
wait_for_logdir() {
  local root="$1" tries="${2:-200}" i d
  for ((i = 0; i < tries; i++)); do
    d=$(find "$root" -maxdepth 1 -type d -name 'agent-gate.*' 2>/dev/null | head -1)
    [ -n "$d" ] && { printf '%s' "$d"; return 0; }
    sleep 0.05
  done
  return 1
}

# (a) THE SUMMARY WRITE FAILS. An unwritable caller-known path (a missing parent
#     directory — the same class as a full disk) makes emit_summary's authoritative
#     write fail AFTER the block, and its declared disposition, are already composed.
#     The bundle is then the ONLY place this run's verdict survives, so it must be
#     kept — and the surviving bundle must say that the declaration is superseded.
td11="$tmp/td-writefail"; mkdir -p "$td11"
run_agg "$td11" "$td11/nodir/summary.txt" PASS
wf_rc=$?
if [ "$wf_rc" -ne 0 ]; then
  ok "AC11a: precondition — an unwritable summary path really failed the run (exit $wf_rc)"
else
  bad "AC11a: precondition failed — the unwritable summary path did not fail the run"
fi
d11=$(one_logdir "$td11"); d11_rc=$?
case "$d11_rc" in
  0) ok "AC11a: a run whose summary write FAILED KEPT its bundle ($d11)" ;;
  1) bad "AC11a: a run whose summary write FAILED lost its bundle — the verdict AND the post-mortem, gone together" ;;
  *) bad "AC11a: could not measure the scratch TMPDIR — refusing to call that a pass" ;;
esac
if [ "$d11_rc" -eq 0 ]; then
  disp11=$(artifact_field "$d11" logdir-disposition)
  case "$disp11" in
    RETAINED*summary\ write\ failed*) ok "AC11a: the bundle's own artifact names the write failure ($disp11)" ;;
    '') bad "AC11a: the retained bundle published NO disposition artifact" ;;
    *) bad "AC11a: the retained bundle named the wrong reason ('$disp11')" ;;
  esac
  sup11=$(artifact_field "$d11" logdir-disposition-superseded)
  case "$sup11" in
    *REMOVED\ at\ exit\ on\ PASS*SUPERSEDED*) ok "AC11a: the artifact states plainly that the block's REMOVED claim is superseded" ;;
    '') bad "AC11a: the bundle carries no correction — a reader holds a block claiming REMOVED beside the directory it claims to have removed" ;;
    *) bad "AC11a: the correction does not quote the superseded claim ('$sup11')" ;;
  esac
fi

# (b) THE RUN IS SIGNALLED. Measured 2026-09-01 on bash 5.2: bash RUNS the EXIT trap
#     for an UNTRAPPED TERM delivered while it waits on a foreground command, and `$?`
#     inside that trap is 0 — so a signalled gate used to take the "status 0, nothing
#     here is anyone's evidence" arm and its bundle was DELETED. Reproduced here with
#     a planted component result standing in for the component logs a real run has:
#     the bundle holds EVIDENCE, so the signal must leave it alone.
td11b="$tmp/td-signalled"; mkdir -p "$td11b"
env -u AGENT_GATE_PARENT_RUN_ID \
    TMPDIR="$td11b" \
    AGENT_GATE_SUMMARY_FILE="$tmp/signalled-summary.txt" \
    CQLITE_GATE_STUB_RUNDIR="$td11b/rundir" \
    CQLITE_GATE_STUB_SLEEP=30 \
    bash "$FAKE_GATE" >"$td11b.out" 2>&1 &
sig_pid=$!
if d11b=$(wait_for_logdir "$td11b"); then
  ok "AC11b: precondition — the signalled run created its bundle ($d11b)"
  : >"$d11b/planted-component.result"
  kill -TERM "$sig_pid" 2>/dev/null
  wait "$sig_pid"; sig_rc=$?
  if [ "$sig_rc" -ge 128 ]; then
    ok "AC11b: precondition — the run really died of the signal (exit $sig_rc)"
  else
    bad "AC11b: precondition failed — the run exited $sig_rc, not of a signal; the case measured something else"
  fi
  if [ -d "$d11b" ]; then
    ok "AC11b: a SIGTERMed run with evidence in its bundle KEPT it"
  else
    bad "AC11b: a SIGTERMed run's bundle was DELETED — the post-mortem case par excellence, removed by the cleanup"
  fi
  disp11b=$(artifact_field "$d11b" logdir-disposition)
  case "$disp11b" in
    RETAINED*evidence*) ok "AC11b: the signalled bundle NAMES its retention ($disp11b)" ;;
    '') bad "AC11b: the signalled bundle published no disposition artifact" ;;
    *) bad "AC11b: the signalled bundle named an unexpected reason ('$disp11b')" ;;
  esac
else
  bad "AC11b: the signalled run never created a bundle — cannot measure"
  kill -TERM "$sig_pid" 2>/dev/null; wait "$sig_pid" 2>/dev/null
fi

# (c) THE REMOVAL ITSELF FAILS. The bundle survives a removal the SUMMARY already
#     declared, and the once-only finalization means nothing will try again. The
#     surviving bundle must carry the correction — the same artifact, the same
#     mechanism.
#
#     The failure is induced with a PATH `rm` shim scoped to this case's own bundle
#     shape, NOT by making the parent directory unwritable: `chmod 500` is a no-op for
#     UID 0, so under root (containers, some CI images) the removal SUCCEEDS and this
#     registered tooling-tests case FAILs on correct behaviour — a guard that reds on
#     correct input is the guard agents learn to waive. The shim is
#     privilege-independent, refuses ONLY `$td11c/agent-gate.*` (delegating every
#     other call to the real rm), and RECORDS its refusal so the case can prove the
#     mechanism fired rather than inferring it from a directory that survived for some
#     other reason.
REAL_RM=$(command -v rm)
if [ -z "$REAL_RM" ] || [ ! -x "$REAL_RM" ]; then
  bad "AC11c: no real rm on PATH — cannot build the scoped shim, refusing to call that a pass"
  REAL_RM=""
fi
td11c="$tmp/td-rmfail"; mkdir -p "$td11c"
rmshim="$tmp/rmshim"; mkdir -p "$rmshim"
rm_refused="$tmp/rm-refused.log"
cat >"$rmshim/rm" <<SHIM
#!/usr/bin/env bash
# Scoped refusal: only this case's bundle, only under its own scratch TMPDIR.
for a in "\$@"; do
  case "\$a" in
    "$td11c"/agent-gate.*)
      printf '%s\\n' "\$a" >>"$rm_refused"
      exit 1 ;;
  esac
done
exec "$REAL_RM" "\$@"
SHIM
chmod +x "$rmshim/rm"
env -u AGENT_GATE_PARENT_RUN_ID \
    PATH="$rmshim:$PATH" \
    TMPDIR="$td11c" \
    AGENT_GATE_SUMMARY_FILE="$tmp/rmfail-summary.txt" \
    CQLITE_GATE_STUB_RUNDIR="$td11c/rundir" \
    CQLITE_GATE_STUB_SLEEP=6 \
    bash "$FAKE_GATE" >"$td11c.out" 2>&1 &
rmf_pid=$!
if d11c=$(wait_for_logdir "$td11c"); then
  wait "$rmf_pid"
  if grep -qxF "$d11c" "$rm_refused" 2>/dev/null; then
    ok "AC11c: precondition — the removal was really ATTEMPTED and the shim refused it"
  else
    bad "AC11c: precondition failed — no removal of $d11c was attempted; the case measured nothing"
  fi
  if [ -d "$d11c" ]; then
    ok "AC11c: precondition — the removal really could not unlink the bundle"
    sup11c=$(artifact_field "$d11c" logdir-disposition-superseded)
    disp11c=$(artifact_field "$d11c" logdir-disposition)
    case "$disp11c" in
      RETAINED*FAILED*) ok "AC11c: the surviving bundle names the FAILED removal ($disp11c)" ;;
      '') bad "AC11c: the surviving bundle published no disposition artifact — its SUMMARY says REMOVED and nothing says otherwise" ;;
      *) bad "AC11c: the surviving bundle named the wrong reason ('$disp11c')" ;;
    esac
    case "$sup11c" in
      *SUPERSEDED*) ok "AC11c: the surviving bundle states that the SUMMARY's claim is superseded" ;;
      *) bad "AC11c: the surviving bundle carries no correction ('${sup11c:-<none>}')" ;;
    esac
  else
    bad "AC11c: precondition failed — the bundle was removed despite the refusing rm shim; nothing was measured"
  fi
else
  bad "AC11c: the run never created a bundle — cannot measure"
  wait "$rmf_pid" 2>/dev/null
fi

# (d) STRUCTURAL, and labelled as such: the ORDERING is the property, and a behavioural
#     case can only sample the orderings that exist today. Three bounded reads of the
#     shipped script pin it against a refactor that re-merges the two steps.
if awk '/^_logdir_decide\(\) \{/,/^\}/' "$GATE" | grep -q 'GATE_LOGDIR_REMOVE=1'; then
  bad "AC11d: _logdir_decide arms the removal itself again — the decision must set the INTENT only"
else
  ok "AC11d: _logdir_decide records an intent and arms no removal"
fi
clear_calls=$(grep -c '^ *_logdir_clear_removal$' "$GATE")
if [ "$clear_calls" = 1 ]; then
  ok "AC11d: the removal has exactly ONE clearance site"
else
  bad "AC11d: the removal has $clear_calls clearance sites — one is the whole point"
fi
verify_line=$(grep -n '^  if \[ -n "\$reason" \]; then$' "$GATE" | head -1 | cut -d: -f1)
clear_line=$(grep -n '^ *_logdir_clear_removal$' "$GATE" | head -1 | cut -d: -f1)
if [ -n "$verify_line" ] && [ -n "$clear_line" ] && [ "$clear_line" -gt "$verify_line" ]; then
  ok "AC11d: the clearance sits DOWNSTREAM of the summary-write verification (line $clear_line > $verify_line)"
else
  bad "AC11d: could not establish that the clearance follows the write verification (verify='${verify_line:-<none>}' clear='${clear_line:-<none>}')"
fi

# ---------------------------------------------------------------------------
# AC12: an UNCREATABLE log directory is a REFUSAL, not a degraded run.
#
# #3637 roborev job 63 finding 1. `mktemp -d` was unchecked, so a temp parent the
# gate cannot write leaves LOG_DIR EMPTY — and RUN_ID with it — after which every
# child path resolves ROOT-LEVEL (`/summary-primary.txt`, `/<component>.log`). The
# removal guard refuses an empty path, correctly, so nothing would ever reclaim
# them. Measured with a TMPDIR that does not exist: the run must exit non-zero
# naming the refusal, and must create no directory anywhere.
# ---------------------------------------------------------------------------
td12="$tmp/td-nomktemp"; mkdir -p "$td12"
env -u AGENT_GATE_PARENT_RUN_ID \
    TMPDIR="$td12/does-not-exist" \
    AGENT_GATE_SUMMARY_FILE="$tmp/nomktemp-summary.txt" \
    AGENT_GATE_TEST_LITE_SCOPED=PASS \
    AGENT_GATE_TEST_LITE_RESULTS="file-size:PASS" \
    bash "$FAKE_GATE" --lite-aggregate-selftest >"$td12.out" 2>&1
rc12=$?
if [ "$rc12" -ne 0 ]; then
  ok "AC12: a gate that cannot create its log directory REFUSES (exit $rc12)"
else
  bad "AC12: a gate that cannot create its log directory ran on anyway (exit 0) — every child path it wrote was root-level"
fi
if grep -q 'could not create a per-run log directory' "$td12.out" 2>/dev/null; then
  ok "AC12: the refusal NAMES the cause rather than failing somewhere downstream"
else
  bad "AC12: the run produced no refusal naming the uncreatable log directory (got: '$(head -1 "$td12.out" 2>/dev/null)')"
fi
if [ "$(count_logdirs "$td12")" = 0 ] && [ ! -d "$td12/does-not-exist" ]; then
  ok "AC12: the refusing run created no directory at all"
else
  bad "AC12: the refusing run left something behind under $td12"
fi

# ---------------------------------------------------------------------------
# AC13 (STRUCTURAL, and labelled as such): the EXIT trap is armed BEFORE the startup
# sweep runs.
#
# #3637 roborev job 63 finding 4 — the register-before-create family this repo
# documents by name (roborev job 282: "round 9 register-before-create, round 14
# clean-up-on-signals"); this is its third instance. The sweep scans the shared temp
# parent and may unlink up to 1,000 aged directories, so it is the longest-running
# step between this run's mktemp and its first component. Armed after it, an INT or
# TERM arriving inside the sweep left THIS run's own fresh directory behind with no
# handler to dispose of it. The ORDER is the property, and a behavioural case could
# only sample the signal timings that happen to be reachable, so it is pinned with
# two bounded reads of the shipped script.
# ---------------------------------------------------------------------------
sweep_line=$(grep -n '^_logdir_sweep$' "$GATE" | head -1 | cut -d: -f1)
trap_line=$(grep -n "^trap '_logdir_cleanup" "$GATE" | head -1 | cut -d: -f1)
if [ -n "$sweep_line" ] && [ -n "$trap_line" ]; then
  ok "AC13: both the sweep invocation (line $sweep_line) and the EXIT-trap arming (line $trap_line) are locatable"
  if [ "$trap_line" -lt "$sweep_line" ]; then
    ok "AC13: the EXIT trap is armed BEFORE the sweep runs"
  else
    bad "AC13: the sweep (line $sweep_line) runs BEFORE the EXIT trap is armed (line $trap_line) — a signal during it leaks this run's own directory"
  fi
else
  bad "AC13: could not locate the sweep invocation and/or the trap arming (sweep='${sweep_line:-<none>}' trap='${trap_line:-<none>}')"
fi

# ---------------------------------------------------------------------------
# AC14: the removal guard must accept a directory whose parent is the filesystem
# ROOT (#3637, roborev job 66 finding 2).
#
# `${d%/*}` strips to the EMPTY STRING for `/agent-gate.ABC123`, while
# GATE_LOGDIR_PARENT normalises to "/", so the direct-parent equality could never
# hold under TMPDIR=/ and BOTH the per-run cleanup and the startup sweep leaked
# every directory on such a box. The failure direction is fail-SAFE — the guard
# refuses to remove — so this is a leak, never a deletion hazard, and the fix
# normalises the DERIVED parent only, leaving every other condition untouched.
#
# The BEHAVIOURAL half needs a real directory whose parent is "/", which an
# unprivileged run cannot create; it runs where the root is writable and is
# DECLARED as not exercised otherwise, never silently skipped. The STRUCTURAL half
# is a bounded read of the shipped guard and runs everywhere.
# ---------------------------------------------------------------------------
guard_src=$(awk '/^_logdir_rm_guarded\(\) \{$/{f=1} f{print} f&&/^\}$/{exit}' "$GATE")
if [ -z "$guard_src" ]; then
  bad "AC14: could not extract _logdir_rm_guarded from the shipped gate — UNMEASURED"
else
  if printf '%s\n' "$guard_src" | grep -qF '[ "${d%/*}" = "$GATE_LOGDIR_PARENT" ]'; then
    bad "AC14: the guard still compares the RAW \${d%/*} against GATE_LOGDIR_PARENT — empty at the root, so it refuses every removal under TMPDIR=/"
  else
    ok "AC14: the guard does not compare a raw \${d%/*} against the recorded parent"
  fi
  if printf '%s\n' "$guard_src" | grep -qF 'parent="/"'; then
    ok "AC14: the guard normalises an EMPTY derived parent to the root"
  else
    bad "AC14: the guard has no empty-derived-parent normalisation — a root temp parent leaks"
  fi
  # Behavioural: run the extracted guard against a real root-level directory.
  if [ -w / ]; then
    rootdir=$(mktemp -d /agent-gate.XXXXXX 2>/dev/null) || rootdir=""
    case "$rootdir" in
      /agent-gate.??????)
        guard_rc=0
        env GATE_LOGDIR_PARENT=/ bash -c '
          '"$guard_src"'
          _logdir_rm_guarded "$1"' _ "$rootdir" || guard_rc=$?
        if [ "$guard_rc" -eq 0 ] && [ ! -d "$rootdir" ]; then
          ok "AC14: the guard REMOVED a directory whose parent is the filesystem root ($rootdir)"
        else
          bad "AC14: the guard refused a legitimate root-parented directory (rc=$guard_rc, still present: $([ -d "$rootdir" ] && echo yes || echo no))"
          rmdir "$rootdir" 2>/dev/null
        fi ;;
      *)
        bad "AC14: / is writable but a root-level fixture could not be created — UNMEASURED, not a pass" ;;
    esac
  else
    ok "AC14: DECLARED NOT EXERCISED — the behavioural root-parent case needs a writable /, which this run does not have; the structural halves above stand in for it"
  fi
fi

# ---------------------------------------------------------------------------
# AC15: the sweep's work cap bounds removal ATTEMPTS, not successes.
# ---------------------------------------------------------------------------
# MEASURED WITH FAILING REMOVALS, which is the only configuration in which the two
# accountings differ (roborev job 70 medium 2): the old loop compared `removed`, so
# while removals SUCCEED both forms stop at the cap and a passing test proves nothing.
# With removals that fail, the old form issued an `rm -rf` for EVERY aged candidate —
# ~35,000 of them on a box that has carried that population — while reporting a bound
# of one.
#
# The cap is pinned by SUBSTITUTING THE ARTIFACT in this case's own scratch copy of
# the gate (the repo idiom — see lib/agent-gate-canonical-pin.sh), never by a
# test-only env seam: a settable cap is one more thing a real invoker can set.
#
# The failure mechanism is a scoped `rm` shim, as AC11c already uses — NOT `chmod`,
# because uid 0 ignores permissions and this file is a registered `tooling-tests`
# case, so a chmod-gated assertion would pass or fail by the host's uid.
td15="$tmp/td-cap"; sf15="$tmp/cap-summary.txt"; mkdir -p "$td15"
cap_gate="$tmp/fakeroot-cap/scripts/agent-gate.sh"
mkdir -p "$(dirname "$cap_gate")"
sed 's/^GATE_LOGDIR_SWEEP_CAP=1000$/GATE_LOGDIR_SWEEP_CAP=2/' "$GATE" >"$cap_gate"
if grep -q '^GATE_LOGDIR_SWEEP_CAP=2$' "$cap_gate" && ! grep -q '^GATE_LOGDIR_SWEEP_CAP=1000$' "$cap_gate"; then
  ok "AC15: the scratch copy's sweep cap really was pinned to 2 (substitution verified)"
else
  bad "AC15: could not pin the sweep cap in the scratch copy — the case would measure the shipped 1000 and prove nothing"
fi
cap_planted=0
for n in 1 2 3 4 5; do
  d="$td15/agent-gate.CAP00$n"
  mkdir -p "$d"
  if [ "$OWNER_MARKER_CAPABLE" = 1 ]; then
    if plant_owner_marker "$d" dead; then cap_planted=$((cap_planted + 1)); fi
  fi
  age_dir "$d" AC15
done
if [ "$OWNER_MARKER_CAPABLE" != 1 ]; then
  # SKIPPED BY NAME, with the safe degradation asserted in its place: the attempt
  # accounting can only be measured against candidates that reach `verified-dead`, and
  # on a host with no establishable owner token none can. So the assertion becomes the
  # one property that IS true here — the sweep attempts NOTHING and takes NOTHING —
  # which needs no rm shim at all.
  ok "AC15: SKIPPED — no owner-marker capability on this host ($host_kind), so no candidate can reach verified-dead and the cap's attempt accounting has no subject; the degradation is asserted below instead"
  env -u AGENT_GATE_PARENT_RUN_ID \
      TMPDIR="$td15" \
      AGENT_GATE_SUMMARY_FILE="$sf15" \
      AGENT_GATE_TEST_LITE_SCOPED=PASS \
      AGENT_GATE_TEST_LITE_RESULTS="file-size:PASS fmt:PASS clippy:PASS" \
      bash "$cap_gate" --lite-aggregate-selftest >"$td15.out" 2>&1
  cap_survivors=0
  for n in 1 2 3 4 5; do
    [ -d "$td15/agent-gate.CAP00$n" ] && cap_survivors=$((cap_survivors + 1))
  done
  if [ "$cap_survivors" = 5 ]; then
    ok "AC15: DEGRADED — all 5 aged candidates survived a capped sweep on a host with no establishable owner token"
  else
    bad "AC15: DEGRADED — only $cap_survivors of 5 aged candidates survived; the sweep removed a bundle whose owner was never verified"
  fi
  sweep15=$(sed -n 's/^logdir-sweep: //p' "$sf15" | tail -1)
  case "$sweep15" in
    "0 REMOVED of 5 aged"*"verified-dead 0"*"unverifiable 2"*"examined 2, removals attempted 0"*"cap 2 REACHED, 3 deferred"*)
      # The census is OF THE EXAMINED SUBSET, so with the cap pinned to 2 exactly two
      # of the five aged candidates are probed (both unverifiable here) and the other
      # three are DEFERRED, unprobed — that is the #3637 job-116 bound, visible in the
      # line rather than asserted.
      ok "AC15: DEGRADED — the sweep line reports 0 REMOVED of 5 aged, 2 examined against a cap of 2 with 3 deferred, 0 removals ATTEMPTED and both examined candidates unverifiable ($sweep15)" ;;
    *) bad "AC15: DEGRADED — the sweep line does not report a capped, unverifiable examined subset with zero attempts (got: '${sweep15:-<none>}')" ;;
  esac
elif [ "$cap_planted" = 5 ]; then
  ok "AC15: precondition — all 5 aged candidates read verified-dead, so every one is a removal the sweep WOULD attempt"
else
  bad "AC15: precondition failed — only $cap_planted of 5 candidates read verified-dead; the cap accounting is not being measured"
fi
cap_rm_log="$tmp/cap-rm-attempts.log"
: >"$cap_rm_log"
cap_shim="$tmp/capshim"; mkdir -p "$cap_shim"
if [ "$OWNER_MARKER_CAPABLE" != 1 ]; then
  : # measured above, in the degradation arm
elif [ -n "${REAL_RM:-}" ] && [ -x "${REAL_RM:-}" ]; then
  cat >"$cap_shim/rm" <<SHIM
#!/usr/bin/env bash
# Scoped refusal + attempt log: only this case's planted candidates, only under its
# own scratch TMPDIR. Everything else — including the run's OWN log dir — is passed
# through to the real rm, so this case leaves no bundle of its own behind.
for a in "\$@"; do
  case "\$a" in
    "$td15"/agent-gate.CAP*)
      printf '%s\\n' "\$a" >>"$cap_rm_log"
      exit 1 ;;
  esac
done
exec "$REAL_RM" "\$@"
SHIM
  chmod +x "$cap_shim/rm"
  env -u AGENT_GATE_PARENT_RUN_ID \
      PATH="$cap_shim:$PATH" \
      TMPDIR="$td15" \
      AGENT_GATE_SUMMARY_FILE="$sf15" \
      AGENT_GATE_TEST_LITE_SCOPED=PASS \
      AGENT_GATE_TEST_LITE_RESULTS="file-size:PASS fmt:PASS clippy:PASS" \
      bash "$cap_gate" --lite-aggregate-selftest >"$td15.out" 2>&1
  cap_attempts=$(grep -c . "$cap_rm_log" 2>/dev/null || true)
  [ -n "$cap_attempts" ] || cap_attempts=0
  if [ "$cap_attempts" = 2 ]; then
    ok "AC15: the sweep ATTEMPTED exactly 2 removals — the pinned cap bounded the attempts, not the successes"
  else
    bad "AC15: the sweep attempted $cap_attempts removals against a cap of 2 (5 candidates, all failing) — the cap counts successes, not attempts"
  fi
  cap_survivors=0
  for n in 1 2 3 4 5; do
    [ -d "$td15/agent-gate.CAP00$n" ] && cap_survivors=$((cap_survivors + 1))
  done
  if [ "$cap_survivors" = 5 ]; then
    ok "AC15: precondition — every refused removal really did leave its directory in place"
  else
    bad "AC15: precondition failed — only $cap_survivors of 5 candidates survived the refusing shim; the attempt count is not measuring failed removals"
  fi
  sweep15=$(sed -n 's/^logdir-sweep: //p' "$sf15" | tail -1)
  case "$sweep15" in
    "0 REMOVED of 5 aged"*) ok "AC15: the sweep line reports 0 REMOVED of 5 aged, not a bare 0 ($sweep15)" ;;
    *) bad "AC15: the sweep line does not report the candidate census (got: '${sweep15:-<none>}')" ;;
  esac
  case "$sweep15" in
    *"removals attempted 2"*"cap 2 REACHED, 3 deferred"*)
      ok "AC15: the sweep line states the attempts AND that the cap was reached with the remainder deferred" ;;
    *) bad "AC15: the sweep line does not state the attempts and the deferred remainder (got: '${sweep15:-<none>}')" ;;
  esac
else
  bad "AC15: no real rm on PATH — cannot build the scoped shim, refusing to call that a pass"
fi

# ---------------------------------------------------------------------------
# AC16: the owner token covers the PID NAMESPACE, and a weaker token is cannot-tell.
# ---------------------------------------------------------------------------
# THE FAILURE THIS MEASURES (roborev job 111 medium 1): two containers can share a
# boot id AND a temp directory while having SEPARATE PID NAMESPACES. A token naming
# only the machine-and-boot therefore matched across the namespace boundary, the live
# peer's pid did not exist in OUR namespace, and its aged bundle read `verified-dead`
# — the sweep deleting a LIVE peer's evidence, the one outcome this liveness gate
# exists to prevent. Same for a hostname, which cannot be established as unique from
# inside a process and does not even change across a reboot.
#
# Probe-level on purpose: the identity lives in the token, so this asserts (a) that
# the marker the SHIPPED writer produces carries the namespace axis at all, and (b)
# that every WEAKER token the old code would have accepted now reads `cannot-tell`.
# The fixtures deliberately do NOT use the gate's `agent-gate.??????` name shape —
# nothing here runs a sweep, and a matching name would enrol them in the hermeticity
# accounting below for no reason.
_logdir_machine_token
ns_tok="$GATE_LOGDIR_MACHINE_TOKEN"
case "$ns_tok" in
  *';pidns='*)
    ok "AC16: the owner token this run writes carries a pid-namespace axis ($ns_tok)" ;;
  '')
    # CONSISTENT WITH THE CAPABILITY VERDICT ABOVE, in both directions: an empty token
    # on LINUX is a regression in the shipped probe and FAILs, exactly as the capability
    # verdict does, and only a non-Linux host may declare it a platform residual. The
    # earlier form said `ok` unconditionally, which is the self-contradiction roborev
    # job 114 named — this file treating the capability as optional here while AC5/AC15/
    # AC17 required it.
    if [ "$host_kind" = Linux ]; then
      bad "AC16: the owner token is EMPTY on a LINUX host ($host_kind) — boot_id and /proc/self/ns/pid both exist here, so the composite token has regressed and the sweep can verify nothing"
    else
      ok "AC16: DECLARED NOT EXERCISED on this non-Linux host ($host_kind) — no boot id and/or no readable /proc/self/ns/pid, so the token is EMPTY and every candidate reads cannot-tell; the sweep can remove nothing here (the fail-safe direction)"
    fi ;;
  *)
    bad "AC16: the owner token does not name the pid namespace (got '$ns_tok') — a live peer in another namespace reads verified-dead" ;;
esac

if [ "$OWNER_MARKER_CAPABLE" = 1 ]; then
  ns_dir="$tmp/nsfixture-dead"
  mkdir -p "$ns_dir"
  if plant_owner_marker "$ns_dir" dead; then
    ok "AC16: precondition — the fixture marker reads verified-dead with THIS run's own token"
  else
    bad "AC16: precondition failed — could not plant a verified-dead marker; the weakened-token cases below would measure nothing"
  fi
  # ns_retoken <machine-value> -> the probe's state for the fixture rewritten to carry
  # <machine-value>. Everything else — the reaped pid, the start token — is untouched,
  # so the machine token is the ONLY thing that can change the answer.
  ns_retoken() {
    sed -i.bak "s|^machine=.*|machine=$1|" "$ns_dir/$GATE_LOGDIR_OWNER_BASENAME" || return 1
    rm -f "$ns_dir/$GATE_LOGDIR_OWNER_BASENAME.bak"
    _logdir_owner_state "$ns_dir"
    printf '%s' "$GATE_LOGDIR_OWNER_STATE"
  }
  # A DIFFERENT pid namespace, same machine and boot: the container case verbatim.
  ns_foreign="${ns_tok%;pidns=*};pidns=1"
  got=$(ns_retoken "$ns_foreign")
  if [ "$got" = cannot-tell ]; then
    ok "AC16: a marker from ANOTHER PID NAMESPACE (same boot) reads cannot-tell, so its bundle is KEPT"
  else
    bad "AC16: a marker from another pid namespace reads '$got' — a live peer's bundle would be swept"
  fi
  # The pre-fix token shape: machine-and-boot with no namespace axis at all.
  got=$(ns_retoken "${ns_tok%%;*}")
  if [ "$got" = cannot-tell ]; then
    ok "AC16: the legacy boot-only token shape reads cannot-tell (the axis is REQUIRED, not optional)"
  else
    bad "AC16: a boot-only token reads '$got' — the namespace axis is being treated as optional"
  fi
  # The withdrawn hostname fallback.
  got=$(ns_retoken "host=$(uname -n 2>/dev/null || printf 'unknown')")
  if [ "$got" = cannot-tell ]; then
    ok "AC16: a hostname-only token reads cannot-tell (a shared hostname can never carry pid identity)"
  else
    bad "AC16: a hostname-only token reads '$got' — the withdrawn host= fallback still grants a match"
  fi
  # And the reverse direction: the axis being required must not have broken the LIVE
  # answer, or the sweep would keep everything for the wrong reason and this suite
  # would read green while proving nothing.
  ns_live="$tmp/nsfixture-live"
  mkdir -p "$ns_live"
  if plant_owner_marker "$ns_live" live; then
    ok "AC16: a marker written in THIS namespace by a LIVE process still reads live"
  else
    bad "AC16: a live marker no longer reads live under the composite token — the probe now answers cannot-tell for everything"
  fi
else
  # The weakened-token cases have no subject where there is no token to weaken. What
  # IS assertable is the degradation itself: the marker the shipped writer produces on
  # this host records `machine=unknown`, which can match nothing, so the probe answers
  # cannot-tell and the bundle is KEPT.
  ok "AC16: SKIPPED — no owner-marker capability on this host ($host_kind), so there is no composite token to weaken; the degradation is asserted below instead"
  ns_none="$tmp/nsfixture-notoken"
  mkdir -p "$ns_none"
  GATE_LOGDIR_CREATED="$ns_none"
  GATE_LOGDIR_OWNER_FILE="$ns_none/$GATE_LOGDIR_OWNER_BASENAME"
  _logdir_write_owner
  _logdir_owner_state "$ns_none"
  if [ "$GATE_LOGDIR_OWNER_STATE" = cannot-tell ]; then
    ok "AC16: DEGRADED — this host's own shipped marker reads cannot-tell, so no bundle is removable and none can be misclassified verified-dead"
  else
    bad "AC16: DEGRADED — a marker written with an unestablishable token reads '$GATE_LOGDIR_OWNER_STATE'; an empty token is granting a match"
  fi
fi

# ---------------------------------------------------------------------------
# AC17: successive capped sweeps examine DIFFERENT, PREDICTED windows.
# ---------------------------------------------------------------------------
# MEASURED WITH MORE CANDIDATES THAN THE CAP AND A DETERMINISTIC FAILING PREFIX
# (roborev job 111 medium 2) — the only configuration in which a fixed start position
# is distinguishable from a rotating one. `find`'s order is stable in practice and a
# directory whose removal fails stays eligible, so the pre-fix loop retried the SAME
# first <cap> entries on every sweep for ever and everything it reported as "deferred
# to the next run" was attempted by NO run: on a box carrying tens of thousands of
# directories, the whole tail past position <cap> leaked permanently. An
# all-succeeding population cannot see this, which is exactly why the cap fix that
# introduced it read correct.
#
# NOTHING HERE DEPENDS ON CHANCE (roborev job 117 medium). The first form of this case
# drove a $RANDOM start offset and asserted properties of the resulting sample — "an
# entry beyond the failing prefix was attempted", "every candidate was attempted at
# least once" — which are PROBABILISTIC assertions, i.e. a flake generator inside a
# registered `tooling-tests` case that every lane's gate of record eventually eats.
# The offset is now DERIVED from the run-id, so each sweep's window is PREDICTABLE:
# every sweep is given an explicit simulated run-id and asserted to have examined
# EXACTLY the window the SHIPPED `_logdir_sweep_start_offset` computes for it —
# delegated to, never reimplemented — over the candidate order read from `find` in that
# same state. Same run-ids, same verdict, every run, on any box.
#
# WHAT THIS CASE DOES NOT ESTABLISH, stated here because the assertions below are
# easy to over-read (roborev job 118 medium). It does NOT establish eventual coverage,
# and no test here can: the offset is a function of the run-id, the run-id IS a random
# `mktemp` suffix, and distinct run-ids can therefore map to the SAME offset, so
# repeated or overlapping windows are possible and a deferred candidate CAN be starved
# indefinitely. That residual is ACCEPTED AND DECLARED — the eventual-coverage
# guarantee would need a persisted cross-process cursor under a lock, whose failure
# modes are worse than the property it buys (the ORDER in which stale temp directories
# are reclaimed), so it is deliberately not bought. Three things ARE asserted, and each
# is worded as exactly what it is: the WORK BOUND holds for EVERY sweep (universally
# true, and the property that matters most, since it is what keeps the sweep bounded on
# a box carrying tens of thousands of directories); differing run-ids yield DIFFERING
# start offsets (so no window is structurally privileged, which is the starvation
# defect job 111 medium 2 actually found); and ONE ENUMERATED run-id sequence produces
# ONE ENUMERATED set of examined windows, whose union happens to be the whole fixture.
# The third is a demonstration about that sequence, never a coverage guarantee.
#
# The failing subset is chosen from `find`'s OWN order, read in this case, rather than
# assumed to be the lexicographic first three: readdir order is not sorted, so a
# name-based guess would not be the prefix the loop actually walks.
#
# Driven in a SEPARATE bash process: the case has to override _logdir_rm_guarded, and
# an override in this shell would silently follow every later case. The driver sources
# the SHIPPED slab (owner probe + guard + offset + sweep) and reuses this file's own
# plant_owner_marker through an exported function, so nothing is reimplemented.
rot_parent="$tmp/rot/parent"
mkdir -p "$rot_parent"
rot_lib="$tmp/rot/sweep-lib.sh"
awk '/^GATE_LOGDIR_OWNER_BASENAME=/,/^# The sweep is INVOKED further down/' "$GATE" \
  | sed '$d' >"$rot_lib"
if grep -q '^_logdir_sweep() {' "$rot_lib" \
   && grep -q '^_logdir_rm_guarded() {' "$rot_lib" \
   && grep -q '^_logdir_owner_state() {' "$rot_lib" \
   && grep -q '^_logdir_sweep_start_offset() {' "$rot_lib"; then
  ok "AC17: the sweep, the removal guard, the owner probe and the rotation-offset function were extracted from the shipped gate"
else
  bad "AC17: could not extract the sweep from the shipped gate — this case would measure nothing"
fi
# STRUCTURAL, and the reason it is here rather than left to the behavioural cases: a
# reintroduced $RANDOM draw would still PASS the window assertions below on the sweeps
# where the draw happened to land on the predicted window, i.e. it would fail
# intermittently instead of failing. The rotation start must come from the derived
# offset and from nothing else, so no executable line of the shipped slab may read
# $RANDOM (the comment that explains why it does not is excluded by the `^[[:space:]]*#`
# filter, not by matching its text).
if grep -vE '^[[:space:]]*#' "$rot_lib" | grep -q 'RANDOM'; then
  bad "AC17: the shipped sweep slab still reads \$RANDOM on an executable line — a probabilistic start offset makes both the mechanism and this case non-deterministic"
else
  ok "AC17: no executable line of the shipped sweep slab reads \$RANDOM — the rotation start is derived, not drawn"
fi
if grep -q '_logdir_sweep_start_offset "\${GATE_LOGDIR_CREATED:-}"' "$rot_lib"; then
  ok "AC17: the sweep takes its rotation start from the run-id (GATE_LOGDIR_CREATED, the value the SUMMARY stamps as run-id:), so a pasted block explains the offset it used"
else
  bad "AC17: the sweep does not derive its rotation start from GATE_LOGDIR_CREATED — the offset is then unexplainable from the SUMMARY"
fi
rot_driver="$tmp/rot/driver.sh"
rot_out="$tmp/rot/driver.tsv"
cat >"$rot_driver" <<'ROTEOF'
#!/usr/bin/env bash
# Argv: <sweep-lib> <parent> <cap> <sweeps>
set -uo pipefail
lib="$1"; parent="$2"; cap="$3"; sweeps="$4"
say() { printf '%s\t%s\n' "$1" "$2"; }
# shellcheck disable=SC1090
. "$lib"
GATE_LOGDIR_PARENT="$parent"
GATE_LOGDIR_SWEEP_CAP="$cap"
GATE_LOGDIR_SWEEP_AGE_DAYS=7
planted=0
for n in 1 2 3 4 5; do
  d="$parent/agent-gate.ROT00$n"
  mkdir -p "$d" || continue
  if plant_owner_marker "$d" dead; then planted=$((planted + 1)); fi
  # THE one mtime boundary, exported into this driver (#3637, roborev job 175 finding 3).
  _age_dir_apply "$d" || say BAD "AC17: could not synthesise an aged mtime for $d — that candidate is not aged, so the sweep never sees it and the rotation accounting below is short one subject"
done
if [ "$planted" = 5 ]; then
  say OK "AC17: precondition — all 5 aged candidates read verified-dead, so each is a removal the sweep WOULD attempt"
else
  say BAD "AC17: precondition failed — only $planted of 5 candidates read verified-dead"
fi
# The failing prefix, in find's own order.
order=$(find "$parent" -maxdepth 1 -type d -name 'agent-gate.*' -mtime +7 2>/dev/null | sed 's#.*/##')
set -- $order
if [ "$#" = 5 ]; then
  say OK "AC17: precondition — 5 aged candidates against a cap of $cap, so the cap must defer part of the population"
else
  say BAD "AC17: precondition failed — find listed $# aged candidates, not 5"
fi
FAILSET=" $1 $2 $3 "
TAILSET=" $4 $5 "

# The simulated per-run identity. `RID%03d` is six alphanumerics after `agent-gate.`,
# i.e. exactly the shape `mktemp -d …XXXXXX` produces, and it is a NAME only: nothing
# is created under it, so a simulated run-id can never join the candidate population.
run_id() { printf '%s/agent-gate.RID%03d' "$parent" "$1"; }

# The candidate order as `find` lists it RIGHT NOW, re-read before every sweep so the
# expectation is against the order that sweep's own `find` will see — no cross-sweep
# stability assumption is needed anywhere in this case.
read_order() {
  CAND=()
  local b
  while IFS= read -r b; do
    [ -n "$b" ] && CAND+=("$b")
  done < <(find "$parent" -maxdepth 1 -type d -name 'agent-gate.*' -mtime +7 2>/dev/null | sed 's#.*/##')
}
# The window a run-id MUST examine: the SHIPPED offset function's answer for that
# run-id and that population size, walked circularly for <cap> entries. Delegated to,
# never reimplemented — an expectation computed by a second implementation of the hash
# would only be testing that second implementation.
predicted_window() {   # <run-id>
  local rid="$1" total="${#CAND[@]}" k=0 idx
  _logdir_sweep_start_offset "$rid" "$total"
  PRED_START="$GATE_LOGDIR_SWEEP_OFFSET"
  PRED=()
  while [ "$k" -lt "$cap" ] && [ "$k" -lt "$total" ]; do
    idx=$(( (PRED_START + k) % total ))
    PRED+=("${CAND[$idx]}")
    k=$((k + 1))
  done
}
setstr() { printf '%s\n' "$@" | LC_ALL=C sort | tr '\n' ' '; }

# ---- PHASE 1: exact windows, over a population NOTHING can remove ----------------
# The stub REFUSES every removal, so the population and its order are constant across
# the phase and each sweep's window is a pure function of its run-id. This is also the
# real-box shape: a directory the box cannot unlink stays eligible for ever.
ATT=""
_logdir_rm_guarded() { ATT="$ATT ${1##*/}"; return 1; }
window_mismatch=0
overrun=0
UNION=""
STARTS=""
first_set=""
sweep=0
while [ "$sweep" -lt "$sweeps" ]; do
  sweep=$((sweep + 1))
  read_order
  GATE_LOGDIR_CREATED=$(run_id "$sweep")
  predicted_window "$GATE_LOGDIR_CREATED"
  ATT=""
  _logdir_sweep
  got=$(setstr $ATT)
  want=$(setstr "${PRED[@]}")
  [ "$got" = "$want" ] || window_mismatch=$((window_mismatch + 1))
  set -- $ATT
  [ "$#" -gt "$cap" ] && overrun=$((overrun + 1))
  [ "$sweep" = 1 ] && first_set="$got"
  UNION="$UNION $ATT"
  case " $STARTS " in *" $PRED_START "*) ;; *) STARTS="$STARTS $PRED_START" ;; esac
done
if [ "$window_mismatch" = 0 ]; then
  say OK "AC17: all $sweeps sweeps examined EXACTLY the window the shipped offset function predicts for their run-id — the rotation is deterministic, not sampled"
else
  say BAD "AC17: $window_mismatch of $sweeps sweeps examined a window other than the one predicted from their run-id"
fi
if [ "$overrun" = 0 ]; then
  say OK "AC17: EVERY one of the $sweeps sweeps examined at most the cap — the work bound is universal, and it survived the rotation"
else
  say BAD "AC17: $overrun of $sweeps sweeps attempted more than $cap removals — the rotation broke the work bound"
fi
# Determinism, asserted as REPEATABILITY of the mechanism and not of the arithmetic:
# the SAME run-id over the same state must examine the SAME window.
read_order
GATE_LOGDIR_CREATED=$(run_id 1)
ATT=""
_logdir_sweep
repeat_set=$(setstr $ATT)
if [ -n "$first_set" ] && [ "$repeat_set" = "$first_set" ]; then
  say OK "AC17: re-running the FIRST run-id examined the identical window — the same run-id always sweeps the same subset, so a sweep is reproducible after the fact"
else
  say BAD "AC17: the same run-id examined two different windows ('$first_set' then '$repeat_set') — the offset is not a function of the run-id"
fi
set -- $STARTS
if [ "$#" -ge 2 ]; then
  say OK "AC17: these $sweeps differing run-ids produced $# DISTINCT start offsets, so runs with differing run-ids do start at different places and no window is structurally privileged (offsets:$STARTS) — differing offsets, NOT guaranteed-disjoint ones: distinct run-ids can collide on one offset"
else
  say BAD "AC17: all $sweeps run-ids produced the same start offset ($STARTS) — successive runs would re-examine one window for ever"
fi
uncovered=""
for n in 1 2 3 4 5; do
  case " $UNION " in
    *" agent-gate.ROT00$n "*) ;;
    *) uncovered="$uncovered agent-gate.ROT00$n" ;;
  esac
done
if [ -z "$uncovered" ]; then
  say OK "AC17: this ENUMERATED sequence of $sweeps run-ids examined all 5 candidates, the tail past the always-failing prefix included — evidence about THIS sequence ONLY, and NOT a coverage guarantee: offsets derive from a random mktemp suffix, so distinct run-ids can collide on one offset and a deferred candidate can be starved indefinitely — an accepted, declared residual, not something this or any case tests away"
else
  say BAD "AC17: this enumerated sequence of $sweeps run-ids never examined:$uncovered — its windows no longer reach every position, so the sequence has stopped demonstrating what it was chosen to demonstrate (the offset function or find's order moved; re-derive the sequence)"
fi
case "$GATE_LOGDIR_SWEEP_LINE" in
  *"rotation start "*" of "*"(derived from run-id suffix "*")"*)
    say OK "AC17: the sweep line reports its rotation start AND the run-id suffix it was derived from, so the offset is explainable from a pasted SUMMARY" ;;
  *)
    say BAD "AC17: the sweep line does not report the rotation start and its provenance (got: '$GATE_LOGDIR_SWEEP_LINE')" ;;
esac

# ---- PHASE 2: real removals, over the same failing prefix -------------------------
# Phase 1 proves which candidates are EXAMINED; this proves the examination converts
# into removals for the entries the guard allows, so "examined" above is not measuring
# a no-op. Deterministic in exactly the same way: the same fixed run-id sequence.
ATT=""
_logdir_rm_guarded() {
  local base="${1##*/}"
  ATT="$ATT $base"
  case "$FAILSET" in *" $base "*) return 1 ;; esac
  rm -rf "$1" 2>/dev/null
  [ -d "$1" ] && return 1
  return 0
}
sweep=0
while [ "$sweep" -lt "$sweeps" ]; do
  sweep=$((sweep + 1))
  GATE_LOGDIR_CREATED=$(run_id "$sweep")
  _logdir_sweep
done
tail_gone=1
for t in $TAILSET; do
  [ -d "$parent/$t" ] && tail_gone=0
done
if [ "$tail_gone" = 1 ]; then
  say OK "AC17: under this enumerated run-id sequence both entries BEYOND the always-failing prefix were reached and REMOVED, so an examined candidate really is removed and PHASE 1's 'examined' is not measuring a no-op — a demonstration for this sequence, not a no-starvation guarantee"
else
  say BAD "AC17: an entry beyond the always-failing prefix survived this enumerated sequence of $sweeps sweeps — examination did not convert into removal for the tail of this fixture"
fi
# Only the refused prefix may survive. Asserted as SET EQUALITY against the prefix the
# stub refuses, so the parent's hermeticity accounting can take this list as proven
# rather than as "whatever happened to be left".
survivors=""
surv_n=0
for n in 1 2 3 4 5; do
  if [ -d "$parent/agent-gate.ROT00$n" ]; then
    survivors="$survivors agent-gate.ROT00$n"
    case "$FAILSET" in
      *" agent-gate.ROT00$n "*) surv_n=$((surv_n + 1)) ;;
      *) surv_n=99 ;;
    esac
  fi
done
if [ "$surv_n" = 3 ]; then
  say OK "AC17: the survivors are EXACTLY the three refused candidates — the two the stub allowed really were removed"
else
  say BAD "AC17: the surviving set is not the refused prefix (survivors:$survivors, refused:$FAILSET)"
fi
say INFO "survivors:$survivors"
exit 0
ROTEOF
export -f plant_owner_marker
if [ "$OWNER_MARKER_CAPABLE" != 1 ]; then
  # SKIPPED BY NAME: rotation is only observable across candidates the sweep would
  # ATTEMPT, and on a host with no establishable owner token there are none. The
  # degradation asserted in its place is the property that decides whether that is
  # safe — a capped sweep over an aged population attempts NOTHING and takes NOTHING.
  ok "AC17: SKIPPED — no owner-marker capability on this host ($host_kind), so no candidate reaches verified-dead and rotation across the population has no subject; the degradation is asserted below instead"
  rot_deg_out="$tmp/rot/degraded.txt"
  for n in 1 2 3 4 5; do
    d="$rot_parent/agent-gate.ROT00$n"
    mkdir -p "$d"
    age_dir "$d" AC17
  done
  if bash -c '
      set -uo pipefail
      # shellcheck disable=SC1090
      . "$1"
      GATE_LOGDIR_PARENT="$2"
      GATE_LOGDIR_SWEEP_CAP=2
      GATE_LOGDIR_SWEEP_AGE_DAYS=7
      GATE_LOGDIR_CREATED="$2/agent-gate.DEG001"
      _logdir_sweep
      printf "%s\n" "$GATE_LOGDIR_SWEEP_LINE"
    ' _ "$rot_lib" "$rot_parent" >"$rot_deg_out" 2>&1; then
    rot_deg_line=$(sed -n 's/^logdir-sweep: //p' "$rot_deg_out" | tail -1)
    case "$rot_deg_line" in
      "0 REMOVED of 5 aged"*"verified-dead 0"*"unverifiable 2"*"examined 2, removals attempted 0"*)
        ok "AC17: DEGRADED — a capped sweep over 5 aged candidates examined 2 (the cap), attempted 0 removals and took none ($rot_deg_line)" ;;
      *) bad "AC17: DEGRADED — the sweep line does not report a capped, unverifiable examined subset with zero attempts (got: '${rot_deg_line:-<none>}')" ;;
    esac
    rot_deg_survivors=0
    rot_survivors=""
    for n in 1 2 3 4 5; do
      if [ -d "$rot_parent/agent-gate.ROT00$n" ]; then
        rot_deg_survivors=$((rot_deg_survivors + 1))
        rot_survivors="$rot_survivors agent-gate.ROT00$n"
      fi
    done
    if [ "$rot_deg_survivors" = 5 ]; then
      ok "AC17: DEGRADED — every one of the 5 aged candidates survived, so cannot-tell kept the whole population"
    else
      bad "AC17: DEGRADED — only $rot_deg_survivors of 5 aged candidates survived; the sweep removed a bundle whose owner was never verified"
    fi
  else
    bad "AC17: DEGRADED — the extracted sweep did not run to completion — UNMEASURED, not a pass"
    sed -n '1,20p' "$rot_deg_out"
    rot_survivors=""
    for n in 1 2 3 4 5; do
      [ -d "$rot_parent/agent-gate.ROT00$n" ] && rot_survivors="$rot_survivors agent-gate.ROT00$n"
    done
  fi
elif bash "$rot_driver" "$rot_lib" "$rot_parent" 2 40 >"$rot_out" 2>"$rot_out.err"; then
  rot_lines=0
  while IFS=$'\t' read -r verdict msg; do
    case "$verdict" in
      OK)  ok "$msg"; rot_lines=$((rot_lines + 1)) ;;
      BAD) bad "$msg"; rot_lines=$((rot_lines + 1)) ;;
      INFO) rot_survivors="${msg#survivors:}" ;;
    esac
  done <"$rot_out"
  # CASE FLOOR (#3544's lesson): a driver that silently stopped after two verdicts
  # would otherwise report a green subset of this case.
  if [ "$rot_lines" -ge 9 ]; then
    ok "AC17: the driver reported all $rot_lines of its verdicts (case floor 9)"
  else
    bad "AC17: the driver reported only $rot_lines verdicts (case floor 9) — a truncated case, not a pass"
    sed -n '1,20p' "$rot_out.err"
  fi
else
  bad "AC17: the rotation driver did not run to completion — UNMEASURED, not a pass"
  sed -n '1,20p' "$rot_out.err"
fi

# ---------------------------------------------------------------------------
# AC18: the cap bounds the candidates EXAMINED, over a population where NOTHING
#       is removable.
# ---------------------------------------------------------------------------
# MEASURED ALL-MARKERLESS (roborev job 116 medium), which is the only configuration
# that can see this defect and is also the configuration every real box is in: a
# directory created before the owner marker existed reads `cannot-tell` for ever, so it
# increments neither `removed` nor `attempted` and, under the previous cap, was PROBED
# BY EVERY GATE START FOR EVER while the line advertised a bound of 1000. A population
# in which candidates are removable cannot distinguish the two accountings, because
# there examined == attempted — which is exactly why the preceding cap fix read correct.
#
# Both halves are asserted, and NEITHER depends on chance (roborev job 117 medium): the
# per-run examination count is CAPPED, and each run examines EXACTLY the window the
# shipped `_logdir_sweep_start_offset` predicts for its own run-id, so successive runs
# provably examine different subsets instead of one permanently re-probed prefix
# (#3637's rotation property one counter down). The earlier form asserted that SOME
# pair of sampled sweeps differed, which is a probabilistic claim about a $RANDOM draw.
# Probes are counted by WRAPPING the shipped `_logdir_owner_state` — extracted from the
# gate, delegated to, never reimplemented — so the count is of the real probe the sweep
# performs.
#
# NO CAPABILITY GATE, deliberately: `cannot-tell` for a MISSING marker is decided
# before any /proc read (the probe returns at the `[ -f ]` test), so this case measures
# identically on every platform and needs no planted marker.
exam_parent="$tmp/exam/parent"
mkdir -p "$exam_parent"
exam_driver="$tmp/exam/driver.sh"
exam_out="$tmp/exam/driver.tsv"
EXAM_POP=12
EXAM_CAP=3
EXAM_SWEEPS=60
cat >"$exam_driver" <<'EXMEOF'
#!/usr/bin/env bash
# Argv: <sweep-lib> <parent> <population> <cap> <sweeps> <probe-log>
set -uo pipefail
lib="$1"; parent="$2"; pop="$3"; cap="$4"; sweeps="$5"; plog="$6"
say() { printf '%s\t%s\n' "$1" "$2"; }
# shellcheck disable=SC1090
. "$lib"
GATE_LOGDIR_PARENT="$parent"
GATE_LOGDIR_SWEEP_CAP="$cap"
GATE_LOGDIR_SWEEP_AGE_DAYS=7
n=0
while [ "$n" -lt "$pop" ]; do
  n=$((n + 1))
  d=$(printf '%s/agent-gate.EXM%03d' "$parent" "$n")
  mkdir -p "$d" || continue
  # NO owner marker: this is the pre-#3637 legacy shape, verbatim.
  _age_dir_apply "$d" || say BAD "AC18: could not synthesise an aged mtime for $d — that candidate is not aged, so the examination accounting below is short one subject"
done
# The shipped probe, renamed, then wrapped: the wrapper LOGS the candidate and
# DELEGATES, so what is counted is the real probe and its real answer.
eval "_shipped_owner_state() $(declare -f _logdir_owner_state | tail -n +2)"
_logdir_owner_state() {
  printf '%s\n' "${1##*/}" >>"$plog"
  _shipped_owner_state "$@"
}
# Precondition: every candidate is cannot-tell, i.e. NOTHING here is removable. Read
# through the shipped probe, not asserted from the absence of a marker file.
unk=0
n=0
while [ "$n" -lt "$pop" ]; do
  n=$((n + 1))
  d=$(printf '%s/agent-gate.EXM%03d' "$parent" "$n")
  _shipped_owner_state "$d"
  [ "$GATE_LOGDIR_OWNER_STATE" = cannot-tell ] && unk=$((unk + 1))
done
if [ "$unk" = "$pop" ]; then
  say OK "AC18: precondition — all $pop markerless aged candidates read cannot-tell, so NONE is removable and only the examination count can be bounded"
else
  say BAD "AC18: precondition failed — only $unk of $pop markerless candidates read cannot-tell; the case is not measuring an unremovable population"
fi
# Any removal at all would mean the probe's cannot-tell took the permissive branch.
rm_attempts=0
_logdir_rm_guarded() { rm_attempts=$((rm_attempts + 1)); return 1; }
# The simulated per-run identity: six alphanumerics after `agent-gate.`, the exact
# shape mktemp -d produces, and a NAME only — nothing is created under it.
run_id() { printf '%s/agent-gate.RUN%03d' "$parent" "$1"; }
read_order() {
  CAND=()
  local b
  while IFS= read -r b; do
    [ -n "$b" ] && CAND+=("$b")
  done < <(find "$parent" -maxdepth 1 -type d -name 'agent-gate.*' -mtime +7 2>/dev/null | sed 's#.*/##')
}
predicted_window() {   # <run-id>
  local rid="$1" total="${#CAND[@]}" k=0 idx
  _logdir_sweep_start_offset "$rid" "$total"
  PRED_START="$GATE_LOGDIR_SWEEP_OFFSET"
  PRED=()
  while [ "$k" -lt "$cap" ] && [ "$k" -lt "$total" ]; do
    idx=$(( (PRED_START + k) % total ))
    PRED+=("${CAND[$idx]}")
    k=$((k + 1))
  done
}
setstr() { printf '%s\n' "$@" | LC_ALL=C sort | tr '\n' ' '; }
union="$plog.union"
: >"$union"
sweep=0
min_probed=-1
max_probed=-1
window_mismatch=0
STARTS=""
reported_mismatch=0
while [ "$sweep" -lt "$sweeps" ]; do
  sweep=$((sweep + 1))
  : >"$plog"
  read_order
  GATE_LOGDIR_CREATED=$(run_id "$sweep")
  predicted_window "$GATE_LOGDIR_CREATED"
  _logdir_sweep
  probed=$(grep -c . "$plog" 2>/dev/null || printf 0)
  cat "$plog" >>"$union"
  [ "$min_probed" = -1 ] && min_probed="$probed"
  [ "$probed" -lt "$min_probed" ] && min_probed="$probed"
  [ "$probed" -gt "$max_probed" ] && max_probed="$probed"
  # The line must REPORT the number actually probed — an advertised bound nobody can
  # check against the work done is the thing this fix replaces.
  case "$GATE_LOGDIR_SWEEP_LINE" in
    *"examined $probed,"*) ;;
    *) reported_mismatch=$((reported_mismatch + 1)) ;;
  esac
  got=$(LC_ALL=C sort "$plog" 2>/dev/null | tr '\n' ' ')
  want=$(setstr "${PRED[@]}")
  [ "$got" = "$want" ] || window_mismatch=$((window_mismatch + 1))
  case " $STARTS " in *" $PRED_START "*) ;; *) STARTS="$STARTS $PRED_START" ;; esac
done
if [ "$max_probed" = "$cap" ] && [ "$min_probed" = "$cap" ]; then
  say OK "AC18: every one of the $sweeps sweeps EXAMINED exactly $cap of the $pop candidates — the cap bounds the probes, not merely the removals"
else
  say BAD "AC18: sweeps examined between $min_probed and $max_probed of $pop candidates against a cap of $cap — an unremovable population is probed unbounded (the whole finding)"
fi
if [ "$reported_mismatch" = 0 ]; then
  say OK "AC18: every sweep's line reported the examined count it actually performed, so the bound is OBSERVABLE in the SUMMARY rather than asserted"
else
  say BAD "AC18: $reported_mismatch of $sweeps sweep lines reported an examined count other than the probes performed (last line: '$GATE_LOGDIR_SWEEP_LINE')"
fi
if [ "$window_mismatch" = 0 ]; then
  say OK "AC18: every sweep examined EXACTLY the window the shipped offset function predicts for its run-id — the bound does not convert into one re-probed prefix, and the case says so deterministically rather than by sampling"
else
  say BAD "AC18: $window_mismatch of $sweeps sweeps examined a subset other than the window predicted from their run-id"
fi
set -- $STARTS
if [ "$#" -ge 2 ]; then
  say OK "AC18: the $sweeps run-ids produced $# DISTINCT start offsets, so successive runs examine different subsets (offsets:$STARTS)"
else
  say BAD "AC18: all $sweeps run-ids produced the same start offset ($STARTS) — capped examination would starve the rest of the population for ever"
fi
covered=0
uncovered=""
n=0
while [ "$n" -lt "$pop" ]; do
  n=$((n + 1))
  b=$(printf 'agent-gate.EXM%03d' "$n")
  if grep -Fxq "$b" "$union" 2>/dev/null; then
    covered=$((covered + 1))
  else
    uncovered="$uncovered $b"
  fi
done
if [ "$covered" = "$pop" ]; then
  say OK "AC18: across this ENUMERATED sequence of $sweeps capped sweeps all $pop candidates were examined at least once ($covered > cap $cap), so the bound does not confine examination to one prefix — evidence about THIS sequence only, NOT eventual coverage: colliding offsets can defer a candidate indefinitely (the same declared residual AC17 names)"
else
  say BAD "AC18: only $covered of $pop candidates were ever examined across $sweeps sweeps; never examined:$uncovered"
fi
if [ "$rm_attempts" = 0 ]; then
  say OK "AC18: no removal was attempted at all — cannot-tell stayed non-permissive for every examined candidate"
else
  say BAD "AC18: $rm_attempts removals were attempted against a population in which every owner is UNVERIFIABLE"
fi
survivors=0
n=0
while [ "$n" -lt "$pop" ]; do
  n=$((n + 1))
  d=$(printf '%s/agent-gate.EXM%03d' "$parent" "$n")
  [ -d "$d" ] && survivors=$((survivors + 1))
done
if [ "$survivors" = "$pop" ]; then
  say OK "AC18: all $pop markerless candidates survived — the sweep does NOT reclaim the pre-marker backlog, the residual this change declares rather than guesses at"
else
  say BAD "AC18: only $survivors of $pop markerless candidates survived; a directory whose owner was never verified was destroyed"
fi
case "$GATE_LOGDIR_SWEEP_LINE" in
  *"unverifiable $cap; examined $cap, removals attempted 0, "*"cap $cap REACHED, "*" deferred to the next run; rotation start "*"(derived from run-id suffix "*")"*)
    say OK "AC18: the sweep line's census is of the EXAMINED SUBSET and names the deferred remainder, the rotation start and the run-id it was derived from ($GATE_LOGDIR_SWEEP_LINE)" ;;
  *)
    say BAD "AC18: the sweep line does not report a capped examined subset with its deferred remainder and derived rotation start (got: '$GATE_LOGDIR_SWEEP_LINE')" ;;
esac
exit 0
EXMEOF
if bash "$exam_driver" "$rot_lib" "$exam_parent" "$EXAM_POP" "$EXAM_CAP" "$EXAM_SWEEPS" \
     "$tmp/exam/probes.log" >"$exam_out" 2>"$exam_out.err"; then
  exam_lines=0
  while IFS=$'\t' read -r verdict msg; do
    case "$verdict" in
      OK)  ok "$msg"; exam_lines=$((exam_lines + 1)) ;;
      BAD) bad "$msg"; exam_lines=$((exam_lines + 1)) ;;
    esac
  done <"$exam_out"
  # CASE FLOOR (#3544's lesson): a driver that stopped early would otherwise report a
  # green subset of this case.
  if [ "$exam_lines" -ge 9 ]; then
    ok "AC18: the driver reported all $exam_lines of its verdicts (case floor 9)"
  else
    bad "AC18: the driver reported only $exam_lines verdicts (case floor 9) — a truncated case, not a pass"
    sed -n '1,20p' "$exam_out.err"
  fi
else
  bad "AC18: the examination-bound driver did not run to completion — UNMEASURED, not a pass"
  sed -n '1,20p' "$exam_out.err"
fi

# ---------------------------------------------------------------------------
# AC19: the aged scan is BOUNDED in what it materializes — INCLUDING when the
#       population GROWS between its two passes, the box's normal state — and its
#       outcome is read THREE-VALUED: a FAILED scan and an UNOBSERVABLE status each
#       remove nothing.
# ---------------------------------------------------------------------------
# The sweep used to assign `find`'s whole output to a shell variable and build a shell
# array of EVERY match before applying the cap, so a box carrying ~7,000 stale
# directories built a ~7,000-element bash array on every gate start — nested gates
# included — for a run that would examine at most <cap> of them (#3637, roborev job 121
# medium, half 1). The selection now happens INSIDE the pipeline, which moves the
# hazard this case exists for: a pipeline's `$?` is its LAST stage's, so find's own exit
# status is no longer directly observable, and a lost status is INDISTINGUISHABLE from
# an empty listing — #1699's find-tristate defect, in the one place where the
# permissive branch would start deleting from a directory the box could not read.
# `find … | head -n <cap>` is worse than lost: it SIGPIPEs find, so a healthy large
# population and an unreadable parent report the SAME non-zero status.
#
# So the status is carried IN BAND on a marker record, and all three answers are
# measured here: MEASURED (the positive control, so the case can see attempts at all),
# find FAILED, and status UNOBSERVABLE. Each of the two non-permissive answers must
# attempt ZERO removals over a population every other input says is removable — the
# only configuration in which a permissive branch is visible.
#
# NO CAPABILITY GATE, deliberately: the driver overrides the shipped owner probe with a
# constant `verified-dead`, so the case measures identically on every platform and its
# subject is the scan, not the liveness half (which AC5/AC15/AC16 own).
#
# NOTHING HERE DEPENDS ON CHANCE: fixed population, fixed cap, fixed run-id, shims that
# always answer the same way.
scan_parent="$tmp/scan/parent"
scan_empty="$tmp/scan/empty"
scan_shim="$tmp/scan/shim"
mkdir -p "$scan_parent" "$scan_empty" "$scan_shim"
scan_driver="$tmp/scan/driver.sh"
scan_out="$tmp/scan/driver.tsv"
SCAN_POP=4
SCAN_CAP=3
SCAN_BIG=5000
# The growing-population pair: $SCAN_GROW_LISTED is four whole blocks of
# $SCAN_GROW_COUNTED, so the pre-fix modulo repeated its window three extra times and
# emitted 4 x cap records where the cap advertised cap.
SCAN_GROW_COUNTED=6
SCAN_GROW_LISTED=24
cat >"$scan_driver" <<'SCNEOF'
#!/usr/bin/env bash
# Argv: <sweep-lib> <parent> <empty-parent> <shim-dir> <population> <cap> <big> \
#       <grow-counted> <grow-listed>
set -uo pipefail
lib="$1"; parent="$2"; empty="$3"; shim="$4"; pop="$5"; cap="$6"; big="$7"
counted="$8"; listed="$9"
say() { printf '%s\t%s\n' "$1" "$2"; }
# shellcheck disable=SC1090
. "$lib"
GATE_LOGDIR_PARENT="$parent"
GATE_LOGDIR_SWEEP_CAP="$cap"
GATE_LOGDIR_SWEEP_AGE_DAYS=7
GATE_LOGDIR_CREATED="$parent/agent-gate.SCN999"
n=0
while [ "$n" -lt "$pop" ]; do
  n=$((n + 1))
  d=$(printf '%s/agent-gate.SCN%03d' "$parent" "$n")
  mkdir -p "$d" || continue
  _age_dir_apply "$d" || say BAD "AC19: could not synthesise an aged mtime for $d — that candidate is not aged, so the scan accounting below is short one subject"
done
# The owner half is not this case's subject: a constant verified-dead makes every
# candidate one the sweep WOULD remove, which is what makes a permissive branch
# visible at all. Removals are REFUSED and COUNTED, so the population is stable across
# every sub-case and `attempts` is the signal.
# The IDENTITY is published too, exactly as the shipped probe does for every definite
# verdict: the sweep re-confirms it at the removal site, and a stub that omitted it
# would make every candidate DECLINE there — this case would then measure a removal
# path it never reached (#3637, roborev job 132).
_logdir_owner_state() { GATE_LOGDIR_OWNER_STATE=verified-dead; GATE_LOGDIR_OWNER_IDENT="pid=1;machine=scn-fixture;pid-start=proc-starttime=1"; }
ATTEMPTS=0
_logdir_rm_guarded() { ATTEMPTS=$((ATTEMPTS + 1)); return 1; }

# ---- 1: POSITIVE CONTROL — a real find over a real population -------------------
# Without it, "zero removals attempted" below would be satisfied by a sweep that never
# attempts anything for an unrelated reason, and every UNMEASURED assertion would pass
# for the wrong reason.
ATTEMPTS=0
_logdir_sweep
ctl_line="$GATE_LOGDIR_SWEEP_LINE"
ctl_attempts="$ATTEMPTS"
case "$ctl_line" in
  *"0 REMOVED of $pop aged"*"examined $cap, removals attempted $cap"*)
    say OK "AC19: POSITIVE CONTROL — a MEASURED scan over $pop aged candidates examined the cap ($cap) and ATTEMPTED $ctl_attempts removals, so this case can see the permissive branch it asserts against below ($ctl_line)" ;;
  *)
    say BAD "AC19: POSITIVE CONTROL failed — a measured scan did not examine the cap and attempt its removals (attempts=$ctl_attempts, line: '$ctl_line')" ;;
esac

# ---- 2: find FAILS, WITH OUTPUT --------------------------------------------------
# The shim lists real candidate paths and exits 1, i.e. exactly the partial-listing
# shape: the paths are valid and removable, and the ONLY thing wrong is the status.
cat >"$shim/find" <<SHIM
#!/usr/bin/env bash
n=0
while [ "\$n" -lt $pop ]; do
  n=\$((n + 1))
  printf '%s/agent-gate.SCN%03d\n' "$parent" "\$n"
done
exit 1
SHIM
chmod +x "$shim/find"
ATTEMPTS=0
real_path="$PATH"; PATH="$shim:$PATH"; _logdir_sweep; PATH="$real_path"
fail_line="$GATE_LOGDIR_SWEEP_LINE"
fail_attempts="$ATTEMPTS"
case "$fail_line" in
  *"UNMEASURED (find rc=1 under $parent)"*)
    say OK "AC19: a FAILED scan that still printed a full, removable listing reports UNMEASURED and NAMES find's status — a failed scan is not an empty one ($fail_line)" ;;
  *)
    say BAD "AC19: a failed scan did not report UNMEASURED with find's status (got: '$fail_line')" ;;
esac
if [ "$fail_attempts" = 0 ]; then
  say OK "AC19: the failed scan attempted ZERO removals over a listing every other input says is removable — the status observation survived the move into the pipeline"
else
  say BAD "AC19: the failed scan attempted $fail_attempts removals — the sweep is acting on a listing it could not verify (the whole find-tristate rule)"
fi

# ---- 3: the STATUS ITSELF UNOBSERVABLE -------------------------------------------
# The reducer, not find, is broken (no awk on the box, a killed pipeline, a truncated
# record). The sweep then has no status to read AT ALL, which must be its own named,
# non-permissive answer rather than a trusted empty listing.
rm -f "$shim/find"
cat >"$shim/awk" <<'SHIM'
#!/usr/bin/env bash
exit 3
SHIM
chmod +x "$shim/awk"
ATTEMPTS=0
real_path="$PATH"; PATH="$shim:$PATH"; _logdir_sweep; PATH="$real_path"
unobs_line="$GATE_LOGDIR_SWEEP_LINE"
unobs_attempts="$ATTEMPTS"
rm -f "$shim/awk"
case "$unobs_line" in
  *"UNMEASURED (scan status unobserved under $parent)"*)
    say OK "AC19: a scan whose STATUS could not be read at all reports UNMEASURED in its own words, textually distinct from find's own failure ($unobs_line)" ;;
  *)
    say BAD "AC19: an unobservable scan status did not report its own UNMEASURED cause (got: '$unobs_line')" ;;
esac
if [ "$unobs_attempts" = 0 ]; then
  say OK "AC19: the unobservable-status scan attempted ZERO removals — an unmeasured scan is never read as an empty population"
else
  say BAD "AC19: the unobservable-status scan attempted $unobs_attempts removals — a missing status took the permissive branch"
fi

# ---- 4: a MEASURED EMPTY population ----------------------------------------------
# The third value: rc 0 with a count of 0 is a FACT about the parent, and it must not
# be reported in the same words as either failure above.
GATE_LOGDIR_PARENT="$empty"
ATTEMPTS=0
_logdir_sweep
empty_line="$GATE_LOGDIR_SWEEP_LINE"
GATE_LOGDIR_PARENT="$parent"
case "$empty_line" in
  *"0 REMOVED of 0 aged"*)
    say OK "AC19: a MEASURED empty parent reports '0 REMOVED of 0 aged', not UNMEASURED — the two are different facts and the line says which ($empty_line)" ;;
  *)
    say BAD "AC19: a measured empty parent did not report '0 REMOVED of 0 aged' (got: '$empty_line')" ;;
esac

# ---- 5: the CAP bounds what reaches this SHELL, over a population it never holds --
# The shim lists $big candidates. Under the previous form all $big would have been
# assigned to a shell variable and then to a shell array before the cap was consulted;
# the selection now happens inside the pipeline, so at most <cap> records reach this
# shell — measured as the number of candidates the walk examines, with the line
# reporting the full population and the deferred remainder so the bound is observable.
cat >"$shim/find" <<SHIM
#!/usr/bin/env bash
n=0
while [ "\$n" -lt $big ]; do
  n=\$((n + 1))
  printf '%s/agent-gate.BIG%06d\n' "$parent" "\$n"
done
exit 0
SHIM
chmod +x "$shim/find"
ATTEMPTS=0
real_path="$PATH"; PATH="$shim:$PATH"; _logdir_sweep; PATH="$real_path"
big_line="$GATE_LOGDIR_SWEEP_LINE"
big_attempts="$ATTEMPTS"
rm -f "$shim/find"
big_deferred=$(( big - cap ))
case "$big_line" in
  *"of $big aged"*"examined $cap, removals attempted $cap"*"cap $cap REACHED, $big_deferred deferred"*)
    say OK "AC19: over a $big-candidate listing the sweep examined exactly $cap and reported the $big_deferred deferred, so the cap bounds what reaches this shell and the bound is OBSERVABLE in the line ($big_line)" ;;
  *)
    say BAD "AC19: a $big-candidate listing was not bounded to the cap with its remainder deferred (got: '$big_line')" ;;
esac
if [ "$big_attempts" = "$cap" ]; then
  say OK "AC19: exactly $cap removals were attempted out of $big candidates — the work bound holds over a population this shell never materializes"
else
  say BAD "AC19: $big_attempts removals were attempted out of $big candidates against a cap of $cap"
fi

# ---- 6: the population GROWS BETWEEN THE TWO PASSES ------------------------------
# THE NORMAL STATE OF THIS BOX, NOT AN EDGE CASE (#3637, roborev job 131 medium): up
# to four lanes plus dozens of nested self-test gates create `agent-gate.*`
# directories continuously, so the second `find` routinely lists MORE entries than
# the first one counted. `k = (n - 1 - start + total) % total` then REPEATS earlier k
# values once n passes `total`, so every further block of `total` entries emitted
# another `want` records — the advertised "at most <want> records reach this shell"
# bound failed exactly where it matters, and a sweep asked to materialize 3 could
# materialize 12. A STABLE population cannot see this, which is why the shim below
# lists a different number of entries on the counting pass and on the selection pass.
#
# The other half is asserted too, because it is the half a naive fix breaks: emission
# stops at the cap, READING does not. An `exit`/`nextfile` in awk would bound the
# output and jump to END BEFORE the in-band status marker had been read, so find's
# status would read `unobserved` for every scan — trading a broken bound for a broken
# tri-state. 6a pins the real listing length and rc on the trailing record; 6c makes
# the SECOND pass fail and requires the sweep to still NAME find's status.
grow_state="$shim/grow"
mkdir -p "$grow_state"
cat >"$shim/find" <<SHIM
#!/usr/bin/env bash
# Per-invocation listing length and exit status, both read from files this driver
# writes: nothing here depends on timing, or on a real peer racing the scan.
i=\$(cat "$grow_state/n" 2>/dev/null || printf 0)
i=\$((i + 1))
printf '%s\n' "\$i" >"$grow_state/n"
lines=\$(sed -n "\${i}p" "$grow_state/lines")
rc=\$(sed -n "\${i}p" "$grow_state/rcs")
n=0
while [ "\$n" -lt "\${lines:-0}" ]; do
  n=\$((n + 1))
  printf '%s/agent-gate.GRW%06d\n' "$parent" "\$n"
done
exit "\${rc:-0}"
SHIM
chmod +x "$shim/find"

# 6a: the SCAN ITSELF, called directly with a counted total of $counted while the
# listing holds $listed — the tightest statement of the bound, since the sweep's own
# `cand[$k]` assignment COLLIDES on a repeated k and therefore cannot see an overrun
# in its examined count. What overruns is what this shell READS AND HOLDS.
printf '%s\n' "$listed" >"$grow_state/lines"
printf '0\n' >"$grow_state/rcs"
printf '0\n' >"$grow_state/n"
grow_w=0
grow_s=""
real_path="$PATH"; PATH="$shim:$PATH"
while IFS= read -r rec; do
  case "$rec" in
    "W "*) grow_w=$((grow_w + 1)) ;;
    "S "*) grow_s="$rec" ;;
  esac
done < <(_logdir_scan_aged window "$counted" 0 "$cap")
PATH="$real_path"
if [ "$grow_w" -le "$cap" ]; then
  say OK "AC19: the window scan emitted $grow_w records (cap $cap) from a listing of $listed against a counted total of $counted — the output bound holds when the population GROWS between the counting and the selection pass"
else
  say BAD "AC19: the window scan emitted $grow_w records against a cap of $cap when the listing ($listed) overran the counted total ($counted) — the advertised materialization bound does not hold under the box's normal state (the finding)"
fi
if [ "$grow_s" = "S 0 $listed" ]; then
  say OK "AC19: that same growing scan still reported find's status AND the real listing length on its trailing record ('$grow_s') — emission stopped at the cap, reading did not, so the status stays observable and the population is not under-reported"
else
  say BAD "AC19: the growing scan's trailing record was '${grow_s:-<none>}', not 'S 0 $listed' — bounding the emission also stopped consuming the scan, which loses find's status or truncates the reported population"
fi

# 6b: the same growth through the WHOLE sweep: the reported total stays the counted
# population, the change is REPORTED rather than absorbed, and the walk still examines
# and attempts at most the cap.
printf '%s\n%s\n' "$counted" "$listed" >"$grow_state/lines"
printf '0\n0\n' >"$grow_state/rcs"
printf '0\n' >"$grow_state/n"
ATTEMPTS=0
real_path="$PATH"; PATH="$shim:$PATH"; _logdir_sweep; PATH="$real_path"
grow_line="$GATE_LOGDIR_SWEEP_LINE"
grow_attempts="$ATTEMPTS"
case "$grow_line" in
  *"of $counted aged"*"examined $cap, removals attempted $cap"*"population changed between scans (counted $counted, listed $listed)"*)
    say OK "AC19: a sweep whose population grew from $counted to $listed between the passes examined the cap ($cap), reported the counted total and NAMED the change instead of absorbing it ($grow_line)" ;;
  *)
    say BAD "AC19: a sweep whose population grew between the passes did not report the counted total with a capped examined subset and the population-change note (got: '$grow_line')" ;;
esac
if [ "$grow_attempts" = "$cap" ]; then
  say OK "AC19: exactly $cap removals were attempted over a listing that grew to $listed — the work bound survives a population growing under the scan"
else
  say BAD "AC19: $grow_attempts removals were attempted against a cap of $cap over a listing that grew to $listed"
fi

# 6c: the SECOND pass fails while the first succeeded — reachable only because the
# window scan is read to the END. A bound implemented by leaving the input early would
# report `scan status unobserved` here, so this sub-case is what separates the two.
printf '%s\n%s\n' "$counted" "$listed" >"$grow_state/lines"
printf '0\n1\n' >"$grow_state/rcs"
printf '0\n' >"$grow_state/n"
ATTEMPTS=0
real_path="$PATH"; PATH="$shim:$PATH"; _logdir_sweep; PATH="$real_path"
grow_fail_line="$GATE_LOGDIR_SWEEP_LINE"
grow_fail_attempts="$ATTEMPTS"
rm -f "$shim/find"
rm -rf "$grow_state"
case "$grow_fail_line" in
  *"UNMEASURED (find rc=1 under $parent)"*)
    say OK "AC19: when the WINDOW pass failed the sweep still named find's status ($grow_fail_line) — the capped emission consumes the scan to completion, so all three UNMEASURED states stay reachable" ;;
  *)
    say BAD "AC19: a failing WINDOW pass did not report 'UNMEASURED (find rc=1)' (got: '$grow_fail_line') — if it reports an unobserved status, the emission bound stopped READING the scan and the tri-state collapsed" ;;
esac
if [ "$grow_fail_attempts" = 0 ]; then
  say OK "AC19: that failed window pass attempted ZERO removals — an unmeasured selection is never acted on, cap or no cap"
else
  say BAD "AC19: the failed window pass attempted $grow_fail_attempts removals — the sweep acted on a selection whose scan it could not verify"
fi
exit 0
SCNEOF
if bash "$scan_driver" "$rot_lib" "$scan_parent" "$scan_empty" "$scan_shim" \
     "$SCAN_POP" "$SCAN_CAP" "$SCAN_BIG" "$SCAN_GROW_COUNTED" "$SCAN_GROW_LISTED" \
     >"$scan_out" 2>"$scan_out.err"; then
  scan_lines=0
  while IFS=$'\t' read -r verdict msg; do
    case "$verdict" in
      OK)  ok "$msg"; scan_lines=$((scan_lines + 1)) ;;
      BAD) bad "$msg"; scan_lines=$((scan_lines + 1)) ;;
    esac
  done <"$scan_out"
  # CASE FLOOR (#3544's lesson): a driver that died after its positive control would
  # otherwise report a green subset of this case.
  if [ "$scan_lines" -ge 14 ]; then
    ok "AC19: the driver reported all $scan_lines of its verdicts (case floor 14)"
  else
    bad "AC19: the driver reported only $scan_lines verdicts (case floor 14) — a truncated case, not a pass"
    sed -n '1,20p' "$scan_out.err"
  fi
else
  bad "AC19: the scan-boundedness driver did not run to completion — UNMEASURED, not a pass"
  sed -n '1,20p' "$scan_out.err"
fi
# STRUCTURAL, and here rather than left to the behavioural cases: the properties above
# are only true while there is ONE scan implementation. A second `find` over the temp
# parent — a "quick" unbounded listing added later for some other purpose — would
# reintroduce both halves of the finding (an O(N) shell array, and a status read from a
# pipeline) while every case above still passed.
scan_find_sites=$(grep -c 'find "\$GATE_LOGDIR_PARENT"' "$rot_lib" 2>/dev/null || printf 0)
if [ "$scan_find_sites" = 1 ]; then
  ok "AC19: the shipped sweep slab scans the temp parent in exactly ONE place, so the bound and the in-band status have one implementation to keep true"
else
  bad "AC19: the shipped sweep slab scans the temp parent in $scan_find_sites places — a second listing would reintroduce the unbounded materialization and the lost status"
fi
if grep -vE '^[[:space:]]*#' "$rot_lib" | grep -qE '(aged|cand)=\$\(find|mapfile|readarray'; then
  bad "AC19: an executable line of the shipped sweep slab captures a whole find listing into shell state — the materialization bound is gone"
else
  ok "AC19: no executable line of the shipped sweep slab captures a whole find listing into shell state"
fi

# ---------------------------------------------------------------------------
# AC20: identity is re-confirmed AT THE REMOVAL SITE, so a SUBSTITUTED pathname
#       is never removed.
# ---------------------------------------------------------------------------
# THE RACE THIS MEASURES (#3637, roborev job 132 medium): the sweep probes a
# directory and removes it LATER, BY PATHNAME. Between the two a concurrent cleanup
# can unlink that directory and `mktemp -d` can hand the very same name to a NEW,
# LIVE run — whose bundle the sweeper then destroys, the single worst outcome this
# change can produce. A case where the directory is UNCHANGED between probe and
# removal cannot see this at all: it passes identically with and without the
# re-check, which is why the substitution is performed here rather than assumed
# improbable.
#
# The substitution is driven DETERMINISTICALLY, never by timing: the driver wraps
# the SHIPPED `_logdir_owner_state`, and at the moment that probe returns
# `verified-dead` for the target it replaces the directory at that pathname with a
# fresh, LIVE-owned one — then restores the state AND the identity the probe
# reported, so the sweep proceeds on exactly the verdict it really formed about
# content that no longer exists.
#
# Driven in a SEPARATE bash process for the same reason AC17 is: it has to override a
# shipped function, and an override in this shell would silently follow every later
# case.
race_parent="$tmp/race/parent"
guard_parent="$tmp/race/guard"
mkdir -p "$race_parent" "$guard_parent"
# STRUCTURAL, and here rather than left to the behavioural halves: the re-check is
# only worth anything at the point of USE. A guard that re-confirmed identity and
# then did something else before unlinking would still pass every behavioural case
# below while reopening the window they exist to narrow.
if grep -q '^_logdir_ident_recheck() {' "$rot_lib"; then
  ok "AC20: the identity re-check was extracted from the shipped gate"
else
  bad "AC20: the shipped sweep slab has no _logdir_ident_recheck — this case would measure nothing"
fi
if grep -qF '_logdir_rm_guarded "$d" "$ident"' "$rot_lib"; then
  ok "AC20: the sweep's removal call carries the identity its own probe established, so the removal site can re-confirm the subject"
else
  bad "AC20: the sweep calls _logdir_rm_guarded without the probed identity — the removal is back to trusting a decision made earlier against a name"
fi
guard_src2=$(awk '/^_logdir_rm_guarded\(\) \{$/{f=1} f{print} f&&/^\}$/{exit}' "$GATE")
if [ -z "$guard_src2" ]; then
  bad "AC20: could not extract _logdir_rm_guarded from the shipped gate — the siting assertion is UNMEASURED"
else
  # The two EXECUTABLE lines immediately preceding the unlink must be the `fi` of the
  # re-check block and the `confirmed` test itself. Comments and blank lines are
  # stripped, so prose may be added freely; a COMMAND may not.
  pre_rm=$(printf '%s\n' "$guard_src2" \
    | grep -vE '^[[:space:]]*#' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | grep -v '^$' \
    | awk '$0=="rm -rf \"$d\" 2>/dev/null || return 1"{print p2 "|" p1; exit} {p2=p1; p1=$0}')
  case "$pre_rm" in
    '[ "$GATE_LOGDIR_RECHECK_STATE" = "confirmed" ] || return 2|fi')
      ok "AC20: the identity re-confirmation is the LAST thing the guard does before the unlink — nothing executes between the confirmed subject and the rm" ;;
    *)
      bad "AC20: the two executable lines before the guard's unlink are not the identity re-confirmation (got: '${pre_rm:-<none>}') — the window this fix narrows has been widened again" ;;
  esac
fi
race_driver="$tmp/race/driver.sh"
race_out="$tmp/race/driver.tsv"
cat >"$race_driver" <<'RACEEOF'
#!/usr/bin/env bash
# Argv: <sweep-lib> <sweep-parent> <guard-parent>
set -uo pipefail
lib="$1"; parent="$2"; gparent="$3"
say() { printf '%s\t%s\n' "$1" "$2"; }
# shellcheck disable=SC1090
. "$lib"
GATE_LOGDIR_PARENT="$parent"
GATE_LOGDIR_SWEEP_CAP=1000
GATE_LOGDIR_SWEEP_AGE_DAYS=7
GATE_LOGDIR_CREATED="$parent/agent-gate.RUN001"

# THE one mtime boundary, exported into this driver (#3637, roborev job 175 finding 3):
# `age_it` keeps its name and its call sites, and delegates.
age_it() { _age_dir_apply "$1" || say BAD "AC20: could not synthesise an aged mtime for $1 — the candidate is not aged, so the sweep never reaches the identity re-check this case measures"; }

# ---- PHASE A: the substitution, through the REAL sweep ---------------------------
sub="$parent/agent-gate.SUB001"
plain="$parent/agent-gate.OK0001"
mkdir -p "$sub" "$plain"
planted=0
plant_owner_marker "$sub" dead && planted=$((planted + 1))
plant_owner_marker "$plain" dead && planted=$((planted + 1))
age_it "$sub"; age_it "$plain"
if [ "$planted" = 2 ]; then
  say OK "AC20: precondition — both aged candidates read verified-dead, so each is a removal the sweep WOULD attempt"
else
  say BAD "AC20: precondition failed — only $planted of 2 candidates read verified-dead; the substitution below would measure nothing"
fi

eval "_shipped_owner_state() $(declare -f _logdir_owner_state | tail -n +2)"
SUBST_DONE=0
SUBST_OK=unattempted
_logdir_owner_state() {
  local st id
  _shipped_owner_state "$@"
  st="$GATE_LOGDIR_OWNER_STATE"; id="$GATE_LOGDIR_OWNER_IDENT"
  if [ "${1:-}" = "$sub" ] && [ "$st" = verified-dead ] && [ "$SUBST_DONE" = 0 ]; then
    # SET FIRST: plant_owner_marker itself calls this probe, and re-entering here
    # would substitute a second time.
    SUBST_DONE=1
    if rm -rf "$sub" && mkdir -p "$sub" && plant_owner_marker "$sub" live; then
      SUBST_OK=yes
    else
      SUBST_OK=no
    fi
    age_it "$sub"
    # The sweep must proceed on the verdict it REALLY formed — about content that is
    # now gone. Restoring both is what makes this the race and not a re-probe.
    GATE_LOGDIR_OWNER_STATE="$st"
    GATE_LOGDIR_OWNER_IDENT="$id"
  fi
}
_logdir_sweep
sweep_line="$GATE_LOGDIR_SWEEP_LINE"
if [ "$SUBST_OK" = yes ]; then
  say OK "AC20: precondition — the target pathname really WAS replaced between the owner probe and the removal (a fresh, LIVE-owned directory now answers to it)"
else
  say BAD "AC20: precondition failed — the substitution did not happen ($SUBST_OK); this case measures nothing"
fi
if [ -d "$sub" ]; then
  say OK "AC20: the sweep did NOT remove the SUBSTITUTED directory — a live run's bundle at a reused pathname survives"
else
  say BAD "AC20: the sweep REMOVED the substituted directory — a live run's bundle destroyed at a reused pathname"
fi
_shipped_owner_state "$sub"
if [ "$GATE_LOGDIR_OWNER_STATE" = live ]; then
  say OK "AC20: the survivor is the SUBSTITUTED directory (its marker reads live through the shipped probe), not the original the probe judged"
else
  say BAD "AC20: the survivor's marker reads '$GATE_LOGDIR_OWNER_STATE', not live — the fixture is not the substitution this case claims to measure"
fi
if [ ! -d "$plain" ]; then
  say OK "AC20: the same sweep still removed the UNSUBSTITUTED verified-dead candidate — the re-check declines a changed subject, it does not disable the sweep"
else
  say BAD "AC20: the sweep removed nothing at all — the re-check is refusing legitimate removals too"
fi
case "$sweep_line" in
  *"1 REMOVED of 2 aged"*"verified-dead 2"*"examined 2, removals attempted 2, declined on identity re-check 1"*)
    say OK "AC20: the sweep line accounts for the declined candidate on its own field and does NOT count it as removed ($sweep_line)" ;;
  *)
    say BAD "AC20: the sweep line does not report 1 REMOVED of 2 with 2 attempts and 1 declined on identity re-check (got: '${sweep_line:-<none>}')" ;;
esac

# ---- PHASE B: the removal guard itself, one row per outcome ----------------------
# A TABLE, because the property is about the CLASS of substitutions and not one of
# them: a live replacement, a DIFFERENT dead replacement (state alone is not
# identity), and a replacement the probe cannot read at all (doubt ⇒ KEEP).
GATE_LOGDIR_PARENT="$gparent"
ident_of() {   # ident_of <dir> -> the identity the shipped probe establishes
  _shipped_owner_state "$1"
  printf '%s' "$GATE_LOGDIR_OWNER_IDENT"
}

b1="$gparent/agent-gate.GRD001"
mkdir -p "$b1"; plant_owner_marker "$b1" dead >/dev/null 2>&1
id1=$(ident_of "$b1")
rc1=0; _logdir_rm_guarded "$b1" "$id1" || rc1=$?
if [ "$rc1" = 0 ] && [ ! -d "$b1" ]; then
  say OK "AC20: POSITIVE CONTROL — an UNCHANGED verified-dead directory is still removed when its identity re-confirms (rc 0)"
else
  say BAD "AC20: POSITIVE CONTROL FAILED — the guard refused an unchanged verified-dead directory (rc=$rc1, present: $([ -d "$b1" ] && echo yes || echo no)); the re-check is breaking legitimate removals"
fi

b2="$gparent/agent-gate.GRD002"
mkdir -p "$b2"; plant_owner_marker "$b2" dead >/dev/null 2>&1
id2=$(ident_of "$b2")
rm -rf "$b2"; mkdir -p "$b2"; plant_owner_marker "$b2" live >/dev/null 2>&1
_logdir_ident_recheck "$b2" "$id2"
st2="$GATE_LOGDIR_RECHECK_STATE"
rc2=0; _logdir_rm_guarded "$b2" "$id2" || rc2=$?
if [ "$st2" = changed ] && [ "$rc2" = 2 ] && [ -d "$b2" ]; then
  say OK "AC20: a pathname now holding a LIVE-owned directory re-checks as 'changed' and the guard declines it (rc 2, directory intact)"
else
  say BAD "AC20: a LIVE-owned substitution re-checked '$st2' and the guard returned rc=$rc2 (present: $([ -d "$b2" ] && echo yes || echo no)) — expected changed/2/present"
fi

b3="$gparent/agent-gate.GRD003"
mkdir -p "$b3"; plant_owner_marker "$b3" dead >/dev/null 2>&1
id3=$(ident_of "$b3")
rm -rf "$b3"; mkdir -p "$b3"; plant_owner_marker "$b3" dead >/dev/null 2>&1
id3b=$(ident_of "$b3")
if [ -n "$id3" ] && [ -n "$id3b" ] && [ "$id3" != "$id3b" ]; then
  say OK "AC20: precondition — the replacement carries a DIFFERENT dead identity, so this row measures identity and not merely state"
else
  say BAD "AC20: precondition failed — the replacement's identity ('$id3b') did not differ from the original's ('$id3'); the identity row below measures nothing"
fi
_logdir_ident_recheck "$b3" "$id3"
st3="$GATE_LOGDIR_RECHECK_STATE"
rc3=0; _logdir_rm_guarded "$b3" "$id3" || rc3=$?
if [ "$st3" = changed ] && [ "$rc3" = 2 ] && [ -d "$b3" ]; then
  say OK "AC20: a pathname now holding a DIFFERENT verified-dead directory is declined too — the state alone is not the subject, the identity is (rc 2, directory intact)"
else
  say BAD "AC20: a different-identity substitution re-checked '$st3' and the guard returned rc=$rc3 (present: $([ -d "$b3" ] && echo yes || echo no)) — expected changed/2/present"
fi

b4="$gparent/agent-gate.GRD004"
mkdir -p "$b4"; plant_owner_marker "$b4" dead >/dev/null 2>&1
id4=$(ident_of "$b4")
rm -rf "$b4"; mkdir -p "$b4"          # replaced by a directory with NO marker at all
_logdir_ident_recheck "$b4" "$id4"
st4="$GATE_LOGDIR_RECHECK_STATE"
rc4=0; _logdir_rm_guarded "$b4" "$id4" || rc4=$?
if [ "$st4" = cannot-tell ] && [ "$rc4" = 2 ] && [ -d "$b4" ]; then
  say OK "AC20: a replacement the probe CANNOT read re-checks as 'cannot-tell' and is KEPT — doubt at the removal site takes the non-permissive branch, exactly as the owner probe does"
else
  say BAD "AC20: an unreadable replacement re-checked '$st4' and the guard returned rc=$rc4 (present: $([ -d "$b4" ] && echo yes || echo no)) — expected cannot-tell/2/present; doubt took the permissive branch"
fi

b5="$gparent/agent-gate.GRD005"
mkdir -p "$b5"; plant_owner_marker "$b5" live >/dev/null 2>&1
rc5=0; _logdir_rm_guarded "$b5" || rc5=$?
if [ "$rc5" = 0 ] && [ ! -d "$b5" ]; then
  say OK "AC20: with NO expected identity the guard removes as before — the per-run removal-on-PASS path, whose owner is the LIVE calling process, is untouched by the re-check"
else
  say BAD "AC20: the guard refused a removal with no expected identity (rc=$rc5, present: $([ -d "$b5" ] && echo yes || echo no)) — the per-run removal-on-PASS half is broken"
fi
exit 0
RACEEOF
if [ "$OWNER_MARKER_CAPABLE" != 1 ]; then
  # No establishable owner token ⇒ no candidate can reach `verified-dead`, so no
  # removal happens at all and the removal site has no subject. The DEGRADATION is
  # asserted positively (the same shape AC5/AC15/AC17 use) instead of skipping silently.
  ok "AC20: SKIPPED — no owner-marker capability on this host ($host_kind), so nothing reaches verified-dead and the removal site has no subject to re-confirm; the keep-everything degradation is asserted by AC5/AC17"
  race_survivors=""
elif bash "$race_driver" "$rot_lib" "$race_parent" "$guard_parent" >"$race_out" 2>"$race_out.err"; then
  race_lines=0
  while IFS=$'\t' read -r verdict msg; do
    case "$verdict" in
      OK)  ok "$msg"; race_lines=$((race_lines + 1)) ;;
      BAD) bad "$msg"; race_lines=$((race_lines + 1)) ;;
    esac
  done <"$race_out"
  # CASE FLOOR (#3544's lesson): a driver that died after PHASE A would otherwise
  # report a green subset of this case.
  if [ "$race_lines" -ge 12 ]; then
    ok "AC20: the driver reported all $race_lines of its verdicts (case floor 12)"
  else
    bad "AC20: the driver reported only $race_lines verdicts (case floor 12) — a truncated case, not a pass"
    sed -n '1,20p' "$race_out.err"
  fi
  race_survivors="$race_parent/agent-gate.SUB001 $guard_parent/agent-gate.GRD002 $guard_parent/agent-gate.GRD003 $guard_parent/agent-gate.GRD004"
else
  bad "AC20: the substitution driver did not run to completion — UNMEASURED, not a pass"
  sed -n '1,20p' "$race_out.err"
  race_survivors=""
fi

# ---------------------------------------------------------------------------
# AC21 (roborev job 173, finding 1): a PASS must not destroy the `file-size`
# OPT-OUT disclosure.
# ---------------------------------------------------------------------------
# `file-size`'s `OPT-OUT` token is NON-FAILING (`_status_is_nonfailing` accepts it), so a
# run with `CQLITE_ALLOW_FILE_GROWTH=1` engaged reaches `RESULT: PASS` — and #3637 removes
# the bundle on a terminal PASS. The row's whole disclosure is a POINTER: #3402/#3401
# deliberately moved the grown-file NAMES out of the SUMMARY row and into `file-size.log`
# INSIDE that bundle. So on the one run where the disclosure matters, the pointer dangled:
# `logs:` resolved to nothing and the names were gone from every reachable artifact.
#
# TWO HALVES, because no hermetic mode reaches a terminal PASS *through* `run_file_size`
# (the no-cargo `--only file-size` run is promoted to `RESULT: PARTIAL`, and `--lite` needs
# a real cargo):
#   (a) THE COMPONENT ARM, end to end on the real gate: a real growth fixture + the real
#       opt-out really does PIN the retention, and the reason really does NAME `file-size`
#       — asserted by observing the pin OUTRANK the `--only` exemption, which is the arm
#       directly below it and the reason this run would otherwise carry;
#   (b) THE DECISION, on the SHIPPED `_logdir_decide`, driven at `PASS` with a control that
#       proves an unpinned PASS still arms the removal. Membership is not detection: (a)
#       alone would pass with the pin ignored at emit time, and (b) alone would pass with
#       the component never setting one.
# ---------------------------------------------------------------------------
case_mark
fs_repo="$tmp/ac21-growth"
fs_td="$tmp/ac21-tmpdir"
d21=""
mkdir -p "$fs_repo/scripts" "$fs_repo/cqlite-core/src" "$fs_td"
cp "$GATE" "$fs_repo/scripts/agent-gate.sh"
# The fixture: an over-threshold .rs file committed at 900 lines and GROWN to 950 in the
# worktree, which is the only shape that reaches the ratchet's growth arm at all. Line
# counts are RE-MEASURED (the file-size suite's rule): a fixture that is not what the case
# claims makes the verdict below meaningless.
awk 'BEGIN { for (i = 1; i <= 900; i++) print "// filler line " i }' >"$fs_repo/cqlite-core/src/big.rs"
printf 'target/\n*.log\n' >"$fs_repo/.gitignore"
fs_fixture_ok=0
if ( cd "$fs_repo" && git "${GIT_CFG[@]}" init -q -b main . \
     && git "${GIT_CFG[@]}" add -A && git "${GIT_CFG[@]}" commit -qm init ) >/dev/null 2>&1; then
  awk 'BEGIN { for (i = 1; i <= 950; i++) print "// filler line " i }' >"$fs_repo/cqlite-core/src/big.rs"
  fs_base_n=$( cd "$fs_repo" && git show HEAD:cqlite-core/src/big.rs 2>/dev/null | wc -l | tr -d ' ' )
  fs_head_n=$(wc -l <"$fs_repo/cqlite-core/src/big.rs" 2>/dev/null | tr -d ' ')
  [ "${fs_base_n:-0}" = 900 ] && [ "${fs_head_n:-0}" = 950 ] && fs_fixture_ok=1
fi
if [ "$fs_fixture_ok" = 1 ]; then
  ok "AC21: precondition — the growth fixture really commits 900 lines and grows to 950"
else
  bad "AC21: the growth fixture is not what the case claims (committed '${fs_base_n:-<none>}', worktree '${fs_head_n:-<none>}') — the OPT-OUT arm is UNMEASURED"
fi
if [ "$fs_fixture_ok" = 1 ]; then
  ( cd "$fs_repo" && env -u AGENT_GATE_PARENT_RUN_ID \
      TMPDIR="$fs_td" \
      AGENT_GATE_SUMMARY_FILE="$tmp/ac21-optout.txt" \
      CQLITE_ALLOW_FILE_GROWTH=1 \
      bash "$fs_repo/scripts/agent-gate.sh" --only file-size ) >"$tmp/ac21-optout.out" 2>&1
  sf21="$tmp/ac21-optout.txt"
  # The row really is OPT-OUT — without this the retention below could be about anything.
  if grep -qE '^file-size: +OPT-OUT \(' "$sf21"; then
    ok "AC21: precondition — the engaged opt-out really produced a file-size: OPT-OUT row"
  else
    bad "AC21: no 'file-size: OPT-OUT' row in the block — the opt-out arm never ran, so the retention is UNMEASURED"
    grep -E '^file-size: ' "$sf21" 2>/dev/null
  fi
  d21=$(logs_field "$sf21") || d21=""
  if [ -n "$d21" ] && [ -d "$d21" ]; then
    ok "AC21: the OPT-OUT run RETAINED its bundle — the logs: pointer resolves"
  else
    bad "AC21: the OPT-OUT run's bundle is GONE ('${d21:-<none>}') — the #3402 disclosure the row points at was destroyed"
  fi
  # …and the artifact the row NAMES is in it, carrying the grown file. The pointer is the
  # disclosure, so a retained-but-empty bundle would satisfy the directory check and none
  # of the property.
  if [ -n "$d21" ] && [ -s "$d21/file-size.log" ] \
     && grep -q 'cqlite-core/src/big.rs' "$d21/file-size.log" 2>/dev/null; then
    ok "AC21: the retained bundle's file-size.log NAMES the grown file — the disclosure #3402/#3401 moved there survives"
  else
    bad "AC21: the retained bundle has no file-size.log naming the grown file — the pointer resolves to nothing"
  fi
  # THE REASON NAMES THE COMPONENT, and the pin is proved by which arm won: this is an
  # `--only` run, whose exemption (AC22) sits directly BELOW the pin in `_logdir_decide`,
  # so a disposition naming `file-size` can only have come from the pin.
  disp21=$(logs_disposition "$sf21")
  case "$disp21" in
    RETAINED:*file-size*OPT-OUT*)
      ok "AC21: the retention NAMES file-size's OPT-OUT ($disp21)" ;;
    RETAINED:*--only*)
      bad "AC21: the retention is attributed to the --only exemption, not to the file-size pin — the pin did not fire ('$disp21')" ;;
    *)
      bad "AC21: the retention does not name the file-size OPT-OUT disclosure (got: '${disp21:-<none>}')" ;;
  esac
else
  bad "AC21: (retention assertion not reached — fixture failed)"
  bad "AC21: (bundle-content assertion not reached — fixture failed)"
  bad "AC21: (named-reason assertion not reached — fixture failed)"
  bad "AC21: (OPT-OUT row assertion not reached — fixture failed)"
fi

# (b) THE DECISION ITSELF, at PASS, on the SHIPPED function.
#
# EXTRACTED FROM THE GATE, never reimplemented — the same rule the owner-marker helpers
# above follow. A second copy of `_logdir_decide` would be a second place for the
# precedence order to drift, and a harness that re-implements the decision measures the
# harness.
decide_lib="$tmp/decide-lib.sh"
awk '/^# _logdir_artifact_inside:/,/^# _logdir_has_content <dir>:/' "$GATE" | sed '$d' >"$decide_lib"
if grep -q '^_logdir_decide() {' "$decide_lib" && grep -q '^_logdir_force_retain() {' "$decide_lib"; then
  ok "AC21: the disposition decider was extracted from the shipped gate"
  # shellcheck disable=SC1090
  . "$decide_lib"
  # decide_reset <log-dir>: the INPUTS the decider reads, all of them, set explicitly so
  # each assertion below states its own premise. Nothing after this case reads these
  # globals (it is the last case before the survivor accounting).
  decide_reset() {
    GATE_LOGDIR_CREATED="$1"
    GATE_LOGDIR_DISPOSITION="RETAINED: no terminal verdict (post-mortem)"
    GATE_LOGDIR_REMOVE=0
    GATE_LOGDIR_REMOVE_INTENT=0
    GATE_LOGDIR_RETAIN_PIN=""
    GATE_LOGDIR_SUPERSEDED_CLAIM=""
    GATE_LOGDIR_DECIDED=0
    SUMMARY_FILE="$tmp/decide-summary-outside.txt"
    HEARTBEAT_FILE="$SUMMARY_FILE.heartbeat"
    INHERITED_PARENT_RUN_ID=""
    ONLY=""
    LITE=0
    AGENT_GATE_KEEP_LOGS=0
  }
  decide_dir="$tmp/ac21-decide"; mkdir -p "$decide_dir"
  # 1. PINNED + PASS -> RETAIN, with the pin's own reason.
  decide_reset "$decide_dir"
  _logdir_force_retain "file-size OPT-OUT disclosure #3402 (test-supplied reason)"
  _logdir_decide PASS
  if [ "$GATE_LOGDIR_REMOVE_INTENT" = 0 ] && [ "$GATE_LOGDIR_REMOVE" = 0 ]; then
    case "$GATE_LOGDIR_DISPOSITION" in
      RETAINED:*file-size*OPT-OUT*)
        ok "AC21: a terminal PASS over a PINNED bundle RETAINS and keeps the pin's reason ($GATE_LOGDIR_DISPOSITION)" ;;
      *)
        bad "AC21: a PASS over a pinned bundle retained under the WRONG reason ('$GATE_LOGDIR_DISPOSITION')" ;;
    esac
  else
    bad "AC21: a terminal PASS ARMED the removal over a PINNED bundle (intent=$GATE_LOGDIR_REMOVE_INTENT remove=$GATE_LOGDIR_REMOVE) — the #3402 disclosure is destroyed"
  fi
  # 2. THE CONTROL: without the pin the same PASS still arms the removal. Without this the
  #    assertion above passes for a gate that never removes anything at all.
  decide_reset "$decide_dir"
  _logdir_decide PASS
  if [ "$GATE_LOGDIR_REMOVE_INTENT" = 1 ]; then
    ok "AC21: CONTROL — an UNPINNED PASS still declares the removal, so the assertion above is about the pin and not about a gate that keeps everything"
  else
    bad "AC21: CONTROL FAILED — an unpinned PASS did not declare a removal (intent=$GATE_LOGDIR_REMOVE_INTENT); the pinned case proves nothing"
  fi
  # 3. The pin sits ABOVE the nested arm: the opt-out is engaged by the OPERATOR's
  #    environment and a nested run inherits it, so the disclosure argument is identical.
  decide_reset "$decide_dir"
  INHERITED_PARENT_RUN_ID="/tmp/agent-gate.PARENTFAKE"
  _logdir_force_retain "file-size OPT-OUT disclosure #3402 (test-supplied reason)"
  _logdir_decide PASS
  if [ "$GATE_LOGDIR_REMOVE_INTENT" = 0 ]; then
    ok "AC21: a NESTED run's pinned bundle is retained too — the opt-out is inherited from the operator's environment, and the nested arm does not outrank it"
  else
    bad "AC21: the nested arm removed a PINNED bundle (intent=$GATE_LOGDIR_REMOVE_INTENT) — an inherited opt-out loses its disclosure"
  fi
else
  bad "AC21: could not extract the disposition decider from the shipped gate — the PASS half is UNMEASURED"
  bad "AC21: (pinned-PASS assertion not reached)"
  bad "AC21: (control not reached)"
  bad "AC21: (nested-pin assertion not reached)"
fi
case_floor AC21 9

# ---------------------------------------------------------------------------
# AC22 (roborev job 173, finding 2): `--only` must not destroy its own product.
# ---------------------------------------------------------------------------
# `--only <component>` is a DIAGNOSTIC whose entire product is that component's log under
# `logs:` — there is no other reason to run it. `_logdir_decide` exempted only
# AGENT_GATE_KEEP_LOGS, the #2874 summary-inside-logdir shape and nested runs.
#
# Asserted in the two places it can go wrong, and NOT on the incidental route: a top-level
# `--only` is promoted to `RESULT: PARTIAL`, so the retaining verdict arm would keep the
# bundle anyway — which is exactly why the exemption is stated where the disposition is
# DECIDED and asserted by NAME here. `--lite --only <component>` is the reachable
# combination that ends `RESULT: PASS`, and the decision half below drives it.
#
# --lite is deliberately NOT exempted (its product is the LITE SUMMARY verdict and it runs
# every fix round; retaining every lite bundle re-creates the accumulation this issue
# closed), so the control asserts what it gets INSTEAD: a disposition that states the
# removal AND the AGENT_GATE_KEEP_LOGS=1 remedy.
# ---------------------------------------------------------------------------
case_mark
only_td="$tmp/ac22-tmpdir"; mkdir -p "$only_td"
sf22="$tmp/ac22-only.txt"
env -u AGENT_GATE_PARENT_RUN_ID \
    TMPDIR="$only_td" \
    AGENT_GATE_SUMMARY_FILE="$sf22" \
    bash "$FAKE_GATE" --only file-size >"$tmp/ac22-only.out" 2>&1
if grep -q '^RESULT: PARTIAL' "$sf22"; then
  ok "AC22: precondition — the --only run reached its own terminal token (RESULT: PARTIAL)"
else
  bad "AC22: precondition failed — the --only run left no PARTIAL verdict; the disposition below is UNMEASURED"
  sed -n '1,20p' "$tmp/ac22-only.out"
fi
d22=$(logs_field "$sf22") || d22=""
if [ -n "$d22" ] && [ -d "$d22" ]; then
  ok "AC22: the --only run RETAINED its bundle"
else
  bad "AC22: the --only run's bundle is GONE ('${d22:-<none>}') — the diagnostic destroyed its own product"
fi
# The PRODUCT, not merely the directory: the selected component's log.
if [ -n "$d22" ] && [ -s "$d22/file-size.log" ]; then
  ok "AC22: the retained bundle holds the selected component's log — the thing the mode exists to produce"
else
  bad "AC22: the retained bundle holds no file-size.log — the diagnostic's product is missing"
fi
disp22=$(logs_disposition "$sf22")
case "$disp22" in
  RETAINED:*--only*)
    ok "AC22: the retention NAMES the only-mode ($disp22)" ;;
  RETAINED:*)
    bad "AC22: the bundle was retained but the reason does not name the only-mode ('$disp22') — an incidental retention keyed on the verdict mapping, not a decision about the mode" ;;
  *)
    bad "AC22: no RETAINED disposition for the --only run (got: '${disp22:-<none>}')" ;;
esac
# The reason must carry no caller text: `$ONLY` is argv, and the block already publishes
# the selection on its own `mode: PARTIAL (--only …)` line.
case "$disp22" in
  *file-size*)
    bad "AC22: the disposition interpolates the --only SELECTION ('$disp22') — argv on a keyed SUMMARY line is a channel this issue removes, not one it adds" ;;
  *)
    ok "AC22: the disposition names the MODE and interpolates no argv" ;;
esac
# THE PASS SHAPE, on the shipped decider: `--lite --only <component>` ends RESULT: PASS,
# which is the case the verdict arm does NOT cover.
if grep -q '^_logdir_decide() {' "$decide_lib" 2>/dev/null; then
  decide_reset "$decide_dir"
  ONLY=clippy; LITE=1
  _logdir_decide PASS
  if [ "$GATE_LOGDIR_REMOVE_INTENT" = 0 ]; then
    case "$GATE_LOGDIR_DISPOSITION" in
      RETAINED:*--only*) ok "AC22: --lite --only <component> at RESULT: PASS RETAINS, named ($GATE_LOGDIR_DISPOSITION)" ;;
      *) bad "AC22: the only-mode PASS retained under the wrong reason ('$GATE_LOGDIR_DISPOSITION')" ;;
    esac
  else
    bad "AC22: an --only run at RESULT: PASS still ARMED the removal (intent=$GATE_LOGDIR_REMOVE_INTENT) — the component log the operator ran it for is deleted"
  fi
  # THE CONTROL, which is also the --lite requirement: same PASS with no --only selection
  # REMOVES, and says so with the remedy.
  decide_reset "$decide_dir"
  LITE=1
  _logdir_decide PASS
  if [ "$GATE_LOGDIR_REMOVE_INTENT" = 1 ]; then
    ok "AC22: CONTROL — a plain --lite PASS still declares the removal, so --only was not widened to --lite"
  else
    bad "AC22: CONTROL FAILED — a plain --lite PASS retained (intent=$GATE_LOGDIR_REMOVE_INTENT); every fix round now leaks a bundle"
  fi
  case "$GATE_LOGDIR_DISPOSITION" in
    REMOVED*--lite*AGENT_GATE_KEEP_LOGS=1*)
      ok "AC22: --lite's own disposition states the removal AND the AGENT_GATE_KEEP_LOGS=1 remedy ($GATE_LOGDIR_DISPOSITION)" ;;
    *)
      bad "AC22: --lite's disposition does not state both the removal and the KEEP_LOGS remedy ('$GATE_LOGDIR_DISPOSITION')" ;;
  esac
  # …and the nested arm still outranks the only-mode exemption: the nested `--only` gates
  # (the documented hermetic `--only file-size` run) are the bulk of the leak #3637 closed,
  # and their reader is a parent asserting on a SUMMARY, never an operator reading a log.
  decide_reset "$decide_dir"
  ONLY=file-size
  INHERITED_PARENT_RUN_ID="/tmp/agent-gate.PARENTFAKE"
  _logdir_decide PASS
  if [ "$GATE_LOGDIR_REMOVE_INTENT" = 1 ]; then
    ok "AC22: a NESTED --only run is still REMOVED — the exemption did not re-open the dozens-per-gate self-test population"
  else
    bad "AC22: a nested --only run is now retained (intent=$GATE_LOGDIR_REMOVE_INTENT) — the leak this issue closed is back"
  fi
else
  bad "AC22: the decider was not extracted — the PASS-shape half is UNMEASURED"
  bad "AC22: (--lite control not reached)"
  bad "AC22: (--lite remedy assertion not reached)"
  bad "AC22: (nested --only assertion not reached)"
fi
case_floor AC22 9

# ---------------------------------------------------------------------------
# AC23 (roborev job 173, finding 3): ONE content predicate for both early-exit arms.
# ---------------------------------------------------------------------------
# Arm 2 (exit status 0) excludes the FULL launch-artifact allowlist
# (`_logdir_is_launch_artifact`: the owner marker, the #2874 private summary and its
# heartbeat/integrity siblings, and the #3755 `gate-slot.ready` / `disk-admission*`
# admission bookkeeping — that last family had to be added mid-merge, after every exit-0
# run silently kept its bundle). Arm 3 (non-zero) excluded ONLY the owner marker, so a
# non-zero exit landing AFTER admission wrote its bookkeeping but BEFORE any component ran
# retained a husk of pure launch artifacts: the exact shape the allowlist closed on the
# other arm, one arm over.
#
# Driven END TO END on the real non-zero exit (the argv/usage refusal AC8 uses), with the
# launch artifacts PLANTED into the LOG_DIR by a `mktemp` shim as it is created — the
# seeding idiom scripts/tests/test_agent_gate_tree_provenance.sh already uses. The shim
# writes a RECEIPT naming the directory it planted into, because "the directory is gone"
# is also what a shim that planted NOTHING would produce.
#
# BOTH DIRECTIONS, from one fixture: launch artifacts alone -> REMOVED (a husk informs
# nobody), the same set plus ONE component log -> RETAINED (there is a post-mortem).
# Plus a MUTANT restoring the owner-marker-only predicate, which must retain the husk —
# otherwise the husk direction is a case that cannot fail.
# ---------------------------------------------------------------------------
case_mark
d23=""; d23m=""
ac23_real_mktemp=$(command -v mktemp 2>/dev/null) || ac23_real_mktemp=""
if [ -z "$ac23_real_mktemp" ]; then
  bad "AC23: no mktemp on PATH — the launch-artifact husk case CANNOT be exercised"
  bad "AC23: (husk direction not reached)"
  bad "AC23: (content direction not reached)"
  bad "AC23: (mutant not reached)"
else
  ac23_bin="$tmp/ac23-bin"; mkdir -p "$ac23_bin"
  cat >"$ac23_bin/mktemp" <<AC23STUB
#!/usr/bin/env bash
d=\$("$ac23_real_mktemp" "\$@") || exit 1
case "\$d" in
  */agent-gate.*)
    if [ -d "\$d" ] && [ -n "\${AC23_PLANT:-}" ]; then
      case "\$AC23_PLANT" in
        launch)
          : >"\$d/gate-slot.ready"
          printf '{}\n' >"\$d/disk-admission-cargo-metadata.json"
          printf 'probe\n' >"\$d/disk-admission.bcap.out"
          ;;
        component)
          : >"\$d/gate-slot.ready"
          printf '{}\n' >"\$d/disk-admission-cargo-metadata.json"
          printf 'warning: something\n' >"\$d/clippy.log"
          ;;
      esac
      printf '%s\n' "\$d" >>"\${AC23_RECEIPT:-/dev/null}"
    fi
    ;;
esac
printf '%s\n' "\$d"
AC23STUB
  chmod +x "$ac23_bin/mktemp"
  # ac23_run <gate-script> <plant-kind> <tmpdir> <receipt> — the argv/usage refusal, which
  # exits 2 AFTER the LOG_DIR exists and BEFORE any component runs: the window the finding
  # is about. Top-level (no parent run id) and with the summary file OUTSIDE the bundle, so
  # the `RESULT: INCOMPLETE` sentinel — the ONE deliberate content exception — is not in it.
  ac23_run() {
    mkdir -p "$3"
    : >"$4"
    env -u AGENT_GATE_PARENT_RUN_ID \
        PATH="$ac23_bin:$PATH" \
        TMPDIR="$3" \
        AGENT_GATE_SUMMARY_FILE="$4.summary" \
        AC23_PLANT="$2" \
        AC23_RECEIPT="$4" \
        AGENT_GATE_INTEGRITY_SELFTEST=not-a-valid-selector \
        bash "$1" >"$4.out" 2>&1
    return 0
  }
  # (a) HUSK: launch artifacts only.
  td23a="$tmp/ac23-husk"; rcpt23a="$tmp/ac23-husk.receipt"
  ac23_run "$FAKE_GATE" launch "$td23a" "$rcpt23a"
  planted23a=$(head -1 "$rcpt23a" 2>/dev/null)
  if [ -n "$planted23a" ]; then
    ok "AC23: precondition — the shim really planted this run's launch artifacts into $planted23a"
  else
    bad "AC23: the shim planted NOTHING — 'the husk was removed' would pass having measured nothing"
  fi
  if [ -n "$planted23a" ] && [ "$(count_logdirs "$td23a")" = 0 ]; then
    ok "AC23: a non-zero exit whose bundle holds ONLY launch artifacts left NO husk"
  else
    bad "AC23: the non-zero exit RETAINED $(count_logdirs "$td23a") husk directory/ies holding nothing but launch artifacts — the #3755 family is content on this arm"
    find "$td23a" -maxdepth 2 2>/dev/null | head -8
  fi
  # (b) CONTENT: the same launch set plus ONE component log.
  td23b="$tmp/ac23-content"; rcpt23b="$tmp/ac23-content.receipt"
  ac23_run "$FAKE_GATE" component "$td23b" "$rcpt23b"
  planted23b=$(head -1 "$rcpt23b" 2>/dev/null)
  if [ -n "$planted23b" ]; then
    ok "AC23: precondition — the shim planted a component log beside the same launch artifacts"
  else
    bad "AC23: the shim planted nothing for the content direction — UNMEASURED"
  fi
  d23=$(one_logdir "$td23b") || d23=""
  if [ -n "$d23" ] && [ -d "$d23" ] && [ -s "$d23/clippy.log" ]; then
    ok "AC23: the SAME non-zero exit RETAINED the bundle once it held one component log"
    disp23=$(artifact_field "$d23" logdir-disposition)
    case "$disp23" in
      RETAINED:*content*) ok "AC23: the retention names the diagnostic content ($disp23)" ;;
      RETAINED:*) bad "AC23: the retention's reason is not the content arm's ('$disp23')" ;;
      *) bad "AC23: the retained husk-plus-content bundle published no reason (got: '${disp23:-<none>}')" ;;
    esac
  else
    bad "AC23: a non-zero exit whose bundle held a component log left NOTHING — the predicate now removes real post-mortem evidence"
    d23=""
    bad "AC23: (reason assertion not reached)"
  fi
  # (c) THE MUTANT: restore the owner-marker-only exclusion — the pre-fix predicate — and
  #     the husk must come back. Only the FIRST occurrence is rewritten: the identical line
  #     in `_logdir_has_evidence` belongs to arm 2, which this case is not about.
  mut23="$tmp/ac23-mutant-gate.sh"
  if awk '
      { l = $0; sub(/^[[:space:]]+/, "", l) }
      !done && l == "_logdir_is_launch_artifact \"$e\" && continue" {
        done = 1
        print substr($0, 1, match($0, /[^[:space:]]/) - 1) "if [ -n \"${GATE_LOGDIR_OWNER_FILE:-}\" ] && [ \"$e\" = \"$GATE_LOGDIR_OWNER_FILE\" ]; then continue; fi"
        next
      }
      { print }
      END { if (!done) exit 3 }
    ' "$GATE" >"$mut23"; then
    chmod +x "$mut23"
    td23m="$tmp/ac23-mutant"; rcpt23m="$tmp/ac23-mutant.receipt"
    ac23_run "$mut23" launch "$td23m" "$rcpt23m"
    d23m=$(one_logdir "$td23m") || d23m=""
    if [ -n "$d23m" ]; then
      ok "AC23 mutant: the owner-marker-only predicate DOES retain the pure-launch husk (proved discriminating)"
    else
      bad "AC23 mutant: the pre-fix predicate left no husk either — the husk direction above is a case that cannot fail"
    fi
  else
    bad "AC23 mutant: the launch-artifact skip was not found in _logdir_has_content — the mutant is vacuous"
  fi
fi
case_floor AC23 6

# ---------------------------------------------------------------------------
# AC24 (roborev job 173, finding 4): the two NEW keys carry no $TMPDIR-derived
# control characters into the block.
# ---------------------------------------------------------------------------
# `logdir-sweep:` embeds `$GATE_LOGDIR_PARENT` — i.e. `$TMPDIR` — VERBATIM, so a
# `TMPDIR=$'/tmp/x\nRESULT: PASS'` emitted an EXTRA LINE inside the block, and one matching
# the completion probe's own `RESULT: (PASS|FAIL)` pattern: environment-controlled data
# forging a terminal verdict. Both new keys now render through `_summary_block_value`, the
# SAME boundary `_status_detail` uses (strip C0+DEL under LC_ALL=C; WITHHOLD — never
# rewrite — a value carrying `RESULT:`).
#
# ASSERTED ON THE BLOCK'S LINE COUNT, against a clean-TMPDIR baseline of the identical run,
# because that is the property: a keyed line must stay ONE line. TWO channels remain and are
# named in the expected delta — `run-id:` and `logs:` are both the raw LOG_DIR path and are
# byte-identical by design (#3637/#3312: `logs:` must match the heartbeat's own `logs:`), so
# the injected block is baseline + EXACTLY 2. It was baseline + 3 before this fix, and the
# MUTANT below re-proves that rather than asserting it.
#
# RUN AGAINST A REFUSAL-DEFEATED COPY OF THE GATE, and that is the point (#3637, roborev job
# 175 finding 1). Those 2 remaining channels used to be a DECLARED residual; they are now
# closed WHERE THE VALUE ENTERS — the shipped gate REFUSES a control-bearing `$TMPDIR` at the
# LOG_DIR creation site, before `mktemp -d`, so no shipped run can carry one as far as this
# renderer (AC27 owns that refusal, in both directions). The `_summary_block_value` boundary
# STAYS regardless, as defence in depth for the next writer of a free-text SUMMARY value —
# and a defence nothing can measure is one that rots, so this case keeps measuring it through
# a copy whose refusal has been defeated by ONE VERIFIED mutation and nothing else.
#
# WITH A POSITIVE CONTROL, because a value that never reached the renderer would also add
# no line: the sweep line must CARRY both halves of the planted marker, joined, on ONE line.
# ---------------------------------------------------------------------------
case_mark
# The refusal-defeated copy. ONE line changed, and the change is VERIFIED both ways (the
# refusal's own predicate is gone; the strip it shares a definition with is untouched).
gate24d="$tmp/ac24-defeated-gate.sh"
if awk '
    { l = $0; sub(/^[[:space:]]+/, "", l) }
    !done && l == "if [ \"$(_gate_cntrl_strip \"$GATE_LOGDIR_PARENT\")\" != \"$GATE_LOGDIR_PARENT\" ]; then" {
      done = 1
      print "if false; then   # AC24: creation-site refusal DEFEATED for this copy only"
      next
    }
    { print }
    END { if (!done) exit 3 }
  ' "$GATE" >"$gate24d" \
   && ! grep -qF '_gate_cntrl_strip "$GATE_LOGDIR_PARENT"' "$gate24d" \
   && grep -qF "LC_ALL=C tr -d '[:cntrl:]'" "$gate24d"; then
  ok "AC24: the copy under test really lost the creation-site refusal and kept the strip (one verified mutation)"
else
  bad "AC24: could not defeat the creation-site refusal in a copy of the gate — every assertion below would measure a REFUSED run instead of the boundary the fix is defence in depth for"
  cp "$GATE" "$gate24d"
fi
# Baseline: the same hermetic run, same copy, under an ordinary TMPDIR.
td24a="$tmp/ac24-clean"; sf24a="$tmp/ac24-clean.txt"
run_agg_gate "$gate24d" "$td24a" "$sf24a" PASS
n24a=$(block_lines "$sf24a")
if [ "${n24a:-0}" -gt 10 ]; then
  ok "AC24: precondition — the clean-TMPDIR baseline block has $n24a lines"
else
  bad "AC24: the baseline block is unusable ($n24a lines) — every count below is UNMEASURED"
fi
# The injection: a $TMPDIR whose LEAF NAME carries a newline between two markers. Legal on
# every filesystem this gate runs on, and reached by the ordinary route.
td24b="$tmp/ac24-inj-AAMARK
ZZMARK/td"; sf24b="$tmp/ac24-inj.txt"
inj24_ok=0
mkdir -p "$td24b" 2>/dev/null && inj24_ok=1
if [ "$inj24_ok" = 1 ]; then
  ok "AC24: precondition — a \$TMPDIR containing a newline really exists on this filesystem"
  run_agg_gate "$gate24d" "$td24b" "$sf24b" PASS
  n24b=$(block_lines "$sf24b")
  exp24=$(( ${n24a:-0} + 2 ))
  if [ "${n24b:-0}" = "$exp24" ]; then
    ok "AC24: the newline-bearing \$TMPDIR added EXACTLY the 2 raw-path channels (run-id:, logs:) and nothing else — $n24b vs baseline $n24a; neither NEW key forged a row (and on the SHIPPED gate those 2 are unreachable too: AC27's creation-site refusal)"
  else
    bad "AC24: the injected block has $n24b lines, expected $exp24 (baseline $n24a + the 2 declared run-id:/logs: channels) — a new key is emitting environment-controlled rows"
    sed -n '1,40p' "$sf24b"
  fi
  # POSITIVE CONTROL: the planted bytes really do reach the renderer, and the strip JOINED
  # them rather than dropping the value.
  sweep24=$(grep -c '^logdir-sweep: ' "$sf24b" 2>/dev/null || true)
  if [ "$sweep24" = 1 ]; then
    ok "AC24: exactly one logdir-sweep: line in the injected block"
  else
    bad "AC24: expected exactly one logdir-sweep: line, found $sweep24"
  fi
  if grep -q '^logdir-sweep: .*AAMARKZZMARK' "$sf24b"; then
    ok "AC24: POSITIVE CONTROL — the planted \$TMPDIR bytes DO reach the sweep line, joined onto ONE line by the strip (so the count above is about sanitisation, not about a value that never arrived)"
  else
    bad "AC24: the planted markers are not on the sweep line — the value never reached the renderer, so the line-count assertion measured nothing"
    grep '^logdir-sweep: ' "$sf24b" 2>/dev/null
  fi
  # And exactly one disposition line, same rule (it is free text on a keyed line too).
  n24disp=$(grep -c '^logdir-disposition: ' "$sf24b" 2>/dev/null || true)
  if [ "$n24disp" = 1 ]; then
    ok "AC24: exactly one logdir-disposition: line in the injected block"
  else
    bad "AC24: expected exactly one logdir-disposition: line, found $n24disp"
  fi
  # THE MUTANT: unpin the strip at the boundary and the extra row comes back. Without it
  # "the count did not grow" is a property of this fixture, not of the fix.
  # Built FROM the refusal-defeated copy, so the mutant differs from the run above by the
  # strip and by nothing else. The target is `_gate_cntrl_strip`'s body — THE one definition
  # of the class the boundary strips with and the refusal compares against (#3637, job 175).
  mut24="$tmp/ac24-mutant-gate.sh"
  if awk '
      { l = $0; sub(/^[[:space:]]+/, "", l) }
      !done && l == "printf '"'"'%s'"'"' \"${1-}\" | LC_ALL=C tr -d '"'"'[:cntrl:]'"'"'" {
        done = 1
        print substr($0, 1, match($0, /[^[:space:]]/) - 1) "printf '"'"'%s'"'"' \"${1-}\""
        next
      }
      { print }
      END { if (!done) exit 3 }
    ' "$gate24d" >"$mut24"; then
    td24m="$tmp/ac24-mut-AAMARK
ZZMARK/td"; sf24m="$tmp/ac24-mut.txt"
    if mkdir -p "$td24m" 2>/dev/null; then
      run_agg_gate "$mut24" "$td24m" "$sf24m" PASS
      n24m=$(block_lines "$sf24m")
      if [ "${n24m:-0}" -gt "$exp24" ]; then
        ok "AC24 mutant: without the strip the newline DOES forge an extra row ($n24m vs $exp24) — proved discriminating"
      else
        bad "AC24 mutant: the unstripped boundary emitted $n24m lines, not more than $exp24 — the assertion above cannot fail"
      fi
      rm -rf "${td24m%/td}"
    else
      bad "AC24 mutant: could not create the mutant's newline TMPDIR — the mutant is vacuous"
    fi
  else
    bad "AC24 mutant: the strip at _gate_cntrl_strip was not found — the mutant is vacuous"
  fi
  # The newline-bearing fixture is removed HERE, after its assertions: the survivor
  # accounting at the end of this file compares NEWLINE-SEPARATED sets of paths, which
  # cannot represent a path that contains a newline. The removal above is asserted, so
  # nothing is being hidden — an unexpected survivor would already have failed the count.
  rm -rf "${td24b%/td}"
else
  bad "AC24: could not create a newline-bearing \$TMPDIR — the injection route is UNMEASURED"
  bad "AC24: (line-count assertion not reached)"
  bad "AC24: (positive control not reached)"
  bad "AC24: (one-sweep-line assertion not reached)"
  bad "AC24: (one-disposition-line assertion not reached)"
  bad "AC24 mutant: (not reached)"
fi
# THE WITHHOLD HALF: a $TMPDIR carrying the completion probe's reserved token. The value is
# WITHHELD rather than rewritten (a rewrite would name a path that exists nowhere), the KEY
# survives — a block that silently loses a key is indistinguishable from one whose sweep
# never ran — and the line does not match the probe's pattern.
td24c="$tmp/ac24-RESULT: PASS/td"; sf24c="$tmp/ac24-hostile.txt"
if mkdir -p "$td24c" 2>/dev/null; then
  ok "AC24: precondition — a \$TMPDIR literally containing 'RESULT: PASS' really exists"
  run_agg "$td24c" "$sf24c" PASS
  sweep24c=$(sed -n 's/^logdir-sweep: //p' "$sf24c" | tail -1)
  case "$sweep24c" in
    *WITHHELD*) ok "AC24: the sweep census carrying the reserved verdict token is WITHHELD ($sweep24c)" ;;
    "")         bad "AC24: the logdir-sweep: KEY vanished with its value — a withheld value must not take the key with it" ;;
    *)          bad "AC24: the sweep census was rendered with the reserved token in it ('$sweep24c')" ;;
  esac
  if sed -n 's/^logdir-sweep: //p' "$sf24c" | grep -Eq 'RESULT: (PASS|FAIL)'; then
    bad "AC24: the sweep line matches the completion probe's own pattern — it would forge a terminal verdict"
  else
    ok "AC24: the sweep line does not match 'RESULT: (PASS|FAIL)' — the refusal quotes nothing"
  fi
  # POSITIVE CONTROL, on an INDEPENDENT channel: the hostile TMPDIR really did reach this
  # block. `logs:` is PATH-ONLY and byte-identical by design (#3637/#3312, AC7/AC9), so its
  # exposure is pre-existing and DECLARED — and it is the proof that the withheld sweep
  # value was withheld rather than never produced.
  if grep -q '^logs: .*RESULT: PASS' "$sf24c"; then
    ok "AC24: POSITIVE CONTROL — the hostile \$TMPDIR did reach the block on the DECLARED path-only logs: channel, so the withheld sweep value was really produced and refused"
  else
    bad "AC24: the hostile \$TMPDIR never reached the block at all — the withholding assertions measured nothing"
    grep -E '^(run-id|logs): ' "$sf24c" 2>/dev/null
  fi
  rm -rf "${td24c%/td}"
else
  bad "AC24: could not create the hostile 'RESULT: PASS' \$TMPDIR — the withholding route is UNMEASURED"
  bad "AC24: (probe-pattern assertion not reached)"
  bad "AC24: (withhold positive control not reached)"
fi
case_floor AC24 11

# ---------------------------------------------------------------------------
# AC25 (roborev job 174, finding A): the opt-out's DISCLOSURE states the OBSERVED
# value, engagement stays LENIENT, and set-but-EMPTY does not engage.
# ---------------------------------------------------------------------------
# THE DEFECT: engagement was `!= 0` (any non-`0` value retains) while all three emitted
# strings printed the LITERAL `AGENT_GATE_KEEP_LOGS=1`. So `AGENT_GATE_KEEP_LOGS=no`
# retained AND the block asserted a value the operator never set.
#
# THE DECIDED FIX fixes the DISCLOSURE, not the engagement, and this case pins BOTH
# halves so a later "tidy-up" that copies the `CQLITE_ALLOW_FILE_GROWTH` precedent
# (exactly `1`, so a typo cannot waive a ratchet) reds here. That precedent does not
# transfer: its permissive branch WAIVES A CHECK, this one KEEPS DATA, so narrowing it
# would make a typo DESTROY the bundle the operator asked to keep.
#
# DETECTION MEASURED, not argued (the pre-fix script driven through this same hermetic
# fixture at `AGENT_GATE_KEEP_LOGS=no`):
#     logdir-disposition: RETAINED: AGENT_GATE_KEEP_LOGS=1
#     logdir-sweep: SKIPPED (AGENT_GATE_KEEP_LOGS=1)
# — i.e. all four (a) assertions on those two lines fail against it. Reproduce with
# `git show <pre-fix-sha>:scripts/agent-gate.sh` copied into a fake checkout and run
# through `--lite-aggregate-selftest`, exactly as run_agg does.
#
# MEMBERSHIP IS NOT DETECTION, so every arm is measured in BOTH directions: the
# observed value must be PRESENT and the hard-coded `AGENT_GATE_KEEP_LOGS=1` must be
# ABSENT from the same run's disposition. The `=1` control exists so the case is about
# the RENDERING and not about retention (AC4 already owns retention under `=1`).
#
# The two hostile values ride the SAME boundary the AC24 keys do (`_summary_block_value`
# at the emit site), so they are asserted the same way: the block's LINE COUNT against a
# baseline of the identical run, with a POSITIVE CONTROL proving the planted bytes really
# reached the renderer. AC24's mutant already proves that strip discriminating; nothing
# here re-proves it.
# ---------------------------------------------------------------------------
case_mark

# --- (a) a lenient, unconventional value: RETAINS, and says `no`, not `1`.
td25a="$tmp/td-keep-no"; sf25a="$tmp/keep-no-summary.txt"
run_agg "$td25a" "$sf25a" PASS AGENT_GATE_KEEP_LOGS=no
d25a=$(logs_field "$sf25a") || d25a=""
if [ -n "$d25a" ] && [ -d "$d25a" ]; then
  ok "AC25a: AGENT_GATE_KEEP_LOGS=no RETAINED a PASS run's bundle (engagement stays lenient: this opt-out keeps DATA, so a typo must not destroy it)"
else
  bad "AC25a: AGENT_GATE_KEEP_LOGS=no did NOT retain a PASS run's bundle ('${d25a:-<none>}') — engagement was narrowed and a typo now DESTROYS the bundle the operator asked to keep"
fi
disp25a=$(logs_disposition "$sf25a")
case "$disp25a" in
  RETAINED*AGENT_GATE_KEEP_LOGS=no*) ok "AC25a: the disposition renders the OBSERVED value ($disp25a)" ;;
  *) bad "AC25a: the disposition does not render the observed value 'no' (got: '${disp25a:-<none>}')" ;;
esac
case "$disp25a" in
  *AGENT_GATE_KEEP_LOGS=1*) bad "AC25a: the disposition still asserts AGENT_GATE_KEEP_LOGS=1, a value this run never set (got: '$disp25a') — the confidently-wrong claim job 174 finding A is about" ;;
  *) ok "AC25a: the disposition does NOT contain the hard-coded 'AGENT_GATE_KEEP_LOGS=1'" ;;
esac
case "$disp25a" in
  *"SET BUT NOT 1"*) ok "AC25a: the disposition ANNOUNCES the value as unconventional-but-HONOURED, so an operator who typed '=no' learns both facts from the line in front of them" ;;
  *) bad "AC25a: a set-but-not-1 value was honoured SILENTLY — the operator cannot tell their value was unconventional (got: '$disp25a')" ;;
esac
sweep25a=$(sed -n 's/^logdir-sweep: //p' "$sf25a" | tail -1)
case "$sweep25a" in
  SKIPPED*AGENT_GATE_KEEP_LOGS=no*) ok "AC25a: the sweep's SKIPPED line renders the observed value too ($sweep25a)" ;;
  *) bad "AC25a: the sweep line does not render the observed value (got: '${sweep25a:-<none>}')" ;;
esac
case "$sweep25a" in
  *AGENT_GATE_KEEP_LOGS=1*) bad "AC25a: the sweep line still asserts AGENT_GATE_KEEP_LOGS=1 ('$sweep25a')" ;;
  *) ok "AC25a: the sweep line does NOT contain the hard-coded 'AGENT_GATE_KEEP_LOGS=1'" ;;
esac

# --- (b) the CONTROL: `=1` renders exactly `=1`, with NO unconventional-value note.
td25b="$tmp/td-keep-one"; sf25b="$tmp/keep-one-summary.txt"
run_agg "$td25b" "$sf25b" PASS AGENT_GATE_KEEP_LOGS=1
d25b=$(logs_field "$sf25b") || d25b=""
disp25b=$(logs_disposition "$sf25b")
if [ -n "$d25b" ] && [ -d "$d25b" ] && [ "$disp25b" = "RETAINED: AGENT_GATE_KEEP_LOGS=1" ]; then
  ok "AC25b: CONTROL — the documented value renders EXACTLY 'RETAINED: AGENT_GATE_KEEP_LOGS=1' (so (a) measures the rendering, not the retention)"
else
  bad "AC25b: CONTROL — expected exactly 'RETAINED: AGENT_GATE_KEEP_LOGS=1' with a surviving bundle (disposition: '${disp25b:-<none>}', dir: '${d25b:-<none>}')"
fi
case "$disp25b" in
  *"SET BUT NOT 1"*) bad "AC25b: the documented value '1' was annotated as unconventional ('$disp25b') — the note must fire only for a value that is not 1" ;;
  *) ok "AC25b: the documented value carries NO unconventional-value note" ;;
esac

# --- (c) SET BUT EMPTY is NOT engaged: an empty value carries no intent to keep.
td25c="$tmp/td-keep-empty"; sf25c="$tmp/keep-empty-summary.txt"
run_agg "$td25c" "$sf25c" PASS AGENT_GATE_KEEP_LOGS=
d25c=$(logs_field "$sf25c") || d25c=""
if [ -n "$d25c" ] && [ ! -d "$d25c" ]; then
  ok "AC25c: a SET-BUT-EMPTY AGENT_GATE_KEEP_LOGS did NOT engage retention — the PASS run removed its own bundle"
else
  bad "AC25c: a SET-BUT-EMPTY AGENT_GATE_KEEP_LOGS engaged retention ('${d25c:-<none>}') — an empty value states no intent to keep anything"
fi
disp25c=$(logs_disposition "$sf25c")
case "$disp25c" in
  REMOVED*) ok "AC25c: and its disposition DECLARES the removal rather than a KEEP_LOGS retention ($disp25c)" ;;
  *) bad "AC25c: the empty-value run's disposition does not declare a removal (got: '${disp25c:-<none>}')" ;;
esac

# --- (d) HOSTILE: a control character in the value must not forge a row.
td25d0="$tmp/td-keep-base"; sf25d0="$tmp/keep-base-summary.txt"
run_agg "$td25d0" "$sf25d0" PASS AGENT_GATE_KEEP_LOGS=1
d25d0=$(logs_field "$sf25d0") || d25d0=""
n25base=$(block_lines "$sf25d0")
if [ "${n25base:-0}" -gt 10 ]; then
  ok "AC25d: precondition — the retaining baseline block has $n25base lines"
else
  bad "AC25d: the retaining baseline block is unusable ($n25base lines) — the counts below are UNMEASURED"
fi
td25d="$tmp/td-keep-ctrl"; sf25d="$tmp/keep-ctrl-summary.txt"
run_agg "$td25d" "$sf25d" PASS "AGENT_GATE_KEEP_LOGS=AAKEEP
ZZKEEP"
d25d=$(logs_field "$sf25d") || d25d=""
n25d=$(block_lines "$sf25d")
if [ "${n25d:-0}" = "${n25base:-0}" ]; then
  ok "AC25d: a newline-bearing opt-out value added NO row to the block ($n25d vs baseline $n25base) — the shared boundary stripped it"
else
  bad "AC25d: the control-character value changed the block from $n25base to $n25d lines — environment-controlled data is forging rows through the disposition key"
  sed -n '1,40p' "$sf25d"
fi
disp25d=$(logs_disposition "$sf25d")
case "$disp25d" in
  *AAKEEPZZKEEP*) ok "AC25d: POSITIVE CONTROL — the planted bytes DO reach the renderer, joined onto ONE line by the strip (so the count above is about sanitisation, not about a value that never arrived)" ;;
  *) bad "AC25d: the planted markers are not on the disposition line ('${disp25d:-<none>}') — the value never reached the renderer, so the line-count assertion measured nothing" ;;
esac
n25ddisp=$(grep -c '^logdir-disposition: ' "$sf25d" 2>/dev/null || true)
if [ "$n25ddisp" = 1 ]; then
  ok "AC25d: exactly one logdir-disposition: line in the injected block"
else
  bad "AC25d: expected exactly one logdir-disposition: line, found $n25ddisp"
fi

# --- (e) HOSTILE: a value carrying the completion probe's reserved token is WITHHELD.
td25e="$tmp/td-keep-token"; sf25e="$tmp/keep-token-summary.txt"
run_agg "$td25e" "$sf25e" PASS 'AGENT_GATE_KEEP_LOGS=RESULT: PASS'
d25e=$(logs_field "$sf25e") || d25e=""
disp25e=$(logs_disposition "$sf25e")
sweep25e=$(sed -n 's/^logdir-sweep: //p' "$sf25e" | tail -1)
n25e=$(block_lines "$sf25e")
if [ "${n25e:-0}" = "${n25base:-0}" ]; then
  ok "AC25e: the reserved-token value added NO row to the block ($n25e vs baseline $n25base)"
else
  bad "AC25e: the reserved-token value changed the block from $n25base to $n25e lines"
  sed -n '1,40p' "$sf25e"
fi
case "$disp25e" in
  *WITHHELD*) ok "AC25e: the disposition carrying the reserved verdict token is WITHHELD, key intact ($disp25e)" ;;
  "")         bad "AC25e: the logdir-disposition: KEY vanished with its value — a withheld value must not take the key with it" ;;
  *)          bad "AC25e: the disposition was rendered with the reserved token in it ('$disp25e')" ;;
esac
if sed -n 's/^logdir-disposition: //p' "$sf25e" | grep -Eq 'RESULT: (PASS|FAIL)'; then
  bad "AC25e: the disposition line matches the completion probe's own pattern — an opt-out value would forge a terminal verdict"
else
  ok "AC25e: the disposition line does not match 'RESULT: (PASS|FAIL)' — the refusal quotes nothing"
fi
case "$sweep25e" in
  *WITHHELD*) ok "AC25e: the sweep census carrying the same token is WITHHELD too, its key intact ($sweep25e)" ;;
  "")         bad "AC25e: the logdir-sweep: KEY vanished with its value" ;;
  *)          bad "AC25e: the sweep census was rendered with the reserved token in it ('$sweep25e')" ;;
esac
# POSITIVE CONTROL, on an INDEPENDENT channel: the withheld wording quotes nothing, so
# the proof that the hostile value was OBSERVED is that it ENGAGED — only an engaged
# opt-out retains a PASS run's bundle, so a surviving directory means the value reached
# the engagement test and the claim renderer ran on it.
if [ -n "$d25e" ] && [ -d "$d25e" ]; then
  ok "AC25e: POSITIVE CONTROL — the hostile value ENGAGED the opt-out (the PASS run's bundle survives), so the withheld value was really produced and refused, not never generated"
else
  bad "AC25e: the hostile value did not engage the opt-out ('${d25e:-<none>}') — the withholding assertions measured nothing"
fi
case_floor AC25 19

# ---------------------------------------------------------------------------
# AC26 (roborev job 174, finding B): the `cli-tests` comment must not claim that
# `$LOG_DIR` is retained. STRUCTURAL, over the SHIPPED script.
# ---------------------------------------------------------------------------
# The comment said the other components' lane logs live under `$LOG_DIR`, "which is
# retained deliberately as the `logs:` bundle" — and after #3637 a terminal PASS REMOVES
# it, so both the contrast and the stated rationale for that block's own `rm -rf` trap
# are false. A comment fix with no guard silently rots again, and this repo's rule is
# that a false rationale in a comment is worse than none — so the stale phrasing is
# asserted ABSENT, and the reworded rationale asserted PRESENT alongside the cleanup it
# explains.
# ---------------------------------------------------------------------------
case_mark
if grep -q 'retained deliberately as the' "$GATE"; then
  bad "AC26: the shipped gate still claims a directory is 'retained deliberately as the ...' — after #3637 a terminal PASS REMOVES \$LOG_DIR, so that rationale is false"
  grep -n 'retained deliberately as the' "$GATE"
else
  ok "AC26: the stale 'retained deliberately as the ...' claim is ABSENT from the shipped gate"
fi
# The window is anchored on the cleanup this comment exists to explain, so the
# assertions below are about THAT block and not about the file at large.
cli_anchor=$(grep -n '_cli_tmp=\$(mktemp -d' "$GATE" | head -1 | cut -d: -f1)
if [ -n "$cli_anchor" ]; then
  ok "AC26: located the cli-tests private-tmpdir block in the shipped gate (line $cli_anchor)"
  cli_body=$(sed -n "$((cli_anchor - 20)),$((cli_anchor + 25))p" "$GATE")
  case "$cli_body" in
    *'trap "rm -rf \"$_cli_tmp\"" EXIT'*) ok "AC26: the _cli_tmp cleanup itself is still in place — the reword kept the mechanism, not just the prose" ;;
    *) bad "AC26: the _cli_tmp cleanup trap is no longer in that block — the reword removed the thing the comment explains" ;;
  esac
  case "$cli_body" in
    *'#3637'*) ok "AC26: the reworded rationale REFERENCES the #3637 disposition rather than a retention claim" ;;
    *) bad "AC26: the cli-tests cleanup rationale does not reference the #3637 disposition — the next reader cannot tell which artifacts survive a green run" ;;
  esac
  ac26_halves=0
  case "$cli_body" in *REMOVED*) ac26_halves=$((ac26_halves + 1)) ;; esac
  case "$cli_body" in *RETAINED*) ac26_halves=$((ac26_halves + 1)) ;; esac
  if [ "$ac26_halves" = 2 ]; then
    ok "AC26: and it states BOTH halves of the disposition (REMOVED on a terminal PASS, RETAINED with a named reason otherwise)"
  else
    bad "AC26: the reworded rationale states only $ac26_halves of the disposition's 2 halves — a half-stated lifetime is the same misleading contrast in a new spelling"
  fi
else
  bad "AC26: could not locate the cli-tests private-tmpdir block — every assertion below is UNMEASURED"
  bad "AC26: (cleanup-trap assertion not reached)"
  bad "AC26: (#3637 reference assertion not reached)"
  bad "AC26: (both-halves assertion not reached)"
fi
case_floor AC26 5

# ---------------------------------------------------------------------------
# AC27 (roborev job 175, finding 1): a control-bearing $TMPDIR is REFUSED AT THE
# CREATION SITE — before `mktemp -d`, so no block can be emitted at all.
# ---------------------------------------------------------------------------
# THE DEFECT: the two new keys render through `_summary_block_value`, but `logs:` on the
# adjacent line prints the raw `$LOG_DIR`. So `TMPDIR=$'/tmp/x\nRESULT: PASS'` STILL put a
# forged terminal-verdict line inside the SUMMARY block — through the one field the fix had
# to leave verbatim. One sanitised line beside a raw one is a block a reader (and the next
# maintainer) reasonably reads as safe from a class it is not.
#
# THE DECIDED FIX IS A REFUSAL, NOT MORE RENDERING. `logs:` is PATH-ONLY and byte-identical
# by rule (#3637/#3312 — `scripts/lib/gate-heartbeat.sh` renders the same field name from
# the same raw variable), so scrubbing it would break that rule AND leave a path that names
# no directory. Refusing the hostile INPUT closes the class for `logs:`, for `run-id:`, for
# the heartbeat's own `logs:` and for `logdir-disposition.txt` in ONE place, and a gate that
# cannot write a trustworthy log path cannot certify anyway.
#
# MEMBERSHIP IS NOT DETECTION, so this case measures four things: (a) the SHIPPED gate
# refuses BY NAME and publishes no verdict at all; (b) with the refusal DEFEATED the
# identical fixture really does forge a `RESULT: PASS` line — the detection proof, measured
# rather than argued; (c) a CLEAN $TMPDIR proceeds to `RESULT: PASS` through the same
# harness, so the case is about the hostile value and not about a broken fixture; and (d)
# structurally, the refusal and `_summary_block_value` share ONE definition of "control
# character", so the two can never disagree about what one is.
# ---------------------------------------------------------------------------
case_mark
td27="$tmp/ac27-forge
RESULT: PASS/td"
sf27="$tmp/ac27-refused.txt"
if mkdir -p "$td27" 2>/dev/null; then
  ok "AC27: precondition — a \$TMPDIR carrying a newline followed by the reserved token really exists on this filesystem"
  if run_agg "$td27" "$sf27" PASS; then
    bad "AC27: the SHIPPED gate ran to completion under a control-bearing \$TMPDIR — the creation-site refusal did not fire"
  else
    ok "AC27: the SHIPPED gate exited NON-ZERO under a control-bearing \$TMPDIR"
  fi
  out27=$(cat "$td27.out" 2>/dev/null || true)
  case "$out27" in
    *"agent-gate: REFUSED"*"control character"*)
      ok "AC27: the refusal is NAMED and says WHY (agent-gate: REFUSED … control character)" ;;
    *)
      bad "AC27: no named control-character refusal on the run's own output — a silent abort is indistinguishable from a crash"
      printf '%s\n' "$out27" | head -5 ;;
  esac
  ac27_remedy=0
  case "$out27" in *"unset TMPDIR"*) ac27_remedy=$((ac27_remedy + 1)) ;; esac
  case "$out27" in *"#3637"*) ac27_remedy=$((ac27_remedy + 1)) ;; esac
  if [ "$ac27_remedy" = 2 ]; then
    ok "AC27: the refusal prints the REMEDY (unset TMPDIR) and names the issue (#3637)"
  else
    bad "AC27: the refusal states only $ac27_remedy of its 2 required parts (remedy, issue reference) — an operator cannot act on it"
  fi
  # NO VERDICT ANYWHERE. The startup `RESULT: INCOMPLETE` sentinel is written far BELOW the
  # creation site, so a refused run publishes no summary file at all — and neither the
  # summary file nor the run's own output may carry a line matching the completion probe's
  # pattern.
  if [ -f "$sf27" ] && grep -Eq 'RESULT: (PASS|FAIL)' "$sf27" 2>/dev/null; then
    bad "AC27: the refused run PUBLISHED a terminal verdict into $sf27"
    grep -nE 'RESULT: (PASS|FAIL)' "$sf27" | head -3
  else
    ok "AC27: the refused run published NO terminal verdict (no summary block at all)"
  fi
  if printf '%s\n' "$out27" | grep -Eq 'RESULT: (PASS|FAIL)'; then
    bad "AC27: the refused run's own output carries a line matching the completion probe's pattern — the refusal itself is a forgery channel"
  else
    ok "AC27: and its output carries NO line matching 'RESULT: (PASS|FAIL)' — nothing downstream can read a verdict out of it"
  fi
  # REFUSE, DON'T QUOTE: a diagnostic reproducing the hostile value would forge the very
  # line the refusal prevents — the rule `_summary_block_value` already follows.
  case "$out27" in
    *ac27-forge*)
      bad "AC27: the refusal ECHOED the hostile \$TMPDIR — reproducing the value forges the line it refuses" ;;
    *)
      ok "AC27: the refusal does NOT echo the offending value (refuse, never quote)" ;;
  esac
  n27=$(count_logdirs "$td27")
  if [ "$n27" = 0 ]; then
    ok "AC27: and NO run directory was created under the hostile parent — the refusal precedes mktemp -d, so there is no husk to reclaim"
  else
    bad "AC27: $n27 run directory(ies) were created under the hostile parent — the refusal runs AFTER creation and leaks"
  fi
  # (b) THE DETECTION PROOF: the same fixture against the refusal-defeated copy AC24 built
  # and VERIFIED (one mutation, nothing else). Without the refusal the newline really does
  # forge a `RESULT: PASS` line — here the path's own tail follows the token, which is
  # exactly what the probe's substring pattern matches.
  sf27m="$tmp/ac27-defeated.txt"
  run_agg_gate "$gate24d" "$td27" "$sf27m" PASS
  if grep -q '^RESULT: PASS/' "$sf27m" 2>/dev/null; then
    ok "AC27 mutant: with the refusal defeated the hostile \$TMPDIR DOES forge a RESULT: PASS line inside the block — proved discriminating"
  else
    bad "AC27 mutant: the refusal-defeated copy forged no verdict line, so the assertions above cannot fail (fixture or mutation is vacuous)"
    grep -nE '^(run-id|logs|RESULT): ' "$sf27m" 2>/dev/null | head -5
  fi
  n27forged=$(grep -c '^RESULT: PASS' "$sf27m" 2>/dev/null || true)
  if [ "${n27forged:-0}" -ge 2 ]; then
    ok "AC27 mutant: its block carries $n27forged 'RESULT: PASS' lines — the real verdict plus the forged one, which is the whole vector"
  else
    bad "AC27 mutant: expected at least 2 'RESULT: PASS' lines in the defeated run's block, found ${n27forged:-0}"
  fi
  # The newline-bearing fixture is removed HERE, after its assertions and before the
  # survivor accounting below, which compares NEWLINE-SEPARATED sets of paths and cannot
  # represent a path containing one (the same reason AC24 removes its own).
  rm -rf "${td27%/td}"
else
  bad "AC27: could not create a control-bearing \$TMPDIR — the refusal is UNMEASURED"
  bad "AC27: (exit-status assertion not reached)"
  bad "AC27: (named-refusal assertion not reached)"
  bad "AC27: (remedy assertion not reached)"
  bad "AC27: (no-verdict assertion not reached)"
  bad "AC27: (no-forged-line assertion not reached)"
  bad "AC27: (refuse-dont-quote assertion not reached)"
  bad "AC27: (no-husk assertion not reached)"
  bad "AC27 mutant: (not reached)"
  bad "AC27 mutant: (line-count assertion not reached)"
fi
# (c) POSITIVE CONTROL: the identical harness with a CLEAN $TMPDIR reaches a terminal PASS.
# Without it, "the hostile run produced no verdict" is satisfied by a harness that produces
# no verdict for anybody.
td27c="$tmp/ac27-clean"; sf27c="$tmp/ac27-clean.txt"
run_agg "$td27c" "$sf27c" PASS
if grep -q '^RESULT: PASS' "$sf27c" 2>/dev/null; then
  ok "AC27: POSITIVE CONTROL — the same run under a CLEAN \$TMPDIR reached RESULT: PASS, so the refusal is about the hostile value and not about the harness"
else
  bad "AC27: POSITIVE CONTROL FAILED — a clean-\$TMPDIR run produced no RESULT: PASS, so every refusal assertion above is unattributable"
fi
# (d) ONE DEFINITION OF "CONTROL CHARACTER", structurally over the shipped gate. Two
# spellings would let the refusal and the strip disagree about what one IS — the per-site
# drift #3312 rules against.
cntrl_sites=$(grep -vE '^[[:space:]]*#' "$GATE" | grep -cF "tr -d '[:cntrl:]'" || true)
if [ "${cntrl_sites:-0}" = 1 ]; then
  ok "AC27: the shipped gate has EXACTLY ONE control-character class site in code (not counting comments)"
else
  bad "AC27: the shipped gate has ${cntrl_sites:-0} control-character class sites in code, expected exactly 1 — the refusal and the strip can now disagree about what a control character is"
  grep -nvE '^[[:space:]]*#' "$GATE" | grep -F "tr -d '[:cntrl:]'" | head -5
fi
if [ "$(sed -n '/^_gate_cntrl_strip() {/,/^}/p' "$GATE" | grep -cF "tr -d '[:cntrl:]'" || true)" = 1 ]; then
  ok "AC27: and that site is inside _gate_cntrl_strip — THE shared definition"
else
  bad "AC27: _gate_cntrl_strip does not hold the class definition — the shared helper is not where the class lives"
fi
if awk '/^_summary_block_value\(\) \{/,/^\}/' "$GATE" | grep -q '_gate_cntrl_strip'; then
  ok "AC27: _summary_block_value STRIPS through the shared helper"
else
  bad "AC27: _summary_block_value no longer strips through _gate_cntrl_strip — the SUMMARY boundary has its own class again"
fi
ref27_line=$(grep -nF '_gate_cntrl_strip "$GATE_LOGDIR_PARENT"' "$GATE" | head -1 | cut -d: -f1)
mk27_line=$(grep -nF 'LOG_DIR=$(mktemp -d "$GATE_LOGDIR_PARENT/agent-gate.XXXXXX"' "$GATE" | head -1 | cut -d: -f1)
if [ -n "$ref27_line" ] && [ -n "$mk27_line" ] && [ "$ref27_line" -lt "$mk27_line" ]; then
  ok "AC27: the refusal COMPARES through the shared helper and sits BEFORE the mktemp -d (line $ref27_line vs $mk27_line) — a refusal after creation would leak the husk it refuses"
else
  bad "AC27: the refusal is missing or does not precede the run-directory creation (refusal line '${ref27_line:-<none>}', mktemp line '${mk27_line:-<none>}')"
fi
case_floor AC27 12

# ---------------------------------------------------------------------------
# AC28 (roborev job 175, finding 3): the mtime-synthesis helper DISCRIMINATES, and
# every aged fixture in this file routes through it.
# ---------------------------------------------------------------------------
# THE DEFECT: AC4's `aged_keep` and AC5's `notours`/`foreign` synthesised their age with a
# bare `touch -d … 2>/dev/null || touch -t … 2>/dev/null` in which BOTH forms could fail
# SILENTLY, while AC5's loop was fail-closed. All three of those fixtures assert "must
# SURVIVE the sweep" — so an un-aged fixture survives TRIVIALLY and the case reports a pass
# having measured nothing.
#
# A FAIL-CLOSED HELPER IS ONLY WORTH ITS NAME IF IT REALLY FAILS, so the discrimination is
# MEASURED against a planted `touch` that always exits 1 — not asserted from the source. And
# the structural half is what stops the next fixture from quietly skipping the boundary: a
# per-site form is a thing to remember, and this file already proved that gets forgotten.
# ---------------------------------------------------------------------------
case_mark
SELF="$SCRIPT_DIR/test_agent_gate_logdir_cleanup.sh"
# (a) DISCRIMINATION: a `touch` that always fails, shadowing the real one on PATH.
shim28="$tmp/ac28-shim"; mkdir -p "$shim28"
printf '#!/bin/sh\nexit 1\n' >"$shim28/touch" && chmod +x "$shim28/touch"
probe28="$tmp/ac28-probe"; mkdir -p "$probe28"
if [ "$( PATH="$shim28:$PATH"; command -v touch )" = "$shim28/touch" ]; then
  ok "AC28: precondition — the failing touch shim really shadows the real one on PATH"
else
  bad "AC28: the touch shim does not shadow the real touch — the discrimination probe below measures nothing"
fi
# In a SUBSHELL, so the `bad` the helper is supposed to emit is CAPTURED and counted here
# once, deliberately, rather than reddening this suite.
if out28=$( PATH="$shim28:$PATH"; age_dir "$probe28" "AC28 shim probe" 2>&1 ); then
  bad "AC28: age_dir returned SUCCESS with a touch that always fails — an un-aged fixture sails straight through it"
else
  ok "AC28: age_dir returns NON-ZERO when neither touch form works"
fi
case "$out28" in
  *"could not synthesise an aged mtime"*)
    ok "AC28: and it reports a NAMED failure naming the fixture, rather than continuing silently" ;;
  *)
    bad "AC28: age_dir failed SILENTLY (output: '${out28:-<none>}') — which is the whole defect of the per-site form" ;;
esac
# (b) POSITIVE CONTROL: the same helper on the same shape of fixture, with a real `touch`,
# succeeds — so (a) is about the failing touch and not about a helper that never works.
probe28b="$tmp/ac28-probe-ok"; mkdir -p "$probe28b"
if age_dir "$probe28b" "AC28 positive control"; then
  ok "AC28: POSITIVE CONTROL — age_dir succeeds with a working touch"
else
  bad "AC28: POSITIVE CONTROL FAILED — age_dir could not age a fixture on this host, so every aged fixture in this file is unmeasured"
fi
if [ -n "$(find "$probe28b" -maxdepth 0 -mtime +7 2>/dev/null)" ]; then
  ok "AC28: and the aged fixture really READS as aged through the sweep's OWN predicate (find -mtime +7)"
else
  bad "AC28: the 'aged' fixture does not satisfy find -mtime +7 — the helper's verification is not the sweep's predicate"
fi
# (c) STRUCTURAL, over the SHIPPED test source: ONE mtime site, and the three fixtures the
# finding named all route through it.
if [ -f "$SELF" ]; then
  ok "AC28: located this suite's own shipped source for the structural half ($SELF)"
  # INVOCATIONS only: comment lines and the helper's own DIAGNOSTIC strings (which have to
  # name `touch -d`/`touch -t` to be useful) are not mtime synthesis, so both classes are
  # excluded by shape rather than by a spelling nobody would maintain.
  touch_sites=$(grep -vE '^[[:space:]]*#' "$SELF" | grep -vE 'bad "|say BAD ' | grep -cE 'touch -[dt] ' || true)
  if [ "${touch_sites:-0}" = 1 ]; then
    ok "AC28: exactly ONE mtime-synthesis site in code across the whole file (comments and diagnostic strings excluded)"
  else
    bad "AC28: ${touch_sites:-0} mtime-synthesis invocations in code, expected exactly 1 — a fixture can age itself without the fail-closed check again"
    grep -nvE '^[[:space:]]*#' "$SELF" | grep -vE 'bad "|say BAD ' | grep -E 'touch -[dt] ' | head -5
  fi
  if [ "$(sed -n '/^_age_dir_apply() {/,/^}/p' "$SELF" | grep -vE 'bad "' | grep -cE 'touch -[dt] ' || true)" = 1 ]; then
    ok "AC28: and that site is inside _age_dir_apply — THE one boundary"
  else
    bad "AC28: _age_dir_apply does not hold the mtime synthesis — the helper is not where it happens"
  fi
  for fx28 in aged_keep notours foreign; do
    if grep -qE "^[[:space:]]*age_dir \"\\\$$fx28\"" "$SELF"; then
      ok "AC28: the '$fx28' fixture (a must-SURVIVE assertion) is aged through age_dir"
    else
      bad "AC28: the '$fx28' fixture does not route through age_dir — if its mtime is never set it survives trivially and its case measures nothing"
    fi
  done
else
  bad "AC28: could not read this suite's own source at $SELF — the structural half is UNMEASURED"
  bad "AC28: (one-site assertion not reached)"
  bad "AC28: (inside-helper assertion not reached)"
  bad "AC28: (aged_keep route not reached)"
  bad "AC28: (notours route not reached)"
  bad "AC28: (foreign route not reached)"
fi
case_floor AC28 10

# ---------------------------------------------------------------------------
# AC29 (roborev job 175, finding 2): the census's `.ansi-stripped` removal no longer
# justifies itself with a bundle "every gate keeps". STRUCTURAL, over the SHIPPED script.
# ---------------------------------------------------------------------------
# Same stale-rationale class as AC26, one function over: the comment justified the removal by
# "it would silently double the size of the `logs:` bundle every gate keeps" — false since
# #3637, where a terminal `RESULT: PASS` REMOVES the bundle, i.e. on the common disposition
# there is no kept bundle to double. A comment fix with no guard rots again, so the stale
# phrasing is asserted ABSENT and the surviving rationale — a derived duplicate consumed by
# the two tallies and read by nothing afterwards, which holds on BOTH dispositions —
# asserted PRESENT, alongside the removal it explains.
# ---------------------------------------------------------------------------
case_mark
if grep -q 'bundle every gate keeps' "$GATE"; then
  bad "AC29: the shipped gate still claims the '\$LOG_DIR' bundle is one 'every gate keeps' — after #3637 a terminal PASS REMOVES it"
  grep -n 'bundle every gate keeps' "$GATE"
else
  ok "AC29: the stale 'bundle every gate keeps' claim is ABSENT from the shipped gate"
fi
ans_anchor=$(grep -n 'ansi-stripped` sibling is a DERIVED DUPLICATE' "$GATE" | head -1 | cut -d: -f1)
if [ -n "$ans_anchor" ]; then
  ok "AC29: located the reworded .ansi-stripped rationale in the shipped gate (line $ans_anchor)"
  ans_body=$(sed -n "$ans_anchor,$((ans_anchor + 16))p" "$GATE")
  case "$ans_body" in
    *"read by"*NOTHING*)
      ok "AC29: it states the SURVIVING rationale — a derived duplicate read by nothing after the tallies" ;;
    *)
      bad "AC29: the reworded rationale does not state that the sibling has no reader after the census — the reason that holds on BOTH dispositions" ;;
  esac
  ans_halves=0
  case "$ans_body" in *'#3637'*) ans_halves=$((ans_halves + 1)) ;; esac
  case "$ans_body" in *REMOVES*) ans_halves=$((ans_halves + 1)) ;; esac
  if [ "$ans_halves" = 2 ]; then
    ok "AC29: and it records WHY the old claim was false (#3637 REMOVES the bundle on a terminal PASS), so the next reader cannot restore it"
  else
    bad "AC29: the reword states only $ans_halves of the 2 parts that keep it from rotting back (the #3637 reference, the removal-on-PASS fact)"
  fi
  case "$ans_body" in
    *'rm -f "$src" 2>/dev/null || true'*)
      ok "AC29: the removal itself is still in place — the reword kept the MECHANISM, not just the prose" ;;
    *)
      bad "AC29: the .ansi-stripped removal is no longer in that block — the reword removed the thing the comment explains" ;;
  esac
else
  bad "AC29: could not locate the reworded .ansi-stripped rationale — every assertion below is UNMEASURED"
  bad "AC29: (surviving-rationale assertion not reached)"
  bad "AC29: (why-the-old-claim-was-false assertion not reached)"
  bad "AC29: (mechanism-intact assertion not reached)"
fi
case_floor AC29 5

# ---------------------------------------------------------------------------
# Hermeticity: this file's own gate runs leave nothing outside their scratch dirs.
# ---------------------------------------------------------------------------
# SET EQUALITY, never a count comparison, and never a `-ge` bound (#3637, roborev job
# 66 finding 3). The previous form asked `discovered >= expected`, which passes for ANY
# number of leaked directories — i.e. it could not fail in the only direction this
# issue is about — and its bound was not even the right number: three further expected
# survivors (the fresh sweep decoy, the retained nested-private dir, the incomplete
# run's bundle) were named in a comment and counted nowhere. Every survivor is now
# enumerated with the case that owns it, and an unexpected directory is NAMED in the
# failure rather than silently absorbed.
expected_dirs=""
expect_dir() {   # expect_dir <path> <owning-case>
  [ -n "${1:-}" ] || return 0
  expected_dirs="${expected_dirs}$1"$'\t'"$2"$'\n'
}
expect_dir "$d2"                    "AC2 retained FAIL run"
expect_dir "$d4"                    "AC4 AGENT_GATE_KEEP_LOGS run"
expect_dir "$aged_keep"             "AC4 aged decoy the suppressed sweep must not take"
expect_dir "${inside_summary%/summary-primary.txt}" "AC3/A retained nested-private run"
expect_dir "${rel_summary%/summary-primary.txt}" "AC3/C retained nested-private run under a relative TMPDIR"
expect_dir "$fresh"                 "AC5 fresh sweep decoy"
expect_dir "$aged_live"             "AC5 aged decoy with a LIVE owner"
expect_dir "$aged_unk"              "AC5 aged decoy with no owner marker (unverifiable)"
# Empty where the owner-marker capability is present; on a host with no establishable
# owner token the aged decoy the sweep WOULD take is KEPT, and this file proves it is.
expect_dir "${ac5_degraded_survivor:-}" "AC5 aged decoy KEPT: no establishable owner token, so cannot-tell"
expect_dir "$d6"                    "AC6 summary-integrity no-clobber run"
expect_dir "$d7"                    "AC6 incomplete run's post-mortem bundle"
expect_dir "$d8"                    "AC7 run under a \" (\"-bearing TMPDIR"
expect_dir "$d11"                   "AC11a run whose summary write failed"
expect_dir "${d11b:-}"              "AC11b SIGTERMed run's post-mortem bundle"
expect_dir "${d11c:-}"              "AC11c run whose removal was refused by the rm shim"
expect_dir "${d9b:-}"               "AC8 AGENT_GATE_KEEP_LOGS early-exit retention"
expect_dir "${d21:-}"               "AC21 file-size OPT-OUT run whose disclosure pins the retention"
expect_dir "${d22:-}"               "AC22 --only diagnostic run whose product is its component log"
expect_dir "${d23:-}"               "AC23 non-zero early exit retained for its one component log"
expect_dir "${d23m:-}"              "AC23 mutant husk the pre-fix owner-marker-only predicate retains"
expect_dir "${d25a:-}"              "AC25a lenient unconventional opt-out value (=no) retention"
expect_dir "${d25b:-}"              "AC25b documented opt-out value (=1) rendering control"
expect_dir "${d25d0:-}"             "AC25d retaining baseline for the block line count"
expect_dir "${d25d:-}"              "AC25d control-character opt-out value retention"
expect_dir "${d25e:-}"              "AC25e reserved-token opt-out value retention"
for n in 1 2 3 4 5; do
  expect_dir "$td15/agent-gate.CAP00$n" "AC15 aged candidate the refusing rm shim could not remove"
done
# AC18's whole population survives BY DESIGN — markerless, therefore cannot-tell,
# therefore KEPT — and the case asserts that as a set above, so this enumerates a
# measured list too.
exam_n=0
while [ "$exam_n" -lt "$EXAM_POP" ]; do
  exam_n=$((exam_n + 1))
  expect_dir "$(printf '%s/agent-gate.EXM%03d' "$exam_parent" "$exam_n")" \
    "AC18 markerless aged candidate the sweep must never reclaim"
done
# AC19's population survives BY DESIGN — its removal stub REFUSES every removal, which
# is what makes `removals attempted` the signal that case reads — so this enumerates a
# measured list too.
scan_n=0
while [ "$scan_n" -lt "$SCAN_POP" ]; do
  scan_n=$((scan_n + 1))
  expect_dir "$(printf '%s/agent-gate.SCN%03d' "$scan_parent" "$scan_n")" \
    "AC19 aged candidate the refusing removal stub could not remove"
done
# AC17's survivors are the three candidates its stub REFUSES, proven as a set by the
# driver's own assertion above — so this enumerates a measured list, not a leak.
for rs in ${rot_survivors:-}; do
  expect_dir "$rot_parent/$rs" "AC17 aged candidate the refusing removal stub could not remove"
done
# AC20's survivors are the substituted pathname the sweep declined and the three
# removal-guard rows whose subject changed under it — a measured set, each proven
# present by the driver's own assertion above.
for rv in ${race_survivors:-}; do
  expect_dir "$rv" "AC20 substituted pathname the removal site declined"
done
# The discovered set is UNBOUNDED IN DEPTH on purpose: the AC7 case runs under
# "$tmp/paren (dir)/td", so its bundle sits at depth 3 and the old -maxdepth 2 scan
# could not see it — an expected survivor the accounting never looked at.
discovered_listing=$(find "$tmp" -type d -name 'agent-gate.*' 2>/dev/null); discovered_rc=$?
if [ "$discovered_rc" -ne 0 ]; then
  # #1699's find-tristate rule: an unreadable scan is UNMEASURED, never "no strays".
  bad "hermetic: could not scan $tmp for agent-gate.* directories (find rc=$discovered_rc) — UNMEASURED, not clean"
else
  exp_sorted=$(printf '%s' "$expected_dirs" | cut -f1 | sort -u)
  got_sorted=$(printf '%s\n' "$discovered_listing" | sed '/^$/d' | sort -u)
  unexpected=$(comm -13 <(printf '%s\n' "$exp_sorted") <(printf '%s\n' "$got_sorted"))
  missing=$(comm -23 <(printf '%s\n' "$exp_sorted") <(printf '%s\n' "$got_sorted"))
  if [ -z "$unexpected" ] && [ -z "$missing" ]; then
    ok "hermetic: the agent-gate.* directories surviving under the scratch root are EXACTLY the $(printf '%s\n' "$exp_sorted" | sed '/^$/d' | wc -l | tr -d ' ') expected survivors"
  fi
  if [ -n "$unexpected" ]; then
    while IFS= read -r u; do
      [ -n "$u" ] || continue
      bad "hermetic: UNEXPECTED surviving directory '$u' — no case in this file expects it (a leak, or a survivor nobody enumerated)"
    done <<EOF
$unexpected
EOF
  fi
  if [ -n "$missing" ]; then
    while IFS= read -r m; do
      [ -n "$m" ] || continue
      owner=$(printf '%s' "$expected_dirs" | awk -F'\t' -v p="$m" '$1==p{print $2; exit}')
      bad "hermetic: EXPECTED survivor '$m' (${owner:-unattributed}) is GONE — a retention this suite proves was not honoured"
    done <<EOF
$missing
EOF
  fi
fi

# ---------------------------------------------------------------------------
# SUITE-WIDE CASE FLOOR (#3544's lesson, applied to the whole file).
# ---------------------------------------------------------------------------
# The per-case floors above each live INSIDE the case they bound, so a span-replacing edit
# that deletes a case deletes its floor with it and the suite reports a green SUBSET of
# itself — the same defect one level up. This assertion lives at the TALLY, which is the one
# place such an edit cannot remove without being obvious.
#
# A FLOOR, not an equality, and the margin is deliberate: the owner-marker capability is a
# LINUX-ONLY dependency (AC5/AC15/AC17/AC18/AC19/AC20 assert the keep-everything degradation
# instead where it is absent) and those branches do not emit the same number of verdicts, so
# an exact total would red on macOS for a reason that is not a regression. Measured 261 on
# this fleet's Linux boxes (226 before AC27/AC28/AC29 raised it by 34 unconditional verdicts,
# 200 before AC25/AC26 raised it by 26); the floor is what notices a DELETED CASE — every
# case in this file contributes at least 5 verdicts — rather than a drifting count.
_total_verdicts=$((PASS + FAIL))
if [ "$_total_verdicts" -ge 246 ]; then
  ok "suite floor: $_total_verdicts verdicts reported (floor 246) — no case was silently dropped"
else
  bad "suite floor: only $_total_verdicts verdicts reported (floor 246) — at least one case was deleted or died before its assertions"
fi

printf '\n%s\n' "scripts/tests/test_agent_gate_logdir_cleanup.sh   passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
