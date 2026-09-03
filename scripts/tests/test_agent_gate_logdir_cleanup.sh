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
touch -d '30 days ago' "$aged_keep" 2>/dev/null || touch -t 202001010000 "$aged_keep"
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
  if ! touch -d '30 days ago' "$aged_path" 2>/dev/null; then
    touch -t 202001010000 "$aged_path" 2>/dev/null \
      || bad "AC5: could not synthesise an aged mtime with touch -d or -t ($aged_path)"
  fi
done
touch -d '30 days ago' "$notours" 2>/dev/null || touch -t 202001010000 "$notours" 2>/dev/null
touch -d '30 days ago' "$foreign"  2>/dev/null || touch -t 202001010000 "$foreign"  2>/dev/null
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
  touch -d '30 days ago' "$d" 2>/dev/null || touch -t 202001010000 "$d" 2>/dev/null
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
  touch -d '30 days ago' "$d" 2>/dev/null || touch -t 202001010000 "$d" 2>/dev/null
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
    touch -d '30 days ago' "$d" 2>/dev/null || touch -t 202001010000 "$d" 2>/dev/null
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
  touch -d '30 days ago' "$d" 2>/dev/null || touch -t 202001010000 "$d" 2>/dev/null
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
  touch -d '30 days ago' "$d" 2>/dev/null || touch -t 202001010000 "$d" 2>/dev/null
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

age_it() { touch -d '30 days ago' "$1" 2>/dev/null || touch -t 202001010000 "$1" 2>/dev/null; }

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

printf '\n%s\n' "scripts/tests/test_agent_gate_logdir_cleanup.sh   passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
