#!/usr/bin/env bash
# Regression test for issue #2671 (epic #2664 harness audit): the TaskCompleted
# issue-gate hook (.claude/hooks/issue-gate.sh) is DEFUSED. The prior version ran
# the FULL gate (12-25 min) under the hook's 600s timeout, from the session cwd,
# plus a synchronous `roborev --wait`. This test proves the defusal holds:
#   (a) the wired test command is the LITE gate (--lite), never the full gate;
#   (b) the gate is invoked with a UNIQUE mktemp AGENT_GATE_SUMMARY_FILE and from
#       the REPO ROOT derived from the hook's own location (not the session cwd);
#   (c) a check that exceeds its wall-clock budget FAILS OPEN (exit 0 + warning),
#       never blocking task completion on a timeout;
#   (d) no roborev invocation remains — statically and behaviorally;
#   plus: a genuine lite FAIL still blocks (exit 2), and a no-Rust-diff repo skips.
#
# Fast + hermetic by design: it never runs the real gate. It builds a throwaway git
# repo, copies the real hook into it, and PATH/relative-path-shims the gate binary
# and roborev with recorders. FAILS against the pre-#2671 hook, PASSES with the fix.
#
# Run standalone:   bash scripts/tests/test_issue_gate_hook.sh
# Or via the gate:  scripts/agent-gate.sh runs it in the tooling-tests component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
HOOK_SRC="$REPO_ROOT/.claude/hooks/issue-gate.sh"
SETTINGS="$REPO_ROOT/.claude/settings.json"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

if [ ! -f "$HOOK_SRC" ]; then
  echo "FAIL - hook not found at $HOOK_SRC" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Static assertions on the wiring
# ---------------------------------------------------------------------------

# settings.json wires the LITE gate, not the bare full gate.
if grep -q '"ISSUE_GATE_TEST_CMD": "scripts/agent-gate.sh --lite"' "$SETTINGS"; then
  ok "settings.json wires ISSUE_GATE_TEST_CMD to the --lite gate"
else
  bad "settings.json does not wire ISSUE_GATE_TEST_CMD to 'scripts/agent-gate.sh --lite'"
fi

# The hook source contains no ACTIVE roborev invocation (comments allowed).
if grep -nE '^[[:space:]]*[^#].*roborev[[:space:]]+(review|wait)' "$HOOK_SRC" >/dev/null 2>&1; then
  bad "hook still contains an active roborev invocation"
else
  ok "hook contains no active roborev invocation (static)"
fi

# ---------------------------------------------------------------------------
# Behavioral harness: build a throwaway repo and drive the real hook
# ---------------------------------------------------------------------------
tmp=$(mktemp -d "${TMPDIR:-/tmp}/issue-gate-hook-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

RECORD="$tmp/gate-record.txt"
ROBOREV_SENTINEL="$tmp/roborev-was-called.txt"
SLEEP_FINISHED_MARKER="$tmp/sleep-grandchild-finished.txt"
SLEEP_PID_FILE="$tmp/sleep-grandchild-pid.txt"
# An EMPTY slots dir every case points at by default, so a real full gate running
# on this machine (e.g. when this self-test runs INSIDE the gate) can never make the
# hook's slot-aware skip fire and flip an unrelated assertion. The slot-skip case
# overrides SLOTS_DIR with a dir holding a genuinely locked slot.
EMPTY_SLOTS="$tmp/empty-slots"
mkdir -p "$EMPTY_SLOTS"

# Build a fake repo with the hook and a recording fake gate.
build_repo() {
  local repo="$1"
  rm -rf "$repo"
  mkdir -p "$repo/.claude/hooks" "$repo/scripts" "$repo/src"
  cp "$HOOK_SRC" "$repo/.claude/hooks/issue-gate.sh"
  chmod +x "$repo/.claude/hooks/issue-gate.sh"

  # Recording fake gate: logs argv, the summary-file env, and its cwd; behavior
  # is driven by FAKE_GATE_MODE (pass|fail|sleep).
  cat >"$repo/scripts/agent-gate.sh" <<'EOF'
#!/usr/bin/env bash
{
  echo "ARGS:$*"
  echo "SUMMARY:${AGENT_GATE_SUMMARY_FILE:-}"
  echo "CWD:$PWD"
} >>"$GATE_RECORD"
case "${FAKE_GATE_MODE:-pass}" in
  fail)
    # Emit a recognizable SUMMARY block so the hook's fail path can echo it.
    [ -n "${AGENT_GATE_SUMMARY_FILE:-}" ] && printf 'AGENT-GATE-LITE-SUMMARY-SENTINEL\nRESULT: FAIL\n' >"$AGENT_GATE_SUMMARY_FILE"
    exit 1 ;;
  # Record this gate process's PID, then sleep LONG (30s) past a small budget and
  # only then drop a marker. The de-flake: the assertion does not race the marker —
  # it proves the process group was actually killed by checking the recorded PID is
  # gone (bounded poll), well before the 30s marker could ever appear. If the hook
  # killed only the direct child, this grandchild survives — kill -0 stays true.
  sleep) echo "$$" >"$SLEEP_PID_FILE"; sleep 30; echo done >"$SLEEP_FINISHED_MARKER"; exit 0 ;;
  *)     exit 0 ;;
esac
EOF
  chmod +x "$repo/scripts/agent-gate.sh"

  ( cd "$repo" || exit 1
    git init -q
    git config user.email t@t
    git config user.name t
    git add -A
    git commit -qm base
    # origin/main = base commit (no Rust yet)
    git update-ref refs/remotes/origin/main HEAD
    # add a Rust file so there IS a diff vs origin/main
    echo 'pub fn f() {}' >src/lib.rs
    git add src/lib.rs
    git commit -qm rust
  )
}

# Run the hook from an UNRELATED cwd, so any repo-root reliance on cwd would break.
run_hook() {
  local repo="$1"
  ( cd "$tmp" && echo '{}' | \
      GATE_RECORD="$RECORD" \
      SLEEP_FINISHED_MARKER="$SLEEP_FINISHED_MARKER" \
      SLEEP_PID_FILE="$SLEEP_PID_FILE" \
      ISSUE_GATE_TEST_CMD="scripts/agent-gate.sh --lite" \
      ISSUE_GATE_COVERAGE_CMD="${COV:-}" \
      ISSUE_GATE_ROBOREV="1" \
      ISSUE_GATE_LITE_BUDGET_SECS="${BUDGET:-480}" \
      FAKE_GATE_MODE="${FAKE_GATE_MODE:-pass}" \
      CQLITE_GATE_SLOTS_DIR="${SLOTS_DIR:-$EMPTY_SLOTS}" \
      PATH="$tmp/shim:$PATH" \
      bash "$repo/.claude/hooks/issue-gate.sh" )
}

# PATH shim: a roborev that trips a sentinel if EVER called.
mkdir -p "$tmp/shim"
cat >"$tmp/shim/roborev" <<EOF
#!/usr/bin/env bash
echo "called" >"$ROBOREV_SENTINEL"
exit 0
EOF
chmod +x "$tmp/shim/roborev"

# --- Case A: PASS path — --lite, unique summary, repo-root cwd, no roborev -------
repoA="$tmp/repoA"
build_repo "$repoA"
: >"$RECORD"
rm -f "$ROBOREV_SENTINEL"
FAKE_GATE_MODE=pass run_hook "$repoA"
rcA=$?

if [ "$rcA" -eq 0 ]; then
  ok "PASS path: hook exits 0"
else
  bad "PASS path: hook exited $rcA (expected 0)"
fi

args_line=$(grep '^ARGS:' "$RECORD" | head -1)
if [ "$args_line" = "ARGS:--lite" ]; then
  ok "gate invoked with '--lite'"
else
  bad "gate argv was '$args_line' (expected 'ARGS:--lite')"
fi

summary_line=$(grep '^SUMMARY:' "$RECORD" | head -1)
summary_path=${summary_line#SUMMARY:}
if [ -n "$summary_path" ] && [[ "$summary_path" == *issue-gate-lite-summary* ]] \
   && [ "$summary_path" != ".agent-gate-lite-summary.txt" ]; then
  ok "gate ran with a unique mktemp AGENT_GATE_SUMMARY_FILE ($summary_path)"
else
  bad "gate summary path was '$summary_path' (expected a unique mktemp issue-gate-lite-summary path)"
fi

cwd_line=$(grep '^CWD:' "$RECORD" | head -1)
gate_cwd=${cwd_line#CWD:}
expected_root=$(cd "$repoA" && git rev-parse --show-toplevel)
if [ "$gate_cwd" = "$expected_root" ]; then
  ok "gate ran from the hook's repo root, not the session cwd ($gate_cwd)"
else
  bad "gate cwd was '$gate_cwd' (expected repo root '$expected_root')"
fi

if [ -f "$ROBOREV_SENTINEL" ]; then
  bad "roborev was invoked (sentinel present) — the hook must not run reviews"
else
  ok "roborev was never invoked (behavioral)"
fi

# --- Case B: genuine FAIL still blocks (exit 2) ---------------------------------
repoB="$tmp/repoB"
build_repo "$repoB"
: >"$RECORD"
failB_out="$tmp/failB.txt"
FAKE_GATE_MODE=fail run_hook "$repoB" 2>"$failB_out"
rcB=$?
if [ "$rcB" -eq 2 ]; then
  ok "genuine lite FAIL blocks task completion (exit 2)"
else
  bad "lite FAIL produced exit $rcB (expected 2)"
fi
# the SUMMARY block (the only retainable text) is echoed into the reason.
if grep -q 'AGENT-GATE-LITE-SUMMARY-SENTINEL' "$failB_out"; then
  ok "FAIL path echoes the lite SUMMARY block into the block reason"
else
  bad "FAIL path did not echo the lite SUMMARY block"
fi
# fix #1: the block reason names the log path, not the raw gate output.
if grep -q 'full output:' "$failB_out"; then
  ok "FAIL path names the log path in the block reason (raw output kept out of the reason)"
else
  bad "FAIL path did not name the log path"
fi

# --- Case C: budget exceeded FAILS OPEN + kills the whole process group ----------
# Load-proof (no timing-coupled marker race): the grandchild records its PID then
# sleeps 30s. We assert exit 0 + warning, then prove the process group is actually
# gone by polling the recorded PID with a bounded cap (never the 30s marker).
repoC="$tmp/repoC"
build_repo "$repoC"
: >"$RECORD"
rm -f "$SLEEP_FINISHED_MARKER" "$SLEEP_PID_FILE"
warn_out="$tmp/warn.txt"
out_c="$tmp/out_c.txt"
FAKE_GATE_MODE=sleep BUDGET=2 run_hook "$repoC" >"$out_c" 2>"$warn_out"
rcC=$?
if [ "$rcC" -eq 0 ]; then
  ok "timeout path fails OPEN (exit 0), never blocking on a budget overrun"
else
  bad "timeout path exited $rcC (expected 0 — fail open)"
fi
if grep -q 'FAILING OPEN' "$warn_out"; then
  ok "timeout path emits a visible FAILING OPEN warning (stderr)"
else
  bad "timeout path did not emit a 'FAILING OPEN' warning on stderr"
fi
# fix #4: the FAILING OPEN notice must also reach stdout (surfaced on exit-0).
if grep -q 'FAILING OPEN' "$out_c"; then
  ok "timeout path emits the FAILING OPEN notice on stdout too (reaches the session)"
else
  bad "timeout path did not emit the FAILING OPEN notice on stdout"
fi

# Read the grandchild PID (bounded poll — the file appears as soon as the shim starts).
gc_pid=""
waited=0
while [ "$waited" -lt 5 ]; do
  if [ -s "$SLEEP_PID_FILE" ]; then gc_pid=$(cat "$SLEEP_PID_FILE"); break; fi
  sleep 1; waited=$((waited + 1))
done
# Poll-with-cap (bounded ~6s) for the grandchild to be gone. The hook already
# TERM/KILLed the group during run_hook; this only confirms it, with generous margin.
gc_gone=0
if [ -n "$gc_pid" ]; then
  waited=0
  while [ "$waited" -lt 6 ]; do
    if kill -0 "$gc_pid" 2>/dev/null; then
      sleep 1; waited=$((waited + 1))
    else
      gc_gone=1; break
    fi
  done
fi
if [ "$gc_gone" -eq 1 ]; then
  ok "timeout path killed the whole process group — grandchild PID $gc_pid is gone (kill -0 fails)"
else
  bad "timeout path left the gate grandchild (PID '${gc_pid:-unknown}') alive — process-group kill failed"
fi
# The 30s marker must never have appeared (secondary sanity check; the process is dead).
if [ ! -f "$SLEEP_FINISHED_MARKER" ]; then
  ok "gate grandchild never wrote its finished marker (killed before the 30s sleep completed)"
else
  bad "gate grandchild wrote its finished marker — it outlived the timeout"
fi

# --- Case C2: fix #4 — a wired coverage command STILL runs after a fail-open timeout
repoC2="$tmp/repoC2"
build_repo "$repoC2"
: >"$RECORD"
rm -f "$SLEEP_PID_FILE"
cov_marker="$tmp/coverage-ran.txt"
rm -f "$cov_marker"
FAKE_GATE_MODE=sleep BUDGET=2 COV="touch $cov_marker" run_hook "$repoC2" >/dev/null 2>&1
rcC2=$?
if [ "$rcC2" -eq 0 ] && [ -f "$cov_marker" ]; then
  ok "fail-open timeout falls through — a wired coverage command still runs (exit 0)"
else
  bad "fail-open timeout did not run the wired coverage command (rc=$rcC2, marker $( [ -f "$cov_marker" ] && echo present || echo absent ))"
fi

# --- Case D: no Rust change (committed AND clean tree) -> cheap skip, gate never runs
repoD="$tmp/repoD"
build_repo "$repoD"
# Advance origin/main to HEAD so there is no committed Rust diff; tree is clean.
( cd "$repoD" && git update-ref refs/remotes/origin/main HEAD )
: >"$RECORD"
FAKE_GATE_MODE=pass run_hook "$repoD"
rcD=$?
if [ "$rcD" -eq 0 ]; then
  ok "no-Rust-change path exits 0"
else
  bad "no-Rust-change path exited $rcD (expected 0)"
fi
if [ ! -s "$RECORD" ]; then
  ok "no-Rust-change path skips the gate entirely (gate never invoked)"
else
  bad "no-Rust-change path invoked the gate (record: $(cat "$RECORD"))"
fi

# --- Case E: fix #3 — committed diff empty but a DIRTY *.rs working tree -> RUN ---
# The skip must consult the working tree; an uncommitted Rust edit must NOT be skipped.
repoE="$tmp/repoE"
build_repo "$repoE"
( cd "$repoE" && git update-ref refs/remotes/origin/main HEAD )  # no committed diff
echo '// uncommitted edit' >>"$repoE/src/lib.rs"                  # dirty *.rs working tree
: >"$RECORD"
FAKE_GATE_MODE=pass run_hook "$repoE"
rcE=$?
if [ "$rcE" -eq 0 ] && [ -s "$RECORD" ]; then
  ok "dirty *.rs working tree runs the gate even with no committed diff (exit 0, gate invoked)"
else
  bad "dirty *.rs working tree did not run the gate (rc=$rcE, record $( [ -s "$RECORD" ] && echo nonempty || echo empty ))"
fi

# --- Case F: fix #5 — a non-integer budget warns, defaults to 480, and RUNS -------
repoF="$tmp/repoF"
build_repo "$repoF"
: >"$RECORD"
warnF_out="$tmp/warnF.txt"
FAKE_GATE_MODE=pass BUDGET=abc run_hook "$repoF" 2>"$warnF_out"
rcF=$?
if [ "$rcF" -eq 0 ] && [ -s "$RECORD" ] && grep -q 'is not a non-negative integer' "$warnF_out"; then
  ok "non-integer budget warns, defaults to 480, and runs the gate normally"
else
  bad "non-integer budget mishandled (rc=$rcF, record $( [ -s "$RECORD" ] && echo nonempty || echo empty ), warned=$(grep -qc 'not a non-negative integer' "$warnF_out" && echo yes || echo no))"
fi

# --- Case G: fix #5 — hook invoked from a NON-git dir -> exit 0, gate never runs ---
# The hook resolves its repo root from its own location; outside any git repo that
# resolution fails, so it exits 0 without running anything.
nongit_hooks="$tmp/nongit/.claude/hooks"
mkdir -p "$nongit_hooks"
cp "$HOOK_SRC" "$nongit_hooks/issue-gate.sh"
chmod +x "$nongit_hooks/issue-gate.sh"
: >"$RECORD"
outG="$tmp/outG.txt"
( cd "$tmp" && echo '{}' | GATE_RECORD="$RECORD" ISSUE_GATE_TEST_CMD="scripts/agent-gate.sh --lite" \
    CQLITE_GATE_SLOTS_DIR="$EMPTY_SLOTS" PATH="$tmp/shim:$PATH" \
    bash "$nongit_hooks/issue-gate.sh" ) >"$outG" 2>&1
rcG=$?
if [ "$rcG" -eq 0 ] && [ ! -s "$RECORD" ] && grep -q 'could not resolve the repo root' "$outG"; then
  ok "non-git invocation exits 0 and never invokes the gate"
else
  bad "non-git invocation mishandled (rc=$rcG, record $( [ -s "$RECORD" ] && echo nonempty || echo empty ))"
fi

# --- Case H: fix #2 — a full gate holding a slot -> advisory check skipped ---------
if command -v python3 >/dev/null 2>&1; then
  repoH="$tmp/repoH"
  build_repo "$repoH"
  live_slots="$tmp/live-slots"
  mkdir -p "$live_slots"
  slot_ready="$tmp/slot-ready.txt"
  rm -f "$slot_ready"
  # Background holder: flock slot.0 (LOCK_EX) and hold it, mimicking the live daemon.
  python3 - "$live_slots" "$slot_ready" >/dev/null 2>&1 <<'PY' &
import sys, os, fcntl, time
d, ready = sys.argv[1], sys.argv[2]
os.makedirs(d, exist_ok=True)
fd = os.open(os.path.join(d, "slot.0"), os.O_RDWR | os.O_CREAT, 0o644)
fcntl.flock(fd, fcntl.LOCK_EX)
open(ready, "w").write("ok")
time.sleep(30)
PY
  holder_pid=$!
  # Wait for the holder to actually hold the lock (bounded).
  waited=0
  while [ "$waited" -lt 5 ] && [ ! -s "$slot_ready" ]; do sleep 1; waited=$((waited + 1)); done
  : >"$RECORD"
  outH="$tmp/outH.txt"
  FAKE_GATE_MODE=pass SLOTS_DIR="$live_slots" run_hook "$repoH" >"$outH" 2>&1
  rcH=$?
  kill "$holder_pid" 2>/dev/null
  wait "$holder_pid" 2>/dev/null
  if [ "$rcH" -eq 0 ] && [ ! -s "$RECORD" ] && grep -q 'full gate active — advisory check skipped' "$outH"; then
    ok "full gate holding a slot -> advisory check skipped (exit 0, gate never invoked)"
  else
    bad "slot-aware skip failed (rc=$rcH, record $( [ -s "$RECORD" ] && echo nonempty || echo empty ), notice=$(grep -qc 'advisory check skipped' "$outH" && echo yes || echo no))"
  fi
else
  ok "slot-aware skip case SKIPPED (no python3 — the hook's slot probe also no-ops without it)"
fi

echo
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
