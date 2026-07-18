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
  fail)  exit 1 ;;
  sleep) sleep 15; exit 0 ;;
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
      ISSUE_GATE_TEST_CMD="scripts/agent-gate.sh --lite" \
      ISSUE_GATE_COVERAGE_CMD="" \
      ISSUE_GATE_ROBOREV="1" \
      ISSUE_GATE_LITE_BUDGET_SECS="${BUDGET:-480}" \
      FAKE_GATE_MODE="${FAKE_GATE_MODE:-pass}" \
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
FAKE_GATE_MODE=fail run_hook "$repoB"
rcB=$?
if [ "$rcB" -eq 2 ]; then
  ok "genuine lite FAIL blocks task completion (exit 2)"
else
  bad "lite FAIL produced exit $rcB (expected 2)"
fi

# --- Case C: budget exceeded FAILS OPEN (exit 0 + warning) ----------------------
repoC="$tmp/repoC"
build_repo "$repoC"
: >"$RECORD"
warn_out="$tmp/warn.txt"
FAKE_GATE_MODE=sleep BUDGET=2 run_hook "$repoC" 2>"$warn_out"
rcC=$?
if [ "$rcC" -eq 0 ]; then
  ok "timeout path fails OPEN (exit 0), never blocking on a budget overrun"
else
  bad "timeout path exited $rcC (expected 0 — fail open)"
fi
if grep -q 'FAILING OPEN' "$warn_out"; then
  ok "timeout path emits a visible FAILING OPEN warning"
else
  bad "timeout path did not emit a 'FAILING OPEN' warning"
fi

# --- Case D: no Rust diff vs origin/main -> cheap skip, gate never runs ----------
repoD="$tmp/repoD"
build_repo "$repoD"
# Advance origin/main to HEAD so there is no Rust diff.
( cd "$repoD" && git update-ref refs/remotes/origin/main HEAD )
: >"$RECORD"
FAKE_GATE_MODE=pass run_hook "$repoD"
rcD=$?
if [ "$rcD" -eq 0 ]; then
  ok "no-Rust-diff path exits 0"
else
  bad "no-Rust-diff path exited $rcD (expected 0)"
fi
if [ ! -s "$RECORD" ]; then
  ok "no-Rust-diff path skips the gate entirely (gate never invoked)"
else
  bad "no-Rust-diff path invoked the gate (record: $(cat "$RECORD"))"
fi

echo
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
