#!/usr/bin/env bash
# Self-test for scripts/flow/lane-builddir.sh (#4641). Hermetic: a fake CARGO_TARGET_DIR and a
# throwaway git repo under mktemp. Never touches the real target dir or any real worktree.
set -uo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
SCRIPT="$REPO_ROOT/scripts/flow/lane-builddir.sh"
PASS=0 FAIL=0
ok()  { echo "PASS - $1"; PASS=$((PASS + 1)); }
bad() { echo "FAIL - $1"; FAIL=$((FAIL + 1)); }
check() { if eval "$2"; then ok "$1"; else bad "$1"; fi; }

tmp=$(cd "$(mktemp -d)" && pwd -P); trap 'rm -rf "$tmp"' EXIT
export CARGO_TARGET_DIR="$tmp/cache/cargo-target"
unset CQLITE_SHARED_TARGET CARGO_BUILD_TARGET_DIR
mkdir -p "$CARGO_TARGET_DIR/wt"

# A throwaway repo with one live linked worktree.
main="$tmp/repo"; live="$tmp/wts/live"; gone="$tmp/wts/gone"
git init -q "$main" && git -C "$main" -c user.name=t -c user.email=t@t commit -q --allow-empty -m init
git -C "$main" worktree add -q "$live" -b live
git -C "$main" worktree add -q "$gone" -b gone

# shellcheck source=../lib/worktree-target-dir.sh
. "$REPO_ROOT/scripts/lib/worktree-target-dir.sh"
hash_of() { gate_worktree_target_hash "$(cd "$1" 2>/dev/null && pwd -P || printf '%s' "$1")"; }

run() { out=$(cd "$main" && bash "$SCRIPT" "$@" 2>&1); rc=$?; }
mkdir_lane() {  # <worktree path> -> creates its wt dir with a marker, prints the dir
  local d="$CARGO_TARGET_DIR/wt/$(hash_of "$1")"
  mkdir -p "$d/debug"; echo junk >"$d/debug/f"; printf '%s\n' "$1" >"$d/.worktree-root"; printf '%s' "$d"
}

# 1. path derivation equals the gate's: the gate's own function, and the real gate entry point.
gate_dir=$(gate_worktree_target_dir "$live")
run path "$live"
check "path == gate_worktree_target_dir (live worktree)" '[ "$rc" = 0 ] && [ "$out" = "$gate_dir" ]'
check "path ends in wt/<hash of physical root>" '[ "$out" = "$CARGO_TARGET_DIR/wt/$(hash_of "$live")" ]'
gate_cli=$(bash "$REPO_ROOT/scripts/lib/worktree-target-dir.sh" "$live")
check "path == scripts/lib/worktree-target-dir.sh CLI output" '[ "$out" = "$gate_cli" ]'
run path "$tmp/wts/never-existed"
check "path works for a nonexistent worktree" '[ "$rc" = 0 ] && [ "$out" = "$CARGO_TARGET_DIR/wt/$(hash_of "$tmp/wts/never-existed")" ]'
check "path has no side effects (no dir created)" '[ ! -e "$out" ]'
# The gate must build in the dir the helper names (agent-gate.sh sources the same lib).
check "agent-gate.sh sources the shared lib" 'grep -q "gate_worktree_target_dir \"\$REPO_ROOT\"" "$REPO_ROOT/scripts/agent-gate.sh"'

# 2. remove refuses outside wt/.
outside="$tmp/outside"; mkdir -p "$outside"; echo keep >"$outside/keep"
run remove --dir "$outside" --worktree-removed
check "remove refuses a dir outside wt/" '[ "$rc" = 2 ] && [ -f "$outside/keep" ]'
mkdir -p "$CARGO_TARGET_DIR/wt/notahash"; run remove --dir "$CARGO_TARGET_DIR/wt/notahash" --worktree-removed
check "remove refuses a non-16-hex child of wt/" '[ "$rc" = 2 ] && [ -d "$CARGO_TARGET_DIR/wt/notahash" ]'
mkdir -p "$CARGO_TARGET_DIR/wt/0123456789abcdef/nested"; run remove --dir "$CARGO_TARGET_DIR/wt/0123456789abcdef/nested" --worktree-removed
check "remove refuses a grandchild of wt/" '[ "$rc" = 2 ] && [ -d "$CARGO_TARGET_DIR/wt/0123456789abcdef/nested" ]'
ln -s "$outside" "$CARGO_TARGET_DIR/wt/aaaaaaaaaaaaaaaa"; run remove --dir "$CARGO_TARGET_DIR/wt/aaaaaaaaaaaaaaaa" --worktree-removed
check "remove refuses a symlinked lane dir and keeps its target" '[ "$rc" = 2 ] && [ -f "$outside/keep" ]'
rm -f "$CARGO_TARGET_DIR/wt/aaaaaaaaaaaaaaaa"; rm -rf "$CARGO_TARGET_DIR/wt/notahash" "$CARGO_TARGET_DIR/wt/0123456789abcdef"

# 3. remove refuses a live worktree.
live_dir=$(mkdir_lane "$live")
run remove "$live"
check "remove refuses a live worktree" '[ "$rc" = 2 ] && [ -d "$live_dir" ] && grep -q "still a live git worktree" <<<"$out"'
run remove --dir "$live_dir"
check "remove --dir refuses a live worktree via its marker" '[ "$rc" = 2 ] && [ -d "$live_dir" ]'
run remove "$live" --worktree-removed
check "remove still refuses when --worktree-removed lies and the checkout exists" '[ "$rc" = 2 ] && [ -d "$live_dir" ]'

# 4. stale lists an orphan and not a live one.
gone_dir=$(mkdir_lane "$gone")
git -C "$main" worktree remove --force "$gone"
orphan_dir=$(mkdir_lane "$tmp/wts/orphan")
run stale
check "stale exits 0 and is report-only" '[ "$rc" = 0 ] && [ -d "$orphan_dir" ] && [ -d "$gone_dir" ] && [ -d "$live_dir" ]'
check "stale lists the orphan" 'grep -Fq "$orphan_dir" <<<"$out"'
check "stale lists the removed worktree's dir" 'grep -Fq "$gone_dir" <<<"$out"'
check "stale does not list the live worktree's dir" '! grep -Fq "$live_dir" <<<"$out"'
check "stale reports a total" 'grep -q "total: 2 stale dir" <<<"$out"'

# 5. remove after the worktree is gone deletes exactly its dir.
run remove "$gone"
check "remove deletes the removed worktree's dir" '[ "$rc" = 0 ] && [ ! -e "$gone_dir" ]'
check "remove left the orphan and the live dir alone" '[ -d "$orphan_dir" ] && [ -d "$live_dir" ]'
run remove "$gone"
check "remove is idempotent (absent dir is rc 0)" '[ "$rc" = 0 ]'
run remove --dir "$orphan_dir"
check "remove --dir deletes an orphan whose marker root is gone" '[ "$rc" = 0 ] && [ ! -e "$orphan_dir" ]'

echo "lane-builddir: $PASS passed, $FAIL failed"
[ "$FAIL" = 0 ]
