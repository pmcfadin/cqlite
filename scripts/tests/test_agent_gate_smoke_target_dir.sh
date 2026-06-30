#!/usr/bin/env bash
# Regression test for issue #1247: the agent-gate smoke step must resolve the
# just-built CLI binary honoring CARGO_TARGET_DIR. When the gate runs from a git
# worktree that shares a target dir via CARGO_TARGET_DIR (the disk-saving pattern
# for parallel worktrees), cargo writes the binary to "$CARGO_TARGET_DIR/debug",
# NOT "$PWD/target/debug" -- so a hardcoded "$PWD/target/debug/cqlite" makes the
# smoke step FAIL with "CQLITE_CLI is set but not executable". The fix resolves
# the path as "${CARGO_TARGET_DIR:-$PWD/target}/debug/cqlite".
#
# Fast + hermetic by design: never builds cargo, never runs the real gate. It
# (a) statically asserts agent-gate.sh contains the CARGO_TARGET_DIR-aware form
# and no longer hardcodes the broken "$PWD/target/debug/cqlite", and (b) evaluates
# the documented resolution expression in both modes (CARGO_TARGET_DIR set vs
# unset) against stubbed binary locations to prove behavior, not just text.
#
# This test FAILS against the old hardcoded behavior and PASSES with the fix.
#
# Run standalone:   bash scripts/tests/test_agent_gate_smoke_target_dir.sh
# Or via the gate:  scripts/agent-gate.sh runs it as part of the tooling-tests component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

# 1) Static: the gate must use the CARGO_TARGET_DIR-aware resolution form.
if grep -q 'CQLITE_CLI="\${CARGO_TARGET_DIR:-\$PWD/target}/debug/cqlite"' "$GATE"; then
  ok "gate resolves CQLITE_CLI via \${CARGO_TARGET_DIR:-\$PWD/target}/debug/cqlite"
else
  bad "gate is missing the CARGO_TARGET_DIR-aware CQLITE_CLI resolution"
fi

# 2) Static: the gate must NOT hardcode the broken "$PWD/target/debug/cqlite".
if grep -q 'CQLITE_CLI="\$PWD/target/debug/cqlite"' "$GATE"; then
  bad "gate still hardcodes CQLITE_CLI=\"\$PWD/target/debug/cqlite\" (the #1247 bug)"
else
  ok "gate no longer hardcodes CQLITE_CLI=\"\$PWD/target/debug/cqlite\""
fi

# resolve_cli mirrors the exact expression the gate uses, so we exercise the real
# resolution semantics rather than re-implementing them.
resolve_cli() { printf '%s' "${CARGO_TARGET_DIR:-$PWD/target}/debug/cqlite"; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-smoke-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

# 3) CARGO_TARGET_DIR set -> binary resolved under the shared target dir, which
#    is where cargo would actually place it in a worktree. The hardcoded
#    "$PWD/target/debug/cqlite" would point at a nonexistent path here.
shared_target="$tmp/shared-target"
mkdir -p "$shared_target/debug"
: >"$shared_target/debug/cqlite"
chmod +x "$shared_target/debug/cqlite"
workdir="$tmp/worktree"
mkdir -p "$workdir"
(
  cd "$workdir"
  CARGO_TARGET_DIR="$shared_target"
  resolved=$(resolve_cli)
  expected="$shared_target/debug/cqlite"
  if [ "$resolved" = "$expected" ] && [ -x "$resolved" ]; then
    exit 0
  fi
  echo "  resolved=$resolved expected=$expected" >&2
  exit 1
) && ok "CARGO_TARGET_DIR set: CLI resolves to the shared target dir (executable)" \
   || bad "CARGO_TARGET_DIR set: CLI did not resolve to the shared target dir"

# 4) CARGO_TARGET_DIR unset -> fall back to "$PWD/target/debug/cqlite".
workdir2="$tmp/worktree2"
mkdir -p "$workdir2/target/debug"
: >"$workdir2/target/debug/cqlite"
chmod +x "$workdir2/target/debug/cqlite"
(
  cd "$workdir2"
  unset CARGO_TARGET_DIR
  resolved=$(resolve_cli)
  expected="$PWD/target/debug/cqlite"
  if [ "$resolved" = "$expected" ] && [ -x "$resolved" ]; then
    exit 0
  fi
  echo "  resolved=$resolved expected=$expected" >&2
  exit 1
) && ok "CARGO_TARGET_DIR unset: CLI falls back to \$PWD/target/debug/cqlite" \
   || bad "CARGO_TARGET_DIR unset: CLI did not fall back to \$PWD/target/debug/cqlite"

echo
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ]
