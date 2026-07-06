#!/usr/bin/env bash
#
# Regression tests for scripts/flow/claim-heartbeat.sh (issue #2089).
#
# Fast + hermetic: a mktemp BARE repo stands in for origin, plus one clone that
# plays every "machine" via HEARTBEAT_MACHINE overrides. No network, no
# GitHub — heartbeats are a pure git-ref mechanism.
#
# Run standalone:   bash scripts/tests/test_claim_heartbeat.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HB="$SCRIPT_DIR/../flow/claim-heartbeat.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

# git in a throwaway identity so commits/pushes work in any sandbox, matching
# scripts/flow/tests/finalize-cleanup.test.sh's convention.
g() { git -c user.email=t@t -c user.name=t -c init.defaultBranch=main -c commit.gpgsign=false "$@"; }

T=$(mktemp -d "${TMPDIR:-/tmp}/claim-heartbeat-test.XXXXXX")
trap 'rm -rf "$T"' EXIT

ORIGIN="$T/origin.git"
WORK="$T/work"
g init --bare -q "$ORIGIN"
g clone -q "$ORIGIN" "$WORK" 2>/dev/null
(
  cd "$WORK" || exit 1
  echo seed >seed.txt
  g add seed.txt
  g commit -qm seed
  g push -q -u origin main
)

# ts_to_epoch_test <ISO8601 UTC ts> — the same portable GNU/BSD parse the
# script uses, duplicated here only to build assertions (never imported).
ts_to_epoch_test() {
  local ts="$1" epoch
  if epoch=$(date -u -d "$ts" +%s 2>/dev/null); then
    printf '%s\n' "$epoch"
    return 0
  fi
  date -u -j -f '%Y-%m-%dT%H:%M:%SZ' "$ts" +%s
}

# craft_old_heartbeat <work> <machine> <issue> <age_seconds> — pushes a
# heartbeat ref directly (bypassing `beat`) with a ts crafted <age_seconds> in
# the past, so age assertions don't depend on real sleeps.
craft_old_heartbeat() {
  local work="$1" machine="$2" issue="$3" age="$4"
  (
    cd "$work" || exit 1
    local now_epoch old_epoch old_ts empty_tree csha
    now_epoch=$(date -u +%s)
    old_epoch=$((now_epoch - age))
    if ! old_ts=$(date -u -r "$old_epoch" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null); then
      old_ts=$(date -u -d "@$old_epoch" +%Y-%m-%dT%H:%M:%SZ)
    fi
    empty_tree=$(git hash-object -t tree --stdin </dev/null)
    csha=$(GIT_AUTHOR_DATE="$old_ts" GIT_COMMITTER_DATE="$old_ts" \
      GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t \
      git commit-tree "$empty_tree" -m "heartbeat issue=${issue} machine=${machine} ts=${old_ts}")
    g push -q origin "${csha}:refs/heartbeats/${machine}"
  )
}

# ===========================================================================
echo "TEST 1: beat creates the ref"
# ===========================================================================
out=$(cd "$WORK" && HEARTBEAT_MACHINE=machineA bash "$HB" beat 100 2>&1)
sha1=$(g -C "$WORK" ls-remote origin "refs/heartbeats/machineA" | awk '{print $1}')
if [ -n "$sha1" ]; then
  ok "beat pushed refs/heartbeats/machineA ($sha1)"
else
  bad "beat did not create refs/heartbeats/machineA — output: $out"
fi

# ===========================================================================
echo "TEST 2: second beat force-updates (same ref, new sha; one ref per machine)"
# ===========================================================================
sleep 1
(cd "$WORK" && HEARTBEAT_MACHINE=machineA bash "$HB" beat 101 >/dev/null 2>&1)
sha2=$(g -C "$WORK" ls-remote origin "refs/heartbeats/machineA" | awk '{print $1}')
count=$(g -C "$WORK" ls-remote origin "refs/heartbeats/machineA" | wc -l | tr -d ' ')
if [ "$count" = "1" ] && [ -n "$sha2" ] && [ "$sha2" != "$sha1" ]; then
  ok "second beat force-updated the SAME ref to a new sha ($sha1 -> $sha2), count=$count"
else
  bad "expected 1 ref with a changed sha; got count=$count sha1=$sha1 sha2=$sha2"
fi

# ===========================================================================
echo "TEST 3: list renders machine/issue/age for 2 machines"
# ===========================================================================
(cd "$WORK" && HEARTBEAT_MACHINE=machineB bash "$HB" beat 202 >/dev/null 2>&1)
list_out=$(cd "$WORK" && bash "$HB" list)
if printf '%s\n' "$list_out" | grep -qE '^machineA +101 ' \
  && printf '%s\n' "$list_out" | grep -qE '^machineB +202 '; then
  ok "list rendered both machineA (issue 101) and machineB (issue 202)"
else
  bad "list output missing expected machine/issue rows:
$list_out"
fi
if printf '%s\n' "$list_out" | grep -qE '^machineA +[0-9]+ +[0-9TZ:-]+ +[0-9]+[smhd]$'; then
  ok "list row shape is machine/issue/ts/age"
else
  bad "list row shape unexpected:
$list_out"
fi

# ===========================================================================
echo "TEST 4: age reflects an old ts (crafted 10h in the past)"
# ===========================================================================
craft_old_heartbeat "$WORK" "machineOld" 6000 36000
list_out=$(cd "$WORK" && bash "$HB" list)
old_line=$(printf '%s\n' "$list_out" | grep -E '^machineOld ' || true)
old_age=$(printf '%s\n' "$old_line" | awk '{print $NF}')
if [[ "$old_age" =~ ^([0-9]+)h$ ]] && [ "${BASH_REMATCH[1]}" -ge 9 ]; then
  ok "machineOld (crafted 10h old) reports age=$old_age (>= 9h)"
else
  bad "expected machineOld age >= 9h, got '$old_age' from line: $old_line"
fi

# ===========================================================================
echo "TEST 5: clear removes the ref"
# ===========================================================================
(cd "$WORK" && bash "$HB" clear machineOld >/dev/null 2>&1)
remaining=$(g -C "$WORK" ls-remote origin "refs/heartbeats/machineOld")
if [ -z "$remaining" ]; then
  ok "clear removed refs/heartbeats/machineOld"
else
  bad "refs/heartbeats/machineOld still present after clear: $remaining"
fi
# idempotent: clearing an already-absent ref must not error
if (cd "$WORK" && bash "$HB" clear machineOld >/dev/null 2>&1); then
  ok "clear on an already-absent ref is a graceful no-op (exit 0)"
else
  bad "clear on an already-absent ref exited non-zero"
fi

# ===========================================================================
echo "TEST 6: beat does not modify the working tree or current branch"
# ===========================================================================
before_branch=$(g -C "$WORK" rev-parse --abbrev-ref HEAD)
before_head=$(g -C "$WORK" rev-parse HEAD)
before_status=$(g -C "$WORK" status --porcelain)
(cd "$WORK" && HEARTBEAT_MACHINE=machineA bash "$HB" beat 303 >/dev/null 2>&1)
after_branch=$(g -C "$WORK" rev-parse --abbrev-ref HEAD)
after_head=$(g -C "$WORK" rev-parse HEAD)
after_status=$(g -C "$WORK" status --porcelain)
if [ "$before_branch" = "$after_branch" ] && [ "$before_head" = "$after_head" ] && [ "$before_status" = "$after_status" ]; then
  ok "beat left branch ($after_branch), HEAD ($after_head), and working tree untouched"
else
  bad "beat mutated local state: branch $before_branch->$after_branch, HEAD $before_head->$after_head, status changed=$([ "$before_status" != "$after_status" ] && echo yes || echo no)"
fi

# ===========================================================================
echo "TEST 7: zero heartbeats renders gracefully"
# ===========================================================================
EMPTY_ORIGIN="$T/empty.git"
g init --bare -q "$EMPTY_ORIGIN"
(cd "$WORK" && g remote add empty-origin "$EMPTY_ORIGIN")
empty_out=$(cd "$WORK" && HEARTBEAT_REMOTE=empty-origin bash "$HB" list)
if printf '%s\n' "$empty_out" | grep -qi "no heartbeats found"; then
  ok "list against a heartbeat-free remote renders gracefully: '$empty_out'"
else
  bad "expected a graceful 'no heartbeats found' message, got: $empty_out"
fi

# ===========================================================================
echo "TEST 8: HEARTBEAT_REMOTE env var is honored (non-default remote name)"
# ===========================================================================
(cd "$WORK" && HEARTBEAT_REMOTE=empty-origin HEARTBEAT_MACHINE=machineC bash "$HB" beat 404 >/dev/null 2>&1)
on_alt=$(g -C "$WORK" ls-remote empty-origin "refs/heartbeats/machineC")
on_default=$(g -C "$WORK" ls-remote origin "refs/heartbeats/machineC")
if [ -n "$on_alt" ] && [ -z "$on_default" ]; then
  ok "HEARTBEAT_REMOTE=empty-origin routed the beat to the non-default remote only"
else
  bad "expected machineC only on empty-origin; on_alt='$on_alt' on_default='$on_default'"
fi

echo
echo "=== claim-heartbeat.sh: $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ]
