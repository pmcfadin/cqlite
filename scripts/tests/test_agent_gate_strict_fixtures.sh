#!/usr/bin/env bash
# #4230: the FULL gate (and --recertify, via _gate_is_strict) must run core-tests,
# write-tests, cli-tests and integration-tests with CQLITE_REQUIRE_FIXTURES=1, so
# corpus-gated tests FAIL on an absent corpus instead of skip-passing. --only/--lite probes
# and AGENT_GATE_ALLOW_MISSING_FIXTURES=1 keep the caller's policy, and the value must not
# leak into dataset-free components or the parent shell. Hermetic: extracts the shipped
# run_component() and drives it with stub children; no cargo.
# Run: bash scripts/tests/test_agent_gate_strict_fixtures.sh   (wired into tooling-tests)
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
GATE="$HERE/../agent-gate.sh"
PASS=0; FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1" >&2; FAIL=$((FAIL + 1)); }

src=$(awk '!f && /^run_component\(\) \{/ {f=1} f {print} f && /^\}$/ {exit}' "$GATE")
[ -n "$src" ] || { bad "could not extract run_component() from $GATE"; exit 1; }
TMP=$(mktemp -d "${TMPDIR:-/tmp}/strict-fixtures.XXXXXX") || exit 1
trap 'rm -rf "$TMP"' EXIT
LOG_DIR="$TMP"; RESULTS="$TMP/results"
record_result() { RECORDED_STATUS="$2"; }
AGENT_GATE_FM_COMPONENT=""
eval "$src" || { bad "run_component() does not parse"; exit 1; }
# Hermetic: CI jobs may export CQLITE_REQUIRE_FIXTURES=1; the leak check below must see only
# what run_component() does, not what this script inherited.
unset CQLITE_REQUIRE_FIXTURES

# probe <mode> <component> -> prints the child's view of CQLITE_REQUIRE_FIXTURES
probe() { # ONLY LITE RECERTIFY ALLOW component [inherited]
  ONLY="$1"; LITE="$2"; RECERTIFY="$3"
  _gate_is_strict() { [ "$LITE" -eq 0 ] && { [ -z "$ONLY" ] || [ "$RECERTIFY" -eq 1 ]; }; }
  if [ -n "${6:-}" ]; then export CQLITE_REQUIRE_FIXTURES="$6"; else unset CQLITE_REQUIRE_FIXTURES; fi
  AGENT_GATE_ALLOW_MISSING_FIXTURES="$4" run_component "$5" bash -c 'echo "seen=${CQLITE_REQUIRE_FIXTURES:-unset}" >&2; exit 0' >/dev/null
  grep -o 'seen=.*' "$LOG_DIR/$5.log"
}
want() { # description expected actual
  if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (want $2, got $3)"; fi
}
for c in core-tests write-tests cli-tests integration-tests; do
  want "$c: full gate exports it"            seen=1     "$(probe '' 0 0 0 $c)"
  want "$c: --recertify exports it"          seen=1     "$(probe "$c" 0 1 0 $c)"
  want "$c: --only probe inherits (unset)"   seen=unset "$(probe "$c" 0 0 0 $c)"
  want "$c: --lite inherits (unset)"         seen=unset "$(probe '' 1 0 0 $c)"
  want "$c: opt-out inherits (unset)"        seen=unset "$(probe '' 0 0 1 $c)"
  want "$c: --only keeps caller's =1"        seen=1     "$(probe "$c" 0 0 0 $c 1)"
done
want "fmt: dataset-free component never gets it" seen=unset "$(probe '' 0 0 0 fmt)"
# Every probe above runs in a $(...) subshell, so call run_component in THIS shell once
# (full-gate strict mode, dataset component) before checking for a leak.
ONLY=""; LITE=0; RECERTIFY=0
_gate_is_strict() { [ "$LITE" -eq 0 ] && { [ -z "$ONLY" ] || [ "$RECERTIFY" -eq 1 ]; }; }
AGENT_GATE_ALLOW_MISSING_FIXTURES=0 run_component core-tests bash -c 'exit 0' >/dev/null
want "no leak into the parent shell" unset "${CQLITE_REQUIRE_FIXTURES:-unset}"
# the gate must keep the cargo-function path: `env` would bypass the observer function.
if awk '/^run_component\(\) \{/,/^\}$/' "$GATE" | grep -q 'env .*"\$@"'; then
  bad "run_component runs the child via env (bypasses the cargo() observer function)"
else ok "run_component does not route the child through env"; fi
printf '%s passed, %s failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
