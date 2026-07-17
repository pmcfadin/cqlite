#!/usr/bin/env bash
#
# Regression tests for scripts/flow/premerge-assert.sh (issue #2668).
#
# Fast + hermetic: `gh` is shimmed by a PATH-prepended mock that returns
# controlled JSON (or a failure) driven by env vars — no network, no GitHub.
#
# Run standalone:   bash scripts/tests/test_premerge_assert.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ASSERT="$SCRIPT_DIR/../flow/premerge-assert.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

T=$(mktemp -d "${TMPDIR:-/tmp}/premerge-assert-test.XXXXXX")
trap 'rm -rf "$T"' EXIT

# --- gh mock -----------------------------------------------------------------
# A fake `gh` on PATH. It reads two env vars set per-case:
#   MOCK_GH_JSON   the exact stdout to emit (compact JSON as `gh --json` gives)
#   MOCK_GH_FAIL   if "1", exit non-zero without output (simulates auth/network)
BIN="$T/bin"
mkdir -p "$BIN"
cat >"$BIN/gh" <<'MOCK'
#!/usr/bin/env bash
if [ "${MOCK_GH_FAIL:-0}" = "1" ]; then
  echo "gh: could not connect" >&2
  exit 1
fi
printf '%s\n' "${MOCK_GH_JSON:-}"
exit 0
MOCK
chmod +x "$BIN/gh"

# run <expected-exit> <description> — invokes the assert with the gh mock on
# PATH, captures combined output + exit code. Sets $OUT and $RC for assertions.
run() {
  local want="$1" desc="$2"
  shift 2
  OUT=$(PATH="$BIN:$PATH" bash "$ASSERT" "$@" 2>&1)
  RC=$?
  if [ "$RC" -ne "$want" ]; then
    bad "$desc (exit $RC, wanted $want)"
    printf '     output: %s\n' "$OUT"
    return 1
  fi
  return 0
}

CERTIFIED="da9a7cb2abc0000000000000000000000000000"
STALE="ca8eb016def0000000000000000000000000000"

# --- Case 1: match -> exit 0 --------------------------------------------------
export MOCK_GH_FAIL=0
export MOCK_GH_JSON="{\"headRefOid\":\"$CERTIFIED\",\"state\":\"OPEN\"}"
if run 0 "match: OPEN + head==certified -> exit 0" 2421 "$CERTIFIED"; then
  case "$OUT" in
    *"PREMERGE: OK $CERTIFIED"*) ok "match: prints PREMERGE: OK <sha>" ;;
    *) bad "match: missing 'PREMERGE: OK <sha>' (got: $OUT)" ;;
  esac
fi

# --- Case 2: mismatch -> exit 2, message names BOTH SHAs -----------------------
export MOCK_GH_JSON="{\"headRefOid\":\"$STALE\",\"state\":\"OPEN\"}"
if run 2 "mismatch: head moved -> exit 2" 2421 "$CERTIFIED"; then
  if [ "${OUT#*"$CERTIFIED"}" != "$OUT" ] && [ "${OUT#*"$STALE"}" != "$OUT" ]; then
    ok "mismatch: message contains BOTH SHAs"
  else
    bad "mismatch: message must contain both certified + actual SHA (got: $OUT)"
  fi
  case "$OUT" in
    *"re-certify before merge"*) ok "mismatch: message says re-certify before merge" ;;
    *) bad "mismatch: missing re-certify guidance (got: $OUT)" ;;
  esac
fi

# --- Case 3: merged/closed PR -> exit 2 ---------------------------------------
export MOCK_GH_JSON="{\"headRefOid\":\"$CERTIFIED\",\"state\":\"MERGED\"}"
if run 2 "merged PR -> exit 2" 2421 "$CERTIFIED"; then
  case "$OUT" in
    *"NOT-OPEN"*|*"closed or merged"*) ok "merged: distinct not-open refusal message" ;;
    *) bad "merged: missing not-open message (got: $OUT)" ;;
  esac
fi

# --- Case 4: gh/network failure -> exit 3 (fail closed) -----------------------
export MOCK_GH_FAIL=1
export MOCK_GH_JSON=""
if run 3 "gh failure -> exit 3 (fail closed)" 2421 "$CERTIFIED"; then
  case "$OUT" in
    *"GH-FAILURE"*) ok "gh-failure: distinct fail-closed message" ;;
    *) bad "gh-failure: missing GH-FAILURE message (got: $OUT)" ;;
  esac
fi
export MOCK_GH_FAIL=0

# --- Case 5: usage guard -> exit 3 --------------------------------------------
if run 3 "missing args -> exit 3" 2421; then
  ok "usage: too few args fails closed (exit 3)"
fi

# --- Case 6: unparseable JSON -> exit 3 (fail closed) -------------------------
export MOCK_GH_JSON="not json at all"
if run 3 "unparseable JSON -> exit 3" 2421 "$CERTIFIED"; then
  ok "unparseable: fails closed (exit 3)"
fi

# --- summary -----------------------------------------------------------------
printf '\n=== premerge-assert: %d passed, %d failed ===\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
