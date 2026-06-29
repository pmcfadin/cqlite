#!/usr/bin/env bash
# Regression test for issue #1175: the agent-gate SUMMARY block must survive
# non-foreground capture (tee pipe, backgrounded capture) and must always be
# recoverable from the authoritative summary file even when a leaked descendant
# keeps the gate's stdout pipe open (the truncation root cause).
#
# Fast by design: exercises only the SUMMARY emission path via
# `agent-gate.sh --emit-summary-selftest`, never the 5-8 min real gate.
#
# Run standalone:   bash scripts/tests/test_agent_gate_summary.sh
# Or via the gate:  (covered by the delivery-telemetry-style tooling tests)
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"
START_MARKER="==== AGENT-GATE SUMMARY ===="
END_MARKER="==== END AGENT-GATE SUMMARY ===="
STAGE_LINE="fmt:" # representative stage line from the selftest block

PASS=0
FAIL=0
ok()   { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

# assert_complete <label> <file>: file must contain start marker, end marker,
# RESULT line, and a representative stage line.
assert_complete() {
  local label="$1" file="$2"
  local missing=()
  grep -q "$START_MARKER" "$file" || missing+=("start-marker")
  grep -q "$END_MARKER"   "$file" || missing+=("end-marker")
  grep -q "^RESULT: "     "$file" || missing+=("RESULT")
  grep -q "$STAGE_LINE"   "$file" || missing+=("stage-line")
  if [ "${#missing[@]}" -eq 0 ]; then
    ok "$label: complete SUMMARY block"
  else
    bad "$label: missing ${missing[*]} (file: $file)"
    echo "------- captured -------"; cat "$file"; echo "------------------------"
  fi
}

tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

# 1. Through a tee pipe (the streamed copy must be complete; no leaked child).
bash "$GATE" --emit-summary-selftest 2>&1 | tee "$tmp/tee.log" >/dev/null
assert_complete "tee-pipe" "$tmp/tee.log"

# 2. Backgrounded capture + wait (streamed copy must be complete).
bash "$GATE" --emit-summary-selftest >"$tmp/bg.log" 2>&1 &
wait
assert_complete "background" "$tmp/bg.log"

# 3. Truncation root cause: a leaked descendant inherits the gate's stdout and
#    keeps the pipe open, so an until-EOF reader would hang/truncate. The
#    authoritative summary file must still be complete. We discover the file path
#    from the (possibly truncated) stream and assert the FILE is intact.
#
#    Wrap the gate so a backgrounded `sleep` inherits fd1 (the pipe), then read
#    the stream with a short alarm so the test itself can't hang.
leak_runner="$tmp/leak.sh"
cat >"$leak_runner" <<EOF
#!/usr/bin/env bash
sleep 30 &            # leaked descendant holding the gate's stdout pipe
exec bash "$GATE" --emit-summary-selftest
EOF
chmod +x "$leak_runner"

# Reader drains until EOF then writes — but is killed at 4s (EOF never comes
# because of the leaked sleep). This models the harness that truncates.
reader='import sys,signal; signal.alarm(4); sys.stdout.buffer.write(sys.stdin.buffer.read())'
{ bash "$leak_runner" 2>/dev/null | python3 -c "$reader" >"$tmp/leak-stream.log" 2>/dev/null; } 2>/dev/null

# The streamed copy may be empty/truncated (that's the bug we tolerate); recover
# the authoritative file. Path is deterministic: summary file lives under the
# LOG_DIR the gate prints. Find the most recent agent-gate.* summary.txt.
summary_file=$(ls -t "${TMPDIR:-/tmp}"/agent-gate.*/summary.txt 2>/dev/null | head -1)
if [ -n "$summary_file" ] && [ -f "$summary_file" ]; then
  assert_complete "leaked-child-summary-file" "$summary_file"
else
  bad "leaked-child: no authoritative summary file produced"
fi
# Document the observed stream behaviour (informational, not asserted).
if grep -q "$END_MARKER" "$tmp/leak-stream.log" 2>/dev/null; then
  echo "info - leaked-child stream HAPPENED to survive (timing); file is the guarantee"
else
  echo "info - leaked-child stream truncated as expected; authoritative file recovered"
fi

echo "----"
echo "passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ]
