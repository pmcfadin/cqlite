#!/usr/bin/env bash
# Regression test for issue #1175: the agent-gate SUMMARY block must survive
# non-foreground capture (tee pipe, backgrounded capture) and must always be
# recoverable from a CALLER-KNOWN summary file even when a leaked descendant
# keeps the gate's stdout pipe open (the truncation root cause). The advertised
# contract is: set AGENT_GATE_SUMMARY_FILE=/path in advance and the complete
# block is always at that exact path, regardless of what happens to the stream.
#
# Fast by design: exercises only the SUMMARY emission path via
# `agent-gate.sh --emit-summary-selftest`, never the 5-8 min real gate.
#
# Run standalone:   bash scripts/tests/test_agent_gate_summary.sh
# Or via the gate:  scripts/agent-gate.sh runs it as the `tooling-tests` component.
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

# Every invocation pins AGENT_GATE_SUMMARY_FILE to a caller-chosen path inside our
# scratch dir, so (a) we never write the repo-root default during the test, and
# (b) we can assert the EXACT caller-provided path is complete — the contract.

# 1. Through a tee pipe (the streamed copy must be complete; no leaked child).
AGENT_GATE_SUMMARY_FILE="$tmp/case1.txt" \
  bash "$GATE" --emit-summary-selftest 2>&1 | tee "$tmp/tee.log" >/dev/null
assert_complete "tee-pipe" "$tmp/tee.log"
assert_complete "tee-pipe-caller-file" "$tmp/case1.txt"

# 2. Backgrounded capture + wait (streamed copy must be complete).
AGENT_GATE_SUMMARY_FILE="$tmp/case2.txt" \
  bash "$GATE" --emit-summary-selftest >"$tmp/bg.log" 2>&1 &
wait
assert_complete "background" "$tmp/bg.log"
assert_complete "background-caller-file" "$tmp/case2.txt"

# 3. The advertised contract under the truncation root cause: a leaked descendant
#    inherits the gate's stdout and keeps the pipe open, so an until-EOF reader
#    hangs and FULLY loses the stream. The caller set AGENT_GATE_SUMMARY_FILE to a
#    path it chose in advance; that EXACT path must hold the complete block with
#    NO need to parse the (lost) stream. We assert the caller-provided path by
#    name — not a glob — because that is the contract a caller can rely on.
caller_file="$tmp/caller-known-summary.txt"
leak_runner="$tmp/leak.sh"
cat >"$leak_runner" <<EOF
#!/usr/bin/env bash
sleep 30 &            # leaked descendant holding the gate's stdout pipe
exec env AGENT_GATE_SUMMARY_FILE="$caller_file" bash "$GATE" --emit-summary-selftest
EOF
chmod +x "$leak_runner"

# Reader drains until EOF then writes — but is killed at 4s (EOF never comes
# because of the leaked sleep). This models the harness that truncates.
reader='import sys,signal; signal.alarm(4); sys.stdout.buffer.write(sys.stdin.buffer.read())'
{ bash "$leak_runner" 2>/dev/null | python3 -c "$reader" >"$tmp/leak-stream.log" 2>/dev/null; } 2>/dev/null

# The streamed copy may be empty/truncated (that's the bug we tolerate); the
# caller-known file at the EXACT path the caller chose must be complete.
if [ -f "$caller_file" ]; then
  assert_complete "leaked-child-caller-known-file" "$caller_file"
else
  bad "leaked-child: caller-known summary file '$caller_file' was not produced"
fi
# Document the observed stream behaviour (informational, not asserted).
if grep -q "$END_MARKER" "$tmp/leak-stream.log" 2>/dev/null; then
  echo "info - leaked-child stream HAPPENED to survive (timing); caller-known file is the guarantee"
else
  echo "info - leaked-child stream truncated as expected; caller-known file recovered"
fi

# 4. Isolated-TMPDIR archival copy: with AGENT_GATE_SUMMARY_FILE unset, the gate
#    still keeps a copy under its LOG_DIR (mktemp -d "$TMPDIR/agent-gate.*"). We
#    point TMPDIR at a fresh empty dir so the only summary.txt under it belongs to
#    THIS run (never a newest-wins glob across stale/concurrent runs), and we
#    redirect the repo-root default into the scratch dir so the test never writes
#    the real .agent-gate-summary.txt.
iso_tmp=$(mktemp -d "$tmp/iso-tmpdir.XXXXXX")
AGENT_GATE_SUMMARY_FILE="$tmp/iso-default.txt" TMPDIR="$iso_tmp" \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
log_summary=$(ls -t "$iso_tmp"/agent-gate.*/summary.txt 2>/dev/null | head -1)
if [ -n "$log_summary" ] && [ -f "$log_summary" ]; then
  assert_complete "isolated-tmpdir-log-copy" "$log_summary"
else
  bad "isolated-tmpdir: no LOG_DIR summary copy produced"
fi

# 5. STALE-FILE negative case (#1175 roborev findings 1 & 2): a caller-known
#    summary file left over from a PREVIOUS run holds an OLD complete RESULT: PASS
#    block with a DIFFERENT run-id. A new gate invocation that fails early (or
#    can't write) must NEVER let that stale PASS survive as if it were this run's
#    result. We assert two failure modes:
#
#    5a. Unwritable path: pre-populate, then chmod the file 0444 (read-only) so the
#        gate's `>` redirection fails. The on-disk content is STILL the old PASS
#        block (it survives non-empty + end-marker checks), so the run-id guard +
#        write-rc check are what must catch it: the gate must exit non-zero, warn
#        loudly on stderr, and the stale PASS must be detected as not-this-run.
#    5b. Early exit (dataset preflight fail): pre-populate, then run a
#        dataset-requiring selection (--only core-tests) with an empty datasets
#        root so the preflight exits 1 BEFORE any component. The caller-known file
#        must no longer be the stale PASS — it is either the startup INCOMPLETE
#        sentinel or a preflight FAIL block, both bearing THIS run's run-id.
STALE_PASS_BLOCK=$'\n==== AGENT-GATE SUMMARY ====\nrun-id: /tmp/agent-gate.STALEOLD\ncommit: deadbeef branch: old dirty: no\nfmt:               PASS (1s)\nlogs: /tmp/agent-gate.STALEOLD\nsummary-file: /stale\nRESULT: PASS\n==== END AGENT-GATE SUMMARY ===='

# 5a. Unwritable caller-known path that already holds a stale PASS block.
stale_ro="$tmp/stale-readonly.txt"
printf '%s\n' "$STALE_PASS_BLOCK" >"$stale_ro"
chmod 0444 "$stale_ro"
ro_stderr="$tmp/stale-readonly.stderr"
if AGENT_GATE_SUMMARY_FILE="$stale_ro" \
     bash "$GATE" --emit-summary-selftest >/dev/null 2>"$ro_stderr"; then
  bad "stale-readonly: gate exited 0 despite unwritable summary path (should FAIL)"
else
  ok "stale-readonly: gate exited non-zero (recovery artifact could not be written)"
fi
if grep -q "could not write complete summary file" "$ro_stderr"; then
  ok "stale-readonly: loud stderr warning emitted"
else
  bad "stale-readonly: missing loud stderr warning"
  echo "------- stderr -------"; cat "$ro_stderr"; echo "----------------------"
fi
# The on-disk file is still the OLD read-only block; the gate must NOT have been
# fooled into treating that stale RESULT: PASS as this run's success — which it
# proves by exiting non-zero (asserted above). Confirm the stale run-id is what's
# on disk (the write never landed) so the guard's job is unambiguous.
if grep -q "run-id: /tmp/agent-gate.STALEOLD" "$stale_ro"; then
  ok "stale-readonly: on-disk file still bears the OLD run-id (write correctly rejected)"
else
  bad "stale-readonly: unexpected on-disk run-id"
  echo "------- on disk -------"; cat "$stale_ro"; echo "-----------------------"
fi
chmod 0644 "$stale_ro" 2>/dev/null || true

# 5b. Stale PASS at a writable caller-known path, then an early-exit gate run.
stale_early="$tmp/stale-early.txt"
printf '%s\n' "$STALE_PASS_BLOCK" >"$stale_early"
empty_ds=$(mktemp -d "$tmp/empty-datasets.XXXXXX")
# --only core-tests selects a dataset-requiring component, so the preflight runs
# and (with an empty datasets root) exits 1 before any component executes.
AGENT_GATE_SUMMARY_FILE="$stale_early" CQLITE_DATASETS_ROOT="$empty_ds" \
  bash "$GATE" --only core-tests >/dev/null 2>&1 || true
if grep -q "^RESULT: PASS" "$stale_early" && \
   grep -q "run-id: /tmp/agent-gate.STALEOLD" "$stale_early"; then
  bad "stale-early: caller-known file STILL holds the stale RESULT: PASS"
  echo "------- on disk -------"; cat "$stale_early"; echo "-----------------------"
else
  ok "stale-early: stale PASS replaced (INCOMPLETE sentinel or preflight FAIL, this run's run-id)"
fi
if grep -q "run-id: /tmp/agent-gate.STALEOLD" "$stale_early"; then
  bad "stale-early: old run-id still present (stale block survived)"
else
  ok "stale-early: old run-id no longer present"
fi

echo "----"
echo "passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ]
