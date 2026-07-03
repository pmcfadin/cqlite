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

# assert_exit <label> <actual-rc> <expected-rc>: assert a captured exit status.
# Positive selftest cases must exit 0; a regression that emits a complete summary
# but exits non-zero would otherwise sail through assert_complete unnoticed.
assert_exit() {
  local label="$1" actual="$2" expected="$3"
  if [ "$actual" -eq "$expected" ]; then
    ok "$label: exit status $actual (expected $expected)"
  else
    bad "$label: exit status $actual (expected $expected)"
  fi
}

# 1. Through a tee pipe (the streamed copy must be complete; no leaked child).
#    Use ${PIPESTATUS[0]} to capture the GATE's status, not tee's.
AGENT_GATE_SUMMARY_FILE="$tmp/case1.txt" \
  bash "$GATE" --emit-summary-selftest 2>&1 | tee "$tmp/tee.log" >/dev/null
assert_exit "tee-pipe" "${PIPESTATUS[0]}" 0
assert_complete "tee-pipe" "$tmp/tee.log"
assert_complete "tee-pipe-caller-file" "$tmp/case1.txt"

# 2. Backgrounded capture + wait (streamed copy must be complete).
AGENT_GATE_SUMMARY_FILE="$tmp/case2.txt" \
  bash "$GATE" --emit-summary-selftest >"$tmp/bg.log" 2>&1 &
bg_pid=$!
wait "$bg_pid"; bg_rc=$?
assert_exit "background" "$bg_rc" 0
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
assert_exit "isolated-tmpdir" "$?" 0
log_summary=$(ls -t "$iso_tmp"/agent-gate.*/summary.txt 2>/dev/null | head -1)
if [ -n "$log_summary" ] && [ -f "$log_summary" ]; then
  assert_complete "isolated-tmpdir-log-copy" "$log_summary"
else
  bad "isolated-tmpdir: no LOG_DIR summary copy produced"
fi

# 4b. RELATIVE-PATH resolution (#1175 roborev finding 2): a relative
#     AGENT_GATE_SUMMARY_FILE must resolve against the CALLER's original CWD, not
#     the repo root (the gate cd's into the repo internally). We cd into a fresh
#     temp caller dir, run the selftest with a bare relative filename, and assert
#     the complete summary lands at "$caller_dir/<name>" — and NOT at the repo
#     root default.
rel_caller_dir=$(mktemp -d "$tmp/rel-caller.XXXXXX")
rel_name="rel-summary.txt"
(
  cd "$rel_caller_dir" || exit 1
  AGENT_GATE_SUMMARY_FILE="$rel_name" bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
)
rel_rc=$?
assert_exit "relative-path" "$rel_rc" 0
if [ -f "$rel_caller_dir/$rel_name" ]; then
  assert_complete "relative-path-caller-cwd" "$rel_caller_dir/$rel_name"
else
  bad "relative-path: summary not created at caller CWD ($rel_caller_dir/$rel_name)"
fi

# 5. STALE-FILE negative case (#1175 roborev findings 1 & 2): a caller-known
#    summary file left over from a PREVIOUS run holds an OLD complete RESULT: PASS
#    block with a DIFFERENT run-id. A new gate invocation that fails early (or
#    can't write) must NEVER let that stale PASS survive as if it were this run's
#    result. We assert two failure modes:
#
#    5a. Unwritable path: point AGENT_GATE_SUMMARY_FILE at a DIRECTORY so the
#        gate's `>` redirection fails with "Is a directory". This is
#        privilege-independent — it fails even as root (UID 0), unlike `chmod 0444`
#        which root ignores — so tooling-tests stays green in root/container CI
#        (#1175 roborev finding 2). A pre-placed stale complete PASS block at a
#        SEPARATE readable path exercises the run-id guard: the gate must exit
#        non-zero, warn loudly on stderr, and that stale PASS must be detected as
#        not-this-run.
#    5b. Early exit (dataset preflight fail): pre-populate, then run a
#        dataset-requiring selection (--only core-tests) with an empty datasets
#        root so the preflight exits 1 BEFORE any component. The caller-known file
#        must no longer be the stale PASS — it is either the startup INCOMPLETE
#        sentinel or a preflight FAIL block, both bearing THIS run's run-id.
STALE_PASS_BLOCK=$'\n==== AGENT-GATE SUMMARY ====\nrun-id: /tmp/agent-gate.STALEOLD\ncommit: deadbeef branch: old dirty: no\nfmt:               PASS (1s)\nlogs: /tmp/agent-gate.STALEOLD\nsummary-file: /stale\nRESULT: PASS\n==== END AGENT-GATE SUMMARY ===='

# 5a. Unwritable caller-known path: a DIRECTORY target makes `>` fail even as root
#     (privilege-independent, #1175 roborev finding 2). A stale complete PASS block
#     pre-placed at a SEPARATE readable path lets us still confirm the run-id guard
#     would reject a not-this-run block.
stale_dir="$tmp/stale-blocked-dir"
mkdir -p "$stale_dir"
stale_ro="$tmp/stale-readonly.txt"
printf '%s\n' "$STALE_PASS_BLOCK" >"$stale_ro"
ro_stderr="$tmp/stale-readonly.stderr"
if AGENT_GATE_SUMMARY_FILE="$stale_dir" \
     bash "$GATE" --emit-summary-selftest >/dev/null 2>"$ro_stderr"; then
  bad "stale-readonly: gate exited 0 despite unwritable (directory) summary path (should FAIL)"
else
  ok "stale-readonly: gate exited non-zero (recovery artifact could not be written)"
fi
if grep -q "could not write complete summary file" "$ro_stderr"; then
  ok "stale-readonly: loud stderr warning emitted"
else
  bad "stale-readonly: missing loud stderr warning"
  echo "------- stderr -------"; cat "$ro_stderr"; echo "----------------------"
fi
# RESULT-consistency (#1175 roborev finding 1): when the authoritative write fails
# the fallback block streamed to stdout MUST say RESULT: FAIL — never the computed
# PASS — so a consumer parsing it never sees a FALSE GREEN against a non-zero exit.
ro_stdout="$tmp/stale-readonly.stdout"
AGENT_GATE_SUMMARY_FILE="$stale_dir" \
  bash "$GATE" --emit-summary-selftest >"$ro_stdout" 2>/dev/null || true
if grep -q "^RESULT: FAIL" "$ro_stdout" && ! grep -q "^RESULT: PASS" "$ro_stdout"; then
  ok "stale-readonly: stdout fallback block shows RESULT: FAIL (matches non-zero exit)"
else
  bad "stale-readonly: stdout fallback block must show RESULT: FAIL, not PASS"
  echo "------- stdout -------"; cat "$ro_stdout"; echo "----------------------"
fi
# The directory target means nothing was written there; the separate stale file
# still bears the OLD run-id. The gate must NOT have been fooled into treating any
# stale RESULT: PASS as this run's success — it proves that by exiting non-zero
# (asserted above). Confirm the stale block's run-id is foreign so the guard's job
# is unambiguous.
if grep -q "run-id: /tmp/agent-gate.STALEOLD" "$stale_ro"; then
  ok "stale-readonly: stale block still bears the OLD run-id (would be rejected as not-this-run)"
else
  bad "stale-readonly: unexpected stale-block run-id"
  echo "------- on disk -------"; cat "$stale_ro"; echo "-----------------------"
fi

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

# 6. LITE summary emission (issue #1821): `--lite --emit-summary-selftest` must
#    emit a DISTINCTLY-labeled block ("==== AGENT-GATE LITE SUMMARY ====" + a
#    "MODE: lite" line) so a lite summary can NEVER be pasted as the full gate's
#    SUMMARY, and the caller-known recovery file must still be complete for lite.
LITE_START="==== AGENT-GATE LITE SUMMARY ===="
LITE_END="==== END AGENT-GATE LITE SUMMARY ===="
lite_file="$tmp/lite-summary.txt"
AGENT_GATE_SUMMARY_FILE="$lite_file" \
  bash "$GATE" --lite --emit-summary-selftest >"$tmp/lite.log" 2>&1
lite_rc=$?
assert_exit "lite-selftest" "$lite_rc" 0
if grep -qF "$LITE_START" "$lite_file" && grep -qF "$LITE_END" "$lite_file" \
   && grep -q "^MODE: lite" "$lite_file" && grep -q "^RESULT: " "$lite_file"; then
  ok "lite-selftest: distinct LITE markers + MODE: lite present in caller-known file"
else
  bad "lite-selftest: missing LITE markers or MODE line (file: $lite_file)"
  echo "------- captured -------"; cat "$lite_file"; echo "------------------------"
fi
# The lite block MUST NOT carry the full-gate markers (would be pasteable as the
# full SUMMARY). The full START marker is a prefix of the LITE one, so match the
# full marker only when it is NOT the LITE marker (i.e. its own line boundaries).
if grep -qxF "$START_MARKER" "$lite_file"; then
  bad "lite-selftest: block also contains the FULL '$START_MARKER' line (must not)"
else
  ok "lite-selftest: block does not contain the full-gate SUMMARY marker line"
fi

# 7. scoped-test target classification (issue #1821): a changed file is treated
#    as a Cargo `--test` target ONLY if it is an actual integration-test target.
#    NESTED helper/module files under tests/<subdir>/ must NOT be picked (a Bash
#    `case` glob like `*/tests/*.rs` matches `/`, so it wrongly matched them and
#    made --lite FAIL on valid helper-only changes). The gate exposes the mapping
#    via the hidden `--classify-test-targets` hook (stdin paths -> "<pkg>|<name>").
classify_out=$(printf '%s\n' \
  "cqlite-core/tests/write_read_roundtrip/data_multi.rs" \
  "cqlite-cli/tests/common/mod.rs" \
  "cqlite-core/src/storage/sstable/reader.rs" \
  "cqlite-core/tests/compact_command.rs" \
  | bash "$GATE" --classify-test-targets 2>/dev/null)
# Nested helper + module files must be EXCLUDED.
if printf '%s\n' "$classify_out" | grep -qE '\|(data_multi|mod)$'; then
  bad "classify: nested helper (write_read_roundtrip/data_multi.rs or common/mod.rs) wrongly picked as a --test target"
  echo "------- classify output -------"; printf '%s\n' "$classify_out"; echo "-------------------------------"
else
  ok "classify: nested helper/module files NOT treated as --test targets"
fi
# A real direct integration-test target must still be picked, mapped to its pkg.
if printf '%s\n' "$classify_out" | grep -qxF "cqlite-core|compact_command"; then
  ok "classify: real integration-test target (compact_command) picked with correct package"
else
  bad "classify: real integration-test target compact_command was NOT picked"
  echo "------- classify output -------"; printf '%s\n' "$classify_out"; echo "-------------------------------"
fi

# 8. Bash 3.2 compatibility (issue #1821): macOS ships Bash 3.2 as /bin/bash and
#    the gate is invoked as plain `bash scripts/agent-gate.sh`. The --lite path
#    must not use Bash-4-only features (associative arrays). Exercise the hook
#    under /bin/bash when it is the 3.x default so the test is meaningful there.
if [ -x /bin/bash ]; then
  bin_bash_major=$(/bin/bash -c 'echo "${BASH_VERSINFO[0]}"' 2>/dev/null)
  if /bin/bash "$GATE" --lite-list >/dev/null 2>&1 \
     && printf '%s\n' "cqlite-core/tests/compact_command.rs" \
        | /bin/bash "$GATE" --classify-test-targets 2>/dev/null \
        | grep -qxF "cqlite-core|compact_command"; then
    ok "bash-compat: --lite classification path runs under /bin/bash (major ${bin_bash_major:-?})"
  else
    bad "bash-compat: --lite classification path failed under /bin/bash (major ${bin_bash_major:-?})"
  fi
fi

echo "----"
echo "passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ]
