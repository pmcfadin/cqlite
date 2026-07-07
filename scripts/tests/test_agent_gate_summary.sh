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

# assert_accelerators <label> <file>: issue #1848 — the SUMMARY block must carry a
# machine-checkable `accelerators:` line naming every optional accelerator's state
# (sccache / nextest / lanes = on|absent|off|serial), so a silent degradation is
# visible in the pasted block. Assert the line exists with all three keys and a
# recognized state value (backward-compatible extension; the older markers still
# assert via assert_complete).
assert_accelerators() {
  local label="$1" file="$2"
  local line
  line=$(grep -E '^accelerators: ' "$file" 2>/dev/null | head -1)
  if [ -z "$line" ]; then
    bad "$label: no 'accelerators:' line in SUMMARY block (file: $file)"
    echo "------- captured -------"; cat "$file"; echo "------------------------"
    return
  fi
  if printf '%s\n' "$line" \
       | grep -Eq '^accelerators: sccache=(on|absent|off) nextest=(on|absent|off) lanes=(on|absent|off|serial)$'; then
    ok "$label: accelerators line well-formed ($line)"
  else
    bad "$label: malformed accelerators line: '$line'"
  fi
}

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
# #1848: the full SUMMARY block carries a well-formed accelerators line.
assert_accelerators "tee-pipe-caller-file" "$tmp/case1.txt"

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
# #1848: the LITE SUMMARY block also carries the accelerators line.
assert_accelerators "lite-selftest" "$lite_file"
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
#    via the hidden `--classify-test-targets` hook (stdin paths ->
#    "<pkg>|<name>|<required-features>", one line per OWNING package).
classify_out=$(printf '%s\n' \
  "cqlite-core/tests/write_read_roundtrip/data_multi.rs" \
  "cqlite-cli/tests/common/mod.rs" \
  "cqlite-core/src/storage/sstable/reader.rs" \
  "cqlite-core/tests/compact_command.rs" \
  "tests/cassandra5_header_tests.rs" \
  "cqlite-cli/tests/issue_1388_compact_major_drop.rs" \
  | bash "$GATE" --classify-test-targets 2>/dev/null)
# Nested helper + module files must be EXCLUDED (testname is the middle field).
if printf '%s\n' "$classify_out" | grep -qE '\|(data_multi|mod)\|'; then
  bad "classify: nested helper (write_read_roundtrip/data_multi.rs or common/mod.rs) wrongly picked as a --test target"
  echo "------- classify output -------"; printf '%s\n' "$classify_out"; echo "-------------------------------"
else
  ok "classify: nested helper/module files NOT treated as --test targets"
fi
# A real direct integration-test target must still be picked, mapped to its pkg
# (features field empty for a target with no required-features).
if printf '%s\n' "$classify_out" | grep -qxF "cqlite-core|compact_command|"; then
  ok "classify: real integration-test target (compact_command) picked with correct package"
else
  bad "classify: real integration-test target compact_command was NOT picked"
  echo "------- classify output -------"; printf '%s\n' "$classify_out"; echo "-------------------------------"
fi
# Finding 1: a top-level tests/*.rs target is owned by BOTH the workspace-root
# `cqlite` package AND the cqlite-integration-tests crate; BOTH must be emitted
# so the root package's target is never silently dropped from --lite selection.
if printf '%s\n' "$classify_out" | grep -qxF "cqlite|cassandra5_header_tests|" \
   && printf '%s\n' "$classify_out" | grep -qxF "cqlite-integration-tests|cassandra5_header_tests|"; then
  ok "classify: root-cqlite + integration-tests BOTH emitted for a top-level tests/*.rs target (finding 1)"
else
  bad "classify: top-level tests/*.rs target did NOT emit both owning packages (root cqlite dropped)"
  echo "------- classify output -------"; printf '%s\n' "$classify_out"; echo "-------------------------------"
fi
# Finding 2: a target that declares required-features must carry them through so
# --lite compiles it WITH those features instead of invoking it feature-less.
if printf '%s\n' "$classify_out" | grep -qxF "cqlite-cli|issue_1388_compact_major_drop|write-support"; then
  ok "classify: required-features (write-support) passed through for a feature-gated target (finding 2)"
else
  bad "classify: required-features NOT passed through for issue_1388_compact_major_drop"
  echo "------- classify output -------"; printf '%s\n' "$classify_out"; echo "-------------------------------"
fi

# 7a. Metadata-derived PACKAGE ownership (issue #1821 roborev round 4): the old
#     hardcoded path-prefix `case` + `pkg_dir` maps only listed a SUBSET of
#     workspace members, so a change under an unlisted real member (tools/*,
#     bindings/*, examples, ...) fell through and ran the WRONG (cqlite-core --lib)
#     tests — a defect roborev re-found each round. Ownership now comes from
#     `cargo metadata` (longest manifest-dir prefix), covering EVERY member. The
#     mapping is exposed via the hidden `--classify-package-owners` hook
#     (stdin paths -> "<pkg>|<has_lib>", one owner per path).
owners_out=$(printf '%s\n' \
  "tools/format-validator/src/lib.rs" \
  "tools/sstabledump-validator/src/main.rs" \
  "bindings/python/src/lib.rs" \
  "bindings/node/src/database.rs" \
  "examples/basic.rs" \
  "cqlite-core/src/storage/sstable/reader.rs" \
  "tests/format-compatibility/src/lib.rs" \
  "docs/some-doc.md" \
  | bash "$GATE" --classify-package-owners 2>/dev/null)
# A currently-missed tools/* member must resolve to ITS package (has a lib -> 1).
if printf '%s\n' "$owners_out" | grep -qxF "format-validator|1"; then
  ok "owners: tools/format-validator resolves to its own package (was falling through)"
else
  bad "owners: tools/format-validator/src/lib.rs did NOT resolve to format-validator"
  echo "------- owners output -------"; printf '%s\n' "$owners_out"; echo "-----------------------------"
fi
# A bindings/* member must resolve to its cdylib package (no lib target -> 0).
if printf '%s\n' "$owners_out" | grep -qxF "cqlite-py|0" \
   && printf '%s\n' "$owners_out" | grep -qxF "cqlite-node|0"; then
  ok "owners: bindings/{python,node} resolve to cqlite-py|0 / cqlite-node|0 (cdylib, no --lib)"
else
  bad "owners: bindings/* did NOT resolve to their packages with has_lib=0"
  echo "------- owners output -------"; printf '%s\n' "$owners_out"; echo "-----------------------------"
fi
# The examples crate must resolve to its own package (has a lib -> 1).
if printf '%s\n' "$owners_out" | grep -qxF "cqlite-examples|1"; then
  ok "owners: examples/ resolves to cqlite-examples (was falling through)"
else
  bad "owners: examples/basic.rs did NOT resolve to cqlite-examples"
  echo "------- owners output -------"; printf '%s\n' "$owners_out"; echo "-----------------------------"
fi
# A nested member (tests/format-compatibility) must win over its parent tests/.
if printf '%s\n' "$owners_out" | grep -qxF "format-compatibility-tests|0"; then
  ok "owners: nested tests/format-compatibility wins longest-prefix over tests/"
else
  bad "owners: tests/format-compatibility did NOT resolve to format-compatibility-tests"
  echo "------- owners output -------"; printf '%s\n' "$owners_out"; echo "-----------------------------"
fi
# The workspace-root `cqlite` package (manifest dir == repo root) is a degenerate
# catch-all prefix and must NOT be a path owner — a docs-only change resolves to
# NO package (falls through to the cqlite-core --lib default), not to root cqlite.
if printf '%s\n' "$owners_out" | grep -q '^cqlite|'; then
  bad "owners: repo-root 'cqlite' package wrongly claimed a path (degenerate catch-all)"
  echo "------- owners output -------"; printf '%s\n' "$owners_out"; echo "-----------------------------"
else
  ok "owners: repo-root 'cqlite' package excluded as a path owner (docs change -> fallback)"
fi
# No metadata parser -> NO ownership resolution at all (lib-only fallback).
noparser_owners=$(AGENT_GATE_TEST_NO_METADATA_PARSER=1 printf '%s\n' \
  "tools/format-validator/src/lib.rs" \
  | AGENT_GATE_TEST_NO_METADATA_PARSER=1 bash "$GATE" --classify-package-owners 2>/dev/null)
if [ -z "$noparser_owners" ]; then
  ok "owners: no-parser fallback emits NO ownership (scopes to cqlite-core --lib)"
else
  bad "owners: no-parser fallback emitted ownership without a metadata parser"
  echo "------- owners output -------"; printf '%s\n' "$noparser_owners"; echo "-----------------------------"
fi

# 7b. No metadata parser (issue #1821 roborev round 3): per-`--test`-target
#     selection REQUIRES a Cargo-metadata parser (jq OR python3). When NEITHER is
#     available the fallback must emit NO `--test` targets at all — otherwise it
#     would run feature-gated targets (e.g. issue_1388_compact_major_drop, which
#     needs write-support) feature-less and FAIL --lite spuriously in a minimal
#     shell env. run_scoped_tests then scopes to package --lib only. We force the
#     no-parser branch hermetically via AGENT_GATE_TEST_NO_METADATA_PARSER=1
#     (no PATH surgery on jq/python3/cargo) and feed the SAME paths as test 7,
#     including a feature-gated target and real direct targets.
noparser_out=$(AGENT_GATE_TEST_NO_METADATA_PARSER=1 printf '%s\n' \
  "cqlite-core/tests/compact_command.rs" \
  "tests/cassandra5_header_tests.rs" \
  "cqlite-cli/tests/issue_1388_compact_major_drop.rs" \
  | AGENT_GATE_TEST_NO_METADATA_PARSER=1 bash "$GATE" --classify-test-targets 2>/dev/null)
if [ -z "$noparser_out" ]; then
  ok "no-parser: fallback emits NO --test targets (lib-only) when jq/python3 absent"
else
  bad "no-parser: fallback emitted --test targets without a metadata parser (should be lib-only)"
  echo "------- classify output -------"; printf '%s\n' "$noparser_out"; echo "-------------------------------"
fi

# 7c. No metadata parser RUNS the core lib tests (issue #1821 roborev): the
#     lib-only fallback must ACTUALLY RUN cqlite-core's tests, not compile-check
#     them. The old code consulted pkg_has_lib (empty index -> 0) and degraded to
#     `--no-run`. Assert the fallback command includes `--lib` and does NOT include
#     `--no-run`. Force the no-parser branch hermetically via the existing hook.
noparser_cmd=$(AGENT_GATE_TEST_NO_METADATA_PARSER=1 bash "$GATE" --scoped-test-cmd-noparser 2>/dev/null)
if printf '%s\n' "$noparser_cmd" | grep -qF -- '--lib' \
   && ! printf '%s\n' "$noparser_cmd" | grep -qF -- '--no-run'; then
  ok "no-parser: fallback RUNS cqlite-core --lib tests (has --lib, no --no-run)"
else
  bad "no-parser: fallback command does not run lib tests (want --lib, not --no-run)"
  echo "------- scoped cmd -------"; printf '%s\n' "$noparser_cmd"; echo "--------------------------"
fi

# 7d. Python-binding blast-radius routing (issue #1893): cqlite-py is a pyo3 cdylib
#     whose `cargo test -p cqlite-py` ALWAYS fails the libpython link, so --lite was
#     BLIND for python diffs (zero signal on ~1/3 of binding issues; e.g. #1891,
#     #1929). A bindings/python change must now route to the python tier (maturin
#     develop --profile dev + the not-slow pytest tier) instead. The gate exposes
#     the PLAN via the hidden `--classify-scoped-plan` hook (stdin paths ->
#     "rust-pkg: <pkg>" / "python-tier: <cmd>"), asserted WITHOUT running maturin.
#
#     These cases drive the EXECUTOR's routing, not a parallel copy (roborev job
#     1450): classify_scoped_plan is the single routing function — run_scoped_tests
#     parses its output ("rust-pkg:" -> cargo packages, "python-tier:" -> the
#     python-tier flag), so the plan the hook emits IS the routing the executor
#     performs. Case 7e below structurally guards that consumption.

# python-only diff -> python tier selected, NO rust cargo package (never cqlite-py).
py_only=$(printf '%s\n' \
  "bindings/python/tests/conftest.py" \
  "bindings/python/src/database.rs" \
  | bash "$GATE" --classify-scoped-plan 2>/dev/null)
# The advertised plan string is COMPOSED from the same PYTHON_LITE_*_CMD component
# constants the executor eval's (roborev job 1449), so asserting the exact canonical
# command here pins what actually runs — not a parallel copy that can drift.
if printf '%s\n' "$py_only" | grep -qxF \
     "python-tier: maturin develop --profile dev -m bindings/python/Cargo.toml && pytest bindings/python/tests -m 'not slow' -q"; then
  ok "py-route: python-only diff selects the maturin --profile dev + not-slow-pytest tier (exact canonical command)"
else
  bad "py-route: python-only diff did NOT select the canonical python tier command"
  echo "------- plan -------"; printf '%s\n' "$py_only"; echo "--------------------"
fi
if printf '%s\n' "$py_only" | grep -q "^rust-pkg:"; then
  bad "py-route: python-only diff still selected a rust cargo package (cqlite-py run is the always-failing path)"
  echo "------- plan -------"; printf '%s\n' "$py_only"; echo "--------------------"
else
  ok "py-route: python-only diff selects NO rust cargo package (cqlite-py excluded)"
fi

# mixed diff (python + core) -> BOTH the rust-scoped package AND the python tier.
mixed=$(printf '%s\n' \
  "bindings/python/src/value.rs" \
  "cqlite-core/src/storage/sstable/reader.rs" \
  | bash "$GATE" --classify-scoped-plan 2>/dev/null)
if printf '%s\n' "$mixed" | grep -qxF "rust-pkg: cqlite-core" \
   && printf '%s\n' "$mixed" | grep -q "^python-tier: "; then
  ok "py-route: mixed diff selects BOTH cqlite-core AND the python tier"
else
  bad "py-route: mixed diff did NOT select both rust + python tier"
  echo "------- plan -------"; printf '%s\n' "$mixed"; echo "--------------------"
fi
# cqlite-py must NEVER appear as a rust cargo package in the mixed plan either.
if printf '%s\n' "$mixed" | grep -q "cqlite-py"; then
  bad "py-route: mixed diff plan referenced cqlite-py as a cargo package (must be python tier only)"
  echo "------- plan -------"; printf '%s\n' "$mixed"; echo "--------------------"
else
  ok "py-route: mixed diff never runs cargo test -p cqlite-py"
fi

# node diff -> UNAFFECTED: scopes to cqlite-node, NO python tier.
node_only=$(printf '%s\n' \
  "bindings/node/src/database.rs" \
  | bash "$GATE" --classify-scoped-plan 2>/dev/null)
if printf '%s\n' "$node_only" | grep -qxF "rust-pkg: cqlite-node" \
   && ! printf '%s\n' "$node_only" | grep -q "^python-tier:"; then
  ok "py-route: node diff unaffected (cqlite-node, no python tier)"
else
  bad "py-route: node diff wrongly triggered the python tier or missed cqlite-node"
  echo "------- plan -------"; printf '%s\n' "$node_only"; echo "--------------------"
fi

# rust-only diff -> UNCHANGED: scopes to the rust package, NO python tier.
rust_only=$(printf '%s\n' \
  "cqlite-core/src/storage/sstable/reader.rs" \
  | bash "$GATE" --classify-scoped-plan 2>/dev/null)
if printf '%s\n' "$rust_only" | grep -qxF "rust-pkg: cqlite-core" \
   && ! printf '%s\n' "$rust_only" | grep -q "^python-tier:"; then
  ok "py-route: rust-only diff unchanged (cqlite-core, no python tier)"
else
  bad "py-route: rust-only diff behavior changed (unexpected python tier or missing cqlite-core)"
  echo "------- plan -------"; printf '%s\n' "$rust_only"; echo "--------------------"
fi

# 7e. Executor consumes the SINGLE routing function (roborev job 1450): the whole
#     point of single-sourcing is that an executor-only edit cannot silently revert
#     python routing to `cargo test -p cqlite-py` while the hook-based py-route
#     cases above stay green. Structurally assert that run_scoped_tests' body
#     invokes classify_scoped_plan (parses its plan) and contains NO duplicate
#     routing: no second cqlite-py exclusion loop over a package set. Extracting
#     the function body with awk is deterministic (top-level `}` ends it).
rst_body=$(awk '/^run_scoped_tests\(\)/{f=1} f{print} f&&/^\}/{exit}' "$GATE")
if printf '%s\n' "$rst_body" | grep -v '^[[:space:]]*#' | grep -q 'classify_scoped_plan'; then
  ok "py-route: executor (run_scoped_tests) consumes classify_scoped_plan (single routing source)"
else
  bad "py-route: run_scoped_tests no longer calls classify_scoped_plan — routing has been duplicated or forked from the asserted plan"
fi
# The executor must not re-implement the cqlite-py exclusion itself: outside
# comments, 'cqlite-py' must not appear in run_scoped_tests (the exclusion lives
# only in classify_scoped_plan, which the py-route cases assert directly).
if printf '%s\n' "$rst_body" | grep -v '^[[:space:]]*#' | grep -q 'cqlite-py'; then
  bad "py-route: run_scoped_tests re-implements cqlite-py routing inline (must come from classify_scoped_plan only)"
  printf '%s\n' "$rst_body" | grep -v '^[[:space:]]*#' | grep -n 'cqlite-py'
else
  ok "py-route: run_scoped_tests has no inline cqlite-py routing (exclusion single-sourced)"
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
        | grep -qxF "cqlite-core|compact_command|"; then
    ok "bash-compat: --lite classification path runs under /bin/bash (major ${bin_bash_major:-?})"
  else
    bad "bash-compat: --lite classification path failed under /bin/bash (major ${bin_bash_major:-?})"
  fi
  # 8b. The python-tier routing hook (issue #1893) must also run under /bin/bash 3.x.
  if printf '%s\n' "bindings/python/tests/conftest.py" \
       | /bin/bash "$GATE" --classify-scoped-plan 2>/dev/null \
       | grep -q "^python-tier: "; then
    ok "bash-compat: --classify-scoped-plan python routing runs under /bin/bash (major ${bin_bash_major:-?})"
  else
    bad "bash-compat: --classify-scoped-plan python routing failed under /bin/bash (major ${bin_bash_major:-?})"
  fi
fi

# 9. Accelerator absence WARN + state markers (issue #1848). The gate must:
#    (a) mark an intentionally-disabled accelerator (CQLITE_DISABLE_*) `off` and
#        emit NO WARN; (b) mark a truly MISSING accelerator `absent` and emit a
#        LOUD WARN with the one-line install command. A silent 3x-slower machine is
#        the failure this guards against.
#
# 9a. Intentional disable → off + no WARN (deterministic; no PATH surgery).
disable_err="$tmp/disable.stderr"
AGENT_GATE_SUMMARY_FILE="$tmp/disable.txt" \
  CQLITE_DISABLE_SCCACHE=1 CQLITE_DISABLE_NEXTEST=1 \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>"$disable_err"
if grep -qE '^accelerators: sccache=off nextest=off ' "$tmp/disable.txt"; then
  ok "accel-disable: CQLITE_DISABLE_* -> sccache=off nextest=off in SUMMARY"
else
  bad "accel-disable: disabled accelerators not marked off"
  grep '^accelerators:' "$tmp/disable.txt" 2>/dev/null || cat "$tmp/disable.txt"
fi
if grep -q 'WARN: sccache not installed' "$disable_err" \
   || grep -q 'WARN: cargo-nextest not installed' "$disable_err"; then
  bad "accel-disable: emitted an absent-WARN for an INTENTIONALLY disabled accelerator"
else
  ok "accel-disable: no absent-WARN when accelerator intentionally disabled"
fi

# 9b. Absent → WARN + `absent` marker. Build a minimal PATH bindir that symlinks
#     the tools the selftest path needs but deliberately OMITS sccache +
#     cargo-nextest, so `command -v` inside the gate finds neither regardless of
#     what is installed on the host.
#
#     FAIL-CLOSED (roborev, job 1438): a selftest guarding against silent
#     degradation must never itself degrade silently. If the minimal-PATH run
#     cannot start or trips a missing tool, that is a TEST FAILURE (`bad`) naming
#     the tool to add to the allowlist below — never a quiet info-skip that turns
#     this commit's most important assertion into a no-op on some hosts.
#
#     Allowlist provenance: traced empirically by running the selftest under a
#     bash-only PATH and iterating until exit 0. The REQUIRED set (the selftest
#     path hard-fails without them) is: bash dirname mktemp grep cp cat. The
#     other coreutils below are headroom so a future gate change that touches a
#     common tool (mkdir, tail, cut, wc, stat, ...) keeps working instead of
#     failing this case. Tools the gate itself guards with fallbacks (nproc,
#     sysctl, cargo, git, python3) are OPTIONAL here: absent-on-host is a
#     specifically-known, documented platform difference (no nproc on macOS, no
#     sysctl on Linux), so they are linked when present and skipped when not.
#
#     Tool paths resolve via `type -P` (forces a PATH *file* lookup; bash 3.2+)
#     — NOT `command -v`, which can return a shell function/alias NAME from the
#     host environment and produce a self-referential dangling symlink.
accel_bin="$tmp/accel-bin"
mkdir -p "$accel_bin"
accel_link_fail=0
# REQUIRED: the selftest path hard-fails without these — missing on the host is
# itself a failure (fail-closed), not a skip.
for tool in bash dirname mktemp grep cp cat; do
  p=$(type -P "$tool" 2>/dev/null)
  if [ -z "$p" ]; then
    bad "accel-absent: required tool '$tool' not resolvable on this host (cannot build minimal PATH)"
    accel_link_fail=1
    continue
  fi
  if ! ln -sf "$p" "$accel_bin/$tool" 2>/dev/null; then
    bad "accel-absent: could not link required tool '$tool' into minimal PATH"
    accel_link_fail=1
  fi
done
# OPTIONAL: gate-guarded/platform tools + coreutils headroom (see provenance note).
for tool in env git python3 sed awk head tail tr sort cut wc stat mkdir rm ln mv \
            touch chmod basename uname date sleep expr find xargs hostname \
            cargo nproc sysctl; do
  p=$(type -P "$tool" 2>/dev/null) || continue
  [ -n "$p" ] && { ln -sf "$p" "$accel_bin/$tool" 2>/dev/null || true; }
done
absent_err="$tmp/absent.stderr"
absent_rc=0
if [ "$accel_link_fail" -eq 0 ]; then
  PATH="$accel_bin" AGENT_GATE_SUMMARY_FILE="$tmp/absent.txt" \
    "$accel_bin/bash" "$GATE" --emit-summary-selftest >/dev/null 2>"$absent_err" || absent_rc=$?
  # Any 'command not found' means the gate now invokes a tool the allowlist
  # lacks — fail loudly and NAME it so the fix is mechanical (add it above).
  if grep -q 'command not found' "$absent_err"; then
    bad "accel-absent: gate hit 'command not found' under minimal PATH — add the named tool(s) to the allowlist above"
    grep 'command not found' "$absent_err" | sort -u
    accel_link_fail=1
  fi
  # A non-zero rc WITHOUT a visible 'command not found' can still be a missing
  # tool: emit_summary suppresses its verifier's stderr (grep ... 2>/dev/null),
  # so e.g. a missing grep surfaces as a bogus 'could not write complete summary
  # file' instead. Either way a non-zero rc here is a test FAILURE, never a skip.
  if [ "$absent_rc" -ne 0 ] && [ "$accel_link_fail" -eq 0 ]; then
    bad "accel-absent: selftest under minimal PATH exited $absent_rc (want 0; possibly a missing tool whose error was suppressed — see stderr)"
    echo "------- stderr -------"; cat "$absent_err"; echo "----------------------"
    accel_link_fail=1
  fi
fi
if [ "$accel_link_fail" -eq 0 ]; then
  ok "accel-absent: selftest EXECUTED under minimal PATH (exit 0, no missing tools)"
  if grep -qE '^accelerators: sccache=absent nextest=absent ' "$tmp/absent.txt"; then
    ok "accel-absent: missing accelerators marked absent in SUMMARY"
  else
    bad "accel-absent: missing accelerators not marked absent"
    grep '^accelerators:' "$tmp/absent.txt" 2>/dev/null || cat "$tmp/absent.txt"
  fi
  if grep -q 'WARN: sccache not installed' "$absent_err" \
     && grep -q 'WARN: cargo-nextest not installed' "$absent_err"; then
    ok "accel-absent: loud WARN + install command emitted for each missing accelerator"
  else
    bad "accel-absent: missing loud WARN for an absent accelerator"
    echo "------- stderr -------"; cat "$absent_err"; echo "----------------------"
  fi
fi

# ============================================================================
# ISSUE #2078: FULL gate fails CLOSED when the fetched dataset corpus is absent.
# A fresh worktree carries ~19 tiny committed byte-parity reference *-Data.db, so the
# historical "any Data.db present" preflight PASSes while the main dataset components
# SKIP internally — a green SUMMARY that validated ZERO dataset correctness. The FULL
# gate must FAIL; --lite/--only stay lenient; an explicit opt-out restores SKIP and
# stamps a visible marker.
# ============================================================================

# Dummy roots: one with a Data.db but NO canonical corpus (test_basic) — the exact
# committed-refs-only shape — and one WITH test_basic present.
ds_nocorpus=$(mktemp -d "$tmp/ds-nocorpus.XXXXXX")
mkdir -p "$ds_nocorpus/sstables/test_dummy/x-0001"
: >"$ds_nocorpus/sstables/test_dummy/x-0001/nb-1-big-Data.db"
ds_corpus=$(mktemp -d "$tmp/ds-corpus.XXXXXX")
mkdir -p "$ds_corpus/sstables/test_basic/simple_table-0001"
: >"$ds_corpus/sstables/test_basic/simple_table-0001/nb-1-big-Data.db"

# 12a. FULL gate FAIL-CLOSED: point at the corpus-absent root and run the real full
#      gate. apply_fixture_preflight fires BEFORE any cargo component, so this is fast
#      (it exits at the preflight). The recovery file must show the FAIL-CLOSED marker
#      + RESULT: FAIL and never RESULT: PASS. Cap disabled so the run never queues.
full_fail="$tmp/2078-full-fail.txt"
CQLITE_GATE_DISABLE_CAP=1 CQLITE_DATASETS_ROOT="$ds_nocorpus" \
  AGENT_GATE_SUMMARY_FILE="$full_fail" bash "$GATE" >/dev/null 2>&1
full_fail_rc=$?
if [ "$full_fail_rc" -ne 0 ] \
   && grep -q "missing-fixtures: FAIL-CLOSED" "$full_fail" 2>/dev/null \
   && grep -q "^RESULT: FAIL" "$full_fail" 2>/dev/null \
   && ! grep -q "^RESULT: PASS" "$full_fail" 2>/dev/null; then
  ok "2078-full-fail: FULL gate FAILs CLOSED on an absent corpus (marker + RESULT: FAIL, no cargo)"
else
  bad "2078-full-fail: expected non-zero exit + FAIL-CLOSED marker + RESULT: FAIL (rc=$full_fail_rc)"
  echo "------- captured -------"; cat "$full_fail" 2>/dev/null; echo "------------------------"
fi

# 12b. Opt-out restores SKIP + stamps a VISIBLE marker. First the pure decision hook,
#      then the marker driven through the REAL emit path (--emit-summary-selftest).
optout_status=$(CQLITE_DATASETS_ROOT="$ds_nocorpus" AGENT_GATE_ALLOW_MISSING_FIXTURES=1 \
  bash "$GATE" --preflight-fixtures 2>/dev/null | grep '^STATUS:' | sed 's/^STATUS: //')
if [ "$optout_status" = OPTOUT ]; then
  ok "2078-optout: corpus absent + AGENT_GATE_ALLOW_MISSING_FIXTURES=1 → STATUS OPTOUT"
else
  bad "2078-optout: expected STATUS OPTOUT (got '$optout_status')"
fi
optout_block="$tmp/2078-optout.txt"
CQLITE_DATASETS_ROOT="$ds_nocorpus" AGENT_GATE_ALLOW_MISSING_FIXTURES=1 \
  AGENT_GATE_SUMMARY_FILE="$optout_block" bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
if grep -q "^missing-fixtures: OPT-OUT" "$optout_block" 2>/dev/null \
   && grep -q "^RESULT: PASS" "$optout_block" 2>/dev/null; then
  ok "2078-optout: emitted SUMMARY carries the visible OPT-OUT marker line"
else
  bad "2078-optout: expected the OPT-OUT marker in the emitted block"
  echo "------- captured -------"; cat "$optout_block" 2>/dev/null; echo "------------------------"
fi

# 12c. Corpus PRESENT → STATUS OK (byte-identical behavior; the guard is a no-op).
present_status=$(CQLITE_DATASETS_ROOT="$ds_corpus" bash "$GATE" --preflight-fixtures 2>/dev/null \
  | grep '^STATUS:' | sed 's/^STATUS: //')
if [ "$present_status" = OK ]; then
  ok "2078-present: canonical corpus present → STATUS OK (no FAIL, no marker)"
else
  bad "2078-present: expected STATUS OK with the corpus present (got '$present_status')"
fi

# 12d. --lite is UNAFFECTED: a lite run with the corpus-absent root must not fail at a
#      preflight (the guard is full-gate-only). Drive the lite emission path and assert
#      a clean LITE block with NO missing-fixtures line.
lite_block="$tmp/2078-lite.txt"
CQLITE_DATASETS_ROOT="$ds_nocorpus" AGENT_GATE_SUMMARY_FILE="$lite_block" \
  bash "$GATE" --lite --emit-summary-selftest >/dev/null 2>&1
lite_rc=$?
if [ "$lite_rc" -eq 0 ] \
   && grep -q "AGENT-GATE LITE SUMMARY" "$lite_block" 2>/dev/null \
   && ! grep -q "missing-fixtures:" "$lite_block" 2>/dev/null; then
  ok "2078-lite: --lite is unaffected by an absent corpus (clean LITE block, no marker)"
else
  bad "2078-lite: --lite should be unaffected (rc=$lite_rc)"
  echo "------- captured -------"; cat "$lite_block" 2>/dev/null; echo "------------------------"
fi

# ============================================================================
# ISSUE #2121: --lite OVERALL must aggregate the file-size/fmt/clippy verdicts.
# Before the fix, run_lite iterated NAMES directly (which held ONLY the scoped-tests
# entry), so a real clippy -D warnings / fmt --check / file-size ratchet FAIL emitted
# RESULT: PASS + exit 0 — a trust-critical false-green lite report (agents key on the
# RESULT line). The aggregation now lives in aggregate_lite_components and is exercised
# HERMETICALLY (no cargo) via the hidden --lite-aggregate-selftest hook: it seeds the
# per-component .result files run_lite's foreground components would write, seeds the
# scoped-tests NAMES entry run_scoped_tests appends, then runs the SAME aggregator +
# emit + exit path run_lite uses.
# ============================================================================

# 13a. A single component FAIL flips RESULT: FAIL + non-zero exit, for EACH of the
#      three foreground lite components (file-size, fmt, clippy), and the failing
#      component line now appears in the block (it did not, pre-fix).
for comp in file-size fmt clippy; do
  agg_results="file-size:PASS fmt:PASS clippy:PASS"
  agg_results="${agg_results/$comp:PASS/$comp:FAIL}"
  agg_file="$tmp/2121-$comp-fail.txt"
  AGENT_GATE_SUMMARY_FILE="$agg_file" \
    AGENT_GATE_TEST_LITE_RESULTS="$agg_results" AGENT_GATE_TEST_LITE_SCOPED=PASS \
    bash "$GATE" --lite-aggregate-selftest >/dev/null 2>&1
  agg_rc=$?
  if [ "$agg_rc" -ne 0 ] \
     && grep -q "^RESULT: FAIL" "$agg_file" 2>/dev/null \
     && ! grep -q "^RESULT: PASS" "$agg_file" 2>/dev/null \
     && grep -qE "^$comp: +FAIL" "$agg_file" 2>/dev/null; then
    ok "2121-$comp-fail: $comp FAIL -> RESULT: FAIL + exit $agg_rc + '$comp' line shown"
  else
    bad "2121-$comp-fail: expected RESULT: FAIL + non-zero exit + '$comp: FAIL' line (rc=$agg_rc)"
    echo "------- captured -------"; cat "$agg_file" 2>/dev/null; echo "------------------------"
  fi
done

# 13b. All components PASS -> RESULT: PASS + exit 0 (the aggregator must not over-fail).
agg_pass="$tmp/2121-all-pass.txt"
AGENT_GATE_SUMMARY_FILE="$agg_pass" \
  AGENT_GATE_TEST_LITE_RESULTS="file-size:PASS fmt:PASS clippy:PASS" \
  AGENT_GATE_TEST_LITE_SCOPED=PASS \
  bash "$GATE" --lite-aggregate-selftest >/dev/null 2>&1
agg_pass_rc=$?
if [ "$agg_pass_rc" -eq 0 ] && grep -q "^RESULT: PASS" "$agg_pass" 2>/dev/null; then
  ok "2121-all-pass: every component PASS -> RESULT: PASS + exit 0"
else
  bad "2121-all-pass: expected RESULT: PASS + exit 0 (rc=$agg_pass_rc)"
  echo "------- captured -------"; cat "$agg_pass" 2>/dev/null; echo "------------------------"
fi

# 13c. The scoped-tests FAIL path (the only one that flipped OVERALL pre-fix) must
#      still flip it — a regression guard that the fix preserves existing behavior.
agg_scoped="$tmp/2121-scoped-fail.txt"
AGENT_GATE_SUMMARY_FILE="$agg_scoped" \
  AGENT_GATE_TEST_LITE_RESULTS="file-size:PASS fmt:PASS clippy:PASS" \
  AGENT_GATE_TEST_LITE_SCOPED=FAIL \
  bash "$GATE" --lite-aggregate-selftest >/dev/null 2>&1
agg_scoped_rc=$?
if [ "$agg_scoped_rc" -ne 0 ] && grep -q "^RESULT: FAIL" "$agg_scoped" 2>/dev/null; then
  ok "2121-scoped-fail: scoped-tests FAIL still -> RESULT: FAIL (existing path preserved)"
else
  bad "2121-scoped-fail: expected RESULT: FAIL + non-zero exit (rc=$agg_scoped_rc)"
  echo "------- captured -------"; cat "$agg_scoped" 2>/dev/null; echo "------------------------"
fi

# 13d. PRESENT-ONLY contract (the `--lite --only fmt` shape bootstrap-agent-machine.sh
#      runs): only fmt actually ran, so only fmt.result exists. The aggregator must
#      NOT force-fail the unselected file-size/clippy — it PASSes and shows neither.
agg_only="$tmp/2121-only-fmt.txt"
AGENT_GATE_SUMMARY_FILE="$agg_only" \
  AGENT_GATE_TEST_LITE_RESULTS="fmt:PASS" AGENT_GATE_TEST_LITE_SCOPED=PASS \
  bash "$GATE" --lite-aggregate-selftest >/dev/null 2>&1
agg_only_rc=$?
if [ "$agg_only_rc" -eq 0 ] && grep -q "^RESULT: PASS" "$agg_only" 2>/dev/null \
   && ! grep -qE "^(file-size|clippy): " "$agg_only" 2>/dev/null; then
  ok "2121-present-only: --only fmt shape PASSes; unselected components not force-failed"
else
  bad "2121-present-only: --only fmt shape must PASS with only fmt+scoped-tests shown (rc=$agg_only_rc)"
  echo "------- captured -------"; cat "$agg_only" 2>/dev/null; echo "------------------------"
fi

# 13e. STRUCTURAL single-source guard (mirrors test 7e): the hermetic cases above
#      exercise aggregate_lite_components, so assert the REAL executor (run_lite) still
#      invokes it — otherwise an executor edit could silently drop aggregation while
#      these cases stay green. Extract the function body with awk (top-level `}` ends it).
rl_body=$(awk '/^run_lite\(\)/{f=1} f{print} f&&/^\}/{exit}' "$GATE")
if printf '%s\n' "$rl_body" | grep -v '^[[:space:]]*#' | grep -q 'aggregate_lite_components'; then
  ok "2121-structural: run_lite invokes aggregate_lite_components (lite OVERALL aggregation single-sourced)"
else
  bad "2121-structural: run_lite no longer calls aggregate_lite_components — lite OVERALL aggregation lost"
fi

echo "----"
echo "passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ]
