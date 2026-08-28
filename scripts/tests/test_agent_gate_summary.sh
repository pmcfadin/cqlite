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

# #2751 defense-in-depth: scrub any AGENT_GATE_SUMMARY_FILE inherited from the
# caller before doing anything. Every case below pins its OWN caller-known path
# per-invocation, so a top-level unset only removes a leaked value that could
# otherwise be clobbered when this script is run standalone by an agent who has the
# var exported (the tooling-tests component scrubs it too — belt-and-suspenders).
unset AGENT_GATE_SUMMARY_FILE
START_MARKER="==== AGENT-GATE SUMMARY ===="
END_MARKER="==== END AGENT-GATE SUMMARY ===="
STAGE_LINE="fmt:" # representative stage line from the selftest block

PASS=0
FAIL=0
ok()   { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }
# A case whose PROPERTY IS UNOBSERVABLE on this box (a Linux-only kernel control on
# Darwin, an unreadable /proc entry) is reported as a SKIP — counted in neither total,
# so it can never be mistaken for a passing assertion (issue #3249 AC3).
skipped() { printf 'skip - %s\n' "$1"; }

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
#
# Two-tier assertion contract for the accelerators line (issues #2903/#2914). The
# line GROWS tokens over time (` sccache-health=` #2641, ` mold=` #2859, and more
# to come), and ` mold=` is Linux-only — so an end-anchored per-token assert is a
# latent, host-conditional break. Therefore:
#   tier 1 (tolerant): every PER-TOKEN assert matches its token as a FIELD via the
#     whole-token idiom (mold_token_is / accel_health_token_is, both used by the
#     asserts above them), so an appended token can never redden it. The token's
#     VALUE is still matched exactly.
#   tier 2 (strict): ONE whole-line grammar, $ACCEL_LINE_RE below, enumerating every
#     legal token. It is the deliberate canary: adding a token to agent-gate.sh
#     without extending this grammar reddens EVERY assert_accelerators call site
#     (~7 of them) with the same `malformed accelerators line: '<the line>'` message,
#     which names the offending line and points here. That is loud and uniform by
#     design — one grammar to extend, not N per-token asserts to chase. Do NOT relax
#     it to a `.*` tail; that is the failure mode this design prevents.
# The ` perf=` group (issue #3249) is appended AFTER the optional ` mold=` group,
# because that is the order accelerators_line emits them; both are Linux-only and
# therefore both are optional here, so a Darwin line (which ends at
# sccache-health) and a Linux line (which carries both) satisfy one grammar.
ACCEL_LINE_RE='^accelerators: sccache=(on|absent|off) nextest=(on|absent|off) lanes=(on|absent|off|serial) sccache-health=(na|ok|warn)( mold=(linked|overridden|present-unconfigured|absent))?( perf=(ok|kptr-restricted|absent|unknown|paranoid-[0-9]+))?$'

# accel_line_of <file>: print the FIRST `accelerators: ` line of <file> (rc 0), or
# print nothing (rc 1). `grep -m1` + capture-to-a-variable, deliberately with NO
# pipeline: this script runs under `set -uo pipefail`, where a
# `grep … | head -1 | grep -q` pipeline can return 141 because the leftmost grep is
# SIGPIPEd by head's early exit — which corrupts the exit status of every PREDICATE
# built on it (a wrong-value NEGATIVE assert would then "pass" without testing
# anything). Every helper below captures the line here first, then matches it
# in-memory, so no predicate rc in this file can be SIGPIPEd.
accel_line_of() {
  local file="$1" line
  line=$(grep -m1 -E '^accelerators: ' "$file" 2>/dev/null) || return 1
  printf '%s\n' "$line"
}

# body_mentions <text> <needle>: PREDICATE (rc 0/1) — does <text> contain <needle>
# on a NON-comment line? Same pipefail hazard as above and it really bites here: the
# `printf … | grep -v … | grep -q` shape this replaces was observed returning 141
# (leftmost grep SIGPIPEd by `grep -q`'s early exit) on a function body large enough
# to outlive the pipe buffer — reddening a POSITIVE structural assert at random and,
# worse, silently GREENING the negative one below it. Comment-stripping happens in a
# command substitution (rc irrelevant); the match is an in-memory `[[ == ]]`.
body_mentions() {
  local text="$1" needle="$2" stripped
  stripped=$(grep -v '^[[:space:]]*#' <<<"$text")
  [[ $stripped == *"$needle"* ]]
}

assert_accelerators() {
  local label="$1" file="$2"
  local line
  line=$(accel_line_of "$file")
  if [ -z "$line" ]; then
    bad "$label: no 'accelerators:' line in SUMMARY block (file: $file)"
    echo "------- captured -------"; cat "$file"; echo "------------------------"
    return
  fi
  # The optional trailing ` mold=<state>` token (issue #2859) appears on Linux
  # hosts only; Darwin output ends at sccache-health, byte-identical to pre-change.
  if [[ $line =~ $ACCEL_LINE_RE ]]; then
    ok "$label: accelerators line well-formed ($line)"
  else
    bad "$label: malformed accelerators line: '$line'"
  fi
}

# accel_token_is <file> <key> <expected>: PREDICATE (rc 0/1, silent) — does the
# accelerators line carry `<key>=<expected>` as a WHOLE space-delimited FIELD? This is
# the tier-1 idiom: a further trailing token (` mold=` #2859, any future one) cannot
# break it, while the value stays exact (`ok` never matches `okay`, `linked` never
# matches `linkedX`). The quoted `$expected` inside the case pattern is literal, so a
# value can never act as a glob; there is no pipeline, so the rc is never SIGPIPEd.
accel_token_is() {
  local file="$1" key="$2" expected="$3" line
  line=$(accel_line_of "$file") || return 1
  case " $line " in
    *" $key=$expected "*) return 0 ;;
  esac
  return 1
}

# mold_token_is <file> <expected>  /  accel_health_token_is <file> <expected>:
# the two tier-1 per-token predicates. Both are shared by the asserts below/above and
# by the 9c-iv regression guard, which must assert both the TRUE and the FALSE outcome.
mold_token_is()          { accel_token_is "$1" mold "$2"; }
accel_health_token_is()  { accel_token_is "$1" sccache-health "$2"; }
# perf_token_is: the same tier-1 whole-field idiom for the ` perf=` token (#3249).
# Today it is the LAST token, i.e. the most exposed to the next appended one — so it
# is built from accel_token_is rather than an end-anchored match, by construction.
perf_token_is()          { accel_token_is "$1" perf "$2"; }

# assert_mold_token <label> <file> <expected>: assert the accelerators line's mold
# token (issue #2859). <expected> is a state (linked|present-unconfigured|absent)
# or the literal "none" to require NO mold token (the Darwin contract).
assert_mold_token() {
  local label="$1" file="$2" expected="$3" line
  line=$(accel_line_of "$file")
  if [ "$expected" = none ]; then
    case " $line " in
      *" mold="*) bad "$label: mold token present but expected none ($line)" ;;
      *)          ok  "$label: no mold token (Darwin contract)" ;;
    esac
  elif mold_token_is "$file" "$expected"; then
    ok "$label: mold=$expected present"
  else
    bad "$label: expected mold=$expected, got: '$line'"
  fi
}

# assert_perf_token <label> <file> <expected>: assert the accelerators line's perf
# capability token (issue #3249). <expected> is a state
# (ok|paranoid-<N>|kptr-restricted|absent|unknown) or the literal "none" to require
# NO perf token (the Darwin contract — perf_event_paranoid is a Linux control).
assert_perf_token() {
  local label="$1" file="$2" expected="$3" line
  line=$(accel_line_of "$file")
  if [ "$expected" = none ]; then
    case " $line " in
      *" perf="*) bad "$label: perf token present but expected none ($line)" ;;
      *)          ok  "$label: no perf token (Darwin contract)" ;;
    esac
  elif perf_token_is "$file" "$expected"; then
    ok "$label: perf=$expected present"
  else
    bad "$label: expected perf=$expected, got: '$line'"
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

# 7c. No metadata parser FAILS LOUDLY (issue #2658): silently narrowing --lite to
#     `cqlite-core --lib` when neither jq nor python3 is present was a
#     false-confidence path on minimal boxes — a green --lite that validated NONE
#     of the dependent/integration crates (nor, post-#2658, the core-src
#     dependent-crate compile-checks). The no-parser path now emits a LOUD-FAIL
#     message naming the missing tooling. Assert the message (a) names jq AND
#     python3, and (b) does NOT advertise a narrowed `cqlite-core --lib` run.
noparser_msg=$(AGENT_GATE_TEST_NO_METADATA_PARSER=1 bash "$GATE" --scoped-noparser-fail-msg 2>/dev/null)
if printf '%s\n' "$noparser_msg" | grep -qF 'jq' \
   && printf '%s\n' "$noparser_msg" | grep -qF 'python3' \
   && ! printf '%s\n' "$noparser_msg" | grep -qF -- '--lib'; then
  ok "no-parser: FAILS loudly naming jq+python3 (never silently narrows to cqlite-core --lib)"
else
  bad "no-parser: fail message does not name the missing tools (or still advertises a --lib narrowing)"
  echo "------- fail msg -------"; printf '%s\n' "$noparser_msg"; echo "-----------------------"
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
if body_mentions "$rst_body" 'classify_scoped_plan'; then
  ok "py-route: executor (run_scoped_tests) consumes classify_scoped_plan (single routing source)"
else
  bad "py-route: run_scoped_tests no longer calls classify_scoped_plan — routing has been duplicated or forked from the asserted plan"
fi
# The executor must not re-implement the cqlite-py exclusion itself: outside
# comments, 'cqlite-py' must not appear in run_scoped_tests (the exclusion lives
# only in classify_scoped_plan, which the py-route cases assert directly).
if body_mentions "$rst_body" 'cqlite-py'; then
  bad "py-route: run_scoped_tests re-implements cqlite-py routing inline (must come from classify_scoped_plan only)"
  printf '%s\n' "$rst_body" | grep -v '^[[:space:]]*#' | grep -n 'cqlite-py'
else
  ok "py-route: run_scoped_tests has no inline cqlite-py routing (exclusion single-sourced)"
fi

# 7f. Core-src dependent-crate compile-check (issue #2658): a cqlite-core src
#     change can break the test code of a SEPARATE test crate (integration-tests,
#     format-compatibility-tests, cli/flight/root-cqlite test targets) without
#     touching that crate's files — invisible to --lite's per-package selection
#     (which routes only packages the diff itself touches), producing the main
#     lite-green->full-red wasted round. A core-src diff must now ALSO emit a
#     `cargo test --no-run` compile-check ("compile-check-pkg: <pkg>") for every
#     dependent test crate. The gate exposes this via the hidden
#     `--classify-core-dependent-compile-check` hook (no cargo test run).
cc_core=$(printf '%s\n' \
  "cqlite-core/src/storage/sstable/reader.rs" \
  | bash "$GATE" --classify-core-dependent-compile-check 2>/dev/null)
# The two acceptance-named dependent test crates must be compile-checked.
if printf '%s\n' "$cc_core" | grep -qxF "compile-check-pkg: cqlite-integration-tests" \
   && printf '%s\n' "$cc_core" | grep -qxF "compile-check-pkg: format-compatibility-tests"; then
  ok "core-dep: core-src diff adds --no-run compile-check of integration-tests + format-compatibility-tests"
else
  bad "core-dep: core-src diff did NOT add the dependent-crate compile-checks"
  echo "------- plan -------"; printf '%s\n' "$cc_core"; echo "--------------------"
fi
# cqlite-core itself must NOT be in the compile-check set (its --lib already runs),
# and cdylib bindings (no test targets) must not appear.
if printf '%s\n' "$cc_core" | grep -qF "compile-check-pkg: cqlite-core" \
   || printf '%s\n' "$cc_core" | grep -qE 'compile-check-pkg: (cqlite-py|cqlite-node)$'; then
  bad "core-dep: compile-check set wrongly included cqlite-core or a cdylib binding"
  echo "------- plan -------"; printf '%s\n' "$cc_core"; echo "--------------------"
else
  ok "core-dep: compile-check set excludes cqlite-core + cdylib bindings"
fi
# A NON-core diff (docs / another crate's src) must add NO compile-check at all.
cc_none=$(printf '%s\n' \
  "docs/some-doc.md" \
  "cqlite-cli/src/main.rs" \
  | bash "$GATE" --classify-core-dependent-compile-check 2>/dev/null)
if [ -z "$cc_none" ]; then
  ok "core-dep: a non-core-src diff adds NO dependent-crate compile-check"
else
  bad "core-dep: a non-core-src diff wrongly emitted compile-check targets"
  echo "------- plan -------"; printf '%s\n' "$cc_none"; echo "--------------------"
fi
# No metadata parser -> NO compile-check set (the caller FAILs loudly instead).
cc_noparser=$(AGENT_GATE_TEST_NO_METADATA_PARSER=1 printf '%s\n' \
  "cqlite-core/src/storage/sstable/reader.rs" \
  | AGENT_GATE_TEST_NO_METADATA_PARSER=1 bash "$GATE" --classify-core-dependent-compile-check 2>/dev/null)
if [ -z "$cc_noparser" ]; then
  ok "core-dep: no-parser emits NO compile-check plan (caller fails loudly)"
else
  bad "core-dep: no-parser wrongly emitted a compile-check plan"
  echo "------- plan -------"; printf '%s\n' "$cc_noparser"; echo "--------------------"
fi
# 7g. Executor consumes the SINGLE compile-check routing function (issue #2658):
#     an executor-only edit must not fork the compile-check plan. Structurally
#     assert run_scoped_tests invokes classify_core_dependent_compile_check.
if body_mentions "$rst_body" 'classify_core_dependent_compile_check'; then
  ok "core-dep: executor (run_scoped_tests) consumes classify_core_dependent_compile_check (single source)"
else
  bad "core-dep: run_scoped_tests does not call classify_core_dependent_compile_check — compile-check routing forked"
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

# 9c. sccache cache-health token (issue #2641). The accelerators line carries a
#     trailing `sccache-health=na|ok|warn` token driven by sccache's OWN error
#     counters (the characterization found the single "corruption under load"
#     incident had zero supporting evidence — so the mitigation is MONITORING the
#     real signal, not blindly auto-disabling caching under load). The state is
#     decided by _sccache_health via two test hooks (AGENT_GATE_TEST_SCCACHE_STATE
#     to force the sccache accelerator state, AGENT_GATE_TEST_SCCACHE_ERRORS to
#     force the error sum) so na/ok/warn assert deterministically without sccache
#     installed and without PATH surgery.
#     These three asserts match `sccache-health=<v>` as a FIELD (`( |$)` — the same
#     idiom as assert_mold_token), never end-anchored on the value: further trailing
#     accelerator tokens (` mold=` #2859, and any future one) must not break them
#     (issue #2903). The value itself stays exact — `ok( |$)` cannot match `okay`,
#     and the whole-line grammar is still asserted by assert_accelerators.
#
# 9c-i. sccache in use, ZERO error counters -> sccache-health=ok, NO corruption WARN.
health_err="$tmp/health-ok.stderr"
AGENT_GATE_SUMMARY_FILE="$tmp/health-ok.txt" \
  AGENT_GATE_TEST_SCCACHE_STATE=on AGENT_GATE_TEST_SCCACHE_ERRORS=0 \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>"$health_err"
if accel_health_token_is "$tmp/health-ok.txt" ok; then
  ok "sccache-health: on + 0 errors -> sccache-health=ok"
else
  bad "sccache-health: expected sccache-health=ok for on + 0 errors"
  grep '^accelerators:' "$tmp/health-ok.txt" 2>/dev/null || cat "$tmp/health-ok.txt"
fi
if grep -q 'WARN:.*corrupted or torn cache' "$health_err"; then
  bad "sccache-health: emitted a corruption WARN with ZERO error counters"
else
  ok "sccache-health: no corruption WARN when error counters are zero"
fi

# 9c-ii. sccache in use, NON-ZERO error counters -> sccache-health=warn + LOUD WARN.
AGENT_GATE_SUMMARY_FILE="$tmp/health-warn.txt" \
  AGENT_GATE_TEST_SCCACHE_STATE=on AGENT_GATE_TEST_SCCACHE_ERRORS=3 \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>"$tmp/health-warn.stderr"
if accel_health_token_is "$tmp/health-warn.txt" warn; then
  ok "sccache-health: on + >0 errors -> sccache-health=warn"
else
  bad "sccache-health: expected sccache-health=warn for on + >0 errors"
  grep '^accelerators:' "$tmp/health-warn.txt" 2>/dev/null || cat "$tmp/health-warn.txt"
fi
if grep -qE 'WARN: sccache reports 3 cache .* corrupted or torn cache' "$tmp/health-warn.stderr"; then
  ok "sccache-health: LOUD WARN emitted (naming count + inspect command) on non-zero error counters"
else
  bad "sccache-health: missing LOUD corruption WARN on non-zero error counters"
  echo "------- stderr -------"; cat "$tmp/health-warn.stderr"; echo "----------------------"
fi
# The mitigation must NOT disable caching or fail the gate on a warn (that would
# increase build pressure — the exact anti-goal from the #2641 characterization).
# Host-independent: a warn must never flip the sccache accelerator to `off`
# (blind auto-disable). The sccache= field reflects the host (on when installed,
# absent otherwise); the invariant is only that a health warn never disables it.
if grep -qE '^accelerators: sccache=off ' "$tmp/health-warn.txt"; then
  bad "sccache-health: sccache was disabled on a warn — the #2641 anti-goal"
  grep '^accelerators:' "$tmp/health-warn.txt" 2>/dev/null || cat "$tmp/health-warn.txt"
else
  ok "sccache-health: caching NOT auto-disabled on a warn (no blind auto-disable)"
fi

# 9c-iii. sccache NOT in use -> sccache-health=na, nothing to probe, no WARN.
AGENT_GATE_SUMMARY_FILE="$tmp/health-na.txt" \
  AGENT_GATE_TEST_SCCACHE_STATE=off \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>"$tmp/health-na.stderr"
if accel_health_token_is "$tmp/health-na.txt" na; then
  ok "sccache-health: sccache not in use -> sccache-health=na"
else
  bad "sccache-health: expected sccache-health=na when sccache not in use"
  grep '^accelerators:' "$tmp/health-na.txt" 2>/dev/null || cat "$tmp/health-na.txt"
fi

# 9c-iv. Regression guard for the NEXT appended accelerators token (issue #2914).
#        #2859 appended a Linux-only ` mold=` token and silently reddened three
#        end-anchored 9c asserts on every Linux host (green on Darwin, so it landed).
#        This case pins the two-tier contract documented at $ACCEL_LINE_RE by
#        synthesizing tomorrow's token on a COPY of a REAL emitted summary — the gate
#        script and its output are untouched, this is a test-side mutation only.
#        The sentinel is deliberately `__unknown-future-token__=x`, a string nobody
#        could ever legitimately ship as an accelerator token: a plausible name (say
#        `lto=thin`) would turn this guard into a false accusation on the day someone
#        correctly adds that token AND extends $ACCEL_LINE_RE.
#        The base fixture forces OS=Linux + mold=linked + perf=ok +
#        sccache-health=ok so the line deterministically carries EVERY tier-1 token
#        on any host, and the sentinel lands immediately after the CURRENT last
#        token (` perf=` since #3249) — the position tomorrow's token will actually
#        occupy. When a token is appended, move this expectation to the new last one.
FUTURE_TOKEN='__unknown-future-token__=x'
future_base="$tmp/health-future-base.txt"
future_accel="$tmp/health-future-token.txt"
AGENT_GATE_SUMMARY_FILE="$future_base" \
  AGENT_GATE_TEST_OS=Linux AGENT_GATE_TEST_MOLD_STATE=linked AGENT_GATE_TEST_PERF_STATE=ok \
  AGENT_GATE_TEST_SCCACHE_STATE=on AGENT_GATE_TEST_SCCACHE_ERRORS=0 \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
# The UNMUTATED base must satisfy the grammar — otherwise the canary below would be
# "armed" by a pre-existing defect rather than by the synthesized token.
assert_accelerators "accel-future-token-base" "$future_base"
sed "s/^accelerators: .*/& $FUTURE_TOKEN/" "$future_base" >"$future_accel"
if grep -qF " perf=ok $FUTURE_TOKEN" "$future_accel"; then
  ok "accel-future-token: guard fixture really carries an unknown token after the last known token"
else
  bad "accel-future-token: guard fixture did not gain a trailing token (guard is vacuous)"
  grep '^accelerators:' "$future_accel" 2>/dev/null || cat "$future_accel"
fi
# Tier 1: BOTH per-token asserts must survive the unknown trailing token — including
# assert_mold_token, whose token is the last one today and so is the most exposed.
if accel_health_token_is "$future_accel" ok; then
  ok "accel-future-token: sccache-health assert survives an unknown trailing token"
else
  bad "accel-future-token: an appended token broke the sccache-health assert (#2914 regression)"
  grep '^accelerators:' "$future_accel" 2>/dev/null || cat "$future_accel"
fi
assert_mold_token "accel-future-token" "$future_accel" linked
# ...and the CURRENT last token, the one most exposed to an appended sibling (#3249).
assert_perf_token "accel-future-token" "$future_accel" ok
# ...and neither may have been weakened into accepting a WRONG value, nor a value of
# which the truth is a prefix/superstring. Tolerating a new token != tolerating a
# bad token value. (The negative direction is asserted through the same predicates
# the asserts above are built from.)
wrong_val_ok=1
for wrong in warn na o okay; do
  if accel_health_token_is "$future_accel" "$wrong"; then
    bad "accel-future-token: health assert wrongly matched sccache-health=$wrong (value check weakened)"
    wrong_val_ok=0
  fi
done
for wrong in absent overridden present-unconfigured linke linkedx; do
  if mold_token_is "$future_accel" "$wrong"; then
    bad "accel-future-token: mold assert wrongly matched mold=$wrong (value check weakened)"
    wrong_val_ok=0
  fi
done
for wrong in absent unknown kptr-restricted paranoid-4 o okx; do
  if perf_token_is "$future_accel" "$wrong"; then
    bad "accel-future-token: perf assert wrongly matched perf=$wrong (value check weakened)"
    wrong_val_ok=0
  fi
done
if [ "$wrong_val_ok" -eq 1 ]; then
  ok "accel-future-token: wrong/partial health+mold+perf values still FAIL (values matched exactly)"
fi
# Tier 2: the whole-line grammar is the canary and must REJECT the unknown token, so
# a future token addition reddens the assert_accelerators call sites with an
# actionable "malformed accelerators line" naming the line — rather than passing in
# silence. If this ever goes green, the grammar has been relaxed to a `.*` tail.
future_line=$(accel_line_of "$future_accel")
if [[ $future_line =~ $ACCEL_LINE_RE ]]; then
  bad "accel-future-token: whole-line grammar accepted an unknown token (canary disarmed)"
else
  ok "accel-future-token: whole-line grammar still REJECTS an unknown token (canary armed)"
fi

# 9d. mold link-accelerator token (issue #2859). On Linux the accelerators line
#     carries a trailing `mold=linked|overridden|present-unconfigured|absent` token;
#     on Darwin it carries NO mold token (byte-identical to pre-change). The host
#     family is forced via AGENT_GATE_TEST_OS and the detected state via
#     AGENT_GATE_TEST_MOLD_STATE, so all four states assert deterministically here.
for state in linked overridden present-unconfigured absent; do
  mold_file="$tmp/mold-$state.txt"
  AGENT_GATE_SUMMARY_FILE="$mold_file" \
    AGENT_GATE_TEST_OS=Linux AGENT_GATE_TEST_MOLD_STATE="$state" \
    bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
  assert_mold_token "mold-linux-$state" "$mold_file" "$state"
  # The whole line must also still pass the (mold-aware) well-formed check.
  assert_accelerators "mold-linux-$state" "$mold_file"
done

# 9d-darwin. Darwin emits NO mold token even with a forced state present — the
#            token is Linux-only; macOS output ends at sccache-health.
mold_darwin="$tmp/mold-darwin.txt"
AGENT_GATE_SUMMARY_FILE="$mold_darwin" \
  AGENT_GATE_TEST_OS=Darwin AGENT_GATE_TEST_MOLD_STATE=linked \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
assert_mold_token "mold-darwin" "$mold_darwin" none
assert_accelerators "mold-darwin" "$mold_darwin"

# 9f. perf profiling capability token (issue #3249). Boxes ship with
#     kernel.perf_event_paranoid = 4, which denies ALL unprivileged perf use — a
#     PERMISSION verdict that reads like a missing CAPABILITY, and one that reverts
#     on reboot when no /etc/sysctl.d drop-in exists. The token makes "this box
#     cannot be profiled" visible in every pasted SUMMARY. Linux-only, same
#     contract as mold: NO token on Darwin. State forced via
#     AGENT_GATE_TEST_PERF_STATE so every value asserts deterministically
#     regardless of the host's real sysctls.
for state in ok paranoid-4 paranoid-2 kptr-restricted absent unknown; do
  perf_file="$tmp/perf-$state.txt"
  AGENT_GATE_SUMMARY_FILE="$perf_file" \
    AGENT_GATE_TEST_OS=Linux AGENT_GATE_TEST_MOLD_STATE=linked AGENT_GATE_TEST_PERF_STATE="$state" \
    bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
  assert_perf_token "perf-linux-$state" "$perf_file" "$state"
  assert_accelerators "perf-linux-$state" "$perf_file"
done

# 9f-darwin. perf_event_paranoid/kptr_restrict are Linux kernel controls, so Darwin
#            emits NO perf token even with a forced state — its line still ends at
#            sccache-health, byte-identical to pre-#3249 output.
perf_darwin="$tmp/perf-darwin.txt"
AGENT_GATE_SUMMARY_FILE="$perf_darwin" \
  AGENT_GATE_TEST_OS=Darwin AGENT_GATE_TEST_MOLD_STATE=linked AGENT_GATE_TEST_PERF_STATE=ok \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
assert_perf_token "perf-darwin" "$perf_darwin" none
assert_accelerators "perf-darwin" "$perf_darwin"

# 9f-real. REAL detection, NO AGENT_GATE_TEST_PERF_STATE: the production branch of
#          _perf_state, reading an actual /proc directory. Without this every case
#          above set the test seam, so hardcoding `_PERF_STATE="ok"` — a gate stamping
#          `perf=ok` on a paranoid-4 box, exactly what AC3 exists to prevent — passed
#          the whole suite, and so did forcing the real branch to always yield
#          `unknown`. The fixture is scripts/perf-capability.sh's own test seam, which
#          is inert without its hermetic marker — and which, since #3249 review R6-1/R6-2,
#          must be provably INSIDE a declared, STAMPED sandbox root. This suite's `$tmp` is
#          that root (every fixture below lives under it), so an out-of-sandbox seam — the
#          real /proc included — cannot steer these cases.
export CQLITE_PERF_TEST_SANDBOX="$tmp"
: >"$tmp/.cqlite-perf-sandbox"
perf_fixture_ok="$tmp/perf-proc-ok"; mkdir -p "$perf_fixture_ok"
printf -- '-1\n' >"$perf_fixture_ok/perf_event_paranoid"
printf '0\n'     >"$perf_fixture_ok/kptr_restrict"
perf_fixture_p4="$tmp/perf-proc-p4"; mkdir -p "$perf_fixture_p4"
printf '4\n' >"$perf_fixture_p4/perf_event_paranoid"
printf '1\n' >"$perf_fixture_p4/kptr_restrict"
for pair in "ok:$perf_fixture_ok" "paranoid-4:$perf_fixture_p4"; do
  want="${pair%%:*}"; fixture="${pair#*:}"
  real_file="$tmp/perf-real-$want.txt"
  env -u AGENT_GATE_TEST_PERF_STATE \
    AGENT_GATE_SUMMARY_FILE="$real_file" AGENT_GATE_TEST_OS=Linux \
    CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_PROC_DIR="$fixture" \
    bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
  assert_perf_token "perf-real-$want" "$real_file" "$want"
  assert_accelerators "perf-real-$want" "$real_file"
done

# 9f-host. THE HOST'S OWN /proc, WITH NO SEAM SET AT ALL (issue #3249 AC3(a)). Every
#          case above — 9f-real included — sets the LIBRARY's fixture seams
#          (CQLITE_PERF_TEST_MODE + CQLITE_PERF_PROC_DIR), so they prove the production
#          BRANCH runs but not that it reads this box's real kernel state. Here every
#          seam the code reads is unset (the five CQLITE_PERF_* seams plus both
#          AGENT_GATE_TEST_* forcings), so the token can only come from
#          /proc/sys/kernel. The expectation is DERIVED from the box's own two controls
#          by the documented rule and asserted as an EXACT whole-field token — never
#          hardcoded to `ok` (a paranoid-4 box MUST fail this case if the gate claims
#          otherwise) and never as a member of the alternation (which every state
#          satisfies). Skipped, not passed, off Linux or when a control is unreadable:
#          a case that cannot observe the property must say so, not bank a green.
perf_host_par_f=/proc/sys/kernel/perf_event_paranoid
perf_host_kptr_f=/proc/sys/kernel/kptr_restrict
perf_host_os=$(uname -s 2>/dev/null || echo unknown)
if [ "$perf_host_os" != Linux ]; then
  skipped "perf-host: host is $perf_host_os, not Linux — perf_event_paranoid/kptr_restrict are Linux controls (9f-darwin covers the no-token contract)"
elif [ ! -r "$perf_host_par_f" ] || [ ! -r "$perf_host_kptr_f" ]; then
  skipped "perf-host: $perf_host_par_f / $perf_host_kptr_f unreadable on this box — no real state to derive an expectation from"
else
  perf_host_par=$(tr -d '[:space:]' <"$perf_host_par_f")
  perf_host_kptr=$(tr -d '[:space:]' <"$perf_host_kptr_f")
  # The rule, from openspec/specs/agent-fleet-runtime/spec.md: paranoid <= 0 AND kptr == 0
  # => ok; paranoid >= 1 => paranoid-<N>; else kptr != 0 => kptr-restricted.
  if ! printf '%s' "$perf_host_par" | grep -Eq '^-?[0-9]+$' \
     || ! printf '%s' "$perf_host_kptr" | grep -Eq '^-?[0-9]+$'; then
    perf_host_want=unknown
  elif [ "$perf_host_par" -ge 1 ]; then
    perf_host_want="paranoid-$perf_host_par"
  elif [ "$perf_host_kptr" -ne 0 ]; then
    perf_host_want=kptr-restricted
  else
    perf_host_want=ok
  fi
  perf_host_file="$tmp/perf-host.txt"
  env -u AGENT_GATE_TEST_PERF_STATE -u AGENT_GATE_TEST_OS \
      -u CQLITE_PERF_TEST_MODE -u CQLITE_PERF_PROC_DIR -u CQLITE_PERF_SYSCTL_DIR \
      -u CQLITE_PERF_SYSCTL_EXTRA_DIRS -u CQLITE_PERF_TEST_PRIV_DIR \
      AGENT_GATE_SUMMARY_FILE="$perf_host_file" \
      bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
  assert_perf_token "perf-host (real /proc: paranoid=$perf_host_par kptr=$perf_host_kptr, no seam set)" \
    "$perf_host_file" "$perf_host_want"
  assert_accelerators "perf-host" "$perf_host_file"
fi

# 9f-free. The gate's emit-time perf path is documented as FREE — in the code, in
#          openspec/specs/agent-fleet-runtime/spec.md, in gate-contract.md and in
#          gate-ops.md: no `perf` exec, NO EXTERNAL PROCESS AT ALL, and no command
#          substitution. That last clause is not pedantry: a `$( )` forks a subshell,
#          so a "no subprocess" claim whose value is read back through one is
#          self-contradictory — and the original assert here only rejected a literal
#          `perf stat`, so the claim shipped UNENFORCED while the path in fact forked
#          several `$( )` per emit and re-sourced the 300-line helper each time.
#          THE STATED COST IS ZERO: zero external processes, zero command
#          substitutions, one source of the helper per gate RUN (not per emit).
#          Asserted three ways below, because each catches a different regression.
PERF_LIB="$(dirname "$GATE")/perf-capability.sh"

# fn_text <file> <name>: a function's verbatim definition text, whether written as a
# single line (`f() { …; }`) or a block ending in a column-0 `}`. Empty output means
# NOT FOUND, which the asserts treat as a FAILURE — a renamed function must never drop
# out of this audit silently.
fn_text() {
  awk -v n="$2" '
    index($0, n "()") == 1 {
      print
      if ($0 ~ /\}[[:space:]]*$/) exit
      inb = 1; next
    }
    inb { print; if ($0 ~ /^\}/) exit }
  ' "$1"
}

# The FULL emit-time path: the gate's two token functions plus every helper function
# they reach. Enumerated explicitly so the audit is a closed set, not a guess.
perf_path_text=""
perf_path_missing=""
for _fn in _perf_state_into _perf_accel_token_into; do
  _t=$(fn_text "$GATE" "$_fn")
  [ -n "$_t" ] || perf_path_missing="$perf_path_missing $_fn"
  perf_path_text="$perf_path_text$_t
"
done
# The containment gate reached from here is the SYNTACTIC one and its THREE builtin-only
# helpers (#3249 review R6-1/R6-2, plus perf_capability_nosymlink from #3261 AC2). Its resolving
# sibling — perf_capability_sandbox_ok_resolved — canonicalizes with `$(cd -P …)` and is
# deliberately NOT on this path: naming it here would be the tell that a fork had been introduced
# into the emit chain.
#   perf_capability_nosymlink joined this set with #3261 AC2 (roborev finding 2): sandbox_ok now
#   calls it on the summary path, so a `$(readlink -f …)`/`$(realpath …)` added there later — the
#   obvious way to "improve" a symlink check — would fork the emit chain. Omitting it left exactly
#   the silently-eroding-guard gap this issue exists to close. It is a CLOSED set: a function that
#   is renamed or removed makes fn_text return empty and reds `perf_path_missing`, so this list
#   cannot quietly under-count.
#   perf_capability_path_lines_ok joined for the SAME reason one round later (#3261, roborev round
#   3): path_within now calls it, so it is on this path, and a future `$(tr -d …)`/`$(printf …)`
#   there would fork the emit chain invisibly. Adding a helper to the emit path and NOT to this list
#   is now a recognised recurring miss — if you touch anything path_within reaches, add it here.
for _fn in perf_capability_token_into perf_capability_proc_read \
           perf_capability_proc_dir_into perf_capability_test_mode \
           perf_capability_seam_set perf_capability_is_int \
           perf_capability_sandbox_ok perf_capability_sandbox_root_into \
           perf_capability_nosymlink perf_capability_path_lines_ok \
           perf_capability_path_within; do
  _t=$(fn_text "$PERF_LIB" "$_fn")
  [ -n "$_t" ] || perf_path_missing="$perf_path_missing $_fn"
  perf_path_text="$perf_path_text$_t
"
done
if [ -z "$perf_path_missing" ]; then
  ok "perf-free: every function on the emit-time perf path was located (closed audit set)"
else
  bad "perf-free: perf-path function(s) not found — renamed without updating this audit?$perf_path_missing"
fi
# (a) STATIC: the whole path contains ZERO command substitutions and ZERO backticks.
#     Counted (not merely grepped) so the failure message states the real number
#     against the documented one.
perf_subs=$(printf '%s\n' "$perf_path_text" | grep -o '\$(' | wc -l | tr -d ' ')
perf_ticks=$(printf '%s\n' "$perf_path_text" | grep -o '`' | wc -l | tr -d ' ')
if [ "$perf_subs" -eq 0 ] && [ "$perf_ticks" -eq 0 ]; then
  ok "perf-free: the emit-time perf path contains 0 command substitutions (documented cost: 0)"
else
  bad "perf-free: the emit-time perf path forks $perf_subs command substitution(s) + $perf_ticks backtick(s) — documented cost is 0; either remove them or correct the claim in the code comment, the spec, gate-contract.md and gate-ops.md"
fi
if ! body_mentions "$perf_path_text" 'perf stat'; then
  ok "perf-free: the emit-time perf path never execs 'perf stat' (free /proc read only)"
else
  bad "perf-free: the emit-time perf path execs perf stat"
fi
# ...and the 300-line helper is sourced ONCE at script scope, not from inside the
# per-emit path (a per-emit source re-reads the file on every summary).
if grep -q '^_PERF_CAP_LOADED=' "$GATE" \
   && ! body_mentions "$perf_path_text" 'perf-capability.sh'; then
  ok "perf-free: the helper is sourced once at script scope, never from the per-emit path"
else
  bad "perf-free: the perf helper is (re-)sourced inside the per-emit path, or the script-scope load flag is gone"
fi
# ...and the call site must consume the token through a VARIABLE: reading
# `$(_perf_accel_token)` there would reintroduce the very fork the path excludes.
accel_fn_text=$(fn_text "$GATE" accelerators_line)
if printf '%s' "$accel_fn_text" | grep -q '_perf_accel_token_into' \
   && ! printf '%s' "$accel_fn_text" | grep -q '\$(_perf_accel_token\|`_perf_accel_token'; then
  ok "perf-free: accelerators_line consumes the perf token through a variable, not a subshell"
else
  bad "perf-free: accelerators_line reads the perf token through a command substitution"
fi
# (b) RUNTIME: run the gate's OWN extracted path with PATH pointing at a NONEXISTENT
#     directory (so no external command can resolve) and with xtrace stamping
#     ${BASH_SUBSHELL} (so ANY subshell — command substitution, pipeline, `( )` — is
#     visible). A static scan can be fooled by an indirection; this cannot. Correct
#     token + no subshell + no attempted exec is the whole claim, executed.
#     Every attempted exec is recorded by `command_not_found_handle`, which appends to
#     a FILE — deliberately NOT to stderr, because a `2>/dev/null` on the offending
#     line inside the code under test hides a stderr-only signal completely (measured:
#     a `id -u >/dev/null 2>&1` mutation was invisible until this handler existed).
#     PATH must name a MISSING DIRECTORY, not be empty: an empty PATH is one empty
#     element = the current directory, so bash tries `./id`, reports ENOENT and never
#     consults the handler. bash 4+; on bash 3.2 the xtrace/stderr grep still fires.
perf_probe="$tmp/perf-free-probe.sh"
perf_extlog="$tmp/perf-free-external.txt"; : >"$perf_extlog"
{
  printf '%s\n' 'set -uo pipefail'
  printf '%s\n' '. "$1"'
  fn_text "$GATE" _perf_state_into
  fn_text "$GATE" _perf_accel_token_into
  printf '%s\n' '_PERF_CAP_LOADED=1'
  printf '%s\n' '_AGENT_GATE_OS=Linux'
  printf '%s\n' 'tok=""'
  printf 'command_not_found_handle() { printf "EXTERNAL:%%s\\n" "$1" >>"%s"; return 127; }\n' "$perf_extlog"
  printf 'PATH=%s\n' "$tmp/perf-free-no-such-bin"
  printf '%s\n' "PS4='+SUB\${BASH_SUBSHELL} '"
  printf '%s\n' 'set -x'
  printf '%s\n' '_perf_accel_token_into tok'
  printf '%s\n' 'set +x'
  printf '%s\n' 'printf "TOKEN[%s]\n" "$tok"'
} >"$perf_probe"
perf_trace="$tmp/perf-free-trace.txt"
perf_probe_out=$(env -u AGENT_GATE_TEST_PERF_STATE -u AGENT_GATE_TEST_OS \
  CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_PROC_DIR="$perf_fixture_p4" \
  bash "$perf_probe" "$PERF_LIB" 2>"$perf_trace")
perf_probe_subshells=$(grep -c 'SUB[1-9]' "$perf_trace" 2>/dev/null || true)
perf_probe_execfail=$(grep -c 'No such file or directory\|command not found' "$perf_trace" 2>/dev/null || true)
perf_probe_ext=$(grep -c '^EXTERNAL:' "$perf_extlog" 2>/dev/null || true)
if [ "$perf_probe_out" = 'TOKEN[ perf=paranoid-4]' ] \
   && [ "${perf_probe_subshells:-0}" -eq 0 ] && [ "${perf_probe_execfail:-0}" -eq 0 ] \
   && [ "${perf_probe_ext:-0}" -eq 0 ]; then
  ok "perf-free: the extracted path yields perf=paranoid-4 with an unresolvable PATH, 0 subshells and 0 external commands (xtrace + not-found-handler verified)"
else
  bad "perf-free: runtime probe failed (out='$perf_probe_out' subshells=$perf_probe_subshells exec-failures=$perf_probe_execfail external=$perf_probe_ext: $(head -3 "$perf_extlog" | tr '\n' ' '))"
  head -20 "$perf_trace"
fi
perf_nopath="$tmp/perf-nopath.txt"
nopath_dir="$tmp/nopath-bin"; mkdir -p "$nopath_dir"
for t in bash sed awk grep cat env date mktemp uname tr cut sort head tail wc git printf sleep python3 rm mv cp mkdir touch dirname basename find id hostname stat diff; do
  src=$(command -v "$t" 2>/dev/null) && ln -sf "$src" "$nopath_dir/$t"
done
(
  env -u AGENT_GATE_TEST_PERF_STATE PATH="$nopath_dir" \
    AGENT_GATE_SUMMARY_FILE="$perf_nopath" AGENT_GATE_TEST_OS=Linux \
    CQLITE_PERF_TEST_MODE=1 CQLITE_PERF_PROC_DIR="$perf_fixture_p4" \
    bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
)
# Asserted as an EXACT whole-field value (not a `.*` presence grep that any state
# would satisfy — including the `unknown`/`absent` states a broken read produces).
assert_perf_token "perf-nopath" "$perf_nopath" paranoid-4
assert_accelerators "perf-nopath" "$perf_nopath"

# 9e. REAL detection (NO AGENT_GATE_TEST_MOLD_STATE override): exercise the actual
#     `command -v mold` + `_mold_block_active` + RUSTFLAGS branches. A stub `mold` is
#     put first on PATH and CARGO_HOME points at a temp dir we (don't) seed with the
#     managed block, so linked / overridden / present-unconfigured are decided by the
#     gate's real logic. The block marker must be the EXACT full line the writer emits
#     (prefix matching would let a user's own `# BEGIN cqlite-mold-*` comment
#     false-positive) — asserted by the notours case below.
mold_bin="$tmp/mold-bin"; mkdir -p "$mold_bin"
printf '#!/usr/bin/env bash\n[ "$1" = --version ] && echo "mold 2.4.0"\nexit 0\n' >"$mold_bin/mold"
chmod +x "$mold_bin/mold"
MOLD_MARK='# BEGIN cqlite-mold (managed by scripts/bootstrap-agent-machine.sh — do not edit inside)'

# 9e-i. mold on PATH + managed block in config.toml -> linked (real detection).
ch1=$(mktemp -d "$tmp/mold-ch1.XXXXXX")
printf '%s\n[target.x86_64-unknown-linux-gnu]\nrustflags = ["-C", "link-arg=-fuse-ld=mold"]\n# END cqlite-mold\n' "$MOLD_MARK" >"$ch1/config.toml"
mf1="$tmp/mold-real-linked.txt"
PATH="$mold_bin:$PATH" AGENT_GATE_SUMMARY_FILE="$mf1" \
  AGENT_GATE_TEST_OS=Linux CARGO_HOME="$ch1" RUSTFLAGS='' \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
assert_mold_token "mold-real-linked" "$mf1" linked

# 9e-ii. managed block in the extension-less `config` file -> linked (both names read).
ch2=$(mktemp -d "$tmp/mold-ch2.XXXXXX")
printf '%s\n# END cqlite-mold\n' "$MOLD_MARK" >"$ch2/config"
mf2="$tmp/mold-real-legacy.txt"
PATH="$mold_bin:$PATH" AGENT_GATE_SUMMARY_FILE="$mf2" \
  AGENT_GATE_TEST_OS=Linux CARGO_HOME="$ch2" RUSTFLAGS='' \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
assert_mold_token "mold-real-legacy-config" "$mf2" linked

# 9e-iii. mold on PATH, NO managed block -> present-unconfigured (real detection).
ch3=$(mktemp -d "$tmp/mold-ch3.XXXXXX")
mf3="$tmp/mold-real-unconf.txt"
PATH="$mold_bin:$PATH" AGENT_GATE_SUMMARY_FILE="$mf3" \
  AGENT_GATE_TEST_OS=Linux CARGO_HOME="$ch3" RUSTFLAGS='' \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
assert_mold_token "mold-real-unconfigured" "$mf3" present-unconfigured

# 9e-iv. managed block active BUT a non-empty RUSTFLAGS exported -> overridden.
mf4="$tmp/mold-real-overridden.txt"
PATH="$mold_bin:$PATH" AGENT_GATE_SUMMARY_FILE="$mf4" \
  AGENT_GATE_TEST_OS=Linux CARGO_HOME="$ch1" RUSTFLAGS='-C target-cpu=native' \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
assert_mold_token "mold-real-overridden" "$mf4" overridden

# 9e-iv-b. CARGO_ENCODED_RUSTFLAGS (higher precedence than RUSTFLAGS, same
#          suppression) also -> overridden, even with RUSTFLAGS empty.
mf4b="$tmp/mold-real-overridden-encoded.txt"
PATH="$mold_bin:$PATH" AGENT_GATE_SUMMARY_FILE="$mf4b" \
  AGENT_GATE_TEST_OS=Linux CARGO_HOME="$ch1" RUSTFLAGS='' \
  CARGO_ENCODED_RUSTFLAGS=$'-C\x1ftarget-cpu=native' \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
assert_mold_token "mold-real-overridden-encoded" "$mf4b" overridden

# 9e-vi. BOTH config files present, block ONLY in the ignored config.toml (cargo reads
#        the extension-less `config`) -> present-unconfigured, proving the detector
#        probes the EFFECTIVE file, not either-of-both.
ch6=$(mktemp -d "$tmp/mold-ch6.XXXXXX")
printf '[net]\nretry = 1\n' >"$ch6/config"
printf '%s\n# END cqlite-mold\n' "$MOLD_MARK" >"$ch6/config.toml"
mf6="$tmp/mold-real-bothfiles.txt"
PATH="$mold_bin:$PATH" AGENT_GATE_SUMMARY_FILE="$mf6" \
  AGENT_GATE_TEST_OS=Linux CARGO_HOME="$ch6" RUSTFLAGS='' \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
assert_mold_token "mold-real-both-files-precedence" "$mf6" present-unconfigured

# 9e-v. marker alignment: a user's own `# BEGIN cqlite-mold-notours` comment (a
#       PREFIX of, but not equal to, the managed marker) must NOT be detected as the
#       block -> present-unconfigured, proving exact-full-line matching.
ch5=$(mktemp -d "$tmp/mold-ch5.XXXXXX")
printf '# BEGIN cqlite-mold-notours my own note\n[build]\njobs = 2\n' >"$ch5/config.toml"
mf5="$tmp/mold-real-notours.txt"
PATH="$mold_bin:$PATH" AGENT_GATE_SUMMARY_FILE="$mf5" \
  AGENT_GATE_TEST_OS=Linux CARGO_HOME="$ch5" RUSTFLAGS='' \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
assert_mold_token "mold-real-marker-alignment" "$mf5" present-unconfigured

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
if body_mentions "$rl_body" 'aggregate_lite_components'; then
  ok "2121-structural: run_lite invokes aggregate_lite_components (lite OVERALL aggregation single-sourced)"
else
  bad "2121-structural: run_lite no longer calls aggregate_lite_components — lite OVERALL aggregation lost"
fi

# 14. NESTED-GATE SUMMARY CLOBBER (#2751): the tooling-tests component runs
#     self-test scripts that recursively invoke agent-gate.sh (the --delta
#     self-test's temp-repo `--delta` runs, this script's `--emit-summary-selftest`
#     runs). If a nested gate INHERITS the parent gate's AGENT_GATE_SUMMARY_FILE it
#     overwrites the parent's summary file mid-run with a foreign verdict (field
#     impact: #2672 read a foreign DELTA REFUSED block; #2600's full gate died in
#     tooling-tests leaving an INCOMPLETE placeholder with a foreign run-id, costing
#     a 57-min re-run). run_tooling_tests must scrub AGENT_GATE_SUMMARY_FILE so no
#     child can inherit it. Two halves prove the fix and make it un-removable:
#       14a structural — run_tooling_tests applies the scrub (FAILs if removed);
#       14b behavioral — the scrub mechanism actually prevents the clobber, with a
#           negative control proving the clobber is real (so 14b cannot pass vacuously).

# 14a. STRUCTURAL (property, not location): the gate must scrub
#      AGENT_GATE_SUMMARY_FILE from the environment exactly ONCE, AFTER the summary
#      path is resolved into the parent's own var and BEFORE any component runs — so
#      no child (present or future) can inherit the path. We assert the property by
#      line ordering rather than pinning to a single component: the (non-comment)
#      scrub line exists, sits AFTER the `case "$SUMMARY_FILE"` resolution, and
#      BEFORE the component runner (`run_component() {`) is even defined. FAILs if
#      the scrub line is deleted.
# Match the primary env scrub verb (`export -n`/`env -u`) that scrubs the path for
# ALL children, optionally wrapped in an `if ! …; then` visible-fallback guard. The
# `^[[:space:]]*` anchor already excludes comment lines, so no extra comment filter is
# needed (a `: #` trailing comment must NOT false-FAIL it). The `unset` fallback line
# is deliberately NOT matched — deleting the primary scrub must still FAIL this test.
scrub_ln=$(grep -nE '^[[:space:]]*(if[[:space:]]+![[:space:]]*)?(export -n|env -u) AGENT_GATE_SUMMARY_FILE' "$GATE" \
             | head -1 | cut -d: -f1)
resolve_ln=$(grep -n '^case "\$SUMMARY_FILE" in' "$GATE" | head -1 | cut -d: -f1)
dispatch_ln=$(grep -n '^run_component() {' "$GATE" | head -1 | cut -d: -f1)
if [ -z "$resolve_ln" ]; then
  # The resolution anchor is the load-bearing "after" boundary; if it moved the
  # property is un-checkable — hard FAIL with a DISTINCT message (not "scrub missing").
  bad "2751-structural: summary-resolution anchor ('case \"\$SUMMARY_FILE\" in') not found — test anchor moved, cannot verify scrub ordering"
elif [ -z "$scrub_ln" ]; then
  bad "2751-structural: no AGENT_GATE_SUMMARY_FILE scrub line in the gate — nested gates can clobber the parent summary (#2751 regression)"
elif [ -z "$dispatch_ln" ]; then
  # Degrade gracefully: the "before dispatch" upper bound moved, but we can still
  # assert the essential property (scrub AFTER resolution). Warn, don't false-FAIL.
  if [ "$scrub_ln" -gt "$resolve_ln" ]; then
    ok "2751-structural: gate scrubs AGENT_GATE_SUMMARY_FILE after summary resolution (line $scrub_ln); NOTE: dispatch anchor 'run_component() {' moved — upper-bound check skipped"
  else
    bad "2751-structural: scrub line ($scrub_ln) is NOT after summary resolution ($resolve_ln) — scrub happens too early to cover the resolved path"
  fi
elif [ "$scrub_ln" -gt "$resolve_ln" ] && [ "$scrub_ln" -lt "$dispatch_ln" ]; then
  ok "2751-structural: gate scrubs AGENT_GATE_SUMMARY_FILE after summary resolution and before component dispatch (line $scrub_ln)"
else
  bad "2751-structural: scrub line out of the resolved-but-pre-dispatch region (scrub=$scrub_ln resolve=$resolve_ln dispatch=$dispatch_ln) — nested gates can clobber the parent summary"
fi

# 14b. BEHAVIORAL: copy the gate into a bare temp dir (hermetic — --emit-summary-selftest
#      needs no cargo/git and writes to its OWN repo-root default when the path is unset).
tt_repo="$tmp/2751-scrub-repo"
mkdir -p "$tt_repo/scripts"
cp "$GATE" "$tt_repo/scripts/agent-gate.sh"
SENTINEL="PARENT-OWNED-SUMMARY-2751-DO-NOT-CLOBBER"

# Negative control: a nested gate that INHERITS an exported AGENT_GATE_SUMMARY_FILE
# overwrites it. This makes the positive assertion meaningful (proves the clobber is
# real and reachable, so 14b cannot pass just because nothing wrote anywhere). We
# assert BOTH that the sentinel is gone AND that a real gate summary marker now
# occupies the file — so a "file vanished/emptied" outcome cannot pass the control.
clob="$tmp/2751-parent-clobber.txt"
printf '%s\n' "$SENTINEL" >"$clob"
( export AGENT_GATE_SUMMARY_FILE="$clob"; cd "$tt_repo" \
    && bash scripts/agent-gate.sh --emit-summary-selftest ) >/dev/null 2>&1
if ! grep -q "$SENTINEL" "$clob" 2>/dev/null \
   && grep -q "$END_MARKER" "$clob" 2>/dev/null; then
  ok "2751-clobber-control: a nested gate with an inherited summary path OVERWRITES it with a gate summary (clobber is real)"
else
  bad "2751-clobber-control: the inherited path was not overwritten with a gate summary — behavioral control invalid"
  echo "------- on disk -------"; cat "$clob" 2>/dev/null; echo "-----------------------"
fi

# Positive: with the scrub (env -u, exactly as the gate applies it after summary
# resolution) the parent's chosen path is untouched, and the scrubbed child writes
# its OWN default. Remove any prior default first so the "child wrote its own
# default" assertion can never pass on a stale file (from the negative control or a
# future case) — it must be (re)created by THIS invocation.
safe="$tmp/2751-parent-safe.txt"
printf '%s\n' "$SENTINEL" >"$safe"
rm -f "$tt_repo"/.agent-gate-*summary.txt  # widen: covers lite/delta default siblings too
# #2874: also scrub AGENT_GATE_PARENT_RUN_ID so this case tests the #2751 env-scrub
# in ISOLATION. When this self-test itself runs INSIDE the gate (the tooling-tests
# component), the enclosing gate exports AGENT_GATE_PARENT_RUN_ID; without this scrub
# the child would be (correctly, per #2874) detected as NESTED and redirect its
# summary to its own private log dir instead of the repo-root default this case
# asserts. Neutralizing the #2874 marker keeps the two mechanisms orthogonal — the
# nested-redirect behavior has its own regression test (test_agent_gate_nested_isolation.sh).
( export AGENT_GATE_SUMMARY_FILE="$safe"; cd "$tt_repo" \
    && env -u AGENT_GATE_SUMMARY_FILE -u AGENT_GATE_PARENT_RUN_ID \
       bash scripts/agent-gate.sh --emit-summary-selftest ) >/dev/null 2>&1
if grep -q "$SENTINEL" "$safe" 2>/dev/null; then
  ok "2751-scrub-prevents-clobber: env -u AGENT_GATE_SUMMARY_FILE leaves the parent's summary file intact"
else
  bad "2751-scrub-prevents-clobber: the parent's summary file was clobbered despite the scrub"
  echo "------- on disk -------"; cat "$safe" 2>/dev/null; echo "-----------------------"
fi
# The scrubbed child must still have emitted a COMPLETED summary — at its OWN
# repo-root default, (re)created by this run — proving it ran fully and simply wrote
# elsewhere, not that it no-op'd or left only the startup INCOMPLETE sentinel.
child_default="$tt_repo/.agent-gate-summary.txt"
if [ -f "$child_default" ] \
   && grep -q "$END_MARKER" "$child_default" 2>/dev/null \
   && grep -q "^RESULT: " "$child_default" 2>/dev/null \
   && ! grep -q "RESULT: INCOMPLETE" "$child_default" 2>/dev/null \
   && ! grep -q "$SENTINEL" "$child_default" 2>/dev/null; then
  ok "2751-scrub-prevents-clobber: the scrubbed child wrote a COMPLETED summary at its own repo-root default instead"
else
  bad "2751-scrub-prevents-clobber: the scrubbed child's own-default summary is missing/incomplete"
  echo "------- on disk -------"; cat "$child_default" 2>/dev/null; echo "-----------------------"
fi

# --- 15. #2926: tree provenance lines are part of the block, and they do NOT ---------
#         disturb the RESULT poll predicates.
# The block grew three lines (`tree-start:`, `tree-end:`, `tree-integrity:`; a fourth,
# `tree-hash-cap:`, only when the untracked-hash cap is engaged). Two properties are
# pinned HERE, next to the rest of the block contract — the guard's own behaviour lives
# in scripts/tests/test_agent_gate_tree_integrity.sh:
#   a. the three lines are present in the canonical emission path;
#   b. NO added line contains the token `RESULT:` — so BOTH the buggy poll predicate
#      (a bare-token match) and the corrected one (`grep -qE 'RESULT: (PASS|FAIL)'`)
#      behave EXACTLY as they did before this change (#2908 is neither fixed nor
#      regressed here). Asserting "exactly one RESULT: token" is what makes (b) a real
#      guard rather than a restatement of (a).
tree_sum="$tmp/2926-tree-lines.txt"
env AGENT_GATE_SUMMARY_FILE="$tree_sum" bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
tree_missing=()
grep -q '^tree-start: '     "$tree_sum" 2>/dev/null || tree_missing+=("tree-start")
grep -q '^tree-end: '       "$tree_sum" 2>/dev/null || tree_missing+=("tree-end")
grep -q '^tree-integrity: ' "$tree_sum" 2>/dev/null || tree_missing+=("tree-integrity")
if [ "${#tree_missing[@]}" -eq 0 ]; then
  ok "2926-tree-lines: the SUMMARY block carries tree-start / tree-end / tree-integrity"
else
  bad "2926-tree-lines: missing ${tree_missing[*]}"
  echo "------- block -------"; cat "$tree_sum" 2>/dev/null; echo "---------------------"
fi
n_result=$(grep -c 'RESULT:' "$tree_sum" 2>/dev/null)
if [ "$n_result" = 1 ] && grep -qE '^RESULT: (PASS|FAIL)' "$tree_sum" 2>/dev/null; then
  ok "2926-poll-predicates: exactly ONE 'RESULT:' token — both poll predicates behave as before (#2908 untouched)"
else
  bad "2926-poll-predicates: found $n_result 'RESULT:' tokens — an added line embeds the token"
  grep -n 'RESULT:' "$tree_sum" 2>/dev/null
fi

# --- 16. #1699: the feature-matrix lanes are REGISTERED, in all three places that ----
#         must agree, and NOT in the fast-loop sets.
# The four lanes (flight-tests, legacy-heuristics, feature-iso-parquet,
# feature-iso-delta-scan) exist because the full gate previously only COMPILED
# cqlite-flight and legacy-heuristics, and only ever built parquet/delta-scan TOGETHER.
# A component is declared in three places that must agree — the COMPONENTS array, the
# dispatch_component case, and (for the two that need fixtures) DATASET_COMPONENTS — so
# an edit that drops a lane from ONE of them silently shrinks the gate of record while
# every run stays green. That is the failure this case exists to red, cheaply: it drives
# `--list` (the real array) and reads the dispatch case AND DATASET_COMPONENTS out of the
# real script. No
# cargo, no git, no network, sub-second — it must stay affordable wherever it runs.
#
# The expensive half of the #3272 observed-to-fire standard (does each lane actually
# FAIL on a planted break?) is a separate opt-in harness; this is deliberately only the
# cheap structural half.
FEATURE_MATRIX_LANES="flight-tests legacy-heuristics feature-iso-parquet feature-iso-delta-scan"

# `--list` prints exactly "${COMPONENTS[@]}", so it IS the array — asserting against it
# beats grepping the source (it cannot pass on a name that is commented out or sitting
# in an unrelated string). Stderr is dropped: the gate announces sccache/nextest there.
lanes_list="$tmp/1699-list.txt"
if bash "$GATE" --list >"$lanes_list" 2>/dev/null; then
  ok "1699-list: scripts/agent-gate.sh --list exits 0"
else
  bad "1699-list: scripts/agent-gate.sh --list failed to run"
fi

# The dispatch case arms, read out of the REAL script: every top-level arm of
# dispatch_component() is a 4-space-indented `<name>)`. Extracting them (rather than
# grepping the whole file for the name) is what makes this an assert about
# REACHABILITY: a name in COMPONENTS with no arm falls through to the `unknown
# component` branch and returns 2, which no other case here would catch.
dispatch_arms="$tmp/1699-dispatch-arms.txt"
awk '/^dispatch_component\(\) \{/,/^\}/' "$GATE" \
  | sed -nE 's/^    ([a-z0-9][a-z0-9-]*)\).*/\1/p' > "$dispatch_arms"
if [ -s "$dispatch_arms" ]; then
  ok "1699-dispatch: extracted $(wc -l < "$dispatch_arms" | tr -d ' ') dispatch_component case arms"
else
  bad "1699-dispatch: extracted ZERO dispatch_component case arms — the extraction itself broke, so every reachability assert below would pass vacuously"
fi

for lane in $FEATURE_MATRIX_LANES; do
  if grep -qxF "$lane" "$lanes_list" 2>/dev/null; then
    ok "1699-registered: $lane is in COMPONENTS (printed by --list)"
  else
    bad "1699-registered: $lane is NOT printed by --list — dropped from the COMPONENTS array"
  fi
  if grep -qxF "$lane" "$dispatch_arms" 2>/dev/null; then
    ok "1699-dispatch: $lane is reachable in dispatch_component"
  else
    bad "1699-dispatch: $lane has NO dispatch_component arm — it would hit 'unknown component' and return 2"
  fi
done

# DATASET_COMPONENTS, the THIRD registry (roborev round-2 finding 3). It is what makes
# a component participate in the #2078 fetched-corpus preflight, so a lane dropped from
# it stops being fixture-guarded while every run stays green — the same silent-shrink
# shape as a missing dispatch arm, in the one place the two asserts above cannot see.
# Both DIRECTIONS are asserted, which is what keeps the registry honest: the two
# EXECUTING lanes (flight-tests, legacy-heuristics) run real dataset-dependent tests and
# MUST be present; the two ISOLATION lanes compile only (`cargo test --lib --no-run`),
# need no fixtures at all, and MUST be absent — adding them would make a fixture-less
# checkout FAIL-CLOSED on lanes that never open a Data.db.
#
# Read out of the REAL script as a single space-delimited assignment; an extraction that
# comes back empty is a FAILURE of the extraction, never a pass, because every
# membership assert below would then be vacuous.
dataset_components=$(sed -nE 's/^DATASET_COMPONENTS="([^"]*)".*/\1/p' "$GATE" | head -1)
if [ -n "$dataset_components" ]; then
  ok "1699-dataset-extract: extracted DATASET_COMPONENTS from the real script ($(printf '%s' "$dataset_components" | wc -w | tr -d ' ') entries)"
else
  bad "1699-dataset-extract: could NOT extract DATASET_COMPONENTS — the extraction itself broke, so every membership assert below would pass vacuously"
fi
for lane in flight-tests legacy-heuristics; do
  case " $dataset_components " in
    *" $lane "*)
      ok "1699-dataset-present: $lane is in DATASET_COMPONENTS (so the #2078 missing-fixtures preflight covers it)" ;;
    *)
      bad "1699-dataset-present: $lane is NOT in DATASET_COMPONENTS — it runs dataset-dependent tests, so missing fixtures would bypass the preflight and it would fail obscurely instead" ;;
  esac
done
for lane in feature-iso-parquet feature-iso-delta-scan; do
  case " $dataset_components " in
    *" $lane "*)
      bad "1699-dataset-absent: $lane is in DATASET_COMPONENTS — it is a compile-only isolation lane (--lib --no-run) that opens no fixture, so enrolling it makes a fixture-less checkout fail-closed for no reason" ;;
    *)
      ok "1699-dataset-absent: $lane is correctly NOT in DATASET_COMPONENTS (compile-only, needs no corpus)" ;;
  esac
done

# The fast-loop sets must NOT inherit these lanes: they are full-gate components, and
# --lite's whole value is that it stays 1-5 min. `--lite-list` prints LITE_COMPONENTS.
lite_list="$tmp/1699-lite-list.txt"
# The exit status is CHECKED, and the output must be non-empty, before the absence of a
# lane name is read as evidence (C re-audit, P2 — its sibling 1699-list above already did
# this). A failed invocation leaves the file empty, and an empty file contains no lane, so
# the "nothing leaked" branch below would report OK having measured nothing: the vacuous
# pass this whole issue exists to eliminate, inside this issue's own new assert.
lite_rc=0
bash "$GATE" --lite-list >"$lite_list" 2>/dev/null || lite_rc=$?
lite_n=$(grep -c . "$lite_list" 2>/dev/null || true)
leaked=""
for lane in $FEATURE_MATRIX_LANES; do
  grep -qxF "$lane" "$lite_list" 2>/dev/null && leaked="$leaked $lane"
done
if [ "$lite_rc" -ne 0 ] || [ "${lite_n:-0}" -eq 0 ]; then
  bad "1699-lite-unchanged: \`--lite-list\` did not produce a readable component list (rc=$lite_rc, lines=${lite_n:-0}) — the leak check has no subject, so its PASS would mean nothing"
elif [ -z "$leaked" ]; then
  ok "1699-lite-unchanged: no feature-matrix lane leaked into LITE_COMPONENTS (${lite_n} lite component(s) read)"
else
  bad "1699-lite-unchanged: LITE_COMPONENTS gained$leaked — --lite is the fast loop, not the gate of record"
fi

# --- 17. #1699: `-D warnings` in the new lanes cannot be silently switched off -------
#
# roborev round-5 finding (Medium). `env RUSTFLAGS="-D warnings" cargo …` is IGNORED
# whenever CARGO_ENCODED_RUSTFLAGS is present, because cargo reads the encoded variable
# first — so a lane whose entire warning-class guard rides on RUSTFLAGS enforces NOTHING
# in such an environment, while its SUMMARY line stays green. MEASURED, not argued: with
# `CARGO_ENCODED_RUSTFLAGS=""` set, a crate with an unused variable built rc=0 under the
# old form and rc=101 through `_deny_warnings`. Even an EMPTY encoded value suppresses
# it, which is the quietest possible route to a vacuous guard.
#
# These are STRUCTURAL asserts, deliberately. A behavioural one would need a real cargo
# build (the measurement above), which does not belong in a sub-second self-test; and the
# regression to guard is textual anyway — somebody reintroducing `env RUSTFLAGS=` on one
# of these invocations because it reads as equivalent.
# `grep -c` + numeric test, not `| grep -q`: under pipefail an early-exiting `grep -q`
# SIGPIPEs `awk` and the pipeline reports 141 on a successful match, which would red this
# assert intermittently. A flaky assert is what teaches people to re-run until green.
if [ "$(awk '/^_deny_warnings\(\) \{/,/^\}/' "$GATE" | grep -c 'CARGO_ENCODED_RUSTFLAGS')" -gt 0 ]; then
  ok "1699-denywarn-helper: _deny_warnings exists and accounts for CARGO_ENCODED_RUSTFLAGS"
else
  bad "1699-denywarn-helper: _deny_warnings is missing or ignores CARGO_ENCODED_RUSTFLAGS — RUSTFLAGS alone is silently inert when the encoded form is set"
fi

# Both branches must be present: APPEND when the operator set flags (dropping them would
# trade one silent behaviour change for another), and UNSET when they did not (an
# empty-but-set value still counts as present to cargo).
dw_body="$tmp/1699-denywarn-body.txt"
awk '/^_deny_warnings\(\) \{/,/^\}/' "$GATE" > "$dw_body"
if grep -q 'env -u CARGO_ENCODED_RUSTFLAGS' "$dw_body"; then
  ok "1699-denywarn-unset: the plain branch UNSETS CARGO_ENCODED_RUSTFLAGS (an empty-but-set value would suppress RUSTFLAGS)"
else
  bad "1699-denywarn-unset: the plain branch does not unset CARGO_ENCODED_RUSTFLAGS — an empty-but-set value silently suppresses -D warnings"
fi
if grep -q '\${CARGO_ENCODED_RUSTFLAGS}' "$dw_body"; then
  ok "1699-denywarn-append: the encoded branch APPENDS to the operator's flags rather than discarding them"
else
  bad "1699-denywarn-append: the encoded branch does not preserve the operator's existing flags"
fi

# The four lanes' cargo invocations must go THROUGH the helper. This is the assert that
# actually stops the regression: the helper being correct is worthless if a lane bypasses
# it. Scoped to the two lane functions so the pre-existing clippy component (which has
# the same latent exposure, filed separately) does not make this assert fail for
# something outside this change.
for fn_ in run_legacy_heuristics run_feature_iso; do
  body_="$tmp/1699-lanefn-$fn_.txt"
  awk -v f="^$fn_\\\\(\\\\) \\\\{" '$0 ~ f, /^\}/' "$GATE" > "$body_"
  if [ ! -s "$body_" ]; then
    bad "1699-denywarn-scope: could not extract $fn_ from the gate — the extraction itself broke, so the asserts below would pass vacuously"
    continue
  fi
  if grep -q '_deny_warnings' "$body_"; then
    ok "1699-denywarn-use: $fn_ compiles through _deny_warnings"
  else
    bad "1699-denywarn-use: $fn_ does not use _deny_warnings — its -D warnings guard is inert under CARGO_ENCODED_RUSTFLAGS"
  fi
  # The oracle must read CODE, not PROSE. First cut matched the lane's own `echo ">>>
  # RUSTFLAGS=-D warnings cargo build …"` progress line and its explanatory comments, and
  # FAILED a correct lane — the #3312 shape (a decision made from a stream carrying both
  # control tokens and someone else's payload), here in the false-FAIL direction. So
  # comments and echoed strings are removed before the match, and the assignment must sit
  # in COMMAND POSITION (line start or after `env`/`&&`/`||`/`;`/`|`/`(`).
  code_="$tmp/1699-lanefn-$fn_-code.txt"
  sed -e 's/[[:space:]]*#.*$//' -e '/^[[:space:]]*echo[[:space:]]/d' "$body_" > "$code_"
  if grep -qE '(^|[;&|(][[:space:]]*|[[:space:]]env[[:space:]]+)RUSTFLAGS=' "$code_"; then
    bad "1699-denywarn-bare: $fn_ sets RUSTFLAGS directly in command position — that form is silently ignored when CARGO_ENCODED_RUSTFLAGS is set; route it through _deny_warnings"
  else
    ok "1699-denywarn-bare: $fn_ sets no bare RUSTFLAGS in command position"
  fi
done

# --- 18. #1699: the co-required-feature census reports cfg SITES (descoped, round 10) ---
#
# This header used to read "counts TEST BODIES, not cfg sites" — the exact claim round 10 DESCOPED and
# §24 now forbids (`1699-r10-no-classification`). A stale header in the file that pins the descope is a
# small instance of this issue's own defect, so it is corrected rather than left as harmless prose.
#
# roborev round-5 finding (Low). The first census grepped a single-line, fixed-ORDER cfg
# pattern and counted MATCHING ATTRIBUTES, so a gated `use` import was reported as an
# omitted test body (3 claimed, 2 real) while a reordered or multi-line cfg was missed
# entirely. This lane's whole deliverable is an accurate declaration of what it does not
# run, so a census that miscounts its own gap is the defect — and both error directions
# matter, over- and under-report.
#
# Asserted BEHAVIOURALLY against fixtures, because the shapes are exactly what a textual
# assert would miss. The helper is extracted rather than sourced: agent-gate.sh dispatches
# when sourced.
coreq_h="$tmp/1699-coreq-helper.sh"
awk '/^_legacy_coreq_sites\(\) \{/,/^\}/' "$GATE" > "$coreq_h"
if [ -s "$coreq_h" ]; then
  ok "1699-coreq-extract: extracted _legacy_coreq_sites from the real script"
  # shellcheck disable=SC1090
  . "$coreq_h"
  coreq_fx="$tmp/1699-coreq-fixture.rs"
  cat > "$coreq_fx" <<'RSFX'
#[cfg(all(feature = "legacy-heuristics", feature = "absent-one"))]
use some::Import;

#[cfg(all(feature = "absent-one", feature = "legacy-heuristics"))]
#[test]
fn reordered_is_found() {}

#[cfg(all(
    feature = "legacy-heuristics",
    feature = "absent-two"
))]
#[tokio::test]
async fn multiline_is_found() {}

#[cfg(all(feature = "legacy-heuristics", not(feature = "absent-one")))]
#[test]
fn negated_is_not_guessed() {}

#[cfg(all(feature = "legacy-heuristics", feature = "present-one"))]
#[test]
fn enabled_coreq_is_no_gap() {}
RSFX
  coreq_out="$tmp/1699-coreq-out.txt"
  _legacy_coreq_sites "$coreq_fx" " default legacy-heuristics present-one " > "$coreq_out" 2>/dev/null
  n_site=$(awk -F'\t' '$1=="site"' "$coreq_out" | wc -l | tr -d ' ')
  n_skip=$(awk -F'\t' '$1=="skip"' "$coreq_out" | wc -l | tr -d ' ')
  n_all=$(wc -l < "$coreq_out" | tr -d ' ')
  # 2 gated TEST fns (reordered + multiline), 1 non-test item (the gated import),
  # 1 negated site reported as unclassified, and the enabled co-req is NOT a gap at all.
  # SITES, not bodies: the classifier was descoped in round 10 (see _legacy_coreq_sites).
  # The fixture holds 3 co-required sites — a gated import, a REORDERED cfg and a
  # MULTI-LINE cfg — so this still pins both parse shapes without claiming a body count.
  if [ "$n_site" = "3" ]; then
    ok "1699-coreq-sites: all 3 co-required sites found (reordered AND multi-line cfg both parsed)"
  else
    bad "1699-coreq-sites: expected 3 co-required sites, got $n_site — a reordered or multi-line cfg is being missed (under-report)"
  fi
  if [ "$n_skip" = "1" ]; then
    ok "1699-coreq-negated: a negated co-required cfg is reported as unclassified, never guessed"
  else
    bad "1699-coreq-negated: expected 1 unclassified negated site, got $n_skip — not(feature=...) means the body compiles when the feature is OFF, the opposite of a gap"
  fi
  if [ "$n_all" = "4" ]; then
    ok "1699-coreq-enabled: an ENABLED co-required feature produces no record (it is not a gap)"
  else
    bad "1699-coreq-enabled: expected 4 records total, got $n_all — an enabled co-required feature is being reported as a gap"
  fi
else
  bad "1699-coreq-extract: could NOT extract _legacy_coreq_sites — the extraction itself broke, so every census assert would pass vacuously"
fi

# --- 19. #1699: the derived lanes must not decide anything via `| grep -q` -----------
#
# `scripts/agent-gate.sh` runs under `set -uo pipefail`. An early-exiting consumer
# (`grep -q`, `grep -m`, `head`) closes the pipe as soon as it is satisfied, so the
# upstream dies of SIGPIPE and the PIPELINE's status is 141 — NON-ZERO ON A SUCCESSFUL
# MATCH. Any `if`/`if !` reading that status therefore gets the wrong answer, and the
# wrongness is TIMING-DEPENDENT: it appears only when the upstream still has output
# buffered when the consumer exits, which is why it reads as an intermittent flake.
#
# This is not hypothetical for this lane. The allowed-zero derivation used
# `! sed … | grep -qE`, so a test file WITH a surviving positive cfg site was classified
# as negative-polarity-only and EXCUSED from the #2039 zero-tests guard — a gated target
# executing nothing would not have failed the lane. MEASURED at 6/6 mis-classifications
# with a match on line 1 of a 200k-line input, 0/6 after the fix. Same defect class as
# #3380, whose guard-test assert this shape makes non-deterministic in BOTH directions
# (a vacuous PASS when SIGPIPE wins, a fire when it does not).
#
# Scoped to the #1699 lane functions: the pattern is pervasive in this repo (~696 sites
# in scripts/) and auditing all of it is #3380's neighbourhood, not this issue's.
for fn_ in run_legacy_heuristics run_feature_iso run_flight_tests; do
  body_="$tmp/1699-pipefail-$fn_.txt"
  awk -v f="^$fn_\\\\(\\\\) \\\\{" '$0 ~ f, /^\}/' "$GATE" > "$body_"
  if [ ! -s "$body_" ]; then
    bad "1699-pipefail-scope: could not extract $fn_ from the gate — the extraction broke, so this assert would pass vacuously"
    continue
  fi
  # Comments are stripped first: the explanation above deliberately NAMES the forbidden
  # shape, and an oracle that reads its own rationale as a violation is the #3312
  # control/data-channel defect (it already false-FAILED once in this issue).
  code_="$tmp/1699-pipefail-$fn_-code.txt"
  sed -e 's/[[:space:]]*#.*$//' "$body_" > "$code_"
  if [ "$(grep -cE '\|[[:space:]]*(grep[[:space:]]+-[a-zA-Z]*[qm]|head([[:space:]]|$))' "$code_")" -gt 0 ]; then
    bad "1699-pipefail: $fn_ pipes into an early-exiting consumer (grep -q/-m or head) — under pipefail that reports 141 on a SUCCESSFUL match, so the branch reading it is wrong intermittently; use grep -c plus a numeric test"
  else
    ok "1699-pipefail: $fn_ makes no decision through a pipeline into an early-exiting consumer"
  fi
done

# --- 20. #1699: the feature oracle is PACKAGE-scoped, not workspace-scoped -----------
#
# roborev round-6 finding (Medium). `cargo metadata` resolves the WHOLE workspace and
# unions features across every member, so cqlite-core came back with `arrow`,
# `arrow-shape-corpus`, `cli-helpers`, `parquet` and `producer-fault-injection` enabled —
# five features turned on only by cqlite-flight / cqlite-py / cqlite-node /
# ws0-corpus-gen, which `cargo test -p cqlite-core` does not build. Measured 14 features
# workspace-wide vs 9 package-scoped, and none of the five is a dev-dependency of
# cqlite-core.
#
# The direction is what makes it a defect: the only consumer is the co-required-feature
# census, which reports GAPS, so an over-broad enabled set makes a real gap look reachable
# and silently DROPS it — an under-report in the output whose whole job is to state
# omissions.
rpf_body="$tmp/1699-rpf.txt"
awk '/^_resolved_package_features\(\) \{/,/^\}/' "$GATE" > "$rpf_body"
if [ ! -s "$rpf_body" ]; then
  bad "1699-featoracle-extract: could not extract _resolved_package_features — the extraction broke, so the asserts below would pass vacuously"
else
  ok "1699-featoracle-extract: extracted _resolved_package_features from the real script"
  rpf_code="$tmp/1699-rpf-code.txt"
  sed 's/[[:space:]]*#.*$//' "$rpf_body" > "$rpf_code"
  if [ "$(grep -cE 'cargo[[:space:]]+tree[[:space:]]+-p' "$rpf_code")" -gt 0 ]; then
    ok "1699-featoracle-scoped: the oracle resolves with a package-scoped 'cargo tree -p'"
  else
    bad "1699-featoracle-scoped: the oracle no longer uses 'cargo tree -p' — a workspace-wide resolve unions OTHER members' features and silently under-reports census gaps"
  fi
  if [ "$(grep -cE 'cargo[[:space:]]+metadata' "$rpf_code")" -eq 0 ]; then
    ok "1699-featoracle-nometa: the oracle makes no workspace-wide 'cargo metadata' feature resolve"
  else
    bad "1699-featoracle-nometa: the oracle is back on 'cargo metadata' — that resolves the whole workspace and reports other members' features as this package's"
  fi
  # Dev edges must stay requested: dev-dependency unification IS applied by `cargo test`,
  # so dropping them would bias the set the other way (over-reporting gaps).
  if [ "$(grep -cE '\-e[[:space:]]+features[^[:space:]]*dev' "$rpf_code")" -gt 0 ]; then
    ok "1699-featoracle-dev: dev edges are included, so genuine dev-dependency unification is still counted"
  else
    bad "1699-featoracle-dev: dev edges are not requested — cargo test DOES apply dev-dependency unification, so the set would be understated"
  fi

  # BEHAVIOURAL: the regression roborev asked for — a feature enabled ONLY by a workspace
  # dependent must be ABSENT. Runs only where cargo can resolve; a missing/failing cargo
  # is reported as an explicit SKIP naming what went unverified, never folded into a pass.
  if command -v cargo >/dev/null 2>&1; then
    # shellcheck disable=SC1090
    . "$rpf_body"
    rpf_out=$(_resolved_package_features cqlite-core --features legacy-heuristics 2>/dev/null || true)
    if [ -z "$rpf_out" ]; then
      echo "SKIP - 1699-featoracle-behaviour: cargo present but the resolve returned nothing (offline registry?) — NOT verified here"
    else
      _leaked=""
      for _f in parquet cli-helpers arrow arrow-shape-corpus producer-fault-injection; do
        case "$rpf_out" in *" $_f "*) _leaked="$_leaked $_f" ;; esac
      done
      if [ -z "$_leaked" ]; then
        ok "1699-featoracle-behaviour: no dependent-only feature (parquet/cli-helpers/arrow/...) is reported as enabled for cqlite-core"
      else
        bad "1699-featoracle-behaviour: dependent-only features are reported as enabled —$_leaked. cargo test -p cqlite-core does not enable them, so census gaps requiring them would be silently dropped"
      fi
      # The complement, so the assert above cannot pass by the oracle returning junk: the
      # features the invocation really does enable MUST be present.
      _absent=""
      for _f in legacy-heuristics default write-support all-compression; do
        case "$rpf_out" in *" $_f "*) ;; *) _absent="$_absent $_f" ;; esac
      done
      if [ -z "$_absent" ]; then
        ok "1699-featoracle-complement: the features the invocation really enables ARE present (so the assert above is not passing on an empty/garbage set)"
      else
        bad "1699-featoracle-complement: genuinely-enabled features are MISSING —$_absent. An understated set over-reports census gaps"
      fi
    fi
  else
    echo "SKIP - 1699-featoracle-behaviour: cargo not available — the dependent-only-feature regression was NOT verified here"
  fi
fi

# --- 21. #1699: guard subjects are DERIVED, and unmodelled cfg shapes stay unclassified
#
# roborev round-7 findings. Three separate places where a guard's SUBJECT was narrower
# than its selector, which is the same "looks covered, isn't" shape as the whole issue:
#   (a) the flight zero-test guard took a hard-coded `src/lib.rs src/main.rs` while
#       `--bins` selects EVERY binary, so a new binary could run zero tests unnoticed;
#   (b) legacy target discovery globbed `tests/*.rs`, which cannot see a target gated only
#       by `required-features` in the manifest, nor a directory-style `tests/foo/main.rs`;
#   (c) the co-required census treated every token as conjunctive, so
#       `any(feature = "legacy-heuristics", feature = "experimental")` — REACHABLE in this
#       lane — was reported as compiled out.
lh_body="$tmp/1699-lhfn.txt"
awk '/^run_legacy_heuristics\(\) \{/,/^\}/' "$GATE" > "$lh_body"
ft_body="$tmp/1699-ftfn.txt"
awk '/^run_flight_tests\(\) \{/,/^\}/' "$GATE" > "$ft_body"
if [ ! -s "$lh_body" ] || [ ! -s "$ft_body" ]; then
  bad "1699-derived-extract: could not extract run_legacy_heuristics/run_flight_tests — extraction broke, so these asserts would pass vacuously"
else
  ok "1699-derived-extract: extracted both lane functions from the real script"
  lh_code="$tmp/1699-lhfn-code.txt"; sed 's/[[:space:]]*#.*$//' "$lh_body" > "$lh_code"
  ft_code="$tmp/1699-ftfn-code.txt"; sed 's/[[:space:]]*#.*$//' "$ft_body" > "$ft_code"

  if [ "$(grep -cE 'check_unittest_targets_ran[^|]*src/lib\.rs' "$ft_code")" -eq 0 ]; then
    ok "1699-derived-flightguard: the flight zero-test guard takes no hard-coded unittest path"
  else
    bad "1699-derived-flightguard: the flight zero-test guard is back on a hard-coded src/lib.rs — --bins selects every binary, so a new one could run zero tests with the guard reporting OK"
  fi
  if [ "$(grep -cE '_package_unittest_srcs' "$ft_code")" -gt 0 ]; then
    ok "1699-derived-flightsubj: the flight guard's subject set is derived from cargo metadata"
  else
    bad "1699-derived-flightsubj: the flight guard's subject set is no longer derived — a hard-coded list beside a wildcard selector drifts silently (#2039)"
  fi
  if [ "$(grep -cE '_package_test_targets_gated' "$lh_code")" -gt 0 ]; then
    ok "1699-derived-legacycand: legacy target candidates come from cargo metadata (sees manifest-gated + directory-style targets)"
  else
    bad "1699-derived-legacycand: legacy target discovery no longer enumerates cargo's test targets — a tests/*.rs glob omits manifest-gated and directory-style targets"
  fi
  if [ "$(grep -cE 'for[[:space:]]+f[[:space:]]+in[[:space:]]+"\$tests_dir"/\*\.rs' "$lh_code")" -eq 0 ]; then
    ok "1699-derived-noglob: membership is not decided by a bare tests/*.rs glob"
  else
    bad "1699-derived-noglob: the tests/*.rs glob is back as the candidate source"
  fi

  # (c) BEHAVIOURAL: any(...) and cfg_attr must come back as unclassified, not as gaps.
  if [ -s "$coreq_h" ]; then
    coreq_fx2="$tmp/1699-coreq-bool.rs"
    cat > "$coreq_fx2" <<'RSFX2'
#[cfg(any(feature = "legacy-heuristics", feature = "absent-one"))]
#[test]
fn any_is_reachable_here_not_a_gap() {}

#[cfg_attr(feature = "absent-one", ignore)]
#[cfg(all(feature = "legacy-heuristics", feature = "absent-one"))]
#[test]
fn cfg_attr_is_not_evaluated() {}

#[cfg(all(feature = "legacy-heuristics", feature = "absent-two"))]
#[test]
fn plain_conjunction_is_still_a_gap() {}
RSFX2
    coreq_out2="$tmp/1699-coreq-bool-out.txt"
    _legacy_coreq_sites "$coreq_fx2" " default legacy-heuristics " > "$coreq_out2" 2>/dev/null
    b_skip=$(awk -F'\t' '$1=="skip"' "$coreq_out2" | wc -l | tr -d ' ')
    b_site=$(awk -F'\t' '$1=="site"' "$coreq_out2" | wc -l | tr -d ' ')
    if [ "$b_skip" = "2" ]; then
      ok "1699-coreq-bool: any(...) and cfg_attr are reported UNCLASSIFIED, not as gaps"
    else
      bad "1699-coreq-bool: expected 2 unclassified sites (any + cfg_attr), got $b_skip — a token list cannot tell a conjunction from a disjunction, and any(...) is REACHABLE in this lane"
    fi
    if [ "$b_site" = "1" ]; then
      ok "1699-coreq-bool-complement: a plain all(...) conjunction is still reported as a site (the skip arm did not swallow everything)"
    else
      bad "1699-coreq-bool-complement: expected 1 genuine conjunctive site, got $b_site — the unclassified arm is over-matching and the census now under-reports"
    fi
  fi
fi

# --- 22. #1699: round-8 — both halves guarded, metadata threaded all the way through ---
#
# Three of round 8's four findings were UNDER-PROPAGATION of round 7's own fix: metadata
# discovery was threaded into the candidate loop and NOT into the allow-zero classifier,
# the census path resolution, or the `--lib` guard. These asserts pin each seam.
if [ -s "$lh_code" ]; then
  # (a) A manifest-gated target is POSITIVELY gated and can never be allowed-zero. Its
  #     source may carry no cfg site at all, so the polarity scan finds nothing and would
  #     have excused exactly the target round 7 added discovery for.
  if [ "$(grep -cE '_mt_how"?[[:space:]]*=[[:space:]]*"?manifest' "$lh_code")" -gt 0 ]; then
    ok "1699-r8-manifest-positive: a manifest-gated target is treated as positively gated (never auto allowed-zero)"
  else
    bad "1699-r8-manifest-positive: the manifest arm no longer bypasses the allow-zero scan — a required-features target has no cfg site, so it would be EXCUSED from the zero-tests guard"
  fi
  # (b) The census must scan cargo's real src_path, not a reconstructed tests/<name>.rs.
  if [ "$(grep -cE '_legacy_coreq_sites[[:space:]]+"\$f_src"' "$lh_code")" -gt 0 ]; then
    ok "1699-r8-census-srcpath: the census scans cargo's src_path, so directory-style and mapped targets are not skipped"
  else
    bad "1699-r8-census-srcpath: the census is reconstructing a source path again — a directory-style or explicitly-mapped [[test]] target does not live there and would be silently skipped"
  fi
  if [ "$(grep -cE 'tests_dir/\$f_\.rs' "$lh_code")" -eq 0 ]; then
    ok "1699-r8-census-noreconstruct: no reconstructed \$tests_dir/\$f_.rs path remains"
  else
    bad "1699-r8-census-noreconstruct: a reconstructed \$tests_dir/\$f_.rs path is back in the census"
  fi
  # (c) The `--lib` half needs its OWN guard: the integration guard keys on
  #     `Running tests/<name>.rs` and cannot see a zero-test lib suite.
  if [ "$(grep -cE 'check_unittest_targets_ran' "$lh_code")" -gt 0 ] \
     && [ "$(grep -cE 'check_no_unexpected_zero_tests' "$lh_code")" -gt 0 ]; then
    ok "1699-r8-both-halves: the legacy lane guards BOTH halves (integration targets AND the --lib unit suite)"
  else
    bad "1699-r8-both-halves: the legacy lane guards only one half — a zero-test --lib suite (or --lib dropped entirely) would leave the lane green on its integration targets alone"
  fi
fi

# (d) BEHAVIOURAL: stacked cfg attributes are ANDed by Rust, so they ARE a gap.
if [ -s "$coreq_h" ]; then
  stack_fx="$tmp/1699-coreq-stacked.rs"
  cat > "$stack_fx" <<'RSFX3'
#[cfg(feature = "legacy-heuristics")]
#[cfg(feature = "absent-one")]
#[test]
fn stacked_is_a_gap() {}

#[cfg(feature = "legacy-heuristics")]
#[test]
fn lh_only_is_not_a_gap() {}

#[cfg(feature = "absent-one")]
#[test]
fn absent_only_is_not_a_gap() {}

#[cfg(feature = "legacy-heuristics")]
#[cfg(any(feature = "absent-one", feature = "absent-two"))]
#[test]
fn stacked_with_any_is_unclassified() {}
RSFX3
  stack_out="$tmp/1699-coreq-stacked-out.txt"
  _legacy_coreq_sites "$stack_fx" " default legacy-heuristics " > "$stack_out" 2>/dev/null
  s_site=$(awk -F'\t' '$1=="site"' "$stack_out" | wc -l | tr -d ' ')
  s_skip=$(awk -F'\t' '$1=="skip"' "$stack_out" | wc -l | tr -d ' ')
  s_all=$(wc -l < "$stack_out" | tr -d ' ')
  if [ "$s_site" = "1" ]; then
    ok "1699-r8-stacked: stacked #[cfg] attributes are recognised as a gap (Rust ANDs them, so they equal the all(...) form)"
  else
    bad "1699-r8-stacked: expected 1 stacked site, got $s_site — a per-attribute scan sees legacy-heuristics with no co-requirement in one attribute and vice versa in the other, arms on neither, and reports a FALSE ZERO-GAP census"
  fi
  if [ "$s_skip" = "1" ]; then
    ok "1699-r8-stacked-any: a stacked cluster containing any(...) is unclassified, not guessed"
  else
    bad "1699-r8-stacked-any: expected 1 unclassified stacked cluster, got $s_skip"
  fi
  # The complement: the two single-token clusters must produce NOTHING, or cluster
  # accumulation is leaking across item boundaries and inventing gaps.
  if [ "$s_all" = "2" ]; then
    ok "1699-r8-stacked-complement: single-token clusters produce no record (accumulation does not leak across items)"
  else
    bad "1699-r8-stacked-complement: expected exactly 2 records, got $s_all — cluster state is leaking across item boundaries and inventing gaps"
  fi
fi

# --- 23. #1699: round-9 — identifier agreement, census subject covers --lib, honest labels
if [ -s "$lh_code" ]; then
  # (a) allowed-zero entries must be spelled the way the GUARD parses them. `--test <name>`
  #     takes cargo's TARGET NAME, but check_no_unexpected_zero_tests keys on the PATH stem
  #     from `Running tests/<path>.rs`. For a directory-style target those differ (`foo` vs
  #     `foo/main`), so a name-spelled allowance never matches and a legitimately
  #     negative-polarity target FAILS the full gate.
  if [ "$(grep -cE '_az_id' "$lh_code")" -gt 0 ]; then
    ok "1699-r9-azid: allowed-zero entries are derived from src_path, so they match the identifier the zero-test guard parses"
  else
    bad "1699-r9-azid: allowed-zero is back to the cargo target name — for a directory-style target that never matches 'Running tests/<path>.rs' and a valid negative-only target would FAIL the gate"
  fi
  # (b) the census subject must include the lib sources the lane executes via --lib.
  if [ "$(grep -cE 'cqlite-core/src' "$lh_code")" -gt 0 ]; then
    ok "1699-r9-census-lib: the census subject includes cqlite-core/src (the --lib half the lane executes)"
  else
    bad "1699-r9-census-lib: the census scans only integration-target roots — an inline co-required unit test in cqlite-core/src would compile out while the census reported every gated body reachable (a FALSE ZERO-GAP, and 3478 sibling unit tests keep the aggregate guard's count nonzero)"
  fi
fi
# (c) no output may still name the retired oracle: a diagnostic that misdirects remediation
#     is its own small version of this issue's defect.
if [ "$(grep -c 'enabled features (cargo metadata)' "$GATE")" -eq 0 ]; then
  ok "1699-r9-label: no output still labels the enabled-feature oracle 'cargo metadata' (it is package-scoped cargo tree)"
else
  bad "1699-r9-label: output still claims the enabled-feature oracle is 'cargo metadata' — it is package-scoped 'cargo tree -p', and a stale label misdirects both audit logs and failure remediation"
fi

# (d) BEHAVIOURAL: an inline co-required unit test (the src/** shape) is detected. This is
#     the fixture roborev asked for; it pins the classifier against the shape the widened
#     subject exists to catch.
if [ -s "$coreq_h" ]; then
  inline_fx="$tmp/1699-coreq-inline.rs"
  cat > "$inline_fx" <<'RSFX4'
pub fn production_code() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(feature = "legacy-heuristics", feature = "absent-one"))]
    #[test]
    fn inline_co_required_unit_test() {
        production_code();
    }

    #[cfg(feature = "legacy-heuristics")]
    #[test]
    fn inline_reachable_unit_test() {}
}
RSFX4
  inline_out="$tmp/1699-coreq-inline-out.txt"
  _legacy_coreq_sites "$inline_fx" " default legacy-heuristics " > "$inline_out" 2>/dev/null
  i_site=$(awk -F'\t' '$1=="site"' "$inline_out" | wc -l | tr -d ' ')
  i_all=$(wc -l < "$inline_out" | tr -d ' ')
  if [ "$i_site" = "1" ] && [ "$i_all" = "1" ]; then
    ok "1699-r9-inline: an inline co-required unit test inside #[cfg(test)] mod tests is detected, and the reachable sibling is not reported"
  else
    bad "1699-r9-inline: expected exactly 1 inline gap record, got $i_site of $i_all — the census would miss (or invent) inline unit-test gaps in cqlite-core/src"
  fi
fi

# --- 24. #1699: round-10 — the census reports SITES, and gating shapes it used to mangle
#
# roborev round-10 (Medium) found that the classifier called a gated `mod tests` "support
# code" and ignored crate-level `#![cfg(...)]` entirely. Its suggested remedy was "preferably
# using Rust syntax tooling", which is the tell: counting test BODIES needs a Rust parser.
# That was the FOURTH consecutive round with a classification finding, so the classifier was
# DESCOPED (see _legacy_coreq_sites) per the pre-commitment recorded in the PR, rather than
# patched a fifth time. These fixtures pin the descoped contract.
if [ -s "$coreq_h" ]; then
  r10_fx="$tmp/1699-coreq-shapes.rs"
  cat > "$r10_fx" <<'RSFX5'
#![cfg(all(feature = "legacy-heuristics", feature = "absent-crate"))]

#[cfg(all(feature = "legacy-heuristics", feature = "absent-mod"))]
mod gated_tests {
    #[test]
    fn one() {}
    #[test]
    fn two() {}
}

#[cfg(feature = "legacy-heuristics")]
#[cfg(feature = "absent-stacked")]
#[test]
fn stacked() {}

#[cfg(any(feature = "legacy-heuristics", feature = "absent-any"))]
#[test]
fn reachable_via_any() {}

#[cfg(feature = "legacy-heuristics")]
#[test]
fn fully_reachable() {}
RSFX5
  r10_out="$tmp/1699-coreq-shapes-out.txt"
  _legacy_coreq_sites "$r10_fx" " default legacy-heuristics " > "$r10_out" 2>/dev/null
  r_site=$(awk -F'\t' '$1=="site"' "$r10_out" | wc -l | tr -d ' ')
  r_skip=$(awk -F'\t' '$1=="skip"' "$r10_out" | wc -l | tr -d ' ')
  r_all=$(wc -l < "$r10_out" | tr -d ' ')
  r_crate=$(awk -F'\t' '$1=="site" && $2=="1"' "$r10_out" | wc -l | tr -d ' ')
  r_mod=$(awk -F'\t' '$1=="site" && $2=="3"' "$r10_out" | wc -l | tr -d ' ')
  if [ "$r_site" = "3" ]; then
    ok "1699-r10-shapes: crate-level #![cfg], a gated mod, and a stacked pair are all reported as sites"
  else
    bad "1699-r10-shapes: expected 3 sites (crate-level, mod, stacked), got $r_site — a gating shape is being missed or merged"
  fi
  if [ "$r_crate" = "1" ] && [ "$r_mod" = "1" ]; then
    ok "1699-r10-inner-split: a crate-level #![cfg] is its OWN site, not merged with the next item's attributes"
  else
    bad "1699-r10-inner-split: the crate-level #![cfg] (line 1) and the mod gate (line 3) are not two distinct sites — an inner attribute gates the enclosing scope and attaches to no following item, so merging them under-counts sites and merges their feature lists"
  fi
  if [ "$r_skip" = "1" ]; then
    ok "1699-r10-any-unclassified: an any(...) cluster is still unclassified, not counted as a site"
  else
    bad "1699-r10-any-unclassified: expected 1 unclassified any(...) cluster, got $r_skip — any(legacy-heuristics, X) is REACHABLE here, so calling it omitted is a false claim"
  fi
  # The complement: the fully-reachable test must produce NOTHING. Without this, a reporter
  # that emitted a record per cluster unconditionally would satisfy every assert above.
  if [ "$r_all" = "4" ]; then
    ok "1699-r10-shapes-complement: the fully-reachable gated test produces no record (the reporter is not emitting unconditionally)"
  else
    bad "1699-r10-shapes-complement: expected exactly 4 records, got $r_all — a site with no co-required feature is being reported, so the census would invent omissions"
  fi
  # The descope itself is pinned: no consumer may reintroduce a body count, because the
  # count is unknowable without parsing Rust (one site can gate a whole module).
  if [ "$(awk -F'\t' '$1=="fn" || $1=="item"' "$r10_out" | wc -l | tr -d ' ')" = "0" ]; then
    ok "1699-r10-no-classification: the reporter emits no fn/item classification (descoped in round 10)"
  else
    bad "1699-r10-no-classification: fn/item classification is back — counting gated test BODIES needs a Rust parser, and four consecutive review rounds found a defect in trying"
  fi
fi

# --- 25. #1699: the zero-test parser is tested DIRECTLY, against synthetic cargo logs ---
#
# roborev round-11 finding (Low), and it was right about the important part: the observation
# harness's Flight plant uses a FAILING ASSERTION, so cargo exits non-zero on its own and the
# lane reds whether or not check_unittest_targets_ran works. The harness therefore did NOT
# validate the parser it claimed to exercise — a guard covered only by a test that would pass
# without it is the exact shape this whole issue is about, one level down.
#
# Tested here against synthetic logs instead of through a compile: the parser's subject is
# cargo's OUTPUT FORMAT, so text fixtures are the right oracle and cost nothing. Cases cover
# observed-nonzero, observed-zero, a missing target, several binaries, and ignored-only runs.
zt_h="$tmp/1699-zt-helper.sh"
# The guard calls _ansi_stripped_log, so the extraction must carry it. Getting this wrong is
# instructive rather than merely annoying: with the helper undefined the redirection target
# became the empty string, the loop read nothing, and check_no_unexpected_zero_tests reported
# OK having parsed zero lines — the vacuous pass, reproduced by accident in the harness.
awk '/^_ansi_stripped_log\(\) \{/,/^\}/' "$GATE" > "$zt_h"
awk '/^check_unittest_targets_ran\(\) \{/,/^\}/' "$GATE" >> "$zt_h"
if [ ! -s "$zt_h" ]; then
  bad "1699-zt-extract: could not extract check_unittest_targets_ran — the extraction broke, so these asserts would pass vacuously"
else
  ok "1699-zt-extract: extracted check_unittest_targets_ran from the real script"
  # shellcheck disable=SC1090
  . "$zt_h"

  zt_case() { # zt_case <name> <expect: pass|fail> <log-content> <expected-target>...
    local cname="$1" expect="$2" content="$3"; shift 3
    local lf="$tmp/1699-zt-$cname.log"
    printf '%s\n' "$content" > "$lf"
    if check_unittest_targets_ran "zt-$cname" "$lf" "$@" >/dev/null 2>&1; then
      if [ "$expect" = "pass" ]; then
        ok "1699-zt-$cname: guard PASSES as expected"
      else
        bad "1699-zt-$cname: guard PASSED but should have FAILED — this is the vacuous-pass direction, the one that matters"
      fi
    else
      if [ "$expect" = "fail" ]; then
        ok "1699-zt-$cname: guard FAILS as expected"
      else
        bad "1699-zt-$cname: guard FAILED but should have PASSED — a false red trains people to re-run until green"
      fi
    fi
  }

  zt_case observed-nonzero pass \
'     Running unittests src/lib.rs (target/debug/deps/x-1)
running 12 tests
test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out' \
    src/lib.rs

  zt_case observed-zero fail \
'     Running unittests src/lib.rs (target/debug/deps/x-1)
running 0 tests
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out' \
    src/lib.rs

  # A target the selection named but cargo never ran: the guard must not pass on silence.
  zt_case missing-target fail \
'     Running unittests src/lib.rs (target/debug/deps/x-1)
running 12 tests
test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out' \
    src/lib.rs src/main.rs

  zt_case two-bins-both-nonzero pass \
'     Running unittests src/lib.rs (target/debug/deps/x-1)
running 12 tests
test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
     Running unittests src/main.rs (target/debug/deps/y-2)
running 2 tests
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out' \
    src/lib.rs src/main.rs

  # The case a --bins selection makes reachable: one binary silently empty.
  zt_case two-bins-one-zero fail \
'     Running unittests src/lib.rs (target/debug/deps/x-1)
running 12 tests
test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
     Running unittests src/main.rs (target/debug/deps/y-2)
running 0 tests
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out' \
    src/lib.rs src/main.rs

  # An all-ignored run still COMPILED and REGISTERED its tests; cargo reports a non-zero
  # `running N tests`, so it is observed. The guard's subject is "did this target execute
  # anything at all", not "did assertions run", so this must PASS — reds here would make the
  # guard fire on a legitimately #[ignore]d suite.
  zt_case ignored-only pass \
'     Running unittests src/lib.rs (target/debug/deps/x-1)
running 3 tests
test result: ok. 0 passed; 0 failed; 3 ignored; 0 measured; 0 filtered out' \
    src/lib.rs

  # An empty expected set must FAIL: a guard with no subject reports OK having measured
  # nothing (#3384). Already asserted in the production path; pinned here behaviourally.
  zt_case empty-subject fail \
'     Running unittests src/lib.rs (target/debug/deps/x-1)
running 12 tests
test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out'
fi

# --- 26. #1699: ONE source set per target (module closure), shared by all consumers ------
#
# roborev round-12. Rounds 11 and 12 were the same shape as round 8: I changed where the data
# comes from and left a consumer reading the old thing. Round 12 was explicit about the cost —
# discovery looked at the module tree while the POLARITY scan and the CENSUS still read only
# the root file, so a positive gate in a child module made a target ALLOWED-ZERO (excused from
# the zero-tests guard) and co-required sites in that child were absent from the census. Both
# silent. So the fix is one source set, computed once, read by everything.
cl_h="$tmp/1699-closure.sh"
awk '/^_rust_module_closure\(\) \{/,/^\}/' "$GATE" > "$cl_h"
if [ ! -s "$cl_h" ]; then
  bad "1699-closure-extract: could not extract _rust_module_closure — extraction broke, so these asserts would pass vacuously"
else
  ok "1699-closure-extract: extracted _rust_module_closure from the real script"
  # shellcheck disable=SC1090
  . "$cl_h"
  cl_root="$tmp/1699-cl"
  rm -rf "$cl_root"; mkdir -p "$cl_root/tests/child" "$cl_root/tests/common" "$cl_root/elsewhere"
  # A flat target root whose gated test lives ONLY in a sibling module (the round-11/12 case)
  cat > "$cl_root/tests/flat.rs" <<'CLF'
mod common;
#[path = "../elsewhere/mapped.rs"]
mod mapped;
#[test]
fn root_has_no_gate() {}
CLF
  cat > "$cl_root/tests/common/mod.rs" <<'CLF'
mod deeper;
#[cfg(all(feature = "legacy-heuristics", feature = "absent-child"))]
#[test]
fn gated_in_child_module() {}
CLF
  printf '#[test]\nfn deep() {}\n' > "$cl_root/tests/common/deeper.rs"
  printf '#[test]\nfn mapped_test() {}\n' > "$cl_root/elsewhere/mapped.rs"
  cl_out=$(_rust_module_closure "$cl_root/tests/flat.rs" 2>"$tmp/1699-cl-unres.txt")
  cl_n=$(printf '%s' "$cl_out" | grep -c . || true)
  # root + common/mod.rs + common/deeper.rs + elsewhere/mapped.rs = 4
  if [ "$cl_n" = "4" ]; then
    ok "1699-closure-reach: the closure reaches mod NAME; (resolved as common/mod.rs), a transitive child, and a #[path]-mapped file outside the tests dir"
  else
    bad "1699-closure-reach: expected 4 reachable sources, got $cl_n — a module layout is being missed, and every consumer of an incomplete set fails in the SILENT direction"
  fi
  if [ ! -s "$tmp/1699-cl-unres.txt" ]; then
    ok "1699-closure-resolves: no spurious UNRESOLVED report on a standard layout (a #[path] mod must not also be name-resolved)"
  else
    bad "1699-closure-resolves: reported UNRESOLVED on a resolvable layout — that would FAIL the lane spuriously: $(tr '\n' ' ' < "$tmp/1699-cl-unres.txt")"
  fi
  # An UNRESOLVED mod must be reported, because an incomplete set is silently permissive.
  printf 'mod nowhere_to_be_found;\n' > "$cl_root/tests/broken.rs"
  _rust_module_closure "$cl_root/tests/broken.rs" >/dev/null 2>"$tmp/1699-cl-unres2.txt"
  if grep -q 'UNRESOLVED nowhere_to_be_found' "$tmp/1699-cl-unres2.txt"; then
    ok "1699-closure-unresolved: an unresolvable mod is REPORTED (the lane fails closed on it)"
  else
    bad "1699-closure-unresolved: an unresolvable mod is silently ignored — the source set would be incomplete and the polarity scan would excuse a gated target"
  fi
  # And the consumers must actually read it, not just be handed it.
  if [ -s "$lh_code" ]; then
    if [ "$(grep -cE '_mt_closure' "$lh_code")" -ge 3 ]; then
      ok "1699-closure-shared: the closure feeds membership, polarity AND the census (>=3 uses)"
    else
      bad "1699-closure-shared: fewer than 3 uses of the shared source set — a consumer is back to reading the root file alone, which is exactly the round-12 defect"
    fi
    if [ "$(grep -cE 'UNRESOLVED|_mt_unres' "$lh_code")" -gt 0 ]; then
      ok "1699-closure-failclosed: the lane fails closed on an unresolved module tree"
    else
      bad "1699-closure-failclosed: the lane no longer fails on an unresolved module tree — an incomplete source set is silently permissive in all three consumers"
    fi
  fi
fi

# --- 27. #1699: _deny_warnings APPENDS to an inherited plain RUSTFLAGS ------------------
#
# roborev round-12 (Medium): the encoded branch preserved the operator's flags while the
# plain branch REPLACED them — so target/sanitizer/codegen flags would be dropped for THESE
# LANES ONLY, compiling something subtly different from every other component in the run.
if [ -s "$dw_body" ]; then
  if [ "$(grep -cE 'RUSTFLAGS="\$\{RUSTFLAGS:\+\$RUSTFLAGS \}-D warnings"' "$dw_body")" -gt 0 ]; then
    ok "1699-r12-rustflags-append: the plain branch APPENDS -D warnings to an inherited RUSTFLAGS"
  else
    bad "1699-r12-rustflags-append: the plain branch replaces RUSTFLAGS instead of appending — an asymmetry with the encoded branch that silently drops the operator's flags for these lanes only"
  fi
fi

# --- 28. #1699: round-13 — mapped targets, every visibility form, skippable targets -----
#
# THE FIRST OF THESE PINS A REGRESSION I CAUSED. Round 11 made the legacy lane refuse a test
# target mapped outside tests/ (the shared guard could not see it, so it could run zero tests
# unnoticed). A coarse round-12 edit spliced over that region and SILENTLY DELETED the
# refusal — and nothing noticed, because with no such target in the tree the branch never
# ran. Round 13 re-reported it, correctly. The lesson is not "be careful editing": it is that
# a guard whose only protection is its own presence in the file has no protection at all. So
# the fix now lives in the shared guard and is pinned BEHAVIOURALLY below.
if [ -s "$zt_h" ]; then
  zn_h="$tmp/1699-zeronotest.sh"
  awk '/^_ansi_stripped_log\(\) \{/,/^\}/' "$GATE" > "$zn_h"
  awk '/^check_no_unexpected_zero_tests\(\) \{/,/^\}/' "$GATE" >> "$zn_h"
  if [ ! -s "$zn_h" ]; then
    bad "1699-r13-zn-extract: could not extract check_no_unexpected_zero_tests — extraction broke, so these asserts would pass vacuously"
  else
    ok "1699-r13-zn-extract: extracted check_no_unexpected_zero_tests from the real script"
    if [ "$(grep -c '_ansi_stripped_log()' "$zn_h")" -gt 0 ]; then
      ok "1699-r13-zn-deps: the extraction carries _ansi_stripped_log (without it the guard reads an EMPTY path and reports OK having parsed nothing)"
    else
      bad "1699-r13-zn-deps: the extraction is missing _ansi_stripped_log — the guard would read an empty path, parse zero lines, and PASS vacuously"
    fi
    # shellcheck disable=SC1090
    . "$zn_h"
    zn_case() { # <name> <expect> <log> <allowed-zero...>
      local cname="$1" expect="$2" content="$3"; shift 3
      local lf="$tmp/1699-zn-$cname.log"
      printf '%s\n' "$content" > "$lf"
      if check_no_unexpected_zero_tests "zn-$cname" "$lf" "$@" >/dev/null 2>&1; then
        [ "$expect" = pass ] && ok "1699-zn-$cname: guard PASSES as expected" \
          || bad "1699-zn-$cname: guard PASSED but should have FAILED — a zero-test target went unrecorded, which is the vacuous pass this guard exists to prevent"
      else
        [ "$expect" = fail ] && ok "1699-zn-$cname: guard FAILS as expected" \
          || bad "1699-zn-$cname: guard FAILED but should have PASSED — a false red trains people to re-run until green"
      fi
    }
    # A target MAPPED OUTSIDE tests/ running zero tests: previously invisible ⇒ vacuous PASS.
    zn_case mapped-zero fail \
'     Running custom/mapped_target.rs (target/debug/deps/m-1)
running 0 tests
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out' 
    # ...and allowable by its package-relative identifier.
    zn_case mapped-zero-allowed pass \
'     Running custom/mapped_target.rs (target/debug/deps/m-1)
running 0 tests
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out' \
      custom/mapped_target
    # The existing tests/ spelling must keep working, byte for byte — this is the
    # compatibility half, and breaking it would red every other caller.
    zn_case tests-relative-still-works pass \
'     Running tests/foo.rs (target/debug/deps/f-1)
running 0 tests
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out' \
      foo
    zn_case tests-relative-unallowed fail \
'     Running tests/foo.rs (target/debug/deps/f-1)
running 0 tests
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out' \
      something_else
    # `--lib`/`--bins` unittest lines belong to the OTHER guard; this one must ignore them,
    # or a legitimately-guarded lib suite would be double-counted and red here.
    zn_case unittests-ignored pass \
'     Running unittests src/lib.rs (target/debug/deps/x-1)
running 0 tests
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out'
  fi
fi

# Every Rust visibility form on a `mod X;` declaration (30 live `pub(crate) mod` lines here).
if [ -s "$cl_h" ] && [ -d "$cl_root" ]; then
  mkdir -p "$cl_root/tests/vis"
  cat > "$cl_root/tests/visroot.rs" <<'CLV'
pub(crate) mod a;
pub(super) mod b;
pub(in crate::x) mod c;
pub mod d;
mod e;
CLV
  for m in a b c d e; do printf '#[test]\nfn t() {}\n' > "$cl_root/tests/vis_$m.rs"; done
  mv "$cl_root/tests/vis_a.rs" "$cl_root/tests/a.rs"; mv "$cl_root/tests/vis_b.rs" "$cl_root/tests/b.rs"
  mv "$cl_root/tests/vis_c.rs" "$cl_root/tests/c.rs"; mv "$cl_root/tests/vis_d.rs" "$cl_root/tests/d.rs"
  mv "$cl_root/tests/vis_e.rs" "$cl_root/tests/e.rs"
  vis_out=$(_rust_module_closure "$cl_root/tests/visroot.rs" 2>"$tmp/1699-vis-unres.txt")
  vis_n=$(printf '%s' "$vis_out" | grep -c . || true)
  if [ "$vis_n" = "6" ]; then
    ok "1699-r13-visibility: all five visibility forms of 'mod X;' are resolved (pub(crate)/pub(super)/pub(in ...)/pub/private)"
  else
    bad "1699-r13-visibility: expected root + 5 modules = 6 sources, got $vis_n — a restricted-visibility mod is being skipped, making its child modules invisible to discovery, polarity AND the census"
  fi
  if [ ! -s "$tmp/1699-vis-unres.txt" ]; then
    ok "1699-r13-visibility-resolved: no spurious UNRESOLVED on restricted-visibility modules"
  else
    bad "1699-r13-visibility-resolved: reported UNRESOLVED on resolvable restricted-visibility modules, which would FAIL the lane spuriously"
  fi
fi

# --- 29. #1699: no GNU-only constructs in the new lanes (macOS is a first-class host) ---
#
# roborev round-14 finding (Medium): the lane used `xargs -r`, which is GNU-only. BSD/macOS
# xargs rejects it, and this gate treats macOS as a first-class host (a `Darwin) … taskpolicy`
# wrapper, a BSD `stat` branch, and an explicit /bin/bash-3.2 floor). On that host the lane
# would have skipped every source-gated target and then reported a FAILED DERIVATION — a
# confusing red rather than a wrong answer, but a red nobody could act on.
#
# Dropping `-r` alone would not have been the fix: without it GNU xargs runs the command once
# with NO file arguments, and `grep -lE <pattern>` with no files reads STDIN. Portable loops
# have neither problem. STATIC lint, following the precedent in
# test_agent_gate_tree_portability.sh, which lints tree-integrity functions the same way.
for fn_ in run_legacy_heuristics run_flight_tests run_feature_iso _rust_module_closure \
           _lh_positive_in_closure _package_test_targets_gated _package_unittest_srcs \
           _resolved_package_features _deny_warnings; do
  body_="$tmp/1699-gnu-$fn_.txt"
  awk -v f="^$fn_\\\\(\\\\) \\\\{" '$0 ~ f, /^\}/' "$GATE" > "$body_"
  if [ ! -s "$body_" ]; then
    bad "1699-gnu-scope: could not extract $fn_ — the extraction broke, so this lint would pass vacuously"
    continue
  fi
  code_="$tmp/1699-gnu-$fn_-code.txt"
  sed 's/[[:space:]]*#.*$//' "$body_" > "$code_"
  # The comments deliberately NAME the forbidden constructs, so strip them first — an oracle
  # that reads its own rationale as a violation is the #3312 defect, and it already
  # false-FAILED once in this issue.
  hits=$(grep -nE '(xargs[^|]*-r|readlink -f|stat -c|sed -i[^.]|date -d|grep -P|sort -V)' "$code_" | head -3)
  if [ -z "$hits" ]; then
    ok "1699-gnu-$fn_: no GNU-only construct"
  else
    bad "1699-gnu-$fn_: GNU-only construct(s) present — macOS is a first-class gate host: $(printf '%s' "$hits" | tr '\n' ' ')"
  fi
done

# --- 30. #1699: both zero-test guards survive CARGO_TERM_COLOR=always (round-15, HIGH) ---
#
# .github/workflows/gate.yml (the nightly FULL gate) sets `CARGO_TERM_COLOR: always`, as do
# seven other workflows and scripts/local/pre-merge.sh. Cargo then emits
#     ESC[1mESC[92m     RunningESC[0m unittests src/lib.rs (...)
# with the reset sequence BETWEEN `Running` and the path, so a parser keyed on literal text
# sees nothing. MEASURED against real cargo output, and the two directions differ:
#   * check_unittest_targets_ran  -> FALSE FAIL (the lanes red on a clean nightly run)
#   * check_no_unexpected_zero_tests -> VACUOUS PASS (a zero-test target goes unrecorded)
# The second is PRE-EXISTING for the guard's other callers on nightly CI.
#
# The ESC byte is injected with printf, not written as \x1b, because these fixtures must
# exercise the same bytes cargo emits.
if [ -s "$zt_h" ] && [ -s "$zn_h" ]; then
  ESC=$(printf '\033')
  col_ok="$tmp/1699-color-ok.log"
  printf '%s[1m%s[92m     Running%s[0m unittests src/lib.rs (target/debug/deps/x-1)\nrunning 12 tests\ntest result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n' \
    "$ESC" "$ESC" "$ESC" > "$col_ok"
  if check_unittest_targets_ran "color-ok" "$col_ok" src/lib.rs >/dev/null 2>&1; then
    ok "1699-r15-color-unittest: a COLOURED healthy log is parsed (no false FAIL on nightly gate.yml)"
  else
    bad "1699-r15-color-unittest: a COLOURED healthy log is not parsed — the new lanes would red on every clean nightly run, reporting 'no Running unittests line' about a healthy log"
  fi

  col_zero="$tmp/1699-color-zero.log"
  printf '%s[1m%s[92m     Running%s[0m tests/foo.rs (target/debug/deps/foo-1)\nrunning 0 tests\ntest result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n' \
    "$ESC" "$ESC" "$ESC" > "$col_zero"
  if check_no_unexpected_zero_tests "color-zero" "$col_zero" >/dev/null 2>&1; then
    bad "1699-r15-color-zerotest: a COLOURED zero-test log PASSES — the target is never associated with its result, so the #2039 guard reports OK having measured nothing (this is the vacuous-pass direction, and it affects the guard's other callers on nightly CI too)"
  else
    ok "1699-r15-color-zerotest: a COLOURED zero-test log is still caught (the #2039 guard is not silently inert under colour)"
  fi

  # A coloured log that is HEALTHY must still pass, or the strip has turned one false verdict
  # into the other.
  col_nonzero="$tmp/1699-color-nonzero.log"
  printf '%s[1m%s[92m     Running%s[0m tests/foo.rs (target/debug/deps/foo-1)\nrunning 7 tests\ntest result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n' \
    "$ESC" "$ESC" "$ESC" > "$col_nonzero"
  if check_no_unexpected_zero_tests "color-nonzero" "$col_nonzero" >/dev/null 2>&1; then
    ok "1699-r15-color-complement: a COLOURED healthy integration log still passes (the strip did not trade one false verdict for the other)"
  else
    bad "1699-r15-color-complement: a COLOURED healthy integration log now FAILS — the ANSI strip is over-matching"
  fi

  # The strip must not be done through a PIPE into the reading loop: that puts the loop in a
  # subshell and discards the accumulated verdict, which for these guards means silently
  # passing — the exact failure they exist to prevent.
  for g_ in check_unittest_targets_ran check_no_unexpected_zero_tests; do
    gb_="$tmp/1699-color-$g_.txt"
    awk -v f="^$g_\\\\(\\\\) \\\\{" '$0 ~ f, /^\}/' "$GATE" > "$gb_"
    # Tests the PROPERTY, not a spelling: the loop must be fed by REDIRECTION and must not be
    # the right-hand side of a pipe. The first cut of this assert matched the literal
    # `done < "$(_ansi_stripped_log ...)"` and broke the moment round 16 hoisted that into a
    # pre-resolved variable — pinning a spelling makes an assert fail on a correct change,
    # which is how asserts get deleted rather than fixed.
    if [ "$(grep -cE 'done[[:space:]]*<[[:space:]]*"\$' "$gb_")" -gt 0 ] \
       && [ "$(grep -cE '\|[[:space:]]*while' "$gb_")" -eq 0 ]; then
      ok "1699-r15-color-nosubshell: $g_ reads the stripped log by REDIRECTION, not through a pipe (a piped loop runs in a subshell and its verdict is discarded)"
    else
      bad "1699-r15-color-nosubshell: $g_ no longer reads a stripped log by redirection — if it was changed to a pipe, the loop runs in a subshell and the accumulated verdict is LOST, which means silently passing"
    fi
  done
fi

# --- 31. #1699: a guard must know whether it measured anything (round-16, HIGH) ---------
#
# Round 15 gave check_no_unexpected_zero_tests / check_unittest_targets_ran a dependency
# (_ansi_stripped_log). Round 16 found the consequence in a place I had not checked: the
# cli-tests component runs its body under `bash -c` and `export -f`s the GUARDS but not the
# helper, so the command substitution yielded the EMPTY STRING, `done < ""` failed, the loop
# never ran — and the guard returned SUCCESS having parsed nothing. That silently disabled the
# CLI zero-test protection. I had fixed the identical shape in a test extraction one commit
# earlier and written a commit message about it.
#
# So the durable fix is not the export line: it is that the guard REFUSES to report OK when it
# could not read its input. Three layers, each pinned, because each alone has a silent mode.
if [ -s "$zn_h" ]; then
  # LAYER 1 — the export list carries the dependency.
  if [ "$(grep -cE '^export -f _ansi_stripped_log' "$GATE")" -gt 0 ]; then
    ok "1699-r16-export-dep: _ansi_stripped_log is export -f'd alongside the guards that call it (the cli-tests bash -c body needs it)"
  else
    bad "1699-r16-export-dep: _ansi_stripped_log is NOT exported — inside the cli-tests 'bash -c' the guard resolves an empty parse source, reads nothing, and reports OK: the CLI zero-test protection silently disabled"
  fi

  # LAYER 2 — BEHAVIOURAL: the guard fails closed with its helper missing. This is the assert
  # that would have caught round 16 regardless of any export list.
  nohelp="$tmp/1699-r16-nohelper.sh"
  awk '/^check_no_unexpected_zero_tests\(\) \{/,/^\}/' "$GATE" > "$nohelp"
  zero_log="$tmp/1699-r16-zero.log"
  printf '     Running tests/foo.rs (target/debug/deps/foo-1)\nrunning 0 tests\ntest result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n' > "$zero_log"
  if ( unset -f _ansi_stripped_log 2>/dev/null; . "$nohelp"; check_no_unexpected_zero_tests "nohelper" "$zero_log" >/dev/null 2>&1 ); then
    bad "1699-r16-failclosed: with _ansi_stripped_log undefined the guard REPORTED OK — a guard that consumed no input has measured nothing and must never pass; this is the exact vacuous pass it exists to prevent, arriving through its own plumbing"
  else
    ok "1699-r16-failclosed: with its helper undefined the guard FAILS CLOSED instead of passing vacuously"
  fi
  # ...and the complement: with the helper present it must still behave normally, or layer 2
  # has simply broken the guard.
  if [ -s "$cl_h" ]; then
    if ( . "$zn_h"; check_no_unexpected_zero_tests "withhelper" "$zero_log" >/dev/null 2>&1 ); then
      bad "1699-r16-failclosed-complement: with the helper present the guard PASSED a zero-test log — the fail-closed check has not broken the guard, but the guard itself is now wrong"
    else
      ok "1699-r16-failclosed-complement: with the helper present the guard still catches a zero-test target (fail-closed did not replace the real check)"
    fi
  fi

  # LAYER 3 — the other extraction site carries the dependency too.
  if [ "$(grep -c '_ansi_stripped_log' scripts/tests/test_agent_gate_cli_tests_enum.sh 2>/dev/null)" -gt 0 ]; then
    ok "1699-r16-enum-dep: test_agent_gate_cli_tests_enum.sh extracts the guard's dependency as well as the guard"
  else
    bad "1699-r16-enum-dep: test_agent_gate_cli_tests_enum.sh extracts the guard without _ansi_stripped_log — its behavioural cases would run against a guard that parses nothing"
  fi
fi

# --- 32. #1699: results with NO recognised banner is a broken parse, not a pass (round-17) --
#
# Round 16 made the guard fail when it could not READ its input. That was not enough: a
# non-empty, perfectly readable log can contain no PARSEABLE `Running` banner (a cargo format
# change, a normalisation that drops the line, output suppressed by a wrapper) and then both
# `target` and `bad` stay empty and the guard returns SUCCESS even for
# `test result: ok. 0 passed`. The vacuous green surviving two rounds of closing it. So the
# guard now demands an AFFIRMATIVE attribution: results present ⇒ at least one banner
# recognised.
if [ -s "$zn_h" ]; then
  nb="$tmp/1699-r17-nobanner.log"
  printf 'some preamble line\nrunning 0 tests\ntest result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n' > "$nb"
  if ( . "$zn_h"; check_no_unexpected_zero_tests "nobanner" "$nb" >/dev/null 2>&1 ); then
    bad "1699-r17-nobanner: a log with test RESULTS but no recognisable 'Running' banner PASSED — nothing was attributed to a target, so the guard measured nothing and reported OK"
  else
    ok "1699-r17-nobanner: results with zero recognised banners is a FAIL (the parse is broken, and a broken parse is never a pass)"
  fi
  # Complement: a log with NO results at all must not be forced to fail — some components
  # legitimately produce no test-result lines, and reddening there would be a false red.
  nrl="$tmp/1699-r17-noresults.log"
  printf '   Compiling foo v0.1.0\n    Finished test profile\n' > "$nrl"
  if ( . "$zn_h"; check_no_unexpected_zero_tests "noresults" "$nrl" >/dev/null 2>&1 ); then
    ok "1699-r17-noresults-complement: a log with no test results at all is not forced to FAIL (the check keys on results PRESENT, not on banners absent)"
  else
    bad "1699-r17-noresults-complement: a log with no test results now FAILS — the affirmative check is over-reaching into a false red"
  fi
fi

# The POSITIVE form round 17 preferred: every DERIVED target must be observed. The zero-test
# guard can only judge targets it saw, so a target that never ran at all is invisible to it.
tob="$tmp/1699-r17-tob.sh"
awk '/^_ansi_stripped_log\(\) \{/,/^\}/' "$GATE" > "$tob"
awk '/^check_test_targets_observed\(\) \{/,/^\}/' "$GATE" >> "$tob"
if [ "$(grep -c 'check_test_targets_observed()' "$tob")" -eq 0 ]; then
  bad "1699-r17-observed-extract: could not extract check_test_targets_observed — extraction broke, so these asserts would pass vacuously"
else
  ok "1699-r17-observed-extract: extracted check_test_targets_observed from the real script"
  obs_log="$tmp/1699-r17-obs.log"
  printf '     Running tests/alpha.rs (x)\nrunning 3 tests\ntest result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n' > "$obs_log"
  if ( . "$tob"; check_test_targets_observed "obs" "$obs_log" alpha >/dev/null 2>&1 ); then
    ok "1699-r17-observed-present: an observed target passes"
  else
    bad "1699-r17-observed-present: an observed target FAILED — false red"
  fi
  if ( . "$tob"; check_test_targets_observed "obs" "$obs_log" alpha beta >/dev/null 2>&1 ); then
    bad "1699-r17-observed-absent: a derived target that produced NO 'Running' banner passed — it did not execute, and the zero-test guard cannot see an absent target"
  else
    ok "1699-r17-observed-absent: a derived target with no banner is a FAIL (it never executed)"
  fi
  if ( . "$tob"; check_test_targets_observed "obs" "$obs_log" >/dev/null 2>&1 ); then
    bad "1699-r17-observed-empty: called with NO expected target it PASSED — a guard with an empty subject set reports OK having measured nothing"
  else
    ok "1699-r17-observed-empty: an empty expected set is a FAIL (#3384's empty-subject rule)"
  fi
  if [ "$(grep -cE 'check_test_targets_observed' "$lh_code")" -gt 0 ]; then
    ok "1699-r17-observed-wired: the legacy lane actually calls it (a helper nothing calls guards nothing)"
  else
    bad "1699-r17-observed-wired: the legacy lane does not call check_test_targets_observed — an uncalled guard is decoration"
  fi
fi

# --- 33. #1699: the coverage CENSUS is the deliverable of the descope, so it is pinned ----
#
# C intent audit finding (P1): nothing anywhere asserted the census. Deleting the emitter broke no test.
# That is the sharpest kind of gap on this issue, because the spec says in as many words that "the
# deliverable of the descope is the DECLARATION, not the narrowing" — so the census was the one output
# whose absence should have been loudest, and it was the one output with no guard at all.
#
# STRUCTURAL, and honest about being so: the census is emitted from inside run_flight_tests around a real
# cargo invocation, so driving it behaviourally means a compile. What a structural assert DOES buy is
# exactly what the audit asked for — deleting or hollowing the emitter now reds — and it checks each
# required ELEMENT rather than merely that some census-shaped text exists.
# A LINE-comment strip, not a trailing-`#` strip, for these checks specifically. The trailing form
# deleted `#3384` from INSIDE the census strings and false-FAILED three elements that were present —
# the same oracle-mangles-its-own-input shape that has bitten twice already in this issue. Dropping only
# whole comment LINES keeps issue references inside `echo` text while still removing prose that merely
# mentions them.
ft_lines="$tmp/1699-ftfn-lines.txt"; grep -v '^[[:space:]]*#' "$ft_body" > "$ft_lines" 2>/dev/null || : 
lh_lines="$tmp/1699-lhfn-lines.txt"; grep -v '^[[:space:]]*#' "$lh_body" > "$lh_lines" 2>/dev/null || :
if [ -s "$ft_code" ]; then
  # (a) the count must be DERIVED at run time, never hard-coded: an understated gap is the silent
  #     under-report this lane exists to remove.
  if [ "$(grep -cE 'declares .*integration|_package_test_targets|cargo metadata' "$ft_code")" -gt 0 ] \
     && [ "$(grep -cE 'declares 4[0-9] integration' "$ft_code")" -eq 0 ]; then
    ok "1699-census-derived: the Flight census counts its integration targets at run time (no hard-coded number)"
  else
    bad "1699-census-derived: the Flight census appears to hard-code its target count — an understated gap cannot drift into a false claim only if the number is derived"
  fi
  # (b) each required element of the declaration.
  for el_ in 'EXECUTES NONE OF THEM' 'WHY' '3384' '3383' 'flight-ci' 'DECLARED, not silent'; do
    if [ "$(grep -cF "$el_" "$ft_lines")" -gt 0 ]; then
      ok "1699-census-element: the Flight census states '$el_'"
    else
      bad "1699-census-element: the Flight census no longer states '$el_' — the declaration is what this lane trades its coverage for, so a hollowed census is a silent narrowing"
    fi
  done
  # (c) BOTH sinks. A gap that appears only on stdout is a gap nobody reads in CI; one only in the log is
  #     a gap nobody reads locally.
  if [ "$(grep -cE '>>> \[\$name\]|>>> \[flight' "$ft_lines")" -gt 0 ] && [ "$(grep -cE '"\$log"' "$ft_lines")" -gt 0 ]; then
    ok "1699-census-both-sinks: the Flight census reaches both stdout and the component log"
  else
    bad "1699-census-both-sinks: the Flight census no longer reaches both stdout and the component log"
  fi
fi
if [ -s "$lh_code" ]; then
  for el_ in 'COVERAGE CENSUS' 'Sites, not bodies' '3373' 'where:'; do
    if [ "$(grep -cF "$el_" "$lh_lines")" -gt 0 ]; then
      ok "1699-census-lh-element: the legacy census states '$el_'"
    else
      bad "1699-census-lh-element: the legacy census no longer states '$el_'"
    fi
  done
fi

# --- 34. #1699: the isolation lanes' INSTRUMENT is pinned (C audit P1) -------------------
#
# Nothing forbade reverting `cargo test --lib --no-run` to `cargo check`. That matters because the
# difference IS the requirement: `cargo check` does not compile the lib's `#[cfg(test)]` modules and is
# therefore blind to the #1978 incident class (a feature-orphaned test-only helper) these lanes exist to
# catch. An earlier draft of the spec mandated `cargo check` here and contradicted the mutual-isolation
# requirement; the spec is fixed, and this pins the code so the two cannot drift apart again.
if [ -s "$tmp/1699-lanefn-run_feature_iso-code.txt" ]; then
  fi_="$tmp/1699-lanefn-run_feature_iso-code.txt"
  for req_ in -- '--lib' '--no-run' '--no-default-features' 'all-compression,'; do
    [ "$req_" = "--" ] && continue
    if [ "$(grep -cF -- "$req_" "$fi_")" -gt 0 ]; then
      ok "1699-iso-instrument: run_feature_iso still passes $req_"
    else
      bad "1699-iso-instrument: run_feature_iso no longer passes $req_ — the instrument IS the requirement here"
    fi
  done
  for forbid_ in 'cargo check' '--all-targets' '--all-features'; do
    if [ "$(grep -cF -- "$forbid_" "$fi_")" -eq 0 ]; then
      ok "1699-iso-forbidden: run_feature_iso does not use '$forbid_'"
    else
      bad "1699-iso-forbidden: run_feature_iso uses '$forbid_' — cargo check is blind to cfg(test) (#1978), --all-targets pulls in ~100 default-feature integration files (measured noise), and --all-features defeats mutual isolation entirely"
    fi
  done
fi

# --- 31. #1699: the cargo-metadata helpers are jq-OR-python3, and the two halves agree ---
#
# roborev round-18 finding (Medium): _package_unittest_srcs and _package_test_targets_gated
# were python3-ONLY, while this gate's documented convention (agent-gate.sh:1126-1130) and
# every incumbent helper is "jq, else python3, else the loud #2658 no-parser failure". On a
# jq-only host — and this gate treats macOS as a first-class host, with a /bin/bash-3.2
# floor and a BSD `stat` branch — both returned a FAILED DERIVATION, so the mandatory
# flight-tests and legacy-heuristics lanes redded the FULL gate on a healthy tree. A false
# red is not the safe direction: it is the verdict agents learn to re-run away from.
#
# TWO asserts, because neither alone is sufficient:
#   (a) STRUCTURAL, over every metadata helper — the class-level guard. The next
#       single-parser helper someone adds is caught here rather than on someone's laptop.
#   (b) DIFFERENTIAL — the jq port and the python original must produce BYTE-IDENTICAL
#       output over this workspace's real metadata. A port is a SECOND IMPLEMENTATION, and
#       #3229's lesson is that its correctness is only knowable by differential testing
#       against the original: the deleted census-exclusion oracle re-derived Go's trim
#       rules in bash, was tested against a MODEL of Go, and its NBSP divergence was
#       unfindable by care.
# DERIVED, NEVER CURATED — the rule both executing lanes are built on, applied to their own
# lint: the subject set is every gate function that INVOKES `cargo metadata`, computed from
# the committed source at run time, so a helper added later is linted with no test edit. A
# broken derivation is a FAIL naming it (below), never a shrunken subject set that greens.
# The pattern is the invocation shape `=$(cargo metadata`, not the bare words, because the
# lanes' own progress messages talk ABOUT cargo metadata and would otherwise be linted as
# helpers that lack a parser they never needed.
meta_fns_=$(sed 's/[[:space:]]*#.*$//' "$GATE" \
  | awk '/^[a-zA-Z_][a-zA-Z0-9_]*\(\)/ { split($1, a, "("); fn = a[1] }
         /=\$\(cargo metadata/ { if (fn != "") print fn }' | sort -u)
n_meta_=$(printf '%s\n' "$meta_fns_" | grep -c . || true)
# The derivation must contain the two helpers this finding was ABOUT. Anything else means the
# extraction moved and the lint is measuring a set that no longer includes its own subject.
for must_ in _package_unittest_srcs _package_test_targets_gated; do
  if printf '%s\n' "$meta_fns_" | grep -qxF "$must_"; then
    ok "1699-r18-parser-derive: the derived cargo-metadata helper set includes $must_ ($n_meta_ helpers derived)"
  else
    bad "1699-r18-parser-derive: $must_ is ABSENT from the derived cargo-metadata helper set ($n_meta_ derived) — the derivation is broken, so every assert below it would pass having measured nothing"
  fi
done
for fn_ in $meta_fns_; do
  body_="$tmp/1699-parser-$fn_.txt"
  awk -v f="^$fn_\\\\(\\\\) \\\\{" '$0 ~ f, /^\}/' "$GATE" > "$body_"
  if [ ! -s "$body_" ]; then
    bad "1699-r18-parser-scope: could not extract $fn_ — the extraction broke, so this lint would pass vacuously"
    continue
  fi
  code_="$tmp/1699-parser-$fn_-code.txt"
  sed 's/[[:space:]]*#.*$//' "$body_" > "$code_"
  jq_n_=$(grep -cF 'command -v jq' "$code_")
  py_n_=$(grep -cF 'command -v python3' "$code_")
  if [ "$jq_n_" -gt 0 ] && [ "$py_n_" -gt 0 ]; then
    ok "1699-r18-parser-$fn_: offers BOTH metadata parsers (jq and python3)"
  else
    bad "1699-r18-parser-$fn_: single-parser metadata helper (jq=$jq_n_ python3=$py_n_) — on a host carrying only the other parser this returns a FAILED DERIVATION and the lane reds on a healthy tree; macOS is a first-class gate host"
  fi
done

# (b) the differential. Needs both parsers AND cargo, since the subject is the REAL
# metadata of this workspace rather than a fixture: a fixture would only cover the shapes
# whoever wrote it already thought of, and the round-7/10/13 findings were all about
# target shapes nobody had thought of (manifest-gated, directory-style, `test = false`,
# required-features-excluded, explicitly path-mapped).
if command -v jq >/dev/null 2>&1 && command -v python3 >/dev/null 2>&1 && command -v cargo >/dev/null 2>&1; then
  repo_root_=$(cd "$SCRIPT_DIR/../.." && pwd)
  while IFS='|' read -r fn_ a1_ a2_; do
    [ -n "$fn_" ] || continue
    body_="$tmp/1699-diff-$fn_.sh"
    awk -v f="^$fn_\\\\(\\\\) \\\\{" '$0 ~ f, /^\}/' "$GATE" > "$body_"
    if [ ! -s "$body_" ]; then
      bad "1699-r18-diff-scope: could not extract $fn_ — the differential would compare nothing"
      continue
    fi
    # Force the python half by SUBSTITUTING THE ARTIFACT in our own scratch copy — never by
    # adding a parser-selection env var to the gate. A test-only seam is one more thing a
    # real invoker can set (#3312), and the point of this assert is what an ordinary host does.
    py_body_="$tmp/1699-diff-$fn_-py.sh"
    sed 's|command -v jq >/dev/null 2>&1|false|' "$body_" > "$py_body_"
    if [ "$(grep -cF 'command -v jq' "$py_body_")" -ne 0 ]; then
      bad "1699-r18-diff-$fn_: could not force the python half in the scratch copy — the differential would be comparing jq with itself, i.e. passing vacuously"
      continue
    fi
    jq_out_="$tmp/1699-diff-$fn_-jq.out"; py_out_="$tmp/1699-diff-$fn_-py.out"
    ( cd "$repo_root_" && . "$body_"    && "$fn_" "$a1_" "$a2_" ) > "$jq_out_" 2>/dev/null; jq_rc_=$?
    ( cd "$repo_root_" && . "$py_body_" && "$fn_" "$a1_" "$a2_" ) > "$py_out_" 2>/dev/null; py_rc_=$?
    if [ "$jq_rc_" -ne 0 ] || [ ! -s "$jq_out_" ]; then
      skipped "1699-r18-diff-$fn_: the jq half produced no output on this box (rc=$jq_rc_; offline registry?) — the two parsers were NOT compared for this helper"
    elif [ "$py_rc_" -ne 0 ] || [ ! -s "$py_out_" ]; then
      bad "1699-r18-diff-$fn_: the python half produced nothing (rc=$py_rc_) while the jq half worked — the incumbent parser is broken, which no lane would survive"
    elif cmp -s "$jq_out_" "$py_out_"; then
      ok "1699-r18-diff-$fn_: jq port and python original agree byte-for-byte over the real workspace metadata ($(grep -c . "$jq_out_") records)"
    else
      bad "1699-r18-diff-$fn_: the jq PORT and the python ORIGINAL DISAGREE — one of the two lanes derives a different subject set depending on which parser the host has: $(diff "$py_out_" "$jq_out_" 2>/dev/null | head -4 | tr '\n' ' ')"
    fi
  done <<'PARSER_DIFF_SPECS'
_package_unittest_srcs|cqlite-flight|lib,bin
_package_test_targets_gated|cqlite-core|legacy-heuristics
PARSER_DIFF_SPECS
else
  skipped "1699-r18-diff: needs jq + python3 + cargo on this host — the two metadata parsers were NOT differentially compared here"
fi

# roborev round-18 (Low): the cli-tests component cleaned its two logs but not the
# `.ansi-stripped` copies the zero-test guards parse, leaking two files per gate run into
# TMPDIR. Structural, because the component itself is a 5-minute cargo run: the trap line is
# the whole of the fix, so pinning the trap line is pinning the fix.
cli_body_="$tmp/1699-r18-cli-trap.txt"
awk '/^    cli-tests\)/, /compaction-byte-parity\)/' "$GATE" > "$cli_body_"
if [ ! -s "$cli_body_" ]; then
  bad "1699-r18-cli-trap-scope: could not extract the cli-tests component — this assert would pass vacuously"
elif [ "$(grep -cF 'log1.ansi-stripped' "$cli_body_")" -gt 0 ] && [ "$(grep -cF 'log2.ansi-stripped' "$cli_body_")" -gt 0 ]; then
  ok "1699-r18-cli-trap: cli-tests cleans the .ansi-stripped siblings its guards parse, not just the two logs"
else
  bad "1699-r18-cli-trap: cli-tests leaks \$log1.ansi-stripped/\$log2.ansi-stripped into TMPDIR on every gate run — the trap removes only the originals"
fi

echo "----"
echo "passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ]
