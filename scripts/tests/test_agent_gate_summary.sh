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
# The containment gate reached from here is the SYNTACTIC one and its two builtin-only
# helpers (#3249 review R6-1/R6-2). Its resolving sibling — perf_capability_sandbox_ok_resolved
# — canonicalizes with `$(cd -P …)` and is deliberately NOT on this path: naming it here would
# be the tell that a fork had been introduced into the emit chain.
for _fn in perf_capability_token_into perf_capability_proc_read \
           perf_capability_proc_dir_into perf_capability_test_mode \
           perf_capability_seam_set perf_capability_is_int \
           perf_capability_sandbox_ok perf_capability_sandbox_root_into \
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

echo "----"
echo "passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ]
