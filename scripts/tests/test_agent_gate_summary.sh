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
# Assertions that could not run because the HOST lacks a tool (not because anything is
# wrong). Counted, because ASSERT_FLOOR is a hand-maintained lower bound on how many
# assertions ran: a declared tooling skip shrinks the runnable set, so comparing the
# floor against PASS ALONE reds a legitimately-configured box for taking the skip path
# the check itself offers (issue #1465 round 11, roborev U1 — the gate deliberately
# supports a node-less host, where node-bindings SKIPs loudly). The floor is therefore
# compared against PASS + SKIPPED_TOOLING, which keeps it a real floor on hosts that
# have everything while never punishing one that does not.
#
# ACCOUNTING MUST BE 1:1, AND FOUR PRE-EXISTING SITES WERE NOT (rounds 13-14, X2/Y1/Y2).
# This paragraph has now been wrong twice — it once claimed the sites were "covered", and
# then that there were two of them — so the numbers below are the MEASURED ones and every
# claim is checkable from a single run:
#   * 1699-r18-diff-*                  2 verdicts (one per PARSER_DIFF_SPEC_ROWS row), 1 skip
#   * 1699-r32-preflight-behaviour[*]  9 verdicts (one per R32_WANT_CASES entry),       1 skip
#   * perf-host                        2 verdicts (token + accelerators line),          1 skip
#   * 1699-featoracle-{behaviour,       2 verdicts inside the cargo-guarded branch,  0 — a
#     complement}                      bare `echo "SKIP …"` that incremented nothing at
#                                      all. (The `1699-featoracle-*` PREFIX covers six
#                                      verdicts; the other four — dev, extract, nometa,
#                                      scoped — are outside that branch and unaffected.)
# All four now loop the SAME declared list their run branch iterates (r18/r32 are 1:1 BY
# CONSTRUCTION for that reason), so displacement is 1:1 rather than hand-kept.
#
# MEASURED, EIGHT HOST SHAPES, each forced SEPARATELY (conflating two capabilities in one
# run is what hid the featoracle site):
#   everything present 401+0 | jq-less 399+2 | cargo-less 397+4 | python3-less 390+11
#   node-less 400+1 | Darwin 399+2 | masked /proc 399+2 | offline registry 399+2
# accounted == 401 in every one of the eight.
SKIPPED_TOOLING=0
ok()   { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }
# A case whose PROPERTY IS UNOBSERVABLE on this box (a Linux-only kernel control on
# Darwin, an unreadable /proc entry) is reported as a SKIP — counted in neither total,
# so it can never be mistaken for a passing assertion (issue #3249 AC3).
skipped() { printf 'skip - %s\n' "$1"; SKIPPED_TOOLING=$((SKIPPED_TOOLING + 1)); }

# out_has <text> <grep-args...>: a SIGPIPE-SAFE text predicate (issue #3727 / #3862). Under
# `set -o pipefail`, `out_has "$big" PAT` can return **141 with the match present**:
# `grep -q` exits at the first match and CLOSES the pipe, so printf's next write dies — which makes
# it a RACE at any payload above bash's ~4 KiB stdio chunk rather than a clean threshold (measured:
# 64 KiB always fine, 128 KiB always 141, and a 41 KB whole-function payload in case 52 flaked once
# under load, reporting `missing: counts` while the pattern was present in the text it printed). A
# here-string is not a pipeline, so grep's own status is the answer and pipefail has nothing to
# override. Any flags that followed `-q` are passed through unchanged.
out_has() { local __t="$1"; shift; grep -q "$@" <<< "$__t"; }

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
# The two #3727 capacity tokens are UNCONDITIONAL and platform-independent, so they are inserted
# BEFORE the optional Linux-only mold/perf groups rather than appended: that keeps ` perf=` the
# last token on a Linux line, which is what case 9c-iv's sentinel position depends on, and keeps
# one grammar serving both a Darwin line (ending at sccache-used) and a Linux one.
ACCEL_CAP_RE='sccache-cap=([0-9]+\((pinned|default|inherited|stale|invalid|invalid-stale|unattributed)\)|unmeasured\((no-stats|unparsed|not-unique|no-binary|no-size)\)|na\(sccache-not-in-use\))'
ACCEL_USED_RE='sccache-used=([0-9]+\(([0-9]+%|cap-zero)\)|unmeasured\((no-stats|unparsed|not-unique|no-binary|no-size)\)|na\(sccache-not-in-use\))'
ACCEL_LINE_RE="^accelerators: sccache=(on|absent|off) nextest=(on|absent|off) lanes=(on|absent|off|serial) sccache-health=(na|ok|warn) $ACCEL_CAP_RE $ACCEL_USED_RE( mold=(linked|overridden|present-unconfigured|absent))?( perf=(ok|kptr-restricted|absent|unknown|paranoid-[0-9]+))?$"

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
if out_has "$classify_out" -E '\|(data_multi|mod)\|'; then
  bad "classify: nested helper (write_read_roundtrip/data_multi.rs or common/mod.rs) wrongly picked as a --test target"
  echo "------- classify output -------"; printf '%s\n' "$classify_out"; echo "-------------------------------"
else
  ok "classify: nested helper/module files NOT treated as --test targets"
fi
# A real direct integration-test target must still be picked, mapped to its pkg
# (features field empty for a target with no required-features).
if out_has "$classify_out" -xF "cqlite-core|compact_command|"; then
  ok "classify: real integration-test target (compact_command) picked with correct package"
else
  bad "classify: real integration-test target compact_command was NOT picked"
  echo "------- classify output -------"; printf '%s\n' "$classify_out"; echo "-------------------------------"
fi
# Finding 1: a top-level tests/*.rs target is owned by BOTH the workspace-root
# `cqlite` package AND the cqlite-integration-tests crate; BOTH must be emitted
# so the root package's target is never silently dropped from --lite selection.
if out_has "$classify_out" -xF "cqlite|cassandra5_header_tests|" \
   && out_has "$classify_out" -xF "cqlite-integration-tests|cassandra5_header_tests|"; then
  ok "classify: root-cqlite + integration-tests BOTH emitted for a top-level tests/*.rs target (finding 1)"
else
  bad "classify: top-level tests/*.rs target did NOT emit both owning packages (root cqlite dropped)"
  echo "------- classify output -------"; printf '%s\n' "$classify_out"; echo "-------------------------------"
fi
# Finding 2: a target that declares required-features must carry them through so
# --lite compiles it WITH those features instead of invoking it feature-less.
if out_has "$classify_out" -xF "cqlite-cli|issue_1388_compact_major_drop|write-support"; then
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
if out_has "$owners_out" -xF "format-validator|1"; then
  ok "owners: tools/format-validator resolves to its own package (was falling through)"
else
  bad "owners: tools/format-validator/src/lib.rs did NOT resolve to format-validator"
  echo "------- owners output -------"; printf '%s\n' "$owners_out"; echo "-----------------------------"
fi
# A bindings/* member must resolve to its cdylib package (no lib target -> 0).
if out_has "$owners_out" -xF "cqlite-py|0" \
   && out_has "$owners_out" -xF "cqlite-node|0"; then
  ok "owners: bindings/{python,node} resolve to cqlite-py|0 / cqlite-node|0 (cdylib, no --lib)"
else
  bad "owners: bindings/* did NOT resolve to their packages with has_lib=0"
  echo "------- owners output -------"; printf '%s\n' "$owners_out"; echo "-----------------------------"
fi
# The examples crate must resolve to its own package (has a lib -> 1).
if out_has "$owners_out" -xF "cqlite-examples|1"; then
  ok "owners: examples/ resolves to cqlite-examples (was falling through)"
else
  bad "owners: examples/basic.rs did NOT resolve to cqlite-examples"
  echo "------- owners output -------"; printf '%s\n' "$owners_out"; echo "-----------------------------"
fi
# A nested member (tests/format-compatibility) must win over its parent tests/.
if out_has "$owners_out" -xF "format-compatibility-tests|0"; then
  ok "owners: nested tests/format-compatibility wins longest-prefix over tests/"
else
  bad "owners: tests/format-compatibility did NOT resolve to format-compatibility-tests"
  echo "------- owners output -------"; printf '%s\n' "$owners_out"; echo "-----------------------------"
fi
# The workspace-root `cqlite` package (manifest dir == repo root) is a degenerate
# catch-all prefix and must NOT be a path owner — a docs-only change resolves to
# NO package (falls through to the cqlite-core --lib default), not to root cqlite.
if out_has "$owners_out" '^cqlite|'; then
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
if out_has "$noparser_msg" -F 'jq' \
   && out_has "$noparser_msg" -F 'python3' \
   && ! out_has "$noparser_msg" -F -- '--lib'; then
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
if out_has "$py_only" -xF \
     "python-tier: maturin develop --profile dev -m bindings/python/Cargo.toml && pytest bindings/python/tests -m 'not slow' -q"; then
  ok "py-route: python-only diff selects the maturin --profile dev + not-slow-pytest tier (exact canonical command)"
else
  bad "py-route: python-only diff did NOT select the canonical python tier command"
  echo "------- plan -------"; printf '%s\n' "$py_only"; echo "--------------------"
fi
if out_has "$py_only" "^rust-pkg:"; then
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
if out_has "$mixed" -xF "rust-pkg: cqlite-core" \
   && out_has "$mixed" "^python-tier: "; then
  ok "py-route: mixed diff selects BOTH cqlite-core AND the python tier"
else
  bad "py-route: mixed diff did NOT select both rust + python tier"
  echo "------- plan -------"; printf '%s\n' "$mixed"; echo "--------------------"
fi
# cqlite-py must NEVER appear as a rust cargo package in the mixed plan either.
if out_has "$mixed" "cqlite-py"; then
  bad "py-route: mixed diff plan referenced cqlite-py as a cargo package (must be python tier only)"
  echo "------- plan -------"; printf '%s\n' "$mixed"; echo "--------------------"
else
  ok "py-route: mixed diff never runs cargo test -p cqlite-py"
fi

# node diff -> UNAFFECTED: scopes to cqlite-node, NO python tier.
node_only=$(printf '%s\n' \
  "bindings/node/src/database.rs" \
  | bash "$GATE" --classify-scoped-plan 2>/dev/null)
if out_has "$node_only" -xF "rust-pkg: cqlite-node" \
   && ! out_has "$node_only" "^python-tier:"; then
  ok "py-route: node diff unaffected (cqlite-node, no python tier)"
else
  bad "py-route: node diff wrongly triggered the python tier or missed cqlite-node"
  echo "------- plan -------"; printf '%s\n' "$node_only"; echo "--------------------"
fi

# rust-only diff -> UNCHANGED: scopes to the rust package, NO python tier.
rust_only=$(printf '%s\n' \
  "cqlite-core/src/storage/sstable/reader.rs" \
  | bash "$GATE" --classify-scoped-plan 2>/dev/null)
if out_has "$rust_only" -xF "rust-pkg: cqlite-core" \
   && ! out_has "$rust_only" "^python-tier:"; then
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
if out_has "$cc_core" -xF "compile-check-pkg: cqlite-integration-tests" \
   && out_has "$cc_core" -xF "compile-check-pkg: format-compatibility-tests"; then
  ok "core-dep: core-src diff adds --no-run compile-check of integration-tests + format-compatibility-tests"
else
  bad "core-dep: core-src diff did NOT add the dependent-crate compile-checks"
  echo "------- plan -------"; printf '%s\n' "$cc_core"; echo "--------------------"
fi
# cqlite-core itself must NOT be in the compile-check set (its --lib already runs),
# and cdylib bindings (no test targets) must not appear.
if out_has "$cc_core" -F "compile-check-pkg: cqlite-core" \
   || out_has "$cc_core" -E 'compile-check-pkg: (cqlite-py|cqlite-node)$'; then
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

# 8c. …AND THE SAME FLOOR APPLIES TO EVERY SCRIPT THE GATE INVOKES (roborev job 273, F1).
#     8/8b exercise the GATE under /bin/bash; nothing enforced the floor on the ~80
#     `scripts/tests/*.sh` and `scripts/ci/*.sh` files the gate SHELLS OUT TO, and one of
#     them (the #3453 feature-matrix annotation guard, which `tooling-tests` ALWAYS runs)
#     shipped a `declare -A` — a bash-4.0-only construct that FAILs the gate of record on
#     macOS's stock /bin/bash 3.2, a host this repository treats as first-class.
#
#     DERIVED, NEVER CURATED: the subject set is every scripts/{ci,tests}/*.sh path that
#     appears in agent-gate.sh, read out of the gate SOURCE, so a newly-wired script joins
#     the lint with no edit here and a new file cannot enter the gate path unchecked.
#
#     THREE construct classes, and the split is deliberate AND was corrected by measurement.
#     Linted, because each has exactly one meaning, no bash-3.2 fallback, and cannot be a
#     false positive on a gate path: the ASSOCIATIVE ARRAY (`declare/local/typeset -A`),
#     CASE-CONVERSION PARAMETER EXPANSION (the `${v` + `^^}` / `,,}` / `^}` / `,}` forms),
#     and `&>>` append-redirection.
#     A FOURTH ARM FOR GNU-ONLY `\b` IN `grep -E` WAS ADDED (job 285) AND REMOVED (job 291).
#     It is the right RULE — POSIX ERE leaves `\b` undefined and BSD grep ignores it — but a
#     line-based grep-for-greps cannot enforce it, and TWO blind spots were measured within one
#     review round:
#       (a) the needle had to be `grep [^|]*\b` to avoid spanning a shell pipe, and `[^|]*`
#           therefore cannot span an ALTERNATION either — so `grep -nE '... (cargo|rustc)\b'`,
#           a real offender in this very repository, evaded it. Alternations are common in
#           greps, so this is not a corner case.
#       (b) a needle ASSEMBLED from fragments (the technique used two paragraphs up to stop
#           this lint matching its own source) also evades it — so the remedy for self-matching
#           doubles as a way to hide a real offender.
#     Closing either needs shell+regex parsing, which is the unbounded problem #3400's parse
#     lint was descoped for and #3229's census oracle removed over: a guard with documented
#     false PASSes is worse than none, because it invites reliance it cannot support. The real
#     offenders found while it existed ARE fixed; what is gone is the claim to detect the next
#     one. That claim is now made by nothing, which is honest, rather than by a lint that
#     misses alternations.
#     NOT linted, because judging them needs context a grep does not have: `mapfile` /
#     `readarray` (a script may define its own function of that name) and COMPUTED negative
#     subscripts (`${a[$i]}` with i<0 is undetectable statically anyway). A lint with false
#     positives is the lint agents learn to waive (#3400's descoped parse lint, #3229's
#     removed census oracle) — so the line is drawn at unambiguity, not at convenience.
#
#     THIS PARAGRAPH PREVIOUSLY SAID NO WIDER LINT WAS WRITTEN AND NAMED THE
#     CASE-CONVERSION FORM AS DELIBERATELY EXCLUDED. Roborev job 277 then found exactly
#     that construct, twice, IN THIS FILE. The 'needs context to judge' argument was true
#     for `mapfile` and false for the expansions; keeping it whole cost a review round.
#
#     AND THE VERDICT DECLARES ITS OWN INCOMPLETENESS (`0 of 3 RECOGNISED`, never a bare
#     all-clear), because this scan is NOT a bash-3.2 proof. Measured against docker
#     bash:3.2 (3.2.57): `bash -n` returns rc=0 for `declare -A`, for the case-conversion
#     expansion and for `mapfile` — they are RUNTIME failures, and `declare -A` is not even
#     fatal without `set -e`. Only EXECUTION under 3.2 establishes compatibility.
b32_scripts=$(grep -oE 'scripts/(ci|tests)/[a-z0-9_-]+\.sh' "$GATE" | sort -u)
b32_offenders=""
b32_scanned=0
while IFS= read -r _b32f; do
  [ -n "$_b32f" ] || continue
  [ -r "$SCRIPT_DIR/../../$_b32f" ] || continue
  b32_scanned=$((b32_scanned + 1))
  # Three UNAMBIGUOUS bash-4 constructs. Each has exactly one meaning, none has a 3.2
  # fallback, and none can be a false positive on a gate path — see the rationale above
  # for why `mapfile` and computed negative subscripts are deliberately NOT here.
  # Comment lines are skipped: this suite DOCUMENTS these constructs in prose (the
  # paragraph above names one), and a lint that matches its own documentation is the
  # self-matching defect this repo keeps re-learning.
  # REDIRECTION, NOT A PIPE (#3685). `grep -q` exits on the first match, so under this file's
  # `set -o pipefail` the producer takes SIGPIPE and the PIPELINE returns 141 — which `if`
  # reads as NO MATCH. That is a FALSE NEGATIVE in a portability lint: measured here, the
  # single real match in test_roborev_guard_portability.sh was silently lost this way, so the
  # lint reported `0 of 4` clean while a match existed. Process substitution keeps grep's own
  # status as the verdict. This is the THIRD instance of #3685 in this branch — the first two
  # were in the annotation guard, and this one was written AFTER filing that issue.
  # THE NEEDLES ARE ASSEMBLED FROM PIECES so this lint cannot match ITS OWN pattern
  # literals. Measured: with the patterns written out, the lint flagged THIS FILE — the
  # self-matching defect the paragraph above names, committed in the very code that warns
  # about it. Comment-stripping is not enough, because these are CODE lines, not comments.
  # Each variable below holds a fragment that is harmless alone; only the concatenation is
  # the construct, and the concatenation exists solely at run time.
  _p_caret='\^'; _p_comma=','
  _p_case='\$\{[A-Za-z_][A-Za-z0-9_]*('"$_p_caret$_p_caret"'|'"$_p_comma$_p_comma"'|'"$_p_caret"'|'"$_p_comma"')\}'
  _p_amp='&'; _p_redir="$_p_amp"'>''>'
  if grep -qE < <(sed 's/#.*$//' "$SCRIPT_DIR/../../$_b32f" 2>/dev/null) \
       -e '^[[:space:]]*(declare|local|typeset)[[:space:]]+-[A-Za-z]*A' \
       -e "$_p_case" \
       -e "$_p_redir"; then
    b32_offenders="$b32_offenders $_b32f"
  fi
done <<EOF
$b32_scripts
EOF
if [ "$b32_scanned" -lt 20 ]; then
  bad "portability-8c: derived only $b32_scanned gate-invoked script(s) from $GATE — the derivation looks broken, so this lint would pass having scanned almost nothing"
elif [ -n "$b32_offenders" ]; then
  bad "portability-8c: gate-invoked script(s) use a NON-PORTABLE construct (bash-4 associative array, bash-4 case-conversion parameter expansion, or bash-4 append-redirection), which fails on macOS — a first-class gate host:$b32_offenders"
else
  ok "portability-8c: 0 of 3 RECOGNISED non-portable constructs (bash-4 associative array, bash-4 case-conversion parameter expansion, bash-4 append-redirection) found across $b32_scanned gate-invoked scripts — NOT an exhaustive portability proof: \`bash -n\` does not catch the bash-4 class (measured: rc=0 for all three) and nothing here executes under a BSD userland; only EXECUTION on a macOS host establishes either. The constructs are deliberately NOT spelled in this message: it would make the lint flag its own diagnostic."
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

# 9c-v. sccache cache-size cap + occupancy tokens (issue #3727). THE TOKEN THIS SUITE EXISTS FOR:
#       the fleet ran for months with SCCACHE_CACHE_SIZE declared in .agent-ami/profile.yaml,
#       never persisted, and every gate SUMMARY silent about the cap actually in force. Each
#       state is driven by the three #3727 hooks, so no sccache install and no PATH surgery is
#       needed; the SOURCE classification additionally reads the real SCCACHE_CACHE_SIZE, which
#       is safe precisely because MAX_BYTES short-circuits any contact with a server.
#       Rows: <SCCACHE_CACHE_SIZE>|<max_bytes>|<default_bytes>|<expected cap token value>
for scc_row in \
  '30G|32212254720|10737418240|32212254720(pinned)' \
  '|10737418240|10737418240|10737418240(default)' \
  '|32212254720|10737418240|32212254720(inherited)' \
  '30G|10737418240|10737418240|10737418240(stale)' \
  '30GiB|10737418240|10737418240|10737418240(invalid)' \
  '30GiB|32212254720|10737418240|32212254720(invalid-stale)' \
  '30GiB|5368709120|10737418240|5368709120(invalid-stale-below)'; do
  scc_val=${scc_row%%|*}; scc_rest=${scc_row#*|}
  scc_max=${scc_rest%%|*}; scc_rest=${scc_rest#*|}
  scc_dflt=${scc_rest%%|*}; scc_want=${scc_rest#*|}
  # The SOURCE word alone names the file/label: the expected token carries parentheses, and a
  # path built from it would be legal-but-hostile to read in a failure message.
  scc_src=${scc_want#*\(}; scc_src=${scc_src%\)}
  # Two rows share the `invalid-stale` TOKEN and differ only in whether the enforced cap is above
  # or below sccache's default (issue #3727 roborev round 3, f2), so the row carries a
  # `-below` suffix for the file/label and the expected token drops it.
  scc_want=${scc_want/-below)/)}
  scc_file="$tmp/scc-cap-$scc_src.txt"
  # An UNSET row must not become an EMPTY one: set-but-empty is a distinct, measured sccache
  # state (it is silently discarded), so the two are driven through different invocations.
  if [ -n "$scc_val" ]; then
    AGENT_GATE_SUMMARY_FILE="$scc_file" \
      AGENT_GATE_TEST_SCCACHE_STATE=on AGENT_GATE_TEST_SCCACHE_ERRORS=0 \
      AGENT_GATE_TEST_SCCACHE_MAX_BYTES="$scc_max" \
      AGENT_GATE_TEST_SCCACHE_USED_BYTES=1375141619 \
      AGENT_GATE_TEST_SCCACHE_DEFAULT_BYTES="$scc_dflt" \
      SCCACHE_CACHE_SIZE="$scc_val" \
      bash "$GATE" --emit-summary-selftest >/dev/null 2>"$tmp/scc-cap-$scc_src.stderr"
  else
    env -u SCCACHE_CACHE_SIZE \
      AGENT_GATE_SUMMARY_FILE="$scc_file" \
      AGENT_GATE_TEST_SCCACHE_STATE=on AGENT_GATE_TEST_SCCACHE_ERRORS=0 \
      AGENT_GATE_TEST_SCCACHE_MAX_BYTES="$scc_max" \
      AGENT_GATE_TEST_SCCACHE_USED_BYTES=1375141619 \
      AGENT_GATE_TEST_SCCACHE_DEFAULT_BYTES="$scc_dflt" \
      bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
  fi
  if accel_token_is "$scc_file" sccache-cap "$scc_want"; then
    ok "sccache-cap: SCCACHE_CACHE_SIZE='$scc_val' + server cap $scc_max -> sccache-cap=$scc_want"
  else
    bad "sccache-cap: expected sccache-cap=$scc_want for value '$scc_val' + server cap $scc_max"
    grep '^accelerators:' "$scc_file" 2>/dev/null || cat "$scc_file"
  fi
  assert_accelerators "sccache-cap-$scc_src" "$scc_file"
done

# 9c-v-b. AND THE `invalid` LABEL MAY NOT BE USED WHERE THE RUNNING CAP IS NOT THE FALLBACK
#         (issue #3727 roborev finding 3). Env-value validity and running-server provenance are
#         two INDEPENDENT axes: `invalid` asserts that the discarded value fell back to the cap
#         printed beside it, which is only true when that cap IS sccache's default. Where it is
#         not, the label would invent a causal link AND invert the remedy — stopping the server
#         would LOWER the cap, because the restart discards the value too. The row above pins the
#         positive; this pins that the weaker label is not reused, which a token-equality assert
#         cannot see on its own.
scc_isw="$tmp/scc-cap-invalid-stale.txt"
if accel_token_is "$scc_isw" sccache-cap '32212254720(invalid)' \
   || accel_token_is "$scc_isw" sccache-cap '32212254720(stale)'; then
  bad "sccache-cap: an invalid value beside a non-fallback running cap was labelled invalid/stale (the two axes collapsed)"
else
  ok "sccache-cap: an invalid value beside a NON-fallback running cap is neither invalid nor stale (axes kept apart)"
fi
# ... and the WARN must name the ordering hazard, not merely the state: an operator who stops the
# server before fixing the value LOWERS the cap. A label without that sentence is a trap.
if grep -q 'invalid-stale' "$tmp/scc-cap-invalid-stale.stderr" 2>/dev/null \
   && grep -q 'FIX THE VALUE FIRST' "$tmp/scc-cap-invalid-stale.stderr" 2>/dev/null \
   && grep -q 'would LOWER the cap' "$tmp/scc-cap-invalid-stale.stderr" 2>/dev/null; then
  ok "sccache-cap: the invalid-stale WARN names the ordering hazard (stopping the server first LOWERS the cap)"
else
  bad "sccache-cap: the invalid-stale WARN does not warn that stopping the server first lowers the cap"
  cat "$tmp/scc-cap-invalid-stale.stderr" 2>/dev/null | head -3
fi

# 9c-v-c. THE DIRECTION OF THE CAP CHANGE IS COMPUTED, NOT ASSUMED (issue #3727 roborev round 3).
#         The invalid-stale WARN tells the operator that a bare `sccache --stop-server` replaces the
#         enforced cap with sccache's FALLBACK; which WAY that moves depends on whether the running
#         cap is above or below the default, and the text used to say "LOWER" unconditionally. Both
#         rows above are re-read here, so a hard-coded direction reds whichever arm it is wrong for.
if grep -q 'would LOWER the cap' "$tmp/scc-cap-invalid-stale.stderr" 2>/dev/null \
   && ! grep -q 'would RAISE the cap' "$tmp/scc-cap-invalid-stale.stderr" 2>/dev/null; then
  ok "sccache-cap: an enforced cap ABOVE sccache's default warns that a restart would LOWER it"
else
  bad "sccache-cap: the above-default arm did not warn about LOWERING the cap"
  cat "$tmp/scc-cap-invalid-stale.stderr" 2>/dev/null | head -3
fi
if grep -q 'would RAISE the cap' "$tmp/scc-cap-invalid-stale-below.stderr" 2>/dev/null \
   && ! grep -q 'would LOWER the cap' "$tmp/scc-cap-invalid-stale-below.stderr" 2>/dev/null; then
  ok "sccache-cap: an enforced cap BELOW sccache's default warns that a restart would RAISE it (the direction is read from the comparison)"
else
  bad "sccache-cap: the below-default arm still claimed the restart would LOWER the cap"
  cat "$tmp/scc-cap-invalid-stale-below.stderr" 2>/dev/null | head -3
fi

# 9c-v-d. NO HARDCODED DEFAULT, AND A MISSING ONE DISCARDS ONLY THE LABELS THAT NEED IT (issue
#         #3727 roborev rounds 6 f2 and 7 f3). The default used to be a constant measured on sccache
#         0.17.0 while the fleet installs sccache UNVERSIONED, so another build's default would have
#         mislabelled `default` as `inherited` and `invalid` as `invalid-stale`, restart guidance
#         included. It is measured per emit now — and round 7 corrected the ORDER: `pinned`/`stale`
#         compare the CONFIGURED value against the ENFORCED cap and need no default at all, so a
#         failed default probe must not discard provenance that WAS established. Both halves are
#         pinned here: unset ⇒ unattributed (the label genuinely needs the default), valid value ⇒
#         still classified.
scc_nodflt="$tmp/scc-cap-nodefault.txt"
env -u SCCACHE_CACHE_SIZE \
  AGENT_GATE_SUMMARY_FILE="$scc_nodflt" \
  AGENT_GATE_TEST_SCCACHE_STATE=on AGENT_GATE_TEST_SCCACHE_ERRORS=0 \
  AGENT_GATE_TEST_SCCACHE_MAX_BYTES=32212254720 AGENT_GATE_TEST_SCCACHE_USED_BYTES=1375141619 \
  AGENT_GATE_TEST_SCCACHE_DEFAULT_BYTES=unknown \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>"$tmp/scc-cap-nodefault.stderr"
if accel_token_is "$scc_nodflt" sccache-cap '32212254720(unattributed)' \
   && ! accel_token_is "$scc_nodflt" sccache-cap '32212254720(inherited)'; then
  ok "sccache-cap: with the variable UNSET and sccache's default unmeasurable, the cap is (unattributed) — no constant stands in for it, and (inherited) is not guessed"
else
  bad "sccache-cap: an unknown default still produced a default-relative label"
  grep '^accelerators:' "$scc_nodflt" 2>/dev/null || cat "$scc_nodflt"
fi
if [ ! -s "$tmp/scc-cap-nodefault.stderr" ] || ! grep -q 'WARN: sccache-cap' "$tmp/scc-cap-nodefault.stderr"; then
  ok "sccache-cap: with the default unknown, no WARN quotes a default it does not have"
else
  bad "sccache-cap: a WARN fired while sccache's default was unknown"
  cat "$tmp/scc-cap-nodefault.stderr" | head -3
fi
assert_accelerators "sccache-cap-nodefault" "$scc_nodflt"
scc_nodflt2="$tmp/scc-cap-nodefault-pinned.txt"
AGENT_GATE_SUMMARY_FILE="$scc_nodflt2" \
  AGENT_GATE_TEST_SCCACHE_STATE=on AGENT_GATE_TEST_SCCACHE_ERRORS=0 \
  AGENT_GATE_TEST_SCCACHE_MAX_BYTES=32212254720 AGENT_GATE_TEST_SCCACHE_USED_BYTES=1375141619 \
  AGENT_GATE_TEST_SCCACHE_DEFAULT_BYTES=unknown SCCACHE_CACHE_SIZE=30G \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
if accel_token_is "$scc_nodflt2" sccache-cap '32212254720(pinned)'; then
  ok "sccache-cap: a VALID configured value still classifies as (pinned) with sccache's default unmeasurable — the missing default discards only default-relative labels"
else
  bad "sccache-cap: a failed default probe discarded provenance that was established (round 7 f3)"
  grep '^accelerators:' "$scc_nodflt2" 2>/dev/null || cat "$scc_nodflt2"
fi

# 9c-v-e. THE NEAR-CAPACITY REMEDY MUST NOT CONTRADICT THE NEIGHBOURING WARN (issue #3727 roborev
#         round 7, f2). In the migration case this whole issue is about — the environment already at
#         50G while the running server still enforces 10G — the `stale` WARN correctly says to
#         restart WITHOUT editing the value, and the fill WARN used to say "raise
#         SCCACHE_CACHE_SIZE" one line later. Two adjacent warnings giving opposite advice is worse
#         than one, so the remedy is derived from the SOURCE.
scc_mig="$tmp/scc-fill-stale.txt"
AGENT_GATE_SUMMARY_FILE="$scc_mig" \
  AGENT_GATE_TEST_SCCACHE_STATE=on AGENT_GATE_TEST_SCCACHE_ERRORS=0 \
  AGENT_GATE_TEST_SCCACHE_MAX_BYTES=10737418240 AGENT_GATE_TEST_SCCACHE_USED_BYTES=10737418240 \
  AGENT_GATE_TEST_SCCACHE_DEFAULT_BYTES=10737418240 SCCACHE_CACHE_SIZE=50G \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>"$tmp/scc-fill-stale.stderr"
if grep -q 'Do NOT edit the value' "$tmp/scc-fill-stale.stderr" 2>/dev/null \
   && ! grep -q 'Raise SCCACHE_CACHE_SIZE' "$tmp/scc-fill-stale.stderr" 2>/dev/null; then
  ok "sccache-cap: at capacity with a STALE server the fill WARN says restart, and does NOT contradict it by telling the operator to raise a value that is already larger"
else
  bad "sccache-cap: the fill WARN contradicted the stale WARN in the migration case"
  grep 'WARN' "$tmp/scc-fill-stale.stderr" 2>/dev/null | head -2
fi
scc_small="$tmp/scc-fill-pinned.txt"
AGENT_GATE_SUMMARY_FILE="$scc_small" \
  AGENT_GATE_TEST_SCCACHE_STATE=on AGENT_GATE_TEST_SCCACHE_ERRORS=0 \
  AGENT_GATE_TEST_SCCACHE_MAX_BYTES=32212254720 AGENT_GATE_TEST_SCCACHE_USED_BYTES=32212254720 \
  AGENT_GATE_TEST_SCCACHE_DEFAULT_BYTES=10737418240 SCCACHE_CACHE_SIZE=30G \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>"$tmp/scc-fill-pinned.stderr"
if grep -q 'Raise SCCACHE_CACHE_SIZE' "$tmp/scc-fill-pinned.stderr" 2>/dev/null \
   && ! grep -q 'Do NOT edit the value' "$tmp/scc-fill-pinned.stderr" 2>/dev/null; then
  ok "sccache-cap: at capacity with a PINNED cap the remedy IS to raise the value (the source-aware branch keeps the useful advice)"
else
  bad "sccache-cap: a genuinely too-small pinned cap lost its raise-the-value advice"
  grep 'WARN' "$tmp/scc-fill-pinned.stderr" 2>/dev/null | head -2
fi

# 9c-v-f. "THE SHELL CANNOT CLASSIFY THIS" IS NOT "SCCACHE DISCARDS THIS" (issue #3727 roborev
#         round 8, f2 — retiring a residual this suite declared twice instead of fixing). The
#         classifier bounded the digits it would multiply and returned INVALID for anything longer,
#         but sccache does NOT uniformly discard those: measured, a 21-digit value falls back to the
#         default while a 19-digit one WRAPS and is ACCEPTED (9999999999999999999G ->
#         2484298143374508032). So an accepted cap was being labelled `invalid`/`invalid-stale`, with
#         the wrong remediation attached. Such values now report unclassified provenance and no WARN.
scc_wrap="$tmp/scc-cap-unclassifiable.txt"
AGENT_GATE_SUMMARY_FILE="$scc_wrap" \
  AGENT_GATE_TEST_SCCACHE_STATE=on AGENT_GATE_TEST_SCCACHE_ERRORS=0 \
  AGENT_GATE_TEST_SCCACHE_MAX_BYTES=2484298143374508032 AGENT_GATE_TEST_SCCACHE_USED_BYTES=1 \
  AGENT_GATE_TEST_SCCACHE_DEFAULT_BYTES=10737418240 SCCACHE_CACHE_SIZE=9999999999999999999G \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>"$tmp/scc-cap-unclassifiable.stderr"
if accel_token_is "$scc_wrap" sccache-cap '2484298143374508032(unattributed)' \
   && ! accel_token_is "$scc_wrap" sccache-cap '2484298143374508032(invalid)' \
   && ! accel_token_is "$scc_wrap" sccache-cap '2484298143374508032(invalid-stale)'; then
  ok "sccache-cap: a value bash cannot classify (19 digits, which sccache WRAPS and accepts) reports unclassified provenance, never (invalid)"
else
  bad "sccache-cap: an sccache-ACCEPTED cap was labelled invalid because bash could not multiply it"
  grep '^accelerators:' "$scc_wrap" 2>/dev/null || cat "$scc_wrap"
fi
if [ ! -s "$tmp/scc-cap-unclassifiable.stderr" ] || ! grep -q 'WARN: sccache-cap' "$tmp/scc-cap-unclassifiable.stderr"; then
  ok "sccache-cap: no WARN prescribes a remedy for a value whose effect this gate could not establish"
else
  bad "sccache-cap: a WARN gave remediation for an unclassifiable value"
  cat "$tmp/scc-cap-unclassifiable.stderr" | head -3
fi
assert_accelerators "sccache-cap-unclassifiable" "$scc_wrap"

# 9c-vi. THE UNMEASURABLE STATE HAS ITS OWN TOKEN, and `0` is not an all-clear. A cap that could
#        not be read must never render blank, never render 0, and never be mistaken for a measured
#        value — this repo's standing rule that a positive verdict requires an affirmative
#        measurement, applied to a token an agent reads out of a pasted block.
scc_unm="$tmp/scc-cap-unmeasured.txt"
AGENT_GATE_SUMMARY_FILE="$scc_unm" \
  AGENT_GATE_TEST_SCCACHE_STATE=on AGENT_GATE_TEST_SCCACHE_ERRORS=0 \
  AGENT_GATE_TEST_SCCACHE_MAX_BYTES=unmeasured AGENT_GATE_TEST_SCCACHE_USED_BYTES=unmeasured \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
if accel_token_is "$scc_unm" sccache-cap 'unmeasured(no-stats)' \
   && accel_token_is "$scc_unm" sccache-used 'unmeasured(no-stats)'; then
  ok "sccache-cap: an unreadable probe renders unmeasured(no-stats) for BOTH tokens"
else
  bad "sccache-cap: an unreadable probe did not render the explicit unmeasurable token"
  grep '^accelerators:' "$scc_unm" 2>/dev/null || cat "$scc_unm"
fi
scc_wrong_hits=0
for scc_wrong in 0 '' unknown 'unmeasured' '0(pinned)'; do
  if accel_token_is "$scc_unm" sccache-cap "$scc_wrong"; then
    bad "sccache-cap: unmeasurable state wrongly matched sccache-cap=$scc_wrong (0/blank read as a value)"
    scc_wrong_hits=$((scc_wrong_hits + 1))
  fi
done
if [ "$scc_wrong_hits" = 0 ]; then
  ok "sccache-cap: unmeasurable state matches NONE of 0/blank/unknown/bare-unmeasured/0(pinned)"
fi
assert_accelerators "sccache-cap-unmeasured" "$scc_unm"

# 9c-vii. sccache NOT in use -> both capacity tokens are na, exactly as sccache-health is. A
#         probe with nothing to probe must say so rather than reporting a cap of 0.
scc_na="$tmp/scc-cap-na.txt"
AGENT_GATE_SUMMARY_FILE="$scc_na" AGENT_GATE_TEST_SCCACHE_STATE=off \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
if accel_token_is "$scc_na" sccache-cap 'na(sccache-not-in-use)' \
   && accel_token_is "$scc_na" sccache-used 'na(sccache-not-in-use)' \
   && accel_health_token_is "$scc_na" na; then
  ok "sccache-cap: sccache not in use -> cap/used/health all na"
else
  bad "sccache-cap: expected na cap/used/health tokens when sccache is not in use"
  grep '^accelerators:' "$scc_na" 2>/dev/null || cat "$scc_na"
fi

# 9c-viii. OCCUPANCY AND THE FILL PERCENTAGE, including the at-capacity marker and the measured
#          legal cap of 0 (`SCCACHE_CACHE_SIZE=0G` yields `0 bytes`, so a percentage is undefined
#          there and must be named rather than divided).
scc_full="$tmp/scc-used-full.txt"
AGENT_GATE_SUMMARY_FILE="$scc_full" \
  AGENT_GATE_TEST_SCCACHE_STATE=on AGENT_GATE_TEST_SCCACHE_ERRORS=0 \
  AGENT_GATE_TEST_SCCACHE_MAX_BYTES=10737418240 AGENT_GATE_TEST_SCCACHE_USED_BYTES=10737418240 \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>"$tmp/scc-used-full.stderr"
if accel_token_is "$scc_full" sccache-used '10737418240(100%)'; then
  ok "sccache-used: a cache at its cap renders 100%"
else
  bad "sccache-used: expected sccache-used=10737418240(100%) for a cache at its cap"
  grep '^accelerators:' "$scc_full" 2>/dev/null || cat "$scc_full"
fi
if grep -q 'WARN:.*sccache' "$tmp/scc-used-full.stderr"; then
  ok "sccache-used: a full cache emits a LOUD WARN (eviction/thrash is actionable)"
else
  bad "sccache-used: no WARN for a cache at 100% of its cap"
  echo "------- stderr -------"; cat "$tmp/scc-used-full.stderr"; echo "----------------------"
fi
scc_zero="$tmp/scc-used-zero.txt"
AGENT_GATE_SUMMARY_FILE="$scc_zero" \
  AGENT_GATE_TEST_SCCACHE_STATE=on AGENT_GATE_TEST_SCCACHE_ERRORS=0 \
  AGENT_GATE_TEST_SCCACHE_MAX_BYTES=0 AGENT_GATE_TEST_SCCACHE_USED_BYTES=0 \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
if accel_token_is "$scc_zero" sccache-used '0(cap-zero)'; then
  ok "sccache-used: a legal zero cap names the undefined percentage instead of dividing"
else
  bad "sccache-used: expected sccache-used=0(cap-zero) for a zero cap"
  grep '^accelerators:' "$scc_zero" 2>/dev/null || cat "$scc_zero"
fi

# 9c-ix. `sccache-health` IS AN ERROR-COUNTER TOKEN AND CANNOT BE CLEARED BY A CAP RAISE (#3727).
#        Stated as a TEST rather than only as a comment: the two signals are independent, so a
#        full cache with zero error counters must report health=ok beside used=100%, and a warn
#        must survive a generous cap. Anyone who later wires occupancy INTO _sccache_health reds
#        here, which is the point — the remedies differ (inspect/reset vs raise the cap).
if accel_health_token_is "$scc_full" ok; then
  ok "sccache-health: a cache at 100% of its cap with zero error counters is still health=ok (capacity is NOT a health input)"
else
  bad "sccache-health: occupancy leaked into the error-counter token (#3727 conflation)"
fi
scc_bigwarn="$tmp/scc-health-bigcap.txt"
AGENT_GATE_SUMMARY_FILE="$scc_bigwarn" \
  AGENT_GATE_TEST_SCCACHE_STATE=on AGENT_GATE_TEST_SCCACHE_ERRORS=3 \
  AGENT_GATE_TEST_SCCACHE_MAX_BYTES=32212254720 AGENT_GATE_TEST_SCCACHE_USED_BYTES=1 \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
if accel_health_token_is "$scc_bigwarn" warn; then
  ok "sccache-health: a generous cap and an empty cache do NOT clear a non-zero error counter"
else
  bad "sccache-health: a cap raise silenced the error-counter token (#3727 conflation)"
fi

# 9c-x. A CAP NOBODY IS PROVEN TO ENFORCE MAY NOT READ AS `pinned` (issue #3727). MEASURED: with
#       no sccache server running, `--show-stats` does not start one and answers `max_cache_size`
#       from the CLIENT's own resolution of SCCACHE_CACHE_SIZE — so the value is echoed straight
#       back, and calling that `pinned` asserts enforcement by a server that does not exist.
#       Attribution is decided by a DIFFERENTIAL (a second read with a sentinel value: a running
#       server's answer does not move, a client's does), forced here by the third hook.
#       `unknown` must land in the same place as `no`: only an affirmative yes may license the
#       other labels.
for scc_attr in no unknown; do
  scc_unattr="$tmp/scc-cap-unattributed-$scc_attr.txt"
  AGENT_GATE_SUMMARY_FILE="$scc_unattr" \
    AGENT_GATE_TEST_SCCACHE_STATE=on AGENT_GATE_TEST_SCCACHE_ERRORS=0 \
    AGENT_GATE_TEST_SCCACHE_MAX_BYTES=32212254720 AGENT_GATE_TEST_SCCACHE_USED_BYTES=1375141619 \
    AGENT_GATE_TEST_SCCACHE_DEFAULT_BYTES=10737418240 AGENT_GATE_TEST_SCCACHE_ATTRIBUTED="$scc_attr" \
    SCCACHE_CACHE_SIZE=30G \
    bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
  if accel_token_is "$scc_unattr" sccache-cap '32212254720(unattributed)'; then
    ok "sccache-cap: attribution '$scc_attr' renders (unattributed), never (pinned)"
  else
    bad "sccache-cap: attribution '$scc_attr' did not render (unattributed)"
    grep '^accelerators:' "$scc_unattr" 2>/dev/null || cat "$scc_unattr"
  fi
  if accel_token_is "$scc_unattr" sccache-cap '32212254720(pinned)'; then
    bad "sccache-cap: an unattributed cap read as (pinned) — enforcement asserted with no server proven"
  fi
  assert_accelerators "sccache-cap-unattributed-$scc_attr" "$scc_unattr"
done

# 9c-xi. AND A NULL `cache_size` IS *NOT* AN ATTRIBUTION SIGNAL — the correction that cost a round
#        (issue #3727). A RUNNING server with an EMPTY cache reports `"cache_size":null` exactly as
#        a client with no server does; measured by starting a real server at 40G on a private port
#        and reading it back (cap 42949672960, size null), the two payloads differing only in their
#        values. So a null size must leave the cap's classification ALONE and show up only in the
#        occupancy token. A test keyed the other way was green against a stub that shared the
#        code's premise — which is why this one asserts the two axes move independently.
scc_nullsize="$tmp/scc-cap-nullsize.txt"
AGENT_GATE_SUMMARY_FILE="$scc_nullsize" \
  AGENT_GATE_TEST_SCCACHE_STATE=on AGENT_GATE_TEST_SCCACHE_ERRORS=0 \
  AGENT_GATE_TEST_SCCACHE_MAX_BYTES=32212254720 AGENT_GATE_TEST_SCCACHE_USED_BYTES=null \
  AGENT_GATE_TEST_SCCACHE_DEFAULT_BYTES=10737418240 SCCACHE_CACHE_SIZE=30G \
  bash "$GATE" --emit-summary-selftest >/dev/null 2>&1
if accel_token_is "$scc_nullsize" sccache-cap '32212254720(pinned)' \
   && accel_token_is "$scc_nullsize" sccache-used 'unmeasured(no-size)'; then
  ok "sccache-cap: a null cache_size on an ATTRIBUTED cap stays (pinned) and only the occupancy is unmeasured(no-size)"
else
  bad "sccache-cap: a null cache_size leaked into the cap's classification (the two axes are collapsed again)"
  grep '^accelerators:' "$scc_nullsize" 2>/dev/null || cat "$scc_nullsize"
fi
assert_accelerators "sccache-cap-nullsize" "$scc_nullsize"

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
# TWO skips per skip branch, because the run branch below emits TWO verdicts (the
# perf-token assert and the accelerators-line assert). A single skip for a two-verdict
# section under-accounts by one, which at an exact ASSERT_FLOOR is a FALSE RED on Darwin
# or on a hardened box where /proc/sys/kernel/{perf_event_paranoid,kptr_restrict} is
# masked (issue #1465 round 14, Y1). Same 1:1 rule as the r18/r32/featoracle sites.
if [ "$perf_host_os" != Linux ]; then
  skipped "perf-host[token]: host is $perf_host_os, not Linux — perf_event_paranoid/kptr_restrict are Linux controls (9f-darwin covers the no-token contract)"
  skipped "perf-host[accelerators-line]: host is $perf_host_os, not Linux — the derived-token accelerators line was NOT verified here"
elif [ ! -r "$perf_host_par_f" ] || [ ! -r "$perf_host_kptr_f" ]; then
  skipped "perf-host[token]: $perf_host_par_f / $perf_host_kptr_f unreadable on this box — no real state to derive an expectation from"
  skipped "perf-host[accelerators-line]: $perf_host_par_f / $perf_host_kptr_f unreadable — the derived-token accelerators line was NOT verified here"
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
if out_has "$accel_fn_text" '_perf_accel_token_into' \
   && ! out_has "$accel_fn_text" '\$(_perf_accel_token\|`_perf_accel_token'; then
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
      # Routed through skipped(), ONCE PER DISPLACED VERDICT (Y2): this branch skips both
      # the behaviour and the complement assert, and a bare `echo` incremented nothing at
      # all — invisible to the accounting rather than merely miscounted.
      skipped "1699-featoracle-behaviour: cargo present but the resolve returned nothing (offline registry?) — NOT verified here"
      skipped "1699-featoracle-complement: cargo present but the resolve returned nothing (offline registry?) — NOT verified here"
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
    skipped "1699-featoracle-behaviour: cargo not available — the dependent-only-feature regression was NOT verified here"
    skipped "1699-featoracle-complement: cargo not available — the enabled-feature complement was NOT verified here"
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
  # THIS EXPECTATION IS REVERSED, and the reversal is the point (roborev round-25, Medium).
  #
  # Round 17 added this as a complement — "some components legitimately produce no test-result
  # lines, and reddening there would be a false red" — and that premise was never measured. It is
  # false for every caller of THIS guard, and while it stood it LICENSED the hole round 25 found:
  # a log with no parseable `test result:` line left every counter at zero and the guard returned
  # SUCCESS, so a truncated log, a killed cargo, a changed output format or a failed ANSI
  # normalisation all read as "nothing wrong".
  #
  # MEASURED against real component logs before flipping it: `flight-tests` 2 result lines,
  # `cli-tests` 32, `legacy-heuristics` likewise non-zero — every caller runs cargo test to a
  # successful exit, so results always exist. The `--no-run` isolation lanes do not call this guard.
  #
  # The general lesson, which is why this comment is long: a complement assert is only as good as
  # the premise it encodes, and an UNMEASURED complement is a licence for the permissive branch it
  # protects. Round 17's complement caught a real over-reach at the time (an affirmative check that
  # redded a legitimate `--lib`-only log — the case immediately above), so it was not wrong to add;
  # it was wrong to state its scope more broadly than had been measured.
  nrl="$tmp/1699-r17-noresults.log"
  printf '   Compiling foo v0.1.0\n    Finished test profile\n' > "$nrl"
  if ( . "$zn_h"; check_no_unexpected_zero_tests "noresults" "$nrl" >/dev/null 2>&1 ); then
    bad "1699-r25-noresults: a log with NO parseable 'test result:' line PASSED — the guard judged zero targets and reported OK, which is the vacuous pass (a truncated log, a killed cargo, a changed format or a failed ANSI strip all land here)"
  else
    ok "1699-r25-noresults: a log with NO parseable 'test result:' line is a FAIL — every caller of this guard has just run cargo test to a successful exit, so results must exist (measured: flight-tests 2, cli-tests 32)"
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
  if out_has "$meta_fns_" -xF "$must_"; then
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
# The spec rows live in ONE variable consumed by BOTH branches (round 13, X2): the skip
# path must emit one `skipped` per verdict it DISPLACES, or ASSERT_FLOOR — a count of
# accounted assertions — reds a host that merely lacks jq/cargo. Deriving the skip from the
# same list keeps the accounting 1:1 by construction rather than by a hand-kept number.
PARSER_DIFF_SPEC_ROWS='_package_unittest_srcs|cqlite-flight|lib,bin
_package_test_targets_gated|cqlite-core|legacy-heuristics'
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
  done <<EOF_PARSER_DIFF_SPECS
$PARSER_DIFF_SPEC_ROWS
EOF_PARSER_DIFF_SPECS
else
  # ONE skip per DISPLACED verdict, over the same rows the branch above would have
  # iterated (X2): a single skip for a two-verdict section under-accounts by one.
  while IFS='|' read -r fn_ _a1_ _a2_; do
    [ -n "$fn_" ] || continue
    skipped "1699-r18-diff-$fn_: needs jq + python3 + cargo on this host — this parser pair was NOT differentially compared here"
  done <<EOF_PARSER_DIFF_SKIP
$PARSER_DIFF_SPEC_ROWS
EOF_PARSER_DIFF_SKIP
fi

# roborev round-18 (Low): the cli-tests component cleaned its two logs but not the
# `.ansi-stripped` copies the zero-test guards parse, leaking two files per gate run into
# TMPDIR. Structural, because the component itself is a 5-minute cargo run: the trap line is
# the whole of the fix, so pinning the trap line is pinning the fix.
cli_body_="$tmp/1699-r18-cli-trap.txt"
awk '/^    cli-tests\)/, /compaction-byte-parity\)/' "$GATE" > "$cli_body_"
if [ ! -s "$cli_body_" ]; then
  bad "1699-r18-cli-trap-scope: could not extract the cli-tests component — this assert would pass vacuously"
elif [ "$(grep -cE 'mktemp -d .*agent-gate-cli' "$cli_body_")" -gt 0 ] \
  && [ "$(grep -cE 'trap "rm -rf .*_cli_tmp' "$cli_body_")" -gt 0 ]; then
  # SUPERSEDES the round-18 form of this assert. That one required the trap to name
  # `$log1.ansi-stripped`/`$log2.ansi-stripped` explicitly, which fixed the LEAK but left the
  # round-31 hazard: `_ansi_stripped_log` writes a PREDICTABLE sibling of its input, and these logs
  # sat in the shared tmp for minutes, so another local user could pre-create that sibling as a
  # symlink and have the guard's `sed` overwrite any file the gate user can write. A private
  # `mktemp -d` closes both at once — nothing to enumerate, nothing guessable — so the assert now
  # pins the DIRECTORY rather than the two derived filenames.
  ok "1699-r31-cli-private-dir: cli-tests logs into a private mktemp -d and removes it wholesale (so the predictable .ansi-stripped sibling is neither guessable nor leaked)"
else
  bad "1699-r31-cli-private-dir: cli-tests is back to bare mktemp files in the shared tmp — _ansi_stripped_log writes a PREDICTABLE sibling there, which another local user can pre-create as a symlink for the guard's sed to follow"
fi

# --- 32. #1699: the census must not name the WRONG RUNNER (roborev round-20, Medium) ---
#
# The census said "WHO DOES RUN THEM: CI's Flight tier … cargo test --package cqlite-flight".
# That is FALSE for a target whose whole crate is gated by an inner `#![cfg(feature = "X")]` with
# X off: it compiles, runs ZERO tests and exits 0 in CI exactly as it would here. cqlite-flight's
# `default` is EMPTY, and MEASURED on this corpus 15 of its 42 test targets carry such a gate — 14
# on `observability-testing`, which this gate enables NOWHERE (its only two occurrences here are in
# comments), and one on `dhat-heap`, which the `memory-budget` component DOES enable.
#
# A census that names the wrong runner is worse than one that admits ignorance, because it closes
# the question — which is the whole failure mode this lane exists to prevent, arriving inside the
# lane's own output. So the two populations are reported separately and the CI claim is scoped.
#
# STRUCTURAL, over the extracted run_flight_tests body: the behavioural form is `--only flight-tests`,
# a multi-minute cargo run that does not belong in a sub-second self-test (it was run by hand, and
# its numbers are in docs/reports/ah6-1699-feature-matrix-lanes.md).
fl_body_="$tmp/1699-r20-flight.txt"
awk '/^run_flight_tests\(\) \{/, /^\}/' "$GATE" > "$fl_body_"
if [ ! -s "$fl_body_" ]; then
  bad "1699-r20-census-scope: could not extract run_flight_tests — every assert below would pass vacuously"
else
  # The unscoped claim must be GONE: it is the defect, and its absence is the fix.
  if [ "$(grep -cF 'WHO DOES RUN THEM' "$fl_body_")" -eq 0 ]; then
    ok "1699-r20-census-unscoped-gone: the census no longer claims a single runner for ALL omitted targets"
  else
    bad "1699-r20-census-unscoped-gone: the census still says 'WHO DOES RUN THEM' of the whole omitted set — false for the crate-gated targets, which run in NO tier (#3375)"
  fi
  # 'run by another component', not 'enabled by' — round 21's whole finding was that ENABLING a
  # feature is not RUNNING the target (memory-budget enables dhat-heap but selects ONE target by
  # name), so the wording was tightened along with the predicate. This assert caught the rename,
  # which is what it is for; the phrase it now pins is the one that is true.
  for needle_ in 'contain an INNER cfg attribute' '#3375' 'WHO RUNS THE REST' 'DOES NOT CLASSIFY THEM' 'needs a Rust parser'; do
    if [ "$(grep -cF -- "$needle_" "$fl_body_")" -gt 0 ]; then
      ok "1699-r20-census-element: the census states '$needle_'"
    else
      bad "1699-r20-census-element: the census no longer states '$needle_' — the omission it describes would go back to being silent"
    fi
  done
  # Counts must be DERIVED, never written down. A literal 14/15/27 in the body is the curated-count
  # defect this lane was built to avoid, and it would go stale the moment a target is added.
  # POSITIVE form: require the VARIABLE on each counting line, rather than forbidding a digit
  # somewhere near a phrase. The first cut did the latter, anchored the digits on the wrong side of
  # the phrase, and a planted literal `OF THOSE, 14 EXECUTE NOWHERE` sailed through it (RED-verified
  # as a MISS). Forbidding a bad spelling is guesswork about where the badness will appear;
  # requiring the derivation is a statement about what must be true.
  derived_ok_=1
  grep -F 'contain an INNER cfg attribute' "$fl_body_" | grep -q '\$gated_n' || derived_ok_=0
  if [ "$derived_ok_" -eq 1 ]; then
    ok "1699-r20-census-derived: the counting line interpolates its DERIVED variable (\$gated_n)"
  else
    bad "1699-r20-census-derived: a census count is no longer interpolated from its derived variable — a literal there goes stale the moment a target is added or a feature joins default, silently: $(grep -nE 'EXECUTE NOWHERE|WHO RUNS THE REMAINING' "$fl_body_" | head -2 | tr '\n' ' ')"
  fi
fi

# The predicate that classifies "runs in another component" must not use `… | grep -q`. Under this
# script's `set -uo pipefail` that returns 141 ON A SUCCESSFUL MATCH (grep exits at the first hit,
# the upstream dies of SIGPIPE), so the predicate reads FALSE exactly when the answer is TRUE.
# THIS BIT THE FUNCTION ON ITS FIRST RUN: the same pattern matched standalone and returned false
# inside the gate, classifying a target `memory-budget` does run as executing nowhere. #3380 is the
# instance that cost this PR a review round; #3387 tracks the ~696 other sites.
# THE LIVE PREDICATE, and a guard against the dead one coming back (roborev round-22, Medium).
# This assert used to extract `_feature_enabled_by_some_component`, which round 21 REPLACED — and
# the replaced function was still sitting in the gate as dead code, so the assert kept passing while
# the predicate actually in use went unchecked. A vacuous pass, in the self-test of the PR about
# vacuous passes, created by superseding a function without deleting it. Both halves are now pinned:
# the live predicate is linted, and the superseded name must not exist at all.
pred_="$tmp/1699-r20-pred.txt"
awk '/^_crate_gated_test_targets\(\) \{/, /^\}/' "$GATE" > "$pred_"
if [ ! -s "$pred_" ]; then
  bad "1699-r20-pipefail-scope: could not extract _crate_gated_test_targets — this assert would pass vacuously"
elif [ "$(sed 's/[[:space:]]*#.*$//' "$pred_" | grep -cE '\|[[:space:]]*grep[^|]*-[a-zA-Z]*q')" -eq 0 ]; then
  ok "1699-r20-pipefail: _crate_gated_test_targets contains no '| grep -q' (which returns 141 on a successful match under pipefail)"
else
  bad "1699-r20-pipefail: '| grep -q' is back in the predicate — under pipefail it reports 141 on a SUCCESSFUL match, so the predicate answers FALSE precisely when it should answer TRUE (#3380/#3387)"
fi

# --- 33. #1699: the crate-gate census reports the OBSERVATION, verbatim (rounds 20-27) ---
#
# The classifier is GONE (see _crate_gated_test_targets for the five rounds of findings that
# retired it). What remains must (a) report every crate-level gate form INCLUDING `cfg_attr`, which
# r27 found the classifier missing, (b) print the gate text rather than a verdict, and (c) never
# reintroduce the classification identifiers — because as long as they exist an assert can bind to
# them instead of to the code that runs (round 22's lesson, learned the hard way).
gg_body_="$tmp/1699-r27-gated.sh"
awk '/^_crate_gated_test_targets\(\) \{/, /^\}/' "$GATE" > "$gg_body_"
if [ ! -s "$gg_body_" ]; then
  bad "1699-r27-verbatim-scope: could not extract _crate_gated_test_targets — every case below would pass vacuously"
else
  gg_src_="$tmp/1699-r27-src"; mkdir -p "$gg_src_"
  while IFS='|' read -r case_ gate_ want_; do
    [ -n "$case_" ] || continue
    if [ "$gate_" = "NONE" ]; then
      printf '//! prose header\nfn t() {}\n' > "$gg_src_/$case_.rs"
    else
      printf '//! prose header\n%s\nfn t() {}\n' "$gate_" > "$gg_src_/$case_.rs"
    fi
    got_=$(
      export GG_SRC="$gg_src_/$case_.rs" GG_REL="tests/$case_.rs"
      _package_test_targets_gated() { printf '%s\t%s\t%s\t%s\n' "${case_}_target" "$GG_SRC" source "$GG_REL"; }
      # shellcheck disable=SC1090
      . "$gg_body_"
      _crate_gated_test_targets somepkg 2>/dev/null | awk -F'\t' '{print $3}'
    )
    got_=${got_:-ABSENT}
    # DESCOPED CONTRACT (round 42): occurrences as `L<line>: <text>`, not a crate-level verdict.
    if [ "$want_" = "ABSENT" ]; then
      if [ -z "$got_" ] || [ "$got_" = "ABSENT" ]; then ok "1699-r42-occurrence[$case_]: an ungated file reports nothing"
      else bad "1699-r42-occurrence[$case_]: reported '$got_' for a file with no inner cfg attribute"; fi
    else
      case "$got_" in
        L*:*) ok "1699-r42-occurrence[$case_]: reported with line number — '$got_'" ;;
        *) bad "1699-r42-occurrence[$case_]: reported '$got_', expected an L<line>:-prefixed occurrence" ;;
      esac
    fi
  done <<'VERBATIM_CASES'
plain_cfg|#![cfg(feature = "x")]|#![cfg(feature = "x")]
cfg_attr|#![cfg_attr(feature = "x", cfg(feature = "y"))]|#![cfg_attr(feature = "x", cfg(feature = "y"))]
negation|#![cfg(not(feature = "x"))]|#![cfg(not(feature = "x"))]
disjunction|#![cfg(any(feature = "x", feature = "y"))]|#![cfg(any(feature = "x", feature = "y"))]
indented|    #![cfg(feature = "x")]|#![cfg(feature = "x")]
ungated|NONE|ABSENT
VERBATIM_CASES
  # A MODULE-LEVEL inner attribute is NOT a crate gate (roborev round-35, Low). `#![cfg(...)]` is
  # legal inside an inline module, where it gates that module only — reporting it as a crate-level
  # gate overstates the census in the same "names something false" direction round 20 opened.
  printf '//! prose\nfn item() {}\nmod m {\n    #![cfg(feature = "x")]\n    fn inner() {}\n}\n' > "$gg_src_/modattr.rs"
  got_=$(
    export GG_SRC="$gg_src_/modattr.rs" GG_REL="tests/modattr.rs"
    _package_test_targets_gated() { printf '%s\t%s\t%s\t%s\n' modattr_target "$GG_SRC" source "$GG_REL"; }
    # shellcheck disable=SC1090
    . "$gg_body_"
    _crate_gated_test_targets somepkg 2>/dev/null | awk -F'\t' '{print $3}'
  )
  # REVERSED BY THE ROUND-42 DESCOPE, deliberately. Distinguishing a module-level inner attribute
  # from a crate-level one needs a Rust parser; five rounds proved a line scan cannot. So it IS
  # reported, as an OCCURRENCE with its line number, and the census says in the same breath that
  # crate-level-ness is not claimed. Reporting a superset with a stated limitation is honest;
  # claiming to have excluded module-level attributes was not.
  case "$got_" in
    L*'#![cfg(feature = "x")]'*)
      ok "1699-r42-module-attr: a module-level inner attribute is reported as an OCCURRENCE (L<line>), with crate-level-ness explicitly not claimed" ;;
    *)
      bad "1699-r42-module-attr: reported '$got_' — the occurrence should still be reported with its line number so a reader can open the file and judge" ;;
  esac
  # Complement: a crate gate BEFORE any item is still reported (the leading-region rule must not
  # have narrowed the real case away).
  printf '//! prose\n// a comment\n\n#![cfg(feature = "x")]\nfn item() {}\n' > "$gg_src_/leading.rs"
  got_=$(
    export GG_SRC="$gg_src_/leading.rs" GG_REL="tests/leading.rs"
    _package_test_targets_gated() { printf '%s\t%s\t%s\t%s\n' leading_target "$GG_SRC" source "$GG_REL"; }
    # shellcheck disable=SC1090
    . "$gg_body_"
    _crate_gated_test_targets somepkg 2>/dev/null | awk -F'\t' '{print $3}'
  )
  case "$got_" in
    L*'#![cfg(feature = "x")]'*)
      ok "1699-r42-module-attr-complement: a genuine gate after comments/blank lines is still reported" ;;
    *)
      bad "1699-r42-module-attr-complement: reported '$got_' — a genuine inner cfg attribute went unreported" ;;
  esac

  # A multiline NON-cfg attribute before the gate (roborev round-41). `#![allow(\n … \n)]` used to
  # end the leading region on its continuation line, so the crate gate after it vanished. The scanner
  # now brackets-balances EVERY inner attribute and emits only cfg/cfg_attr — structural rather than
  # a list of spellings, which is what the previous four rounds kept extending.
  printf '#![allow(\n    clippy::needless_range_loop,\n    dead_code\n)]\n#![cfg(feature = "x")]\nfn t() {}\n' > "$gg_src_/multiattr.rs"
  got_=$(
    export GG_SRC="$gg_src_/multiattr.rs" GG_REL="tests/multiattr.rs"
    _package_test_targets_gated() { printf '%s\t%s\t%s\t%s\t%s\n' ma2_t "$GG_SRC" source "$GG_REL" ""; }
    # shellcheck disable=SC1090
    . "$gg_body_"
    _crate_gated_test_targets somepkg 2>/dev/null | awk -F'\t' '{print $3}'
  )
  case "$got_" in
    L*'#![cfg(feature = "x")]'*)
    ok "1699-r41-multiattr: a gate after a MULTILINE non-cfg attribute is still reported"
    ;;
    *) bad "1699-r41-multiattr: reported '$got_' — the occurrence vanished after a multiline non-cfg attribute" ;;
  esac
  # and the non-cfg attribute must NOT be emitted as if it were a gate
  case "$got_" in
    *allow*) bad "1699-r41-multiattr-purity: a non-cfg attribute leaked into the occurrence report: '$got_'" ;;
    *) ok "1699-r41-multiattr-purity: only cfg/cfg_attr lines are reported, not every attribute" ;;
  esac

  # MULTILINE attributes (roborev round-28, Medium). rustfmt breaks a long condition across lines,
  # and the line-based extraction reduced `#![cfg(all(` … `))]` to `#![cfg(` — discarding exactly
  # the condition a reader needs. There is NO such attribute in this corpus today (measured: 0
  # across cqlite-flight/tests), which is precisely why it needs a fixture: the defect would have
  # stayed invisible until somebody reformatted a file, and then it would have moved a target into
  # the "no gate" population silently.
  printf '//! prose\n#![cfg(all(\n    feature = "a",\n    feature = "b"\n))]\nfn t() {}\n' > "$gg_src_/multiline.rs"
  got_=$(
    export GG_SRC="$gg_src_/multiline.rs" GG_REL="tests/multiline.rs"
    _package_test_targets_gated() { printf '%s\t%s\t%s\t%s\n' multiline_target "$GG_SRC" source "$GG_REL"; }
    # shellcheck disable=SC1090
    . "$gg_body_"
    _crate_gated_test_targets somepkg 2>/dev/null | awk -F'\t' '{print $3}'
  )
  case "$got_" in
    L*'#![cfg(all('*)
    ok "1699-r28-multiline: a multiline gate reports its first line with a line number (the reader opens the file for the rest)"
    ;;
    *) bad "1699-r28-multiline: reported '$got_' — the occurrence is missing entirely, so the target reads as having no inner cfg attribute" ;;
  esac
  # And a multiline cfg_attr, since that is the form r27 added and r28 truncated.
  printf '//! prose\n#![cfg_attr(\n    feature = "a",\n    cfg(feature = "b")\n)]\nfn t() {}\n' > "$gg_src_/multiline_attr.rs"
  got_=$(
    export GG_SRC="$gg_src_/multiline_attr.rs" GG_REL="tests/multiline_attr.rs"
    _package_test_targets_gated() { printf '%s\t%s\t%s\t%s\n' ma_target "$GG_SRC" source "$GG_REL"; }
    # shellcheck disable=SC1090
    . "$gg_body_"
    _crate_gated_test_targets somepkg 2>/dev/null | awk -F'\t' '{print $3}'
  )
  case "$got_" in
    L*'#![cfg_attr('*)
      ok "1699-r28-multiline-attr: a multiline cfg_attr occurrence is reported with its line number" ;;
    *)
      bad "1699-r28-multiline-attr: reported '$got_'" ;;
  esac

  # Stacked gates: BOTH must appear, because they are conjunctive and reporting one hides the other.
  printf '//! prose\n#![cfg(feature = "a")]\n#![cfg(feature = "b")]\nfn t() {}\n' > "$gg_src_/stacked.rs"
  got_=$(
    export GG_SRC="$gg_src_/stacked.rs" GG_REL="tests/stacked.rs"
    _package_test_targets_gated() { printf '%s\t%s\t%s\t%s\n' stacked_target "$GG_SRC" source "$GG_REL"; }
    # shellcheck disable=SC1090
    . "$gg_body_"
    _crate_gated_test_targets somepkg 2>/dev/null | awk -F'\t' '{print $3}'
  )
  n_occ_=$(printf '%s' "$got_" | grep -o 'L[0-9]*:' | wc -l | tr -d ' ')
  if [ "${n_occ_:-0}" -ge 2 ]; then
    ok "1699-r42-stacked: both stacked inner cfg attributes are reported as separate occurrences ($n_occ_)"
  else
    bad "1699-r42-stacked: reported '$got_' ($n_occ_ occurrences) — reporting one of several hides half the reason a target runs nothing"
  fi
  # An unreadable declared source is a FAILED derivation, not a skip (round 26).
  chmod 000 "$gg_src_/plain_cfg.rs" 2>/dev/null
  if [ -r "$gg_src_/plain_cfg.rs" ]; then
    skipped "1699-r27-verbatim-unreadable: cannot make a file unreadable here — the failed-derivation path was NOT verified"
  elif (
      export GG_SRC="$gg_src_/plain_cfg.rs" GG_REL="tests/plain_cfg.rs"
      _package_test_targets_gated() { printf '%s\t%s\t%s\t%s\n' t "$GG_SRC" source "$GG_REL"; }
      # shellcheck disable=SC1090
      . "$gg_body_"; _crate_gated_test_targets somepkg >/dev/null 2>&1 ); then
    bad "1699-r27-verbatim-unreadable: an UNREADABLE declared source returned SUCCESS — the target is dropped from the census and lands among the ungated rest by omission"
  else
    ok "1699-r27-verbatim-unreadable: an unreadable declared source is a failed derivation, not a silent skip"
  fi
  chmod 644 "$gg_src_/plain_cfg.rs" 2>/dev/null
fi

# The classification identifiers must STAY gone.
for goneid_ in _component_runs_target _feature_enabled_by_some_component gated_names elsewhere_names unclass_names; do
  if [ "$(grep -cF "$goneid_" "$GATE")" -eq 0 ]; then
    ok "1699-r27-classifier-gone[$goneid_]: absent from the gate"
  else
    bad "1699-r27-classifier-gone[$goneid_]: back in the gate — the crate-gate classification was retired after five rounds of findings (grammar, stacked gates, conjunctions, compile-only invocations, cfg_attr); reintroducing it reintroduces them"
  fi
done


# --- 35. #1699: a FAILED ANSI normalisation is not a fallback (roborev round-25, Medium) ---
#
# `_ansi_stripped_log` used to hand back the ORIGINAL path when it could not read the log or could
# not write the stripped copy. Under `CARGO_TERM_COLOR` the coloured original is exactly what the
# parsers cannot read (round 15), so the fallback converted a normalisation failure into a vacuous
# PASS. It now returns non-zero and every caller fail-closes.
#
# WRITTEN BECAUSE THE RED CHECK FOUND NOTHING. Reverting the fix by hand — restoring the silent
# `printf '%s' "$logfile"` — left the whole suite GREEN, so that half of the fix was protected only
# by its own presence in the file. That is round 13's lesson in this same PR: a guard whose only
# protection is its own presence has no protection at all. Two behavioural cases, one per failure
# mode, plus a structural assert that the fallback cannot come back.
if [ -s "$zn_h" ]; then
  # (a) UNREADABLE log
  unr_="$tmp/1699-r25-unreadable.log"
  printf 'Running tests/foo.rs\nrunning 1 tests\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n' > "$unr_"
  chmod 000 "$unr_" 2>/dev/null
  if [ -r "$unr_" ]; then
    skipped "1699-r25-unreadable: cannot make a file unreadable on this box (running as root?) — the unreadable-log path was NOT verified"
  elif ( . "$zn_h"; check_no_unexpected_zero_tests "unreadable" "$unr_" >/dev/null 2>&1 ); then
    bad "1699-r25-unreadable: an UNREADABLE log PASSED — the guard could not read its input and still reported OK, which is the vacuous pass these guards exist to prevent"
  else
    ok "1699-r25-unreadable: an unreadable log is a FAIL, not a fallback to parsing it anyway"
  fi
  chmod 644 "$unr_" 2>/dev/null

  # (b) UNWRITABLE destination: the log is readable but the `.ansi-stripped` sibling cannot be
  # created, which is the second failure mode the old fallback swallowed.
  rodir_="$tmp/1699-r25-ro"; mkdir -p "$rodir_"
  ro_log_="$rodir_/x.log"
  printf 'Running tests/foo.rs\nrunning 1 tests\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n' > "$ro_log_"
  chmod 500 "$rodir_" 2>/dev/null
  if ( : > "$rodir_/probe" ) 2>/dev/null; then
    rm -f "$rodir_/probe" 2>/dev/null
    skipped "1699-r25-unwritable: cannot make a directory unwritable on this box — the failed-strip path was NOT verified"
  elif ( . "$zn_h"; check_no_unexpected_zero_tests "unwritable" "$ro_log_" >/dev/null 2>&1 ); then
    bad "1699-r25-unwritable: a log whose normalised copy could NOT be written PASSED — the guard fell back to the un-normalised original, which under colour is unparseable, so it measured nothing"
  else
    ok "1699-r25-unwritable: a failed normalisation is a FAIL, not a silent fallback to the coloured original"
  fi
  chmod 700 "$rodir_" 2>/dev/null

  # (c) structural: the fallback must not return
  ansi_="$tmp/1699-r25-ansi.txt"
  awk '/^_ansi_stripped_log\(\) \{/, /^\}/' "$GATE" > "$ansi_"
  if [ ! -s "$ansi_" ]; then
    bad "1699-r25-ansi-scope: could not extract _ansi_stripped_log — this assert would pass vacuously"
  elif [ "$(sed 's/[[:space:]]*#.*$//' "$ansi_" | grep -cE "printf '%s' \"\\\$logfile\"")" -eq 0 ]; then
    ok "1699-r25-ansi-nofallback: _ansi_stripped_log never returns the un-normalised original path"
  else
    bad "1699-r25-ansi-nofallback: the silent fallback to \$logfile is back — a failed normalisation would again be reported as a usable parse source"
  fi
fi



# --- 36. #1699: a target OBSERVED but never JUDGED is a FAIL (roborev round-26, Medium) ---
#
# The guard keyed each `test result:` line to the banner before it, but never checked that a banner
# GOT one. So a log carrying every expected banner and an earlier result — a truncated log, a killed
# test binary, a result line the parse missed — passed while one target was silently never judged.
# That is the same hole as "no results at all" (round 25) restricted to a single target, which is
# harder to see because the log looks healthy in every other respect.
if [ -s "$zn_h" ]; then
  # (a) two banners, only the FIRST gets a result: the second is an orphan.
  orph="$tmp/1699-r26-orphan.log"
  printf 'Running tests/first.rs\nrunning 3 tests\ntest result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\nRunning tests/second.rs\nrunning 2 tests\n' > "$orph"
  if ( . "$zn_h"; check_no_unexpected_zero_tests "orphan" "$orph" >/dev/null 2>&1 ); then
    bad "1699-r26-orphan-eof: a log whose LAST target got no 'test result:' line PASSED — that target was observed running and never judged, so the guard skipped exactly the target it was asked about"
  else
    ok "1699-r26-orphan-eof: a target observed at EOF with no result is a FAIL"
  fi

  # (b) banner, banner, result: the first target is an orphan even though results exist.
  orph2="$tmp/1699-r26-orphan-mid.log"
  printf 'Running tests/first.rs\nrunning 3 tests\nRunning tests/second.rs\nrunning 2 tests\ntest result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n' > "$orph2"
  if ( . "$zn_h"; check_no_unexpected_zero_tests "orphan-mid" "$orph2" >/dev/null 2>&1 ); then
    bad "1699-r26-orphan-mid: a log where a banner is followed by ANOTHER banner without a result PASSED — the first target was observed and never judged, and the later result made the log look complete"
  else
    ok "1699-r26-orphan-mid: a banner superseded by the next banner without a result is a FAIL"
  fi

  # (c) COMPLEMENT: a healthy two-target log must still pass, or the check has over-reached into a
  # false red — the mistake round 17 made twice and the reason every fixture here carries one.
  healthy="$tmp/1699-r26-healthy.log"
  printf 'Running tests/first.rs\nrunning 3 tests\ntest result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\nRunning tests/second.rs\nrunning 2 tests\ntest result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out\n' > "$healthy"
  if ( . "$zn_h"; check_no_unexpected_zero_tests "healthy" "$healthy" >/dev/null 2>&1 ); then
    ok "1699-r26-orphan-complement: a healthy two-target log still PASSES (the orphan check is not a blanket red)"
  else
    bad "1699-r26-orphan-complement: a HEALTHY two-target log now FAILS — the orphan check is over-reaching, which would red every clean run"
  fi
fi

# AN ASSERT-COUNT FLOOR, because "0 failed" is not evidence that the asserts RAN (#1699).
# Demonstrated on this very file: a malformed `local` declaration in the gate
# (`shift 2 _orphans=""` — bash rejects it with "shift: too many arguments") made several
# extracted-function subshells die under `set -u`, and the suite reported
#     passed: 296  failed: 0
# nine asserts fewer than the run before it, with no failure anywhere. A suite that can lose
# whole sections and still exit 0 is the vacuous pass this file exists to catch, one level up.
#
# A FLOOR, not an exact count: new asserts are added constantly and an equality check would red on
# every addition. Raise it deliberately when you add a section — the ratchet is the point, and the
# number below is the count measured at the commit that introduced this check.
# --- 35b. #1699: _deny_warnings refuses inherited lint controls (round-35, Medium) --------
#
# `-D warnings` goes last so it wins over another `-D`/`-W` — but it does NOT win over
# `--cap-lints allow` (caps every lint below deny) or `--force-warn <spec>` (forces the lint back to
# a warning). Either makes these lanes' whole warning-class guard SILENTLY INERT while the SUMMARY
# line stays green: #1981's defect reintroduced through the ENVIRONMENT rather than the code. A
# guard switchable off by an inherited variable is not a guard.
dw_="$tmp/1699-r35-dw.sh"
awk '/^_deny_warnings\(\) \{/, /^\}/' "$GATE" > "$dw_"
if [ ! -s "$dw_" ]; then
  bad "1699-r35-denywarn-scope: could not extract _deny_warnings — these asserts would pass vacuously"
else
  for spec_ in "RUSTFLAGS=--cap-lints allow" "RUSTFLAGS=--force-warn warnings"; do
    var_=${spec_%%=*}; val_=${spec_#*=}
    if ( export "$var_=$val_"; . "$dw_"; _deny_warnings true ) >/dev/null 2>&1; then
      bad "1699-r35-denywarn-refuse[$val_]: _deny_warnings ACCEPTED an inherited '$val_' — the appended -D warnings cannot override it, so the lane's warning guard is inert while reporting PASS"
    else
      ok "1699-r35-denywarn-refuse[$val_]: _deny_warnings fails closed on an inherited '$val_'"
    fi
  done
  # And the ENCODED form, which takes precedence over RUSTFLAGS entirely.
  if ( export CARGO_ENCODED_RUSTFLAGS="$(printf -- '--cap-lints\037allow')"; . "$dw_"; _deny_warnings true ) >/dev/null 2>&1; then
    bad "1699-r35-denywarn-refuse[encoded]: _deny_warnings ACCEPTED --cap-lints via CARGO_ENCODED_RUSTFLAGS, which takes precedence over RUSTFLAGS — the quietest possible route to an inert guard"
  else
    ok "1699-r35-denywarn-refuse[encoded]: _deny_warnings fails closed on --cap-lints in CARGO_ENCODED_RUSTFLAGS too"
  fi
  # COMPLEMENT: an ordinary inherited RUSTFLAGS must still be accepted, or this becomes a false red
  # on every box that sets a target-cpu or a sanitizer flag.
  if ( export RUSTFLAGS="-C target-cpu=native"; . "$dw_"; _deny_warnings true ) >/dev/null 2>&1; then
    ok "1699-r35-denywarn-complement: an ordinary inherited RUSTFLAGS is still accepted and appended to"
  else
    bad "1699-r35-denywarn-complement: _deny_warnings now refuses a HARMLESS inherited RUSTFLAGS — that is a false red on any box with a target-cpu or sanitizer flag set"
  fi
fi

# --- 36b. #1699: the `bash -c` top-level-`local` LINT WAS ABANDONED (recorded, not hidden) ------
#
# `bash -n` ACCEPTS `local` at a script's top level; bash rejects it at RUNTIME ("local: can only be
# used in a function"). So the class survives every syntax check and fails the component minutes
# into a cargo run. I introduced exactly that in the round-31 fix and caught it by hand.
#
# A lint for it needs to EXTRACT each `run_component X bash -c '…'` body, and three successive
# extractors each produced FALSE POSITIVES on healthy code:
#   1. awk scanning for a terminator LINE (`^' ;;$`) — the terminator sits at the END of a content
#      line, so it over-ran into unrelated gate comments containing apostrophes ("component's
#      command") and reported 7 syntax failures that did not exist;
#   2. python keyed on the literal `' ;;` — the core-tests arm ends `"${@:3}"' \` because arguments
#      follow it, so the same over-run happened for that arm;
#   3. python taking the NEXT apostrophe as the terminator — correct for a plain single-quoted
#      string, but these bodies embed quotes with the `'"'"'` idiom, so it truncated one body and
#      reported an unterminated string.
#
# Correct extraction requires a shell-quoting parser. Writing one here would be a second
# implementation of shell lexing whose correctness is only knowable by differential testing against
# a shell — the exact reasoning that retired #3229's census-exclusion oracle, which was DELETED
# because its false-PASS count kept rising. The mirror case applies just as strongly: a lint that
# reds healthy code is the one agents learn to ignore, and then it protects nothing.
#
# SO THERE IS NO LINT, and the residual is stated instead of implied: a top-level `local` in a
# `bash -c` body will be caught by the FULL GATE RUNNING that component, not before. That is a real
# reduction in fast-loop coverage, accepted. What IS pinned, soundly and without extraction, is the
# property the round-31 fix established (1699-r31-cli-private-dir, above).

# --- 37. #1699: the flight lane's own fixture preflight (roborev round-30, Medium) ------
#
# Enrolling the lane in DATASET_COMPONENTS is not enough. The generic full-gate preflight requires
# only the canonical `test_basic` corpus, but the unit suite this lane EXECUTES contains a
# real-fixture test (cqlite-flight/src/stats.rs) that returns early — three separate ways — when
# test_timeseries/sensor_data or its Statistics.db is absent, EVEN WITH CQLITE_DATASETS_ROOT set.
# So a partial corpus produced a green lane that had skipped the coverage it advertises: #3220's
# rule ("never let a dataset-dependent test pass on an empty dataset") and this issue's thesis at
# once.
#
# STRUCTURAL, and labelled as such: the behavioural form needs a FULL gate run against a doctored
# corpus root, which is a 30-component run and does not belong in a sub-second self-test. What is
# pinned here is that the check exists, tests BOTH halves, and is full-gate-only.
fl_pf_="$tmp/1699-r30-flight.txt"
awk '/^run_flight_tests\(\) \{/, /^\}/' "$GATE" > "$fl_pf_"
if [ ! -s "$fl_pf_" ]; then
  bad "1699-r30-preflight-scope: could not extract run_flight_tests — these asserts would pass vacuously"
else
  pf_ok_=1
  grep -q 'sensor_data-\*' "$fl_pf_" || pf_ok_=0
  grep -q 'Statistics.db' "$fl_pf_" || pf_ok_=0
  if [ "$pf_ok_" -eq 1 ]; then
    ok "1699-r30-preflight-halves: the lane checks BOTH the sensor_data dir AND a -Statistics.db (the dir alone still lets the test return early)"
  else
    bad "1699-r30-preflight-halves: the fixture preflight no longer checks both halves — the real-fixture test returns early on a missing Statistics.db too, so the lane would report a green over skipped coverage (#3220)"
  fi
  # FULL gate only, spelled the way this script spells it: --only and --lite are probes.
  if grep -qE '\[ -z "\$ONLY" \] && \[ "\$LITE" -eq 0 \]' "$fl_pf_"; then
    ok "1699-r30-preflight-fullonly: the fixture preflight is gated on FULL-gate mode, leaving --only/--lite lenient"
  else
    bad "1699-r30-preflight-fullonly: the fixture preflight is no longer full-gate-only — either it has become unconditional (redding every --only probe) or it has lost its mode test entirely"
  fi
fi

# BEHAVIOURAL, not just structural (roborev round-32, Medium): the preflight must require EVERY
# `sensor_data-*` match to qualify, because the Rust test takes the FIRST `read_dir` match in
# UNSPECIFIED order. "Some match is complete" does not imply "the test will find a complete one",
# so a second incomplete directory — or a prefix-matching regular file, which read_dir also yields —
# is enough to make the test skip while the lane reports PASS.
#
# The preflight block is extracted from the gate and run against doctored corpus roots, with the few
# variables it touches stubbed. No cargo, no gate slot, sub-second.
# Same 1:1 accounting rule as the r18 differential above (round 13, X2): this section
# emits NINE verdicts, so its skip path must emit nine. The case list is declared once and
# consumed by both branches.
R32_WANT_CASES='CASE good: PASS
CASE second_incomplete: FAIL-CLOSED
CASE prefix_file: FAIL-CLOSED
CASE dangling_symlink: FAIL-CLOSED
CASE nullglob: FAIL-CLOSED
CASE nullglob_good: PASS
CASE valid_dir_symlink: PASS
CASE base_is_symlink: PASS
CASE no_match: FAIL-CLOSED'
if ! command -v python3 >/dev/null 2>&1; then
  while IFS= read -r _r32skip_; do
    [ -n "$_r32skip_" ] || continue
    skipped "1699-r32-preflight-behaviour[${_r32skip_%%:*}]: needs python3 — NOT verified here"
  done <<EOF_R32_SKIP
$R32_WANT_CASES
EOF_R32_SKIP
else
  pf_report_="$tmp/1699-r32-pf.txt"
  python3 - "$GATE" "$tmp" > "$pf_report_" 2>&1 <<'PF_PY'
import os, subprocess, sys
gate, tmp = sys.argv[1], sys.argv[2]
src = open(gate).read()
i = src.index("  # LANE-SPECIFIC FIXTURE PREFLIGHT")
j = src.index("  # The enabled set.")
block = src[i:j]

def run(root, prelude=""):
    harness = (
        '%s\nONLY=""\nLITE=0\nname=flight-tests\nlog=/dev/null\nstatus=PASS\nstart=0\n'
        'record_result(){ :; }\nCQLITE_DATASETS_ROOT="%s"\n'
        'f(){\n%s\n echo PREFLIGHT-PASSED\n}\nf\n' % (prelude, root, block)
    )
    r = subprocess.run(["bash", "-c", harness], capture_output=True, text=True)
    return (r.stdout + r.stderr)

# (a) every match complete -> the preflight passes
good = os.path.join(tmp, "pf-good"); d = os.path.join(good, "sstables/test_timeseries/sensor_data-a")
os.makedirs(d, exist_ok=True); open(os.path.join(d, "nb-1-big-Statistics.db"), "w").close()
out = run(good)
print("CASE good:", "PASS" if "PREFLIGHT-PASSED" in out else "FAIL")

# (b) a SECOND, incomplete match -> must fail closed and name it
bad = os.path.join(tmp, "pf-bad")
for n in ("sensor_data-a", "sensor_data-b"):
    os.makedirs(os.path.join(bad, "sstables/test_timeseries", n), exist_ok=True)
open(os.path.join(bad, "sstables/test_timeseries/sensor_data-a/nb-1-big-Statistics.db"), "w").close()
out = run(bad)
print("CASE second_incomplete:", "FAIL-CLOSED" if ("FAIL-CLOSED" in out and "sensor_data-b" in out) else "MISSED")

# (c) a prefix-matching regular FILE -> must fail closed
fil = os.path.join(tmp, "pf-file"); base = os.path.join(fil, "sstables/test_timeseries")
os.makedirs(os.path.join(base, "sensor_data-a"), exist_ok=True)
open(os.path.join(base, "sensor_data-a/nb-1-big-Statistics.db"), "w").close()
open(os.path.join(base, "sensor_data-stray"), "w").close()
out = run(fil)
print("CASE prefix_file:", "FAIL-CLOSED" if ("FAIL-CLOSED" in out and "not-a-directory" in out) else "MISSED")

# (d2) a DANGLING SYMLINK match -> must fail closed. `test -e` follows the link and is FALSE for a
# broken one, so the loop used to skip it while the Rust test's read_dir still yields it and may
# pick it first (roborev round-33).
dang = os.path.join(tmp, "pf-dangling"); base = os.path.join(dang, "sstables/test_timeseries")
os.makedirs(os.path.join(base, "sensor_data-a"), exist_ok=True)
open(os.path.join(base, "sensor_data-a/nb-1-big-Statistics.db"), "w").close()
os.symlink(os.path.join(tmp, "does-not-exist-anywhere"), os.path.join(base, "sensor_data-dangling"))
out = run(dang)
print("CASE dangling_symlink:", "FAIL-CLOSED" if ("FAIL-CLOSED" in out and "dangling-symlink" in out) else "MISSED")

# (d3) NULLGLOB inherited: an unmatched `*-Statistics.db` pattern expands to nothing, so a bare
# `ls` would list the CWD and SUCCEED. The check must not depend on an ambient shell option this
# script never sets (roborev round-34). Same corpus as (b)'s bad case, run under `shopt -s nullglob`.
out = run(bad, prelude="shopt -s nullglob")
print("CASE nullglob:", "FAIL-CLOSED" if ("FAIL-CLOSED" in out and "sensor_data-b" in out) else "MISSED")
# and the good corpus must still pass under nullglob (no false red)
out = run(good, prelude="shopt -s nullglob")
print("CASE nullglob_good:", "PASS" if "PREFLIGHT-PASSED" in out else "FAIL")

# (d4) a VALID symlink to a fixture directory must PASS. `find` defaults to `-P` and does not follow
# its starting point, so without `-H` this was a FALSE RED on a legitimate corpus layout — the
# direction that teaches people to waive a check (roborev round-35).
lnk = os.path.join(tmp, "pf-validlink"); base = os.path.join(lnk, "sstables/test_timeseries")
real = os.path.join(lnk, "real-fixture")
os.makedirs(real, exist_ok=True); os.makedirs(base, exist_ok=True)
open(os.path.join(real, "nb-1-big-Statistics.db"), "w").close()
os.symlink(real, os.path.join(base, "sensor_data-viasymlink"))
out = run(lnk)
print("CASE valid_dir_symlink:", "PASS" if "PREFLIGHT-PASSED" in out else "FAIL")

# (d5) the BASE directory itself is a symlink -> must still find the fixtures (roborev round-38).
# Round 35 added `-H` to the per-entry find and left the outer enumeration at the default `-P`, so a
# symlinked `test_timeseries` enumerated NOTHING and the preflight failed the gate on a valid layout.
blnk = os.path.join(tmp, "pf-baselink"); real_ts = os.path.join(blnk, "real_ts")
os.makedirs(os.path.join(real_ts, "sensor_data-a"), exist_ok=True)
open(os.path.join(real_ts, "sensor_data-a/nb-1-big-Statistics.db"), "w").close()
os.makedirs(os.path.join(blnk, "sstables"), exist_ok=True)
os.symlink(real_ts, os.path.join(blnk, "sstables/test_timeseries"))
out = run(blnk)
print("CASE base_is_symlink:", "PASS" if "PREFLIGHT-PASSED" in out else "FAIL")

# (d) nothing matches -> must fail closed
none = os.path.join(tmp, "pf-none"); os.makedirs(os.path.join(none, "sstables/test_timeseries"), exist_ok=True)
out = run(none)
print("CASE no_match:", "FAIL-CLOSED" if ("FAIL-CLOSED" in out and "NOTHING matches" in out) else "MISSED")
PF_PY
  while IFS= read -r want_; do
    [ -n "$want_" ] || continue
    if grep -qF "$want_" "$pf_report_"; then
      ok "1699-r32-preflight-behaviour[${want_%%:*}]: ${want_#*: }"
    else
      bad "1699-r32-preflight-behaviour[${want_%%:*}]: expected '${want_#*: }' — got: $(grep -F "${want_%%:*}" "$pf_report_" | head -1)"
    fi
  done <<EOF_R32_CASES
$R32_WANT_CASES
EOF_R32_CASES
fi

# The harness must not compile into a predictable shared directory (round-30, Medium): concurrent
# harnesses corrupt each other, and on a multi-user host a pre-created directory means the harness
# executes artifacts somebody else controls.
hn_="$SCRIPT_DIR/test_agent_gate_feature_matrix_lanes.sh"
if [ ! -r "$hn_" ]; then
  skipped "1699-r30-private-target: harness not readable from here — NOT verified"
elif [ "$(grep -cE '^TARGET="\$WORK/' "$hn_")" -gt 0 ] && [ "$(grep -cE '^TARGET="\$\{TMPDIR:-/tmp\}/ah6' "$hn_")" -eq 0 ]; then
  ok "1699-r30-private-target: the harness compiles into a per-invocation private dir under \$WORK, not a predictable shared path"
else
  bad "1699-r30-private-target: the harness is back to a predictable shared target dir — two concurrent harnesses corrupt each other, and on a multi-user host it executes artifacts from a directory another user can pre-create"
fi

# The tally is printed AFTER every assertion and after this floor (roborev round-27, Low): it used
# to print before section 36 and before the floor, so the script could announce `failed: 0` and then
# add failures underneath it. A summary that precedes its own subject is the same shape as a verdict
# that precedes its measurement.
# --- 37b. #1699: same-line `#[path]` + `mod`, and doc-comment mentions (round-40, Medium) ----
#
# `_rust_module_closure` matched `#[path = ...]` ANYWHERE on a line and then `next`ed
# unconditionally. Two consequences: the common single-line form `#[path = "child.rs"] mod child;`
# recorded the path and SKIPPED the `mod`, so the child module was never queued — legacy-gated tests
# inside it escaped discovery, polarity AND the census, silently and in the under-reporting
# direction; and a doc comment mentioning that syntax could bind a stale path to the next real `mod`.
mc_="$tmp/1699-r40-mc.sh"
awk '/^_rust_module_closure\(\) \{/, /^\}/' "$GATE" > "$mc_"
if [ ! -s "$mc_" ]; then
  bad "1699-r40-closure-scope: could not extract _rust_module_closure — these asserts would pass vacuously"
else
  mc_dir_="$tmp/1699-r40-src"; mkdir -p "$mc_dir_"
  printf 'fn t() {}\n' > "$mc_dir_/child.rs"
  printf 'fn t() {}\n' > "$mc_dir_/plain.rs"
  # (a) same-line #[path] + mod  -> the child MUST be in the closure
  printf '#[path = "child.rs"] mod child;\n' > "$mc_dir_/sameline.rs"
  got_=$( . "$mc_"; _rust_module_closure "$mc_dir_/sameline.rs" 2>/dev/null | grep -c 'child.rs' )
  if [ "${got_:-0}" -gt 0 ]; then
    ok "1699-r40-closure[sameline]: a same-line #[path] + mod queues the child module"
  else
    bad "1699-r40-closure[sameline]: the child was NOT queued — everything in that module escapes discovery, polarity and the census, silently"
  fi
  # (b) a DOC COMMENT mentioning the syntax must not bind a path to the following mod
  printf '//! see #[path = "wrong.rs"] for details\nmod plain;\n' > "$mc_dir_/doccomment.rs"
  got_=$( . "$mc_"; _rust_module_closure "$mc_dir_/doccomment.rs" 2>/dev/null | grep -c 'wrong.rs' )
  if [ "${got_:-0}" -eq 0 ]; then
    ok "1699-r40-closure[doccomment]: a #[path] mentioned in a doc comment does not bind a stale path"
  else
    bad "1699-r40-closure[doccomment]: a doc-comment mention bound 'wrong.rs' to the next mod — the closure then reads a file the compiler never sees"
  fi
  # (b2) BLOCK-COMMENTED declarations are not declarations (roborev round-41). A `/* mod ghost; */`
  # was read as real, so the closure could demand a file Rust never includes — failing the lane as
  # UNRESOLVED on a commented-out example.
  printf '/* #[path = "ghost.rs"] mod ghost; */\nmod plain;\n' > "$mc_dir_/commented.rs"
  unres_="$tmp/1699-r41-unres.txt"
  got_=$( . "$mc_"; _rust_module_closure "$mc_dir_/commented.rs" 2>"$unres_" | grep -c 'ghost' )
  if [ "${got_:-0}" -eq 0 ] && [ "$(grep -c ghost "$unres_" 2>/dev/null)" -eq 0 ]; then
    ok "1699-r41-closure[commented]: a block-commented #[path]/mod is ignored, not resolved and not reported UNRESOLVED"
  else
    bad "1699-r41-closure[commented]: a commented-out declaration was treated as real (closure hits=$got_, unresolved=$(grep -c ghost "$unres_" 2>/dev/null)) — the lane fails on an example nobody compiles"
  fi
  # (b3) a block comment BETWEEN a real #[path] and its mod must not clear the pending path
  printf '#[path = "child.rs"]\n/* explanatory note */\nmod child;\n' > "$mc_dir_/pathgap.rs"
  got_=$( . "$mc_"; _rust_module_closure "$mc_dir_/pathgap.rs" 2>/dev/null | grep -c 'child.rs' )
  if [ "${got_:-0}" -gt 0 ]; then
    ok "1699-r41-closure[pathgap]: a block comment between #[path] and mod preserves the pending path"
  else
    bad "1699-r41-closure[pathgap]: the comment cleared the pending path, so the mod resolved to the WRONG file (or not at all)"
  fi

  # (c) COMPLEMENT: the ordinary two-line form must still work (the fix must not narrow it away)
  printf '#[path = "child.rs"]\nmod child;\n' > "$mc_dir_/twoline.rs"
  got_=$( . "$mc_"; _rust_module_closure "$mc_dir_/twoline.rs" 2>/dev/null | grep -c 'child.rs' )
  if [ "${got_:-0}" -gt 0 ]; then
    ok "1699-r40-closure[twoline]: the ordinary attribute-then-mod form still resolves"
  else
    bad "1699-r40-closure[twoline]: the two-line form BROKE — the fix narrowed away the common case"
  fi
fi

# --- 38. #1699: block comments must not split an attribute cluster (round-36, Medium) ----
#
# `_legacy_coreq_sites` treated only `//` as cluster trivia, so a `/* … */` between stacked
# `#[cfg(feature = "legacy-heuristics")]` and `#[cfg(feature = "experimental")]` attributes SPLIT the
# cluster — the co-required site was dropped and the census could report a FALSE ZERO GAP, the silent
# under-report direction, in the one output whose whole job is to state omissions.
cs_="$tmp/1699-r36-coreq.sh"
awk '/^_legacy_coreq_sites\(\) \{/, /^\}/' "$GATE" > "$cs_"
if [ ! -s "$cs_" ]; then
  bad "1699-r36-coreq-scope: could not extract _legacy_coreq_sites — these asserts would pass vacuously"
else
  cs_src_="$tmp/1699-r36-src"; mkdir -p "$cs_src_"
  # (a) a SINGLE-LINE block comment between the two attributes
  printf '#[cfg(feature = "legacy-heuristics")]\n/* trivia */\n#[cfg(feature = "experimental")]\n#[test]\nfn t() {}\n' > "$cs_src_/inline.rs"
  # (b) a MULTILINE block comment between them
  printf '#[cfg(feature = "legacy-heuristics")]\n/* first\n   second\n   third */\n#[cfg(feature = "experimental")]\n#[test]\nfn t() {}\n' > "$cs_src_/multiline.rs"
  # (c) control: no comment at all must still report the site (the fix must not have broken the base case)
  printf '#[cfg(feature = "legacy-heuristics")]\n#[cfg(feature = "experimental")]\n#[test]\nfn t() {}\n' > "$cs_src_/plain.rs"
  for case_ in inline multiline plain; do
    got_=$( . "$cs_"; _legacy_coreq_sites "$cs_src_/$case_.rs" " default legacy-heuristics " 2>/dev/null | grep -c . )
    if [ "${got_:-0}" -gt 0 ]; then
      ok "1699-r36-coreq[$case_]: the co-required site is reported across the trivia ($got_ site line(s))"
    else
      bad "1699-r36-coreq[$case_]: the site was DROPPED — the cluster split on trivia, so the census would report a false zero gap for a body that cannot execute"
    fi
  done
fi

# --- 39. #1699: never name a target whose required-features are unmet (round-36, Medium) --
#
# The lane added EVERY discovered target to `--test`, while the producer discarded the manifest
# `required-features` list. Cargo REJECTS an explicit `--test <name>` whose required-features are
# unmet, so a target needing `legacy-heuristics` PLUS another disabled feature made the lane fail on
# entirely correct code — a FALSE RED. Producer now emits the full list as a 5th field; the lane
# compares ALL of it and reports unmet targets as a coverage GAP instead of invoking them.
lh_body_="$tmp/1699-r36-lh.txt"
awk '/^run_legacy_heuristics\(\) \{/, /^\}/' "$GATE" > "$lh_body_"
gp_body_="$tmp/1699-r36-gp.txt"
awk '/^_package_test_targets_gated\(\) \{/, /^\}/' "$GATE" > "$gp_body_"
if [ ! -s "$lh_body_" ] || [ ! -s "$gp_body_" ]; then
  bad "1699-r36-rf-scope: could not extract run_legacy_heuristics / _package_test_targets_gated — these asserts would pass vacuously"
else
  # the producer must emit five fields, else the 5th silently lands in the 4th for every consumer
  if [ "$(grep -cE '(join\(","\)|",".join\(rf\))' "$gp_body_")" -ge 2 ]; then
    ok "1699-r36-rf-emitted: the producer emits required-features in BOTH parser halves"
  else
    bad "1699-r36-rf-emitted: a parser half no longer emits required-features — the consumer then compares an EMPTY list, and every target looks satisfiable, restoring the false red"
  fi
  # every consumer must read five fields (the last read var absorbs the remainder otherwise)
  n_rd_=$(grep -cE "read -r (_tn sp _how rel _rf_ignored|_mt_name _mt_src _mt_how _mt_rel _mt_rf)" "$GATE")
  if [ "${n_rd_:-0}" -ge 2 ]; then
    ok "1699-r36-rf-consumers: both consumers of the record read 5 fields ($n_rd_ found)"
  else
    bad "1699-r36-rf-consumers: a consumer still reads 4 fields, so required-features would be appended to the path field silently ($n_rd_ found)"
  fi
  # the comparison must happen BEFORE the loop needs it, and the gap must be reported
  ord_ok_=1
  ln_res_=$(grep -nE 'lh_enabled=\$\(_resolved_package_features' "$lh_body_" | head -1 | cut -d: -f1)
  ln_loop_=$(grep -nE 'read -r _mt_name' "$lh_body_" | head -1 | cut -d: -f1)
  [ -n "$ln_res_" ] && [ -n "$ln_loop_" ] && [ "$ln_res_" -lt "$ln_loop_" ] || ord_ok_=0
  if [ "$ord_ok_" -eq 1 ]; then
    ok "1699-r36-rf-order: the feature set is resolved (line $ln_res_) BEFORE the target loop that compares against it (line $ln_loop_)"
  else
    bad "1699-r36-rf-order: the resolve is not before the loop (resolve=$ln_res_ loop=$ln_loop_) — the comparison would run against an EMPTY set, mark every target unmet, and silently empty the lane"
  fi
  if [ "$(grep -cF 'NOT invoked' "$lh_body_")" -gt 0 ]; then
    ok "1699-r36-rf-declared: an excluded target is DECLARED as a coverage gap, not silently dropped"
  else
    bad "1699-r36-rf-declared: an excluded target is dropped with no output — the subject set shrinks with no trace, which is the defect this whole change exists to remove"
  fi
fi

# --- 40. #1699: round-37 — decide before you record, and block comments in BOTH scanners --
#
# (a) MEDIUM: the required-features check sat AFTER `observe_ids+=(...)`, so a target the lane
# deliberately does not invoke was still demanded to have a `Running` banner by
# `check_test_targets_observed` — a FALSE RED produced by the fix meant to prevent one. Asserted by
# LINE ORDER, because the property is "the decision precedes every record of the target".
lh40_="$tmp/1699-r37-lh.txt"
awk '/^run_legacy_heuristics\(\) \{/, /^\}/' "$GATE" > "$lh40_"
if [ ! -s "$lh40_" ]; then
  bad "1699-r37-order-scope: could not extract run_legacy_heuristics — this assert would pass vacuously"
else
  # COMMENTS STRIPPED FIRST. The first cut compared raw line numbers and matched the very comment
  # that DESCRIBES the defect ("...sat AFTER `observe_ids+=(...)`..."), reporting observe=75 against
  # decision=87 — an oracle reading its own rationale as evidence, which is the trap CLAUDE.md
  # records and which already cost this file a false FAIL once (the round-14 GNU lint).
  lh40c_="$tmp/1699-r37-lh-code.txt"
  sed 's/[[:space:]]*#.*$//' "$lh40_" > "$lh40c_"
  d_=$(grep -nE '_rf_off=""' "$lh40c_" | head -1 | cut -d: -f1)
  o_=$(grep -nE 'observe_ids\+=' "$lh40c_" | head -1 | cut -d: -f1)
  t_=$(grep -nE 'targets\+=\(--test' "$lh40c_" | head -1 | cut -d: -f1)
  if [ -n "$d_" ] && [ -n "$o_" ] && [ -n "$t_" ] && [ "$d_" -lt "$o_" ] && [ "$d_" -lt "$t_" ]; then
    ok "1699-r37-decide-first: the required-features decision (line $d_) precedes observe_ids ($o_) and --test ($t_)"
  else
    bad "1699-r37-decide-first: a target is RECORDED before the decision to invoke it (decision=$d_ observe=$o_ test=$t_) — check_test_targets_observed then demands a banner for a target the lane never runs, which is a false red on valid code"
  fi
fi

# (b) LOW: the crate-gate leading-region scanner must treat block comments as trivia — the same
# shape round 36 fixed in `_legacy_coreq_sites` and did NOT carry to this sibling. A leading
# `/* … */` or `/*! … */` (idiomatic module docs) was read as the first ITEM, so every crate-level
# `#![cfg(...)]` after it vanished from the Flight census.
if [ -s "$gg_body_" ]; then
  for case_ in blockhdr innerdoc multilinehdr; do
    case "$case_" in
      blockhdr)      printf '/* file header */\n#![cfg(feature = "x")]\nfn t() {}\n' > "$gg_src_/$case_.rs" ;;
      innerdoc)      printf '/*! module docs */\n#![cfg(feature = "x")]\nfn t() {}\n' > "$gg_src_/$case_.rs" ;;
      multilinehdr)  printf '/* line one\n   line two\n   line three */\n#![cfg(feature = "x")]\nfn t() {}\n' > "$gg_src_/$case_.rs" ;;
    esac
    got_=$(
      export GG_SRC="$gg_src_/$case_.rs" GG_REL="tests/$case_.rs"
      _package_test_targets_gated() { printf '%s\t%s\t%s\t%s\t%s\n' "${case_}_t" "$GG_SRC" source "$GG_REL" ""; }
      # shellcheck disable=SC1090
      . "$gg_body_"
      _crate_gated_test_targets somepkg 2>/dev/null | awk -F'\t' '{print $3}'
    )
    case "$got_" in
      L*'#![cfg(feature = "x")]'*)
      ok "1699-r37-blockhdr[$case_]: a gate after a block comment is still reported"
      ;;
      *) bad "1699-r37-blockhdr[$case_]: reported '$got_' — the occurrence vanished, so the target reads as having no inner cfg attribute" ;;
    esac
  done
fi

# (c) LOW: the library census scan must distinguish "no matches" (grep 1) from a read error
# (grep >= 2). Suppressing both meant an unreadable directory produced an empty list, reported as a
# clean ZERO-GAP census — a census nobody took, presented as a census with nothing to report.
if [ -s "$lh40_" ]; then
  if [ "$(grep -cE '_libsrc_rc' "$lh40_")" -gt 0 ] && [ "$(grep -cE '_libsrc_rc" -ge 2|_libsrc_rc\" -ge 2|-ge 2' "$lh40_")" -gt 0 ]; then
    ok "1699-r37-libscan: the library census scan checks its exit status and fails only on a real error (>=2), not on 'no matches'"
  else
    bad "1699-r37-libscan: the library census scan ignores its exit status again — an unreadable source directory yields an empty list and is reported as a clean zero-gap census"
  fi
fi

# --- 41. #1699: the unmet-target diagnostic must name the TARGET (round-38, Medium) -------
#
# `rf_unmet` used `$base`, which round 37's hoist left assigned ~60 lines BELOW the append — so the
# diagnostic named the previous iteration's target, or nothing on the first. A census that
# misattributes its own gaps is worse than one that omits them: it sends the reader to a target that
# is fine. Structural, over the extracted function with comments stripped (an earlier assert in this
# file matched the comment describing its own defect).
lh41_="$tmp/1699-r38-lh.txt"
awk '/^run_legacy_heuristics\(\) \{/, /^\}/' "$GATE" | sed 's/[[:space:]]*#.*$//' > "$lh41_"
if [ ! -s "$lh41_" ]; then
  bad "1699-r38-attrib-scope: could not extract run_legacy_heuristics — this assert would pass vacuously"
elif [ "$(grep -cE 'rf_unmet="\$rf_unmet \$_mt_name' "$lh41_")" -gt 0 ] \
  && [ "$(grep -cE 'rf_unmet="\$rf_unmet \$base' "$lh41_")" -eq 0 ]; then
  ok "1699-r38-attrib: the unmet-target gap names \$_mt_name (available at the top of the loop), not \$base (assigned later)"
else
  bad "1699-r38-attrib: the gap diagnostic is back to a variable assigned AFTER the append — it will name the previous target or nothing, misattributing the census gap"
fi

# --- 42. #1699: root-pass findings — decide, then filter, then record; and no fail-open scans ---
#
# The root-checkout roborev pass on the GATED sha found three, all fail-open or mis-scoped, and the
# gate was green at the time — which is the whole argument for running a review on the certified sha.
#
# (a) ORDER: membership decides whether the target is our SUBJECT; required-features decides whether
# we may INVOKE it; nothing may RECORD it before both. Round 37 hoisted the filter above the
# membership test and the census then invented gaps: 5 measured false claims in shipped output, none
# legacy-gated (issue_1495_arrow_accessor_parity(arrow), issue_1695_query_timeout(cli-helpers),
# issue_1869_big_clustering_slice_readat(work-counters), issue_2148_statistics_toc_single_walk,
# issue_2302_written_index_resolve). A census that invents gaps is worse than one that omits them.
lh42_="$tmp/1699-r43-lh.txt"
awk '/^run_legacy_heuristics\(\) \{/, /^\}/' "$GATE" | sed 's/[[:space:]]*#.*$//' > "$lh42_"
if [ ! -s "$lh42_" ]; then
  bad "1699-r43-order-scope: could not extract run_legacy_heuristics — these asserts would pass vacuously"
else
  m_=$(grep -nE '_mt_hit" -eq 1' "$lh42_" | head -1 | cut -d: -f1)
  f_=$(grep -nE '_rf_off=""' "$lh42_" | head -1 | cut -d: -f1)
  r_=$(grep -nE 'rf_unmet="' "$lh42_" | tail -1 | cut -d: -f1)
  o_=$(grep -nE 'observe_ids\+=' "$lh42_" | head -1 | cut -d: -f1)
  t_=$(grep -nE 'targets\+=\(--test' "$lh42_" | head -1 | cut -d: -f1)
  if [ -n "$m_" ] && [ -n "$f_" ] && [ -n "$o_" ] && [ -n "$t_" ] \
     && [ "$m_" -lt "$f_" ] && [ "$f_" -lt "$o_" ] && [ "$f_" -lt "$t_" ]; then
    ok "1699-r43-order: membership ($m_) precedes the required-features filter ($f_), which precedes every record (observe_ids $o_, --test $t_)"
  else
    bad "1699-r43-order: the loop decides in the wrong order (membership=$m_ filter=$f_ record=$o_/$t_) — filtering before membership makes the census invent gaps for targets that are not its subject; recording before filtering demands a banner for a target never invoked"
  fi
  # (b) the membership scan must be TRI-STATE: match / no match / ERROR
  if [ "$(grep -cE 'grep -cE "\$cfg_site" "\$_mt_cf"\)?; _mt_rc=\$\?|_mt_rc" -ge 2' "$lh42_")" -ge 1 ] \
     && [ "$(grep -cE 'grep -cE "\$cfg_site" "\$_mt_cf" 2>/dev/null' "$lh42_")" -eq 0 ]; then
    ok "1699-r43-membership-tristate: the cfg-site scan captures grep's status and fails on >=2, instead of reading a read-error as 'no legacy site'"
  else
    bad "1699-r43-membership-tristate: the cfg-site scan is back to swallowing errors — a scan failure then silently drops the target from the lane, and a dropped target cannot fail the zero-tests guard, so an empty run passes"
  fi
fi

# (c) the census occurrence scan must not end in `|| true`
gg43_="$tmp/1699-r43-gg.txt"
awk '/^_crate_gated_test_targets\(\) \{/, /^\}/' "$GATE" | sed 's/[[:space:]]*#.*$//' > "$gg43_"
if [ ! -s "$gg43_" ]; then
  bad "1699-r43-census-scope: could not extract _crate_gated_test_targets — this assert would pass vacuously"
elif [ "$(grep -cE '\|\| true' "$gg43_")" -eq 0 ] && [ "$(grep -cE '_gr_rc" -ge 2' "$gg43_")" -gt 0 ]; then
  ok "1699-r43-census-tristate: the occurrence scan propagates read errors (grep >=2) instead of ending in '|| true'"
else
  bad "1699-r43-census-tristate: the occurrence scan swallows failures again — a partial or failed scan is then reported as 'no gated occurrences', which is the census's own all-clear produced by the census failing"
fi

# LOWERED DELIBERATELY, 300 -> 285, and this is the ratchet working rather than being defeated.
# Roborev round 27's descope removed the crate-gate CLASSIFIER, and with it 25 assertions that
# tested a feature grammar, a conjunction evaluator and a selector predicate which no longer exist.
# Their replacement (section 33, the verbatim-observation cases) is smaller BECAUSE nothing is
# interpreted any more. A floor must be moved consciously when the subject legitimately shrinks —
# otherwise it becomes the thing people edit to make a run green, which is the failure it exists to
# prevent. It caught this shrink on the first run: 294 against a floor of 300.
# --- 43. #1699: root pass at aabae56ea — a cfg-GATED child module, and no false "verbatim" ---
# Medium: the closure followed child modules while DISCARDING the cfg attributes gating them, so
# `#[cfg(feature = "experimental")] mod child;` read as reachable at this lane feature set — a
# legacy-gated test inside `child` counted as executable while an ungated sibling kept the target
# nonzero, and the co-required census reported NO gap. Low: the occurrence report claimed to be
# verbatim while capturing only the OPENING line of a multiline attribute, and the assert that was
# supposed to pin it only checked the `L<line>:` prefix — so it could not see the truncation.
cg_h="$tmp/1699-cfggate-fn.sh"
awk '/^_rust_module_closure\(\) \{/,/^\}/' "$GATE" > "$cg_h"
if ! grep -q 'CFG-GATED-MOD' "$cg_h"; then
  bad "1699-cfggate-extract: extracted closure has no CFG-GATED-MOD report — extraction broke or the fix is gone, so the cases below would pass vacuously"
else
  ok "1699-cfggate-extract: extracted the closure and it carries the CFG-GATED-MOD report"

  cg_root="$tmp/1699-cfggate"; mkdir -p "$cg_root/tests"
  printf '#[cfg(feature = "experimental")]\nmod gated_child;\nmod plain_child;\n' > "$cg_root/tests/preceding.rs"
  printf '#[cfg(feature = "experimental")] mod gated_inline;\n' > "$cg_root/tests/inline.rs"
  printf 'mod plain_child;\n' > "$cg_root/tests/ungated.rs"
  : > "$cg_root/tests/gated_child.rs"; : > "$cg_root/tests/plain_child.rs"; : > "$cg_root/tests/gated_inline.rs"

  # shellcheck source=/dev/null
  # A BRACES GROUP, NEVER A SUBSHELL. `bad()` increments a shell variable, so a failing assert
  # inside `( … )` PRINTS its FAIL line and is never counted — the suite would report
  # "failed: 0" and exit 0 with 22 assert calls unaccounted for. That is the vacuous pass this
  # suite exists to detect, in the suite itself; the tally, the floor and the exit status all
  # depend on the counters living in ONE shell. Introduced here and caught by the tally not
  # growing when four asserts were added.
  { . "$cg_h"

    cg_out=$(_rust_module_closure "$cg_root/tests/preceding.rs" 2>"$tmp/1699-cg-e1.txt")
    if grep -q '^CFG-GATED-MOD gated_child ' "$tmp/1699-cg-e1.txt"; then
      ok "1699-cfggate-preceding: a cfg on the line ABOVE \`mod\` is reported, not silently followed"
    else
      bad "1699-cfggate-preceding: \`#[cfg(...)]\` + \`mod gated_child;\` produced no CFG-GATED-MOD — a gated child would count as executable coverage"
    fi
    # the cfg TEXT must travel with the report: file+line alone cannot be compared to a feature set
    if grep -q 'experimental' "$tmp/1699-cg-e1.txt"; then
      ok "1699-cfggate-text: the report carries the gating cfg text"
    else
      bad "1699-cfggate-text: CFG-GATED-MOD named the module but not the cfg gating it"
    fi
    # and it must NOT leak onto the next declaration
    if grep -q '^CFG-GATED-MOD plain_child ' "$tmp/1699-cg-e1.txt"; then
      bad "1699-cfggate-noleak: the gate text leaked onto the UNGATED sibling — a false gap report on ordinary code teaches agents to waive this lane"
    else
      ok "1699-cfggate-noleak: the gate text does not leak onto the ungated sibling"
    fi
    # the child is still RESOLVED — the source set must stay complete, only its status is unknown
    if out_has "$cg_out" 'gated_child.rs'; then
      ok "1699-cfggate-resolved: the gated child is still in the source set (reported, not dropped)"
    else
      bad "1699-cfggate-resolved: the gated child vanished from the source set — dropping it is the SILENT direction this fix exists to close"
    fi

    _rust_module_closure "$cg_root/tests/inline.rs" >/dev/null 2>"$tmp/1699-cg-e2.txt"
    if grep -q '^CFG-GATED-MOD gated_inline ' "$tmp/1699-cg-e2.txt"; then
      ok "1699-cfggate-inline: the SAME-LINE \`#[cfg(...)] mod x;\` form is reported too"
    else
      bad "1699-cfggate-inline: same-line cfg+mod produced no report — the form the mod rule sees directly"
    fi

    # A cfg attached to something that is NOT a module must not leak forward onto a later
    # ungated `mod` (roborev job 97, Medium). The noleak case above cannot see this: it only
    # covered a cfg attached to a mod, where the mod rule resets the pending text itself.
    printf '#[cfg(feature = "experimental")]\nfn helper() {}\n\nmod plain_child;\n' > "$cg_root/tests/leakfn.rs"
    printf '#[cfg(feature = "experimental")]\nstruct S;\nmod plain_child;\n' > "$cg_root/tests/leakstruct.rs"
    for lk in leakfn leakstruct; do
      _rust_module_closure "$cg_root/tests/$lk.rs" >/dev/null 2>"$tmp/1699-cg-$lk.txt"
      if [ -s "$tmp/1699-cg-$lk.txt" ]; then
        bad "1699-cfggate-leak[$lk]: a cfg on a non-module item leaked onto a later UNGATED mod — a false gap report on ordinary code: $(tr '\n' ' ' < "$tmp/1699-cg-$lk.txt")"
      else
        ok "1699-cfggate-leak[$lk]: a cfg on a non-module item does not tag the following ungated mod"
      fi
    done

    # A MULTILINE parent gate (roborev job 99, Medium). rustfmt writes `#[cfg(all(` across lines,
    # and those continuation lines match no attribute pattern — so the cluster-end reset destroyed
    # the pending gate text and the child read as UNCONDITIONAL. This is a REGRESSION FIXTURE in
    # the strict sense: the defect was introduced by the fix for the cfg-on-a-function leak, since
    # clearing gatetxt at cluster end is exactly what made a continuation line destructive.
    mkdir -p "$cg_root/tests/support"
    : > "$cg_root/tests/support/datasets_root.rs"
    printf '#[cfg(all(\n    feature = "state_machine",\n    feature = "cli-helpers"\n))]\n#[path = "support/datasets_root.rs"]\nmod gated_child;\n\nmod plain_child;\n' > "$cg_root/tests/multiattr.rs"
    mc_out=$(_rust_module_closure "$cg_root/tests/multiattr.rs" 2>"$tmp/1699-cg-multi.txt")
    if grep -q '^CFG-GATED-MOD gated_child ' "$tmp/1699-cg-multi.txt"; then
      ok "1699-cfggate-multiline: a MULTILINE parent gate is still reported (continuation lines do not clear the pending cfg)"
    else
      bad "1699-cfggate-multiline: a rustfmt-valid multiline #[cfg(all(...))] before \`mod\` produced NO report — the child reads as unconditional and a gated test inside it counts as executable"
    fi
    # the WHOLE condition must survive, not just the opening line: the reader is told to compare
    # it against the enabled feature set, and `cfg(all(` alone cannot be compared to anything
    if grep -q 'cli-helpers' "$tmp/1699-cg-multi.txt"; then
      ok "1699-cfggate-multitext: the full multiline condition is carried in the report"
    else
      bad "1699-cfggate-multitext: only the opening line of the multiline cfg was reported — the conditions the reader must compare are missing"
    fi
    # and the ungated sibling after a MULTILINE cluster must still be clean
    if grep -q '^CFG-GATED-MOD plain_child ' "$tmp/1699-cg-multi.txt"; then
      bad "1699-cfggate-multileak: the multiline gate leaked onto the ungated sibling that follows it"
    else
      ok "1699-cfggate-multileak: the multiline gate does not leak onto the following ungated sibling"
    fi
    # the child must still RESOLVE through its #[path], i.e. the balance rule did not eat the path
    if out_has "$mc_out" 'support/datasets_root.rs'; then
      ok "1699-cfggate-multipath: the #[path] after a multiline cfg still resolves the child"
    else
      bad "1699-cfggate-multipath: the child did not resolve through its #[path] — the balance rule swallowed the path attribute, shrinking the source set"
    fi

    # A DELIMITER INSIDE A STRING LITERAL (roborev job 101, Medium). `doc = "]"` closed the
    # cluster EARLY, so the real closing line cleared the pending cfg and the child read as
    # UNCONDITIONAL — it HID a gap. The comment at that seam had claimed the skew could only ever
    # report a gap that was not there; true of an unmatched `[`, false of an unmatched `]`.
    printf '#[cfg(all(\n    feature = "state_machine",\n    doc = "]"\n))]\nmod gated_child;\n' > "$cg_root/tests/strbracket.rs"
    _rust_module_closure "$cg_root/tests/strbracket.rs" >/dev/null 2>"$tmp/1699-cg-strb.txt"
    if grep -q '^CFG-GATED-MOD gated_child ' "$tmp/1699-cg-strb.txt"; then
      ok "1699-cfggate-strdelim: a `]` inside an attribute string literal does not close the cluster early"
    else
      bad "1699-cfggate-strdelim: a delimiter inside a string literal closed the cluster early, so the gated child read as unconditional and the gap was HIDDEN"
    fi
    # A shape the strip CANNOT handle must be DECLARED, not resolved. Carrying an UNCLASSIFIED
    # marker forward did NOT work — the cluster-end rule wiped it before any `mod` saw it, so the
    # code meant to declare the unknown hid it instead. Declared at the point of detection now.
    printf '#[cfg(all(\n    feature = "state_machine",\n    doc = r#"weird ) ] stuff"#\n))]\nmod gated_child;\n' > "$cg_root/tests/rawstr.rs"
    _rust_module_closure "$cg_root/tests/rawstr.rs" >/dev/null 2>"$tmp/1699-cg-raw.txt"
    if grep -q 'UNCLASSIFIED' "$tmp/1699-cg-raw.txt"; then
      ok "1699-cfggate-unmodelled: a raw-string attribute shape is DECLARED unclassified, not silently resolved"
    else
      bad "1699-cfggate-unmodelled: an unmodelled string shape produced NO report — the set of Rust attribute shapes this scan does not model is OPEN, so silence there is a hidden gap"
    fi

    _rust_module_closure "$cg_root/tests/ungated.rs" >/dev/null 2>"$tmp/1699-cg-e3.txt"
    if [ -s "$tmp/1699-cg-e3.txt" ]; then
      bad "1699-cfggate-quiet: an UNGATED module tree produced stderr output — the caller FAILs on any stderr, so this reds the lane on ordinary code: $(cat "$tmp/1699-cg-e3.txt")"
    else
      ok "1699-cfggate-quiet: an ungated module tree stays silent (no false fail-closed)"
    fi
  }
fi

# the caller must name the RIGHT cause — a wrong diagnosis costs the next reader the investigation
# `grep -c`, NOT `| grep -q`: under `set -o pipefail` a matching `grep -q` exits immediately, awk
# dies of SIGPIPE, and the PIPELINE reports 141 on a SUCCESSFUL match — the #3380 shape this very PR
# documents. It bit this assert first time out, reporting the fix absent while it was present.
lh_fn=$(awk '/^run_legacy_heuristics\(\) \{/,/^\}/' "$GATE")
if [ "$(printf '%s' "$lh_fn" | grep -cF 'CFG-GATED-MOD')" -gt 0 ]; then
  ok "1699-cfggate-cause: run_legacy_heuristics distinguishes the cfg-gated report from unresolved"
else
  bad "1699-cfggate-cause: the caller does not mention CFG-GATED-MOD — it would report a cfg-gated child as 'could not resolve the module tree', the wrong remedy"
fi
# THE SPLIT, both directions. Failing the lane on a cfg-gated mod was tried and reverted: the tree
# legitimately carries `#[cfg(all(feature=..))] #[path=..] mod support;` on shared test helpers, and
# a lane that reds on correct input is the lane agents learn to waive. So the gap must be DECLARED
# and must NOT reach the fatal branch — two separate claims, asserted separately.
if [ "$(printf '%s' "$lh_fn" | grep -cF "grep -v '^CFG-GATED-MOD '")" -gt 0 ]; then
  ok "1699-cfggate-split: the fatal branch is fed the NON-cfg-gated half only"
else
  bad "1699-cfggate-split: the fatal branch is not filtered — a cfg-gated helper module would FAIL the lane on ordinary committed code (measured: 3 such targets in cqlite-core)"
fi
if [ "$(printf '%s' "$lh_fn" | grep -cF 'DECLARED GAP')" -gt 0 ]; then
  ok "1699-cfggate-declared: an unevaluated subtree is DECLARED, not silently followed"
else
  bad "1699-cfggate-declared: no DECLARED GAP report — an unclassified subtree would be invisible, which is the silent direction the finding names"
fi
if [ "$(printf '%s' "$lh_fn" | grep -cF 'cfg-gated-subtree gaps:')" -gt 0 ]; then
  ok "1699-cfggate-census: the gap COUNT reaches the census (a counter nobody reads is the same defect one level down)"
else
  bad "1699-cfggate-census: the gap count is tracked but never reported — the census would still read as if every module were reached unconditionally"
fi
# and the count must be affirmative in BOTH states: a key with no subject still has to say so
if [ "$(printf '%s' "$lh_fn" | grep -cF 'gaps: 0 RECOGNISED')" -gt 0 ]; then
  ok "1699-cfggate-zero: the zero case is stated affirmatively AND qualified as RECOGNISED, so a pasted census shows the check ran without reading as verified absence"
else
  bad "1699-cfggate-zero: no affirmative qualified zero line — a bare 0 reads as verified absence, and no zero line at all is indistinguishable from the scan not running"
fi

# A gap is DECLARED only for a confirmed SUBJECT of the lane (roborev job 97, Medium). Declaring
# at the closure — before membership and required-features — reported gaps for targets carrying no
# legacy site at all (measured: issue_2827_partition_access_bytes), and a census diluted with
# irrelevant entries is a census nobody reads. Same decide-then-record ordering as observe_ids.
lh_code=$(printf '%s\n' "$lh_fn" | sed 's/^[[:space:]]*#.*$//')
lh_gapline=$(printf '%s\n' "$lh_code" | grep -n 'lh_gap_detail+=' | head -1 | cut -d: -f1)
lh_obsline=$(printf '%s\n' "$lh_code" | grep -n 'observe_ids+=' | head -1 | cut -d: -f1)
lh_memline=$(printf '%s\n' "$lh_code" | grep -n '_mt_hit" -eq 1' | head -1 | cut -d: -f1)
if [ -n "$lh_gapline" ] && [ -n "$lh_memline" ] && [ "$lh_memline" -lt "$lh_gapline" ]; then
  ok "1699-cfggate-order: membership (line $lh_memline) is decided BEFORE a gap is recorded (line $lh_gapline)"
else
  bad "1699-cfggate-order: a cfg-gated gap is recorded at line ${lh_gapline:-?} but membership is decided at line ${lh_memline:-?} — a non-subject target would be declared a coverage gap"
fi
if [ -n "$lh_gapline" ] && [ -n "$lh_obsline" ] && [ "$lh_gapline" -lt "$lh_obsline" ]; then
  ok "1699-cfggate-atsubject: the gap is recorded at the point the target becomes a subject"
else
  bad "1699-cfggate-atsubject: gap record (${lh_gapline:-?}) is not adjacent to observe_ids (${lh_obsline:-?})"
fi
# and it must not be written to a log the census then truncates with `>` (job 97, Low)
lh_gaptext=$(printf '%s\n' "$lh_code" | grep -F 'DECLARED GAP' || true)
lh_gapbad=$(printf '%s\n' "$lh_gaptext" | grep -vF 'lh_gap_detail+=' | grep -c . || true)
if [ "${lh_gapbad:-0}" -gt 0 ]; then
  bad "1699-cfggate-persist: 'DECLARED GAP' text appears outside lh_gap_detail+= ($lh_gapbad line(s)) — an emit outside the buffer is either declared before membership or truncated by the census '>' redirect"
else
  ok "1699-cfggate-persist: the gap text exists only as buffered census detail, so it is neither premature nor truncated"
fi
if [ "$(printf '%s' "$lh_fn" | grep -cF 'lh_census+=("  $_gd")')" -gt 0 ]; then
  ok "1699-cfggate-incensus: the gap DETAIL travels in the census, so it survives into the component log"
else
  bad "1699-cfggate-incensus: the gap detail never reaches lh_census — only the aggregate count would land in the log a reader inspects"
fi

# Low: the report must not CLAIM verbatim text it does not capture.
if [ "$(awk '/^_crate_gated_test_targets\(\) \{/,/^\}/' "$GATE" | sed 's/^[[:space:]]*#.*$//' | grep -ci 'verbatim')" -gt 0 ]; then
  bad "1699-cfgsite-noverbatim: the occurrence report still claims 'verbatim' while capturing only an attribute opening line — the claim is falsifiable by any multiline #![cfg(all("
else
  ok "1699-cfgsite-noverbatim: the occurrence report no longer claims verbatim capture"
fi
# and the truncation must be MARKED, tested on real multiline input rather than on the prefix alone
cs_fn=$(awk '/^_crate_gated_test_targets\(\) \{/,/^\}/' "$GATE")
if out_has "$cs_fn" 's/\$/+/'; then
  ok "1699-cfgsite-marker: a continued attribute is marked with a truncation indicator"
else
  bad "1699-cfgsite-marker: no truncation marker in the cfg-site report — a multiline attribute is silently reported as if complete"
fi
cs_multi="$tmp/1699-cfgsite-multi.rs"
printf '#![cfg(all(\n    test,\n    feature = "legacy-heuristics"\n))]\nfn x() {}\n' > "$cs_multi"
cs_single="$tmp/1699-cfgsite-single.rs"
printf '#![cfg(feature = "legacy-heuristics")]\nfn y() {}\n' > "$cs_single"
cs_render() { # replicate the report pipeline over one file
  local _o; _o=$(grep -nE '^[[:space:]]*#!\[[[:space:]]*cfg(_attr)?[[:space:]]*\(' "$1") || true
  printf '%s\n' "$_o" | sed 's/^\([0-9]*\):[[:space:]]*/L\1: /' \
    | sed 's/$/+/; s/\()\][[:space:]]*\)+$/\1/' | tr '\n' ' ' | sed 's/  */ /g; s/^ //; s/ $//'
}
if [ "$(cs_render "$cs_multi")" = 'L1: #![cfg(all(+' ]; then
  ok "1699-cfgsite-multiline: a multiline attribute renders with the truncation marker"
else
  bad "1699-cfgsite-multiline: multiline attribute rendered as [$(cs_render "$cs_multi")], expected the opening line plus a truncation marker"
fi
if [ "$(cs_render "$cs_single")" = 'L1: #![cfg(feature = "legacy-heuristics")]' ]; then
  ok "1699-cfgsite-singleline: a COMPLETE single-line attribute is not falsely marked truncated"
else
  bad "1699-cfgsite-singleline: single-line attribute rendered as [$(cs_render "$cs_single")] — a false truncation marker on complete input"
fi

# --- 44. #1699: the CENSUS a reader pastes must not claim an exhaustive all-clear (job 108) -----
# The seam disclaimer is read by whoever EDITS the scanner. These two lines are what every agent
# reading a gate log sees, and they said "every legacy-heuristics-gated cfg site is reachable" and
# "every module reached is reached unconditionally" — universal claims from a scan the same file
# declares NON-EXHAUSTIVE forty lines above. That is this change's own headline sentence applied to
# the one place it had not been: a clean result presented as a verified one. Pinned here because the
# qualifier is otherwise one careless edit from regressing, and this is the third unpinned claim on
# this branch to drift.
lh_fn44=$(awk '/^run_legacy_heuristics\(\) \{/,/^\}/' "$GATE")
# every affirmative-ZERO census line must say RECOGNISED — a bare 0 reads as verified absence
z_lines=$(printf '%s\n' "$lh_fn44" | grep -E 'lh_census\+=\("(co-required-feature census|cfg-gated-subtree gaps): 0' || true)
z_n=$(printf '%s\n' "$z_lines" | grep -c . || true)
if [ "${z_n:-0}" -eq 0 ]; then
  bad "1699-census-zero-present: found NO affirmative-zero census line — either the census stopped reporting the zero case (so absence of a gap is indistinguishable from the scan not running) or this assert stopped matching"
else
  ok "1699-census-zero-present: found $z_n affirmative-zero census line(s) to check"
  z_bad=$(printf '%s\n' "$z_lines" | grep -vF 'RECOGNISED' | grep -c . || true)
  if [ "${z_bad:-0}" -gt 0 ]; then
    bad "1699-census-recognised: $z_bad affirmative-zero census line(s) report a bare 0 without RECOGNISED — an agent pasting that census reads a verified all-clear from a non-exhaustive scan"
  else
    ok "1699-census-recognised: every affirmative-zero census line qualifies its 0 as RECOGNISED"
  fi
fi
# and the non-exhaustiveness must be stated ON the census, not only in the seam comments
if [ "$(printf '%s' "$lh_fn44" | grep -cE 'lh_census\+=\("[^"]*(NON-EXHAUSTIVE|does not recognise is invisible)')" -ge 4 ]; then
  ok "1699-census-nonexhaustive: every census branch — zero AND non-zero — states its own non-exhaustiveness in the emitted text"
else
  bad "1699-census-nonexhaustive: the emitted census does not state that the scan is non-exhaustive — the disclaimer would reach maintainers reading the source and not agents reading a gate log"
fi
# The NON-ZERO branch is the one that renders on any tree that actually has sites, so it is pinned
# BY BRANCH and not merely by an occurrence count: the first version of this fix qualified only the
# zero branches, and a count-based assert would have accepted it while the rendered census stayed
# unqualified. Found by running the lane, which is why the emitted surface is asserted at all.
if [ "$(printf '%s' "$lh_fn44" | grep -A6 'lh_census+=("  where:' | grep -cF 'NON-EXHAUSTIVE')" -gt 0 ]; then
  ok "1699-census-nonzero-branch: the POPULATED census branch (the one that renders when sites exist) states its non-exhaustiveness"
else
  bad "1699-census-nonzero-branch: the populated census branch lists sites under 'WHAT THIS LANE DOES NOT EXECUTE' with no non-exhaustiveness note — a reader takes the list for the complete set of omissions"
fi

# the exact falsified wordings must never come back
for phrase in "every legacy-heuristics-gated cfg site is reachable" "every module reached is reached unconditionally"; do
  if [ "$(printf '%s' "$lh_fn44" | grep -cF "$phrase")" -gt 0 ]; then
    bad "1699-census-noregress: the census again claims '$phrase' — a universal the scan cannot support"
  else
    ok "1699-census-noregress: the census no longer claims '$(echo "$phrase" | cut -c1-34)...'"
  fi
done

# --- 45. #1699: required-features must be validated for EVERY included target (job 111) --------
# The check used to sit INSIDE `if [ "$_mt_how" != "manifest" ]`, so a target cargo itself gates on
# the feature skipped validation and was then passed to cargo explicitly — which rejects a target
# whose required-features are unmet, FAILING the lane on a correct target.
#
# MEASURED BY if/fi DEPTH, NOT BY INDENTATION. The first version of this assert compared indent
# widths and PASSED ON THE BROKEN CODE: in the defective version the check was written at the SAME
# 4-space indent as the branch that contained it, which is very likely why the nesting error went
# unnoticed by every human reader for 46 rounds. Indentation is a PROXY for nesting; the property
# is nesting. Counting `if` openers against `fi` closers is the property itself.
lh_fn45=$(awk '/^run_legacy_heuristics\(\) \{/,/^\}/' "$GATE")
rf_scope=$(printf '%s\n' "$lh_fn45" | awk '
  BEGIN { started = 0; depth = 0; verdict = "NOT-FOUND" }
  # open the tracked region at the manifest branch
  !started && /^[[:space:]]*if \[ "\$_mt_how" != "manifest" \]/ { started = 1; depth = 1; next }
  !started { next }
  # the required-features check: INSIDE if we are still nested, OUTSIDE once depth hit 0
  /^[[:space:]]*if \[ -n "\$\{_mt_rf:-\}" \]/ && verdict == "NOT-FOUND" {
    verdict = (depth > 0) ? "INSIDE" : "OUTSIDE"
  }
  # if/fi only: they nest independently of while/done, so this balances correctly
  /^[[:space:]]*(el)?if [^;]*; then$|^[[:space:]]*if .*; then$/ { depth++ }
  /^[[:space:]]*fi$/ { depth-- }
  END { print verdict }')
case "$rf_scope" in
  OUTSIDE) ok "1699-rf-scope: the required-features check sits OUTSIDE the manifest branch (if/fi depth), so manifest-gated targets are validated too" ;;
  INSIDE)  bad "1699-rf-scope: the required-features check is nested INSIDE the manifest branch — a manifest-gated target skips validation and is then handed to cargo, which rejects it and reds the lane on a CORRECT target" ;;
  *)       bad "1699-rf-scope-extract: could not locate the manifest branch or the required-features check (verdict '$rf_scope') — the assert would otherwise pass vacuously" ;;
esac

# --- 46. #1699: a DECLARED coverage gap must reach the component log, not only stdout ----------
# Found by probing the job-111 fix with a synthetic manifest-gated target: the exclusion WAS
# declared — on stdout. The census opens "$log" with `>`, so anything appended earlier is truncated,
# and a reader inspecting the component log saw nothing. Same class as job 108's Low finding, and
# the file already stated the principle beside the offending echoes.
lh_fn46=$(awk '/^run_legacy_heuristics\(\) \{/,/^\}/' "$GATE")
for decl in rf_unmet negonly; do
  if [ "$(printf '%s' "$lh_fn46" | grep -cE "lh_census\+=\(\"[^\"]*\\\$$decl")" -gt 0 ]; then
    ok "1699-decl-in-log[$decl]: the declaration travels in the census, so it reaches the component log"
  else
    bad "1699-decl-in-log[$decl]: \$$decl is declared on stdout only — the census '>' redirect truncates anything written to \$log before it, so a reader inspecting the component log sees no coverage gap"
  fi
done

# --- 47. #1699: the EMITTED labels must not claim verbatim, and find must not fail open (job 114)
# Two findings, one shape each. (a) `_crate_gated_test_targets` stopped claiming "verbatim" at job
# 101 — but the CENSUS still emitted it, so the retired claim survived on the surface a reader acts
# on. FOURTH instance of source-vs-emitted on this branch, which is why the assert targets EMITTED
# strings specifically. (b) `[ -z "$(find ... 2>/dev/null)" ]` cannot tell "no match" from "find
# failed", and `done < <(find ...)` discards the status entirely — so a partial enumeration
# satisfied a per-subject check over the survivors, which is the empty-subject-set shape this
# component set exists to remove, inside the component set.
gate_src=$(cat "$GATE")
emit_verbatim=$(printf '%s\n' "$gate_src" | grep -nE '^[[:space:]]*(census\+=|lh_census\+=|echo )' | grep -F 'verbatim' | grep -vF 'not verbatim' | grep -c . || true)
if [ "${emit_verbatim:-0}" -gt 0 ]; then
  bad "1699-emit-noverbatim: $emit_verbatim EMITTED string(s) still claim 'verbatim' while the scan captures only an attribute opening line — the retired claim survives on the surface a reader acts on"
else
  ok "1699-emit-noverbatim: no emitted string claims verbatim capture (the sole 'not verbatim' mention is the disclaimer)"
fi
# the opening-line wording must actually be present in the emitted census, not merely absent-of-lie
if [ "$(printf '%s\n' "$gate_src" | grep -cE '(census\+=|echo )[^#]*OPENING LINE')" -ge 3 ]; then
  ok "1699-emit-openingline: the emitted census states that the attribute is an OPENING LINE"
else
  bad "1699-emit-openingline: the emitted census does not say what it DOES report — dropping a false claim without stating the true one leaves the reader guessing"
fi
# (b) both find sites must observe the exit status
if [ "$(printf '%s\n' "$gate_src" | grep -cE 'done < <\(find ')" -gt 0 ]; then
  bad "1699-find-status: a find is consumed by process substitution, whose exit status is DISCARDED — a partial enumeration would satisfy the per-subject checks over the survivors"
else
  ok "1699-find-status: no find is consumed by a status-discarding process substitution"
fi
if [ "$(printf '%s\n' "$gate_src" | sed 's/^[[:space:]]*#.*$//' | grep -cE '\[ -z "\$\(find ')" -gt 0 ]; then
  bad "1699-find-tristate: a find result is tested with [ -z \"\$(find ...)\" ], which reads a FAILED scan as 'no match' — the two need different remedies"
else
  ok "1699-find-tristate: no find result is collapsed onto a two-valued emptiness test"
fi

# --- 48. #1699: the closure-report split must observe grep status (self-review after job 115) ---
# `|| true` on the two splitting greps masked exit >=2, so an unreadable stream yielded two EMPTY
# halves and the code found neither a fatal report nor a gap — a clean pass derived from a failed
# scan. Entry is gated on `[ -s "$_mt_unres" ]`, so "both halves empty" is only reachable via a read
# failure or an unrecognised report: exactly the case the comment above it CLAIMED to fail on and
# had no branch for. Sixth claim-vs-code instance on this branch, and the first found by self-review
# rather than by a reviewer — which matters here because prompt-content absence means nothing proves
# the reviewer received this file.
lh_fn48=$(awk '/^run_legacy_heuristics\(\) \{/,/^\}/' "$GATE")
if [ "$(printf '%s' "$lh_fn48" | grep -cE "grep +(-v )?'\^CFG-GATED-MOD ' \"\\\$_mt_unres\" > \"\\\$_mt_(fatal|gaps)\" +\|\| true")" -gt 0 ]; then
  bad "1699-split-status: a closure-report split grep ends in '|| true', so exit >=2 reads as 'no reports' — a non-empty stream would pass as silence"
else
  ok "1699-split-status: neither splitting grep discards its exit status"
fi
lh_fn48_code=$(printf '%s\n' "$lh_fn48" | sed 's/^[[:space:]]*#.*$//')
sg_missing=""
out_has "$lh_fn48_code" -E '_sp_rc1=\$\?' || sg_missing="$sg_missing status-capture-1"
out_has "$lh_fn48_code" -E '_sp_rc2=\$\?' || sg_missing="$sg_missing status-capture-2"
out_has "$lh_fn48_code" -E '\[ "\$_sp_rc[12]" -ge 2 \]' || sg_missing="$sg_missing status-test"
out_has "$lh_fn48_code" -E '\[ ! -s "\$_mt_fatal" \].*\[ ! -s "\$_mt_gaps" \]' || sg_missing="$sg_missing both-empty-test"
if [ -n "$sg_missing" ]; then
  bad "1699-split-grammar: the closed grammar is not IMPLEMENTED — missing:$sg_missing. Asserted on control flow (comments stripped), because grepping the diagnostic TEXT would stay green if the branch were deleted and its explanation left behind"
else
  ok "1699-split-grammar: the closed grammar is implemented in code — both statuses captured, tested >=2, and the both-halves-empty case decided"
fi

# --- 50. #1699: the polarity scan is THREE-valued and its caller must honour that (job 117) -----
# `[ "$(sed … | grep -c …)" -gt 0 ]` captured only the COUNT, so a failed scan produced empty output,
# compared as 0, and read as "no positive cfg site" — which routes the target into allow_zero, and an
# allowed-zero target that IS positively gated can then run zero tests and PASS. A false green in the
# one scan that must not guess. The caller matters equally: `elif cmd; then` treats every non-zero
# alike, so exit 2 would have taken the same branch as exit 1 — the same fail-open one line away
# from its own fix. Asserted on the FUNCTION and on the CALL SITE, comments stripped.
pol_fn=$(awk '/^_lh_positive_in_closure\(\) \{/,/^\}/' "$GATE" | sed 's/^[[:space:]]*#.*$//')
pol_missing=""
out_has "$pol_fn" -E 'return 2' || pol_missing="$pol_missing fn-returns-2"
out_has "$pol_fn" -E '_pc_rc" -ge 2' || pol_missing="$pol_missing fn-tests-ge2"
if out_has "$pol_fn" -E "sed -E 's/not"; then
  pol_missing="$pol_missing fn-still-strips"
fi
out_has "$pol_fn" -E '_pc_allow=' || pol_missing="$pol_missing fn-has-allowlist"
out_has "$pol_fn" -E '_pc_sites.*-ne.*_pc_allowed' || pol_missing="$pol_missing fn-compares-sites-to-allowed"
if out_has "$pol_fn" -E '\|[[:space:]]*grep'; then
  pol_missing="$pol_missing fn-uses-a-pipeline"
fi
pol_caller=$(awk '/^run_legacy_heuristics\(\) \{/,/^\}/' "$GATE" | sed 's/^[[:space:]]*#.*$//')
out_has "$pol_caller" -E '_lh_positive_in_closure "\$_mt_closure" "\$cfg_site" \|\| _pol_rc=\$\?' \
  || pol_missing="$pol_missing caller-captures-status"
out_has "$pol_caller" -E '\[ "\$_pol_rc" -ge 2 \]' || pol_missing="$pol_missing caller-tests-ge2"
if [ -n "$pol_missing" ]; then
  bad "1699-polarity-tristate: the polarity scan or its caller collapses 'could not tell' onto 'no positive site' — missing:$pol_missing. A failed scan then routes the target into allow_zero and a positively-gated target can pass with zero tests"
else
  ok "1699-polarity-tristate: allow-zero is granted only by an ALLOWLIST of the one recognised direct-negative shape (no strip, so no residue to miss), the scan returns a third state on a read error, and the call site tests for it before the two-valued chain"
fi

# --- 51. #1699: the polarity strip is nesting-blind, so nested negation must not allow-zero (119) -
# BEHAVIOURAL, not structural: the reviewer asked for a double-negation fixture and a structural
# assert would not have caught this. The strip removes every `not(feature = "legacy-heuristics")`
# globally with no nesting awareness, so `not(not(LH))` — a POSITIVE expression — became `not()`,
# matched nothing, and classified negative-only: the target was allow-zero'd and could pass having
# executed no tests. Fixed conservatively rather than with a cfg-expression parser, which is the
# unbounded-surface trap this change is about.
pol_h="$tmp/1699-pol-fn.sh"
awk '/^_lh_positive_in_closure\(\) \{/,/^\}/' "$GATE" > "$pol_h"
if ! grep -q 'POLARITY-UNRECOGNISED' "$pol_h"; then
  bad "1699-pol-extract: extracted polarity fn has no POLARITY-UNRECOGNISED branch — extraction broke or the fix is gone, so the cases below would pass vacuously"
else
  ok "1699-pol-extract: extracted the polarity scan and it carries the unrecognised-shape branch"
  pol_dir="$tmp/1699-pol"; mkdir -p "$pol_dir"
  printf '#[cfg(not(not(feature = "legacy-heuristics")))]\n#[test]\nfn t() {}\n' > "$pol_dir/double.rs"
  printf '#[cfg(not(feature = "legacy-heuristics"))]\n#[test]\nfn t() {}\n'      > "$pol_dir/neg.rs"
  printf '#[cfg(feature = "legacy-heuristics")]\n#[test]\nfn t() {}\n'           > "$pol_dir/pos.rs"
  printf '#[cfg(all(not(feature = "legacy-heuristics"), test))]\nfn t() {}\n'    > "$pol_dir/andneg.rs"
  # shellcheck source=/dev/null
  { . "$pol_h"
    pol_site='feature[[:space:]]*=[[:space:]]*"legacy-heuristics"'
    _pol_case() { # <file> <expected-rc> <label>
      _lh_positive_in_closure "$pol_dir/$1" "$pol_site" >/dev/null 2>&1; local rc=$?
      if [ "$rc" -eq "$2" ]; then
        ok "1699-pol[$1]: $3 (rc=$rc)"
      else
        bad "1699-pol[$1]: expected rc=$2 ($3) but got rc=$rc — a wrong polarity here either excuses a positively-gated target from the zero-tests guard, or reds a legitimately negative one"
      fi
    }
    _pol_case double.rs 0 "nested negation is NOT treated as negative-only, so the target is never allow-zero'd"
    _pol_case neg.rs    1 "a simple negative gate is still negative — the one target relying on allow-zero keeps it"
    _pol_case pos.rs    0 "a plain positive gate is positive"
    # job 120: an OUTER not around a compound containing not(LH). Strips to `not(all(, test))` —
    # no feature reference AND no `not()` residue — so both the strip and job 119's residue check
    # read it as negative-only, while the expression is TRUE when the feature is on.
    printf '#[cfg(not(all(not(feature = "legacy-heuristics"), test)))]\nfn t() {}\n' > "$pol_dir/outer.rs"
    printf '#[cfg(all(feature = "legacy-heuristics", feature = "experimental"))]\nfn t() {}\n' > "$pol_dir/coreq.rs"
    _pol_case outer.rs  0 "an outer not() around a compound containing not(LH) is NOT excusable"
    _pol_case coreq.rs  0 "a co-required positive gate is not excusable"
    # andneg is now POSITIVE, not negative: the allowlist recognises exactly one shape, and
    # `all(not(LH), test)` is not it. Conservative, and it costs only an excusal.
    _pol_case andneg.rs 0 "all(not(LH), test) is not the recognised direct-negative form, so it is not excused"
  }
fi

# --- 52. #1699: the excusal POLICY must reach the component log, not only stdout -----------------
# The per-file POLARITY-UNRECOGNISED notes go to stderr, which lands on gate stdout and NOT in the
# component log — measured 4 on stdout, 0 in the log. Same class as rf_unmet. The census now states
# the policy and the counts, which is what a reader needs to interpret an excusal at all.
lh_fn52=$(awk '/^run_legacy_heuristics\(\) \{/,/^\}/' "$GATE")
pol_cen_missing=""
out_has "$lh_fn52" -E 'lh_census\+=\("polarity: \$\{#allow_zero\[@\]\} of \$count' \
  || pol_cen_missing="$pol_cen_missing counts"
out_has "$lh_fn52" -F 'excusable ONLY when EVERY' || pol_cen_missing="$pol_cen_missing policy"
if [ -n "$pol_cen_missing" ]; then
  bad "1699-pol-census: the excusal policy/counts do not reach the census — missing:$pol_cen_missing. A reader of the component log would see an allowed-zero entry with no way to tell what earned it"
else
  ok "1699-pol-census: the census states how many targets are excusable, out of how many, and the one shape that earns it"
fi

# ============================================================================
# ISSUE #1465 (recomposed onto #3522): node-bindings runs the WHOLE jest suite, and the
# leak budgets are AFFIRMED BY NAME from that run's own --json report. There is no
# lane-level dataset decision any more — #3522's component-level opt-out SKIP is the
# sole gate — so what needs covering here is the NOTE VOCABULARY (the SUMMARY's only
# statement about whether the budgets ran) and the gate's expected-budget-test list.
#
# Why the note TEXT and not an exit code: every state exits 0 or 1 for reasons that do
# not distinguish them, so a pass/fail assertion cannot tell them apart — the same "a
# bare red is not evidence" rule the rest of this suite is built on.
# ============================================================================

# 1465a. The affirmation helper must be WIRED to the component, and it must read the
#        report #3522 already writes rather than running jest a second time. Both are
#        source facts, asserted structurally: a second executor is exactly what the
#        recomposition removed, and a future edit that reintroduces one should red here.
nll_fn=$(sed -n '/^_node_leak_lane_affirm() {/,/^}$/p' "$GATE")
nll_component=$(sed -n '/^run_node_bindings() {/,/^}$/p' "$GATE")
if [ -n "$nll_fn" ] \
   && ! out_has "$nll_fn" -E 'npm (run )?test' \
   && out_has "$nll_component" '_node_leak_lane_affirm "$(_node_leak_lane_note_file)" "$suite_json"' \
   && [ "$(printf '%s' "$nll_component" | grep -cE '^[[:space:]]*npm (run )?test( |$)')" -eq 1 ]; then
  ok "1465-one-executor: the affirmation is wired to the component, runs no jest itself, and node-bindings invokes npm test exactly once"
else
  bad "1465-one-executor: affirmation missing/unwired, or it runs its own jest, or npm test is invoked more than once"
  printf '%s' "$nll_component" | grep -n 'npm test\|_node_leak_lane_affirm' | sed 's/^/    /'
fi

# 1465b. Every early return between the pessimistic pre-write and the affirmation must
#        leave a note that does NOT claim the budgets ran. Asserted on the component's
#        source: the pre-write precedes STEP 1, and the only state that says RAN is
#        written by the affirmation.
nll_prewrite=$(printf '%s\n' "$nll_component" | grep -n '_node_leak_lane_note NOT-REACHED' | cut -d: -f1 | head -1)
nll_step1=$(printf '%s\n' "$nll_component" | grep -n 'STEP 1 — install' | cut -d: -f1 | head -1)
nll_ran_writers=$(printf '%s\n' "$nll_component" | grep -c '_node_leak_lane_note RUN' || true)
if [ -n "$nll_prewrite" ] && [ -n "$nll_step1" ] && [ "$nll_prewrite" -lt "$nll_step1" ] \
   && [ "$nll_ran_writers" -eq 0 ]; then
  ok "1465-pessimistic-first: NOT-REACHED is written before STEP 1 and nothing in the component writes RAN except the affirmation"
else
  bad "1465-pessimistic-first: prewrite=$nll_prewrite step1=$nll_step1 in-component RAN writers=$nll_ran_writers"
fi

# 1465c. The component-level dataset SKIP must declare the leak-lane state, or a skipped
#        component leaves NO `node-bindings-leak-lane:` line and "no line" becomes
#        ambiguous between "it ran" and "this gate predates the line".
# SIGPIPE-FREE MATCH, DELIBERATELY NOT `printf | grep -q` (#3685). Measured at this site:
#        `out_has "$nll_component" PATTERN` under this suite's `set -uo pipefail`
#        returned **rc=141** in 30 of 80 runs (37.5%) — `grep -q` exits at the first match, closing
#        the read end, and whichever of `printf`'s write(2) calls lands after that gets EPIPE. Over
#        those 80 runs `rc=1` occurred ZERO times: the note was present EVERY time, so the pipeline
#        was inverting a TRUE assertion into a FAIL. It red two consecutive gates of record on an
#        identical tree digest (#3414) before the mechanism was found.
#
#        DANGER HEURISTIC for the other ~200 sites in #3685 — large variable x EARLY match. Here the
#        data is 34,397 bytes (it FITS a 65,536-byte pipe, so this is syscall interleaving, NOT
#        buffer exhaustion) and the first match is at byte 3,372, so grep discards 31,025 bytes it
#        never reads: near-maximal exposure. A pattern near the END of its data is nearly safe.
#        `[[ ]]` glob measured 0/80 on the identical variable and load. Only THIS site is changed.
if [[ $nll_component == *'_node_leak_lane_note SKIP-OPTOUT'* ]]; then
  ok "1465-skip-declares: the #3522 opt-out SKIP branch writes the SKIP-OPTOUT note"
else
  bad "1465-skip-declares: the opt-out SKIP branch does not declare the leak-lane state"
fi

# 1465g. THE AFFIRMATION ITSELF, driven with SYNTHETIC jest reports (round 10, roborev
#        R2). The recomposition pointed the affirmation at the WHOLE-SUITE report, which
#        widened the title namespace from one file to 28 — so a same-titled `passed` test
#        in another suite could satisfy it for a leak test that was skipped, and a
#        duplicate title inside the leak suite collapsed to one Map key and could not be
#        seen as an extra either. Both were verified to PASS against the pre-fix code.
#
#        Synthetic reports are the right oracle: the adversarial shapes can be written
#        directly instead of hoping a real run produces them. The SHIPPED function is
#        extracted and driven — no re-implementation.
if ! command -v node >/dev/null 2>&1; then
  skipped "1465-affirm-synthetic: needs node — NOT verified here"
else
  nll_syn_dir="$tmp/1465-affirm"
  mkdir -p "$nll_syn_dir"
  nll_b1="repeated query rejections stay under the leak budget"
  nll_b2="abandoned streaming iterators stay under the leak budget"
  # Repo-relative paths, because the affirmation anchors on
  # `/bindings/node/__test__/leak-paths.test.js` (round 11, T1).
  nll_leak='/x/repo/bindings/node/__test__/leak-paths.test.js'
  nll_other='/x/repo/bindings/node/__test__/impostor.test.js'
  nll_otherpkg='/x/other/pkg/bindings/node/__test__/leak-paths.test.js'
  nll_unanchored='/x/repo/zzbindings/node/__test__/leak-paths.test.js'
  # <case> <json>
  nll_mk() { printf '%s' "$2" > "$nll_syn_dir/$1.json"; }
  nll_mk happy "{\"testResults\":[{\"name\":\"$nll_leak\",\"assertionResults\":[{\"title\":\"$nll_b1\",\"status\":\"passed\"},{\"title\":\"$nll_b2\",\"status\":\"passed\"}]}]}"
  nll_mk other_suite "{\"testResults\":[{\"name\":\"$nll_leak\",\"assertionResults\":[{\"title\":\"$nll_b1\",\"status\":\"pending\"},{\"title\":\"$nll_b2\",\"status\":\"passed\"}]},{\"name\":\"$nll_other\",\"assertionResults\":[{\"title\":\"$nll_b1\",\"status\":\"passed\"}]}]}"
  nll_mk duplicate "{\"testResults\":[{\"name\":\"$nll_leak\",\"assertionResults\":[{\"title\":\"$nll_b1\",\"status\":\"pending\"},{\"title\":\"$nll_b1\",\"status\":\"passed\"},{\"title\":\"$nll_b2\",\"status\":\"passed\"}]}]}"
  nll_mk suite_absent "{\"testResults\":[{\"name\":\"$nll_other\",\"assertionResults\":[{\"title\":\"$nll_b1\",\"status\":\"passed\"},{\"title\":\"$nll_b2\",\"status\":\"passed\"}]}]}"
  nll_mk extra "{\"testResults\":[{\"name\":\"$nll_leak\",\"assertionResults\":[{\"title\":\"$nll_b1\",\"status\":\"passed\"},{\"title\":\"$nll_b2\",\"status\":\"passed\"},{\"title\":\"a third thing stay under the leak budget\",\"status\":\"skipped\"}]}]}"
  # T1 shapes: a SECOND suite at the anchored path supplying passes for a real leak
  # suite that ran nothing (J_split); a SOLE copy in another package; and an
  # unanchored tail. All three were AFFIRMED before the fix.
  nll_mk split "{\"testResults\":[{\"name\":\"$nll_leak\",\"assertionResults\":[]},{\"name\":\"$nll_otherpkg\",\"assertionResults\":[{\"title\":\"$nll_b1\",\"status\":\"passed\"},{\"title\":\"$nll_b2\",\"status\":\"passed\"}]}]}"
  nll_mk impostor_pkg "{\"testResults\":[{\"name\":\"/x/other/pkg/__test__/leak-paths.test.js\",\"assertionResults\":[{\"title\":\"$nll_b1\",\"status\":\"passed\"},{\"title\":\"$nll_b2\",\"status\":\"passed\"}]}]}"
  nll_mk unanchored "{\"testResults\":[{\"name\":\"$nll_unanchored\",\"assertionResults\":[{\"title\":\"$nll_b1\",\"status\":\"passed\"},{\"title\":\"$nll_b2\",\"status\":\"passed\"}]}]}"
  nll_mk dup_suite "{\"testResults\":[{\"name\":\"$nll_leak\",\"assertionResults\":[{\"title\":\"$nll_b1\",\"status\":\"pending\"},{\"title\":\"$nll_b2\",\"status\":\"passed\"}]},{\"name\":\"$nll_leak\",\"assertionResults\":[{\"title\":\"$nll_b1\",\"status\":\"passed\"}]}]}"
  : > "$nll_syn_dir/broken.json"
  printf '%s' 'not json {{{' > "$nll_syn_dir/broken.json"

  # <case> -> rc, via the SHIPPED function with only its declarations sourced.
  nll_affirm_rc() {
    local jsonf="$1" note rc
    note=$(mktemp "$nll_syn_dir/note.XXXXXX")
    (
      eval "$(sed -n '/^_NODE_LEAK_BUDGET_TESTS="/,/"$/p' "$GATE")"
      eval "$(sed -n '/^_NODE_LEAK_BUDGET_TITLE_SUFFIX=/p' "$GATE")"
      eval "$(sed -n '/^_NODE_LEAK_SUITE_FILE=/p' "$GATE")"
      eval "$(sed -n '/^_node_leak_lane_note() {/,/^}/p' "$GATE")"
      eval "$(sed -n '/^_node_leak_lane_affirm() {/,/^}$/p' "$GATE")"
      _node_leak_lane_affirm "$note" "$jsonf" >/dev/null 2>&1
    )
    rc=$?
    printf '%s' "$rc"
    rm -f "$note"
  }
  nll_syn_ok=1
  # The happy path must be the ONLY one that affirms.
  [ "$(nll_affirm_rc "$nll_syn_dir/happy.json")" = 0 ] || { nll_syn_ok=0; echo "  happy report did NOT affirm"; }
  for _c in other_suite duplicate suite_absent extra broken missing split impostor_pkg unanchored dup_suite; do
    _f="$nll_syn_dir/$_c.json"
    [ "$_c" = missing ] && _f="$nll_syn_dir/does-not-exist.json"
    if [ "$(nll_affirm_rc "$_f")" = 0 ]; then
      nll_syn_ok=0; echo "  adversarial report '$_c' was AFFIRMED (must fail closed)"
    fi
  done
  if [ "$nll_syn_ok" -eq 1 ]; then
    ok "1465-affirm-synthetic: the affirmation accepts ONLY the happy report — a same-titled test in another suite, a duplicate title, an absent leak suite, an unexpected extra, malformed JSON, a missing file, a SPLIT report (real suite empty + another package supplying passes), a sole same-named suite in another package, an unanchored path tail and a duplicated leak suite all FAIL closed"
  else
    bad "1465-affirm-synthetic: the affirmation is not fail-closed on every adversarial synthetic report (see above)"
  fi
fi

# 1465h. THE GATE OF RECORD CANNOT BE RELAXED (round 12, roborev V1). The leak budgets
#        double when CQLITE_LEAK_BUDGET_RELAX holds its opt-in token; the predecessor
#        keyed on `CI`, which GitHub Actions sets unconditionally, so gate.yml's nightly
#        FULL-gate backstop ran every ceiling at 2x while presenting itself as strict.
#        Two source properties are pinned:
#          * node-bindings UNSETS the variable for its node invocations, so an inherited
#            export cannot weaken it (a convention would not survive a re-used runner);
#          * the lane's multiplier is not keyed on any ambient marker.
#        WHAT THIS CANNOT COVER, named rather than implied: it is a source-shape check.
#        It cannot prove the RUNTIME environment of some future invocation, and it does
#        not evaluate the JS — the multiplier's value mapping is pinned by the lane's own
#        pure tests ("budget relaxation is OPT-IN ...", 16 falsy spellings + the token).
nll_comp_v1=$(sed -n '/^run_node_bindings() {/,/^}$/p' "$GATE")
nll_leakfile_v1="$SCRIPT_DIR/../../bindings/node/__test__/leak-paths.test.js"
nll_v1_ok=1
# SIGPIPE-FREE MATCH (#3685), second and LAST measured-dangerous site in this file. The
# `printf | grep -q` form here false-FAILed 12/40 (30%): the data is 34,397 bytes and this
# pattern sits at byte 6,331, so `grep -q` exits having DISCARDED 28,066 bytes and whichever
# of printf's write(2) calls lands after that gets EPIPE -> rc=141 -> a FAIL on a TRUE
# assertion. It is why `1465-gate-strict` red 1-in-3 suite runs even after L4618 was fixed.
#        SORT KEY for #3685's other sites is BYTES DISCARDED, not "early match" — the
#        discarded count IS the race window. Measured here: 305-byte data at 14% => 0/40
#        (too small to race); 34,397-byte data at 93%/98% => 0/40 (only ~2.5KB discarded);
#        34,397 at 18%/10% => 30%/37.5%. Small OR late is safe; only large AND early fires.
#        `grep -c` sites below are SAFE by construction — a count reads all input, so there
#        is no early exit to race. Only the two `grep -q` sites needed changing.
[[ $nll_comp_v1 == *'leak_strict_env=(-u CQLITE_LEAK_BUDGET_RELAX)'* ]] \
  || { nll_v1_ok=0; echo "  node-bindings does not declare the strict leak-budget env"; }
# every `env` that launches node in this component must carry the unset array
nll_env_launches=$(printf '%s\n' "$nll_comp_v1" | grep -cE '^[[:space:]]*if (! )?env ')
nll_env_stripped=$(printf '%s\n' "$nll_comp_v1" | grep -c 'leak_strict_env\[@\]')
[ "$nll_env_launches" -ge 2 ] && [ "$nll_env_stripped" -eq "$nll_env_launches" ] \
  || { nll_v1_ok=0; echo "  $nll_env_stripped of $nll_env_launches env launches strip CQLITE_LEAK_BUDGET_RELAX"; }
# ...AND IN THE RIGHT ORDER, which is the property presence cannot see (round 13, X1,
# roborev High). `env` stops parsing options at the first operand, so a `-u` placed after a
# NAME=VALUE is treated as the COMMAND: in full-gate mode `fixture_env` is
# `CQLITE_REQUIRE_FIXTURES=1`, and `env "${fixture_env[@]}" "${leak_strict_env[@]}" …`
# died with `env: '-u': No such file or directory` (127) on every host with node+npm —
# while `--only`/`--lite`, where `fixture_env` is itself the `-u` pair, passed. Only an
# order assertion distinguishes those two compositions, so this counts POSITIONS.
nll_order_ok=1
nll_order_seen=0
while IFS= read -r _envline; do
  nll_order_seen=$((nll_order_seen + 1))
  # position of the first NAME=VALUE (or the fixture_env array, which IS an assignment in
  # full-gate mode) vs the position of the option-bearing unset array
  _first_assign=$(printf '%s\n' "$_envline" | tr ' ' '\n' | grep -nE '^[A-Za-z_][A-Za-z0-9_]*=|fixture_env\[@\]' | head -1 | cut -d: -f1)
  _opt_pos=$(printf '%s\n' "$_envline" | tr ' ' '\n' | grep -nE '^-u$|^-i$|leak_strict_env\[@\]' | head -1 | cut -d: -f1)
  if [ -n "$_first_assign" ] && [ -n "$_opt_pos" ] && [ "$_opt_pos" -gt "$_first_assign" ]; then
    nll_order_ok=0
    echo "  operand-order hazard: an option/unset-array at position $_opt_pos follows an assignment at $_first_assign in: $_envline"
  fi
done <<EOF_ORDER
$(printf '%s\n' "$nll_comp_v1" | tr '\n' '\001' | sed 's/\\\001[[:space:]]*/ /g' | tr '\001' '\n' | grep -E '^[[:space:]]*if (! )?env ')
EOF_ORDER
[ "$nll_order_seen" -ge 2 ] || { nll_order_ok=0; echo "  only $nll_order_seen env launch(es) found to order-check (expected >= 2)"; }
[ "$nll_order_ok" -eq 1 ] || nll_v1_ok=0
if [ ! -r "$nll_leakfile_v1" ]; then
  nll_v1_ok=0; echo "  leak lane not readable at $nll_leakfile_v1"
else
  # No ambient marker may DECIDE the multiplier. Mentions inside comments are fine (the
  # file documents the defect), so only non-comment lines are inspected.
  nll_ambient=$(grep -nE '^[[:space:]]*[^/*[:space:]].*process\.env\.(CI|GITHUB_ACTIONS|BUILDKITE|JENKINS[A-Z_]*)\b' "$nll_leakfile_v1" || true)
  [ -z "$nll_ambient" ] || { nll_v1_ok=0; echo "  the lane keys on an ambient CI marker: $nll_ambient"; }
  grep -q "resolveBudgetRelaxation" "$nll_leakfile_v1" \
    || { nll_v1_ok=0; echo "  the lane has no named opt-in resolver"; }
  # Y4: the in-lane strictness control keys on CQLITE_JEST_JSON as its "this is the gate"
  # marker. NOTHING otherwise pins the pairing, so a rename on either side would silently
  # turn the control into a no-op instead of reddening. Both halves asserted here.
  out_has "$nll_comp_v1" 'CQLITE_JEST_JSON=' \
    || { nll_v1_ok=0; echo "  node-bindings no longer EXPORTS CQLITE_JEST_JSON — the lane-side strictness control has no marker to read"; }
  grep -q 'process\.env\.CQLITE_JEST_JSON' "$nll_leakfile_v1" \
    || { nll_v1_ok=0; echo "  the lane no longer READS CQLITE_JEST_JSON — its gate-of-record strictness assertion cannot fire"; }
fi
if [ "$nll_v1_ok" -eq 1 ]; then
  ok "1465-gate-strict: node-bindings unsets CQLITE_LEAK_BUDGET_RELAX for all $nll_env_launches node launches, every -u PRECEDES every assignment in all $nll_order_seen of them (the X1 full-gate hazard), the CQLITE_JEST_JSON marker is exported by the component AND read by the lane (so the in-lane strictness control cannot be renamed into a no-op), and the lane decides relaxation by a named opt-in rather than an ambient CI marker"
else
  bad "1465-gate-strict: the gate path could be relaxed by an inherited environment (see above)"
fi

# 1465d. The note vocabulary is CLOSED, single-sourced, and DISTINCT. Every state the
#        component or the hook can write must render its own line: prefix-and-no-UNKNOWN
#        was not enough (roborev J4) — two states rendering the SAME text would have
#        passed, which is the uniform-output blind spot this diff has hit before. So the
#        rendered lines are collected and counted for UNIQUENESS.
#        All six are execution outcomes of the component, so none is reachable from a
#        pure decision (the hook that used to offer one was deleted with the lane-level
#        dataset gate); they are asserted here at the TEXT level, which is what stops a
#        future state from silently falling through the UNKNOWN arm (one already did).
nll_states="RUN SKIP-OPTOUT NO-NODE NOT-REACHED ENTERED-FAILED NO-BUDGET-AFFIRMATION"
nll_vocab_ok=1
nll_rendered="$tmp/1465-notes.txt"
: >"$nll_rendered"
nll_count=0
for _st in $nll_states; do
  _line=$(bash -c '. /dev/stdin <<<"$(sed -n "/^_node_leak_lane_note() {/,/^}/p" "$1")"; _node_leak_lane_note "$2"' _ "$GATE" "$_st" 2>/dev/null)
  nll_count=$((nll_count + 1))
  printf '%s\n' "$_line" >>"$nll_rendered"
  case "$_line" in
    "node-bindings-leak-lane: "*) : ;;
    *) nll_vocab_ok=0; echo "  state '$_st' rendered: '$_line'" ;;
  esac
  # The FALLBACK's own signature, not the bare word: a legitimate note may say
  # "UNKNOWN" about something it does not know (ENTERED-FAILED says execution is
  # UNKNOWN, round 10 S3), and matching the word alone turned that into a false
  # failure of this case.
  case "$_line" in
    *"UNKNOWN state '"*) nll_vocab_ok=0; echo "  state '$_st' fell through to the UNKNOWN arm" ;;
  esac
done
# DISTINCTNESS (roborev J4): as many unique lines as states.
nll_unique=$(sort -u "$nll_rendered" | wc -l | tr -d ' ')
if [ "$nll_unique" -ne "$nll_count" ]; then
  nll_vocab_ok=0
  echo "  only $nll_unique unique note line(s) for $nll_count states — two states render the SAME text:"
  sort "$nll_rendered" | uniq -d | sed 's/^/    /'
fi
_unknown_line=$(bash -c '. /dev/stdin <<<"$(sed -n "/^_node_leak_lane_note() {/,/^}/p" "$1")"; _node_leak_lane_note "$2"' _ "$GATE" "SOMETHING-NEW" 2>/dev/null)
case "$_unknown_line" in
  *"UNKNOWN state 'SOMETHING-NEW'"*) : ;;
  *) nll_vocab_ok=0; echo "  an unrecognised state did NOT report itself: '$_unknown_line'" ;;
esac
if [ "$nll_vocab_ok" -eq 1 ]; then
  ok "1465-note-vocab: all $nll_count states render a DISTINCT note ($nll_unique unique), and an unrecognised state reports itself"
else
  bad "1465-note-vocab: the note vocabulary is not closed/distinct (see above)"
fi

# 1465e. The two failure states must SAY what they mean and CLAIM NO MORE than they
#        know. ENTERED-FAILED replaced a NOT-REACHED that blamed npm ci for a real
#        budget failure, so it must distinguish itself from an earlier step — but it is
#        inferred from an EXIT CODE, so it must NOT claim the budget tests executed:
#        a jest harness/config/setup/loader error that ran nothing lands in the same arm
#        (round 10, S3). NO-BUDGET-AFFIRMATION must name the affirmation rather than a
#        test count.
#
#        Asserted POSITIVELY (it must state the uncertainty) and NEGATIVELY (no phrase
#        that asserts execution), because grepping for the absence of one exact phrase
#        would pass for any rewording that overclaims differently.
nll_entered=$(bash -c '. /dev/stdin <<<"$(sed -n "/^_node_leak_lane_note() {/,/^}/p" "$1")"; _node_leak_lane_note ENTERED-FAILED' _ "$GATE" 2>/dev/null)
nll_noaffirm=$(bash -c '. /dev/stdin <<<"$(sed -n "/^_node_leak_lane_note() {/,/^}/p" "$1")"; _node_leak_lane_note NO-BUDGET-AFFIRMATION' _ "$GATE" 2>/dev/null)
nll_e_ok=1
out_has "$nll_entered" "REACHED" || { nll_e_ok=0; echo "  ENTERED-FAILED does not say the invocation was reached"; }
out_has "$nll_entered" "NOT an earlier" || { nll_e_ok=0; echo "  ENTERED-FAILED does not distinguish itself from an earlier step"; }
out_has "$nll_entered" -E "UNKNOWN|unknown" || { nll_e_ok=0; echo "  ENTERED-FAILED does not state that execution is UNKNOWN"; }
# No phrase may assert that the budgets ran. Each of these would be an overclaim.
for _bad in "DID execute" "so the leak budgets ran" "the budgets ran" "budgets executed"; do
  if out_has "$nll_entered" -F "$_bad"; then
    nll_e_ok=0; echo "  ENTERED-FAILED overclaims execution via: '$_bad'"
  fi
done
out_has "$nll_noaffirm" -E "named (#1465 )?budget test" || { nll_e_ok=0; echo "  NO-BUDGET-AFFIRMATION does not name the budget tests"; }
if [ "$nll_e_ok" -eq 1 ]; then
  ok "1465-failure-states: ENTERED-FAILED says REACHED + execution UNKNOWN + not-an-earlier-step and asserts no execution; NO-BUDGET-AFFIRMATION names the budget tests"
else
  bad "1465-failure-states: the failure notes do not state their own meaning (or overclaim)"
  echo "  ENTERED-FAILED: $nll_entered"
  echo "  NO-BUDGET-AFFIRMATION: $nll_noaffirm"
fi

# 1465f. The expected BUDGET-TEST list the gate affirms against must be non-empty, and
#        every name in it must appear in the lane — an expectation that drifts from the
#        test file is how a budget test goes silently uncovered (roborev J1).
#
#        SCOPE, stated because the name used to promise more (round 7, N-c): the COUNT
#        comparison enumerates the lane by the jest-title SUFFIX, so it sees a declared
#        budget test only if it carries that suffix. A new budget test titled WITHOUT the
#        suffix escapes this case — the runtime affirmation arm is what catches that,
#        because a title it cannot see is a name it cannot report as passed. The
#        enumeration is deliberately suffix-based rather than syntax-based: matching
#        `test('`/`it('`/backticks/indentation is spelling-sensitive, and a grep that
#        misses a legal spelling would FALSE-PASS this case.
nll_expected=$(sed -n '/^_NODE_LEAK_BUDGET_TESTS="/,/"$/p' "$GATE" | sed '1s/^_NODE_LEAK_BUDGET_TESTS="//; $s/"$//')
nll_expected_n=$(printf '%s\n' "$nll_expected" | grep -c . || true)
nll_suffix=$(sed -n 's/^_NODE_LEAK_BUDGET_TITLE_SUFFIX="\(.*\)"$/\1/p' "$GATE")
nll_leakfile="$SCRIPT_DIR/../../bindings/node/__test__/leak-paths.test.js"
if [ ! -r "$nll_leakfile" ] || [ -z "$nll_suffix" ]; then
  # N-d: a missing file or an unreadable suffix is a NAMED failure, not an arithmetic
  # error from a `grep -c || echo 0` that emitted two lines.
  bad "1465-budget-list: cannot measure — leak file readable=$([ -r "$nll_leakfile" ] && echo yes || echo no), suffix='${nll_suffix:-<empty>}'"
else
  # Count TITLE LINES declaring a budget test: any line that both looks like a jest
  # test declaration (a quoted title followed by a comma) and ends its title with the
  # suffix. Quote-agnostic ('", `) and indentation-agnostic.
  nll_actual_n=$(grep -cE "^[[:space:]]*(test|it)\(([\"'\`])[^\"'\`]*${nll_suffix}\2" "$nll_leakfile" || true)
  nll_missing=0
  while IFS= read -r _name; do
    [ -n "$_name" ] || continue
    grep -qF "$_name" "$nll_leakfile" || { nll_missing=1; echo "  expected budget test not found in the lane: '$_name'"; }
  done <<<"$nll_expected"
  if [ "$nll_expected_n" -ge 2 ] && [ "$nll_missing" -eq 0 ] && [ "$nll_expected_n" -eq "$nll_actual_n" ]; then
    ok "1465-budget-list: the gate expects $nll_expected_n named budget test(s), all present in the lane, and the lane declares exactly that many SUFFIX-BEARING budget tests"
  else
    bad "1465-budget-list: expected=$nll_expected_n suffix-bearing-in-lane=$nll_actual_n missing=$nll_missing"
  fi
fi

# Y3 (round 14): a skip announced with a bare `echo` is INVISIBLE to SKIPPED_TOOLING, and
# that is how the featoracle sites (Y2) went unaccounted — two displaced verdicts that
# incremented nothing. This guard scans THIS FILE for any SKIP announcement that does not
# go through `skipped()`.
#
# WHAT IT CANNOT COVER, stated so nobody reads it as covering both halves: it checks the
# ROUTE, never the COUNT. A site that calls `skipped()` ONCE while its run branch emits
# nine verdicts (the r32 shape) is invisible here — that is the Y1/Y2 class, and no static
# check can see it, because the displaced count is a property of the branch not taken.
# Comment lines are excluded (this paragraph names the pattern it forbids).
self_src_="$SCRIPT_DIR/$(basename "$0")"
if [ ! -r "$self_src_" ]; then
  bad "skip-routing: cannot read $self_src_ to verify that no SKIP announcement bypasses skipped()"
else
  bypass_=$(grep -nE '^[^#]*(echo|printf)[^|]*"[[:space:]]*(SKIP|skip)[[:space:]-]' "$self_src_" \
            | grep -v 'skipped()' || true)
  if [ -z "$bypass_" ]; then
    ok "skip-routing: every SKIP announcement in this suite goes through skipped() (route only — it cannot check that a site's skip COUNT matches its run-branch verdict count)"
  else
    bad "skip-routing: SKIP announced without skipped(), so SKIPPED_TOOLING misses it and ASSERT_FLOOR reds a legitimate host: $bypass_"
  fi
fi

# --- 53. #3453: the all-features-check lane is REGISTERED where it must be, ABSENT ---
#         where it must be, and its invocation cannot silently stop being all-features.
#
# WHY THIS EXISTS. Before #3453 no cargo invocation in the gate ever passed
# `observability` (run_clippy EXCLUDES the OTel stack by #1844 design, core-tests runs
# `--features cli-helpers`, minimal-build runs `--no-default-features`), so a defect
# reachable only with that feature on could not fail the gate of record while failing
# pr-gate.yml's `cargo test -p cqlite-core --lib --all-features`. Measured on PR #3382: a
# 31/31 gate PASS that never executed the test pinning that PR's own fix.
#
# FOUR REGISTRIES MUST AGREE, and the failure of any one is SILENT: a name in COMPONENTS
# with no dispatch arm hits `unknown component` (return 2); a name absent from COMPONENTS
# is simply never run while every SUMMARY stays green. Both directions are asserted here,
# for the same reason section 16 does it for the #1699 lanes.
#
# THE ABSENCES ARE AS LOAD-BEARING AS THE PRESENCES. This lane must NOT be in
# DATASET_COMPONENTS (it opens no fixture; enrolling it would make a fixture-less
# checkout fail closed for nothing — and #3522's rule is that a never-SKIPping lane
# folded into a SKIP-aware one is a coverage hole wearing a SKIP's clothes), and it must
# NOT leak into LITE_COMPONENTS / DELTA_COMPONENTS, whose whole value is staying fast.
#
# The STRUCTURAL asserts on the function body are here rather than in the opt-in
# planted-break harness because the regression they guard is TEXTUAL: someone narrowing
# `--all-features` to a curated list (the thing run_clippy already has to maintain by
# hand), dropping `-p` for `--workspace` (which would build the cqlite-cli-owned duckdb
# amalgamation from source — the #916 cost this lane was scoped to avoid), or removing
# `_deny_warnings` because `-- -D warnings` reads as equivalent (it is not: an inherited
# CARGO_ENCODED_RUSTFLAGS silently defeats a bare RUSTFLAGS — #1699 round 5). Sub-second,
# no cargo, no network. The EXPENSIVE half — does the lane actually red on a planted
# observability-only defect, and do the existing components stay green on the same plant?
# — is scripts/tests/test_agent_gate_all_features_lane.sh, deliberately opt-in.
AFC_LANE=all-features-check

afc_list="$tmp/3453-list.txt"
if bash "$GATE" --list >"$afc_list" 2>/dev/null && [ -s "$afc_list" ]; then
  ok "3453-list-extract: \`--list\` produced a readable COMPONENTS listing ($(grep -c . "$afc_list" | tr -d ' ') components)"
else
  bad "3453-list-extract: \`--list\` failed or produced nothing — every membership assert below would be vacuous"
fi
if grep -qxF "$AFC_LANE" "$afc_list" 2>/dev/null; then
  ok "3453-registered: $AFC_LANE is in COMPONENTS (printed by --list)"
else
  bad "3453-registered: $AFC_LANE is NOT printed by --list — dropped from COMPONENTS, so the OTel stack is once again compiled by no gate component"
fi

# Reuses section 16's extraction of the REAL dispatch arms (4-space-indented `<name>)`).
if grep -qxF "$AFC_LANE" "$dispatch_arms" 2>/dev/null; then
  ok "3453-dispatch: $AFC_LANE is reachable in dispatch_component"
else
  bad "3453-dispatch: $AFC_LANE has NO dispatch_component arm — it would hit 'unknown component' and return 2"
fi

# DATASET_COMPONENTS: must be ABSENT. `$dataset_components` is extracted (with its own
# fail-closed guard) in section 16.
if [ -z "$dataset_components" ]; then
  bad "3453-dataset-absent: DATASET_COMPONENTS could not be extracted, so this absence assert has no subject"
else
  case " $dataset_components " in
    *" $AFC_LANE "*)
      bad "3453-dataset-absent: $AFC_LANE is in DATASET_COMPONENTS — it needs nothing beyond cargo, so enrolling it makes a fixture-less checkout fail closed for a lane that opens no Data.db" ;;
    *)
      ok "3453-dataset-absent: $AFC_LANE is correctly NOT in DATASET_COMPONENTS (never SKIPs, needs no corpus)" ;;
  esac
fi

# The two fast-loop sets, read from the script's OWN listing hooks rather than the source.
for _afc_mode in lite delta; do
  # Uppercase via `tr`, NOT bash-4 case-conversion expansion (the `${v` + `^^}` form):
  # that throws `bad substitution` on macOS's stock /bin/bash 3.2, a first-class gate
  # host, and this file is ALWAYS invoked by tooling-tests (roborev job 277, #3453).
  _afc_MODE=$(printf '%s' "$_afc_mode" | tr '[:lower:]' '[:upper:]')
  _afc_f="$tmp/3453-$_afc_mode-list.txt"
  _afc_rc=0
  bash "$GATE" "--$_afc_mode-list" >"$_afc_f" 2>/dev/null || _afc_rc=$?
  _afc_n=$(grep -c . "$_afc_f" 2>/dev/null || true)
  if [ "$_afc_rc" -ne 0 ] || [ "${_afc_n:-0}" -eq 0 ]; then
    bad "3453-$_afc_mode-absent: \`--$_afc_mode-list\` did not produce a readable list (rc=$_afc_rc, lines=${_afc_n:-0}) — the absence check has no subject"
  elif grep -qxF "$AFC_LANE" "$_afc_f" 2>/dev/null; then
    bad "3453-$_afc_mode-absent: $AFC_LANE leaked into ${_afc_MODE}_COMPONENTS — it is a full-gate component (a cold --all-features build), and --$_afc_mode is the fast loop"
  else
    ok "3453-$_afc_mode-absent: $AFC_LANE is correctly absent from ${_afc_MODE}_COMPONENTS (${_afc_n} entries read)"
  fi
done

# --- the invocation itself, structurally ------------------------------------------
afc_fn=$(awk '/^run_all_features_check\(\) \{/,/^\}/' "$GATE")
if [ -n "$afc_fn" ]; then
  ok "3453-fn-extract: extracted run_all_features_check's body ($(printf '%s\n' "$afc_fn" | grep -c . | tr -d ' ') lines)"
else
  bad "3453-fn-extract: could NOT extract run_all_features_check — it is missing, or renamed, and every structural assert below would pass vacuously"
fi
# The INVOCATION lines only. `^[^#]*cargo (check|clippy)` matched 8 lines, because this
# function also NAMES its passes in its own log output (`echo "[$name] pass 1/2 cargo
# check …"`) — a reporting line is not an invocation, and counting it made the
# two-passes assert red on correct code. Comments and any line that merely PRINTS the
# words are excluded; what remains is what cargo actually runs.
afc_cargo=$(printf '%s\n' "$afc_fn" \
  | grep -E 'cargo (check|clippy) ' \
  | grep -vE '^[[:space:]]*#' \
  | grep -vE '(echo|printf|declaration=)' || true)
afc_n_cargo=$(printf '%s' "$afc_cargo" | grep -c . || true)
if [ "${afc_n_cargo:-0}" -eq 2 ]; then
  ok "3453-two-passes: exactly two cargo invocations (the ruled check + clippy pair)"
else
  bad "3453-two-passes: found ${afc_n_cargo:-0} cargo check/clippy invocations, expected 2 — the owner ruling for #3453 is a check AND a clippy pass"
fi
afc_bad=""
out_has "$afc_cargo" -- '--all-features' || afc_bad="$afc_bad no---all-features"
[ "$(printf '%s\n' "$afc_cargo" | grep -c -- '--all-features' || true)" = 2 ] || afc_bad="$afc_bad not-both---all-features"
[ "$(printf '%s\n' "$afc_cargo" | grep -c -- '--all-targets' || true)" = 2 ] || afc_bad="$afc_bad not-both---all-targets"
out_has "$afc_cargo" -- '--package cqlite-core' || afc_bad="$afc_bad not-package-scoped"
out_has "$afc_cargo" -- '--workspace' && afc_bad="$afc_bad uses---workspace"
if [ -z "$afc_bad" ]; then
  ok "3453-invocation: both passes are \`--package cqlite-core --all-features --all-targets\` and neither is --workspace (which would build cqlite-cli's bundled duckdb from source — the #916 cost)"
else
  bad "3453-invocation:$afc_bad — this lane's entire subject is the feature set nothing else enables, so a narrowed feature list or a widened package scope silently retires it (or blows its minutes budget)"
fi
if [ "$(printf '%s\n' "$afc_cargo" | grep -c '_deny_warnings' || true)" = 2 ]; then
  ok "3453-denywarn: both passes go through _deny_warnings, so -D warnings cannot be made inert by an inherited CARGO_ENCODED_RUSTFLAGS (#1699 round 5)"
else
  bad "3453-denywarn: a pass does not go through _deny_warnings — a bare \`env RUSTFLAGS=-D warnings\` is SILENTLY IGNORED when CARGO_ENCODED_RUSTFLAGS is set, even when empty"
fi
if out_has "$afc_fn" '_resolved_package_features cqlite-core --all-features'; then
  ok "3453-subject-derived: the declared feature set is read back from CARGO, not echoed from the flag this function passes"
else
  bad "3453-subject-derived: run_all_features_check no longer derives its feature set via _resolved_package_features — a lane that prints its own arguments states nothing about the build that happened"
fi
if out_has "$afc_fn" -E '^[^#]*status=SKIP'; then
  bad "3453-never-skips: run_all_features_check gained a SKIP branch — it needs nothing beyond cargo, and a SKIP here is a coverage hole wearing a SKIP's clothes (#3522)"
else
  ok "3453-never-skips: run_all_features_check has no SKIP branch (it depends on nothing but cargo)"
fi
if out_has "$afc_fn" 'declaration="\[\$name\] subject:'; then
  ok "3453-declares: the lane emits a subject declaration (package + feature set + targets), not a bare status token"
else
  bad "3453-declares: the lane no longer declares what it measured — issue #3453's own remedy is 'report a measurement, not a decision'"
fi

# --- 54. #3453: EVERY component line in the emitted block NAMES its feature matrix -----
#
# WHY THIS IS HERE and not only in the dedicated guard: this suite owns the SUMMARY BLOCK
# CONTRACT, and "each component line states what it certified" is now part of that
# contract (owner ruling 2026-08-30). A bare `core-tests: PASS (412s)` cannot distinguish a
# run that certified the OTLP stack from one that never enabled it — the whole subject of
# #3453 — so a block whose lines lost their matrix is a REGRESSION OF THIS BLOCK, whatever
# the annotation machinery itself does.
#
# DIVISION OF LABOUR, stated so neither side is mistaken for the other: the COMPLETENESS
# census (every name in COMPONENTS resolves to a declared class, and the declared-vs-
# EXECUTED differential over the eight `bash -c` bodies) lives in
# scripts/tests/test_agent_gate_feature_matrix_annotation.sh, which runs in tooling-tests.
# Case 54e asserts that guard is still REGISTERED there, so the census cannot be silently
# dropped and leave only these block-shape checks behind.
#
# Host-INDEPENDENT: bash plus --emit-summary-selftest (no cargo, no python3, no jq, no
# network, no datasets), so none of these five can become a declared tooling skip.
fm_sum="$tmp/3453-annot-summary.txt"
if AGENT_GATE_SUMMARY_FILE="$fm_sum" bash "$GATE" --emit-summary-selftest >/dev/null 2>&1; then
  fm_lines=$(grep -cE '^[a-z][a-z-]*: +(PASS|FAIL|SKIP) \([0-9]+s\)' "$fm_sum")
  fm_annot=$(grep -cE '^[a-z][a-z-]*: +(PASS|FAIL|SKIP) \([0-9]+s\) +\[.+\]$' "$fm_sum")
  if [ "$fm_lines" -gt 0 ] && [ "$fm_annot" = "$fm_lines" ]; then
    ok "3453-annot-a: all $fm_lines component line(s) carry a bracketed feature matrix"
  else
    bad "3453-annot-a: only $fm_annot of $fm_lines component lines carry a feature matrix"
    grep -E '^[a-z][a-z-]*: +(PASS|FAIL|SKIP)' "$fm_sum" || true
  fi
  if grep -qE '^[a-z][a-z-]*: +(PASS|FAIL|SKIP).*\[(UNDECLARED|UNCLASSIFIED)' "$fm_sum"; then
    bad "3453-annot-b: a component line reads UNDECLARED/UNCLASSIFIED in the reference block"
  else
    ok "3453-annot-b: no component line reads UNDECLARED/UNCLASSIFIED in the reference block"
  fi
  if grep -E '^[a-z][a-z-]*: +(PASS|FAIL|SKIP)' "$fm_sum" | grep -q 'RESULT:'; then
    bad "3453-annot-c: an annotation embeds the RESULT: token — it would break the #2908 poll predicate"
  else
    ok "3453-annot-c: no annotation embeds the RESULT: token (the one-RESULT invariant is safe)"
  fi
else
  bad "3453-annot-a: --emit-summary-selftest exited non-zero"
  bad "3453-annot-b: not evaluated (selftest failed)"
  bad "3453-annot-c: not evaluated (selftest failed)"
fi
# The FOUR non-observed renderings are a CLOSED set, each explicit. This is the property
# that makes a blank annotation unrepresentable: every branch that cannot report an
# observed matrix still prints a NAMED state.
fm_tokens_missing=()
grep -q "printf '\[no-cargo\]'" "$GATE" || fm_tokens_missing+=(no-cargo)
grep -q 'feature set NOT observed' "$GATE" || fm_tokens_missing+=(via-driver-not-observed)
grep -q "printf '\[UNDECLARED\]'" "$GATE" || fm_tokens_missing+=(UNDECLARED)
grep -q 'UNCLASSIFIED' "$GATE" || fm_tokens_missing+=(UNCLASSIFIED)
grep -q 'component SKIPped' "$GATE" || fm_tokens_missing+=(skipped-before-cargo)
# …and, since the eight `bash -c` bodies record at EXECUTION time (#3453 roborev job 269
# blocker 2), a component that FAILs before its first cargo call legitimately leaves an
# EMPTY sidecar. That state is NAMED too — it is a fact we know exactly, and UNDECLARED
# would understate it.
# The FAIL text is composed from a `$what` phrase (which names the metadata-probe exclusion
# and, for an indirect component, the DRIVER), so both halves are asserted rather than one
# contiguous literal that the composition would hide.
grep -qF 'no cargo build/test invoked (component FAILed before ' "$GATE" || fm_tokens_missing+=(failed-before-cargo)
grep -qF 'its first cargo build/test invocation' "$GATE" || fm_tokens_missing+=(failed-before-cargo-what)
# …and the TWO states added by roborev job 273: `unobservable:<why>` (F2 — cargo may run in
# child processes and this shell can say neither what nor whether), and an indirect
# component whose DRIVER was never reached (F3 — the state the old code mis-reported as an
# unobserved cargo invocation).
grep -q 'cargo not observable' "$GATE" || fm_tokens_missing+=(cargo-not-observable)
grep -q 'never reached' "$GATE" || fm_tokens_missing+=(driver-never-reached)
grep -q 'before reaching its driver' "$GATE" || fm_tokens_missing+=(failed-before-driver)
if [ "${#fm_tokens_missing[@]}" -eq 0 ]; then
  ok "3453-annot-d: every non-observed state has an EXPLICIT rendering (no-cargo / via <driver> NOT observed / cargo-not-observable / driver-never-reached / UNDECLARED / UNCLASSIFIED / SKIPped / FAILed-before-cargo / FAILed-before-driver) — a blank annotation is unrepresentable"
else
  bad "3453-annot-d: missing explicit rendering(s): ${fm_tokens_missing[*]}"
fi
fm_guard=scripts/tests/test_agent_gate_feature_matrix_annotation.sh
if [ -r "$SCRIPT_DIR/../../$fm_guard" ] && grep -q "$fm_guard" "$GATE"; then
  ok "3453-annot-e: the completeness/no-drift guard exists AND is registered in the gate (tooling-tests)"
else
  bad "3453-annot-e: $fm_guard missing (looked under $SCRIPT_DIR/../..) or not registered in $GATE — the COMPONENTS completeness census would be silently gone"
fi

# TOLERANT BY DELIBERATE CHOICE, not by neglect (issue #1465 round 14 — the FALLBACK the
# coordination lead authorised, taken on the evidence below).
#
# The measured `accounted` on a fully-equipped host is 401, and the accounting is 1:1
# across EIGHT separately-forced host shapes (everything-present, jq-less, cargo-less,
# python3-less, node-less, Darwin, masked /proc, offline cargo registry — each measured
# individually at 401; the per-shape numbers are in the invariant header above). An EXACT
# floor of 401 would nonetheless be a hair-trigger: it reds on any skip site that displaces
# more than one verdict, and three rounds of enumeration found FOUR such sites
# (1699-r18-diff, 1699-r32-preflight-behaviour, perf-host,
# 1699-featoracle-{behaviour,complement}) — two of them
# only after two prior enumerations had declared the set complete. The scans that back a
# completeness claim are heuristics (bounded lookahead, regex shapes), the skip-routing
# guard above checks the ROUTE and explicitly not the COUNT, and no static check can see a
# count that lives in the branch not taken.
#
# So the floor is `measured - largest single displacement` = 401 - 9 (the r32 section's
# nine want_ cases).
#
# WHAT 392 ACTUALLY DETECTS — the BOUND, measured, not the aspiration (round 15, F2; an
# earlier version of this paragraph claimed it "still catches a whole section dying
# silently", and that was FALSE): a real run emits 401 verdicts across 269 distinct
# labelled sections, and the LARGEST section is exactly 9 (1699-r32-preflight-behaviour;
# next are py-route 8, then several at 6). The slack IS 9, so **no single section is
# covered — not even the largest**. 392 detects only a loss of >= 10 verdicts, i.e. a
# MULTI-SECTION disappearance. Single-section detection is deferred to issue #3611.
#
# WHY THE TRADE IS STILL RIGHT, given that bound:
#   * a mid-file abort (the `set -u`/extraction-failure shape this floor was written for)
#     exits NON-ZERO and reds through the exit status, before the floor block is reached —
#     the floor was never the only guard against it;
#   * nearly every section carries its OWN fail-closed guard for the same failure
#     (`bad "…-scope: could not extract …"`), which reds with a named cause rather than as
#     an arithmetic shortfall;
#   * the base rate of undiscovered non-1:1 sites is demonstrably non-zero (four found,
#     two of them post-"complete"), so an exact floor buys <=9-verdict detection at the
#     price of a FALSE RED on a legitimately-configured host — which this repo's doctrine
#     calls the worse failure ("a lane that reds on correct input is the lane people learn
#     to waive").
# The durable contribution here is the SKIPPED_TOOLING accounting and the four 1:1 fixes,
# not the number. #3611 carries the enumeration, the four defects, the eight host shapes,
# and a better derivation than an exact count (a floor on the number of distinct verdict
# LABELS observed, which is structurally immune to the displacement problem).
# 405 -> 410 on #3453 (Phase B): section 54 adds 5 asserts, host-INDEPENDENT for the same
# reason (bash plus --emit-summary-selftest; no cargo/python3/jq/network/datasets), so the
# same "raise by exactly the number added" rule applies and the ~9 margin is preserved.
# 392 -> 405 on #3453: section 53 adds 13 asserts, every one of them host-INDEPENDENT
# (bash plus the gate's own --list/--lite-list/--delta-list hooks; no cargo, no python3,
# no jq, no network), so none of them can turn into a declared skip on any of the eight
# host shapes enumerated above. Raising the floor by exactly the number added therefore
# preserves the deliberate ~9 margin rather than widening it — a floor that stays put
# while the suite grows is a floor that stops detecting a silently-dying section, which
# is the only thing it is for.
# 410 -> 452: the #3727 capacity-token cases (9c-v..9c-xi) add exactly 42 host-independent
# verdicts — 5 cap-source rows x (token + whole-line grammar) = 10, the unmeasurable state
# (token + its negative-match sweep + grammar) = 3, the na state, used=100%, its LOUD WARN,
# used cap-zero, the two health-is-not-capacity asserts, and 9c-x's unattributed pair (token +
# grammar) = 2, the invalid-stale row + its two axes-kept-apart asserts = 4, and the attribution
# pair (9c-x's two forced outcomes x2 + 9c-xi's null-size-is-not-attribution pair) = 4. COUNTED FROM
# A REAL RUN, not from
# arithmetic over the source (this file's own header records that its hand-kept accounting has
# been wrong twice): the run that added them reported `accounted: 439`, against 420 before, so
# the +42 above is a measured difference (the last three: 9c-v-f's unclassifiable-value token, its
# no-WARN assert and its grammar check) and the deliberate ~10 margin is preserved rather than
# widened. Setting the floor AT the accounted figure would remove that margin, which is what
# absorbs the host-conditional verdicts enumerated above.
ASSERT_FLOOR=452
# PASS + SKIPPED_TOOLING, not PASS alone: a DECLARED tooling skip is accounted for
# rather than counted against the floor (see SKIPPED_TOOLING). A section that dies
# silently still reds, because a dead section increments neither counter.
ASSERT_ACCOUNTED=$((PASS + SKIPPED_TOOLING))
if [ "$ASSERT_ACCOUNTED" -lt "$ASSERT_FLOOR" ]; then
  echo "FAIL - assert-floor: only $ASSERT_ACCOUNTED accounted assertions ($PASS passed + $SKIPPED_TOOLING declared tooling skips), floor is $ASSERT_FLOOR. Sections are being SKIPPED or dying silently (an extraction that broke, a subshell aborting under set -u), and 'failed: 0' over a shrunken subject set is exactly the vacuous pass this suite tests for."
  FAIL=$((FAIL + 1))
fi
echo "----"
echo "passed: $PASS  failed: $FAIL  skipped(tooling): $SKIPPED_TOOLING  accounted: $ASSERT_ACCOUNTED (floor $ASSERT_FLOOR)"
[ "$FAIL" -eq 0 ]
