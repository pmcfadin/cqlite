#!/usr/bin/env bash
# test_ws0_embedded_steps_execute.sh — THE EXECUTE DIRECTION OF THE DRIVER'S OWN EMBEDDED PYTHON
# (issue #3451).
#
# # WHY THIS FILE EXISTS — a step that is only ever PARSED is a step that has never RUN
#
# `scripts/perf/ws0-baseline.sh` shipped with SEVEN instances of a defect that no CPython accepts:
# a backslash inside an f-string EXPRESSION, which the tokenizer reads as a line continuation
# (`SyntaxError: unexpected character after line continuation character`). They lived in the
# driver's TWO multi-line embedded python blocks — the SESSION-CORPUS-PIN step and the
# CPU-PIN-VERIFICATION step — and both steps are `|| exit 2`, so the whole WS0 measurement path was
# unrunnable end to end on `main`.
#
# Every existing guard was green throughout, and each for a structural reason rather than by
# accident:
#
#   * the python is not a `.py` file, so no linter, formatter or import ever reads it;
#   * `bash -n` parses the driver as SHELL, where a `python3 -c '…'` body is ONE opaque
#     single-quoted string — valid whatever it contains;
#   * every hermetic self-test stops at `--validate-args-only`, which sits ABOVE both steps, and
#     #3272 round 2 finding 14 deliberately made the accept-direction cases execute NOTHING
#     (running the real driver invoked `sudo sysctl` and three cargo builds inside a gate
#     component). Correct, and it left these two steps with no coverage that EXECUTES them.
#
# So the property this file asserts is the one nothing else can:
#
#     THE DRIVER'S EMBEDDED PYTHON STEPS **RUN**, ON FIXTURE INPUTS, AND PRODUCE THEIR ARTIFACTS.
#
# # HOW, AND THE TWO THINGS THAT MAKE IT A MEASUREMENT RATHER THAN A RESTATEMENT
#
# 1. THE BLOCK TEXT IS EXTRACTED FROM THE SHIPPED DRIVER on every run
#    (`ws0_embedded_python.py`), never copied into this file. A self-test carrying its own copy
#    certifies THE COPY: it stays green while the shipped step is broken, which is exactly the
#    state #3451 found. The extractor fails CLOSED — an unclassifiable `python3` shape, or a block
#    whose closing delimiter it cannot find, is a finding naming the driver rather than a silent
#    omission that would print like a clean file.
#
# 2. EVERY ACCEPT ASSERTION HAS A PAIRED POSITIVE CONTROL, observed to FIRE against a scratch copy
#    of the driver under `$TMPDIR` carrying the injected defect. That is this issue's own lesson:
#    the owner's `grep -c` for the bad spelling returned `0` against a file holding all seven
#    instances, because the pattern was over-escaped — and a check that CANNOT fire reports nothing
#    and looks identical to a check that fired and found nothing. The defective spelling is
#    CONSTRUCTED IN CODE at the injection site (never written literally anywhere in this tree), so
#    no committed file carries an example a reader or a text-level probe could mistake for the
#    real thing.
#
# # WHAT IS REACHED, AND WHAT IS NOT — stated, because an honest partial is fine and a SILENT
#     partial is the thing this rig exists to refuse
#
# REACHED:
#   * both embedded steps EXECUTED, with the argv and the environment the driver gives them, over a
#     few-KB fixture corpus: exit 0, the artifact written, the pin lines on stdout, and the SHIPPED
#     READER (`verify_session_corpus_pin` / `verify_pinning_record`) accepting what the step wrote;
#   * THREE SETS OF ENVIRONMENT-VARIABLE NAMES AGREE: the names THE DRIVER EXPORTS (read from its
#     bash source), the shipped `ws0_session.MANIFEST_CONFIG_FIELDS` (read by importing it), and
#     the environment this suite builds. Executing the blocks alone cannot see a renamed export —
#     the python is untouched, so the census, the compile check and the execute cases all stay
#     green while the real rig hits the step's own `FATAL: … was not exported` and its caller
#     exits 2, i.e. #3451's exact symptom. The same cross-check covers the CPU-pin step's four
#     `WS0_PIN_*` inputs, there against the literal names the extracted block reads;
#   * EVERY DISCOVERED embedded block in the driver COMPILES, so instance #8 anywhere in that file
#     is caught and not only the two steps this issue repaired. DISCOVERED is the operative word
#     and its scope is stated exactly: every `-c`-form invocation REGARDLESS of how its command
#     word is spelled (the census anchors on the flag), plus every invocation whose command word
#     contains a literal `python` — both DISCOVERED and CLASSIFIED over the LOGICAL-LINE
#     reconstruction, so a backslash-newline continuation neither hides an anchor nor turns an
#     ordinary invocation into a refusal. Block BODIES are still read from the original text,
#     because inside single quotes a backslash is literal and bash continues nothing. What this
#     does not reach is in NOT REACHED below;
#   * the extractor's delimiter in BOTH directions: it reports a defect, AND it does not
#     manufacture one on good input. A block is delimited by bash's quoting rules rather than by a
#     line pattern, so all three closer shapes this repository actually uses are handled — the
#     column-0 closer, the closer at the end of the last body line with bash arguments trailing,
#     and the `'"'"'` literal-apostrophe idiom mid-body.
#
# NOT REACHED (and therefore NOT claimed):
#   * the driver's SHELL path into these steps. Reaching them in situ means passing the CPU-sibling
#     verification against real sysfs, `relax_perf_sysctls` (a host `sudo sysctl -w`) and a release
#     build — the exact escape `lib-ws0-hermetic.sh` exists to prevent. What is asserted is that the
#     step's python runs; that the driver's env exports match the names the block reads is asserted
#     structurally (the `MANIFEST_CONFIG_FIELDS` equality above and the driver's own fatal-on-absent
#     branch, exercised below), not by executing the shell around it.
#   * THE BASH ERROR HANDLING AROUND THE STEPS. This suite executes the embedded PYTHON; the
#     `|| { echo "FATAL: …" >&2; exit 2; }` clause that makes a step's failure fatal is not a
#     subject. MEASURED: deleting that clause is invisible here — the python is unchanged, so the
#     census, the compile check and the execute cases all stay green while a failed pin silently
#     becomes non-fatal and the session proceeds with no corpus pin, refused much later by the
#     reporter. Deliberately NOT asserted structurally: a check keyed on where `|| {` sits is
#     brittle, and a brittle assert on correct code is the false-red that gets a guard waived.
#     Stated here so the limit is known rather than assumed away.
#   * CROSS-INTERPRETER PORTABILITY, AT ALL. The compile check establishes that the blocks parse
#     on THE INTERPRETER RUNNING IT, and nothing here establishes more than that. Concretely, the
#     regression that would NOT be caught is a subscript written inside an f-string expression
#     with the SAME quote character as the f-string itself — legal from 3.12 under PEP 701 and a
#     SyntaxError below it. It is DESCRIBED rather than reproduced here, as the driver's own
#     comment describes it, and for the same reason: a literal example of a defect inside the
#     prose about that defect is one more thing to be mistaken for the real thing. That comment
#     remains the durable record of why locals are bound instead.
#
#     A tokenizer model of that spelling lived here for two review rounds and was REMOVED (#3451
#     round 4). It was wrong twice — first overclaiming, then flagging legal triple-quoted code,
#     a FALSE RED, which is the worse direction because a guard that reds on correct code is the
#     guard people learn to waive. Each fix was individually right and the model stayed wrong
#     somewhere new, which is what a second implementation of CPython's tokenizer does when its
#     correctness can only be established by differential testing against an original we do not
#     have on this box. THE ONLY HONEST ORACLE IS A REAL INTERPRETER: compiling the blocks under
#     an actual 3.9/3.11, which is a CI lane and not a hermetic self-test.
#
#     What survives is the oracle that is real: no CPython accepts the BACKSLASH form, so the
#     compile check catches the defect this issue is actually about on 3.9 through 3.12 alike.
#   * A `-c` FLAG SPELLED THROUGH A VARIABLE (`$FLAG` where `FLAG=-c`). Every quoting spelling
#     bash glues into `-c` IS discovered — `-"c"`, `-'c'`, `"-c"`, `'-c'`, `\-c` — but a flag
#     whose text never appears cannot be, and resolving it needs the shell. Stated rather than
#     chased.
#   * COMPLETE DISCOVERY OF EMBEDDED PYTHON, which is NOT STATICALLY ACHIEVABLE and is therefore a
#     stated decision rather than an oversight. A shell command word can be spelled arbitrarily —
#     a variable, a concatenation, `$(which python3)`, an alias, `eval` — so enumerating the
#     spellings is the same open list the round-6 inversion escaped, one level down. The two
#     anchors cover the decidable part: the `-c` FLAG (any command-word spelling) and a literal
#     `python` word (any argument shape), both over the logical-line reconstruction (bash deletes
#     backslash-newline before tokenising, and so does discovery — a single closed rule bash
#     itself applies, not another spelling on a list). NOT discovered: an indirectly-spelled
#     command word COMBINED with a non-`-c` form — `$PYTHON <<'PY'`, or code reaching python
#     through `eval`.
#     Closing that would require interpreting bash, which is the thing this file does not do.
#     If a future review reports another command-word spelling, the answer is this limit, not
#     more matching.
#   * anything measured: no `perf`, no Flight server, no rep loop, no 2.8 GB corpus.
#
# Hermetic: python3, a few KB under `$TMPDIR`, and `PYTHONDONTWRITEBYTECODE=1` so importing the
# shipped modules writes no `__pycache__` into the checkout. No sudo, cargo, perf, taskset,
# network, root, and no
# invocation of the driver itself (this file never runs it — it reads it).
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DRIVER="$REPO_ROOT/scripts/perf/ws0-baseline.sh"
PERF_DIR="$REPO_ROOT/scripts/perf"
EXTRACT="$REPO_ROOT/scripts/tests/ws0_embedded_python.py"

fails=0
# `checks` counts what actually RAN, so the floor at the end can see a block that silently never
# executed while the gate reads only the exit code. Never incremented inside a `( … )` subshell or
# a pipeline stage: a suite that printed 22 FAILs and reported `failed: 0` has already happened in
# this tree.
checks=0
pass() { checks=$((checks + 1)); echo "ok   - $1"; }
fail() { checks=$((checks + 1)); echo "FAIL - $1"; fails=$((fails + 1)); }

[ -f "$DRIVER" ] || { echo "FAIL - missing $DRIVER"; exit 1; }
[ -f "$EXTRACT" ] || { echo "FAIL - missing $EXTRACT"; exit 1; }
# python3 is a HARD REQUIREMENT of this rig (the driver refuses to run without it), so its absence
# is a FAILED CHECK and never a skip: exiting 0 here would record the component as SUCCESS with
# none of the checks below having run — the vacuous green this rig exists to refuse.
command -v python3 >/dev/null 2>&1 || {
  echo "FAIL - python3 is not installed. It is a HARD REQUIREMENT of the WS0 rig, so its absence"
  echo "       is a failed check and not a skip."
  exit 1
}

# shellcheck source=scripts/tests/lib-ws0-fixtures.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-fixtures.sh"

# A NAMED TEMPLATE, and the result VERIFIED before anything derives a path from it or a trap is
# installed. This file is `set -uo pipefail` with no `-e`, so an unchecked `mktemp -d` failure
# would leave `$TMP` EMPTY: every path below becomes an absolute one (`/corpus`, `/session`,
# `/step-output.txt`), a privileged runner writes persistent artifacts at the filesystem root, and
# `cleanup` then `rm -rf`s a path built the same way. Non-empty AND an existing directory, checked
# BEFORE `trap`, is the whole fix.
# NO BYTECODE INTO THE CHECKOUT (#3451 post-rebase round 8, F2). Importing the shipped modules
# from `scripts/tests` and `scripts/perf` writes `__pycache__/` there, so the header's "a few KB
# under $TMPDIR" was FALSE — gitignored, so nothing reached git, but the claim was still wrong,
# and an overclaiming header is the class we just deleted an eval-safety paragraph for.
export PYTHONDONTWRITEBYTECODE=1
# `assert` IS REMOVED BY -O (#3451 post-rebase round 9, F1). Measured:
# `PYTHONOPTIMIZE=1 python3 -c 'assert False, "fires"'` prints nothing and exits 0. Ten checks in
# this suite were `assert`s, so an inherited variable would have switched all ten off at once —
# a valid-but-swapped field passing silently. They are explicit conditionals now, which is THE
# fix; unsetting the variable is only a second line of defence, because an env precaution is
# itself something a new call site can miss.
unset PYTHONOPTIMIZE

TMP="$(mktemp -d "${TMPDIR:-/tmp}/ws0-embedded-steps.XXXXXX")" || TMP=""
if [ -z "$TMP" ] || [ ! -d "$TMP" ]; then
  echo "FAIL - could not create a scratch directory under ${TMPDIR:-/tmp}; refusing to run, because"
  echo "       every path below would otherwise be built from an EMPTY prefix and the cleanup trap"
  echo "       would rm -rf it."
  exit 1
fi
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT
# Where an executed step's combined output lands. A FILE rather than a command substitution, so
# the status variables the runners set survive into the caller (see `run_pin_step`).
STEP_OUT="$TMP/step-output.txt"
: > "$STEP_OUT"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# census <driver> / compile_blocks <driver> / emit_block <driver> <n> — the extractor's three
# modes. Called through `python3` with the script as an ARGUMENT, never executed directly, so no
# line here can be read as an invocation of the measurement driver.
census() { python3 "$EXTRACT" census "$1" 2>&1; }
compile_blocks() { python3 "$EXTRACT" compile "$1" 2>&1; }
emit_block() { python3 "$EXTRACT" emit "$1" "$2"; }

# findings_of <output> — the extractor's output minus its `#COMPLETE` marker, i.e. the findings.
# The marker is filtered HERE rather than by each reader, for the reason `lib-ws0-hermetic.sh`
# gives: a diagnostic a human must remember to ignore is one they will read as a finding.
# `([[:space:]]|$)` rather than `\b`: the word boundary is a GNU extension that BSD grep (macOS)
# does not interpret, so the record lines would survive this filter and every `findings_of` test
# below would see them as findings — the mandatory gate failing on a supported host (#3451 review
# round 4, same macOS family as the bash 3.2 item).
findings_of() {
  grep -v '^#COMPLETE ' <<<"$1" \
    | grep -vE '^(BLOCK|MENTION|SCRIPT)([[:space:]]|$)' \
    | grep -v '^$'
}

# driver_step_env <driver> <block-needle> — the `WS0_CFG_*`/`WS0_PIN_*` environment THE DRIVER
# ITSELF builds for that step, as `NAME=VALUE` lines, with controlled values substituted for the
# shell variables it reads.
#
# WHY THIS EXISTS (#3451 post-rebase round 2, F3). Every other check validates export NAMES and
# prefix membership; the executions then used values THIS SUITE constructed. So a production
# MAPPING error — `WS0_CFG_TEMPS="$ARMS"`, or two swapped `WS0_PIN_*` right-hand sides — left the
# whole suite green while the driver recorded a configuration it never measured. That is the
# #3272 F1 class the driver's own comment is about: the reporter READS its configuration from the
# manifest, so a wrong value there is a report describing a run that did not happen.
#
# It takes the RIGHT-HAND SIDES FROM THE DRIVER rather than restating them here, because a table
# of expected values in this file would be a second copy of the driver's intent and would drift.
#
# The controlled values are chosen so a SWAP IS DETECTABLE BY THE SHIPPED VALIDATORS rather than
# by a comparison written here: `temps` and `arms` have DISJOINT legal sets (warm/cold vs
# bypass/merge), so `WS0_CFG_TEMPS="$ARMS"` yields `temps=bypass`, which `session_manifest_config`
# refuses. The detector is production code, not a fixture expectation.
driver_step_env() {
  # PARSED AND SUBSTITUTED, NEVER EVALUATED (#3451 post-rebase round 7, F3). The round-2 version
  # ran `eval "$prefix env"` on the argument that the contiguity check bounded the input to
  # `NAME=` words with no separator. MEASURED, that check ADMITS `WS0_CFG_X=$(helper)`,
  # backticks and `${OTHER}` — all assignment-shaped, none containing a separator — so a driver
  # line of that shape would have executed repository-derived shell inside a test documented as
  # hermetic, in a mandatory gate component. `--resolve` parses a restricted grammar instead and
  # refuses, by name, every construct it does not explicitly support.
  python3 "$REPO_ROOT/scripts/tests/ws0_export_prefix.py" "$1" WS0_ "$2" --resolve \
    REPS=2 TEMPS=warm ARMS=bypass SCAN_PASSES=3 \
    SERVER_CPUS=2,10 CLIENT_CPUS=4,12 \
    STEP_DURATION=45s COLD_STEP_DURATION=1s \
    FLIGHT_ENDPOINT="$WS0_FIXTURE_ENDPOINT" BASELINE_MODE=non-baseline \
    EVENTS=cycles,instructions BIN_DIR_RECORDED=target/release PROFILE_RECORDED=off \
    "QUIESCENCE_RECORDED=NOT VERIFIED (no timeseries supplied)" \
    "WS0_SERVER_SIBLINGS=server cpu 2 siblings 2,10" "CPU_TOPOLOGY_ROOT=$TMP/fake-topology" \
    FLIGHT_SERVER_CPUS=2,10 FLIGHT_PIN_MODE=siblings FLIGHT_ALLOCATOR=system \
    "WS0_FLIGHT_PIN_VERIFIED=flight cpu 2 siblings 2,10" \
    "FLIGHT_ALLOCATOR_LIB_RECORDED=none (fixture)" \
    "FLIGHT_ALLOCATOR_VERIFICATION=per rep from /proc/<pid>/maps (fixture)"
}

# WHAT EACH RECORDED FIELD MUST BE, given the controlled inputs above (#3451 post-rebase round 5,
# F2). Distinct values are only half the fix: they make a swap PRODUCE a different artifact, and
# these assertions are what NOTICE it. Without them a swap between two fields the same validator
# accepts — `server_siblings_expanded` and `topology_root` are both just non-empty strings —
# still passes every shipped check.
#
# This is not a restatement of the driver's mapping. It is the definition of the controlled
# experiment: "the manifest's `temps` must be the value I put in the variable the driver reads
# for `temps`". A swapped right-hand side breaks exactly that.
WS0_EXPECTED_CFG=(
  "reps=2" "temps=warm" "arms=bypass" "scan_passes=3"
  "server_cpus=2,10" "client_cpus=4,12" "step_duration=45s/1s"
  "flight_endpoint=$WS0_FIXTURE_ENDPOINT" "baseline_mode=non-baseline"
  "events=cycles,instructions" "bin_dir=target/release" "profile=off"
  "quiescence=NOT VERIFIED (no timeseries supplied)"
  "flight_server_cpus=2,10"
)
# ...and the PIN side: the FIELD SET is derived from the driver, the VALUES are stated here
# (#3451 post-rebase round 6, F3).
#
# The hand-written list omitted `topology_root` while the verification claimed every
# driver-mapped pin field was compared. Adding the missing line fixes the symptom; the DEFECT is
# that a list can be incomplete and nothing notices, so the NEXT field added to the driver would
# be silently uncovered too.
#
# DERIVING THE VALUE AS WELL WAS THE OBVIOUS FIX AND IT IS CIRCULAR — I built it and measured it
# failing. Pairing each field with the value of the variable THE DRIVER maps it to means a
# swapped mapping supplies its own expectation: with
# `WS0_PIN_TOPOLOGY_ROOT="$WS0_SERVER_SIBLINGS"` injected into the real driver, expected and
# actual agreed and the suite passed. That is the artifact acting as its own oracle.
#
# So the two halves come from two sources, which is the shape that has held up everywhere in this
# suite: the FIELD SET is read out of the driver (nothing can be uncovered) and the EXPECTED
# VALUE is stated below (a wrong mapping is detectable), and the two sets are asserted EQUAL — so
# a field added to the driver fails loudly for want of a stated expectation rather than passing
# unchecked.
WS0_EXPECTED_PIN=(
  "server_cpus=2,10" "client_cpus=4,12"
  "server_siblings_expanded=server cpu 2 siblings 2,10"
  "topology_root=$TMP/fake-topology"
  # THE FLIGHT ARM (#3551). Each value is DISTINCT from every other in this table, which is what
  # makes a swapped right-hand side in the driver detectable at all: `flight_pin_verified` and
  # `flight_allocator_lib` are both just non-empty strings to the shipped validator, so only a
  # comparison against the value each was GIVEN can tell them apart.
  "flight_server_cpus=2,10" "flight_pin_mode=siblings"
  "flight_pin_verified=flight cpu 2 siblings 2,10"
  "flight_allocator=system" "flight_allocator_lib=none (fixture)"
  "flight_allocator_verification=per rep from /proc/<pid>/maps (fixture)"
)

# driver_pin_fields <driver> — the record fields the CPU block sources from the ENVIRONMENT, read
# out of the block itself (`"<field>": os.environ["WS0_PIN_<VAR>"]`). The completeness half.
driver_pin_fields() {
  python3 - "$REPO_ROOT/scripts/tests" "$1" <<'PY'
import pathlib, re, sys
sys.path.insert(0, sys.argv[1])
from ws0_embedded_python import census

records, _findings = census(pathlib.Path(sys.argv[2]))
owners = [r for r in records if r["kind"] == "BLOCK" and "pinning_record_path" in r["body"]]
if len(owners) != 1:
    print(f"AMBIGUOUS: {len(owners)} block(s) write the pinning record", file=sys.stderr)
    raise SystemExit(2)
fields = re.findall(r'"([a-z_]+)":\s*os\.environ\["WS0_PIN_[A-Z_]+"\]', owners[0]["body"])
if not fields:
    print("NO MAPPING: the CPU-pin block no longer sources any record field from the"
          " environment, so nothing can be derived — check the driver before relaxing this.",
          file=sys.stderr)
    raise SystemExit(3)
print(" ".join(sorted(set(fields))))
PY
}

# local_binding_of <driver> <block-needle> — the first `NAME = MAPPING["KEY"]` line in the block
# that calls <block-needle>, printed as `NAME MAPPING KEY`.
#
# DERIVED, NOT HARDCODED (#3451 review round 12). The injection controls used the literal names
# `pin_sha` / `rec_server_cpus`, and #3455 renamed those bindings to `_sha` / `_scpus` — so after
# the rebase every injection reported INJECTION IMPOSSIBLE and four controls could not fire. They
# failed loudly, which is the vacuity guard working, but a control keyed on a name the driver is
# free to change is a control that breaks on maintenance. The binding SHAPE is what the fix is
# about (a subscript bound to a local before the f-string), so the shape is what is matched.
local_binding_of() {
  python3 - "$REPO_ROOT/scripts/tests" "$1" "$2" <<'PY'
import pathlib, re, sys
sys.path.insert(0, sys.argv[1])
from ws0_embedded_python import census
records, _findings = census(pathlib.Path(sys.argv[2]))
owners = [r for r in records if r["kind"] == "BLOCK" and sys.argv[3] in r["body"]]
if len(owners) != 1:
    print(f"AMBIGUOUS: {len(owners)} block(s) call {sys.argv[3]!r}", file=sys.stderr)
    raise SystemExit(2)
pattern = re.compile(r'^([A-Za-z_]\w*) = ([A-Za-z_]\w*)\["(\w+)"\]$', re.M)
found = pattern.search(owners[0]["body"])
if not found:
    print(f"NO LOCAL BINDING found in the block calling {sys.argv[3]!r} — the step no longer"
          " binds a subscript to a local before its f-string, which is the very shape #3451"
          " exists to keep. Check the driver before relaxing this.", file=sys.stderr)
    raise SystemExit(3)
print(" ".join(found.groups()))
PY
}

# defective_copy <src> <dest> <placeholder> <mapping> <key> — a scratch copy of the driver whose
# f-string placeholder `{<placeholder>}` has been rewritten to the INLINE SUBSCRIPT spelling that
# NO CPython parses: a backslash inside the expression, which the tokenizer reads as a line
# continuation. That is the defect this issue is about, and it fails identically on 3.9 through
# 3.12, which is why the compile check that catches it needs no interpreter model.
#
# Exits non-zero when the placeholder is not found EXACTLY once, so a control that silently
# injected nothing is a failure rather than a green "the check found no defect".
defective_copy() {
  python3 - "$@" <<'PY'
import pathlib, sys
src, dest, placeholder, mapping, key = sys.argv[1:6]
# CONSTRUCTED from character codes rather than written out, so this file does not ship a literal
# example of the spelling (#3312's rule about prose inside a diff naming its own oracle, applied
# to a test that must build its own bad input).
backslash, dquote = chr(92), chr(34)
bad = "{" + mapping + "[" + backslash + dquote + key + backslash + dquote + "]}"
text = pathlib.Path(src).read_text()
needle = "{" + placeholder + "}"
found = text.count(needle)
if found != 1:
    print(f"INJECTION IMPOSSIBLE: {needle} occurs {found} time(s) in {src}, expected exactly 1."
          " The positive control below would observe NOTHING and pass vacuously, so this is a"
          " failure of the control rather than of its subject.", file=sys.stderr)
    raise SystemExit(1)
pathlib.Path(dest).write_text(text.replace(needle, bad))
PY
}

# export_prefix_membership <driver> <VAR-PREFIX> <BLOCK-NEEDLE>
#   -> "<in-this-block's-logical-line> <total-in-file>".
#
# An exported variable only reaches the step if its assignment is part of the CONTIGUOUS
# environment-assignment prefix of the `python3 -c` command THAT READS IT. Remove one continuation
# backslash and it becomes a standalone shell assignment python never sees — the driver dies on
# the missing variable while every other check here passes, because the name-set comparisons see
# it (still in the file) and the block executions see it (this suite builds their environment
# itself). Same symptom as #3451 itself.
#
# BOUND TO ONE BLOCK, NOT UNIONED ACROSS ALL OF THEM (#3451 review round 11). An earlier version
# fed one set from EVERY `python3 -c` logical line, so moving a `WS0_CFG_*` assignment out of the
# session-pin prefix and into the CPU-pin invocation's prefix left the total unchanged and the
# check green — while the session-pin step died at runtime on the absent variable. The consuming
# block is identified BY THE SHIPPED WRITER IT CALLS, the way this suite locates blocks
# everywhere, so reordering the driver cannot silently swap the two.
#
# Decidable, and it reuses the shipped joiner and census rather than parsing a command prefix:
# after logical-line joining, a step's assignments and its invocation ARE ONE LINE. Both are
# IMPORTED — a second copy here would drift, and then this check would certify the copy.
export_prefix_membership() {
  python3 "$REPO_ROOT/scripts/tests/ws0_export_prefix.py" "$1" "$2" "$3"
}

# ============================================================================
# PART 1 — THE EXTRACTOR READS THE SHIPPED DRIVER, AND FAILS CLOSED
# ============================================================================
# Everything below rests on the census finding the right subject in the right file. A census that
# silently found nothing would make every later assertion vacuous in exactly the way the seven
# shipped instances were invisible, so the census is asserted before it is used.

census_out="$(census "$DRIVER")"
if grep -q '^#COMPLETE ' <<<"$census_out" && [ -z "$(findings_of "$census_out")" ]; then
  pass "census: the shipped driver's python occurrences are ALL classified, with no finding ($(grep '^#COMPLETE ' <<<"$census_out"))"
else
  fail "census: the driver's python census did not COMPLETE cleanly — $(findings_of "$census_out" | head -3)"
fi

block_count="$(grep -c '^BLOCK	' <<<"$census_out")"
# EXACTLY the blocks this driver carries, not a floor. An under-count is the vacuous-green shape
# and a floor cannot see it once met: a delimiter that silently stopped recognising one shape
# would drop that block from BOTH the census and the compile check while the count still cleared
# a floor. (MEASURED tree-wide: a column-0-only closer rule found 31 blocks where the correct rule
# finds 59 — a loose delimiter under-counts SUBJECTS, it does not merely mis-cut them.)
#
# THE COUNT IS THE DRIVER'S, AND THE DRIVER MOVES. #3455 took it from 3 blocks to 5, so bumping
# this is the expected maintenance and the failure message says so. Everything else about the two
# steps is located STRUCTURALLY — by the shipped writer each body calls — precisely so that a
# driver edit costs one constant here and nothing more.
EXPECTED_BLOCKS=5
if [ "$block_count" -eq "$EXPECTED_BLOCKS" ]; then
  pass "census: exactly $block_count embedded python block(s) in the driver — the count is the DRIVER\x27s and moves with it; the two pin steps are located structurally, not by position"
else
  fail "census: $block_count embedded block(s) found in $DRIVER, expected $EXPECTED_BLOCKS. If a step was ADDED, bump EXPECTED_BLOCKS; if the count DROPPED, the extractor has stopped seeing a shape and the missing block is being compiled by nothing"
fi

# WHICH block is which, by CONTENT. The two steps are located by a shipped symbol each body calls,
# so a reordering of the driver cannot silently point this suite at the wrong step — and a step
# that DISAPPEARED is a failure here rather than a suite that quietly tests one block twice.
find_block() { # find_block <driver> <needle> — the index of the ONE block containing <needle>
  local drv="$1" needle="$2" n total hit="" body out
  out="$(python3 "$EXTRACT" census "$drv" 2>&1)"
  # THE MARKER IS REQUIRED, as every other caller requires it (#3451 review round 7, finding 2).
  # Without it a census that died — or never had a subject — yields zero BLOCK lines, and this
  # function would report a confident `ABSENT` that a positive control reads as success.
  if ! grep -q '^#COMPLETE ' <<<"$out"; then
    echo "INCOMPLETE"
    return 1
  fi
  total="$(grep -c '^BLOCK	' <<<"$out")"
  for ((n = 1; n <= total; n++)); do
    body="$(emit_block "$drv" "$n")"
    if grep -q -- "$needle" <<<"$body"; then
      [ -n "$hit" ] && { echo "AMBIGUOUS"; return 1; }
      hit="$n"
    fi
  done
  [ -n "$hit" ] || { echo "ABSENT"; return 1; }
  echo "$hit"
}

PIN_BLOCK="$(find_block "$DRIVER" 'write_session_corpus_pin')"
if [[ "$PIN_BLOCK" =~ ^[0-9]+$ ]]; then
  pass "census: the SESSION-CORPUS-PIN step is embedded block $PIN_BLOCK, located by the shipped writer it calls"
else
  fail "census: the session-corpus-pin step could not be located in $DRIVER ($PIN_BLOCK) — the execute-direction cases below would have no subject"
fi
CPU_BLOCK="$(find_block "$DRIVER" 'pinning_record_path')"
if [[ "$CPU_BLOCK" =~ ^[0-9]+$ ]]; then
  pass "census: the CPU-PIN-VERIFICATION step is embedded block $CPU_BLOCK, located by the shipped writer it calls"
else
  fail "census: the CPU-pin-verification step could not be located in $DRIVER ($CPU_BLOCK)"
fi

# --- POSITIVE CONTROL 1a: a block that cannot be DELIMITED is a finding, not a silent skip -----
# A scratch copy truncated at the opening quote of the first embedded block, with a body that
# contains no apostrophe of its own, so the shell string is genuinely never closed. Extracting to
# end-of-file would compile a truncated body and report a defect that is the extractor's own, so
# the extractor must refuse instead.
#
# The construction targets the scanner's end-of-file branch DIRECTLY. An earlier version of this
# control deleted every line beginning with a quote, which stopped working the moment the
# delimiter learned bash's real rule (any later quote in the file closes the string) — a control
# that silently stopped constructing its own subject, which is the class this suite exists to
# refuse. It failed loudly, which is the floor working.
UNDELIMITED="$TMP/undelimited-ws0-driver.sh"
python3 - "$DRIVER" "$UNDELIMITED" <<'INJECT'
import pathlib, sys
text = pathlib.Path(sys.argv[1]).read_text()
q = chr(39)
opener = "python3 -c " + q + "\n"
i = text.index(opener)
# Everything through the opening quote, then a body with no quote anywhere in it and no closer.
pathlib.Path(sys.argv[2]).write_text(text[: i + len(opener)] + "import sys\nprint(1)\n")
INJECT
und_out="$(census "$UNDELIMITED")"
if [ -n "$(findings_of "$und_out")" ] && grep -q 'never closed' <<<"$und_out"; then
  pass "census CONTROL fired: an unterminated block is a FINDING — $(findings_of "$und_out" | head -1 | cut -c1-110)"
else
  fail "census CONTROL did not fire: an unterminated embedded block must be a finding, got: $(head -3 <<<"$und_out")"
fi

# --- POSITIVE CONTROL 1b: an UNRECOGNISED python3 shape is a finding ---------------------------
# A `python3 -m …` step (or any shape the census does not know) may be carrying code the compile
# check would never see, so it fails closed rather than being skipped.
UNKNOWN_SHAPE="$TMP/unknown-shape-ws0-driver.sh"
{ cat "$DRIVER"; printf '%s\n' 'python3 -m ws0_something --run'; } > "$UNKNOWN_SHAPE"
unk_out="$(census "$UNKNOWN_SHAPE")"
if grep -q 'matches none of the shapes' <<<"$unk_out"; then
  pass "census CONTROL fired: an unrecognised python3 invocation shape is a FINDING (fail-closed, so a future step cannot fall outside the compile check)"
else
  fail "census CONTROL did not fire: an unrecognised python3 shape must be a finding, got: $(findings_of "$unk_out" | head -2)"
fi

# --- POSITIVE CONTROL 1c: a step that DISAPPEARED is a failure, not a suite testing one block twice
NO_PIN_STEP="$TMP/no-pin-step-ws0-driver.sh"
python3 - "$DRIVER" "$NO_PIN_STEP" <<'PY'
import pathlib, sys
text = pathlib.Path(sys.argv[1]).read_text()
pathlib.Path(sys.argv[2]).write_text(text.replace("write_session_corpus_pin", "some_other_writer"))
PY
no_pin_rc=$?
# THE FIXTURE MUST HAVE BEEN WRITTEN (#3451 review round 7, finding 2). If it could not be, the
# locator reads a MISSING FILE as `ABSENT` and this positive control passes having observed
# nothing — a control that cannot fail, inside the suite whose whole subject is refusing exactly
# that. The status is checked, and the file's existence with it.
if [ "$no_pin_rc" -ne 0 ] || [ ! -s "$NO_PIN_STEP" ]; then
  fail "census CONTROL: the disappeared-step fixture could not be written (rc=$no_pin_rc), so the control could not fire — it would otherwise read a missing file as ABSENT and pass vacuously"
elif [ "$(find_block "$NO_PIN_STEP" 'write_session_corpus_pin')" = "ABSENT" ]; then
  pass "census CONTROL fired: with the session-pin writer gone from a REAL fixture (existence asserted), the locator reports ABSENT rather than picking another block"
else
  fail "census CONTROL did not fire: the block locator must report ABSENT when its subject is not in the driver"
fi

# --- CONTROL 1c-bis: THE OTHER DIRECTION — the extractor must not MANUFACTURE a finding --------
# Every control above observes the census REPORTING something. A delimiter can also be wrong the
# other way, and that failure is worse in practice: it reds the gate on correct code, and a key
# that reds on correct input is the key agents learn to waive.
#
# The subject is a REAL sibling that carries the `'"'"'` idiom — the bash spelling that closes the
# single-quoted string, emits a literal apostrophe from a double-quoted segment and reopens. A
# delimiter that stops at the next quote cuts that block mid-body. `lib-ws0-fixtures.sh` says so
# itself, at the line that uses it: mishandling it "silently truncated this whole library — and it
# presented as every OTHER case in the suite failing on an absent pinning-verification.json."
#
# MEASURED, and this is why the file is the control rather than an illustration: under a
# next-quote delimiter this file yields exactly ONE false `SyntaxError` ('{' was never closed).
IDIOM_FILE="$REPO_ROOT/scripts/tests/lib-ws0-fixtures.sh"
idiom_census="$(census "$IDIOM_FILE")"
idiom_compile="$(compile_blocks "$IDIOM_FILE")"
idiom_blocks="$(grep -c '^BLOCK	' <<<"$idiom_census")"
idiom_apostrophe=0
for ((bn = 1; bn <= idiom_blocks; bn++)); do
  emit_block "$IDIOM_FILE" "$bn" | grep -q "'" && idiom_apostrophe=1
done
# THE SUBJECT IS THE IDIOM, NOT A FINDING COUNT. This file also contains a python HEREDOC at
# `lib-ws0-fixtures.sh:149`, and since round 6 removed heredoc support that is a finding BY
# DESIGN — the allowlist tells its author to teach the census rather than compiling a body the
# shell may have rewritten. So the assertion is about the blocks that ARE delimited (they must all
# compile, and one must carry a literal apostrophe, which a truncating delimiter cannot produce)
# plus the requirement that every finding on this file is the heredoc one. Asserted by CONTENT
# rather than by count, so a NEW kind of false finding here still fails.
idiom_other="$(findings_of "$idiom_census" | grep -v 'matches none of the shapes' || true)"
if [ "$idiom_blocks" -ge 1 ] && [ "$idiom_apostrophe" -eq 1 ] \
   && [ -z "$idiom_other" ] && [ -z "$(findings_of "$idiom_compile" | grep -v 'matches none of the shapes' || true)" ]; then
  pass "census NO-FALSE-FINDING: $idiom_blocks block(s) in lib-ws0-fixtures.sh (which uses the literal-apostrophe idiom) are delimited, ALL COMPILE, and one body carries a literal apostrophe — the extractor rejoins the idiom instead of cutting the body there (its heredoc at :149 is an expected allowlist finding, not a false one)"
else
  fail "census manufactured a finding on GOOD input: lib-ws0-fixtures.sh gave blocks=$idiom_blocks apostrophe-in-a-body=$idiom_apostrophe unexpected-finding='$(head -1 <<<"$idiom_other")'"
fi

# --- CONTROL 1c-ter: the closer at the END of the last body line, bash arguments trailing -------
# Shape 2 of three (see the extractor header). It is idiomatic and already in use at
# `test-data/scripts/gen-perf-corpus-bti.sh`, `scripts/lib/gate-notify.sh` and
# `docs/reports/ws0-3217-artifacts/harness/common.sh`, so it is the shape most likely to be written
# into this driver next. Exercised against a SCRATCH copy rather than one of those files, so the
# control cannot drift when they change: the block must be delimited (not reported undelimited) AND
# a defect inside it must still be reported.
#
# NO APOSTROPHE anywhere in the injected body, and that is load-bearing rather than style: the body
# sits inside SHELL SINGLE QUOTES, so one closes the string early. An earlier version of this
# fixture used a dict literal with quoted keys, was truncated at its first apostrophe, and STILL
# "reported a defect" — the truncation, not the injected one. Round 5's word-boundary check turned
# that into a visible failure, which is the check working; the fixture is now apostrophe-free, the
# same rule `lib-ws0-fixtures.sh` records about its own bodies.
TRAILING_DRIVER="$TMP/trailing-closer-ws0-driver.sh"
python3 - "$DRIVER" "$TRAILING_DRIVER" <<'INJECT'
import pathlib, sys
# The same defect class as the two shipped steps, built from character codes so no committed file
# holds a literal example of the spelling.
backslash, dquote = chr(92), chr(34)
bad = "{d[" + backslash + dquote + "k" + backslash + dquote + "]}"
q = chr(39)
step = ("python3 -c " + q + "\nd = dict(k=1)\nprint(f" + dquote + bad + dquote + ")"
        + q + " \"$OUT_DIR\"\n")
pathlib.Path(sys.argv[2]).write_text(pathlib.Path(sys.argv[1]).read_text() + step)
INJECT
tr_census="$(census "$TRAILING_DRIVER")"
tr_compile="$(compile_blocks "$TRAILING_DRIVER")"
tr_blocks="$(grep -c '^BLOCK	' <<<"$tr_census")"
if [ "$tr_blocks" -eq "$((block_count + 1))" ] && [ -z "$(findings_of "$tr_census")" ] \
   && grep -q 'DOES NOT COMPILE' <<<"$tr_compile"; then
  pass "census CONTROL fired (trailing-closer shape): a block closed at the END of its last body line is DELIMITED ($tr_blocks blocks, no undelimited finding) and a defect inside it is REPORTED"
else
  fail "census CONTROL did not fire (trailing-closer shape): blocks=$tr_blocks (expected $((block_count + 1))), census='$(findings_of "$tr_census" | head -1)', compile='$(findings_of "$tr_compile" | head -1)'"
fi

# --- CONTROL 1c-quater: TWO invocations on ONE line, the SECOND carrying the defect ------------
# The census advertises "every occurrence". An earlier version searched each line ONCE and then
# skipped whole lines, so a second invocation after a `;`, an `&&` or an inline block's closing
# quote was silently dropped — a vacuous pass inside the TOTAL property itself. MEASURED against
# `python3 -c '<ok>'; python3 -c '<defect>'`: `blocks=1 occurrences=1`, and the defect in the
# second block was invisible to the compile check.
#
# The defect is put in the SECOND block deliberately: with it in the first, a line-at-a-time
# scanner still reports it and the control proves nothing.
TWO_PER_LINE="$TMP/two-per-line-ws0-driver.sh"
python3 - "$DRIVER" "$TWO_PER_LINE" <<'INJECT'
import pathlib, sys
q = chr(39)
step = ("python3 -c " + q + "import sys" + q + "; python3 -c " + q + "import os," + q + "\n")
pathlib.Path(sys.argv[2]).write_text(pathlib.Path(sys.argv[1]).read_text() + step)
INJECT
two_census="$(census "$TWO_PER_LINE")"
two_compile="$(compile_blocks "$TWO_PER_LINE")"
two_blocks="$(grep -c '^BLOCK	' <<<"$two_census")"
if [ "$two_blocks" -eq "$((block_count + 2))" ] && grep -q 'DOES NOT COMPILE' <<<"$two_compile"; then
  pass "census CONTROL fired (two per line): BOTH invocations on one line are counted ($two_blocks blocks) and the defect in the SECOND is REPORTED — the scan advances by what each classification consumed, not by whole lines"
else
  fail "census CONTROL did not fire (two per line): blocks=$two_blocks (expected $((block_count + 2))), compile said '$(findings_of "$two_compile" | head -1)'"
fi

# --- CONTROL 1e: a block's closing quote must be followed by a shell WORD BOUNDARY -------------
# Bash CONCATENATES adjacent word fragments, so a block written `-c 'pass'" +"` runs `pass +`.
# Extracting the quoted part alone would approve a program python never receives — a FALSE PASS,
# measured: bash raised SyntaxError while the census reported `compiled=1 findings=0`.
#
# BOTH DIRECTIONS, and the ACCEPT half is the one that matters most here. The driver's own inline
# block closes `')" || {` — a `)` immediately after the quote — so a whitespace-only boundary rule
# would flag 1 of the driver's 3 real blocks on its first run. That exact shape is pinned below so
# a future tightening cannot break the real subject silently; the false-red direction is what got
# the previous oracle deleted.
BOUNDARY_OK="$TMP/boundary-ok-ws0-driver.sh"
python3 - "$BOUNDARY_OK" <<'INJECT'
import pathlib, sys
q = chr(39)
# The driver's own closer shape, reproduced exactly: `')" || {`.
pathlib.Path(sys.argv[1]).write_text(
    'now="$(python3 -c ' + q + 'import time; print(time.monotonic_ns())' + q + ')" || {\n'
    '  exit 2\n}\n')
INJECT
BOUNDARY_BAD="$TMP/boundary-bad-ws0-driver.sh"
python3 - "$BOUNDARY_BAD" <<'INJECT'
import pathlib, sys
q, dq = chr(39), chr(34)
# An ADJACENT FRAGMENT: bash appends it, so the program python runs is not the quoted text.
pathlib.Path(sys.argv[1]).write_text(
    'python3 -c ' + q + 'pass' + q + dq + ' +' + dq + '\n')
INJECT
bound_ok="$(census "$BOUNDARY_OK")"
bound_bad="$(census "$BOUNDARY_BAD")"
if [ "$(grep -c '^BLOCK	' <<<"$bound_ok")" -eq 1 ] && [ -z "$(findings_of "$bound_ok")" ] \
   && grep -q 'word boundary' <<<"$bound_bad"; then
  pass "census word-boundary, BOTH directions: the driver's own \`')\" || {\` closer is ACCEPTED (a shell metacharacter is a boundary, not just whitespace) while an adjacent fragment is a FINDING — bash would concatenate it and python would receive different source"
else
  fail "census word-boundary: accept-blocks=$(grep -c '^BLOCK	' <<<"$bound_ok") accept-findings='$(findings_of "$bound_ok" | head -1)' reject='$(findings_of "$bound_bad" | head -1)'"
fi

# --- CONTROL 1f: A PATH-QUALIFIED INVOCATION IS SEEN AND NAMED (r6F3 regression) ---------------
# The prior matcher required the bare word, so `/usr/bin/python3 -c '<defect>'` was not
# mis-handled — it was INVISIBLE: `blocks=0 findings=0 occurrences=0`, a defective block reported
# as clean. The candidate net now matches any word whose BASENAME is python/python3, and the
# allowlist admits only the bare spelling, so the path-qualified form is a FINDING.
PATHQ="$TMP/path-qualified.sh"
python3 - "$PATHQ" <<'INJECT'
import pathlib, sys
q = chr(39)
pathlib.Path(sys.argv[1]).write_text("/usr/bin/python3 -c " + q + "import os," + q + "\n")
INJECT
pathq_out="$(census "$PATHQ")"
if grep -q 'not the bare' <<<"$pathq_out"; then
  pass "census CONTROL fired (path-qualified): /usr/bin/python3 is SEEN and NAMED rather than invisible — $(findings_of "$pathq_out" | head -1 | cut -c1-96)"
else
  fail "census CONTROL did not fire (path-qualified): a path-qualified invocation must be a finding, got: $(head -2 <<<"$pathq_out")"
fi

# --- CONTROL 1g: A HEREDOC IS A FINDING — support was REMOVED, deliberately --------------------
# Heredoc handling was speculative (the driver has none) and cost THREE findings: an unquoted
# delimiter is shell-expanded before python sees it, a composed delimiter means bash uses a
# different tag, and with multiple redirects bash uses the LAST. Each was a FALSE PASS — a body
# compiled that python never receives. Under the allowlist a heredoc step is a FINDING, which is
# the correct outcome: it tells its author to teach the census rather than silently approving an
# expanded or truncated body.
HEREDOC_ANY="$TMP/heredoc-any.sh"
python3 - "$HEREDOC_ANY" <<'INJECT'
import pathlib, sys
q = chr(39)
tag = "PY" + "ANY"
pathlib.Path(sys.argv[1]).write_text(
    "python3 - <<" + q + tag + q + "\nimport os,\n" + tag + "\n")
INJECT
hd_any="$(census "$HEREDOC_ANY")"
if [ "$(grep -c '^BLOCK	' <<<"$hd_any")" -eq 0 ] && grep -q 'matches none of the shapes' <<<"$hd_any"; then
  pass "census CONTROL fired (heredoc removed): a python heredoc is a FINDING, not a block — the three false passes heredoc support cost are unreachable because the support is gone"
else
  fail "census CONTROL did not fire (heredoc removed): a heredoc must be a finding, got: $(head -2 <<<"$hd_any")"
fi

# --- CONTROL 1h: LOGICAL-LINE RECONSTRUCTION — three cases, each with its own right answer ------
# A candidate must be FOUND before it can be classified, and BOTH steps read the logical line bash
# builds by deleting backslash-newline. The three cases pin the whole behaviour, and they do NOT
# share an expected outcome — which is the point, because an earlier version got case A wrong in a
# way a uniform "everything is a finding" assertion would have hidden:
#
#   A  literal python3, continuation   a BLOCK. After joining this IS `python3 -c 'prog'`, so it
#                                      classifies normally and the COMPILE check reports its
#                                      defect. A driver block reformatted across a continuation
#                                      must keep working rather than become a refusal — that is
#                                      what makes the joining worth having.
#   B  $PYTHON,        continuation    exactly ONE finding. This is the case that was INVISIBLE
#                                      (`findings=0 occurrences=0`) before joining: the indirect
#                                      word defeats the token matcher and the continuation hides
#                                      the quote from the `-c` anchor.
#   C  $PYTHON,        same line       exactly ONE finding, via the `-c` flag anchor.
python3 - "$TMP" <<'INJECT'
import pathlib, sys
q, bs = chr(39), chr(92)
tmp = pathlib.Path(sys.argv[1])
prog = q + "import os," + q
(tmp / "indirect-A.sh").write_text("python3 -c " + bs + "\n" + prog + "\n")
(tmp / "indirect-B.sh").write_text("$PYTHON -c " + bs + "\n" + prog + "\n")
(tmp / "indirect-C.sh").write_text("$PYTHON -c " + prog + "\n")
INJECT
indirect_rc=$?
a_census="$(census "$TMP/indirect-A.sh")"
a_compile="$(compile_blocks "$TMP/indirect-A.sh")"
b_findings="$(findings_of "$(census "$TMP/indirect-B.sh")" | wc -l | tr -d ' ')"
c_findings="$(findings_of "$(census "$TMP/indirect-C.sh")" | wc -l | tr -d ' ')"
if [ "$indirect_rc" -ne 0 ]; then
  fail "census CONTROL: the logical-line fixtures could not be written (rc=$indirect_rc), so the control could not fire"
elif [ "$(grep -c '^BLOCK	' <<<"$a_census")" -eq 1 ] && [ -z "$(findings_of "$a_census")" ] \
     && grep -q 'DOES NOT COMPILE' <<<"$a_compile" \
     && [ "$b_findings" -eq 1 ] && [ "$c_findings" -eq 1 ]; then
  pass "census CONTROL fired (logical-line reconstruction): a LITERAL python3 split across a continuation is a BLOCK whose defect the compile check reports, while an INDIRECT command word is exactly ONE finding whether the quote is on the same line or past the continuation"
else
  fail "census CONTROL did not fire: A-blocks=$(grep -c '^BLOCK	' <<<"$a_census") A-findings='$(findings_of "$a_census" | head -1)' A-compile='$(findings_of "$a_compile" | head -1)' B-findings=$b_findings C-findings=$c_findings"
fi

# --- CONTROL 1h-bis: NO `-c` INVOCATION IS SILENTLY ABSENT --------------------------------------
# THE PROPERTY THAT MATTERS, replacing an earlier "at most one finding per invocation" assert that
# was the wrong shape (#3451 review round 9). That earlier invariant was COSMETIC and could only
# be satisfied by SEMANTIC machinery — deciding which invocation a match belongs to needs shell
# nesting and quoting — and the syntactic approximation built for it had a hole immediately:
#
#     v=$(python3 helper.py ) $PY -c 'import os,'
#
# the inner `python3 helper.py` classified as a harmless SCRIPT and the suppression swallowed the
# OUTER `-c` anchor, so invalid code escaped with the census reporting clean. Suppression was
# BLINDNESS; duplicate findings are only NOISE. So the suppression is gone and the assertion is
# the one that cannot be satisfied by hiding: a file containing a `-c '…'` invocation must yield a
# BLOCK or at least one FINDING — never neither.
#
# Swept across every negative fixture this suite builds, so a future change that makes ANY of them
# vanish fails here rather than in a review round.
absent_ok=1
absent_detail=""
python3 - "$TMP" <<'INJECT'
import pathlib, sys
q, dq, bs = chr(39), chr(34), chr(92)
tmp = pathlib.Path(sys.argv[1])
prog = q + "import os," + q
# The round-9 escape: a command substitution whose inner invocation looks harmless.
(tmp / "absent-nested.sh").write_text("v=$(python3 helper.py ) $PY -c " + prog + "\n")
# ...and the shapes the earlier rounds closed, re-swept together.
(tmp / "absent-indirect.sh").write_text("$PYTHON -c " + prog + "\n")
(tmp / "absent-continuation.sh").write_text("$PYTHON -c " + bs + "\n" + prog + "\n")
(tmp / "absent-pathq.sh").write_text("/usr/bin/python3 -c " + prog + "\n")
(tmp / "absent-fragment.sh").write_text("python3 -c " + q + "pass" + q + dq + " +" + dq + "\n")
INJECT
absent_rc=$?
for absent_case in nested indirect continuation pathq fragment; do
  absent_out="$(census "$TMP/absent-$absent_case.sh")"
  absent_blocks="$(grep -c '^BLOCK	' <<<"$absent_out")"
  absent_finds="$(findings_of "$absent_out" | wc -l | tr -d ' ')"
  if [ "$absent_blocks" -eq 0 ] && [ "$absent_finds" -eq 0 ]; then
    absent_ok=0
    absent_detail="$absent_detail $absent_case(silent)"
  fi
done
if [ "$absent_rc" -eq 0 ] && [ "$absent_ok" -eq 1 ]; then
  pass "census CONTROL fired (no silent absence): every -c invocation across 5 negative fixtures — including the command-substitution nesting that defeated the old suppression — yields a BLOCK or a FINDING, never neither"
else
  fail "census CONTROL did not fire: fixture-rc=$absent_rc silently-absent:$absent_detail — an invocation the census neither blocks nor reports is the false pass this whole file exists to refuse"
fi

# --- CONTROL 1i: A MISSING SUBJECT EXITS NONZERO ------------------------------------------------
# `census <a path that does not exist>` used to print a message and exit 0, so a caller that
# checks the STATUS read "no subject at all" as "nothing wrong" — the vacuous pass one level up
# from everything else this suite asserts.
python3 "$EXTRACT" census "$TMP/definitely-not-a-driver.sh" >/dev/null 2>&1
missing_rc=$?
if [ "$missing_rc" -ne 0 ]; then
  pass "census CONTROL fired (missing subject): an absent driver exits NONZERO (rc=$missing_rc), so a status-checking caller cannot read 'no subject' as 'nothing wrong'"
else
  fail "census CONTROL did not fire: an absent driver must exit nonzero, got rc=$missing_rc"
fi

# --- CONTROL 1j: a leading CONTROL WORD is stepped over, not read as the command ---------------
# `if $PY -c 'bad'`, `time $PY -c 'bad'` and `! $PY -c 'bad'` each resolved their command word to
# the control word — a plain literal containing no `python` — and were skipped as "another
# program": three silent absences.
#
# BOTH DIRECTIONS, and the accept half is why the words are STEPPED OVER rather than refused.
# Refusing a leading reserved word would turn `if grep -c 'foo' file; then :; fi` into a finding,
# which is ordinary shell — the false-red failure that already cost this checker two earlier
# designs. The words come from `ws0_hermeticity_lint.RESERVED_WORDS`, which is asserted for SET
# EQUALITY against `bash -c 'compgen -k'`, so this is a lookup against an oracle rather than a
# second hand-written list.
python3 - "$TMP" <<'INJECT'
import pathlib, sys
q = chr(39)
tmp = pathlib.Path(sys.argv[1])
bad = q + "import os," + q
(tmp / "ctlword-if.sh").write_text("if $PY -c " + bad + "; then :; fi\n")
(tmp / "ctlword-time.sh").write_text("time $PY -c " + bad + "\n")
(tmp / "ctlword-bang.sh").write_text("! $PY -c " + bad + "\n")
(tmp / "ctlword-if-grep.sh").write_text("if grep -c " + q + "foo" + q + " file; then :; fi\n")
(tmp / "ctlword-time-grep.sh").write_text("time grep -c " + q + "foo" + q + " file\n")
INJECT
ctlword_rc=$?
ctlword_ok=1
ctlword_detail=""
for ctlword_case in if time bang; do
  [ -n "$(findings_of "$(census "$TMP/ctlword-$ctlword_case.sh")")" ] \
    || { ctlword_ok=0; ctlword_detail="$ctlword_detail $ctlword_case(missed)"; }
done
for ctlword_case in if-grep time-grep; do
  [ -z "$(findings_of "$(census "$TMP/ctlword-$ctlword_case.sh")")" ] \
    || { ctlword_ok=0; ctlword_detail="$ctlword_detail $ctlword_case(false-red)"; }
done
if [ "$ctlword_rc" -eq 0 ] && [ "$ctlword_ok" -eq 1 ]; then
  pass "census CONTROL fired (control words): a leading if/time/! is STEPPED OVER so the real command word is judged — the indirect forms are findings while the same constructs leading a literal grep stay clean, which is why the words are skipped and not refused"
else
  fail "census CONTROL did not fire (control words): fixture-rc=$ctlword_rc problems:$ctlword_detail"
fi

# --- STRUCTURAL: no consumer classifies from PHYSICAL text -------------------------------------
# THE ONLY CHECK HERE THAT CAN STOP INSTANCE SIX. The joined-vs-physical inconsistency has
# produced FIVE findings across five rounds — discovery vs classification, the flag anchor's
# command word, the export prefix's question, a comment swallowing a live command, and the
# `for … in` probe reading the wrong line. Each was fixed behaviourally, and behavioural cases
# only ever cover the shapes someone already thought of, which is precisely why there were five.
#
# The rule is one sentence: the JOINED text is the representation for classification, comparison
# and command-word extraction; the physical text may be used ONLY for diagnostics and line
# numbers. Enforced by requiring every use of the physical-line variable inside `census` to carry
# a `physical-ok:` marker naming why — so adding a classification that reads physical text fails
# HERE, at the shape nobody thought of, rather than in a review round.
#
# Deliberately grep-shaped: the property is "which variable is read where", which is visible in
# the source and needs no parse. A marker someone must write is also a decision someone must make.
struct_out="$(python3 - "$REPO_ROOT/scripts/tests/ws0_embedded_python.py" <<'PY'
import pathlib, re, sys
src = pathlib.Path(sys.argv[1]).read_text().split("\n")
try:
    start = next(i for i, l in enumerate(src) if l.startswith("def census("))
except StopIteration:
    print("NO-SUBJECT: census() not found, so this assert has nothing to enforce")
    raise SystemExit(0)
end = next((i for i in range(start + 1, len(src)) if src[i].startswith("def ")), len(src))
unmarked = [
    f"{i + 1}: {src[i].strip()[:70]}"
    for i in range(start, end)
    if re.search(r"\bphysical_line\b", src[i]) and "physical-ok:" not in src[i]
]
# NON-VACUITY: the variable must EXIST, or an empty result would mean "renamed" rather than
# "clean" — the guard would then pass by having no subject.
present = sum(1 for i in range(start, end) if re.search(r"\bphysical_line\b", src[i]))
print(f"present={present}")
for u in unmarked:
    print(f"UNMARKED {u}")
PY
)"
struct_present="$(sed -n 's/^present=//p' <<<"$struct_out")"
if [ "${struct_present:-0}" -ge 1 ] && ! grep -q '^UNMARKED' <<<"$struct_out" \
   && ! grep -q '^NO-SUBJECT' <<<"$struct_out"; then
  pass "STRUCTURAL (joined-vs-physical): all $struct_present use(s) of the physical line inside census() carry a physical-ok: marker — a classification added against physical text fails here rather than in a sixth review round"
else
  fail "STRUCTURAL (joined-vs-physical): $(grep -c '^UNMARKED' <<<"$struct_out") unmarked physical-text use(s) in census(), present=${struct_present:-0} — classification must read the JOINED line; mark a diagnostic-only use with physical-ok: $(grep '^UNMARKED' <<<"$struct_out" | head -2 | tr '\n' ' ')$(grep '^NO-SUBJECT' <<<"$struct_out")"
fi

# --- CONTROL 1k: the SKIP PATH IS AN ALLOWLIST, so an unanticipated word cannot slip through ---
# Every silent absence this issue produced came through one door: "the command word is a literal
# not containing python, therefore skip", which decides safety by what a word is NOT. That let
# `if`, `-I`, `#`, `env` and `command` through — five spellings, five separate rounds. Skipping
# now requires MEMBERSHIP of `_NON_PYTHON_DASH_C_COMMANDS`.
#
# BOTH DIRECTIONS, because an allowlist has the widest false-red surface of anything here: the
# three previously-invisible forms must be findings, and every allowlisted command must still be
# clean — including the driver's own `grep -c`, which is the one this suite would break first.
python3 - "$TMP" <<'INJECT'
import pathlib, sys
q = chr(39)
tmp = pathlib.Path(sys.argv[1])
bad = q + "import os," + q
(tmp / "skip-env.sh").write_text('env "$PY" -c ' + bad + "\n")
(tmp / "skip-command.sh").write_text('command "$PY" -c ' + bad + "\n")
(tmp / "skip-midcomment.sh").write_text("x=1 # comment " + chr(92) + "\n$PY -c " + bad + "\n")
(tmp / "skip-grep.sh").write_text("grep -c " + q + "foo" + q + " file\n")
(tmp / "skip-sort.sh").write_text("sort -cu file\n")
(tmp / "skip-tar.sh").write_text("tar -cf out.tar dir\n")
INJECT
skip_rc=$?
skip_ok=1
skip_detail=""
for skip_case in env command midcomment; do
  [ -n "$(findings_of "$(census "$TMP/skip-$skip_case.sh")")" ] \
    || { skip_ok=0; skip_detail="$skip_detail $skip_case(missed)"; }
done
for skip_case in grep sort tar; do
  [ -z "$(findings_of "$(census "$TMP/skip-$skip_case.sh")")" ] \
    || { skip_ok=0; skip_detail="$skip_detail $skip_case(false-red)"; }
done
if [ "$skip_rc" -eq 0 ] && [ "$skip_ok" -eq 1 ]; then
  pass "census CONTROL fired (allowlisted skip): env, command and a mid-line comment are FINDINGS while every allowlisted command stays clean — skipping by membership, so the next unanticipated spelling cannot grant itself a skip by lacking a signal"
else
  fail "census CONTROL did not fire (allowlisted skip): fixture-rc=$skip_rc problems:$skip_detail"
fi

# --- CONTROL 1k-bis: a MID-LINE comment ends at its newline (the DISCRIMINATING shape) ----------
# Round 6 recognised a comment only at the first non-blank of a line, and round 7's control for it
# COULD NOT TELL: `x=1 # comment \` produced a finding because `#` is not on the command
# allowlist, not because the comment was respected — the joiner had still merged the lines. The
# check passed and the property never held.
#
# So the case that discriminates puts an ALLOWLISTED command before the comment. With comments
# handled, `grep foo file # note \` + `$PY -c 'bad'` is a finding because the joiner refuses to
# cross the comment and `$PY` is then the command word; without, resolution reads the allowlisted
# `grep` and skips a live invocation. The `x=1` form is kept for coverage but proves less.
#
# A control has to distinguish the claimed mechanism from every other reason the same result
# could occur. That is this issue's own lesson, and it was my own test that failed it.
python3 - "$TMP" <<'INJECT'
import pathlib, sys
q, bs = chr(39), chr(92)
tmp = pathlib.Path(sys.argv[1])
bad = q + "import os," + q
(tmp / "midcomment-allowlisted.sh").write_text(
    "grep foo file # note " + bs + "\n$PY -c " + bad + "\n")
(tmp / "midcomment-assignment.sh").write_text("x=1 # comment " + bs + "\n$PY -c " + bad + "\n")
(tmp / "midcomment-nocont.sh").write_text("grep foo file # note\n")
INJECT
midc_rc=$?
midc_ok=1
midc_detail=""
for midc_case in allowlisted assignment; do
  [ -n "$(findings_of "$(census "$TMP/midcomment-$midc_case.sh")")" ] \
    || { midc_ok=0; midc_detail="$midc_detail $midc_case(missed)"; }
done
[ -z "$(findings_of "$(census "$TMP/midcomment-nocont.sh")")" ] \
  || { midc_ok=0; midc_detail="$midc_detail nocont(false-red)"; }
if [ "$midc_rc" -eq 0 ] && [ "$midc_ok" -eq 1 ]; then
  pass "census CONTROL fired (mid-line comment, DISCRIMINATING): with an ALLOWLISTED command before the comment, a continuation across it no longer hides the following invocation — the shape that separates 'comments are respected' from 'the command word happened not to be allowlisted', while the same comment WITHOUT a continuation stays clean"
else
  fail "census CONTROL did not fire (mid-line comment): fixture-rc=$midc_rc problems:$midc_detail"
fi

# --- CONTROL 1m: the `-c` flag, in every quoting spelling bash glues together ------------------
# Bash concatenates the fragments of `-"c"` and `-'c'` into `-c`, and an anchor matching only the
# bare spelling missed both — measured at findings=0, silent absences.
#
# ALL SIX SPELLINGS ARE ASSERTED, not just the two that were broken, and that matters: the fully
# quoted `"-c"` and `'-c'` already failed closed by a DIFFERENT path, so a control testing only
# those would have reported this fixed while the concatenated forms stayed blind. Testing the
# space rather than the named example is what separated them.
python3 - "$TMP" <<'INJECT'
import pathlib, sys
q, dq, bs = chr(39), chr(34), chr(92)
tmp = pathlib.Path(sys.argv[1])
bad = q + "import os," + q
for name, flag in (
    ("bare", "-c"), ("quoted-dq", dq + "-c" + dq), ("quoted-sq", q + "-c" + q),
    ("concat-dq", "-" + dq + "c" + dq), ("concat-sq", "-" + q + "c" + q),
    ("escaped", bs + "-c"),
):
    (tmp / f"flag-{name}.sh").write_text("$PY " + flag + " " + bad + "\n")
INJECT
flag_rc=$?
flag_ok=1
flag_detail=""
for flag_case in bare quoted-dq quoted-sq concat-dq concat-sq escaped; do
  [ -n "$(findings_of "$(census "$TMP/flag-$flag_case.sh")")" ] \
    || { flag_ok=0; flag_detail="$flag_detail $flag_case(missed)"; }
done
if [ "$flag_rc" -eq 0 ] && [ "$flag_ok" -eq 1 ]; then
  pass "census CONTROL fired (flag spellings): all six quoting spellings of -c that bash glues into the same flag are FINDINGS — including the two concatenated forms that were silent while the fully-quoted ones already fired, which is why the whole space is asserted rather than the named example"
else
  fail "census CONTROL did not fire (flag spellings): fixture-rc=$flag_rc problems:$flag_detail"
fi

# --- CONTROL 1l: the assignment prefix is PARSED, and refuses what an eval would have run -------
# Round 2 authorised evaluating the driver's prefix on the argument that the contiguity check
# bounded the input to `NAME=` words with no separator. MEASURED, that check ADMITS
# `WS0_CFG_X=$(helper)`, backticks and `${OTHER}` — all assignment-shaped, none containing a
# separator — so repository-derived shell would have run inside a test documented as hermetic,
# in a mandatory gate component. There is no eval now; a restricted grammar refuses each form BY
# NAME, and this control is what keeps that true.
grammar_ok=1
grammar_detail=""
for grammar_case in 'subst=$(helper)' 'backtick=`helper`' 'unknown=${OTHER}'; do
  grammar_label="${grammar_case%%=*}"
  grammar_value="${grammar_case#*=}"
  GRAMMAR_DRV="$TMP/grammar-$grammar_label-driver.sh"
  python3 - "$DRIVER" "$GRAMMAR_DRV" "$grammar_value" <<'INJECT'
import pathlib, sys
text = pathlib.Path(sys.argv[1]).read_text()
needle = 'WS0_CFG_REPS="$REPS"'
if text.count(needle) != 1:
    print(f"INJECTION IMPOSSIBLE: {needle} occurs {text.count(needle)} time(s)", file=sys.stderr)
    raise SystemExit(1)
pathlib.Path(sys.argv[2]).write_text(text.replace(needle, "WS0_CFG_REPS=" + sys.argv[3]))
INJECT
  if [ $? -ne 0 ]; then
    grammar_ok=0; grammar_detail="$grammar_detail $grammar_label(not-injected)"; continue
  fi
  if driver_step_env "$GRAMMAR_DRV" write_session_corpus_pin >/dev/null 2>&1; then
    grammar_ok=0; grammar_detail="$grammar_detail $grammar_label(ADMITTED)"
  fi
done
if [ "$grammar_ok" -eq 1 ]; then
  pass "grammar CONTROL fired: command substitution, backticks and an unknown parameter reference are each REFUSED by the prefix parser — the constructs the round-2 eval would have executed, and which its stated safety bound did not exclude"
else
  fail "grammar CONTROL did not fire:$grammar_detail — an unsupported assignment form was accepted, which is the hermeticity breach this replaced"
fi

# ============================================================================
# PART 2 — THE TOTAL PROPERTY: EVERY EMBEDDED BLOCK COMPILES
# ============================================================================
# Instance #8 anywhere in that file is caught here, not just the two steps #3451 repaired. This is
# the check `bash -n` structurally cannot make: to bash the body is one opaque string.
comp_out="$(compile_blocks "$DRIVER")"
if grep -q '^#COMPLETE compiled=' <<<"$comp_out" && [ -z "$(findings_of "$comp_out")" ]; then
  pass "compile-all: EVERY embedded python block in the driver compiles ($(grep '^#COMPLETE ' <<<"$comp_out"))"
else
  fail "compile-all: an embedded python block in $DRIVER does not compile — $(findings_of "$comp_out" | head -3)"
fi

# --- POSITIVE CONTROL 2: the compile check FIRES on the injected defect, per block --------------
# Both subject blocks are injected SEPARATELY, because a check that only ever saw block 1 would be
# indistinguishable from one that reports the whole file at once.
control_compile() { # control_compile <label> <placeholder> <mapping> <key>
  local label="$1" dest="$TMP/defect-$2.sh" out
  if ! defective_copy "$DRIVER" "$dest" "$2" "$3" "$4" 2>"$TMP/inject.err"; then
    fail "compile CONTROL ($label): the defect could not be injected, so the control could not fire — $(head -2 "$TMP/inject.err")"
    return
  fi
  out="$(compile_blocks "$dest")"
  # Asserts the FAILURE and its CLASS, never CPython's phrasing: the wording of that diagnostic is
  # interpreter-specific, so grepping it would red on the 3.9-3.11 this repository pins. Both
  # tokens are produced by the checker itself (`DOES NOT COMPILE`, and the exception class name).
  if grep -q 'DOES NOT COMPILE' <<<"$out" && grep -q 'SyntaxError' <<<"$out"; then
    pass "compile CONTROL fired ($label): $(grep 'DOES NOT COMPILE' <<<"$out" | head -1 | cut -c1-120)"
  else
    fail "compile CONTROL did NOT fire ($label): the injected defect must be reported, got: $(findings_of "$out" | head -2)"
  fi
}
read -r cb_name cb_map cb_key <<<"$(local_binding_of "$DRIVER" write_session_corpus_pin)"
control_compile "session-corpus-pin step" "$cb_name" "$cb_map" "$cb_key"
read -r pb_name pb_map pb_key <<<"$(local_binding_of "$DRIVER" pinning_record_path)"
control_compile "CPU-pin-verification step" "$pb_name" "$pb_map" "$pb_key"

# ============================================================================
# PART 3 — THE SESSION-CORPUS-PIN STEP **RUNS**
# ============================================================================
# The fixture corpus is the shipped `ws0_make_corpus` (a real `Data.db` of a few KB plus every
# auxiliary component, with MEASURED digests) — never a hand-rolled identity, for the reason that
# library states: a fixture that composed the shape itself would keep passing after the shipped
# writer changed.
CORPUS="$TMP/corpus"
ws0_make_corpus "$CORPUS" 100 2560
OUT="$TMP/session"
mkdir -p "$OUT"

# The Flight ticket must exist BEFORE the pin — `write_session_corpus_pin` measures its digest and
# an absent template is `Invalid` there. That ordering is the driver's, and it is reproduced here
# through the SHIPPED writer rather than by writing the JSON.
python3 - "$PERF_DIR" "$OUT" "$CORPUS" <<'PY'
import pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_ticket_input import write_ticket_template
write_ticket_template(pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3]) / "ws0-events.cql")
PY
ticket_rc=$?
if [ "$ticket_rc" -eq 0 ]; then
  pass "fixture: a tiny corpus and the session's Flight ticket were written by the SHIPPED writers, in the driver's own order"
else
  fail "fixture: the shipped ticket writer failed (rc=$ticket_rc); the execute cases below would fail on the fixture rather than on their subject"
fi

# THE CONFIGURATION THE STEP READS, keyed by the SHIPPED field list. The names are
# `WS0_CFG_<FIELD-UPPERCASED>`, derived by the block itself from
# `ws0_session.MANIFEST_CONFIG_FIELDS`, so this suite must not carry a guessed list: the key set
# below is asserted EQUAL to the shipped tuple, and a field added there without a value here is a
# failure rather than a step refusing at run time for a reason nobody expected.
# An INDEXED array of `name=value`, not `declare -A`: associative arrays are bash 4.0+, and macOS
# ships /bin/bash 3.2, which this repository treats as a supported target (see
# `test_agent_gate_tree_portability.sh` and the `BASH_VERSINFO` probe in
# `test_agent_gate_delta.sh`). This file was the only ws0 suite using one, so it would have failed
# `tooling-tests` there. ONE source of truth still — the names are derived from the pairs by
# `cfg_names`, so a value and its key cannot drift apart.
CFG_PAIRS=(
  "reps=1" "temps=warm" "arms=bypass" "scan_passes=1"
  "server_cpus=2,10" "client_cpus=4,12" "step_duration=45s/1s"
  "flight_endpoint=$WS0_FIXTURE_ENDPOINT"
  # ...and #3551's flight pin. EQUAL to `server_cpus`, because that is THE DRIVER'S OWN DEFAULT
  # SHAPE (`--flight-server-cpus` defaults to `--server-cpus`) — the same rule the comment below
  # states for events/profile/quiescence: a fixture pinning a value the driver never produces
  # makes the round trip approve an artifact production would refuse.
  "flight_server_cpus=2,10"
  # ...and the fields #3455 added. Each value is THE DRIVER'S OWN DEFAULT SHAPE, not merely
  # something the validator accepts — the round-8 lesson about the Flight endpoint applied here
  # before it could cost a round: a fixture pinning a value the driver never produces makes the
  # round trip approve an artifact production would refuse. `events` is the driver's counter list,
  # `profile`/`quiescence` are its own closed-grammar defaults (`off`, and the unverified
  # sentinel), and `bin_dir` is a release bin path.
  "events=cycles,instructions"
  "bin_dir=target/release"
  "profile=off"
  "quiescence=NOT VERIFIED (no timeseries supplied)"
  # `non-baseline`, and that is the only honest value available here: this is a few-KB synthetic
  # corpus, and the shipped `require_canonical_or_declared` REFUSES a divergent corpus in
  # `baseline` mode — correctly. Declaring the mode is the supported way past it, not a way round
  # it: the step still runs the real comparison and records every divergence it finds.
  "baseline_mode=non-baseline"
)
cfg_names() { local pair; for pair in "${CFG_PAIRS[@]}"; do printf '%s\n' "${pair%%=*}"; done; }
cfg_value() {
  local pair
  for pair in "${CFG_PAIRS[@]}"; do
    [ "${pair%%=*}" = "$1" ] && { printf '%s' "${pair#*=}"; return 0; }
  done
  return 1
}
cfg_keys="$(cfg_names | sort | tr '\n' ' ')"
shipped_keys="$(python3 - "$PERF_DIR" <<'PY'
import sys
sys.path.insert(0, sys.argv[1])
from ws0_session import MANIFEST_CONFIG_FIELDS
print(" ".join(sorted(MANIFEST_CONFIG_FIELDS)) + " ")
PY
)"
if [ "$cfg_keys" = "$shipped_keys" ]; then
  pass "config-fields: this suite supplies EXACTLY ws0_session.MANIFEST_CONFIG_FIELDS ($(echo "$shipped_keys" | wc -w) fields), so the environment it builds is the shipped list rather than a guess"
else
  fail "config-fields: the suite's WS0_CFG_* keys [$cfg_keys] differ from the shipped MANIFEST_CONFIG_FIELDS [$shipped_keys]"
fi

# ...and the THIRD set, which is the one that actually ships: the names THE DRIVER EXPORTS.
#
# The two checks above compare this SUITE's environment against the shipped field list. Neither
# says anything about the driver's bash plumbing, because the execute cases build their own
# environment — so a renamed export was invisible. MEASURED attack: `WS0_CFG_TEMPS=` ->
# `WS0_CFG_TEMSP=` in the driver leaves the embedded python untouched, so the census and the
# compile check stay clean (`#COMPLETE compiled=3 findings=0`) and the blocks still execute
# perfectly under the environment this suite builds — while the real rig hits the step's own
# `FATAL: WS0_CFG_TEMPS was not exported`, the `|| exit 2` fires, and the driver is unrunnable end
# to end. That is #3451's exact symptom, discovered by whoever next tries to run it.
#
# THREE SETS MUST AGREE, and the two sides of each comparison come from GENUINELY DIFFERENT
# SOURCES — the driver's BASH SOURCE on one side, the python import on the other. A set-equality
# check whose halves came from one place would agree by construction and prove nothing.
driver_cfg_keys="$(grep -oE '^WS0_CFG_[A-Z_]+=' "$DRIVER" | sed 's/^WS0_CFG_//; s/=$//' \
  | tr '[:upper:]' '[:lower:]' | sort | tr '\n' ' ')"
if [ "$driver_cfg_keys" = "$shipped_keys" ]; then
  pass "config-exports: the DRIVER exports exactly the WS0_CFG_* names the shipped MANIFEST_CONFIG_FIELDS requires — a renamed export is caught statically, not by an operator discovering the rig is unrunnable"
else
  fail "config-exports: the driver exports [$driver_cfg_keys] but the step reads MANIFEST_CONFIG_FIELDS [$shipped_keys]. The step refuses at run time on the difference and its caller exits 2, so the rig is unrunnable end to end"
fi

# --- CONTROL: the renamed export is OBSERVED to fire ------------------------------------------
RENAMED_EXPORT="$TMP/renamed-export-ws0-driver.sh"
inject_rc=0
python3 - "$DRIVER" "$RENAMED_EXPORT" 2>"$TMP/inject-renamed.err" <<'INJECT'
import pathlib, sys
text = pathlib.Path(sys.argv[1]).read_text()
needle = "WS0_CFG_TEMPS="
if text.count(needle) != 1:
    print(f"INJECTION IMPOSSIBLE: {needle} occurs {text.count(needle)} time(s)", file=sys.stderr)
    raise SystemExit(1)
pathlib.Path(sys.argv[2]).write_text(text.replace(needle, "WS0_CFG_TEMSP="))
INJECT
inject_rc=$?
renamed_keys="$(grep -oE '^WS0_CFG_[A-Z_]+=' "$RENAMED_EXPORT" | sed 's/^WS0_CFG_//; s/=$//' \
  | tr '[:upper:]' '[:lower:]' | sort | tr '\n' ' ')"
renamed_compile="$(compile_blocks "$RENAMED_EXPORT")"
if [ "$inject_rc" -ne 0 ]; then
  fail "config-exports CONTROL: the rename could not be injected, so the control could not fire — $(head -2 "$TMP/inject-renamed.err")"
elif [ "$renamed_keys" != "$shipped_keys" ] && [ -z "$(findings_of "$renamed_compile")" ]; then
  pass "config-exports CONTROL fired: a single renamed export is caught by the set comparison — and the compile check is SILENT about it (the python is untouched), which is why this check exists separately"
else
  fail "config-exports CONTROL did not fire: renamed=[$renamed_keys] shipped=[$shipped_keys], compile said '$(findings_of "$renamed_compile" | head -1)'"
fi

# ...and the assignments must be part of the step's ENVIRONMENT-ASSIGNMENT PREFIX, not merely
# present somewhere in the file (#3451 review round 10, finding 1). The set comparisons above
# collect names GLOBALLY, so a single removed continuation backslash turns an export into a
# standalone assignment python never sees — the driver dies, and every check here still passes.
# Each prefix against ITS OWN consuming block, named by the shipped writer that block calls.
for export_pair in "WS0_CFG_:write_session_corpus_pin" "WS0_PIN_:pinning_record_path"; do
  export_prefix="${export_pair%%:*}"
  export_needle="${export_pair#*:}"
  if ! read -r in_line total_in_file \
       <<<"$(export_prefix_membership "$DRIVER" "$export_prefix" "$export_needle")"; then
    fail "export-prefix ($export_prefix): the membership could not be computed, so the check did not run"
    continue
  fi
  if [ -n "${in_line:-}" ] && [ "${total_in_file:-0}" -gt 0 ] && [ "$in_line" -eq "$total_in_file" ]; then
    pass "export-prefix ($export_prefix): all $total_in_file assignment(s) are in the CONTIGUOUS ENVIRONMENT-ASSIGNMENT PREFIX of the block that reads them (located by $export_needle) — genuinely exported to that step, not merely present in the file, nor on its logical line behind a separator, nor exported to a different step"
  else
    fail "export-prefix ($export_prefix): only ${in_line:-?} of ${total_in_file:-?} assignment(s) are in the contiguous assignment prefix of the block calling $export_needle. One outside that prefix is never exported TO IT: the step refuses at run time on the missing variable and its caller exits 2"
  fi
done

# --- CONTROL: a removed continuation, and a relocated assignment, both FIRE --------------------
python3 - "$DRIVER" "$TMP" <<'INJECT'
import pathlib, sys
text = pathlib.Path(sys.argv[1]).read_text()
tmp = pathlib.Path(sys.argv[2])
bs = chr(92)
# (a) drop ONE continuation backslash, so that assignment leaves the prefix.
needle = 'WS0_CFG_TEMPS="$TEMPS" ' + bs + "\n"
if text.count(needle) != 1:
    print(f"INJECTION IMPOSSIBLE: continuation needle occurs {text.count(needle)} time(s)",
          file=sys.stderr)
    raise SystemExit(1)
(tmp / "export-nobackslash.sh").write_text(
    text.replace(needle, 'WS0_CFG_TEMPS="$TEMPS"\n'))
# (b) MOVE an assignment out of the prefix entirely, keeping it in the file.
(tmp / "export-relocated.sh").write_text(
    text.replace(needle, "").replace(
        "#!/usr/bin/env bash", '#!/usr/bin/env bash\nWS0_CFG_TEMPS="$TEMPS"', 1))
# (c-pre) BREAK THE PREFIX WITH A `;`, leaving the assignment on the SAME logical line. This is
# the case a membership test structurally cannot see: the name is still present, still on the
# invocation's own logical line, and python no longer receives it.
semi = 'WS0_CFG_BASELINE_MODE="$BASELINE_MODE" ' + bs + "\n"
if text.count(semi) != 1:
    print(f"INJECTION IMPOSSIBLE: semicolon needle occurs {text.count(semi)} time(s)",
          file=sys.stderr)
    raise SystemExit(1)
(tmp / "export-semicolon.sh").write_text(
    text.replace(semi, 'WS0_CFG_BASELINE_MODE="$BASELINE_MODE"; ' + bs + "\n"))
# (c) MOVE it into the OTHER step's prefix. This is the round-11 case: the name is still in the
# file AND still in a `python3 -c` logical line, so a check that unioned across invocations saw
# nothing wrong while the session-pin step died on the absent variable.
pin_prefix = 'WS0_PIN_SERVER_CPUS="$SERVER_CPUS" ' + bs + "\n"
if text.count(pin_prefix) != 1:
    print(f"INJECTION IMPOSSIBLE: pin-prefix needle occurs {text.count(pin_prefix)} time(s)",
          file=sys.stderr)
    raise SystemExit(1)
(tmp / "export-otherblock.sh").write_text(
    text.replace(needle, "").replace(pin_prefix, needle + pin_prefix))
INJECT
export_inject_rc=$?
if [ "$export_inject_rc" -ne 0 ]; then
  fail "export-prefix CONTROL: the injections could not be made, so the controls could not fire"
else
  export_ctl_ok=1
  export_ctl_detail=""
  for export_case in nobackslash relocated otherblock semicolon; do
    read -r c_in c_total \
      <<<"$(export_prefix_membership "$TMP/export-$export_case.sh" WS0_CFG_ write_session_corpus_pin)"
    if [ "$c_in" -eq "$c_total" ]; then
      export_ctl_ok=0
      export_ctl_detail="$export_ctl_detail $export_case($c_in/$c_total)"
    fi
  done
  if [ "$export_ctl_ok" -eq 1 ]; then
    pass "export-prefix CONTROL fired on all FOUR ways an export stops being one: a removed continuation, a relocation out of every prefix, a relocation into the OTHER step's prefix, and a semicolon that leaves the assignment on the SAME logical line while python stops receiving it"
  else
    fail "export-prefix CONTROL did not fire:$export_ctl_detail — the membership check cannot see an assignment leaving the prefix"
  fi
fi

# ...and the SAME class for the CPU-pin step, whose four inputs have no shipped field list. Here
# the two sources are the driver's BASH exports and the LITERAL names the extracted BLOCK reads,
# which is again two independent derivations of one fact.
driver_pin_keys="$(grep -oE '^WS0_PIN_[A-Z_]+=' "$DRIVER" | sed 's/=$//' | sort -u | tr '\n' ' ')"
block_pin_keys="$(emit_block "$DRIVER" "$CPU_BLOCK" | grep -oE 'WS0_PIN_[A-Z_]+' | sort -u | tr '\n' ' ')"
if [ -n "$block_pin_keys" ] && [ "$driver_pin_keys" = "$block_pin_keys" ]; then
  pass "pin-exports: the DRIVER exports exactly the WS0_PIN_* names the CPU-pin step reads ($(echo "$block_pin_keys" | wc -w) of them) — the step reads them with os.environ[...], which raises KeyError on a rename and exits 2"
else
  fail "pin-exports: the driver exports [$driver_pin_keys] and the CPU-pin step reads [$block_pin_keys]"
fi

RENAMED_PIN="$TMP/renamed-pin-ws0-driver.sh"
inject_rc=0
python3 - "$DRIVER" "$RENAMED_PIN" 2>"$TMP/inject-renamed_pin.err" <<'INJECT'
import pathlib, sys
text = pathlib.Path(sys.argv[1]).read_text()
needle = "WS0_PIN_SIBLINGS="
if text.count(needle) != 1:
    print(f"INJECTION IMPOSSIBLE: {needle} occurs {text.count(needle)} time(s)", file=sys.stderr)
    raise SystemExit(1)
pathlib.Path(sys.argv[2]).write_text(text.replace(needle, "WS0_PIN_SIBLING="))
INJECT
inject_pin_rc=$?
renamed_pin_keys="$(grep -oE '^WS0_PIN_[A-Z_]+=' "$RENAMED_PIN" | sed 's/=$//' | sort -u | tr '\n' ' ')"
if [ "$inject_pin_rc" -ne 0 ]; then
  fail "pin-exports CONTROL: the rename could not be injected, so the control could not fire — $(head -2 "$TMP/inject-renamed_pin.err")"
elif [ "$renamed_pin_keys" != "$block_pin_keys" ]; then
  pass "pin-exports CONTROL fired: a renamed WS0_PIN_* export no longer matches the names the step reads"
else
  fail "pin-exports CONTROL did not fire: renamed=[$renamed_pin_keys] block=[$block_pin_keys]"
fi

# run_pin_step <driver> <out-dir> [omit-field] — execute the corpus-pin block extracted from
# <driver>, with the argv and environment the driver gives it.
#
# The step's combined output goes to a FILE (`$STEP_OUT`) and its status to `run_pin_rc`, and
# NEITHER is read through a command substitution — `run_pin_rc=$(...)` would run the whole
# function in a SUBSHELL, where the assignment dies and the caller reads a stale 0. That is the
# same class as the assert-counter-in-a-subshell defect this tree has already shipped once, and it
# was MEASURED here: the first draft of this suite reported rc=0 for a step whose python had
# refused to parse.
run_pin_step() {
  local drv="$1" out="$2" omit="${3:-}" idx field body
  # `-u` OPTIONS FIRST: `env` stops reading options at the first NAME=VALUE, so a `-u` appended
  # after the assignments is taken as a command to execute (measured: `env: -u: No such file or
  # directory`, rc 127) — a control that dies before reaching its subject.
  local -a unset_args=() env_args=()
  # THE VALUES COME FROM THE DRIVER, not from this suite (#3451 post-rebase round 2, F3). Every
  # other check validates NAMES; the executions used to run on an environment this file built, so
  # a production MAPPING error (`WS0_CFG_TEMPS="$ARMS"`) left everything green while the driver
  # recorded a configuration it never measured. `driver_step_env` evaluates the driver's OWN
  # validated assignment prefix with controlled inputs — see its comment for why evaluating that
  # text is bounded, and for why a swap is caught by the SHIPPED validators rather than by an
  # expectation written here.
  local line
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    env_args+=("$line")
  done < <(driver_step_env "$drv" write_session_corpus_pin)
  if [ "${#env_args[@]}" -eq 0 ]; then
    run_pin_rc=91
    echo "driver_step_env produced no environment for this driver" > "$STEP_OUT"
    return
  fi
  for field in $(cfg_names); do
    if [ "$field" = "$omit" ]; then
      # UNSET, not merely "not passed" (#3451 review round 1, finding 3). `env` INHERITS the
      # caller environment, so omitting the assignment leaves a value the operator happened to
      # have exported — and the control then measures nothing while reporting a failure. `-u`
      # rather than an empty value: the step treats an empty string as absent too, but "was not
      # exported" is the condition being tested and `-u` is the only spelling that states it.
      # UNSET for the child, and also REMOVED from the driver-derived list — `env -u NAME`
      # after `NAME=value` on the same command line does not win, so both halves are needed.
      local omit_var="WS0_CFG_$(echo "$field" | tr '[:lower:]' '[:upper:]')"
      unset_args+=("-u" "$omit_var")
      local -a kept=()
      local e
      for e in "${env_args[@]}"; do
        case "$e" in "$omit_var="*) ;; *) kept+=("$e") ;; esac
      done
      env_args=(${kept[@]+"${kept[@]}"})
    fi
  done
  idx="$(find_block "$drv" 'write_session_corpus_pin')"
  [[ "$idx" =~ ^[0-9]+$ ]] || { run_pin_rc=90; echo "block not located: $idx" > "$STEP_OUT"; return; }
  body="$(emit_block "$drv" "$idx")"
  # `${arr[@]+"${arr[@]}"}` — an EMPTY array expands to NOTHING under `set -u` instead of
  # aborting the shell, which is the repo-wide idiom (`test_fetch_datasets_tracked_guard.sh`).
  # `unset_args` is empty on every call that omits nothing.
  env ${unset_args[@]+"${unset_args[@]}"} "${env_args[@]}" \
    python3 -c "$body" "$PERF_DIR" "$CORPUS" "$out" \
    "$REPO_ROOT" > "$STEP_OUT" 2>&1
  run_pin_rc=$?
}

run_pin_rc=0
run_pin_step "$DRIVER" "$OUT"; pin_out="$(cat "$STEP_OUT")"
pin_file="$(python3 - "$PERF_DIR" "$OUT" <<'PY'
import pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_session import session_pin_path
print(session_pin_path(pathlib.Path(sys.argv[2])))
PY
)"
if [ "$run_pin_rc" -eq 0 ] && [ -s "$pin_file" ]; then
  pass "EXECUTE session-corpus-pin: the SHIPPED step ran to completion and wrote $(basename "$pin_file")"
else
  fail "EXECUTE session-corpus-pin: the shipped step did not run (rc=$run_pin_rc): $(head -4 <<<"$pin_out")"
fi
# Its three pin LINES, each asserted by name: the step's whole output contract, and the thing an
# operator reads to know which corpus and which configuration this session is about.
for line_tag in 'corpus pin:' 'config pin:' 'canonical pin:'; do
  if grep -q "$line_tag" <<<"$pin_out"; then
    pass "EXECUTE session-corpus-pin: printed '$line_tag' — $(grep -m1 "$line_tag" <<<"$pin_out" | cut -c1-96)"
  else
    fail "EXECUTE session-corpus-pin: the step did not print '$line_tag' (output: $(head -3 <<<"$pin_out"))"
  fi
done

# ...and the SHIPPED READER accepts what the SHIPPED STEP wrote. A writer/reader round trip is the
# one thing no fixture-fed reject case can establish, and it is where a disagreement surfaces as a
# refusal blaming the operator.
if python3 - "$PERF_DIR" "$OUT" "$CORPUS" "${WS0_EXPECTED_CFG[@]}" <<'PY'
import pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_report import ARMS_ALLOWED, TEMPS_ALLOWED
from ws0_session import session_manifest_config, verify_session_corpus_pin
from ws0_validate import load_corpus_identity
session, corpus = pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3])
report = verify_session_corpus_pin(session, corpus, load_corpus_identity(corpus))
# ...AND the shipped CONFIGURATION reader, which `verify_session_corpus_pin` does not call
# (#3451 review round 8, finding 2). Without it "the SHIPPED reader accepts what the SHIPPED step
# wrote" was verified by a WEAKER reader than the one that runs in production: the pin verifier
# checks corpus identity and says nothing about the configuration, so a manifest the real reporter
# would refuse passed here. `session_manifest_config` is what `ws0_report.py:250` calls.
cfg = session_manifest_config(session, TEMPS_ALLOWED, ARMS_ALLOWED)
# EVERY field against its expected value, independently. A swap between two fields the same
# validator accepts is invisible to the validator and visible only here.
# argv: 1=perf dir, 2=session, 3=corpus, 4.. = the expected `field=value` pairs.
expected = dict(pair.split("=", 1) for pair in sys.argv[4:])
if not expected:
    print("no expected fields were passed; this check would assert nothing",
          file=sys.stderr)
    raise SystemExit(1)
# The reader NORMALISES the selection fields into lists (`temps` -> ['warm'], `events` ->
# ['cycles','instructions']), so the comparison joins a list back to the comma form the driver
# was handed. That is a comparison SHAPE, not a weakening: a swapped right-hand side still
# yields a different joined string.
def _norm(value):
    if isinstance(value, (list, tuple)):
        return ",".join(str(item) for item in value)
    return str(value)

mismatched = {
    k: (cfg.get(k), v) for k, v in expected.items() if _norm(cfg.get(k)) != v
}
if mismatched:
    print(f"manifest fields differ from the controlled inputs: {mismatched}",
          file=sys.stderr)
    raise SystemExit(1)
# The reader's own report, asserted field by field so this case cannot pass on a verifier that
# returned an empty dict: the pin was taken BEFORE measurement, and it carries the digests of the
# corpus, the schema and the Flight ticket the step measured from disk.
for _label, _ok in (
    ("pinned_before_measurement is True", report.get("pinned_before_measurement") is True),
    ("pinned_data_db_sha256 is a digest", len(report.get("pinned_data_db_sha256", "")) == 64),
    ("pinned_schema_sha256 is a digest", len(report.get("pinned_schema_sha256", "")) == 64),
    ("pinned_ticket_sha256 is a digest", len(report.get("pinned_ticket_sha256", "")) == 64),
    ("pinned_components >= 5", report.get("pinned_components", 0) >= 5),
):
    if not _ok:
        print(f"pin report failed: {_label} — {report}", file=sys.stderr)
        raise SystemExit(1)
PY
then
  pass "EXECUTE session-corpus-pin: the SHIPPED READERS accept what the shipped STEP wrote — verify_session_corpus_pin for the corpus identity AND session_manifest_config (what ws0_report.py itself calls) for the configuration, so the round trip is against the production reader rather than a weaker one"
else
  fail "EXECUTE session-corpus-pin: a shipped reader refused what the shipped step wrote"
fi

# --- CONTROL 3a-pre: the configuration reader DISCRIMINATES, it does not accept everything ------
# The accept above is only evidence if the same reader can refuse. A `grpc://` endpoint — the
# spelling this suite used before round 8, and one the driver never produces — must be REJECTED by
# `session_manifest_config`. Note the STEP accepts it (it records configuration verbatim); the
# refusal comes from the production reader, which is exactly the gap that made the old round trip
# weaker than it read.
run_pin_rc=0
OUT_BADCFG="$TMP/session-badcfg"; mkdir -p "$OUT_BADCFG"
python3 - "$PERF_DIR" "$OUT_BADCFG" "$CORPUS" <<'PY'
import pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_ticket_input import write_ticket_template
write_ticket_template(pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3]) / "ws0-events.cql")
PY
# The bad value is injected at THE DRIVER'S OWN INPUT (the shell variable its prefix reads),
# not into a table in this file — since F3 the execution takes its values from the driver, so
# overriding a local table would no longer reach the step at all.
WS0_FIXTURE_ENDPOINT_SAVED="$WS0_FIXTURE_ENDPOINT"
WS0_FIXTURE_ENDPOINT="grpc://127.0.0.1:1"
run_pin_step "$DRIVER" "$OUT_BADCFG"; badcfg_out="$(cat "$STEP_OUT")"
WS0_FIXTURE_ENDPOINT="$WS0_FIXTURE_ENDPOINT_SAVED"
if [ "$run_pin_rc" -eq 0 ] && python3 - "$PERF_DIR" "$OUT_BADCFG" <<'PY'
import pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_report import ARMS_ALLOWED, TEMPS_ALLOWED
from ws0_session import session_manifest_config
from ws0_validate import Invalid
try:
    session_manifest_config(pathlib.Path(sys.argv[2]), TEMPS_ALLOWED, ARMS_ALLOWED)
except Invalid:
    raise SystemExit(0)
raise SystemExit(1)
PY
then
  pass "config-reader CONTROL fired: a grpc:// endpoint is written by the step and REFUSED by session_manifest_config — so the accept above is a measurement, not a reader that takes anything"
else
  fail "config-reader CONTROL did not fire: session_manifest_config must refuse a non-http endpoint (step rc=$run_pin_rc, out: $(head -2 <<<"$badcfg_out"))"
fi

# --- CONTROL 3a-map: a SWAPPED RIGHT-HAND SIDE is caught (#3451 post-rebase round 2, F3) --------
# The mapping defect this whole change is about: `WS0_CFG_TEMPS="$ARMS"` exports a real name with
# the wrong value, so every NAME-level check stays green while the driver records a configuration
# it never measured. Detected by the SHIPPED validator rather than by an expectation here —
# `temps` and `arms` have disjoint legal sets, so the swap yields `temps=bypass` and
# `session_manifest_config` refuses it.
SWAPPED="$TMP/swapped-rhs-ws0-driver.sh"
python3 - "$DRIVER" "$SWAPPED" <<'INJECT'
import pathlib, sys
text = pathlib.Path(sys.argv[1]).read_text()
needle = 'WS0_CFG_TEMPS="$TEMPS"'
if text.count(needle) != 1:
    print(f"INJECTION IMPOSSIBLE: {needle} occurs {text.count(needle)} time(s)", file=sys.stderr)
    raise SystemExit(1)
pathlib.Path(sys.argv[2]).write_text(text.replace(needle, 'WS0_CFG_TEMPS="$ARMS"'))
INJECT
swap_rc=$?
if [ "$swap_rc" -ne 0 ]; then
  fail "mapping CONTROL: the swap could not be injected, so the control could not fire"
else
  OUT_SWAP="$TMP/session-swapped"; mkdir -p "$OUT_SWAP"
  python3 - "$PERF_DIR" "$OUT_SWAP" "$CORPUS" <<'PY'
import pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_ticket_input import write_ticket_template
write_ticket_template(pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3]) / "ws0-events.cql")
PY
  run_pin_rc=0
  run_pin_step "$SWAPPED" "$OUT_SWAP"; swap_out="$(cat "$STEP_OUT")"
  if [ "$run_pin_rc" -eq 0 ] && ! python3 - "$PERF_DIR" "$OUT_SWAP" <<'PY'
import pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_report import ARMS_ALLOWED, TEMPS_ALLOWED
from ws0_session import session_manifest_config
session_manifest_config(pathlib.Path(sys.argv[2]), TEMPS_ALLOWED, ARMS_ALLOWED)
PY
  then
    pass "mapping CONTROL fired: a swapped right-hand side (WS0_CFG_TEMPS taking \$ARMS) is written by the step and REFUSED by the shipped reader — values are now taken from the driver, so a mapping error is visible where a name check cannot see it"
  else
    fail "mapping CONTROL did not fire: a swapped right-hand side must reach the manifest and be refused (step rc=$run_pin_rc, out: $(head -2 <<<"$swap_out"))"
  fi
fi

# --- POSITIVE CONTROL 3a: the same harness OBSERVES the defective step failing -----------------
DEFECTIVE_PIN="$TMP/defect-exec-pin.sh"
if defective_copy "$DRIVER" "$DEFECTIVE_PIN" "$cb_name" "$cb_map" "$cb_key" 2>"$TMP/inject-pin.err"; then
  OUT_BAD="$TMP/session-bad"; mkdir -p "$OUT_BAD"
  python3 - "$PERF_DIR" "$OUT_BAD" "$CORPUS" <<'PY'
import pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_ticket_input import write_ticket_template
write_ticket_template(pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3]) / "ws0-events.cql")
PY
  run_pin_rc=0
  run_pin_step "$DEFECTIVE_PIN" "$OUT_BAD"; bad_out="$(cat "$STEP_OUT")"
  bad_pin="$OUT_BAD/$(basename "$pin_file")"
  if [ "$run_pin_rc" -ne 0 ] && grep -q 'SyntaxError' <<<"$bad_out" && [ ! -e "$bad_pin" ]; then
    pass "EXECUTE CONTROL fired (session-corpus-pin): the injected step FAILS to run (rc=$run_pin_rc, SyntaxError) and writes NO pin — so the accept above is a measurement"
  else
    fail "EXECUTE CONTROL did NOT fire (session-corpus-pin): rc=$run_pin_rc, pin-exists=$([ -e "$bad_pin" ] && echo yes || echo no), out: $(head -3 <<<"$bad_out")"
  fi
else
  fail "EXECUTE CONTROL (session-corpus-pin): the defect could not be injected — $(head -2 "$TMP/inject-pin.err")"
fi

# --- POSITIVE CONTROL 3b: the step's OWN fail-closed branch, on an unexported config field ------
# Not a syntax control: this one proves the environment the harness builds is genuinely READ. With
# one WS0_CFG_* absent the step must refuse and NAME the variable, which is the driver's own
# diagnostic for a field it forgot to export.
run_pin_rc=0
OUT_OMIT="$TMP/session-omit"; mkdir -p "$OUT_OMIT"
python3 - "$PERF_DIR" "$OUT_OMIT" "$CORPUS" <<'PY'
import pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_ticket_input import write_ticket_template
write_ticket_template(pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3]) / "ws0-events.cql")
PY
run_pin_step "$DRIVER" "$OUT_OMIT" temps; omit_out="$(cat "$STEP_OUT")"
if [ "$run_pin_rc" -ne 0 ] && grep -q 'WS0_CFG_TEMPS' <<<"$omit_out"; then
  pass "EXECUTE CONTROL fired (config env is genuinely read): with WS0_CFG_TEMPS unexported the step REFUSES and names the variable"
else
  fail "EXECUTE CONTROL did NOT fire: an unexported WS0_CFG_TEMPS must make the step refuse by name (rc=$run_pin_rc, out: $(head -3 <<<"$omit_out"))"
fi

# ...and the same control with the variable ALREADY EXPORTED in the caller's environment, which is
# the state it silently failed in before (#3451 review round 1, finding 3): `env` inherits, so an
# operator who happened to have `WS0_CFG_TEMPS` set made the "absent" variable present and the
# control red on a correct tree. A control that reds on a correct tree is the control people learn
# to waive, and it was also not measuring what it claimed.
run_pin_rc=0
OUT_INHERIT="$TMP/session-inherit"; mkdir -p "$OUT_INHERIT"
python3 - "$PERF_DIR" "$OUT_INHERIT" "$CORPUS" <<'PY'
import pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_ticket_input import write_ticket_template
write_ticket_template(pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3]) / "ws0-events.cql")
PY
export WS0_CFG_TEMPS="inherited-from-the-callers-shell"
run_pin_step "$DRIVER" "$OUT_INHERIT" temps; inherit_out="$(cat "$STEP_OUT")"
unset WS0_CFG_TEMPS
if [ "$run_pin_rc" -ne 0 ] && grep -q 'WS0_CFG_TEMPS' <<<"$inherit_out"; then
  pass "EXECUTE CONTROL fired (env inheritance): the omitted field is UNSET for the child, so the control still measures absence even when the caller has WS0_CFG_TEMPS exported"
else
  fail "EXECUTE CONTROL leaked the caller's environment: with WS0_CFG_TEMPS exported the step did not see it as absent (rc=$run_pin_rc, out: $(head -3 <<<"$inherit_out"))"
fi

# ============================================================================
# PART 4 — THE CPU-PIN-VERIFICATION STEP **RUNS**
# ============================================================================
# Its inputs are four `WS0_PIN_*` variables and the session dir. Nothing here reads real sysfs or
# runs `taskset`: the topology root is a string the step RECORDS, and recording is the whole
# subject of the step (the verification itself happens in `lib-cpu.sh`, covered by
# `test_ws0_cpu_pinning_guards.sh` against an injected topology root).
run_cpu_step() { # run_cpu_step <driver> <out-dir>
  local drv="$1" out="$2" idx body
  idx="$(find_block "$drv" 'pinning_record_path')"
  [[ "$idx" =~ ^[0-9]+$ ]] || { run_cpu_rc=90; echo "block not located: $idx" > "$STEP_OUT"; return; }
  body="$(emit_block "$drv" "$idx")"
  # THE VALUES COME FROM THE DRIVER, exactly as the session-pin step's do (#3451 post-rebase
  # round 3, F3). Round 2 wired only `write_session_corpus_pin`, so this step still ran on
  # fixture constants AND had its record verified against those same constants — a swapped
  # `WS0_PIN_SERVER_CPUS="$CLIENT_CPUS"` stayed green. Same shape as the round-11 union bug: a
  # check right about one step and silent about the other.
  local -a cpu_env=()
  local line
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    cpu_env+=("$line")
  done < <(driver_step_env "$drv" pinning_record_path)
  if [ "${#cpu_env[@]}" -eq 0 ]; then
    run_cpu_rc=91
    echo "driver_step_env produced no environment for the CPU-pin step" > "$STEP_OUT"
    return
  fi
  env "${cpu_env[@]}" python3 -c "$body" "$PERF_DIR" "$out" > "$STEP_OUT" 2>&1
  run_cpu_rc=$?
}

run_cpu_rc=0
run_cpu_step "$DRIVER" "$OUT"; cpu_out="$(cat "$STEP_OUT")"
cpu_file="$(python3 - "$PERF_DIR" "$OUT" <<'PY'
import pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_pinning import pinning_record_path
print(pinning_record_path(pathlib.Path(sys.argv[2])))
PY
)"
if [ "$run_cpu_rc" -eq 0 ] && [ -s "$cpu_file" ]; then
  pass "EXECUTE cpu-pin-verification: the SHIPPED step ran to completion and wrote $(basename "$cpu_file")"
else
  fail "EXECUTE cpu-pin-verification: the shipped step did not run (rc=$run_cpu_rc): $(head -4 <<<"$cpu_out")"
fi
if grep -q 'pinning pin:' <<<"$cpu_out"; then
  pass "EXECUTE cpu-pin-verification: printed 'pinning pin:' — $(grep -m1 'pinning pin:' <<<"$cpu_out" | cut -c1-96)"
else
  fail "EXECUTE cpu-pin-verification: the step did not print 'pinning pin:' (output: $(head -3 <<<"$cpu_out"))"
fi
# THE TWO STEPS ARE CROSS-CHECKED AGAINST EACH OTHER, not each against its own constants
# (#3451 post-rebase round 3, F3). The CPU record's pins are verified against the CPU lists read
# from THE SESSION MANIFEST the other step wrote — which is precisely the comparison
# `verify_pinning_record` exists to make in production. Verified against fixture constants
# instead, a swap in ONE prefix was invisible; and because the manifest and the record were
# validated INDEPENDENTLY, a MATCHING swap in both was invisible too. Reading one side from the
# other is what makes a consistent swap detectable.
# THE TWO SETS MUST AGREE before the values are compared: everything the driver maps must have a
# stated expectation, and nothing stated may be absent from the driver.
pin_derived="$(driver_pin_fields "$DRIVER")"
pin_stated="$(printf '%s\n' "${WS0_EXPECTED_PIN[@]}" | sed 's/=.*//' | sort | tr '\n' ' ' | sed 's/ $//')"
if [ -n "$pin_derived" ] && [ "$pin_derived" = "$pin_stated" ]; then
  pass "cpu-pin field coverage: the fields the driver sources from the environment [$pin_derived] are EXACTLY those with a stated expected value — a field added to the driver cannot be silently uncovered, and the values stay independent of the driver's own mapping"
else
  fail "cpu-pin field coverage: the driver maps [$pin_derived] but expectations are stated for [$pin_stated]. A driver-mapped field with no stated value is UNCHECKED; a stated value for a field the driver no longer maps is a stale claim"
fi
if python3 - "$PERF_DIR" "$OUT" ${WS0_EXPECTED_PIN[@]+"${WS0_EXPECTED_PIN[@]}"} <<'PY'
import json, pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_pinning import PINNING_RECORD_FIELDS, pinning_record_path, verify_pinning_record
from ws0_report import ARMS_ALLOWED, TEMPS_ALLOWED
from ws0_session import session_manifest_config
session = pathlib.Path(sys.argv[2])
# The CPU lists AS THE MANIFEST RECORDS THEM — the session-pin step's output, not a constant.
config = session_manifest_config(session, TEMPS_ALLOWED, ARMS_ALLOWED)
rec = verify_pinning_record(
    session, config["server_cpus"], config["client_cpus"], config["flight_server_cpus"]
)
missing = [f for f in PINNING_RECORD_FIELDS if not rec.get(f)]
if missing:
    print(f"pinning record is missing required fields: {missing}", file=sys.stderr)
    raise SystemExit(1)
# ...AND EACH RECORDED FIELD AGAINST ITS CONTROLLED INPUT (#3451 post-rebase round 5, F2).
# `server_siblings_expanded` and `topology_root` are both just non-empty strings to the shipped
# validator, so swapping their right-hand sides in the driver passes every check above. Only
# comparing each field to the value it was GIVEN can tell them apart.
written = json.loads(pinning_record_path(session).read_text())
expected = dict(pair.split("=", 1) for pair in sys.argv[3:])
if not expected:
    print("no expected pin fields were passed; this check would assert nothing",
          file=sys.stderr)
    raise SystemExit(1)
mismatched = {
    k: (written.get(k), v) for k, v in expected.items() if str(written.get(k)) != v
}
if mismatched:
    print(f"pinning-record fields differ from the controlled inputs: {mismatched}",
          file=sys.stderr)
    raise SystemExit(1)
PY
then
  pass "EXECUTE cpu-pin-verification: the SHIPPED READER accepts the record the shipped STEP wrote; its pins are checked against the CPU lists THE SESSION MANIFEST records (the two steps cross-checked against each other, not each against the same constants), and every recorded field against the distinct value it was given"
else
  fail "EXECUTE cpu-pin-verification: the shipped reader refused the record the shipped step wrote, or its pins disagree with the manifest the session-pin step produced"
fi

# --- CONTROL 4-map: a SWAPPED CPU right-hand side is caught, in BOTH shapes ---------------------
# Two swaps, because they defeat different checks and the second is the one round 3's F3 is about:
#
#   pin-only    WS0_PIN_SERVER_CPUS="$CLIENT_CPUS" — the record disagrees with the manifest.
#   consistent  the SAME swap in BOTH prefixes — each artifact is self-consistent, so validating
#               them independently sees nothing. Only reading the record's expectation FROM the
#               manifest catches it... and it does not: a consistent swap is, by construction, a
#               session that pinned and recorded the same (wrong) lists. See the assertion below
#               for what is actually claimed.
for cpu_swap in pin-only consistent; do
  SWAPDIR="$TMP/cpuswap-$cpu_swap"; mkdir -p "$SWAPDIR"
  SWAPDRV="$TMP/cpuswap-$cpu_swap-driver.sh"
  python3 - "$DRIVER" "$SWAPDRV" "$cpu_swap" <<'INJECT'
import pathlib, sys
text = pathlib.Path(sys.argv[1]).read_text()
pin = 'WS0_PIN_SERVER_CPUS="$SERVER_CPUS"'
cfg = 'WS0_CFG_SERVER_CPUS="$SERVER_CPUS"'
if text.count(pin) != 1 or text.count(cfg) != 1:
    print("INJECTION IMPOSSIBLE: the CPU assignments are not both present exactly once",
          file=sys.stderr)
    raise SystemExit(1)
out = text.replace(pin, 'WS0_PIN_SERVER_CPUS="$CLIENT_CPUS"')
if sys.argv[3] == "consistent":
    out = out.replace(cfg, 'WS0_CFG_SERVER_CPUS="$CLIENT_CPUS"')
pathlib.Path(sys.argv[2]).write_text(out)
INJECT
  swap_inject_rc=$?
  if [ "$swap_inject_rc" -ne 0 ]; then
    fail "cpu-mapping CONTROL ($cpu_swap): the swap could not be injected, so it could not fire"
    continue
  fi
  python3 - "$PERF_DIR" "$SWAPDIR" "$CORPUS" <<'PY'
import pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_ticket_input import write_ticket_template
write_ticket_template(pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3]) / "ws0-events.cql")
PY
  # BOTH STEPS MUST HAVE SUCCEEDED AND BOTH ARTIFACTS MUST EXIST BEFORE THE VERDICT MEANS
  # ANYTHING (#3451 post-rebase round 5, F3). This control used to treat ANY refusal as success
  # without checking either status, either artifact, or the REASON — so a setup failure or a
  # step that never ran satisfied it, and it passed without ever demonstrating that the
  # mismatch was detected. A control that cannot fail, inside the suite whose subject is
  # refusing exactly that.
  run_pin_rc=0; run_cpu_rc=0
  run_pin_step "$SWAPDRV" "$SWAPDIR" >/dev/null 2>&1
  run_cpu_step "$SWAPDRV" "$SWAPDIR" >/dev/null 2>&1
  if [ "$run_pin_rc" -ne 0 ] || [ "$run_cpu_rc" -ne 0 ]; then
    fail "cpu-mapping CONTROL ($cpu_swap): a step did not run (pin rc=$run_pin_rc, cpu rc=$run_cpu_rc), so nothing about detection was demonstrated"
    continue
  fi
  if [ ! -s "$SWAPDIR/session-corpus-pin.json" ] || [ ! -s "$SWAPDIR/pinning-verification.json" ]; then
    fail "cpu-mapping CONTROL ($cpu_swap): an artifact is missing, so the verifier below would refuse for the wrong reason"
    continue
  fi
  # ...and the refusal must be FOR THE SERVER-CPU MISMATCH, matched on the reason rather than on
  # the mere fact of a refusal. `refused-other` is a distinct verdict so a refusal for any other
  # cause fails the control instead of satisfying it.
  # The heredoc belongs to the COMMAND SUBSTITUTION, not to a trailing assignment. Attached to
  # the latter, `python3 -` reads no program: under a terminal it BLOCKS (the suite hung), and
  # with stdin closed it exits 0 having done nothing — which reads as "the verifier accepted",
  # i.e. the control silently inverts. Both observed while writing this.
  swap_reason="$(python3 - "$PERF_DIR" "$SWAPDIR" 2>&1 >/dev/null <<'PY'
import pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_pinning import verify_pinning_record
from ws0_report import ARMS_ALLOWED, TEMPS_ALLOWED
from ws0_session import session_manifest_config
session = pathlib.Path(sys.argv[2])
config = session_manifest_config(session, TEMPS_ALLOWED, ARMS_ALLOWED)
verify_pinning_record(
    session, config["server_cpus"], config["client_cpus"], config["flight_server_cpus"]
)
PY
)"
  swap_status=$?
  if [ "$swap_status" -eq 0 ]; then
    swap_verdict=accepted
  elif grep -qi 'server' <<<"$swap_reason" && grep -q "$(cfg_value client_cpus)" <<<"$swap_reason"; then
    swap_verdict=refused
  else
    swap_verdict=refused-other
  fi
  if [ "$cpu_swap" = "pin-only" ] && [ "$swap_verdict" = refused ]; then
    pass "cpu-mapping CONTROL fired (pin-only): WS0_PIN_SERVER_CPUS taking \$CLIENT_CPUS makes the record disagree with the manifest, and the cross-check REFUSES it — invisible while the record was verified against fixture constants"
  elif [ "$cpu_swap" = "consistent" ] && [ "$swap_verdict" = accepted ]; then
    pass "cpu-mapping STATED LIMIT (consistent): swapping BOTH prefixes together is ACCEPTED, and that is honest rather than a gap this suite can close — the manifest and the record then agree, so no cross-check between them can tell; catching it needs an oracle for what the CPU lists OUGHT to be, which only the host topology has (test_ws0_cpu_pinning_guards.sh's subject)"
  else
    fail "cpu-mapping CONTROL ($cpu_swap): expected the cross-check to $([ "$cpu_swap" = pin-only ] && echo 'refuse NAMING the server-CPU mismatch' || echo accept), got $swap_verdict — reason was: $(head -c 200 <<<"$swap_reason")"
  fi
done

# --- POSITIVE CONTROL 4: the same harness OBSERVES the defective CPU step failing ---------------
DEFECTIVE_CPU="$TMP/defect-exec-cpu.sh"
if defective_copy "$DRIVER" "$DEFECTIVE_CPU" "$pb_name" "$pb_map" "$pb_key" 2>"$TMP/inject-cpu.err"; then
  OUT_BAD2="$TMP/session-bad-cpu"; mkdir -p "$OUT_BAD2"
  run_cpu_rc=0
  run_cpu_step "$DEFECTIVE_CPU" "$OUT_BAD2"; bad_cpu_out="$(cat "$STEP_OUT")"
  bad_cpu_file="$OUT_BAD2/$(basename "$cpu_file")"
  if [ "$run_cpu_rc" -ne 0 ] && grep -q 'SyntaxError' <<<"$bad_cpu_out" && [ ! -e "$bad_cpu_file" ]; then
    pass "EXECUTE CONTROL fired (cpu-pin-verification): the injected step FAILS to run (rc=$run_cpu_rc, SyntaxError) and writes NO record"
  else
    fail "EXECUTE CONTROL did NOT fire (cpu-pin-verification): rc=$run_cpu_rc, record-exists=$([ -e "$bad_cpu_file" ] && echo yes || echo no), out: $(head -3 <<<"$bad_cpu_out")"
  fi
else
  fail "EXECUTE CONTROL (cpu-pin-verification): the defect could not be injected — $(head -2 "$TMP/inject-cpu.err")"
fi

# ============================================================================
# A MINIMUM CHECK COUNT
# ============================================================================
# `set -uo pipefail` (no `-e`) means a block that silently never executes lowers the count and
# registers NO failure, while the gate reads only the exit code.
#
# AN EXACT COUNT, FAILING IN BOTH DIRECTIONS (#3451 post-rebase round 6, F4).
#
# STILL NAMED `MIN_CHECKS`, and that is a cross-suite contract rather than a leftover:
# `test_ws0_hermeticity.sh`'s `floor-present` check requires that identifier in every ws0 suite,
# and renaming it here broke that sibling — measured. The comparison below is EQUALITY, which is
# strictly stronger than the floor the lint is looking for, so the contract is met and the guard
# is better than the name suggests.
#
# `-lt` made this a FLOOR, and a floor is satisfied by adding coverage without bumping it — after
# which the new coverage can be deleted again and the stale floor still passes. It sat 18 below
# actual for exactly that reason. Equality means a change in either direction is a decision
# someone makes here, in one line, with the failure text saying which direction and why.
MIN_CHECKS=51
echo
if [ "$checks" -ne "$MIN_CHECKS" ]; then
  echo "FAIL - $checks check(s) ran; this suite has EXACTLY $MIN_CHECKS."
  if [ "$checks" -lt "$MIN_CHECKS" ]; then
    echo "       FEWER: a block silently never executed. It would otherwise lower the count with"
    echo "       no failure registered, and the gate reads only the exit code (#3451)."
  else
    echo "       MORE: you added coverage. Bump MIN_CHECKS to $checks — deliberately, so the"
    echo "       ratchet keeps holding. A floor would have accepted this silently, and the new"
    echo "       coverage could then be deleted later while still meeting the stale floor."
  fi
  exit 1
fi
if [ "$fails" -eq 0 ]; then
  echo "PASS - all $checks WS0 embedded-step EXECUTE-direction checks fired as specified"
  exit 0
fi
echo "FAIL - $fails of $checks WS0 embedded-step EXECUTE-direction check(s) FAILED"
exit 1
