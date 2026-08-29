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
#   * the environment-variable names the corpus-pin step reads are DERIVED from the shipped
#     `ws0_session.MANIFEST_CONFIG_FIELDS` and asserted equal to it, so a field added there without
#     a driver export is a failure here rather than a refusal at report time;
#   * EVERY embedded block in the driver COMPILES — the total property, so instance #8 anywhere in
#     that file is caught and not only the two steps this issue repaired;
#   * EVERY embedded block parses on EVERY INTERPRETER THIS REPOSITORY RUNS, not merely on this
#     box. `compile()` cannot answer that: PEP 701 made the nested SAME-TYPE quote spelling legal
#     in 3.12, so a regression to it — the alternative the driver's own comment says it rejected —
#     passes on a 3.12 box and breaks the 3.11 workflows this repo pins. Two oracles, each sound
#     over its own interpreter range, with the one that answered NAMED in the output.
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
#   * anything measured: no `perf`, no Flight server, no rep loop, no 2.8 GB corpus.
#
# Hermetic: python3, a few KB under `$TMPDIR`. No sudo, cargo, perf, taskset, network, root, and no
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

TMP="$(mktemp -d)"
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
findings_of() { grep -v '^#COMPLETE ' <<<"$1" | grep -vE '^(BLOCK|MENTION|SCRIPT)\b' | grep -v '^$'; }

# defective_copy <src> <dest> <placeholder> <mapping> <key> [flavour] — a scratch copy of the
# driver whose f-string placeholder `{<placeholder>}` has been rewritten to an INLINE SUBSCRIPT.
#
# Two flavours, because they fail on DIFFERENT interpreters and the suite must observe both:
#   escaped  a backslash inside the expression. No CPython parses it — the shipped defect.
#   nested   the SAME quote character as the enclosing f-string. Legal from 3.12 (PEP 701) and a
#            SyntaxError on everything older, so `compile()` on a 3.12 box CANNOT see it. This is
#            the alternative the driver's comment says was rejected.
#
# Exits non-zero when the placeholder is not found EXACTLY once, so a control that silently
# injected nothing is a failure rather than a green "the check found no defect".
defective_copy() {
  python3 - "$@" <<'PY'
import pathlib, sys
src, dest, placeholder, mapping, key = sys.argv[1:6]
flavour = sys.argv[6] if len(sys.argv) > 6 else "escaped"
# Both spellings are CONSTRUCTED from character codes rather than written out, so this file does
# not ship a literal example of either (#3312's rule about prose inside a diff naming its own
# oracle, applied to a test that must build its own bad input).
backslash, dquote = chr(92), chr(34)
lead = backslash + dquote if flavour == "escaped" else dquote
bad = "{" + mapping + "[" + lead + key + lead + "]}"
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

# portable <file> / portable_source <file> — the PEP 701 oracle over a driver's blocks, or over an
# ordinary python file.
portable() { python3 "$EXTRACT" portable "$1" 2>&1; }
portable_source() { python3 "$EXTRACT" portable-source "$1" 2>&1; }

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
# and a floor cannot see it once the floor is met: a delimiter that silently stopped recognising
# one shape would drop that block from BOTH the census and the compile check while the count still
# cleared a floor. (MEASURED tree-wide while building this: a column-0-only closer rule found 31
# blocks where the correct rule finds 59 — a loose delimiter under-counts SUBJECTS, it does not
# merely mis-cut them.) The three are the two fatal pin STEPS plus the inline monotonic-clock read.
# Adding an embedded step to this driver is a deliberate act, so bump this constant deliberately;
# the compile property below then covers the new block automatically.
EXPECTED_BLOCKS=3
if [ "$block_count" -eq "$EXPECTED_BLOCKS" ]; then
  pass "census: exactly $block_count embedded python block(s) in the driver — the 2 fatal pin steps plus the inline monotonic-clock read"
else
  fail "census: $block_count embedded block(s) found in $DRIVER, expected $EXPECTED_BLOCKS. If a step was ADDED, bump EXPECTED_BLOCKS; if the count DROPPED, the extractor has stopped seeing a shape and the missing block is being compiled by nothing"
fi

# WHICH block is which, by CONTENT. The two steps are located by a shipped symbol each body calls,
# so a reordering of the driver cannot silently point this suite at the wrong step — and a step
# that DISAPPEARED is a failure here rather than a suite that quietly tests one block twice.
find_block() { # find_block <driver> <needle> — the index of the ONE block containing <needle>
  local drv="$1" needle="$2" n total hit="" body
  total="$(python3 "$EXTRACT" census "$drv" 2>/dev/null | grep -c '^BLOCK	')"
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
if grep -q 'does not recognise' <<<"$unk_out"; then
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
if [ "$(find_block "$NO_PIN_STEP" 'write_session_corpus_pin')" = "ABSENT" ]; then
  pass "census CONTROL fired: with the session-pin writer gone from the driver, the locator reports ABSENT rather than picking another block"
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
if [ "$idiom_blocks" -ge 1 ] && [ -z "$(findings_of "$idiom_census")" ] \
   && [ -z "$(findings_of "$idiom_compile")" ] && [ "$idiom_apostrophe" -eq 1 ]; then
  pass "census NO-FALSE-FINDING: $idiom_blocks block(s) in lib-ws0-fixtures.sh (which uses the literal-apostrophe idiom) are delimited and ALL COMPILE — the extractor rejoins the idiom instead of cutting the body there"
else
  fail "census manufactured a finding on GOOD input: lib-ws0-fixtures.sh gave blocks=$idiom_blocks apostrophe-in-a-body=$idiom_apostrophe census='$(findings_of "$idiom_census" | head -1)' compile='$(findings_of "$idiom_compile" | head -1)'"
fi

# --- CONTROL 1c-ter: the closer at the END of the last body line, bash arguments trailing -------
# Shape 2 of three (see the extractor header). It is idiomatic and already in use at
# `test-data/scripts/gen-perf-corpus-bti.sh`, `scripts/lib/gate-notify.sh` and
# `docs/reports/ws0-3217-artifacts/harness/common.sh`, so it is the shape most likely to be written
# into this driver next. Exercised against a SCRATCH copy rather than one of those files, so the
# control cannot drift when they change: the block must be delimited (not reported undelimited) AND
# a defect inside it must still be reported.
TRAILING_DRIVER="$TMP/trailing-closer-ws0-driver.sh"
python3 - "$DRIVER" "$TRAILING_DRIVER" <<'INJECT'
import pathlib, sys
# The same defect class as the two shipped steps, built from character codes so no committed file
# holds a literal example of the spelling.
backslash, dquote = chr(92), chr(34)
bad = "{d[" + backslash + dquote + "k" + backslash + dquote + "]}"
q = chr(39)
step = ("python3 -c " + q + "\nd = {'k': 1}\nprint(f'" + bad + "')" + q + " \"$OUT_DIR\"\n")
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

# --- POSITIVE CONTROL 1d: a FUTURE step written as a heredoc is censused and compiled ----------
# The driver carries no python heredoc today. The census handles the shape anyway, and an
# unexercised branch in a fail-closed extractor is exactly the code that is discovered to be broken
# by the first person who needs it — which is the complaint #3451 records. So the shape is
# exercised against a scratch copy: the block must be COUNTED, and a defect inside it must be
# REPORTED, or a step added that way would fall outside the compile check entirely.
HEREDOC_DRIVER="$TMP/heredoc-step-ws0-driver.sh"
python3 - "$DRIVER" "$HEREDOC_DRIVER" <<'INJECT'
import pathlib, sys
# A minimal future step in the heredoc shape, carrying the same defect class the two `-c` steps
# shipped: a subscript inside an f-string expression, built from character codes so that no
# committed file holds a literal example of the spelling.
backslash, dquote = chr(92), chr(34)
bad = "{d[" + backslash + dquote + "k" + backslash + dquote + "]}"
tag = "PY" + "STEP"
step = ("python3 - \"$OUT_DIR\" <<'" + tag + "'\n"
        "d = {'k': 1}\n"
        "print(f'" + bad + "')\n"
        + tag + "\n")
pathlib.Path(sys.argv[2]).write_text(pathlib.Path(sys.argv[1]).read_text() + step)
INJECT
hd_census="$(census "$HEREDOC_DRIVER")"
hd_compile="$(compile_blocks "$HEREDOC_DRIVER")"
hd_blocks="$(grep -c '^BLOCK	' <<<"$hd_census")"
if [ "$hd_blocks" -eq "$((block_count + 1))" ] && grep -q 'heredoc' <<<"$hd_census" \
   && grep -q 'DOES NOT COMPILE' <<<"$hd_compile"; then
  pass "census CONTROL fired (heredoc shape): a step written as a python heredoc is COUNTED ($hd_blocks blocks) and a defect inside it is REPORTED — a future step in that shape cannot fall outside the compile check"
else
  fail "census CONTROL did not fire (heredoc shape): blocks=$hd_blocks (expected $((block_count + 1))), compile said: $(findings_of "$hd_compile" | head -2)"
fi

# --- CONTROL 1d-bis: the heredoc terminator rule is the SHELL's, per form ----------------------
# `<<TAG` takes the terminator EXACTLY — a space-indented `  TAG` is ordinary body to the shell, so
# accepting it truncates the block and hands python a body it never receives. `<<-TAG` strips
# leading TABS from the terminator AND from every body line, so leaving them in place turns a
# perfectly good body into an IndentationError the shell would never produce. Both are asserted,
# because a `.strip()` comparison was wrong in both directions at once.
SPACE_TERM_DRIVER="$TMP/space-terminator-ws0-driver.sh"
python3 - "$DRIVER" "$SPACE_TERM_DRIVER" <<'INJECT'
import pathlib, sys
tag = "PY" + "SPACED"
# The ONLY line matching the tag is INDENTED WITH SPACES, which the shell does not accept for a
# plain `<<`, so the heredoc is genuinely unterminated.
step = ("python3 - <<'" + tag + "'\nprint(1)\n  " + tag + "\n")
pathlib.Path(sys.argv[2]).write_text(pathlib.Path(sys.argv[1]).read_text() + step)
INJECT
sp_out="$(census "$SPACE_TERM_DRIVER")"
if grep -q 'never terminated' <<<"$sp_out" && grep -q 'exact match' <<<"$sp_out"; then
  pass "census CONTROL fired (plain <<): a SPACE-INDENTED line matching the tag is body, not a terminator — the block is reported unterminated rather than silently truncated"
else
  fail "census CONTROL did not fire (plain <<): a space-indented terminator must not delimit a plain-form heredoc, got: $(findings_of "$sp_out" | head -2)"
fi

TAB_TERM_DRIVER="$TMP/tab-terminator-ws0-driver.sh"
python3 - "$DRIVER" "$TAB_TERM_DRIVER" <<'INJECT'
import pathlib, sys
tag = "PY" + "TABBED"
tab = chr(9)
# `<<-`: tabs are stripped from the terminator AND from the body. If the body's tabs survive, the
# extracted source is indented and does not compile — which is how this control discriminates.
step = ("python3 - <<-'" + tag + "'\n" + tab + "print(1)\n" + tab + tag + "\n")
pathlib.Path(sys.argv[2]).write_text(pathlib.Path(sys.argv[1]).read_text() + step)
INJECT
tab_census="$(census "$TAB_TERM_DRIVER")"
tab_compile="$(compile_blocks "$TAB_TERM_DRIVER")"
tab_blocks="$(grep -c '^BLOCK	' <<<"$tab_census")"
if [ "$tab_blocks" -eq "$((block_count + 1))" ] && [ -z "$(findings_of "$tab_census")" ] \
   && [ -z "$(findings_of "$tab_compile")" ]; then
  pass "census CONTROL fired (<<- form): a TAB-indented terminator delimits and the body's leading tabs are stripped as the shell strips them — the extracted source compiles ($tab_blocks blocks)"
else
  fail "census CONTROL did not fire (<<- form): blocks=$tab_blocks (expected $((block_count + 1))), census='$(findings_of "$tab_census" | head -1)', compile='$(findings_of "$tab_compile" | head -1)'"
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
  if grep -q 'DOES NOT COMPILE' <<<"$out" && grep -q 'unexpected character after line continuation' <<<"$out"; then
    pass "compile CONTROL fired ($label): $(grep 'DOES NOT COMPILE' <<<"$out" | head -1 | cut -c1-120)"
  else
    fail "compile CONTROL did NOT fire ($label): the injected defect must be reported, got: $(findings_of "$out" | head -2)"
  fi
}
control_compile "session-corpus-pin step" pin_sha pin data_db_sha256
control_compile "CPU-pin-verification step" rec_server_cpus rec server_cpus

# ============================================================================
# PART 2b — THE BLOCKS PARSE ON EVERY INTERPRETER THIS REPO RUNS, NOT JUST THIS BOX
# ============================================================================
# `compile()` answers "does this parse HERE". The property the repaired steps need is "does this
# parse on every interpreter this repository runs on", and the two differ for exactly one
# spelling: a subscript inside an f-string expression written with NESTED SAME-TYPE QUOTES. PEP
# 701 made it legal in 3.12; it is a SyntaxError on everything older. That is the alternative the
# driver's own comment says was rejected — and until this part existed, nothing enforced it: a
# regression to that form passes `compile()` on a 3.12 box and breaks the 3.11 workflows this
# repository pins (and the `>=3.9` the python bindings declare).
#
# TWO ORACLES, EACH SOUND OVER ITS OWN RANGE, AND THE PASS LINE NAMES WHICH ONE ANSWERED. On 3.12+
# a tokenizer walk is the oracle, because `compile()` there accepts the bad form; below 3.12
# `compile()` IS the oracle, because the bad form does not parse at all. Nothing is conditional on
# an optional interpreter being installed — a check that skips when a dependency is absent is the
# coverage gap this whole issue is about.
port_out="$(portable "$DRIVER")"
port_marker="$(grep -m1 '^#COMPLETE ' <<<"$port_out")"
if [ -n "$port_marker" ] && [ -z "$(findings_of "$port_out")" ]; then
  pass "portable: every embedded block parses on EVERY interpreter this repo runs, not just this box ($port_marker)"
else
  fail "portable: an embedded block in $DRIVER uses a spelling that only parses on this box — $(findings_of "$port_out" | head -2)"
fi

# --- CONTROL 2b-i: the 3.12-only spelling is REFUSED, and `compile` alone cannot see it ---------
NESTED_DRIVER="$TMP/nested-quote-ws0-driver.sh"
if defective_copy "$DRIVER" "$NESTED_DRIVER" pin_sha pin data_db_sha256 nested 2>"$TMP/inject-nested.err"; then
  nest_port="$(portable "$NESTED_DRIVER")"
  nest_comp="$(compile_blocks "$NESTED_DRIVER")"
  nest_oracle="$(sed -n 's/.*oracle=\([a-z]*\).*/\1/p' <<<"$nest_port" | head -1)"
  if grep -q 'NOT PORTABLE' <<<"$nest_port"; then
    pass "portable CONTROL fired: the nested SAME-TYPE quote spelling is REFUSED — $(grep -m1 'NOT PORTABLE' <<<"$nest_port" | cut -c1-118)"
  else
    fail "portable CONTROL did NOT fire: the 3.12-only spelling must be refused, got: $(findings_of "$nest_port" | head -2)"
  fi
  # ...and WHY the new oracle earns its place, asserted rather than argued: on a 3.12+ box the
  # plain compile check is SILENT about this input. On an older box compile is the oracle and must
  # report it. Both branches assert; neither is a skip, and the pass line says which ran.
  if [ "$nest_oracle" = "tokenizer" ]; then
    if [ -z "$(findings_of "$nest_comp")" ]; then
      pass "portable CONTROL discriminates: on this 3.12+ interpreter (oracle=tokenizer) the plain compile check is SILENT about the same input — so the portability oracle is doing work compile cannot"
    else
      fail "on a 3.12+ interpreter compile() must ACCEPT the nested spelling (that is the trap); it reported: $(findings_of "$nest_comp" | head -1)"
    fi
  else
    if grep -q 'DOES NOT COMPILE' <<<"$nest_comp"; then
      pass "portable CONTROL discriminates: on this pre-3.12 interpreter (oracle=compile) the nested spelling does not parse at all, and the compile check reports it"
    else
      fail "on a pre-3.12 interpreter the nested spelling must fail to compile; it did not"
    fi
  fi
else
  fail "portable CONTROL: the nested-quote defect could not be injected — $(head -2 "$TMP/inject-nested.err")"
fi

# --- CONTROL 2b-ii: THE ACCEPT DIRECTION — a DIFFERENT-type nested quote is legal everywhere -----
# `f"{x['k']}"` parses on every interpreter, and flagging it would be a false red on ordinary code.
# The same-type/different-type boundary IS the check, so both sides of it are asserted. Driven over
# a standalone source file rather than an injected block: an apostrophe inside a `python3 -c '…'`
# body would terminate the shell string, which is why the driver cannot use that spelling either.
python3 - "$TMP" <<'INJECT'
import pathlib, sys
dq, sq = chr(34), chr(39)
tmp = pathlib.Path(sys.argv[1])
(tmp / "portable-ok.py").write_text(
    "x = {'k': 1}\nprint(f" + dq + "{x[" + sq + "k" + sq + "]}" + dq + ")\n")
(tmp / "portable-remedy.py").write_text(
    "x = {'k': 1}\nv = x['k']\nprint(f" + dq + "{v}" + dq + ")\n")
INJECT
ok_out="$(portable_source "$TMP/portable-ok.py")"
rem_out="$(portable_source "$TMP/portable-remedy.py")"
if [ -z "$(findings_of "$ok_out")" ] && [ -z "$(findings_of "$rem_out")" ] \
   && grep -q '^#COMPLETE ' <<<"$ok_out" && grep -q '^#COMPLETE ' <<<"$rem_out"; then
  pass "portable ACCEPT direction: a DIFFERENT-type nested quote (legal on every interpreter) and the local-binding remedy are BOTH clean — the check discriminates rather than refusing every subscript"
else
  fail "portable manufactured a finding on portable code: different-type='$(findings_of "$ok_out" | head -1)' remedy='$(findings_of "$rem_out" | head -1)'"
fi

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
declare -A CFG=(
  [reps]=1 [temps]=warm [arms]=bypass [scan_passes]=1
  [server_cpus]=2,10 [client_cpus]=4,12 [step_duration]=45s/1s
  [flight_endpoint]=grpc://127.0.0.1:1
  # `non-baseline`, and that is the only honest value available here: this is a few-KB synthetic
  # corpus, and the shipped `require_canonical_or_declared` REFUSES a divergent corpus in
  # `baseline` mode — correctly. Declaring the mode is the supported way past it, not a way round
  # it: the step still runs the real comparison and records every divergence it finds.
  [baseline_mode]=non-baseline
)
cfg_keys="$(printf '%s\n' "${!CFG[@]}" | sort | tr '\n' ' ')"
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
  for field in "${!CFG[@]}"; do
    if [ "$field" = "$omit" ]; then
      # UNSET, not merely "not passed" (#3451 review round 1, finding 3). `env` INHERITS the
      # caller environment, so omitting the assignment leaves a value the operator happened to
      # have exported — and the control then measures nothing while reporting a failure. `-u`
      # rather than an empty value: the step treats an empty string as absent too, but "was not
      # exported" is the condition being tested and `-u` is the only spelling that states it.
      unset_args+=("-u" "WS0_CFG_$(echo "$field" | tr '[:lower:]' '[:upper:]')")
      continue
    fi
    env_args+=("WS0_CFG_$(echo "$field" | tr '[:lower:]' '[:upper:]')=${CFG[$field]}")
  done
  idx="$(find_block "$drv" 'write_session_corpus_pin')"
  [[ "$idx" =~ ^[0-9]+$ ]] || { run_pin_rc=90; echo "block not located: $idx" > "$STEP_OUT"; return; }
  body="$(emit_block "$drv" "$idx")"
  env "${unset_args[@]}" "${env_args[@]}" python3 -c "$body" "$PERF_DIR" "$CORPUS" "$out" \
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
if python3 - "$PERF_DIR" "$OUT" "$CORPUS" <<'PY'
import pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_session import verify_session_corpus_pin
from ws0_validate import load_corpus_identity
session, corpus = pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3])
report = verify_session_corpus_pin(session, corpus, load_corpus_identity(corpus))
# The reader's own report, asserted field by field so this case cannot pass on a verifier that
# returned an empty dict: the pin was taken BEFORE measurement, and it carries the digests of the
# corpus, the schema and the Flight ticket the step measured from disk.
assert report.get("pinned_before_measurement") is True, report
assert len(report.get("pinned_data_db_sha256", "")) == 64, report
assert len(report.get("pinned_schema_sha256", "")) == 64, report
assert len(report.get("pinned_ticket_sha256", "")) == 64, report
assert report.get("pinned_components", 0) >= 5, report
PY
then
  pass "EXECUTE session-corpus-pin: the SHIPPED READER (verify_session_corpus_pin) accepts the pin the shipped STEP wrote — writer and reader agree end to end"
else
  fail "EXECUTE session-corpus-pin: the shipped reader refused the pin the shipped step wrote"
fi

# --- POSITIVE CONTROL 3a: the same harness OBSERVES the defective step failing -----------------
DEFECTIVE_PIN="$TMP/defect-exec-pin.sh"
if defective_copy "$DRIVER" "$DEFECTIVE_PIN" pin_sha pin data_db_sha256 2>"$TMP/inject-pin.err"; then
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
  env WS0_PIN_SERVER_CPUS="${CFG[server_cpus]}" WS0_PIN_CLIENT_CPUS="${CFG[client_cpus]}" \
      WS0_PIN_SIBLINGS="server cpu 2 siblings 2,10; server cpu 10 siblings 2,10" \
      WS0_PIN_TOPOLOGY_ROOT="$TMP/fake-topology" \
      python3 -c "$body" "$PERF_DIR" "$out" > "$STEP_OUT" 2>&1
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
if python3 - "$PERF_DIR" "$OUT" "${CFG[server_cpus]}" "${CFG[client_cpus]}" <<'PY'
import pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_pinning import PINNING_RECORD_FIELDS, verify_pinning_record
rec = verify_pinning_record(pathlib.Path(sys.argv[2]), sys.argv[3], sys.argv[4])
missing = [f for f in PINNING_RECORD_FIELDS if not rec.get(f)]
assert not missing, missing
PY
then
  pass "EXECUTE cpu-pin-verification: the SHIPPED READER (verify_pinning_record) accepts the record the shipped STEP wrote, with every PINNING_RECORD_FIELDS field populated"
else
  fail "EXECUTE cpu-pin-verification: the shipped reader refused the record the shipped step wrote"
fi

# --- POSITIVE CONTROL 4: the same harness OBSERVES the defective CPU step failing ---------------
DEFECTIVE_CPU="$TMP/defect-exec-cpu.sh"
if defective_copy "$DRIVER" "$DEFECTIVE_CPU" rec_server_cpus rec server_cpus 2>"$TMP/inject-cpu.err"; then
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
# registers NO failure, while the gate reads only the exit code. Deliberately below the current
# count (so adding a case does not red it) and far above zero.
MIN_CHECKS=26
echo
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would otherwise lower the count with no"
  echo "       failure registered, and the gate reads only the exit code (#3451)."
  exit 1
fi
if [ "$fails" -eq 0 ]; then
  echo "PASS - all $checks WS0 embedded-step EXECUTE-direction checks fired as specified"
  exit 0
fi
echo "FAIL - $fails of $checks WS0 embedded-step EXECUTE-direction check(s) FAILED"
exit 1
