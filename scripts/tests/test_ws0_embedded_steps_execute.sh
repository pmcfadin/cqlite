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
#     that file is caught and not only the two steps this issue repaired.
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

# defective_copy <src> <dest> <placeholder> <mapping> <key> — a scratch copy of the driver whose
# f-string placeholder `{<placeholder>}` has been rewritten to the INLINE SUBSCRIPT spelling that
# no CPython parses. Exits non-zero when the placeholder is not found EXACTLY once, so a control
# that silently injected nothing is a failure rather than a green "the check found no defect".
defective_copy() {
  python3 - "$@" <<'PY'
import pathlib, sys
src, dest, placeholder, mapping, key = sys.argv[1:6]
# The defect is CONSTRUCTED from character codes rather than written out, so this file does not
# ship a literal example of the spelling whose absence the rig cares about (#3312's rule about
# prose inside a diff naming its own oracle, applied to a test that must build its own bad input).
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
# A FLOOR, not an equality. Equality would red on a legitimately-added block while adding no
# safety: the TOTAL compile property below covers any new block whatever the count, and the two
# SUBJECT blocks are identified by content (next check) rather than by position. What the floor
# catches is wholesale extractor breakage — a census that suddenly sees one block, or none.
MIN_BLOCKS=3
if [ "$block_count" -ge "$MIN_BLOCKS" ]; then
  pass "census: $block_count embedded python block(s) found in the driver (floor $MIN_BLOCKS)"
else
  fail "census: only $block_count embedded block(s) found in $DRIVER; this driver carries at least $MIN_BLOCKS, so the extractor is not seeing its subject"
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
# The closing-quote line of the corpus-pin block is deleted in a scratch copy. Extracting to
# end-of-file would compile a truncated body and report a defect that is the extractor's own, so
# the extractor must refuse instead.
UNDELIMITED="$TMP/undelimited-ws0-driver.sh"
python3 - "$DRIVER" "$UNDELIMITED" <<'PY'
import pathlib, sys
lines = pathlib.Path(sys.argv[1]).read_text().split("\n")
# Every line that CLOSES an embedded block (a leading single quote) is removed, which is the
# shape a mis-edited driver leaves behind.
pathlib.Path(sys.argv[2]).write_text("\n".join(l for l in lines if not l.startswith("'")))
PY
und_out="$(census "$UNDELIMITED")"
if [ -n "$(findings_of "$und_out")" ] && grep -q 'cannot be delimited' <<<"$und_out"; then
  pass "census CONTROL fired: an undelimited block is a FINDING — $(findings_of "$und_out" | head -1 | cut -c1-110)"
else
  fail "census CONTROL did not fire: an undelimited embedded block must be a finding, got: $(head -3 <<<"$und_out")"
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
  local -a env_args=()
  for field in "${!CFG[@]}"; do
    [ "$field" = "$omit" ] && continue
    env_args+=("WS0_CFG_$(echo "$field" | tr '[:lower:]' '[:upper:]')=${CFG[$field]}")
  done
  idx="$(find_block "$drv" 'write_session_corpus_pin')"
  [[ "$idx" =~ ^[0-9]+$ ]] || { run_pin_rc=90; echo "block not located: $idx" > "$STEP_OUT"; return; }
  body="$(emit_block "$drv" "$idx")"
  env "${env_args[@]}" python3 -c "$body" "$PERF_DIR" "$CORPUS" "$out" "$REPO_ROOT" \
    > "$STEP_OUT" 2>&1
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
MIN_CHECKS=18
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
