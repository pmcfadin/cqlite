#!/usr/bin/env bash
# gate-component-verdict.sh — answer "did THIS COMPONENT pass?" from a gate summary,
# as an assertion SEPARATE from "did the run finish?" (issue #3750).
#
# THE DEFECT THIS CLOSES
# ----------------------
# `--only <component>` demotes a successful run to a PARTIAL verdict token (agent-gate.sh,
# on purpose — a component probe must never be pastable as the gate of record), while the
# mandated #3041 completion probe was published UNANCHORED and accepting only the PASS and
# FAIL tokens. So it terminated on failure and SPUN FOREVER ON SUCCESS. A lane spun 8+
# minutes past a terminal PASS and then re-ran an 18-minute component that had passed.
#
# The first fix for that was to widen the completion grammar to accept PARTIAL. That
# removes the hang and introduces a WORSE bug the moment anything reads success out of
# it, because:
#
#     PARTIAL says THE RUN ENDED. It does not say MY COMPONENT PASSED.
#
# A component probe therefore needs TWO assertions, and this script is the second one:
#
#   1. COMPLETION — did the run end? PRIMARY signal: the process EXIT STATUS, where the
#      poller can observe it (observing an exit status at all means the run ended; for
#      `--only`, agent-gate.sh exits 3). FALLBACK, for a detached run whose exit status
#      the poller never sees: the anchored, token-terminated text grammar for that MODE
#      (see the grammars at the end of this header). `scripts/gate-liveness.sh` is the
#      shared reader for it, and this script ASKS IT rather than re-greping it.
#   2. VERDICT — did the component pass? THIS script, read from the component's OWN
#      line. A completed run whose component SKIPped or is ABSENT is NOT a pass: a SKIP
#      means the check never ran, which is the vacuous pass itself.
#
# EVERY POSITIVE VERDICT IS AN AFFIRMATIVE MEASUREMENT (CLAUDE.md, #3229)
# ----------------------------------------------------------------------
# PASS requires ALL of:
#   * the shared reader reports COMPLETE for THIS run (terminal verdict, framing intact,
#     run-id bound);
#   * the file holds EXACTLY ONE block, whose opener is the FULL-gate marker — a LITE or
#     DELTA block is a different claim and is REFUSED, not answered;
#   * `tree-integrity:` appears exactly once in that block and its token is exactly PASS,
#     and no `summary-integrity:` line is present. Those two lines are the gate's own
#     statement that the run is NON-CERTIFYING, and they invalidate EVERY component in
#     the block (#2926/#2874) — unlike a SIBLING component's failure, which says nothing
#     about this one;
#   * the block carries EXACTLY ONE line for the requested component, in the emitter's own
#     component-line shape, and its status token is EXACTLY PASS;
#   * where the block states an `--only` scope, the requested component is in it.
# Nothing else reaches PASS. Everything unmeasurable is COULD-NOT-MEASURE with a NAMED
# cause and is NEVER read as a pass. NOT-PASS is likewise an affirmative reading: the
# component's line was found and its status is not PASS, the block AFFIRMATIVELY does not
# name it, or the gate declared the block non-certifying.
#
#   VERDICT             exit  meaning
#   PASS                  0   this component's own line reads PASS in a valid, complete run
#   NOT-PASS              1   read affirmatively, and it is not a pass (FAIL/SKIP/absent)
#   COULD-NOT-MEASURE     4   no verdict is available, WHATEVER THE REASON; the printed
#                             cause says what was unmeasurable, quoting the shared reader
#                             verbatim where the reader is what answered
#   USAGE                64   the request is malformed, or is not this tool's to serve
#
# THIS IS NOT A COMPLETION PROBE, AND IT HAS NO OPINION ABOUT LIVENESS — NEVER CALL IT IN A
# LOOP. Establish completion FIRST: the process EXIT STATUS where you can observe it (for
# `--only`, agent-gate.sh exits 3), else `scripts/gate-liveness.sh`, which is the
# three-valued liveness authority (COMPLETE / RUNNING / STALLED / UNKNOWN) and the only one
# of the two that may be polled. Then ask this for the verdict, once.
#
# A RETRYABILITY TAXONOMY WAS TRIED HERE AND DESCOPED (#3750, review round 2), and the
# reason is worth keeping because it is the standing shape: a second exit code for "still
# running" produced three independent findings in one round, and the harmful one was
# unanswerable by patching. `--no-wait` makes the reader's STALLED (rc 3) UNREACHABLE — its
# `confirmation-skipped` arm returns UNKNOWN 4 instead — so an INCOMPLETE summary with a
# VALID, run-id-matching but slightly STALE beat, routine on a multi-lane box, arrives as
# rc 4 and was reported as permanent. A lane obeying that relaunches a LIVE gate: two gates
# on one summary path. The same line quoted the reader's own "This is NOT a stall … Re-read."
# verbatim inside a sentence saying do not retry, i.e. asserted both halves of a
# contradiction. So this tool now makes exactly the binary distinction it can actually
# support — the reader says COMPLETE, or it does not — and adds no verdict of its own.
# Subtraction cannot introduce a false PASS.
#
# WHAT THIS IS NOT. It is not a gate-of-record verdict and cannot be made into one:
# `--mode record` is a NAMED REFUSAL naming `scripts/flow/premerge-assert.sh`, which
# already owns that grammar (it binds the certified sha, requires exactly one full block,
# and refuses the PARTIAL token exactly) — pinned by scripts/tests/test_premerge_assert.sh.
# Re-implementing it here would be a second place for it to drift. And `--mode` is not
# merely VALIDATED here: it is ENFORCED against the artifact, or declaring `--mode only`
# while pointing at a lite summary would defeat every refusal below.
#
# THE TWO REFUSALS CARRY DIFFERENT CODES ON PURPOSE. `--mode lite` is a malformed REQUEST —
# the caller asked this tool for a verdict it does not give — so it is USAGE (64), which no
# amount of different input fixes. A LITE opener under `--mode only` is a well-formed
# request about an ARTIFACT that cannot answer it, which is a failed MEASUREMENT (4): the
# same request against the right summary succeeds.
#
# THE OUTPUT INVARIANT — AND IT COVERS `--help` (#3312)
# ----------------------------------------------------
# Every line this script prints, on stdout and stderr AND from `--help`, begins
# `gate-verdict: `, and no output ever spells a bare terminal verdict in the gate's own
# `RESULT` form, nor an AGENT-GATE block marker. Run context is rendered
# `run-result=<TOKEN>`. Reason: this tool's output gets pasted, and a line spelling the
# gate's verdict form would MATCH the documented gate-of-record completion probe — the
# artifact becoming the credential. That is why THIS HEADER never spells those literals
# either: `--help` prints it, so a header that spelled them WOULD BE the emitted token,
# which is CLAUDE.md #3312 instance 2 verbatim.
#
# Usage:
#   bash scripts/gate-component-verdict.sh <summary-file> --mode only \
#        --component <name> [--run-id <id>] [--heartbeat <path>]
#
# --run-id BINDS THE ANSWER TO A RUN (#2874). Pass it whenever you know it: without it a
# block left by a CONCURRENT PEER in the same checkout answers about the peer's gate.
#
# THE THREE DOCUMENTED TEXT-COMPLETION GRAMMARS, ONE PER RUN MODE (fallback only; prefer
# the exit status). Quoted in their ANCHORED, token-terminated form, which is also the only
# form safe to print here:
#   record (full / --lite):            grep -qE '^RESULT: (PASS|FAIL)([[:space:]]|$)'
#   only   (--only <component>):       grep -qE '^RESULT: (PASS|FAIL|PARTIAL)([[:space:]]|$)'
#   delta  (--delta <anchor>):         grep -qE '^RESULT: (PASS|FAIL|PARTIAL|ERROR|REFUSED)([[:space:]]|$)'
# Unanchored, the first also matches a PASSENGER token, the second a PARTIALLY token and the
# third an ERRORS token — a spelling check masquerading as a state check. The record grammar
# must keep REFUSING the PARTIAL token, and it does; it refuses ERROR and REFUSED too.
#
# WHY --delta NEEDS ITS OWN, AND WHY IT IS NOT IN THE RECORD ONE (#3750 review round 3).
# `--delta` can terminate with ERROR (4 emit sites in `run_delta`) or REFUSED (3 more, via
# `emit_summary "$(_tree_result REFUSED)"` — which is why a grep for `emit_summary REFUSED`
# finds nothing and the token looks unemitted; it IS emitted, and gate-liveness.sh's comment
# enumerating it is accurate, not stale). Both are `--delta`-ONLY: every one of the seven
# sites is inside `run_delta`, and a full gate emits only PASS or FAIL. So a `--delta` poller
# using the RECORD grammar HANGS FOREVER on a terminal outcome — #3750's own defect class in
# a third mode. Record therefore stays exactly PASS|FAIL (widening it would weaken the
# gate-of-record probe for nothing, and that refusal is load-bearing), and `--delta` gets its
# own set.
#
# ONE SOURCE OF TRUTH: the delta set is gate-liveness.sh's ALREADY-ENUMERATED terminal set,
# token for token, rather than a second independent list — so "what is terminal" is decided
# in one place. It therefore carries PARTIAL (which `--delta` cannot itself emit; that is the
# `--only` demotion) and REFUSED, which is the reader's DEFENSIVE set; ERROR is the emit a
# `--delta` run is most likely to hand you. Better than any of the three: ASK the reader,
# which is that single source of truth executable instead of transcribed.
#
# AND WHY WIDENING A COMPLETION GRAMMAR IS SAFE HERE, WHICH IT WOULD NOT HAVE BEEN BEFORE:
# matching ERROR/REFUSED as COMPLETION cannot create a false pass, because completion and
# verdict are now SEPARATE assertions — the verdict is an affirmative read of the PASS token
# exactly (premerge-assert.sh) or of the component's own line (this script). Before that
# separation the two questions shared one token, and widening would have been dangerous.
# This fix is thus ENABLED by the change it was a finding against, which is the reason three
# completion grammars are not three chances to be wrong.
set -uo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
LIVENESS="$HERE/gate-liveness.sh"
# The gate's OWN component set, as data (#3544). Resolved from THIS script's directory with
# NO env override: the constrained party must not choose its own authority (#3312), and a
# settable path is one more thing a caller can point at a list of its own choosing.
MANIFEST="$HERE/agent-gate.components"

SUMMARY=""; MODE=""; COMPONENT=""; WANT_RUN_ID=""; HB=""

# Every runtime emit goes through these, so the anchor and the no-verdict-form rule hold
# for every path rather than at each printf site (CLAUDE.md #3312: an invariant over
# OUTPUT needs a check on the OUTPUT PATH). DISPLAY-ONLY: every decision is made on the
# raw value before any renderer runs. Control characters are stripped because git permits
# newlines in paths and a diagnostic quoted back from the shared reader can carry
# anything — an unsanitised value emits a line with NO prefix and breaks the anchor
# everything else rests on.
# ORDER IS LOAD-BEARING: CONTROLS ARE STRIPPED **FIRST**, THEN TOKENS ARE DEFUSED.
# Defusing first lets a token SPLIT BY A CONTROL CHARACTER (`RES<0x01>ULT: PASS`) survive the
# defuse untouched and then be REASSEMBLED by the strip that follows — so the output
# invariant would hold only by an argument about which values can reach the renderer, rather
# than structurally. Swapping the stages costs nothing and removes the argument.
_safe() {  # <text> -> controls stripped, THEN pastable gate tokens defused
  printf '%s' "$1" \
    | tr '\n\r\t' '   ' | tr -d '\000-\010\013\014\016-\037\177' \
    | sed -e 's/RESULT:/RESULT(defused)/g' -e 's/==== AGENT-GATE/====(defused) AGENT-GATE/g'
}
say()  { printf 'gate-verdict: %s\n' "$(_safe "$1")"; }
sayerr(){ printf 'gate-verdict: %s\n' "$(_safe "$1")" >&2; }

# verdict <TOKEN> <exit> <text> — the single terminal emit.
verdict() {
  local tok="$1" rc="$2" txt="$3"
  say "$tok $txt"
  [ -n "$SUMMARY" ] && say "summary: $SUMMARY"
  exit "$rc"
}
usage_refusal() { sayerr "USAGE $1"; exit 64; }
cnm()      { verdict COULD-NOT-MEASURE 4 "${COMPONENT:-<no component>} ($1)"; }
notpass()  { verdict NOT-PASS 1 "$COMPONENT ($1)"; }

# --help goes through the SAME anchor as every verdict. It deliberately does NOT go
# through _safe: the header is committed source, not a runtime value, and defusing would
# MANGLE the two grammars it exists to teach. The header itself is written so the
# invariant holds without defusing.
_print_help() {
  awk 'NR>1 { if ($0 !~ /^#/) exit; print }' "$0" \
    | while IFS= read -r _l; do printf 'gate-verdict: %s\n' "$_l"; done
}

# _need_val <opt> <remaining-argc>: an option's missing value must be an ANCHORED USAGE
# refusal at 64. `${2:?…}` exits 1 with an unanchored bash diagnostic, i.e. the one output
# path that escaped the invariant this script documents.
_need_val() { [ "$2" -ge 2 ] || usage_refusal "option '$1' requires a value; see --help"; }

while [ $# -gt 0 ]; do
  case "$1" in
    --mode)      _need_val --mode      $#; MODE="$2";        shift 2 ;;
    --component) _need_val --component $#; COMPONENT="$2";   shift 2 ;;
    --run-id)    _need_val --run-id    $#; WANT_RUN_ID="$2"; shift 2 ;;
    --heartbeat) _need_val --heartbeat $#; HB="$2";          shift 2 ;;
    -h|--help)   _print_help; exit 0 ;;
    -*)          usage_refusal "unknown option '$1'" ;;
    *)           if [ -n "$SUMMARY" ]; then usage_refusal "unexpected extra argument '$1'"; fi
                 SUMMARY="$1"; shift ;;
  esac
done

[ -n "$SUMMARY" ] || usage_refusal "a summary-file path is required; see --help"

# THE ACCEPTED-VERDICT SET IS A PARAMETER OF THE RUN MODE (#3750), never implicit and
# never one grammar serving both. The modes this tool does not serve are refusals that
# NAME their authority, so a caller is routed rather than left to improvise — which is how
# the record grammar got improvised as prefix greps in the first place. The mode is
# ENFORCED against the artifact further down; validating it here and never reading it
# again is what let `--mode only` answer about a lite block.
case "$MODE" in
  only) ;;
  "")   usage_refusal "--mode is required (only|record|lite|delta). The accepted-verdict set is a parameter of the run MODE, so it is never implicit; see --help" ;;
  record)
        usage_refusal "--mode record is not this tool's verdict to give. The gate-of-record grammar is owned by scripts/flow/premerge-assert.sh (it binds the certified sha, requires exactly one full block, and refuses the PARTIAL verdict token). A component line is NOT a certification" ;;
  lite)
        usage_refusal "--mode lite is a different claim entirely: a lite PASS is silent about 32 of the full gate's components, and its clippy is per-package scoped, so a lite block answers only for LITE_COMPONENTS. Read the LITE block, and never treat it as the gate of record" ;;
  delta)
        usage_refusal "--mode delta is not this tool's verdict to give: a delta re-certification is bound to its anchor's full PASS, which scripts/flow/premerge-assert.sh checks (Case B)" ;;
  *)    usage_refusal "unknown --mode '$MODE'; the closed set is only|record|lite|delta and only 'only' is served here" ;;
esac

[ -n "$COMPONENT" ] || usage_refusal "--mode only requires --component <name>"
# CLOSED NAME GRAMMAR, matching scripts/agent-gate.components' own: a name is
# [A-Za-z0-9._-]+ and may not start with `-`. Refusing anything else is what keeps the
# name out of the regex as metacharacters, so the pattern below cannot be steered by it.
#
# GLOB, NOT `grep -E`. A line-based check validates a name CONTAINING A NEWLINE, and the
# name is then interpolated into a grep pattern where each line becomes its own
# alternative — a bare `^fmt` among them. That only failed to produce a PASS by accident,
# and safe-by-accident is not safe. `case` matches the WHOLE value, newlines included.
case "$COMPONENT" in
  [A-Za-z0-9]*) ;;
  *) usage_refusal "a component name must begin with a letter or digit (see scripts/agent-gate.components)" ;;
esac
case "$COMPONENT" in
  *[!A-Za-z0-9._-]*)
    usage_refusal "component name is outside the closed grammar [A-Za-z0-9._-]+ (see scripts/agent-gate.components)" ;;
esac
# AND THE NAME MUST BE A REAL COMPONENT, not merely a syntactically legal one (F6-2).
# Validating only the SHAPE let a METADATA line masquerade as a component line: a block
# carrying `launch-nonce: PASS (1s)  [x]` answered `PASS launch-nonce`, though no such
# component ran. `launch-nonce`'s value comes from AGENT_GATE_LAUNCH_NONCE, which the CALLER
# sets — so the planter is the asker, i.e. INVOKER-CLASS and out of the threat model by this
# repo's own triage rule. It is enforced anyway for two practical reasons, stated rather than
# dressed up as a vulnerability: the membership test costs nothing because the manifest
# already exists, and it turns a TYPO into a NAMED REFUSAL instead of a silent
# COULD-NOT-MEASURE that reads like a broken summary.
#
# NOT A SECOND LIST: `scripts/agent-gate.components` IS the gate's own component set as data,
# and the gate asserts it against its running COMPONENTS array on every run (fail-closed
# `manifest-stale`, #3544). Parsed with THAT file's own closed grammar — one name per line,
# blank lines and `#` comments skipped, NO TRIMMING, because a parser that trims is a parser
# that guesses.
#
# CANNOT FALSE-FAIL, measured: every component-name source reachable in a FULL-marker block
# is a COMPONENTS member, and the three non-manifest names (`scoped-tests`,
# `node-tests`, `shell-selftests`) are emitted only by run_lite/run_delta paths, whose
# LITE/DELTA markers the mode check below refuses outright.
#
# A REFUSAL, NOT A GUESS: no fuzzy matching and no "did you mean", which would invite acting
# on a name the caller did not ask for.
if [ ! -r "$MANIFEST" ]; then
  # FAIL-CLOSED. A membership test that silently stops testing when its list is missing is
  # exactly the permissive branch this script exists to refuse.
  usage_refusal "the component manifest is not readable at $MANIFEST, so '$COMPONENT' cannot be confirmed to be a real component — refusing rather than skipping the check"
fi
_in_manifest=0
while IFS= read -r _mn || [ -n "$_mn" ]; do
  case "$_mn" in ''|'#'*) continue ;; esac
  [ "$_mn" = "$COMPONENT" ] && { _in_manifest=1; break; }
done < "$MANIFEST"
if [ "$_in_manifest" -ne 1 ]; then
  usage_refusal "'$COMPONENT' is not a component: it is absent from the gate's own component manifest ($MANIFEST, which the gate asserts against its running component set every run). A component-shaped METADATA line is not a component verdict, and a typo is refused by name rather than answered"
fi
# `.` is the one accepted character that is also a regex metacharacter. Escape it rather
# than trusting it to match itself.
COMP_RE=$(printf '%s' "$COMPONENT" | sed 's/\./[.]/g')

[ -n "$HB" ] || HB="$SUMMARY.heartbeat"

# _count_re <extended-regex> <file> — echo the match count; return 2 on a FAILED grep.
# `grep -c` returns 1 for a zero count, which is not an error; anything >= 2 is, and
# folding that onto "no match" would be an affirmative claim from a failed measurement.
_count_re() {
  local n rc
  n=$(grep -cE "$1" "$2" 2>/dev/null); rc=$?
  [ "$rc" -ge 2 ] && return 2
  printf '%s' "${n:-0}"
  return 0
}
# _RESERVED_LINES — EVERY reserved `name:` line this reader consults, enumerated in ONE place
# so a future one cannot join without going through the two-step below:
#
#     summary-integrity   tree-integrity   mode   RESULT   <the component row>
#
# `run-id:` is deliberately NOT here: this script never reads it — gate-liveness.sh owns that
# binding (#2874) — and the two block MARKERS are not `name:` lines, so they keep their own
# exact anchored regex, where a malformed marker already refuses.
#
# THE TWO-STEP, AND WHY STEP 1 EXISTS AT ALL. Every recognizer in this file used to COUNT its
# line with the FULL accepted grammar, so a line that was PRESENT BUT MALFORMED matched
# nothing and the block read as if the line were not there — and ABSENT is the permissive
# branch. Measured, three times over, as literal false PASSes: `summary-integrity:FAIL`
# (no space) was invisible; a valid `tree-integrity: PASS` beside a malformed
# `tree-integrity:FAIL` counted as one; a valid component row beside a malformed
# same-component row counted as one. So:
#
#   STEP 1  count by BARE PREFIX `^<name>:`, making NO assumption about what follows;
#   STEP 2  validate that single line against the exact accepted grammar, and treat
#           MALFORMED as its OWN named refusal — never as absence.
#
# This is the affirmative-measurement rule applied to the RECOGNIZER rather than to the
# verdict: key the permissive branch on the AFFIRMATIVE value, never on `!= <bad>`. The class
# had appeared FIVE times here, once introduced by the round that was fixing it, which is why
# the sweep is uniform rather than per-instance.

# _count_prefix <name> <file> — STEP 1. Same tri-state contract as _count_re: echoes the
# count, returns 2 on a FAILED grep, because a failed measurement is never an absence either.
_count_prefix() { _count_re "^$1:" "$2"; }

# _reserved_token <name> <file> — STEP 2. Echoes the line's token, or returns 1 when the line
# is MALFORMED (present by prefix, but not `<name>:<whitespace><token>`). Every caller turns
# that non-zero into its own named refusal.
_reserved_token() {
  local v
  v=$(_key_token "$1" "$2") || return 1
  [ -n "$v" ] || return 1
  printf '%s' "$v"
}

# _key_token <key> <file> — the FIRST whitespace-delimited token after `<key>: `, which is
# how premerge-assert.sh compares these same lines. Token-exact by construction, so a
# trailing detail (`PASS (lockfile-settled: …)`) does not change the verdict and a longer
# spelling (`PASSENGER`) is not accepted as the shorter one.
_key_token() {
  sed -n "s/^$1:[[:space:]]\{1,\}\([^[:space:]]\{1,\}\).*/\1/p" "$2" | head -1
}

# ---------------------------------------------------------------------------
# ONE READ, ONE SNAPSHOT, BOTH ASSERTIONS.
#
# The summary is a SHARED path that agent-gate.sh rewrites in place with `>`, so a
# per-question re-open can sample two different versions of the file — one run's
# `run-id:` combined with another run's component line. Both the completion question
# (delegated to gate-liveness.sh) and the verdict question below are answered from the
# SAME snapshot, so the two can never disagree about which run they read.
#
# THE BYTES ARE SNAPSHOTTED UNCHANGED, AND A SUMMARY CARRYING ESCAPES IS REFUSED — NOT
# NORMALISED (#3750 review round 6, F6-1).
#
# This used to strip ANSI here, and NORMALISE-THEN-VALIDATE MANUFACTURES EVIDENCE:
# `RESULT: P<ESC>[31mASS` strips to exactly `RESULT: PASS`, and so does a split
# `tree-integrity:` token or a split component status. It is R2-1 with the arrow reversed —
# that one defused gate tokens BEFORE stripping controls, reassembling a FORBIDDEN token on
# the way OUT; this reassembled a VALID one on the way IN. Measured as three literal false
# PASSes before the fix.
#
# THE #3400 TENSION, RESOLVED HERE SO NOBODY "RESTORES" THE STRIP ON DOCTRINE GROUNDS:
# #3400 mandates colour-immunity at the parse site, and ITS SUBJECT IS CARGO OUTPUT — which
# really is coloured, and whose colour survives redirection to a file. A SUMMARY BLOCK IS THE
# GATE'S OWN ARTIFACT, written with plain `echo`/`printf` and never coloured. So for a
# SUMMARY parse, stripping buys nothing and can only manufacture tokens: colour-immunity for
# cargo output, REFUSAL for our own artifact.
#
# A summary carrying an escape is therefore not a summary this gate wrote, and the honest
# answer is a NAMED REFUSAL rather than a repair. TAB, LF and CR are TOLERATED: the
# feature-matrix annotation is free text derived from a real cargo argv, and with nothing
# stripped they cannot splice a token, so refusing them would be a false FAIL bought for
# nothing.
# ---------------------------------------------------------------------------
# The mktemp failure goes through the NORMAL verdict path like every other cause, so it
# lands on stdout with the `summary:` line: a caller parsing verdicts must not have to read
# a second stream for one branch.
# `mktemp`'s OWN stderr is CAPTURED, not inherited, so the one branch that cannot route
# through the sanitizer can no longer emit an unprefixed line ahead of the anchored response.
# Its text is reported THROUGH the sanitized path rather than discarded — the diagnostic is
# the useful part of this failure, and the invariant is that every line carries the anchor,
# not that nothing is said. Same family as the round-6 `--help` finding.
#
# CAPTURED INTO A VARIABLE, NOT A FILE, and that is the whole trick: the first attempt wrote
# the stderr to a sibling under `$TMPDIR` — the very directory that is missing in the failure
# this handles — so the redirect itself failed and bash emitted the exact unanchored line the
# change was removing. `2>&1` inside the substitution needs no directory to exist. On success
# mktemp prints only the path, so the variable IS the path.
_mkout=$(mktemp -d "${TMPDIR:-/tmp}/gate-component-verdict.XXXXXX" 2>&1) \
  || cnm "snapshot-unavailable; a scratch directory for the one-read snapshot could not be created under ${TMPDIR:-/tmp}${_mkout:+ — mktemp: $_mkout}"
SNAPDIR="$_mkout"
trap 'rm -rf "$SNAPDIR"' EXIT
SNAP="$SNAPDIR/summary"
BLOCK="$SNAPDIR/block"

if [ ! -e "$SUMMARY" ]; then
  cnm "summary-absent; no file at that path — the run may not have started, or the path is wrong"
fi
if [ ! -f "$SUMMARY" ] || [ ! -r "$SUMMARY" ]; then
  cnm "summary-unreadable; not a readable regular file"
fi
if ! cat "$SUMMARY" > "$SNAP" 2>/dev/null; then
  cnm "summary-unreadable; could not snapshot the summary"
fi
# ESC plus every other C0 control except TAB/LF/CR. `tr -d` then a size compare, so the test
# needs no regex over binary and cannot itself be steered by the content.
if [ "$(tr -d '\000-\010\013\014\016-\037\177' < "$SNAP" | wc -c)" \
     != "$(wc -c < "$SNAP")" ]; then
  cnm "summary-carries-escapes; this summary contains an escape or control sequence, so it is not an artifact this gate wrote — REFUSED rather than normalised, because stripping the sequence would splice a split token into a valid one (a manufactured verdict). Colour-immunity (#3400) is for CARGO output; a summary block is the gate's own plain-text artifact"
fi

# ---------------------------------------------------------------------------
# ASSERTION 1 — COMPLETION, by ASKING the shared reader (roborev job 172: one
# implementation, one grammar). gate-liveness.sh already enumerates the terminal set
# from agent-gate.sh, requires the block's end marker (a truncated artifact is
# permanent and must never be believed), and enforces the #2874 run-id binding. A
# second grep here would be a second place for all three to drift.
#
# It is pointed at the SNAPSHOT with the REAL heartbeat path, so it reads exactly the
# bytes the verdict below is read from. `--no-wait` because a non-terminal block is
# non-answerable whichever non-terminal state it is in, so the stall-confirmation sleep
# would buy nothing. That rationale holds PRECISELY BECAUSE no retryability claim is
# derived from the reader's rc — the descoped taxonomy falsified it, and removing the
# taxonomy makes it true again.
#
# ONLY ITS `COMPLETE` ANSWER IS READ (exit 0). Its other codes are deliberately NOT
# interpreted here: this script draws no distinction it cannot support, and describing the
# STALLED branch would be a claim about a mechanism `--no-wait` makes UNREACHABLE anyway
# (the reader returns UNKNOWN from its `confirmation-skipped` arm instead).
# ---------------------------------------------------------------------------
if [ ! -r "$LIVENESS" ]; then
  cnm "reader-absent; the shared completion reader is not readable at $LIVENESS, so completion cannot be established — and this script deliberately re-implements neither the terminal grammar nor the run-id binding"
fi
declare -a _gl_args=("$SNAP" --heartbeat "$HB" --no-wait)
[ -n "$WANT_RUN_ID" ] && _gl_args+=(--run-id "$WANT_RUN_ID")
GL_OUT=$(bash "$LIVENESS" "${_gl_args[@]}" 2>&1); GL_RC=$?
if [ "$GL_RC" -ne 0 ]; then
  # Name the SUMMARY rather than the private snapshot the reader was handed: that path is
  # gone by the time anyone reads this line. Exact literal substitution, never a regex —
  # TMPDIR is caller-controlled and would arrive as a pattern.
  _gl_first=$(printf '%s\n' "$GL_OUT" | head -1)
  _gl_first="${_gl_first//"$SNAP"/"$SUMMARY"}"
  _gl_first="${_gl_first//"$SNAPDIR"/(snapshot)}"
  # ONE code, and NO verdict of our own about liveness. The reader's answer is quoted
  # VERBATIM and its rc is named, so a caller that needs the liveness distinction asks the
  # authority (which is polled, and which this tool is not) instead of reading an opinion
  # out of an exit code that cannot carry one. See the DESCOPE note in the header.
  cnm "run-not-terminal; the shared reader did not report a terminal verdict for this request, so no component verdict can be read — gate-liveness.sh is the liveness authority and this is not a completion probe — gate-liveness.sh: ${_gl_first:-no answer} [rc=$GL_RC]"
fi

# ---------------------------------------------------------------------------
# BOUND EVERY REMAINING READ TO THE VALIDATED BLOCK.
#
# gate-liveness.sh:143-149 DECLARES its own residual: its structure check constrains the
# COUNTS and ORDERING of opener / closer / RESULT / run-id, and NEVER that no lines sit
# OUTSIDE the span. A stale tail from a previous write to the same path is therefore
# inside the FILE and outside the BLOCK, and a whole-file grep returns it as this run's
# verdict — a false PASS in a tool whose entire subject is the vacuous pass.
#
# The reader decides VALIDITY; this decides only EXTENT, which it cannot delegate because
# it needs line numbers to slice with. A non-unique or inverted extent is a REFUSAL, never
# a guess at which block was meant.
# ---------------------------------------------------------------------------
_OPEN_RE='^==== AGENT-GATE( LITE| DELTA)? SUMMARY ====$'
_CLOSE_RE='^==== END AGENT-GATE( LITE| DELTA)? SUMMARY ====$'
_n_open=$(_count_re "$_OPEN_RE" "$SNAP")  || cnm "block-extent-unmeasurable; the opener scan failed"
_n_close=$(_count_re "$_CLOSE_RE" "$SNAP") || cnm "block-extent-unmeasurable; the closer scan failed"
if [ "$_n_open" != 1 ] || [ "$_n_close" != 1 ]; then
  cnm "block-extent-not-unique; the file holds $_n_open opener(s) and $_n_close closer(s), so no read can be bounded to one block — a concurrent or appended write, and which block is this run's cannot be established"
fi
_o=$(grep -nE "$_OPEN_RE"  "$SNAP" | head -1 | cut -d: -f1)
_c=$(grep -nE "$_CLOSE_RE" "$SNAP" | head -1 | cut -d: -f1)
case "$_o$_c" in *[!0-9]*|"") cnm "block-extent-unmeasurable; the block's line numbers could not be read" ;; esac
if [ "$_o" -ge "$_c" ]; then
  cnm "block-extent-inverted; the closer at line $_c precedes the opener at line $_o"
fi
if ! sed -n "${_o},${_c}p" "$SNAP" > "$BLOCK" 2>/dev/null; then
  cnm "block-extent-unmeasurable; the block could not be sliced out of the snapshot"
fi

# ---------------------------------------------------------------------------
# AND THE BLOCK STILL HAS A TAIL — so every META read stops at `RESULT:` (F5).
#
# A RESIDUAL OF B1'S OWN FIX, and the same class as it: B1 bounded reads to the BLOCK,
# and a line sitting between `RESULT:` and the closing marker is inside the block and
# AFTER the verdict. A stale or injected component line there was being accepted as this
# run's verdict.
#
# MEASURED rather than assumed: every one of the shipped gate's nine `echo "RESULT: …`
# writes is IMMEDIATELY followed by `$SUMMARY_END_MARKER`, so nothing legitimate is ever
# written between them and truncating there cannot drop real content. The suite asserts
# that premise BY DERIVATION over agent-gate.sh (case 19.3), so it cannot rot silently.
#
# $BODY is what every META read uses — the integrity lines, the `--only` scope line and
# the component lines. Only the RESULT token itself is read from $BLOCK, since that is the
# line $BODY stops at.
BODY="$SNAPDIR/body"
_res_ln=$(grep -n '^RESULT: ' "$BLOCK" | head -1 | cut -d: -f1)
case "$_res_ln" in
  ''|*[!0-9]*) cnm "block-body-unmeasurable; the block's RESULT line number could not be read, so the meta section cannot be bounded" ;;
esac
if [ "$_res_ln" -le 1 ]; then
  cnm "block-body-empty; the block's RESULT line is at line $_res_ln, leaving no meta section to read"
fi
if ! sed -n "1,$(( _res_ln - 1 ))p" "$BLOCK" > "$BODY" 2>/dev/null; then
  cnm "block-body-unmeasurable; the meta section could not be sliced out of the block"
fi

# ---------------------------------------------------------------------------
# ENFORCE THE MODE AGAINST THE ARTIFACT (not merely against the flag).
#
# `--only` uses the FULL-gate markers (only --lite/--delta swap them, agent-gate.sh's
# marker block), so a LITE or DELTA opener means the caller declared `--mode only` and
# pointed at a different claim. Answering it returns, for instance, --lite's PER-PACKAGE
# SCOPED clippy as a component verdict — exactly the misreading the `--mode lite` refusal
# above exists to prevent.
# ---------------------------------------------------------------------------
_opener=$(sed -n "${_o}p" "$SNAP")
case "$_opener" in
  '==== AGENT-GATE SUMMARY ====') ;;
  *' LITE '*)
    cnm "wrong-block-for-mode; this is a LITE block, and --mode only answers about a full-marker run. A lite PASS is silent about 32 of the full gate's components and its clippy is per-package scoped, so a lite component line is a DIFFERENT claim — read the LITE block directly" ;;
  *' DELTA '*)
    cnm "wrong-block-for-mode; this is a DELTA block, and --mode only answers about a full-marker run. A delta re-certification is bound to its anchor's full PASS — scripts/flow/premerge-assert.sh (Case B) is the authority" ;;
  *)
    cnm "wrong-block-for-mode; the opener at line $_o is not a recognised summary marker" ;;
esac

# ---------------------------------------------------------------------------
# THE INTEGRITY LINES INVALIDATE EVERY COMPONENT IN THE BLOCK.
#
# A mutated-mid-run run stamps a FAILing `tree-integrity:` line and a FAIL verdict while
# the component line still reads PASS (#2926); a side-lane summary clobber stamps
# `summary-integrity:` the same way (#2874). Emitting PASS from such a block would report
# a verdict the gate itself declared non-certifying. This is the ONE exception to "a
# sibling component's failure says nothing about mine": these lines are about the WHOLE
# block.
#
# Checked BEFORE the component read, because they are the stronger statement and the more
# actionable cause. Token compare exactly as premerge-assert.sh does it.
# ---------------------------------------------------------------------------
# MEASURED FIRST, ACTED ON BELOW (#3951). The two integrity lines are INDEPENDENT
# declarations, and the tree-integrity branch returns EARLY — so with the summary-integrity
# line read only afterwards, an UNCERTAIN tree verdict (SKIP / PENDING / absent / ambiguous /
# unrecognised) refused with COULD-NOT-MEASURE and never consulted a `summary-integrity: FAIL`
# sitting beside it, which the header documents as NOT-PASS. NEVER LET A "CANNOT TELL" MASK A
# "DEFINITELY NOT": `summary-integrity: FAIL` is affirmatively established and block-wide
# (#2874 — a foreign run-id clobbered the path mid-run), so it outranks an unmeasured tree
# reading. This is a MEASUREMENT only: no verdict is taken here, so the ORDER of the verdicts
# below is unchanged and `tree-integrity: FAIL` keeps its own, already-certain cause.
#
# Neither ordering can produce a false PASS — every branch involved is a refusal — so what
# this buys is the PRECISION of the refusal, and a header that agrees with its code.
_n_si=$(_count_prefix summary-integrity "$BODY") \
  || cnm "summary-integrity-unmeasurable; the scan for the summary-integrity line failed"
# STEP 2 is deferred to the verdict sites below, NOT taken here, so the ORDER of the verdicts
# is unchanged and `tree-integrity: FAIL` keeps its own already-certain cause (#3951).
# `_si_malformed=1` records "present by prefix, ungrammatical" as a THIRD state, distinct from
# both a readable token and absence.
_si=""; _si_malformed=0
if [ "$_n_si" -eq 1 ]; then
  _si=$(_reserved_token summary-integrity "$BODY") || { _si=""; _si_malformed=1; }
fi

# _si_preempt <what-was-unmeasured>: promote an UNCERTAIN tree refusal to the certain
# summary-integrity one, and ONLY on an unambiguous FAIL. A duplicated or unrecognised
# summary-integrity line is itself unmeasurable, so it promotes nothing — ambiguity is never a
# guess in either direction. Returns (no-op) when there is nothing affirmative to promote.
_si_preempt() {
  [ "$_n_si" -eq 1 ] && [ "$_si" = FAIL ] || return 0
  notpass "summary-integrity FAIL — a mid-run summary clobber was detected (#2874), so the block is non-certifying and NO component verdict in it can be read as a pass. Reported IN PREFERENCE to the tree-integrity reading, which was itself UNMEASURED here ($1): an affirmatively established failure outranks a cannot-tell (#3951)"
}

_n_ti=$(_count_prefix tree-integrity "$BODY") \
  || cnm "tree-integrity-unmeasurable; the scan for the tree-integrity line failed"
if [ "$_n_ti" -eq 0 ]; then
  _si_preempt "the block carries no tree-integrity line"
  cnm "tree-integrity-absent; the block carries no tree-integrity line, so whether the tree was stable during the run cannot be established — never assumed benign (#2926)"
fi
if [ "$_n_ti" -gt 1 ]; then
  _si_preempt "the block carries $_n_ti tree-integrity lines"
  cnm "tree-integrity-ambiguous; the block carries $_n_ti tree-integrity lines, and ambiguity is never resolved in favour of PASS"
fi
_ti=$(_reserved_token tree-integrity "$BODY") || {
  # PRESENT BY PREFIX, UNGRAMMATICAL. Its own refusal, and NOT the `absent` one: those are
  # different facts and only one of them is actionable.
  _si_preempt "the tree-integrity line is present but ungrammatical"
  cnm "tree-integrity-malformed; the block carries a tree-integrity line that is not '<key>: <token>', so its verdict cannot be read — reported as MALFORMED rather than absent, because a line the gate could not have emitted is not the same fact as no line at all"
}
case "$_ti" in
  PASS) ;;
  FAIL)
    notpass "tree-integrity FAIL — the gate declared this run NON-CERTIFYING (a mid-run tree mutation invalidates EVERY component in the block, #2926), so NO component verdict in this block can be read as a pass — including this one, whose own line this check deliberately has not read" ;;
  SKIP|PENDING)
    _si_preempt "tree-integrity read '$_ti'"
    cnm "tree-integrity '$_ti' — the tree check never ran (SKIP) or the run never reached its terminal emit (PENDING), so tree stability is UNMEASURED. Deliberately NOT a FAIL: an unmeasured check is not a failed one" ;;
  *)
    _si_preempt "the tree-integrity token '${_ti:-<unreadable>}' is outside its closed set"
    cnm "tree-integrity token '${_ti:-<unreadable>}' is outside the closed set PASS|FAIL|SKIP|PENDING, so it is never read as a pass" ;;
esac
# The tree verdict is PASS by the time we reach here, so this is the summary-integrity line's
# own verdict, read from the values measured above (#3951: measured once, acted on twice).
if [ "$_n_si" -gt 1 ]; then
  cnm "summary-integrity-ambiguous; the block carries $_n_si summary-integrity lines"
fi
if [ "$_si_malformed" -eq 1 ]; then
  # STEP 2's refusal, and NOT the "unrecognised token" one below: a line that is present by
  # prefix but ungrammatical was the measured false PASS (`summary-integrity:FAIL`, no space,
  # read as if the line were absent), so it is named for what it is.
  cnm "summary-integrity-malformed; the block carries a summary-integrity line that is not '<key>: <token>', so the clobber declaration cannot be read — reported as MALFORMED rather than absent, because the gate emits this line ONLY on detection and its presence is never benign (#2874)"
fi
if [ "$_n_si" -eq 1 ]; then
  # agent-gate.sh emits this line ONLY on detection, and only ever with a FAIL token — so
  # its mere PRESENCE is the non-certifying signal. An unexpected token is still not a pass.
  case "$_si" in
    FAIL) notpass "summary-integrity FAIL — a mid-run summary clobber was detected (#2874), so the block is non-certifying and NO component verdict in it can be read as a pass — including this one, whose own line this check deliberately has not read" ;;
    *)    cnm "summary-integrity token '${_si:-<unreadable>}' is unrecognised; the gate emits this line only on detection, so its presence is never benign" ;;
  esac
fi

# THE READER GAVE US COMPLETION; THE MODE FILTER IS OURS (#3750 review round 4, F1).
# gate-liveness.sh's terminal set is MODE-INVARIANT BY DESIGN — it answers "is there a
# verdict", not "for which mode" — so it accepts `--delta`'s ERROR and REFUSED. THE READER
# IS RIGHT TO BE MODE-INVARIANT AND THIS SCRIPT IS RIGHT TO BE MODE-SPECIFIC: that is the
# same completion/verdict division one level down, and delegating the first does not
# delegate the second. So the token is required to be IN the set this script PUBLISHES for
# `--mode only` (see the three grammars in the header) — keyed on the AFFIRMATIVE value,
# never on "not one of the bad ones", which is the one rule this whole script is built on.
#
# Probably unreachable today: the gate emits ERROR/REFUSED only from `run_delta`, whose DELTA
# opener the mode check above already refuses. Enforced anyway — "unreachable today" is an
# argument someone has to re-derive, not a property the code holds.
RUN_TOKEN=$(_key_token RESULT "$BLOCK")
case "$RUN_TOKEN" in
  PASS|FAIL|PARTIAL) ;;
  *) cnm "run-token-outside-mode-set '${RUN_TOKEN:-<unreadable>}'; --mode only accepts PASS|FAIL|PARTIAL, the set this script publishes for that mode. ERROR and REFUSED are --delta-only terminal tokens, which the shared completion reader accepts because ITS set is mode-invariant by design — filtering it for this mode is this script's job, not the reader's" ;;
esac

# ---------------------------------------------------------------------------
# ASSERTION 2 — THE VERDICT, from the component's OWN line, inside the block.
#
# The line shape is agent-gate.sh's `_fm_summary_line`:
#   printf '%-18s %s (%s)  %s' "<name>:" "<STATUS>" "<secs>s" "<annotation>"
# so a component line is `^<name>: +<STATUS> (<digits>s)`. The `(<digits>s)` field is
# what STRUCTURALLY distinguishes a component line from a META line: `tree-integrity:
# PASS` and `component-set: PASS (36/36 vs …)` carry no duration, so a mistyped or
# non-component name cannot return a confident PASS. Derived from the emitter's format
# rather than from a second list of component names, which would be a second thing to
# drift.
#
# Read by REDIRECTION into a variable, never a `while read` pipeline whose verdict would
# be discarded in a subshell (#3400), and grep's status is CHECKED: rc >= 2 is a failed
# measurement, not "no match".
# ---------------------------------------------------------------------------
# FULLY ANCHORED, AND REQUIRING THE **ANNOTATED** SHAPE — the round-5 correction.
# Validating only a PREFIX accepted `fmt: PASS (1s) arbitrary text` as a genuine component
# verdict (F2). Anchored at BOTH ends now, AND against ONE shape rather than two:
#
#   annotated    agent-gate.sh's `_fm_summary_line`:          printf '%-18s %s (%s)  %s'
#                -> TWO spaces after the duration, then a non-empty annotation. ACCEPTED.
#   unannotated  agent-gate.sh's `_tree_boundary_meta_lines`: printf '%-18s %s (%ss)\n'
#                -> NOTHING after the duration. REJECTED HERE, because it is unreachable.
#
# ROUND 4 ACCEPTED BOTH AND THAT WAS WRONG — not about the emitter, about the ORDER OF THE
# CHECKS. The unannotated shape is real (this lane's own tree-mutated run emitted
# `tooling-tests:     FAIL (512s)`), but it can never reach this regex. MEASURED from source,
# all four legs: `_tree_boundary_meta_lines` has EXACTLY ONE caller (`_tree_boundary_fail`);
# that caller requires TREE_GUARDED=1, so no SKIP path coexists; it calls
# `_tree_detection_mark` immediately before, whose BOTH arms route to `_tree_fail_closed`,
# which sets `tree-integrity: FAIL`; and `_emit_terminal_summary` names none of
# `_tree_finalize`/`_tree_meta_array`/`TREE_INTEGRITY_LINE`, so nothing resets it to PASS in
# between. So the unannotated shape occurs ONLY in `tree-integrity: FAIL` blocks — which the
# integrity precondition ABOVE rejects before the component read. Accepting it here was DEAD
# permissiveness, the same root cause as the `mode:` skip above.
#
# THE SUITE KEEPS BOTH GUARDS AGAINST GETTING THIS WRONG AGAIN: 17.3 pins the real boundary
# block being rejected ON INTEGRITY (the reachable behaviour), and 17.5 asserts BY DERIVATION
# that the shipped gate still has exactly TWO component-line emitters — so if a third appears,
# or if the boundary emitter ever starts appearing in an integrity-PASS block, it reds rather
# than silently widening what counts as certifying evidence.
#
# DECLARED RESIDUAL: the annotation is FREE TEXT containing spaces (`[test cqlite-core
# --features cli-helpers]`), so garbage APPENDED to an annotation is indistinguishable from
# annotation content and the tail must stay permissive after the two spaces. What the
# anchoring closes is the SINGLE-space tail, which no emitter can produce.
_COMP_LINE_RE="^${COMP_RE}: +[A-Za-z][A-Za-z-]* \([0-9]+s\)  .+$"
# STEP 1 — COUNT BY BARE PREFIX. Counting with the full row grammar was the third measured
# false PASS: one valid `PASS` row plus a same-component row in a malformed or unannotated
# shape left the count at 1, so an artifact that could not have been coherently emitted
# answered PASS.
COMP_N=$(_count_prefix "$COMP_RE" "$BODY") \
  || cnm "component-scan-failed; the scan for this component's line failed, and a failed measurement is never reported as an absence"

# A `PARTIAL` RUN TOKEN **REQUIRES** ITS `--only` SCOPE LINE (F4), and then the component
# must be in it.
#
# The old rule skipped the check when no `mode:` line was present, justified by the
# tree-integrity BOUNDARY emit writing none. THAT JUSTIFICATION IS VOID, and this is the
# same root cause as the component-shape tightening below: B3's integrity precondition
# landed UPSTREAM of here, which made the justifying case unreachable at the point of use,
# and the permissiveness went dead without anyone re-deriving it.
#
# MEASURED from source, not inherited: `OVERALL=PARTIAL` has EXACTLY ONE site in the gate
# (its `--only` demotion), and the `mode: PARTIAL (--only …)` line is appended TWO LINES
# ABOVE IT INSIDE THE SAME `if [ -n "$ONLY" ]` block — so a PARTIAL token and its scope line
# are inseparable BY CONSTRUCTION. No other emitter publishes a PARTIAL token. The only
# component-line-bearing blocks with no `mode:` line are the boundary FAIL blocks, whose
# token is FAIL, and which the integrity gate above rejects regardless.
#
# TRAP 2 THEREFORE SURVIVES, NARROWED TO WHAT IS REAL: the scope line is required only where
# the RUN TOKEN says the run was scoped. A FAIL-token block with no `mode:` line is still
# answerable, so this cannot red a legitimate boundary-shaped block.
#
# TRAP 1 IS UNCHANGED: `--only` takes a COMMA-SEPARATED LIST, so equality would red a
# correct `--only fmt,clippy`. Membership uses the gate's OWN predicate — comma to space,
# then a whole-word match (agent-gate.sh's `grep -qw "$name" <<<"${ONLY//,/ }"`) — so the two
# agree by construction and any looseness can only widen membership.
# STEP 1 counts `^mode:` by BARE PREFIX. Safe because the gate emits exactly ONE lowercase
# `mode:` line and only for `--only` (its uppercase `MODE:` lite/delta line is a different
# key, and those blocks are refused by the opener check above anyway), so a legitimate
# full-marker block carries no other `mode:` line for this to trip over.
_n_mode=$(_count_prefix mode "$BODY") \
  || cnm "mode-scope-unmeasurable; the scan for the block's --only scope failed"
if [ "$_n_mode" -gt 1 ]; then
  cnm "mode-scope-ambiguous; the block carries $_n_mode mode: lines, and ambiguity is never resolved in favour of PASS"
fi
# STEP 2: the one line must match the scope grammar exactly. A malformed one is its OWN
# refusal — reading it as "no scope line at all" would silently skip the membership test.
if [ "$_n_mode" -eq 1 ] && ! grep -qE '^mode: PARTIAL \(--only [^)]' "$BODY"; then
  cnm "mode-scope-ungrammatical; the block carries a mode: line that is not 'mode: PARTIAL (--only <non-empty list>)' — which covers both a mis-spelled line and an EMPTY scope, neither of which the gate can emit — so the run's scope cannot be read. Reported as its own kind rather than as no scope at all, which would silently skip the membership test"
fi
if [ "$RUN_TOKEN" = PARTIAL ] && [ "$_n_mode" -ne 1 ]; then
  cnm "mode-scope-missing; the run token is PARTIAL, which the gate emits ONLY from its --only demotion, and that demotion appends the 'mode: PARTIAL (--only …)' line in the same block — so a PARTIAL token with $_n_mode scope lines is a shape no emitter produces"
fi
if [ "$_n_mode" -eq 1 ]; then
  # Non-empty by construction: STEP 2 above refused an empty scope, so the separate
  # empty-scope check that used to sit here is gone. Two branches emitting one kind is what
  # let a control pass for the wrong reason.
  _scope=$(sed -n 's/^mode: PARTIAL (--only \([^)]*\)).*/\1/p' "$BODY" | head -1)
  # `-F`: the name is DATA here, not a pattern. Every other site interpolates the
  # `.`-escaped COMP_RE; a raw `$COMPONENT` as a BRE would make `a.b` match `axb`.
  if [ "$COMP_N" -ge 1 ] && ! grep -Fqw -- "$COMPONENT" <<<"${_scope//,/ }"; then
    cnm "mode-scope-contradiction; the block states it ran only '$_scope', yet carries a component line for '$COMPONENT' — the block's own scope and its content disagree, and which to believe cannot be established"
  fi
fi

if [ "$COMP_N" -gt 1 ]; then
  cnm "ambiguous-component-line; the block carries $COMP_N lines whose prefix is this component, so which one is the verdict cannot be established — counted by BARE PREFIX on purpose, so a malformed sibling row cannot hide behind a valid one; run-result=$RUN_TOKEN"
fi

if [ "$COMP_N" -eq 0 ]; then
  # AFFIRMATIVELY ABSENT from a block we have established is complete, valid and
  # single-extent: the component was not selected, or it crashed before recording. Either
  # way the check did not pass, and this must never soften to "probably fine".
  # Genuinely absent rather than merely ungrammatical, because STEP 1 counted the BARE
  # PREFIX. The old "a non-component line with that prefix exists" hint is GONE because it is
  # now unreachable: any such line lands on the malformed refusal below instead.
  notpass "component-absent; the run completed and its block carries no line at all whose prefix is this component; run-result=$RUN_TOKEN"
fi

# STEP 2 — validate the ONE row against the exact emitted grammar. Malformed is its own
# refusal, never "absent": the block DOES name this component, in a shape no emitter produces.
COMP_LINES=$(grep -E "$_COMP_LINE_RE" "$BODY"); _grc=$?
if [ "$_grc" -ge 2 ]; then
  cnm "component-scan-failed; validating this component's row failed (rc=$_grc), and a failed measurement is never reported as an absence"
fi
if [ -z "$COMP_LINES" ]; then
  cnm "component-line-malformed; the block carries a line whose prefix is this component, but it is not a component row in the emitter's own shape ('<name>: <STATUS> (<N>s)  <annotation>'), so no verdict can be read from it — reported as MALFORMED rather than absent; run-result=$RUN_TOKEN"
fi

COMP_STATUS=$(printf '%s' "$COMP_LINES" | sed -E "s/^${COMP_RE}: +([A-Za-z][A-Za-z-]*) \([0-9]+s\).*/\1/")
# CLOSED STATUS GRAMMAR, matched EXACTLY as a token (#3229 / the prefix-glob defect this
# repo has now made three times). An unrecognised token is COULD-NOT-MEASURE — if a future
# gate adds a fourth status, a lane asks a human rather than this reader guessing.
case "$COMP_STATUS" in
  PASS)
    verdict PASS 0 "$COMPONENT (its own component line reads PASS in a completed, tree-integrity-PASS full-marker run; run-result=$RUN_TOKEN. THIS IS ONE COMPONENT, NOT THE GATE OF RECORD)" ;;
  FAIL)
    notpass "its own component line reads FAIL; run-result=$RUN_TOKEN" ;;
  SKIP)
    notpass "its own component line reads SKIP — the check NEVER RAN, which is the vacuous pass itself, so this is not a pass; run-result=$RUN_TOKEN" ;;
  *)
    cnm "unrecognised-status '$COMP_STATUS'; the closed set is PASS|FAIL|SKIP and a token outside it is never read as a pass; run-result=$RUN_TOKEN" ;;
esac
