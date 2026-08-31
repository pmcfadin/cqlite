#!/usr/bin/env bash
#
# Regression tests for scripts/flow/premerge-assert.sh (issues #2668, #3465).
#
# Fast + hermetic: `gh` is shimmed by a PATH-prepended mock that emits the
# two-token line the script's `--jq '.headRefOid + " " + .state'` expression
# would produce (or a failure), driven by env vars — no network, no GitHub.
#
# #3465 adds the gate-of-record half: the assert now takes a THIRD, REQUIRED
# argument naming the FULL gate's summary file, and refuses to merge without a
# `==== AGENT-GATE SUMMARY ====` block carrying RESULT: PASS, tree-integrity:
# PASS, and provenance matching the certified sha. Those cases refuse BEFORE the
# `gh` call, so most of them do not need the mock at all — but they run with it on
# PATH anyway, so a case that wrongly reaches `gh` cannot pass by accident.
#
# Run standalone:   bash scripts/tests/test_premerge_assert.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ASSERT="$SCRIPT_DIR/../flow/premerge-assert.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

T=$(mktemp -d "${TMPDIR:-/tmp}/premerge-assert-test.XXXXXX")
trap 'rm -rf "$T"' EXIT

# --- gh mock -----------------------------------------------------------------
# A fake `gh` on PATH. Since the script parses via gh's built-in `--jq`, the
# mock stands in for gh-plus-jq and emits the already-extracted two-token line
# "<headRefOid> <state>". Two env vars drive it per-case:
#   MOCK_GH_OUT    the exact stdout to emit (the two-token --jq result)
#   MOCK_GH_FAIL   if "1", exit non-zero without output (simulates auth/network)
BIN="$T/bin"
mkdir -p "$BIN"
cat >"$BIN/gh" <<'MOCK'
#!/usr/bin/env bash
if [ "${MOCK_GH_FAIL:-0}" = "1" ]; then
  echo "gh: could not connect" >&2
  exit 1
fi
printf '%s\n' "${MOCK_GH_OUT:-}"
exit 0
MOCK
chmod +x "$BIN/gh"

# run <expected-exit> <description> <args...> — invokes the assert with the gh
# mock on PATH, captures combined output + exit code. Sets $OUT and $RC.
run() {
  local want="$1" desc="$2"
  shift 2
  OUT=$(PATH="$BIN:$PATH" bash "$ASSERT" "$@" 2>&1)
  RC=$?
  if [ "$RC" -ne "$want" ]; then
    bad "$desc (exit $RC, wanted $want)"
    printf '     output: %s\n' "$OUT"
    return 1
  fi
  return 0
}

CERTIFIED="da9a7cb2abc00000000000000000000000000000"   # full 40-char hex
STALE="ca8eb016def11111111111111111111111111111"       # full 40-char hex

# The two abbreviations the gate ACTUALLY writes for $CERTIFIED: `commit:` is
# `printf '%.7s'` and `tree-start:` is `_tree_short` = `printf '%.12s'`, both of
# the same verified capture (scripts/agent-gate.sh). Two independent widths.
C7="da9a7cb"
C12="da9a7cb2abc0"

# --- summary fixture builders -------------------------------------------------
# emit_summary_block <start-marker> <end-marker> <mode-line|-> <commit-val|-> \
#                    <tree-start-val|-> <tree-integrity-val|-> <result-val|->
# A "-" omits that line entirely. Line SHAPES are copied from a real full-gate
# summary (/tmp/cqlite-gates/**/full-gate.txt), trailing fields included, so the
# parser is exercised against the grammar the gate emits and not a simplification.
emit_summary_block() {
  local start="$1" end="$2" mode="$3" commit="$4" tstart="$5" ti="$6" result="$7"
  printf '%s\n' "$start"
  printf 'run-id: /tmp/agent-gate.9cIQgX\n'
  [ "$mode" = "-" ] || printf '%s\n' "$mode"
  [ "$commit" = "-" ] || printf 'commit: %s branch: issue-3465-require-gate-of-record dirty: no\n' "$commit"
  printf 'datasets: 144 Data.db files under /data/datasets\n'
  printf 'accelerators: sccache=on nextest=on lanes=on mold=absent perf=paranoid-4\n'
  [ "$tstart" = "-" ] || printf 'tree-start: %s dirty: no digest: 671a6275687c\n' "$tstart"
  printf 'tree-end: %s dirty: no digest: 671a6275687c\n' "$tstart"
  [ "$ti" = "-" ] || printf 'tree-integrity: %s\n' "$ti"
  printf 'file-size:         PASS (0s)\n'
  printf 'smoke:             PASS (193s)\n'
  printf 'logs: /tmp/agent-gate.9cIQgX\n'
  [ "$result" = "-" ] || printf 'RESULT: %s\n' "$result"
  printf '%s\n' "$end"
}

FULL_S="==== AGENT-GATE SUMMARY ===="
FULL_E="==== END AGENT-GATE SUMMARY ===="
LITE_S="==== AGENT-GATE LITE SUMMARY ===="
LITE_E="==== END AGENT-GATE LITE SUMMARY ===="
DELTA_S="==== AGENT-GATE DELTA SUMMARY ===="
DELTA_E="==== END AGENT-GATE DELTA SUMMARY ===="

# full_block [commit] [tree-start] [tree-integrity] [result] -> STDOUT.
# Separate from full_summary because composing two blocks into one file cannot go
# through a `>"$f"` helper: `full_summary /dev/stdout` inside a `{ } > file` group
# TRUNCATES the file, which silently produced a ONE-block fixture for a
# two-block case (found while writing these tests — the case passed vacuously).
full_block() {
  emit_summary_block "$FULL_S" "$FULL_E" "-" \
    "${1:-$C7}" "${2:-$C12}" "${3:-PASS}" "${4:-PASS}"
}

# full_summary <file> [commit] [tree-start] [tree-integrity] [result]
full_summary() {
  local f="$1"
  shift
  full_block "$@" >"$f"
}

GOOD="$T/full-pass.txt"
full_summary "$GOOD"

# refused <description> <summary-file> [needle] — every gate-of-record refusal is
# exit 2 with the NO-GATE-OF-RECORD verdict; an optional needle pins the cause so
# a case cannot pass by refusing for the WRONG reason.
refused() {
  local desc="$1" f="$2" needle="${3:-}"
  if run 2 "$desc" 2421 "$CERTIFIED" "$f"; then
    case "$OUT" in
      *"PREMERGE: NO-GATE-OF-RECORD"*) ;;
      *) bad "$desc: missing NO-GATE-OF-RECORD verdict (got: $OUT)"; return 1 ;;
    esac
    if [ -n "$needle" ] && [ "${OUT#*"$needle"}" = "$OUT" ]; then
      bad "$desc: refusal does not name the cause '$needle' (got: $OUT)"
      return 1
    fi
    ok "$desc"
  fi
}

export MOCK_GH_FAIL=0
export MOCK_GH_OUT="$CERTIFIED OPEN"

# =============================================================================
# Pre-#3465 behaviour, now carrying the third argument
# =============================================================================

# --- Case 1: match -> exit 0 --------------------------------------------------
if run 0 "match: OPEN + head==certified + gate of record -> exit 0" 2421 "$CERTIFIED" "$GOOD"; then
  case "$OUT" in
    *"PREMERGE: OK $CERTIFIED"*) ok "match: prints PREMERGE: OK <sha>" ;;
    *) bad "match: missing 'PREMERGE: OK <sha>' (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: GATE-OF-RECORD commit: $C7 tree-start: $C12 tree-integrity: PASS dirty: no"*)
      ok "match: prints the GATE-OF-RECORD evidence line (commit/tree-start/integrity/dirty)" ;;
    *) bad "match: missing GATE-OF-RECORD evidence line (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"summary: $GOOD"*) ok "match: evidence line names the summary file" ;;
    *) bad "match: evidence line must name the summary file (got: $OUT)" ;;
  esac
fi

# --- Case 2: mismatch -> exit 2, message names BOTH SHAs -----------------------
export MOCK_GH_OUT="$STALE OPEN"
if run 2 "mismatch: head moved -> exit 2" 2421 "$CERTIFIED" "$GOOD"; then
  if [ "${OUT#*"$CERTIFIED"}" != "$OUT" ] && [ "${OUT#*"$STALE"}" != "$OUT" ]; then
    ok "mismatch: message contains BOTH SHAs"
  else
    bad "mismatch: message must contain both certified + actual SHA (got: $OUT)"
  fi
  case "$OUT" in
    *"re-certify before merge"*) ok "mismatch: message says re-certify before merge" ;;
    *) bad "mismatch: missing re-certify guidance (got: $OUT)" ;;
  esac
fi

# --- Case 3: merged/closed PR -> exit 2 ---------------------------------------
export MOCK_GH_OUT="$CERTIFIED MERGED"
if run 2 "merged PR -> exit 2" 2421 "$CERTIFIED" "$GOOD"; then
  case "$OUT" in
    *"NOT-OPEN"*|*"closed or merged"*) ok "merged: distinct not-open refusal message" ;;
    *) bad "merged: missing not-open message (got: $OUT)" ;;
  esac
fi

# --- Case 4: gh/network failure -> exit 3 (fail closed) -----------------------
export MOCK_GH_FAIL=1
export MOCK_GH_OUT=""
if run 3 "gh failure -> exit 3 (fail closed)" 2421 "$CERTIFIED" "$GOOD"; then
  case "$OUT" in
    *"GH-FAILURE"*) ok "gh-failure: distinct fail-closed message" ;;
    *) bad "gh-failure: missing GH-FAILURE message (got: $OUT)" ;;
  esac
fi
export MOCK_GH_FAIL=0
export MOCK_GH_OUT="$CERTIFIED OPEN"

# --- Case 5: usage guard -> exit 3 --------------------------------------------
if run 3 "missing args -> exit 3" 2421; then
  ok "usage: too few args fails closed (exit 3)"
fi

# --- Case 6: malformed --jq output (missing state token) -> exit 3 ------------
export MOCK_GH_OUT="$CERTIFIED"   # only one token, no state
if run 3 "malformed --jq output -> exit 3" 2421 "$CERTIFIED" "$GOOD"; then
  ok "malformed: fails closed (exit 3)"
fi
export MOCK_GH_OUT="$CERTIFIED OPEN"

# --- Case 7: certified SHA passed UPPERCASE -> still matches (normalization) --
CERT_UPPER=$(printf '%s' "$CERTIFIED" | tr '[:lower:]' '[:upper:]')
if run 0 "uppercase certified SHA -> normalized match -> exit 0" 2421 "$CERT_UPPER" "$GOOD"; then
  ok "normalization: uppercase certified SHA still matches lowercase head + summary"
fi

# --- Case 8: certified SHA wrong length -> exit 3 -----------------------------
if run 3 "short certified SHA -> exit 3" 2421 "da9a7cb2" "$GOOD"; then
  ok "validation: abbreviated (non-40-char) SHA fails closed (exit 3)"
fi

# =============================================================================
# #3465 — the gate of record is REQUIRED
# =============================================================================

# --- Case 9: the third argument is REQUIRED, not optional ---------------------
# An OPTIONAL argument would leave the honour system exactly where it is, so the
# pre-#3465 two-argument invocation must FAIL LOUDLY (usage, exit 3).
if run 3 "pre-#3465 two-arg invocation -> exit 3 (usage)" 2421 "$CERTIFIED"; then
  case "$OUT" in
    *"gate-of-record-summary"*) ok "usage: two-arg call fails closed and names <gate-of-record-summary>" ;;
    *) bad "usage: two-arg refusal must name the missing argument (got: $OUT)" ;;
  esac
fi
if run 3 "empty third argument -> exit 3 (usage)" 2421 "$CERTIFIED" ""; then
  ok "usage: an EMPTY summary path is a usage failure, not an absent-file refusal"
fi

# --- Case 10: summary file absent / empty -------------------------------------
refused "summary file absent -> refuse" "$T/does-not-exist.txt" "does not exist"
: >"$T/empty.txt"
refused "summary file EMPTY -> refuse" "$T/empty.txt" "EMPTY"

# --- Case 11: LITE-only summary -> refuse (this is literally the #3465 case) --
# PR #3408 merged on 22 lite PASSes and no full gate. --lite emits DISTINCT
# markers plus a MODE line, so a lite summary carries ZERO full-gate blocks.
emit_summary_block "$LITE_S" "$LITE_E" \
  "MODE: lite (FAST ITERATION — NOT the gate of record; full agent-gate.sh must PASS once before merge)" \
  "$C7" "$C12" PASS PASS >"$T/lite-only.txt"
refused "LITE-only summary (the #3408 case) -> refuse" "$T/lite-only.txt" "ZERO full-gate blocks"
if [ "${OUT#*"found 1 lite"}" != "$OUT" ]; then
  ok "lite-only: refusal NAMES the lite block it found (actionable diagnostic)"
else
  bad "lite-only: refusal should name the lite block it found (got: $OUT)"
fi

# --- Case 12: DELTA-only summary -> refuse ------------------------------------
emit_summary_block "$DELTA_S" "$DELTA_E" \
  "MODE: delta (TEST/DOCS-ONLY RE-CERTIFICATION — NOT the gate of record; gate of record = the full agent-gate.sh PASS at anchor abc1234)" \
  "$C7" "$C12" PASS PASS >"$T/delta-only.txt"
refused "DELTA-only summary -> refuse" "$T/delta-only.txt" "ZERO full-gate blocks"
if [ "${OUT#*"0 lite, 1 delta"}" != "$OUT" ]; then
  ok "delta-only: refusal NAMES the delta block it found"
else
  bad "delta-only: refusal should name the delta block it found (got: $OUT)"
fi

# --- Case 13: the #3041 launch SENTINEL alone -> refuse -----------------------
# The gate writes this at LAUNCH (before the #1825 slot is even granted): a
# FULL-header block with run-id + tree-start + `RESULT: INCOMPLETE (gate did not
# finish)` and NO tree-integrity/commit. It is a liveness placeholder, NOT a
# verdict — accepting it would certify a still-queued or dead gate.
{
  printf '%s\n' "$FULL_S"
  printf 'run-id: /tmp/agent-gate.9cIQgX\n'
  printf 'tree-start: %s dirty: no digest: 671a6275687c\n' "$C12"
  printf 'RESULT: INCOMPLETE (gate did not finish)\n'
  printf '%s\n' "$FULL_E"
} >"$T/sentinel.txt"
refused "launch sentinel (RESULT: INCOMPLETE) alone -> refuse" "$T/sentinel.txt" "INCOMPLETE"

# --- Case 14: RESULT: FAIL -> refuse -----------------------------------------
full_summary "$T/result-fail.txt" "$C7" "$C12" PASS FAIL
refused "RESULT: FAIL -> refuse" "$T/result-fail.txt" "RESULT verdict token in the full-gate block is 'FAIL'"

# --- Case 15: near-miss RESULT verdict tokens -> refuse (token-exact) --------
# A `PASS*` prefix test would check a SPELLING rather than a STATE.
for near in PASSthisNeverRan PASS-MEASUREMENT-DID-NOT-HAPPEN PASSED; do
  full_summary "$T/near-$near.txt" "$C7" "$C12" PASS "$near"
  refused "near-miss RESULT token '$near' -> refuse (token-exact, not a prefix)" \
    "$T/near-$near.txt" "RESULT verdict token in the full-gate block is '$near'"
done

# --- Case 16: tree-integrity FAIL / PENDING / SKIP -> refuse -----------------
# A run whose worktree mutated mid-run is not a certification (#2926); PENDING
# means it never reached the terminal emit; SKIP means the check never ran.
full_summary "$T/ti-fail.txt" "$C7" "$C12" \
  "FAIL (tree-mutated-midrun; head da9a7cb->ca8eb01; changed: src/lib.rs)" PASS
refused "tree-integrity: FAIL -> refuse" "$T/ti-fail.txt" "tree-integrity verdict token in the full-gate block is 'FAIL'"
full_summary "$T/ti-pending.txt" "$C7" "$C12" PENDING PASS
refused "tree-integrity: PENDING -> refuse" "$T/ti-pending.txt" "token in the full-gate block is 'PENDING'"
full_summary "$T/ti-skip.txt" "$C7" "$C12" "SKIP (no capture)" PASS
refused "tree-integrity: SKIP -> refuse" "$T/ti-skip.txt" "token in the full-gate block is 'SKIP'"

# --- Case 17: missing RESULT / tree-integrity lines -> refuse ----------------
full_summary "$T/no-result.txt" "$C7" "$C12" PASS "-"
refused "no RESULT: line -> refuse" "$T/no-result.txt" "no 'RESULT:' line"
full_summary "$T/no-ti.txt" "$C7" "$C12" "-" PASS
refused "no tree-integrity: line -> refuse" "$T/no-ti.txt" "no 'tree-integrity:' line"

# --- Case 18: commit: / tree-start: mismatch -> refuse (the #3616 class) -----
# This is the OTHER motivating escape, distinct from #3408's "no gate at all":
# #3616 = "a real gate, someone else's". A closer located its run dir by RECENCY
# (`ls -t /tmp/agent-gate.*`), read a PEER LANE's dir, saw 33/37 components PASS
# and was about to merge #3616 on PR #3580's verdict — the count, the dir and the
# timestamps were all real, and only the `run-id:` line (read by a human) exposed
# it. premerge-assert cannot verify `run-id:` (it did not launch the gate), so the
# sha binding is what makes a cross-lane verdict a mechanical refusal.
# Each abbreviation is compared at ITS OWN width (7 and 12), against a DIFFERENT
# sha here.
#
# First the composite #3616 shape: a fully well-formed FULL block, RESULT: PASS,
# tree-integrity: PASS, everything a genuine gate of record has — except that its
# provenance names a peer PR's head throughout.
full_summary "$T/peer-lane.txt" "ca8eb01" "ca8eb016def1" PASS PASS
refused "#3616: a valid full-gate PASS naming a PEER LANE's sha -> refuse" \
  "$T/peer-lane.txt" "does not match the certified sha"
# ...and the single-field variants, which is where the two independent widths pay.
full_summary "$T/commit-mismatch.txt" "ca8eb01" "$C12" PASS PASS
refused "commit: mismatch (7 hex of a different sha) -> refuse" \
  "$T/commit-mismatch.txt" "'commit:' value 'ca8eb01' in the full-gate block does not match the certified sha at 7 chars"
full_summary "$T/tstart-mismatch.txt" "$C7" "ca8eb016def1" PASS PASS
refused "tree-start: mismatch (12 hex of a different sha) -> refuse" \
  "$T/tstart-mismatch.txt" "'tree-start:' value 'ca8eb016def1' in the full-gate block does not match the certified sha at 12 chars"
# A tree-start that agrees on the FIRST 7 chars but diverges at 8..12 is exactly
# what the second, wider compare buys over the 7-hex one.
full_summary "$T/tstart-wide.txt" "$C7" "da9a7cbffff0" PASS PASS
refused "tree-start: diverges only beyond 7 chars -> refuse (the wider compare pays)" \
  "$T/tstart-wide.txt" "'tree-start:' value 'da9a7cbffff0' in the full-gate block does not match"
# ...and one that is NARROWER than the 12 the gate emits still has to match at
# its OWN width, which it does — proving the compare uses the VALUE's length, not
# a fixed assumed width. 8 hex, not 4: 4 would be BELOW the floor (next case), and
# pinning the loosest accepted width would make that leniency a requirement.
full_summary "$T/tstart-short.txt" "$C7" "da9a7cb2" PASS PASS
if run 0 "8-hex tree-start that prefixes the certified sha -> accepted at ITS width" \
  2421 "$CERTIFIED" "$T/tstart-short.txt"; then
  ok "width: the compare uses the VALUE's own length (8 hex accepted, not padded/globbed)"
fi
# The FLOOR is 7 — the narrowest abbreviation the gate ever emits (commit: is
# printf '%.7s'). A 4-hex value that DOES prefix the certified sha is refused
# anyway: accepted at its own width it would be a 1-in-65536 accidental
# cross-lane match, i.e. the #3616 class this compare exists to refuse.
full_summary "$T/tstart-4hex.txt" "$C7" "da9a" PASS PASS
refused "4-hex tree-start (below the 7 floor) -> refuse even though it prefixes" \
  "$T/tstart-4hex.txt" "is 4 hex chars — outside the 7..40 range"

# --- Case 19: non-hex commit:/tree-start: -> refuse, never skipped -----------
# The gate writes these placeholders when its capture failed or there was no git
# worktree. "Nothing was recorded" must REFUSE, not be waved through.
full_summary "$T/commit-unverified.txt" "unverified" "$C12" PASS PASS
refused "commit: unverified (capture failed) -> refuse" \
  "$T/commit-unverified.txt" "is not lowercase hex"
full_summary "$T/tstart-selftest.txt" "$C7" "selftest" PASS PASS
refused "tree-start: selftest -> refuse" "$T/tstart-selftest.txt" "is not lowercase hex"
# `tree-start: (not captured)` — the awk token is the literal "(not".
full_summary "$T/tstart-notcap.txt" "$C7" "(not" PASS PASS
refused "tree-start: (not captured) -> refuse" "$T/tstart-notcap.txt" "is not lowercase hex"
# An UPPERCASE hex abbreviation is not something git emits; refuse rather than
# silently normalize a value whose provenance we cannot explain.
full_summary "$T/commit-upper.txt" "DA9A7CB" "$C12" PASS PASS
refused "commit: UPPERCASE hex -> refuse" "$T/commit-upper.txt" "is not lowercase hex"
# Missing lines entirely.
full_summary "$T/no-commit.txt" "-" "$C12" PASS PASS
refused "no commit: line -> refuse" "$T/no-commit.txt" "no 'commit:' line"
full_summary "$T/no-tstart.txt" "$C7" "-" PASS PASS
refused "no tree-start: line -> refuse" "$T/no-tstart.txt" "no 'tree-start:' line"

# --- Case 20: TWO full-gate blocks -> refuse (ambiguous, never "take the last") --
{ full_block "$C7" "$C12" PASS FAIL
  full_block "$C7" "$C12" PASS PASS
} >"$T/two-blocks.txt"
if [ "$(grep -c -x -F "$FULL_S" "$T/two-blocks.txt")" -eq 2 ]; then
  ok "two-blocks fixture: the file really does hold TWO full-gate start markers"
else
  bad "two-blocks fixture: expected 2 start markers in the fixture"
fi
refused "two full-gate blocks -> refuse as AMBIGUOUS" "$T/two-blocks.txt" "2 full-gate blocks"
if [ "${OUT#*"take the last block"}" != "$OUT" ]; then
  ok "two-blocks: refusal explains why 'take the last one' is unsafe"
else
  bad "two-blocks: expected the rationale for refusing rather than picking (got: $OUT)"
fi

# --- Case 21: an UNTERMINATED full block -> refuse ---------------------------
# A killed/truncated gate leaves a start marker with no exact end marker.
full_summary "$T/unterminated.txt"
grep -v -x -F "$FULL_E" "$T/unterminated.txt" >"$T/unterminated2.txt"
refused "unterminated full block (truncated file) -> refuse" \
  "$T/unterminated2.txt" "UNTERMINATED"

# --- Case 22: a MODE: line inside a FULL-header block -> refuse --------------
# Belt for the header separation: the FULL gate emits NO MODE: line.
emit_summary_block "$FULL_S" "$FULL_E" \
  "MODE: lite (FAST ITERATION — NOT the gate of record)" \
  "$C7" "$C12" PASS PASS >"$T/full-with-mode.txt"
refused "MODE: line inside a FULL-header block -> refuse" \
  "$T/full-with-mode.txt" "carries a MODE: line"

# --- Case 23: prose QUOTING the markers is not a block ----------------------
# Whole-line-exact anchoring defends against PROSE copies of a marker —
# indented, `>`-quoted, fenced or mid-sentence — which CLAUDE.md, issue bodies,
# PR comments and the doctrine files this change edits all contain, and against
# a TRUNCATED pattern such as `AGENT-GATE SUMMARY ====`, which matches all four
# markers (full/lite start and end). Note the end marker does NOT contain the
# start marker as a substring: `END ` sits between `====` and `AGENT-GATE`.
{
  printf 'The gate emits a "%s" block whose RESULT: PASS is the verdict.\n' "$FULL_S"
  printf 'Its terminator is %s and it is not a block opener.\n' "$FULL_E"
  printf '  %s\n' "$FULL_S"      # indented -> not whole-line-exact
  printf 'RESULT: PASS\n'
  printf 'tree-integrity: PASS\n'
} >"$T/prose-only.txt"
refused "prose quoting the markers -> counted as ZERO blocks" \
  "$T/prose-only.txt" "ZERO full-gate blocks"

# A real block AFTER prose that quotes the markers still parses as exactly one.
{ cat "$T/prose-only.txt"; full_block; } >"$T/prose-then-block.txt"
if [ "$(grep -c -x -F "$FULL_S" "$T/prose-then-block.txt")" -eq 1 ]; then
  ok "prose+block fixture: exactly one whole-line-exact start marker, plus quoted ones"
else
  bad "prose+block fixture: expected exactly 1 whole-line-exact start marker"
fi
if run 0 "prose quoting the markers + ONE real block -> exit 0" \
  2421 "$CERTIFIED" "$T/prose-then-block.txt"; then
  ok "anchoring: quoted/indented markers are inert; the one real block certifies"
fi

# --- Case 24: ANSI-coloured summary still parses (#3400) --------------------
# BELT, stated precisely: the summary FILE's block lines are `echo`s of computed
# strings (scripts/agent-gate.sh emit_summary), so they are NOT coloured —
# CARGO_TERM_COLOR colours cargo output inside gate.log, not the block. What the
# strip covers is a block RECOVERED FROM A COLOURED CAPTURE rather than from the
# summary file, and colour does survive redirection (#3400). Non-vacuous either
# way: without the strip this fixture refuses a perfectly good gate of record.
ESC=$(printf '\033')
sed -e "s/RESULT: PASS/${ESC}[32mRESULT${ESC}[0m: ${ESC}[1;32mPASS${ESC}[0m/" \
    -e "s/tree-integrity: PASS/tree-integrity: ${ESC}[32mPASS${ESC}[0m/" \
    -e "s/^${FULL_S}\$/${ESC}[1m${FULL_S}${ESC}[0m/" \
    "$GOOD" >"$T/coloured.txt"
if grep -q "$ESC" "$T/coloured.txt"; then
  ok "colour fixture: the fixture really does contain ANSI escapes"
else
  bad "colour fixture: expected ANSI escapes in the fixture"
fi
if run 0 "ANSI-coloured full summary -> still parsed -> exit 0" \
  2421 "$CERTIFIED" "$T/coloured.txt"; then
  ok "ansi: escapes are stripped before marker/verdict matching (#3400)"
fi

# --- Case 25: dirty: is REPORTED, not enforced -------------------------------
# Deliberate: failing on `dirty: yes` is not in the #3465 ruling. It must be
# VISIBLE at the merge point, but it does not block.
{
  printf '%s\n' "$FULL_S"
  printf 'commit: %s branch: main dirty: yes\n' "$C7"
  printf 'tree-start: %s dirty: yes digest: 671a6275687c\n' "$C12"
  printf 'tree-integrity: PASS\n'
  printf 'RESULT: PASS\n'
  printf '%s\n' "$FULL_E"
} >"$T/dirty.txt"
if run 0 "dirty: yes -> still exit 0 (reported, not enforced)" 2421 "$CERTIFIED" "$T/dirty.txt"; then
  case "$OUT" in
    *"dirty: yes"*) ok "dirty: a dirty gate of record is REPORTED in the evidence line" ;;
    *) bad "dirty: the evidence line must report dirty: yes (got: $OUT)" ;;
  esac
fi

# --- Case 26: the mutated-path commit: parenthetical parses ------------------
# On the #2926 mutation path `commit:` carries a trailing parenthetical; token
# extraction must be robust to it. (tree-integrity is FAIL there, so this fixture
# keeps integrity PASS and only exercises the LINE SHAPE.)
{
  printf '%s\n' "$FULL_S"
  printf 'commit: %s branch: main dirty: no (VERIFIED START — the identity this run executed against)\n' "$C7"
  printf 'tree-start: %s dirty: no digest: 671a6275687c\n' "$C12"
  printf 'tree-integrity: PASS\n'
  printf 'RESULT: PASS\n'
  printf '%s\n' "$FULL_E"
} >"$T/commit-paren.txt"
if run 0 "commit: with a trailing parenthetical -> token extracted -> exit 0" \
  2421 "$CERTIFIED" "$T/commit-paren.txt"; then
  ok "shape: commit: token extraction survives a trailing parenthetical"
fi

# --- Case 27: the gate-of-record check runs BEFORE any gh call ---------------
# It is offline and cheap, and "you have no gate of record" must be reportable
# without a network round trip. Proven by a gh mock that FAILS: a missing gate of
# record must still yield the NO-GATE-OF-RECORD refusal (exit 2), not GH-FAILURE.
export MOCK_GH_FAIL=1
if run 2 "no gate of record + gh DOWN -> NO-GATE-OF-RECORD (offline check first)" \
  2421 "$CERTIFIED" "$T/lite-only.txt"; then
  case "$OUT" in
    *"NO-GATE-OF-RECORD"*)
      case "$OUT" in
        *"GH-FAILURE"*) bad "ordering: reached gh before validating the summary (got: $OUT)" ;;
        *) ok "ordering: the summary is validated BEFORE any gh call" ;;
      esac
      ;;
    *) bad "ordering: expected NO-GATE-OF-RECORD with gh down (got: $OUT)" ;;
  esac
fi
export MOCK_GH_FAIL=0

# =============================================================================
# #3465 review — the ANCHORED DELTA PAIR (optional 4th argument)
# =============================================================================
# CLAUDE.md's #1892 rule MANDATES that a test/docs-only diff on top of a full
# PASS at anchor X re-certifies with `--delta X` and "never a repeat full gate",
# and that the PR record BOTH blocks. So the merged head legitimately differs
# from the gate of record's sha, and a 3-arg-only guard red on correct,
# doctrine-mandated input.
ANCHOR="ab12cd34ef560000000000000000000000000000"   # the full gate's sha (X)
A7="ab12cd3"
A12="ab12cd34ef56"
if [ "${#ANCHOR}" -eq 40 ] && [ "${ANCHOR:0:7}" = "$A7" ] && [ "${ANCHOR:0:12}" = "$A12" ]; then
  ok "anchor fixture: ANCHOR is 40 hex and A7/A12 are its own 7/12-char prefixes"
else
  bad "anchor fixture: ANCHOR/A7/A12 are inconsistent"
fi

DELTA_MODE="MODE: delta (TEST/DOCS-ONLY RE-CERTIFICATION — NOT the gate of record; gate of record = the full agent-gate.sh PASS at anchor $ANCHOR)"

# delta_block [anchor] [commit] [tree-start] [tree-integrity] [result] [mode] \
#             [anchor-parenthetical] -> STDOUT.  "-" omits a line entirely.
# Line SHAPES are copied from scripts/agent-gate.sh's delta emit site
# (anchor_meta + SUMMARY_MODE_LINE), trailing fields included.
delta_block() {
  local anchor="${1:-$ANCHOR}" commit="${2:-$C7}" tstart="${3:-$C12}" \
        ti="${4:-PASS}" result="${5:-PASS}" mode="${6:-$DELTA_MODE}" \
        paren="${7:-(full-gate PASS commit)}"
  printf '%s\n' "$DELTA_S"
  printf 'run-id: /tmp/agent-gate.dLt4Qx\n'
  [ "$mode" = "-" ] || printf '%s\n' "$mode"
  [ "$commit" = "-" ] || printf 'commit: %s branch: issue-3465-require-gate-of-record dirty: no\n' "$commit"
  [ "$anchor" = "-" ] || printf 'delta-anchor: %s %s\n' "$anchor" "$paren"
  printf 'delta-anchor-run-id: /tmp/agent-gate.9cIQgX\n'
  printf 'gate-of-record: full agent-gate.sh run at %s (this DELTA re-certifies a test/docs-only diff; it is NOT a substitute for the full gate)\n' "$anchor"
  printf 'delta-executors: cargo test -p cqlite-core --test issue_3465 (3), docs (2)\n'
  [ "$tstart" = "-" ] || printf 'tree-start: %s dirty: no digest: 671a6275687c\n' "$tstart"
  printf 'tree-end: %s dirty: no digest: 671a6275687c\n' "$tstart"
  [ "$ti" = "-" ] || printf 'tree-integrity: %s\n' "$ti"
  printf 'logs: /tmp/agent-gate.dLt4Qx\n'
  [ "$result" = "-" ] || printf 'RESULT: %s\n' "$result"
  printf '%s\n' "$DELTA_E"
}
delta_summary() { local f="$1"; shift; delta_block "$@" >"$f"; }

# refused_pair <desc> <full-file> <delta-file> [needle] — a 4-arg refusal.
refused_pair() {
  local desc="$1" f="$2" d="$3" needle="${4:-}"
  if run 2 "$desc" 2421 "$CERTIFIED" "$f" "$d"; then
    case "$OUT" in
      *"PREMERGE: NO-GATE-OF-RECORD"*) ;;
      *) bad "$desc: missing NO-GATE-OF-RECORD verdict (got: $OUT)"; return 1 ;;
    esac
    if [ -n "$needle" ] && [ "${OUT#*"$needle"}" = "$OUT" ]; then
      bad "$desc: refusal does not name the cause '$needle' (got: $OUT)"
      return 1
    fi
    ok "$desc"
  fi
}

ANCHORFULL="$T/anchor-full.txt"
full_summary "$ANCHORFULL" "$A7" "$A12" PASS PASS
GOODDELTA="$T/good-delta.txt"
delta_summary "$GOODDELTA"

# --- Case 28(a): 3-arg call with an ANCHOR-only summary still refuses ---------
# The red half of the blocker: without the fourth argument the anchor's full
# PASS names a different tree than the head being merged, and that is exactly
# the #3616 shape — indistinguishable from a peer lane's summary.
refused "anchor-only full summary at a MOVED head (3 args) -> refuse" \
  "$ANCHORFULL" "does not match the certified sha"

# --- Case 28(b): the anchored delta PAIR at that same moved head -> accept ---
if run 0 "anchored delta pair (full PASS at X + delta at Y) -> exit 0" \
  2421 "$CERTIFIED" "$ANCHORFULL" "$GOODDELTA"; then
  case "$OUT" in
    *"PREMERGE: OK $CERTIFIED"*) ok "delta pair: prints PREMERGE: OK <sha>" ;;
    *) bad "delta pair: missing PREMERGE: OK (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: GATE-OF-RECORD commit: $A7 tree-start: $A12"*)
      ok "delta pair: the GATE-OF-RECORD line names the ANCHOR's provenance" ;;
    *) bad "delta pair: GATE-OF-RECORD line must name the anchor (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: DELTA-RECERT anchor: $ANCHOR commit: $C7 tree-start: $C12"*)
      ok "delta pair: a DISTINCT DELTA-RECERT line names the anchor + the merged tree" ;;
    *) bad "delta pair: missing the DELTA-RECERT evidence line (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"summary: $GOODDELTA"*) ok "delta pair: the DELTA-RECERT line names the delta summary file" ;;
    *) bad "delta pair: DELTA-RECERT must name the delta summary file (got: $OUT)" ;;
  esac
fi

# --- Case 28(c): delta-anchor: naming a DIFFERENT sha -> refuse --------------
# This is the property that makes the pair a CHAIN rather than two unrelated
# blocks: without it, ANY full PASS could anchor ANY delta run.
delta_summary "$T/delta-wrong-anchor.txt" "$STALE"
refused_pair "delta-anchor: names a DIFFERENT sha than the full block -> refuse" \
  "$ANCHORFULL" "$T/delta-wrong-anchor.txt" \
  "in the full-gate block does not match the delta block's anchor sha"

# --- Case 28(d): the delta run's OWN provenance must cover the merged tree ---
delta_summary "$T/delta-wrong-commit.txt" "$ANCHOR" "ca8eb01" "$C12"
refused_pair "delta commit: does not match the certified sha -> refuse" \
  "$ANCHORFULL" "$T/delta-wrong-commit.txt" \
  "'commit:' value 'ca8eb01' in the delta block does not match the certified sha"
delta_summary "$T/delta-wrong-tstart.txt" "$ANCHOR" "$C7" "ca8eb016def1"
refused_pair "delta tree-start: does not match the certified sha -> refuse" \
  "$ANCHORFULL" "$T/delta-wrong-tstart.txt" \
  "'tree-start:' value 'ca8eb016def1' in the delta block does not match the certified sha"

# --- Case 28(e): delta-anchor: (UNRESOLVED) -> refuse ------------------------
# scripts/agent-gate.sh emits this on its ERROR path when the --delta anchor does
# not resolve to a commit. Tested at RESULT: PASS so the needle pins the ANCHOR
# check rather than the RESULT check...
delta_summary "$T/delta-unresolved.txt" "$ANCHOR" "$C7" "$C12" PASS PASS "$DELTA_MODE" "(UNRESOLVED)"
refused_pair "delta-anchor: (UNRESOLVED) -> refuse" \
  "$ANCHORFULL" "$T/delta-unresolved.txt" "(UNRESOLVED)"
# ...and once in the shape the gate really emits it (RESULT: ERROR, a ref name
# rather than a resolved sha), which must also refuse.
delta_summary "$T/delta-unresolved-real.txt" "HEAD~3" "$C7" "$C12" PASS ERROR \
  "$DELTA_MODE" "(UNRESOLVED)"
refused_pair "delta-anchor: (UNRESOLVED) in its real RESULT: ERROR shape -> refuse" \
  "$ANCHORFULL" "$T/delta-unresolved-real.txt"

# --- Case 29: the delta-anchor value's own shape -----------------------------
delta_summary "$T/delta-no-anchor.txt" "-"
refused_pair "delta block with NO delta-anchor: line -> refuse" \
  "$ANCHORFULL" "$T/delta-no-anchor.txt" "has no 'delta-anchor:' line"
delta_summary "$T/delta-anchor-short.txt" "$A12"
refused_pair "delta-anchor: abbreviated (12 hex, not the resolved 40) -> refuse" \
  "$ANCHORFULL" "$T/delta-anchor-short.txt" "is 12 hex chars,"
delta_summary "$T/delta-anchor-nonhex.txt" "unverified"
refused_pair "delta-anchor: non-hex -> refuse" \
  "$ANCHORFULL" "$T/delta-anchor-nonhex.txt" "is not lowercase hex"

# --- Case 30: the fourth argument must BE a delta block ----------------------
refused_pair "a FULL summary passed as the fourth argument -> refuse" \
  "$ANCHORFULL" "$GOOD" "holds ZERO delta blocks"
if [ "${OUT#*"found 1 full"}" != "$OUT" ]; then
  ok "fourth-arg: refusal NAMES the full block it found instead of a delta one"
else
  bad "fourth-arg: refusal should name what it found (got: $OUT)"
fi
refused_pair "a LITE summary passed as the fourth argument -> refuse" \
  "$ANCHORFULL" "$T/lite-only.txt" "holds ZERO delta blocks"
{ delta_block; delta_block; } >"$T/two-deltas.txt"
if [ "$(grep -c -x -F "$DELTA_S" "$T/two-deltas.txt")" -eq 2 ]; then
  ok "two-deltas fixture: the file really does hold TWO delta start markers"
else
  bad "two-deltas fixture: expected 2 delta start markers"
fi
refused_pair "TWO delta blocks in the fourth argument -> refuse as AMBIGUOUS" \
  "$ANCHORFULL" "$T/two-deltas.txt" "holds 2 delta blocks"
refused_pair "fourth-argument file absent -> refuse" \
  "$ANCHORFULL" "$T/no-such-delta.txt" "delta summary file does not exist"
: >"$T/empty-delta.txt"
refused_pair "fourth-argument file EMPTY -> refuse" \
  "$ANCHORFULL" "$T/empty-delta.txt" "delta summary file is EMPTY"
if run 3 "EMPTY fourth argument -> exit 3 (usage, never a silent 3-arg downgrade)" \
  2421 "$CERTIFIED" "$ANCHORFULL" ""; then
  ok "usage: an empty fourth argument is a usage failure, not '3-arg mode'"
fi
if run 3 "five arguments -> exit 3 (usage)" 2421 "$CERTIFIED" "$ANCHORFULL" "$GOODDELTA" extra; then
  ok "usage: a fifth argument fails closed rather than being ignored"
fi

# --- Case 31: MODE: delta is REQUIRED in the delta block (the INVERSE belt) --
# In the FULL block a MODE: line is a refusal; in the DELTA block its ABSENCE
# is, and the token is asserted AFFIRMATIVELY rather than merely tolerated.
delta_summary "$T/delta-no-mode.txt" "$ANCHOR" "$C7" "$C12" PASS PASS "-"
refused_pair "delta block with NO MODE: line -> refuse (presence is REQUIRED here)" \
  "$ANCHORFULL" "$T/delta-no-mode.txt" "has no 'MODE:' line"
delta_summary "$T/delta-mode-lite.txt" "$ANCHOR" "$C7" "$C12" PASS PASS \
  "MODE: lite (FAST ITERATION — NOT the gate of record)"
refused_pair "delta header + MODE: lite -> refuse (token asserted, not presence)" \
  "$ANCHORFULL" "$T/delta-mode-lite.txt" "MODE token is 'lite', not 'delta'"

# --- Case 32: the delta block's own verdicts ---------------------------------
delta_summary "$T/delta-result-fail.txt" "$ANCHOR" "$C7" "$C12" PASS FAIL
refused_pair "delta RESULT: FAIL -> refuse" \
  "$ANCHORFULL" "$T/delta-result-fail.txt" "RESULT verdict token in the delta block is 'FAIL'"
delta_summary "$T/delta-ti-fail.txt" "$ANCHOR" "$C7" "$C12" \
  "FAIL (tree-mutated-midrun; head da9a7cb->ca8eb01; changed: docs/x.md)" PASS
refused_pair "delta tree-integrity: FAIL -> refuse" \
  "$ANCHORFULL" "$T/delta-ti-fail.txt" "tree-integrity verdict token in the delta block is 'FAIL'"
delta_block >"$T/delta-unterminated.txt.full"
grep -v -x -F "$DELTA_E" "$T/delta-unterminated.txt.full" >"$T/delta-unterminated.txt"
refused_pair "UNTERMINATED delta block -> refuse" \
  "$ANCHORFULL" "$T/delta-unterminated.txt" "UNTERMINATED"

# --- Case 33: the ANCHOR block is still held to the full-gate contract -------
# Case B relaxes ONLY the anchor's sha binding. Everything else it must satisfy
# in Case A it must still satisfy here.
{ full_block "$A7" "$A12" PASS FAIL; } >"$T/anchor-fail.txt"
refused_pair "anchor block RESULT: FAIL -> refuse even with a valid delta" \
  "$T/anchor-fail.txt" "$GOODDELTA" "RESULT verdict token in the full-gate block is 'FAIL'"
refused_pair "a LITE-only file as the ANCHOR -> refuse (the #3408 case, 4-arg form)" \
  "$T/lite-only.txt" "$GOODDELTA" "ZERO full-gate blocks"
full_summary "$T/anchor-nonhex.txt" "unverified" "$A12" PASS PASS
refused_pair "anchor commit: non-hex -> refuse (a real sha is still required)" \
  "$T/anchor-nonhex.txt" "$GOODDELTA" "is not lowercase hex"

# =============================================================================
# #3465 review — the remaining refusal branches
# =============================================================================

# --- Case 34: DUPLICATE keys inside one block -> refuse ----------------------
# "Last one wins" would let a doctored line override the real verdict. Without
# these fixtures the >1 branch of assert_single_key was dead code.
{
  printf '%s\n' "$FULL_S"
  printf 'commit: %s branch: main dirty: no\n' "$C7"
  printf 'tree-start: %s dirty: no digest: 671a6275687c\n' "$C12"
  printf 'tree-integrity: PASS\n'
  printf 'RESULT: FAIL\n'
  printf 'RESULT: PASS\n'
  printf '%s\n' "$FULL_E"
} >"$T/dup-result.txt"
refused "TWO RESULT: lines in one block -> refuse as AMBIGUOUS" \
  "$T/dup-result.txt" "has 2 'RESULT:' lines"
{
  printf '%s\n' "$FULL_S"
  printf 'commit: ca8eb01 branch: main dirty: no\n'
  printf 'commit: %s branch: main dirty: no\n' "$C7"
  printf 'tree-start: %s dirty: no digest: 671a6275687c\n' "$C12"
  printf 'tree-integrity: PASS\n'
  printf 'RESULT: PASS\n'
  printf '%s\n' "$FULL_E"
} >"$T/dup-commit.txt"
refused "TWO commit: lines in one block -> refuse as AMBIGUOUS" \
  "$T/dup-commit.txt" "has 2 'commit:' lines"

# --- Case 35: an UNREADABLE summary file -> refuse ---------------------------
# Distinct from absent and from empty. Skipped when running as root, which
# ignores the mode bit.
if [ "$(id -u)" = "0" ]; then
  ok "unreadable summary: SKIPPED (running as root ignores the mode bit)"
else
  cp "$GOOD" "$T/unreadable.txt"
  chmod 000 "$T/unreadable.txt"
  refused "summary file present but UNREADABLE -> refuse" \
    "$T/unreadable.txt" "is not readable"
  chmod 644 "$T/unreadable.txt"
fi

# --- Case 36: the PARSER TOOL failing is exit 3, not exit 2 ------------------
# A missing/ENOMEM-ing awk is a broken BOX, not a verdict about the gate of
# record — telling that caller to re-run a 45-minute gate would be the wrong
# remedy, so this one path exits 3 and NAMES the tool.
BADBIN="$T/badbin"
mkdir -p "$BADBIN"
printf '#!/bin/sh\necho "awk: cannot allocate" >&2\nexit 1\n' >"$BADBIN/awk"
chmod +x "$BADBIN/awk"
OUT=$(PATH="$BADBIN:$BIN:$PATH" bash "$ASSERT" 2421 "$CERTIFIED" "$GOOD" 2>&1)
RC=$?
if [ "$RC" -eq 3 ]; then
  case "$OUT" in
    *"TOOL-FAILURE"*awk*) ok "tool failure: a failing awk is exit 3 and NAMES the tool" ;;
    *) bad "tool failure: exit 3 but the message must name the tool (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"NO-GATE-OF-RECORD"*) bad "tool failure: must NOT be reported as NO-GATE-OF-RECORD (got: $OUT)" ;;
    *) ok "tool failure: not misreported as a missing gate of record" ;;
  esac
else
  bad "tool failure: wanted exit 3, got $RC (output: $OUT)"
fi

# --- Case 37: the REAL --only shape -> refused by RESULT, not by the belt ----
# scripts/agent-gate.sh emits, for --only, the FULL markers, RESULT: PARTIAL and
# a LOWERCASE `mode: PARTIAL (--only …)` line. The MODE: belt is case-sensitive
# ($1 == "MODE:") and deliberately does NOT catch it: the property that matters
# is the RESULT compare. This is the most plausible real mis-invocation.
{
  printf '%s\n' "$FULL_S"
  printf 'run-id: /tmp/agent-gate.9cIQgX\n'
  printf 'commit: %s branch: issue-3465-require-gate-of-record dirty: no\n' "$C7"
  printf 'tree-start: %s dirty: no digest: 671a6275687c\n' "$C12"
  printf 'tree-end: %s dirty: no digest: 671a6275687c\n' "$C12"
  printf 'tree-integrity: PASS\n'
  printf 'mode: PARTIAL (--only file-size) - does NOT count as the gate\n'
  printf 'file-size:         PASS (0s)\n'
  printf 'RESULT: PARTIAL\n'
  printf '%s\n' "$FULL_E"
} >"$T/only-partial.txt"
if grep -q -x -F 'mode: PARTIAL (--only file-size) - does NOT count as the gate' "$T/only-partial.txt"; then
  ok "--only fixture: carries the LOWERCASE mode: PARTIAL line the gate really emits"
else
  bad "--only fixture: expected the verbatim lowercase mode: PARTIAL line"
fi
refused "a real --only summary (RESULT: PARTIAL) -> refuse" \
  "$T/only-partial.txt" "RESULT verdict token in the full-gate block is 'PARTIAL'"
case "$OUT" in
  *"carries a MODE: line"*)
    bad "--only: refused by the case-sensitive MODE: belt, not by RESULT (got: $OUT)" ;;
  *) ok "--only: the case-sensitive MODE: belt is deliberately NOT what catches it" ;;
esac

# --- Case 38: a NESTED sub-gate block -> refuse ------------------------------
# #2874: a gate spawned by an enclosing gate stamps `nested-under: <parent>` and
# emits the FULL markers at the SAME tree, so the sha binding provably cannot
# distinguish it. One affirmative line closes the only wrong-file class the sha
# compare cannot see.
{
  printf '%s\n' "$FULL_S"
  printf 'run-id: /tmp/agent-gate.nested1\n'
  printf 'nested-under: /tmp/agent-gate.9cIQgX\n'
  printf 'commit: %s branch: main dirty: no\n' "$C7"
  printf 'tree-start: %s dirty: no digest: 671a6275687c\n' "$C12"
  printf 'tree-integrity: PASS\n'
  printf 'RESULT: PASS\n'
  printf '%s\n' "$FULL_E"
} >"$T/nested.txt"
refused "nested sub-gate (nested-under:) at the RIGHT sha -> refuse" \
  "$T/nested.txt" "nested-under"
delta_summary "$T/delta-plain.txt"
{ grep -v -x -F "$DELTA_E" "$T/delta-plain.txt"
  printf 'nested-under: /tmp/agent-gate.9cIQgX\n'
  printf '%s\n' "$DELTA_E"
} >"$T/delta-nested.txt"
refused_pair "nested sub-gate in the DELTA block -> refuse" \
  "$ANCHORFULL" "$T/delta-nested.txt" "nested-under"

# --- Case 39: the success path DISCLAIMS what it did not prove (#3650) ------
# A squash-merge composes this diff with main's CURRENT tip, so the tree
# certified here and the tree that lands are different objects for any PR whose
# base is behind main. `PREMERGE: GATE-OF-RECORD` must not be readable as full
# certification.
if run 0 "success path prints the SCOPE disclaimer" 2421 "$CERTIFIED" "$GOOD"; then
  case "$OUT" in
    *"PREMERGE: SCOPE"*) ok "scope: the success path prints a PREMERGE: SCOPE clause" ;;
    *) bad "scope: missing the PREMERGE: SCOPE clause (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"#3650"*) ok "scope: the clause names the follow-up issue (#3650)" ;;
    *) bad "scope: the clause must name #3650 (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"does NOT prove"*) ok "scope: the clause states what was NOT proven" ;;
    *) bad "scope: the clause must state what was NOT proven (got: $OUT)" ;;
  esac
fi
if run 0 "success path prints SCOPE in the anchored-delta case too" \
  2421 "$CERTIFIED" "$ANCHORFULL" "$GOODDELTA"; then
  case "$OUT" in
    *"PREMERGE: SCOPE"*) ok "scope: the delta pair carries the same disclaimer" ;;
    *) bad "scope: the delta pair must carry the SCOPE clause too (got: $OUT)" ;;
  esac
fi
# #3650 SLICE 1 EXTENSION. Slice 1 ships the base-staleness ADVISORY, which does
# NOT close the merge-result gap, so all THREE original SCOPE lines are RETAINED
# verbatim and only ONE line is added pointing at the advisory. Pinning the three
# by their own text (not just by the marker) is what makes "retained" checkable:
# a reword that quietly dropped one would otherwise still satisfy Case 39 above.
SCOPE1="this proves a full gate PASSed on THIS tree"
SCOPE2="the tree was certified against current main (#3650)"
SCOPE3="composes this diff with main tip, which no gate here has executed"
for shape in direct delta; do
  if [ "$shape" = direct ]; then
    run 0 "SCOPE retained (direct)" 2421 "$CERTIFIED" "$GOOD" || continue
  else
    run 0 "SCOPE retained (anchored delta)" 2421 "$CERTIFIED" "$ANCHORFULL" "$GOODDELTA" || continue
  fi
  missing=""
  for needle in "$SCOPE1" "$SCOPE2" "$SCOPE3"; do
    [ "${OUT#*"$needle"}" = "$OUT" ] && missing="$missing | $needle"
  done
  if [ -z "$missing" ]; then
    ok "scope($shape): all THREE #3465 SCOPE lines are retained verbatim (#3650 slice 1)"
  else
    bad "scope($shape): a SCOPE line was dropped:$missing"
  fi
  case "$OUT" in
    *"PREMERGE: SCOPE the PREMERGE: ADVISORY lines below measure that gap"*)
      ok "scope($shape): the added SCOPE line points at the advisory" ;;
    *) bad "scope($shape): the added SCOPE line must point at the advisory (got: $OUT)" ;;
  esac
done

# --- Case 41: the base-staleness ADVISORY (#3650 slice 1) -------------------
# Slice 1's whole contract is that it changes NO verdict. The advisory is
# resolved from premerge-assert.sh's OWN directory with no env override
# (#3312's enforcer rule), so a case needing a different advisory SUBSTITUTES
# THE ARTIFACT in a scratch copy of the tree — never a path variable, which
# would be one more seam a real invoker could set.
#
# flow_copy <name> <advisory-body|ABSENT> — builds the scratch copy and sets the
# global $COPY. It sets a GLOBAL rather than printing a path on purpose: a
# command-substitution form runs in a SUBSHELL, so a bad() inside it would
# increment a counter that dies with the subshell, and a failed copy would make
# the caller's `&&` chain skip the case SILENTLY — a vacuous pass. The `local`
# declarations are split too: `local a="$1" d="$T/$a/x"` reads $a before the
# assignment takes effect and dies under `set -u`.
flow_copy() {
  local name="$1" body="$2"
  local d="$T/$name/flow"
  COPY=""
  mkdir -p "$d"
  if ! cp "$ASSERT" "$d/premerge-assert.sh"; then
    bad "flow_copy($name): could not copy premerge-assert.sh into the scratch tree"
    return 1
  fi
  if [ "$body" != ABSENT ]; then
    printf '%s\n' "$body" >"$d/base-staleness.sh"
    chmod +x "$d/base-staleness.sh"
  fi
  if [ ! -f "$d/premerge-assert.sh" ]; then
    bad "flow_copy($name): the scratch copy is missing after cp"
    return 1
  fi
  COPY="$d/premerge-assert.sh"
  return 0
}

# run_copy <expected-exit> <desc> <copied-assert> <args...>
run_copy() {
  local want="$1" desc="$2" script="$3"
  shift 3
  OUT=$(PATH="$BIN:$PATH" bash "$script" "$@" 2>&1)
  RC=$?
  if [ "$RC" -ne "$want" ]; then
    bad "$desc (exit $RC, wanted $want)"
    printf '     output: %s\n' "$OUT"
    return 1
  fi
  return 0
}

STALE_ADV='#!/usr/bin/env bash
printf "BASE-STALENESS: behind 107 commits (on origin/main, not reachable from the merge-base)\n"
printf "BASE-STALENESS: blast-radius 22 RECOGNISED of 107 commits behind\n"
printf "BASE-STALENESS: verdict STALE-RECOGNISED\n"
exit 4'
if flow_copy adv-stale "$STALE_ADV" &&
  run_copy 0 "a STALE-RECOGNISED advisory still merges (slice 1 changes no verdict)" \
    "$COPY" 2421 "$CERTIFIED" "$GOOD"; then
  case "$OUT" in
    *"PREMERGE: OK $CERTIFIED"*) ok "advisory: a stale base still reaches PREMERGE: OK" ;;
    *) bad "advisory: the verdict must be unchanged by the advisory (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: ADVISORY BASE-STALENESS: verdict STALE-RECOGNISED"*)
      ok "advisory: the finding is printed on PREMERGE: ADVISORY lines" ;;
    *) bad "advisory: STALE-RECOGNISED must be reported on ADVISORY lines (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: ADVISORY BASE-STALENESS: behind 107 commits"*)
      ok "advisory: every line of the advisory's report is carried through" ;;
    *) bad "advisory: the behind-count line must be carried through (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: SCOPE"*"#3650"*) ok "advisory: the SCOPE disclaimer is STILL printed alongside it" ;;
    *) bad "advisory: the SCOPE lines must survive (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"treat exit 5 /"*"UNMEASURED as STALE"*)
      ok "advisory: the UNMEASURED-is-stale consumer contract is stated at the merge point" ;;
    *) bad "advisory: the UNMEASURED-is-stale contract must be stated (got: $OUT)" ;;
  esac
fi

# NON-VACUITY for the four stub cases below: the STALE stub above genuinely
# carries the shape the case claims (exit 4 + the verdict token), so a case
# asserting "a stale advisory does not change the verdict" is not testing a stub
# that silently exits 0.
if [ "${STALE_ADV#*exit 4}" != "$STALE_ADV" ] &&
  [ "${STALE_ADV#*STALE-RECOGNISED}" != "$STALE_ADV" ]; then
  ok "advisory fixture: the stub really exits 4 and really reports STALE-RECOGNISED"
else
  bad "advisory fixture: the stale stub does not have the shape the case claims"
fi

for pair in "5:an UNMEASURED (exit 5) advisory" "9:a BROKEN (exit 9) advisory" \
  "0:a NO-STALENESS (exit 0) advisory"; do
  code="${pair%%:*}"
  what="${pair#*:}"
  body="#!/usr/bin/env bash
printf 'BASE-STALENESS: verdict from a stub exiting %s\n' $code
exit $code"
  if flow_copy "adv-exit-$code" "$body" &&
    run_copy 0 "$what cannot change the exit code" "$COPY" 2421 "$CERTIFIED" "$GOOD"; then
    case "$OUT" in
      *"PREMERGE: ADVISORY"*"exit $code"*)
        ok "advisory: $what is REPORTED with its exit code, and is not fatal" ;;
      *) bad "advisory: $what must be reported with exit $code (got: $OUT)" ;;
    esac
  fi
done

# An ABSENT advisory is reported, not fatal — the artifact is simply not copied.
if flow_copy adv-absent ABSENT &&
  run_copy 0 "an ABSENT advisory cannot fail the assert" "$COPY" 2421 "$CERTIFIED" "$GOOD"; then
  case "$OUT" in
    *"PREMERGE: ADVISORY base-staleness.sh is ABSENT"*)
      ok "advisory: an absent advisory is named on an ADVISORY line, and is not fatal" ;;
    *) bad "advisory: an absent advisory must be reported (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: OK $CERTIFIED"*) ok "advisory: the verdict is unchanged when the advisory is absent" ;;
    *) bad "advisory: an absent advisory must not change the verdict (got: $OUT)" ;;
  esac
fi

# An advisory printing NOTHING is reported too (an empty report is not a clean one).
SILENT_ADV='#!/usr/bin/env bash
exit 0'
if flow_copy adv-silent "$SILENT_ADV" &&
  run_copy 0 "a SILENT advisory is reported, not read as clean" "$COPY" 2421 "$CERTIFIED" "$GOOD"; then
  case "$OUT" in
    *"produced NO output"*) ok "advisory: an empty report is named rather than read as a finding" ;;
    *) bad "advisory: an empty report must be named (got: $OUT)" ;;
  esac
fi

# A REFUSAL is unaffected: the advisory runs only on the success path, so a
# refusing invocation carries no ADVISORY lines and still exits 2.
if flow_copy adv-refuse "$STALE_ADV" &&
  run_copy 2 "a refusal is unaffected by the advisory" "$COPY" 2421 "$CERTIFIED" "$T/lite-only.txt"; then
  case "$OUT" in
    *"PREMERGE: ADVISORY"*) bad "advisory: a refusal must not carry ADVISORY lines (got: $OUT)" ;;
    *) ok "advisory: a refusal path prints no ADVISORY lines and still exits 2" ;;
  esac
fi

# NON-VACUITY for the wiring itself: the stub cases above would all pass if the
# real script invoked some OTHER path. Run the REAL, shipped script and require
# the REAL advisory's own prefix to appear — that is the only case that proves
# the wiring points at scripts/flow/base-staleness.sh.
if run 0 "the SHIPPED script invokes the SHIPPED advisory" 2421 "$CERTIFIED" "$GOOD"; then
  # The needle is the shipped advisory's own PREFIX, not one of its measured
  # lines: the suite's cwd is not guaranteed to be a git work tree, and an
  # UNMEASURED run is a perfectly good demonstration that the shipped artifact
  # ran. Pinning a measured line here would red on a correct run.
  case "$OUT" in
    *"PREMERGE: ADVISORY BASE-STALENESS:"*)
      ok "advisory: the shipped premerge-assert really runs scripts/flow/base-staleness.sh" ;;
    *) bad "advisory: the shipped script must invoke the shipped advisory (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"PREMERGE: ADVISORY"*"NON-EXHAUSTIVE"*)
      ok "advisory: the advisory's own non-exhaustiveness travels to the merge point" ;;
    *) bad "advisory: the NON-EXHAUSTIVE lines must travel with the report (got: $OUT)" ;;
  esac
fi


# --- Case 40: the three exit-3 causes are DISTINGUISHABLE (nit 8) ------------
# Exit 3 covers a usage error, a tool failure and a gh failure. The CODES are
# unchanged (documented), so the caller tells them apart by the MARKER — a
# forgotten third argument must never be escalated upward as a GitHub outage.
if run 3 "usage error prints PREMERGE: USAGE" 2421 "$CERTIFIED"; then
  case "$OUT" in
    *"PREMERGE: USAGE"*) ok "exit 3: a usage error carries the PREMERGE: USAGE marker" ;;
    *) bad "exit 3: usage error must carry PREMERGE: USAGE (got: $OUT)" ;;
  esac
  case "$OUT" in
    *"GH-FAILURE"*|*"TOOL-FAILURE"*)
      bad "exit 3: a usage error must not read as a gh/tool failure (got: $OUT)" ;;
    *) ok "exit 3: a usage error is NOT reported as a gh failure" ;;
  esac
fi
export MOCK_GH_FAIL=1
if run 3 "gh failure prints PREMERGE: GH-FAILURE only" 2421 "$CERTIFIED" "$GOOD"; then
  case "$OUT" in
    *"PREMERGE: USAGE"*) bad "exit 3: a gh failure must not read as a usage error (got: $OUT)" ;;
    *) ok "exit 3: a gh failure is NOT reported as a usage error" ;;
  esac
fi
export MOCK_GH_FAIL=0

# --- summary -----------------------------------------------------------------
printf '\n=== premerge-assert: %d passed, %d failed ===\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
