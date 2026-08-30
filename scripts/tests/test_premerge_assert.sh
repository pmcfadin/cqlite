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
# CLAUDE.md, issue threads and PR bodies quote these markers inline; and
# "==== END AGENT-GATE SUMMARY ====" CONTAINS the start marker as a substring, so
# a substring match would count every terminator as a new block.
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
# Colour SURVIVES redirection to a file, and the gate's own mandated capture is
# coloured (18 workflows set CARGO_TERM_COLOR=always). A parser keyed on
# uncoloured text would refuse a perfectly good gate of record.
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

# --- summary -----------------------------------------------------------------
printf '\n=== premerge-assert: %d passed, %d failed ===\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
