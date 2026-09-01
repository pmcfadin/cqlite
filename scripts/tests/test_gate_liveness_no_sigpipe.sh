#!/usr/bin/env bash
# test_gate_liveness_no_sigpipe.sh — structural guard for issue #3803.
#
# THE DEFECT
# ----------
# `tooling-tests` FAILed the full gate of record on PR #3794 with 2 of 257 cases red in
# scripts/tests/test_gate_liveness.sh, both with the expected verdict CAUSE text replaced by:
#
#     scripts/gate-liveness.sh: line 338: printf: write error: Broken pipe
#
# The shape was `printf '%s\n' "$text" | grep -m1 "^$k: "`. `grep -m1` EXITS ON ITS FIRST MATCH,
# closing the read end while bash's BUILTIN `printf` may still be writing. bash does not die on
# SIGPIPE the way an external command does — it reports the failed write on stderr — so the
# diagnostic string a caller was about to read is replaced by shell noise. Under `set -o pipefail`
# (which scripts/gate-liveness.sh sets) it is worse than cosmetic: the pipeline's status becomes
# 141 even though grep matched, so `... || return 0` fires on a SUCCESSFUL read.
#
# It is timing-dependent: it fires under gate load and not on an idle box, so A GREEN RUN OF THE
# BEHAVIOURAL SUITE IS NOT EVIDENCE OF A FIX. That is why this guard is STRUCTURAL. It asserts the
# channel is GONE (#3312: remove the shared channel, do not pick a rarer delimiter) rather than
# trying to lose a race on purpose.
#
# THE RULE
# --------
# In scripts/gate-liveness.sh, no bash BUILTIN writer (`printf`/`echo`) may feed a pipe. Every such
# site is a herestring (`reader <<<"$text"`), which bash implements with a temp file: there is no
# writer left to take EPIPE, so an early-exiting reader is harmless. An EXTERNAL writer (grep, sed)
# feeding `| head -1` is NOT in scope — an external command takes SIGPIPE's default disposition and
# dies silently, emitting nothing onto the channel this file's callers read.
#
# The guard scans the SHIPPED file (never a copy or a model of it), so unrouting one site reds this
# suite instead of greening it — the idiom of scripts/tests/test_cargo_output_parsers.sh.
set -uo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../.." && pwd)
SUBJECT="$REPO_ROOT/scripts/gate-liveness.sh"

# Case floor (CLAUDE.md, #3544): a span-replacing edit that silently deletes cases yields a green
# tally over a shrunken suite. The count below must never go DOWN without a stated reason.
CASE_FLOOR=9

pass=0; fail=0; cases=0
ok()   { cases=$((cases+1)); pass=$((pass+1)); printf 'ok   %s\n' "$1"; }
bad()  { cases=$((cases+1)); fail=$((fail+1)); printf 'FAIL %s\n' "$1"; [ $# -gt 1 ] && printf '     %s\n' "$2"; }

# ---------------------------------------------------------------------------
# DECLARED SCOPE — printed on EVERY run, pass or fail.
#
# CLAUDE.md: "a narrowed lane DECLARES the narrowing at run time" — a lane that omits coverage
# silently is indistinguishable from one that covers it. This guard is NARROWED to ONE file by
# deliberate scope decision on #3803, and the two files below carry the SAME defect shape TODAY and
# are guarded by NOTHING. Naming them is the whole point of this block; a follow-up issue owns them.
# ---------------------------------------------------------------------------
declare -a UNGUARDED=(
  "scripts/flow/claim.sh"
  "scripts/flow/roborev-review-oracles.sh"
)
print_scope() {
  printf '\n==== DECLARED SCOPE (test_gate_liveness_no_sigpipe.sh, #3803) ====\n'
  printf 'guarded:   scripts/gate-liveness.sh (ONE file)\n'
  printf 'UNGUARDED: %d file(s) carrying the SAME builtin-writer-into-pipe shape, by scope decision\n' "${#UNGUARDED[@]}"
  local f
  for f in "${UNGUARDED[@]}"; do printf '           - %s\n' "$f"; done
  printf 'NON-EXHAUSTIVE: this guard recognises a PATTERN SET (a `printf`/`echo` builtin lexically\n'
  printf '           feeding a `|` on one line), NOT every possible spelling. A writer split across a\n'
  printf '           line continuation, hidden behind a function, or piped from a process substitution\n'
  printf '           is NOT recognised and would pass unseen.\n'
  printf '==== END DECLARED SCOPE ====\n\n'
}
print_scope

# ---------------------------------------------------------------------------
# The matcher. Emits "<lineno>:<text>" per offending line; nothing when clean.
# Full-line comments are skipped (this file's prose quotes the defective shape verbatim, and so
# does scripts/gate-liveness.sh's own commentary); a trailing comment is stripped only when it is
# preceded by whitespace and the line has no quote character after it, so nothing is guessed.
# ---------------------------------------------------------------------------
violations() {
  local file="$1"
  awk '
    { line = $0 }
    line ~ /^[[:space:]]*#/ { next }
    line ~ /(^|[^[:alnum:]_.\/-])(printf|echo)[[:space:]][^|]*\|[^|]/ {
      printf "%d:%s\n", NR, line
    }
  ' "$file"
}

# ---------------------------------------------------------------------------
# 1. The subject must EXIST and be READABLE. Never derive a pass from a scan that did not happen.
# ---------------------------------------------------------------------------
if [ -r "$SUBJECT" ]; then
  ok "1 subject is readable: $SUBJECT"
else
  bad "1 subject is readable" "REFUSING: $SUBJECT is absent or unreadable — the scan could not be performed"
  printf '\npassed=%d failed=%d cases=%d\n' "$pass" "$fail" "$cases"
  exit 1
fi

# ---------------------------------------------------------------------------
# 2. The subject must be NON-EMPTY, and the count is REPORTED — a guard that reports zero
#    violations over an empty subject greens vacuously (CLAUDE.md's ruling on the descoped lint).
# ---------------------------------------------------------------------------
subject_lines=$(wc -l <"$SUBJECT" | tr -d ' ')
subject_code=$(grep -cv '^[[:space:]]*\(#\|$\)' "$SUBJECT")
printf 'subject: %s (%s lines, %s non-comment non-blank)\n' "$SUBJECT" "$subject_lines" "$subject_code"
if [ "${subject_lines:-0}" -gt 100 ] && [ "${subject_code:-0}" -gt 100 ]; then
  ok "2 subject is non-empty (${subject_lines} lines, ${subject_code} code lines)"
else
  bad "2 subject is non-empty" "REFUSING: only ${subject_lines} lines / ${subject_code} code lines — this is not the shipped reader"
fi

# ---------------------------------------------------------------------------
# 3. POSITIVE CONTROL for the matcher. Without this, "0 violations" is indistinguishable from
#    "the matcher matches nothing at all". It must red on the EXACT shape #3803 reported.
# ---------------------------------------------------------------------------
tmp=$(mktemp -d) || { printf 'FAIL could not mktemp\n'; exit 1; }
trap 'rm -rf "$tmp"' EXIT

cat >"$tmp/pos.sh" <<'POS'
#!/usr/bin/env bash
_field() {
  local text="$1" k="$2" line
  line=$(printf '%s\n' "$text" | grep -m1 "^$k: ") || return 0
  printf '%s' "${line#"$k": }"
}
POS
pos_out=$(violations "$tmp/pos.sh")
pos_n=$(printf '%s' "$pos_out" | grep -c . )
if [ "$pos_n" -eq 1 ] && printf '%s' "$pos_out" | grep -q 'grep -m1'; then
  ok "3 positive control: the matcher NAMES the #3803 shape (line ${pos_out%%:*})"
else
  bad "3 positive control" "matcher found $pos_n site(s) in a fixture containing exactly one; the scan cannot be trusted"
fi

# ---------------------------------------------------------------------------
# 4. Second positive control: the two-hop `... | head -1` form, where the early exit is head's and
#    the builtin takes EPIPE at the far end of the pipeline.
# ---------------------------------------------------------------------------
cat >"$tmp/pos2.sh" <<'POS2'
#!/usr/bin/env bash
open_ln=$(printf '%s\n' "$t" | grep -nxF 'X' | head -1 | cut -d: -f1)
_bp=$(printf '%s\n' "$1" | sed -n 's/^beater-pid: //p' | head -1)
POS2
pos2_n=$(violations "$tmp/pos2.sh" | grep -c .)
if [ "$pos2_n" -eq 2 ]; then
  ok "4 positive control: the matcher NAMES both two-hop (| head -1) forms"
else
  bad "4 positive control (two-hop)" "expected 2 sites, matcher found $pos2_n"
fi

# ---------------------------------------------------------------------------
# 5. NEGATIVE CONTROL: the herestring form is the FIX and must NOT be flagged. A matcher that
#    reds on correct input is the guard agents learn to waive.
# ---------------------------------------------------------------------------
cat >"$tmp/neg.sh" <<'NEG'
#!/usr/bin/env bash
_field() {
  local text="$1" k="$2" line
  line=$(grep -m1 "^$k: " <<<"$text") || return 0
  printf '%s' "${line#"$k": }"
}
open_ln=$(grep -nxF 'X' <<<"$t" | head -1 | cut -d: -f1)
verdict() { echo "gate-liveness: $1 ($3)"; }
if [ "$a" = "b" ] || [ "$c" = "d" ]; then printf 'x\n'; fi
NEG
neg_n=$(violations "$tmp/neg.sh" | grep -c .)
if [ "$neg_n" -eq 0 ]; then
  ok "5 negative control: the herestring form, and \`||\`, are NOT flagged (0 RECOGNISED)"
else
  bad "5 negative control" "matcher flagged $neg_n site(s) in a clean fixture — false positives"
fi

# ---------------------------------------------------------------------------
# 6. THE ASSERTION. Scan the SHIPPED reader.
# ---------------------------------------------------------------------------
sub_out=$(violations "$SUBJECT")
sub_n=$(printf '%s' "$sub_out" | grep -c .)
if [ "$sub_n" -eq 0 ]; then
  ok "6 scripts/gate-liveness.sh: builtin-writer-into-pipe sites: 0 RECOGNISED"
else
  bad "6 scripts/gate-liveness.sh: builtin-writer-into-pipe sites" "$sub_n RECOGNISED — each can emit \`printf: write error: Broken pipe\` onto a verdict a caller reads (#3803). Use a herestring:"
  while IFS= read -r v; do
    [ -n "$v" ] || continue
    printf '     %s:%s\n' "$SUBJECT" "$v"
  done <<<"$sub_out"
fi

# ---------------------------------------------------------------------------
# 7. NON-VACUITY of case 6: the shipped file must actually CONTAIN the converted form. Without
#    this, deleting every reader from the file would satisfy case 6. The floor is a case floor,
#    not a target — it may only go DOWN with a stated reason.
# ---------------------------------------------------------------------------
HERESTRING_FLOOR=20
here_n=$(grep -c '<<<"\$' "$SUBJECT")
if [ "${here_n:-0}" -ge "$HERESTRING_FLOOR" ]; then
  ok "7 non-vacuity: $here_n herestring reader(s) present (floor $HERESTRING_FLOOR)"
else
  bad "7 non-vacuity" "only $here_n herestring reader(s) found, floor is $HERESTRING_FLOOR — case 6's clean verdict is not evidence of anything"
fi

# ---------------------------------------------------------------------------
# 8. The specific site named in #3803 must be the herestring form, by name.
# ---------------------------------------------------------------------------
if grep -q 'grep -m1 "\^\$k: " <<<"\$text"' "$SUBJECT"; then
  ok "8 the #3803 site (_field) reads via a herestring"
else
  bad "8 the #3803 site (_field)" "_field's reader is not the expected herestring form; #3803's own site is unpinned"
fi

# ---------------------------------------------------------------------------
# 9. The subject must still be syntactically valid bash after the conversions.
# ---------------------------------------------------------------------------
if bash -n "$SUBJECT" 2>"$tmp/syn.err"; then
  ok "9 subject parses (bash -n)"
else
  bad "9 subject parses (bash -n)" "$(cat "$tmp/syn.err")"
fi

printf '\npassed=%d failed=%d cases=%d (floor %d)\n' "$pass" "$fail" "$cases" "$CASE_FLOOR"
if [ "$cases" -lt "$CASE_FLOOR" ]; then
  printf 'FAIL case-floor: ran %d cases, floor is %d — a green tally over a shrunken suite is not a pass\n' "$cases" "$CASE_FLOOR"
  exit 1
fi
[ "$fail" -eq 0 ] || exit 1
exit 0
