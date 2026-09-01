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
# In scripts/gate-liveness.sh, no bash BUILTIN writer (`printf`/`echo`) may feed a pipeline that
# contains a SHORT-CIRCUITING reader. Every such site is a herestring (`reader <<<"$text"`), which
# bash implements with a temp file: there is no writer left to take EPIPE, so an early-exiting
# reader is harmless.
#
# TWO THINGS THIS DELIBERATELY DOES NOT ASSERT.
#   * An EXTERNAL writer (grep, sed) feeding a short-circuiting reader is NOT a violation: an
#     external command takes SIGPIPE's default disposition and dies silently, emitting nothing onto
#     the channel this file's callers read. Only a bash builtin narrates its own failed write.
#   * A builtin feeding a reader that runs to EOF (`grep -c`, `cut`, `wc`) is NOT a violation:
#     nothing closes the pipe early, so no write can fail.
# Narrowing to the ACTUAL hazard is what keeps this guard from redding on correct code — and a lane
# that reds on correct input is the lane agents learn to waive.
#
# The guard scans the SHIPPED file (never a copy or a model of it), so unrouting one site reds this
# suite instead of greening it — the idiom of scripts/tests/test_cargo_output_parsers.sh.
set -uo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../.." && pwd)
SUBJECT="$REPO_ROOT/scripts/gate-liveness.sh"

# Case floor (CLAUDE.md, #3544): a span-replacing edit that silently deletes cases yields a green
# tally over a shrunken suite. This is ENFORCED (exit 1), not merely printed, and may only go DOWN
# with a stated reason.
CASE_FLOOR=13

pass=0; fail=0; cases=0
ok()   { cases=$((cases+1)); pass=$((pass+1)); printf 'ok   %s\n' "$1"; }
bad()  { cases=$((cases+1)); fail=$((fail+1)); printf 'FAIL %s\n' "$1"; [ $# -gt 1 ] && printf '     %s\n' "$2"; return 0; }

# ---------------------------------------------------------------------------
# DECLARED SCOPE — printed on EVERY run, pass or fail.
#
# CLAUDE.md: "a narrowed lane DECLARES the narrowing at run time" — a lane that omits coverage
# silently is indistinguishable from one that covers it. This guard is narrowed on TWO axes: to ONE
# FILE (a scope decision on #3803), and to a RECOGNISED READER SET (the fix for a measured
# false-positive class). Both narrowings, and the residual each buys, are named here.
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
  printf 'recognised readers (a pipeline segment whose FIRST WORD is one of these is a hazard):\n'
  printf '           head | grep with a -q/-m flag | read | sed with a `q` command | awk containing `exit`\n'
  printf 'NON-EXHAUSTIVE, and these are the residuals the narrowing buys:\n'
  printf '           1. A builtin feeding an UNRECOGNISED short-circuiting reader is NOT DETECTED.\n'
  printf '              The reader set above is a recognised list, not a proof about every command\n'
  printf '              that can exit before EOF; a new one passes unseen until it is added here.\n'
  printf '           2. The scan is LEXICAL and per-line. It does NOT tokenise bash: a writer split\n'
  printf '              across a line continuation, hidden behind a function, or fed from a process\n'
  printf '              substitution is NOT recognised.\n'
  printf '           3. Comments are NOT stripped beyond whole-line ones. A `|` followed by a\n'
  printf '              recognised reader INSIDE a trailing comment would be REPORTED. This is\n'
  printf '              deliberate: stripping it needs quote-awareness, and a quote-aware strip that\n'
  printf '              guesses wrong drops a REAL site (a false negative in a mandatory gate\n'
  printf '              component), which is the worse direction.\n'
  printf '==== END DECLARED SCOPE ====\n\n'
}
print_scope

# ---------------------------------------------------------------------------
# The matcher. Emits "<lineno>:<text>" per offending line; nothing when clean.
#
# WHAT IT ACTUALLY DOES, stated so the comment cannot outrun the code (an earlier revision claimed
# a trailing-comment strip that was never implemented):
#   1. Skips WHOLE-LINE comments only. Nothing else is stripped, and no attempt is made to know
#      what is inside a quote.
#   2. Requires a bash builtin writer token (`printf`/`echo`) on the line.
#   3. Masks `||` so a logical OR is never read as a pipe, then splits the line on `|`.
#   4. Reports the line ONLY IF some segment AFTER the first begins with a RECOGNISED
#      short-circuiting reader. This is the narrowing that removes the false-positive class: in
#      `printf 'a | b\n'`, `echo "col1|col2"`, `printf '%s\n' "x"  # note |` and
#      `v=$(printf '%s' "$x"); other | thing`, the token after the pipe is not a recognised reader,
#      so none of them is reported.
# ---------------------------------------------------------------------------
violations() {
  local file="$1"
  awk '
    { line = $0 }
    line ~ /^[[:space:]]*#/ { next }
    line !~ /(^|[^[:alnum:]_.\/-])(printf|echo)[[:space:]]/ { next }
    {
      rest = line
      gsub(/\|\|/, "\001", rest)        # a logical OR is not a pipe
      n = split(rest, seg, "|")
      if (n < 2) next
      hazard = 0
      for (i = 2; i <= n; i++) {
        s = seg[i]
        sub(/^[[:space:]]+/, "", s)
        sub(/^&[[:space:]]*/, "", s)     # `|&` redirects stderr too; still a pipe
        sub(/^[[:space:]]+/, "", s)
        if (match(s, /^[A-Za-z_][A-Za-z0-9_.-]*/) == 0) continue
        w = substr(s, 1, RLENGTH)
        if (w == "head")                                              hazard = 1
        else if (w == "grep" && s ~ /(^|[[:space:]])-[A-Za-z]*[qm]/)  hazard = 1
        else if (w == "read")                                         hazard = 1
        else if (w == "sed"  && s ~ /(^|[;'\''"[:space:]])[0-9]*q([;'\''"[:space:]]|$)/) hazard = 1
        else if (w == "awk"  && s ~ /exit/)                           hazard = 1
      }
      if (hazard) printf "%d:%s\n", NR, line
    }
  ' "$file"
}

n_violations() { violations "$1" | grep -c . ; }

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

tmp=$(mktemp -d) || { printf 'FAIL could not mktemp\n'; exit 1; }
trap 'rm -rf "$tmp"' EXIT

# ---------------------------------------------------------------------------
# 3. POSITIVE CONTROL for the matcher. Without this, "0 violations" is indistinguishable from
#    "the matcher matches nothing at all". It must red on the EXACT shape #3803 reported.
# ---------------------------------------------------------------------------
cat >"$tmp/pos.sh" <<'POS'
#!/usr/bin/env bash
_field() {
  local text="$1" k="$2" line
  line=$(printf '%s\n' "$text" | grep -m1 "^$k: ") || return 0
  printf '%s' "${line#"$k": }"
}
POS
pos_out=$(violations "$tmp/pos.sh")
pos_n=$(grep -c . <<<"$pos_out")
if [ "$pos_n" -eq 1 ] && grep -q 'grep -m1' <<<"$pos_out"; then
  ok "3 positive control: the matcher NAMES the #3803 shape (line ${pos_out%%:*})"
else
  bad "3 positive control" "matcher found $pos_n site(s) in a fixture containing exactly one; the scan cannot be trusted"
fi

# ---------------------------------------------------------------------------
# 4. Second positive control: the two-hop `... | head -1` form, where the early exit is head's and
#    the builtin takes EPIPE at the far end of the pipeline. The token right after the pipe here is
#    `grep -nxF` / `sed -n`, NEITHER of which short-circuits — the hazard is downstream, so the
#    matcher must scan EVERY segment and not just the first.
# ---------------------------------------------------------------------------
cat >"$tmp/pos2.sh" <<'POS2'
#!/usr/bin/env bash
open_ln=$(printf '%s\n' "$t" | grep -nxF 'X' | head -1 | cut -d: -f1)
_bp=$(printf '%s\n' "$1" | sed -n 's/^beater-pid: //p' | head -1)
POS2
pos2_n=$(n_violations "$tmp/pos2.sh")
if [ "$pos2_n" -eq 2 ]; then
  ok "4 positive control: the matcher NAMES both two-hop (| head -1) forms"
else
  bad "4 positive control (two-hop)" "expected 2 sites, matcher found $pos2_n"
fi

# ---------------------------------------------------------------------------
# 5. NEGATIVE CONTROL: the herestring form is the FIX and must NOT be flagged, and `||` is not a
#    pipe. A matcher that reds on correct input is the guard agents learn to waive.
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
cnt=$(printf '%s\n' "$t" | grep -c '^x: ')
NEG
neg_n=$(n_violations "$tmp/neg.sh")
if [ "$neg_n" -eq 0 ]; then
  ok "5 negative control: herestrings, \`||\`, and a run-to-EOF reader are NOT flagged (0 RECOGNISED)"
else
  bad "5 negative control" "matcher flagged $neg_n site(s) in a clean fixture — false positives"
fi

# ---------------------------------------------------------------------------
# 6-9. NEGATIVE CONTROLS for the measured FALSE-POSITIVE CLASS (roborev job 20).
#
# The first matcher treated ANY `|` after a builtin as a pipeline, so all four shapes below were
# reported. scripts/gate-liveness.sh contains none of them TODAY, so the guard was green BY LUCK:
# anyone adding one would have redded the MANDATORY tooling-tests component on CORRECT code. These
# four cases are the regression pins. Each is a SEPARATE case so a fix that repairs three of them
# cannot hide behind an aggregate.
# ---------------------------------------------------------------------------
_fp_case() { # _fp_case <n> <label> <line-of-bash>
  local n="$1" label="$2" body="$3" got
  printf '%s\n' '#!/usr/bin/env bash' >"$tmp/fp$n.sh"
  printf '%s\n' "$body" >>"$tmp/fp$n.sh"
  got=$(n_violations "$tmp/fp$n.sh")
  if [ "$got" -eq 0 ]; then
    ok "$n negative control (false-positive class): $label — 0 RECOGNISED"
  else
    bad "$n negative control (false-positive class): $label" "matcher flagged $got site(s) on CORRECT code: $body"
  fi
}
_fp_case 6 "a pipe inside the FORMAT STRING"        "printf 'a | b\\n'"
_fp_case 7 "a pipe inside a TRAILING COMMENT"       'printf '"'"'%s\n'"'"' "x"   # comment with |'
_fp_case 8 "a pipe inside a QUOTED ARGUMENT"        'echo "col1|col2"'
_fp_case 9 "an UNRELATED later pipeline, same line" 'v=$(printf '"'"'%s'"'"' "$x"); other | thing'

# ---------------------------------------------------------------------------
# 10. THE ASSERTION. Scan the SHIPPED reader.
# ---------------------------------------------------------------------------
sub_out=$(violations "$SUBJECT")
sub_n=$(grep -c . <<<"$sub_out")
if [ "$sub_n" -eq 0 ]; then
  ok "10 scripts/gate-liveness.sh: builtin-writer-into-short-circuiting-reader sites: 0 RECOGNISED"
else
  bad "10 scripts/gate-liveness.sh: builtin-writer-into-short-circuiting-reader sites" "$sub_n RECOGNISED — each can emit \`printf: write error: Broken pipe\` onto a verdict a caller reads (#3803). Use a herestring:"
  while IFS= read -r v; do
    [ -n "$v" ] || continue
    printf '     %s:%s\n' "$SUBJECT" "$v"
  done <<<"$sub_out"
fi

# ---------------------------------------------------------------------------
# 11. NON-VACUITY of case 10: the shipped file must actually CONTAIN the converted form. Without
#     this, deleting every reader from the file would satisfy case 10. The floor is a case floor,
#     not a target — it may only go DOWN with a stated reason.
# ---------------------------------------------------------------------------
HERESTRING_FLOOR=20
here_n=$(grep -c '<<<"\$' "$SUBJECT")
if [ "${here_n:-0}" -ge "$HERESTRING_FLOOR" ]; then
  ok "11 non-vacuity: $here_n herestring reader(s) present (floor $HERESTRING_FLOOR)"
else
  bad "11 non-vacuity" "only $here_n herestring reader(s) found, floor is $HERESTRING_FLOOR — case 10's clean verdict is not evidence of anything"
fi

# ---------------------------------------------------------------------------
# 12. The specific site named in #3803 must be the herestring form, by name.
# ---------------------------------------------------------------------------
if grep -q 'grep -m1 "\^\$k: " <<<"\$text"' "$SUBJECT"; then
  ok "12 the #3803 site (_field) reads via a herestring"
else
  bad "12 the #3803 site (_field)" "_field's reader is not the expected herestring form; #3803's own site is unpinned"
fi

# ---------------------------------------------------------------------------
# 13. The subject must still be syntactically valid bash after the conversions.
# ---------------------------------------------------------------------------
if bash -n "$SUBJECT" 2>"$tmp/syn.err"; then
  ok "13 subject parses (bash -n)"
else
  bad "13 subject parses (bash -n)" "$(cat "$tmp/syn.err")"
fi

printf '\npassed=%d failed=%d cases=%d (floor %d)\n' "$pass" "$fail" "$cases" "$CASE_FLOOR"
if [ "$cases" -lt "$CASE_FLOOR" ]; then
  printf 'FAIL case-floor: ran %d cases, floor is %d — a green tally over a shrunken suite is not a pass\n' "$cases" "$CASE_FLOOR"
  exit 1
fi
[ "$fail" -eq 0 ] || exit 1
exit 0
