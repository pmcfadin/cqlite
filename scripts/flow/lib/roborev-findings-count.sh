#!/usr/bin/env bash
#
# lib/roborev-findings-count.sh — the ONE definition of "how many findings does this
# roborev review text report", shared by scripts/flow/roborev-review-checks.sh (REVIEW
# time) and scripts/flow/premerge-review-binding.sh (MERGE time). Issue #4050.
#
# WHY THIS FILE EXISTS
# --------------------
# #3626 makes a lead-authorized findings deferral grant only when the marker's `count=`
# EQUALS the count the review observed — that equality is the whole affirmative half of
# the authorization. Review time had a count; the merge point did not, so
# `premerge-review-binding.sh` returned UNMEASURED for EVERY authorized-but-findings-
# bearing record and a validly deferred PR was permanently unmergeable (measured: PRs
# #3859, #3858, #3816 hard-blocked).
#
# The job record carries no findings-count FIELD, but it DOES carry the review TEXT
# (`roborev show <job> --json` → `output`/`verdict_text`; measured on this box's jobs
# 120/116/115, all `verdict='F'`, `status='done'`, 834/789/835 bytes, still retrievable
# days later). So the merge point can derive the SAME count from the SAME
# daemon-recorded artifact — and the only sound way to do that is to run the SAME CODE.
# A second implementation's correctness is only knowable by differential testing against
# the first (#3229), and a divergence between these two ends is an authorization bypass
# in either direction, so there is ONE definition and both callers source it.
#
# The recogniser was MOVED here VERBATIM from roborev-review-checks.sh, comments
# included, because the comments ARE the contract: the block terminator must stay
# LINE-INITIAL (matched mid-sentence it closed the block early when a finding's own
# prose contained the word "Summary:", under-counting the findings).
#
# THE RECOGNISER OVER PROSE DOES NOT CLOSE, AND THAT IS SOUND HERE FOR A REASON THAT
# MUST NOT BE WEAKENED (#3564, and see the merge-time call site):
#   * SAME CODE OVER IDENTICAL BYTES, BY CONSTRUCTION. A findings deferral can be GRANTED
#     only on the `--recheck-job` path (`roborev_check_findings_deferral` returns before
#     looking at anything unless `RECHECK_JOB` is set), and on that path the transcript IS
#     the record's review text: roborev-review.sh copies `$RECORD_OUTPUT_FILE` — the
#     `output`/`verdict_text` field, extracted by `roborev-job-facts.py` — over `$LOG`
#     before any text check runs. So both ends run THIS code over the SAME daemon-recorded
#     bytes, and the non-closure cannot produce a review-vs-merge disagreement or widen
#     what review time already granted. That follows from the recheck-only restriction, not
#     from luck: a deferral is never granted off a LIVE reviewer transcript, which is the one
#     input that could have diverged from the stored record.
#   * NOTHING here derives CLEANLINESS from prose. `NONE`/clean stays reachable ONLY
#     from the record's structured verdict letter. This answers "how many", and only for
#     a record already affirmatively `F`.
# It does NOT make the count tamper-proof against a party who can write roborev's
# database; that actor is invoker-class and out of model (#3312's triage rule).
#
# CONTRACT FOR CALLERS — THREE-VALUED, and the third value is never folded onto a
# measurement:
#
#   roborev_findings_block       <transcript> <block-out>  0 = a block was extracted
#                                                          1 = the transcript could not
#                                                              be read / awk failed
#                                 (<block-out> is TRUNCATED either way, so a caller that
#                                  wants the file to exist unconditionally still gets it)
#   roborev_findings_marker_count <block-file>             echoes a non-negative integer
#                                                          and returns 0 when a census
#                                                          was TAKEN; echoes NOTHING and
#                                                          returns 1 when it could not be
#   roborev_findings_count       <transcript> <block-out>   the two above, composed
#
# A `0` from these functions means "measured, and there are none" — it NEVER means "could
# not measure", which is the empty/return-1 answer. A caller must decide what to do with
# each; folding the unknown answer onto the measured one is the permissive collapse this
# repository refuses everywhere else.
#
# CONSTRAINTS
#   macOS bash 3.2 compatible. SOURCED, never executed: it defines functions and nothing
#   else — no `set -e`, no side effects, no output at source time — so it cannot change a
#   sourcing script's shell options or emit an unanchored line into its output. Every
#   external tool has its stderr SUPPRESSED: both callers publish anchored output, and a
#   native `awk:`/`grep:` diagnostic from inside a sourced function is a line with no
#   prefix on the caller's stream (the #3822 rule, same reason).

# roborev_findings_block <transcript> <block-out> — extract the FINDINGS BLOCK.
#
# The block runs from a Findings heading/label to the Summary heading/label — the
# measured real shapes. Everything outside it (a quoted "[Low]" in prose, a severity
# word inside a Problem sentence) is ignored: deriving the count from a regex over the
# WHOLE transcript was a real weakness (codex, round 5), because incidental or QUOTED
# severity text anywhere in the output set findings: PRESENT, which then exempted a
# genuinely vacuous "no code changes" verdict from the authoritative tier-1 failure.
#
# THE TERMINATOR MUST BE A LINE-INITIAL Summary LABEL: matched mid-sentence it closed the
# block early when a finding's own prose contained the word, under-counting the findings.
roborev_findings_block() {
  local transcript="$1" out="$2" rc
  # TRUNCATE FIRST, UNCONDITIONALLY. The review-time caller's own fail-closed argument
  # depends on the block file existing and being EMPTY when nothing could be extracted;
  # a stale file from an earlier lookup would be read as this run's measurement.
  # THE SUPPRESSOR COMES FIRST: bash applies redirections LEFT TO RIGHT, so
  # `: >"$out" 2>/dev/null` prints its OWN unprefixed `Permission denied` before stderr
  # is diverted — a line with no anchor on the caller's stream (#3822 clause 6). The
  # <block-out> path is the CALLER's own scratch file, not repository content.
  : 2>/dev/null >"$out" || return 1
  # `-f` AND `-r`: an unreadable transcript is a THIRD answer, not zero findings.
  { [ -f "$transcript" ] && [ -r "$transcript" ]; } || return 1
  awk 'BEGIN { inblock = 0 }
       tolower($0) ~ /^[[:space:]]*#{1,4}[[:space:]]*(review[[:space:]]+)?findings?/ { inblock = 1; next }
       tolower($0) ~ /^[[:space:]]*findings?[[:space:]]*:/ { inblock = 1; next }
       tolower($0) ~ /^[[:space:]]*#{1,4}[[:space:]]*summary/ { inblock = 0 }
       tolower($0) ~ /^[[:space:]]*summary[[:space:]]*:/ { inblock = 0 }
       inblock { print }' "$transcript" 2>/dev/null >"$out"
  rc=$?
  [ "$rc" -eq 0 ] || return 1
  return 0
}

# roborev_findings_marker_count <block-file> — count the SEVERITY MARKERS in the block.
#
# THREE-VALUED. `grep` exits 1 for "no match", which IS a measurement (zero markers), and
# 2+ for a real failure, which is NOT — and reading the two alike is the two-valued
# collapse that turns an unperformable scan into a confident zero.
roborev_findings_marker_count() {
  local block="$1" raw rc n
  { [ -f "$block" ] && [ -r "$block" ]; } || return 1
  raw=$(grep -oiE '\*\*severity\*\*[[:space:]]*:[[:space:]]*(critical|high|medium|low)|\[(critical|high|medium|low)\]|(^|[^[:alnum:]])(critical|high|medium|low): ' "$block" 2>/dev/null)
  rc=$?
  case "$rc" in
    0 | 1) : ;;
    *) return 1 ;;
  esac
  if [ -z "$raw" ]; then
    printf '0'
    return 0
  fi
  n=$(printf '%s\n' "$raw" | wc -l | tr -d '[:space:]')
  # A `wc` that answered something that is not a number answered nothing.
  case "$n" in
    '' | *[!0-9]*) return 1 ;;
  esac
  printf '%s' "$n"
  return 0
}

# roborev_findings_count <transcript> <block-out> — the composed answer.
roborev_findings_count() {
  local transcript="$1" out="$2" count
  roborev_findings_block "$transcript" "$out" || return 1
  count=$(roborev_findings_marker_count "$out") || return 1
  case "$count" in
    '' | *[!0-9]*) return 1 ;;
  esac
  printf '%s' "$count"
  return 0
}
