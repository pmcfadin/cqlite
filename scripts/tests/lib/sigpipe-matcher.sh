#!/usr/bin/env bash
# shellcheck shell=bash
# sigpipe-matcher.sh — THE ONE matcher for the piped-builtin-writer (EPIPE/SIGPIPE) shape.
#
# WHY A LIB (#4061). This matcher had exactly one consumer, the #3803 structural guard over
# scripts/gate-liveness.sh. #4061 adds a SECOND: a class ratchet over every git-tracked
# scripts/**/*.sh. Two copies of a recogniser is the defect CLAUDE.md names outright — "a
# canonical form implemented twice diverges silently" — and this one is a 40-line awk program
# whose exact boundary rules cost nine review findings over six rounds to arrive at. So it lives
# here ONCE and both consumers source it:
#
#   scripts/tests/test_gate_liveness_no_sigpipe.sh  (#3803) — 33 cases that PIN this matcher:
#       positive controls, the six DECLARED false-positive classes, and residuals a/b/c
#       (incl. case 9t, which pins the escaped-command-word MISS so the declaration cannot
#       silently outgrow the code). Those cases are what makes the shared matcher trustworthy
#       for BOTH consumers — they differentially pin it, and they must keep passing UNCHANGED.
#   scripts/tests/test_scripts_sigpipe_ratchet.sh   (#4061) — the class ratchet.
#
# Sourced, never executed. It defines ONE function and sets no shell options, so it cannot
# change its caller's `set -e`/`pipefail` posture.
#
# THE RULE, THE DECLARED FALSE POSITIVES AND THE RESIDUALS are stated in full below and are
# printed at run time by BOTH consumers. A count from this matcher is a count of SHAPE MATCHES,
# never of confirmed hazards.

# ---------------------------------------------------------------------------
# The matcher. Emits "<lineno>:<text>" per offending line; nothing when clean.
#
# WHAT IT ACTUALLY DOES, stated so the comment cannot outrun the code (an earlier revision claimed
# a trailing-comment strip that was never implemented):
#   1. Skips WHOLE-LINE comments only. Nothing else is stripped, and no attempt is made to know
#      what is inside a quote.
#   2. Requires a bash builtin writer token (`printf`/`echo`) on the line.
#   3. Masks `||` so a logical OR is never read as a pipe, then splits the line on `|`.
#   (The numbered narrow recipe that used to be here — segment splitting plus a recognised-reader
#   set — described the design that was removed. See THE RULE at the top of this file.)
#
sigpipe_violations() {
  local file="$1"
  awk '
    # THE BROAD FORM (lead ruling on REQUEST-3803-B, 2026-09-03). A bash BUILTIN writer
    # (printf/echo) with a pipe anywhere after it on the same line is a FAIL. Full stop. No quote
    # tracking, no command-segment splitting, no recognised-reader set, no stage ordering.
    #
    # WHY THE NARROW FORM WAS ABANDONED. Six review rounds produced NINE findings in a ~50-line
    # matcher and the per-round count ROSE (1,1,1,2,2,3). Three of the nine were false NEGATIVES:
    # a quoted semicolon splitting a command and hiding a real hazard; an escaped quote
    # desynchronising the scanner and masking a real pipe; an unquoted backslash doing the same;
    # `echo|head` missed for want of whitespace after the builtin. Each fix to the recogniser
    # opened the next hole, because it was a recogniser over bash-as-written -- a grammar the
    # author controls, so the residual set never closes (#3312).
    #
    # THE ASYMMETRY THAT DECIDES IT (#3229). A guard with documented false-PASSes is worse than no
    # guard: it hides defects while reading green. A guard with loud false POSITIVES costs NOISE,
    # not blindness -- it fails in the direction someone notices and fixes. The broad form catches
    # every hazard raised across all six rounds, including the four false negatives above.
    #
    # THE DECLARED FALSE-POSITIVE SET -- accepted noise, not oversight. Each of these is CORRECT
    # code that this guard REPORTS. The remedy is always the same: restructure the line (move the
    # pipe off it, or split the statement). None occurs in the subject today.
    #   1. a pipe inside a format string        printf %s "a | b"
    #   2. a pipe inside a quoted argument      echo "col1|col2"
    #   3. a pipe in a trailing comment         printf %s "$x"   # see: cmd | head
    #   4. an unrelated later pipeline          v=$(printf %s "$x"); other | grep -q y
    #   5. a quoted option-looking pattern      printf %s "$t" | grep -e "text -q"
    #   6. a run-to-EOF reader                  printf %s "$t" | grep -c foo
    # NOT in this set, because the broad form gets it RIGHT: a writer in the LAST pipeline stage
    # (`producer | grep -q x | printf %s done`) feeds nothing and is correctly NOT reported.
    # Narrowing any of these is issue #3992, whose acceptance criteria are the nine findings.
    #
    # First `|` that is a pipe rather than half of `||`.
    # Searches from `from`, so the pipe found is one the WRITER could feed. Taking the first
    # pipe on the whole line instead was a FALSE NEGATIVE I shipped and caught by test:
    # `producer | printf %s "$x" | grep -q y` has a pipe BEFORE the writer, and skipping on
    # that comparison dropped a real hazard.
    function first_pipe(s, from,   i, c, n) {
      n = length(s)
      if (from < 1) from = 1
      for (i = from; i <= n; i++) {
        c = substr(s, i, 1)
        if (c != "|") continue
        if (substr(s, i + 1, 1) == "|") { i++; continue }
        if (i > 1 && substr(s, i - 1, 1) == "|") continue
        return i
      }
      return 0
    }
    { line = $0 }
    line ~ /^[[:space:]]*#/ { next }
    {
      # The writer token boundary is ANY non-word character or end of line, not just whitespace
      # or a pipe. Narrowing it to those two was a FALSE NEGATIVE (roborev job 110): bash accepts
      # a redirection immediately after the builtin, so `printf<<<"$t"|head -1`, `echo>&1|head -1`
      # and `printf>/dev/null|head -1` are all valid, all hazardous, and all evaded the guard.
      # Verified valid with `bash -n`. Excluding alnum/_/- from the boundary is what still keeps
      # `printfoo | head` and `echoes | head` out -- those are different words, not writers.
      # The two boundary classes are IDENTICAL on purpose. They were asymmetric -- `.` and `/`
      # excluded before the writer but accepted after it -- so `printf.local | head` and
      # `echo/tool | head` were reported as builtins (roborev job 116, undeclared false
      # positives). A boundary rule that differs by side is a rule nobody can state in one
      # sentence, which is how the asymmetry survived three rounds of review.
      if (!match(line, /(^|[^[:alnum:]_.\/-])(printf|echo)([^[:alnum:]_.\/-]|$)/)) next
      # Scan for the pipe from just past the WRITER WORD, not from RSTART. match() may consume a
      # LEADING boundary character, and if that character is itself a pipe then starting at RSTART
      # finds it and treats the writer as upstream of it -- reporting `producer|printf %s done`,
      # where the builtin is the FINAL stage and cannot take EPIPE (roborev job 115, an UNDECLARED
      # false positive that also contradicted case 9c). Equally, the scan must NOT start past the
      # TRAILING boundary character, because for `echo|head` that character IS the pipe and
      # skipping it would drop a real hazard. So: end of the writer word, then scan.
      wpos = RSTART
      if (substr(line, wpos, 1) !~ /[pe]/) wpos = wpos + 1   # a leading boundary char was consumed
      wlen = (substr(line, wpos, 1) == "p") ? 6 : 4          # printf | echo
      p = first_pipe(line, wpos + wlen)
      if (p == 0) next
      printf "%d:%s\n", NR, line
    }
  ' "$file"
}
