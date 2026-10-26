# scripts/tests/lib/nested-gate-cause.bash — issue #4171
#
# A self-test that drives a NESTED full gate and then asserts on that gate's SUMMARY block reads a
# missing guard line as "the guard was defeated". That is only true if the nested run REACHED the
# guard. A nested run that STOPPED at the tree-integrity boundary (a mid-run tree mutation, or a
# capture failure) never ran it, and reporting the case's own assertion text then sends the operator
# after a guard that is fine — measured on #4159/PR #4162 and the 2026-10-23 nightly, where a parent
# heartbeat inside the checkout surfaced as `3148-…: an environment value defeated the schemas
# fail-closed guard`.
#
# nested_gate_stop_cause <pre-guard-ERE> <summary-file>…
#   Pass ONLY the summaries whose assertion FAILED. rc 0 + ONE line on stdout naming the boundary
#   stop iff EVERY one of them PROVES, from the gate's own boundary-stage record, that the run
#   stopped BEFORE the case's guard could execute:
#     * `tree-integrity: FAIL`, AND
#     * `components-completed: … (run STOPPED at the tree-integrity boundary …` — a boundary stop,
#       not a terminal-capture failure after everything ran, AND
#     * its `detected-after-component:` line matches <pre-guard-ERE>: the components the gate runs
#       BEFORE that guard (e.g. '^detected-after-component: (file-size)$' for the #3148 schemas
#       preflight — `run_file_size` executes before both preflights, and its boundary stops the run
#       before they are reached).
#   POSITIVE evidence only (roborev job 35): the ABSENCE of a guard's output is never read as "the
#   guard never ran", because a guard that runs and regresses silently leaves the same absence.
#   rc 1 and no output otherwise — any absent, unreadable or EMPTY summary (it proves nothing about
#   where that run stopped; roborev job 31), any summary lacking the proof, or none given. So one
#   boundary-stopped run cannot relabel another that shows a genuine regression (roborev job 30).
nested_gate_stop_cause() {
  local pre="$1" f line first="" n=0
  shift
  [ -n "$pre" ] || return 1         # no pre-guard boundary set: never claim "never ran"
  for f in "$@"; do
    [ -n "$f" ] && [ -r "$f" ] && [ -s "$f" ] || return 1
    line=$(grep -m1 '^tree-integrity: FAIL' "$f" 2>/dev/null) || return 1
    [ -n "$line" ] || return 1
    grep -q '^components-completed: .*run STOPPED at the tree-integrity boundary' "$f" 2>/dev/null || return 1
    grep -Eq -- "$pre" "$f" 2>/dev/null || return 1
    n=$((n + 1))
    [ -n "$first" ] || first=$line
  done
  [ "$n" -gt 0 ] || return 1
  printf 'the nested gate STOPPED at the tree-integrity boundary before this case'"'"'s guard ran — the cause is the tree, not the guard (#4171): %s%s\n' \
    "$first" "$([ "$n" -gt 1 ] && printf ' (all %s failed runs stopped there)' "$n")"
  return 0
}

# nested_gate_bad <message> <pre-guard-ERE> <failed-summary-file>…: the attribution-safe `bad`.
# When every failed summary PROVES a boundary stop before the guard ran, the reported line LEADS
# with that cause and labels the case's own text as NOT the cause; otherwise exactly `bad <message>`.
# Requires the caller's `bad` function.
nested_gate_bad() {
  local msg="$1" pre="$2" cause
  shift 2
  if cause=$(nested_gate_stop_cause "$pre" "$@"); then
    bad "${msg%%:*}: INCONCLUSIVE — $cause [case text, NOT the cause: ${msg#*: }]"
  else
    bad "$msg"
  fi
}
