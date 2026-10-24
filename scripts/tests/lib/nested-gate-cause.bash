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
# nested_gate_stop_cause <reached-ERE> <summary-file>…
#   Pass ONLY the summaries whose assertion FAILED. rc 0 + ONE line on stdout naming the boundary
#   stop iff EVERY readable one of them carries `tree-integrity: FAIL` AND no line matching
#   <reached-ERE> — the evidence that the case's guard DID run (e.g. '^(missing-schemas|schemas): '
#   for the #3148 schemas preflight). So:
#     * a tree failure detected AFTER the guard ran (a later boundary, or the terminal capture)
#       leaves that evidence in the block, and the guard's own failure stands (roborev job 24);
#     * when several failed, ONE boundary-stopped run cannot relabel another that shows a genuine
#       regression (roborev job 30): any failed summary that is not a pre-guard stop keeps the
#       case's own text.
#   rc 1 and no output otherwise — including when no given file is readable, which is the case's
#   own failure to report, not a boundary stop.
nested_gate_stop_cause() {
  local reached="$1" f line first="" n=0
  shift
  [ -n "$reached" ] || return 1     # no guard evidence to test against: never claim "never ran"
  for f in "$@"; do
    [ -n "$f" ] && [ -r "$f" ] || continue
    line=$(grep -m1 '^tree-integrity: FAIL' "$f" 2>/dev/null) || return 1
    [ -n "$line" ] || return 1
    grep -Eq -- "$reached" "$f" 2>/dev/null && return 1   # this one reached the guard
    n=$((n + 1))
    [ -n "$first" ] || first=$line
  done
  [ "$n" -gt 0 ] || return 1
  printf 'the nested gate STOPPED at the tree-integrity boundary before this case'"'"'s guard ran — the cause is the tree, not the guard (#4171): %s%s\n' \
    "$first" "$([ "$n" -gt 1 ] && printf ' (all %s failed runs stopped there)' "$n")"
  return 0
}

# nested_gate_bad <message> <reached-ERE> <summary-file>…: the attribution-safe `bad`. When a
# nested summary shows a boundary stop BEFORE the guard ran, the reported line LEADS with that
# cause and labels the case's own text as NOT the cause; otherwise it is exactly `bad <message>`.
# Requires the caller's `bad` function.
nested_gate_bad() {
  local msg="$1" reached="$2" cause
  shift 2
  if cause=$(nested_gate_stop_cause "$reached" "$@"); then
    bad "${msg%%:*}: INCONCLUSIVE — $cause [case text, NOT the cause: ${msg#*: }]"
  else
    bad "$msg"
  fi
}
