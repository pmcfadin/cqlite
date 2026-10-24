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
# nested_gate_stop_cause <summary-file>…
#   rc 0 + ONE line on stdout naming the boundary stop, when any given summary carries
#   `tree-integrity: FAIL`; rc 1 and no output otherwise (including an absent/unreadable file —
#   that is the case's own failure to report, not a boundary stop).
nested_gate_stop_cause() {
  local f line
  for f in "$@"; do
    [ -n "$f" ] && [ -r "$f" ] || continue
    line=$(grep -m1 '^tree-integrity: FAIL' "$f" 2>/dev/null) || continue
    [ -n "$line" ] || continue
    printf 'the nested gate STOPPED at the tree-integrity boundary before this case'"'"'s guard ran — the cause is the tree, not the guard (#4171): %s\n' "$line"
    return 0
  done
  return 1
}

# nested_gate_bad <message> <summary-file>…: the attribution-safe `bad`. When a nested summary shows
# a boundary stop, the reported line LEADS with that cause and labels the case's own text as NOT the
# cause; otherwise it is exactly `bad <message>`. Requires the caller's `bad` function.
nested_gate_bad() {
  local msg="$1" cause
  shift
  if cause=$(nested_gate_stop_cause "$@"); then
    bad "${msg%%:*}: INCONCLUSIVE — $cause [case text, NOT the cause: ${msg#*: }]"
  else
    bad "$msg"
  fi
}
