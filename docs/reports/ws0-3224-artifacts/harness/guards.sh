# #3224 measurement guards — sourced, never run.
#
#   source docs/reports/ws0-3224-artifacts/harness/guards.sh
#
# WHY. Four of the six roborev findings on PR #3286 were the same defect wearing
# four costumes: a measurement step whose failure was PRINTED but not FATAL, so a
# failed capture could be analysed, published, or skipped on resume as though it
# had succeeded. In a harness whose entire deliverable is numbers with trustworthy
# provenance, a fail-open is a false-PASS, and a false-PASS costs the doctrine
# rather than a round. These functions are the single home for "a nonzero return
# code stops the run", so a future step cannot re-invent a softer version of it.
#
# CONTRACT: sourcing has NO side effects beyond defining functions. Every guard
# writes its diagnosis to stderr and returns non-zero; none of them exit, so the
# caller decides between `|| exit` and accumulating failures. Each diagnosis names
# WHAT failed, the VALUE that failed it, and the remedy — a guard whose message
# does not let the reader act is a guard they will learn to waive.

# ws0_guard_rc <label> <rc> [<extra-context>]
#   The plain "nonzero means stop" guard. Use it at EVERY measured command whose
#   rc was captured; printing rc and carrying on is what findings 2, 5 and 6 all
#   did.
ws0_guard_rc() {
  local label="$1" rc="$2" extra="${3:-}"
  if [ "$rc" -ne 0 ]; then
    printf 'FATAL: %s exited rc=%s — a failed measurement cannot be published.%s\n' \
      "$label" "$rc" "${extra:+ $extra}" >&2
    return 1
  fi
  return 0
}

# ws0_guard_all_rc_zero <name=rc> [<name=rc> ...]
#   Fail closed on ANY nonzero arm, and NAME the arms it checked. The named-arm
#   listing is the point: roborev finding #4 was a validity expression that
#   omitted two of the six arms it advertised, and an expression that silently
#   covers a subset is indistinguishable from one that covers all of them. This
#   prints the roster it actually tested, so the coverage is visible in the log
#   rather than inferred from the source.
ws0_guard_all_rc_zero() {
  local bad="" checked="" pair name rc
  for pair in "$@"; do
    name="${pair%%=*}"; rc="${pair#*=}"
    checked="$checked $name"
    case "$rc" in
      ''|*[!0-9]*) bad="$bad $name=<unrecordable:'$rc'>" ;;
      0) : ;;
      *) bad="$bad $name=$rc" ;;
    esac
  done
  if [ -z "$checked" ]; then
    printf 'FATAL: ws0_guard_all_rc_zero called with no arms — a validity check with no subject has no verdict to give.\n' >&2
    return 1
  fi
  if [ -n "$bad" ]; then
    printf 'FATAL: nonzero/unrecordable arm(s):%s (checked:%s)\n' "$bad" "$checked" >&2
    return 1
  fi
  printf 'rc all-zero across arms:%s\n' "$checked"
  return 0
}
