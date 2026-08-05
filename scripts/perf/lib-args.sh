#!/usr/bin/env bash
# lib-args.sh — the WS0 driver's ARGUMENT VALIDATION helpers (issue #3272 finding 5 +
# finding 7, hardened by review round 1).
#
# Sourced, not executed, and it sets NO shell options: `set -euo pipefail` in a library
# mutates the SOURCING shell's options, which is the caller's decision.
#
# Split out under the campsite rule, along a clean seam: these are two pure-ish
# validators over strings, with no dependency on the rig's state, and every one of them
# exists because a value that should have been refused was accepted. The CALL SITES stay
# in `ws0-baseline.sh` so what is actually validated remains visible at its top level —
# a validator nobody calls is the #3249 shape.
#
# Two functions, and both are hardened against the SAME class from two directions:
#
#   `require_positive_int`  — a positive integer inside a bound that AGREES with the
#                             reporter's (`ws0_validate.MAX_COUNT`), so a value the
#                             report would refuse is refused BEFORE the reps run.
#   `parse_duration_ms`     — `<n>ms`/`<n>s`/`<n>m` in DECIMAL, digit-capped before any
#                             arithmetic, with MALFORMED and TOO-LONG reported as
#                             DIFFERENT causes (`duration_reject`).

# Every numeric option is validated POSITIVE, up front, before any build, cache drop
# or measurement (issue #3272, finding 5). `--reps` had no validation at all: at
# `--reps 0` every `for rep in $(seq 1 0)` loop body was skipped, so the driver ran
# to completion having measured NOTHING and handed the reporter an empty session —
# which (pre-fix) also exited zero. A non-numeric `--reps abc` made `seq` emit
# nothing and print its own diagnostic into the middle of a "successful" run. The
# vacuous-green class: a run that measured nothing must not look like one that did.
#
# # The driver's cap and the reporter's cap AGREE (issue #3272 review)
#
# `--reps` had a 9-DIGIT cap here and the reporter has a 100,000 cap, so `--reps 200000`
# passed this validation, ran TWO HUNDRED THOUSAND reps — each one a full-corpus scan
# plus a Flight rep, i.e. days of measurement — and was refused only by `ws0_report.py`
# at the very end. Refusing a value after acting on it is not refusing it. The bound is
# now stated once, here, at the reporter's value, so the driver rejects up front exactly
# what the reporter would reject at the end.
#
# It is deliberately NOT read out of the python module at runtime: the driver must be
# able to validate its arguments before it depends on anything else, and a cap parsed by
# grepping a python file is a second failure mode. Instead the two are pinned equal by
# `scripts/tests/test_ws0_report_guards.sh` (the "driver's MAX_COUNT EQUALS
# ws0_validate.MAX_COUNT" case), which reads both and FAILS on a mismatch — the drift is
# caught mechanically rather than trusted to a comment. The reference used to name
# `test_ws0_fabrication_guards.sh`, which does not contain that assert (#3272 review round
# 2 nit): a pointer to the wrong file reads as coverage that does not exist, and the next
# reader either hunts for it or concludes the drift is unguarded.
MAX_COUNT=100000
require_positive_int() { # require_positive_int <flag> <value> [max]
  local flag="$1" value="$2" max="${3:-$MAX_COUNT}"
  case "$value" in
    ''|*[!0-9]*)
      echo "FATAL: --$flag must be a positive integer (got '$value')" >&2; exit 2 ;;
  esac
  # DIGIT COUNT first, before any arithmetic — bash arithmetic is signed 64-bit and
  # WRAPS SILENTLY, so a 20-digit value becomes an arbitrary (possibly small,
  # possibly negative) number and the range checks below would be comparing
  # something other than what the caller wrote. Measured: `99999999999999999999`
  # evaluates to 7766279631452241919. 9 digits is past any legitimate rep count or
  # port and cannot wrap.
  if [[ "${#value}" -gt 9 ]]; then
    echo "FATAL: --$flag is absurdly large (got '$value', ${#value} digits)." >&2
    echo "       Refused before arithmetic: bash arithmetic wraps at 64 bits, so a" >&2
    echo "       value this size would be range-checked as some other number." >&2
    exit 2
  fi
  # 10# for the same reason parse_duration_ms uses it: `08` is not octal here.
  if (( 10#$value < 1 )); then
    echo "FATAL: --$flag must be at least 1 (got '$value')." >&2
    echo "       A run with --$flag below 1 measures nothing, and a report over" >&2
    echo "       nothing is not a smaller version of the requested claim — it is a" >&2
    echo "       vacuous success (issue #3272)." >&2
    exit 2
  fi
  if [[ -n "$max" ]] && (( 10#$value > max )); then
    echo "FATAL: --$flag must be at most $max (got '$value')" >&2
    if [[ "$max" == "$MAX_COUNT" ]]; then
      echo "       This is the SAME bound ws0_report.py enforces (ws0_validate.MAX_COUNT)," >&2
      echo "       stated here so the value is refused BEFORE the reps run rather than" >&2
      echo "       after: the driver used to accept anything under 10 digits, so e.g." >&2
      echo "       --reps 200000 looped 200,000 full-corpus reps and was rejected only by" >&2
      echo "       the report at the end (issue #3272)." >&2
    fi
    exit 2
  fi
}

# parse_duration_ms <value> — echo milliseconds, non-zero on a malformed value.
# Accepts the loadgen's `<n>ms` / `<n>s` / `<n>m` forms only: a bare `45` is
# REJECTED rather than guessed at, since guessing seconds-vs-millis would silently
# measure a step 1000x from the one requested.
#
# EVERY component enters arithmetic as `10#$n` — DECIMAL, explicitly (issue #3272,
# finding 7). Bash's `$((...))` reads a leading-zero literal as OCTAL, which made
# this function silently wrong for a whole class of ordinary spellings:
#
#   `010s`     -> $((010 * 1000))   = 8000 ms.  A caller asking for 10s measured 8s.
#   `030ms`    -> $((030))          = 24 ms.
#   `08s`      -> a HARD bash error, "08: value too great for base (error token is
#                 08)", which the `case` then reported as "must be <n>ms, <n>s or
#                 <n>m" — a complaint about the FORMAT of a value whose format is
#                 fine, sending the reader after the wrong thing.
#   `010000ms` -> 4096 ms, i.e. UNDER the 5000ms cold ceiling while really being
#                 10s — so the octal parse could smuggle a BLENDED cold step past
#                 the guard added for #3096 finding 2.
#
# The regex already restricts `$n` to digits, so `10#` cannot fail on a value that
# reaches it; it only fixes the base. Note the `*ms` case must stay FIRST — `*s`
# would otherwise match `45ms` and leave a trailing `m`.
#
# The DIGIT-LENGTH CAP closes the OTHER half of the same defect class, and it is a
# bypass of exactly the same shape as the octal one. Bash arithmetic is signed
# 64-bit and WRAPS SILENTLY, so a large-but-well-formed value multiplied by 1000 or
# 60000 lands on a small positive number:
#
#   `2305843009213693956s` -> $((… * 1000)) wraps to **4000** ms, UNDER the 5000ms
#   cold ceiling. A caller could therefore smuggle a blended cold step past the
#   guard of #3096 finding 2 with an absurd duration, exactly as `010000ms` could
#   via the octal parse. Verified against this driver before the fix: the value ran
#   straight through the ceiling to the corpus check.
#
# A duration is capped at 9 digits — ~11.5 days in ms, ~31 years in seconds, far
# past any legitimate step — which keeps the largest product (999999999 * 60000)
# near 6e13, four orders of magnitude inside 2^63. So the multiply cannot wrap and
# the ceiling comparison is on the number the caller actually wrote. REJECTED rather
# than clamped: a clamp would measure a step other than the one requested without
# saying so, which is the failure mode this whole function is being hardened against.
#
# The two rejections are reported SEPARATELY (issue #3272 review). Both used to return
# `1`, so the caller printed "must be <n>ms, <n>s or <n>m" for a value whose FORMAT was
# perfectly fine and whose only problem was its LENGTH — reintroducing, for
# `99999999999999999999ms`, exactly the misleading format-complaint this function
# criticizes three paragraphs up for `08s`. A diagnostic that sends the reader after the
# wrong thing is a defect regardless of which branch produces it. So:
#   rc=1  MALFORMED   — no recognized unit, or non-digits before it.
#   rc=3  TOO LONG    — well-formed, but more digits than can be multiplied safely.
DURATION_MAX_DIGITS=9
DURATION_RC_MALFORMED=1
DURATION_RC_TOO_LONG=3
parse_duration_ms() {
  local v="$1" n
  case "$v" in
    *ms) n="${v%ms}" ;;
    *s)  n="${v%s}" ;;
    *m)  n="${v%m}" ;;
    *)   return "$DURATION_RC_MALFORMED" ;;
  esac
  [[ "$n" =~ ^[0-9]+$ ]] || return "$DURATION_RC_MALFORMED"
  # The DIGIT COUNT is compared before any arithmetic touches the value: a numeric
  # bound would itself be evaluated by the arithmetic that wraps.
  [[ "${#n}" -le "$DURATION_MAX_DIGITS" ]] || return "$DURATION_RC_TOO_LONG"
  case "$v" in
    *ms) echo "$((10#$n))" ;;
    *s)  echo "$((10#$n * 1000))" ;;
    *m)  echo "$((10#$n * 60000))" ;;
  esac
}

# The COLD-STEP CEILING, in milliseconds. Owned HERE, and read by the driver — never the
# other way round (#3272 review round 2 nit).
#
# `duration_reject` interpolated `$COLD_STEP_MAX_MS`, which was defined ONLY in
# `ws0-baseline.sh`. So this library was not self-contained: under the `set -u` its own
# header tells callers to expect, ANY other caller — a test sourcing it to drive one
# validator, a future second driver — died on an unbound variable instead of printing the
# diagnostic, and the `exit 2` on the next line never ran. A library that dies rather than
# diagnoses is worse than one that says nothing, because the failure names the wrong thing.
#
# The value lives with the function that quotes it, and the driver reads it from here.
# `${COLD_STEP_MAX_MS:-}` would have been the smaller edit and the wrong one: it defaults a
# LOAD-BEARING NUMBER to empty, so the diagnostic would read "slip UNDER the ms cold-step
# ceiling" and the ceiling comparison in the driver would compare against nothing.
COLD_STEP_MAX_MS=5000

# duration_reject <flag> <value> <rc> — the diagnostic for the SPECIFIC failure, then
# exit. Split out because both call sites need it and neither may guess the cause.
duration_reject() {
  local flag="$1" val="$2" rc="$3"
  if [[ "$rc" == "$DURATION_RC_TOO_LONG" ]]; then
    local digits="${val%%[^0-9]*}"
    echo "FATAL: --$flag is too LONG: '$val' carries ${#digits} digits, and the maximum" >&2
    echo "       is $DURATION_MAX_DIGITS (999999999ms is ~11.5 days; 999999999s is ~31 years)." >&2
    echo "       The FORMAT is fine — this is a RANGE refusal, stated as one. Bash" >&2
    echo "       arithmetic is signed 64-bit and wraps silently, so a longer value" >&2
    echo "       multiplied by 1000 or 60000 lands on some unrelated small number:" >&2
    echo "       2305843009213693956s wraps to 4000ms, which would slip UNDER the" >&2
    echo "       ${COLD_STEP_MAX_MS}ms cold-step ceiling and smuggle a blended cold" >&2
    echo "       measurement past that guard. Refused, never clamped." >&2
  else
    echo "FATAL: --$flag must be <n>ms, <n>s or <n>m (got '$val')" >&2
    echo "       A bare number is refused rather than guessed at: seconds-vs-millis" >&2
    echo "       would silently measure a step 1000x from the one requested." >&2
  fi
  exit 2
}
