# #3224 positive-control VERDICT LOGIC — the pure decision layer, sourced, never run.
#
#   source docs/reports/ws0-3224-artifacts/harness/verdict-logic.sh
#
# WHY THIS IS A SEPARATE FILE. Two of the six roborev findings on PR #3286 were
# fail-open defects in exactly these functions, and neither was reachable by any
# test: the logic lived inside a 590-line script whose every entry point needs
# perf, a C compiler and ~20 minutes of bare-metal chase time. A guard nothing can
# exercise is a guard nobody can show to be load-bearing. Everything here is a
# pure function of injected globals, so selftest-guards.sh can hand it the exact
# bad input each guard is supposed to catch and assert the rejection.
#
# CONTRACT: sourcing this file has NO side effects beyond defining functions and
# threshold defaults. It reads and writes these globals, all owned by the caller:
#
#   reads   MED[arm/event]      median count, or a non-numeric diagnosis token
#           MUXMIN[arm/event]   min enabled% seen across that arm's reps
#           ACCESSES            chase accesses per arm (for per-access rates)
#   writes  EV_VERDICT[event] EV_MOVE[event] EV_RATE[event]
#           MISSRATE_F MISSRATE_H MISSRATE_RISE   (compute_missrate only)

# ------------------------------------------------------------------ thresholds
# Single-homed here so the library and the script cannot disagree; the caller may
# override any of them BEFORE calling, which is how the selftest pins them.
: "${MOVE_MIN_MILLI:=2000}"      # P3/P5 minimum movement between arms, EITHER
                                 #   direction, x1000. Measured healthy-host
                                 #   margin: LLC-loads 3.54x, cache-refs ~8x.
: "${MISSRATE_MIN_MILLI:=1500}"  # P4 minimum rise in misses/loads, x1000.
                                 #   Measured healthy-host value 4.39x
                                 #   (13.95% -> 61.23%).
: "${MUX_MIN_PCT:=99}"           # below this a count is a multiplexed estimate

# ------------------------------------------------------------------ small math
isnum() { case "$1" in ''|*[!0-9]*) return 1 ;; *) return 0 ;; esac; }
fmt_milli() { printf '%d.%03d' "$(( $1 / 1000 ))" "$(( $1 % 1000 ))"; }
show_milli() { if [ "$1" = inf ]; then printf 'inf'; elif isnum "$1"; then fmt_milli "$1"; else printf 'na'; fi; }

ratio_milli() { # $1 hostile $2 friendly -> milli-ratio, or "inf"/"na"
  if ! isnum "$1" || ! isnum "$2"; then echo na; return; fi
  if [ "$2" -eq 0 ]; then if [ "$1" -gt 0 ]; then echo inf; else echo na; fi; return; fi
  echo $(( $1 * 1000 / $2 ))
}
rate_milli() { if isnum "$1"; then echo $(( $1 * 1000 / ACCESSES )); else echo na; fi; }

# Direction-agnostic movement: max/min. See positive-control.sh's P3-P5 header —
# raw LLC-loads legitimately FALLS in the hostile arm on healthy hardware (the
# prefetcher stops issuing them), so asserting a direction here is a false-FAIL
# generator.
move_milli() {
  if ! isnum "$1" || ! isnum "$2"; then echo na; return; fi
  local hi=$1 lo=$2
  [ "$1" -lt "$2" ] && { hi=$2; lo=$1; }
  if [ "$lo" -eq 0 ]; then if [ "$hi" -gt 0 ]; then echo inf; else echo na; fi; return; fi
  echo $(( hi * 1000 / lo ))
}

# LLC miss rate = misses/loads, x1000. The invariant that survives prefetcher
# behaviour: hostility raises the FRACTION of LLC accesses that miss.
compute_missrate() {
  MISSRATE_F=na; MISSRATE_H=na; MISSRATE_RISE=na
  local lf="${MED[friendly/LLC-loads]}" lh="${MED[hostile/LLC-loads]}"
  local mf="${MED[friendly/LLC-load-misses]}" mh="${MED[hostile/LLC-load-misses]}"
  isnum "$lf" && isnum "$lh" && isnum "$mf" && isnum "$mh" || return
  [ "$lf" -gt 0 ] && [ "$lh" -gt 0 ] || return
  MISSRATE_F=$(( mf * 1000 / lf )); MISSRATE_H=$(( mh * 1000 / lh ))
  [ "$MISSRATE_F" -gt 0 ] || { MISSRATE_RISE=inf; return; }
  MISSRATE_RISE=$(( MISSRATE_H * 1000 / MISSRATE_F ))
}

# ----------------------------------------------------------------- the verdict
#
# CHECK ORDER IS LOAD-BEARING AND WAS WRONG TWICE. Stated explicitly because both
# defects were invisible reorderings:
#
#  (1) MULTIPLEXING FIRST (roborev finding #3, PR #3286). A count taken at <99%
#      enabled is a SCALED ESTIMATE, so every downstream question about it —
#      "did it move?", "is it zero?", "did the rate rise?" — is being asked of a
#      number the hardware did not actually produce. It used to be a WARNING
#      printed by report_ev while the counter still returned OK, i.e. a
#      multiplexed estimate could be certified as a sound counter and carry the
#      whole gate to PASS. It is checked before the value tests because its
#      remedy (split the event group and re-run) must be applied before any
#      value-based diagnosis means anything.
#
#  (2) THE LLC-load-misses SPECIAL CASE BEFORE THE MOVEMENT GATE (roborev
#      finding #1, PR #3286). P4 gates that counter on the MISS RATE, never on
#      raw magnitude, and the reason is measured, not theoretical: on the
#      healthy i4i.metal target box raw LLC-load-misses moved only 1.240x
#      (54,391 -> 67,449) while the miss rate rose 4.39x (13.95% -> 61.23%).
#      Applying the >=2x movement gate first therefore returned
#      UNRELIABLE_NO_MOVEMENT and RED-FLAGGED A HEALTHY HOST — the exact
#      false-FAIL class the header block spends 40 lines warning against,
#      reintroduced by an ordering. That healthy-host case is now a regression
#      input in selftest-guards.sh, so re-inverting the order fails a test
#      instead of silently rejecting good hosts.
#
#      This does NOT weaken P4. The miss-rate gate keeps its teeth: a flat miss
#      rate is still UNRELIABLE_MISSRATE_FLAT and still fails, which the
#      selftest also pins.
declare -A EV_VERDICT EV_MOVE EV_RATE
evaluate() { # $1 event -> sets EV_VERDICT/EV_MOVE/EV_RATE
  local ev="$1" h="${MED[hostile/$1]}" f="${MED[friendly/$1]}"
  local m q; m="$(move_milli "$h" "$f")"; q="$(rate_milli "$h")"
  EV_MOVE[$ev]="$m"; EV_RATE[$ev]="$q"
  if ! isnum "$h" || ! isnum "$f"; then EV_VERDICT[$ev]="${h}"; return; fi

  # (1) multiplexing — EITHER arm. A scaled estimate is not a count.
  #
  # THE PERMISSIVE BRANCH IS KEYED ON THE AFFIRMATIVE VALUE, never on "not the bad
  # one" (roborev round 2 finding #5). An enabled percentage is a THREE-state
  # signal — healthy, below the floor, or UNREADABLE — and the first version of
  # this check tested only "below the floor", so the unreadable state inherited the
  # pass. Reading a percentage is the ONLY evidence that a count is not a
  # multiplexed estimate, so a percentage that could not be read is a NON-PASSING
  # state, not a missing nice-to-have: `MUX_UNREADABLE` from the accumulator lands
  # here as a non-numeric value and must FAIL rather than fall through `isnum`.
  #
  # The `:-100` default is deliberate and NOT the same hazard: it applies only to an
  # arm with no MUXMIN entry at all, which happens exactly when EV_STATUS != PROGRAMS
  # — and such an event has already returned above on the non-numeric MED check, so
  # the default is unreachable for any event that got this far. Recorded here because
  # a future reader will otherwise read it as the defect this comment is about.
  local mh="${MUXMIN[hostile/$ev]:-100}" mf="${MUXMIN[friendly/$ev]:-100}"
  if ! isnum "$mh" || ! isnum "$mf"; then
    EV_VERDICT[$ev]=UNRELIABLE_MUX_UNREADABLE; return
  fi
  if [ "$mh" -lt "$MUX_MIN_PCT" ] || [ "$mf" -lt "$MUX_MIN_PCT" ]; then
    EV_VERDICT[$ev]=UNRELIABLE_MULTIPLEXED; return
  fi

  # A hard zero is a defect in either arm-shape; a LOW friendly reading is not.
  if [ "$h" -eq 0 ] && [ "$f" -eq 0 ]; then EV_VERDICT[$ev]=SILENT_ZERO; return; fi
  if [ "$h" -eq 0 ]; then EV_VERDICT[$ev]=HOSTILE_ZERO; return; fi

  # (2) P4: LLC-load-misses is gated on the MISS RATE, never on raw movement.
  if [ "$ev" = LLC-load-misses ]; then
    if [ "$MISSRATE_RISE" = inf ]; then EV_VERDICT[$ev]=OK
    elif isnum "$MISSRATE_RISE" && [ "$MISSRATE_RISE" -ge "$MISSRATE_MIN_MILLI" ]; then EV_VERDICT[$ev]=OK
    elif [ "$MISSRATE_RISE" = na ]; then EV_VERDICT[$ev]=UNRELIABLE_MISSRATE_UNCOMPUTABLE
    else EV_VERDICT[$ev]=UNRELIABLE_MISSRATE_FLAT; fi
    return
  fi

  # P3/P5 and the advisory counters: must MOVE AT ALL, in either direction.
  local moved=0
  { [ "$m" = inf ] || { isnum "$m" && [ "$m" -ge "$MOVE_MIN_MILLI" ]; }; } && moved=1
  if [ "$moved" -eq 0 ]; then EV_VERDICT[$ev]=UNRELIABLE_NO_MOVEMENT; return; fi
  EV_VERDICT[$ev]=OK
}

# The mux figure report_ev prints. Kept next to the check that consumes it so the
# printed number and the gated number can never diverge.
ev_mux_min() { # $1 event -> the lower of the two arms' enabled percentages
  # An unreadable arm is REPORTED as unreadable, not silently replaced by the other
  # arm's healthy number: substituting the readable arm here would print a reassuring
  # percentage beside an UNRELIABLE_MUX_UNREADABLE verdict, which is the printed-number
  # /gated-number divergence report_ev's own comment warns about.
  local mh="${MUXMIN[hostile/$1]:-100}" mf="${MUXMIN[friendly/$1]:-100}"
  if ! isnum "$mh" || ! isnum "$mf"; then echo UNREADABLE; return; fi
  if [ "$mf" -lt "$mh" ]; then echo "$mf"; else echo "$mh"; fi
}
