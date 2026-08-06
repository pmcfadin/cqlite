#!/usr/bin/env bash
# lib-corpus-boundary.sh — ARE THE BYTES BEING MEASURED STILL THE BYTES THAT WERE PINNED?
# (issue #3272 review round 22.)
#
# Sourced, not executed, and it sets NO shell options: `set -euo pipefail` in a library mutates the
# SOURCING shell's options, which is the caller's decision (the rule every `lib-*.sh` sibling
# follows). The driver sets all three itself.
#
# # Why this library exists, and why it is not "the guard already exists"
#
# Round 21 built `verify_corpus_boundary` in `scripts/perf/ws0_corpus_bytes.py`: it re-hashes the
# pinned components FROM DISK at a measurement boundary, refuses the rep and names what changed. It
# came with a 17-check suite, and every one of those checks passed. And NOTHING CALLED IT — the
# module was imported and re-exported, and no driver invoked it, so in a real session the guard
# never ran.
#
# That is not a tidy-up. It is #3249's defect with a different spelling: a hardcoded
# `_PERF_STATE="ok"` survived 118/118 tests, and an unwired guard is INDISTINGUISHABLE FROM IT from
# the outside — a green suite and zero protection. The bar this branch is held to is not "the guard
# exists" but "the guard has been OBSERVED TO FIRE", and a function nothing calls cannot fire at
# all. So the wiring is the finding, and this library is the one line that closes it.
#
# # Why a library rather than four lines in the driver
#
# `scripts/perf/ws0-baseline.sh` sits at a hard 950-line budget, and the gate's `file-size` ratchet
# is `.rs`-ONLY, so a shell file crosses its campsite-rule target SILENTLY (checked with `wc -l`,
# never left to the gate). The seam is the one every rig library owns — ONE question about whether a
# measurement means what it says:
#
#     lib-cpu.sh             are the pinned CPUs one physical core?
#     lib-host-state.sh      is the host's state put back?
#     lib-args.sh            are the arguments values this rig can measure?
#     lib-perf-lint.sh       is the counting domain CPU-wide?
#     lib-server.sh          which program did the Flight arm actually measure?
#     lib-outdir.sh          do the artifacts being read all come from ONE session?
#     lib-measure.sh         how is ONE rep of an arm executed, prewarmed and counted?
#     lib-binaries.sh        WHICH PROGRAMS are measured, and are they this revision's?
#     lib-inputs.sh          WHICH SCHEMA are the bytes read with, and WHICH REQUEST is asked?
#     lib-corpus-boundary.sh are the bytes still the PINNED bytes, MID-RUN?
#
# What stays in the driver is the CALL SITE, inside the rep loop, because the ORDER is the property:
# a boundary check that ran anywhere but between reps would verify the ends, which is exactly the
# window a pre/post pair is blind to.
#
# # WHY IT IS CALLED PER ARM-REP AND NOT ONCE PER ROUND
#
# The claim this rig produces is the `bare/flight` RATIO, and its numerator and denominator are
# measured by DIFFERENT ARMS WITHIN ONE ROUND. So a component replaced between this round's scan rep
# and this round's Flight rep lands DIRECTLY on the ratio — the two arms measured different bytes
# while every recorded identity check still agreed. A once-per-round check would step over that
# window. Each arm's rep therefore ends with a boundary, and since rep N's end is rep N+1's start,
# the pin covers the first boundary and the report-time re-hash the last: every boundary is covered.
#
# # WHAT THIS LIBRARY READS FROM THE DRIVER, stated because it is a real coupling
#
# `$OUT_DIR` (the session dir holding `session-corpus-pin.json`, which is what the bytes are
# compared AGAINST) and `$CORPUS`. That is the same coupling `lib-binaries.sh` and `lib-inputs.sh`
# record rather than hide, and under the driver's `set -u` an unset global here fails loudly rather
# than verifying an empty path.

# THIS LIBRARY'S OWN DIRECTORY, resolved from `BASH_SOURCE` at source time, for the same reason
# `lib-measure.sh` does it: `$HERE` is the DRIVER's global and is not in the coupling list above, so
# reaching for it would add an undocumented one — and a caller that sourced this without setting
# `HERE` would then die inside the measurement loop, mid-rep.
WS0_BOUNDARY_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# verify_corpus_boundary_or_refuse <label> — ONE measurement boundary. 0 when the pinned components
# are byte-identical to the pin; NON-ZERO, having named the component, when they are not.
#
# # Fail-closed, in every direction, and NONE of it resting on `set -e`
#
# The status is captured EXPLICITLY into `rc` and returned, so a caller's `|| exit 1` is what
# terminates the run. There is deliberately no `|| true`, no `2>/dev/null` and no `|| echo`
# anywhere in this file: round 21's F4 was exactly that — a swallowed status turning a refusal into
# a note — and a boundary check that warns is a boundary check that does not exist.
#
# Both non-zero statuses REFUSE, and they are distinguished only in the DIAGNOSTIC:
#
#   * exit 1 — the corpus changed (or a component could not be hashed, or the pin is unusable). The
#     verifier's own message names the component and is passed through verbatim.
#   * exit 2 — a USAGE error, i.e. this call site is wrong. It still refuses, because a boundary
#     that could not be checked has NOT been verified: "assume unchanged" is the vacuous pass the
#     whole check exists to remove.
#
# Anything else (a python that died, a signal) is also a refusal, for the same reason.
verify_corpus_boundary_or_refuse() {
  local label="$1" out rc=0
  out="$(python3 "$WS0_BOUNDARY_LIB_DIR/ws0_corpus_bytes.py" "$OUT_DIR" "$CORPUS" "$label" 2>&1)" \
    || rc=$?
  if [[ "$rc" -eq 0 ]]; then
    printf '%s\n' "$out"
    return 0
  fi
  printf '%s\n' "$out" >&2
  echo "FATAL: the corpus could not be verified UNCHANGED at measurement boundary '$label'," >&2
  echo "       so this rep is REFUSED and the session cannot be reported. The bytes on disk" >&2
  echo "       are compared against session-corpus-pin.json, which was stamped BEFORE the" >&2
  echo "       first rep — not against the corpus's own corpus-identity.json, which can be" >&2
  echo "       refreshed beside a replaced component and is self-consistent at every" >&2
  echo "       boundary. A mutation restored before reporting is invisible at BOTH ENDS of a" >&2
  echo "       session and visible only from inside the run, which is why this is checked" >&2
  echo "       here (#3272 rounds 21-22)." >&2
  if [[ "$rc" -eq 2 ]]; then
    echo "       This was a USAGE error (exit 2), i.e. THIS CALL SITE is wrong, not the" >&2
    echo "       corpus. It still refuses: a boundary that could not be checked has not" >&2
    echo "       been verified, and 'assume unchanged' is the vacuous pass this check" >&2
    echo "       exists to remove." >&2
  fi
  return "$rc"
}
