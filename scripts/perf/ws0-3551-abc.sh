#!/usr/bin/env bash
# ws0-3551-abc.sh — the interleaved A/B/C(/C0) driver for issue #3551.
#
# WHY THIS EXISTS. `ws0-baseline.sh` measures ONE configuration per invocation, so an A/B/C
# comparison is a SET of its sessions and the interleaving is a property of HOW they are
# ordered. `docs/reports/ws0-3096-artifacts/measurement-method.md` §3b requires, verbatim:
# one rep at a time, never all reps of an arm back to back (step 1); the arm order rotated
# every round (step 2); the drift control carried in EVERY run (step 3); differences taken
# WITHIN a round and the direction count reported (step 4); rows/s AND cycles/row AND IPC per
# run (step 5). §3b.1 states plainly that the committed rig implements NONE of that and makes
# no interleaving claim. This script is that operator obligation, written down and runnable
# instead of performed by hand and asserted afterwards.
#
# WHAT IT CLAIMS AND WHAT IT DOES NOT. It claims the ORDER IT EXECUTED, because it executed
# it: the rotation is computed here and every session's position is recorded here. It does NOT
# claim the box was quiet — that is `ws0_quiescence.py`'s job, passed through per session — and
# it does not claim the arms differ only as labelled; each session's own recorded pinning is
# the authority for that, which is why the aggregator reads configuration back OUT of the
# artifacts rather than restating this file's table.
#
# THE CONTROL, which is the whole reason the arms are shaped this way. Only `--flight-server-cpus`
# and the allocator knobs vary; `--server-cpus` is IDENTICAL in every arm, so the bare-scan leg
# is code-identical AND pin-identical everywhere and its movement across arms is drift plus
# contamination and nothing else. That is §3b step 3's control. Vary `--server-cpus` per arm and
# you lose it — the bare scan becomes a second treatment and there is nothing left to read the
# first one against.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CORPUS=""
BIN_DIR=""
# ARM E's OWN BINARY SET (#3997). Empty = arm E is not part of this set, and the arm list below
# does not carry it. See `arm_bin_dir` for why arm E is a second --bin-dir rather than a
# per-binary override, and for the invariant this driver enforces on it up front.
BIN_DIR_E=""
OUT=""
ROUNDS=3
STEP_DURATION="45s"
QUIESCENCE_TS=""
JEMALLOC_LIB=""
ARENA_MAX=2
PORT=18815
# The pins. Arm A is the #3096/#3248 configuration verbatim; B/C0/C move ONE cpu of the flight
# pin off its sibling and onto a second physical core. Both are TWO logical CPUs, so the
# admission ceiling `clamp(2 x available_parallelism, 2, 64)` is unmoved — asserted from each
# server's own log by ws0-baseline.sh, never assumed here.
PIN_A="2,10"
PIN_B="2,3"

# The binaries this set MEASURES, and therefore the ones whose BYTES the arms must share. The
# same list `ws0_binaries.MEASURED_BINARIES` uses; it is restated here rather than imported
# because this file's use of it is a FINGERPRINT over one directory, not the session's
# provenance record, and a shell driver importing a python constant to build a shell array
# would buy nothing but a second failure mode.
MEASURED_BINARIES=(ws0-scan-bench cqlite-flight flight-loadgen)

# ARM E (#3997) — THE ONE ARM THAT MEASURES DIFFERENT BYTES, AND THE ONE THAT IS OPT-IN.
#
# Its id and its differing binary are named ONCE here because three places must agree on them:
# this driver (which arm gets which --bin-dir), `ws0_abc_aggregate.py` (whose cross-arm binary
# invariant grants this arm — and only this arm, on this binary — a NAMED exception), and
# `scripts/tests/test_ws0_abc_driver_guards.sh` (which asserts BOTH directions of that
# exception). The aggregator states the same pair in its own constants; the guard suite reads
# both and refuses a disagreement, because two sides of one exception silently naming different
# arms is how a narrow exception becomes a disabled check.
ARM_E="E"
ARM_E_BINARY="cqlite-flight"

usage() {
  cat <<EOF
ws0-3551-abc.sh — issue #3551 interleaved SMT-unpin + allocator trial

  --corpus DIR       ws0-corpus-gen corpus root. REQUIRED.
  --bin-dir DIR      ONE frozen binary set measured by EVERY arm. REQUIRED, and required to be
                     one directory: the arms must not differ in their binaries (#3248 withdrew a
                     machine-code claim for exactly that reason), so this is deliberately not
                     per-arm. ARM E is the ONE exception, and it is opt-in — see --bin-dir-e.
  --bin-dir-e DIR    OPTIONAL, and giving it is what ADDS ARM E to this set (#3997). A SECOND
                     frozen binary set whose $ARM_E_BINARY is a DIFFERENT build — the linked
                     jemalloc #[global_allocator] — and whose other measured binaries are the
                     SAME BYTES as --bin-dir's. Both halves are CHECKED from the digests before
                     anything is measured: an identical $ARM_E_BINARY would publish two labels
                     for one treatment, and a differing ws0-scan-bench or flight-loadgen would
                     move the drift control or the client apparatus, which is a second treatment.
                     AND THE ALLOCATOR IS ASKED OF BOTH BINARIES, not inferred from the digests:
                     \`$ARM_E_BINARY --version\` must report 'allocator: system' in --bin-dir and
                     'allocator: jemalloc' in --bin-dir-e (R2.1), before anything runs. A digest
                     difference proves DIFFERENT BYTES and never 'jemalloc' — any unrelated
                     rebuild satisfies it — and /proc/<pid>/maps cannot see a statically linked
                     allocator, so this surface is the only thing that can tell them apart.
                     Omit it and the set is A/B/C0/C/D exactly as before.
  --out DIR          Where the r<N>-<arm>/ session dirs go. REQUIRED. A (round, arm) that
                     already holds a results.json is SKIPPED, so an interrupted set resumes
                     instead of starting over — which matters on a shared box. The resume is
                     CHECKED, not assumed: see abc-run.json below.
  --rounds N         Rounds; each round runs every arm once, order rotated (default $ROUNDS).
                     Deliberately NOT part of the run fingerprint — extending a set from 3
                     rounds to 5 over the same --out is a legitimate resume.
  --step-duration D  Flight loadgen step hold per rep (default $STEP_DURATION).
  --arena-max N      MALLOC_ARENA_MAX for arm C0 (default $ARENA_MAX).
  --jemalloc-lib P   Passed through for arm C on a host with a non-standard path.
  --quiescence-timeseries F
                     Passed to every session. Its ABSENCE is recorded by ws0-baseline.sh as
                     'quiescence: NOT VERIFIED', so omitting it cannot look verified.
  --port N           Loopback port (default $PORT).
  -h, --help         This text.

Arms: a 2x2 of PIN x ALLOCATOR, plus the arena probe. Deltas across ONE axis at a time are
what is attributable; arm C differs from arm A in BOTH axes and on its own is not.

                     glibc        jemalloc
  1 phys core $PIN_A     A            D
  2 phys cores $PIN_B    B            C
  C0 = B + MALLOC_ARENA_MAX=$ARENA_MAX (#3217 partC F1-AC2's pre-registered arena experiment)
  E  = arm A's FLAGS EXACTLY, measured against --bin-dir-e's binary set (#3997, R3.1/R3.3).
       Present only when --bin-dir-e is given. Arms A/B/C0/C/D vary the allocator by
       LD_PRELOAD into one binary, which is what let #3551 hold every arm's bytes identical;
       arm E is the SHIPPED form — jemalloc LINKED as the binary's #[global_allocator] — so it
       is the first arm that legitimately runs different bytes from arm A. Its flags say
       --flight-allocator system because nothing is preloaded: the per-rep check then asserts
       an EMPTY LD_PRELOAD and no jemalloc *mapping*, both of which a statically linked
       allocator satisfies. So the allocator column of the aggregate's configuration table
       reads 'system' for arm E too, and the allocator difference is visible ONLY as the
       binary digest — which is why the aggregate prints those digests per arm rather than
       leaving arm E's treatment to be inferred from a flag that cannot show it.

\$OUT/abc-run.json is this set's RUN FINGERPRINT: the corpus path AND its recorded Data.db
sha256 + row count, the --bin-dir path AND a digest of every measured binary in it, the arm set
and each arm's exact flag list, --step-duration, --arena-max, --jemalloc-lib and --port. It is
WRITTEN on the first invocation and VERIFIED on every later one; a differing field is a REFUSAL
naming the field and both values, because two sessions measured under different treatments are
not a paired experiment however much the directory layout says they are. And the WRITE itself
REFUSES an --out that already holds r<N>-<arm>/results.json: with no run record there is
nothing to compare against, so those sessions cannot be shown to be this experiment and
adopting them under a fingerprint written now would publish two treatments as one set.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --corpus) CORPUS="${2:-}"; shift 2 ;;
    --bin-dir) BIN_DIR="${2:-}"; shift 2 ;;
    --bin-dir-e) BIN_DIR_E="${2:-}"; shift 2 ;;
    --out) OUT="${2:-}"; shift 2 ;;
    --rounds) ROUNDS="${2:-}"; shift 2 ;;
    --step-duration) STEP_DURATION="${2:-}"; shift 2 ;;
    --arena-max) ARENA_MAX="${2:-}"; shift 2 ;;
    --jemalloc-lib) JEMALLOC_LIB="${2:-}"; shift 2 ;;
    --quiescence-timeseries) QUIESCENCE_TS="${2:-}"; shift 2 ;;
    --port) PORT="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "FATAL: unknown argument $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$CORPUS" ]]  || { echo "FATAL: --corpus is required" >&2; exit 2; }
[[ -n "$BIN_DIR" ]] || { echo "FATAL: --bin-dir is required" >&2; exit 2; }
[[ -n "$OUT" ]]     || { echo "FATAL: --out is required" >&2; exit 2; }
[[ "$ROUNDS" =~ ^[1-9][0-9]*$ ]] || { echo "FATAL: --rounds must be a positive integer, got '$ROUNDS'" >&2; exit 2; }
[[ "$ARENA_MAX" =~ ^[1-9][0-9]*$ ]] || { echo "FATAL: --arena-max must be a positive integer, got '$ARENA_MAX'" >&2; exit 2; }
[[ -d "$CORPUS" ]]  || { echo "FATAL: --corpus '$CORPUS' is not a directory" >&2; exit 2; }
[[ -d "$BIN_DIR" ]] || { echo "FATAL: --bin-dir '$BIN_DIR' is not a directory" >&2; exit 2; }
# Checked HERE, with the other argument checks and BEFORE the first side effect below, for this
# file's standing reason: refusing an impossible configuration AFTER acting on it is the defect.
[[ -z "$BIN_DIR_E" || -d "$BIN_DIR_E" ]] \
  || { echo "FATAL: --bin-dir-e '$BIN_DIR_E' is not a directory" >&2; exit 2; }

# ARM E'S TREATMENT IS ASKED OF THE BINARY, NEVER INFERRED FROM ITS BYTES (#3997, R2.1;
# roborev round 2, job 132).
#
# The digest precondition below establishes that arm E's $ARM_E_BINARY is a DIFFERENT build from
# the control's. "Different" is not "jemalloc": a rebuild with another feature set, a stale
# binary from another branch, or any unrelated rebuild satisfies it while still linking glibc
# malloc — and NOTHING downstream catches that. Arm E runs under `--flight-allocator system` by
# construction (nothing is preloaded), and `verify_flight_server_allocator`'s
# `/proc/<pid>/maps` check CANNOT see a statically linked jemalloc — that blindness is exactly
# what lets arm E carry the `system` label. So the mapping check is structurally silent here,
# the digest check is satisfied, and the aggregate would then label a glibc-vs-glibc pair a
# linked-allocator comparison and attribute run-to-run noise to jemalloc.
#
# So the allocator is READ OFF THE BINARY'S OWN `--version` SURFACE, which is R2's whole
# purpose: the reported string is derived from the SAME `cfg` that installs the
# `#[global_allocator]`, so it cannot disagree with what was linked. This retires the rig-only
# residual "a tikv-jemallocator-linked binary leaves no libjemalloc mapping" — reasoned, never
# measured — by removing the need to infer anything.
#
# BOTH SIDES ARE ASKED, because either alone is insufficient: E reporting `jemalloc` while the
# CONTROL also links jemalloc makes the A-vs-E delta not an allocator delta at all.
#
# SCOPE, stated because it is a decision: this fires only when `--bin-dir-e` is given, i.e. when
# arm E is in the set and the linked-allocator claim is being made. A set without arm E is
# byte-identical in behaviour to the pre-#3997 driver (an opt-in that changes the default is not
# opt-in), so its control binary is NOT asked — a linked-jemalloc `--bin-dir` would make arms
# A/B/C0/C/D silently jemalloc arms, which this check does not cover and which nothing else
# does either. NAMED, not closed.
#
# NO SYMBOL READER IS INVOLVED, deliberately. `nm`/`readelf` would be a second oracle for one
# fact, would need its own three-valued unmeasurable handling, and would make a binutils
# installation a hard prerequisite of a measurement rig that does not otherwise need one.
# `--version` decides it, from the same cfg — `scripts/tests/test_flight_allocator_link.sh`
# owns the symbol-level assertion, at build time, where the toolchain is present by definition.
ALLOCATOR_SURFACE_RE='^allocator: (jemalloc|system)$'

# require_reported_allocator <label> <binary> <expected> — fail CLOSED on every unmeasurable
# state, each naming its own cause, because "the binary did not say" and "the binary said the
# other thing" are different operator actions (rebuild vs. point --bin-dir-e somewhere else).
require_reported_allocator() {
  local label="$1" bin="$2" expect="$3" rc=0 out n
  local -a matched
  if [[ ! -f "$bin" ]]; then
    echo "FATAL: $label '$bin' is not a file, so its allocator could not be READ." >&2
    echo "       Arm $ARM_E's treatment is the LINKED allocator, and R2.1's --version surface is" >&2
    echo "       the only thing that can state which one was linked." >&2
    exit 2
  fi
  if [[ ! -x "$bin" ]]; then
    echo "FATAL: $label '$bin' is not EXECUTABLE, so its reported allocator is UNMEASURED." >&2
    echo "       Refused rather than skipped: a digest difference proves DIFFERENT BYTES and" >&2
    echo "       never 'jemalloc', so nothing else in this rig can establish arm $ARM_E's" >&2
    echo "       treatment. Remedy: chmod +x, or point --bin-dir-e at a real build directory." >&2
    exit 2
  fi
  if command -v timeout >/dev/null 2>&1; then
    out="$(timeout -k 5 60 "$bin" --version 2>&1)" || rc=$?
  else
    # A missing bound must not inherit the permissive branch, and an unbounded `--version` on a
    # wrong or wedged binary would hang the whole set with no verdict — which blocks the
    # measurement anyway, so refusing now with a named remedy strictly dominates hanging later.
    echo "FATAL: no timeout(1) on this host, so \`$label --version\` cannot be BOUNDED." >&2
    echo "       Refused rather than run unbounded: a wedged binary would hang the set with no" >&2
    echo "       verdict. Remedy: install coreutils' timeout." >&2
    exit 2
  fi
  if [[ $rc -ne 0 ]]; then
    echo "FATAL: \`$bin --version\` exited $rc, so its allocator is UNMEASURED." >&2
    echo "       R2.1 requires --version to short-circuit before argument validation and exit 0." >&2
    echo "       Its output follows:" >&2
    printf '%s\n' "$out" | sed 's/^/         | /' >&2
    exit 2
  fi
  mapfile -t matched < <(printf '%s\n' "$out" | grep -E "$ALLOCATOR_SURFACE_RE") || true
  n=${#matched[@]}
  if [[ $n -ne 1 ]]; then
    echo "FATAL: \`$bin --version\` printed $n line(s) matching '$ALLOCATOR_SURFACE_RE';" >&2
    echo "       R2.1's contract is EXACTLY ONE. Neither 0 nor 2+ is read as either allocator:" >&2
    echo "       an unrecognised or absent line is UNMEASURED, and defaulting either way is how" >&2
    echo "       a glibc build would be measured as the jemalloc arm. Its output follows:" >&2
    printf '%s\n' "$out" | sed 's/^/         | /' >&2
    exit 2
  fi
  if [[ "${matched[0]}" != "allocator: $expect" ]]; then
    echo "FATAL: $label '$bin' reports '${matched[0]}', expected 'allocator: $expect'." >&2
    echo "       Arm $ARM_E is the LINKED-jemalloc treatment and every other arm selects its" >&2
    echo "       allocator by LD_PRELOAD into the control binary, so the pair must read" >&2
    echo "       --bin-dir=system and --bin-dir-e=jemalloc. A digest DIFFERENCE proves only" >&2
    echo "       different bytes — any unrelated rebuild satisfies it — and /proc/<pid>/maps" >&2
    echo "       cannot see a statically linked allocator, so this surface is the only thing" >&2
    echo "       that can tell the two apart. Remedy: build --bin-dir-e's $ARM_E_BINARY with" >&2
    echo "       --features jemalloc (and --bin-dir's without it), or drop --bin-dir-e." >&2
    exit 2
  fi
  echo "arm $ARM_E precondition: $label '$bin' reports '${matched[0]}' (R2.1)"
}

if [[ -n "$BIN_DIR_E" ]]; then
  require_reported_allocator "--bin-dir $ARM_E_BINARY" "$BIN_DIR/$ARM_E_BINARY" system
  require_reported_allocator "--bin-dir-e $ARM_E_BINARY" "$BIN_DIR_E/$ARM_E_BINARY" jemalloc
fi

mkdir -p "$OUT"

ARMS=(A B C0 C D)

# Arm E joins the set only when its binary set is supplied — see --bin-dir-e in the usage text
# and `arm_bin_dir` below. Omitted, this driver's arm set and every fingerprint field it writes
# are byte-identical to the pre-#3997 ones.
if [[ -n "$BIN_DIR_E" ]]; then
  ARMS+=("$ARM_E")
fi

arm_flags() {
  # The one place an arm's identity is defined. Printed into the run record below AND read back
  # out of each session's own recorded pinning by the aggregator, so a divergence between what
  # this table says and what was measured is detectable rather than assumed away.
  case "$1" in
    A)  printf '%s\n' --flight-server-cpus "$PIN_A" --flight-pin-mode siblings --flight-allocator system ;;
    B)  printf '%s\n' --flight-server-cpus "$PIN_B" --flight-pin-mode distinct-cores --flight-allocator system ;;
    C0) printf '%s\n' --flight-server-cpus "$PIN_B" --flight-pin-mode distinct-cores --flight-allocator system --flight-malloc-arena-max "$ARENA_MAX" ;;
    C)  printf '%s\n' --flight-server-cpus "$PIN_B" --flight-pin-mode distinct-cores --flight-allocator jemalloc ;;
    # ARM D EXISTS TO BREAK A CONFOUND, and it was added because the first 4-arm set measured
    # one: arm C differs from arm A in TWO properties at once (the pin AND the allocator), so a
    # C-vs-A delta cannot distinguish "jemalloc is worth X on this workload" from "jemalloc is
    # what unlocks the second physical core that glibc's malloc was preventing the server from
    # using". Those are different claims with different production consequences, and A->B
    # (measured SLOWER on two cores under glibc) is what makes the second one plausible.
    #
    # With D the arm set is a clean 2x2 of pin x allocator, plus C0 as the arena probe:
    #
    #                 glibc      jemalloc
    #   1 core 2,10      A          D
    #   2 cores 2,3      B          C
    #
    # So (D-A) prices the allocator at a FIXED pin, (B-A) prices the pin at a FIXED allocator,
    # and (C-D)-(B-A) is the interaction — which is the quantity the SMT hypothesis is actually
    # about. Measuring only A/B/C leaves the headline attributable to either variable.
    D)  printf '%s\n' --flight-server-cpus "$PIN_A" --flight-pin-mode siblings --flight-allocator jemalloc ;;
    # ARM E IS ARM A'S FLAG LIST, CHARACTER FOR CHARACTER, and that is the whole design (#3997).
    # The treatment is carried entirely by `arm_bin_dir` below: arm E measures a binary that
    # LINKS jemalloc as its `#[global_allocator]`, so there is nothing to preload and no knob to
    # turn — `--flight-allocator jemalloc` here would set LD_PRELOAD and make arm E a
    # preload-AND-link arm, i.e. two changes at once, which is the confound arm D was added to
    # break. `system` is therefore the correct and honest value: it is what the SERVER'S
    # ENVIRONMENT will hold, it is what `lib-flight-arm.sh` verifies per rep from
    # /proc/<pid>/{environ,maps}, and a statically linked jemalloc leaves no `libjemalloc`
    # MAPPING for that check to trip on.
    #
    # Consequence, stated because it is the reason R3.3 exists: arms A and E record an IDENTICAL
    # treatment, so NOTHING in the recorded flags distinguishes them. The distinguishing fact is
    # the `cqlite-flight` digest in each session's own `binary_provenance`, which is why the
    # aggregator prints it per arm.
    E)  printf '%s\n' --flight-server-cpus "$PIN_A" --flight-pin-mode siblings --flight-allocator system ;;
    *)  echo "FATAL: unknown arm '$1'" >&2; return 2 ;;
  esac
}

# arm_bin_dir <arm> — WHICH FROZEN BINARY SET THIS ARM MEASURES (#3997).
#
# The one place that answer is given, mirroring `arm_flags` deliberately rather than being
# folded into it: `arm_flags` output is FINGERPRINTED as an arm's flag list and read back out of
# each session's recorded pinning, while the binary set is fingerprinted as digests and read
# back out of each session's `binary_provenance`. Two different records, two different refusals.
#
# WHY A SECOND --bin-dir AND NOT A PER-BINARY OVERRIDE. `ws0-baseline.sh` derives the session's
# whole `binary_provenance` block from `--bin-dir` (`ws0_binaries.py`), and that block is the
# ONLY thing the aggregator can read the measured bytes out of. A `--flight-binary <path>`
# override — the shape #3997's task list sketched — would launch one binary while the session
# recorded the digest of another, so R3.3's exception would be granted against a digest that
# named the wrong program: exactly the unobserved-treatment defect the rest of this file is
# built to refuse. A second `--bin-dir` needs no new plumbing anywhere and keeps every digest
# truthful. Build it as: the jemalloc `cqlite-flight` plus HARDLINKS (or copies) of --bin-dir's
# own `ws0-scan-bench` and `flight-loadgen`, whose sameness is checked below.
#
# THE `*)` BRANCH IS TOTAL, NOT A PERMISSIVE DEFAULT. The question this function answers has
# exactly two answers by construction — arm E measures --bin-dir-e's set, every other arm
# measures --bin-dir's — so there is no unknown-arm case for it to swallow: `$BIN_DIR` is the
# CORRECT answer for any arm id that is not E, including one that should never have been asked
# for. Enumerating the arms here instead would duplicate `arm_flags`'s table for no gain, and
# `arm_flags` already refuses an unknown arm by name.
arm_bin_dir() {
  case "$1" in
    E) printf '%s\n' "$BIN_DIR_E" ;;
    *) printf '%s\n' "$BIN_DIR" ;;
  esac
}

# sha256_of <file> — THREE-VALUED, and the third value is a REFUSAL.
#
# present  -> the digest on stdout, rc 0
# missing/unreadable, or NO DIGEST TOOL ON THIS BOX -> a named FATAL, rc 2
#
# The last case is why this is not a one-liner: an absent `sha256sum` returning an EMPTY digest
# would compare equal to another run's empty digest and report two different binary sets as
# identical — a comparison that could not be made, reported as a comparison that passed.
sha256_of() {
  local f="$1" out=""
  if [[ ! -f "$f" ]]; then
    echo "FATAL: '$f' does not exist, so the bytes this set measures cannot be identified." >&2
    echo "       Build the binaries into --bin-dir before starting the set." >&2
    return 2
  fi
  if command -v sha256sum >/dev/null 2>&1; then
    out="$(sha256sum "$f")" || out=""
  elif command -v shasum >/dev/null 2>&1; then
    out="$(shasum -a 256 "$f")" || out=""
  else
    echo "FATAL: neither sha256sum nor shasum is installed, so '$f' cannot be DIGESTED and two" >&2
    echo "       binary sets cannot be told apart. This is refused rather than skipped: an" >&2
    echo "       unmeasurable digest compared equal would report a changed binary as unchanged." >&2
    return 2
  fi
  out="${out%% *}"
  if [[ ! "$out" =~ ^[0-9a-f]{64}$ ]]; then
    echo "FATAL: could not read a sha256 for '$f' (digest tool produced '$out')." >&2
    return 2
  fi
  printf '%s\n' "$out"
}

echo "== #3551 interleaved A/B/C =="
echo "corpus:   $CORPUS"
echo "bins:     $BIN_DIR"
echo "out:      $OUT"
echo "rounds:   $ROUNDS   arms: ${ARMS[*]}"
echo "control:  bare scan pinned to --server-cpus (IDENTICAL in every arm) — method §3b step 3"

# ===========================================================================
# THE RUN FINGERPRINT — what makes a RESUME a resume rather than two experiments
# ===========================================================================
# The SKIP in the loop below is deliberate and STAYS: this box is shared with nine other lanes
# and a set that has to start over loses its window. But a skip is only sound if the sessions
# being kept were produced by THIS configuration, and nothing checked that. Point --out at an
# earlier set's directory, or change the corpus, a pin, the allocator or the binaries between
# invocations, and the aggregator receives a supposedly PAIRED experiment whose rounds were
# measured under different treatments — every downstream check reads the artifacts as one set,
# because that is what the directory says they are.
#
# So the first invocation WRITES `abc-run.json` and every later one VERIFIES it field by field,
# refusing with the differing field NAMED and both values printed. Covered: everything that
# would make two sessions incomparable.
#
#   * the corpus PATH *and* its recorded `Data.db` sha256 + row count — a path can be
#     repopulated with a different corpus, so the path alone would not notice;
#   * the --bin-dir PATH *and* a digest of every measured binary in it: the arms must measure
#     IDENTICAL BYTES, which is the whole reason --bin-dir is not per-arm (#3248 withdrew a
#     machine-code claim for exactly that reason);
#   * the arm SET and, per arm, the EXACT flag list `arm_flags` emits, so a changed pin, pin
#     mode, allocator or arena cap is caught at the flag level rather than inferred from it;
#   * --step-duration, --arena-max, --jemalloc-lib and --port.
#
# `--rounds` IS DELIBERATELY EXCLUDED. Extending a set from 3 rounds to 5 over the same --out is
# a legitimate resume — the same experiment with more pairs — so refusing it would red CORRECT
# INPUT, and a guard that reds on correct input is the guard an operator learns to work around.
# `--arena-max` is included even though it reaches only arm C0, because it is part of that arm's
# flag list and a changed cap changes that treatment.
#
# Every probe here is THREE-VALUED — present / verified-absent / could-not-measure — and
# could-not-measure is a REFUSAL, never "compatible": a comparison that could not be made has
# not been made. An absent or unreadable `corpus-identity.json`, an unreadable or ungrammatical
# `abc-run.json`, a missing binary and a box with no digest tool are each their own named
# refusal carrying its own remedy.
# Read through a COMMAND SUBSTITUTION and not `mapfile < <(...)`: a process substitution's exit
# status is not the reading builtin's, so `mapfile … || exit` would have DISCARDED every refusal
# below and continued with an empty identity — a could-not-measure silently taking the
# permissive branch, which is the exact shape this block exists to remove.
identity_rc=0
identity_out="$(python3 - "$CORPUS/corpus-identity.json" <<'PY'
import json
import pathlib
import sys

p = pathlib.Path(sys.argv[1])
if not p.exists():
    sys.stderr.write(
        f"FATAL: no corpus identity at {p} — this set's corpus cannot be IDENTIFIED, so a\n"
        "       resume could not tell one corpus from another sitting at the same path.\n"
        "       Regenerate the corpus with tools/ws0-corpus-gen, which writes this file\n"
        "       beside the data.\n"
    )
    raise SystemExit(2)
try:
    raw = p.read_text()
except OSError as exc:
    sys.stderr.write(
        f"FATAL: {p} EXISTS but could not be READ ({exc}). This is refused rather than\n"
        "       skipped: an unreadable identity is UNMEASURED, not compatible.\n"
    )
    raise SystemExit(2)
try:
    identity = json.loads(raw)
except ValueError as exc:
    sys.stderr.write(f"FATAL: {p} is not readable JSON ({exc}) — the corpus is UNIDENTIFIED.\n")
    raise SystemExit(2)
if not isinstance(identity, dict):
    sys.stderr.write(f"FATAL: {p} must hold a JSON object, got {type(identity).__name__}.\n")
    raise SystemExit(2)
sha = identity.get("data_db_sha256")
rows = identity.get("rows")
if not isinstance(sha, str) or not sha:
    sys.stderr.write(
        f"FATAL: {p} carries no usable 'data_db_sha256' (got {sha!r}) — the corpus BYTES\n"
        "       cannot be pinned, so a repopulated corpus path would resume silently.\n"
    )
    raise SystemExit(2)
if isinstance(rows, bool) or not isinstance(rows, int) or rows <= 0:
    sys.stderr.write(
        f"FATAL: {p} carries no usable 'rows' (got {rows!r}) — a corpus with no row count\n"
        "       is not a measurable corpus.\n"
    )
    raise SystemExit(2)
print(sha)
print(rows)
PY
)" || identity_rc=$?
[[ $identity_rc -eq 0 ]] || exit "$identity_rc"
mapfile -t corpus_identity <<<"$identity_out"
[[ ${#corpus_identity[@]} -eq 2 && -n "${corpus_identity[0]}" && -n "${corpus_identity[1]}" ]] \
  || { echo "FATAL: could not read the corpus identity at $CORPUS/corpus-identity.json" >&2; exit 2; }

fp=("corpus_path=$CORPUS"
    "corpus_data_db_sha256=${corpus_identity[0]}"
    "corpus_rows=${corpus_identity[1]}"
    "bin_dir=$BIN_DIR"
    "step_duration=$STEP_DURATION"
    "arena_max=$ARENA_MAX"
    "jemalloc_lib=$JEMALLOC_LIB"
    "port=$PORT"
    "arms=${ARMS[*]}")
for b in "${MEASURED_BINARIES[@]}"; do
  digest="$(sha256_of "$BIN_DIR/$b")" || exit 2
  fp+=("binary_sha256.$b=$digest")
done
# ARM E'S BINARY SET, AND THE TWO-SIDED PRECONDITION THAT EARNS ITS EXCEPTION (#3997, R3.3).
#
# `ws0_abc_aggregate.py` grants arm E a NAMED exception to the cross-arm "every arm ran the same
# bytes" invariant. An exception is only narrow if BOTH of its edges are checked, so both are
# checked HERE, from the digests, before a single rep runs:
#
#   * $ARM_E_BINARY MUST DIFFER. Identical bytes would make arm E a second label for arm A's
#     treatment, and the aggregate would publish a "linked jemalloc" row measured from the
#     glibc binary — a permitted exception used to hide the absence of the thing under test.
#   * EVERY OTHER MEASURED BINARY MUST BE THE SAME BYTES. `ws0-scan-bench` IS the drift control
#     and `flight-loadgen` is the client apparatus; moving either makes arm E differ from arm A
#     in two properties, which is the confound arm D exists to break. The aggregate refuses
#     these on its own too — that is the "still FAILs on any other cross-arm binary difference"
#     half of R3.3 — but a refusal AFTER a multi-hour set has run is a refusal that costs the
#     rig its window, so the same facts are established up front from the same digests.
#
# These fields are recorded ONLY when arm E is in the set, so a set started before #3997 (or
# without --bin-dir-e) resumes against a byte-identical fingerprint. Switching arm E on or off
# over one --out is caught by the `arms` field, which changes with it.
if [[ -n "$BIN_DIR_E" ]]; then
  fp+=("bin_dir_e=$BIN_DIR_E")
  for b in "${MEASURED_BINARIES[@]}"; do
    digest="$(sha256_of "$BIN_DIR/$b")" || exit 2
    digest_e="$(sha256_of "$BIN_DIR_E/$b")" || exit 2
    fp+=("binary_sha256_e.$b=$digest_e")
    if [[ "$b" == "$ARM_E_BINARY" ]]; then
      if [[ "$digest" == "$digest_e" ]]; then
        echo "FATAL: --bin-dir-e '$BIN_DIR_E/$b' is the SAME BYTES as --bin-dir '$BIN_DIR/$b'" >&2
        echo "       (sha256 $digest). Arm $ARM_E exists to measure a DIFFERENT $ARM_E_BINARY —" >&2
        echo "       the build that LINKS jemalloc as its #[global_allocator]. Identical bytes" >&2
        echo "       make arm $ARM_E a second LABEL for arm A's treatment, and the aggregate" >&2
        echo "       would then publish a linked-allocator row measured from the control binary." >&2
        echo "       Remedy: build --bin-dir-e's $ARM_E_BINARY with the jemalloc feature, or drop" >&2
        echo "       --bin-dir-e and run the A/B/C0/C/D set." >&2
        exit 2
      fi
    elif [[ "$digest" != "$digest_e" ]]; then
      echo "FATAL: --bin-dir-e '$BIN_DIR_E/$b' differs from --bin-dir '$BIN_DIR/$b'" >&2
      echo "         --bin-dir   $digest" >&2
      echo "         --bin-dir-e $digest_e" >&2
      echo "       Only $ARM_E_BINARY may differ between arm $ARM_E and the other arms." >&2
      echo "       ws0-scan-bench IS the drift control and flight-loadgen is the client" >&2
      echo "       apparatus, so a differing one makes arm $ARM_E differ from arm A in TWO" >&2
      echo "       properties and its delta attributable to neither." >&2
      echo "       Remedy: hardlink or copy --bin-dir's $b into --bin-dir-e rather than" >&2
      echo "       rebuilding it, so the bytes are the same bytes by construction." >&2
      exit 2
    fi
  done
fi
for arm in "${ARMS[@]}"; do
  mapfile -t af < <(arm_flags "$arm")
  fp+=("arm_flags.$arm=${af[*]}")
done

rc=0
python3 - "$OUT/abc-run.json" "${fp[@]}" <<'PY' || rc=$?
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
fields = {}
for arg in sys.argv[2:]:
    key, _, value = arg.partition("=")
    if not key:
        sys.stderr.write(f"FATAL: internal: fingerprint argument {arg!r} has no field name.\n")
        raise SystemExit(2)
    fields[key] = value

REMEDY = (
    "       This --out belongs to a DIFFERENT experiment. Either point --out at a FRESH\n"
    "       directory, or re-run with the configuration the recorded values above name.\n"
    "       --rounds is deliberately NOT fingerprinted, so extending a set is not this.\n"
)

if path.exists():
    try:
        raw = path.read_text()
    except OSError as exc:
        sys.stderr.write(
            f"FATAL: {path} EXISTS but could not be READ ({exc}), so this resume could not be\n"
            "       CHECKED. Refused rather than accepted: an unverifiable resume is not a\n"
            "       compatible one. Fix the permissions, or start the set in a fresh --out.\n"
        )
        raise SystemExit(2)
    try:
        record = json.loads(raw)
    except ValueError as exc:
        sys.stderr.write(
            f"FATAL: {path} is not readable JSON ({exc}), so this resume could not be CHECKED.\n"
            "       Refused rather than accepted. Start the set in a fresh --out.\n"
        )
        raise SystemExit(2)
    recorded = record.get("fields") if isinstance(record, dict) else None
    if not isinstance(recorded, dict):
        sys.stderr.write(
            f"FATAL: {path} carries no `fields` object, so this resume could not be CHECKED\n"
            "       against anything. Refused rather than accepted; start in a fresh --out.\n"
        )
        raise SystemExit(2)
    problems = []
    for key in sorted(set(recorded) | set(fields)):
        if key not in recorded:
            problems.append(
                f"{key}: NOT RECORDED by the existing run record; this invocation has"
                f" {fields[key]!r}"
            )
        elif key not in fields:
            problems.append(
                f"{key}: recorded as {recorded[key]!r}; this invocation does not supply it"
            )
        elif str(recorded[key]) != fields[key]:
            problems.append(
                f"{key}: recorded {str(recorded[key])!r}, this invocation {fields[key]!r}"
            )
    if problems:
        sys.stderr.write(
            f"FATAL: {path} records an INCOMPATIBLE run — resuming would combine sessions\n"
            "       measured under different treatments into one supposedly paired set.\n"
        )
        for problem in problems:
            sys.stderr.write(f"       DIFFERS {problem}\n")
        sys.stderr.write(REMEDY)
        raise SystemExit(2)
    print(
        f"resume:   VERIFIED against {path} — all {len(fields)} fingerprint field(s) identical"
    )
else:
    # NO RUN RECORD — SO THIS --out MUST NOT ALREADY HOLD SESSIONS (roborev #3551 round 3 F1).
    #
    # The verified branch above closed the case where a fingerprint EXISTS; this is the other
    # half of the same defect, and it is the half that fails silently. With no `abc-run.json`
    # there is NOTHING to compare this invocation's configuration against, so any `r<N>-<arm>/
    # results.json` already sitting here is a session of an experiment whose treatments cannot
    # be shown to be these — and the loop below SKIPS such a session after validating only its
    # round, its arm and its exit status, none of which is a treatment. Writing a fresh
    # fingerprint over them would stamp this invocation's configuration onto sessions measured
    # under another one, and the aggregator would then pair across treatments under a table
    # describing one. Reachable by accident in one gesture: `--out` pointed at an earlier set
    # whose run record was deleted, or a set started before this fingerprint existed at all.
    #
    # So it is REFUSED, not adopted: "cannot be shown to be this experiment" is a
    # could-not-measure, and this rig's standing rule is that a comparison that could not be
    # made has not been made. A FRESH (empty or absent) --out is unaffected, which is the
    # legitimate path this must not red.
    try:
        adopted = sorted(
            p.parent.name for p in path.parent.glob("r*-*/results.json") if p.is_file()
        )
    except OSError as exc:
        # THREE-VALUED: the scan itself failing is not "nothing is here". An unreadable --out
        # is UNMEASURED and takes the refusal, never the permissive branch.
        sys.stderr.write(
            f"FATAL: {path.parent} could not be SCANNED for existing sessions ({exc}), so it"
            " cannot be\n"
            "       shown to be empty. Refused rather than assumed fresh: an unmeasurable\n"
            "       directory is not an empty one. Fix the permissions, or use a fresh --out.\n"
        )
        raise SystemExit(2)
    if adopted:
        sys.stderr.write(
            f"FATAL: {path.parent} ALREADY HOLDS session artifacts but carries NO"
            f" {path.name}, so\n"
            "       nothing records which configuration measured them. This invocation would"
            " write a\n"
            "       fresh run record and then ADOPT them: the loop skips a measured (round,"
            " arm) after\n"
            "       validating its round, arm and exit status only — none of which is a"
            " TREATMENT — so\n"
            "       sessions from an earlier or differently configured set would be published"
            " as one\n"
            "       paired experiment. Refused: they cannot be SHOWN to be this experiment,"
            " and a\n"
            "       comparison that could not be made has not been made.\n"
            "       ADOPTABLE session(s) (each holds a results.json):\n"
        )
        for name in adopted[:10]:
            sys.stderr.write(f"         {name}\n")
        if len(adopted) > 10:
            sys.stderr.write(f"         ... and {len(adopted) - 10} more\n")
        sys.stderr.write(
            "       Remedy: point --out at a FRESH directory, or restore the"
            f" {path.name} of the\n"
            "       set these sessions belong to (which is then VERIFIED field by field"
            " instead).\n"
        )
        raise SystemExit(2)
    body = {
        "issue": "#3551",
        "fields": fields,
        "note": (
            "the RUN FINGERPRINT of this A/B/C set, written by the first invocation and"
            " VERIFIED field-by-field by every later one. A differing field is a REFUSAL:"
            " sessions measured under different treatments are not a paired experiment."
        ),
        "rounds_excluded": (
            "--rounds is deliberately NOT a field here. Extending a set from 3 rounds to 5"
            " over the same --out is a legitimate resume — the same experiment with more"
            " pairs — and refusing it would red correct input."
        ),
    }
    try:
        path.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n")
    except OSError as exc:
        sys.stderr.write(
            f"FATAL: could not WRITE the run record {path} ({exc}). Refused rather than run"
            " unfingerprinted: an unrecorded set cannot be resumed safely.\n"
        )
        raise SystemExit(2)
    print(f"resume:   run record WRITTEN to {path} ({len(fields)} fingerprint field(s))")
PY
[[ $rc -eq 0 ]] || exit "$rc"
echo

# verify_measured_session <dir> <round> <arm> — a session may only be SKIPPED once its own
# window record says it is the session this (round, arm) slot expects.
#
# `results.json` alone establishes NOTHING about provenance: it is the reporter's output and
# carries no round, no position and no arm LABEL of this set's vocabulary. A directory holding
# one but no `abc-window.json` was produced by something other than this driver (or by a driver
# run whose window write failed), and a window describing a DIFFERENT arm means the directory
# was moved or renamed — in both cases the pairing the aggregator will perform is a fiction.
# A recorded non-zero `exit` is the third case: the window is written for FAILED sessions on
# purpose (so the failure can be correlated against the box-load timeseries), so a failed
# session's leftover `results.json` must never be silently adopted as a measurement.
#
# Each refusal NAMES THE DIRECTORY, because the operator's next action is on that directory and
# a set is 12 to 20 of them.
verify_measured_session() {
  local dir="$1" want_round="$2" want_arm="$3" vrc=0
  python3 - "$dir" "$want_round" "$want_arm" <<'PY' || vrc=$?
import json
import pathlib
import sys

d = pathlib.Path(sys.argv[1])
want_round, want_arm = sys.argv[2], sys.argv[3]
w = d / "abc-window.json"
if not w.exists():
    sys.stderr.write(
        f"FATAL: {d} holds a results.json but NO abc-window.json, so NOTHING establishes which\n"
        f"       arm or which round produced it — it cannot be adopted as this set's ({want_round},\n"
        f"       {want_arm}) session. Refused rather than skipped. Remedy: remove {d} and re-run,\n"
        "       which re-measures that (round, arm).\n"
    )
    raise SystemExit(2)
try:
    record = json.loads(w.read_text())
except (OSError, ValueError) as exc:
    sys.stderr.write(
        f"FATAL: {d} holds a results.json but its abc-window.json could not be READ ({exc}), so\n"
        "       this session's provenance is UNMEASURED — which is refused, never treated as\n"
        f"       compatible. Remedy: remove {d} and re-run.\n"
    )
    raise SystemExit(2)
if not isinstance(record, dict):
    sys.stderr.write(
        f"FATAL: {d}: abc-window.json must hold a JSON object, got"
        f" {type(record).__name__} — this session's provenance is UNMEASURED.\n"
    )
    raise SystemExit(2)
for field in ("arm", "round", "exit"):
    if field not in record:
        sys.stderr.write(
            f"FATAL: {d}: abc-window.json carries no {field!r}, so this session cannot be\n"
            f"       attributed. Refused rather than skipped. Remedy: remove {d} and re-run.\n"
        )
        raise SystemExit(2)
if str(record["arm"]) != want_arm:
    sys.stderr.write(
        f"FATAL: {d}: abc-window.json records arm {str(record['arm'])!r} but the directory name\n"
        f"       says arm {want_arm!r}. A session measured under one arm cannot stand in for\n"
        f"       another — that is the treatment itself. Remedy: remove {d} and re-run.\n"
    )
    raise SystemExit(2)
if str(record["round"]) != want_round:
    sys.stderr.write(
        f"FATAL: {d}: abc-window.json records round {str(record['round'])!r} but the directory\n"
        f"       name says round {want_round!r}. The pairing is BY ROUND, so a mislabelled round\n"
        f"       pairs the wrong sessions. Remedy: remove {d} and re-run.\n"
    )
    raise SystemExit(2)
if record["exit"] != 0:
    sys.stderr.write(
        f"FATAL: {d}: abc-window.json records exit {record['exit']!r} — that session FAILED, so\n"
        "       its leftover results.json is not a measurement this set may adopt. The window is\n"
        "       written for failed sessions on purpose (so the failure can be correlated against\n"
        f"       the box-load timeseries). Remedy: remove {d} and re-run.\n"
    )
    raise SystemExit(2)
PY
  [[ $vrc -eq 0 ]] || exit "$vrc"
}

n=${#ARMS[@]}
for ((r = 1; r <= ROUNDS; r++)); do
  # STEP 2: rotate. Round r starts at arm (r-1) mod n, so no arm holds a fixed position and no
  # arm is ever measured twice in a row at the same point in the box's own drift.
  order=()
  for ((i = 0; i < n; i++)); do
    order+=("${ARMS[$(((r - 1 + i) % n))]}")
  done
  echo "-- round $r/$ROUNDS  order: ${order[*]}"
  pos=0
  for arm in "${order[@]}"; do
    pos=$((pos + 1))
    dir="$OUT/r$r-$arm"
    if [[ -f "$dir/results.json" ]]; then
      # The run fingerprint above establishes that THIS INVOCATION's configuration matches the
      # one the set was started with; this establishes that THIS DIRECTORY holds the session
      # the slot expects. Two different questions, and the first cannot answer the second.
      verify_measured_session "$dir" "$r" "$arm"
      echo "   [$pos/$n] $arm  SKIP (measured, window VERIFIED: $dir/results.json)"
      continue
    fi
    mapfile -t extra < <(arm_flags "$arm")
    started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "   [$pos/$n] $arm  start $started -> $dir"
    # The argv is BUILT AS AN ARRAY rather than assembled with `${VAR:+...}` expansions at the
    # call site. Two reasons, and the first is not stylistic: `lib-perf-lint.sh`'s
    # `is_var_command` correctly flags a command line whose leading word is a variable
    # expansion, because it cannot know the variable does not hold `perf` — so the conditional
    # form tripped the rig's own perf-invocation lint and FATALed the shipped driver's
    # self-check (MEASURED: `ws0-3551-abc.sh:148: perf/stat invocation outside the single
    # perf_stat_c wrapper, unmarked`, which then cascaded into 5 hermeticity failures). Marking
    # the line `perf-lint-allow` would have silenced a lint that was reasoning correctly; the
    # array makes the leading word the literal `bash` instead. Second, an empty optional value
    # cannot become an empty positional argument this way.
    # THE BINARY SET IS PER-ARM, and for every arm but E it is the one `--bin-dir` names — see
    # `arm_bin_dir`. Resolved through the function rather than branched here so there is exactly
    # one place that answers "which bytes did this arm measure", the same way `arm_flags`
    # answers "under which flags".
    arm_bins="$(arm_bin_dir "$arm")"
    local_args=(--corpus "$CORPUS" --bin-dir "$arm_bins" --out "$dir"
                --reps 1 --temp warm --arm bypass
                --step-duration "$STEP_DURATION" --port "$PORT")
    if [[ -n "$QUIESCENCE_TS" ]]; then
      local_args+=(--quiescence-timeseries "$QUIESCENCE_TS")
    fi
    if [[ -n "$JEMALLOC_LIB" ]]; then
      local_args+=(--jemalloc-lib "$JEMALLOC_LIB")
    fi
    local_args+=("${extra[@]}")
    set +e
    bash "$HERE/ws0-baseline.sh" "${local_args[@]}" > "$OUT/r$r-$arm.log" 2>&1
    rc=$?
    set -e
    ended="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    # The window is recorded whether the session passed or failed. A FAILED session's window is
    # what lets its failure be correlated against the box-load timeseries afterwards, which is
    # the whole reason the timeseries is kept outside the worktree.
    mkdir -p "$dir"
    # Assembled into a variable rather than a multi-line `printf`, for the same lint reason as
    # the argv array above: a CONTINUATION line whose first word is `"$r"` is, to a line-oriented
    # lint, a command held in a variable — and `is_var_command` cannot see the backslash on the
    # line before. MEASURED: `ws0-3551-abc.sh:174: perf/stat invocation outside the single
    # perf_stat_c wrapper, unmarked`. Every line below starts with either an assignment prefix
    # or a literal command word, so the lint reads what is actually happening.
    window_json="{\"round\":$r,\"position_in_round\":$pos,\"arms_in_round\":$n"
    window_json="$window_json,\"arm\":\"$arm\",\"started\":\"$started\",\"ended\":\"$ended\""
    window_json="$window_json,\"exit\":$rc,\"order\":\"${order[*]}\"}"
    printf '%s\n' "$window_json" > "$dir/abc-window.json"
    if [[ $rc -ne 0 ]]; then
      echo "FATAL: round $r arm $arm exited $rc — see $OUT/r$r-$arm.log" >&2
      echo "       Earlier rounds are intact; re-running with the same --out RESUMES." >&2
      exit "$rc"
    fi
    echo "        done $ended"
  done
done

echo
# The printed command's --arms is DERIVED from the arm set actually measured, never a literal.
# It was a literal `A,B,C0,C` and arm D's arrival silently made the instruction WRONG: following
# it excluded every D session, i.e. dropped the one arm the confound-breaking phase existed for
# (roborev, #3551 round 2). A printed command gets run verbatim.
_arms_csv=$(IFS=,; echo "${ARMS[*]}")
echo "all rounds complete. aggregate with:"
echo "  python3 $HERE/ws0_abc_aggregate.py --root $OUT --arms $_arms_csv --baseline ${ARMS[0]}"
# ...and, when arm E is in the set, the TWO-ARM aggregate #3997's kill criterion is read from.
# Printed as a SECOND command rather than replacing the one above: the whole-set table is still
# what makes each arm's delta readable against the others, and R3.1 asks for the A/E pair
# specifically (median Δrows/s, Δcycles/row, IPC, VmHWM and VmRSS per arm). Derived from the
# arm ids, never a literal — the line above was a literal once and arm D's arrival silently
# made it wrong (roborev, #3551 round 2).
if [[ -n "$BIN_DIR_E" ]]; then
  echo "and, for #3997's pre-registered A/$ARM_E kill criterion, the two-arm table:"
  echo "  python3 $HERE/ws0_abc_aggregate.py --root $OUT --arms ${ARMS[0]},$ARM_E --baseline ${ARMS[0]}"
fi
