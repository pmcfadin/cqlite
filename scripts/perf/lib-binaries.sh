#!/usr/bin/env bash
# lib-binaries.sh — THE PROGRAMS UNDER MEASUREMENT: building them, and recording WHICH ONES
# (issue #3272 review round 10, M2).
#
# Sourced, not executed, and it sets NO shell options: `set -euo pipefail` in a library mutates the
# SOURCING shell's options, which is the caller's decision (the rule `lib-cpu.sh`, `lib-args.sh`,
# `lib-host-state.sh`, `lib-perf-lint.sh`, `lib-server.sh`, `lib-outdir.sh` and `lib-measure.sh`
# follow — `lib-cpu.sh` had to be corrected for exactly that). The driver sets all three itself.
#
# # Why this is a library, and why THIS seam
#
# Split out under the campsite rule: M2's provenance record took `ws0-baseline.sh` to 986 lines,
# 186 over the ~800 source target and moving the wrong way. Note the gate's `file-size` ratchet is
# `.rs`-ONLY, so a shell file crosses the threshold SILENTLY; this is checked with `wc -l`.
#
# The seam is a RESPONSIBILITY, not a line count. Every rig library owns one question about whether
# a measurement means what it says; this one owns the question the report's ratio is ABOUT:
#
#     lib-cpu.sh          are the pinned CPUs one physical core?
#     lib-host-state.sh   is the host's state put back?
#     lib-args.sh         are the arguments values this rig can measure?
#     lib-perf-lint.sh    is the counting domain CPU-wide?
#     lib-outdir.sh       do the artifacts being read all come from ONE session?
#     lib-measure.sh      how is ONE rep of an arm executed, prewarmed and counted?
#     lib-binaries.sh     WHICH PROGRAMS are being measured, and are they this revision's?
#
# What deliberately STAYS in the driver: the ORDER of operations (arguments before creation,
# verification before measurement, binaries before the pin, the pin before the first rep), the
# round loop, and `perf_stat_c`.
#
# # WHY `perf_stat_c` IS NOT IN HERE — the same load-bearing reason it is not in `lib-measure.sh`
#
# `perf_invocation_lint_tree` DISCOVERS which file owns the single perf wrapper and lints EXACTLY
# ONE file in `owner` mode and every other `scripts/perf/*.sh` in `library` mode, where DEFINING
# `perf_stat_c` is itself a FINDING ("the rig has exactly ONE"). Moving the wrapper into a library
# would flip the owner and invert layer 1 of the three-layer perf guard. This library neither
# defines nor calls it: it runs before the first perf window exists.
#
# # WHAT THIS LIBRARY READS FROM THE DRIVER, stated because it is a real coupling
#
# `build_release_binaries` reads `$DO_BUILD`, `$REPO_ROOT`, `$OUT_DIR`; `record_measured_binaries`
# reads `$DO_BUILD`, `$HERE`, `$OUT_DIR`, `$BIN`, `$REPO_ROOT`. That is the same coupling the code
# had as driver-local statements, recorded rather than hidden. Under the driver's `set -u` an unset
# global is a fatal error rather than an empty expansion, so a caller that sourced this and skipped
# the setup fails loudly instead of measuring nothing.
#
# # FAILURE PROPAGATION, stated because a split is where it gets lost
#
# Both functions `return 2` on failure and NEITHER runs in a command substitution at its call site,
# so `|| exit 2` in the driver is what terminates the run — the `exit`-inside-a-subshell trap that
# bit `lib-outdir.sh` on this branch cannot apply here. No error path became permissive: every
# refusal that was a `{ echo …; exit 2; }` is now a `{ echo …; return 2; }` under the driver's
# `|| exit`, and the diagnostics are unchanged.

# THIS LIBRARY'S OWN DIRECTORY, resolved from `BASH_SOURCE` at source time — the pattern
# `lib-measure.sh` established. NOT the driver's `$HERE`: that would be an undocumented coupling,
# and under `set -u` a caller that skipped it would die mid-run rather than at source time.
WS0_BINARIES_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# The three programs this rig MEASURES — the bare-scan arm, the Flight server, the load generator.
# `ws0_binaries.MEASURED_BINARIES` holds the same set for the record's completeness check; the two
# are the same fact in the two languages that need it, and `test_ws0_provenance_guards.sh` asserts
# they AGREE, so a program added to one and not the other is a finding rather than a silent gap.
WS0_MEASURED_BINARIES=(ws0-scan-bench cqlite-flight flight-loadgen)

# build_release_binaries — the release build (unless `--no-build`), then EXISTENCE of every binary.
#
# The existence loop runs in BOTH modes deliberately: after a build it confirms the build produced
# what this rig runs, and under `--no-build` it is the only thing standing between the driver and a
# missing artifact. Its diagnostic names `--no-build` because that is the likely cause.
build_release_binaries() {
  if [[ "$DO_BUILD" == "1" ]]; then
    echo "building release binaries…"
    (cd "$REPO_ROOT" && cargo build --release -p ws0-corpus-gen -p cqlite-flight -p flight-loadgen) \
      > "$OUT_DIR/build.log" 2>&1 \
      || { echo "FATAL: release build failed — see $OUT_DIR/build.log" >&2; return 2; }
  fi
  local b
  for b in "${WS0_MEASURED_BINARIES[@]}"; do
    [[ -x "$BIN/$b" ]] || {
      echo "FATAL: $BIN/$b missing (drop --no-build, or build it)" >&2; return 2; }
  done
}

# record_measured_binaries — WHICH BINARIES ARE ABOUT TO BE MEASURED (#3272 round 10, M2).
#
# `--no-build` accepts ANY executable already under `target/release`, and the session manifest
# recorded neither the source revision nor any binary digest — so a STALE artifact could be measured
# and reported as a result for the current checkout. This rig's whole output is a RATIO BETWEEN TWO
# BINARIES: an old `cqlite-flight` against a current `ws0-scan-bench` is a number about two moments
# in the repo's history, indistinguishable in the report from a number about one.
#
# `--no-build` is RETAINED, not forbidden — re-measuring without a 5-minute rebuild is the normal
# operator loop, and removing it would push an operator toward editing the driver. What made it
# dangerous was the SILENCE. Recorded here (revision, dirty state, build mode, per-binary sha256)
# and REQUIRED by `ws0_report.py`; and one check is ENFORCED, failing closed: a binary written
# BEFORE the HEAD commit cannot have been built from it. Full argument, including why report time
# does NOT re-derive these digests (the F6 argument — a results dir is reviewed on other hosts and
# after rebuilds): `scripts/perf/ws0_binaries.py`.
#
# The build MODE is derived from `$DO_BUILD` here rather than passed in, so it cannot disagree with
# what `build_release_binaries` above actually did.
record_measured_binaries() {
  local mode="reused"
  [[ "$DO_BUILD" == "1" ]] && mode="built"
  # The configuration reaches python through the ENVIRONMENT rather than a positional argv, for the
  # reason the driver's other python call sites do: a continuation line whose first token is a bare
  # `"$VAR"` is treated as a possible INVOCATION by `perf_invocation_lint`'s fail-closed layer 1.
  WS0_BUILD_MODE="$mode" python3 -c '
import os, pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_validate import Invalid
from ws0_binaries import describe_record, record_binary_provenance
try:
    rec = record_binary_provenance(pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3]),
                                   pathlib.Path(sys.argv[4]), os.environ["WS0_BUILD_MODE"])
except Invalid as exc:
    print(f"FATAL: {exc}", file=sys.stderr)
    raise SystemExit(1)
print(describe_record(rec))
' "$WS0_BINARIES_LIB_DIR" "$OUT_DIR" "$BIN" "$REPO_ROOT" \
    || { echo "FATAL: could not record WHICH BINARIES this session measures — the report" >&2
         echo "       REQUIRES it, because --no-build accepts any executable already under" >&2
         echo "       target/release, so an unrecorded session may have measured artifacts of a" >&2
         echo "       different revision and reported them as results for this checkout, and" >&2
         echo "       this rig's entire output is a RATIO BETWEEN TWO BINARIES (#3272 M2)." >&2
         return 2; }
}
