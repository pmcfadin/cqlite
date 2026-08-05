#!/usr/bin/env bash
# lib-measure.sh — THE TWO MEASUREMENT LEGS: how ONE rep of each arm is executed and
# counted (issue #3272 review round 9, campsite-rule split).
#
# Sourced, not executed, and it sets NO shell options: `set -euo pipefail` in a library mutates
# the SOURCING shell's options, which is the caller's decision (the same rule `lib-cpu.sh`,
# `lib-args.sh`, `lib-host-state.sh`, `lib-perf-lint.sh`, `lib-server.sh` and `lib-outdir.sh`
# follow). The driver sets all three itself.
#
# # Why this is a library, and why THIS seam
#
# Split out of `ws0-baseline.sh` under the campsite rule: source target ~800 lines, and round 9's
# guard fixes took that file to 1008 — 200+ over and moving the wrong way. Note the gate's
# `file-size` ratchet is `.rs`-ONLY, so a shell file crosses the threshold SILENTLY; this is
# checked with `wc -l` rather than left to the gate.
#
# The seam is a RESPONSIBILITY, not a line count. Every other rig library owns one question about
# whether a measurement means what it says; this one owns the last of them:
#
#     lib-cpu.sh          are the pinned CPUs one physical core?
#     lib-host-state.sh   is the host's state put back?
#     lib-args.sh         are the arguments values this rig can measure?
#     lib-perf-lint.sh    is the counting domain CPU-wide?
#     lib-outdir.sh       do the artifacts being read all come from ONE session?
#     lib-measure.sh      how is ONE rep of an arm executed, prewarmed and counted?
#
# What deliberately STAYS in the driver, because it is the part that must remain legible in one
# file: the ORDER of operations (arguments before creation, verification before measurement, the
# pin before the first rep), the round/rotation loop, and `perf_stat_c`.
#
# # WHY `perf_stat_c` IS NOT IN HERE, which is load-bearing rather than a judgement call
#
# The obvious "measurement legs" seam would also take the perf wrapper. It MUST NOT, and the
# reason is a lint invariant this rig rests on: `perf_invocation_lint_tree` DISCOVERS which file
# owns the single wrapper and lints EXACTLY ONE file in `owner` mode and every other
# `scripts/perf/*.sh` in `library` mode — where defining `perf_stat_c` is itself a FINDING ("the
# rig has exactly ONE"). Moving the wrapper into a library would flip the owner and make the
# driver a library that must not define it, inverting layer 1 of the three-layer perf guard.
# `scripts/tests/test_ws0_cpu_pinning_guards.sh` also text-extracts it with
# `awk '/^perf_stat_c\(\)/,/^}/' "$DRIVER"`. So the wrapper stays where its ownership is
# asserted, and this library CALLS it — which is exactly what every other caller in the rig does.
#
# # WHAT THIS LIBRARY READS FROM THE DRIVER, stated because it is a real coupling
#
# These functions are the rig's most environment-dependent code and are called ONLY by the
# driver's measurement loop, AFTER every argument check, the topology verification, the corpus
# and schema verification and the session pin. They read driver globals — `$SERVER_CPUS`,
# `$CLIENT_CPUS`, `$BIN`, `$CORPUS`, `$OUT_DIR`, `$PORT`, `$TICKET_TEMPLATE`, `$SCAN_PASSES`,
# `$STEP_DURATION`, `$COLD_STEP_DURATION`, `$SERVER_PID` — and call `perf_stat_c`,
# `drop_caches_if_cold`, `stop_server`, `require_port_free` and `await_server_ready`.
#
# That coupling is UNCHANGED by the split (these were driver-local functions reading the same
# globals), and it is recorded rather than hidden. It is also why the driver sources this LAST,
# after `lib-server.sh`: the sourcing order is the dependency order.
#
# Under the driver's `set -u` an unset global is a fatal error rather than an empty expansion, so
# a caller that sourced this and skipped the setup fails loudly instead of measuring nothing.

# ---------------------------------------------------------------------------
# Arm A — the bare scan
# ---------------------------------------------------------------------------
measure_scan() {
  local temp="$1" rep="$2" tag="scan-$temp-$rep"
  drop_caches_if_cold "$temp"

  # --- untimed PREWARM (warm arm only) -----------------------------------------
  # A full scan OUTSIDE every perf window, before the measured legs, so the warm
  # arm measures warm work (issue #3096 review, finding 1).
  #
  # Why this is not optional. `--setup-only` opens the corpus and ingests the
  # schema; it does NOT read the `Data.db` pages the scan streams. So on a
  # genuinely cold page cache — a fresh box, or a `--temp cold` rep earlier in the
  # same session having dropped the caches — the FIRST "warm" rep faulted those
  # pages in from disk and was measured partly cold. At `--reps 1` that partly-cold
  # rep IS the reported median, and nothing in the output said so: the warm/cold
  # separation spec R2/AC5 requires had silently broken. The Flight arm has always
  # prewarmed (below); this arm did not, and it is the DENOMINATOR of the 1.3x
  # ratio.
  #
  # FAIL CLOSED here, unlike the Flight arm's record-and-continue. The bias
  # direction is what differs: a partly-cold BARE SCAN reads SLOWER, which SHRINKS
  # `bare/flight` and makes the 1.3x target EASIER to hit — a degradation that can
  # manufacture a win. (A degraded Flight prewarm biases against do_get, so
  # continuing with a recorded label is honest there.) A prewarm scan that fails
  # while the timed scan would succeed is also not a thing: same binary, same
  # arguments, same corpus.
  #
  # Skipped on the cold arm BY DESIGN — a prewarm there would make "cold"
  # meaningless.
  local prewarm_status="skipped-cold-arm"
  if [[ "$temp" == "warm" ]]; then
    if taskset -c "$SERVER_CPUS" "$BIN/ws0-scan-bench" \
        --corpus "$CORPUS" --passes 1 \
        > "$OUT_DIR/$tag.prewarm.json" 2> "$OUT_DIR/$tag.prewarm.err"; then
      prewarm_status="ok"
    else
      prewarm_status="FAILED-exit-$?"
      printf '%s\n' "$prewarm_status" > "$OUT_DIR/$tag.prewarm.status"
      echo "FATAL: bare-scan PREWARM failed for $tag ($prewarm_status)." >&2
      echo "       Without it this 'warm' rep is partly cold, which makes the bare scan" >&2
      echo "       read SLOWER and the 1.3x bare/flight target EASIER — a degradation" >&2
      echo "       that can manufacture a win, so it is refused rather than labelled." >&2
      echo "       See $OUT_DIR/$tag.prewarm.err" >&2
      exit 1
    fi
  fi
  printf '%s\n' "$prewarm_status" > "$OUT_DIR/$tag.prewarm.status"

  # Setup-only leg: the corpus open + schema ingest, under its OWN perf window,
  # so its cycles can be SUBTRACTED from the full run (spec R2).
  perf_stat_c "$OUT_DIR/perf-$tag-setup.csv" \
    taskset -c "$SERVER_CPUS" "$BIN/ws0-scan-bench" \
      --corpus "$CORPUS" --setup-only \
    > "$OUT_DIR/$tag-setup.json" 2> "$OUT_DIR/$tag-setup.err"

  drop_caches_if_cold "$temp"
  perf_stat_c "$OUT_DIR/perf-$tag.csv" \
    taskset -c "$SERVER_CPUS" "$BIN/ws0-scan-bench" \
      --corpus "$CORPUS" --passes "$SCAN_PASSES" \
    > "$OUT_DIR/$tag.json" 2> "$OUT_DIR/$tag.err" \
    || { echo "FATAL: bare-scan rep $tag failed — see $OUT_DIR/$tag.err" >&2; exit 1; }
  echo "  $tag done"
}

# ---------------------------------------------------------------------------
# Arm B — Flight do_get over a real loopback transport
# ---------------------------------------------------------------------------
measure_flight() {
  local temp="$1" rep="$2" arm="$3" tag="flight-$arm-$temp-$rep"
  local step="$STEP_DURATION"
  [[ "$temp" == "cold" ]] && step="$COLD_STEP_DURATION"
  # Only the previous rep's own server — never a `pkill` by name.
  stop_server
  require_port_free "before $tag"
  drop_caches_if_cold "$temp"

  CQLITE_FLIGHT_MERGE_PATH="$arm" taskset -c "$SERVER_CPUS" "$BIN/cqlite-flight" \
    --data-dir "$CORPUS" --listen "127.0.0.1:$PORT" \
    > "$OUT_DIR/$tag.server.log" 2>&1 &
  SERVER_PID=$!
  await_server_ready "$tag"

  # Prewarm OUTSIDE the perf window (warm arm only): opens the readers and fills
  # the warm-handle registry, so the measured window is steady-state scan work
  # and not one-off setup. On the COLD arm this is deliberately skipped — a
  # prewarm would make "cold" meaningless.
  #
  # The outcome is RECORDED, not swallowed (issue #3096 review). A silently failed
  # prewarm downgrades a "warm" claim to a partly-cold one, and the old `|| true`
  # left nothing in results.json or summary.txt to say so. The bias runs AGAINST
  # the Flight arm (a cold-ish arm measures slower), so it cannot manufacture a
  # win — but an unrecorded degradation is still an unrecorded degradation. The
  # run continues rather than aborting: a rep that is honestly labelled
  # `prewarm-failed` is more useful than no rep, and ws0_report.py surfaces the
  # label in every report it writes.
  local prewarm_status="skipped-cold-arm"
  if [[ "$temp" == "warm" ]]; then
    if taskset -c "$CLIENT_CPUS" "$BIN/flight-loadgen" \
        --endpoint "http://127.0.0.1:$PORT" --ticket-template "$TICKET_TEMPLATE" \
        --shape full --ramp 1 --step-duration 20s --round prewarm --out /dev/null \
        > "$OUT_DIR/$tag.prewarm.log" 2>&1; then
      prewarm_status="ok"
    else
      prewarm_status="FAILED-exit-$?"
      echo "  WARNING: prewarm FAILED for $tag ($prewarm_status) — this 'warm' rep is" >&2
      echo "           partly cold. Recorded in results.json and summary.txt; see" >&2
      echo "           $OUT_DIR/$tag.prewarm.log" >&2
    fi
  fi
  printf '%s\n' "$prewarm_status" > "$OUT_DIR/$tag.prewarm.status"

  perf_stat_c "$OUT_DIR/perf-$tag.csv" \
    taskset -c "$CLIENT_CPUS" "$BIN/flight-loadgen" \
      --endpoint "http://127.0.0.1:$PORT" --ticket-template "$TICKET_TEMPLATE" \
      --shape full --ramp 1 --step-duration "$step" \
      --round "$tag" --out "$OUT_DIR/$tag.jsonl" \
    > "$OUT_DIR/$tag.log" 2>&1 \
    || { stop_server; echo "FATAL: flight rep $tag failed — see $OUT_DIR/$tag.log" >&2; exit 1; }

  stop_server
  echo "  $tag done"
}
