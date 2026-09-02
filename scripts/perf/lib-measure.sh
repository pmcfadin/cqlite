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
# `$CLIENT_CPUS`, `$BIN`, `$CORPUS`, `$OUT_DIR`, `$PORT`, `$FLIGHT_ENDPOINT`, `$TICKET_TEMPLATE`,
# `$SCAN_PASSES`, `$STEP_DURATION`, `$COLD_STEP_DURATION`, `$SERVER_PID`, and since #3551
# `$FLIGHT_SERVER_CPUS`, `$FLIGHT_ALLOCATOR`, `$FLIGHT_ALLOCATOR_LIB` and
# `$FLIGHT_ALLOCATOR_LIB_BASENAME` — and call `perf_stat_c`, `drop_caches_if_cold`,
# `stop_server`, `require_port_free` and `await_server_ready`.
#
# They also WRITE two driver globals, which is why both are listed rather than left implicit:
# `$SERVER_PID` (as they always have) and, since #3551, `$PERF_COUNT_CPUS` — the CPU-WIDE
# counting domain, which each leg sets to the CPUs ITS OWN server ran on. That assignment lives
# in the leg rather than in the driver's loop deliberately: the perf window and the `taskset`
# that decides where the work runs are three lines apart HERE, so they cannot drift out of
# agreement, whereas a loop-side assignment is one refactor away from counting the other arm's
# cores. The driver initialises it before any leg runs, so it is never unset.
#
# `$FLIGHT_ENDPOINT` (#3272 round 14, F2) is the ONE spelling of the measured server: the driver
# derives it from the validated `$PORT` and stamps it into the session manifest before rep 1, and
# the reporter compares it EXACTLY against every rep's recorded `endpoint`. These call sites used to
# compose `http://127.0.0.1:$PORT` themselves, which is a second spelling of a pinned fact — under
# `set -u` an unset global fails loudly here, whereas a locally-recomposed one would silently differ
# from the pin and make every rep of a correct run refuse.
#
# That coupling is UNCHANGED by the split (these were driver-local functions reading the same
# globals), and it is recorded rather than hidden. It is also why the driver sources this LAST,
# after `lib-server.sh`: the sourcing order is the dependency order.
#
# Under the driver's `set -u` an unset global is a fatal error rather than an empty expansion, so
# a caller that sourced this and skipped the setup fails loudly instead of measuring nothing.

# THIS LIBRARY'S OWN DIRECTORY, resolved from `BASH_SOURCE` at source time (#3272 round 10, F-A).
#
# `ws0_prewarm.py` is a sibling of this file, and the obvious spelling for reaching it would be the
# driver's `$HERE`. Deliberately NOT that: `$HERE` is not in the driver-globals list documented
# above, so using it would add an UNDOCUMENTED coupling — and under the driver's `set -u` a caller
# that sourced this library without setting `HERE` would die inside the measurement loop, after the
# server is up and a rep is in flight. `BASH_SOURCE[0]` inside a sourced function resolves to THIS
# file whatever the caller did, so the library locates its own sibling and the coupling list above
# stays true.
WS0_MEASURE_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---------------------------------------------------------------------------
# Arm C's evidence — WHICH ALLOCATOR IS THE SERVER PROCESS ACTUALLY RUNNING?
# ---------------------------------------------------------------------------
# verify_flight_allocator_mapping <maps-path> <mode> <lib-basename> <tag>
#
# Echoes the affirmative evidence and returns 0; prints a refusal on stderr and returns 1.
#
# # WHY THIS EXISTS AT ALL, which is the whole of arm C
#
# `LD_PRELOAD` FAILS OPEN. glibc prints
#
#     ERROR: ld.so: object '<path>' from LD_PRELOAD cannot be preloaded ...: ignored.
#
# on stderr and **CONTINUES WITH SYSTEM MALLOC**, exit code 0, server healthy, every row served.
# So without reading the running process, `--flight-allocator jemalloc` would produce a rep that
# is a BYTE-IDENTICAL DUPLICATE of the system arm under a label saying otherwise — the
# instrument-reports-success-without-having-measured shape this rig's integrity contract is built
# around (#3272), and the worst available version of it, because the two arms would agree and the
# agreement would read as a result.
#
# # BOTH DIRECTIONS ARE ASSERTED, and the negative is not the weaker half
#
# On the `system` arm a jemalloc mapping is a REFUSAL, not a curiosity: an operator with
# `LD_PRELOAD` exported in their shell would have the CONTROL arm running the very allocator
# under test, which does not add noise — it INVERTS the comparison. The launch empties
# `LD_PRELOAD` for that reason, and this is what establishes that the emptying worked.
#
# # THE READ IS THREE-VALUED, and "could not measure" is a REFUSAL
#
# An absent maps file (the process already exited), an unreadable one, and an EMPTY one are each
# COULD-NOT-MEASURE — never "no jemalloc mapping is present", which is what a two-valued
# `grep -q` would have made of them. A live process always has a non-empty `maps`, so an empty
# read is a failed measurement and not evidence of anything. That distinction is the difference
# between this check and a check that passes whenever it cannot look.
#
# Every read is a bash builtin over the file's own bytes — no `grep`, no pipeline — because this
# runs under the driver's `set -o pipefail`, where a `grep | head` on a match closes the pipe and
# reports FAILURE on the SUCCESS case (the trap #3248 already hit in this rig, at `nm | grep -q`).
#
# HERMETIC BY CONSTRUCTION: the maps PATH is a parameter, so
# `scripts/tests/test_ws0_flight_arm_guards.sh` drives every branch — including the absent-mapping
# branch this check exists for — against synthetic maps files, with no server, no root and no
# `/proc` of its own.
verify_flight_allocator_mapping() {
  local maps="$1" mode="$2" needle="$3" tag="$4"
  local line="" first_match="" saw_jemalloc=0 n=0
  # THE THREE-VALUED READ. `-L` before `-e`, because `-e` FOLLOWS a symlink and would report a
  # dangling one as plain absence — a different cause with a different remedy.
  if [[ -L "$maps" && ! -e "$maps" ]] || [[ ! -e "$maps" ]]; then
    echo "FATAL: $maps does not exist, so which allocator the Flight server of $tag is running" >&2
    echo "       COULD NOT BE MEASURED. The usual cause is that the process exited between" >&2
    echo "       readiness and this read — see ${OUT_DIR:-<results-dir>}/$tag.server.log." >&2
    echo "       This is a refusal and never a pass: 'the mapping is absent' and 'the mapping" >&2
    echo "       could not be looked for' are different facts, and only the first one could ever" >&2
    echo "       support a verdict (#3551)." >&2
    return 1
  fi
  while IFS= read -r line; do
    n=$((n + 1))
    if [[ -n "$needle" && -z "$first_match" && "$line" == *"$needle"* ]]; then first_match="$line"; fi
    if [[ "$line" == *jemalloc* ]]; then saw_jemalloc=1; fi
  done < "$maps"
  if ((n == 0)); then
    echo "FATAL: $maps was readable but EMPTY, so which allocator the Flight server of $tag is" >&2
    echo "       running COULD NOT BE MEASURED. A live process always publishes its mappings," >&2
    echo "       so an empty read is a failed measurement rather than evidence that no jemalloc" >&2
    echo "       mapping is present — the permissive reading of an unmeasurable state is the" >&2
    echo "       vacuous pass this rig refuses (#3551)." >&2
    return 1
  fi
  case "$mode" in
    jemalloc)
      if [[ -z "$needle" ]]; then
        echo "FATAL: --flight-allocator jemalloc but no library basename was passed to the" >&2
        echo "       mapping check for $tag, so there is nothing to look for. The driver" >&2
        echo "       resolves the library before the first rep; this is an internal" >&2
        echo "       inconsistency and it stops the run rather than asserting nothing (#3551)." >&2
        return 1
      fi
      if [[ -n "$first_match" ]]; then
        echo "jemalloc VERIFIED for $tag: $needle is mapped in the server process ($n mappings read from $maps) | $first_match"
        return 0
      fi
      echo "FATAL: --flight-allocator jemalloc, but NO mapping of '$needle' is present in the" >&2
      echo "       Flight server process of $tag ($n mappings read from $maps)." >&2
      echo "       LD_PRELOAD FAILS OPEN: glibc prints \"object ... cannot be preloaded ...:" >&2
      echo "       ignored\" and CONTINUES with system malloc, exit 0, server healthy. So this" >&2
      echo "       rep would have been a byte-identical duplicate of the system arm under a" >&2
      echo "       label saying otherwise, which is worse than no rep (#3551)." >&2
      echo "       Check ${OUT_DIR:-<results-dir>}/$tag.server.log for that ld.so line, and that the library" >&2
      echo "       matches the binary's architecture." >&2
      return 1 ;;
    system)
      if ((saw_jemalloc == 1)); then
        echo "FATAL: --flight-allocator system, but the Flight server process of $tag HAS a" >&2
        echo "       jemalloc mapping ($n mappings read from $maps)." >&2
        echo "       The most likely cause is an LD_PRELOAD exported in the invoking" >&2
        echo "       environment (the launch empties it, so also suspect /etc/ld.so.preload or" >&2
        echo "       a jemalloc linked into the binary itself)." >&2
        echo "       This is refused rather than noted because the CONTROL arm running the" >&2
        echo "       allocator under test does not add noise — it INVERTS the comparison the" >&2
        echo "       whole session exists to make (#3551)." >&2
        return 1
      fi
      echo "system VERIFIED for $tag: no jemalloc mapping in the server process ($n mappings read from $maps)"
      return 0 ;;
    *)
      echo "FATAL: the allocator mapping check for $tag was passed the unknown mode '$mode'." >&2
      echo "       The driver's argument loop refuses every value but system|jemalloc, so this" >&2
      echo "       is an internal inconsistency. It refuses rather than picking a direction to" >&2
      echo "       assert (#3551)." >&2
      return 1 ;;
  esac
}

# ---------------------------------------------------------------------------
# Arm A — the bare scan
# ---------------------------------------------------------------------------
measure_scan() {
  local temp="$1" rep="$2" tag="scan-$temp-$rep"
  # THE COUNTING DOMAIN IS THIS LEG'S OWN CPUs (#3551). The bare scan always runs on
  # `$SERVER_CPUS` — that is what makes it a pin-identical drift control across arms that differ
  # only in the FLIGHT pin — so this is the value it has always had; it is stated here rather
  # than assumed because the flight leg now sets a different one.
  PERF_COUNT_CPUS="$SERVER_CPUS"
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
  #
  # `ok` REQUIRES A COMPLETE CORPUS SCAN, NOT PROCESS SUCCESS (#3272 round 12, F2).
  # This used to set `prewarm_status="ok"` from the `if` on the bench's exit alone, while
  # redirecting its JSON — which carries `rows_denominator` and a per-pass `rows` — to a file
  # NOBODY READ. `scan_bench` refuses a zero-row pass itself, so exit 0 established "something
  # was read"; it established nothing about HOW MUCH. A partial ingestion (round 10's F-B class)
  # exits 0 having scanned a fraction, leaving the pages this rep is about to be MEASURED over
  # cold while the label reads warm. So the discarded value is read and every timed pass must
  # have observed exactly the PINNED corpus row count (session-corpus-pin.json `rows` — the
  # oracle, never a threshold).
  local prewarm_status="skipped-cold-arm"
  if [[ "$temp" == "warm" ]]; then
    local prewarm_rc=0
    taskset -c "$SERVER_CPUS" "$BIN/ws0-scan-bench" \
        --corpus "$CORPUS" --passes 1 \
        > "$OUT_DIR/$tag.prewarm.json" 2> "$OUT_DIR/$tag.prewarm.err" || prewarm_rc=$?
    # ONE LINE deliberately, for the reason the Flight leg below records: a continuation whose
    # first token is a bare `"$VAR"` is classified a POSSIBLE perf invocation by
    # `lib-perf-lint.sh`'s fail-closed layer 1.
    prewarm_status="$(python3 "$WS0_MEASURE_LIB_DIR/ws0_prewarm.py" scan "$prewarm_rc" "$OUT_DIR/$tag.prewarm.json" "$OUT_DIR")"
    [[ -n "$prewarm_status" ]] || prewarm_status="FAILED-classifier-produced-nothing"
    # FAIL CLOSED on the label, an EXACT comparison (matching `ws0_validate.PREWARM_REQUIRED`).
    # The classifier never exits non-zero — the decision to abort belongs to this LEG, whose bias
    # direction is what makes it different from the Flight arm's record-and-continue.
    if [[ "$prewarm_status" != "ok" ]]; then
      printf '%s\n' "$prewarm_status" > "$OUT_DIR/$tag.prewarm.status"
      echo "FATAL: bare-scan PREWARM did not complete a FULL corpus scan for $tag" >&2
      echo "       ($prewarm_status). The status is derived from the bench's OWN JSON —" >&2
      echo "       every timed pass must have observed exactly the PINNED corpus row" >&2
      echo "       count — not from its exit code, so an exit-0 run that scanned a" >&2
      echo "       FRACTION reads as a failure (#3272 F2)." >&2
      echo "       Without a complete prewarm this 'warm' rep is partly cold, which makes" >&2
      echo "       the bare scan read SLOWER and the 1.3x bare/flight target EASIER — a" >&2
      echo "       degradation that can manufacture a win, so it is refused rather than" >&2
      echo "       labelled. See $OUT_DIR/$tag.prewarm.err and $OUT_DIR/$tag.prewarm.json" >&2
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
  # THE COUNTING DOMAIN IS WHERE THIS SERVER ACTUALLY RUNS (#3551). `perf stat -C` counts the
  # SERVER's CPUs while the load generator runs on the client set, so with a flight pin that
  # differs from `$SERVER_CPUS` the old domain would have collected cycles from cores that served
  # nothing and divided them by this rep's rows. Equal to `$SERVER_CPUS` whenever the flight pin
  # is defaulted, so the argv is unchanged for every pre-#3551 invocation.
  PERF_COUNT_CPUS="$FLIGHT_SERVER_CPUS"

  # `LD_PRELOAD` IS ALWAYS SET, AND ON THE SYSTEM ARM IT IS SET TO EMPTY (#3551).
  #
  # Two facts in one line. On the jemalloc arm it preloads the resolved library into the SERVER
  # PROCESS ONLY — the binary is byte-identical across arms, which is the whole reason to do this
  # with a preload rather than a build flag. On the system arm it EMPTIES any value inherited
  # from the operator's environment, rather than trusting it to be unset: a control arm quietly
  # running jemalloc does not merely add noise, it INVERTS the comparison. What it was set to is
  # recorded in pinning-verification.json, and the outcome is OBSERVED below rather than assumed.
  local preload=""
  [[ "$FLIGHT_ALLOCATOR" == "jemalloc" ]] && preload="$FLIGHT_ALLOCATOR_LIB"
  CQLITE_FLIGHT_MERGE_PATH="$arm" LD_PRELOAD="$preload" taskset -c "$FLIGHT_SERVER_CPUS" "$BIN/cqlite-flight" \
    --data-dir "$CORPUS" --listen "127.0.0.1:$PORT" \
    > "$OUT_DIR/$tag.server.log" 2>&1 &
  SERVER_PID=$!
  await_server_ready "$tag"

  # ...AND THE ALLOCATOR IS VERIFIED FROM THE RUNNING PROCESS, PER REP (#3551).
  # AFTER `await_server_ready`, because a process that has not finished starting has not yet
  # mapped its libraries. FATAL either way: an unmet expectation means this rep measured an arm
  # other than the one it is labelled, which is worse than no rep.
  local alloc_status=""
  if ! alloc_status="$(verify_flight_allocator_mapping \
        "/proc/$SERVER_PID/maps" "$FLIGHT_ALLOCATOR" "$FLIGHT_ALLOCATOR_LIB_BASENAME" "$tag")"; then
    printf '%s\n' "FAILED-$FLIGHT_ALLOCATOR-allocator-UNVERIFIED" > "$OUT_DIR/$tag.allocator.status"
    stop_server
    echo "FATAL: the allocator this rep was LABELLED with was not the one the server process" >&2
    echo "       is running ($tag, --flight-allocator $FLIGHT_ALLOCATOR). See the refusal" >&2
    echo "       above and $OUT_DIR/$tag.server.log." >&2
    exit 1
  fi
  printf '%s\n' "$alloc_status" > "$OUT_DIR/$tag.allocator.status"

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
  #
  # `ok` REQUIRES AN AFFIRMATIVE MEASUREMENT, NOT AN EXIT STATUS (#3272 round 10, F-A).
  # This used to set `prewarm_status="ok"` from the `if` on the loadgen's exit alone, while
  # passing `--out /dev/null` — discarding the ONLY record of what the prewarm did. The loadgen
  # exits 0 whenever the ramp completes, and a step whose every request was shed (#2420) or
  # errored completes normally, because those outcomes are COUNTED rather than fatal. So a
  # prewarm that served NOTHING, or that streamed zero rows, was recorded as healthy and the rep
  # claimed a WARM measurement having faulted in nothing. Same class as AC1's finding 2
  # (`skipped-cold-arm` satisfying the prewarm guard) at a different line.
  #
  # So the JSONL is RETAINED to a real path and `ws0_prewarm.classify_prewarm_jsonl` decides the
  # label from it. The honest-degradation behaviour is unchanged — the run continues either way.
  #
  # ...AND `ok` NOW REQUIRES A COMPLETE CORPUS SCAN (#3272 round 12, F2). F-A's rule was
  # `requests_ok >= 1 AND rows_total >= 1`, which is a NON-ZERO check where the property is a
  # COMPLETENESS one: a request that streamed 40 of 200,000 rows satisfied it while leaving
  # essentially every page cold. The oracle is the PINNED corpus row count from
  # session-corpus-pin.json (`rows`), never a threshold — hence `$OUT_DIR` is passed, so the
  # classifier reads the pin rather than trusting a count this leg supplied.
  local prewarm_status="skipped-cold-arm"
  if [[ "$temp" == "warm" ]]; then
    local prewarm_rc=0
    # NOT `/dev/null`: this artifact IS the evidence the status is derived from.
    taskset -c "$CLIENT_CPUS" "$BIN/flight-loadgen" \
        --endpoint "$FLIGHT_ENDPOINT" --ticket-template "$TICKET_TEMPLATE" \
        --shape full --ramp 1 --step-duration 20s --round prewarm \
        --out "$OUT_DIR/$tag.prewarm.jsonl" \
        > "$OUT_DIR/$tag.prewarm.log" 2>&1 || prewarm_rc=$?
    # The classifier never fails and never exits non-zero (it must not abort a rep the rig has
    # decided to keep and label), so its output is the status. A classifier that could not run at
    # all would leave this empty, which is caught immediately below rather than recorded as blank.
    # ONE LINE deliberately. Split across a `\` continuation, the second line's first token is
    # `"$prewarm_rc"` — a bare variable expansion — and `lib-perf-lint.sh`'s `is_var_command`
    # classifies such a line as a POSSIBLE perf invocation (an unresolvable command word fails
    # CLOSED, which is correct: `$CMD -p 1` must not be waved through). So the continuation was a
    # real finding, not a false positive, and the fix is to give the lint a resolvable command
    # word rather than to mark the line `perf-lint-allow` — a marker here would suppress a check
    # that is working.
    prewarm_status="$(python3 "$WS0_MEASURE_LIB_DIR/ws0_prewarm.py" flight "$prewarm_rc" "$OUT_DIR/$tag.prewarm.jsonl" "$OUT_DIR")"
    [[ -n "$prewarm_status" ]] || prewarm_status="FAILED-classifier-produced-nothing"
    # An EXACT comparison, matching `ws0_validate.PREWARM_REQUIRED`'s exact per-temperature match —
    # a prefix test here would call a hypothetical `ok-ish` label healthy while the reporter called
    # it degraded, i.e. two vocabularies for one fact.
    if [[ "$prewarm_status" != "ok" ]]; then
      echo "  WARNING: prewarm DEGRADED for $tag ($prewarm_status) — this 'warm' rep is" >&2
      echo "           partly cold. The status is derived from the prewarm's OWN JSONL" >&2
      echo "           (every successful request must have streamed the PINNED corpus row" >&2
      echo "           count), not from its exit code, so an exit-0 run that served nothing" >&2
      echo "           — or scanned only a FRACTION — reads as a failure (#3272 F-A/F2)." >&2
      echo "           Recorded in results.json and summary.txt; see" >&2
      echo "           $OUT_DIR/$tag.prewarm.log and $OUT_DIR/$tag.prewarm.jsonl" >&2
    fi
  fi
  printf '%s\n' "$prewarm_status" > "$OUT_DIR/$tag.prewarm.status"

  perf_stat_c "$OUT_DIR/perf-$tag.csv" \
    taskset -c "$CLIENT_CPUS" "$BIN/flight-loadgen" \
      --endpoint "$FLIGHT_ENDPOINT" --ticket-template "$TICKET_TEMPLATE" \
      --shape full --ramp 1 --step-duration "$step" \
      --round "$tag" --out "$OUT_DIR/$tag.jsonl" \
    > "$OUT_DIR/$tag.log" 2>&1 \
    || { stop_server; echo "FATAL: flight rep $tag failed — see $OUT_DIR/$tag.log" >&2; exit 1; }

  stop_server
  echo "  $tag done"
}
