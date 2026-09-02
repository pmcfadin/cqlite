#!/usr/bin/env bash
# lib-flight-arm.sh — WHAT, EXACTLY, DIFFERS ABOUT THE FLIGHT ARM — AND WAS IT VERIFIED?
# (issue #3551, campsite-rule split out of `ws0-baseline.sh`.)
#
# Sourced, not executed, and it sets NO shell options: `set -euo pipefail` in a library mutates
# the SOURCING shell's options, which is the caller's decision (the rule every other rig library
# follows). The driver sets all three itself.
#
# # Why this is a library, and why THIS seam
#
# `--flight-server-cpus` / `--flight-pin-mode` / `--flight-allocator` exist so that ONE property
# of the Flight arm can move while the bare-scan arm stays code-identical AND pin-identical in
# the same session — §3b step 3's drift control, which the rig had never had. Adding them took
# `ws0-baseline.sh` ~250 lines further past the ~800-line source target, and that file's own
# header records the rule: every guard round since #3272 round 9 has been answered by a SPLIT
# rather than by growth (the gate's `file-size` ratchet is `.rs`-ONLY, so a shell file crosses
# the threshold SILENTLY — this is checked with `wc -l`).
#
# The seam is a RESPONSIBILITY and not a line count. Every rig library owns one question about
# whether a measurement means what it says; this one owns the question #3551 introduces:
#
#     lib-cpu.sh          are the pinned CPUs one physical core?
#     lib-flight-arm.sh   the two arms no longer run the same way — what differs, and was the
#                         difference VERIFIED rather than requested?
#     lib-measure.sh      how is ONE rep of an arm executed, prewarmed and counted?
#
# What deliberately STAYS in the driver: the flag DEFAULTS and the `--help` text (an argument's
# default belongs beside the argument), the argument-loop arms, and the ORDER of operations —
# each function here is called from ONE visible line in the driver, at the point in the sequence
# where it belongs, because that order is itself a correctness property.
#
# # WHAT THIS LIBRARY READS AND WRITES, stated because it is a real coupling
#
# It reads `$FLIGHT_SERVER_CPUS`, `$FLIGHT_PIN_MODE`, `$FLIGHT_ALLOCATOR`,
# `$FLIGHT_ALLOCATOR_LIB` and `$CLIENT_CPUS` — the driver's validated argument values — and calls
# `verify_cpus_online`, `verify_sibling_pair`, `verify_distinct_cores` and `verify_disjoint` from
# `lib-cpu.sh`, so the driver sources this AFTER that one (the sourcing order is the dependency
# order). `record_flight_allocator_facts` WRITES four driver globals and
# `verify_flight_arm_pin` writes `$WS0_FLIGHT_PIN_VERIFIED`; each is listed at its own function,
# and under the driver's `set -u` a caller that skipped the setup fails loudly rather than
# measuring nothing.

# The paths probed when `--jemalloc-lib` is not given. Multi-arch Debian/Ubuntu first (the
# delivery box), then the two common non-multi-arch layouts. Beside the prober that consumes it,
# rather than in the driver's defaults block, because the LIST and the THREE-VALUED probe are one
# decision: what counts as "this host has no jemalloc".
FLIGHT_ALLOCATOR_LIB_CANDIDATES="/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
/usr/lib/aarch64-linux-gnu/libjemalloc.so.2
/usr/lib64/libjemalloc.so.2
/usr/lib/libjemalloc.so.2
/usr/local/lib/libjemalloc.so.2"

# --- WHAT STATE IS ONE PATH IN? THREE-VALUED, because two-valued always guesses (#3551) -----
# `present` / `absent` / a NAMED unusable state. A plain `[ -f ]` collapses "this host has no
# libjemalloc" onto "this host has one I cannot read", and those have different remedies —
# install it vs fix its permissions — while the permissive reading of either is a run labelled
# `jemalloc` that measured system malloc. `-L` is tested FIRST because `-e`/`-f` FOLLOW the link,
# so a DANGLING symlink would otherwise read as `absent`, i.e. as a host with no jemalloc when
# what it has is a broken install.
flight_lib_state() {
  local path="$1"
  if [[ -L "$path" && ! -e "$path" ]]; then echo "dangling-symlink"; return 1; fi
  if [[ ! -e "$path" ]]; then echo "absent"; return 1; fi
  if [[ ! -f "$path" ]]; then echo "not-a-regular-file"; return 1; fi
  if [[ ! -r "$path" ]]; then echo "unreadable"; return 1; fi
  echo "present"
}

# --- RESOLVE THE PRELOADED LIBRARY ONCE, BEFORE ANY MEASUREMENT (#3551) ---------------------
# Echoes the resolved absolute path; refuses (rc 1, diagnostic on stderr) otherwise. Called
# ABOVE the argument boundary because it reads nothing but file metadata and because "refusing a
# value after acting on it is not refusing it" — the same rule --bin-dir and --profile-out follow
# a few lines up. An unusable candidate is a REFUSAL naming it, never a skip to the next one: a
# silently skipped unreadable library is how a host with jemalloc installed reports that it has
# none.
resolve_flight_allocator_lib() {
  local cand state
  if [[ -n "$FLIGHT_ALLOCATOR_LIB" ]]; then
    state="$(flight_lib_state "$FLIGHT_ALLOCATOR_LIB")" || {
      echo "FATAL: --jemalloc-lib '$FLIGHT_ALLOCATOR_LIB' is $state, not a readable regular file." >&2
      echo "       --flight-allocator jemalloc preloads exactly this path into the Flight" >&2
      echo "       server, so an unusable one has no reachable success: glibc would print" >&2
      echo "       'object ... cannot be preloaded ... ignored' and CONTINUE with system" >&2
      echo "       malloc, and the rep would be arm B wearing arm C's label. Refused here," >&2
      echo "       before any build, cache drop or measurement." >&2
      echo "       Install it and drop the flag:  sudo apt-get install -y libjemalloc2" >&2
      return 1
    }
    printf '%s\n' "$FLIGHT_ALLOCATOR_LIB"
    return 0
  fi
  while IFS= read -r cand; do
    [[ -n "$cand" ]] || continue
    state="$(flight_lib_state "$cand")" && { printf '%s\n' "$cand"; return 0; }
    if [[ "$state" != "absent" ]]; then
      echo "FATAL: the candidate jemalloc library '$cand' is $state." >&2
      echo "       That is a COULD-NOT-MEASURE state, not an absence, so it is refused rather" >&2
      echo "       than skipped: skipping it would report 'jemalloc is not installed' about a" >&2
      echo "       host that has it, and the remedy for the two is different (#3551)." >&2
      echo "       Fix that path, or name a usable one with --jemalloc-lib PATH." >&2
      return 1
    fi
  done <<<"$FLIGHT_ALLOCATOR_LIB_CANDIDATES"
  echo "FATAL: --flight-allocator jemalloc was requested and no jemalloc library was found." >&2
  echo "       Probed (each verified ABSENT, not merely unmatched):" >&2
  while IFS= read -r cand; do
    [[ -n "$cand" ]] || continue
    echo "         $cand" >&2
  done <<<"$FLIGHT_ALLOCATOR_LIB_CANDIDATES"
  echo "       Remedy:  sudo apt-get install -y libjemalloc2" >&2
  echo "       (or name the path with --jemalloc-lib PATH). This is a REFUSAL and never a" >&2
  echo "       fall-through to --flight-allocator system: an arm C that quietly measured" >&2
  echo "       system malloc would be a byte-identical duplicate of arm B under a label that" >&2
  echo "       says otherwise (#3551)." >&2
  return 1
}

# --- WHAT IS RECORDED for the allocator, resolved ONCE (#3551) ------------------------------
# Sets four driver globals — `FLIGHT_ALLOCATOR_LIB`, `FLIGHT_ALLOCATOR_LIB_RECORDED`,
# `FLIGHT_ALLOCATOR_LIB_BASENAME`, `FLIGHT_ALLOCATOR_VERIFICATION` — so the manifest, the pin
# record and the per-rep verification all read ONE value. Returns non-zero (with the resolver's
# own diagnostic already on stderr) rather than exiting, so the caller decides the exit code:
# this is called ABOVE the driver's argument boundary, where every refusal is exit 2.
# `none (...)` is a positive statement rather than an empty field: "no library" and "nobody wrote
# the field down" must not look the same in an artifact the report cites.
record_flight_allocator_facts() {
if [[ "$FLIGHT_ALLOCATOR" == "jemalloc" ]]; then
  FLIGHT_ALLOCATOR_LIB="$(resolve_flight_allocator_lib)" || return 1
  FLIGHT_ALLOCATOR_LIB_RECORDED="$FLIGHT_ALLOCATOR_LIB"
  FLIGHT_ALLOCATOR_LIB_BASENAME="${FLIGHT_ALLOCATOR_LIB##*/}"
  # WHAT IS ASSERTED PER REP, recorded verbatim into the pin record so the report's allocator
  # line cites a mechanism rather than a label. It also states its OWN LIMIT, in the record, for
  # the reason `provenance` does: the per-rep files are written where the observation is made and
  # NOTHING AT REPORT TIME requires them to be present.
  FLIGHT_ALLOCATOR_VERIFICATION="per rep, AFTER await_server_ready: /proc/<server-pid>/maps is READ and must carry a mapping whose path contains '$FLIGHT_ALLOCATOR_LIB_BASENAME'; an absent mapping is FATAL for that rep, and an unreadable/empty maps file is FATAL as COULD-NOT-MEASURE (never read as verified). Necessary because glibc prints 'object ... cannot be preloaded ... ignored' and CONTINUES with system malloc, which would make this arm a byte-identical duplicate of the system arm under a label saying otherwise. Each rep's outcome is written to <tag>.allocator.status by scripts/perf/lib-flight-arm.sh verify_flight_allocator_mapping. DECLARED LIMIT: the driver ABORTS on a failure, and nothing at REPORT time requires those per-rep files to exist — that completeness check is the boundary-observation shape (#3272 round 22) and is NOT implemented for the allocator (#3551)."
else
  FLIGHT_ALLOCATOR_LIB=""
  FLIGHT_ALLOCATOR_LIB_RECORDED="none (system malloc; any inherited LD_PRELOAD is EMPTIED for the server launch, and the absence of a jemalloc mapping is asserted per rep)"
  FLIGHT_ALLOCATOR_LIB_BASENAME=""
  # THE NEGATIVE IS ASSERTED TOO. A control arm silently running jemalloc — an operator with
  # `LD_PRELOAD` exported in their shell — would INVERT the comparison, so it is refused rather
  # than assumed: the launch EMPTIES `LD_PRELOAD` and the absence of a jemalloc mapping is then
  # OBSERVED. Same declared limit as the jemalloc branch.
  FLIGHT_ALLOCATOR_VERIFICATION="per rep, AFTER await_server_ready: /proc/<server-pid>/maps is READ and must carry NO jemalloc mapping; one present is FATAL, and an unreadable/empty maps file is FATAL as COULD-NOT-MEASURE (never read as verified). LD_PRELOAD is EMPTIED for the server launch rather than trusted to be unset, because a control arm quietly running jemalloc inverts the whole result. Each rep's outcome is written to <tag>.allocator.status by scripts/perf/lib-flight-arm.sh verify_flight_allocator_mapping. DECLARED LIMIT: the driver ABORTS on a failure, and nothing at REPORT time requires those per-rep files to exist — that completeness check is the boundary-observation shape (#3272 round 22) and is NOT implemented for the allocator (#3551)."
fi
}

# --- THE FLIGHT ARM'S PIN, VERIFIED WITH THE SAME RIGOUR AS THE SERVER'S (#3551) -----------
# Three checks, all BEFORE the first rep and all fail-closed, in the order whose diagnostic is
# most specific:
#
#   1. every CPU EXISTS and is ONLINE. `sched_setaffinity` ANDs the requested mask with
#      `cpu_online_mask`, so an offline member is silently dropped and the manifest then records
#      CPUs that never ran an instruction (#3272 round 21, one flag over).
#   2. the requested PIN MODE holds, read from the real `thread_siblings_list`. Two modes, two
#      affirmative assertions — never a relaxation. The `*` arm is an internal fail-closed guard:
#      the argument loop already refused every other value, and a mode reaching here unhandled
#      must stop the run rather than inherit either assertion.
#   3. DISJOINT from the client set, for the reason the server set is: a client sharing a
#      physical core with the server puts the client's own cost inside the counted window.
#
# Returns non-zero on any refusal (each callee's diagnostic is already on stderr) so the driver
# decides the exit code, and echoes what it verified.
#
# `verify_cpus_online`'s stdout is DISCARDED here and its refusal is what this call is for: its
# success line ends "only the SERVER set must be one physical core", which is a true statement
# about `--server-cpus` and a misleading one printed under a flight label. The substance of the
# flight verification is the pin-mode echo below, which is captured and RECORDED.
verify_flight_arm_pin() {
verify_cpus_online "$FLIGHT_SERVER_CPUS" "flight server" >/dev/null || return 1
echo "flight server CPUs: $FLIGHT_SERVER_CPUS -> verified present and ONLINE"
case "$FLIGHT_PIN_MODE" in
  siblings)
    WS0_FLIGHT_PIN_VERIFIED="$(verify_sibling_pair "$FLIGHT_SERVER_CPUS" "flight server")" || return 1 ;;
  distinct-cores)
    WS0_FLIGHT_PIN_VERIFIED="$(verify_distinct_cores "$FLIGHT_SERVER_CPUS" "flight server")" || return 1 ;;
  *)
    echo "FATAL: --flight-pin-mode '$FLIGHT_PIN_MODE' reached the topology stage unhandled." >&2
    echo "       The argument loop refuses every value but siblings|distinct-cores, so this is" >&2
    echo "       an internal inconsistency. It stops the run rather than defaulting to either" >&2
    echo "       assertion: which property was VERIFIED is what the report claims (#3551)." >&2
    return 1 ;;
esac
echo "$WS0_FLIGHT_PIN_VERIFIED"
verify_disjoint "$FLIGHT_SERVER_CPUS" "$CLIENT_CPUS" || return 1
}

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
