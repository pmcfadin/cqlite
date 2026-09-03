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

# ===========================================================================================
# THE ENVIRONMENT IS PART OF THE MEASUREMENT, AND IT WAS RECORDED NOWHERE (#3551 item 8)
# ===========================================================================================
# `lib-binaries.sh` freezes and digests three binaries; the session manifest captured NO
# environment at all (the only `os.environ` read in the whole session/binary-spec path was
# `WS0_BUILD_MODE`). With ONE binary set across all arms — deliberate, and kept — the artifact
# sets for "glibc" and "jemalloc" therefore differed in NOTHING that is written down: arm A and
# arm C were indistinguishable in every recorded field, which makes arm C unfalsifiable.
#
# The governing rule is `docs/reports/ws0-3552-report.md` §4: "state `RUSTFLAGS` and
# `CARGO_ENCODED_RUSTFLAGS` as measured … A reproduction only corroborates if its environment
# differs — not just its tree, box, or operator." So both are recorded here too, AS MEASURED:
# this box exports `RUSTFLAGS=-D warnings` by default, which silently drops cargo's managed mold
# flags, and an unrecorded environment has already cost this repo an hour once.
#
# AMBIENT and INJECTED are SEPARATE fields, and that separation is the substance: "the operator
# had a stray LD_PRELOAD" and "the rig set one on purpose" are different facts, and only one of
# them is a defect.

# The environment variables that can change what a measurement MEANS. `MALLOC_*` is a family, so
# it is DISCOVERED at run time rather than enumerated — a knob nobody listed is exactly the one
# that would go unrecorded.
WS0_ENV_KEYS_RECORDED="LD_PRELOAD LD_LIBRARY_PATH RUSTFLAGS CARGO_ENCODED_RUSTFLAGS"

# ws0_ambient_env_record — the AMBIENT values, as MEASURED in this driver's own environment.
#
# Echoes one `; `-joined line of `KEY=VALUE` entries, with an affirmative `<unset>` for an absent
# variable and `MALLOC_VARS=<none>` for an empty family: "nothing was set" and "nobody wrote the
# field down" must not look the same in an artifact the report cites. `${VAR+set}` distinguishes
# UNSET from SET-EMPTY, which `${VAR:-}` cannot — and a set-empty `LD_PRELOAD` is a different
# fact from an absent one.
#
# DECLARED RESIDUAL: the rendering is `; `-joined, so a VALUE containing `; ` is visually
# ambiguous in the record. It is recorded VERBATIM rather than escaped, because mangling it would
# make the field lie about what was measured, and the values that matter here (`-D warnings`,
# a library path) do not contain the separator. The KEYS are what the reader asserts.
ws0_ambient_env_record() {
  local key out="" v mallocs=""
  for key in $WS0_ENV_KEYS_RECORDED; do
    if [[ -z "${!key+set}" ]]; then v="<unset>"; else v="${!key}"; fi
    out+="$key=$v; "
  done
  # THE MALLOC FAMILY, DISCOVERED BY PREFIX. `compgen -v MALLOC_` lists every set variable with
  # that prefix, so a `MALLOC_*` nobody anticipated is still recorded — an enumerated list would
  # miss exactly the knob that then goes unrecorded. Sorted, so two runs on one box render
  # identically. `|| true` INSIDE the group because compgen exits non-zero when nothing matches
  # and this rig runs under `set -o pipefail`: without it a clean environment would make the
  # pipeline fail rather than report "none".
  local m
  for m in $( { compgen -v MALLOC_ || true; } | sort ); do
    mallocs+="${mallocs:+,}$m=${!m}"
  done
  out+="MALLOC_VARS=${mallocs:-<none>}"
  printf '%s\n' "$out"
}

# FAIL CLOSED on an ambient allocator variable, BEFORE the first rep (#3551 item 8).
#
# Recorded is not enough for these two: `ws0-scan-bench` inherits the driver's environment, so an
# ambient `LD_PRELOAD` or `MALLOC_*` puts the BARE-SCAN DRIFT CONTROL on the allocator under test
# — and the flight arm's own check would NOT catch it, because the system arm's launch sets
# `LD_PRELOAD` empty for the server. The control arm would be perturbed while every check passed.
#
# `LD_LIBRARY_PATH`/`RUSTFLAGS` are RECORDED and NOT refused: they do not change the allocator,
# and `RUSTFLAGS` is set by default on this box (refusing it would red every correct run here).
# There is deliberately no opt-out variable — an escape hatch on a measurement guard can only buy
# a confident wrong number.
refuse_ambient_allocator_env() {
  local bad="" m
  if [[ -n "${LD_PRELOAD:-}" ]]; then bad+="LD_PRELOAD='$LD_PRELOAD' "; fi
  for m in $( { compgen -v MALLOC_ || true; } | sort ); do bad+="$m='${!m}' "; done
  if [[ -n "$bad" ]]; then
    echo "FATAL: this driver's own environment carries allocator settings: ${bad% }" >&2
    echo "       They would be INHERITED by ws0-scan-bench, which is the BARE-SCAN arm — the" >&2
    echo "       code-identical, pin-identical DRIFT CONTROL that method §3b step 3 requires." >&2
    echo "       A control arm running the allocator under test does not add noise, it INVERTS" >&2
    echo "       the comparison the session exists to make, and the flight arm's own check" >&2
    echo "       cannot see it (the system arm's launch sets LD_PRELOAD empty for the SERVER)." >&2
    echo "       The rig injects allocator settings PER PROCESS, on the flight server's launch" >&2
    echo "       line only, so an ambient one is never how this is asked for." >&2
    echo "       Remedy: run the rig with them removed, e.g." >&2
    echo "         env -u LD_PRELOAD -u MALLOC_ARENA_MAX scripts/perf/ws0-baseline.sh ..." >&2
    echo "       and use --flight-allocator / --flight-malloc-arena-max instead (#3551)." >&2
    return 1
  fi
}

# assert_scan_env_unperturbed <tag> — the BARE SCAN received NEITHER, asserted per rep.
#
# Echoes the affirmative evidence; returns non-zero with a diagnostic otherwise. Called from
# `measure_scan` immediately before its bench launches, and the check is against THIS SHELL's
# environment because that is exactly what those children inherit — same process, nothing can
# change in between. `refuse_ambient_allocator_env` already ran before the first rep; this is the
# per-rep evidence that it is STILL true (a later edit could export one mid-run), which is what
# turns "the rig does not inject into the scan" from an intention into an observation.
assert_scan_env_unperturbed() {
  local tag="$1" bad="" m
  if [[ -n "${LD_PRELOAD:-}" ]]; then bad+="LD_PRELOAD='$LD_PRELOAD' "; fi
  for m in $( { compgen -v MALLOC_ || true; } | sort ); do bad+="$m='${!m}' "; done
  if [[ -n "$bad" ]]; then
    echo "FATAL: the bare-scan rep $tag would inherit allocator settings from the driver's" >&2
    echo "       environment: ${bad% }." >&2
    echo "       The bare scan is the DRIFT CONTROL and must be identical across arms, so this" >&2
    echo "       is refused rather than recorded: a perturbed control inverts the comparison" >&2
    echo "       instead of adding noise (#3551)." >&2
    return 1
  fi
  echo "scan env VERIFIED for $tag: no LD_PRELOAD and no MALLOC_* in the environment the bench inherits"
}

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
# value after acting on it is not refusing it" — the rule --bin-dir and --profile-out follow in
# the driver. An unusable candidate is a REFUSAL naming it, never a skip to the next one: a
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
  # AN AFFIRMATIVE "not injected", never an empty field (#3551): "no arena cap was set" and
  # "nobody wrote the field down" must not look the same in an artifact the report cites.
  if [[ -n "$FLIGHT_MALLOC_ARENA_MAX" ]]; then
    FLIGHT_ARENA_RECORDED="MALLOC_ARENA_MAX=$FLIGHT_MALLOC_ARENA_MAX (injected into the flight SERVER process only)"
  else
    FLIGHT_ARENA_RECORDED="not injected (MALLOC_ARENA_MAX is ABSENT from the server environment, which is deliberately NOT the same as setting it to 0)"
  fi
  if [[ "$FLIGHT_ALLOCATOR" == "jemalloc" ]]; then
    FLIGHT_ALLOCATOR_LIB="$(resolve_flight_allocator_lib)" || return 1
    FLIGHT_ALLOCATOR_LIB_RECORDED="$FLIGHT_ALLOCATOR_LIB"
    FLIGHT_ALLOCATOR_LIB_BASENAME="${FLIGHT_ALLOCATOR_LIB##*/}"
    # WHAT IS ASSERTED PER REP, recorded verbatim into the pin record so the report's allocator
    # line cites a mechanism rather than a label. It also states its OWN LIMIT, in the record, for
    # the reason `provenance` does: the per-rep files are written where the observation is made and
    # NOTHING AT REPORT TIME requires them to be present.
      FLIGHT_ALLOCATOR_VERIFICATION="per rep, AFTER await_server_ready, BOTH /proc/<server-pid>/environ AND /proc/<server-pid>/maps are READ, because they prove DIFFERENT things and neither is sufficient alone: environ must carry the whole NUL-separated entry 'LD_PRELOAD=$FLIGHT_ALLOCATOR_LIB' (what the process RECEIVED, measured rather than echoed from what the driver believes it passed) AND maps must carry a mapping whose path contains '$FLIGHT_ALLOCATOR_LIB_BASENAME' (that the preload TOOK EFFECT — environ alone cannot see glibc silently ignoring an unloadable object and continuing with system malloc, and maps alone cannot see MALLOC_ARENA_MAX at all, since an arena cap leaves no mapping). ARENA: $FLIGHT_ARENA_RECORDED, asserted as an exact whole entry (a substring match would confuse =1 with =16). Either half absent is FATAL for that rep; an unreadable or empty environ/maps is FATAL as COULD-NOT-MEASURE, never read as verified. Each rep's outcome is written to <tag>.allocator.status by scripts/perf/lib-flight-arm.sh verify_flight_server_allocator. DECLARED LIMIT: the driver ABORTS on a failure, and nothing at REPORT time requires those per-rep files to exist — that completeness check is the boundary-observation shape (#3272 round 22) and is NOT implemented for the allocator (#3551)."
  else
    FLIGHT_ALLOCATOR_LIB=""
    FLIGHT_ALLOCATOR_LIB_RECORDED="none (system malloc; any inherited LD_PRELOAD is EMPTIED for the server launch, and the absence of a jemalloc mapping is asserted per rep)"
    FLIGHT_ALLOCATOR_LIB_BASENAME=""
    # THE NEGATIVE IS ASSERTED TOO. A control arm silently running jemalloc — an operator with
    # `LD_PRELOAD` exported in their shell — would INVERT the comparison, so it is refused rather
    # than assumed: the launch EMPTIES `LD_PRELOAD` and the absence of a jemalloc mapping is then
    # OBSERVED. Same declared limit as the jemalloc branch.
      FLIGHT_ALLOCATOR_VERIFICATION="per rep, AFTER await_server_ready, BOTH /proc/<server-pid>/environ AND /proc/<server-pid>/maps are READ: environ must carry NO non-empty 'LD_PRELOAD=' entry (the launch sets it EMPTY rather than trusting it to be unset, and an ambient one is refused before the first rep) AND maps must carry NO jemalloc mapping. Both halves, because they prove different things: environ shows what the process RECEIVED and maps shows what TOOK EFFECT. ARENA: $FLIGHT_ARENA_RECORDED, asserted as an exact whole entry. Either violation is FATAL — a CONTROL arm quietly running the allocator under test does not add noise, it INVERTS the comparison — and an unreadable or empty environ/maps is FATAL as COULD-NOT-MEASURE. Each rep's outcome is written to <tag>.allocator.status by scripts/perf/lib-flight-arm.sh verify_flight_server_allocator. DECLARED LIMIT: the driver ABORTS on a failure, and nothing at REPORT time requires those per-rep files to exist — that completeness check is the boundary-observation shape (#3272 round 22) and is NOT implemented for the allocator (#3551)."
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
# Arm C's evidence — WHAT DID THE SERVER PROCESS RECEIVE, AND WHAT TOOK EFFECT?
# ---------------------------------------------------------------------------
# verify_flight_server_allocator <maps-path> <environ-path> <mode> <lib-path> <arena> <tag>
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
# # BOTH /proc FILES, BECAUSE THEY PROVE DIFFERENT THINGS AND NEITHER IS SUFFICIENT
#
#   * `environ` proves what the process actually RECEIVED. It is the ONLY way to verify
#     `MALLOC_ARENA_MAX` at all — an arena cap leaves NO mapping — and it is a measurement rather
#     than an echo of what the driver believes it passed.
#   * `maps` proves the preload TOOK EFFECT. `environ` alone cannot see glibc's silent fallback
#     to system malloc on an unloadable object.
#
# So: jemalloc ⇒ BOTH (the exact `LD_PRELOAD=<path>` entry AND a jemalloc mapping). Arena cap ⇒
# the exact `MALLOC_ARENA_MAX=<N>` entry. `system` ⇒ NEITHER a jemalloc mapping NOR a non-empty
# `LD_PRELOAD` entry.
#
# # WHOLE ENTRIES, NEVER A SUBSTRING OF THE BLOB
#
# `environ` is NUL-separated, and a substring match on the blob makes `MALLOC_ARENA_MAX=1` match
# `MALLOC_ARENA_MAX=16`. So it is split on NUL (`read -r -d ''`), each entry is matched by its
# `KEY=` prefix, and the VALUE is then compared for EXACT equality.
#
# # BOTH READS ARE THREE-VALUED, and "could not measure" is a REFUSAL
#
# An absent file (the process exited), an unreadable one, and an EMPTY one are each
# COULD-NOT-MEASURE — never "no jemalloc mapping is present" / "no LD_PRELOAD was received",
# which is what a two-valued `grep -q` would have made of them. A live process always has a
# non-empty `maps` and a non-empty `environ`, so an empty read is a failed measurement and not
# evidence of anything. That distinction is the difference between this check and a check that
# passes whenever it cannot look.
#
# Every read is a bash builtin over the files' own bytes — no `grep`, no pipeline — because this
# runs under the driver's `set -o pipefail`, where a `grep | head` on a match closes the pipe and
# reports FAILURE on the SUCCESS case (the trap #3248 already hit in this rig, at `nm | grep -q`).
#
# HERMETIC BY CONSTRUCTION: both paths are PARAMETERS, so
# `scripts/tests/test_ws0_flight_arm_guards.sh` drives every branch — including the
# absent-mapping and absent-entry branches this check exists for — against synthetic files, with
# no server, no root and no `/proc` of its own.
verify_flight_server_allocator() {
  local maps="$1" environ="$2" mode="$3" lib="$4" arena="$5" tag="$6"
  local needle="${lib##*/}" line="" entry="" first_match="" saw_jemalloc=0 n_maps=0 n_env=0
  local have_preload=0 preload_value="" have_arena=0 arena_value=""
  local f
  # THE THREE-VALUED PRESENCE PROBE, for BOTH files. `-L` before `-e`, because `-e` FOLLOWS a
  # symlink and would report a dangling one as plain absence — a different cause with a
  # different remedy.
  for f in "$maps" "$environ"; do
    if [[ -L "$f" && ! -e "$f" ]] || [[ ! -e "$f" ]]; then
      echo "FATAL: $f does not exist, so the allocator state of the Flight server of $tag COULD" >&2
      echo "       NOT BE MEASURED. The usual cause is that the process exited between readiness" >&2
      echo "       and this read — see ${OUT_DIR:-<results-dir>}/$tag.server.log." >&2
      echo "       This is a refusal and never a pass: 'the mapping/entry is absent' and 'it" >&2
      echo "       could not be looked for' are different facts, and only the first one could" >&2
      echo "       ever support a verdict (#3551)." >&2
      return 1
    fi
  done
  while IFS= read -r line; do
    n_maps=$((n_maps + 1))
    if [[ -n "$needle" && -z "$first_match" && "$line" == *"$needle"* ]]; then first_match="$line"; fi
    if [[ "$line" == *jemalloc* ]]; then saw_jemalloc=1; fi
  done < "$maps"
  # NUL-SEPARATED, one WHOLE entry per iteration. The `|| [[ -n "$entry" ]]` tail is required: a
  # final entry with no trailing NUL leaves `read` non-zero with the value still set, and
  # dropping it would silently lose the last variable in the environment.
  while IFS= read -r -d '' entry || [[ -n "$entry" ]]; do
    n_env=$((n_env + 1))
    case "$entry" in
      LD_PRELOAD=*) have_preload=1; preload_value="${entry#LD_PRELOAD=}" ;;
      MALLOC_ARENA_MAX=*) have_arena=1; arena_value="${entry#MALLOC_ARENA_MAX=}" ;;
    esac
    entry=""
  done < "$environ"
  if ((n_maps == 0)); then
    echo "FATAL: $maps was readable but EMPTY, so the allocator state of the Flight server of" >&2
    echo "       $tag COULD NOT BE MEASURED. A live process always publishes its mappings, so an" >&2
    echo "       empty read is a failed measurement rather than evidence that no jemalloc" >&2
    echo "       mapping is present — the permissive reading of an unmeasurable state is the" >&2
    echo "       vacuous pass this rig refuses (#3551)." >&2
    return 1
  fi
  if ((n_env == 0)); then
    echo "FATAL: $environ was readable but EMPTY, so what the Flight server of $tag RECEIVED" >&2
    echo "       COULD NOT BE MEASURED. A live process always has a non-empty environment, so an" >&2
    echo "       empty read is a failed measurement rather than evidence that nothing was" >&2
    echo "       injected (#3551)." >&2
    return 1
  fi
  # --- THE ARENA CAP, checked for BOTH directions in every mode -----------------------------
  # It is independent of the allocator arm, so it is asserted before the mode dispatch. Not
  # requested ⇒ the entry must be ABSENT, which is meaningful because an AMBIENT `MALLOC_*` is
  # refused before the first rep: nothing else could have put it there.
  if [[ -n "$arena" ]]; then
    if ((have_arena == 0)) || [[ "$arena_value" != "$arena" ]]; then
      echo "FATAL: --flight-malloc-arena-max $arena was requested, but the Flight server process" >&2
      echo "       of $tag did not receive it: MALLOC_ARENA_MAX is" >&2
      if ((have_arena == 0)); then
        echo "       ABSENT from its environment ($n_env entries read from $environ)." >&2
      else
        echo "       '$arena_value' there, not '$arena' ($n_env entries read from $environ)." >&2
      fi
      echo "       An arena cap leaves NO mapping, so environ is the only place this is" >&2
      echo "       observable at all — without it the rep would be labelled with a cap it never" >&2
      echo "       had, which is #3217 partC F1's experiment measuring nothing (#3551)." >&2
      return 1
    fi
  elif ((have_arena == 1)); then
    echo "FATAL: no arena cap was requested, but the Flight server process of $tag received" >&2
    echo "       MALLOC_ARENA_MAX='$arena_value' ($n_env entries read from $environ)." >&2
    echo "       The driver injects it only when asked and REFUSES an ambient MALLOC_* before the" >&2
    echo "       first rep, so this rep is capped when the session says it is not — a" >&2
    echo "       configuration difference between arms that no recorded field describes (#3551)." >&2
    return 1
  fi
  case "$mode" in
    jemalloc)
      if [[ -z "$lib" ]]; then
        echo "FATAL: --flight-allocator jemalloc but no library path was passed to the allocator" >&2
        echo "       check for $tag, so there is nothing to look for. The driver resolves the" >&2
        echo "       library before the first rep; this is an internal inconsistency and it stops" >&2
        echo "       the run rather than asserting nothing (#3551)." >&2
        return 1
      fi
      # THE RECEIVED HALF. An exact VALUE comparison on the whole entry, so a preload of a
      # DIFFERENT library cannot satisfy it.
      if ((have_preload == 0)) || [[ "$preload_value" != "$lib" ]]; then
        echo "FATAL: --flight-allocator jemalloc, but the Flight server process of $tag did not" >&2
        echo "       RECEIVE the preload: its LD_PRELOAD is" >&2
        if ((have_preload == 0)); then
          echo "       ABSENT ($n_env environment entries read from $environ)." >&2
        else
          echo "       '$preload_value', not '$lib' ($n_env entries read from $environ)." >&2
        fi
        echo "       environ is what the process actually received, as opposed to what the" >&2
        echo "       driver believes it passed — which is why it is read rather than assumed." >&2
        return 1
      fi
      # THE TOOK-EFFECT HALF. environ cannot see this: glibc IGNORES an unloadable object and
      # continues with system malloc, exit 0, server healthy.
      if [[ -z "$first_match" ]]; then
        echo "FATAL: --flight-allocator jemalloc: the Flight server process of $tag RECEIVED" >&2
        echo "       LD_PRELOAD='$lib', but NO mapping of '$needle' is present in it" >&2
        echo "       ($n_maps mappings read from $maps)." >&2
        echo "       LD_PRELOAD FAILS OPEN: glibc prints \"object ... cannot be preloaded ...:" >&2
        echo "       ignored\" and CONTINUES with system malloc, exit 0, server healthy. So this" >&2
        echo "       rep would have been a byte-identical duplicate of the system arm under a" >&2
        echo "       label saying otherwise, which is worse than no rep (#3551). This is exactly" >&2
        echo "       why BOTH files are read: environ proved the injection, maps refutes the" >&2
        echo "       effect." >&2
        echo "       Check ${OUT_DIR:-<results-dir>}/$tag.server.log for that ld.so line, and" >&2
        echo "       that the library matches the binary's architecture." >&2
        return 1
      fi
      echo "jemalloc VERIFIED for $tag: RECEIVED LD_PRELOAD=$lib ($n_env environ entries) and $needle is MAPPED in the server process ($n_maps mappings) | $first_match"
      return 0 ;;
    system)
      # THE CONTROL ARM'S NEGATIVE, both files. An EMPTY `LD_PRELOAD=` entry is accepted and a
      # non-empty one is not: the launch sets the variable to EMPTY rather than trusting it to be
      # unset, and glibc preloads nothing for an empty value. So the assertion is on the VALUE,
      # affirmatively, rather than on the entry's presence.
      if ((have_preload == 1)) && [[ -n "$preload_value" ]]; then
        echo "FATAL: --flight-allocator system, but the Flight server process of $tag RECEIVED" >&2
        echo "       LD_PRELOAD='$preload_value' ($n_env entries read from $environ)." >&2
        echo "       The launch sets that variable EMPTY, and an ambient one is refused before" >&2
        echo "       the first rep, so suspect /etc/ld.so.preload or an edit to the launch line." >&2
        echo "       Refused rather than noted because the CONTROL arm running the allocator" >&2
        echo "       under test does not add noise — it INVERTS the comparison the whole session" >&2
        echo "       exists to make (#3551)." >&2
        return 1
      fi
      if ((saw_jemalloc == 1)); then
        echo "FATAL: --flight-allocator system, but the Flight server process of $tag HAS a" >&2
        echo "       jemalloc mapping ($n_maps mappings read from $maps)." >&2
        echo "       Its LD_PRELOAD is clean, so suspect /etc/ld.so.preload or a jemalloc linked" >&2
        echo "       into the binary itself — and note this is the half environ CANNOT see." >&2
        echo "       Refused rather than noted because the CONTROL arm running the allocator" >&2
        echo "       under test INVERTS the comparison (#3551)." >&2
        return 1
      fi
      echo "system VERIFIED for $tag: no non-empty LD_PRELOAD received ($n_env environ entries) and no jemalloc mapping in the server process ($n_maps mappings)"
      return 0 ;;
    *)
      echo "FATAL: the allocator check for $tag was passed the unknown mode '$mode'." >&2
      echo "       The driver's argument loop refuses every value but system|jemalloc, so this" >&2
      echo "       is an internal inconsistency. It refuses rather than picking a direction to" >&2
      echo "       assert (#3551)." >&2
      return 1 ;;
  esac
}
