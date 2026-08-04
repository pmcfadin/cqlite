#!/usr/bin/env bash
# CPU-topology helpers for the issue #3096 measurement rig.
#
# The single job here is to make the `taskset` pinning VERIFIED rather than
# assumed. "CPU 2 and CPU 10 are the two hyperthreads of one physical core" is
# true on this box and false on plenty of others; a rig that assumes it silently
# measures two DIFFERENT physical cores and reports a per-core figure that is
# nothing of the kind. So the pair is read from
# `/sys/devices/system/cpu/cpu*/topology/thread_siblings_list` and the run FAILS
# CLOSED when the requested set is not exactly one physical core's siblings.
#
# Sourced, not executed.
#
# It sets NO shell options (#3272 review). `set -euo pipefail` here mutated the
# SOURCING shell's options — a library silently changing its caller's error handling,
# which is the caller's decision and not a detail a `source` line advertises. The
# driver sets all three itself; a future non-driver caller (a test sourcing this to
# drive one function) gets to keep whatever options it chose.

# --- the sysfs topology root, INJECTABLE for testing only (issue #3272, item 10) --
#
# The sibling check below is the load-bearing guarantee of the whole
# same-session both-arm methodology: if the two pinned CPUs are NOT one physical
# core's hyperthreads, every per-core figure the rig prints is a figure of
# something else. A guarantee that has never been OBSERVED to reject a
# non-sibling set is not evidence it would (#3249), and it could not be observed
# because the path was a hardcoded `/sys/...` literal — untestable without a
# particular CPU layout and root.
#
# So the root is a variable. Two properties keep that from becoming the bypass:
#
#  * It is READ ONCE here from `CQLITE_WS0_CPU_TOPOLOGY_ROOT`, and any non-default
#    value is ANNOUNCED ON STDERR every single time — a shimmed run cannot be
#    quiet about it.
#  * `assert_real_cpu_topology` (called by the driver BEFORE any measurement)
#    FAILS CLOSED when the override is set, so it can only ever be used by a test
#    that sources this library directly. A measurement run that tried to point
#    the sibling check at a fake tree stops before it measures anything.
CPU_TOPOLOGY_ROOT="${CQLITE_WS0_CPU_TOPOLOGY_ROOT:-/sys/devices/system/cpu}"
if [[ "$CPU_TOPOLOGY_ROOT" != "/sys/devices/system/cpu" ]]; then
  echo "NOTE: CPU topology root OVERRIDDEN to '$CPU_TOPOLOGY_ROOT' (test-only; a" >&2
  echo "      measurement run refuses this — see assert_real_cpu_topology)." >&2
fi

# FAIL CLOSED unless the sibling check is reading the REAL host topology.
#
# The driver calls this before it verifies any pinning. Without it, the override
# above would be a way to satisfy the pinning guarantee with a fabricated
# `thread_siblings_list` — i.e. the guard added for #3096 trap 2 could be
# bypassed by an env var, which is precisely the "a fix moved the problem" shape
# #3272 exists to close.
assert_real_cpu_topology() {
  if [[ -n "${CQLITE_WS0_CPU_TOPOLOGY_ROOT:-}" ]]; then
    echo "FATAL: CQLITE_WS0_CPU_TOPOLOGY_ROOT is set ('$CQLITE_WS0_CPU_TOPOLOGY_ROOT')." >&2
    echo "       That override exists ONLY so scripts/tests/ can prove the sibling check" >&2
    echo "       rejects a non-sibling set. A measurement run must read the REAL host" >&2
    echo "       topology: verifying the pinning against a fabricated sysfs tree would" >&2
    echo "       make every per-core figure this rig prints unverified (issue #3272)." >&2
    echo "       Unset it and re-run." >&2
    return 1
  fi
  if [[ ! -d "$CPU_TOPOLOGY_ROOT" ]]; then
    echo "FATAL: $CPU_TOPOLOGY_ROOT does not exist — the sibling check cannot read the" >&2
    echo "       host's CPU topology, so the pinning cannot be VERIFIED (issue #3096" >&2
    echo "       spec R2). This rig runs on Linux; there is no fallback, because an" >&2
    echo "       assumed sibling pair is the defect the check exists to prevent." >&2
    return 1
  fi
}

# Expand a Linux CPU list ("0-3,8", "2,10") into one sorted, space-separated list.
#
# Two properties that are easy to lose in an edit (issue #3096 review):
#
#  * `_parts` is declared `local`. It used to leak into the SOURCING shell, where
#    a caller's own `_parts` would be silently clobbered by any pinning check.
#  * an EMPTY expansion returns empty rather than expanding an empty array. Under
#    `set -u` on bash < 4.4, `"${out[@]}"` on an empty array is an unbound-variable
#    error — so an empty/garbage CPU spec died here with a shell diagnostic instead
#    of reaching `verify_sibling_pair`'s fail-closed "CPU list is empty" message.
cpu_list_expand() {
  local spec="$1" part lo hi i
  local -a out=() _parts=()
  IFS=',' read -r -a _parts <<<"$spec"
  for part in "${_parts[@]}"; do
    if [[ "$part" == *-* ]]; then
      lo="${part%%-*}"; hi="${part##*-}"
      for ((i = lo; i <= hi; i++)); do out+=("$i"); done
    elif [[ -n "$part" ]]; then
      out+=("$part")
    fi
  done
  if ((${#out[@]} == 0)); then
    return 0
  fi
  printf '%s\n' "${out[@]}" | sort -n -u | tr '\n' ' ' | sed 's/ $//'
}

# The sorted sibling list of one logical CPU, read from sysfs.
#
# An unreadable file is a FAILURE, never an empty answer: `verify_sibling_pair`
# compares `got` against `want`, and an empty `got` from a silent read failure
# would compare unequal and produce a confusing "not the sibling set" diagnostic
# for what is really "the topology could not be read".
cpu_siblings_of() {
  local cpu="$1" f="$CPU_TOPOLOGY_ROOT/cpu$1/topology/thread_siblings_list"
  [[ -r "$f" ]] || { echo "FATAL: $f is unreadable — cannot VERIFY that cpu$cpu's pinning is a physical core" >&2; return 1; }
  cpu_list_expand "$(cat "$f")"
}

# FAIL CLOSED unless `$1` is exactly the sibling set of ONE physical core.
#
# Checked from BOTH ends: the requested set must equal cpuA's sibling list AND
# every member must report that same list. A one-sided check would accept a set
# that happens to contain a core's siblings plus a stray CPU.
verify_sibling_pair() {
  local spec="$1" label="${2:-pinned}" want got cpu
  want="$(cpu_list_expand "$spec")"
  [[ -n "$want" ]] || { echo "FATAL: $label CPU list is empty" >&2; return 1; }
  for cpu in $want; do
    got="$(cpu_siblings_of "$cpu")" || return 1
    if [[ "$got" != "$want" ]]; then
      echo "FATAL: $label CPU set '$spec' (expanded: $want) is NOT the sibling set of one" >&2
      echo "       physical core. cpu$cpu's thread_siblings_list is '$got'." >&2
      echo "       Pinning to two different physical cores would make every per-core" >&2
      echo "       figure this rig prints wrong, so the run stops here (spec R2)." >&2
      return 1
    fi
  done
  echo "$label CPUs: $spec -> verified siblings of one physical core ($want)"
}

# Print every physical core's sibling pair — the menu a caller picks from.
list_sibling_pairs() {
  local f seen=() s
  for f in "$CPU_TOPOLOGY_ROOT"/cpu[0-9]*/topology/thread_siblings_list; do
    [[ -r "$f" ]] || continue
    s="$(cpu_list_expand "$(cat "$f")")"
    if [[ ! " ${seen[*]-} " == *" $s "* ]]; then
      seen+=("$s")
      echo "  ${s// /,}"
    fi
  done
}

# The two pinned sets must not overlap: a client sharing a physical core with the
# server would land the client's own CPU cost inside the server's `perf -C` window.
verify_disjoint() {
  local a b x y
  a="$(cpu_list_expand "$1")"; b="$(cpu_list_expand "$2")"
  for x in $a; do
    for y in $b; do
      if [[ "$x" == "$y" ]]; then
        echo "FATAL: server CPUs ($1) and client CPUs ($2) overlap on cpu$x — the client's" >&2
        echo "       own cost would be counted inside the server's perf -C window." >&2  # perf-lint-allow: a diagnostic STRING
        return 1
      fi
    done
  done
}
