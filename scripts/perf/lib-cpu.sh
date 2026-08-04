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

set -euo pipefail

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
cpu_siblings_of() {
  local cpu="$1" f="/sys/devices/system/cpu/cpu$1/topology/thread_siblings_list"
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
  for f in /sys/devices/system/cpu/cpu[0-9]*/topology/thread_siblings_list; do
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
        echo "       own cost would be counted inside the server's perf -C window." >&2
        return 1
      fi
    done
  done
}
