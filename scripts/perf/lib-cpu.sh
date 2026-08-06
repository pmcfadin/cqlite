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

# THE WRAP-PROOF DECIMAL PRIMITIVES live in `lib-args.sh` and are used HERE (#3272 round 7, F2).
#
# Sourced defensively rather than assumed: this file is sourced STANDALONE by
# `scripts/tests/test_ws0_cpu_pinning_guards.sh` to drive one function at a time, so it may not
# have `lib-args.sh` in scope. Guarded on the FUNCTION rather than on a sentinel variable — the
# function is what is needed, so its presence is the affirmative measurement of the dependency
# being satisfied, and a partially-sourced library cannot look satisfied.
#
# Why the primitives are not copied here: three bash-arithmetic wraparound findings in three
# places (rounds 4 and 7) is what motivated a shared mechanism. A second copy would be a fourth
# site free to drift from the other three, which is the shape being retired.
if ! declare -F decimal_le >/dev/null 2>&1; then
  # shellcheck source=scripts/perf/lib-args.sh
  source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib-args.sh"
fi
if ! declare -F decimal_le >/dev/null 2>&1; then
  echo "FATAL: lib-cpu.sh needs decimal_le from lib-args.sh, and sourcing it did not define" >&2
  echo "       it. Without that primitive, CPU indices would be range-checked with bash" >&2
  echo "       arithmetic, which WRAPS at 64 bits: MEASURED, '9223372036854775809-0' passed" >&2
  echo "       BOTH bounds as a negative lo and drove an unbounded expansion loop (#3272 F2)." >&2
  echo "       This is a refusal rather than a fallback: a bound check that can wrap is not a" >&2
  echo "       bound check." >&2
  return 1 2>/dev/null || exit 1
fi

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

# The largest logical CPU index this rig will accept, and the largest number of CPUs one
# `--server-cpus`/`--client-cpus` list may expand to.
#
# `CPU_INDEX_MAX` is Linux's own `CONFIG_NR_CPUS` ceiling on x86_64 (8192), so no legitimate
# spec is refused; `CPU_LIST_MAX` is a size bound on the EXPANSION, because the number of
# CPUs the rig pins is a physical core's siblings (2) plus a client set (single digits) and
# a spec expanding to hundreds is a mistake, not a machine.
CPU_INDEX_MAX=8191
CPU_LIST_MAX=1024

# Validate ONE element of a CPU list — `N` or `LO-HI` — against a STRICT DECIMAL GRAMMAR.
# Prints the canonical `lo hi` on success; on failure prints a diagnostic to stderr and
# returns 1.
#
# # This is a SECURITY fix, not tidying (issue #3272 review round 4, B3)
#
# `cpu_list_expand` fed range endpoints STRAIGHT INTO BASH ARITHMETIC:
#
#     lo="${part%%-*}"; hi="${part##*-}"
#     for ((i = lo; i <= hi; i++)); do out+=("$i"); done
#
# `(( ))` EVALUATES its operands as arithmetic expressions, and bash's arithmetic evaluator
# performs COMMAND SUBSTITUTION inside an array subscript. MEASURED against the pre-fix
# function, on this box:
#
#     cpu_list_expand '1-x[$(touch /tmp/PWNED2)]'   =>   /tmp/PWNED2 created, exit 0
#
# i.e. ARBITRARY COMMAND EXECUTION from a `--server-cpus` argument. Two lesser defects on the
# same line, also measured: `cpu_list_expand '1+1'` returned the string `1+1` (an arithmetic
# expression accepted as a CPU id), and `cpu_list_expand 0-999999999` looped a billion times
# appending to an array — it did not finish in 3 seconds and would exhaust memory long before
# the measurement started.
#
# The fix is an ALLOWLIST GRAMMAR checked with a bash pattern BEFORE any arithmetic, which is
# the only posture with nothing left to enumerate: every element must match `[0-9]+` or
# `[0-9]+-[0-9]+` exactly. `+`, `*`, `$(`, `[`, a leading `-`, whitespace, an empty endpoint
# and a bare `-` are all refused by the same rule, without this function having to know that
# arithmetic expansion exists.
#
# Leading zeros are handled by `10#` on the comparisons, so `08` is 8 and not an invalid octal
# — the same defect class as the `010s` duration bug in `lib-args.sh` (#3096 review nit 7).
#
# # THE RANGE CHECK HAPPENS BEFORE ANY ARITHMETIC (#3272 review round 7, F2)
#
# The allowlist grammar above stops COMMAND SUBSTITUTION, and that half was right. What it does
# not stop is a well-formed decimal too large for signed 64-bit arithmetic, and the bound check
# was `[[ "$lo" -gt "$CPU_INDEX_MAX" ]]` AFTER `lo=$((10#$part))` — i.e. it compared the WRAPPED
# value. MEASURED on this box, against the pre-fix function:
#
#     cpu_range_validate '9223372036854775809-0'  =>  '-9223372036854775807 0', exit 0
#     cpu_range_validate '18446744073709559807'   =>  '8191 8191', exit 0
#
# The first is the dangerous one, and it defeats BOTH bounds at once. A negative `lo` is `-gt
# CPU_INDEX_MAX`? No. `hi -lt lo`? No (0 > -9.2e18). And in `cpu_list_expand` the size guard
# `hi - lo + 1 + ${#out[@]} > CPU_LIST_MAX` computes ~9.2e18, which ITSELF wraps negative — so
# the cap is passed too, and `for ((i = lo; i <= hi; i++))` then appends ~9.2e18 elements to a
# bash array. Not a crash on the arguments: an OOM in the middle of a measurement, with the
# argument that caused it having been accepted.
#
# The second is quieter and worse in kind: an index far above any real CPU accepted AS the
# in-range maximum, so the sibling check would proceed to verify pinning for cpu8191.
#
# So the ENDPOINTS ARE COMPARED AS CANONICAL DECIMAL STRINGS (`decimal_le`, no arithmetic at
# all) BEFORE `$(( ))` ever sees them. That is correct for a decimal of ANY length — there is no
# digit cap to choose, and nothing left to wrap. `10#` below then converts values already known
# to be <= 8191.
cpu_range_validate() {
  local part="$1" label="${2:-CPU list}" lo hi raw_lo raw_hi
  if [[ ! "$part" =~ ^[0-9]+(-[0-9]+)?$ ]]; then
    echo "FATAL: $label element '$part' is not a CPU index or range." >&2
    echo "       Every element must be N or LO-HI, decimal digits only (e.g. '2', '0-3')." >&2
    echo "       This is an ALLOWLIST, and it is a security boundary rather than tidiness:" >&2
    echo "       these endpoints used to enter bash arithmetic directly, and (( )) performs" >&2
    echo "       COMMAND SUBSTITUTION inside an array subscript. MEASURED on the pre-fix" >&2
    echo "       code: --server-cpus '1-x[\$(touch /tmp/PWNED)]' created the file and" >&2
    echo "       exited 0 (issue #3272 B3). An arithmetic expression like '1+1' was also" >&2
    echo "       accepted as a CPU id." >&2
    return 1
  fi
  # THE RAW DECIMAL STRINGS, extracted with parameter expansion only — no arithmetic yet.
  if [[ "$part" == *-* ]]; then
    raw_lo="${part%%-*}"; raw_hi="${part##*-}"
  else
    raw_lo="$part"; raw_hi="$part"
  fi
  # RANGE-CHECKED AS DECIMAL STRINGS, so a value of any length is compared as written rather
  # than as whatever it wraps to (#3272 F2). `decimal_le` performs no arithmetic.
  if ! decimal_le "$raw_lo" "$CPU_INDEX_MAX" || ! decimal_le "$raw_hi" "$CPU_INDEX_MAX"; then
    echo "FATAL: $label element '$part' names a CPU index above $CPU_INDEX_MAX." >&2
    echo "       That is Linux's own CONFIG_NR_CPUS ceiling on x86_64, so no real machine" >&2
    echo "       has it. MEASURED on the pre-fix code: '0-999999999' looped a billion times" >&2
    echo "       appending to a bash array — it did not finish in 3 seconds and would" >&2
    echo "       exhaust memory before the measurement started (#3272 B3)." >&2
    echo "       The comparison is on the DECIMAL STRING, before any arithmetic: bash" >&2
    echo "       arithmetic is signed 64-bit and WRAPS, so '9223372036854775809-0' used to" >&2
    echo "       become a NEGATIVE lo that passed this bound AND the expansion cap, then" >&2
    echo "       drove a ~9.2e18-iteration loop (#3272 F2)." >&2
    return 1
  fi
  # Only NOW, on values already proved <= 8191, is arithmetic safe. `10#` for the leading-zero
  # reason above.
  lo=$((10#$raw_lo)); hi=$((10#$raw_hi))
  if [[ "$hi" -lt "$lo" ]]; then
    echo "FATAL: $label element '$part' is a REVERSED range ($lo > $hi)." >&2
    echo "       A reversed range expanded to NOTHING and was silently dropped, so" >&2
    echo "       '--server-cpus 10-2' pinned to an empty set and the sibling check" >&2
    echo "       complained about the wrong thing (#3272 B3). State it as $hi-$lo." >&2
    return 1
  fi
  printf '%s %s\n' "$lo" "$hi"
}

# Expand a Linux CPU list ("0-3,8", "2,10") into one sorted, space-separated list.
#
# FAIL-CLOSED on a malformed spec (issue #3272 review round 4, B3): every element goes
# through `cpu_range_validate` BEFORE any arithmetic, and the total expansion is capped. A
# rejected spec returns 1 with the diagnostic on stderr rather than an empty or partial list,
# because an empty list reaches `verify_sibling_pair` as "CPU list is empty" — naming the
# wrong cause for what is really a refused argument.
#
# Two properties that are easy to lose in an edit (issue #3096 review):
#
#  * `_parts` is declared `local`. It used to leak into the SOURCING shell, where
#    a caller's own `_parts` would be silently clobbered by any pinning check.
#  * an EMPTY expansion returns empty rather than expanding an empty array. Under
#    `set -u` on bash < 4.4, `"${out[@]}"` on an empty array is an unbound-variable
#    error — so an empty/garbage CPU spec died here with a shell diagnostic instead
#    of reaching `verify_sibling_pair`'s fail-closed "CPU list is empty" message.
#
# # AN EMPTY ELEMENT IS NOW REFUSED, NOT SKIPPED (#3272 review round 11, F4)
#
# `2,,10` and `,` used to be SKIPPED element by element, so `2,,10` expanded to `2 10` and a
# spec that was nothing but separators expanded to NOTHING and returned SUCCESS. Skipping is
# the wrong disposition for the same reason a reversed range was: the operator wrote something
# this rig cannot act on, and quietly acting on a DIFFERENT set than was written is how a
# measurement ends up describing something nobody asked for. It is refused HERE, where the
# element is visible, rather than downstream where the diagnostic can only be about the whole
# list.
#
# The TOTALLY EMPTY spec (`""`) is deliberately still an empty SUCCESSFUL expansion: bash splits
# it into ZERO elements, so there is no empty element to name, and the callers
# (`verify_sibling_pair`, `verify_disjoint`) are the layer that knows an empty SET is fatal —
# each now says so in its own terms (that is the other half of F4).
#
# # A TRAILING COMMA IS AN EMPTY ELEMENT TOO (#3272 review round 16, L1)
#
# The check above was right; the PARSER silently discarded the case it was written for. `IFS=','
# read -r -a` DROPS a trailing empty field, so `2,10,` split into exactly two elements — `2` and
# `10`, both non-empty — and sailed past the emptiness test that `2,,10` and `,2,10` hit. The
# operator wrote a spec this rig cannot act on and got a measurement of a set they did not write,
# which is the whole of what F4 exists to prevent; F4 just never saw it.
#
# It is fixed at the PARSE, not by a second anchored `^,|,$` grammar check, so the ONE diagnostic
# keeps naming the offending POSITION: a trailing comma is refused as "the empty element at
# position 3", the same sentence `2,,10` gets, rather than a separate message about commas that
# would have to be kept in agreement with this one. A `,#` sentinel is appended before the split
# and its element dropped afterward, which makes every trailing field a REAL element.
cpu_list_expand() {
  local spec="$1" label="${2:-CPU list}" part bounds lo hi i idx=0
  local -a out=() _parts=()
  # An empty spec stays ZERO elements (the sentinel would otherwise invent one, turning `""` into
  # a position-1 empty-element refusal and changing the callers' "CPU list is empty" diagnostic).
  if [[ -n "$spec" ]]; then
    IFS=',' read -r -a _parts <<<"$spec,#"
    unset "_parts[$((${#_parts[@]} - 1))]"
  fi
  for part in ${_parts[@]+"${_parts[@]}"}; do
    idx=$((idx + 1))
    # An EMPTY element is a REFUSAL (#3272 F4). Pre-fix it was `continue`, so `2,,10` silently
    # measured `2 10` and `,`/`,,` expanded to nothing and returned 0. Round 16's L1 made a
    # TRAILING empty field reach here at all — bash's split had been dropping it.
    if [[ -z "$part" ]]; then
      echo "FATAL: $label '$spec' has an EMPTY element (position $idx)." >&2
      echo "       An empty element used to be SKIPPED, so '2,,10' silently expanded to" >&2
      echo "       '2 10' and a spec of nothing but separators expanded to NOTHING and" >&2
      echo "       returned SUCCESS — pinning a set the operator did not write, or none at" >&2
      echo "       all (#3272 F4). State the list without the empty element." >&2
      return 1
    fi
    bounds="$(cpu_range_validate "$part" "$label")" || return 1
    lo="${bounds%% *}"; hi="${bounds##* }"
    if (( hi - lo + 1 + ${#out[@]} > CPU_LIST_MAX )); then
      echo "FATAL: $label '$spec' expands to more than $CPU_LIST_MAX CPUs." >&2
      echo "       This rig pins a physical core's siblings (2) plus a client set; a spec" >&2
      echo "       this large is a mistake, and expanding it before saying so is how a" >&2
      echo "       measurement run dies on memory instead of on its arguments (#3272 B3)." >&2
      return 1
    fi
    for ((i = lo; i <= hi; i++)); do out+=("$i"); done
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
  # The failure is PROPAGATED (#3272 review round 4, B3). `cpu_list_expand` can now REFUSE a
  # malformed list, and sysfs is a file — a truncated or garbage `thread_siblings_list` must
  # fail the sibling verification rather than becoming an empty `got` that compares unequal and
  # produces a "not the sibling set" diagnostic for what is really an unreadable topology.
  cpu_list_expand "$(cat "$f")" "$f" || return 1
}

# FAIL CLOSED unless `$1` is exactly the sibling set of ONE physical core.
#
# Checked from BOTH ends: the requested set must equal cpuA's sibling list AND
# every member must report that same list. A one-sided check would accept a set
# that happens to contain a core's siblings plus a stray CPU.
verify_sibling_pair() {
  local spec="$1" label="${2:-pinned}" want got cpu
  # A REFUSED spec fails HERE, with `cpu_range_validate`'s own diagnostic already on stderr
  # (#3272 B3) — never as an empty `want` reported as "CPU list is empty", which named the
  # wrong cause for a malformed argument.
  want="$(cpu_list_expand "$spec" "$label CPUs")" || return 1
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
    # A malformed sysfs entry is SKIPPED here rather than fatal: this function prints an
    # informational MENU (`--help`'s "pairs on this box"), so one unreadable entry should not
    # stop the help text. The load-bearing path is `verify_sibling_pair`, which fails closed.
    s="$(cpu_list_expand "$(cat "$f")" "$f")" || continue
    if [[ ! " ${seen[*]-} " == *" $s "* ]]; then
      seen+=("$s")
      echo "  ${s// /,}"
    fi
  done
}

# The two pinned sets must not overlap: a client sharing a physical core with the
# server would land the client's own CPU cost inside the server's `perf -C` window.
#
# # AN EMPTY EXPANDED SET IS REFUSED HERE (#3272 review round 11, F4)
#
# The B3 comment below was right that a MALFORMED spec must propagate — and it does. What it did
# not cover is the spec that is legitimately parseable and expands to NOTHING: `--client-cpus ''`
# is split by bash into ZERO elements, so `cpu_list_expand` returns SUCCESS with empty output,
# and the two nested loops below then iterate over nothing and this function returns 0.
#
# `verify_sibling_pair` already refuses an empty SERVER set ("CPU list is empty"), but the CLIENT
# set is deliberately NOT sibling-checked (a multi-core client set is legitimate), so nothing in
# the topology stage looked at it at all. MEASURED consequence: `--client-cpus ''` passed the
# whole topology stage and failed later at `taskset -c '' …` inside `measure_flight` — i.e. AFTER
# `relax_perf_sysctls` mutated the host's perf hardening and AFTER a full `cargo build --release`.
# An argument-shaped mistake must cost an argument-shaped failure.
#
# Refused for BOTH sets rather than the client alone: this function is the one place both
# expansions exist side by side, and a guard that covers one operand is the partial fix this
# issue keeps finding.
verify_disjoint() {
  local a b x y
  # Both specs re-validated and the refusal PROPAGATED (#3272 B3): a spec that expands to
  # nothing trivially satisfies disjointness, so a silent empty here would turn a malformed
  # `--client-cpus` into a PASS.
  a="$(cpu_list_expand "$1" "server CPUs")" || return 1
  b="$(cpu_list_expand "$2" "client CPUs")" || return 1
  # ...and an EMPTY expansion is refused, naming WHICH set (#3272 F4). Two separate checks, not
  # one combined test, because the remedy differs per flag and a diagnostic that says "one of
  # them" makes the operator guess.
  if [[ -z "$a" ]]; then
    echo "FATAL: server CPUs ('$1') expand to an EMPTY set." >&2
    echo "       An empty set trivially satisfies disjointness, so this used to PASS and fail" >&2
    echo "       later at taskset — after the host sysctls were weakened and after a full" >&2
    echo "       release build (#3272 F4). Pass --server-cpus as one physical core's" >&2
    echo "       siblings (see the pairs listed by --help)." >&2
    return 1
  fi
  if [[ -z "$b" ]]; then
    echo "FATAL: client CPUs ('$2') expand to an EMPTY set." >&2
    echo "       The client set is deliberately NOT sibling-checked (a multi-core client set is" >&2
    echo "       legitimate), so this function is the only place it is examined at all. An" >&2
    echo "       empty set trivially satisfies disjointness, so '--client-cpus \"\"' used to" >&2
    echo "       pass the whole topology stage and fail later at taskset — after the host" >&2
    echo "       sysctls were weakened and after a full release build (#3272 F4)." >&2
    return 1
  fi
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
