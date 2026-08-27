#!/usr/bin/env bash
# #3299 harness self-test — every guard is OBSERVED TO FIRE.
#
# THE BAR (#3249/#3272). A guard that exists is worth nothing; a guard whose
# failure path no test can reach is the defect this file exists to prevent. So
# each case below feeds ONE guard the ONE input it must reject, and asserts BOTH
# the exit status AND the specific `GUARD-FAIL <CODE>` diagnostic — matching the
# code, not the prose, so the messages stay editable.
#
# It also asserts the POSITIVE controls: well-formed inputs must PASS, and the
# aligned-window attribution must compute an answer that is known exactly in
# advance. A suite in which everything fails is as broken as one in which
# nothing does.
#
# HERMETIC: no cargo, no perf, no sudo, no corpus, no network, no /sys reads.
# It runs anywhere python3 and bash exist.
set -Eeuo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GUARDS="$HERE/guards.py"
FIX="$HERE/selftest-fixtures.py"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

PASS=0; FAIL=0

# Assert: the command exits 3 (GUARD-FAIL) AND names the expected code.
expect_guard_fail() {
  local want="$1"; shift
  local out rc=0
  out="$("$@" 2>&1)" || rc=$?
  if [[ $rc -ne 3 ]]; then
    echo "FAIL  [$want] expected exit 3 (GUARD-FAIL), got $rc"; echo "      $out"; FAIL=$((FAIL+1)); return
  fi
  if ! grep -q "GUARD-FAIL $want" <<<"$out"; then
    echo "FAIL  [$want] exited 3 but did not report that code:"; echo "      $out"; FAIL=$((FAIL+1)); return
  fi
  echo "ok    [$want] fired: $(grep -o "GUARD-FAIL $want.*" <<<"$out" | head -1 | cut -c1-96)..."
  PASS=$((PASS+1))
}

expect_ok() {
  local label="$1"; shift
  local out rc=0
  out="$("$@" 2>&1)" || rc=$?
  if [[ $rc -ne 0 ]]; then
    echo "FAIL  [$label] expected exit 0, got $rc"; echo "      $out"; FAIL=$((FAIL+1)); return
  fi
  echo "ok    [$label] passed"; PASS=$((PASS+1))
}

echo "=== #3299 harness self-test — each guard observed to fire ==="
echo

# ---------------------------------------------------------------- topology ---
SIB="$TMP/siblings.map"
python3 "$FIX" siblings --path "$SIB"                       # 8 cores, (c, c+8)
SIB64="$TMP/siblings64.map"
python3 "$FIX" siblings --path "$SIB64" --cores 64 --offset 64

echo "-- cpuset --"
expect_ok "cpuset S=1 complete pair" \
  python3 "$GUARDS" cpuset --s 1 --cpus 0,8 --siblings "$SIB"
expect_ok "cpuset S=6 complete pairs" \
  python3 "$GUARDS" cpuset --s 6 --cpus 0,8,1,9,2,10,3,11,4,12,5,13 --siblings "$SIB"
# #3217's actual S=1 set on a (c, c+8) box: one thread of EACH of two different
# physical cores. A NUMA check passes it; this must not.
expect_guard_fail CPUSET_NOT_SIBLING_GROUP \
  python3 "$GUARDS" cpuset --s 1 --cpus 2,10 --siblings "$SIB64"
expect_guard_fail CPUSET_NOT_SIBLING_GROUP \
  python3 "$GUARDS" cpuset --s 6 --cpus 0,1,2,3,4,5,8,9,10,11,12,13 --siblings "$SIB64"
expect_guard_fail CPUSET_COUNT_MISMATCH \
  python3 "$GUARDS" cpuset --s 1 --cpus 0,8,1,9 --siblings "$SIB"
expect_guard_fail CPUSET_HEADROOM \
  python3 "$GUARDS" cpuset --s 7 --cpus 0,8,1,9,2,10,3,11,4,12,5,13,6,14 --siblings "$SIB"
expect_guard_fail CPUSET_UNKNOWN_CPU \
  python3 "$GUARDS" cpuset --s 1 --cpus 0,99 --siblings "$SIB"
expect_guard_fail CPUSET_MALFORMED \
  python3 "$GUARDS" cpuset --s 1 --cpus "zero,eight" --siblings "$SIB"
printf 'garbage line\n' > "$TMP/bad.map"
expect_guard_fail CPUSET_MAP_MALFORMED \
  python3 "$GUARDS" cpuset --s 1 --cpus 0,8 --siblings "$TMP/bad.map"

# --------------------------------------------------------------- perf CSV ----
echo
echo "-- perf-csv --"
python3 "$FIX" perf-csv --path "$TMP/good.csv"
expect_ok "perf-csv all events at 100.00%" \
  python3 "$GUARDS" perf-csv --csv "$TMP/good.csv"
expect_guard_fail PERF_CSV_MISSING \
  python3 "$GUARDS" perf-csv --csv "$TMP/does-not-exist.csv"
: > "$TMP/empty.csv"
expect_guard_fail PERF_CSV_MISSING \
  python3 "$GUARDS" perf-csv --csv "$TMP/empty.csv"
for case in not-counted not-supported multiplexed absent unparseable zero; do
  python3 "$FIX" perf-csv --path "$TMP/$case.csv" --case "$case"
done
expect_guard_fail PERF_EVENT_NOT_COUNTED python3 "$GUARDS" perf-csv --csv "$TMP/not-counted.csv"
expect_guard_fail PERF_EVENT_NOT_COUNTED python3 "$GUARDS" perf-csv --csv "$TMP/not-supported.csv"
expect_guard_fail PERF_MULTIPLEXED      python3 "$GUARDS" perf-csv --csv "$TMP/multiplexed.csv"
expect_guard_fail PERF_EVENT_ABSENT     python3 "$GUARDS" perf-csv --csv "$TMP/absent.csv"
expect_guard_fail PERF_EVENT_UNPARSEABLE python3 "$GUARDS" perf-csv --csv "$TMP/unparseable.csv"
expect_guard_fail PERF_EVENT_ZERO       python3 "$GUARDS" perf-csv --csv "$TMP/zero.csv"
# The #3217 silent-instrument class, closed at the INPUT boundary: an LLC event
# the Step 1 census proved dead here cannot be configured at all, so its hard 0
# can never be published as an L3 measurement.
for ev in LLC-load-misses cache-misses mem_load_retired.l3_miss longest_lat_cache.miss r4f2e; do
  expect_guard_fail PERF_FORBIDDEN_EVENT \
    python3 "$GUARDS" perf-csv --csv "$TMP/good.csv" --events "cycles,$ev"
done

# ---------------------------------------------------------- aligned window ---
echo
echo "-- aligned window --"
python3 "$FIX" window --dir "$TMP/w-good" --case good --workers 2
expect_ok "window well-formed, 2 workers span it" \
  python3 "$GUARDS" window --repdir "$TMP/w-good"

# NUMERICAL positive control: the fixture is built so the exact answer is known
# in advance (2 workers x 300,000 rows/s over a 60 s window, boundaries landing
# exactly on progress records => 36,000,000 rows and 600,000 rows/s, zero
# shortfall). This is what proves the attribution ARITHMETIC, not just that the
# guard tolerated the input.
ATTR="$(python3 "$GUARDS" window --repdir "$TMP/w-good")"
cat > "$TMP/check-attribution.py" <<'PY'
import json, sys
d = json.loads(sys.argv[1])
bad = 0
def eq(name, got, want):
    global bad
    if got != want:
        print(f"      [attribution {name}] got {got!r}, want {want!r}"); bad = 1
eq("rows_in_window_total", d["rows_in_window_total"], 36_000_000)
eq("aggregate_rows_per_s", round(d["aggregate_rows_per_s"]), 600_000)
eq("window_ns", d["window_ns"], 60_000_000_000)
eq("shortfall", d["attribution_shortfall_max_frac"], 0.0)
sys.exit(bad)
PY
if python3 "$TMP/check-attribution.py" "$ATTR"; then
  echo "ok    [attribution arithmetic] 36,000,000 rows / 60.000 s = 600,000 rows/s, shortfall 0"
  PASS=$((PASS+1))
else
  echo "FAIL  [attribution arithmetic] the aligned-window attribution computed the wrong answer"
  FAIL=$((FAIL+1))
fi

expect_guard_fail WINDOW_MISSING \
  python3 "$GUARDS" window --repdir "$TMP/no-such-rep"
python3 "$FIX" window --dir "$TMP/w-nowin" --case no-window --workers 2
expect_guard_fail WINDOW_MISSING       python3 "$GUARDS" window --repdir "$TMP/w-nowin"
python3 "$FIX" window --dir "$TMP/w-late" --case late-start --workers 2
expect_guard_fail WINDOW_NOT_SPANNED   python3 "$GUARDS" window --repdir "$TMP/w-late"
python3 "$FIX" window --dir "$TMP/w-early" --case early-stop --workers 2
expect_guard_fail WINDOW_NOT_SPANNED   python3 "$GUARDS" window --repdir "$TMP/w-early"
python3 "$FIX" window --dir "$TMP/w-short" --case shortfall --workers 2
expect_guard_fail WINDOW_SHORTFALL     python3 "$GUARDS" window --repdir "$TMP/w-short"
python3 "$FIX" window --dir "$TMP/w-zero" --case zero-rows --workers 2
expect_guard_fail WINDOW_ZERO_ROWS     python3 "$GUARDS" window --repdir "$TMP/w-zero"
python3 "$FIX" window --dir "$TMP/w-aff" --case affinity --workers 2
expect_guard_fail WINDOW_AFFINITY_MISMATCH python3 "$GUARDS" window --repdir "$TMP/w-aff"
python3 "$FIX" window --dir "$TMP/w-miss" --case missing-progress --workers 2
expect_guard_fail WINDOW_WORKER_MISSING python3 "$GUARDS" window --repdir "$TMP/w-miss"
python3 "$FIX" window --dir "$TMP/w-nosum" --case missing-summary --workers 2
expect_guard_fail WINDOW_WORKER_MISSING python3 "$GUARDS" window --repdir "$TMP/w-nosum"
python3 "$FIX" window --dir "$TMP/w-one" --case one-sample --workers 2
expect_guard_fail WINDOW_WORKER_MISSING python3 "$GUARDS" window --repdir "$TMP/w-one"
python3 "$FIX" window --dir "$TMP/w-drift" --case window-drift --workers 2
expect_guard_fail WINDOW_COUNTER_MISMATCH python3 "$GUARDS" window --repdir "$TMP/w-drift"
python3 "$FIX" window --dir "$TMP/w-span" --case bad-span --workers 2
expect_guard_fail WINDOW_SPAN          python3 "$GUARDS" window --repdir "$TMP/w-span"

# A rep claiming N=6 streams whose directory holds only 2 must not aggregate.
python3 "$FIX" window --dir "$TMP/w-n6" --case good --workers 2
python3 - "$TMP/w-n6/window.json" <<'PY'
import json, sys
p = sys.argv[1]
d = json.load(open(p)); d["n"] = 6; json.dump(d, open(p, "w"))
PY
expect_guard_fail WINDOW_WORKER_MISSING python3 "$GUARDS" window --repdir "$TMP/w-n6"

# S AND N ARE DIFFERENT DIMENSIONS. Raising only S (the CORE count) must not
# change how many streams are validated: a window with S=6, N=2 and 2 workers is
# a legitimate point (2 streams on 6 cores), not an incomplete rep. This is the
# positive half of the conflation check — if the guard read the worker count
# from `s` it would demand six workers here and fail a valid measurement.
python3 "$FIX" window --dir "$TMP/w-s6n2" --case good --workers 2
python3 - "$TMP/w-s6n2/window.json" <<'PY'
import json, sys
p = sys.argv[1]
d = json.load(open(p)); d["s"] = 6; json.dump(d, open(p, "w"))
PY
expect_ok "S=6 with N=2 is a valid point, not an incomplete rep" \
  python3 "$GUARDS" window --repdir "$TMP/w-s6n2"

# An old-schema window.json with no `n` at all is refused rather than defaulted.
python3 "$FIX" window --dir "$TMP/w-non" --case good --workers 2
python3 - "$TMP/w-non/window.json" <<'PY'
import json, sys
p = sys.argv[1]
d = json.load(open(p)); d.pop("n"); json.dump(d, open(p, "w"))
PY
expect_guard_fail WINDOW_MISSING python3 "$GUARDS" window --repdir "$TMP/w-non"

# ------------------------------------------------------ no relaxation knob ---
echo
echo "-- no escape hatch --"
# A guard with an env override is not a guard. Asserted structurally: the value
# a hostile/careless operator would reach for must not be settable from the
# environment. (Needle split so this assertion cannot match its own line.)
if grep -nE 'os''\.environ|os''\.getenv|environ''\.get|getenv''\(' "$GUARDS" | grep -q .; then
  echo "FAIL  [no-escape-hatch] guards.py reads the environment; a measurement guard"
  echo "      with an override can only buy a confident wrong number"
  FAIL=$((FAIL+1))
else
  echo "ok    [no-escape-hatch] guards.py reads no environment variable"
  PASS=$((PASS+1))
fi

echo
echo "=== self-test: $PASS passed, $FAIL failed ==="
[[ $FAIL -eq 0 ]] || exit 1
