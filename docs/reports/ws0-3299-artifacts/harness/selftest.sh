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

# --- FAIL-OPEN regression: a check must not skip itself and return success ---
# Each field below used to be OPTIONAL in guard_window: omitting it skipped the
# check it feeds and the rep still PASSED. That is this issue's recurring shape —
# a check reporting success having measured nothing — inside the guard layer
# itself. Each absence must now be refused.
echo
echo "-- required window fields (fail-open regression) --"
for c in no-worker-cpus no-perf-csv no-perf-cpus; do
  python3 "$FIX" window --dir "$TMP/w-$c" --case "$c" --workers 2
  expect_guard_fail WINDOW_FIELD_MISSING python3 "$GUARDS" window --repdir "$TMP/w-$c"
done
# An EMPTY perf_cpus is falsy, so the required-field check catches it first.
python3 "$FIX" window --dir "$TMP/w-empty-perf-cpus" --case empty-perf-cpus --workers 2
expect_guard_fail WINDOW_FIELD_MISSING python3 "$GUARDS" window --repdir "$TMP/w-empty-perf-cpus"
# A perf_cpus of "," is TRUTHY but names no CPU — it passes the presence check
# and would divide by zero in the counter-window comparison. This is what keeps
# the ncpus==0 branch reachable rather than dead code.
for c in degenerate-perf-cpus short-worker-cpus; do
  python3 "$FIX" window --dir "$TMP/w-$c" --case "$c" --workers 2
  expect_guard_fail WINDOW_FIELD_MALFORMED python3 "$GUARDS" window --repdir "$TMP/w-$c"
done
python3 "$FIX" window --dir "$TMP/w-notc" --case no-task-clock --workers 2
expect_guard_fail WINDOW_NO_TASK_CLOCK python3 "$GUARDS" window --repdir "$TMP/w-notc"

# --- THE COUNTED CPUs MUST BE THE WORKED CPUs ------------------------------
# The guard counted `perf_cpus` and never compared it with the set the workers
# actually ran on, so a SUBSTITUTED CPU was invisible: cardinality is unchanged,
# `task-clock` still reads window x ncpus (it accrues on idle CPUs too), and the
# rep passed with an unrelated CPU's counters attributed to worker rows. Both
# directions of the divergence must fire, and the identity must PASS on the
# well-formed fixture — which is also the shape every committed window.json has.
echo
echo "-- counted CPUs == worked CPUs --"
expect_ok "counted CPU set IS the worked CPU set" \
  python3 "$GUARDS" window --repdir "$TMP/w-good"
python3 "$FIX" window --dir "$TMP/w-uncounted" --case uncounted-worker-cpu --workers 2
expect_guard_fail WINDOW_CPU_SET_MISMATCH python3 "$GUARDS" window --repdir "$TMP/w-uncounted"
python3 "$FIX" window --dir "$TMP/w-idlecpu" --case idle-counted-cpu --workers 2
expect_guard_fail WINDOW_CPU_SET_MISMATCH python3 "$GUARDS" window --repdir "$TMP/w-idlecpu"
python3 "$FIX" window --dir "$TMP/w-dupcpu" --case duplicate-perf-cpu --workers 2
expect_guard_fail WINDOW_FIELD_MALFORMED python3 "$GUARDS" window --repdir "$TMP/w-dupcpu"
python3 "$FIX" window --dir "$TMP/w-nonintcpu" --case noninteger-perf-cpu --workers 2
expect_guard_fail WINDOW_FIELD_MALFORMED python3 "$GUARDS" window --repdir "$TMP/w-nonintcpu"

# ------------------------------------ phase 2: the Flight do_get step record ---
echo
echo "-- flight-step (phase 2) --"
python3 "$FIX" flight-step --path "$TMP/fl-good.jsonl"
expect_ok "flight-step well-formed" \
  python3 "$GUARDS" flight-step --jsonl "$TMP/fl-good.jsonl"
expect_guard_fail FLIGHT_RECORD_MISSING \
  python3 "$GUARDS" flight-step --jsonl "$TMP/fl-nope.jsonl"
for c in empty no-step; do python3 "$FIX" flight-step --path "$TMP/fl-$c.jsonl" --case "$c"; done
expect_guard_fail FLIGHT_RECORD_MISSING python3 "$GUARDS" flight-step --jsonl "$TMP/fl-empty.jsonl"
expect_guard_fail FLIGHT_RECORD_MISSING python3 "$GUARDS" flight-step --jsonl "$TMP/fl-no-step.jsonl"
# THE one this guard exists for: a zero-row do_get presents as a very FAST one,
# because a server answering NotFound completes every request immediately.
python3 "$FIX" flight-step --path "$TMP/fl-zero.jsonl" --case zero-rows
expect_guard_fail FLIGHT_ZERO_ROWS python3 "$GUARDS" flight-step --jsonl "$TMP/fl-zero.jsonl"
for c in errors unavailable no-ok; do
  python3 "$FIX" flight-step --path "$TMP/fl-$c.jsonl" --case "$c"
  expect_guard_fail FLIGHT_REQUEST_ERRORS python3 "$GUARDS" flight-step --jsonl "$TMP/fl-$c.jsonl"
done
python3 "$FIX" flight-step --path "$TMP/fl-two.jsonl" --case two-steps
expect_guard_fail FLIGHT_STEP_COUNT python3 "$GUARDS" flight-step --jsonl "$TMP/fl-two.jsonl"

# --- THE EVIDENCE IS REQUIRED, NOT OPTIONAL --------------------------------
# `requests_ok`, `requests_error`, `requests_unavailable` and `rows_per_s` were
# all conditional, so a record carrying a positive `rows_total` and nothing else
# passed as a measurement: no success accounting, no error accounting, no
# throughput. Absence of an error count is not evidence of no errors.
for c in no-requests-ok no-requests-error no-requests-unavailable no-rows-per-s; do
  python3 "$FIX" flight-step --path "$TMP/fl-$c.jsonl" --case "$c"
  expect_guard_fail FLIGHT_FIELD_MISSING python3 "$GUARDS" flight-step --jsonl "$TMP/fl-$c.jsonl"
done
# Present but not a number: refused as malformed rather than coerced.
python3 "$FIX" flight-step --path "$TMP/fl-bad-rate.jsonl" --case bad-rate
expect_guard_fail FLIGHT_FIELD_MALFORMED python3 "$GUARDS" flight-step --jsonl "$TMP/fl-bad-rate.jsonl"
# Positive rows at a zero rate is a record inconsistent with itself.
python3 "$FIX" flight-step --path "$TMP/fl-zero-rate.jsonl" --case zero-rate
expect_guard_fail FLIGHT_ZERO_ROWS python3 "$GUARDS" flight-step --jsonl "$TMP/fl-zero-rate.jsonl"
# POSITIVE control at the REAL committed shape: the exact `flight-loadgen.step/v1`
# key set of ../phase2-run/doget-s1-r1.jsonl must still pass.
python3 "$FIX" flight-step --path "$TMP/fl-real.jsonl" --case real-shape
expect_ok "flight-step accepts the real committed step/v1 record" \
  python3 "$GUARDS" flight-step --jsonl "$TMP/fl-real.jsonl"

# --------------------------------------------------------- corpus identity ---
echo
echo "-- corpus identity --"
# THE DEFECT THIS SECTION EXISTS FOR: sweep.sh used to resolve the corpus with
# `find "$CORPUS" -name '*-Data.db' -print -quit` — the first arbitrary match
# ANYWHERE under the root — so it could verify one file while the worker scanned
# another. `guards.py corpus` resolves the ONE path scan-worker opens.
#
# HOW THIS STAYS HERMETIC. guards.py reads the pinned digest from the committed
# Rust constant, resolved relative to ITS OWN location. A copy placed at the same
# relative depth inside a temp tree therefore reads a temp PIN, which makes every
# branch — the digest comparison included — observable without a 2.6 GB corpus.
FR="$TMP/fakerepo"
FG="$FR/docs/reports/ws0-3299-artifacts/harness/guards.py"
mkdir -p "$FR/tools/ws0-corpus-gen/src" "$(dirname "$FG")"
cp "$GUARDS" "$FG"

# The stand-in corpus carries the SAME COMPONENT SHAPE as the real one — the
# eight #3096 components, plus the schema emitted beside them — because the guard
# now verifies EVERY component and the schema, not just the Data.db.
CORP="$TMP/corpus"
mkdir -p "$CORP/ws0/events"
for c in Data.db Index.db Statistics.db Summary.db Filter.db CRC.db TOC.txt Digest.crc32; do
  printf 'pinned measurement corpus stand-in: %s\n' "$c" > "$CORP/ws0/events/nb-1-big-$c"
done
printf 'CREATE TABLE ws0.events (...);\n' > "$CORP/ws0-events.cql"
PIN_SHA="$(sha256sum "$CORP/ws0/events/nb-1-big-Data.db" | cut -d' ' -f1)"
PIN_BYTES="$(stat -c %s "$CORP/ws0/events/nb-1-big-Data.db")"

# Plant BOTH oracles at the depth the guard resolves them from: the Rust pin
# (quantities) and the #3096 identity artifact (the per-component map). They are
# derived from the SAME stand-in files, so the fake repo is self-consistent and
# every refusal below is reachable without the real 2.6 GB corpus.
plant_pin() {  # <pin-root> [<corpus-dir>]
  mkdir -p "$1/tools/ws0-corpus-gen/src" "$1/docs/reports/ws0-3096-artifacts"
  [[ $# -ge 2 ]] || return 0
  python3 "$TMP/plant-pin.py" "$1" "$2"
}
cat > "$TMP/plant-pin.py" <<'PLANT'
"""Write a self-consistent Rust pin + #3096 component map for a stand-in corpus."""
import hashlib, json, os, sys
root, corp = sys.argv[1], sys.argv[2]
tbl = os.path.join(corp, "ws0", "events")
comps, total = {}, 0
for name in sorted(os.listdir(tbl)):
    b = open(os.path.join(tbl, name), "rb").read()
    comps[name] = {"name": name, "bytes": len(b), "sha256": hashlib.sha256(b).hexdigest()}
    total += len(b)
data = next(n for n in comps if n.endswith("-Data.db"))
schema = hashlib.sha256(open(os.path.join(corp, "ws0-events.cql"), "rb").read()).hexdigest()
with open(os.path.join(root, "tools/ws0-corpus-gen/src/measurement_corpus.rs"), "w") as fh:
    fh.write(f'pub const DATA_DB_BYTES: u64 = {comps[data]["bytes"]};\n')
    fh.write(f'pub const DATA_DB_SHA256: &str = "{comps[data]["sha256"]}";\n')
    fh.write(f'pub const SCHEMA_SHA256: &str = "{schema}";\n')
    fh.write(f"pub const TOTAL_COMPONENT_BYTES: u64 = {total};\n")
with open(os.path.join(root, "docs/reports/ws0-3096-artifacts/corpus-identity.json"), "w") as fh:
    json.dump({"components": comps, "compression_info_present": False}, fh)
PLANT
plant_pin "$FR" "$CORP"

# POSITIVE control: the pinned bytes, at the exact path the worker opens.
expect_ok "corpus identity: pinned Data.db at <corpus>/ws0/events" \
  python3 "$FG" corpus --corpus "$CORP"

# THE PRE-FIX CASE. The decoy is BYTE-IDENTICAL to the pin but lives elsewhere
# under the root, and `<corpus>/ws0/events` does not exist — so the old
# `find -print -quit` check PASSED this tree while the worker had nothing to
# scan. It must now refuse.
DEC="$TMP/corpus-decoy"
mkdir -p "$DEC/backup/2026-08/ks/tbl"
cp "$CORP/ws0/events/nb-1-big-Data.db" "$DEC/backup/2026-08/ks/tbl/nb-1-big-Data.db"
expect_guard_fail CORPUS_DATA_DB_ABSENT \
  python3 "$FG" corpus --corpus "$DEC"
# Same shape one level in: the right table dir exists but is empty, and a
# pin-identical decoy sits beside it.
DEC2="$TMP/corpus-decoy2"
mkdir -p "$DEC2/ws0/events" "$DEC2/ws0/events_old"
cp "$CORP/ws0/events/nb-1-big-Data.db" "$DEC2/ws0/events_old/nb-1-big-Data.db"
expect_guard_fail CORPUS_DATA_DB_ABSENT \
  python3 "$FG" corpus --corpus "$DEC2"

# Two Data.db in the scanned directory: the scan reads BOTH, so one pinned
# identity cannot describe the input.
AMB="$TMP/corpus-ambiguous"
cp -a "$CORP" "$AMB"
cp "$AMB/ws0/events/nb-1-big-Data.db" "$AMB/ws0/events/nb-2-big-Data.db"
expect_guard_fail CORPUS_DATA_DB_AMBIGUOUS \
  python3 "$FG" corpus --corpus "$AMB"

# A compressed corpus is a DIFFERENT corpus (#3096 Corpus B is uncompressed).
CMP="$TMP/corpus-compressed"
cp -a "$CORP" "$CMP"
: > "$CMP/ws0/events/nb-1-big-CompressionInfo.db"
expect_guard_fail CORPUS_COMPRESSED \
  python3 "$FG" corpus --corpus "$CMP"

BAD="$TMP/corpus-bytes"
cp -a "$CORP" "$BAD"
printf 'x' >> "$BAD/ws0/events/nb-1-big-Data.db"
expect_guard_fail CORPUS_BYTES_MISMATCH \
  python3 "$FG" corpus --corpus "$BAD"

# THE CASE A BYTE COUNT CANNOT SEE: same size, different bytes. This is why the
# guard digests the file instead of stat-ing it.
SHA="$TMP/corpus-sha"
cp -a "$CORP" "$SHA"
printf 'PINNED measurement corpus stand-in: Data.db\n' > "$SHA/ws0/events/nb-1-big-Data.db"
[[ "$(stat -c %s "$SHA/ws0/events/nb-1-big-Data.db")" == "$PIN_BYTES" ]] \
  || { echo "FAIL  [corpus sha fixture] same-size fixture is not the same size"; FAIL=$((FAIL+1)); }
expect_guard_fail CORPUS_SHA_MISMATCH \
  python3 "$FG" corpus --corpus "$SHA"

# --- THE WHOLE CORPUS, NOT JUST THE Data.db --------------------------------
# The scan also consumes the SCHEMA and the auxiliary components (Index.db,
# Statistics.db, Summary.db, Filter.db, ...), all of which change scan
# BEHAVIOUR. Hashing Data.db alone certified a corpus with a modified sidecar or
# a modified schema as canonical. Every one of those divergences must now fire.
CMISS="$TMP/corpus-missing-component"
cp -a "$CORP" "$CMISS"
rm "$CMISS/ws0/events/nb-1-big-Summary.db"
expect_guard_fail CORPUS_COMPONENT_MISSING \
  python3 "$FG" corpus --corpus "$CMISS"

CEXTRA="$TMP/corpus-extra-component"
cp -a "$CORP" "$CEXTRA"
printf 'not a canonical component\n' > "$CEXTRA/ws0/events/nb-1-big-Rows.db"
expect_guard_fail CORPUS_COMPONENT_EXTRA \
  python3 "$FG" corpus --corpus "$CEXTRA"

# A SIDECAR of the right length whose bytes differ: the case a component count,
# a byte total, or a Data.db-only digest all miss.
CSHA="$TMP/corpus-sidecar-sha"
cp -a "$CORP" "$CSHA"
printf 'PINNED measurement corpus stand-in: Index.db\n' > "$CSHA/ws0/events/nb-1-big-Index.db"
[[ "$(stat -c %s "$CSHA/ws0/events/nb-1-big-Index.db")" \
   == "$(stat -c %s "$CORP/ws0/events/nb-1-big-Index.db")" ]] \
  || { echo "FAIL  [sidecar sha fixture] same-size fixture is not the same size"; FAIL=$((FAIL+1)); }
expect_guard_fail CORPUS_COMPONENT_SHA_MISMATCH \
  python3 "$FG" corpus --corpus "$CSHA"

CBYTES="$TMP/corpus-sidecar-bytes"
cp -a "$CORP" "$CBYTES"
printf 'x' >> "$CBYTES/ws0/events/nb-1-big-Statistics.db"
expect_guard_fail CORPUS_COMPONENT_BYTES_MISMATCH \
  python3 "$FG" corpus --corpus "$CBYTES"

# THE SCHEMA. scan-worker defaults --schema to <corpus>/ws0-events.cql and builds
# its table metadata from it, so a modified schema decodes the same bytes
# differently — an unhashed schema is an unverified corpus.
CNOSCH="$TMP/corpus-no-schema"
cp -a "$CORP" "$CNOSCH"
rm "$CNOSCH/ws0-events.cql"
expect_guard_fail CORPUS_SCHEMA_ABSENT \
  python3 "$FG" corpus --corpus "$CNOSCH"

CSCH="$TMP/corpus-schema-edited"
cp -a "$CORP" "$CSCH"
printf 'CREATE TABLE ws0.events (,,,);\n' > "$CSCH/ws0-events.cql"
expect_guard_fail CORPUS_SCHEMA_MISMATCH \
  python3 "$FG" corpus --corpus "$CSCH"

# THE COMPONENT MAP IS ITSELF AN ORACLE, so it is corroborated before use: an
# unreadable one REFUSES (it never degrades to "Data.db only"), and one that
# disagrees with the independently-parsed Rust pin is not usable as the
# expectation — a swapped or edited artifact must not silently BECOME canonical.
FR3="$TMP/fakerepo-nomap"
FG3="$FR3/docs/reports/ws0-3299-artifacts/harness/guards.py"
mkdir -p "$(dirname "$FG3")"
cp "$GUARDS" "$FG3"
plant_pin "$FR3" "$CORP"
rm "$FR3/docs/reports/ws0-3096-artifacts/corpus-identity.json"
expect_guard_fail CORPUS_MAP_UNREADABLE \
  python3 "$FG3" corpus --corpus "$CORP"
printf '{"components": {}}\n' > "$FR3/docs/reports/ws0-3096-artifacts/corpus-identity.json"
expect_guard_fail CORPUS_MAP_UNREADABLE \
  python3 "$FG3" corpus --corpus "$CORP"

FR4="$TMP/fakerepo-badmap"
FG4="$FR4/docs/reports/ws0-3299-artifacts/harness/guards.py"
mkdir -p "$(dirname "$FG4")"
cp "$GUARDS" "$FG4"
plant_pin "$FR4" "$CORP"
# Edit ONE component's recorded size in the artifact alone: the map's sum no
# longer equals the Rust pin's TOTAL_COMPONENT_BYTES, so the two canonical
# sources disagree and neither may serve as the expectation.
python3 - "$FR4/docs/reports/ws0-3096-artifacts/corpus-identity.json" <<'EDIT'
import json, sys
p = sys.argv[1]
d = json.load(open(p))
n = next(k for k in d["components"] if k.endswith("-Filter.db"))
d["components"][n]["bytes"] += 1
json.dump(d, open(p, "w"))
EDIT
expect_guard_fail CORPUS_MAP_UNCORROBORATED \
  python3 "$FG4" corpus --corpus "$CORP"
# ...and a map naming a CompressionInfo.db is not the #1406-boundary corpus's map.
plant_pin "$FR4" "$CORP"
python3 - "$FR4/docs/reports/ws0-3096-artifacts/corpus-identity.json" <<'EDIT'
import json, sys
p = sys.argv[1]
d = json.load(open(p))
n = next(k for k in d["components"] if k.endswith("-Filter.db"))
c = dict(d["components"][n], name="nb-1-big-CompressionInfo.db")
d["components"]["nb-1-big-CompressionInfo.db"] = c
del d["components"][n]
json.dump(d, open(p, "w"))
EDIT
expect_guard_fail CORPUS_MAP_UNCORROBORATED \
  python3 "$FG4" corpus --corpus "$CORP"

# An UNCONSULTABLE pin refuses; it never passes. (Affirmative-measurement rule.)
FR2="$TMP/fakerepo-nopin"
FG2="$FR2/docs/reports/ws0-3299-artifacts/harness/guards.py"
mkdir -p "$(dirname "$FG2")"
cp "$GUARDS" "$FG2"
expect_guard_fail CORPUS_PIN_UNREADABLE \
  python3 "$FG2" corpus --corpus "$CORP"
mkdir -p "$FR2/tools/ws0-corpus-gen/src"
printf 'pub const DATA_DB_BYTES: u64 = 12;\n// the digest constant was renamed\n' \
  > "$FR2/tools/ws0-corpus-gen/src/measurement_corpus.rs"
expect_guard_fail CORPUS_PIN_UNPARSEABLE \
  python3 "$FG2" corpus --corpus "$CORP"

# AND THE COMMITTED ORACLES THEMSELVES: the in-tree guards.py must parse the real
# `measurement_corpus.rs` AND the real `ws0-3096-artifacts/corpus-identity.json`,
# and the two must CORROBORATE each other. Both are read BEFORE the corpus is
# resolved, so an empty corpus reaching CORPUS_DATA_DB_ABSENT proves all of that
# happened against the committed files. A renamed constant surfaces here as
# CORPUS_PIN_UNPARSEABLE; an edited artifact as CORPUS_MAP_UNCORROBORATED.
mkdir -p "$TMP/corpus-empty"
expect_guard_fail CORPUS_DATA_DB_ABSENT \
  python3 "$GUARDS" corpus --corpus "$TMP/corpus-empty"

# ---------------------------------------------------- equivalence control ---
echo
echo "-- equivalence control (it must be able to FAIL) --"
# THE DEFECT THIS SECTION EXISTS FOR: `derive.py --equivalence` used to print the
# residual and then state, unconditionally and in prose, that it was "inside the
# bench's own single-run spread", exiting 0 whatever the numbers said. A control
# that cannot fail is not a control — a severely divergent worker would have
# passed it and its S-sweep points would have been published as comparable to the
# #3096/#3272 rig's. Both directions are asserted here, because a control that
# always REFUSES is equally useless.
plant_equiv() {  # <dir> <worker-rows/s> <shortfall-frac> <bench pass rows/s...>
  local d="$1" w="$2" sf="$3"; shift 3
  mkdir -p "$d"
  python3 - "$d" "$w" "$sf" "$@" <<'EOF'
import json, os, sys
d, w, sf, passes = sys.argv[1], float(sys.argv[2]), float(sys.argv[3]), sys.argv[4:]
json.dump({"passes": [{"rows_per_sec": float(p)} for p in passes]},
          open(os.path.join(d, "equiv-scan-bench.json"), "w"))
json.dump({"aggregate_rows_per_s": w, "attribution_shortfall_max_frac": sf},
          open(os.path.join(d, "equiv-worker-window.json"), "w"))
EOF
}

# POSITIVE control, and it is the COMMITTED RUN: the exact figures published in
# ../smoke/equivalence.md (bench passes 366,638 / 361,779 / 358,983; worker
# 356,763; shortfall 0.0639%). Residual -1.32% against the bench's own 2.1%
# spread — inside it, so the control must PASS and print VERDICT: EQUIVALENT.
plant_equiv "$TMP/eq-good" 356763 0.000639 366638 361779 358983
expect_ok "equivalence: the committed run's residual is within the bench spread" \
  python3 "$HERE/derive.py" --equivalence "$TMP/eq-good"
if python3 "$HERE/derive.py" --equivalence "$TMP/eq-good" | grep -q '\*\*VERDICT: EQUIVALENT\.\*\*'; then
  echo "ok    [equivalence verdict text] the passing run states its verdict"; PASS=$((PASS+1))
else
  echo "FAIL  [equivalence verdict text] a passing run did not print VERDICT: EQUIVALENT"
  FAIL=$((FAIL+1))
fi

# THE ONE IT COULD NOT SEE BEFORE: a worker 20% slower than the bench, with the
# same 2.1% bench spread. The pre-fix code printed this and exited 0.
plant_equiv "$TMP/eq-slow" 289423 0.000639 366638 361779 358983
expect_guard_fail EQUIV_DIVERGENCE \
  python3 "$HERE/derive.py" --equivalence "$TMP/eq-slow"
# The OTHER direction is a divergence too: a worker 15% FASTER than the arm it
# claims to be is not the same code path either.
plant_equiv "$TMP/eq-fast" 416046 0.000639 366638 361779 358983
expect_guard_fail EQUIV_DIVERGENCE \
  python3 "$HERE/derive.py" --equivalence "$TMP/eq-fast"
# JUST OUTSIDE the bound: -2.5% residual against a 2.1% spread. The bound is the
# measured spread, so a near-miss must refuse rather than round into a pass.
plant_equiv "$TMP/eq-edge" 352759 0.0 366638 361779 358983
expect_guard_fail EQUIV_DIVERGENCE \
  python3 "$HERE/derive.py" --equivalence "$TMP/eq-edge"
# THE SHORTFALL IS ADDED BACK, not ignored: the same raw delta that refuses above
# passes once the harness's known-low bias accounts for the part of it that is
# instrument, not engine.
plant_equiv "$TMP/eq-sf" 352759 0.0056 366638 361779 358983
expect_ok "equivalence: a near-miss inside the bound once the known-low bias is added back" \
  python3 "$HERE/derive.py" --equivalence "$TMP/eq-sf"

# AN UNMEASURED BOUND CANNOT LICENSE A PASS: one bench pass measures no spread.
plant_equiv "$TMP/eq-1pass" 356763 0.000639 361779
expect_guard_fail EQUIV_NO_SPREAD \
  python3 "$HERE/derive.py" --equivalence "$TMP/eq-1pass"

# ---------------------------------------- STRUCTURAL: the worker is pinned ---
echo
echo "-- exact dependency pins (structural) --"
# THE DEFECT THIS SECTION EXISTS FOR: scan-worker/ is its OWN workspace, so the
# repo's root Cargo.lock does not pin its build, and its own lockfile is
# gitignored (roborev's compiled-in `**/Cargo.lock` deny-list, #3278). With caret
# ranges the binary that produced every published number could not be rebuilt — a
# later `cargo build` resolves newer minors and measures different codegen.
expect_ok "worker deps are =exact and the recorded closure agrees" \
  python3 "$HERE/check-exact-pins.py" "$HERE/scan-worker/Cargo.toml"
# NEGATIVE CONTROLS. Without these the check above could pass vacuously.
mkdir -p "$TMP/pins"
sed 's/^clap = { version = "=4.6.6"/clap = { version = "4.4"/' \
  "$HERE/scan-worker/Cargo.toml" > "$TMP/pins/caret.toml"
expect_guard_fail PIN_NOT_EXACT \
  python3 "$HERE/check-exact-pins.py" "$TMP/pins/caret.toml" \
    "$HERE/scan-worker/measured-build-lockfile.txt"
sed 's|^cqlite-core = { path = "\(.*\)", features|cqlite-core = { path = "\1", version = "0.16.1", features|' \
  "$HERE/scan-worker/Cargo.toml" > "$TMP/pins/pathver.toml"
expect_guard_fail PIN_NOT_EXACT \
  python3 "$HERE/check-exact-pins.py" "$TMP/pins/pathver.toml" \
    "$HERE/scan-worker/measured-build-lockfile.txt"
printf '[package]\nname = "x"\nversion = "0.1.0"\n' > "$TMP/pins/nodeps.toml"
expect_guard_fail PIN_NOT_EXACT \
  python3 "$HERE/check-exact-pins.py" "$TMP/pins/nodeps.toml" \
    "$HERE/scan-worker/measured-build-lockfile.txt"
expect_guard_fail PIN_MANIFEST_UNREADABLE \
  python3 "$HERE/check-exact-pins.py" "$TMP/pins/does-not-exist.toml" \
    "$HERE/scan-worker/measured-build-lockfile.txt"
expect_guard_fail PIN_LOCK_MISSING \
  python3 "$HERE/check-exact-pins.py" "$HERE/scan-worker/Cargo.toml" \
    "$TMP/pins/no-such-lock.txt"
# THE DRIFT DIRECTION: the record must be a record OF THIS BUILD. A closure whose
# clap version is not the pinned one is stale, and a stale record is worse than
# none — it looks like provenance.
python3 - "$HERE/scan-worker/measured-build-lockfile.txt" "$TMP/pins/stale.txt" <<'EOF'
import sys
src, dst = sys.argv[1], sys.argv[2]
text = open(src).read().replace('name = "clap"\nversion = "4.6.6"',
                                'name = "clap"\nversion = "4.5.0"')
open(dst, "w").write(text)
EOF
expect_guard_fail PIN_LOCK_DISAGREES \
  python3 "$HERE/check-exact-pins.py" "$HERE/scan-worker/Cargo.toml" "$TMP/pins/stale.txt"

# ------------------------------------- STRUCTURAL: process-lifecycle safety ---
# Asks WHERE a spawn is, not what it looks like, so a new one added anywhere
# outside a cleanup guarantee fails however it is written. Two consecutive review
# rounds found this same class (orphaned workers, then an unreaped perf), which
# is why it is now checked structurally rather than case by case.
echo
echo "-- process lifecycle (structural) --"
expect_ok "every child spawn is under a cleanup guarantee" \
  python3 "$HERE/check-lifecycle.py" "$HERE/rep.py"
# NEGATIVE CONTROL: the exact pre-fix shape — a local list, appended outside any
# try — must be REJECTED. Without this the check above could pass vacuously.
cat > "$TMP/lifecycle-bad.py" <<'PY'
import subprocess
def launch(cmds):
    procs = []
    for c in cmds:
        procs.append(subprocess.Popen(c))
    return procs
PY
if python3 "$HERE/check-lifecycle.py" "$TMP/lifecycle-bad.py" >/dev/null 2>&1; then
  echo "FAIL  [lifecycle negative control] the check ACCEPTED the pre-fix orphan shape"
  FAIL=$((FAIL+1))
else
  echo "ok    [lifecycle negative control] rejects a local-list spawn outside try"
  PASS=$((PASS+1))
fi

# -------------------------------- STRUCTURAL: a verdict is computed, not told ---
echo
echo "-- verdict channel (structural) --"
expect_ok "derive.py has no hand-written verdict channel" \
  python3 "$HERE/check-no-verdict-channel.py" "$HERE/derive.py"
# NEGATIVE CONTROLS — both shapes the hatch has taken or could take. Without
# these the check above could pass vacuously.
sed 's/--extension", action="append"/--verdict-override", action="append"/' \
  "$HERE/derive.py" > "$TMP/vc-flag.py"
sed 's/verdicts.update(extension_verdicts(d, reps))/verdicts.update(json.load(open("v.json")))/' \
  "$HERE/derive.py" > "$TMP/vc-file.py"
for shape in flag file; do
  if python3 "$HERE/check-no-verdict-channel.py" "$TMP/vc-$shape.py" >/dev/null 2>&1; then
    echo "FAIL  [verdict channel: $shape] the check ACCEPTED a reintroduced override"
    FAIL=$((FAIL+1))
  else
    echo "ok    [verdict channel: $shape] rejects a reintroduced override"
    PASS=$((PASS+1))
  fi
done

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
