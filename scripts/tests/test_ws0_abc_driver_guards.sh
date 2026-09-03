#!/usr/bin/env bash
# test_ws0_abc_driver_guards.sh — THE INTERLEAVED A/B/C SET'S OWN GUARDS (issue #3551).
#
# # Subject
#
# `ws0-3551-abc.sh` and `ws0_abc_aggregate.py` are the two halves of one claim: that a set of
# `ws0-baseline.sh` sessions in one directory is ONE PAIRED EXPERIMENT. Neither had a single
# test. Three roborev findings, all of the same family — an artifact ADOPTED without its
# provenance being established — and each is a case below:
#
#   * F1 (High) the RESUME accepted any existing `results.json`. The skip itself is deliberate
#     and stays (this box is shared and a set that starts over loses its window), so the fix is
#     a RUN FINGERPRINT written on the first invocation and VERIFIED on every later one, plus
#     per-session window validation. Reusing an `--out` with a changed corpus, binary, pin,
#     allocator or arena cap silently combined incompatible sessions into a "paired" set.
#   * F2 (Medium) the aggregator read CONFIGURATION from the first complete round while
#     aggregating MEASUREMENTS from all of them, so a later round could carry a different
#     treatment and produce a delta ACROSS treatments under a table describing only one.
#   * F3 (Medium) `ratio bare/flight` was computed as flight cycles/row over bare cycles/row —
#     inverted with respect to its own label AND a different QUANTITY from the one that name
#     denotes everywhere else in this rig, where it is a ROWS/S ratio (`ws0_report.py`,
#     `ws0-baseline.sh`'s printed line, `ws0-3248-artifacts/ac0/DELTA-TABLE.md`).
#
# # The bar, per #3249 (a hardcoded `_PERF_STATE="ok"` survived 118/118 tests)
#
# OBSERVED TO FIRE, and observed to fire ON THE PLANTED THING:
#
#   * every refusal case is paired with the ACCEPT direction of the same check — a guard that
#     only ever reds is the guard an operator works around, and case 1a is that control for the
#     whole fingerprint;
#   * every RED arm differs from its passing control in EXACTLY ONE property, and where the
#     property is planted by editing a scratch copy of the driver, THE PLANT IS ASSERTED TO HAVE
#     TAKEN (`abc_copy` refuses a `sed` that left the copy byte-identical) — a `sed` that matched
#     nothing produces a RED arm identical to the control, which passes while proving nothing;
#   * every refusal is matched on its OWN diagnostic — the FIELD name, both VALUES, or the
#     DIRECTORY — never on the mere fact of a non-zero exit, which an unrelated breakage
#     produces identically.
#
# # Hermetic, and hermetic AFFIRMATIVELY
#
# No cargo, perf, taskset, sudo, root, corpus binaries, server or network. The driver under test
# is a SCRATCH COPY with a recording STUB `ws0-baseline.sh` placed beside it, so the real
# measurement driver is never on the path `$HERE/ws0-baseline.sh` resolves to. Two oracles rather
# than an assumption: the stub's own log (case 0a proves the harness CAN see an invocation, so
# "the log is empty" in the accept cases is a measurement and not an absence) and
# `lib-ws0-hermetic.sh`'s recording shims, asserted empty.
#
# The driver copy is invoked as `bash ./ws0-3551-abc.sh` from inside its own directory rather
# than through a `"$var"` path. That is not a style choice: `ws0_hermeticity_lint` treats a
# command word it cannot resolve as a possible driver invocation (fail closed, correctly), so a
# `bash "$dir/ws0-3551-abc.sh"` would need a `ws0-hermetic-allow` marker — an exemption where a
# literal command word needs none. Removing the ambiguity beats marking it.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ABC_DRIVER="$REPO_ROOT/scripts/perf/ws0-3551-abc.sh"

# THE ARM SET IS DERIVED FROM THE DRIVER, NEVER RESTATED HERE (#3551).
#
# This suite first hard-coded `A B C0 C` in five places, and adding arm D to the driver reddened
# two cases that were testing nothing about arm D — a curated copy of a set the driver owns. Per
# this repo's standing rule the subject set is computed from committed source at run time, so a
# new arm is picked up rather than breaking the suite; and a FAILED derivation is a FAIL that
# NAMES the derivation, never a fallback to a default, which would silently re-curate the list.
_arms_line=$(grep -oE '^ARMS=\([^)]*\)' "$ABC_DRIVER" | head -1) || true
if [ -z "$_arms_line" ]; then
  printf 'FAIL - derivation: no top-level ARMS=(...) in %s, so the arm set cannot be derived\n' \
    "$ABC_DRIVER" >&2
  exit 1
fi
# shellcheck disable=SC2206  # deliberate word split of a literal array body
DRIVER_ARMS=(${_arms_line#ARMS=(})
DRIVER_ARMS=("${DRIVER_ARMS[@]%)}")
NARMS=${#DRIVER_ARMS[@]}
if [ "$NARMS" -lt 2 ]; then
  printf 'FAIL - derivation: %s declares %d arm(s); a pairing suite needs at least 2\n' \
    "$ABC_DRIVER" "$NARMS" >&2
  exit 1
fi
AGG="$REPO_ROOT/scripts/perf/ws0_abc_aggregate.py"

fails=0
# `checks` counts what actually RAN (incremented by pass/fail themselves), so the floor at the
# end can see a block that silently never executed — this file has no `set -e`.
checks=0
pass() { checks=$((checks + 1)); echo "ok   - $1"; }
fail() { checks=$((checks + 1)); echo "FAIL - $1"; fails=$((fails + 1)); }

for f in "$ABC_DRIVER" "$AGG"; do
  [ -f "$f" ] || { echo "FAIL - missing $f"; exit 1; }
done
# python3 is a HARD REQUIREMENT of this rig (`ws0-baseline.sh` refuses to run without it, and
# the aggregator IS python), so its absence is a FAILED CHECK and not a skip: exiting 0 here
# would record the gate component as SUCCESS with none of these checks having run.
command -v python3 >/dev/null 2>&1 || {
  echo "FAIL - python3 is not installed. It is a HARD REQUIREMENT of the WS0 rig, and a skip"
  echo "       here would report this component SUCCESS with 0 checks run."
  exit 1
}

TMP="$(mktemp -d)"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

# shellcheck source=scripts/tests/lib-ws0-hermetic.sh
source "$REPO_ROOT/scripts/tests/lib-ws0-hermetic.sh"
ws0_hermetic_init "$TMP"

export ABC_STUB_LOG="$TMP/stub-invocations.txt"
: > "$ABC_STUB_LOG"

# ---------------------------------------------------------------------------
# The fixture tools, written once and driven by argument
# ---------------------------------------------------------------------------
# `mut.py` mutates ONE dotted field of a JSON document (or DELETES it), which is how every RED
# arm below differs from its control in exactly one property.
cat > "$TMP/mut.py" <<'PY'
import json
import pathlib
import sys

p = pathlib.Path(sys.argv[1])
doc = json.loads(p.read_text())
keys = sys.argv[2].split(".")
cur = doc
for k in keys[:-1]:
    cur = cur[k]
if sys.argv[3] == "__DELETE__":
    del cur[keys[-1]]
else:
    try:
        cur[keys[-1]] = json.loads(sys.argv[3])
    except ValueError:
        cur[keys[-1]] = sys.argv[3]
p.write_text(json.dumps(doc))
PY

# `mkset.py` writes a synthetic A/B/C session set: one `r<N>-<arm>/results.json` per (round,
# arm), carrying exactly the fields the aggregator reads. The measured values are IDENTICAL in
# every round on purpose, so every median is exactly the value supplied and a numeric assertion
# has one expected answer.
cat > "$TMP/mkset.py" <<'PY'
import json
import pathlib
import sys

out = pathlib.Path(sys.argv[1])
rounds = int(sys.argv[2])
arms = sys.argv[3].split(",")
scan_rps, flight_rps, scan_cpr, flight_cpr = (float(v) for v in sys.argv[4:8])

TREATMENT = {
    "A": ("2,10", "siblings", "system", None),
    "B": ("2,3", "distinct-cores", "system", None),
    "C0": ("2,3", "distinct-cores", "system", 2),
    "C": ("2,3", "distinct-cores", "jemalloc", None),
}
for rnd in range(1, rounds + 1):
    for arm in arms:
        fpin, mode, alloc, arena = TREATMENT[arm]
        d = out / f"r{rnd}-{arm}"
        d.mkdir(parents=True, exist_ok=True)
        doc = {
            "measurements": [
                {"temperature": "warm", "arm": "bare_scan", "reps": [{}],
                 "rows_per_sec": {"median": scan_rps, "spread_pct_of_median": 1.0, "n": 1},
                 "cycles_per_row": {"median": scan_cpr}, "ipc": {"median": 1.45},
                 "row_denominator_total": 1000},
                {"temperature": "warm", "arm": "flight_bypass", "reps": [{}],
                 "rows_per_sec": {"median": flight_rps, "spread_pct_of_median": 2.0, "n": 1},
                 "cycles_per_row": {"median": flight_cpr}, "ipc": {"median": 1.36},
                 "row_denominator_total": 2000},
            ],
            "pinning": {
                "server_cpus": "2,10", "client_cpus": "4,12",
                "flight_server_cpus": fpin, "flight_pin_mode": mode,
                "flight_allocator": alloc, "flight_malloc_arena_max": arena,
                "counter_mode": (f"perf stat -C 2,10 for the bare-scan arm and -C {fpin} for"
                                 " the Flight arm (CPU-WIDE; never -p)"),
            },
            "corpus_identity": {"data_db_sha256": "abc123", "rows": 1000},
            "binary_provenance": {"binaries": {
                "ws0-scan-bench": {"sha256": "a" * 64},
                "cqlite-flight": {"sha256": "b" * 64},
                "flight-loadgen": {"sha256": "c" * 64}}},
            "flight_admission": {"max_concurrent_scans": 4,
                                 "max_concurrent_scans_source": "derived",
                                 "available_parallelism": 2},
        }
        (d / "results.json").write_text(json.dumps(doc))
PY

# `col.py` reads ONE cell out of the aggregator's markdown, by SECTION heading, arm and COLUMN
# HEADER — never by field position. A positional read would keep passing after a column moved
# and would then be asserting about a different quantity, which is this issue's own F3.
cat > "$TMP/col.py" <<'PY'
import pathlib
import sys

lines = pathlib.Path(sys.argv[1]).read_text().splitlines()
section, arm, column = sys.argv[2], sys.argv[3], sys.argv[4]
start = None
for i, line in enumerate(lines):
    if line.startswith("## ") and section in line:
        start = i
        break
if start is None:
    sys.stderr.write(f"no section heading contains {section!r}\n")
    raise SystemExit(3)
header = None
for line in lines[start:]:
    if line.startswith("## ") and start != lines.index(line):
        break
    if not line.startswith("|"):
        continue
    cells = [c.strip() for c in line.strip("|").split("|")]
    if header is None:
        if cells[0] == "arm":
            header = cells
        continue
    if set(line) <= set("|- "):
        continue
    if cells[0] == arm:
        if column not in header:
            sys.stderr.write(f"no column {column!r} in {header!r}\n")
            raise SystemExit(3)
        print(cells[header.index(column)])
        raise SystemExit(0)
sys.stderr.write(f"no row for arm {arm!r} under {section!r}\n")
raise SystemExit(3)
PY

mut() { python3 "$TMP/mut.py" "$@"; }
mkset() { python3 "$TMP/mkset.py" "$@"; }
cell() { python3 "$TMP/col.py" "$@"; }

# ===========================================================================
# THE DRIVER HARNESS
# ===========================================================================
# abc_copy <destdir> [sed-expr] — a scratch copy of the shipped driver, with the recording STUB
# `ws0-baseline.sh` beside it so `$HERE/ws0-baseline.sh` can never resolve to the real one.
#
# When a `sed` expression is given, the edit is ASSERTED TO HAVE TAKEN. That assert is the whole
# reason this is a function: a `sed` whose pattern stopped matching (a reflowed line, a renamed
# variable) leaves a copy byte-identical to the control, and the RED arm then "passes" having
# planted nothing. The three arm properties this suite must move — the pins, the pin modes and
# the allocators — are NOT command-line flags of the driver (they are its definition of an arm),
# so substituting the artifact is the only honest way to move one; a settable seam would be one
# more thing a real operator could set.
abc_copy() {
  local dest="$1" expr="${2:-}"
  mkdir -p "$dest"
  cp "$ABC_DRIVER" "$dest/ws0-3551-abc.sh"
  cat > "$dest/ws0-baseline.sh" <<'STUBEOF'
#!/usr/bin/env bash
# The STUB. It RECORDS its argv and writes NO results.json, so a session it "measures" stays
# unmeasured and the next invocation must still decide to run it.
printf 'INVOKED %s\n' "$*" >> "$ABC_STUB_LOG"
exit 0
STUBEOF
  chmod +x "$dest/ws0-baseline.sh" "$dest/ws0-3551-abc.sh"
  if [ -n "$expr" ]; then
    sed -i.bak "$expr" "$dest/ws0-3551-abc.sh" && rm -f "$dest/ws0-3551-abc.sh.bak"
    if cmp -s "$ABC_DRIVER" "$dest/ws0-3551-abc.sh"; then
      fail "PLANT DID NOT TAKE: sed '$expr' left the driver copy byte-identical to the shipped one — this RED arm would prove nothing"
      return 1
    fi
  fi
  return 0
}

# run_abc <driverdir> <args…> — invoke the copy, with the hermeticity shims on PATH, resetting
# both oracles first so each case's reading describes that case alone.
run_abc() {
  local dir="$1"; shift
  : > "$ABC_STUB_LOG"
  ws0_hermetic_reset
  ( cd "$dir" && PATH="$WS0_SHIM_BIN:$PATH" bash ./ws0-3551-abc.sh "$@" ) 2>&1
}

# `grep -c` PRINTS 0 and EXITS 1 when it matches nothing, so a `|| echo 0` fallback emits the
# count TWICE ("0\n0") and every `[ "$n" -eq 0 ]` downstream becomes a bash syntax error rather
# than a comparison — measured. The status is absorbed into an assignment instead.
stub_invocations() {
  local n
  n=$(grep -c '^INVOKED ' "$ABC_STUB_LOG" 2>/dev/null) || n="${n:-0}"
  printf '%s\n' "${n:-0}"
}

# A corpus is, to this driver, a directory holding `corpus-identity.json` — it never opens the
# Data.db (that is `ws0-baseline.sh`'s job, and it is stubbed), so no fixture bytes are needed.
make_identity() {
  local dir="$1" sha="$2" rows="$3"
  mkdir -p "$dir"
  printf '{"data_db_sha256":"%s","rows":%s,"seed":7}\n' "$sha" "$rows" > "$dir/corpus-identity.json"
}
make_bins() {
  local dir="$1" tag="$2" b
  mkdir -p "$dir"
  for b in ws0-scan-bench cqlite-flight flight-loadgen; do
    printf 'ELF-ish %s %s\n' "$b" "$tag" > "$dir/$b"
  done
}
# Mark a (round, arm) session dir MEASURED: a results.json plus the window record the driver
# writes. Both, because either alone is one of the states this suite refuses.
make_measured() {
  local out="$1" rnd="$2" arm="$3"
  mkdir -p "$out/r$rnd-$arm"
  printf '{}\n' > "$out/r$rnd-$arm/results.json"
  printf '{"round":%s,"position_in_round":1,"arms_in_round":%s,"arm":"%s","exit":0}\n' \
    "$rnd" "$NARMS" "$arm" > "$out/r$rnd-$arm/abc-window.json"
}
measure_all() {
  local out="$1" rounds="$2" r arm
  for ((r = 1; r <= rounds; r++)); do
    for arm in "${DRIVER_ARMS[@]}"; do make_measured "$out" "$r" "$arm"; done
  done
}

CORPUS="$TMP/corpus"
BINS="$TMP/bins"
make_identity "$CORPUS" "deadbeefsha" 1000
make_bins "$BINS" one
abc_copy "$TMP/d-base" || true

# ===========================================================================
# PART 0 — THE HARNESS'S OWN ORACLES
# ===========================================================================
# Without this case, every "the stub recorded nothing" assertion below could be satisfied by a
# stub that cannot record at all, and every "the shims recorded nothing" by shims that never
# ran — the #3249 shape (a hardcoded `ok` surviving 118 tests) reproduced in the harness rather
# than in the code under test.
OUT="$TMP/out-fresh"
out=$(run_abc "$TMP/d-base" --corpus "$CORPUS" --bin-dir "$BINS" --out "$OUT" --rounds 1); rc=$?
n=$(stub_invocations)
if [ "$rc" -eq 0 ] && [ "$n" -eq "$NARMS" ]; then
  pass "0a. the harness CAN observe a measurement: a fresh --out runs all $NARMS arms of round 1 through the stub (recorded $n invocations)"
else
  fail "0a. a fresh --out must invoke the stub once per arm (rc=$rc, invocations=$n, out: $out)"
fi
if grep -q "run record WRITTEN to $OUT/abc-run.json" <<<"$out" && [ -f "$OUT/abc-run.json" ]; then
  pass "0a. the first invocation WRITES the run fingerprint, and says so"
else
  fail "0a. the first invocation must write abc-run.json (out: $out)"
fi
if ws0_driver_ran_hermetically; then
  pass "0a. and it ran HERMETICALLY — sudo/cargo/perf/taskset recorded nothing"
else

  fail "0a. the driver executed a shimmed tool: $(ws0_hermetic_calls | tr '\n' ' ')"
fi
# The fingerprint's own content, asserted once: a field list nobody checks is a field list that
# can quietly shrink, and every refusal case below rests on these being present.
_expected_fields=(corpus_path corpus_data_db_sha256 corpus_rows bin_dir step_duration
                  arena_max jemalloc_lib port arms binary_sha256.ws0-scan-bench
                  binary_sha256.cqlite-flight binary_sha256.flight-loadgen)
# One `arm_flags.<arm>` per DERIVED arm, so a new arm must appear in the fingerprint too.
for _a in "${DRIVER_ARMS[@]}"; do _expected_fields+=("arm_flags.$_a"); done
for field in "${_expected_fields[@]}"; do
  if grep -q "\"$field\"" "$OUT/abc-run.json"; then
    pass "0b. the fingerprint records $field"
  else
    fail "0b. the fingerprint must record $field (have: $(tr -d '\n' < "$OUT/abc-run.json"))"
  fi
done
if grep -q '"rounds"' "$OUT/abc-run.json"; then
  fail "0b. --rounds must NOT be fingerprinted — extending a set is a legitimate resume"
else
  pass "0b. --rounds is NOT a fingerprint field, and the record says why (rounds_excluded)"
fi
if grep -q '"rounds_excluded"' "$OUT/abc-run.json"; then
  pass "0b. ...and the exclusion is RECORDED in the artifact, not only in the source"
else
  fail "0b. abc-run.json must record why --rounds is excluded"
fi

# ===========================================================================
# PART 1 — THE RUN FINGERPRINT: THE ACCEPT DIRECTION, THEN ONE FIELD AT A TIME
# ===========================================================================
# 1a. THE POSITIVE CONTROL. An identical configuration over an --out whose sessions are all
# measured must be ACCEPTED, must SKIP every session, and must measure NOTHING. Without this
# half, a fingerprint check hardcoded to refuse would satisfy every case that follows.
BASE_OUT="$TMP/out-base"
measure_all "$BASE_OUT" 3
base_args=(--corpus "$CORPUS" --bin-dir "$BINS" --out "$BASE_OUT" --rounds 3)
out=$(run_abc "$TMP/d-base" "${base_args[@]}"); rc=$?
if [ "$rc" -eq 0 ] && grep -q "run record WRITTEN" <<<"$out"; then
  pass "1a. (setup) the fingerprint is written for the base set"
else
  fail "1a. (setup) the base set's fingerprint must be written (rc=$rc, out: $out)"
fi
out=$(run_abc "$TMP/d-base" "${base_args[@]}"); rc=$?
skips=$(grep -c 'SKIP (measured, window VERIFIED' <<<"$out")
n=$(stub_invocations)
if [ "$rc" -eq 0 ] && grep -q "resume:   VERIFIED against" <<<"$out" \
   && [ "$skips" -eq "$((3 * NARMS))" ] && [ "$n" -eq 0 ]; then
  pass "1a. an IDENTICAL configuration is ACCEPTED, all 12 sessions SKIPPED, and NOTHING re-measured"
else
  fail "1a. an identical resume must be accepted and skip everything (rc=$rc, skips=$skips, stub=$n, out: $out)"
fi
if ws0_driver_ran_hermetically; then
  pass "1a. ...and the accepted resume touched no shimmed tool either"
else
  fail "1a. the accepted resume executed a shimmed tool: $(ws0_hermetic_calls | tr '\n' ' ')"
fi

# refuses_naming <label> <expected-token>… — run the driver and require BOTH a non-zero exit
# AND every expected token in the diagnostic. The tokens are the point: an unrelated breakage
# produces the same exit code, so a bare red is not evidence of anything.
refuses_naming() {
  local label="$1" dir="$2"; shift 2
  local -a expect=()
  while [ "$1" != "--" ]; do expect+=("$1"); shift; done
  shift
  local out rc missing=""
  out=$(run_abc "$dir" "$@"); rc=$?
  if [ "$rc" -eq 0 ]; then
    fail "$label: must REFUSE, exited 0 (out: $(tail -3 <<<"$out"))"
    return
  fi
  local token
  for token in "${expect[@]}"; do
    grep -qF -- "$token" <<<"$out" || missing="$missing [$token]"
  done
  if [ -n "$missing" ]; then
    fail "$label: refused but did not NAME$missing (out: $(tail -6 <<<"$out"))"
    return
  fi
  if [ "$(stub_invocations)" -ne 0 ]; then
    fail "$label: refused but MEASURED something first ($(stub_invocations) stub invocation(s))"
    return
  fi
  pass "$label"
}

# --- 1b. the corpus PATH. A second directory with the SAME identity content, so the ONLY
# difference is the path itself.
make_identity "$TMP/corpus-elsewhere" "deadbeefsha" 1000
refuses_naming "1b. a changed corpus PATH is REFUSED, naming the field and both paths" \
  "$TMP/d-base" "corpus_path" "$CORPUS" "$TMP/corpus-elsewhere" "INCOMPATIBLE" -- \
  --corpus "$TMP/corpus-elsewhere" --bin-dir "$BINS" --out "$BASE_OUT" --rounds 3

# --- 1c. the corpus SHA at the SAME path — the case a path comparison cannot see, and the
# reason the sha is in the fingerprint at all.
make_identity "$CORPUS" "cafebabesha" 1000
refuses_naming "1c. a REPOPULATED corpus (same path, different Data.db sha) is REFUSED, naming the sha field and both digests" \
  "$TMP/d-base" "corpus_data_db_sha256" "deadbeefsha" "cafebabesha" -- "${base_args[@]}"
make_identity "$CORPUS" "deadbeefsha" 1000
# ...and the same path with a different ROW COUNT, which is the other half of the identity.
make_identity "$CORPUS" "deadbeefsha" 2000
refuses_naming "1c. a changed corpus ROW COUNT is REFUSED, naming the field and both counts" \
  "$TMP/d-base" "corpus_rows" "1000" "2000" -- "${base_args[@]}"
make_identity "$CORPUS" "deadbeefsha" 1000

# --- 1d. the --bin-dir PATH. A second directory whose binaries are BYTE-IDENTICAL, so the
# digests agree and the path is the only moving part.
make_bins "$TMP/bins-elsewhere" one
refuses_naming "1d. a changed --bin-dir PATH is REFUSED, naming the field and both paths" \
  "$TMP/d-base" "bin_dir" "$BINS" "$TMP/bins-elsewhere" -- \
  --corpus "$CORPUS" --bin-dir "$TMP/bins-elsewhere" --out "$BASE_OUT" --rounds 3

# --- 1e. a BINARY DIGEST at the same path — the arms must measure identical bytes, which is
# the whole reason --bin-dir is not per-arm.
printf 'ELF-ish cqlite-flight REBUILT\n' > "$BINS/cqlite-flight"
refuses_naming "1e. a REBUILT binary (same --bin-dir, different bytes) is REFUSED, naming binary_sha256.cqlite-flight" \
  "$TMP/d-base" "binary_sha256.cqlite-flight" "DIFFERS" -- "${base_args[@]}"
make_bins "$BINS" one

# --- 1f–1i. each ARM's own identity, one property at a time. These are not flags — they are the
# driver's definition of an arm — so each is planted by editing a scratch copy, and `abc_copy`
# has asserted the edit took.
if abc_copy "$TMP/d-pin-a" 's/^PIN_A="2,10"$/PIN_A="6,14"/'; then
  refuses_naming "1f. arm A's PIN changed is REFUSED, naming arm_flags.A and both flag lists" \
    "$TMP/d-pin-a" "arm_flags.A" "2,10" "6,14" -- "${base_args[@]}"
fi
if abc_copy "$TMP/d-pin-b" 's/^PIN_B="2,3"$/PIN_B="4,5"/'; then
  # PIN_B is arm B, C0 and C's pin, so all three flag lists move — every one of them named.
  refuses_naming "1g. the SHARED distinct-core pin changed is REFUSED, naming arm_flags.B, arm_flags.C0 and arm_flags.C" \
    "$TMP/d-pin-b" "arm_flags.B" "arm_flags.C0" "arm_flags.C" "4,5" -- "${base_args[@]}"
fi
if abc_copy "$TMP/d-mode" 's/--flight-pin-mode siblings/--flight-pin-mode distinct-cores/'; then
  refuses_naming "1h. arm A's PIN MODE changed is REFUSED, naming arm_flags.A and both modes" \
    "$TMP/d-mode" "arm_flags.A" "siblings" "distinct-cores" -- "${base_args[@]}"
fi
if abc_copy "$TMP/d-alloc" 's/--flight-allocator jemalloc/--flight-allocator system/'; then
  refuses_naming "1i. arm C's ALLOCATOR changed is REFUSED, naming arm_flags.C and both allocators" \
    "$TMP/d-alloc" "arm_flags.C" "jemalloc" "system" -- "${base_args[@]}"
fi

# --- 1j. the ARENA CAP, which IS a flag, and which reaches one arm's flag list.
refuses_naming "1j. a changed --arena-max is REFUSED, naming arena_max AND arm C0's flag list" \
  "$TMP/d-base" "arena_max" "arm_flags.C0" "--flight-malloc-arena-max 4" -- \
  --corpus "$CORPUS" --bin-dir "$BINS" --out "$BASE_OUT" --rounds 3 --arena-max 4

# --- 1k. the step duration and the port: measurement apparatus, and two sessions held for
# different durations or served on different ports are not the same experiment.
refuses_naming "1k. a changed --step-duration is REFUSED, naming step_duration and both values" \
  "$TMP/d-base" "step_duration" "45s" "20s" -- \
  --corpus "$CORPUS" --bin-dir "$BINS" --out "$BASE_OUT" --rounds 3 --step-duration 20s
refuses_naming "1k. a changed --port is REFUSED, naming port and both values" \
  "$TMP/d-base" "port" "18815" "19999" -- \
  --corpus "$CORPUS" --bin-dir "$BINS" --out "$BASE_OUT" --rounds 3 --port 19999
refuses_naming "1k. a changed --jemalloc-lib is REFUSED, naming jemalloc_lib" \
  "$TMP/d-base" "jemalloc_lib" "/opt/lib/libjemalloc.so.2" -- \
  --corpus "$CORPUS" --bin-dir "$BINS" --out "$BASE_OUT" --rounds 3 \
  --jemalloc-lib /opt/lib/libjemalloc.so.2

# --- 1l. THE DELIBERATE EXCLUSION. Extending the set from 3 rounds to 5 over the same --out is
# a legitimate resume, so it must be ACCEPTED. This is the case that would red if someone
# "tightened" the fingerprint by adding every argument to it.
measure_all "$BASE_OUT" 5
out=$(run_abc "$TMP/d-base" --corpus "$CORPUS" --bin-dir "$BINS" --out "$BASE_OUT" --rounds 5)
rc=$?
skips=$(grep -c 'SKIP (measured, window VERIFIED' <<<"$out")
if [ "$rc" -eq 0 ] && grep -q "resume:   VERIFIED against" <<<"$out" && [ "$skips" -eq "$((5 * NARMS))" ]; then
  pass "1l. --rounds 3 -> 5 over the SAME --out is ACCEPTED (the deliberate exclusion), all $((5 * NARMS)) sessions skipped"
else
  fail "1l. extending the round count must be accepted (rc=$rc, skips=$skips, out: $(tail -5 <<<"$out"))"
fi
# ...and narrowing it too: 5 -> 2 is the same fact from the other side, and it must not refuse
# either. A resume that accepts only growth would still red an operator re-checking one round.
out=$(run_abc "$TMP/d-base" --corpus "$CORPUS" --bin-dir "$BINS" --out "$BASE_OUT" --rounds 2)
rc=$?
if [ "$rc" -eq 0 ] && grep -q "resume:   VERIFIED against" <<<"$out"; then
  pass "1l. --rounds 5 -> 2 is ACCEPTED as well — the exclusion is of the FIELD, not of one direction"
else
  fail "1l. reducing the round count must be accepted too (rc=$rc, out: $(tail -5 <<<"$out"))"
fi

# --- 1m–1p. COULD-NOT-MEASURE. Each of these is a state in which the comparison COULD NOT BE
# MADE, and every one of them must be a REFUSAL rather than "compatible" — a comparison that
# could not be made has not been made.
UNREADABLE_OUT="$TMP/out-unreadable"
measure_all "$UNREADABLE_OUT" 1
out=$(run_abc "$TMP/d-base" --corpus "$CORPUS" --bin-dir "$BINS" --out "$UNREADABLE_OUT" --rounds 1)
chmod 000 "$UNREADABLE_OUT/abc-run.json"
if [ "$(id -u)" -eq 0 ]; then
  # root reads anything, so the case cannot be constructed — and a SKIP here would be a hole
  # wearing a skip's clothes, so it is a FAILED check naming why.
  fail "1m. cannot construct an unreadable abc-run.json as root — run this suite unprivileged"
else
  refuses_naming "1m. an UNREADABLE abc-run.json is REFUSED (not 'compatible'), naming the file and the read failure" \
    "$TMP/d-base" "abc-run.json" "could not be READ" "not a" -- \
    --corpus "$CORPUS" --bin-dir "$BINS" --out "$UNREADABLE_OUT" --rounds 1
fi
chmod 644 "$UNREADABLE_OUT/abc-run.json"

printf 'this is not json\n' > "$UNREADABLE_OUT/abc-run.json"
refuses_naming "1m. an abc-run.json that is not JSON is REFUSED, naming the file" \
  "$TMP/d-base" "abc-run.json" "not readable JSON" -- \
  --corpus "$CORPUS" --bin-dir "$BINS" --out "$UNREADABLE_OUT" --rounds 1
printf '{"issue":"#3551"}\n' > "$UNREADABLE_OUT/abc-run.json"
refuses_naming "1m. an abc-run.json with no \`fields\` object is REFUSED — there is nothing to compare against" \
  "$TMP/d-base" "abc-run.json" "carries no \`fields\` object" -- \
  --corpus "$CORPUS" --bin-dir "$BINS" --out "$UNREADABLE_OUT" --rounds 1

if [ "$(id -u)" -ne 0 ]; then
  chmod 000 "$CORPUS/corpus-identity.json"
  refuses_naming "1n. an UNREADABLE corpus-identity.json is REFUSED, naming the file and stating it is UNMEASURED" \
    "$TMP/d-base" "corpus-identity.json" "could not be READ" "UNMEASURED" -- "${base_args[@]}"
  chmod 644 "$CORPUS/corpus-identity.json"
else
  fail "1n. cannot construct an unreadable corpus-identity.json as root — run this suite unprivileged"
fi
mv "$CORPUS/corpus-identity.json" "$TMP/identity.parked"
refuses_naming "1o. an ABSENT corpus-identity.json is REFUSED, naming the file and the remedy" \
  "$TMP/d-base" "no corpus identity at" "ws0-corpus-gen" -- "${base_args[@]}"
printf 'not json\n' > "$CORPUS/corpus-identity.json"
refuses_naming "1o. a MALFORMED corpus-identity.json is REFUSED, naming the file" \
  "$TMP/d-base" "corpus-identity.json" "not readable JSON" -- "${base_args[@]}"
printf '{"rows":1000}\n' > "$CORPUS/corpus-identity.json"
refuses_naming "1o. a corpus identity with NO data_db_sha256 is REFUSED, naming the field" \
  "$TMP/d-base" "data_db_sha256" -- "${base_args[@]}"
printf '{"data_db_sha256":"deadbeefsha","rows":0}\n' > "$CORPUS/corpus-identity.json"
refuses_naming "1o. a corpus identity whose rows is 0 is REFUSED — a corpus with no rows is not a measurable corpus" \
  "$TMP/d-base" "'rows'" "not a measurable corpus" -- "${base_args[@]}"
mv "$TMP/identity.parked" "$CORPUS/corpus-identity.json"

mv "$BINS/flight-loadgen" "$TMP/loadgen.parked"
refuses_naming "1p. a MISSING measured binary is REFUSED, naming the binary and the remedy" \
  "$TMP/d-base" "flight-loadgen" "does not exist" "Build the binaries" -- "${base_args[@]}"
mv "$TMP/loadgen.parked" "$BINS/flight-loadgen"

# ...and the accept direction ONE MORE TIME, after all that mutation, so nothing above left the
# fixture in a state that makes the remaining cases vacuous.
out=$(run_abc "$TMP/d-base" "${base_args[@]}"); rc=$?
if [ "$rc" -eq 0 ] && grep -q "resume:   VERIFIED against" <<<"$out"; then
  pass "1q. the base configuration is STILL accepted after every mutation was reverted — no case above passed by leaving the fixture broken"
else
  fail "1q. the reverted fixture must be accepted again (rc=$rc, out: $(tail -5 <<<"$out"))"
fi

# ===========================================================================
# PART 2 — A SKIPPED SESSION MUST PROVE IT IS THE SESSION THE SLOT EXPECTS
# ===========================================================================
# `results.json` alone establishes NO provenance: it is the reporter's output and carries no
# round, no position and no arm label of this set's vocabulary. Every refusal here names the
# DIRECTORY, because that is what the operator acts on and a set is 12 to 20 of them.
WIN_OUT="$TMP/out-window"
measure_all "$WIN_OUT" 1
win_args=(--corpus "$CORPUS" --bin-dir "$BINS" --out "$WIN_OUT" --rounds 1)
out=$(run_abc "$TMP/d-base" "${win_args[@]}"); rc=$?
if [ "$rc" -eq 0 ] && [ "$(grep -c 'SKIP (measured, window VERIFIED' <<<"$out")" -eq "$NARMS" ]; then
  pass "2a. (control) all $NARMS intact sessions are ACCEPTED and skipped"
else
  fail "2a. (control) the intact window set must be accepted (rc=$rc, out: $out)"
fi

mv "$WIN_OUT/r1-B/abc-window.json" "$TMP/window.parked"
refuses_naming "2b. a session with a results.json but NO abc-window.json is REFUSED, naming the DIRECTORY" \
  "$TMP/d-base" "$WIN_OUT/r1-B" "NO abc-window.json" -- "${win_args[@]}"
mv "$TMP/window.parked" "$WIN_OUT/r1-B/abc-window.json"

mut "$WIN_OUT/r1-B/abc-window.json" arm C
refuses_naming "2c. a window recording a DIFFERENT ARM is REFUSED, naming the directory and both arms" \
  "$TMP/d-base" "$WIN_OUT/r1-B" "records arm 'C'" "says arm 'B'" -- "${win_args[@]}"
mut "$WIN_OUT/r1-B/abc-window.json" arm B

mut "$WIN_OUT/r1-C0/abc-window.json" round 7
refuses_naming "2d. a window recording a DIFFERENT ROUND is REFUSED, naming the directory and both rounds" \
  "$TMP/d-base" "$WIN_OUT/r1-C0" "records round '7'" "says round '1'" -- "${win_args[@]}"
mut "$WIN_OUT/r1-C0/abc-window.json" round 1

mut "$WIN_OUT/r1-C/abc-window.json" exit 3
refuses_naming "2e. a window recording a NON-ZERO exit is REFUSED — a failed session's leftover results.json is not a measurement" \
  "$TMP/d-base" "$WIN_OUT/r1-C" "records exit 3" -- "${win_args[@]}"
mut "$WIN_OUT/r1-C/abc-window.json" exit 0

printf 'not json\n' > "$WIN_OUT/r1-A/abc-window.json"
refuses_naming "2f. an UNREADABLE window is REFUSED (provenance UNMEASURED), naming the directory" \
  "$TMP/d-base" "$WIN_OUT/r1-A" "could not be READ" -- "${win_args[@]}"
printf '{"round":1,"arm":"A"}\n' > "$WIN_OUT/r1-A/abc-window.json"
refuses_naming "2g. a window with no \`exit\` field is REFUSED, naming the directory and the field" \
  "$TMP/d-base" "$WIN_OUT/r1-A" "carries no 'exit'" -- "${win_args[@]}"
make_measured "$WIN_OUT" 1 A
out=$(run_abc "$TMP/d-base" "${win_args[@]}"); rc=$?
if [ "$rc" -eq 0 ] && [ "$(grep -c 'SKIP (measured, window VERIFIED' <<<"$out")" -eq "$NARMS" ]; then
  pass "2h. every window mutation reverted, the set is accepted again — no case above passed on a broken fixture"
else
  fail "2h. the reverted window set must be accepted (rc=$rc, out: $out)"
fi

# ===========================================================================
# PART 3 — THE AGGREGATOR: CONFIGURATION VALIDATED OVER EVERY (round, arm)
# ===========================================================================
agg_refuses_naming() {
  local label="$1" root="$2"; shift 2
  local -a expect=("$@")
  local out rc missing="" token
  out=$(python3 "$AGG" --root "$root" --arms A,B,C0,C --baseline A 2>&1); rc=$?
  if [ "$rc" -eq 0 ]; then
    fail "$label: must REFUSE, exited 0"
    return
  fi
  for token in "${expect[@]}"; do
    grep -qF -- "$token" <<<"$out" || missing="$missing [$token]"
  done
  if [ -n "$missing" ]; then
    fail "$label: refused but did not NAME$missing (out: $(tail -3 <<<"$out"))"
    return
  fi
  pass "$label"
}

SET="$TMP/set"
mkset "$SET" 3 A,B,C0,C 400000 250000 20000 25000
out=$(python3 "$AGG" --root "$SET" --arms A,B,C0,C --baseline A); rc=$?
if [ "$rc" -eq 0 ] && grep -q "Configuration VALIDATED over every aggregated (round, arm): 12 session(s)" <<<"$out"; then
  pass "3a. (control) a clean 3-round 4-arm set is ACCEPTED and DECLARES that it validated all 12 sessions"
else
  fail "3a. (control) the clean set must be accepted (rc=$rc, out: $(tail -3 <<<"$out"))"
fi
if grep -q "SCOPE: the aggregated sessions only" <<<"$out"; then
  pass "3a. ...and it DECLARES the scope of that validation rather than leaving it to be assumed"
else
  fail "3a. the validation must declare its scope (out: $(head -12 <<<"$out"))"
fi

# --- 3b. PER-ARM TREATMENT STABILITY, one field at a time in a LATER round — which is exactly
# what the old code could not see, because it read configuration from round one only.
mut "$SET/r2-C0/results.json" pinning.flight_allocator jemalloc
agg_refuses_naming "3b. round 2 changing one arm's ALLOCATOR is REFUSED, naming the arm, the field and both values" \
  "$SET" "arm C0's TREATMENT changed" "flight_allocator" "'system'" "'jemalloc'" "r2-C0"
mut "$SET/r2-C0/results.json" pinning.flight_allocator system

mut "$SET/r3-B/results.json" pinning.flight_server_cpus 4,5
agg_refuses_naming "3c. round 3 changing one arm's FLIGHT PIN is REFUSED, naming the arm, the field and both pins" \
  "$SET" "arm B's TREATMENT changed" "flight_server_cpus" "'2,3'" "'4,5'" "r3-B"
mut "$SET/r3-B/results.json" pinning.flight_server_cpus 2,3

mut "$SET/r2-B/results.json" pinning.flight_pin_mode siblings
agg_refuses_naming "3c. a changed PIN MODE in a later round is REFUSED, naming the arm and both modes" \
  "$SET" "arm B's TREATMENT changed" "flight_pin_mode" "'distinct-cores'" "'siblings'"
mut "$SET/r2-B/results.json" pinning.flight_pin_mode distinct-cores

mut "$SET/r2-C0/results.json" pinning.flight_malloc_arena_max 4
agg_refuses_naming "3c. a changed ARENA CAP in a later round is REFUSED, naming the arm, the field and both caps" \
  "$SET" "arm C0's TREATMENT changed" "flight_malloc_arena_max" " 2," " 4."
mut "$SET/r2-C0/results.json" pinning.flight_malloc_arena_max 2

mut "$SET/r3-C/results.json" pinning.counter_mode "perf stat -C 9,9 for everything"
agg_refuses_naming "3c. a changed COUNTER MODE in a later round is REFUSED, naming the arm and the field" \
  "$SET" "arm C's TREATMENT changed" "counter_mode"
mut "$SET/r3-C/results.json" pinning.counter_mode \
  "perf stat -C 2,10 for the bare-scan arm and -C 2,3 for the Flight arm (CPU-WIDE; never -p)"

# --- 3d. CROSS-ARM INVARIANTS. The scan pin IS the drift control's own pin: if it moves
# between arms there is nothing left to read the treatment against.
mut "$SET/r1-B/results.json" pinning.server_cpus 2,11
agg_refuses_naming "3d. a SCAN PIN that differs between arms is REFUSED, naming the field, both pins and the lost drift control" \
  "$SET" "CROSS-ARM INVARIANTS" "server_cpus" "'2,10'" "'2,11'" "DRIFT CONTROL IS GONE"
mut "$SET/r1-B/results.json" pinning.server_cpus 2,10

mut "$SET/r2-C/results.json" pinning.client_cpus 6,14
agg_refuses_naming "3d. a CLIENT PIN that differs across the set is REFUSED, naming the field and both values" \
  "$SET" "CROSS-ARM INVARIANTS" "client_cpus" "'4,12'" "'6,14'"
mut "$SET/r2-C/results.json" pinning.client_cpus 4,12

mut "$SET/r3-A/results.json" corpus_identity.data_db_sha256 othersha
agg_refuses_naming "3d. a differing CORPUS IDENTITY is REFUSED, naming the field and both digests" \
  "$SET" "corpus_identity.data_db_sha256" "'abc123'" "'othersha'"
mut "$SET/r3-A/results.json" corpus_identity.data_db_sha256 abc123

mut "$SET/r2-B/results.json" binary_provenance.binaries.cqlite-flight '{"sha256":"dddd"}'
agg_refuses_naming "3d. a differing BINARY DIGEST is REFUSED, naming the binary and both digests" \
  "$SET" "binary_sha256.cqlite-flight" "'dddd'"
mut "$SET/r2-B/results.json" binary_provenance.binaries.cqlite-flight \
  '{"sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}'

# --- 3e. THE ADMISSION TRIPLE. All three fields, because the ceiling alone cannot distinguish a
# re-derivation from a pin, and `available_parallelism` is the input that moves with the mask.
mut "$SET/r1-C/results.json" flight_admission.available_parallelism 4
agg_refuses_naming "3e. a differing available_parallelism is REFUSED, naming the field, both values and WHY it is a second treatment" \
  "$SET" "ADMISSION TRIPLE" "available_parallelism" "resolve_max_concurrent_scans" "AFFINITY MASK"
mut "$SET/r1-C/results.json" flight_admission.available_parallelism 2

mut "$SET/r2-A/results.json" flight_admission.max_concurrent_scans 8
agg_refuses_naming "3e. a differing admission CEILING is REFUSED, naming the field and both values" \
  "$SET" "ADMISSION TRIPLE" "max_concurrent_scans" " 4," " 8."
mut "$SET/r2-A/results.json" flight_admission.max_concurrent_scans 4

mut "$SET/r3-C0/results.json" flight_admission.max_concurrent_scans_source pinned
agg_refuses_naming "3e. a differing admission SOURCE (derived vs pinned) is REFUSED — the ceiling alone cannot tell them apart" \
  "$SET" "ADMISSION TRIPLE" "max_concurrent_scans_source" "'derived'" "'pinned'"
mut "$SET/r3-C0/results.json" flight_admission.max_concurrent_scans_source derived

# --- 3f. AN ABSENT FIELD IS COULD-NOT-MEASURE, AND IS REFUSED WITH THE FIELD NAMED. Not
# skipped: a session recorded before a field existed is exactly the session whose treatment
# cannot be shown to match, so skipping the comparison would let this issue's own defect pass.
mut "$SET/r2-C/results.json" pinning.flight_malloc_arena_max __DELETE__
agg_refuses_naming "3f. an ABSENT pinning field is REFUSED naming the field, not silently skipped" \
  "$SET" "flight_malloc_arena_max" "NOT RECORDED" "Refused rather than skipped"
mut "$SET/r2-C/results.json" pinning.flight_malloc_arena_max null

mut "$SET/r1-A/results.json" flight_admission __DELETE__
agg_refuses_naming "3f. an ABSENT flight_admission block is REFUSED naming the field" \
  "$SET" "flight_admission" "NOT RECORDED"
mut "$SET/r1-A/results.json" flight_admission \
  '{"max_concurrent_scans":4,"max_concurrent_scans_source":"derived","available_parallelism":2}'

mut "$SET/r1-B/results.json" binary_provenance __DELETE__
agg_refuses_naming "3f. an ABSENT binary_provenance is REFUSED naming the field — the rig's output is a ratio between two binaries" \
  "$SET" "binary_provenance" "NOT RECORDED"
mut "$SET/r1-B/results.json" binary_provenance \
  '{"binaries":{"ws0-scan-bench":{"sha256":"a"},"cqlite-flight":{"sha256":"b"},"flight-loadgen":{"sha256":"c"}}}'
# ...and that substitution left DIFFERENT digests in r1-B, which the cross-arm check must catch
# on its own — so revert it properly and re-assert the clean set below.
mkset "$SET" 3 A,B,C0,C 400000 250000 20000 25000

mut "$SET/r3-B/results.json" corpus_identity.rows __DELETE__
agg_refuses_naming "3f. an ABSENT corpus row count is REFUSED naming the field" \
  "$SET" "corpus_identity" "'rows'" "NOT RECORDED"
mut "$SET/r3-B/results.json" corpus_identity.rows 1000

mut "$SET/r2-C0/results.json" pinning __DELETE__
agg_refuses_naming "3f. an ABSENT pinning block is REFUSED naming the block and the field it could not read" \
  "$SET" "not an object" "flight_server_cpus"
mkset "$SET" 3 A,B,C0,C 400000 250000 20000 25000

# --- 3g. the clean set once more, so no case above passed by leaving the fixture broken.
out=$(python3 "$AGG" --root "$SET" --arms A,B,C0,C --baseline A); rc=$?
if [ "$rc" -eq 0 ]; then
  pass "3g. the regenerated clean set is accepted again — every RED arm above differed from THIS control"
else
  fail "3g. the regenerated clean set must be accepted (rc=$rc, out: $(tail -3 <<<"$out"))"
fi

# ===========================================================================
# PART 4 — THE RATIO, ITS DIRECTION, THE CYCLES/ROW DELTA AND THE ORDERING
# ===========================================================================
# `ratio bare/flight` is an ESTABLISHED QUANTITY in this rig and it is a ROWS/S ratio:
# `ws0_report.py` computes `scan_rps / fl_rps`, `ws0-baseline.sh` printed `1.34x` for bare
# 338,090 and flight 252,789 rows/s (1.337) on a real session whose CYCLES quotient was
# 23374/19485 = 1.20, and DELTA-TABLE.md's 1.4862x is 370,134/249,041. The fixture below is
# chosen so all three candidate readings differ, because a fixture on which they agree cannot
# tell them apart:
#
#   rows/s(bare)/rows/s(flight) = 400000/250000 = 1.6000   <- the rig's definition
#   flight cpr / bare cpr       =  25000/ 20000 = 1.2500   <- what the code used to print
#   bare cpr   / flight cpr     =  20000/ 25000 = 0.8000   <- the "fixed inversion" of that
REPORT="$TMP/report.md"
python3 "$AGG" --root "$SET" --arms A,B,C0,C --baseline A --out "$REPORT" >/dev/null
got=$(cell "$REPORT" "Layer 1" A "ratio bare/flight (median)")
if [ "$got" = "1.6000x" ]; then
  pass "4a. ratio bare/flight = rows/s(bare)/rows/s(flight): 400,000/250,000 = 1.6000x, asserted numerically"
else
  fail "4a. ratio bare/flight must be 1.6000x (rows/s), got '$got'"
fi
if [ "$got" != "1.2500x" ] && [ "$got" != "0.8000x" ]; then
  pass "4a. ...and it is NEITHER cycles quotient (1.2500x, the old value) NOR its inverse (0.8000x) — the fixture separates all three"
else
  fail "4a. the printed ratio is a CYCLES quotient ('$got'), which is the wrong quantity"
fi
got=$(cell "$REPORT" "Layer 1" A "cycles/row delta (median)")
if [ "$got" = "+5,000 (+25.0%)" ]; then
  pass "4b. cycles/row delta = flight - bare: 25,000 - 20,000 = +5,000 (+25.0%), the rig's own definition and formatting"
else
  fail "4b. cycles/row delta must be '+5,000 (+25.0%)', got '$got'"
fi

# THE DIRECTION, pinned by a fixture whose FLIGHT LEG IS FASTER than the bare scan. An
# equal-valued fixture cannot distinguish the two orderings, and a bare-faster one alone cannot
# either: only a run in which the ratio must land BELOW 1 does.
FAST="$TMP/set-flight-faster"
mkset "$FAST" 3 A,B,C0,C 250000 400000 25000 20000
FAST_REPORT="$TMP/report-fast.md"
python3 "$AGG" --root "$FAST" --arms A,B,C0,C --baseline A --out "$FAST_REPORT" >/dev/null
got=$(cell "$FAST_REPORT" "Layer 1" A "ratio bare/flight (median)")
if [ "$got" = "0.6250x" ]; then
  pass "4c. a FLIGHT-FASTER set yields a ratio BELOW 1 (250,000/400,000 = 0.6250x) — the direction is pinned, not just the arithmetic"
else
  fail "4c. a flight-faster set must give 0.6250x, got '$got'"
fi
got=$(cell "$FAST_REPORT" "Layer 1" A "cycles/row delta (median)")
if [ "$got" = "-5,000 (-20.0%)" ]; then
  pass "4c. ...and its cycles/row delta is NEGATIVE (-5,000, -20.0%), which the rig deliberately allows"
else
  fail "4c. a cheaper flight arm must give '-5,000 (-20.0%)', got '$got'"
fi

# --- 4d. THE OTHER COLUMN LABELS. One inverted label means none of them had been checked, so
# every column that names a quantity is asserted to hold THAT quantity.
got=$(cell "$REPORT" "Layer 1" A "cycles/row (median)")
if [ "$got" = "25,000" ]; then
  pass "4d. Layer 1's 'cycles/row (median)' is the FLIGHT leg's cycles/row (25,000), which is what the table is about"
else
  fail "4d. Layer 1 cycles/row must be the flight leg's 25,000, got '$got'"
fi
got=$(cell "$REPORT" "drift control" A "cycles/row (median)")
if [ "$got" = "20,000" ]; then
  pass "4d. the control table's 'cycles/row (median)' is the BARE-SCAN leg's (20,000) — the two tables name the same quantity of different legs"
else
  fail "4d. the control's cycles/row must be the bare scan's 20,000, got '$got'"
fi
got=$(cell "$REPORT" "Layer 2" A "rows/s (median)")
if [ "$got" = "250,000" ]; then
  pass "4d. Layer 2's 'rows/s (median)' is the FLIGHT leg's rows/s (250,000)"
else
  fail "4d. Layer 2 rows/s must be 250,000, got '$got'"
fi
got=$(cell "$REPORT" "drift control" A "rows/s (median)")
if [ "$got" = "400,000" ]; then
  pass "4d. the control table's 'rows/s (median)' is the BARE-SCAN leg's (400,000)"
else
  fail "4d. the control's rows/s must be 400,000, got '$got'"
fi
got=$(cell "$REPORT" "Layer 2" A "row denominator (median)")
if [ "$got" = "2,000" ]; then
  pass "4d. 'row denominator (median)' is the flight leg's recorded denominator, medianed over every round rather than read from round one"
else
  fail "4d. the row denominator must be 2,000, got '$got'"
fi
if grep -q "paired Δcycles/row vs A" "$REPORT" && grep -q "paired Δrows/s vs A" "$REPORT"; then
  pass "4d. each paired-delta column NAMES ITS QUANTITY (Δcycles/row vs Δrows/s) — 'paired vs A' named neither"
else
  fail "4d. the paired columns must name their quantities (headers: $(grep -c '^|' "$REPORT") table lines)"
fi
if grep -q "direction (cycles/row vs A)" "$REPORT" && grep -q "direction (rows/s vs A)" "$REPORT"; then
  pass "4d. each direction column names the quantity whose direction it counts"
else
  fail "4d. the direction columns must name their quantity"
fi
if grep -q "rows/s spread" "$REPORT" && grep -q "cycles/row spread" "$REPORT"; then
  pass "4d. the control table's two 'spread' columns are DISAMBIGUATED — they were both headed 'spread'"
else
  fail "4d. the two spread columns must name their quantities"
fi

# --- 4e. A SINGLE-ROUND SPREAD IS NOT 0.00%. `(max-min)/median` over one value is arithmetically
# zero and reads as MEASURED tightness, which is a positive verdict from an unmeasurable input.
ONE="$TMP/set-one-round"
mkset "$ONE" 1 A,B,C0,C 400000 250000 20000 25000
ONE_REPORT="$TMP/report-one.md"
python3 "$AGG" --root "$ONE" --arms A,B,C0,C --baseline A --out "$ONE_REPORT" >/dev/null
got=$(cell "$ONE_REPORT" "Layer 1" A "cycles/row spread")
if [ "$got" = "n/a (1 round)" ]; then
  pass "4e. a ONE-ROUND set reports its spread as 'n/a (1 round)', not a measured-looking 0.00%"
else
  fail "4e. a one-round spread must read 'n/a (1 round)', got '$got'"
fi

# --- 4f. THE ORDERING IS LOAD-BEARING. The control is what makes a treatment delta readable at
# all, so it is printed BEFORE any treatment figure — not as a footnote somebody reads after
# quoting the delta.
ctl=$(grep -n '^## The drift control' "$REPORT" | cut -d: -f1)
l1=$(grep -n '^## Layer 1' "$REPORT" | cut -d: -f1)
l2=$(grep -n '^## Layer 2' "$REPORT" | cut -d: -f1)
cfg=$(grep -n '^## Configuration' "$REPORT" | cut -d: -f1)
if [ -n "$ctl" ] && [ -n "$l1" ] && [ -n "$l2" ] && [ -n "$cfg" ] \
   && [ "$ctl" -lt "$l1" ] && [ "$l1" -lt "$l2" ] && [ "$l2" -lt "$cfg" ]; then
  pass "4f. the DRIFT CONTROL table is printed FIRST (line $ctl), before Layer 1 ($l1), Layer 2 ($l2) and the configuration ($cfg)"
else
  fail "4f. the control must precede every treatment table (ctl=$ctl l1=$l1 l2=$l2 cfg=$cfg)"
fi
vld=$(grep -n '^Configuration VALIDATED' "$REPORT" | cut -d: -f1)
if [ -n "$vld" ] && [ -n "$ctl" ] && [ "$vld" -lt "$ctl" ]; then
  pass "4f. ...and the configuration VALIDATION is declared before any figure at all (line $vld)"
else
  fail "4f. the validation declaration must precede the control table (vld=$vld ctl=$ctl)"
fi
if grep -q "Control movement across arms:" "$REPORT"; then
  pass "4f. the control's own MOVEMENT is stated beside it — a delta smaller than it is not readable"
else
  fail "4f. the control movement line must be present"
fi

# --- 4g. EXISTING BEHAVIOUR THAT MUST SURVIVE THE REFACTOR: an incomplete round is dropped
# WHOLE and NAMED, and a set with no complete round is REFUSED. Both directions, because
# "dropped" and "refused" are different states and only one of them can be read as a result.
PART="$TMP/set-partial"
mkset "$PART" 3 A,B,C0,C 400000 250000 20000 25000
rm -rf "$PART/r2-C0"
out=$(python3 "$AGG" --root "$PART" --arms A,B,C0,C --baseline A); rc=$?
if [ "$rc" -eq 0 ] && grep -q "Rounds pairable: \[1, 3\]  (DROPPED, incomplete: \[2\])" <<<"$out"; then
  pass "4g. a round MISSING AN ARM is dropped WHOLE and NAMED (round 2), and the remaining pairs are still reported"
else
  fail "4g. an incomplete round must be dropped and named (rc=$rc, first line: $(head -1 <<<"$out"))"
fi
rm -rf "$PART/r1-C0" "$PART/r3-C0"
out=$(python3 "$AGG" --root "$PART" --arms A,B,C0,C --baseline A 2>&1); rc=$?
if [ "$rc" -ne 0 ] && grep -q "nothing is pairable" <<<"$out"; then
  pass "4g. a set in which NO round carries every arm is REFUSED — 'nothing is pairable', never a partial table"
else
  fail "4g. a set with no complete round must be refused (rc=$rc, out: $(tail -2 <<<"$out"))"
fi
# ...and the aggregated set is validated over the PAIRABLE rounds only, which is a decision this
# suite pins: a dropped round contributes to no figure, so refusing on its configuration would
# red an interrupted-but-correct resume.
mkset "$PART" 3 A,B,C0,C 400000 250000 20000 25000
rm -rf "$PART/r2-C0"
mut "$PART/r2-A/results.json" pinning.flight_allocator jemalloc
out=$(python3 "$AGG" --root "$PART" --arms A,B,C0,C --baseline A); rc=$?
if [ "$rc" -eq 0 ] && grep -q "8 session(s) = 2 pairable round(s)" <<<"$out"; then
  pass "4g. a DROPPED round's configuration is not examined (8 sessions validated, not 11) — refusing on it would red a correct resume"
else
  fail "4g. the validation scope must be the pairable rounds only (rc=$rc, out: $(sed -n 3p <<<"$out"))"
fi

# ==========================================================================
# A MINIMUM CHECK COUNT — this file has no `set -e`
# ==========================================================================
# Without it, a block that silently never executes (a helper returning early, a `$(...)` whose
# command vanished) LOWERS the count and registers NO failure, and the gate reads only the exit
# code. The floor is DERIVED FROM A MEASURED RUN and set below the observed count, so adding a
# case cannot red the suite. RE-DERIVE IT BY RUNNING THE SUITE at each addition, never by
# counting the source — the loops and helpers multiply, and a source estimate understated a
# floor by 29 elsewhere in this repo's history.
MIN_CHECKS=90
if [ "$checks" -lt "$MIN_CHECKS" ]; then
  echo
  echo "FAIL - only $checks check(s) ran; this suite has at least $MIN_CHECKS."
  echo "       A block that silently never executed would lower the count with no failure"
  echo "       registered, and the gate reads only the exit code."
  exit 1
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "PASS - all $checks WS0 A/B/C (run fingerprint / session provenance / configuration / ratio) checks fired as specified"
  exit 0
fi
echo "FAIL - $fails of $checks check(s) failed"
exit 1
