#!/usr/bin/env bash
#
# selftest-analyze.sh -- deterministic tests for analyze-ab.py (issue #3649).
#
# The analyzer's statistics and its verdict rule are the reviewable core of this
# artifact set, and they are tested here against SYNTHETIC JSONL fixtures
# constructed in a scratch directory -- never against a live run, and never
# against wall-clock timing (this repository lints wall-clock threshold asserts
# out of the correctness test path, #2642). Every case is reproducible on a
# laptop in seconds, so there is no excuse for publishing a figure from this
# harness without running it first.
#
# It prints a case count and a CASE FLOOR. A green tally over a silently
# shrunken suite is a known defect class in this repository (a span-replacing
# edit deleted four cases from another suite and it reported "failed: 0" at 102
# instead of 105 for a whole round), so a suite that has lost cases reds here.
#
# Usage: bash selftest-analyze.sh          (needs python3 only)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ANALYZER="$HERE/analyze-ab.py"
DRIVER="$HERE/ab-throughput.sh"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

PASSED=0
FAILED=0
CASE_FLOOR=110

ok()  { PASSED=$((PASSED + 1)); printf '  ok      %s\n' "$1"; }
bad() { FAILED=$((FAILED + 1)); printf '  BROKEN  %s\n' "$1"; }

expect() { # <description> <condition-already-evaluated:0|1>
  if [ "$2" -eq 0 ]; then ok "$1"; else bad "$1"; fi
}

# ---------------------------------------------------------------------------
# Fixture generator: a manifest plus one single-record JSONL per (arm, replicate)
# ---------------------------------------------------------------------------
cat > "$TMP/mkfixture.py" <<'PYEOF'
import json
import os
import sys

STEP = "flight-loadgen.step/v1"


def step_record(round_label, rate, duration=60.0, requests_ok=5):
    # Latency is made a plausible function of the rate so the two arms differ in
    # the latency block too; nothing in the verdict rule reads it.
    per_request_ms = (duration * 1000.0) / requests_ok
    return {
        "schema": STEP,
        "round": round_label,
        "endpoint": "http://127.0.0.1:8815",
        "ts_unix_ms": 1780000000000,
        "seed": 42,
        "step": 0,
        "target_concurrency": 1,
        "shape": "full",
        "duration_s": duration,
        "requests_ok": requests_ok,
        "requests_unavailable": 0,
        "requests_error": 0,
        "error_codes": {},
        "qps": requests_ok / duration,
        "rows_per_s": rate,
        "bytes_per_s": rate * 200.0,
        "rows_total": int(rate * duration),
        "bytes_total": int(rate * duration * 200),
        "latency_ms": {
            "p50": per_request_ms,
            "p95": per_request_ms * 1.10,
            "p99": per_request_ms * 1.20,
            "max": per_request_ms * 1.35,
            "samples": requests_ok,
        },
    }


def main():
    outdir = sys.argv[1]
    requested = int(sys.argv[2])
    spec = sys.argv[3]  # "base:head,base:head,..." one entry per replicate
    # Optional 4th arg: the concurrency ramp. Default "1" (single-stream). For a
    # multi-step ramp the spec's rate is the PEAK and it sits at the top step,
    # with lower steps scaled down -- so the peak-selection logic is exercised
    # rather than trivially satisfied by a one-element ladder.
    ramp = sys.argv[4] if len(sys.argv) > 4 else "1"
    steps = [int(v) for v in ramp.split(",")]
    os.makedirs(outdir, exist_ok=True)
    runs = []
    for index, entry in enumerate(spec.split(","), start=1):
        base_rate, head_rate = (float(v) for v in entry.split(":"))
        for arm, rate in (("base", base_rate), ("head", head_rate)):
            name = "%s-r%02d.jsonl" % (arm, index)
            scale = [0.55, 0.78, 0.91, 1.0]
            with open(os.path.join(outdir, name), "w", encoding="utf-8") as handle:
                for position, concurrency in enumerate(steps):
                    factor = 1.0 if len(steps) == 1 else scale[min(position, len(scale) - 1)]
                    if position == len(steps) - 1:
                        factor = 1.0
                    record = step_record("%s-r%02d" % (arm, index), rate * factor)
                    record["step"] = position
                    record["target_concurrency"] = concurrency
                    handle.write(json.dumps(record))
                    handle.write("\n")
            runs.append({
                "arm": arm,
                "replicate": index,
                "file": name,
                "temperature": "warm",
                "admission_observed": "16",
                "admission_source": "flag",
            })
    manifest = {
        "schema": "ab-3649.manifest/v1",
        "driver_version": "selftest-fixture",
        "generated_utc": "2026-09-01T00:00:00Z",
        "replicates_requested": requested,
        "arms": {
            "base": {"commit": "0" * 40, "ref": "cfa93fe99^"},
            "head": {"commit": "1" * 40, "ref": "cfa93fe99"},
        },
        "workload": {
            "shape": "full",
            "step_duration": "60s",
            "ramp": ramp,
            "prewarm": True,
            "server_cpus": "0,2",
            "client_cpus": "1,3",
            "temperature": "warm",
            "merge_path": "merge",
            "max_concurrent_scans": 16,
            "batch_size": 8192,
            "max_batch_bytes": "server-default",
            "admission_wait_timeout_ms": "server-default",
        },
        "corpus": {
            "path": "/data/ab-3649/corpus/sstables",
            "data_db_bytes": 681574400,
            "data_db_files": 3,
            "min_bytes_required": 268435456,
            "min_sstables_required": 2,
            "rows_declared": 3999890,
        },
        "host": {
            "instance_type": "i4i.xlarge",
            "nproc": 4,
            "loadavg1": "0.05",
            "kernel": "selftest",
        },
        "runs": runs,
    }
    with open(os.path.join(outdir, "manifest.json"), "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=1, sort_keys=True)
        handle.write("\n")


main()
PYEOF

mkfixture() { python3 "$TMP/mkfixture.py" "$@"; }

RC=0
run_analyzer() { # <dir> [extra args...]   -- the single-stream section
  local dir="$1"; shift
  set +e
  python3 "$ANALYZER" --single-stream "$dir/manifest.json" "$@" \
    > "$TMP/out.txt" 2> "$TMP/err.txt"
  RC=$?
  set -e
}

run_util() { # <dir> [extra args...]   -- the utilization section
  local dir="$1"; shift
  set +e
  python3 "$ANALYZER" --utilization "$dir/manifest.json" "$@" \
    > "$TMP/out.txt" 2> "$TMP/err.txt"
  RC=$?
  set -e
}

run_both() { # <single-stream-dir> <utilization-dir>
  set +e
  python3 "$ANALYZER" --single-stream "$1/manifest.json" --utilization "$2/manifest.json" \
    > "$TMP/out.txt" 2> "$TMP/err.txt"
  RC=$?
  set -e
}

anchored() { # every line of both streams carries the prefix
  local file
  for file in "$TMP/out.txt" "$TMP/err.txt"; do
    if [ -s "$file" ] && grep -qv '^AB-3649: ' "$file"; then
      return 1
    fi
  done
  return 0
}

# `verdict <quantity> <TOKEN>`: one line per section, the quantity in a fixed
# position, so a two-section report still has exactly one verdict per quantity.
verdict_token() { # [quantity]
  sed -n "s/^AB-3649: verdict ${1:-[a-z-]*} \([A-Z][A-Z-]*\)\$/\1/p" "$TMP/out.txt"
}

check_verdict() { # <description> <expected-token> <expected-exit> [quantity]
  local desc="$1" want="$2" want_rc="$3" quantity="${4:-}"
  local got
  got="$(verdict_token "$quantity")"
  local lines
  lines="$(printf '%s\n' "$got" | grep -c . || true)"
  if [ "$lines" != "1" ]; then
    bad "$desc (expected exactly one verdict line, got $lines)"
    return
  fi
  if [ "$got" != "$want" ]; then
    bad "$desc (verdict $got, expected $want)"
    return
  fi
  if [ "$RC" != "$want_rc" ]; then
    bad "$desc (exit $RC, expected $want_rc)"
    return
  fi
  if ! anchored; then
    bad "$desc (an output line does not carry the AB-3649 anchor)"
    return
  fi
  ok "$desc -> $want (exit $RC)"
}

check_cause() { # <description> <expected-cause>
  if grep -qE "^AB-3649: cause [a-z-]+ $2\$" "$TMP/err.txt"; then
    ok "$1 -> cause $2"
  else
    bad "$1 (no 'AB-3649: cause <quantity> $2' line; stderr: $(head -2 "$TMP/err.txt" | tr '\n' ' '))"
  fi
}

echo "==== analyze-ab.py self-test (issue #3649) ===="
echo

# ---------------------------------------------------------------------------
echo "-- the four measured verdicts --"

# 1. A clean, clearly-positive effect whose interval sits inside 1.10-1.25.
mkfixture "$TMP/meets" 6 "100000:116000,100000:117000,100000:117000,100000:118000,100000:118000,100000:119000"
run_analyzer "$TMP/meets"
check_verdict "clean positive effect, tight interval inside the band" MEETS-TARGET 0

# 2. THE CASE THIS WHOLE FILE EXISTS FOR. A large point-estimate difference with
#    heavily overlapping dispersion, in the spirit of the rejected proxy bench
#    (base 78.6 ms [69.5, 88.4] vs head 66.5 ms [54.5, 83.2]) -- expressed here
#    as throughput, since rows/s is what the served path reports. The point
#    estimate lands at ~1.16x, INSIDE the target band, and it must still NOT
#    become MEETS-TARGET: the interval is [0.97, 1.40] and covers both no-effect
#    and the band.
mkfixture "$TMP/inconclusive" 6 \
  "12723:10814.55,12723:12086.85,12723:13995.3,12723:15013.14,12723:18448.35,12723:20611.26"
run_analyzer "$TMP/inconclusive"
check_verdict "large point difference, overlapping dispersion" INCONCLUSIVE 6
if grep -q '^AB-3649: test ci-contains-1.0 yes$' "$TMP/out.txt"; then
  ok "the inconclusive case reports that its interval covers 1.0"
else
  bad "the inconclusive case did not report ci-contains-1.0"
fi
if grep -qE '^AB-3649: ratio single-stream point 1\.1[0-9]+ ' "$TMP/out.txt"; then
  ok "a point estimate sitting INSIDE the target band still does not earn a verdict"
else
  bad "the inconclusive fixture no longer has a point estimate inside the band, so it is no longer testing the case it exists for"
fi

# 3. A real, MEASURED no-effect: ratio ~1.0 with a tight interval. This is a
#    different fact from inconclusive and must not collapse onto it -- the
#    interval rules the target band out even though it straddles 1.0.
mkfixture "$TMP/noeffect" 6 \
  "100000:99900,100000:100000,100000:100100,100000:100200,100000:99950,100000:100050"
run_analyzer "$TMP/noeffect"
check_verdict "measured no effect, tight interval around 1.0" BELOW-TARGET 4
if grep -q '^AB-3649: test ci-contains-1.0 yes$' "$TMP/out.txt" \
   && grep -q '^AB-3649: test ci-entirely-below-band yes$' "$TMP/out.txt"; then
  ok "the no-effect case is distinguished from the inconclusive one by its own test lines"
else
  bad "the no-effect case did not report both covering 1.0 and excluding the band"
fi

# 4. The 1.5-1.9 region. It must render against the 1.10-1.25 BAND, with the
#    ceiling merely named. There is no verdict token that endorses the ceiling.
mkfixture "$TMP/ceiling" 6 \
  "100000:169000,100000:170000,100000:170000,100000:171000,100000:171000,100000:172000"
run_analyzer "$TMP/ceiling"
check_verdict "a ratio in the 1.5-1.9 region" ABOVE-TARGET 5
if grep -q '^AB-3649: target profile narrow band \[1.10, 1.25\] source ' "$TMP/out.txt"; then
  ok "the 1.5-1.9 case is still rendered against the 1.10-1.25 band"
else
  bad "the 1.5-1.9 case did not print the 1.10-1.25 band"
fi
if grep -q '^AB-3649: ceiling 1.5-1.9x is a rig-narrow UTILIZATION ceiling' "$TMP/out.txt"; then
  ok "the ceiling is named on the run that lands in it"
else
  bad "the ceiling was not named"
fi
if grep -qE 'MEETS-CEILING|CEILING-MET|MEETS-UTILIZATION' "$TMP/out.txt"; then
  bad "the output invented a ceiling-endorsing token"
else
  ok "no ceiling-endorsing verdict token exists in the output"
fi

# The wide profile has its own band and the same rule.
run_analyzer "$TMP/meets" --profile wide
check_verdict "the wide profile tests the 1.05-1.10 band, not the narrow one" ABOVE-TARGET 5

echo
echo "-- every input the analyzer cannot measure, cause by cause --"

# 5. Malformed JSONL.
mkfixture "$TMP/malformed" 6 "100000:110000,100000:110000,100000:110000,100000:110000,100000:110000,100000:110000"
printf '{"schema": "flight-loadgen.step/v1", NOT JSON\n' > "$TMP/malformed/head-r03.jsonl"
run_analyzer "$TMP/malformed"
check_verdict "a malformed JSONL record" UNMEASURED 7
check_cause "malformed JSONL" run-file-not-jsonl

# 6. A missing run file the manifest still declares.
mkfixture "$TMP/missing" 6 "100000:110000,100000:110000,100000:110000,100000:110000,100000:110000,100000:110000"
rm -f "$TMP/missing/base-r02.jsonl"
run_analyzer "$TMP/missing"
check_verdict "a declared run file that is not on disk" UNMEASURED 7
check_cause "missing run file" run-file-unreadable

# 7. An empty run file (the shape a killed loadgen leaves).
mkfixture "$TMP/empty" 6 "100000:110000,100000:110000,100000:110000,100000:110000,100000:110000,100000:110000"
: > "$TMP/empty/head-r01.jsonl"
run_analyzer "$TMP/empty"
check_verdict "an empty run file" UNMEASURED 7
check_cause "empty run file" run-file-empty

# 8. An unpaired replicate: interleaving is the whole design, so a lone arm is
#    not analysable.
mkfixture "$TMP/unpaired" 6 "100000:110000,100000:110000,100000:110000,100000:110000,100000:110000,100000:110000"
python3 - "$TMP/unpaired/manifest.json" <<'PYEOF'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["runs"] = [r for r in manifest["runs"] if not (r["arm"] == "head" and r["replicate"] == 4)]
manifest["replicates_requested"] = 6
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYEOF
run_analyzer "$TMP/unpaired"
check_verdict "a replicate present for one arm only" UNMEASURED 7
check_cause "unpaired replicate" unpaired-replicates

# 9. Fewer completed pairs than the session requested -- the shortfall reaches
#    the analyzer as an explicit manifest fact, never as an absence.
mkfixture "$TMP/short" 8 "100000:117000,100000:117000,100000:118000,100000:118000"
run_analyzer "$TMP/short"
check_verdict "fewer completed pairs than requested" UNMEASURED 7
check_cause "replicate shortfall" replicate-shortfall

# 10. Too few pairs to bootstrap at all.
mkfixture "$TMP/tiny" 2 "100000:117000,100000:118000"
run_analyzer "$TMP/tiny"
check_verdict "a pair count below the bootstrap floor" UNMEASURED 7
check_cause "insufficient pairs" insufficient-pairs

# 11. A replicate that recorded a request error is not a throughput measurement.
mkfixture "$TMP/errors" 6 "100000:110000,100000:110000,100000:110000,100000:110000,100000:110000,100000:110000"
python3 - "$TMP/errors/head-r02.jsonl" <<'PYEOF'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    record = json.loads(handle.read())
record["requests_error"] = 1
record["error_codes"] = {"Internal": 1}
with open(path, "w", encoding="utf-8") as handle:
    handle.write(json.dumps(record) + "\n")
PYEOF
run_analyzer "$TMP/errors"
check_verdict "a replicate carrying a request error" UNMEASURED 7
check_cause "request error in a replicate" run-errors

# 12. Admission shedding changes what was measured (#2420).
mkfixture "$TMP/shed" 6 "100000:110000,100000:110000,100000:110000,100000:110000,100000:110000,100000:110000"
python3 - "$TMP/shed/base-r05.jsonl" <<'PYEOF'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    record = json.loads(handle.read())
record["requests_unavailable"] = 3
with open(path, "w", encoding="utf-8") as handle:
    handle.write(json.dumps(record) + "\n")
PYEOF
run_analyzer "$TMP/shed"
check_verdict "a replicate that was admission-shed" UNMEASURED 7
check_cause "admission shed in a replicate" run-shed

# 13. A zero-row scan: green by omission is the failure this repository refuses.
mkfixture "$TMP/zerorows" 6 "100000:110000,100000:110000,100000:110000,100000:110000,100000:110000,100000:110000"
python3 - "$TMP/zerorows/head-r06.jsonl" <<'PYEOF'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    record = json.loads(handle.read())
record["rows_per_s"] = 0.0
record["rows_total"] = 0
with open(path, "w", encoding="utf-8") as handle:
    handle.write(json.dumps(record) + "\n")
PYEOF
run_analyzer "$TMP/zerorows"
check_verdict "a replicate that returned no rows" UNMEASURED 7
check_cause "zero-row replicate" run-degenerate

# 14. A multi-step ramp is not this design.
mkfixture "$TMP/multistep" 6 "100000:110000,100000:110000,100000:110000,100000:110000,100000:110000,100000:110000"
cat "$TMP/multistep/base-r01.jsonl" "$TMP/multistep/base-r01.jsonl" > "$TMP/multistep/tmp.jsonl"
mv "$TMP/multistep/tmp.jsonl" "$TMP/multistep/base-r01.jsonl"
run_analyzer "$TMP/multistep"
check_verdict "a run file holding more than one step record" UNMEASURED 7
check_cause "multi-step run file" run-record-count

# 15/16/17. Manifest-level refusals.
mkdir -p "$TMP/nomanifest"
run_analyzer "$TMP/nomanifest"
check_verdict "an absent manifest" UNMEASURED 7
check_cause "absent manifest" manifest-unreadable

mkdir -p "$TMP/badmanifest"
printf 'this is not json\n' > "$TMP/badmanifest/manifest.json"
run_analyzer "$TMP/badmanifest"
check_verdict "a manifest that is not JSON" UNMEASURED 7
check_cause "manifest that is not JSON" manifest-not-json

mkfixture "$TMP/wrongschema" 6 "100000:110000,100000:110000,100000:110000,100000:110000,100000:110000,100000:110000"
python3 - "$TMP/wrongschema/manifest.json" <<'PYEOF'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["schema"] = "ab-3649.manifest/v0"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYEOF
run_analyzer "$TMP/wrongschema"
check_verdict "a manifest carrying an unknown schema tag" UNMEASURED 7
check_cause "unknown manifest schema" manifest-schema

echo
echo "-- determinism, anchoring, and the structural property of the source --"

# 18. Same input twice -> byte-identical output on both streams.
run_analyzer "$TMP/meets"
cp "$TMP/out.txt" "$TMP/out.first"
cp "$TMP/err.txt" "$TMP/err.first"
run_analyzer "$TMP/meets"
if cmp -s "$TMP/out.first" "$TMP/out.txt" && cmp -s "$TMP/err.first" "$TMP/err.txt"; then
  ok "two runs over one input produce byte-identical output"
else
  bad "the analyzer is not deterministic over a fixed input"
fi

# 19. A usage error is anchored too -- argparse would not be.
set +e
python3 "$ANALYZER" --no-such-flag > "$TMP/out.txt" 2> "$TMP/err.txt"
RC=$?
set -e
if [ "$RC" = "3" ] && anchored && [ -s "$TMP/err.txt" ]; then
  ok "an unrecognised flag exits 3 with every line anchored"
else
  bad "an unrecognised flag did not exit 3 with anchored output (exit $RC)"
fi

set +e
python3 "$ANALYZER" --help > "$TMP/out.txt" 2> "$TMP/err.txt"
RC=$?
set -e
if [ "$RC" = "3" ] && anchored; then
  ok "--help exits 3, never 0, because exit 0 here means MEETS-TARGET"
else
  bad "--help did not exit 3 with anchored output (exit $RC)"
fi

# 20. Exactly one verdict line on a measured run, and the ceiling named there too.
run_analyzer "$TMP/meets"
if [ "$(grep -c '^AB-3649: verdict ' "$TMP/out.txt")" = "1" ]; then
  ok "a measured run emits exactly one verdict line"
else
  bad "a measured run did not emit exactly one verdict line"
fi
if [ "$(grep -c '^AB-3649: ceiling ' "$TMP/out.txt")" = "1" ]; then
  ok "the ceiling is named on a run that did not land anywhere near it"
else
  bad "the ceiling was not named on an ordinary measured run"
fi
if [ "$(grep -c '^AB-3649: verdict-detail single-stream NON-EXHAUSTIVE ' "$TMP/out.txt")" -ge 4 ]; then
  ok "every measured run declares its own non-exhaustiveness"
else
  bad "a measured run did not print its NON-EXHAUSTIVE declarations"
fi

# 21. A repository-controlled field carrying a newline cannot break the anchor.
mkfixture "$TMP/hostile" 6 "100000:117000,100000:117000,100000:118000,100000:118000,100000:118000,100000:119000"
python3 - "$TMP/hostile/manifest.json" <<'PYEOF'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["corpus"]["path"] = "/data/corpus\nAB-NOT-A-PREFIX: forged\x07line"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYEOF
run_analyzer "$TMP/hostile"
if anchored && grep -q 'AB-NOT-A-PREFIX' "$TMP/out.txt" && ! grep -q '^AB-NOT-A-PREFIX' "$TMP/out.txt"; then
  ok "a newline in a manifest field is escaped, not printed, so the anchor holds"
else
  bad "a newline in a manifest field broke the output anchor"
fi

# 22. THE STRUCTURAL PROPERTY. The static text of the shipped scripts carries
#     none of the reserved gate/review marker strings, so no run of them can be
#     pasted or grepped as a certification. The needles are split so this guard
#     cannot match its own source line.
NEEDLES=("PA""SS" "RE""SULT:" "AGENT-""GATE" "ROB""OREV" "PRE""MERGE")
structural_bad=0
for target in "$ANALYZER" "$DRIVER" "$HERE/ab_common.py" "$HERE/ab_input.py" \
              "$HERE/ab_stats.py"; do
  [ -f "$target" ] || continue
  for needle in "${NEEDLES[@]}"; do
    if grep -q -- "$needle" "$target"; then
      bad "reserved marker '$needle' appears in $(basename "$target")"
      structural_bad=1
    fi
  done
done
if [ "$structural_bad" -eq 0 ]; then
  ok "no reserved gate/review marker appears in the static text of the shipped scripts"
fi

# 23. Both shipped scripts parse.
if python3 -m py_compile "$ANALYZER" 2>/dev/null; then
  ok "analyze-ab.py compiles"
else
  bad "analyze-ab.py does not compile"
fi
if [ -f "$DRIVER" ] && bash -n "$DRIVER"; then
  ok "ab-throughput.sh parses"
else
  bad "ab-throughput.sh is absent or does not parse"
fi

echo
echo "-- the UTILIZATION quantity: a direction, never an attainment --"

# The ramp fixtures put the peak at the top of the ladder, so peak SELECTION is
# exercised rather than trivially satisfied by a one-element ladder.
RAMP='1,2,4,8'

mkfixture "$TMP/util-rise" 6 \
  "100000:132000,100000:134000,100000:134000,100000:136000,100000:136000,100000:138000" "$RAMP"
run_util "$TMP/util-rise"
check_verdict "utilization throughput that rose measurably" RISES 0 utilization
if grep -q '^AB-3649: pair 1 ladder-compared 1,2,4,8 base-peak-at-concurrency 8 head-peak-at-concurrency 8$' "$TMP/out.txt"; then
  ok "the utilization section reports the ladder compared and where each peak fell"
else
  bad "the utilization section did not report the ladder and peak concurrency"
fi
if grep -q '^AB-3649: excluded-steps 0 RECOGNISED$' "$TMP/out.txt"; then
  ok "a clean ramp reports 0 RECOGNISED exclusions, never a bare 0"
else
  bad "a clean ramp did not print an affirmative zero-exclusion line"
fi

mkfixture "$TMP/util-fall" 6 \
  "100000:88000,100000:87000,100000:87000,100000:86000,100000:86000,100000:85000" "$RAMP"
run_util "$TMP/util-fall"
check_verdict "utilization throughput that fell measurably" FALLS 4 utilization

mkfixture "$TMP/util-flat" 6 \
  "100000:85000,100000:99000,100000:118000,100000:132000,100000:92000,100000:145000" "$RAMP"
run_util "$TMP/util-flat"
check_verdict "utilization throughput with no established direction" INCONCLUSIVE 6 utilization

# THE STRUCTURAL SEPARATION. The 1.5-1.9x ceiling is not a target, so the
# utilization section must be incapable of emitting a band verdict, and the
# single-stream section incapable of emitting a direction verdict.
run_util "$TMP/util-rise"
if grep -qE '^AB-3649: verdict utilization (MEETS-TARGET|ABOVE-TARGET|BELOW-TARGET)$' "$TMP/out.txt"; then
  bad "the utilization section emitted a target-band verdict token"
else
  ok "the utilization section cannot emit a target-band verdict token"
fi
if grep -q '^AB-3649: target NONE-BY-DESIGN ' "$TMP/out.txt"; then
  ok "the utilization section states that no band or ceiling reaches its rule"
else
  bad "the utilization section did not declare its rule threshold-free"
fi
if grep -q '^AB-3649: ceiling 1.5-1.9x is a rig-narrow UTILIZATION ceiling' "$TMP/out.txt"; then
  ok "the ceiling is NAMED in the utilization section"
else
  bad "the utilization section did not name the ceiling"
fi
if grep -qE 'MEETS-CEILING|CEILING-MET|REACHES-CEILING|ceiling-attained' "$TMP/out.txt"; then
  bad "the utilization output invented a ceiling-attainment token"
else
  ok "no ceiling-attainment token exists anywhere in the utilization output"
fi
run_analyzer "$TMP/meets"
if grep -qE '^AB-3649: verdict single-stream (RISES|FALLS)$' "$TMP/out.txt"; then
  bad "the single-stream section emitted a direction verdict token"
else
  ok "the single-stream section cannot emit a direction verdict token"
fi

# A manifest handed to the wrong section is a real mistake and gets its own cause.
run_util "$TMP/meets"
check_verdict "a --ramp 1 manifest supplied as the utilization section" UNMEASURED 7 utilization
check_cause "ramp-1 manifest in the utilization section" mode-manifest-mismatch
run_analyzer "$TMP/util-rise"
check_verdict "a ramp manifest supplied as the single-stream section" UNMEASURED 7 single-stream
check_cause "ramp manifest in the single-stream section" mode-manifest-mismatch

echo
echo "-- admission control (#2420) cannot be silently averaged into a throughput --"

shed_step() { # <dir> <arm> <replicate> <0-based-step> <count>
  python3 - "$1/$2-r$(printf '%02d' "$3").jsonl" "$4" "$5" <<'PYINNER'
import json
import sys

path, step, count = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
with open(path, encoding="utf-8") as handle:
    records = [json.loads(line) for line in handle if line.strip()]
records[step]["requests_unavailable"] = count
with open(path, "w", encoding="utf-8") as handle:
    for record in records:
        handle.write(json.dumps(record) + "\n")
PYINNER
}

# Shed the top step on BOTH arms of every replicate: the ladder stays matched,
# the shed steps are excluded, and each exclusion is named.
mkfixture "$TMP/util-shed" 6 \
  "100000:132000,100000:134000,100000:134000,100000:136000,100000:136000,100000:138000" "$RAMP"
for r in 1 2 3 4 5 6; do
  shed_step "$TMP/util-shed" base "$r" 3 7
  shed_step "$TMP/util-shed" head "$r" 3 7
done
run_util "$TMP/util-shed"
check_verdict "a ramp whose top step was admission-shed on both arms" RISES 0 utilization
if grep -q '^AB-3649: excluded-step replicate 1 arm base concurrency 8 requests-unavailable 7 reason admission-shed-2420$' "$TMP/out.txt"; then
  ok "each admission-shed step is excluded and reported as an explicit fact"
else
  bad "an admission-shed step was not reported as an explicit exclusion"
fi
if grep -q '^AB-3649: excluded-steps 12 RECOGNISED$' "$TMP/out.txt"; then
  ok "the exclusion count is reported, so a silently shrunken ladder is visible"
else
  bad "the exclusion count was not reported"
fi
if grep -q '^AB-3649: pair 1 ladder-compared 1,2,4 base-peak-at-concurrency 4 ' "$TMP/out.txt"; then
  ok "the peak is taken over the SURVIVING ladder, not the declared one"
else
  bad "the peak was not taken over the surviving ladder"
fi

# Shed a DIFFERENT step on each arm: the ladders no longer match, so a peak
# taken over them is not a ratio.
mkfixture "$TMP/util-asym" 6 \
  "100000:132000,100000:134000,100000:134000,100000:136000,100000:136000,100000:138000" "$RAMP"
for r in 1 2 3 4 5 6; do
  shed_step "$TMP/util-asym" base "$r" 3 7
done
run_util "$TMP/util-asym"
check_verdict "arms whose surviving ladders differ" UNMEASURED 7 utilization
check_cause "mismatched surviving ladders" ramp-steps-not-comparable

# Every step shed: the run measured the admission ceiling and nothing else.
mkfixture "$TMP/util-allshed" 6 \
  "100000:132000,100000:134000,100000:134000,100000:136000,100000:136000,100000:138000" "$RAMP"
for r in 1 2 3 4 5 6; do
  for step in 0 1 2 3; do
    shed_step "$TMP/util-allshed" base "$r" "$step" 3
    shed_step "$TMP/util-allshed" head "$r" "$step" 3
  done
done
run_util "$TMP/util-allshed"
check_verdict "a ramp in which every step was shed" UNMEASURED 7 utilization
check_cause "a fully shed ramp" ramp-fully-shed

# At single-stream concurrency a shed can only mean something is badly wrong, so
# there it is a refusal rather than an exclusion.
run_analyzer "$TMP/shed"
check_verdict "a shed at single-stream concurrency" UNMEASURED 7 single-stream
check_cause "shed at concurrency 1" run-shed

# The arms must have been served under the SAME admission ceiling.
mkfixture "$TMP/adm-mismatch" 6 "100000:117000,100000:117000,100000:118000,100000:118000,100000:118000,100000:119000"
python3 - "$TMP/adm-mismatch/manifest.json" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for entry in manifest["runs"]:
    if entry["arm"] == "head":
        entry["admission_observed"] = "8"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
run_analyzer "$TMP/adm-mismatch"
check_verdict "arms served under different admission ceilings" UNMEASURED 7 single-stream
check_cause "differing admission ceilings" admission-mismatch

# An unobservable ceiling is disclosed, not assumed to agree.
mkfixture "$TMP/adm-unobs" 6 "100000:117000,100000:117000,100000:118000,100000:118000,100000:118000,100000:119000"
python3 - "$TMP/adm-unobs/manifest.json" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for entry in manifest["runs"]:
    entry["admission_observed"] = "NOT-OBSERVED"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
run_analyzer "$TMP/adm-unobs"
check_verdict "an admission ceiling the driver could not observe" MEETS-TARGET 0 single-stream
if grep -q '^AB-3649: verdict-detail single-stream ADMISSION ' "$TMP/out.txt"; then
  ok "an unobserved admission ceiling is disclosed as uncorroborated, not assumed"
else
  bad "an unobserved admission ceiling was passed off as corroborated"
fi
if grep -q '^AB-3649: admission max-concurrent-scans requested 16 observed NOT-OBSERVED ' "$TMP/out.txt"; then
  ok "requested and observed admission values are printed side by side"
else
  bad "the admission line did not print requested and observed separately"
fi

echo
echo "-- both sections in one report --"

run_both "$TMP/meets" "$TMP/util-flat"
if [ "$(grep -c '^AB-3649: verdict ' "$TMP/out.txt")" = "2" ]; then
  ok "a two-section report carries exactly two verdict lines"
else
  bad "a two-section report did not carry exactly two verdict lines"
fi
if [ "$(verdict_token single-stream)" = "MEETS-TARGET" ] \
   && [ "$(verdict_token utilization)" = "INCONCLUSIVE" ]; then
  ok "each section's verdict is keyed by its own quantity"
else
  bad "the two sections' verdicts are not separately addressable"
fi
if [ "$RC" = "6" ]; then
  ok "with both sections the exit is the largest, so the least affirmative governs"
else
  bad "a two-section exit was $RC, expected 6 (the larger of 0 and 6)"
fi
if [ "$(grep -c '^AB-3649: ==== section ' "$TMP/out.txt")" = "2" ] \
   && [ "$(grep -c '^AB-3649: ---- end section ' "$TMP/out.txt")" = "2" ]; then
  ok "the two sections are delimited so neither can be read as the other"
else
  bad "the two sections are not delimited"
fi

# One unusable session must never suppress the other.
run_both "$TMP/malformed" "$TMP/util-rise"
if [ "$(verdict_token single-stream)" = "UNMEASURED" ] \
   && [ "$(verdict_token utilization)" = "RISES" ] && [ "$RC" = "7" ]; then
  ok "an UNMEASURED section does not suppress the section that did measure"
else
  bad "an UNMEASURED section suppressed the other section (rc=$RC)"
fi

run_both "$TMP/meets" "$TMP/util-rise"
cp "$TMP/out.txt" "$TMP/both.first"
run_both "$TMP/meets" "$TMP/util-rise"
if cmp -s "$TMP/both.first" "$TMP/out.txt"; then
  ok "a two-section report is deterministic too"
else
  bad "a two-section report is not deterministic"
fi

set +e
python3 "$ANALYZER" --profile narrow > "$TMP/out.txt" 2> "$TMP/err.txt"
RC=$?
set -e
if [ "$RC" = "3" ] && anchored; then
  ok "neither section requested is a usage error, not an empty report"
else
  bad "an invocation naming no section did not exit 3 (exit $RC)"
fi

echo
echo "-- the #3058 single-source fast path cannot silently null the measurement --"

# With one source on disk and no pinned merge arm, #3058 routes every request
# onto a fast path #2820 never touched -- on BOTH arms -- so the ratio is 1.0 by
# construction. That is a measurement of nothing and must not render a verdict.
mkfixture "$TMP/bypassed" 6 "100000:117000,100000:117000,100000:118000,100000:118000,100000:118000,100000:119000"
python3 - "$TMP/bypassed/manifest.json" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["workload"]["merge_path"] = "auto"
manifest["corpus"]["data_db_files"] = 1
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
run_analyzer "$TMP/bypassed"
check_verdict "one source with no pinned merge arm" UNMEASURED 7
check_cause "the #3058 fast path served both arms" merge-path-bypassed

# With several sources the arm cannot be settled from the manifest, so this is a
# disclosure rather than a refusal.
mkfixture "$TMP/unpinned" 6 "100000:117000,100000:117000,100000:118000,100000:118000,100000:118000,100000:119000"
python3 - "$TMP/unpinned/manifest.json" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["workload"]["merge_path"] = "auto"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
run_analyzer "$TMP/unpinned"
check_verdict "several sources with no pinned merge arm still renders" MEETS-TARGET 0
if grep -q '^AB-3649: verdict-detail single-stream MERGE-PATH ' "$TMP/out.txt"; then
  ok "an unpinned merge arm is disclosed beside the verdict"
else
  bad "an unpinned merge arm was not disclosed"
fi

run_analyzer "$TMP/meets"
if grep -q '^AB-3649: merge-path merge$' "$TMP/out.txt" \
   && ! grep -q '^AB-3649: verdict-detail single-stream MERGE-PATH ' "$TMP/out.txt"; then
  ok "a pinned merge arm is recorded and carries no disclosure"
else
  bad "a pinned merge arm was not recorded cleanly"
fi

echo
echo "-- a CONTROL session may render a verdict, but never a discharging one --"

# A null / sensitivity control is still a real measurement, so it must produce a
# token; what it must NOT do is read as if it discharged the acceptance criteria.
mkfixture "$TMP/control" 6 "100000:116000,100000:117000,100000:117000,100000:118000,100000:118000,100000:119000"
python3 - "$TMP/control/manifest.json" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["control"] = "sensitivity"
manifest["server_extra"] = {"base": "", "head": "--max-batch-bytes 1"}
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
run_analyzer "$TMP/control"
check_verdict "a labelled control session still renders a token" MEETS-TARGET 0
if grep -q "^AB-3649: control sensitivity$" "$TMP/out.txt"; then
  ok "the control label is printed on its own line"
else
  bad "the control label was not printed"
fi
if grep -q '^AB-3649: verdict-detail single-stream CONTROL this session is labelled ' "$TMP/out.txt"; then
  ok "a control session says in its own output that it does not discharge the criteria"
else
  bad "a control session did not disclaim discharging the criteria"
fi
if grep -q '^AB-3649: verdict-detail single-stream CONTROL the two arms were served under DIFFERENT ' "$TMP/out.txt"; then
  ok "asymmetric per-arm server flags are disclosed beside the verdict"
else
  bad "asymmetric per-arm server flags were not disclosed"
fi
if grep -q '^AB-3649: arm head server-extra \[--max-batch-bytes 1\]$' "$TMP/out.txt"; then
  ok "the injected handicap is printed verbatim"
else
  bad "the injected handicap was not printed"
fi

# An ordinary measurement says so, and carries neither disclaimer.
run_analyzer "$TMP/meets"
if grep -q '^AB-3649: control none$' "$TMP/out.txt" \
   && ! grep -q '^AB-3649: verdict-detail single-stream CONTROL ' "$TMP/out.txt"; then
  ok "an ordinary measurement reports control none and carries no control disclaimer"
else
  bad "an ordinary measurement was mislabelled as a control"
fi

echo
echo "-- the driver's fail-closed guards, exercised without a rig --"

# The driver refuses long before it builds anything, so its pre-flight guards are
# testable on any box. These are the guards RUNBOOK.md's procedure depends on.
run_driver() { # <args...>
  set +e
  bash "$DRIVER" "$@" > "$TMP/out.txt" 2> "$TMP/err.txt"
  RC=$?
  set -e
}

check_driver() { # <description> <expected-exit> [expected-cause]
  local desc="$1" want_rc="$2" want_cause="${3:-}"
  if [ "$RC" != "$want_rc" ]; then
    bad "$desc (exit $RC, expected $want_rc)"
    return
  fi
  if ! anchored; then
    bad "$desc (an output line does not carry the AB-3649 anchor)"
    return
  fi
  if [ -n "$want_cause" ] && ! grep -q "^AB-3649: cause $want_cause$" "$TMP/err.txt"; then
    bad "$desc (cause '$want_cause' absent; stderr: $(head -2 "$TMP/err.txt" | tr '\n' ' '))"
    return
  fi
  ok "$desc -> exit $RC${want_cause:+ cause $want_cause}"
}

if [ ! -f "$DRIVER" ]; then
  bad "the driver is absent, so none of its guards could be exercised"
else
  # Two sources, so the #3058 SSTable-count guard is satisfied and the guards
  # under test are the ones each case names.
  mkdir -p "$TMP/tinycorpus/ks/tbl"
  head -c 4096 /dev/zero > "$TMP/tinycorpus/ks/tbl/nb-1-big-Data.db"
  head -c 4096 /dev/zero > "$TMP/tinycorpus/ks/tbl/nb-2-big-Data.db"
  mkdir -p "$TMP/onesstcorpus/ks/tbl"
  head -c 4096 /dev/zero > "$TMP/onesstcorpus/ks/tbl/nb-1-big-Data.db"
  mkdir -p "$TMP/emptycorpus"
  printf '{"version": 2, "keyspace": "ks", "table": "tbl"}\n' > "$TMP/ticket.json"

  run_driver --help
  check_driver "the driver --help exits 3, never 0" 3

  run_driver --no-such-flag
  check_driver "an unrecognised driver flag exits 3" 3

  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" --max-concurrent-scans 4 --replicates 2
  check_driver "the driver refuses fewer than 3 replicates" 3

  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json"
  check_driver "the driver refuses to run without a pinned admission ceiling" 3

  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --ramp 1,2,4,8
  check_driver "the driver refuses a ramp that tops out above the admission pin" 3

  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" --max-concurrent-scans 4 --server-cpus 0,2
  check_driver "the driver refuses a server CPU set with no client CPU set" 3

  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" --max-concurrent-scans 4 \
    --server-cpus 0,2 --client-cpus 2,3 --work-dir "$TMP/w-overlap" --min-corpus-bytes 1
  check_driver "the driver refuses overlapping server and client CPU sets" 3

  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/nope.json" --max-concurrent-scans 4 \
    --work-dir "$TMP/w-tpl"
  check_driver "an absent ticket template" 2 ticket-template-absent

  run_driver --corpus "$TMP/emptycorpus" --ticket-template "$TMP/ticket.json" --max-concurrent-scans 4 \
    --work-dir "$TMP/w-empty" --min-corpus-bytes 1
  check_driver "a corpus holding no Data.db files" 2 corpus-empty

  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" --max-concurrent-scans 4 \
    --work-dir "$TMP/w-small"
  check_driver "a corpus below the stated minimum size" 2 corpus-too-small

  run_driver --corpus "$TMP/onesstcorpus" --ticket-template "$TMP/ticket.json" --max-concurrent-scans 4 \
    --work-dir "$TMP/w-onesst" --min-corpus-bytes 1
  check_driver "a single-SSTable corpus, which #3058 would route past the merge" \
    2 corpus-too-few-sstables

  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" --max-concurrent-scans 4 \
    --work-dir "$TMP/w-badarm" --min-corpus-bytes 1 --merge-path sideways
  check_driver "an unrecognised --merge-path value" 3

  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" --max-concurrent-scans 4 \
    --work-dir "$TMP/w-same" --min-corpus-bytes 1 --repo "$HERE" \
    --base-ref HEAD --head-ref HEAD
  check_driver "two arm refs resolving to the same commit" 2 arm-refs-identical

  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" --max-concurrent-scans 4 \
    --work-dir "$TMP/w-badref" --min-corpus-bytes 1 --repo "$HERE" \
    --base-ref no-such-rev-3649 --head-ref HEAD
  check_driver "an arm ref that resolves to nothing" 2 arm-ref-unresolvable

  # A refusal must still leave a manifest, so the analyzer sees the shortfall as
  # a fact rather than as an absence.
  if [ -f "$TMP/w-badref/results/manifest.json" ]; then
    ok "an aborted session still writes a manifest recording what completed"
  else
    bad "an aborted session left no manifest, so a shortfall would read as an absence"
  fi
  run_analyzer "$TMP/w-badref/results"
  check_verdict "the analyzer refuses an aborted session's manifest" UNMEASURED 7
fi

# ---------------------------------------------------------------------------
ACCOUNTED=$((PASSED + FAILED))
echo
echo "==== self-test tally ===="
printf 'cases ok: %d   broken: %d   accounted: %d (floor %d)\n' \
  "$PASSED" "$FAILED" "$ACCOUNTED" "$CASE_FLOOR"
if [ "$ACCOUNTED" -lt "$CASE_FLOOR" ]; then
  echo "case-floor: only $ACCOUNTED cases were accounted against a floor of $CASE_FLOOR."
  echo "Cases are being skipped or dying silently, and a zero-broken tally over a"
  echo "shrunken suite is exactly the vacuous green this floor exists to catch."
  exit 1
fi
if [ "$FAILED" -ne 0 ]; then
  echo "The analyzer is not behaving as its verdict rule requires. Do not publish a"
  echo "verdict from it until every case is ok."
  exit 1
fi
echo "Every case behaved as the verdict rule requires, including the two that must"
echo "NOT produce a number: the overlapping-dispersion case and every unmeasurable"
echo "input."
exit 0
