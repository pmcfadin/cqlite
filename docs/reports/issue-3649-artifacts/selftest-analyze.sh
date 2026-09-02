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
CASE_FLOOR=409

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
            # Counterbalanced by replicate parity, exactly as the driver does:
            # base first on odd replicates, head first on even ones.
            if index % 2 == 1:
                position = 1 if arm == "base" else 2
            else:
                position = 2 if arm == "base" else 1
            runs.append({
                "arm": arm,
                "replicate": index,
                "file": name,
                "temperature": "warm",
                "admission_observed": "16",
                "admission_source": "flag",
                "batch_size_observed": "8192",
                "max_batch_bytes_observed": "4194304",
                "wait_timeout_ms_observed": "30000",
                "position_in_pair": position,
                "loadgen_commit": "2" * 40,
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
        "loadgen": {"commit": "2" * 40, "ref": "cfa93fe99"},
        "workload": {
            "shape": "full",
            "step_duration": "60s",
            "step_duration_seconds": 60.0,
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
            "compressed": True,
            "storage": "LOCAL",
            "storage_detail": "nvme0n1 Amazon EC2 NVMe Instance Storage",
            "rows_declared": 3999890,
        },
        "host": {
            "instance_type": "i4i.xlarge",
            "nproc": 4,
            "loadavg1": "0.05",
            "load_limit": "2.00",
            "contention": "QUIET",
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

check_remedy_shared() { # <description>
  local line missing
  line="$(grep '^AB-3649: verdict-detail single-stream ADMISSION-REMEDY ' "$TMP/out.txt" || true)"
  missing=''
  case "$line" in *"ONLY while the rig is live"*) ;; *) missing="$missing time-window" ;; esac
  case "$line" in *"server.log"*)                 ;; *) missing="$missing log-path" ;; esac
  case "$line" in *"lost with the instance"*)     ;; *) missing="$missing logs-die-with-rig" ;; esac
  case "$line" in *"not evidence the arms disagreed"*) ;; *) missing="$missing honest-scope" ;; esac
  if [ -z "$missing" ]; then
    ok "$1"
  else
    bad "$1 -- the remedy line has lost:$missing"
  fi
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

# THE STRUCTURAL MOVE OF ROUND 9: one resolver, and the driver reads nothing
# else. Asserted structurally, because the failure mode is a NEW option added
# later that bypasses it -- which no behavioural case can anticipate.
if python3 - "$DRIVER" "$HERE" <<'PYINNER'
import re
import sys

sys.path.insert(0, sys.argv[2])
import ab_driver_support as S

source = open(sys.argv[1], encoding="utf-8").read()
uncommented = re.sub(r"^\s*#.*$", "", source, flags=re.M)
problems = []
if "resolve-session" not in source:
    problems.append("the driver does not call resolve-session")
if re.search(r"\beval\b", uncommented):
    problems.append("the driver uses `eval` on a value it did not construct")
# THE SUBJECT SET IS DERIVED, NOT CURATED. Every `--option)` arm in the driver's
# own dispatch must carry a disposition -- so adding an option without deciding
# whether it reaches the resolver reds here, which a hand-written list of
# "server-configuration options" could never do. A curated list inside the guard
# would be a second place to forget an option, i.e. the exact failure the
# resolver exists to remove.
accepted = set(re.findall(r"^\s*(--[a-z-]+)\)", source, re.M))
if len(accepted) < 20:
    problems.append("only %d options were derived from the dispatch; the "
                    "derivation has broken" % len(accepted))
for flag in sorted(accepted):
    if flag not in S.OPTION_DISPOSITION:
        problems.append(
            "%s is accepted by the driver but has no disposition: decide whether "
            "it reaches the resolver" % flag)
for flag, (state, why) in sorted(S.OPTION_DISPOSITION.items()):
    if state not in ("resolver-input", "not-server-config"):
        problems.append("%s has an unknown disposition %r" % (flag, state))
    if not why.strip():
        problems.append("%s has an empty reason" % flag)
    if flag not in accepted:
        problems.append("%s has a disposition but the driver does not accept it" % flag)
    if (state == "resolver-input") != (flag in S.RESOLVER_INPUTS):
        problems.append(
            "%s is dispositioned %r but %s in RESOLVER_INPUTS"
            % (flag, state, "IS" if flag in S.RESOLVER_INPUTS else "is NOT"))
for problem in problems:
    sys.stderr.write("AB-3649: %s\n" % problem)
raise SystemExit(1 if problems else 0)
PYINNER
then
  ok "every option the driver accepts is dispositioned, and the resolver is its only configuration producer"
else
  bad "the session configuration has more than one producer (see stderr above)"
fi

# 22. THE STRUCTURAL PROPERTY. The static text of the shipped scripts carries
#     none of the reserved gate/review marker strings, so no run of them can be
#     pasted or grepped as a certification. The needles are split so this guard
#     cannot match its own source line.
NEEDLES=("PA""SS" "RE""SULT:" "AGENT-""GATE" "ROB""OREV" "PRE""MERGE")
structural_bad=0
for target in "$ANALYZER" "$DRIVER" "$HERE/ab_common.py" "$HERE/ab_input.py" \
              "$HERE/ab_stats.py" "$HERE/ab_driver_support.py"; do
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
if python3 -m py_compile "$HERE/ab_driver_support.py" 2>/dev/null; then
  ok "ab_driver_support.py compiles"
else
  bad "ab_driver_support.py does not compile"
fi
# A comment asserting a mechanism that does not exist is the decay this repo
# lints for: the driver's helpers are a FILE now, so nothing may claim they are
# read out of the script by a sed extraction.
if grep -q 'sed' "$DRIVER" && grep -qi 'extract' "$DRIVER"; then
  bad "the driver still claims its helpers are extracted by sed"
else
  ok "the driver makes no claim about a sed extraction of its helpers"
fi

echo
echo "-- the DRIVER's record validator, which nothing used to execute --"

# This is the section whose absence let a 110-case green suite coexist with a
# driver that hard-coded ONE step record while advertising --ramp. run_one needs
# a rig; its validator does not, and it now lives in an executable file.
SUPPORT="$HERE/ab_driver_support.py"

mkstep() { # <out> <round> <concurrency> [rate] [shed] [duration]
  python3 - "$1" "$2" "$3" "${4:-100000}" "${5:-0}" "${6:-60}" <<'PYINNER'
import json
import sys

path, label, concurrency, rate, shed, duration = sys.argv[1:7]
rate, duration = float(rate), float(duration)
record = {
    "schema": "flight-loadgen.step/v1",
    "round": label,
    "endpoint": "http://127.0.0.1:8815",
    "ts_unix_ms": 1780000000000,
    "seed": 42,
    "step": 0,
    "target_concurrency": int(concurrency),
    "shape": "full",
    "duration_s": duration,
    "requests_ok": 5,
    "requests_unavailable": int(shed),
    "requests_error": 0,
    "error_codes": {},
    "qps": 5 / duration,
    "rows_per_s": rate,
    "bytes_per_s": rate * 200.0,
    "rows_total": int(rate * duration),
    "bytes_total": int(rate * duration * 200),
    "latency_ms": {"p50": 1.0, "p95": 1.1, "p99": 1.2, "max": 1.3, "samples": 5},
}
with open(path, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(record) + "\n")
PYINNER
}

run_support() { # <args...>
  set +e
  python3 "$SUPPORT" "$@" > "$TMP/out.txt" 2> "$TMP/err.txt"
  RC=$?
  set -e
}

check_support() { # <description> <expected-exit> [expected-cause]
  local desc="$1" want_rc="$2" want_cause="${3:-}"
  if [ "$RC" != "$want_rc" ]; then
    bad "$desc (exit $RC, expected $want_rc)"
    return
  fi
  if ! anchored; then
    bad "$desc (an output line does not carry the AB-3649 anchor)"
    return
  fi
  if [ -n "$want_cause" ] && ! grep -q "^AB-3649: cause $want_cause\$" "$TMP/err.txt"; then
    bad "$desc (cause '$want_cause' absent; stderr: $(head -2 "$TMP/err.txt" | tr '\n' ' '))"
    return
  fi
  ok "$desc -> exit $RC${want_cause:+ cause $want_cause}"
}

# THE CASE THAT WOULD HAVE CAUGHT IT: a four-step replicate against a four-step
# ramp. The old validator refused this, so every --ramp 1,2,4,8 session died
# after two release builds and a full measurement pass.
rm -f "$TMP/ramp.jsonl"
for c in 1 2 4 8; do mkstep "$TMP/ramp.jsonl" base-r01 "$c"; done
run_support validate-replicate "$TMP/ramp.jsonl" base-r01 1,2,4,8
check_support "a four-step replicate against a four-step ramp" 0
if [ "$(grep -c '^AB-3649: run base-r01 step ' "$TMP/out.txt")" = "4" ]; then
  ok "every step of a ramp replicate is reported, not just the first"
else
  bad "the validator did not report every step of a ramp replicate"
fi

rm -f "$TMP/one.jsonl"; mkstep "$TMP/one.jsonl" base-r01 1
run_support validate-replicate "$TMP/one.jsonl" base-r01 1
check_support "a one-step replicate against a --ramp 1 session" 0

run_support validate-replicate "$TMP/one.jsonl" base-r01 1,2,4,8
check_support "one record where the ramp declares four" 1 replicate-invalid
run_support validate-replicate "$TMP/ramp.jsonl" base-r01 1
check_support "four records where the ramp declares one" 1 replicate-invalid

run_support validate-replicate "$TMP/ramp.jsonl" head-r01 1,2,4,8
check_support "a replicate whose round label names the other arm" 1 replicate-invalid

rm -f "$TMP/wrongc.jsonl"
for c in 1 2 4 16; do mkstep "$TMP/wrongc.jsonl" base-r01 "$c"; done
run_support validate-replicate "$TMP/wrongc.jsonl" base-r01 1,2,4,8
check_support "a step whose concurrency is not the declared one" 1 replicate-invalid

rm -f "$TMP/shed1.jsonl"; mkstep "$TMP/shed1.jsonl" base-r01 1 100000 4
run_support validate-replicate "$TMP/shed1.jsonl" base-r01 1
check_support "a shed at single-stream concurrency is fatal to the driver" 1 replicate-invalid

# On a ramp the analyzer EXCLUDES shed steps, so the driver must not contradict
# it by dying -- it says so loudly instead.
rm -f "$TMP/shedr.jsonl"
mkstep "$TMP/shedr.jsonl" base-r01 1
mkstep "$TMP/shedr.jsonl" base-r01 2
mkstep "$TMP/shedr.jsonl" base-r01 4
mkstep "$TMP/shedr.jsonl" base-r01 8 100000 6
run_support validate-replicate "$TMP/shedr.jsonl" base-r01 1,2,4,8
check_support "a shed ramp step is reported, not fatal, so the two agree" 0
if grep -q '^AB-3649: run base-r01 step 3 concurrency 8 SHED requests-unavailable 6' "$TMP/out.txt"; then
  ok "the driver names the shed step and says the analyzer will exclude it"
else
  bad "the driver did not name the shed step"
fi

rm -f "$TMP/inf.jsonl"
python3 - "$TMP/inf.jsonl" <<'PYINNER'
import json
import sys

record = {
    "schema": "flight-loadgen.step/v1", "round": "base-r01", "target_concurrency": 1,
    "duration_s": 60.0, "requests_ok": 5, "requests_unavailable": 0,
    "requests_error": 0, "rows_per_s": float("inf"), "rows_total": 1,
    "latency_ms": {"p50": 1.0},
}
with open(sys.argv[1], "w", encoding="utf-8") as handle:
    handle.write(json.dumps(record) + "\n")
PYINNER
run_support validate-replicate "$TMP/inf.jsonl" base-r01 1
check_support "a non-finite rate refused by the driver too" 1 replicate-invalid

# The duration grammar MIRRORS flight-loadgen's (tools/flight-loadgen/src/ramp.rs:224).
# A stricter grammar refuses work that has already been done; a looser one makes
# the docstring's "field for field" claim false. Both directions are pinned.
for good_dur in "60 60.0" "60s 60.0" "500ms 0.5" "2m 120.0" "0.5 0.5" "1e18 1e+18"; do
  set -- $good_dur
  run_support parse-duration "$1"
  if [ "$RC" = "0" ] && [ "$(cat "$TMP/out.txt")" = "$2" ]; then
    ok "the duration grammar accepts '$1' as flight-loadgen does"
  else
    bad "the duration grammar refused '$1', which flight-loadgen accepts (got '$(cat "$TMP/out.txt")')"
  fi
done
for bad_dur in "0" "-5s" "nope" "nan" "inf" "1e30" "1e308m" "-1"; do
  run_support parse-duration "$bad_dur"
  if [ "$RC" = "1" ] && anchored; then
    ok "the duration grammar refuses '$bad_dur' as flight-loadgen does"
  else
    bad "the duration grammar accepted '$bad_dur' (exit $RC)"
  fi
done

# A ticket that narrows the scan cannot receive a full-scan verdict.
printf '{"version":2,"keyspace":"ks","table":"t","ddl":"CREATE TABLE ks.t (a int PRIMARY KEY)","limit":null,"predicates":[],"filter":null,"aggregation":null,"columns":null,"token_start":null,"token_end":null,"wraparound":false}\n' > "$TMP/tk-full.json"
run_support validate-ticket "$TMP/tk-full.json"
check_support "a full-ring, unprojected, unfiltered ticket" 0
# THE VALIDATOR MUST NOT BE STRICTER THAN THE SERVER. Read from
# cqlite-flight/src/ticket.rs rather than assumed:
#   * `wraparound` is retained for wire compatibility and NOT CONSULTED since
#     #3634 (ticket.rs:244-255), so refusing a ticket for setting it would reject
#     a template the server accepts;
#   * EQUAL endpoints derive `start >= end`, giving `token > start OR token <= end`
#     -- every token, the full ring expressed as a Cassandra wraparound range;
#   * an EXPLICIT (i64::MIN, i64::MAX] is half-open and therefore drops the token
#     equal to i64::MIN, which is a real token (#3633). Not the full ring.
printf '{"version":2,"keyspace":"ks","table":"t","ddl":"CREATE TABLE ks.t (a int PRIMARY KEY)","wraparound":true}\n' > "$TMP/tk-wrap.json"
run_support validate-ticket "$TMP/tk-wrap.json"
check_support "a ticket setting the ignored wraparound flag" 0
printf '{"version":2,"keyspace":"ks","table":"t","ddl":"CREATE TABLE ks.t (a int PRIMARY KEY)","token_start":5,"token_end":5}\n' > "$TMP/tk-eq.json"
run_support validate-ticket "$TMP/tk-eq.json"
check_support "a ticket whose equal token endpoints ARE the whole ring" 0
# `pathsafe::validate_snapshot` REJECTS an empty name, so accepting one here
# burned both builds before the load generator refused it -- the same rig
# economics as the relative work directory. Sixth validator-versus-consumer.
printf '{"version":2,"keyspace":"ks","table":"t","ddl":"CREATE TABLE ks.t (a int PRIMARY KEY)","snapshot":""}\n' > "$TMP/tk-emptysnap.json"
run_support validate-ticket "$TMP/tk-emptysnap.json"
check_support "a ticket with an empty snapshot name, which the server rejects" 1 ticket-identifier-invalid
printf '{"version":2,"keyspace":"ks","table":"t","ddl":"CREATE TABLE ks.t (a int PRIMARY KEY)","snapshot":"snap-1_a"}\n' > "$TMP/tk-snap.json"
run_support validate-ticket "$TMP/tk-snap.json"
check_support "a ticket with a valid snapshot name" 0
# EACH NARROWING IS WELL-FORMED, so this loop tests the FULL-RING half and not
# the schema half. Three of these used to be malformed (`"filter":"x>1"`,
# `"aggregation":"count"`, a predicate with no `column`), which the four-field
# validator could not see -- so they were refused for the RIGHT VERDICT with the
# WRONG REASON, and the loop silently proved less than it claimed.
for narrowing in '"limit":100' '"predicates":[{"column":"a","op":"Equal","value":1}]' '"filter":{"Compare":{"column":"a","op":"Gt","value":1}}' '"aggregation":{"aggregates":[]}' '"columns":["a"]' '"token_start":42' '"token_end":42' '"token_start":-9223372036854775808,"token_end":9223372036854775807'; do
  printf '{"version":2,"keyspace":"ks","table":"t","ddl":"CREATE TABLE ks.t (a int PRIMARY KEY)",%s}\n' "$narrowing" > "$TMP/tk-bad.json"
  run_support validate-ticket "$TMP/tk-bad.json"
  if [ "$RC" = "1" ] && grep -q '^AB-3649: cause ticket-not-full-ring$' "$TMP/err.txt"; then
    ok "a ticket with ${narrowing:0:44} is refused as not a full-ring scan"
  else
    bad "a ticket with ${narrowing:0:44} was accepted as a full scan (exit $RC)"
  fi
done

# Requested pinning and EFFECTIVE pinning are different facts.
MY_CPUS="$(awk '/^Cpus_allowed_list:/ {print $2}' /proc/$$/status 2>/dev/null || true)"
if [ -n "$MY_CPUS" ]; then
  run_support check-affinity "$$" "$MY_CPUS"
  if [ "$RC" = "0" ] && [ "$(cat "$TMP/out.txt")" = "VERIFIED" ]; then
    ok "check-affinity verifies a process against its real allowed CPU set"
  else
    bad "check-affinity did not verify a correct pin (exit $RC, '$(cat "$TMP/out.txt")')"
  fi
  run_support check-affinity "$$" "999"
  if [ "$RC" = "1" ] && grep -q '^AB-3649: cause affinity-mismatch$' "$TMP/err.txt"; then
    ok "check-affinity refuses a pin the process does not actually have"
  else
    bad "check-affinity accepted a pin the process does not have (exit $RC)"
  fi
else
  ok "check-affinity skipped: this platform exposes no Cpus_allowed_list (declared, not assumed)"
fi

# The startup sweep reads every echoed field, not only the resolved one.
printf '%s\n' '2026-09-01T10:00:00Z  INFO cqlite_flight: cqlite-flight starting listen=127.0.0.1:8815 batch_size=8192 max_batch_bytes=4194304 max_inflight_egress_bytes=12582912 max_concurrent_scans=16 max_concurrent_scans_source=flag available_parallelism=4 admission_wait_timeout_ms=30000 max_concurrent_streams=100' > "$TMP/startup-full.log"
sweep_bad=''
for probe in "batch-size 8192" "max-batch-bytes 4194304" "wait-timeout-ms 30000"; do
  set -- $probe
  [ "$(python3 "$SUPPORT" parse-startup "$TMP/startup-full.log" "$1")" = "$2" ] || sweep_bad="$sweep_bad $1"
done
if [ "$(python3 "$SUPPORT" parse-startup "$TMP/startup-full.log" scans)" != "16" ]; then
  sweep_bad="$sweep_bad scans-adjacency"
fi
if [ -z "$sweep_bad" ]; then
  ok "every echoed server field is read back from the real startup line, and max_concurrent_streams beside it is not mistaken for max_concurrent_scans"
else
  bad "the startup sweep could not read:$sweep_bad"
fi

# FINDING 2: the constructed argv must name each option EXACTLY ONCE. The
# project's Clap command does not enable self-overrides, so a duplicate is a
# parse failure -- and this is checkable WITHOUT the binary, which is what makes
# it worth having, because no stub reproduces Clap.
argv_dupe_bad=''
for extra in "" "--max-batch-bytes 1" "--batch-size 1" "--admission-wait-timeout-ms 5000"; do
  set +e
  argv="$(python3 "$SUPPORT" server-argv /b/cqlite-flight /d 127.0.0.1:0 8192 4194304 30000 16 "$extra" 2>/dev/null)"
  argv_rc=$?
  set -e
  if [ "$argv_rc" != "0" ]; then
    argv_dupe_bad="$argv_dupe_bad [extra='$extra' refused]"
    continue
  fi
  dupes="$(printf '%s\n' "$argv" | grep '^--' | sort | uniq -d)"
  [ -z "$dupes" ] || argv_dupe_bad="$argv_dupe_bad [extra='$extra' dupes=$(printf '%s' "$dupes" | tr '\n' ',')]"
done
if [ -z "$argv_dupe_bad" ]; then
  ok "the constructed server argv names every option exactly once, whatever the per-arm extras"
else
  bad "the constructed argv repeats an option, which the real Clap rejects:$argv_dupe_bad"
fi
run_support server-argv /b/cqlite-flight /d 127.0.0.1:0 8192 4194304 30000 16 "--nope 1"
check_support "an unrecognised per-arm option, which could only be appended" 1 server-extra-unrecognised
run_support server-argv /b/cqlite-flight /d 127.0.0.1:0 8192 4194304 30000 16 "--max-batch-bytes"
check_support "a per-arm option with no value" 1 server-extra-unrecognised
# FINDING 3 (round 9): `--max-concurrent-scans` was declared per-arm overridable
# and validated globally, so any effective override failed at run time. Rejected
# rather than threaded through, because two arms admitted at different
# concurrencies shed at different ramp steps and the analyzer already refuses the
# resulting mismatched ladders -- a per-arm ceiling cannot produce a comparable
# measurement, so there is nothing to thread.
run_support server-argv /b/cqlite-flight /d 127.0.0.1:0 8192 4194304 30000 16 "--max-concurrent-scans 4"
check_support "a per-arm admission ceiling, which cannot be comparable" 1 server-extra-unrecognised

# FINDING 1: the server must not inherit this shell's environment. Structural,
# because the failure is a variable that is ABSENT here and present on a rig.
if python3 - "$DRIVER" <<'PYINNER'
import re
import sys

source = open(sys.argv[1], encoding="utf-8").read()
block = re.search(r"local -a server_env=\((.*?)\n  \)", source, re.S)
problems = []
if not block:
    problems.append("no server_env allowlist found")
else:
    body = block.group(1)
    if "env -i" not in body:
        problems.append("the server environment is not built with `env -i`")
    if "RUST_LOG=info" not in body:
        problems.append("RUST_LOG is not pinned to an INFO-capable filter")
    # Every admitted entry must be an explicit assignment, never a bare
    # pass-through of whatever the caller had.
    for name in re.findall(r'"([A-Z_]+)=', body):
        if name.startswith("CQLITE_") and name != "CQLITE_FLIGHT_MERGE_PATH":
            problems.append("unpinned CQLite variable admitted: %s" % name)
if re.search(r'server_env\+=\(\s*"?\$\{?[A-Za-z_]', source):
    problems.append("the allowlist is extended with an unquoted caller value")
for problem in problems:
    sys.stderr.write("AB-3649: %s\n" % problem)
raise SystemExit(1 if problems else 0)
PYINNER
then
  ok "the server is launched under `env -i` with a named allowlist and a pinned RUST_LOG"
else
  bad "the server would inherit this shell's environment (see stderr above)"
fi

# THE WITHIN-PAIR ORDER RULE, EXECUTED. This is the one driver decision whose
# failure mode is a confident wrong answer rather than an error: if base always
# ran first, a drift inside a pair would land on the head arm every time and bias
# every ratio in one direction, with every statistical test still passing. It
# lives in the helper precisely so it can be run here.
order_bad=''
for probe in "1 base head" "2 head base" "3 base head" "4 head base" "7 base head"; do
  set -- $probe
  [ "$(python3 "$SUPPORT" pair-order "$1")" = "$2 $3" ] || order_bad="$order_bad rep$1"
done
if [ -z "$order_bad" ]; then
  ok "the within-pair order alternates with replicate parity"
else
  bad "the within-pair order rule is wrong for:$order_bad"
fi
# ...and the property that matters is the BALANCE it produces over a session.
balance_report="$(
  base_first=0; head_first=0
  for r in $(seq 1 8); do
    case "$(python3 "$SUPPORT" pair-order "$r")" in
      base*) base_first=$((base_first + 1)) ;;
      *)     head_first=$((head_first + 1)) ;;
    esac
  done
  echo "$base_first $head_first"
)"
if [ "$balance_report" = "4 4" ]; then
  ok "over an even replicate count the two orderings run exactly as often"
else
  bad "eight replicates did not balance the two orderings ($balance_report)"
fi
run_support pair-order 0
check_support "pair-order refuses a non-positive replicate" 2

for bad_ramp in "1,abc" "1,²" "2" "4,2" "1,1" "0"; do
  run_support validate-ramp "$bad_ramp"
  if [ "$RC" = "1" ] && anchored; then
    ok "the ramp validator refuses '$bad_ramp'"
  else
    bad "the ramp validator accepted '$bad_ramp' (exit $RC)"
  fi
done
run_support validate-ramp "1,2,4,8"
if [ "$RC" = "0" ] && [ "$(cat "$TMP/out.txt")" = "8 utilization" ]; then
  ok "a valid ramp yields its top step and the section that can consume it"
else
  bad "the ramp validator did not report top and section for a valid ramp"
fi
run_support validate-ramp "1"
if [ "$RC" = "0" ] && [ "$(cat "$TMP/out.txt")" = "1 single-stream" ]; then
  ok "--ramp 1 maps to the single-stream section"
else
  bad "--ramp 1 did not map to the single-stream section"
fi

printf '%s\n' '2026-09-01T10:00:00Z  INFO cqlite_flight: cqlite-flight starting listen=127.0.0.1:8815 batch_size=8192 max_batch_bytes=4194304 max_inflight_egress_bytes=12582912 max_concurrent_scans=16 max_concurrent_scans_source=flag available_parallelism=4 admission_wait_timeout_ms=30000 max_concurrent_streams=100' > "$TMP/startup.log"
if [ "$(python3 "$SUPPORT" parse-startup "$TMP/startup.log" scans)" = "16" ] \
   && [ "$(python3 "$SUPPORT" parse-startup "$TMP/startup.log" source)" = "flag" ]; then
  ok "the startup parser reads the ceiling and its provenance from a real line"
else
  bad "the startup parser did not read the plain-format startup line"
fi
printf '%s\n' '{"fields":{"max_concurrent_scans":8,"max_concurrent_scans_source":"derived"},"message":"cqlite-flight starting"}' > "$TMP/startup.json"
if [ "$(python3 "$SUPPORT" parse-startup "$TMP/startup.json" scans)" = "8" ] \
   && [ "$(python3 "$SUPPORT" parse-startup "$TMP/startup.json" source)" = "derived" ]; then
  ok "the startup parser also reads a JSON-formatted startup line"
else
  bad "the startup parser did not read a JSON startup line"
fi
if [ "$(python3 "$SUPPORT" parse-startup /nonexistent scans)" = "NOT-OBSERVED" ] \
   && [ "$(printf 'nothing here\n' > "$TMP/quiet.log"; python3 "$SUPPORT" parse-startup "$TMP/quiet.log" scans)" = "NOT-OBSERVED" ]; then
  ok "an unreadable or silent server log yields NOT-OBSERVED, never a value"
else
  bad "the startup parser invented a value it could not read"
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
# REVISED RULING (round 10): a session where NOTHING was observed has no evidence
# about the configuration it ran under, so it is UNMEASURED rather than a
# decisive verdict behind a disclosure. Partial stays disclosed -- there the
# observed runs constrain the unobserved ones.
run_analyzer "$TMP/adm-unobs"
check_verdict "a session where no startup line was read at all" UNMEASURED 7 single-stream
check_cause "no startup line observed anywhere" startup-unobserved


echo
echo "-- nothing non-finite may reach a verdict rule --"

poison() { # <dir> <arm> <replicate> <field> <python-literal>
  python3 - "$1/$2-r$(printf '%02d' "$3").jsonl" "$4" "$5" <<'PYINNER'
import json
import sys

path, field, literal = sys.argv[1], sys.argv[2], sys.argv[3]
value = {"inf": float("inf"), "-inf": float("-inf"), "nan": float("nan")}.get(literal)
if value is None:
    value = float(literal)
with open(path, encoding="utf-8") as handle:
    records = [json.loads(line) for line in handle if line.strip()]
# Preserve the field's JSON type: poisoning an integer field with a float would
# trip the shape check instead of the property under test.
existing = records[0].get(field)
if isinstance(existing, int) and not isinstance(existing, bool) and value == value \
        and value not in (float("inf"), float("-inf")):
    value = int(value)
records[0][field] = value
with open(path, "w", encoding="utf-8") as handle:
    for record in records:
        handle.write(json.dumps(record) + "\n")
PYINNER
}

for poison_value in inf nan; do
  mkfixture "$TMP/nf-$poison_value" 6 "100000:117000,100000:117000,100000:118000,100000:118000,100000:118000,100000:119000"
  poison "$TMP/nf-$poison_value" head 3 rows_per_s "$poison_value"
  run_analyzer "$TMP/nf-$poison_value"
  check_verdict "a rows_per_s of $poison_value" UNMEASURED 7 single-stream
  check_cause "rows_per_s of $poison_value" run-non-finite
done

# `inf > 0` is TRUE, which is how this used to reach the rule. Pin the predicate
# itself, since it is the one line the whole guard rests on.
if python3 - "$HERE" <<'PYINNER'
import sys

sys.path.insert(0, sys.argv[1])
import ab_stats as S

bad = [float("inf"), float("-inf"), float("nan"), 0.0, -1.0]
good = [1e-300, 1.0, 1e300]
raise SystemExit(
    0 if all(not S.is_usable_ratio(v) for v in bad)
    and all(S.is_usable_ratio(v) for v in good) else 1
)
PYINNER
then
  ok "is_usable_ratio rejects inf, -inf, NaN, zero and negative, and accepts finite positives"
else
  bad "is_usable_ratio does not gate the values that reach the verdict rule"
fi

# A ratio can be non-finite from two FINITE, internally consistent operands.
mkfixture "$TMP/nf-ratio" 6 "100000:117000,100000:117000,100000:118000,100000:118000,100000:118000,100000:119000"
python3 - "$TMP/nf-ratio" <<'PYINNER'
import json
import os
import sys

root = sys.argv[1]
# Both records stay finite AND internally consistent (rows_per_s x duration_s ==
# rows_total, duration inside the declared band), yet head/base overflows.
for name, rate, duration, rows in (
    ("base-r01.jsonl", 1.0 / 240.0, 240.0, 1),
    ("head-r01.jsonl", 2.9e306, 60.0, int(2.9e306 * 60.0)),
):
    path = os.path.join(root, name)
    with open(path, encoding="utf-8") as handle:
        record = json.loads(handle.read())
    record["rows_per_s"] = rate
    record["duration_s"] = duration
    record["rows_total"] = rows
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(record) + "\n")
PYINNER
run_analyzer "$TMP/nf-ratio"
check_verdict "a ratio that overflows from two finite operands" UNMEASURED 7 single-stream
check_cause "an overflowing ratio" ratio-non-finite

echo
echo "-- an n=3 bootstrap is the observed RANGE, not an interval --"

# At n=3 the all-minimum resample has probability 1/27 = 3.7%, which exceeds the
# 2.5% tail, so the bounds ARE (min, max). The refusal is an affirmative
# measurement of that -- it compares the interval the bootstrap actually
# returned -- so it keeps working if someone changes n, the tail or the floor.
mkfixture "$TMP/degen3" 3 "100000:116000,100000:118000,100000:120000"
run_analyzer "$TMP/degen3" --min-pairs 3
check_verdict "three pairs, where the bootstrap contributes nothing" UNMEASURED 7 single-stream
check_cause "an n=3 bootstrap" bootstrap-degenerate

# Identical ratios are degenerate at ANY n: a zero-width interval lands inside
# whatever band contains the point.
mkfixture "$TMP/degen-flat" 6 "100000:117000,100000:117000,100000:117000,100000:117000,100000:117000,100000:117000"
run_analyzer "$TMP/degen-flat"
check_verdict "six identical pairs, giving a zero-width interval" UNMEASURED 7 single-stream
check_cause "a zero-width interval" bootstrap-degenerate

# The floor moved to 5, so the old default of 3 is now refused before any
# statistics are computed.
mkfixture "$TMP/four" 4 "100000:117000,100000:117000,100000:118000,100000:118000"
run_analyzer "$TMP/four"
check_verdict "four pairs against the new floor of five" UNMEASURED 7 single-stream
check_cause "four pairs" insufficient-pairs

# ...and five NON-identical pairs is a real interval again.
mkfixture "$TMP/five" 5 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000"
run_analyzer "$TMP/five"
check_verdict "five distinct pairs" MEETS-TARGET 0 single-stream

echo
echo "-- declared versus observed, applied to the step records themselves --"

mkfixture "$TMP/rec" 6 "100000:117000,100000:117000,100000:118000,100000:118000,100000:118000,100000:119000"
poison "$TMP/rec" head 2 target_concurrency 32
run_analyzer "$TMP/rec"
check_verdict "a record whose concurrency is not the declared one" UNMEASURED 7 single-stream
check_cause "a concurrency the manifest does not declare" ramp-order-mismatch

mkfixture "$TMP/rec2" 6 "100000:117000,100000:117000,100000:118000,100000:118000,100000:118000,100000:119000"
poison "$TMP/rec2" base 4 duration_s 1.5
run_analyzer "$TMP/rec2"
check_verdict "a 1.5-second record under a declared 60-second step" UNMEASURED 7 single-stream
check_cause "a step duration nothing like the declared one" step-duration-mismatch

mkfixture "$TMP/rec3" 6 "100000:117000,100000:117000,100000:118000,100000:118000,100000:118000,100000:119000"
poison "$TMP/rec3" head 5 rows_total 12
run_analyzer "$TMP/rec3"
check_verdict "a record whose rows_total contradicts its own rate" UNMEASURED 7 single-stream
check_cause "an internally inconsistent record" record-internally-inconsistent

mkfixture "$TMP/rec4" 6 "100000:117000,100000:117000,100000:118000,100000:118000,100000:118000,100000:119000"
python3 - "$TMP/rec4/head-r02.jsonl" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    record = json.loads(handle.read())
record["round"] = "base-r02"
with open(path, "w", encoding="utf-8") as handle:
    handle.write(json.dumps(record) + "\n")
PYINNER
run_analyzer "$TMP/rec4"
check_verdict "a file filed under the arm its own round label denies" UNMEASURED 7 single-stream
check_cause "a mislabelled replicate file" round-label-mismatch

echo
echo "-- nothing escapes as an unanchored traceback --"

# Each of these used to raise out of run_section: exit 1, an unprefixed
# traceback, and a section with NO verdict line -- worse than a wrong verdict,
# because nothing downstream can even detect it.
mkfixture "$TMP/tb1" 6 "100000:117000,100000:117000,100000:118000,100000:118000,100000:118000,100000:119000"
python3 - "$TMP/tb1/manifest.json" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["arms"]["base"] = "commit cfa93fe99^"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
run_analyzer "$TMP/tb1"
check_verdict "an arms entry that is a string, not an object" UNMEASURED 7 single-stream
check_cause "a string where an object was expected" manifest-field

mkfixture "$TMP/tb2" 6 "100000:117000,100000:117000,100000:118000,100000:118000,100000:118000,100000:119000"
python3 - "$TMP/tb2/manifest.json" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
# "²".isdigit() is True and int() of it raises -- a manifest field that used
# to escape as a traceback.
manifest["workload"]["ramp"] = "1,²"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
run_analyzer "$TMP/tb2"
check_verdict "a ramp containing a superscript two" UNMEASURED 7 single-stream
check_cause "a non-ASCII digit in the ramp" manifest-field

mkfixture "$TMP/tb3" 6 "100000:117000,100000:117000,100000:118000,100000:118000,100000:118000,100000:119000"
python3 - "$TMP/tb3/manifest.json" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
del manifest["workload"]["step_duration_seconds"]
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
run_analyzer "$TMP/tb3"
check_verdict "a manifest with no canonical step duration" UNMEASURED 7 single-stream
check_cause "an absent canonical step duration" manifest-field

# THE FALSE-REFUSAL CASE. `--step-duration 60` is valid to flight-loadgen (bare
# means seconds), so a FINISHED session must not be declined over a suffix the
# load generator never required. The driver normalises at pre-flight, so the raw
# string can be anything the loadgen accepts.
mkfixture "$TMP/bareseconds" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
python3 - "$TMP/bareseconds/manifest.json" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["workload"]["step_duration"] = "60"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
run_analyzer "$TMP/bareseconds"
check_verdict "a bare-seconds step duration, which flight-loadgen accepts" MEETS-TARGET 0 single-stream

echo
echo "-- partial admission observation is not agreement --"

mkfixture "$TMP/adm-partial" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
python3 - "$TMP/adm-partial/manifest.json" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for entry in manifest["runs"]:
    if entry["arm"] == "head":
        entry["admission_observed"] = "NOT-OBSERVED"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
run_analyzer "$TMP/adm-partial"
check_verdict "half the runs observed, half not" MEETS-TARGET 0 single-stream
if grep -q 'corroboration partial (6 of 12 runs)' "$TMP/out.txt"; then
  ok "the admission line counts which runs actually corroborated the ceiling"
else
  bad "partial admission observation was not counted"
fi
if grep -q '^AB-3649: admission max-concurrent-scans requested 16 observed 16 corroboration partial ' "$TMP/out.txt"; then
  ok "requested and observed admission values are printed side by side, with the corroboration state"
else
  bad "the admission line did not print requested, observed and corroboration together"
fi
if grep -q 'PARTIAL OBSERVATION IS NOT AGREEMENT' "$TMP/out.txt"; then
  ok "partial observation is disclosed rather than reduced to the observed value"
else
  bad "partial observation was silently upgraded to agreement"
fi
# THE REMEDY MUST TRAVEL WITH THE DIAGNOSTIC, and it is pinned so a wording pass
# cannot delete it silently -- which is the failure mode this whole round was
# about. Pinned by CONTENT, not just by the key: a line that keeps the key and
# loses the fix, the time window or the honest scope is not the line that was
# reviewed.
if grep -q '^AB-3649: verdict-detail single-stream ADMISSION-REMEDY ' "$TMP/out.txt"; then
  ok "a partial corroboration names its remedy in the OUTPUT, not only in the runbook"
else
  bad "a partial corroboration named a state with no remedy -- the shape this repo's fail-closed diagnostics exist to correct"
fi
check_remedy_shared "the partial remedy carries the time window, the log path and the honest scope"
# THE REMEDY DIFFERS BY STATE. The gate-pin verdict splits NOT-HONOURED from
# default because a shared remedy sends an operator in a circle; the same applies
# here, so the two states must not print the same first action.
if grep -q 'ADMISSION-REMEDY.*specific to the runs that did not report' "$TMP/out.txt"; then
  ok "the PARTIAL remedy points at the runs that did not report, not at the parser"
else
  bad "the partial remedy does not name the runs that did not report"
fi
if [ "$(grep -c '^AB-3649: verdict-detail single-stream ADMISSION-REMEDY ' "$TMP/out.txt")" = "1" ]; then
  ok "the remedy is ONE line, not a paragraph in the output"
else
  bad "the remedy grew past one line in the output"
fi
run_analyzer "$TMP/five"
if grep -q 'corroboration agreed (10 of 10 runs)' "$TMP/out.txt" \
   && ! grep -q '^AB-3649: verdict-detail single-stream ADMISSION ' "$TMP/out.txt"; then
  ok "full corroboration is stated as such and carries no caveat"
else
  bad "a fully corroborated ceiling was not reported as agreed"
fi
if grep -q 'ADMISSION-REMEDY' "$TMP/out.txt"; then
  bad "a fully corroborated run printed a remedy for a problem it does not have"
else
  ok "the remedy appears only where there is something to remedy"
fi

echo
echo "-- corroboration is a claim about PROVENANCE, not just a value --"

# `agreed` says the ceiling we passed is the ceiling that took effect. Counting
# observed VALUES alone let that word mean less than it says: a numeric ceiling
# paired with a derived or env source counted as fully corroborated.
mkfixture "$TMP/prov-derived" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
python3 - "$TMP/prov-derived/manifest.json" derived <<'PYINNER'
import json
import sys

path, source = sys.argv[1], sys.argv[2]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for entry in manifest["runs"]:
    if entry["arm"] == "head":
        entry["admission_source"] = source
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
run_analyzer "$TMP/prov-derived"
check_verdict "a run whose ceiling came from somewhere other than our flag" UNMEASURED 7 single-stream
check_cause "a non-flag admission provenance" admission-provenance

# A MISSING provenance is not a wrong one: it downgrades corroboration rather
# than refusing, exactly as a missing value does.
mkfixture "$TMP/prov-absent" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
python3 - "$TMP/prov-absent/manifest.json" NOT-OBSERVED <<'PYINNER'
import json
import sys

path, source = sys.argv[1], sys.argv[2]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for entry in manifest["runs"]:
    if entry["arm"] == "head":
        entry["admission_source"] = source
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
run_analyzer "$TMP/prov-absent"
check_verdict "a run whose provenance could not be read" MEETS-TARGET 0 single-stream
if grep -q 'corroboration partial (6 of 12 runs)' "$TMP/out.txt"; then
  ok "a value without a flag provenance does not count toward corroboration"
else
  bad "a value with no provenance was counted as corroborated"
fi

echo
echo "-- the target band is defined for --shape full --"

mkfixture "$TMP/shape-bad" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
# The manifest AND the records: a session that ran limit-k produced limit-k
# records, and the two must agree or the reconciliation refuses it first (which
# is a different, also-correct refusal, and would leave this case untested).
python3 - "$TMP/shape-bad" <<'PYINNER'
import json
import os
import sys

root = sys.argv[1]
path = os.path.join(root, "manifest.json")
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["workload"]["shape"] = "limit-k"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
for name in os.listdir(root):
    if not name.endswith(".jsonl"):
        continue
    target = os.path.join(root, name)
    with open(target, encoding="utf-8") as handle:
        records = [json.loads(line) for line in handle if line.strip()]
    for record in records:
        record["shape"] = "limit-k"
    with open(target, "w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")
PYINNER
run_analyzer "$TMP/shape-bad"
check_verdict "a limit-k session scored against the full-scan band" UNMEASURED 7 single-stream
check_cause "a workload that is not a full scan" shape-not-full

# A CONTROL may use any shape -- its verdict is disclaimed either way -- but the
# shape must be named beside it.
python3 - "$TMP/shape-bad/manifest.json" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["control"] = "shape-probe"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
run_analyzer "$TMP/shape-bad"
check_verdict "a labelled control may use another shape" MEETS-TARGET 0 single-stream
if grep -q '^AB-3649: verdict-detail single-stream SHAPE the workload was --shape limit-k' "$TMP/out.txt"; then
  ok "a non-full shape is named beside the verdict it disclaims"
else
  bad "a non-full shape was not disclosed"
fi
run_analyzer "$TMP/meets"
if grep -q '^AB-3649: verdict-detail single-stream SHAPE ' "$TMP/out.txt"; then
  bad "a full-scan session printed a shape disclaimer it does not need"
else
  ok "a full-scan session carries no shape disclaimer"
fi

echo
echo "-- within-pair order must be counterbalanced, and COUNTED from the record --"

# Interleaving across replicates controls drift BETWEEN pairs. A gradient WITHIN
# a pair -- thermal ramp, a neighbour starting -- lands on whichever arm runs
# second, every time, and biases every ratio the same way. No test of the
# statistics can catch that: every one of them passes.
run_analyzer "$TMP/meets"
if grep -q '^AB-3649: counterbalance base-first 3 head-first 3 residual 0 pair(s)$' "$TMP/out.txt"; then
  ok "an even replicate count counterbalances exactly, and the counts are reported"
else
  bad "the counterbalance counts were not reported for an even session"
fi
if grep -q '^AB-3649: counterbalance order-by-replicate 1:base,2:head,3:base,4:head,5:base,6:head$' "$TMP/out.txt"; then
  ok "the executed order is printed per replicate, so a reader can check it"
else
  bad "the per-replicate executed order was not printed"
fi
if grep -q '^AB-3649: verdict-detail single-stream COUNTERBALANCE ' "$TMP/out.txt"; then
  bad "an exactly balanced session printed a residual disclosure it does not need"
else
  ok "an exactly balanced session carries no counterbalance residual"
fi

# An ODD count cannot balance exactly. That residual is disclosed, not refused:
# refusing it would red a correct session.
mkfixture "$TMP/cb-odd" 5 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000"
run_analyzer "$TMP/cb-odd"
check_verdict "an odd replicate count, which cannot balance exactly" MEETS-TARGET 0 single-stream
if grep -q '^AB-3649: counterbalance base-first 3 head-first 2 residual 1 pair(s)$' "$TMP/out.txt" \
   && grep -q '^AB-3649: verdict-detail single-stream COUNTERBALANCE 3 pair(s) ran base-first' "$TMP/out.txt"; then
  ok "an odd count's one-pair residual is disclosed rather than hidden or refused"
else
  bad "an odd count's counterbalance residual was not disclosed"
fi

# THE DEFECT ITSELF: every pair in the same order.
mkfixture "$TMP/cb-broken" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
python3 - "$TMP/cb-broken/manifest.json" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for entry in manifest["runs"]:
    entry["position_in_pair"] = 1 if entry["arm"] == "base" else 2
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
run_analyzer "$TMP/cb-broken"
check_verdict "every pair run in the same within-pair order" UNMEASURED 7 single-stream
check_cause "an uncounterbalanced session" counterbalance-broken

# Counterbalancing that is not RECORDED is counterbalancing that cannot be
# checked -- so an absent or duplicated position is a refusal, not an assumption.
for breakage in absent duplicate; do
  mkfixture "$TMP/cb-$breakage" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
  python3 - "$TMP/cb-$breakage/manifest.json" "$breakage" <<'PYINNER'
import json
import sys

path, breakage = sys.argv[1], sys.argv[2]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for entry in manifest["runs"]:
    if entry["replicate"] != 1:
        continue
    if breakage == "absent":
        entry.pop("position_in_pair", None)
    else:
        entry["position_in_pair"] = 1
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
  run_analyzer "$TMP/cb-$breakage"
  check_verdict "a pair whose executed order is $breakage" UNMEASURED 7 single-stream
  check_cause "an unrecorded within-pair order ($breakage)" position-not-recorded
done

echo
echo "-- an observation that is taken must be COMPARED --"

# Round 2 finding 1, one field over: max_batch_bytes and the admission wait
# timeout were read off the startup line and then neither persisted nor compared,
# so two arms could be verdicted under different effective configurations.
for cfg_field in batch_size_observed max_batch_bytes_observed wait_timeout_ms_observed; do
  mkfixture "$TMP/cfg-$cfg_field" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
  python3 - "$TMP/cfg-$cfg_field/manifest.json" "$cfg_field" <<'PYINNER'
import json
import sys

path, name = sys.argv[1], sys.argv[2]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for entry in manifest["runs"]:
    if entry["arm"] == "head":
        entry[name] = "999999"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
  run_analyzer "$TMP/cfg-$cfg_field"
  check_verdict "arms served under a different $cfg_field" UNMEASURED 7 single-stream
  check_cause "a differing $cfg_field" server-config-mismatch
done
run_analyzer "$TMP/meets"
cfg_report_bad=''
for expect in "batch-size value 8192" "max-batch-bytes value 4194304" "wait-timeout-ms value 30000"; do
  grep -q "^AB-3649: server-observed $expect corroboration agreed (12 of 12 runs)\$" "$TMP/out.txt" \
    || cfg_report_bad="$cfg_report_bad ${expect%% *}"
done
if [ -z "$cfg_report_bad" ]; then
  ok "every readback reports its value AND its corroboration counts, through one type"
else
  bad "these readbacks were not reported with corroboration counts:$cfg_report_bad"
fi
# THE THIRD INSTANCE: a field observed for only some runs must not read as
# agreement, and it must inherit that from the shared type rather than a
# per-field guard.
mkfixture "$TMP/cfg-partial" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
python3 - "$TMP/cfg-partial/manifest.json" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for entry in manifest["runs"]:
    if entry["arm"] == "head":
        entry["max_batch_bytes_observed"] = "NOT-OBSERVED"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
run_analyzer "$TMP/cfg-partial"
check_verdict "a readback observed for only half the runs" MEETS-TARGET 0 single-stream
if grep -q '^AB-3649: server-observed max-batch-bytes value 4194304 corroboration partial (6 of 12 runs)$' "$TMP/out.txt" \
   && grep -q '^AB-3649: verdict-detail single-stream READBACK max-batch-bytes was observed for 6 of 12 runs' "$TMP/out.txt"; then
  ok "a partially observed readback is counted and disclosed, not read as agreement"
else
  bad "a partially observed readback was treated as agreement"
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
  head -c 512 /dev/zero > "$TMP/tinycorpus/ks/tbl/nb-1-big-CompressionInfo.db"
  head -c 512 /dev/zero > "$TMP/tinycorpus/ks/tbl/nb-2-big-CompressionInfo.db"
  # Unrelated tables and a snapshot subtree that the census must NOT count: this
  # is finding 1's shape, and without them the served-scope guard is untested.
  mkdir -p "$TMP/tinycorpus/other/bigtable" "$TMP/tinycorpus/ks/tbl/snapshots/s1"
  head -c 400000000 /dev/zero > "$TMP/tinycorpus/other/bigtable/nb-9-big-Data.db" 2>/dev/null || \
    head -c 4096 /dev/zero > "$TMP/tinycorpus/other/bigtable/nb-9-big-Data.db"
  head -c 4096 /dev/zero > "$TMP/tinycorpus/ks/tbl/snapshots/s1/nb-8-big-Data.db"
  mkdir -p "$TMP/onesstcorpus/ks/tbl"
  head -c 4096 /dev/zero > "$TMP/onesstcorpus/ks/tbl/nb-1-big-Data.db"
  head -c 512 /dev/zero > "$TMP/onesstcorpus/ks/tbl/nb-1-big-CompressionInfo.db"
  # ...and the one-source corpus gets a SECOND file elsewhere, so the #3058 guard
  # can only pass by counting the wrong directory.
  mkdir -p "$TMP/onesstcorpus/ks/decoy"
  head -c 4096 /dev/zero > "$TMP/onesstcorpus/ks/decoy/nb-2-big-Data.db"
  # The census describes the SERVED directory, so an "empty corpus" is now a
  # served directory that exists and holds no Data.db -- not an empty data root,
  # which is a different (and separately named) refusal.
  mkdir -p "$TMP/emptycorpus/ks/tbl"
  mkdir -p "$TMP/nosuchtable/ks/other"
  printf '{"version": 2, "keyspace": "ks", "table": "tbl", "ddl": "CREATE TABLE ks.tbl (a int PRIMARY KEY)"}\n' > "$TMP/ticket.json"

  # A SCRATCH REPOSITORY, SO NO CASE DEPENDS ON WHERE THIS SUITE WAS RUN FROM.
  # The driver resolves --repo before its corpus, lock, ref and CPU checks, so a
  # case that omits --repo only reaches the guard it names when the suite happens
  # to live inside a git checkout. Run from a copied directory that is not one,
  # ten cases red for a reason that has nothing to do with what they test -- and
  # one (the CPU-overlap case) went on PASSING, because both the guard it names
  # and the repo refusal exit 3. A case that tests a property AND an environment
  # will eventually be read as evidence about the property alone.
  #
  # Every case below that must get PAST repo resolution therefore names this
  # repository explicitly. The two cases that test repo resolution itself
  # deliberately do not.
  SCRATCH="$TMP/scratchrepo"
  mkdir -p "$SCRATCH"
  (
    cd "$SCRATCH"
    git init -q .
    git config user.email selftest@example.invalid
    git config user.name selftest
    printf 'one\n' > f.txt && git add f.txt && git commit -qm one
    printf 'two\n' > f.txt && git commit -qam two
  ) > /dev/null 2>&1
  if [ "$(git -C "$SCRATCH" rev-list --count HEAD 2>/dev/null || echo 0)" -ge 2 ]; then
    ok "the driver-guard cases have a scratch repository, so none of them depends on where this suite lives"
  else
    bad "the scratch repository was not created, so the driver cases below are testing their environment"
  fi

  run_driver --help
  check_driver "the driver --help exits 3, never 0" 3

  # FINDING 4: a value-taking option with no value used to `shift 2` past the end
  # and exit 1 with an unanchored bash error.
  for lonely in --corpus --ticket-template --replicates --max-concurrent-scans \
                --batch-size --step-duration --ramp --control; do
    run_driver "$lonely"
    if [ "$RC" = "3" ] && anchored \
       && grep -q "^AB-3649: usage-error $lonely requires a value\$" "$TMP/err.txt"; then
      ok "$lonely with no value is an anchored usage error"
    else
      bad "$lonely with no value exited $RC without an anchored usage error"
    fi
  done
  # STRUCTURAL: every arm that consumes a value must be in the one list, or the
  # next option added is the next one to miss the guard. Done in Python because a
  # sed/tr pipeline over a line-continued shell array is its own source of bugs.
  if python3 - "$DRIVER" <<'PYINNER'
import re
import sys

source = open(sys.argv[1], encoding="utf-8").read()
declared = re.search(r'VALUE_OPTS="((?:[^"\\]|\\.)*)"', source, re.S)
if not declared:
    sys.stderr.write("AB-3649: VALUE_OPTS is not a single quoted assignment\n")
    raise SystemExit(1)
listed = set(declared.group(1).replace("\\\n", " ").split())
consuming = set(re.findall(r"^\s*(--[a-z-]+)\)[^\n]*shift 2", source, re.M))
missing = sorted(consuming - listed)
if missing:
    sys.stderr.write("AB-3649: not in VALUE_OPTS: %s\n" % " ".join(missing))
    raise SystemExit(1)
if not consuming:
    sys.stderr.write("AB-3649: found no value-consuming arms, so this guard proved nothing\n")
    raise SystemExit(1)
PYINNER
  then
    ok "every value-consuming option arm appears in the one VALUE_OPTS guard list"
  else
    bad "an option arm shifts a value without being in VALUE_OPTS (see stderr above)"
  fi


  for bogus in why todo TBD "  " "<why>"; do
    run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" --max-concurrent-scans 4 \
      --work-dir "$TMP/w-attest" --control selftest --repo "$SCRATCH" --attest-local-storage "$bogus"
    # exit 3 is this driver's usage-error code, as for --help; exit 2 is a
    # named refusal, which a bad flag value is not.
    check_driver "an attestation reason of '$bogus'" 3
  done

  run_driver --no-such-flag
  check_driver "an unrecognised driver flag exits 3" 3

  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" --max-concurrent-scans 4 --replicates 4
  check_driver "the driver refuses fewer than 5 replicates" 3

  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json"
  check_driver "the driver refuses to run without a pinned admission ceiling" 3

  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --ramp 1,2,4,8
  check_driver "the driver refuses a ramp that tops out above the admission pin" 3

  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" --max-concurrent-scans 4 --server-cpus 0,2
  check_driver "the driver refuses a server CPU set with no client CPU set" 3

  # The CPU-set check runs AFTER ref resolution, so this case must name refs that
  # exist in the scratch repository; the default arms (cfa93fe99 and its parent)
  # do not. Getting this wrong is how the case previously reported the right exit
  # code from the wrong guard.
  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" --max-concurrent-scans 4 \
    --server-cpus 0,2 --client-cpus 2,3 --work-dir "$TMP/w-overlap" --min-corpus-bytes 1 --control selftest \
    --min-sstables 1 --repo "$SCRATCH" --base-ref HEAD~1 --head-ref HEAD
  check_driver "the driver refuses overlapping server and client CPU sets" 3

  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/nope.json" --max-concurrent-scans 4 \
    --work-dir "$TMP/w-tpl" --repo "$SCRATCH"
  check_driver "an absent ticket template" 2 ticket-template-absent

  run_driver --corpus "$TMP/emptycorpus" --ticket-template "$TMP/ticket.json" --max-concurrent-scans 4 \
    --work-dir "$TMP/w-empty" --min-corpus-bytes 1 --control selftest --repo "$SCRATCH"
  check_driver "a served directory holding no Data.db files" 2 corpus-empty
  run_driver --corpus "$TMP/nosuchtable" --ticket-template "$TMP/ticket.json" --max-concurrent-scans 4 \
    --work-dir "$TMP/w-nodir" --min-corpus-bytes 1 --control selftest --repo "$SCRATCH"
  check_driver "a ticket naming a table that is not under the data root" 2 served-dir-absent

  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" --max-concurrent-scans 4 \
    --work-dir "$TMP/w-small" --repo "$SCRATCH"
  check_driver "a corpus below the stated minimum size" 2 corpus-too-small

  # FINDING 1: the #3058 guard must count the SERVED directory. This corpus has a
  # second Data.db under a DIFFERENT table, so the old whole-root census would
  # have counted two and let a single-source served table through -- the exact
  # phantom the guard exists to stop.
  run_driver --corpus "$TMP/onesstcorpus" --ticket-template "$TMP/ticket.json" --max-concurrent-scans 4 \
    --work-dir "$TMP/w-onesst" --min-corpus-bytes 1 --control selftest --repo "$SCRATCH"
  check_driver "a served table with one SSTable and a decoy elsewhere under the data root" \
    2 corpus-too-few-sstables
  # ...and the size floor is likewise a claim about the served table: this corpus
  # has a large Data.db under another table and a snapshot subtree, neither of
  # which the server would read.
  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" --max-concurrent-scans 4 \
    --work-dir "$TMP/w-scope" --repo "$SCRATCH"
  check_driver "a served table below the size floor, with a large unrelated table alongside" \
    2 corpus-too-small
  if grep -q 'served-dir .*ks/tbl data-db-files 2 data-db-bytes 8192' "$TMP/out.txt"; then
    ok "the census counts the served directory only, not the data root"
  else
    bad "the census did not scope itself to the served directory"
  fi
  # ...and it must mirror the CONTAINMENT check too, not only the scope: the
  # server excludes an entry whose canonical target escapes the served directory,
  # so a symlinked decoy must satisfy neither floor.
  mkdir -p "$TMP/symcorpus/ks/tbl" "$TMP/symcorpus/elsewhere"
  head -c 4096 /dev/zero > "$TMP/symcorpus/ks/tbl/nb-1-big-Data.db"
  head -c 512 /dev/zero > "$TMP/symcorpus/ks/tbl/nb-1-big-CompressionInfo.db"
  head -c 400000 /dev/zero > "$TMP/symcorpus/elsewhere/real-Data.db"
  ln -sf "$TMP/symcorpus/elsewhere/real-Data.db" "$TMP/symcorpus/ks/tbl/nb-2-big-Data.db"
  run_driver --corpus "$TMP/symcorpus" --ticket-template "$TMP/ticket.json" --max-concurrent-scans 4 \
    --work-dir "$TMP/w-sym" --min-corpus-bytes 1 --control selftest --repo "$SCRATCH"
  check_driver "a symlinked decoy standing in for a second SSTable" 2 corpus-too-few-sstables
  # A HARD link -- which is what a Cassandra snapshot is -- canonicalises inside
  # the directory and must still count, or the check would red on real corpora.
  mkdir -p "$TMP/hardcorpus/ks/tbl"
  head -c 4096 /dev/zero > "$TMP/hardcorpus/ks/tbl/nb-1-big-Data.db"
  if ln "$TMP/hardcorpus/ks/tbl/nb-1-big-Data.db" "$TMP/hardcorpus/ks/tbl/nb-2-big-Data.db" 2>/dev/null; then
    census_out="$(python3 "$SUPPORT" census-served "$TMP/hardcorpus" "$TMP/ticket.json" 2>/dev/null)"
    if [ "${census_out%% *}" = "2" ]; then
      ok "a hard link, as a Cassandra snapshot uses, still counts"
    else
      bad "the containment check wrongly excluded a hard link ('$census_out')"
    fi
  else
    ok "hard-link case skipped: this filesystem does not support them (declared, not assumed)"
  fi

  # FINDING 5: arms served under different flags is a control by definition.
  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --head-server-extra '--max-batch-bytes 1'
  check_driver "asymmetric per-arm flags without a control label" 3
  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --head-server-extra '--max-batch-bytes 1' \
    --control sensitivity --work-dir "$TMP/w-asym" --min-corpus-bytes 1 \
    --min-sstables 1 --repo "$SCRATCH" --base-ref HEAD~1 --head-ref HEAD
  if [ "$RC" != "3" ]; then
    ok "a labelled control may serve the arms asymmetrically"
  else
    bad "a labelled control was refused for asymmetric flags (exit $RC)"
  fi

  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" --max-concurrent-scans 4 \
    --work-dir "$TMP/w-badarm" --min-corpus-bytes 1 --control selftest --merge-path sideways
  check_driver "an unrecognised --merge-path value" 3

  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" --max-concurrent-scans 4 \
    --work-dir "$TMP/w-same" --min-corpus-bytes 1 --control selftest --min-sstables 1 --repo "$SCRATCH" \
    --base-ref HEAD --head-ref HEAD
  check_driver "two arm refs resolving to the same commit" 2 arm-refs-identical

  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" --max-concurrent-scans 4 \
    --work-dir "$TMP/w-badref" --min-corpus-bytes 1 --control selftest --min-sstables 1 --repo "$SCRATCH" \
    --base-ref no-such-rev-3649 --head-ref HEAD
  check_driver "an arm ref that resolves to nothing" 2 arm-ref-unresolvable

  # P1-3: every ramp element, through the same validator the helper exposes.
  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --ramp 1,abc
  check_driver "the driver refuses a ramp with a non-numeric element" 3
  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --ramp 2
  check_driver "the driver refuses a ramp that maps to no analyzer section" 3

  # FINDING 3 (round 8): a RELATIVE --work-dir. `CARGO_TARGET_DIR` is read after
  # the driver cds into the worktree, so a relative path put the target directory
  # somewhere the driver then did not look -- both arms compiling, then
  # `build-incomplete`, on a metered box. The e2e cases all pass absolute paths,
  # which is the natural thing to write and therefore exactly what a harness does
  # not cover by accident.
  ( cd "$TMP" && bash "$DRIVER" --corpus "$TMP/tinycorpus" \
      --ticket-template "$TMP/ticket.json" --max-concurrent-scans 4 \
      --work-dir ./relwd --repo "$SCRATCH" --min-corpus-bytes 1 --control selftest --min-sstables 1 \
      --base-ref HEAD~1 --head-ref HEAD ) > "$TMP/out.txt" 2>&1 || true
  if grep -qE "^AB-3649: work-dir /" "$TMP/out.txt"; then
    ok "a relative --work-dir is canonicalised to an absolute path before anything derives from it"
  else
    bad "a relative --work-dir was not canonicalised: $(grep -m1 '^AB-3649: work-dir' "$TMP/out.txt")"
  fi

  # ROUND 12 FINDING 1: the compressed-corpus requirement was documented by us
  # and enforced by nothing -- and an uncompressed corpus biases the ratio TOWARD
  # the target, so the failure was in the favourable direction.
  mkdir -p "$TMP/plaincorpus/ks/tbl"
  head -c 4096 /dev/zero > "$TMP/plaincorpus/ks/tbl/nb-1-big-Data.db"
  head -c 4096 /dev/zero > "$TMP/plaincorpus/ks/tbl/nb-2-big-Data.db"
  run_driver --corpus "$TMP/plaincorpus" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --work-dir "$TMP/w-plain" --min-corpus-bytes 1 \
    --min-sstables 1 --control selftest --repo "$SCRATCH"
  # A control MAY measure an uncompressed corpus; only a measurement may not.
  if grep -q 'corpus compression UNCOMPRESSED' "$TMP/out.txt"; then
    ok "an uncompressed corpus is detected and named"
  else
    bad "the census did not detect a missing CompressionInfo.db"
  fi
  run_driver --corpus "$TMP/plaincorpus" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --work-dir "$TMP/w-plain2" --repo "$SCRATCH"
  check_driver "a measurement over an uncompressed corpus" 2 corpus-uncompressed
  # A zero-length CompressionInfo.db is ABSENT, not present: this repository
  # records that an empty one makes SELECT return 0 rows silently.
  : > "$TMP/plaincorpus/ks/tbl/nb-1-big-CompressionInfo.db"
  : > "$TMP/plaincorpus/ks/tbl/nb-2-big-CompressionInfo.db"
  run_driver --corpus "$TMP/plaincorpus" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --work-dir "$TMP/w-plain3" --repo "$SCRATCH"
  check_driver "a zero-length CompressionInfo.db, which reads as absent" 2 corpus-uncompressed

  # ROUND 12 FINDING 2: a ticket missing a required field used to fail after all
  # three release builds.
  printf '{"version":2,"keyspace":"ks","table":"t"}\n' > "$TMP/tk-noddl.json"
  run_support validate-ticket "$TMP/tk-noddl.json"
  check_support "a ticket with no ddl, which flight-loadgen cannot deserialise" 1 ticket-schema-invalid
  # ROUND 13 FINDING 2, the TOO-STRICT half: `version` carries
  # `#[serde(default = "default_ticket_version")]`, so a ticket without it
  # deserialises fine. Demanding it red a CORRECT ticket -- the shape an operator
  # learns to waive, and worse on a rig, where waiving means --control and a
  # disclaimed verdict. This case asserted the wrong rule until round 13.
  printf '{"keyspace":"ks","table":"t","ddl":"CREATE TABLE ks.t (a int PRIMARY KEY)"}\n' > "$TMP/tk-nover.json"
  run_support validate-ticket "$TMP/tk-nover.json"
  check_support "a ticket with no version, which serde defaults" 0
  printf '{"version":2,"keyspace":"ks","table":"t","ddl":"   "}\n' > "$TMP/tk-blankddl.json"
  run_support validate-ticket "$TMP/tk-blankddl.json"
  check_support "a ticket whose ddl is blank" 1 ticket-schema-invalid

  # FINDING 2 (round 9): THE FLOORS ARE FLOORS. An unlabelled measurement may not
  # lower them -- the third route to #3058's bypass was simply passing
  # `--min-sstables 1` and serving a single-source table.
  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --min-sstables 1
  check_driver "a measurement lowering the SSTable floor" 3
  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --min-corpus-bytes 1
  check_driver "a measurement lowering the corpus-size floor" 3
  if grep -q 'documented floor' "$TMP/err.txt"; then
    ok "the floors are refused by name, with the documented minimum stated"
  else
    bad "the floor refusal did not name the documented minimum"
  fi
  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --min-sstables 1 --min-corpus-bytes 1 \
    --control floor-probe --work-dir "$TMP/w-floorctl" --repo "$SCRATCH" \
    --base-ref HEAD~1 --head-ref HEAD
  if [ "$RC" != "3" ]; then
    ok "a labelled control may lower the floors, where its verdict is disclaimed"
  else
    bad "a labelled control could not lower the floors (exit $RC)"
  fi

  # FINDING 1 (round 9): no `eval` on operator-supplied values.
  #
  # HONEST ACCOUNTING OF WHAT THIS CASE PROVES. The primary guarantee is
  # STRUCTURAL -- the no-`eval` assert below -- because with the resolver in
  # place a dangerous payload is REFUSED before it could reach an interpreter,
  # so a "did it execute" check cannot fail however the code is written. What
  # this case binds is (a) the refusal itself, which is behavioural and can fail,
  # and (b) the absence of side effects, which would catch an `eval` placed
  # UPSTREAM of validation -- exactly where the original one was.
  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --control inject \
    --head-server-extra '--max-batch-bytes $(touch '"$TMP"'/PWNED)' \
    --work-dir "$TMP/w-inject" --min-corpus-bytes 1 --min-sstables 1 --repo "$SCRATCH"
  if [ "$RC" = "3" ] && grep -qE 'session-config-invalid|not resolvable per arm' "$TMP/err.txt"; then
    ok "a shell-metacharacter payload in a per-arm flag is refused, not resolved"
  else
    bad "a shell-metacharacter payload was not refused (exit $RC)"
  fi
  if [ -e "$TMP/PWNED" ]; then
    bad "a command substitution in a per-arm flag value was EXECUTED by the driver"
  else
    ok "...and produced no side effect, which would catch an eval before validation"
  fi

  # FINDING 1 (round 8): per-arm extras are a SECOND route to the batch-size
  # floor, and symmetric extras need no control label.
  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --base-server-extra '--batch-size 0' \
    --head-server-extra '--batch-size 0'
  check_driver "symmetric per-arm extras that zero the batch size" 3
  if grep -q 'resolved --batch-size' "$TMP/err.txt"; then
    ok "the floor is enforced on the RESOLVED value, so the extras route inherits it"
  else
    bad "the batch-size floor was bypassed through per-arm extras"
  fi

  # FINDING 5: --batch-size 0 is silently clamped to one row per batch by the
  # server, so the manifest would not record the value that was used.
  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --batch-size 0
  check_driver "the driver refuses --batch-size 0" 3

  # FINDING 2: the shape and the ticket must match the claim the report makes.
  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --shape limit-k
  check_driver "the driver refuses a non-full shape for a measurement session" 3
  printf '{"version":2,"keyspace":"ks","table":"t","ddl":"CREATE TABLE ks.t (a int PRIMARY KEY)","limit":100}\n' > "$TMP/tk-narrow.json"
  # NOT a control: the ticket check applies to measurements only, so labelling
  # this one would skip the very guard it names. It therefore also cannot lower
  # the corpus floors -- and does not need to, because the ticket is validated
  # before the census runs.
  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/tk-narrow.json" \
    --max-concurrent-scans 4 --work-dir "$TMP/w-narrow" --repo "$SCRATCH"
  check_driver "the driver refuses a ticket carrying a LIMIT" 2 ticket-not-full-ring
  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/tk-narrow.json" \
    --max-concurrent-scans 4 --work-dir "$TMP/w-narrow-ctl" --min-corpus-bytes 1 \
    --min-sstables 1 --repo "$SCRATCH" --control shape-probe \
    --base-ref HEAD~1 --head-ref HEAD --shape limit-k
  if [ "$RC" != "2" ] || grep -q 'ticket-not-full-ring' "$TMP/err.txt"; then
    bad "a labelled control was still refused for a narrowed ticket (exit $RC)"
  else
    ok "a labelled control may narrow the ticket; the analyzer disclaims its verdict"
  fi

  # FINDING 3: a step duration flight-loadgen accepts must be accepted here too,
  # and one it rejects must fail BEFORE the builds rather than after the money.
  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --step-duration nope
  check_driver "the driver refuses a step duration flight-loadgen would reject" 3

  # P0-3: --rows-declared reached int() unvalidated.
  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --rows-declared 3,999,890
  check_driver "the driver refuses a --rows-declared with separators" 3

  # P1-6: an ordinary operator mistake must not leak an unanchored line.
  mkdir -p "$TMP/notarepo"
  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --repo "$TMP/notarepo"
  check_driver "--repo pointing at a directory that is not a repository" 3
  if grep -qi 'fatal:' "$TMP/err.txt"; then
    bad "a raw git 'fatal:' line leaked past the anchor"
  else
    ok "a non-repository --repo is reported anchored, with no raw git output"
  fi
  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --repo "$TMP/no-such-dir-3649"
  check_driver "--repo pointing at a directory that does not exist" 3
  if [ -s "$TMP/err.txt" ]; then
    ok "a missing --repo produces a diagnostic, not a silent exit 1"
  else
    bad "a missing --repo produced no output at all"
  fi

  # P1-5: two sessions in one work directory. The second must be refused BEFORE
  # it can truncate the first's ledger.
  # RESTRUCTURE DEBRIS, FOUND BY SWEEPING RATHER THAN BY A RED: this case used to
  # assert that a lock-refused session did not truncate `results/runs.jsonl`. The
  # driver stopped writing there in round 4, so the assertion became trivially
  # true and the case proved nothing. The property worth asserting now is that a
  # refused session leaves NOTHING -- which is why the lock is taken before the
  # session directory is created.
  mkdir -p "$TMP/w-locked/.session-lock"
  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --work-dir "$TMP/w-locked" --min-corpus-bytes 1 --control selftest --min-sstables 1 \
    --repo "$SCRATCH"
  check_driver "a second session in a work directory already in use" 2 work-dir-busy
  if [ -z "$(find "$TMP/w-locked" -maxdepth 1 -type d -name 'run-*' 2>/dev/null)" ]; then
    ok "a session refused by the lock leaves no directory behind at all"
  else
    bad "a lock-refused session created a session directory before checking the lock"
  fi
  # ...AND IT MUST NOT RELEASE THE LOCK IT FAILED TO TAKE. The trap is armed
  # BEFORE acquisition, so `cleanup` runs on this path too; keying its rmdir on
  # the directory merely existing would delete the PEER'S lock, turning a leak
  # into a silent mutual-exclusion failure -- strictly worse than the leak being
  # fixed. This is the case that stops the obvious fix.
  if [ -d "$TMP/w-locked/.session-lock" ]; then
    ok "a lock-refused session leaves the holder's lock alone"
  else
    bad "a lock-refused session DELETED the lock another session holds"
  fi
  rmdir "$TMP/w-locked/.session-lock"

  # THE LEAK ITSELF: a session that fails AFTER taking the lock must release it,
  # or the work directory is permanently unusable. `--corpus` naming nothing is a
  # refusal that happens well after acquisition.
  run_driver --corpus "$TMP/no-such-corpus-dir" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --work-dir "$TMP/w-leak" --min-corpus-bytes 1 --control selftest --min-sstables 1 \
    --repo "$SCRATCH"
  check_driver "a session failing after it took the lock" 2 corpus-absent
  if [ -d "$TMP/w-leak/.session-lock" ]; then
    bad "a session that failed after acquiring the lock LEAKED it, blocking every later session"
  else
    ok "a session that fails after acquiring the lock releases it"
  fi
  # ...and the proof that the release is real: the next session gets past the
  # lock rather than being refused work-dir-busy. A stat on a directory is not
  # the property an operator cares about; being able to run again is.
  run_driver --corpus "$TMP/no-such-corpus-dir" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --work-dir "$TMP/w-leak" --min-corpus-bytes 1 --control selftest --min-sstables 1 \
    --repo "$SCRATCH"
  check_driver "a session reusing a work directory whose predecessor failed" 2 corpus-absent

  # P1-7: a worktree at the right commit but carrying uncommitted edits builds
  # code the manifest does not describe.
  mkdir -p "$TMP/w-dirty"
  git -C "$SCRATCH" worktree add -q --detach "$TMP/w-dirty/wt-base" HEAD > /dev/null 2>&1
  printf 'uncommitted\n' >> "$TMP/w-dirty/wt-base/f.txt"
  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --work-dir "$TMP/w-dirty" --min-corpus-bytes 1 --control selftest \
    --min-sstables 1 --repo "$SCRATCH" --base-ref HEAD --head-ref HEAD~1
  check_driver "a reused worktree at the right commit but not clean" 2 worktree-dirty

  # ROUND 2 FINDING 4 / ROUND 3 FINDING 2 / ROUND 4 FINDING 2 -- one class, now
  # closed by construction rather than by sequencing. Each session writes ONLY to
  # `<work-dir>/run-<session-id>/`, a name no other session can produce, so an
  # earlier session's results cannot be reached by any code path here: not a
  # failed pre-flight, not a failed build, not a lost port, not a kill.
  mkdir -p "$TMP/w-prior/run-EARLIER-SESSION"
  printf '{"arm":"base","replicate":1,"file":"base-r01.jsonl"}\n' \
    > "$TMP/w-prior/run-EARLIER-SESSION/runs.jsonl"
  printf '{"schema":"ab-3649.manifest/v1","note":"an earlier session"}\n' \
    > "$TMP/w-prior/run-EARLIER-SESSION/manifest.json"
  printf 'earlier replicate data\n' \
    > "$TMP/w-prior/run-EARLIER-SESSION/base-r01.jsonl"
  prior_before="$(cat "$TMP/w-prior/run-EARLIER-SESSION/"* | cksum)"
  prior_files="$(find "$TMP/w-prior/run-EARLIER-SESSION" -type f | sort | tr '\n' ' ')"
  run_driver --corpus "$TMP/tinycorpus" --ticket-template "$TMP/ticket.json" \
    --max-concurrent-scans 4 --work-dir "$TMP/w-prior" --repo "$SCRATCH"
  check_driver "a re-used work directory whose new attempt fails pre-flight" 2 corpus-too-small
  if [ "$(cat "$TMP/w-prior/run-EARLIER-SESSION/"* | cksum)" = "$prior_before" ]; then
    ok "an earlier session's manifest, ledger AND replicate files are byte-identical after a failed attempt"
  else
    bad "a failed attempt altered an earlier session's results"
  fi
  if grep -q 'this session wrote only to ' "$TMP/out.txt"; then
    ok "and the abort names the only directory it wrote to"
  else
    bad "the abort did not name the directory it confined itself to"
  fi
  # The replicate JSONLs live in the session directory too -- round 4 finding 2
  # was that the manifest was staged while the files it references were not.
  # Compared as a SET, not by mtime: the previous form asked whether any
  # `base-r01.jsonl` was NEWER than the prior manifest, which depends on
  # filesystem timestamp granularity and on the order the fixture happened to be
  # created -- a case whose verdict moves with the filesystem is not a case.
  if [ "$(find "$TMP/w-prior/run-EARLIER-SESSION" -type f | sort | tr '\n' ' ')" \
       = "$prior_files" ]; then
    ok "the earlier session's directory gained and lost no files"
  else
    bad "a file appeared in or vanished from the earlier session's directory"
  fi

  # ALSO RESTRUCTURE DEBRIS: this checked `results/manifest.json`, a path the
  # driver no longer writes, so it passed by looking at nothing. A pre-flight
  # abort now writes a truthful manifest into ITS OWN session directory recording
  # zero runs, and the analyzer must refuse THAT rather than read it as a result.
  BADREF_DIR="$(find "$TMP/w-badref" -maxdepth 1 -type d -name 'run-*' 2>/dev/null | head -1)"
  if [ -n "$BADREF_DIR" ] && [ -f "$BADREF_DIR/manifest.json" ]; then
    ok "a pre-flight abort writes a truthful manifest into its own session directory"
  else
    bad "a pre-flight abort left no manifest to be refused"
  fi
  if [ -n "$BADREF_DIR" ]; then
    run_analyzer "$BADREF_DIR"
    check_verdict "the analyzer refuses a session that completed no replicate" UNMEASURED 7
  fi
fi

echo
echo "-- THE DRIVER, EXECUTED END TO END under PATH shims --"

# WHY THIS SECTION EXISTS. Everything above tests the analyzer and the helpers
# the driver was refactored to expose. Nothing ran the SESSION LOOP -- and
# `bash -n` cannot see a command that does not exist or a variable that was never
# exported, so a driver that died on its first replicate, after both release
# builds, passed every check this suite had. That is the fifth instance of one
# class in this lane (see FINDINGS.md §12), and the rule in §9 says stop fixing
# instances and remove the reason the class exists.
#
# So the loop runs here, with the three externals replaced by PATH shims -- the
# idiom this repo's own gate self-tests use for exactly this
# (scripts/tests/test_agent_gate_feature_matrix_annotation.sh drives components
# under a recording PATH-shim `cargo`). No source seam and no test-only flag: a
# real invoker has nothing extra to trip over.
#
# DECLARED GAP, because a lane that omits coverage silently is indistinguishable
# from one that covers it: the REAL cargo build, the REAL cqlite-flight and the
# REAL flight-loadgen are NOT exercised anywhere. This proves the driver's own
# logic -- ordering, plumbing, recording, promotion -- against realistic
# behaviour, not that cqlite-flight works.

e2e_supported=1
command -v python3 >/dev/null 2>&1 || e2e_supported=0
if [ "$e2e_supported" = "0" ]; then
  bad "the end-to-end section needs python3 and could not run"
else
  SHIMBIN="$TMP/shimbin"
  mkdir -p "$SHIMBIN"

  # --- the stub server: binds a REAL socket, so the ephemeral-port plumbing is
  # --- exercised rather than simulated, and prints the two lines the driver
  # --- parses (configuration, then the post-bind readiness line).
  cat > "$SHIMBIN/stub-cqlite-flight" <<'STUBEOF'
#!/usr/bin/env python3
import socket
import sys
import time

args = sys.argv[1:]


def opt(name, default=None):
    return args[args.index(name) + 1] if name in args else default


listen = opt("--listen", "127.0.0.1:0")
host, _, port = listen.partition(":")
sock = socket.socket()
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind((host, int(port)))
sock.listen(16)
bound = "%s:%d" % (host, sock.getsockname()[1])

# TRANSCRIBED FROM `cli::log_startup` (cqlite-flight/src/cli.rs:207-226), field
# for field and IN ITS ORDER, not from what the parser happens to want. A stub
# that prints what the parser expects rather than what the server emits passes
# the test and fails on the rig -- #3042's round-trip lesson in a new place: a
# stub I wrote agreeing with a parser I wrote proves self-consistency, not
# correctness. The asymmetry that rescues it is that the contract is committed
# source sitting right there, so it is transcribed and cited rather than
# inferred. tracing's Full format renders the message first, then the fields.
#
# The three fields the driver does NOT read are printed anyway, because their
# ABSENCE would make this an easier line to parse than the real one -- and
# `max_concurrent_streams` in particular shares a prefix with
# `max_concurrent_scans`, so leaving it out would hide an adjacency bug in the
# regex rather than expose it.
print(
    "INFO cqlite_flight: cqlite-flight starting listen=%s batch_size=%s "
    "max_batch_bytes=%s max_inflight_egress_bytes=12582912 "
    "max_concurrent_scans=%s max_concurrent_scans_source=flag "
    "available_parallelism=4 admission_wait_timeout_ms=%s "
    "max_concurrent_streams=100"
    % (
        listen,
        opt("--batch-size", "8192"),
        opt("--max-batch-bytes", "4194304"),
        opt("--max-concurrent-scans", "16"),
        opt("--admission-wait-timeout-ms", "30000"),
    ),
    flush=True,
)
# `cli::log_listening` (cqlite-flight/src/cli.rs:228-241) prints the address
# ACTUALLY BOUND, which with `--listen 127.0.0.1:0` is the only place the real
# port appears -- the startup line above still says `:0`. Printing the requested
# address here instead would hide a driver that reads the wrong field, which is
# the single bug the port-0 design depends on not having.
print("INFO cqlite_flight: cqlite-flight listening on %s listening_on=%s" % (bound, bound), flush=True)
while True:
    time.sleep(3600)
STUBEOF

  # --- a SECOND stub server: identical, but ANSI-COLOURED the way
  # --- `tracing_subscriber::fmt` colours -- the FIELD NAME and the `=` styled,
  # --- which is what makes a `name=value` pattern match nothing. Colour is on by
  # --- default and survives redirection to a file, so an uncoloured log is not
  # --- what a rig produces. The first three stub-fidelity failures were a
  # --- missing field, a permissive argv and an ignored `-p`, all enumerable from
  # --- cli.rs; this one is a FORMAT, and you only get it by asking what the real
  # --- output looks like on the wire.
  cat > "$SHIMBIN/stub-cqlite-flight-ansi" <<'STUBEOF'
#!/usr/bin/env python3
import re
import socket
import sys
import time

args = sys.argv[1:]


def opt(name, default=None):
    return args[args.index(name) + 1] if name in args else default


def emit(line):
    coloured = re.sub(
        r"([a-z_]+)=",
        lambda m: "\x1b[3m" + m.group(1) + "\x1b[0m\x1b[2m=\x1b[0m",
        line,
    )
    print("\x1b[2m2026-09-01T10:00:00Z\x1b[0m \x1b[32m INFO\x1b[0m " + coloured,
          flush=True)


listen = opt("--listen", "127.0.0.1:0")
host, _, port = listen.partition(":")
sock = socket.socket()
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind((host, int(port)))
sock.listen(16)
bound = "%s:%d" % (host, sock.getsockname()[1])

emit(
    "cqlite_flight: cqlite-flight starting listen=%s batch_size=%s "
    "max_batch_bytes=%s max_inflight_egress_bytes=12582912 "
    "max_concurrent_scans=%s max_concurrent_scans_source=flag "
    "available_parallelism=4 admission_wait_timeout_ms=%s "
    "max_concurrent_streams=100"
    % (
        listen,
        opt("--batch-size", "8192"),
        opt("--max-batch-bytes", "4194304"),
        opt("--max-concurrent-scans", "16"),
        opt("--admission-wait-timeout-ms", "30000"),
    )
)
emit("cqlite_flight: cqlite-flight listening on %s listening_on=%s" % (bound, bound))
while True:
    time.sleep(3600)
STUBEOF
  chmod +x "$SHIMBIN/stub-cqlite-flight-ansi"

  # --- the stub load generator: CONNECTS to the endpoint it was handed, so a
  # --- wrong or stale address fails the run rather than passing quietly, and
  # --- writes one internally-consistent record per ramp step.
  cat > "$SHIMBIN/stub-flight-loadgen" <<'STUBEOF'
#!/usr/bin/env python3
import json
import socket
import sys

args = sys.argv[1:]


def opt(name, default=None):
    return args[args.index(name) + 1] if name in args else default


endpoint = opt("--endpoint", "")
host, _, port = endpoint.replace("http://", "").partition(":")
# Proves the driver handed us a real, live address -- the whole point of the
# ephemeral-port change is that this is OUR server and not somebody else's.
with socket.create_connection((host, int(port)), timeout=10):
    pass

steps = [int(v) for v in opt("--ramp", "1").split(",")]
raw = opt("--step-duration", "60s")
for suffix, scale in (("ms", 0.001), ("m", 60.0), ("s", 1.0)):
    if raw.endswith(suffix):
        duration = float(raw[: -len(suffix)]) * scale
        break
else:
    duration = float(raw)

out = opt("--out", "/dev/null")
label = opt("--round", "")
# A deterministic rate that differs by arm, with a small per-replicate jitter.
# The jitter is NOT decoration: without it every ratio is identical, the
# bootstrap interval has zero width, and the analyzer correctly refuses the
# session as `bootstrap-degenerate` -- which is the guard working, and would
# make this case unable to reach a verdict. Derived from crc32 rather than
# `hash()`, which is salted per process and would make the suite
# non-deterministic.
import zlib

rate = 120000.0 if label.startswith("head") else 100000.0
rate *= 1.0 + ((zlib.crc32(label.encode()) % 11) - 5) / 500.0
with open(out, "w", encoding="utf-8") as handle:
    for position, concurrency in enumerate(steps):
        factor = 1.0 if position == len(steps) - 1 else 0.6
        value = rate * factor
        rows = int(value * duration)
        handle.write(
            json.dumps(
                {
                    "schema": "flight-loadgen.step/v1",
                    "round": label,
                    "endpoint": endpoint,
                    "ts_unix_ms": 1780000000000,
                    "seed": 42,
                    "step": position,
                    "target_concurrency": concurrency,
                    "shape": opt("--shape", "full"),
                    "duration_s": duration,
                    "requests_ok": 5,
                    "requests_unavailable": 0,
                    "requests_error": 0,
                    "error_codes": {},
                    "qps": 5 / duration,
                    "rows_per_s": rows / duration,
                    "bytes_per_s": value * 200.0,
                    "rows_total": rows,
                    "bytes_total": rows * 200,
                    "latency_ms": {"p50": 1.0, "p95": 1.1, "p99": 1.2, "max": 1.3, "samples": 5},
                }
            )
            + "\n"
        )
STUBEOF

  # --- the stub cargo: records its argv and produces the two binaries the
  # --- driver then executes from that arm's own target directory.
  cat > "$SHIMBIN/cargo" <<'STUBEOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "${AB_SELFTEST_CARGO_LOG:-/dev/null}"
case "${1:-}" in
  build)
    # HONOURS `-p`, because real cargo does. Producing both binaries whatever was
    # asked for made the stub MORE PERMISSIVE than cargo -- the same fidelity
    # failure as the startup line and the argv, arriving at the package
    # selection. It is what let "each arm builds its own load generator" look
    # indistinguishable from "one shared client" on the filesystem.
    rel="${CARGO_TARGET_DIR:?stub cargo needs CARGO_TARGET_DIR}/release"
    mkdir -p "$rel"
    want=" $* "
    case "$want" in *" -p cqlite-flight "*)
      cp "${AB_SELFTEST_SHIMBIN:?}/stub-cqlite-flight" "$rel/cqlite-flight"
      chmod +x "$rel/cqlite-flight" ;;
    esac
    case "$want" in *" -p flight-loadgen "*)
      cp "${AB_SELFTEST_SHIMBIN:?}/stub-flight-loadgen" "$rel/flight-loadgen"
      chmod +x "$rel/flight-loadgen" ;;
    esac
    ;;
esac
exit 0
STUBEOF
  chmod +x "$SHIMBIN/cargo" "$SHIMBIN/stub-cqlite-flight" "$SHIMBIN/stub-flight-loadgen"

  # A served corpus big enough for the floors this case passes.
  # SPARSE, so the end-to-end sessions clear the DOCUMENTED corpus floor without
  # writing 256 MiB. The floors are no longer lowerable for a measurement, and
  # running the e2e cases as controls instead would mean the measurement path was
  # never executed -- which is the coverage hole this whole section exists to
  # close. `truncate` reports the size the census reads and costs no blocks.
  mkdir -p "$TMP/e2e-corpus/ks/tbl"
  truncate -s 150000000 "$TMP/e2e-corpus/ks/tbl/nb-1-big-Data.db"
  truncate -s 150000000 "$TMP/e2e-corpus/ks/tbl/nb-2-big-Data.db"
  # The compressed-corpus requirement is now ENFORCED, so a served SSTable
  # without a usable CompressionInfo.db is refused for a measurement.
  head -c 512 /dev/zero > "$TMP/e2e-corpus/ks/tbl/nb-1-big-CompressionInfo.db"
  head -c 512 /dev/zero > "$TMP/e2e-corpus/ks/tbl/nb-2-big-CompressionInfo.db"
  printf '{"version":2,"keyspace":"ks","table":"tbl","ddl":"CREATE TABLE ks.tbl (a int PRIMARY KEY)","limit":null,"predicates":[],"filter":null,"aggregation":null,"columns":null,"token_start":null,"token_end":null,"wraparound":false}\n' \
    > "$TMP/e2e-ticket.json"

  run_e2e() { # <work-dir> <extra driver args...>
    local wd="$1"; shift
    set +e
    AB_SELFTEST_SHIMBIN="$SHIMBIN" \
    AB_SELFTEST_CARGO_LOG="$TMP/cargo-argv.log" \
    PATH="$SHIMBIN:$PATH" \
      bash "$DRIVER" \
        --corpus "$TMP/e2e-corpus" --ticket-template "$TMP/e2e-ticket.json" \
        --work-dir "$wd" --repo "$SCRATCH" \
        --base-ref HEAD~1 --head-ref HEAD \
        --max-concurrent-scans 16 \
        --attest-local-storage 'selftest harness: scratch corpus on the test box, whose device model this probe does not recognise; the storage class is not the property these cases exercise' \
        --replicates 5 --step-duration 1s "$@" \
        > "$TMP/out.txt" 2> "$TMP/err.txt"
    RC=$?
    set -e
  }

  : > "$TMP/cargo-argv.log"
  run_e2e "$TMP/e2e-ss" --ramp 1 --no-prewarm
  if [ "$RC" = "0" ]; then
    ok "a complete single-stream session runs end to end under the shims"
  else
    bad "the driver could not complete a session (exit $RC): $(grep -m2 '^AB-3649: cause' "$TMP/err.txt" | tr '\n' ' ')"
  fi
  if ! anchored; then
    bad "a real session emitted a line without the AB-3649 anchor"
  else
    ok "every line of a real session carries the anchor"
  fi

  E2E_DIR="$(find "$TMP/e2e-ss" -maxdepth 1 -type d -name 'run-*' 2>/dev/null | head -1)"
  if [ -n "$E2E_DIR" ] && [ -f "$E2E_DIR/manifest.json" ]; then
    ok "the session wrote a manifest into its own run directory"
  else
    bad "no session directory with a manifest was produced"
  fi
  if [ "$(find "$TMP/e2e-ss" -maxdepth 2 -name '*-r0*.jsonl' 2>/dev/null | wc -l)" = "10" ]; then
    ok "all ten replicate files (5 pairs x 2 arms) were written"
  else
    bad "expected 10 replicate JSONL files, found $(find "$TMP/e2e-ss" -maxdepth 2 -name '*-r0*.jsonl' 2>/dev/null | wc -l)"
  fi
  # FINDING 2: the census reached the manifest, rather than defaulting to zero.
  if [ -n "$E2E_DIR" ] && python3 - "$E2E_DIR/manifest.json" <<'PYINNER'
import json
import sys

corpus = json.load(open(sys.argv[1], encoding="utf-8"))["corpus"]
raise SystemExit(
    0 if corpus["data_db_files"] == 2 and corpus["data_db_bytes"] == 300000000 else 1
)
PYINNER
  then
    ok "the manifest records the real corpus census, not zeros"
  else
    bad "the manifest's corpus census is wrong -- the AC requires the corpus size be stated"
  fi
  # The two arms really were built separately, with their own target dirs.
  if [ "$(grep -c 'target/release\|build --release' "$TMP/cargo-argv.log" 2>/dev/null || echo 0)" -ge 2 ]; then
    ok "each arm was built once, through its own cargo invocation"
  else
    bad "the two arms were not both built"
  fi
  # And the analyzer reads what the driver produced -- the two halves meeting is
  # the property no fixture can establish.
  if [ -n "$E2E_DIR" ]; then
    set +e
    python3 "$ANALYZER" --single-stream "$E2E_DIR/manifest.json" \
      > "$TMP/out.txt" 2> "$TMP/err.txt"
    RC=$?
    set -e
    if [ "$(verdict_token single-stream)" = "MEETS-TARGET" ] && [ "$RC" = "0" ]; then
      ok "the analyzer consumes a REAL driver manifest and renders a verdict"
    else
      bad "the analyzer could not read the driver's own output (verdict '$(verdict_token single-stream)', exit $RC)"
    fi
    if grep -q '^AB-3649: counterbalance base-first 3 head-first 2 residual 1 pair(s)$' "$TMP/out.txt"; then
      ok "the order the driver ACTUALLY ran is what the analyzer counts"
    else
      bad "the executed order did not reach the analyzer"
    fi
    if grep -q 'corroboration agreed (10 of 10 runs)' "$TMP/out.txt"; then
      ok "the admission ceiling was read back from every real server start"
    else
      bad "the admission readback did not survive a real session"
    fi
  fi

  # THE COLOURED-LOG SESSION. Left unfixed this was total: every startup regex
  # failing means every field NOT-OBSERVED, corroboration `none`, and -- after
  # round 10's ruling -- every real rig session refused as UNMEASURED.
  cp "$SHIMBIN/stub-cqlite-flight" "$SHIMBIN/stub-cqlite-flight.plain"
  cp "$SHIMBIN/stub-cqlite-flight-ansi" "$SHIMBIN/stub-cqlite-flight"
  run_e2e "$TMP/e2e-ansi" --ramp 1 --no-prewarm
  cp "$SHIMBIN/stub-cqlite-flight.plain" "$SHIMBIN/stub-cqlite-flight"
  if [ "$RC" = "0" ]; then
    ok "a session whose server logs are ANSI-coloured completes"
  else
    bad "a coloured server log broke the session (exit $RC): $(grep -m2 '^AB-3649: cause' "$TMP/err.txt" | tr '\n' ' ')"
  fi
  ANSI_DIR="$(find "$TMP/e2e-ansi" -maxdepth 1 -type d -name 'run-*' 2>/dev/null | head -1)"
  if [ -n "$ANSI_DIR" ] && grep -q $'\x1b\[' "$ANSI_DIR/logs/base-r01.server.log" 2>/dev/null; then
    ok "...and the captured log really does contain escape sequences"
  else
    bad "the coloured-log case did not actually produce a coloured log, so it proved nothing"
  fi
  if [ -n "$ANSI_DIR" ]; then
    set +e
    python3 "$ANALYZER" --single-stream "$ANSI_DIR/manifest.json" > "$TMP/out.txt" 2> "$TMP/err.txt"
    RC=$?
    set -e
    if [ "$(verdict_token single-stream)" != "UNMEASURED" ] \
       && grep -q 'corroboration agreed' "$TMP/out.txt"; then
      ok "every server field is still read back through the colour"
    else
      bad "colour defeated the startup readback: $(grep -m1 '^AB-3649: cause single-stream' "$TMP/err.txt")"
    fi
  fi

  # A ramp session, so the multi-step validation path runs for real too.
  run_e2e "$TMP/e2e-ut" --ramp 1,2 --no-prewarm
  if [ "$RC" = "0" ]; then
    ok "a complete concurrency-ramp session runs end to end"
  else
    bad "the ramp session failed (exit $RC): $(grep -m2 '^AB-3649: cause' "$TMP/err.txt" | tr '\n' ' ')"
  fi
  UT_DIR="$(find "$TMP/e2e-ut" -maxdepth 1 -type d -name 'run-*' 2>/dev/null | head -1)"
  if [ -n "$UT_DIR" ]; then
    set +e
    python3 "$ANALYZER" --utilization "$UT_DIR/manifest.json" > "$TMP/out.txt" 2> "$TMP/err.txt"
    RC=$?
    set -e
    if [ "$(verdict_token utilization)" = "RISES" ]; then
      ok "a real ramp session yields a utilization verdict"
    else
      bad "the utilization section could not read a real ramp session ('$(verdict_token utilization)')"
    fi
  fi

  # THE SENSITIVITY CONTROL, RUN FOR REAL. Two individually-correct rules --
  # "refuse cross-arm config differences" and "asymmetric per-arm flags require
  # --control" -- made the runbook's own positive control unrunnable, and nothing
  # short of executing it would have shown that. This is the case that would have
  # caught it.
  run_e2e "$TMP/e2e-ctl" --ramp 1 --no-prewarm --control sensitivity     --head-server-extra '--max-batch-bytes 1'
  if [ "$RC" = "0" ]; then
    ok "the sensitivity control -- deliberately asymmetric -- completes a session"
  else
    bad "the sensitivity control could not run (exit $RC): $(grep -m2 '^AB-3649: cause' "$TMP/err.txt" | tr '
' ' ')"
  fi
  CTL_DIR="$(find "$TMP/e2e-ctl" -maxdepth 1 -type d -name 'run-*' 2>/dev/null | head -1)"
  if [ -n "$CTL_DIR" ]; then
    set +e
    python3 "$ANALYZER" --single-stream "$CTL_DIR/manifest.json"       > "$TMP/out.txt" 2> "$TMP/err.txt"
    RC=$?
    set -e
    if [ "$(verdict_token single-stream)" != "UNMEASURED" ]; then
      ok "the analyzer ANALYSES the sensitivity control instead of refusing it"
    else
      bad "the analyzer refused the sensitivity control: $(grep -m1 '^AB-3649: cause single-stream' "$TMP/err.txt")"
    fi
    if grep -q '^AB-3649: verdict-detail single-stream CONTROL the two arms were served under DIFFERENT ' "$TMP/out.txt"; then
      ok "...and still discloses that the arms were served differently"
    else
      bad "the control's asymmetry was not disclosed"
    fi
    # EXACTLY the declared difference, and nothing else: an observed value that
    # does not match what the manifest declared is still a refusal.
    python3 - "$CTL_DIR/manifest.json" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["expected_server_config"]["head"]["max_batch_bytes_observed"] = "999"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
    set +e
    python3 "$ANALYZER" --single-stream "$CTL_DIR/manifest.json"       > "$TMP/out.txt" 2> "$TMP/err.txt"
    RC=$?
    set -e
    if grep -q '^AB-3649: cause single-stream server-config-unexpected$' "$TMP/err.txt"; then
      ok "a control whose arms differ in a way the manifest did NOT declare is refused"
    else
      bad "an undeclared per-arm difference was permitted under a control label"
    fi
  fi

  # FINDING 1 (round 10): ONE load generator, so the client cannot vary with the
  # server commit. Checked on the filesystem the session actually produced.
  if [ -n "$E2E_DIR" ]; then
    if [ -x "$TMP/e2e-ss/target-loadgen/release/flight-loadgen" ] \
       && [ ! -e "$TMP/e2e-ss/target-base/release/flight-loadgen" ] \
       && [ ! -e "$TMP/e2e-ss/target-head/release/flight-loadgen" ]; then
      ok "exactly one load generator was built, and neither arm has its own"
    else
      bad "an arm built its own load generator, so the client varies with the server commit"
    fi
    if python3 - "$E2E_DIR/manifest.json" <<'PYINNER'
import json
import sys

manifest = json.load(open(sys.argv[1], encoding="utf-8"))
commits = {run.get("loadgen_commit") for run in manifest["runs"]}
raise SystemExit(
    0 if len(commits) == 1 and manifest["loadgen"]["commit"] in commits else 1
)
PYINNER
    then
      ok "every run records the same load-generator commit, and the manifest names it"
    else
      bad "the load-generator provenance is not recorded consistently"
    fi
  fi

  # The prewarm path is a separate branch and has its own way to be wrong.
  run_e2e "$TMP/e2e-warm" --ramp 1
  if [ "$RC" = "0" ]; then
    ok "a session with the warming pass enabled also completes"
  else
    bad "the prewarm branch failed (exit $RC): $(grep -m2 '^AB-3649: cause' "$TMP/err.txt" | tr '\n' ' ')"
  fi

  # THE MISNAMED CASE, FIXED. This ran `--temperature warm` into a directory
  # called `e2e-cold` and then inspected the WARM session -- so cold-cache
  # handling and `prewarm: false` were untested while the name asserted coverage.
  # Fifth case in this lane that did not test what it claimed.
  #
  # A genuine cold session needs passwordless sudo to drop the page cache, which
  # a suite may not assume. What IS testable, and is the property that matters
  # here, is that cold FAILS CLOSED without it -- a run that could not drop the
  # cache must not proceed and record itself as cold.
  if sudo -n true 2>/dev/null; then
    ok "cold fail-closed case skipped: this box HAS passwordless sudo, so the refusal cannot be provoked (declared, not assumed)"
  else
    run_e2e "$TMP/e2e-cold" --ramp 1 --temperature cold --no-prewarm
    if [ "$RC" = "2" ] && grep -q 'cold-drop-failed' "$TMP/err.txt"; then
      ok "a cold session without passwordless sudo fails closed rather than running warm"
    else
      bad "a cold session that could not drop the page cache did not fail closed (exit $RC)"
    fi
  fi
  # ...and `prewarm: false` is exercised for real by the --no-prewarm session,
  # which is the half that needs no privileges.
  SS_DIR="$(find "$TMP/e2e-ss" -maxdepth 1 -type d -name 'run-*' 2>/dev/null | head -1)"
  if [ -n "$SS_DIR" ] && python3 - "$SS_DIR" <<'PYINNER'
import json
import os
import sys

manifest = json.load(open(os.path.join(sys.argv[1], "manifest.json"), encoding="utf-8"))
raise SystemExit(0 if manifest["workload"]["prewarm"] is False else 1)
PYINNER
  then
    ok "a --no-prewarm session records prewarm: false"
  else
    bad "a --no-prewarm session did not record prewarm: false"
  fi

  # FINDING 3 (round 6): the manifest must record what HAPPENED, not what was
  # asked for -- checked against the warm session, where a pass really ran.
  WARM_DIR="$(find "$TMP/e2e-warm" -maxdepth 1 -type d -name 'run-*' 2>/dev/null | head -1)"
  if [ -n "$WARM_DIR" ] && python3 - "$WARM_DIR" <<'PYINNER'
import json
import os
import sys

root = sys.argv[1]
manifest = json.load(open(os.path.join(root, "manifest.json"), encoding="utf-8"))
recorded = manifest["workload"]["prewarm"]
happened = any(
    name.endswith(".prewarm.log")
    for name in os.listdir(os.path.join(root, "logs"))
)
raise SystemExit(0 if recorded == happened else 1)
PYINNER
  then
    ok "the manifest's prewarm flag matches whether a warming pass actually ran"
  else
    bad "the manifest records a prewarm value the session did not perform"
  fi

  # No server may outlive the session: the reap path runs for real here.
  if pgrep -f "$TMP/e2e-ss.*cqlite-flight" >/dev/null 2>&1 \
     || pgrep -f "$TMP/e2e-warm.*cqlite-flight" >/dev/null 2>&1; then
    bad "a stub server outlived its session"
  else
    ok "every server was reaped; none outlived its session"
  fi
  echo "  note    DECLARED GAP 1: the real cargo build, cqlite-flight and flight-loadgen are"
  echo "          exercised by nothing here -- these cases prove the DRIVER's logic only."
  echo "  note    DECLARED GAP 2: the stub is MORE PERMISSIVE than the real binary. It"
  echo "          parses its own argv, so an argument line Clap would REJECT still runs"
  echo "          here -- a duplicated option, an unknown flag, a bad value. That class"
  echo "          is covered structurally instead (the server-argv cases above assert no"
  echo "          option is emitted twice); nothing in this suite can reproduce Clap."
fi

echo
echo "-- one client for both arms, checked rather than assumed --"

mkfixture "$TMP/lg-split" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
python3 - "$TMP/lg-split/manifest.json" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for entry in manifest["runs"]:
    if entry["arm"] == "head":
        entry["loadgen_commit"] = "9" * 40
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
run_analyzer "$TMP/lg-split"
check_verdict "arms driven by different load generators" UNMEASURED 7 single-stream
check_cause "a client that varies with the server commit" loadgen-provenance-mismatch
run_analyzer "$TMP/meets"
if grep -q '^AB-3649: loadgen commit 2222222222222222222222222222222222222222 ref ' "$TMP/out.txt"; then
  ok "the load generator's provenance is reported, not merely checked"
else
  bad "the load-generator provenance was not reported"
fi

# A guard satisfiable by ABSENCE is not satisfied. One arm omitting its commit --
# or every run omitting it -- used to leave at most one value in the set, so the
# confound check passed with no evidence at all.
for absence in one all; do
  mkfixture "$TMP/lg-$absence" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
  python3 - "$TMP/lg-$absence/manifest.json" "$absence" <<'PYINNER'
import json
import sys

path, absence = sys.argv[1], sys.argv[2]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for entry in manifest["runs"]:
    if absence == "all" or entry["arm"] == "head":
        entry.pop("loadgen_commit", None)
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
  run_analyzer "$TMP/lg-$absence"
  check_verdict "loadgen provenance missing from $absence run(s)" UNMEASURED 7 single-stream
  check_cause "provenance absent ($absence)" loadgen-provenance-absent
done
# ...and a commit the manifest does not itself declare is a mismatch, not a pass.
mkfixture "$TMP/lg-undecl" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
python3 - "$TMP/lg-undecl/manifest.json" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["loadgen"]["commit"] = "8" * 40
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
run_analyzer "$TMP/lg-undecl"
check_verdict "runs naming a client the manifest does not declare" UNMEASURED 7 single-stream
check_cause "a client the manifest does not declare" loadgen-provenance-mismatch

# The analyzer re-checks compression rather than trusting the manifest's driver.
mkfixture "$TMP/uncompressed" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
python3 - "$TMP/uncompressed/manifest.json" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["corpus"]["compressed"] = False
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
run_analyzer "$TMP/uncompressed"
check_verdict "a measurement the manifest records as uncompressed" UNMEASURED 7 single-stream
check_cause "an uncompressed corpus" corpus-uncompressed
# ...and absence is not compression either.
python3 - "$TMP/uncompressed/manifest.json" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
del manifest["corpus"]["compressed"]
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
run_analyzer "$TMP/uncompressed"
check_verdict "a manifest that says nothing about compression" UNMEASURED 7 single-stream

echo
echo "-- requirements that can be disclosed but not honestly refused --"

# THE SWEEP. A rig class is not reliably derivable from a host string, so
# refusing on it would red a correct rig the day someone uses i4i.2xlarge -- the
# guard people learn to waive. Disclosed instead, and the disclosure is pinned.
mkfixture "$TMP/wronghost" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
python3 - "$TMP/wronghost/manifest.json" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["host"]["instance_type"] = "c7i.4xlarge"
manifest["host"]["loadavg1"] = "18.4"
manifest["host"]["contention"] = "CONTENDED"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
run_analyzer "$TMP/wronghost"
check_verdict "a session run off the named rig" MEETS-TARGET 0 single-stream
if grep -q '^AB-3649: verdict-detail single-stream HOST the acceptance criteria name the field i4i' "$TMP/out.txt"; then
  ok "a session off the named rig is disclosed, not silently scored"
else
  bad "running off the named rig was not disclosed"
fi
if grep -q '^AB-3649: verdict-detail single-stream HOST the one-minute load average' "$TMP/out.txt"; then
  ok "a contended box at session start is disclosed beside the verdict"
else
  bad "a loaded host was not disclosed"
fi
# ---- the properties the `i4i` label stood for -------------------------------
# CHECK THE PROPERTY, NOT THE LABEL. The acceptance criteria say "field i4i rig";
# what that stood for is a corpus on LOCAL NVMe on an UNCONTENDED box. Neither is
# derivable from a host string, and both are measurable. They are dispositioned
# DIFFERENTLY on purpose -- storage is a stable device fact and is REFUSED;
# contention is a decaying one-minute average and is DISCLOSED -- so each is
# pinned to the disposition it actually has, not to a shared idea of rigour.
for spec in \
  "NETWORK:nvme0n1 Amazon Elastic Block Store:refuse" \
  "NOT-MEASURABLE:- no device model:disclose" \
  "UNRECOGNISED:nvme0n1 Samsung SSD 980 PRO:unrecognised"
do
  token="${spec%%:*}"; rest="${spec#*:}"
  detail="${rest%%:*}"; want="${rest##*:}"
  mkfixture "$TMP/storage-$want" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
  AB_TOKEN="$token" AB_DETAIL="$detail" python3 - "$TMP/storage-$want/manifest.json" <<'PYINNER'
import json
import os
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["corpus"]["storage"] = os.environ["AB_TOKEN"]
manifest["corpus"]["storage_detail"] = os.environ["AB_DETAIL"]
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
  run_analyzer "$TMP/storage-$want"
  if [ "$want" = refuse ]; then
    check_verdict "a corpus on network storage" UNMEASURED 7 single-stream
    check_cause "a network-backed corpus" corpus-network-storage
  else
    # NOT-KNOWN-LOCAL IS NOT A VERDICT. Both of these used to disclose and score.
    # The criteria REQUIRE local NVMe, so a probe that could not tell leaves the
    # requirement unsatisfied -- and a four-state classifier is only as good as
    # the four-way disposition downstream of it.
    check_verdict "a corpus whose storage is $token" UNMEASURED 7 single-stream
    check_cause "a corpus whose storage is $token" corpus-storage-unverified
  fi
done

# A CLOSED TOKEN SET, because `!= "NETWORK"` accepts a typo -- and SILENCE is not
# NOT-MEASURABLE: a manifest that never asked and one that asked and could not
# tell are different facts, so the first refuses instead of inheriting the
# permissive branch. This is the sentinel rule applied to the record itself.
for spec in "corpus:storage:LOACL" "host:contention:probably fine"; do
  holder="${spec%%:*}"; rest="${spec#*:}"; key="${rest%%:*}"; value="${rest##*:}"
  mkfixture "$TMP/rigtok" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
  AB_H="$holder" AB_K="$key" AB_V="$value" python3 - "$TMP/rigtok/manifest.json" <<'PYINNER'
import json
import os
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest[os.environ["AB_H"]][os.environ["AB_K"]] = os.environ["AB_V"]
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
  run_analyzer "$TMP/rigtok"
  check_cause "an unrecognised $holder.$key token is refused, not read as 'not the bad one'" manifest-field
done
for spec in "corpus:storage" "host:contention"; do
  holder="${spec%%:*}"; key="${spec##*:}"
  mkfixture "$TMP/rigmissing" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
  AB_H="$holder" AB_K="$key" python3 - "$TMP/rigmissing/manifest.json" <<'PYINNER'
import json
import os
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
del manifest[os.environ["AB_H"]][os.environ["AB_K"]]
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
  run_analyzer "$TMP/rigmissing"
  check_cause "a manifest silent about $holder.$key refuses rather than passing as unmeasurable" manifest-field
done

# The contention disclosure is keyed on the RECORDED TOKEN, so NOT-MEASURABLE
# reports itself. The predecessor read it as a quiet box (`except ValueError:
# busy = False`) -- a third value crammed into a two-valued test.
mkfixture "$TMP/nocontention" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
python3 - "$TMP/nocontention/manifest.json" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["host"]["contention"] = "NOT-MEASURABLE"
manifest["host"]["loadavg1"] = "NOT-RECORDED"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
run_analyzer "$TMP/nocontention"
if grep -q '^AB-3649: verdict-detail single-stream HOST whether the box was contended could not be measured' "$TMP/out.txt"; then
  ok "an unmeasurable load probe is disclosed as a gap, not scored as a quiet box"
else
  bad "an unmeasurable load probe was silently read as quiet"
fi

# ---- resolved integers are parsed, bounded and CANONICAL ---------------------
# Two failures pointing opposite ways. `[0-9]+` accepted `08`, which Clap parses
# as 8 -- so the startup line echoed `8`, the read-back compared it against the
# string `08`, and a CORRECT session died on a mismatch that was not one, after
# the builds. And an oversized value reached Clap only at server launch, which is
# also after all three release builds on a metered box.
resolved_uint_case() { # <name> <batch> <scans> <want-rc> [<canonical-batch>]
  set +e
  python3 "$SUPPORT" resolve-session "$2" NOT-REQUESTED NOT-REQUESTED "$3" \
    268435456 2 1 '' '' '' > "$TMP/rs-out.txt" 2> "$TMP/rs-err.txt"
  local rc=$?
  set -e
  if [ "$rc" != "$4" ]; then
    bad "resolved uint $1 (exit $rc, expected $4)"
    return
  fi
  if [ -n "${5:-}" ]; then
    local got
    got="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["base"]["batch_size_observed"])' "$TMP/rs-out.txt" 2>/dev/null || echo PARSE-FAILED)"
    if [ "$got" != "$5" ]; then
      bad "resolved uint $1 canonicalised to '$got', expected '$5'"
      return
    fi
  fi
  ok "resolved uint $1 -> exit $rc${5:+ canonical $5}"
}
resolved_uint_case plain             8192 4 0 8192
# THE FALSE-MISMATCH HALF: a leading zero is a different STRING and the same
# NUMBER, and the read-back compares strings.
resolved_uint_case leading-zero      08192 4 0 8192
resolved_uint_case leading-zeros-only 0008 4 0 8
resolved_uint_case scans-leading-zero 8192 04 0 8192
# THE LATE-FAILURE HALF: refused here, not by Clap after three release builds.
resolved_uint_case batch-over-u64    99999999999999999999 4 1
resolved_uint_case scans-over-u64    8192 99999999999999999999 1
resolved_uint_case batch-zero        0 4 1
resolved_uint_case scans-zero        8192 0 1
resolved_uint_case batch-not-numeric 8k 4 1
# u64::MAX itself is in range; one past it is not. The boundary is the thing
# most likely to be written down wrong, so it is the thing pinned.
resolved_uint_case batch-at-u64-max  18446744073709551615 4 0 18446744073709551615
resolved_uint_case batch-past-u64-max 18446744073709551616 4 1
# The overflow message must NOT carry the zero-clamp explanation: that pairing
# printed two unrelated explanations of one number.
if python3 "$SUPPORT" resolve-session 99999999999999999999 NOT-REQUESTED NOT-REQUESTED 4 268435456 2 1 '' '' '' 2>&1 \
     | grep -q 'exceeds 18446744073709551615.*clamps 0 to one row'; then
  bad "the overflow message carries the zero-clamp note, which explains a different failure"
else
  ok "each resolved-integer failure carries only its own explanation"
fi

# ---- the ticket validator mirrors the DESERIALISER, both directions ----------
# Eighth validator-versus-consumer instance, and the first wrong in BOTH
# directions at once: too STRICT (it demanded `version`, which carries a serde
# default, so it red a ticket the consumer accepts) and too LOOSE (only four
# fields were looked at, so `"predicates": {}` passed pre-flight and failed after
# all three release builds). Both halves are pinned, because fixing one and not
# the other is how a validator ends up wrong in a new direction.
ticket_case() { # <name> <json> <schema-rc> <full-ring-rc>
  printf '%s\n' "$2" > "$TMP/tk-$1.json"
  set +e
  python3 "$SUPPORT" validate-ticket-schema "$TMP/tk-$1.json" >/dev/null 2>&1
  local schema_rc=$?
  python3 "$SUPPORT" validate-ticket "$TMP/tk-$1.json" >/dev/null 2>&1
  local full_rc=$?
  set -e
  if [ "$schema_rc" = "$3" ] && [ "$full_rc" = "$4" ]; then
    ok "ticket $1 -> schema $schema_rc, full-ring $full_rc"
  else
    bad "ticket $1 (schema $schema_rc want $3; full-ring $full_rc want $4)"
  fi
}

FULL_TICKET='{"version":2,"keyspace":"ks","table":"t","ddl":"CREATE TABLE ks.t (a int PRIMARY KEY)","snapshot":null,"token_start":null,"token_end":null,"wraparound":false,"columns":null,"predicates":[],"filter":null,"aggregation":null,"limit":null}'
ticket_case complete           "$FULL_TICKET" 0 0
# TOO STRICT, the half that reds a CORRECT ticket on the rig: `version` carries
# `#[serde(default = "default_ticket_version")]`, so a ticket without it
# deserialises fine and must be accepted by both halves.
# `version` is deliberately ABSENT here and PRESENT in every malformed case
# above, so each case isolates the field it names.
ticket_case no-version         '{"keyspace":"ks","table":"t","ddl":"CREATE TABLE ks.t (a int PRIMARY KEY)"}' 0 0
# NOT deny_unknown_fields, so the consumer IGNORES an unknown key -- rejecting it
# would refuse a ticket from a newer connector that the server reads fine.
ticket_case unknown-field      '{"keyspace":"ks","table":"t","ddl":"CREATE TABLE ks.t (a int PRIMARY KEY)","future_knob":7}' 0 0
# TOO LOOSE, the half that fails AFTER all three builds.
ticket_case predicates-object  '{"version":2,"keyspace":"ks","table":"t","ddl":"d","predicates":{}}' 1 1
ticket_case predicates-badop   '{"version":2,"keyspace":"ks","table":"t","ddl":"d","predicates":[{"column":"a","op":"NOPE","value":1}]}' 1 1
ticket_case predicates-nocol   '{"version":2,"keyspace":"ks","table":"t","ddl":"d","predicates":[{"op":"Equal","value":1}]}' 1 1
ticket_case wraparound-string  '{"version":2,"keyspace":"ks","table":"t","ddl":"d","wraparound":"yes"}' 1 1
ticket_case columns-not-strings '{"version":2,"keyspace":"ks","table":"t","ddl":"d","columns":[1,2]}' 1 1
ticket_case limit-negative     '{"version":2,"keyspace":"ks","table":"t","ddl":"d","limit":-1}' 1 1
ticket_case token-not-int      '{"version":2,"keyspace":"ks","table":"t","ddl":"d","token_start":"0"}' 1 1
ticket_case version-not-u8     '{"keyspace":"ks","table":"t","ddl":"d","version":300}' 1 1
ticket_case ddl-missing        '{"version":2,"keyspace":"ks","table":"t"}' 1 1
ticket_case ddl-null           '{"version":2,"keyspace":"ks","table":"t","ddl":null}' 1 1
ticket_case ddl-blank          '{"version":2,"keyspace":"ks","table":"t","ddl":"   "}' 1 1
# A JSON bool is a Python int; serde is not so forgiving and neither is this.
ticket_case version-bool       '{"keyspace":"ks","table":"t","ddl":"d","version":true}' 1 1
# THE TWO HALVES ARE SEPARATE: a narrowed ticket DESERIALISES fine, so the schema
# half accepts it and only the full-ring half refuses. That split is the whole
# reason a control can be schema-checked without being forced to be a full scan.
ticket_case narrowed-limit     '{"version":2,"keyspace":"ks","table":"t","ddl":"d","limit":10}' 0 1
ticket_case narrowed-columns   '{"version":2,"keyspace":"ks","table":"t","ddl":"d","columns":["a"]}' 0 1

# STRUCTURAL: the control branch must apply the SCHEMA half. It used to check
# only that the file was JSON, so a control wasted the same three release builds
# on a ticket the load generator could not read.
if grep -q 'validate-ticket-schema "\$TICKET_TEMPLATE"' "$DRIVER"; then
  ok "a control's ticket is schema-checked, not merely parsed as JSON"
else
  bad "the control branch does not run the ticket schema check"
fi

# THE ATTESTATION COVERS IGNORANCE, NEVER EVIDENCE. An operator may assert that
# an unrecognised device is local; nobody may assert that an IDENTIFIED network
# device is not -- otherwise the one thing this check exists to refuse is the one
# thing a flag turns off. Both directions are pinned, because only the pair says
# what the override means.
for spec in "UNRECOGNISED:nvme0n1 Samsung SSD 980 PRO:grants" "NETWORK:nvme0n1 Amazon Elastic Block Store:refuses"; do
  token="${spec%%:*}"; rest="${spec#*:}"
  detail="${rest%%:*}"; want="${rest##*:}"
  mkfixture "$TMP/attest-$want" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
  AB_TOKEN="$token" AB_DETAIL="$detail" python3 - "$TMP/attest-$want/manifest.json" <<'PYINNER'
import json
import os
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["corpus"]["storage"] = os.environ["AB_TOKEN"]
manifest["corpus"]["storage_detail"] = os.environ["AB_DETAIL"]
manifest["corpus"]["storage_attestation"] = "rig-2026-09: instance store confirmed by hand against the launch template"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
  run_analyzer "$TMP/attest-$want"
  if [ "$want" = grants ]; then
    check_verdict "an attested unrecognised device" MEETS-TARGET 0 single-stream
    if grep -q '^AB-3649: verdict-detail single-stream STORAGE-ATTESTED .*NOT independently verified' "$TMP/out.txt"; then
      ok "an attestation travels with the verdict, saying what it rests on"
    else
      bad "an attested verdict did not print the attestation beside itself"
    fi
  else
    check_verdict "an attested NETWORK device" UNMEASURED 7 single-stream
    check_cause "an attestation cannot overrule an identified network device" corpus-network-storage
  fi
done

# An attestation with nothing recorded in it is not an attestation.
mkfixture "$TMP/attest-blank" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
python3 - "$TMP/attest-blank/manifest.json" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["corpus"]["storage"] = "UNRECOGNISED"
manifest["corpus"]["storage_attestation"] = "   "
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
run_analyzer "$TMP/attest-blank"
check_cause "a blank attestation" manifest-field

# STRUCTURAL: the driver must not REFUSE on the storage probe, only warn. The
# refusal lives in the analyzer, where the false claim would be made. If it moved
# back into the driver, this suite's own end-to-end sessions would pass or fail
# depending on whether `df` names a resolvable device for the scratch directory
# -- which on this box it does not (`/dev/root`), so they would pass BY ACCIDENT
# here and refuse on a box that names a real one. Pinned structurally because the
# behavioural case cannot be written without a network-backed path to point at.
if python3 - "$DRIVER" <<'PYINNER'
import re
import sys

source = open(sys.argv[1], encoding="utf-8").read()
# ANCHORED ON THE FULL CONDITION. A second `AB_STORAGE" = "NETWORK"` test now
# exists inside the attestation block -- which DOES `die`, deliberately, because
# an attestation may not overrule an identified network device -- so a loose
# anchor would read that block's body and report the wrong branch.
window = re.search(
    r'AB_STORAGE" = "NETWORK" \] && \[ -z "\$CONTROL" \]; then.*?\nfi\n',
    source, re.S)
if not window:
    sys.stderr.write("AB-3649: the driver's NETWORK-storage branch was not found\n")
    raise SystemExit(1)
body = window.group(0)
if re.search(r"^\s*die ", body, re.M):
    sys.stderr.write("AB-3649: the driver refuses on network storage; that refusal "
                     "belongs in the analyzer\n")
    raise SystemExit(1)
if "warn " not in body:
    sys.stderr.write("AB-3649: the driver neither warns nor refuses on network storage\n")
    raise SystemExit(1)
if "analyze-ab.py refuses it" not in body:
    sys.stderr.write("AB-3649: the warning does not name the refusal the operator "
                     "will hit at analysis\n")
    raise SystemExit(1)
PYINNER
then
  ok "the driver warns on network storage and names the analysis refusal, rather than refusing itself"
else
  bad "the driver's network-storage handling is not warn-and-name (see stderr above)"
fi

# THE FALL-THROUGH. The probe's first version returned LOCAL for any model that
# was not EBS, so an NFS-backed loop device or another cloud's network volume
# passed as a local disk -- a pass derived from the ABSENCE of a bad signal, in
# the one check added to stop exactly that. Pinned by calling the classifier
# directly, because no path on this box reports a third-party model.
if python3 - "$HERE" <<'PYINNER'
import sys

sys.path.insert(0, sys.argv[1])
import ab_driver_support as S

cases = {
    "Amazon Elastic Block Store": "NETWORK",
    "Amazon EC2 NVMe Instance Storage": "LOCAL",
    "Samsung SSD 980 PRO": "UNRECOGNISED",
    "NetApp LUN C-Mode": "UNRECOGNISED",
    "": "NOT-MEASURABLE",
}
problems = []
for model, want in cases.items():
    got = S.classify_storage_model(model)
    if got != want:
        problems.append("model %r classified %s, expected %s" % (model, got, want))
for problem in problems:
    sys.stderr.write("AB-3649: %s\n" % problem)
raise SystemExit(1 if problems else 0)
PYINNER
then
  ok "an unrecognised device model is UNRECOGNISED, not waved through as local"
else
  bad "the storage classifier mis-sorts a device model (see stderr above)"
fi

if python3 - "$DRIVER" <<'PYINNER'
import re
import sys

source = open(sys.argv[1], encoding="utf-8").read()
block = re.search(r'if \[ -n "\$ATTEST_LOCAL_STORAGE" \]; then.*?\n^fi$',
                  source, re.S | re.M)
if not block:
    sys.stderr.write("AB-3649: the driver has no attestation block\n")
    raise SystemExit(1)
body = block.group(0)
if 'AB_STORAGE" = "NETWORK"' not in body:
    sys.stderr.write("AB-3649: the attestation block does not check for an "
                     "identified NETWORK device, so an attestation could "
                     "overrule evidence\n")
    raise SystemExit(1)
if not re.search(r"^\s*die corpus-network-storage-attested", body, re.M):
    sys.stderr.write("AB-3649: the attestation block does not REFUSE on an "
                     "identified NETWORK device\n")
    raise SystemExit(1)
PYINNER
then
  ok "an attestation cannot be used to overrule an identified network device"
else
  bad "the driver's attestation block does not refuse identified network storage (see stderr above)"
fi

# The storage PROBE itself, against real paths. `/` on this box is EBS-backed, so
# the NETWORK arm is exercised against a genuine device rather than a mock; a box
# where it is not simply reports LOCAL or NOT-MEASURABLE, all three of which are
# recognised tokens, which is the property under test.
probe_out="$(python3 "$SUPPORT" probe-storage / 2>/dev/null || echo 'FAILED - -')"
case "${probe_out%% *}" in
  LOCAL|NETWORK|UNRECOGNISED|NOT-MEASURABLE)
    ok "probe-storage answers with a recognised token for a real path" ;;
  *)
    bad "probe-storage answered '${probe_out%% *}', which is not a recognised token" ;;
esac
if python3 "$SUPPORT" probe-storage /no/such/path 2>/dev/null | grep -q '^NOT-MEASURABLE '; then
  ok "probe-storage reports an unresolvable path as NOT-MEASURABLE, not as local"
else
  bad "probe-storage did not report an unresolvable path as NOT-MEASURABLE"
fi

run_analyzer "$TMP/meets"
if grep -q '^AB-3649: verdict-detail single-stream HOST ' "$TMP/out.txt"; then
  bad "an i4i session on a quiet box printed a host disclosure it does not need"
else
  ok "an i4i session on a quiet box carries no host disclosure"
fi
if [ "$(grep -c '^AB-3649: sections-coverage this run covers single-stream' "$TMP/out.txt")" = 2 ]; then
  ok "a single-section report names the missing quantity, before AND after the verdict"
else
  bad "a single-section report did not name the missing quantity twice"
fi
if grep -q '^AB-3649: sections-coverage .*does NOT cover utilization' "$TMP/out.txt"; then
  ok "the coverage note names the quantity that is missing, not just that one is"
else
  bad "the coverage note did not name the absent quantity"
fi

echo
echo "-- the none refusal covers EVERY readback, not just admission --"

# The ruling was about corroboration as such; it had been applied to the one
# field under discussion, so a session whose batch size was never observed still
# rendered a verdict -- and the batch size is the mechanism under measurement.
for field in batch_size_observed max_batch_bytes_observed wait_timeout_ms_observed; do
  mkfixture "$TMP/none-$field" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
  python3 - "$TMP/none-$field/manifest.json" "$field" <<'PYINNER'
import json
import sys

path, field = sys.argv[1], sys.argv[2]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
for entry in manifest["runs"]:
    entry[field] = "NOT-OBSERVED"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
  run_analyzer "$TMP/none-$field"
  check_verdict "a session where $field was never observed" UNMEASURED 7 single-stream
  check_cause "no observation of $field" startup-unobserved
done

echo
echo "-- the analyzer enforces the DOCUMENTED floors, not the recorded ones --"

# THE THIRD ROUTE TO #3058's BYPASS: the operator simply lowers the bar. The
# analyzer must not derive validity from a threshold the session under test
# chose, so it checks the documented minimum and ignores `min_*_required`.
for breach in files bytes; do
  mkfixture "$TMP/floor-$breach" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
  python3 - "$TMP/floor-$breach/manifest.json" "$breach" <<'PYINNER'
import json
import sys

path, breach = sys.argv[1], sys.argv[2]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
if breach == "files":
    manifest["corpus"]["data_db_files"] = 1
    manifest["corpus"]["min_sstables_required"] = 1
else:
    manifest["corpus"]["data_db_bytes"] = 4096
    manifest["corpus"]["min_bytes_required"] = 1
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
  run_analyzer "$TMP/floor-$breach"
  check_verdict "a measurement whose corpus is below the $breach floor" UNMEASURED 7 single-stream
  check_cause "a corpus below the documented $breach floor" corpus-below-floor
done
# ...and a labelled control may sit below them, because its verdict is disclaimed.
python3 - "$TMP/floor-files/manifest.json" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    manifest = json.load(handle)
manifest["control"] = "floor-probe"
with open(path, "w", encoding="utf-8") as handle:
    json.dump(manifest, handle, indent=1, sort_keys=True)
PYINNER
run_analyzer "$TMP/floor-files"
check_verdict "a labelled control below the floors" MEETS-TARGET 0 single-stream

echo
echo "-- every shared field is reconciled, or excused by name --"

# THE SWEEP, MECHANISED. Nine shared fields were reconciled one at a time, each
# after a review found it missing; the tenth (`shape`) is what made the pattern
# undeniable. These cases assert COMPLETENESS in both directions, so an
# unreconciled field cannot join quietly: every key of a REAL step record must
# appear in RECORD_FIELD_DISPOSITION, and every `workload` key of a REAL manifest
# must appear in WORKLOAD_DISPOSITION.
if [ -n "$E2E_DIR" ] && [ -f "$E2E_DIR/base-r01.jsonl" ]; then
  RECON_RECORD="$E2E_DIR/base-r01.jsonl"
  RECON_MANIFEST="$E2E_DIR/manifest.json"
else
  mkfixture "$TMP/recon" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
  RECON_RECORD="$TMP/recon/base-r01.jsonl"
  RECON_MANIFEST="$TMP/recon/manifest.json"
fi
if python3 - "$HERE" "$RECON_RECORD" "$RECON_MANIFEST" <<'PYINNER'
import json
import sys

sys.path.insert(0, sys.argv[1])
import ab_input as I

problems = []
with open(sys.argv[2], encoding="utf-8") as handle:
    record = json.loads(handle.readline())
for key in sorted(record):
    if key not in I.RECORD_FIELD_DISPOSITION:
        problems.append("step-record field %r has no disposition" % key)
for key, (state, why) in sorted(I.RECORD_FIELD_DISPOSITION.items()):
    if state not in ("reconciled", "checked", "excused"):
        problems.append("record field %r has an unknown disposition %r" % (key, state))
    if not why.strip():
        problems.append("record field %r has an empty reason" % key)

workload = json.load(open(sys.argv[3], encoding="utf-8"))["workload"]
for key in sorted(workload):
    if key not in I.WORKLOAD_DISPOSITION:
        problems.append("workload field %r has no disposition" % key)
for key, (state, why) in sorted(I.WORKLOAD_DISPOSITION.items()):
    if state not in ("constrains", "excused"):
        problems.append("workload field %r has an unknown disposition %r" % (key, state))
    if not why.strip():
        problems.append("workload field %r has an empty reason" % key)

# A table that describes nothing would satisfy every check above.
if len(I.RECORD_FIELD_DISPOSITION) < 15 or len(I.WORKLOAD_DISPOSITION) < 10:
    problems.append("a disposition table has shrunk below its floor")
for problem in problems:
    sys.stderr.write("AB-3649: %s\n" % problem)
raise SystemExit(1 if problems else 0)
PYINNER
then
  ok "every field of a real step record and a real manifest carries a disposition"
else
  bad "a shared field has no recorded disposition (see stderr above)"
fi

# ...and `shape`, the field that prompted the table, is actually reconciled.
mkfixture "$TMP/shape-drift" 6 "100000:116000,100000:117000,100000:118000,100000:119000,100000:120000,100000:117500"
python3 - "$TMP/shape-drift/head-r03.jsonl" <<'PYINNER'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    record = json.loads(handle.read())
record["shape"] = "limit-k"
with open(path, "w", encoding="utf-8") as handle:
    handle.write(json.dumps(record) + "\n")
PYINNER
run_analyzer "$TMP/shape-drift"
check_verdict "a record produced under a shape the manifest does not declare" UNMEASURED 7 single-stream
check_cause "a record whose shape contradicts the manifest" shape-record-mismatch

echo
echo "-- cross-references in FINDINGS.md name the section they point at --"

# I have now introduced a stale `§n` twice while renumbering, in consecutive
# rounds, and this repository treats a reference that no longer says what it
# used to as decay rather than cosmetics. A bare `§13` cannot be checked -- both
# the old and the new number exist -- so every cross-reference carries a short
# title and this case asserts the title still matches that section's heading.
if [ ! -f "$HERE/FINDINGS.md" ]; then
  ok "cross-reference check skipped: FINDINGS.md is not beside this suite (declared, not assumed)"
elif python3 - "$HERE/FINDINGS.md" <<'PYINNER'
import re
import sys

text = open(sys.argv[1], encoding="utf-8").read()
headings = {
    int(number): title.lower()
    for number, title in re.findall(r"^## (\d+)\.\s+(.*)$", text, re.M)
}
flat = " ".join(text.split())
bad = []
for number, title in re.findall(r"§(\d+)\s*\(([^)]+)\)", flat):
    number = int(number)
    if number not in headings:
        bad.append("§%d names no section" % number)
    elif title.strip().lower().rstrip(".") not in headings[number]:
        bad.append("§%d says %r but that section is %r" % (number, title, headings[number]))
# EVERY INTERNAL reference needs a title, not only the lesson ones. Scoping this
# to sections 9+ was my reasoning that the fact sections are stable -- and a bare
# `\u00a76` pointing at what is actually \u00a75 got straight past it, because a
# renumbering shifts fact sections too once one is inserted among them. What a
# bare number is legitimately used for is a reference to ANOTHER document, which
# always names it: `RUNBOOK.md \u00a72`, `phase2-verify-row-engine.md \u00a73.2`,
# or a `>` quotation carrying someone else's numbering. So the exemption is
# "names a document or is quoted", not "is a low number".
lines = text.splitlines()
bare = []
for line in lines:
    if line.lstrip().startswith(">") or ".md" in line:
        continue
    # `\u00a7(\d+)(?!\s*\()` BACKTRACKS: on `\u00a718 (title)` the greedy `\d+`
    # takes 18, the lookahead fails, and it retries with `1` -- which passes,
    # because the next character is `8`. So a correctly titled reference reported
    # itself as a bare one, and `\u00a73.2` (another document's section) reported
    # as `\u00a73`. `(?![\d.])` pins the number to its whole self and exempts a
    # dotted foreign reference at the same time.
    for n in re.findall(r"\u00a7(\d+)(?![\d.])(?!\s*\()", line):
        if int(n) in headings:
            bare.append(n)
if bare:
    bad.append(
        "internal references with no title: %s -- a bare number cannot be checked "
        "against the section it points at" % ", ".join(sorted(set(bare))))
if not re.search(r"§\d+\s*\(", flat):
    bad.append("no titled cross-references found, so this guard proved nothing")
for problem in bad:
    sys.stderr.write("AB-3649: %s\n" % problem)
raise SystemExit(1 if bad else 0)
PYINNER
then
  ok "every FINDINGS.md cross-reference names the section it points at, and matches it"
else
  bad "a cross-reference does not match its section (see stderr above)"
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
