"""
Manifest and replicate-JSONL loading for #3649, with every refusal named.

Nothing here renders a verdict; it either returns validated pairs or raises
`Unmeasured` with a cause an operator can act on. A positive result requires an
affirmative measurement, so every branch that cannot establish something raises
rather than defaulting.

MODES
-----
`single-stream` -- the manifest's ramp must be exactly `1` and each run file
must hold exactly ONE step record. This is the quantity the target band applies
to.

`utilization` -- the manifest's ramp must have two or more steps and each run
file must hold exactly that many records. The comparison quantity is the PEAK
`rows_per_s` across the ramp, and the two arms of a pair must have the SAME
surviving concurrency ladder or they are not comparable.

ADMISSION CONTROL IS A CONFOUNDER, NOT A DETAIL
-----------------------------------------------
`cqlite-flight` admits a bounded number of concurrent `do_get` scans (#2420,
WS4; `cqlite-flight/src/cli.rs:59-73`). Past the ceiling a request waits
`--admission-wait-timeout-ms` and is then SHED with gRPC `UNAVAILABLE`, which
`flight-loadgen` counts separately as `requests_unavailable`. A ramp step above
the ceiling therefore measures THE ADMISSION CEILING, not merge throughput --
and it looks like a plateau, which is exactly the shape someone would misread as
saturation.

So: a shed step is NOT COMPARABLE. In `single-stream` mode -- where a shed at
concurrency 1 can only mean something is badly wrong -- it is a refusal. In
`utilization` mode it is EXCLUDED and every exclusion is reported as an explicit
fact, with the surviving ladder required to match between the arms. And the
resolved admission ceiling OBSERVED at each server's startup must agree across
every run, or the arms were not run under the same admission setting and the
ratio is not a ratio.
"""

import json
import os

from ab_common import Unmeasured

SCHEMA_MANIFEST = "ab-3649.manifest/v1"
SCHEMA_STEP = "flight-loadgen.step/v1"

MODE_SINGLE_STREAM = "single-stream"
MODE_UTILIZATION = "utilization"

# A value the driver writes when it could not read the server's own startup line.
NOT_OBSERVED = "NOT-OBSERVED"


def _require(obj, key, kinds, where):
    if key not in obj:
        raise Unmeasured("manifest-field", "%s: missing field %r" % (where, key))
    value = obj[key]
    if not isinstance(value, kinds) or isinstance(value, bool) and kinds is int:
        raise Unmeasured(
            "manifest-field",
            "%s: field %r has the wrong type (%s)" % (where, key, type(value).__name__),
        )
    return value


def ramp_steps(manifest):
    """The concurrency ladder the manifest declares, as a list of ints."""
    raw = manifest.get("workload", {})
    raw = raw.get("ramp") if isinstance(raw, dict) else None
    if not isinstance(raw, str) or not raw.strip():
        raise Unmeasured(
            "manifest-field", "manifest.workload.ramp is missing or not a string"
        )
    steps = []
    for part in raw.split(","):
        part = part.strip()
        if not part.isdigit() or int(part) < 1:
            raise Unmeasured(
                "manifest-field",
                "manifest.workload.ramp is %r, which is not a comma-separated list "
                "of positive integers" % raw,
            )
        steps.append(int(part))
    return steps


def load_manifest(path, mode):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            raw = handle.read()
    except OSError as exc:
        raise Unmeasured(
            "manifest-unreadable", "%s: %s" % (path, exc.strerror or "unreadable")
        )
    try:
        manifest = json.loads(raw)
    except ValueError as exc:
        raise Unmeasured("manifest-not-json", "%s: %s" % (path, exc))
    if not isinstance(manifest, dict):
        raise Unmeasured("manifest-not-json", "%s: top level is not an object" % path)

    schema = manifest.get("schema")
    if schema != SCHEMA_MANIFEST:
        raise Unmeasured(
            "manifest-schema",
            "%s: schema is %r, expected %r" % (path, schema, SCHEMA_MANIFEST),
        )

    requested = _require(manifest, "replicates_requested", int, "manifest")
    if requested < 1:
        raise Unmeasured("manifest-field", "manifest: replicates_requested must be >= 1")
    _require(manifest, "arms", dict, "manifest")
    for arm in ("base", "head"):
        if arm not in manifest["arms"]:
            raise Unmeasured("manifest-field", "manifest: arms.%s is missing" % arm)
        _require(manifest["arms"][arm], "commit", str, "manifest.arms.%s" % arm)
    _require(manifest, "runs", list, "manifest")
    _require(manifest, "corpus", dict, "manifest")
    _require(manifest["corpus"], "data_db_bytes", int, "manifest.corpus")
    _require(manifest["corpus"], "data_db_files", int, "manifest.corpus")

    # A manifest pasted into the wrong section is a real mistake, so it gets its
    # own cause rather than surfacing later as a confusing record-count refusal.
    steps = ramp_steps(manifest)
    if mode == MODE_SINGLE_STREAM and steps != [1]:
        raise Unmeasured(
            "mode-manifest-mismatch",
            "%s declares ramp %r, which is a concurrency ramp, but it was supplied "
            "as the SINGLE-STREAM manifest; the target band applies to the "
            "single-stream quantity only" % (path, manifest["workload"]["ramp"]),
        )
    if mode == MODE_UTILIZATION and len(steps) < 2:
        raise Unmeasured(
            "mode-manifest-mismatch",
            "%s declares ramp %r, a single point, but it was supplied as the "
            "UTILIZATION manifest; utilization throughput is measured over a "
            "concurrency ramp" % (path, manifest["workload"]["ramp"]),
        )
    return manifest, steps


def _read_records(path):
    if not os.path.exists(path):
        raise Unmeasured("run-file-unreadable", "%s: no such file" % path)
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            raw = handle.read()
    except OSError as exc:
        raise Unmeasured(
            "run-file-unreadable", "%s: %s" % (path, exc.strerror or "unreadable")
        )
    lines = [line for line in raw.splitlines() if line.strip()]
    if not lines:
        raise Unmeasured("run-file-empty", "%s: no JSONL records" % path)
    records = []
    for number, line in enumerate(lines, start=1):
        try:
            record = json.loads(line)
        except ValueError as exc:
            raise Unmeasured("run-file-not-jsonl", "%s line %d: %s" % (path, number, exc))
        if not isinstance(record, dict):
            raise Unmeasured(
                "run-file-not-jsonl", "%s line %d: not a JSON object" % (path, number)
            )
        records.append(record)
    return records


def _validate_shape(record, path, index):
    where = "%s record %d" % (path, index)
    if record.get("schema") != SCHEMA_STEP:
        raise Unmeasured(
            "run-record-schema",
            "%s: schema is %r, expected %r" % (where, record.get("schema"), SCHEMA_STEP),
        )
    for name, kinds in (
        ("requests_ok", int),
        ("requests_error", int),
        ("requests_unavailable", int),
        ("target_concurrency", int),
        ("duration_s", (int, float)),
        ("rows_per_s", (int, float)),
        ("qps", (int, float)),
        ("rows_total", int),
        ("latency_ms", dict),
    ):
        if name not in record:
            raise Unmeasured(
                "run-record-field", "%s: step record is missing %r" % (where, name)
            )
        value = record[name]
        if isinstance(value, bool) or not isinstance(value, kinds):
            raise Unmeasured(
                "run-record-field",
                "%s: step record field %r has the wrong type" % (where, name),
            )
    for percentile in ("p50", "p95", "p99", "max"):
        if not isinstance(record["latency_ms"].get(percentile), (int, float)):
            raise Unmeasured(
                "run-record-field",
                "%s: latency_ms.%s missing or non-numeric" % (where, percentile),
            )
    # A request ERROR is a failure, never a load-shedding artifact, and it
    # disqualifies the step in either mode.
    if record["requests_error"] > 0:
        raise Unmeasured(
            "run-errors",
            "%s: requests_error=%d -- a step with any request error is not a "
            "throughput measurement" % (where, record["requests_error"]),
        )


def _validate_usable(record, path, index):
    """A step that is being COUNTED must carry real work."""
    where = "%s record %d" % (path, index)
    if record["requests_ok"] < 1:
        raise Unmeasured("run-degenerate", "%s: requests_ok=0" % where)
    if not record["duration_s"] > 0:
        raise Unmeasured("run-degenerate", "%s: duration_s is not positive" % where)
    if not record["rows_per_s"] > 0:
        raise Unmeasured(
            "run-degenerate",
            "%s: rows_per_s is not positive -- the scan returned no rows, which on "
            "a corpus of the required size means the ticket template does not name "
            "a table that is present" % where,
        )


class RunPoint(object):
    """One arm's comparable measurement for one replicate."""

    def __init__(self, rate, records, peak_concurrency, ladder, shed):
        self.rate = rate
        self.records = records
        self.peak_concurrency = peak_concurrency
        self.ladder = ladder
        self.shed = shed


def load_run(path, mode, declared_steps):
    records = _read_records(path)
    expected = 1 if mode == MODE_SINGLE_STREAM else len(declared_steps)
    if len(records) != expected:
        raise Unmeasured(
            "run-record-count",
            "%s: %d step records, expected exactly %d (the manifest declares ramp "
            "%s)" % (path, len(records), expected, declared_steps),
        )
    for index, record in enumerate(records, start=1):
        _validate_shape(record, path, index)

    if mode == MODE_SINGLE_STREAM:
        record = records[0]
        if record["requests_unavailable"] > 0:
            raise Unmeasured(
                "run-shed",
                "%s: requests_unavailable=%d at concurrency %d -- admission "
                "shedding (#2420) at single-stream concurrency means the server "
                "was not serving what was asked of it, so this is not a "
                "measurement of merge throughput"
                % (path, record["requests_unavailable"], record["target_concurrency"]),
            )
        _validate_usable(record, path, 1)
        return RunPoint(
            record["rows_per_s"], records, record["target_concurrency"], (1,), ()
        )

    # Utilization: exclude shed steps, report each exclusion, peak over the rest.
    shed = []
    surviving = []
    for index, record in enumerate(records, start=1):
        if record["requests_unavailable"] > 0:
            shed.append(
                (record["target_concurrency"], record["requests_unavailable"])
            )
            continue
        _validate_usable(record, path, index)
        surviving.append(record)
    if not surviving:
        raise Unmeasured(
            "ramp-fully-shed",
            "%s: every ramp step was admission-shed, so the run measured the "
            "admission ceiling and nothing else; raise --max-concurrent-scans to "
            "at least the top of the ramp" % path,
        )
    best = max(surviving, key=lambda r: r["rows_per_s"])
    ladder = tuple(sorted(r["target_concurrency"] for r in surviving))
    return RunPoint(
        best["rows_per_s"], surviving, best["target_concurrency"], ladder, tuple(shed)
    )


def admission_of(entry):
    """The admission ceiling OBSERVED at that run's server startup."""
    value = entry.get("admission_observed")
    if value is None:
        return NOT_OBSERVED
    return str(value)


def collect_pairs(manifest, manifest_dir, mode, declared_steps):
    """Resolve the manifest's declared runs into replicate-indexed pairs."""
    seen = {}
    admissions = {}
    for entry in manifest["runs"]:
        if not isinstance(entry, dict):
            raise Unmeasured("manifest-field", "manifest.runs: entry is not an object")
        arm = entry.get("arm")
        replicate = entry.get("replicate")
        filename = entry.get("file")
        if arm not in ("base", "head"):
            raise Unmeasured(
                "manifest-field", "manifest.runs: arm is %r, expected base|head" % arm
            )
        if not isinstance(replicate, int) or isinstance(replicate, bool):
            raise Unmeasured(
                "manifest-field", "manifest.runs: replicate is not an integer"
            )
        if not isinstance(filename, str) or not filename:
            raise Unmeasured("manifest-field", "manifest.runs: file is missing")
        key = (arm, replicate)
        if key in seen:
            raise Unmeasured(
                "duplicate-run",
                "manifest.runs declares arm %s replicate %d twice" % (arm, replicate),
            )
        path = filename
        if not os.path.isabs(path):
            path = os.path.join(manifest_dir, path)
        seen[key] = load_run(path, mode, declared_steps)
        admissions[key] = admission_of(entry)

    # Both arms must have been served under the SAME admission ceiling, or the
    # ratio is between two differently-throttled servers.
    observed = {v for v in admissions.values() if v != NOT_OBSERVED}
    if len(observed) > 1:
        raise Unmeasured(
            "admission-mismatch",
            "the runs record more than one observed --max-concurrent-scans value "
            "(%s); the arms were not served under the same admission ceiling, so "
            "their throughputs are not comparable" % ", ".join(sorted(observed)),
        )

    base_reps = sorted(rep for (arm, rep) in seen if arm == "base")
    head_reps = sorted(rep for (arm, rep) in seen if arm == "head")
    only_base = [rep for rep in base_reps if rep not in head_reps]
    only_head = [rep for rep in head_reps if rep not in base_reps]
    if only_base or only_head:
        raise Unmeasured(
            "unpaired-replicates",
            "replicates present for one arm only: base=%s head=%s -- an unpaired "
            "replicate cannot enter a paired analysis"
            % (only_base or "none", only_head or "none"),
        )
    if not base_reps:
        raise Unmeasured("unpaired-replicates", "manifest declares no runs at all")

    pairs = []
    for rep in base_reps:
        base, head = seen[("base", rep)], seen[("head", rep)]
        if mode == MODE_UTILIZATION and base.ladder != head.ladder:
            raise Unmeasured(
                "ramp-steps-not-comparable",
                "replicate %d compared different concurrency ladders after "
                "excluding admission-shed steps (base %s, head %s); a peak taken "
                "over different ladders is not a ratio"
                % (rep, list(base.ladder), list(head.ladder)),
            )
        pairs.append((rep, base, head))

    admission = observed.pop() if observed else NOT_OBSERVED
    return pairs, admission
