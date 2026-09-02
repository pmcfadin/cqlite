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
import math
import os
import re

from ab_common import Unmeasured

SCHEMA_MANIFEST = "ab-3649.manifest/v1"
SCHEMA_STEP = "flight-loadgen.step/v1"

#: EVERY FIELD A STEP RECORD CARRIES, AND WHAT IS DONE ABOUT IT.
#:
#: Nine fields have been reconciled against the manifest one at a time, each
#: added after a review found it missing: `target_concurrency`, `duration_s`,
#: `rows_total`, `round`, the admission ceiling, `max_batch_bytes`, the wait
#: timeout, the CPU affinity, the pair order -- and then `shape`, which is what
#: prompted this table. The recurrence is the evidence that adding them one at a
#: time IS the defect: nothing stopped a tenth field from being unreconciled,
#: because nothing enumerated the set.
#:
#: So the set is enumerated here, every entry carries a disposition, and
#: `selftest-analyze.sh` asserts that EVERY key of a real step record appears in
#: it. A field added to `flight-loadgen`'s record and not accounted for here
#: fails that case rather than being silently unchecked.
#:
#: DECLARED RESIDUAL, because the completeness is one-directional: this proves
#: every RECORD field is accounted for. It cannot prove that a MANIFEST field
#: which ought to constrain records has been remembered -- `WORKLOAD_DISPOSITION`
#: below is the mirror, and between them they cover both sides, but neither can
#: know about a constraint nobody thought of.
RECORD_FIELD_DISPOSITION = {
    "schema": ("checked", "asserted equal to the flight-loadgen schema tag"),
    "round": ("reconciled", "the manifest run entry's arm and replicate"),
    "target_concurrency": ("reconciled", "workload.ramp at this record's position"),
    "shape": ("reconciled", "workload.shape"),
    "duration_s": ("reconciled", "workload.step_duration_seconds, wide band"),
    "requests_ok": ("checked", "must be positive for a counted step"),
    "requests_error": ("checked", "must be zero"),
    "requests_unavailable": ("checked", "zero, or the step is excluded (utilization)"),
    "rows_per_s": ("checked", "finite and positive"),
    "qps": ("checked", "finite"),
    "rows_total": ("checked", "internally consistent with rows_per_s x duration_s"),
    "latency_ms": ("checked", "percentile keys present and numeric"),
    "error_codes": ("excused", "empty whenever requests_error is zero, which is asserted"),
    "endpoint": ("excused", "the port is ephemeral, so the manifest records no endpoint"),
    "seed": ("excused", "flight-loadgen's own ticket seed; this driver never sets it"),
    "ts_unix_ms": ("excused", "wall clock; nothing in the manifest constrains it"),
    "step": ("excused", "positional index, already implied by target_concurrency"),
    "bytes_per_s": ("excused", "no verdict reads it"),
    "bytes_total": ("excused", "no verdict reads it"),
}

#: The mirror: every `workload` field the driver records, and whether it
#: constrains a step record. Same completeness case, other direction.
WORKLOAD_DISPOSITION = {
    "shape": ("constrains", "record.shape"),
    "profile": ("excused",
                "the target band this session declared; it constrains no record "
                "field because it is about the BAND the ratio is compared to, "
                "not about the work the records describe. It is the analyzer's "
                "single source for that band -- see resolve_profile"),
    "ramp": ("constrains", "record.target_concurrency, positionally"),
    "step_duration_seconds": ("constrains", "record.duration_s, wide band"),
    "step_duration": ("excused", "display only; the canonical value is the seconds field"),
    "prewarm": ("excused", "describes a pass whose output goes to /dev/null"),
    "prewarm_requested": ("excused", "recorded beside the effective value"),
    "temperature": ("excused", "page-cache state; no record field reflects it"),
    "merge_path": ("excused", "the server does not log it, so nothing can corroborate it"),
    "server_cpus": ("excused", "verified against /proc at run time, not in the record"),
    "client_cpus": ("excused", "not verified; declared in FINDINGS.md"),
    "ticket_template": ("excused",
                        "the path of the FROZEN per-session copy every run read; "
                        "its content is validated at pre-flight and recorded in "
                        "ticket_content"),
    "ticket_original": ("excused",
                        "the mutable path the frozen copy was taken from, kept "
                        "for provenance only -- nothing reads it after the copy"),
    "ticket_sha256": ("excused",
                      "the frozen copy's digest; it proves every run read the "
                      "same bytes, which is the property the freeze exists for"),
    "ticket_content": ("excused",
                       "the frozen ticket itself, so a reader can see what was "
                       "served without the session directory still existing"),
    "max_concurrent_scans": ("excused", "corroborated from the server's startup line"),
    "batch_size": ("excused", "corroborated from the server's startup line"),
    "max_batch_bytes": ("excused", "corroborated from the server's startup line"),
    "admission_wait_timeout_ms": ("excused", "corroborated from the server's startup line"),
}

MODE_SINGLE_STREAM = "single-stream"
MODE_UTILIZATION = "utilization"

# A value the driver writes when it could not read the server's own startup line.
NOT_OBSERVED = "NOT-OBSERVED"


def _require(obj, key, kinds, where):
    # `key not in obj` on a STRING is a substring test, and `obj[key]` on a
    # string then raises TypeError -- so `arms.base = "commit cfa93fe99^"` used
    # to escape as an unanchored traceback instead of a named refusal.
    if not isinstance(obj, dict):
        raise Unmeasured(
            "manifest-field",
            "%s: expected an object, found %s" % (where, type(obj).__name__),
        )
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
        # NOT `str.isdigit()`: it is True for characters like the superscript
        # two, whose `int()` then raises -- an unanchored traceback from a
        # manifest field. An explicit ASCII-digit match has no such gap.
        if not re.fullmatch(r"[0-9]+", part) or int(part) < 1:
            raise Unmeasured(
                "manifest-field",
                "manifest.workload.ramp is %r, which is not a comma-separated list "
                "of positive integers" % raw,
            )
        steps.append(int(part))
    if not steps:
        raise Unmeasured("manifest-field", "manifest.workload.ramp is empty")
    # Strictly increasing, because the reconciliation below identifies a record
    # BY ITS POSITION in the ladder; duplicates would make that ambiguous.
    for earlier, later in zip(steps, steps[1:]):
        if later <= earlier:
            raise Unmeasured(
                "manifest-field",
                "manifest.workload.ramp is %r, which is not strictly increasing" % raw,
            )
    return steps


def step_duration_seconds(manifest):
    """The declared per-step hold, in seconds.

    Read from `workload.step_duration_seconds`, which the driver NORMALISES at
    pre-flight through the same grammar `flight-loadgen` uses. This analyzer does
    NOT re-parse the raw `step_duration` string, and that is the point: a second
    grammar is a second thing to drift, and when it drifted STRICTER it refused
    completed sessions -- `--step-duration 60` is a valid bare-seconds value to
    the load generator, so a session could build both arms, run every replicate
    and meter a rig, and then be declined over a missing unit suffix. On a box
    you cannot get back, a false refusal after the data exists cannot be
    recovered from. One canonical field, produced once, before the money is
    spent.
    """
    node = manifest.get("workload", {})
    value = node.get("step_duration_seconds") if isinstance(node, dict) else None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise Unmeasured(
            "manifest-field",
            "manifest.workload.step_duration_seconds is missing or non-numeric; "
            "the driver normalises the step duration at pre-flight and records it "
            "there, so a manifest without it did not come from a session this "
            "analyzer can reconcile",
        )
    if not math.isfinite(float(value)) or value <= 0:
        raise Unmeasured(
            "manifest-field",
            "manifest.workload.step_duration_seconds is %r, which is not a "
            "positive finite number of seconds" % value,
        )
    return float(value)


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
    _require(manifest, "host", dict, "manifest")
    # THE RIG PROPERTIES MUST BE RECORDED, AND RECORDED AS A KNOWN TOKEN. A
    # manifest SILENT about them is not the same as one recording
    # NOT-MEASURABLE: the first did not ask, the second asked and could not tell.
    # Silence therefore refuses here rather than inheriting the permissive branch
    # downstream -- the sentinel rule this lane keeps re-learning. The token set
    # is CLOSED for the same reason: `!= "NETWORK"` would accept a typo.
    for holder, key, allowed in (
        ("corpus", "storage", ("LOCAL", "NETWORK", "UNRECOGNISED", "NOT-MEASURABLE")),
        ("corpus", "compression",
         ("LZ4", "OTHER", "UNRECOGNISED", "UNPARSEABLE", "MISSING", "NO-SSTABLES")),
        ("host", "contention", ("QUIET", "CONTENDED", "NOT-MEASURABLE")),
    ):
        value = _require(manifest[holder], key, str, "manifest.%s" % holder)
        if value not in allowed:
            raise Unmeasured(
                "manifest-field",
                "manifest.%s.%s is %r, which is not one of %s"
                % (holder, key, value, "|".join(allowed)),
            )

    # THE ATTESTATION IS AN AUTHORIZATION, so its shape is checked rather than
    # merely read: a non-string, or a blank string, would otherwise reach the
    # verdict-adjacent disclosure as the only evidence for a criteria requirement.
    attestation = manifest["corpus"].get("storage_attestation")
    if attestation is not None:
        if not isinstance(attestation, str) or not attestation.strip():
            raise Unmeasured(
                "manifest-field",
                "manifest.corpus.storage_attestation is present but is not a "
                "non-empty string (%r); an attestation with nothing recorded in "
                "it is not an attestation" % (attestation,),
            )

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


def validate_record_shape(record, path, index, declared_shape):
    """PUBLIC because `ab_driver_support.validate_replicate` calls it.

    ONE VALIDATOR FOR ONE RECORD SCHEMA. The driver used to check a handful of
    fields by hand, so it accepted records the analyzer later refused -- and a
    malformed `latency_ms` reached a `.get` on a non-dict and produced an
    UNANCHORED TRACEBACK. A second validator would drift from this one within
    two rounds, and the drift would present exactly as that symptom: the driver
    passing what the analyzer rejects, after the rig is gone.
    """
    where = "%s record %d" % (path, index)
    # RECONCILED, like every other shared field: a manifest declaring `full` must
    # not reference records produced under a narrowed shape and still receive a
    # target-band verdict.
    if declared_shape is not None and record.get("shape") != declared_shape:
        raise Unmeasured(
            "shape-record-mismatch",
            "%s: the record was produced with shape %r but the manifest declares "
            "%r; the band this verdict is scored against is defined for the shape "
            "the manifest names"
            % (where, record.get("shape"), declared_shape),
        )
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


def validate_record_usable(record, path, index, expected_concurrency, declared_duration_s):
    """A step that is being COUNTED must carry real work, and must describe the
    step the manifest says it is."""
    where = "%s record %d" % (path, index)
    if record["requests_ok"] < 1:
        raise Unmeasured("run-degenerate", "%s: requests_ok=0" % where)

    # FINITENESS BEFORE POSITIVITY. `inf > 0` is True, so a non-finite rate used
    # to pass the positivity test, reach `geometric_mean` as `log(inf) = inf`,
    # and render `ci95% [NON-FINITE, NON-FINITE]` beside a confident verdict --
    # the renderer asked the question the decision never did. NaN is worse: it
    # makes the bootstrap's `draws.sort()` order arbitrarily, so the percentile
    # indices select meaningless elements.
    for name in ("duration_s", "rows_per_s", "qps"):
        value = record[name]
        if not math.isfinite(float(value)):
            raise Unmeasured(
                "run-non-finite",
                "%s: %s is %r -- a non-finite value is not a measurement, and it "
                "would reach the verdict rule as a comparison that silently "
                "succeeds" % (where, name, value),
            )
    if not record["duration_s"] > 0:
        raise Unmeasured("run-degenerate", "%s: duration_s is not positive" % where)
    if not record["rows_per_s"] > 0:
        raise Unmeasured(
            "run-degenerate",
            "%s: rows_per_s is not positive -- the scan returned no rows, which on "
            "a corpus of the required size means the ticket template does not name "
            "a table that is present" % where,
        )

    # DECLARED VERSUS OBSERVED. "A value we passed and a value the server
    # resolved are different facts" -- the principle this harness already applies
    # to the admission ceiling, now applied to the step records themselves.
    # Without it, records from an entirely different session (1.5-second steps at
    # concurrency 32) were analysed as concurrency-1 60-second steps and rendered
    # a valid-looking verdict.
    if record["target_concurrency"] != expected_concurrency:
        raise Unmeasured(
            "ramp-order-mismatch",
            "%s: target_concurrency is %d but the manifest declares %d at this "
            "position in the ramp; the records do not describe the session the "
            "manifest describes"
            % (where, record["target_concurrency"], expected_concurrency),
        )
    # A deliberately WIDE band: a step's measured elapsed legitimately exceeds
    # its hold by the tail of the last in-flight request, and a slow full scan
    # can overshoot substantially. It is here to catch records from a different
    # session, not to police timing.
    low = 0.5 * declared_duration_s
    high = 3.0 * declared_duration_s + 60.0
    if not low <= record["duration_s"] <= high:
        raise Unmeasured(
            "step-duration-mismatch",
            "%s: duration_s is %.3f, outside [%.3f, %.3f] for a declared step of "
            "%.3fs; these records did not come from the session this manifest "
            "describes" % (where, record["duration_s"], low, high, declared_duration_s),
        )
    # `rows_per_s` IS `rows_total / duration_s` in flight-loadgen, so these must
    # agree to float precision. Disagreement means the record was edited.
    implied = record["rows_per_s"] * record["duration_s"]
    if record["rows_total"] <= 0 or abs(implied - record["rows_total"]) > max(
        1e-3 * record["rows_total"], 1.0
    ):
        raise Unmeasured(
            "record-internally-inconsistent",
            "%s: rows_per_s x duration_s = %.3f but rows_total is %d; "
            "flight-loadgen computes the first from the second, so they cannot "
            "disagree in a record it produced"
            % (where, implied, record["rows_total"]),
        )


class RunPoint(object):
    """One arm's comparable measurement for one replicate."""

    def __init__(self, rate, records, peak_concurrency, ladder, shed):
        self.rate = rate
        self.records = records
        self.peak_concurrency = peak_concurrency
        self.ladder = ladder
        self.shed = shed


def load_run(path, mode, declared_steps, expected_round, declared_duration_s,
             declared_shape):
    records = _read_records(path)
    expected = 1 if mode == MODE_SINGLE_STREAM else len(declared_steps)
    if len(records) != expected:
        raise Unmeasured(
            "run-record-count",
            "%s: %d step records, expected exactly %d (the manifest declares ramp "
            "%s)" % (path, len(records), expected, declared_steps),
        )
    for index, record in enumerate(records, start=1):
        validate_record_shape(record, path, index, declared_shape)
        # The round label is stamped by the driver from the (arm, replicate) it
        # is running, so a file listed under the wrong entry is caught here
        # rather than silently analysed as the arm it is filed under.
        if record.get("round") != expected_round:
            raise Unmeasured(
                "round-label-mismatch",
                "%s record %d: round is %r but this file is listed as %r in the "
                "manifest" % (path, index, record.get("round"), expected_round),
            )

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
        validate_record_usable(record, path, 1, 1, declared_duration_s)
        return RunPoint(record["rows_per_s"], records, 1, (1,), ())

    # Utilization: exclude shed steps, report each exclusion, peak over the rest.
    shed = []
    surviving = []
    for index, record in enumerate(records, start=1):
        expected_concurrency = declared_steps[index - 1]
        if record["requests_unavailable"] > 0:
            # Even an EXCLUDED step must be the step it claims to be, or the
            # surviving-ladder comparison is over a ladder we did not verify.
            if record["target_concurrency"] != expected_concurrency:
                raise Unmeasured(
                    "ramp-order-mismatch",
                    "%s record %d: target_concurrency is %d but the manifest "
                    "declares %d at this position in the ramp"
                    % (path, index, record["target_concurrency"], expected_concurrency),
                )
            shed.append(
                (record["target_concurrency"], record["requests_unavailable"])
            )
            continue
        validate_record_usable(record, path, index, expected_concurrency, declared_duration_s)
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


#: `MaxConcurrentScansSource::as_str` (cqlite-flight/src/admission.rs:183-193).
#: Only `flag` means the value we passed is the value that took effect.
ADMISSION_SOURCES = ("flag", "env", "derived", "derived-fallback")


def admission_of(entry):
    """The admission ceiling OBSERVED at that run's server startup, and its
    provenance. Either may be NOT-OBSERVED, and they are separate facts."""
    value = entry.get("admission_observed")
    source = entry.get("admission_source")
    return (
        NOT_OBSERVED if value is None else str(value),
        NOT_OBSERVED if source is None else str(source),
    )


class Corroboration(object):
    """ONE type for every "the driver read this back from the server" fact.

    THREE INSTANCES OF ONE DEFECT SAID THE PER-FIELD APPROACH WAS THE PROBLEM.
    Round 2 was admission provenance counted as agreement; round 3 was a readback
    parsed and never compared; round 4 was a comparison run only over the subset
    that happened to parse. Each was fixed where it was found, and the next field
    inherited nothing. So the decision is made HERE, once, and every field is
    constructed through it -- a new field cannot be added without getting the
    partial case, because there is nowhere else to add one.

    THE POLICY, stated once:

      * values that were observed and DISAGREE  -> the caller refuses; two arms
        served under different configurations are not comparable;
      * observed for EVERY run                  -> `agreed`;
      * observed for SOME runs                  -> `partial`, disclosed with the
        counts. Not a refusal: the driver already asserts per-run equality
        against the requested value affirmatively and dies on a mismatch it can
        read, so an unread line costs corroboration, not correctness;
      * observed for NO run                     -> `none`, disclosed.

    `qualified` is separate from "has a value" because one field needs more than
    a value to corroborate anything: the admission ceiling also needs
    `admission_source == "flag"`, since a numeric ceiling the server says it
    DERIVED is not evidence that the pin we passed took effect.
    """

    def __init__(self, name, per_run):
        #: `per_run`: {run_key: (value, qualified_bool)}
        self.name = name
        self.total = len(per_run)
        self.values = sorted(
            {value for value, _ in per_run.values() if value != NOT_OBSERVED}
        )
        self.observed = sum(1 for _, ok in per_run.values() if ok)

    @property
    def value(self):
        return self.values[0] if len(self.values) == 1 else NOT_OBSERVED

    @property
    def disagrees(self):
        return len(self.values) > 1

    @property
    def state(self):
        if self.observed == 0:
            return "none"
        if self.observed < self.total:
            return "partial"
        return "agreed"

    @property
    def corroborated(self):
        return self.state == "agreed"


#: `MaxConcurrentScansSource::as_str` (cqlite-flight/src/admission.rs:183-193).
#: Only `flag` means the value we passed is the value that took effect.
ADMISSION_SOURCES = ("flag", "env", "derived", "derived-fallback")

#: Every server field the driver reads back from its own startup line. Adding one
#: here is all it takes: the Corroboration type supplies the rest.
SERVER_READBACK_FIELDS = (
    "batch_size_observed",
    "max_batch_bytes_observed",
    "wait_timeout_ms_observed",
)


def admission_of(entry):
    """The admission ceiling OBSERVED at that run's server startup, and its
    provenance. Either may be NOT-OBSERVED, and they are separate facts."""
    value = entry.get("admission_observed")
    source = entry.get("admission_source")
    return (
        NOT_OBSERVED if value is None else str(value),
        NOT_OBSERVED if source is None else str(source),
    )


def collect_pairs(manifest, manifest_dir, mode, declared_steps):
    """Resolve the manifest's declared runs into replicate-indexed pairs."""
    declared_duration_s = step_duration_seconds(manifest)
    declared_shape = manifest.get("workload", {})
    declared_shape = (
        declared_shape.get("shape") if isinstance(declared_shape, dict) else None
    )
    if not isinstance(declared_shape, str) or not declared_shape:
        raise Unmeasured(
            "manifest-field",
            "manifest.workload.shape is missing; the records cannot be reconciled "
            "against a shape the manifest does not name",
        )
    seen = {}
    admissions = {}
    positions = {}
    server_config = {}
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
        expected_round = "%s-r%02d" % (arm, replicate)
        seen[key] = load_run(
            path, mode, declared_steps, expected_round, declared_duration_s,
            declared_shape,
        )
        value, source = admission_of(entry)
        # An explicitly NON-FLAG provenance is a refusal, not a downgrade: the
        # server told us the ceiling came from somewhere other than the flag we
        # passed, so the run was served under a configuration we did not choose.
        if source != NOT_OBSERVED and source != "flag":
            raise Unmeasured(
                "admission-provenance",
                "%s replicate %d reports admission_source=%r, not 'flag': the "
                "ceiling in force came from somewhere other than the "
                "--max-concurrent-scans this session passed, so the run was "
                "served under a configuration this session did not choose"
                % (arm, replicate, source),
            )
        admissions[key] = (value, source)

        # The ACTUAL executed order, read from the record rather than inferred
        # from the parity rule that was supposed to produce it.
        position = entry.get("position_in_pair")
        if position not in (1, 2):
            raise Unmeasured(
                "position-not-recorded",
                "%s replicate %d does not record which half of its pair it ran in "
                "(position_in_pair=%r). Counterbalancing that is not recorded is "
                "counterbalancing that cannot be checked"
                % (arm, replicate, position),
            )
        positions[key] = position

        # Every readback field goes through the ONE type, including the runs
        # where it was NOT observed -- which is the bug this replaces: comparing
        # only the subset that parsed is how a partial observation became an
        # agreement for the third time.
        for name in SERVER_READBACK_FIELDS:
            observed = entry.get(name)
            observed = NOT_OBSERVED if observed is None else str(observed)
            server_config.setdefault(name, {})[key] = (
                observed,
                observed != NOT_OBSERVED,
            )

    # Both arms must have been served under the SAME admission ceiling, or the
    # ratio is between two differently-throttled servers.
    # The same type, with the extra qualification only this field needs.
    admission = Corroboration(
        "max_concurrent_scans",
        {
            key: (value, value != NOT_OBSERVED and source == "flag")
            for key, (value, source) in admissions.items()
        },
    )
    if admission.disagrees:
        raise Unmeasured(
            "admission-mismatch",
            "the runs record more than one observed --max-concurrent-scans value "
            "(%s); the arms were not served under the same admission ceiling, so "
            "their throughputs are not comparable" % ", ".join(admission.values),
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

    readbacks = {
        name: Corroboration(name, by_run) for name, by_run in server_config.items()
    }
    # A DECLARED DIFFERENCE IS NOT A MISMATCH -- but only exactly the declared
    # one, and only under a control label. Two individually-correct rules
    # collided here: "refuse cross-arm config differences" and "asymmetric
    # per-arm flags require --control". Together they made the runbook's own
    # sensitivity control unrunnable, which is worse than most defects in this
    # harness, because that control is what distinguishes "no effect" from "this
    # box cannot measure". The reconciliation is a structured expectation the
    # driver records as data, not a blanket rule with an exception.
    control = manifest.get("control")
    control = control if isinstance(control, str) and control else None
    expected = manifest.get("expected_server_config")
    expected = expected if isinstance(expected, dict) else {}

    def raw_declared(arm, field):
        """What the manifest says this arm was configured with, verbatim.

        `NOT-REQUESTED` is a REAL value here, meaning "the server default" -- and
        it is exactly the base arm's entry in the sensitivity control, where only
        the head arm overrides `--max-batch-bytes`. Collapsing it to "unknown"
        made that control's difference undeclared, which is the collision this
        whole block exists to resolve.
        """
        arm_expected = expected.get(arm)
        if not isinstance(arm_expected, dict):
            return None
        value = arm_expected.get(field)
        return None if value in (None, "") else str(value)

    def declared(arm, field):
        """The value an OBSERVATION must equal, or None when the driver could not
        know it -- a server default is not something this session chose."""
        value = raw_declared(arm, field)
        return None if value == "NOT-REQUESTED" else value

    for name, corroboration in sorted(readbacks.items()):
        by_arm = {}
        # NOT named `seen`: that is the outer dict of loaded run points, and
        # shadowing it here made `seen[("base", rep)]` index a bool further down.
        for (arm, _rep), (value, was_observed) in server_config[name].items():
            if was_observed:
                by_arm.setdefault(arm, set()).add(value)
        # Within one arm there is never a licensed difference.
        for arm, values in sorted(by_arm.items()):
            if len(values) > 1:
                raise Unmeasured(
                    "server-config-mismatch",
                    "the %s arm's runs report more than one observed %s (%s); one "
                    "arm was not served consistently, so nothing built on it is "
                    "comparable"
                    % (arm, name.replace("_observed", ""), ", ".join(sorted(values))),
                )
        # An arm that was observed must match what the manifest DECLARED for it.
        for arm, values in sorted(by_arm.items()):
            want = declared(arm, name)
            observed_value = next(iter(values))
            if want is not None and observed_value != want:
                raise Unmeasured(
                    "server-config-unexpected",
                    "the %s arm reports %s=%s but the manifest declares %s for it; "
                    "the server was not configured the way this session recorded"
                    % (arm, name.replace("_observed", ""), observed_value, want),
                )
        if not corroboration.disagrees:
            continue
        base_raw, head_raw = raw_declared("base", name), raw_declared("head", name)
        difference_declared = (
            base_raw is not None and head_raw is not None and base_raw != head_raw
        )
        if control and difference_declared:
            continue
        raise Unmeasured(
            "server-config-mismatch",
            "the arms report different observed %s (%s)%s; two arms served under "
            "different configurations are not comparable"
            % (
                name.replace("_observed", ""),
                ", ".join(corroboration.values),
                ""
                if difference_declared
                else " and the manifest declares no such difference",
            ),
        )

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

    # WHICH ARM RAN FIRST, per pair, and whether the counterbalancing happened.
    first_by_rep = {}
    for rep in base_reps:
        base_pos, head_pos = positions[("base", rep)], positions[("head", rep)]
        if base_pos == head_pos:
            raise Unmeasured(
                "position-not-recorded",
                "replicate %d records both arms in position %d of the pair; one of "
                "them ran first and the record does not say which"
                % (rep, base_pos),
            )
        first_by_rep[rep] = "base" if base_pos == 1 else "head"
    base_first = sum(1 for arm in first_by_rep.values() if arm == "base")
    head_first = len(first_by_rep) - base_first
    # Parity counterbalancing yields |base_first - head_first| == n mod 2, so a
    # difference greater than 1 means it did not happen. An odd replicate count
    # legitimately leaves one pair unbalanced; that residual is DISCLOSED in the
    # report rather than refused, because refusing it would red correct input.
    if abs(base_first - head_first) > 1:
        raise Unmeasured(
            "counterbalance-broken",
            "%d pairs ran base-first and %d ran head-first: the within-pair order "
            "was not counterbalanced, so a monotonic drift inside a pair lands on "
            "the same arm every time and biases every ratio in one direction -- "
            "with a tight interval, which is worse than a noisy one"
            % (base_first, head_first),
        )

    session = {
        "order_by_replicate": first_by_rep,
        "base_first": base_first,
        "head_first": head_first,
        "readbacks": readbacks,
    }
    return pairs, admission, session
