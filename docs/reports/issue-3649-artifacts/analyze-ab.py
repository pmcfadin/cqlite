#!/usr/bin/env python3
"""
analyze-ab.py -- the paired A/B throughput statistics + verdict for issue #3649.

WHAT QUESTION THIS ANSWERS
--------------------------
Issue #3649 asks for the served-path throughput effect of #2820 (the batched
k-way merge egress fan-in, commit cfa93fe99) measured on the field i4i narrow
rig, "with dispersion, not just a point estimate". This script consumes the
replicate JSONL that `ab-throughput.sh` produces and renders exactly one
verdict token against the target band, from a closed set.

WHY REPLICATES, AND WHY PAIRED
------------------------------
`flight-loadgen` emits per ramp step a latency HISTOGRAM (p50/p95/p99/max) but
a SINGLE POINT ESTIMATE of throughput (`qps` / `rows_per_s` / `bytes_per_s` =
count / duration_s -- see tools/flight-loadgen/src/record.rs). There is no
within-step throughput dispersion to recover, and the tool computes no interval.
Dispersion therefore has to come from REPEATED RUNS, and the runs have to be
INTERLEAVED (base, head, base, head, ...) so that host drift over the session is
shared by both arms rather than aliased onto whichever arm ran second. Replicate
i of base is then paired with replicate i of head, and the statistic is over the
per-pair RATIO -- which is what interleaving buys and what an unpaired
comparison throws away.

THE STATISTIC
-------------
  * per-pair ratio r_i = head_i.rows_per_s / base_i.rows_per_s
  * point estimate    = geometric mean of {r_i} (the natural centre for ratio
    data: the geometric mean of head/base is the reciprocal of the geometric
    mean of base/head, which is not true of the arithmetic mean)
  * interval          = percentile bootstrap over the PAIRS, fixed seed, so two
    runs over the same input produce byte-identical output
Each arm's own mean/median rows_per_s and its own bootstrap interval are printed
too, plus the latency percentiles, so a reader can see dispersion in the raw
arms and not only in the ratio.

THE TARGET, AND THE THING THAT IS NOT THE TARGET
------------------------------------------------
Target: ~1.1-1.25x narrow single-stream, ~1.05-1.1x wide
(docs/research/phase2-verify-row-engine.md section 3.2).

1.5-1.9x IS NOT A TARGET. Section 3.2 records it as a rig-narrow UTILIZATION
ceiling, explicitly "and unmeasured". This script NAMES it on every run and
NEVER tests against it. Testing against it and falling short would file a
phantom regression against #2820, which is a correct change.

THE VERDICT RULE (closed token set)
-----------------------------------
Let CI = [cl, ch] be the bootstrap interval of the ratio and [lo, hi] the target
band for the selected profile.

  1. CI entirely inside [lo, hi]        -> MEETS-TARGET
  2. else ch < lo                       -> BELOW-TARGET
  3. else cl > hi                       -> ABOVE-TARGET
  4. else                               -> INCONCLUSIVE
  any input the script cannot measure   -> UNMEASURED  (nonzero exit)

Rules 2 and 3 fire only when the WHOLE interval is on one side of the band, i.e.
only when the data RULE THE TARGET OUT. Rule 4 is everything else: an interval
that overlaps the band without being contained in it cannot distinguish "meets
the target" from "does not", and that is reported as such rather than rounded
into a number-shaped verdict. This is the single most important behaviour in the
file -- #3649 exists because a point estimate with overlapping intervals was
correctly refused once already (base 78.6 ms [69.5, 88.4] vs head 66.5 ms
[54.5, 83.2]: a ~15% point difference that measured the box as much as the
branch).

ONE READING DECISION, RECORDED RATHER THAN BURIED. "The interval overlaps 1.0"
is NOT by itself INCONCLUSIVE here. A TIGHT interval straddling 1.0 -- say
[0.99, 1.02] -- excludes the whole target band, so it is a MEASURED no-effect
(rule 2, BELOW-TARGET), which is a different fact from "the data cannot tell".
A WIDE interval straddling 1.0 also overlaps the band and lands in rule 4. Both
underlying facts are printed on their own `test` lines every run, so a reader
who prefers a different rule can apply it to the same numbers.

THE OUTPUT CANNOT BE PASTED AS A CERTIFICATION
-----------------------------------------------
This report gets pasted into a GitHub issue, so it is anchored the way
scripts/flow/base-staleness.sh is anchored:
  (a) EVERY line, stdout AND stderr, begins with `AB-3649: `.
  (b) Every dynamic field is control-character sanitized (newline, CR, other C0,
      DEL -> a visible escape), because an unsanitized value containing a
      newline emits a line with no prefix at all and breaks the one anchor
      everything else rests on. Values are otherwise printed verbatim.
  (c) The verdict appears ONLY on an `AB-3649: verdict ` line, carrying one
      token from the closed set. Prose goes on `verdict-detail` lines, so the
      verdict line's token position can never hold a word.
  (d) This script's own STATIC TEXT contains none of the reserved gate/review
      marker strings enumerated in selftest-analyze.sh -- asserted structurally
      over this source file by that self-test, which is a provable property,
      unlike a claim about one sample run.
DECLARED RESIDUAL: a manifest field is repository/operator-controlled and is
printed verbatim, so it CAN contain a reserved substring. The anchor is what
makes that harmless: every line it can land on is visibly an `AB-3649:` line.

EXIT CODES, AND THE CONSUMER CONTRACT
-------------------------------------
  0  MEETS-TARGET     measured
  4  BELOW-TARGET     measured
  5  ABOVE-TARGET     measured
  6  INCONCLUSIVE     measured
  7  UNMEASURED       NOT measured -- a consumer must treat this as no result,
                      never as a permissive default
  3  usage error      (also what --help exits with, deliberately: exit 0 means
                      MEETS-TARGET here, so a run that measured nothing at all
                      must never produce it)
Exit 0 is not an assertion of quality; it is the token MEETS-TARGET and nothing
more.
"""

import json
import math
import os
import random
import sys

PREFIX = "AB-3649: "

SCHEMA_MANIFEST = "ab-3649.manifest/v1"
SCHEMA_STEP = "flight-loadgen.step/v1"

# Target bands, docs/research/phase2-verify-row-engine.md section 3.2.
TARGET_BANDS = {
    "narrow": (1.10, 1.25),
    "wide": (1.05, 1.10),
}
# Named on every run, tested against never.
CEILING_TEXT = (
    "1.5-1.9x is a rig-narrow UTILIZATION ceiling recorded as unmeasured in "
    "docs/research/phase2-verify-row-engine.md section 3.2; it is NAMED here "
    "and is NOT a throughput target and is NOT tested against"
)

DEFAULT_SEED = 3649
DEFAULT_RESAMPLES = 10000
DEFAULT_CI_LEVEL = 0.95
DEFAULT_MIN_PAIRS = 3

VERDICT_EXIT = {
    "MEETS-TARGET": 0,
    "BELOW-TARGET": 4,
    "ABOVE-TARGET": 5,
    "INCONCLUSIVE": 6,
    "UNMEASURED": 7,
}
EXIT_USAGE = 3


# --------------------------------------------------------------------------
# Anchored, sanitized emission. Nothing in this file writes to a stream except
# through these two functions.
# --------------------------------------------------------------------------

def sanitize(text):
    """Render control characters visible so no value can break the anchor."""
    named = {"\n": "\\n", "\r": "\\r", "\t": "\\t"}
    chunks = []
    for ch in str(text):
        code = ord(ch)
        if ch in named:
            chunks.append(named[ch])
        elif code < 0x20 or code == 0x7F:
            chunks.append("\\x%02x" % code)
        else:
            chunks.append(ch)
    return "".join(chunks)


def out(line=""):
    sys.stdout.write(PREFIX + sanitize(line) + "\n")


def err(line=""):
    sys.stderr.write(PREFIX + sanitize(line) + "\n")


class Unmeasured(Exception):
    """Raised for every input this script cannot measure. Carries a named cause."""

    def __init__(self, cause, detail):
        super().__init__(cause)
        self.cause = cause
        self.detail = detail


# --------------------------------------------------------------------------
# Argument parsing. Hand-rolled rather than argparse, because argparse writes
# its usage and errors to stderr WITHOUT the prefix, which would break (a).
# --------------------------------------------------------------------------

HELP_LINES = [
    "analyze-ab.py --manifest <path> [options]",
    "",
    "  --manifest <path>    ab-3649.manifest/v1 written by ab-throughput.sh (required)",
    "  --profile <name>     target profile: narrow | wide            (default narrow)",
    "  --seed <int>         bootstrap seed, recorded in the report   (default %d)" % DEFAULT_SEED,
    "  --resamples <int>    bootstrap resamples                      (default %d)" % DEFAULT_RESAMPLES,
    "  --ci-level <float>   two-sided interval level, 0 < L < 1      (default %.2f)" % DEFAULT_CI_LEVEL,
    "  --min-pairs <int>    refuse to render below this many pairs   (default %d)" % DEFAULT_MIN_PAIRS,
    "  -h, --help           print this and exit %d" % EXIT_USAGE,
    "",
    "exit: 0 MEETS-TARGET  4 BELOW-TARGET  5 ABOVE-TARGET  6 INCONCLUSIVE",
    "      7 UNMEASURED (treat as no result)  3 usage",
]


def parse_args(argv):
    opts = {
        "manifest": None,
        "profile": "narrow",
        "seed": DEFAULT_SEED,
        "resamples": DEFAULT_RESAMPLES,
        "ci_level": DEFAULT_CI_LEVEL,
        "min_pairs": DEFAULT_MIN_PAIRS,
    }
    takes_value = {
        "--manifest": ("manifest", str),
        "--profile": ("profile", str),
        "--seed": ("seed", int),
        "--resamples": ("resamples", int),
        "--ci-level": ("ci_level", float),
        "--min-pairs": ("min_pairs", int),
    }
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg in ("-h", "--help"):
            for line in HELP_LINES:
                out(line)
            sys.exit(EXIT_USAGE)
        if arg not in takes_value:
            usage_error("unrecognised argument: %s" % arg)
        if i + 1 >= len(argv):
            usage_error("%s requires a value" % arg)
        key, caster = takes_value[arg]
        try:
            opts[key] = caster(argv[i + 1])
        except ValueError:
            usage_error("%s: not a valid value: %s" % (arg, argv[i + 1]))
        i += 2

    if opts["manifest"] is None:
        usage_error("--manifest is required")
    if opts["profile"] not in TARGET_BANDS:
        usage_error(
            "--profile must be one of: %s" % ", ".join(sorted(TARGET_BANDS))
        )
    if opts["resamples"] < 100:
        usage_error("--resamples must be at least 100")
    if not 0.0 < opts["ci_level"] < 1.0:
        usage_error("--ci-level must be strictly between 0 and 1")
    if opts["min_pairs"] < 2:
        usage_error("--min-pairs must be at least 2")
    return opts


def usage_error(detail):
    err("usage-error %s" % detail)
    for line in HELP_LINES:
        err(line)
    sys.exit(EXIT_USAGE)


# --------------------------------------------------------------------------
# Input: the manifest, then the per-run JSONL it names.
# --------------------------------------------------------------------------

def _require(obj, key, kinds, where):
    if key not in obj:
        raise Unmeasured("manifest-field", "%s: missing field %r" % (where, key))
    value = obj[key]
    if not isinstance(value, kinds):
        raise Unmeasured(
            "manifest-field",
            "%s: field %r has the wrong type (%s)" % (where, key, type(value).__name__),
        )
    return value


def load_manifest(path):
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
        raise Unmeasured(
            "manifest-field", "manifest: replicates_requested must be >= 1"
        )
    _require(manifest, "arms", dict, "manifest")
    for arm in ("base", "head"):
        if arm not in manifest["arms"]:
            raise Unmeasured("manifest-field", "manifest: arms.%s is missing" % arm)
        _require(manifest["arms"][arm], "commit", str, "manifest.arms.%s" % arm)
    _require(manifest, "runs", list, "manifest")
    _require(manifest, "corpus", dict, "manifest")
    _require(manifest["corpus"], "data_db_bytes", int, "manifest.corpus")
    _require(manifest["corpus"], "data_db_files", int, "manifest.corpus")
    return manifest


def load_run_record(path):
    """Load the single step record a `--ramp 1` run file must contain."""
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
            raise Unmeasured(
                "run-file-not-jsonl", "%s line %d: %s" % (path, number, exc)
            )
        if not isinstance(record, dict):
            raise Unmeasured(
                "run-file-not-jsonl", "%s line %d: not a JSON object" % (path, number)
            )
        records.append(record)
    if len(records) != 1:
        raise Unmeasured(
            "run-record-count",
            "%s: %d step records, expected exactly 1 (the A/B design is "
            "single-stream, --ramp 1)" % (path, len(records)),
        )
    record = records[0]
    if record.get("schema") != SCHEMA_STEP:
        raise Unmeasured(
            "run-record-schema",
            "%s: schema is %r, expected %r"
            % (path, record.get("schema"), SCHEMA_STEP),
        )
    for field, kinds in (
        ("requests_ok", int),
        ("requests_error", int),
        ("requests_unavailable", int),
        ("duration_s", (int, float)),
        ("rows_per_s", (int, float)),
        ("qps", (int, float)),
        ("rows_total", int),
        ("latency_ms", dict),
    ):
        if field not in record:
            raise Unmeasured(
                "run-record-field", "%s: step record is missing %r" % (path, field)
            )
        if not isinstance(record[field], kinds) or isinstance(record[field], bool):
            raise Unmeasured(
                "run-record-field",
                "%s: step record field %r has the wrong type" % (path, field),
            )
    for percentile in ("p50", "p95", "p99", "max"):
        if not isinstance(record["latency_ms"].get(percentile), (int, float)):
            raise Unmeasured(
                "run-record-field",
                "%s: latency_ms.%s missing or non-numeric" % (path, percentile),
            )

    if record["requests_error"] > 0:
        raise Unmeasured(
            "run-errors",
            "%s: requests_error=%d -- a replicate with any request error is not a "
            "throughput measurement" % (path, record["requests_error"]),
        )
    if record["requests_unavailable"] > 0:
        raise Unmeasured(
            "run-shed",
            "%s: requests_unavailable=%d -- admission shedding (#2420) changes what "
            "was measured" % (path, record["requests_unavailable"]),
        )
    if record["requests_ok"] < 1:
        raise Unmeasured("run-degenerate", "%s: requests_ok=0" % path)
    if not record["duration_s"] > 0:
        raise Unmeasured("run-degenerate", "%s: duration_s is not positive" % path)
    if not record["rows_per_s"] > 0:
        raise Unmeasured(
            "run-degenerate",
            "%s: rows_per_s is not positive -- the scan returned no rows, which on "
            "a corpus this size means the ticket template does not name a table that "
            "is present" % path,
        )
    return record


def collect_pairs(manifest, manifest_dir):
    """Resolve the manifest's declared runs into replicate-indexed pairs."""
    seen = {}
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
        seen[key] = load_run_record(path)

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

    pairs = [(rep, seen[("base", rep)], seen[("head", rep)]) for rep in base_reps]
    return pairs


# --------------------------------------------------------------------------
# Statistics. Deterministic by construction: one seeded generator per estimator,
# nearest-rank percentiles, no wall clock anywhere.
# --------------------------------------------------------------------------

def geometric_mean(values):
    return math.exp(sum(math.log(v) for v in values) / len(values))


def arithmetic_mean(values):
    return sum(values) / len(values)


def median(values):
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _nearest_rank(sorted_values, quantile):
    n = len(sorted_values)
    index = int(math.ceil(quantile * n)) - 1
    return sorted_values[min(max(index, 0), n - 1)]


def bootstrap_ci(values, statistic, seed, resamples, level):
    """Percentile bootstrap over `values`, resampled with replacement."""
    rng = random.Random(seed)
    n = len(values)
    draws = []
    for _ in range(resamples):
        sample = [values[rng.randrange(n)] for _ in range(n)]
        draws.append(statistic(sample))
    draws.sort()
    alpha = (1.0 - level) / 2.0
    return _nearest_rank(draws, alpha), _nearest_rank(draws, 1.0 - alpha)


# --------------------------------------------------------------------------
# The verdict. Pure, so the self-test can drive it through real input.
# --------------------------------------------------------------------------

def decide(ci_low, ci_high, band_low, band_high):
    """Map a ratio interval onto the closed verdict token set. See module docs."""
    if band_low <= ci_low and ci_high <= band_high:
        return "MEETS-TARGET"
    if ci_high < band_low:
        return "BELOW-TARGET"
    if ci_low > band_high:
        return "ABOVE-TARGET"
    return "INCONCLUSIVE"


VERDICT_DETAIL = {
    "MEETS-TARGET": (
        "the whole {level} interval of the per-pair throughput ratio lies inside "
        "the target band [{lo}, {hi}]"
    ),
    "BELOW-TARGET": (
        "the whole {level} interval lies BELOW the target band [{lo}, {hi}]: the "
        "target is ruled out by the data. This is a measured result, not an "
        "absence of one -- note it fires whether or not the interval straddles "
        "1.0, because a tight interval around 1.0 is a measured no-effect"
    ),
    "ABOVE-TARGET": (
        "the whole {level} interval lies ABOVE the target band [{lo}, {hi}]. The "
        "band, not the ceiling, is what was tested"
    ),
    "INCONCLUSIVE": (
        "the {level} interval overlaps the target band [{lo}, {hi}] without being "
        "contained in it, so the data cannot distinguish meeting the target from "
        "not meeting it. This is reported as a non-result and must not be "
        "rounded into a number-shaped verdict, and no regression may be filed "
        "from it"
    ),
}


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------

def fmt(value, digits=4):
    if value is None:
        return "NOT-RECORDED"
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return "NON-FINITE"
    return "%.*f" % (digits, value)


def field(manifest, *path):
    node = manifest
    for key in path:
        if not isinstance(node, dict) or key not in node:
            return "NOT-RECORDED"
        node = node[key]
    if node is None:
        return "NOT-RECORDED"
    if isinstance(node, bool):
        return "yes" if node else "no"
    return str(node)


def report(opts, manifest, pairs):
    band_low, band_high = TARGET_BANDS[opts["profile"]]
    level_text = "%.0f%%" % (opts["ci_level"] * 100.0)

    base_rates = [base["rows_per_s"] for _, base, _ in pairs]
    head_rates = [head["rows_per_s"] for _, _, head in pairs]
    ratios = [head["rows_per_s"] / base["rows_per_s"] for _, base, head in pairs]

    # One seed per estimator, derived from the recorded seed, so the three
    # intervals do not share a draw sequence and each is reproducible alone.
    ratio_lo, ratio_hi = bootstrap_ci(
        ratios, geometric_mean, opts["seed"], opts["resamples"], opts["ci_level"]
    )
    base_lo, base_hi = bootstrap_ci(
        base_rates, arithmetic_mean, opts["seed"] + 1, opts["resamples"], opts["ci_level"]
    )
    head_lo, head_hi = bootstrap_ci(
        head_rates, arithmetic_mean, opts["seed"] + 2, opts["resamples"], opts["ci_level"]
    )

    verdict = decide(ratio_lo, ratio_hi, band_low, band_high)

    out("=== issue #3649 -- served-path A/B throughput, #2820 batched merge fan-in ===")
    out("manifest %s" % opts["manifest"])
    out("manifest-generated-utc %s" % field(manifest, "generated_utc"))
    out("driver-version %s" % field(manifest, "driver_version"))
    out("arm base commit %s" % field(manifest, "arms", "base", "commit"))
    out("arm head commit %s" % field(manifest, "arms", "head", "commit"))
    out(
        "host instance-type %s nproc %s loadavg1 %s kernel %s"
        % (
            field(manifest, "host", "instance_type"),
            field(manifest, "host", "nproc"),
            field(manifest, "host", "loadavg1"),
            field(manifest, "host", "kernel"),
        )
    )
    out("corpus path %s" % field(manifest, "corpus", "path"))
    out(
        "corpus data-db-bytes %s data-db-files %s min-required-bytes %s"
        % (
            field(manifest, "corpus", "data_db_bytes"),
            field(manifest, "corpus", "data_db_files"),
            field(manifest, "corpus", "min_bytes_required"),
        )
    )
    out("corpus rows-declared %s" % field(manifest, "corpus", "rows_declared"))
    out(
        "workload shape %s ramp %s step-duration %s prewarm %s"
        % (
            field(manifest, "workload", "shape"),
            field(manifest, "workload", "ramp"),
            field(manifest, "workload", "step_duration"),
            field(manifest, "workload", "prewarm"),
        )
    )
    out(
        "pinning server-cpus %s client-cpus %s"
        % (
            field(manifest, "workload", "server_cpus"),
            field(manifest, "workload", "client_cpus"),
        )
    )
    out("thermal-state %s" % field(manifest, "workload", "temperature"))
    merge_path = field(manifest, "workload", "merge_path")
    out("merge-path %s" % merge_path)
    control = manifest.get("control")
    control = control if isinstance(control, str) and control else None
    out("control %s" % (control if control else "none"))
    extra_base = field(manifest, "server_extra", "base")
    extra_head = field(manifest, "server_extra", "head")
    out("arm base server-extra [%s]" % ("" if extra_base == "NOT-RECORDED" else extra_base))
    out("arm head server-extra [%s]" % ("" if extra_head == "NOT-RECORDED" else extra_head))
    asymmetric = extra_base != extra_head
    out(
        "replicates requested %d paired %d order interleaved-base-head"
        % (manifest["replicates_requested"], len(pairs))
    )
    out("statistic geometric-mean-of-per-pair-ratios metric rows_per_s")
    out(
        "bootstrap method percentile resamples %d seed %d ci-level %s"
        % (opts["resamples"], opts["seed"], fmt(opts["ci_level"], 3))
    )

    for replicate, base, head in pairs:
        out(
            "pair %d base-rows-per-s %s head-rows-per-s %s ratio %s"
            % (
                replicate,
                fmt(base["rows_per_s"], 2),
                fmt(head["rows_per_s"], 2),
                fmt(head["rows_per_s"] / base["rows_per_s"]),
            )
        )
    for replicate, base, head in pairs:
        out(
            "pair %d base-requests-ok %d base-duration-s %s head-requests-ok %d "
            "head-duration-s %s"
            % (
                replicate,
                base["requests_ok"],
                fmt(base["duration_s"], 2),
                head["requests_ok"],
                fmt(head["duration_s"], 2),
            )
        )

    for name, rates, lo, hi in (
        ("base", base_rates, base_lo, base_hi),
        ("head", head_rates, head_lo, head_hi),
    ):
        out(
            "arm %s rows-per-s mean %s median %s min %s max %s ci%s [%s, %s]"
            % (
                name,
                fmt(arithmetic_mean(rates), 2),
                fmt(median(rates), 2),
                fmt(min(rates), 2),
                fmt(max(rates), 2),
                level_text,
                fmt(lo, 2),
                fmt(hi, 2),
            )
        )
    for name, index in (("base", 1), ("head", 2)):
        records = [pair[index] for pair in pairs]
        for percentile in ("p50", "p95", "p99", "max"):
            values = [rec["latency_ms"][percentile] for rec in records]
            out(
                "arm %s latency-ms %s median-across-replicates %s min %s max %s"
                % (name, percentile, fmt(median(values), 3), fmt(min(values), 3), fmt(max(values), 3))
            )

    out(
        "ratio point %s median-of-pairs %s ci%s [%s, %s]"
        % (
            fmt(geometric_mean(ratios)),
            fmt(median(ratios)),
            level_text,
            fmt(ratio_lo),
            fmt(ratio_hi),
        )
    )
    out(
        "target profile %s band [%s, %s] source docs/research/phase2-verify-row-engine.md-section-3.2"
        % (opts["profile"], fmt(band_low, 2), fmt(band_high, 2))
    )
    out("ceiling %s" % CEILING_TEXT)
    out("test ci-contains-1.0 %s" % ("yes" if ratio_lo <= 1.0 <= ratio_hi else "no"))
    out(
        "test ci-within-target-band %s"
        % ("yes" if band_low <= ratio_lo and ratio_hi <= band_high else "no")
    )
    out("test ci-entirely-below-band %s" % ("yes" if ratio_hi < band_low else "no"))
    out("test ci-entirely-above-band %s" % ("yes" if ratio_lo > band_high else "no"))
    out(
        "test ci-overlaps-band %s"
        % ("yes" if ratio_hi >= band_low and ratio_lo <= band_high else "no")
    )

    out("verdict %s" % verdict)
    out(
        "verdict-detail %s"
        % VERDICT_DETAIL[verdict].format(
            level=level_text, lo=fmt(band_low, 2), hi=fmt(band_high, 2)
        )
    )
    if merge_path != "merge":
        out(
            "verdict-detail MERGE-PATH CQLITE_FLIGHT_MERGE_PATH was %r, not "
            "'merge', so the #3058 single-source fast path may have served some or "
            "all requests -- that path is not the one #2820 changed" % merge_path
        )
    if control:
        out(
            "verdict-detail CONTROL this session is labelled %r, so its verdict "
            "describes the control and does NOT discharge the #3649 acceptance "
            "criteria" % control
        )
    if asymmetric:
        out(
            "verdict-detail CONTROL the two arms were served under DIFFERENT "
            "server flags, so the difference measured is the injected one and not "
            "the commit pair's"
        )
    for line in non_exhaustive_lines(len(pairs)):
        out("verdict-detail NON-EXHAUSTIVE %s" % line)
    return verdict


def non_exhaustive_lines(n_pairs):
    return [
        "this compares two commits on ONE host, ONE corpus, ONE workload shape and "
        "ONE concurrency; nothing here generalises to another shape or to a wide "
        "row profile",
        "flight-loadgen reports throughput as a SINGLE point estimate per step, so "
        "all dispersion here is BETWEEN-replicate; within-step variance is not "
        "observable from its JSONL and is not modelled",
        "the interval is a percentile bootstrap over %d pairs; with a pair count "
        "this small the interval is itself imprecise, and a wider interval is the "
        "honest reading, never a tighter one" % n_pairs,
        "a difference measured here is a difference between two commits, not "
        "evidence about the mechanism; the mechanism oracle is "
        "cqlite-core/tests/issue_2820_merge_fanin_batch.rs and it is a separate "
        "check",
        "no attribution is performed: this script does not decompose the delta "
        "into send-count, syscall or cache terms",
    ]


def main(argv):
    opts = parse_args(argv)
    try:
        manifest = load_manifest(opts["manifest"])
        manifest_dir = os.path.dirname(os.path.abspath(opts["manifest"]))
        pairs = collect_pairs(manifest, manifest_dir)
        merge_path = manifest.get("workload", {})
        merge_path = merge_path.get("merge_path") if isinstance(merge_path, dict) else None
        files = manifest["corpus"]["data_db_files"]
        if merge_path != "merge" and files < 2:
            raise Unmeasured(
                "merge-path-bypassed",
                "the corpus holds %d *-Data.db file(s) and CQLITE_FLIGHT_MERGE_PATH "
                "was %r rather than 'merge': issue #3058 routes a single-source "
                "request onto a fast path that never enters the k-way merge, so "
                "BOTH arms ran code #2820 did not touch and the ratio is 1.0 by "
                "construction" % (files, merge_path),
            )
        if len(pairs) < manifest["replicates_requested"]:
            raise Unmeasured(
                "replicate-shortfall",
                "%d pairs against %d requested -- the driver did not complete the "
                "requested replicate count, and a short session is not silently "
                "analysed as if it were the requested one"
                % (len(pairs), manifest["replicates_requested"]),
            )
        if len(pairs) < opts["min_pairs"]:
            raise Unmeasured(
                "insufficient-pairs",
                "%d pairs, below the --min-pairs floor of %d: a bootstrap over "
                "fewer pairs than that reports an interval it cannot support"
                % (len(pairs), opts["min_pairs"]),
            )
    except Unmeasured as exc:
        err("cause %s" % exc.cause)
        err("cause-detail %s" % exc.detail)
        out("verdict UNMEASURED")
        out("verdict-detail no throughput verdict was rendered; cause %s" % exc.cause)
        out(
            "verdict-detail a consumer must treat UNMEASURED as no result, never as "
            "a permissive default"
        )
        return VERDICT_EXIT["UNMEASURED"]

    verdict = report(opts, manifest, pairs)
    return VERDICT_EXIT[verdict]


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
