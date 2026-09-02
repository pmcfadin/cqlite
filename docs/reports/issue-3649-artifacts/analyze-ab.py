#!/usr/bin/env python3
"""
analyze-ab.py -- the paired A/B statistics and verdicts for issue #3649.

WHAT QUESTION THIS ANSWERS
--------------------------
#3649 asks for the served-path throughput effect of #2820 (the batched k-way
merge egress fan-in, commit cfa93fe99) measured on the field i4i narrow rig,
"with dispersion, not just a point estimate". This script consumes the replicate
JSONL that `ab-throughput.sh` produces and renders a verdict from a closed token
set.

TWO QUANTITIES, TWO SECTIONS, TWO VERDICT LINES
-----------------------------------------------
The acceptance criteria carry two different measurements and the sources
verdict them differently, so this script reports them SEPARATELY and never
merges them:

  --single-stream <manifest>   a `--ramp 1` session, verdicted against the
                               ~1.1-1.25x narrow / ~1.05-1.1x wide TARGET BAND
  --utilization <manifest>     a concurrency-ramp session, reported as a
                               DIRECTION with an interval -- "rises measurably"
                               -- and never as attainment of the 1.5-1.9x
                               rig-narrow utilization CEILING

At least one is required; both may be given, and each is analysed
independently, so one unusable session never suppresses the other. The rules
themselves, with their citations, live in `ab_stats.py`; the loading and every
named refusal live in `ab_input.py`; the anchored emission lives in
`ab_common.py`.

WHY REPLICATES, AND WHY PAIRED
------------------------------
`flight-loadgen` emits per ramp step a latency HISTOGRAM but a SINGLE POINT
ESTIMATE of throughput (`rows_per_s` = count / duration_s -- see
tools/flight-loadgen/src/record.rs). There is no within-step dispersion to
recover and the tool computes no interval, so dispersion has to come from
REPEATED RUNS, INTERLEAVED (base, head, base, head, ...) so host drift over the
session is shared by both arms rather than aliased onto whichever ran second.
Replicate i of base pairs with replicate i of head and the statistic is over the
per-pair RATIO -- which is what interleaving buys.

THE OUTPUT CANNOT BE PASTED AS A CERTIFICATION
-----------------------------------------------
Every line, stdout and stderr, begins with `AB-3649: `; every dynamic field is
control-character sanitized; a verdict appears ONLY on an
`AB-3649: verdict <quantity> <TOKEN>` line; prose goes on `verdict-detail`
lines; and the static text of every module carries none of the reserved
gate/review marker strings, asserted structurally by `selftest-analyze.sh`.
DECLARED RESIDUAL: manifest fields are operator-controlled and printed verbatim,
so one CAN contain a reserved substring -- the anchor is what makes that
harmless.

EXIT CODES, AND THE CONSUMER CONTRACT
-------------------------------------
Per section: 0 MEETS-TARGET / RISES; 4 BELOW-TARGET / FALLS; 5 ABOVE-TARGET;
6 INCONCLUSIVE; 7 UNMEASURED. With both sections present the process exit is the
LARGEST of the two, so the least affirmative outcome governs. 3 is a usage error
and is also what --help exits with, deliberately: exit 0 has a meaning here, so
a run that measured nothing must never produce it. Exit 0 is not an assertion of
quality; it is a token and nothing more. A consumer must treat 7 / UNMEASURED as
no result, never as a permissive default.
"""

import os
import sys

from ab_common import (
    MIN_CORPUS_BYTES_FLOOR,
    MIN_SSTABLES_FLOOR,
    Unmeasured,
    err,
    out,
)
from ab_input import (
    MODE_SINGLE_STREAM,
    MODE_UTILIZATION,
    collect_pairs,
    load_manifest,
)
import ab_stats as S

# Named on every run of both sections, tested against never.
CEILING_TEXT = (
    "1.5-1.9x is a rig-narrow UTILIZATION ceiling recorded as unmeasured in "
    "docs/research/phase2-verify-row-engine.md line 115; the plan of record asks "
    "only that utilization throughput rise measurably TOWARD it "
    "(docs/architecture/throughput-program-2026-07.md line 371). It is NAMED here "
    "and is NOT a target, NOT a threshold, and NOT tested against in either section"
)

SECTION_EXIT = {
    "MEETS-TARGET": 0,
    "RISES": 0,
    "BELOW-TARGET": 4,
    "FALLS": 4,
    "ABOVE-TARGET": 5,
    "INCONCLUSIVE": 6,
    "UNMEASURED": 7,
}
EXIT_USAGE = 3

HELP_LINES = [
    "analyze-ab.py [--single-stream <manifest>] [--utilization <manifest>] [options]",
    "",
    "  --single-stream <path>  ab-3649.manifest/v1 from a --ramp 1 session;",
    "                          verdicted against the target band",
    "  --utilization <path>    ab-3649.manifest/v1 from a concurrency-ramp session;",
    "                          reported as a direction, never against the ceiling",
    "  (at least one of the two is required)",
    "",
    "  --profile <name>     target profile for the single-stream section:",
    "                       narrow | wide                            (default narrow)",
    "  --seed <int>         bootstrap seed, recorded in the report   (default %d)" % S.DEFAULT_SEED,
    "  --resamples <int>    bootstrap resamples                      (default %d)" % S.DEFAULT_RESAMPLES,
    "  --ci-level <float>   two-sided interval level, 0 < L < 1      (default %.2f)" % S.DEFAULT_CI_LEVEL,
    "  --min-pairs <int>    refuse to render below this many pairs   (default %d)" % S.DEFAULT_MIN_PAIRS,
    "  -h, --help           print this and exit %d" % EXIT_USAGE,
    "",
    "exit (per section): 0 MEETS-TARGET|RISES  4 BELOW-TARGET|FALLS",
    "                    5 ABOVE-TARGET  6 INCONCLUSIVE  7 UNMEASURED",
    "with both sections the process exit is the LARGEST of the two; 3 is usage",
]


def usage_error(detail):
    err("usage-error %s" % detail)
    for line in HELP_LINES:
        err(line)
    sys.exit(EXIT_USAGE)


def parse_args(argv):
    opts = {
        "single_stream": None,
        "utilization": None,
        "profile": "narrow",
        "seed": S.DEFAULT_SEED,
        "resamples": S.DEFAULT_RESAMPLES,
        "ci_level": S.DEFAULT_CI_LEVEL,
        "min_pairs": S.DEFAULT_MIN_PAIRS,
    }
    takes_value = {
        "--single-stream": ("single_stream", str),
        "--utilization": ("utilization", str),
        "--profile": ("profile", str),
        "--seed": ("seed", int),
        "--resamples": ("resamples", int),
        "--ci-level": ("ci_level", float),
        "--min-pairs": ("min_pairs", int),
    }
    index = 0
    while index < len(argv):
        arg = argv[index]
        if arg in ("-h", "--help"):
            for line in HELP_LINES:
                out(line)
            sys.exit(EXIT_USAGE)
        if arg not in takes_value:
            usage_error("unrecognised argument: %s" % arg)
        if index + 1 >= len(argv):
            usage_error("%s requires a value" % arg)
        key, caster = takes_value[arg]
        try:
            opts[key] = caster(argv[index + 1])
        except ValueError:
            usage_error("%s: not a valid value: %s" % (arg, argv[index + 1]))
        index += 2

    if opts["single_stream"] is None and opts["utilization"] is None:
        usage_error("at least one of --single-stream or --utilization is required")
    if opts["profile"] not in S.TARGET_BANDS:
        usage_error("--profile must be one of: %s" % ", ".join(sorted(S.TARGET_BANDS)))
    if opts["resamples"] < 100:
        usage_error("--resamples must be at least 100")
    if not 0.0 < opts["ci_level"] < 1.0:
        usage_error("--ci-level must be strictly between 0 and 1")
    if opts["min_pairs"] < 2:
        usage_error("--min-pairs must be at least 2")
    return opts


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------

def fmt(value, digits=4):
    if value is None:
        return "NOT-RECORDED"
    if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
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


def non_exhaustive_lines(mode, n_pairs):
    lines = [
        "this compares two commits on ONE host, ONE corpus, ONE workload shape and "
        "ONE admission setting; nothing here generalises to another shape, another "
        "row width, or another concurrency regime",
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
    if mode == MODE_UTILIZATION:
        lines.append(
            "the utilization quantity is the PEAK rows_per_s over the surviving "
            "concurrency ladder, not the area under the curve and not a fitted "
            "saturation point; a ladder that does not reach saturation reports the "
            "peak it did reach and says nothing about where saturation is"
        )
    return lines


def render_common(manifest, mode, admission, session):
    out("mode %s" % mode)
    out("manifest-generated-utc %s" % field(manifest, "generated_utc"))
    out("driver-version %s" % field(manifest, "driver_version"))
    out("arm base commit %s" % field(manifest, "arms", "base", "commit"))
    out("arm head commit %s" % field(manifest, "arms", "head", "commit"))
    out(
        "loadgen commit %s ref %s (ONE client, both arms)"
        % (field(manifest, "loadgen", "commit"), field(manifest, "loadgen", "ref"))
    )
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
    # The census describes the ONE directory the ticket is served from, not the
    # data root -- so the size floor and the >=2-SSTable #3058 guard are claims
    # about the table under measurement.
    out("corpus served-dir %s" % field(manifest, "corpus", "served_dir"))
    out("corpus compressed %s" % field(manifest, "corpus", "compressed"))
    # THE NUMBER CARRIES WHAT IT WAS MEASURED ON. An LZ4-derived band and the
    # algorithm actually decoded are the same fact stated twice, and a report
    # that omits the second invites the comparison it cannot support.
    out(
        "corpus compression %s (%s)"
        % (
            field(manifest, "corpus", "compression"),
            field(manifest, "corpus", "compression_detail"),
        )
    )
    # THE PROPERTIES THE `i4i` LABEL STOOD FOR, reported as themselves. A rig
    # class is not reliably derivable from a hostname, but the two things the
    # class was chosen FOR are measurable, and each is reported separately: a
    # NOT-MEASURABLE storage probe and a verified local disk are different facts.
    out(
        "corpus storage %s (%s)"
        % (
            field(manifest, "corpus", "storage"),
            field(manifest, "corpus", "storage_detail"),
        )
    )
    out(
        "host contention %s loadavg1 %s limit %s"
        % (
            field(manifest, "host", "contention"),
            field(manifest, "host", "loadavg1"),
            field(manifest, "host", "load_limit"),
        )
    )
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
        "server batch-size %s max-batch-bytes %s step-duration-seconds %s"
        % (
            field(manifest, "workload", "batch_size"),
            field(manifest, "workload", "max_batch_bytes"),
            field(manifest, "workload", "step_duration_seconds"),
        )
    )
    out(
        "admission max-concurrent-scans requested %s observed %s corroboration %s "
        "(%d of %d runs) wait-timeout-ms %s"
        % (
            field(manifest, "workload", "max_concurrent_scans"),
            admission.value,
            admission.state,
            admission.observed,
            admission.total,
            field(manifest, "workload", "admission_wait_timeout_ms"),
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
    # Every readback renders through the ONE type, with its own counts, so a
    # field that was observed for only some runs cannot read as agreement.
    for name in sorted(session["readbacks"]):
        corroboration = session["readbacks"][name]
        out(
            "server-observed %s value %s corroboration %s (%d of %d runs)"
            % (
                name.replace("_observed", "").replace("_", "-"),
                corroboration.value,
                corroboration.state,
                corroboration.observed,
                corroboration.total,
            )
        )
    out("merge-path %s" % field(manifest, "workload", "merge_path"))


def analyze(mode, path, opts):
    """Emit one section and return its verdict token."""
    quantity = mode
    out("==== section %s ====" % quantity)
    manifest, declared_steps = load_manifest(path, mode)
    manifest_dir = os.path.dirname(os.path.abspath(path))

    merge_path = manifest.get("workload", {})
    merge_path = merge_path.get("merge_path") if isinstance(merge_path, dict) else None
    files = manifest["corpus"]["data_db_files"]
    if merge_path != "merge" and files < 2:
        raise Unmeasured(
            "merge-path-bypassed",
            "the corpus holds %d *-Data.db file(s) and CQLITE_FLIGHT_MERGE_PATH was "
            "%r rather than 'merge': issue #3058 routes a single-source request onto "
            "a fast path that never enters the k-way merge, so BOTH arms ran code "
            "#2820 did not touch and the ratio is 1.0 by construction"
            % (files, merge_path),
        )

    # THE TARGET BAND IS DEFINED FOR `--shape full` OVER THE WHOLE RING (the AC's
    # first line). A point, limit-k, filtered or aggregating session scored
    # against it is a wrong answer wearing a right-looking shape. A CONTROL may
    # use any shape -- its verdict is already disclaimed -- but an unlabelled
    # session may not.
    shape = manifest.get("workload", {})
    shape = shape.get("shape") if isinstance(shape, dict) else None
    control = manifest.get("control")
    control = control if isinstance(control, str) and control else None
    if shape != "full" and not control:
        raise Unmeasured(
            "shape-not-full",
            "the session ran --shape %r, but the #3649 target band and the "
            "utilization direction are both defined for --shape full over the "
            "whole ring; a narrowed workload scored against them would be a "
            "verdict about a different quantity. Re-run with --shape full, or "
            "label the session --control <label> to have its verdict disclaimed"
            % shape,
        )

    # THE FLOORS, ENFORCED INDEPENDENTLY OF WHAT THE SESSION RECORDED. The
    # manifest carries the thresholds the session chose; this checks the
    # DOCUMENTED minimums instead. A verdict must not derive its validity from a
    # number its own subject picked -- and #3058's bypass has now been reachable
    # three ways, the third being an operator simply passing `--min-sstables 1`.
    # Same reason the shape is re-checked here rather than trusting that this
    # driver produced the manifest.
    if not control:
        corpus = manifest["corpus"]
        if corpus["data_db_bytes"] < MIN_CORPUS_BYTES_FLOOR:
            raise Unmeasured(
                "corpus-below-floor",
                "the served corpus holds %d Data.db bytes, below the documented "
                "floor of %d. The manifest's own min_bytes_required is deliberately "
                "not consulted: a measurement cannot authorise itself by recording "
                "a smaller threshold"
                % (corpus["data_db_bytes"], MIN_CORPUS_BYTES_FLOOR),
            )
        if corpus["data_db_files"] < MIN_SSTABLES_FLOOR:
            raise Unmeasured(
                "corpus-below-floor",
                "the served corpus holds %d *-Data.db file(s), below the documented "
                "floor of %d. Below it a single-source served table takes #3058's "
                "fast path on BOTH arms, neither executes the code #2820 changed, "
                "and the ratio is 1.0 by construction"
                % (corpus["data_db_files"], MIN_SSTABLES_FLOOR),
            )

    # THE COMPRESSED-CORPUS REQUIREMENT, RE-CHECKED HERE. Documented in
    # FINDINGS.md and enforced by nothing until round 12 -- and the failure is in
    # the FAVOURABLE direction, because removing LZ4 decode removes real CPU from
    # the denominator and inflates the ratio. Re-checked rather than trusted for
    # the usual reason: a manifest is data, and this analyzer does not get to
    # assume which driver produced it.
    # THE ALGORITHM, RE-CHECKED. `compressed: true` says metadata exists; the
    # requirement is that it is LZ4, because the band was derived against LZ4
    # decode work. A manifest silent about the algorithm refuses rather than
    # inheriting the permissive branch -- "did not ask" and "asked and it was
    # LZ4" are different facts, the sentinel rule this lane keeps re-learning.
    if not control and manifest["corpus"].get("compression") != "LZ4":
        raise Unmeasured(
            "corpus-compression-not-lz4",
            "the manifest records the served corpus compression as %r (%s). The "
            "field is LZ4 and the target band was derived against LZ4 decode "
            "work, so a ratio measured against different -- or unverified -- "
            "decompression is not comparable to it. Label the session --control "
            "if that is what you meant to measure"
            % (manifest["corpus"].get("compression", "NOT-RECORDED"),
               manifest["corpus"].get("compression_detail", "no detail recorded")),
        )
    if not control and manifest["corpus"].get("compressed") is not True:
        raise Unmeasured(
            "corpus-uncompressed",
            "the manifest does not record the served corpus as compressed. The "
            "field is LZ4, and an uncompressed corpus biases the measured ratio "
            "TOWARD the target -- so this is not a conservative failure. Label the "
            "session --control if an uncompressed corpus is what you meant to "
            "measure",
        )

    # THE RIG PROPERTIES, RE-CHECKED HERE FOR THE SAME REASON AS COMPRESSION: a
    # manifest is data, and this analyzer does not get to assume which driver
    # produced it.
    #
    # A MEASUREMENT VERDICT REQUIRES `LOCAL`, NOT MERELY "NOT NETWORK". The
    # previous form refused only the affirmative bad value, on the reasoning that
    # a probe which could not run is a gap and should be disclosed rather than
    # refused. That reasoning is right about DISCLOSURE and wrong about a
    # VERDICT: the acceptance criteria REQUIRE local NVMe, and "we could not
    # tell" does not satisfy a requirement. A four-state classifier is only as
    # good as the four-way disposition downstream of it -- so the SAN LUN that
    # the classifier was fixed to stop calling LOCAL was still being handed a
    # verdict, one layer down, because only `NETWORK` was refused here.
    storage_state = manifest["corpus"].get("storage")
    attestation = manifest["corpus"].get("storage_attestation")
    if not control and storage_state == "NETWORK":
        raise Unmeasured(
            "corpus-network-storage",
            "the manifest records the served corpus on network storage (%s). The "
            "#3649 rig is a field i4i box for the property that its corpus is on "
            "LOCAL NVMe; a network hop inside the read path is variable latency "
            "added to the very quantity being measured. Label the session "
            "--control if that is what you meant to measure"
            % manifest["corpus"].get("storage_detail", "no detail recorded"),
        )
    # AN ATTESTATION COVERS IGNORANCE, NEVER EVIDENCE -- so it is consulted only
    # after the NETWORK refusal above, and can never reach it. An operator may
    # assert that an unrecognised device is local; nobody may assert that an
    # identified network device is not, or the one thing this check exists to
    # refuse becomes the one thing a flag turns off.
    if not control and storage_state != "LOCAL" and not attestation:
        raise Unmeasured(
            "corpus-storage-unverified",
            "the manifest records the corpus storage as %s (%s), so it is NOT "
            "known to be local -- and the acceptance criteria require local NVMe. "
            "A probe that could not tell is a gap in the record, and a gap does "
            "not satisfy a requirement. Re-run with the corpus on a device whose "
            "model is recognised, or pass --attest-local-storage <why> to record "
            "an operator attestation that travels with the verdict, or label the "
            "session --control"
            % (storage_state, manifest["corpus"].get("storage_detail",
                                                     "no detail recorded")),
        )
    pairs, admission, session = collect_pairs_checked(
        manifest, manifest_dir, mode, declared_steps
    )
    if len(pairs) < manifest["replicates_requested"]:
        raise Unmeasured(
            "replicate-shortfall",
            "%d pairs against %d requested -- the driver did not complete the "
            "requested replicate count, and a short session is not silently analysed "
            "as if it were the requested one"
            % (len(pairs), manifest["replicates_requested"]),
        )
    if len(pairs) < opts["min_pairs"]:
        raise Unmeasured(
            "insufficient-pairs",
            "%d pairs, below the --min-pairs floor of %d: a bootstrap over fewer "
            "pairs than that reports an interval it cannot support"
            % (len(pairs), opts["min_pairs"]),
        )

    stats = compute(mode, pairs, opts)
    render_common(manifest, mode, admission, session)
    return report(mode, manifest, pairs, admission, opts, stats, session)


def compute(mode, pairs, opts):
    """Everything statistical, computed and CHECKED before a line is emitted.

    Emission comes after, so a refusal cannot leave a half-rendered section --
    and every value that reaches a `decide_*` rule has been established finite.
    """
    base_rates = [base.rate for _, base, _ in pairs]
    head_rates = [head.rate for _, _, head in pairs]

    ratios = []
    for replicate, base, head in pairs:
        ratio = head.rate / base.rate
        # BOTH operands can be finite and positive and still produce a ratio that
        # is not: a subnormal base (1e-320 passes `> 0`) overflows the quotient to
        # inf, and a subnormal head over a large base underflows it to 0.0, whose
        # `math.log` raises. Neither needs a hand-edited record.
        if not S.is_usable_ratio(ratio):
            raise Unmeasured(
                "ratio-non-finite",
                "replicate %d: head %r / base %r is %r, which is not a finite "
                "positive ratio; no verdict can rest on it"
                % (replicate, head.rate, base.rate, ratio),
            )
        ratios.append(ratio)

    ratio_lo, ratio_hi = S.bootstrap_ci(
        ratios, S.geometric_mean, opts["seed"], opts["resamples"], opts["ci_level"]
    )
    for name, bound in (("lower", ratio_lo), ("upper", ratio_hi)):
        if not S.is_usable_ratio(bound):
            raise Unmeasured(
                "ci-non-finite",
                "the %s interval bound is %r; a verdict rule compares it happily "
                "and the renderer would print NON-FINITE beside a confident token"
                % (name, bound),
            )
    if S.interval_is_degenerate(ratio_lo, ratio_hi, ratios):
        raise Unmeasured(
            "bootstrap-degenerate",
            "the computed interval [%r, %r] is exactly (min, max) of the observed "
            "ratios, so the bootstrap contributed nothing and this is the observed "
            "RANGE reported as a confidence interval. It happens whenever the "
            "all-minimum resample is likelier than the tail (probability 1/n**n, "
            "which at n=3 is 3.7%% against a 2.5%% tail) and whenever every ratio "
            "is identical. Use more replicates -- the floor is %d and 7 is the "
            "recommendation" % (ratio_lo, ratio_hi, S.DEFAULT_MIN_PAIRS),
        )

    base_lo, base_hi = S.bootstrap_ci(
        base_rates, S.arithmetic_mean, opts["seed"] + 1, opts["resamples"], opts["ci_level"]
    )
    head_lo, head_hi = S.bootstrap_ci(
        head_rates, S.arithmetic_mean, opts["seed"] + 2, opts["resamples"], opts["ci_level"]
    )
    return {
        "base_rates": base_rates,
        "head_rates": head_rates,
        "ratios": ratios,
        "ratio_ci": (ratio_lo, ratio_hi),
        "base_ci": (base_lo, base_hi),
        "head_ci": (head_lo, head_hi),
    }


def collect_pairs_checked(manifest, manifest_dir, mode, declared_steps):
    """`collect_pairs`, plus the evidence floor a verdict may not rest below.

    REVISED RULING (round 10). Partial corroboration stays a disclosure: the
    observed subset genuinely constrains the unobserved one, because the pin is
    ONE manifest-level value passed identically to both arms and the driver dies
    on any per-run mismatch it can read. But that argument has a qualifier -- "it
    can read" -- and when NOTHING was read it protects nothing: the batch size,
    the admission provenance and the merge-path pin are all unverified, and a
    decisive verdict behind a disclosure is a verdict resting on absence. Which
    is the single thing this instrument exists not to do.

    Applied to controls too. This is about whether evidence exists, not about
    which band a verdict is scored against.
    """
    pairs, admission, session = collect_pairs(
        manifest, manifest_dir, mode, declared_steps
    )
    # THE RULE IS ABOUT CORROBORATION AS SUCH, NOT ABOUT ADMISSION. It was applied
    # to the field we happened to be discussing, so a session whose admission
    # ceiling parsed but whose BATCH SIZE was never observed still rendered a
    # verdict -- and the batch size is the mechanism under measurement. Every
    # required readback gets it, independently; `partial` stays a disclosure for
    # all of them, because there the observed runs constrain the unobserved.
    unobserved = []
    if admission.state == "none":
        unobserved.append("max_concurrent_scans")
    for name in sorted(session["readbacks"]):
        if session["readbacks"][name].state == "none":
            unobserved.append(name.replace("_observed", ""))
    if unobserved:
        raise Unmeasured(
            "startup-unobserved",
            "these server settings were not observed for ANY of the %d runs: %s. "
            "A disclosure is the right answer to a PARTIAL observation, where the "
            "runs that were read constrain the ones that were not; it is the wrong "
            "answer to no observation at all, which leaves the verdict resting on "
            "absence" % (admission.total, ", ".join(sorted(set(unobserved)))),
        )
    # ONE CLIENT FOR BOTH ARMS, CHECKED RATHER THAN ASSUMED. The driver builds a
    # single load generator, but a manifest is data and this analyzer does not
    # get to assume which driver produced it.
    # SATISFIABLE BY ABSENCE IS NOT SATISFIED. Collecting only the runs that
    # HAVE a commit means one arm omitting it -- or every run omitting it --
    # leaves at most one value in the set and the guard passes with no evidence
    # that both arms used the same client. Same sentinel class as every other
    # instance in this lane: a missing value inheriting the permissive branch.
    missing = [
        "%s-r%02d" % (e.get("arm"), e.get("replicate"))
        for e in manifest["runs"]
        if not e.get("loadgen_commit")
    ]
    if missing:
        raise Unmeasured(
            "loadgen-provenance-absent",
            "%d run(s) record no load-generator commit (%s); the confound this "
            "checks for -- a client that varies with the server commit -- cannot "
            "be ruled out by runs that say nothing about which client drove them"
            % (len(missing), ", ".join(missing[:4]) + ("…" if len(missing) > 4 else "")),
        )
    commits = {}
    for entry in manifest["runs"]:
        commits.setdefault(str(entry["loadgen_commit"]), set()).add(entry.get("arm"))
    declared = manifest.get("loadgen")
    declared = declared.get("commit") if isinstance(declared, dict) else None
    if len(commits) > 1:
        raise Unmeasured(
            "loadgen-provenance-mismatch",
            "the runs were driven by more than one load generator (%s); a client "
            "that varies with the server commit turns a client-side change into "
            "apparent server throughput, and no amount of dispersion reporting "
            "would reveal it"
            % ", ".join("%s used by %s" % (c, ",".join(sorted(a)))
                        for c, a in sorted(commits.items())),
        )
    if not declared or declared not in commits:
        raise Unmeasured(
            "loadgen-provenance-mismatch",
            "the runs name load-generator %s but the manifest declares %r; the "
            "session's own record of which client it built does not match the "
            "client the runs say drove them"
            % (", ".join(sorted(commits)), declared),
        )
    return pairs, admission, session


def report(mode, manifest, pairs, admission, opts, stats, session):
    level_text = "%.0f%%" % (opts["ci_level"] * 100.0)
    control = manifest.get("control")
    control = control if isinstance(control, str) and control else None
    extra_base = field(manifest, "server_extra", "base")
    extra_head = field(manifest, "server_extra", "head")
    asymmetric = extra_base != extra_head
    merge_path = field(manifest, "workload", "merge_path")

    out("control %s" % (control if control else "none"))
    out("arm base server-extra [%s]" % ("" if extra_base == "NOT-RECORDED" else extra_base))
    out("arm head server-extra [%s]" % ("" if extra_head == "NOT-RECORDED" else extra_head))
    out(
        "replicates requested %d paired %d order counterbalanced-by-replicate-parity"
        % (manifest["replicates_requested"], len(pairs))
    )
    # THE EXECUTED ORDER, COUNTED FROM THE RECORD. Interleaving across replicates
    # controls drift between pairs; only alternating the order WITHIN a pair stops
    # a monotonic gradient landing on the same arm every time.
    out(
        "counterbalance base-first %d head-first %d residual %d pair(s)"
        % (
            session["base_first"],
            session["head_first"],
            abs(session["base_first"] - session["head_first"]),
        )
    )
    out(
        "counterbalance order-by-replicate %s"
        % ",".join(
            "%d:%s" % (rep, arm)
            for rep, arm in sorted(session["order_by_replicate"].items())
        )
    )

    base_rates = stats["base_rates"]
    head_rates = stats["head_rates"]
    ratios = stats["ratios"]
    ratio_lo, ratio_hi = stats["ratio_ci"]
    base_lo, base_hi = stats["base_ci"]
    head_lo, head_hi = stats["head_ci"]

    metric = (
        "rows_per_s at concurrency 1"
        if mode == MODE_SINGLE_STREAM
        else "PEAK rows_per_s over the surviving concurrency ladder"
    )
    out("statistic geometric-mean-of-per-pair-ratios metric %s" % metric)
    out(
        "bootstrap method percentile resamples %d seed %d ci-level %s"
        % (opts["resamples"], opts["seed"], fmt(opts["ci_level"], 3))
    )

    if mode == MODE_UTILIZATION:
        for replicate, base, head in pairs:
            out(
                "pair %d ladder-compared %s base-peak-at-concurrency %d "
                "head-peak-at-concurrency %d"
                % (
                    replicate,
                    ",".join(str(c) for c in base.ladder),
                    base.peak_concurrency,
                    head.peak_concurrency,
                )
            )
        shed_total = 0
        for replicate, base, head in pairs:
            for arm_name, point in (("base", base), ("head", head)):
                for concurrency, count in point.shed:
                    shed_total += 1
                    out(
                        "excluded-step replicate %d arm %s concurrency %d "
                        "requests-unavailable %d reason admission-shed-2420"
                        % (replicate, arm_name, concurrency, count)
                    )
        out("excluded-steps %d RECOGNISED" % shed_total)

    for replicate, base, head in pairs:
        out(
            "pair %d base-rows-per-s %s head-rows-per-s %s ratio %s"
            % (replicate, fmt(base.rate, 2), fmt(head.rate, 2), fmt(head.rate / base.rate))
        )
    for name, rates, low, high in (
        ("base", base_rates, base_lo, base_hi),
        ("head", head_rates, head_lo, head_hi),
    ):
        out(
            "arm %s rows-per-s mean %s median %s min %s max %s ci%s [%s, %s]"
            % (
                name,
                fmt(S.arithmetic_mean(rates), 2),
                fmt(S.median(rates), 2),
                fmt(min(rates), 2),
                fmt(max(rates), 2),
                level_text,
                fmt(low, 2),
                fmt(high, 2),
            )
        )
    for name, index in (("base", 1), ("head", 2)):
        points = [pair[index] for pair in pairs]
        for percentile in ("p50", "p95", "p99", "max"):
            values = [
                max(rec["latency_ms"][percentile] for rec in point.records)
                for point in points
            ]
            out(
                "arm %s latency-ms %s worst-step-median-across-replicates %s min %s max %s"
                % (name, percentile, fmt(S.median(values), 3), fmt(min(values), 3), fmt(max(values), 3))
            )

    out(
        "ratio %s point %s median-of-pairs %s ci%s [%s, %s]"
        % (
            mode,
            fmt(S.geometric_mean(ratios)),
            fmt(S.median(ratios)),
            level_text,
            fmt(ratio_lo),
            fmt(ratio_hi),
        )
    )
    out("ceiling %s" % CEILING_TEXT)

    if mode == MODE_SINGLE_STREAM:
        band_low, band_high = S.TARGET_BANDS[opts["profile"]]
        out(
            "target profile %s band [%s, %s] source "
            "docs/research/phase2-verify-row-engine.md-line-107"
            % (opts["profile"], fmt(band_low, 2), fmt(band_high, 2))
        )
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
        verdict = S.decide_single_stream(ratio_lo, ratio_hi, band_low, band_high)
        detail = S.SINGLE_STREAM_DETAIL[verdict].format(
            level=level_text, lo=fmt(band_low, 2), hi=fmt(band_high, 2)
        )
    else:
        out(
            "target NONE-BY-DESIGN the utilization quantity is reported as a "
            "direction; no band and no ceiling is supplied to the rule that decides "
            "it, so no attainment claim is expressible"
        )
        out("test ci-contains-1.0 %s" % ("yes" if ratio_lo <= 1.0 <= ratio_hi else "no"))
        out("test ci-entirely-above-1.0 %s" % ("yes" if ratio_lo > 1.0 else "no"))
        out("test ci-entirely-below-1.0 %s" % ("yes" if ratio_hi < 1.0 else "no"))
        verdict = S.decide_utilization(ratio_lo, ratio_hi)
        detail = S.UTILIZATION_DETAIL[verdict].format(level=level_text)

    out("verdict %s %s" % (mode, verdict))
    out("verdict-detail %s %s" % (mode, detail))
    if merge_path != "merge":
        out(
            "verdict-detail %s MERGE-PATH CQLITE_FLIGHT_MERGE_PATH was %r, not "
            "'merge', so the #3058 single-source fast path may have served some or "
            "all requests -- that path is not the one #2820 changed" % (mode, merge_path)
        )
    for name in sorted(session["readbacks"]):
        corroboration = session["readbacks"][name]
        if not corroboration.corroborated:
            out(
                "verdict-detail %s READBACK %s was observed for %d of %d runs; the "
                "observed values agree, but PARTIAL OBSERVATION IS NOT AGREEMENT -- "
                "the unobserved runs corroborate nothing. Same remedy as the "
                "admission ceiling, and the same window: only while the rig is live"
                % (
                    mode,
                    name.replace("_observed", "").replace("_", "-"),
                    corroboration.observed,
                    corroboration.total,
                )
            )
    if not admission.corroborated:
        if admission.state == "none":
            out(
                "verdict-detail %s ADMISSION the servers' resolved "
                "--max-concurrent-scans was NOT OBSERVED from any startup line, so "
                "the requested value is recorded but corroborated by nothing" % mode
            )
        else:
            out(
                "verdict-detail %s ADMISSION the resolved --max-concurrent-scans "
                "was observed for only %d of %d runs; the observed values agree, "
                "but PARTIAL OBSERVATION IS NOT AGREEMENT -- the unobserved runs "
                "corroborate nothing"
                % (mode, admission.observed, admission.total)
            )
        # THE REMEDY TRAVELS WITH THE DIAGNOSTIC. This block gets pasted into an
        # issue and read weeks later by somebody who never had RUNBOOK.md open,
        # possibly after the rig is gone -- and "was this fixable at the time?" is
        # the first question a reviewer of a marginal result asks. Naming the
        # state without naming the fix is the shape `missing-fixtures`,
        # `missing-schemas` and the component-set verdict all exist to correct.
        #
        # AND THE REMEDY DIFFERS BY STATE, which is the other half of that
        # precedent: the gate-pin verdict splits `NOT-HONOURED` from `default`
        # precisely because a shared remedy sends an operator in a circle. Here
        # the first action genuinely differs -- with SOME lines parsed the format
        # is fine and the fault is specific to the runs that did not report;
        # with NONE parsed, no individual run is the subject and the parse or the
        # log format itself is. One line per state, and the fuller guidance stays
        # in RUNBOOK.md steps 5 and 6.
        if admission.state == "none":
            remedy = (
                "NO startup line parsed anywhere, so the subject is the parse or "
                "the server log format itself, not any one run: check that "
                "<work-dir>/logs/<arm>-r<NN>.server.log holds a `cqlite-flight "
                "starting` line at all, and run `ab_driver_support.py parse-startup` "
                "against it"
            )
        else:
            remedy = (
                "some startup lines parsed and some did not, so the format is "
                "fine and the fault is specific to the runs that did not report: "
                "read those runs' <work-dir>/logs/<arm>-r<NN>.server.log and "
                "compare with one that did"
            )
        out(
            "verdict-detail %s ADMISSION-REMEDY fixable ONLY while the rig is "
            "live -- %s, then re-run the affected pass; the server logs are lost "
            "with the instance. This is LESS CORROBORATION, not evidence the arms "
            "disagreed: the driver dies affirmatively on any per-run observed != "
            "requested it can read" % (mode, remedy)
        )
    if control:
        out(
            "verdict-detail %s CONTROL this session is labelled %r, so its verdict "
            "describes the control and does NOT discharge the #3649 acceptance "
            "criteria" % (mode, control)
        )
    if field(manifest, "workload", "shape") != "full":
        out(
            "verdict-detail %s SHAPE the workload was --shape %s, not 'full'; the "
            "target band and the utilization direction are defined for a full-ring "
            "scan, so this verdict is not about the quantity #3649 asks for"
            % (mode, field(manifest, "workload", "shape"))
        )
    if asymmetric:
        out(
            "verdict-detail %s CONTROL the two arms were served under DIFFERENT "
            "server flags, so the difference measured is the injected one and not "
            "the commit pair's" % mode
        )
    if session["base_first"] != session["head_first"]:
        out(
            "verdict-detail %s COUNTERBALANCE %d pair(s) ran base-first and %d "
            "head-first: an odd replicate count cannot balance exactly, so one "
            "within-pair ordering is represented once more than the other. Any "
            "drift inside a pair is therefore cancelled to within one pair, not "
            "exactly -- run an EVEN replicate count to remove this residual"
            % (mode, session["base_first"], session["head_first"])
        )
    # THE SWEEP (round 12): requirements the runbook states that no check can
    # honestly REFUSE, disclosed here rather than left unstated. Refusing on a
    # host name would red a correct rig the day someone uses `i4i.2xlarge`, and a
    # guard that reds on correct input is the guard people learn to waive.
    host_type = field(manifest, "host", "instance_type")
    if not host_type.startswith("i4i"):
        out(
            "verdict-detail %s HOST the acceptance criteria name the field i4i "
            "narrow rig, and this session ran on %r. That cannot be refused from "
            "inside -- a rig class is not reliably derivable from a host string -- "
            "so it is DISCLOSED: a verdict measured off the named rig is not the "
            "verdict the criteria ask for" % (mode, host_type)
        )
    # CONTENTION, from the token the driver RECORDED rather than re-derived here.
    # The previous form was `float(loadavg) > 2.0` under `except ValueError:
    # busy = False`, which quietly read NOT-RECORDED as a quiet box -- a third
    # value crammed into a two-valued test, the exact class this lane keeps
    # finding. Each state now says what it is, and NOT-MEASURABLE is not a pass.
    contention = field(manifest, "host", "contention")
    if contention == "CONTENDED":
        out(
            "verdict-detail %s HOST the one-minute load average at session start "
            "was %s against a limit of %s, so something else was using the box. A "
            "contended host is the condition this issue exists because of -- the "
            "proxy bench it rejected measured the box as much as the branch. This "
            "is DISCLOSED, not refused: loadavg decays over a minute, so it also "
            "reports load the session itself has finished causing, and refusing "
            "would red a correct rig on an operator's second attempt"
            % (mode, field(manifest, "host", "loadavg1"),
               field(manifest, "host", "load_limit"))
        )
    elif contention == "NOT-MEASURABLE":
        out(
            "verdict-detail %s HOST whether the box was contended could not be "
            "measured. That is a gap in the record, not a quiet host" % mode
        )
    # LOCAL STORAGE -- the other property the `i4i` label stood for. NETWORK is
    # refused upstream; only the unmeasurable state reaches here, and it is
    # reported as itself rather than as a verified local disk.
    storage = field(manifest, "corpus", "storage")
    attested = manifest["corpus"].get("storage_attestation")
    if attested and storage != "LOCAL":
        # THE ATTESTATION TRAVELS WITH THE NUMBER. It is the only evidence this
        # verdict has for a requirement the criteria state, so it is printed
        # beside the verdict rather than left in the manifest for nobody to read.
        out(
            "verdict-detail %s STORAGE-ATTESTED the probe reported %s (%s) and "
            "the operator attested the corpus is on local storage: %r. This "
            "verdict rests on that attestation for the local-NVMe requirement, "
            "which was NOT independently verified"
            % (mode, storage, field(manifest, "corpus", "storage_detail"),
               attested)
        )
    if storage == "NOT-MEASURABLE":
        out(
            "verdict-detail %s STORAGE whether the corpus sits on local or "
            "network storage could not be measured (%s). The rig is specified for "
            "local NVMe; confirm it by hand before reporting this verdict"
            % (mode, field(manifest, "corpus", "storage_detail"))
        )
    elif storage == "UNRECOGNISED":
        out(
            "verdict-detail %s STORAGE the corpus device reports a model this "
            "probe does not recognise (%s), so it is NOT known to be local. The "
            "only signal that separates network-attached block storage from "
            "instance storage is the vendor model string -- rotational flags and "
            "filesystem types are identical for both -- so an unrecognised device "
            "is declared, not assumed local. Confirm it by hand"
            % (mode, field(manifest, "corpus", "storage_detail"))
        )
    for line in non_exhaustive_lines(mode, len(pairs)):
        out("verdict-detail %s NON-EXHAUSTIVE %s" % (mode, line))
    out("---- end section %s ----" % mode)
    return verdict


def run_section(mode, path, opts):
    try:
        return analyze(mode, path, opts)
    except Unmeasured as exc:
        return _unmeasured(mode, exc.cause, exc.detail)
    except Exception as exc:  # noqa: BLE001 -- see below
        # A traceback breaks the anchoring invariant AND leaves the section with
        # NO verdict line, which is worse than a wrong verdict because nothing
        # downstream can detect it. Every escape becomes an anchored UNMEASURED
        # with the exception named. SystemExit and KeyboardInterrupt are
        # BaseException and deliberately still propagate.
        return _unmeasured(
            mode,
            "internal-error",
            "%s: %s -- this is a defect in the analyzer, not a property of the "
            "measurement; the section is reported UNMEASURED rather than left "
            "without a verdict" % (type(exc).__name__, exc),
        )


def _unmeasured(mode, cause, detail):
    err("cause %s %s" % (mode, cause))
    err("cause-detail %s %s" % (mode, detail))
    out("verdict %s UNMEASURED" % mode)
    out("verdict-detail %s no verdict was rendered; cause %s" % (mode, cause))
    out(
        "verdict-detail %s a consumer must treat UNMEASURED as no result, never "
        "as a permissive default" % mode
    )
    out("---- end section %s ----" % mode)
    return "UNMEASURED"


def main(argv):
    try:
        return _main(argv)
    except Exception as exc:  # noqa: BLE001 -- the outermost anchor
        err("cause internal-error %s: %s" % (type(exc).__name__, exc))
        out("verdict harness UNMEASURED")
        out(
            "verdict-detail harness the analyzer failed before any section could "
            "be decided; no result was produced"
        )
        return SECTION_EXIT["UNMEASURED"]


def _main(argv):
    opts = parse_args(argv)
    requested = []
    if opts["single_stream"] is not None:
        requested.append((MODE_SINGLE_STREAM, opts["single_stream"]))
    if opts["utilization"] is not None:
        requested.append((MODE_UTILIZATION, opts["utilization"]))

    out("=== issue #3649 -- served-path A/B throughput, #2820 batched merge fan-in ===")
    # A REPORT THAT COVERS ONE QUANTITY AND IS SILENT ABOUT THE OTHER IS HOW AN
    # INCOMPLETE SESSION GETS READ AS A COMPLETE ANSWER -- the same class as a PR
    # body over-claiming. So the coverage is stated in the report's OWN output,
    # naming the quantity that is missing, at the top AND after the verdicts:
    # a reader who scrolls to the verdict must not have to remember a header.
    covered = [mode for mode, _ in requested]
    missing = [m for m in (MODE_SINGLE_STREAM, MODE_UTILIZATION) if m not in covered]
    out("sections %s" % ",".join(covered))
    coverage_note = (
        "sections-coverage this run covers %s. The acceptance criteria require "
        "BOTH the single-stream target band and the utilization direction, so it "
        "does NOT cover %s and does not discharge the criteria on its own"
        % (",".join(covered), ",".join(missing))
    )
    if missing:
        out(coverage_note)
    for mode, path in requested:
        out("manifest %s %s" % (mode, path))

    codes = []
    for mode, path in requested:
        codes.append(SECTION_EXIT[run_section(mode, path, opts)])
    if missing:
        out(coverage_note)
    return max(codes)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
