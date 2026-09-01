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

from ab_common import Unmeasured, err, out
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


def render_common(manifest, mode, admission):
    out("mode %s" % mode)
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
        "server batch-size %s max-batch-bytes %s"
        % (
            field(manifest, "workload", "batch_size"),
            field(manifest, "workload", "max_batch_bytes"),
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

    pairs, admission = collect_pairs(manifest, manifest_dir, mode, declared_steps)
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
    render_common(manifest, mode, admission)
    return report(mode, manifest, pairs, admission, opts, stats)


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


def report(mode, manifest, pairs, admission, opts, stats):
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
        "replicates requested %d paired %d order interleaved-base-head"
        % (manifest["replicates_requested"], len(pairs))
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
    if control:
        out(
            "verdict-detail %s CONTROL this session is labelled %r, so its verdict "
            "describes the control and does NOT discharge the #3649 acceptance "
            "criteria" % (mode, control)
        )
    if asymmetric:
        out(
            "verdict-detail %s CONTROL the two arms were served under DIFFERENT "
            "server flags, so the difference measured is the injected one and not "
            "the commit pair's" % mode
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
    out("sections %s" % ",".join(mode for mode, _ in requested))
    for mode, path in requested:
        out("manifest %s %s" % (mode, path))

    codes = []
    for mode, path in requested:
        codes.append(SECTION_EXIT[run_section(mode, path, opts)])
    return max(codes)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
