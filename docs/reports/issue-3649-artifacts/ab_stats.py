"""
The statistics and the two verdict rules for #3649.

Pure functions, no I/O, so `selftest-analyze.sh` can drive the rules through
real input and so the report module has nothing to decide.

DETERMINISM IS A PROPERTY OF THIS MODULE
----------------------------------------
One seeded generator per estimator, nearest-rank percentiles, and no wall clock
anywhere, so two runs over one input produce byte-identical output. The seed is
recorded in the report; it is not a hidden constant.

TWO QUANTITIES, TWO RULES -- THIS IS THE PART THAT IS EASY TO GET WRONG
------------------------------------------------------------------------
The acceptance criteria carry two different measurements, and the sources
verdict them differently:

  * SINGLE-STREAM throughput (`--ramp 1`) is what the TARGET BAND applies to.
    `docs/research/phase2-verify-row-engine.md` line 107: "Revised: ~1.1-1.25x
    narrow single-stream, ~1.05-1.1x wide". Rule: `decide_single_stream`.

  * UTILIZATION throughput (a concurrency ramp) is what 1.5-1.9x relates to, and
    that figure is a CEILING, not a target. Same file, line 115: "Keep 1.5-1.9x
    as a rig-narrow ceiling, not a field figure". The plan of record phrases the
    M2 criterion as util throughput "rises measurably toward the 1.5-1.9x
    ceiling" (`docs/architecture/throughput-program-2026-07.md` line 371) --
    a DIRECTIONAL claim with an interval, never a comparison against 1.5-1.9x.
    Rule: `decide_utilization`, whose token set contains nothing that could be
    read as having met a ceiling, and which never receives the ceiling figure as
    an argument. It cannot test against it because it is not given it.
"""

import math
import random

# Target bands for the SINGLE-STREAM quantity only.
# docs/research/phase2-verify-row-engine.md line 107.
TARGET_BANDS = {
    "narrow": (1.10, 1.25),
    "wide": (1.05, 1.10),
}

SINGLE_STREAM_TOKENS = ("MEETS-TARGET", "BELOW-TARGET", "ABOVE-TARGET", "INCONCLUSIVE")
UTILIZATION_TOKENS = ("RISES", "FALLS", "INCONCLUSIVE")

DEFAULT_SEED = 3649
DEFAULT_RESAMPLES = 10000
DEFAULT_CI_LEVEL = 0.95
# 5, not 3. At n <= 3 a 10000-draw percentile bootstrap is not an interval at
# all -- see `interval_is_degenerate` for the arithmetic.
DEFAULT_MIN_PAIRS = 5

#: The vCPU count of the narrow rig the target band was derived on: `i4i.xlarge`,
#: the M0 server-direct profile, "4 vCPU / 2 physical Ice Lake cores"
#: (RUNBOOK.md line 9, sourced from
#: website/src/content/docs/field-validation/m0-throughput.md's method note).
#:
#: THIS IS THE vCPU COUNT (4), NOT THE PHYSICAL CORE COUNT (2), stated because
#: the profile names both and a later reader would otherwise be right to
#: "correct" it. `nproc` reports vCPUs, so 4 is the number that comparison needs;
#: the physical count is not portably measurable and is not what is asserted.
NARROW_PROFILE_NPROC = 4


def geometric_mean(values):
    """The natural centre for ratio data.

    The geometric mean of head/base is the reciprocal of the geometric mean of
    base/head, which is not true of the arithmetic mean -- so the direction in
    which the ratio happens to be written cannot move the answer.

    Callers MUST have established that every value is finite and strictly
    positive: `math.log(0.0)` raises and `math.log(inf)` is `inf`. Both are
    reachable from real inputs that pass a naive `> 0` test -- a ratio can
    underflow to 0.0 from two finite operands, and overflow to inf from a
    subnormal denominator -- so the check lives at the input boundary
    (`ab_input`) and again on the computed ratios (`analyze-ab`), not here.
    """
    return math.exp(sum(math.log(v) for v in values) / len(values))


def is_usable_ratio(value):
    """Finite and strictly positive -- the precondition of everything above."""
    return isinstance(value, float) and math.isfinite(value) and value > 0.0


def arithmetic_mean(values):
    return sum(values) / len(values)


def median(values):
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def interval_is_degenerate(ci_low, ci_high, values):
    """Did the bootstrap contribute anything, or is this just the observed range?

    THE ARITHMETIC. The percentile bounds are nearest-rank over `resamples`
    draws, so at the 2.5% tail the lower bound is draw number
    `ceil(0.025 * resamples)`. The all-minimum resample -- every one of the n
    draws landing on the smallest value -- has probability `1 / n**n`. At n = 3
    that is 1/27 = 3.7%, which EXCEEDS the 2.5% tail, so the 2.5th percentile of
    the draws IS `min(values)`, and by symmetry the 97.5th IS `max(values)`. The
    "95% confidence interval" is then exactly the observed range, at coverage far
    below 95%, and three identical pairs produce a ZERO-WIDTH interval that
    trivially lands inside any band containing them.

    This is measured, not predicted: it is checked against the interval the
    bootstrap ACTUALLY returned rather than against a hard-coded n. So it keeps
    working if someone changes the resample count, the tail, or the floor -- and
    it also catches the all-values-identical case, which is degenerate at any n.
    """
    return ci_low == min(values) and ci_high == max(values)


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


# ---------------------------------------------------------------------------
# Rule 1 -- SINGLE-STREAM, against the target band
# ---------------------------------------------------------------------------

def decide_single_stream(ci_low, ci_high, band_low, band_high):
    """Map a ratio interval onto the single-stream token set.

      1. interval entirely inside [lo, hi]  -> MEETS-TARGET
      2. else ci_high < lo                  -> BELOW-TARGET
      3. else ci_low  > hi                  -> ABOVE-TARGET
      4. else                               -> INCONCLUSIVE

    Rules 2 and 3 fire only when the WHOLE interval is on one side of the band,
    i.e. only when the data RULE THE TARGET OUT. Rule 4 is everything else: an
    interval overlapping the band without being contained in it cannot
    distinguish "meets the target" from "does not", and that is reported as such
    rather than rounded into a number-shaped verdict.

    ONE READING DECISION, RECORDED RATHER THAN BURIED. "The interval overlaps
    1.0" is NOT by itself INCONCLUSIVE. A TIGHT interval straddling 1.0 -- say
    [0.99, 1.02] -- excludes the whole band, so it is a MEASURED no-effect (rule
    2), which is a different fact from "the data cannot tell". A WIDE interval
    straddling 1.0 also overlaps the band and lands in rule 4. Both underlying
    facts are printed on their own `test` lines every run, so a reader who
    prefers a different rule can apply it to the same numbers.
    """
    if band_low <= ci_low and ci_high <= band_high:
        return "MEETS-TARGET"
    if ci_high < band_low:
        return "BELOW-TARGET"
    if ci_low > band_high:
        return "ABOVE-TARGET"
    return "INCONCLUSIVE"


# ---------------------------------------------------------------------------
# Rule 2 -- UTILIZATION, a direction with an interval and nothing else
# ---------------------------------------------------------------------------

def decide_utilization(ci_low, ci_high):
    """Map a ratio interval onto the utilization token set.

      interval entirely above 1.0 -> RISES
      interval entirely below 1.0 -> FALLS
      interval covering 1.0       -> INCONCLUSIVE

    Note what this signature does NOT take: a band, a ceiling, or any threshold.
    The 1.5-1.9x figure is a rig-narrow UTILIZATION CEILING recorded as
    unmeasured, and the M2 criterion is that util throughput "rises measurably
    toward" it -- a direction, not an attainment. Passing the ceiling in here at
    all would make a comparison against it expressible, so it is not passed in,
    and no token in `UTILIZATION_TOKENS` could carry such a claim.
    """
    if ci_low > 1.0:
        return "RISES"
    if ci_high < 1.0:
        return "FALLS"
    return "INCONCLUSIVE"


SINGLE_STREAM_DETAIL = {
    "MEETS-TARGET": (
        "the whole {level} interval of the per-pair single-stream throughput "
        "ratio lies inside the target band [{lo}, {hi}]"
    ),
    "BELOW-TARGET": (
        "the whole {level} interval lies BELOW the target band [{lo}, {hi}]: the "
        "target is ruled out by the data. This is a measured result, not an "
        "absence of one -- note it fires whether or not the interval straddles "
        "1.0, because a tight interval around 1.0 is a measured no-effect"
    ),
    "ABOVE-TARGET": (
        "the whole {level} interval lies ABOVE the target band [{lo}, {hi}]. The "
        "band, not any ceiling, is what was tested"
    ),
    "INCONCLUSIVE": (
        "the {level} interval overlaps the target band [{lo}, {hi}] without being "
        "contained in it, so the data cannot distinguish meeting the target from "
        "not meeting it. This is reported as a non-result and must not be "
        "rounded into a number-shaped verdict, and no regression may be filed "
        "from it"
    ),
}

UTILIZATION_DETAIL = {
    "RISES": (
        "the whole {level} interval of the per-pair peak-utilization throughput "
        "ratio lies above 1.0, so utilization throughput rose measurably. This "
        "is a DIRECTION with an interval and nothing more: no attainment of the "
        "1.5-1.9x ceiling is claimed, computed, or testable here"
    ),
    "FALLS": (
        "the whole {level} interval lies below 1.0, so utilization throughput "
        "fell measurably on this rig and corpus"
    ),
    "INCONCLUSIVE": (
        "the {level} interval covers 1.0, so no direction is established. "
        "Reported as a non-result, never rounded into a number-shaped verdict"
    ),
}
