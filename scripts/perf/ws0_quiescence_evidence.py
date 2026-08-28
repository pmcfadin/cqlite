#!/usr/bin/env python3
"""Assert a quiescence verdict is SELF-CONSISTENT with the word QUIESCENT it prints.

WHY THIS IS A MODULE AND NOT THREE MORE `if`s. Three consecutive review rounds found defects in
the reporter's inline verdict check -- job 73 F2 (the label was accepted with no evidence at all),
then job 75 F1/F2/F3 (load thresholds unchecked; `coverage_gap_bound_s` optional so DELETING the
bound skipped the comparison; `census_breadth` copied verbatim while contradicting
`narrow_census_records`). Every fix was a reaction to whichever hole the reviewer happened to find,
and the reviewer kept finding more, because pointwise patching of a checkable object converges only
as fast as review does.

The verdict is a CLOSED record: `ws0_quiescence.judge()` writes a known set of fields. So "which
fields did I check" has a complete answer, and this module answers it:

  * EVERY leaf field is declared in FIELDS with its type and its rule.
  * A MISSING declared field is an error -- never "checked if present", which is the shape that
    let `coverage_gap_bound_s` be deleted to escape its own comparison.
  * An UNDECLARED field is ALSO an error. That is the part that stops the regress: when
    `judge()` grows a field, this module fails until someone decides what the field means, rather
    than silently not checking it.

THE LOAD BOUNDS ARE ASYMMETRIC AND THAT IS DELIBERATE -- mirrored from `ws0_quiescence.judge()`
rather than reinvented, because guessing here produces a guard that reds valid runs:
  * `load1_before` is bounded by `max_load1` ALWAYS.
  * `load1_after` and `load1_movement` are bounded ONLY when `load1_after_is_bounded` is true.
    `load1` is a one-minute decaying average, so a sample taken right after a CPU-bound window
    reads the window's OWN residue; bounding it there measures how hard the rig just worked. A
    real committed verdict carries movement 0.99 against a 0.5 bound and is legitimately
    QUIESCENT for exactly this reason.
"""
from __future__ import annotations

import datetime
import math
from typing import Any, Callable, Dict, Optional, Tuple

# Tolerance for re-deriving a recorded float from its own components. Generous on purpose: the
# subject is "did this verdict contradict itself", not float reproducibility.
_EPS = 1e-6


class EvidenceError(Exception):
    """The verdict's own record does not support its conclusion."""


def _is_num(v: Any) -> bool:
    return not isinstance(v, bool) and isinstance(v, (int, float)) and math.isfinite(v)


def _is_int(v: Any) -> bool:
    return not isinstance(v, bool) and isinstance(v, int)


def _iso(v: Any) -> Optional[float]:
    if not isinstance(v, str):
        return None
    try:
        dt = datetime.datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.timestamp()


# path -> (predicate, human description). The path is dotted; `[]` means "a list".
FIELDS: Dict[str, Tuple[Callable[[Any], bool], str]] = {
    "verdict": (lambda v: v == "QUIESCENT", "the literal string QUIESCENT"),
    "competing_before": (lambda v: _is_int(v) and v >= 0, "a non-negative integer"),
    "competing_after": (lambda v: _is_int(v) and v >= 0, "a non-negative integer"),
    "load1_before": (_is_num, "a finite number"),
    "load1_after": (_is_num, "a finite number"),
    "load1_movement": (lambda v: _is_num(v) and v >= 0, "a finite non-negative number"),
    "load1_after_is_bounded": (lambda v: isinstance(v, bool), "a boolean"),
    "load1_after_note": (lambda v: isinstance(v, str) and v.strip() != "", "a non-empty string"),
    "thresholds.max_load1": (lambda v: _is_num(v) and v > 0, "a positive finite number"),
    "thresholds.max_load1_movement": (lambda v: _is_num(v) and v > 0,
                                      "a positive finite number"),
    "before.competing": (lambda v: isinstance(v, list), "a list"),
    "before.competing_count": (lambda v: _is_int(v) and v >= 0, "a non-negative integer"),
    "before.load.load1": (_is_num, "a finite number"),
    "before.load.load5": (_is_num, "a finite number"),
    "before.load.load15": (_is_num, "a finite number"),
    "before.load.runnable": (lambda v: isinstance(v, str) and "/" in v, "an 'a/b' string"),
    "after.competing": (lambda v: isinstance(v, list), "a list"),
    "after.competing_count": (lambda v: _is_int(v) and v >= 0, "a non-negative integer"),
    "after.load.load1": (_is_num, "a finite number"),
    "after.load.load5": (_is_num, "a finite number"),
    "after.load.load15": (_is_num, "a finite number"),
    "after.load.runnable": (lambda v: isinstance(v, str) and "/" in v, "an 'a/b' string"),
    "window_census.samples": (lambda v: _is_int(v) and v >= 1,
                              "an integer >= 1 (a zero-sample window is UNMEASURED, not quiet)"),
    "window_census.competing_samples": (lambda v: _is_int(v) and v >= 0,
                                        "a non-negative integer"),
    "window_census.coverage_largest_gap_s": (lambda v: _is_num(v) and v >= 0,
                                             "a finite non-negative number"),
    # REQUIRED, not optional. Job 75 F2: this was read as `if gap and bound`, so DELETING the
    # bound skipped the comparison that the bound exists to enable.
    "window_census.coverage_gap_bound_s": (lambda v: _is_num(v) and v > 0,
                                           "a positive finite number (REQUIRED: without it the"
                                           " coverage comparison silently would not run)"),
    "window_census.load1_min": (_is_num, "a finite number"),
    "window_census.load1_max": (_is_num, "a finite number"),
    "window_census.load1_mean": (_is_num, "a finite number"),
    "window_census.narrow_census_records": (lambda v: _is_int(v) and v >= 0,
                                            "a non-negative integer"),
    "window_census.census_breadth": (lambda v: isinstance(v, str) and v.strip() != "",
                                     "a non-empty string"),
    "window_census.timeseries": (lambda v: isinstance(v, str) and v.strip() != "",
                                 "a non-empty path string"),
    "window_census.window.start": (lambda v: _iso(v) is not None, "an ISO-8601 instant"),
    "window_census.window.end": (lambda v: _iso(v) is not None, "an ISO-8601 instant"),
}


def _leaves(obj: Any, prefix: str = "") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            path = f"{prefix}.{k}" if prefix else str(k)
            if isinstance(v, dict):
                out.update(_leaves(v, path))
            else:
                out[path] = v
    return out


def assert_self_consistent(verdict: Any, where: str) -> None:
    """Raise EvidenceError unless the verdict's own record supports QUIESCENT."""
    if not isinstance(verdict, dict):
        raise EvidenceError(f"{where} is not a JSON object, so it carries no evidence at all.")
    present = _leaves(verdict)

    missing = sorted(set(FIELDS) - set(present))
    if missing:
        raise EvidenceError(
            f"{where} is missing {len(missing)} required field(s): {', '.join(missing)}."
            " A QUIESCENT verdict is only as good as the record behind it, and a field that is"
            " absent was not measured. Re-run with the current ws0_quiescence.py."
        )
    # THE PART THAT STOPS THE REGRESS. An undeclared field means `judge()` grew something this
    # module has never considered; failing here forces the decision instead of silently skipping.
    extra = sorted(set(present) - set(FIELDS))
    if extra:
        raise EvidenceError(
            f"{where} carries {len(extra)} field(s) this checker does not know:"
            f" {', '.join(extra)}. The verdict writer has changed. Declare each field in"
            " ws0_quiescence_evidence.FIELDS with its rule -- an unchecked field is an"
            " unverified claim, and three review rounds were spent discovering exactly that."
        )
    for path, (ok, desc) in FIELDS.items():
        if not ok(present[path]):
            raise EvidenceError(
                f"{where} `{path}` is {present[path]!r}, which is not {desc}."
            )

    # ---- cross-field consistency: a verdict must not disagree with itself ----
    for side in ("before", "after"):
        lst, cnt = present[f"{side}.competing"], present[f"{side}.competing_count"]
        if cnt != len(lst):
            raise EvidenceError(
                f"{where} `{side}.competing_count` is {cnt} but `{side}.competing` holds"
                f" {len(lst)} entr(y/ies). An internally contradictory boundary sample is"
                " refused rather than reconciled."
            )
        if cnt != 0:
            raise EvidenceError(
                f"{where} says QUIESCENT but the {side} boundary census lists {cnt} competing"
                f" process(es): {lst[:3]}. The verdict contradicts its own evidence."
            )
        if present[f"competing_{side}"] != cnt:
            raise EvidenceError(
                f"{where} top-level `competing_{side}` is {present[f'competing_{side}']} but"
                f" `{side}.competing_count` is {cnt}. The same quantity recorded twice must"
                " agree."
            )
        if abs(present[f"load1_{side}"] - present[f"{side}.load.load1"]) > _EPS:
            raise EvidenceError(
                f"{where} top-level `load1_{side}` is {present[f'load1_{side}']} but"
                f" `{side}.load.load1` is {present[f'{side}.load.load1']}. The same"
                " observation recorded twice must agree."
            )

    if present["window_census.competing_samples"] != 0:
        raise EvidenceError(
            f"{where} says QUIESCENT but records"
            f" {present['window_census.competing_samples']} in-window sample(s) with a competing"
            " process. The label is not the measurement."
        )
    if present["window_census.narrow_census_records"] > present["window_census.samples"]:
        raise EvidenceError(
            f"{where} records {present['window_census.narrow_census_records']} narrow-census"
            f" record(s) among only {present['window_census.samples']} in-window sample(s),"
            " which is impossible."
        )
    # Job 75 F3: the published caveat string must AGREE with the count it describes, or the
    # report prints `FULL` over a narrow census.
    narrow = present["window_census.narrow_census_records"]
    breadth = present["window_census.census_breadth"]
    claims_full = breadth.startswith("FULL")
    if claims_full and narrow != 0:
        raise EvidenceError(
            f"{where} `census_breadth` claims {breadth!r} while"
            f" `narrow_census_records` is {narrow}. The caveat the report PUBLISHES would"
            " understate the evidence; a caveat that disagrees with its own count is refused."
        )
    if not claims_full and narrow == 0:
        raise EvidenceError(
            f"{where} `census_breadth` is {breadth!r} but `narrow_census_records` is 0."
            " The caveat overstates the limitation; the two must agree."
        )
    gap = present["window_census.coverage_largest_gap_s"]
    bound = present["window_census.coverage_gap_bound_s"]
    if gap > bound:
        raise EvidenceError(
            f"{where} says QUIESCENT but its largest in-window sampling gap ({gap}s) exceeds"
            f" its own stated bound ({bound}s). A window with an unobserved stretch that wide"
            " was not watched end to end."
        )
    lo, mean, hi = (present["window_census.load1_min"], present["window_census.load1_mean"],
                    present["window_census.load1_max"])
    if not (lo - _EPS <= mean <= hi + _EPS):
        raise EvidenceError(
            f"{where} in-window load1 mean {mean} lies outside its own min/max [{lo}, {hi}],"
            " which is impossible."
        )
    start, end = (_iso(present["window_census.window.start"]),
                  _iso(present["window_census.window.end"]))
    if start is None or end is None or not start < end:
        raise EvidenceError(
            f"{where} judged window {present['window_census.window.start']!r} .."
            f" {present['window_census.window.end']!r} does not run forwards."
        )

    # ---- the load thresholds, with the writer's OWN asymmetry ----
    max_l1 = present["thresholds.max_load1"]
    l1_before, l1_after = present["load1_before"], present["load1_after"]
    if l1_before > max_l1:
        raise EvidenceError(
            f"{where} says QUIESCENT but `load1_before` is {l1_before}, above its own"
            f" `thresholds.max_load1` of {max_l1}. ws0_quiescence.judge() refuses this"
            " unconditionally, so a verdict recording it contradicts the writer that produced it."
        )
    movement = abs(l1_after - l1_before)
    if abs(movement - present["load1_movement"]) > 1e-3:
        raise EvidenceError(
            f"{where} records `load1_movement` {present['load1_movement']} but its own"
            f" boundary samples differ by {movement:.4f}."
        )
    # ONLY when the after boundary was actually settled. Bounding it otherwise would refuse an
    # honest CPU-bound run reading its own decaying-average residue -- a real committed verdict
    # carries movement 0.99 against a 0.5 bound for exactly that reason.
    if present["load1_after_is_bounded"]:
        if l1_after > max_l1:
            raise EvidenceError(
                f"{where} declares the after boundary SETTLED and bounded, but `load1_after` is"
                f" {l1_after}, above `thresholds.max_load1` of {max_l1}."
            )
        max_mv = present["thresholds.max_load1_movement"]
        if movement > max_mv:
            raise EvidenceError(
                f"{where} declares the after boundary SETTLED and bounded, but load1 moved"
                f" {movement:.2f} between boundaries, above"
                f" `thresholds.max_load1_movement` of {max_mv}."
            )
