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


# The canonical bounds a verdict's thresholds MAY ONLY TIGHTEN against. Mirrored from
# ws0_quiescence.DEFAULT_MAX_LOAD1 / DEFAULT_MAX_LOAD1_MOVEMENT, whose CLI already refuses a
# loosened knob ("QUIESCENCE_THRESHOLD_LOOSENED ... this knob may only tighten"). Re-asserted HERE
# because the reporter's job is not to trust the artifact: the writer's check protects a RUN, this
# one protects a REPORT, and a verdict reaching the reporter with `max_load1: 999` is
# self-consistent in every other respect while describing a bar nobody would accept (job 78 F2).
# Duplicated rather than imported to keep this module dependency-free; the values are asserted
# against the writer's by scripts/tests/test_ws0_quiescence_evidence_guards.sh.
# The census RULES the writer applies, mirrored for `_expected_census_scope`. Same posture as
# the bounds above: duplicated to keep this module dependency-free, drift-checked against
# ws0_quiescence.COMPETING_COMMS / COMPETING_CMDLINE by the guard suite. A rule ADDED to the
# writer without updating these makes the scope-note derivation disagree, which reds the guard
# suite -- deliberately: widening what the census covers changes what a zero census MEANS, and
# that sentence is published in the verdict.
CANONICAL_COMPETING_COMMS = ("rustc", "cargo", "cc1", "cc1plus", "ld", "lld", "mold")
CANONICAL_COMPETING_CMDLINE = ("agent-gate.sh",)


def _expected_census_breadth(narrow: int, samples: int) -> str:
    """Recompose `census_breadth` exactly as ws0_quiescence.judge() does.

    ASSERTED BY DERIVATION, NOT BY INSPECTION (roborev job 80 finding 2). Job 78 F3 said "a derived
    field must be asserted against its inputs, not passed through", and I implemented that as
    `startswith("FULL")` -- which is the same weakness one level down. Sniffing a prefix is not
    asserting a derivation: for a narrow census, ANY other nonempty text passed, so a misleading
    caveat could be published verbatim beside a nonzero `narrow_census_records`.
    """
    if narrow == 0:
        return "FULL (competing_count present on every in-window record)"
    return (f"NARROW on {narrow} of {samples} record(s): those carry"
            " rustc/cargo/gate only, so a short-lived cc1/ld/lld/mold between boundaries"
            " would not appear. Stated rather than implied.")


def _expected_census_scope(samples: int) -> str:
    """Recompose `census_scope` exactly as ws0_quiescence.census_scope_note() does.

    SAME FAMILY AS `census_breadth` ABOVE, and the reason is the same: the field states WHAT A
    ZERO CENSUS DOES NOT COVER (#3551 defect 3 -- 91 consecutive samples read
    competing_count=0 while load1 reached 6.39 and the pinned CPUs measured up to 86% busy).
    Inspected rather than derived, ANY other nonempty text would pass -- including a
    reassurance, which is precisely the thing this field exists to prevent. So it is asserted as
    a DERIVATION from the sample count it describes.

    Mirrored rather than imported, keeping this module dependency-free, and drift-checked
    against the writer by scripts/tests/test_ws0_quiescence_evidence_guards.sh (which compares
    the two functions' OUTPUT over several sample counts, not their source text).
    """
    return (
        f"BOUNDED, NOT SILENT: a zero census across {samples} in-window record(s) bounds"
        f" COMPILERS AND LINKERS ({', '.join(CANONICAL_COMPETING_COMMS)}) plus the named"
        f" script(s) ({', '.join(CANONICAL_COMPETING_CMDLINE)}) and NOTHING ELSE. It does NOT"
        " bound total foreign load: a peer lane running node, jest, python, git or a shell"
        " suite is INVISIBLE to this census, so this is 0 RECOGNISED competing processes, never"
        " 'nothing was running'. MEASURED (#3551): 91 consecutive samples read"
        " competing_count=0 while load1 reached 6.39 with 9 runnable tasks and the pinned CPUs"
        " measured a median 8% / max 86% busy with foreign work. In-window load1 and the"
        " per-sample `percpu` jiffy snapshot are CONTEXT, NOT GATES -- read them before"
        " trusting this verdict."
    )


def _expected_load1_after_note(is_bounded: bool) -> str:
    """Recompose `load1_after_note` exactly as ws0_quiescence.judge() does. Same family as above:
    a descriptive string composed from another field, so it is checked by regeneration. This one
    was NOT reported by review -- it is the second member of the family, swept rather than waited
    for."""
    if is_bounded:
        return "bounded: the caller asserted this sample was taken after settling"
    return ("RECORDED, NOT BOUNDED: load1 is a 1-minute decaying average, so a sample taken"
            " immediately after a CPU-bound window reads the window's own residue. The"
            " binding in-window check is the timeseries census.")


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
    # #3551 defect 3: the verdict must state what its zero census does NOT bound.
    "window_census.census_scope": (lambda v: isinstance(v, str) and v.strip() != "",
                                   "a non-empty string"),
    "window_census.timeseries": (lambda v: isinstance(v, str) and v.strip() != "",
                                 "a non-empty path string"),
    "window_census.window.start": (lambda v: _iso(v) is not None, "an ISO-8601 instant"),
    "window_census.window.end": (lambda v: _iso(v) is not None, "an ISO-8601 instant"),
}


# =============================================================================================
# THE PROPERTY MATRIX. Properties x fields, and every field MUST appear in exactly one cell.
#
# WHY THIS EXISTS (#3248, coordination ruling on roborev job 80). Closing the FIELD enumeration
# bounded WHICH things get checked and said nothing about HOW WELL. Two rounds then found instances
# of property families I had already named -- P1 applied to two of its three fields, P2 implemented
# as a prefix sniff. Fixing those as instances would have left the same hole open one field over.
#
# So the matrix is the SOURCE of the checks below, not a description of them: the enforcement loops
# iterate these tables. And `_assert_matrix_total()` requires every declared field to be classified,
# so a new field in FIELDS fails until someone decides which property applies. "Which cells did I
# fill?" therefore has a complete answer by construction, the way "which fields did I check?" does.
# =============================================================================================

# P1 -- A SELF-DECLARED LIMIT MUST CONFORM TO A CANONICAL CONSTANT. The verdict declares the bar it
# judged itself against; nothing stops it declaring a bar nobody would accept. Values mirrored from
# ws0_quiescence and drift-checked against it by the guard suite.
P1_CANONICAL: Dict[str, float] = {
    "thresholds.max_load1": 2.0,                    # DEFAULT_MAX_LOAD1
    "thresholds.max_load1_movement": 0.5,           # DEFAULT_MAX_LOAD1_MOVEMENT
    "window_census.coverage_gap_bound_s": 30.0,     # MAX_SAMPLE_GAP_S (writer enforces at :294)
}

# P2 -- A DERIVED OR DUPLICATED FIELD MUST BE ASSERTED AGAINST ITS INPUTS, never inspected. Each
# entry recomputes the field from what it is derived from; the loop requires equality. Nine members,
# not the two review happened to report: a value recorded twice is derived from itself.
P2_DERIVATIONS: Dict[str, Any] = {
    "load1_movement": (
        lambda f: abs(f["load1_after"] - f["load1_before"]),
        "abs(load1_after - load1_before)", 1e-3),
    "load1_before": (lambda f: f["before.load.load1"], "before.load.load1", _EPS),
    "load1_after": (lambda f: f["after.load.load1"], "after.load.load1", _EPS),
    "competing_before": (lambda f: f["before.competing_count"], "before.competing_count", 0),
    "competing_after": (lambda f: f["after.competing_count"], "after.competing_count", 0),
    "before.competing_count": (lambda f: len(f["before.competing"]), "len(before.competing)", 0),
    "after.competing_count": (lambda f: len(f["after.competing"]), "len(after.competing)", 0),
    "window_census.census_breadth": (
        lambda f: _expected_census_breadth(f["window_census.narrow_census_records"],
                                           f["window_census.samples"]),
        "f(narrow_census_records, samples)", None),
    "load1_after_note": (
        lambda f: _expected_load1_after_note(f["load1_after_is_bounded"]),
        "f(load1_after_is_bounded)", None),
    "window_census.census_scope": (
        lambda f: _expected_census_scope(f["window_census.samples"]),
        "f(samples)", None),
}

# TYPE_ONLY -- neither self-declared nor derived: a primary observation, or a literal. Listed
# EXPLICITLY so the matrix is total and a new field cannot land here by default.
TYPE_ONLY: frozenset = frozenset({
    "verdict",
    "load1_after_is_bounded",
    "before.competing", "after.competing",
    "before.load.load1", "before.load.load5", "before.load.load15", "before.load.runnable",
    "after.load.load1", "after.load.load5", "after.load.load15", "after.load.runnable",
    "window_census.samples", "window_census.competing_samples",
    "window_census.coverage_largest_gap_s",
    "window_census.load1_min", "window_census.load1_max", "window_census.load1_mean",
    "window_census.narrow_census_records", "window_census.timeseries",
    "window_census.window.start", "window_census.window.end",
})


# Readability aliases, SOURCED FROM P1_CANONICAL so there is exactly one table. An earlier version
# of this file carried a second literal copy of these three values -- a duplicated constant with no
# drift check is the same shape as the defect the matrix exists to close, one level down.
CANONICAL_MAX_LOAD1 = P1_CANONICAL["thresholds.max_load1"]
CANONICAL_MAX_LOAD1_MOVEMENT = P1_CANONICAL["thresholds.max_load1_movement"]
CANONICAL_COVERAGE_GAP_BOUND_S = P1_CANONICAL["window_census.coverage_gap_bound_s"]
# Back-compat name for the guard suite's drift check, which asserts the table against the writer.
CANONICAL_BOUNDS = P1_CANONICAL


def _assert_matrix_total() -> None:
    """Every declared field is classified by exactly one property. THE PIN.

    Without this the matrix is a comment: someone adds a field to FIELDS, no cell claims it, and it
    silently gets type-checking only -- which is exactly how P1 came to cover two of its three
    fields. Raising here converts that into an immediate, named failure.
    """
    classified = set(P1_CANONICAL) | set(P2_DERIVATIONS) | set(TYPE_ONLY)
    unclassified = sorted(set(FIELDS) - classified)
    if unclassified:
        raise EvidenceError(
            f"PROPERTY MATRIX INCOMPLETE: {len(unclassified)} declared field(s) have no property"
            f" cell: {', '.join(unclassified)}. Classify each as P1_CANONICAL (a self-declared"
            " limit), P2_DERIVATIONS (derived or duplicated from another field), or TYPE_ONLY (a"
            " primary observation). An unclassified field gets type-checking only, which is the"
            " job-80 defect."
        )
    stray = sorted(classified - set(FIELDS))
    if stray:
        raise EvidenceError(
            f"PROPERTY MATRIX names {len(stray)} field(s) absent from FIELDS:"
            f" {', '.join(stray)}. A cell for a field that does not exist enforces nothing."
        )
    overlap = sorted((set(P1_CANONICAL) & set(P2_DERIVATIONS))
                     | (set(P1_CANONICAL) & set(TYPE_ONLY))
                     | (set(P2_DERIVATIONS) & set(TYPE_ONLY)))
    if overlap:
        raise EvidenceError(
            f"PROPERTY MATRIX double-classifies: {', '.join(overlap)}. Exactly one cell per field,"
            " so the enforcement loops cannot disagree about which rule governs."
        )


def _leaves(obj: Any, prefix: str = "") -> Dict[str, Any]:
    """Flatten to dotted paths. A key CONTAINING a dot is refused, not flattened.

    THE DOT IS THE PATH SEPARATOR, SO A KEY CONTAINING ONE CAN FORGE A PATH (roborev job 78
    finding 1). A literal top-level key `"before.competing": []` produces the SAME dotted path as
    the nested `before` -> `competing`, and dict order is insertion order, so whichever comes last
    wins. Demonstrated: a verdict with a DIRTY nested `before.competing` (one competing process)
    plus a later literal `"before.competing": []` PASSED the supposedly closed checker.

    It evaded every other guard for a precise reason: the forged key is not an UNDECLARED field --
    it collides with a DECLARED one. So the undeclared-field check, which is what makes this
    checker closed, could not see it.

    This is the control/data-channel confusion from #3312 in miniature: the path string is CONTROL
    and the key names are DATA, and they shared an encoding. The fix removes the shared channel
    rather than picking a rarer separator -- a key with a dot cannot be a legitimate field of this
    record, so it is refused outright.
    """
    out: Dict[str, Any] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = str(k)
            if "." in key:
                raise EvidenceError(
                    f"key {key!r} (under {prefix or 'the top level'!r}) contains a '.', which is"
                    " this checker's PATH SEPARATOR. Such a key can forge the flattened path of a"
                    " nested field and overwrite its value, so it is refused rather than"
                    " flattened. ws0_quiescence.judge() never emits one."
                )
            path = f"{prefix}.{key}" if prefix else key
            if isinstance(v, dict):
                out.update(_leaves(v, path))
            else:
                out[path] = v
    return out


def assert_self_consistent(verdict: Any, where: str) -> None:
    """Raise EvidenceError unless the verdict's own record supports QUIESCENT."""
    if not isinstance(verdict, dict):
        raise EvidenceError(f"{where} is not a JSON object, so it carries no evidence at all.")
    _assert_matrix_total()
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

    # ---- P2: every derived/duplicated field, recomputed from its inputs ----
    for path, (derive, description, tol) in P2_DERIVATIONS.items():
        expected = derive(present)
        actual = present[path]
        if tol is None:
            agrees = actual == expected
        else:
            agrees = abs(actual - expected) <= tol
        if not agrees:
            raise EvidenceError(
                f"{where} `{path}` is {actual!r}, but its own inputs ({description}) give"
                f" {expected!r}. A derived field must AGREE with what it is derived from -- a"
                " verdict that disagrees with itself certifies nothing."
            )

    # ---- the remaining cross-field facts, which are not single-field derivations ----
    for side in ("before", "after"):
        if present[f"{side}.competing_count"] != 0:
            raise EvidenceError(
                f"{where} says QUIESCENT but the {side} boundary census lists"
                f" {present[f'{side}.competing_count']} competing process(es):"
                f" {present[f'{side}.competing'][:3]}. The verdict contradicts its own evidence."
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
    start_ts, end_ts = (_iso(present["window_census.window.start"]),
                        _iso(present["window_census.window.end"]))
    if start_ts is None or end_ts is None or not start_ts < end_ts:
        raise EvidenceError(
            f"{where} judged window {present['window_census.window.start']!r} .."
            f" {present['window_census.window.end']!r} does not run forwards."
        )

    # ---- the load thresholds, with the writer's OWN asymmetry ----
    for path, canonical in P1_CANONICAL.items():
        recorded = present[path]
        if recorded > canonical:
            raise EvidenceError(
                f"{where} records `{path}` of {recorded}, LOOSER than the canonical {canonical}."
                " A bound may only TIGHTEN -- ws0_quiescence enforces each of these, and a verdict"
                " carrying a loosened one describes a bar this report will not certify, however"
                " self-consistent the rest of it is."
            )
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
