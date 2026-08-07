#!/usr/bin/env python3
"""DOES THE RECORDED CANONICAL COMPARISON SUPPORT THE REPORT'S CLAIM? (#3272 round 15)

The READER half of the canonical-corpus check, split from `ws0_canonical_corpus.py` under the
campsite rule: rounds 14 and 15 took that file to 1087 lines against the ~800 source target. The
seam is a RESPONSIBILITY boundary rather than a cut at a convenient line, and the two sides answer
different questions about different inputs:

    ws0_canonical_corpus  IS THIS CORPUS THE CANONICAL ONE?  reads the RUST PIN + the committed
                          artifact + a corpus identity, and performs the comparison. Runs BEFORE
                          the first rep, in the driver.
    this module           DOES THE RECORD SUPPORT ITS OWN VERDICT?  reads a SESSION PIN written by
                          that comparison, and never opens the Rust pin or the artifact. Runs at
                          REPORT TIME, in the reporter.

Why the reporter reads a record instead of re-running the comparison is the reason the split is
along this line at all: the canonical pin can be re-pinned between measurement and reporting, and a
results dir is routinely reviewed from another checkout — so a report-time comparison would judge
the session against a shape it never ran against, in EITHER direction (a session that WAS canonical
reported as divergent, or a divergent one blessed by a pin that moved). The two halves therefore
CANNOT share an implementation, and keeping them in one file made that look like an accident of
layout rather than the design it is.

What crosses the seam is imported, never restated: the census (`CANONICAL_CENSUS`), the two mode
words, the non-baseline label, and the two source paths all live on the classifier side, which owns
them, and this module compares against those constants. A second spelling of any of them would be
the two-place invariant this whole issue keeps finding.
"""

from __future__ import annotations

import pathlib

from ws0_canonical_corpus import (
    CANONICAL_ARTIFACT_REL,
    CANONICAL_CENSUS,
    MODE_BASELINE,
    MODE_NON_BASELINE,
    NON_BASELINE_LABEL,
    RUST_PIN_REL,
)
from ws0_validate import Invalid, exact_int

# The pin field this module's record is written to. Defined HERE, in the module that owns the
# record's shape, and imported by the writer/reader — one spelling, so they cannot drift onto two
# names (which would present as an absent-field refusal on a session that recorded it correctly).
PIN_CANONICAL_FIELD = "canonical_corpus"

# The record's REQUIRED keys. Every one is read by `verify_pinned_canonical_corpus` below; a key
# added here without a reader is the written-but-unread shape round 6's B2 found, so the assert at
# the bottom of this module closes that direction at import.
CANONICAL_RECORD_FIELDS = (
    "mode",
    "is_canonical",
    "is_baseline",
    "label",
    "divergences",
    "compared_fields",
    "canonical_pin_source",
    # THE COMPONENT MAP'S PROVENANCE AND EXTENT (#3272 round 15, A). Both were written by
    # `classify_corpus` and read by NOTHING: neither was required here nor validated below, so
    # removing or altering either left the report still claiming the complete component map had been
    # verified. That is round 6's B2 shape — a field WRITTEN and compared against nothing anywhere
    # in the tree — reintroduced by round 14's F4, in the very commit that added the map comparison
    # whose scope these two fields describe. The import-time assert at the bottom of this module
    # exists to make exactly that impossible and could not see it, because the fields were never
    # DECLARED here to be checked against the reader.
    "canonical_component_source",
    "canonical_components",
)


def verify_pinned_canonical_corpus(pin_path: pathlib.Path, pin: dict) -> dict:
    """Require the pre-measurement CANONICAL COMPARISON, and require it to be self-supporting.

    # Why the reporter reads this rather than re-deriving it

    Re-deriving at report time would compare the corpus against whatever the canonical pin says
    NOW. A re-pin between measurement and reporting (or a report generated from a different
    checkout — results dirs are routinely reviewed elsewhere) would then judge the session against
    a shape it never ran against, in either direction: a session that WAS canonical reported as
    divergent, or a divergent one silently blessed by a pin that moved. Same reason the CPU-pin
    verification is recorded where it was made rather than re-read at report time (#3272 F6).

    # What is checked, and why a bare `is_baseline` would not be enough

    The record must SUPPORT its own verdict. `is_baseline` is re-derived here from `mode` and
    `is_canonical`, and `is_canonical` from whether `divergences` is empty — so a hand-edited
    `is_baseline: true` sitting beside a non-empty divergence list is REFUSED rather than printed.
    A recorded boolean nobody re-derives is the written-but-unread shape with an extra step.
    """
    if not isinstance(pin, dict):
        raise Invalid(f"{pin_path} must hold a JSON object")
    rec = pin.get(PIN_CANONICAL_FIELD)
    if not isinstance(rec, dict) or not rec:
        raise Invalid(
            f"{pin_path} records no `{PIN_CANONICAL_FIELD}` block, so it does not record whether"
            " the corpus it measured is THE CANONICAL MEASUREMENT CORPUS. Before #3272 round 13's"
            " F3 the pin recorded the identity of whatever corpus it was handed and compared it"
            " against nothing, so a smoke-sized corpus was self-consistent through every"
            " downstream check and published as a WS0 BASELINE. Re-run the session with the"
            " current driver, which performs the comparison before the first rep."
        )
    absent = [f for f in CANONICAL_RECORD_FIELDS if f not in rec]
    if absent:
        raise Invalid(
            f"{pin_path} `{PIN_CANONICAL_FIELD}` is INCOMPLETE — no {', '.join(absent)}. A"
            " partial record cannot establish whether this session measured the canonical corpus."
        )
    mode, diffs = rec["mode"], rec["divergences"]
    if mode not in (MODE_BASELINE, MODE_NON_BASELINE):
        raise Invalid(
            f"{pin_path} `{PIN_CANONICAL_FIELD}.mode` is {mode!r}, not {MODE_BASELINE!r} or"
            f" {MODE_NON_BASELINE!r} — an unrecognised mode supports neither answer."
        )
    if not isinstance(diffs, list):
        raise Invalid(
            f"{pin_path} `{PIN_CANONICAL_FIELD}.divergences` must be a list, got"
            f" {type(diffs).__name__} — the verdict below is DERIVED from it."
        )
    # ...AND EVERY ELEMENT IS A NON-EMPTY STRING (#3272 round 15, B). The list's shape was checked
    # and its CONTENTS were not, so a non-string element passed validation and then raised an
    # UNCAUGHT `TypeError` in the reporter's `"; ".join(canonical["divergences"])` — a traceback
    # instead of a named refusal, on the artifact this record exists to police. MEASURED: a
    # `[{"rows": "wrong"}]` reached `sequence item 0: expected str instance, dict found`.
    #
    # An EMPTY string is refused too, and that is the load-bearing half: `divergences` is the
    # evidence for the verdict, and the report PRINTS these strings as the reason a run is not a
    # baseline. An empty element counts toward `len(diffs)` (so the verdict re-derivation is
    # satisfied) while printing NOTHING, so a reader is told a run diverged in N fields and shown
    # fewer than N reasons.
    for i, d in enumerate(diffs):
        if not isinstance(d, str) or not d.strip():
            raise Invalid(
                f"{pin_path} `{PIN_CANONICAL_FIELD}.divergences[{i}]` is {d!r}, not a non-empty"
                " string. Each element is the SENTENCE the report prints as a reason this corpus is"
                " not the canonical one: a non-string raises an uncaught TypeError where the report"
                " joins them, and an empty one counts toward the divergence total while printing no"
                " reason at all (#3272 round 15, B)."
            )
    # THE VERDICTS ARE RE-DERIVED, never trusted — and they must be EXACT JSON BOOLEANS.
    #
    # `bool()` was the defect (#3272 round 15, B): it is TRUTHINESS, so the STRING `"false"` is
    # TRUE. MEASURED — a record carrying `"is_baseline": "false"` was ACCEPTED and re-emitted with
    # `is_baseline` still the string `'false'`, which the reporter then treats as true in
    # `if canonical["is_baseline"]` and prints the BASELINE title over it. Symmetrically `0`, `""`
    # and `None` are FALSE to `bool()` without being the recorded `false`. This is the `!= BAD`
    # permissive-coercion shape this whole issue exists to remove, and `divergences`'s own `bool`
    # comparison two hundred lines up already does it correctly: `isinstance(got, bool)`, then
    # identity. Same rule here.
    for verdict in ("is_canonical", "is_baseline"):
        if not isinstance(rec[verdict], bool):
            raise Invalid(
                f"{pin_path} `{PIN_CANONICAL_FIELD}.{verdict}` is {rec[verdict]!r}"
                f" ({type(rec[verdict]).__name__}), not the JSON boolean true or false. It is"
                " re-derived below and read by the reporter as a verdict, and a truthiness test"
                f" would have read the STRING 'false' as TRUE — publishing a smoke corpus under the"
                " BASELINE title (#3272 round 15, B)."
            )
    if rec["is_canonical"] != (not diffs):
        raise Invalid(
            f"{pin_path} `{PIN_CANONICAL_FIELD}` CONTRADICTS ITSELF: is_canonical="
            f"{rec['is_canonical']!r} beside {len(diffs)} recorded divergence(s). The verdict is"
            " DERIVED from the divergences, so these cannot both be true — this record was edited."
        )
    if rec["is_baseline"] != (not diffs and mode == MODE_BASELINE):
        raise Invalid(
            f"{pin_path} `{PIN_CANONICAL_FIELD}` CONTRADICTS ITSELF: is_baseline="
            f"{rec['is_baseline']!r} with mode={mode!r} and {len(diffs)} divergence(s). A run is a"
            " baseline only when the corpus matched EVERY canonical field AND"
            f" {MODE_BASELINE!r} was requested."
        )
    if not isinstance(rec["label"], str) or not rec["label"].strip():
        raise Invalid(
            f"{pin_path} `{PIN_CANONICAL_FIELD}.label` is empty — the report PRINTS this label, so"
            " a non-baseline run would be published carrying no words saying so."
        )
    if not rec["is_baseline"] and NON_BASELINE_LABEL not in rec["label"]:
        raise Invalid(
            f"{pin_path} `{PIN_CANONICAL_FIELD}.label` does not carry"
            f" {NON_BASELINE_LABEL!r} on a run that is NOT a baseline. The label is the ONLY thing"
            " in the printed report that distinguishes a smoke run from a baseline, so a softened"
            " one is the whole finding back again."
        )
    # THE COMPARISON'S SCOPE IS CHECKED, not merely carried. A record listing FEWER compared
    # fields than this module requires describes a WEAKER comparison than the one the report will
    # cite — a session pinned by an older driver that compared three fields would otherwise be
    # reported exactly like one that compared all nine.
    compared = rec["compared_fields"]
    if not isinstance(compared, list):
        raise Invalid(
            f"{pin_path} `{PIN_CANONICAL_FIELD}.compared_fields` must be a list, got"
            f" {type(compared).__name__}."
        )
    want = sorted(CANONICAL_CENSUS)
    if sorted(str(c) for c in compared) != want:
        missing = sorted(set(want) - {str(c) for c in compared})
        raise Invalid(
            f"{pin_path} `{PIN_CANONICAL_FIELD}` records a comparison over"
            f" {len(compared)} field(s), not this module's {len(want)}"
            + (f" (no {', '.join(missing)})" if missing else "")
            + ". The report cites this as 'the canonical fields were compared', so a narrower"
            " recorded comparison would be published as a full one. Re-run the session with the"
            " current driver."
        )
    # ...and WHICH pin it was compared against, so the report names its source rather than
    # asserting one. A record naming a different file is not this rig's canonical comparison.
    if rec["canonical_pin_source"] != RUST_PIN_REL:
        raise Invalid(
            f"{pin_path} `{PIN_CANONICAL_FIELD}.canonical_pin_source` is"
            f" {rec['canonical_pin_source']!r}, not {RUST_PIN_REL!r} — the recorded comparison was"
            " made against a different pin, so it does not establish this rig's canonical shape."
        )
    # ...and WHICH ARTIFACT the COMPONENT MAP was compared against (#3272 round 15, A). Same rule as
    # the pin source above, for the second of the two canonical sources: the map is the one canonical
    # value that lives in the artifact rather than in Rust, so a record naming a different file did
    # not compare against this rig's canonical component map. This field was WRITTEN and read by
    # nothing — altering it left the report still citing the complete map as verified.
    if rec["canonical_component_source"] != CANONICAL_ARTIFACT_REL:
        raise Invalid(
            f"{pin_path} `{PIN_CANONICAL_FIELD}.canonical_component_source` is"
            f" {rec['canonical_component_source']!r}, not {CANONICAL_ARTIFACT_REL!r} — the recorded"
            " COMPONENT MAP comparison was made against a different artifact, so it does not"
            " establish this rig's canonical component set. The map is the one canonical value that"
            " lives in the artifact rather than in the Rust pin (#3272 round 15, A)."
        )
    # ...and HOW MANY COMPONENTS that comparison covered, CHECKED rather than carried.
    #
    # The report prints the component map as compared name-by-name; a record claiming ZERO (or a
    # fractional, boolean or negative count) would be published under that sentence having compared
    # nothing. `exact_int` for the same reason as every other integer in this module: `int()` accepts
    # bools and truncates floats (round 12's F5).
    comp_count = exact_int(
        f"{pin_path} `{PIN_CANONICAL_FIELD}.canonical_components`",
        rec["canonical_components"],
        "It is the number of canonical components the recorded comparison covered, which the report"
        " cites as the complete component map (#3272 round 15, A).",
    )
    if comp_count < 1:
        raise Invalid(
            f"{pin_path} `{PIN_CANONICAL_FIELD}.canonical_components` is {comp_count}, so the"
            " recorded comparison covered NO components while the report cites the COMPLETE"
            " component map as compared. An empty canonical map is fatal at classify time"
            " (`canonical_components` refuses it), so a record claiming one was edited."
        )
    # THE COUNT IS CROSS-CHECKED, on the one run where the record itself determines it. A CANONICAL
    # run compared the corpus's component names against the canonical map BOTH DIRECTIONS and found
    # no divergence, so the two key sets are EQUAL — therefore this count must equal the number of
    # components the pin recorded for the corpus it measured. Two records of one fact are two chances
    # to disagree, so they are compared rather than assumed; the count is deliberately NOT re-derived
    # from the artifact on disk, for the reason this whole function exists (a re-pin between
    # measurement and reporting would judge the session against a map it never ran against).
    #
    # Only for a CANONICAL run: a divergent corpus legitimately has a different number of components
    # — that difference IS one of the recorded divergences.
    if rec["is_canonical"]:
        pinned = pin.get("components")
        if not isinstance(pinned, dict) or not pinned:
            raise Invalid(
                f"{pin_path} records `{PIN_CANONICAL_FIELD}.is_canonical` true — which means the"
                " corpus's component map was compared against the canonical one and matched — while"
                " the pin itself records no `components` map for the corpus it measured. A canonical"
                " verdict cannot rest on a map that was never recorded (#3272 round 15, A)."
            )
        if len(pinned) != comp_count:
            raise Invalid(
                f"{pin_path} `{PIN_CANONICAL_FIELD}` CONTRADICTS ITSELF: it records a CANONICAL"
                f" verdict over {comp_count} canonical component(s) while the pin records"
                f" {len(pinned)} component(s) for the corpus measured. A canonical corpus matched"
                " the canonical map in BOTH directions, so the two counts are equal by"
                " construction — one of them was edited (#3272 round 15, A)."
            )
    out = dict(rec)
    out["source"] = str(pin_path)
    return out


# THE DECLARED-BUT-UNREAD DIRECTION, closed at import (#3272 round 6, B2's lesson). Round 5's F3
# added the component map to the pin and NOTHING in the tree read it, so the field read as a guard
# while being inert. This assert makes the same mistake impossible for this record: every declared
# field must be SUBSCRIPTED in the reader's own source.
#
# THE ORACLE IS VALIDATED BEFORE ITS NEGATIVE IS TRUSTED, and that is not ceremony — the first
# version of this assert searched `repr(co_consts)` for `"label"` WITH double quotes, while the repr
# spells constants with SINGLE quotes, so it FAILED on a field the reader does read. A guard that
# reds on correct input is the guard people learn to delete. The positive control below is a name
# this reader provably does NOT read, so if the scan cannot see the difference it says so.
def _reader_reads(field: str) -> bool:
    import inspect

    src = inspect.getsource(verify_pinned_canonical_corpus)
    return f'rec["{field}"]' in src or f"rec[{field!r}]" in src


if _reader_reads("a_field_this_reader_does_not_read"):  # pragma: no cover
    raise Invalid(
        "the declared-but-unread scan reports a field the reader CANNOT be reading, so it cannot"
        " distinguish read from unread and its negative means nothing (#3272: validate the oracle"
        " before trusting it)."
    )
for _f in CANONICAL_RECORD_FIELDS:
    if not _reader_reads(_f):
        raise Invalid(
            f"`{_f}` is declared in CANONICAL_RECORD_FIELDS but verify_pinned_canonical_corpus"
            " never subscripts it — a recorded field nobody reads is the written-but-unread shape"
            " (#3272 round 6, B2). Wire it, or remove the declaration."
        )
del _f
