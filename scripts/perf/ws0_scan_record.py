#!/usr/bin/env python3
"""The BARE-SCAN producer's RECORD SURFACE: the fixed scan contract, verified (#3272).

The sibling of `ws0_loadgen_record.py` for arm A. That module answers "which fields of the LOAD
GENERATOR's record does the reporting path account for, and what must each equal"; this answers the
same question for `ws0-scan-bench`'s JSON (`tools/ws0-corpus-gen/src/bin/scan_bench.rs`), which is
the arm the whole reported ratio is DIVIDED BY.

# The finding

`ws0_collect.check_scan_passes` hardened the COUNTERS thoroughly — exactly `--scan-passes` records,
every pass observing the whole corpus, the aggregates DERIVED and cross-checked — and validated
NOTHING ABOUT WHAT PRODUCED THEM. The bench records seven measurement-defining fields beside those
counters (`arm`, `surface`, `corpus`, `schema`, `table_dirs_ingested`, `query`, `fold`) and the
reporter read none of them. So a scan run under a DIFFERENT `--fold` or a DIFFERENT `--project`
satisfies every existing check — the pass count is right, every pass observes exactly the pinned
corpus row count, the recorded aggregates equal the derived sums — while measuring MATERIALLY
DIFFERENT WORK, and is published as the bare-scan arm of the 1.3x ratio.

That is the class this issue has now found five times: A VALIDATOR THAT CHECKS THE COUNTERS BUT NOT
WHAT PRODUCED THEM. `target_concurrency` (round 12 F3) was a loadgen input classified `ignored`;
`round` (round 14 F1) was required present and never compared; `endpoint` (round 14 F2) was
classified `ignored` as "the loopback address". Every one was a true statement about the FIELD
standing in for a claim about the FIGURE.

It is worse here in one specific respect. `collect_scan` EMITS `"surface":
"cqlite_core::Database::execute_streaming"` into its own measurement block as an unconditional
literal — so the report ASSERTS which public surface produced the figure while the artifact's own
statement of that surface sat unread beside it. A claim printed about a field nobody compared.

# The mechanism, and why it is `FIXED_INPUTS`'s and not a new one

The four fields verified here have CONSTANT correct values for this rig, so they take the shape
round 12's F3 built for exactly that: a table of `field -> (value, why)`, compared EXACTLY after a
TYPE check, with the reason recorded beside the value. A scan-SCOPED instance rather than an entry
in the loadgen's table, because they are a different producer's fields with different values — one
table holding two producers' constants would have to be indexed by producer to be read at all,
which is two tables with extra steps.

`corpus`, `schema` and `table_dirs_ingested` are NOT here: their correct values come from the
SESSION (the pinned corpus path, the schema beside it, the table directory ingestion selected), so
they take the `SESSION_BOUND_INPUTS` shape instead — a constant expectation for them would be a
constant that differs per run, i.e. not a constant.
"""

from __future__ import annotations

from ws0_session import CORPUS_TABLE_SUBPATH
from ws0_validate import Invalid

# The projection this rig scans. `ws0-scan-bench --project` defaults to `*`, the driver never passes
# it, and `*` is the shape arm B streams — the two arms must read the SAME COLUMNS or the ratio
# compares a narrow scan against a full one.
SCAN_PROJECTION = "*"

# ============================================================================
# THE FIXED SCAN CONTRACT (#3272)
# ============================================================================
# Each entry is `(value, why)`, the shape `ws0_loadgen_record.FIXED_INPUTS` uses, and the reason is
# the sentence the refusal ends with. Every reason states what the WRONG value costs the FIGURE,
# never merely what the field is — a true sentence about the field is how three of these got
# classified `ignored` in the first place.
SCAN_FIXED_INPUTS: dict[str, tuple[object, str]] = {
    "arm": (
        "bare_scan",
        "the bench writes `bare_scan` from its TIMED branch and `bare_scan_setup_only` from"
        " `--setup-only`, which performs the corpus open and schema ingest and DELIBERATELY DOES"
        " NOT SCAN. Those two artifacts are written side by side in every rep (`<tag>.json` and"
        " `<tag>-setup.json`), so the setup-only record is the nearest wrong file on disk — and it"
        " is the SUBTRAHEND of this arm's cycles/row, i.e. reading it as the timed run would divide"
        " a setup cost by itself. This is the record's own statement of which branch produced it"
        " (#3272)",
    ),
    "surface": (
        "cqlite_core::Database::execute_streaming",
        "the record's own statement of WHICH PUBLIC SURFACE produced the measured rows, and the"
        " rig's entire claim is a comparison BETWEEN two named surfaces (spec R1). `collect_scan`"
        " emits this exact string into its measurement block as an UNCONDITIONAL LITERAL, so"
        " before this check the report ASSERTED the surface while the artifact's own statement of"
        " it went unread — a printed claim about a field nobody compared (#3272)",
    ),
    "query": (
        f"SELECT {SCAN_PROJECTION} FROM {CORPUS_TABLE_SUBPATH[0]}.{CORPUS_TABLE_SUBPATH[1]}",
        "the record's own statement of WHICH QUERY was measured, composed by the bench from"
        " `--project`, `--keyspace` and `--table`. A narrower projection reads FEWER COLUMNS off"
        " the same rows: the pass count is unchanged, every pass still observes exactly the pinned"
        " corpus row count, the aggregates still equal the derived sums — and the rows/s is a"
        " different measurement, reported as the denominator of a ratio against arm B's full-ring"
        " SELECT *. This is the request substitution of round 10's M1 and round 14's F2 on arm A"
        " (#3272)",
    ),
    "fold": (
        False,
        "`--fold` folds EVERY CELL of every row into a digest to prove materialization, and the"
        " bench's own documentation says it INFLATES THE NUMBER and that the reported figure must"
        " be the unfolded one. A folded run therefore measures the scan PLUS a per-cell hash over"
        " the whole corpus, and is slower for a reason that has nothing to do with the read path"
        " under study — which biases `bare/flight` DOWNWARD and makes the 1.3x target EASIER to"
        " hit, the same manufacture-a-win direction the bare-scan prewarm fails closed on"
        " (#3272)",
    ),
}

for _k, _spec in SCAN_FIXED_INPUTS.items():
    if len(_spec) != 2:
        raise Invalid(
            f"SCAN_FIXED_INPUTS[{_k!r}] has {len(_spec)} element(s); every entry must be"
            " (VALUE, WHY). A malformed entry would raise an unpacking error deep inside the"
            " checker at report time rather than being refused here, where the table is."
        )
    if not isinstance(_spec[1], str) or len(_spec[1].strip()) < 20:
        raise Invalid(
            f"SCAN_FIXED_INPUTS[{_k!r}] carries no substantive REASON ({_spec[1]!r}). The reason"
            " is the sentence the refusal ends with, and a refusal that names two differing values"
            " and nothing about what the difference costs the FIGURE is one an operator cannot act"
            " on — which is the diagnostic bar round 14's F1/F2 set for this class."
        )
del _k, _spec


def check_scan_fixed_inputs(tag: str, payload: dict) -> dict:
    """REQUIRE the bare-scan record's FIXED contract, BEFORE any counter is consumed (#3272).

    Ordered before the counters deliberately, the same order `collect_flight` uses: a record from a
    different workload must be refused FOR THAT, not for a downstream consequence of it — and the
    counters of a differently-produced scan are frequently VALID, which is the whole finding.

    Each field is REQUIRED PRESENT and compared EXACTLY. Absent is an ERROR rather than an assumed
    default, for the reason every check in this rig is: `payload.get(k, <the value we want>)` makes
    the comparison pass precisely when the artifact is SILENT about it.

    Compared with `==` after a TYPE check, never through a coercion. That matters concretely for
    `fold`, whose expectation is the boolean `False`: `not payload["fold"]` would accept `0`, `""`,
    `null` and an absent key alike, and `payload["fold"] == False` alone would accept `0` — so a
    record that says nothing coherent about folding would satisfy the unfolded contract.

    Returns what was verified, so the rep's record can state WHAT was checked rather than merely
    that something was.
    """
    verified: dict[str, object] = {}
    for key, (want, why) in SCAN_FIXED_INPUTS.items():
        if key not in payload:
            raise Invalid(
                f"bare-scan rep {tag} payload carries no `{key}`, so the value this rig FIXES it to"
                f" ({want!r}) was NOT OBSERVED and cannot be asserted. A missing input is an error,"
                " never an assumed default — defaulting it would make the check pass exactly when"
                f" the artifact is silent (#3272). {why}"
            )
        got = payload[key]
        # A bool is not an int and an int is not a bool: `True == 1` in python, so an `arm`-style
        # value check written loosely would let `fold: 0`/`fold: 1` stand in for the boolean the
        # bench actually writes.
        if isinstance(got, bool) != isinstance(want, bool) or type(got) is not type(want):
            raise Invalid(
                f"bare-scan rep {tag} recorded `{key}` = {got!r} ({type(got).__name__}), but"
                f" ws0-scan-bench writes it as {type(want).__name__}. Compared without a coercion"
                " deliberately: a coercion would let a value of the wrong kind satisfy the"
                f" contract. {why}"
            )
        if got != want:
            raise Invalid(
                f"bare-scan rep {tag} recorded `{key}` = {got!r}, but this rig requires {want!r}."
                " Its COUNTERS can be entirely valid — the right number of passes, every pass"
                " observing exactly the pinned corpus row count, the aggregates equal to the"
                " derived sums — while the work measured is materially different, so the counters"
                f" cannot detect this and did not. {why}. Re-run the rep with the driver, which"
                " fixes this input; do not report a scan produced under different conditions as"
                " this arm's result."
            )
        verified[key] = got
    return verified
