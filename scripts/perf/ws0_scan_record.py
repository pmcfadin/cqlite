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

import pathlib

from ws0_schema_input import SCHEMA_FILENAME
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


# ============================================================================
# THE SESSION-BOUND SCAN INPUTS (#3272)
# ============================================================================
# `SCAN_FIXED_INPUTS` above can only verify a field whose correct value is a CONSTANT. The remaining
# three are just as measurement-determining and have no constant correct value — what each must
# equal depends on WHICH CORPUS this session pinned — so they take the shape round 14's F1 built for
# exactly that, `(SOURCE, WHY, CONSEQUENCE)`, with the consequence PER FIELD because the three lose
# different things: other bytes, other DDL, other SSTables.
#
# The expectations come from the SESSION's pinned corpus, never from a reporter argument: the same
# provenance rule `endpoint` follows. A value that cannot be supplied cannot disagree, so a scan
# performed over a different corpus cannot be excused by re-reporting with a matching flag.
SCAN_SESSION_BOUND_INPUTS: dict[str, tuple[str, str, str]] = {
    "corpus": (
        "the corpus this session PINNED before the first rep (`session-corpus-pin.json`)",
        "the record's own statement of WHICH BYTES this arm scanned. Every corpus check in the rig"
        " is about the corpus the REPORTER was pointed at — the pin, the component map, the schema"
        " digest, the pinned row count every pass is required to observe — and NONE of them reads"
        " what the BENCH says it opened. So a rep run against a SECOND corpus on the same box (a"
        " peer lane, a regenerated copy, a smoke corpus) satisfies every one of them whenever the"
        " two share a row count, and is reported as a measurement over the pinned bytes. That is"
        " round 13 F3's corpus substitution reaching arm A through the one field that could have"
        " named it (#3272)",
        "THE ROWS WERE READ FROM A DIFFERENT CORPUS than the one this session pinned, so every"
        " byte-level identity this report prints — the Data.db digest, the component map, the"
        " schema — describes bytes this arm never opened. Point the bench at the pinned corpus and"
        " re-run; do not report a scan of other bytes.",
    ),
    "schema": (
        f"`{SCHEMA_FILENAME}` beside the pinned corpus",
        "the record's own statement of WHICH DDL the bare scan ingested. The schema decides how the"
        " bytes are INTERPRETED — the column set, the types, the clustering — and the two arms read"
        " it ASYMMETRICALLY (this arm ingests it on every invocation; the Flight ticket is generated"
        " from it once at setup), which is why round 6's R2 pinned its digest. That pin verifies the"
        " file AT THE PINNED PATH; it cannot see this arm having ingested a DIFFERENT FILE, because"
        " nothing compared the path the bench recorded (#3272)",
        "THIS ARM INGESTED A DIFFERENT SCHEMA FILE than the one pinned and digested, so the pinned"
        " digest verifies a file this scan did not read — and the Flight arm generated its ticket"
        " from the pinned one, which makes the two arms answer questions asked of different DDL."
        " Re-run against the pinned schema; do not report a cross-arm ratio over two schemas.",
    ),
    "table_dirs_ingested": (
        "the ONE table directory under the pinned corpus"
        f" (`{'/'.join(CORPUS_TABLE_SUBPATH)}`), which the bench recorded selecting",
        "the EXACT directory set ingestion selected, recorded by round 10's F-B after a substring"
        " filter silently absorbed a name-extending sibling and made the two arms measure different"
        " SSTable sets. F-B closed the SELECTION (`TableDirSelection::Exact`) and had the bench"
        " RECORD what it observed — and then nothing read the record, so the fix was verified only"
        " inside the process that could not report its own failure. Reading it here is what makes"
        " that recorded observation evidence: an extra directory also changes the GENERATION COUNT,"
        " and the generation count selects the scan route (#3272)",
        "THIS ARM INGESTED A DIFFERENT SET OF TABLE DIRECTORIES than the single pinned one, so the"
        " rows measured here did not come from the same SSTables the Flight arm served — and the"
        " cross-arm ratio, which is this rig's only output, compares two corpora. Re-run against the"
        " pinned corpus; do not report a ratio across different SSTable sets.",
    ),
}

for _k, _spec in SCAN_SESSION_BOUND_INPUTS.items():
    if len(_spec) != 3:
        raise Invalid(
            f"SCAN_SESSION_BOUND_INPUTS[{_k!r}] has {len(_spec)} element(s); every entry must be"
            " (SOURCE, WHY, CONSEQUENCE). A two-element entry would raise an unpacking error deep"
            " inside the checker at report time rather than being refused here, and one with an"
            " EMPTY consequence would unpack cleanly and produce a refusal naming two differing"
            " values and nothing about what the difference costs."
        )
    for _pos, _label in enumerate(("SOURCE", "WHY", "CONSEQUENCE")):
        if not isinstance(_spec[_pos], str) or len(_spec[_pos].strip()) < 20:
            raise Invalid(
                f"SCAN_SESSION_BOUND_INPUTS[{_k!r}]'s {_label} is not a substantive sentence"
                f" ({_spec[_pos]!r}). The three members lose DIFFERENT things — other bytes, other"
                " DDL, other SSTables — so one shared sentence could only say two values differ,"
                " which an operator cannot act on."
            )
del _k, _spec, _pos, _label


def scan_session_bound_expectations(pinned_corpus: pathlib.Path) -> dict[str, object]:
    """What each SESSION-BOUND field of a bare-scan record MUST equal (#3272).

    The sibling of `ws0_flight_arm.session_bound_expectations`, built from the corpus the SESSION
    PINNED rather than from a caller's preference, and completeness is asserted in BOTH directions:
    a field classified as verified with nothing supplying its expected value would be verified by
    nothing while the census says otherwise, and an expectation built for a field the table does not
    carry would be silently DROPPED by a checker that loops over the TABLE.

    The three values are composed exactly as `scan_bench.rs` composes them, so the comparison moves
    when the layout does: `corpus` is the root the bench was handed, `schema` is
    `<corpus>/<SCHEMA_FILENAME>` (its `--schema` default), and `table_dirs_ingested` is the single
    `<corpus>/<ks>/<table>` that `verify_exact_scope` required the selection to equal.
    """
    if not isinstance(pinned_corpus, pathlib.Path) or not str(pinned_corpus).strip():
        raise Invalid(
            "internal: no pinned corpus was supplied for the bare-scan session-bound checks (got"
            f" {pinned_corpus!r}). An empty expectation would compare every record against nothing"
            " while this module says these fields are verified — the half-wired guard #3272 keeps"
            " finding."
        )
    expected: dict[str, object] = {
        "corpus": str(pinned_corpus),
        "schema": str(pinned_corpus / SCHEMA_FILENAME),
        "table_dirs_ingested": [str(pinned_corpus.joinpath(*CORPUS_TABLE_SUBPATH))],
    }
    absent = [k for k in SCAN_SESSION_BOUND_INPUTS if k not in expected]
    if absent:
        raise Invalid(
            "internal: no expectation is built for the session-bound scan field(s)"
            f" {', '.join(sorted(absent))}. Each is classified as VERIFIED against the session, so"
            " leaving it without an expected value would mean this module claims a check that"
            " nothing performs (#3272)."
        )
    extra = [k for k in expected if k not in SCAN_SESSION_BOUND_INPUTS]
    if extra:
        raise Invalid(
            f"internal: an expectation is built for {', '.join(sorted(extra))}, which"
            " SCAN_SESSION_BOUND_INPUTS does not classify as session-bound. The checker loops over"
            " the TABLE, so this value would be silently dropped — a field wired here while nothing"
            " verifies it (#3272)."
        )
    return expected


def check_scan_session_bound_inputs(tag: str, payload: dict, expected: dict) -> dict:
    """REQUIRE the bare-scan record's SESSION-BOUND fields to match the PINNED corpus (#3272).

    Compared EXACTLY, never by a substring/prefix/suffix test — the rule `endpoint` follows, and it
    matters for the same reason: a prefix comparison would call `/data/ws0-corpus` and
    `/data/ws0-corpus-2` the same corpus, which is the peer-lane case this closes, and it would be
    the SUBSTRING defect of round 10's F-B reappearing inside the checker built to detect it.

    `table_dirs_ingested` is a LIST and is compared as one, element by element in order: the bench
    writes the selection `verify_exact_scope` required to be exactly the intended directory, so the
    correct value is a one-element list. Compared without flattening or set-coercion, because a
    coercion is how a SUPERSET becomes indistinguishable from the intended set.
    """
    verified: dict[str, object] = {}
    for key, (source, why, consequence) in SCAN_SESSION_BOUND_INPUTS.items():
        if key not in expected:
            raise Invalid(
                f"internal: no expected value was supplied for the session-bound scan field `{key}`"
                f" (rep {tag}), whose correct value comes from {source}. A session-bound field with"
                " no expectation would be verified by nothing while this module says it is"
                " verified. Supply it in scan_session_bound_expectations."
            )
        want = expected[key]
        if key not in payload:
            raise Invalid(
                f"bare-scan rep {tag} payload carries no `{key}`, so {source} could not be compared"
                " against it and this record cannot be attributed to this session's corpus at all."
                f" A missing field is an error, never an assumed default (#3272). {why}"
            )
        got = payload[key]
        if isinstance(got, bool) or type(got) is not type(want):
            raise Invalid(
                f"bare-scan rep {tag} recorded `{key}` = {got!r} ({type(got).__name__}), but"
                f" ws0-scan-bench writes it as {type(want).__name__}. Compared without a coercion"
                " deliberately: a coercion is how a value of the wrong shape satisfies the"
                f" contract. {why}"
            )
        if got != want:
            raise Invalid(
                f"bare-scan rep {tag} recorded `{key}` = {got!r}, but for this session {source} is"
                f" {want!r}. Its COUNTERS can be entirely valid — the right number of passes, every"
                " pass observing exactly the pinned corpus row count — so the counters cannot"
                f" detect this and did not. {why}. {consequence}"
            )
        verified[key] = got
    return verified


# WHICH SCAN DISPOSITIONS A CHECKER ACTUALLY COMPARES — asserted at import, so a field cannot be
# classified as verified by a table no function reads. The same closure `ws0_loadgen_record` applies
# to its two tables, for the reason round 12's F2 gave: the freeze happened and the check on it was
# nominal.
_SCAN_EXPECTATION_TABLES: dict[str, dict] = {
    "verified-fixed-input": SCAN_FIXED_INPUTS,
    "session-bound": SCAN_SESSION_BOUND_INPUTS,
}
_SCAN_CHECKED_DISPOSITIONS = {
    "verified-fixed-input": check_scan_fixed_inputs,
    "session-bound": check_scan_session_bound_inputs,
}
for _disp, _table in _SCAN_EXPECTATION_TABLES.items():
    if not _table:
        raise Invalid(
            f"the scan {_disp!r} table is EMPTY, so that disposition verifies nothing while this"
            " module claims it does — a check with no subject prints exactly like a passing one"
        )
    if _disp not in _SCAN_CHECKED_DISPOSITIONS:
        raise Invalid(
            f"the scan disposition {_disp!r} has an expectation table but no checker function reads"
            " it, so a field in it would be verified by nothing (#3272)"
        )
for _disp in _SCAN_CHECKED_DISPOSITIONS:
    if _disp not in _SCAN_EXPECTATION_TABLES:
        raise Invalid(
            f"the scan disposition {_disp!r} has a checker but no expectation table, so the checker"
            " would compare against nothing"
        )
# ...and NO FIELD MAY BE CLASSIFIED TWICE. A key in both tables would be compared against a constant
# AND against the session; whichever comparison ran second would decide, while the other read as a
# second layer of defence that in fact contributes nothing.
_both = set(SCAN_FIXED_INPUTS) & set(SCAN_SESSION_BOUND_INPUTS)
if _both:
    raise Invalid(
        f"the scan field(s) {', '.join(sorted(_both))} are classified BOTH fixed and session-bound."
        " A field has ONE correct-value source; two would mean two expectations for one fact, and a"
        " report cannot say which one it verified."
    )
del _disp, _table, _both
