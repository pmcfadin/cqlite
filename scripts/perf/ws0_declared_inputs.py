#!/usr/bin/env python3
"""WHICH INPUTS A BOUNDARY CHECK MUST COVER — derived from the session, never enumerated
(#3272 review round 24).

Split out of `ws0_corpus_bytes.py` under the campsite rule: that file crossed the ~800-line source
target when this layer landed in it, and the `file-size` ratchet is `.rs`-ONLY, so a Python file
crosses SILENTLY — the size is a verdict its author owes, not a datum to pass on. This is a split by
RESPONSIBILITY rather than a waiver, and it extends the seam the four sibling modules already draw:

    ws0_session.py         the SESSION's identity   — which corpus, which configuration?
    ws0_corpus_bytes.py    the BYTES               — hash them, and compare them to the pin
    ws0_declared_inputs.py the SCOPE               — WHICH inputs must that comparison cover?
    ws0_pin_components.py  the PINNED COMPONENT SET — are all the corpus's parts the pinned ones?
    ws0_schema_input.py    the SCHEMA's identity    — which schema was it read WITH?
    ws0_ticket_input.py    the TICKET's identity    — which request was measured?

# The finding this module IS

`verify_corpus_boundary` re-hashed the SSTable components and nothing else, while TWO other files are
re-read DURING measurement: `ws0-events.cql` (the bare scan ingests it on every invocation) and
`ticket-template.json` (`flight-loadgen --ticket-template` re-reads it on every invocation of every
rep of every arm). So the mutate-then-restore sequence the boundary check exists to catch STILL
WORKED, aimed at the schema or the request instead of a component: both ends of the session see the
original bytes, every report-time check agrees, and the boundary published `7 of 7 components
verified` — a count complete RELATIVE TO ITS OWN TOO-SMALL LIST.

A guard that verifies 7 of 9 inputs and reports success is issuing a verdict about 2 inputs it never
looked at. Per this branch's rule, a positive verdict requires an AFFIRMATIVE MEASUREMENT, and the
omission biases TOWARD the claim — a session whose schema or request changed mid-run reports as
verified — so it is refused, not captioned.

# WHY THE SET IS DERIVED, which is the whole reason this module is separate

The fix is not "add two more files to the list". A HAND-MAINTAINED ENUMERATION IS HOW THOSE TWO CAME
TO BE OMITTED, and this branch has already replaced two such enumerations with derivations. So the
covered set is read OFF THE SESSION'S OWN PIN — every field that declares a digest — and a declared
field this module cannot resolve REFUSES the rep and NAMES it. That is the property that stops the
finding recurring at a fourth scope: a per-invocation input added later with nothing covering it
FAILS, rather than being silently absent from a complete-looking count.

`COVERAGE` below is therefore not the covered set. It is how a DERIVED field is resolved to bytes,
and its incompleteness is loud by construction.
"""

from __future__ import annotations

import pathlib

from ws0_validate import Invalid, _SHA256_RE


# A PIN FIELD THAT DECLARES AN INPUT'S DIGEST is spelled `<something>_sha256`. The covered set is
# DERIVED by this suffix rather than enumerated: `components_source`/`config`/`note`/`corpus` do not
# end in it, so the derivation picks out exactly the digest declarations and nothing else.
DIGEST_FIELD_SUFFIX = "_sha256"

# ...plus the COMPONENT MAP, which declares many inputs under one key.
COMPONENT_MAP_FIELD = "components"

# The pin field whose digest DUPLICATES a component-map entry (the top-level `Data.db` identity).
DATA_DB_FIELD = "data_db_sha256"


def declared_inputs(pin: dict) -> list[str]:
    """The pin fields that DECLARE a measurement input's identity — read OFF THE PIN.

    This is the derivation the finding turns on. A boundary check over a hand-written list reports a
    count that is complete RELATIVE TO ITS OWN LIST (measured: `7 of 7 components verified` while a
    live mutation of `ws0-events.cql` went unmentioned), so the set is taken from the session's own
    declaration instead. Every returned field must resolve through `COVERAGE`, and one that does not
    REFUSES the rep — which is what makes a per-invocation input added later with nothing covering it
    a failure rather than a silent third scope for the same defect.
    """
    fields = [k for k in pin if isinstance(k, str) and k.endswith(DIGEST_FIELD_SUFFIX)]
    if COMPONENT_MAP_FIELD in pin:
        fields.append(COMPONENT_MAP_FIELD)
    return sorted(fields)


def coverage() -> dict:
    """How each DERIVED field is resolved to bytes on disk, and WHY it needs covering.

    NOT the covered set — see the module docstring. This is the resolution table `declared_inputs`'s
    output is looked up in, and a field missing from it is a REFUSAL rather than an omission.

    The filenames come from the modules that OWN each input — one spelling, so the boundary cannot
    hash a different path from the one the report-time check verifies. Imported function-locally
    because both of those modules import `sha256_file` from `ws0_session`, which re-exports it from
    `ws0_corpus_bytes`; a module-scope import would be a cycle. (The same reason `ws0_session` states
    for its own function-local imports.)

    `root` names WHICH directory the path is relative to, and the two differ by OWNERSHIP: the schema
    belongs to the shared corpus, while the ticket lives in the session's exclusively-claimed output
    directory (#3272 round 13, F2).
    """
    from ws0_schema_input import SCHEMA_FILENAME
    from ws0_session import PIN_TICKET_FIELD
    from ws0_ticket_input import TICKET_FILENAME

    return {
        COMPONENT_MAP_FIELD: {
            "kind": "component-set",
            "why": "the SSTable components a scan reads, and those that shape how it reads",
        },
        DATA_DB_FIELD: {
            "kind": "declared-component",
            "why": "the pin's TOP-LEVEL Data.db digest, a second declaration of a component's"
                   " identity that can disagree with the component map it duplicates",
        },
        "schema_sha256": {
            "kind": "file",
            "root": "corpus",
            "filename": SCHEMA_FILENAME,
            "why": "the DDL. The bare scan INGESTS IT ON EVERY INVOCATION while the Flight"
                   " ticket was generated from it once, so a change mid-run makes the two arms"
                   " measure DIFFERENT SCHEMAS (#3272 R2)",
        },
        PIN_TICKET_FIELD: {
            "kind": "file",
            "root": "session",
            "filename": TICKET_FILENAME,
            "why": "the REQUEST — keyspace, table, DDL, token range, projection, predicates,"
                   " aggregation, limit. `flight-loadgen --ticket-template` RE-READS IT ON EVERY"
                   " INVOCATION of every rep of every arm (#3272 M1)",
        },
    }


def pinned_digest(p: pathlib.Path, pin: dict, field: str) -> str:
    """The pin's digest for `field`, or `Invalid`. An unusable declaration is never a match."""
    value = pin.get(field)
    if not isinstance(value, str) or not _SHA256_RE.match(value):
        raise Invalid(
            f"{p}: `{field}` is {value!r}, which is not 64 lowercase hex characters. This field"
            " DECLARES a measurement input, so a truncated or malformed digest cannot identify the"
            " bytes the reps read — and it is refused rather than skipped: an unverifiable"
            " declaration is not an absent one (#3272 round 24)."
        )
    return value


def _verify_declared_file(
    p: pathlib.Path, pin: dict, field: str, path: pathlib.Path, why: str, label: str
) -> dict:
    """Re-hash ONE declared single-file input from disk against the pin, or refuse the rep.

    FAILS CLOSED on an absent or unhashable file (`sha256_file` raises, and a missing one is named
    here): "assume unchanged" is the vacuous pass the whole check exists to remove.
    """
    from ws0_corpus_bytes import SESSION_CORPUS_PIN, sha256_file

    pinned = pinned_digest(p, pin, field)
    if not path.is_file():
        raise Invalid(
            f"THE PINNED MEASUREMENT INPUT {path} IS ABSENT at boundary {label}, but this session"
            f" pinned its digest ({pinned}) before the first rep. It is {why}. A rep cannot be"
            " verified over an input that is not there, and it is NOT assumed unchanged."
        )
    disk = sha256_file(path)
    if disk != pinned:
        raise Invalid(
            f"A MEASUREMENT INPUT CHANGED DURING MEASUREMENT: {path} hashes to {disk} at boundary"
            f" {label}, but {SESSION_CORPUS_PIN} pinned {pinned} (field `{field}`) before the first"
            f" rep. This file is {why}. Reps on either side of this boundary read DIFFERENT BYTES,"
            " and restoring the file before the report would leave every report-time check in"
            " agreement — which is why it is checked HERE, inside the run. Until round 24 the"
            " boundary check covered only the SSTable components, so this input was one the check"
            " reported success about without ever looking at it (#3272 round 24). This session"
            " cannot be reported."
        )
    return {"input": field, "path": str(path), "sha256": disk, "kind": "file"}


def _verify_declared_data_db(p: pathlib.Path, pin: dict, pinned: dict, label: str) -> dict:
    """The pin's TOP-LEVEL `data_db_sha256`, covered AFFIRMATIVELY by the component map.

    Not an exemption. The component map has just been re-hashed from disk at this boundary, so
    asserting that this second declaration EQUALS the re-hashed entry establishes that it describes
    the bytes that were observed — rather than leaving it as a field the boundary skipped because
    something else looked similar.
    """
    declared = pinned_digest(p, pin, DATA_DB_FIELD)
    names = sorted(n for n in pinned if n.endswith("-Data.db"))
    if len(names) != 1:
        raise Invalid(
            f"{p}: the pinned component map names {len(names)} *-Data.db entries"
            f" ({', '.join(names) or 'none'}), so the pin's top-level `{DATA_DB_FIELD}` cannot be"
            f" tied to a component that was re-hashed at boundary {label}. A declaration nothing"
            " covers is refused, never assumed."
        )
    observed = pinned[names[0]].get("sha256")
    if declared != observed:
        raise Invalid(
            f"{p}: the pin declares {DATA_DB_FIELD} {declared} but its component map records"
            f" {observed!r} for {names[0]}, which is the entry re-hashed FROM DISK at boundary"
            f" {label}. The pin contradicts itself about the bytes that were measured, so which"
            " digest the reps read cannot be established."
        )
    return {
        "input": DATA_DB_FIELD,
        "path": names[0],
        "sha256": observed,
        "kind": "declared-component",
    }


def verify_declared_inputs(
    p: pathlib.Path,
    pin: dict,
    pinned: dict,
    session_dir: pathlib.Path,
    corpus: pathlib.Path,
    table_dir: pathlib.Path,
    label: str,
    components_checked: int,
) -> list[dict]:
    """Verify EVERY input the pin declares at one boundary; refuse on any that is uncovered.

    `pinned` is the pin's component map AS ALREADY RE-HASHED FROM DISK by the caller, and
    `components_checked` how many of them were — this function does not re-walk them, it covers the
    OTHER declarations and ties the duplicate `data_db_sha256` to what was observed.

    The uncovered-field refusal is the anti-recurrence property, and it is deliberately raised BEFORE
    any single-file input is resolved: a run must not report a partial verification as a complete one
    even in the diagnostic.

    Returns one record per declared input (its name, the path, the digest observed), so the caller's
    boundary observation can name WHICH inputs were covered rather than only counting components — a
    bare count being complete only about whatever list produced it, which is this finding.
    """
    table = coverage()
    declared = declared_inputs(pin)
    uncovered = [f for f in declared if f not in table]
    if uncovered:
        raise Invalid(
            f"{p} DECLARES measurement input(s) this boundary check does not cover:"
            f" {', '.join(uncovered)}. A guard that verifies some of the inputs and reports success"
            " is issuing a verdict about the ones it never looked at — MEASURED, that shape"
            " published `7 of 7 components verified` for a session whose schema was live-mutated"
            " (#3272 round 24). The covered set is DERIVED from the pin precisely so a NEW"
            " per-invocation input fails HERE instead of being silently omitted the way"
            " schema_sha256 and ticket_template_sha256 were. Add its resolution to `coverage()` in"
            " scripts/perf/ws0_declared_inputs.py."
        )
    verified: list[dict] = []
    for field in declared:
        spec = table[field]
        kind = spec["kind"]
        if kind == "component-set":
            verified.append({
                "input": field,
                "path": str(table_dir),
                "components": components_checked,
                "kind": kind,
            })
        elif kind == "declared-component":
            verified.append(_verify_declared_data_db(p, pin, pinned, label))
        else:
            root = session_dir if spec["root"] == "session" else corpus
            verified.append(
                _verify_declared_file(
                    p, pin, field, root / spec["filename"], spec["why"], label
                )
            )
    return verified
