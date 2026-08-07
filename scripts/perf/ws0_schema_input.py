#!/usr/bin/env python3
"""THE SCHEMA AS A VERIFIED MEASUREMENT INPUT (#3272 review round 6, R2).

Split out of `ws0_session.py` under the campsite rule — that file reached the ~800-line source
target exactly, so this is a split by RESPONSIBILITY rather than a waiver. The seam:

    ws0_session.py        the CORPUS's identity — which DATA was measured?
    ws0_schema_input.py   the SCHEMA's identity — which SCHEMA was it read WITH?

# The finding

`ws0-events.cql` is a MEASUREMENT INPUT and it was outside every verification the rig performs.
It was absent from `verify_corpus_bytes` (Data.db only), from `verify_corpus_components` (the
SSTable components only — the DDL is not in the table directory), and from the pre-measurement
session pin. The driver's only check was `[[ -r "$DDL_FILE" ]]`: readable, nothing more.

**The two arms read it asymmetrically, which is what makes a modification silent AND harmful:**

* the BARE SCAN ingests the DDL on EVERY invocation, so it sees whatever the file says now;
* the FLIGHT TICKET is generated from it ONCE, at session setup.

So editing the file between setup and a later rep makes the two arms measure DIFFERENT SCHEMAS —
a different column set, a different clustering order, a different type — while every recorded
identity still agrees and the report exits 0. A head-to-head number between two arms reading two
schemas is not a comparison of anything.

# The mechanism

The digest is recorded by the generator into `corpus-identity.json` (`schema_sha256`, an
identity field like any other) and into the session pin, so it is:

  * verified BEFORE measurement (the driver stamps the pin from the identity), and
  * verified AT REPORTING against both the recorded identity and the bytes on disk.

The file is small (a few hundred bytes), so unlike `Data.db` there is no cost argument for ever
skipping its digest — and therefore no flag that can. `--skip-corpus-digest` exists because
hashing 2.8 GB per report is real; hashing 291 bytes is not, and a skip here could only buy a
vacuous green.

# The BACKWARD-COMPATIBILITY decision, stated rather than defaulted

A corpus generated before this field existed has no `schema_sha256`. Such an identity is
REFUSED, not silently accepted: an absent digest means the schema was never pinned, which is the
condition the finding is about, and treating "no record" as "nothing to check" is precisely the
fail-open shape (a check that does not run prints exactly like one that passed). The remedy is a
regeneration, and the refusal says so.

## Why this REFUSES while `--verify-against` reads a pre-pin identity (#3272 round 7, F1/F6)

The two look inconsistent and are not — they are asked different questions, so the same absence
means different things:

* HERE the question is "may this session's measurement be reported?" The schema is a LIVE
  MEASUREMENT INPUT that both arms are reading right now, asymmetrically, so an unpinned schema
  means the two arms may not have read the same one. That is unreportable, and refusing is the
  only answer that is not a claim about something unmeasured. The remedy is available and cheap:
  regenerate, which the current generator always records the digest for.
* `--verify-against` (`tools/ws0-corpus-gen/src/main.rs`) is asked "does this regeneration
  reproduce a RECORDED identity?" That record may legitimately predate any given field, and a
   2026-08-03 artifact will always predate a 2026-08-04 field, so a required-field read makes the
  determinism check permanently unrunnable against it. It therefore READS the absence — and
  reports it as a THIRD STATE (`PARTIAL`, exit non-zero, the field named as UNVERIFIED), never
  as agreement.

The shared rule is the one that matters: **neither path treats an absent digest as a match.** One
refuses to report, the other reports that it could not verify. What is forbidden in both is
silence.
"""

from __future__ import annotations

import pathlib

from ws0_session import sha256_file
from ws0_validate import Invalid, _SHA256_RE

# The DDL the generator emits beside the corpus. Both arms resolve exactly this path, so the
# name is not configurable here either — a schema at a different path is a different corpus.
SCHEMA_FILENAME = "ws0-events.cql"


def schema_path(corpus: pathlib.Path) -> pathlib.Path:
    return corpus / SCHEMA_FILENAME


def recorded_schema_digest(identity: dict, corpus: pathlib.Path) -> str:
    """The `schema_sha256` the recorded identity carries, or `Invalid`.

    ABSENT IS A REFUSAL, not a skip (see the module docstring): an identity with no schema
    digest never pinned the schema, so the file both arms read is unverified — which is the
    finding, not an exemption from it.
    """
    value = identity.get("schema_sha256")
    if value is None:
        raise Invalid(
            f"{corpus / 'corpus-identity.json'} records no `schema_sha256`, so the SCHEMA both"
            f" measurement arms read ({SCHEMA_FILENAME}) is not pinned by anything. The two arms"
            " read it ASYMMETRICALLY — the bare scan ingests it on every invocation, the Flight"
            " ticket is generated from it once — so a modification between them makes the arms"
            " measure DIFFERENT SCHEMAS while every other recorded identity still agrees and the"
            " report exits 0 (#3272 R2). This corpus predates the schema pin: regenerate it with"
            " tools/ws0-corpus-gen, which records the digest of the DDL it emits."
        )
    if not isinstance(value, str) or not _SHA256_RE.match(value):
        raise Invalid(
            f"{corpus / 'corpus-identity.json'}: `schema_sha256` is {value!r}, which is not 64"
            " lowercase hex characters. A truncated or malformed digest cannot identify the"
            f" schema the measurement was taken with."
        )
    return value


def verify_schema_input(corpus: pathlib.Path, identity: dict) -> dict:
    """Verify `ws0-events.cql` against the recorded identity AND the bytes on disk.

    Returns the verification RECORD, so the report carries the observation rather than a bare
    boolean — and so a reader can tell a digest that was DERIVED from one that was merely
    recorded.

    There is deliberately NO `skip_digest` parameter. The file is a few hundred bytes; the cost
    argument that justifies `--skip-corpus-digest` for a 2.8 GB `Data.db` does not exist here, so
    a skip could only ever buy a vacuous green.
    """
    recorded = recorded_schema_digest(identity, corpus)
    path = schema_path(corpus)
    if not path.is_file():
        raise Invalid(
            f"{path} is MISSING, but the recorded identity pins its digest ({recorded}). Both"
            " measurement arms read this file — the bare scan ingests it, the Flight ticket is"
            " generated from it — so a corpus without it cannot be measured at all. Regenerate"
            " the corpus."
        )
    measured = sha256_file(path)
    if measured != recorded:
        raise Invalid(
            f"THE SCHEMA CHANGED. {path} hashes to {measured} but the recorded identity pins"
            f" {recorded}. This file is a MEASUREMENT INPUT read by BOTH arms, and they read it"
            " asymmetrically: the bare scan ingests it on EVERY invocation while the Flight"
            " ticket was generated from it ONCE at setup. So a modification makes the two arms"
            " measure DIFFERENT SCHEMAS — a different column set, clustering order or type —"
            " and a head-to-head number between two arms reading two schemas compares nothing."
            " Nothing else in the recorded identity can see this, because the schema was outside"
            " both corpus verification and the session pin (#3272 R2). Restore the corpus's own"
            " DDL, or regenerate the corpus and re-measure."
        )
    return {
        "schema": str(path),
        "schema_bytes": path.stat().st_size,
        "schema_sha256_recorded": recorded,
        "schema_sha256_measured": measured,
        "sha256_verified": True,
        "note": (
            f"{SCHEMA_FILENAME} is a MEASUREMENT INPUT read by both arms (the bare scan ingests"
            " it per invocation; the Flight ticket is generated from it once), so its digest is"
            " re-derived from disk on every report — always, with no skip flag: the file is a few"
            " hundred bytes, so an opt-out could only buy a vacuous green"
        ),
    }


def verify_pinned_schema(
    pin_path: pathlib.Path, pin: dict, corpus: pathlib.Path, identity: dict
) -> dict:
    """Compare the PINNED schema digest against the report-time identity AND the bytes on disk.

    The pre-measurement half of R2, and the same argument as B2's component comparison: verifying
    the schema against the corpus's OWN report-time `corpus-identity.json` cannot see a schema
    replaced mid-session with the identity refreshed beside it, because that state is
    self-consistent at report time. The pin is the identity captured BEFORE, compared AFTER.

    Written by `write_session_corpus_pin` and READ HERE — the round-6 lesson (B2) being that a
    recorded field with no reader is not a guard.
    """
    pinned = pin.get("schema_sha256")
    if pinned is None:
        raise Invalid(
            f"{pin_path} records no `schema_sha256`, so the SCHEMA this session measured with is"
            " not pinned. A schema replaced mid-session — with corpus-identity.json refreshed"
            " beside it — is self-consistent at report time and therefore invisible to the"
            " report-time schema check (#3272 R2). Re-run the session with the current driver,"
            " which pins the schema digest before the first rep."
        )
    if not isinstance(pinned, str) or not _SHA256_RE.match(pinned):
        raise Invalid(
            f"{pin_path}: `schema_sha256` is {pinned!r}, which is not 64 lowercase hex"
            " characters — a truncated pin cannot identify the schema this session measured with"
        )
    current = recorded_schema_digest(identity, corpus)
    if pinned != current:
        raise Invalid(
            f"THE SCHEMA CHANGED under this session. The session was started against a schema"
            f" whose sha256 is {pinned} (stamped before the first rep), but the corpus now"
            f" records {current}. The bare scan re-reads the DDL on every invocation while the"
            " Flight ticket was generated from the schema present at setup, so the two arms did"
            " not necessarily measure the same schema. Re-run the session."
        )
    path = schema_path(corpus)
    try:
        measured = sha256_file(path)
    except OSError as exc:
        raise Invalid(
            f"the pinned schema {path} cannot be read: {exc}. The pin names it as part of the"
            " measured corpus, so an unreadable schema means the session cannot be reported."
        ) from None
    if measured != pinned:
        raise Invalid(
            f"the schema {path} hashes to {measured} ON DISK but the session pinned {pinned}."
            " The recorded identity agreed with the pin, so this is an identity file that was"
            " edited rather than regenerated — the pin is compared against BOTH the identity and"
            " the bytes for exactly this case."
        )
    return {
        "pinned_schema_sha256": pinned,
        "note": (
            "the schema digest was captured in the session dir BEFORE the first rep and"
            " re-compared here against both the report-time identity and the bytes on disk"
        ),
    }
