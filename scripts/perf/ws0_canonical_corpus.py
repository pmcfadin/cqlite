#!/usr/bin/env python3
"""IS THIS CORPUS THE CANONICAL MEASUREMENT CORPUS? (#3272 round 13, F3)

# The finding

`ws0-baseline.sh`'s pre-measurement pin snapshots the identity of the corpus it was HANDED and
compares it against nothing. Every downstream check is then a self-consistency check about that
corpus: the pin matches the report-time identity, the components match the pin, the schema matches
its recorded digest. All of it is true of a corpus generated with smoke-test row counts, a
different seed, or any other noncanonical parameter — so such a corpus passed the driver AND the
reporter as a WS0 BASELINE, and the printed report said nothing to distinguish it.

The identity being verified was never compared to the identity the baseline is DEFINED as.

# The bridge, and why it is a source PARSE rather than a build

The four canonical constants live in `tools/ws0-corpus-gen/src/measurement_corpus.rs` — in RUST,
and (measured before this module existed) `grep -rl 'measurement_corpus\\|DATA_DB_SHA256\\|
SCHEMA_SHA256' scripts/` returned ZERO hits. So this is a cross-language bridge, not a wiring job,
and the only question is which direction the values travel.

The precedent is `tools/ws0-corpus-gen/src/bin/verify_commands.rs` (round 10's L1), built for
exactly this shape: a Rust binary that EMITS values derived from the in-tree pins for a shell
consumer, chosen so a re-pin cannot leave a hand-copied value stale. Its PRINCIPLE is followed
here; its MECHANISM is not, and the reason is a hard constraint rather than a preference: this
check runs inside a gate component and inside hermetic self-tests, neither of which may run
`cargo build` (minutes, a network-fed registry, and a build product a self-test then depends on).
`cargo run --bin ws0-verify-commands` is an operator command; this is not.

The two mechanisms that do not need a build are a COMMITTED GENERATED ARTIFACT with a drift
assert, and a PARSE OF THE RUST SOURCE. This module parses the source, and the reason is this
issue's own subject: a generated artifact is a SECOND COPY of every value, and its correctness
rests on a drift assert somebody has to keep running. A parse has no second copy at all — change
`ROWS` in Rust and this module reads the new value in the same commit, with nothing to regenerate
and nothing to fall stale. What a parse can do that a copy cannot is FAIL TO FIND a constant, so
that is the failure mode it is built for: every required constant is mandatory, and an absent,
renamed or unparseable one raises `Invalid` naming the constant and the file. It never falls back
to a default, and there is deliberately no way to skip a constant it cannot read — a bridge that
degrades to "assume canonical" would be the vacuous pass this whole issue exists to remove.

The grammar it parses is `pub const NAME: TYPE = LITERAL;` over integer, float and string
literals — the whole of what these constants are. It is not a Rust parser and does not pretend to
be: anything it does not recognise for a REQUIRED constant is an error.

# A SMOKE CORPUS MUST STILL RUN

Refusing every small corpus would break the rig's own smoke path, and this issue has broken three
documented operator commands in exactly that way (rounds 9 and 10 — a fix that made a command
unable to succeed, which teaches an operator to stop running it and loses the whole check). So a
noncanonical corpus is not forbidden: it is refused AS A BASELINE and admitted under an EXPLICIT
non-baseline mode, `--non-baseline`, whose output is LABELLED IN WORDS in both the session manifest
and the printed report. The label is the point — a smoke run must be unmistakable as one to
somebody reading the report, not merely absent from a field they would have to know to check.

# ROUND 14, F4 — THE CLASSIFICATION TRUSTED A *TOTAL*, AND READ 8 OF 15 FIELDS

The comparison above shipped comparing nine SCALARS, one of which was `total_component_bytes`, and
this module's own docs recorded the gap as a known weakness: "the canonical component NAME SET is
not pinned in Rust, so it is covered in aggregate by `total_component_bytes` rather than
name-by-name". A sum is not a set. Two components can be resized in opposite directions, or
replaced with same-length different bytes, or one dropped and another grown, and the aggregate is
unmoved — so a corpus whose `Index.db` and `Statistics.db` are not the canonical ones was
classified CANONICAL and published as a WS0 BASELINE.

## THE CITED FIELD WAS ONE OF SEVEN, AND TWO OF THE OTHERS WERE THE INPUT ANCHORS

The component map is the field the review named. It is not the extent of the defect, and the
extent is what this fix is built against. MEASURED — the classifier's field set intersected with
the committed canonical artifact's 15 keys:

    READS (8)      bytes_per_row cells_per_row data_db_bytes data_db_sha256 partitions
                   rows rows_per_partition total_component_bytes
    NEVER READ (7) components compression_info_present differs_from_prior_corpus issue
                   not_a_correctness_oracle seed table

`seed` and `table` are the INPUT ANCHORS, and their absence is the same defect the RUST side of
this pin already found and fixed under review B2 — reintroduced here in another language.
`measurement_corpus_pin.rs` records what that costs, measured: changing
`generate::DEFAULT_SEED` from `30_960_001` to `99_999_999` left ALL 47 Rust tests GREEN, because
the determinism tests use the constant symmetrically. A digest pin is only a pin together with the
inputs that determine it — so a corpus generated at a DIFFERENT SEED, or of a DIFFERENT TABLE, was
classified CANONICAL by this module. `compression_info_present` is worse in kind than the total:
issue #1406 is the claim boundary that CQLite's write surface emits UNCOMPRESSED SSTables, the
artifact records `false` to assert it, and the classifier never looked — so a COMPRESSED corpus was
classifiable as the canonical one.

Fixing the component map alone would have left five of those uncompared and the class open. So the
fix is not a comparison bolted on beside a total: it is a CENSUS of the canonical identity's
fields (`CANONICAL_CENSUS` + `NOT_IDENTITY`), closed BOTH WAYS — at import, that every disposition
names a constant the bridge parses and a kind the comparison handles; at classify time, that every
key the canonical artifact carries HAS a disposition, fail-closed on one that does not. A field
cannot be silently outside the comparison any more, which is what makes the next instance
unrepresentable rather than fixed. This is F3's `BINARY_SPEC_DISPOSITION` shape one level out.

There is deliberately NO non-verifying disposition — no "present but not compared", which is what
F1 deleted `required-present` for. A field is either COMPARED against something (`pin`,
`composed`, `literal`, `map`) or recorded in `NOT_IDENTITY` as CARRYING NO IDENTITY CLAIM with a
reason that must name its own field, which is a decision about the field rather than a check that
reads as coverage while performing none.

The mitigation the docs cited was CIRCULAR, and that is the whole of the finding.
`ws0_pin_components.verify_pinned_components` does compare every component name, size and digest —
against the CORPUS'S OWN recorded identity and the bytes beside it. That establishes
self-consistency: this corpus is internally coherent and has not changed mid-session. It cannot
establish that this component map is THE CANONICAL ONE, because both sides of it come from the
corpus under test. Nothing anywhere compared an auxiliary component against the canonical corpus.

F1's question, asked of this field: *what is the complete thing this could compare?* For a corpus
that is the full component map (name -> size, digest) plus the row/partition/byte scalars plus the
schema digest. The scalars and the schema were already compared; the map was not. So the map is
compared now, both directions on names and exactly on size and digest, and
`total_component_bytes` stops being a handle standing in for a set — it becomes a DERIVED
consequence of a map that was compared (see `canonical_components`, which refuses an artifact whose
component sizes do not sum to the pinned total).

## Where the canonical map lives, and why this is NOT the registry shape

F2 wired its field into F1's `SESSION_BOUND_INPUTS` registry because a driver-supplied value
cannot be derived from anything; F3 deliberately did not, because a frozen binary's path IS
derivable from the session dir plus its key. F4's values are a third thing: they are ALREADY
RECORDED, in the committed identity of the real 2026-08-03 canonical corpus
(`CANONICAL_ARTIFACT_REL`). So this compares DIRECTLY AGAINST THE SOURCE OF RECORD, with no
registry and no new pin — the same reasoning F3 used, applied to a value that is on disk rather
than derivable.

The eight component name/size/digest triples are deliberately NOT copied into
`measurement_corpus.rs` first. That would be a SECOND COPY of every value whose correctness rests
on a drift assert somebody has to keep running — the exact argument this module already makes for
parsing the Rust source rather than committing a generated copy of it. The artifact is the only
place the auxiliary map exists, and `tools/ws0-corpus-gen/tests/measurement_corpus_pin.rs` already
locks the artifact to the Rust pin field-by-field where the two overlap, so the two sources cannot
drift apart on anything both of them carry.

Two sources, then, each authoritative for what it holds — and the artifact side is CORROBORATED
against the Rust side before it is used as an expectation (component sizes sum to
`TOTAL_COMPONENT_BYTES`; the `*-Data.db` component's size and digest equal `DATA_DB_BYTES` and
`DATA_DB_SHA256`), for the same reason `canonical_pins` corroborates its own parse: a swapped or
truncated artifact must not become the canonical expectation.

## What this comparison does and does not reach, stated rather than assumed

It compares the corpus's RECORDED map against the canonical map. It does NOT re-hash 2.8 GB here:
`verify_corpus_components` already does that in the same run, and a second derivation would be a
second implementation whose disagreement with the first would be undiagnosable. The chain that
closes is three links, each of which exists:

    committed canonical artifact  --(this module)-->  the corpus's recorded identity
    the corpus's recorded identity --(ws0_pin_components / verify_corpus_components)--> bytes on disk

Before F4 the FIRST link was absent for every component, and the second link alone is
self-consistency.
"""

from __future__ import annotations

import json
import pathlib
import re

from ws0_validate import Invalid, _SHA256_RE

# The Rust module that IS the pin, repo-relative. Named once.
RUST_PIN_REL = "tools/ws0-corpus-gen/src/measurement_corpus.rs"

# The committed identity of the REAL canonical corpus (recorded 2026-08-03) — the ONE place the
# canonical AUXILIARY component map exists. See the module docstring for why the map is read from
# here rather than copied into `measurement_corpus.rs`, and why that is the F3 shape rather than
# F1's registry.
CANONICAL_ARTIFACT_REL = "docs/reports/ws0-3096-artifacts/corpus-identity.json"

# The two baseline modes, as the words that appear in the manifest and the report. Constants
# because three files compare against them, and a mode spelled two ways is a mode nobody checks.
MODE_BASELINE = "baseline"
MODE_NON_BASELINE = "non-baseline"

# How the non-baseline mode is LABELLED to a human reader. One string, used by the report and
# asserted by the self-tests, so the label cannot be softened in one place and not the other.
NON_BASELINE_LABEL = (
    "NOT A WS0 BASELINE — this corpus is NOT the canonical measurement corpus"
)

# THE CENSUS: every field of the canonical corpus identity, and HOW each is established.
#
# Keyed by the `corpus-identity.json` field name — the artifact's key set is the SUBJECT of the
# closure below, so keying by it is what lets the two be compared directly. Each value is
# `(source, spec)`:
#
#   ("pin", (CONST, kind))    a constant in `RUST_PIN_REL`, parsed by `_parse_const`.
#   ("composed", (name, ...)) built from constants in OTHER Rust modules (the generator's seed,
#                             the schema's keyspace/table) — the INPUT ANCHORS. Separate from
#                             "pin" because they are not in `RUST_PIN_REL` and are parsed from
#                             the file that actually determines them, never retyped here.
#   ("literal", value)        a value asserted by the canonical shape itself rather than pinned
#                             anywhere — `compression_info_present: false` IS issue #1406.
#   ("map", None)             the component map, compared name-by-name/size/digest against the
#                             committed artifact by `component_divergences`.
#
# `int`/`str` are compared exactly. `bytes_per_row` is a FLOAT compared with a tolerance for the
# same reason `measurement_corpus.rs`'s own consistency test uses one: the pin records it to the
# precision the artifact recorded, not to full float precision.
#
# TOTAL_COMPONENT_BYTES IS RETAINED, and is no longer standing in for anything. The map is
# compared, so the total is now a DERIVED consequence — kept because a total that disagrees with a
# map that agrees is a corrupt artifact, which `canonical_components` refuses.
CANONICAL_CENSUS: dict[str, tuple[str, object]] = {
    "rows": ("pin", ("ROWS", "int")),
    "partitions": ("pin", ("PARTITIONS", "int")),
    "rows_per_partition": ("pin", ("ROWS_PER_PARTITION", "int")),
    "cells_per_row": ("pin", ("CELLS_PER_ROW", "int")),
    "data_db_bytes": ("pin", ("DATA_DB_BYTES", "int")),
    "total_component_bytes": ("pin", ("TOTAL_COMPONENT_BYTES", "int")),
    "bytes_per_row": ("pin", ("BYTES_PER_ROW", "float")),
    "data_db_sha256": ("pin", ("DATA_DB_SHA256", "str")),
    "schema_sha256": ("pin", ("SCHEMA_SHA256", "str")),
    # THE INPUT ANCHORS (#3272 round 14, F4). Never compared before this round, in either the
    # component map's company or on their own — see the module docstring for the measured cost.
    "seed": ("composed", ("seed",)),
    "table": ("composed", ("table",)),
    # ISSUE #1406's CLAIM BOUNDARY. The canonical corpus is uncompressed and the artifact records
    # `false` to say so; unread, a COMPRESSED corpus was classifiable as the canonical one.
    "compression_info_present": ("literal", False),
    # THE COMPONENT MAP — the field the review cited.
    "components": ("map", None),
}

# The fields the canonical artifact carries that CARRY NO IDENTITY CLAIM, each with the reason.
#
# This is the census's other half and it exists so the closure below can be exhaustive against the
# artifact's REAL key set: a key in neither map FAILS. There is deliberately no third,
# non-verifying disposition — a field is compared, or it is recorded here as not being an identity
# claim at all. Every reason must NAME ITS OWN FIELD (asserted at import), which is what makes a
# reason copy-pasted from a neighbour detectable; the same rule the Rust side's
# `reason_is_acceptable` enforces, for the same finding.
NOT_IDENTITY: dict[str, str] = {
    "issue": (
        "`issue` is a provenance LABEL naming #3096, not a measured quantity — two corpora of the"
        " same shape recorded under different issue numbers are the same corpus"
    ),
    "not_a_correctness_oracle": (
        "`not_a_correctness_oracle` is the #3042 disclaimer PROSE carried in the artifact for a"
        " human reader; its wording identifies no corpus"
    ),
    "differs_from_prior_corpus": (
        "`differs_from_prior_corpus` is PROSE recording which #3058/#3100 digest this corpus is"
        " NOT, so there is nothing for it to be compared against"
    ),
}

# `bytes_per_row` is recorded to 7 decimal places; a difference below this is a rounding artifact
# of the recorded precision, not a different corpus. Any real change in shape moves it far more.
_BPR_TOLERANCE = 1e-6

_INT_RE = r"[0-9][0-9_]*"


def _pin_source(repo_root: pathlib.Path) -> str:
    p = repo_root / RUST_PIN_REL
    try:
        return p.read_text()
    except OSError as exc:
        raise Invalid(
            f"the canonical measurement-corpus pin at {p} is unreadable ({exc}), so whether a"
            " corpus is the canonical one CANNOT be decided. Refused rather than assumed: a"
            " bridge that degraded to 'assume canonical' here would pass a smoke corpus as a"
            " WS0 baseline, which is the finding this module closes (#3272 round 13, F3)."
        ) from None


def _parse_const(src: str, name: str, kind: str, where: pathlib.Path):
    """One `pub const NAME: TYPE = LITERAL;`, or `Invalid`. Never a default."""
    if kind == "int":
        m = re.search(rf"^pub const {name}: *[a-z0-9]+ *= *({_INT_RE}) *;", src, re.M)
        if m:
            return int(m.group(1).replace("_", ""))
    elif kind == "float":
        m = re.search(rf"^pub const {name}: *f64 *= *({_INT_RE}\.[0-9_]+) *;", src, re.M)
        if m:
            return float(m.group(1).replace("_", ""))
    elif kind == "str":
        m = re.search(rf'^pub const {name}: *&str *= *"([^"]*)" *;', src, re.M)
        if m:
            return m.group(1)
    else:  # pragma: no cover — a programming error in CANONICAL_CENSUS, not an input error
        raise Invalid(f"unknown literal kind {kind!r} for {name}")
    raise Invalid(
        f"{where}: no parseable `pub const {name}` ({kind}) — the canonical value for this field"
        " is UNKNOWN, so a corpus cannot be checked against it. This is the failure mode a source"
        " parse is built for and it is FATAL, never a skipped field: a constant renamed, removed"
        " or reformatted must red this check rather than silently drop a comparison (#3272 round"
        " 13, F3)."
    )


# The INPUT ANCHORS, and the Rust file each is parsed from. NOT in `RUST_PIN_REL`: they live in the
# modules that actually DETERMINE them, and they are read from there rather than retyped here for
# the reason `measurement_corpus_pin.rs` gives — a literal repeated in a third place is free to
# drift from both of the first two. Same grammar, same fatal-on-absent rule.
_ANCHOR_SOURCES: dict[str, tuple[str, str, str]] = {
    # field -> (rust file, constant, kind)
    "seed": ("tools/ws0-corpus-gen/src/generate.rs", "DEFAULT_SEED", "int"),
    "_keyspace": ("tools/ws0-corpus-gen/src/schema.rs", "KEYSPACE", "str"),
    "_table": ("tools/ws0-corpus-gen/src/schema.rs", "TABLE", "str"),
}


def _anchor_pins(repo_root: pathlib.Path) -> dict:
    """The INPUT ANCHORS, parsed from the modules that determine them (#3272 round 14, F4).

    `table` is COMPOSED exactly as `generate()` composes the identity field — `KEYSPACE.TABLE` —
    so the comparison moves when either constant does. Mandatory, like every other constant this
    bridge reads: an anchor that cannot be parsed is fatal, never a dropped comparison.
    """
    out: dict[str, object] = {}
    parsed: dict[str, object] = {}
    for key, (rel, const, kind) in _ANCHOR_SOURCES.items():
        p = repo_root / rel
        try:
            src = p.read_text()
        except OSError as exc:
            raise Invalid(
                f"the canonical INPUT ANCHOR source {p} is unreadable ({exc}), so the"
                f" {const} that determines the pinned corpus CANNOT be read. A digest pin is only"
                " a pin together with the inputs that determine it (#3272 round 14, F4)."
            ) from None
        parsed[key] = _parse_const(src, const, kind, p)
    out["seed"] = parsed["seed"]
    out["table"] = f"{parsed['_keyspace']}.{parsed['_table']}"
    return out


def canonical_pins(repo_root: pathlib.Path) -> dict:
    """Every canonical constant, parsed from the Rust pin. All of them or `Invalid`."""
    src = _pin_source(repo_root)
    where = repo_root / RUST_PIN_REL
    pins = {}
    for _field, (source, spec) in CANONICAL_CENSUS.items():
        if source == "pin":
            const, kind = spec  # type: ignore[misc]
            pins[const] = _parse_const(src, const, kind, where)
    # The parse is CORROBORATED against the pin's own internal relationships before it is used, so
    # a regex that matched the wrong literal cannot become the canonical expectation. These are
    # the same relationships `measurement_corpus.rs`'s consistency test asserts in Rust — checked
    # again HERE because the property being established is that THIS PARSE read them correctly.
    if pins["ROWS"] != pins["PARTITIONS"] * pins["ROWS_PER_PARTITION"]:
        raise Invalid(
            f"{where}: the parsed constants are not internally consistent (ROWS"
            f" {pins['ROWS']} != PARTITIONS {pins['PARTITIONS']} x ROWS_PER_PARTITION"
            f" {pins['ROWS_PER_PARTITION']}), so this parse read something other than the pin."
        )
    if abs(pins["DATA_DB_BYTES"] / pins["ROWS"] - pins["BYTES_PER_ROW"]) > _BPR_TOLERANCE:
        raise Invalid(
            f"{where}: the parsed constants are not internally consistent (DATA_DB_BYTES/ROWS"
            f" != BYTES_PER_ROW {pins['BYTES_PER_ROW']}), so this parse read something other"
            " than the pin."
        )
    for const in ("DATA_DB_SHA256", "SCHEMA_SHA256"):
        if len(pins[const]) != 64:
            raise Invalid(
                f"{where}: {const} parsed as {len(pins[const])} chars, not a 64-char sha256 —"
                " this parse read something other than the pin."
            )
    return pins


def canonical_components(repo_root: pathlib.Path, pins: dict) -> dict:
    """The canonical COMPONENT MAP, read from the committed artifact and CORROBORATED (F4).

    The map is the one canonical value that exists nowhere else: `measurement_corpus.rs` pins
    quantities, not filenames. So it is read from `CANONICAL_ARTIFACT_REL` — the recorded identity
    of the real 2026-08-03 corpus — rather than copied into Rust first, because a copy is a second
    source of truth whose correctness rests on a drift assert somebody has to keep running (the
    argument this module already makes for parsing Rust rather than committing a generated copy).

    IT IS CORROBORATED AGAINST THE RUST PIN BEFORE BEING USED AS AN EXPECTATION, for the same
    reason `canonical_pins` corroborates its own parse: a swapped, truncated or edited artifact
    must not silently BECOME the canonical expectation. Three relationships, all of which the Rust
    side independently pins:

      * the component sizes SUM to `TOTAL_COMPONENT_BYTES`;
      * the `*-Data.db` component's size equals `DATA_DB_BYTES`;
      * ...and its digest equals `DATA_DB_SHA256`.

    An artifact failing any of them is refused as an ORACLE, never used as one — which is also
    what stops `total_component_bytes` from being the standalone handle it was: the total is now
    checked to be the sum of a map that was itself compared.
    """
    p = repo_root / CANONICAL_ARTIFACT_REL
    try:
        art = json.loads(p.read_text())
    except (OSError, ValueError) as exc:
        raise Invalid(
            f"the committed canonical corpus identity at {p} is unreadable ({exc}), so the"
            " canonical COMPONENT MAP is UNKNOWN and no corpus can be checked against it. Refused"
            " rather than assumed: falling back to comparing the aggregate total alone is exactly"
            " the finding this closes — a sum is not a set (#3272 round 14, F4)."
        ) from None
    if not isinstance(art, dict):
        raise Invalid(f"{p} must hold a JSON object, got {type(art).__name__}")
    comps = art.get("components")
    if not isinstance(comps, dict) or not comps:
        raise Invalid(
            f"{p} records no `components` map, so the canonical component set is UNKNOWN. This is"
            " fatal rather than a skipped comparison: the map is the only place the canonical"
            " auxiliary components exist, and 'assume canonical' here would restore the aggregate"
            "-only classification (#3272 round 14, F4)."
        )
    out: dict[str, dict] = {}
    total = 0
    for name, spec in comps.items():
        if not isinstance(spec, dict):
            raise Invalid(f"{p}: canonical component {name!r} is not a record")
        size = spec.get("bytes")
        sha = spec.get("sha256")
        if not isinstance(size, int) or size <= 0:
            raise Invalid(
                f"{p}: canonical component {name!r} records bytes={size!r}, which cannot be a"
                " size — this artifact cannot serve as the canonical expectation"
            )
        if not isinstance(sha, str) or not _SHA256_RE.match(sha):
            raise Invalid(
                f"{p}: canonical component {name!r} records sha256={sha!r}, which is not 64"
                " lowercase hex characters — this artifact cannot serve as the canonical"
                " expectation"
            )
        out[name] = {"bytes": size, "sha256": sha}
        total += size
    # CORROBORATION against the Rust pin, which is independently parsed.
    if total != pins["TOTAL_COMPONENT_BYTES"]:
        raise Invalid(
            f"{p}: the canonical component sizes sum to {total:,} but"
            f" {RUST_PIN_REL} pins TOTAL_COMPONENT_BYTES={pins['TOTAL_COMPONENT_BYTES']:,}. The"
            " two canonical sources disagree, so neither can be used as the expectation — one of"
            " them was edited alone (#3272 round 14, F4)."
        )
    data = [n for n in out if n.endswith("-Data.db")]
    if len(data) != 1:
        raise Invalid(
            f"{p}: the canonical component map names {len(data)} `*-Data.db` component(s), not"
            " exactly one, so it cannot be corroborated against the pinned Data.db digest"
        )
    d = out[data[0]]
    if d["bytes"] != pins["DATA_DB_BYTES"] or d["sha256"] != pins["DATA_DB_SHA256"]:
        raise Invalid(
            f"{p}: the canonical {data[0]} records {d['bytes']:,} B / {d['sha256']} while"
            f" {RUST_PIN_REL} pins {pins['DATA_DB_BYTES']:,} B / {pins['DATA_DB_SHA256']}. The two"
            " canonical sources disagree about the SAME component, so the artifact cannot be"
            " trusted as the canonical component map."
        )
    return out


def component_divergences(identity: dict, canonical: dict) -> list[str]:
    """Every way a corpus's component map differs from the canonical one (#3272 round 14, F4).

    The comparison the pre-fix classification could not express. `total_component_bytes` moved only
    with the AGGREGATE, so a corpus with an altered `Index.db` and a compensating `Statistics.db`
    — or same-length different bytes in either — kept the canonical total and was classified
    canonical. Names are compared BOTH DIRECTIONS (an absent component is a different read path; an
    extra one means this is not the pinned corpus), sizes and digests exactly.

    An ABSENT map is a divergence, not a skip: a corpus whose components are unrecorded is not
    established to be the canonical one.
    """
    got = identity.get("components")
    if not isinstance(got, dict) or not got:
        return [
            "components: RECORDED NOTHING (canonical map names"
            f" {len(canonical)} component(s): {', '.join(sorted(canonical))}) — an absent"
            " component map cannot establish the corpus is canonical, so it is a divergence, not"
            " a skip"
        ]
    out = []
    for name in sorted(set(canonical) - set(got)):
        out.append(
            f"components[{name}]: ABSENT (canonical {canonical[name]['bytes']:,} B) — a component"
            " the canonical corpus has and this one does not is a different read path"
        )
    for name in sorted(set(got) - set(canonical)):
        out.append(
            f"components[{name}]: PRESENT but not part of the canonical corpus — an extra"
            " component means this directory is not the pinned corpus"
        )
    for name in sorted(set(canonical) & set(got)):
        want = canonical[name]
        spec = got[name]
        if not isinstance(spec, dict):
            out.append(
                f"components[{name}]: recorded as {type(spec).__name__}, not a record with its"
                " size and digest, so it cannot be compared against the canonical component"
            )
            continue
        if spec.get("bytes") != want["bytes"]:
            out.append(
                f"components[{name}].bytes: {spec.get('bytes')!r} (canonical"
                f" {want['bytes']:,}) — pre-#3272-round-14 only the AGGREGATE was compared, so a"
                " compensating change in another component hid this"
            )
        if spec.get("sha256") != want["sha256"]:
            out.append(
                f"components[{name}].sha256: {spec.get('sha256')!r} (canonical"
                f" {want['sha256']}) — the same-length-different-bytes case, which NO aggregate"
                " comparison can see"
            )
    return out


def _schema_digest_from_disk(corpus: pathlib.Path) -> str | None:
    """`ws0-events.cql`'s real digest, or `None` if it is not there to be read.

    Imported function-locally to keep this module's import graph a leaf: `ws0_schema_input` owns
    the schema's filename and `ws0_session` owns the hasher, and neither should be pulled in at
    import time by a module the driver loads before either.
    """
    from ws0_schema_input import schema_path
    from ws0_session import sha256_file

    p = schema_path(corpus)
    try:
        return sha256_file(p)
    except OSError:
        return None


def divergences(
    identity: dict,
    pins: dict,
    corpus: pathlib.Path | None = None,
    anchors: dict | None = None,
    canonical_comps: dict | None = None,
) -> list[str]:
    """Every way `identity` differs from the canonical pin. Empty list == canonical.

    Walks `CANONICAL_CENSUS` — every disposition, not a hand-listed subset — so a field cannot be
    outside the comparison without being outside the census, which the closure in
    `classify_corpus` fails on. `anchors` and `canonical_comps` are the two non-`RUST_PIN_REL`
    expectations; both are REQUIRED for the census's `composed`/`map`/`literal` dispositions, and
    an absent one is a refusal rather than a skipped comparison (a value not observed is never a
    pass).

    Every field is compared and every difference is REPORTED, rather than returning on the first:
    an operator who generated a smoke corpus wants to see that the rows, the bytes and the digest
    all differ, not to rediscover each one on the next run.

    # AN ABSENT `schema_sha256` FALLS BACK TO THE BYTES ON DISK, and that is not a softening

    An identity field that is ABSENT is a divergence, not a skip — a corpus whose shape cannot be
    established is not established to be canonical. `schema_sha256` needs one qualification, and
    getting it wrong would have repeated round 9's F1 exactly: the field was added AFTER the
    2026-08-03 corpus was recorded (see `measurement_corpus::SCHEMA_SHA256`'s own docs), so the
    committed artifact — the identity of the REAL canonical corpus — does not carry it. Treating
    that as a divergence would make the canonical corpus itself unable to be measured as a
    baseline, which is precisely the "documented command that cannot succeed" failure this issue
    has now hit three times.

    So when the artifact records no `schema_sha256` and the schema FILE is on disk, its digest is
    read from THE FILE. That is a STRONGER oracle than the recorded field, not a weaker one: the
    file is the input both arms actually read, while the recorded value is a claim about it. When
    neither is available the field IS a divergence, unchanged.
    """
    out: list[str] = []
    for field, (source, spec) in CANONICAL_CENSUS.items():
        # THE COMPONENT MAP — its own comparison, which reports per component.
        if source == "map":
            if canonical_comps is None:
                raise Invalid(
                    "the canonical COMPONENT MAP was not supplied, so the component comparison"
                    " CANNOT be performed. Refused rather than skipped: comparing the aggregate"
                    " total alone is the finding (#3272 round 14, F4)."
                )
            out.extend(component_divergences(identity, canonical_comps))
            continue
        if source == "pin":
            const, kind = spec  # type: ignore[misc]
            want = pins[const]
        elif source == "composed":
            if anchors is None:
                raise Invalid(
                    "the canonical INPUT ANCHORS were not supplied, so `seed`/`table` CANNOT be"
                    " compared. A digest pin is only a pin together with the inputs that"
                    " determine it (#3272 round 14, F4)."
                )
            (anchor_key,) = spec  # type: ignore[misc]
            want = anchors[anchor_key]
            kind = "int" if isinstance(want, int) else "str"
        else:  # "literal" — the canonical shape asserts the value itself (#1406)
            want = spec
            kind = "bool"
        got = identity.get(field)
        if got is None and field == "schema_sha256" and corpus is not None:
            got = _schema_digest_from_disk(corpus)
        if got is None:
            out.append(
                f"{field}: RECORDED NOTHING (canonical {want!r}) — an absent field cannot"
                " establish the corpus is canonical, so it is a divergence, not a skip"
            )
            continue
        if kind == "float":
            try:
                same = abs(float(got) - float(want)) <= _BPR_TOLERANCE
            except (TypeError, ValueError):
                same = False
        elif kind == "int":
            try:
                same = int(got) == int(want)
            except (TypeError, ValueError):
                same = False
        elif kind == "bool":
            # EXACT, not truthiness: `0`, `""` and `None` are not the recorded `false` that
            # asserts #1406's claim boundary, and a truthy test would accept any of them.
            same = isinstance(got, bool) and got is want
        else:
            same = str(got) == str(want)
        if not same:
            out.append(f"{field}: {got!r} (canonical {want!r})")
    return out


def classify_corpus(
    repo_root: pathlib.Path, identity: dict, corpus: pathlib.Path | None = None
) -> dict:
    """Is this corpus the canonical measurement corpus? A RECORD, never a bare verdict.

    Returns the comparison ITSELF — which constants were compared, from which file, and every
    divergence found — so the manifest and the report cite an observation rather than a boolean
    somebody has to trust. `is_canonical` is derived from `divergences` here, in one place.
    """
    pins = canonical_pins(repo_root)
    anchors = _anchor_pins(repo_root)
    comps = canonical_components(repo_root, pins)
    # THE CENSUS IS CLOSED AGAINST THE ARTIFACT'S REAL KEY SET, BEFORE ANY COMPARISON (F4).
    #
    # This is the check whose absence made the cited finding one of SEVEN. Reading the canonical
    # artifact and subtracting both dispositions is the only direction that can see a field NOBODY
    # THOUGHT OF: an enumeration of what the comparison covers can only ever certify its own
    # scope — measured, the shipped classifier read 8 of the artifact's 15 keys and every test
    # passed, with `seed`, `table` and `compression_info_present` among the 7 it never consulted.
    _require_census_covers_the_artifact(repo_root)
    diffs = divergences(identity, pins, corpus, anchors, comps)
    return {
        "is_canonical": not diffs,
        "compared_fields": sorted(CANONICAL_CENSUS),
        "canonical_pin_source": RUST_PIN_REL,
        "canonical_component_source": CANONICAL_ARTIFACT_REL,
        "canonical_components": len(comps),
        "divergences": diffs,
        "scope": (
            f"every one of the {len(CANONICAL_CENSUS)} identity field(s) the canonical corpus is"
            f" defined by was compared, including the COMPLETE component map ({len(comps)}"
            f" component(s), name/size/digest, both directions) against {CANONICAL_ARTIFACT_REL},"
            f" and the INPUT ANCHORS (seed, table) against the generator/schema constants that"
            " determine them. Pre-#3272-round-14 the component set was covered only in AGGREGATE"
            " by total_component_bytes — a sum, not a set — and seed/table/compression_info_present"
            " were not compared at all; the fields the canonical artifact carries that assert no"
            f" identity ({', '.join(sorted(NOT_IDENTITY))}) are recorded as such rather than"
            " silently uncompared"
        ),
    }


def _require_census_covers_the_artifact(repo_root: pathlib.Path) -> None:
    """Every key the canonical artifact carries has a DISPOSITION. Fail-closed (F4).

    The direction matters and is the whole value of this check: it reads the ARTIFACT and subtracts
    the census, so a field the census's author never thought of FAILS. The reverse arrangement — an
    assert over the fields the comparison covers — certifies its own scope and is what let the
    cited `total_component_bytes` finding sit beside six more uncompared fields.

    A field is COMPARED (`CANONICAL_CENSUS`) or recorded as asserting no identity (`NOT_IDENTITY`).
    There is no third disposition, deliberately: F1 deleted `required-present` because a
    "present but not compared" state reads as coverage while performing none.
    """
    p = repo_root / CANONICAL_ARTIFACT_REL
    try:
        art = json.loads(p.read_text())
    except (OSError, ValueError) as exc:
        raise Invalid(
            f"the committed canonical corpus identity at {p} is unreadable ({exc}), so whether"
            " every canonical field is compared CANNOT be established (#3272 round 14, F4)."
        ) from None
    if not isinstance(art, dict) or not art:
        raise Invalid(f"{p} must hold a non-empty JSON object")
    unaccounted = sorted(set(art) - set(CANONICAL_CENSUS) - set(NOT_IDENTITY))
    if unaccounted:
        raise Invalid(
            f"{p} carries field(s) {', '.join(unaccounted)} that ws0_canonical_corpus neither"
            " COMPARES (CANONICAL_CENSUS) nor records as asserting no identity (NOT_IDENTITY)."
            " Every field is a decision: an unaccounted one is a quantity the canonical artifact"
            " asserts and the classification never consults — which is the F4 finding itself, in"
            " which `total_component_bytes` was compared while `seed`, `table`,"
            " `compression_info_present` and the component map were not. Add it to whichever map"
            " applies (#3272 round 14, F4)."
        )


def require_canonical_or_declared(
    repo_root: pathlib.Path, identity: dict, mode: str, corpus: pathlib.Path | None = None
) -> dict:
    """The pre-measurement gate: a noncanonical corpus may run, but NOT as a baseline.

    * canonical corpus + `baseline` mode -> ACCEPTED as a baseline.
    * noncanonical corpus + `baseline` mode -> `Invalid`, naming every divergence AND naming the
      flag that runs it anyway. A refusal that does not say how to proceed is how rounds 9 and 10
      broke three documented commands.
    * noncanonical corpus + `non-baseline` mode -> ACCEPTED, and the returned record carries the
      label the report prints.
    * canonical corpus + `non-baseline` mode -> ACCEPTED and labelled non-baseline. The OPERATOR's
      declaration wins over the shape, deliberately: `--non-baseline` says "do not publish this as
      a baseline", which is a statement about the RUN (a throwaway, a rig change under test), not
      only about the corpus, and silently upgrading it to a baseline would overrule that.
    """
    if mode not in (MODE_BASELINE, MODE_NON_BASELINE):
        raise Invalid(
            f"baseline mode must be {MODE_BASELINE!r} or {MODE_NON_BASELINE!r}, got {mode!r}"
        )
    rec = classify_corpus(repo_root, identity, corpus)
    rec["mode"] = mode
    rec["is_baseline"] = rec["is_canonical"] and mode == MODE_BASELINE
    if mode == MODE_BASELINE and not rec["is_canonical"]:
        listed = "\n         ".join(rec["divergences"])
        raise Invalid(
            "THIS CORPUS IS NOT THE CANONICAL MEASUREMENT CORPUS, so it cannot be measured as a"
            " WS0 BASELINE. It diverges from the pin in"
            f" {rec['canonical_pin_source']} in {len(rec['divergences'])} field(s):\n"
            f"         {listed}\n"
            "         Pre-#3272-round-13 this ran and REPORTED AS A BASELINE: the pin recorded"
            " the identity of whatever corpus it was handed and compared it against nothing, so"
            " a smoke-sized or differently-seeded corpus was self-consistent all the way through"
            " the reporter.\n"
            "         TO RUN IT ANYWAY, which is supported and is the smoke path: pass"
            " --non-baseline. The session and the report are then LABELLED"
            f" '{NON_BASELINE_LABEL}' and the run is not a baseline.\n"
            "         TO PRODUCE A BASELINE: regenerate the canonical corpus with"
            " `cargo run --release -p ws0-corpus-gen --bin ws0-corpus-gen -- --out <root>`"
            " (~2.8 GB, minutes)."
        )
    rec["label"] = (
        "the canonical measurement corpus, measured as a WS0 BASELINE"
        if rec["is_baseline"]
        else NON_BASELINE_LABEL
        + (
            " (the corpus IS canonical; --non-baseline was requested, so the run is not"
            " published as a baseline)"
            if rec["is_canonical"]
            else f" ({len(rec['divergences'])} field(s) diverge from"
            f" {rec['canonical_pin_source']})"
        )
    )
    return rec


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
    # THE VERDICTS ARE RE-DERIVED, never trusted. A hand-edited `is_baseline: true` beside a
    # non-empty divergence list must be refused: that is the substitution this record exists to
    # make impossible, and a recorded boolean nobody re-derives cannot make it so.
    if bool(rec["is_canonical"]) != (not diffs):
        raise Invalid(
            f"{pin_path} `{PIN_CANONICAL_FIELD}` CONTRADICTS ITSELF: is_canonical="
            f"{rec['is_canonical']!r} beside {len(diffs)} recorded divergence(s). The verdict is"
            " DERIVED from the divergences, so these cannot both be true — this record was edited."
        )
    if bool(rec["is_baseline"]) != (not diffs and mode == MODE_BASELINE):
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

# THE CENSUS IS CLOSED AT IMPORT TOO (#3272 round 14, F4), on the properties that do not need the
# artifact — so a malformed disposition is a load-time error in every consumer rather than a
# refusal on somebody's measurement run. The artifact-facing half (every artifact key has a
# disposition) needs a repo root and runs in `classify_corpus`.
#
# Checked here: no field carries two dispositions (which one applied would depend on read order);
# every `pin` names a kind `divergences` can actually compare; every `NOT_IDENTITY` reason NAMES
# ITS OWN FIELD, which is what makes a reason copy-pasted from a neighbour detectable — the
# property the Rust side's `reason_is_acceptable` enforces, added here for the same finding.
_both = sorted(set(CANONICAL_CENSUS) & set(NOT_IDENTITY))
if _both:
    raise Invalid(
        f"{', '.join(_both)} appear(s) in BOTH CANONICAL_CENSUS and NOT_IDENTITY — one field, one"
        " disposition, or which rule applied would depend on read order (#3272 round 14, F4)."
    )
del _both
_KINDS = ("int", "float", "str")
for _field, (_source, _spec) in CANONICAL_CENSUS.items():
    if _source not in ("pin", "composed", "literal", "map"):
        raise Invalid(
            f"CANONICAL_CENSUS[{_field!r}] declares source {_source!r}, which `divergences` cannot"
            " compare. There is deliberately no non-verifying disposition: a field is compared, or"
            " it belongs in NOT_IDENTITY (#3272 round 14, F4)."
        )
    if _source == "pin":
        _c, _k = _spec  # type: ignore[misc]
        if _k not in _KINDS:
            raise Invalid(
                f"CANONICAL_CENSUS[{_field!r}] declares literal kind {_k!r}; the parser handles"
                f" {_KINDS}. An unparseable kind would be a comparison that cannot run."
            )
    if _source == "composed":
        (_a,) = _spec  # type: ignore[misc]
        if _a not in ("seed", "table"):
            raise Invalid(
                f"CANONICAL_CENSUS[{_field!r}] composes from anchor {_a!r}, which `_anchor_pins`"
                " does not build — the comparison would raise KeyError at classify time."
            )
del _field, _source, _spec, _KINDS
for _field, _reason in NOT_IDENTITY.items():
    _low = _reason.lower()
    if _field.lower() not in _low and not all(
        _w in _low for _w in _field.lower().split("_") if len(_w) >= 4
    ):
        raise Invalid(
            f"the NOT_IDENTITY reason for `{_field}` does not NAME the field, so it cannot be"
            " distinguished from a reason copied from a neighbouring entry — the copy-paste case"
            " a length or presence check cannot see at all (#3272 round 14, F4)."
        )
    if any(_p in _low for _p in ("tbd", "todo", "fixme", "for now", "see above")):
        raise Invalid(
            f"the NOT_IDENTITY reason for `{_field}` is a PLACEHOLDER. That is the form that parks"
            " a real gap as an audited decision: the check reads as coverage, the reason reads as"
            " a decision, and nobody looks again."
        )
del _field, _reason, _low
