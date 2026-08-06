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
"""

from __future__ import annotations

import pathlib
import re

from ws0_validate import Invalid

# The Rust module that IS the pin, repo-relative. Named once.
RUST_PIN_REL = "tools/ws0-corpus-gen/src/measurement_corpus.rs"

# The two baseline modes, as the words that appear in the manifest and the report. Constants
# because three files compare against them, and a mode spelled two ways is a mode nobody checks.
MODE_BASELINE = "baseline"
MODE_NON_BASELINE = "non-baseline"

# How the non-baseline mode is LABELLED to a human reader. One string, used by the report and
# asserted by the self-tests, so the label cannot be softened in one place and not the other.
NON_BASELINE_LABEL = (
    "NOT A WS0 BASELINE — this corpus is NOT the canonical measurement corpus"
)

# The canonical constants this module requires, mapped to the `corpus-identity.json` field each is
# compared against, with the literal KIND each is parsed as.
#
# `int` fields are compared exactly. `bytes_per_row` is a FLOAT and is compared with a tolerance
# for the same reason `measurement_corpus.rs`'s own consistency test uses one: the pin records it
# to the precision the artifact recorded, not to full float precision.
#
# WHAT THIS COVERS, AND WHAT IT DOES NOT — stated rather than left to be assumed. The canonical
# component NAME SET is not pinned in Rust (the constants describe quantities, not filenames), so
# it is not compared name-by-name here. `total_component_bytes` is the aggregate that stands in
# for it: any component added, removed or resized moves it. That is weaker than a name-set
# comparison and is recorded as weaker; the per-component name/size/digest checks the pin already
# performs (`ws0_pin_components.verify_pinned_components`) are what cover the set itself, against
# the corpus's own recorded identity.
CANONICAL_FIELDS: dict[str, tuple[str, str]] = {
    "ROWS": ("rows", "int"),
    "PARTITIONS": ("partitions", "int"),
    "ROWS_PER_PARTITION": ("rows_per_partition", "int"),
    "CELLS_PER_ROW": ("cells_per_row", "int"),
    "DATA_DB_BYTES": ("data_db_bytes", "int"),
    "TOTAL_COMPONENT_BYTES": ("total_component_bytes", "int"),
    "BYTES_PER_ROW": ("bytes_per_row", "float"),
    "DATA_DB_SHA256": ("data_db_sha256", "str"),
    "SCHEMA_SHA256": ("schema_sha256", "str"),
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
    else:  # pragma: no cover — a programming error in CANONICAL_FIELDS, not an input error
        raise Invalid(f"unknown literal kind {kind!r} for {name}")
    raise Invalid(
        f"{where}: no parseable `pub const {name}` ({kind}) — the canonical value for this field"
        " is UNKNOWN, so a corpus cannot be checked against it. This is the failure mode a source"
        " parse is built for and it is FATAL, never a skipped field: a constant renamed, removed"
        " or reformatted must red this check rather than silently drop a comparison (#3272 round"
        " 13, F3)."
    )


def canonical_pins(repo_root: pathlib.Path) -> dict:
    """Every canonical constant, parsed from the Rust pin. All of them or `Invalid`."""
    src = _pin_source(repo_root)
    where = repo_root / RUST_PIN_REL
    pins = {}
    for const, (_field, kind) in CANONICAL_FIELDS.items():
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
    identity: dict, pins: dict, corpus: pathlib.Path | None = None
) -> list[str]:
    """Every way `identity` differs from the canonical pin. Empty list == canonical.

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
    out = []
    for const, (field, kind) in CANONICAL_FIELDS.items():
        want = pins[const]
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
    diffs = divergences(identity, pins, corpus)
    return {
        "is_canonical": not diffs,
        "compared_fields": sorted(f for f, _k in CANONICAL_FIELDS.values()),
        "canonical_pin_source": RUST_PIN_REL,
        "divergences": diffs,
        "scope": (
            "the canonical component NAME SET is not pinned in Rust, so it is covered in"
            " aggregate by total_component_bytes rather than name-by-name; the per-component"
            " name/size/digest comparison is the session pin's"
            " (ws0_pin_components.verify_pinned_components), against the corpus's own identity"
        ),
    }


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
