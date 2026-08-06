#!/usr/bin/env python3
"""THE SESSION MANIFEST and the corpus IDENTITY VERIFICATION (#3272 review round 5).

Split out of `ws0_validate.py` under the campsite rule: that file was at 855 lines against a
~800 target before F1/F3 added anything, so this is a split by RESPONSIBILITY rather than a
waiver. The seam:

    ws0_validate.py   the QUANTITY domain — was this number validly observed?
    ws0_session.py    the SESSION's identity — is this report ABOUT what it says it is?

Two round-5 findings live here, and they are the same defect at two scopes:

* **F1 — the reporting CONFIGURATION came from the CURRENT CLI.** `ws0_report.py` took
  `--reps`, `--temps`, `--arms`, `--server-cpus`, `--client-cpus`, `--step-duration` and
  `--scan-passes` from whoever invoked it, so re-reporting a session dir with FEWER reps, a
  narrower arm set, different CPU pins or a different `--scan-passes` produced a confident
  report that ignored measured artifacts and printed the REPLACEMENT configuration as though
  it had been verified. The session's configuration is now recorded BEFORE measurement and the
  reporter READS IT FROM THE MANIFEST — see `session_manifest_config`, and the note there on
  why reading beats matching.

* **F3 — corpus verification checked ONLY `Data.db`.** The generator records every emitted
  component with its size and sha256 (`CorpusIdentity.components`), and the scans consume the
  auxiliary components too — `Index.db` above all — so a modified `Index.db` could change
  measured behaviour while the report claimed the corpus identity had been verified.
  `verify_corpus_bytes` now verifies the COMPLETE recorded component set.

The mechanism for both is ONE pre-measurement record: the driver stamps
`session-corpus-pin.json` before rep 1 and the reporter refuses a session whose manifest it
cannot match. Round 4 introduced that file for the corpus digest alone; F1/F3 EXTEND the same
record rather than adding a second parallel one, so there is exactly one place a session's
identity is established.
"""

from __future__ import annotations

import hashlib
import json
import pathlib

from ws0_canonical_corpus import MODE_BASELINE, MODE_NON_BASELINE
from ws0_validate import (
    Invalid,
    _SHA256_RE,
    cli_count,
    nonempty_selection,
    positive_int,
)


# Where a ws0 corpus's SSTable components live, relative to the corpus root — the
# layout `ws0-corpus-gen` writes and both measurement arms resolve.
CORPUS_TABLE_SUBPATH = ("ws0", "events")

# Read the Data.db in 8 MiB slices. The measurement corpus is ~2.8 GB, so the digest
# must stream: reading it whole would need 2.8 GB of RSS to verify a fixture.
_DIGEST_CHUNK = 8 << 20


def locate_corpus_data_db(corpus: pathlib.Path) -> pathlib.Path:
    """The single `*-Data.db` the measurement read, or `Invalid`.

    Ambiguity is fatal in both directions. NO `Data.db` means there is nothing for
    the recorded identity to be the identity OF. TWO means the identity records one
    digest for two candidate files, and picking either would be a guess about which
    the measurement actually streamed — a heuristic, in the one place the whole rig
    is trying to be authoritative about (#28, #3272 review B6).
    """
    table_dir = corpus.joinpath(*CORPUS_TABLE_SUBPATH)
    if not table_dir.is_dir():
        raise Invalid(
            f"{table_dir} is not a directory — the corpus identity cannot be verified"
            " against the bytes that were measured, because there are no bytes there."
            " Regenerate with tools/ws0-corpus-gen."
        )
    found = sorted(p for p in table_dir.iterdir() if p.name.endswith("-Data.db"))
    if not found:
        raise Invalid(
            f"{table_dir} holds no *-Data.db, so the recorded corpus identity"
            " describes nothing that is present. A report may not identify bytes it"
            " cannot read."
        )
    if len(found) > 1:
        raise Invalid(
            f"{table_dir} holds {len(found)} *-Data.db files"
            f" ({', '.join(p.name for p in found)}), but corpus-identity.json records"
            " ONE digest. Which one the measurement streamed cannot be determined, and"
            " guessing is exactly the heuristic this rig refuses. Measure a corpus with"
            " a single SSTable."
        )
    return found[0]


def sha256_file(path: pathlib.Path) -> str:
    """Streaming lowercase-hex sha256 of `path` (constant memory, any file size)."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(_DIGEST_CHUNK)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def verify_corpus_bytes(
    corpus: pathlib.Path, identity: dict, skip_digest: bool = False
) -> dict:
    """Compare the RECORDED identity against the `Data.db` actually present.

    #3272 review B6: corpus identity was trusted ENTIRELY from
    `corpus-identity.json`. The file was validated for internal consistency and the
    `Data.db` was never opened, so stale metadata sitting beside different bytes
    misidentified the corpus while every other check — including the row-count
    validation the identity feeds — passed. The report then printed that recorded
    sha256 under "corpus sha256:" as the identity of the measured bytes.

    Two comparisons, deliberately split by cost:

    * **SIZE — always.** A `stat`. There is no argument for skipping it, so there is
      no flag that can.
    * **SHA-256 — streamed, opt-outable ONLY visibly.** Digesting 2.8 GB costs
      seconds of IO per report run. `skip_digest` (the driver/reporter's
      `--skip-corpus-digest`) omits it, and the returned record then carries
      `sha256_verified: False` with `data_db_sha256_measured: None`, which the
      reporter STAMPS into the summary as `CORPUS DIGEST UNVERIFIED`. A silent skip
      is not available: an unverified identity that reads like a verified one is the
      defect, not the cost.

    Returns the verification RECORD (what was measured, and by what), so the report
    carries the observation rather than a bare boolean.
    """
    data_db = locate_corpus_data_db(corpus)
    measured_bytes = data_db.stat().st_size
    recorded_bytes = identity["data_db_bytes"]
    if measured_bytes != recorded_bytes:
        raise Invalid(
            f"{corpus / 'corpus-identity.json'} records data_db_bytes"
            f" {recorded_bytes:,} but {data_db.name} is {measured_bytes:,} bytes on"
            " disk. The recorded identity does not describe the corpus that would be"
            " measured, so every figure derived from it (bytes/row, the row"
            " denominator, the corpus digest printed in the summary) would name the"
            " wrong bytes. Regenerate the corpus, or point --corpus at the one the"
            " identity was recorded from."
        )

    record = {
        "data_db": str(data_db),
        "data_db_bytes_measured": measured_bytes,
        "size_verified": True,
        "data_db_sha256_recorded": identity["data_db_sha256"],
        "data_db_sha256_measured": None,
        "sha256_verified": False,
        "note": "",
    }
    if skip_digest:
        record["note"] = (
            "CORPUS DIGEST UNVERIFIED: --skip-corpus-digest was passed, so the"
            f" recorded sha256 was NOT compared against {data_db.name}. The size"
            " matched. Anything citing this report's corpus identity is citing the"
            " RECORDED digest, not an observed one."
        )
        return record

    measured_sha = sha256_file(data_db)
    record["data_db_sha256_measured"] = measured_sha
    if measured_sha != identity["data_db_sha256"]:
        raise Invalid(
            f"{corpus / 'corpus-identity.json'} records data_db_sha256"
            f" {identity['data_db_sha256']} but {data_db.name} hashes to"
            f" {measured_sha}. The size matched, so this is the case a size check"
            " alone cannot see: DIFFERENT BYTES of the same length, or an identity"
            " file that outlived the corpus beside it. Every #3096 figure is bound to"
            " a specific corpus digest; measuring one corpus and reporting another's"
            " identity is how a comparison against a recorded number becomes"
            " meaningless. Regenerate, or measure the corpus this identity describes."
        )
    record["sha256_verified"] = True
    record["note"] = (
        f"the recorded size and sha256 were both re-derived from {data_db.name} at"
        " report time; the identity describes the bytes that were measured"
    )
    return record


def verify_corpus_components(
    corpus: pathlib.Path, identity: dict, skip_digest: bool = False
) -> dict:
    """Verify the COMPLETE recorded component set, not only `Data.db` (#3272 F3).

    # The finding

    `verify_corpus_bytes` above checks one file. But a scan does not read only `Data.db`: it
    reads `Index.db`, and the `Statistics.db`/`Summary.db`/`Filter.db`/`CompressionInfo.db`
    components shape how it reads. So a MODIFIED AUXILIARY COMPONENT could change measured
    behaviour — a truncated `Index.db` alters the read pattern the whole rig exists to
    measure — while the report stated that corpus verification had succeeded. The generator
    already records every emitted component with its size and sha256
    (`CorpusIdentity.components`, `scan_components` in tools/ws0-corpus-gen/src/identity.rs);
    nothing read them.

    # Which components must be present is taken from the RECORDED IDENTITY, never a list

    There is deliberately no hardcoded component set here. Formats differ (a BTI corpus has
    `Rows.db`/`Partitions.db` where BIG has `Index.db`; an uncompressed corpus has no
    `CompressionInfo.db` at all, which for this rig is REQUIRED by #1406), so a hardcoded list
    would either reject a legitimate corpus or quietly skip a component. The recorded identity
    is the authority: exactly the components it records must be present, with the sizes and
    digests it records, and a component present on disk that the identity does NOT record is
    equally a finding — that is a corpus which is not the one described.

    # Cost

    Sizes are always verified (a `stat` each). Digests stream in 8 MiB slices, so a multi-GB
    corpus stays feasible under bounded memory, and `--skip-corpus-digest` omits them for
    EVERY component uniformly — never for some and not others, because a partial verification
    reported as a verification is this issue's whole subject. The unverified case stamps the
    same `identity unverified` note the `Data.db` path does, now covering the full set.
    """
    recorded = identity.get("components")
    if not isinstance(recorded, dict) or not recorded:
        raise Invalid(
            f"{corpus / 'corpus-identity.json'} records no `components` map, so the corpus's"
            " AUXILIARY components cannot be verified — only Data.db could be, and a scan also"
            " reads Index.db (plus the Statistics/Summary/Filter components that shape how it"
            " reads). A modified auxiliary component changes measured behaviour while the"
            " report claims the corpus identity was verified (#3272 F3). Regenerate the corpus"
            " with tools/ws0-corpus-gen, which records every emitted component."
        )
    table_dir = corpus.joinpath(*CORPUS_TABLE_SUBPATH)
    present = {p.name for p in table_dir.iterdir() if p.is_file()}
    missing = sorted(set(recorded) - present)
    if missing:
        raise Invalid(
            f"{table_dir} is MISSING recorded component(s): {', '.join(missing)}."
            " The recorded identity describes a corpus whose components are not all there, so"
            " it does not describe the corpus that would be measured. A scan reads more than"
            " Data.db — an absent Index.db changes the read pattern this rig measures."
            " Regenerate the corpus."
        )
    # A component PRESENT but NOT RECORDED is equally a finding: this is not the corpus the
    # identity describes, and a stray component can be read by a scan.
    unrecorded = sorted(present - set(recorded))
    if unrecorded:
        raise Invalid(
            f"{table_dir} holds component(s) the recorded identity does NOT describe:"
            f" {', '.join(unrecorded)}. The identity is the authority for what this corpus IS,"
            " so an extra component means the directory is not that corpus — a second SSTable"
            " generation, or a hand-added file a scan may read. Regenerate, or measure the"
            " corpus this identity was recorded from."
        )
    components: dict[str, dict] = {}
    for name in sorted(recorded):
        spec = recorded[name]
        if not isinstance(spec, dict):
            raise Invalid(
                f"{corpus / 'corpus-identity.json'}: component {name!r} is a"
                f" {type(spec).__name__}, not a record with its size and digest"
            )
        path = table_dir / name
        rec_bytes = positive_int(
            f"corpus-identity.json: component {name!r} 'bytes'", spec.get("bytes")
        )
        rec_sha = spec.get("sha256")
        if not isinstance(rec_sha, str) or not _SHA256_RE.match(rec_sha):
            raise Invalid(
                f"corpus-identity.json: component {name!r} records 'sha256' {rec_sha!r},"
                " which is not 64 lowercase hex characters — a truncated or absent digest"
                " cannot identify the component that was measured"
            )
        measured_bytes = path.stat().st_size
        if measured_bytes != rec_bytes:
            raise Invalid(
                f"corpus component {name} is {measured_bytes:,} bytes on disk but the"
                f" recorded identity says {rec_bytes:,}. The corpus that would be measured is"
                " not the one the identity describes, and this component is READ BY THE SCAN"
                " (or shapes how it reads), so the measurement would be of something other"
                " than the recorded corpus (#3272 F3)."
            )
        entry = {
            "bytes_recorded": rec_bytes,
            "bytes_measured": measured_bytes,
            "size_verified": True,
            "sha256_recorded": rec_sha,
            "sha256_measured": None,
            "sha256_verified": False,
        }
        if not skip_digest:
            measured_sha = sha256_file(path)
            entry["sha256_measured"] = measured_sha
            if measured_sha != rec_sha:
                raise Invalid(
                    f"corpus component {name} hashes to {measured_sha} but the recorded"
                    f" identity says {rec_sha}. The size matched, so this is the case a size"
                    " check alone cannot see: DIFFERENT BYTES of the same length. This"
                    " component is consumed by the scan, so the measured behaviour is not the"
                    " recorded corpus's behaviour (#3272 F3)."
                )
            entry["sha256_verified"] = True
        components[name] = entry
    verified = sum(1 for e in components.values() if e["sha256_verified"])
    return {
        "components_recorded": len(components),
        "components_verified_size": len(components),
        "components_verified_sha256": verified,
        "components": components,
        # AFFIRMATIVE, and it states the SCOPE of what was verified: a reader can tell a full
        # verification from a size-only one without inferring it from a flag's absence.
        "note": (
            f"all {len(components)} recorded component(s) were re-stat'ed and"
            f" {verified} of {len(components)} re-hashed at report time"
            if verified
            else f"all {len(components)} recorded component(s) were re-stat'ed; NO digest was"
            " re-derived (--skip-corpus-digest), so every component's identity below is the"
            " RECORDED one, not an observed one"
        ),
    }


# The name of the identity the DRIVER stamps into the session dir BEFORE it measures
# anything. Distinct from the corpus's own `corpus-identity.json`, which lives beside the data
# and can be replaced under a session at any time.
SESSION_CORPUS_PIN = "session-corpus-pin.json"

# The pin's Flight-ticket digest field (#3272 round 10, M1). Defined HERE, in the module that owns
# the pin's shape, and imported by `ws0_ticket_input` — one spelling, so the writer below and the
# reader over there cannot drift onto two names, which would present as an absent-field refusal on
# a session that pinned the ticket correctly. (The dependency runs THIS WAY because
# `ws0_ticket_input` already imports `sha256_file` from here; the reverse would be a cycle.)
PIN_TICKET_FIELD = "ticket_template_sha256"


def session_pin_path(session_dir: pathlib.Path) -> pathlib.Path:
    return session_dir / SESSION_CORPUS_PIN


def _measure_ticket_digest(session_dir: pathlib.Path) -> str:
    """The Flight ticket's digest, from `ws0_ticket_input` (#3272 round 10, M1).

    Imported function-locally for the same reason `verify_pinned_schema` and
    `verify_pinned_components` are: that module imports `sha256_file` from THIS one, so a
    module-scope import here would be a cycle. The split is by responsibility — see each module's
    docstring.

    Takes the SESSION dir since round 13's F2 moved the ticket out of the shared corpus and into the
    session's exclusively-claimed output directory.
    """
    from ws0_ticket_input import measure_ticket_digest

    return measure_ticket_digest(session_dir)


def write_session_corpus_pin(
    session_dir: pathlib.Path,
    corpus: pathlib.Path,
    identity: dict,
    config: dict | None = None,
    canonical: dict | None = None,
) -> dict:
    """Record WHICH CORPUS — and, since F1, WHICH CONFIGURATION — this session will measure.

    Called by `ws0-baseline.sh` BEFORE the first rep, and read back by the reporter (see
    `verify_session_corpus_pin`).

    # The finding (#3272 review round 4)

    The corpus digest was verified only against the corpus present AT REPORT TIME. No corpus
    identity was captured in the session dir before measurement, so two real sequences
    attributed measurements to bytes that were never measured:

    * RE-REPORTING an old result dir against a DIFFERENT corpus. `ws0_report.py --dir <old>
      --corpus <other>` re-derives `<other>`'s digest, finds it self-consistent, and prints it
      as the identity of figures measured over something else. Nothing in the old dir said
      which corpus it came from.
    * CHANGING THE CORPUS MID-RUN. A regeneration (or a second lane writing the same path)
      between rep 1 and rep N leaves report time verifying the LAST state of the corpus while
      the earlier reps measured the earlier bytes.

    Verifying at report time cannot see either, because both are consistent at report time.
    The pin is the missing half: an identity captured BEFORE, compared AFTER.

    What is recorded is the SIZE and the recorded DIGEST plus the corpus path — never a
    re-hash: this runs on the measurement's critical path, and a 2.8 GB hash per session would
    be paid by every run. The digest RE-DERIVATION stays at report time
    (`verify_corpus_bytes`); what the pin adds is that the identity being re-derived is the one
    the session STARTED with.

    # SCOPE, corrected (#3272 round 6, B2)

    F3's version of this docstring claimed the pin "covers everything a scan reads". That was
    FALSE in the only sense that matters: the component map was written and read by nothing, so
    the coverage was of the FILE, not of the CHECK. It is true now — `verify_session_corpus_pin`
    compares every pinned component's name, size and digest against both the report-time
    identity and the bytes on disk — and stated as what the pin covers rather than as what it
    records, because "recorded" was the word doing the misleading work.
    """
    pin = {
        "corpus": str(corpus),
        "rows": identity["rows"],
        "data_db_bytes": identity["data_db_bytes"],
        "data_db_sha256": identity["data_db_sha256"],
        # THE COMPLETE RECORDED COMPONENT SET (#3272 F3): every component's size and digest,
        # not just Data.db's. Copied from the identity rather than re-hashed — this is on the
        # measurement's critical path (see the note below).
        #
        # READ BACK by `verify_session_corpus_pin`, which compares this map against the
        # report-time identity AND the bytes on disk. That reader did NOT exist when F3 added
        # this field (#3272 round 6, B2, found independently by both reviewers): the map was
        # WRITTEN and compared against NOTHING ANYWHERE IN THE TREE, so the field read as a
        # guard while being inert. What F3 actually closed was `verify_corpus_components`,
        # against the corpus's OWN report-time `corpus-identity.json` — self-consistent at
        # report time, which is precisely the blind spot the PIN exists to cover.
        "components": {
            name: {"bytes": spec.get("bytes"), "sha256": spec.get("sha256")}
            for name, spec in sorted((identity.get("components") or {}).items())
            if isinstance(spec, dict)
        },
        # THE SCHEMA DIGEST (#3272 R2). `ws0-events.cql` is a MEASUREMENT INPUT — both arms
        # read it, ASYMMETRICALLY (the bare scan ingests it per invocation; the Flight ticket is
        # generated from it once) — so a modification between setup and a later rep makes the
        # two arms measure DIFFERENT SCHEMAS while every other recorded identity still agrees.
        # READ BACK by `ws0_schema_input.verify_pinned_schema`.
        "schema_sha256": identity.get("schema_sha256"),
        # THE FLIGHT TICKET'S DIGEST (#3272 round 10, M1). `ticket-template.json` IS THE REQUEST
        # — keyspace, table, DDL, token range, column projection, predicates, aggregation, limit —
        # and `flight-loadgen --ticket-template` re-reads it on EVERY invocation of every rep of
        # every arm. It used to be created AFTER this pin and to appear in no verified record, so it
        # could be changed between reps or between ARMS while the corpus stayed untouched: every
        # corpus digest, the component set and the schema all still agreed, and the report exited 0
        # having compared two arms that answered DIFFERENT QUERIES. Same class as round 10's F-B one
        # layer out (F-B: different corpora; this: different requests).
        #
        # DERIVED HERE, from the bytes on disk, rather than accepted as an argument — deliberately.
        # A caller-supplied digest is a value this writer would record without observing, and a
        # recorded value nobody measured is the shape this whole issue exists to remove; it would
        # also give the fixture path a way to pin a request it never wrote. So there is exactly one
        # implementation and the pin's ticket field can only ever describe a real file.
        #
        # The corollary for the DRIVER: the template must EXIST by the time this runs, i.e. it must
        # be created BEFORE the pin. That ordering is the other half of the fix, and this call is
        # what enforces it — an absent template is `Invalid` here, not an absent pin field.
        #
        # Measured from the SESSION DIR, not the corpus (#3272 round 13, F2): the ticket lives in the
        # exclusively-claimed output directory, so a concurrent session measuring the same corpus
        # cannot write the file this digest describes.
        #
        # READ BACK by `ws0_ticket_input.verify_pinned_ticket`.
        PIN_TICKET_FIELD: _measure_ticket_digest(session_dir),
        # THE MEASUREMENT CONFIGURATION (#3272 F1). Recorded here, before rep 1, and READ BACK
        # BY THE REPORTER as its own configuration — see `session_manifest_config` for why the
        # reporter reads it rather than matching against it.
        "config": dict(config or {}),
        # WHETHER THE CORPUS IS THE CANONICAL MEASUREMENT CORPUS (#3272 round 13, F3).
        #
        # The `config.baseline_mode` word above says which claim the RUN makes; this records the
        # COMPARISON that word rests on — which canonical fields were compared, against which
        # source file, and every divergence found. Recorded rather than derived at report time for
        # the same reason the corpus pin exists at all: the canonical pin can be re-pinned between
        # measurement and reporting, and a comparison performed at report time would then be
        # against a shape the session never ran against.
        #
        # DERIVED, never accepted as an argument: `require_canonical_or_declared` performs the
        # comparison and the driver passes its RECORD through. A caller-supplied verdict would be a
        # value this writer records without observing, which is the shape this whole issue removes.
        "canonical_corpus": dict(canonical or {}),
        "note": (
            "the corpus identity AND THE MEASUREMENT CONFIGURATION this session was STARTED"
            " against, stamped before the first rep. ws0_report.py REQUIRES it, refuses a"
            " report whose corpus no longer matches, and TAKES ITS CONFIGURATION FROM HERE"
            " rather than from its own command line — re-reporting an old session dir against"
            " a different corpus, or with fewer reps / a narrower arm set / different CPU pins,"
            " is otherwise invisible because all of it is self-consistent at report time"
            " (#3272 round 4 and round 5 F1/F3)."
        ),
    }
    session_pin_path(session_dir).write_text(json.dumps(pin, indent=1) + "\n")
    return pin


# The configuration fields a session is DEFINED by — every one of which changes what was
# measured, so every one must come from the session rather than from the reporting command line.
#
# `reps`/`scan_passes` are COUNTS (validated as CLI counts); `temps`/`arms` are SELECTIONS;
# the rest are opaque strings recorded verbatim.
#
# # THE DISPOSITION IS DECLARED, AND CHECKED IN BOTH DIRECTIONS (#3272 round 9, F7)
#
# This tuple had NO ORACLE, unlike its two siblings in this rig — `RECORD_FIELD_DISPOSITION` is
# walked against the live `StepRecord`, and `COMPARED_FIELDS` against the artifact's real key set.
# MEASURED: adding an 8th field to the tuple left `session_manifest_config` returning the same 7
# keys with no error, and adding an unclassified `config.prewarm_mode="DISABLED-ENTIRELY"` to a
# manifest was silently ignored, rc=0, absent from results.json. All 7 declared fields ARE
# currently read, so there was no live unread field — but nothing forced the next one, and this
# was the ROOT OF F6: `server_cpus` sat in this list as an opaque string and reached the report's
# "verified physical-core siblings" claim having been validated by nothing.
#
# So each field now DECLARES how it is validated, and `check_manifest_config_surface` closes both
# directions:
#
#   * DECLARED-BUT-UNREAD — every declared field must be produced by the reader (asserted at
#     import against `_MANIFEST_READER_KEYS`), so adding a field to the tuple without wiring it
#     is an ERROR rather than a key that silently never appears in results.json;
#   * PRESENT-BUT-UNCLASSIFIED — a manifest carrying a `config` key nobody declared is REFUSED,
#     so the next configuration field cannot be silently ignored the way `prewarm_mode` was.
#
# The `server_cpus` entry records what F6 established: it is opaque HERE (re-parsing it would be
# a second implementation of `cpu_list_expand`, which is the right call) and it is tied to a real
# verification ELSEWHERE, by `ws0_pinning.verify_pinning_record`. "Opaque" is only acceptable
# because something else is not.
MANIFEST_CONFIG_DISPOSITION: dict[str, str] = {
    "reps": "validated as a CLI COUNT (cli_count), so a hand-edited `reps: 0` cannot produce a"
            " vacuous-but-successful report (#3272 finding 5)",
    "scan_passes": "validated as a CLI COUNT, and cross-checked against the per-pass records by"
                   " the bare-scan collector (#3272 F2)",
    "temps": "validated as a NON-EMPTY SELECTION over TEMPS_ALLOWED",
    "arms": "validated as a NON-EMPTY SELECTION over ARMS_ALLOWED",
    "server_cpus": "a non-empty recorded STRING here — deliberately not re-parsed, because that"
                   " would be a second implementation of cpu_list_expand — and tied to a REAL"
                   " sibling verification by ws0_pinning.verify_pinning_record, which requires"
                   " the driver's record and requires this value to equal the list actually"
                   " verified (#3272 F6). Opaque here is only acceptable because it is checked"
                   " there.",
    "client_cpus": "as server_cpus: a non-empty recorded STRING, tied to the driver's recorded"
                   " verification by ws0_pinning.verify_pinning_record (#3272 F6)",
    "step_duration": "a non-empty recorded STRING (`<warm>/<cold>`), reported verbatim; the"
                     " DURATIONS that bound a rep were validated by the driver's own argument"
                     " checks (lib-args.sh) before the session ran",
    "baseline_mode": "validated as one of ws0_canonical_corpus.MODE_BASELINE /"
                     " MODE_NON_BASELINE, and — the substance — tied to a REAL pre-measurement"
                     " COMPARISON against the canonical pin in"
                     " tools/ws0-corpus-gen/src/measurement_corpus.rs, recorded in the pin's"
                     " `canonical_corpus` block. `baseline` is only recordable for a corpus that"
                     " was OBSERVED to match every canonical field, because the driver refuses"
                     " the run otherwise (#3272 round 13, F3); a noncanonical corpus can only"
                     " reach `non-baseline`, which the report LABELS in words. Opaque-string"
                     " recording would have been the F6 shape all over again",
}

# Declaration order preserved, and DERIVED from the disposition rather than written twice: two
# copies of the field list is the drift this issue keeps finding.
MANIFEST_CONFIG_FIELDS = tuple(MANIFEST_CONFIG_DISPOSITION)

# The keys `session_manifest_config` actually PRODUCES, besides its own `source`. Named here so
# the declared-but-unread direction can be asserted at import — a field added to the disposition
# and never wired would otherwise be a key that silently never reaches results.json.
_MANIFEST_READER_KEYS = (
    "reps",
    "scan_passes",
    "temps",
    "arms",
    "server_cpus",
    "client_cpus",
    "step_duration",
    "baseline_mode",
)

# AT IMPORT, both directions, so a half-wired field cannot ship (the pattern
# `ws0_loadgen_record.py` established for `ZERO_REQUIRED_COUNTERS`).
for _k in MANIFEST_CONFIG_FIELDS:
    if _k not in _MANIFEST_READER_KEYS:
        raise Invalid(
            f"`{_k}` is declared in MANIFEST_CONFIG_DISPOSITION but is not produced by"
            " session_manifest_config — a declared field the reader never emits is a"
            " configuration property the report claims to take from the session and does not"
            " (#3272 F7). Wire it, or remove the declaration."
        )
    if not MANIFEST_CONFIG_DISPOSITION[_k].strip():
        raise Invalid(f"`{_k}` must declare HOW it is validated, not an empty string")
for _k in _MANIFEST_READER_KEYS:
    if _k not in MANIFEST_CONFIG_DISPOSITION:
        raise Invalid(
            f"session_manifest_config produces `{_k}` but it is not declared in"
            " MANIFEST_CONFIG_DISPOSITION — every configuration field must record how it is"
            " validated, because an opaque field reaches the report as an unchecked claim"
            " (#3272 F6/F7)."
        )
del _k


def check_manifest_config_surface(path: pathlib.Path, config: dict) -> None:
    """Refuse a manifest `config` carrying a field nobody CLASSIFIED (#3272 round 9, F7).

    The mirror of `ws0_loadgen_record.check_record_surface`, for the configuration surface.
    MEASURED before this existed: adding `config.prewarm_mode = "DISABLED-ENTIRELY"` to a
    manifest was silently ignored — rc=0, absent from results.json — so a driver that grew a new
    configuration knob would have had it dropped on the floor by the reporter while the report
    continued to describe the session as fully characterised.

    Refused rather than warned, for the reason this whole issue exists: a configuration field
    nobody reads is a property of the measurement the report does not know about, and a report
    that does not know a knob was set cannot say anything true about what it measured.
    """
    unknown = sorted(k for k in config if k not in MANIFEST_CONFIG_DISPOSITION)
    if unknown:
        raise Invalid(
            f"{path} `config` carries field(s) this reporter has never classified:"
            f" {', '.join(unknown)}. Every configuration field must be declared in"
            " MANIFEST_CONFIG_DISPOSITION with HOW it is validated, because an unclassified"
            " field is one nobody reads: MEASURED, a `prewarm_mode` added to a manifest was"
            " silently ignored and absent from results.json (#3272 F7). Declare it — and if it"
            " can change what a figure means, VALIDATE it rather than recording it verbatim."
        )


def session_manifest_config(
    session_dir: pathlib.Path, temps_allowed: tuple[str, ...], arms_allowed: tuple[str, ...]
) -> dict:
    """THE session's measurement configuration, read FROM the manifest (#3272 F1).

    # The finding

    `ws0_report.py` took `--reps`, `--temps`, `--arms`, `--scan-passes`, the CPU pins and the
    step durations from ITS OWN command line. Nothing tied any of them to the session being
    reported, so re-reporting a measured session dir with a DIFFERENT configuration produced a
    successful, confident report:

    * `--reps 1` over a 3-rep session IGNORED two thirds of the measured artifacts and
      published rep 1 as the median of the run;
    * `--arms bypass` over a both-arms session silently dropped the `merge` arm and printed no
      PARTIAL MATRIX banner, because as far as the reporter knew, bypass was all that was asked
      for;
    * different `--server-cpus`/`--client-cpus` printed the REPLACEMENT pins under
      "pinning: … (verified physical-core siblings)" — a verification claim about CPUs the
      session never used;
    * a different `--scan-passes` mismatched the per-pass contract (see F2) or, before F2,
      nothing at all.

    In every case the report ASSERTED the replacement configuration had been verified.

    # Why the reporter READS the configuration rather than MATCHING its arguments against it

    Both close the finding; reading is strictly stronger, and the reason is the one this issue
    keeps rediscovering: **a value that cannot be supplied cannot disagree.** A matching design
    keeps seven parameters on the command line and adds seven comparisons — so it needs each
    comparison to be present and correct, and a parameter added later without a comparison is
    silently unchecked again (exactly F4's shape: a field nobody wired). Reading removes the
    input entirely: there is no `--reps` to be wrong, so there is no `--reps` check to omit.

    The residual cost of reading, stated rather than discovered: the reporter can no longer be
    pointed at a session with a deliberately different configuration. That is the POINT — such
    a report was never valid — and the honest way to report a subset is to say so, which is
    what the recorded selection and its PARTIAL MATRIX banner already do.

    The reporter's `--reps`/`--temps`/`--arms`/`--scan-passes`/CPU flags are therefore REMOVED
    rather than left as ignored arguments: an accepted-and-ignored flag is a silent lie to the
    operator who passed it.
    """
    p = session_pin_path(session_dir)
    if not p.exists():
        raise Invalid(
            f"this session dir carries no {SESSION_CORPUS_PIN} ({p}), so it does not record"
            " WHAT CONFIGURATION it measured. The reporter takes its reps, temperatures, arms,"
            " scan-passes and CPU pins FROM that record (#3272 F1), because taking them from"
            " the reporting command line let a re-report with fewer reps or a narrower arm set"
            " ignore measured artifacts and still claim the replacement configuration had been"
            " verified. Re-run the session with scripts/perf/ws0-baseline.sh, which stamps the"
            " manifest before the first rep."
        )
    try:
        pin = json.loads(p.read_text())
    except (OSError, ValueError) as exc:
        raise Invalid(f"{p} is not readable JSON: {exc}") from None
    if not isinstance(pin, dict):
        raise Invalid(f"{p} must hold a JSON object, got {type(pin).__name__}")
    config = pin.get("config")
    if not isinstance(config, dict) or not config:
        raise Invalid(
            f"{p} records no `config` — this session dir predates the configuration manifest"
            " (#3272 F1), so WHAT was measured (how many reps, which temperatures and arms,"
            " how many scan passes, which CPUs) is unknown. It cannot be supplied from the"
            " command line: that is exactly the substitution the manifest exists to prevent."
            " Re-run the session with the current driver."
        )
    absent = [f for f in MANIFEST_CONFIG_FIELDS if f not in config]
    if absent:
        raise Invalid(
            f"{p} `config` is INCOMPLETE — no {', '.join(absent)}. Every field of the"
            " configuration is one the report makes a claim about, so a partial manifest cannot"
            " establish what this session measured. Re-run the session with the current driver."
        )
    # ...and the OTHER direction (#3272 F7): a field PRESENT in the manifest that nobody
    # classified is refused, so the next configuration knob cannot be silently dropped the way a
    # `prewarm_mode` was (measured: rc=0, ignored, absent from results.json).
    check_manifest_config_surface(p, config)
    # Each field through the SAME validator the CLI used to apply, so a hand-edited manifest
    # cannot smuggle a vacuous configuration (`reps: 0` was #3272 finding 5) past the reader.
    out = {
        "reps": cli_count("reps", config["reps"]),
        "scan_passes": cli_count("scan-passes", config["scan_passes"]),
        "temps": nonempty_selection("temps", str(config["temps"]), temps_allowed),
        "arms": nonempty_selection("arms", str(config["arms"]), arms_allowed),
    }
    # A CLOSED SET, not a recorded string (#3272 round 13, F3). An unrecognised value is REFUSED
    # rather than reported verbatim, for the reason the roborev wrapper's verdict scan is a closed
    # grammar: a mode nobody planned for would otherwise reach the report as a claim, and the
    # report's whole job here is to say whether the run IS a baseline. `baseline` additionally
    # requires the recorded canonical COMPARISON below — the word alone establishes nothing.
    mode = config["baseline_mode"]
    if mode not in (MODE_BASELINE, MODE_NON_BASELINE):
        raise Invalid(
            f"{p} `config.baseline_mode` is {mode!r}, which is not"
            f" {MODE_BASELINE!r} or {MODE_NON_BASELINE!r}. An unrecognised mode is refused rather"
            " than reported verbatim: the report states whether this run is a WS0 baseline, and a"
            " value nobody planned for cannot support either answer (#3272 round 13, F3)."
        )
    out["baseline_mode"] = mode
    for key in ("server_cpus", "client_cpus", "step_duration"):
        value = config[key]
        if not isinstance(value, str) or not value.strip():
            raise Invalid(
                f"{p} `config.{key}` is {value!r}, which is not a recorded value. The report"
                f" prints it as a property of the measurement, so it must have been observed."
            )
        out[key] = value
    out["source"] = str(p)
    return out



def verify_session_corpus_pin(
    session_dir: pathlib.Path,
    corpus: pathlib.Path,
    identity: dict,
    measured_components: dict | None = None,
) -> dict:
    """Require the session's PRE-MEASUREMENT corpus pin, and require it to still match.

    REQUIRED, not optional: an absent pin means this session dir does not record which corpus
    it measured, and a report over it would attribute its figures to whatever `--corpus` the
    reader happened to pass. That is the fail-open shape — a check that silently does not run
    while the summary prints a digest as the measured one.

    Compared on all three of PATH, SIZE and DIGEST, each for a different reason:

    * the recorded DIGEST is the identity itself. A different digest is a different corpus.
    * the recorded SIZE is compared too, so a pin whose digest field was hand-edited to match
      still has to agree on a second, independent number.
    * the PATH is compared last and is the WEAKEST of the three — a corpus can legitimately be
      moved — so a path difference alone is REPORTED in the record rather than fatal. The two
      byte-level fields are what decide.

    ...and, since #3272 round 6 B2, THE COMPLETE PINNED COMPONENT SET, delegated to
    `_verify_pinned_components`. The three `Data.db` fields above cannot see an auxiliary
    component replaced mid-session, which is a real and silent way to change the read pattern
    being measured. That comparison is UNCONDITIONAL: it is not gated on a flag, because a pin
    component check that can be switched off is the fail-open shape one level out.
    """
    p = session_pin_path(session_dir)
    if not p.exists():
        raise Invalid(
            f"this session dir carries no {SESSION_CORPUS_PIN} ({p}), so it does not record"
            " WHICH CORPUS it measured. A report over it would attribute its figures to"
            " whatever --corpus the reader passed: re-reporting an old result dir against a"
            " different corpus is self-consistent AT REPORT TIME and therefore invisible to"
            " the report-time digest check (#3272 round 4). Re-run the session with"
            " scripts/perf/ws0-baseline.sh, which stamps the pin before the first rep."
        )
    try:
        pin = json.loads(p.read_text())
    except (OSError, ValueError) as exc:
        raise Invalid(f"{p} is not readable JSON: {exc}") from None
    if not isinstance(pin, dict):
        raise Invalid(f"{p} must hold a JSON object, got {type(pin).__name__}")
    for key in ("rows", "data_db_bytes", "data_db_sha256"):
        if key not in pin:
            raise Invalid(
                f"{p} carries no {key!r} — the session's corpus pin is incomplete, so it"
                " cannot establish which bytes this session measured"
            )
    pinned_rows = positive_int(f"{p}: 'rows'", pin["rows"])
    pinned_bytes = positive_int(f"{p}: 'data_db_bytes'", pin["data_db_bytes"])
    pinned_sha = pin["data_db_sha256"]
    if not isinstance(pinned_sha, str) or not _SHA256_RE.match(pinned_sha):
        raise Invalid(
            f"{p}: 'data_db_sha256' must be 64 lowercase hex characters (got"
            f" {pinned_sha!r}); a truncated pin cannot identify the measured bytes"
        )
    if pinned_sha != identity["data_db_sha256"]:
        raise Invalid(
            f"THE CORPUS CHANGED. This session was started against a corpus whose Data.db"
            f" sha256 is {pinned_sha} (stamped in {SESSION_CORPUS_PIN} before the first rep),"
            f" but --corpus {corpus} now records {identity['data_db_sha256']}. Every figure in"
            " this session was measured over the PINNED bytes; reporting it under this"
            " corpus's identity would attribute the measurements to bytes that were never"
            " measured. Two real ways to get here, both invisible to the report-time digest"
            " check because both are self-consistent at report time: re-reporting an old"
            " result dir against a different corpus, and a corpus regenerated (or written by"
            " another lane) DURING the run (#3272 round 4). Point --corpus at the corpus this"
            " session measured, or re-run the session."
        )
    if pinned_bytes != identity["data_db_bytes"] or pinned_rows != identity["rows"]:
        raise Invalid(
            f"THE CORPUS SHAPE CHANGED under this session. {SESSION_CORPUS_PIN} records"
            f" {pinned_rows:,} rows / {pinned_bytes:,} Data.db bytes; --corpus {corpus} now"
            f" records {identity['rows']:,} rows / {identity['data_db_bytes']:,} bytes."
            " The digest matched, so this is an identity file that was edited rather than"
            " regenerated — two independent numbers must agree, not one."
        )
    # THE PINNED COMPONENT SET — unconditional (#3272 round 6, B2). Raises on any divergence.
    # Imported function-locally, like the schema check below: both sibling modules import
    # `sha256_file`/`CORPUS_TABLE_SUBPATH` from THIS module, so a module-scope import here would
    # be a cycle. The split is by responsibility (see each module's docstring).
    from ws0_pin_components import verify_pinned_components

    comps = verify_pinned_components(p, pin, corpus, identity, measured_components)
    # ...and THE PINNED SCHEMA (#3272 round 6, R2), same argument: verifying the schema against
    # the corpus's own report-time identity cannot see a schema replaced mid-session with that
    # identity refreshed beside it. Imported here rather than at module scope because
    # `ws0_schema_input` imports `sha256_file` from THIS module — a function-local import keeps
    # the dependency one-directional at import time.
    from ws0_schema_input import verify_pinned_schema

    schema = verify_pinned_schema(p, pin, corpus, identity)
    # ...and THE FLIGHT TICKET (#3272 round 10, M1) — THE REQUEST. Same argument as the schema's,
    # one layer out: the schema decides how the bytes are INTERPRETED, the ticket decides WHICH
    # QUERY is asked. `flight-loadgen --ticket-template` re-reads it on every invocation of every
    # rep of every arm, and it was created AFTER this pin and recorded NOWHERE — so a template
    # changed between reps, or between ARMS, left the corpus untouched and therefore left every
    # corpus digest, the component set and the schema all in agreement while the report compared
    # two arms that answered different questions.
    #
    # UNCONDITIONAL, like the component and schema comparisons: a request check that can be
    # switched off is the fail-open shape one level out, and at a few hundred bytes a skip could
    # only buy a vacuous green. Function-local import for the cycle reason above.
    from ws0_ticket_input import verify_pinned_ticket

    # THE SESSION DIR, not the corpus (#3272 round 13, F2): the request is a property of THIS
    # session and lives in its exclusively-claimed output directory.
    ticket = verify_pinned_ticket(p, pin, session_dir)
    return {
        "pinned_before_measurement": True,
        "pinned_corpus_path": pin.get("corpus"),
        "pinned_data_db_sha256": pinned_sha,
        "pinned_data_db_bytes": pinned_bytes,
        "pinned_rows": pinned_rows,
        # The WEAKEST of the three comparisons, reported rather than enforced: a corpus can
        # legitimately be moved, and the two byte-level fields already decided the question.
        "corpus_path_unchanged": pin.get("corpus") == str(corpus),
        "pinned_components": comps["pinned_components"],
        "pinned_components_verified_size": comps["pinned_components_verified_size"],
        "pinned_components_verified_sha256": comps["pinned_components_verified_sha256"],
        "pinned_components_note": comps["note"],
        "pinned_schema_sha256": schema["pinned_schema_sha256"],
        "pinned_schema_note": schema["note"],
        # THE REQUEST (#3272 round 10, M1) — the Flight ticket, pinned before the first rep and
        # re-derived here from disk.
        "pinned_ticket_sha256": ticket["pinned_ticket_sha256"],
        "pinned_ticket_bytes": ticket["ticket_bytes"],
        "pinned_ticket_note": ticket["note"],
        "note": (
            "the corpus identity was captured in the session dir BEFORE the first rep and"
            " re-compared here on rows + data_db_bytes + sha256, PLUS the complete pinned"
            f" component set ({comps['pinned_components']} component(s)) against both the"
            " report-time identity and the bytes on disk, PLUS the SCHEMA and the FLIGHT TICKET"
            " (the request every Flight rep re-read); the path is reported, not enforced (a corpus"
            " may be moved)"
        ),
    }
