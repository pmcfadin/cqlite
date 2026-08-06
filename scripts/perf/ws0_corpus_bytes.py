#!/usr/bin/env python3
"""THE CORPUS'S BYTES, MEASURED — never a sidecar's claim about them (#3272 review round 21).

Split out of `ws0_session.py` under the campsite rule: that file was at 885 lines against a ~800
target before this round added anything, so this is a split by RESPONSIBILITY rather than a waiver.
The seam across the four session modules:

    ws0_session.py        the SESSION's identity — which corpus, which configuration?
    ws0_corpus_bytes.py   the CORPUS's BYTES — what is actually on disk, right now?
    ws0_pin_components.py the PINNED COMPONENT SET — are all the corpus's parts the pinned ones?
    ws0_schema_input.py   the SCHEMA's identity — which schema was it read WITH?

# The finding (round 21): THE PIN COPIED A CLAIM INSTEAD OF HASHING THE BYTES

`write_session_corpus_pin` built the pre-measurement pin by COPYING `data_db_sha256` and the whole
`components` map out of `corpus-identity.json`. The corpus's own sidecar was therefore both the
subject and the oracle: the pin and the sidecar agreed BY CONSTRUCTION, whatever the bytes on disk
had done. A pin that copies a claim is not a measurement — it is a restatement of the thing it was
supposed to check, and the same class #3042 records (both sides make the identical mistake, so the
round-trip closes while the real property is false).

The concrete sequence it let through, which is why the direction of the bias matters: MUTATE a
component DURING measurement and RESTORE it BEFORE reporting. Every recorded identity check then
passes — the pin equals the sidecar because it was copied from it, and the report-time re-hash sees
the restored bytes — while different repetitions measured DIFFERENT BYTES. The failure makes a
session that measured inconsistent bytes report as a clean, identity-verified one: it biases TOWARD
the claim, which is why it is refused rather than captioned.

# The fix, in two halves

* `measure_component_digests` HASHES THE FILES. The pin's digests are now OBSERVED values, and the
  sidecar becomes something to COMPARE AGAINST — a disagreement is an ERROR naming the component,
  never a value silently copied over. A component that cannot be hashed FAILS CLOSED; "assume
  unchanged" is not available.
* `verify_corpus_boundary` re-hashes the ACTUAL bytes at a MEASUREMENT BOUNDARY and compares them
  against the pin, refusing the rep and naming the component that changed. Each boundary appends an
  OBSERVATION to `corpus-boundary-observations.jsonl` in the session dir, so what was verified, and
  when, is a record rather than a claim. This is the half a pre/post pair cannot cover: a mutation
  that is restored before reporting is invisible at both ends and visible only from INSIDE the run.

The residual, stated rather than left to be discovered: the boundary verifier must be CALLED per
rep by the measurement driver, which is a different file and a different owner. Until it is, this
module is what the self-tests drive and what a driver has to call — one function, one CLI entry
point, no arguments to get wrong.
"""

from __future__ import annotations

import hashlib
import json
import pathlib
import sys

from ws0_validate import Invalid, _SHA256_RE, positive_int


# Where a ws0 corpus's SSTable components live, relative to the corpus root — the
# layout `ws0-corpus-gen` writes and both measurement arms resolve.
CORPUS_TABLE_SUBPATH = ("ws0", "events")

# Read the Data.db in 8 MiB slices. The measurement corpus is ~2.8 GB, so the digest
# must stream: reading it whole would need 2.8 GB of RSS to verify a fixture.
_DIGEST_CHUNK = 8 << 20

# The name of the identity the DRIVER stamps into the session dir BEFORE it measures anything.
# Distinct from the corpus's own `corpus-identity.json`, which lives beside the data and can be
# replaced under a session at any time.
SESSION_CORPUS_PIN = "session-corpus-pin.json"

# Where each MEASUREMENT-BOUNDARY observation is appended (one JSON object per line). A record of
# what was re-hashed and when, so the boundary checks that ran are readable rather than assumed.
BOUNDARY_OBSERVATIONS = "corpus-boundary-observations.jsonl"


def session_pin_path(session_dir: pathlib.Path) -> pathlib.Path:
    return session_dir / SESSION_CORPUS_PIN


def boundary_observations_path(session_dir: pathlib.Path) -> pathlib.Path:
    return session_dir / BOUNDARY_OBSERVATIONS


def sha256_file(path: pathlib.Path) -> str:
    """Streaming lowercase-hex sha256 of `path` (constant memory, any file size).

    FAILS CLOSED: an unreadable file raises `Invalid` naming it, rather than returning a sentinel a
    caller could treat as "unchanged". A digest that could not be derived is never a match.
    """
    h = hashlib.sha256()
    try:
        with path.open("rb") as fh:
            while True:
                chunk = fh.read(_DIGEST_CHUNK)
                if not chunk:
                    break
                h.update(chunk)
    except OSError as exc:
        raise Invalid(
            f"{path} could not be read to derive its sha256: {exc}. A component whose bytes cannot"
            " be hashed is NOT assumed unchanged — an unobserved digest is never a match (#3272"
            " round 21)."
        ) from None
    return h.hexdigest()


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


def measure_component_digests(corpus: pathlib.Path, identity: dict) -> dict:
    """HASH the corpus's components, and COMPARE the sidecar against what was measured.

    # The finding (#3272 round 21)

    The pre-measurement pin copied `data_db_sha256` and the whole `components` map out of
    `corpus-identity.json`. So the pin could not DISAGREE with that sidecar — it was derived from
    it — and every downstream comparison against the pin was a comparison against the sidecar's
    own assertion, restated. #3249's bar is not "the guard exists" but "the guard has been OBSERVED
    to fire", and a digest copied from a file can never fire against that file: it is a hardcoded
    verdict with extra steps.

    # What this returns, and what it refuses

    Every recorded component (and the `Data.db`) is STAT'ed and HASHED from disk. The sidecar's
    corresponding values are then compared against the measured ones and a disagreement is an
    ERROR NAMING THE COMPONENT — the sidecar is retained as something to compare against, never as
    a source of truth. An unhashable component fails closed (see `sha256_file`).

    Note the direction of the trust: the returned digests are the MEASURED ones. A caller writing
    them into a pin is recording an observation, so a pin built from this cannot describe bytes
    nobody read.
    """
    recorded = identity.get("components")
    if not isinstance(recorded, dict) or not recorded:
        raise Invalid(
            f"{corpus / 'corpus-identity.json'} records no `components` map, so there is no"
            " recorded component set to MEASURE against. A pin over it could only describe"
            " Data.db, and a scan reads Index.db plus the Statistics/Summary/Filter components"
            " that shape how it reads. Regenerate the corpus with tools/ws0-corpus-gen, which"
            " records every emitted component."
        )
    data_db = locate_corpus_data_db(corpus)
    table_dir = corpus.joinpath(*CORPUS_TABLE_SUBPATH)
    measured: dict[str, dict] = {}
    for name in sorted(recorded):
        spec = recorded[name]
        if not isinstance(spec, dict):
            raise Invalid(
                f"{corpus / 'corpus-identity.json'}: component {name!r} is a"
                f" {type(spec).__name__}, not a record with its size and digest"
            )
        rec_bytes = positive_int(
            f"corpus-identity.json: component {name!r} 'bytes'", spec.get("bytes")
        )
        rec_sha = spec.get("sha256")
        if not isinstance(rec_sha, str) or not _SHA256_RE.match(rec_sha):
            raise Invalid(
                f"corpus-identity.json: component {name!r} records 'sha256' {rec_sha!r}, which is"
                " not 64 lowercase hex characters — a truncated or absent digest cannot identify"
                " the component that was measured"
            )
        path = table_dir / name
        try:
            disk_bytes = path.stat().st_size
        except OSError as exc:
            raise Invalid(
                f"corpus component {name} cannot be stat'ed at {path}: {exc}. The recorded"
                " identity names it as part of this corpus, so an unreadable component means the"
                " corpus that would be measured is not the one described. It is NOT assumed"
                " unchanged (#3272 round 21)."
            ) from None
        disk_sha = sha256_file(path)
        if disk_bytes != rec_bytes or disk_sha != rec_sha:
            raise Invalid(
                f"CORPUS COMPONENT {name} DISAGREES WITH THE RECORDED IDENTITY at pin time:"
                f" {corpus / 'corpus-identity.json'} records {rec_bytes:,} bytes / {rec_sha},"
                f" the bytes on disk are {disk_bytes:,} / {disk_sha}. The pin records MEASURED"
                " digests (#3272 round 21) — it used to COPY these values, which made the pin and"
                " the sidecar agree by construction however the bytes differed — so a sidecar that"
                " does not describe the corpus is refused HERE, before the first rep, rather than"
                " pinned as though it had been observed. Regenerate the corpus, or point the run at"
                " the corpus this identity was recorded from."
            )
        measured[name] = {"bytes": disk_bytes, "sha256": disk_sha}
    # The Data.db must be one of the recorded components, checked BEFORE it is subscripted below —
    # an unrecorded Data.db is a sidecar describing a different corpus, and diagnosing it as a
    # KeyError would blame the wrong artifact.
    if data_db.name not in measured:
        raise Invalid(
            f"{data_db.name} is the *-Data.db that would be measured, but the recorded identity's"
            f" `components` map does not describe it (it names {', '.join(sorted(measured))})."
            " The pin's Data.db digest is MEASURED from this file, so an unrecorded Data.db means"
            " the sidecar describes a different corpus."
        )
    # ...and the sidecar's TOP-LEVEL `data_db_bytes`/`data_db_sha256`, which is a SECOND recorded
    # copy of the same fact and can disagree with both the `components` map and disk. Compared HERE,
    # at pin time, and that placement is the substance: `verify_corpus_bytes` catches a stale
    # top-level digest at report time ONLY when digests are not skipped, so under
    # `--skip-corpus-digest` this was the one recorded identity nothing ever hashed. The pin's own
    # hash is on the SETUP path — once per session, not once per rep — so no flag scopes it away.
    top_sha = identity.get("data_db_sha256")
    top_bytes = identity.get("data_db_bytes")
    if top_sha != measured[data_db.name]["sha256"] or top_bytes != measured[data_db.name]["bytes"]:
        raise Invalid(
            f"{corpus / 'corpus-identity.json'} records data_db_bytes {top_bytes!r} /"
            f" data_db_sha256 {top_sha!r}, but {data_db.name} is"
            f" {measured[data_db.name]['bytes']:,} bytes hashing to"
            f" {measured[data_db.name]['sha256']}. The sidecar's top-level identity is a SECOND"
            " recorded copy of the same fact and it does not describe the bytes on disk, so a pin"
            " taken from it would name a corpus that is not there. Refused at PIN time, before the"
            " first rep: the report-time check only re-derives this digest when"
            " --skip-corpus-digest is absent, so under that flag this was the one recorded identity"
            " nothing ever hashed (#3272 round 21). Regenerate the corpus, or measure the corpus"
            " this identity was recorded from."
        )
    return {
        "data_db": data_db.name,
        "data_db_bytes": measured[data_db.name]["bytes"],
        "data_db_sha256": measured[data_db.name]["sha256"],
        "components": measured,
        # AFFIRMATIVE and specific: the pin carries WHERE its digests came from, so a reader can
        # tell an observed identity from a copied one without inferring it from a field's absence.
        "source": "measured from the component bytes on disk (never copied from"
                  " corpus-identity.json, which is COMPARED against these values)",
    }


def verify_corpus_boundary(
    session_dir: pathlib.Path, corpus: pathlib.Path, label: str, record: bool = True
) -> dict:
    """Re-hash the ACTUAL component bytes at a MEASUREMENT BOUNDARY, against the PIN.

    # Why a boundary check, and not just a pre/post pair

    A pin taken before rep 1 and a re-hash at report time are BOTH blind to the sequence in the
    finding: mutate a component DURING measurement and restore it BEFORE reporting. Both ends see
    the original bytes, every recorded identity check passes, and the reps in between measured
    different bytes. The only place that mutation is visible is from INSIDE the run — hence a check
    at each boundary, comparing the bytes that are there NOW against the pin.

    # What it compares, and what it refuses

    Every component the pin names is STAT'ed and HASHED from disk and compared against the PIN's
    recorded size and digest — not against `corpus-identity.json`, which can be refreshed beside a
    replaced component and is therefore self-consistent at every boundary. A divergence is an ERROR
    that REFUSES the rep and NAMES the component. An unhashable component fails closed.

    Each call appends an observation to `corpus-boundary-observations.jsonl`, so the boundaries that
    were actually verified are a record. `record=False` is available to a caller that only wants the
    verdict (the report path, which has its own record) — it never suppresses a refusal.
    """
    p = session_pin_path(session_dir)
    if not p.exists():
        raise Invalid(
            f"this session dir carries no {SESSION_CORPUS_PIN} ({p}), so there is nothing for a"
            " measurement boundary to verify the corpus AGAINST. The pin is stamped before the"
            " first rep; a boundary check without it would report that it had verified the corpus"
            " while comparing against nothing."
        )
    try:
        pin = json.loads(p.read_text())
    except (OSError, ValueError) as exc:
        raise Invalid(f"{p} is not readable JSON: {exc}") from None
    if not isinstance(pin, dict):
        raise Invalid(f"{p} must hold a JSON object, got {type(pin).__name__}")
    pinned = pin.get("components")
    if not isinstance(pinned, dict) or not pinned:
        raise Invalid(
            f"{p} records no `components` map, so a boundary check could only cover Data.db —"
            " while a scan reads Index.db and is shaped by the Statistics/Summary/Filter"
            " components. Re-run the session with the current driver, which pins the complete"
            " component set before the first rep."
        )
    table_dir = corpus.joinpath(*CORPUS_TABLE_SUBPATH)
    if not table_dir.is_dir():
        raise Invalid(
            f"{table_dir} is not a directory at boundary {label!r}, so the pinned corpus is not"
            " there to be verified. The measurement cannot continue over bytes that are absent."
        )
    present = {q.name for q in table_dir.iterdir() if q.is_file()}
    vanished = sorted(set(pinned) - present)
    appeared = sorted(present - set(pinned))
    if vanished or appeared:
        raise Invalid(
            f"THE CORPUS COMPONENT SET CHANGED DURING MEASUREMENT (boundary {label})."
            + (f" Pinned but now absent: {', '.join(vanished)}." if vanished else "")
            + (f" Present but never pinned: {', '.join(appeared)}." if appeared else "")
            + " Reps before this boundary measured the pinned set; a component that came or went"
            " changes what a scan reads, and restoring it before the report would leave every"
            " recorded identity check passing (#3272 round 21). This session cannot be reported."
        )
    checked = 0
    for name in sorted(pinned):
        spec = pinned[name]
        if not isinstance(spec, dict):
            raise Invalid(
                f"{p}: pinned component {name!r} is a {type(spec).__name__}, not a record with its"
                " size and digest, so a boundary check has nothing to compare against"
            )
        pin_bytes = positive_int(f"{p}: component {name!r} 'bytes'", spec.get("bytes"))
        pin_sha = spec.get("sha256")
        if not isinstance(pin_sha, str) or not _SHA256_RE.match(pin_sha):
            raise Invalid(
                f"{p}: pinned component {name!r} records 'sha256' {pin_sha!r}, which is not 64"
                " lowercase hex characters — a truncated pin cannot identify the measured bytes"
            )
        path = table_dir / name
        try:
            disk_bytes = path.stat().st_size
        except OSError as exc:
            raise Invalid(
                f"pinned corpus component {name} cannot be stat'ed at {path} at boundary {label}:"
                f" {exc}. It is NOT assumed unchanged — a component whose bytes cannot be observed"
                " has not been verified."
            ) from None
        disk_sha = sha256_file(path)
        if disk_bytes != pin_bytes or disk_sha != pin_sha:
            raise Invalid(
                f"THE CORPUS CHANGED DURING MEASUREMENT: component {name} is {disk_bytes:,} bytes"
                f" / {disk_sha} at boundary {label}, but {SESSION_CORPUS_PIN} pinned"
                f" {pin_bytes:,} / {pin_sha} before the first rep. Reps on either side of this"
                " boundary measured DIFFERENT BYTES. Restoring the component before the report"
                " would leave the pin, the sidecar and the report-time re-hash all in agreement —"
                " which is why this is checked HERE, inside the run, and not only at the ends"
                " (#3272 round 21). This session cannot be reported."
            )
        checked += 1
    observation = {
        "boundary": label,
        "corpus": str(corpus),
        "components_verified": checked,
        "components_pinned": len(pinned),
        "verified_against": SESSION_CORPUS_PIN,
        "note": (
            f"all {checked} pinned component(s) were re-stat'ed and re-hashed FROM DISK at this"
            " boundary and compared against the pin — not against corpus-identity.json, which can"
            " be refreshed beside a replaced component and is self-consistent at every boundary"
        ),
    }
    if record:
        try:
            with boundary_observations_path(session_dir).open("a") as fh:
                fh.write(json.dumps(observation) + "\n")
        except OSError as exc:
            raise Invalid(
                f"the boundary observation for {label!r} could not be recorded in"
                f" {boundary_observations_path(session_dir)}: {exc}. A verification nobody can read"
                " back is not evidence, so this is an error rather than a silent omission."
            ) from None
    return observation


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


def _main(argv: list[str]) -> int:
    """`ws0_corpus_bytes.py <session-dir> <corpus> <boundary-label>` — one measurement boundary.

    The entry point a measurement driver calls between reps. It exists as a CLI because the driver
    is shell: one command, three positional paths, non-zero and a named component on any change, so
    there is no argument list to get wrong and no verdict for the caller to interpret.
    """
    if len(argv) != 4:
        print(
            "usage: ws0_corpus_bytes.py <session-dir> <corpus> <boundary-label>\n"
            "       re-hashes the pinned corpus components FROM DISK and refuses (exit 1) if any"
            " of them changed since the pre-measurement pin",
            file=sys.stderr,
        )
        return 2
    try:
        obs = verify_corpus_boundary(
            pathlib.Path(argv[1]), pathlib.Path(argv[2]), argv[3]
        )
    except Invalid as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        return 1
    print(
        f"corpus boundary {obs['boundary']}: {obs['components_verified']} of"
        f" {obs['components_pinned']} pinned component(s) re-hashed from disk and unchanged"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv))
