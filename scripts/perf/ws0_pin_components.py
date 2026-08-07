#!/usr/bin/env python3
"""THE PINNED COMPONENT SET, compared (#3272 review round 6, B2).

Split out of `ws0_session.py` under the campsite rule: adding this reader plus R2's schema
verification pushed that file past the ~800-line source target, so this is a split by
RESPONSIBILITY rather than a waiver. The seam across the three session modules:

    ws0_session.py        the SESSION's identity — which corpus, which configuration?
    ws0_pin_components.py the PINNED COMPONENT SET — are all the corpus's parts the pinned ones?
    ws0_schema_input.py   the SCHEMA's identity — which schema was it read WITH?

# The finding, in one sentence

Round 5's F3 added the complete component set to the session pin; the writer wrote it, the driver
printed its count, and **nothing anywhere in the tree ever read it** — so the field read as a
guard while being inert. This module is the reader.
"""

from __future__ import annotations

import pathlib

from ws0_session import CORPUS_TABLE_SUBPATH, SESSION_CORPUS_PIN
from ws0_validate import Invalid, _SHA256_RE, positive_int


def verify_pinned_components(
    pin_path: pathlib.Path,
    pin: dict,
    corpus: pathlib.Path,
    identity: dict,
    measured: dict | None = None,
) -> dict:
    """Compare the PINNED component map against the report-time identity AND the bytes on disk.

    # The finding (#3272 round 6, B2 — found independently by BOTH reviewers)

    Round 5's F3 fix added the complete component set to the pin. `write_session_corpus_pin`
    wrote it; the driver even printed its count. **Nothing ever read it.** A grep of the whole
    tree found no consumer, so `verify_session_corpus_pin` still compared only `rows`,
    `data_db_bytes` and `data_db_sha256` — and the sibling `verify_corpus_components` closes F3
    only against the corpus's OWN report-time `corpus-identity.json`, i.e. against a file that
    can be refreshed beside the component it describes.

    The triggering state, which is what makes this a report-integrity defect rather than an
    unused field: regenerate or replace `Index.db` between rep 1 and the report and refresh
    `corpus-identity.json` beside it, leaving `Data.db` untouched. The pin's three fields still
    match. The identity is self-consistent with disk. So the report EXITS 0 and prints

        corpus comps : all 5 recorded component(s) were re-stat'ed and 5 of 5 re-hashed

    — an affirmative FULL-verification claim, while an auxiliary component that shapes the read
    pattern being measured is not the one that was measured. Written-but-unread data reading as
    a guard is this issue's defining defect class.

    # What is compared, and why all four

    * the pinned NAME SET, both directions. A component that DISAPPEARED changes the read path
      (an absent `Index.db` is a different scan); a component that APPEARED means the directory
      is no longer the pinned corpus.
    * every component's SIZE, against the identity's record AND against `stat` on disk. Two
      independent comparisons because the identity is a file an editor can rewrite, and disk is
      the thing actually read.
    * every component's DIGEST, likewise against both — the same-length-different-bytes case a
      size check cannot see.

    The digest comparison against the IDENTITY is free (both are recorded strings). Against DISK
    it needs a hash of multi-GB files, which `verify_corpus_components` has ALREADY performed in
    the same report over the same files — so `measured` is that result (its per-component
    `sha256_measured`), passed in rather than re-derived. Two reasons it is a PARAMETER and not
    an internal re-hash: hashing 2.8 GB twice per report is a real cost, and a second derivation
    is a second implementation whose disagreement with the first would be undiagnosable.

    Under `--skip-corpus-digest` no digest was observed at all. This then reports the SCOPE of
    what was compared (name + size, both sources) instead of treating an unobserved digest as a
    match — a value not observed is never a pass. `measured` absent is likewise reported as
    unobserved rather than silently skipped.
    """
    pinned = pin.get("components")
    if not isinstance(pinned, dict) or not pinned:
        raise Invalid(
            f"{pin_path} records no `components` map, so the identity captured BEFORE"
            " measurement covers only Data.db. A scan also reads Index.db and the"
            " Statistics/Summary/Filter components that shape how it reads, and those can be"
            " replaced mid-session with corpus-identity.json refreshed beside them — which is"
            " self-consistent at report time and therefore invisible to the report-time"
            " component check (#3272 round 6, B2). Re-run the session with the current driver,"
            " which pins the complete component set before the first rep."
        )
    current = identity.get("components")
    if not isinstance(current, dict) or not current:
        raise Invalid(
            f"{corpus / 'corpus-identity.json'} records no `components` map, so the pinned"
            f" component set in {pin_path} cannot be compared against anything. The pin names"
            f" {len(pinned)} component(s); refusing rather than reporting a comparison that did"
            " not happen."
        )
    table_dir = corpus.joinpath(*CORPUS_TABLE_SUBPATH)
    vanished = sorted(set(pinned) - set(current))
    appeared = sorted(set(current) - set(pinned))
    if vanished or appeared:
        raise Invalid(
            "THE CORPUS COMPONENT SET CHANGED under this session."
            + (f" Pinned but now absent: {', '.join(vanished)}." if vanished else "")
            + (f" Present but never pinned: {', '.join(appeared)}." if appeared else "")
            + " The session was started against the pinned set and every figure was measured"
            " over it; a component that came or went changes what a scan reads. Point --corpus"
            " at the corpus this session measured, or re-run the session."
        )
    checked_sha = 0
    for name in sorted(pinned):
        spec = pinned[name]
        if not isinstance(spec, dict):
            raise Invalid(
                f"{pin_path}: pinned component {name!r} is a {type(spec).__name__}, not a"
                " record with its size and digest, so it cannot establish what was measured"
            )
        pin_bytes = positive_int(f"{pin_path}: component {name!r} 'bytes'", spec.get("bytes"))
        pin_sha = spec.get("sha256")
        if not isinstance(pin_sha, str) or not _SHA256_RE.match(pin_sha):
            raise Invalid(
                f"{pin_path}: pinned component {name!r} records 'sha256' {pin_sha!r}, which is"
                " not 64 lowercase hex characters — a truncated pin cannot identify the bytes"
                " this session measured"
            )
        cur = current.get(name)
        if not isinstance(cur, dict):
            raise Invalid(
                f"{corpus / 'corpus-identity.json'}: component {name!r} is a"
                f" {type(cur).__name__}, not a record, so the pinned component cannot be"
                " compared against it"
            )
        if cur.get("bytes") != pin_bytes:
            raise Invalid(
                f"CORPUS COMPONENT {name} CHANGED SIZE under this session:"
                f" {SESSION_CORPUS_PIN} pinned {pin_bytes:,} bytes before the first rep, the"
                f" corpus now records {cur.get('bytes')!r}. This component is read by the scan"
                " (or shapes how it reads), so the figures were measured over a different"
                " corpus than the one being reported (#3272 round 6, B2)."
            )
        if cur.get("sha256") != pin_sha:
            raise Invalid(
                f"CORPUS COMPONENT {name} CHANGED under this session: {SESSION_CORPUS_PIN}"
                f" pinned sha256 {pin_sha} before the first rep, the corpus now records"
                f" {cur.get('sha256')!r}. This is the mid-session replacement the report-time"
                " component check CANNOT see — refreshing corpus-identity.json beside the"
                " replaced component leaves the corpus self-consistent at report time while"
                " Data.db, and therefore the pin's other three fields, are unchanged"
                " (#3272 round 6, B2). Re-run the session."
            )
        # ...and against DISK, which is the thing actually read. The identity is a file; an
        # editor can make it agree with the pin without the bytes agreeing with either.
        path = table_dir / name
        try:
            disk_bytes = path.stat().st_size
        except OSError as exc:
            raise Invalid(
                f"pinned corpus component {name} cannot be stat'ed at {path}: {exc}. The pin"
                " names it as part of the measured corpus, so an unreadable component means the"
                " corpus being reported is not the one measured."
            ) from None
        if disk_bytes != pin_bytes:
            raise Invalid(
                f"pinned corpus component {name} is {disk_bytes:,} bytes ON DISK but"
                f" {SESSION_CORPUS_PIN} pinned {pin_bytes:,}. The recorded identity agreed with"
                " the pin, so this is an identity file that was edited rather than regenerated"
                " — the pin is compared against BOTH the identity and the bytes for exactly"
                " this case."
            )
        # The DISK digest: the one `verify_corpus_components` already derived in this same report
        # over this same file. ABSENT means no digest was OBSERVED (`--skip-corpus-digest`, or an
        # entry that recorded none), which is reported as scope below, never as a match.
        entry = (measured or {}).get(name)
        disk_sha = entry.get("sha256_measured") if isinstance(entry, dict) else None
        if disk_sha is not None:
            if disk_sha != pin_sha:
                raise Invalid(
                    f"pinned corpus component {name} hashes to {disk_sha} ON DISK but"
                    f" {SESSION_CORPUS_PIN} pinned {pin_sha}. Both sizes matched, so this is the"
                    " same-length-different-bytes case neither a size check nor the recorded"
                    " identity can see."
                )
            checked_sha += 1
    return {
        "pinned_components": len(pinned),
        "pinned_components_verified_size": len(pinned),
        "pinned_components_verified_sha256": checked_sha,
        "note": (
            f"all {len(pinned)} component(s) pinned BEFORE the first rep were re-compared"
            f" against the report-time identity AND the bytes on disk;"
            f" {checked_sha} of {len(pinned)} digests were re-derived from disk"
            if checked_sha
            else f"all {len(pinned)} component(s) pinned BEFORE the first rep were re-compared"
            " on NAME and SIZE against both the report-time identity and the bytes on disk; NO"
            " digest was re-derived (--skip-corpus-digest), so no component's CONTENT was"
            " confirmed to be the pinned content"
        ),
    }

