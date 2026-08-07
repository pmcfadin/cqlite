#!/usr/bin/env python3
"""Validity checks for analyze-3225.py — corpus identity and the admission ceiling.

Split out of analyze-3225.py to keep both files near the ~800-line campsite target
(CLAUDE.md). These two are the checks that decide whether the measured curve is a
measurement AT ALL, so they live together: each answers a question the numbers
themselves cannot answer, and each fails CLOSED — a positive verdict here requires an
affirmative measurement, never the absence of a bad signal. Not a CLI — imported by
name (underscored, unlike the hyphenated harness executables) from the script beside it.
"""
from __future__ import annotations

import datetime
import glob
import hashlib
import os
import re


def read_corpus_data_db_sha(sha_file: str):
    """sha256 of the corpus Data.db, PARSED from the committed shasum artifact.

    Fail-closed: an unreadable file, no `*-Data.db` line, or more than one
    distinct Data.db digest is returned as an error rather than as a digest,
    because AC6 requires naming the exact bytes every arm read.
    """
    try:
        with open(sha_file) as fh:
            lines = fh.read().splitlines()
    except OSError as exc:
        return None, "unreadable (%s: %s)" % (type(exc).__name__, exc)
    digests = set()
    for line in lines:
        parts = line.split()
        if len(parts) >= 2 and parts[-1].endswith("-Data.db"):
            digests.add(parts[0])
    if not digests:
        return None, "no '*-Data.db' line in %s" % sha_file
    if len(digests) > 1:
        return None, "%d distinct Data.db digests in %s — the corpus is not one file" % (
            len(digests), sha_file)
    return digests.pop(), None


def read_prep_geometry_sha(geometry_file: str):
    """The prep-time Data.db sha256 as recorded in the committed geometry table.

    A SECOND committed witness of the same prep measurement, written by
    compare-geometry.py from the shasum artifact at corpus-preparation time. Requiring
    both to exist and agree means the seal below rests on two committed records rather
    than on one file anyone could have rewritten. Fail-closed on unreadable, absent or
    ambiguous, exactly like the shasum artifact.
    """
    try:
        with open(geometry_file) as fh:
            text = fh.read()
    except OSError as exc:
        return None, "unreadable (%s: %s)" % (type(exc).__name__, exc)
    found = sorted(set(re.findall(r"^\s*this run \(#\d+\)\s*:\s*([0-9a-f]{64})\s*$",
                                  text, re.MULTILINE)))
    if not found:
        return None, "no 'this run (#NNNN) : <sha256>' line in %s" % geometry_file
    if len(found) > 1:
        return None, "%d distinct 'this run' digests in %s" % (len(found), geometry_file)
    return found[0], None


def _parse_utc(ts):
    """'2026-08-06T22:54:59Z' -> epoch seconds, or None. None is never a time."""
    if not isinstance(ts, str):
        return None
    try:
        return datetime.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=datetime.timezone.utc).timestamp()
    except ValueError:
        return None


def _sha256_file(path: str):
    h = hashlib.sha256()
    try:
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
    except OSError as exc:
        return None, "%s: %s" % (type(exc).__name__, exc)
    return h.hexdigest(), None


def bracketed_seal(arms, sha_file, geometry_file, now_epoch=None):
    """Was the staged corpus SEALED across the whole measurement window?

    The second of the three named methods (see corpus_identity). It is a DIFFERENT
    oracle from per-arm digests, not a weaker reading of the same one: the digest
    recorded at PREP, before the first arm, is compared against the file re-measured
    NOW, after the last arm, and the file's mtime is required to predate the first arm's
    start. Any write to the file — including a swap-and-restore — moves mtime, so an
    mtime older than the window plus content equal to the prep record is an affirmative
    measurement that the staged bytes did not change at any point during the sweep.

    WHAT IT DOES NOT PROVE, and this must not be read past: it seals the staged FILE.
    It does not independently witness that each arm OPENED that exact path. That link
    comes from each arm's own corpus-basis.json recording the same `stage_dir`,
    `data_db_files` and `ondisk_compressed_bytes` — which is therefore checked HERE, as
    part of the seal verdict, rather than left implicit next to it. The residual after
    both halves: an arm could in principle have been pointed at a different path whose
    basis fields coincide. Per-arm digests close that; the seal does not. Two further
    limits: mtime is forgeable by a deliberate `touch`, so the seal is evidence against
    accidental modification and swap-and-restore, not against an adversary; and it
    requires the staged file to still exist, so it expires when the box is reclaimed.

    Every sub-condition is keyed on its AFFIRMATIVE value and every one must hold.
    """
    out = {"method": "bracketed-seal", "checks": [], "ok": False}

    def check(name, ok, detail):
        out["checks"].append({"check": name, "ok": bool(ok), "detail": detail})
        return bool(ok)

    # ---- the two committed PREP records -------------------------------------
    prep_sha, prep_err = read_corpus_data_db_sha(sha_file)
    ok_shasum = check("prep-sha-shasum-artifact", prep_sha is not None,
                      prep_err or "%s records %s" % (os.path.basename(sha_file), prep_sha))
    geo_sha, geo_err = read_prep_geometry_sha(geometry_file)
    ok_geo = check("prep-sha-geometry-record", geo_sha is not None,
                   geo_err or "%s records %s" % (os.path.basename(geometry_file), geo_sha))
    ok_prep = check(
        "prep-records-agree",
        bool(ok_shasum and ok_geo and prep_sha == geo_sha),
        ("both committed prep records name %s" % prep_sha) if (ok_shasum and ok_geo and prep_sha == geo_sha)
        else "the two committed prep records do not both name one digest (shasum=%s, geometry=%s)"
             % (prep_sha, geo_sha))

    # ---- what the ARMS say they read ----------------------------------------
    bases = [(a["arm"], a["corpus_basis"]) for a in arms]
    present = [(lab, cb) for lab, cb in bases if cb.get("present")]
    fields = ("stage_dir", "data_db_files", "ondisk_compressed_bytes")
    disagreeing = []
    # `{}`, never None: every read below goes through ref.get(...), and today those are
    # safe only because an earlier conjunct short-circuits first. That is exactly the
    # kind of implicit protection this round is about — one reordering away from an
    # AttributeError inside the check that is supposed to fail closed. An empty basis
    # makes every affirmative sub-condition read None and refuse, which is the intent.
    ref = present[0][1] if present else {}
    for lab, cb in present:
        for f in fields:
            if cb.get(f) is None:
                disagreeing.append("%s: %s is absent" % (lab, f))
            elif cb.get(f) != ref.get(f):
                disagreeing.append("%s: %s=%r but %s has %r"
                                   % (lab, f, cb.get(f), present[0][0], ref.get(f)))
    ok_arms = check(
        "arms-agree-on-the-staged-file",
        bool(present) and len(present) == len(bases) and not disagreeing,
        ("all %d arm(s) record stage_dir=%s, %s *-Data.db, %s on-disk bytes"
         % (len(present), ref.get("stage_dir"), ref.get("data_db_files"),
            ref.get("ondisk_compressed_bytes"))) if (present and len(present) == len(bases)
                                                     and not disagreeing)
        else "; ".join(disagreeing) or "not every arm published a corpus basis (%d of %d)"
                                       % (len(present), len(bases)))

    stage_dir = ref.get("stage_dir")
    data_dbs = sorted(glob.glob(os.path.join(stage_dir, "**", "*-Data.db"), recursive=True)) \
        if stage_dir else []
    ok_present = check(
        "staged-file-still-present",
        bool(stage_dir) and len(data_dbs) == 1 and ref.get("data_db_files") == 1,
        ("%s" % data_dbs[0]) if len(data_dbs) == 1 and ref.get("data_db_files") == 1
        else "found %d *-Data.db under %r; the seal needs the ONE file the prep digest names"
             % (len(data_dbs), stage_dir))
    staged = data_dbs[0] if ok_present else None

    size = os.path.getsize(staged) if staged else None
    check("staged-bytes-match-the-recorded-basis",
          size is not None and size == ref.get("ondisk_compressed_bytes"),
          "%s bytes on disk vs %s recorded" % (size, ref.get("ondisk_compressed_bytes")))

    now_sha, sha_read_err = _sha256_file(staged) if staged else (None, "no staged file to read")
    check("current-sha-matches-prep",
          bool(ok_prep and now_sha is not None and now_sha == prep_sha),
          ("re-measured NOW = %s, identical to the prep record" % now_sha)
          if (ok_prep and now_sha == prep_sha)
          else "re-measured %s vs prep %s (%s)" % (now_sha, prep_sha, sha_read_err or "differ"))

    # ---- the WINDOW: first arm start, last point, file mtime ----------------
    starts = {a["arm"]: _parse_utc((a.get("run_config") or {}).get("started_utc")) for a in arms}
    unparsed = sorted(lab for lab, t in starts.items() if t is None)
    ok_window = check(
        "every-arm-start-time-known",
        bool(starts) and not unparsed,
        "earliest arm start %s" % (
            datetime.datetime.fromtimestamp(min(t for t in starts.values() if t is not None),
                                            datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            if starts and not unparsed else "-")
        if not unparsed else "no parseable started_utc in run-config.json for: %s"
                             % ", ".join(unparsed))
    first_start = min((t for t in starts.values() if t is not None), default=None)

    mtime = os.path.getmtime(staged) if staged else None
    check("mtime-predates-the-first-arm",
          bool(ok_window and mtime is not None and first_start is not None and mtime < first_start),
          ("mtime %s < first arm start %s, and any write would have moved it, so the file was "
           "unmodified across the whole window"
           % (datetime.datetime.fromtimestamp(mtime, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
              datetime.datetime.fromtimestamp(first_start, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")))
          if (mtime is not None and first_start is not None and mtime < first_start)
          else "mtime=%s first-arm-start=%s — the staged file was touched at or after the sweep began"
               % (mtime, first_start))

    last_point = max((a.get("points_ts_unix_ms_max") or 0) for a in arms) / 1000.0 if arms else 0
    now_epoch = now_epoch if now_epoch is not None else datetime.datetime.now(
        datetime.timezone.utc).timestamp()
    check("seal-measured-after-the-last-arm",
          bool(last_point) and now_epoch > last_point,
          "re-measured at %s, after the last recorded point at %s" % (
              datetime.datetime.fromtimestamp(now_epoch, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
              datetime.datetime.fromtimestamp(last_point, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
              if last_point else "-")
          if last_point and now_epoch > last_point
          else "no point timestamp to close the window against (last_point=%s)" % last_point)

    out["staged_file"] = staged
    out["prep_sha256"] = prep_sha
    out["current_sha256"] = now_sha
    out["failed_checks"] = [c["check"] for c in out["checks"] if not c["ok"]]
    # Affirmative: every named sub-condition returned its GOOD value. One unmeasured
    # condition is one hole, and a seal with a hole is not a seal.
    out["ok"] = bool(out["checks"]) and not out["failed_checks"]
    out["verdict"] = (
        "SEALED: the staged Data.db re-measured now is byte-identical to the digest recorded at "
        "prep, and its mtime predates the first arm, so it was unmodified across the entire "
        "measurement window. Every arm's corpus-basis.json names that same path, file count and "
        "byte size. RESIDUAL: this seals the FILE and the arms' recorded basis — it does not "
        "independently witness each arm opening that path, which only a per-arm digest does."
        if out["ok"] else
        "NOT SEALED: %s" % ", ".join(out["failed_checks"]))
    return out


def corpus_identity(arms, sha_file, geometry_file=None, now_epoch=None):
    """One corpus, or a named disagreement. AC6 needs the exact bytes named.

    Every arm writes its own corpus-basis.json; if two arms disagree on the staged
    bytes then their curves are not comparable and the peak table is meaningless.
    Checked field-by-field and reported as FAIL rather than averaged away.

    The metadata fields (path, file count, byte sizes) are NECESSARY but NOT
    SUFFICIENT: a different Data.db of the same size at the same path satisfies every
    one of them. Byte identity is a claim about CONTENT, so it is answered by a
    CONTENT measurement. There are exactly THREE named methods, and the one used is
    always printed, because they do not prove the same thing:

      per-arm-digest   STRONGEST, and the method for every future round: run-3225.sh
                       records `data_db_sha256` per arm, measured before and after that
                       arm. All arms' digests must be equal AND equal the committed
                       shasum artifact.
      bracketed-seal   A DIFFERENT valid oracle, for a round whose harness stamped no
                       per-arm digest: the digest recorded at prep (two committed
                       records) equals the file re-measured after the last arm, the
                       file's mtime predates the first arm, and every arm's basis names
                       that same path/count/size. See bracketed_seal() for exactly what
                       this does and does not prove — it is NOT equivalent to per-arm
                       digests, and the residual is printed with the verdict.
      unverified       Neither is available. NOT a pass, and a non-zero exit. An absent
                       measurement is not agreement.

    A CONTRADICTION is always FAIL and is never rescued by the seal: if a recorded
    per-arm digest disagrees with another arm's or with the artifact, or an arm recorded
    that the corpus changed under it, no other method may overturn that.
    """
    fields = ("stage_dir", "data_db_files", "ondisk_compressed_bytes",
              "logical_uncompressed_bytes", "sstables_compressed", "sstables_uncompressed")
    sha, sha_err = read_corpus_data_db_sha(sha_file)
    out = {
        "sha256_data_db": sha,
        "sha256_source": sha_file,
        "sha256_error": sha_err,
        "per_arm": [],
        "disagreements": [],
        "missing_basis_arms": [a["arm"] for a in arms if not a["corpus_basis"].get("present")],
        "arms_without_recorded_digest": [],
        "digest_disagreements": [],
    }
    ref = None
    ref_digest = None
    ref_digest_arm = None
    for a in arms:
        cb = a["corpus_basis"]
        rec = {"arm": a["arm"], "present": cb.get("present", False),
               "rows_per_scan_observed": a.get("rows_per_scan_observed")}
        if cb.get("present"):
            rec.update({k: cb.get(k) for k in fields})
            if ref is None:
                ref = rec
            else:
                for k in fields + ("rows_per_scan_observed",):
                    if rec.get(k) != ref.get(k):
                        out["disagreements"].append(
                            "%s: %s = %r, but %s has %r" % (a["arm"], k, rec.get(k),
                                                            ref["arm"], ref.get(k)))
        else:
            rec["reason"] = cb.get("reason")

        # ---- the CONTENT half, evaluated on affirmative evidence only ----------
        # A manifest digest covers a multi-file corpus; the single-file digest is
        # what the committed shasum artifact can be compared against. Either one
        # present counts as "this arm measured its bytes"; neither does not.
        digest = cb.get("data_db_sha256") if cb.get("present") else None
        manifest = cb.get("data_db_sha256_manifest") if cb.get("present") else None
        rec["data_db_sha256"] = digest
        rec["data_db_sha256_manifest"] = manifest
        rec["data_db_sha256_basis"] = cb.get("data_db_sha256_basis") if cb.get("present") else None
        digest_error = cb.get("data_db_sha256_error") if cb.get("present") else None
        if digest_error:
            rec["data_db_sha256_error"] = digest_error
            out["digest_disagreements"].append("%s: %s" % (a["arm"], digest_error))
        if digest is None and manifest is None:
            out["arms_without_recorded_digest"].append(a["arm"])
            rec["digest_state"] = (
                "NOT RECORDED — this arm's corpus-basis.json carries no data_db_sha256. "
                "The arm predates the per-arm digest stamp, or the stamp failed. Its bytes "
                "are UNVERIFIED, not agreed.")
        else:
            key = digest or manifest
            if ref_digest is None:
                ref_digest, ref_digest_arm = key, a["arm"]
                rec["digest_state"] = "recorded (reference for the arm-to-arm comparison)"
            elif key != ref_digest:
                out["digest_disagreements"].append(
                    "%s: data_db digest %s, but %s recorded %s" % (
                        a["arm"], key, ref_digest_arm, ref_digest))
                rec["digest_state"] = "RECORDED AND DIFFERENT from %s" % ref_digest_arm
            else:
                rec["digest_state"] = "recorded, equal to %s" % ref_digest_arm
        out["per_arm"].append(rec)

    # The recorded digests must also equal the digest in the committed artifact —
    # arms agreeing with each other about the WRONG corpus is still a wrong corpus.
    recorded_singles = {r["data_db_sha256"] for r in out["per_arm"] if r.get("data_db_sha256")}
    out["digest_matches_artifact"] = None
    if recorded_singles and sha is not None:
        bad = sorted(d for d in recorded_singles if d != sha)
        out["digest_matches_artifact"] = not bad
        if bad:
            out["digest_disagreements"].append(
                "recorded per-arm digest(s) %s do not match the committed artifact's %s (%s)"
                % (", ".join(bad), sha, sha_file))

    out["reference"] = ref
    out["reference_digest"] = ref_digest
    metadata_ok = bool(ref) and not out["disagreements"] and not out["missing_basis_arms"] \
        and sha is not None
    # Affirmative: every arm measured, every measurement agreed, and the measurement
    # was checked against the independent artifact. Anything else is not a pass.
    digest_ok = (not out["digest_disagreements"]
                 and not out["arms_without_recorded_digest"]
                 and ref_digest is not None
                 and out["digest_matches_artifact"] is True)
    out["metadata_ok"] = metadata_ok
    out["digest_ok"] = digest_ok

    # The SECOND method, always evaluated and always published, so a reader can see
    # which methods were available rather than only which one answered.
    if geometry_file is None:
        geometry_file = os.path.join(os.path.dirname(sha_file), "corpus-geometry.txt")
    seal = bracketed_seal(arms, sha_file, geometry_file, now_epoch=now_epoch)
    out["bracketed_seal"] = seal

    contradiction = bool(out["digest_disagreements"] or out["disagreements"]) or not metadata_ok
    if metadata_ok and digest_ok:
        state, method = "PASS", "per-arm-digest"
        verdict = ("PASS (method=per-arm-digest): every arm recorded a sha256 of the bytes it "
                   "read, all arms' digests are equal, and they match the committed artifact "
                   "(%s)." % sha)
    elif contradiction:
        # Never rescued by the seal: a measurement that DISAGREES is not an absent one.
        state, method = "FAIL", "contradicted"
        verdict = "FAIL: " + "; ".join(filter(None, [
            sha_err,
            "arms without a corpus-basis.json: %s" % ", ".join(out["missing_basis_arms"])
            if out["missing_basis_arms"] else None,
            "; ".join(out["disagreements"]) or None,
            "; ".join(out["digest_disagreements"]) or None,
            None if ref else "no arm published a corpus basis",
        ]))
    elif seal["ok"]:
        state, method = "PASS", "bracketed-seal"
        verdict = ("PASS (method=bracketed-seal): no arm recorded a per-arm digest, so byte "
                   "identity is established by the seal instead — %s" % seal["verdict"])
    else:
        state, method = "UNVERIFIED", "unverified"
        verdict = (
            "UNVERIFIED — NOT a pass: %d arm(s) recorded NO sha256 of the bytes they read "
            "(%s), and the bracketed seal does not hold either (%s). Their corpus-basis.json "
            "agrees with the others on stage path, file count and byte sizes, but those are "
            "metadata: a different file of the same size at the same path satisfies every one "
            "of them. Byte identity is therefore UNMEASURED. A per-arm digest cannot be "
            "backfilled — a digest taken today records today's bytes, not the bytes that arm "
            "read." % (len(out["arms_without_recorded_digest"]),
                       ", ".join(out["arms_without_recorded_digest"]),
                       ", ".join(seal["failed_checks"]) or "not evaluated"))
    out["state"] = state
    out["method"] = method
    out["ok"] = state == "PASS"
    out["verdict"] = verdict
    return out


def admission_ceiling(arms):
    """Did the admission ceiling COVER the ramp? Read from the record, not assumed.

    This is the sound form of the check the round-1 report made with
    `requests_unavailable == 0`. That inference does not hold: the sweep runs with
    `--admission-wait-timeout-ms 30000`, so a request arriving over the ceiling does
    not fail — it WAITS for a permit and then SUCCEEDS. A throttled curve therefore
    reports ZERO rejections. Zero is corroborating; it is not probative.

    What IS probative is the ceiling itself: every point stamps `server_flags`, so the
    effective `--max-concurrent-scans` can be read per point and compared against the
    largest N that arm actually drove. Fail closed on anything unreadable — an
    unparseable ceiling is an unknown ceiling, not a fine one.
    """
    out = {"per_arm": [], "problems": []}
    for a in arms:
        flags = a.get("server_flags") or ""
        ceilings = set()
        toks = str(flags).split()
        for i, t in enumerate(toks):
            if t == "--max-concurrent-scans" and i + 1 < len(toks):
                ceilings.add(toks[i + 1])
        ramp = a.get("ramp_measured") or []
        rec = {"arm": a["arm"], "ramp_max_N": max(ramp) if ramp else None,
               "server_flags": flags or None,
               "max_concurrent_scans_recorded": sorted(ceilings) or None,
               "max_concurrent_scans_uniform_across_points":
                   a.get("server_flags_uniform_across_points")}
        problem = None
        if not ramp:
            problem = "%s: no N was measured, so there is no ramp to cover" % a["arm"]
        elif len(ceilings) != 1:
            problem = ("%s: server_flags records %d distinct --max-concurrent-scans value(s) "
                       "(%s) — the effective ceiling is not determinable"
                       % (a["arm"], len(ceilings), ", ".join(sorted(ceilings)) or "none"))
        elif not next(iter(ceilings)).isdigit():
            problem = "%s: --max-concurrent-scans %r is not an integer" % (
                a["arm"], next(iter(ceilings)))
        elif a.get("server_flags_uniform_across_points") is False:
            problem = ("%s: server_flags is NOT identical across this arm's points, so one "
                       "ceiling cannot describe the arm" % a["arm"])
        else:
            ceiling = int(next(iter(ceilings)))
            rec["max_concurrent_scans"] = ceiling
            rec["covers_ramp"] = ceiling >= max(ramp)
            if not rec["covers_ramp"]:
                problem = ("%s: ceiling %d < max(N) %d — every N above the ceiling measured the "
                           "admission gate, not the curve (and would still report 0 rejections, "
                           "because over-ceiling requests WAIT and then succeed)"
                           % (a["arm"], ceiling, max(ramp)))
        if problem:
            rec["covers_ramp"] = False
            rec["problem"] = problem
            out["problems"].append(problem)
        out["per_arm"].append(rec)
    out["ok"] = bool(out["per_arm"]) and not out["problems"]
    out["verdict"] = (
        "PASS: every arm recorded --max-concurrent-scans >= the largest N it drove, so the "
        "ceiling could not bind. This — not the rejection count — is the evidence: with a "
        "30 s admission wait timeout a throttled point still reports 0 rejections."
        if out["ok"] else
        "FAIL: " + ("; ".join(out["problems"]) if out["problems"]
                    else "no arm was checked, so the ceiling is unverified"))
    return out


def evidence_completeness(arms):
    """Were the counters the validity claims rest on actually RECORDED on every point?

    Every "0 rejections", "0 client-saturated reps" and "the ceiling was 64" statement
    reads a per-point field. Python renders a missing field as None, and None flows
    through `or 0`, `x == 0` and `if p.get(...)` as the CLEAN answer — so an unstamped
    counter and a counter that measured zero are indistinguishable downstream. This
    check is the difference: it asserts the fields were PRESENT, so the zeros above are
    measurements rather than absences. Fails closed, per arm, naming the field.
    """
    out = {"per_arm": [], "problems": []}
    for a in arms:
        missing = a.get("point_fields_missing") or {}
        rec = {"arm": a["arm"], "points": a.get("points_total"), "fields_missing": missing}
        if not a.get("points_total"):
            rec["problem"] = "%s: no points at all" % a["arm"]
        elif missing:
            rec["problem"] = "%s: %s" % (a["arm"], "; ".join(
                "%s absent on %d of %d point(s)" % (f, n, a["points_total"])
                for f, n in sorted(missing.items())))
        if rec.get("problem"):
            out["problems"].append(rec["problem"])
        out["per_arm"].append(rec)
    out["ok"] = bool(out["per_arm"]) and not out["problems"]
    out["verdict"] = (
        "PASS: every point in every arm carries the counters the validity claims read, so "
        "the zeros reported above are measured zeros, not absent fields."
        if out["ok"] else
        "FAIL: " + ("; ".join(out["problems"]) if out["problems"]
                    else "no arm was checked, so evidence completeness is unverified"))
    return out
