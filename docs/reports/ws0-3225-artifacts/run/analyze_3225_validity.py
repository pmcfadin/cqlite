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


def corpus_identity(arms, sha_file):
    """One corpus, or a named disagreement. AC6 needs the exact bytes named.

    Every arm writes its own corpus-basis.json; if two arms disagree on the staged
    bytes then their curves are not comparable and the peak table is meaningless.
    Checked field-by-field and reported as FAIL rather than averaged away.

    The metadata fields (path, file count, byte sizes) are NECESSARY but NOT
    SUFFICIENT: a different Data.db of the same size at the same path satisfies every
    one of them. Byte identity is a claim about CONTENT, so it is answered by a
    CONTENT measurement — the per-arm `data_db_sha256` that run-3225.sh records
    immediately before and after each arm. Three states, and only the first is a pass:

      PASS       every arm recorded a digest, they are all equal, and they equal the
                 digest in the committed shasum artifact;
      FAIL       a recorded digest disagrees with another arm's or with the artifact,
                 or an arm recorded that the corpus CHANGED under it;
      UNVERIFIED an arm recorded NO digest. That is not agreement — it is an absent
                 measurement, and an absent measurement never yields a positive
                 verdict. Arms swept before the digest stamp existed land here by
                 construction, which is the honest answer for them.
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
    out["ok"] = metadata_ok and digest_ok

    if out["ok"]:
        state, verdict = "PASS", (
            "PASS: every arm recorded a sha256 of the bytes it read, all arms' digests are "
            "equal, and they match the committed artifact (%s)." % sha)
    elif out["digest_disagreements"] or out["disagreements"] or not metadata_ok:
        state, verdict = "FAIL", "FAIL: " + "; ".join(filter(None, [
            sha_err,
            "arms without a corpus-basis.json: %s" % ", ".join(out["missing_basis_arms"])
            if out["missing_basis_arms"] else None,
            "; ".join(out["disagreements"]) or None,
            "; ".join(out["digest_disagreements"]) or None,
            None if ref else "no arm published a corpus basis",
        ]))
    else:
        state, verdict = "UNVERIFIED", (
            "UNVERIFIED — NOT a pass: %d arm(s) recorded NO sha256 of the bytes they read "
            "(%s). Their corpus-basis.json agrees with the others on stage path, file count "
            "and byte sizes, but those are metadata: a different file of the same size at the "
            "same path satisfies every one of them. Byte identity across these arms is "
            "therefore UNMEASURED. run-3225.sh now stamps the digest per arm; arms swept "
            "before that fix cannot be retro-certified, because a digest taken today records "
            "today's bytes, not the bytes that arm read." % (
                len(out["arms_without_recorded_digest"]),
                ", ".join(out["arms_without_recorded_digest"])))
    out["state"] = state
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
