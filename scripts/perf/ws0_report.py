#!/usr/bin/env python3
"""Aggregate one `ws0-baseline.sh` session into results.json + a human summary.

Reporting rules this file enforces, from issue #3096 spec R1/R2 and hardened by
issue #3272:

* Every figure is **rows/s AND cycles/row**. There is deliberately no code path
  here that emits a CPU-SHARE ("% of cycles in X"): a share can fall while rows/s
  is unmoved, which the spec records as a FAIL, so the rig never produces the
  number that could be mistaken for a win.
* **Warm and cold are separate rows**, never averaged into one claim. Every warm
  rep of BOTH arms carries the outcome of an untimed prewarm (`prewarm`,
  `prewarm_all_ok`); an unrecorded or failed one is flagged in the summary, because
  an unprewarmed "warm" rep is a partly-cold measurement wearing a warm label. The
  cold arm's `skipped-cold-arm` sentinel satisfies the requirement for a COLD rep
  ONLY — on a warm rep it is fatal (#3272 finding 2).
* The **median** of N reps is reported and the **spread** (min..max, and its
  percentage of the median) is printed beside it. No silent mean.
* **Setup is subtracted** from the bare scan's cycles: the driver measured a
  `--setup-only` leg under its own perf window, and `cycles_scan =
  cycles_total - cycles_setup`. Both counters must be OBSERVED — an absent or
  uncounted one is an error, never a `0` that would make "setup-subtracted" a lie
  (#3272 finding 4).
* The **row denominator is printed with every figure**, so no derived number is
  divisible by an unstated count.
* **Zero rows exits non-zero** rather than reporting a measurement.
* The **request count is asserted per temperature**, not inferred: a cold Flight rep
  must be exactly ONE successful request (requests 2..N would be warm), and every
  rep's rows must be `requests_ok x corpus_rows` — an exact number of full corpus
  scans. A rep that violates either is refused rather than reported. The corpus row
  count is REQUIRED, so this can never be silently skipped (#3272 finding 1).
* The **SELECTION** (which temperatures and arms this session ran) is recorded in
  `results.json` and printed in the summary, so a narrow run cannot later be read
  as a full matrix (#3272 finding 6).

Every fail-closed decision lives in `ws0_validate.py`; this file aggregates what
that module permits. There is no environment variable that relaxes any of it.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from ws0_collect import (  # noqa: E402  (path set above; stdlib-only, no deps)
    collect_scan,
    prewarm_warning,
)
# ARM B lives in its own module since #3272's F2 split: one file per MEASUREMENT ARM, which is
# the seam the rig is built around (the two arms are separate claims measured through different
# surfaces with different contracts).
from ws0_flight_arm import collect_flight  # noqa: E402
# THE ARROW-VOLUME CAVEAT, BESIDE THE FIGURES (#3272 round 20). Rounds 18/19 stated the withdrawal
# in `results.json` and in ONE bullet at the bottom of the NOTES; a reader of the summary's numbers
# and its PASS / BELOW TARGET verdicts saw nothing. Imported from the module that owns the claim's
# wording, so the summary text and the record cannot describe it differently.
from ws0_content_volume import (  # noqa: E402
    content_volume_caveat_lines,
    content_volume_verdict_caveat_lines,
)
# THE STANDING NOTES — the session-INVARIANT claims and non-claims printed under every report. Its
# own module since #3272 round 22 (ws0_report.py was at the ~800-line source target), along the seam
# `ws0_content_volume` already follows: the wording of a claim lives with the module that owns the
# fact, and what was left here was the ~90 lines belonging to no single check.
from ws0_report_notes import (  # noqa: E402
    content_volume_note_lines,
    counting_note_lines,
    executed_arm_note_lines,
    fixture_scope_note_lines,
    selection_and_request_note_lines,
)
from ws0_rounds import (  # noqa: E402
    collect_recorded_round_metadata,
    paired_rounds,
    recorded_round_metadata_lines,
)
from ws0_validate import (  # noqa: E402
    Invalid,
    existing_dir,
    load_corpus_identity,
    positive_derived,
)
# The SESSION's identity — which corpus, which configuration — lives in its own module since
# #3272 F1/F3 (ws0_validate.py was already at 855 lines against a ~800 target).
from ws0_session import (  # noqa: E402
    session_manifest_config,
    session_pin_path,
    verify_corpus_bytes,
    verify_corpus_components,
    verify_session_corpus_pin,
)
# The SCHEMA as a verified measurement input — its own module since #3272 R2 (ws0_session.py was
# at the ~800-line source target exactly, so this is a split by responsibility, not a waiver).
from ws0_schema_input import verify_schema_input  # noqa: E402
# IS THIS CORPUS THE CANONICAL MEASUREMENT CORPUS — #3272 round 13, F3. The pre-measurement pin
# recorded the identity of whatever corpus it was handed and compared it against NOTHING, so a
# smoke-sized or differently-seeded corpus was self-consistent through every check here and
# published as a WS0 BASELINE. The comparison is made BEFORE measurement and RECORDED; this reads
# the record and re-derives its verdict.
from ws0_canonical_record import verify_pinned_canonical_corpus  # noqa: E402
# The CPU PINNING's recorded verification — #3272 round 9 F6. The reporter printed "verified
# physical-core siblings" about manifest strings nothing had checked; the driver now records what
# it verified and this asserts the manifest agrees with it.
from ws0_pinning import verify_pinning_record  # noqa: E402
from ws0_binaries import verify_binary_provenance  # noqa: E402
# DID EVERY MEASUREMENT BOUNDARY THIS SESSION OWED ACTUALLY HAPPEN — #3272 round 22. Round 21 wrote
# the boundary record and round 22 wired the check that produces it; NOTHING READ IT. Own module
# because ws0_report.py was at the ~800-line source target; full argument there.
from ws0_boundary_observations import (  # noqa: E402
    boundary_observation_lines,
    boundary_observation_note_lines,
    verify_boundary_observations,
)

TEMPS_ALLOWED = ("warm", "cold")
ARMS_ALLOWED = ("bypass", "merge")
def fmt(label: str, block: dict) -> str:
    rps, cpr = block["rows_per_sec"], block["cycles_per_row"]
    return (
        f"  {label:<34} {rps['median']:>12,.0f} rows/s  "
        f"[{rps['min']:,.0f}..{rps['max']:,.0f}, spread {rps['spread_pct_of_median']:.1f}%]   "
        f"{cpr['median']:>10,.0f} cycles/row "
        f"[{cpr['min']:,.0f}..{cpr['max']:,.0f}, {cpr['spread_pct_of_median']:.1f}%]   "
        f"IPC {block['ipc']['median']:.2f}   rows={block['row_denominator_total']:,} "
        f"(n={rps['n']})"
    )


def selection_lines(temps: list[str], arms: list[str], reps: int) -> list[str]:
    """The SELECTION, stated in the human summary (#3272 finding 6).

    Completeness is judged against what the caller SELECTED, so the selection has
    to be visible: otherwise a `--temp warm --arm bypass` session reads exactly
    like a full warm+cold x bypass+merge matrix that happened to print fewer rows.
    """
    full = len(temps) == len(TEMPS_ALLOWED) and len(arms) == len(ARMS_ALLOWED)
    lines = [
        f"selection    : temperatures [{' '.join(temps)}] x arms [{' '.join(arms)}]"
        f" x {reps} rep(s)"
        f"  ({len(temps) * len(arms) * reps} measured legs per arm-pair)",
    ]
    if not full:
        lines.append(
            "               !! PARTIAL MATRIX — this session ran only the selection"
            f" above (full = temperatures [{' '.join(TEMPS_ALLOWED)}] x arms"
            f" [{' '.join(ARMS_ALLOWED)}]). Absent combinations were NOT MEASURED"
            " here; do not read this report as covering them."
        )
    return lines


def corpus_identity_lines(verification: dict) -> list[str]:
    """State whether the printed corpus digest was OBSERVED or merely recorded.

    The line above prints `corpus sha256:` from `corpus-identity.json`. Pre-#3272 it
    was never compared against anything, so a reader could not tell a re-derived
    digest from a recorded one. Now the distinction is printed, and the unverified
    case is loud — the digest is what binds every #3096 figure to a specific corpus.
    """
    if verification["sha256_verified"]:
        return [
            "corpus verify: size AND sha256 re-derived from "
            f"{pathlib.Path(verification['data_db']).name} at report time "
            f"({verification['data_db_bytes_measured']:,} B) — the identity describes "
            "the bytes that were measured",
        ]
    return [
        "corpus verify: !! CORPUS DIGEST UNVERIFIED (--skip-corpus-digest) — the size "
        f"matched ({verification['data_db_bytes_measured']:,} B) but the sha256 above "
        "is the RECORDED value, NOT one observed from "
        f"{pathlib.Path(verification['data_db']).name}.",
        "               Anything citing this report's corpus identity is citing an "
        "unverified digest; re-run without the flag before publishing a comparison.",
    ]


def build_report(args: argparse.Namespace) -> tuple[dict, list[str]]:
    """The whole report, or `Invalid`. No fabricated value anywhere in here."""
    d = existing_dir("dir", args.dir)
    corpus = existing_dir("corpus", args.corpus)
    # THE CONFIGURATION COMES FROM THE SESSION, NOT FROM THIS COMMAND LINE (#3272 F1).
    #
    # `--reps`, `--temps`, `--arms`, `--scan-passes`, `--server-cpus`, `--client-cpus` and
    # `--step-duration` used to be arguments HERE, tied to nothing. Re-reporting a measured
    # session with fewer reps IGNORED the surplus artifacts and published rep 1 as the run's
    # median; a narrower `--arms` silently dropped an arm with no PARTIAL MATRIX banner; a
    # different `--server-cpus` printed the REPLACEMENT pins under a "verified physical-core
    # siblings" claim about CPUs the session never used. Each produced a confident report
    # asserting that the replacement configuration had been verified.
    #
    # The flags are REMOVED rather than validated against the manifest: a value that cannot be
    # supplied cannot disagree, so there is no comparison left to omit — and an
    # accepted-but-ignored flag is a silent lie to whoever passed it. See
    # `ws0_session.session_manifest_config`.

    # REQUIRED, fail-closed: an absent identity used to silently disable the
    # full-corpus-per-request assert while the NOTES claimed it ran (#3272 f1).
    identity = load_corpus_identity(corpus)
    # …and the recorded identity is checked against the BYTES ACTUALLY PRESENT
    # (#3272 review B6). Reading the identity file only ever established that the
    # file was self-consistent; stale metadata beside different bytes misidentified
    # the corpus while the report printed the recorded digest as the measured one.
    identity_verification = verify_corpus_bytes(
        corpus, identity, skip_digest=args.skip_corpus_digest
    )
    # ...and EVERY OTHER RECORDED COMPONENT (#3272 F3). Verifying `Data.db` alone left the
    # auxiliary components unchecked, although a scan reads `Index.db` and is shaped by the
    # Statistics/Summary/Filter components — so a modified auxiliary file could change measured
    # behaviour while the report claimed corpus verification had succeeded. Which components
    # must exist comes from the recorded identity, never a hardcoded list.
    component_verification = verify_corpus_components(
        corpus, identity, skip_digest=args.skip_corpus_digest
    )
    # ...and THE SCHEMA (#3272 round 6, R2). `ws0-events.cql` is a MEASUREMENT INPUT that was
    # outside every verification the rig performed: absent from the Data.db check, absent from
    # the component check (it is not in the table directory), absent from the pin. The driver
    # only tested it was READABLE. And the two arms read it ASYMMETRICALLY — the bare scan
    # ingests it on every invocation, the Flight ticket is generated from it once at setup — so
    # a modification between them makes the arms measure DIFFERENT SCHEMAS while the report
    # stays valid by its own account. No skip flag: the file is a few hundred bytes, so an
    # opt-out could only buy a vacuous green.
    schema_verification = verify_schema_input(corpus, identity)
    # ...and the identity re-derived above must be the one this SESSION WAS STARTED AGAINST
    # (#3272 review round 4). `verify_corpus_bytes` compares the recorded identity to the bytes
    # present AT REPORT TIME, which is self-consistent for both of the sequences that attribute
    # figures to bytes nobody measured: re-reporting an old result dir under a different
    # `--corpus`, and a corpus regenerated mid-run. The driver stamps `session-corpus-pin.json`
    # before the first rep; this REQUIRES it and refuses a mismatch.
    # ...and, since round 6 B2, THE COMPLETE PINNED COMPONENT SET. The three Data.db fields
    # cannot see an auxiliary component replaced mid-session with `corpus-identity.json`
    # refreshed beside it — self-consistent at report time, so `verify_corpus_components` above
    # passes and the report printed an affirmative full-verification note while an
    # Index.db that shapes the measured read pattern was not the one measured.
    #
    # The per-component digests `verify_corpus_components` JUST derived are handed over rather
    # than re-derived: hashing a 2.8 GB corpus twice per report is a real cost, and a second
    # derivation would be a second implementation whose disagreement would be undiagnosable.
    session_pin = verify_session_corpus_pin(
        d, corpus, identity, component_verification.get("components")
    )

    # ORDER MATTERS, and it is a diagnostic decision: the CORPUS is validated first (below),
    # then the manifest. A session pointed at a corpus with no identity must be refused AS a
    # corpus problem — naming the absent `corpus-identity.json` — rather than as an absent
    # manifest, which would send the reader to the wrong artifact.
    config = session_manifest_config(d, TEMPS_ALLOWED, ARMS_ALLOWED)
    # WHETHER THIS SESSION IS A WS0 BASELINE AT ALL (#3272 round 13, F3). Read from the
    # pre-measurement record, never re-derived here: the canonical pin can be re-pinned between
    # measurement and reporting (and a results dir is routinely reviewed from another checkout), so
    # a report-time comparison would judge the session against a shape it never ran against — in
    # EITHER direction. `verify_pinned_canonical_corpus` requires the record AND requires it to
    # support its own verdict, so a hand-edited `is_baseline: true` beside recorded divergences is
    # refused rather than printed.
    canonical = verify_pinned_canonical_corpus(
        session_pin_path(d), json.loads(session_pin_path(d).read_text())
    )
    # The mode the manifest declares and the mode the comparison was MADE under must agree. Two
    # records of one fact are two chances to disagree, so they are checked rather than assumed —
    # this is the only place both are in scope.
    if canonical["mode"] != config["baseline_mode"]:
        raise Invalid(
            f"the session manifest declares baseline_mode={config['baseline_mode']!r} while its"
            f" recorded canonical comparison was made under {canonical['mode']!r}. One of the two"
            " was edited; a report cannot say which claim this run makes."
        )
    reps = config["reps"]
    scan_passes = config["scan_passes"]
    temps = config["temps"]
    arms = config["arms"]
    server_cpus = config["server_cpus"]
    client_cpus = config["client_cpus"]
    step_duration = config["step_duration"]
    # WHICH COUNTERS AND WHICH BINARIES (#3248). Read from the manifest for the same reason as
    # everything above: a value that cannot be supplied cannot disagree. Promoted to the report's
    # TOP LEVEL rather than left only in the session pin, because the report makes claims ABOUT
    # them -- cycles/row and IPC are claims about specific counters, and this rig's whole output
    # is a ratio between two binaries, so which build produced them is not a footnote.
    events = config["events"]
    bin_dir = config["bin_dir"]
    # WHICH SERVER PRODUCED THE MEASURED ROWS (#3272 round 14, F2). Read from the pre-measurement
    # manifest and passed to every Flight arm, which compares it against EVERY rep's recorded
    # `endpoint`. Deliberately NOT a reporter argument, for the reason F1 gave for the whole
    # configuration: a value that cannot be supplied cannot disagree, so a record produced against
    # another server cannot be excused by re-reporting with a matching flag.
    flight_endpoint = config["flight_endpoint"]
    # WHICH CORPUS PATH THE BARE-SCAN ARM MUST HAVE OPENED (#3272). Arm A's own record states the
    # corpus, schema and table directories it read, and those statements were read by NOBODY — so
    # every corpus check in the rig was about the corpus the REPORTER was pointed at while nothing
    # established the BENCH opened it.
    #
    # Resolved from the PIN and NEVER from `--corpus`, and the MOVED-corpus case is what settles
    # which of the two it must be. The pin treats a move as REPORTED-not-fatal (the bytes decide;
    # `corpus_path_unchanged` above), and the first version of this line therefore fell back to the
    # reporter's `--corpus` on a move — WHICH IS BACKWARDS. The bench ran at MEASUREMENT time, when
    # the corpus was at the PINNED path, so that is the path it recorded; the moved path is one it
    # could not have known. Falling back to `--corpus` would have compared the artifacts against a
    # location that did not exist when they were written, refusing every moved-corpus session — and,
    # worse, it would have made the expectation follow a REPORT-TIME ARGUMENT, so a substituted
    # corpus could be excused by re-reporting with a matching `--corpus`. The pin is the whole
    # reason this is provenance.
    #
    # Fail-closed on a pin recording no path: an absent path cannot be compared, and defaulting it
    # would silently disable three comparisons.
    pinned_corpus_path = session_pin.get("pinned_corpus_path")
    if not isinstance(pinned_corpus_path, str) or not pinned_corpus_path.strip():
        raise Invalid(
            f"the session corpus pin records no usable corpus PATH (got {pinned_corpus_path!r}), so"
            " the bare-scan arm's recorded `corpus`, `schema` and `table_dirs_ingested` cannot be"
            " compared against the corpus this session measured. Refused rather than defaulted:"
            " falling back to --corpus would make a report-time argument the authority for a"
            " pre-measurement fact, which is how a substituted corpus gets excused by re-reporting"
            " (#3272)."
        )
    pinned_scan_corpus = pathlib.Path(pinned_corpus_path)
    # THE PINNING CLAIM'S EVIDENCE (#3272 round 9, F6). The two CPU lists above are manifest
    # strings, and `session_manifest_config` deliberately does not re-check them — correctly, but
    # the check that DID run was against the driver's argv and nothing tied the two together, so
    # a manifest edited to `99,99` printed "verified physical-core siblings" and exited 0. This
    # REQUIRES the driver's recorded sibling verification and requires it to be ABOUT these lists.
    pinning_verification = verify_pinning_record(d, server_cpus, client_cpus)
    # WHICH BINARIES PRODUCED THIS RATIO (#3272 round 10, M2). `--no-build` accepts any executable
    # already under target/release and nothing recorded the revision or any digest, so a stale
    # artifact could be measured and reported as a result for the current checkout. REQUIRED here;
    # the digests are deliberately NOT re-derived (a results dir is reviewed on other hosts and after
    # rebuilds — see ws0_binaries for the full argument, which is F6's).
    binary_provenance = verify_binary_provenance(d)
    corpus_rows = identity["rows"]
    # The PINNED columns-per-row, taken from the same REQUIRED-and-complete identity as the row
    # count (#3272 round 17). `load_corpus_identity` has already validated it as a positive exact
    # integer, so this is a read of an established quantity — the bare-scan collector's cell check
    # is a wiring of a pin the rig already had, not a new source of truth.
    corpus_cells_per_row = identity["cells_per_row"]
    # DID EVERY MEASUREMENT BOUNDARY HAPPEN (#3272 round 22)? The boundary verifier refuses a rep
    # whose corpus changed and RECORDS what it re-hashed — and until now nothing read that record.
    # Because each `.round` artifact is written BEFORE its boundary check, a refusal after the final
    # arm leaves a COMPLETE, reportable artifact set; restore the corpus and invoke this reporter
    # directly and every end-state check agrees (pin, sidecar and report-time re-hash all see the
    # restored bytes), so the figure PUBLISHED. Publishing because no failure reached us is a pass
    # derived from an ABSENCE — the failure was out-of-band by construction, it killed the driver —
    # so the evidence is REQUIRED and required to be COMPLETE: exactly one observation per boundary,
    # with missing, duplicate and unexpected each refusing. The expected set is DERIVED from the
    # manifest read above, never enumerated.
    boundary_observations = verify_boundary_observations(d, temps, arms, reps)
    full_matrix = len(temps) == len(TEMPS_ALLOWED) and len(arms) == len(ARMS_ALLOWED)

    results = {
        "issue": "#3096 (rig hardened by #3272)",
        "corpus": str(corpus),
        "corpus_identity": {
            k: identity[k]
            for k in (
                "seed",
                "rows",
                "partitions",
                "cells_per_row",
                "data_db_bytes",
                "data_db_sha256",
                "bytes_per_row",
            )
        },
        # What was OBSERVED about the corpus at report time, not what it claimed
        # about itself (#3272 review B6).
        "corpus_identity_verification": identity_verification,
        # The COMPLETE component set, re-stat'ed and (unless --skip-corpus-digest) re-hashed
        # (#3272 F3).
        "corpus_component_verification": component_verification,
        # THE SCHEMA, re-hashed from disk on every report — a MEASUREMENT INPUT both arms read
        # (asymmetrically), which was outside every verification the rig performed (#3272 R2).
        "schema_input_verification": schema_verification,
        # WHERE THIS REPORT'S CONFIGURATION CAME FROM (#3272 F1) — the pre-measurement manifest,
        # not this invocation's arguments.
        "configuration_source": {
            "manifest": config["source"],
            "note": (
                "reps, temperatures, arms, scan_passes, the CPU pins, the counted EVENTS and"
                " the binary SOURCE DIRECTORY were READ FROM the session manifest stamped before"
                " the first rep; they are not arguments to ws0_report.py, so a re-report cannot"
                " substitute a different configuration and claim it was verified (#3272 F1,"
                " #3248)"
            ),
        },
        # ...and that the corpus is the one the SESSION STARTED against, established from a pin
        # written before the first rep (#3272 round 4).
        "session_corpus_pin": session_pin,
        # ...and that EVERY BOUNDARY BETWEEN THE ENDS was verified too (#3272 round 22), read back
        # from the driver's own record. The three fields above are END-STATE checks and a mutation
        # restored before reporting satisfies all of them; this is the only field in the document
        # that can distinguish that state. It is reached only on a COMPLETE record, so the report
        # cannot claim `sha256_verified: true` over a bypassed boundary — there is no results.json
        # at all in that case (the single `Invalid` exit writes nothing).
        "boundary_observation_completeness": boundary_observations,
        "canonical_corpus": canonical,
        # WHICH PROGRAMS the ratio is between (#3272 round 10, M2) — the revision, the dirty state,
        # the build mode and every measured binary's digest, observed by the driver before the first
        # rep. This rig's output is a ratio between two binaries, so this is provenance.
        "binary_provenance": binary_provenance,
        "pinning": {
            "server_cpus": server_cpus,
            "client_cpus": client_cpus,
            "counter_mode": f"perf stat -C {server_cpus} (CPU-WIDE; never -p)",
            # THE RECORDED OBSERVATION, not the word "verified" over a module name (#3272 F6).
            # This used to read `"verified": "thread_siblings_list, fail-closed
            # (scripts/perf/lib-cpu.sh)"` — an unconditional string, printed about CPU lists the
            # reporting path never validated. It now carries the driver's own record of what it
            # checked (including that record's stated provenance limit), asserted above to be
            # ABOUT the lists this report prints.
            "verification": pinning_verification,
        },
        # The SELECTION this session ran, recorded so a narrow run can never be
        # read as a full matrix (#3272 finding 6). Completeness is judged against
        # exactly this: every selected (arm, temperature) must have all `reps`.
        "selection": {
            "temperatures": temps,
            "arms": arms,
            "temperatures_available": list(TEMPS_ALLOWED),
            "arms_available": list(ARMS_ALLOWED),
            "full_matrix": full_matrix,
            "note": (
                "completeness is asserted for the SELECTED combinations only; an"
                " unselected temperature or arm was NOT MEASURED in this session"
                " and this report says nothing about it"
            ),
        },
        "reps": reps,
        "step_duration": step_duration,
        "scan_passes": scan_passes,
        "events": events,
        # The SOURCE directory of the measured binaries. The reps execute FROZEN COPIES under
        # measured-bin/, so `binary_provenance` digests describe the bytes that ran but cannot
        # say which BUILD produced them -- a symbol-bearing profiling build and a stripped
        # release build are otherwise indistinguishable here (#3248).
        "bin_dir": bin_dir,
        "measurements": [],
    }

    lines = [
        "",
        # THE HEADLINE SAYS WHETHER THIS IS A BASELINE (#3272 round 13, F3). The title used to read
        # "WS0 SAME-SESSION BASELINE" unconditionally, over ANY corpus — so a smoke-sized corpus was
        # published under the word BASELINE in the first line of the report. The label is the ONLY
        # thing distinguishing the two to a reader, so it goes in the title rather than in a field
        # somebody would have to know to look for.
        (
            "==== WS0 SAME-SESSION BASELINE (issue #3096 rig, hardened #3272) ===="
            if canonical["is_baseline"]
            else "==== WS0 SAME-SESSION MEASUREMENT — *** NOT A BASELINE *** (issue #3096 rig,"
            " hardened #3272) ===="
        ),
        # ...and the label IN WORDS, on its own line, in BOTH modes — an affirmative statement in
        # the baseline case too, so a reader can tell "this run was checked and IS canonical" from
        # "this rig does not check", which the absence of a line cannot express.
        f"baseline     : {canonical['label']}"
        + (
            ""
            if canonical["is_baseline"]
            else "\n               DIVERGES from "
            + canonical["canonical_pin_source"]
            + " in: "
            + "; ".join(canonical["divergences"])
            if canonical["divergences"]
            else ""
        )
        + f" [{len(canonical['compared_fields'])} canonical field(s) compared BEFORE the first"
        f" rep, recorded in {pathlib.Path(canonical['source']).name}]",
        f"corpus       : {corpus}",
        f"corpus sha256: {identity['data_db_sha256']}",
        *corpus_identity_lines(identity_verification),
        # THE COMPLETE COMPONENT SET (#3272 F3), stated so a reader can see the verification
        # covered everything a scan reads and not `Data.db` alone.
        f"corpus comps : {component_verification['note']}",
        # ...and where the CONFIGURATION below came from (#3272 F1): the session, not this
        # command line.
        "config       : READ FROM the pre-measurement session manifest"
        f" ({pathlib.Path(config['source']).name}) — reps/temps/arms/scan-passes/CPU pins are"
        " NOT arguments to this reporter, so a re-report cannot substitute them",
        # The PRE-MEASUREMENT pin, stated so a reader can see the report is about the corpus
        # the session started against and not merely one that is self-consistent now (#3272
        # round 4).
        "corpus pin   : this session was STARTED against this corpus"
        f" (session-corpus-pin.json: {session_pin['pinned_rows']} rows /"
        f" {session_pin['pinned_data_db_bytes']:,} B), re-compared here"
        + (
            ""
            if session_pin["corpus_path_unchanged"]
            else f"; NOTE the corpus was MOVED (pinned path"
            f" {session_pin['pinned_corpus_path']}) — the bytes match, so this is reported"
            " rather than fatal"
        ),
        # ...and EVERY BOUNDARY BETWEEN THE ENDS (#3272 round 22), printed directly under the pin
        # because it is the half the pin cannot cover: the pin and the report-time re-hash are both
        # END-STATE observations, and a component mutated mid-run and restored before reporting
        # satisfies both. Printed in the AFFIRMATIVE case too — the absence of a line cannot tell a
        # reader "verified between every rep" from "this rig checks the ends only", which is how a
        # record that was written and never read went unnoticed for a round.
        *boundary_observation_lines(boundary_observations),
        f"corpus shape : {identity['rows']} rows / "
        f"{identity['partitions']} partitions / "
        f"{identity['bytes_per_row']:.2f} B/row",
        # THE REQUEST (#3272 round 10, M1). Printed beside the corpus pin because it is the other
        # half of "what was measured": the corpus says over WHICH BYTES, this says WHICH QUERY. It
        # was outside every verified record, so a template changed between arms left the corpus
        # untouched and every corpus digest in agreement while two arms answered different
        # questions.
        # WHICH PROGRAMS (#3272 round 10, M2). The corpus pin says over which bytes, the request pin
        # which query — this says which binaries, which is what the reported ratio is BETWEEN.
        # ...and the REVISION IS NAMED ONLY WHEN IT WAS OBSERVED (#3272 round 12, F1). This line
        # used to print a sha in BOTH build modes, so a `--no-build` session — binaries accepted off
        # the disk, possibly another branch's — was REPORTED as belonging to this checkout's HEAD. A
        # newer mtime establishes that they were written after that commit and nothing about which
        # revision produced them, so the sha was a value nobody observed: the fabricated-value class
        # AC3 exists to remove, in its most dangerous form, because a plausible sha reads exactly
        # like an established one. `checkout_revision_at_measurement` is printed instead, under a
        # name that claims only what it can support.
        f"binary pin   : {len(binary_provenance['binaries'])} binaries"
        + (
            f" at {binary_provenance['source_revision_short']}"
            if binary_provenance["source_revision_observed"]
            else " at an UNKNOWN source revision (reused binaries: --no-build accepted them off the"
            " disk, so which revision BUILT them is NOT established; checkout was at"
            f" {binary_provenance['checkout_revision_at_measurement'][:12]} while measuring)"
        )
        + (
            f" (DIRTY tree, {binary_provenance['source_dirty_paths']} changed path(s) — the"
            " revision does NOT fully describe what was built)"
            if binary_provenance["source_dirty"]
            else " (clean tree)"
        )
        + f", build mode {binary_provenance['build_mode']}; digests in binary-provenance.json",
        f"request pin  : ticket-template.json sha256"
        f" {session_pin['pinned_ticket_sha256'][:16]}…"
        f" ({session_pin['pinned_ticket_bytes']} B) — pinned BEFORE the first rep and re-derived"
        " from disk here; every Flight rep re-read this file",
        # The claim NAMES ITS EVIDENCE (#3272 round 9, F6). It used to read
        # `server {server_cpus} (verified physical-core siblings)` unconditionally, about a
        # manifest string nothing had validated — MEASURED, a manifest edited to `99,99` printed
        # `server 99,99 (verified physical-core siblings)` and exited 0. The word "verified" now
        # stands on the driver's recorded observation, which the reporter has asserted is about
        # exactly these lists, and the line says WHERE that observation came from.
        f"pinning      : server {server_cpus} (physical-core siblings"
        f" {pinning_verification['server_siblings_expanded'].split('(')[-1].rstrip(')')} verified"
        f" on {pinning_verification['host']} pre-measurement, recorded in"
        f" {pathlib.Path(pinning_verification['source']).name}), client {client_cpus}",
        f"counters     : perf stat -C {server_cpus}  [CPU-WIDE; no -p anywhere]",
        # WHICH SERVER SERVED THE ROWS THOSE COUNTERS DESCRIBE (#3272 round 14, F2). Printed
        # directly under the `counters` line deliberately: the pairing of the two is the property —
        # `perf -C` measures the pinned cores, and this states that the rows divided by those cycles
        # were served by the process on them. Previously `endpoint` was classified IGNORED, so a
        # record produced against another server (a peer lane's process on another port, a remote
        # host) was reported here with no line saying so.
        f"server pinned: {flight_endpoint} — EVERY Flight rep's recorded `endpoint` was compared"
        " EXACTLY against this pre-measurement pin, so the rows above were served by the process"
        " those counters measure",
        f"reps         : {reps} (median reported, spread shown)",
        *selection_lines(temps, arms, reps),
        "",
    ]

    # The per-rep round metadata is INTEGRITY-CHECKED and RECORDED per temperature. No
    # ordering/interleaving property is derived from it: that claim was DELETED in #3272
    # round 4 (it returned a positive verdict at one round having compared nothing), and
    # re-adding an OBSERVED drift control is tracked by #3287/#3299.
    recorded_rounds: dict[str, dict] = {}
    for temp in temps:
        # `scan_passes` and `corpus_rows` are passed IN (#3272 F2): the bare-scan collector
        # now validates the per-pass records against both — exactly `--scan-passes` of them,
        # each having observed the whole corpus — and DERIVES the rep's rows and seconds from
        # them. Previously `--scan-passes` was recorded in results.json and compared against
        # nothing, and the `passes` array was never read at all.
        # `pinned_scan_corpus` (#3272): the corpus PATH this session pinned before the first rep,
        # against which every rep's recorded `corpus`, `schema` and `table_dirs_ingested` are
        # compared. Taken from the PIN rather than from `--corpus`, for the reason `flight_endpoint`
        # is: it is a pre-measurement fact, so a scan performed over other bytes cannot be excused
        # by re-reporting with a matching flag.
        # `corpus_cells_per_row` (#3272 round 17): every pass's CELL count must be
        # `corpus_rows x cells_per_row`, so a scan returning every row with MISSING COLUMNS is
        # refused instead of published — the row check could not see it, and the rig's ratio is a
        # measurement of exactly that content volume.
        scan = collect_scan(
            d,
            temp,
            reps,
            scan_passes,
            corpus_rows,
            corpus_cells_per_row,
            pinned_scan_corpus,
        )
        results["measurements"].append(scan)
        lines.append(f"[{temp.upper()}]")
        lines.append(fmt("bare scan (execute_streaming)", scan))
        lines += prewarm_warning(scan, "bare-scan", temp)
        for arm in arms:
            fl = collect_flight(d, temp, arm, reps, corpus_rows, flight_endpoint)
            results["measurements"].append(fl)
            # The label says the arm was REQUESTED, and it is derived FROM THE BLOCK rather than
            # from the loop variable (#3272 round 16). Two properties, both deliberate:
            #
            # * `(bypass requested)` not `(bypass)`. `CQLITE_FLIGHT_MERGE_PATH=bypass` only PREFERS
            #   the fast path — cqlite-flight declines it on any correctness precondition and falls
            #   through to the k-way merger — and the server does not report the arm it took (see
            #   ws0_flight_arm.MERGE_PATH_OBSERVABILITY_NOTE). So a row labelled `(bypass)` could be
            #   a MERGER measurement, and this rig's headline is a bare/flight ratio PER ARM: the two
            #   arm rows could be the same code twice under different labels. The printed label must
            #   not out-claim results.json.
            # * Read from `fl["requested_merge_path"]`, so the summary and results.json cannot
            #   disagree: a rename on one side that missed the other would raise a KeyError here
            #   rather than print a label the JSON does not support.
            lines.append(fmt(f"flight do_get ({fl['requested_merge_path']} requested)", fl))
            lines += prewarm_warning(fl, f"flight/{arm}", temp)
            # THE ARROW-VOLUME CAVEAT, DIRECTLY UNDER THE FIGURE IT QUALIFIES (#3272 round 20).
            # Beside the number, not appended once at the bottom — a caveat eleven bullets below
            # the figure is the shape that produced this finding, and on a COLD-ONLY session (no
            # preflight, so NO comparison at all) the only human-readable text was a NOTES bullet
            # worded for the compared case, i.e. it said the opposite of what happened. Emitted in
            # BOTH states because neither is a verification; the function has no silent branch, and
            # an unrecognised record shape raises rather than printing nothing.
            lines += content_volume_caveat_lines(fl, f"flight/{arm}", temp)
            # Every operand of every printed figure, through the SHARED validator (#3272
            # review round 3, B2). No permissive numeric fallback anywhere in the reporting
            # path: `scan_rps / fl_rps if fl_rps else float("inf")` used to publish `inf x`
            # as the bare/flight ratio for a Flight arm that measured NOTHING — a printable
            # figure standing in for an absent one, and the most flattering possible reading
            # of the arm under study. The pre-round-3 replacement tested `<= 0`, which still
            # admitted `inf`/`nan`; `positive_derived` requires FINITE and positive, so a
            # `nan` median cannot reach the `>= target` comparison (NaN compares False, which
            # would print BELOW TARGET for an arm that measured nothing — a verdict, from an
            # absence). `spread()` refuses these upstream too; this is the local statement of
            # the same rule at the point of use.
            scan_rps = positive_derived(
                f"the bare-scan median rows/s for {arm} ({temp})",
                scan["rows_per_sec"]["median"],
                "it is the ratio's NUMERATOR and the 1.3x target's basis",
            )
            fl_rps = positive_derived(
                f"the flight median rows/s for {arm} ({temp})",
                fl["rows_per_sec"]["median"],
                "it is the ratio's DENOMINATOR",
            )
            scan_cpr = positive_derived(
                f"the bare-scan median cycles/row for {arm} ({temp})",
                scan["cycles_per_row"]["median"],
                "it is the DIVISOR of the printed cycles/row percentage delta",
            )
            fl_cpr = positive_derived(
                f"the flight median cycles/row for {arm} ({temp})",
                fl["cycles_per_row"]["median"],
                "it is the numerator of the printed cycles/row percentage delta",
            )
            ratio = scan_rps / fl_rps
            target = scan_rps / 1.3
            verdict = "PASS" if fl_rps >= target else "BELOW TARGET"
            lines.append(
                f"      ratio bare/flight = {ratio:.2f}x   "
                f"1.3x target => do_get must reach {target:,.0f} rows/s   [{verdict}]"
            )
            # ...AND UNDER THE VERDICT ITSELF (#3272 round 20). A `[PASS]`/`[BELOW TARGET]` is the
            # line somebody quotes, so it carries what it is conditional on rather than relying on
            # the reader having read the figure caveat two lines up. The `verdict` is passed in so
            # the text can name the direction a short payload moves THIS verdict.
            lines += content_volume_verdict_caveat_lines(fl, f"flight/{arm}", verdict)
            # The DELTA is deliberately unconstrained in sign — a Flight arm that costs
            # FEWER cycles/row than the bare scan is a legitimate (and desirable) result.
            # Its DIVISOR is what needed the domain, and both operands are validated above.
            lines.append(
                f"      cycles/row delta  = {fl_cpr - scan_cpr:+,.0f} "
                f"({(fl_cpr / scan_cpr - 1) * 100:+.1f}%)"
            )
            # The PAIRED within-round comparison, beside the medians (#3272 B5). The
            # median-vs-median line above is retained because it is the figure the
            # 1.3x spec target is stated against, but it is not left standing ALONE:
            # this rig's own recorded evidence is that a couple of percent of median
            # difference is not readable at its spreads.
            #
            # `fl["per_round_paired"] = None` used to precede this call as a "reset". It was
            # DEAD CODE (#3272 review round 3 nit): the very next statement overwrites it on
            # success, and on a raise nothing is written at all — the reporter exits 1 without
            # producing a results.json. A line whose only effect is invisible reads as a
            # deliberate initialization and invites a reader to assume a partial-write path
            # exists, so it is removed rather than commented.
            rounds, paired_lines = paired_rounds(scan, fl)
            fl["per_round_paired"] = rounds
            lines += paired_lines
        # The artifact-set INTEGRITY check over every arm of this temperature at once — it
        # is a property of the recorded ROUND, so it cannot be checked per arm-pair.
        # `bare_scan` participates as an arm because it IS one: it is measured in every
        # round. This produces RECORDED DATA plus refusals, never a verdict (#3272 round 4).
        arms_meta = {"bare_scan": scan["round_metadata"]}
        for m in results["measurements"]:
            if m["temperature"] == temp and m["arm"].startswith("flight_"):
                arms_meta[m["arm"]] = m["round_metadata"]
        recorded_rounds[temp] = collect_recorded_round_metadata(temp, arms_meta)
        lines.append("")

    results["recorded_round_metadata"] = recorded_rounds
    lines += [
        "NOTES",
        "  * warm and cold are SEPARATE claims above; nothing here is blended.",
    ]
    # What the round artifacts RECORD, and an explicit statement that no ordering claim is
    # made from them (#3272 round 4). There is no code path here that asserts the session
    # was interleaved.
    for temp in temps:
        lines += recorded_round_metadata_lines(recorded_rounds[temp])
    lines += [
        *selection_and_request_note_lines(),
        *executed_arm_note_lines(),
        *content_volume_note_lines(),
        *counting_note_lines(),
        # ...and the MID-RUN boundary record (#3272 round 22), between the corpus-identity bullet
        # above (an END-STATE observation) and the fixture-scope ones below: it is the only bullet
        # that speaks to the window a pre/post pair is blind to.
        *boundary_observation_note_lines(),
        *fixture_scope_note_lines(),
    ]
    return results, lines


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True)
    ap.add_argument("--corpus", required=True)
    # `--server-cpus`, `--client-cpus`, `--reps`, `--temps`, `--arms`, `--step-duration` and
    # `--scan-passes` are DELIBERATELY ABSENT (#3272 F1). They are properties OF THE SESSION and
    # are read from its pre-measurement manifest; accepting them here let a re-report substitute
    # a different configuration and claim it had been verified. They are removed rather than
    # ignored, so passing one is an argparse error a caller can see rather than a value that
    # silently does nothing.
    # The ONLY relaxation anywhere in the reporting path, and it is not a relaxation
    # of a VERDICT: it omits a multi-GB re-hash and RECORDS that it did, in both the
    # summary (a loud `CORPUS DIGEST UNVERIFIED` banner) and results.json
    # (`sha256_verified: false`). The size comparison is unaffected — it is a stat, so
    # there is nothing to opt out of. There is deliberately no env var: a flag on the
    # command line is in the transcript of the run (#3272 review B6).
    ap.add_argument(
        "--skip-corpus-digest",
        action="store_true",
        help=(
            "skip re-hashing the corpus Data.db (seconds of IO on a 2.8 GB corpus)."
            " The report then STAMPS 'CORPUS DIGEST UNVERIFIED' and records"
            " sha256_verified=false; the size check still runs."
        ),
    )
    args = ap.parse_args()

    try:
        results, lines = build_report(args)
    except Invalid as exc:
        # One exit path for every fail-closed decision, so no guard can be added
        # that reports a problem without exiting non-zero.
        print(f"FATAL: {exc}", file=sys.stderr)
        return 1

    (pathlib.Path(args.dir) / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
