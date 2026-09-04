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
import datetime
import json
import math
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
from ws0_flight_arm import collect_flight, flight_rep_tag  # noqa: E402
# DID EVERY FLIGHT REP RUN UNDER THE SAME ADMISSION CEILING — #3551 item 10. The ceiling is
# DERIVED from available_parallelism, which respects the CPU affinity mask, so it moves with
# --flight-server-cpus: two arms could differ in the pin AND in how much work the server admits.
from ws0_flight_admission import verify_flight_admission  # noqa: E402
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
from ws0_quiescence_evidence import (  # noqa: E402
    EvidenceError,
    assert_self_consistent as assert_verdict_self_consistent,
)
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
def fmt(label: str, block: dict, counted_cpus: str) -> str:
    """One arm's figures, WITH THE CPUS THEY WERE COUNTED ON (#3551).

    `counted_cpus` is REQUIRED rather than defaulted: since the two arms can be pinned
    differently, "cycles/row" means "hardware-thread cycles on THESE cpus per row", and the two
    arms may legitimately name different lists. A default here would let one arm's figure be
    printed under the other arm's counted list, which is the #3551 defect wearing a report's
    clothes.
    """
    rps, cpr = block["rows_per_sec"], block["cycles_per_row"]
    return (
        f"  {label:<34} {rps['median']:>12,.0f} rows/s  "
        f"[{rps['min']:,.0f}..{rps['max']:,.0f}, spread {rps['spread_pct_of_median']:.1f}%]   "
        f"{cpr['median']:>10,.0f} cycles/row "
        f"[{cpr['min']:,.0f}..{cpr['max']:,.0f}, {cpr['spread_pct_of_median']:.1f}%]   "
        f"IPC {block['ipc']['median']:.2f}   rows={block['row_denominator_total']:,} "
        f"(n={rps['n']})   counted on cpus {counted_cpus}"
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
    # WHERE THE FLIGHT SERVER RAN (#3551) — read from the manifest for the same reason as
    # everything else here, and tied to the driver's recorded verification below.
    flight_server_cpus = config["flight_server_cpus"]
    # THE ENVIRONMENT THIS SESSION RAN IN (#3551 item 8) — ambient and injected, separately,
    # because with one binary set across all arms it is the only thing that distinguishes them.
    env_ambient = config["env_ambient"]
    env_injected = config["env_injected"]
    step_duration = config["step_duration"]
    # WHICH COUNTERS AND WHICH BINARIES (#3248). Read from the manifest for the same reason as
    # everything above: a value that cannot be supplied cannot disagree. Promoted to the report's
    # TOP LEVEL rather than left only in the session pin, because the report makes claims ABOUT
    # them -- cycles/row and IPC are claims about specific counters, and this rig's whole output
    # is a ratio between two binaries, so which build produced them is not a footnote.
    events = config["events"]
    bin_dir = config["bin_dir"]
    profile = config["profile"]
    # THE QUIESCENCE CLAIM NEEDS ITS EVIDENCE, NOT JUST ITS INTENT (#3248, roborev job 64
    # finding 2).
    #
    # `config.quiescence` is stamped BEFORE the first rep, so it records what the run INTENDED.
    # The judgement happens AFTER every measurement artifact is complete, which means a session
    # that was REJECTED or INTERRUPTED still has a complete artifact set and a manifest saying
    # `judged against <path>` -- and re-reporting it would print that claim with no successful
    # verdict anywhere. The intent is not the evidence.
    #
    # So a configured session must carry `quiescence-verdict.json`, it must say QUIESCENT, and
    # it must name the SAME timeseries the manifest does. An unjudged session is fine and says
    # so; a session that CLAIMS judgement and cannot show it is refused.
    quiescence_intent = config["quiescence"]
    quiescence_verdict = None
    if quiescence_intent.startswith("judged against "):
        declared_ts = quiescence_intent[len("judged against "):].strip()
        vpath = d / "quiescence-verdict.json"
        if not vpath.exists():
            raise Invalid(
                f"the session manifest says {quiescence_intent!r}, but {vpath.name} is absent."
                " The judgement runs AFTER the measurement artifacts are complete, so a"
                " rejected or interrupted session looks identical to a certified one from the"
                " artifacts alone. A quiescence claim requires its verdict; re-run, or report a"
                " session that does not claim to have been judged."
            )
        try:
            verdict = json.loads(vpath.read_text())
        except (OSError, ValueError) as exc:
            raise Invalid(f"{vpath.name} is not readable JSON: {exc}") from None
        if not isinstance(verdict, dict) or verdict.get("verdict") != "QUIESCENT":
            raise Invalid(
                f"{vpath.name} does not record a QUIESCENT verdict"
                f" (verdict={verdict.get('verdict') if isinstance(verdict, dict) else '?'!r})."
                " A session whose own verdict is not QUIESCENT must not be reported as judged."
            )
        # THE VERDICT MUST BE SELF-CONSISTENT WITH ITS OWN CONCLUSION, AND THAT CHECK IS NOW
        # CLOSED (#3248, roborev jobs 73 + 75). This was ~95 lines of inline field checks grown
        # one review round at a time: job 73 F2 added evidence checking at all, then job 75 found
        # three more holes in it -- load thresholds unchecked, `coverage_gap_bound_s` optional so
        # deleting the bound skipped its own comparison, and `census_breadth` published while
        # contradicting `narrow_census_records`.
        #
        # Patching pointwise converged only as fast as the reviewer found holes, so the method was
        # the defect. `ws0_quiescence_evidence` declares EVERY field of the verdict with its rule,
        # errors on a MISSING one, and -- the part that stops the regress -- errors on an
        # UNDECLARED one, so a new field in `judge()` fails here until someone decides what it
        # means instead of silently going unchecked.
        try:
            assert_verdict_self_consistent(verdict, vpath.name)
        except EvidenceError as exc:
            raise Invalid(str(exc)) from None
        # THE FIELD IS REQUIRED, NOT "COMPARED IF PRESENT" (#3248, roborev job 66 finding 3).
        #
        # The first version compared only `if recorded_ts is not None`, so a verdict WITHOUT
        # the field was accepted — a QUIESCENT claim published with no evidence it came from
        # the timeseries the manifest declares. That is a pass derived from the ABSENCE of a
        # bad signal, which is the rule this issue keeps restating; it is the THIRD time this
        # exact shape has appeared in my own guards, which is why it is called out here rather
        # than quietly patched.
        recorded_ts = (verdict.get("window_census") or {}).get("timeseries")
        if recorded_ts is None:
            raise Invalid(
                f"{vpath.name} records no `window_census.timeseries`, so nothing establishes"
                " WHICH load record this verdict was produced from. A verdict that cannot name"
                " its own subject cannot support the manifest's claim; re-run with the current"
                " ws0_quiescence.py, which records it."
            )
        if recorded_ts != declared_ts:
            raise Invalid(
                f"{vpath.name} was judged against {recorded_ts!r} but the manifest declares"
                f" {declared_ts!r}. A verdict from a DIFFERENT timeseries does not establish"
                " anything about this session."
            )
        # THE CAVEAT TRAVELS WITH THE VERDICT (#3248, roborev job 69 finding 2). A window whose
        # records carry only the narrow census can still be certified -- a timeseries recorded
        # before `competing_count` existed is legitimate -- but publishing the QUIESCENT verdict
        # WITHOUT its breadth would state a stronger claim than the evidence supports, which is
        # the whole failure mode this issue is about. The verdict recorded the breadth already;
        # the reporter was dropping it on the floor.
        _wc = verdict.get("window_census") or {}
        # THE VERDICT MUST COVER *THIS* SESSION'S MEASUREMENT WINDOW, NOT MERELY NAME THE SAME
        # FILE (#3248, roborev job 70 finding 3). The `timeseries` check above binds the verdict
        # to the right SAMPLER; it says nothing about WHEN. `box-load.jsonl` is a single
        # long-lived file spanning every session on this box, so a clean verdict judged over a
        # DIFFERENT ten-minute window of the SAME file satisfied every check here and certified
        # this session. That is a pass borrowed from an adjacent measurement -- the same shape as
        # a verdict from a different file, one level down.
        #
        # The session's window is derived from the REP PAYLOADS' own `ts_unix_ms`, never from an
        # argument: a value that cannot be supplied cannot disagree (the reason `flight_endpoint`
        # and the corpus are read from the manifest rather than the command line). Payload records
        # are selected STRUCTURALLY, by carrying the field, rather than by a filename allowlist a
        # newly-added arm would silently escape.
        _reps_seen = 0
        _t_lo = None
        _t_hi = None
        for _pf in sorted(d.glob("*.jsonl")):
            try:
                _text = _pf.read_text()
            except OSError as exc:
                raise Invalid(
                    f"{_pf.name} is unreadable, so this session's measurement window cannot be"
                    f" established and the quiescence verdict cannot be bound to it: {exc}"
                ) from None
            for _line in _text.splitlines():
                if not _line.strip():
                    continue
                try:
                    _r = json.loads(_line)
                except ValueError:
                    continue
                if not isinstance(_r, dict) or "ts_unix_ms" not in _r:
                    continue
                _ts = _r["ts_unix_ms"]
                _dur = _r.get("duration_s", 0)
                if isinstance(_ts, bool) or not isinstance(_ts, (int, float)) \
                        or not math.isfinite(_ts) or _ts <= 0:
                    raise Invalid(
                        f"{_pf.name} carries an unusable `ts_unix_ms` ({_ts!r}) on a rep record,"
                        " so the measurement window cannot be bounded. A window that cannot be"
                        " COMPUTED must never be treated as covered."
                    )
                if isinstance(_dur, bool) or not isinstance(_dur, (int, float)) \
                        or not math.isfinite(_dur) or _dur < 0:
                    raise Invalid(
                        f"{_pf.name} carries an unusable `duration_s` ({_dur!r}) beside a rep"
                        " timestamp; the rep's extent is then unknown, so the window would be"
                        " UNDERSTATED and the coverage check weaker than it reads."
                    )
                _reps_seen += 1
                # `ts_unix_ms` IS THE REP'S END, ESTABLISHED FROM THE PRODUCER'S SOURCE RATHER
                # THAN ASSUMED. The first version of this check widened each rep by its duration
                # in BOTH directions, reasoning that the record "does not say which end" and that
                # symmetric widening was the conservative choice. It was conservative and WRONG:
                # it pushed the session window 18 s past the true end and REFUSED a correctly
                # covered session -- a red on correct input, which is the failure mode agents
                # learn to waive. Two independent measurements then settled it (payload mtime
                # equals `ts` to the second) and `tools/flight-loadgen/src/ramp.rs:184-188` is the
                # authority: `duration_s = started.elapsed()` and `ts_unix_ms = SystemTime::now()`
                # are BOTH taken after every worker has joined. So the extent is [ts - dur, ts].
                _lo = (_ts / 1000.0) - _dur
                _hi = _ts / 1000.0
                _t_lo = _lo if _t_lo is None else min(_t_lo, _lo)
                _t_hi = _hi if _t_hi is None else max(_t_hi, _hi)
        if _reps_seen == 0 or _t_lo is None or _t_hi is None:
            raise Invalid(
                "no rep payload in this session carries a `ts_unix_ms`, so the session's own"
                " measurement window is unknown and the QUIESCENT verdict cannot be bound to it."
                " An unbindable verdict states nothing about this session, so it is refused"
                " rather than published (#3248)."
            )
        _win = _wc.get("window") if isinstance(_wc.get("window"), dict) else None
        if _win is None or _win.get("start") is None or _win.get("end") is None:
            raise Invalid(
                f"{vpath.name} records no `window_census.window` start/end, so the verdict cannot"
                " be shown to cover this session's measurement window. Re-run with the current"
                " ws0_quiescence.py, which records the judged window."
            )

        def _epoch(label: str, value: object) -> float:
            if not isinstance(value, str):
                raise Invalid(f"{vpath.name} window {label} is not a string ({value!r}).")
            try:
                _dt = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                raise Invalid(
                    f"{vpath.name} window {label} ({value!r}) is not an ISO-8601 instant, so the"
                    " judged window cannot be compared with the measurement window."
                ) from None
            if _dt.tzinfo is None:
                _dt = _dt.replace(tzinfo=datetime.timezone.utc)
            return _dt.timestamp()

        _v_start = _epoch("start", _win.get("start"))
        _v_end = _epoch("end", _win.get("end"))
        # THE TWO SIDES ARE RECORDED AT DIFFERENT RESOLUTIONS (#3248, roborev job 78 finding 3).
        # The driver stamps the window with `date -u +%Y-%m-%dT%H:%M:%SZ`, i.e. TRUNCATED to whole
        # seconds, while the rep payloads carry `ts_unix_ms` at millisecond resolution. Truncation
        # moves both edges EARLIER, and the two directions are not symmetric:
        #   * the START moving earlier WIDENS the window -- conservative, no slack needed;
        #   * the END moving earlier NARROWS it, so a window that genuinely covered a rep ending
        #     at .900 can read as ending at .000 and FALSE-RED a valid session.
        # So the end is compared with exactly one second of slack -- the maximum the recorded
        # resolution can hide, not a guessed margin: a stamp of T means the true instant lies in
        # [T, T+1). Anything larger would be an invented tolerance, and this guard has already
        # cost one round by padding "conservatively" in the wrong direction.
        _END_TRUNCATION_SLACK_S = 1.0
        if _v_start > _t_lo or (_v_end + _END_TRUNCATION_SLACK_S) < _t_hi:
            raise Invalid(
                f"{vpath.name} was judged over {_win.get('start')}..{_win.get('end')}, which does"
                f" NOT cover this session's FLIGHT-REP window"
                f" ({datetime.datetime.fromtimestamp(_t_lo, datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}"
                f"..{datetime.datetime.fromtimestamp(_t_hi, datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')},"
                f" from {_reps_seen} rep record(s), each spanning [ts-duration, ts]). A clean"
                " verdict from an ADJACENT window of the same long-lived timeseries establishes"
                " nothing about the window these numbers were measured in."
            )
        # REQUIRED, NOT "COPIED IF PRESENT" (#3248, roborev job 70 finding 4). These were read
        # with `.get()`, so a verdict missing its sample count, its coverage bound or its census
        # breadth still published `QUIESCENT` with `null` caveats -- a stronger claim than the
        # evidence supports, printed as though the caveat had been checked and found empty. A
        # missing caveat is an UNMEASURED caveat, which is exactly this issue's recurring shape.
        for _k in ("samples", "coverage_largest_gap_s", "narrow_census_records", "census_breadth"):
            if _wc.get(_k) is None:
                raise Invalid(
                    f"{vpath.name} records no `window_census.{_k}`, so the QUIESCENT verdict"
                    " cannot be published with the caveats that qualify it. A null caveat reads"
                    " as an absent concern; re-run with the current ws0_quiescence.py."
                )
        quiescence_verdict = {
            "verdict": verdict.get("verdict"),
            "in_window_samples": _wc.get("samples"),
            "coverage_largest_gap_s": _wc.get("coverage_largest_gap_s"),
            "narrow_census_records": _wc.get("narrow_census_records"),
            "census_breadth": _wc.get("census_breadth"),
            # WHICH WINDOW was judged, and that it covers the FLIGHT reps (asserted above).
            # Recorded so a reader can re-check the binding rather than trust that it happened
            # (#3248 job 70).
            "judged_window": {"start": _win.get("start"), "end": _win.get("end")},
            # DELIBERATELY NOT `covers_measurement_window`, WHICH IS WHAT THIS FIELD FIRST SAID.
            # The window is derived from `ts_unix_ms`, and the ONLY producer of that field in the
            # whole rig is the flight loadgen step record (tools/flight-loadgen/src/record.rs).
            # The bare-scan arm's payload is `scan-<temp>-<rep>.json` -- a `.json`, not `.jsonl`,
            # and it carries no absolute time at all, only `timed_scan_secs`. So the scan arm's
            # extent CANNOT be derived from the records, and naming this field for the whole
            # measurement would have claimed coverage of an arm the check never saw.
            #
            # The gap is not hypothetical: arm positions ALTERNATE, and this session's own records
            # show it (scan at position 1,2,1 and flight at 2,1,2 across the three reps). A scan
            # rep at position 1 therefore begins BEFORE the round's flight rep, and one at
            # position 2 ends AFTER it, so the assert is under-strict by roughly one scan rep at
            # each edge. What it does still catch is the case it was built for -- a verdict
            # borrowed from an ADJACENT window of the same long-lived timeseries, which is minutes
            # away, not seconds. Widening by a guessed margin was considered and refused: an
            # invented bound is not a measurement, and this same guard already cost one round by
            # padding "conservatively" in the wrong direction.
            "covers_flight_rep_window": True,
            "flight_rep_window_source": (
                "derived from ts_unix_ms/duration_s in the session's *.jsonl rep payloads; the"
                " bare-scan arm records no absolute time, so its extent is NOT covered by this"
                " assert (#3248)"
            ),
            "flight_rep_records": _reps_seen,
        }
    quiescence = quiescence_intent
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
    pinning_verification = verify_pinning_record(
        d, server_cpus, client_cpus, flight_server_cpus
    )
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
        # THE ENVIRONMENT, AS MEASURED AND AS INJECTED (#3551 item 8). At the TOP LEVEL rather
        # than inside `pinning`, because it is a property of the whole session and because a
        # reader comparing two results.json files for a reproduction has to find it without
        # knowing which subsection the rig happened to file it under (ws0-3552 §4).
        "environment": {
            "ambient": env_ambient,
            "injected": env_injected,
            "note": (
                "AMBIENT is the driver's own environment as MEASURED before the first rep;"
                " INJECTED is what the rig set, on the flight server's launch line ONLY. They"
                " are separate fields because a stray operator variable and a deliberate"
                " injection are different facts. An ambient LD_PRELOAD or MALLOC_* is REFUSED by"
                " the driver, because ws0-scan-bench would inherit it and the bare scan is the"
                " drift control (#3551)."
            ),
        },
        "pinning": {
            "server_cpus": server_cpus,
            "client_cpus": client_cpus,
            # THE FLIGHT ARM'S OWN PIN AND ALLOCATOR (#3551), each carrying what was VERIFIED
            # rather than what was requested. `flight_pin_claim` is derived from the record's
            # closed mode set by ws0_pinning, so this document can never describe a
            # distinct-cores pin in the sibling vocabulary.
            "flight_server_cpus": flight_server_cpus,
            # WHAT EACH ARM'S CYCLES WERE COUNTED ON (#3551 item 6), as a mapping rather than
            # only inside the prose of `counter_mode`: a machine reader comparing two arms needs
            # to know that "cycles/row" is per-hardware-thread-set and that the two sets may
            # differ. Derived from the same two values the driver's verified pairing table is.
            "counted_cpus_by_arm": {"scan": server_cpus, "flight": flight_server_cpus},
            "flight_pin_mode": pinning_verification["flight_pin_mode"],
            "flight_pin_claim": pinning_verification["flight_pin_claim"],
            "flight_allocator": pinning_verification["flight_allocator"],
            "flight_allocator_lib": pinning_verification["flight_allocator_lib"],
            "flight_malloc_arena_max": pinning_verification["flight_malloc_arena_max"],
            # THE COUNTING DOMAIN IS PER ARM since the flight pin became separable: the bare
            # scan is counted over the server set and the flight arm over the flight set, which
            # is where its server actually ran. Stated as two entries rather than one, because a
            # single `-C {server_cpus}` string was FALSE for the flight arm the moment the two
            # pins could differ.
            "counter_mode": (
                f"perf stat -C {server_cpus} for the bare-scan arm and"
                f" -C {flight_server_cpus} for the Flight arm (CPU-WIDE; never -p)"
            ),
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
        # Whether a SAMPLING PROFILE was attached to these counting windows, and at what
        # frequency. A profiled run pays observer overhead, so its throughput figures are not
        # baseline figures -- and nothing else in this document could tell a reader that
        # (#3248): the same symbol-bearing bin_dir runs both ways.
        "profile": profile,
        # Whether this session was judged against an external box-load timeseries, or not
        # judged at all. Recorded both ways: an unjudged session is UNVERIFIED, not quiet
        # (#3248).
        "quiescence": quiescence,
        # The VERDICT, not the intent. None when the session did not claim to be judged; a
        # session that claimed it and could not show one never reaches here (#3248).
        "quiescence_verdict": quiescence_verdict,
        "measurements": [],
    }

    # THE SINGLE SOURCE OF TRUTH FOR BASELINE-NESS. Read by the title AND by the `profile` line, so
    # the two cannot contradict each other -- which they did, in BOTH directions, one round apart
    # (job 80 F3: title claimed BASELINE on a profiled run; job 82 F1: the profile line claimed
    # "throughput is a baseline" on a non-canonical corpus while the title denied it).
    #
    # A run is a baseline only if the corpus is canonical AND no sampling profiler was attached:
    # observer overhead measures 1.6-4.3% on rows/s, so a profiled run's throughput is not a
    # baseline however canonical its corpus.
    # DEFERRED DEFECT, MEASURED: `--bin-dir` CAN PUT A NON-RELEASE BUILD UNDER THIS LABEL.
    # (#3248 roborev job 84 F2; follow-up https://github.com/pmcfadin/cqlite/issues/3469 family 4.)
    #
    # This asks about the corpus and the profiler and NOT about which BUILD produced the measured
    # binaries. `--bin-dir` accepts any directory of executables, so a debug or custom-profile
    # build whose codegen is not the release baseline can be reported under `BASELINE`.
    #
    # MEASURED IMPACT: none on any published run -- every measurement used target/perfsym or
    # target/release, and `binary_provenance` digests each measured binary, so a reader can check
    # which bytes ran. Deferred on that basis.
    #
    # THIS IS THE THIRD WAY A RUN COULD BE MISLABELLED A BASELINE (after the corpus, fixed in
    # #3272 round 13, and the profiler, fixed in job 80 F3). The recurrence says the right shape
    # is an ALLOWLIST of build profiles permitted to claim BASELINE, not a fourth condition bolted
    # onto this boolean.
    is_baseline_run = canonical["is_baseline"] and profile == "off"

    lines = [
        "",
        # THE HEADLINE SAYS WHETHER THIS IS A BASELINE (#3272 round 13, F3). The title used to read
        # "WS0 SAME-SESSION BASELINE" unconditionally, over ANY corpus — so a smoke-sized corpus was
        # published under the word BASELINE in the first line of the report. The label is the ONLY
        # thing distinguishing the two to a reader, so it goes in the title rather than in a field
        # somebody would have to know to look for.
        # A PROFILED RUN IS NOT A BASELINE EITHER, AND THE TITLE USED TO SAY IT WAS (#3248,
        # coordination ruling on roborev job 80: make the distinction "impossible to miss rather
        # than merely present").
        #
        # `is_baseline` asked only whether the CORPUS was canonical. So a profiled run on the
        # canonical corpus printed `==== WS0 SAME-SESSION BASELINE ====` in its first line while
        # the `profile` line six lines below said "these are NOT baseline numbers". THE REPORT
        # CONTRADICTED ITSELF, and the title is what a reader who reads one line reads. Adding the
        # `profile` field (F3's first half) put the truth in the document and left the headline
        # lying, which is a worse state than not having the field: two statements, one wrong, and
        # the wrong one louder.
        #
        # Observer overhead measures 1.6-4.3% on rows/s here, so it is inside every throughput
        # figure below. That disqualifies the run as a baseline exactly as a non-canonical corpus
        # does, and the title now says so for either cause.
        (
            "==== WS0 SAME-SESSION BASELINE (issue #3096 rig, hardened #3272) ===="
            if is_baseline_run
            else "==== WS0 SAME-SESSION MEASUREMENT — *** NOT A BASELINE *** (issue #3096 rig,"
            " hardened #3272) ===="
        ),
        # ...and WHY it is not one, when the corpus was canonical but a profiler was attached: the
        # reader should not have to reconcile the title with a field further down.
        *(["               (not a baseline because a SAMPLING PROFILER was attached:"
           f" {profile} — observer overhead is inside every throughput figure below)"]
          if canonical["is_baseline"] and not is_baseline_run else []),
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
        # THE FLIGHT ARM'S PIN, IN ITS OWN VOCABULARY (#3551). The claim word comes from
        # `ws0_pinning.FLIGHT_PIN_CLAIM[mode]`, so a `distinct-cores` pin can NEVER be printed as
        # `physical-core siblings`: the two are mutually exclusive properties and the report says
        # which one was actually read out of thread_siblings_list. The expanded sets are the
        # driver's own echo, sliced the same way the server line slices its own.
        f"flight pin   : flight server {flight_server_cpus}"
        f" (verified {pinning_verification['flight_pin_claim']} pre-measurement, recorded in"
        f" {pathlib.Path(pinning_verification['source']).name})"
        + ("   [SAME PIN AS THE BARE-SCAN ARM]" if flight_server_cpus == server_cpus
           else "   [DIFFERENT PIN FROM THE BARE-SCAN ARM — the bare scan is the pin-identical"
                " drift control; read the flight/scan difference as being about this pin]"),
        # The driver's OWN echo, verbatim — the expanded sibling sets it read out of sysfs for
        # each pinned CPU. Printed whole rather than sliced: for `distinct-cores` the substance is
        # one set PER CPU (that they are pairwise different is the property), so any slice that
        # showed a single set would be showing less than was verified.
        f"  read       : {pinning_verification['flight_pin_verified']}",
        # ...AND THE ALLOCATOR, WITH ITS EVIDENCE (#3551). Never the bare word `jemalloc`: the
        # preload FAILS OPEN (glibc ignores an unloadable object and continues with system
        # malloc), so the only thing worth printing is what was OBSERVED in the running process.
        f"allocator    : flight server ran under {pinning_verification['flight_allocator']}"
        f" (library: {pinning_verification['flight_allocator_lib']})",
        f"  arena      : {pinning_verification['flight_malloc_arena_max']}",
        f"  evidence   : {pinning_verification['flight_allocator_verification']}",
        # THE ENVIRONMENT, immediately after the allocator lines it explains and before the
        # counters: `env injected` is WHERE the allocator above came from, and `env ambient` is
        # the fact a reproduction has to compare (ws0-3552 §4).
        f"env ambient  : {env_ambient}",
        f"env injected : {env_injected}",
        f"counters     : perf stat -C {server_cpus} (bare scan) /"
        f" -C {flight_server_cpus} (Flight)  [CPU-WIDE; no -p anywhere]"
        f"   events: {','.join(events)}",
        # EVERY FIELD THAT CHANGES HOW A NUMBER SHOULD BE READ, IN THE PRINTED REPORT (#3248,
        # roborev job 80 finding 3). These four were added to results.json and NEVER to the human
        # summary, so a PROFILED run -- which pays 1.6-4.3% measured observer overhead and is
        # therefore NOT a baseline -- printed IDENTICALLY to an unprofiled one, and a session whose
        # quiescence was `NOT VERIFIED` printed identically to a certified one. A reader of the
        # printed report could not tell them apart, which makes the machine-readable field a
        # record nobody consults.
        #
        # `profile` and `quiescence` carry an explicit warning rather than a bare value: the value
        # alone requires the reader to know what `on freq=499` implies about the throughput
        # figures above it.
        f"binaries     : {bin_dir}"
        + ("   [SYMBOL-BEARING BUILD]" if "perfsym" in str(bin_dir)
           or "perfprof" in str(bin_dir) else ""),
        # THE BASELINE CLAIM IS MADE IN ONE PLACE AND READ IN TWO (#3248, roborev job 82 F1).
        #
        # This line was a conditional on `profile` ALONE, so with no profiler attached it said
        # "throughput is a baseline" UNCONDITIONALLY -- including on a non-canonical corpus, where
        # the title correctly says NOT A BASELINE. The fix for job 80 F3 therefore introduced the
        # SAME contradiction in the opposite direction, one line below, in the same commit.
        #
        # THE GENERALISABLE FORM: FIXING A CONTRADICTION IN ONE DIRECTION DOES NOT FIX THE PAIR.
        # When two fields must agree, assert the AGREEMENT rather than each field against a
        # constant -- a conditional title and an unconditional string cannot agree by construction.
        f"profile      : {profile}"
        + ("   !! PROFILED — observer overhead is INSIDE the throughput figures above;"
           " these are NOT baseline numbers" if profile != "off"
           else ("   (no sampling profiler attached; throughput is a baseline)" if is_baseline_run
                 else "   (no sampling profiler attached — but this run is NOT a baseline; see the"
                      " title above)")),
        f"quiescence   : {quiescence}"
        + ("" if quiescence.startswith("judged against")
           else "   !! this session was NOT checked for competing load — UNVERIFIED, not quiet"),
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
            tuple(events),
        )
        results["measurements"].append(scan)
        lines.append(f"[{temp.upper()}]")
        lines.append(fmt("bare scan (execute_streaming)", scan, server_cpus))
        lines += prewarm_warning(scan, "bare-scan", temp)
        for arm in arms:
            fl = collect_flight(
                d, temp, arm, reps, corpus_rows, flight_endpoint, tuple(events)
            )
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
            lines.append(fmt(f"flight do_get ({fl['requested_merge_path']} requested)", fl, flight_server_cpus))
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

    # DID EVERY FLIGHT REP ADMIT THE SAME AMOUNT OF WORK (#3551 item 10)? Read back from each
    # rep's own server log rather than pinned: pinning --max-concurrent-scans would change the
    # configuration #3248 measured and would hide exactly this drift.
    #
    # AFTER THE COLLECTION LOOP, deliberately, and that ORDER is a correctness property rather
    # than a preference: the collectors above refuse an ABSENT or malformed rep with a diagnostic
    # naming what is wrong with THAT rep ("collected 0 of 1", "carries 2 step records"), and
    # running this first PREEMPTED all of them — measured, it turned six other suites' specific
    # refusals into "carries no server log", blaming the artifact this check happens to read
    # first. A check that fires before the more specific one makes every diagnostic downstream of
    # it unreachable. By here every selected rep has been established to exist, so an absent
    # server log is genuinely about the log.
    flight_admission = verify_flight_admission(d, temps, arms, reps, flight_rep_tag)
    # WHAT THE FLIGHT SERVER ADMITTED, per rep and agreed. Recorded rather than merely asserted:
    # a reader comparing two sessions needs the ceiling AND its input (available_parallelism),
    # because the ceiling is a FUNCTION of the pin. Assigned here rather than in the results
    # literal above for the ordering reason stated at the call — the value does not exist until
    # the collectors have run, and the collectors must run first.
    results["flight_admission"] = flight_admission
    results["recorded_round_metadata"] = recorded_rounds
    lines += [
        # THE ADMISSION CEILING, printed with the tail rather than the header block — the value
        # cannot exist up there, because this check must run AFTER the collectors whose specific
        # refusals it would otherwise preempt (see the call above).
        f"admission    : max_concurrent_scans={flight_admission['max_concurrent_scans']}"
        f" (source {flight_admission['max_concurrent_scans_source']},"
        f" available_parallelism={flight_admission['available_parallelism']}) —"
        f" OBSERVED IDENTICAL across all {flight_admission['reps_agreeing']} flight rep(s),"
        " read back from each server log; deliberately NOT pinned",
        "",
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
