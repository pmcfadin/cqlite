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
    collect_flight,
    collect_scan,
    prewarm_warning,
)
from ws0_rounds import (  # noqa: E402
    collect_recorded_round_metadata,
    paired_rounds,
    recorded_round_metadata_lines,
)
from ws0_validate import (  # noqa: E402
    Invalid,
    cli_count,
    existing_dir,
    load_corpus_identity,
    nonempty_selection,
    positive_derived,
    verify_corpus_bytes,
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
    reps = cli_count("reps", args.reps)
    scan_passes = cli_count("scan-passes", args.scan_passes)
    d = existing_dir("dir", args.dir)
    corpus = existing_dir("corpus", args.corpus)
    temps = nonempty_selection("temps", args.temps, TEMPS_ALLOWED)
    arms = nonempty_selection("arms", args.arms, ARMS_ALLOWED)

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
    corpus_rows = identity["rows"]
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
        "pinning": {
            "server_cpus": args.server_cpus,
            "client_cpus": args.client_cpus,
            "counter_mode": f"perf stat -C {args.server_cpus} (CPU-WIDE; never -p)",
            "verified": "thread_siblings_list, fail-closed (scripts/perf/lib-cpu.sh)",
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
        "step_duration": args.step_duration,
        "scan_passes": scan_passes,
        "measurements": [],
    }

    lines = [
        "",
        "==== WS0 SAME-SESSION BASELINE (issue #3096 rig, hardened #3272) ====",
        f"corpus       : {corpus}",
        f"corpus sha256: {identity['data_db_sha256']}",
        *corpus_identity_lines(identity_verification),
        f"corpus shape : {identity['rows']} rows / "
        f"{identity['partitions']} partitions / "
        f"{identity['bytes_per_row']:.2f} B/row",
        f"pinning      : server {args.server_cpus} (verified physical-core siblings), "
        f"client {args.client_cpus}",
        f"counters     : perf stat -C {args.server_cpus}  [CPU-WIDE; no -p anywhere]",
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
        scan = collect_scan(d, temp, reps)
        results["measurements"].append(scan)
        lines.append(f"[{temp.upper()}]")
        lines.append(fmt("bare scan (execute_streaming)", scan))
        lines += prewarm_warning(scan, "bare-scan", temp)
        for arm in arms:
            fl = collect_flight(d, temp, arm, reps, corpus_rows)
            results["measurements"].append(fl)
            lines.append(fmt(f"flight do_get ({arm})", fl))
            lines += prewarm_warning(fl, f"flight/{arm}", temp)
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
        "  * only the SELECTION printed above was measured; an absent temperature or "
        "arm was NOT run and nothing here speaks to it (results.json .selection).",
        "  * every COLD flight rep is verified to be EXACTLY ONE successful request "
        "(requests_ok == 1) and every rep's rows an exact multiple of the corpus row "
        "count, so no warm request can be reported inside a cold figure; a rep that "
        "violates either is REFUSED, not blended. The corpus row count is REQUIRED "
        "(an absent corpus-identity.json is fatal), so this check can never be "
        "skipped while these notes claim it ran (#3272).",
        "  * every figure is rows/s AND cycles/row; no CPU-share is reported "
        "(a share shift with unmoved rows/s is a FAIL, spec R1).",
        "  * the bare scan's cycles are SETUP-SUBTRACTED (a separately measured "
        "--setup-only perf window); the Flight arm's setup is outside its window. "
        "BOTH counters were observed — an absent or uncounted perf event is fatal, "
        "never a 0 (#3272).",
        "  * `cycles` is summed over BOTH SMT siblings of the pinned physical core, "
        "so cycles/row is a per-physical-core figure counted on two hardware threads.",
        "    Both arms are counted identically, so the ratio and the arm-to-arm "
        "delta are unaffected.",
        "  * every rep of BOTH arms records its PREWARM outcome in results.json "
        "(prewarm/prewarm_all_ok); a degraded prewarm is flagged above, never swallowed.",
        "    A warm rep is prewarmed by an UNTIMED full pass outside its perf window; "
        "the cold arm is deliberately never prewarmed, and its `skipped-cold-arm` "
        "sentinel satisfies the requirement for a COLD rep ONLY (#3272).",
        "  * the corpus identity is verified against the BYTES MEASURED, not trusted "
        "from corpus-identity.json: the recorded size is always re-stat'ed and the "
        "recorded sha256 re-derived from the Data.db unless --skip-corpus-digest was "
        "passed, in which case the line above says CORPUS DIGEST UNVERIFIED (#3272).",
        "  * the corpus is CQLite-written + CQLite-read: a PERFORMANCE FIXTURE ONLY "
        "(#3042), never a correctness oracle.",
        "  * the #3058/#3100 absolutes (240,100 / 312,155 rows/s) were corpus- and "
        "machine-bound and are NOT reproduced here.",
        "",
    ]
    return results, lines


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True)
    ap.add_argument("--corpus", required=True)
    ap.add_argument("--server-cpus", required=True)
    ap.add_argument("--client-cpus", required=True)
    # Deliberately NOT `type=int`: argparse would exit 2 with its own message for a
    # non-integer, but would happily accept `0` and `-3`. The validation is in
    # ws0_validate.cli_count, where both cases fail with a reason (#3272 f5).
    ap.add_argument("--reps", required=True)
    ap.add_argument("--temps", required=True)
    ap.add_argument("--arms", required=True)
    ap.add_argument("--step-duration", required=True)
    ap.add_argument("--scan-passes", required=True)
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
