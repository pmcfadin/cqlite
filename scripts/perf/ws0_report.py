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
import statistics
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from ws0_validate import (  # noqa: E402  (path set above; stdlib-only, no deps)
    Invalid,
    classify_prewarm,
    existing_dir,
    load_corpus_identity,
    nonempty_selection,
    positive_int,
    read_perf_counters,
    require_complete,
)

TEMPS_ALLOWED = ("warm", "cold")
ARMS_ALLOWED = ("bypass", "merge")
# The events every perf leg must carry. Named here so an absent one is reported by
# name rather than defaulting to zero.
REQUIRED_EVENTS = ("cycles", "instructions")


def spread(values: list[float]) -> dict[str, float]:
    lo, hi = min(values), max(values)
    med = statistics.median(values)
    return {
        "median": med,
        "min": lo,
        "max": hi,
        "spread_abs": hi - lo,
        "spread_pct_of_median": (hi - lo) / med * 100.0 if med else 0.0,
        "n": len(values),
    }


def read_prewarm(d: pathlib.Path, tag: str) -> str:
    """The prewarm outcome THIS rep recorded, or `unrecorded`.

    Absent file => the driver predates the recording, or the rep died before its
    prewarm. Either way the warm/cold separation is UNVERIFIED for that rep, which
    is reported rather than assumed healthy (issue #3096 review, finding 1).
    """
    p = d / f"{tag}.prewarm.status"
    return p.read_text().strip() if p.exists() else "unrecorded"


def prewarm_block(prewarm: list[dict], temp: str) -> dict:
    """The prewarm record + the single `prewarm_all_ok` field a reader can check.

    `classify_prewarm` is TEMPERATURE-SCOPED (#3272 finding 2): the cold arm's
    `skipped-cold-arm` sentinel can only satisfy a COLD rep, and raises on a warm
    one instead of quietly counting as success.
    """
    return {
        "prewarm": prewarm,
        "prewarm_all_ok": all(
            classify_prewarm(temp, p["status"]) == "ok" for p in prewarm
        ),
        "prewarm_required_status": temp,
    }


def prewarm_warning(block: dict, arm_label: str, temp: str) -> list[str]:
    """The loud summary line for a degraded prewarm. Never swallowed."""
    if block.get("prewarm_all_ok", True):
        return []
    degraded = [
        p for p in block["prewarm"] if classify_prewarm(temp, p["status"]) != "ok"
    ]
    return [
        f"      !! PREWARM DEGRADED on {arm_label} rep(s) "
        + ", ".join(f"{p['rep']}={p['status']}" for p in degraded)
        + " — this 'warm' figure may be partly cold; the warm/cold separation"
        " (spec R2/AC5) is UNVERIFIED for those reps"
    ]


def collect_scan(d: pathlib.Path, temp: str, reps: int) -> dict:
    rows_per_sec: list[float] = []
    cycles_per_row: list[float] = []
    ipc: list[float] = []
    rows_total = 0
    setup_cycles_total = 0
    per_rep = []
    missing: list[str] = []
    prewarm: list[dict] = []
    for rep in range(1, reps + 1):
        tag = f"scan-{temp}-{rep}"
        payload_path = d / f"{tag}.json"
        if not payload_path.exists():
            # A rep whose artifacts are missing is NOT a smaller sample: it is an
            # incomplete run, and silently `continue`ing it published a median over
            # fewer reps than the caller asked for with only `n=` to say so (issue
            # #3096 review). Fail instead — see require_complete below.
            missing.append(payload_path.name)
            continue
        payload = json.loads(payload_path.read_text())
        rows = int(payload["rows_denominator"])
        if rows == 0:
            raise Invalid(f"bare-scan rep {tag} observed ZERO rows — not a measurement")
        secs = float(payload["timed_scan_secs"])
        # Both legs' counters must be OBSERVED. `.get("cycles", 0)` used to
        # fabricate a zero here, so a run with no setup artifact at all was
        # reported "SETUP-SUBTRACTED" having subtracted nothing (#3272 finding 4).
        total = read_perf_counters(d / f"perf-{tag}.csv", f"{tag} (full run)", REQUIRED_EVENTS)
        setup = read_perf_counters(
            d / f"perf-{tag}-setup.csv", f"{tag} (setup-only leg)", REQUIRED_EVENTS
        )
        # Setup SUBTRACTED (spec R2). A non-positive result would mean the setup
        # leg somehow cost more than the full run, which is a broken measurement,
        # not a small number — surfaced rather than hidden.
        cyc = total["cycles"] - setup["cycles"]
        ins = total["instructions"] - setup["instructions"]
        if cyc <= 0:
            raise Invalid(
                f"{tag} setup-subtracted cycles are {cyc} (total="
                f"{total['cycles']}, setup={setup['cycles']}) — "
                "the subtraction is not meaningful; re-run"
            )
        # This arm's prewarm outcome, recorded by ws0-baseline.sh exactly as the
        # Flight arm's is (issue #3096 review, finding 1): the bare scan is the
        # DENOMINATOR of the 1.3x ratio, so an unprewarmed "warm" rep biases the
        # ratio in the claim's favour and must be visible in the artifact.
        prewarm.append({"rep": rep, "status": read_prewarm(d, tag)})
        rows_per_sec.append(rows / secs)
        cycles_per_row.append(cyc / rows)
        ipc.append(ins / cyc)
        rows_total += rows
        setup_cycles_total += setup["cycles"]
        per_rep.append(
            {
                "rep": rep,
                "rows": rows,
                "secs": secs,
                "rows_per_sec": rows / secs,
                "cycles_total": total["cycles"],
                "cycles_setup": setup["cycles"],
                "cycles_scan": cyc,
                "cycles_per_row": cyc / rows,
                "setup_secs": payload.get("setup_secs"),
                "prewarm": prewarm[-1]["status"],
            }
        )
    # This (arm, temperature) was SELECTED by the caller — an unselected one is
    # never iterated — so it must be complete (#3272 finding 6).
    require_complete(f"bare scan ({temp})", per_rep, reps, missing)
    return {
        "arm": "bare_scan",
        "surface": "cqlite_core::Database::execute_streaming",
        "temperature": temp,
        "rows_per_sec": spread(rows_per_sec),
        "cycles_per_row": spread(cycles_per_row),
        "ipc": spread(ipc),
        "row_denominator_total": rows_total,
        "setup_cycles_subtracted_total": setup_cycles_total,
        # Issue #3096 review, finding 1: the warm bare-scan arm had no untimed
        # prewarm at all, so `prewarm_all_ok` here is the single field a reader can
        # check that this arm's "warm" really was warm.
        **prewarm_block(prewarm, temp),
        "reps": per_rep,
    }


def expected_requests(temp: str) -> str:
    """The request count a rep of this temperature MUST have, stated not implied.

    Issue #3096 review, finding 2: this used to be reconstructible only by dividing
    a rep's `rows` by the corpus row count in your head. Recorded explicitly here so
    a reader sees the contract, and asserted per rep by `check_request_count`.
    """
    if temp == "cold":
        return "exactly 1 (only the first request after the cache drop is cold)"
    return ">=1, each one a full corpus scan"


def check_request_count(
    tag: str, temp: str, requests_ok: object, rows: int, corpus_rows: int
) -> int:
    """Assert the per-temperature request contract, or raise.

    Two properties, both fail-closed (issue #3096 review, finding 2):

    * A **cold** rep must have completed **exactly one** successful request. The
      driver keeps `--cold-step-duration` short precisely so the loadgen issues one
      request, but that is a duration heuristic against an unknown scan time — if
      the corpus finishes inside the step, requests 2..N read the pages request 1
      faulted in and their WARM rows land in a figure labelled "cold". A caller can
      also trigger it directly by raising the option. So the OBSERVED count is
      checked, not the duration.
    * Every rep's rows must be an exact multiple of the corpus row count, i.e.
      `rows == requests_ok * corpus_rows`. A remainder means some request did not
      scan the whole corpus, so the per-request row denominator is not what the
      report says it is.

    `corpus_rows` is a REQUIRED int, never `None`: an absent corpus identity used to
    disable the second property silently while the NOTES claimed it ran (#3272
    finding 1), so the identity is now loaded fail-closed by
    `ws0_validate.load_corpus_identity` before any of this runs.
    """
    if requests_ok is None:
        raise Invalid(
            f"flight rep {tag} step record carries no `requests_ok` — the"
            " per-temperature request contract cannot be verified, and an unverified"
            " cold rep is exactly how warm requests get reported as cold"
        )
    count = int(requests_ok)
    if count < 1:
        raise Invalid(f"flight rep {tag} completed {count} successful requests")
    if temp == "cold" and count != 1:
        raise Invalid(
            f"flight COLD rep {tag} completed {count} successful requests;"
            f" expected {expected_requests('cold')}."
            " Only the FIRST request after the cache drop is cold: requests 2..N read the"
            " pages request 1 faulted in, so their WARM rows would be blended into a figure"
            " reported as 'cold', which spec R2/AC5 forbids."
            " Lower --cold-step-duration so the loadgen issues a single request, or measure"
            " this as a warm rep."
        )
    if rows % corpus_rows != 0 or rows // corpus_rows != count:
        raise Invalid(
            f"flight rep {tag} observed {rows:,} rows over {count} successful"
            f" request(s), which is not {count} x the corpus row count"
            f" ({corpus_rows:,}). At least one request did not scan the whole corpus,"
            " so the per-request row denominator is not the one this report would"
            " print. Re-run rather than reporting a partial scan."
        )
    return count


def collect_flight(
    d: pathlib.Path, temp: str, arm: str, reps: int, corpus_rows: int
) -> dict:
    rows_per_sec: list[float] = []
    cycles_per_row: list[float] = []
    ipc: list[float] = []
    rows_total = 0
    per_rep = []
    missing: list[str] = []
    prewarm: list[dict] = []
    for rep in range(1, reps + 1):
        tag = f"flight-{arm}-{temp}-{rep}"
        jsonl = d / f"{tag}.jsonl"
        if not jsonl.exists():
            missing.append(jsonl.name)
            continue
        records = [json.loads(x) for x in jsonl.read_text().splitlines() if x.strip()]
        if not records:
            raise Invalid(f"flight rep {tag} produced no step record")
        rec = records[-1]
        rows = int(rec["rows_total"])
        if rows == 0:
            raise Invalid(f"flight rep {tag} observed ZERO rows — not a measurement")
        if int(rec.get("requests_error", 0)) > 0:
            raise Invalid(f"flight rep {tag} had {rec['requests_error']} failed request(s)")
        requests_ok = check_request_count(tag, temp, rec.get("requests_ok"), rows, corpus_rows)
        # The prewarm outcome for THIS rep, recorded by ws0-baseline.sh.
        prewarm.append({"rep": rep, "status": read_prewarm(d, tag)})
        counters = read_perf_counters(d / f"perf-{tag}.csv", tag, REQUIRED_EVENTS)
        cyc = counters["cycles"]
        ins = counters["instructions"]
        if cyc <= 0:
            raise Invalid(f"flight rep {tag} recorded no cycles — perf -C window was empty")
        rows_per_sec.append(float(rec["rows_per_s"]))
        cycles_per_row.append(cyc / rows)
        ipc.append(ins / cyc)
        rows_total += rows
        per_rep.append(
            {
                "rep": rep,
                "rows": rows,
                "requests_ok": requests_ok,
                "requests_expected": expected_requests(temp),
                "rows_per_scan_observed": rows / requests_ok,
                "rows_per_scan_expected": corpus_rows,
                "duration_s": rec.get("duration_s"),
                "rows_per_sec": float(rec["rows_per_s"]),
                "cycles": cyc,
                "cycles_per_row": cyc / rows,
                "prewarm": prewarm[-1]["status"],
            }
        )
    require_complete(f"flight do_get {arm} ({temp})", per_rep, reps, missing)
    return {
        "arm": f"flight_do_get_{arm}",
        "surface": "arrow_flight FlightService::do_get (loopback gRPC)",
        "temperature": temp,
        "forced_merge_path": arm,
        "rows_per_sec": spread(rows_per_sec),
        "cycles_per_row": spread(cycles_per_row),
        "ipc": spread(ipc),
        "row_denominator_total": rows_total,
        # Issue #3096 review, finding 2: the request count each temperature MUST
        # have, asserted per rep by `check_request_count` and stated here so no
        # reader has to reconstruct it from row denominators.
        "requests_expected_per_rep": expected_requests(temp),
        # Unconditionally true now: the corpus identity is REQUIRED, so this can
        # never be a report that skipped the check while claiming it (#3272 f1).
        "full_corpus_per_request_verified": True,
        "corpus_rows_used_for_verification": corpus_rows,
        "setup_cycles_subtracted_total": 0,
        "setup_note": (
            "server start + (warm only) prewarm happen BEFORE the perf window opens, "
            "so setup is outside the window by construction rather than subtracted"
        ),
        # Issue #3096 review: a failed prewarm silently degraded a "warm" claim.
        # Every rep's outcome is recorded here, and `prewarm_all_ok` is the single
        # field a reader can check.
        **prewarm_block(prewarm, temp),
        "reps": per_rep,
    }


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


def build_report(args: argparse.Namespace) -> tuple[dict, list[str]]:
    """The whole report, or `Invalid`. No fabricated value anywhere in here."""
    reps = positive_int("reps", args.reps)
    scan_passes = positive_int("scan-passes", args.scan_passes)
    d = existing_dir("dir", args.dir)
    corpus = existing_dir("corpus", args.corpus)
    temps = nonempty_selection("temps", args.temps, TEMPS_ALLOWED)
    arms = nonempty_selection("arms", args.arms, ARMS_ALLOWED)

    # REQUIRED, fail-closed: an absent identity used to silently disable the
    # full-corpus-per-request assert while the NOTES claimed it ran (#3272 f1).
    identity = load_corpus_identity(corpus)
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
            scan_rps = scan["rows_per_sec"]["median"]
            fl_rps = fl["rows_per_sec"]["median"]
            ratio = scan_rps / fl_rps if fl_rps else float("inf")
            target = scan_rps / 1.3
            verdict = "PASS" if fl_rps >= target else "BELOW TARGET"
            lines.append(
                f"      ratio bare/flight = {ratio:.2f}x   "
                f"1.3x target => do_get must reach {target:,.0f} rows/s   [{verdict}]"
            )
            lines.append(
                f"      cycles/row delta  = "
                f"{fl['cycles_per_row']['median'] - scan['cycles_per_row']['median']:+,.0f} "
                f"({(fl['cycles_per_row']['median'] / scan['cycles_per_row']['median'] - 1) * 100:+.1f}%)"
            )
        lines.append("")

    lines += [
        "NOTES",
        "  * warm and cold are SEPARATE claims above; nothing here is blended.",
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
    # ws0_validate.positive_int, where both cases fail with a reason (#3272 f5).
    ap.add_argument("--reps", required=True)
    ap.add_argument("--temps", required=True)
    ap.add_argument("--arms", required=True)
    ap.add_argument("--step-duration", required=True)
    ap.add_argument("--scan-passes", required=True)
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
