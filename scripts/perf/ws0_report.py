#!/usr/bin/env python3
"""Aggregate one `ws0-baseline.sh` session into results.json + a human summary.

Reporting rules this file enforces, from issue #3096 spec R1/R2:

* Every figure is **rows/s AND cycles/row**. There is deliberately no code path
  here that emits a CPU-SHARE ("% of cycles in X"): a share can fall while rows/s
  is unmoved, which the spec records as a FAIL, so the rig never produces the
  number that could be mistaken for a win.
* **Warm and cold are separate rows**, never averaged into one claim.
* The **median** of N reps is reported and the **spread** (min..max, and its
  percentage of the median) is printed beside it. No silent mean.
* **Setup is subtracted** from the bare scan's cycles: the driver measured a
  `--setup-only` leg under its own perf window, and `cycles_scan =
  cycles_total - cycles_setup`.
* The **row denominator is printed with every figure**, so no derived number is
  divisible by an unstated count.
* **Zero rows exits non-zero** rather than reporting a measurement.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import statistics
import sys


def read_perf_csv(path: pathlib.Path) -> dict[str, int]:
    """Sum `perf stat -x,` counters by event name.

    Summed across the CPUs in the `-C` set: `perf stat -C a,b` emits one line per
    event, already aggregated, but a `--per-core` variant would emit several — so
    summing is correct in both shapes and never silently drops a line.
    """
    counters: dict[str, int] = {}
    if not path.exists():
        return counters
    for line in path.read_text().splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        fields = line.split(",")
        if len(fields) > 2 and fields[0].strip():
            try:
                counters[fields[2].strip()] = counters.get(fields[2].strip(), 0) + int(fields[0])
            except ValueError:
                continue
    return counters


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


def require_complete(label: str, per_rep: list, reps: int, missing: list[str]) -> None:
    """FAIL when fewer than `reps` reps were collected (issue #3096 review).

    A rep with missing artifacts used to be silently skipped, so a session that
    lost half its reps still printed a median — with only the `n=` field to betray
    it, in a report whose whole contract is "median of N with the spread stated".
    An INCOMPLETE collection is not a smaller sample of the same claim; it is a
    different claim, and it is more often a crashed rep than a deliberate one.

    Two cases are deliberately distinguished:

    * `per_rep` empty AND nothing missing -> this (arm, temperature) was never
      run; the caller's `--temp`/`--arm` selection says so. Not an error.
    * `per_rep` empty but artifacts missing, or a PARTIAL collection -> fatal.
    """
    if not per_rep and not missing:
        return
    if len(per_rep) < reps:
        sys.exit(
            f"FATAL: {label} collected {len(per_rep)} of {reps} requested reps"
            f" — missing artifacts: {', '.join(missing) or '<none named>'}."
            " A median over fewer reps than requested is a different claim than the"
            " one asked for; re-run the missing reps rather than reporting this."
        )


def collect_scan(d: pathlib.Path, temp: str, reps: int) -> dict:
    rows_per_sec: list[float] = []
    cycles_per_row: list[float] = []
    ipc: list[float] = []
    rows_total = 0
    setup_cycles_total = 0
    per_rep = []
    missing: list[str] = []
    for rep in range(1, reps + 1):
        tag = f"scan-{temp}-{rep}"
        payload_path = d / f"{tag}.json"
        if not payload_path.exists():
            # A rep whose artifacts are missing is NOT a smaller sample: it is an
            # incomplete run, and silently `continue`ing it published a median over
            # fewer reps than the caller asked for with only `n=` to say so (issue
            # #3096 review). Fail instead.
            missing.append(payload_path.name)
            continue
        payload = json.loads(payload_path.read_text())
        rows = int(payload["rows_denominator"])
        if rows == 0:
            sys.exit(f"FATAL: bare-scan rep {tag} observed ZERO rows — not a measurement")
        secs = float(payload["timed_scan_secs"])
        total = read_perf_csv(d / f"perf-{tag}.csv")
        setup = read_perf_csv(d / f"perf-{tag}-setup.csv")
        # Setup SUBTRACTED (spec R2). Clamped at 0: a negative would mean the
        # setup leg somehow cost more than the full run, which is a broken
        # measurement, not a small number — surfaced rather than hidden.
        cyc = total.get("cycles", 0) - setup.get("cycles", 0)
        ins = total.get("instructions", 0) - setup.get("instructions", 0)
        if cyc <= 0:
            sys.exit(
                f"FATAL: {tag} setup-subtracted cycles are {cyc} (total="
                f"{total.get('cycles', 0)}, setup={setup.get('cycles', 0)}) — "
                "the subtraction is not meaningful; re-run"
            )
        rows_per_sec.append(rows / secs)
        cycles_per_row.append(cyc / rows)
        ipc.append(ins / cyc if cyc else 0.0)
        rows_total += rows
        setup_cycles_total += setup.get("cycles", 0)
        per_rep.append(
            {
                "rep": rep,
                "rows": rows,
                "secs": secs,
                "rows_per_sec": rows / secs,
                "cycles_total": total.get("cycles", 0),
                "cycles_setup": setup.get("cycles", 0),
                "cycles_scan": cyc,
                "cycles_per_row": cyc / rows,
                "setup_secs": payload.get("setup_secs"),
            }
        )
    if not per_rep:
        # Nothing collected at all: this (arm, temperature) was not run — the
        # caller's `--temp`/`--arm` selection decides that, so it is not an error.
        # A PARTIAL collection is.
        require_complete(f"bare scan ({temp})", per_rep, reps, missing)
        return {}
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
        "reps": per_rep,
    }


def collect_flight(d: pathlib.Path, temp: str, arm: str, reps: int) -> dict:
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
            sys.exit(f"FATAL: flight rep {tag} produced no step record")
        rec = records[-1]
        rows = int(rec["rows_total"])
        if rows == 0:
            sys.exit(f"FATAL: flight rep {tag} observed ZERO rows — not a measurement")
        if int(rec.get("requests_error", 0)) > 0:
            sys.exit(f"FATAL: flight rep {tag} had {rec['requests_error']} failed request(s)")
        # The prewarm outcome for THIS rep, recorded by ws0-baseline.sh. Absent
        # file => the driver predates the recording (or the rep died before the
        # prewarm), which is itself reported rather than assumed healthy.
        status_path = d / f"{tag}.prewarm.status"
        prewarm.append(
            {
                "rep": rep,
                "status": (
                    status_path.read_text().strip() if status_path.exists() else "unrecorded"
                ),
            }
        )
        counters = read_perf_csv(d / f"perf-{tag}.csv")
        cyc = counters.get("cycles", 0)
        ins = counters.get("instructions", 0)
        if cyc <= 0:
            sys.exit(f"FATAL: flight rep {tag} recorded no cycles — perf -C window was empty")
        rows_per_sec.append(float(rec["rows_per_s"]))
        cycles_per_row.append(cyc / rows)
        ipc.append(ins / cyc if cyc else 0.0)
        rows_total += rows
        per_rep.append(
            {
                "rep": rep,
                "rows": rows,
                "requests_ok": rec.get("requests_ok"),
                "rows_per_scan_observed": (
                    rows / rec["requests_ok"] if rec.get("requests_ok") else None
                ),
                "duration_s": rec.get("duration_s"),
                "rows_per_sec": float(rec["rows_per_s"]),
                "cycles": cyc,
                "cycles_per_row": cyc / rows,
                "prewarm": prewarm[-1]["status"],
            }
        )
    if not per_rep:
        require_complete(f"flight do_get {arm} ({temp})", per_rep, reps, missing)
        return {}
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
        "setup_cycles_subtracted_total": 0,
        "setup_note": (
            "server start + (warm only) prewarm happen BEFORE the perf window opens, "
            "so setup is outside the window by construction rather than subtracted"
        ),
        # Issue #3096 review: a failed prewarm silently degraded a "warm" claim.
        # Every rep's outcome is recorded here, and `prewarm_all_ok` is the single
        # field a reader can check.
        "prewarm": prewarm,
        "prewarm_all_ok": all(
            p["status"] in ("ok", "skipped-cold-arm") for p in prewarm
        ),
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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True)
    ap.add_argument("--corpus", required=True)
    ap.add_argument("--server-cpus", required=True)
    ap.add_argument("--client-cpus", required=True)
    ap.add_argument("--reps", type=int, required=True)
    ap.add_argument("--temps", required=True)
    ap.add_argument("--arms", required=True)
    ap.add_argument("--step-duration", required=True)
    ap.add_argument("--scan-passes", required=True)
    args = ap.parse_args()

    d = pathlib.Path(args.dir)
    temps = args.temps.split()
    arms = args.arms.split()

    identity = {}
    idp = pathlib.Path(args.corpus) / "corpus-identity.json"
    if idp.exists():
        identity = json.loads(idp.read_text())

    results = {
        "issue": "#3096",
        "corpus": args.corpus,
        "corpus_identity": {
            k: identity.get(k)
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
        "reps": args.reps,
        "step_duration": args.step_duration,
        "scan_passes": int(args.scan_passes),
        "measurements": [],
    }

    lines = [
        "",
        "==== ISSUE #3096 SAME-SESSION BASELINE ====",
        f"corpus       : {args.corpus}",
        f"corpus sha256: {identity.get('data_db_sha256', '<unrecorded>')}",
        f"corpus shape : {identity.get('rows', '?')} rows / "
        f"{identity.get('partitions', '?')} partitions / "
        f"{identity.get('bytes_per_row', 0):.2f} B/row",
        f"pinning      : server {args.server_cpus} (verified physical-core siblings), "
        f"client {args.client_cpus}",
        f"counters     : perf stat -C {args.server_cpus}  [CPU-WIDE; no -p anywhere]",
        f"reps         : {args.reps} (median reported, spread shown)",
        "",
    ]

    for temp in temps:
        scan = collect_scan(d, temp, args.reps)
        if not scan:
            continue
        results["measurements"].append(scan)
        lines.append(f"[{temp.upper()}]")
        lines.append(fmt("bare scan (execute_streaming)", scan))
        for arm in arms:
            fl = collect_flight(d, temp, arm, args.reps)
            if not fl:
                continue
            results["measurements"].append(fl)
            lines.append(fmt(f"flight do_get ({arm})", fl))
            if not fl.get("prewarm_all_ok", True):
                degraded = [p for p in fl["prewarm"] if p["status"] not in ("ok", "skipped-cold-arm")]
                lines.append(
                    "      !! PREWARM DEGRADED on rep(s) "
                    + ", ".join(f"{p['rep']}={p['status']}" for p in degraded)
                    + " — this 'warm' figure is partly cold (biased AGAINST do_get)"
                )
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
        "  * every figure is rows/s AND cycles/row; no CPU-share is reported "
        "(a share shift with unmoved rows/s is a FAIL, spec R1).",
        "  * the bare scan's cycles are SETUP-SUBTRACTED (a separately measured "
        "--setup-only perf window); the Flight arm's setup is outside its window.",
        "  * `cycles` is summed over BOTH SMT siblings of the pinned physical core, "
        "so cycles/row is a per-physical-core figure counted on two hardware threads.",
        "    Both arms are counted identically, so the ratio and the arm-to-arm "
        "delta are unaffected.",
        "  * every flight rep's PREWARM outcome is recorded in results.json "
        "(prewarm/prewarm_all_ok); a degraded prewarm is flagged above, never swallowed.",
        "  * the corpus is CQLite-written + CQLite-read: a PERFORMANCE FIXTURE ONLY "
        "(#3042), never a correctness oracle.",
        "  * the #3058/#3100 absolutes (240,100 / 312,155 rows/s) were corpus- and "
        "machine-bound and are NOT reproduced here.",
        "",
    ]

    (d / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
