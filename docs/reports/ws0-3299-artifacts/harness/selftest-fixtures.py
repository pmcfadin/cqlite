#!/usr/bin/env python3
"""Synthetic fixtures for `selftest.sh` — hermetic: no cargo, perf, sudo or corpus.

Each `--case` builds a rep directory that is well-formed EXCEPT for the one
property the guard under test must reject, so a firing guard proves it rejected
that property and not something incidental.

The `good` case is also a NUMERICAL control: its numbers are chosen so the
correct attribution is exact and known in advance (each worker emits a record
every 100 ms carrying exactly 30,000 more rows, i.e. 300,000 rows/s, and the
window boundaries land exactly on record timestamps), so `selftest.sh` can assert
the aggregate the guard computes rather than merely that it exited 0. A guard
suite in which everything fails is as broken as one in which nothing does.
"""

import argparse
import json
import os

NS = 1_000_000_000
T0 = 1 * NS           # window opens at t=1s
T1 = 61 * NS          # window closes at t=61s  (span exactly 60s)
STEP_NS = NS // 10    # a progress record every 100 ms
ROWS_PER_STEP = 30_000  # => 300,000 rows/s per worker, exactly


def write_progress(path, first_ns, last_ns, step_ns, rows_per_step, start_rows=0):
    n = 0
    with open(path, "w") as fh:
        t = first_ns
        while t <= last_ns:
            fh.write(json.dumps({"t_ns": t, "rows": start_rows + n * rows_per_step}) + "\n")
            t += step_ns
            n += 1
    return n


def write_summary(path, worker_id, affinity):
    with open(path, "w") as fh:
        json.dump(
            {
                "arm": "bare_scan",
                "worker_id": worker_id,
                "observed_affinity": affinity,
                "rows_total": 1,
                "prewarm_rows": 1,
            },
            fh,
        )


def build(case, d, workers):
    os.makedirs(d, exist_ok=True)
    # A well-formed perf.csv accompanies every window fixture, so the
    # counter-window check has something to read; `window-drift` supplies a
    # deliberately inconsistent one.
    perf_csv(
        os.path.join(d, "perf.csv"),
        "window-drift" if case == "window-drift" else "good",
        ncpus=2 * workers,
    )
    t0, t1 = T0, T1
    worker_cpus = [[i, i + 8] for i in range(workers)]

    for i in range(workers):
        prog = os.path.join(d, f"worker-{i}.progress.jsonl")
        summ = os.path.join(d, f"worker-{i}.summary.json")
        affinity = list(worker_cpus[i])

        # Default: records from 0.5 s to 61.5 s, so the window sits strictly
        # inside and both boundaries land exactly on a record.
        first, last, step, rps = T0 - NS // 2, T1 + NS // 2, STEP_NS, ROWS_PER_STEP

        if case == "late-start" and i == workers - 1:
            # First record AFTER the window opened: this worker was not yet
            # producing rows for the whole window.
            first = T0 + 2 * NS
        elif case == "early-stop" and i == workers - 1:
            # Last record BEFORE the window closed: it stopped inside the window.
            last = T1 - 2 * NS
        elif case == "shortfall":
            # 1 s sample interval, offset so the window opens 0.4 s after a
            # record and closes 0.6 s after one: 1.0 s of 60 s = 1.667%
            # unattributed, over the 0.5% bound.
            first, last, step = T0 - NS + (4 * NS) // 10, T1 + NS, NS
            rps = ROWS_PER_STEP * 10
        elif case == "zero-rows":
            rps = 0
        elif case == "one-sample" and i == workers - 1:
            first, last = T0 - NS // 2, T0 - NS // 2

        write_progress(prog, first, last, step, rps)
        if case == "missing-summary" and i == workers - 1:
            if os.path.exists(summ):
                os.unlink(summ)
        elif case == "affinity" and i == workers - 1:
            # Ran on ONE thread of the pair: the half-populated-core failure.
            write_summary(summ, i, [worker_cpus[i][0]])
        else:
            write_summary(summ, i, affinity)

    if case == "missing-progress":
        os.unlink(os.path.join(d, f"worker-{workers - 1}.progress.jsonl"))
    if case == "bad-span":
        t0, t1 = T1, T0
    if case == "no-task-clock":
        # A perf.csv with every event EXCEPT task-clock: the counter-window check
        # then has no evidence, which must FAIL rather than be skipped.
        rows = [ln for ln in open(os.path.join(d, "perf.csv")) if "task-clock" not in ln]
        with open(os.path.join(d, "perf.csv"), "w") as fh:
            fh.writelines(rows)

    if case != "no-window":
        win = {
                    # S = pinned cores, N = concurrent streams. The fixtures pin
                    # one core per worker, so here they coincide numerically;
                    # they are still written as SEPARATE fields, because the
                    # guard must read the stream count from `n`.
            "s": workers,
            "n": workers,
            "rep": 1,
            "round": 1,
            "t0_ns": t0,
            "t1_ns": t1,
            "worker_cpus": worker_cpus,
            "events": "instructions,cycles,L1-dcache-loads,L1-dcache-load-misses,task-clock",
            "perf_cpus": ",".join(str(c) for g in worker_cpus for c in g),
            "perf_csv": "perf.csv",
        }
        # FAIL-OPEN cases: a field whose absence used to SKIP its own check and
        # still return success. Each must now be refused.
        if case == "no-worker-cpus":
            del win["worker_cpus"]
        elif case == "no-perf-csv":
            del win["perf_csv"]
        elif case == "no-perf-cpus":
            del win["perf_cpus"]
        elif case == "empty-perf-cpus":
            win["perf_cpus"] = ""            # falsy -> caught as MISSING
        elif case == "degenerate-perf-cpus":
            win["perf_cpus"] = ","           # TRUTHY but names no CPU -> MALFORMED
        elif case == "short-worker-cpus":
            win["worker_cpus"] = worker_cpus[:-1]
        elif case == "uncounted-worker-cpu":
            # A CPU a worker RAN ON that perf did not count. `worker_cpus` and the
            # workers' own affinity readback still agree, so the affinity check
            # passes; only the counted-vs-worked comparison can see this.
            win["perf_cpus"] = ",".join(
                str(c) for g in worker_cpus for c in g if c != worker_cpus[-1][-1])
        elif case == "idle-counted-cpu":
            # A CPU perf counted that NO worker used: its cycles are attributed to
            # these workers' rows.
            win["perf_cpus"] = win["perf_cpus"] + ",99"
        elif case == "duplicate-perf-cpu":
            # The same CPU named twice inflates the counted-CPU count.
            win["perf_cpus"] = win["perf_cpus"] + f",{worker_cpus[0][0]}"
        elif case == "noninteger-perf-cpu":
            win["perf_cpus"] = win["perf_cpus"] + ",cpu7"
        with open(os.path.join(d, "window.json"), "w") as fh:
            json.dump(win, fh)


def perf_csv(path, case, ncpus=2):
    """`perf stat -x,` rows: value,unit,event,run_time_ns,pct_running.

    `task-clock` is written as `window x ncpus` in ns, which is what CPU-wide
    counting produces by construction and what `WINDOW_COUNTER_MISMATCH` checks.
    """
    rows = [
        ("120000000000", "instructions", "100.00"),
        ("60000000000", "cycles", "100.00"),
        ("30000000000", "L1-dcache-loads", "100.00"),
        ("900000000", "L1-dcache-load-misses", "100.00"),
        (str((T1 - T0) * ncpus), "task-clock", "100.00"),  # window x ncpus, in ns
    ]
    if case == "not-counted":
        rows[3] = ("<not counted>", "L1-dcache-load-misses", "100.00")
    elif case == "not-supported":
        rows[3] = ("<not supported>", "L1-dcache-load-misses", "0.00")
    elif case == "multiplexed":
        rows[1] = ("60000000000", "cycles", "87.31")
    elif case == "absent":
        rows = [r for r in rows if r[1] != "cycles"]
    elif case == "unparseable":
        rows[1] = ("sixty-billion", "cycles", "100.00")
    elif case == "zero":
        rows[3] = ("0", "L1-dcache-load-misses", "100.00")
    elif case == "negative":
        # A hardware counter delta cannot be negative: a corrupt or edited record.
        rows[3] = ("-900000000", "L1-dcache-load-misses", "100.00")
    elif case == "pct-not-finite":
        # `nan` PARSES as a float and then compares FALSE against `< 100.0`, so an
        # unguarded bound would wave it through as if it had been checked.
        rows[1] = ("60000000000", "cycles", "nan")
    elif case == "value-not-finite":
        rows[1] = ("inf", "cycles", "100.00")
    elif case == "window-drift":
        # task-clock reports three quarters of the driver's window: perf counted
        # a DIFFERENT interval from the one the rows were attributed to.
        rows[4] = (str(int((T1 - T0) * ncpus * 0.75)), "task-clock", "100.00")
    with open(path, "w") as fh:
        fh.write("# started on Thu Jan  1 00:00:00 2026\n\n")
        for val, ev, pct in rows:
            fh.write(f"{val},,{ev},60000000000,{pct},,\n")


def flight_step(path, case):
    """One `flight-loadgen` step record (phase 2's occupancy evidence)."""
    rec = {"round": "t", "concurrency": 8, "rows_total": 12_345_678,
           "rows_per_s": 205_761.3, "requests_ok": 4096,
           "requests_error": 0, "requests_unavailable": 0}
    lines = [rec]
    if case == "zero-rows":
        rec["rows_total"] = 0
    elif case == "errors":
        rec["requests_error"] = 7
    elif case == "unavailable":
        rec["requests_unavailable"] = 3
    elif case == "no-ok":
        rec["requests_ok"] = 0
    elif case == "two-steps":
        lines = [rec, dict(rec, concurrency=16)]
    elif case == "no-rows-per-s":
        del rec["rows_per_s"]
    elif case == "no-requests-ok":
        del rec["requests_ok"]
    elif case == "no-requests-error":
        del rec["requests_error"]
    elif case == "no-requests-unavailable":
        del rec["requests_unavailable"]
    elif case == "zero-rate":
        rec["rows_per_s"] = 0
    elif case == "bad-rate":
        rec["rows_per_s"] = "fast"
    elif case == "real-shape":
        # EXACTLY the `flight-loadgen.step/v1` key set of the committed
        # ../phase2-run/doget-s1-r1.jsonl, so the positive control is the shape
        # the campaign actually recorded rather than a shape invented here.
        rec.clear()
        rec.update({
            "schema": "flight-loadgen.step/v1", "round": "r1",
            "endpoint": "http://127.0.0.1:18903", "ts_unix_ms": 1787871479569,
            "seed": 42, "step": 0, "target_concurrency": 1, "shape": "full",
            "duration_s": 36.462841232, "requests_ok": 2,
            "requests_unavailable": 0, "requests_error": 0, "error_codes": {},
            "qps": 0.05485036087217439, "rows_per_s": 219401.44348869755,
            "bytes_per_s": 2778018729.4648724, "rows_total": 8000000,
            "bytes_total": 101294455872,
            "latency_ms": {"p50": 18219.007, "p95": 18268.159,
                           "p99": 18268.159, "max": 18268.159, "samples": 2},
        })
    elif case == "no-step":
        lines = [{"round": "t", "note": "a record with no rows_total"}]
    elif case == "empty":
        lines = []
    with open(path, "w") as fh:
        for line in lines:
            fh.write(json.dumps(line) + "\n")


def siblings_map(path, cores=8, offset=8):
    with open(path, "w") as fh:
        for c in range(cores):
            fh.write(f"{c} {c},{c + offset}\n")
        for c in range(cores):
            fh.write(f"{c + offset} {c},{c + offset}\n")


# --- tampering with a COMMITTED attribution (the read path's negative controls) ---
#
# `derive.py` is the tool a reader runs against the committed tree to reproduce
# the published table, and for a committed rep the evidence it reads is
# `attribution.json` (the raw progress records are far too voluminous to commit).
# So every way that file can be wrong needs a case that OBSERVES the refusal.
#
# The ops that move a timestamp RECOMPUTE the derived fields consistently, so the
# only check that can catch them is the one under test — otherwise a case would
# "pass" via an incidental inconsistency and prove nothing about the property it
# names.


def _load_rep(repdir):
    with open(os.path.join(repdir, "window.json")) as fh:
        win = json.load(fh)
    with open(os.path.join(repdir, "attribution.json")) as fh:
        att = json.load(fh)
    return win, att


def _resync_record(rec, t0, t1):
    """Make one record internally consistent again after its bounds were moved."""
    rec["attributed_span_ns"] = rec["t_b_ns"] - rec["t_a_ns"]
    rec["rows_per_s"] = rec["rows_in_window"] / (rec["attributed_span_ns"] / 1e9)
    rec["attribution_shortfall_frac"] = (
        (rec["t_a_ns"] - t0) + (t1 - rec["t_b_ns"])) / (t1 - t0)


def _resync_summary(att, t0, t1):
    total = sum(r["rows_in_window"] for r in att["per_worker"])
    att["rows_in_window_total"] = total
    att["aggregate_rows_per_s"] = total / ((t1 - t0) / 1e9)
    att["attribution_shortfall_max_frac"] = max(
        r["attribution_shortfall_frac"] for r in att["per_worker"])


def _tamper_perf_csv(repdir, win, op):
    """Edit ONE row of a committed perf.csv — the read path's counter controls.

    A committed `perf.csv` is evidence the reproduction path re-reads, so every
    way a counter can be wrong needs a case that OBSERVES the read-time refusal.
    Before `derive.py` shared the write path's validator, all three of these were
    ACCEPTED on read and published.
    """
    out = []
    for line in open(os.path.join(repdir, win["perf_csv"])):
        f = line.split(",")
        if len(f) > 4 and f[2] == "cycles":
            if op == "counter-zeroed":
                f[0] = "0"                      # a dead instrument, not a measured zero
            elif op == "counter-negative":
                f[0] = "-60000000000"
            elif op == "pct-not-finite":
                f[4] = "nan"                    # parses, then compares FALSE against every bound
            line = ",".join(f)
        out.append(line)
    with open(os.path.join(repdir, win["perf_csv"]), "w") as fh:
        fh.writelines(out)


def tamper(repdir, op):  # noqa: C901 — a flat table of one-line mutations
    win, att = _load_rep(repdir)
    t0, t1 = int(win["t0_ns"]), int(win["t1_ns"])
    per = att["per_worker"]

    if op == "rows-bumped":
        # THE LEAD CASE: a row count raised, nothing else touched. It inflates the
        # published aggregate directly.
        per[0]["rows_in_window"] += 1000
    elif op == "rows-bumped-resynced":
        # The same edit with the record's OWN derived fields and the summary
        # brought back into agreement: internally consistent, and refused only
        # because the rate no longer follows from the rows and the span.
        per[0]["rows_in_window"] += 1000
        per[0]["rows_per_s"] = per[0]["rows_in_window"] / (
            per[0]["attributed_span_ns"] / 1e9)
        _resync_summary(att, t0, t1)
    elif op == "duplicate-worker":
        per[-1]["worker"] = per[0]["worker"]
    elif op == "unknown-worker":
        per[-1]["worker"] = 99
    elif op == "worker-dropped":
        del per[-1]
        _resync_summary(att, t0, t1)
    elif op == "timestamp-outside-window":
        per[0]["t_b_ns"] = t1 + 1 * NS
        _resync_record(per[0], t0, t1)
        _resync_summary(att, t0, t1)
    elif op == "shortfall-over-bound":
        per[0]["t_a_ns"] = per[0]["t_a_ns"] + 1 * NS   # 1 s of a 60 s window = 1.67%
        _resync_record(per[0], t0, t1)
        _resync_summary(att, t0, t1)
    elif op == "shortfall-misstated":
        per[0]["attribution_shortfall_frac"] += 0.001  # still under the bound
        att["attribution_shortfall_max_frac"] = max(
            r["attribution_shortfall_frac"] for r in per)
    elif op == "span-misstated":
        per[0]["attributed_span_ns"] += 1_000_000
    elif op == "total-misstated":
        att["rows_in_window_total"] += 1000
    elif op == "aggregate-misstated":
        att["aggregate_rows_per_s"] *= 1.05
    elif op == "record-field-dropped":
        del per[0]["rows_per_s"]
    elif op == "summary-field-dropped":
        del att["attribution_shortfall_max_frac"]
    elif op == "n-misstated":
        att["n"] += 1
    elif op == "window-misstated":
        att["window_ns"] += 1000
    elif op in ("counter-zeroed", "counter-negative", "pct-not-finite"):
        _tamper_perf_csv(repdir, win, op)
        return
    elif op == "task-clock-drift":
        # perf's enabled interval no longer matches the driver's [T0, T1]: the
        # read path must re-decide this, not merely print it.
        ncpus = len([c for c in win["perf_cpus"].split(",") if c])
        out = []
        for line in open(os.path.join(repdir, win["perf_csv"])):
            f = line.split(",")
            if len(f) > 2 and f[2] == "task-clock":
                f[0] = str(int((t1 - t0) * ncpus * 0.75))
                line = ",".join(f)
            out.append(line)
        with open(os.path.join(repdir, win["perf_csv"]), "w") as fh:
            fh.writelines(out)
        return
    else:
        raise SystemExit(f"unknown tamper op {op!r}")

    with open(os.path.join(repdir, "attribution.json"), "w") as fh:
        json.dump(att, fh)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="what", required=True)
    w = sub.add_parser("window")
    w.add_argument("--dir", required=True)
    w.add_argument("--case", default="good")
    w.add_argument("--workers", type=int, default=2)
    p = sub.add_parser("perf-csv")
    p.add_argument("--path", required=True)
    p.add_argument("--case", default="good")
    fl = sub.add_parser("flight-step")
    fl.add_argument("--path", required=True)
    fl.add_argument("--case", default="good")
    s = sub.add_parser("siblings")
    s.add_argument("--path", required=True)
    s.add_argument("--cores", type=int, default=8)
    s.add_argument("--offset", type=int, default=8)
    tp = sub.add_parser("tamper")
    tp.add_argument("--repdir", required=True)
    tp.add_argument("--op", required=True)
    a = ap.parse_args()
    if a.what == "window":
        build(a.case, a.dir, a.workers)
    elif a.what == "tamper":
        tamper(a.repdir, a.op)
    elif a.what == "perf-csv":
        perf_csv(a.path, a.case)
    elif a.what == "flight-step":
        flight_step(a.path, a.case)
    else:
        siblings_map(a.path, a.cores, a.offset)


if __name__ == "__main__":
    main()
