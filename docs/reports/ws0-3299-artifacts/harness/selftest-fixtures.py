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

    if case != "no-window":
        with open(os.path.join(d, "window.json"), "w") as fh:
            json.dump(
                {
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
                },
                fh,
            )


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
    elif case == "window-drift":
        # task-clock reports three quarters of the driver's window: perf counted
        # a DIFFERENT interval from the one the rows were attributed to.
        rows[4] = (str(int((T1 - T0) * ncpus * 0.75)), "task-clock", "100.00")
    with open(path, "w") as fh:
        fh.write("# started on Thu Jan  1 00:00:00 2026\n\n")
        for val, ev, pct in rows:
            fh.write(f"{val},,{ev},60000000000,{pct},,\n")


def siblings_map(path, cores=8, offset=8):
    with open(path, "w") as fh:
        for c in range(cores):
            fh.write(f"{c} {c},{c + offset}\n")
        for c in range(cores):
            fh.write(f"{c + offset} {c},{c + offset}\n")


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
    s = sub.add_parser("siblings")
    s.add_argument("--path", required=True)
    s.add_argument("--cores", type=int, default=8)
    s.add_argument("--offset", type=int, default=8)
    a = ap.parse_args()
    if a.what == "window":
        build(a.case, a.dir, a.workers)
    elif a.what == "perf-csv":
        perf_csv(a.path, a.case)
    else:
        siblings_map(a.path, a.cores, a.offset)


if __name__ == "__main__":
    main()
