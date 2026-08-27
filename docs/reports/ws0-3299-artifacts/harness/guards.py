#!/usr/bin/env python3
"""#3299 S-sweep guards — ONE implementation, driven by the rig AND by the selftest.

Every validator the sweep depends on lives here and is reachable from the command
line, so `selftest.sh` can feed each one the input it must reject and observe it
fire. That is the #3249/#3272 bar: not "the guard exists" but "the guard has been
OBSERVED to fire". A guard whose failure path no test can reach is the defect,
not the protection.

All guards FAIL CLOSED. Exit codes:

    0   pass
    2   usage error
    3   GUARD-FAIL — a measurement was refused

Every refusal prints exactly one `GUARD-FAIL <CODE>: <why>` line to stderr, and
the selftest asserts the CODE (not the prose), so the diagnostics stay editable.

DELIBERATELY ABSENT: any environment variable that relaxes any bound here. An
escape hatch on a measurement guard can only ever buy a confident wrong number.
"""

import argparse
import json
import os
import sys

# --- the counter contract ---------------------------------------------------
#
# Exactly the events the #3299 protocol measures, all four of which the Step 1
# census on THIS box proved REAL at 100.00% enabled (see ../host/README.md).
# `task-clock` is a software event (it consumes no PMC). It is NOT a utilisation
# measure here: under CPU-wide (`-C`) counting it is elapsed-time x ncpus BY
# CONSTRUCTION, so any "utilisation" derived from it reads 1.0 whatever the
# machine is doing. It is carried for the one thing it CAN prove — see
# `WINDOW_COUNTER_MISMATCH`.
REQUIRED_EVENTS = (
    "instructions",
    "cycles",
    "L1-dcache-loads",
    "L1-dcache-load-misses",
    "task-clock",
)

# Every LLC spelling the Step 1 census proved UNAVAILABLE on this host: two are
# `<not supported>`, the rest programmed cleanly at 100.00% enabled and returned a
# hard 0 on a 2 GiB serial-dependency pointer chase — i.e. an unavailable
# instrument, not a measured zero (#3217's silent-instrument failure).
#
# They are refused as INPUT, not merely omitted from the default set, so a later
# edit to the event list cannot quietly reintroduce one and publish its 0 as a
# measurement of L3 behaviour. AC3 is deferred on this box; it is not approximated.
CENSUS_UNAVAILABLE_EVENTS = frozenset(
    {
        "LLC-loads",
        "LLC-load-misses",
        "LLC-stores",
        "LLC-store-misses",
        "cache-references",
        "cache-misses",
        "mem_load_retired.l3_miss",
        "mem_load_retired.l3_hit",
        "longest_lat_cache.miss",
        "longest_lat_cache.reference",
        "r4f2e",
        "r412e",
    }
)

# The window may lose at most this fraction of its span to sample-boundary
# attribution at the two ends (see README, "the aligned window").
DEFAULT_SHORTFALL_BOUND = 0.005

# perf's enabled interval may differ from the driver's [T0, T1] by at most this
# fraction (see `WINDOW_COUNTER_MISMATCH`). The observed disagreement on this box
# is ~1e-5; the tolerance covers the ACK round trip, not a real drift.
DEFAULT_COUNTER_WINDOW_TOLERANCE = 0.01


def fail(code, msg):
    print(f"GUARD-FAIL {code}: {msg}", file=sys.stderr)
    sys.exit(3)


# --- topology ---------------------------------------------------------------


def read_sibling_map(path):
    """CPU -> its complete sibling group, parsed from a sysfs dump.

    Format (one line per logical CPU): `<cpu> <sibling-list>`, e.g. `0 0,8`.
    The rig generates this from `thread_siblings_list`; the selftest writes
    synthetic ones. Reading a FILE rather than sysfs directly is what makes the
    topology guard testable on any box.
    """
    groups = {}
    with open(path) as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) != 2:
                fail("CPUSET_MAP_MALFORMED", f"{path}:{lineno}: expected `<cpu> <sibling-list>`, got {line!r}")
            try:
                cpu = int(parts[0])
                sibs = tuple(sorted(int(x) for x in parts[1].split(",")))
            except ValueError:
                fail("CPUSET_MAP_MALFORMED", f"{path}:{lineno}: non-integer CPU id in {line!r}")
            groups[cpu] = sibs
    if not groups:
        fail("CPUSET_MAP_MALFORMED", f"{path}: no CPUs parsed")
    return groups


def guard_cpuset(args):
    """The pinned set must be an exact union of S COMPLETE sibling groups.

    #3224 measured why this is the load-bearing guard: #3217's `S=1 -> "2,10"` is
    one thread of each of TWO different physical cores on a `(c, c+8)` box — it
    would have measured S=2 on half-populated cores while labelling the point S=1.
    A NUMA check passes that set; only a sibling-group check rejects it.
    """
    groups = read_sibling_map(args.siblings)
    try:
        cpus = sorted({int(x) for x in args.cpus.split(",") if x != ""})
    except ValueError:
        fail("CPUSET_MALFORMED", f"--cpus {args.cpus!r} is not a comma-separated CPU list")
    if not cpus:
        fail("CPUSET_MALFORMED", "--cpus is empty")

    unknown = [c for c in cpus if c not in groups]
    if unknown:
        fail("CPUSET_UNKNOWN_CPU", f"CPU(s) {unknown} are not present in {args.siblings}")

    # Every group touched must be present in FULL.
    touched = {}
    for c in cpus:
        touched.setdefault(groups[c], []).append(c)
    for grp, members in sorted(touched.items()):
        if sorted(members) != sorted(grp):
            fail(
                "CPUSET_NOT_SIBLING_GROUP",
                f"physical core {grp} is only HALF pinned: {sorted(members)} of {list(grp)}. "
                f"A half-populated core measures a different machine than a whole one — "
                f"per-core figures taken this way are silently ~halved.",
            )

    if len(touched) != args.s:
        fail(
            "CPUSET_COUNT_MISMATCH",
            f"--cpus {args.cpus} covers {len(touched)} physical core(s) but S={args.s} was requested",
        )

    total_cores = len({g for g in groups.values()})
    if args.s > total_cores - args.headroom_cores:
        fail(
            "CPUSET_HEADROOM",
            f"S={args.s} leaves fewer than {args.headroom_cores} physical cores unpinned "
            f"({total_cores} exist). The driver, perf and the OS need headroom off the "
            f"measured cores, or their cost lands in the counters.",
        )
    print(f"CPUSET-OK s={args.s} cores={sorted(touched)} total_physical={total_cores}")
    return 0


# --- perf CSV ---------------------------------------------------------------


def parse_perf_csv(path):
    """`perf stat -x,` rows -> {event: (value_or_marker, pct_running)}.

    Layout: 1=value 2=unit 3=event 4=run_time_ns 5=pct_running. With `--control`
    the rows are emitted once, at disable/exit.
    """
    if not os.path.exists(path):
        fail("PERF_CSV_MISSING", f"{path} does not exist — an absent counter file is FATAL, never a zero")
    out = {}
    with open(path) as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line.strip() or line.startswith("#"):
                continue
            f = line.split(",")
            if len(f) < 5:
                continue
            out[f[2].strip()] = (f[0].strip(), f[4].strip())
    if not out:
        fail("PERF_CSV_MISSING", f"{path} parsed to zero counter rows — refusing to treat that as a measurement")
    return out


def guard_perf_csv(args):
    events = [e for e in (args.events or ",".join(REQUIRED_EVENTS)).split(",") if e]

    forbidden = sorted(set(events) & CENSUS_UNAVAILABLE_EVENTS)
    if forbidden:
        fail(
            "PERF_FORBIDDEN_EVENT",
            f"{forbidden} were proven UNAVAILABLE on this host by the Step 1 census "
            f"(<not supported>, or a hard 0 at 100.00% enabled on a workload that cannot "
            f"have zero). Their 0 is an absent instrument, not a measurement. AC3 is "
            f"DEFERRED on this box, never approximated from a dead counter.",
        )

    rows = parse_perf_csv(args.csv)
    for ev in events:
        if ev not in rows:
            fail("PERF_EVENT_ABSENT", f"event {ev!r} is not in {args.csv} (present: {sorted(rows)})")
        val, pct = rows[ev]
        if "<not counted>" in val or "<not supported>" in val:
            fail("PERF_EVENT_NOT_COUNTED", f"event {ev!r} reads {val!r} — an unavailable instrument, never a 0")
        try:
            ival = int(float(val))
        except ValueError:
            fail("PERF_EVENT_UNPARSEABLE", f"event {ev!r} value {val!r} is not a number")
        try:
            fpct = float(pct)
        except ValueError:
            fail("PERF_EVENT_UNPARSEABLE", f"event {ev!r} pct_running {pct!r} is not a number")
        if fpct < 100.0:
            fail(
                "PERF_MULTIPLEXED",
                f"event {ev!r} ran only {fpct}% of the window — that is a SCALED ESTIMATE, "
                f"not a count. This issue's kill criterion requires 100.00% on every event; "
                f"the rep is discarded.",
            )
        if ival == 0:
            fail(
                "PERF_EVENT_ZERO",
                f"event {ev!r} counted 0 over a window in which rows were demonstrably "
                f"produced. On this host that signature means an unavailable instrument "
                f"(see ../host/README.md), so it is refused rather than published.",
            )
    print(f"PERF-OK events={len(events)} all at 100.00% enabled")
    return 0


# --- the aligned window ------------------------------------------------------


def load_progress(path):
    recs = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            recs.append((int(d["t_ns"]), int(d["rows"])))
    recs.sort()
    return recs


def attribute_window(repdir, t0, t1, n, shortfall_bound):
    """Rows produced INSIDE [t0, t1], per worker, by DIFFERENCING observed records.

    `n` is the number of WORKERS (streams), not the core count.

    Returns (per_worker, diagnostics). Never interpolates and never assumes a
    rate: each worker contributes `rows(b) - rows(a)` for two records it actually
    emitted, with a <= b both inside the window. The residual (a - t0) + (t1 - b)
    is the ATTRIBUTION SHORTFALL, bounded by the guard below, and it biases the
    reported rate DOWNWARD — the conservative direction.
    """
    span = t1 - t0
    if span <= 0:
        fail("WINDOW_SPAN", f"window span is {span} ns — t1 must be strictly after t0")
    per = []
    for i in range(n):
        prog = os.path.join(repdir, f"worker-{i}.progress.jsonl")
        summ = os.path.join(repdir, f"worker-{i}.summary.json")
        if not os.path.exists(prog):
            fail("WINDOW_WORKER_MISSING", f"{prog} is absent — a rep is only valid if all {n} workers reported")
        if not os.path.exists(summ):
            fail("WINDOW_WORKER_MISSING", f"{summ} is absent — worker {i} did not exit cleanly")
        recs = load_progress(prog)
        if len(recs) < 2:
            fail("WINDOW_WORKER_MISSING", f"worker {i} emitted {len(recs)} progress record(s); >= 2 are required")

        # SPANNING (outer bracket): the worker must have been producing rows both
        # BEFORE the window opened and AFTER it closed. This is what makes the
        # window genuinely S-concurrent — mechanically observed, never assumed
        # from having launched S processes.
        before = [r for r in recs if r[0] <= t0]
        after = [r for r in recs if r[0] >= t1]
        if not before:
            fail(
                "WINDOW_NOT_SPANNED",
                f"worker {i}'s first progress record is at t={recs[0][0]}, AFTER the window "
                f"opened at {t0}: it was not yet producing rows for the whole window, so "
                f"fewer than S scans were concurrent.",
            )
        if not after:
            fail(
                "WINDOW_NOT_SPANNED",
                f"worker {i}'s last progress record is at t={recs[-1][0]}, BEFORE the window "
                f"closed at {t1}: it stopped (finished, stalled or died) inside the window, so "
                f"fewer than S scans were concurrent for part of it.",
            )

        # ATTRIBUTION (inner bracket).
        a = min((r for r in recs if r[0] >= t0), key=lambda r: r[0])
        b = max((r for r in recs if r[0] <= t1), key=lambda r: r[0])
        if a[0] > b[0]:
            fail(
                "WINDOW_SHORTFALL",
                f"worker {i} emitted NO progress record inside [{t0}, {t1}] — its sample "
                f"interval is longer than the window, so no rows can be attributed to it "
                f"without assuming a rate.",
            )
        rows = b[1] - a[1]
        if rows <= 0:
            fail(
                "WINDOW_ZERO_ROWS",
                f"worker {i} produced {rows} rows inside the window — a scan that observed "
                f"nothing is a failure, not a measurement.",
            )
        shortfall = ((a[0] - t0) + (t1 - b[0])) / span
        if shortfall > shortfall_bound:
            fail(
                "WINDOW_SHORTFALL",
                f"worker {i} leaves {shortfall:.4%} of the window unattributed (bound "
                f"{shortfall_bound:.4%}). Rows are only counted between records the worker "
                f"actually emitted, so an interval this coarse would understate the rate by "
                f"more than the bound. Lower --progress-rows or lengthen the window.",
            )
        per.append(
            {
                "worker": i,
                "rows_in_window": rows,
                "t_a_ns": a[0],
                "t_b_ns": b[0],
                "attributed_span_ns": b[0] - a[0],
                "attribution_shortfall_frac": shortfall,
                "rows_per_s": rows / ((b[0] - a[0]) / 1e9),
            }
        )
    return per


def counter_window_drift(repdir, win, counters):
    """|task-clock - window x ncpus| / (window x ncpus). See WINDOW_COUNTER_MISMATCH."""
    ncpus = len([c for c in win["perf_cpus"].split(",") if c != ""])
    expected = float(int(win["t1_ns"]) - int(win["t0_ns"])) * ncpus
    return abs(counters["task-clock"] - expected) / expected


def guard_window(args):
    win = os.path.join(args.repdir, "window.json")
    if not os.path.exists(win):
        fail("WINDOW_MISSING", f"{win} is absent — no measured window, so no measurement")
    with open(win) as fh:
        w = json.load(fh)
    t0, t1 = int(w["t0_ns"]), int(w["t1_ns"])
    # WORKER COUNT IS N, NOT S. They are different dimensions (S = pinned cores,
    # N = concurrent streams) and conflating them would silently validate only
    # the first S of N workers — i.e. a rep could pass with streams nobody
    # checked. Read fail-closed: an old-schema window.json without `n` is
    # refused rather than defaulted.
    if "n" not in w:
        fail(
            "WINDOW_MISSING",
            f"{win} carries no `n` (stream count). This harness is two-dimensional "
            f"(S = pinned cores, N = concurrent streams); a window without N cannot say "
            f"how many workers should have reported.",
        )
    s, n = int(w["s"]), int(w["n"])
    per = attribute_window(args.repdir, t0, t1, n, args.shortfall_bound)

    # Pinning, OBSERVED: each worker's own sched_getaffinity readback must equal
    # the sibling pair it was supposed to own.
    expected = w.get("worker_cpus")
    if expected:
        for i in range(n):
            with open(os.path.join(args.repdir, f"worker-{i}.summary.json")) as fh:
                got = sorted(int(x) for x in json.load(fh)["observed_affinity"])
            want = sorted(int(x) for x in expected[i])
            if got != want:
                fail(
                    "WINDOW_AFFINITY_MISMATCH",
                    f"worker {i} ran on CPUs {got} but was pinned to {want}. The kernel, not "
                    f"the taskset argument, is the authority on where it ran.",
                )
    # THE COUNTER WINDOW AND THE ROW WINDOW MUST BE THE SAME INTERVAL.
    #
    # That identity is the central claim of this harness, and until now it rested
    # on the control-FIFO handshake being correct. It is now MEASURED. Under
    # CPU-wide counting `task-clock` accumulates elapsed time on every pinned CPU
    # whether or not anything runs, so over a window of W ns on N CPUs it must
    # read W*N. If perf's enabled interval had drifted from the driver's
    # [T0, T1] — a missed ACK, a late enable, a disable that did not take — this
    # is where it shows up, and the rep is refused.
    csv_name = w.get("perf_csv")
    if csv_name:
        csv_path = os.path.join(args.repdir, csv_name)
        ncpus = len([c for c in w["perf_cpus"].split(",") if c != ""])
        counters = parse_perf_csv(csv_path)
        if "task-clock" in counters:
            val, _pct = counters["task-clock"]
            try:
                task_clock_ns = float(val)
            except ValueError:
                fail("PERF_EVENT_UNPARSEABLE", f"task-clock value {val!r} is not a number")
            expected = float(t1 - t0) * ncpus
            drift = abs(task_clock_ns - expected) / expected
            if drift > args.counter_window_tolerance:
                fail(
                    "WINDOW_COUNTER_MISMATCH",
                    f"perf counted {task_clock_ns:.0f} ns of task-clock over {ncpus} CPUs, but "
                    f"the driver's window [{t0}, {t1}] is {expected:.0f} ns x CPU — a "
                    f"{drift:.4%} disagreement (tolerance "
                    f"{args.counter_window_tolerance:.4%}). The counters and the rows were "
                    f"therefore NOT taken over the same interval, which is the one property "
                    f"the aligned window exists to guarantee.",
                )

    total = sum(p["rows_in_window"] for p in per)
    print(
        json.dumps(
            {
                "s": s,
                "n": n,
                "window_ns": t1 - t0,
                "rows_in_window_total": total,
                "aggregate_rows_per_s": total / ((t1 - t0) / 1e9),
                "attribution_shortfall_max_frac": max(p["attribution_shortfall_frac"] for p in per),
                "counter_window_drift_frac": drift if csv_name and "task-clock" in counters else None,
                "per_worker": per,
            }
        )
    )
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("cpuset", help="verify a pinned CPU set is S complete sibling groups")
    c.add_argument("--s", type=int, required=True)
    c.add_argument("--cpus", required=True)
    c.add_argument("--siblings", required=True)
    c.add_argument("--headroom-cores", type=int, default=2)
    c.set_defaults(fn=guard_cpuset)

    p = sub.add_parser("perf-csv", help="verify every event counted at 100.00%%")
    p.add_argument("--csv", required=True)
    p.add_argument("--events", default=None)
    p.set_defaults(fn=guard_perf_csv)

    w = sub.add_parser("window", help="verify + attribute an aligned concurrency window")
    w.add_argument("--repdir", required=True)
    w.add_argument("--shortfall-bound", type=float, default=DEFAULT_SHORTFALL_BOUND)
    w.add_argument("--counter-window-tolerance", type=float, default=DEFAULT_COUNTER_WINDOW_TOLERANCE)
    w.set_defaults(fn=guard_window)

    args = ap.parse_args()
    sys.exit(args.fn(args))


if __name__ == "__main__":
    main()
