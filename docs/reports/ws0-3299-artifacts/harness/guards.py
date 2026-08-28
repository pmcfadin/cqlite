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
import hashlib
import json
import os
import re
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
                f"more than the bound. Lower --progress-ms or lengthen the window.",
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

    # EVERY FIELD BELOW IS REQUIRED. None of these checks may be skipped by the
    # absence of its own input.
    #
    # They used to be conditional (`if expected:`, `if csv_name:`, `if
    # "task-clock" in counters:`), so a window.json missing `worker_cpus` skipped
    # affinity verification and one missing `perf_csv`/`task-clock` skipped the
    # counter-window verification — and the rep still returned SUCCESS. That is
    # this issue's own recurring failure shape, a CHECK THAT REPORTS SUCCESS
    # HAVING MEASURED NOTHING, sitting in the guard layer whose entire purpose is
    # to prevent it. It is the same class as an LLC counter that programs cleanly
    # and returns a hard zero: absence of a bad signal is not a good signal.
    for field in ("worker_cpus", "perf_csv", "perf_cpus"):
        if not w.get(field):
            fail(
                "WINDOW_FIELD_MISSING",
                f"{win} carries no `{field}`. This field is REQUIRED: without it the "
                f"corresponding fail-closed check cannot run, and a guard that silently "
                f"skips itself and returns success is worse than no guard at all.",
            )

    # Pinning, OBSERVED: each worker's own sched_getaffinity readback must equal
    # the CPU set it was supposed to own.
    expected = w["worker_cpus"]
    if len(expected) != n:
        fail(
            "WINDOW_FIELD_MALFORMED",
            f"{win} lists {len(expected)} worker_cpus entries but n={n}; the affinity check "
            f"would verify a different set of workers than the rep measured.",
        )
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
    # THE COUNTED CPUs MUST BE THE WORKED CPUs — not merely the right NUMBER of
    # them. Everything below this point attributes CPU-wide counters to the rows
    # the workers produced, and that attribution is only sound if the set perf
    # counted IS the set the workers ran on. Counting the CARDINALITY (which is
    # all the counter-window check needs) cannot see a substitution: swap one
    # pinned CPU for an idle one and `ncpus` is unchanged, `task-clock` still
    # reads W x N because it accrues on idle CPUs too, and the rep passes while a
    # worker's cycles were never counted and an unrelated CPU's were.
    #
    # sweep.sh builds `--worker-cpus` and `--perf-cpus` from ONE `$CPUS`
    # variable, so the two agree by construction today. That is a property of the
    # DRIVER, and a guard that relies on its driver being correct is checking
    # nothing: this makes the driver's guarantee an OBSERVED one, so a future
    # invocation that passes the two separately (or a hand-run rep.py, which
    # takes them as independent flags) cannot publish mis-attributed counters.
    perf_cpu_list = [c for c in w["perf_cpus"].split(",") if c != ""]
    try:
        perf_set = {int(c) for c in perf_cpu_list}
    except ValueError:
        fail("WINDOW_FIELD_MALFORMED",
             f"{win} has a non-integer entry in `perf_cpus` ({w['perf_cpus']!r}); the counted CPU "
             f"set cannot be compared with the worked one.")
    if not perf_set:
        fail("WINDOW_FIELD_MALFORMED",
             f"{win} has a `perf_cpus` naming no CPU ({w['perf_cpus']!r}); there is no counted "
             f"CPU set to compare with the worked one, and the counter-window check divides by "
             f"the counted CPU count.")
    if len(perf_set) != len(perf_cpu_list):
        fail("WINDOW_FIELD_MALFORMED",
             f"{win} lists a CPU more than once in `perf_cpus` ({w['perf_cpus']!r}); perf counts "
             f"each CPU once, so a duplicated entry inflates the counted-CPU count and the "
             f"counter-window comparison divides by the wrong number.")
    worked_set = {int(c) for grp in expected for c in grp}
    if worked_set != perf_set:
        unc = sorted(worked_set - perf_set)
        idle = sorted(perf_set - worked_set)
        fail(
            "WINDOW_CPU_SET_MISMATCH",
            f"the workers ran on CPUs {sorted(worked_set)} but perf counted {sorted(perf_set)}. "
            f"Uncounted worked CPU(s): {unc or 'none'}; counted CPU(s) no worker used: "
            f"{idle or 'none'}. Every counter below is attributed to these workers' rows, so the "
            f"counted set must BE the worked set — the same cardinality is not the same set.",
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
    csv_name = w["perf_csv"]
    csv_path = os.path.join(args.repdir, csv_name)
    ncpus = len(perf_set)
    counters = parse_perf_csv(csv_path)
    if "task-clock" not in counters:
        fail(
            "WINDOW_NO_TASK_CLOCK",
            f"{csv_path} carries no `task-clock`. It is the ONLY evidence that perf's enabled "
            f"interval equals the driver's [T0, T1] — the central claim of the aligned window "
            f"— so its absence means that claim is unverified, not that it holds.",
        )
    if True:
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
                "counter_window_drift_frac": drift,
                "per_worker": per,
            }
        )
    )
    return 0


# --- phase 2: the Flight `do_get` arm -----------------------------------------


def guard_flight_step(args):
    """Validate ONE `flight-loadgen` step record for the `do_get` arm.

    Phase 2 reuses THIS module rather than forking a second set of checks, so a
    guard fixed here is fixed for both arms. What differs is only the occupancy
    question: the bare-scan arm asks it of per-worker progress records, and this
    asks it of the loadgen's own step record.

    A ZERO-ROW `do_get` IS THE FAILURE THIS EXISTS TO CATCH. It would otherwise
    look like an extremely fast one — the server returning `NotFound` for every
    request completes very quickly and reports a large request rate. #3224 hit
    exactly this: `rows_total % corpus == 0` passed on `0 % 3999890 == 0` while
    the server logged `discovered 0 tables across 0 keyspaces` behind 2,258,606
    `NotFound`s, and the rep returned rc=0.
    """
    if not os.path.exists(args.jsonl):
        fail("FLIGHT_RECORD_MISSING", f"{args.jsonl} does not exist — no loadgen record, so no measurement")
    recs = []
    with open(args.jsonl) as fh:
        for line in fh:
            line = line.strip()
            if line:
                recs.append(json.loads(line))
    steps = [r for r in recs if "rows_total" in r]
    if not steps:
        fail("FLIGHT_RECORD_MISSING", f"{args.jsonl} holds no step record carrying rows_total")
    if len(steps) != 1:
        fail(
            "FLIGHT_STEP_COUNT",
            f"{args.jsonl} holds {len(steps)} step records; this arm runs ONE concurrency per "
            f"rep so the counted perf interval matches exactly one step. Multiple steps would "
            f"span the ramp between them, which is not a measured window.",
        )
    st = steps[0]
    rows = int(st.get("rows_total", 0))
    if rows <= 0:
        fail(
            "FLIGHT_ZERO_ROWS",
            f"the step returned {rows} rows. A zero-row do_get is a FAILURE, never a "
            f"measurement — and it presents as a very FAST one, because a server answering "
            f"NotFound completes every request immediately.",
        )
    # EVERY FIELD BELOW IS REQUIRED — the same fail-open shape closed in
    # guard_window's required-field block, in this arm.
    #
    # These four used to be conditional (`if key in st`, `if "requests_ok" in
    # st`, and `rows_per_s` merely echoed through `st.get`), so a record that
    # carried a positive `rows_total` and NOTHING ELSE passed as a valid
    # measurement: no success accounting, no error accounting, and no
    # throughput. That is a check reporting success having measured nothing —
    # and it is worse here than elsewhere, because a partially-failing do_get
    # run is exactly what this guard exists to refuse (#3224: 2,258,606
    # `NotFound`s behind an rc=0 rep). `flight-loadgen`'s
    # `flight-loadgen.step/v1` schema emits all four on every step (see
    # ../phase2-run/doget-*.jsonl), so an absent one means the record is not a
    # step record of the measured run — never that the arm had no errors.
    for key in ("requests_ok", "requests_error", "requests_unavailable", "rows_per_s"):
        if key not in st:
            fail(
                "FLIGHT_FIELD_MISSING",
                f"{args.jsonl}: the step record carries no `{key}`. It is REQUIRED: absence of "
                f"an error/success count is not evidence of no errors, and absence of "
                f"`rows_per_s` leaves the step with no measured throughput at all. A guard that "
                f"skips itself when its input is missing returns success having checked nothing.",
            )
    for key in ("requests_error", "requests_unavailable"):
        try:
            n_bad = int(st[key])
        except (TypeError, ValueError):
            fail("FLIGHT_FIELD_MALFORMED", f"{args.jsonl}: `{key}` is {st[key]!r}, not a count")
        if n_bad != 0:
            fail("FLIGHT_REQUEST_ERRORS", f"step reported {key}={st[key]}; a rep with failed requests is not a measurement")
    try:
        n_ok = int(st["requests_ok"])
    except (TypeError, ValueError):
        fail("FLIGHT_FIELD_MALFORMED", f"{args.jsonl}: `requests_ok` is {st['requests_ok']!r}, not a count")
    if n_ok <= 0:
        fail("FLIGHT_REQUEST_ERRORS", f"step reported requests_ok={n_ok}")
    try:
        rate = float(st["rows_per_s"])
    except (TypeError, ValueError):
        fail("FLIGHT_FIELD_MALFORMED", f"{args.jsonl}: `rows_per_s` is {st['rows_per_s']!r}, not a rate")
    if rate <= 0:
        fail(
            "FLIGHT_ZERO_ROWS",
            f"step reported rows_per_s={rate}. A step that returned {rows} rows at a "
            f"non-positive rate is inconsistent with itself; neither number can be published.",
        )
    print(json.dumps({"rows_total": rows, "rows_per_s": rate, "requests_ok": n_ok, "record": args.jsonl}))
    return 0


# --- corpus identity ---------------------------------------------------------
#
# THE EXACT PATH THE WORKER OPENS, and nothing else. The pre-fix check resolved
# `find "$CORPUS" -name '*-Data.db' -print -quit` — the FIRST arbitrary match
# ANYWHERE under the root — so an unrelated, valid, correctly-sized Data.db
# elsewhere in the tree could satisfy the identity check while the worker read a
# DIFFERENT corpus (possibly compressed, possibly the wrong geometry). Verifying
# one file and measuring another is the same false-assurance shape as a control
# that cannot fail: the check passes and says nothing about the measurement.
#
# `scan-worker` opens `<corpus>/<keyspace>/<table>/` with clap defaults
# `ws0`/`events`, and sweep.sh exposes no flag to change either, so the measured
# file is exactly ONE path. It is spelled here as a CONSTANT, not a parameter: a
# guard whose subject is caller-selectable can be pointed away from the subject.
CORPUS_KEYSPACE = "ws0"
CORPUS_TABLE = "events"

# Where the pinned identity comes from. NOT a copy: the digest and byte count are
# read from the committed Rust constants in `tools/ws0-corpus-gen/src/
# measurement_corpus.rs` — the same single source of truth
# `scripts/perf/ws0_canonical_corpus.py` reads — so this guard cannot drift from
# the rig's pin. Path is resolved relative to THIS file (harness -> artifacts ->
# reports -> docs -> repo root), never from the environment.
CORPUS_PIN_REL = os.path.join("tools", "ws0-corpus-gen", "src", "measurement_corpus.rs")


def _repo_root():
    return os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", ".."))


def read_corpus_pins():
    """`DATA_DB_BYTES` and `DATA_DB_SHA256`, parsed from the committed Rust pin.

    Fails closed on every way the pin can fail to answer: missing file,
    unreadable file, absent constant, malformed constant. An unconsultable
    oracle yields a REFUSAL, never a pass (a positive verdict requires an
    affirmative measurement).
    """
    path = os.path.join(_repo_root(), CORPUS_PIN_REL)
    try:
        with open(path) as fh:
            text = fh.read()
    except OSError as exc:
        fail("CORPUS_PIN_UNREADABLE",
             f"cannot read the pinned corpus identity at {path}: {exc}. The pin is the ONLY "
             f"oracle for corpus identity; without it no corpus can be certified.")
    m_sha = re.search(r'pub\s+const\s+DATA_DB_SHA256\s*:\s*&str\s*=\s*"([0-9a-fA-F]{64})"', text)
    m_bytes = re.search(r"pub\s+const\s+DATA_DB_BYTES\s*:\s*u64\s*=\s*([0-9_]+)\s*;", text)
    if not m_sha or not m_bytes:
        fail("CORPUS_PIN_UNPARSEABLE",
             f"{path}: could not parse DATA_DB_SHA256 (64 hex) and/or DATA_DB_BYTES. The pin "
             f"moved or was reformatted; fix the parse rather than skipping the check.")
    return int(m_bytes.group(1).replace("_", "")), m_sha.group(1).lower()


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def guard_corpus(args):
    """The file the worker will scan IS the pinned #3096 measurement corpus."""
    expect_bytes, expect_sha = read_corpus_pins()
    table_dir = os.path.join(args.corpus, CORPUS_KEYSPACE, CORPUS_TABLE)
    try:
        entries = sorted(os.listdir(table_dir))
    except OSError as exc:
        fail("CORPUS_DATA_DB_ABSENT",
             f"{table_dir} is not a readable directory ({exc}). This is the EXACT path "
             f"scan-worker opens (<corpus>/{CORPUS_KEYSPACE}/{CORPUS_TABLE}); a *-Data.db "
             f"elsewhere under {args.corpus} is a different corpus and does not count.")
    data = [e for e in entries if e.endswith("-Data.db")]
    if not data:
        fail("CORPUS_DATA_DB_ABSENT",
             f"no *-Data.db in {table_dir} — the exact directory scan-worker opens. A "
             f"*-Data.db elsewhere under {args.corpus} does not satisfy this check.")
    if len(data) > 1:
        fail("CORPUS_DATA_DB_AMBIGUOUS",
             f"{table_dir} holds {len(data)} *-Data.db files ({', '.join(data)}). The scan "
             f"would read all of them; a single pinned identity cannot describe that set.")
    data_db = os.path.join(table_dir, data[0])
    comp = [e for e in entries if e.endswith("-CompressionInfo.db")]
    if comp:
        fail("CORPUS_COMPRESSED",
             f"{table_dir} carries {comp[0]}. The #3096 measurement corpus is UNCOMPRESSED "
             f"(693.69 B/row); a compressed corpus is a DIFFERENT corpus and its numbers are "
             f"not comparable (cross-corpus division is forbidden on this issue).")
    actual_bytes = os.path.getsize(data_db)
    if actual_bytes != expect_bytes:
        fail("CORPUS_BYTES_MISMATCH",
             f"{data_db} is {actual_bytes} bytes; the pin says {expect_bytes}. A different "
             f"corpus makes every cross-point comparison invalid.")
    actual_sha = sha256_file(data_db)
    if actual_sha != expect_sha:
        fail("CORPUS_SHA_MISMATCH",
             f"{data_db} digests {actual_sha}; the pin says {expect_sha}. Same size, "
             f"different bytes — exactly what a byte-count-only check misses.")
    print(json.dumps({
        "data_db": data_db,
        "bytes": actual_bytes,
        "sha256": actual_sha,
        "compressed": False,
        "pin_source": os.path.join(_repo_root(), CORPUS_PIN_REL),
    }))
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

    k = sub.add_parser("corpus", help="verify the EXACT Data.db the worker opens is the pinned corpus")
    k.add_argument("--corpus", required=True)
    k.set_defaults(fn=guard_corpus)

    f = sub.add_parser("flight-step", help="verify one flight-loadgen step record (phase 2)")
    f.add_argument("--jsonl", required=True)
    f.set_defaults(fn=guard_flight_step)

    args = ap.parse_args()
    sys.exit(args.fn(args))


if __name__ == "__main__":
    main()
