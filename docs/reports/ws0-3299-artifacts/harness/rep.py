#!/usr/bin/env python3
"""#3299 — ONE rep at ONE value of S: S pinned bare scans, one aligned window.

Runs INSIDE the containment scope (`test-data/scripts/perf-run-contained.sh`),
launched by `sweep.sh`. It owns the part of the protocol that must be exact:

  1. launch S workers, each `taskset`-pinned to ONE complete physical core;
  2. wait for every worker to finish prewarming and signal ready (WARM protocol —
     no first-touch page-cache population may fall inside the window);
  3. wait until every worker has emitted >= 2 post-barrier progress records, i.e.
     all S are OBSERVED to be producing rows concurrently;
  4. open the measurement window with perf's control FIFO and close it the same
     way, recording T0/T1 on the same CLOCK_MONOTONIC the workers timestamp with;
  5. stop the workers and write `window.json`.

THE ALIGNED WINDOW — the methodological core (see README for the full statement).
The window is NOT each scan's start-to-finish. It is an interval strictly inside
the steady state, and BOTH the counters and the rows are taken over that ONE
interval: counters because perf's enable/disable brackets it exactly, rows
because each worker emits `(monotonic_ns, cumulative_rows)` records that are
DIFFERENCED across it. Nothing is interpolated and no rate is assumed.

Why the control FIFO rather than `perf stat -- sleep D`: with a plain child, the
counted interval includes perf's own startup and the child's exec, and the
driver cannot know when counting actually began, so the row window and the
counter window would be two different intervals that merely look alike. With
`-D -1 --control fifo:ctl,ack` the driver enables counting, waits for perf's ACK,
and only then reads T0 — the two windows share their boundaries by construction.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time

CLOCK = time.CLOCK_MONOTONIC


def now_ns():
    return time.clock_gettime_ns(CLOCK)


def die(msg, code=4):
    print(f"rep.py: FATAL: {msg}", file=sys.stderr)
    sys.exit(code)


def verify_clock_source(worker_bin):
    """Affirmatively check that python's CLOCK_MONOTONIC is the workers' clock.

    The whole attribution rests on timestamps taken in two different runtimes
    being comparable. That is TRUE (both reach the same vDSO `clock_gettime`),
    but 'true by argument' is what this rig does not accept: the worker exposes
    `--print-monotonic-ns`, so the claim is measured once, outside the window.
    """
    a = now_ns()
    out = subprocess.run([worker_bin, "--print-monotonic-ns"], capture_output=True, text=True, check=True)
    w = int(out.stdout.strip())
    b = now_ns()
    if not (a <= w <= b):
        die(
            f"clock-source check FAILED: worker read {w}, which is outside the driver's "
            f"bracketing reads [{a}, {b}]. The two processes are not on the same clock, so "
            f"no cross-process window attribution is valid."
        )
    return {"driver_before_ns": a, "worker_ns": w, "driver_after_ns": b, "bracket_ns": b - a}


def launch_workers(args, rundir, worker_cpus):
    procs = []
    for i, cpus in enumerate(worker_cpus):
        cpu_arg = ",".join(str(c) for c in cpus)
        cmd = [
            "taskset", "-c", cpu_arg,
            args.worker_bin,
            "--corpus", args.corpus,
            "--keyspace", args.keyspace,
            "--table", args.table,
            "--worker-id", str(i),
            "--rundir", rundir,
            "--progress-rows", str(args.progress_rows),
            "--prewarm-passes", str(args.prewarm_passes),
            "--max-secs", str(args.max_secs),
        ]
        log = open(os.path.join(rundir, f"worker-{i}.log"), "w")
        procs.append((i, subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT), log))
    return procs


def wait_ready(procs, rundir, timeout_s):
    """Block until every worker has prewarmed. A worker that DIED is fatal here."""
    deadline = time.monotonic() + timeout_s
    while True:
        ready = [i for i, _, _ in procs if os.path.exists(os.path.join(rundir, f"ready-{i}"))]
        for i, p, _ in procs:
            if p.poll() is not None and i not in ready:
                die(f"worker {i} exited rc={p.returncode} before signalling ready (see worker-{i}.log)")
        if len(ready) == len(procs):
            return
        if time.monotonic() > deadline:
            die(f"only {len(ready)}/{len(procs)} workers prewarmed within {timeout_s}s")
        time.sleep(0.05)


def wait_steady(procs, rundir, min_samples, timeout_s):
    """Block until every worker has emitted >= min_samples post-barrier records.

    This is the affirmative observation that all S scans are concurrently
    producing rows BEFORE the window opens — the precondition the aligned-window
    convention exists to satisfy. It is checked again after the fact by the
    `WINDOW_NOT_SPANNED` guard, from the recorded timestamps.
    """
    deadline = time.monotonic() + timeout_s
    while True:
        counts = []
        for i, p, _ in procs:
            if p.poll() is not None:
                die(f"worker {i} exited rc={p.returncode} during steady-state ramp (see worker-{i}.log)")
            path = os.path.join(rundir, f"worker-{i}.progress.jsonl")
            try:
                with open(path) as fh:
                    counts.append(sum(1 for line in fh if line.strip()))
            except FileNotFoundError:
                counts.append(0)
        if min(counts) >= min_samples:
            return counts
        if time.monotonic() > deadline:
            die(f"steady state not reached in {timeout_s}s; per-worker progress samples: {counts}")
        time.sleep(0.05)


def perf_window(args, rundir, cpu_list):
    """Open, hold and close the counting window via perf's control FIFO."""
    ctl = os.path.join(rundir, "perf.ctl")
    ack = os.path.join(rundir, "perf.ack")
    for f in (ctl, ack):
        if os.path.exists(f):
            os.unlink(f)
        os.mkfifo(f)
    csv = os.path.join(rundir, "perf.csv")

    # `-D -1` starts with counters DISABLED; `-C` is CPU-wide counting over
    # exactly the pinned CPUs. Per-process/per-thread counting is forbidden by
    # the rig (spec R2, `scripts/perf/lib-perf-lint.sh`): it measured >2x observer
    # cost on this workload. The `sleep` child merely holds perf open; it is not
    # the workload and is not on the measured CPUs.
    cmd = [
        "perf", "stat", "-x,", "-o", csv,
        "-D", "-1",
        "--control", f"fifo:{ctl},{ack}",
        "-C", cpu_list,
        "-e", args.events,
        "--", "sleep", str(args.duration_s + args.perf_slack_s),
    ]
    perf_log = open(os.path.join(rundir, "perf.log"), "w")
    proc = subprocess.Popen(cmd, stdout=perf_log, stderr=subprocess.STDOUT)

    # Opening our write end blocks until perf opens its read end: that IS the
    # readiness handshake, so no sleep-and-hope is needed.
    ctl_fd = os.open(ctl, os.O_WRONLY)
    ack_fd = os.open(ack, os.O_RDONLY | os.O_NONBLOCK)

    def command(word, timeout=30.0):
        os.write(ctl_fd, (word + "\n").encode())
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                data = os.read(ack_fd, 64)
                if data:
                    return
            except BlockingIOError:
                pass
            time.sleep(0.001)
        die(f"perf did not acknowledge '{word}' within {timeout}s")

    command("enable")
    t0 = now_ns()
    time.sleep(args.duration_s)
    command("disable")
    t1 = now_ns()
    os.close(ctl_fd)
    os.close(ack_fd)

    rc = proc.wait(timeout=args.perf_slack_s + 60)
    perf_log.close()
    if rc != 0:
        die(f"perf exited rc={rc} (see perf.log)")
    return t0, t1, csv


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--s", type=int, required=True)
    ap.add_argument("--rep", type=int, required=True)
    ap.add_argument("--round", type=int, required=True)
    ap.add_argument("--rundir", required=True)
    ap.add_argument("--worker-bin", required=True)
    ap.add_argument("--corpus", required=True)
    ap.add_argument("--keyspace", default="ws0")
    ap.add_argument("--table", default="events")
    ap.add_argument("--worker-cpus", required=True, help="JSON list of per-worker CPU lists, e.g. [[0,8],[1,9]]")
    ap.add_argument("--events", required=True)
    ap.add_argument("--duration-s", type=float, required=True)
    ap.add_argument("--progress-rows", type=int, default=16384)
    ap.add_argument("--prewarm-passes", type=int, default=1)
    ap.add_argument("--max-secs", type=int, default=900)
    ap.add_argument("--ready-timeout-s", type=float, default=600.0)
    ap.add_argument("--steady-samples", type=int, default=3)
    ap.add_argument("--steady-timeout-s", type=float, default=300.0)
    ap.add_argument("--perf-slack-s", type=float, default=5.0)
    args = ap.parse_args()

    worker_cpus = json.loads(args.worker_cpus)
    if len(worker_cpus) != args.s:
        die(f"--worker-cpus has {len(worker_cpus)} entries but --s is {args.s}")
    cpu_list = ",".join(str(c) for cpus in worker_cpus for c in cpus)

    rundir = args.rundir
    if os.path.exists(rundir):
        shutil.rmtree(rundir)
    os.makedirs(rundir)

    clock_check = verify_clock_source(args.worker_bin)
    procs = launch_workers(args, rundir, worker_cpus)
    try:
        wait_ready(procs, rundir, args.ready_timeout_s)
        with open(os.path.join(rundir, "go"), "w") as fh:
            fh.write(f"{now_ns()}\n")
        steady = wait_steady(procs, rundir, args.steady_samples, args.steady_timeout_s)
        t0, t1, csv = perf_window(args, rundir, cpu_list)
    finally:
        # The stop file is written on EVERY path, including a failure, so a rep
        # that dies cannot leave six scans spinning on a metered box.
        with open(os.path.join(args.rundir, "stop"), "w") as fh:
            fh.write(f"{now_ns()}\n")
        for i, p, log in procs:
            try:
                p.wait(timeout=180)
            except subprocess.TimeoutExpired:
                p.kill()
            log.close()

    bad = [(i, p.returncode) for i, p, _ in procs if p.returncode != 0]
    if bad:
        die(f"workers exited non-zero: {bad} (a scan that observed nothing exits non-zero by design)")

    window = {
        "issue": 3299,
        "s": args.s,
        "rep": args.rep,
        "round": args.round,
        "t0_ns": t0,
        "t1_ns": t1,
        "window_ns": t1 - t0,
        "requested_duration_s": args.duration_s,
        "perf_cpus": cpu_list,
        "worker_cpus": worker_cpus,
        "events": args.events,
        "perf_csv": os.path.basename(csv),
        "progress_rows": args.progress_rows,
        "prewarm_passes": args.prewarm_passes,
        "steady_samples_at_go": steady,
        "clock_source_check": clock_check,
        "protocol": "warm; aligned window strictly inside S-concurrent steady state",
    }
    with open(os.path.join(rundir, "window.json"), "w") as fh:
        json.dump(window, fh, indent=2)
    print(json.dumps({"rep": args.rep, "s": args.s, "window_ns": t1 - t0, "rundir": rundir}))


if __name__ == "__main__":
    main()
