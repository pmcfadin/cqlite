#!/usr/bin/env python3
"""Fold one sweep point's raw captures into a single JSONL record (issue #3217).

Inputs: the perf-stat CSV, the flight-loadgen step JSONL, and a context JSON
written by sweep.sh holding the /proc deltas and the run identity.

Two contracts this file exists to keep:

AC6 (byte basis) -- every throughput field is EXPLICITLY named and paired with a
`*_basis` string. There is no bare `MB_per_s` anywhere. Where a basis cannot be
established the value is `null` and the basis string says why. We never divide
`rchar` by `read_bytes` or by `syscr`: they are three different layers (bytes
requested by the process / bytes fetched from the block device / read syscall
count) and any ratio of them is meaningless, so all three are reported raw.

AC1 (validity) -- if the CLIENT pinned set exceeded the saturation threshold, the
point measured the client, not the engine. It is stamped `client_saturated:true`
and `validity:"INVALID_CLIENT_SATURATED"`, and the reason is spelled out in the
record so a downstream reader cannot quote its throughput as a server number.
"""
from __future__ import annotations

import argparse
import json
import sys


def parse_perf_csv(path: str) -> dict:
    """perf stat -x, output -> {event: count}. Unsupported events -> None."""
    out: dict[str, object] = {}
    try:
        fh = open(path)
    except OSError:
        return out
    with fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            f = line.split(",")
            if len(f) < 3:
                continue
            ev = f[2].strip()
            if not ev:
                continue
            raw = f[0].strip()
            if raw in ("<not counted>", "<not supported>", ""):
                out.setdefault(ev, None)
                continue
            try:
                out[ev] = out.get(ev) or 0
                out[ev] = (out[ev] or 0) + int(float(raw))
            except ValueError:
                out.setdefault(ev, None)
    return out


def last_step(path: str) -> dict:
    recs = []
    try:
        for line in open(path):
            line = line.strip()
            if line:
                recs.append(json.loads(line))
    except OSError:
        return {}
    return recs[-1] if recs else {}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--perf-csv", required=True)
    ap.add_argument("--step-jsonl", required=True)
    ap.add_argument("--context-json", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()

    ctx = json.load(open(a.context_json))
    perf = parse_perf_csv(a.perf_csv)
    step = last_step(a.step_jsonl)

    dur = float(step.get("duration_s") or ctx.get("wall_secs") or 0.0) or None
    rows = int(step.get("rows_total") or 0)
    ok = int(step.get("requests_ok") or 0)
    rows_s = float(step.get("rows_per_s") or 0.0)

    n_srv = int(ctx["server_cpu_count"])
    n_cli = int(ctx["client_cpu_count"])
    srv_cpu = float(ctx["server_cpu_secs_delta"])
    cli_busy = float(ctx["client_cpuset_busy_secs_delta"])

    srv_util = (srv_cpu / dur / n_srv) if dur and n_srv else None
    cli_util = (cli_busy / dur / n_cli) if dur and n_cli else None

    thr = float(ctx["client_saturation_threshold"])
    saturated = bool(cli_util is not None and cli_util > thr)

    # ---- byte basis (AC6). Three separately-labelled bases, never a bare MB/s.
    basis = ctx.get("corpus_basis") or {}
    rows_per_scan = (rows / ok) if ok else None
    ondisk_total = basis.get("ondisk_compressed_bytes")
    logical_total = basis.get("logical_uncompressed_bytes")

    def per_row(total):
        if total is None or not rows_per_scan:
            return None
        return total / rows_per_scan

    ondisk_bpr = per_row(ondisk_total)
    logical_bpr = ctx.get("logical_bytes_per_row_override") or per_row(logical_total)

    def rate(bpr):
        return (rows_s * bpr) if (bpr and rows_s) else None

    unavailable = (
        "UNAVAILABLE: needs both a corpus byte total and an observed rows-per-scan "
        "(rows_total/requests_ok); reported null rather than fabricated"
    )

    rec = {
        "schema": "ws0-3217.sweep-point/v1",
        "label": ctx["label"],
        "ts_unix_ms": ctx["ts_unix_ms"],
        "harness_commit": ctx.get("harness_commit"),

        # ---- configuration ----
        "server_physical_cores_S": ctx.get("server_physical_cores_S"),
        "server_cpus": ctx["server_cpus"],
        "server_cpu_count_hw_threads": n_srv,
        "client_cpus": ctx["client_cpus"],
        "client_cpu_count_hw_threads": n_cli,
        "merge_path": ctx["merge_path"],
        "target_concurrency_N": ctx["N"],
        "rep": ctx["rep"],
        "reps_total": ctx["reps_total"],
        "step_seconds_requested": ctx["step_seconds"],
        "shape": step.get("shape"),
        "seed": step.get("seed"),
        "server_flags": ctx.get("server_flags"),

        # ---- validity gate (AC1) — deliberately near the top of the record ----
        "validity": "INVALID_CLIENT_SATURATED" if saturated else "OK",
        "client_saturated": saturated,
        "client_cpu_utilization_of_pinned_set": cli_util,
        "client_saturation_threshold": thr,
        "client_saturation_note": (
            "CLIENT SATURATED: the client pinned set exceeded %.0f%% busy, so this point "
            "measured the LOADGEN, not the engine. It MUST NOT be reported as a server "
            "throughput measurement." % (thr * 100)
        ) if saturated else "client pinned set below threshold; point is a valid server measurement",

        # ---- throughput, per AC6: rows first, then three named byte bases ----
        "duration_s": dur,
        "requests_ok": ok,
        "requests_unavailable": step.get("requests_unavailable"),
        "requests_error": step.get("requests_error"),
        "error_codes": step.get("error_codes"),
        "admission_clean": (step.get("requests_unavailable") == 0),
        "rows_total": rows,
        "rows_per_scan_observed": rows_per_scan,
        "rows_per_s_aggregate": rows_s,
        "rows_per_s_per_stream": (rows_s / ctx["N"]) if ctx["N"] else None,
        "qps": step.get("qps"),

        "bytes_per_s_logical_uncompressed": rate(logical_bpr),
        "bytes_per_s_logical_uncompressed_basis": (
            "rows_per_s x logical-uncompressed bytes/row (%s)" % (
                "operator override WS0_LOGICAL_BYTES_PER_ROW"
                if ctx.get("logical_bytes_per_row_override")
                else "corpus CompressionInfo.db dataLength / rows_per_scan_observed")
        ) if logical_bpr else unavailable,

        "bytes_per_s_ondisk_compressed": rate(ondisk_bpr),
        "bytes_per_s_ondisk_compressed_basis": (
            "rows_per_s x on-disk bytes/row (sum of *-Data.db sizes / rows_per_scan_observed)"
        ) if ondisk_bpr else unavailable,

        "bytes_per_s_arrow_wire_capacity": step.get("bytes_per_s"),
        "bytes_per_s_arrow_wire_capacity_basis": (
            "flight-loadgen bytes_per_s: Arrow buffer CAPACITY bytes summed client-side. "
            "NOT compressed gRPC-on-the-wire bytes and NOT the SSTable byte volume."
        ),
        "logical_uncompressed_bytes_per_row": logical_bpr,
        "ondisk_compressed_bytes_per_row": ondisk_bpr,

        "latency_ms": step.get("latency_ms"),

        # ---- server CPU ----
        "server_cpu_secs": srv_cpu,
        "server_cpu_utilization_of_pinned_set": srv_util,
        "rows_per_server_cpu_sec": (rows / srv_cpu) if srv_cpu else None,

        # ---- perf stat over the SERVER cpu set ----
        "perf_scope": "perf stat -C %s (CPU-wide over the server pinned set)" % ctx["server_cpus"],
        "cycles": perf.get("cycles"),
        "instructions": perf.get("instructions"),
        "IPC": (perf["instructions"] / perf["cycles"])
               if perf.get("cycles") and perf.get("instructions") else None,
        "cycles_per_row": (perf["cycles"] / rows) if perf.get("cycles") and rows else None,
        "instructions_per_row": (perf["instructions"] / rows) if perf.get("instructions") and rows else None,
        "context_switches_cpu_wide": perf.get("context-switches"),
        "context_switches_per_second_cpu_wide": (perf["context-switches"] / dur)
               if perf.get("context-switches") and dur else None,
        "cpu_migrations_cpu_wide": perf.get("cpu-migrations"),
        "task_clock_msec": perf.get("task-clock"),

        # ---- AC5: voluntary / involuntary split, process-scoped ----
        "server_voluntary_ctxt_switches": ctx["server_ctxt_delta"].get("voluntary_ctxt_switches"),
        "server_nonvoluntary_ctxt_switches": ctx["server_ctxt_delta"].get("nonvoluntary_ctxt_switches"),
        "server_voluntary_ctxt_switches_per_s": (
            ctx["server_ctxt_delta"].get("voluntary_ctxt_switches", 0) / dur) if dur else None,
        "server_nonvoluntary_ctxt_switches_per_s": (
            ctx["server_ctxt_delta"].get("nonvoluntary_ctxt_switches", 0) / dur) if dur else None,
        "ctxt_switch_scope_note": (
            "voluntary/nonvoluntary are the MAIN-THREAD counters from /proc/<pid>/status and "
            "under-count a multi-threaded server; context_switches_cpu_wide from perf -C is the "
            "whole-pinned-set figure. Reported side by side, never summed."
        ),

        # ---- /proc/<pid>/io: three layers, reported separately, NEVER divided ----
        "server_io_delta": {
            k: ctx["server_io_delta"].get(k) for k in ("rchar", "read_bytes", "syscr")
        },
        "server_io_delta_note": (
            "rchar = bytes the process requested via read(); read_bytes = bytes actually fetched "
            "from the block device; syscr = read syscall count. Different layers - do not divide "
            "one by another."
        ),
    }

    with open(a.out, "a") as fh:
        fh.write(json.dumps(rec) + "\n")

    flag = "  <<< CLIENT SATURATED - NOT A SERVER MEASUREMENT" if saturated else ""
    adm = "" if rec["admission_clean"] else "  <<< requests_unavailable=%s" % rec["requests_unavailable"]
    print("N=%-3s rep=%s rows/s=%-14.1f srv_util=%-6s cli_util=%-6s%s%s" % (
        ctx["N"], ctx["rep"], rows_s,
        ("%.3f" % srv_util) if srv_util is not None else "n/a",
        ("%.3f" % cli_util) if cli_util is not None else "n/a", flag, adm))
    return 0


if __name__ == "__main__":
    sys.exit(main())
