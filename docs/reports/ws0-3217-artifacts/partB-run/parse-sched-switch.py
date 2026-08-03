#!/usr/bin/env python3
"""Fully-symbolized voluntary-park COUNTS from `perf script` sched:sched_switch.

Issue #3217 Part B. This is the instrument that answers the "~1,960 voluntary
parks per 8192-row Flight batch — WHAT is parking?" question, because:

  * `offcputime` charges MICROSECONDS, so a site that parks 900 times per batch
    for 2 us each is nearly INVISIBLE to it while dominating the switch count.
    Counts and durations must both be measured; neither substitutes.
  * bpftrace's `ustack` left ~50% of user frames as RAW HEX on this binary
    (measured on park-s6-N1), whereas `perf script` symbolizes and demangles the
    same stacks essentially completely (AC3: 0.019-0.027% unsymbolized).

sched:sched_switch fires in the OUTGOING task's context, so the recorded stack is
the stack the thread parked on, and the tracepoint's prev-state field separates
the two kinds of switch: `R` (TASK_RUNNING) means PREEMPTED (involuntary);
anything else (`S`, `D`, `I`, ...) means it BLOCKED (voluntary).

Reads `perf script -F comm,tid,event,trace,ip,sym` on stdin or from a file.

Usage: parse-sched-switch.py [script.txt] [--rows-per-s R] [--window-secs W]
                             [--batch-rows 8192] [--label L]
                             [--out-json f] [--out-table f] [--out-folded f]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict

HDR = re.compile(r"^(\S.*?)\s+(\d+)\s+sched:sched_switch:\s+.*\]\s+(\S+)\s+==>")

# Ordered site table; matching is LEAF-FIRST over frames (the innermost frame is
# the reason the thread parked). Every entry is a symbol observed in a real
# capture. Granularities are the production constants, cited in the report.
SITES: list[tuple[str, list[str], str]] = [
    ("do_get_mpsc_handoff", ["ChannelSink", "CreditedBatch"],
     "cqlite-flight do_get bounded mpsc, cap 4: 1 reserve + 1 emit per Flight batch"),
    ("egress_credit", ["EgressCredit", "egress_credit", "EgressReservation"],
     "per-stream in-flight capacity-byte credit pool"),
    ("core_raw_chunk_chan", ["Sender<bytes::bytes::Bytes>", "scan_stream_windowed_read",
                             "decode_scan_chunk", "read_compressed_chunk_sync"],
     "cqlite-core raw decompressed-chunk channel, cap 8: 1 send per 16 KiB chunk"),
    ("core_query_rows_chan", ["QueryRowMsg", "summary_scan::query_rows", "QueryRowStream"],
     "cqlite-core query_rows std sync_channel, cap 4: 1 send per 128 rows"),
    ("core_windowed_batch_chan", ["Vec<(cqlite_core::types::RowKey", "scan_stream_windowed",
                                  "scan_stream_forwarder"],
     "cqlite-core windowed batch channel, cap 2: 1 send per 256 rows"),
    ("glibc_malloc_arena_lock", ["__lll_lock_wait", "_int_malloc", "_int_free", "arena_get",
                                 "malloc_consolidate", "__libc_malloc", "cfree", "tcache"],
     "glibc allocator arena futex (NOT one of AC4's seven buckets; reported explicitly)"),
    ("grpc_egress", ["arrow_flight::encode", "tonic::codec", "EncodeBody", "hyper::proto::h2",
                     "h2::proto", "http_body", "poll_frame", "tcp_sendmsg", "sock_sendmsg"],
     "Arrow-Flight encode + tonic/h2 + socket write"),
    ("tokio_runtime_idle", ["multi_thread::worker", "io::driver", "blocking::pool",
                            "epoll_wait", "park_timeout", "tokio::runtime::time"],
     "runtime worker / io-driver / blocking-pool park with nothing to do"),
]


def site_of(frames: list[str]) -> tuple[str, str]:
    for f in frames:           # frames arrive LEAF-FIRST from perf script
        for name, pats, _ in SITES:
            for p in pats:
                if p in f:
                    return name, p
    return "other", ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("src", nargs="?")
    ap.add_argument("--rows-per-s", type=float)
    ap.add_argument("--window-secs", type=float, default=10.0)
    ap.add_argument("--batch-rows", type=float, default=8192.0)
    ap.add_argument("--label", default="")
    ap.add_argument("--out-json")
    ap.add_argument("--out-table")
    ap.add_argument("--out-folded")
    a = ap.parse_args()
    fh = open(a.src) if a.src else sys.stdin

    vol = invol = 0
    site: Counter = Counter()
    site_by_comm: dict[str, Counter] = defaultdict(Counter)
    folded: Counter = Counter()
    examples: dict[str, list] = defaultdict(list)
    cur_comm = cur_state = None
    frames: list[str] = []

    def flush():
        nonlocal vol, invol, frames, cur_comm, cur_state
        if cur_state is None:
            frames = []
            return
        if cur_state == "R":
            invol += 1
        else:
            vol += 1
            nm, pat = site_of(frames)
            site[nm] += 1
            site_by_comm[nm][cur_comm] += 1
            if a.out_folded:
                folded[cur_comm + ";" + ";".join(reversed(frames))] += 1
            if len(examples[nm]) < 2 and frames:
                examples[nm].append({"comm": cur_comm, "matched": pat,
                                     "leaf_frames": frames[:8]})
        frames = []
        cur_comm = cur_state = None

    for line in fh:
        m = HDR.match(line)
        if m:
            flush()
            cur_comm, cur_state = m.group(1).strip(), m.group(3)
            continue
        s = line.strip()
        if s and cur_state is not None:
            parts = s.split(None, 1)
            frames.append(parts[1].split(" (")[0] if len(parts) > 1 else s)
    flush()

    w, tot = a.window_secs, vol + invol
    bps = (a.rows_per_s / a.batch_rows) if a.rows_per_s else None
    doc = {
        "schema": "ws0-3217.sched-switch-counts/v1", "label": a.label,
        "window_secs": w, "unit": "EVENTS (context switches)",
        "voluntary": vol, "voluntary_per_s": vol / w,
        "involuntary": invol, "involuntary_per_s": invol / w,
        "total_switches": tot,
        "rows_per_s": a.rows_per_s, "flight_batch_rows": a.batch_rows,
        "flight_batches_per_s": bps,
        "voluntary_parks_per_flight_batch": (vol / w / bps) if bps else None,
        "voluntary_parks_per_1k_rows": (vol / w / a.rows_per_s * 1000) if a.rows_per_s else None,
        "sites": [
            {"site": nm, "parks": site[nm],
             "pct_of_voluntary": 100.0 * site[nm] / vol if vol else 0.0,
             "parks_per_s": site[nm] / w,
             "parks_per_flight_batch": (site[nm] / w / bps) if bps else None,
             "by_comm": dict(site_by_comm[nm]),
             "description": next((d for n, _, d in SITES if n == nm), "unmatched"),
             "examples": examples[nm]}
            for nm in sorted(site, key=lambda k: -site[k])],
    }
    L = ["==== WS0 #3217 VOLUNTARY PARKS BY SITE (perf sched:sched_switch, EVENT counts) ===="]
    L.append("label: %s   window=%.0fs" % (a.label, w))
    L.append("voluntary %d (%.0f/s)   involuntary %d (%.0f/s)" % (vol, vol / w, invol, invol / w))
    if bps:
        L.append("rows/s %.0f  ->  %.2f flight batches/s  ->  %.0f voluntary parks per %d-row batch"
                 " (%.1f per 1k rows)" % (a.rows_per_s, bps,
                                          doc["voluntary_parks_per_flight_batch"], a.batch_rows,
                                          doc["voluntary_parks_per_1k_rows"]))
    L.append("")
    L.append("%-26s %11s %8s %11s %12s" % ("site", "parks", "pct", "parks/s", "parks/batch"))
    for s_ in doc["sites"]:
        L.append("%-26s %11d %7.2f%% %11.0f %12s" % (
            s_["site"], s_["parks"], s_["pct_of_voluntary"], s_["parks_per_s"],
            ("%.0f" % s_["parks_per_flight_batch"]) if s_["parks_per_flight_batch"] else "n/a"))
    t = "\n".join(L) + "\n"
    if a.out_json:
        open(a.out_json, "w").write(json.dumps(doc, indent=1) + "\n")
    if a.out_table:
        open(a.out_table, "w").write(t)
    if a.out_folded:
        with open(a.out_folded, "w") as f:
            for k, v in folded.items():
                f.write("%s %d\n" % (k, v))
    sys.stdout.write(t)
    return 0


if __name__ == "__main__":
    sys.exit(main())
