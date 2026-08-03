#!/usr/bin/env python3
"""Parse `park-count.bt` bpftrace output into per-site voluntary-park COUNTS.

Issue #3217 Part B. offcputime charges MICROSECONDS of blocked time; this charges
EVENTS. The two answer different questions and the "~1,960 voluntary parks per
8192-row batch" question is an EVENT question — a site can dominate the event
count while contributing almost no blocked time (a high-frequency, short park),
which is exactly the shape a fine-grained handoff has.

bpftrace prints `@vol_by_stack[<comm>, \n <frame>\n <frame>...\n]: <count>`.
Frames are RAW Rust symbols (bpftrace, like bcc, does not demangle Rust), so this
demangles via the same helper the off-CPU folded files use.

Sites are named from the FIRST frame (innermost-out) matching the site table
below, which is derived from the actual pipeline, not guessed:

  do_get_mpsc_emit/reserve  cqlite-flight ChannelSink -> bounded mpsc, cap 4,
                            ONE reserve + ONE emit per Flight RecordBatch
  core_query_rows_channel   cqlite-core summary_scan::query_rows std sync_channel,
                            QUERY_ROWS_PER_BATCH=128 rows/msg, cap 4
  core_windowed_batch_chan  scan_stream_windowed batch channel,
                            BATCH_EMIT_ROWS=256 rows/msg, cap 2
  core_raw_chunk_chan       scan_stream_windowed_read raw_tx.blocking_send(Bytes),
                            ONE send per 16 KiB COMPRESSION CHUNK, cap 8
  malloc_arena_lock         glibc arena futex (__lll_lock_wait / malloc / cfree)
  grpc_h2_socket            tonic/hyper/h2 egress + socket
  tokio_idle_park           runtime worker / io-driver park (idle, not a handoff)

Usage: parse-park-count.py <park-count.txt> [--out-json f] [--out-table f]
                           [--label L] [--rows-per-s R] [--window-secs W]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from demangle_helper import demangle_frame  # noqa: E402

# Ordered; FIRST match against the whole demangled stack wins. Order matters for
# the same reason it does in classify-offcpu.py: an inner handoff frame is more
# specific than the outer runtime frame that hosts it.
SITES: list[tuple[str, list[str]]] = [
    ("do_get_mpsc_emit_reserve", [
        "cqlite_flight::streaming::ChannelSink", "streaming::ChannelSink",
        "Sender<core::result::Result<cqlite_flight",
        "CreditedBatch",
    ]),
    ("egress_credit_acquire", ["egress_credit", "EgressCredit"]),
    ("core_query_rows_channel", [
        "summary_scan::query_rows", "QueryRowMsg", "QueryRowStream",
    ]),
    ("core_raw_chunk_chan", [
        "Sender<bytes::bytes::Bytes>", "scan_stream_windowed_read",
        "feed_compressed", "decode_scan_chunk",
    ]),
    ("core_windowed_batch_chan", [
        "scan_stream_windowed", "BATCH_EMIT", "scan_stream_forwarder",
        "Sender<core::result::Result<alloc::vec::Vec<(cqlite_core::types::RowKey",
    ]),
    ("malloc_arena_lock", [
        "__lll_lock_wait", "arena_get", "_int_malloc", "_int_free",
        "malloc_consolidate", "tcache",
    ]),
    ("grpc_h2_socket", [
        "arrow_flight::encode", "tonic::codec::encode", "hyper::proto::h2",
        "h2::proto", "http_body", "tcp_sendmsg", "sock_sendmsg",
    ]),
    ("tokio_idle_park", [
        "multi_thread::worker::Launch", "io::driver::Driver>::turn",
        "blocking::pool::Spawner", "park_timeout", "epoll_wait",
        "runtime::scheduler",
    ]),
]

_HDR = re.compile(r"^@(\w+)\[(.*?)(?:,\s*)?$")
_ONE = re.compile(r"^@(\w+)(?:\[([^\]]*)\])?:\s+(\d+)\s*$")
_TAIL = re.compile(r"^\]:\s+(\d+)\s*$")


def site_of(stack: str) -> tuple[str, str]:
    for name, pats in SITES:
        for p in pats:
            if p in stack:
                return name, p
    return "other", ""


def parse(path: str):
    """-> (scalars, [(comm, [frames], count)])"""
    scalars: dict[str, int] = {}
    by_comm: dict[str, dict[str, int]] = defaultdict(dict)
    stacks: list[tuple[str, list[str], int]] = []
    cur_comm, cur_frames = None, []
    for raw in open(path):
        line = raw.rstrip("\n")
        m = _ONE.match(line.strip())
        if m and cur_comm is None:
            name, key, val = m.group(1), m.group(2), int(m.group(3))
            if key is None:
                scalars[name] = val
            else:
                by_comm[name][key] = val
            continue
        m = _HDR.match(line.strip())
        if m and m.group(1) == "vol_by_stack":
            cur_comm = m.group(2).strip().rstrip(",")
            cur_frames = []
            continue
        if cur_comm is not None:
            m = _TAIL.match(line.strip())
            if m:
                stacks.append((cur_comm, cur_frames, int(m.group(1))))
                cur_comm, cur_frames = None, []
            elif line.strip():
                cur_frames.append(demangle_frame(line.strip()))
    return scalars, dict(by_comm), stacks


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("src")
    ap.add_argument("--out-json")
    ap.add_argument("--out-table")
    ap.add_argument("--label", default="")
    ap.add_argument("--rows-per-s", type=float)
    ap.add_argument("--window-secs", type=float, default=30.0)
    ap.add_argument("--batch-rows", type=float, default=8192.0)
    a = ap.parse_args()

    scalars, by_comm, stacks = parse(a.src)
    tot = sum(c for _, _, c in stacks)
    site: Counter = Counter()
    examples: dict[str, list] = defaultdict(list)
    for comm, frames, c in stacks:
        s = ";".join(frames)
        nm, pat = site_of(s)
        site[nm] += c
        if len(examples[nm]) < 3:
            examples[nm].append({"count": c, "comm": comm,
                                 "matched": pat, "frames": frames[:6]})

    vol_total = scalars.get("vol_total", tot)
    invol_total = scalars.get("invol_total", 0)
    w = a.window_secs
    batches_per_s = (a.rows_per_s / a.batch_rows) if a.rows_per_s else None

    doc = {
        "schema": "ws0-3217.park-count/v1", "label": a.label, "src": a.src,
        "window_secs": w, "unit": "EVENTS (voluntary off-CPU switches), not microseconds",
        "voluntary_total": vol_total, "voluntary_per_s": vol_total / w,
        "involuntary_total": invol_total, "involuntary_per_s": invol_total / w,
        "stack_attributed_total": tot,
        "stack_coverage_pct": (tot / vol_total * 100.0) if vol_total else 0.0,
        "rows_per_s": a.rows_per_s, "flight_batch_rows": a.batch_rows,
        "flight_batches_per_s": batches_per_s,
        "voluntary_parks_per_flight_batch": (vol_total / w / batches_per_s) if batches_per_s else None,
        "by_comm_voluntary": by_comm.get("vol_by_comm", {}),
        "by_comm_involuntary": by_comm.get("invol_by_comm", {}),
        "sites": [
            {"site": nm, "parks": site[nm],
             "pct_of_attributed": (site[nm] / tot * 100.0) if tot else 0.0,
             "parks_per_s": site[nm] / w,
             "parks_per_flight_batch": (site[nm] / w / batches_per_s) if batches_per_s else None,
             "examples": examples[nm]}
            for nm in sorted(site, key=lambda k: -site[k])
        ],
    }

    L = ["==== WS0 #3217 VOLUNTARY PARK COUNTS BY SITE (events, not microseconds) ===="]
    L.append("label: %s   window=%.0fs" % (a.label, w))
    L.append("voluntary %d (%.0f/s)   involuntary %d (%.0f/s)   stack-attributed %d (%.1f%% of voluntary)"
             % (vol_total, vol_total / w, invol_total, invol_total / w, tot,
                doc["stack_coverage_pct"]))
    if batches_per_s:
        L.append("rows/s %.0f -> %.2f flight batches/s -> %.0f voluntary parks per %d-row flight batch"
                 % (a.rows_per_s, batches_per_s, doc["voluntary_parks_per_flight_batch"], a.batch_rows))
    L.append("")
    L.append("%-28s %12s %8s %12s %14s" % ("site", "parks", "pct", "parks/s", "parks/batch"))
    for s in doc["sites"]:
        L.append("%-28s %12d %7.2f%% %12.0f %14s" % (
            s["site"], s["parks"], s["pct_of_attributed"], s["parks_per_s"],
            ("%.0f" % s["parks_per_flight_batch"]) if s["parks_per_flight_batch"] else "n/a"))
    table = "\n".join(L) + "\n"
    if a.out_json:
        open(a.out_json, "w").write(json.dumps(doc, indent=1) + "\n")
    if a.out_table:
        open(a.out_table, "w").write(table)
    sys.stdout.write(table)
    return 0


if __name__ == "__main__":
    sys.exit(main())
