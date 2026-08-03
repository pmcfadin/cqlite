#!/usr/bin/env python3
"""AC4 blocked-stack attribution, v2 — the match table refined against REAL symbols.

Issue #3217 Part B. v1 (`harness/classify-offcpu.py`) was written before any real
symbol existed. Against the first real capture it put 76-83% of blocked time in
`other`, for TWO reasons, both tooling and neither a measurement result:

  1. bcc emits RAW Rust v0 symbols (`_RNvNvMs0_...`); v1's patterns are the
     DEMANGLED spellings, so almost nothing matched. Fixed by demangling first
     (`demangle-folded.py`) and matching on the demangled text.
  2. v1's patterns named the shapes the do_get handoff was EXPECTED to have. The
     real profile is dominated by frames v1 never anticipated (see below).

WHAT v2 ADDS (each entry is a symbol observed in a real capture, not a guess):

  mpsc_send_park          + `tokio::future::block_on::block_on::<...Sender...send`
                            (this IS `blocking_send`'s expansion — v1's
                            `blocking_send` literal never appears in a stack)
                          + `std::sync::mpmc::Sender` / `SyncSender`
                            (cqlite-core's query_rows channel is a STD
                            sync_channel, not a tokio mpsc — v1 had no std case)
                          + `spawn_streaming`
  mpsc_recv_park          + `QueryRowStream>::next_batch`, `std::sync::mpmc::Receiver`
  tonic_grpc_socket_write + `arrow_flight::encode::FlightDataEncoder`, `poll_frame`,
                            `http_body`, `EncodeBody`
  tokio_scheduler         + `multi_thread::worker::Launch`, `io::driver::Driver>::turn`,
                            `blocking::pool::Spawner`

THE SEVENTH BUCKET IS NOT A DUMPING GROUND. AC4 fixes the bucket set at seven, so
the largest real cause v1 had no bucket for — **glibc malloc arena lock
contention** (`__lll_lock_wait_private` under `_int_malloc`/`cfree`) — correctly
lands in `other`. Hiding it there would be the whole failure mode this file
exists to prevent, so `other` is broken out by named cause in `other_breakdown`,
quantified, and NEVER left as an unnamed residue.

CHANNEL IDENTITY IS THE LOAD-BEARING DISTINCTION (this is the point of Part B).
`mpsc_send_park` is not one channel: the bypass read path stacks FOUR bounded
channels between the SSTable and the wire, and lumping them together would
attribute core-read-path parks to the `do_get` handoff — the exact wrong answer.
So every send/recv park is ALSO tagged with which channel it belongs to,
identified from the channel's ITEM TYPE in the demangled symbol:

  | channel                   | item type in symbol            | cap | granularity     |
  |---------------------------|--------------------------------|-----|-----------------|
  | do_get_batch (THE handoff)| `CreditedBatch` / `ChannelSink`| 4   | 1 per RecordBatch (8192 rows) |
  | core_raw_chunk            | `Sender<bytes::bytes::Bytes>`  | 8   | 1 per 16 KiB compression chunk |
  | core_windowed_batch       | `Vec<(RowKey, Scan...)>`       | 2   | 1 per 256 rows  |
  | core_query_rows           | `QueryRowMsg`                  | 4   | 1 per 128 rows  |

Capacities/granularities are the production constants: `DO_GET_CHANNEL_CAPACITY`
(cqlite-flight/src/streaming.rs), `RAW_CHUNK_CHANNEL_CAP` / `BATCH_EMIT_ROWS` /
`BATCH_CHANNEL_CAP` (cqlite-core scan_stream_windowed.rs), `QUERY_ROWS_PER_BATCH`
/ `QUERY_ROWS_CHANNEL_BATCHES` (summary_scan/query_rows.rs).

Usage: classify-offcpu-v2.py <folded> [--out-json f] [--out-table f] [--label L]
                             [--already-demangled]
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from demangle_helper import demangle_frame  # noqa: E402

MATCH_TABLE: list[tuple[str, list[str]]] = [
    ("egress_credit_acquire", [
        "egress_credit", "EgressCredit", "EgressReservation", "EgressBudget",
    ]),
    ("mpsc_send_park", [
        "ChannelSink", "spawn_streaming", "run_merge_catching_panics",
        "stream_subphase", "GrpcWrite",
        "block_on::<<tokio::sync::mpsc::bounded::Sender",
        "mpsc::bounded::Sender", "blocking_send",
        "std::sync::mpmc::Sender", "SyncSender", "mpmc::Sender",
    ]),
    ("mpsc_recv_park", [
        "mpsc::bounded::Receiver", "ReceiverStream", "poll_recv", "recv_many",
        "std::sync::mpmc::Receiver", "mpmc::Receiver",
        "QueryRowStream>::next_batch", "QueryRowStream",
    ]),
    ("tonic_grpc_socket_write", [
        "tcp_sendmsg", "sock_sendmsg", "__sys_sendto", "sk_stream_wait_memory",
        "tcp_push", "framed_write", "poll_flush", "poll_write",
        "tonic::codec::encode", "EncodeBody", "h2::proto", "hyper::proto::h2",
        "arrow_flight::encode", "http_body", "poll_frame",
    ]),
    ("disk_io", [
        "io_schedule", "submit_bio", "blk_mq", "blkdev", "nvme",
        "wait_on_page_bit", "folio_wait_bit", "filemap_", "read_pages",
        "page_cache_ra", "ext4", "xfs_", "btrfs",
        "__x64_sys_pread64", "ksys_pread64", "vfs_read", "new_sync_read",
        "wait_on_buffer", "balance_dirty_pages",
    ]),
    ("tokio_scheduler", [
        "tokio::runtime::scheduler", "multi_thread::worker", "multi_thread::worker::Launch",
        "blocking::pool", "blocking::pool::Spawner", "io::driver::Driver>::turn",
        "runtime::park", "Parker", "park_timeout", "epoll_wait", "io::driver",
        "tokio::runtime::time", "condvar", "pthread_cond",
    ]),
]
BUCKETS = [b for b, _ in MATCH_TABLE] + ["other"]

# `other` is broken out by NAMED cause; nothing stays an unnamed residue.
OTHER_CAUSES: list[tuple[str, list[str]]] = [
    ("glibc_malloc_arena_lock", ["__lll_lock_wait", "_int_malloc", "_int_free",
                                 "arena_get", "malloc_consolidate", "tcache",
                                 "__libc_malloc", "cfree"]),
    ("kernel_page_fault", ["handle_mm_fault", "do_user_addr_fault", "exc_page_fault"]),
    ("mmap_munmap", ["__x64_sys_mmap", "__x64_sys_munmap", "vm_munmap"]),
    ("thread_lifecycle", ["clone3", "do_exit", "__x64_sys_exit", "Thread::new"]),
    ("unsymbolized_frames_only", ["[unknown]"]),
]

# `tokio_scheduler` is the generic bucket, so it too is broken out: an IDLE
# runtime park (a worker or io-driver with nothing to do) and a park inside a
# live task mean completely different things for a throughput analysis.
TOKIO_SUB: list[tuple[str, list[str]]] = [
    ("idle_worker_park", ["multi_thread::worker::Launch", "multi_thread::worker"]),
    ("io_driver_epoll_park", ["io::driver::Driver>::turn", "io::driver", "epoll_wait"]),
    ("blocking_pool_idle_thread", ["blocking::pool::Spawner", "blocking::pool"]),
    ("timer_park", ["tokio::runtime::time", "park_timeout"]),
]

CHANNELS: list[tuple[str, list[str], str]] = [
    ("do_get_batch", ["CreditedBatch", "ChannelSink"],
     "cap 4, ONE reserve + ONE emit per Flight RecordBatch (8192 rows) - THE #3217 handoff"),
    ("core_raw_chunk", ["Sender<bytes::bytes::Bytes>", "scan_stream_windowed_read",
                        "decode_scan_chunk"],
     "cap 8, ONE send per 16 KiB compression chunk (23.63 rows/chunk on this corpus)"),
    ("core_query_rows", ["QueryRowMsg", "summary_scan::query_rows", "QueryRowStream"],
     "std sync_channel, cap 4, ONE send per 128 rows"),
    ("core_windowed_batch", ["Vec<(cqlite_core::types::RowKey", "scan_stream_windowed",
                             "scan_stream_forwarder"],
     "cap 2, ONE send per 256 rows"),
]


def first_match(stack: str, table) -> tuple[str, str]:
    """LEAF-FIRST frame scan. The reason a thread is off-CPU is the innermost
    thing it called, NOT whatever happens to appear anywhere in its stack.

    v1 matched a pattern anywhere in the whole stack, which inverts causality on
    every real stack: the producer thread's stack contains `pread64` (it read a
    chunk earlier in the same call chain) AND `Sender::send` (where it is
    actually parked), so a whole-stack match attributed a CHANNEL park to
    `disk_io` (measured: v1 reported 278 s of `disk_io` at S=6/N=16 that is in
    fact channel-send blocking). Folded stacks run root -> leaf, so scanning
    reversed finds the innermost matching frame first; bucket table order only
    breaks ties WITHIN one frame."""
    frames = stack.split(";")
    for f in reversed(frames):
        for entry in table:
            name, pats = entry[0], entry[1]
            for p in pats:
                if p in f:
                    return name, p
    return "", ""


def classify(stack: str) -> tuple[str, str]:
    n, p = first_match(stack, MATCH_TABLE)
    return (n or "other"), p


# Frames that are the MECHANISM of every off-CPU stack. Stripped from the
# DISPLAY only (never from matching) so the printed stack shows the cause rather
# than the same futex/schedule tail on every line.
MECHANISM = ("futex", "__schedule", "schedule", "finish_task_switch", "do_syscall_64",
             "entry_SYSCALL_64", "x64_sys_call", "syscall", "__x64_sys_futex",
             "do_futex", "__futex_wait", "futex_wait", "futex_do_wait")


def short(stack: str, keep: int = 4) -> str:
    f = [x for x in stack.split(";") if not any(m in x for m in MECHANISM)]
    if not f:
        f = stack.split(";")
    f = [x[:150] for x in f]
    return " <- ".join(reversed(f[-keep:]))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("folded")
    ap.add_argument("--out-json")
    ap.add_argument("--out-table")
    ap.add_argument("--label", default="")
    ap.add_argument("--top", type=int, default=20)
    ap.add_argument("--already-demangled", action="store_true")
    a = ap.parse_args()

    entries = []
    for line in open(a.folded):
        line = line.rstrip("\n")
        if not line.strip():
            continue
        try:
            stack, val = line.rsplit(" ", 1)
            us = int(float(val))
        except ValueError:
            continue
        if not a.already_demangled:
            stack = ";".join(demangle_frame(f) for f in stack.split(";"))
        entries.append((stack, us))
    if not entries:
        print("ERROR: no folded stacks parsed", file=sys.stderr)
        return 1

    total = sum(u for _, u in entries)
    bucket_us = {b: 0 for b in BUCKETS}
    bucket_stacks = defaultdict(list)
    other_us = {c[0]: 0 for c in OTHER_CAUSES}
    other_us["unnamed"] = 0
    other_unnamed = []
    tokio_us = {c[0]: 0 for c in TOKIO_SUB}
    tokio_us["unnamed"] = 0
    chan_us = {c[0]: 0 for c in CHANNELS}
    chan_us["unattributed_channel"] = 0

    for stack, us in entries:
        b, _ = classify(stack)
        bucket_us[b] += us
        bucket_stacks[b].append((us, stack))
        if b == "other":
            c, _ = first_match(stack, OTHER_CAUSES)
            if c:
                other_us[c] += us
            else:
                other_us["unnamed"] += us
                other_unnamed.append((us, stack))
        if b == "tokio_scheduler":
            c, _ = first_match(stack, TOKIO_SUB)
            tokio_us[c or "unnamed"] += us
        if b in ("mpsc_send_park", "mpsc_recv_park", "egress_credit_acquire"):
            c, _ = first_match(stack, CHANNELS)
            chan_us[c or "unattributed_channel"] += us

    handoff_total = sum(chan_us.values())
    doc = {
        "schema": "ws0-3217.offcpu-attribution/v2",
        "label": a.label, "folded_file": a.folded,
        "total_blocked_time_us": total, "unique_stacks": len(entries),
        "value_unit": "microseconds of off-CPU (blocked) time, summed over all threads",
        "v2_note": ("match table refined against REAL demangled symbols; v1 put 76-83% in "
                    "`other` because bcc emits raw Rust v0 mangling and v1's patterns were "
                    "the demangled spellings"),
        "buckets": [
            {"bucket": b, "blocked_time_us": bucket_us[b],
             "pct_of_total_blocked": (bucket_us[b] / total * 100.0) if total else 0.0,
             "unique_stacks": len(bucket_stacks[b]), "present": bucket_us[b] > 0,
             "absent_note": None if bucket_us[b] > 0 else
                 "EXPLICIT ZERO: no stack in this capture matched this bucket",
             "top_stacks": [{"blocked_time_us": u, "stack": short(s)}
                            for u, s in sorted(bucket_stacks[b], reverse=True)[:3]]}
            for b in BUCKETS],
        "other_breakdown": [
            {"cause": k, "blocked_time_us": v,
             "pct_of_total_blocked": (v / total * 100.0) if total else 0.0}
            for k, v in sorted(other_us.items(), key=lambda kv: -kv[1])],
        "tokio_scheduler_breakdown": [
            {"cause": k, "blocked_time_us": v,
             "pct_of_total_blocked": (v / total * 100.0) if total else 0.0}
            for k, v in sorted(tokio_us.items(), key=lambda kv: -kv[1])],
        "other_unnamed_top": [{"blocked_time_us": u, "stack": short(s)}
                              for u, s in sorted(other_unnamed, reverse=True)[:10]],
        "channel_identity": {
            "note": ("which bounded channel each send/recv park belongs to. The bypass read "
                     "path stacks FOUR channels between SSTable and wire; only `do_get_batch` "
                     "is the #3217 handoff."),
            "total_handoff_blocked_us": handoff_total,
            "channels": [
                {"channel": name, "blocked_time_us": chan_us[name],
                 "pct_of_total_blocked": (chan_us[name] / total * 100.0) if total else 0.0,
                 "pct_of_handoff_blocked": (chan_us[name] / handoff_total * 100.0)
                                            if handoff_total else 0.0,
                 "description": desc}
                for name, _, desc in CHANNELS
            ] + [{"channel": "unattributed_channel",
                  "blocked_time_us": chan_us["unattributed_channel"],
                  "pct_of_total_blocked": (chan_us["unattributed_channel"] / total * 100.0) if total else 0.0,
                  "pct_of_handoff_blocked": (chan_us["unattributed_channel"] / handoff_total * 100.0) if handoff_total else 0.0,
                  "description": "send/recv park whose channel item type did not resolve"}],
        },
        "ranked_stacks": [
            {"rank": i + 1, "blocked_time_us": u,
             "pct_of_total_blocked": (u / total * 100.0) if total else 0.0,
             "bucket": classify(s)[0], "stack": s}
            for i, (s, u) in enumerate(sorted(entries, key=lambda kv: -kv[1])[:a.top])],
    }

    L = ["==== WS0 #3217 OFF-CPU BLOCKED-STACK ATTRIBUTION (v2) ===="]
    L.append("label: %s   total blocked: %.3f s across %d unique stacks"
             % (a.label, total / 1e6, len(entries)))
    L.append("")
    L.append("%-26s %14s %8s %8s" % ("bucket", "blocked (s)", "pct", "stacks"))
    for b in doc["buckets"]:
        L.append("%-26s %14.4f %7.2f%% %8d%s" % (
            b["bucket"], b["blocked_time_us"] / 1e6, b["pct_of_total_blocked"],
            b["unique_stacks"], "   (explicit zero)" if not b["present"] else ""))
    L.append("%-26s %14.4f %7.2f%%" % ("TOTAL", total / 1e6, 100.0))
    L.append("")
    L.append("`other` broken out by NAMED cause (AC4 fixes 7 buckets; nothing is left unnamed):")
    for o in doc["other_breakdown"]:
        if o["blocked_time_us"]:
            L.append("    %-30s %12.4f s  %6.2f%%" % (o["cause"], o["blocked_time_us"] / 1e6,
                                                      o["pct_of_total_blocked"]))
    L.append("")
    L.append("`tokio_scheduler` broken out (an IDLE runtime park is not a cost):")
    for o in doc["tokio_scheduler_breakdown"]:
        if o["blocked_time_us"]:
            L.append("    %-30s %12.4f s  %6.2f%%" % (o["cause"], o["blocked_time_us"] / 1e6,
                                                      o["pct_of_total_blocked"]))
    L.append("")
    L.append("CHANNEL IDENTITY of send/recv/credit parks (%.4f s total):" % (handoff_total / 1e6))
    L.append("    %-22s %12s %9s %9s" % ("channel", "blocked(s)", "%total", "%handoff"))
    for c in doc["channel_identity"]["channels"]:
        L.append("    %-22s %12.4f %8.2f%% %8.2f%%   %s" % (
            c["channel"], c["blocked_time_us"] / 1e6, c["pct_of_total_blocked"],
            c["pct_of_handoff_blocked"], c["description"][:64]))
    table = "\n".join(L) + "\n"
    if a.out_json:
        open(a.out_json, "w").write(json.dumps(doc, indent=1) + "\n")
    if a.out_table:
        open(a.out_table, "w").write(table)
    sys.stdout.write(table)
    return 0


if __name__ == "__main__":
    sys.exit(main())
