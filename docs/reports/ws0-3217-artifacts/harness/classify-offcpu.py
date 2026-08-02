#!/usr/bin/env python3
"""Rank and CLASSIFY blocked (off-CPU) stacks for issue #3217, Part C / AC4.

Input: a folded off-CPU stack file, one `frame;frame;... <value>` line per unique
stack, value in MICROSECONDS of blocked time (bcc `offcputime-bpfcc -f`, or the
bpftrace fallback folded the same way).

Output: a ranked table of blocked time by stack, every stack assigned to exactly
ONE of the seven AC4 buckets, plus the residue.

Two AC4 rules this file enforces mechanically:
  * every bucket is either quantified or reported as an EXPLICIT ZERO. A bucket
    absent from the table is an acceptance failure; a 0 is fine. So all seven
    buckets are always emitted.
  * nothing is silently swallowed. Every stack that lands in `other` is listed
    in the `unclassified` residue (ranked, with example frames) so the match
    table can be extended rather than quietly under-counting.

--------------------------------------------------------------------------------
THE MATCH TABLE (ordered; FIRST rule whose pattern appears anywhere in the stack
wins). Order is the whole design, so it is stated explicitly:

 1 egress_credit_acquire   BEFORE mpsc_send_park. `ChannelSink::reserve` calls
                           the egress-credit reserve, so a park there carries
                           BOTH sets of frames; credit acquisition is the more
                           specific cause and must win.
 2 mpsc_send_park          the producer (a `spawn_blocking` thread) parked in
                           `Handle::block_on` waiting for a free slot in the
                           bounded do_get channel (capacity 4). THE thing #3217
                           is trying to indict or acquit.
 3 mpsc_recv_park          BEFORE tonic_grpc_socket_write. The receiver is polled
                           BY tonic's stream machinery, so a recv park carries
                           tonic/h2 frames too; "gRPC layer idle waiting for a
                           batch" is the meaningful attribution, not "socket".
 4 tonic_grpc_socket_write write-specific markers only (sendmsg / framed write /
                           socket buffer wait), so it cannot swallow every
                           tonic-adjacent stack.
 5 disk_io                 BEFORE tokio_scheduler: a page-cache miss / block wait
                           is a concrete cause, the scheduler park is generic.
 6 tokio_scheduler         runtime park / worker idle / blocking-pool idle. The
                           residual "not blocked on anything nameable" bucket.
 7 other                   everything else -> listed in `unclassified`.

Deliberately NOT match keys: `futex`, `schedule`, `finish_task_switch`,
`__schedule`. Those are the MECHANISM of every off-CPU stack, not the reason;
matching on them would swallow the whole profile into one bucket.
--------------------------------------------------------------------------------

Usage:
  classify-offcpu.py <folded.txt> [--top 25] [--out-json f] [--out-table f]
                     [--label L] [--min-us 0]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict

# (bucket, [substrings]) — matched case-insensitively against the whole stack.
# Rust symbols appear either demangled (`tokio::sync::mpsc::bounded::Sender`) or
# in perf's `$LT$`/`..` mangled forms, so both spellings are listed.
MATCH_TABLE: list[tuple[str, list[str]]] = [
    ("egress_credit_acquire", [
        "egress_credit", "egresscredit", "egress_permit", "egresspermit",
        "egress_budget", "egressbudget", "egress_reservation", "egressreservation",
    ]),
    ("mpsc_send_park", [
        "channelsink",                       # cqlite_flight::streaming::ChannelSink
        "stream_subphase",                   # wraps ONLY the egress reserve/send (#2819)
        "streamsubphase", "grpcwrite",
        "mpsc::bounded::sender", "mpsc..bounded..sender",
        "bounded::sender", "bounded..sender",
        "sender::reserve", "sender..reserve",
        "blocking_send", "chan::tx", "chan..tx",
        "run_merge_catching_panics",
    ]),
    ("mpsc_recv_park", [
        "mpsc::bounded::receiver", "mpsc..bounded..receiver",
        "bounded::receiver", "bounded..receiver",
        "receiverstream", "receiver_stream",
        "poll_recv", "recv_many", "chan::rx", "chan..rx",
    ]),
    ("tonic_grpc_socket_write", [
        "tcp_sendmsg", "sock_sendmsg", "__sys_sendto", "sk_stream_wait_memory",
        "tcp_push", "framed_write", "framedwrite",
        "poll_flush", "poll_write", "flush_buf",
        "tonic::codec::encode", "tonic..codec..encode",
        "h2::proto::streams::send", "h2..proto..streams..send",
        "hyper::proto::h2", "hyper..proto..h2",
    ]),
    ("disk_io", [
        "io_schedule", "submit_bio", "blk_mq", "blkdev", "nvme",
        "wait_on_page_bit", "folio_wait_bit", "filemap_", "read_pages",
        "page_cache_ra", "ext4", "xfs_", "btrfs",
        "__x64_sys_pread64", "ksys_pread64", "vfs_read", "new_sync_read",
        "wait_on_buffer", "balance_dirty_pages",
    ]),
    ("tokio_scheduler", [
        "tokio::runtime::scheduler", "tokio..runtime..scheduler",
        "multi_thread::worker", "multi_thread..worker",
        "blocking::pool", "blocking..pool", "blocking_pool",
        "runtime::park", "runtime..park", "parker", "park_timeout",
        "epoll_wait", "io::driver", "io..driver", "driver::park", "driver..park",
        "tokio::runtime::time", "tokio..runtime..time",
        "condvar", "pthread_cond",
    ]),
]
BUCKETS = [b for b, _ in MATCH_TABLE] + ["other"]

# Frames that are pure mechanism; never used to classify (documented above).
MECHANISM_ONLY = ("futex", "__schedule", "schedule", "finish_task_switch",
                  "do_syscall_64", "entry_SYSCALL_64")

_BYTES_REPR = re.compile(r"^b'(.*)'$")


def _clean(frame: str) -> str:
    m = _BYTES_REPR.match(frame.strip())
    return m.group(1) if m else frame.strip()


def classify(stack: str) -> tuple[str, str]:
    """-> (bucket, matched_pattern). First rule in table order wins."""
    low = stack.lower()
    for bucket, pats in MATCH_TABLE:
        for p in pats:
            if p in low:
                return bucket, p
    return "other", ""


def parse_folded(path: str, min_us: int):
    out = []
    for line in open(path):
        line = line.rstrip("\n")
        if not line.strip():
            continue
        try:
            stack, val = line.rsplit(" ", 1)
            us = int(float(val))
        except ValueError:
            continue
        if us < min_us:
            continue
        frames = [_clean(f) for f in stack.split(";")]
        out.append((";".join(frames), us))
    return out


def short(stack: str, keep: int = 6) -> str:
    f = stack.split(";")
    if len(f) <= keep:
        return stack
    return f[0] + ";...;" + ";".join(f[-(keep - 1):])


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("folded")
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--out-json")
    ap.add_argument("--out-table")
    ap.add_argument("--label", default="")
    ap.add_argument("--min-us", type=int, default=0)
    a = ap.parse_args()

    entries = parse_folded(a.folded, a.min_us)
    if not entries:
        print("ERROR: no folded stacks parsed from %s" % a.folded, file=sys.stderr)
        return 1

    total_us = sum(us for _, us in entries)
    bucket_us: dict[str, int] = {b: 0 for b in BUCKETS}
    bucket_stacks: dict[str, list] = defaultdict(list)
    bucket_patterns: dict[str, set] = defaultdict(set)
    for stack, us in entries:
        b, pat = classify(stack)
        bucket_us[b] += us
        bucket_stacks[b].append((us, stack))
        if pat:
            bucket_patterns[b].add(pat)

    ranked = sorted(entries, key=lambda kv: -kv[1])[: a.top]
    residue = sorted(bucket_stacks.get("other", []), reverse=True)

    doc = {
        "schema": "ws0-3217.offcpu-attribution/v1",
        "label": a.label,
        "folded_file": a.folded,
        "total_blocked_time_us": total_us,
        "unique_stacks": len(entries),
        "value_unit": "microseconds of off-CPU (blocked) time, summed over all threads",
        "classification_order_note": (
            "first rule in MATCH_TABLE order wins; egress_credit_acquire precedes "
            "mpsc_send_park (ChannelSink::reserve carries both), and mpsc_recv_park "
            "precedes tonic_grpc_socket_write (the receiver is polled by tonic)"),
        "mechanism_frames_excluded_from_matching": list(MECHANISM_ONLY),
        # AC4: every bucket present, explicit zero when absent.
        "buckets": [
            {
                "bucket": b,
                "blocked_time_us": bucket_us[b],
                "pct_of_total_blocked": (bucket_us[b] / total_us * 100.0) if total_us else 0.0,
                "unique_stacks": len(bucket_stacks.get(b, [])),
                "present": bucket_us[b] > 0,
                "absent_note": None if bucket_us[b] > 0 else
                    "EXPLICIT ZERO: no off-CPU stack in this capture matched this bucket",
                "matched_patterns": sorted(bucket_patterns.get(b, [])),
                "match_patterns_defined": dict(MATCH_TABLE).get(b, []),
                "top_stacks": [
                    {"blocked_time_us": us, "stack": short(s)}
                    for us, s in sorted(bucket_stacks.get(b, []), reverse=True)[:3]
                ],
            }
            for b in BUCKETS
        ],
        "ranked_stacks": [
            {"rank": i + 1, "blocked_time_us": us,
             "pct_of_total_blocked": (us / total_us * 100.0) if total_us else 0.0,
             "bucket": classify(s)[0], "matched_pattern": classify(s)[1],
             "stack": s}
            for i, (s, us) in enumerate(ranked)
        ],
        "unclassified": {
            "blocked_time_us": bucket_us["other"],
            "pct_of_total_blocked": (bucket_us["other"] / total_us * 100.0) if total_us else 0.0,
            "unique_stacks": len(residue),
            "note": ("full residue listed so nothing is silently swallowed; extend MATCH_TABLE "
                     "if a real cause is hiding here"),
            "stacks": [{"blocked_time_us": us, "stack": s} for us, s in residue[:50]],
        },
    }

    L = []
    L.append("==== WS0 #3217 OFF-CPU BLOCKED-STACK ATTRIBUTION ====")
    L.append("label: %s   total blocked: %.3f s across %d unique stacks"
             % (a.label, total_us / 1e6, len(entries)))
    L.append("")
    L.append("%-26s %14s %8s %8s" % ("bucket", "blocked (s)", "pct", "stacks"))
    for b in doc["buckets"]:
        L.append("%-26s %14.4f %7.2f%% %8d%s" % (
            b["bucket"], b["blocked_time_us"] / 1e6, b["pct_of_total_blocked"],
            b["unique_stacks"], "   (explicit zero)" if not b["present"] else ""))
    L.append("%-26s %14.4f %7.2f%%" % ("TOTAL", total_us / 1e6, 100.0))
    L.append("")
    L.append("top %d blocked stacks:" % len(doc["ranked_stacks"]))
    L.append("%-5s %13s %7s  %-24s %s" % ("rank", "blocked(s)", "pct", "bucket", "stack"))
    for r in doc["ranked_stacks"]:
        L.append("%-5d %13.4f %6.2f%%  %-24s %s" % (
            r["rank"], r["blocked_time_us"] / 1e6, r["pct_of_total_blocked"],
            r["bucket"], short(r["stack"], 5)))
    if residue:
        L.append("")
        L.append("unclassified residue (%.4f s, %.2f%%) — top %d:" % (
            bucket_us["other"] / 1e6, doc["unclassified"]["pct_of_total_blocked"],
            min(10, len(residue))))
        for us, s in residue[:10]:
            L.append("    %10.4f s  %s" % (us / 1e6, short(s, 5)))
    table = "\n".join(L) + "\n"

    if a.out_json:
        open(a.out_json, "w").write(json.dumps(doc, indent=1) + "\n")
    if a.out_table:
        open(a.out_table, "w").write(table)
    else:
        sys.stdout.write(table)
    return 0


if __name__ == "__main__":
    sys.exit(main())
