#!/usr/bin/env python3
"""Saturating full-table scan of ws0.events over the native CQL protocol.

Head-to-head Arm 1 driver for CQLite issue #3026 (WS0 of umbrella #3023).

Why this exists and not `fullscan.py`: fullscan.py issues its 512 range queries
STRICTLY SEQUENTIALLY, so the server idles for every client round trip. That is
fine for a row-count oracle but it makes wall-clock rows/s a client-latency
measurement, not a server-throughput one -- and once the daemon is pinned to a
single hardware thread we need that thread SATURATED for `rows/s per physical
core` to mean anything. This driver keeps `--inflight` range queries in flight
at all times via execute_async.

Two workload shapes, because they isolate different halves of Cassandra's cost:

  --mode count : `SELECT count(*)` per token range. Server does partition/row
                 read + deserialize + iterate, then returns ONE row. EXCLUDES
                 native-protocol row serialization. This is the closest
                 counterpart to CQLite's bare core scan.
  --mode rows  : `SELECT *` per token range, every row actually pulled to the
                 client and discarded. INCLUDES serialization + protocol
                 framing. The counterpart to CQLite's Flight/Arrow surface.
                 Wall-clock here is often CLIENT bound -- read cycles/row from
                 perf stat on the daemon, not rows/s.

Prints a JSON object on stdout so the caller can post-process; human lines to
stderr. Fails loudly (exit 2) if the row total does not match --expect-rows.
"""
import argparse
import json
import sys
import time
from cassandra.cluster import Cluster
from cassandra.policies import WhiteListRoundRobinPolicy

MIN_TOKEN = -(2 ** 63)
MAX_TOKEN = 2 ** 63 - 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ranges", type=int, default=512)
    ap.add_argument("--inflight", type=int, default=8)
    ap.add_argument("--mode", choices=["count", "rows"], default="count")
    ap.add_argument("--fetch-size", type=int, default=5000)
    ap.add_argument("--expect-rows", type=int, default=3999890)
    ap.add_argument("--label", default="")
    # Client sharding: the single-process Python driver cannot saturate even ONE
    # pinned daemon core on `SELECT *` (measured daemon utilization 63%), which
    # would turn wall rows/s into a client measurement. Sharding the ring across
    # independent processes moves the bottleneck back onto the metered daemon.
    ap.add_argument("--shards", type=int, default=1)
    ap.add_argument("--shard-index", type=int, default=0)
    args = ap.parse_args()

    cluster = Cluster(["127.0.0.1"], protocol_version=5,
                      load_balancing_policy=WhiteListRoundRobinPolicy(["127.0.0.1"]))
    s = cluster.connect("ws0")
    s.default_timeout = 900.0
    s.default_fetch_size = args.fetch_size

    if args.mode == "count":
        sel = "count(*)"
    else:
        sel = ("part_id, seq, event_time, blob_a, blob_b, device_id, "
               "metric_a, metric_b, metric_c, payload, region, status")
    first = s.prepare(f"SELECT {sel} FROM events "
                      "WHERE token(part_id) >= ? AND token(part_id) <= ?")
    rest = s.prepare(f"SELECT {sel} FROM events "
                     "WHERE token(part_id) > ? AND token(part_id) <= ?")
    first.fetch_size = args.fetch_size
    rest.fetch_size = args.fetch_size

    n = args.ranges
    edges = [MIN_TOKEN + (MAX_TOKEN - MIN_TOKEN) * i // n for i in range(n + 1)]
    edges[-1] = MAX_TOKEN

    my = [i for i in range(n) if i % args.shards == args.shard_index]
    total = 0
    pending = []
    t0 = time.monotonic()

    def drain_one():
        nonlocal total
        fut = pending.pop(0)
        rs = fut.result()
        if args.mode == "count":
            total += rs.one()[0]
        else:
            # Walk EVERY page and EVERY row; touch a column so the driver must
            # actually deserialize rather than hand back a lazy handle.
            cnt = 0
            for row in rs:
                cnt += 1
                if row[0] is None:
                    raise SystemExit("null partition key -- unexpected")
            total += cnt

    for i in my:
        stmt = first if i == 0 else rest
        pending.append(s.execute_async(stmt, (edges[i], edges[i + 1]),
                                       timeout=900.0))
        while len(pending) >= args.inflight:
            drain_one()
    while pending:
        drain_one()

    dt = time.monotonic() - t0
    cluster.shutdown()

    out = {
        "label": args.label,
        "engine": "cassandra-5.0.8",
        "surface": f"native-cql/{args.mode}",
        "ranges": n,
        "inflight": args.inflight,
        "fetch_size": args.fetch_size,
        "rows": total,
        "wall_secs": dt,
        "rows_per_sec_wall": total / dt,
        "uncompressed_MB_per_s": total * 692.70 / dt / 1e6,
        "compressed_MB_per_s": total * 195.96 / dt / 1e6,
    }
    print(json.dumps(out))
    print(f"[{args.label}] mode={args.mode} inflight={args.inflight} "
          f"rows={total} secs={dt:.3f} rows/s={total/dt:,.0f}", file=sys.stderr)
    if args.shards > 1:
        sys.exit(0)
    if total != args.expect_rows:
        print(f"FAIL: rows {total} != expected {args.expect_rows}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
