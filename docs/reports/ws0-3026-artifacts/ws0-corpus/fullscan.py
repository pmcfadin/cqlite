#!/usr/bin/env python3
"""Full-table scan of ws0.events by token range (WS0 / CQLite #3026).

Why token ranges and not `SELECT count(*) FROM ws0.events`: the single-statement
aggregate over 4M rows exceeds the server-side range read timeout on this node and
fails. Splitting the ring keeps each aggregate small, still touches every partition
exactly once, and keeps client-side traffic tiny (one row per range) so the scan is
server-I/O-bound rather than loopback-bound.

Usage: fullscan.py [n_ranges]
Prints: total rows, wall time, and effective scan throughput.
"""
import sys
import time
from cassandra.cluster import Cluster
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement

MIN_TOKEN = -(2 ** 63)
MAX_TOKEN = 2 ** 63 - 1


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 512
    cluster = Cluster(["127.0.0.1"], protocol_version=5,
                      load_balancing_policy=WhiteListRoundRobinPolicy(["127.0.0.1"]))
    s = cluster.connect("ws0")
    s.default_timeout = 600.0

    edges = [MIN_TOKEN + (MAX_TOKEN - MIN_TOKEN) * i // n for i in range(n + 1)]
    edges[-1] = MAX_TOKEN

    first = s.prepare("SELECT count(*) FROM events "
                      "WHERE token(part_id) >= ? AND token(part_id) <= ?")
    rest = s.prepare("SELECT count(*) FROM events "
                     "WHERE token(part_id) > ? AND token(part_id) <= ?")

    total = 0
    t0 = time.monotonic()
    for i in range(n):
        stmt = first if i == 0 else rest
        total += s.execute(stmt, (edges[i], edges[i + 1]), timeout=600.0).one()[0]
    dt = time.monotonic() - t0
    cluster.shutdown()

    print(f"SCAN ranges           : {n}")
    print(f"SCAN rows counted     : {total}")
    print(f"SCAN wall seconds     : {dt:.3f}")
    print(f"SCAN rows/s           : {total/dt:,.0f}")
    print(f"SCAN uncompressed MB/s: {total*692.70/dt/1e6:.1f}  (at measured 692.70 B/row)")
    print(f"SCAN compressed  MB/s : {total*195.96/dt/1e6:.1f}  (at measured 195.96 B/row)")


if __name__ == "__main__":
    main()
