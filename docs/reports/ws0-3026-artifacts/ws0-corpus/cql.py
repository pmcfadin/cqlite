#!/usr/bin/env python3
"""Minimal CQL runner for WS0 (cqlsh is unusable: needs Python 3.6-3.11, box has 3.12).

Usage:  cql.py "<statement>" ["<statement>" ...]
        cql.py -f file.cql
Prints rows as TSV.
"""
import sys
from cassandra.cluster import Cluster
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

TIMEOUT = 1200.0


def run(stmts):
    cluster = Cluster(["127.0.0.1"], protocol_version=5,
                      load_balancing_policy=WhiteListRoundRobinPolicy(["127.0.0.1"]))
    s = cluster.connect()
    s.default_timeout = TIMEOUT
    for st in stmts:
        st = st.strip()
        if not st:
            continue
        print(f"--- {st[:160]}")
        rs = s.execute(SimpleStatement(st, consistency_level=ConsistencyLevel.ONE,
                                       fetch_size=1000), timeout=TIMEOUT)
        if rs.column_names:
            print("\t".join(rs.column_names))
            n = 0
            for r in rs:
                print("\t".join(str(x) for x in r))
                n += 1
                if n >= 2000:
                    print("... truncated")
                    break
    cluster.shutdown()


if __name__ == "__main__":
    args = sys.argv[1:]
    if args and args[0] == "-f":
        body = open(args[1]).read()
        stmts = [x for x in body.split(";") if x.strip()]
    else:
        stmts = args
    run(stmts)
