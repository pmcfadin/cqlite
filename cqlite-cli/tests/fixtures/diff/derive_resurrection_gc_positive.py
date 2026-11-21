#!/usr/bin/env python3
"""Derive the expected `cqlite diff` report for test_tomb.resurrection_gc_positive (issue #4201).

INDEPENDENT of CQLite: the input is Cassandra's own `sstabledump` output for the two generations
(`nb-1-big-Data.db.jsonl`, `nb-2-big-Data.db.jsonl`, datasets-v4, Cassandra 5.0.8) and the output
follows from Cassandra's rules, never from `cqlite diff`:

  * a cell is deleted by a tombstone iff cell.timestamp <= markedForDeleteAt
    (cassandra-5.0.8 db/DeletionTime.java:173-175);
  * a tombstone is purgeable iff localDeletionTime < now - gc_grace_seconds, strict
    (db/ColumnFamilyStore.java:1967-1970, db/partitions/PurgeFunction.java:41-42).

Side A = both generations, side B = generation 1 only (issue #4201 AC 1).

usage: derive_resurrection_gc_positive.py <table-dir> <now-epoch-seconds> > expected.json
"""
import datetime
import json
import pathlib
import sys

GC_GRACE = 864000  # test-data/schemas/tombstone-parity.cql table 5


def micros(stamp):
    """RFC3339 (sstabledump's spelling) -> epoch microseconds."""
    date = datetime.datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    return int(date.timestamp()) * 1_000_000 + date.microsecond


def fmt_micros(value):
    date = datetime.datetime.fromtimestamp(value // 1_000_000, tz=datetime.timezone.utc)
    return date.strftime("%Y-%m-%dT%H:%M:%SZ")


def secs(stamp):
    return micros(stamp) // 1_000_000


def load(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def main():
    table_dir, now = pathlib.Path(sys.argv[1]), int(sys.argv[2])
    gen1 = load(table_dir / "nb-1-big-Data.db.jsonl")
    gen2 = load(table_dir / "nb-2-big-Data.db.jsonl")
    gc_before = now - GC_GRACE

    # Side B = generation 1: every live cell. key (pk, ck, column) -> (value, ts_micros)
    b = {}
    order = []  # partition order = Data.db order = token order (Cassandra writes it so)
    for part in gen1:
        pk = part["partition"]["key"][0]
        order.append(pk)
        for row in part["rows"]:
            ts = micros(row["liveness_info"]["tstamp"])
            for cell in row["cells"]:
                b[(pk, str(row["clustering"][0]), cell["name"])] = (cell["value"], ts)

    # Generation 2's tombstones: (kind, pk, ck, column, marked_micros, ldt_secs)
    tombs = []
    for part in gen2:
        pk = part["partition"]["key"][0]
        info = part["partition"].get("deletion_info")
        if info:
            tombs.append(("partition", pk, None, None, micros(info["marked_deleted"]),
                          secs(info["local_delete_time"])))
        for row in part["rows"]:
            ck = str(row["clustering"][0])
            info = row.get("deletion_info")
            if info:
                tombs.append(("row", pk, ck, None, micros(info["marked_deleted"]),
                              secs(info["local_delete_time"])))
            for cell in row["cells"]:
                if "deletion_info" in cell:
                    tombs.append(("cell", pk, ck, cell["name"], micros(cell["tstamp"]),
                                  secs(cell["deletion_info"]["local_delete_time"])))

    def covers(t, key):
        kind, pk, ck, column, marked, _ = t
        if key[0] != pk or b[key][1] > marked:  # DeletionTime.deletes: ts <= markedForDeleteAt
            return False
        if kind == "partition":
            return True
        if kind == "row":
            return key[1] == ck
        return key[1] == ck and key[2] == column

    # Side A = generation 1 minus everything a generation-2 tombstone deletes.
    a = {key: value for key, value in b.items() if not any(covers(t, key) for t in tombs)}

    partitions = []
    summary = dict(partitions_compared=0, equal=0, only_a=0, only_b=0, newer_in_a=0, newer_in_b=0,
                   tombstones_only_a=0, tombstones_only_b=0,
                   resurrection_risk_before_gc_grace=0, resurrection_risk_past_gc_grace=0)
    kind_rank = {"partition": 0, "range": 1, "row": 2, "collection": 3, "cell": 4}
    for pk in order:
        cells = []
        for key in sorted((k for k in b if k[0] == pk), key=lambda k: (int(k[1]), k[2])):
            side_b = {"value": b[key][0], "writetime": fmt_micros(b[key][1])}
            if key in a:
                cells.append({"clustering": key[1], "column": key[2], "status": "equal",
                              "a": dict(side_b), "b": side_b})
                summary["equal"] += 1
            else:
                cells.append({"clustering": key[1], "column": key[2], "status": "only-b",
                              "a": None, "b": side_b})
                summary["only_b"] += 1
        tombstones = []
        mine = [t for t in tombs if t[1] == pk]
        for t in sorted(mine, key=lambda t: (kind_rank[t[0]], int(t[2] or 0), t[3] or "")):
            kind, _, ck, column, marked, ldt = t
            shadowed = sorted((k for k in b if covers(t, k)), key=lambda k: (int(k[1]), k[2]))
            risk = {"status": "yes" if shadowed else "no"}
            if shadowed:
                phase = "past-gc-grace" if ldt < gc_before else "before-gc-grace"
                risk["phase"] = phase
                risk["shadowed_cells"] = [{"clustering": k[1], "column": k[2], "side": "b"} for k in shadowed]
                summary["resurrection_risk_" + phase.replace("-", "_")] += 1
            entry = {"kind": kind, "clustering": ck, "side": "a",
                     "deletion_time": fmt_micros(marked),
                     "local_deletion_time": datetime.datetime.fromtimestamp(
                         ldt, tz=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                     "resurrection_risk": risk}
            if column:
                entry["column"] = column
            tombstones.append(entry)
            summary["tombstones_only_a"] += 1
        partitions.append({"partition_key": pk, "cells": cells, "tombstones": tombstones})
        summary["partitions_compared"] += 1

    report = {
        "now": datetime.datetime.fromtimestamp(now, tz=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "table": "test_tomb.resurrection_gc_positive",
        "gc_grace_seconds": GC_GRACE,
        "side_a": {"dir": "<a>", "generations": ["nb-1-big", "nb-2-big"]},
        "side_b": {"dir": "<b>", "generations": ["nb-1-big"]},
        "partitions": partitions,
        "summary": summary,
    }
    json.dump(report, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")


main()
