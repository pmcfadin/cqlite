#!/usr/bin/env python3
"""A stub `docker` for scripts/tests/test_gen_perf_corpus_bti.sh (issue #3234).

WHY: the BTI perf-corpus generator's row-count cross-checks and the manifest
writer's happy path only run when a Cassandra container answers -- i.e. never, in
any hermetic test. Both were therefore asserted by NOTHING: the manifest writer's
`sstable_record` / `aggregate_row_plan` / relative `sstable_dir` /
`meets_8mib_read_plane_floor` had no coverage at all, and the two cross-checks
("COPY imported N, the CSV held M" and "Statistics.db totalRows == sstabledump
rows") had never been observed to FIRE.

This stub stands in for the `docker` binary (`DOCKER=<this file>` /
`--docker <this file>`), so the generator runs end to end -- real row driver, real
CSVs, real fail-closed asserts, real manifest -- with no container, no network and
no sudo. Everything Cassandra would have produced is FABRICATED here:

  * `nodetool flush` materializes one more `da-<gen>-bti-*` generation (all 8
    components, incl. a genuinely parseable CompressionInfo.db header and a sparse
    Data.db above the 8 MiB read-plane floor) under the bind-mounted data dir;
  * `cqlsh ... COPY` reports the row count of the CSV that was last `docker cp`'d;
  * `sstabledump -l` and `sstablemetadata` report the counts the ROW PLAN records
    for that generation's chunk, so the generator's two cross-checks compare two
    independent readbacks exactly as they do in production.

FAULT INJECTION (this is the point -- an assert nobody has seen fail is a claim,
not a test), via env:
  STUB_IMPORT_SHORT=1    COPY reports one row fewer than the CSV held
  STUB_META_SHORT=1      sstablemetadata reports one row fewer than sstabledump
  STUB_NO_HISTOGRAM=1    sstablemetadata omits the "Partition Size:" histogram
  STUB_META_EXIT=<n>     sstablemetadata prints a COMPLETE, valid-looking metadata
                         block (real totalRows, real "Partition Size:" histogram) and
                         then EXITS <n>: the "plausible output, nonzero status" case
                         that must never be parsed into a manifest (roborev #3234 M1)
  STUB_ROWS_DELTA=<n>    add <n> to the rows sstablemetadata reports (manifest
                         row-count cross-check)
  STUB_PARTITIONS_DELTA=<n>  add <n> to the partition histogram total (manifest
                         partition-count cross-check)
  STUB_NO_SSTABLE_DIR=1  `ls -d /var/lib/cassandra/data/<ks>/<tbl>-*` finds NOTHING and
                         exits 1: publish() then dies while LOCATING the container's
                         table directory, which is the failure the stale-provenance
                         window (roborev #3234 F3) hinged on -- it fires after the
                         schema is captured and before any SSTable is copied
  STUB_SCHEMA_MARK=<s>   append `-- mark: <s>` to the DESCRIBE KEYSPACE output, so two
                         runs over the same corpus root produce TEXTUALLY DISTINCT
                         schema captures and "whose schema got published?" is decidable

This is a TEST DOUBLE. It is not authority for any on-disk format: it fabricates
only the metadata TEXT the generator parses, never SSTable content.
"""

from __future__ import annotations

import json
import os
import struct
import subprocess
import sys

STATE = os.environ.get("STUB_STATE") or "/tmp/stub-docker-state"
KS = os.environ.get("STUB_KS", "perf_bti_stub")
TBL = os.environ.get("STUB_TBL", "wide_multiclustering")
YAML = os.environ.get("STUB_YAML", "")
PLAN = os.environ.get("STUB_PLAN", "")
DATA_MIB = int(os.environ.get("STUB_DATA_MIB", "9"))
TABLE_UUID = "a1b2c3d40000000000000000000000ff"
assert len(TABLE_UUID) == 32
CONTAINER_DATA = "/var/lib/cassandra"


def _flag(name: str) -> bool:
    return os.environ.get(name, "") not in ("", "0")


def _delta(name: str) -> int:
    return int(os.environ.get(name, "0") or "0")


def state_path(name: str) -> str:
    os.makedirs(STATE, exist_ok=True)
    return os.path.join(STATE, name)


def put(name: str, value: str) -> None:
    with open(state_path(name), "w") as fh:
        fh.write(value)


def get(name: str, default: str = "") -> str:
    try:
        with open(state_path(name)) as fh:
            return fh.read().strip()
    except OSError:
        return default


def host_table_dir() -> str:
    return os.path.join(get("datadir"), "data", KS, f"{TBL}-{TABLE_UUID}")


def plan_record(chunk: int) -> dict:
    """The row driver's OBSERVED plan record for `chunk` (never a guess)."""
    if not PLAN or not os.path.exists(PLAN):
        sys.stderr.write(f"stub-docker: no row plan at {PLAN!r}\n")
        sys.exit(90)
    for line in open(PLAN):
        line = line.strip()
        if not line:
            continue
        rec = json.loads(line)
        if rec["chunk"] == chunk:
            return rec
    sys.stderr.write(f"stub-docker: row plan has no record for chunk {chunk}\n")
    sys.exit(91)


def compression_info(chunk_count: int, data_length: int) -> bytes:
    """A CompressionInfo.db header read-compression-info.py genuinely parses."""

    def utf(s: str) -> bytes:
        b = s.encode("utf-8")
        return struct.pack(">H", len(b)) + b

    out = utf("LZ4Compressor")
    out += struct.pack(">i", 1) + utf("chunk_length_in_kb") + utf("16")
    out += struct.pack(">i", 16384)          # chunk length
    out += struct.pack(">i", 2147483647)     # max compressed length
    out += struct.pack(">q", data_length)    # uncompressed data length
    out += struct.pack(">i", chunk_count)
    out += b"".join(struct.pack(">q", i * 16000) for i in range(chunk_count))
    return out


def write_generation() -> None:
    """`nodetool flush`: one more SSTable generation, as a flush would leave it."""
    d = host_table_dir()
    os.makedirs(d, exist_ok=True)
    gen = 1 + len([f for f in os.listdir(d) if f.endswith("-Data.db")])
    stem = f"da-{gen}-bti"
    data = os.path.join(d, f"{stem}-Data.db")
    with open(data, "wb") as fh:                    # sparse: the size assert reads it
        fh.truncate(DATA_MIB * 1024 * 1024)
    with open(os.path.join(d, f"{stem}-Rows.db"), "wb") as fh:
        fh.truncate(4096)                           # non-empty row-index trie
    for comp in ("Partitions.db", "Statistics.db", "Filter.db"):
        with open(os.path.join(d, f"{stem}-{comp}"), "wb") as fh:
            fh.truncate(512)
    with open(os.path.join(d, f"{stem}-CompressionInfo.db"), "wb") as fh:
        fh.write(compression_info(64, DATA_MIB * 2 * 1024 * 1024))
    with open(os.path.join(d, f"{stem}-Digest.crc32"), "w") as fh:
        fh.write("1234567890\n")
    with open(os.path.join(d, f"{stem}-TOC.txt"), "w") as fh:
        fh.write("".join(f"{c}\n" for c in (
            "Data.db", "Partitions.db", "Rows.db", "Statistics.db",
            "CompressionInfo.db", "Filter.db", "Digest.crc32", "TOC.txt",
        )))


def gen_of(path: str) -> int:
    base = os.path.basename(path)
    return int(base.split("-")[1])


def sstabledump(path: str) -> int:
    """`sstabledump -l`: one JSON line per partition, `rows` summing to the plan."""
    rec = plan_record(gen_of(path) - 1)
    rows, parts = rec["rows"], rec["partitions"]
    per, extra = divmod(rows, parts)
    for p in range(parts):
        n = per + (1 if p < extra else 0)
        print(json.dumps({
            "partition": {"key": [str(rec["pk_min"] + p)], "position": p * 128},
            "rows": [
                {"type": "row", "position": i, "clustering": ["alpha", i],
                 "cells": [{"name": "payload", "value": "deadbeef"}]}
                for i in range(n)
            ],
        }))
    return 0


def sstablemetadata(path: str) -> int:
    """`sstablemetadata`: the Statistics.db readback the cross-checks compare."""
    rec = plan_record(gen_of(path) - 1)
    rows = rec["rows"] + _delta("STUB_ROWS_DELTA") - (1 if _flag("STUB_META_SHORT") else 0)
    parts = rec["partitions"] + _delta("STUB_PARTITIONS_DELTA")
    print(f"SSTable: {path}")
    print("Partitioner: org.apache.cassandra.dht.Murmur3Partitioner")
    print("SSTable Level: 0")
    print(f"totalRows: {rows}")
    print(f"totalColumnsSet: {rows}")
    print("Compression ratio: 0.4127")
    print("Estimated droppable tombstones: 0.0")
    if not _flag("STUB_NO_HISTOGRAM"):
        a = parts // 2
        b = parts - a
        print("Partition Size:")
        print("   size (bytes) | count  %   histogram ")
        print(f"   1109         | {a}     50  OOOOOOOOOOOOOOOO")
        print(f"   1331         | {b}     50  OOOOOOOOOOOOOOOO")
    print("Percentiles")
    print("   50th      1109.0")
    # Everything above is complete and plausible; only the STATUS says otherwise. This
    # is the shape a reader that parses stdout without checking the exit code cannot
    # distinguish from success (roborev #3234 M1).
    return _delta("STUB_META_EXIT")


DESCRIBE = """
CREATE KEYSPACE {ks} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}  AND durable_writes = false;

CREATE TABLE {ks}.{tbl} (
    pk int,
    bucket text,
    seq int,
    payload text,
    PRIMARY KEY (pk, bucket, seq)
) WITH CLUSTERING ORDER BY (bucket ASC, seq ASC)
    AND compaction = {{'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}}
    AND compression = {{'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}};
"""


def do_exec(argv: list[str]) -> int:
    argv = argv[1:]                                  # drop the container name
    while argv and argv[0].startswith("-"):
        argv.pop(0)
    if not argv:
        return 0
    tool, rest = argv[0], argv[1:]
    if tool == "cqlsh":
        stmt = rest[rest.index("-e") + 1] if "-e" in rest else ""
        if stmt.startswith("SELECT cluster_name"):
            print("stub_cluster")
            return 0
        if stmt.startswith("DESCRIBE KEYSPACE"):
            print(DESCRIBE.format(ks=KS, tbl=TBL))
            mark = os.environ.get("STUB_SCHEMA_MARK", "")
            if mark:
                print(f"-- mark: {mark}")
            return 0
        if stmt.startswith("COPY "):
            n = int(get("csv_rows", "0"))
            if _flag("STUB_IMPORT_SHORT"):
                n -= 1
            print(f"Processed: {n} rows; Rate: 1000 rows/s")
            print(f"{n} rows imported from 1 files in 0.5 seconds (0 skipped).")
            return 0
        return 0
    if tool == "nodetool":
        if rest and rest[0] == "flush":
            write_generation()
        return 0
    if tool == "bash":
        cmd = rest[-1]
        if "cassandra.yaml" in cmd:
            if not YAML:
                sys.stderr.write("stub-docker: STUB_YAML is unset\n")
                return 92
            # Run the generator's REAL yaml snippet against a local copy.
            return subprocess.run(
                ["bash", "-c", cmd.replace("/etc/cassandra/cassandra.yaml", YAML)]
            ).returncode
        if cmd.startswith("ls -d "):
            if _flag("STUB_NO_SSTABLE_DIR"):
                # What the real `ls -d <glob>` does when the glob matches nothing:
                # no stdout, a diagnostic on stderr, nonzero status.
                sys.stderr.write(
                    f"ls: cannot access '{CONTAINER_DATA}/data/{KS}/{TBL}-*': "
                    "No such file or directory\n"
                )
                return 1
            print(f"{CONTAINER_DATA}/data/{KS}/{TBL}-{TABLE_UUID}")
            return 0
        if "sstabledump" in cmd:
            return sstabledump(cmd.split("'")[1])
        if "sstablemetadata" in cmd:
            return sstablemetadata(cmd.split("'")[1])
        return 0
    return 0


def do_run(argv: list[str]) -> int:
    mounts = [argv[i + 1] for i, a in enumerate(argv) if a == "-v"]
    if "--entrypoint" in argv:                       # the manifest writer's readback
        target = argv[-1]
        host = next((m.split(":")[0] for m in mounts if ":/data" in m), "")
        return sstablemetadata(os.path.join(host, os.path.basename(target)))
    for m in mounts:                                 # `run -d`: the node itself
        parts = m.split(":")
        if len(parts) >= 2 and parts[1] == CONTAINER_DATA:
            put("datadir", parts[0])
    print("stubcontainerid")
    return 0


def main(argv: list[str]) -> int:
    args = argv[1:]
    i = 0
    while i < len(args) and args[i].startswith("-"):  # global flags
        i += 1
    if i >= len(args):
        return 0
    cmd, rest = args[i], args[i + 1:]
    if cmd == "version":
        print("stub-docker (test double for issue #3234)")
        return 0
    if cmd in ("rm", "restart", "logs", "stop", "start", "kill", "pull"):
        return 0
    if cmd == "cp":
        src = rest[0]
        if os.path.exists(src):
            with open(src, "rb") as fh:
                put("csv_rows", str(sum(1 for _ in fh)))
        return 0
    if cmd == "run":
        return do_run(rest)
    if cmd == "exec":
        return do_exec(rest)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
