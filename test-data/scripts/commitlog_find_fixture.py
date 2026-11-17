#!/usr/bin/env python3
"""Derive the `cqlite find` CommitLog fixture ground truth (#4205).

Trims a raw CommitLog segment exactly like commitlog_fixture_derivation.derive_fixtures, then
walks it with that module's INDEPENDENT frame walker (`iter_valid_frames`) to record, for every
mutation record that targets the users table, its byte position. Insert i of the seed list is
matched to the i-th such record (single-update INSERTs, int partition key) and the key is
cross-checked, so a position can never be attributed to the wrong insert.

Usage: commitlog_find_fixture.py derive <raw> <find_dir> <segment_name> <table_id> <table_dir>
                                 <image> <id:name:age:timestamp:flushed>...
"""
import json
import struct
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import commitlog_fixture_derivation as base  # noqa: E402


def derive(argv):
    raw_path, find_dir, seg_name, table_id, table_dir, image, *seeds = argv
    raw = Path(raw_path).read_bytes()
    last = len(raw)
    while last > 0 and raw[last - 1] == 0:
        last -= 1
    if last == 0:
        raise base.FixtureFormatError("raw segment contains no nonzero bytes")
    clean = raw[: min(len(raw), last + 64)]
    tid = uuid.UUID(table_id).bytes

    frames = []
    for start, _bs, _be, _fe, body in base.iter_valid_frames(clean):
        if len(body) >= 22 and body[0] == 1 and body[1:17] == tid:
            if body[17] != 4:
                raise base.FixtureFormatError(f"record at {start}: unexpected pk length")
            frames.append((start, struct.unpack_from(">i", body, 18)[0]))

    inserts = []
    for s in seeds:
        i, name, age, ts, flushed = s.split(":")
        inserts.append({"id": int(i), "name": name, "age": int(age),
                        "timestamp": int(ts), "flushed": flushed == "1"})
    if len(frames) != len(inserts):
        raise base.FixtureFormatError(
            f"{len(frames)} users records in the segment, {len(inserts)} inserts expected")
    for ins, (start, key) in zip(inserts, frames):
        if ins["id"] != key:
            raise base.FixtureFormatError(f"record at {start} has key {key}, expected {ins['id']}")
        ins["position"] = start

    version, segment_id, _, _ = base._descriptor(clean)
    out = Path(find_dir)
    (out / "commitlog" / seg_name).write_bytes(clean)
    gt = {
        "cassandra_image": image,
        "keyspace": "commitlog_test",
        "table": "users",
        "table_id": table_id,
        "table_dir": table_dir,
        "schema": "CREATE TABLE commitlog_test.users (id int PRIMARY KEY, name text, age int)",
        "segment": {"file": seg_name, "id": segment_id, "version": version},
        "inserts": inserts,
    }
    (out / "commitlog-find-ground-truth.json").write_text(json.dumps(gt, indent=2) + "\n")
    print(f"[find-fixture] {len(inserts)} inserts, positions={[i['position'] for i in inserts]}")


if __name__ == "__main__":
    if len(sys.argv) > 2 and sys.argv[1] == "derive":
        derive(sys.argv[2:])
    else:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
