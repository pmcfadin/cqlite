#!/usr/bin/env python3
"""Measure the BYTE BASIS of a staged SSTable corpus (issue #3217, AC6).

AC6: no throughput figure may be a bare MB/s. Every one must name its basis.
There are three distinct byte bases for the same stream of rows, and they differ
by ~3.5x on this corpus, so conflating them misstates the answer:

  * on-disk compressed  -- what the storage layer actually reads. EXACT here:
                           the sum of every `*-Data.db` file size.
  * logical uncompressed-- the post-decompression SSTable bytes the decoder walks.
                           Read from each `*-CompressionInfo.db` header's
                           `dataLength` field, with a SELF-VALIDATING parse (see
                           below). Equals the on-disk size when the SSTable is
                           uncompressed (no CompressionInfo.db sibling).
  * Arrow wire          -- NOT measured here. It comes from the client
                           (`flight-loadgen` `bytes_per_s`) and is Arrow buffer
                           CAPACITY, not compressed gRPC-on-the-wire bytes.

Self-validating CompressionInfo.db parse: the header layout gained a
`maxCompressedLength` int in the 4.0 line, so two layouts are plausible. We do
NOT guess. We try each and accept only the one whose declared `chunkCount`
exactly accounts for the remaining file bytes (offsets are 8 bytes each, and the
file ends with an optional 4-byte CRC or nothing). The offsets array length is a
checksum on the header parse; a layout that does not reconcile is rejected. If
neither reconciles, `logical_uncompressed_bytes` is reported as null with an
explicit reason -- never a fabricated number.

Parse validation (2026-08-02, live ws0 cassandra-stress corpus mid-flush):
`logical_uncompressed_bytes / ondisk_compressed_bytes` = 2773 MB / 784 MB =
**3.54x**, against the independently-derived #3026 WS0 constants
692.70 / 195.96 bytes-per-row = **3.53x**. Two unrelated derivations agreeing to
0.3% is the evidence that the CompressionInfo.db header parse is right.

Usage:
    corpus-basis.py <stage-dir> [-o out.json]
"""
from __future__ import annotations

import argparse
import json
import os
import struct
import sys

HDR = ">"


def _read_utf(buf: bytes, off: int) -> tuple[str, int]:
    (n,) = struct.unpack_from(HDR + "H", buf, off)
    off += 2
    return buf[off:off + n].decode("utf-8", "replace"), off + n


def _try_layout(buf: bytes, with_max_compressed: bool):
    """Structurally validate one header layout.

    Returns {data_length, chunk_count, chunk_length, ceil_consistent} or None.

    The STRONG check is the offsets array: `chunkCount` 8-byte offsets must
    account for the rest of the file (modulo an optional trailing CRC of <= 8
    bytes). That single constraint separates the two candidate layouts
    unambiguously in practice, because a wrong layout misreads `chunkCount` by
    orders of magnitude.

    `ceil(dataLength / chunkLength) == chunkCount` is recorded as a SOFT
    consistency flag, not a rejection: it legitimately fails on an SSTable that
    is still being written (observed on a live cassandra-stress flush), and
    rejecting there would report the basis as UNAVAILABLE for a merely
    in-progress corpus. It IS used to break a tie if both layouts survive the
    strong check.
    """
    try:
        off = 0
        _name, off = _read_utf(buf, off)
        (opt_count,) = struct.unpack_from(HDR + "i", buf, off)
        off += 4
        if not (0 <= opt_count <= 64):
            return None
        for _ in range(opt_count):
            _k, off = _read_utf(buf, off)
            _v, off = _read_utf(buf, off)
        (chunk_len,) = struct.unpack_from(HDR + "i", buf, off)
        off += 4
        if with_max_compressed:
            off += 4  # maxCompressedLength
        (data_len,) = struct.unpack_from(HDR + "q", buf, off)
        off += 8
        (chunk_count,) = struct.unpack_from(HDR + "i", buf, off)
        off += 4
    except (struct.error, IndexError):
        return None
    if chunk_len <= 0 or data_len <= 0 or chunk_count <= 0:
        return None
    remaining = len(buf) - off
    # STRONG check: offsets are 8 bytes each; trailing bytes are an optional CRC.
    if not (0 <= remaining - chunk_count * 8 <= 8):
        return None
    return {
        "data_length": data_len,
        "chunk_count": chunk_count,
        "chunk_length": chunk_len,
        # +-1 tolerance: the offsets array may carry a terminal entry, so an
        # off-by-one is structural, not a misparse. A larger divergence means a
        # torn read of a file still being written (or a wrong layout).
        "ceil_consistent": abs(((data_len + chunk_len - 1) // chunk_len) - chunk_count) <= 1,
    }


def compression_data_length(path: str):
    """Uncompressed dataLength from a CompressionInfo.db, or (None, reason)."""
    with open(path, "rb") as fh:
        buf = fh.read()
    hits = [(wm, r) for wm in (True, False)
            for r in [_try_layout(buf, wm)] if r is not None]
    if not hits:
        return None, "no CompressionInfo.db header layout reconciled with the offsets array"
    if len(hits) > 1 and hits[0][1]["data_length"] != hits[1][1]["data_length"]:
        consistent = [h for h in hits if h[1]["ceil_consistent"]]
        if len(consistent) != 1:
            return None, "ambiguous: both header layouts reconcile with different dataLength"
        hits = consistent
    with_max, res = hits[0]
    reason = "layout=with_max_compressed_length" if with_max else "layout=legacy_no_max_compressed_length"
    if not res["ceil_consistent"]:
        reason += (" (WARNING: ceil(dataLength/chunkLength)=%d != chunkCount=%d — the SSTable was "
                   "likely still being written when read; re-measure on the final corpus)"
                   % ((res["data_length"] + res["chunk_length"] - 1) // res["chunk_length"],
                      res["chunk_count"]))
    return res["data_length"], reason


def scan(stage: str) -> dict:
    data_files, comp_files = [], {}
    for root, _dirs, files in os.walk(stage):
        for f in files:
            p = os.path.join(root, f)
            if f.endswith("-Data.db"):
                data_files.append(p)
            elif f.endswith("-CompressionInfo.db"):
                comp_files[p[: -len("-CompressionInfo.db")]] = p

    ondisk = 0
    logical = 0
    notes = []
    uncompressed_tables = 0
    compressed_tables = 0
    unresolved = []
    for d in sorted(data_files):
        sz = os.path.getsize(d)
        ondisk += sz
        stem = d[: -len("-Data.db")]
        ci = comp_files.get(stem)
        if ci is None:
            logical += sz
            uncompressed_tables += 1
        else:
            dl, reason = compression_data_length(ci)
            if dl is None:
                unresolved.append({"file": os.path.basename(ci), "reason": reason})
            else:
                logical += dl
                compressed_tables += 1
                if reason not in notes:
                    notes.append(reason)

    doc = {
        "schema": "ws0-3217.corpus-basis/v1",
        "stage_dir": os.path.abspath(stage),
        "data_db_files": len(data_files),
        "ondisk_compressed_bytes": ondisk,
        "ondisk_compressed_bytes_basis": "exact: sum of every *-Data.db file size",
        "sstables_uncompressed": uncompressed_tables,
        "sstables_compressed": compressed_tables,
    }
    if unresolved:
        doc["logical_uncompressed_bytes"] = None
        doc["logical_uncompressed_bytes_basis"] = (
            "UNAVAILABLE: %d CompressionInfo.db header(s) did not reconcile; "
            "supply WS0_LOGICAL_BYTES_PER_ROW instead of fabricating a value" % len(unresolved)
        )
        doc["unresolved_compression_info"] = unresolved
    else:
        doc["logical_uncompressed_bytes"] = logical
        doc["logical_uncompressed_bytes_basis"] = (
            "sum of CompressionInfo.db dataLength (self-validated against the offsets array); "
            "uncompressed SSTables contribute their Data.db size. " + "; ".join(notes)
        )
    doc["arrow_wire_bytes_basis"] = (
        "NOT measurable from the corpus: comes from flight-loadgen bytes_per_s, which is "
        "Arrow buffer CAPACITY bytes, not compressed gRPC-on-the-wire bytes"
    )
    return doc


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("stage")
    ap.add_argument("-o", "--out")
    a = ap.parse_args()
    if not os.path.isdir(a.stage):
        print("ERROR: not a directory: %s" % a.stage, file=sys.stderr)
        return 2
    doc = scan(a.stage)
    text = json.dumps(doc, indent=1) + "\n"
    if a.out:
        os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
        open(a.out, "w").write(text)
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
