#!/usr/bin/env python3
"""Read the AUTHORITATIVE compression parameters out of a Cassandra
``*-CompressionInfo.db`` component (issue #3068 corpus verification).

The chunk length used by a written SSTable is recorded in CompressionInfo.db,
NOT inferred from the table DDL (a schema change after the write, or a
Cassandra-side clamp, would make the DDL a lie). Per
``org.apache.cassandra.io.compress.CompressionMetadata.Writer.writeHeader()``
(and docs/sstables-definitive-guide/chapters/09-compressioninfo-and-chunking.md,
"CompressionInfo.db Format (serialization exactness)") the layout is:

    UTF   compressor class name                   (2-byte length + modified-UTF8)
    int   option count
      UTF key / UTF value  (option count times)
    int   chunk length (uncompressed bytes per chunk)
    int   max compressed length                   (>= "na" only; Integer.MAX_VALUE
                                                   when min_compress_ratio = 0)
    long  data length (total uncompressed bytes)
    int   chunk count
    long  chunk offset  (chunk count times)

All values are Java big-endian, all fixed-width (the chunk map is a flat u64
array, NOT varint pairs). The parse is self-verifying: the chunk-offset array
must exactly fill the remainder of the file, which pins every preceding field.

Usage:  read-compression-info.py <path-to-CompressionInfo.db> [--json]
"""

from __future__ import annotations

import json
import struct
import sys


class Reader:
    def __init__(self, buf: bytes) -> None:
        self.buf = buf
        self.pos = 0

    def take(self, n: int) -> bytes:
        if self.pos + n > len(self.buf):
            raise ValueError(
                f"truncated CompressionInfo.db: want {n} bytes at {self.pos}, "
                f"file is {len(self.buf)} bytes"
            )
        out = self.buf[self.pos : self.pos + n]
        self.pos += n
        return out

    def u16(self) -> int:
        return struct.unpack(">H", self.take(2))[0]

    def i32(self) -> int:
        return struct.unpack(">i", self.take(4))[0]

    def i64(self) -> int:
        return struct.unpack(">q", self.take(8))[0]

    def utf(self) -> str:
        return self.take(self.u16()).decode("utf-8")


def parse(path: str) -> dict:
    with open(path, "rb") as fh:
        r = Reader(fh.read())

    compressor = r.utf()
    options = {}
    for _ in range(r.i32()):
        key = r.utf()
        options[key] = r.utf()
    chunk_length = r.i32()
    # Present for every format in scope (>= "na"); Integer.MAX_VALUE means
    # min_compress_ratio = 0, i.e. chunks are stored compressed unconditionally.
    max_compressed_length = r.i32()
    data_length = r.i64()
    chunk_count = r.i32()

    # Offsets are 8 bytes each and must exactly fill the remainder.
    offsets_bytes = len(r.buf) - r.pos
    expected = chunk_count * 8
    if offsets_bytes != expected:
        raise ValueError(
            f"chunk offset array size mismatch: header says {chunk_count} chunks "
            f"({expected} bytes) but {offsets_bytes} bytes remain"
        )
    first_offset = struct.unpack(">q", r.take(8))[0] if chunk_count else None

    return {
        "path": path,
        "compressor": compressor,
        "options": options,
        "chunk_length_bytes": chunk_length,
        "chunk_length_kb": chunk_length / 1024.0,
        "max_compressed_length": max_compressed_length,
        "uncompressed_data_length_bytes": data_length,
        "chunk_count": chunk_count,
        "first_chunk_offset": first_offset,
    }


def main(argv: list[str]) -> int:
    args = [a for a in argv[1:] if not a.startswith("--")]
    if len(args) != 1:
        print(__doc__, file=sys.stderr)
        return 2
    info = parse(args[0])
    if "--json" in argv[1:]:
        print(json.dumps(info, indent=2, sort_keys=True))
    else:
        print(f"compressor             : {info['compressor']}")
        print(f"options                : {info['options']}")
        print(
            f"chunk_length           : {info['chunk_length_bytes']} bytes "
            f"({info['chunk_length_kb']:g} KiB)"
        )
        print(f"uncompressed data len  : {info['uncompressed_data_length_bytes']} bytes")
        print(f"chunk count            : {info['chunk_count']}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
