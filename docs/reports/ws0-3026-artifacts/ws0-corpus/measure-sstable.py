#!/usr/bin/env python3
"""Measure an SSTable's compression geometry directly from CompressionInfo.db.

Format (org.apache.cassandra.io.compress.CompressionMetadata, writeHeader at :375):
    UTF   compressorName
    int   optionCount ; then optionCount * (UTF key, UTF value)
    int   chunkLength
    int   maxCompressedLength      (present for versions with hasMaxCompressedLength(); na+/nb yes)
    long  dataLength               <-- UNCOMPRESSED length of Data.db
    int   chunkCount
    long[chunkCount] offsets

Usage: measure-sstable.py <path-to-*-CompressionInfo.db> [--no-maxcomp]
"""
import struct
import sys
import os


def read_utf(f):
    (n,) = struct.unpack(">H", f.read(2))
    return f.read(n).decode("utf-8")


def main():
    p = sys.argv[1]
    has_maxcomp = "--no-maxcomp" not in sys.argv
    data_db = p.replace("-CompressionInfo.db", "-Data.db")
    with open(p, "rb") as f:
        compressor = read_utf(f)
        (nopt,) = struct.unpack(">i", f.read(4))
        opts = {}
        for _ in range(nopt):
            k = read_utf(f)
            v = read_utf(f)
            opts[k] = v
        (chunk_length,) = struct.unpack(">i", f.read(4))
        max_comp = None
        if has_maxcomp:
            (max_comp,) = struct.unpack(">i", f.read(4))
        (data_length,) = struct.unpack(">q", f.read(8))
        (chunk_count,) = struct.unpack(">i", f.read(4))
        offsets_bytes = f.read(8 * chunk_count)
        offsets = struct.unpack(">%dq" % chunk_count, offsets_bytes)
        trailing = f.read()

    comp_len = os.path.getsize(data_db)
    print(f"CompressionInfo.db      : {p}")
    print(f"Data.db                 : {data_db}")
    print(f"compressor              : {compressor}")
    print(f"other options           : {opts}")
    print(f"chunk_length (bytes)    : {chunk_length}   ({chunk_length // 1024} KiB)")
    print(f"max_compressed_length   : {max_comp}")
    print(f"dataLength UNCOMPRESSED : {data_length} bytes ({data_length/1e9:.4f} GB)")
    print(f"Data.db on disk (comp)  : {comp_len} bytes ({comp_len/1e9:.4f} GB)")
    print(f"chunk count             : {chunk_count}")
    print(f"trailing bytes unread   : {len(trailing)}  (0 == format parsed exactly)")
    print(f"compression ratio       : {data_length / comp_len:.4f}x")
    print(f"expected chunk count    : {-(-data_length // chunk_length)}"
          f"  (match={chunk_count == -(-data_length // chunk_length)})")
    # per-chunk compressed sizes
    sizes = [offsets[i + 1] - offsets[i] for i in range(chunk_count - 1)]
    sizes.append(comp_len - offsets[-1])
    # last chunk also carries a 4-byte checksum per chunk; offsets are chunk starts
    sizes_s = sorted(sizes)
    n = len(sizes_s)
    print(f"per-chunk compressed size (incl 4B checksum): min={sizes_s[0]} "
          f"p50={sizes_s[n//2]} p99={sizes_s[int(n*0.99)]} max={sizes_s[-1]} "
          f"mean={sum(sizes)/n:.1f}")
    if len(sys.argv) > 2 and sys.argv[2].isdigit():
        rows = int(sys.argv[2])
        print(f"rows                    : {rows}")
        print(f"UNCOMPRESSED bytes/row  : {data_length / rows:.2f}")
        print(f"COMPRESSED bytes/row    : {comp_len / rows:.2f}")


if __name__ == "__main__":
    main()
