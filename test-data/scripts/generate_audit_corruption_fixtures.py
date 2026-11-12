#!/usr/bin/env python3
"""Audit corruption fixtures (issue #4195, `cqlite verify --mode audit`).

Called by generate-corruption-corpus.sh. Six deterministic, offline byte edits
of Cassandra-5.0.8-written datasets-v4 files, each isolating ONE cross-component
invariant. Like the rest of the corpus: no randomness, no timestamps, clean
sources are only read, corrupted binaries are gitignored and regenerable
byte-for-byte; only the manifest entries (printed to stdout as YAML) and the
mutation arithmetic below are committed.

Every mutation is computed from the clean file's OWN structure (never from
CQLite's verifier output), and asserts its structural premises before editing.

Usage: generate_audit_corruption_fixtures.py --corrupt-dir D --root R [--root R ...]
                                             [--verify-only]
Exit 0 on success. YAML for the manifest goes to stdout; the report to stderr.
"""
import argparse, hashlib, os, shutil, struct, sys, zlib


def sha(b):
    return hashlib.sha256(b).hexdigest()


def vint_len(first):
    n = 0
    mask = 0x80
    while n < 8 and first & mask:
        n += 1
        mask >>= 1
    return n + 1


def index_entries(b):
    """[(entry_offset, key_len, off_vint_pos, off_vint_len)] of a BIG nb/oa Index.db."""
    out, i = [], 0
    while i < len(b):
        kl = struct.unpack(">H", b[i:i + 2])[0]
        p = i + 2 + kl
        ol = vint_len(b[p])
        q = p + ol
        pl = vint_len(b[q])
        # promoted length vint value (0 on every fixture used here)
        val = b[q] if pl == 1 else None
        assert val == 0, "fixture premise: no promoted index in %d" % i
        out.append((i, kl, p, ol))
        i = q + pl
    assert i == len(b)
    return out


def m_index_entry_offset_swap(files):
    b = bytearray(files["Index.db"])
    ents = index_entries(bytes(b))
    for (e0, e1) in zip(ents, ents[1:]):
        o0, o1 = bytes(b[e0[2]:e0[2] + e0[3]]), bytes(b[e1[2]:e1[2] + e1[3]])
        if e0[3] == e1[3] and o0 != o1:
            b[e0[2]:e0[2] + e0[3]], b[e1[2]:e1[2] + e1[3]] = o1, o0
            return "Index.db", bytes(b), {
                "mutation_type": "swap_adjacent_vints",
                "byte_offset": e0[2],
                "original_bytes_hex": o0.hex() + "," + o1.hex(),
                "mutated_bytes_hex": o1.hex() + "," + o0.hex(),
                "detail": "data_offset VInts of Index.db entries at byte offsets %d and %d swapped" % (e0[0], e1[0]),
            }
    raise SystemExit("no adjacent index entries with equal-length distinct offset VInts")


def summary_layout(b):
    mi, size, off_sz, sl, fs = struct.unpack(">iiqii", b[:24])
    offs = struct.unpack("<%di" % size, b[24:24 + 4 * size])  # native (LE) in the file
    return size, offs


def m_summary_sample_position_corrupt(files):
    sb = bytearray(files["Summary.db"])
    ents = index_entries(files["Index.db"])
    starts = [e[0] for e in ents]
    size, offs = summary_layout(bytes(sb))
    assert size >= 3
    j = 1  # sample 1 (not the last: its key length needs the next offset)
    keylen = offs[j + 1] - offs[j] - 8
    pos_at = 24 + offs[j] + keylen
    orig = struct.unpack("<q", sb[pos_at:pos_at + 8])[0]
    assert orig in starts, "premise: summary position is an Index.db entry start"
    new = starts[1]
    assert new != orig and new in starts
    sb[pos_at:pos_at + 8] = struct.pack("<q", new)
    return "Summary.db", bytes(sb), {
        "mutation_type": "setint64_le",
        "byte_offset": pos_at,
        "original_bytes_hex": struct.pack("<q", orig).hex(),
        "mutated_bytes_hex": struct.pack("<q", new).hex(),
        "detail": "sample 1 position %d -> %d (start of Index.db entry 1)" % (orig, new),
    }


def m_filter_db_bit_flip_bti(files):
    b = bytearray(files["Filter.db"])
    hashes, longs = struct.unpack(">ii", b[:8])
    assert len(b) == 8 + 8 * longs
    for at in range(8, len(b)):
        if b[at]:
            orig = b[at]
            low = orig & -orig
            b[at] = orig ^ low
            return "Filter.db", bytes(b), {
                "mutation_type": "bitflip",
                "byte_offset": at,
                "original_bytes_hex": "%02x" % orig,
                "mutated_bytes_hex": "%02x" % b[at],
                "detail": "lowest set bit of the first non-zero bit-array byte cleared 1->0",
            }
    raise SystemExit("Filter.db bit array is empty")


def m_bti_data_offset_out_of_bounds(files):
    b = bytearray(files["Partitions.db"])
    # Two PayloadOnly leaves [0x08][hash][signed position]; negative => DataOffset(~pos).
    assert b[0] == 0x08 and b[3] == 0x08, "premise: two leading 1-byte-payload leaves"
    orig = b[5]
    assert orig >= 0x80, "premise: second leaf is a DataOffset leaf"
    b[5] = 0x80  # -128 => DataOffset(127), past this table's 93-byte logical Data.db
    return "Partitions.db", bytes(b), {
        "mutation_type": "setbyte",
        "byte_offset": 5,
        "original_bytes_hex": "%02x" % orig,
        "mutated_bytes_hex": "80",
        "detail": "second leaf DataOffset(%d) -> DataOffset(127)" % (~(orig - 256)),
    }


def m_compression_info_short_coverage(files):
    b = files["CompressionInfo.db"]
    i = 0
    n = struct.unpack(">H", b[i:i + 2])[0]; i += 2 + n
    o = struct.unpack(">I", b[i:i + 4])[0]; i += 4
    for _ in range(o):
        n = struct.unpack(">H", b[i:i + 2])[0]; i += 2 + n
        n = struct.unpack(">H", b[i:i + 2])[0]; i += 2 + n
    cl, mx, dl, cnt = struct.unpack(">IIqI", b[i:i + 20])
    count_at = i + 16
    assert len(b) == i + 20 + 8 * cnt and cnt == -(-dl // cl), "premise: exact-coverage table"
    nb = bytearray(b[:-8])
    nb[count_at:count_at + 4] = struct.pack(">I", cnt - 1)
    return "CompressionInfo.db", bytes(nb), {
        "mutation_type": "drop_last_chunk_offset",
        "byte_offset": count_at,
        "original_bytes_hex": "%08x" % cnt,
        "mutated_bytes_hex": "%08x" % (cnt - 1),
        "detail": "chunk count %d -> %d and the final 8-byte offset removed" % (cnt, cnt - 1),
    }


def m_statistics_partition_count_mismatch(files):
    b = bytearray(files["Statistics.db"])
    n = struct.unpack(">i", b[:4])[0]
    toc = [struct.unpack(">ii", b[8 + 8 * k:16 + 8 * k]) for k in range(n)]
    starts = [t[1] for t in toc] + [len(b)]
    k = [t[0] for t in toc].index(2)  # STATS
    s, e = starts[k], starts[k + 1] - 4
    assert zlib.crc32(bytes(b[s:e])) == struct.unpack(">I", b[e:e + 4])[0], "premise: STATS CRC valid"
    cnt = struct.unpack(">i", b[s:s + 4])[0]
    for bkt in range(cnt):
        at = s + 4 + 16 * bkt + 8
        v = struct.unpack(">q", b[at:at + 8])[0]
        if v:
            b[at:at + 8] = struct.pack(">q", v + 1)
            crc_at = e
            b[crc_at:crc_at + 4] = struct.pack(">I", zlib.crc32(bytes(b[s:e])))
            return "Statistics.db", bytes(b), {
                "mutation_type": "setint64_be+crc",
                "byte_offset": at,
                "original_bytes_hex": struct.pack(">q", v).hex(),
                "mutated_bytes_hex": struct.pack(">q", v + 1).hex(),
                "detail": "estimatedPartitionSize bucket %d count %d -> %d; the STATS component CRC32 at byte %d recomputed so the file stays valid" % (bkt, v, v + 1, crc_at),
            }
    raise SystemExit("estimatedPartitionSize histogram is empty")


# name, manifest_key, keyspace, table, prefix (nb-1-big / da-2-bti), components needed, mutator,
# expected_error_class, invariant, rationale
FIXTURES = [
    ("index_entry_offset_swap", "cass.corruption.audit.index_entry_offset_swap", "test_basic", "simple_table", "nb-1-big",
     ["Index.db"], m_index_entry_offset_swap, "IndexPositionKeyMismatch", "index-data-identity",
     "Two adjacent Index.db entries' data_offset VInts (equal length) are swapped: each entry stays structurally valid, so check_big_index stays green, but each key now names the other partition's Data.db offset."),
    ("summary_sample_position_corrupt", "cass.corruption.audit.summary_sample_position_corrupt", "test_basic", "simple_table", "nb-1-big",
     ["Summary.db", "Index.db"], m_summary_sample_position_corrupt, "SummarySampleMismatch", "summary-index-correlation",
     "One Summary.db sample's position is rewritten to the start of a DIFFERENT, in-bounds Index.db entry."),
    ("filter_db_bit_flip_bti", "cass.corruption.audit.filter_db_bit_flip_bti", "test_da", "wide_table", "da-2-bti",
     ["Filter.db"], m_filter_db_bit_flip_bti, "FilterFalseNegative", "bloom-no-false-negative",
     "BTI analogue of filter_db_bit_flip: one bit-array bit cleared 1->0 so a present partition key hashes to an unset bit."),
    ("bti_data_offset_out_of_bounds", "cass.corruption.audit.bti_data_offset_out_of_bounds", "test_da", "ttl_table", "da-2-bti",
     ["Partitions.db"], m_bti_data_offset_out_of_bounds, "BtiDataOffsetOutOfBounds", "bti-trie-bounds",
     "A DataOffset leaf's payload is rewritten to a position past the end of Data.db."),
    ("compression_info_short_coverage", "cass.corruption.audit.compression_info_short_coverage", "test_comp", "lz4_table", "nb-1-big",
     ["CompressionInfo.db"], m_compression_info_short_coverage, "CompressionCoverageGap", "compression-coverage",
     "The LAST chunk offset is dropped (count decremented): the chunk table declares one chunk fewer than ceil(data_length/chunk_length)."),
    ("statistics_partition_count_mismatch", "cass.corruption.audit.statistics_partition_count_mismatch", "test_comp", "lz4_table", "nb-1-big",
     ["Statistics.db"], m_statistics_partition_count_mismatch, "StatisticsScanMismatch", "statistics-vs-scan",
     "One estimatedPartitionSize bucket count is raised by 1 (component CRC32 recomputed): the file stays valid but declares one more partition than a scan finds."),
]


def fixture_dir_sha256(d):
    def sidecar(n):
        return n == ".DS_Store" or n.startswith("._") or n.endswith(".db.jsonl") or n.endswith(".db.txt")
    names = sorted(n for n in os.listdir(d) if os.path.isfile(os.path.join(d, n)) and not sidecar(n))
    h = hashlib.sha256()
    for n in names:
        h.update(n.encode()); h.update(b"\x00")
        data = open(os.path.join(d, n), "rb").read()
        h.update(len(data).to_bytes(8, "big")); h.update(data)
    h.update(len(names).to_bytes(8, "big"))
    return h.hexdigest()


def find_source(roots, ks, table):
    for r in roots:
        d = os.path.join(r, ks)
        if os.path.isdir(d):
            for n in sorted(os.listdir(d)):
                p = os.path.join(d, n)
                if n.startswith(table + "-") and os.path.isdir(p) and any(f.endswith("-Data.db") for f in os.listdir(p)):
                    return p
    return None


def rel_clean(p):
    for marker in ("/datasets/sstables/",):
        if marker in p:
            return "datasets/sstables/" + p.split(marker, 1)[1]
    return p


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--corrupt-dir", required=True)
    ap.add_argument("--root", action="append", required=True)
    ap.add_argument("--verify-only", action="store_true")
    a = ap.parse_args()
    for (name, key, ks, table, prefix, comps, mut, errc, inv, why) in FIXTURES:
        src = find_source(a.root, ks, table)
        if src is None:
            sys.exit("%s: clean source %s.%s not found under %s" % (name, ks, table, a.root))
        files = {}
        for c in comps:
            files[c] = open(os.path.join(src, "%s-%s" % (prefix, c)), "rb").read()
        if name == "bti_data_offset_out_of_bounds":
            files = {"Partitions.db": files["Partitions.db"]}
        comp, mutated, info = mut(files)
        dest = os.path.join(a.corrupt_dir, name)
        target = os.path.join(dest, "%s-%s" % (prefix, comp))
        if a.verify_only:
            on_disk = open(target, "rb").read()
            if on_disk != mutated:
                sys.exit("%s: on-disk %s differs from its deterministic mutation" % (name, target))
        else:
            if os.path.isdir(dest):
                shutil.rmtree(dest)
            shutil.copytree(src, dest, ignore=shutil.ignore_patterns("*.db.jsonl", "*.db.txt", ".DS_Store", "._*"))
            open(target, "wb").write(mutated)
        clean = open(os.path.join(src, "%s-%s" % (prefix, comp)), "rb").read()
        # every other component byte-identical to the clean source
        for f in os.listdir(dest):
            if f == "%s-%s" % (prefix, comp):
                continue
            if open(os.path.join(dest, f), "rb").read() != open(os.path.join(src, f), "rb").read():
                sys.exit("%s: component %s changed besides %s" % (name, f, comp))
        print("  - name: %s" % name)
        print("    manifest_key: %s" % key)
        print("    status: audit")
        print("    component: %s-%s" % (prefix, comp))
        print('    clean_source_table: "%s"' % table)
        print('    clean_source_path: "%s"' % rel_clean(os.path.join(src, "%s-%s" % (prefix, comp))))
        print('    corrupted_path: "corruption/test_comp_corrupt/%s/%s-%s"' % (name, prefix, comp))
        print("    mutation_type: %s" % info["mutation_type"])
        print("    byte_offset: %d" % info["byte_offset"])
        print('    original_bytes_hex: "%s"' % info["original_bytes_hex"])
        print('    mutated_bytes_hex: "%s"' % info["mutated_bytes_hex"])
        print('    original_sha256: "%s"' % sha(clean))
        print('    corrupted_sha256: "%s"' % sha(mutated))
        print("    original_size_bytes: %d" % len(clean))
        print("    corrupted_size_bytes: %d" % len(mutated))
        print("    expected_failing_component: %s" % comp)
        print("    expected_error_class: %s" % errc)
        print("    audit_invariant: %s" % inv)
        print('    mutation_detail: "%s"' % info["detail"].replace('"', "'"))
        print('    rationale: "%s"' % why.replace('"', "'"))
        print("    cassandra_verdict: n/a")
        print('    verdict_parity: "n/a: no Cassandra-side check for this invariant"')
        print('    verdict_note: "Audit-only fixture (issue #4195): not part of the sstableverify parity set; status audit keeps sstable_parity_corruption_verify (status active only) unaffected."')
        print('    fixture_dir_sha256: "%s"' % fixture_dir_sha256(dest))
        print("", end="")
        sys.stderr.write("[audit-corrupt] %-36s %-20s @%-8d %s\n" % (name, comp, info["byte_offset"], info["detail"]))


if __name__ == "__main__":
    main()
