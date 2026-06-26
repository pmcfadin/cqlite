//! Strict `CompressionInfo.db` + compressed-chunk byte parity (Epic #968 / issue #986).
//!
//! Proves CQLite decodes the compression metadata Apache Cassandra 5.0 persisted
//! into `CompressionInfo.db` byte-for-byte, and that the inline per-chunk CRC32
//! trailers Cassandra wrote inside `Data.db` validate and decompress exactly.
//!
//! Scope owned here (issue #986):
//!   * **`CompressionInfo.db` byte-for-byte round-trip** — CQLite parses the
//!     Cassandra-written bytes, and re-serialising the decoded struct with the
//!     authoritative `CompressionMetadata.java` layout reproduces the original
//!     file byte-for-byte. A round-trip that reproduces every byte proves the
//!     decode is complete and position-exact: nothing was dropped, padded, or
//!     misaligned (the class of bug #638). This is the byte oracle — the Cassandra
//!     file itself — without needing a separate textual dump tool.
//!   * **Decoded-field semantics** — compressor simple-name is a known Cassandra
//!     compressor; `chunk_length` is within bounds; `max_compressed_length` is
//!     present (Cassandra >= "na" / all 5.0); the chunk-offset table is strictly
//!     ascending, starts at 0, and its length equals `ceil(data_length /
//!     chunk_length)` (the Cassandra chunk-count invariant).
//!   * **Inline chunk-CRC + decompression parity vs `Data.db`** — every chunk's
//!     4-byte big-endian inline CRC32 trailer is validated and the chunk is
//!     decompressed; the concatenated output length equals `data_length`. This is
//!     the end-to-end proof the offset table, compressed boundaries, and inline
//!     CRCs are mutually consistent with the bytes Cassandra wrote.
//!
//! Fail-closed contract (the established dataset convention — skip on total
//! binary absence, fail on present-but-wrong):
//!   * The committed `*-TOC.txt` manifests always exist and are the driver: every
//!     TOC that lists `CompressionInfo.db` defines an expected-compressed fixture.
//!     Zero TOCs (a broken reference path) turns the lane red, not green.
//!   * The binary `*-CompressionInfo.db` / `*-Data.db` are fetched fixtures
//!     (gitignored). When a fixture's binary is absent, *that* fixture is skipped
//!     (recorded), never silently counted as a pass. When present, every field is
//!     compared byte-for-byte and any mismatch fails.
//!   * If binaries ARE present, the lane must actually have compared something and
//!     exercised every storage format present on disk — a green run with zero
//!     comparisons is a false pass.
//!
//! Out of scope here (see manifest `planned` entries and child issues):
//!   * Real Deflate / Zstd fixture chunk parity — the committed corpus ships LZ4
//!     and Snappy; Deflate/Zstd fixture generation is tracked under epic #970.
//!     Those compressors are decoded if present but not required by this lane.
//!   * Zstd dictionary compression (intentionally deferred, epic #970).
//!   * `Digest.crc32` whole-`Data.db` byte parity — owned by issue #1047 in the
//!     `sstable_parity_toc_component_test` suite.

use std::path::{Path, PathBuf};

use cqlite_core::storage::sstable::chunk_decompressor::create_decompressor_from_file;
use cqlite_core::storage::sstable::compression_info::CompressionInfo;

/// Compressor simple-names Cassandra 5.0 can write into `CompressionInfo.db`
/// (the `ICompressor` implementation's `getClass().getSimpleName()`).
const KNOWN_COMPRESSORS: [&str; 4] = [
    "LZ4Compressor",
    "SnappyCompressor",
    "DeflateCompressor",
    "ZstdCompressor",
];

/// Resolve the committed datasets root (env override first, else workspace tree).
fn datasets_sstables_root() -> PathBuf {
    let root = if let Ok(root) = std::env::var("CQLITE_DATASETS_ROOT") {
        PathBuf::from(root)
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(|workspace| workspace.join("test-data/datasets"))
            .unwrap_or_else(|| PathBuf::from("test-data/datasets"))
    };
    root.join("sstables")
}

/// Recursively collect every committed `*-TOC.txt` manifest, skipping macOS
/// AppleDouble shadow files (`._*`).
fn collect_toc_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("._") {
            continue;
        }
        if path.is_dir() {
            collect_toc_files(&path, out);
        } else if name.ends_with("-TOC.txt") {
            out.push(path);
        }
    }
}

/// Every committed `*-TOC.txt` whose manifest lists `CompressionInfo.db`, i.e.
/// every fixture Cassandra wrote as compressed. Fail-closed: an empty result
/// (broken reference path) turns the strict lane red.
fn compressed_fixture_tocs() -> Vec<PathBuf> {
    let root = datasets_sstables_root();
    let mut tocs = Vec::new();
    collect_toc_files(&root, &mut tocs);
    tocs.retain(|toc| {
        std::fs::read_to_string(toc)
            .map(|s| s.lines().any(|l| l.trim() == "CompressionInfo.db"))
            .unwrap_or(false)
    });
    tocs.sort();
    assert!(
        !tocs.is_empty(),
        "no committed *-TOC.txt manifests listing CompressionInfo.db found under {} — strict \
         CompressionInfo.db parity cannot run (fail-closed guard, not a skip)",
        root.display()
    );
    tocs
}

/// Derive the sibling `*-CompressionInfo.db` binary path from a `*-TOC.txt`.
fn compression_info_for(toc: &Path) -> PathBuf {
    let name = toc
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_else(|| panic!("non-UTF8 TOC name: {}", toc.display()));
    toc.with_file_name(name.replace("-TOC.txt", "-CompressionInfo.db"))
}

/// Derive the sibling `*-Data.db` binary path from a `*-TOC.txt`.
fn data_db_for(toc: &Path) -> PathBuf {
    let name = toc
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or_else(|| panic!("non-UTF8 TOC name: {}", toc.display()));
    toc.with_file_name(name.replace("-TOC.txt", "-Data.db"))
}

/// Independent re-serialiser of a parsed `CompressionInfo` using the authoritative
/// `CompressionMetadata.java` (>= "na" / 5.0) layout. Deliberately NOT the library
/// writer: producing the *Cassandra* bytes from the decoded fields, and asserting
/// they equal the original file, is what makes the round-trip a byte-for-byte
/// oracle rather than a self-consistency check of one code path.
fn serialize_compression_info(info: &CompressionInfo) -> Vec<u8> {
    let mut out = Vec::new();

    // writeUTF(compressor simple name): 2-byte BE length + UTF-8 bytes.
    let name = info.algorithm.as_bytes();
    out.extend_from_slice(&(name.len() as u16).to_be_bytes());
    out.extend_from_slice(name);

    // writeInt(option_count) + option_count * (writeUTF key, writeUTF value).
    out.extend_from_slice(&(info.option_pairs.len() as u32).to_be_bytes());
    for (k, v) in &info.option_pairs {
        let kb = k.as_bytes();
        out.extend_from_slice(&(kb.len() as u16).to_be_bytes());
        out.extend_from_slice(kb);
        let vb = v.as_bytes();
        out.extend_from_slice(&(vb.len() as u16).to_be_bytes());
        out.extend_from_slice(vb);
    }

    // writeInt(chunk_length), writeInt(max_compressed_length), writeLong(data_length).
    out.extend_from_slice(&info.chunk_length.to_be_bytes());
    out.extend_from_slice(&info.max_compressed_length.to_be_bytes());
    out.extend_from_slice(&info.data_length.to_be_bytes());

    // writeInt(chunk_count) + chunk_count * writeLong(chunk_offset).
    out.extend_from_slice(&(info.chunk_offsets.len() as u32).to_be_bytes());
    for &off in &info.chunk_offsets {
        out.extend_from_slice(&off.to_be_bytes());
    }

    out
}

/// Strictly validate the decoded-field semantics of a `CompressionInfo.db`
/// against the invariants Cassandra guarantees when it writes the component.
fn assert_field_semantics(info: &CompressionInfo, ci: &Path) {
    assert!(
        KNOWN_COMPRESSORS.contains(&info.algorithm.as_str()),
        "{}: unknown compressor simple-name {:?} (expected one of {:?}) — \
         misdecoded algorithm or unsupported compressor",
        ci.display(),
        info.algorithm,
        KNOWN_COMPRESSORS,
    );
    assert!(
        info.chunk_length > 0 && info.chunk_length <= 256 * 1024 * 1024,
        "{}: chunk_length {} out of range — misdecoded",
        ci.display(),
        info.chunk_length,
    );
    assert!(
        !info.chunk_offsets.is_empty(),
        "{}: zero chunk offsets — misdecoded",
        ci.display(),
    );
    assert_eq!(
        info.chunk_offsets[0],
        0,
        "{}: first chunk offset {} != 0 (Cassandra's first compressed chunk starts at Data.db byte 0)",
        ci.display(),
        info.chunk_offsets[0],
    );
    // Strictly ascending offsets.
    for i in 1..info.chunk_offsets.len() {
        assert!(
            info.chunk_offsets[i] > info.chunk_offsets[i - 1],
            "{}: chunk offsets not strictly ascending at [{}]: {} <= {}",
            ci.display(),
            i,
            info.chunk_offsets[i],
            info.chunk_offsets[i - 1],
        );
    }
    // Cassandra chunk-count invariant (lower bound only): every chunk decompresses
    // to at most `chunk_length` uncompressed bytes, so the offset table needs *at
    // least* ceil(data_length / chunk_length) entries. It is NOT an equality, and
    // there is no useful upper bound: the compressed writer cuts a chunk early on a
    // forced flush/sync and can append degenerate empty trailing chunks, so a real
    // file can carry MORE chunks than the ceiling and even more than data_length
    // (e.g. 1 byte of data + an empty trailing chunk -> chunk_count 2, data_length 1;
    // the committed `system/local` fixture has data_length 708 < chunk_length 16384
    // yet two chunks). The exact total is cross-checked end-to-end by step (3) below,
    // which decompresses every chunk and asserts the sum equals data_length — that is
    // what catches over-read chunk metadata. This lower bound proves chunk_count is at
    // least consistent with data_length / chunk_length rather than a lucky short read.
    if info.data_length > 0 {
        let min_chunks = info.data_length.div_ceil(info.chunk_length as u64) as usize;
        assert!(
            info.chunk_offsets.len() >= min_chunks,
            "{}: chunk_count {} < ceil(data_length {} / chunk_length {}) = {} — \
             too few chunks to hold the data, inconsistent compression metadata",
            ci.display(),
            info.chunk_offsets.len(),
            info.data_length,
            info.chunk_length,
            min_chunks,
        );
    }
}

/// Strict, byte-for-byte `CompressionInfo.db` + inline-chunk-CRC parity across the
/// committed compressed corpus.
#[test]
fn compression_info_db_strict_byte_parity() {
    let tocs = compressed_fixture_tocs();

    let mut compared = 0usize;
    let mut skipped_no_ci = 0usize;
    let mut decompressed = 0usize;
    let mut skipped_no_data = 0usize;

    // Coverage proof.
    let mut saw_lz4 = false;
    let mut saw_snappy = false;
    let mut saw_deflate = false;
    let mut saw_zstd = false;
    let mut saw_multi_chunk = false; // > 1 chunk (offset table with real boundaries)
    let mut saw_partial_final = false; // data_length not a chunk_length multiple
    let mut format_nb = 0usize;
    let mut format_oa = 0usize;
    let mut format_da = 0usize;

    // Which formats AND compressors have a fetched CompressionInfo.db on disk, computed
    // up front so we can require real coverage for every format/compressor the local
    // dataset actually contains — not just an OR over the corpus minimums (which would
    // let a partial fetch claim LZ4+Snappy parity while only one is present).
    let mut present_nb = false;
    let mut present_oa = false;
    let mut present_da = false;
    let mut present_lz4 = false;
    let mut present_snappy = false;
    for toc in &tocs {
        let ci = compression_info_for(toc);
        if !ci.exists() {
            continue;
        }
        match ci.file_name().and_then(|n| n.to_str()) {
            Some(n) if n.starts_with("nb-") => present_nb = true,
            Some(n) if n.starts_with("oa-") => present_oa = true,
            Some(n) if n.starts_with("da-") => present_da = true,
            _ => {}
        }
        // Parse the (small) header to learn which compressor this fixture uses, so the
        // coverage assertions below can require each present real compressor.
        if let Ok(bytes) = std::fs::read(&ci) {
            if let Ok(info) = CompressionInfo::parse(&bytes) {
                match info.algorithm.as_str() {
                    "LZ4Compressor" => present_lz4 = true,
                    "SnappyCompressor" => present_snappy = true,
                    _ => {}
                }
            }
        }
    }

    for toc in &tocs {
        let ci = compression_info_for(toc);
        if !ci.exists() {
            // Fetched fixture absent (e.g. CI without datasets): skip THIS one.
            skipped_no_ci += 1;
            continue;
        }

        let bytes =
            std::fs::read(&ci).unwrap_or_else(|e| panic!("read {} failed: {e}", ci.display()));

        // (1) Decode and validate field semantics.
        let info = CompressionInfo::parse(&bytes).unwrap_or_else(|e| {
            panic!(
                "{}: CQLite failed to decode CompressionInfo.db: {e:?}",
                ci.display()
            )
        });
        assert_field_semantics(&info, &ci);

        // (2) Byte-for-byte round-trip: re-serialising the decoded struct with the
        //     authoritative Cassandra layout must reproduce the original file exactly.
        let reserialized = serialize_compression_info(&info);
        assert_eq!(
            reserialized.len(),
            bytes.len(),
            "{}: re-serialised CompressionInfo.db length {} != original {} — \
             decode dropped or invented bytes",
            ci.display(),
            reserialized.len(),
            bytes.len(),
        );
        if reserialized != bytes {
            // Report the first differing byte for debugging.
            let first_diff = reserialized
                .iter()
                .zip(bytes.iter())
                .position(|(a, b)| a != b)
                .unwrap_or(0);
            panic!(
                "{}: CompressionInfo.db round-trip is not byte-for-byte (first diff at byte {}: \
                 cqlite 0x{:02x} vs cassandra 0x{:02x}) — decode/encode is not the exact inverse \
                 of Cassandra's CompressionMetadata layout",
                ci.display(),
                first_diff,
                reserialized[first_diff],
                bytes[first_diff],
            );
        }

        // (3) Inline chunk-CRC + decompression parity against the sibling Data.db.
        //     Iterate EVERY chunk record in the offset table (not read_all_data, which
        //     is bounded by data_length and would skip a degenerate empty trailing
        //     chunk): each chunk is decompressed via decompress_chunk_by_index, which
        //     validates its 4-byte big-endian inline CRC32 trailer. The concatenated
        //     length of all chunks must equal data_length — proving the offset table,
        //     compressed boundaries, inline CRCs, and chunk sizes are all mutually
        //     consistent with the bytes Cassandra wrote, including any extra chunks
        //     beyond ceil(data_length / chunk_length).
        let data_path = data_db_for(toc);
        if data_path.exists() {
            let mut dec = create_decompressor_from_file(&ci)
                .unwrap_or_else(|e| panic!("{}: build decompressor failed: {e:?}", ci.display()));
            let mut data_file = std::fs::File::open(&data_path)
                .unwrap_or_else(|e| panic!("open {} failed: {e}", data_path.display()));

            assert_eq!(
                dec.chunk_count(),
                info.chunk_offsets.len(),
                "{}: decompressor chunk_count {} != parsed offset-table length {}",
                ci.display(),
                dec.chunk_count(),
                info.chunk_offsets.len(),
            );

            let mut total: u64 = 0;
            for chunk_index in 0..dec.chunk_count() {
                let chunk = dec
                    .decompress_chunk_by_index(&mut data_file, chunk_index)
                    .unwrap_or_else(|e| {
                        panic!(
                            "{}: inline-CRC validation / decompression of chunk {} in {} failed: {e:?}",
                            ci.display(),
                            chunk_index,
                            data_path.display(),
                        )
                    });
                total += chunk.len() as u64;
            }
            assert_eq!(
                total,
                info.data_length,
                "{}: total decompressed length {} (over {} chunks) != CompressionInfo.db data_length {}",
                ci.display(),
                total,
                info.chunk_offsets.len(),
                info.data_length,
            );
            decompressed += 1;
        } else {
            skipped_no_data += 1;
        }

        // Coverage bookkeeping.
        match info.algorithm.as_str() {
            "LZ4Compressor" => saw_lz4 = true,
            "SnappyCompressor" => saw_snappy = true,
            "DeflateCompressor" => saw_deflate = true,
            "ZstdCompressor" => saw_zstd = true,
            _ => {}
        }
        // A genuine multi-chunk boundary requires more than one *addressable* data
        // chunk, i.e. data_length > chunk_length. A file with >1 offset but
        // data_length <= chunk_length (the degenerate empty-trailing-chunk case, e.g.
        // system/local) has only one addressable chunk and must NOT satisfy the
        // multi-chunk coverage claim.
        if info.data_length > info.chunk_length as u64 {
            saw_multi_chunk = true;
        }
        if info.data_length % info.chunk_length as u64 != 0 {
            saw_partial_final = true;
        }
        match ci.file_name().and_then(|n| n.to_str()) {
            Some(n) if n.starts_with("nb-") => format_nb += 1,
            Some(n) if n.starts_with("oa-") => format_oa += 1,
            Some(n) if n.starts_with("da-") => format_da += 1,
            _ => {}
        }

        compared += 1;
    }

    eprintln!(
        "compression_info_db_strict_byte_parity: {compared} compared, {skipped_no_ci} skipped \
         (CompressionInfo.db absent) | {decompressed} decompressed vs Data.db, {skipped_no_data} \
         skipped (Data.db absent) | formats nb={format_nb} oa={format_oa} da={format_da} \
         | compressors lz4={saw_lz4} snappy={saw_snappy} deflate={saw_deflate} zstd={saw_zstd} \
         | partial_final_chunk={saw_partial_final}"
    );

    let any_present = present_nb || present_oa || present_da;
    if !any_present {
        // Dataset-absent SKIP path (distinct from a silent pass): the committed
        // TOC references were validated for presence above (compressed_fixture_tocs
        // fails closed), but there is no fetched binary to compare against here.
        eprintln!(
            "compression_info_db_strict_byte_parity: SKIP — no *-CompressionInfo.db binaries \
             fetched ({skipped_no_ci} TOC references list CompressionInfo.db without a binary)"
        );
        return;
    }

    // Binaries ARE present: the lane must have proved something and exercised every
    // storage format the local dataset contains.
    assert!(
        compared > 0,
        "CompressionInfo.db binaries are present (nb={present_nb} oa={present_oa} da={present_da}) \
         but zero fixtures were compared — strict parity lane proved nothing"
    );
    if present_nb {
        assert!(
            format_nb > 0,
            "nb-* CompressionInfo.db present but none compared — nb parity unproven"
        );
    }
    if present_oa {
        assert!(
            format_oa > 0,
            "oa-* CompressionInfo.db present but none compared — oa parity unproven"
        );
    }
    if present_da {
        assert!(
            format_da > 0,
            "da-* CompressionInfo.db present but none compared — da parity unproven"
        );
    }

    // The offset table must be exercised with real multi-chunk boundaries; a corpus
    // of only single-chunk files would not prove the chunk-offset parity claim.
    assert!(
        saw_multi_chunk,
        "no compressed fixture with > 1 chunk — multi-chunk offset-table parity unproven"
    );

    // Every committed real compressor (LZ4 / Snappy) that is PRESENT on disk must have
    // been decoded — not merely one of them. The manifest claims both LZ4 and Snappy
    // as mirrored byte-for-byte parity; this requires each independently so a corpus
    // missing one cannot falsely claim coverage for both. (Deflate/Zstd are decoded if
    // present but not required — fixtures tracked under epic #970.)
    assert!(
        present_lz4 || present_snappy,
        "no LZ4 or Snappy CompressionInfo.db present despite present binaries — \
         real-compressor parity corpus is missing its minimums"
    );
    if present_lz4 {
        assert!(
            saw_lz4,
            "LZ4 CompressionInfo.db present but no LZ4 fixture decoded — LZ4 parity unproven"
        );
    }
    if present_snappy {
        assert!(
            saw_snappy,
            "Snappy CompressionInfo.db present but no Snappy fixture decoded — Snappy parity unproven"
        );
    }

    // When at least one Data.db was fetched alongside, the inline-CRC + decompression
    // path must actually have run (never silently degrade to metadata-only).
    if skipped_no_data < compared {
        assert!(
            decompressed > 0,
            "Data.db binaries present alongside CompressionInfo.db but no chunk was decompressed \
             — inline-CRC parity unproven"
        );
    }

    // Note: saw_partial_final / saw_deflate / saw_zstd are informational coverage,
    // surfaced in the summary line above rather than asserted — the corpus could
    // legitimately align on chunk_length, and Deflate/Zstd fixtures are `planned`
    // (epic #970), so requiring them here would be a false-red.
}

/// Corrupted `CompressionInfo.db` and corrupted compressed chunks fail closed with
/// explicit errors in strict mode — never silently accepted, never coerced to a
/// placeholder. Uses fetched binaries as the clean baseline (skips when none are
/// fetched).
#[test]
fn compression_info_db_strict_corruption_fails_closed() {
    let tocs = compressed_fixture_tocs();
    let Some(toc) = tocs.iter().find(|t| compression_info_for(t).exists()) else {
        eprintln!(
            "compression_info_db_strict_corruption_fails_closed: SKIP — no *-CompressionInfo.db \
             binary fetched"
        );
        return;
    };
    let ci = compression_info_for(toc);
    let clean = std::fs::read(&ci).unwrap_or_else(|e| panic!("read {} failed: {e}", ci.display()));

    // Sanity: the clean fixture decodes.
    let info = CompressionInfo::parse(&clean)
        .unwrap_or_else(|e| panic!("clean {} failed to parse: {e:?}", ci.display()));

    // (a) Truncate below the header → explicit decode error.
    {
        let truncated = &clean[..3.min(clean.len())];
        assert!(
            CompressionInfo::parse(truncated).is_err(),
            "strict decode accepted a truncated CompressionInfo.db — must fail closed"
        );
    }

    // (b) Zero the chunk_length field → "chunk_length cannot be zero". The field
    //     sits immediately after the header (algorithm UTF + option pairs).
    {
        let mut header_len = 2 + info.algorithm.len() + 4;
        for (k, v) in &info.option_pairs {
            header_len += 2 + k.len() + 2 + v.len();
        }
        let mut corrupt = clean.clone();
        for b in corrupt.iter_mut().skip(header_len).take(4) {
            *b = 0;
        }
        assert!(
            CompressionInfo::parse(&corrupt).is_err(),
            "strict decode accepted a CompressionInfo.db with chunk_length=0 — must fail closed"
        );
    }

    // (c) Corrupt the compressor-name length prefix to an impossible value → the
    //     UTF read runs past the buffer and errors.
    {
        let mut corrupt = clean.clone();
        corrupt[0] = 0xff;
        corrupt[1] = 0xff;
        assert!(
            CompressionInfo::parse(&corrupt).is_err(),
            "strict decode accepted a CompressionInfo.db with a bogus name length — must fail closed"
        );
    }

    // (d) Corrupt a compressed chunk byte in Data.db → inline CRC32 mismatch on read.
    //     Only when the sibling Data.db is fetched.
    let data_path = data_db_for(toc);
    if data_path.exists() {
        let clean_data = std::fs::read(&data_path)
            .unwrap_or_else(|e| panic!("read {} failed: {e}", data_path.display()));
        assert!(
            clean_data.len() > 8,
            "{}: Data.db too small to corrupt a chunk",
            data_path.display()
        );
        // Flip a byte early in the first compressed chunk (byte 4 is safely inside
        // the first chunk's payload, before its trailing CRC).
        let tmp =
            std::env::temp_dir().join(format!("cqlite-986-corrupt-data-{}.db", std::process::id()));
        let mut corrupt_data = clean_data.clone();
        corrupt_data[4] ^= 0xff;
        std::fs::write(&tmp, &corrupt_data)
            .unwrap_or_else(|e| panic!("write {} failed: {e}", tmp.display()));

        let mut dec = create_decompressor_from_file(&ci)
            .unwrap_or_else(|e| panic!("build decompressor failed: {e:?}"));
        let mut f = std::fs::File::open(&tmp)
            .unwrap_or_else(|e| panic!("open {} failed: {e}", tmp.display()));
        let result = dec.read_all_data(&mut f);
        let _ = std::fs::remove_file(&tmp);
        assert!(
            result.is_err(),
            "decompressor accepted a corrupted compressed chunk (inline CRC mismatch) — \
             must fail closed"
        );

        eprintln!(
            "compression_info_db_strict_corruption_fails_closed: corruption rejected against {} \
             (and chunk-CRC against {})",
            ci.display(),
            data_path.display(),
        );
    } else {
        eprintln!(
            "compression_info_db_strict_corruption_fails_closed: CompressionInfo.db corruption \
             rejected against {} (Data.db absent — inline-CRC corruption case skipped)",
            ci.display(),
        );
    }
}
