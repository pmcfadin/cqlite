//! Issue #997 (Epic #970): strict CompressionInfo.db field & byte parity tests.
//!
//! These tests parse the real Cassandra 5.0.2 `CompressionInfo.db` files from the
//! `test_comp` compression fixture matrix and assert STRICTLY, byte-for-byte, that
//! CQLite's parser produces the exact values Cassandra wrote.
//!
//! Asserted (no heuristics — every value cross-checked against committed goldens):
//!   * exact Cassandra compressor name string per fixture
//!     (`LZ4Compressor` / `SnappyCompressor` / `DeflateCompressor` / `ZstdCompressor`)
//!   * `chunk_length`, `max_compressed_length`, `data_length` (total uncompressed),
//!     and `chunk_count`
//!   * the exact ordered chunk-offset sequence (full vector compare, not just count)
//!   * per-chunk on-disk lengths derived from adjacent offsets (final from Data.db size)
//!   * CompressionInfo.db carries OFFSETS ONLY — no inline CRC trailers / chunk-CRC arrays
//!     (the byte layout ends immediately after the last offset)
//!   * `uncompressed_table` has NO CompressionInfo.db (absence handled correctly)
//!   * `short_final_chunk`: derived final chunk uncompressed length < `chunk_length`
//!
//! Fixtures resolve via `CQLITE_DATASETS_ROOT`. The table directory UUID is NEVER
//! hardcoded; we glob `<root>/sstables/test_comp/<table>-*`. When the dataset (or its
//! gitignored `*.db` binaries) is absent, the tests SKIP cleanly; when the fixture is
//! present but a comparison would be empty/zero, the tests FAIL loudly.
//!
//! On any field mismatch, the helper assertion prints the fixture path, field name,
//! expected value, actual value, byte offset, and a hexdump window around that offset.

use cqlite_core::storage::sstable::compression_info::CompressionInfo;
use std::path::{Path, PathBuf};

// ----------------------------------------------------------------------------
// Golden model parsed from the committed `nb-1-big-CompressionInfo.db.txt` sidecar
// ----------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct ChunkGolden {
    idx: usize,
    offset: u64,
    on_disk_len: u64,
    comp_len: u64,
    raw_uncompressed_len: u64,
    raw_stored: bool,
}

#[derive(Debug, Clone)]
struct CompressionGolden {
    algorithm: String,
    option_count: u32,
    chunk_length: u32,
    max_compressed_length: u32,
    total_uncompressed_length: u64,
    chunk_count: usize,
    data_db_size_bytes: u64,
    chunks: Vec<ChunkGolden>,
    short_final_chunk: bool,
    raw_stored_chunk_count: usize,
}

/// Parse the human-readable `*.CompressionInfo.db.txt` golden sidecar.
fn parse_golden(text: &str, golden_path: &Path) -> CompressionGolden {
    let mut algorithm = None;
    let mut option_count = None;
    let mut chunk_length = None;
    let mut max_compressed_length = None;
    let mut total_uncompressed_length = None;
    let mut chunk_count = None;
    let mut data_db_size_bytes = None;
    let mut short_final_chunk = false;
    let mut raw_stored_chunk_count = None;
    let mut chunks = Vec::new();

    let kv = |line: &str, key: &str| -> Option<String> {
        let prefix = format!("{key}:");
        line.strip_prefix(&prefix).map(|v| v.trim().to_string())
    };

    for line in text.lines() {
        let line = line.trim_end();
        if let Some(v) = kv(line, "algorithm") {
            algorithm = Some(v);
        } else if let Some(v) = kv(line, "option_count") {
            option_count = Some(v.parse().expect("option_count parse"));
        } else if let Some(v) = kv(line, "chunk_length") {
            chunk_length = Some(v.parse().expect("chunk_length parse"));
        } else if let Some(v) = kv(line, "max_compressed_length") {
            max_compressed_length = Some(v.parse().expect("max_compressed_length parse"));
        } else if let Some(v) = kv(line, "total_uncompressed_length") {
            total_uncompressed_length = Some(v.parse().expect("total_uncompressed_length parse"));
        } else if let Some(v) = kv(line, "chunk_count") {
            chunk_count = Some(v.parse().expect("chunk_count parse"));
        } else if let Some(v) = kv(line, "data_db_size_bytes") {
            data_db_size_bytes = Some(v.parse().expect("data_db_size_bytes parse"));
        } else if let Some(rest) = line.strip_prefix("short_final_chunk:") {
            short_final_chunk = rest.trim_start().starts_with("True");
        } else if let Some(rest) = line.strip_prefix("raw_stored_chunk_count:") {
            // "raw_stored_chunk_count: N  indices: [...]"
            let n = rest
                .split_whitespace()
                .next()
                .expect("raw_stored_chunk_count value");
            raw_stored_chunk_count = Some(n.parse().expect("raw_stored_chunk_count parse"));
        } else if line.starts_with(char::is_numeric) && line.contains('\t') {
            // per-chunk data row: idx offset on_disk_len comp_len raw_uncompressed_len raw_stored
            let cols: Vec<&str> = line.split('\t').collect();
            if cols.len() == 6 {
                chunks.push(ChunkGolden {
                    idx: cols[0].parse().expect("chunk idx"),
                    offset: cols[1].parse().expect("chunk offset"),
                    on_disk_len: cols[2].parse().expect("on_disk_len"),
                    comp_len: cols[3].parse().expect("comp_len"),
                    raw_uncompressed_len: cols[4].parse().expect("raw_uncompressed_len"),
                    raw_stored: cols[5].eq_ignore_ascii_case("true"),
                });
            }
        }
    }

    CompressionGolden {
        algorithm: algorithm
            .unwrap_or_else(|| panic!("golden missing algorithm: {}", golden_path.display())),
        option_count: option_count.expect("golden option_count"),
        chunk_length: chunk_length.expect("golden chunk_length"),
        max_compressed_length: max_compressed_length.expect("golden max_compressed_length"),
        total_uncompressed_length: total_uncompressed_length
            .expect("golden total_uncompressed_length"),
        chunk_count: chunk_count.expect("golden chunk_count"),
        data_db_size_bytes: data_db_size_bytes.expect("golden data_db_size_bytes"),
        chunks,
        short_final_chunk,
        raw_stored_chunk_count: raw_stored_chunk_count.expect("golden raw_stored_chunk_count"),
    }
}

// ----------------------------------------------------------------------------
// Fixture resolution (CQLITE_DATASETS_ROOT, glob keyspace/table dir, no UUID hardcode)
// ----------------------------------------------------------------------------

/// Resolve `<CQLITE_DATASETS_ROOT>/sstables/test_comp/<table>-<uuid>/` by globbing the
/// table prefix. Returns `None` when the dataset root is unset or the directory absent.
fn resolve_fixture_dir(table: &str) -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let comp_dir = Path::new(&root).join("sstables").join("test_comp");
    if !comp_dir.is_dir() {
        return None;
    }
    let prefix = format!("{table}-");
    let mut matches: Vec<PathBuf> = std::fs::read_dir(&comp_dir)
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with(&prefix))
                    .unwrap_or(false)
        })
        .collect();
    matches.sort();
    assert!(
        matches.len() <= 1,
        "ambiguous fixture dirs for table '{table}' under {}: {:?}",
        comp_dir.display(),
        matches
    );
    matches.into_iter().next()
}

// ----------------------------------------------------------------------------
// Failure diagnostics: fixture path, field, expected, actual, byte offset, hexdump
// ----------------------------------------------------------------------------

/// Render a hexdump window of `data` centered on `offset` (16 bytes/row, ASCII gutter).
fn hexdump_window(data: &[u8], offset: usize, context_rows: usize) -> String {
    let row = 16usize;
    let center = (offset / row) * row;
    let start = center.saturating_sub(context_rows * row);
    let end = (center + (context_rows + 1) * row).min(data.len());

    let mut out = String::new();
    let mut addr = start;
    while addr < end {
        let line_end = (addr + row).min(end);
        out.push_str(&format!("  {addr:08x}  "));
        for (i, byte) in data.iter().enumerate().take(addr + row).skip(addr) {
            if i < line_end {
                let marker = if i == offset { '>' } else { ' ' };
                out.push(marker);
                out.push_str(&format!("{:02x}", byte));
            } else {
                out.push_str("   ");
            }
        }
        out.push_str("  |");
        for &b in data.iter().take(line_end).skip(addr) {
            out.push(if (0x20..0x7f).contains(&b) {
                b as char
            } else {
                '.'
            });
        }
        out.push_str("|\n");
        addr += row;
    }
    out
}

/// Assert `actual == expected` for a named field; on failure emit the full diagnostic
/// block (fixture path, field, expected, actual, byte offset, hexdump window).
fn assert_field<T: PartialEq + std::fmt::Debug>(
    fixture_path: &Path,
    raw: &[u8],
    field: &str,
    byte_offset: usize,
    expected: T,
    actual: T,
) {
    if expected != actual {
        panic!(
            "\nCompressionInfo.db parity failure\n  fixture: {}\n  field:   {}\n  expected: {:?}\n  actual:   {:?}\n  byte_offset: {} (0x{:x})\n  hexdump:\n{}",
            fixture_path.display(),
            field,
            expected,
            actual,
            byte_offset,
            byte_offset,
            hexdump_window(raw, byte_offset.min(raw.len().saturating_sub(1)), 1),
        );
    }
}

// ----------------------------------------------------------------------------
// Byte-offset model of the CompressionInfo.db header (for diagnostics + layout assert)
// ----------------------------------------------------------------------------

struct HeaderOffsets {
    chunk_length: usize,
    max_compressed_length: usize,
    data_length: usize,
    chunk_count: usize,
    first_offset: usize,
}

/// Compute the byte offsets of each header field given the parsed algorithm + options.
/// Mirrors `CompressionMetadata.writeHeader()` exactly.
fn header_offsets(info: &CompressionInfo) -> HeaderOffsets {
    // writeUTF(name): u16 len + name bytes
    let mut pos = 2 + info.algorithm.len();
    // writeInt(option_count)
    pos += 4;
    // option pairs: each writeUTF(key)+writeUTF(value)
    for (k, v) in &info.option_pairs {
        pos += 2 + k.len() + 2 + v.len();
    }
    let chunk_length = pos;
    let max_compressed_length = chunk_length + 4;
    let data_length = max_compressed_length + 4;
    let chunk_count = data_length + 8;
    let first_offset = chunk_count + 4;
    HeaderOffsets {
        chunk_length,
        max_compressed_length,
        data_length,
        chunk_count,
        first_offset,
    }
}

// ----------------------------------------------------------------------------
// Core driver: parse a compressed fixture and assert strict parity vs golden
// ----------------------------------------------------------------------------

/// Run the full strict-parity battery for one compressed table fixture.
///
/// `expected_algorithm` pins the exact Cassandra compressor name; the golden sidecar
/// independently cross-checks every numeric field. Returns a one-line summary string
/// `(algorithm, chunk_count, final_chunk_uncompressed_len)` for the test report.
fn run_compressed_fixture(table: &str, expected_algorithm: &str) -> Option<(String, usize, u64)> {
    let dir = match resolve_fixture_dir(table) {
        Some(d) => d,
        None => {
            eprintln!("SKIP {table}: CQLITE_DATASETS_ROOT unset or test_comp/{table}-* dir absent");
            return None;
        }
    };

    let ci_path = dir.join("nb-1-big-CompressionInfo.db");
    let golden_path = dir.join("nb-1-big-CompressionInfo.db.txt");

    if !ci_path.exists() {
        eprintln!(
            "SKIP {table}: {} missing (gitignored binary not fetched)",
            ci_path.display()
        );
        return None;
    }
    assert!(
        golden_path.exists(),
        "{table}: CompressionInfo.db present but golden sidecar missing: {}",
        golden_path.display()
    );

    let raw = std::fs::read(&ci_path).unwrap_or_else(|e| panic!("read {}: {e}", ci_path.display()));
    assert!(
        !raw.is_empty(),
        "{table}: CompressionInfo.db present but empty: {}",
        ci_path.display()
    );

    let golden_text = std::fs::read_to_string(&golden_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", golden_path.display()));
    let golden = parse_golden(&golden_text, &golden_path);

    let info = CompressionInfo::parse(&raw)
        .unwrap_or_else(|e| panic!("{table}: parse {} failed: {e}", ci_path.display()));

    let off = header_offsets(&info);

    // --- exact compressor name string (pinned + cross-checked against golden) ---
    assert_field(
        &ci_path,
        &raw,
        "algorithm",
        2,
        expected_algorithm,
        info.algorithm.as_str(),
    );
    assert_field(
        &ci_path,
        &raw,
        "algorithm (golden cross-check)",
        2,
        golden.algorithm.as_str(),
        info.algorithm.as_str(),
    );

    // --- option_count (none of these fixtures carry inline options) ---
    assert_field(
        &ci_path,
        &raw,
        "option_count",
        6,
        golden.option_count as usize,
        info.option_pairs.len(),
    );

    // --- scalar header fields ---
    assert_field(
        &ci_path,
        &raw,
        "chunk_length",
        off.chunk_length,
        golden.chunk_length,
        info.chunk_length,
    );
    assert_field(
        &ci_path,
        &raw,
        "max_compressed_length",
        off.max_compressed_length,
        golden.max_compressed_length,
        info.max_compressed_length,
    );
    assert_field(
        &ci_path,
        &raw,
        "data_length (total uncompressed)",
        off.data_length,
        golden.total_uncompressed_length,
        info.data_length,
    );
    assert_field(
        &ci_path,
        &raw,
        "chunk_count",
        off.chunk_count,
        golden.chunk_count,
        info.chunk_offsets.len(),
    );

    // --- exact ordered offset sequence (full vector compare) ---
    let golden_offsets: Vec<u64> = golden.chunks.iter().map(|c| c.offset).collect();
    if info.chunk_offsets != golden_offsets {
        // find first divergent index for a precise byte offset + hexdump
        let bad = (0..info.chunk_offsets.len().max(golden_offsets.len()))
            .find(|&i| info.chunk_offsets.get(i) != golden_offsets.get(i))
            .unwrap_or(0);
        let byte = off.first_offset + bad * 8;
        assert_field(
            &ci_path,
            &raw,
            &format!("chunk_offsets[{bad}]"),
            byte,
            golden_offsets.get(bad).copied(),
            info.chunk_offsets.get(bad).copied(),
        );
        // vector equality (should fail here if individual index slipped through)
        assert_field(
            &ci_path,
            &raw,
            "chunk_offsets (full vector)",
            off.first_offset,
            golden_offsets.clone(),
            info.chunk_offsets.clone(),
        );
    }

    // --- derive per-chunk on-disk size from adjacent offsets; final from Data.db size ---
    let data_db_size = golden.data_db_size_bytes;
    for i in 0..info.chunk_offsets.len() {
        // golden rows are index-ordered; confirm the per-chunk row idx matches position
        assert_eq!(
            golden.chunks[i].idx, i,
            "{table}: golden chunk row idx {} out of order at position {i}",
            golden.chunks[i].idx
        );
        let derived = info
            .compressed_chunk_size(i, data_db_size)
            .unwrap_or_else(|| panic!("{table}: compressed_chunk_size({i}) returned None"));
        let byte = off.first_offset + i * 8;
        assert_field(
            &ci_path,
            &raw,
            &format!("derived on_disk_len[{i}] (next_offset - offset)"),
            byte,
            golden.chunks[i].on_disk_len,
            derived,
        );
        // compressed payload length = on-disk length - 4-byte trailing CRC word
        assert_field(
            &ci_path,
            &raw,
            &format!("derived comp_len[{i}] (on_disk_len - 4 CRC word)"),
            byte,
            golden.chunks[i].comp_len,
            derived - 4,
        );
    }

    // --- offsets strictly ascending (mirrors CompressionMetadata invariant) ---
    for i in 1..info.chunk_offsets.len() {
        assert!(
            info.chunk_offsets[i] > info.chunk_offsets[i - 1],
            "{table}: chunk offsets not strictly ascending at index {i}: {} <= {}",
            info.chunk_offsets[i],
            info.chunk_offsets[i - 1]
        );
    }

    // --- LAYOUT: offsets only, NO inline CRC trailers / chunk-CRC arrays ---
    // CompressionInfo.db ends EXACTLY after the last 8-byte offset. Per-chunk CRC32
    // words live inline in Data.db, never here. Any trailing bytes => spurious CRC array.
    let expected_len = off.first_offset + info.chunk_offsets.len() * 8;
    assert_field(
        &ci_path,
        &raw,
        "file length (offsets only — no trailing CRC array / chunk-CRC trailers)",
        expected_len,
        expected_len,
        raw.len(),
    );

    // --- derive final chunk UNCOMPRESSED length from total - last_offset_uncompressed ---
    // raw_uncompressed_len for chunk i = chunk_length, except the final chunk which
    // covers (total_uncompressed_length - i*chunk_length).
    let last = info.chunk_offsets.len() - 1;
    let final_uncompressed = info.data_length - (last as u64) * (info.chunk_length as u64);
    assert_field(
        &ci_path,
        &raw,
        "derived final-chunk uncompressed length",
        off.chunk_count,
        golden.chunks[last].raw_uncompressed_len,
        final_uncompressed,
    );

    // --- golden self-consistency: short_final + raw_stored count (cross-check) ---
    let golden_short_final = golden.chunks[last].raw_uncompressed_len < golden.chunk_length as u64;
    assert_eq!(
        golden_short_final, golden.short_final_chunk,
        "{table}: golden short_final_chunk flag inconsistent with final raw_uncompressed_len"
    );
    let golden_raw_count = golden.chunks.iter().filter(|c| c.raw_stored).count();
    assert_eq!(
        golden_raw_count, golden.raw_stored_chunk_count,
        "{table}: golden raw_stored_chunk_count inconsistent with per-chunk raw_stored flags"
    );

    Some((
        info.algorithm.clone(),
        info.chunk_offsets.len(),
        final_uncompressed,
    ))
}

// ----------------------------------------------------------------------------
// Per-fixture tests: one per compressor, pinning the exact Cassandra name string
// ----------------------------------------------------------------------------

#[test]
fn lz4_table_compressioninfo_strict_parity() {
    if let Some((algo, chunks, final_len)) = run_compressed_fixture("lz4_table", "LZ4Compressor") {
        assert_eq!(algo, "LZ4Compressor");
        assert!(
            chunks > 0,
            "lz4_table: zero chunks compared (present-but-empty)"
        );
        eprintln!(
            "lz4_table: algorithm={algo} chunk_count={chunks} final_uncompressed_len={final_len}"
        );
    }
}

#[test]
fn snappy_table_compressioninfo_strict_parity() {
    if let Some((algo, chunks, final_len)) =
        run_compressed_fixture("snappy_table", "SnappyCompressor")
    {
        assert_eq!(algo, "SnappyCompressor");
        assert!(chunks > 0, "snappy_table: zero chunks compared");
        eprintln!("snappy_table: algorithm={algo} chunk_count={chunks} final_uncompressed_len={final_len}");
    }
}

#[test]
fn deflate_table_compressioninfo_strict_parity() {
    if let Some((algo, chunks, final_len)) =
        run_compressed_fixture("deflate_table", "DeflateCompressor")
    {
        assert_eq!(algo, "DeflateCompressor");
        assert!(chunks > 0, "deflate_table: zero chunks compared");
        eprintln!("deflate_table: algorithm={algo} chunk_count={chunks} final_uncompressed_len={final_len}");
    }
}

#[test]
fn zstd_table_compressioninfo_strict_parity() {
    if let Some((algo, chunks, final_len)) = run_compressed_fixture("zstd_table", "ZstdCompressor")
    {
        assert_eq!(algo, "ZstdCompressor");
        assert!(chunks > 0, "zstd_table: zero chunks compared");
        eprintln!(
            "zstd_table: algorithm={algo} chunk_count={chunks} final_uncompressed_len={final_len}"
        );
    }
}

/// `short_final_chunk`: the LAST chunk's derived uncompressed length must be strictly
/// less than `chunk_length` (the scenario this fixture is built to exercise).
#[test]
fn short_final_chunk_final_chunk_is_short() {
    let table = "short_final_chunk";
    let dir = match resolve_fixture_dir(table) {
        Some(d) => d,
        None => {
            eprintln!("SKIP {table}: dataset root unset or dir absent");
            return;
        }
    };
    let ci_path = dir.join("nb-1-big-CompressionInfo.db");
    if !ci_path.exists() {
        eprintln!("SKIP {table}: {} missing", ci_path.display());
        return;
    }

    // Full strict battery first (algorithm + every field + offsets).
    let (algo, chunks, final_len) =
        run_compressed_fixture(table, "LZ4Compressor").expect("short_final_chunk fixture present");

    let raw = std::fs::read(&ci_path).expect("read CompressionInfo.db");
    let info = CompressionInfo::parse(&raw).expect("parse short_final_chunk");

    // The defining assertion: derived final uncompressed length < chunk_length.
    assert!(
        final_len < info.chunk_length as u64,
        "{table}: expected SHORT final chunk (< chunk_length={}), got final_uncompressed_len={final_len}",
        info.chunk_length
    );
    eprintln!(
        "short_final_chunk: algorithm={algo} chunk_count={chunks} final_uncompressed_len={final_len} < chunk_length={}",
        info.chunk_length
    );
}

/// `incompressible_uncompressed_chunk`: full strict parity. This fixture sets
/// `max_compressed_length == chunk_length` (raw-fallback enabled); we assert that
/// header field exactly and confirm the offset deltas reflect raw-stored chunks
/// (on-disk len == chunk_length + 4 CRC word for all but the short final chunk).
#[test]
fn incompressible_uncompressed_chunk_strict_parity() {
    let table = "incompressible_uncompressed_chunk";
    if let Some((algo, chunks, final_len)) = run_compressed_fixture(table, "LZ4Compressor") {
        assert_eq!(algo, "LZ4Compressor");
        let dir = resolve_fixture_dir(table).expect("dir");
        let raw = std::fs::read(dir.join("nb-1-big-CompressionInfo.db")).expect("read");
        let info = CompressionInfo::parse(&raw).expect("parse");
        // raw-fallback enabled: max_compressed_length equals chunk_length (not i32::MAX).
        assert_eq!(
            info.max_compressed_length, info.chunk_length,
            "{table}: raw-fallback fixture must have max_compressed_length == chunk_length"
        );
        eprintln!("incompressible_uncompressed_chunk: algorithm={algo} chunk_count={chunks} final_uncompressed_len={final_len} max_compressed_length={}", info.max_compressed_length);
    }
}

/// `uncompressed_table`: compression disabled => NO CompressionInfo.db component.
/// Assert the file is absent and that the directory instead carries CRC.db.
#[test]
fn uncompressed_table_has_no_compressioninfo() {
    let table = "uncompressed_table";
    let dir = match resolve_fixture_dir(table) {
        Some(d) => d,
        None => {
            eprintln!("SKIP {table}: dataset root unset or dir absent");
            return;
        }
    };
    // The directory itself must exist for a meaningful assertion; require Data.db so
    // we don't silently pass against an un-fetched fixture.
    let data_db = dir.join("nb-1-big-Data.db");
    if !data_db.exists() {
        eprintln!(
            "SKIP {table}: {} missing (binary not fetched)",
            data_db.display()
        );
        return;
    }

    let ci_path = dir.join("nb-1-big-CompressionInfo.db");
    assert!(
        !ci_path.exists(),
        "{table}: compression-disabled fixture must NOT have a CompressionInfo.db, found {}",
        ci_path.display()
    );

    // And it must carry a component-level CRC.db instead (the uncompressed-path checksum).
    let crc_db = dir.join("nb-1-big-CRC.db");
    assert!(
        crc_db.exists(),
        "{table}: uncompressed fixture should carry CRC.db (got TOC without it) at {}",
        crc_db.display()
    );
    eprintln!(
        "uncompressed_table: no CompressionInfo.db (CRC.db present) — absence handled correctly"
    );
}
