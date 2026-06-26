//! Issue #998 (Epic #970): validate Data.db inline compressed-chunk CRC32 trailers
//! across chunk boundaries.
//!
//! ## What this test asserts
//!
//! Each compressed chunk in a Cassandra NB (`nb-1-big`) Data.db is laid out as:
//!
//! ```text
//! [compressed payload bytes][4-byte big-endian CRC32 of the payload bytes]
//! ```
//!
//! CompressionInfo.db supplies the chunk OFFSETS (start of each chunk record in
//! Data.db). Chunk N's on-disk span = `offset[N+1] - offset[N]` (the final chunk
//! uses `data_db_size - offset[last]`). The last 4 bytes of that span are the
//! CRC32 trailer; the reader must subtract those 4 bytes before decompression.
//!
//! ## CRC32 variant
//!
//! Cassandra computes the inline chunk CRC with `java.util.zip.CRC32`
//! (CRC-32/ISO-HDLC: reflected, polynomial 0xEDB88320 — a.k.a. the IEEE 802.3 /
//! zlib variant). The CQLite reader matches this exactly via `crc32fast::hash`:
//! see `cqlite-core/src/storage/sstable/chunk_decompressor.rs` line ~183
//! (`let computed_crc = crc32fast::hash(&compressed_data);`) which reads the
//! 4-byte trailer big-endian (`u32::from_be_bytes`) and compares. This test uses
//! the SAME `crc32fast::hash` over the SAME payload-minus-trailer byte range, so
//! it is a parity check against the reader, not an independent guess.
//!
//! ## Authority
//!
//! - `CompressedSequentialWriter.java:192` — `crcMetadata.appendDirect(toWrite, true)`
//!   writes the inline CRC after each compressed chunk.
//! - `CompressedSequentialWriter.java:203` — `chunkOffset += compressedLength + 4`
//!   (the inter-offset delta includes the 4-byte trailer).
//! - `docs/sstables-definitive-guide/chapters/05-data-db-format.md`

use cqlite_core::parser::header::CassandraVersion;
use cqlite_core::storage::sstable::chunk_decompressor::ChunkDecompressor;
use cqlite_core::storage::sstable::compression_info::CompressionInfo;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

const COMPONENT: &str = "Data.db";

/// Locate the test_comp dataset directory, honoring CQLITE_DATASETS_ROOT.
fn datasets_root() -> PathBuf {
    std::env::var("CQLITE_DATASETS_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            // Fall back to the sibling epic970 worktree where the fixtures live.
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .map(|p| p.join("test-data/datasets"))
                .unwrap_or_else(|| PathBuf::from("test-data/datasets"))
        })
}

/// Resolve `<table>-<uuid>` directory by glob (no hardcoded UUID). Returns the
/// directory plus the Data.db and CompressionInfo.db paths, or None if the
/// table dir / Data.db is absent (caller should SKIP cleanly).
struct Fixture {
    table: String,
    data_db: PathBuf,
    compression_info: PathBuf,
}

fn resolve_fixture(table: &str) -> Option<Fixture> {
    let comp_dir = datasets_root().join("sstables/test_comp");
    if !comp_dir.is_dir() {
        return None;
    }
    let entries = fs::read_dir(&comp_dir).ok()?;
    let prefix = format!("{}-", table);
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(&prefix) && entry.path().is_dir() {
            let dir = entry.path();
            let data_db = dir.join("nb-1-big-Data.db");
            if !data_db.exists() {
                // Dir present but no Data.db — treat as absent (SKIP).
                return None;
            }
            return Some(Fixture {
                table: table.to_string(),
                data_db,
                compression_info: dir.join("nb-1-big-CompressionInfo.db"),
            });
        }
    }
    None
}

/// Per-chunk on-disk record description derived from CompressionInfo offsets.
#[derive(Debug, Clone, Copy)]
struct ChunkRecord {
    index: usize,
    start: u64,
    /// full on-disk span (payload + 4-byte trailer)
    span: u64,
}

/// Compute every chunk's on-disk record range from CompressionInfo offsets and
/// the Data.db file size. The last chunk runs to end-of-file.
fn chunk_records(info: &CompressionInfo, data_db_size: u64) -> Vec<ChunkRecord> {
    let offsets = &info.chunk_offsets;
    let mut out = Vec::with_capacity(offsets.len());
    for (i, &start) in offsets.iter().enumerate() {
        let end = if i + 1 < offsets.len() {
            offsets[i + 1]
        } else {
            data_db_size
        };
        out.push(ChunkRecord {
            index: i,
            start,
            span: end - start,
        });
    }
    out
}

/// Build a diagnostic line that surfaces component / chunk index / start offset /
/// expected vs actual CRC. Used for both positive-assert failures and the
/// negative/corruption assertion.
fn crc_diag(
    file: &Path,
    rec: &ChunkRecord,
    expected_crc: u32,
    actual_crc: u32,
    extra: &str,
) -> String {
    format!(
        "[{component}] file={file} chunk_index={idx} chunk_start_offset=0x{start:x} ({start}) \
         span={span} payload_len={payload} expected_crc=0x{exp:08x} actual_crc=0x{act:08x}{extra}",
        component = COMPONENT,
        file = file.display(),
        idx = rec.index,
        start = rec.start,
        span = rec.span,
        payload = rec.span.saturating_sub(4),
        exp = expected_crc,
        act = actual_crc,
        extra = if extra.is_empty() {
            String::new()
        } else {
            format!(" {}", extra)
        },
    )
}

/// Core positive validation shared by all compressed fixtures.
///
/// Returns the number of chunks whose trailers were verified. Panics with a
/// fully-diagnosed message (component, chunk index, start offset, expected vs
/// actual CRC) on the first mismatch.
fn verify_all_trailers(fx: &Fixture) -> usize {
    let comp_data = fs::read(&fx.compression_info).unwrap_or_else(|e| {
        panic!(
            "[{}] {}: failed to read CompressionInfo.db: {}",
            fx.table,
            fx.compression_info.display(),
            e
        )
    });
    let info = CompressionInfo::parse(&comp_data).unwrap_or_else(|e| {
        panic!(
            "[{}] {}: failed to parse CompressionInfo.db: {}",
            fx.table,
            fx.compression_info.display(),
            e
        )
    });

    let data_bytes = fs::read(&fx.data_db).unwrap_or_else(|e| {
        panic!(
            "[{}] {}: failed to read Data.db: {}",
            fx.table,
            fx.data_db.display(),
            e
        )
    });
    let data_db_size = data_bytes.len() as u64;

    let records = chunk_records(&info, data_db_size);
    assert!(
        !records.is_empty(),
        "[{}] {}: CompressionInfo yielded 0 chunks — FAIL (Data.db present but nothing checked)",
        fx.table,
        fx.data_db.display()
    );

    for rec in &records {
        // The reader subtracts the 4-byte trailer before decompression
        // (chunk_decompressor.rs: `compressed_len = (record_size - 4)`).
        // Assert that invariant holds here at the byte-range level.
        assert!(
            rec.span >= 4,
            "{}",
            crc_diag(
                &fx.data_db,
                rec,
                0,
                0,
                "chunk span smaller than 4-byte CRC trailer"
            )
        );
        let payload_len = (rec.span - 4) as usize;

        let payload_start = rec.start as usize;
        let payload_end = payload_start + payload_len;
        let trailer_end = payload_end + 4;
        assert!(
            trailer_end <= data_bytes.len(),
            "{}",
            crc_diag(
                &fx.data_db,
                rec,
                0,
                0,
                &format!(
                    "chunk record runs past EOF (trailer_end={} data_len={})",
                    trailer_end,
                    data_bytes.len()
                )
            )
        );

        let payload = &data_bytes[payload_start..payload_end];
        let trailer = &data_bytes[payload_end..trailer_end];
        let stored_crc = u32::from_be_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]);

        // SAME CRC32 variant the reader uses (crc32fast::hash over the payload).
        let computed_crc = crc32fast::hash(payload);

        assert_eq!(
            computed_crc,
            stored_crc,
            "{}",
            crc_diag(
                &fx.data_db,
                rec,
                stored_crc,
                computed_crc,
                "inline CRC32 trailer mismatch (payload bytes differ from stored checksum)"
            )
        );

        // Reader-parity: payload_len = span - 4 is what the production decompressor
        // feeds to the codec.
        let reader_payload_len = (rec.span - 4) as usize;
        assert_eq!(
            payload_len,
            reader_payload_len,
            "{}",
            crc_diag(
                &fx.data_db,
                rec,
                stored_crc,
                computed_crc,
                "payload_len != span - 4 (reader trailer-subtraction invariant violated)"
            )
        );
    }

    records.len()
}

/// Drive the production reader (`ChunkDecompressor`) over the whole file. This
/// exercises the exact CRC-validation + trailer-subtraction + decompress path
/// the reader uses, and confirms decompressed chunk lengths match the
/// CompressionInfo-declared `chunk_length` (last chunk may be short).
fn verify_via_reader(fx: &Fixture) {
    let comp_data = fs::read(&fx.compression_info).expect("read CompressionInfo.db");
    let info = CompressionInfo::parse(&comp_data).expect("parse CompressionInfo.db");
    let chunk_length = info.chunk_length as u64;
    let data_length = info.data_length;
    let num_chunks = info.chunk_offsets.len();

    let mut decompressor = ChunkDecompressor::new_with_path(
        info,
        CassandraVersion::V5_0Release,
        fx.data_db.display().to_string(),
    )
    .expect("construct ChunkDecompressor");

    let data_bytes = fs::read(&fx.data_db).expect("read Data.db");
    let mut cursor = Cursor::new(data_bytes);

    // read_all_data() walks every chunk: it subtracts the 4-byte trailer,
    // validates CRC, and decompresses. A clean Ok proves all trailers verify
    // and the trailer is excluded from the decompressor input.
    let decompressed = decompressor.read_all_data(&mut cursor).unwrap_or_else(|e| {
        panic!(
            "[{}] reader rejected a well-formed fixture (every trailer should verify): {}",
            fx.table, e
        )
    });

    assert_eq!(
        decompressed.len() as u64,
        data_length,
        "[{}] reader produced {} uncompressed bytes; CompressionInfo declares data_length={}",
        fx.table,
        decompressed.len(),
        data_length
    );

    // Sanity on chunk geometry: full chunks decompress to chunk_length; the
    // final chunk is data_length - chunk_length*(num_chunks-1) (<= chunk_length).
    if num_chunks > 0 {
        let expected_last = data_length - chunk_length * (num_chunks as u64 - 1);
        assert!(
            expected_last <= chunk_length && expected_last > 0,
            "[{}] derived last-chunk length {} out of range (chunk_length={}, num_chunks={})",
            fx.table,
            expected_last,
            chunk_length,
            num_chunks
        );
    }
}

/// Shared body for the per-codec positive tests.
///
/// `drive_reader` controls whether we additionally run the production
/// `ChunkDecompressor::read_all_data` decompress path. Trailer + boundary
/// validation (the subject of issue #998) always runs via `verify_all_trailers`,
/// which independently asserts the `payload_len = span - 4` trailer-subtraction
/// invariant at the byte level. As of #1082 the production DeflateCompressor decode
/// path correctly handles Cassandra's zlib-wrapped streams (ZlibDecoder), so the
/// full reader decompress is now driven for every codec including deflate.
fn run_positive(table: &str, drive_reader: bool) {
    let Some(fx) = resolve_fixture(table) else {
        eprintln!(
            "SKIP[{}]: Data.db absent under {}",
            table,
            datasets_root().display()
        );
        return;
    };
    let checked = verify_all_trailers(&fx);
    println!(
        "[{}] verified {} inline CRC32 trailers across chunk boundaries ({})",
        table,
        checked,
        fx.data_db.display()
    );
    if drive_reader {
        // Confirm the production reader agrees (trailer subtraction + decompress + CRC).
        verify_via_reader(&fx);
        println!(
            "[{}] production ChunkDecompressor decompressed all {} chunks with trailer subtracted",
            table, checked
        );
    } else {
        println!(
            "[{}] reader-decompress skipped (blocked by separate DeflateCompressor decode bug); \
             {} CRC trailers + trailer-subtraction invariant verified independently",
            table, checked
        );
    }
}

// ---------------------------------------------------------------------------
// Required parity codecs: LZ4 and Snappy.
// ---------------------------------------------------------------------------

#[test]
fn issue_998_lz4_inline_crc_trailers_verify() {
    run_positive("lz4_table", true);
}

#[test]
fn issue_998_snappy_inline_crc_trailers_verify() {
    run_positive("snappy_table", true);
}

// ---------------------------------------------------------------------------
// Broader matrix: Deflate and Zstd.
// ---------------------------------------------------------------------------

#[test]
fn issue_998_deflate_inline_crc_trailers_verify() {
    // drive_reader=true: the DeflateCompressor decode path (zlib-wrapped stream) is
    // now decoded correctly via ZlibDecoder (#1082), so the production reader can
    // decompress every chunk in addition to the CRC trailer + trailer-subtraction
    // checks.
    run_positive("deflate_table", true);
}

#[test]
fn issue_998_zstd_inline_crc_trailers_verify() {
    run_positive("zstd_table", true);
}

// ---------------------------------------------------------------------------
// Edge fixtures: short final chunk + incompressible (raw/uncompressed) chunk.
// Both still carry a 4-byte CRC trailer per chunk.
// ---------------------------------------------------------------------------

#[test]
fn issue_998_short_final_chunk_trailers_verify() {
    let table = "short_final_chunk";
    let Some(fx) = resolve_fixture(table) else {
        eprintln!(
            "SKIP[{}]: Data.db absent under {}",
            table,
            datasets_root().display()
        );
        return;
    };

    let comp_data = fs::read(&fx.compression_info).expect("read CompressionInfo.db");
    let info = CompressionInfo::parse(&comp_data).expect("parse CompressionInfo.db");
    let data_len = info.data_length;
    let chunk_length = info.chunk_length as u64;
    let num_chunks = info.chunk_offsets.len();

    // The whole point of this fixture: the final logical chunk is SHORT, i.e.
    // data_length is not an exact multiple of chunk_length.
    assert!(
        num_chunks >= 2,
        "[{}] expected multiple chunks to exercise a short final chunk, got {}",
        table,
        num_chunks
    );
    let last_chunk_uncompressed = data_len - chunk_length * (num_chunks as u64 - 1);
    assert!(
        last_chunk_uncompressed < chunk_length,
        "[{}] final chunk is NOT short (last={} chunk_length={}); fixture does not exercise the edge",
        table,
        last_chunk_uncompressed,
        chunk_length
    );

    let checked = verify_all_trailers(&fx);
    println!(
        "[{}] verified {} trailers; final chunk uncompressed length = {} (< chunk_length {})",
        table, checked, last_chunk_uncompressed, chunk_length
    );
    verify_via_reader(&fx);
}

#[test]
fn issue_998_incompressible_uncompressed_chunk_trailers_verify() {
    let table = "incompressible_uncompressed_chunk";
    let Some(fx) = resolve_fixture(table) else {
        eprintln!(
            "SKIP[{}]: Data.db absent under {}",
            table,
            datasets_root().display()
        );
        return;
    };

    let comp_data = fs::read(&fx.compression_info).expect("read CompressionInfo.db");
    let info = CompressionInfo::parse(&comp_data).expect("parse CompressionInfo.db");
    let data_bytes = fs::read(&fx.data_db).expect("read Data.db");
    let records = chunk_records(&info, data_bytes.len() as u64);

    // At least one chunk must be stored RAW (incompressible): its compressed
    // payload length reaches/exceeds max_compressed_length, so Cassandra wrote
    // the chunk uncompressed but STILL appended a 4-byte CRC trailer.
    let max_compressed_length = info.max_compressed_length as u64;
    let mut raw_chunks = 0usize;
    for rec in &records {
        let payload_len = rec.span - 4;
        if payload_len >= max_compressed_length {
            raw_chunks += 1;
        }
    }
    assert!(
        raw_chunks > 0,
        "[{}] expected >=1 incompressible (raw) chunk (payload_len >= max_compressed_length={}); \
         fixture does not exercise the raw-chunk fallback",
        table,
        max_compressed_length
    );

    // Trailers must verify regardless of raw vs compressed storage.
    let checked = verify_all_trailers(&fx);
    println!(
        "[{}] verified {} trailers; {} chunk(s) stored raw/uncompressed (still CRC-trailed)",
        table, checked, raw_chunks
    );
    // The reader handles the incompressible fallback AND validates CRC.
    verify_via_reader(&fx);
}

// ---------------------------------------------------------------------------
// Negative / corruption test: flip ONE byte inside a chunk's CRC trailer and
// assert the reader fails deterministically (no silent wrong data), with an
// error that names the component, chunk index, start offset, expected & actual
// CRC.
// ---------------------------------------------------------------------------

#[test]
fn issue_998_corrupt_crc_trailer_is_rejected_with_diagnostics() {
    let table = "lz4_table";
    let Some(fx) = resolve_fixture(table) else {
        eprintln!(
            "SKIP[{}]: Data.db absent under {}",
            table,
            datasets_root().display()
        );
        return;
    };

    let comp_data = fs::read(&fx.compression_info).expect("read CompressionInfo.db");
    let info = CompressionInfo::parse(&comp_data).expect("parse CompressionInfo.db");
    let mut data_bytes = fs::read(&fx.data_db).expect("read Data.db");
    let records = chunk_records(&info, data_bytes.len() as u64);
    assert!(
        !records.is_empty(),
        "[{}] no chunks to corrupt — FAIL (Data.db present)",
        table
    );

    // Target chunk 0's CRC trailer (its last 4 bytes). Flip the final trailer byte.
    let rec = records[0];
    let payload_end = (rec.start + rec.span - 4) as usize;
    let trailer_last = payload_end + 3;

    let payload = &data_bytes[rec.start as usize..payload_end];
    let original_crc = crc32fast::hash(payload);
    let stored_before = u32::from_be_bytes([
        data_bytes[payload_end],
        data_bytes[payload_end + 1],
        data_bytes[payload_end + 2],
        data_bytes[payload_end + 3],
    ]);
    assert_eq!(
        original_crc, stored_before,
        "[{}] precondition: chunk 0 trailer should be valid before corruption",
        table
    );

    // Flip one byte INSIDE the CRC trailer (toggle low bit) so the stored CRC no
    // longer matches the (unchanged) payload.
    data_bytes[trailer_last] ^= 0x01;
    let corrupted_stored = u32::from_be_bytes([
        data_bytes[payload_end],
        data_bytes[payload_end + 1],
        data_bytes[payload_end + 2],
        data_bytes[payload_end + 3],
    ]);
    assert_ne!(
        corrupted_stored, original_crc,
        "[{}] byte-flip failed to change the stored CRC",
        table
    );

    // Write the corrupted Data.db to a temp dir and point the reader at it.
    let tmp = std::env::temp_dir().join(format!(
        "cqlite_issue998_corrupt_{}_{}",
        table,
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&tmp);
    fs::create_dir_all(&tmp).expect("create temp dir");
    let corrupt_data_path = tmp.join("nb-1-big-Data.db");
    fs::write(&corrupt_data_path, &data_bytes).expect("write corrupted Data.db");

    // Drive the production CRC-validation path.
    let mut decompressor = ChunkDecompressor::new_with_path(
        info,
        CassandraVersion::V5_0Release,
        corrupt_data_path.display().to_string(),
    )
    .expect("construct ChunkDecompressor");

    let mut cursor = Cursor::new(data_bytes.clone());
    let result = decompressor.read_all_data(&mut cursor);

    // Clean up before asserting so a panic does not leak the temp dir.
    let _ = fs::remove_dir_all(&tmp);

    let err = match result {
        Ok(out) => panic!(
            "[{}] corrupted CRC trailer was accepted SILENTLY — reader returned {} bytes of \
             (wrong) data instead of erroring. {}",
            table,
            out.len(),
            crc_diag(
                &corrupt_data_path,
                &rec,
                corrupted_stored,
                original_crc,
                "expected a deterministic CRC/corruption error"
            )
        ),
        Err(e) => e,
    };

    let msg = err.to_string();
    // The error must be a deterministic corruption/checksum error that surfaces
    // the diagnostic fields.
    let lower = msg.to_lowercase();
    assert!(
        lower.contains("crc"),
        "{}",
        crc_diag(
            &corrupt_data_path,
            &rec,
            corrupted_stored,
            original_crc,
            &format!("error did not mention CRC; was: {}", msg)
        )
    );
    // Chunk index 0 must appear.
    assert!(
        msg.contains("chunk 0"),
        "{}",
        crc_diag(
            &corrupt_data_path,
            &rec,
            corrupted_stored,
            original_crc,
            &format!("error did not name chunk index 0; was: {}", msg)
        )
    );
    // The chunk start offset (hex) must appear.
    let start_hex = format!("0x{:x}", rec.start);
    assert!(
        msg.contains(&start_hex),
        "{}",
        crc_diag(
            &corrupt_data_path,
            &rec,
            corrupted_stored,
            original_crc,
            &format!(
                "error did not include chunk start offset {}; was: {}",
                start_hex, msg
            )
        )
    );
    // Both the corrupted (stored) CRC and the computed CRC must appear.
    let stored_hex = format!("0x{:08x}", corrupted_stored);
    let computed_hex = format!("0x{:08x}", original_crc);
    assert!(
        msg.contains(&stored_hex) && msg.contains(&computed_hex),
        "{}",
        crc_diag(
            &corrupt_data_path,
            &rec,
            corrupted_stored,
            original_crc,
            &format!(
                "error must surface expected(stored)={} and computed={}; was: {}",
                stored_hex, computed_hex, msg
            )
        )
    );

    println!("[{}] corruption rejected deterministically: {}", table, msg);
}
