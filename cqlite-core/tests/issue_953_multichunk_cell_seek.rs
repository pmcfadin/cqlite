//! Issue #953 (Epic #951) MEDIUM regression: the within-SSTable seek must bound
//! its decompression window by an AUTHORITATIVE next-partition offset, and must
//! return a target ROW that SPANS MULTIPLE COMPRESSION CHUNKS intact — never
//! truncated or empty.
//!
//! ## The bug being pinned
//!
//! The previous size-free BTI/BIG bound used a ROW-COUNT STABILITY guard: it
//! stopped appending chunks once a different-key row appeared AND the target-key
//! row count had not grown since the last appended chunk. That guard is itself a
//! heuristic. When a chunk completes NO new target row — e.g. a single row whose
//! cells span MULTIPLE compression chunks, so the row is still mid-decode — the
//! count is "stable", and any garbage/next-key artifact the parser sees in the
//! still-truncated tail is falsely accepted as a next-partition boundary while the
//! target partition is incomplete. Result: a TRUNCATED or EMPTY result for an
//! existing partition. The fix replaces the guard with the SUCCESSOR partition's
//! offset (the next trie/index entry) as the exclusive end, decompressing exactly
//! the chunks covering `[offset, end)` so the whole multi-chunk row is materialised
//! before the single parse.
//!
//! ## Fixture: real BTI `test_da.wide_table`, RE-COMPRESSED at a tiny chunk size
//!
//! The fixture is the real Cassandra 5.0 BTI (`da`) `test_da.wide_table`
//! (`pk int, ck int, payload text, PRIMARY KEY (pk, ck)`; 3 partitions × 300 rows
//! of ~2 KiB payload, LZ4). Using REAL data keeps the proper table metadata so the
//! within-SSTable seek actually engages (a WriteEngine SSTable reads back with
//! `table_name = "unknown"`, which the seek's strict wrong-table guard rejects, so
//! it cannot exercise the seek). The test:
//!   1. copies every component into a temp dir,
//!   2. DECOMPRESSES the LZ4 `Data.db` chunk-by-chunk into the raw data section,
//!   3. RE-COMPRESSES it at a 512-byte chunk size (so each ~2 KiB row spans ~4
//!      chunks — the multi-chunk-row case), overwriting `Data.db` and writing the
//!      matching `CompressionInfo.db` (every UNCOMPRESSED `Partitions.db` trie
//!      offset is preserved; only the physical chunk layout changes),
//!   4. ingests the temp dir and seeks each partition, asserting the rows come back
//!      complete and byte-identical to the full scan.
//!
//! Against the old stability-guard code the seek returns a TRUNCATED/EMPTY decode
//! for a partition whose rows span chunks (`partitions_decoded == 0`, masked by a
//! silent full-scan fallback); this test pins both the value parity AND
//! `partitions_decoded == 1`, so it fails on the old code and passes on the fix.
//!
//! Gated on cli-helpers + state_machine + lz4, excluded under `tombstones` (which
//! compiles out the seek + work counters). Requires `CQLITE_DATASETS_ROOT` with the
//! fetched binaries.
//!
//! ## Skip vs fail-closed (issue #1856)
//!
//! The `test_da/wide_table-*` fixture's `-Data.db`/`-CompressionInfo.db` binaries
//! are local-only (NOT in the fetchable `cassandra5-small-full-v3.4` asset). The
//! fixture *directory* can exist on a fresh checkout carrying only the committed
//! JSONL/CRC/TOC while those binaries are absent — the NORMAL state of a worktree
//! gate. In that state the test SKIPS (honest eprintln naming the absent binary),
//! never panics. It hard-FAILS (fail-closed) only when
//! `CQLITE_PARITY_REQUIRE_DATASETS=1` is set (the required parity gate), so that
//! gate can never green-pass without actually running. When the binary IS present,
//! a 0-row full scan is a genuine read regression (failure), never a skip.

#![cfg(all(
    feature = "write-support",
    feature = "cli-helpers",
    feature = "state_machine",
    feature = "lz4",
    not(feature = "tombstones")
))]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::QueryRow;
use cqlite_core::storage::sstable::compression::{Compression, CompressionAlgorithm};
use cqlite_core::storage::sstable::compression_info::CompressionInfo;
use cqlite_core::storage::sstable::work_counters;
use cqlite_core::storage::sstable::writer::CompressionAlgorithm as WriterCompressionAlgorithm;
use cqlite_core::storage::sstable::writer::CompressionInfoWriter;
use cqlite_core::storage::sstable::writer::{create_compressor, CompressedDataWriter};
use cqlite_core::{Database, Value};
use tempfile::TempDir;

const QUALIFIED_TABLE: &str = "test_da.wide_table";

/// Re-compression chunk size. 512 bytes << the ~2 KiB payload row, so each row's
/// cell spans ~4 compression chunks — the multi-chunk-row case the removed
/// row-count-stability guard truncated.
const REPACK_CHUNK_SIZE: usize = 512;

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

/// CI fail-closed switch (issue #1856, mirrors #1242). The required parity gate
/// sets `CQLITE_PARITY_REQUIRE_DATASETS=1`; in that mode an absent fixture must
/// PANIC rather than silently skip and green-pass. Locally (env unset) the test
/// keeps its skip-on-absence behavior.
fn parity_datasets_required() -> bool {
    std::env::var("CQLITE_PARITY_REQUIRE_DATASETS")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Skip (honest eprintln) when local, but FAIL-CLOSED (panic) under
/// `CQLITE_PARITY_REQUIRE_DATASETS=1`. `reason` names the actual cause (e.g. the
/// absent `-Data.db` binary) so the log/panic can never be misleading (#1853).
fn skip_or_fail_closed(reason: &str) {
    if parity_datasets_required() {
        panic!(
            "multichunk_row_seek_returns_full_partition: \
             CQLITE_PARITY_REQUIRE_DATASETS=1 but {reason} — required parity gate cannot \
             green-pass without running fail-closed (issue #1856)"
        );
    }
    eprintln!("Skipping (multi-chunk-row seek): {reason}");
}

fn schemas_dir() -> Option<PathBuf> {
    if let Some(root) = datasets_root() {
        if let Some(dir) = root.parent().and_then(|p| {
            let d = p.join("schemas");
            d.exists().then_some(d)
        }) {
            return Some(dir);
        }
    }
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = manifest_dir.parent()?.join("test-data").join("schemas");
    dir.exists().then_some(dir)
}

/// Locate the real `test_da/wide_table-<uuid>/` SSTable directory that actually
/// carries the local-only `-Data.db` binary.
///
/// A fresh checkout can have the directory present with only the committed
/// JSONL/CRC/TOC and NO `-Data.db` (the binary is not in the fetchable asset), so
/// the presence check requires the binary itself — not just the directory — to
/// avoid a false `Some` that later hard-panics (issue #1856).
fn real_wide_table_dir() -> Option<PathBuf> {
    let base = datasets_root()?.join("sstables").join("test_da");
    let entries = std::fs::read_dir(&base).ok()?;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("wide_table-") && dir_has_data_db(&path) {
                    return Some(path);
                }
            }
        }
    }
    None
}

/// True when `dir` contains a `*-Data.db` component (the local-only binary).
fn dir_has_data_db(dir: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    entries.flatten().any(|e| {
        e.path()
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.ends_with("-Data.db"))
    })
}

/// Decompress a chunked LZ4 `Data.db` into its raw (uncompressed) data section.
///
/// Each on-disk chunk record is `[lz4-size-prepended payload][crc32: 4 BE]`. The
/// record size (delta to the next chunk offset, or to EOF for the last chunk)
/// INCLUDES the trailing 4-byte CRC, so the LZ4 payload is the record minus its
/// last 4 bytes. Concatenating every decompressed chunk reproduces the exact bytes
/// the reader's `stitch_all_chunks` would yield.
fn decompress_data_section(compressed: &[u8], info: &CompressionInfo) -> Vec<u8> {
    let codec = Compression::new(CompressionAlgorithm::Lz4).expect("lz4 codec");
    let total = compressed.len() as u64;
    let mut raw = Vec::with_capacity(info.data_length as usize);
    for i in 0..info.chunk_offsets.len() {
        let start = info.compressed_chunk_offset(i).expect("chunk offset") as usize;
        let record_len = info.compressed_chunk_size(i, total).expect("chunk size") as usize;
        assert!(
            record_len >= 4,
            "chunk {i} record too small for a 4-byte CRC ({record_len} bytes)"
        );
        let payload = &compressed[start..start + record_len - 4];
        let chunk = codec.decompress(payload).expect("decompress chunk");
        raw.extend_from_slice(&chunk);
    }
    assert_eq!(
        raw.len() as u64,
        info.data_length,
        "decompressed data section length must equal CompressionInfo.data_length"
    );
    raw
}

/// Copy the real BTI `wide_table` SSTable into a temp dir, re-compress its Data.db
/// at `REPACK_CHUNK_SIZE`, and return the data directory to ingest. Returns `None`
/// (signalling a skip) when the dataset is absent.
fn build_repacked_sstable(temp: &TempDir) -> Option<PathBuf> {
    let src_dir = real_wide_table_dir()?;
    let src_name = src_dir.file_name()?.to_str()?.to_string();

    // Mirror the Cassandra layout the ingester expects:
    //   <data_dir>/test_da/wide_table-<uuid>/<components>
    let data_dir = temp.path().join("sstables");
    let dst_dir = data_dir.join("test_da").join(&src_name);
    std::fs::create_dir_all(&dst_dir).expect("create dst dir");

    let mut data_path: Option<PathBuf> = None;
    let mut info_path: Option<PathBuf> = None;
    for entry in std::fs::read_dir(&src_dir).expect("read src dir").flatten() {
        let src = entry.path();
        let Some(name) = src.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        // The `.jsonl` reference is not an SSTable component; skip it.
        if name.ends_with(".jsonl") {
            continue;
        }
        let dst = dst_dir.join(name);
        std::fs::copy(&src, &dst).expect("copy component");
        if name.ends_with("-Data.db") {
            data_path = Some(dst.clone());
        } else if name.ends_with("-CompressionInfo.db") {
            info_path = Some(dst.clone());
        }
    }
    // A fixture dir may exist with only the committed JSONL/CRC/TOC and no
    // local-only `-Data.db`/`-CompressionInfo.db` binary. Treat that as a genuine
    // absence (return `None` -> skip), never a panic (issue #1856). `real_wide_table_dir`
    // already requires `-Data.db`, so this is belt-and-suspenders for the pair.
    let (Some(data_path), Some(info_path)) = (data_path, info_path) else {
        return None;
    };

    // Decompress with the ORIGINAL CompressionInfo, then re-compress small-chunked.
    let original_compressed = std::fs::read(&data_path).expect("read Data.db");
    let original_info = {
        let bytes = std::fs::read(&info_path).expect("read CompressionInfo.db");
        CompressionInfo::parse(&bytes).expect("parse CompressionInfo.db")
    };
    let raw = decompress_data_section(&original_compressed, &original_info);

    let compressor = create_compressor(WriterCompressionAlgorithm::Lz4).expect("lz4 compressor");
    let mut writer = CompressedDataWriter::with_chunk_size(compressor, REPACK_CHUNK_SIZE);
    writer.write(&raw).expect("re-compress");
    let (repacked, metadata) = writer.finish().expect("finish re-compression");
    assert!(
        metadata.chunk_count() > original_info.chunk_offsets.len(),
        "small-chunk re-compression must produce MORE chunks than the original \
         (got {}, original {})",
        metadata.chunk_count(),
        original_info.chunk_offsets.len()
    );

    std::fs::write(&data_path, &repacked).expect("overwrite Data.db");
    CompressionInfoWriter::new(info_path)
        .write(&metadata)
        .expect("write small-chunk CompressionInfo.db");

    Some(data_dir)
}

async fn open_db(data_dir: &Path) -> Database {
    let schema_path = schemas_dir()
        .expect("schemas dir")
        .join("wide-table-bti.cql");
    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir: data_dir.to_path_buf(),
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some("/test_da/".to_string()),
    };
    let result = ingest(config).await.expect("ingest");
    assert!(
        result.schema_load_result.schemas_loaded > 0,
        "schema must load"
    );
    result.database
}

fn pk_value(row: &QueryRow) -> Option<i32> {
    match row.values.get("pk") {
        Some(Value::Integer(i)) => Some(*i),
        _ => None,
    }
}

fn row_fingerprint(row: &QueryRow) -> BTreeMap<String, String> {
    row.values
        .iter()
        .map(|(k, v)| (k.to_string(), format!("{v:?}")))
        .collect()
}

fn fingerprints(rows: &[QueryRow]) -> Vec<BTreeMap<String, String>> {
    let mut out: Vec<_> = rows.iter().map(row_fingerprint).collect();
    out.sort_by_key(|m| format!("{m:?}"));
    out
}

/// Issue #3890 (AC2): per-ROW column-presence parity against the scan oracle.
///
/// The row-count and fingerprint-set assertions above are set-level: they cannot
/// say WHICH row lost WHICH column. A seek row whose cell decode stopped part-way
/// (the #3890 truncation class) is missing every column after the failure point,
/// so pair each seek row with its scan row by clustering key (`ck`) and require the
/// expected column set to be present and equal, naming the divergence.
fn assert_columns_present_and_equal(pk: i32, got: &[QueryRow], want: &[QueryRow]) {
    let by_ck = |rows: &[QueryRow]| -> BTreeMap<i32, BTreeMap<String, String>> {
        rows.iter()
            .filter_map(|r| match r.values.get("ck") {
                Some(Value::Integer(ck)) => Some((*ck, row_fingerprint(r))),
                _ => None,
            })
            .collect()
    };
    let want_by_ck = by_ck(want);
    let got_by_ck = by_ck(got);
    assert_eq!(
        want_by_ck.len(),
        want.len(),
        "pk={pk}: every oracle row must carry an integer `ck` to pair on"
    );
    assert_eq!(
        got_by_ck.len(),
        got.len(),
        "pk={pk}: every seek row must carry an integer `ck` to pair on — a row that lost `ck` \
         is itself a truncated row (issue #3890)"
    );
    for (ck, want_cols) in &want_by_ck {
        let got_cols = got_by_ck.get(ck).unwrap_or_else(|| {
            panic!("pk={pk}: seek result is missing the row ck={ck} the full scan returned")
        });
        for (col, want_val) in want_cols {
            match got_cols.get(col) {
                Some(got_val) => assert_eq!(
                    got_val, want_val,
                    "pk={pk} ck={ck}: seek column '{col}' diverges from the full scan"
                ),
                None => panic!(
                    "pk={pk} ck={ck}: seek row is MISSING column '{col}' the full scan \
                     returned — a point/seek row truncated mid-cell (issue #3890). \
                     Seek row has {:?}",
                    got_cols.keys().collect::<Vec<_>>()
                ),
            }
        }
        for col in got_cols.keys() {
            assert!(
                want_cols.contains_key(col),
                "pk={pk} ck={ck}: seek row carries column '{col}' the full scan did not return"
            );
        }
    }
}

/// THE regression: seek each partition of a small-chunked SSTable whose rows span
/// multiple compression chunks. Every partition's rows must come back COMPLETE and
/// byte-identical to the full scan, the seek must DECODE the partition itself
/// (`partitions_decoded == 1`, the direct pre-fix signal), and a NON-last
/// partition's seek must be chunk-bounded well below the file total.
///
/// Counter is process-global, so the value-parity and chunk-bound assertions live
/// in ONE serialized test.
#[tokio::test]
async fn multichunk_row_seek_returns_full_partition() {
    let temp = TempDir::new().expect("temp dir");
    let Some(data_dir) = build_repacked_sstable(&temp) else {
        skip_or_fail_closed("test_da.wide_table fixture -Data.db binary not present");
        return;
    };
    let db = open_db(&data_dir).await;

    // Full scan: ground truth (whole-section path; not chunk-bounded).
    let full = db
        .execute(&format!("SELECT pk, ck, payload FROM {QUALIFIED_TABLE}"))
        .await
        .expect("full scan");
    // The `-Data.db` binary IS present (required by `real_wide_table_dir`), so a
    // 0-row scan is a genuine read regression, not an absent-fixture skip (#1856).
    assert!(
        !full.rows.is_empty(),
        "Issue #1856: wide_table -Data.db is present but the full scan returned 0 rows — \
         a genuine read regression, not an absent-fixture skip"
    );

    let mut by_partition: BTreeMap<i32, Vec<QueryRow>> = BTreeMap::new();
    for row in full.rows {
        if let Some(pk) = pk_value(&row) {
            by_partition.entry(pk).or_default().push(row);
        }
    }
    assert!(
        by_partition.len() >= 2,
        "wide_table must have >= 2 partitions to exercise the successor bound (got {})",
        by_partition.len()
    );
    // Sanity: these ARE multi-row partitions (300 rows each).
    for (pk, rows) in &by_partition {
        assert!(
            rows.len() > 1,
            "partition pk={pk} must be multi-row (got {})",
            rows.len()
        );
    }

    // Seek each partition; collect (pk, chunks, decoded, rows) for the bound check.
    let mut costs: Vec<(i32, u64, u64, usize)> = Vec::new();
    for (pk, expected_rows) in by_partition.iter() {
        work_counters::reset();
        let targeted = db
            .execute(&format!(
                "SELECT pk, ck, payload FROM {QUALIFIED_TABLE} WHERE pk = {pk}"
            ))
            .await
            .unwrap_or_else(|e| panic!("seek pk={pk} failed: {e}"));
        let decoded = work_counters::partitions_decoded();
        let chunks = work_counters::chunks_decompressed();

        assert_eq!(
            targeted.rows.len(),
            expected_rows.len(),
            "Issue #953: seek for pk={pk} must return ALL {} rows, not {} — a short count means \
             the multi-chunk row was truncated by the removed stability guard",
            expected_rows.len(),
            targeted.rows.len()
        );
        assert_eq!(
            fingerprints(&targeted.rows),
            fingerprints(expected_rows),
            "Issue #953: seek rows for pk={pk} must be byte-identical to the full scan"
        );
        // Issue #3890 (AC2): partition completeness was asserted by ROW COUNT plus
        // a whole-row fingerprint set; neither states, per row, that every expected
        // COLUMN is present. Assert that directly, so a point/seek row truncated
        // mid-cell (the class #3890 fixes) names the missing column instead of
        // sliding through as a differing fingerprint set.
        assert_columns_present_and_equal(*pk, &targeted.rows, expected_rows);
        assert_eq!(
            decoded, 1,
            "Issue #953: the within-SSTable seek for pk={pk} must DECODE exactly one partition \
             (got {decoded}). decoded == 0 means the chunk-targeted seek truncated the \
             multi-chunk row to an empty/partial decode and silently fell back to a full scan — \
             the stability-guard bug this fix removes."
        );
        assert!(
            chunks >= 2,
            "pk={pk}: at {REPACK_CHUNK_SIZE}-byte chunks a 300-row partition spans many chunks, \
             so the seek must decompress >= 2 (got {chunks}) — the bound is meaningful"
        );
        costs.push((*pk, chunks, decoded, targeted.rows.len()));
    }

    // Chunk-bound co-assertion on the NON-last partitions: each must be bounded to
    // roughly its own chunk span, well under the whole file. The LAST partition (in
    // Data.db order) reads the most chunks (to the data-section-length bound), so
    // the bound assertion deliberately targets the others.
    let last_pk = costs
        .iter()
        .max_by_key(|(_, chunks, _, _)| *chunks)
        .map(|(pk, _, _, _)| *pk)
        .expect("at least one partition");
    let max_chunks = costs
        .iter()
        .map(|(_, chunks, _, _)| *chunks)
        .max()
        .unwrap_or(0);

    let mut checked_non_last = 0usize;
    for (pk, chunks, _, _) in &costs {
        if *pk == last_pk {
            continue;
        }
        // A non-last partition's seek reads its own span plus at most the few
        // chunks needed to cross into the successor's first row; it must be
        // STRICTLY less than the last partition's to-data-end read.
        assert!(
            *chunks < max_chunks,
            "Issue #953 MEDIUM: non-last partition pk={pk} decompressed {chunks} chunks, not \
             strictly fewer than the max {max_chunks}. Equality means the seek stitched toward \
             the data end instead of stopping at the authoritative successor offset."
        );
        checked_non_last += 1;
    }
    assert!(
        checked_non_last >= 1,
        "expected at least one non-last partition to bound-check"
    );
    println!(
        "Issue #953 multi-chunk-row seek: {} partitions, last_pk={last_pk}, max_chunks={max_chunks}, \
         checked {checked_non_last} non-last; costs={costs:?}",
        costs.len()
    );
}
