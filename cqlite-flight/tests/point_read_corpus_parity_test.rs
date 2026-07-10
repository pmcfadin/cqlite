//! IMPORTANT-2 (roborev, issue #2207): dual-path byte-parity for the COMPRESSED
//! chunk-targeted seek (`point_compaction.rs`'s `Some(len)` window branch +
//! `pull_chunk_window`) over REAL, compressed, `nb`-format corpus tables.
//!
//! The compressed seek path is otherwise unverified by any synthetic fixture:
//! the write-engine test fixtures used elsewhere in this crate are always
//! UNCOMPRESSED (issue #1406 claim boundary — the production write surface
//! never emits `CompressionInfo.db`), so every other point-read test in this
//! crate exercises only the uncompressed (`point_read_whole_section`) window
//! strategy. A bug in the compressed chunk-index arithmetic would silently
//! return wrong rows with no fallback — exactly the risk this test retires.
//!
//! Two real corpus tables are used (both genuinely compressed `nb`-format,
//! confirmed via their sibling `CompressionInfo.db`, both ALL-SCALAR columns —
//! deliberately avoiding `test_collections`/`app_metrics.tags`-style multi-cell
//! collection columns, whose value reassembly is a SEPARATE, pre-existing gap
//! in the Flight producer's `entry_to_row` — unrelated to #2207 and hit
//! identically by the scan-path oracle, so it is not this test's concern):
//! - `test_wide_rows.large_blob_table` — a single-component UUID partition key,
//!   and the LARGEST all-scalar compressed table in the corpus (~58 KiB
//!   decompressed across 50 partitions), so the target key deliberately picked
//!   at the far end of the file forces a NON-ZERO `target_chunk` and proves the
//!   multi-chunk `pull_chunk_window` loop, not just chunk 0.
//! - `test_timeseries.app_metrics` — a genuinely COMPOSITE partition key
//!   `(application_id, metric_name)` (both `text`), satisfying the "no
//!   single-component-TEXT-PK fixture in this corpus" constraint by using a
//!   composite full-PK equality instead (in-spec per the design). Its `tags`
//!   MAP column is deliberately left UNDECLARED in this test's schema (see
//!   above) — omitted, not silently dropped: `entry_to_row` builds `RowCells`
//!   from every on-disk cell regardless of schema declaration, but the Arrow
//!   conversion only touches DECLARED columns, so an undeclared column is
//!   simply never read on either path, an identical no-op for scan and point.
//!
//! Both tests skip (never fail) when `CQLITE_DATASETS_ROOT` is unset or the
//! specific `Data.db` binary is absent (a worktree without
//! `fetch-datasets.sh`), but assert `rows > 0` whenever they run — never a
//! silent 0-row false pass.
//!
//! The large-table test also asserts a WORK-DONE bound via
//! `work_counters::chunks_decompressed()` (process-global, incremented once per
//! chunk `pull_chunk_window` decompresses): the point path must decompress
//! FEWER chunks than the file's total `chunk_count` (4) — proving the seek
//! actually targets a late chunk rather than degrading to a full-file scan
//! that would happen to produce the same (correct) rows. Since this counter is
//! process-global and BOTH tests in this file exercise the compressed path
//! (each incrementing it), `TEST_LOCK` serializes them so the chunk-count
//! window is never corrupted by the sibling test's concurrent decompress call
//! (precedent: `cqlite-core/tests/issue_953_multichunk_cell_seek.rs`).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;

use cqlite_core::query::{SSTableFilterOp, SSTablePredicate};
use cqlite_core::schema::{KeyColumn, TableSchema};
use cqlite_core::storage::sstable::work_counters;
use cqlite_core::types::Value;
use cqlite_flight::cancel::CancelFlag;
use cqlite_flight::filter::{FilterExpr, ScanSpec};
use cqlite_flight::producer::{DirSource, MergeProducer, SstableSource};

/// Serializes the two tests in this file so the process-global
/// `work_counters::chunks_decompressed()` window one test resets+reads is
/// never corrupted by the other's concurrent decompress call.
static TEST_LOCK: Mutex<()> = Mutex::new(());

/// Parse a standard `8-4-4-4-12` hyphenated UUID string into raw 16 bytes.
fn uuid_bytes(s: &str) -> [u8; 16] {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    assert_eq!(hex.len(), 32, "not a 16-byte UUID string: {s:?}");
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).expect("valid hex");
    }
    out
}

fn col(name: &str, ty: &str, nullable: bool) -> cqlite_core::schema::Column {
    cqlite_core::schema::Column {
        name: name.into(),
        data_type: ty.into(),
        nullable,
        default: None,
        is_static: false,
    }
}

/// Run the dual-path (scan vs point) byte-parity comparison for `filter` over
/// `table_dir`, asserting > 0 rows and identical concatenated Arrow batches.
fn assert_dual_path_parity(schema: &TableSchema, table_dir: &std::path::Path, filter: FilterExpr) {
    let spec = ScanSpec {
        token: None,
        filter: Some(filter),
        projection: None,
        limit: None,
    };

    let scan_producer = MergeProducer::with_spec(schema.clone(), 64, spec.clone()).unwrap();
    let scan_batches = scan_producer
        .produce_from_paths(DirSource::new(table_dir).data_paths().unwrap())
        .unwrap();

    let point_producer = MergeProducer::with_spec(schema.clone(), 64, spec).unwrap();
    let paths = point_producer
        .resolve_paths(&DirSource::new(table_dir))
        .unwrap();
    let point_batches = point_producer
        .produce_streaming_to_vec(paths, &CancelFlag::new())
        .unwrap();

    let scan_rows: usize = scan_batches.iter().map(|b| b.num_rows()).sum();
    let point_rows: usize = point_batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        scan_rows > 0,
        "the scan-path oracle must find at least one row for the target key \
         (never a silent 0-row false pass)"
    );
    assert_eq!(
        point_rows, scan_rows,
        "point path row count must match the scan-path oracle"
    );

    let arrow_schema = scan_producer.arrow_schema().unwrap();
    let scan_combined: RecordBatch = concat_batches(&arrow_schema.into(), &scan_batches).unwrap();
    let arrow_schema2 = point_producer.arrow_schema().unwrap();
    let point_combined: RecordBatch =
        concat_batches(&arrow_schema2.into(), &point_batches).unwrap();

    assert_eq!(
        point_combined, scan_combined,
        "the compressed chunk-targeted seek's point path must be byte-identical \
         to the scan+filter oracle over the real compressed corpus table"
    );
}

/// Run ONLY the point path for `filter` over `table_dir` and return the number
/// of chunks `pull_chunk_window` decompressed (`work_counters::reset()`
/// immediately before, `chunks_decompressed()` immediately after — the caller
/// must hold [`TEST_LOCK`] for the whole window, since the counter is
/// process-global).
fn point_path_chunks_decompressed(
    schema: &TableSchema,
    table_dir: &std::path::Path,
    filter: FilterExpr,
) -> u64 {
    let spec = ScanSpec {
        token: None,
        filter: Some(filter),
        projection: None,
        limit: None,
    };
    let producer = MergeProducer::with_spec(schema.clone(), 64, spec).unwrap();
    let paths = producer.resolve_paths(&DirSource::new(table_dir)).unwrap();
    work_counters::reset();
    let _ = producer
        .produce_streaming_to_vec(paths, &CancelFlag::new())
        .unwrap();
    work_counters::chunks_decompressed()
}

/// `test_wide_rows.large_blob_table` — single-component UUID PK, all-scalar
/// columns, ~58 KiB decompressed data section (50 partitions). The target key
/// is the LAST partition in the file (`position: 59485`), forcing a non-zero
/// `target_chunk` at any standard Cassandra `chunk_length` — proving the
/// multi-chunk `pull_chunk_window` loop, not just an offset-0 shortcut.
#[test]
fn dual_path_parity_compressed_corpus_large_single_pk_table() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());

    let Some(root) = std::env::var_os("CQLITE_DATASETS_ROOT") else {
        eprintln!("CQLITE_DATASETS_ROOT unset — skipping real-corpus compressed-seek parity");
        return;
    };
    let table_dir = PathBuf::from(&root)
        .join("sstables")
        .join("test_wide_rows")
        .join("large_blob_table-6d81d000a25111f0a3fef1a551383fb9");
    let data_db = table_dir.join("nb-1-big-Data.db");
    if !data_db.is_file() {
        eprintln!("real fixture Data.db binary absent (run fetch-datasets.sh) — skipping");
        return;
    }
    assert!(
        table_dir.join("nb-1-big-CompressionInfo.db").is_file(),
        "large_blob_table must be a genuinely compressed nb fixture for this test to be meaningful"
    );

    let schema = TableSchema {
        keyspace: "test_wide_rows".into(),
        table: "large_blob_table".into(),
        partition_keys: vec![KeyColumn {
            name: "file_id".into(),
            data_type: "uuid".into(),
            position: 0,
        }],
        clustering_keys: vec![cqlite_core::schema::ClusteringColumn {
            name: "chunk_id".into(),
            data_type: "int".into(),
            position: 0,
            order: Default::default(),
        }],
        columns: vec![
            col("file_id", "uuid", false),
            col("chunk_id", "int", false),
            col("file_name", "text", true),
            col("mime_type", "text", true),
            col("chunk_data", "blob", true),
            col("chunk_size", "int", true),
            col("total_chunks", "int", true),
            col("checksum", "text", true),
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    // The LAST partition in the file (position 59485 of ~58 KiB decompressed)
    // — deliberately far from offset 0 (computed via the corpus JSONL, not
    // guessed) so the seek must target a late chunk correctly.
    let target = uuid_bytes("7bb497c5-eb6e-4f0b-a6ac-8b891de47747");
    let filter = FilterExpr::Leaf(SSTablePredicate {
        column: "file_id".into(),
        operation: SSTableFilterOp::Equal,
        values: vec![Value::Uuid(target)],
        token_columns: None,
    });

    assert_dual_path_parity(&schema, &table_dir, filter.clone());

    // Work-done proof: the real file has `chunk_count = 4` (16 KiB
    // `chunk_length`, ~58 KiB decompressed — confirmed via `CompressionInfo.db`
    // at authoring time). The target partition is the LAST one in the file, so
    // a correct seek starts at chunk 3, not chunk 0 — this bounds the point
    // path to decompressing FEWER than the file's total chunk count, proving
    // the seek genuinely targets a late chunk rather than a full-file scan
    // that happens to produce the same (correct) rows.
    let chunks = point_path_chunks_decompressed(&schema, &table_dir, filter);
    assert!(
        chunks > 0,
        "the compressed chunk-targeted path must decompress at least one chunk"
    );
    assert!(
        chunks < 4,
        "a targeted seek of the LAST partition must decompress FEWER than the \
         file's total 4 chunks (got {chunks}) — a full-file scan would need all 4"
    );
}

/// `test_timeseries.app_metrics` — a genuinely COMPOSITE partition key
/// `(application_id, metric_name)`, both `text`. Satisfies the "no
/// single-component-TEXT-PK fixture" constraint by using a composite full-PK
/// equality (in-spec). The `tags` MAP column is deliberately undeclared (see
/// module doc) to avoid a separate, pre-existing, unrelated Flight-producer
/// multi-cell-collection gap that affects the scan path identically.
#[test]
fn dual_path_parity_compressed_corpus_composite_pk_table() {
    let _guard = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());

    let Some(root) = std::env::var_os("CQLITE_DATASETS_ROOT") else {
        eprintln!("CQLITE_DATASETS_ROOT unset — skipping real-corpus compressed-seek parity");
        return;
    };
    let table_dir = PathBuf::from(&root)
        .join("sstables")
        .join("test_timeseries")
        .join("app_metrics-6c87b890a25111f0a3fef1a551383fb9");
    let data_db = table_dir.join("nb-1-big-Data.db");
    if !data_db.is_file() {
        eprintln!("real fixture Data.db binary absent (run fetch-datasets.sh) — skipping");
        return;
    }
    assert!(
        table_dir.join("nb-1-big-CompressionInfo.db").is_file(),
        "app_metrics must be a genuinely compressed nb fixture for this test to be meaningful"
    );

    let schema = TableSchema {
        keyspace: "test_timeseries".into(),
        table: "app_metrics".into(),
        partition_keys: vec![
            KeyColumn {
                name: "application_id".into(),
                data_type: "text".into(),
                position: 0,
            },
            KeyColumn {
                name: "metric_name".into(),
                data_type: "text".into(),
                position: 1,
            },
        ],
        clustering_keys: vec![cqlite_core::schema::ClusteringColumn {
            name: "timestamp".into(),
            data_type: "timestamp".into(),
            position: 0,
            order: Default::default(),
        }],
        columns: vec![
            col("application_id", "text", false),
            col("metric_name", "text", false),
            col("timestamp", "timestamp", false),
            col("value", "double", true),
            col("unit", "text", true),
            // `tags MAP<TEXT, TEXT>` deliberately undeclared — see module doc.
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    // A real composite key from the corpus JSONL (application_id="goal",
    // metric_name="interest").
    let filter = FilterExpr::And(vec![
        FilterExpr::Leaf(SSTablePredicate {
            column: "application_id".into(),
            operation: SSTableFilterOp::Equal,
            values: vec![Value::Text("goal".into())],
            token_columns: None,
        }),
        FilterExpr::Leaf(SSTablePredicate {
            column: "metric_name".into(),
            operation: SSTableFilterOp::Equal,
            values: vec![Value::Text("interest".into())],
            token_columns: None,
        }),
    ]);

    assert_dual_path_parity(&schema, &table_dir, filter);
}
