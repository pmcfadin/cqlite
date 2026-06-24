//! Issue #954 (Epic #951): push single-column clustering-key range/equality
//! restrictions down to a within-partition seek.
//!
//! For a fully-constrained partition key plus a single-column clustering
//! restriction (`ck >= a AND ck < b`, single-bound `ck >/>=/</<=`, or `ck = ?`),
//! the executor consults the target partition's authoritative BTI row index
//! (`Rows.db`) to decode ONLY the row-index block(s) covering the requested
//! clustering range — so a wide-partition slice decodes O(matched rows + index
//! block slack), not the whole partition. The post-scan `evaluate_leaf` backstop
//! trims the block-granularity over-read, so the result is byte-identical to the
//! full-partition-decode + post-filter path.
//!
//! These tests pin THREE properties against the BTI (`da`) wide-partition fixture
//! `test_da.wide_table` (`PRIMARY KEY (pk, ck)`, int pk, 3 partitions pk=1/2/3,
//! each 300 rows ck=0..299, LZ4 — so the partition spans many compression
//! chunks):
//!   1. **Parity** — the slice query returns EXACTLY the rows the full-scan path
//!      (filtered to the same predicate in memory) returns.
//!   2. **Bounded decode** — `work_counters::rows_decoded()` is bounded by the
//!      slice size plus one index block of slack, and well below the partition's
//!      300 rows.
//!   3. **Honest access path** — the engaged slice reports
//!      `AccessPath::ClusteringSlice`; a partition-only lookup (no clustering
//!      restriction) still reports `PartitionLookup`.
//!
//! Requires `CQLITE_DATASETS_ROOT` and the fetched binary SSTables; skipped (not
//! failed) when the data isn't present. Excluded under `tombstones` (that build
//! compiles out the seek and the work counters).

#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    not(feature = "tombstones")
))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::access_path::{self, AccessPath};
use cqlite_core::storage::sstable::work_counters;
use cqlite_core::Database;
use cqlite_core::Value;

const QUALIFIED_TABLE: &str = "test_da.wide_table";
const KEYSPACE_FILTER: &str = "/test_da/";
/// Clustering rows per partition in the fixture (ck = 0..299).
const PARTITION_ROW_COUNT: usize = 300;

/// Serialize the tests: the work counters and the access-path probe are
/// process-global, so two of these running concurrently would clobber each
/// other's `reset()` / read window. `tokio::sync::Mutex` so the guard can be held
/// across `.await`.
static PROBE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn schemas_dir() -> Option<PathBuf> {
    if let Some(root) = datasets_root() {
        let dir = root.parent()?.join("schemas");
        if dir.exists() {
            return Some(dir);
        }
    }
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = manifest_dir.parent()?.join("test-data").join("schemas");
    dir.exists().then_some(dir)
}

async fn setup() -> Result<Database, String> {
    let root = datasets_root().ok_or("CQLITE_DATASETS_ROOT not set or missing")?;
    let schema_path = schemas_dir()
        .ok_or("schemas dir not found")?
        .join("wide-table-bti.cql");
    if !schema_path.exists() {
        return Err(format!("schema not found at {schema_path:?}"));
    }
    let data_dir = root.join("sstables");
    if !data_dir.exists() {
        return Err(format!("sstables dir not found at {data_dir:?}"));
    }

    let config = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(KEYSPACE_FILTER.to_string()),
    };
    let result = ingest(config)
        .await
        .map_err(|e| format!("ingestion failed: {e}"))?;
    if result.schema_load_result.schemas_loaded == 0 {
        return Err("no schemas loaded".to_string());
    }
    Ok(result.database)
}

/// The sorted `ck` integers a query returned (for pk=1), to compare against an
/// expected slice independent of row ordering.
fn cks(rows: &[cqlite_core::query::result::QueryRow]) -> Vec<i32> {
    let mut out: Vec<i32> = rows
        .iter()
        .filter_map(|r| match r.values.get("ck") {
            Some(Value::Integer(i)) => Some(*i),
            _ => None,
        })
        .collect();
    out.sort_unstable();
    out
}

async fn skip_or_db() -> Option<Database> {
    match setup().await {
        Ok(db) => Some(db),
        Err(e) => {
            eprintln!("Skipping (BTI wide_table): {e}");
            None
        }
    }
}

/// Run one slice query under the probe lock, returning `(returned_cks,
/// rows_decoded, access_path)`. Resets both process-global probes first.
async fn run_slice(db: &Database, where_clause: &str) -> (Vec<i32>, u64, Option<AccessPath>) {
    work_counters::reset();
    access_path::reset();
    let result = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {QUALIFIED_TABLE} WHERE {where_clause}"
        ))
        .await
        .unwrap_or_else(|e| panic!("slice query `{where_clause}` failed: {e}"));
    let rows_decoded = work_counters::rows_decoded();
    let path = result.metadata.access_path.clone();
    (cks(&result.rows), rows_decoded, path)
}

// ---------------------------------------------------------------------------
// 1. Two-bound contiguous range: parity + bounded decode + ClusteringSlice.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn two_bound_range_slice_parity_and_bounded_decode() {
    let _g = PROBE_LOCK.lock().await;
    let Some(db) = skip_or_db().await else {
        return;
    };

    // Sanity: the full partition has 300 rows (data fetched).
    let full = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {QUALIFIED_TABLE} WHERE pk = 1"
        ))
        .await
        .expect("full partition read must succeed");
    if full.rows.is_empty() {
        eprintln!("Skipping: wide_table returned 0 rows (Data.db not fetched?)");
        return;
    }
    assert_eq!(
        full.rows.len(),
        PARTITION_ROW_COUNT,
        "fixture invariant: pk=1 must hold {PARTITION_ROW_COUNT} clustering rows",
    );

    // `ck >= 100 AND ck < 110` selects exactly ck = 100..=109 (10 rows).
    let expected: Vec<i32> = (100..110).collect();
    let (returned, rows_decoded, path) = run_slice(&db, "pk = 1 AND ck >= 100 AND ck < 110").await;

    // Parity: exactly the rows the in-memory filter would yield.
    assert_eq!(
        returned, expected,
        "Issue #954: pk=1 AND ck in [100,110) must return ck=100..=109",
    );

    // Honest access path: the clustering seek engaged.
    assert_eq!(
        path,
        Some(AccessPath::ClusteringSlice),
        "Issue #954: an engaged single-column clustering slice must report ClusteringSlice",
    );
    assert_eq!(
        access_path::last(),
        Some(AccessPath::ClusteringSlice),
        "Issue #954: the access-path probe must record ClusteringSlice",
    );

    // Bounded decode: O(slice + one index block of slack), well below 300.
    // The slice is 10 rows; allow generous block-granularity slack but require
    // it to be far under the full partition (a regression to full-partition
    // decode reads ~300).
    let bound = expected.len() as u64 + 64;
    assert!(
        rows_decoded > 0 && rows_decoded <= bound,
        "Issue #954: rows_decoded ({rows_decoded}) must be in (0, {bound}] for a 10-row slice; \
         a full-partition decode would read ~{PARTITION_ROW_COUNT}",
    );
    assert!(
        rows_decoded < PARTITION_ROW_COUNT as u64,
        "Issue #954: rows_decoded ({rows_decoded}) must be strictly below the partition's \
         {PARTITION_ROW_COUNT} rows (the whole point of the slice seek)",
    );
    println!(
        "Issue #954 two-bound range: returned {} rows, decoded {rows_decoded} (bound {bound})",
        returned.len()
    );
}

// ---------------------------------------------------------------------------
// 2. Single-bound `ck <` : parity + bounded decode.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn single_bound_lt_slice_parity_and_bounded_decode() {
    let _g = PROBE_LOCK.lock().await;
    let Some(db) = skip_or_db().await else {
        return;
    };
    // Skip if no data.
    let probe = db
        .execute(&format!(
            "SELECT ck FROM {QUALIFIED_TABLE} WHERE pk = 2 LIMIT 1"
        ))
        .await
        .expect("probe must succeed");
    if probe.rows.is_empty() {
        eprintln!("Skipping: wide_table returned 0 rows (Data.db not fetched?)");
        return;
    }

    // `ck < 20` selects ck = 0..=19 (20 rows).
    let expected: Vec<i32> = (0..20).collect();
    let (returned, rows_decoded, path) = run_slice(&db, "pk = 2 AND ck < 20").await;

    assert_eq!(
        returned, expected,
        "Issue #954: pk=2 AND ck < 20 must return ck=0..=19"
    );
    assert_eq!(
        path,
        Some(AccessPath::ClusteringSlice),
        "Issue #954: `ck < ?` must engage the clustering slice",
    );
    let bound = expected.len() as u64 + 64;
    assert!(
        rows_decoded > 0 && rows_decoded <= bound,
        "Issue #954: rows_decoded ({rows_decoded}) must be in (0, {bound}] for a 20-row `ck < 20` \
         slice; full-partition decode reads ~{PARTITION_ROW_COUNT}",
    );
    println!("Issue #954 single-bound ck<20: decoded {rows_decoded} (bound {bound})");
}

// ---------------------------------------------------------------------------
// 3. Single-bound `ck >=` : parity (start fast-forward narrows decode).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn single_bound_gte_slice_parity_and_bounded_decode() {
    let _g = PROBE_LOCK.lock().await;
    let Some(db) = skip_or_db().await else {
        return;
    };
    let probe = db
        .execute(&format!(
            "SELECT ck FROM {QUALIFIED_TABLE} WHERE pk = 3 LIMIT 1"
        ))
        .await
        .expect("probe must succeed");
    if probe.rows.is_empty() {
        eprintln!("Skipping: wide_table returned 0 rows (Data.db not fetched?)");
        return;
    }

    // `ck >= 290` selects ck = 290..=299 (10 rows) — the TAIL of the partition,
    // so the start fast-forward must skip ~290 leading rows.
    let expected: Vec<i32> = (290..300).collect();
    let (returned, rows_decoded, path) = run_slice(&db, "pk = 3 AND ck >= 290").await;

    assert_eq!(
        returned, expected,
        "Issue #954: pk=3 AND ck >= 290 must return ck=290..=299",
    );
    assert_eq!(
        path,
        Some(AccessPath::ClusteringSlice),
        "Issue #954: `ck >= ?` must engage the clustering slice",
    );
    let bound = expected.len() as u64 + 64;
    assert!(
        rows_decoded > 0 && rows_decoded <= bound,
        "Issue #954: rows_decoded ({rows_decoded}) must be in (0, {bound}] for a tail `ck >= 290` \
         slice (the start fast-forward must skip the ~290 leading rows, not decode all \
         {PARTITION_ROW_COUNT})",
    );
    println!("Issue #954 single-bound ck>=290 (tail): decoded {rows_decoded} (bound {bound})");
}

// ---------------------------------------------------------------------------
// 4. Equality `ck = ?` : parity + bounded decode.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn equality_slice_parity_and_bounded_decode() {
    let _g = PROBE_LOCK.lock().await;
    let Some(db) = skip_or_db().await else {
        return;
    };
    let probe = db
        .execute(&format!(
            "SELECT ck FROM {QUALIFIED_TABLE} WHERE pk = 1 LIMIT 1"
        ))
        .await
        .expect("probe must succeed");
    if probe.rows.is_empty() {
        eprintln!("Skipping: wide_table returned 0 rows (Data.db not fetched?)");
        return;
    }

    let (returned, rows_decoded, path) = run_slice(&db, "pk = 1 AND ck = 150").await;
    assert_eq!(
        returned,
        vec![150],
        "Issue #954: pk=1 AND ck = 150 must return exactly ck=150"
    );
    assert_eq!(
        path,
        Some(AccessPath::ClusteringSlice),
        "Issue #954: `ck = ?` must engage the clustering slice",
    );
    let bound = 1u64 + 64;
    assert!(
        rows_decoded > 0 && rows_decoded <= bound,
        "Issue #954: rows_decoded ({rows_decoded}) must be in (0, {bound}] for `ck = 150`; \
         full-partition decode reads ~{PARTITION_ROW_COUNT}",
    );
    println!("Issue #954 equality ck=150: decoded {rows_decoded} (bound {bound})");
}

// ---------------------------------------------------------------------------
// 5. Partition-only lookup (no clustering restriction) still reports
//    PartitionLookup — honest fallback, NOT a fake ClusteringSlice.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn partition_only_lookup_reports_partition_lookup_not_clustering_slice() {
    let _g = PROBE_LOCK.lock().await;
    let Some(db) = skip_or_db().await else {
        return;
    };
    access_path::reset();
    let result = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {QUALIFIED_TABLE} WHERE pk = 1"
        ))
        .await
        .expect("partition read must succeed");
    if result.rows.is_empty() {
        eprintln!("Skipping: wide_table returned 0 rows (Data.db not fetched?)");
        return;
    }
    assert_eq!(
        result.metadata.access_path,
        Some(AccessPath::PartitionLookup),
        "Issue #954: a partition-only lookup (no clustering restriction) must report \
         PartitionLookup, NOT a fake ClusteringSlice",
    );
    assert_eq!(
        result.rows.len(),
        PARTITION_ROW_COUNT,
        "partition-only lookup must return all {PARTITION_ROW_COUNT} rows",
    );
}

// ---------------------------------------------------------------------------
// 6. Full parity sweep: every slice equals the full-scan-filtered baseline.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn slice_results_equal_full_scan_filtered_baseline() {
    let _g = PROBE_LOCK.lock().await;
    let Some(db) = skip_or_db().await else {
        return;
    };
    let full = db
        .execute(&format!(
            "SELECT pk, ck, payload FROM {QUALIFIED_TABLE} WHERE pk = 1"
        ))
        .await
        .expect("full partition read must succeed");
    if full.rows.is_empty() {
        eprintln!("Skipping: wide_table returned 0 rows (Data.db not fetched?)");
        return;
    }
    let all_cks = cks(&full.rows);

    // For each shape, the in-memory baseline filter over the full partition's cks
    // must equal the pushed-down slice's returned cks.
    // (where clause, in-memory predicate over `ck`) baseline cases.
    type SliceCase = (&'static str, fn(i32) -> bool);
    let cases: &[SliceCase] = &[
        ("pk = 1 AND ck >= 50 AND ck < 75", |c| (50..75).contains(&c)),
        ("pk = 1 AND ck > 200", |c| c > 200),
        ("pk = 1 AND ck <= 5", |c| c <= 5),
        ("pk = 1 AND ck = 0", |c| c == 0),
        ("pk = 1 AND ck = 299", |c| c == 299),
    ];

    for (where_clause, pred) in cases {
        let expected: Vec<i32> = all_cks.iter().copied().filter(|c| pred(*c)).collect();
        let (returned, _decoded, _path) = run_slice(&db, where_clause).await;
        assert_eq!(
            returned, expected,
            "Issue #954: slice `{where_clause}` must equal the full-scan-filtered baseline",
        );
    }
    println!("Issue #954: all clustering-slice shapes match the full-scan-filtered baseline");
}

// ---------------------------------------------------------------------------
// 7. DESC clustering correctness (issue #954 High-severity fix).
//
// For a `CLUSTERING ORDER BY (ck DESC)` table the rows are stored in REVERSED
// physical byte order. The clustering-slice seek selects row-index blocks in
// physical (byte-comparable) order, so the CQL lower/upper bounds must be
// SWAPPED into physical order for DESC before block selection — otherwise a
// predicate like `ck >= v` builds `[enc(v), +∞]` and SKIPS the matching rows
// (which sort to the LOW physical-byte side), and the post-filter cannot recover
// rows that were never decoded.
//
// The load-bearing bound-normalization swap is unit-tested in
// `cqlite-core::storage::sstable::reader::data_access::tests`
// (`physical_bounds_desc_*`): those tests FAIL against the un-swapped (buggy)
// mapping and PASS with the fix. This integration test exercises the SEEK path
// end-to-end on a DESC BTI fixture WHEN ONE GENUINELY EXISTS.
//
// REAL DETECTION (not an unconditional skip): `find_desc_bti_table` scans every
// `schemas_dir()` `.cql` file, parses each `CREATE TABLE`, and selects the first
// table whose FIRST clustering column is `DESC` (per its `WITH CLUSTERING ORDER
// BY (<firstck> DESC ...)` clause) AND whose on-disk SSTable dir
// (`sstables/<keyspace>/<table>-*/`) contains a NON-EMPTY `da-*-Rows.db` (the BTI
// wide-partition shape — a per-partition row index — that the seek engages on).
// Only such a table can exercise the DESC seek end-to-end.
//
//   * Found  -> ingest it, pick a real partition key (via a full scan), and
//               assert PARITY (`pk = ? AND ck >= a AND ck < b`, a single bound,
//               and `ck = ?` each return EXACTLY the full-scan-in-memory-filtered
//               rows), plus an HONEST access path (ClusteringSlice if the seek
//               engaged, else PartitionLookup — both acceptable; the load-bearing
//               assertion for DESC is correctness/parity).
//   * Absent -> skip (not fail) AFTER the real scan reported no matching table.
//
// FIXTURE STATUS: as of this change no DESC BTI fixture is present (the only BTI
// table with a populated `Rows.db` is the ASC `test_da.wide_table`); a DESC one
// cannot be produced without Cassandra. This test therefore SKIPS today — but the
// skip is the result of REAL inspection, and the test ACTIVATES automatically the
// moment a DESC-first-clustering BTI fixture with a non-empty `Rows.db` is added.
// The DESC correctness of the fix is also pinned NOW by `physical_bounds_desc_*`.
// ---------------------------------------------------------------------------

/// A DESC-first-clustering BTI table discovered by cross-referencing a parsed
/// schema `.cql` against an on-disk SSTable dir with a non-empty `da-*-Rows.db`.
#[derive(Debug, Clone)]
struct DescBtiTable {
    schema_path: PathBuf,
    keyspace: String,
    table: String,
    /// First (and, for the seek, the targeted) partition-key column name.
    pk_col: String,
    /// First clustering column — the one whose physical order is DESC.
    ck_col: String,
}

/// Parse a single `CREATE TABLE` body (the text between `CREATE TABLE ... (` and
/// the matching `)` that closes the column list) for its `PRIMARY KEY (...)`
/// columns. Returns `(partition_key_cols, clustering_cols)`. A leading
/// parenthesised group is the composite partition key; everything after it (or
/// the first column, if no parenthesised group) is clustering. Robust to inline
/// `col TYPE PRIMARY KEY` single-column PKs (no clustering).
fn parse_primary_key(body: &str) -> Option<(Vec<String>, Vec<String>)> {
    // Inline single-column PK: `<col> <type> PRIMARY KEY` on a column line.
    for raw_line in body.split(',') {
        let line = raw_line.trim();
        let upper = line.to_uppercase();
        if upper.contains("PRIMARY KEY") && !upper.starts_with("PRIMARY KEY") {
            // e.g. "id UUID PRIMARY KEY"
            let col = line.split_whitespace().next()?.to_string();
            return Some((vec![col], Vec::new()));
        }
    }

    // Standalone `PRIMARY KEY ( ... )` clause.
    let upper_body = body.to_uppercase();
    let pk_at = upper_body.find("PRIMARY KEY")?;
    let after = &body[pk_at + "PRIMARY KEY".len()..];
    let open = after.find('(')?;
    // Find the matching close paren for this PRIMARY KEY clause.
    let mut depth = 0usize;
    let mut end = None;
    for (i, c) in after[open..].char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    end = Some(open + i);
                    break;
                }
            }
            _ => {}
        }
    }
    let inner = &after[open + 1..end?];

    // Split the PK clause into top-level, comma-separated components, honoring a
    // nested parenthesised partition-key group as a single first component.
    let mut components: Vec<String> = Vec::new();
    let mut buf = String::new();
    let mut depth = 0usize;
    for c in inner.chars() {
        match c {
            '(' => {
                depth += 1;
                buf.push(c);
            }
            ')' => {
                depth -= 1;
                buf.push(c);
            }
            ',' if depth == 0 => {
                components.push(buf.trim().to_string());
                buf.clear();
            }
            _ => buf.push(c),
        }
    }
    if !buf.trim().is_empty() {
        components.push(buf.trim().to_string());
    }
    if components.is_empty() {
        return None;
    }

    let first = components.remove(0);
    let partition: Vec<String> = if first.starts_with('(') {
        first
            .trim_matches(|c| c == '(' || c == ')')
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        vec![first]
    };
    let clustering: Vec<String> = components.into_iter().filter(|s| !s.is_empty()).collect();
    Some((partition, clustering))
}

/// True if the `CREATE TABLE` text declares its FIRST clustering column DESC via a
/// `WITH ... CLUSTERING ORDER BY (<first> DESC ...)` clause, returning that first
/// clustering column name when so.
fn first_clustering_desc(stmt: &str) -> Option<String> {
    let upper = stmt.to_uppercase();
    let at = upper.find("CLUSTERING ORDER BY")?;
    let after = &stmt[at + "CLUSTERING ORDER BY".len()..];
    let open = after.find('(')?;
    let close = after[open..].find(')')? + open;
    let inner = &after[open + 1..close];
    let first = inner.split(',').next()?.trim();
    let mut it = first.split_whitespace();
    let col = it.next()?.to_string();
    let dir = it.next().unwrap_or("ASC").to_uppercase();
    (dir == "DESC").then_some(col)
}

/// The `USE <keyspace>;` (or `CREATE KEYSPACE <keyspace>`) declared in a schema
/// file — the keyspace its tables live under on disk.
fn schema_keyspace(text: &str) -> Option<String> {
    for line in text.lines() {
        let t = line.trim();
        let upper = t.to_uppercase();
        if let Some(rest) = upper.strip_prefix("USE ") {
            let name = &t[t.len() - rest.len()..];
            let name = name.trim().trim_end_matches(';').trim();
            if !name.is_empty() {
                return Some(name.to_string());
            }
        }
    }
    // Fallback: CREATE KEYSPACE [IF NOT EXISTS] <name> ...
    let upper = text.to_uppercase();
    if let Some(at) = upper.find("CREATE KEYSPACE") {
        let after = &text[at + "CREATE KEYSPACE".len()..];
        let cleaned = after.trim_start();
        let cleaned = cleaned
            .strip_prefix("IF NOT EXISTS")
            .or_else(|| cleaned.strip_prefix("if not exists"))
            .unwrap_or(cleaned)
            .trim_start();
        let name: String = cleaned
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if !name.is_empty() {
            return Some(name);
        }
    }
    None
}

/// On-disk: does `sstables/<keyspace>/<table>-*/` hold a NON-EMPTY `da-*-Rows.db`
/// (the BTI wide-partition shape the clustering seek engages on)?
fn has_nonempty_bti_rows_db(keyspace: &str, table: &str) -> bool {
    let Some(root) = datasets_root() else {
        return false;
    };
    let ks_dir = root.join("sstables").join(keyspace);
    let Ok(entries) = std::fs::read_dir(&ks_dir) else {
        return false;
    };
    let prefix = format!("{table}-");
    for tbl in entries.flatten() {
        let name = tbl.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with(&prefix) {
            continue;
        }
        let Ok(files) = std::fs::read_dir(tbl.path()) else {
            continue;
        };
        for f in files.flatten() {
            let fname = f.file_name();
            let fname = fname.to_string_lossy();
            if fname.starts_with("da-")
                && fname.ends_with("-Rows.db")
                && f.metadata().map(|m| m.len() > 0).unwrap_or(false)
            {
                return true;
            }
        }
    }
    false
}

/// Genuinely detect a DESC-first-clustering BTI table: scan every schema `.cql`,
/// parse each `CREATE TABLE`, and return the FIRST table whose first clustering
/// column is DESC AND whose on-disk dir has a non-empty `da-*-Rows.db`.
fn find_desc_bti_table() -> Option<DescBtiTable> {
    let dir = schemas_dir()?;
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .ok()?
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().map(|e| e == "cql").unwrap_or(false))
        .collect();
    files.sort();

    for path in files {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let Some(keyspace) = schema_keyspace(&text) else {
            continue;
        };
        let upper = text.to_uppercase();

        // Walk each `CREATE TABLE` occurrence.
        let mut search_from = 0usize;
        while let Some(rel) = upper[search_from..].find("CREATE TABLE") {
            let stmt_start = search_from + rel;
            // Statement ends at the next top-level `;`.
            let stmt_end = text[stmt_start..]
                .find(';')
                .map(|o| stmt_start + o)
                .unwrap_or(text.len());
            let stmt = &text[stmt_start..stmt_end];
            search_from = stmt_end + 1;

            // Table name: token after CREATE TABLE [IF NOT EXISTS].
            let header = &stmt[..stmt.find('(').unwrap_or(stmt.len())];
            let header_upper = header.to_uppercase();
            let name_region = header_upper
                .strip_prefix("CREATE TABLE")
                .map(|r| r.trim_start())
                .and_then(|r| {
                    r.strip_prefix("IF NOT EXISTS")
                        .map(|x| x.trim_start())
                        .or(Some(r))
                })
                .unwrap_or("");
            let consumed = header.len() - name_region.len();
            let table: String = header[consumed..]
                .trim_start()
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if table.is_empty() {
                continue;
            }

            // First clustering column must be DESC.
            let Some(desc_ck) = first_clustering_desc(stmt) else {
                continue;
            };
            // Parse PK to confirm the DESC column is the first clustering column
            // and to learn the partition-key column to target.
            let body = {
                let open = stmt.find('(');
                match open {
                    Some(o) => &stmt[o + 1..],
                    None => continue,
                }
            };
            let Some((pk_cols, ck_cols)) = parse_primary_key(body) else {
                continue;
            };
            let (Some(pk_col), Some(ck_col)) = (pk_cols.first(), ck_cols.first()) else {
                continue;
            };
            if !ck_col.eq_ignore_ascii_case(&desc_ck) {
                continue;
            }
            if !has_nonempty_bti_rows_db(&keyspace, &table) {
                continue;
            }
            return Some(DescBtiTable {
                schema_path: path.clone(),
                keyspace,
                table,
                pk_col: pk_col.clone(),
                ck_col: ck_col.clone(),
            });
        }
    }
    None
}

/// Ingest a single discovered DESC BTI table by its own schema file, filtered to
/// its keyspace.
async fn setup_desc(t: &DescBtiTable) -> Result<Database, String> {
    let root = datasets_root().ok_or("CQLITE_DATASETS_ROOT not set or missing")?;
    let data_dir = root.join("sstables");
    if !data_dir.exists() {
        return Err(format!("sstables dir not found at {data_dir:?}"));
    }
    let config = IngestionConfig {
        schema_paths: vec![t.schema_path.clone()],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{}/", t.keyspace)),
    };
    let result = ingest(config)
        .await
        .map_err(|e| format!("ingestion failed: {e}"))?;
    if result.schema_load_result.schemas_loaded == 0 {
        return Err("no schemas loaded".to_string());
    }
    Ok(result.database)
}

/// A clustering value extracted from a row, comparable across the int/text/bigint
/// shapes BTI clustering columns take in this dataset. Only the variants we can
/// totally order and render back into a CQL literal are represented.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
enum CkVal {
    Int(i64),
    Text(String),
}

impl CkVal {
    fn from_value(v: &Value) -> Option<CkVal> {
        match v {
            Value::Integer(i) => Some(CkVal::Int(*i as i64)),
            Value::BigInt(i) | Value::Timestamp(i) | Value::Time(i) => Some(CkVal::Int(*i)),
            Value::SmallInt(i) => Some(CkVal::Int(*i as i64)),
            Value::TinyInt(i) => Some(CkVal::Int(*i as i64)),
            Value::Date(d) => Some(CkVal::Int(*d as i64)),
            Value::Text(s) => Some(CkVal::Text(s.clone())),
            _ => None,
        }
    }

    /// Render as a CQL literal usable on the right of a clustering comparison.
    fn literal(&self) -> String {
        match self {
            CkVal::Int(i) => i.to_string(),
            CkVal::Text(s) => format!("'{}'", s.replace('\'', "''")),
        }
    }
}

/// A partition-key value rendered as a CQL literal for `WHERE pk = <lit>`.
fn pk_literal(v: &Value) -> Option<String> {
    Some(match v {
        Value::Integer(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::SmallInt(i) => i.to_string(),
        Value::TinyInt(i) => i.to_string(),
        Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Uuid(b) => {
            let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
            format!(
                "{}-{}-{}-{}-{}",
                &h[0..8],
                &h[8..12],
                &h[12..16],
                &h[16..20],
                &h[20..32]
            )
        }
        _ => return None,
    })
}

#[tokio::test]
async fn desc_clustering_slice_correct_or_documented_skip() {
    let _g = PROBE_LOCK.lock().await;

    // REAL detection: scan the schema `.cql` files for a CREATE TABLE whose first
    // clustering column is DESC and whose on-disk dir has a non-empty
    // `da-*-Rows.db`. Skip (not fail) only when the scan finds none.
    let Some(t) = find_desc_bti_table() else {
        eprintln!(
            "Skipping DESC clustering-slice integration test: scanned all schema .cql files and \
             found NO CREATE TABLE with a DESC first clustering column backed by a non-empty \
             da-*-Rows.db (BTI wide partition). This is the current dataset reality (the only BTI \
             table with a populated Rows.db is the ASC test_da.wide_table). DESC correctness is \
             pinned by the `physical_bounds_desc_*` unit tests in data_access.rs; this test \
             activates automatically once a DESC BTI fixture is added."
        );
        return;
    };

    eprintln!(
        "DESC clustering-slice integration: detected DESC BTI table {}.{} \
         (pk={}, first ck={} DESC) -> running end-to-end seek parity assertions.",
        t.keyspace, t.table, t.pk_col, t.ck_col
    );

    let db = match setup_desc(&t).await {
        Ok(db) => db,
        Err(e) => {
            eprintln!(
                "Skipping DESC clustering-slice integration test: ingestion unavailable: {e}"
            );
            return;
        }
    };
    let qualified = format!("{}.{}", t.keyspace, t.table);

    // Full scan to pick a real partition key and learn its in-memory cks.
    let full = match db.execute(&format!("SELECT * FROM {qualified}")).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Skipping: full scan of {qualified} failed: {e}");
            return;
        }
    };
    if full.rows.is_empty() {
        eprintln!("Skipping: {qualified} returned 0 rows (Data.db not fetched?)");
        return;
    }

    // Choose the partition key with the most clustering rows (best slice exercise).
    use std::collections::BTreeMap;
    let mut by_pk: BTreeMap<String, Vec<CkVal>> = BTreeMap::new();
    let mut pk_lit_for: BTreeMap<String, String> = BTreeMap::new();
    for row in &full.rows {
        let (Some(pk_v), Some(ck_v)) = (row.values.get(&t.pk_col), row.values.get(&t.ck_col))
        else {
            continue;
        };
        let (Some(pk_lit), Some(ck)) = (pk_literal(pk_v), CkVal::from_value(ck_v)) else {
            continue;
        };
        by_pk.entry(pk_lit.clone()).or_default().push(ck);
        pk_lit_for.entry(pk_lit.clone()).or_insert(pk_lit);
    }
    let Some((pk_lit, cks_in_pk)) = by_pk.into_iter().max_by_key(|(_, v)| v.len()) else {
        eprintln!("Skipping: no comparable (pk, ck) pair decoded from {qualified}");
        return;
    };
    if cks_in_pk.len() < 2 {
        eprintln!(
            "Skipping: chosen partition pk={pk_lit} has <2 clustering rows; cannot exercise a slice"
        );
        return;
    }

    // Sort the partition's cks ascending (logical CQL order) for picking bounds.
    let mut sorted = cks_in_pk.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    // Helper: run a clustering predicate and return its decoded cks for this pk.
    let collect_cks = |rows: &[cqlite_core::query::result::QueryRow]| -> Vec<CkVal> {
        let mut v: Vec<CkVal> = rows
            .iter()
            .filter_map(|r| r.values.get(&t.ck_col).and_then(CkVal::from_value))
            .collect();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        v
    };
    let in_mem = |pred: &dyn Fn(&CkVal) -> bool| -> Vec<CkVal> {
        let mut v: Vec<CkVal> = sorted.iter().filter(|c| pred(c)).cloned().collect();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        v
    };

    // Pick a two-bound range [lo, hi) from the interior of the sorted cks.
    let lo = sorted[sorted.len() / 4].clone();
    let hi = sorted[(sorted.len() * 3) / 4].clone();
    // Single equality target: a value guaranteed present.
    let eq = sorted[sorted.len() / 2].clone();

    // Run a slice and report (rows, access_path). Resets the global probes.
    async fn run(
        db: &Database,
        qualified: &str,
        where_clause: &str,
        select_cols: &str,
    ) -> Result<
        (
            Vec<cqlite_core::query::result::QueryRow>,
            Option<AccessPath>,
        ),
        String,
    > {
        work_counters::reset();
        access_path::reset();
        let r = db
            .execute(&format!(
                "SELECT {select_cols} FROM {qualified} WHERE {where_clause}"
            ))
            .await
            .map_err(|e| format!("slice `{where_clause}` failed: {e}"))?;
        let path = r.metadata.access_path.clone();
        Ok((r.rows, path))
    }

    let select_cols = format!("{}, {}", t.pk_col, t.ck_col);

    // 7a. Two-bound contiguous range parity.
    {
        let where_clause = format!(
            "{pk} = {pkl} AND {ck} >= {lo} AND {ck} < {hi}",
            pk = t.pk_col,
            pkl = pk_lit,
            ck = t.ck_col,
            lo = lo.literal(),
            hi = hi.literal(),
        );
        let (rows, path) = run(&db, &qualified, &where_clause, &select_cols)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let returned = collect_cks(&rows);
        let lo2 = lo.clone();
        let hi2 = hi.clone();
        let expected = in_mem(&move |c| *c >= lo2 && *c < hi2);
        assert_eq!(
            returned, expected,
            "Issue #954 DESC: `{where_clause}` must equal the full-scan-filtered baseline",
        );
        assert!(
            matches!(
                path,
                Some(AccessPath::ClusteringSlice) | Some(AccessPath::PartitionLookup)
            ),
            "Issue #954 DESC: access path must be honest (ClusteringSlice or PartitionLookup), got {path:?}",
        );
        eprintln!(
            "DESC two-bound [{}, {}): returned {} rows, path {:?}",
            lo.literal(),
            hi.literal(),
            returned.len(),
            path
        );
    }

    // 7b. Single-bound `ck >= mid` parity (the DESC-sensitive shape: matching rows
    //     sort to the LOW physical-byte side and must NOT be skipped).
    {
        let where_clause = format!(
            "{pk} = {pkl} AND {ck} >= {v}",
            pk = t.pk_col,
            pkl = pk_lit,
            ck = t.ck_col,
            v = eq.literal(),
        );
        let (rows, path) = run(&db, &qualified, &where_clause, &select_cols)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let returned = collect_cks(&rows);
        let eqv = eq.clone();
        let expected = in_mem(&move |c| *c >= eqv);
        assert_eq!(
            returned, expected,
            "Issue #954 DESC: `{where_clause}` (single lower bound) must equal the baseline; \
             a missing DESC bound-swap would silently drop the low-physical-byte matches",
        );
        assert!(
            matches!(
                path,
                Some(AccessPath::ClusteringSlice) | Some(AccessPath::PartitionLookup)
            ),
            "Issue #954 DESC: access path must be honest, got {path:?}",
        );
        eprintln!(
            "DESC single-bound ck>={}: returned {} rows, path {:?}",
            eq.literal(),
            returned.len(),
            path
        );
    }

    // 7c. Equality `ck = mid` parity.
    {
        let where_clause = format!(
            "{pk} = {pkl} AND {ck} = {v}",
            pk = t.pk_col,
            pkl = pk_lit,
            ck = t.ck_col,
            v = eq.literal(),
        );
        let (rows, path) = run(&db, &qualified, &where_clause, &select_cols)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        let returned = collect_cks(&rows);
        let eqv = eq.clone();
        let expected = in_mem(&move |c| *c == eqv);
        assert_eq!(
            returned, expected,
            "Issue #954 DESC: `{where_clause}` (equality) must equal the baseline",
        );
        assert!(
            matches!(
                path,
                Some(AccessPath::ClusteringSlice) | Some(AccessPath::PartitionLookup)
            ),
            "Issue #954 DESC: access path must be honest, got {path:?}",
        );
        eprintln!(
            "DESC equality ck={}: returned {} rows, path {:?}",
            eq.literal(),
            returned.len(),
            path
        );
    }

    eprintln!(
        "Issue #954 DESC: end-to-end seek parity verified on {}.{} (first ck DESC).",
        t.keyspace, t.table
    );
}
