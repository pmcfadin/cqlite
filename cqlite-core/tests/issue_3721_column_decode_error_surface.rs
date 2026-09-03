//! Issue #3721 — a per-column decode failure must be OBSERVABLE, never a
//! successful `SELECT` with the failing column *and every later on-disk column*
//! silently missing.
//!
//! # The defect this pins
//!
//! Row assembly (`row_decoder/row_data.rs`) had exactly ONE error handler per
//! column path, and both were a bare `break`:
//!
//! * the COMPLEX arm — shared by `parse_complex_column_inner` (the compaction /
//!   elements-out read, under `if compaction_complex_out.is_some()`) and
//!   `parse_complex_column` (the user-facing read), which feed one `parse_result`
//!   binding and one `match`; and
//! * the SIMPLE arm, whose comment (`// CRITICAL FIX: Stop parsing remaining
//!   columns…`) was honest about the mechanism and silent about the consequence.
//!
//! `break` exits the per-column loop, after which the row is assembled from the
//! cells collected SO FAR and returned as `Ok`. The failing column and every later
//! on-disk column therefore vanish while the read reports SUCCESS —
//! indistinguishable from "those columns are genuinely null", which is the worst
//! shape a read bug can take because nothing downstream can defend against it.
//! Cassandra does not serve a short row: a cell it cannot read raises out of
//! `UnfilteredSerializer` and fails the read.
//!
//! # Measured before the fix (this file's own fixtures, `main` @ 394e25e5e)
//!
//! ```text
//! test_da.simple_table,     `name` declared INET  -> Ok, 3 rows, each missing
//!                                                    `name` AND `salary`
//! test_signed_coll.signed_int_collections,
//!                           `m` key declared BIGINT -> Ok, ZERO rows
//! ```
//!
//! Both exit 0. After the fix both are an `Error::ColumnDecode` naming the column.
//!
//! # How a decode failure is provoked without forging bytes
//!
//! Every fixture below is REAL Cassandra-written data; only the DECLARED type
//! supplied alongside it is varied, which is exactly the user-visible scenario the
//! issue measured (`cqlite --schema <cm as map<int,int>> --query …`) and exactly
//! the class Cassandra's own corruption checks (`inet` is 4 or 16 bytes, a fixed
//! width is that width, a composite must be fully consumed) raise on genuinely
//! damaged data. A CQLite-WRITTEN fixture could not be the oracle here: it would
//! be invariant to a uniform framing error on both sides (see the validation
//! playbook / #3042).
//!
//! Every assertion is at the PUBLIC surface (`Database::execute`, and
//! `SSTableReader::iterate_all_partitions_for_compaction` for the compaction arm),
//! because the whole defect is that the unit level looks fine.

use std::path::{Path, PathBuf};

use cqlite_core::error::ErrorCategory;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::access_path::AccessPath;
use cqlite_core::schema::parse_cql_schema;
use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::{Config, Database, Error, Platform};

#[path = "support/datasets_root.rs"]
mod datasets_root;

use datasets_root::{describe_search, sstables_root_for_table};

/// COMMITTED BIG (`nb`) fixture: `id int PRIMARY KEY, s set<int>, m map<int,text>`,
/// both collections NON-FROZEN (multicell), so `m` is decoded by the COMPLEX arm.
const COMPLEX_KEYSPACE: &str = "test_signed_coll";
const COMPLEX_TABLE: &str = "signed_int_collections";
const COMPLEX_DIR: &str = "signed_int_collections-c9762550748d11f1a94ae34493d77740";
const COMPLEX_DATA_DB: &str = "nb-1-big-Data.db";

/// COMMITTED BTI (`da`) fixture: `id uuid PRIMARY KEY, name text, age int,
/// salary bigint, active boolean, created timestamp`. Regular columns are stored
/// in alphabetical order, so `salary` is an on-disk column AFTER `name`.
const SIMPLE_KEYSPACE: &str = "test_da";
const SIMPLE_TABLE: &str = "simple_table";

/// Resolve a fixture root, or FAIL — never skip (issue #3220). Both fixtures are
/// force-added to git, so absence is a resolution defect, not a legitimate state.
fn fixture_root(keyspace: &str, table: &str) -> PathBuf {
    sstables_root_for_table(keyspace, table).unwrap_or_else(|| {
        panic!(
            "{keyspace}.{table} is COMMITTED to git and must resolve in every checkout, \
             unconditionally (issue #3220) — {}.\n  remedy: git restore --source=HEAD -- \
             test-data/datasets/sstables",
            describe_search(keyspace, table)
        )
    })
}

fn write_schema(dir: &Path, body: &str) -> PathBuf {
    let path = dir.join("schema.cql");
    std::fs::write(&path, body).expect("write scratch schema");
    path
}

async fn open_db(root: &Path, schema: &Path, keyspace: &str) -> Database {
    let cfg = IngestionConfig {
        schema_paths: vec![schema.to_path_buf()],
        data_dir: root.to_path_buf(),
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: Some(format!("/{keyspace}/")),
    };
    let result = ingest(cfg).await.expect("ingestion succeeds");
    assert!(
        result.schema_load_result.schemas_loaded > 0,
        "schema must load"
    );
    result.database
}

/// Run `SELECT *` over `keyspace.table` with `schema_body` declared for it.
async fn select_all(
    keyspace: &str,
    table: &str,
    schema_body: &str,
) -> Result<Vec<Vec<(String, String)>>, Error> {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = fixture_root(keyspace, table);
    let schema = write_schema(dir.path(), schema_body);
    let db = open_db(&root, &schema, keyspace).await;
    let result = db
        .execute(&format!("SELECT * FROM {keyspace}.{table}"))
        .await?;
    Ok(result
        .rows
        .iter()
        .map(|row| {
            let mut kv: Vec<(String, String)> = row
                .values
                .iter()
                .map(|(k, v)| (k.to_string(), format!("{v:?}")))
                .collect();
            kv.sort();
            kv
        })
        .collect())
}

/// Assert the error is the dedicated, MATCHABLE per-column variant naming
/// `column`, and that it carries the underlying cause as a `source` — i.e. that a
/// `tracing::debug!` is no longer the only record of the condition (AC6).
fn assert_column_decode(err: &Error, column: &str) {
    let Error::ColumnDecode {
        column: named,
        source,
        ..
    } = err
    else {
        panic!("expected Error::ColumnDecode for column '{column}', got: {err:?}");
    };
    assert_eq!(named, column, "the error must NAME the failing column");
    assert!(
        !source.to_string().is_empty(),
        "the underlying decode failure must be preserved as the error `source`"
    );
    // A machine-readable record, not just a log line: the variant classifies as
    // damaged data on both public taxonomies.
    assert_eq!(err.category(), ErrorCategory::Data);
    assert!(!err.is_recoverable());
    let rendered = err.to_string();
    assert!(
        rendered.contains(column),
        "the rendered error must name the column; got: {rendered}"
    );
}

/// Roborev blocker 3: `column_type` must name the type the decode was DISPATCHED
/// on — the SUPPLIED schema type for a matched column — and, where it differs, the
/// on-disk header marshal type alongside it. Reporting only the header type
/// misidentifies the cause exactly when the declaration is what is wrong.
fn assert_dispatch_type(err: &Error, declared: &str, on_disk: &str) {
    let Error::ColumnDecode { column_type, .. } = err else {
        panic!("expected Error::ColumnDecode, got: {err:?}");
    };
    assert!(
        column_type
            .to_ascii_lowercase()
            .contains(&declared.to_ascii_lowercase()),
        "the reported type must name the DISPATCH type `{declared}` (the supplied \
         schema type the failed decode ran on), got `{column_type}`"
    );
    assert!(
        column_type
            .to_ascii_lowercase()
            .contains(&on_disk.to_ascii_lowercase()),
        "the reported type must also name the on-disk header marshal type \
         `{on_disk}`, so the report identifies both the dispatch that failed and \
         the bytes it was pointed at; got `{column_type}`"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Schemas. Each pair is IDENTICAL except for the one declared type that provokes
// a decode failure, so the difference in outcome is attributable to that type.
// ─────────────────────────────────────────────────────────────────────────────

const COMPLEX_HEADER: &str = "\
CREATE KEYSPACE IF NOT EXISTS test_signed_coll WITH replication = \
{'class': 'SimpleStrategy', 'replication_factor': 1};
USE test_signed_coll;
";

/// The fixture's real shape.
const COMPLEX_TABLE_CORRECT: &str = "\
CREATE TABLE IF NOT EXISTS test_signed_coll.signed_int_collections (
    id INT PRIMARY KEY,
    s  SET<INT>,
    m  MAP<INT, TEXT>
);
";

/// `m`'s KEY declared `BIGINT` (8 bytes) while the on-disk cell paths are 4-byte
/// `int` keys, so `parse_cell_path_key` returns `Err` on the FIRST element of `m`.
/// `m` sorts before `s`, so the pre-fix swallow dropped BOTH columns.
const COMPLEX_TABLE_WRONG_KEY: &str = "\
CREATE TABLE IF NOT EXISTS test_signed_coll.signed_int_collections (
    id INT PRIMARY KEY,
    s  SET<INT>,
    m  MAP<BIGINT, TEXT>
);
";

const SIMPLE_HEADER: &str = "\
CREATE KEYSPACE IF NOT EXISTS test_da WITH replication = \
{'class': 'SimpleStrategy', 'replication_factor': 1};
USE test_da;
";

const SIMPLE_TABLE_CORRECT: &str = "\
CREATE TABLE IF NOT EXISTS test_da.simple_table (
    id UUID PRIMARY KEY,
    name TEXT,
    age INT,
    salary BIGINT,
    active BOOLEAN,
    created TIMESTAMP
);
";

/// `name` declared `INET`, whose decode enforces Cassandra's 4-or-16-byte address
/// width; the on-disk `name` values are longer text. `salary` sorts after `name`,
/// so it is the "later on-disk column" the pre-fix swallow also dropped.
const SIMPLE_TABLE_WRONG_TYPE: &str = "\
CREATE TABLE IF NOT EXISTS test_da.simple_table (
    id UUID PRIMARY KEY,
    name INET,
    age INT,
    salary BIGINT,
    active BOOLEAN,
    created TIMESTAMP
);
";

fn complex_schema(table: &str) -> String {
    format!("{COMPLEX_HEADER}{table}")
}

fn simple_schema(table: &str) -> String {
    format!("{SIMPLE_HEADER}{table}")
}

// ─────────────────────────────────────────────────────────────────────────────
// AC1 + AC5 + AC7 — both sites, at the public read surface.
// ─────────────────────────────────────────────────────────────────────────────

/// COMPLEX site (`row_data.rs`'s complex `Err` arm). Pre-fix this `SELECT`
/// returned `Ok` with ZERO rows: `m` failed, `break` left the loop, the row had no
/// data cells left and disappeared — a successful query over a fixture that
/// demonstrably has a row.
#[tokio::test]
async fn complex_column_decode_failure_fails_the_select_naming_the_column() {
    let err = select_all(
        COMPLEX_KEYSPACE,
        COMPLEX_TABLE,
        &complex_schema(COMPLEX_TABLE_WRONG_KEY),
    )
    .await
    .expect_err(
        "a complex column that cannot be decoded must FAIL the read — pre-#3721 this \
         returned Ok with the column, its successors and (here) the whole row missing",
    );
    assert_column_decode(&err, "m");
}

/// SIMPLE site (`row_data.rs`'s simple `Err` arm — the `// CRITICAL FIX` comment).
/// Pre-fix this `SELECT` returned `Ok` with 3 rows, each carrying
/// `active`/`age`/`created`/`id` and missing BOTH `name` (the failing column) and
/// `salary` (the later on-disk column) — the exact "successful SELECT with missing
/// columns" symptom.
#[tokio::test]
async fn simple_column_decode_failure_fails_the_select_naming_the_column() {
    let err = select_all(
        SIMPLE_KEYSPACE,
        SIMPLE_TABLE,
        &simple_schema(SIMPLE_TABLE_WRONG_TYPE),
    )
    .await
    .expect_err(
        "a simple column that cannot be decoded must FAIL the read — pre-#3721 this \
         returned Ok with `name` and `salary` silently absent",
    );
    assert_column_decode(&err, "name");
}

/// The POINT read path must answer identically to the full scan (issue #1918's
/// differential contract): both reach the same `parse_block_emit*` decode, so a
/// column decode failure that fails one must fail the other. A point read that
/// still swallowed would be worse than the original defect — the same query would
/// succeed or fail depending on which path was chosen.
#[tokio::test]
async fn a_targeted_point_read_fails_the_same_way_as_the_full_scan() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = fixture_root(COMPLEX_KEYSPACE, COMPLEX_TABLE);
    let schema = write_schema(dir.path(), &complex_schema(COMPLEX_TABLE_WRONG_KEY));
    let db = open_db(&root, &schema, COMPLEX_KEYSPACE).await;
    let err = db
        .execute(&format!(
            "SELECT * FROM {COMPLEX_KEYSPACE}.{COMPLEX_TABLE} WHERE id = 1"
        ))
        .await
        .expect_err("a partition-targeted read must surface the same failure");
    assert_column_decode(&err, "m");
}

// ─────────────────────────────────────────────────────────────────────────────
// AC4 — the fix must not become "abort the row" for columns that decode fine.
//
// These are the CONTROLS for the two cases above: the SAME fixture, the SAME
// query, the SAME code path, differing only in the one declared type. Every
// on-disk column — including the ones that come AFTER the column mis-declared in
// the sibling case — is returned unchanged. Without them, a fix that failed every
// read would pass the two cases above.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_decodable_complex_column_leaves_every_column_of_the_row_intact() {
    let rows = select_all(
        COMPLEX_KEYSPACE,
        COMPLEX_TABLE,
        &complex_schema(COMPLEX_TABLE_CORRECT),
    )
    .await
    .expect("correct-schema SELECT must succeed");
    assert_eq!(rows.len(), 1, "present fixture must return its row");
    let names: Vec<&str> = rows[0].iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(
        names,
        vec!["id", "m", "s"],
        "every on-disk column must be present, including `s`, which follows the \
         column the sibling case mis-declares"
    );
    let values: Vec<&str> = rows[0].iter().map(|(_, v)| v.as_str()).collect();
    assert!(
        values[1].starts_with("Map(["),
        "`m` must decode as a map, got {}",
        values[1]
    );
    assert!(
        values[2].starts_with("Set(["),
        "`s` must decode as a set, got {}",
        values[2]
    );
}

#[tokio::test]
async fn a_decodable_simple_column_leaves_every_column_of_the_row_intact() {
    let rows = select_all(
        SIMPLE_KEYSPACE,
        SIMPLE_TABLE,
        &simple_schema(SIMPLE_TABLE_CORRECT),
    )
    .await
    .expect("correct-schema SELECT must succeed");
    assert_eq!(rows.len(), 3, "present fixture must return its 3 rows");
    for row in &rows {
        let names: Vec<&str> = row.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(
            names,
            vec!["active", "age", "created", "id", "name", "salary"],
            "every on-disk column must be present, including `name` (mis-declared in \
             the sibling case) and `salary` (the column after it)"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// AC3 — the compaction / elements-out arm.
//
// `iterate_all_partitions_for_compaction` is the entry point that drives
// `row_data.rs`'s `if compaction_complex_out.is_some()` branch (via
// `CompactionPolicy::on_data_row`). Pre-fix it received the same truncated row and
// answered its caller `Some(next_offset)` / `None` with NO error — so compaction
// could have WRITTEN OUT a row missing the failing column and every later one,
// turning a read defect into durable data loss.
// ─────────────────────────────────────────────────────────────────────────────

async fn open_complex_reader() -> SSTableReader {
    let root = fixture_root(COMPLEX_KEYSPACE, COMPLEX_TABLE);
    let data_db = root
        .join(COMPLEX_KEYSPACE)
        .join(COMPLEX_DIR)
        .join(COMPLEX_DATA_DB);
    assert!(
        data_db.is_file(),
        "committed fixture {} must exist (issue #3220)",
        data_db.display()
    );
    let cfg = Config::default();
    let platform = std::sync::Arc::new(Platform::new(&cfg).await.expect("platform"));
    SSTableReader::open(&data_db, &cfg, platform)
        .await
        .expect("open committed nb fixture")
}

#[tokio::test]
async fn compaction_read_surfaces_the_column_decode_failure_to_its_caller() {
    let reader = open_complex_reader().await;

    // Control first: with the fixture's real shape the compaction read succeeds and
    // yields the row, so the failure below is attributable to the declared type and
    // not to this entry point being broken.
    let good = parse_cql_schema(COMPLEX_TABLE_CORRECT).expect("parse schema");
    let rows = reader
        .iterate_all_partitions_for_compaction(Some(&good))
        .await
        .expect("compaction read of the correct-schema fixture");
    assert!(
        !rows.is_empty(),
        "present fixture must yield rows on the compaction path — 0 rows is a read \
         regression, never a pass"
    );

    let bad = parse_cql_schema(COMPLEX_TABLE_WRONG_KEY).expect("parse schema");
    let err = reader
        .iterate_all_partitions_for_compaction(Some(&bad))
        .await
        .expect_err(
            "the compaction / elements-out arm must SURFACE a column decode failure — \
             pre-#3721 it handed back a truncated row with no error (issue #3721 AC3)",
        );
    assert_column_decode(&err, "m");
}

#[tokio::test]
async fn streaming_compaction_read_surfaces_the_column_decode_failure() {
    let reader = open_complex_reader().await;
    let bad = parse_cql_schema(COMPLEX_TABLE_WRONG_KEY).expect("parse schema");
    let cancel = cqlite_core::storage::scan_cancel::ScanCancel::default();
    let mut seen = 0usize;
    let err = reader
        .stream_all_partitions_for_compaction(Some(&bad), &cancel, |_row| {
            seen += 1;
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .await
        .expect_err(
            "the STREAMING compaction driver must surface the same condition as the \
             buffered one — they share `CompactionPolicy::on_data_row`",
        );
    assert_column_decode(&err, "m");
    assert_eq!(
        seen, 0,
        "no truncated row may be emitted before the failure is surfaced"
    );
}

/// The complex-arm cases describe their fixture as the BIG (`nb`) format; pin that,
/// so a corpus reshuffle cannot silently retarget the lane at a different format and
/// leave the prose above asserting something untrue.
#[tokio::test]
async fn fixture_identity_is_pinned() {
    let reader = open_complex_reader().await;
    assert_eq!(
        reader.format_version().expect("format version"),
        "nb",
        "the complex-arm fixture must be the BIG `nb` format this file describes"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Roborev blocker 1 — the cell-flags byte is NOT an end-of-cells terminator.
//
// A revision of the fix kept a `not_a_cell` predicate: a simple column whose cell
// flags byte exceeded `0x1F` `break`ed the column loop, so a SHORT ROW was again
// reported as a successful read — the original defect in a narrower place.
//
// No such marker exists in the format. At `cassandra-5.0.8`,
// `db/rows/UnfilteredSerializer.deserializeRowBody` fixes the column set BEFORE
// any cell is read (`hasAllColumns ? headerColumns :
// Columns.serializer.deserializeSubset(headerColumns, in)`) and iterates exactly
// that set; cell reading is bounded by the columns bitmap / subset encoding, never
// by a sentinel flags value. `db/rows/Cell.Serializer` defines five bits
// (`0x01|0x02|0x04|0x08|0x10` = `0x1F`) and ignores the rest.
//
// Provoked WITHOUT forging bytes, exactly like every other case in this file: a
// declared type WIDER than the on-disk one over-consumes, so the cursor lands
// inside the following cell and the next flags byte carries a row-flag bit. Which
// byte it lands on is data, but that a mis-sized dispatch misaligns the cursor is
// the framing contract, not a byte pattern (issue #28).
//
// MEASURED at `feb5aee62` (the revision under review): each case below returned
// `Ok` with 3 rows and the trailing on-disk columns silently absent.
// ─────────────────────────────────────────────────────────────────────────────

/// `active` is a 1-byte `boolean` on disk; declared `INT` the scalar decode
/// consumes 4, leaving the cursor inside the next cell. `created` is the on-disk
/// column after it (regulars are alphabetical: active, age, created, name,
/// salary), so its flags byte is read from the middle of a value.
const SIMPLE_TABLE_OVERWIDE: &str = "\
CREATE TABLE IF NOT EXISTS test_da.simple_table (
    id UUID PRIMARY KEY,
    name TEXT,
    age INT,
    salary BIGINT,
    active INT,
    created TIMESTAMP
);
";

#[tokio::test]
async fn a_misaligning_cell_flags_byte_fails_the_read_instead_of_ending_the_row() {
    let err = select_all(
        SIMPLE_KEYSPACE,
        SIMPLE_TABLE,
        &simple_schema(SIMPLE_TABLE_OVERWIDE),
    )
    .await
    .expect_err(
        "a cell-flags byte outside Cassandra's five-bit set is a DECODE FAILURE, not \
         an end-of-cells marker — at feb5aee62 this returned Ok with 3 rows and the \
         trailing on-disk columns silently missing (roborev blocker 1)",
    );
    assert_column_decode(&err, "created");
    // The condition really is the flags-byte one this case exists for, not some
    // other failure that happens to abort the same read.
    let Error::ColumnDecode { source, .. } = &err else {
        unreachable!("assert_column_decode already matched the variant");
    };
    let cause = source.to_string();
    assert!(
        cause.contains("invalid cell flags"),
        "the surfaced cause must be the misaligned cell-flags rejection \
         (`cell_value.rs`, issue #191), got: {cause}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Roborev blocker 2 — an INDEX-POSITIONED walk may not answer with partial rows.
//
// `end_of_partition_or_bail` took a `resynchronising_walk` flag and, when set,
// folded every `ColumnDecode` back into the `Ok(())` end-of-partition signal — so
// the clustering-slice and reverse-index read paths returned PARTIAL RESULTS
// SILENTLY. That is the original defect surviving on exactly the public paths a
// wide-partition `SELECT` takes, and the 434-line first draft of this file never
// reached them: its fixtures are single-block partitions with no promoted index.
//
// Fixture: the committed Cassandra `test_big.wide_partition` (`nb`), pk=1 holding
// 290 live rows over ~600 KiB, for which Cassandra emitted a MULTI-BLOCK promoted
// `IndexInfo` array — so `WHERE pk = 1 AND ck > … AND ck < …` engages the
// promoted-index window and `ORDER BY ck DESC` the back-to-front block walk.
//
// MEASURED at `feb5aee62`, `payload` declared `INET`: the slice and the reverse
// read each returned `Ok` with ZERO rows, while the SAME query without the index
// narrowing (`WHERE pk = 1`) correctly returned `Error::ColumnDecode`. One query
// therefore succeeded or failed according to which read path was chosen.
// ─────────────────────────────────────────────────────────────────────────────

const WIDE_KEYSPACE: &str = "test_big";
const WIDE_TABLE: &str = "wide_partition";

const WIDE_HEADER: &str = "\
CREATE KEYSPACE IF NOT EXISTS test_big WITH replication = \
{'class': 'SimpleStrategy', 'replication_factor': 1};
USE test_big;
";

/// The fixture's real shape.
const WIDE_TABLE_CORRECT: &str = "\
CREATE TABLE IF NOT EXISTS test_big.wide_partition (
    pk INT,
    ck INT,
    payload TEXT,
    PRIMARY KEY (pk, ck)
);
";

/// `payload` declared `INET`, whose decode enforces Cassandra's 4-or-16-byte
/// address width against on-disk `text` values of ~2 KiB.
const WIDE_TABLE_WRONG_TYPE: &str = "\
CREATE TABLE IF NOT EXISTS test_big.wide_partition (
    pk INT,
    ck INT,
    payload INET,
    PRIMARY KEY (pk, ck)
);
";

fn wide_schema(table: &str) -> String {
    format!("{WIDE_HEADER}{table}")
}

const SLICE_QUERY: &str =
    "SELECT * FROM test_big.wide_partition WHERE pk = 1 AND ck > 100 AND ck < 140";
const REVERSE_QUERY: &str = "SELECT * FROM test_big.wide_partition WHERE pk = 1 ORDER BY ck DESC";
const FULL_PARTITION_QUERY: &str = "SELECT * FROM test_big.wide_partition WHERE pk = 1";

/// Run one query over the wide-partition fixture with `schema_body` declared,
/// returning its `ck` values in the order the query produced them, plus the
/// reported access path.
async fn wide_query_with_path(
    schema_body: &str,
    query: &str,
) -> Result<(Vec<i32>, Option<AccessPath>), Error> {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = fixture_root(WIDE_KEYSPACE, WIDE_TABLE);
    let schema = write_schema(dir.path(), schema_body);
    let db = open_db(&root, &schema, WIDE_KEYSPACE).await;
    let result = db.execute(query).await?;
    let cks = result
        .rows
        .iter()
        .filter_map(|r| match r.values.get("ck") {
            Some(cqlite_core::types::Value::Integer(i)) => Some(*i),
            _ => None,
        })
        .collect();
    Ok((cks, result.metadata.access_path))
}

async fn wide_query(schema_body: &str, query: &str) -> Result<Vec<i32>, Error> {
    wide_query_with_path(schema_body, query)
        .await
        .map(|(cks, _)| cks)
}

#[tokio::test]
async fn a_clustering_slice_read_surfaces_the_failure_instead_of_returning_partial_rows() {
    let err = wide_query(&wide_schema(WIDE_TABLE_WRONG_TYPE), SLICE_QUERY)
        .await
        .expect_err(
            "an index-positioned clustering-slice read must abandon the optimization \
             and surface the decode failure — at feb5aee62 it returned Ok with ZERO \
             rows (roborev blocker 2)",
        );
    assert_column_decode(&err, "payload");
    assert_dispatch_type(&err, "inet", "text");
}

#[tokio::test]
async fn a_reverse_index_read_surfaces_the_failure_instead_of_returning_partial_rows() {
    let err = wide_query(&wide_schema(WIDE_TABLE_WRONG_TYPE), REVERSE_QUERY)
        .await
        .expect_err(
            "the promoted-index reverse block walk must abandon the optimization and \
             surface the decode failure — at feb5aee62 it returned Ok with ZERO rows",
        );
    assert_column_decode(&err, "payload");
}

/// The three read paths must AGREE. Without this the two cases above could be
/// satisfied by a fix that failed every wide-partition read, and the property the
/// issue is about — one query answering differently depending on which path was
/// chosen — would still be untested from the other side.
#[tokio::test]
async fn every_wide_partition_read_path_answers_the_same_way() {
    for query in [SLICE_QUERY, REVERSE_QUERY, FULL_PARTITION_QUERY] {
        let err = wide_query(&wide_schema(WIDE_TABLE_WRONG_TYPE), query)
            .await
            .expect_err("every read path must surface the failure: {query}");
        assert_column_decode(&err, "payload");
    }
}

// ─── CONTROLS: the fallback must cost the ROWS nothing, only the fast path ───

/// The correct-schema slice still returns exactly its rows. This is the case the
/// earlier revision's `resynchronising_walk` suppression existed to protect, and
/// the one a naive "propagate everywhere" fix breaks: the promoted-index window
/// over-reads into a `text` value on this real fixture, so the windowed walk DOES
/// fail here. Retracting the narrowing and re-reading the full partition returns
/// the same 39 rows the pre-fix swallow returned — measured identical at
/// `feb5aee62` and at HEAD.
#[tokio::test]
async fn a_correct_schema_clustering_slice_still_returns_every_row() {
    let (mut cks, path) = wide_query_with_path(&wide_schema(WIDE_TABLE_CORRECT), SLICE_QUERY)
        .await
        .expect("correct-schema clustering slice must succeed");
    // The promoted-index narrowing must still be ENGAGED, not merely correct by
    // falling back. This is what separates "the over-run was eliminated" from "the
    // over-run is compensated for": a phantom-partition over-run would trip
    // `indexed_walk_falls_back`, retract the narrowing and report `PartitionLookup`.
    assert_eq!(
        path,
        Some(AccessPath::ClusteringSlice),
        "the clustering-slice fast path must survive the fix — a fallback here would \
         mean the windowed walk is still over-running a real row boundary"
    );
    cks.sort_unstable();
    assert_eq!(
        cks,
        (101..140).collect::<Vec<i32>>(),
        "ck in (100,140) must return 101..=139 — the fallback may cost the fast \
         path, never a row"
    );
}

#[tokio::test]
async fn a_correct_schema_reverse_read_still_returns_every_row_in_order() {
    let cks = wide_query(&wide_schema(WIDE_TABLE_CORRECT), REVERSE_QUERY)
        .await
        .expect("correct-schema reverse read must succeed");
    let mut ascending = cks.clone();
    ascending.sort_unstable();
    assert_eq!(
        cks.len(),
        290,
        "pk=1 holds 290 live rows (ck 0..299 minus the range tombstone over 30..39)"
    );
    assert_eq!(
        cks,
        ascending.iter().rev().copied().collect::<Vec<i32>>(),
        "ORDER BY ck DESC must return the identical set in exact reverse order"
    );
}

#[tokio::test]
async fn a_correct_schema_full_partition_read_is_unchanged() {
    let cks = wide_query(&wide_schema(WIDE_TABLE_CORRECT), FULL_PARTITION_QUERY)
        .await
        .expect("correct-schema full-partition read must succeed");
    assert_eq!(cks.len(), 290, "pk=1 holds 290 live rows");
}

// ─────────────────────────────────────────────────────────────────────────────
// Roborev blocker 3 — the reported type is the DISPATCH type.
// ─────────────────────────────────────────────────────────────────────────────

/// A matched SIMPLE column dispatches on the SUPPLIED schema type
/// (`ColumnToParse.kind`, resolved from `schema.map(data_type)`), so reporting the
/// on-disk header marshal type alone points a reader at the data when it is the
/// DECLARATION that is wrong. Pre-fix this read `text`.
#[tokio::test]
async fn the_reported_type_is_the_simple_columns_dispatch_type() {
    let err = select_all(
        SIMPLE_KEYSPACE,
        SIMPLE_TABLE,
        &simple_schema(SIMPLE_TABLE_WRONG_TYPE),
    )
    .await
    .expect_err("mis-declared `name` must fail the read");
    assert_dispatch_type(&err, "inet", "text");
}

/// A COMPLEX column decodes its container from the on-disk header type (#1081)
/// but its cell-path KEY and ELEMENT from the supplied schema type — and the
/// key/element parameters a caller must fix live only in the latter. Pre-fix this
/// reported the on-disk type alone, naming neither the declared `bigint` key nor
/// the fact that the declaration was the failing dispatch. The two spellings must
/// BOTH appear, because the whole point is to tell them apart: the report reads
/// `map<BIGINT, TEXT> (on-disk map<int, text>)`.
#[tokio::test]
async fn the_reported_type_names_the_complex_columns_declared_key_type() {
    let err = select_all(
        COMPLEX_KEYSPACE,
        COMPLEX_TABLE,
        &complex_schema(COMPLEX_TABLE_WRONG_KEY),
    )
    .await
    .expect_err("mis-declared map key must fail the read");
    assert_dispatch_type(&err, "map<bigint, text>", "map<int, text>");
}

// ─────────────────────────────────────────────────────────────────────────────
// Roborev job 10 — the FIFTH swallow site: the row body runs OUT OF BYTES while
// the row's own column set still names a column as PRESENT.
//
// `row_data.rs`'s per-column loop opened with
//
// ```text
// if offset >= data.len() {
//     tracing::debug!("… Reached end of data at column {} ('{}'), parsed {}/{} …");
//     break;
// }
// ```
//
// — a `break`, so row assembly returned `Ok` with that column and every successor
// missing, while the log line ADMITTED having decoded fewer cells than the column
// set names. Same shape as the four sites above, reached by a different route.
//
// This is TRUNCATED OR CORRUPT DATA, never a legitimate boundary. At
// `cassandra-5.0.8`, `db/rows/UnfilteredSerializer.deserializeRowBody` fixes the
// column set BEFORE any cell is read (`hasAllColumns ? headerColumns :
// Columns.serializer.deserializeSubset(headerColumns, in)`) and `columns.apply(…)`
// iterates exactly that set; running out of input mid-set raises out of the
// serializer. Cassandra serves no short row.
//
// The legitimate end of a row is a DIFFERENT control-flow path and is untouched: a
// row whose column set is fully consumed leaves the `for` loop normally and never
// evaluates this condition. It is also evaluated only AFTER the
// missing-columns-bitmap skip, so every column reaching it is one the row itself
// declares has cell bytes on disk. The controls at the end of this section pin
// that the reads which SHOULD succeed still do.
//
// # What reaches this site, measured — read this before adding a case
//
// A merely SHORT row does NOT reach it: `row_data.rs` already rejects
// `row_size > available` upstream (issue #2481), so a row whose declared extent
// runs past the buffer fails there. The condition here needs a row whose FRAMING
// places its column set's first present column at exactly the end of the buffer —
// i.e. a cursor that is wrong, which is corruption evidence and is exactly what the
// `column_decode_error` module says may not be answered with a short row.
//
// It is provoked by TRUNCATING a copy of a real Cassandra-written `Data.db` at a
// PINNED length (nothing is synthesized; the bytes that remain are Cassandra's, and
// a CQLite-written fixture could not be the oracle — #3042). The scanner rejects the
// genuinely truncated row upstream, resynchronises past it, and lands on a framing
// whose first present column has no bytes left. Each case asserts the untruncated
// size, so a regenerated fixture fails loudly instead of silently testing something
// else.
//
// MEASURED with the site restored to its `break`: both cases below return
// `Ok` with ZERO rows — a SUCCESSFUL `SELECT` reporting no data at all over a
// damaged SSTable that does hold a row. After the fix both are an
// `Error::ColumnDecode` naming the column, its type and the offset.
//
// The scratch copy omits the CHECKSUM components (`CRC.db`, `Digest.crc32`). They
// are integrity metadata over the file's bytes, not part of the row format, and they
// are a SEPARATE layer: an uncompressed table's `CRC.db` rejects this truncation
// before the decoder sees it, which is correct and is not the property under test —
// the row decoder must be sound on the bytes it is handed, and it is handed them
// whenever that layer is absent (a BTI/compressed table writes no `CRC.db`) or the
// damage is chunk-aligned. The `…_without_its_checksum_components` controls prove
// the omission is not itself what fails the read.
//
// # NOT covered here: the compaction path (reported, not faked)
//
// `iterate_all_partitions_for_compaction` cannot reach this site today, and no test
// below pretends otherwise. Its driver does not resynchronise, so on the same
// truncated fixtures the #2481 `row_size > available` rejection above is the last
// thing that happens and the driver folds that `Err` into "end of partition" — a
// SEPARATE swallow (`Ok` with zero rows), out of scope for this change. Measured
// over cuts 0..60 of six committed uncompressed fixtures: not one reaches this site
// via the compaction entry point. The compaction arm of the propagation itself is
// covered by `compaction_read_surfaces_the_column_decode_failure_to_its_caller` and
// `streaming_compaction_read_surfaces_the_column_decode_failure` above.
// ─────────────────────────────────────────────────────────────────────────────

/// # The SIMPLE-arm truncation case moved, and why (issue #3721)
///
/// This fixture (`id int PRIMARY KEY, fs frozen<set<int>>, fm frozen<map<int,text>>`
/// — frozen collections are single-cell, so both regular columns take the SIMPLE
/// arm) used to carry the SIMPLE-arm `row body exhausted` case at `keep = 108`. It
/// no longer can, and the reason is worth stating because it is not a fixture
/// regression:
///
/// The truncation makes the REAL partition's row fail on row-size framing (#2481,
/// `row_size=124 … exceeds available data (88 bytes remain)`), which is correctly
/// read as end-of-partition, so the scan RESYNCHRONISES and decodes phantom
/// partitions out of the middle of the real row's cell values (measured: phantom
/// headers at 19, 58, 85). The `row body exhausted` this case asserted came from the
/// LAST of those phantoms. Byte `0x4f` of that same row is the `z` of the text value
/// `"zero"` — `0x7a`, which has `IS_MARKER` (`0x02`) set — so the resync walk reaches
/// a range-tombstone MARKER at offset 79 before any phantom row can run out of
/// bytes, and the marker layer answers first.
///
/// That marker is therefore NOT a second, removable defect: it is the truncation's
/// own artifact at a fixed offset inside the fixture's real payload. RE-MEASURED
/// exhaustively over every `keep` in `20..=145` after the read path stopped
/// converting a marker PARSE failure into a partition terminator (roborev job 78):
///
/// ```text
///  20..= 79  Ok(0 rows)              the separate row-framing swallow this file's
///                                    header already records as out of scope
///  80..=143  Err marker-unparseable  the marker at offset 79 cannot be PARSED
/// 144..=145  Ok(1 row)               the untruncated fixture reads cleanly
/// ```
///
/// Both boundaries and the CLASS moved with that fix, which is why the table is
/// dated: the refusal used to come from the shadow FSM rejecting the bound kind
/// (`0x65` = 101, the `e` of `"zero"`) over `87..=143`, and now comes from the
/// marker's own body not parsing over `80..=143` — 7 more keeps, and one layer
/// earlier. `144..=145` is the load-bearing row: the UNTRUNCATED fixture still
/// reads cleanly, so nothing here refuses a correct read.
///
/// There is no `keep` that isolates the column-level condition, so per the split
/// this case pins the MARKER-level refusal on these bytes, and the column-level
/// `row body exhausted` pin is the COMPLEX case below — whose fixture is
/// marker-clean at every `keep` in `20..=109`, RE-MEASURED under the same fix
/// (`20..=78` `Ok(0)`, `79..=107` the row-level refusal, `108..=109` `Ok(1)`).
const RESYNC_MARKER_TABLE: &str = "frozen_int_collections";
const RESYNC_MARKER_DIR: &str = "frozen_int_collections-c9820c30748d11f1a94ae34493d77740";
/// The fixture's untruncated size, asserted so a corpus change cannot silently move
/// the truncation onto some other condition.
const RESYNC_MARKER_FULL_LEN: usize = 145;
/// Truncated length whose resync walk reaches the payload byte read as a marker.
const RESYNC_MARKER_KEEP: usize = 108;

const RESYNC_MARKER_SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS test_signed_coll.frozen_int_collections (
    id INT PRIMARY KEY,
    fs FROZEN<SET<INT>>,
    fm FROZEN<MAP<INT, TEXT>>
);
";

/// COMPLEX-arm subject: the same fixture the complex cases above use. Since the
/// SIMPLE-arm truncation case moved to the marker level (see
/// [`RESYNC_MARKER_TABLE`]), this is the ONE remaining pin on `row_body_exhausted`,
/// and it holds because this fixture's resync walk is marker-clean: MEASURED over
/// every `keep` in `20..=109`, no read of it produces a range-tombstone marker
/// refusal — `79` is the `row body exhausted` under test, `80..=107` are
/// `Complex cell m.0: invalid flags 0xff`, and `108..=109` read cleanly. Changing
/// the keep below without re-measuring can silently reintroduce the collision.
const TRUNC_COMPLEX_FULL_LEN: usize = 109;
const TRUNC_COMPLEX_KEEP: usize = 79;

/// Copy `keyspace/dir` into a scratch tree, truncating `Data.db` to `keep` bytes
/// (`None` = untruncated) and omitting the checksum components. The `TempDir` is
/// returned with the root: dropping it deletes the tree.
fn truncated_copy(
    keyspace: &str,
    table: &str,
    dir: &str,
    full_len: usize,
    keep: Option<usize>,
) -> (tempfile::TempDir, PathBuf) {
    let src = fixture_root(keyspace, table).join(keyspace).join(dir);
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().join("sstables");
    let dst = root.join(keyspace).join(dir);
    std::fs::create_dir_all(&dst).expect("scratch dir");
    for entry in std::fs::read_dir(&src).expect("read committed fixture dir") {
        let entry = entry.expect("dir entry");
        let name = entry.file_name().to_string_lossy().to_string();
        // Checksum components only — see the section header.
        if name.contains("CRC") || name.contains("Digest") {
            continue;
        }
        let bytes = std::fs::read(entry.path()).expect("read component");
        if name.ends_with("-Data.db") {
            assert_eq!(
                bytes.len(),
                full_len,
                "{keyspace}.{table}'s Data.db is {} bytes, not the {full_len} this case \
                 pins — the fixture was regenerated and the truncation length below no \
                 longer selects the condition under test",
                bytes.len()
            );
            let keep = keep.unwrap_or(bytes.len());
            std::fs::write(dst.join(&name), &bytes[..keep]).expect("write truncated Data.db");
        } else {
            std::fs::write(dst.join(&name), &bytes).expect("write component");
        }
    }
    (tmp, root)
}

/// `SELECT *` over a scratch copy — the PUBLIC read path.
async fn select_all_truncated(
    keyspace: &str,
    table: &str,
    dir: &str,
    full_len: usize,
    keep: Option<usize>,
    schema_table: &str,
) -> Result<usize, Error> {
    let (tmp, root) = truncated_copy(keyspace, table, dir, full_len, keep);
    let schema = write_schema(tmp.path(), &complex_schema(schema_table));
    let db = open_db(&root, &schema, keyspace).await;
    let result = db
        .execute(&format!("SELECT * FROM {keyspace}.{table}"))
        .await?;
    Ok(result.rows.len())
}

/// The exhaustion must be the per-column variant AND must say what it measured:
/// which column of how many was still outstanding, and where the body ran out. A
/// bare "corrupt SSTable" would leave an operator no better off than the
/// `tracing::debug!` this replaces.
fn assert_row_body_exhausted(err: &Error, column: &str, at: usize, on_disk_columns: usize) {
    assert_column_decode(err, column);
    let Error::ColumnDecode { offset, source, .. } = err else {
        unreachable!("assert_column_decode already matched the variant");
    };
    assert_eq!(
        *offset, at,
        "the failure must be reported at the byte where the body ran out"
    );
    let cause = source.to_string();
    assert!(
        cause.contains("row body exhausted"),
        "the surfaced cause must be the row-body-exhaustion one this case exists for, \
         not some other failure aborting the same read; got: {cause}"
    );
    assert!(
        cause.contains(&format!("of {on_disk_columns}")),
        "the cause must say how much of the column set was outstanding (`of \
         {on_disk_columns}`), which is what distinguishes truncation from the end of a \
         row; got: {cause}"
    );
}

/// A truncation whose RESYNC walk reads real payload as a range-tombstone marker
/// must FAIL the read, not truncate the partition (issue #3721, marker level).
///
/// Named for what it pins, which has now moved TWICE — see the comment in the body
/// for both moves. [`RESYNC_MARKER_TABLE`] carries the exhaustive `keep`
/// measurement showing this fixture can no longer reach the column-level condition,
/// and why the unframeable marker is the truncation's own artifact rather than a
/// second defect that could be removed from the fixture.
///
/// Pre-#3721 EVERY marker-`Err` arm on the read path answered with the framing
/// terminator — a bare `break` in the two buffered block loops, `MarkerOutcome::Stop`
/// in the streaming policy — so this read returned `Ok`, dropping the marker and
/// every later row of the partition from a SUCCESSFUL query: the same shape as the
/// column-level swallow this file's other cases pin, one structural level up.
#[tokio::test]
async fn a_truncation_whose_resync_walk_lands_on_an_unframeable_marker_fails_the_select() {
    let err = select_all_truncated(
        COMPLEX_KEYSPACE,
        RESYNC_MARKER_TABLE,
        RESYNC_MARKER_DIR,
        RESYNC_MARKER_FULL_LEN,
        Some(RESYNC_MARKER_KEEP),
        RESYNC_MARKER_SCHEMA,
    )
    .await
    .expect_err(
        "a truncation whose resync walk lands on bytes it cannot frame as a well-formed \
         unfiltered is damaged data and must FAIL the read — pre-#3721 this `break`ed and \
         reported a SUCCESSFUL query, dropping every later row of the partition",
    );
    // WHICH LAYER REFUSES CHANGED, AND THE SUBJECT CHANGED WITH IT (roborev job 75).
    //
    // This case used to reach the shadow FSM: the marker parser returned `Ok` for these
    // bytes because it OVERWROTE its cursor with the declared `body_end` after decoding
    // the deletion times from beyond it, so a body-inconsistent frame still produced a
    // marker, which the FSM then refused for its bound kind. That `Ok` was the High
    // finding of job 75 — on the compaction path it wrote a tombstone whose timestamps
    // were read from the NEXT unfiltered. The parser now requires the body to be
    // consumed EXACTLY, so these bytes are refused AS an ill-framed marker, the walk
    // falls through to the row path, and the row's own declared extent refuses them.
    //
    // The PROPERTY under test is unchanged and is what this asserts: the read FAILS
    // rather than returning `Ok` with rows silently dropped. What is no longer asserted
    // here is the marker refusal's WORDING — that is not a coverage loss, because
    // `issue_3721_range_marker_refusal_surface.rs` asserts the identical needle set
    // ("range-tombstone marker" / "could not be represented faithfully" / "partition
    // body continues at offset" / "bound kind") across FOUR public surfaces (full scan,
    // point read, cell-metadata scan, streaming) against a marker that IS well framed,
    // which is the honest fixture for that diagnostic. Asserting it from a truncation
    // that no longer frames one would be pinning a message to the wrong input.
    let rendered = err.to_string();
    assert_eq!(err.category(), ErrorCategory::Data);
    assert!(!err.is_recoverable());
    // SECOND MOVE OF THIS CASE'S REFUSAL LAYER — recorded because the migration is the
    // informative part, and because a name or a needle left behind by it would describe
    // the wrong subject.
    //
    // Move 1: the marker parser once returned `Ok` for a body-inconsistent frame (it
    // overwrote its cursor with the declared `body_end`), so this reached the shadow FSM
    // and was refused for its bound kind. Requiring the body to be consumed EXACTLY made
    // these bytes an ill-framed MARKER, and the walk then fell through to the row path,
    // which refused on the row's own extent — so this asserted `row body exhausted`.
    //
    // Move 2 (this one): the READ path no longer converts a marker PARSE failure into a
    // partition terminator. `MarkerOutcome::Stop` is GONE, so the refusal is raised at
    // the layer that actually measured it — the marker — instead of surfacing as a
    // downstream row error about a row that does not exist. That is strictly more
    // truthful, and it is why the needles below are the marker's and not the row's.
    //
    // What is asserted is the CHAIN, because each link is a separate claim an operator
    // depends on: the finality fact (no refill can help), the marker-layer condition,
    // and the parser's OWN cause surviving to the surface rather than being discarded
    // and re-synthesised.
    // THIRD MOVE OF THIS CASE'S REFUSAL LAYER (#3782). Recorded rather than rewritten
    // silently, because the migration is the informative part and this is now the
    // clearest of the three.
    //
    // Move 1 refused at the ROW layer (`row body exhausted`); move 2 at the MARKER layer
    // (`MarkerOutcome::Stop` removed, so an unframeable marker surfaces as itself).
    // Move 3: #3782 added a row_size-vs-available-bytes guard that fires BEFORE row
    // assembly begins, so this truncated fixture is refused at the ROW EXTENT — the
    // earliest and most accurate layer available, since the row DECLARES more bytes than
    // the buffer holds and no column decode is attempted at all.
    //
    // The property is unchanged in all three: the read FAILS rather than returning `Ok`
    // with rows silently dropped. What moved is which layer measured it first, and each
    // move was to an EARLIER, more truthful one. The marker wording is still pinned by
    // `issue_3721_range_marker_refusal_surface.rs` across four public surfaces against a
    // marker that IS framed — the honest fixture for it.
    for needle in [
        // The row's own declared extent versus what the buffer actually holds.
        "row_size=",
        "exceeds available data",
        "remain after the row_size VInt",
        "truncated or corrupt row",
    ] {
        assert!(
            rendered.contains(needle),
            "the refusal must name `{needle}`; got: {rendered}"
        );
    }
    // A marker refusal is NOT the per-column variant: conflating the two is exactly what
    // the dedicated `Error::ColumnDecode` variant exists to prevent, and this case now
    // never reaches a column.
    assert!(
        !matches!(err, Error::ColumnDecode { .. }),
        "a marker refusal must not be reported as a per-COLUMN decode failure; got: {err:?}"
    );
    // The parser is shared by the read and compaction paths, so its diagnostics may name
    // NEITHER. This is a SELECT; a message ending "(compaction)" would send an operator
    // to the wrong subsystem. Pinned here, at the public surface, because the parser
    // cannot see which caller it has and so cannot assert this about itself.
    assert!(
        !rendered.contains("(compaction)"),
        "a SELECT's refusal must not attribute itself to compaction; got: {rendered}"
    );
}

/// COMPLEX arm. Same condition with a non-frozen (multicell) collection outstanding.
#[tokio::test]
async fn a_complex_column_left_without_bytes_fails_the_select() {
    let err = select_all_truncated(
        COMPLEX_KEYSPACE,
        COMPLEX_TABLE,
        COMPLEX_DIR,
        TRUNC_COMPLEX_FULL_LEN,
        Some(TRUNC_COMPLEX_KEEP),
        COMPLEX_TABLE_CORRECT,
    )
    .await
    .expect_err("the same condition with a COMPLEX column outstanding must also fail");
    // REFUSAL LAYER MOVED EARLIER (#3782), and the property is unchanged: the read FAILS
    // rather than returning `Ok` with the complex column and every later one dropped.
    //
    // This fixture is TRUNCATED, so #3782's row_size-vs-available-bytes guard fires
    // BEFORE row assembly starts — the row declares more bytes than the buffer holds, so
    // no column is ever dispatched and `assert_row_body_exhausted` / `assert_dispatch_type`
    // would now be asserting about a column decode that correctly never happens. Naming a
    // column here would attribute the failure to the wrong subject.
    //
    // The per-column assertions still hold where they belong — the decode-failure cases in
    // this file, whose rows are INTACT and whose columns genuinely fail — so this is a
    // relocation of the measurement, not a loss of it.
    let rendered = err.to_string();
    for needle in [
        "row_size=",
        "exceeds available data",
        "remain after the row_size VInt",
        "truncated or corrupt row",
    ] {
        assert!(
            rendered.contains(needle),
            "the row-extent refusal must name `{needle}`; got: {rendered}"
        );
    }
    assert_eq!(err.category(), ErrorCategory::Data);
    assert!(!err.is_recoverable());
    assert!(
        !matches!(err, Error::ColumnDecode { .. }),
        "a row-extent refusal must not be attributed to a column; got: {err:?}"
    );
}

// ─── CONTROLS: the truncation is what fails the read, not the scratch copy ───

/// The SAME scratch copy, UNTRUNCATED, reads cleanly — so the two cases above are
/// attributable to the missing bytes and not to the omitted checksum components or
/// to copying the fixture. Without this, a fix that failed every read of a
/// checksum-less SSTable would satisfy them.
#[tokio::test]
async fn the_resync_marker_subject_still_reads_cleanly_without_its_checksum_components() {
    let rows = select_all_truncated(
        COMPLEX_KEYSPACE,
        RESYNC_MARKER_TABLE,
        RESYNC_MARKER_DIR,
        RESYNC_MARKER_FULL_LEN,
        None,
        RESYNC_MARKER_SCHEMA,
    )
    .await
    .expect("the untruncated scratch copy must read cleanly");
    assert_eq!(
        rows, 1,
        "present fixture must return its row — 0 rows is a read regression, never a pass"
    );
}

#[tokio::test]
async fn the_complex_subject_still_reads_cleanly_without_its_checksum_components() {
    let rows = select_all_truncated(
        COMPLEX_KEYSPACE,
        COMPLEX_TABLE,
        COMPLEX_DIR,
        TRUNC_COMPLEX_FULL_LEN,
        None,
        COMPLEX_TABLE_CORRECT,
    )
    .await
    .expect("the untruncated scratch copy must read cleanly");
    assert_eq!(rows, 1, "present fixture must return its row");
}
