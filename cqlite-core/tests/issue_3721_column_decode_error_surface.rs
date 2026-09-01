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

/// Run one query over the wide-partition fixture with `schema_body` declared.
async fn wide_query(schema_body: &str, query: &str) -> Result<Vec<i32>, Error> {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = fixture_root(WIDE_KEYSPACE, WIDE_TABLE);
    let schema = write_schema(dir.path(), schema_body);
    let db = open_db(&root, &schema, WIDE_KEYSPACE).await;
    let result = db.execute(query).await?;
    Ok(result
        .rows
        .iter()
        .filter_map(|r| match r.values.get("ck") {
            Some(cqlite_core::types::Value::Integer(i)) => Some(*i),
            _ => None,
        })
        .collect())
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
    let mut cks = wide_query(&wide_schema(WIDE_TABLE_CORRECT), SLICE_QUERY)
        .await
        .expect("correct-schema clustering slice must succeed");
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
