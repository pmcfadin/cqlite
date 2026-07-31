//! Issue #3032: COMPOUND-clustering BTI slices through the public `SELECT` path,
//! validated against the committed `sstabledump` JSONL golden.
//!
//! ## What this lane covers that no existing one can
//!
//! `test_da/wide_table` — until #3032 the corpus's only wide BTI fixture — has a
//! SINGLE `int` clustering column. Every slice over it therefore bounds the WHOLE
//! clustering key, so it structurally cannot exercise:
//!
//! * a bound naming only a PROPER PREFIX of the clustering key
//!   (`WHERE bucket = 'bo'` on `PRIMARY KEY (pk, bucket, seq)`);
//! * a bound on a NON-first clustering component (`… AND seq >= 10 AND seq < 20`);
//! * the OSS50 VARIABLE-LENGTH (`text`) component encoding at all — every
//!   `wide_table` separator is a fixed-width `40 80 00 00 <byte>`.
//!
//! All three run here, against a real Apache Cassandra 5.0.2 `da` SSTable.
//!
//! ## Why it caught two defects
//!
//! Both were invisible to a CQLite-written/CQLite-read round trip (the #3042 blind
//! spot: the writer and the reader shared the same mistake) and to the physical
//! JSONL goldens (which enumerate on-disk cells, not `SELECT` output):
//!
//! 1. `encode_varlen_oss50` terminated a `text`/`blob` component with `00 FF`.
//!    Cassandra's `ByteSource.AbstractEscaper` (cassandra-5.0.8
//!    `utils/bytecomparable/ByteSource.java:309-380`) terminates with a SINGLE
//!    `ESCAPE` (`0x00`) unless the data itself ends in a zero. The real on-disk
//!    separator for `('bo', 12)` in this fixture's `Rows.db` is
//!    `40 62 6f 00 40 80 00 00 0c` — terminator `00`. A `00 FF` terminator sorts
//!    ABOVE the `0x40 NEXT_COMPONENT` that follows it, i.e. above every key it was
//!    meant to bound.
//! 2. `physical_byte_bounds_for_slice` emitted bare prefixes with no bound
//!    terminator. `ClusteringPrefix.Kind` (`db/ClusteringPrefix.java:68-81`) emits
//!    `LT_NEXT_COMPONENT` (`0x20`) for `INCL_START`/`EXCL_END` and
//!    `GT_NEXT_COMPONENT` (`0x60`) for `INCL_END`/`EXCL_START`, so a prefix bound
//!    sorts below/above its extensions on purpose. A bare prefix always sorts
//!    BELOW, so an upper prefix bound truncated the slice to its first row-index
//!    block.
//!
//! ## Oracle
//!
//! Every expected row set is RE-DERIVED at runtime from the committed
//! `da-2-bti-Data.db.jsonl` golden (Cassandra's own `sstabledump` output), never
//! hand-authored and never read back from CQLite. A regenerated fixture therefore
//! moves the expectation with it instead of silently drifting.
//!
//! Each slice runs under BOTH forced read paths (`ReadPathMode::Point`, which
//! resolves a `Rows.db` byte window, and `ReadPathMode::Full`, which decodes the
//! whole partition and filters) and both must equal the golden — a point-only
//! assertion would have passed on the defective build for the multi-component
//! slices, which take the un-narrowed path.
//!
//! Fail-closed (AC8): an absent fixture/schema SKIPs loudly and FAILS under
//! `CQLITE_REQUIRE_FIXTURES=1`; a present-but-empty component is ALWAYS a hard
//! failure; and a partition that yields zero golden rows is a failure, never a pass.
#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    not(feature = "tombstones")
))]

use cqlite_core::config::ReadPathMode;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::types::Value;
use cqlite_core::Database;
use std::path::{Path, PathBuf};

const MC_DIR: &str = "sstables/test_da/multiclustering_table-fd74ad508d2311f1a29b6d2c15dcffdf";
const GEN: &str = "da-2-bti";
const TABLE: &str = "test_da.multiclustering_table";
const SCHEMA_FILE: &str = "multiclustering-table-bti.cql";

/// Total physical rows in the committed golden — re-derived below and asserted, so
/// a truncated or regenerated fixture cannot pass silently.
const GOLDEN_ROWS: usize = 468;

/// Fail-closed switch: an absent fixture is a hard FAILURE, not a clean skip.
fn require_fixtures() -> bool {
    std::env::var("CQLITE_REQUIRE_FIXTURES")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-core has a parent repo dir")
        .to_path_buf()
}

/// Datasets root holding the fixture: `CQLITE_DATASETS_ROOT` when it has it, else
/// the in-repo committed corpus.
fn datasets_root() -> Option<PathBuf> {
    let candidates = [
        std::env::var("CQLITE_DATASETS_ROOT")
            .ok()
            .map(PathBuf::from),
        Some(repo_root().join("test-data").join("datasets")),
    ];
    let found = candidates
        .into_iter()
        .flatten()
        .find(|root| root.join(MC_DIR).join(format!("{GEN}-Data.db")).exists());
    if found.is_none() {
        let msg = format!("{MC_DIR}/{GEN}-Data.db not found (committed fixture missing)");
        assert!(
            !require_fixtures(),
            "CQLITE_REQUIRE_FIXTURES=1 but {msg} — fail-closed"
        );
        eprintln!("SKIP: {msg}");
    }
    found
}

/// The committed schema for this table, preferring the datasets root's sibling
/// `schemas/` dir (mirrors the other dataset lanes) and falling back to the repo.
fn schema_path(root: &Path) -> Option<PathBuf> {
    let candidates = [
        root.parent().map(|p| p.join("schemas").join(SCHEMA_FILE)),
        Some(
            repo_root()
                .join("test-data")
                .join("schemas")
                .join(SCHEMA_FILE),
        ),
    ];
    let found = candidates.into_iter().flatten().find(|p| p.exists());
    if found.is_none() {
        assert!(
            !require_fixtures(),
            "CQLITE_REQUIRE_FIXTURES=1 but the committed schema {SCHEMA_FILE} is absent \
             — fail-closed"
        );
        eprintln!("SKIP: schema {SCHEMA_FILE} not found");
    }
    found
}

/// One golden row's primary key, as `(pk, bucket, seq)`.
type GoldenRow = (i32, String, i32);

/// Every `type == "row"` entry in the committed sstabledump golden, as
/// `(pk, bucket, seq)` in file order (which IS clustering order within a
/// partition). A present-but-EMPTY golden, or a partition with no rows, is a hard
/// failure — never a pass.
fn golden_rows(root: &Path) -> Vec<GoldenRow> {
    let path = root.join(MC_DIR).join(format!("{GEN}-Data.db.jsonl"));
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read the committed golden {}: {e}", path.display()));
    assert!(
        !text.trim().is_empty(),
        "golden {} is present but EMPTY — never pass on it",
        path.display()
    );
    let mut out = Vec::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let v: serde_json::Value = serde_json::from_str(line).expect("golden line must be JSON");
        let pk: i32 = v
            .pointer("/partition/key/0")
            .and_then(|k| k.as_str())
            .and_then(|s| s.parse().ok())
            .expect("golden partition key is a single int");
        let rows = v
            .get("rows")
            .and_then(|r| r.as_array())
            .expect("golden partition has rows");
        let before = out.len();
        for r in rows
            .iter()
            .filter(|r| r.get("type").and_then(|t| t.as_str()) == Some("row"))
        {
            let bucket = r
                .pointer("/clustering/0")
                .and_then(|c| c.as_str())
                .expect("clustering[0] is the text `bucket`")
                .to_string();
            let seq = r
                .pointer("/clustering/1")
                .and_then(|c| c.as_i64())
                .expect("clustering[1] is the int `seq`") as i32;
            out.push((pk, bucket, seq));
        }
        assert!(
            out.len() > before,
            "golden partition pk={pk} contributed ZERO rows — a fixture that decodes to \
             nothing is a FAILURE, never a pass"
        );
    }
    assert_eq!(
        out.len(),
        GOLDEN_ROWS,
        "the committed golden must hold {GOLDEN_ROWS} physical rows"
    );
    out
}

async fn open_db(root: &Path, schema: &Path, mode: ReadPathMode) -> Database {
    let mut core_config = cqlite_core::Config::default();
    core_config.query.forced_read_path = Some(mode);
    let cfg = IngestionConfig {
        schema_paths: vec![schema.to_path_buf()],
        data_dir: root.join("sstables"),
        version_hint: None,
        core_config,
        table_directory_filter: Some("/test_da/".to_string()),
    };
    let result = ingest(cfg).await.expect("ingestion must succeed");
    assert!(
        result.schema_load_result.schemas_loaded > 0,
        "the multiclustering_table schema must load"
    );
    result.database
}

/// Run `SELECT pk, bucket, seq … WHERE <predicate>` and return the decoded
/// `(pk, bucket, seq)` triples IN RESULT ORDER.
async fn select_triples(db: &Database, predicate: &str) -> Vec<GoldenRow> {
    let query = format!("SELECT pk, bucket, seq FROM {TABLE} WHERE {predicate}");
    let res = db
        .execute(&query)
        .await
        .unwrap_or_else(|e| panic!("`{query}` must succeed: {e}"));
    res.rows
        .iter()
        .map(|r| {
            let pk = match r.values.get("pk") {
                Some(Value::Integer(v)) => *v,
                other => panic!("`{query}`: pk decoded as {other:?}"),
            };
            let bucket = match r.values.get("bucket") {
                Some(Value::Text(b)) => String::from_utf8_lossy(b).into_owned(),
                other => panic!("`{query}`: bucket decoded as {other:?}"),
            };
            let seq = match r.values.get("seq") {
                Some(Value::Integer(v)) => *v,
                other => panic!("`{query}`: seq decoded as {other:?}"),
            };
            (pk, bucket, seq)
        })
        .collect()
}

/// The slice matrix. Each case is `(predicate, golden filter)`; the expected rows
/// are whatever the GOLDEN says match, so the expectation is Cassandra's, not
/// CQLite's.
#[allow(clippy::type_complexity)]
fn slice_cases() -> Vec<(String, Box<dyn Fn(&GoldenRow) -> bool>)> {
    let mut cases: Vec<(String, Box<dyn Fn(&GoldenRow) -> bool>)> = Vec::new();

    // (1) PROPER-PREFIX bounds: only the FIRST clustering component is restricted,
    // so the bound is shorter than a `Rows.db` separator. This is the shape that
    // drives the row-index prefix narrowing, and the shape both #3032 defects broke.
    for (pk, bucket) in [
        (1, "alpha"),
        (1, "bo"),
        (1, "charlie-extended-bucket"),
        (2, "delta"),
        (2, "ep"),
        (3, "foxtrot-long-bucket-name"),
        (3, "golf"),
        (3, "hh"),
    ] {
        let b = bucket.to_string();
        cases.push((
            format!("pk = {pk} AND bucket = '{bucket}'"),
            Box::new(move |(p, bk, _): &GoldenRow| *p == pk && bk == &b),
        ));
    }

    // A prefix RANGE (not just equality) on the first component.
    cases.push((
        "pk = 1 AND bucket >= 'bo' AND bucket < 'charlie-extended-bucket'".to_string(),
        Box::new(|(p, bk, _): &GoldenRow| {
            *p == 1 && bk.as_str() >= "bo" && bk.as_str() < "charlie-extended-bucket"
        }),
    ));
    cases.push((
        "pk = 2 AND bucket > 'bo' AND bucket <= 'delta'".to_string(),
        Box::new(|(p, bk, _): &GoldenRow| *p == 2 && bk.as_str() > "bo" && bk.as_str() <= "delta"),
    ));

    // (2) Bounds on MORE THAN the first clustering component (issue #3032 AC5).
    cases.push((
        "pk = 1 AND bucket = 'bo' AND seq >= 10 AND seq < 20".to_string(),
        Box::new(|(p, bk, s): &GoldenRow| *p == 1 && bk == "bo" && (10..20).contains(s)),
    ));
    cases.push((
        "pk = 2 AND bucket = 'delta' AND seq > 8 AND seq <= 23".to_string(),
        Box::new(|(p, bk, s): &GoldenRow| *p == 2 && bk == "delta" && *s > 8 && *s <= 23),
    ));
    cases.push((
        "pk = 3 AND bucket = 'hh' AND seq < 4".to_string(),
        Box::new(|(p, bk, s): &GoldenRow| *p == 3 && bk == "hh" && *s < 4),
    ));
    // A full two-component point read.
    cases.push((
        "pk = 3 AND bucket = 'golf' AND seq = 7".to_string(),
        Box::new(|(p, bk, s): &GoldenRow| *p == 3 && bk == "golf" && *s == 7),
    ));

    cases
}

/// Every compound-clustering slice returns EXACTLY the golden's matching rows, in
/// clustering order, on BOTH the point and the full read path.
#[tokio::test]
async fn compound_clustering_slices_match_the_sstabledump_golden() {
    let Some(root) = datasets_root() else {
        return;
    };
    let Some(schema) = schema_path(&root) else {
        return;
    };
    let golden = golden_rows(&root);

    let point = open_db(&root, &schema, ReadPathMode::Point).await;
    let full = open_db(&root, &schema, ReadPathMode::Full).await;

    // Anti-empty-pass: the fixture really decodes, whole-partition, on both paths.
    for pk in [1, 2, 3] {
        let expected: Vec<GoldenRow> = golden
            .iter()
            .filter(|(p, _, _)| *p == pk)
            .cloned()
            .collect();
        assert!(
            !expected.is_empty(),
            "the golden must hold rows for pk={pk}"
        );
        for (label, db) in [("point", &point), ("full", &full)] {
            let got = select_triples(db, &format!("pk = {pk}")).await;
            assert_eq!(
                got,
                expected,
                "{label}: the full pk={pk} partition must equal the golden's {} rows, in \
                 clustering order",
                expected.len()
            );
        }
    }

    for (predicate, matches) in slice_cases() {
        let expected: Vec<GoldenRow> = golden.iter().filter(|r| matches(r)).cloned().collect();
        // Every case must be a REAL narrowing: non-empty, and strictly fewer rows
        // than its whole partition — otherwise the comparison could pass vacuously.
        let pk = expected.first().map(|(p, _, _)| *p).unwrap_or_else(|| {
            panic!(
                "`{predicate}` matches ZERO golden rows — a slice case \
                                       must select something"
            )
        });
        let partition_rows = golden.iter().filter(|(p, _, _)| *p == pk).count();
        assert!(
            !expected.is_empty() && expected.len() < partition_rows,
            "`{predicate}` must select a non-empty PROPER subset of pk={pk}'s \
             {partition_rows} rows; got {}",
            expected.len()
        );

        for (label, db) in [("point", &point), ("full", &full)] {
            let got = select_triples(db, &predicate).await;
            assert_eq!(
                got,
                expected,
                "{label}: `{predicate}` must return exactly the golden's {} matching rows \
                 in clustering order — a truncated window shows up here as a PREFIX or \
                 SUFFIX of the expected set",
                expected.len()
            );
        }
        eprintln!(
            "PASS `{predicate}` — {} rows, point == full == golden",
            expected.len()
        );
    }
}
