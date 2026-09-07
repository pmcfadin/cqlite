//! Issue #4114 — `vector<float, n>` must decode from CASSANDRA-WRITTEN bytes.
//!
//! # The defect this pins
//! CQLite had no `vector` decoder at all. The value of a `vector<float, n>`
//! column is `4 * n` contiguous big-endian binary32 elements with NO framing of
//! any kind, and CQLite's cell reader treated the column as VARIABLE-width, so it
//! consumed the value's own FIRST BYTE as a vint length. That has three regimes,
//! and only the third is visible to a user as an error:
//!
//! * leading byte too large for the bytes remaining → bounds check → `Err`;
//! * leading byte satisfiable but not equal to `width - 1` → the row-body
//!   under/over-consumption guard rejects the row → `Err`;
//! * leading byte EXACTLY `width - 1` → the misread consumes `1 + len == width`
//!   bytes, the row body balances, and a WRONG VALUE is emitted with NO error and
//!   exit 0. `vector_exact` below is that case, and it is the whole point of the
//!   issue.
//!
//! # Oracle — Cassandra-written bytes, never CQLite's own output (#3042)
//! Fixture: `test-data/fixtures/issue_4114/test_vector/<table>-<uuid>/`, written
//! by Cassandra 5.0 (`nb-1-big-*`, generator
//! `test-data/scripts/generate-issue-4114-vector-float.sh`, schema
//! `test-data/schemas/issue-4114-vector-float.cql`). Every expectation below is
//! read from the `sstabledump` golden `nb-1-big-Data.db.jsonl` that sits beside
//! each `Data.db` — parsed AT RUN TIME by [`golden_rows`] rather than transcribed,
//! so no hand-typed byte or float can drift from what Cassandra actually wrote.
//! The headline `vector_exact` values are ALSO transcribed literally, so a golden
//! regeneration cannot silently soften the regression case.
//!
//! A hand-written byte literal cannot settle this at all: it would prove the
//! decoder agrees with its author's model of the framing, and that model being
//! wrong is exactly the defect. Likewise a CQLite-written round-trip, which is
//! invariant to a uniform framing error on both sides (CLAUDE.md, #3042).
//!
//! # FORMAT AUTHORITY (pinned `cassandra-5.0.8` — a CQLite `file:line` is never
//! format authority, #3041)
//! `git show cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/VectorType.java`
//! * `:94-101` — the width AND the serializer come from the ELEMENT type:
//!   `valueLengthIfFixed = elementType.valueLengthIfFixed() * dimension` and
//!   `serializer = elementType.isValueLengthFixed() ? FixedLengthSerializer : …`.
//! * `:445-460` — `FixedLengthSerializer.split` slices `dimension` successive
//!   `elementLength` windows, reads NO prefix, then `checkConsumedFully`.
//! * `FloatType.java:148-152` — `valueLengthIfFixed() == 4`; and
//!   `ByteBufferUtil.java:512-515` reads it with `ByteBuffer.getFloat`, whose
//!   default order is BIG_ENDIAN. So each element is a big-endian binary32.
//! * `AbstractType.java:535-552` / `:603-610` — cell framing branches PURELY on
//!   `valueLengthIfFixed()`: `>= 0` writes/skips the value RAW, `-1` writes/skips
//!   it with a vint length. A `vector<float, n>` therefore carries no length
//!   prefix, while the `z_after` `text` in the very same row does.
//! * `:409-414` — "we don't allow empty vectors, so we can just check for null":
//!   NULL is legal and is the ONLY absent shape; a zero-length value throws
//!   (`:365-368`, `:515-517`).
//!
//! # Fixture-root resolution (AC2/#3220) — CHECKOUT-relative, and it CANNOT skip
//! Deliberately NOT `sstables_root_for_table`. That resolver searches
//! `$CQLITE_DATASETS_ROOT` first, and this fixture is COMMITTED SOURCE that no
//! fetched corpus carries — so on every gate box (which exports a machine-local
//! root such as `/data/datasets`) the resolver would look where the fixture is not.
//! A committed fixture's location is knowable without consulting the environment,
//! so it is anchored on `CARGO_MANIFEST_DIR`, exactly as the committed CQL schemas
//! are (#3148). Precedent: `issue_3790_collection_order_cassandra_golden.rs`
//! (which explains the same shadowing hazard) and
//! `issue_3747_empty_map_key.rs` / `issue_3722_udt_field_type_fidelity.rs`.
//!
//! Committed ⇒ `must_run` UNCONDITIONALLY: there is no `return`, no
//! `eprintln!("skipping")` and no environment branch anywhere in this file. Every
//! resolution failure PANICS naming the table, and each case resolves ITS OWN
//! table (a per-`#[test]` case, never one loop with a suite-wide `ran > 0`, which
//! cannot see one table skipping behind its siblings — #3220).

#![cfg(feature = "lz4")]
//  ^ SOURCE-level gate, deliberately not a `[[test]] required-features` entry:
//  this file is in the package `exclude` list (it reads a fixture outside the
//  package), and a manifest target would survive into a published manifest
//  pointing at a source file that does not. `lz4` arrives via the DEFAULT
//  `all-compression` set, so every lane that runs this package has it.
//
//  The fixture is real Cassandra output and LZ4-compressed (`CompressionInfo.db`
//  present, `LZ4Compressor` in the metadata), so reading it needs `lz4`; without
//  the feature the target would compile and then fail at run time on a blob it
//  cannot decompress — a false red, not a signal.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::types::Value;
use cqlite_core::Config;

const KEYSPACE: &str = "test_vector";

// ════════════════════════════════════════════════════════════════════════════
// Fixture resolution — committed, therefore must_run: fail closed (#3220)
// ════════════════════════════════════════════════════════════════════════════

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-core always has a workspace parent directory")
        .to_path_buf()
}

/// The `<table>-<uuid>` directory of a committed fixture table.
///
/// GLOBBED, because regenerating the fixture mints a fresh table UUID and a
/// hardcoded path would rot. AMBIGUITY IS A HARD FAILURE: a stale generation left
/// committed beside a new one would make the winner depend on `read_dir` order,
/// i.e. this test would validate against whichever oracle the filesystem happened
/// to yield. So exactly one candidate carrying BOTH halves of the oracle (the
/// Cassandra-written `*-Data.db` and its `*-Data.db.jsonl` golden) is required.
fn table_dir(table: &str) -> PathBuf {
    let ks = repo_root()
        .join("test-data/fixtures/issue_4114")
        .join(KEYSPACE);
    let mut candidates: Vec<PathBuf> = std::fs::read_dir(&ks)
        .unwrap_or_else(|e| {
            panic!(
                "committed fixture keyspace dir unreadable ({ks:?}): {e} — \
                 {KEYSPACE}.{table} is git-tracked source and must resolve in \
                 every checkout, unconditionally (#3220: must_run). This path is \
                 checkout-relative by design and is NOT affected by \
                 CQLITE_DATASETS_ROOT."
            )
        })
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(&format!("{table}-")))
        })
        .filter(|p| {
            let has = |suffix: &str| {
                std::fs::read_dir(p).is_ok_and(|rd| {
                    rd.flatten()
                        .any(|e| e.file_name().to_str().is_some_and(|n| n.ends_with(suffix)))
                })
            };
            has("-Data.db") && has("-Data.db.jsonl")
        })
        .collect();
    candidates.sort();
    match candidates.len() {
        1 => candidates.remove(0),
        0 => panic!(
            "no {table}-<uuid> directory under {ks:?} carries BOTH a *-Data.db and \
             its *-Data.db.jsonl golden. The .db binaries are force-added \
             (gitignored `*.db`); a committed fixture must never be absent."
        ),
        n => panic!(
            "{n} candidate {table}-<uuid> directories under {ks:?} ({candidates:?}) \
             — a stale generation is still committed. Exactly one oracle, or the \
             winner depends on read_dir order."
        ),
    }
}

fn single_data_db(dir: &Path) -> PathBuf {
    let mut found: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("fixture dir unreadable ({dir:?}): {e}"))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
        .collect();
    assert_eq!(found.len(), 1, "expected exactly one Data.db in {dir:?}");
    found.pop().expect("length asserted as 1")
}

// ════════════════════════════════════════════════════════════════════════════
// The oracle: the sstabledump JSONL golden, parsed at run time
// ════════════════════════════════════════════════════════════════════════════

/// One golden cell. `sstabledump` renders a `vector<float, n>` as a JSON ARRAY of
/// n numbers (it asks the type for its elements), a `text` as a JSON string, and a
/// cell with no value as `{"deletion_info": …}` — the shape a NULL insert leaves.
#[derive(Debug, Clone, PartialEq)]
enum GoldenCell {
    /// `4 * n` bytes' worth of elements, in on-disk order.
    Floats(Vec<f32>),
    Text(String),
    /// A cell carrying `deletion_info` and no `value`: NULL. Cassandra has no
    /// EMPTY vector (`VectorType.java:409-414`), so this is the only absent shape.
    Deleted,
}

#[derive(Debug, Clone)]
struct GoldenRow {
    /// The partition key. Every fixture table has a single `int` key, which
    /// `sstabledump` renders as a decimal STRING (`"key":["1"]`).
    partition_key: i32,
    /// The `int` clustering value, when the table has one (`"clustering":[10]`).
    clustering: Option<i32>,
    cells: BTreeMap<String, GoldenCell>,
}

/// Every row of a table's `sstabledump` golden, in file order.
///
/// Fail-closed on every shape this does not recognise: a golden it cannot read is
/// not an oracle, and silently yielding fewer rows would make a decode regression
/// look like agreement.
fn golden_rows(table: &str) -> Vec<GoldenRow> {
    let dir = table_dir(table);
    let path = single_data_db(&dir).with_extension("db.jsonl");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("committed golden unreadable ({path:?}): {e}"));
    let mut rows = Vec::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let partition: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("golden line is not JSON ({path:?}): {e}"));
        let key_text = partition["partition"]["key"][0]
            .as_str()
            .unwrap_or_else(|| panic!("golden partition key is not a string: {partition}"));
        let partition_key: i32 = key_text
            .parse()
            .unwrap_or_else(|e| panic!("golden partition key {key_text:?} is not an int: {e}"));
        let golden_rows = partition["rows"]
            .as_array()
            .unwrap_or_else(|| panic!("golden partition has no rows array: {partition}"));
        for row in golden_rows {
            assert_eq!(
                row["type"].as_str(),
                Some("row"),
                "the fixture holds only live rows; got {row}"
            );
            let clustering = match &row["clustering"] {
                serde_json::Value::Null => None,
                serde_json::Value::Array(components) => {
                    assert_eq!(
                        components.len(),
                        1,
                        "fixture clustering keys are single-component: {row}"
                    );
                    Some(
                        i32::try_from(components[0].as_i64().unwrap_or_else(|| {
                            panic!("golden clustering component is not an int: {row}")
                        }))
                        .expect("fixture clustering values are small ints"),
                    )
                }
                other => panic!("unexpected golden clustering shape: {other}"),
            };
            let mut cells = BTreeMap::new();
            for cell in row["cells"]
                .as_array()
                .unwrap_or_else(|| panic!("golden row has no cells array: {row}"))
            {
                let name = cell["name"]
                    .as_str()
                    .unwrap_or_else(|| panic!("golden cell has no name: {cell}"))
                    .to_string();
                let value = match &cell["value"] {
                    serde_json::Value::Array(elements) => GoldenCell::Floats(
                        elements
                            .iter()
                            .map(|e| {
                                // The golden prints each element with Java's
                                // Float.toString, i.e. enough digits to identify
                                // the binary32 uniquely, so f64 -> f32 recovers
                                // the exact bytes Cassandra wrote.
                                e.as_f64().unwrap_or_else(|| {
                                    panic!("golden vector element is not a number: {cell}")
                                }) as f32
                            })
                            .collect(),
                    ),
                    serde_json::Value::String(s) => GoldenCell::Text(s.clone()),
                    serde_json::Value::Null if !cell["deletion_info"].is_null() => {
                        GoldenCell::Deleted
                    }
                    other => panic!("unexpected golden cell value shape: {other} in {cell}"),
                };
                assert!(
                    cells.insert(name.clone(), value).is_none(),
                    "golden row repeats cell {name}: {row}"
                );
            }
            rows.push(GoldenRow {
                partition_key,
                clustering,
                cells,
            });
        }
    }
    assert!(
        !rows.is_empty(),
        "the committed golden for {table} yielded ZERO rows ({path:?}) — \
         0-rows-when-present is a failure, never a skip"
    );
    rows
}

// ════════════════════════════════════════════════════════════════════════════
// The subject: the read path the CLI uses (`read-sstable` → get_all_entries)
// ════════════════════════════════════════════════════════════════════════════

/// Every decoded row of a fixture table, as `(partition-key bytes, name -> Value)`
/// in emission order.
///
/// This is `SSTableReader::get_all_entries`, the exact call
/// `cqlite-cli/src/commands/read_sstable.rs` makes, so the wiring evidence for
/// #4114 and this test exercise ONE path. Column types come from the SSTable's own
/// serialization header (its marshal type strings), which is what makes this a
/// test of the `VectorType(FloatType , n)` type parsers as well as of the value
/// decoder. `SELECT`'s CQL-schema-driven path is covered separately below.
async fn decoded_rows(table: &str) -> Vec<(Vec<u8>, BTreeMap<String, Value>)> {
    let data_path = single_data_db(&table_dir(table));
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("platform init for a committed fixture read"),
    );
    let reader = SSTableReader::open(&data_path, &config, platform)
        .await
        .unwrap_or_else(|e| panic!("open committed fixture {data_path:?} failed: {e}"));
    let entries = reader
        .get_all_entries()
        .await
        .unwrap_or_else(|e| panic!("scan of committed fixture {data_path:?} failed: {e}"));
    assert!(
        !entries.is_empty(),
        "committed fixture {table} decoded ZERO rows — 0-rows-when-present is a \
         failure (#3220 must_run), never a skip"
    );
    entries
        .into_iter()
        .filter_map(|(_, key, row)| {
            row.into_cells().map(|cells| {
                (
                    key.0.to_vec(),
                    cells
                        .into_iter()
                        .map(|(name, value)| (name.to_string(), value))
                        .collect::<BTreeMap<_, _>>(),
                )
            })
        })
        .collect()
}

fn vector(elements: &[f32]) -> Value {
    Value::List(elements.iter().copied().map(Value::Float32).collect())
}

/// The decoded cells of the row identified by `(partition_key, clustering)`.
///
/// A clustered table emits one entry per row under the SAME partition key, so the
/// clustering value is part of the identity. It arrives on the schema-less path as
/// a `clustering_key` cell (no CQL schema is consulted, so the column's declared
/// name is not available); its VALUE is the assertion subject either way.
fn row_of<'a>(
    rows: &'a [(Vec<u8>, BTreeMap<String, Value>)],
    golden: &GoldenRow,
) -> &'a BTreeMap<String, Value> {
    let key = golden.partition_key.to_be_bytes().to_vec();
    let mut matching = rows.iter().filter(|(k, cells)| {
        *k == key
            && match golden.clustering {
                None => true,
                Some(ck) => cells
                    .values()
                    .any(|v| matches!(v, Value::Integer(i) if *i == ck)),
            }
    });
    let found = matching.next().unwrap_or_else(|| {
        panic!(
            "no decoded row for partition {} clustering {:?}; the golden has one, so \
             the row was LOST (a mis-framed cell aborts the row loop, #3890). \
             Decoded rows: {rows:#?}",
            golden.partition_key, golden.clustering
        )
    });
    assert!(
        matching.next().is_none(),
        "more than one decoded row matches partition {} clustering {:?}",
        golden.partition_key,
        golden.clustering
    );
    &found.1
}

/// Assert EVERY golden cell of EVERY golden row, not just the vector column.
///
/// The whole row is the subject because a mis-framed cell desyncs the offset for
/// every LATER cell in the row and the row loop then ABORTS, dropping those cells
/// silently (CLAUDE.md's fifth blind spot, #3890). A test that inspected only the
/// vector column could not see the `z_after` sentinel disappear — which is exactly
/// what the pre-fix reader did to it.
fn assert_full_rows_match_golden(table: &str, rows: &[(Vec<u8>, BTreeMap<String, Value>)]) {
    let golden = golden_rows(table);
    for g in &golden {
        let decoded = row_of(rows, g);
        for (name, expected) in &g.cells {
            let actual = decoded.get(name).unwrap_or_else(|| {
                panic!(
                    "column {name} is ABSENT from the decoded row for partition {} \
                     clustering {:?} — the golden has it. Decoded: {decoded:#?}",
                    g.partition_key, g.clustering
                )
            });
            match expected {
                GoldenCell::Floats(elements) => assert_eq!(
                    actual,
                    &vector(elements),
                    "column {name}, partition {} clustering {:?}: a \
                     vector<float, {}> is {} raw big-endian binary32 bytes with NO \
                     length prefix (VectorType.java:94-101, :445-460)",
                    g.partition_key,
                    g.clustering,
                    elements.len(),
                    4 * elements.len()
                ),
                GoldenCell::Text(s) => assert_eq!(
                    actual,
                    &Value::text(s),
                    "SENTINEL column {name}, partition {} clustering {:?}: a text \
                     cell adjacent to a vector must still decode — if it does not, \
                     the vector's framing desynced the row (#3890)",
                    g.partition_key,
                    g.clustering
                ),
                GoldenCell::Deleted => assert!(
                    matches!(actual, Value::Null | Value::Tombstone(_)),
                    "column {name}, partition {} clustering {:?}: the golden cell \
                     carries deletion_info and NO value, so nothing may be decoded \
                     for it. Cassandra has no empty vector \
                     (VectorType.java:409-414), so a NULL vector must never \
                     surface as a value. Got {actual:?}",
                    g.partition_key,
                    g.clustering
                ),
            }
        }
    }
    assert_eq!(
        rows.len(),
        golden.len(),
        "decoded row count must equal the golden's for {table}"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// THE REGRESSION CASE — vector_exact, the fully SILENT mis-decode
// ════════════════════════════════════════════════════════════════════════════

/// **THE POINT OF ISSUE #4114.** `vector_exact.v3` is built so the pre-fix
/// misread balances the row body EXACTLY and therefore raises NO error: the first
/// element is `2^-105`, whose big-endian encoding is `0b 00 00 00`, so the phantom
/// vint length `0x0b == 11` plus its own byte consumes precisely the 12 bytes the
/// value occupies. Nothing downstream can notice.
///
/// MEASURED PRE-FIX (`cqlite read-sstable … vector_exact/nb-1-big-Data.db
/// --format json`, `.drive-issue-4114/silent-misdecode-measurement.md`):
/// **exit 0**, two rows emitted, `v3` = an 11-byte BLOB
/// `0x0000003f80000040000000` — the TAIL of the vector's own bytes — instead of
/// `[2.4651903e-32, 1.0, 2.0]`.
///
/// The expected values here are ALSO written out literally, unlike every other
/// case in this file, so that regenerating the golden cannot soften the one case
/// the issue exists for. They are the golden's own numbers, and the assertion
/// below cross-checks them against the golden that ships beside the `Data.db`.
///
/// Golden (`vector_exact/nb-1-big-Data.db.jsonl`):
/// ```text
/// {"key":["1"], … "cells":[{"name":"v3","value":[2.4651903e-32,1.0,2.0]}]}
/// {"key":["2"], … "cells":[{"name":"v3","value":[2.4651903e-32,4.5,-5.0]}]}
/// ```
/// `2.4651903e-32` is exactly `2^-105` (`0x0b000000`) — chosen for its LEADING
/// BYTE, not its magnitude. The two rows have DIFFERENT tails so a constant wrong
/// answer cannot pass as a data-derived one.
#[tokio::test]
async fn ac3_vector_exact_silent_misdecode_is_fixed() {
    const TABLE: &str = "vector_exact";
    let rows = decoded_rows(TABLE).await;

    // Literal expectations first: the golden's numbers, typed out.
    let expected: [(i32, [f32; 3]); 2] = [
        (1, [2.4651903e-32, 1.0, 2.0]),
        (2, [2.4651903e-32, 4.5, -5.0]),
    ];
    for (pk, elements) in expected {
        let decoded = row_of(
            &rows,
            &GoldenRow {
                partition_key: pk,
                clustering: None,
                cells: BTreeMap::new(),
            },
        );
        let v3 = decoded
            .get("v3")
            .unwrap_or_else(|| panic!("partition {pk} decoded without a v3 column: {decoded:#?}"));
        assert!(
            !matches!(v3, Value::Blob(_)),
            "partition {pk} v3 came back as a BLOB — that is the #4114 silent \
             mis-decode itself: the pre-fix reader consumed the value's own first \
             byte (0x0b) as a vint length and handed back the 11-byte TAIL of the \
             value, at exit 0. Got {v3:?}"
        );
        assert_eq!(
            v3,
            &vector(&elements),
            "partition {pk} v3 must be the three big-endian binary32 elements \
             Cassandra wrote"
        );
    }

    // …and the same rows re-derived from the golden, so the literals above cannot
    // drift from the fixture and the sentinel/absence rules apply uniformly.
    assert_full_rows_match_golden(TABLE, &rows);
}

// ════════════════════════════════════════════════════════════════════════════
// AC3 — the required coverage matrix, one `#[test]` per table (per-case must_run)
// ════════════════════════════════════════════════════════════════════════════

/// AC3: `n == 1` (the degenerate width, and the shape most easily confused with a
/// bare `float`), `n == 384` (a typical embedding width, 1536 bytes) and a NULL
/// vector, all in a PARTITION-KEY-ONLY table, with the `a_before`/`z_after` text
/// sentinels that bracket the vector columns in on-disk (column-name) order.
///
/// Golden (`vector_pk_only/nb-1-big-Data.db.jsonl`), all three partitions:
/// ```text
/// key ["1"]: a_before "before-1", v1 [1.5],   v384 [0.0,0.5,…,191.5],      z_after "after-1"
/// key ["2"]: a_before "before-2", v1 DELETED, v384 DELETED,                z_after "after-2"
/// key ["3"]: a_before "before-3", v1 [-2.25], v384 [1000.0,1000.5,…,1191.5], z_after "after-3"
/// ```
#[tokio::test]
async fn ac3_pk_only_n1_n384_and_null_vectors() {
    const TABLE: &str = "vector_pk_only";
    let rows = decoded_rows(TABLE).await;
    assert_full_rows_match_golden(TABLE, &rows);

    let golden = golden_rows(TABLE);

    // n == 1: exactly 4 bytes, one element. Named explicitly so the AC3 case is
    // visible in this file and not merely implied by the golden's contents.
    let n1 = golden
        .iter()
        .filter_map(|g| match g.cells.get("v1") {
            Some(GoldenCell::Floats(e)) => Some(e.len()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        n1,
        vec![1, 1],
        "the golden must still carry two live vector<float, 1> cells"
    );

    // n == 384: the width a real embedding column has.
    let n384 = golden
        .iter()
        .filter_map(|g| match g.cells.get("v384") {
            Some(GoldenCell::Floats(e)) => Some(e.len()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        n384,
        vec![384, 384],
        "the golden must still carry two live vector<float, 384> cells"
    );

    // The NULL case: partition 2's vectors are absent, and its text sentinels are
    // NOT — the row survives a null vector on both sides of it.
    let null_row = golden
        .iter()
        .find(|g| g.partition_key == 2)
        .unwrap_or_else(|| panic!("the golden must still carry the NULL-vector partition"));
    assert_eq!(null_row.cells.get("v1"), Some(&GoldenCell::Deleted));
    assert_eq!(null_row.cells.get("v384"), Some(&GoldenCell::Deleted));
    let decoded = row_of(&rows, null_row);
    assert_eq!(
        decoded.get("z_after"),
        Some(&Value::text("after-2")),
        "a NULL vector must not disturb the columns after it"
    );
}

/// AC3: the same type in a table WITH A CLUSTERING COLUMN — a different row-header
/// and clustering-prefix path, and the shape `parser::repair_clustering` governs.
///
/// Golden (`vector_clustered/nb-1-big-Data.db.jsonl`), one partition, two rows:
/// ```text
/// key ["1"] clustering [10]: v3 [1.0,2.5,-3.75], z_after "ck-after-10"
/// key ["1"] clustering [20]: v3 [4.5,-5.0,6.25], z_after "ck-after-20"
/// ```
/// Byte-verified: in the `ck=10` row the 12-byte payload
/// `3f800000 40200000 c0700000` is preceded by the cell FLAGS byte `0x08` and NOT
/// by a vint length `0x0c`, while the `text` cell that follows it in the SAME row
/// is framed `08 0b` = flags + vint length 11 (`.drive-issue-4114/format-authority.md`).
/// Fixed and variable framing side by side in one Cassandra-written row.
#[tokio::test]
async fn ac3_clustered_table_vector_with_clustering_column() {
    const TABLE: &str = "vector_clustered";
    let rows = decoded_rows(TABLE).await;
    assert_full_rows_match_golden(TABLE, &rows);

    let golden = golden_rows(TABLE);
    let clusterings: Vec<Option<i32>> = golden.iter().map(|g| g.clustering).collect();
    assert_eq!(
        clusterings,
        vec![Some(10), Some(20)],
        "the AC3 clustered case needs the golden's two clustered rows; without a \
         clustering column this table stops testing what it exists for"
    );
    // Both rows sit in ONE partition, so the two decoded rows must be told apart
    // by their clustering value — which `row_of` does, and which also proves the
    // clustering component itself still decodes beside a vector column.
    assert_eq!(rows.len(), 2, "one partition, two clustered rows");
}

/// AC3: the vector as the LAST regular column — nothing follows it to desync
/// into, so a mis-framed value leaves unconsumed bytes at the END of the row body
/// instead of corrupting a later cell. Pre-fix this table failed CLOSED (exit 5,
/// ZERO rows: the row-body accounting guard rejected the row and the scan aborted
/// on the first bad partition), which is why `vector_exact` — where the accounting
/// happens to balance — is the silent case and this one is not.
///
/// Golden (`vector_last/nb-1-big-Data.db.jsonl`):
/// ```text
/// key ["1"]: v3 [0.0,1.0,2.0]         (first element 0x00000000)
/// key ["2"]: v3 [3.85186e-34,1.0,2.0] (first element 0x08000000 == 2^-111)
/// ```
#[tokio::test]
async fn ac3_vector_as_last_regular_column() {
    const TABLE: &str = "vector_last";
    let rows = decoded_rows(TABLE).await;
    assert_full_rows_match_golden(TABLE, &rows);
    assert_eq!(
        rows.len(),
        2,
        "pre-fix this table emitted ZERO rows — the scan aborted on the first \
         partition whose row body did not balance"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// The OTHER read surface: `SELECT` driven by the committed CQL schema
// ════════════════════════════════════════════════════════════════════════════
//
// The cases above resolve column types from the SSTable's own serialization
// header (marshal spelling `VectorType(FloatType , n)`). A `SELECT` through
// `ingest` resolves them from the committed CQL text (`vector<float, 384>`) — a
// DIFFERENT type parser feeding the same value decoder. Both must agree with the
// golden, so both are asserted.
//
// `cqlite_core::ingestion` is gated behind `cli-helpers` (the gate's `core-tests`
// component runs `--features cli-helpers`, so this lane executes there). The
// cases above are deliberately NOT gated, so this target can never run zero tests.

#[cfg(feature = "cli-helpers")]
#[tokio::test]
async fn ac3_select_surface_decodes_vectors_from_the_cql_schema() {
    use cqlite_core::ingestion::{ingest, IngestionConfig};

    const TABLE: &str = "vector_pk_only";
    // Resolve the fixture first, so a missing committed fixture is a NAMED
    // failure for this case too rather than an empty result set.
    let _ = table_dir(TABLE);

    let config = IngestionConfig {
        schema_paths: vec![repo_root().join("test-data/schemas/issue-4114-vector-float.cql")],
        data_dir: repo_root().join("test-data/fixtures/issue_4114"),
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: Some(format!("{TABLE}-")),
    };
    let db = ingest(config)
        .await
        .expect("the committed fixture and schema must ingest (both are git-tracked source)")
        .database;
    let result = db
        .execute(&format!("SELECT * FROM {KEYSPACE}.{TABLE}"))
        .await
        .unwrap_or_else(|e| panic!("SELECT over the committed fixture failed: {e}"));
    assert!(
        !result.rows.is_empty(),
        "SELECT decoded ZERO rows from the committed fixture"
    );

    let golden = golden_rows(TABLE);
    for g in &golden {
        let row = result
            .rows
            .iter()
            .find(|r| r.values.get("id") == Some(&Value::Integer(g.partition_key)))
            .unwrap_or_else(|| {
                panic!(
                    "SELECT returned no row for id={} though the golden has one",
                    g.partition_key
                )
            });
        for (name, expected) in &g.cells {
            match expected {
                GoldenCell::Floats(elements) => assert_eq!(
                    row.values.get(name.as_str()),
                    Some(&vector(elements)),
                    "SELECT id={} column {name}: the CQL-schema-driven path must \
                     decode the same {} bytes the header-driven path does",
                    g.partition_key,
                    4 * elements.len()
                ),
                GoldenCell::Text(s) => assert_eq!(
                    row.values.get(name.as_str()),
                    Some(&Value::text(s)),
                    "SELECT id={} sentinel column {name} must still decode beside a \
                     vector",
                    g.partition_key
                ),
                GoldenCell::Deleted => assert!(
                    matches!(
                        row.values.get(name.as_str()),
                        None | Some(Value::Null) | Some(Value::Tombstone(_))
                    ),
                    "SELECT id={} column {name} is NULL in the golden; a value here \
                     would be invented (Cassandra has no empty vector)",
                    g.partition_key
                ),
            }
        }
    }
}
