//! CommitLog reader parity oracle (issue #2389).
//!
//! The oracle is the CommitLog analog of CQLite's query-semantics oracle: a
//! KNOWN set of mutations was inserted into a real Cassandra 5.0.2 node and the
//! resulting segment captured before discard (see
//! `test-data/scripts/generate-commitlog-fixtures.sh`). This test asserts that
//! `CommitLogReader` decodes exactly those mutations — table id, partition key,
//! and cell values — matching the committed ground truth. There is no
//! sstabledump-equivalent for commitlog segments, so "what we inserted vs. what
//! the reader produced" IS the oracle.
//!
//! Fixtures are small, committed reference binaries (`git add -f`), so this test
//! never silently passes on missing data.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use cqlite_core::storage::commitlog::{
    parse_table_id, ColumnSpec, CommitLogReader, CommitLogSchema, SchemaSet,
};

fn commitlog_dir() -> PathBuf {
    // Prefer the committed in-repo fixtures (present in any checkout/worktree).
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("test-data/datasets/commitlog");
    if repo.is_dir() {
        return repo;
    }
    // Fall back to an explicit datasets root if provided.
    if let Ok(root) = std::env::var("CQLITE_DATASETS_ROOT") {
        return PathBuf::from(root).join("commitlog");
    }
    repo
}

#[derive(serde::Deserialize)]
struct GroundTruth {
    table_id: String,
    inserts: Vec<InsertRow>,
}

#[derive(serde::Deserialize)]
struct InsertRow {
    id: i32,
    name: String,
    age: i32,
}

fn load_ground_truth(dir: &Path) -> GroundTruth {
    let path = dir.join("commitlog-ground-truth.json");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read ground truth {}: {e}", path.display()));
    serde_json::from_str(&text).expect("parse ground truth JSON")
}

fn users_schema() -> CommitLogSchema {
    CommitLogSchema {
        keyspace: "commitlog_test".into(),
        table: "users".into(),
        partition_key: vec![ColumnSpec::new("id", "int")],
        clustering: vec![],
        columns: vec![
            ColumnSpec::new("age", "int"),
            ColumnSpec::new("name", "text"),
        ],
    }
}

fn find_fixture(dir: &Path, prefix: &str) -> PathBuf {
    let entries = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read commitlog dir {}: {e}", dir.display()));
    for e in entries.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if name.starts_with(prefix) && name.ends_with(".log") {
            return e.path();
        }
    }
    panic!(
        "no fixture starting with {prefix:?} under {}",
        dir.display()
    );
}

/// Requirement: mutation stream decoding matches ground truth.
#[test]
fn decoded_mutations_match_inserted_set() {
    let dir = commitlog_dir();
    let gt = load_ground_truth(&dir);
    let table_id = parse_table_id(&gt.table_id).expect("table id");

    let mut schemas: SchemaSet = HashMap::new();
    schemas.insert(table_id, users_schema());

    let clean = find_fixture(&dir, "clean-");
    let reader = CommitLogReader::open_with_schemas(&clean, schemas).expect("open clean segment");

    // Requirement: valid Cassandra 5.0 segment header parses successfully.
    assert_eq!(reader.descriptor().version, 7, "descriptor version");
    assert!(
        reader.descriptor().compression_class.is_none(),
        "clean fixture must be uncompressed"
    );

    // Collect only OUR table's partition updates from the full mutation stream
    // (the segment also contains system-keyspace mutations — filter by table id).
    let mut decoded: HashMap<i32, (String, i32)> = HashMap::new();
    let mut it = reader.mutations();
    for res in it.by_ref() {
        let mutation = res.expect("mutation decode must not error on the clean segment");
        for upd in &mutation.updates {
            if upd.table_id != table_id {
                continue;
            }
            assert!(upd.rows_decoded, "our table's rows must fully decode");
            assert_eq!(upd.partition_key.len(), 4, "int partition key is 4 bytes");
            let id = i32::from_be_bytes([
                upd.partition_key[0],
                upd.partition_key[1],
                upd.partition_key[2],
                upd.partition_key[3],
            ]);
            assert_eq!(upd.rows.len(), 1, "one row per insert");
            let row = &upd.rows[0];
            let mut name = None;
            let mut age = None;
            for cell in &row.cells {
                match cell.column.as_str() {
                    "name" => {
                        name = cell
                            .value
                            .as_ref()
                            .map(|v| String::from_utf8_lossy(v).into_owned())
                    }
                    "age" => {
                        age = cell.value.as_ref().map(|v| {
                            let b: [u8; 4] = v
                                .as_slice()
                                .try_into()
                                .unwrap_or_else(|_| panic!("age cell value not 4 bytes: {v:?}"));
                            i32::from_be_bytes(b)
                        })
                    }
                    other => panic!("unexpected column {other}"),
                }
            }
            decoded.insert(id, (name.expect("name"), age.expect("age")));
        }
    }
    assert!(!it.truncated(), "clean fixture must not be truncated");

    // The insert-set-vs-decoded-set oracle.
    assert_eq!(
        decoded.len(),
        gt.inserts.len(),
        "decoded exactly the inserted count"
    );
    for row in &gt.inserts {
        let got = decoded
            .get(&row.id)
            .unwrap_or_else(|| panic!("missing decoded mutation for id={}", row.id));
        assert_eq!(got.0, row.name, "name for id={}", row.id);
        assert_eq!(got.1, row.age, "age for id={}", row.id);
    }
}

/// Requirement: torn-tail truncation tolerance.
#[test]
fn truncated_segment_returns_clean_prefix_and_flags_truncation() {
    let dir = commitlog_dir();
    let clean = find_fixture(&dir, "truncated-");
    let reader = CommitLogReader::open(&clean).expect("open truncated segment");

    let mut count = 0usize;
    let mut saw_err = false;
    let mut it = reader.mutations();
    for res in it.by_ref() {
        // Cleanly-decoded records before the tear are returned; a torn body is
        // reported as end-of-stream (truncation), never a panic. A structural
        // decode may still hit corruption if the tear lands mid-record — that is
        // surfaced as a typed Err, which is acceptable (no panic).
        match res {
            Ok(_) => count += 1,
            Err(_) => saw_err = true,
        }
    }
    assert!(count > 0, "some mutations decode before the tear");
    // Exactly one of these outcomes must hold, matching the comment above: the
    // walker either reaches a torn tail cleanly (truncated()) or the fixture's
    // cut point lands mid-record and surfaces as a typed Err first. Asserting
    // only truncated() made this test fixture-offset-fragile — regenerating
    // with a different insert set could shift the tear into the Err case and
    // fail confusingly (roborev finding, review-first pass).
    assert!(
        it.truncated() || saw_err,
        "torn tail must surface as either Iterator::truncated() or a typed Err, not silently"
    );
}

/// Requirement: malformed input never panics (corrupt CRC).
#[test]
fn corrupt_crc_segment_returns_typed_error_without_panic() {
    let dir = commitlog_dir();
    let corrupt = find_fixture(&dir, "corrupt-crc-");
    let reader = CommitLogReader::open(&corrupt).expect("descriptor still parses");

    let mut saw_error = false;
    for res in reader.mutations() {
        if res.is_err() {
            saw_error = true;
            break;
        }
    }
    assert!(
        saw_error,
        "a flipped payload byte must surface as a typed CRC error"
    );
}
