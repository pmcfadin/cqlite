//! CommitLog reader parity oracle (issue #2389).
//!
//! The oracle is the CommitLog analog of CQLite's query-semantics oracle: a
//! KNOWN set of mutations was inserted into a real Cassandra 5.0.8 node and the
//! resulting segment captured before discard (see
//! `test-data/scripts/generate-commitlog-fixtures.sh`). This test asserts that
//! `CommitLogReader` decodes exactly those mutations — table id, partition key,
//! and cell values — matching the committed ground truth. There is no
//! sstabledump-equivalent for commitlog segments, so "what we inserted vs. what
//! the reader produced" IS the oracle.
//!
//! Fixtures are small, committed reference binaries (`git add -f`), so this test
//! never silently passes on missing data.

use std::collections::{BTreeSet, HashMap};
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
    segment: SegmentRecord,
}

/// Which segment the three fixtures are, recorded by the generator: consumers select
/// the files BY NAME from here, never by prefix glob, so a stale extra file under the
/// directory can never be picked up.
#[derive(serde::Deserialize)]
struct SegmentRecord {
    id: i64,
    version: i32,
    files: SegmentFiles,
}

#[derive(serde::Deserialize)]
struct SegmentFiles {
    clean: String,
    truncated: String,
    corrupt_crc: String,
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

/// The fixture file the ground truth names, checked to be the recorded segment: it must
/// exist and its descriptor (`i32 version`, `i64 segment id`, big-endian) must match.
fn recorded_fixture(dir: &Path, gt: &GroundTruth, file_name: &str) -> PathBuf {
    let path = dir.join(file_name);
    let bytes = std::fs::read(&path)
        .unwrap_or_else(|e| panic!("ground truth names {file_name}, unreadable: {e}"));
    let version = i32::from_be_bytes(bytes[0..4].try_into().expect("descriptor version"));
    let id = i64::from_be_bytes(bytes[4..12].try_into().expect("descriptor segment id"));
    assert_eq!(
        (version, id),
        (gt.segment.version, gt.segment.id),
        "{file_name} is not the segment the ground truth records"
    );
    path
}

/// Requirement: mutation stream decoding matches ground truth.
#[test]
fn decoded_mutations_match_inserted_set() {
    let dir = commitlog_dir();
    let gt = load_ground_truth(&dir);
    let table_id = parse_table_id(&gt.table_id).expect("table id");

    let mut schemas: SchemaSet = HashMap::new();
    schemas.insert(table_id, users_schema());

    let clean = recorded_fixture(&dir, &gt, &gt.segment.files.clean);
    let reader = CommitLogReader::open_with_schemas(&clean, schemas).expect("open clean segment");

    // Requirement: valid Cassandra 5.0 segment header parses successfully.
    assert_eq!(reader.descriptor().version, 7, "descriptor version");
    assert!(
        reader.descriptor().compression_class.is_none(),
        "clean fixture must be uncompressed"
    );

    assert_eq!(gt.inserts.len(), 5, "fixture ground truth insert count");
    let ground_truth_ids = gt.inserts.iter().map(|row| row.id).collect::<BTreeSet<_>>();
    assert_eq!(ground_truth_ids, BTreeSet::from([1, 2, 3, 4, 5]));

    // Collect only OUR table's partition updates from the full mutation stream
    // (the segment also contains system-keyspace mutations — filter by table id).
    let mut decoded: HashMap<i32, (String, i32)> = HashMap::new();
    let mut decoded_ids = BTreeSet::new();
    let mut decoded_count = 0usize;
    let mut it = reader.mutations();
    for (_pos, res) in it.by_ref() {
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
            assert!(
                decoded_ids.insert(id),
                "duplicate target-table update for id={id}"
            );
            decoded_count += 1;
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
            assert!(
                decoded
                    .insert(id, (name.expect("name"), age.expect("age")))
                    .is_none(),
                "duplicate decoded row for id={id}"
            );
        }
    }
    assert!(!it.truncated(), "clean fixture must not be truncated");

    // Pin both the target-table count and identity set. The segment also has
    // Cassandra system-table traffic, whose total is intentionally not part of
    // this oracle.
    assert_eq!(decoded_count, 5, "decoded target-table update count");
    assert_eq!(decoded_ids, ground_truth_ids, "decoded target-table ids");

    // The insert-set-vs-decoded-set value oracle.
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
    let gt = load_ground_truth(&dir);
    let table_id = parse_table_id(&gt.table_id).expect("table id");
    assert_eq!(gt.inserts.len(), 5, "fixture ground truth insert count");
    let ground_truth_ids = gt.inserts.iter().map(|row| row.id).collect::<BTreeSet<_>>();
    assert_eq!(ground_truth_ids, BTreeSet::from([1, 2, 3, 4, 5]));

    let torn = recorded_fixture(&dir, &gt, &gt.segment.files.truncated);
    let reader = CommitLogReader::open(&torn).expect("open truncated segment");

    // The real truncated fixture ends after exactly the first four target-table
    // writes. Track identities and reject duplicates so system-table traffic or
    // repeated updates cannot satisfy the oracle accidentally.
    let mut decoded_ids = BTreeSet::new();
    let mut decoded_count = 0usize;
    let mut it = reader.mutations();
    for (_pos, res) in it.by_ref() {
        let mutation = res.expect("truncated fixture must not yield typed errors");
        for update in &mutation.updates {
            if update.table_id != table_id {
                continue;
            }
            assert_eq!(
                update.partition_key.len(),
                4,
                "int partition key is 4 bytes"
            );
            let id = i32::from_be_bytes([
                update.partition_key[0],
                update.partition_key[1],
                update.partition_key[2],
                update.partition_key[3],
            ]);
            assert!(
                decoded_ids.insert(id),
                "duplicate target-table update for id={id}"
            );
            decoded_count += 1;
        }
    }
    assert!(
        decoded_count == 4,
        "expected exactly four target-table updates before the tear, got {decoded_count}"
    );
    assert_eq!(decoded_ids, BTreeSet::from([1, 2, 3, 4]));
    assert!(
        it.truncated(),
        "truncated fixture must terminate with Iterator::truncated()"
    );
}

/// Requirement: malformed input never panics (corrupt CRC).
#[test]
fn corrupt_crc_segment_returns_typed_error_without_panic() {
    let dir = commitlog_dir();
    let gt = load_ground_truth(&dir);
    let corrupt = recorded_fixture(&dir, &gt, &gt.segment.files.corrupt_crc);
    let reader = CommitLogReader::open(&corrupt).expect("descriptor still parses");

    let mut saw_error = false;
    for (_pos, res) in reader.mutations() {
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

/// Selection is by the names the ground truth records: a stale extra segment that a
/// prefix glob would also match (or match first) is ignored, and a file that is not the
/// recorded segment is refused.
#[test]
fn fixtures_are_selected_by_recorded_name_not_by_prefix() {
    let dir = commitlog_dir();
    let gt = load_ground_truth(&dir);
    let tmp = tempfile::tempdir().expect("tempdir");
    for name in [
        &gt.segment.files.clean,
        &gt.segment.files.truncated,
        &gt.segment.files.corrupt_crc,
    ] {
        std::fs::copy(dir.join(name), tmp.path().join(name)).expect("copy fixture");
    }
    // A decoy from an older generation: same prefix, different (still valid) segment.
    let decoy = format!("clean-CommitLog-{}-1.log", gt.segment.version);
    let mut bytes = std::fs::read(dir.join(&gt.segment.files.clean)).expect("read clean");
    bytes[4..12].copy_from_slice(&1i64.to_be_bytes());
    std::fs::write(tmp.path().join(&decoy), &bytes).expect("write decoy");

    let picked = recorded_fixture(tmp.path(), &gt, &gt.segment.files.clean);
    assert_eq!(
        picked.file_name().and_then(|n| n.to_str()),
        Some(gt.segment.files.clean.as_str())
    );
    // Asking for the decoy by name is refused: its descriptor is not the recorded segment.
    let refused = std::panic::catch_unwind(|| recorded_fixture(tmp.path(), &gt, &decoy));
    assert!(refused.is_err(), "a non-recorded segment must be refused");
}
