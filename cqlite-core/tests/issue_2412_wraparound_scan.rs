//! Issue #2412 — roborev endgame finding (High, silent data loss): a WRAPAROUND
//! token-range warm scan must enumerate BOTH ring segments, not just the one the
//! forward-only `Index.db` walk happens to start in.
//!
//! `summary_scan.rs`'s `walk_in_range_partition_slices` streams `Index.db`
//! FORWARD from a single start offset to EOF — the file is not circular. Before
//! this fix, a wraparound range `(start_excl, MAX] ∪ [MIN, end_incl]`
//! (`start_excl > end_incl`) began the walk at
//! `scan_start_position_for_token(start_excl)` — the HIGH-token segment's start
//! — which can only ever reach entries AT OR AFTER that file position. The
//! LOW-token segment (`token <= end_incl`) physically precedes it in the
//! token-ordered `Index.db` and was silently skipped in full: every in-range
//! partition in that segment vanished from the result with no error, no WARN,
//! nothing (issue #28: authoritative structure was available — the summary
//! samples make the LOW segment's positions perfectly locatable — this was a
//! pure walk-direction bug, not a missing-data one).
//!
//! Fix: a wraparound range starts the walk at offset 0 (the true beginning of
//! `Index.db`) so both segments are reachable; the per-entry
//! `ScanTokenBound::contains` filter already selects exactly the two segments,
//! and `can_stop_past` already refuses to early-stop for a wraparound bound
//! (verified here to still hold, not merely assumed). Wrapping itself is DERIVED
//! from the endpoints, `start_excl >= end_incl`, the way `Range.isWrapAround`
//! does it (#3634) — it was a caller-supplied flag when this test was written.
//!
//! This test builds ONE BIG generation with `N` partitions (comfortably over
//! `min_index_interval` so `Summary.db` carries multiple samples — otherwise a
//! single-sample summary makes `scan_start_position_for_token` return offset 0
//! for ANY token, masking the bug), picks a wraparound range whose two segments
//! BOTH hold real in-range partitions (verified from the ACTUAL computed token
//! order, never assumed), and asserts every partition in EITHER segment is
//! emitted through the public `stream_all_partitions_for_query` surface — the
//! same call chain the flight warm query path drives.

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::sstable::reader::{SSTableReader, ScanTokenBound};
use cqlite_core::storage::sstable::summary_reader::SummaryReader;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::util::cassandra_murmur3::cassandra_murmur3_token;
use cqlite_core::Config;

const KS: &str = "wraparound_ks";
const TBL: &str = "items";
/// Comfortably over the default `min_index_interval` (128) so `Summary.db`
/// carries multiple samples spanning distinct `Index.db` positions — a
/// single-sample summary would mask the bug (its one covering position is
/// always offset 0, so even the OLD forward-only walk would "accidentally"
/// start at the beginning).
const N: usize = 400;
/// Token rank the full-ring case picks. Deliberately well into the token order,
/// but the discriminating property is never ASSUMED from this number — the test
/// MEASURES the floor-sample offset this token would have produced (see
/// `full_ring_emits_every_partition_not_just_those_past_the_floor_sample`).
const FULL_RING_RANK: usize = 250;

fn schema() -> TableSchema {
    TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn key_bytes(id: i32) -> Vec<u8> {
    // Single-component int partition key: raw 4-byte big-endian value (the
    // on-disk raw-key encoding CQLite's writer uses for a single `int` PK).
    id.to_be_bytes().to_vec()
}

fn mutation(id: i32) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::text(format!("v{id}")),
        }],
        1_000_000 + id as i64,
        None,
    )
}

/// Build ONE BIG generation with `N` single-int-PK partitions, all in one
/// generation (large flush threshold — mirrors the established convention in
/// `cqlite-flight::warm::registry::spin_tests_2383::build_big_single_gen`).
fn build_single_gen() -> (tempfile::TempDir, PathBuf) {
    let sch = schema();
    let temp = tempfile::TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, sch)
        .with_flush_threshold(1usize << 30)
        .with_durability(cqlite_core::storage::write_engine::Durability::Disabled);
    let mut engine = WriteEngine::new(config).expect("engine");
    for id in 0..N as i32 {
        engine.write(mutation(id)).expect("write");
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(engine.flush()).expect("flush").expect("info");

    let table_dir = data_dir.join(KS).join(TBL);
    let data_files: Vec<_> = std::fs::read_dir(&table_dir)
        .expect("table dir")
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
        .collect();
    assert_eq!(
        data_files.len(),
        1,
        "fixture must hold exactly ONE generation"
    );
    (temp, data_files[0].path())
}

async fn open_reader(data_path: &std::path::Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.unwrap());
    SSTableReader::open(data_path, &config, platform)
        .await
        .unwrap()
}

/// Drive `stream_all_partitions_for_query` and collect every emitted partition
/// key's decoded `id` (int PK) — the public surface the flight warm query path
/// (`from_readers::drive_query_stream`) calls.
async fn emitted_ids(reader: &SSTableReader, token_bound: Option<ScanTokenBound>) -> Vec<i32> {
    let sch = schema();
    let cancel = ScanCancel::default();
    let mut ids = Vec::new();
    reader
        .stream_all_partitions_for_query(Some(&sch), &cancel, token_bound, |row| {
            let key_bytes: &[u8] = &row.key.0;
            let id = i32::from_be_bytes(key_bytes.try_into().expect("4-byte int PK"));
            ids.push(id);
            Ok(ControlFlow::Continue(()))
        })
        .await
        .expect("stream_all_partitions_for_query");
    ids.sort_unstable();
    ids
}

/// THE blocker pin (roborev endgame, High): a wraparound range whose two
/// segments BOTH hold real partitions must emit every partition in EITHER
/// segment — not just the one the forward walk happens to start in.
#[test]
fn wraparound_range_emits_partitions_from_both_segments() {
    // Compute every partition's ACTUAL token (never assumed) and sort ascending
    // — this is the token order Index.db/Summary.db are physically written in.
    let mut by_token: Vec<(i32, i64)> = (0..N as i32)
        .map(|id| (id, cassandra_murmur3_token(&key_bytes(id))))
        .collect();
    by_token.sort_by_key(|(_, tok)| *tok);

    // HIGH segment: ranks 251..N (49 partitions) — start_excl = rank 250's token.
    // LOW segment: ranks 0..=10 (11 partitions) — end_incl = rank 10's token.
    let start_excl = by_token[250].1;
    let end_incl = by_token[10].1;
    assert!(
        start_excl > end_incl,
        "fixture must produce a genuine wraparound pair (start > end); got \
         start_excl={start_excl} end_incl={end_incl}"
    );

    let expected_low: Vec<i32> = by_token[0..=10].iter().map(|(id, _)| *id).collect();
    let expected_high: Vec<i32> = by_token[251..N].iter().map(|(id, _)| *id).collect();
    let mut expected: Vec<i32> = expected_low
        .iter()
        .chain(expected_high.iter())
        .copied()
        .collect();
    expected.sort_unstable();
    // Non-vacuity: BOTH segments must genuinely hold partitions, or this test
    // cannot discriminate "walk started in the wrong segment" from "no bug to find".
    assert!(!expected_low.is_empty() && !expected_high.is_empty());

    let (_temp, data_path) = build_single_gen();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let reader = rt.block_on(open_reader(&data_path));

    let bound = ScanTokenBound {
        start_excl,
        end_incl,
    };
    let got = rt.block_on(emitted_ids(&reader, Some(bound)));

    assert_eq!(
        got,
        expected,
        "a wraparound range must emit every partition in BOTH segments (low: \
         {} partitions, high: {} partitions) — got {} partitions total. A \
         forward-only walk starting in the high segment silently drops the low \
         segment (roborev endgame finding, High, data loss)",
        expected_low.len(),
        expected_high.len(),
        got.len()
    );
}

/// The FULL RING (`start_excl == end_incl`, the #2228 convention) must emit
/// EVERY partition — end to end, through the same public surface.
///
/// This pins a behaviour the flag-carrying `ScanTokenBound` got WRONG, and is a
/// SUPERSET of #3634's stated acceptance criteria: #3634 asks only that wrapping
/// be derived from the endpoints, and this data-loss fix falls out of it.
///
/// Under the old form a full ring was built with `wraparound: false` (flight's
/// single-token topology, `query_rows_panic_tests::full_ring()`), so it took the
/// NON-wrapping arm of the walk's start-offset choice and began at
/// `scan_start_position_for_token(start_excl)`. That is the FLOOR SAMPLE's
/// position, not the beginning: `summary_reader/mod.rs`'s
/// `scan_start_position_for_token` takes `partition_point(token <= start_excl)`
/// and then `saturating_sub(1)`, i.e. the last sample AT OR BELOW the token. Yet
/// `contains` answered `true` for every token (the `#2228` early-return), so
/// every partition sorting below that sample was silently dropped — a partial
/// result set with no error, the failure mode that reads as "the range is empty".
///
/// Deriving wrapping from `start_excl >= end_incl` puts the full ring on the
/// wrapping arm, where the walk starts at offset 0 and the filter admits
/// everything: a full walk, unfiltered in effect.
///
/// That the case DISCRIMINATES is measured, not assumed: the test asks this
/// fixture's own `Summary.db` for `scan_start_position_for_token(token)` — the
/// exact offset the old code would have started at — and requires it to be
/// non-zero. An earlier draft compared two consts (`FULL_RING_RANK >
/// DEFAULT_MIN_INDEX_INTERVAL`), which is a tautology at runtime and would have
/// let a changed sampling interval quietly make this case inert.
#[test]
fn full_ring_emits_every_partition_not_just_those_past_the_floor_sample() {
    let mut by_token: Vec<(i32, i64)> = (0..N as i32)
        .map(|id| (id, cassandra_murmur3_token(&key_bytes(id))))
        .collect();
    by_token.sort_by_key(|(_, tok)| *tok);

    assert_eq!(by_token.len(), N, "fixture must hold all {N} partitions");
    let token = by_token[FULL_RING_RANK].1;

    // Every partition, because the full ring admits every token (#2228).
    let mut expected: Vec<i32> = by_token.iter().map(|(id, _)| *id).collect();
    expected.sort_unstable();
    assert_eq!(expected.len(), N);

    let (_temp, data_path) = build_single_gen();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let reader = rt.block_on(open_reader(&data_path));

    // Non-vacuity, and the whole reason this case discriminates: MEASURE the
    // offset the OLD code would have started at, rather than inferring it from a
    // hard-coded `min_index_interval`. `scan_start_position_for_token` is this
    // fixture's own `Summary.db` answering for this exact token, so a non-zero
    // result is affirmative evidence that the old full-ring path began PAST the
    // beginning and therefore skipped every partition below that sample. If this
    // ever measures 0 the case is inert, and saying so is better than a green
    // test that proves nothing.
    let old_start_offset = rt.block_on(async {
        let summary_path = data_path.with_file_name(
            data_path
                .file_name()
                .and_then(|n| n.to_str())
                .expect("fixture Data.db name is valid UTF-8")
                .replace("-Data.db", "-Summary.db"),
        );
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        SummaryReader::open(&summary_path, platform)
            .await
            .expect("fixture must carry a readable Summary.db")
            .scan_start_position_for_token(token)
    });
    assert_ne!(
        old_start_offset, 0,
        "this case cannot discriminate: the floor sample for the full-ring token \
         (rank {FULL_RING_RANK} of {N}) is already offset 0, so the old \
         flag-carrying behaviour would have started at the beginning too and this \
         test would pass either way"
    );

    let bound = ScanTokenBound {
        start_excl: token,
        end_incl: token,
    };
    assert!(
        bound.is_wraparound(),
        "equal endpoints must wrap (Range.isWrapAround\'s `>=`, #3634) — that is \
         what routes the walk to offset 0"
    );
    let got = rt.block_on(emitted_ids(&reader, Some(bound)));

    // Non-vacuity, part 2: an empty emit is never a pass here.
    assert!(
        !got.is_empty(),
        "the full ring emitted NOTHING — the fixture or the walk is broken, and \
         an empty result must never read as agreement"
    );
    let dropped: Vec<i32> = expected
        .iter()
        .copied()
        .filter(|id| !got.contains(id))
        .collect();
    assert_eq!(
        got,
        expected,
        "a FULL-RING bound ({token}, {token}] must emit all {} partitions; got \
         {} and dropped {} of them. This is the flag-carrying form\'s data loss: \
         the walk started at the FLOOR SAMPLE for the token (rank \
         {FULL_RING_RANK} of {N}) instead of offset 0, so every partition sorting \
         BELOW that sample was skipped while `contains` still admitted every \
         token",
        expected.len(),
        got.len(),
        dropped.len()
    );
}

/// Control: a NORMAL (non-wraparound) range is unaffected by this fix — it must
/// still emit exactly its one contiguous segment, matching the pre-fix behavior.
#[test]
fn non_wraparound_range_is_unaffected() {
    let mut by_token: Vec<(i32, i64)> = (0..N as i32)
        .map(|id| (id, cassandra_murmur3_token(&key_bytes(id))))
        .collect();
    by_token.sort_by_key(|(_, tok)| *tok);

    // A contiguous mid-ring segment: ranks 100..=150.
    let start_excl = by_token[99].1;
    let end_incl = by_token[150].1;
    assert!(
        start_excl < end_incl,
        "non-wraparound pair must have start < end"
    );

    let mut expected: Vec<i32> = by_token[100..=150].iter().map(|(id, _)| *id).collect();
    expected.sort_unstable();
    assert!(!expected.is_empty());

    let (_temp, data_path) = build_single_gen();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let reader = rt.block_on(open_reader(&data_path));

    let bound = ScanTokenBound {
        start_excl,
        end_incl,
    };
    let got = rt.block_on(emitted_ids(&reader, Some(bound)));

    assert_eq!(
        got, expected,
        "a non-wraparound range must emit exactly its one contiguous segment, \
         unaffected by the wraparound fix"
    );
}
