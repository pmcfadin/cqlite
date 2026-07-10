//! Partition point-read behavioral tests (issue #2207, Stages 2–5).
//!
//! Exercises the wired `do_get` point path over real in-process SSTables (built
//! via the write engine, so no external fixtures): the work-done probe (merge
//! steps bounded by candidate lookups, not table partition count), dual-path
//! byte-parity vs the full-scan+filter path over a tombstoned multi-generation
//! corpus, the index-less fail-safe (#2295 shape), cooperative cancellation
//! (#2264), and LIMIT discipline. These live in-crate (not `tests/`) so they can
//! use the `testutil` write-engine fixture builders and the pub(crate)
//! `produce_streaming_to_vec` seam.

use crate::cancel::CancelFlag;
use crate::filter::ScanSpec;
use crate::producer::{DirSource, MergeProducer, ProducerError, SstableSource};
use crate::testutil::{
    build_sstables, clustering_schema, delete_row, simple_schema, write_clustered, write_row,
};
use crate::ticket::{FlightTicket, Predicate, PredicateExpr, PredicateOp};

use cqlite_core::schema::TableSchema;
use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::write_engine::merge::MergeStep;
use cqlite_core::storage::write_engine::{build_single_partition_merger, KWayMerger, PartitionKey};
use cqlite_core::types::Value;
use serde_json::json;

/// A `do_get` ticket carrying a single `column = value` (int) equality predicate.
fn eq_ticket(column: &str, value: i32) -> FlightTicket {
    FlightTicket {
        keyspace: "flight_ks".into(),
        table: "items".into(),
        predicates: vec![Predicate {
            column: column.into(),
            op: PredicateOp::Equal,
            value: json!(value),
        }],
        ..Default::default()
    }
}

/// Raw partition-key bytes for a single-int PK.
fn int_pk_bytes(schema: &cqlite_core::schema::TableSchema, column: &str, v: i32) -> Vec<u8> {
    PartitionKey::single(column, Value::Integer(v))
        .to_bytes(schema)
        .expect("serialize pk")
}

/// Count the partitions a merger emits (steps until `Complete`).
fn count_partitions(mut merger: KWayMerger) -> usize {
    let mut n = 0;
    while let MergeStep::Partition { .. } = merger.step().expect("step") {
        n += 1;
    }
    n
}

/// The rows a `simple_schema` producer emits, as `(id, name, score)` sorted.
fn simple_rows(
    batches: &[arrow::record_batch::RecordBatch],
) -> Vec<(i32, Option<String>, Option<i32>)> {
    use arrow::array::{Array, Int32Array, StringArray};
    let mut out = Vec::new();
    for b in batches {
        let ids = b
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = b
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let scores = b
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            let name = if names.is_null(i) {
                None
            } else {
                Some(names.value(i).to_string())
            };
            let score = if scores.is_null(i) {
                None
            } else {
                Some(scores.value(i))
            };
            out.push((ids.value(i), name, score));
        }
    }
    out.sort();
    out
}

// ---- Stage 3.3: work-done probe (merge steps bounded by candidate lookups) ----

#[test]
fn point_merger_steps_only_the_target_partition_not_the_whole_table() {
    let schema = simple_schema();
    // 12 distinct partitions in one SSTable.
    let rows: Vec<_> = (0..12)
        .map(|i| write_row(i, &format!("n{i}"), i * 10, 100))
        .collect();
    let (_t, _d, dir) = build_sstables(&schema, vec![rows]);
    let paths = DirSource::new(&dir).data_paths().unwrap();

    // A full k-way scan steps EVERY partition (this is what `do_get` does on main).
    let full = KWayMerger::new(paths.clone(), &schema).unwrap();
    assert_eq!(count_partitions(full), 12, "the table has 12 partitions");

    // The point merger for id=3 steps ONE partition — the work-done proof. On main
    // there is no point path, so the same query steps all 12; this bounds it to 1.
    let key = int_pk_bytes(&schema, "id", 3);
    let merger = build_single_partition_merger(paths, &[key], &schema, ScanCancel::default())
        .unwrap()
        .expect("target partition present");
    assert_eq!(
        count_partitions(merger),
        1,
        "point path examines only the target partition, not all 12"
    );
}

#[test]
fn point_merger_for_absent_key_is_none() {
    let schema = simple_schema();
    let rows: Vec<_> = (0..5).map(|i| write_row(i, "n", i, 100)).collect();
    let (_t, _d, dir) = build_sstables(&schema, vec![rows]);
    let paths = DirSource::new(&dir).data_paths().unwrap();

    // id=999 is not present → no runs → None (the caller streams zero rows).
    let key = int_pk_bytes(&schema, "id", 999);
    let built =
        build_single_partition_merger(paths, &[key], &schema, ScanCancel::default()).unwrap();
    assert!(built.is_none(), "absent key yields no merger");
}

#[test]
fn full_pk_in_list_is_bounded_by_the_listed_keys() {
    let schema = simple_schema();
    let rows: Vec<_> = (0..20).map(|i| write_row(i, "n", i, 100)).collect();
    let (_t, _d, dir) = build_sstables(&schema, vec![rows]);
    let paths = DirSource::new(&dir).data_paths().unwrap();

    // IN (3, 7, 11) → exactly 3 partitions stepped, not 20.
    let keys: Vec<Vec<u8>> = [3, 7, 11]
        .iter()
        .map(|v| int_pk_bytes(&schema, "id", *v))
        .collect();
    let merger = build_single_partition_merger(paths, &keys, &schema, ScanCancel::default())
        .unwrap()
        .expect("some present");
    assert_eq!(count_partitions(merger), 3, "bounded by the 3 listed keys");
}

/// SUGGESTION (roborev, issue #2207): `pk IN (5, 5)` must not double-seek the
/// same partition — a duplicate key in the requested set is deduped BEFORE
/// probing, so the merge sees exactly ONE `MergeEntry` for that partition per
/// candidate SSTable, never two. Note: `merge_partition_rows`'s per-cluster
/// reconciliation already absorbs a redundant same-run duplicate for THIS
/// fixture shape without a wrong final answer (this test also passes without
/// the dedup canonicalization) — so this is a no-op-safety + work-avoidance
/// regression guard (the SUGGESTION's actual "double-seek" concern), not a
/// red-then-green correctness proof.
#[test]
fn duplicate_in_list_key_does_not_double_seek_the_partition() {
    let schema = clustering_schema();
    let (_t, _d, dir) = build_sstables(&schema, vec![vec![write_clustered(3, "a", 1, 100)]]);
    let paths = DirSource::new(&dir).data_paths().unwrap();

    let key = int_pk_bytes(&schema, "pk", 3);
    // The exact same key requested twice.
    let merger =
        build_single_partition_merger(paths, &[key.clone(), key], &schema, ScanCancel::default())
            .unwrap()
            .expect("the partition is present");

    let mut steps = 0;
    let mut row_count = 0;
    let mut m = merger;
    while let MergeStep::Partition { rows, .. } = m.step().unwrap() {
        steps += 1;
        row_count += rows.len();
    }
    assert_eq!(steps, 1, "a duplicated key must still yield ONE partition");
    assert_eq!(
        row_count, 1,
        "a duplicated key must not duplicate the partition's single clustering row"
    );
}

/// The Murmur3 token for `column = v` under `schema`, via the same public
/// `PartitionKey::to_decorated_key` the routing/merge machinery uses.
fn token_of(schema: &TableSchema, column: &str, v: i32) -> i64 {
    PartitionKey::single(column, Value::Integer(v))
        .to_decorated_key(schema)
        .expect("decorate key")
        .token
}

/// **BLOCKER regression** (roborev on f75dccc2): a multi-key point read (`IN` /
/// `Or`) combines each candidate SSTable's seeked rows in REQUESTED-KEY order,
/// not token order. `KWayMerger::step`/`refill_heap` require every run to yield
/// entries in ascending `MergeEntry` order (token, key, clustering) —
/// `refill_heap` buffers only ONE entry per run at a time and relies on that
/// invariant. An out-of-order run causes `step()` to split one run's
/// contribution across two heap pops: partitions can duplicate, and a newer
/// generation's overwrite/tombstone can stop shadowing the older generation
/// (split reconciliation).
///
/// This fixture requests 3 keys in the EXACT REVERSE of their natural ascending
/// token order (computed, not guessed) — the worst-case mismatch — with one key
/// overwritten and one row-tombstoned in a newer generation, and asserts the
/// point path is byte-identical to the scan path (and matches the expected
/// post-reconciliation rows directly).
#[test]
fn point_path_in_list_out_of_token_order_matches_scan_across_generations() {
    let schema = simple_schema();
    let ids = [5, 9, 13];

    // gen1: all three present.
    let gen1 = vec![
        write_row(ids[0], "a1", 1, 100),
        write_row(ids[1], "b1", 1, 100),
        write_row(ids[2], "c1", 1, 100),
    ];
    // gen2: ids[1] overwritten (newer ts); ids[2] row-tombstoned (newer ts).
    let gen2 = vec![write_row(ids[1], "b2", 2, 200), delete_row(ids[2], 300)];
    let (_t, _d, dir) = build_sstables(&schema, vec![gen1, gen2]);

    // Natural ascending token order for the 3 distinct ids, then request them in
    // the EXACT REVERSE — deliberately mismatched (never "happens to align").
    let mut by_token: Vec<i32> = ids.to_vec();
    by_token.sort_by_key(|&v| token_of(&schema, "id", v));
    let request_order: Vec<i32> = by_token.iter().rev().copied().collect();
    assert_ne!(
        request_order, by_token,
        "3 distinct ids' reverse token order must differ from ascending order (test setup)"
    );

    let ticket = FlightTicket {
        keyspace: "flight_ks".into(),
        table: "items".into(),
        filter: Some(PredicateExpr::In {
            column: "id".into(),
            values: request_order.iter().map(|v| json!(v)).collect(),
        }),
        ..Default::default()
    };
    let spec = ScanSpec::from_ticket(&ticket, &schema).unwrap();

    let scan_producer = MergeProducer::with_spec(schema.clone(), 4, spec.clone()).unwrap();
    let scan_batches = scan_producer
        .produce_from_paths(DirSource::new(&dir).data_paths().unwrap())
        .unwrap();

    let point_producer = MergeProducer::with_spec(schema.clone(), 4, spec).unwrap();
    let paths = point_producer.resolve_paths(&DirSource::new(&dir)).unwrap();
    let point_batches = point_producer
        .produce_streaming_to_vec(paths, &CancelFlag::new())
        .unwrap();

    assert_eq!(
        simple_rows(&point_batches),
        simple_rows(&scan_batches),
        "an out-of-token-order IN-list point read must be byte-identical to the scan path"
    );
    // Concretely: ids[0] is untouched (gen1 live), ids[1] is the gen2 overwrite,
    // ids[2] is tombstoned (absent from the result).
    let expected = vec![
        (ids[0], Some("a1".to_string()), Some(1)),
        (ids[1], Some("b2".to_string()), Some(2)),
    ];
    assert_eq!(
        simple_rows(&point_batches),
        expected,
        "reconciliation must survive the out-of-token-order combine: ids[1] shows the \
         gen2 overwrite (b2/2), ids[2] is shadowed by its gen2 tombstone"
    );
}

// ---- Stage 3.1/3.2: dual-path byte-parity over a tombstoned multi-gen corpus ----

/// Build a 2-generation `simple_schema` corpus with an overwrite and a tombstone:
/// - gen1: id3=(a,1), id4=(x,1), id7=(z,1)
/// - gen2: id3=(a2,2) overwrite (newer ts), id7 row-tombstoned (newer ts)
fn tombstoned_corpus() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    cqlite_core::schema::TableSchema,
) {
    let schema = simple_schema();
    let gen1 = vec![
        write_row(3, "a", 1, 100),
        write_row(4, "x", 1, 100),
        write_row(7, "z", 1, 100),
    ];
    let gen2 = vec![write_row(3, "a2", 2, 200), delete_row(7, 300)];
    let (temp, _data, dir) = build_sstables(&schema, vec![gen1, gen2]);
    (temp, dir, schema)
}

fn assert_point_equals_scan(
    schema: &cqlite_core::schema::TableSchema,
    dir: &std::path::Path,
    key_col: &str,
    key_val: i32,
) {
    let ticket = eq_ticket(key_col, key_val);
    let spec = ScanSpec::from_ticket(&ticket, schema).unwrap();

    // Scan path (route not applied — the collect path always full-scans + filters).
    let scan_producer = MergeProducer::with_spec(schema.clone(), 4, spec.clone()).unwrap();
    let scan_batches = scan_producer
        .produce_from_paths(DirSource::new(dir).data_paths().unwrap())
        .unwrap();

    // Point path (route applied) over the same resolved paths.
    let point_producer = MergeProducer::with_spec(schema.clone(), 4, spec).unwrap();
    let paths = point_producer.resolve_paths(&DirSource::new(dir)).unwrap();
    let point_batches = point_producer
        .produce_streaming_to_vec(paths, &CancelFlag::new())
        .unwrap();

    assert_eq!(
        simple_rows(&point_batches),
        simple_rows(&scan_batches),
        "point path must be byte-identical to scan+filter for pk={key_val}"
    );
}

#[test]
fn point_path_matches_scan_on_overwritten_key() {
    let (_t, dir, schema) = tombstoned_corpus();
    // id3 was overwritten in gen2 → LWW picks (a2, 2) on BOTH paths.
    assert_point_equals_scan(&schema, &dir, "id", 3);
    // Spot-check the reconciled value is the newer one.
    let ticket = eq_ticket("id", 3);
    let spec = ScanSpec::from_ticket(&ticket, &schema).unwrap();
    let producer = MergeProducer::with_spec(schema.clone(), 4, spec).unwrap();
    let paths = producer.resolve_paths(&DirSource::new(&dir)).unwrap();
    let rows = simple_rows(
        &producer
            .produce_streaming_to_vec(paths, &CancelFlag::new())
            .unwrap(),
    );
    assert_eq!(rows, vec![(3, Some("a2".to_string()), Some(2))]);
}

#[test]
fn point_path_matches_scan_on_tombstoned_key() {
    let (_t, dir, schema) = tombstoned_corpus();
    // id7 was row-tombstoned in gen2 → BOTH paths return zero rows (reconciled).
    assert_point_equals_scan(&schema, &dir, "id", 7);
    let ticket = eq_ticket("id", 7);
    let spec = ScanSpec::from_ticket(&ticket, &schema).unwrap();
    let producer = MergeProducer::with_spec(schema.clone(), 4, spec).unwrap();
    let paths = producer.resolve_paths(&DirSource::new(&dir)).unwrap();
    let rows = simple_rows(
        &producer
            .produce_streaming_to_vec(paths, &CancelFlag::new())
            .unwrap(),
    );
    assert!(
        rows.is_empty(),
        "a tombstoned partition yields no rows on the point path"
    );
}

#[test]
fn point_path_matches_scan_on_live_untouched_key() {
    let (_t, dir, schema) = tombstoned_corpus();
    assert_point_equals_scan(&schema, &dir, "id", 4);
}

// ---- Stage 4.1: index-less fail-safe (#2295 Data.db-only shape) ----

#[test]
fn index_less_candidate_is_read_never_skipped() {
    let schema = simple_schema();
    let (_t, _d, dir) = build_sstables(&schema, vec![vec![write_row(3, "keep", 9, 100)]]);

    // Simulate the #2295 field shape: strip the random-access index (Summary.db)
    // so `has_partition_index()` is false and the reader must fall back to a scan.
    let mut stripped = 0;
    for entry in std::fs::read_dir(&dir).unwrap().flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with("Summary.db") {
            std::fs::remove_file(entry.path()).unwrap();
            stripped += 1;
        }
    }
    assert!(stripped >= 1, "the fixture must ship a Summary.db to strip");

    let ticket = eq_ticket("id", 3);
    let spec = ScanSpec::from_ticket(&ticket, &schema).unwrap();
    let producer = MergeProducer::with_spec(schema.clone(), 4, spec).unwrap();
    let paths = producer.resolve_paths(&DirSource::new(&dir)).unwrap();
    let rows = simple_rows(
        &producer
            .produce_streaming_to_vec(paths, &CancelFlag::new())
            .unwrap(),
    );

    // The key lives ONLY in the index-less SSTable: it MUST still be returned. The
    // inverted "skip on missing index" behaviour would drop it and fail here.
    assert_eq!(
        rows,
        vec![(3, Some("keep".to_string()), Some(9))],
        "index-less SSTable must be scanned, never skipped (fail-safe)"
    );
}

// ---- Stage 4.2: cooperative cancellation + LIMIT ----

#[test]
fn pre_cancelled_point_read_stops_without_masking_errors() {
    let schema = simple_schema();
    let rows: Vec<_> = (0..8).map(|i| write_row(i, "n", i, 100)).collect();
    let (_t, _d, dir) = build_sstables(&schema, vec![rows]);

    let ticket = eq_ticket("id", 3);
    let spec = ScanSpec::from_ticket(&ticket, &schema).unwrap();
    let producer = MergeProducer::with_spec(schema.clone(), 4, spec).unwrap();
    let paths = producer.resolve_paths(&DirSource::new(&dir)).unwrap();

    let cancelled = CancelFlag::new();
    cancelled.cancel();
    let err = producer
        .produce_streaming_to_vec(paths, &cancelled)
        .expect_err("a pre-cancelled point read aborts");
    // Cancellation maps to the DISTINCT Cancelled variant, never a masked Merge
    // error (issue #2264).
    assert!(
        matches!(err, ProducerError::Cancelled),
        "pre-cancel must surface as Cancelled, got {err:?}"
    );
}

#[test]
fn point_read_respects_limit_over_a_wide_partition() {
    let schema = clustering_schema();
    // One partition pk=3 with 5 clustering rows.
    let rows: Vec<_> = ["a", "b", "c", "d", "e"]
        .iter()
        .enumerate()
        .map(|(i, ck)| write_clustered(3, ck, i as i32, 100))
        .collect();
    let (_t, _d, dir) = build_sstables(&schema, vec![rows]);

    let ticket = FlightTicket {
        keyspace: "flight_ks".into(),
        table: "wide".into(),
        predicates: vec![Predicate {
            column: "pk".into(),
            op: PredicateOp::Equal,
            value: json!(3),
        }],
        limit: Some(2),
        ..Default::default()
    };
    let spec = ScanSpec::from_ticket(&ticket, &schema).unwrap();
    let producer = MergeProducer::with_spec(schema.clone(), 4, spec).unwrap();
    let paths = producer.resolve_paths(&DirSource::new(&dir)).unwrap();
    let batches = producer
        .produce_streaming_to_vec(paths, &CancelFlag::new())
        .unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2, "LIMIT 2 streams at most 2 rows on the point path");
}
