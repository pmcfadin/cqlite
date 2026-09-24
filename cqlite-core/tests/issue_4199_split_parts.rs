//! Issue #4199 (spec R4.1, R4.3) — `split_sstable --parts N` divides the
//! source exactly once: the union of every part's rows equals the source's
//! rows exactly once each, ranges are disjoint and strictly ascending, and
//! every part is independently `verify --mode full` clean (already asserted
//! internally by `split_sstable` before a part is ever published — this test
//! additionally reads each part back and checks its row count against the
//! manifest's own `rows`).

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use cqlite_core::storage::write_engine::extract_split::{split_sstable, SplitBoundary};
use tempfile::TempDir;

#[path = "support/extract_split_fixture.rs"]
mod fixture;
use fixture::{datasets_root, decode_all_rows, require_fixtures_strict, single_data_db, table_schema, KS, TABLE};

const N: u32 = 4;

#[tokio::test]
async fn parts_n_partitions_the_source_exactly_once() {
    let Ok(table_dir) = datasets_root::resolve_table_generation_dir(KS, TABLE) else {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but {KS}.{TABLE} is absent; {}",
                datasets_root::describe_search(KS, TABLE)
            );
        }
        eprintln!("[issue_4199] {KS}.{TABLE} fixture absent (dataset not fetched); skipping");
        return;
    };

    let schema = table_schema();
    let source_data_db = single_data_db(&table_dir);
    let source_rows = decode_all_rows(&source_data_db, &schema).await;
    assert!(
        source_rows.len() >= N as usize,
        "fixture must carry at least N rows to make an N-way split meaningful"
    );

    let temp = TempDir::new().expect("tempdir");
    let out_dir = temp.path().join("out");
    let report = split_sstable(&source_data_db, SplitBoundary::Parts(N), &out_dir, &schema)
        .await
        .expect("split_sstable must not error for a healthy input");

    assert!(report.refused.is_none(), "got {:?}", report.refused);
    assert_eq!(report.parts.len(), N as usize, "exactly N parts must be produced");
    for p in &report.parts {
        assert_eq!(p.verify, "pass", "every part must be verify --mode full clean");
    }

    // Disjoint, strictly ascending token ranges (D3.2), asserted directly.
    for w in report.parts.windows(2) {
        assert!(
            w[1].min_token > w[0].max_token,
            "part token ranges must be disjoint and strictly ascending: {:?} then {:?}",
            w[0],
            w[1]
        );
    }

    // Partition-count sum equals the source's total.
    let total_partitions: usize = report.parts.iter().map(|p| p.partitions).sum();
    assert_eq!(total_partitions, report.source_partitions);

    // Union of per-part dumps equals the staged source's dump: no duplicate,
    // no missing partition.
    let mut union_rows = Vec::new();
    for (idx, part) in report.parts.iter().enumerate() {
        let part_table_dir = out_dir
            .join(format!("part-{idx:04}"))
            .join(&schema.keyspace)
            .join(&schema.table);
        let part_data_db = single_data_db(&part_table_dir);
        let part_rows = decode_all_rows(&part_data_db, &schema).await;
        assert_eq!(
            part_rows.len(),
            part.rows,
            "part {idx}'s read-back row count must equal its manifest's `rows`"
        );
        union_rows.extend(part_rows);
    }

    let mut union_keys: Vec<_> = union_rows.iter().map(|r| r.key.0.to_vec()).collect();
    union_keys.sort();
    union_keys.dedup();
    let mut source_keys: Vec<_> = source_rows.iter().map(|r| r.key.0.to_vec()).collect();
    source_keys.sort();
    source_keys.dedup();
    assert_eq!(
        union_keys, source_keys,
        "the union of every part's partition keys must equal the source's, no duplicate/missing"
    );
    assert_eq!(
        union_rows.len(),
        source_rows.len(),
        "no duplicate/missing ROW (not just partition) across the union"
    );

    // Sort by (partition key, row timestamp) rather than key alone: a
    // key-only sort is STABLE, so it preserves each side's OWN pre-sort
    // relative order for same-key rows — which is exactly the property this
    // comparison must NOT depend on (split's writer re-sorts a partition's
    // rows by clustering key before writing, so nothing guarantees the two
    // sides visit a multi-row partition's rows in the same relative order).
    // `row_timestamp` gives a real total order without relying on that.
    let sort_key = |r: &cqlite_core::storage::sstable::reader::CompactionRow| {
        (r.key.0.to_vec(), r.row_timestamp)
    };
    let mut union_sorted = union_rows.clone();
    union_sorted.sort_by_key(sort_key);
    let mut source_sorted = source_rows.clone();
    source_sorted.sort_by_key(sort_key);
    // A named (key, row_timestamp) set-difference BEFORE the full struct
    // comparison: a bare `assert_eq!` on the whole `Vec` prints both
    // multi-hundred-row vectors in full on any mismatch, which is how the
    // `pre_seed_encoding_baselines` omission this test caught (every
    // part's writer defaulted its timestamp-delta baseline instead of
    // inheriting the source's own Statistics.db, silently CORRUPTING —
    // not merely losing — any row whose real timestamp undercut that
    // default) took real effort to pin down. This block fails with the
    // exact offending (key, timestamp) pairs named instead.
    {
        use std::collections::BTreeSet;
        let u_ts: BTreeSet<(Vec<u8>, i64)> = union_sorted.iter().map(sort_key).collect();
        let s_ts: BTreeSet<(Vec<u8>, i64)> = source_sorted.iter().map(sort_key).collect();
        let only_union: Vec<_> = u_ts.difference(&s_ts).cloned().collect();
        let only_source: Vec<_> = s_ts.difference(&u_ts).cloned().collect();
        assert!(
            only_union.is_empty() && only_source.is_empty(),
            "(key,row_timestamp) set diff -- only in union: {only_union:?}; only in source: \
             {only_source:?}"
        );
    }
    assert_eq!(
        union_sorted, source_sorted,
        "the union's rows must be dump-for-dump equal to the source's, once each"
    );
}
