//! Unit tests for `find.rs`: the D5 comparison table, key-literal splitting, table-dir parsing.

use super::*;
use cqlite_core::storage::commitlog::Need;
use cqlite_core::storage::sstable::reader::HitDetail;
use cqlite_core::storage::sstable::reader::{ByteLength, CellCoverage};

/// A hit whose cell `a` and liveness marker are at `ts` (so a mutation of exactly those at or
/// below `ts` is provably flushed).
fn hit(ts: i64) -> GenRow {
    hit_with(
        ts,
        CellCoverage {
            cell_timestamps: [("a".to_string(), ts)].into_iter().collect(),
            liveness_timestamp: Some(ts),
            deletion_timestamp: None,
        },
    )
}

fn hit_with(ts: i64, coverage: CellCoverage) -> GenRow {
    GenRow {
        basename: "g-hit".into(),
        outcome: GenOutcome::Probe(GenerationProbe::Hit(HitDetail {
            data_offset: Some(0),
            byte_length: ByteLength::ToEof,
            max_writetime: ts,
            has_partition_deletion: false,
            coverage,
        })),
    }
}

fn cell(column: &str, timestamp: i64) -> Result<Vec<Need>, &'static str> {
    Ok(vec![Need::Cell {
        column: column.into(),
        timestamp,
    }])
}

fn miss() -> GenRow {
    GenRow {
        basename: "g-miss".into(),
        outcome: GenOutcome::Probe(GenerationProbe::IndexMiss),
    }
}

fn bad() -> GenRow {
    GenRow {
        basename: "g-bad".into(),
        outcome: GenOutcome::Unreadable("open failed".into()),
    }
}

#[test]
fn d5_unmeasured_mutation_propagates_its_cause() {
    let u = unflushed(
        Writetime::Unmeasured("clustered table not decoded"),
        cell("a", 5),
        &[hit(5)],
    );
    assert_eq!(
        u,
        Unflushed::Unmeasured("clustered table not decoded".into())
    );
}

#[test]
fn d5_at_or_below_max_is_no_above_is_yes_absent_everywhere_is_yes() {
    assert_eq!(
        unflushed(Writetime::Known(5), cell("a", 5), &[hit(5), miss()]),
        Unflushed::No
    );
    assert_eq!(
        unflushed(Writetime::Known(4), cell("a", 4), &[hit(5)]),
        Unflushed::No
    );
    assert_eq!(
        unflushed(Writetime::Known(6), cell("a", 6), &[hit(5)]),
        Unflushed::Yes
    );
    assert_eq!(
        unflushed(Writetime::Known(1), cell("a", 1), &[miss(), miss()]),
        Unflushed::Yes
    );
}

/// Roborev job 299 M1: the SSTables hold column `a` at ts 100; a CommitLog-only write of column
/// `b` at ts 90 is at or below the partition max but NOT covered, so it is `Unmeasured`, never
/// a guessed "flushed". Minimal synthetic case (no Cassandra fixture has a partial-column
/// shape): the coverage struct is built by hand in the shape `probe_generation` produces.
#[test]
fn d5_a_lower_timestamp_other_column_is_not_proven_flushed() {
    let sst = hit_with(
        100,
        CellCoverage {
            cell_timestamps: [("a".to_string(), 100)].into_iter().collect(),
            liveness_timestamp: None,
            deletion_timestamp: None,
        },
    );
    assert_eq!(
        unflushed(Writetime::Known(90), cell("b", 90), &[sst.clone()]),
        Unflushed::Unmeasured(
            "mutation coverage not proven by SSTable cells: column b @ 90".into()
        )
    );
    // Same column, lower timestamp: proven (the SSTable cell wins reconciliation).
    assert_eq!(
        unflushed(Writetime::Known(90), cell("a", 90), &[sst.clone()]),
        Unflushed::No
    );
    // Same column but the mutation's cell is NEWER than the SSTable's: not proven either.
    assert_eq!(
        unflushed(
            Writetime::Known(100),
            Ok(vec![
                Need::Cell {
                    column: "a".into(),
                    timestamp: 100
                },
                Need::Liveness(100)
            ]),
            &[sst.clone()]
        ),
        Unflushed::Unmeasured(
            "mutation coverage not proven by SSTable cells: row liveness marker @ 100".into()
        )
    );
    // A covering deletion at or above the mutation proves it shadowed.
    let shadow = hit_with(
        100,
        CellCoverage {
            deletion_timestamp: Some(95),
            ..CellCoverage::default()
        },
    );
    assert_eq!(
        unflushed(Writetime::Known(90), cell("b", 90), &[shadow]),
        Unflushed::No
    );
    // An undecodable shape names its cause.
    assert_eq!(
        unflushed(
            Writetime::Known(90),
            Err("a cell carries no timestamp"),
            &[sst]
        ),
        Unflushed::Unmeasured(
            "mutation coverage not proven by SSTable cells: a cell carries no timestamp".into()
        )
    );
}

#[test]
fn d5_an_unreadable_generation_blocks_yes_but_not_a_proven_no() {
    assert_eq!(
        unflushed(Writetime::Known(4), cell("a", 4), &[hit(5), bad()]),
        Unflushed::No
    );
    assert_eq!(
        unflushed(Writetime::Known(6), cell("a", 6), &[hit(5), bad()]),
        Unflushed::Unmeasured("generation g-bad could not be probed".into())
    );
    assert_eq!(
        unflushed(Writetime::Known(1), cell("a", 1), &[bad()]),
        Unflushed::Unmeasured("generation g-bad could not be probed".into())
    );
}

#[test]
fn exit_code_is_2_for_unreadable_generation_segment_or_torn_tail() {
    let ok = FindReport {
        generations: vec![miss()],
        commitlog: None,
    };
    assert_eq!(ok.exit_code(), 0);
    let g = FindReport {
        generations: vec![bad()],
        commitlog: None,
    };
    assert_eq!(g.exit_code(), 2);
    let s = FindReport {
        generations: vec![miss()],
        commitlog: Some(vec![CommitEntry::SegmentFailure {
            segment: "x.log".into(),
            cause: "c".into(),
        }]),
    };
    assert_eq!(s.exit_code(), 2);
    let t = FindReport {
        generations: vec![miss()],
        commitlog: Some(vec![CommitEntry::SegmentTruncated {
            segment: "x.log".into(),
        }]),
    };
    assert_eq!(t.exit_code(), 2, "a torn tail hides records: fail closed");
}

#[test]
fn key_literals_split_on_top_level_commas_only() {
    assert_eq!(split_key_literals("42"), vec!["42"]);
    assert_eq!(split_key_literals("'a,b', 7"), vec!["'a,b'", "7"]);
    assert_eq!(split_key_literals("'it''s',2"), vec!["'it''s'", "2"]);
}

#[test]
fn table_dir_name_yields_table_and_dashed_id() {
    let (t, id) = parse_table_dir_name(Path::new("/x/ks/users-dc2d1cf0dcc811f1be1b6d4cbb83f4d8"))
        .expect("parses");
    assert_eq!(t, "users");
    assert_eq!(id, "dc2d1cf0-dcc8-11f1-be1b-6d4cbb83f4d8");
    assert!(parse_table_dir_name(Path::new("/x/users")).is_none());
    assert!(parse_table_dir_name(Path::new("/x/users-nothex")).is_none());
}

#[test]
fn key_literals_respect_tuple_collection_map_and_udt_nesting() {
    assert_eq!(split_key_literals("(1, 'a'), 7"), vec!["(1, 'a')", "7"]);
    assert_eq!(
        split_key_literals("[1, 2, 3], 'x'"),
        vec!["[1, 2, 3]", "'x'"]
    );
    assert_eq!(
        split_key_literals("{1: 'a', 2: 'b'}, 3"),
        vec!["{1: 'a', 2: 'b'}", "3"]
    );
    assert_eq!(
        split_key_literals("{street: 'a,b', zip: 1}, [(1,2),(3,4)]"),
        vec!["{street: 'a,b', zip: 1}", "[(1,2),(3,4)]"]
    );
    assert_eq!(split_key_literals("\"a,b\", 2"), vec!["\"a,b\"", "2"]);
    assert_eq!(split_key_literals("42"), vec!["42"]);
}

fn schema_for(cql: &str) -> TableSchema {
    cqlite_core::schema::parse_cql_schema(cql).unwrap_or_else(|e| panic!("{cql}: {e}"))
}

#[test]
fn key_encoding_does_not_depend_on_the_partition_key_column_name() {
    // The CQL schema parser cannot itself read every identifier (e.g. an embedded `""`), so
    // parse a plain schema and RENAME the key column to the raw, unquoted names a quoted
    // identifier denotes: reserved word, space, embedded double quote, mixed case, and a name
    // that would inject CQL if it were interpolated into the synthetic query.
    for name in [
        "select",
        "my col",
        "a\"b",
        "MixedCase",
        "k = 1 OR x",
        "'; DROP",
    ] {
        let mut schema = schema_for("CREATE TABLE ks.t (k int PRIMARY KEY, v text);");
        schema.partition_keys[0].name = name.to_string();
        let got = encode_partition_key(&schema, "42")
            .unwrap_or_else(|e| panic!("partition key column {name:?}: {e:#}"));
        assert_eq!(got, 42i32.to_be_bytes().to_vec(), "column {name:?}");
    }
    // Composite key: both literals are encoded whatever the columns are called.
    let mut schema = schema_for("CREATE TABLE ks.t (a int, b text, v int, PRIMARY KEY ((a, b)));");
    schema.partition_keys[0].name = "a b".into();
    schema.partition_keys[1].name = "sel\"ect".into();
    assert!(encode_partition_key(&schema, "1, 'x'").is_ok());
}
