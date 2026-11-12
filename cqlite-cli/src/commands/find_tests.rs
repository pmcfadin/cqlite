//! Unit tests for `find.rs`: the D5 comparison table, key-literal splitting, table-dir parsing.

use super::*;
use cqlite_core::storage::sstable::reader::ByteLength;
use cqlite_core::storage::sstable::reader::HitDetail;

fn hit(ts: i64) -> GenRow {
    GenRow {
        basename: "g-hit".into(),
        outcome: GenOutcome::Probe(GenerationProbe::Hit(HitDetail {
            data_offset: Some(0),
            byte_length: ByteLength::ToEof,
            max_writetime: ts,
            has_partition_deletion: false,
        })),
    }
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
        unflushed(Writetime::Known(5), &[hit(5), miss()]),
        Unflushed::No
    );
    assert_eq!(unflushed(Writetime::Known(4), &[hit(5)]), Unflushed::No);
    assert_eq!(unflushed(Writetime::Known(6), &[hit(5)]), Unflushed::Yes);
    assert_eq!(
        unflushed(Writetime::Known(1), &[miss(), miss()]),
        Unflushed::Yes
    );
}

#[test]
fn d5_an_unreadable_generation_blocks_yes_but_not_a_proven_no() {
    assert_eq!(
        unflushed(Writetime::Known(4), &[hit(5), bad()]),
        Unflushed::No
    );
    assert_eq!(
        unflushed(Writetime::Known(6), &[hit(5), bad()]),
        Unflushed::Unmeasured("generation g-bad could not be probed".into())
    );
    assert_eq!(
        unflushed(Writetime::Known(1), &[bad()]),
        Unflushed::Unmeasured("generation g-bad could not be probed".into())
    );
}

#[test]
fn exit_code_is_2_only_for_unreadable_generation_or_segment() {
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
    assert_eq!(t.exit_code(), 0);
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
