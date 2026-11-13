//! Issue #4193, requirement R1 — untraced merges compile to today's code and
//! cost nothing measurable (design.md §D2, spec `reconcile-decision-trail`
//! R1.2 "structural zero-cost assertions").
//!
//! This is the STRUCTURAL half of R1: `size_of::<NoTrace>() == 0`, every
//! production constructor still type-checks as `KWayMerger<NoTrace>` (so no
//! existing call site needed to change to add the trace sink), and
//! `size_of::<MergeEntry>()` is pinned so a future change that widens the
//! hot-path egress type is caught here rather than only in a benchmark. R1.1
//! (the perf-gate bench thresholds) is the RUNTIME half and lives in
//! `.github/workflows/perf-regression.yml` — this file cannot substitute for
//! that, it only proves the mechanism is capable of costing nothing.

#![cfg(feature = "write-support")]

use std::mem::size_of;

use cqlite_core::storage::write_engine::merge::trace::NoTrace;
use cqlite_core::storage::write_engine::merge::{KWayMerger, MergeEntry};

/// A zero-sized type has no fields to initialize/read/drop, so every `NoTrace`
/// method call ([`cqlite_core::storage::write_engine::merge::trace::TraceSink`]'s
/// `cell`/`tombstone`/`generation_probe`, each an empty `#[inline]` body per
/// design.md §D2) compiles to nothing observable at the type level. This does
/// not itself prove the CALL SITES are optimized away (that's R1.1's job); it
/// proves the type carries no state that could force one.
#[test]
fn no_trace_is_zero_sized() {
    assert_eq!(
        size_of::<NoTrace>(),
        0,
        "NoTrace must stay a zero-sized type — a field added here would make \
         every untraced KWayMerger pay for a sink no caller asked for"
    );
}

/// Accepts only `&KWayMerger<NoTrace>` — a compile-time assertion, not a
/// runtime one. If any constructor below stopped returning `KWayMerger<NoTrace>`
/// (e.g. because a generic `S` leaked into its signature), this file fails to
/// compile, which is the point: R1 requires every EXISTING call site to keep
/// compiling unchanged.
fn assert_untraced(_merger: &KWayMerger<NoTrace>) {}

/// `KWayMerger::new`/`new_cancellable`/`new_with_gc`/`new_with_gc_and_registry`
/// (`mod.rs`) and `new_with_gc_and_registry_cancellable` (`constructors.rs`)
/// all return `Result<KWayMerger<NoTrace>>` — proven by constructing each over
/// an empty, guaranteed-to-error input list (no I/O needed: the point of this
/// test is the TYPE each constructor returns, not that it succeeds against
/// real files).
#[test]
fn full_scan_constructors_return_no_trace() {
    use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
    use std::collections::HashMap;

    let schema = TableSchema {
        keyspace: "zero_cost_ks".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![Column {
            name: "id".to_string(),
            data_type: "int".to_string(),
            nullable: false,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    // Every one of these takes an EMPTY `input_paths`, which every constructor
    // in this family accepts without touching the filesystem (the empty-runs
    // guard lives on `from_row_iterators`/`new_from_readers`, not here) —
    // `Self` in each signature already fixes the return type at `KWayMerger`
    // (= `KWayMerger<NoTrace>`, the struct's default type parameter), so a
    // successful construction is sufficient to exercise the type-level claim
    // this test makes.
    if let Ok(merger) = KWayMerger::new(vec![], &schema) {
        assert_untraced(&merger);
    }
    if let Ok(merger) = KWayMerger::new_cancellable(
        vec![],
        &schema,
        cqlite_core::storage::scan_cancel::ScanCancel::default(),
    ) {
        assert_untraced(&merger);
    }
    if let Ok(merger) = KWayMerger::new_with_gc(vec![], &schema, None, None) {
        assert_untraced(&merger);
    }
    if let Ok(merger) = KWayMerger::new_with_gc_and_registry(vec![], &schema, None, None, None) {
        assert_untraced(&merger);
    }
    if let Ok(merger) = KWayMerger::new_with_gc_and_registry_cancellable(
        vec![],
        &schema,
        None,
        None,
        None,
        cqlite_core::storage::scan_cancel::ScanCancel::default(),
    ) {
        assert_untraced(&merger);
    }
}

/// `build_single_partition_merger`/`_with_registry`/`_from_readers` (R1's
/// `build_single_partition_merger*` glob — NOT `_with_trace`, which is
/// explicitly generic over `S` and is exempt by design) all return
/// `Result<Option<KWayMerger<NoTrace>>>`. An empty `paths`/`keys` list is
/// enough to exercise the type without touching the filesystem.
#[test]
fn point_read_builders_return_no_trace() {
    use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
    use std::collections::HashMap;

    let schema = TableSchema {
        keyspace: "zero_cost_ks".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![Column {
            name: "id".to_string(),
            data_type: "int".to_string(),
            nullable: false,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let result = cqlite_core::storage::write_engine::merge::build_single_partition_merger(
        vec![],
        &[],
        &schema,
        cqlite_core::storage::scan_cancel::ScanCancel::default(),
    )
    .expect("an empty candidate list is not itself an error");
    if let Some(merger) = result {
        assert_untraced(&merger);
    }

    let result =
        cqlite_core::storage::write_engine::merge::build_single_partition_merger_with_registry(
            vec![],
            &[],
            &schema,
            None,
            cqlite_core::storage::scan_cancel::ScanCancel::default(),
        )
        .expect("an empty candidate list is not itself an error");
    if let Some(merger) = result {
        assert_untraced(&merger);
    }

    let result =
        cqlite_core::storage::write_engine::merge::build_single_partition_merger_from_readers(
            vec![],
            &[],
            &schema,
            cqlite_core::storage::scan_cancel::ScanCancel::default(),
            cqlite_core::storage::write_engine::PointAccessRecording::Record,
        )
        .expect("an empty candidate list is not itself an error");
    if let Some(merger) = result {
        assert_untraced(&merger);
    }
}

/// `KWayMerger::from_row_iterators` and `KWayMerger::new_from_readers` both
/// return `Result<KWayMerger<NoTrace>>` and both REJECT an empty run/reader
/// list (`Error::InvalidInput`) — so the `Err` arm itself is the type-level
/// proof for this pair: the function signature fixes the `Ok` type as
/// `KWayMerger<NoTrace>` whether or not this particular call succeeds, and
/// there is no zero-argument way to reach the `Ok` arm without real
/// SSTable/iterator inputs this test does not need to construct.
#[test]
fn seeked_and_reader_constructors_are_declared_no_trace() {
    use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
    use std::collections::HashMap;

    let schema = TableSchema {
        keyspace: "zero_cost_ks".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![Column {
            name: "id".to_string(),
            data_type: "int".to_string(),
            nullable: false,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let err = KWayMerger::from_row_iterators(vec![], &schema)
        .expect_err("an empty run list must be rejected, not silently accepted");
    assert!(matches!(err, cqlite_core::Error::InvalidInput(_)));

    let err = KWayMerger::new_from_readers(
        vec![],
        &schema,
        cqlite_core::storage::scan_cancel::ScanCancel::default(),
        None,
    )
    .expect_err("an empty reader list must be rejected, not silently accepted");
    assert!(matches!(err, cqlite_core::Error::InvalidInput(_)));

    // The compile-time proof: a function taking exactly `KWayMerger<NoTrace>`
    // (never `&KWayMerger<NoTrace>`, since we have no instance to hand it)
    // must still name that type for the file to compile.
    fn accepts_no_trace_by_value(_merger: KWayMerger<NoTrace>) {}
    let _ = accepts_no_trace_by_value as fn(KWayMerger<NoTrace>);
}

/// Pins `size_of::<MergeEntry>()` (issue #4193, requirement R1: "`MergeEntry`
/// ... SHALL gain no field"). A value change here means either a legitimate,
/// reviewed size change to `MergeEntry` (update the literal) or a regression
/// that added a trace-only field to the hot-path egress type, which R1
/// forbids — the trace sink is threaded as a SEPARATE type parameter on
/// `KWayMerger<S>`, never as a field on the row type every merge step
/// allocates.
#[test]
fn merge_entry_size_is_pinned() {
    let actual = size_of::<MergeEntry>();
    assert_eq!(
        actual, 288,
        "size_of::<MergeEntry>() changed from the pinned 288 bytes (measured on \
         a 64-bit target, cargo test -p cqlite-core --test \
         issue_4193_trace_sink_zero_cost) to {actual}. If this is a reviewed, \
         intentional change to MergeEntry itself, update this literal. If it \
         grew because of the #4193 trace sink, that is exactly the regression \
         R1 forbids — thread the sink through KWayMerger<S>'s type parameter \
         instead."
    );
}
