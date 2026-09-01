//! Unit tests for the single-source merge-bypass predicate and row source
//! (issue #3058), split out of `bypass.rs` to keep that file under the campsite
//! file-size target (epic #1116).

use super::*;

/// The override parse is total and case-insensitive, and an unrecognized
/// value degrades to automatic rather than to a surprising arm.
#[test]
fn forced_path_parse_is_total() {
    assert_eq!(ForcedMergePath::parse(None), ForcedMergePath::Auto);
    assert_eq!(ForcedMergePath::parse(Some("")), ForcedMergePath::Auto);
    assert_eq!(
        ForcedMergePath::parse(Some("nonsense")),
        ForcedMergePath::Auto
    );
    assert_eq!(
        ForcedMergePath::parse(Some("bypass")),
        ForcedMergePath::Bypass
    );
    assert_eq!(
        ForcedMergePath::parse(Some(" BYPASS ")),
        ForcedMergePath::Bypass
    );
    assert_eq!(
        ForcedMergePath::parse(Some("merge")),
        ForcedMergePath::Merge
    );
    assert_eq!(
        ForcedMergePath::parse(Some("Merge")),
        ForcedMergePath::Merge
    );
}

/// A zero-source set is not a single source (the caller returns before this,
/// but the predicate must be total and fail closed).
#[test]
fn empty_reader_set_is_not_a_single_source() {
    let schema = crate::testutil::simple_schema();
    assert_eq!(
        bypass_reason(&[], &schema, ForcedMergePath::Auto, false, None),
        BypassReason::MultipleSources
    );
}

/// An aggregating request never selects the fast path, even with one source
/// (belt-and-braces: the aggregate route returns earlier still).
#[test]
fn aggregating_request_never_selects_the_fast_path() {
    let schema = crate::testutil::simple_schema();
    assert_eq!(
        bypass_reason(&[], &schema, ForcedMergePath::Auto, true, None),
        BypassReason::Aggregating
    );
}

/// Issue #3095: a DECLARED static column does not force the merge arm — and, since the
/// #3140 deletion guard was RETIRED, nothing else about this file does either.
///
/// The fixture is the sharp case for that retirement. CQLite's own `Statistics.db`
/// writer does not emit Cassandra's `EncodingStats.DELETION_TIME_EPOCH` "no deletion
/// recorded" sentinel, so every CQLite-written file used to report
/// `may_contain_deletions() == true` and every static-bearing CQLite fixture therefore
/// tripped the guard (`BypassReason::StaticColumnsWithDeletions`). With the fast arm's
/// cell-tombstone handling fixed at its source (PR #3122,
/// `row_decoder`'s `PartitionShadow::cell_tombstone_dropped`) that guard is gone, so
/// this same fixture is now SELECTED.
///
/// The Cassandra-bytes half is `issue_3095_flight_static_columns.rs`, where
/// `test_deltas.static_with_rows`, `test_writeparity.static_clustering_shape` AND the
/// deletion-bearing `test_tomb.static_with_tombstones` all take the bypass.
#[test]
fn a_declared_static_column_no_longer_forces_the_merge_arm() {
    use crate::testutil::{simple_schema, write_row};
    let (_temp, readers) = open_readers(vec![vec![write_row(1, "a", 10, 100)]]);
    let mut schema = simple_schema();
    assert_eq!(
        bypass_reason(&readers, &schema, ForcedMergePath::Auto, false, None),
        BypassReason::Selected,
        "control: without the static column this request takes the fast path"
    );
    if let Some(c) = schema.columns.iter_mut().find(|c| c.name == "name") {
        c.is_static = true;
    }
    assert_eq!(
        bypass_reason(&readers, &schema, ForcedMergePath::Auto, false, None),
        BypassReason::Selected,
        "issue #3095: a static column the schema DECLARES is not refused for BEING \
         static; issue #3140: nor is the file refused for declaring a deletion"
    );
}

/// Open ONE real reader over a single-SSTable fixture, so the predicate is
/// exercised against genuine reader metadata rather than a stub.
fn open_readers(
    batches: Vec<Vec<cqlite_core::storage::write_engine::Mutation>>,
) -> (tempfile::TempDir, Vec<Arc<SSTableReader>>) {
    use crate::testutil::{build_sstables, simple_schema};
    let schema = simple_schema();
    let (temp, _data, table_dir) = build_sstables(&schema, batches);
    let mut data_dbs: Vec<std::path::PathBuf> = std::fs::read_dir(&table_dir)
        .expect("table dir")
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
        .collect();
    data_dbs.sort();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let readers = rt.block_on(async {
        let config = cqlite_core::Config::default();
        let platform = Arc::new(cqlite_core::Platform::new(&config).await.expect("platform"));
        let mut out = Vec::new();
        for p in data_dbs {
            out.push(Arc::new(
                SSTableReader::open(&p, &config, platform.clone())
                    .await
                    .expect("reader opens"),
            ));
        }
        out
    });
    (temp, readers)
}

/// Spec R1: exactly ONE post-prune source, an empty `dropped_columns`, no
/// aggregation and no forced merge → the fast path is selected.
#[test]
fn one_source_with_a_clean_schema_selects_the_fast_path() {
    use crate::testutil::{simple_schema, write_row};
    let (_temp, readers) = open_readers(vec![vec![write_row(1, "a", 10, 100)]]);
    assert_eq!(readers.len(), 1, "the fixture is exactly one generation");
    assert_eq!(
        bypass_reason(
            &readers,
            &simple_schema(),
            ForcedMergePath::Auto,
            false,
            None
        ),
        BypassReason::Selected
    );
}

/// Spec R1: two post-prune sources take the merge arm.
#[test]
fn two_sources_take_the_merge_arm() {
    use crate::testutil::{simple_schema, write_row};
    let (_temp, readers) = open_readers(vec![
        vec![write_row(1, "a", 10, 100)],
        vec![write_row(2, "b", 20, 200)],
    ]);
    assert_eq!(readers.len(), 2, "the fixture is two generations");
    assert_eq!(
        bypass_reason(
            &readers,
            &simple_schema(),
            ForcedMergePath::Auto,
            false,
            None
        ),
        BypassReason::MultipleSources
    );
}

/// Spec R1: a non-empty `dropped_columns` map takes the merge arm, so the
/// reconciler's timestamp-based dropped-column purge (Step 3b) still runs.
#[test]
fn a_non_empty_dropped_columns_map_takes_the_merge_arm() {
    use crate::testutil::{simple_schema, write_row};
    let (_temp, readers) = open_readers(vec![vec![write_row(1, "a", 10, 100)]]);
    let mut schema = simple_schema();
    schema
        .dropped_columns
        .insert("gone".to_string(), 1_700_000_000_000_000);
    assert_eq!(
        bypass_reason(&readers, &schema, ForcedMergePath::Auto, false, None),
        BypassReason::DroppedColumns
    );
}

/// Spec R1: even under a forced `bypass`, a correctness precondition still
/// wins — the override can never make the fast path serve a 2-source table.
#[test]
fn forced_bypass_never_overrides_a_correctness_precondition() {
    use crate::testutil::{simple_schema, write_row};
    let (_temp, readers) = open_readers(vec![
        vec![write_row(1, "a", 10, 100)],
        vec![write_row(2, "b", 20, 200)],
    ]);
    assert_eq!(
        bypass_reason(
            &readers,
            &simple_schema(),
            ForcedMergePath::Bypass,
            false,
            None
        ),
        BypassReason::MultipleSources
    );
}

/// Roborev BLOCKER (issue #3058): opening the fast-path source and then
/// dropping it — exactly what the `Unsupported` fallback does before handing
/// the request to the k-way merger — must leave the CALLER's cancellation
/// flag UN-cancelled. `ScanCancel` clones share one `Arc<AtomicBool>`, so a
/// stream that cancelled the caller's clone on drop would poison the very
/// fallback it exists to enable (the merger would be built pre-cancelled and
/// return `Cancelled`/zero rows) and would make the request's `CancelFlag`
/// single-use even on the success path.
#[test]
fn dropping_the_scan_source_does_not_poison_the_callers_cancel() {
    use crate::producer::MergeProducer;
    use crate::testutil::{simple_schema, total_rows, write_row};
    let (_temp, readers) = open_readers(vec![vec![
        write_row(1, "a", 10, 100),
        write_row(2, "b", 20, 100),
    ]]);
    let schema = simple_schema();
    let cancel = crate::cancel::CancelFlag::new();

    let source = ScanRowSource::open(
        Arc::clone(&readers[0]),
        schema.clone(),
        None,
        1_700_000_000,
        cancel.scan_cancel(),
    )
    .expect("the source opens");
    assert!(
        source.is_some(),
        "this fixture IS servable by the fast path"
    );
    drop(source);

    assert!(
        !cancel.is_cancelled(),
        "dropping the fast-path source must not cancel the request"
    );
    assert!(
        !cancel.scan_cancel().is_cancelled(),
        "…including the shared synchronous ScanCancel the merger polls"
    );

    // The fallback the blocker is about: with that SAME flag, the merge arm
    // must still return the FULL row set (pre-fix it returned zero rows).
    let producer = MergeProducer::new(schema, 1024).expect("producer");
    let batches = producer
        .produce_streaming_from_readers_to_vec(readers, &cancel)
        .expect("the merge arm runs with a non-poisoned flag");
    assert_eq!(
        total_rows(&batches),
        2,
        "the merge arm returns every row after a fast-path source was dropped"
    );
}

/// Spec R1 (roborev, issue #3058): a non-frozen collection whose element/key the
/// two arms do NOT collapse identically takes the MERGE arm, so `SELECT *` cannot
/// return one thing at one generation and another at two. A `list<frozen<udt>>` is
/// NOT affected — its cell path is a position TimeUUID, and the merge arm serves
/// it.
///
/// With NO `UdtScope` (this case) every composite is refused, because the merge
/// arm resolves UDT references through the ticket registry and cannot decode a
/// bare `Custom` — the fail-closed direction. Issue #2339's registry-aware
/// narrowing (a RESOLVABLE composite SET element is served by both arms) is
/// pinned separately by
/// [`a_resolvable_composite_set_element_selects_the_fast_arm`].
#[test]
fn a_composite_keyed_collection_forces_the_merge_arm() {
    use crate::testutil::{simple_schema, write_row};
    let (_temp, readers) = open_readers(vec![vec![write_row(1, "a", 10, 100)]]);
    let base = simple_schema();
    assert_eq!(
        bypass_reason(&readers, &base, ForcedMergePath::Auto, false, None),
        BypassReason::Selected,
        "control: the plain schema WOULD take the fast path"
    );

    for refused in [
        "set<frozen<contact_info>>",
        "map<frozen<contact_info>, text>",
        "map<frozen<tuple<int, text>>, text>",
        // A composite SET element with NO registry scope: the merge arm cannot
        // resolve it, so the fast arm is refused (issue #2339).
        "set<frozen<tuple<int, text>>>",
        "set<frozen<list<int>>>",
        // Case-insensitive parse: this is refused by the `Set` arm, exactly
        // like its lowercase spelling.
        "SET<FROZEN<CONTACT_INFO>>",
        // A BARE (non-frozen) UDT is MULTI-CELL: the merge arm's
        // `assemble_complex` `_` fall-through keeps only the last element's
        // scalar while the fast arm builds the whole `Value::Udt`
        // (#927/#1081) — the divergence class this guard exists for.
        "contact_info",
        "tuple<int, text>",
    ] {
        let mut schema = base.clone();
        if let Some(c) = schema.columns.iter_mut().find(|c| c.name == "name") {
            c.data_type = refused.to_string();
        }
        assert_eq!(
            bypass_reason(&readers, &schema, ForcedMergePath::Auto, false, None),
            BypassReason::MulticellArmDivergence,
            "`{refused}` must take the merge arm"
        );
    }

    for allowed in [
        "list<frozen<address_type>>",
        "set<text>",
        "map<text, frozen<contact_info>>",
        "frozen<set<frozen<contact_info>>>",
        "set<inet>",
        "int",
        // A FROZEN UDT is ONE cell — it never reaches the multi-element
        // collapse, so both arms serve it identically.
        "frozen<contact_info>",
        "frozen<tuple<int, text>>",
    ] {
        let mut schema = base.clone();
        if let Some(c) = schema.columns.iter_mut().find(|c| c.name == "name") {
            c.data_type = allowed.to_string();
        }
        assert_eq!(
            bypass_reason(&readers, &schema, ForcedMergePath::Auto, false, None),
            BypassReason::Selected,
            "`{allowed}` is served identically by both arms and must stay on \
             the fast path"
        );
    }
}

/// Issue #2339: the composite-SET-element clause is REGISTRY-AWARE.
///
/// Both arms decode a composite set element structurally now, but they resolve
/// the element TYPE from different places: the merge arm from the ticket DDL's
/// `UdtScope`, the single-generation decoder from the SSTable's OWN marshal type.
/// So the fast arm is safe only when the scope can resolve it — otherwise the
/// merge arm fails closed while the fast arm succeeds, which is exactly the
/// arm-dependent outcome the guard exists to prevent.
///
/// A composite MAP KEY stays refused whatever the scope: the divergence merely
/// swapped sides (the merge arm decodes it; `parse_cell_path_key` in the
/// single-generation decoder has no composite arm and falls back to an opaque
/// `Value::Blob`).
#[test]
fn a_resolvable_composite_set_element_selects_the_fast_arm() {
    use crate::testutil::{simple_schema, write_row};
    use cqlite_core::schema::udt_registry_from_cql;
    use cqlite_core::storage::write_engine::merge::UdtScope;

    let (_temp, readers) = open_readers(vec![vec![write_row(1, "a", 10, 100)]]);
    let base = simple_schema();
    let registry = udt_registry_from_cql(
        "CREATE TYPE contact_info (email text, phone text);",
        &base.keyspace,
    );
    let resolving = Some(UdtScope {
        registry: &registry,
        keyspace: &base.keyspace,
    });
    // A scope whose KEYSPACE does not match the one the registry was built under
    // resolves NOTHING — the mismatch #2339's `UdtScope` exists to make explicit.
    let wrong_keyspace = Some(UdtScope {
        registry: &registry,
        keyspace: "some_other_keyspace",
    });

    let with_type = |ty: &str| {
        let mut schema = base.clone();
        if let Some(c) = schema.columns.iter_mut().find(|c| c.name == "name") {
            c.data_type = ty.to_string();
        }
        schema
    };

    let udt_set = with_type("set<frozen<contact_info>>");
    assert_eq!(
        bypass_reason(&readers, &udt_set, ForcedMergePath::Auto, false, resolving),
        BypassReason::Selected,
        "a RESOLVABLE composite set element is decoded by both arms (issue #2339)"
    );
    assert_eq!(
        bypass_reason(&readers, &udt_set, ForcedMergePath::Auto, false, None),
        BypassReason::MulticellArmDivergence,
        "control: without a scope the merge arm cannot resolve it, so refuse"
    );
    assert_eq!(
        bypass_reason(
            &readers,
            &udt_set,
            ForcedMergePath::Auto,
            false,
            wrong_keyspace
        ),
        BypassReason::MulticellArmDivergence,
        "control: a scope keyed on the WRONG keyspace resolves nothing, so refuse"
    );

    // An UNKNOWN UDT name is unresolvable even WITH a registry.
    assert_eq!(
        bypass_reason(
            &readers,
            &with_type("set<frozen<not_registered>>"),
            ForcedMergePath::Auto,
            false,
            resolving
        ),
        BypassReason::MulticellArmDivergence,
        "an unresolvable composite set element must still take the merge arm"
    );

    // A nested frozen COLLECTION element needs no registry at all — it carries its
    // own structure — so it is served by both arms even with no scope. Pinned
    // end-to-end on Cassandra bytes by `issue_3058_forced_path_differential.rs`'s
    // `cx_nested_frozen_collections` case.
    for structural in ["set<frozen<list<int>>>", "set<frozen<map<text,int>>>"] {
        assert_eq!(
            bypass_reason(
                &readers,
                &with_type(structural),
                ForcedMergePath::Auto,
                false,
                resolving
            ),
            BypassReason::Selected,
            "`{structural}` carries its own structure and is served by both arms"
        );
    }

    // A composite MAP KEY is refused regardless of the scope.
    for map_key in [
        "map<frozen<contact_info>, text>",
        "map<frozen<tuple<int, text>>, text>",
    ] {
        assert_eq!(
            bypass_reason(
                &readers,
                &with_type(map_key),
                ForcedMergePath::Auto,
                false,
                resolving
            ),
            BypassReason::MulticellArmDivergence,
            "`{map_key}`: the single-generation decoder serves a composite map key as \
             an opaque Blob, so the arms still diverge"
        );
    }
}

/// Roborev F2 (issue #2339): the bypass predicate's "the merge arm can resolve
/// this" answer and the merge arm's ACTUAL behaviour must agree — including for a
/// keyspace-QUALIFIED UDT reference (`set<frozen<ks.contact_info>>`), which is how
/// Cassandra emits a UDT column type and what the CQL parser retains.
///
/// The predicate resolves with `UdtRegistry::resolve_type`, which is
/// qualifier-aware; the merge arm built its comparator with
/// `ComparatorType::from_cql_type_with_registry`, whose `Custom` arm looks the
/// reference up by BARE name only. So for a qualified reference the predicate
/// selected the single-generation arm while a MULTI-generation merged read of the
/// same table failed closed — a correctness outcome flipping on SSTable
/// generation count, which is the defect #2339 exists to remove.
///
/// This test drives BOTH sides for real (the predicate, and
/// `assemble_read_cells` on a Cassandra-framed `cell_path`) and asserts they
/// agree, so a future divergence between the two resolvers reds here rather than
/// only under a two-generation table.
#[test]
fn the_bypass_predicate_and_the_merge_arm_agree_on_a_qualified_udt_reference() {
    use crate::testutil::{simple_schema, write_row};
    use cqlite_core::schema::udt_registry_from_cql;
    use cqlite_core::storage::write_engine::merge::{assemble_read_cells, CellData, UdtScope};
    use cqlite_core::Value;

    let (_temp, readers) = open_readers(vec![vec![write_row(1, "a", 10, 100)]]);
    let base = simple_schema();
    let registry = udt_registry_from_cql(
        "CREATE TYPE contact_info (email text, phone text);",
        &base.keyspace,
    );
    let scope = || {
        Some(UdtScope {
            registry: &registry,
            keyspace: &base.keyspace,
        })
    };

    /// `contact_info { email: "a@b", phone: "1" }` in Cassandra's frozen-UDT
    /// framing: an i32-BE length per field (`TupleType.buildValue`, pinned
    /// `cassandra-5.0.8`; `UserType extends TupleType`).
    const CONTACT_PATH: &[u8] = &[
        0, 0, 0, 3, b'a', b'@', b'b', // email "a@b"
        0, 0, 0, 1, b'1', // phone "1"
    ];

    let with_type = |ty: &str| {
        let mut schema = base.clone();
        if let Some(c) = schema.columns.iter_mut().find(|c| c.name == "name") {
            c.data_type = ty.to_string();
        }
        schema
    };

    // (declared type, must BOTH arms serve it?)
    let cases = [
        ("set<frozen<contact_info>>", true),
        // The SAME type by a keyspace-qualified reference.
        (
            &format!("set<frozen<{}.contact_info>>", base.keyspace)[..],
            true,
        ),
        // Control: a name in no registry — neither arm may claim it.
        ("set<frozen<not_registered>>", false),
    ];

    for (declared, expect_served) in cases {
        let schema = with_type(declared);
        let predicate_selects =
            bypass_reason(&readers, &schema, ForcedMergePath::Auto, false, scope())
                == BypassReason::Selected;

        let element = CellData {
            column: "name".into(),
            value: Value::blob(Vec::new()),
            timestamp: 1,
            ttl: None,
            cell_path: Some(CONTACT_PATH.to_vec()),
            local_deletion_time: None,
            is_complex_element: true,
            is_deleted: false,
            has_empty_value: false,
        };
        let merge_arm_decodes = assemble_read_cells(vec![element], &schema, None, scope()).is_ok();

        assert_eq!(
            predicate_selects, merge_arm_decodes,
            "`{declared}`: the bypass predicate says resolvable={predicate_selects} while \
             the merged-read arm decodes={merge_arm_decodes} — a disagreement makes \
             correctness depend on SSTable generation count (issue #2339 F2)"
        );
        assert_eq!(
            merge_arm_decodes, expect_served,
            "`{declared}`: expected both arms to serve it = {expect_served}"
        );
    }
}

/// Roborev (issue #3058): the ONE accounting difference between the arms is
/// the documented one — a fully-suppressed partition is counted as scanned by
/// the merge arm (it arrives as `StreamingStep::PartitionEnd`) and not by the
/// fast arm (the walk emits only surviving rows, so the source never learns
/// the partition existed; see `SourceStep::PartitionEnd`'s doc). Everything
/// else must match: the emitted rows AND the examined-row progress counter.
///
/// Drives both arms DIRECTLY (no env mutation) so this cannot race a sibling
/// lib test.
#[test]
fn progress_accounting_difference_between_the_arms_is_the_documented_one() {
    use crate::producer::{CollectSink, MergeProducer};
    use crate::scan_progress::ScanProgress;
    use crate::testutil::{simple_schema, total_rows, write_row};
    use cqlite_core::storage::write_engine::KWayMerger;

    // pk=1 survives; pk=2 is written and then row-deleted in the SAME
    // generation, so it is a partition that exists on disk and yields NO row.
    let schema = simple_schema();
    let (_temp, readers) = open_readers(vec![vec![
        write_row(1, "a", 10, 100),
        crate::testutil::write_row(2, "gone", 20, 100),
        crate::testutil::delete_row(2, 200),
    ]]);
    let producer = MergeProducer::new(schema.clone(), 1024).expect("producer");
    let cancel = crate::cancel::CancelFlag::new();

    // FAST arm, driven directly.
    let fast_progress = ScanProgress::default();
    let mut fast_batches = Vec::new();
    {
        let mut source = ScanRowSource::open(
            Arc::clone(&readers[0]),
            schema.clone(),
            None,
            1_700_000_000,
            cancel.scan_cancel(),
        )
        .expect("source opens")
        .expect("this fixture is servable by the fast path");
        let mut sink = CollectSink(&mut fast_batches);
        producer
            .drive_row_source(
                &mut source,
                &cancel,
                &mut sink,
                &fast_progress,
                cqlite_core::query::AccessPath::FullScan.label(),
            )
            .expect("fast arm drives");
    }

    // MERGE arm, driven directly over the SAME reader.
    let merge_progress = ScanProgress::default();
    let mut merge_batches = Vec::new();
    {
        let mut merger =
            KWayMerger::new_from_readers(readers.clone(), &schema, cancel.scan_cancel(), None)
                .expect("merger builds")
                .with_now_secs(Some(1_700_000_000));
        let mut sink = CollectSink(&mut merge_batches);
        producer
            .drive_merge_over(
                &mut merger,
                &cancel,
                &mut sink,
                &merge_progress,
                cqlite_core::query::AccessPath::FullScan.label(),
            )
            .expect("merge arm drives");
    }

    assert_eq!(
        total_rows(&fast_batches),
        total_rows(&merge_batches),
        "the suppressed partition must not surface on EITHER arm, and the \
         surviving row must surface on both"
    );
    assert_eq!(
        total_rows(&fast_batches),
        1,
        "exactly the one live partition's row survives"
    );
    assert_eq!(
        fast_progress.flushed_rows(),
        merge_progress.flushed_rows(),
        "the EXAMINED-ROW progress counter must be arm-invariant: a suppressed \
         partition materializes no row on either arm"
    );
}

/// `merge` wins over everything, including an aggregating request — it is the
/// field kill switch and must be absolute.
#[test]
fn forced_merge_is_absolute() {
    let schema = crate::testutil::simple_schema();
    assert_eq!(
        bypass_reason(&[], &schema, ForcedMergePath::Merge, false, None),
        BypassReason::ForcedMerge
    );
}
