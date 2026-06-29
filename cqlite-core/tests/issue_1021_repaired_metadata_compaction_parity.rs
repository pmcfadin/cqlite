//! Repair-metadata compaction parity (issue #1021, parent epic #973).
//!
//! Proves CQLite PRESERVES the persisted repair state (`repairedAt`,
//! `pendingRepair`, `isTransient`) through compaction when the inputs are
//! COMPATIBLE (same repair state), and REJECTS a compaction that mixes
//! repaired / unrepaired / pending-repair inputs — Apache Cassandra partitions
//! compaction candidates by repair state and never mixes them in one compaction
//! (the `CompactionTaskTest` reject-mixed-repair-state expectation). CQLite
//! cannot reproduce Cassandra's repair-boundary tombstone constraints, so a
//! mixed set is rejected with a typed error rather than silently merged.
//!
//! Fixtures: the published Cassandra 5.0 corpus contains NO repaired /
//! pending-repair / transient SSTable (every fixture is unrepaired; producing a
//! repaired fixture requires a live cluster + `nodetool repair` /
//! `sstablerepairedset` / anticompaction, which this environment lacks — see the
//! manifest `partial` reason). These tests therefore CONSTRUCT input SSTables
//! with controlled repair states via the real `SSTableWriter`
//! (`set_repair_state`), which is the same byte-level mechanism the compaction
//! merge path uses to carry repair metadata forward. This is provable,
//! non-fabricated coverage of the repaired / pending / transient DISTINCT
//! states. The read-side decode is validated against the real corpus by
//! `sstable_parity_repaired_metadata_test`.
//!
//! SCOPE: parse + preserve + classify + reject ONLY. Nothing here establishes
//! repair-aware tombstone purging (a separate correctness concern).

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use cqlite_core::parser::repair_metadata::{parse_repair_metadata, RepairField};
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::version_gate::{BigVersionGates, VersionGates};
use cqlite_core::storage::sstable::writer::SSTableWriter;
use cqlite_core::storage::write_engine::merge::{classify_inputs, compact_sstables, RepairState};
use cqlite_core::storage::write_engine::{CellOperation, Mutation, PartitionKey, TableId};
use cqlite_core::types::Value;
use tempfile::TempDir;

/// Minimal single-partition-key, no-clustering schema so the compaction output's
/// covered-clustering Slice is empty and the full repair-state walk succeeds on
/// the output (issue #1021 read-path note).
fn schema() -> TableSchema {
    TableSchema {
        keyspace: "repair_ks".to_string(),
        table: "items".to_string(),
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

fn write_row(id: i32, name: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new("repair_ks", "items"),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        }],
        ts,
        None,
    )
}

/// Write a single `nb`-format input SSTable carrying the given partitions and a
/// chosen repair state. Returns the published `Data.db` path.
fn write_input(
    dir: &Path,
    generation: u64,
    rows: &[(i32, &str, i64)],
    repaired_at: i64,
    pending_repair: Option<[u8; 16]>,
    is_transient: bool,
) -> PathBuf {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let s = schema();
    let mut writer = SSTableWriter::new(dir.to_path_buf(), generation, &s).expect("writer");
    writer.set_repair_state(repaired_at, pending_repair, is_transient);

    for &(id, name, ts) in rows {
        let m = write_row(id, name, ts);
        let key = m.decorated_key(&s).expect("decorated key");
        writer
            .write_partition(key, vec![m])
            .expect("write partition");
    }
    let info = rt.block_on(writer.finish()).expect("finish");
    info.data_path
}

fn nb_gates() -> VersionGates {
    VersionGates::Big(BigVersionGates::from_version("nb").expect("nb gates"))
}

/// Decode the persisted repair state of a published SSTable from its
/// `Statistics.db` sibling, using the full version-gated walk.
fn decode_output_repair_state(
    data_path: &Path,
) -> (i64, RepairField<Option<[u8; 16]>>, RepairField<bool>) {
    let name = data_path.file_name().and_then(|n| n.to_str()).unwrap();
    let stats = data_path.with_file_name(name.replace("Data.db", "Statistics.db"));
    let bytes = std::fs::read(&stats).expect("read output Statistics.db");
    let md = parse_repair_metadata(&bytes, Some(&nb_gates())).expect("decode output repair state");
    (md.repaired_at, md.pending_repair, md.is_transient)
}

/// AC2: compacting COMPATIBLE inputs (identical repair state) preserves the
/// repairedAt / pendingRepair / isTransient into the merged output Statistics.db.
#[test]
fn compaction_preserves_shared_repaired_state() {
    let temp = TempDir::new().expect("tempdir");
    let in_dir = temp.path().join("inputs");
    let out_dir = temp.path().join("out");
    std::fs::create_dir_all(&in_dir).unwrap();

    let repaired_at: i64 = 1_700_000_000_000;
    let pending: [u8; 16] = [
        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc,
        0xfe,
    ];

    // Two inputs in the SAME repair state (repaired + pending + transient).
    let a = write_input(
        &in_dir.join("a"),
        1,
        &[(1, "a-1", 100), (2, "a-2", 100)],
        repaired_at,
        Some(pending),
        true,
    );
    let b = write_input(
        &in_dir.join("b"),
        2,
        &[(1, "b-1", 200), (3, "b-3", 200)],
        repaired_at,
        Some(pending),
        true,
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let report = rt
        .block_on(compact_sstables(
            vec![b, a],
            &out_dir,
            &schema(),
            10,
            None,
            None,
            true,
        ))
        .expect("compaction of compatible inputs must succeed");

    let (out_repaired, out_pending, out_transient) =
        decode_output_repair_state(&report.output.data_path);
    assert_eq!(
        out_repaired, repaired_at,
        "merged output must preserve repairedAt"
    );
    assert_eq!(
        out_pending,
        RepairField::Decoded(Some(pending)),
        "merged output must preserve the pendingRepair UUID"
    );
    assert_eq!(
        out_transient,
        RepairField::Decoded(true),
        "merged output must preserve isTransient"
    );
}

/// AC2 (unrepaired baseline): two unrepaired inputs compact to an unrepaired
/// output (the common corpus case — preservation is a no-op vs the previous
/// hardcoded zeros, but proven end-to-end).
#[test]
fn compaction_preserves_unrepaired_state() {
    let temp = TempDir::new().expect("tempdir");
    let in_dir = temp.path().join("inputs");
    let out_dir = temp.path().join("out");
    std::fs::create_dir_all(&in_dir).unwrap();

    let a = write_input(&in_dir.join("a"), 1, &[(1, "a-1", 100)], 0, None, false);
    let b = write_input(&in_dir.join("b"), 2, &[(2, "b-2", 200)], 0, None, false);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let report = rt
        .block_on(compact_sstables(
            vec![b, a],
            &out_dir,
            &schema(),
            10,
            None,
            None,
            true,
        ))
        .expect("compaction of unrepaired inputs must succeed");

    let (out_repaired, out_pending, out_transient) =
        decode_output_repair_state(&report.output.data_path);
    assert_eq!(out_repaired, 0);
    assert_eq!(out_pending, RepairField::Decoded(None));
    assert_eq!(out_transient, RepairField::Decoded(false));
}

/// AC3: a compaction mixing repaired and unrepaired inputs is REJECTED with a
/// typed error — never silently merged.
#[test]
fn compaction_rejects_mixed_repaired_unrepaired() {
    let temp = TempDir::new().expect("tempdir");
    let in_dir = temp.path().join("inputs");
    let out_dir = temp.path().join("out");
    std::fs::create_dir_all(&in_dir).unwrap();

    // One repaired input, one unrepaired input → mixed.
    let repaired = write_input(
        &in_dir.join("a"),
        1,
        &[(1, "a-1", 100)],
        1_700_000_000_000,
        None,
        false,
    );
    let unrepaired = write_input(&in_dir.join("b"), 2, &[(2, "b-2", 200)], 0, None, false);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let result = rt.block_on(compact_sstables(
        vec![unrepaired, repaired],
        &out_dir,
        &schema(),
        10,
        None,
        None,
        true,
    ));

    let err = result.expect_err("mixed repaired/unrepaired compaction must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("repair boundary"),
        "rejection error must name the repair boundary, got: {msg}"
    );

    // No output SSTable must have been published.
    let published_outputs = std::fs::read_dir(&out_dir)
        .map(|rd| rd.flatten().count())
        .unwrap_or(0);
    assert_eq!(
        published_outputs, 0,
        "a rejected mixed-state compaction must not publish any output"
    );
}

/// AC3 (pending-repair boundary): inputs that disagree on `pendingRepair` are
/// also rejected (a pending-repair input must not merge with a non-pending one).
#[test]
fn compaction_rejects_mixed_pending_repair() {
    let temp = TempDir::new().expect("tempdir");
    let in_dir = temp.path().join("inputs");
    let out_dir = temp.path().join("out");
    std::fs::create_dir_all(&in_dir).unwrap();

    let pending = write_input(
        &in_dir.join("a"),
        1,
        &[(1, "a-1", 100)],
        0,
        Some([0x42; 16]),
        false,
    );
    let no_pending = write_input(&in_dir.join("b"), 2, &[(2, "b-2", 200)], 0, None, false);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let result = rt.block_on(compact_sstables(
        vec![no_pending, pending],
        &out_dir,
        &schema(),
        10,
        None,
        None,
        true,
    ));
    assert!(
        result.is_err(),
        "a pending-repair input must not compact with a non-pending input"
    );
}

/// AC3 (classifier unit): `classify_inputs` returns the shared state for
/// compatible inputs and an error for a mixed set, independent of the merge
/// machinery.
#[test]
fn classify_inputs_returns_shared_or_rejects() {
    let temp = TempDir::new().expect("tempdir");
    let in_dir = temp.path().join("inputs");
    std::fs::create_dir_all(&in_dir).unwrap();

    let a = write_input(&in_dir.join("a"), 1, &[(1, "a-1", 100)], 500, None, false);
    let b = write_input(&in_dir.join("b"), 2, &[(2, "b-2", 200)], 500, None, false);
    let c = write_input(&in_dir.join("c"), 3, &[(3, "c-3", 300)], 0, None, false);

    // Compatible (a, b): shared repairedAt = 500.
    let shared = classify_inputs(&[a.clone(), b.clone()]).expect("compatible classify");
    assert_eq!(
        shared,
        RepairState {
            repaired_at: 500,
            pending_repair: None,
            is_transient: false,
        }
    );

    // Mixed (a, c): rejected.
    assert!(
        classify_inputs(&[a, c]).is_err(),
        "mixed repairedAt must be rejected by classify_inputs"
    );
}

/// Resolve the committed datasets root (env override first, else workspace tree).
fn datasets_sstables_root() -> PathBuf {
    let root = if let Ok(root) = std::env::var("CQLITE_DATASETS_ROOT") {
        PathBuf::from(root)
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(|workspace| workspace.join("test-data/datasets"))
            .unwrap_or_else(|| PathBuf::from("test-data/datasets"))
    };
    root.join("sstables")
}

/// Derive the `Statistics.db` sibling path from a `Data.db` path.
fn stats_sibling(data: &Path) -> PathBuf {
    let name = data.file_name().and_then(|n| n.to_str()).unwrap();
    data.with_file_name(name.replace("Data.db", "Statistics.db"))
}

/// Authoritatively decode a real clustered SSTable's repair fields from its
/// `Statistics.db`. Returns the full `RepairMetadata` so the caller can assert on
/// the `Decoded` vs `Unparsed` distinction directly.
fn decode_real_repair(data: &Path) -> Option<cqlite_core::parser::repair_metadata::RepairMetadata> {
    let stats = stats_sibling(data);
    let bytes = std::fs::read(&stats).ok()?;
    let gates = VersionGates::from_path(&stats).ok()?;
    parse_repair_metadata(&bytes, Some(&gates)).ok()
}

/// REGRESSION + AC2/AC3 (issue #1021, HIGH finding resolved): for clustered
/// `oa`/`da` SSTables the full version-gated walk now skips PAST the
/// `improvedMinMax` covered-clustering `Slice` by resolving each clustering
/// column's `valueLengthIfFixed()` from the persisted `clusteringTypes` and
/// mirroring Cassandra's `ClusteringBoundOrBoundary.Serializer.skipValues`. So
/// `pendingRepair` / `isTransient` are now decoded AUTHORITATIVELY (from real
/// bytes) instead of reported as `Unparsed`.
///
/// This drives off the REAL corpus: the `test_oa` `static_table` /
/// `tombstone_table` fixtures (clustered, non-empty covered-clustering Slice) must
/// now report `pendingRepair = Decoded(None)` and `isTransient = Decoded(false)`
/// — the genuine unrepaired state proven from bytes. `classify_inputs` must
/// SUCCEED on a set of such inputs and PRESERVE the (unrepaired) repair state,
/// keying off the authoritatively decoded fields (NOT defaulting an unknown).
///
/// Skip-on-absence (datasets not fetched) but fail-on-present-wrong: if a real
/// clustered fixture is on disk, the fields MUST decode and classification MUST
/// succeed with the authoritative unrepaired state.
#[test]
fn classify_inputs_decodes_clustered_oa_repair_fields_authoritatively() {
    let root = datasets_sstables_root();
    // Real clustered oa/da fixtures whose covered-clustering Slice is non-empty:
    // before #1021's type-aware skip these decoded pendingRepair/isTransient as
    // Unparsed; they must now decode authoritatively.
    let candidates = [
        root.join("test_oa/static_table-4bba006064e711f1bd3ac7dbf655c673/oa-2-big-Data.db"),
        root.join("test_oa/tombstone_table-4bc746d064e711f1bd3ac7dbf655c673/oa-2-big-Data.db"),
        root.join("test_da/simple_table-de1be8b064e711f19ad401a8c8227b11/da-2-bti-Data.db"),
    ];

    let present: Vec<PathBuf> = candidates.iter().filter(|p| p.exists()).cloned().collect();

    if present.is_empty() {
        eprintln!(
            "classify_inputs_decodes_clustered_oa_repair_fields_authoritatively: SKIP — \
             no clustered Data.db fixtures fetched under {}",
            root.display()
        );
        return;
    }

    // The fields must now DECODE (not be Unparsed) and decode to the genuine
    // unrepaired state. Fail-on-present-wrong: a clustered fixture that still
    // reports Unparsed would mean the type-aware skip failed to traverse it.
    for data in &present {
        let md = decode_real_repair(data)
            .unwrap_or_else(|| panic!("{}: repair metadata must decode", data.display()));
        assert_eq!(
            md.pending_repair,
            RepairField::Decoded(None),
            "{}: clustered SSTable pendingRepair must now decode authoritatively (None) \
             via the type-aware covered-Slice skip, not be reported Unparsed",
            data.display()
        );
        assert_eq!(
            md.is_transient,
            RepairField::Decoded(false),
            "{}: clustered SSTable isTransient must now decode authoritatively (false)",
            data.display()
        );
        assert_eq!(
            md.repaired_at,
            0,
            "{}: corpus is unrepaired",
            data.display()
        );
    }

    // Single-input classification: must succeed and report the authoritatively
    // decoded unrepaired state.
    for data in &present {
        let state = classify_inputs(std::slice::from_ref(data)).unwrap_or_else(|e| {
            panic!(
                "{}: classify_inputs must SUCCEED for a valid clustered SSTable whose \
                 repair fields now decode authoritatively (issue #1021), got: {e}",
                data.display()
            )
        });
        assert_eq!(
            state,
            RepairState {
                repaired_at: 0,
                pending_repair: None,
                is_transient: false,
            },
            "{}: a valid unrepaired clustered SSTable must classify as the unrepaired state",
            data.display()
        );
    }

    // Multi-input common case: two same-state inputs classify as compatible and
    // return the shared (authoritatively decoded) unrepaired state.
    if present.len() >= 2 {
        let shared = classify_inputs(&present).unwrap_or_else(|e| {
            panic!(
                "classify_inputs must SUCCEED for valid same-state clustered SSTables \
                 (issue #1021), got: {e}"
            )
        });
        assert_eq!(
            shared,
            RepairState {
                repaired_at: 0,
                pending_repair: None,
                is_transient: false,
            },
            "valid unrepaired clustered SSTables must classify as the shared unrepaired state"
        );
    }
}
