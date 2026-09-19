//! Issue #4196 (spec R2.4, R3.1) — a partition whose decode fails partway is
//! lost WHOLE: no prefix of it appears in salvage's output (design D2, the
//! resurrection-bug rationale the whole design rests on).
//!
//! # Round 20 — a "rows decoded before failure" count is UNREACHABLE today,
//! # proven structurally, not merely unfound by search; round 21 removed it
//!
//! Round 19 left "no fixture demonstrating a non-empty decoded prefix" (for
//! a since-removed `Loss.rows_decoded_before_failure` manifest field) as an
//! open search problem. Round 20 (lead-directed) built the search tool —
//! `corrupt_byte_fixture::Mutation::AtDecompressedOffset` flips ONE byte at
//! a CALLER-CHOSEN decompressed position via the same search-and-verify
//! technique the other `Mutation` variants use — and exhaustively scanned
//! EVERY flippable byte across an ENTIRE real multi-row partition
//! (`test_timeseries.tick_data`'s first partition, 7 rows spanning
//! decompressed offsets `[0, 443)`, `TIMEUUID` clustering — deliberately
//! NOT a `TEXT`-clustering table, since the lead's ruling was that the
//! failure need not come from a text clustering key): 345 candidate offsets
//! tried, 62 genuinely flippable (a clean single-byte change verified by
//! re-decompression), EVERY ONE of which — from the first byte of row 1
//! through the last byte of row 6, the partition's LAST row — produced a
//! ZERO decoded-rows count when it produced a `Decode` loss at all (the
//! rest were `Truncated`).
//!
//! **Root cause, found in source, not inferred from the null result**:
//! `parse_one_partition_for_compaction` delegates to
//! `drive_partition_sliding`
//! (`cqlite-core/src/storage/sstable/reader/parsing/row_decoder/partition_driver.rs`),
//! whose own comment states the mechanism precisely — "Finding 1 (#827):
//! buffer this partition's rows locally and forward them to the external
//! `emit` only once the partition is CONFIRMED complete (an `Emitted`
//! return)". Every row a partition decodes is held in a local `pending:
//! Vec<P::Row>` and is handed to the caller's `emit` callback EXCLUSIVELY
//! inside the `flush_and_emitted!` macro, which fires ONLY on a
//! structurally-complete partition (the `END_OF_PARTITION` marker, or a
//! final-chunk truncated-body flush). A mid-partition `Err` from ANYWHERE
//! in the row-parsing loop propagates via `?` and reaches the caller
//! WITHOUT EVER calling `flush_and_emitted!` — `pending` (and every row
//! already decoded within it) is simply dropped. So a mid-partition
//! `PartitionAtOffsetOutcome::DecodeError` is reached with ZERO rows
//! externally visible **by construction, for every partition, every
//! corruption, unconditionally** — not a property of any one fixture.
//! Issue #827 itself is CLOSED (a 2025 perf change bounding K-way-merge
//! memory independent of input size — the buffering exists for a real,
//! deliberate reason, not an oversight) and unrelated to #3721 (also
//! CLOSED; a DIFFERENT swallow, the per-COLUMN `break` in the SCAN read
//! path's row assembly, not this per-PARTITION buffering in the
//! COMPACTION/salvage decode path).
//!
//! **Round 21 (roborev, issue #4196) resolution**: `Loss.rows_decoded_before_failure`
//! — the field salvage's manifest previously reported to an operator for
//! every `class: "decode"` loss — was REMOVED (along with
//! `PartitionAtOffsetOutcome::DecodeError`'s own matching field one layer
//! down). A manifest field ALWAYS `0` while its doc claimed it counted
//! decoded rows misled the operator design D5 exists for. This did NOT
//! weaken design D2's actual guarantee — a partition whose decode fails
//! partway contributes NOTHING to the output; `pending`'s drop-on-error IS
//! that guarantee, one layer removed from what the removed field could
//! ever have observed. Issue #4218 tracks reinstating a real count once
//! `drive_partition_sliding` can report incremental progress. The test
//! below asserts the actual safety property this whole file exists to
//! prove — zero output rows for the needle partition key, and the loss
//! named in the manifest — independent of any row-count field.
//!
//! # Oracle (#3042)
//!
//! `corrupt_byte_fixture::stage_control_and_mutated` — a REAL Cassandra
//! 5.0 fixture (`test_basic.composite_key_table`) staged twice, pristine and
//! with exactly ONE decompressed byte flipped inside a clustering value
//! (issue #3782's mutation), CRC recomputed so the corruption is invisible to
//! integrity checks and length-preserving. The needle partition's identity is
//! derived from `Index.db`'s own recorded positions
//! (`corrupt_byte_fixture::index_partition_positions`), not assumed.
//!
//! # BOTH format families, because R2.4 names both
//!
//! The scenario names `BIG_COMPOSITE` **and** `BTI_MULTICLUSTERING`. The `da`
//! leg (`corrupt_row_in_a_bti_partition_loses_it_whole_never_a_prefix`) stages
//! the committed `test_da.multiclustering_table` through the SAME `stage_spec`
//! mutation path, because `bti_scan_with_metadata_cancellable` reaches the row
//! parse by a DIFFERENT route than BIG's `sequential_scan` (issue #3782) — a
//! property proven on `nb` alone says nothing about `da`. BTI has no
//! `Index.db`, so that leg derives the needle's identity from the committed
//! `*-Data.db.jsonl` `sstabledump` golden's own per-partition `position`
//! instead; see the section header above that test for why that is the
//! equivalent authoritative record and not a substitute oracle.

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use std::path::PathBuf;

use cqlite_core::storage::write_engine::salvage::{salvage_sstable, LossClass, SalvageOptions};
use tempfile::TempDir;

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/corrupt_byte_fixture.rs"]
mod fixture;

use fixture::{FixtureSpec, BIG_COMPOSITE, BTI_MULTICLUSTERING, FIX_KS, FIX_TABLE};

fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// The `TableSchema` for an arbitrary [`FixtureSpec`], read from its COMMITTED
/// CQL file (`test-data/schemas/`, resolved checkout-relative — never derived
/// from `CQLITE_DATASETS_ROOT`, per #3131).
fn schema_for(spec: &FixtureSpec) -> cqlite_core::schema::TableSchema {
    let schema_path = datasets_root::schema_path(spec.schema_file).expect("committed CQL schema");
    let cql = std::fs::read_to_string(schema_path).expect("read schema");
    let start = cql
        .find(&format!("CREATE TABLE IF NOT EXISTS {}", spec.table))
        .expect("CREATE TABLE statement");
    let end = start + cql[start..].find(';').expect("statement terminator") + 1;
    let mut t = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end])
        .expect("parse CREATE TABLE");
    t.keyspace = spec.keyspace.to_string();
    t
}

fn table_schema() -> cqlite_core::schema::TableSchema {
    schema_for(&BIG_COMPOSITE)
}

fn single_data_db(dir: &std::path::Path) -> PathBuf {
    let mut found: Vec<PathBuf> = Vec::new();
    for e in std::fs::read_dir(dir).expect("read dir").flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if name.ends_with("-Data.db") {
            found.push(e.path());
        }
    }
    match found.len() {
        1 => found.pop().unwrap(),
        n => panic!("{dir:?}: expected exactly ONE Data.db, found {n} ({found:?})"),
    }
}

/// R2.4 + R3.1: the mutated fixture's needle partition is lost whole (class
/// `decode` — see the module doc for why a decoded-rows count cannot be
/// reported, structurally, and was removed rather than shipped always-zero),
/// and salvage's output carries ZERO rows for that partition key — never a
/// prefix — while every OTHER partition in the control matches the output
/// byte-for-byte (compaction-row decode equality, the same oracle the R1
/// healthy-parity BTI case uses).
#[tokio::test]
async fn corrupt_row_loses_the_needle_partition_whole_never_a_prefix() {
    let Some(fixture_dir) = datasets_root::sstables_root_for_table(FIX_KS, FIX_TABLE)
        .map(|root| datasets_root::table_generation_dirs(&root, FIX_KS, FIX_TABLE))
        .and_then(|dirs| dirs.into_iter().next())
    else {
        if require_fixtures_strict() {
            panic!(
                "CQLITE_REQUIRE_FIXTURES=1 but {FIX_KS}.{FIX_TABLE} is absent; {}",
                datasets_root::describe_search(FIX_KS, FIX_TABLE)
            );
        }
        eprintln!(
            "[issue_4196] {FIX_KS}.{FIX_TABLE} fixture absent (dataset not fetched); skipping"
        );
        return;
    };

    let staged = fixture::stage_control_and_mutated(&fixture_dir, "salvage-atomicity");

    // The needle partition: Index.db's LAST entry whose recorded position is
    // <= the mutation's decompressed offset (positions are ascending, so this
    // is the partition CONTAINING that offset) -- an on-disk-format fact from
    // the CONTROL copy, never assumed.
    let control_positions = fixture::index_partition_positions(&staged.control_dir);
    let (needle_key, needle_pos) = control_positions
        .iter()
        .rfind(|(_, pos)| *pos <= staged.mutated_offset)
        .cloned()
        .expect("a partition containing the mutated offset must exist");

    let schema = table_schema();

    // Salvage the MUTATED copy.
    let mutated_data_db = single_data_db(&staged.mutated_dir);
    let temp = TempDir::new().expect("tempdir");
    let mutated_out_root = temp.path().join("mutated-out");
    let mutated_report = salvage_sstable(
        &mutated_data_db,
        &mutated_out_root,
        &schema,
        SalvageOptions::default(),
    )
    .await
    .expect("salvage of the mutated fixture must not error (a classified loss, not an Err)");

    // R2.4: exactly the needle partition is lost, class `decode`.
    let needle_hex = hex::encode(&needle_key);
    let needle_loss = mutated_report
        .losses
        .iter()
        .find(|l| l.key_hex == needle_hex)
        .unwrap_or_else(|| {
            panic!(
                "needle partition (key_hex={needle_hex}, Index.db pos={needle_pos}) is not in \
                 the loss list: {:?}",
                mutated_report.losses
            )
        });
    assert_eq!(
        needle_loss.class,
        LossClass::Decode,
        "needle partition loss must classify `decode`; got {:?}",
        needle_loss
    );
    // R3.1's scenario text originally described a needle partition with
    // >= 2 rows decoded before the corrupt one. Round 20 proved that
    // scenario UNREACHABLE by construction (see this file's module doc,
    // "Round 20" section, for the full derivation and source citation) —
    // `drive_partition_sliding` buffers a whole partition's rows and only
    // forwards them to its caller on structural completion (issue #827),
    // so a mid-partition error is reached with NOTHING externally visible
    // yet, for every partition and every corruption, unconditionally.
    // Round 21 (roborev, issue #4196) then removed the `Loss` field
    // (`rows_decoded_before_failure`) that would have reported this — a
    // manifest field ALWAYS `0` while its doc claimed it counted decoded
    // rows misled the operator design D5 exists for. The property this
    // test asserts does NOT depend on that count either way: it is the
    // real safety guarantee (zero output rows for the needle partition,
    // asserted below) that D2 rests on, independent of any diagnostic
    // field.
    let other_losses: Vec<_> = mutated_report
        .losses
        .iter()
        .filter(|l| l.key_hex != needle_hex)
        .collect();
    assert!(
        other_losses.is_empty(),
        "only the needle partition should be lost; also lost: {other_losses:?}"
    );
    // roborev, issue #4196: pin the exact recovered count and refusal state
    // UNCONDITIONALLY, rather than guarding the substantive assertions below
    // behind a runtime `if partitions.recovered > 0` — a fixture change that
    // silently dropped every OTHER partition too would otherwise make this
    // test pass having compared nothing (the vacuity class this repo's
    // doctrine calls out). `control_positions.len() - 1` is every partition
    // except the needle.
    assert_eq!(
        mutated_report.partitions.recovered,
        control_positions.len() - 1,
        "expected every partition except the needle to be recovered"
    );
    assert!(
        mutated_report.refused.is_none(),
        "a partial loss with real survivors must not refuse; got {:?}",
        mutated_report.refused
    );

    // R3.1: the output holds ZERO rows for the needle partition key -- never
    // a prefix. Seek by key on the salvage output via a full compaction-row
    // decode and filter to the needle key (mirrors the R1 BTI healthy-parity
    // test's oracle).
    let mutated_out_table_dir = mutated_out_root.join(&schema.keyspace).join(&schema.table);
    let salvaged_data_db = single_data_db(&mutated_out_table_dir);
    let rows = decode_all_rows(&salvaged_data_db, &schema).await;
    let needle_rows_in_output: Vec<_> = rows
        .iter()
        .filter(|r| r.key.as_bytes() == needle_key.as_slice())
        .collect();
    assert!(
        needle_rows_in_output.is_empty(),
        "output must hold ZERO rows for the needle partition (a prefix would resurrect data \
         shadowed by whatever made this partition undecodable); found {}",
        needle_rows_in_output.len()
    );

    // Every OTHER partition matches the CONTROL byte-for-byte (compaction-row
    // decode equality).
    let control_out_root = temp.path().join("control-out");
    let control_data_db = single_data_db(&staged.control_dir);
    let control_report = salvage_sstable(
        &control_data_db,
        &control_out_root,
        &schema,
        SalvageOptions::default(),
    )
    .await
    .expect("salvage of the pristine control must succeed cleanly");
    assert!(
        control_report.losses.is_empty(),
        "control fixture salvage must have zero losses; got {:?}",
        control_report.losses
    );
    let control_out_table_dir = control_out_root.join(&schema.keyspace).join(&schema.table);
    let control_salvaged_data_db = single_data_db(&control_out_table_dir);
    let control_rows = decode_all_rows(&control_salvaged_data_db, &schema).await;
    let control_rows_minus_needle: Vec<_> = control_rows
        .iter()
        .filter(|r| r.key.as_bytes() != needle_key.as_slice())
        .collect();

    let mutated_rows = decode_all_rows(&salvaged_data_db, &schema).await;
    assert_eq!(
        mutated_rows.len(),
        control_rows_minus_needle.len(),
        "every OTHER partition's row count must match the control minus the needle"
    );
    assert_eq!(
        &mutated_rows,
        &control_rows_minus_needle
            .into_iter()
            .cloned()
            .collect::<Vec<_>>(),
        "every OTHER partition's rows must byte-match the control (minus the needle partition, \
         which is a total loss)"
    );

    eprintln!(
        "[issue_4196] {FIX_KS}.{FIX_TABLE}: needle partition (key_hex={needle_hex}) lost whole \
         (class decode), zero rows in output, every other partition byte-matches the control."
    );
}

// ---------------------------------------------------------------------------
// R2.4's BTI (`da`) leg — the C-audit's B1.
//
// The scenario names TWO fixtures, `BIG_COMPOSITE` **and**
// `BTI_MULTICLUSTERING`, because the `da` full scan reaches the same row parse
// by a DIFFERENT route: `bti_scan_with_metadata_cancellable` stitches the whole
// data section and calls `parse_block_with_cell_metadata`, where BIG goes
// through `sequential_scan`/`parse_block` (issue #3782). A partition-atomicity
// property proven only on `nb` says nothing about `da`.
//
// # Why the needle's identity comes from the sstabledump GOLDEN here
//
// The BIG leg derives it from `Index.db`'s own recorded positions. BTI has NO
// `Index.db` — its partition index is the `Partitions.db` trie, which this
// harness deliberately does not re-implement. The equivalent AUTHORITATIVE
// Cassandra-written record is the committed `da-2-bti-Data.db.jsonl`
// `sstabledump` golden: every `partition` object carries the `position` (the
// UNCOMPRESSED data-file offset) Cassandra itself reported for that partition.
// So the derivation stays Cassandra-written bytes (#3041/#3042) — never a scan
// for a byte pattern (#28) and never CQLite's own output.
//
// The fixture is FULLY git-tracked (9 components incl. `Data.db`,
// `Partitions.db`, `Rows.db` and the golden), so this leg fails CLOSED
// unconditionally — unlike the BIG leg beside it, whose fixture is fetched.
// ---------------------------------------------------------------------------

/// Every `(partition key as declared `int` pk, UNCOMPRESSED data-section
/// position)` pair the committed `sstabledump` golden records, in file order.
///
/// Strict by construction: a golden whose shape drifts (a non-`int`-shaped key,
/// a missing `position`, a non-ascending position list, zero partitions) FAILS
/// by name rather than yielding a plausible-but-wrong needle.
fn golden_partition_positions(fixture_dir: &std::path::Path) -> Vec<(i32, usize)> {
    let golden = {
        let mut found: Option<PathBuf> = None;
        for e in std::fs::read_dir(fixture_dir)
            .expect("read fixture dir")
            .flatten()
        {
            if e.file_name().to_string_lossy().ends_with("-Data.db.jsonl") {
                found = Some(e.path());
                break;
            }
        }
        found.unwrap_or_else(|| {
            panic!(
                "{fixture_dir:?}: no *-Data.db.jsonl sstabledump golden — it is committed beside \
                 the Data.db and is THE Cassandra-written oracle for this leg's partition \
                 positions; a missing golden is a broken checkout, never a skip"
            )
        })
    };
    let text = std::fs::read_to_string(&golden).unwrap_or_else(|e| panic!("read {golden:?}: {e}"));
    let mut out: Vec<(i32, usize)> = Vec::new();
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let v: serde_json::Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("{golden:?}: not JSON: {e}"));
        let partition = v
            .get("partition")
            .and_then(|p| p.as_object())
            .unwrap_or_else(|| panic!("{golden:?}: partition object missing"));
        let key_array = partition
            .get("key")
            .and_then(|k| k.as_array())
            .unwrap_or_else(|| panic!("{golden:?}: partition key missing"));
        assert_eq!(
            key_array.len(),
            1,
            "{golden:?}: this table declares a SINGLE-column `pk int` partition key; a \
             {}-component key means the fixture is not the one this derivation applies to",
            key_array.len()
        );
        let pk: i32 = key_array[0]
            .as_str()
            .unwrap_or_else(|| panic!("{golden:?}: sstabledump renders an `int` pk as a string"))
            .parse()
            .unwrap_or_else(|e| panic!("{golden:?}: pk is not an i32: {e}"));
        let position = partition
            .get("position")
            .and_then(|p| p.as_u64())
            .unwrap_or_else(|| {
                panic!(
                    "{golden:?}: partition {pk} has no `position` — the needle's identity is \
                        derived from it, so its absence must fail loudly"
                )
            }) as usize;
        if let Some((prev_pk, prev)) = out.last() {
            assert!(
                position > *prev,
                "{golden:?}: partition positions must ASCEND (pk {pk} at {position} follows pk \
                 {prev_pk} at {prev}); the needle derivation below is a reverse scan over an \
                 ordered list"
            );
        }
        out.push((pk, position));
    }
    assert!(
        out.len() >= 2,
        "{golden:?}: this leg needs at least TWO partitions — with one, 'the needle is lost and \
         every OTHER partition survives' is unobservable; got {}",
        out.len()
    );
    out
}

/// The committed `test_da.multiclustering_table` generation directory, or a hard
/// failure. Its binaries are git-tracked, so absence is a broken checkout and
/// must never skip (#3220) — this is deliberately NOT gated on
/// `CQLITE_REQUIRE_FIXTURES`.
fn bti_generation_dir() -> PathBuf {
    let (ks, table) = (BTI_MULTICLUSTERING.keyspace, BTI_MULTICLUSTERING.table);
    datasets_root::sstables_root_for_table(ks, table)
        .map(|root| datasets_root::table_generation_dirs(&root, ks, table))
        .and_then(|dirs| dirs.into_iter().next())
        .unwrap_or_else(|| {
            panic!(
                "COMMITTED fixture {ks}.{table} is absent — its Data.db, Partitions.db, Rows.db \
                 and sstabledump golden are all git-tracked, so this is a broken checkout, NOT an \
                 unfetched dataset, and must never skip (#3220). {}",
                datasets_root::describe_search(ks, table)
            )
        })
}

/// R2.4 (BTI half) + R3.1: the same property as the BIG leg above, on a real
/// Cassandra 5.0 **`da`/BTI** fixture staged through the SAME
/// `stage_spec`/mutation/measurement path — the needle partition is lost WHOLE
/// (class `decode`), salvage's output holds ZERO rows for it, and every OTHER
/// partition is recovered and byte-matches the pristine control.
#[tokio::test]
async fn corrupt_row_in_a_bti_partition_loses_it_whole_never_a_prefix() {
    let fixture_dir = bti_generation_dir();
    let (ks, table) = (BTI_MULTICLUSTERING.keyspace, BTI_MULTICLUSTERING.table);

    // Cassandra's OWN recorded partition positions, before anything is staged.
    let golden_positions = golden_partition_positions(&fixture_dir);

    let staged = fixture::stage_spec(&BTI_MULTICLUSTERING, &fixture_dir, "salvage-atomicity-bti");

    // The needle partition: the LAST golden partition whose recorded position is
    // <= the mutation's decompressed offset (positions ascend, asserted above,
    // so this is the partition CONTAINING that offset).
    let (needle_pk, needle_pos) = golden_positions
        .iter()
        .rfind(|(_, pos)| *pos <= staged.mutated_offset)
        .copied()
        .unwrap_or_else(|| {
            panic!(
                "no golden partition starts at or before the mutated offset {} — the mutation \
                 landed outside every partition this oracle knows about",
                staged.mutated_offset
            )
        });
    // A single-column `int` partition key serializes as its 4-byte big-endian
    // value (`Int32Type`), with no composite framing — so this is the on-disk
    // key the manifest reports in `key_hex`.
    let needle_key = needle_pk.to_be_bytes().to_vec();
    let needle_hex = hex::encode(&needle_key);

    let schema = schema_for(&BTI_MULTICLUSTERING);

    let mutated_data_db = single_data_db(&staged.mutated_dir);
    let temp = TempDir::new().expect("tempdir");
    let mutated_out_root = temp.path().join("mutated-out");
    let mutated_report = salvage_sstable(
        &mutated_data_db,
        &mutated_out_root,
        &schema,
        SalvageOptions::default(),
    )
    .await
    .expect("salvage of the mutated BTI fixture must not error (a classified loss, not an Err)");

    // R2.4: exactly the needle partition is lost, class `decode`.
    let needle_loss = mutated_report
        .losses
        .iter()
        .find(|l| l.key_hex == needle_hex)
        .unwrap_or_else(|| {
            panic!(
                "needle partition (pk={needle_pk}, key_hex={needle_hex}, golden position \
                 {needle_pos}) is not in the loss list: {:?}",
                mutated_report.losses
            )
        });
    assert_eq!(
        needle_loss.class,
        LossClass::Decode,
        "needle partition loss must classify `decode`; got {:?}",
        needle_loss
    );
    let other_losses: Vec<_> = mutated_report
        .losses
        .iter()
        .filter(|l| l.key_hex != needle_hex)
        .collect();
    assert!(
        other_losses.is_empty(),
        "only the needle partition should be lost; also lost: {other_losses:?}"
    );
    // UNCONDITIONALLY, never behind an `if recovered > 0` (the #3220 vacuity
    // class): a fixture change that silently dropped every OTHER partition too
    // must red this test rather than let it pass having compared nothing.
    assert_eq!(
        mutated_report.partitions.recovered,
        golden_positions.len() - 1,
        "expected every partition except the needle to be recovered ({} golden partitions)",
        golden_positions.len()
    );
    assert!(
        mutated_report.refused.is_none(),
        "a partial loss with real survivors must not refuse; got {:?}",
        mutated_report.refused
    );

    // R3.1: ZERO rows for the needle partition in the output -- never a prefix.
    let mutated_out_table_dir = mutated_out_root.join(&schema.keyspace).join(&schema.table);
    let salvaged_data_db = single_data_db(&mutated_out_table_dir);
    let mutated_rows = decode_all_rows(&salvaged_data_db, &schema).await;
    let needle_rows_in_output: Vec<_> = mutated_rows
        .iter()
        .filter(|r| r.key.as_bytes() == needle_key.as_slice())
        .collect();
    assert!(
        needle_rows_in_output.is_empty(),
        "output must hold ZERO rows for the needle partition (a prefix would resurrect data \
         shadowed by whatever made this partition undecodable); found {}",
        needle_rows_in_output.len()
    );

    // Every OTHER partition matches the pristine CONTROL byte-for-byte.
    let control_out_root = temp.path().join("control-out");
    let control_data_db = single_data_db(&staged.control_dir);
    let control_report = salvage_sstable(
        &control_data_db,
        &control_out_root,
        &schema,
        SalvageOptions::default(),
    )
    .await
    .expect("salvage of the pristine BTI control must succeed cleanly");
    assert!(
        control_report.losses.is_empty(),
        "control fixture salvage must have zero losses; got {:?}",
        control_report.losses
    );
    let control_out_table_dir = control_out_root.join(&schema.keyspace).join(&schema.table);
    let control_salvaged_data_db = single_data_db(&control_out_table_dir);
    let control_rows = decode_all_rows(&control_salvaged_data_db, &schema).await;
    assert!(
        !control_rows.is_empty(),
        "the control must decode a NON-ZERO number of rows, or the comparison below is vacuous"
    );
    let control_rows_minus_needle: Vec<_> = control_rows
        .iter()
        .filter(|r| r.key.as_bytes() != needle_key.as_slice())
        .cloned()
        .collect();
    assert!(
        control_rows_minus_needle.len() < control_rows.len(),
        "the control must actually CONTAIN the needle partition (pk={needle_pk}), or 'the needle \
         is lost' compares nothing"
    );
    assert_eq!(
        mutated_rows.len(),
        control_rows_minus_needle.len(),
        "every OTHER partition's row count must match the control minus the needle"
    );
    assert_eq!(
        &mutated_rows, &control_rows_minus_needle,
        "every OTHER partition's rows must byte-match the control (minus the needle partition, \
         which is a total loss)"
    );

    eprintln!(
        "[issue_4196] {ks}.{table} (da/BTI): needle partition pk={needle_pk} \
         (key_hex={needle_hex}, golden position {needle_pos}) lost whole (class decode), zero rows \
         in output, {} of {} partitions recovered and byte-matching the control.",
        mutated_report.partitions.recovered,
        golden_positions.len()
    );
}

async fn decode_all_rows(
    data_db: &std::path::Path,
    schema: &cqlite_core::schema::TableSchema,
) -> Vec<cqlite_core::storage::sstable::reader::CompactionRow> {
    use cqlite_core::platform::Platform;
    use cqlite_core::storage::sstable::SSTableReader;
    use std::sync::Arc;

    let config = cqlite_core::Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let reader = SSTableReader::open(data_db, &config, platform)
        .await
        .expect("open reader");
    reader
        .iterate_all_partitions_for_compaction(Some(schema))
        .await
        .expect("decode rows")
}
