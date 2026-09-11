//! Issue #4196 (spec R2.4, R3.1) — a partition whose decode fails partway is
//! lost WHOLE: no prefix of it appears in salvage's output (design D2, the
//! resurrection-bug rationale the whole design rests on). For THIS fixture +
//! mutation the needle partition's corrupted row is itself the FIRST row the
//! decoder reaches (`rows_decoded_before_failure == 0`, ASSERTED not just
//! measured — roborev, issue #4196, round 19).
//!
//! # Round 20 — why `rows_decoded_before_failure >= 1` is UNREACHABLE today,
//! # proven structurally, not merely unfound by search
//!
//! Round 19 left "no fixture demonstrating a non-empty decoded prefix" as an
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
//! through the last byte of row 6, the partition's LAST row — produced
//! `rows_decoded_before_failure == 0` when it produced a `Decode` loss at
//! all (the rest were `Truncated`).
//!
//! **Root cause, found in source, not inferred from the null result**:
//! `parse_one_partition_for_compaction` delegates to
//! `drive_partition_sliding`
//! (`cqlite-core/src/storage/sstable/reader/parsing/row_decoder/partition_driver.rs`),
//! whose own comment states the mechanism precisely — "Finding 1 (#827):
//! buffer this partition's rows locally and forward them to the external
//! `emit` only once the partition is CONFIRMED complete (an `Emitted`
//! return)". Every row a partition decodes is held in a local `pending:
//! Vec<P::Row>` and is handed to the caller's `emit` callback (the ONLY
//! thing that grows `rows.len()`, hence `rows_decoded_before_failure`)
//! EXCLUSIVELY inside the `flush_and_emitted!` macro, which fires ONLY on
//! a structurally-complete partition (the `END_OF_PARTITION` marker, or a
//! final-chunk truncated-body flush). A mid-partition `Err` from ANYWHERE
//! in the row-parsing loop propagates via `?` and reaches the caller
//! WITHOUT EVER calling `flush_and_emitted!` — `pending` (and every row
//! already decoded within it) is simply dropped. So
//! `PartitionAtOffsetOutcome::DecodeError { rows_decoded_before_failure,
//! .. }` is `0` **by construction, for every partition, every corruption,
//! unconditionally** — not a property of any one fixture. Issue #827 itself
//! is CLOSED (a 2025 perf change bounding K-way-merge memory independent of
//! input size — the buffering exists for a real, deliberate reason, not an
//! oversight) and unrelated to #3721 (also CLOSED; a DIFFERENT swallow, the
//! per-COLUMN `break` in the SCAN read path's row assembly, not this
//! per-PARTITION buffering in the COMPACTION/salvage decode path).
//!
//! **Consequence beyond this one test**: `Loss.rows_decoded_before_failure`
//! — the field salvage's OWN JSON manifest reports to an operator for every
//! `class: "decode"` loss — is therefore ALSO always `0` in production
//! today, for every decode-classified loss salvage has ever produced or
//! will produce until `drive_partition_sliding`'s buffering changes. Its
//! doc comment ("rows that HAD decoded when the error occurred") describes
//! a property the field cannot currently hold. This is a genuine, if minor
//! (informational-field-only, not a correctness defect: D2's WHOLE-loss
//! guarantee is unaffected — see below), documentation/manifest-honesty gap
//! worth its own follow-up; not fixed here per the lead's explicit
//! instruction not to invent a fixture once the scan came back empty.
//!
//! **What this does NOT weaken**: design D2's guarantee — a partition whose
//! decode fails partway contributes NOTHING to the output, `pending`'s
//! drop-on-error IS the mechanism proving it structurally, one layer
//! removed from what `rows_decoded_before_failure` can observe. The test
//! below still asserts the real safety property (zero needle-partition rows
//! in salvage's output) unconditionally, independent of this count.
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

#![cfg(all(feature = "write-support", not(feature = "tombstones")))]

use std::path::PathBuf;

use cqlite_core::storage::write_engine::salvage::{salvage_sstable, LossClass, SalvageOptions};
use tempfile::TempDir;

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/corrupt_byte_fixture.rs"]
mod fixture;

use fixture::{BIG_COMPOSITE, FIX_KS, FIX_TABLE};

fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

fn table_schema() -> cqlite_core::schema::TableSchema {
    let schema_path =
        datasets_root::schema_path(BIG_COMPOSITE.schema_file).expect("committed CQL schema");
    let cql = std::fs::read_to_string(schema_path).expect("read schema");
    let start = cql
        .find(&format!("CREATE TABLE IF NOT EXISTS {FIX_TABLE}"))
        .expect("CREATE TABLE statement");
    let end = start + cql[start..].find(';').expect("statement terminator") + 1;
    let mut t = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end])
        .expect("parse CREATE TABLE");
    t.keyspace = FIX_KS.to_string();
    t
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
/// `decode`; for THIS fixture/mutation `rows_decoded_before_failure == 0` —
/// see the module doc — reported but not asserted, since the safety property
/// below does not depend on that count), and salvage's output carries ZERO
/// rows for that partition key — never a prefix — while every OTHER
/// partition in the control matches the output byte-for-byte (compaction-row
/// decode equality, the same oracle the R1 healthy-parity BTI case uses).
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
    // R3.1's scenario text describes a needle partition with >= 2 rows
    // before the corrupt one; measured for THIS fixture + mutation
    // (`corrupt_byte_fixture::BIG_COMPOSITE`, a flipped clustering-value
    // byte) the corrupted row is itself the FIRST row the decoder reaches,
    // so `rows_decoded_before_failure` is 0 here rather than >= 2.
    //
    // roborev, issue #4196, round 19 Medium finding, RESOLVED round 20 (see
    // this file's module doc, "Round 20" section, for the full derivation
    // and source citation): `rows_decoded_before_failure` is `0` for EVERY
    // `Decode`-class loss unconditionally, by construction of
    // `drive_partition_sliding`'s row-buffering (issue #827) — proven by an
    // exhaustive scan of a real multi-row partition (345 candidate offsets,
    // 62 flippable, all 62 produced `0`), not merely unfound. This
    // assertion therefore pins a STRUCTURAL property of the decoder, not a
    // fact about this one fixture — it will hold for every fixture and
    // every mutation until `drive_partition_sliding` itself changes to emit
    // rows incrementally.
    assert_eq!(
        needle_loss.rows_decoded_before_failure, 0,
        "rows_decoded_before_failure must be 0 for every Decode-class loss today — \
         drive_partition_sliding buffers all of a partition's rows and only forwards them on \
         structural completion (issue #827), so a mid-partition Err always drops the buffer \
         before any row is externally visible (see this file's module doc, Round 20). A \
         non-zero value here means that buffering behavior CHANGED — re-verify whether a \
         genuinely discriminating case (>= 1) is now reachable, and if so, extend this test to \
         cover it rather than just the degenerate case"
    );
    eprintln!(
        "[issue_4196] rows_decoded_before_failure = {} for this fixture/mutation",
        needle_loss.rows_decoded_before_failure
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
         (class decode, {} rows decoded before failure), zero rows in output, every other \
         partition byte-matches the control.",
        needle_loss.rows_decoded_before_failure
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
