//! Issue #3928 — a partition HEADER that cannot be decoded on a buffer PROVEN
//! COMPLETE is DATA LOSS, so the walk must REFUSE instead of skipping a byte and
//! resynchronising.
//!
//! # The defect
//!
//! #3782 closed the ROW arm: a row decode error on a proven-complete stitched
//! buffer (`BufferExtent::Complete`) or at the sliding driver's final chunk
//! (`at_final_chunk`) now surfaces. The partition-HEADER arms were untouched and
//! still answered a malformed or undecodable header with
//! `tracing::warn!` + `offset += 1` (block-emit) / `ParseStep::Emitted(1)` and
//! `PartitionStreamStep::Consumed(1)` (the two drivers — the same byte resync,
//! spelled as a consumed-byte count).
//!
//! Two consequences, and the second is the bad one:
//!
//! 1. the malformed partition is silently DROPPED;
//! 2. the one-byte resync can land on MISALIGNED bytes that parse as a plausible
//!    header, so the walk INVENTS a partition that does not exist.
//!
//! #3782's probe attributed **15 of 35** corrupted read-path losses to the
//! header arm (`block_emit_windowed.rs`) against 20 to the row arm, so this was
//! the larger share.
//!
//! **The row COUNT goes UP while data is lost**, which is why nothing in this
//! file compares counts: every assertion is over the MULTISET of partition keys
//! against the pristine, Cassandra-written control.
//!
//! # Oracle (#3042)
//!
//! Every expectation comes from Cassandra-written bytes or from the pinned
//! `cassandra-5.0.8` source, never from CQLite's prior behaviour:
//!
//! * the control leg is the untouched Cassandra fixture, and the mutated leg
//!   differs from it by exactly ONE decompressed byte (asserted by the harness);
//! * the BIG (`nb`) site is the low byte of the 2-byte key length
//!   `ByteBufferUtil.writeWithShortLength` wrote (`ByteBufferUtil.java:362-368`),
//!   zeroed — so the header declares a 0-byte key while the key Cassandra wrote
//!   still follows, and every later structure is misframed;
//! * the BTI (`da`) site is the partition-level `DeletionTime` discriminator,
//!   which Cassandra writes as exactly `IS_LIVE_DELETION = 0b1000_0000` for a
//!   live partition and whose own reader THROWS on any other byte with that bit
//!   set — `if ((flags & 0xFF) != IS_LIVE_DELETION) throw new IOException(
//!   "Corrupted sstable. Invalid flags found deserializing DeletionTime")`
//!   (`DeletionTime.java:208-230`). So refusal is the FORMAT's expectation.
//!
//! # Pre-fix measurement (this file's cases, on `main` @ 05134c947)
//!
//! | surface (BIG, key-length byte zeroed)     | control | before the fix |
//! |-------------------------------------------|---------|----------------|
//! | see `docs`-free note in each case below   |         |                |
//!
//! The numbers are asserted, not tabulated: each case names what it observed in
//! its own panic message, so a table here could never go stale.
#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    // EVERY fixture this target reads is LZ4-compressed (the harness ASSERTS it
    // before mutating), and without the `lz4` feature the production
    // decompressor answers `Err("LZ4 compression not available")`, so the target
    // would still RUN and fail on its PRISTINE CONTROLS — a false FAIL wearing a
    // correctness failure's clothes (#3950). `Cargo.toml`'s `required-features`
    // for this target must always agree with this list.
    feature = "lz4"
))]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::platform::Platform;
use cqlite_core::schema::TableSchema;
use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::Database;

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/corrupt_byte_fixture.rs"]
mod fixture;
#[path = "support/multiset.rs"]
mod multiset;

use fixture::{FixtureSpec, BIG_COMPOSITE_HEADER, BTI_MULTICLUSTERING_HEADER};

/// The fixture's GENERATION directory, resolved per TABLE (#3220) so a root that
/// holds the keyspace but not this table cannot silently win the selection. The
/// two specs live in DIFFERENT roots on a fleet box — the BTI one is
/// git-committed in the checkout, the BIG one is fetched-corpus-only — so
/// evidence, never a preference order, decides (#3104). "Not found" is a loud
/// named panic, never a skip.
fn fixture_dir(spec: &FixtureSpec) -> PathBuf {
    datasets_root::resolve_table_generation_dir(spec.keyspace, spec.table).unwrap_or_else(|why| {
        panic!(
            "fixture {}.{} has no usable generation directory: {why}",
            spec.keyspace, spec.table
        )
    })
}

fn schema_file(spec: &FixtureSpec) -> PathBuf {
    datasets_root::schema_path(spec.schema_file).expect("committed CQL schema (#3148)")
}

fn table_schema(spec: &FixtureSpec) -> TableSchema {
    let cql = std::fs::read_to_string(schema_file(spec)).expect("read schema");
    let start = cql
        .find(&format!("CREATE TABLE IF NOT EXISTS {}", spec.table))
        .expect("CREATE TABLE statement");
    let end = start + cql[start..].find(';').expect("statement terminator") + 1;
    let mut t = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end])
        .expect("parse CREATE TABLE");
    t.keyspace = spec.keyspace.to_string();
    t
}

async fn open_db(spec: &FixtureSpec, data_dir: PathBuf) -> Database {
    ingest(IngestionConfig {
        schema_paths: vec![schema_file(spec)],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{}/", spec.keyspace)),
    })
    .await
    .expect("ingest")
    .database
}

async fn open_reader(dir: &Path) -> SSTableReader {
    let config = cqlite_core::Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    SSTableReader::open(&fixture::comp_file(dir, "-Data.db"), &config, platform)
        .await
        .expect("open SSTableReader")
}

/// What one read surface answered on one leg of a staged pair.
///
/// `keys` is `None` when the surface REFUSED (`Err`) and `Some(keys)` when it
/// answered `Ok` — carrying the partition key of every emitted element, in emit
/// order, so a DUPLICATE is visible. A set would hide the resync's
/// re-emission of a partition it had already emitted, which is one of the ways
/// the #3782/#3928 shape makes the count go UP while data is lost.
struct Outcome {
    name: &'static str,
    keys: Option<Vec<Vec<u8>>>,
}

impl Outcome {
    fn describe(&self, control: &std::collections::BTreeMap<Vec<u8>, usize>) -> String {
        match &self.keys {
            None => format!("{}: REFUSED", self.name),
            Some(keys) => {
                let got = multiset::multiset(keys.iter().cloned());
                let lost = multiset::deficit(&got, control);
                let fabricated = multiset::surplus(&got, control);
                format!(
                    "{}: Ok with {} emitted key occurrence(s); {} LOST [{}], {} FABRICATED [{}]",
                    self.name,
                    keys.len(),
                    lost.iter().map(|(_, n)| n).sum::<usize>(),
                    multiset::describe(&lost),
                    fabricated.iter().map(|(_, n)| n).sum::<usize>(),
                    multiset::describe(&fabricated),
                )
            }
        }
    }
}

/// Every partition-key-bearing read surface, evaluated over the generation in
/// `dir`.
///
/// The set deliberately spans BOTH propagation mechanisms this issue touches,
/// because they are different code with different discriminators and a fix to
/// one says nothing about the other:
///
/// * `distinct_partition_keys` / `partition_verify_scan` / `get_all_entries` /
///   `iterate_all_partitions` reach `parse_block_emit*`, whose discriminator is
///   the explicit `BufferExtent` the caller states;
/// * `iterate_all_partitions_for_compaction` reaches `drive_partition_sliding`
///   and `stream_all_partitions_for_compaction` reaches
///   `stream_partition_body_incremental`, whose discriminator is the driver's own
///   `at_final_chunk`.
async fn observe(dir: &Path, schema: &TableSchema) -> Vec<Outcome> {
    let reader = open_reader(dir).await;
    let mut out: Vec<Outcome> = Vec::new();

    out.push(Outcome {
        name: "distinct_partition_keys",
        keys: reader.distinct_partition_keys().await.ok(),
    });
    out.push(Outcome {
        name: "partition_verify_scan",
        keys: reader
            .partition_verify_scan()
            .await
            .ok()
            .map(|rows| rows.into_iter().map(|(k, _ldt)| k).collect()),
    });
    out.push(Outcome {
        name: "get_all_entries",
        keys: reader.get_all_entries().await.ok().map(|rows| {
            rows.into_iter()
                .map(|(_t, k, _r)| k.as_bytes().to_vec())
                .collect()
        }),
    });
    out.push(Outcome {
        name: "iterate_all_partitions",
        keys: reader.iterate_all_partitions().await.ok().map(|rows| {
            rows.into_iter()
                .map(|(k, _r)| k.as_bytes().to_vec())
                .collect()
        }),
    });
    out.push(Outcome {
        name: "iterate_all_partitions_for_compaction",
        keys: reader
            .iterate_all_partitions_for_compaction(Some(schema))
            .await
            .ok()
            .map(|rows| {
                rows.into_iter()
                    .map(|r| r.key.as_bytes().to_vec())
                    .collect()
            }),
    });

    let cancel = cqlite_core::storage::scan_cancel::ScanCancel::new();
    let mut streamed: Vec<Vec<u8>> = Vec::new();
    let streamed_ok = reader
        .stream_all_partitions_for_compaction(Some(schema), &cancel, |row| {
            streamed.push(row.key.as_bytes().to_vec());
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .await
        .is_ok();
    out.push(Outcome {
        name: "stream_all_partitions_for_compaction",
        keys: streamed_ok.then_some(streamed),
    });

    out
}

/// The control leg's partition-key multiset, plus the proof it is non-empty
/// (0-rows-when-present is a failure, never a pass).
async fn control_keys(
    dir: &Path,
    spec: &FixtureSpec,
) -> std::collections::BTreeMap<Vec<u8>, usize> {
    let reader = open_reader(dir).await;
    let keys = reader.distinct_partition_keys().await.unwrap_or_else(|e| {
        panic!(
            "the pristine {}.{} must enumerate its partition keys: {e}",
            spec.keyspace, spec.table
        )
    });
    assert!(
        !keys.is_empty(),
        "0-rows-when-present: the pristine {}.{} control enumerated no partition keys",
        spec.keyspace,
        spec.table
    );
    multiset::multiset(keys)
}

/// Assert that EVERY surface answered on a well-formed control leg, and did so
/// with the same partition-key multiset — so a later "the mutated leg refused"
/// can never be a surface that refuses on healthy data too.
fn assert_control_is_healthy(
    observed: &[Outcome],
    control: &std::collections::BTreeMap<Vec<u8>, usize>,
    spec: &FixtureSpec,
) {
    for o in observed {
        let keys = o.keys.as_ref().unwrap_or_else(|| {
            panic!(
                "control leg: {}.{} surface `{}` REFUSED a PRISTINE Cassandra fixture — the \
                 mutated-leg expectations below would be meaningless",
                spec.keyspace, spec.table, o.name
            )
        });
        let got = multiset::multiset(keys.iter().cloned());
        let surplus = multiset::surplus(&got, control);
        let deficit = multiset::deficit(&got, control);
        assert!(
            surplus.is_empty() && deficit.is_empty(),
            "control leg: {}.{} surface `{}` disagrees with `distinct_partition_keys` on a \
             PRISTINE fixture: {} surplus [{}], {} missing [{}]",
            spec.keyspace,
            spec.table,
            o.name,
            surplus.iter().map(|(_, n)| n).sum::<usize>(),
            multiset::describe(&surplus),
            deficit.iter().map(|(_, n)| n).sum::<usize>(),
            multiset::describe(&deficit)
        );
    }
}

/// AC1 — on a PROVEN-COMPLETE buffer a malformed partition header REFUSES.
///
/// The BIG (`nb`) fixture is a SINGLE compressed chunk, so every surface below
/// reaches its parse with the complete extent stated (`BufferExtent::Complete`)
/// or at the sliding driver's final chunk (`at_final_chunk == true`) — the two
/// authoritative "no further bytes can arrive" signals. There is therefore no
/// straddle reading of the failure available: the header is corrupt, and the
/// partition is lost unless the error is reported.
#[tokio::test]
async fn every_proven_complete_surface_refuses_a_malformed_partition_header() {
    let spec = &BIG_COMPOSITE_HEADER;
    let staged = fixture::stage_spec(spec, &fixture_dir(spec), "hdr-big");
    let schema = table_schema(spec);

    let control = control_keys(&staged.control_dir, spec).await;
    let control_observed = observe(&staged.control_dir, &schema).await;
    assert_control_is_healthy(&control_observed, &control, spec);

    let mutated = observe(&staged.mutated_dir, &schema).await;
    let tolerated: Vec<String> = mutated
        .iter()
        .filter(|o| o.keys.is_some())
        .map(|o| o.describe(&control))
        .collect();
    assert!(
        tolerated.is_empty(),
        "AC1: with the first partition header's key-length byte zeroed (decompressed offset {}, \
         {} position(s) changed) on a single-chunk fixture, every surface reaches its parse with \
         the buffer PROVEN COMPLETE, so each must REFUSE rather than byte-resync. {} of {} \
         surfaces still answered Ok:\n  {}",
        staged.mutated_offset,
        staged.mutated_span,
        tolerated.len(),
        mutated.len(),
        tolerated.join("\n  ")
    );
}

/// AC2 — NO FABRICATION: a single corrupted header byte may never cause a
/// partition to be emitted that is not in the pristine fixture.
///
/// This is deliberately weaker than AC1 and therefore MORE durable: it holds
/// even for a surface that legitimately tolerates (a window-bounded walk), and
/// it is the property the resync violated. A surplus key occurrence — including
/// a SURPLUS DUPLICATE of a real key, which a set comparison cannot see — is a
/// partition the SSTable does not contain.
///
/// Asserted over the multiset of emitted partition keys against the pristine
/// control, NEVER over a row count: the measured failure RAISES the count.
#[tokio::test]
async fn no_surface_fabricates_a_partition_from_a_corrupted_header_byte() {
    let spec = &BIG_COMPOSITE_HEADER;
    let staged = fixture::stage_spec(spec, &fixture_dir(spec), "fab-big");
    let schema = table_schema(spec);

    let control = control_keys(&staged.control_dir, spec).await;
    let fabricating: Vec<String> = observe(&staged.mutated_dir, &schema)
        .await
        .into_iter()
        .filter(|o| match &o.keys {
            None => false,
            Some(keys) => {
                !multiset::surplus(&multiset::multiset(keys.iter().cloned()), &control).is_empty()
            }
        })
        .map(|o| o.describe(&control))
        .collect();
    assert!(
        fabricating.is_empty(),
        "AC2: one corrupted header byte must never make a surface emit a partition the pristine \
         Cassandra fixture does not contain (the pre-fix one-byte resync landed on misaligned \
         bytes that parsed as a plausible header). Surfaces that FABRICATED:\n  {}",
        fabricating.join("\n  ")
    );
}

/// AC1 on the user-facing `SELECT` surface, materializing and streaming.
#[tokio::test]
async fn select_refuses_a_malformed_partition_header() {
    let spec = &BIG_COMPOSITE_HEADER;
    let staged = fixture::stage_spec(spec, &fixture_dir(spec), "sel-big");
    let sql = format!("SELECT * FROM {}.{}", spec.keyspace, spec.table);

    let control = open_db(spec, staged.control_root.clone())
        .await
        .execute(&sql)
        .await
        .expect("the pristine fixture must SELECT cleanly")
        .rows
        .len();
    assert!(
        control > 0,
        "0-rows-when-present: the control SELECT must return rows"
    );

    let db = open_db(spec, staged.mutated_root.clone()).await;
    match db.execute(&sql).await {
        Err(e) => assert_corruption_kind(&e, "Database::execute"),
        Ok(r) => panic!(
            "a malformed partition header must REFUSE, not resync: `SELECT` answered Ok with {} \
             rows against a control of {control} — and a count at or ABOVE the control is the \
             fabrication shape, not evidence of health",
            r.rows.len()
        ),
    }

    let cfg = cqlite_core::query::result::StreamingConfig {
        buffer_size: 8,
        ..Default::default()
    };
    let mut saw_error = false;
    let mut ok_rows = 0usize;
    match db.execute_streaming(&sql, cfg).await {
        Err(_) => saw_error = true,
        Ok(mut it) => {
            while let Some(item) = it.next_async().await {
                match item {
                    Ok(_) => ok_rows += 1,
                    Err(_) => saw_error = true,
                }
            }
        }
    }
    assert!(
        saw_error,
        "the streaming SELECT must surface the header decode error; it silently yielded \
         {ok_rows} rows against a control of {control}"
    );
}

/// AC1 on BTI (`da`) — a DIFFERENT corruption class through a DIFFERENT route.
///
/// `da` carries `hasUIntDeletionTime`, so its partition-level `DeletionTime` HAS
/// an invalid encoding (`DeletionTime.java:222-230` throws on it) and the header
/// reaches CQLite's `parse_partition_header_full` **error** arm rather than the
/// key-length **validation** arm the BIG lane exercises. The route differs too:
/// a `da` full scan stitches the whole data section in
/// `bti_scan_with_metadata_cancellable`, where BIG goes through
/// `sequential_scan`.
#[tokio::test]
async fn bti_scan_refuses_a_partition_header_cassandra_itself_rejects() {
    let spec = &BTI_MULTICLUSTERING_HEADER;
    let staged = fixture::stage_spec(spec, &fixture_dir(spec), "hdr-bti");
    let schema = table_schema(spec);

    let control = control_keys(&staged.control_dir, spec).await;
    let control_observed = observe(&staged.control_dir, &schema).await;
    assert_control_is_healthy(&control_observed, &control, spec);

    let mutated = observe(&staged.mutated_dir, &schema).await;
    let tolerated: Vec<String> = mutated
        .iter()
        .filter(|o| o.keys.is_some())
        .map(|o| o.describe(&control))
        .collect();
    assert!(
        tolerated.is_empty(),
        "AC1 (BTI): the partition-level DeletionTime discriminator at decompressed offset {} is \
         0xFF, which Cassandra's own DeletionTime.Serializer.deserialize throws on, so every \
         surface must REFUSE. {} of {} surfaces still answered Ok:\n  {}",
        staged.mutated_offset,
        tolerated.len(),
        mutated.len(),
        tolerated.join("\n  ")
    );

    let sql = format!("SELECT * FROM {}.{}", spec.keyspace, spec.table);
    let control_rows = open_db(spec, staged.control_root.clone())
        .await
        .execute(&sql)
        .await
        .expect("the pristine BTI fixture must SELECT cleanly")
        .rows
        .len();
    assert!(
        control_rows > 0,
        "0-rows-when-present: the BTI control SELECT must return rows"
    );
    match open_db(spec, staged.mutated_root.clone())
        .await
        .execute(&sql)
        .await
    {
        Err(e) => assert_corruption_kind(&e, "BTI Database::execute"),
        Ok(r) => panic!(
            "the BTI SELECT must REFUSE a header Cassandra rejects: got Ok with {} rows against \
             a control of {control_rows}",
            r.rows.len()
        ),
    }
}

/// AC3 — the NEGATIVE CONTROL for the two SLIDING-DRIVER arms this change
/// touches, over the whole discovered corpus.
///
/// `issue_3782_corrupt_row_refusal.rs`'s corpus case calls
/// `iterate_all_partitions` and `get_all_entries` and explicitly does NOT call
/// either `*_for_compaction` surface — so the header arms in
/// `drive_partition_sliding` and `stream_partition_body_incremental`, whose
/// discriminator is `at_final_chunk`, had no well-formed-corpus control at all.
/// This case adds it: on a well-formed table those arms must never fire at the
/// final chunk, so nothing may start refusing.
///
/// The subject set is the UNION of every candidate root, deduplicated by table
/// identity, with each table's bytes read from the root that actually carries
/// them (#3220/#3104), and the committed subjects in [`MUST_RUN`] are asserted
/// PER CASE so this cannot pass by omission.
#[tokio::test]
async fn corpus_wide_well_formed_tables_still_compact_without_refusal() {
    let config = cqlite_core::Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let mut scanned_tables: BTreeSet<(String, String)> = BTreeSet::new();
    let mut scanned_sstables = 0usize;
    let mut with_rows = 0usize;

    for (keyspace, table) in corpus_table_identities() {
        for data in data_files_for_table(&keyspace, &table) {
            let Ok(reader) = SSTableReader::open(&data, &config, platform.clone()).await else {
                // Unopenable (an out-of-scope format, a sidecar-only fixture):
                // not a subject of this lane. A MUST_RUN table that never opens
                // is caught by the per-case assertion below.
                continue;
            };
            scanned_sstables += 1;
            scanned_tables.insert((keyspace.clone(), table.clone()));
            // No schema is threaded in: these surfaces take `Option<&TableSchema>`
            // and the corpus spans 100+ tables. `None` still drives the partition
            // HEADER walk — the subject of this issue — through both drivers.
            let keys = reader.distinct_partition_keys().await.unwrap_or_else(|e| {
                panic!("#3928 regression: well-formed {data:?} now REFUSES the stitched partition-key walk: {e}")
            });
            let buffered = reader
                .iterate_all_partitions_for_compaction(None)
                .await
                .unwrap_or_else(|e| {
                    panic!("#3928 regression: well-formed {data:?} now REFUSES buffered compaction: {e}")
                });
            let cancel = cqlite_core::storage::scan_cancel::ScanCancel::new();
            let mut streamed = 0usize;
            reader
                .stream_all_partitions_for_compaction(None, &cancel, |_row| {
                    streamed += 1;
                    Ok(std::ops::ControlFlow::Continue(()))
                })
                .await
                .unwrap_or_else(|e| {
                    panic!("#3928 regression: well-formed {data:?} now REFUSES streaming compaction: {e}")
                });
            if !keys.is_empty() || !buffered.is_empty() || streamed > 0 {
                with_rows += 1;
            }
        }
    }

    for (keyspace, table) in MUST_RUN {
        assert!(
            scanned_tables.contains(&((*keyspace).to_string(), (*table).to_string())),
            "must_run corpus subject {keyspace}.{table} was never scanned; {}",
            datasets_root::describe_search(keyspace, table)
        );
    }
    assert!(
        scanned_tables.len() >= 20,
        "case floor: expected a real corpus, scanned only {} tables ({} SSTables) ({})",
        scanned_tables.len(),
        scanned_sstables,
        datasets_root::describe_roots()
    );
    assert!(
        with_rows > 0,
        "0-rows-when-present: {} tables / {scanned_sstables} SSTables scanned and none yielded a \
         partition",
        scanned_tables.len()
    );
}

/// Committed/required corpus subjects the corpus case MUST scan — fail-closed
/// unconditionally, never a skip (#3220). Same pair, and for the same reason, as
/// `issue_3782_corrupt_row_refusal.rs`: neither candidate root is a superset of
/// the other, so the scan takes their union and resolves each table by evidence.
const MUST_RUN: &[(&str, &str)] = &[
    ("test_da", "multiclustering_table"),
    ("test_basic", "composite_key_table"),
];

/// Every `(keyspace, table)` identity that carries a real `*-Data.db` under ANY
/// candidate root — the UNION, deduplicated by identity. A `break` on the first
/// non-empty root is a PREFERENCE ORDERING and misses the checkout-only tables
/// (#3220).
fn corpus_table_identities() -> BTreeSet<(String, String)> {
    let mut ids: BTreeSet<(String, String)> = BTreeSet::new();
    for root in datasets_root::sstables_root_candidates() {
        let Ok(keyspaces) = std::fs::read_dir(&root) else {
            continue;
        };
        for ks in keyspaces.flatten() {
            if !ks.path().is_dir() {
                continue;
            }
            let keyspace = ks.file_name().to_string_lossy().to_string();
            let Ok(tables) = std::fs::read_dir(ks.path()) else {
                continue;
            };
            for table in tables.flatten() {
                if !table.path().is_dir() {
                    continue;
                }
                let dir_name = table.file_name().to_string_lossy().to_string();
                // `<table>-<generation uuid>`; a CQL table name cannot contain
                // `-`, so the last separator is the generation boundary.
                let Some((name, _generation)) = dir_name.rsplit_once('-') else {
                    continue;
                };
                if datasets_root::table_has_data(&root, &keyspace, name) {
                    ids.insert((keyspace.clone(), name.to_string()));
                }
            }
        }
    }
    ids
}

/// Every `*-Data.db` of `<keyspace>.<table>` under the root that EVIDENCE picks
/// for that table (`sstables_root_for_table`, the sanctioned per-table resolver),
/// across all of that table's generation directories.
fn data_files_for_table(keyspace: &str, table: &str) -> Vec<PathBuf> {
    let Some(root) = datasets_root::sstables_root_for_table(keyspace, table) else {
        return Vec::new();
    };
    let mut out: Vec<PathBuf> = Vec::new();
    for dir in datasets_root::table_generation_dirs(&root, keyspace, table) {
        if let Ok(rd) = std::fs::read_dir(&dir) {
            for f in rd.flatten() {
                let p = f.path();
                if p.to_string_lossy().ends_with("-Data.db") {
                    out.push(p);
                }
            }
        }
    }
    out.sort();
    out
}

/// Assert the refusal's KIND, not a message substring.
///
/// A message check stays green through a refactor that re-wraps the decode error
/// in a different variant while forwarding the text, which is the no-heuristics
/// shape: it reads bytes of a rendered string instead of the authoritative
/// discriminant.
fn assert_corruption_kind(e: &cqlite_core::Error, surface: &str) {
    assert!(
        matches!(e, cqlite_core::Error::Corruption(_)),
        "{surface}: the refusal must carry the header decode error's own kind \
         (Error::Corruption), not a re-wrapped generic; got {e:?}"
    );
}

/// TEMPORARY diagnostic — not committed.
#[tokio::test]
#[ignore]
async fn diag_measure() {
    for spec in [&BIG_COMPOSITE_HEADER, &BTI_MULTICLUSTERING_HEADER] {
        let staged = fixture::stage_spec(spec, &fixture_dir(spec), "diag");
        let schema = table_schema(spec);
        println!(
            "\n=== {}.{} mutated_offset={} span={}",
            spec.keyspace, spec.table, staged.mutated_offset, staged.mutated_span
        );
        let sink = LogSink::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(sink.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        let ctl = observe(&staged.control_dir, &schema).await;
        let mut_out = {
            let _d = tracing::subscriber::set_default(subscriber);
            observe_verbose(&staged.mutated_dir, &schema).await
        };
        for (c, m) in ctl.iter().zip(mut_out.iter()) {
            let cn = c.keys.as_ref().map(|k| k.len());
            println!("  {:42} control={:?}  mutated={}", c.name, cn, m);
        }
        let logs = sink.text();
        for line in logs.lines().take(20) {
            println!("  WARN> {line}");
        }
    }
}

#[derive(Clone, Default)]
struct LogSink(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
impl std::io::Write for LogSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("m").extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogSink {
    type Writer = LogSink;
    fn make_writer(&'a self) -> LogSink {
        self.clone()
    }
}
impl LogSink {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("m").clone()).to_string()
    }
}

async fn observe_verbose(dir: &Path, schema: &TableSchema) -> Vec<String> {
    let reader = open_reader(dir).await;
    let mut v = Vec::new();
    v.push(match reader.distinct_partition_keys().await {
        Ok(k) => format!("Ok({} keys)", k.len()),
        Err(e) => format!("Err({e})"),
    });
    v.push(match reader.partition_verify_scan().await {
        Ok(k) => format!("Ok({} parts)", k.len()),
        Err(e) => format!("Err({e})"),
    });
    v.push(match reader.get_all_entries().await {
        Ok(k) => format!("Ok({} rows)", k.len()),
        Err(e) => format!("Err({e})"),
    });
    v.push(match reader.iterate_all_partitions().await {
        Ok(k) => format!("Ok({} rows)", k.len()),
        Err(e) => format!("Err({e})"),
    });
    v.push(
        match reader
            .iterate_all_partitions_for_compaction(Some(schema))
            .await
        {
            Ok(k) => format!("Ok({} rows)", k.len()),
            Err(e) => format!("Err({e})"),
        },
    );
    let cancel = cqlite_core::storage::scan_cancel::ScanCancel::new();
    let mut n = 0usize;
    v.push(
        match reader
            .stream_all_partitions_for_compaction(Some(schema), &cancel, |_r| {
                n += 1;
                Ok(std::ops::ControlFlow::Continue(()))
            })
            .await
        {
            Ok(()) => format!("Ok({n} rows)"),
            Err(e) => format!("Err({e}) after {n}"),
        },
    );
    v
}
