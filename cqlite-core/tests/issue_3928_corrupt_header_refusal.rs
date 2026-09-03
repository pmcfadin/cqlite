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
//! # Pre-fix measurement
//!
//! Measured by running THIS file against `main` @ 05134c947 (i.e. with only the
//! src half of the fix reverted), `CQLITE_DATASETS_ROOT=/data/datasets`.
//!
//! On the BTI (`da`) fixture — the discriminator flipped to `0xFF` — **all six
//! surfaces answered `Ok`**:
//!
//! | surface                                   | control | before the fix |
//! |-------------------------------------------|---------|----------------|
//! | `distinct_partition_keys`                 | 3 keys  | `Ok`, **5** keys — 1 real key LOST, 3 FABRICATED |
//! | `partition_verify_scan`                   | 3       | `Ok`, **5** — same split |
//! | `get_all_entries`                         | 468 rows| `Ok`, **0** — the whole table, silently |
//! | `iterate_all_partitions`                  | 468     | `Ok`, **0** |
//! | `iterate_all_partitions_for_compaction`   | 468     | `Ok`, **404** — 180 rows LOST, 116 FABRICATED |
//! | `stream_all_partitions_for_compaction`    | 468     | `Ok`, **404** — same split |
//!
//! The fabricated keys are byte strings like `"bos8-p1bos8-p1…"` — clustering
//! and payload bytes read as a partition key, i.e. partitions the SSTable does
//! not contain. Compaction would have written that back to disk.
//!
//! On the BIG (`nb`) fixture the pre-fix surfaces already answered `Err`, but
//! INCIDENTALLY: the header arm resynchronised and #3782's ROW arm then refused
//! the garbage it landed on. That is why the BIG case asserts the ABSENCE of the
//! resync WARN as well as the refusal — see its own comment.
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

/// Assert that EVERY surface answered on the well-formed control leg, and agreed
/// on WHICH partitions the fixture holds — so "the mutated leg refused" can
/// never be a surface that refuses on healthy data too, and the per-surface
/// controls the fabrication check compares against are themselves sound.
///
/// The comparison is over the DISTINCT key set, not the multiset: these surfaces
/// have deliberately different granularities (`distinct_partition_keys` emits
/// one entry per PARTITION, `get_all_entries` one per ROW), so their
/// multiplicities legitimately differ — measured on the BTI fixture, 3 vs 468.
/// The per-surface multiset is still the oracle for FABRICATION below; it is
/// just compared against the SAME surface on the pristine leg.
fn assert_control_is_healthy(
    observed: &[Outcome],
    control_partitions: &std::collections::BTreeMap<Vec<u8>, usize>,
    spec: &FixtureSpec,
) {
    let expected: BTreeSet<&Vec<u8>> = control_partitions.keys().collect();
    for o in observed {
        let keys = o.keys.as_ref().unwrap_or_else(|| {
            panic!(
                "control leg: {}.{} surface `{}` REFUSED a PRISTINE Cassandra fixture — the \
                 mutated-leg expectations below would be meaningless",
                spec.keyspace, spec.table, o.name
            )
        });
        assert!(
            !keys.is_empty(),
            "0-rows-when-present: {}.{} surface `{}` answered Ok with NOTHING on a pristine \
             fixture",
            spec.keyspace,
            spec.table,
            o.name
        );
        let got: BTreeSet<&Vec<u8>> = keys.iter().collect();
        assert_eq!(
            got.len(),
            expected.len(),
            "control leg: {}.{} surface `{}` sees {} distinct partition key(s) where \
             `distinct_partition_keys` sees {} — the surfaces disagree about the PRISTINE \
             fixture, so neither can be an oracle for the mutated one",
            spec.keyspace,
            spec.table,
            o.name,
            got.len(),
            expected.len()
        );
        assert!(
            got == expected,
            "control leg: {}.{} surface `{}` reports a DIFFERENT set of partition keys than \
             `distinct_partition_keys` on the PRISTINE fixture",
            spec.keyspace,
            spec.table,
            o.name
        );
    }
}

/// The surfaces of the mutated leg that still answered `Ok`, described against
/// the control leg's SAME-SURFACE result.
///
/// Pairing surface-with-itself is what makes a fabrication verdict meaningful:
/// these surfaces emit at different granularities, so a cross-surface multiset
/// comparison would report hundreds of spurious "surplus" occurrences (measured:
/// 465 on the BTI fixture, from `get_all_entries`' per-ROW emit against a
/// per-PARTITION control).
fn tolerating(control: &[Outcome], mutated: &[Outcome]) -> Vec<String> {
    assert_eq!(
        control.len(),
        mutated.len(),
        "the two legs must be observed through the same surface list"
    );
    control
        .iter()
        .zip(mutated.iter())
        .filter(|(_, m)| m.keys.is_some())
        .map(|(c, m)| {
            let ctl = c
                .keys
                .as_ref()
                .map(|k| multiset::multiset(k.iter().cloned()))
                .unwrap_or_default();
            m.describe(&ctl)
        })
        .collect()
}

/// The surfaces of the mutated leg that FABRICATED — answered `Ok` carrying a
/// partition-key occurrence the SAME surface did not produce on the pristine
/// fixture.
fn fabricating(control: &[Outcome], mutated: &[Outcome]) -> Vec<String> {
    assert_eq!(
        control.len(),
        mutated.len(),
        "the two legs must be observed through the same surface list"
    );
    control
        .iter()
        .zip(mutated.iter())
        .filter_map(|(c, m)| {
            let keys = m.keys.as_ref()?;
            let ctl = c
                .keys
                .as_ref()
                .map(|k| multiset::multiset(k.iter().cloned()))
                .unwrap_or_default();
            let got = multiset::multiset(keys.iter().cloned());
            multiset::surplus(&got, &ctl)
                .is_empty()
                .then_some(())
                .map_or_else(|| Some(m.describe(&ctl)), |()| None)
        })
        .collect()
}

/// Capture WARN-and-above tracing output into a shared buffer, so a case can
/// assert what a code path did NOT log.
#[derive(Clone, Default)]
struct LogSink(Arc<std::sync::Mutex<Vec<u8>>>);
impl std::io::Write for LogSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .expect("log sink mutex")
            .extend_from_slice(buf);
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
        String::from_utf8_lossy(&self.0.lock().expect("log sink mutex").clone()).to_string()
    }
}

/// The two WARNs the pre-#3928 block-emit header arm emitted immediately before
/// `offset += 1`, verbatim from
/// `block_emit_windowed/partition_header_arm.rs`. Since the fix they are
/// reachable ONLY on a `BufferExtent::Window`, so their presence on a
/// full-extent walk IS the resync.
const RESYNC_WARNS: &[&str] = &[
    "Skipping malformed partition header",
    "Failed to parse partition header",
];

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

    let control_partitions = control_keys(&staged.control_dir, spec).await;
    let control_observed = observe(&staged.control_dir, &schema).await;
    assert_control_is_healthy(&control_observed, &control_partitions, spec);

    // The refusal is asserted TOGETHER with the absence of the resync, because on
    // THIS fixture the refusal alone does not pin #3928: measured pre-fix, all six
    // surfaces already answered `Err` here — but with a ROW decode error
    // (`Clustering 'clustering_key_1': invalid UTF-8`) raised by #3782's arm AFTER
    // the header arm had resynchronised onto misaligned bytes. That green was
    // incidental: it depended on the garbage the resync landed on failing later.
    // So this case also requires the resync itself not to have happened — which
    // pre-fix it demonstrably did, twice, and which no surface in this set may do,
    // since every one of them walks a full extent.
    let sink = LogSink::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(sink.clone())
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .finish();
    // A THREAD-LOCAL dispatcher (`set_default`), never a global one: sibling cases
    // in this target walk the whole corpus on other threads and a global sink
    // would mix their WARNs in.
    let mutated = {
        let _dispatch = tracing::subscriber::set_default(subscriber);
        observe(&staged.mutated_dir, &schema).await
    };
    let logs = sink.text();
    for needle in RESYNC_WARNS {
        assert!(
            !logs.contains(needle),
            "AC1: a full-extent walk RESYNCHRONISED past the corrupt header — it logged \
             {needle:?} and skipped a byte, which both drops this partition and can invent \
             another out of misaligned bytes. On a proven-complete buffer that arm must \
             REFUSE. Captured WARN output was:\n{logs}"
        );
    }

    let tolerated = tolerating(&control_observed, &mutated);
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
    // BOTH fixtures, because the pre-fix fabrication was MEASURED on the BTI one
    // (`distinct_partition_keys` answered Ok with 5 keys where the fixture holds
    // 3) while the BIG one lost partitions instead. A case that covered only the
    // fixture where the harm was loss would assert this property where it was
    // never violated.
    let mut fabricated: Vec<String> = Vec::new();
    for (spec, tag) in [
        (&BIG_COMPOSITE_HEADER, "fab-big"),
        (&BTI_MULTICLUSTERING_HEADER, "fab-bti"),
    ] {
        let staged = fixture::stage_spec(spec, &fixture_dir(spec), tag);
        let schema = table_schema(spec);
        let control_partitions = control_keys(&staged.control_dir, spec).await;
        let control_observed = observe(&staged.control_dir, &schema).await;
        assert_control_is_healthy(&control_observed, &control_partitions, spec);
        let mutated = observe(&staged.mutated_dir, &schema).await;
        fabricated.extend(
            fabricating(&control_observed, &mutated)
                .into_iter()
                .map(|d| format!("{}.{}: {d}", spec.keyspace, spec.table)),
        );
    }
    let fabricating = fabricated;
    assert!(
        fabricating.is_empty(),
        "AC2: one corrupted header byte must never make a surface emit a partition the pristine \
         Cassandra fixture does not contain (the pre-fix one-byte resync landed on misaligned \
         bytes that parsed as a plausible header). Surfaces that FABRICATED:\n  {}",
        fabricating.join("\n  ")
    );
}

/// AC1 on the user-facing `SELECT` surface, materializing and streaming.
///
/// SCOPE, stated because a green run here must not be read as this issue's pin:
/// measured pre-fix, `SELECT` ALREADY refused on this fixture — the header arm
/// resynchronised and #3782's ROW arm then refused the garbage it landed on. So
/// this is a REGRESSION guard for the surface a user actually calls; the
/// evidence for #3928 is the resync-absence assertion in
/// `every_proven_complete_surface_refuses_a_malformed_partition_header` and the
/// two BTI-covering cases, all three of which FAIL pre-fix.
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

    let control_partitions = control_keys(&staged.control_dir, spec).await;
    let control_observed = observe(&staged.control_dir, &schema).await;
    assert_control_is_healthy(&control_observed, &control_partitions, spec);

    let mutated = observe(&staged.mutated_dir, &schema).await;
    let tolerated = tolerating(&control_observed, &mutated);
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
///
/// # AC3, measured rather than argued (2026-09-03, `CQLITE_DATASETS_ROOT=/data/datasets`)
///
/// Counters were planted at every tolerance site — the three ROW arms and the
/// five HEADER arms — and the whole corpus was walked on `iterate_all_partitions`,
/// `get_all_entries` and both `*_for_compaction` surfaces, on the PRE-fix tree
/// and on the POST-fix tree. **The two runs are identical:** 126 tables / 148
/// SSTables / 71313 emitted elements, **542** mid-stream row tolerations (all on
/// the streaming compaction driver, all at `at_final_chunk == false`), **0**
/// firings at ANY header arm in either direction, and **0** errors.
///
/// So the header-arm refusal costs nothing on well-formed input, and the row
/// arm's toleration count did not move — which is AC3's property. The
/// instrumentation was temporary and is not committed; re-take the measurement
/// the same way if the number is ever in doubt.
///
/// #3782 records this figure as **614** over "42 well-formed corpus tables
/// (10913 rows)". That is a DIFFERENT measurement — a different surface set on a
/// smaller subject set — not a regression: 542 is what these four surfaces fire
/// on this 126-table corpus, and it is the SAME 542 before and after, which is
/// the only comparison that can answer "did this change move it".
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
