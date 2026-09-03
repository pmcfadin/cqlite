//! Issue #3782 — a row that cannot be decoded at the FINAL chunk is DATA LOSS,
//! so every surface must REFUSE rather than return a short (or, on compaction, a
//! *longer but wrong*) result.
//!
//! # The defect
//!
//! `SlidingPartitionPolicy::on_data_row` returned `Option<usize>`, collapsing
//! every decode error into "the row did not parse". The driver then treated that
//! as end-of-partition. Measured on a REAL Cassandra fixture with ONE byte of a
//! `text` clustering value flipped (see `support/corrupt_byte_fixture.rs`):
//!
//! | surface                                  | control | before the fix |
//! |------------------------------------------|---------|----------------|
//! | `Database::execute`                      | 100     | `Ok`, 23 rows  |
//! | `Database::execute_streaming`            | 100     | `Ok`, 23 rows  |
//! | `iterate_all_partitions_for_compaction`  | 100     | `Ok`, **102** rows — 2 partition keys LOST, 3 FABRICATED |
//! | `stream_all_partitions_for_compaction`   | 100     | `Ok`, 102 rows |
//!
//! Those four numbers were measured on the BIG fixture with the byte flipped at
//! the mutation site this harness still pins (`flip_offset_in_needle: 0`), so
//! they stay reproducible; the BTI row of the story is the separate case below,
//! measured at 120 of 468.
//!
//! The compaction number is the dangerous one: the row COUNT goes UP while real
//! data is lost, so no count-based check can see it, and compaction would write
//! that loss back to disk permanently.
//!
//! # The discriminator, and why it is not a heuristic
//!
//! `at_final_chunk` is an authoritative property of the sliding-window driver:
//! at the final chunk no further bytes can arrive, so a decode error there can
//! never be a row straddling a chunk boundary — it is truncation or corruption,
//! and both are data loss. Mid-stream the SAME error is a legitimate straddling
//! row and stays tolerant (`NeedMore`, refill, re-parse). Measured across 42
//! well-formed corpus tables (10913 rows) the tolerant path fires 614 times,
//! **100% of them at `at_final_chunk == false`, ZERO at `true`** — which is what
//! `corpus_wide_well_formed_tables_still_decode_without_refusal` below guards.
//!
//! # Oracle (#3042)
//!
//! Every expectation is derived from Cassandra-written bytes: the control leg is
//! the untouched fixture, and the mutated leg differs from it by exactly one
//! decompressed byte. No CQLite-written SSTable is involved, so a uniform
//! framing mistake could not make this pass.
//!
//! # DECLARED GAP — AC2 SPLITS: half (a) is delivered, half (b) is #3949
//!
//! AC2 has two halves, and only the first is deliverable inside #3782:
//!
//! - **(a) "the error surfaces rather than truncating" — DELIVERED**, and
//!   pinned here on every read surface: `Database::execute`,
//!   `Database::execute_streaming`, and (the route the issue is titled for)
//!   `SSTableReader::iterate_all_partitions` over the index-random-read path.
//! - **(b) "no WARN-and-fall-back-to-sequential-scan detour" — NOT DELIVERED,
//!   and not deliverable in this issue.** On the index-random-read route the
//!   refusal arrives only AFTER issue #2302's Signal-B detour: the materialising
//!   arm answers a corrupt partition body with `Ok(None)` — that issue's
//!   DESIGNATED routing for this exact condition — so the caller WARNs and
//!   re-walks Data.db sequentially before the `Err` finally surfaces. Removing
//!   the detour means changing #2302's routing in `partition_lookup.rs` /
//!   `full_index_scan.rs`: a src change in another subsystem, out of scope for
//!   #3782. Tracked as **#3949**.
//!
//! `index_random_read_path_refuses_a_corrupt_row_carrying_the_decode_errors_kind`
//! below therefore asserts the detour WARN's PRESENCE, so half (b) cannot be
//! silently lost behind a green suite; **flipping that assertion to `!contains`
//! is #3949's completion signal**. Two further residuals of this change are
//! declared in the differential lane
//! (`point_vs_full_differential/issue_3782_corrupt_agreement.rs`): the point
//! read path (#3922) and the partition-HEADER resync arm (#3928).
#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    // EVERY fixture this target reads is LZ4-compressed (the harness ASSERTS it
    // before mutating), and without the `lz4` feature the production
    // decompressor answers `Err("LZ4 compression not available")`
    // (`storage/sstable/compression.rs`) — so with only the two features above
    // the target still RAN and failed on its PRISTINE CONTROLS, a false FAIL
    // presenting as a correctness failure (roborev job 59 finding 2, #3950).
    // `Cargo.toml`'s `required-features` for this target must always agree with
    // this list.
    feature = "lz4"
))]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::platform::Platform;
use cqlite_core::query::result::StreamingConfig;
use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::Database;

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/corrupt_byte_fixture.rs"]
mod fixture;
#[path = "support/multiset.rs"]
mod multiset;

use fixture::{comp_file, FixtureSpec, BIG_COMPOSITE, BTI_MULTICLUSTERING, FIX_KS, FIX_TABLE};

/// The fixture's GENERATION directory, resolved per TABLE (#3220) so a root that
/// holds the keyspace but not this table cannot silently win the selection. The
/// two specs live in DIFFERENT roots on a fleet box — the BTI one is
/// git-committed in the checkout, the BIG one is fetched-corpus-only — so
/// evidence, never a preference order, decides (#3104).
///
/// Delegated to [`datasets_root::resolve_table_generation_dir`], which REQUIRES a
/// `*-Data.db` in the chosen directory and picks deterministically rather than
/// taking the first `read_dir` hit (roborev job 57 finding 2 — the checkout's
/// copy of THIS BIG fixture's directory holds sidecars only, so an
/// order-dependent selection can bind to an unusable generation). "Not found" is
/// a loud named panic here, never a skip.
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

fn table_schema(spec: &FixtureSpec) -> cqlite_core::schema::TableSchema {
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
    SSTableReader::open(&comp_file(dir, "-Data.db"), &config, platform)
        .await
        .expect("open SSTableReader")
}

/// AC2 — the READ path refuses the corrupt fixture instead of silently returning
/// 23 of 100 rows, on BOTH the materializing and the streaming surface.
#[tokio::test]
async fn read_path_refuses_a_corrupt_row_instead_of_truncating() {
    let staged = fixture::stage_control_and_mutated(&fixture_dir(&BIG_COMPOSITE), "read");
    let sql = format!("SELECT * FROM {FIX_KS}.{FIX_TABLE}");

    let control = open_db(&BIG_COMPOSITE, staged.control_root.clone())
        .await
        .execute(&sql)
        .await
        .expect("the pristine fixture must read cleanly")
        .rows
        .len();
    assert!(
        control > 0,
        "0-rows-when-present: the control read must return rows"
    );

    let db = open_db(&BIG_COMPOSITE, staged.mutated_root.clone()).await;
    match db.execute(&sql).await {
        Err(_) => {}
        Ok(r) => panic!(
            "a corrupt clustering value must REFUSE, not truncate: got Ok with {} of {control} \
             rows (before #3782 this was Ok/23)",
            r.rows.len()
        ),
    }

    let cfg = StreamingConfig {
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
        "the streaming read must surface the decode error; it silently yielded {ok_rows} of \
         {control} rows"
    );
}

/// Capture WARN-and-above tracing output into a shared buffer, so a test can
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

/// The #2302 index-random-read detour WARN, verbatim from
/// `partition_lookup.rs`'s `iterate_all_partitions_cancellable`.
const INDEX_DETOUR_WARN: &str = "falling back to a full sequential scan";

/// AC2 on the **index-random-read** path — the route the issue is TITLED for,
/// and the one route no other case in this file reaches.
///
/// `read_path_refuses_a_corrupt_row_instead_of_truncating` above goes through
/// `Database::execute` → `scan_inner` → `sequential_scan` → the STITCHED
/// `parse_block`; the compaction cases go through the `*_for_compaction`
/// surfaces. Neither reaches `SSTableReader::iterate_all_partitions` →
/// `iterate_all_partitions_via_full_index` (`full_index_scan.rs`), which has no
/// in-crate caller outside `data_access`/`partition_lookup` — so before this
/// case the issue's headline route had no committed coverage at all.
///
/// # What this pins, and what it CANNOT pin (measured, 2026-09-02)
///
/// It pins the refusal: `iterate_all_partitions` on the corrupt fixture answers
/// `Err(Error::Corruption)` and never an `Ok` with a short partition set
/// (pre-fix on this fixture: `Ok`, 23 of 100).
///
/// It does NOT pin the `BufferExtent::Complete` parse this change added at
/// `full_index_scan.rs:311`. **That line is not what makes this test pass:
/// reverting it to `BufferExtent::Window` does NOT red this case** (measured
/// 2026-09-02), so a green run here may never be read as that line's pin.
///
/// The reason is a property of the route rather than of this test: the
/// per-entry coverage oracle `partition_slice_fully_consumed` runs FIRST and
/// already answers `false` for the corrupt partition (its structure-only drive
/// stops at the undecodable clustering value, so `consumed < raw.len()`), so
/// the walk returns `Ok(None)` BEFORE reaching the parse below it — line 311 is
/// UNREACHABLE with a decode error. No corrupt fixture available here can reach
/// it either, since any decode error that would fire there also stops the
/// structure-only drive one step earlier.
///
/// # DECLARED GAP — AC2 half (b): the refusal arrives via the #2302 Signal-B detour (#3949)
///
/// AC2's second half ("no WARN-and-fall-back-to-sequential-scan detour") is
/// NOT satisfied on this route, and it is not an oversight in the #3782 fix:
/// the `Ok(None)` above is issue #2302's **Signal B**, whose exhaustive exit
/// table (`full_index_scan.rs` module doc, roborev jobs 1609/1610) DESIGNATES
/// "a corrupt/truncated partition body" as an `incomplete Ok(None)` that the
/// caller reports LOUDLY and then re-walks sequentially. What #3782 changed is
/// the DESTINATION: that sequential re-walk now REFUSES instead of returning 23
/// rows.
///
/// The route measured end to end, which is the behaviour this case composes:
/// `partition_slice_fully_consumed` ⇒ `Ok(None)` (`full_index_scan.rs`) → the
/// detour WARN at `partition_lookup.rs:671` → a full sequential re-walk of
/// Data.db → `Err`, kind preserved.
///
/// So half (a) of AC2 — "the error surfaces rather than truncating" — is
/// delivered and pinned by the match below, and half (b) is neither delivered
/// nor deliverable in #3782: making the error surface directly means changing
/// #2302's documented Signal-B routing (the materialising arm's `Ok(None)`,
/// versus the streaming sibling's `Err::corruption` on the same predicate at
/// `full_index_stream.rs:356`), a src change in another subsystem. Tracked as
/// **#3949**.
///
/// The WARN's PRESENCE is therefore asserted rather than left unstated, because
/// a gap this file does not name is a gap the next reader cannot find. That
/// assertion pins the gap's PRESENCE only; **flipping it to `!contains` is
/// #3949's completion signal**, and this comment goes with it.
///
/// The capture is a THREAD-LOCAL dispatcher (`set_default`), never a global one:
/// the sibling corpus case in this target calls `iterate_all_partitions` over
/// the whole corpus on other threads, and a global sink would mix its WARNs in.
#[tokio::test]
async fn index_random_read_path_refuses_a_corrupt_row_carrying_the_decode_errors_kind() {
    let staged = fixture::stage_control_and_mutated(&fixture_dir(&BIG_COMPOSITE), "indexwalk");

    // Route precondition, as an ON-DISK fact: a BIG SSTable carrying an
    // `Index.db` and NO `Partitions.db` is what routes `iterate_all_partitions`
    // into `iterate_all_partitions_via_full_index` rather than straight to the
    // sequential fallback.
    let components: Vec<String> = std::fs::read_dir(&staged.mutated_dir)
        .expect("read staged fixture dir")
        .flatten()
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert!(
        components.iter().any(|c| c.ends_with("-Index.db")),
        "the index-random-read route needs an Index.db; staged components: {components:?}"
    );
    assert!(
        !components.iter().any(|c| c.ends_with("-Partitions.db")),
        "this case pins the BIG index-random-read route; a Partitions.db would route it \
         through BTI instead: {components:?}"
    );

    let control = open_reader(&staged.control_dir).await;
    assert!(
        control.has_partition_index(),
        "the control reader must have loaded a random-access partition index, or this case \
         never reaches the index-random-read path at all"
    );
    let control_partitions = control
        .iterate_all_partitions()
        .await
        .expect("the pristine fixture must walk the index-random-read path cleanly");
    assert!(
        !control_partitions.is_empty(),
        "0-rows-when-present: the control index walk must yield partitions"
    );

    let sink = LogSink::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(sink.clone())
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .finish();
    let got = {
        let _dispatch = tracing::subscriber::set_default(subscriber);
        let mutated = open_reader(&staged.mutated_dir).await;
        mutated.iterate_all_partitions().await
    };

    match got {
        Err(e) => assert_corruption_kind(&e, "iterate_all_partitions (index-random-read)"),
        Ok(rows) => panic!(
            "the index-random-read walk must REFUSE a corrupt row: got Ok with {} of {} \
             partitions (before #3782 this was Ok/23)",
            rows.len(),
            control_partitions.len()
        ),
    }

    // The DECLARED GAP above (AC2 half (b), tracked as #3949), asserted so it
    // cannot be silently lost. This also proves the capture is non-vacuous: the
    // substring is the #2302 WARN this very route emits, so a sink that saw
    // nothing would red here. Flipping this to `!contains` is #3949's
    // completion signal.
    let logs = sink.text();
    assert!(
        logs.contains(INDEX_DETOUR_WARN),
        "declared gap #3949 (#2302 Signal B): today the index-random-read refusal arrives \
         AFTER the documented WARN + sequential re-walk, so that WARN must be observable \
         here. If it is gone, the routing changed — flip this assertion to `!contains` and \
         update the case doc, which is AC2's second half. Captured WARN output was:\n{logs}"
    );
}

/// AC6/AC8 — BOTH compaction surfaces refuse. Compaction is the surface that
/// would WRITE the loss back to disk, and before the fix it reported MORE rows
/// than the control while losing two real partitions and fabricating three.
#[tokio::test]
async fn compaction_refuses_a_corrupt_row_and_never_loses_or_fabricates_partitions() {
    let staged = fixture::stage_control_and_mutated(&fixture_dir(&BIG_COMPOSITE), "compact");
    let schema = table_schema(&BIG_COMPOSITE);

    let control_reader = open_reader(&staged.control_dir).await;
    let control_rows = control_reader
        .iterate_all_partitions_for_compaction(Some(&schema))
        .await
        .expect("the pristine fixture must compact cleanly");
    assert!(
        !control_rows.is_empty(),
        "0-rows-when-present: the control compaction must yield rows"
    );
    // MULTISET of partition keys, never a SET (roborev job 57 finding 1): the
    // #3782 shape is "count goes UP while data is lost", and one of the ways it
    // goes up is the header-arm resync RE-EMITTING a partition already emitted
    // (declared gap #3928). A set comparison cannot see a surplus duplicate of a
    // legitimate key, which is exactly the fabrication this case is named for.
    let control_counts = multiset::multiset(control_rows.iter().map(|r| r.key.as_bytes().to_vec()));

    // AC8: the well-formed partition set is unchanged, and the two compaction
    // surfaces still agree row-for-row on it — asserted as MULTISET equality of
    // the emitted keys, not as a row COUNT. A count can be equal while one key
    // is dropped and another duplicated, which is the very trade #3782 measured.
    //
    // Scope, stated because it is easy to overclaim: this (and the sibling
    // `stitched_verifier_scans_*` case) compares KEY MULTISETS, never BYTES. It
    // cannot evidence "well-formed compaction OUTPUT is byte-identical" — the
    // oracle for that claim is the gate's own `compaction-byte-parity`
    // component, which diffs CQLite-written compaction output against Apache
    // Cassandra's. Cite that component, not this case, for byte identity.
    let cancel = cqlite_core::storage::scan_cancel::ScanCancel::new();
    let mut streamed_keys: Vec<Vec<u8>> = Vec::new();
    control_reader
        .stream_all_partitions_for_compaction(Some(&schema), &cancel, |row| {
            streamed_keys.push(row.key.as_bytes().to_vec());
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .await
        .expect("the pristine fixture must stream-compact cleanly");
    let streamed_counts = multiset::multiset(streamed_keys.iter().cloned());
    assert_eq!(
        streamed_keys.len(),
        control_rows.len(),
        "the buffered and streaming compaction surfaces must agree on a well-formed fixture"
    );
    let stream_surplus = multiset::surplus(&streamed_counts, &control_counts);
    let stream_deficit = multiset::deficit(&streamed_counts, &control_counts);
    // Reported as bounded surplus/deficit lists rather than `assert_eq!` on the
    // two maps: an equality failure would dump a whole corpus of keys, and the
    // two lists ARE the difference a reader needs.
    assert!(
        stream_surplus.is_empty() && stream_deficit.is_empty(),
        "the two compaction surfaces must agree row-for-row on a well-formed fixture: \
         streaming has {} surplus key occurrence(s) [{}] and {} missing [{}] \
         (the row COUNTS above can be equal while one key is duplicated and another \
         dropped, which is why this is a multiset comparison)",
        stream_surplus.iter().map(|(_, n)| n).sum::<usize>(),
        multiset::describe(&stream_surplus),
        stream_deficit.iter().map(|(_, n)| n).sum::<usize>(),
        multiset::describe(&stream_deficit)
    );

    let mutated_reader = open_reader(&staged.mutated_dir).await;
    match mutated_reader
        .iterate_all_partitions_for_compaction(Some(&schema))
        .await
    {
        Err(_) => {}
        Ok(rows) => {
            // If it ever returns Ok again, it may NEVER be the #3782 shape: a
            // partition silently dropped, or one invented out of misaligned
            // bytes — INCLUDING a surplus duplicate of a real one, which the
            // multiset counts below report and a set difference would not.
            let counts = multiset::multiset(rows.iter().map(|r| r.key.as_bytes().to_vec()));
            let lost = multiset::deficit(&counts, &control_counts);
            let fabricated = multiset::surplus(&counts, &control_counts);
            panic!(
                "compaction must refuse a corrupt row: got Ok with {} rows (control {}), \
                 {} partition-key occurrence(s) LOST [{}] and {} FABRICATED [{}] \
                 (before #3782: 102 rows, 2 lost, 3 fabricated)",
                rows.len(),
                control_rows.len(),
                lost.iter().map(|(_, n)| n).sum::<usize>(),
                multiset::describe(&lost),
                fabricated.iter().map(|(_, n)| n).sum::<usize>(),
                multiset::describe(&fabricated)
            );
        }
    }

    let cancel = cqlite_core::storage::scan_cancel::ScanCancel::new();
    let mut emitted = 0usize;
    let streamed = mutated_reader
        .stream_all_partitions_for_compaction(Some(&schema), &cancel, |_row| {
            emitted += 1;
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .await;
    assert!(
        streamed.is_err(),
        "streaming compaction must refuse a corrupt row; it silently emitted {emitted} rows \
         against a control of {}",
        control_rows.len()
    );
}

/// Table identity: `(keyspace, table)` with the generation/uuid directory suffix
/// dropped, so the same table reachable from two candidate roots is ONE subject.
type TableId = (String, String);

/// Committed/required corpus subjects that MUST be scanned by the corpus case
/// below — fail-closed unconditionally, never a skip (#3220).
///
/// `test_da.multiclustering_table` is a git-COMMITTED `da`/BTI fixture
/// (`git ls-files` carries its `-Data.db`) and lives in the checkout corpus
/// ONLY: a fleet `CQLITE_DATASETS_ROOT` does not hold it. `test_basic
/// .composite_key_table` is the BIG subject of every other case in this file and
/// is the mirror image — a FETCHED fixture whose checkout directory holds
/// sidecars only. So neither candidate root is a superset of the other and any
/// fixed root preference misses one of the two (#3104), which is exactly why the
/// scan below takes their UNION and resolves each table by EVIDENCE.
///
/// Both are unconditional here even though only the first is committed: the AC2/
/// AC6/AC8 cases in this same target already hard-panic in `fixture_dir` when the
/// BIG fixture is absent, so this target cannot pass without it either way — a
/// conditional floor here could only hide a corpus that shrank, never enable a
/// configuration that legitimately lacks the fixture.
const MUST_RUN: &[(&str, &str)] = &[
    ("test_da", "multiclustering_table"),
    ("test_basic", "composite_key_table"),
];

/// Every `(keyspace, table)` identity that carries a real `*-Data.db` under ANY
/// candidate root — the UNION, deduplicated by identity.
///
/// Enumerating the union (rather than committing to the first root that yields
/// anything) is the #3220 rule: a `break` on the first non-empty root is a
/// PREFERENCE ORDERING, and measured on this box it binds to the fetched corpus
/// and never sees the three checkout-only tables at all, the BTI subject among
/// them.
fn corpus_table_identities() -> BTreeSet<TableId> {
    let mut ids: BTreeSet<TableId> = BTreeSet::new();
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
                // Presence is judged by an actual `*-Data.db` (the repo commits
                // JSONL sidecars for fixtures whose binaries are gitignored).
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
    // EVERY usable generation directory, deterministically ordered — this case
    // is the one that deliberately covers all of them, where the staging cases
    // above take the one defined generation.
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

/// AC3 — the NEGATIVE CONTROL, and the highest-value test in this change.
///
/// The fix must not convert a single legitimate MID-STREAM toleration into a
/// refusal. Across the whole discovered corpus every well-formed table must
/// still decode without error on the two surfaces this case actually calls —
/// `iterate_all_partitions` (the index/partition walk) and `get_all_entries`
/// (the block walk) — and must still yield rows. Neither is a `*_for_compaction`
/// entry point: the compaction surfaces are covered on the corrupt fixture by
/// `compaction_refuses_a_corrupt_row_and_never_loses_or_fabricates_partitions`
/// above, and on well-formed input by the gate's own `compaction-byte-parity`
/// component. On the measured corpus the tolerant path fires 614 times here; any
/// of them turning into an `Err` reds this test.
///
/// Row-count EQUALITY with the pre-change behaviour is covered by the corpus
/// parity suites the gate already runs (the sstabledump JSONL goldens and the
/// query-semantics oracle); what this lane adds is the property those cannot
/// express — that no well-formed table started REFUSING.
///
/// The subject set is the UNION of every candidate root, deduplicated by table
/// identity, with each table's bytes read from the root that actually carries
/// them (#3220/#3104) — and the committed subjects in [`MUST_RUN`] are asserted
/// PER CASE, so this cannot pass by omission the way a suite-wide floor can.
#[tokio::test]
async fn corpus_wide_well_formed_tables_still_decode_without_refusal() {
    let config = cqlite_core::Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let mut scanned_tables: BTreeSet<TableId> = BTreeSet::new();
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
            let partitions = reader.iterate_all_partitions().await.unwrap_or_else(|e| {
                panic!("#3782 regression: well-formed {data:?} now REFUSES the index walk: {e}")
            });
            let entries = reader.get_all_entries().await.unwrap_or_else(|e| {
                panic!("#3782 regression: well-formed {data:?} now REFUSES the block walk: {e}")
            });
            if !partitions.is_empty() || !entries.is_empty() {
                with_rows += 1;
            }
        }
    }

    // Per-CASE, fail-closed, unconditional: a committed fixture is source, so
    // "not found" is a hard failure and never a skip (#3220).
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

/// AC2 on the BTI (`da`) full scan — roborev job 48.
///
/// The BIG lane above exercises `sequential_scan` → `stitch_and_parse_all_chunks`,
/// which already declared its stitched buffer complete. A `da` reader takes a
/// DIFFERENT route to the same parse: `sequential_scan`/`get_all_entries` both
/// delegate to `bti_scan_with_metadata_cancellable`, which stitches the whole
/// data section itself and calls `parse_block_with_cell_metadata`. That call did
/// NOT state the buffer's extent, so it inherited the tolerant break and the
/// `da` scan kept silently truncating after the fix landed for BIG.
///
/// Observed RED before the fix: `get_all_entries` returned `Ok` with FEWER rows
/// than the control instead of surfacing the decode error.
#[tokio::test]
async fn bti_scan_refuses_a_corrupt_row_instead_of_truncating() {
    let spec = &BTI_MULTICLUSTERING;
    let staged = fixture::stage_spec(spec, &fixture_dir(spec), "bti");

    let control = open_reader(&staged.control_dir).await;
    let control_entries = control
        .get_all_entries()
        .await
        .expect("the pristine BTI fixture must read cleanly");
    assert!(
        !control_entries.is_empty(),
        "0-rows-when-present: the BTI control read must return rows"
    );

    let mutated = open_reader(&staged.mutated_dir).await;
    match mutated.get_all_entries().await {
        Err(e) => assert_corruption_kind(&e, "BTI get_all_entries"),
        Ok(rows) => panic!(
            "the BTI scan must REFUSE a corrupt row, not truncate: got Ok with {} of {} rows \
             (one compressed byte flipped; {} decompressed positions changed from offset {})",
            rows.len(),
            control_entries.len(),
            staged.mutated_span,
            staged.mutated_offset
        ),
    }

    // The user-facing SELECT surface takes the same route with
    // `read_shadowing = true`, so it must refuse too.
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
        Err(_) => {}
        Ok(r) => panic!(
            "the BTI SELECT must REFUSE a corrupt row: got Ok with {} of {control_rows} rows",
            r.rows.len()
        ),
    }
}

/// AC6 on the STITCHED compaction/verifier entry points.
///
/// `distinct_partition_keys` and `partition_verify_scan` do not use the
/// per-chunk sliding stream the two `*_for_compaction` surfaces use: each
/// stitches the whole data section and parses it in one shot. Their refusal
/// therefore rests on a different contract (`at_final_chunk = true` passed by
/// `parse_block_for_compaction*`), and nothing pinned it — so this case pins
/// BOTH that they refuse and, if they ever answer `Ok` again, that the answer
/// neither loses nor fabricates a partition key.
#[tokio::test]
async fn stitched_verifier_scans_refuse_and_never_lose_or_fabricate_partitions() {
    let staged = fixture::stage_control_and_mutated(&fixture_dir(&BIG_COMPOSITE), "stitched");

    let control = open_reader(&staged.control_dir).await;
    // MULTISET again (roborev job 57 finding 1): a duplicated key is fabrication
    // and a set cannot express it.
    let control_counts = multiset::multiset(
        control
            .distinct_partition_keys()
            .await
            .expect("the pristine fixture must enumerate partition keys"),
    );
    assert!(
        !control_counts.is_empty(),
        "0-rows-when-present: the control must enumerate partition keys"
    );
    assert!(
        !control
            .partition_verify_scan()
            .await
            .expect("the pristine fixture must verify-scan cleanly")
            .is_empty(),
        "0-rows-when-present: the control verify scan must yield partitions"
    );

    let mutated = open_reader(&staged.mutated_dir).await;
    match mutated.distinct_partition_keys().await {
        Err(e) => assert_corruption_kind(&e, "distinct_partition_keys"),
        Ok(keys) => {
            let got = multiset::multiset(keys);
            let lost = multiset::deficit(&got, &control_counts);
            let fabricated = multiset::surplus(&got, &control_counts);
            panic!(
                "the stitched key enumeration must refuse a corrupt row: got Ok with {} \
                 distinct keys (control {}), {} occurrence(s) LOST [{}] and {} FABRICATED [{}]",
                got.len(),
                control_counts.len(),
                lost.iter().map(|(_, n)| n).sum::<usize>(),
                multiset::describe(&lost),
                fabricated.iter().map(|(_, n)| n).sum::<usize>(),
                multiset::describe(&fabricated)
            );
        }
    }

    match mutated.partition_verify_scan().await {
        Err(e) => assert_corruption_kind(&e, "partition_verify_scan"),
        Ok(rows) => {
            let got = multiset::multiset(rows.into_iter().map(|(k, _ldt)| k));
            let lost = multiset::deficit(&got, &control_counts);
            let fabricated = multiset::surplus(&got, &control_counts);
            panic!(
                "the stitched verify scan must refuse a corrupt row: got Ok with {} distinct \
                 partitions (control {}), {} occurrence(s) LOST [{}] and {} FABRICATED [{}]",
                got.len(),
                control_counts.len(),
                lost.iter().map(|(_, n)| n).sum::<usize>(),
                multiset::describe(&lost),
                fabricated.iter().map(|(_, n)| n).sum::<usize>(),
                multiset::describe(&fabricated)
            );
        }
    }
}

/// The staging harness must not LEAK the fixtures it copies (roborev job 59
/// finding 1, #3950).
///
/// Every case in this target copies TWO complete SSTable generations into a temp
/// directory; before this the directory was PID-named and never removed, so
/// repeated runs accumulated them indefinitely — and these lanes run on fleet
/// boxes whose disk hygiene is already a live concern.
///
/// BOTH halves are pinned, because only the pair distinguishes a fix from a
/// regression in the other direction: the staged bytes must be READABLE while
/// the `Staged` value is alive (a guard dropped too early would make the
/// fixtures vanish mid-test, which is worse than the leak), and the whole
/// staging root must be GONE once it drops. The `drop` is explicit rather than
/// end-of-scope so the post-condition can be asserted at all.
#[test]
fn the_staging_harness_removes_both_generations_on_drop() {
    let staged = fixture::stage_control_and_mutated(&fixture_dir(&BIG_COMPOSITE), "cleanup");
    let root = staged.staging_root().to_path_buf();
    let control_data = comp_file(&staged.control_dir, "-Data.db");
    let mutated_data = comp_file(&staged.mutated_dir, "-Data.db");

    // Alive: both generations are real, readable files under the staging root.
    for staged_data in [&control_data, &mutated_data] {
        assert!(
            staged_data.starts_with(&root),
            "staged {staged_data:?} must live under the harness's own staging root {root:?}"
        );
        assert!(
            std::fs::metadata(staged_data)
                .map(|m| m.len() > 0)
                .unwrap_or(false),
            "the staged fixture {staged_data:?} must be a non-empty file while the Staged \
             guard is alive"
        );
    }

    drop(staged);

    // Dropped: nothing is left behind — not the two generations, not the root.
    assert!(
        !root.exists(),
        "the staging harness leaked {root:?}: dropping Staged must remove both copied \
         generations (roborev job 59 finding 1, #3950)"
    );
    assert!(
        !control_data.exists() && !mutated_data.exists(),
        "the staged Data.db files must be gone with their staging root: control {} / \
         mutated {}",
        control_data.exists(),
        mutated_data.exists()
    );
}

/// AC1 — assert the decode error's KIND, not a message substring.
///
/// A message check alone (`e.to_string().contains("clustering_key2")`) stays
/// green through a refactor that re-wraps the decode error in a different
/// variant while forwarding the text, which is the no-heuristics shape: it reads
/// bytes of a rendered string instead of the authoritative discriminant. The
/// kind IS the thing #3782 AC1 preserves, so it is asserted structurally.
fn assert_corruption_kind(e: &cqlite_core::Error, surface: &str) {
    assert!(
        matches!(e, cqlite_core::Error::Corruption(_)),
        "{surface}: the refusal must carry the DECODE error's own kind \
         (Error::Corruption), not a re-wrapped generic; got {e:?}"
    );
}

/// Roborev job 52 (finding 2) — the extent parameter must be NAMEABLE from
/// OUTSIDE the crate.
///
/// `V5CompressedLegacyParser` is public API by a two-hop `doc(hidden)`
/// re-export (`parsing/mod.rs` → `reader/mod.rs` → `lib.rs`'s `pub mod
/// storage`), and #3782 made four of its `pub fn`s take a `BufferExtent`. While
/// that enum was `pub(crate)` those methods were UNCALLABLE from outside — an
/// out-of-crate caller could not write the argument — and `private_interfaces`
/// did not fire, so nothing but this test observes the break.
///
/// This file IS out-of-crate (an integration test), so the `use` below is the
/// assertion: it fails to COMPILE if the type stops travelling the parser's own
/// public path, or if a variant is renamed or made non-exhaustive-constructible.
#[test]
fn the_block_emit_extent_is_nameable_beside_its_parser_from_outside_the_crate() {
    use cqlite_core::storage::sstable::reader::{BufferExtent, V5CompressedLegacyParser};

    // The parser item itself, on the public path.
    let _parser_ctor = V5CompressedLegacyParser::new;
    // Both variants, constructed by an out-of-crate caller.
    let complete = BufferExtent::Complete;
    let window = BufferExtent::Window;
    assert_ne!(
        complete, window,
        "the two extents must stay distinguishable: Complete REFUSES a decode \
         error at the buffer end (data loss), Window tolerates it (a row \
         straddling into the next chunk)"
    );
    // `Copy`, so passing it to a parse call never moves the caller's value.
    let copied = complete;
    assert_eq!(copied, BufferExtent::Complete);
}
