//! Shared scaffolding for the issue #3928 partition-HEADER refusal lanes.
//!
//! Two lanes, two CORRUPTION CLASSES, one set of surfaces:
//!
//! * `issue_3928_corrupt_header_refusal.rs` — a header byte FLIPPED, so
//!   `partition_header_readiness` answers `Malformed` or `Ready`-then-invalid.
//! * `issue_3928_truncated_header_refusal.rs` — a header that RAN OUT, so it
//!   answers `Incomplete`; the bytes are a PREFIX of the fixture's own stitched
//!   section, which is what a truncated file presents.
//!
//! They were one file until it crossed the 1500-line test threshold; splitting
//! by corruption class (rather than by, say, surface) keeps each lane's oracle
//! and its pre-fix measurements together, and puts everything BOTH need here so
//! neither can drift into its own copy of a walk.
#![allow(dead_code)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::platform::Platform;
use cqlite_core::schema::TableSchema;
use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::Database;

use super::datasets_root;
use super::fixture::{self, FixtureSpec};
use super::multiset;

/// The fixture's GENERATION directory, resolved per TABLE (#3220) so a root that
/// holds the keyspace but not this table cannot silently win the selection. The
/// two specs live in DIFFERENT roots on a fleet box — the BTI one is
/// git-committed in the checkout, the BIG one is fetched-corpus-only — so
/// evidence, never a preference order, decides (#3104). "Not found" is a loud
/// named panic, never a skip.
pub fn fixture_dir(spec: &FixtureSpec) -> PathBuf {
    datasets_root::resolve_table_generation_dir(spec.keyspace, spec.table).unwrap_or_else(|why| {
        panic!(
            "fixture {}.{} has no usable generation directory: {why}",
            spec.keyspace, spec.table
        )
    })
}

pub fn schema_file(spec: &FixtureSpec) -> PathBuf {
    datasets_root::schema_path(spec.schema_file).expect("committed CQL schema (#3148)")
}

pub fn table_schema(spec: &FixtureSpec) -> TableSchema {
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

pub async fn open_db(spec: &FixtureSpec, data_dir: PathBuf) -> Database {
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

pub async fn open_reader(dir: &Path) -> SSTableReader {
    let config = cqlite_core::Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    SSTableReader::open(&fixture::comp_file(dir, "-Data.db"), &config, platform)
        .await
        .expect("open SSTableReader")
}

/// What one read surface answered on one leg of a staged pair.
///
/// `keys` is `Err(message)` when the surface REFUSED and `Ok(keys)` when it
/// answered `Ok` — carrying the partition key of every emitted element, in emit
/// order, so a DUPLICATE is visible. A set would hide the resync's re-emission
/// of a partition it had already emitted, which is one of the ways the
/// #3782/#3928 shape makes the count go UP while data is lost.
///
/// The refusal's MESSAGE is retained rather than discarded: a control leg that
/// refuses a PRISTINE fixture is a broken lane, and "it refused" without saying
/// why sends the next reader back to the debugger.
pub struct Outcome {
    pub name: &'static str,
    pub keys: Result<Vec<Vec<u8>>, String>,
}

impl Outcome {
    pub fn describe(&self, control: &std::collections::BTreeMap<Vec<u8>, usize>) -> String {
        match &self.keys {
            Err(why) => format!("{}: REFUSED ({why})", self.name),
            Ok(keys) => {
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

/// Render a refusal for a diagnostic.
pub fn why(e: cqlite_core::Error) -> String {
    e.to_string()
}

/// A WARN-level subscriber writing into `sink`, for `tracing::subscriber::set_default`.
pub fn warn_subscriber(sink: &LogSink) -> impl tracing::Subscriber + Send + Sync {
    tracing_subscriber::fmt()
        .with_writer(sink.clone())
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .finish()
}

/// A control-leg surface's own partition-key multiset. An empty multiset for a
/// surface that REFUSED the control leg, which `assert_control_is_healthy`
/// already panics on — this only keeps the rendering total.
pub fn control_multiset(c: &Outcome) -> std::collections::BTreeMap<Vec<u8>, usize> {
    c.keys
        .as_ref()
        .map(|k| multiset::multiset(k.iter().cloned()))
        .unwrap_or_default()
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
pub async fn observe(dir: &Path, schema: &TableSchema) -> Vec<Outcome> {
    let reader = open_reader(dir).await;
    let mut out: Vec<Outcome> = Vec::new();

    out.push(Outcome {
        name: "distinct_partition_keys",
        keys: reader.distinct_partition_keys().await.map_err(why),
    });
    out.push(Outcome {
        name: "partition_verify_scan",
        keys: reader
            .partition_verify_scan()
            .await
            .map_err(why)
            .map(|rows| rows.into_iter().map(|(k, _ldt)| k).collect()),
    });
    out.push(Outcome {
        name: "get_all_entries",
        keys: reader.get_all_entries().await.map_err(why).map(|rows| {
            rows.into_iter()
                .map(|(_t, k, _r)| k.as_bytes().to_vec())
                .collect()
        }),
    });
    out.push(Outcome {
        name: "iterate_all_partitions",
        keys: reader
            .iterate_all_partitions()
            .await
            .map_err(why)
            .map(|rows| {
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
            .map_err(why)
            .map(|rows| {
                rows.into_iter()
                    .map(|r| r.key.as_bytes().to_vec())
                    .collect()
            }),
    });

    let cancel = cqlite_core::storage::scan_cancel::ScanCancel::new();
    let mut streamed: Vec<Vec<u8>> = Vec::new();
    let streamed = reader
        .stream_all_partitions_for_compaction(Some(schema), &cancel, |row| {
            streamed.push(row.key.as_bytes().to_vec());
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .await
        .map_err(why)
        // The rows emitted BEFORE a refusal are deliberately discarded: a
        // surface that refused emitted no answer, and counting its partial
        // emission as an answer would let a refusal be scored for fabrication.
        .map(|()| streamed);
    out.push(Outcome {
        name: "stream_all_partitions_for_compaction",
        keys: streamed,
    });

    out
}

/// The control leg's partition-key multiset, plus the proof it is non-empty
/// (0-rows-when-present is a failure, never a pass).
pub async fn control_keys(
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
pub fn assert_control_is_healthy(
    observed: &[Outcome],
    control_partitions: &std::collections::BTreeMap<Vec<u8>, usize>,
    spec: &FixtureSpec,
) {
    let expected: BTreeSet<&Vec<u8>> = control_partitions.keys().collect();
    for o in observed {
        let keys = o.keys.as_ref().unwrap_or_else(|why| {
            panic!(
                "control leg: {}.{} surface `{}` REFUSED a PRISTINE Cassandra fixture ({why}) \
                 — the mutated-leg expectations below would be meaningless",
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
pub fn tolerating(control: &[Outcome], mutated: &[Outcome]) -> Vec<String> {
    assert_eq!(
        control.len(),
        mutated.len(),
        "the two legs must be observed through the same surface list"
    );
    control
        .iter()
        .zip(mutated.iter())
        .filter(|(_, m)| m.keys.is_ok())
        .map(|(c, m)| m.describe(&control_multiset(c)))
        .collect()
}

/// The surfaces of the mutated leg that FABRICATED — answered `Ok` carrying a
/// partition-key occurrence the SAME surface did not produce on the pristine
/// fixture.
pub fn fabricating(control: &[Outcome], mutated: &[Outcome]) -> Vec<String> {
    assert_eq!(
        control.len(),
        mutated.len(),
        "the two legs must be observed through the same surface list"
    );
    control
        .iter()
        .zip(mutated.iter())
        .filter_map(|(c, m)| {
            let keys = m.keys.as_ref().ok()?;
            let ctl = control_multiset(c);
            let got = multiset::multiset(keys.iter().cloned());
            if multiset::surplus(&got, &ctl).is_empty() {
                None
            } else {
                Some(m.describe(&ctl))
            }
        })
        .collect()
}

/// Capture WARN-and-above tracing output into a shared buffer, so a case can
/// assert what a code path did NOT log.
#[derive(Clone, Default)]
pub struct LogSink(pub Arc<std::sync::Mutex<Vec<u8>>>);
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
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("log sink mutex").clone()).to_string()
    }
}

/// The two WARNs the pre-#3928 block-emit header arm emitted immediately before
/// `offset += 1`, verbatim from
/// `block_emit_windowed/partition_header_arm.rs`. Since the fix they are
/// reachable ONLY on a `BufferExtent::Window`, so their presence on a
/// full-extent walk IS the resync.
pub const RESYNC_WARNS: &[&str] = &[
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
/// `issue_3782_corrupt_row_refusal.rs`: neither candidate root is a superset of
/// the other, so the scan takes their union and resolves each table by evidence.
pub const MUST_RUN: &[(&str, &str)] = &[
    ("test_da", "multiclustering_table"),
    ("test_basic", "composite_key_table"),
];

/// Every `(keyspace, table)` identity that carries a real `*-Data.db` under ANY
/// candidate root — the UNION, deduplicated by identity. A `break` on the first
/// non-empty root is a PREFERENCE ORDERING and misses the checkout-only tables
/// (#3220).
pub fn corpus_table_identities() -> BTreeSet<(String, String)> {
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
pub fn data_files_for_table(keyspace: &str, table: &str) -> Vec<PathBuf> {
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
pub fn assert_corruption_kind(e: &cqlite_core::Error, surface: &str) {
    assert!(
        matches!(e, cqlite_core::Error::Corruption(_)),
        "{surface}: the refusal must carry the header decode error's own kind \
         (Error::Corruption), not a re-wrapped generic; got {e:?}"
    );
}
/// Where a truncated `Data.db` ends inside the LAST partition's header.
///
/// The three variants are the three distinct answers
/// `partition_header_readiness` gives for a header that ran out, and each is
/// decided by a DIFFERENT branch of that classifier — so covering one says
/// nothing about the others.
#[derive(Clone, Copy, Debug)]
pub enum HeaderCut {
    /// ONE byte of the header survives: fewer than the two any header needs.
    /// The classifier's `data.len() < 2` branch. This is the tail the block-emit
    /// walk deliberately tolerates, and the one the drivers report as `Done`.
    OneByte,
    /// The 2-byte key length survives and the KEY is cut in half. The
    /// classifier's `data.get(deletion_offset) == None` branch.
    InsideKey,
    /// Key length and key survive; the partition-level `DeletionTime` is cut
    /// (`nb` needs 12 bytes and 4 are present). The classifier's
    /// `deletion_offset + deletion_time_min > data.len()` branch.
    InsideDeletionTime,
}

/// The pristine stitched section, the last partition header's offset, and the
/// byte count that leaves that header cut as `cut` describes.
pub fn truncation(dir: &Path, cut: HeaderCut) -> (Vec<u8>, usize, usize) {
    let dec = fixture::stitched_data_section(dir);
    let hdr = fixture::last_partition_header_offset(dir, &dec);
    // `writeWithShortLength`: 2-byte big-endian key length
    // (`SortedTablePartitionWriter.start`, cassandra-5.0.8). Cross-checked
    // against Index.db by the helper above, so this is a measured length.
    let key_len = usize::from(u16::from_be_bytes([dec[hdr], dec[hdr + 1]]));
    assert!(
        key_len >= 4,
        "this fixture's partition key is {key_len} byte(s); the InsideKey cut needs at \
         least 4 so that some key bytes survive AND some are cut"
    );
    let keep = match cut {
        HeaderCut::OneByte => hdr + 1,
        HeaderCut::InsideKey => hdr + 2 + key_len / 2,
        // `nb`'s DeletionTime is a fixed 12 bytes (`DeletionTime.LegacySerializer`:
        // 4-byte localDeletionTime + 8-byte markedForDeleteAt), so 4 present is a
        // genuine mid-field cut.
        HeaderCut::InsideDeletionTime => hdr + 2 + key_len + 4,
    };
    assert!(
        keep > hdr && keep < dec.len(),
        "the {cut:?} cut must land strictly inside the last header: keep={keep}, \
         header at {hdr}, section {} byte(s)",
        dec.len()
    );
    (dec, hdr, keep)
}

/// A parser configured the way the stitched read paths configure theirs, minus
/// the Statistics.db timestamp bases (which affect decoded VALUES, never
/// framing). The control assertions in each case below require this parser to
/// decode the PRISTINE section completely, so its adequacy for these cases is
/// measured rather than assumed.
pub fn framing_parser(
    spec: &FixtureSpec,
) -> cqlite_core::storage::sstable::reader::V5CompressedLegacyParser {
    cqlite_core::storage::sstable::reader::V5CompressedLegacyParser::new(
        spec.keyspace.to_string(),
        spec.table.to_string(),
        0,
        0,
        None,
    )
}

/// Partition keys the block-emit walk (`parse_block` → `parse_block_emit_windowed`
/// with NO row-body window, the route `stitch_and_parse_all_chunks` takes) emits
/// over `buf`, or the refusal.
pub fn block_walk(
    spec: &FixtureSpec,
    schema: &TableSchema,
    reader: &SSTableReader,
    buf: &[u8],
) -> Result<Vec<Vec<u8>>, String> {
    use cqlite_core::storage::sstable::reader::BufferExtent;
    let mut keys: Vec<Vec<u8>> = Vec::new();
    framing_parser(spec)
        .parse_block_emit_windowed(
            buf,
            BufferExtent::Complete,
            Some(schema),
            reader,
            None,
            |(_t, k, _r)| {
                keys.push(k.as_bytes().to_vec());
                Ok(std::ops::ControlFlow::Continue(()))
            },
        )
        .map_err(why)
        .map(|()| keys)
}

/// The same buffer through the CELL-METADATA walk — `parse_block_with_cell_metadata`,
/// the route `stitch_and_parse_all_chunks_with_metadata` takes when a query
/// carries `WRITETIME(col)`/`TTL(col)` (`ProjectionFlags::include_cell_metadata`).
pub fn metadata_walk(
    spec: &FixtureSpec,
    schema: &TableSchema,
    reader: &SSTableReader,
    buf: &[u8],
) -> Result<Vec<Vec<u8>>, String> {
    use cqlite_core::storage::sstable::reader::BufferExtent;
    framing_parser(spec)
        .parse_block_with_cell_metadata(buf, BufferExtent::Complete, Some(schema), reader)
        .map_err(why)
        .map(|rows| {
            rows.into_iter()
                .map(|(_t, k, _r, _m)| k.as_bytes().to_vec())
                .collect()
        })
}

/// The SLIDING driver, at its FINAL chunk — `parse_one_partition_for_compaction`,
/// the public entry to `drive_partition_sliding`, which is also what
/// `stream_partition_body_incremental` shares its header arm with.
pub fn driver_at_final_chunk(
    spec: &FixtureSpec,
    schema: &TableSchema,
    reader: &SSTableReader,
    buf: &[u8],
) -> Result<String, String> {
    let mut emitted = 0usize;
    framing_parser(spec)
        .parse_one_partition_for_compaction(buf, Some(schema), reader, true, &mut |_row| {
            emitted += 1;
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .map_err(why)
        .map(|step| format!("{step:?} after emitting {emitted} row(s)"))
}

/// B1(b) + the `SELECT *` vs `WRITETIME(col)` divergence — the two stitched
/// walks must AGREE, and on an unbounded proven-complete buffer both must
/// REFUSE.
///
/// `stitch_and_parse_all_chunks` (`data_access/mod.rs:249`) and
/// `stitch_and_parse_all_chunks_with_metadata` (`:288`) hand the SAME
/// `stitch_all_chunks` buffer to their parses with the SAME
/// `BufferExtent::Complete`; they are the plain and
/// `WRITETIME`/`TTL`-projection variants of ONE query. So before this round a
/// `Data.db` truncated to one surviving header byte answered `SELECT *` with
/// `Ok` (last partition silently dropped, via the block walk's tail carve-out)
/// and `SELECT *, WRITETIME(c)` with `Err` (the metadata walk has no such
/// carve-out) — on the same file.
///
/// This case asserts the property at the parse both routes are handed, which is
/// where the divergence lives; it does not drive the two SQL statements, because
/// that would need a rewritten `CompressionInfo.db` and CRC to truncate the file
/// Used as the POSITIVE CONTROL for the resync-WARN absence assertions: it
/// differs from `block_walk` in exactly one argument, so a resync it logs and
/// `block_walk` does not is attributable to the extent alone.
pub fn window_walk(
    spec: &FixtureSpec,
    schema: &TableSchema,
    reader: &SSTableReader,
    buf: &[u8],
) -> Result<Vec<Vec<u8>>, String> {
    use cqlite_core::storage::sstable::reader::BufferExtent;
    let mut keys: Vec<Vec<u8>> = Vec::new();
    framing_parser(spec)
        .parse_block_emit_windowed(
            buf,
            BufferExtent::Window,
            Some(schema),
            reader,
            None,
            |(_t, k, _r)| {
                keys.push(k.as_bytes().to_vec());
                Ok(std::ops::ControlFlow::Continue(()))
            },
        )
        .map_err(why)
        .map(|()| keys)
}

/// The SLIDING driver MID-STREAM (`at_final_chunk == false`) — the legitimately
/// tolerant leg, where a truncated header may still be completed by the next
/// chunk and `NeedMore` is the correct answer.
pub fn driver_mid_stream(
    spec: &FixtureSpec,
    schema: &TableSchema,
    reader: &SSTableReader,
    buf: &[u8],
) -> Result<String, String> {
    let mut emitted = 0usize;
    framing_parser(spec)
        .parse_one_partition_for_compaction(buf, Some(schema), reader, false, &mut |_row| {
            emitted += 1;
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .map_err(why)
        .map(|step| format!("{step:?} after emitting {emitted} row(s)"))
}

/// The block-emit walk over `buf` with a #954 row-body window — a BOUNDED walk,
/// at the caller's stated extent.
pub fn bounded_walk(
    spec: &FixtureSpec,
    schema: &TableSchema,
    reader: &SSTableReader,
    buf: &[u8],
    window: (usize, usize),
    extent: cqlite_core::storage::sstable::reader::BufferExtent,
) -> Result<Vec<Vec<u8>>, String> {
    let mut keys: Vec<Vec<u8>> = Vec::new();
    framing_parser(spec)
        .parse_block_emit_windowed(
            buf,
            extent,
            Some(schema),
            reader,
            Some(window),
            |(_t, k, _r)| {
                keys.push(k.as_bytes().to_vec());
                Ok(std::ops::ControlFlow::Continue(()))
            },
        )
        .map_err(why)
        .map(|()| keys)
}
