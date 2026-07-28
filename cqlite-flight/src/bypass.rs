//! Single-source (one post-prune SSTable) merge bypass for the warm `do_get`
//! row route (issue #3058).
//!
//! # What it does
//!
//! `MergeProducer::produce_streaming_from_readers` used to build a `KWayMerger`
//! UNCONDITIONALLY, so a `SELECT` over a table with a single SSTable generation
//! paid the whole cross-generation reconciliation machinery — a per-input
//! producer thread + per-ROW channel handoff, full-fidelity `CompactionRow`
//! decoding with a per-row `HashMap<String, CellWriteMetadata>` the read path
//! never reads, and `reconcile_cluster_with_overlap_counted` per clustering
//! group — to reconcile ONE generation against itself.
//!
//! With exactly one source there is nothing to reconcile ACROSS: read-time
//! SELECT semantics (partition deletions, range tombstones, row/cell tombstones,
//! TTL expiry, static-cell injection) are applied INSIDE the decoder by
//! `PartitionShadow` on the single-generation query walk (`read_shadowing =
//! true`, issue #1741). This module decides — from authoritative state only —
//! whether that walk may serve the request, and adapts it to the shared row
//! drive loop.
//!
//! # The predicate is conjunctive and FAIL-CLOSED (issue #28)
//!
//! [`bypass_reason`] consults ONLY: the POST-prune source count, the schema's
//! `dropped_columns` map, the aggregation flag, and the forced-path override —
//! plus the reader's own component metadata via
//! `SSTableReader::supports_streaming_query_scan`. It never looks at a file
//! size, a `Statistics.db` estimate, or SSTable byte content. Anything that
//! cannot be established takes the slow, known-correct merge arm.
//!
//! # The forced-path seam
//!
//! `CQLITE_FLIGHT_MERGE_PATH` (`bypass` | `merge`, unset = automatic) is a
//! PERMANENT, documented seam mirroring `CQLITE_READ_PATH` from the #1918
//! point-vs-full differential lane. It is how the two arms are proven
//! equivalent over the SAME bytes at a PINNED `now` — without it the fast path
//! would silently become the only path over every single-SSTable fixture and
//! there would be nothing to compare against. It is also the field's kill
//! switch if the fast path is ever found wrong: `merge` restores the previous
//! behaviour with no redeploy.
//!
//! `merge` is absolute (never take the fast path). `bypass` requests the fast
//! path but NEVER overrides a correctness precondition — a 2-source table under
//! `bypass` still merges, because an override that could return wrong rows is
//! not a useful knob. So `bypass` is `auto` with an explicit, assertable name;
//! the arm actually taken is observable through
//! [`cqlite_core::storage::read_path_probe`].

use std::sync::Arc;

use cqlite_core::schema::{CqlType, TableSchema};
use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::sstable::reader::{
    QueryRowBatch, QueryRowStream, SSTableReader, ScanTokenBound,
};
use cqlite_core::storage::write_engine::DecoratedKey;
use cqlite_core::types::ScanRow;
use cqlite_core::util::cassandra_murmur3::cassandra_murmur3_token;
use cqlite_core::RowKey;

use crate::producer::ProducerError;
use crate::row_source::{PendingRow, RowSource, SourceStep};

/// The forced-path override environment variable (see the module docs).
pub const MERGE_PATH_ENV: &str = "CQLITE_FLIGHT_MERGE_PATH";

/// Which arm the operator/test pinned for the warm row route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ForcedMergePath {
    /// No pin: the bypass predicate decides (production default).
    #[default]
    Auto,
    /// Prefer the single-source fast path wherever it is CORRECT to take it.
    Bypass,
    /// Never take the fast path — always drive the k-way merger.
    Merge,
}

impl ForcedMergePath {
    /// Read the override once (call once per request, never per row).
    pub fn from_env() -> Self {
        Self::parse(std::env::var(MERGE_PATH_ENV).ok().as_deref())
    }

    /// Pure parse of the override's raw value, so the mapping is unit-testable
    /// without mutating the process environment. An unset, empty, or
    /// unrecognized value is [`ForcedMergePath::Auto`] — a typo must never
    /// silently change query RESULTS, only (at most) which equivalent arm runs.
    pub fn parse(raw: Option<&str>) -> Self {
        match raw.map(str::trim) {
            Some(v) if v.eq_ignore_ascii_case("bypass") => Self::Bypass,
            Some(v) if v.eq_ignore_ascii_case("merge") => Self::Merge,
            _ => Self::Auto,
        }
    }
}

/// Why the warm row route took the arm it took — an authoritative, loggable
/// reason rather than a bare bool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BypassReason {
    /// Every precondition held: the single-source fast path is selected.
    Selected,
    /// `CQLITE_FLIGHT_MERGE_PATH=merge` pinned the merge arm.
    ForcedMerge,
    /// The request aggregates (unreachable here — `produce_streaming_from_readers`
    /// returns early on `is_aggregating()`; kept so the predicate is total).
    Aggregating,
    /// The POST-prune source count is not exactly 1.
    MultipleSources,
    /// `schema.dropped_columns` is non-empty: the reconciler's timestamp-based
    /// dropped-column purge (`write_engine/merge/reconcile.rs`, Step 3b) has NO
    /// `PartitionShadow` counterpart, so the fast path could surface a cell the
    /// merge path would purge. Latent today (the Flight schema comes from the
    /// ticket DDL, whose parser hardcodes an empty map) — this is the
    /// fail-closed guard against that becoming live.
    DroppedColumns,
    /// The single reader lacks the components the single-generation streaming
    /// query walk needs (no `Index.db`, or a BTI reader).
    ReaderUnsupported,
    /// The schema declares a NON-FROZEN collection whose element (set) or key
    /// (map) is a frozen UDT/tuple/nested collection — a "composite-keyed"
    /// collection.
    ///
    /// The two arms disagree by CONSTRUCTION on these columns, and in a way that
    /// would make a query's outcome depend on the generation count (roborev,
    /// issue #3058):
    /// * the MERGE arm's reassembler FAILS CLOSED (`read_assembly.rs`'s
    ///   `key_is_opaque_composite` → `composite_collection_unsupported`, issue
    ///   #2339) rather than emit opaque bytes into a typed Arrow builder;
    /// * the single-generation decoder returns the collapsed value happily.
    ///
    /// So without this guard `SELECT *` over such a table would ERROR at two
    /// generations and SUCCEED at one — i.e. start failing after a flush and
    /// start working after a compaction. That is a query-result change, which
    /// this change's contract (spec R6) forbids, so the schema takes the merge
    /// arm and today's behaviour is preserved EXACTLY. Making both arms serve
    /// these columns is owned by #2339, exactly as the static divergence is owned
    /// by #3095.
    CompositeKeyedCollection,
    /// The schema declares a STATIC column.
    ///
    /// The two arms genuinely disagree on static-row shape today, in OPPOSITE
    /// directions, so neither can be adopted silently (measured on
    /// `test_writeparity.static_clustering_shape` and on a static fixture with a
    /// static-ONLY partition):
    /// * the merge arm emits the static row as its OWN `ck = null` row and does
    ///   NOT inject the static value into the partition's clustering rows —
    ///   Cassandra does the opposite;
    /// * the single-generation decoder injects the static cells into every
    ///   clustering row (correct) but emits NOTHING for a partition that has a
    ///   static row and NO clustering rows — Cassandra returns one row there, so
    ///   taking the fast path would DROP that row.
    ///
    /// Changing query results is out of this change's remit either way, so a
    /// static-bearing schema takes the merge arm and keeps today's behaviour
    /// EXACTLY. Reconciling both arms with Cassandra's static semantics is a
    /// follow-up (it changes the core read path, not just Flight routing).
    StaticColumns,
}

impl BypassReason {
    /// Whether the fast path was selected.
    pub fn is_selected(self) -> bool {
        matches!(self, Self::Selected)
    }
}

/// The conjunctive, fail-closed bypass predicate (issue #3058, spec R1).
///
/// `readers` MUST be the POST-prune reader set (`prune_readers`), so token
/// pruning that leaves one source selects the fast path and a pre-prune count of
/// 2 does not veto it.
pub fn bypass_reason(
    readers: &[Arc<SSTableReader>],
    schema: &TableSchema,
    forced: ForcedMergePath,
    aggregating: bool,
) -> BypassReason {
    if forced == ForcedMergePath::Merge {
        return BypassReason::ForcedMerge;
    }
    if aggregating {
        return BypassReason::Aggregating;
    }
    let [only] = readers else {
        return BypassReason::MultipleSources;
    };
    if !schema.dropped_columns.is_empty() {
        return BypassReason::DroppedColumns;
    }
    if schema.columns.iter().any(|c| c.is_static) {
        return BypassReason::StaticColumns;
    }
    if schema
        .columns
        .iter()
        .any(|c| declares_composite_keyed_collection(&c.data_type))
    {
        return BypassReason::CompositeKeyedCollection;
    }
    if !only.supports_streaming_query_scan() {
        return BypassReason::ReaderUnsupported;
    }
    BypassReason::Selected
}

/// Whether `data_type` declares a collection the MERGE arm's reassembler refuses
/// (issue #2339) — see [`BypassReason::CompositeKeyedCollection`].
///
/// MIRRORS the authority that actually fails closed, `read_assembly.rs`'s
/// `key_is_opaque_composite`: a non-frozen `set<X>` whose element `X`, or a
/// non-frozen `map<K, _>` whose key `K`, is (after unwrapping `frozen`) a
/// tuple / UDT / nested collection, or a `Custom` type name other than the two
/// the scalar codec decodes (`time`, `inet`). A `list<…>` is always fine: its
/// cell path is a position TimeUUID, never the value.
///
/// A type string that does not parse counts as composite (fail-closed): an
/// unrepresentable type must never silently select the fast arm.
fn declares_composite_keyed_collection(data_type: &str) -> bool {
    let Ok(parsed) = CqlType::parse(data_type) else {
        return true;
    };
    // A FROZEN collection is one opaque cell — it never takes the per-element
    // cell-path path the merge arm refuses.
    match parsed {
        CqlType::Set(inner) => is_opaque_composite(&inner),
        CqlType::Map(key, _) => is_opaque_composite(&key),
        // `CqlType::parse` also returns `Custom` for a type string whose
        // STRUCTURE it could not parse (e.g. an uppercase `SET<…>`). A bare name
        // is a UDT reference — one opaque cell both arms serve identically — but
        // an unparsed structure could be any collection, so fail closed.
        CqlType::Custom(name) => name.contains('<'),
        _ => false,
    }
}

/// Whether a collection element/key type is undecodable by the merge arm's
/// scalar codec (the `key_is_opaque_composite` rule).
fn is_opaque_composite(ty: &CqlType) -> bool {
    match ty {
        CqlType::Frozen(inner) => is_opaque_composite(inner),
        CqlType::Tuple(_)
        | CqlType::Udt(_, _)
        | CqlType::Set(_)
        | CqlType::List(_)
        | CqlType::Map(_, _) => true,
        // Only these two `Custom` names are decoded by the scalar codec; every
        // other name is a UDT reference (or unparsed structure) and is opaque.
        CqlType::Custom(name) => {
            let bare = name.rsplit(':').next().unwrap_or(name);
            !(bare == "time" || bare == "inet")
        }
        _ => false,
    }
}

/// The single-generation row source: pulls `(RowKey, ScanRow)` batches from the
/// core query walk and presents them to the shared drive loop.
///
/// Everything downstream of `next_step` — batching, `max_batch_bytes`,
/// `CancelFlag` polling, `ScanProgress`, predicate/projection application — is
/// the SAME code the merge arm runs; only where the rows come from differs.
pub(crate) struct ScanRowSource {
    stream: QueryRowStream,
    batch: std::vec::IntoIter<(RowKey, ScanRow)>,
    /// Memoized decoration of the CURRENT partition key. Rows of a partition
    /// arrive consecutively, so the Murmur3 token is computed once per
    /// partition, not once per row (a byte compare replaces the hash).
    current: Option<DecoratedKey>,
    /// Set once the walk has handed over at least one row, so a late
    /// `Unsupported` (a contract violation) is distinguishable from the
    /// pre-emit one the caller may fall back on.
    emitted_any: bool,
}

impl ScanRowSource {
    /// Open and PRIME the single-generation source.
    ///
    /// Returns `Ok(None)` when the walk reports it cannot serve this reader —
    /// which it does BEFORE emitting anything — so the caller falls back to the
    /// k-way merge arm with no partial output (fail-closed). `now_secs` is the
    /// request's reconciliation clock, threaded into the walk's read-shadowing
    /// parser so TTL expiry is evaluated at exactly the instant the merge arm
    /// would use (`with_now_secs`), never an ambient wall-clock read.
    pub(crate) fn open(
        reader: Arc<SSTableReader>,
        schema: TableSchema,
        token_bound: Option<ScanTokenBound>,
        now_secs: i64,
        scan_cancel: ScanCancel,
    ) -> Result<Option<Self>, ProducerError> {
        let mut stream = reader
            .open_query_row_stream(schema, token_bound, now_secs, scan_cancel)
            .map_err(ProducerError::Merge)?;
        // Prime: the "cannot serve this reader" signal is the FIRST message, so
        // the fallback decision is made before a single row reaches the client.
        let first = match stream.next_batch() {
            None => Vec::new(),
            Some(Ok(QueryRowBatch::Unsupported)) => return Ok(None),
            Some(Ok(QueryRowBatch::Rows(rows))) => rows,
            Some(Err(e)) => return Err(map_scan_error(e)),
        };
        let emitted_any = !first.is_empty();
        Ok(Some(Self {
            stream,
            batch: first.into_iter(),
            current: None,
            emitted_any,
        }))
    }

    /// Decorate `key` with its Murmur3 token, reusing the memoized decoration
    /// while the partition is unchanged.
    fn decorate(&mut self, key: &RowKey) -> DecoratedKey {
        let bytes = key.as_bytes();
        if let Some(current) = &self.current {
            if current.key == bytes {
                return current.clone();
            }
        }
        let decorated = DecoratedKey::new(cassandra_murmur3_token(bytes), bytes.to_vec());
        self.current = Some(decorated.clone());
        decorated
    }
}

/// Map a core scan error onto the producer's error taxonomy, preserving a
/// cooperative cancellation as `Cancelled` rather than a generic merge failure
/// (issue #2264: map by VARIANT, never by racing the cancel flag).
fn map_scan_error(e: cqlite_core::Error) -> ProducerError {
    match e {
        cqlite_core::Error::Cancelled => ProducerError::Cancelled,
        other => ProducerError::Merge(other),
    }
}

impl RowSource for ScanRowSource {
    fn next_step(&mut self) -> Result<SourceStep, cqlite_core::Error> {
        loop {
            if let Some((key, row)) = self.batch.next() {
                let decorated = self.decorate(&key);
                return Ok(SourceStep::Row(decorated, PendingRow::Scanned(key, row)));
            }
            match self.stream.next_batch() {
                None => return Ok(SourceStep::Complete),
                Some(Ok(QueryRowBatch::Rows(rows))) => {
                    self.emitted_any |= !rows.is_empty();
                    self.batch = rows.into_iter();
                }
                Some(Ok(QueryRowBatch::Unsupported)) => {
                    // The walk contracts to report this pre-emit only; a late
                    // one means the stream lied about what it had served.
                    return Err(cqlite_core::Error::corruption(format!(
                        "single-source query row stream reported Unsupported after \
                         emitting rows (emitted_any={}) — issue #3058",
                        self.emitted_any
                    )));
                }
                Some(Err(e)) => return Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
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
            bypass_reason(&[], &schema, ForcedMergePath::Auto, false),
            BypassReason::MultipleSources
        );
    }

    /// An aggregating request never selects the fast path, even with one source
    /// (belt-and-braces: the aggregate route returns earlier still).
    #[test]
    fn aggregating_request_never_selects_the_fast_path() {
        let schema = crate::testutil::simple_schema();
        assert_eq!(
            bypass_reason(&[], &schema, ForcedMergePath::Auto, true),
            BypassReason::Aggregating
        );
    }

    /// A schema declaring a STATIC column takes the merge arm, fail-closed: the
    /// two arms disagree on static-row shape today (see [`BypassReason::StaticColumns`]),
    /// so the fast path must not silently change those results.
    #[test]
    fn a_static_column_forces_the_merge_arm() {
        use crate::testutil::{simple_schema, write_row};
        let (_temp, readers) = open_readers(vec![vec![write_row(1, "a", 10, 100)]]);
        let mut schema = simple_schema();
        // An otherwise-selecting single-source request: only the static column
        // differs, so this isolates the static precondition.
        assert_eq!(
            bypass_reason(&readers, &schema, ForcedMergePath::Auto, false),
            BypassReason::Selected,
            "control: without the static column this request WOULD take the fast path"
        );
        if let Some(c) = schema.columns.iter_mut().find(|c| c.name == "name") {
            c.is_static = true;
        }
        assert_eq!(
            bypass_reason(&readers, &schema, ForcedMergePath::Auto, false),
            BypassReason::StaticColumns
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
            bypass_reason(&readers, &simple_schema(), ForcedMergePath::Auto, false),
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
            bypass_reason(&readers, &simple_schema(), ForcedMergePath::Auto, false),
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
            bypass_reason(&readers, &schema, ForcedMergePath::Auto, false),
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
            bypass_reason(&readers, &simple_schema(), ForcedMergePath::Bypass, false),
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

    /// Spec R1 (roborev, issue #3058): a non-frozen collection whose element/key
    /// is a frozen UDT takes the MERGE arm, so `SELECT *` cannot start working
    /// at one generation and erroring at two (#2339 fails the merge arm closed on
    /// exactly these columns). A `list<frozen<udt>>` is NOT affected — its cell
    /// path is a position TimeUUID, and the merge arm serves it.
    #[test]
    fn a_composite_keyed_collection_forces_the_merge_arm() {
        use crate::testutil::{simple_schema, write_row};
        let (_temp, readers) = open_readers(vec![vec![write_row(1, "a", 10, 100)]]);
        let base = simple_schema();
        assert_eq!(
            bypass_reason(&readers, &base, ForcedMergePath::Auto, false),
            BypassReason::Selected,
            "control: the plain schema WOULD take the fast path"
        );

        for refused in [
            "set<frozen<contact_info>>",
            "map<frozen<contact_info>, text>",
            "set<frozen<tuple<int, text>>>",
            "map<frozen<tuple<int, text>>, text>",
            "set<frozen<list<int>>>",
            // Unparsed STRUCTURE (uppercase): could be any collection → fail closed.
            "SET<FROZEN<CONTACT_INFO>>",
        ] {
            let mut schema = base.clone();
            if let Some(c) = schema.columns.iter_mut().find(|c| c.name == "name") {
                c.data_type = refused.to_string();
            }
            assert_eq!(
                bypass_reason(&readers, &schema, ForcedMergePath::Auto, false),
                BypassReason::CompositeKeyedCollection,
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
            // A bare UDT reference is ONE opaque cell, served identically by both
            // arms — it is not a composite-keyed collection.
            "contact_info",
        ] {
            let mut schema = base.clone();
            if let Some(c) = schema.columns.iter_mut().find(|c| c.name == "name") {
                c.data_type = allowed.to_string();
            }
            assert_eq!(
                bypass_reason(&readers, &schema, ForcedMergePath::Auto, false),
                BypassReason::Selected,
                "`{allowed}` is served identically by both arms and must stay on \
                 the fast path"
            );
        }
    }

    /// `merge` wins over everything, including an aggregating request — it is the
    /// field kill switch and must be absolute.
    #[test]
    fn forced_merge_is_absolute() {
        let schema = crate::testutil::simple_schema();
        assert_eq!(
            bypass_reason(&[], &schema, ForcedMergePath::Merge, false),
            BypassReason::ForcedMerge
        );
    }
}
