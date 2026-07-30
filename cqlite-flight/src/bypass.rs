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
//! [`bypass_reason`] consults exactly these inputs, and nothing else:
//! * the POST-prune source count (from the authoritative `*-Data.db` listing);
//! * the reader's parsed COMPONENT metadata
//!   (`SSTableReader::supports_streaming_query_scan`);
//! * the reader's own SERIALIZATION HEADER for on-disk static columns
//!   (`on_disk_static_columns` / `static_columns_are_known`) — authoritative
//!   on-disk metadata, consulted because the caller schema alone cannot settle
//!   the static question (see [`BypassReason::StaticColumns`]);
//! * the CALLER-SUPPLIED schema (the ticket DDL) for `dropped_columns`, declared
//!   static columns, and declared column TYPES. This is caller INPUT, not on-disk
//!   metadata — it is the decode contract the request itself supplies, and it is
//!   what makes the type-shape guards expressible at all. Where it is not
//!   sufficient alone (the static question) the on-disk header above is consulted
//!   too;
//! * the aggregation flag and the forced-path override.
//!
//! It never looks at a file size, a `Statistics.db` row/size ESTIMATE, or SSTable
//! byte content, and never infers a type or a behaviour from a byte pattern
//! (issue #28). Anything that cannot be established takes the slow,
//! known-correct merge arm.
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
// `map_core_error` is the ONE core-error -> producer-taxonomy mapping (issue
// #2264: map by VARIANT, never by racing the cancel flag), shared with the merge
// arm rather than duplicated per source.
use crate::row_source::{map_core_error, PendingRow, RowSource, SourceStep};

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
    /// The schema declares a MULTI-CELL (non-frozen) column whose two arms do not
    /// collapse identically: a "composite-keyed" collection (a non-frozen `set`
    /// whose element, or `map` whose key, is a frozen UDT/tuple/nested
    /// collection), OR a non-frozen top-level UDT.
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
    ///
    /// A non-frozen top-level UDT diverges the same way but SILENTLY rather than
    /// by failing closed: `assemble_complex`'s `_` fall-through keeps only the
    /// LAST element's scalar, while the single-generation decoder assembles the
    /// full `Value::Udt` (#927/#1081). Same hazard, same treatment.
    /// [`declares_composite_keyed_collection`] tabulates every
    /// `assemble_complex` arm and which of them diverge.
    MulticellArmDivergence,
    /// The SSTable's static-column content cannot be settled against the caller
    /// schema, so the two arms could disagree on static-row shape.
    ///
    /// Both arms now implement Cassandra's `processPartition()` static semantics
    /// (issue #3095): statics are merged into every clustering row and a
    /// static-only partition returns exactly one row on the merge arm
    /// (`statics::StaticMergeSource`) and on the single-generation arm (the
    /// decoder's own static injection plus its static-only-partition emission).
    /// A schema that DECLARES its static columns is therefore servable by the fast
    /// path, and the equality is proven over Cassandra-written bytes by
    /// `cqlite-flight/tests/issue_3095_flight_static_columns.rs`.
    ///
    /// Two cases still fail closed, because the arms' static handling is keyed off
    /// the CALLER SCHEMA and would diverge without it:
    /// * a static column present in the SSTable's own serialization header but NOT
    ///   declared by the ticket DDL (a DDL predating an
    ///   `ALTER TABLE ADD … STATIC`, or a hand-built ticket; an `nb` header carries
    ///   no embedded schema to cross-check against, #3097). The single-generation
    ///   decoder would still surface that partition's static-only row while the
    ///   merge arm, seeing no declared static column, would not adapt at all;
    /// * an SSTable whose serialization header could not be parsed, where the
    ///   question cannot be answered at all
    ///   ([`SSTableReader::static_columns_are_known`]).
    StaticColumns,
    /// A STATIC-bearing table whose SSTable declares at least one DELETION, so it may
    /// contain a simple CELL tombstone — a shape on which the two arms genuinely
    /// diverge today (issue #3140).
    ///
    /// Mechanism, measured on the CASSANDRA-WRITTEN `test_tomb.static_with_tombstones`
    /// (one generation; its `ck = 3` carries a cell tombstone on `row_col`):
    /// * the MERGE arm is CORRECT — `assemble_read_cells` drops a simple cell
    ///   tombstone, the column reads null, and `SELECT *` returns Cassandra's 3 rows;
    /// * the single-generation FAST arm surfaces the tombstone as a raw
    ///   `Value::Tombstone`, and the Arrow encoder then hard-errors
    ///   (`expected Text value, got Tombstone(..)`), aborting `do_get` with ZERO rows.
    ///
    /// That is a read-path ARM DIVERGENCE on real Cassandra bytes — precisely what this
    /// predicate exists to fail closed on — and it is DISTINCT from #3094, which is a
    /// CQLite-WRITTEN shape reproducing identically on BOTH arms (so not a divergence
    /// at all). Fixing the fast arm's cell-tombstone handling is issue #3140; when that
    /// lands, THIS variant and its predicate branch are retired and
    /// `test_tomb.static_with_tombstones` becomes an ordinary both-arms differential.
    ///
    /// Deliberately scoped to STATIC-bearing tables — the tables #3095 newly admitted
    /// to the fast arm. The same fast-arm defect exists for a NON-static
    /// single-generation table, but that is pre-existing #3058 behaviour this change
    /// does not touch (and is #3140's remit). Narrowing further is not possible from
    /// authoritative metadata: `EncodingStats.minLocalDeletionTime` cannot tell a cell
    /// tombstone from a row/range/partition one or a TTL'd cell (see
    /// [`SSTableReader::may_contain_deletions`]), so a static-bearing file that
    /// declares NO deletion at all still takes the fast arm.
    StaticColumnsWithDeletions,
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
    // Statics (issue #3095): both arms now implement Cassandra's
    // `processPartition()` semantics, so a static-bearing table IS servable by the
    // fast path — but only when the caller schema DECLARES the file's static
    // columns, because each arm's static handling is keyed off that schema.
    //
    // SCHEMA AUTHORITY (issue #3097, which settled it): the CALLER's ticket schema is
    // authoritative for DECODE on both arms — #3097 threaded it into the merge arm's
    // enumeration so a clustering column no longer decodes under the reader header's
    // placeholder name. `schema` here IS that authoritative schema, so this predicate
    // and #3097 read the same source of truth. The file's own serialization header is
    // consulted for ONE narrow, different purpose: detecting that the caller schema is
    // STALE (an `nb` header carries no embedded schema to cross-check against, so a DDL
    // predating an `ALTER TABLE ADD … STATIC` would otherwise sail through). That is a
    // staleness cross-check, NOT a competing notion of decode authority.
    if !only.static_columns_are_known() {
        return BypassReason::StaticColumns;
    }
    let declared_static: std::collections::HashSet<&str> = schema
        .columns
        .iter()
        .filter(|c| c.is_static)
        .map(|c| c.name.as_str())
        .collect();
    if only
        .on_disk_static_columns()
        .iter()
        .any(|on_disk| !declared_static.contains(on_disk.as_str()))
    {
        return BypassReason::StaticColumns;
    }
    // Issue #3140 (fail-closed, scoped to the static-bearing tables #3095 newly
    // admitted): a simple CELL tombstone diverges between the arms — the merge arm
    // drops it (column reads null, matching Cassandra) while the fast arm surfaces a
    // raw `Value::Tombstone` the Arrow encoder rejects, aborting `do_get` with zero
    // rows. The narrowest AUTHORITATIVE pre-read signal is "this file declares a
    // deletion at all" (`EncodingStats.minLocalDeletionTime`); the metadata cannot
    // single out a cell tombstone. A static-bearing file that declares NO deletion
    // still takes the fast arm.
    let has_statics = !declared_static.is_empty() || !only.on_disk_static_columns().is_empty();
    if has_statics && only.may_contain_deletions() {
        return BypassReason::StaticColumnsWithDeletions;
    }
    if schema
        .columns
        .iter()
        .any(|c| declares_composite_keyed_collection(&c.data_type))
    {
        return BypassReason::MulticellArmDivergence;
    }
    if !only.supports_streaming_query_scan() {
        return BypassReason::ReaderUnsupported;
    }
    BypassReason::Selected
}

/// Whether `data_type` declares a MULTI-CELL (non-frozen) column shape the two
/// arms do NOT collapse identically — see
/// [`BypassReason::MulticellArmDivergence`].
///
/// Mirrors `write_engine/merge/read_assembly.rs::assemble_complex` ARM BY ARM —
/// that function is the ONLY place the merge arm collapses a multi-cell column,
/// so its arms are the complete divergence surface. Enumerated exhaustively so a
/// future member of this class is not discovered one at a time:
///
/// | `assemble_complex` arm | vs the single-generation collapse | refused here |
/// |---|---|---|
/// | `Set(scalar)` | EQUIVALENT — sorted `Value::Set` | no |
/// | `Set(opaque composite)` | DIVERGENT — fails closed (#2339) | YES |
/// | `List(_)`, any element type | EQUIVALENT — sorted `Value::List` | no |
/// | `Map(scalar, _)` | EQUIVALENT — `Value::Map` | no |
/// | `Map(opaque composite, _)` | DIVERGENT — fails closed (#2339) | YES |
/// | `_` fall-through, i.e. a NON-FROZEN top-level UDT (or other non-collection complex column) | DIVERGENT SILENTLY — merge arm returns `last_value(elements)`, only the LAST element's scalar, while the single-generation decoder assembles the full `Value::Udt` (#927/#1081) | YES |
/// | column not declared in the caller schema | `last_value`, but such columns are never emitted to Arrow | no — unobservable |
/// | `CqlType::parse` error on the declared type | DIVERGENT — merge arm errors, fast arm decodes | YES — unparseable is refused |
///
/// "Opaque composite" is `read_assembly.rs`'s own `key_is_opaque_composite` rule:
/// after unwrapping `frozen`, a tuple / UDT / nested collection, or a `Custom`
/// name other than the two the scalar codec decodes (`time`, `inet`).
///
/// FROZEN shapes are excluded throughout: a frozen collection or frozen UDT is
/// ONE cell, so it never reaches `assemble_complex`'s multi-element path and both
/// arms serve it identically.
fn declares_composite_keyed_collection(data_type: &str) -> bool {
    let Ok(parsed) = CqlType::parse(data_type) else {
        return true;
    };
    match parsed {
        CqlType::Set(inner) => is_opaque_composite(&inner),
        CqlType::Map(key, _) => is_opaque_composite(&key),
        // A LIST is element-for-element equivalent on both arms: its cell path is
        // a position TimeUUID, so the order is authoritative either way — even for
        // a `frozen<UDT>` element.
        CqlType::List(_) => false,
        // A NON-FROZEN, top-level MULTI-CELL complex column — a bare UDT
        // reference (`Custom`, e.g. `contact_info`), or an explicit `Udt`/`Tuple` —
        // lands on `assemble_complex`'s `_` fall-through, which keeps only the
        // LAST element's value while the single-generation decoder assembles the
        // whole `Value::Udt` (#927/#1081). `CqlType::parse` also yields `Custom`
        // for a type string whose structure it could not parse, which is refused
        // for the same fail-closed reason.
        CqlType::Custom(_) | CqlType::Udt(_, _) | CqlType::Tuple(_) => true,
        // `Frozen(_)` and scalars are ONE cell, so they never reach
        // `assemble_complex`'s multi-element path at all.
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
            Some(Err(e)) => return Err(map_core_error(e)),
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

impl RowSource for ScanRowSource {
    fn next_step(&mut self) -> Result<SourceStep, ProducerError> {
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
                    return Err(ProducerError::Merge(cqlite_core::Error::corruption(
                        format!(
                            "single-source query row stream reported Unsupported after \
                             emitting rows (emitted_any={}) — issue #3058",
                            self.emitted_any
                        ),
                    )));
                }
                Some(Err(e)) => return Err(map_core_error(e)),
            }
        }
    }
}

#[cfg(test)]
#[path = "bypass_tests.rs"]
mod tests;
