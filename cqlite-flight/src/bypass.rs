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
use cqlite_core::storage::write_engine::merge::{first_unorderable_leaf, UdtScope};
use cqlite_core::storage::write_engine::DecoratedKey;
use cqlite_core::types::{ComparatorType, ScanRow};
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
    /// Issue #2339 CLOSED the composite SET ELEMENT half: `read_assembly.rs` now
    /// decodes a `frozen<udt>`/`frozen<tuple>`/nested-`frozen<collection>` set
    /// element structurally from its `cell_path` with the same value deserializer
    /// the single-generation decoder uses, so such a column no longer forces the
    /// merge arm — PROVIDED the element type RESOLVES. The merge arm resolves UDT
    /// references through the ticket DDL's `UdtRegistry`, while the
    /// single-generation decoder resolves them from the SSTable's OWN marshal
    /// type; so a `set<frozen<udt>>` whose ticket DDL carries no matching
    /// `CREATE TYPE` still diverges (merge fails closed, fast arm succeeds) and is
    /// still refused.
    ///
    /// The composite MAP KEY half is still refused, with the divergence now on the
    /// OTHER side: the merge arm decodes the key structurally while the
    /// single-generation decoder's `parse_cell_path_key` (complex_column.rs) has no
    /// composite arm and falls back to an opaque `Value::Blob`. Closing that is the
    /// single-generation reader's job, not this assembler's.
    ///
    /// Without these guards `SELECT *` over such a table would return DIFFERENT
    /// values at one generation and at two — i.e. change after a flush and change
    /// back after a compaction. That is a query-result change, which the #3058
    /// contract (spec R6) forbids, so the schema takes the merge arm.
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
///
/// **PUBLIC ARITY IS PRESERVED (roborev job 116 F2).** `cqlite_flight::bypass` is a
/// public module, so adding a required parameter here is a breaking change for
/// downstream callers even though every in-tree caller was updated. The
/// registry-aware form is a SEPARATE name, exactly as #2339 already did for
/// `assemble_read_cells` / `assemble_read_cells_with_udts` in `cqlite-core`.
///
/// Passing no UDT scope is fail-CLOSED, not a silent downgrade: an unresolvable
/// composite element counts as divergent and vetoes the fast path.
pub fn bypass_reason(
    readers: &[Arc<SSTableReader>],
    schema: &TableSchema,
    forced: ForcedMergePath,
    aggregating: bool,
) -> BypassReason {
    bypass_reason_with_udts(readers, schema, forced, aggregating, None)
}

/// [`bypass_reason`] plus the ticket's UDT scope, which lets a RESOLVABLE composite
/// set element select the fast path (issue #2339). Separate name so the public
/// four-argument signature above keeps compiling for downstream callers.
pub fn bypass_reason_with_udts(
    readers: &[Arc<SSTableReader>],
    schema: &TableSchema,
    forced: ForcedMergePath,
    aggregating: bool,
    udts: Option<UdtScope<'_>>,
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
    // NO deletion guard here (issue #3140, RETIRED). A static-bearing SSTable that may
    // contain a simple CELL tombstone used to fail closed to the merge arm, because the
    // fast arm surfaced the deleted cell as a raw `Value::Tombstone` the Arrow encoder
    // rejected. The single-generation decoder now drops a simple cell tombstone at its
    // source (`row_decoder`'s `PartitionShadow::cell_tombstone_dropped`, PR #3122), so
    // both arms return Cassandra's rows with the column NULL and there is nothing left
    // to fail closed on. Pinned end to end on the CASSANDRA-WRITTEN
    // `test_tomb.static_with_tombstones` by `issue_3095_flight_static_columns.rs`, whose
    // `static_with_tombstones/select-star` case is now an ordinary both-arms
    // differential (and asserts the bypass leg built ZERO mergers, so it cannot pass by
    // silently routing back to the merge arm).
    if schema
        .columns
        .iter()
        .any(|c| declares_composite_keyed_collection(&c.data_type, udts))
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
/// | `Set(opaque composite)` RESOLVABLE through `registry` | EQUIVALENT since #2339 — both arms decode the element structurally from its `cell_path` | no |
/// | `Set(opaque composite)` NOT resolvable (a UDT reference the ticket DDL declared no `CREATE TYPE` for) | DIVERGENT — the merge arm fails closed, while the single-generation decoder resolves the element from the SSTable's OWN marshal type and succeeds | YES |
/// | `List(_)`, any element type | EQUIVALENT — sorted `Value::List` | no |
/// | `Map(scalar, _)` | EQUIVALENT — `Value::Map` | no |
/// | `Map(opaque composite, _)` | DIVERGENT — the merge arm now decodes the key structurally (#2339) while the single-generation decoder's `parse_cell_path_key` has no composite arm and falls back to an opaque `Value::Blob` (complex_column.rs). The divergence SWAPPED SIDES rather than closing; the remaining half is the single-generation reader's, not this assembler's | YES |
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
fn declares_composite_keyed_collection(data_type: &str, udts: Option<UdtScope<'_>>) -> bool {
    let Ok(parsed) = CqlType::parse(data_type) else {
        return true;
    };
    match parsed {
        // A composite SET element is decoded by BOTH arms since #2339 — but the
        // merge arm needs the type to RESOLVE (an all-lowercase UDT name parses to
        // a bare `Custom` with no field list), while the single-generation decoder
        // resolves it from the SSTable's own marshal type and never consults the
        // ticket registry. So an UNRESOLVABLE composite element still diverges and
        // is still refused.
        CqlType::Set(inner) => {
            // ARM-DEPENDENT SUCCESS IS WORSE THAN EITHER ARM'S OWN BEHAVIOUR
            // (roborev job 116 F1). Since #4063 the merged arm REFUSES to order a
            // composite whose leaf has no Cassandra-compatible ordering
            // (`varint`/`decimal`/`uuid`/`timeuuid`, and a `Custom` name with no
            // implemented ordering). A one-source read that bypassed merging would
            // decode and RETURN such a collection, so the very same query would
            // begin failing the moment a second SSTable appeared. Veto the fast
            // path so both arms fail closed identically and the behaviour does not
            // depend on how many files happen to be on disk.
            //
            // The leaf set is NOT restated here: `first_unorderable_leaf` is
            // cqlite-core's own predicate, so the bypass arm and the merged arm
            // cannot drift into two answers — the divergence class #2339 exists to
            // remove, one crate over.
            if merged_arm_refuses_ordering(&inner, udts) {
                return true;
            }
            is_opaque_composite(&inner) && !merge_arm_resolves_composite(&inner, udts)
        }
        // A composite MAP KEY is still divergent, in the opposite direction from
        // before #2339: the merge arm decodes it structurally, the
        // single-generation decoder's `parse_cell_path_key` has no composite arm
        // and falls back to an opaque `Value::Blob`. Closing that half belongs to
        // the single-generation reader.
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

/// Whether the MERGE arm can resolve `ty` — a composite collection element — to a
/// decodable STRUCTURE, i.e. whether `read_assembly.rs`'s composite decode will
/// succeed rather than fail closed (issue #2339).
///
/// `read_assembly` builds the element comparator with
/// `ComparatorType::from_cql_type_with_registry`, which leaves a UDT reference it
/// cannot find in the registry as a bare `ComparatorType::Custom` — a type with no
/// field list, which the assembler then (correctly) refuses rather than guess. This
/// mirrors that resolution ON THE DECLARED TYPE ONLY (no-heuristics, issue #28):
/// resolve through the registry, then require that NO unresolved UDT reference
/// remains anywhere in the type tree.
///
/// No scope ⇒ nothing can resolve ⇒ `false` (refuse the fast arm), which is the
/// fail-closed direction.
/// True when the MERGED arm would refuse to ORDER `ty` because a leaf has no
/// Cassandra-compatible ordering (issue #4063), asked of cqlite-core's
/// `first_unorderable_leaf` so there is ONE authority for that question.
///
/// **Asked on the RESOLVED type, and deliberately NOT an answer about resolvability.**
/// The two refusals are different and must not be conflated: an UNRESOLVED UDT name
/// is a bare `Custom("contact_info")`, for which `supports_ordering()` is false, so
/// asking this on the raw declaration would refuse every unresolved composite as
/// "unorderable" — over-broad, and it reds the resolvable-element fast path #2339
/// exists to enable. Resolvability already has its own veto
/// (`!merge_arm_resolves_composite`), and the merged arm has its own separate
/// fail-closed path for it (`first_unresolved_custom`).
///
/// So when the type cannot be resolved here this returns FALSE and defers — which is
/// not a hole: the caller's resolvability veto fires on exactly that case, and the
/// merged arm refuses it independently.
fn merged_arm_refuses_ordering(ty: &CqlType, udts: Option<UdtScope<'_>>) -> bool {
    let Some(udts) = udts else {
        return false;
    };
    let resolved = udts.registry.resolve_type(ty, udts.keyspace);
    match ComparatorType::from_cql_type(&resolved) {
        Ok(cmp) => first_unorderable_leaf(&cmp).is_some(),
        // A comparator that cannot be BUILT is not an ordering verdict; the
        // resolvability veto owns that case.
        Err(_) => false,
    }
}

fn merge_arm_resolves_composite(ty: &CqlType, udts: Option<UdtScope<'_>>) -> bool {
    let Some(udts) = udts else {
        return false;
    };
    fully_resolved(&udts.registry.resolve_type(ty, udts.keyspace))
}

/// Whether every UDT reference in `ty` carries its field list — the property
/// `ComparatorType::from_cql_type_with_registry` needs to produce a decodable
/// comparator (see [`merge_arm_resolves_composite`]).
fn fully_resolved(ty: &CqlType) -> bool {
    match ty {
        CqlType::Frozen(inner) | CqlType::List(inner) | CqlType::Set(inner) => {
            fully_resolved(inner)
        }
        CqlType::Map(k, v) => fully_resolved(k) && fully_resolved(v),
        CqlType::Tuple(fields) => fields.iter().all(fully_resolved),
        CqlType::Udt(_, fields) => {
            !fields.is_empty() && fields.iter().all(|(_, t)| fully_resolved(t))
        }
        // A `Custom` that survived resolution is either an unresolved UDT reference
        // or a genuinely unknown type: undecodable either way, EXCEPT the two names
        // the scalar codec handles (which are not composites and never reach here).
        CqlType::Custom(name) => {
            let bare = name.rsplit(':').next().unwrap_or(name);
            bare == "time" || bare == "inet"
        }
        _ => true,
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
