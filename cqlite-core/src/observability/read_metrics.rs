//! Read-path metric emission at BATCH granularity (issue #1701, epic #1686).
//!
//! # Why this module exists
//!
//! The four headline read metrics — [`catalog::READ_ROWS`], [`catalog::READ_BYTES`],
//! [`catalog::READ_PARTITIONS`] and [`catalog::READ_DURATION`] — were documented,
//! registered as instruments, rendered in the operator metric reference and shown
//! in this module's parent doc example, while NO production read path ever updated
//! them (finding AI1 of `docs/reports/platform-observability-audit-2026-07-01.md`).
//! A documented metric that is never written is worse than an absent one: an
//! operator's dashboard shows a flat zero and reads it as "no reads happening".
//!
//! # The granularity rule
//!
//! The span-granularity doctrine in [`catalog`] applies to metrics too: emission is
//! per read OPERATION, never per row. [`ReadOpMeter`] accumulates a scan's rows and
//! partitions in two `u64` fields and emits ONE counter add per metric plus ONE
//! duration recording when the operation ends. `read.bytes` is emitted once per
//! decompressed chunk ([`record_decompressed_bytes`]) — the coarsest grain at which
//! the decompressed size is actually known.
//!
//! # Which SUBSYSTEM's bytes `read.bytes` counts (issue #1701, roborev F3)
//!
//! `read.rows` / `read.partitions` / `read.duration` are emitted at an ENUMERATED set
//! of core query read boundaries — `SSTableManager`'s materializing scans and point
//! reads, and the `JoinedStream` streaming handles — so a compaction can never
//! contribute to them: compaction reads its inputs through
//! `stream_all_partitions_for_compaction` on a reader directly, never through those
//! boundaries.
//!
//! That set is a CLAIM ABOUT THOSE BOUNDARIES, not about the whole product. A caller
//! that reaches the readers by another route is NOT metered by this module — notably
//! the Arrow Flight `do_get` path, which drives `KWayMerger`/`QueryRowStream` directly
//! and emits its own rows/partitions from `cqlite-flight`'s `ScanProgressMeter` with no
//! duration at all. Extending duration coverage there needs Flight-level metering and
//! capture infrastructure in that crate, and is deliberately out of scope here; nothing
//! in this module or the operator reference should be read as claiming it. Flight reads
//! DO contribute `read.bytes`, because they decode through the same chunk plane.
//!
//! `read.bytes` is different, and deliberately so. It is credited inside the CHUNK
//! DECODE PLANE (`reader::chunk_source`), which is handed a `ReadAt` + a
//! `CompressionInfo` and cannot know WHO asked — the same plane serves a query read,
//! a compaction read, and a verification scan. So the metric answers "how many
//! `Data.db` bytes did this process materialise", including compaction's own reads.
//!
//! That is a real limitation for an operator reading query amplification off
//! `read.bytes` while a compaction runs, and the honest fix is NOT to guess at the
//! plane: it needs a read-PURPOSE dimension threaded from each caller (or a separate
//! compaction-bytes-read instrument), which reaches into the compaction merge
//! producer and is a change to compaction plumbing rather than metric wiring. It is
//! therefore recorded here and left to a follow-up, not silently approximated: the
//! plane never invents a purpose label it cannot know, exactly as it never invents
//! the SSTable format label.
//!
//! # Zero-cost when off
//!
//! [`ReadOpMeter::start`] consults [`obs::metrics_active`] ONCE (the issue #2819 M1
//! pattern) and returns an INERT meter when metrics are not being collected: no
//! [`Instant`] is sampled, no key is retained, and every later `record_*` call is a
//! single branch on a `None`. With the `observability` feature off,
//! `metrics_active()` is a compile-time `false`, so the whole meter degenerates to
//! that branch and the emission helpers compile to no-ops.

use std::sync::Arc;
use std::time::{Duration, Instant};

use super::read_phase::{ReadPhase, ReadPhaseTimings};
use super::{catalog, AttrValue};
use crate::observability as obs;
use crate::storage::sstable::compression::CompressionAlgorithm;
use crate::types::RowKey;

/// The bounded [`catalog::attr::COMPRESSION`] value for "this SSTable has no
/// compressor", so an uncompressed read is attributed to a NAMED series rather than
/// an anonymous one (issue #1701). Same spelling the write side uses for
/// `CompressionAlgorithm::None`.
pub(crate) const COMPRESSION_NONE: &str = "none";

/// Map a compression algorithm to its bounded [`catalog::attr::COMPRESSION`] value.
///
/// Bounded by the `CompressionAlgorithm` enum itself (five variants), never by a
/// string read out of `CompressionInfo.db` — an on-disk algorithm name is
/// file-controlled and would be an unbounded metric dimension. Mirrors the write
/// side's `CompressedDataWriter::compression_attr` (issue #1036).
pub(crate) fn compression_attr(algorithm: &CompressionAlgorithm) -> &'static str {
    match algorithm {
        CompressionAlgorithm::Lz4 => "lz4",
        CompressionAlgorithm::Snappy => "snappy",
        CompressionAlgorithm::Deflate => "deflate",
        CompressionAlgorithm::Zstd => "zstd",
        CompressionAlgorithm::None => COMPRESSION_NONE,
    }
}

/// Count `bytes` of `Data.db` payload (post-decompression) into
/// [`catalog::READ_BYTES`], tagged with the bounded `compression` label.
///
/// Called once per chunk the read path materialises, from the single chunk decode
/// plane (`reader::chunk_source`) — every one of its four decode exits, INCLUDING
/// the ones that hand back raw bytes: an UNCOMPRESSED SSTable (`compression =
/// "none"`) and a Cassandra-stored-raw incompressible chunk read `Data.db` payload
/// exactly like a decompressed chunk does. Uncompressed is a first-class read path,
/// not an edge case — CQLite's own production write surface emits uncompressed
/// SSTables only (the #1406 claim boundary) — so a `"none"` read that went
/// uncounted would make the metric silently understate real I/O.
///
/// A resident decompressed-chunk cache HIT is counted too, and the reason is a fact
/// about the feed rather than a preference (issue #1701, roborev round 5): the
/// windowed scan calls `read_compressed_chunk_sync` /
/// `read_uncompressed_piece_sync` UNCONDITIONALLY and only then asks the cache, and
/// neither reader consults it — so the disk read has ALREADY happened and that cache
/// saves DECOMPRESSION, not I/O. An earlier version of this doc asserted the
/// opposite, and a test asserted it too; they agreed with each other and were both
/// wrong, which is how a warm scan came to report zero bytes for I/O it really
/// performed. The one place a read is genuinely skipped — `get_cached_data`'s
/// early return, which reads no `Data.db` at all — stays uncounted.
///
/// The label is a `&'static str` from [`compression_attr`]'s closed mapping, so this
/// costs no allocation and no formatting on the per-chunk path.
pub(crate) fn record_decompressed_bytes(bytes: usize, compression: &'static str) {
    if bytes == 0 {
        return;
    }
    obs::add_counter(
        catalog::READ_BYTES,
        bytes as u64,
        &[(
            catalog::attr::COMPRESSION,
            AttrValue::StaticStr(compression),
        )],
    );
}

/// One read OPERATION's row/partition/duration accounting, emitted ONCE at the end.
///
/// A scan stream owns one of these for its whole lifetime; a point read owns one for
/// the duration of the lookup. [`finish`](Self::finish) is idempotent and is also
/// called from `Drop`, so an ABANDONED read (the common `LIMIT` shape: the consumer
/// stops polling and drops the stream) still reports the work it did instead of
/// vanishing from the metrics.
pub(crate) struct ReadOpMeter(Option<Accounting>);

struct Accounting {
    started: Instant,
    /// `Some(label)` when the operation reads ONE SSTable of a known format;
    /// `None` for a cross-generation merge, whose reconciled rows come from
    /// possibly mixed-format inputs, so no single format label is honest
    /// (the rule [`catalog::READ_ROWS`] documents).
    format: Option<&'static str>,
    rows: u64,
    partitions: u64,
    /// The previous row's partition key. Scan producers emit rows GROUPED BY
    /// partition in on-disk (token) order and every emitted row carries its
    /// PARTITION key — the row key is the one decoded from the partition header
    /// (`row_decoder/row_framing.rs::parse_partition_header_full`, cloned onto each
    /// of that partition's rows in `row_decoder/block_emit.rs`) — so a CHANGE of key
    /// is a partition boundary.
    ///
    /// That is not a new inference: it is the same rule the read-work probe already
    /// applies at the BTI and BIG walks ("changed partition key = one more partition
    /// BODY decoded", `work_counters::add_stream_walk_partition_parsed` call sites,
    /// issues #2398/#3109). Because emission is token-ordered, a partition appearing
    /// in several inputs of a merge arrives as ONE adjacent run and is counted once.
    ///
    /// Retained as an `Arc` clone (a refcount bump, no key copy).
    last_partition: Option<Arc<[u8]>>,
    /// This operation's read-PHASE accumulator (issue #1707), shared with the
    /// pipeline threads that do the io/decompress/decode/merge work. Owned here so
    /// the four `cqlite.read.phase.*` histograms inherit the whole `ReadOpMeter`
    /// lifecycle — idempotent [`finish`](ReadOpMeter::finish), `Drop`-safe, and
    /// absent entirely on an [`inert`](ReadOpMeter::inert) sub-scan, so a fan-out
    /// merge's per-generation scans can never double-count.
    phases: Arc<ReadPhaseTimings>,
    emitted: bool,
}

impl ReadOpMeter {
    /// A meter for a read operation that SHOULD be measured.
    ///
    /// `format` is the single-SSTable format label, or `None` for a merged
    /// (format-agnostic) grain. Returns an inert meter when metrics are not being
    /// collected, so nothing — not even the `Instant` sample — is paid for.
    pub(crate) fn start(format: Option<&'static str>) -> Self {
        if !obs::metrics_active() {
            return Self(None);
        }
        Self(Some(Accounting {
            started: Instant::now(),
            format,
            rows: 0,
            partitions: 0,
            last_partition: None,
            phases: Arc::new(ReadPhaseTimings::default()),
            emitted: false,
        }))
    }

    /// A meter that never records and never emits.
    ///
    /// Used by the stream boundaries that are NOT a top-level read operation: a
    /// fan-out merge's per-generation sub-scan (the merge holds the operation) and
    /// the per-row → batch re-chunker (its source stream is already measured).
    /// Measuring those would count the same rows two or three times.
    pub(crate) fn inert() -> Self {
        Self(None)
    }

    /// This operation's read-phase accumulator, for propagation to the threads that
    /// actually perform the work (issue #1707), or `None` for an inert meter.
    ///
    /// Handed to each `spawn_blocking` / producer-thread closure at its SPAWN SITE
    /// and re-installed there: thread-locals are not inherited across a spawn, and
    /// the io/decompress/decode/merge phases all run on threads that never see this
    /// meter. `None` (an inert meter, or metrics not being collected) propagates as
    /// "no sink", which makes every seam a single thread-local peek.
    pub(crate) fn phase_sink(&self) -> Option<Arc<ReadPhaseTimings>> {
        self.0.as_ref().map(|acc| Arc::clone(&acc.phases))
    }

    /// Account one delivered row, and a partition boundary when its key differs
    /// from the previous row's.
    ///
    /// # `read.rows` counts rows this read DELIVERED, not every row decoded
    ///
    /// An ABANDONED stream reports the rows it polled plus the rows the producer had
    /// already enqueued (the `Drop` drain). A row the producer was mid-`send` with when
    /// the channel closed is NOT counted: it was decoded, then rejected and discarded,
    /// so it never reached the consumer and could never have appeared in a result set
    /// (issue #1701, roborev round 5). Counting it would let `read.rows` exceed the rows
    /// the query could possibly have returned, which is the less useful of the two
    /// errors. The residual is bounded by the channel's in-flight sends, not by the
    /// backlog — that unbounded case was the round-2 defect, and closing this one means
    /// accounting at the producer's materialization boundary through shared state, which
    /// is a redesign of this seam rather than a fix to it.
    ///
    /// # `read.partitions` counts partitions this read DELIVERED rows from
    ///
    /// Boundaries come from the keys of EMITTED rows, so a partition whose every row is
    /// shadowed by a tombstone or expired by TTL emits nothing and contributes ZERO —
    /// while Flight's k-way MERGE arm calls its own `record_partition()` for a partition
    /// it scanned, and so counts it. The gap between the two producers is exactly the
    /// number of fully-suppressed partitions (issue #1701, roborev round 5).
    ///
    /// That asymmetry is NOT introduced here, and it is already documented on the Flight
    /// side between Flight's OWN two arms (`cqlite-flight/src/row_source.rs`): the
    /// single-generation arm "CANNOT" surface such partitions because "the walk emits
    /// only SURVIVING rows … the source never learns it existed", and closing it "would
    /// need a new partition-boundary signal threaded out of two core walks".
    ///
    /// Wiring that signal is deliberately NOT attempted here — this issue's scope is
    /// emitting the metric at batch granularity from the seams that already exist, and
    /// threading a new boundary signal through two core walks is a separate change with
    /// its own parity risk. Recorded instead of quietly leaving the two counts to differ,
    /// because a metric that means two things across producers is worse for an operator
    /// than one that means a narrower thing consistently. `read.rows` is unaffected: a
    /// suppressed partition materialises no rows on either producer.
    pub(crate) fn record_row(&mut self, key: &RowKey) {
        let Some(acc) = self.0.as_mut() else {
            return;
        };
        acc.rows = acc.rows.saturating_add(1);
        let same_partition = acc
            .last_partition
            .as_ref()
            .is_some_and(|prev| Arc::ptr_eq(prev, &key.0) || **prev == *key.0);
        if !same_partition {
            acc.partitions = acc.partitions.saturating_add(1);
            acc.last_partition = Some(Arc::clone(&key.0));
        }
    }

    /// Account every row of an ALREADY-MATERIALIZED result (issue #1701 F1).
    ///
    /// The materializing read boundaries (`SSTableManager::scan` and the
    /// partition-targeted scans) hand their whole `Vec` here in one call, so the
    /// emission stays at operation granularity — this is bookkeeping over a result
    /// the caller already holds, not a per-row emission. Written to take an iterator
    /// of KEYS so a 2-tuple `(key, row)` and a 3-tuple `(key, row, cell_metadata)`
    /// result both fit (`rows.iter().map(|(k, ..)| k)`).
    pub(crate) fn record_keys<'a>(&mut self, keys: impl IntoIterator<Item = &'a RowKey>) {
        if self.0.is_none() {
            return;
        }
        for key in keys {
            self.record_row(key);
        }
    }

    /// Suppress this meter permanently: neither [`finish`](Self::finish) nor `Drop`
    /// will emit (issue #1701, roborev round 4).
    ///
    /// # KNOWN GAP a declining boundary leaves, stated rather than implied
    ///
    /// The declining attempt's own latency is EXCLUDED from the logical read: the
    /// fallback path starts a fresh meter, so whatever the attempt spent — reader
    /// resolution, candidate pruning, and for the reverse seam possibly a promoted-index
    /// decode — is reported by nobody (issue #1701, roborev round 5). By the
    /// entry-at-function rule (`manager_point_read`'s module doc) it SHOULD be counted:
    /// excluding setup hides exactly the stall an operator hunts.
    ///
    /// Closing it means ONE meter owned by the common caller
    /// (`query::select_executor::lookup`) and threaded into both attempts — the pattern
    /// `stream_generations_for_read` now uses. Not done here: it changes a public-ish
    /// manager signature with several callers and edits `storage/sstable/mod.rs`, which is
    /// far over the campsite threshold, so it belongs in its own reviewed change rather
    /// than a fifth review round of this one. Bounded meanwhile: a decline on the cheap
    /// eligibility checks (static columns, non-fixed clustering) costs almost nothing;
    /// only a decline past those carries real excluded latency.
    ///
    /// For a read boundary that may DECLINE the read and hand it to a different,
    /// ALREADY-METERED path. `Drop` emits, so simply returning would report a
    /// duration for the attempt AND another for the path that actually served the
    /// read — two operations for one logical read, which is exactly the
    /// over-counting roborev B1 removed. The declining boundary therefore discards
    /// its meter rather than dropping it. Distinct from [`inert`](Self::inert),
    /// which is chosen at CONSTRUCTION for a boundary that is never an operation;
    /// this is decided at RUNTIME, after the attempt has already begun timing.
    ///
    /// Gated to the build that HAS a declining boundary: the only one is
    /// `SSTableManager::scan_partition_clustering_reverse`, and `reverse_scan.rs` is
    /// itself `#![cfg(not(feature = "tombstones"))]` (the seek/reverse paths exist only
    /// on the default build). Under `--all-features` that module is compiled out, so an
    /// ungated `discard` is dead code and fails the crate's `-D warnings` — and a blanket
    /// `#[allow(dead_code)]` would silence the NEXT genuinely-dead method too. Widen this
    /// cfg when a second declining boundary appears outside that module.
    #[cfg(not(feature = "tombstones"))]
    pub(crate) fn discard(&mut self) {
        if let Some(acc) = self.0.as_mut() {
            acc.emitted = true;
        }
    }

    /// Emit this operation's totals. Idempotent: the second call is a no-op, so a
    /// stream that observed its own end of stream and is then dropped emits once.
    pub(crate) fn finish(&mut self) {
        let Some(acc) = self.0.as_mut() else {
            return;
        };
        if acc.emitted {
            return;
        }
        acc.emitted = true;

        // Held in a local so the borrowed attribute slice below outlives the calls.
        let format_attr = acc
            .format
            .map(|f| [(catalog::attr::SSTABLE_FORMAT, AttrValue::StaticStr(f))]);
        let attrs: &[(&'static str, AttrValue)] = match format_attr.as_ref() {
            Some(a) => a,
            None => &[],
        };

        if acc.rows > 0 {
            obs::add_counter(catalog::READ_ROWS, acc.rows, attrs);
        }
        if acc.partitions > 0 {
            obs::add_counter(catalog::READ_PARTITIONS, acc.partitions, attrs);
        }
        // Always ONE recording per completed operation, including a read that
        // returned nothing: a zero-row read still consumed latency, and dropping it
        // would bias the distribution toward the rows-returning reads.
        obs::record_histogram(
            catalog::READ_DURATION,
            acc.started.elapsed().as_secs_f64(),
            attrs,
        );

        // The four read PHASES (issue #1707): ONE sample per phase per completed
        // operation, from the counters the pipeline threads accumulated into.
        //
        // A phase with ZERO accumulated nanos is SKIPPED, not recorded as `0.0`, and
        // that asymmetry with `read.duration` above is deliberate: a zero there means
        // "measured, and it was fast", while a zero here means the phase NEVER RAN —
        // an uncompressed SSTable decompresses nothing, a single-generation scan
        // merges nothing. Recording `0.0` would assert a measurement that was never
        // taken, and would drag every percentile of a real phase toward zero for
        // every read that does not perform it.
        //
        // ATTRIBUTE-FREE, deliberately, and NOT `attrs`: this family is declared
        // "**Attributes**: none" by `catalog_read_phase` and carries an empty
        // `attributes` in every `operator_docs_annotations_read_phase` row, so
        // emitting `cqlite.sstable.format` here would make the emission and the
        // declaration disagree — two statements of one fact, one of them wrong.
        // The declaration is the one to keep: the merge phase is format-agnostic
        // (it spans generations), so a per-format phase dimension could not be
        // honestly populated across the family anyway. `read.duration` above keeps
        // `attrs` — its format attribute IS declared.
        //
        // PARTIAL-SNAPSHOT CAVEAT (issue #1707): the surrounding prose says "ONE
        // sample per phase per COMPLETED scan", and completion here means THIS
        // METER completed — `finish()` runs when the stream is dropped. The
        // detached feed/parse tasks hold their own `Arc<ReadPhaseTimings>` clone
        // and keep accumulating into it after that, so an ABANDONED scan (a `LIMIT`
        // the consumer stopped reading, a dropped stream) emits a PARTIAL snapshot
        // of an operation still in flight, not a final total. That is the same
        // shape `read.duration` already has (it measures until the drop, not until
        // the pipeline quiesces) and is why the emission is here rather than behind
        // a join: waiting for the detached tasks would make an abandoned scan's
        // metric arrive late, or never.
        let phase_attrs: &[(&'static str, AttrValue)] = &[];
        for (phase, name) in [
            (ReadPhase::Io, catalog::READ_PHASE_IO),
            (ReadPhase::Decompress, catalog::READ_PHASE_DECOMPRESS),
            (ReadPhase::Decode, catalog::READ_PHASE_DECODE),
            (ReadPhase::Merge, catalog::READ_PHASE_MERGE),
        ] {
            let nanos = acc.phases.nanos(phase);
            if nanos > 0 {
                obs::record_histogram(name, Duration::from_nanos(nanos).as_secs_f64(), phase_attrs);
            }
        }
    }
}

impl Drop for ReadOpMeter {
    fn drop(&mut self) {
        self.finish();
    }
}
