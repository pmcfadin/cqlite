//! Operator annotations for the read-PHASE timing histograms and the
//! reader/WAL resource gauges (issue #1707, AI7 of epic #1686).
//!
//! A SECOND annotation table beside `operator_docs_annotations.rs`, which was
//! already at the campsite-rule source target (#1116): the table is append-only
//! data, so a new family gets a new file rather than growing the existing one.
//! Both are listed in `operator_docs_annotations::ANNOTATION_TABLES` and the
//! renderer walks the union in catalog order.
//!
//! Prose split (issue #1707): the PER-METRIC operator sentence lives in these
//! `interpretation` fields (they are what lands in the two committed markdown
//! artifacts); the CROSS-metric "why was this query slow?" narrative — which
//! order to read the phases in, and what to cross-check — lives in the
//! `observability` module docs, so it is not duplicated per row.

// No `attr` import: this family is deliberately attribute-FREE (every dimension
// multiplies cardinality, and none of these seven can name a bounded dimension it
// honestly knows), so every entry's `attributes` is empty.
use super::super::catalog;
use super::super::operator_docs::{MetricDoc, MetricKind};

/// The read-phase + resource-gauge annotations (catalog declaration order).
pub(super) const ANNOTATIONS: &[MetricDoc] = &[
    MetricDoc {
        name: catalog::READ_PHASE_IO,
        kind: MetricKind::Histogram,
        unit: catalog::unit::SECONDS,
        summary: "Wall time ONE completed scan spent reading Data.db bytes (positional chunk/piece reads incl. CRC verify, decompression excluded) — exactly one sample per scan, never per chunk.",
        attributes: &[],
        interpretation: "Read this FIRST when a scan is slow: io dominating the four read.phase.* series means the scan is disk/page-cache bound, so decode or merge tuning cannot help it. Small on a warm page cache. The phases run on concurrent pipeline threads, so they OVERLAP and do not sum to read.duration — compare them against EACH OTHER, not against wall time. SURFACE COVERAGE IS PARTIAL: io is recorded ONLY by the streaming scan surfaces (scan_stream / scan_stream_batched over a chunk-stitching reader) — the streaming cross-generation merge records merge and decompress but NO io, because its producer threads read through a route that has no io seam; the materializing SSTableManager::scan, the BIG reverse-clustering scan, the BTI trie walk, point reads and compaction reads emit read.duration with NO phase series at all — so an absent read.phase.* breakdown beside a rising read.duration means the phase was NOT MEASURED on that read path, never that it was fast.",
        round_item: "—",
    },
    MetricDoc {
        name: catalog::READ_PHASE_DECOMPRESS,
        kind: MetricKind::Histogram,
        unit: catalog::unit::SECONDS,
        summary: "Wall time ONE completed scan spent decompressing chunk payloads, measured in the single chunk-decode plane around the compressor call only.",
        attributes: &[],
        interpretation: "Proportional to compressed bytes read; a growing share points at chunk length / compressor choice, or at re-decompressing the same chunks (a decompressed-chunk cache too small for the scan's window). ABSENT — not zero — for an uncompressed SSTable, which decompresses nothing; a 0.0 sample would claim a measurement that never happened. SURFACE COVERAGE IS PARTIAL (cqlite.read.phase.io lists the instrumented and uninstrumented surfaces): an absent read.phase.* breakdown beside a rising read.duration means the phase was NOT MEASURED on that read path, never that it was fast.",
        round_item: "—",
    },
    MetricDoc {
        name: catalog::READ_PHASE_DECODE,
        kind: MetricKind::Histogram,
        unit: catalog::unit::SECONDS,
        summary: "Wall time ONE completed scan spent decoding rows/cells from already-resident bytes, accumulated per PARTITION at the parse boundary (never per row).",
        attributes: &[],
        interpretation: "Usually the largest phase of a WARM full scan — that is healthy, it is the CPU work of the read. Alarming when it grows relative to the rows delivered: wide partitions, many collection/UDT cells, or a schema-less fallback decode doing more work per row. SURFACE COVERAGE IS PARTIAL (cqlite.read.phase.io lists the instrumented and uninstrumented surfaces): an absent read.phase.* breakdown beside a rising read.duration means the phase was NOT MEASURED on that read path, never that it was fast.",
        round_item: "—",
    },
    MetricDoc {
        name: catalog::READ_PHASE_MERGE,
        kind: MetricKind::Histogram,
        unit: catalog::unit::SECONDS,
        summary: "Wall time ONE completed cross-generation read spent in the k-way merge/reconcile step, with the blocking merge-input recv-wait SUBTRACTED so producer starvation is not counted as merge CPU.",
        attributes: &[],
        interpretation: "Recorded ONLY on the cross-generation merge route: a single-generation scan has nothing to merge and records no sample, so absence means \"nothing to merge\", never \"merge was free\". Grows with the number of overlapping generations and with reconcile work (tombstones, LWW collapse) — a merge-dominated read delivering few rows is the compaction-lag smell, so cross-check cqlite.compaction.lag. SURFACE COVERAGE IS PARTIAL: the STREAMING cross-generation merge records this; the materializing merge_generations_for_read beneath SSTableManager::scan records nothing — so absence here is \"nothing to merge\" only when the other read.phase.* series ARE present for the same traffic, and otherwise means NOT MEASURED on that read path, never that it was fast.",
        round_item: "—",
    },
    MetricDoc {
        name: catalog::READER_FDS_OPEN,
        kind: MetricKind::Gauge,
        unit: catalog::unit::FDS,
        summary: "OS file descriptors the SSTable readers currently hold, reported by the readers themselves (an atomic incremented where a descriptor is really minted and decremented in the matching Drop) — no /proc read.",
        attributes: &[],
        interpretation: "Rises with concurrent scans and falls as they complete (there is no reader pool, by design). A level climbing toward the process ulimit -n is EMFILE pressure, visible here BEFORE an open fails. Counts DESCRIPTORS, so an mmap-backed source contributes 0 and an Arc clone of an existing handle contributes nothing. Distinct from cqlite.proc.fds, the whole-process /proc sample (sockets and the WAL included): proc.fds minus this is roughly the non-reader footprint.",
        round_item: "—",
    },
    MetricDoc {
        name: catalog::WAL_SIZE,
        kind: MetricKind::Gauge,
        unit: catalog::unit::BYTES,
        summary: "Current on-disk size of the active write-ahead log, reported by the write engine after each successful mutation (a value the engine already tracks — nothing is stat'ed).",
        attributes: &[],
        interpretation: "Healthy shape is a saw-tooth: grows with writes, drops back when a flush truncates/rotates the log. A level that only climbs means flushes are not keeping up — a disk-space problem AND a startup-latency one, because next open's recovery cost is a function of this size (cross-check cqlite.wal.recovery.duration).",
        round_item: "—",
    },
    MetricDoc {
        name: catalog::WAL_RECOVERY_DURATION,
        kind: MetricKind::Histogram,
        unit: catalog::unit::SECONDS,
        summary: "How long write-ahead-log recovery at engine open took, in seconds — both halves of it: the CRC validation scan of the log (plus any torn-tail trim) and the replay of its entries into the memtable. Normally ONE sample per process (recovery runs exactly once per open), so read it as a value rather than a distribution.",
        attributes: &[],
        interpretation: "Near-zero after a clean shutdown; seconds means a crash left a large log to validate and replay, which directly delays open — pair it with cqlite.wal.size, whose growth makes this grow. On a large or corrupt-tail WAL the validation scan is the dominant half, so the timer spans the whole open-and-recover window. A histogram rather than a gauge because the gauge plane is i64 and a sub-second recovery would truncate to a fabricated 0. Recorded at EVERY open, including when there was nothing to recover (a fresh WAL genuinely took ~0s, which is a real measurement) and including a corrupt-WAL open, so absence means no engine was opened in this process, never that recovery was skipped.",
        round_item: "—",
    },
];
