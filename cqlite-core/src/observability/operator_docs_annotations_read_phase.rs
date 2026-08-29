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
        interpretation: "Read this FIRST when a scan is slow: io dominating the four read.phase.* series means the scan is disk/page-cache bound, so decode or merge tuning cannot help it. Small on a warm page cache. The phases run on concurrent pipeline threads, so they OVERLAP and do not sum to read.duration — compare them against EACH OTHER, not against wall time. Recorded on the windowed scan driver; a read that never enters it records nothing rather than a fabricated 0.",
        round_item: "—",
    },
    MetricDoc {
        name: catalog::READ_PHASE_DECOMPRESS,
        kind: MetricKind::Histogram,
        unit: catalog::unit::SECONDS,
        summary: "Wall time ONE completed scan spent decompressing chunk payloads, measured in the single chunk-decode plane around the compressor call only.",
        attributes: &[],
        interpretation: "Proportional to compressed bytes read; a growing share points at chunk length / compressor choice, or at re-decompressing the same chunks (a decompressed-chunk cache too small for the scan's window). ABSENT — not zero — for an uncompressed SSTable, which decompresses nothing; a 0.0 sample would claim a measurement that never happened.",
        round_item: "—",
    },
    MetricDoc {
        name: catalog::READ_PHASE_DECODE,
        kind: MetricKind::Histogram,
        unit: catalog::unit::SECONDS,
        summary: "Wall time ONE completed scan spent decoding rows/cells from already-resident bytes, accumulated per PARTITION at the parse boundary (never per row).",
        attributes: &[],
        interpretation: "Usually the largest phase of a WARM full scan — that is healthy, it is the CPU work of the read. Alarming when it grows relative to the rows delivered: wide partitions, many collection/UDT cells, or a schema-less fallback decode doing more work per row.",
        round_item: "—",
    },
    MetricDoc {
        name: catalog::READ_PHASE_MERGE,
        kind: MetricKind::Histogram,
        unit: catalog::unit::SECONDS,
        summary: "Wall time ONE completed cross-generation read spent in the k-way merge/reconcile step, with the blocking merge-input recv-wait SUBTRACTED so producer starvation is not counted as merge CPU.",
        attributes: &[],
        interpretation: "Recorded ONLY on the cross-generation merge route: a single-generation scan has nothing to merge and records no sample, so absence means \"nothing to merge\", never \"merge was free\". Grows with the number of overlapping generations and with reconcile work (tombstones, LWW collapse) — a merge-dominated read delivering few rows is the compaction-lag smell, so cross-check cqlite.compaction.lag.",
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
        interpretation: "Healthy shape is a saw-tooth: grows with writes, drops back when a flush truncates/rotates the log. A level that only climbs means flushes are not keeping up — a disk-space problem AND a startup-latency one, because next open's replay cost is a function of this size (cross-check cqlite.wal.replay.duration).",
        round_item: "—",
    },
    MetricDoc {
        name: catalog::WAL_REPLAY_DURATION,
        kind: MetricKind::Gauge,
        unit: catalog::unit::SECONDS,
        summary: "How long the LAST write-ahead-log replay took, in seconds. A gauge, not a histogram: replay happens exactly once per engine open, so a histogram would hold one sample per process.",
        attributes: &[],
        interpretation: "Near-zero after a clean shutdown; seconds means a crash left a large log to replay, which directly delays open. Emitted even when there was nothing to replay — a fresh WAL genuinely took ~0s, which is a real measurement, so absence of this series means no engine was opened in this process, never that replay was skipped.",
        round_item: "—",
    },
];
