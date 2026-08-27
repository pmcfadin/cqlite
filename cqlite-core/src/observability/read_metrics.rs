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
//! # Zero-cost when off
//!
//! [`ReadOpMeter::start`] consults [`obs::metrics_active`] ONCE (the issue #2819 M1
//! pattern) and returns an INERT meter when metrics are not being collected: no
//! [`Instant`] is sampled, no key is retained, and every later `record_*` call is a
//! single branch on a `None`. With the `observability` feature off,
//! `metrics_active()` is a compile-time `false`, so the whole meter degenerates to
//! that branch and the emission helpers compile to no-ops.

use std::sync::Arc;
use std::time::Instant;

use super::{catalog, AttrValue};
use crate::observability as obs;
use crate::storage::sstable::compression::CompressionAlgorithm;
use crate::types::RowKey;

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
        CompressionAlgorithm::None => "none",
    }
}

/// Count `bytes` of DECOMPRESSED `Data.db` payload into [`catalog::READ_BYTES`].
///
/// Called once per chunk the read path materialises, from the single chunk decode
/// plane (`reader::chunk_source`). A chunk served from the resident decompressed
/// chunk cache reads no `Data.db` bytes and is deliberately NOT counted — the
/// metric is "bytes read from Data.db (post-decompression)", so counting a cache
/// hit would overstate the I/O the read performed.
pub(crate) fn record_decompressed_bytes(bytes: usize, compression: Option<&'static str>) {
    if bytes == 0 {
        return;
    }
    match compression {
        Some(algorithm) => obs::add_counter(
            catalog::READ_BYTES,
            bytes as u64,
            &[(catalog::attr::COMPRESSION, AttrValue::StaticStr(algorithm))],
        ),
        None => obs::add_counter(catalog::READ_BYTES, bytes as u64, &[]),
    }
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
    /// partition in on-disk order and every emitted row carries its PARTITION key
    /// (the row key comes from the partition header — `row_framing.rs`), so a
    /// change of key is a partition boundary. Retained as an `Arc` clone (a
    /// refcount bump, no key copy).
    last_partition: Option<Arc<[u8]>>,
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

    /// Account one delivered row, and a partition boundary when its key differs
    /// from the previous row's.
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
    }
}

impl Drop for ReadOpMeter {
    fn drop(&mut self) {
        self.finish();
    }
}
