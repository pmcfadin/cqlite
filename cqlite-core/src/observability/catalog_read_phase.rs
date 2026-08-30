//! Read-path PHASE timing metric names + the reader-reported fd gauge (issue
//! #1707, AI7 of epic #1686) — declared in a sibling file so `catalog.rs` stays
//! inside the campsite-rule source target (#1116). Every constant is re-exported
//! from [`super`], so the public paths (`observability::catalog::READ_PHASE_IO`, …)
//! are unchanged and no caller moves.
//!
//! Declaration-parsing note: the catalog guards recover `pub const IDENT: &str =
//! "…";` declarations from SOURCE and scan this file too (see
//! `catalog_tests::catalog_sources`). A future split must be added there as well, or
//! the guards go blind to the constants it moves.

// ---------------------------------------------------------------------------
// Read-path PHASE timings + reader-reported resource gauges (issue #1707, AI7 of
// epic #1686). A slow `SELECT` used to be un-localisable from metrics alone:
// `READ_DURATION` says the read was slow and nothing says WHERE the time went.
// These four histograms split one completed scan's accumulated wall time into
// io / decompress / decode / merge — accumulated at existing function seams into a
// per-scan `ReadPhaseTimings`, and emitted as exactly ONE sample per phase per
// COMPLETED scan (never per chunk, never per row).
//
// NAMING (#1707): the issue text spelled these `read.phase.io_ms` etc. The `_ms`
// suffix was deliberately NOT adopted — every timing metric in this catalog is
// base-unit SECONDS ([`super::READ_DURATION`], [`super::WAL_SYNC_DURATION`], the `cqlite.rpc.*`
// durations), which is what OTel's semantic conventions ask for, so an `_ms`-named
// metric carrying [`super::unit::SECONDS`] would misdescribe its own values and an `ms`
// unit would break the repo-wide (and OTel) base-unit convention.
//
// ACCOUNTING CAVEAT, stated here because it changes how the numbers are read: the
// read pipeline is CONCURRENT (an IO/decode feed thread, a blocking parse thread, a
// merge consumer thread), so these phases OVERLAP in wall-clock and DO NOT sum to
// the scan's [`super::READ_DURATION`]. They are per-phase totals for attribution — which
// phase dominates, and how it moves between two runs — not a decomposition of
// latency. Same caveat as the #2819 `stream_*` sub-phases.
// ---------------------------------------------------------------------------

/// `cqlite.read.phase.io` — histogram `s` (issue #1707).
///
/// Wall time ONE completed scan spent in `Data.db` READS: the synchronous
/// positional chunk/piece reads the windowed scan's IO half performs
/// (`read_compressed_chunk_sync` / `read_uncompressed_piece_sync`), CRC verify
/// included, decompression excluded (that is [`READ_PHASE_DECOMPRESS`]).
///
/// **Healthy vs alarming**: on a warm page cache this is a small fraction of the
/// scan; io dominating (say > half the recorded phase time) means the scan is
/// disk/page-fault bound — cold storage, an evicted page cache, or a network
/// filesystem — not CPU bound, so a decode/merge optimisation will not help it.
/// Compare it against [`READ_PHASE_DECODE`] before tuning anything.
///
/// **Coverage**: the windowed scan driver (the dominant `SELECT *` path). A read
/// that never enters that driver (a point read, the BTI trie walk) records NO
/// sample at all — absence means the phase DID NOT RUN. A `0.0` sample is a
/// different statement: the phase ran and measured zero. **Attributes**: none.
pub const READ_PHASE_IO: &str = "cqlite.read.phase.io";

/// `cqlite.read.phase.decompress` — histogram `s` (issue #1707).
///
/// Wall time ONE completed scan spent DECOMPRESSING chunk payloads, measured
/// inside the single chunk-decode plane (`reader::chunk_source`) around the
/// compressor call ONLY — the read that fetched the compressed bytes is
/// [`READ_PHASE_IO`], and a resident decompressed-chunk cache HIT does no
/// decompression and so adds nothing.
///
/// **Healthy vs alarming**: proportional to compressed bytes; a growing share
/// points at chunk-length/compressor choice or at re-decompressing the same chunks
/// (a cache too small for the scan's window).
///
/// **ABSENT, never zero, for an UNCOMPRESSED SSTable** — CQLite's own write surface
/// emits uncompressed SSTables (#1406) and those reads decompress NOTHING, so this
/// series carries no sample at all for them. Absence and `0.0` are DIFFERENT
/// statements and the emitter distinguishes them by tracking phase ENTRY separately
/// from duration (#1707): absence means the phase did not run, while `0.0` means it
/// ran and measured zero — decompression too fast for the clock, not the absence of
/// decompression. **Attributes**: none.
pub const READ_PHASE_DECOMPRESS: &str = "cqlite.read.phase.decompress";

/// `cqlite.read.phase.decode` — histogram `s` (issue #1707).
///
/// Wall time ONE completed scan spent DECODING rows/cells out of already-resident
/// decompressed bytes — accumulated per PARTITION at the windowed scan's parse
/// boundary (`parse_one_partition_with_timestamps`), which is the coarsest boundary
/// that still separates decode from io. Deliberately NOT per row: the row/cell
/// decoder is the hottest loop in the read path and is never instrumented.
///
/// **Healthy vs alarming**: normally the largest phase of a warm full scan (decode
/// is CPU-bound work over resident bytes). Alarming when it grows relative to the
/// rows delivered — wide partitions, many collection/UDT cells, or a schema-less
/// fallback decode doing more work per row. **Attributes**: none.
pub const READ_PHASE_DECODE: &str = "cqlite.read.phase.decode";

/// `cqlite.read.phase.merge` — histogram `s` (issue #1707).
///
/// Wall time ONE completed cross-generation read spent in the k-way MERGE +
/// reconcile step (`KWayMerger::step`), with the blocking merge-input recv-wait
/// SUBTRACTED — that wait is producer starvation (i.e. io on another thread), not
/// merge CPU, and counting it would make every disk-bound scan look merge-bound.
/// Same exclusion the #2819 `stream_merge` sub-phase applies, through the same
/// per-thread recv-wait accumulator.
///
/// **Coverage, stated because it is narrower than its siblings**: this phase is
/// recorded only on the CROSS-GENERATION merge route (`stream_generations_for_read`).
/// A single-generation scan performs no k-way merge, so it records NO sample —
/// absence here means "there was nothing to merge", not "merge was free".
///
/// **A `0.0` sample means a merge DID run and measured zero**, which on this series
/// has a specific and useful reading: the recv-wait subtraction consumed the whole
/// step, i.e. the merge thread spent all its time waiting on starved producers. That
/// case used to be reported as ABSENCE — "single generation" — which was a false
/// statement produced by the subtraction that exists to keep the number honest
/// (#1707). Entry is now tracked separately from duration, so the zero is emitted.
///
/// **Healthy vs alarming**: grows with the number of overlapping generations and
/// with reconcile work (tombstones, LWW collapse); a merge-dominated read with few
/// delivered rows is the compaction-lag smell — cross-check
/// [`super::COMPACTION_LAG`]. **Attributes**: none.
pub const READ_PHASE_MERGE: &str = "cqlite.read.phase.merge";

/// `cqlite.reader.fds.open` — gauge `{fd}` (issue #1707).
///
/// OS file descriptors the SSTable READERS currently hold open, reported BY the
/// readers themselves: a process-wide atomic incremented at each site that really
/// mints a descriptor (a scan source's buffered/`O_DIRECT` handle, a positional
/// reader's handle, a reader cold-open) and decremented in the matching `Drop`.
///
/// **It counts descriptors, so an mmap-backed source contributes 0** — `Mapped`
/// sources hold a mapping, not an fd, and an `Arc` clone of an existing handle is
/// not a new descriptor. Neither is invented as a number here.
///
/// **Healthy vs alarming**: rises with concurrent scans and falls as they complete
/// (there is no reader pool, by #815 design: each scan opens its own handles);
/// a level that climbs toward the process `ulimit -n` is the `EMFILE` pressure this
/// gauge exists to make visible BEFORE an open fails.
///
/// DISTINCT from [`super::PROC_FDS`], and the difference is the point: `PROC_FDS` is the
/// PROCESS total sampled from `/proc/self/fd` (~2s cadence, Linux only), so it
/// includes sockets, the WAL, and everything else. This gauge is the READER-OWNED
/// subset, needs no `/proc`, and is exact at its update points. `PROC_FDS` minus
/// this is roughly the non-reader footprint. **Attributes**: none.
pub const READER_FDS_OPEN: &str = "cqlite.reader.fds.open";
