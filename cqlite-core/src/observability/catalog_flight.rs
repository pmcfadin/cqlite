//! Arrow Flight gRPC metric names — the `cqlite.rpc.*`, `cqlite.warm.cache.*` and
//! `cqlite.flight.admission.*` constants of [`super`] (issue #1041 / #2310 / #2420).
//!
//! Split out of `catalog.rs` mechanically (issue #1707, campsite rule #1116): the
//! Arrow Flight RPC surface is one responsibility and it is the only family in the
//! catalog emitted entirely from ANOTHER crate (`cqlite-flight`), so it reads as a
//! unit. Every constant is re-exported from [`super`], so the public paths
//! (`observability::catalog::RPC_DURATION`, …) are unchanged and no caller moves.
//!
//! Declaration-parsing note: the catalog guards recover `pub const IDENT: &str =
//! "…";` declarations from SOURCE, and they scan `catalog.rs` AND this file (see
//! `catalog_tests::catalog_sources`). A future split must be added there too, or the
//! guards go blind to the constants it moves.

// ---------------------------------------------------------------------------
// Arrow Flight gRPC service (issue #1041) — emitted from `cqlite-flight`.
// ---------------------------------------------------------------------------

/// `cqlite.rpc.requests` — counter `1`.
///
/// Total Arrow Flight RPC requests served, one increment per completed RPC.
/// Bounded attributes: [`attr::RPC_METHOD`] (fixed `FlightService` method set)
/// and [`attr::RPC_STATUS`] (`ok`/`error`) so a dashboard computes per-method
/// error rate from one series. NEVER carries request payloads or ticket data.
pub const RPC_REQUESTS: &str = "cqlite.rpc.requests";

/// `cqlite.rpc.duration` — histogram `s`.
///
/// Distribution of Arrow Flight RPC handler durations in seconds (handler entry
/// to response/stream construction). Bounded attributes: [`attr::RPC_METHOD`],
/// [`attr::RPC_STATUS`].
pub const RPC_DURATION: &str = "cqlite.rpc.duration";

/// `cqlite.rpc.in_flight` — gauge `1`.
///
/// Number of Arrow Flight RPCs currently being handled (incremented on entry,
/// decremented on completion). Bounded attributes: [`attr::RPC_METHOD`].
pub const RPC_IN_FLIGHT: &str = "cqlite.rpc.in_flight";

/// `cqlite.rpc.rows` — counter `{row}`.
///
/// Total rows returned to clients by `do_get` (summed across emitted record
/// batches). Emitted incrementally during a long-running scan (issue #2162): a
/// monotonic counter delta per record batch as it passes toward the client, so a
/// climbing value reads as a healthy long scan and a flat one (while
/// [`RPC_IN_FLIGHT`] > 0) as a stall. The counter total over a fully-drained
/// stream is unchanged — only the emission cadence moved from stream-end to
/// per-batch. Bounded attributes: [`attr::RPC_METHOD`].
pub const RPC_ROWS: &str = "cqlite.rpc.rows";

/// `cqlite.rpc.bytes` — counter `By`.
///
/// Total record-batch payload bytes streamed to clients by `do_get` (in-memory
/// Arrow batch size, pre-IPC-framing). Emitted incrementally during a
/// long-running scan (issue #2162): a monotonic counter delta per record batch;
/// the total over a fully-drained stream is byte-identical to the pre-#2162
/// single end-of-stream emission. Bounded attributes: [`attr::RPC_METHOD`].
pub const RPC_BYTES: &str = "cqlite.rpc.bytes";

/// `cqlite.rpc.phase.duration` — histogram `s` (issue #2162; `admission` phase
/// added #2420 roborev-1700; `validate` phase added #2420 roborev-1702).
///
/// Wall time a `do_get` spends in each of a bounded, closed set of execution
/// phases — `validate` (parsing the ticket bytes, BEFORE any admission-permit
/// acquire — a syntactically malformed ticket records ONLY this phase),
/// `admission` (queued waiting for, or immediately granted, an admission permit
/// — see #2420 — AFTER validation but BEFORE any producer/schema construction
/// or filesystem access), `resolve` (producer/schema construction + path
/// discovery/token prune), `merge_setup` (opening input SSTables + building the
/// k-way merger, the #2157 stall suspect), and `stream` (partitions stepping +
/// batches flowing to the client). Recorded once per phase transition, so a
/// `do_get` dominated by opening SSTables shows its wall time accumulating in
/// `merge_setup` BEFORE the first batch, and a `do_get` queued behind a
/// saturated admission ceiling shows it accumulating in `admission` BEFORE
/// `resolve` even starts — a stall (or queueing delay, or a flood of malformed
/// tickets) that emits zero rows still localizes to a phase. `cqlite.rpc.duration`
/// already includes admission wait time in the RPC total; this is the per-phase
/// breakdown field triage uses to localize WHERE that time went (e.g. #2398).
/// Bounded attributes: [`attr::RPC_METHOD`], [`attr::RPC_PHASE`] (the closed
/// five-value set). NEVER carries a ticket, key, token range, or query-text
/// attribute.
pub const RPC_PHASE_DURATION: &str = "cqlite.rpc.phase.duration";

/// `cqlite.rpc.phase.active` — gauge `1` (issue #2361; `admission` phase added
/// #2420 roborev-1700; `validate` phase added #2420 roborev-1702).
///
/// In-flight visibility of the phase a `do_get` is CURRENTLY executing, set to 1
/// on phase entry and back to 0 on exit (via [`super::super`]'s `PhaseTimer`
/// transition/`Drop`). [`RPC_PHASE_DURATION`] only records a sample once a phase
/// COMPLETES, so a `do_get` wedged forever in `stream` (the #2361 hang: a merge
/// that never returns a batch) recorded NOTHING — this gauge shows `stream = 1`
/// for the entire hang, so a stall is observable BEFORE completion; likewise a
/// `do_get` queued behind a saturated admission ceiling shows `admission = 1`
/// for the whole wait. Bounded attributes: [`attr::RPC_METHOD`],
/// [`attr::RPC_PHASE`] (the closed five-value set) — low cardinality (methods ×
/// 5 phases). NEVER a ticket/key/query value.
pub const RPC_PHASE_ACTIVE: &str = "cqlite.rpc.phase.active";

/// `cqlite.warm.cache.hits` — counter `{1}` (issue #2310).
///
/// Flight warm-handle cache hits: a request whose probed SSTable generation set
/// matched the cached set, served from warm parsed state with ZERO reader-open
/// and zero Index/Summary/Statistics/bloom parse. No attributes (bounded).
pub const WARM_CACHE_HITS: &str = "cqlite.warm.cache.hits";

/// `cqlite.warm.cache.misses` — counter `{1}` (issue #2310).
///
/// Flight warm-handle cache misses: no cached entry, or the generation set
/// changed so a (delta) rebuild was required. No attributes (bounded).
pub const WARM_CACHE_MISSES: &str = "cqlite.warm.cache.misses";

/// `cqlite.warm.cache.evicts` — counter `{1}` (issue #2310).
///
/// Warm generations evicted, whether by LRU (byte-budget pressure) or because a
/// rebuild found them removed on disk. No attributes (bounded).
pub const WARM_CACHE_EVICTS: &str = "cqlite.warm.cache.evicts";

/// `cqlite.warm.cache.refresh` — counter `{1}` (issue #2310).
///
/// Warm-handle refresh outcomes, tagged by [`attr::WARM_REFRESH_OUTCOME`]
/// (`unchanged` / `rebuilt_delta` / `fail_closed_retained`) — the single bounded
/// dimension. Distinguishes a warm hit from a delta rebuild from a fail-closed
/// retention in metrics alone (spec Requirement 6).
pub const WARM_CACHE_REFRESH: &str = "cqlite.warm.cache.refresh";

/// `cqlite.flight.admission.limit` — gauge `1` (issue #2420, WS4).
///
/// The configured `do_get` admission ceiling `K` (the `--max-concurrent-scans`
/// value). A constant level while the server runs; recorded on startup so a
/// dashboard can chart `in_use` against the limit. No attributes (bounded).
/// DISTINCT from [`RPC_IN_FLIGHT`]: this is the CONFIGURED ceiling, not a live
/// count.
pub const FLIGHT_ADMISSION_LIMIT: &str = "cqlite.flight.admission.limit";

/// `cqlite.flight.admission.in_use` — gauge `1` (issue #2420, WS4).
///
/// `do_get` admission permits currently held — the number of scans ADMITTED and
/// in-flight (an up/down level like [`RPC_IN_FLIGHT`], but counting only admitted
/// scans, not every accepted RPC incl. the ones parked waiting for a permit).
/// Returns to zero when every admitted scan completes/cancels/disconnects (the
/// RAII permit release). No attributes (bounded). DISTINCT from [`RPC_IN_FLIGHT`].
pub const FLIGHT_ADMISSION_IN_USE: &str = "cqlite.flight.admission.in_use";

/// `cqlite.flight.admission.waiting` — gauge `1` (issue #2420, WS4).
///
/// `do_get` requests currently parked on `acquire`, waiting for an admission
/// permit to free within the permit-wait timeout. A non-zero value is the
/// backpressure signal: offered concurrency has exceeded the ceiling and requests
/// are queuing rather than degrading together. No attributes (bounded).
pub const FLIGHT_ADMISSION_WAITING: &str = "cqlite.flight.admission.waiting";

/// `cqlite.flight.admission.rejected_total` — counter `{1}` (issue #2420, WS4).
///
/// `do_get` requests rejected because no admission permit freed within the
/// permit-wait timeout — each returned to the client as gRPC `UNAVAILABLE` (so the
/// connector's #2241 replica-failover treats it as retry-safe), before any record
/// batch was delivered. A monotonic total; scale-free. No attributes (bounded).
pub const FLIGHT_ADMISSION_REJECTED_TOTAL: &str = "cqlite.flight.admission.rejected_total";

/// `cqlite.flight.admission.wait_seconds` — histogram `s` (issue #2420, WS4).
///
/// Distribution of how long a `do_get` waited on `acquire` before it was admitted
/// (a permit freed) OR rejected (the wait timeout elapsed). Localizes admission
/// pressure: a rising tail means requests are increasingly queuing for permits.
/// No attributes (bounded).
pub const FLIGHT_ADMISSION_WAIT_SECONDS: &str = "cqlite.flight.admission.wait_seconds";
