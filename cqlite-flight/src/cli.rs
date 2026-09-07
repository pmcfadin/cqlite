//! The `cqlite-flight` server binary's command-line surface.
//!
//! **Why this lives in the LIBRARY rather than in `main.rs`.** Issue #3225's AC4
//! requires the `--max-concurrent-scans` precedence chain (flag → env → derived)
//! and its four provenance labels to be asserted **through the real clap
//! parser** — a hand-built config would prove nothing about what an operator's
//! command line actually does. A binary target's `Args` is unreachable from
//! `tests/`, so the argument definitions, the parse entry point, the provenance
//! resolution and the startup log event all live here, where an integration
//! test can drive them. `main.rs` is the thin wiring that calls them.
//!
//! The `--max-concurrent-scans` argument deliberately carries **no
//! `default_value_t`** (it did until #3225): with a default installed, clap
//! cannot distinguish "the operator typed 64" from "nobody typed anything", and
//! that distinction *is* the provenance AC4 asks for. The argument is therefore
//! an `Option<usize>` and [`resolve_max_concurrent_scans`] supplies the default,
//! reading [`clap::parser::ValueSource`] to label where the value came from.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::parser::ValueSource;
use clap::{ArgMatches, CommandFactory, FromArgMatches, Parser};

use crate::admission::{
    self, ExplicitScansOrigin, ResolvedMaxConcurrentScans, DEFAULT_WAIT_TIMEOUT_MS,
    ENV_MAX_CONCURRENT_SCANS, ENV_WAIT_TIMEOUT_MS,
};
use crate::batch_bytes::{DEFAULT_MAX_BATCH_BYTES, ENV_MAX_BATCH_BYTES};
use crate::egress_credit::{DEFAULT_MAX_INFLIGHT_EGRESS_BYTES, ENV_MAX_INFLIGHT_EGRESS_BYTES};

/// The clap argument id of `--max-concurrent-scans`.
///
/// Named as a constant because provenance is resolved by asking
/// [`ArgMatches::value_source`] for exactly this id: a field rename would
/// otherwise silently degrade every explicit value to `derived`. Asserted
/// against the real [`clap::Command`] in the #3225 provenance tests.
pub const ARG_MAX_CONCURRENT_SCANS: &str = "max_concurrent_scans";

/// Command-line arguments.
#[derive(Parser, Debug)]
#[command(
    name = "cqlite-flight",
    about = "Arrow Flight server for compacted CQLite SSTables",
    // `version` here only makes the FLAG exist; the text it prints is replaced
    // per-invocation by [`command_with_allocator`], because the linked allocator
    // is known to the BINARY and not to this library (issue #3997, R2.1).
    version
)]
pub struct Args {
    /// Root directory holding `<keyspace>/<table>[-<uuid>]/` SSTable dirs.
    #[arg(long)]
    pub data_dir: PathBuf,

    /// Address to listen on.
    #[arg(long, default_value = "0.0.0.0:8815")]
    pub listen: SocketAddr,

    /// Maximum rows per Arrow record batch.
    #[arg(long, default_value_t = 8192)]
    pub batch_size: usize,

    /// Maximum concurrently admitted `do_get` scans (issue #2420, WS4). A `do_get`
    /// acquires a permit before opening any SSTable; past this ceiling requests
    /// wait up to `--admission-wait-timeout-ms`, then are shed with gRPC
    /// `UNAVAILABLE` (retry-safe for the connector's replica failover).
    ///
    /// Unset, the ceiling is DERIVED from the parallelism available to this
    /// process (issue #3225): `clamp(2 x hardware threads, 2, 64)`, honouring
    /// the CPU affinity mask and the cgroup CPU quota, so a narrow server or a
    /// small container is not admitted past its measured peak-throughput
    /// concurrency. The 64 ceiling is the pre-#3225 constant, kept as the cap;
    /// pass `--max-concurrent-scans 64` to restore that behaviour exactly. An
    /// explicit value here always wins and is never clamped toward the derived
    /// one. The startup log reports the effective value and its provenance.
    #[arg(long, env = ENV_MAX_CONCURRENT_SCANS)]
    pub max_concurrent_scans: Option<usize>,

    /// How long a saturated `do_get` waits for an admission permit before it is
    /// rejected with `UNAVAILABLE` (issue #2420, WS4). Short bursts under this
    /// budget are absorbed transparently with no client-visible error.
    #[arg(long, env = ENV_WAIT_TIMEOUT_MS, default_value_t = DEFAULT_WAIT_TIMEOUT_MS)]
    pub admission_wait_timeout_ms: u64,

    /// Maximum Arrow PAYLOAD bytes per record batch (issue #2825, T4). A batch is
    /// finished on whichever of `--batch-size` or this cap trips FIRST, so a wide
    /// (blob/text) schema can no longer produce an unbounded
    /// `batch_size x row_width` batch. Denominated in payload bytes (the sum of
    /// Arrow buffer lengths), NOT `get_array_memory_size()`, which reports buffer
    /// capacity and runs up to `BATCH_BYTES_CAPACITY_FACTOR` (2x) higher; a
    /// consumer budgeting resident memory uses
    /// `cqlite_flight::batch_bytes::worst_case_batch_capacity_bytes`. The 4 MiB
    /// default leaves the row-cap binding on every narrow shape, so narrow-path
    /// batch boundaries are unchanged. A single row wider than the cap is still
    /// delivered, as a one-row batch; `0` and `1` degrade to one row per batch.
    #[arg(long, env = ENV_MAX_BATCH_BYTES, default_value_t = DEFAULT_MAX_BATCH_BYTES)]
    pub max_batch_bytes: usize,

    /// Maximum Arrow CAPACITY bytes in flight on ONE streaming `do_get` (issue
    /// #2821). Credit for a batch is reserved BEFORE the batch is materialized
    /// and released when it has left the stream, so per-stream egress residency
    /// is bounded in BYTES rather than by a batch count multiplied by an
    /// unbounded row width. Denominated in `RecordBatch::get_array_memory_size()`
    /// (buffer CAPACITY) — a DIFFERENT currency from `--max-batch-bytes`, which
    /// is Arrow PAYLOAD bytes; convert between them only with
    /// `cqlite_flight::batch_bytes::worst_case_batch_capacity_bytes`. A single
    /// batch may exceed the whole ceiling and is still delivered (it takes the
    /// whole pool), so the guaranteed bound is `max(ceiling, one maximum batch)`
    /// = max(12 MiB, ~8.4 MiB) = 12 MiB PER STREAM at the shipped defaults —
    /// size a deployment against that, not against the 8 MiB one-batch figure.
    /// The bound is over SERVER-SIDE residency: bytes this process holds on the
    /// egress path. It covers GOVERNED EGRESS CAPACITY only, so it is a floor
    /// for sizing rather than a per-stream total: the row buffer and the
    /// encoder's queued `FlightData` (~4 MiB at defaults) are additional
    /// server-side memory on the same stream and are not counted here. Batches a client retains after receiving them are the
    /// client's memory and are deliberately not charged here. `0` degrades to
    /// strict one-batch-at-a-time egress, never a hang.
    #[arg(
        long,
        env = ENV_MAX_INFLIGHT_EGRESS_BYTES,
        default_value_t = DEFAULT_MAX_INFLIGHT_EGRESS_BYTES
    )]
    pub max_inflight_egress_bytes: usize,
}

/// The clap [`Command`](clap::Command) with `--version`'s long form carrying the
/// allocator this process linked (issue #3997, requirement R2.1).
///
/// R2.1's contract is exact: `cqlite-flight --version` stdout contains **exactly
/// one** line matching `^allocator: (jemalloc|system)$`. clap prints the version
/// text verbatim after the `<name> ` prefix on the first line, so the allocator
/// goes on its OWN line — the second — which satisfies the anchored grammar;
/// embedding it in the first line would not.
///
/// BOTH `version` (`-V`) and `long_version` (`--version`) are set to the same
/// text, deliberately. clap's default is for `-V` to print the short form and
/// `--version` the long one, which would make the allocator observable through
/// one flag and not the other — a distinction an operator has no reason to
/// expect from a two-line version string. R2.1 names `--version`; this makes
/// `-V` answer identically rather than merely satisfying the letter.
///
/// `allocator` is a parameter rather than a constant read here because a linked
/// `#[global_allocator]` is a property of the BIN target. This library is also
/// compiled into every integration-test binary, where the `jemalloc` feature can
/// be on while no allocator is installed at all, so a value derived here would
/// be able to disagree with what the running process actually uses. The single
/// source of truth is `main.rs`'s `ALLOCATOR`, adjacent to the install site.
pub fn command_with_allocator(allocator: &str) -> clap::Command {
    let version = format!("{}\nallocator: {allocator}", env!("CARGO_PKG_VERSION"));
    Args::command()
        .version(version.clone())
        .long_version(version)
}

impl Args {
    /// Parse the process command line, returning the typed arguments **and** the
    /// [`ArgMatches`] they came from.
    ///
    /// The matches are returned because provenance (issue #3225, AC4) is a
    /// property of the parse, not of the parsed value: only
    /// [`ArgMatches::value_source`] can say whether a value arrived from the
    /// command line, the environment, or nowhere. Exits with clap's usage
    /// message on a parse error, exactly as [`Parser::parse`] does.
    ///
    /// `allocator` is the caller's linked-allocator name; see
    /// [`command_with_allocator`] for why it is passed in. `--version` and
    /// `--help` short-circuit inside clap BEFORE required-argument validation,
    /// so `cqlite-flight --version` works with no `--data-dir`.
    pub fn parse_with_matches(allocator: &str) -> (Self, ArgMatches) {
        let matches = command_with_allocator(allocator).get_matches();
        match Self::from_arg_matches(&matches) {
            Ok(args) => (args, matches),
            Err(e) => e.exit(),
        }
    }

    /// [`Args::parse_with_matches`] over an explicit argv, returning the error
    /// instead of exiting — the entry point the #3225 precedence tests drive, so
    /// they exercise the REAL parser (including its `env =` attributes) rather
    /// than a hand-built configuration.
    ///
    /// Takes the same `allocator` string, so a test can drive the exact
    /// `Command` the binary builds rather than a differently-configured one.
    pub fn try_parse_with_matches_from<I, T>(
        allocator: &str,
        argv: I,
    ) -> Result<(Self, ArgMatches), clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let matches = command_with_allocator(allocator).try_get_matches_from(argv)?;
        let args = Self::from_arg_matches(&matches)?;
        Ok((args, matches))
    }
}

/// Resolve the effective `--max-concurrent-scans` ceiling and its provenance
/// from a real parse (issue #3225, AC4).
///
/// Precedence is clap's own: its `env =` attribute already prefers a
/// command-line value over the environment, so this only has to LABEL which one
/// clap used, and supply the parallelism-derived default when clap supplied
/// nothing at all.
pub fn resolve_max_concurrent_scans(
    args: &Args,
    matches: &ArgMatches,
) -> ResolvedMaxConcurrentScans {
    admission::resolve_max_concurrent_scans(
        explicit_max_concurrent_scans(args, matches),
        admission::probe_available_parallelism(),
    )
}

/// The explicitly configured ceiling and where the operator supplied it from, or
/// `None` when nobody configured one.
fn explicit_max_concurrent_scans(
    args: &Args,
    matches: &ArgMatches,
) -> Option<(usize, ExplicitScansOrigin)> {
    let value = args.max_concurrent_scans?;
    // A value REACHED us, so an operator supplied it: the only question left is
    // by which route, and the answer must never be "derived".
    match matches.value_source(ARG_MAX_CONCURRENT_SCANS) {
        Some(ValueSource::CommandLine) => Some((value, ExplicitScansOrigin::CommandLine)),
        Some(ValueSource::EnvVariable) => Some((value, ExplicitScansOrigin::Environment)),
        // The argument carries NO `default_value`, so clap cannot report
        // `DefaultValue` here, and a `None` source cannot accompany a present
        // value. `ValueSource` is `#[non_exhaustive]`, so this arm exists only
        // for a clap variant that does not yet exist: keep the operator's value
        // and label it as the non-command-line supply route, which is the only
        // other way clap can produce one for this argument.
        _ => Some((value, ExplicitScansOrigin::Environment)),
    }
}

/// Emit the single `cqlite-flight starting` startup event (issue #1041), now
/// carrying the admission ceiling's provenance (issue #3225, AC4).
///
/// One event, not two: `max_concurrent_scans_source` and
/// `available_parallelism` join the fields already there, so a log capture
/// answers "why is this server admitting N?" on its own.
///
/// * `admission_limit` is [`crate::admission::Admission::limit`] — the
///   POST-clamp value the semaphore was actually built with, which is what an
///   operator needs to see when #2420's `[1, Semaphore::MAX_PERMITS]` clamp
///   adjusted the requested one.
/// * `available_parallelism` is OMITTED (not logged as a placeholder) when the
///   oracle returned no answer; `max_concurrent_scans_source` is then
///   `derived-fallback`, so the absence is never ambiguous.
/// * `allocator` is the linked global allocator (issue #3997, requirement
///   R2.2) — the SAME string `--version` prints, so a log capture and an
///   out-of-band `--version` can never disagree. Passed in by `main.rs` for the
///   reason documented on [`command_with_allocator`].
pub fn log_startup(
    args: &Args,
    scans: &ResolvedMaxConcurrentScans,
    admission_limit: usize,
    max_concurrent_streams: u32,
    allocator: &str,
) {
    let listen = args.listen;
    tracing::info!(
        %listen,
        batch_size = args.batch_size,
        max_batch_bytes = args.max_batch_bytes,
        max_inflight_egress_bytes = args.max_inflight_egress_bytes,
        max_concurrent_scans = admission_limit,
        max_concurrent_scans_source = scans.source.as_str(),
        available_parallelism = scans.available_parallelism,
        admission_wait_timeout_ms = args.admission_wait_timeout_ms,
        max_concurrent_streams,
        allocator,
        "cqlite-flight starting"
    );
}

/// The POST-BIND readiness line (issue #3384).
///
/// Distinct from [`log_startup`], which records CONFIGURATION and is necessarily
/// written before the port is acquired. This one is emitted only once a listener
/// exists, so its presence is proof the process owns `bound` — the property a
/// readiness probe needs and that a configuration line cannot supply, since a
/// process can log its configuration and then die of `EADDRINUSE`.
///
/// `bound` is the ADDRESS ACTUALLY BOUND, not the one requested: with
/// `--listen 127.0.0.1:0` the requested port is 0 and only this line names the
/// port a client can dial.
pub fn log_listening(bound: std::net::SocketAddr) {
    tracing::info!(listening_on = %bound, "cqlite-flight listening on {bound}");
}
