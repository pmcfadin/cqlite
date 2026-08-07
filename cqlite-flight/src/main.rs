//! `cqlite-flight` — Arrow Flight server exposing on-the-fly compacted SSTable
//! data. Runs co-located with a Cassandra node and reads its local SSTables.

use arrow_flight::flight_service_server::FlightServiceServer;
use tonic::transport::Server;

use cqlite_flight::admission::{Admission, AdmissionConfig, WaitBudget};
use cqlite_flight::cli::{self, Args};
use cqlite_flight::egress_credit::EgressBudget;
use cqlite_flight::service::CqliteFlightService;
use cqlite_flight::shutdown::shutdown_signal;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `ObservabilityConfig` is always compiled (it carries no OTel types), so
    // reading it here works identically whether or not the `observability`
    // feature is on. Snapshot the bits `log_observability_status` needs before
    // moving `obs_cfg` into `init` below.
    let obs_cfg = cqlite_core::observability::ObservabilityConfig::from_env();
    let obs_enabled = obs_cfg.enabled;
    let obs_endpoint = obs_cfg.endpoint.clone();

    // Initialise observability (issue #1041, epic #1031) BEFORE composing the
    // tracing subscriber, so `observability::tracing_layer()` returns the live
    // OTel export layer. The guard flushes/shuts down OTel on drop; hold it for
    // the whole process lifetime. With the `observability` feature off (or
    // CQLITE_OTEL_ENABLED unset) this is an inert no-op.
    let _otel_guard = cqlite_core::observability::init(obs_cfg)?;
    // Install the global W3C text-map propagator so incoming gRPC `traceparent`
    // metadata can be extracted into an OTel context and used to parent the
    // per-RPC spans (continuing a client's distributed trace into the service).
    #[cfg(feature = "observability")]
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    init_tracing_subscriber();

    // Issue #2128: the published image was previously built without
    // `--features observability`, so `CQLITE_OTEL_ENABLED=true` was silently
    // inert — no metrics, no traces, no error, nothing in the startup log to
    // say why. Make the inert-vs-active state visible instead.
    log_observability_status(obs_enabled, &obs_endpoint);

    // Parse WITH the `ArgMatches` (issue #3225): the admission ceiling's
    // provenance — flag vs env vs derived — is a property of the parse, and
    // only `ArgMatches::value_source` can report it.
    let (args, matches) = Args::parse_with_matches();
    let listen = args.listen;
    // The effective ceiling and where it came from. With nothing configured this
    // is `clamp(2 x available_parallelism, 2, 64)` (issue #3225) rather than the
    // flat 64 constant, so a narrow server or a CPU-quota-limited container is
    // not admitted past its measured peak-throughput concurrency.
    let scans = cli::resolve_max_concurrent_scans(&args, &matches);
    // Admission control (issue #2420, WS4): the owned Semaphore is the real,
    // observable, cancel-releasable ceiling.
    let admission = Admission::new(AdmissionConfig {
        max_concurrent_scans: scans.value,
        wait_budget: WaitBudget::Timeout(Duration::from_millis(args.admission_wait_timeout_ms)),
    });
    let admission_limit = admission.limit();
    // Keep a copy of the data-dir for the saturation sampler's readdir-only
    // table-discovery walk (issue #2684) before the service takes ownership.
    let sampler_data_dir = args.data_dir.clone();
    let service =
        CqliteFlightService::with_admission(args.data_dir.clone(), args.batch_size, admission)
            // Issue #2825: the byte-cap half of the dual batch boundary.
            .with_max_batch_bytes(args.max_batch_bytes)
            // Issue #2821: the per-stream in-flight egress capacity-byte ceiling.
            .with_egress_budget(EgressBudget::bytes(args.max_inflight_egress_bytes));

    // A coarse tonic transport backstop, generously ABOVE the admission ceiling:
    // it guards the HTTP/2 accept loop / stream table from a client opening far
    // more streams than `K`, but the Semaphore — not this cap — is the real
    // admission throttle (issue #2420). `saturating_mul` avoids overflow on an
    // extreme configured `K`.
    let max_concurrent_streams: u32 = u32::try_from(admission_limit)
        .unwrap_or(u32::MAX)
        .saturating_mul(4)
        .max(1024);

    cli::log_startup(&args, &scans, admission_limit, max_concurrent_streams);
    // Saturation instrumentation (issue #2419, WS2): spawn the background sampler
    // that drives the `cqlite.proc.*` OS-resource gauges (thread/fd/RSS) on a ~2s
    // cadence. It takes its OWN `shutdown_signal()` future — the same SIGTERM /
    // ctrl_c source the tonic server drains on — so it terminates on shutdown
    // without perturbing the server's shutdown wiring (no leaked task). The
    // atomic-backed gauges (`egress_channel_depth`, `blocking_tasks_in_use`)
    // update at their own call sites, independent of this cadence.
    // The sampler also drives `cqlite.flight.tables_discovered` (issue #2684) via
    // a readdir-only walk of `sampler_data_dir` each tick, and emits the one-time
    // `discovered N tables across M keyspaces under <data-dir>` startup log line
    // after its first sample.
    let _sampler = tokio::spawn(cqlite_flight::saturation::run_sampler(
        cqlite_flight::saturation::DEFAULT_SAMPLE_INTERVAL,
        sampler_data_dir,
        shutdown_signal(),
    ));
    // Graceful shutdown (issue #1473): on ctrl_c / SIGTERM, tonic stops
    // accepting new connections and drains in-flight RPCs rather than tearing
    // every open stream down abruptly.
    Server::builder()
        .max_concurrent_streams(max_concurrent_streams)
        .add_service(FlightServiceServer::new(service))
        .serve_with_shutdown(listen, shutdown_signal())
        .await?;
    Ok(())
}

/// Install the unified `tracing_subscriber` registry (issue #1041, epic #1031).
///
/// Mirrors the CLI (#1033): an `EnvFilter` (honouring `RUST_LOG`, default
/// `info`) plus a human-readable `fmt` layer, and — when the `observability`
/// feature is enabled — the OTel bridge layer from
/// `cqlite_core::observability::tracing_layer()`. That layer is `None` (a no-op)
/// unless `init()` installed an exporting provider, so this composes safely in
/// every configuration. `init()` MUST have run first.
fn init_tracing_subscriber() {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::{fmt, EnvFilter};

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let registry = tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt::layer());

    #[cfg(feature = "observability")]
    let registry = registry.with(cqlite_core::observability::tracing_layer());

    // `try_init` is tolerant of a subscriber already being set (e.g. in tests).
    let _ = registry.try_init();
}

/// Log the startup observability state so `CQLITE_OTEL_ENABLED=true` is never
/// silently inert (issue #2128).
///
/// `enabled` / `endpoint` come from [`cqlite_core::observability::ObservabilityConfig::from_env`],
/// which is always compiled regardless of the `observability` feature. Two
/// outcomes, both logged only when `enabled`:
///
/// * Feature compiled IN — one `info` line naming the OTLP endpoint traces and
///   metrics are being exported to.
/// * Feature compiled OUT — one `warn` line: the binary honours none of the
///   `CQLITE_OTEL_*` vars, so the operator isn't left wondering why
///   VictoriaMetrics/Tempo stay empty.
///
/// A no-op when `enabled` is false (the default, and the common case for a
/// build that never sets `CQLITE_OTEL_ENABLED`).
fn log_observability_status(enabled: bool, endpoint: &str) {
    if !enabled {
        return;
    }
    #[cfg(feature = "observability")]
    {
        tracing::info!(
            endpoint,
            "observability enabled, exporting to OTLP endpoint"
        );
    }
    #[cfg(not(feature = "observability"))]
    {
        let _ = endpoint;
        tracing::warn!(
            "CQLITE_OTEL_ENABLED is set but this binary was compiled without the \
             `observability` feature — CQLITE_OTEL_* environment variables are inert; \
             no metrics or traces will be exported"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_observability_status_disabled_is_a_noop() {
        // Must never panic regardless of feature state, and there's nothing to
        // assert beyond "it returns" — disabled is the common, silent case.
        log_observability_status(false, "http://localhost:4317");
    }

    #[test]
    fn log_observability_status_enabled_does_not_panic() {
        // Exercises the active branch (info-line-with-feature or
        // warn-line-without-feature depending on how this crate was built).
        log_observability_status(true, "http://localhost:4317");
    }
}
