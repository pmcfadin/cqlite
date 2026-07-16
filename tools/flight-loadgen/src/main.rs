//! `flight-loadgen` CLI (issue #2418, epic #2313 WS1).
//!
//! Drives a raw `FlightServiceClient` concurrency ramp of `do_get` requests
//! DIRECTLY against a running `cqlite-flight` endpoint — no Trino, no JDBC, no
//! `cqlite-core` query engine on the client path — and emits one
//! `flight-loadgen.step/v1` JSONL record per ramp step (the "server-direct
//! ceiling" that feeds the #2399 round-N C-throughput block).
//!
//! Run `flight-loadgen --help` for flags, or `flight-loadgen --self-test` to
//! exercise the full client→server→JSONL pipeline against an in-process fixture.

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;

use flight_loadgen::ramp::{parse_duration, parse_ramp, run_ramp, RampConfig, StepBound};
use flight_loadgen::record::StepRecord;
use flight_loadgen::selftest::run_self_test;
use flight_loadgen::shape::{MixWeights, Shape, ShapeGen};

use cqlite_flight::ticket::FlightTicket;

/// Raw FlightServiceClient concurrency-ramp load generator for cqlite-flight.
#[derive(Debug, Parser)]
#[command(name = "flight-loadgen", version, about)]
struct Cli {
    /// `cqlite-flight` endpoint, e.g. `http://127.0.0.1:8815`. Required unless
    /// `--self-test`.
    #[arg(long)]
    endpoint: Option<String>,

    /// Path to the base ticket-template JSON (connector-shaped `FlightTicket`:
    /// keyspace/table/ddl/snapshot, full ring, no limit). Required unless
    /// `--self-test`.
    #[arg(long)]
    ticket_template: Option<PathBuf>,

    /// Ordered target concurrencies, comma-separated (one ramp step each).
    #[arg(long, default_value = "1,2,4,8,16,32")]
    ramp: String,

    /// Per-step hold duration (e.g. `30s`, `500ms`, `2m`).
    #[arg(long, default_value = "30s")]
    step_duration: String,

    /// Workload shape: `point` | `limit-k` | `full` | `mixed`.
    #[arg(long, default_value = "mixed")]
    shape: String,

    /// `LIMIT` k for the `limit-k` (and mixed) shape.
    #[arg(long, default_value_t = 100)]
    limit_k: u64,

    /// Token sub-range width for the `point` shape (a small fraction of the ring).
    #[arg(long, default_value_t = 1 << 40)]
    point_width: i64,

    /// Mixed-shape weights, e.g. `ptr=0.6,lim=0.3,full=0.1`.
    #[arg(long, default_value = "ptr=0.6,lim=0.3,full=0.1")]
    mix: String,

    /// Deterministic RNG seed for ticket selection.
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Round label stamped on each JSONL record.
    #[arg(long, default_value = "")]
    round: String,

    /// Write JSONL records here (one object per line). Defaults to stdout.
    #[arg(long)]
    out: Option<PathBuf>,

    /// Per-worker TCP connect timeout (e.g. `5s`).
    #[arg(long, default_value = "5s")]
    connect_timeout: String,

    /// Run the in-process wiring self-test instead of a real ramp: serve a tiny
    /// fixture on an ephemeral loopback port and run a concurrency-1,
    /// fixed-request-count ramp against it. Ignores `--endpoint`/`--ticket-template`.
    #[arg(long)]
    self_test: bool,

    /// Fixed request count for `--self-test` (count-bounded, no wall-clock).
    #[arg(long, default_value_t = 3)]
    self_test_requests: u64,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("flight-loadgen: failed to build tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(run(cli)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("flight-loadgen: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), String> {
    let records = if cli.self_test {
        run_self_test(cli.self_test_requests).await?
    } else {
        run_real_ramp(&cli).await?
    };
    write_records(&records, cli.out.as_deref())
}

/// Drive the operator ramp against `--endpoint` using `--ticket-template`.
async fn run_real_ramp(cli: &Cli) -> Result<Vec<StepRecord>, String> {
    let endpoint = cli
        .endpoint
        .clone()
        .ok_or("--endpoint is required (unless --self-test)")?;
    let template_path = cli
        .ticket_template
        .as_ref()
        .ok_or("--ticket-template is required (unless --self-test)")?;

    let template = load_template(template_path)?;
    let shape = Shape::parse(&cli.shape)?;
    let mix = MixWeights::parse(&cli.mix)?;
    let concurrencies = parse_ramp(&cli.ramp)?;
    let step_duration = parse_duration(&cli.step_duration)?;
    let connect_timeout = parse_duration(&cli.connect_timeout)?;

    let gen = ShapeGen::new(template, cli.seed, cli.limit_k, cli.point_width, mix);
    let config = RampConfig {
        concurrencies,
        bound: StepBound::Duration(step_duration),
        shape,
        round: cli.round.clone(),
        endpoint,
        connect_timeout,
        seed: cli.seed,
    };
    run_ramp(&config, &gen).await
}

/// Load and validate the base ticket template from disk.
fn load_template(path: &std::path::Path) -> Result<FlightTicket, String> {
    let bytes = std::fs::read(path)
        .map_err(|e| format!("reading --ticket-template {}: {e}", path.display()))?;
    FlightTicket::from_bytes(&bytes)
        .map_err(|e| format!("parsing --ticket-template {}: {e}", path.display()))
}

/// Write records as JSONL (one object per line) to `out` or stdout.
fn write_records(records: &[StepRecord], out: Option<&std::path::Path>) -> Result<(), String> {
    let mut buf = String::new();
    for rec in records {
        let line = rec
            .to_jsonl()
            .map_err(|e| format!("serializing record: {e}"))?;
        buf.push_str(&line);
        buf.push('\n');
    }
    match out {
        Some(path) => std::fs::write(path, buf.as_bytes())
            .map_err(|e| format!("writing --out {}: {e}", path.display())),
        None => {
            let stdout = std::io::stdout();
            let mut lock = stdout.lock();
            lock.write_all(buf.as_bytes())
                .and_then(|()| lock.flush())
                .map_err(|e| format!("writing stdout: {e}"))
        }
    }
}
