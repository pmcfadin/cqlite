//! End-to-end wiring evidence for the `--max-batch-bytes` /
//! `CQLITE_MAX_BATCH_BYTES` byte-cap knob (issue #2825).
//!
//! The in-crate unit suite (`src/batch_bytes_tests.rs`) proves the boundary rule
//! against the producer seams. This binary proves the **knob** — that the
//! operator-facing configuration actually reaches those seams — by starting the
//! REAL `cqlite-flight` server binary (`CARGO_BIN_EXE_cqlite-flight`, so clap
//! parses the real `Args`, reads the real env var, and writes the real startup
//! log), streaming a REAL `do_get` through a REAL `FlightServiceClient`, and
//! observing where the batch boundaries fall. A helper-only unit test could not
//! catch a knob that is parsed and then dropped on the floor.
//!
//! The fixture is the synthetic wide-row shape from
//! `cqlite_flight::wide_row_fixture`, generated in process — never the fetched
//! `test_wide_rows` corpus, which would make every assertion here pass vacuously
//! in an unfetched checkout.
//!
//! No assertion in this file compares an elapsed duration against a threshold
//! (#2642). The timeouts below are liveness bounds on process/socket readiness,
//! not correctness properties.

use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use futures::TryStreamExt;
use tonic::transport::Channel;

use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
use cqlite_flight::batch_bytes::{DEFAULT_MAX_BATCH_BYTES, ENV_MAX_BATCH_BYTES};
use cqlite_flight::wide_row_fixture as fx;

/// 220 rows x 4 KiB payload ~ 900 KiB of blob: enough that a 64 KiB cap cuts
/// ~14 batches while the default `--batch-size 8192` row-cap never binds.
const ROWS: i32 = 220;
const PAYLOAD: usize = 4096;

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

/// Flush the synthetic wide-row fixture into a real SSTable and return the temp
/// dir (keep it alive) plus the server's `--data-dir`.
fn build_wide_fixture() -> (tempfile::TempDir, PathBuf) {
    let schema = fx::wide_row_schema();
    let temp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema);
    let mut engine = WriteEngine::new(config).expect("engine");
    for m in fx::wide_row_mutations(ROWS, PAYLOAD) {
        engine.write(m).expect("write");
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");
    rt.block_on(engine.flush()).expect("flush").expect("info");
    (temp, data_dir)
}

/// The wire ticket the connector would send for the wide fixture.
fn ticket_bytes() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "keyspace": fx::BYTECAP_KS,
        "table": fx::WIDE_TBL,
        "ddl": fx::WIDE_DDL,
    }))
    .expect("ticket json")
}

// ---------------------------------------------------------------------------
// Server process control
// ---------------------------------------------------------------------------

/// A spawned `cqlite-flight` server process, killed on drop so a failing
/// assertion can never leak a listener.
struct ServerProcess {
    child: Child,
    addr: SocketAddr,
    log: PathBuf,
    // Kept so the log file outlives the process.
    _log_dir: tempfile::TempDir,
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl ServerProcess {
    /// Everything the server wrote to stdout+stderr so far (the startup log),
    /// with the `tracing-subscriber` ANSI styling stripped so the field
    /// assertions match plain `key=value` text.
    fn log(&self) -> String {
        strip_ansi(&std::fs::read_to_string(&self.log).unwrap_or_default())
    }
}

/// Drop CSI escape sequences (`ESC [ <params> <final byte>`) from `s`.
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\u{1b}' {
            out.push(c);
            continue;
        }
        // Skip the `[` introducer, then every parameter/intermediate byte up to
        // and including the final byte in `@..=~` (e.g. the `m` of `ESC[32m`).
        if chars.peek() == Some(&'[') {
            chars.next();
        }
        for c in chars.by_ref() {
            if ('@'..='~').contains(&c) {
                break;
            }
        }
    }
    out
}

/// An ephemeral loopback port that is free right now.
fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let port = l.local_addr().expect("local_addr").port();
    drop(l);
    port
}

/// Start the REAL server binary over `data_dir` with `extra_args` and `env`, and
/// wait until it accepts connections.
///
/// Retries on a port that another process grabbed in the gap between
/// [`free_port`] and the child's own bind — a startup race, not a correctness
/// property, so no assertion depends on timing here.
fn start_server(data_dir: &Path, extra_args: &[String], env: &[(&str, String)]) -> ServerProcess {
    let exe = env!("CARGO_BIN_EXE_cqlite-flight");
    let mut last_log = String::new();
    for _ in 0..3 {
        let port = free_port();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
        let log_dir = tempfile::TempDir::new().expect("log dir");
        let log = log_dir.path().join("server.log");
        let out = std::fs::File::create(&log).expect("log file");
        let err = out.try_clone().expect("clone log fd");

        let mut cmd = Command::new(exe);
        cmd.arg("--data-dir")
            .arg(data_dir)
            .arg("--listen")
            .arg(addr.to_string())
            .args(extra_args)
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err));
        // Start from a clean slate so a stray CQLITE_MAX_BATCH_BYTES in the
        // developer's environment cannot silently change what is under test.
        cmd.env_remove(ENV_MAX_BATCH_BYTES);
        for (k, v) in env {
            cmd.env(k, v);
        }
        let child = cmd.spawn().expect("spawn cqlite-flight");
        let mut server = ServerProcess {
            child,
            addr,
            log,
            _log_dir: log_dir,
        };

        // Liveness wait: poll the socket until the server accepts.
        for _ in 0..200 {
            if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(50)).is_ok() {
                return server;
            }
            if let Ok(Some(_)) = server.child.try_wait() {
                break; // exited early (likely a port collision) — retry
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        last_log = server.log();
    }
    panic!("cqlite-flight never became ready; last server log:\n{last_log}");
}

/// Stream a real `do_get` against `addr` and return the row count of each
/// decoded batch, in order.
async fn stream_batch_row_counts(addr: SocketAddr) -> Vec<usize> {
    let channel = Channel::from_shared(format!("http://{addr}"))
        .expect("endpoint")
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await
        .expect("connect");
    let mut client = FlightServiceClient::new(channel);
    let resp = client
        .do_get(Ticket::new(ticket_bytes()))
        .await
        .expect("do_get rpc");
    let stream =
        FlightRecordBatchStream::new_from_flight_data(resp.into_inner().map_err(|e| e.into()));
    let batches: Vec<_> = stream.try_collect().await.expect("decode batches");
    batches.iter().map(|b| b.num_rows()).collect()
}

/// Start a server with the given cap configuration and return its decoded
/// per-batch row counts plus the startup log.
fn run_scan(
    data_dir: &Path,
    extra_args: &[String],
    env: &[(&str, String)],
) -> (Vec<usize>, String) {
    let server = start_server(data_dir, extra_args, env);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("rt");
    let counts = rt.block_on(stream_batch_row_counts(server.addr));
    (counts, server.log())
}

fn assert_non_vacuous(counts: &[usize], what: &str) {
    let total: usize = counts.iter().sum();
    assert_eq!(
        total, ROWS as usize,
        "{what}: streamed {total} rows, expected {ROWS} — vacuous fixture"
    );
    assert!(
        !counts.is_empty(),
        "{what}: no batches decoded from the stream"
    );
}

// ---------------------------------------------------------------------------
// Requirement 6: the knob governs a real streamed do_get
// ---------------------------------------------------------------------------

/// Two distinct `--max-batch-bytes` CLI values, over the same ticket, produce
/// correspondingly different batch boundaries through a real streamed `do_get`:
/// the smaller cap yields strictly more batches with strictly fewer rows each.
///
/// FAILS on pre-change `main`: `--max-batch-bytes` does not exist there, so the
/// server rejects the flag and never starts.
#[test]
fn cli_flag_governs_streamed_do_get_batch_boundaries() {
    let (_temp, data_dir) = build_wide_fixture();

    let (small, _) = run_scan(
        &data_dir,
        &["--max-batch-bytes".into(), (32 * 1024).to_string()],
        &[],
    );
    let (large, _) = run_scan(
        &data_dir,
        &["--max-batch-bytes".into(), (256 * 1024).to_string()],
        &[],
    );

    assert_non_vacuous(&small, "small CLI cap");
    assert_non_vacuous(&large, "large CLI cap");
    assert!(
        small.len() > 1,
        "small cap produced a single batch — the cap is not wired"
    );
    assert!(
        small.len() > large.len(),
        "smaller cap yielded {} batches, not more than the larger cap's {}: \
         {small:?} vs {large:?}",
        small.len(),
        large.len()
    );
    let (max_small, max_large) = (
        small.iter().copied().max().unwrap_or(0),
        large.iter().copied().max().unwrap_or(0),
    );
    assert!(
        max_small < max_large,
        "smaller cap's widest batch ({max_small} rows) is not smaller than the \
         larger cap's ({max_large} rows)"
    );
}

/// `CQLITE_MAX_BATCH_BYTES` alone — no CLI flag — governs the same real streamed
/// `do_get`, and the observed boundaries match the environment-configured cap.
///
/// FAILS on pre-change `main`: nothing reads that variable there, so both runs
/// produce identical single-batch output.
#[test]
fn env_var_governs_streamed_do_get_batch_boundaries() {
    let (_temp, data_dir) = build_wide_fixture();

    let (small, small_log) = run_scan(
        &data_dir,
        &[],
        &[(ENV_MAX_BATCH_BYTES, (32 * 1024).to_string())],
    );
    let (large, large_log) = run_scan(
        &data_dir,
        &[],
        &[(ENV_MAX_BATCH_BYTES, (256 * 1024).to_string())],
    );

    assert_non_vacuous(&small, "small env cap");
    assert_non_vacuous(&large, "large env cap");
    // The configured value reached the server's own view of its configuration.
    assert!(
        small_log.contains(&format!("max_batch_bytes={}", 32 * 1024)),
        "startup log does not record the env-configured cap:\n{small_log}"
    );
    assert!(
        large_log.contains(&format!("max_batch_bytes={}", 256 * 1024)),
        "startup log does not record the env-configured cap:\n{large_log}"
    );
    assert!(
        small.len() > large.len(),
        "env cap did not change the boundary: {small:?} vs {large:?}"
    );
}

/// With neither flag nor environment variable, the effective cap is
/// `DEFAULT_MAX_BATCH_BYTES` and the startup log records it — and the whole
/// ~900 KiB fixture, being far under 4 MiB, streams as a single row-cut batch,
/// confirming the default is a no-op on sub-cap data.
#[test]
fn the_default_cap_applies_and_is_logged() {
    let (_temp, data_dir) = build_wide_fixture();
    let (counts, log) = run_scan(&data_dir, &[], &[]);
    assert_non_vacuous(&counts, "default cap");
    assert!(
        log.contains(&format!("max_batch_bytes={DEFAULT_MAX_BATCH_BYTES}")),
        "startup log does not record the default cap ({DEFAULT_MAX_BATCH_BYTES}):\n{log}"
    );
    assert_eq!(
        counts.len(),
        1,
        "the ~900 KiB fixture should fit one 4 MiB batch, got {counts:?}"
    );
}

// ---------------------------------------------------------------------------
// Requirement 7: content invariance across the transport
// ---------------------------------------------------------------------------

/// A capped and an effectively-unbounded run stream the same rows in the same
/// order with the same values and the same Arrow schema — only the batch
/// boundaries differ.
#[test]
fn capped_and_unbounded_streams_carry_identical_content() {
    let (_temp, data_dir) = build_wide_fixture();

    let fetch = |cap: String| {
        let server = start_server(&data_dir, &["--max-batch-bytes".into(), cap], &[]);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("rt");
        rt.block_on(async {
            let channel = Channel::from_shared(format!("http://{}", server.addr))
                .expect("endpoint")
                .connect_timeout(Duration::from_secs(10))
                .connect()
                .await
                .expect("connect");
            let mut client = FlightServiceClient::new(channel);
            let resp = client
                .do_get(Ticket::new(ticket_bytes()))
                .await
                .expect("do_get rpc");
            let stream = FlightRecordBatchStream::new_from_flight_data(
                resp.into_inner().map_err(|e| e.into()),
            );
            let batches: Vec<arrow::record_batch::RecordBatch> =
                stream.try_collect().await.expect("decode");
            let schema = batches.first().map(|b| b.schema()).expect("a batch");
            let n_batches = batches.len();
            let cat = arrow::compute::concat_batches(&schema, &batches).expect("concat");
            (n_batches, cat)
        })
    };

    let (small_n, small) = fetch((32 * 1024).to_string());
    let (big_n, big) = fetch(usize::MAX.to_string());
    assert!(
        small_n > big_n,
        "the cap changed no boundary ({small_n} vs {big_n}) — comparison vacuous"
    );
    assert_eq!(small.schema(), big.schema(), "Arrow schema differs");
    assert_eq!(small.num_rows(), ROWS as usize);
    assert_eq!(small, big, "row content or order differs across cap values");
}
