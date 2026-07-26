//! End-to-end wiring evidence for the `--max-inflight-egress-bytes` /
//! `CQLITE_MAX_INFLIGHT_EGRESS_BYTES` per-stream egress ceiling (issue #2821).
//!
//! The in-crate suites (`src/egress_credit_tests.rs`, `src/egress_budget_tests.rs`)
//! prove the governor's behaviour against the producer/stream seams. This binary
//! proves the **knob** — that the operator-facing configuration actually reaches
//! that governor — by starting the REAL `cqlite-flight` server binary
//! (`CARGO_BIN_EXE_cqlite-flight`, so clap parses the real `Args`, reads the real
//! env var and writes the real startup log), streaming a REAL `do_get` through a
//! REAL `FlightServiceClient`, and observing that the configured ceiling governs
//! delivery. A helper-only unit test could not catch a knob that is parsed and
//! then dropped on the floor.
//!
//! The fixture is the synthetic wide-row shape from
//! `cqlite_flight::wide_row_fixture` (merged with issue #2825, REUSED here, never
//! duplicated), generated in process — never the fetched `test_wide_rows` corpus,
//! which would make every assertion here pass vacuously in an unfetched checkout.
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
use cqlite_flight::batch_bytes::ENV_MAX_BATCH_BYTES;
use cqlite_flight::egress_credit::{
    DEFAULT_MAX_INFLIGHT_EGRESS_BYTES, ENV_MAX_INFLIGHT_EGRESS_BYTES,
};
use cqlite_flight::wide_row_fixture as fx;

/// 120 rows x 16 KiB payload ~ 1.9 MiB of blob, cut into ~30 batches by the
/// 64 KiB per-batch payload cap below — many times any ceiling used here.
const ROWS: i32 = 120;
const PAYLOAD: usize = 16 * 1024;
const BATCH_CAP: usize = 64 * 1024;

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

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

fn ticket_bytes() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "keyspace": fx::BYTECAP_KS,
        "table": fx::WIDE_TBL,
        "ddl": fx::WIDE_DDL,
    }))
    .expect("ticket json")
}

// ---------------------------------------------------------------------------
// Server process control (mirrors tests/issue_2825_max_batch_bytes_e2e.rs)
// ---------------------------------------------------------------------------

struct ServerProcess {
    child: Child,
    addr: SocketAddr,
    log: PathBuf,
    _log_dir: tempfile::TempDir,
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl ServerProcess {
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

fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let port = l.local_addr().expect("local_addr").port();
    drop(l);
    port
}

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
            .arg("--max-batch-bytes")
            .arg(BATCH_CAP.to_string())
            .args(extra_args)
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err));
        // Start from a clean slate so a stray value in the developer's
        // environment cannot silently change what is under test.
        cmd.env_remove(ENV_MAX_BATCH_BYTES);
        cmd.env_remove(ENV_MAX_INFLIGHT_EGRESS_BYTES);
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

/// Stream a real `do_get` and return every decoded batch.
async fn stream_batches(addr: SocketAddr) -> Vec<arrow::record_batch::RecordBatch> {
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
    stream.try_collect().await.expect("decode batches")
}

fn run_scan(
    data_dir: &Path,
    extra_args: &[String],
    env: &[(&str, String)],
) -> (Vec<arrow::record_batch::RecordBatch>, String) {
    let server = start_server(data_dir, extra_args, env);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("rt");
    let batches = rt.block_on(stream_batches(server.addr));
    (batches, server.log())
}

fn assert_non_vacuous(batches: &[arrow::record_batch::RecordBatch], what: &str) {
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, ROWS as usize,
        "{what}: streamed {total} rows, expected {ROWS} — vacuous fixture"
    );
    assert!(
        batches.len() > 4,
        "{what}: only {} batches decoded — the fixture must exercise the ceiling",
        batches.len()
    );
}

// ---------------------------------------------------------------------------
// Requirement: the CLI-configured ceiling governs a real streamed do_get
// ---------------------------------------------------------------------------

/// A tiny CLI-configured ceiling — far SMALLER than one batch of this fixture —
/// still delivers every row through a real streamed `do_get`, in the same order
/// and with the same content as a generous ceiling. This is the wiring evidence
/// AND the end-to-end deadlock-avoidance guard: a governor that reached the
/// producer but did not clamp an over-ceiling reservation would hang here, and a
/// deferred slot released only on the NEXT yield would hang here too.
///
/// FAILS on pre-change `main`: `--max-inflight-egress-bytes` does not exist
/// there, so the server rejects the flag and never starts.
#[test]
fn a_tiny_cli_ceiling_still_streams_every_row_through_a_real_do_get() {
    let (_temp, data_dir) = build_wide_fixture();

    let (tiny, tiny_log) = run_scan(
        &data_dir,
        &["--max-inflight-egress-bytes".into(), "1024".into()],
        &[],
    );
    let (roomy, _) = run_scan(
        &data_dir,
        &[
            "--max-inflight-egress-bytes".into(),
            (8 * 1024 * 1024).to_string(),
        ],
        &[],
    );

    assert_non_vacuous(&tiny, "1 KiB ceiling");
    assert_non_vacuous(&roomy, "8 MiB ceiling");
    assert!(
        tiny_log.contains("max_inflight_egress_bytes=1024"),
        "the startup log does not record the CLI-configured ceiling:\n{tiny_log}"
    );
    // The ceiling governs RESIDENCY, never content: same schema, same rows, same
    // order, whatever the ceiling.
    let schema = tiny[0].schema();
    let tiny_all = arrow::compute::concat_batches(&schema, &tiny).expect("concat tiny");
    let roomy_all = arrow::compute::concat_batches(&schema, &roomy).expect("concat roomy");
    assert_eq!(
        tiny_all, roomy_all,
        "the egress ceiling changed the streamed content"
    );
    // Non-vacuity of the clamp: a single batch of this fixture really is bigger
    // than the whole 1 KiB ceiling, so every batch took the clamp path.
    let largest = tiny
        .iter()
        .map(|b| b.get_array_memory_size())
        .max()
        .unwrap_or(0);
    assert!(
        largest > 1024,
        "the fixture's batches ({largest} B) must exceed the 1 KiB ceiling for this \
         to prove the deadlock-avoidance clamp end to end"
    );
}

/// `CQLITE_MAX_INFLIGHT_EGRESS_BYTES` alone — no CLI flag — reaches the server's
/// own view of its configuration, and an explicit flag OVERRIDES it.
///
/// FAILS on pre-change `main`: nothing reads that variable there.
#[test]
fn the_env_var_backs_the_flag_and_the_flag_overrides_it() {
    let (_temp, data_dir) = build_wide_fixture();

    let (from_env, env_log) = run_scan(
        &data_dir,
        &[],
        &[(ENV_MAX_INFLIGHT_EGRESS_BYTES, (256 * 1024).to_string())],
    );
    assert_non_vacuous(&from_env, "env-configured ceiling");
    assert!(
        env_log.contains(&format!("max_inflight_egress_bytes={}", 256 * 1024)),
        "startup log does not record the env-configured ceiling:\n{env_log}"
    );

    let (_overridden, flag_log) = run_scan(
        &data_dir,
        &[
            "--max-inflight-egress-bytes".into(),
            (512 * 1024).to_string(),
        ],
        &[(ENV_MAX_INFLIGHT_EGRESS_BYTES, (256 * 1024).to_string())],
    );
    assert!(
        flag_log.contains(&format!("max_inflight_egress_bytes={}", 512 * 1024)),
        "an explicit flag must override the environment variable:\n{flag_log}"
    );
}

/// With neither flag nor environment variable the effective ceiling is
/// `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES`, and the startup log records it — so the
/// governor is ON by default on the deployment path, not opt-in.
#[test]
fn the_default_ceiling_applies_and_is_logged() {
    let (_temp, data_dir) = build_wide_fixture();
    let (batches, log) = run_scan(&data_dir, &[], &[]);
    assert_non_vacuous(&batches, "default ceiling");
    assert!(
        log.contains(&format!(
            "max_inflight_egress_bytes={DEFAULT_MAX_INFLIGHT_EGRESS_BYTES}"
        )),
        "startup log does not record the default ceiling \
         ({DEFAULT_MAX_INFLIGHT_EGRESS_BYTES}):\n{log}"
    );
}
