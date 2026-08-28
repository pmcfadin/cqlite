//! Spawn-and-wait control for the REAL `cqlite-flight` server binary, shared by
//! the end-to-end knob-wiring tests (issue #3384).
//!
//! `tests/issue_2821_egress_budget_e2e.rs` and
//! `tests/issue_2825_max_batch_bytes_e2e.rs` each carried a byte-identical copy of
//! this helper. Both copies had the SAME readiness defect (below), so the fix had
//! to land twice — the shape issue #1577 exists to stop. One definition now, used
//! by both binaries.
//!
//! # The readiness contract, and the defect it replaces
//!
//! [`free_port`] binds `127.0.0.1:0`, reads the port and DROPS the listener; the
//! child then re-binds it. That is a TOCTOU window by construction: another
//! process (including a sibling test in the same binary, which runs three of these
//! in parallel) can take the port in the gap, and our child then dies of
//! `EADDRINUSE`. The window cannot be closed from here — the server binary takes a
//! `--listen` address, not an inherited socket — so the collision is retried
//! instead.
//!
//! What matters is that the collision is DETECTED. The previous readiness loop
//! polled `TcpStream::connect_timeout` FIRST and returned the instant it
//! succeeded, consulting `child.try_wait()` only after a FAILED connect. So when
//! our child had died of `EADDRINUSE` while the colliding listener still held the
//! port, the loop returned a DEAD CHILD paired with a FOREIGN SOCKET: the test
//! then streamed from someone else's server and asserted against its own dead
//! child's empty log file — which presents exactly as
//! `startup log does not record the env-configured ceiling`.
//!
//! Readiness therefore now means all three of:
//!
//! 1. **Our child is alive** (`try_wait()` is `None`) — checked BEFORE a
//!    successful connect is accepted, and again after, since a child that loses
//!    the bind race exits promptly at bind time.
//! 2. **Our child logged its own startup line**, carrying OUR listen address, into
//!    OUR private temp-dir log. A foreign server cannot put that there.
//! 3. **The socket accepts.**
//!
//! The startup line used to be the CONFIGURATION line (`cqlite-flight starting`),
//! and roborev (issue #3384) showed that was not enough: `log_startup` runs BEFORE
//! `serve_with_shutdown` binds, so a child could log the expected address, lose the
//! bind race, still be alive when the probe connected to the FOREIGN listener, pass
//! the final `try_wait`, and only then exit with `EADDRINUSE`. The server now emits
//! a separate line AFTER `TcpListener::bind` returns (`cli::log_listening`), and
//! that is what (2) matches — a line no process that failed to bind can have
//! written.
//!
//! Residual, stated rather than implied away: a child could bind, log, and then die
//! while a foreign process takes the freed port before (3) connects. That requires a
//! crash in a window bounded by two adjacent statements, rather than the ordinary
//! bind race the old loop returned from as a matter of course.
//!
//! No assertion here compares an elapsed duration against a threshold (#2642): the
//! timeouts are liveness bounds on process/socket readiness, not correctness
//! properties.

#![allow(dead_code)]

use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

/// The startup line `cqlite_flight::cli::log_startup` emits, and the field
/// carrying the address it was told to listen on. Together they identify OUR
/// child's log rather than merely "some server started".
/// The POST-BIND readiness line (`cli::log_listening`, issue #3384).
///
/// Deliberately NOT the `cqlite-flight starting` configuration line: that one is
/// written BEFORE the port is acquired, so a child can log it, lose the bind race
/// to a sibling, and still be alive when the probe connects to the FOREIGN
/// listener — which is precisely the residual roborev found in the first version
/// of this readiness fix. Only a line written after `TcpListener::bind` returned
/// proves this child owns the port.
const LISTENING_LINE: &str = "cqlite-flight listening on";

/// Readiness poll budget: 200 attempts x 50ms socket timeout + 50ms sleep.
const READY_ATTEMPTS: usize = 200;
const READY_POLL: Duration = Duration::from_millis(50);
/// Port-collision retries (see the module doc — the TOCTOU is retried, not closed).
const BIND_ATTEMPTS: usize = 3;

/// A spawned `cqlite-flight` server process, killed on drop so a failing
/// assertion can never leak a listener.
pub struct ServerProcess {
    child: Child,
    /// The address this server was told to listen on, and (per the readiness
    /// contract) the one its own startup log records.
    pub addr: SocketAddr,
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
    pub fn log(&self) -> String {
        strip_ansi(&std::fs::read_to_string(&self.log).unwrap_or_default())
    }

    /// `Some(status)` once the child has exited, `None` while it is running.
    fn exited(&mut self) -> Option<std::process::ExitStatus> {
        self.child.try_wait().ok().flatten()
    }

    /// Has THIS child written its own startup line for THIS listen address?
    fn logged_own_startup(&self) -> bool {
        let log = self.log();
        log.contains(&format!("{LISTENING_LINE} {}", self.addr))
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

/// An ephemeral loopback port that is free right now (see the module doc for the
/// TOCTOU this leaves open and how [`start_server`] detects it).
fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let port = l.local_addr().expect("local_addr").port();
    drop(l);
    port
}

/// How to launch the server under test.
pub struct ServerSpec<'a> {
    /// `--data-dir`.
    pub data_dir: &'a Path,
    /// Arguments appended after `--data-dir`/`--listen`.
    pub args: &'a [String],
    /// Environment to SET for the child.
    pub env: &'a [(&'a str, String)],
    /// Environment to REMOVE first, so a stray value in the developer's
    /// environment cannot silently change what is under test.
    pub env_remove: &'a [&'a str],
}

/// Start the REAL server binary per `spec` and return once it is ready — which
/// means our own child is alive, has logged its own startup line for our listen
/// address, and accepts connections (see the module doc).
///
/// Panics with the last child's log if no attempt became ready.
pub fn start_server(spec: ServerSpec<'_>) -> ServerProcess {
    let exe = env!("CARGO_BIN_EXE_cqlite-flight");
    let mut last_log = String::new();
    for _ in 0..BIND_ATTEMPTS {
        let port = free_port();
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
        let log_dir = tempfile::TempDir::new().expect("log dir");
        let log = log_dir.path().join("server.log");
        let out = std::fs::File::create(&log).expect("log file");
        let err = out.try_clone().expect("clone log fd");

        let mut cmd = Command::new(exe);
        cmd.arg("--data-dir")
            .arg(spec.data_dir)
            .arg("--listen")
            .arg(addr.to_string())
            .args(spec.args)
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err));
        for k in spec.env_remove {
            cmd.env_remove(k);
        }
        // Pin the child's log filter (roborev, issue #3384). Readiness below waits for
        // an INFO line the server emits after binding, and the child would otherwise
        // INHERIT `RUST_LOG` — so a developer or CI runner with `RUST_LOG=warn` would
        // make a perfectly healthy server never become "ready", and the failure would
        // look like a hung server rather than a filtered log line. Set before the
        // caller's env so a test can still override it deliberately.
        cmd.env("RUST_LOG", "info");
        for (k, v) in spec.env {
            cmd.env(k, v);
        }
        let child = cmd.spawn().expect("spawn cqlite-flight");
        let server = ServerProcess {
            child,
            addr,
            log,
            _log_dir: log_dir,
        };

        match wait_until_ready(server) {
            Ok(ready) => return ready,
            // Not ready: our child either exited (a lost bind race) or never came
            // up within the budget. Either way, retry on a fresh port.
            Err(log) => last_log = log,
        }
    }
    panic!("cqlite-flight never became ready; last server log:\n{last_log}");
}

/// The readiness contract of the module doc, applied to ONE spawned child.
///
/// Takes the child BY VALUE and hands it back on success, so a not-ready child is
/// dropped (and killed) here rather than escaping to the caller: `Err` carries only
/// its log, for the panic message.
fn wait_until_ready(mut server: ServerProcess) -> Result<ServerProcess, String> {
    for _ in 0..READY_ATTEMPTS {
        // (1) OUR child must still be running. Checked FIRST: a child that lost
        // the bind race is already gone, and accepting a connect in that state is
        // how the old loop paired a dead child with a foreign socket.
        if server.exited().is_some() {
            return Err(server.log());
        }
        // (2) OUR child must have logged its own startup line for OUR address,
        // into OUR private log — a foreign server cannot write there. (3) the
        // socket must accept. Then (1) again: the child could have exited between
        // the log check and the connect, leaving a foreign listener behind.
        if server.logged_own_startup()
            && std::net::TcpStream::connect_timeout(&server.addr, READY_POLL).is_ok()
            && server.exited().is_none()
        {
            return Ok(server);
        }
        std::thread::sleep(READY_POLL);
    }
    Err(server.log())
}
