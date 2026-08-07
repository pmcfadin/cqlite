//! Affinity conformance for the derived admission default (issue #3225, §4).
//!
//! The container-correctness claim in AC2 is that the derived ceiling follows
//! the parallelism available to **this process**, not the host's CPU count.
//! `tests/issue_3225_derived_default.rs` guards that STRUCTURALLY (no
//! host-topology read on the derivation path); this file is the BEHAVIOURAL
//! half: start the real server binary under a restricted CPU affinity mask and
//! read the derived ceiling back out of its startup log.
//!
//! **`#[ignore]`d by design, and it carries no timing assertion of any kind.**
//! It launches a process and binds a socket, which is not something the
//! correctness gate should do on every run, and CLAUDE.md bans wall-clock
//! threshold asserts from the correctness path. Nothing here measures duration —
//! the assertions are purely "mask size N ⇒ logged `available_parallelism` N ⇒
//! logged ceiling `clamp(2N, 2, 64)`".
//!
//! Run it explicitly (Linux, `util-linux` `taskset` required):
//!
//! ```text
//! cargo test -p cqlite-flight --test issue_3225_affinity_conformance -- --ignored --nocapture
//! ```
//!
//! The **cgroup** arm of the same claim is recorded as evidence captured on the
//! measurement box (see the #3225 report), not as a test: the gate runner's
//! cgroup is not ours to control, so asserting on it here would only be
//! asserting on whatever cgroup CI happens to hand us.

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

use cqlite_flight::admission::derive_max_concurrent_scans;

/// Start the server pinned to `cpu_list` and return its `cqlite-flight starting`
/// log line.
fn startup_line_under_affinity(cpu_list: &str) -> String {
    let data_dir = tempfile::tempdir().expect("temp data dir");
    let mut child = Command::new("taskset")
        .arg("-c")
        .arg(cpu_list)
        .arg(env!("CARGO_BIN_EXE_cqlite-flight"))
        .arg("--data-dir")
        .arg(data_dir.path())
        // Port 0: let the kernel pick, so a concurrent run cannot collide.
        .arg("--listen")
        .arg("127.0.0.1:0")
        .env("RUST_LOG", "info")
        // Neither the flag nor the env var for the ceiling: this must DERIVE.
        .env_remove("CQLITE_MAX_CONCURRENT_SCANS")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| {
            panic!(
                "could not run `taskset -c {cpu_list} <cqlite-flight>`: {e}. This conformance \
                 check needs Linux + util-linux `taskset`."
            )
        });

    let stdout = child.stdout.take().expect("piped stdout");
    // The startup event is emitted BEFORE the server begins serving, so this
    // read terminates on a healthy start; on a failed start the pipe reaches
    // EOF and the `expect` below reports it.
    let found = BufReader::new(stdout)
        .lines()
        .map_while(Result::ok)
        .find(|line| line.contains("cqlite-flight starting"));

    let _ = child.kill();
    let _ = child.wait();

    let line = found.unwrap_or_else(|| {
        panic!("the server produced no `cqlite-flight starting` line under `taskset -c {cpu_list}`")
    });
    strip_ansi(&line)
}

/// Drop ANSI SGR escapes (`ESC [ … m`) from a log line.
///
/// `tracing_subscriber`'s `fmt` layer colourises field names when it believes it
/// has a terminal, which would otherwise split `available_parallelism` from its
/// `=1`. Stripping is version-independent, unlike relying on `NO_COLOR`.
fn strip_ansi(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            // Consume through the sequence's final byte (`m` for SGR).
            for escaped in chars.by_ref() {
                if escaped.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Assert the derived ceiling follows a mask of exactly `cpus` CPUs.
fn assert_mask_derives(cpu_list: &str, cpus: usize) {
    let line = startup_line_under_affinity(cpu_list);
    let expected_parallelism = format!("available_parallelism={cpus}");
    let expected_ceiling = format!("max_concurrent_scans={}", derive_max_concurrent_scans(cpus));
    assert!(
        line.contains(&expected_parallelism),
        "under `taskset -c {cpu_list}` the log must report `{expected_parallelism}` (the MASK \
         size, not the host's CPU count): {line}"
    );
    assert!(
        line.contains(&expected_ceiling),
        "under `taskset -c {cpu_list}` the derived ceiling must be `{expected_ceiling}`: {line}"
    );
    assert!(
        line.contains(r#"max_concurrent_scans_source="derived""#),
        "with neither flag nor env set the provenance must be `derived`: {line}"
    );
}

#[test]
#[ignore = "spawns the server binary under taskset; run explicitly (see the module docs)"]
fn a_one_cpu_mask_derives_the_floor() {
    // A one-CPU mask derives 2, never 1: a single-permit server serialises every
    // scan, and #3217 measured N=1 as the worst point at every width.
    assert_mask_derives("0", 1);
}

#[test]
#[ignore = "spawns the server binary under taskset; run explicitly (see the module docs)"]
fn a_two_cpu_mask_derives_twice_the_mask_size() {
    let host = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(1);
    assert!(
        host >= 2,
        "this check needs at least 2 CPUs available to the test process (host reports {host})"
    );
    assert_mask_derives("0,1", 2);
}
