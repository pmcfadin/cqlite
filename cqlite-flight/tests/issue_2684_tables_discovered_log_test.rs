//! Issue #2684 spec Requirement 5 — the startup log line reports the discovered
//! table/keyspace count.
//!
//! The saturation sampler emits EXACTLY ONE `info`-level line
//! (`discovered N tables across M keyspaces under <data-dir>`) after its first
//! sample, `Once`-guarded (`saturation::log_tables_discovered_once`, called from
//! `run_sampler` after the first `sample_once`). This test captures the emitted
//! `tracing` output and asserts that exactly one such INFO line is emitted,
//! naming the genuine table count N, keyspace count M, and the data-dir path —
//! even if the sampler ticks more than once (the `Once` guard).
//!
//! # Isolation
//!
//! `log_tables_discovered_once`'s `Once` guard is PROCESS-GLOBAL, so a second
//! `run_sampler` in the same binary would NOT re-emit the line — this test lives
//! in its OWN integration-test binary with a single `#[test]` (matching the other
//! #2684 gauge tests' one-test-per-binary isolation convention) so the guard is
//! pristine when the test runs.
//!
//! The capturing subscriber is installed as the PROCESS-GLOBAL default
//! (`set_global_default`, not a thread-local `set_default`): `run_sampler` and its
//! log site run on a tokio worker thread distinct from the test task, which a
//! thread-local default would not observe (the #1703 cross-thread hazard). One
//! `#[test]` per binary makes the single global call safe.
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-flight --test issue_2684_tables_discovered_log_test
//! ```

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;

/// Rendered `message` field of a captured event.
#[derive(Default)]
struct MessageVisitor {
    message: String,
}
impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        }
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        }
    }
}

/// Captures the rendered message of every event it sees (installed behind an
/// `INFO` level filter, so only INFO-and-above events reach it).
#[derive(Clone)]
struct CaptureLayer {
    messages: Arc<Mutex<Vec<String>>>,
    events: Arc<AtomicUsize>,
}

impl<S: Subscriber> Layer<S> for CaptureLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        self.events.fetch_add(1, Ordering::Relaxed);
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        if let Ok(mut v) = self.messages.lock() {
            v.push(visitor.message);
        }
    }
}

/// Build a `<data_dir>/<keyspace>/<table>/nb-1-big-Data.db` fixture (readdir-only
/// discovery counts a dir directly containing a `*-Data.db` name — no real
/// SSTable content is parsed).
fn make_table_dir(data_dir: &Path, keyspace: &str, table: &str) {
    let dir = data_dir.join(keyspace).join(table);
    std::fs::create_dir_all(&dir).expect("create table dir");
    std::fs::write(dir.join("nb-1-big-Data.db"), b"x").expect("write data.db");
}

/// Spec Requirement 5: after the first sample the sampler emits EXACTLY ONE
/// INFO-level line naming the genuine table count, keyspace count, and the
/// data-dir path — and only once, even across multiple ticks (the `Once` guard).
#[test]
fn startup_log_line_reports_discovered_table_and_keyspace_count() {
    // A known layout: 3 genuine table dirs across 2 keyspaces (ks1: a, b; ks2: c).
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let data_dir = tmp.path().to_path_buf();
    make_table_dir(&data_dir, "ks1", "a");
    make_table_dir(&data_dir, "ks1", "b");
    make_table_dir(&data_dir, "ks2", "c");
    const EXPECTED_TABLES: u64 = 3;
    const EXPECTED_KEYSPACES: u64 = 2;

    // The exact line the production `Once`-guarded site formats.
    let expected_line = format!(
        "discovered {} tables across {} keyspaces under {}",
        EXPECTED_TABLES,
        EXPECTED_KEYSPACES,
        data_dir.display()
    );

    let messages = Arc::new(Mutex::new(Vec::<String>::new()));
    let events = Arc::new(AtomicUsize::new(0));
    let layer = CaptureLayer {
        messages: messages.clone(),
        events: events.clone(),
    }
    .with_filter(LevelFilter::INFO);
    // PROCESS-GLOBAL default: the log site runs on a tokio worker thread, not the
    // test task (a thread-local default would miss it — the #1703 hazard). Safe
    // to call once: this binary has exactly one `#[test]`.
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layer))
        .expect("set_global_default must succeed (only test in this binary)");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("multi-thread runtime");
    rt.block_on(async {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(cqlite_flight::saturation::run_sampler(
            // A tiny interval lets the sampler tick several times before shutdown,
            // proving the `Once` guard emits the line only ONCE across many ticks.
            Duration::from_millis(5),
            data_dir.clone(),
            async move {
                let _ = rx.await;
            },
        ));
        // Let the sampler tick repeatedly, then stop it.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let _ = tx.send(());
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("the sampler handle must resolve after shutdown (no forever-run)")
            .expect("the sampler task completed without panicking");
    });

    let captured = messages.lock().expect("messages lock").clone();
    // The sampler ran; SOME INFO event was seen (non-vacuous capture).
    assert!(
        events.load(Ordering::Relaxed) > 0,
        "the capturing subscriber saw no INFO events at all — capture is vacuous"
    );

    let discovered: Vec<&String> = captured
        .iter()
        .filter(|m| m.starts_with("discovered ") && m.contains(" tables across "))
        .collect();
    assert_eq!(
        discovered.len(),
        1,
        "EXACTLY ONE `discovered … tables …` INFO line must be emitted (the Once \
         guard), even though the sampler ticked many times; captured discovery \
         lines: {discovered:#?} (all INFO messages: {captured:#?})"
    );

    let line = discovered[0];
    // The line names the genuine table count, keyspace count, AND the data-dir
    // path — asserted as the full formatted string so a substring collision
    // (e.g. a digit inside the path) cannot pass it vacuously.
    assert_eq!(
        line, &expected_line,
        "the startup line must report N tables, M keyspaces, and the data-dir path \
         (spec Requirement 5)"
    );
    // Belt-and-braces: each named component is present.
    assert!(
        line.contains(&format!("{EXPECTED_TABLES} tables"))
            && line.contains(&format!("{EXPECTED_KEYSPACES} keyspaces"))
            && line.contains(&data_dir.display().to_string()),
        "the line must name the table count, keyspace count, and data-dir path"
    );
}
