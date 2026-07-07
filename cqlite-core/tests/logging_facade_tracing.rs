//! Facade doctrine tests for the `logging-facade-tracing` OpenSpec change (#1706).
//!
//! `cqlite-core` must emit every runtime *event* through the `tracing` facade
//! only. Two tests enforce this structurally:
//!
//! 1. [`bridge_less_tracing_subscriber_captures_corruption_event`] — the
//!    acceptance oracle. A `tracing`-only subscriber with **no** `tracing-log`
//!    `LogTracer` bridge installed (exactly what a modern embedder wires) must
//!    receive a `cqlite-core` corruption diagnostic. On the pre-migration tree
//!    this event is emitted through the `log` facade and is silently dropped by
//!    a bridge-less `tracing` subscriber, so this test is RED on `main`.
//!
//! 2. [`no_log_event_macros_remain_in_core_src`] — the grep-guard. Zero
//!    `log::{warn,info,debug,error,trace}!` event macros may remain in
//!    `cqlite-core/src`, word-boundary matched so identifiers that merely
//!    contain the substring `log::` (e.g. `catalog::`, `dialog::`) do not count.
//!    RED before the sweep, GREEN after.

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};
use tracing::subscriber::with_default;
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::Registry;

/// One captured `tracing` event: its level plus the rendered `message` field.
#[derive(Clone, Debug)]
struct CapturedEvent {
    level: Level,
    message: String,
}

/// A stock `tracing` `Layer` that records every event it sees into a shared
/// `Vec`. This is what a real embedder observes through a plain `tracing`
/// subscriber — no `log`/`LogTracer` bridge is involved.
struct CaptureLayer {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl<S: Subscriber> Layer<S> for CaptureLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        let captured = CapturedEvent {
            level: *event.metadata().level(),
            message: visitor.message,
        };
        if let Ok(mut events) = self.events.lock() {
            events.push(captured);
        }
    }
}

/// Extracts the rendered `message` field from an event.
#[derive(Default)]
struct MessageVisitor {
    message: String,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        }
    }
}

/// Acceptance oracle: a `tracing`-only subscriber (NO `LogTracer` bridge)
/// receives a `cqlite-core` corrupt/invalid-SSTable diagnostic.
///
/// The driver is [`cqlite_core::parser::header::parse_magic_and_version`], a
/// public API. Feeding it an unknown magic number exercises the "never silently
/// swallow a corrupt/invalid SSTable header" path, which emits the
/// `Unknown magic number: 0x…` event before rejecting the input.
///
/// On the pre-migration tree this event is a `log::error!`; a bridge-less
/// `tracing` subscriber never sees it, so the assertions below fail (RED). After
/// the facade migration it is a `tracing::error!` and is delivered end to end.
#[test]
fn bridge_less_tracing_subscriber_captures_corruption_event() {
    let events = Arc::new(Mutex::new(Vec::<CapturedEvent>::new()));
    let subscriber = Registry::default().with(CaptureLayer {
        events: Arc::clone(&events),
    });

    // Scope the subscriber to this closure via `tracing`'s own dispatcher.
    // Deliberately no `tracing_log::LogTracer` / `env_logger` is installed:
    // this mirrors an embedder that wires only a `tracing` subscriber.
    with_default(subscriber, || {
        // 0xDEADBEEF is not a recognized Cassandra magic number, so the header
        // parser emits the corrupt/invalid-header diagnostic and rejects it.
        let bytes = [0xDEu8, 0xAD, 0xBE, 0xEF, 0x00, 0x01];
        let result = cqlite_core::parser::header::parse_magic_and_version(&bytes);
        assert!(
            result.is_err(),
            "unknown magic number must be rejected by the header parser"
        );
    });

    let captured = events
        .lock()
        .expect("capture buffer mutex must not be poisoned");

    // The corruption diagnostic must have reached the bridge-less subscriber.
    let corruption_event = captured
        .iter()
        .find(|e| e.message.contains("Unknown magic number"));

    assert!(
        corruption_event.is_some(),
        "a bridge-less tracing subscriber MUST receive the corrupt-SSTable-header \
         diagnostic ('Unknown magic number'); on the pre-migration tree it is a \
         `log::` event and is silently dropped. Captured events: {captured:?}"
    );

    // Level must be preserved (this diagnostic is emitted at ERROR).
    let corruption_event = corruption_event.expect("checked Some above");
    assert_eq!(
        corruption_event.level,
        Level::ERROR,
        "the corrupt-header diagnostic must keep its ERROR level after migration"
    );
}

/// Grep-guard: no `log::{warn,info,debug,error,trace}!` event macro may remain
/// anywhere under `cqlite-core/src`.
#[test]
fn no_log_event_macros_remain_in_core_src() {
    let src_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    assert!(
        src_root.is_dir(),
        "cqlite-core/src must exist at {}",
        src_root.display()
    );

    let mut offenders: Vec<String> = Vec::new();
    let mut rust_files = Vec::new();
    collect_rust_files(&src_root, &mut rust_files);
    assert!(
        !rust_files.is_empty(),
        "expected to find .rs files under {}",
        src_root.display()
    );

    for file in &rust_files {
        let contents = match fs::read_to_string(file) {
            Ok(c) => c,
            Err(e) => panic!("failed to read {}: {e}", file.display()),
        };
        for (idx, line) in contents.lines().enumerate() {
            if let Some(macro_name) = line_has_log_event_macro(line) {
                offenders.push(format!(
                    "{}:{}: log::{}! -> {}",
                    file.display(),
                    idx + 1,
                    macro_name,
                    line.trim()
                ));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "found {} residual `log::` event macro(s) in cqlite-core/src \
         (migrate them to `tracing::`):\n{}",
        offenders.len(),
        offenders.join("\n")
    );
}

/// Returns the macro name (`warn`/`info`/`debug`/`error`/`trace`) if `line`
/// contains a word-boundary-anchored `log::<macro>!` invocation, else `None`.
///
/// The boundary check rejects identifiers that merely *contain* `log::`, e.g.
/// `catalog::error!` or `dialog::warn!`.
fn line_has_log_event_macro(line: &str) -> Option<&'static str> {
    const MACROS: [&str; 5] = ["warn", "info", "debug", "error", "trace"];
    let bytes = line.as_bytes();
    for macro_name in MACROS {
        let needle = format!("log::{macro_name}!");
        let mut search_from = 0;
        while let Some(rel) = line[search_from..].find(&needle) {
            let start = search_from + rel;
            // Word-boundary: the char immediately before `log` must not be part
            // of an identifier (letter, digit, or underscore).
            let boundary_ok = start == 0 || {
                let prev = bytes[start - 1];
                !(prev.is_ascii_alphanumeric() || prev == b'_')
            };
            if boundary_ok {
                return Some(macro_name);
            }
            search_from = start + needle.len();
        }
    }
    None
}

/// Recursively collect every `.rs` file under `dir`.
fn collect_rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => panic!("failed to read dir {}: {e}", dir.display()),
    };
    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => panic!("failed to read dir entry under {}: {e}", dir.display()),
        };
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, out);
        } else if path.extension().and_then(|s| s.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}
