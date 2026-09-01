//! Issue #3612 (roborev round 8, finding 2) — the CARDINALITY of the
//! undecodable-map-key diagnostic.
//!
//! ## The property under test
//! R8-F2 moved this diagnostic from a `warn!` inside the per-ENTRY cell-path key
//! decoder to an aggregate in `complex_column`'s map branch: the decoder now only
//! sets `opaque_out`, and the caller emits AT MOST ONE line per column per ROW,
//! carrying `affected_entries`. That is a behaviour change, and until this file it
//! was asserted by nothing — a revert to the per-entry spray would have been
//! silently green, and the module header that described the old shape stayed
//! wrong for two rounds because no test disagreed with it.
//!
//! Three cases, each independently falsifiable:
//! 1. TWO undecodable entries in one column produce exactly ONE record, whose
//!    `affected_entries` is 2 — a per-entry emitter reds on the COUNT (2 records),
//!    and a caller that forgot the counter reds on the FIELD.
//! 2. A decodable key type produces NONE — the diagnostic is conditional.
//! 3. A key DECLARED `blob` produces NONE — `Value::Blob` is the CORRECT decode
//!    there, and warning about it is the misleading-diagnostic half of #3612.
//!
//! ## Why this is a UNIT test, and why it synthesises its bytes
//! The unmodellable-key class CANNOT be reached through a schema-provided public
//! read: the CQL layer rejects an undefined UDT and rejects a quoted-custom or
//! `vector<…>` map key outright, and the no-schema path resolves the key type from
//! the on-disk marshal form, which decodes. That measurement is
//! `cqlite-core/tests/issue_3612_cell_path_key_error_surface.rs`'s, not a
//! convenience assumption here. `parse_complex_column_inner` is also `pub(crate)`,
//! so an integration test could not call it even if a fixture existed.
//!
//! Synthesising the complex-column bytes is sound for THIS property specifically:
//! the subject is a LOGGING cardinality, not an on-disk framing rule, so the
//! symmetric-round-trip blindness of #3042 does not apply — a uniform framing
//! error in the builder below would make the parse fail, not make the assertion
//! pass. Nothing here is evidence about the wire format; the fixture-backed tests
//! own that.
//!
//! ## The capture is a thread-local subscriber, not log scraping
//! `tracing::subscriber::with_default` installs the capturing subscriber for THIS
//! THREAD and this call only, so there is no global state, no `serial_test`
//! requirement, and no interference from tests running in parallel. Records are
//! read from the `tracing` event itself — target, level and structured fields —
//! never from formatted text on a stream.
//!
//! ## Why this module is declared from `cell_path_key.rs`
//! It is a sibling of `cell_path_key_tests.rs` on disk, where it belongs. It is
//! DECLARED from `cell_path_key.rs` (which owns the `opaque_out` signal and states
//! its cardinality contract) because `complex_column.rs` is 2949 lines — far over
//! the file-size ratchet — and adding a line to it would fail the gate's
//! `file-size` component (epic #1116).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::Registry;

use crate::schema::Column;
use crate::storage::sstable::reader::parsing::row_decoder::V5CompressedLegacyParser;
use crate::Value;

/// The target the aggregate diagnostic is emitted on (`complex_column`'s map
/// branch). Every other record this decode path emits is a `tracing::debug!` on
/// the default module-path target, so filtering on this one isolates the subject.
const DECODE_TARGET: &str = "cqlite::decode";
const COLUMN: &str = "m_subject";

// ── the parsed record, captured structurally ────────────────────────────────

#[derive(Debug, Clone)]
struct CapturedEvent {
    level: String,
    target: String,
    fields: BTreeMap<String, String>,
}

impl CapturedEvent {
    fn field(&self, name: &str) -> Option<&str> {
        self.fields.get(name).map(String::as_str)
    }
}

#[derive(Default)]
struct FieldVisitor {
    fields: BTreeMap<String, String>,
}

impl FieldVisitor {
    fn put(&mut self, field: &Field, rendered: String) {
        self.fields.insert(field.name().to_string(), rendered);
    }
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // `%value` (Display) also arrives here, wrapped in a type whose `Debug`
        // forwards to `Display` — so `column`/`declared_type` render unquoted.
        self.put(field, format!("{value:?}"));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.put(field, value.to_string());
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.put(field, value.to_string());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.put(field, value.to_string());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.put(field, value.to_string());
    }
}

struct CapturingLayer {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl<S: Subscriber> Layer<S> for CapturingLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        let meta = event.metadata();
        if let Ok(mut events) = self.events.lock() {
            events.push(CapturedEvent {
                level: meta.level().to_string(),
                target: meta.target().to_string(),
                fields: visitor.fields,
            });
        }
    }
}

/// Run `f` with a THREAD-LOCAL capturing subscriber and return its result
/// alongside every event it emitted. No global subscriber is installed, so
/// parallel tests neither pollute this capture nor are polluted by it.
fn capture<T>(f: impl FnOnce() -> T) -> (T, Vec<CapturedEvent>) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let subscriber = Registry::default().with(CapturingLayer {
        events: Arc::clone(&events),
    });
    let out = tracing::subscriber::with_default(subscriber, f);
    let captured = events.lock().map(|v| v.clone()).unwrap_or_default();
    (out, captured)
}

// ── the subject: a multicell map column, one row ────────────────────────────

fn parser() -> V5CompressedLegacyParser {
    V5CompressedLegacyParser::new("test_ks".to_string(), "t".to_string(), 0, 0, None)
}

fn column(data_type: &str) -> Column {
    Column {
        name: COLUMN.to_string(),
        data_type: data_type.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

/// The on-disk bytes of ONE multicell map column with no complex deletion:
/// `[cell count][cell]…`, each cell `[flags][cell-path len][path][value len][value]`.
///
/// `0x08` is `USE_ROW_TIMESTAMP`, which is what lets a cell carry no timestamp,
/// no localDeletionTime and no TTL — the shortest legal cell, and enough for a
/// diagnostic-cardinality subject. Counts and lengths stay below 128 so each
/// unsigned VInt is its own single byte.
fn multicell_map_bytes(entries: &[(&[u8], i32)]) -> Vec<u8> {
    assert!(entries.len() < 128, "keep the cell count a 1-byte VInt");
    let mut out = vec![entries.len() as u8];
    for (path, value) in entries {
        assert!(path.len() < 128, "keep the cell-path length a 1-byte VInt");
        out.push(0x08);
        out.push(path.len() as u8);
        out.extend_from_slice(path);
        out.push(4);
        out.extend_from_slice(&value.to_be_bytes());
    }
    out
}

/// Decode one multicell map column and return its value plus the records emitted
/// on [`DECODE_TARGET`].
fn decode_capturing_diagnostics(
    data_type: &str,
    entries: &[(&[u8], i32)],
) -> (Value, Vec<CapturedEvent>) {
    let parser = parser();
    let col = column(data_type);
    let bytes = multicell_map_bytes(entries);
    let (parsed, events) = capture(|| {
        parser.parse_complex_column_inner(&bytes, 0, &col, data_type, false, 0, None, None)
    });
    let (value, _consumed, _meta) =
        parsed.unwrap_or_else(|e| panic!("the synthesized {data_type} column must parse: {e}"));
    let on_target = events
        .into_iter()
        .filter(|e| e.target == DECODE_TARGET)
        .collect();
    (value, on_target)
}

// ════════════════════════════════════════════════════════════════════════════

/// R8-F2's property: TWO undecodable keys in one column emit exactly ONE record,
/// carrying the COUNT. The pre-R8-F2 per-entry `warn!` emits two and carries no
/// count, so this reds on a revert in two independent ways.
#[test]
fn two_undecodable_map_keys_emit_one_aggregated_record_carrying_the_count() {
    // `mystery_type` is modelled by no decoder and resolves through no registry
    // (this parser has none), so the shared decoder returns the raw bytes.
    let (value, records) =
        decode_capturing_diagnostics("map<mystery_type, int>", &[(b"k1a", 1), (b"k2b", 2)]);

    assert_eq!(
        records.len(),
        1,
        "the undecodable-key diagnostic is ONE line per column per row, not one \
         per entry (issue #3612, R8-F2); got {records:#?}"
    );
    let record = &records[0];
    assert_eq!(
        record.level, "WARN",
        "the aggregate is a warning: {record:#?}"
    );
    assert_eq!(
        record.field("affected_entries"),
        Some("2"),
        "the record must carry HOW MANY entries were affected — the number an \
         operator needs, and the one a per-entry spray destroys: {record:#?}"
    );
    assert_eq!(
        record.field("total_entries"),
        Some("2"),
        "…alongside the column's entry count, so the affected fraction is \
         readable: {record:#?}"
    );
    assert_eq!(
        record.field("column"),
        Some(COLUMN),
        "the record must name the column: {record:#?}"
    );
    assert_eq!(
        record.field("declared_type"),
        Some("mystery_type"),
        "…and the declared key type it could not model: {record:#?}"
    );

    // The row stays WHOLE: an unmodellable key is reported, never an `Err` (an
    // `Err` is swallowed by row assembly into a truncated row — see
    // `cell_path_key.rs`'s error-budget rule).
    let Value::Map(entries) = value else {
        panic!("a multicell map column decodes to Value::Map; got {value:?}");
    };
    assert_eq!(entries.len(), 2, "both entries survive as opaque keys");
    for (key, _) in &entries {
        assert!(
            matches!(key, Value::Blob(_)),
            "an unmodellable key surfaces as opaque bytes; got {key:?}"
        );
    }
}

/// The control: a key type the reader DOES model emits nothing. Without this, a
/// diagnostic that fired unconditionally would satisfy the test above.
#[test]
fn a_decodable_map_key_type_emits_no_diagnostic() {
    let (value, records) = decode_capturing_diagnostics(
        "map<int, int>",
        &[(&7i32.to_be_bytes(), 1), (&9i32.to_be_bytes(), 2)],
    );
    assert!(
        records.is_empty(),
        "a decodable key type must emit no undecodable-key record; got {records:#?}"
    );
    let Value::Map(entries) = value else {
        panic!("a multicell map column decodes to Value::Map; got {value:?}");
    };
    assert_eq!(
        entries.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
        vec![Value::Integer(7), Value::Integer(9)],
        "the control must really have decoded its keys, or it proves nothing"
    );
}

/// The misleading-diagnostic half of #3612: a key DECLARED `blob` decodes
/// CORRECTLY to `Value::Blob`, so it must stay silent even though the decode
/// result is indistinguishable from the undecodable case.
#[test]
fn a_declared_blob_map_key_emits_no_diagnostic() {
    let (value, records) = decode_capturing_diagnostics("map<blob, int>", &[(b"k1a", 1)]);
    assert!(
        records.is_empty(),
        "a DECLARED blob key is a correct decode and must not be reported as \
         undecodable; got {records:#?}"
    );
    let Value::Map(entries) = value else {
        panic!("a multicell map column decodes to Value::Map; got {value:?}");
    };
    assert_eq!(entries.len(), 1);
    assert!(matches!(&entries[0].0, Value::Blob(_)));
}
