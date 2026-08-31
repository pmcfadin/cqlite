//! One rule for rendering a UDT as JSON: **its declared fields and nothing else**.
//!
//! # Why this exists (issue #3629, parent class #3504)
//!
//! Two JSON renderers — `cqlite-cli`'s `JSONWriter::value_to_json` and
//! `cqlite-core`'s `impl ToJson for Value` — each used to `insert("_type", …)`
//! into the SAME `serde_json` object that then received the UDT's own declared
//! fields. That makes type identity (our control data) and the user's field names
//! (their data) share ONE channel, which is a defect twice over:
//!
//! * a UDT that DECLARES a field named `_type` — legal CQL via a quoted
//!   identifier — silently OVERWRITES the marker, so the type name is
//!   unrecoverable and the collision is invisible; and
//! * every UDT that declares no such field carries a key Cassandra never wrote,
//!   so the output diverges from `sstabledump` for ordinary data.
//!
//! # The reference rule
//!
//! `crate::util::value_fmt::ValueFormatter::format_udt` (the table/CSV display
//! path) already renders fields only, matching the `sstabledump` golden
//! (`test-data/fixtures/issue_3504/`, whose non-colliding `p` cell dumps as
//! `{"label": …, "real_field": 7}`). This helper is that rule, for JSON.
//!
//! # Why it is generic over the field renderer
//!
//! The two callers deliberately render field VALUES by DIFFERENT rules — 11 arms
//! differ (blobs/uuids/inet as CLI hex vs core base64, timestamps as human
//! strings vs raw integers, maps as `[{key,value}]` vs a Display-keyed object).
//! Converging the whole writer would be wrong, so only the UDT arm is shared and
//! each caller keeps its own field-value renderer.
//!
//! Call sites (both must stay on this helper):
//! * `cqlite-cli/src/output/json.rs` — `JSONWriter::value_to_json`
//! * `cqlite-core/src/query/result.rs` — `impl ToJson for Value`
//!
//! Coverage: `cqlite-cli/tests/issue_3629_cli_udt_json_namespace.rs` and
//! `cqlite-core/tests/issue_3629_core_tojson_udt_namespace.rs` (independent per
//! site, so the two copies cannot silently diverge again).

use serde_json::{Map, Value as JsonValue};

use crate::types::{UdtValue, Value};

/// Render `udt` as a JSON object holding its DECLARED FIELDS AND NOTHING ELSE.
///
/// * Field declaration order is preserved (`serde_json` is built with
///   `preserve_order`).
/// * A field whose value is `None` renders as JSON `null`: a null-valued field is
///   still a declared field, and the null is the user's data.
/// * `udt.type_name` / `udt.keyspace` are deliberately NOT emitted. `--format
///   json` has no metadata channel, and inventing a key inside the field
///   namespace is the defect this helper exists to remove.
///
/// `render_field` renders each present field value by the CALLER's own rules.
pub fn udt_to_json_object<F>(udt: &UdtValue, render_field: F) -> JsonValue
where
    F: Fn(&Value) -> JsonValue,
{
    let mut object = Map::with_capacity(udt.fields.len());
    for field in &udt.fields {
        let rendered = match &field.value {
            Some(value) => render_field(value),
            None => JsonValue::Null,
        };
        object.insert(field.name.clone(), rendered);
    }
    JsonValue::Object(object)
}
