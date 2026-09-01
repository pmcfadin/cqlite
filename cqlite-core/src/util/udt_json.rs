//! One rule for rendering a UDT as JSON: **its declared fields and nothing else**.
//!
//! # Why this exists (issue #3629, parent class #3504)
//!
//! Two JSON renderers — `cqlite-cli`'s per-cell JSON rendering (then
//! `JSONWriter::value_to_json`, now `JsonCell::from_value`) and `cqlite-core`'s
//! `impl ToJson for Value` — each used to `insert("_type", …)`
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
//! # The reference rule, and its PRIMARY SOURCE
//!
//! Authority is Cassandra, read at the pinned tag — not CQLite's own code:
//! `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/UserType.java:261`
//! (`toJSONString`) builds `{"<field>": <value>, …}` by iterating
//! `for (int i = 0; i < types.size(); i++)` over `stringFieldNames` ALONE. It
//! emits **no type key and no keyspace key**, and where a field's buffer is
//! absent it appends the literal `null` (line 280). So "declared fields, nothing
//! else, `null` for an absent one" is Cassandra's rule, not a CQLite convention.
//!
//! Corroborated by `sstabledump` on the committed Cassandra-written fixture
//! `test-data/fixtures/issue_3504/`, whose non-colliding `p` cell dumps as
//! `{"label": …, "real_field": 7}` — and whose `id 3` `c` cell dumps
//! `"_type": null`, which line 280 says is the CORRECT rendering of a null
//! FIELD and not a residue of the removed marker.
//!
//! `crate::util::value_fmt::ValueFormatter::format_udt` (the table/CSV display
//! path) already followed that rule. This helper is the same rule, for JSON.
//!
//! # Why it is generic over the field renderer
//!
//! The two callers deliberately render field VALUES by DIFFERENT rules — 11 arms
//! differ (blobs/uuids/inet as CLI hex vs core base64, timestamps as human
//! strings vs raw integers, maps as `[{key,value}]` vs a Display-keyed object).
//! `decimal` and `varint` are two of those 11 and were divergent BEFORE #3644 (a
//! quoted display string against core's `{scale, unscaled: <base64>}` and a
//! base64 string); #3644 changed the KIND of the CLI side — an unquoted JSON
//! number — without changing the count. Converging the whole writer would be
//! wrong, so only the UDT arm is shared and each caller keeps its own field-value
//! renderer.
//!
//! Call sites (both must stay on this rule):
//! * `cqlite-cli/src/output/json_cell.rs` — `JsonCell::from_value`, through
//!   [`udt_render_fields`] (its cell type is not a `serde_json::Value`, so it
//!   shares the RULE rather than the return type)
//! * `cqlite-core/src/query/result.rs` — `impl ToJson for Value`, through
//!   [`udt_to_json_object`], which is itself a thin adapter over
//!   [`udt_render_fields`]
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
    udt_render_fields(
        udt,
        render_field,
        || JsonValue::Null,
        |name, rendered: JsonValue| {
            object.insert(name.to_string(), rendered);
        },
    );
    JsonValue::Object(object)
}

/// The SAME rule as [`udt_to_json_object`], for a caller whose rendered field is
/// not a [`JsonValue`].
///
/// `cqlite-cli`'s JSON egress renders a `decimal`/`varint` as a RAW JSON number
/// fragment, which `serde_json::Value` cannot hold without `arbitrary_precision`
/// (see `cqlite-cli/src/output/json_cell.rs`), so its field renderer produces its
/// own cell type. Sharing the rule rather than the return type is what keeps
/// "declared fields, nothing else, `null` for an absent one" in ONE place: this
/// function IS the rule and [`udt_to_json_object`] is a thin adapter over it.
///
/// `absent` BUILDS the caller's spelling of JSON `null`, emitted for a field
/// whose value is `None` — a null-valued field is still a declared field. It is a
/// constructor rather than a value so the cell type need not be `Clone`
/// (`cqlite-cli`'s carries a `Box`).
pub fn udt_render_fields<T, F, A, E>(udt: &UdtValue, render_field: F, absent: A, mut emit: E)
where
    F: Fn(&Value) -> T,
    A: Fn() -> T,
    E: FnMut(&str, T),
{
    for field in &udt.fields {
        let rendered = match &field.value {
            Some(value) => render_field(value),
            None => absent(),
        };
        emit(&field.name, rendered);
    }
}
