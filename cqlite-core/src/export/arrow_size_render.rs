//! Rendered-representation bounds for the Arrow payload-byte estimator
//! (issue #2825).
//!
//! Columns with no authoritative CQL type (and the `Tuple`/`Udt`/`Frozen`/
//! `Tombstone`/`Null` flat arms) are converted by `build_string_array`, which
//! renders the value through `ValueFormatter::format_value`; the flat
//! `build_list_array` / `build_map_array` render each ELEMENT the same way. The
//! constants here each bound one `format_value` arm, and
//! [`Estimator::charge_rendered`](super::Estimator::charge_rendered) walks a
//! value against them.
//!
//! Split out of `arrow_size.rs` so both files stay under the campsite threshold
//! (epic #1116). Declared as a CHILD module of `arrow_size`, so it can see that
//! module's private [`Estimator`](super::Estimator) and [`Shape`](super::Shape).

use super::{Estimator, Shape};
use crate::types::Value;

/// `"true"` / `"false"`.
pub(super) const RENDER_BOOL_BYTES: usize = 8;
/// Any integral variant rendered as decimal (`i64::MIN` is 20 chars).
pub(super) const RENDER_INT_BYTES: usize = 24;
/// `f32`/`f64` via `{}`/`{:e}`, plus `NaN`/`Infinity`.
pub(super) const RENDER_FLOAT_BYTES: usize = 40;
/// `"a8f167f0-ebe7-4f20-a386-31ff138bec3b"`.
pub(super) const RENDER_UUID_BYTES: usize = 40;
/// `YYYY-MM-DD HH:MM:SS.fff+0000` or `<invalid-timestamp:-9223372036854775808>`.
pub(super) const RENDER_TIMESTAMP_BYTES: usize = 48;
/// `YYYY-MM-DD` or the `<invalid-date:…>` fallback.
pub(super) const RENDER_DATE_BYTES: usize = 48;
/// `HH:MM:SS.nnnnnnnnn` or the `<invalid-time:…>` fallback.
pub(super) const RENDER_TIME_BYTES: usize = 48;
/// Full IPv6 text form, or the invalid-length fallback.
pub(super) const RENDER_INET_BYTES: usize = 64;
/// `"{months}mo{days}d{nanos}ns"` at the widest.
pub(super) const RENDER_DURATION_BYTES: usize = 64;
/// `"<deleted@{i64}>"`.
pub(super) const RENDER_TOMBSTONE_BYTES: usize = 40;
/// `"null"`.
pub(super) const RENDER_NULL_BYTES: usize = 8;
/// The brackets/braces a rendered container puts AROUND its children.
pub(super) const RENDER_CONTAINER_BYTES: usize = 8;
/// The `", "` (or `": "`) a rendered container puts BETWEEN its children —
/// charged per child, because the children themselves are charged separately.
pub(super) const RENDER_SEPARATOR_BYTES: usize = 2;
/// Decimal digits produced per magnitude byte (`log10(256) < 2.41`), rounded up.
pub(super) const DECIMAL_DIGITS_PER_BYTE: usize = 3;
/// `ValueFormatter::format_decimal`'s zero-padding ceiling: past this the render
/// switches to bounded exponent form, so padding can never exceed it.
pub(super) const DECIMAL_SCALE_RENDER_CAP: usize = 1_000_001;
/// Worst-case UTF-8 expansion of one input byte under `String::from_utf8_lossy`:
/// an invalid byte becomes U+FFFD, three bytes.
pub(super) const LOSSY_UTF8_EXPANSION: usize = 3;
/// Worst-case JSON escape of one input byte: `\u00XX`, six characters.
pub(super) const JSON_ESCAPE_EXPANSION: usize = 6;

impl<'a> Estimator<'a> {
    /// Charge the `Utf8` rendering of `value` via `ValueFormatter::format_value`.
    ///
    /// Content only — the caller has already charged whatever slot overhead the
    /// rendering lands in. Leaf variants get a constant bound; container
    /// variants charge their bracket/separator overhead here and push their
    /// children as further INLINE rendered nodes (they are sub-parts of this one
    /// string, not array slots of their own).
    pub(super) fn charge_rendered(&mut self, value: Option<&'a Value>) {
        let Some(value) = value else {
            return;
        };
        match super::unwrap_frozen_value(value) {
            Value::Null => self.add(RENDER_NULL_BYTES),
            // EMPTY-BUFFER SENTINEL (issue #3805): renders as the EMPTY STRING
            // (`ValueFormatter::format_value` → `""`, matching
            // `sstabledump`'s `"path" : [ "" ]`), so it contributes ZERO
            // rendered content bytes. Charging 0 is exact here, not optimistic:
            // the rendering has no length that can vary.
            Value::Empty(_) => {}
            Value::Boolean(_) => self.add(RENDER_BOOL_BYTES),
            Value::TinyInt(_)
            | Value::SmallInt(_)
            | Value::Integer(_)
            | Value::BigInt(_)
            | Value::Counter(_) => self.add(RENDER_INT_BYTES),
            Value::Float(_) | Value::Float32(_) => self.add(RENDER_FLOAT_BYTES),
            // LOSSY path: `ValueFormatter::format_value` renders text with
            // `String::from_utf8_lossy`, which expands EACH invalid byte to a
            // 3-byte U+FFFD. `Value::Text` is UTF-8-validated at construction
            // (issue #1644), which would make `s.len()` exact — but that
            // invariant is not enforced by the type, and a `Value::Text` nested
            // inside a rendered container reaches this arm without passing the
            // strict builders' `str::from_utf8` check. Charge the lossy worst
            // case (review B5); the STRICT typed arm keeps `s.len()` because its
            // builder hard-errors on invalid UTF-8 instead of expanding it.
            Value::Text(s) => self.add(s.len().saturating_mul(LOSSY_UTF8_EXPANSION)),
            // `0x`-prefixed lowercase hex: two chars per byte.
            Value::Blob(b) => self.add(
                b.len()
                    .saturating_mul(2)
                    .saturating_add(RENDER_CONTAINER_BYTES),
            ),
            Value::Timestamp(_) => self.add(RENDER_TIMESTAMP_BYTES),
            Value::Date(_) => self.add(RENDER_DATE_BYTES),
            Value::Time(_) => self.add(RENDER_TIME_BYTES),
            Value::Uuid(_) => self.add(RENDER_UUID_BYTES),
            Value::Varint(b) => self.add(
                b.len()
                    .saturating_mul(DECIMAL_DIGITS_PER_BYTE)
                    .saturating_add(RENDER_CONTAINER_BYTES),
            ),
            // Digits, plus `format_decimal`'s bounded zero padding (past
            // `DECIMAL_SCALE_RENDER_CAP` it switches to exponent form).
            Value::Decimal { scale, unscaled } => self.add(
                unscaled
                    .len()
                    .saturating_mul(DECIMAL_DIGITS_PER_BYTE)
                    .saturating_add((scale.unsigned_abs() as usize).min(DECIMAL_SCALE_RENDER_CAP))
                    .saturating_add(RENDER_CONTAINER_BYTES),
            ),
            Value::Duration { .. } => self.add(RENDER_DURATION_BYTES),
            Value::Inet(_) => self.add(RENDER_INET_BYTES),
            Value::Tombstone(_) => self.add(RENDER_TOMBSTONE_BYTES),
            Value::Json(json) => {
                let bytes = json_render_bytes(json, &mut self.budget);
                self.add(bytes);
            }
            Value::List(items) | Value::Set(items) | Value::Tuple(items) => {
                self.add(
                    RENDER_CONTAINER_BYTES
                        .saturating_add(items.len().saturating_mul(RENDER_SEPARATOR_BYTES)),
                );
                self.charge_children(items.iter().map(|v| (Shape::RenderedInline, Some(v))));
            }
            Value::Map(pairs) => {
                self.add(
                    RENDER_CONTAINER_BYTES.saturating_add(
                        pairs
                            .len()
                            .saturating_mul(2)
                            .saturating_mul(RENDER_SEPARATOR_BYTES),
                    ),
                );
                self.charge_children(pairs.iter().flat_map(|(k, v)| {
                    [
                        (Shape::RenderedInline, Some(k)),
                        (Shape::RenderedInline, Some(v)),
                    ]
                }));
            }
            Value::Udt(udt) => {
                self.add(RENDER_CONTAINER_BYTES);
                for f in &udt.fields {
                    if self.total == usize::MAX {
                        return;
                    }
                    // `format_udt` emits `name: value` pairs — the name and both
                    // separators are charged here, the value as a child node.
                    self.add(f.name.len().saturating_add(RENDER_CONTAINER_BYTES));
                    self.charge_child(Shape::RenderedInline, f.value.as_ref());
                }
            }
            // Unreachable after `unwrap_frozen_value`; kept so the match is
            // exhaustive without a `_` arm.
            Value::Frozen(_) => self.add(RENDER_CONTAINER_BYTES),
        }
    }
}

/// Bounded upper bound on `serde_json::Value::to_string().len()`.
///
/// Iterative with its own share of the caller's node budget, so a deeply nested
/// JSON document cannot recurse or spin. Returns `usize::MAX` when the budget is
/// exhausted (fail closed).
pub(super) fn json_render_bytes(root: &serde_json::Value, budget: &mut usize) -> usize {
    let mut total = 0usize;
    let mut stack = vec![root];
    while let Some(node) = stack.pop() {
        if *budget == 0 {
            return usize::MAX;
        }
        *budget -= 1;
        match node {
            serde_json::Value::Null => total = total.saturating_add(RENDER_NULL_BYTES),
            serde_json::Value::Bool(_) => total = total.saturating_add(RENDER_BOOL_BYTES),
            serde_json::Value::Number(_) => total = total.saturating_add(RENDER_FLOAT_BYTES),
            // JSON string escaping can expand a byte to `\u00XX` (6 chars).
            serde_json::Value::String(s) => {
                total = total.saturating_add(
                    s.len()
                        .saturating_mul(JSON_ESCAPE_EXPANSION)
                        .saturating_add(RENDER_CONTAINER_BYTES),
                )
            }
            serde_json::Value::Array(items) => {
                total = total.saturating_add(
                    RENDER_CONTAINER_BYTES
                        .saturating_add(items.len().saturating_mul(RENDER_SEPARATOR_BYTES)),
                );
                if items.len() > *budget {
                    return usize::MAX;
                }
                stack.extend(items.iter());
            }
            serde_json::Value::Object(map) => {
                total = total.saturating_add(RENDER_CONTAINER_BYTES);
                if map.len() > *budget {
                    return usize::MAX;
                }
                for (k, v) in map {
                    total = total.saturating_add(
                        k.len()
                            .saturating_mul(JSON_ESCAPE_EXPANSION)
                            .saturating_add(RENDER_CONTAINER_BYTES),
                    );
                    stack.push(v);
                }
            }
        }
    }
    total
}
