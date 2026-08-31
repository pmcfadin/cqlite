//! Result-row construction for the Node binding.
//!
//! Split out of `value.rs` under the campsite rule (#1116) when #3630's scope
//! statements pushed that file past the 800-line threshold. The split is BY
//! RESPONSIBILITY, not by line count: this module owns turning a decoded row's
//! `HashMap<String, Value>` into a JavaScript object — the once-per-result
//! interned column keys (#1446) and the property-definition write path (#3630) —
//! while `value.rs` keeps CQL VALUE conversion. It sits beside its own mechanism
//! module, [`crate::row_properties`].
//!
//! Everything here moved verbatim; the two `## SCOPE:` statements #3630's lead
//! ruling requires are inside `row_to_object` where the code they describe lives.

use crate::value::{value_to_napi, ConvCtx};
use cqlite_core::types::Value;
use napi::{Env, JsObject, JsString, Result};

/// Reusable, once-per-result column-key structure for row construction.
///
/// Issue #1446: both the interned `JsString` handles and the membership set are
/// built a single time per result set (they depend only on the column list,
/// which is constant across every row) so a wide-table scan pays neither the
/// `O(rows × columns)` string re-interning nor a per-row `HashSet` rebuild.
pub struct ColumnKeys {
    /// `(lookup_name, pre-interned JS key)` in authoritative SELECT order.
    /// `JsString` is a `Copy` handle valid for the enclosing `Env` scope.
    ordered: Vec<(String, JsString)>,
    /// Membership set of the ordered names, for O(1) "is this value covered by
    /// the authoritative column list?" checks in [`row_to_object`].
    known: std::collections::HashSet<String>,
}

/// Intern the SELECT-order column names into a reusable [`ColumnKeys`].
///
/// Called once per result set; the returned structure is borrowed for every row.
pub fn intern_column_keys(env: &Env, names: &[String]) -> Result<ColumnKeys> {
    let ordered = names
        .iter()
        .map(|name| Ok((name.clone(), env.create_string(name)?)))
        .collect::<Result<Vec<_>>>()?;
    let known = names.iter().cloned().collect();
    Ok(ColumnKeys { ordered, known })
}

/// Convert row values to a JavaScript object in authoritative SELECT order.
///
/// Issue #1446: property insertion order equals `columns` order (V8 preserves
/// string-key insertion order), so `Object.keys(row)` matches
/// `columns.map(c => c.name)` — not `HashMap` hash order. `keys` are the
/// once-per-result handles from [`intern_column_keys`], reused across every row.
///
/// Issue #1448: `ctx` carries the napi `Env` plus the per-result-cached
/// `Set`/`Map` constructors, threaded into [`value_to_napi`] so a scan with many
/// collection cells fetches each constructor once per result, not once per cell.
///
/// ## Why columns are DEFINED and not ASSIGNED (issue #3630)
///
/// A column name is DATA — a quoted CQL identifier or a `SELECT ... AS` alias
/// makes any string reachable. This function used to write each name with an
/// ordinary property assignment, and an assignment is a JavaScript `[[Set]]`,
/// which CONSULTS THE PROTOTYPE CHAIN. `Object.prototype` carries exactly one
/// accessor, `__proto__`, so a column of that name reached its inherited SETTER
/// instead of becoming a property. MEASURED on the Cassandra-5.0.2-written
/// `test-data/fixtures/issue_3630` fixture before this fix: a string-valued
/// `"__proto__"` column VANISHED — absent from `Object.keys`, not an own
/// property, reading back as `Object.prototype` — with no error and no warning
/// anywhere. Every row of every result set goes through here, so the blast
/// radius was larger than #3504's UDT field bag, which only rows carrying a UDT
/// cell reached.
///
/// `napi_define_properties` performs `[[DefineOwnProperty]]`, which does not
/// consult the prototype at all, so the channel is REMOVED rather than filtered.
/// Deliberately NOT a special case on the literal string `__proto__`: that is
/// picking a rarer delimiter, and it would leave every other inherited name —
/// including any a future JavaScript adds to `Object.prototype` — able to
/// intercept a declared column. The fixture carries `"constructor"` and
/// `"toString"` precisely to distinguish the two: a literal-name check passes
/// every `__proto__` case and is indistinguishable from a real fix until
/// something asks about a different name.
///
/// ## Why a row KEEPS `Object.prototype`, while #3504's field bag does not
///
/// [`udt_to_object`] rejects this same mechanism for the field bag (see its doc
/// comment) because own-property definition leaves `'toString' in fields` true
/// and `fields.constructor` truthy, so an absence probe on the bag still reads
/// inherited junk. That reasoning is SOUND and is not overturned here — it does
/// not TRANSFER, because the axis is what the surface can be probed AGAINST:
///
/// * A row arrives beside `result.columns`, the authoritative SELECT column
///   list, and `Object.hasOwn(row, name)` answers absence exactly. The
///   inherited-junk cost is one the caller has a better instrument than.
/// * A UDT field bag arrives with NO declared key list — the fields are all
///   there is — so the object IS its only absence instrument, and one that
///   answers `in` for names it does not hold cannot express absence at all.
///
/// Rows are also a DOCUMENTED plain-object surface (`lib/index.d.ts`:
/// `interface Row { [column: string]: Value }`), whose consumers call
/// `row.hasOwnProperty(...)`, spread rows, and hand them to code expecting a
/// normal prototype. `Object.create(null)` would break all of that on EVERY row
/// of EVERY query to fix a name almost no schema uses. So: same tradeoff,
/// refused there, accepted here, on a structural difference rather than a
/// preference.
///
/// ACCEPTED COST, stated: `'toString' in row` stays true and `row.constructor`
/// stays truthy. `Object.hasOwn(row, name)` is the correct absence probe and
/// `lib/index.d.ts` says so. One residual is outside this function's reach:
/// `Object.assign(target, row)` performs `[[Set]]` on `target`, so a caller who
/// copies a row into a fresh `{}` re-loses a `__proto__` column. `{...row}` and
/// `Object.fromEntries(Object.entries(row))` do not, because both DEFINE.
pub fn row_to_object(
    ctx: &ConvCtx,
    keys: &ColumnKeys,
    values: &std::collections::HashMap<String, Value>,
) -> Result<JsObject> {
    let env = ctx.env();
    let mut obj = env.create_object()?;
    // Every column is DEFINED, never ASSIGNED — see the `## Why columns are
    // DEFINED` section on this function. Descriptors are accumulated and applied
    // in ONE `define_properties` call so V8 sees them in this exact order, which
    // is what preserves #1446's `Object.keys(row)` ordering contract.
    let mut descriptors = crate::row_properties::RowProperties::with_capacity(keys.ordered.len());
    // Emit the selected columns that are present in this row's values, in
    // authoritative SELECT order (#1446). For the normal case where metadata
    // names match the value keys, this is every column, so `Object.keys(row)`
    // equals `columns.map(c => c.name)`. A metadata column with no matching value
    // is skipped (not null-filled): for aggregate queries core's metadata uses a
    // fallback name like `col_0` while the value is keyed by the expression name
    // like `Count(*)`, and null-filling would emit a phantom `col_0: null`
    // alongside the real cell.
    for (col_name, js_key) in &keys.ordered {
        if let Some(value) = values.get(col_name) {
            let js_value = value_to_napi(ctx, value)?;
            // The interned `JsString` from `intern_column_keys` is reused as the
            // descriptor's name (#1446), which is the whole reason this uses the
            // raw descriptor form — see `row_properties`.
            descriptors.push(js_key, &js_value);
        }
    }
    // Never drop cells (#1446 roborev): emit any values the authoritative column
    // list does not cover — an aggregate value keyed differently from its
    // metadata name, or a streaming `SELECT *` whose schema lookup failed leaves
    // `metadata.columns` empty while rows are still yielded — in a deterministic
    // (name-sorted) order rather than dropping them or using nondeterministic
    // hash order. Extras are detected by membership against the precomputed
    // `known` set (built once per result), so the common path where metadata
    // covers every cell allocates no set and does no sort.
    let mut extra: Vec<&String> = values
        .keys()
        .filter(|name| !keys.known.contains(name.as_str()))
        .collect();
    if !extra.is_empty() {
        extra.sort();
        for name in extra {
            if let Some(value) = values.get(name) {
                let js_value = value_to_napi(ctx, value)?;
                // SAME mechanism as the interned path above. Both paths write a
                // user-controlled name, so a fix reaching only one leaves the
                // other live — #3504 found exactly that duplication one file
                // over, and #3630's spec makes them ONE requirement.
                //
                // No interned handle exists here (extras are keyed by the value
                // map, not by the column list), so the name is created per row.
                // That is the rare branch and it already sorts, so the cost is
                // not on the hot path the #1446 interning protects.
                //
                // ## SCOPE: this branch is UNREACHABLE from the Node public
                // ## surface today — MEASURED, not un-attempted (issue #3630)
                //
                // This is a scope statement, not a TODO. Nobody has "not got
                // round to" a test here: reaching this branch requires a value
                // key that `metadata.columns` does not cover, and five routes
                // were measured through `Database.executeNative`, none of which
                // produces one:
                //
                //   * `SELECT *` + full schema  -> metadata covers all columns
                //   * `SELECT id, "__proto__"`  -> the quoted name DEGRADES to
                //                                  `col_1` in metadata AND the
                //                                  value is keyed `col_1` too
                //   * `SELECT "__proto__"`      -> same, `col_0`
                //   * `SELECT COUNT(*)`         -> metadata `Count(*)`, value
                //                                  keyed identically
                //   * schemaless open           -> metadata still complete, from
                //                                  Statistics.db's authoritative
                //                                  serialization header
                //   * partial schema            -> values are PROJECTED to the
                //                                  declared columns
                //
                // The mechanism is shared with the interned path above (both go
                // through `RowProperties`), so a divergence between them is
                // structurally impossible while that holds — but a SHARED HELPER
                // IS NOT COVERAGE: this branch's own line is executed by no test,
                // so inlining or special-casing it would break silently.
                //
                // TRIGGER that makes this testable — check this before assuming
                // it is still unreachable: any query path that yields a value key
                // absent from `result.columns`. Concretely, if the aggregate path
                // stops keying values by their metadata name, or if a projection
                // stops filtering values to the declared column set. Tracked in
                // the #3630 follow-up. Lead ruling on `coord-request: 3630-R2`
                // batched this rather than adding a test-only public entry point:
                // a coverage gap is a known unknown, a test-only surface is a
                // permanent compatibility obligation.
                let js_name = env.create_string(name)?;
                descriptors.push(&js_name, &js_value);
            }
        }
    }
    descriptors.define_on(env, &mut obj)?;
    Ok(obj)
}
