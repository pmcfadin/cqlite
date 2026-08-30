//! Comparing ONE user-defined-type value, golden vs CLI (issue #1491).
//!
//! Split out of `golden_value_compare.rs` under the campsite rule (CLAUDE.md,
//! epic #1135), which had reached the ~1500-line test-file target. A UDT is the
//! one value shape whose FIELD SET is fixed by a second committed DDL statement
//! (`CREATE TYPE`, not `CREATE TABLE`), so it is the only place the comparison
//! asks the schema which members must exist — a distinct question from the
//! structural walk in the parent module.
//!
//! Reached only through [`super::compare_value_at`], and every helper it uses
//! (`At`, `pair`, `describe`, `brief`) still lives in the parent, so this file
//! adds no surface: the call site is unchanged.

use super::super::schema::UdtType;
use super::{brief, compare_value_at, describe, pair, At, Egress, Kinding};
use serde_json::{Map, Value};

/// How each egress format spells a UDT, for the diagnostic.
fn udt_spelling(egress: Egress) -> &'static str {
    match egress {
        Egress::Json => "a field→value JSON object",
        Egress::Csv => "a `{key,value}` list decoded from the flat `{k: v, …}` field",
    }
}

/// A UDT: always a field→value object in the dump. On the CLI side the accepted
/// representation is FORMAT-SCOPED, because each format has exactly one:
///
///   * **JSON** — a field→value object, plus a `_type` discriminator the CLI adds
///     and the golden does not carry. It is REQUIRED to be present, to be a
///     string, and to name the type the committed `CREATE TYPE` declares (folded
///     for case, because an unquoted CQL identifier is case-insensitive); only
///     then is it dropped from the field set, and only from the CLI side. It used
///     to be stripped unconditionally, so a missing or wrongly-named
///     discriminator passed even though the DDL knows the answer (issue #1491
///     review finding R3). A `{key,value}` pair array is the CLI's *map*
///     spelling, so accepting one here would let a UDT that regressed to the map
///     representation pass; it is therefore rejected (review finding F3).
///   * **CSV** — a `{key,value}` list, and only that. CSV delivers the whole cell
///     as one flat `{k: v, …}` text carrying nothing that could distinguish a map
///     from a UDT, so [`super::csv_container`] decodes EVERY brace-delimited body
///     into the pair spelling. An object on this side would mean the decoder was
///     bypassed.
///
/// Field NAMES must agree between the two sides, and each name must be one the
/// `CREATE TYPE` declares — an undeclared field name has no declared type, and a
/// value with no declared type is never compared permissively.
pub(super) fn compare_udt(
    golden: &Map<String, Value>,
    cli: &Value,
    egress: Egress,
    udt: &UdtType,
    at: &At<'_, '_>,
) -> Result<(), String> {
    let c: Map<String, Value> = match (egress, cli) {
        (Egress::Json, Value::Object(fields)) => {
            check_udt_discriminator(fields, udt)?;
            fields
                .iter()
                .filter(|(k, _)| k.as_str() != DISCRIMINATOR)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        }
        (Egress::Csv, Value::Array(entries)) => {
            let mut out = Map::new();
            for entry in entries {
                let (key, value) = pair(entry, egress)?;
                let Value::String(name) = key else {
                    return Err(format!(
                        "udt `{}`: decoded field name {} is not a string",
                        udt.name,
                        brief(&describe(key, egress))
                    ));
                };
                // A repeated field name USED to overwrite, so an egress carrying an
                // extra spurious field compared equal to the golden whenever the
                // LAST occurrence happened to match (issue #1491 review finding J2).
                // Duplicate egress is malformed, not something to reconcile.
                if let Some(previous) = out.insert(name.clone(), value.clone()) {
                    return Err(format!(
                        "udt `{}`: the {egress:?} egress repeats the field `{name}` ({} then \
                         {}) — a duplicate field is malformed output, and comparing only the \
                         last occurrence would hide the earlier one",
                        udt.name,
                        brief(&describe(&previous, egress)),
                        brief(&describe(value, egress))
                    ));
                }
            }
            out
        }
        (_, other) => {
            return Err(format!(
                "the schema declares the UDT `{}`, but the {egress:?} egress value {} is not \
                 {}",
                udt.name,
                brief(&describe(other, egress)),
                udt_spelling(egress)
            ))
        }
    };
    // THE DDL IS THE AUTHORITY FOR WHICH FIELDS MUST EXIST, asked of EACH SIDE
    // SEPARATELY and BEFORE the two sides are compared with each other.
    //
    // Side-vs-side agreement alone accepted an INCOMPLETE value: a field the
    // committed `CREATE TYPE` declares and NEITHER side emitted was absent from
    // both `missing` and `extra`, the order check below filters by
    // `contains_key`, and the golden-field iteration never visits it — so the
    // whole comparison passed over a field nobody rendered (issue #1491 review
    // finding W1). It is the same rule this file already applies one level up to a
    // row's COLUMNS: an exclusion excludes a VALUE, never a position's PRESENCE
    // (see [`SkipPaths`]), and a missing declared column is a failure whatever the
    // golden happens to hold.
    //
    // NEITHER SIDE MAY DEFAULT HERE, and that is the one place this differs from
    // the row level. A row's golden legitimately omits a never-written CELL, so
    // there the golden side is allowed to be short. A frozen UDT's fields are not
    // cells: they live inside ONE value cell, and `cassandra-5.0.8`
    // `UserType.toJSONString` writes `for (int i = 0; i < types.size(); i++)` over
    // the DECLARED type list, emitting `null` for a field whose buffer is absent
    // (`valueBuffer == null` => `sb.append("null")`). So a dump of a frozen UDT
    // carries EVERY declared field, always — measured on the committed goldens
    // too: `udt_frozen_person` renders `"last_name":null` rather than dropping the
    // field, and so does `udt_null_inner`'s `"city":null` nested inside a frozen
    // collection. A golden short of a declared field therefore means the committed
    // DDL does not describe the type the dump was taken under, which is a fault to
    // report and not a gap to tolerate.
    //
    // Scoped to what `compare_udt` actually sees, which is the frozen (single-cell)
    // spelling: a golden `Value::Object` for the whole UDT. A NON-frozen UDT is
    // dumped as one cell PER FIELD with a `CellPath`, never as one object, so it
    // does not reach here — every UDT case in this lane declares `multicell: &[]`.
    for (side, fields) in [("golden", golden), ("cli", &c)] {
        let absent: Vec<&str> = udt
            .fields
            .iter()
            .map(|(name, _)| name.as_str())
            .filter(|name| !fields.contains_key(*name))
            .collect();
        if !absent.is_empty() {
            return Err(format!(
                "udt `{}` at `{}`: the {side} side emits none of the declared field(s) \
                 {absent:?} — the committed CREATE TYPE declares {:?}, and \
                 cassandra-5.0.8 UserType.toJSONString emits every declared field \
                 (`null` when its value is absent), so a declared field that is \
                 rendered by NEITHER side is a missing field and not an agreement",
                udt.name,
                at.path,
                udt.fields
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>()
            ));
        }
    }
    let mut missing: Vec<&String> = golden.keys().filter(|k| !c.contains_key(*k)).collect();
    let mut extra: Vec<&String> = c.keys().filter(|k| !golden.contains_key(*k)).collect();
    missing.sort();
    extra.sort();
    if !missing.is_empty() || !extra.is_empty() {
        return Err(format!(
            "udt `{}` fields differ: absent from cli {missing:?}, absent from golden {extra:?}",
            udt.name
        ));
    }
    // FIELD ORDER, from the committed `CREATE TYPE`. `cassandra-5.0.8`
    // `UserType.toJSONString` writes `for (int i = 0; i < types.size(); i++)` over
    // `stringFieldNames`, so a UDT's fields are emitted in DECLARATION order (and
    // every declared field is emitted, `null` when absent). Both sides render the
    // same value, so both must be in that order — the same rule `compare_map`
    // applies to a map's entries (finding N2), stated here against the DDL rather
    // than against either side.
    let declared: Vec<&str> = udt.fields.iter().map(|(name, _)| name.as_str()).collect();
    for (side, fields) in [("golden", golden), ("cli", &c)] {
        let emitted: Vec<&str> = fields.keys().map(String::as_str).collect();
        let expected: Vec<&str> = declared
            .iter()
            .copied()
            .filter(|name| fields.contains_key(*name))
            .collect();
        if emitted != expected {
            return Err(format!(
                "udt `{}`: the {side} field order is {emitted:?}, but the committed \
                 CREATE TYPE declares {expected:?} — cassandra-5.0.8 \
                 UserType.toJSONString emits a UDT's fields in declaration order",
                udt.name
            ));
        }
    }
    for (field, gv) in golden {
        let field_ty = udt
            .fields
            .iter()
            .find(|(name, _)| name == field)
            .map(|(_, t)| t)
            .ok_or_else(|| {
                format!(
                    "udt `{}` has no declared field `{field}` — the committed CREATE TYPE \
                     is the authority for its field types",
                    udt.name
                )
            })?;
        // The missing/extra field sets above already agree, so this cannot be
        // absent. Stated as an error rather than defaulted to `Null`: a default
        // here would silently compare an absent field as a null one if that
        // agreement check ever moved.
        let cv = c.get(field).ok_or_else(|| {
            format!(
                "udt `{}`: field `{field}` vanished between the field-set check and the \
                 comparison",
                udt.name
            )
        })?;
        // A UDT field is a cell VALUE (a frozen UDT's fields live inside one value
        // cell; a non-frozen UDT's field IS the cell value), so the golden keeps
        // its natural JSON kind.
        let member = at.field(field, Kinding::Natural);
        compare_value_at(gv, cv, egress, field_ty, &member)
            .map_err(|why| format!(".{field} {why}"))?;
    }
    Ok(())
}

/// The field name the JSON egress adds to a UDT object to name its type.
const DISCRIMINATOR: &str = "_type";

/// The JSON egress's UDT `_type` field must be PRESENT, a STRING, and the type
/// name the committed `CREATE TYPE` declares.
///
/// Stripping it unconditionally (the first cut of this file) made the
/// discriminator untestable in the one lane that renders it: a UDT object with no
/// `_type` at all, or one naming the wrong type — which is what a UDT resolved
/// against the wrong `CREATE TYPE` would produce — compared equal (issue #1491
/// review finding R3). The expected name comes from the DDL, so this is an
/// assertion against the committed schema and not against CQLite's own output.
///
/// The comparison folds ASCII case because an UNQUOTED CQL identifier is
/// case-insensitive — Cassandra stores `Person` and `person` as the same type
/// name — so requiring exact case would assert something CQL does not mean. Every
/// `CREATE TYPE` in `test-data/schemas/` is unquoted.
fn check_udt_discriminator(fields: &Map<String, Value>, udt: &UdtType) -> Result<(), String> {
    // `_type` is a LEGAL CQL field name, and a UDT declaring one would make the
    // discriminator indistinguishable from that field: the discriminator is
    // dropped from the CLI's field set, so the declared field would then read as
    // absent from the CLI and the DDL-presence rule above would report a field the
    // egress did render. No committed `CREATE TYPE` declares one; the collision is
    // REFUSED rather than resolved by guessing which of the two the value is.
    if udt
        .fields
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case(DISCRIMINATOR))
    {
        return Err(format!(
            "udt `{}`: the committed CREATE TYPE declares a field named `{DISCRIMINATOR}`, \
             which is also the name the JSON egress uses for its type discriminator — the \
             two are indistinguishable in the emitted object, so this lane cannot compare \
             such a UDT",
            udt.name
        ));
    }
    match fields.get(DISCRIMINATOR) {
        Some(Value::String(name)) if name.eq_ignore_ascii_case(&udt.name) => Ok(()),
        Some(Value::String(name)) => Err(format!(
            "udt `{}`: the JSON egress names the type `{name}` in its `{DISCRIMINATOR}` \
             discriminator, but the committed CREATE TYPE declares `{}`",
            udt.name, udt.name
        )),
        Some(other) => Err(format!(
            "udt `{}`: the JSON egress's `{DISCRIMINATOR}` discriminator is {}, not a \
             string naming the type",
            udt.name,
            brief(&describe(other, Egress::Json))
        )),
        None => Err(format!(
            "udt `{}`: the JSON egress object carries no `{DISCRIMINATOR}` discriminator \
             — the committed CREATE TYPE declares this value as `{}`, and the JSON \
             egress names a UDT's type in that field",
            udt.name, udt.name
        )),
    }
}

#[cfg(test)]
#[path = "golden_value_compare_udt_tests.rs"]
mod tests;
