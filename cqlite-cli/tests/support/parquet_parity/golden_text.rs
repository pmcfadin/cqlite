//! Preparing the sstabledump golden TEXT so a declared-`decimal`/`varint`
//! LITERAL survives the parse (#1490 rounds 10–13).
//!
//! # The defect this closes: an `f64` cannot identify the value it came from
//!
//! `sstabledump` writes a `decimal` CELL as a bare JSON number, and the shared
//! comparator (`canonical_jsonl::CanonicalValue::from_json`) parses a bare JSON
//! number into an `f64`. The harness used to RECOVER the decimal from that
//! double — render it at the export scale, then check that neither one-unit
//! neighbour parses to the same double — and treat a unique answer as exact.
//!
//! That is unsound in principle, not merely imprecise. `0.100000000000000001`
//! and `0.1` parse to the SAME `f64`; the recovered value is `0.1`, both
//! neighbours round elsewhere, so the recovery reported itself EXACT and a
//! golden literal carrying eighteen fractional digits was canonicalized as
//! `0.1`. A lossy export would then have compared EQUAL — a false PASS in the
//! one place this harness exists to be trusted. No amount of neighbour probing
//! fixes it: by the time the value is an `f64` the distinguishing digits are
//! already gone.
//!
//! `varint` has the same disease one exponent further out: a valid `varint`
//! above `u64::MAX` fails both `Number::as_i64` and `as_u64` and lands on an
//! `f64` too, while the exported side reads that column back as an exact
//! `Decimal128(38, 0)` integer — so the comparison was `Float` vs `Int`, a
//! false mismatch, with the digits that would have shown a real corruption
//! already lost.
//!
//! So the literal TEXT has to survive the parse. This module keeps it, and
//! `declared.rs` REFUSES a declared-`decimal`/`varint` position that arrives as
//! a double — so a lexeme this module fails to keep produces a loud refusal,
//! never a comparison.
//!
//! # How: keep the text AT DESERIALIZATION TIME, then quote the declared ones
//!
//! Each golden line is deserialized against the sstabledump structure it
//! actually has — an object, whose `rows` is an array of objects, whose `cells`
//! is an array of objects — with every value held as a
//! [`serde_json::value::RawValue`], i.e. as its ORIGINAL TEXT. So the parse
//! itself never destroys a lexeme, and a CELL is whatever sits at
//! `rows[i].cells[j]`: a POSITION in the document that `serde_json` resolves,
//! not a shape this module recognises.
//!
//! Then the number lexemes at declared-`decimal`/`varint` POSITIONS are QUOTED
//! — turned into JSON strings, verbatim, digit for digit — by the ONE
//! declared-type descent in [`super::declared::preserve_lexemes`]. The shared
//! parser then hands the harness the literal itself, and the declared-type door
//! routes it through [`super::decimal::exact_from_text`] (a `decimal`) or an
//! exact `i128` parse (a `varint`): the same exact, `f64`-free reads a `decimal`
//! PRIMARY-KEY component already used (Cassandra stringifies those, so their
//! text was never lost).
//!
//! Everything the descent does not rewrite is re-emitted from its retained
//! `RawValue`, verbatim — so a `double` column's literal still reaches
//! `serde_json`'s (`float_roundtrip`, exact) parser unchanged and the exact-bit
//! `float`/`double` comparison is untouched.
//!
//! # Why deserialization-time, and not a walker over the parsed tree
//!
//! The predecessor of this module (a hand-written JSON scanner plus a tree
//! walker, deleted in round 13) parsed the line itself and then WALKED the whole
//! parsed tree looking for cells.
//! It produced four review findings in two rounds, and they were one defect:
//! the walker re-derived structure the parse had destroyed, and asked "is this a
//! cell?" from an object's SHAPE — any nested object carrying a `"name"` string,
//! at any depth. So a frozen map or UDT field spelled `{"name":"amount", …}` was
//! rewritten per the unrelated `amount` COLUMN's declaration, corrupting the
//! oracle. Inferring a role from a value's shape is the same class as inferring
//! a TYPE from a value's bytes, which issue #28 forbids.
//!
//! Structure is therefore never re-derived here. `serde_json` resolves the
//! document's own structure once, cells are the members of a row's `cells`
//! array and nothing else, and no object anywhere else in the line can become
//! one however it is spelled. `a_nested_object_named_like_a_decimal_column_is_not_a_cell`
//! pins it.
//!
//! `arbitrary_precision` is the other way to keep a number's text through the
//! parse, and it is deliberately NOT enabled: it changes `serde_json::Number`'s
//! representation, equality and serialization for every serde_json user in these
//! test targets — including the files whose assertions compare serialized JSON
//! output — so a decimal fix would move unrelated determinism contracts as a
//! side effect. `canonical_jsonl.rs` itself is `cqlite-core`-owned and shared by
//! several lanes, so teaching it to carry a decimal is out of this issue's scope.
//!
//! # Fail-closed
//!
//! Any line that is not one JSON object, a `rows` that is not an array of
//! objects, a `cells` that is not an array of objects, or a cell without a
//! string `name` is an `Err` that aborts the case. It never falls back to the
//! untransformed text, because that would silently restore the `f64` path this
//! module removes.
//!
//! A DUPLICATE object key is refused too, at EVERY depth, before anything reads
//! a value from the line — see [`reject_duplicate_keys`]. JSON permits one and
//! the two readers of this document disagree about which occurrence the object
//! has, so a golden carrying one has no single value to compare.

#![allow(dead_code)]

use std::fmt;

use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;

use super::cql_type::ColumnType;
use super::declared;

/// A JSON object whose every value is held as its ORIGINAL TEXT, in the order
/// the document wrote them.
///
/// Order is a correctness property here, not a nicety: a frozen map arrives as a
/// JSON object and the comparison depends on its order being the one sstabledump
/// wrote (Cassandra's key-comparator order), which is the order the Arrow map
/// carries — see `declared::canonicalize_golden`. So the order is held
/// EXPLICITLY, in a `Vec`, rather than inherited from whichever map type
/// `serde_json`'s `preserve_order` feature happens to select.
/// (`serde_json::Map` cannot be used at all: it is hard-coded to `Value`
/// values, which is exactly the parse that destroys the lexemes.)
#[derive(Debug)]
pub struct RawObject(Vec<(String, Box<RawValue>)>);

impl RawObject {
    /// The retained text of `key`, if the object has it.
    pub fn get(&self, key: &str) -> Option<&RawValue> {
        self.0.iter().find(|(k, _)| k == key).map(|(_, v)| &**v)
    }

    /// Replace `key`'s value IN PLACE, keeping its original position; append it
    /// if the object does not have it.
    pub fn set(&mut self, key: &str, value: Box<RawValue>) {
        match self.0.iter_mut().find(|(k, _)| k == key) {
            Some((_, slot)) => *slot = value,
            None => self.0.push((key.to_string(), value)),
        }
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&String, &mut Box<RawValue>)> {
        self.0.iter_mut().map(|(k, v)| (&*k, v))
    }
}

impl<'de> Deserialize<'de> for RawObject {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ObjectVisitor;
        impl<'de> Visitor<'de> for ObjectVisitor {
            type Value = RawObject;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a JSON object")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<RawObject, A::Error> {
                let mut out: Vec<(String, Box<RawValue>)> =
                    Vec::with_capacity(map.size_hint().unwrap_or(0));
                while let Some((k, v)) = map.next_entry::<String, Box<RawValue>>()? {
                    // A DUPLICATE key is REFUSED, never resolved. JSON permits
                    // one, and the two readers of this document would then
                    // disagree about which value it has: this type keeps the
                    // FIRST occurrence, while the shared `serde_json::Value`
                    // parse downstream keeps the LAST. Silently picking either
                    // would mean the harness rewrote one value and compared the
                    // other — the oracle disagreeing with itself.
                    //
                    // This refusal is LOCAL: it fires only for an object this
                    // type actually deserializes, which is not every object in
                    // the line (a UDT value is returned as the identity and
                    // never opened). The TOTAL check is
                    // `reject_duplicate_keys`, run over the whole line first;
                    // this one is kept because it holds for every future caller
                    // of `RawObject` whether or not it went through that door.
                    if out.iter().any(|(seen, _)| *seen == k) {
                        return Err(de::Error::custom(format!(
                            "the JSON object carries the duplicate key {k:?}; the harness \
                             refuses to choose which occurrence is the value"
                        )));
                    }
                    out.push((k, v));
                }
                Ok(RawObject(out))
            }
        }
        deserializer.deserialize_map(ObjectVisitor)
    }
}

impl Serialize for RawObject {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (k, v) in &self.0 {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}

/// Placeholder markers a golden must never contain.
///
/// MIRRORS `canonical_jsonl`'s private `PLACEHOLDER_MARKERS`, re-applied here
/// because the harness now reads the golden TEXT itself (to keep decimal
/// lexemes) and so no longer goes through `load_golden_document_with_keys`,
/// which owns that refusal. `canonical_jsonl.rs` is `cqlite-core`-owned, so the
/// const cannot be shared from here; `golden_placeholder_marker_is_refused`
/// pins this copy.
const PLACEHOLDER_MARKERS: &[&str] = &[
    "\"PLACEHOLDER\"",
    "\"__PLACEHOLDER__\"",
    "\"TODO\"",
    "\"GENERATED_PLACEHOLDER\"",
    "PLACEHOLDER_REFERENCE",
];

/// The first placeholder marker `content` carries, if any.
pub fn placeholder_marker(content: &str) -> Option<&'static str> {
    PLACEHOLDER_MARKERS
        .iter()
        .copied()
        .find(|m| content.contains(m))
}

// ---------------------------------------------------------------------------
// DUPLICATE OBJECT KEYS — refused at EVERY depth, before anything reads a value
// ---------------------------------------------------------------------------
//
// JSON permits a duplicate key, and the two readers of this document DISAGREE
// about which occurrence the object has: `RawObject` above keeps the FIRST, the
// shared `serde_json::Value` parse downstream keeps the LAST. So a golden
// carrying one is a document whose value depends on who is reading it, and the
// harness refuses it rather than choose.
//
// `RawObject`'s own refusal is LOCAL: it fires only for an object the lexeme
// descent actually DESERIALIZES. Whole regions of a line are never opened —
// most sharply a UDT value, which `declared::preserve_lexemes` returns as the
// IDENTITY (its field types are not declared to the harness, so no position
// inside one can be known to be a `decimal`/`varint`). A duplicate key inside
// such a value was therefore invisible: the later `serde_json::Value` parse
// silently selected one occurrence and the golden passed this stage. That is
// most dangerous for exactly the cases whose known export gap defers the value
// comparison (#3556), because then the golden stage is the ONLY thing that
// would have noticed a malformed golden at all.
//
// The check below is therefore TOTAL: it visits every object in the line, at
// every depth, whether or not any descent reaches it and whether or not any
// lexeme there needs rewriting. It is NOT a second position-deciding traversal —
// it decides nothing about types, positions or roles, asks only the pure JSON
// SYNTAX question "does this object repeat a key?", and produces no value. In
// particular the numbers it walks past are DISCARDED, never read for a value,
// so the `f64` this module exists to avoid is not reachable through it.

/// A JSON value checked, at every depth, for DUPLICATE OBJECT KEYS — and for
/// nothing else.
///
/// Carries the JSON PATH of the value it is about to visit, so the refusal names
/// WHERE the duplicate is (`rows[0].cells[2].value.field`) rather than only which
/// key repeated. Values are consumed and thrown away.
struct NoDuplicateKeys<'p>(&'p str);

impl<'de> DeserializeSeed<'de> for NoDuplicateKeys<'_> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for NoDuplicateKeys<'_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let mut seen: Vec<String> = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
            if seen.contains(&key) {
                return Err(de::Error::custom(format!(
                    "the JSON object at {} carries the duplicate key {key:?}; the harness \
                     refuses to choose which occurrence is the value",
                    self.0
                )));
            }
            let child = format!("{}.{key}", self.0);
            map.next_value_seed(NoDuplicateKeys(&child))?;
            seen.push(key);
        }
        Ok(())
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        let mut i = 0usize;
        loop {
            let child = format!("{}[{i}]", self.0);
            if seq.next_element_seed(NoDuplicateKeys(&child))?.is_none() {
                return Ok(());
            }
            i += 1;
        }
    }

    // Every SCALAR is accepted and discarded. These arms exist because
    // `Visitor`'s defaults REJECT the type they are given, so a missing arm
    // would turn an ordinary golden number or string into a parse error. None of
    // them reads a value: the argument is dropped.
    fn visit_bool<E: de::Error>(self, _: bool) -> Result<(), E> {
        Ok(())
    }
    fn visit_i64<E: de::Error>(self, _: i64) -> Result<(), E> {
        Ok(())
    }
    fn visit_u64<E: de::Error>(self, _: u64) -> Result<(), E> {
        Ok(())
    }
    fn visit_f64<E: de::Error>(self, _: f64) -> Result<(), E> {
        Ok(())
    }
    fn visit_str<E: de::Error>(self, _: &str) -> Result<(), E> {
        Ok(())
    }
    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_none<E: de::Error>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

/// REFUSE `line` if any JSON object in it, at any depth, repeats a key.
///
/// Run over the WHOLE line before anything reads a value from it, so a region no
/// descent opens (a UDT value, a `path` component, a field the harness does not
/// declare) is covered exactly like one it does.
pub fn reject_duplicate_keys(line: &str) -> Result<(), String> {
    let mut de = serde_json::Deserializer::from_str(line);
    NoDuplicateKeys("<line>")
        .deserialize(&mut de)
        .map_err(|e| e.to_string())?;
    de.end().map_err(|e| e.to_string())
}

/// Rewrite a whole JSONL document, quoting the number lexemes that sit at a
/// declared-`decimal`/`varint` POSITION — and no others.
///
/// Blank lines are preserved as-is; every other line is deserialized against the
/// sstabledump structure, transformed and re-emitted from its retained text. A
/// line whose structure does not hold is an `Err` naming the line.
pub fn preserve_exact_lexemes(content: &str, columns: &[ColumnType]) -> Result<String, String> {
    let mut out = String::with_capacity(content.len());
    for (idx, line) in content.lines().enumerate() {
        if line.trim().is_empty() {
            out.push_str(line);
            out.push('\n');
            continue;
        }
        let rewritten =
            rewrite_line(line.trim(), columns).map_err(|e| format!("line {}: {e}", idx + 1))?;
        out.push_str(&rewritten);
        out.push('\n');
    }
    Ok(out)
}

/// One sstabledump line: its `rows` are rewritten, every other field is
/// re-emitted from the text it arrived as.
fn rewrite_line(line: &str, columns: &[ColumnType]) -> Result<String, String> {
    // TOTAL, and first: every object in the line, at every depth, including the
    // regions the lexeme descent never opens. See the section above.
    reject_duplicate_keys(line)?;
    let mut top: RawObject = serde_json::from_str(line)
        .map_err(|e| format!("an sstabledump line must be one JSON object: {e}"))?;
    if let Some(rows_text) = top.get("rows").map(|r| r.get().to_string()) {
        let mut rows: Vec<RawObject> = serde_json::from_str(&rows_text)
            .map_err(|e| format!("`rows` must be an array of JSON objects: {e}"))?;
        for (i, row) in rows.iter_mut().enumerate() {
            rewrite_row(row, columns).map_err(|e| format!("rows[{i}]: {e}"))?;
        }
        set_raw(&mut top, "rows", &rows)?;
    }
    emit(&top)
}

/// One row: its `cells` are rewritten, every other field is untouched. A row
/// with no `cells` (a range tombstone marker) has nothing to rewrite.
fn rewrite_row(row: &mut RawObject, columns: &[ColumnType]) -> Result<(), String> {
    let Some(cells_text) = row.get("cells").map(|c| c.get().to_string()) else {
        return Ok(());
    };
    let mut cells: Vec<RawObject> = serde_json::from_str(&cells_text)
        .map_err(|e| format!("`cells` must be an array of JSON objects: {e}"))?;
    for (i, cell) in cells.iter_mut().enumerate() {
        rewrite_cell(cell, columns).map_err(|e| format!("cells[{i}]: {e}"))?;
    }
    set_raw(row, "cells", &cells)
}

/// One CELL: hand its `value` to the declared-type descent at the position that
/// cell's `value` occupies.
///
/// The column is identified from the cell's OWN `name` field — never from the
/// value's shape, which would be the role-guessing that produced the round-12
/// finding. A cell naming a column the case does not declare is left untouched
/// here; `golden_rows::reject_undeclared_cells` FAILS the case on it, which is a
/// louder answer than anything this pass could give.
fn rewrite_cell(cell: &mut RawObject, columns: &[ColumnType]) -> Result<(), String> {
    let Some(name_text) = cell.get("name").map(|n| n.get().to_string()) else {
        return Err(
            "an sstabledump cell has no `name`, so the column whose declared type governs it \
             cannot be identified"
                .to_string(),
        );
    };
    let name: String = serde_json::from_str(&name_text)
        .map_err(|e| format!("an sstabledump cell's `name` must be a JSON string: {e}"))?;
    let Some(col) = columns.iter().find(|c| c.name == name) else {
        return Ok(());
    };
    // WHICH position the cell's `value` sits at is a declared-type question, so
    // it is answered by the declared-type door, not here.
    let Some(at) =
        declared::cell_value_declared(col, format!("cell '{}' ({})", col.name, col.declared))
    else {
        return Ok(());
    };
    // A cell carrying only a `deletion_info` has no value to preserve.
    let Some(value) = cell.get("value") else {
        return Ok(());
    };
    let rewritten = declared::preserve_lexemes(value, &at)?;
    let raw = RawValue::from_string(rewritten)
        .map_err(|e| format!("the rewritten value of cell '{name}' is not valid JSON: {e}"))?;
    cell.set("value", raw);
    Ok(())
}

/// Serialize `value` and put it back under `key`, keeping the key's ORIGINAL
/// position in the object.
fn set_raw<T: Serialize>(object: &mut RawObject, key: &str, value: &T) -> Result<(), String> {
    let text =
        serde_json::to_string(value).map_err(|e| format!("re-emitting `{key}` failed: {e}"))?;
    let raw = RawValue::from_string(text)
        .map_err(|e| format!("the re-emitted `{key}` is not valid JSON: {e}"))?;
    object.set(key, raw);
    Ok(())
}

fn emit(object: &RawObject) -> Result<String, String> {
    serde_json::to_string(object).map_err(|e| format!("re-emitting the line failed: {e}"))
}
