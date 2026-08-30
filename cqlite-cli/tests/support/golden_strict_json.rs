//! A JSON parse that REFUSES a duplicate object key (issue #1491 review finding
//! K2).
//!
//! # Why the oracle cannot use `serde_json::from_str` for this
//!
//! `serde_json::Value`'s own `Deserialize` inserts each key into a map as it goes,
//! so a repeated key silently LAST-WINS. Every consumer downstream then sees one
//! key with the last value, and the earlier one has vanished — from the value
//! comparison, from the column-shape check and from the cell count alike. So
//! malformed egress like
//!
//! ```text
//! [{"pk": 1, "v": "wrong", "v": "expected"}]
//! ```
//!
//! compared EQUAL to a golden carrying `v = "expected"`, and the same held one
//! level in, for a duplicated UDT field or a duplicated `_type` discriminator.
//!
//! That is the JSON half of the duplicate-key defect whose CSV halves are already
//! closed: a repeated CSV header row column (`compare::cli_csv_rows`) and a
//! repeated decoded UDT field (`compare::compare_udt`), both finding J2. A
//! duplicate is MALFORMED OUTPUT, not something to reconcile: JSON's object model
//! gives it no meaning, so no reading of it can be trusted, and quietly keeping
//! one of the two occurrences is precisely how a difference that matters gets
//! hidden.
//!
//! Deliberately NOT aligned with `cqlite-cli/src/output/json.rs`, which carries a
//! documented `dedup_keys_last_wins` for the WRITER. That is the writer's own
//! stated behaviour; here the question is whether the bytes it produced are
//! well-formed, and an oracle that adopted the writer's rule would be unable to
//! see the writer break it (CLAUDE.md: a CQLite `file:line` is never authority for
//! what is correct).
//!
//! # Also used for the GOLDEN
//!
//! The same rule is applied to each `*-Data.db.jsonl` line. A duplicate key there
//! would silently DISCARD part of the oracle — the same shape as two multicell map
//! cells for one key, which `golden_rows` already refuses rather than collapses.
//! An oracle the reader must drop part of is not a usable oracle.
//!
//! # One parse, not a second implementation
//!
//! The check runs INSIDE the real `serde_json` parse (a `DeserializeSeed` +
//! `Visitor` pair building the same `Value` serde_json would), rather than as a
//! separate hand-written scan of the text alongside it. A separate scanner would
//! be a second implementation of JSON whose agreement with the first is only
//! knowable by differential testing — the failure mode CLAUDE.md records for
//! ported oracles. Number handling therefore matches `serde_json::Value`
//! exactly (i64 / u64 / f64, no arbitrary precision), so nothing about existing
//! value comparisons changes.

use serde::de::{DeserializeSeed, Deserializer, Error as DeError, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use std::fmt;

/// Parse `text` as JSON, refusing any duplicate object key.
///
/// `root` names the document for diagnostics (`"egress"`, `"golden line 3"`); the
/// reported path is built from it, so a duplicate is always attributable to a
/// position — the row and the key, and the field path when it is nested.
pub fn parse(text: &str, root: &str) -> Result<Value, String> {
    let mut de = serde_json::Deserializer::from_str(text);
    let value = At::new(root)
        .deserialize(&mut de)
        .map_err(|e| e.to_string())?;
    // Trailing content is an error, exactly as `serde_json::from_str` treats it.
    de.end().map_err(|e| e.to_string())?;
    Ok(value)
}

/// One position in the document, carried through the parse so a duplicate key can
/// be reported with its path rather than as a bare key name.
struct At {
    path: String,
}

impl At {
    fn new(path: &str) -> Self {
        At {
            path: path.to_string(),
        }
    }

    fn child_index(&self, index: usize) -> Self {
        At {
            path: format!("{}[{index}]", self.path),
        }
    }

    fn child_key(&self, key: &str) -> Self {
        At {
            path: if self.path.is_empty() {
                key.to_string()
            } else {
                format!("{}.{key}", self.path)
            },
        }
    }
}

impl<'de> DeserializeSeed<'de> for At {
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for At {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_unit<E: DeError>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_none<E: DeError>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }

    fn visit_bool<E: DeError>(self, v: bool) -> Result<Value, E> {
        Ok(Value::Bool(v))
    }

    fn visit_i64<E: DeError>(self, v: i64) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }

    fn visit_u64<E: DeError>(self, v: u64) -> Result<Value, E> {
        Ok(Value::Number(v.into()))
    }

    fn visit_f64<E: DeError>(self, v: f64) -> Result<Value, E> {
        // `serde_json::Value` maps a non-finite float onto `Value::Null` here.
        // Unreachable from JSON TEXT — the grammar has no `Infinity`/`NaN`
        // literal, so `serde_json` rejects them before this point — and reported
        // rather than defaulted, because a `null` standing in for a number is
        // exactly the swallowed difference this module exists to stop.
        match Number::from_f64(v) {
            Some(n) => Ok(Value::Number(n)),
            None => Err(E::custom(format!(
                "{}: the non-finite number {v} has no JSON representation",
                self.path
            ))),
        }
    }

    fn visit_str<E: DeError>(self, v: &str) -> Result<Value, E> {
        Ok(Value::String(v.to_string()))
    }

    fn visit_string<E: DeError>(self, v: String) -> Result<Value, E> {
        Ok(Value::String(v))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut out = Vec::new();
        loop {
            let at = self.child_index(out.len());
            match seq.next_element_seed(at)? {
                Some(value) => out.push(value),
                None => break,
            }
        }
        Ok(Value::Array(out))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut out = Map::new();
        while let Some(key) = map.next_key::<String>()? {
            // The value is consumed BEFORE the duplicate is reported, so the
            // refusal is decided on a complete, well-understood entry.
            let value = map.next_value_seed(self.child_key(&key))?;
            if let Some(previous) = out.get(&key) {
                return Err(A::Error::custom(format!(
                    "{}: duplicate object key `{key}` ({} then {}) — a repeated key is \
                     malformed JSON output, and keeping only one of the two would hide \
                     the other",
                    self.path,
                    brief(previous),
                    brief(&value)
                )));
            }
            out.insert(key, value);
        }
        Ok(Value::Object(out))
    }
}

/// A truncated rendering for a diagnostic (the corpus carries 4 KiB blobs).
fn brief(v: &Value) -> String {
    const LIMIT: usize = 80;
    let text = v.to_string();
    if text.chars().count() <= LIMIT {
        return text;
    }
    let head: String = text.chars().take(LIMIT).collect();
    format!("{head}…({} chars)", text.chars().count())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The defect: `serde_json::Value`'s own parse accepts the malformed egress and
    /// last-wins, so it compares EQUAL to a correct row. Asserted here so the
    /// reason this module exists is pinned rather than described.
    #[test]
    fn the_permissive_parse_last_wins_and_this_one_refuses() {
        let malformed = r#"[{"pk":1,"v":"wrong","v":"expected"}]"#;
        let permissive: Value =
            serde_json::from_str(malformed).expect("serde_json accepts a duplicate key");
        assert_eq!(
            permissive,
            serde_json::from_str::<Value>(r#"[{"pk":1,"v":"expected"}]"#).expect("valid"),
            "the permissive parse makes malformed egress indistinguishable from correct \
             egress — which is the whole defect"
        );

        let why = parse(malformed, "egress").expect_err("a duplicate key must be refused");
        assert!(
            why.contains("duplicate object key `v`") && why.contains("egress[0]"),
            "the refusal must name the row and the duplicated key: {why}"
        );
        assert!(
            why.contains("wrong") && why.contains("expected"),
            "the refusal must name BOTH occurrences, so neither is silently preferred: {why}"
        );
    }

    /// A duplicate NESTED one level in — a repeated UDT field, or a repeated
    /// `_type` discriminator — is refused with the field path, not just the row.
    #[test]
    fn a_duplicate_nested_field_is_refused_with_its_path() {
        for body in [
            r#"[{"pk":1,"e":{"_type":"employee","name":"a","name":"b"}}]"#,
            r#"[{"pk":1,"e":{"_type":"employee","_type":"other","name":"a"}}]"#,
        ] {
            let why = parse(body, "egress").expect_err("a nested duplicate must be refused");
            assert!(
                why.contains("egress[0].e"),
                "the refusal must name the nested path: {why}"
            );
        }
        // A duplicate inside an ARRAY member is reached too, so a UDT inside a
        // collection is covered by the same rule.
        let why = parse(r#"[{"pk":1,"c":[{"k":1,"k":2}]}]"#, "egress")
            .expect_err("a duplicate inside a collection member must be refused");
        assert!(
            why.contains("egress[0].c[0]"),
            "the refusal must name the member path: {why}"
        );
    }

    /// The parse is otherwise `serde_json`'s: every value kind round-trips
    /// identically, so nothing about the existing value comparisons changes.
    #[test]
    fn a_well_formed_document_parses_exactly_as_serde_json_does() {
        for text in [
            r#"[]"#,
            r#"[{"a":1,"b":-2,"c":1.5,"d":"x","e":null,"f":true,"g":[1,2],"h":{"i":{}}}]"#,
            // The widths the goldens actually carry: an i64 edge, a u64 beyond
            // i64, and a decimal too long for f64 (which serde_json reduces to an
            // f64 — matched exactly, not improved on).
            r#"{"n":-9223372036854775808,"u":18446744073709551615,
                "d":123456789012345678901234567890.123}"#,
            r#""just a string""#,
            r#"null"#,
        ] {
            let strict = parse(text, "doc").unwrap_or_else(|why| panic!("{text}: {why}"));
            let permissive: Value =
                serde_json::from_str(text).unwrap_or_else(|e| panic!("{text}: {e}"));
            assert_eq!(strict, permissive, "{text}");
        }
    }

    /// Malformed JSON and trailing content are errors, exactly as
    /// `serde_json::from_str` treats them — so routing the oracle through this
    /// parse cannot make a broken document readable.
    #[test]
    fn invalid_json_and_trailing_content_are_still_errors() {
        for text in ["{", "[1,]", r#"{"a":1} {"b":2}"#, r#"{"a":1}trailing"#] {
            assert!(parse(text, "doc").is_err(), "`{text}` must not parse",);
        }
    }

    /// An empty object key is a legal JSON key and not a duplicate of anything, so
    /// the rule is about repetition and not about the key's content.
    #[test]
    fn distinct_keys_including_an_empty_one_are_accepted() {
        let value = parse(r#"{"":1,"a":2}"#, "doc").expect("distinct keys are fine");
        assert_eq!(value.as_object().map(Map::len), Some(2));
        let why = parse(r#"{"":1,"":2}"#, "doc").expect_err("a repeated empty key is a duplicate");
        assert!(why.contains("duplicate object key"), "{why}");
    }
}
