//! Declared-CQL-type model for the Parquet↔JSONL parity harness (issue #1490).
//!
//! # Why the harness declares types instead of reading CQLite's Arrow schema
//!
//! Canonicalizing the sstabledump golden needs the column's declared type in two
//! places:
//!
//!   * **Multicell collections.** sstabledump emits one CELL PER ELEMENT with a
//!     `path`, so a `set<int>` arrives as five path-only cells and a
//!     `map<int,text>` as four `path`+`value` cells. Reassembling them into one
//!     collection value requires knowing which kind of collection it is — and
//!     whether it is `frozen` (one whole JSON value) or not (per-element cells).
//!   * **Path components are STRINGIFIED.** A `set<int>` element arrives as
//!     `"-2"`, not `-2` (same rule as partition keys, see
//!     `canonical_jsonl::CanonicalValue::from_json_key`), so coercing it back to
//!     an integer needs the declared element type. Coercing blindly would let a
//!     `set<text>` containing `"5"` compare equal to a `set<int>` containing `5`.
//!
//! The type could be read out of the Parquet file's own Arrow schema, which
//! CQLite derives from the CQL schema. That would be CIRCULAR: the harness would
//! interpret the Cassandra-written oracle through the very mapping under test,
//! so a column mapped to the wrong Arrow type could still compare equal
//! (#3041 — CQLite is never authority for what is correct). So each case
//! DECLARES its columns with the type text copied from the committed
//! `test-data/schemas/*.cql` (i.e. from the Cassandra schema that produced the
//! fixture), and the harness asserts the Parquet schema carries exactly that
//! column set.
//!
//! The parser handles the type grammar the corpus actually uses:
//! scalars, `set<…>`, `list<…>`, `map<…,…>`, `tuple<…>` and `frozen<…>`
//! wrappers around any of those. Anything else is an ERROR (never a permissive
//! fallback): an unparsed type would silently degrade to "compare as JSON",
//! which is how a wrong oracle passes.

#![allow(dead_code)]

/// A parsed declared CQL type.
#[derive(Debug, Clone, PartialEq)]
pub enum CqlTypeSpec {
    /// A scalar type, e.g. `int`, `text`, `float`, `decimal`.
    Scalar(String),
    /// `set<E>` / `list<E>` — `frozen` recorded separately by [`ColumnType`].
    Seq {
        kind: SeqKind,
        elem: Box<CqlTypeSpec>,
    },
    /// `map<K,V>`.
    Map {
        key: Box<CqlTypeSpec>,
        value: Box<CqlTypeSpec>,
    },
    /// `tuple<A,B,…>`.
    Tuple(Vec<CqlTypeSpec>),
    /// A user-defined type, named but not structurally known to the harness.
    ///
    /// A frozen UDT arrives as ONE JSON object whose field values are already
    /// typed by sstabledump, so the harness never needs the field types — but it
    /// does need to know this is not a scalar.
    Udt(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeqKind {
    Set,
    List,
}

/// A declared column: name, parsed type, and whether the declaration was
/// wrapped in `frozen<…>` at the TOP level (which decides multicell-vs-single
/// cell in the sstabledump dump, the one distinction the harness cannot infer).
#[derive(Debug, Clone)]
pub struct ColumnType {
    pub name: String,
    pub spec: CqlTypeSpec,
    pub frozen: bool,
    /// The type text exactly as declared, for diagnostics.
    pub declared: String,
}

impl ColumnType {
    /// True when sstabledump emits this column as one cell PER ELEMENT (a
    /// non-frozen collection), rather than a single cell carrying one value.
    pub fn is_multicell_collection(&self) -> bool {
        !self.frozen && matches!(self.spec, CqlTypeSpec::Seq { .. } | CqlTypeSpec::Map { .. })
    }
}

/// Parse a declared column type, e.g. `map<int, text>` or `frozen<person>`.
///
/// `udts` names the user-defined types declared by the case's schema file, so a
/// bare identifier that is NOT a known scalar and NOT a declared UDT is an error
/// rather than being waved through as a UDT.
pub fn parse_column(name: &str, declared: &str, udts: &[&str]) -> Result<ColumnType, String> {
    let trimmed = declared.trim();
    let (frozen, inner) = match strip_wrapper(trimmed, "frozen") {
        Some(inner) => (true, inner),
        None => (false, trimmed),
    };
    let spec = parse_spec(inner, udts)?;
    Ok(ColumnType {
        name: name.to_string(),
        spec,
        frozen,
        declared: trimmed.to_string(),
    })
}

/// Every scalar CQL type name the corpus declares. An unlisted name is an
/// ERROR: a permissive default would canonicalize an unknown type by JSON shape
/// and quietly weaken the oracle.
const SCALARS: &[&str] = &[
    "ascii",
    "bigint",
    "blob",
    "boolean",
    "counter",
    "date",
    "decimal",
    "double",
    "duration",
    "float",
    "inet",
    "int",
    "smallint",
    "text",
    "time",
    "timestamp",
    "timeuuid",
    "tinyint",
    "uuid",
    "varchar",
    "varint",
];

fn parse_spec(t: &str, udts: &[&str]) -> Result<CqlTypeSpec, String> {
    let t = t.trim();
    if let Some(inner) = strip_wrapper(t, "frozen") {
        // Nested frozen (`list<frozen<person>>`): frozen-ness only matters at the
        // top level (multicell vs single cell), so unwrap it here.
        return parse_spec(inner, udts);
    }
    if let Some(inner) = strip_wrapper(t, "set") {
        return Ok(CqlTypeSpec::Seq {
            kind: SeqKind::Set,
            elem: Box::new(parse_spec(inner, udts)?),
        });
    }
    if let Some(inner) = strip_wrapper(t, "list") {
        return Ok(CqlTypeSpec::Seq {
            kind: SeqKind::List,
            elem: Box::new(parse_spec(inner, udts)?),
        });
    }
    if let Some(inner) = strip_wrapper(t, "map") {
        let parts = split_top_level(inner)?;
        if parts.len() != 2 {
            return Err(format!("map<…> needs exactly 2 parameters, got: {t}"));
        }
        return Ok(CqlTypeSpec::Map {
            key: Box::new(parse_spec(&parts[0], udts)?),
            value: Box::new(parse_spec(&parts[1], udts)?),
        });
    }
    if let Some(inner) = strip_wrapper(t, "tuple") {
        let parts = split_top_level(inner)?;
        let mut out = Vec::with_capacity(parts.len());
        for p in parts {
            out.push(parse_spec(&p, udts)?);
        }
        return Ok(CqlTypeSpec::Tuple(out));
    }
    let lower = t.to_ascii_lowercase();
    if SCALARS.contains(&lower.as_str()) {
        return Ok(CqlTypeSpec::Scalar(lower));
    }
    if udts.iter().any(|u| u.eq_ignore_ascii_case(t)) {
        return Ok(CqlTypeSpec::Udt(lower));
    }
    Err(format!(
        "unrecognized CQL type '{t}' (not a known scalar and not one of the \
         case's declared UDTs {udts:?}) — declare it rather than letting the \
         harness guess"
    ))
}

/// `strip_wrapper("set<int>", "set")` → `Some("int")`; case-insensitive on the
/// wrapper name, and only matches a BALANCED `<…>` that spans the whole input.
fn strip_wrapper<'a>(t: &'a str, wrapper: &str) -> Option<&'a str> {
    let t = t.trim();
    if t.len() <= wrapper.len() + 2 {
        return None;
    }
    if !t[..wrapper.len()].eq_ignore_ascii_case(wrapper) {
        return None;
    }
    let rest = t[wrapper.len()..].trim_start();
    let inner = rest.strip_prefix('<')?.strip_suffix('>')?;
    // Reject `set<a>,set<b>` shapes where the `>` closed early.
    let mut depth = 0i32;
    for ch in inner.chars() {
        match ch {
            '<' => depth += 1,
            '>' => depth -= 1,
            _ => {}
        }
        if depth < 0 {
            return None;
        }
    }
    (depth == 0).then_some(inner)
}

/// Split `int, text` / `text, frozen<address>` on top-level commas.
fn split_top_level(inner: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut cur = String::new();
    for ch in inner.chars() {
        match ch {
            '<' => {
                depth += 1;
                cur.push(ch);
            }
            '>' => {
                depth -= 1;
                if depth < 0 {
                    return Err(format!("unbalanced '>' in '{inner}'"));
                }
                cur.push(ch);
            }
            ',' if depth == 0 => {
                out.push(cur.trim().to_string());
                cur = String::new();
            }
            _ => cur.push(ch),
        }
    }
    if depth != 0 {
        return Err(format!("unbalanced '<' in '{inner}'"));
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    Ok(out)
}
