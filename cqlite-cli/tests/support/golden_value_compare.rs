//! The comparator half of the AD2 egress-parity oracle (issue #1491).
//!
//! Included as a submodule of `golden_value_parity`, which owns the scalar
//! canonicalization rules and the golden reader. This file owns:
//!
//!   * reading the CLI's own `export` output back (`--format json`, `--format csv`),
//!   * pairing golden rows with CLI rows by primary key, and
//!   * the recursive value comparison.
//!
//! The comparison walks BOTH sides together rather than canonicalizing each side
//! independently, because the two renderings of a container differ in *shape*, not
//! just in text: a map is a JSON object in the dump and an array of
//! `{"key","value"}` pairs in the CLI. Walking in step means each rule is stated
//! once, at the point where both spellings are in hand.
//!
//! # The column set comes from the committed DDL, and only the golden may default
//!
//! The compared column set is the one the committed `CREATE TABLE` declares (see
//! [`super::schema`]) — never the union of what the two sides happen to carry.
//! That asymmetry is the whole point:
//!
//!   * the CLI must render EVERY declared column, so a column it OMITS is a
//!     failure naming the column, and a column it invents that the DDL does not
//!     declare is a failure naming that;
//!   * only a MISSING GOLDEN cell may be read as an expected `null`, because the
//!     physical dump legitimately omits a cell that was never written — that
//!     absence IS the expected null.
//!
//! Defaulting BOTH sides to `null` (the first cut of this file) made "an absent
//! cell renders as `null`" untestable: a column CQLite omitted entirely compared
//! equal to a golden null, and a spurious extra null column passed too — in a lane
//! whose `nb_absent_vs_null_regular` case exists for exactly that property.
//!
//! Every value is compared against its DECLARED CQL type, threaded through nesting
//! (collection element, map key, map value, UDT field, tuple position), so the
//! numeric normalization applies only where the DDL says the value is a number and
//! a `text` value is compared as an exact string.

use super::schema::{CqlType, TableSchema, UdtType};
use super::{canon_scalar, canon_typed, csv_container, Depth, Egress, Row};
use serde_json::{Map, Value};
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// The outcome of one table × one egress format.
#[derive(Debug, Default)]
pub struct Report {
    /// Human-readable divergences, each naming the row key and the column.
    pub diffs: Vec<String>,
    /// Cells actually value-compared. Zero on a non-empty table is a failure the
    /// caller must treat as such (a comparison that compared nothing is vacuous).
    pub compared_cells: usize,
    /// How many of [`Self::compared_cells`] were collection/UDT cells. Reported
    /// affirmatively in the census so "containers are covered" is a measurement
    /// rather than an assumption.
    pub container_cells: usize,
    /// Container cells REFUSED because the golden's own content cannot survive
    /// the unquoted CSV rendering (see `csv_container::ambiguity`). Counted, and
    /// named in [`Self::ambiguity_reasons`], so the narrowing is declared at run
    /// time rather than inferred from a silent gap.
    pub ambiguous_container_cells: usize,
    /// One deduplicated `column (reason)` entry per refusal cause.
    pub ambiguity_reasons: Vec<String>,
    /// Declared skip paths that matched NOTHING in this table's walk. An
    /// exclusion that no longer applies is a silent widening of coverage, so the
    /// caller must treat a non-empty list as a failure.
    pub skips_never_applied: Vec<String>,
}

/// Value paths excluded from the comparison, with a hit tally.
///
/// A path is fully qualified from the row: `sf` excludes a whole column, `e.home`
/// excludes ONE field of the `frozen<employee>` in column `e` while `e.name` and
/// `e.level` keep being compared. Whole-column granularity alone was too coarse
/// and cost real coverage — skipping `e` for its one divergent inner field left
/// `udt_nested` comparing nothing but its primary key (issue #1491 review finding
/// F5).
///
/// Every match is recorded, so [`Self::never_applied`] turns a stale exclusion
/// into a failure instead of a quiet gap.
pub struct SkipPaths<'a> {
    paths: &'a [&'a str],
    hit: RefCell<BTreeSet<String>>,
}

impl<'a> SkipPaths<'a> {
    pub fn new(paths: &'a [&'a str]) -> Self {
        Self {
            paths,
            hit: RefCell::new(BTreeSet::new()),
        }
    }

    /// Is this exact path excluded? Records the hit.
    fn excludes(&self, path: &str) -> bool {
        if self.paths.contains(&path) {
            self.hit.borrow_mut().insert(path.to_string());
            return true;
        }
        false
    }

    fn never_applied(&self) -> Vec<String> {
        let hit = self.hit.borrow();
        self.paths
            .iter()
            .filter(|p| !hit.contains(**p))
            .map(|p| (*p).to_string())
            .collect()
    }
}

/// `parent` extended by a named step (a UDT field).
fn field_path(parent: &str, step: &str) -> String {
    if parent.is_empty() {
        step.to_string()
    } else {
        format!("{parent}.{step}")
    }
}

/// `parent` extended by a positional/keyed step (a collection member, a tuple
/// slot, a map value).
fn index_path(parent: &str, index: &str) -> String {
    format!("{parent}[{index}]")
}

/// Pair rows by primary key and compare every column the committed DDL declares.
///
/// `schema` is the authority for BOTH the column set and each value's CQL type;
/// `pk`/`ck` are used for row pairing and diagnostics only.
pub fn compare_rows(
    golden: &[Row],
    cli: &[Row],
    schema: &TableSchema,
    pk: &[&str],
    ck: &[&str],
    skip_columns: &[&str],
    egress: Egress,
) -> Report {
    let mut report = Report::default();
    let skips = SkipPaths::new(skip_columns);
    if golden.len() != cli.len() {
        report.diffs.push(format!(
            "row count: golden {} vs {egress:?} egress {}",
            golden.len(),
            cli.len()
        ));
        return report;
    }
    let mut golden: Vec<&Row> = golden.iter().collect();
    let mut cli: Vec<&Row> = cli.iter().collect();
    // `sort_by_cached_key`: the key embeds the whole row (see `row_sort_key`), so a
    // 900-row table with 300-byte payloads would otherwise rebuild multi-kilobyte
    // keys O(n log n) times.
    golden.sort_by_cached_key(|r| row_sort_key(r, pk, ck, egress));
    cli.sort_by_cached_key(|r| row_sort_key(r, pk, ck, egress));

    // Column-SHAPE divergences are properties of the table, not of one row, so
    // each is reported ONCE (naming the first row it was seen on) instead of 900
    // identical lines. Detection still runs per row.
    let mut shape_seen: BTreeSet<String> = BTreeSet::new();

    for (g, c) in golden.iter().zip(cli.iter()) {
        let key = row_message_key(g, pk, ck, egress);
        report.diffs.extend(undeclared_columns(
            g,
            c,
            schema,
            &key,
            egress,
            &mut shape_seen,
        ));

        for column in &schema.columns {
            let name = column.name.as_str();
            // A WHOLE-column exclusion. A dotted `col.field` entry does not match
            // here; it is applied inside the walk, so the column's other fields
            // keep being compared.
            if skips.excludes(name) {
                continue;
            }
            // The CLI must render EVERY declared column. An omitted one is a
            // divergence, NOT an implicit null: reading it as null is what made
            // the absent-cell property untestable.
            let Some(cv) = c.get(name) else {
                if shape_seen.insert(format!("missing:{name}")) {
                    report.diffs.push(format!(
                        "row[{key}].{name}: absent from the {egress:?} egress row — the \
                         committed CREATE TABLE {} declares `{name}` ({}), so it must be \
                         rendered (a null cell as `null`, an empty CSV field)",
                        schema.table,
                        column.ty.describe()
                    ));
                }
                continue;
            };
            // Only the GOLDEN may default: `sstabledump` omits a cell that was
            // never written, so a missing golden cell IS the expected null.
            let gv = g.get(name).unwrap_or(&Value::Null);
            // CSV has no types, so a container arrives as one flat text field and
            // has to be decoded back into the golden's shape before comparison.
            let decoded = match csv_decoded(gv, cv, egress, name, &skips) {
                Ok(decoded) => decoded,
                Err(Refusal::Ambiguous(why)) => {
                    report.ambiguous_container_cells += 1;
                    let entry = format!("{name} ({why})");
                    if !report.ambiguity_reasons.contains(&entry) {
                        report.ambiguity_reasons.push(entry);
                    }
                    continue;
                }
                Err(Refusal::Unparseable(why)) => {
                    report.compared_cells += 1;
                    report.container_cells += 1;
                    report.diffs.push(format!(
                        "row[{key}].{name}: unparseable CSV container: {why}"
                    ));
                    continue;
                }
            };
            let cv = decoded.as_ref().unwrap_or(cv);
            report.compared_cells += 1;
            if matches!(gv, Value::Array(_) | Value::Object(_)) {
                report.container_cells += 1;
            }
            if let Err(why) =
                compare_value_at(gv, cv, egress, &column.ty, Depth::TopLevel, name, &skips)
            {
                report.diffs.push(format!("row[{key}].{name}: {why}"));
            }
        }
    }
    report.skips_never_applied = skips.never_applied();
    report
}

/// Columns present on either side that the committed `CREATE TABLE` does not
/// declare. On the CLI side that is a spurious column (which, when it held
/// `null`, used to pass); on the golden side it means the case names the wrong
/// schema/table, so the expectation itself is stale.
fn undeclared_columns(
    g: &Row,
    c: &Row,
    schema: &TableSchema,
    key: &str,
    egress: Egress,
    shape_seen: &mut BTreeSet<String>,
) -> Vec<String> {
    let mut out = Vec::new();
    for name in c.keys() {
        if schema.column(name).is_none() && shape_seen.insert(format!("cli-extra:{name}")) {
            out.push(format!(
                "row[{key}].{name}: the {egress:?} egress row carries a column the \
                 committed CREATE TABLE {} does not declare",
                schema.table
            ));
        }
    }
    for name in g.keys() {
        if schema.column(name).is_none() && shape_seen.insert(format!("golden-extra:{name}")) {
            out.push(format!(
                "row[{key}].{name}: the golden carries a cell for a column the committed \
                 CREATE TABLE {} does not declare — the case names the wrong schema or \
                 the transcription is stale",
                schema.table
            ));
        }
    }
    out
}

/// Why a CSV container cell could not be decoded.
enum Refusal {
    /// The GOLDEN's own content cannot survive the unquoted rendering, so no
    /// reading of the CLI's text is trustworthy. Decided from the golden alone,
    /// so it can never be caused by the defect under test.
    Ambiguous(String),
    /// The CLI's text is not the grammar at all (wrong bracket, unbalanced
    /// brackets, a map entry with no `: `). That IS a divergence, so it is
    /// reported as one rather than refused.
    Unparseable(String),
}

/// Decode a CSV cell whose golden counterpart is a container. `Ok(None)` means
/// no decoding applies — the JSON lane, a scalar column, or a CSV cell that is
/// not text (an empty field decodes to `null`, and `compare_value` is what
/// should name that shape mismatch).
fn csv_decoded(
    gv: &Value,
    cv: &Value,
    egress: Egress,
    path: &str,
    skips: &SkipPaths<'_>,
) -> Result<Option<Value>, Refusal> {
    if egress != Egress::Csv || !matches!(gv, Value::Array(_) | Value::Object(_)) {
        return Ok(None);
    }
    if let Some(why) = csv_container::ambiguity(gv) {
        return Err(Refusal::Ambiguous(why));
    }
    let Value::String(text) = cv else {
        return Ok(None);
    };
    // The decoder is given the exclusion set so an EXCLUDED member is left as raw
    // text instead of being required to invert the grammar. Without it a single
    // excluded inner field fails the whole cell, which is what forced
    // `udt_nested`'s exclusion to be whole-column (review finding F5).
    csv_container::decode_at(gv, text, path, &|p: &str| skips.excludes(p))
        .map(Some)
        .map_err(Refusal::Unparseable)
}

/// A total, side-independent ordering key: the canonical primary key, then the
/// whole canonicalized row as a tie-break so pairing stays deterministic even if
/// a fixture ever carried duplicate keys.
fn row_sort_key(row: &Row, pk: &[&str], ck: &[&str], egress: Egress) -> String {
    let mut parts: Vec<String> = Vec::new();
    for name in pk.iter().chain(ck.iter()) {
        parts.push(describe(row.get(*name).unwrap_or(&Value::Null), egress));
    }
    parts.push("|".to_string());
    for (name, value) in row {
        parts.push(format!("{name}={}", describe(value, egress)));
    }
    parts.join("\u{1}")
}

/// The primary key alone, for diagnostics. Deliberately NOT [`row_sort_key`]: that
/// one appends the whole row so pairing stays total, which would put a 4 KiB blob
/// into every failure message.
fn row_message_key(row: &Row, pk: &[&str], ck: &[&str], egress: Egress) -> String {
    pk.iter()
        .chain(ck.iter())
        .map(|name| {
            format!(
                "{name}={}",
                brief(&describe(row.get(*name).unwrap_or(&Value::Null), egress))
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Truncate a rendering for a diagnostic. Failure messages have to be READABLE:
/// the tables here carry 4 KiB blobs and 300-character text payloads, and an
/// untruncated diff of 64 such cells buries the one fact the reader needs.
fn brief(s: &str) -> String {
    const LIMIT: usize = 120;
    if s.chars().count() <= LIMIT {
        return s.to_string();
    }
    let head: String = s.chars().take(LIMIT).collect();
    format!("{head}…({} chars total)", s.chars().count())
}

/// A stable textual description of any value, for ordering and diagnostics only.
fn describe(value: &Value, egress: Egress) -> String {
    match value {
        Value::Array(items) => format!(
            "[{}]",
            items
                .iter()
                .map(|v| describe(v, egress))
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Object(fields) => format!(
            "{{{}}}",
            fields
                .iter()
                .map(|(k, v)| format!("{k}:{}", describe(v, egress)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        scalar => match canon_scalar(scalar, egress) {
            Ok(canon) => canon.describe(),
            Err(why) => format!("<{why}>"),
        },
    }
}

/// Compare one golden value against one CLI value, under the column's DECLARED
/// CQL type, with no exclusions — the entry point unit tests use.
pub fn compare_value(
    golden: &Value,
    cli: &Value,
    egress: Egress,
    ty: &CqlType,
) -> Result<(), String> {
    compare_value_at(
        golden,
        cli,
        egress,
        ty,
        Depth::TopLevel,
        "",
        &SkipPaths::new(&[]),
    )
}

/// The recursive worker.
///
/// The type drives the whole walk: it says which shape each side must have and,
/// at the leaves, which canonicalization applies. Types are threaded through
/// nesting so a `text` map value or UDT field is compared exactly even when its
/// content looks numeric.
///
/// `depth` is what CSV's empty-field rule keys on (see [`super::Depth`]), and
/// `path` is the fully-qualified position of this value in the row, which is how
/// a `SkipPaths` entry can name one UDT field rather than a whole column.
fn compare_value_at(
    golden: &Value,
    cli: &Value,
    egress: Egress,
    ty: &CqlType,
    depth: Depth,
    path: &str,
    skips: &SkipPaths<'_>,
) -> Result<(), String> {
    if skips.excludes(path) {
        return Ok(());
    }
    // A column that is absent/null on BOTH sides, whatever its declared shape.
    if matches!(golden, Value::Null) && matches!(cli, Value::Null) {
        return Ok(());
    }
    match ty {
        // set / list / frozen collection: same shape both sides, order-sensitive.
        // Cassandra emits a collection in comparator order and the CLI reads it in
        // storage order, so a reordering IS a divergence, not a normalization.
        CqlType::List(element) | CqlType::Set(element) => {
            let (g, c) = arrays(golden, cli, egress, ty)?;
            if g.len() != c.len() {
                return Err(format!(
                    "collection length golden {} vs cli {} (golden={}, cli={})",
                    g.len(),
                    c.len(),
                    brief(&describe(golden, egress)),
                    brief(&describe(cli, egress))
                ));
            }
            for (i, (gi, ci)) in g.iter().zip(c.iter()).enumerate() {
                compare_value_at(
                    gi,
                    ci,
                    egress,
                    element,
                    Depth::Inside,
                    &index_path(path, &i.to_string()),
                    skips,
                )
                .map_err(|why| format!("[{i}] {why}"))?;
            }
            Ok(())
        }
        CqlType::Tuple(items) => {
            let (g, c) = arrays(golden, cli, egress, ty)?;
            if g.len() != items.len() || c.len() != items.len() {
                return Err(format!(
                    "tuple arity golden {} vs cli {} but the schema declares {} field(s)",
                    g.len(),
                    c.len(),
                    items.len()
                ));
            }
            for (i, ((gi, ci), ity)) in g.iter().zip(c.iter()).zip(items.iter()).enumerate() {
                compare_value_at(
                    gi,
                    ci,
                    egress,
                    ity,
                    Depth::Inside,
                    &index_path(path, &i.to_string()),
                    skips,
                )
                .map_err(|why| format!("[{i}] {why}"))?;
            }
            Ok(())
        }
        // map: object in the dump, array of {"key","value"} pairs in the CLI (and
        // the CSV decoder produces that same pair spelling).
        CqlType::Map(key_ty, value_ty) => match (golden, cli) {
            (Value::Object(g), Value::Array(c)) => {
                compare_map(g, c, egress, key_ty, value_ty, path, skips)
            }
            _ => Err(shape_error("map", golden, cli, egress)),
        },
        CqlType::Udt(udt) => match golden {
            Value::Object(g) => compare_udt(g, cli, egress, udt, path, skips),
            _ => Err(shape_error(&udt.name, golden, cli, egress)),
        },
        // A scalar type: both sides canonicalized UNDER THAT TYPE, so the numeric
        // rule applies only where the DDL declares a number.
        _ => {
            let g = canon_typed(golden, egress, ty, depth)?;
            let c = canon_typed(cli, egress, ty, depth)?;
            if g == c {
                Ok(())
            } else {
                Err(format!(
                    "golden {} vs cli {} (declared {})",
                    brief(&g.describe()),
                    brief(&c.describe()),
                    ty.describe()
                ))
            }
        }
    }
}

/// Both sides as arrays, or an error naming the declared type.
fn arrays<'v>(
    golden: &'v Value,
    cli: &'v Value,
    egress: Egress,
    ty: &CqlType,
) -> Result<(&'v Vec<Value>, &'v Vec<Value>), String> {
    match (golden, cli) {
        (Value::Array(g), Value::Array(c)) => Ok((g, c)),
        _ => Err(shape_error(&ty.describe(), golden, cli, egress)),
    }
}

fn shape_error(expected: &str, golden: &Value, cli: &Value, egress: Egress) -> String {
    format!(
        "the schema declares `{expected}`, but golden={} and cli={} are not both that shape",
        brief(&describe(golden, egress)),
        brief(&describe(cli, egress))
    )
}

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
///     and the golden does not carry (dropped from the CLI side only). A
///     `{key,value}` pair array is the CLI's *map* spelling, so accepting one here
///     would let a UDT that regressed to the map representation pass; it is
///     therefore rejected (issue #1491 review finding F3).
///   * **CSV** — a `{key,value}` list, and only that. CSV delivers the whole cell
///     as one flat `{k: v, …}` text carrying nothing that could distinguish a map
///     from a UDT, so [`super::csv_container`] decodes EVERY brace-delimited body
///     into the pair spelling. An object on this side would mean the decoder was
///     bypassed.
///
/// Field NAMES must agree between the two sides, and each name must be one the
/// `CREATE TYPE` declares — an undeclared field name has no declared type, and a
/// value with no declared type is never compared permissively.
fn compare_udt(
    golden: &Map<String, Value>,
    cli: &Value,
    egress: Egress,
    udt: &UdtType,
    path: &str,
    skips: &SkipPaths<'_>,
) -> Result<(), String> {
    let c: Map<String, Value> = match (egress, cli) {
        (Egress::Json, Value::Object(fields)) => fields
            .iter()
            .filter(|(k, _)| k.as_str() != "_type")
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
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
                out.insert(name.clone(), value.clone());
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
        let cv = c.get(field).unwrap_or(&Value::Null);
        compare_value_at(
            gv,
            cv,
            egress,
            field_ty,
            Depth::Inside,
            &field_path(path, field),
            skips,
        )
        .map_err(|why| format!(".{field} {why}"))?;
    }
    Ok(())
}

/// One `{"key":…,"value":…}` entry of the CLI's map/UDT spelling, with the key
/// left as the RAW value.
///
/// Deliberately does NOT stringify the key: doing so applied a text projection
/// before the declared key type could be applied, so a `map<text,…>` golden key
/// `"0"` compared equal to an incorrectly emitted JSON numeric key `0` — defeating
/// the typed comparison in the one place a map most needs it (issue #1491 review
/// finding F2).
fn pair<'v>(entry: &'v Value, egress: Egress) -> Result<(&'v Value, &'v Value), String> {
    let object = entry.as_object().ok_or_else(|| {
        format!(
            "cli map entry is not an object: {}",
            describe(entry, egress)
        )
    })?;
    if object.len() != 2 || !object.contains_key("key") || !object.contains_key("value") {
        return Err(format!(
            "cli map entry is not a {{key,value}} pair: {}",
            brief(&describe(entry, egress))
        ));
    }
    let key = object.get("key").unwrap_or(&Value::Null);
    let value = object.get("value").unwrap_or(&Value::Null);
    Ok((key, value))
}

/// Is this a type whose values are single scalars? Map keys are paired by their
/// canonical scalar form, so a container key has no pairing rule here.
fn is_scalar_type(ty: &CqlType) -> bool {
    !matches!(
        ty,
        CqlType::List(_) | CqlType::Set(_) | CqlType::Map(..) | CqlType::Tuple(_) | CqlType::Udt(_)
    )
}

/// Compare a map: golden object vs the CLI's `{key,value}` pair list, paired by a
/// key canonicalized UNDER THE DECLARED KEY TYPE — so a `map<int,…>` pairs the
/// golden's `"-5"` with the CLI's `-5`, while a `map<text,…>` compares its keys
/// exactly AND by JSON kind, so a numeric key `0` does not satisfy the golden's
/// `"0"`.
///
/// The golden's keys are JSON object keys, hence always strings; the CLI's keep
/// whatever kind the egress gave them. Both go through the same
/// `canon_typed(…, key_ty, …)`, which is what makes the kind comparison possible.
fn compare_map(
    golden: &Map<String, Value>,
    cli: &[Value],
    egress: Egress,
    key_ty: &CqlType,
    value_ty: &CqlType,
    path: &str,
    skips: &SkipPaths<'_>,
) -> Result<(), String> {
    if !is_scalar_type(key_ty) {
        return Err(format!(
            "the schema declares the map key type `{}`, which is not a scalar — this lane \
             pairs map keys by their canonical scalar form and has no rule for a container \
             key",
            key_ty.describe()
        ));
    }
    // A key canonicalization FAILURE is propagated, never folded into the sort
    // key: a `<reason>` string would still pair with an identical `<reason>` on
    // the other side and compare equal.
    let canon_key = |v: &Value| -> Result<String, String> {
        canon_typed(v, egress, key_ty, Depth::Inside).map(|canon| canon.describe())
    };
    let mut g: Vec<(String, &Value)> = Vec::with_capacity(golden.len());
    for (k, v) in golden {
        g.push((canon_key(&Value::String(k.clone()))?, v));
    }
    let mut c: Vec<(String, &Value)> = Vec::with_capacity(cli.len());
    for entry in cli {
        let (key, value) = pair(entry, egress)?;
        c.push((canon_key(key)?, value));
    }
    if g.len() != c.len() {
        return Err(format!("map size golden {} vs cli {}", g.len(), c.len()));
    }
    g.sort_by(|a, b| a.0.cmp(&b.0));
    c.sort_by(|a, b| a.0.cmp(&b.0));
    for ((gk, gv), (ck, cv)) in g.iter().zip(c.iter()) {
        if gk != ck {
            return Err(format!("map key golden {gk} vs cli {ck}"));
        }
        compare_value_at(
            gv,
            cv,
            egress,
            value_ty,
            Depth::Inside,
            &index_path(path, gk),
            skips,
        )
        .map_err(|why| format!("[{gk}] {why}"))?;
    }
    Ok(())
}

// ===========================================================================
// Reading the CLI's own egress back
// ===========================================================================

/// Parse `export --format json` output: a JSON array of row objects.
pub fn cli_json_rows(text: &str) -> Result<Vec<Row>, String> {
    let parsed: Value = serde_json::from_str(text).map_err(|e| format!("invalid JSON: {e}"))?;
    let array = parsed
        .as_array()
        .ok_or_else(|| "JSON egress is not an array".to_string())?;
    array
        .iter()
        .enumerate()
        .map(|(i, row)| {
            row.as_object()
                .map(|o| o.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                .ok_or_else(|| format!("JSON egress row {i} is not an object"))
        })
        .collect()
}

/// Parse `export --format csv` output. Every cell arrives as text; an EMPTY cell
/// becomes `null`, which is how the CLI writes an absent value.
pub fn cli_csv_rows(text: &str) -> Result<Vec<Row>, String> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(text.as_bytes());
    let headers: Vec<String> = reader
        .headers()
        .map_err(|e| format!("CSV header: {e}"))?
        .iter()
        .map(str::to_string)
        .collect();
    if headers.is_empty() {
        return Err("CSV egress has no header row".to_string());
    }
    let mut rows = Vec::new();
    for (i, record) in reader.records().enumerate() {
        let record = record.map_err(|e| format!("CSV record {i}: {e}"))?;
        if record.len() != headers.len() {
            return Err(format!(
                "CSV record {i} has {} fields, header has {}",
                record.len(),
                headers.len()
            ));
        }
        let mut row = Row::new();
        for (name, field) in headers.iter().zip(record.iter()) {
            let value = if field.is_empty() {
                Value::Null
            } else {
                Value::String(field.to_string())
            };
            row.insert(name.clone(), value);
        }
        rows.push(row);
    }
    Ok(rows)
}

// ===========================================================================
// Fixture staging
// ===========================================================================

/// The `<keyspace>/<table>-<uuid>` directory holding this table's SSTable, chosen
/// per TABLE by evidence (#3220), or an error naming every root searched.
pub fn fixture_dir(keyspace: &str, table: &str) -> Result<PathBuf, String> {
    let root = super::datasets_root::sstables_root_for_table(keyspace, table)
        .ok_or_else(|| super::datasets_root::describe_search(keyspace, table))?;
    let prefix = format!("{table}-");
    let mut matches: Vec<PathBuf> = std::fs::read_dir(root.join(keyspace))
        .map_err(|e| format!("cannot read {}: {e}", root.join(keyspace).display()))?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with(&prefix))
                    .unwrap_or(false)
                && has_data_db(p)
        })
        .collect();
    matches.sort();
    matches.into_iter().next().ok_or_else(|| {
        format!(
            "no {prefix}* directory with a *-Data.db under {}",
            root.join(keyspace).display()
        )
    })
}

fn has_data_db(dir: &Path) -> bool {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(Result::ok).any(|e| {
                e.file_name()
                    .to_str()
                    .map(|n| n.ends_with("-Data.db"))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

/// The `*-Data.db.jsonl` golden sitting beside the fixture's `*-Data.db`.
pub fn golden_path(fixture: &Path) -> Result<PathBuf, String> {
    let mut found: Vec<PathBuf> = std::fs::read_dir(fixture)
        .map_err(|e| format!("cannot read {}: {e}", fixture.display()))?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.ends_with("-Data.db.jsonl"))
                .unwrap_or(false)
        })
        .collect();
    found.sort();
    found
        .into_iter()
        .next()
        .ok_or_else(|| format!("no *-Data.db.jsonl golden in {}", fixture.display()))
}

/// Stage a `--data-dir` holding EXACTLY this one table, by copying the fixture's
/// component files into `<dest>/<keyspace>/<fixture-dir-name>/`.
///
/// One table per data dir keeps each case independent (a sibling table's
/// unparseable component cannot perturb it) and keeps the whole lane fast: CLI
/// ingestion walks one directory instead of the whole corpus, so ~50 CLI
/// invocations stay in the low seconds. Copied rather than symlinked so the lane
/// does not depend on `std::os::unix`.
pub fn stage_single_table(dest: &Path, keyspace: &str, fixture: &Path) -> Result<(), String> {
    let name = fixture
        .file_name()
        .ok_or_else(|| format!("{} has no final component", fixture.display()))?;
    let target = dest.join(keyspace).join(name);
    std::fs::create_dir_all(&target)
        .map_err(|e| format!("cannot create {}: {e}", target.display()))?;
    let entries = std::fs::read_dir(fixture)
        .map_err(|e| format!("cannot read {}: {e}", fixture.display()))?;
    let mut copied = 0usize;
    for entry in entries.filter_map(Result::ok) {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(file_name) = path.file_name() else {
            continue;
        };
        std::fs::copy(&path, target.join(file_name))
            .map_err(|e| format!("cannot copy {}: {e}", path.display()))?;
        copied += 1;
    }
    if copied == 0 {
        return Err(format!(
            "no component files copied from {}",
            fixture.display()
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "golden_value_compare_tests.rs"]
mod tests;
