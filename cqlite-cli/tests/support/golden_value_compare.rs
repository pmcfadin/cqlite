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
use super::{canon_scalar, canon_typed, csv_container, Egress, Row};
use serde_json::{Map, Value};
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
        report.diffs.extend(undeclared_columns(g, c, schema, &key, egress, &mut shape_seen));

        for column in &schema.columns {
            let name = column.name.as_str();
            if skip_columns.contains(&name) {
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
            let decoded = match csv_decoded(gv, cv, egress) {
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
                    report
                        .diffs
                        .push(format!("row[{key}].{name}: unparseable CSV container: {why}"));
                    continue;
                }
            };
            let cv = decoded.as_ref().unwrap_or(cv);
            report.compared_cells += 1;
            if matches!(gv, Value::Array(_) | Value::Object(_)) {
                report.container_cells += 1;
            }
            if let Err(why) = compare_value(gv, cv, egress, &column.ty) {
                report.diffs.push(format!("row[{key}].{name}: {why}"));
            }
        }
    }
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
fn csv_decoded(gv: &Value, cv: &Value, egress: Egress) -> Result<Option<Value>, Refusal> {
    if egress != Egress::Csv || !matches!(gv, Value::Array(_) | Value::Object(_)) {
        return Ok(None);
    }
    if let Some(why) = csv_container::ambiguity(gv) {
        return Err(Refusal::Ambiguous(why));
    }
    let Value::String(text) = cv else {
        return Ok(None);
    };
    csv_container::decode(gv, text)
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
/// CQL type.
///
/// The type drives the whole walk: it says which shape each side must have and,
/// at the leaves, which canonicalization applies. Types are threaded through
/// nesting so a `text` map value or UDT field is compared exactly even when its
/// content looks numeric.
pub fn compare_value(
    golden: &Value,
    cli: &Value,
    egress: Egress,
    ty: &CqlType,
) -> Result<(), String> {
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
                compare_value(gi, ci, egress, element).map_err(|why| format!("[{i}] {why}"))?;
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
                compare_value(gi, ci, egress, ity).map_err(|why| format!("[{i}] {why}"))?;
            }
            Ok(())
        }
        // map: object in the dump, array of {"key","value"} pairs in the CLI (and
        // the CSV decoder produces that same pair spelling).
        CqlType::Map(key_ty, value_ty) => match (golden, cli) {
            (Value::Object(g), Value::Array(c)) => compare_map(g, c, egress, key_ty, value_ty),
            _ => Err(shape_error("map", golden, cli, egress)),
        },
        CqlType::Udt(udt) => match golden {
            Value::Object(g) => compare_udt(g, cli, egress, udt),
            _ => Err(shape_error(&udt.name, golden, cli, egress)),
        },
        // A scalar type: both sides canonicalized UNDER THAT TYPE, so the numeric
        // rule applies only where the DDL declares a number.
        _ => {
            let g = canon_typed(golden, egress, ty)?;
            let c = canon_typed(cli, egress, ty)?;
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

/// A UDT: an object in the dump; an object with an added `_type` discriminator in
/// the JSON egress, and the `{key,value}` pair spelling once decoded from CSV.
///
/// Field NAMES must agree between the two sides (that check is unchanged), and
/// each name must be one the `CREATE TYPE` declares — an undeclared field name has
/// no declared type, and a value with no declared type is never compared
/// permissively.
fn compare_udt(
    golden: &Map<String, Value>,
    cli: &Value,
    egress: Egress,
    udt: &UdtType,
) -> Result<(), String> {
    let c: Map<String, Value> = match cli {
        Value::Object(fields) => fields
            .iter()
            .filter(|(k, _)| k.as_str() != "_type")
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        Value::Array(entries) => {
            let mut out = Map::new();
            for entry in entries {
                let (key, value) = pair(entry, egress)?;
                out.insert(key, value.clone());
            }
            out
        }
        other => {
            return Err(format!(
                "the schema declares the UDT `{}`, but the cli value {} is neither an \
                 object nor a {{key,value}} list",
                udt.name,
                brief(&describe(other, egress))
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
        compare_value(gv, cv, egress, field_ty).map_err(|why| format!(".{field} {why}"))?;
    }
    Ok(())
}

/// One `{"key":…,"value":…}` entry of the CLI's map/UDT spelling.
fn pair(entry: &Value, egress: Egress) -> Result<(String, &Value), String> {
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
    let key = match key {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    Ok((key, value))
}

/// Compare a map: golden object vs the CLI's `{key,value}` pair list, paired by a
/// key canonicalized UNDER THE DECLARED KEY TYPE — so a `map<int,…>` pairs the
/// golden's `"-5"` with the CLI's `-5`, while a `map<text,…>` compares its keys
/// exactly.
fn compare_map(
    golden: &Map<String, Value>,
    cli: &[Value],
    egress: Egress,
    key_ty: &CqlType,
    value_ty: &CqlType,
) -> Result<(), String> {
    let canon_key = |v: &Value| -> String {
        match canon_typed(v, egress, key_ty) {
            Ok(canon) => canon.describe(),
            Err(why) => format!("<{why}>"),
        }
    };
    let mut g: Vec<(String, &Value)> = golden
        .iter()
        .map(|(k, v)| (canon_key(&Value::String(k.clone())), v))
        .collect();
    let mut c: Vec<(String, &Value)> = Vec::new();
    for entry in cli {
        let (key, value) = pair(entry, egress)?;
        c.push((canon_key(&Value::String(key)), value));
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
        compare_value(gv, cv, egress, value_ty).map_err(|why| format!("[{gk}] {why}"))?;
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
mod tests {
    use super::*;
    use serde_json::json;

    fn row(pairs: &[(&str, Value)]) -> Row {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    /// A schema for the unit cases, parsed from DDL exactly as the lane parses the
    /// committed files — so these cases exercise the real authority, not a mock.
    fn schema_of(ddl: &str, table: &str) -> TableSchema {
        match super::super::schema::from_ddl(ddl, table) {
            Ok(schema) => schema,
            Err(why) => panic!("{table}: {why}"),
        }
    }

    fn set_schema() -> TableSchema {
        schema_of("CREATE TABLE t (id int PRIMARY KEY, s set<text>);", "t")
    }

    /// The refusal PATH, not just the predicate: no corpus fixture carries a
    /// `, `-bearing collection member, so without this the wiring from
    /// `csv_container::ambiguity` to the census counters never executes and the
    /// lane's `0 REFUSED` line would be unfalsifiable.
    #[test]
    fn a_csv_unrepresentable_container_is_refused_and_named() {
        let schema = set_schema();
        let golden = vec![row(&[("id", json!(1)), ("s", json!(["a, b"]))])];
        // The CLI text is IRRELEVANT to the refusal: it is decided from the
        // golden alone, so the defect under test can never cause it.
        let cli = vec![row(&[("id", json!("1")), ("s", json!("{a, b}"))])];
        let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Csv);

        assert!(
            report.diffs.is_empty(),
            "unexpected diffs: {:?}",
            report.diffs
        );
        assert_eq!(report.ambiguous_container_cells, 1);
        assert_eq!(
            report.container_cells, 0,
            "a refused cell is not a compared one"
        );
        assert_eq!(report.compared_cells, 1, "`id` is still compared");
        assert_eq!(report.ambiguity_reasons.len(), 1);
        assert!(
            report.ambiguity_reasons[0].starts_with("s ("),
            "the refusal must name its column: {:?}",
            report.ambiguity_reasons
        );
    }

    /// A representable container is compared, and a wrong member fails. Pins the
    /// other side of the same branch so "refused" can never quietly become the
    /// default.
    #[test]
    fn a_representable_container_is_compared_and_a_wrong_member_fails() {
        let schema = set_schema();
        let golden = vec![row(&[("id", json!(1)), ("s", json!(["a", "b"]))])];
        let good = vec![row(&[("id", json!("1")), ("s", json!("{a, b}"))])];
        let report = compare_rows(&golden, &good, &schema, &["id"], &[], &[], Egress::Csv);
        assert!(
            report.diffs.is_empty(),
            "unexpected diffs: {:?}",
            report.diffs
        );
        assert_eq!(report.container_cells, 1);
        assert_eq!(report.ambiguous_container_cells, 0);

        let bad = vec![row(&[("id", json!("1")), ("s", json!("{a, c}"))])];
        let report = compare_rows(&golden, &bad, &schema, &["id"], &[], &[], Egress::Csv);
        assert_eq!(
            report.diffs.len(),
            1,
            "a wrong member must fail: {:?}",
            report.diffs
        );
        assert!(report.diffs[0].contains(".s:"), "{:?}", report.diffs);
    }

    /// A CLI cell that is not the grammar at all is a DIVERGENCE, not a refusal.
    #[test]
    fn an_unparseable_container_is_reported_not_refused() {
        let schema = set_schema();
        let golden = vec![row(&[("id", json!(1)), ("s", json!(["a", "b"]))])];
        let cli = vec![row(&[("id", json!("1")), ("s", json!("a, b"))])];
        let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Csv);
        assert_eq!(report.ambiguous_container_cells, 0);
        assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
        assert!(
            report.diffs[0].contains("unparseable CSV container"),
            "{:?}",
            report.diffs
        );
    }

    // =======================================================================
    // The column set is the DDL's, and only the golden may default to null
    // =======================================================================

    const ABSENT_DDL: &str =
        "CREATE TABLE t (pk int, ck int, anchor text, reg text, PRIMARY KEY (pk, ck));";

    fn absent_schema() -> TableSchema {
        schema_of(ABSENT_DDL, "t")
    }

    fn absent_golden() -> Vec<Row> {
        // The `sstabledump` shape of `test_types.nb_absent_vs_null_regular` row 1:
        // `reg` was never written, so the physical dump simply has no such cell.
        vec![row(&[
            ("pk", json!(1)),
            ("ck", json!(1)),
            ("anchor", json!("anchor_absent")),
        ])]
    }

    /// The property `nb_absent_vs_null_regular` exists for: an absent cell must be
    /// RENDERED as null. Reading an omitted egress column as null (the first cut of
    /// this file) made it unfalsifiable.
    #[test]
    fn a_column_the_egress_omits_is_a_named_failure() {
        let schema = absent_schema();
        let golden = absent_golden();

        let rendered = vec![row(&[
            ("pk", json!(1)),
            ("ck", json!(1)),
            ("anchor", json!("anchor_absent")),
            ("reg", Value::Null),
        ])];
        let report = compare_rows(&golden, &rendered, &schema, &["pk"], &["ck"], &[], Egress::Json);
        assert!(
            report.diffs.is_empty(),
            "an absent golden cell rendered as null is the expected outcome: {:?}",
            report.diffs
        );
        assert_eq!(report.compared_cells, 4, "every declared column is compared");

        // The regression: the egress drops the column entirely.
        let omitted = vec![row(&[
            ("pk", json!(1)),
            ("ck", json!(1)),
            ("anchor", json!("anchor_absent")),
        ])];
        let report = compare_rows(&golden, &omitted, &schema, &["pk"], &["ck"], &[], Egress::Json);
        assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
        assert!(
            report.diffs[0].contains("reg") && report.diffs[0].contains("absent from the"),
            "the failure must name the omitted column: {:?}",
            report.diffs
        );
    }

    /// The mirror image: a column the DDL does not declare must not pass just
    /// because it holds `null`.
    #[test]
    fn a_spurious_extra_column_is_a_named_failure() {
        let schema = absent_schema();
        let golden = absent_golden();
        let extra = vec![row(&[
            ("pk", json!(1)),
            ("ck", json!(1)),
            ("anchor", json!("anchor_absent")),
            ("reg", Value::Null),
            ("ghost", Value::Null),
        ])];
        let report = compare_rows(&golden, &extra, &schema, &["pk"], &["ck"], &[], Egress::Json);
        assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
        assert!(
            report.diffs[0].contains("ghost") && report.diffs[0].contains("does not declare"),
            "the failure must name the undeclared column: {:?}",
            report.diffs
        );
    }

    /// A value where the golden has no cell at all is still a divergence — the
    /// golden's absence is an expected NULL, not a wildcard.
    #[test]
    fn a_value_where_the_golden_has_no_cell_still_fails() {
        let schema = absent_schema();
        let golden = absent_golden();
        let invented = vec![row(&[
            ("pk", json!(1)),
            ("ck", json!(1)),
            ("anchor", json!("anchor_absent")),
            ("reg", json!("invented")),
        ])];
        let report = compare_rows(
            &golden,
            &invented,
            &schema,
            &["pk"],
            &["ck"],
            &[],
            Egress::Json,
        );
        assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
        assert!(
            report.diffs[0].contains(".reg:") && report.diffs[0].contains("invented"),
            "{:?}",
            report.diffs
        );
    }

    /// A golden cell for a column the named schema does not declare means the
    /// expectation itself is stale, so it is a failure rather than silent coverage.
    #[test]
    fn a_golden_cell_for_an_undeclared_column_is_a_named_failure() {
        let schema = absent_schema();
        let golden = vec![row(&[
            ("pk", json!(1)),
            ("ck", json!(1)),
            ("anchor", json!("a")),
            ("dropped", json!("x")),
        ])];
        let cli = vec![row(&[
            ("pk", json!(1)),
            ("ck", json!(1)),
            ("anchor", json!("a")),
            ("reg", Value::Null),
        ])];
        let report = compare_rows(&golden, &cli, &schema, &["pk"], &["ck"], &[], Egress::Json);
        assert!(
            report
                .diffs
                .iter()
                .any(|d| d.contains("dropped") && d.contains("does not declare")),
            "{:?}",
            report.diffs
        );
    }

    /// A shape divergence is reported ONCE per column, not once per row — the
    /// tables in this lane run to 900 rows.
    #[test]
    fn a_column_shape_divergence_is_reported_once_per_column() {
        let schema = absent_schema();
        let golden: Vec<Row> = (1..=5)
            .map(|i| {
                row(&[
                    ("pk", json!(1)),
                    ("ck", json!(i)),
                    ("anchor", json!("a")),
                    ("reg", json!("v")),
                ])
            })
            .collect();
        let cli: Vec<Row> = (1..=5)
            .map(|i| {
                row(&[
                    ("pk", json!(1)),
                    ("ck", json!(i)),
                    ("anchor", json!("a")),
                ])
            })
            .collect();
        let report = compare_rows(&golden, &cli, &schema, &["pk"], &["ck"], &[], Egress::Json);
        assert_eq!(
            report.diffs.len(),
            1,
            "5 rows missing the same column must report once: {:?}",
            report.diffs
        );
    }

    /// A declared `skip_columns` entry still suppresses its column, so the
    /// measured-divergence gaps keep working.
    #[test]
    fn a_declared_skip_column_is_not_required_to_be_rendered() {
        let schema = absent_schema();
        let golden = absent_golden();
        let omitted = vec![row(&[
            ("pk", json!(1)),
            ("ck", json!(1)),
            ("anchor", json!("anchor_absent")),
        ])];
        let report = compare_rows(
            &golden,
            &omitted,
            &schema,
            &["pk"],
            &["ck"],
            &["reg"],
            Egress::Json,
        );
        assert!(
            report.diffs.is_empty(),
            "a declared skip must stay declared: {:?}",
            report.diffs
        );
        assert_eq!(report.compared_cells, 3);
    }

    // =======================================================================
    // Types come from the DDL: a numeric-looking text is NOT a number
    // =======================================================================

    // One DDL per shape: the comparison now REQUIRES every declared column to be
    // rendered, so a schema carrying columns a case does not exercise would fail
    // for the right reason in the wrong test.
    const TEXT_DDL: &str = "CREATE TABLE t (id int PRIMARY KEY, zip text);";
    const NUM_DDL: &str = "CREATE TABLE t (id int PRIMARY KEY, n int);";
    const UDT_MAP_DDL: &str = "CREATE TYPE address (street text, city text, zip text); \
         CREATE TABLE t (id int PRIMARY KEY, ma map<text, frozen<address>>);";
    const INT_MAP_DDL: &str = "CREATE TABLE t (id int PRIMARY KEY, mi map<int, text>);";

    /// BLOCKER 2: a CQL `text` value holding `\"22201\"` must NOT compare equal to
    /// the JSON number `22201`, and `\"00000\"` must not equal `\"0\"`.
    #[test]
    fn a_numeric_looking_text_column_is_compared_exactly() {
        let schema = schema_of(TEXT_DDL, "t");
        let golden = vec![row(&[("id", json!(1)), ("zip", json!("22201"))])];

        let same = vec![row(&[("id", json!(1)), ("zip", json!("22201"))])];
        let report = compare_rows(&golden, &same, &schema, &["id"], &[], &[], Egress::Json);
        assert!(report.diffs.is_empty(), "{:?}", report.diffs);

        for wrong in [json!(22201), json!("22201.0"), json!("022201")] {
            let cli = vec![row(&[("id", json!(1)), ("zip", wrong.clone())])];
            let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Json);
            assert_eq!(
                report.diffs.len(),
                1,
                "text {wrong} must not equal the golden text \"22201\": {:?}",
                report.diffs
            );
            assert!(report.diffs[0].contains(".zip:"), "{:?}", report.diffs);
        }

        // Zero padding, the second half of the finding.
        let padded = vec![row(&[("id", json!(1)), ("zip", json!("00000"))])];
        let stripped = vec![row(&[("id", json!(1)), ("zip", json!("0"))])];
        let report = compare_rows(&padded, &stripped, &schema, &["id"], &[], &[], Egress::Json);
        assert_eq!(
            report.diffs.len(),
            1,
            "\"00000\" must not equal \"0\": {:?}",
            report.diffs
        );
    }

    /// The same strictness one level in: a `text` UDT field nested inside a map
    /// value. This is the shape the `udt_collections` fixture actually carries
    /// (`ma frozen<map<text, frozen<address>>>`, zip `\"22201\"`/`\"00000\"`).
    #[test]
    fn a_numeric_looking_text_udt_field_is_compared_exactly() {
        let schema = schema_of(UDT_MAP_DDL, "t");
        let golden = vec![row(&[
            ("id", json!(1)),
            (
                "ma",
                json!({"home": {"street": "1 Navy Way", "city": "Arlington", "zip": "00000"}}),
            ),
        ])];
        let cli_ok = vec![row(&[
            ("id", json!(1)),
            (
                "ma",
                json!([{"key": "home", "value": {"_type": "address", "street": "1 Navy Way",
                        "city": "Arlington", "zip": "00000"}}]),
            ),
        ])];
        let report = compare_rows(&golden, &cli_ok, &schema, &["id"], &[], &[], Egress::Json);
        assert!(report.diffs.is_empty(), "{:?}", report.diffs);
        assert_eq!(report.container_cells, 1);

        for wrong in [json!(0), json!("0")] {
            let cli = vec![row(&[
                ("id", json!(1)),
                (
                    "ma",
                    json!([{"key": "home", "value": {"_type": "address",
                            "street": "1 Navy Way", "city": "Arlington", "zip": wrong}}]),
                ),
            ])];
            let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Json);
            assert_eq!(
                report.diffs.len(),
                1,
                "a nested text zip {wrong} must not equal \"00000\": {:?}",
                report.diffs
            );
            assert!(report.diffs[0].contains("zip"), "{:?}", report.diffs);
        }
    }

    /// The normalization that must SURVIVE: a numeric column's golden spelling is a
    /// string (a partition key, a collection path) and the CLI's is a number.
    #[test]
    fn a_numeric_column_still_compares_across_spellings() {
        let schema = schema_of(NUM_DDL, "t");
        let golden = vec![row(&[("id", json!("1")), ("n", json!("-5"))])];
        let cli = vec![row(&[("id", json!(1)), ("n", json!(-5))])];
        let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Json);
        assert!(
            report.diffs.is_empty(),
            "a numeric column must still pair a string spelling with a number: {:?}",
            report.diffs
        );

        // And a WRONG number still fails.
        let wrong = vec![row(&[("id", json!(1)), ("n", json!(-6))])];
        let report = compare_rows(&golden, &wrong, &schema, &["id"], &[], &[], Egress::Json);
        assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
    }

    /// Map KEYS are canonicalized under the declared KEY type: numeric for
    /// `map<int,…>` (the dump renders every path as a string), exact for
    /// `map<text,…>`.
    #[test]
    fn map_keys_are_canonicalized_under_the_declared_key_type() {
        let schema = schema_of(INT_MAP_DDL, "t");
        let golden = vec![row(&[("id", json!(1)), ("mi", json!({"-5": "v"}))])];
        let cli = vec![row(&[
            ("id", json!(1)),
            ("mi", json!([{"key": -5, "value": "v"}])),
        ])];
        let report = compare_rows(&golden, &cli, &schema, &["id"], &[], &[], Egress::Json);
        assert!(report.diffs.is_empty(), "{:?}", report.diffs);

        let wrong = vec![row(&[
            ("id", json!(1)),
            ("mi", json!([{"key": -6, "value": "v"}])),
        ])];
        let report = compare_rows(&golden, &wrong, &schema, &["id"], &[], &[], Egress::Json);
        assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);

        // A text-keyed map compares its keys exactly.
        let schema = schema_of(UDT_MAP_DDL, "t");
        let golden = vec![row(&[
            ("id", json!(1)),
            (
                "ma",
                json!({"00000": {"street": "s", "city": "c", "zip": "z"}}),
            ),
        ])];
        let wrong = vec![row(&[
            ("id", json!(1)),
            (
                "ma",
                json!([{"key": 0, "value": {"street": "s", "city": "c", "zip": "z"}}]),
            ),
        ])];
        let report = compare_rows(&golden, &wrong, &schema, &["id"], &[], &[], Egress::Json);
        assert_eq!(
            report.diffs.len(),
            1,
            "a text map key \"00000\" must not equal the number 0: {:?}",
            report.diffs
        );
    }

    /// CSV keeps the same type rule: a `text` cell is exact even though every CSV
    /// field arrives as text.
    #[test]
    fn the_csv_lane_uses_the_declared_types_too() {
        let schema = schema_of(TEXT_DDL, "t");
        let golden = vec![row(&[("id", json!(1)), ("zip", json!("00000"))])];
        let ok = vec![row(&[("id", json!("1")), ("zip", json!("00000"))])];
        let report = compare_rows(&golden, &ok, &schema, &["id"], &[], &[], Egress::Csv);
        assert!(report.diffs.is_empty(), "{:?}", report.diffs);

        let wrong = vec![row(&[("id", json!("1")), ("zip", json!("0"))])];
        let report = compare_rows(&golden, &wrong, &schema, &["id"], &[], &[], Egress::Csv);
        assert_eq!(report.diffs.len(), 1, "{:?}", report.diffs);
        assert!(report.diffs[0].contains(".zip:"), "{:?}", report.diffs);
    }
}
