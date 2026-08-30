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
//! Columns are compared over the UNION of the golden's and the CLI's column names,
//! with an absent name read as `null`. That is what makes "an absent cell renders
//! as `null`" testable without a schema: `sstabledump` simply omits a cell that was
//! never written, so a golden row's missing column IS a null, and if the CLI
//! renders a value there (or drops a column the golden has) the comparison fails
//! and names the column.

use super::{canon_scalar, Egress, Row};
use serde_json::{Map, Value};
use std::path::{Path, PathBuf};

/// The outcome of one table × one egress format.
#[derive(Debug, Default)]
pub struct Report {
    /// Human-readable divergences, each naming the row key and the column.
    pub diffs: Vec<String>,
    /// Cells actually value-compared. Zero on a non-empty table is a failure the
    /// caller must treat as such (a comparison that compared nothing is vacuous).
    pub compared_cells: usize,
    /// Container-valued cells the CSV lane deliberately did not compare (see the
    /// type-level rule in the parent module).
    pub skipped_container_cells: usize,
}

/// Pair rows by primary key and compare every column.
pub fn compare_rows(
    golden: &[Row],
    cli: &[Row],
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

    for (g, c) in golden.iter().zip(cli.iter()) {
        let key = row_message_key(g, pk, ck, egress);
        let mut columns: Vec<&String> = g.keys().chain(c.keys()).collect();
        columns.sort();
        columns.dedup();
        for column in columns {
            if skip_columns.contains(&column.as_str()) {
                continue;
            }
            let gv = g.get(column).unwrap_or(&Value::Null);
            let cv = c.get(column).unwrap_or(&Value::Null);
            if egress == Egress::Csv && matches!(gv, Value::Array(_) | Value::Object(_)) {
                report.skipped_container_cells += 1;
                continue;
            }
            report.compared_cells += 1;
            if let Err(why) = compare_value(gv, cv, egress) {
                report.diffs.push(format!("row[{key}].{column}: {why}"));
            }
        }
    }
    report
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

/// Compare one golden value against one CLI value.
pub fn compare_value(golden: &Value, cli: &Value, egress: Egress) -> Result<(), String> {
    match (golden, cli) {
        // set / list / frozen collection: same shape both sides, order-sensitive.
        // Cassandra emits a collection in comparator order and the CLI reads it in
        // storage order, so a reordering IS a divergence, not a normalization.
        (Value::Array(g), Value::Array(c)) => {
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
                compare_value(gi, ci, egress).map_err(|why| format!("[{i}] {why}"))?;
            }
            Ok(())
        }
        // map: object in the dump, array of {"key","value"} pairs in the CLI.
        (Value::Object(g), Value::Array(c)) => compare_map(g, c, egress),
        // UDT: object both sides; the CLI adds a `_type` discriminator.
        (Value::Object(g), Value::Object(c)) => {
            let c: Map<String, Value> = c
                .iter()
                .filter(|(k, _)| k.as_str() != "_type")
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            let mut missing: Vec<&String> = g.keys().filter(|k| !c.contains_key(*k)).collect();
            let mut extra: Vec<&String> = c.keys().filter(|k| !g.contains_key(*k)).collect();
            missing.sort();
            extra.sort();
            if !missing.is_empty() || !extra.is_empty() {
                return Err(format!(
                    "udt fields differ: absent from cli {missing:?}, absent from golden {extra:?}"
                ));
            }
            for (field, gv) in g {
                let cv = c.get(field).unwrap_or(&Value::Null);
                compare_value(gv, cv, egress).map_err(|why| format!(".{field} {why}"))?;
            }
            Ok(())
        }
        (Value::Array(_) | Value::Object(_), _) => Err(format!(
            "golden container vs non-container cli value (golden={}, cli={})",
            brief(&describe(golden, egress)),
            brief(&describe(cli, egress))
        )),
        (_, Value::Array(_) | Value::Object(_)) => Err(format!(
            "golden scalar vs cli container (golden={}, cli={})",
            brief(&describe(golden, egress)),
            brief(&describe(cli, egress))
        )),
        _ => {
            let g = canon_scalar(golden, egress)?;
            let c = canon_scalar(cli, egress)?;
            if g == c {
                Ok(())
            } else {
                Err(format!(
                    "golden {} vs cli {}",
                    brief(&g.describe()),
                    brief(&c.describe())
                ))
            }
        }
    }
}

fn compare_map(golden: &Map<String, Value>, cli: &[Value], egress: Egress) -> Result<(), String> {
    let mut g: Vec<(String, &Value)> = golden
        .iter()
        .map(|(k, v)| (describe(&Value::String(k.clone()), egress), v))
        .collect();
    let mut c: Vec<(String, &Value)> = Vec::new();
    for entry in cli {
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
        c.push((describe(key, egress), value));
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
        compare_value(gv, cv, egress).map_err(|why| format!("[{gk}] {why}"))?;
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
