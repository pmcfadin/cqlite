//! The committed CQL schema as the AUTHORITY for a case's declaration (#1490
//! round 6).
//!
//! # The false PASS this closes
//!
//! Every [`ParityCase`](super::ParityCase) hand-copies its column list, its
//! column TYPES and its key definitions out of `test-data/schemas/*.cql`. Those
//! copies are the harness's ground truth: `arrow_expect` derives the EXPECTED
//! Arrow type from the case's declared CQL type (deliberately, since reading the
//! expectation out of CQLite's own mapping would be circular — #3041), and
//! `golden_rows` canonicalizes the sstabledump oracle by the same declaration.
//!
//! Nothing checked the copy. Only the column NAMES were verified — against the
//! exported Parquet schema, which is the code under test. So a declaration that
//! drifted (a typo, a schema change, or a type copied from the wrong table) to
//! MATCH an erroneous export mapping made the Arrow type check AND the value
//! comparison both pass: a case could be green about a column whose real
//! declared type nobody ever compared. That is a false PASS at the root of the
//! chain, so the declaration is now validated against the committed schema on
//! every run, fail-closed.
//!
//! # Why parsing the `.cql` here is not circular
//!
//! The `.cql` fixtures are the **Cassandra** schemas the corpus was written with
//! — primary source in the sense of the format-authority rule. What would be
//! circular is deriving the expectation from CQLite's parse of them:
//! `cqlite-core`'s schema loader is exactly what the export uses to build the
//! Arrow schema this harness is asserting against, so a defect in that loader
//! would appear on BOTH sides and cancel. This module therefore reads the
//! committed text itself, with a small parser that lives entirely in test
//! support and shares no code with the export path.
//!
//! # Fail-closed rules
//!
//! * An unreadable schema file, a table the schema does not declare, or a
//!   `CREATE TABLE` statement this parser cannot parse is an ERROR — never a
//!   skipped check. A check that quietly does nothing is the shape of the defect
//!   above.
//! * EVERY disagreement is reported, not the first: a drifted declaration
//!   usually drifts in a family (a table renamed, a type widened), and one
//!   message naming all of them is what makes the diagnosis possible.
//! * A comparison of type TEXT, normalized only for CASE and WHITESPACE
//!   (`MAP<TEXT, TEXT>` == `map<text,text>`). Deliberately NOT an equivalence up
//!   to CQL aliases: `varchar` and `text` denote the same type, but the rule the
//!   harness relies on is "copied from the committed schema", and a case spelling
//!   a column differently from its schema has not been copied from it.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

use super::datasets_root;
use super::ParityCase;

/// Whether a case's declaration is validated against its committed schema.
///
/// The opt-out exists for the harness's own SYNTHETIC controls: several of them
/// deliberately MIS-DECLARE a column so the Arrow type check can be shown to red
/// (`type_check_reds_on_a_wrong_arrow_type` and friends), and one names a schema
/// that deliberately does not declare the table so the real export aborts. Those
/// declarations are wrong ON PURPOSE, and a schema check would red on exactly
/// the property they exist to demonstrate.
///
/// It is spelled as a NAMED variant carrying a REQUIRED reason so a reader sees
/// the opt-out at the declaration site, and every opt-out is announced on stderr
/// on every run — never a silent exemption, and never a boolean whose `false`
/// could be a copy-paste accident.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaCheck {
    /// Validate the declared columns, types and key definitions against the
    /// committed `test-data/schemas/<schema>.cql`.
    Committed,
    /// A synthetic control whose declaration is deliberately not the schema's.
    Synthetic { why: &'static str },
}

/// One column as the committed schema declares it.
#[derive(Debug, Clone)]
pub struct SchemaColumn {
    pub name: String,
    /// The type text exactly as the schema spells it (`MAP<TEXT, TEXT>`).
    pub cql_type: String,
    pub is_static: bool,
}

/// One `CREATE TABLE` as the committed schema declares it.
#[derive(Debug, Clone, Default)]
pub struct SchemaTable {
    pub columns: Vec<SchemaColumn>,
    pub partition_key: Vec<String>,
    pub clustering: Vec<String>,
}

/// A whole parsed `.cql` fixture: its tables keyed by `(keyspace, table)`, and
/// the user-defined types it declares.
#[derive(Debug, Clone, Default)]
pub struct SchemaFile {
    pub tables: BTreeMap<(String, String), SchemaTable>,
    pub udts: BTreeSet<String>,
}

impl SchemaFile {
    pub fn table(&self, keyspace: &str, table: &str) -> Option<&SchemaTable> {
        self.tables
            .get(&(keyspace.to_ascii_lowercase(), table.to_ascii_lowercase()))
    }

    fn tables_in(&self, keyspace: &str) -> Vec<&str> {
        let ks = keyspace.to_ascii_lowercase();
        self.tables
            .keys()
            .filter(|(k, _)| *k == ks)
            .map(|(_, t)| t.as_str())
            .collect()
    }
}

/// Load and parse a committed schema fixture by file name (`da-test.cql`).
pub fn load(schema_file: &str) -> Result<SchemaFile, String> {
    let path = datasets_root::schema_path(schema_file).ok_or_else(|| {
        format!(
            "committed schema fixture '{schema_file}' is unreadable — the case's declaration \
             cannot be validated against it"
        )
    })?;
    let text =
        std::fs::read_to_string(&path).map_err(|e| format!("reading {}: {e}", path.display()))?;
    parse(&text).map_err(|e| format!("{}: {e}", path.display()))
}

/// Validate ONE case's declaration against its committed schema.
///
/// `Ok(n)` reports how many declared columns were verified (so a caller can
/// census the check rather than trust that it ran). `Err` names EVERY
/// disagreement.
pub fn validate_declaration(case: &ParityCase) -> Result<usize, String> {
    let schema = load(case.schema)?;
    let Some(table) = schema.table(case.keyspace, case.table) else {
        return Err(format!(
            "{}: the committed schema '{}' does not declare this table (it declares {:?} in \
             keyspace '{}') — the case's schema fixture or table name has drifted",
            case.id(),
            case.schema,
            schema.tables_in(case.keyspace),
            case.keyspace
        ));
    };

    let mut problems: Vec<String> = Vec::new();

    // --- column SET -------------------------------------------------------
    let declared: BTreeSet<String> = case
        .columns
        .iter()
        .map(|(n, _)| n.to_ascii_lowercase())
        .collect();
    let actual: BTreeSet<String> = table
        .columns
        .iter()
        .map(|c| c.name.to_ascii_lowercase())
        .collect();
    for missing in actual.difference(&declared) {
        problems.push(format!(
            "column '{missing}' is declared by the schema but NOT by the case — every schema \
             column is coverage, so an omitted column is an uncompared column"
        ));
    }
    for extra in declared.difference(&actual) {
        problems.push(format!(
            "column '{extra}' is declared by the case but NOT by the schema"
        ));
    }

    // --- column TYPES -----------------------------------------------------
    let mut verified = 0usize;
    for (name, declared_type) in case.columns {
        let lower = name.to_ascii_lowercase();
        let Some(col) = table.columns.iter().find(|c| c.name == lower) else {
            continue; // already reported as an extra column above
        };
        if normalize_type(declared_type) != normalize_type(&col.cql_type) {
            problems.push(format!(
                "column '{name}': the case declares '{declared_type}' but the committed schema \
                 declares '{}' — the declared type is what the Arrow type check and the golden \
                 canonicalization are BOTH derived from, so a drifted copy makes both pass",
                col.cql_type
            ));
        } else {
            verified += 1;
        }
    }

    // --- KEY definitions --------------------------------------------------
    let case_pk: Vec<String> = case
        .partition_key
        .iter()
        .map(|n| n.to_ascii_lowercase())
        .collect();
    if case_pk != table.partition_key {
        problems.push(format!(
            "partition key: the case declares {:?} but the committed schema declares {:?} \
             (ORDER is significant — it is the order the golden's `partition.key` array uses)",
            case.partition_key, table.partition_key
        ));
    }
    let case_ck: Vec<String> = case
        .clustering
        .iter()
        .map(|n| n.to_ascii_lowercase())
        .collect();
    if case_ck != table.clustering {
        problems.push(format!(
            "clustering key: the case declares {:?} but the committed schema declares {:?} \
             (ORDER is significant — it is the order the golden's `row.clustering` array uses)",
            case.clustering, table.clustering
        ));
    }

    // --- declared UDT names ------------------------------------------------
    // One-directional on purpose: a case need only name the UDTs its own columns
    // use, but a name the schema never declares means the case's `udts` list has
    // drifted, and `cql_type::parse_column` would then accept a UDT that does
    // not exist.
    for udt in case.udts {
        if !schema.udts.contains(&udt.to_ascii_lowercase()) {
            problems.push(format!(
                "declared UDT '{udt}' is not a CREATE TYPE in the committed schema (it declares \
                 {:?})",
                schema.udts
            ));
        }
    }

    if problems.is_empty() {
        return Ok(verified);
    }
    Err(format!(
        "{}: the case's declaration DISAGREES with its committed schema '{}' — the declaration \
         is the harness's ground truth (it is what the Arrow type expectation and the golden \
         canonicalization are derived from), so a drift here can make a wrong export mapping \
         compare EQUAL:\n  - {}",
        case.id(),
        case.schema,
        problems.join("\n  - ")
    ))
}

/// Lowercase and strip ALL whitespace, so `MAP<TEXT, TEXT>` == `map<text,text>`.
pub fn normalize_type(t: &str) -> String {
    t.chars()
        .filter(|c| !c.is_whitespace())
        .flat_map(|c| c.to_lowercase())
        .collect()
}

/// Parse a committed `.cql` fixture.
pub fn parse(text: &str) -> Result<SchemaFile, String> {
    let stripped = strip_comments(text);
    let mut out = SchemaFile::default();
    let mut keyspace = String::new();

    for stmt in split_statements(&stripped) {
        let stmt = stmt.trim();
        if stmt.is_empty() {
            continue;
        }
        let upper = stmt.to_ascii_uppercase();
        if upper.starts_with("USE ") {
            keyspace = identifier(&stmt["USE ".len()..]);
        } else if upper.starts_with("CREATE TYPE") {
            let after = strip_create(stmt, "CREATE TYPE");
            let open = after
                .find('(')
                .ok_or_else(|| format!("CREATE TYPE without a field list: {}", head(stmt)))?;
            out.udts.insert(identifier(&after[..open]));
        } else if upper.starts_with("CREATE TABLE") || upper.starts_with("CREATE COLUMNFAMILY") {
            let kw = if upper.starts_with("CREATE TABLE") {
                "CREATE TABLE"
            } else {
                "CREATE COLUMNFAMILY"
            };
            let after = strip_create(stmt, kw);
            let (name, body) = split_name_and_body(after)?;
            let (ks, table) = match name.rsplit_once('.') {
                Some((k, t)) => (identifier(k), identifier(t)),
                None => (keyspace.clone(), identifier(name)),
            };
            if ks.is_empty() {
                return Err(format!(
                    "table '{table}' is declared before any USE <keyspace> and is not \
                     keyspace-qualified, so the harness cannot tell which keyspace it belongs to"
                ));
            }
            let parsed = parse_table_body(&table, body)?;
            out.tables.insert((ks, table), parsed);
        }
        // Everything else (CREATE KEYSPACE, CREATE INDEX, INSERT, ALTER, …) is
        // irrelevant here. A CREATE TABLE the parser cannot handle is an ERROR
        // above, so an unrecognized statement can never silently hide a table.
    }
    Ok(out)
}

/// Drop the leading `CREATE TABLE`/`CREATE TYPE` keyword and an optional
/// `IF NOT EXISTS`, leaving `<name> ( … ) [WITH …]`.
fn strip_create<'a>(stmt: &'a str, keyword: &str) -> &'a str {
    let rest = stmt[keyword.len()..].trim_start();
    // `to_ascii_uppercase` is length-preserving, so the byte offsets agree.
    if rest.to_ascii_uppercase().starts_with("IF NOT EXISTS") {
        return rest["IF NOT EXISTS".len()..].trim_start();
    }
    rest
}

/// `simple_table ( … ) WITH …` → (`simple_table`, the column-list body).
fn split_name_and_body(s: &str) -> Result<(&str, &str), String> {
    let open = s
        .find('(')
        .ok_or_else(|| format!("CREATE TABLE without a column list: {}", head(s)))?;
    let mut depth = 0i32;
    let mut close = None;
    for (i, ch) in s[open..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    close = Some(open + i);
                    break;
                }
            }
            _ => {}
        }
    }
    let close = close.ok_or_else(|| {
        format!(
            "CREATE TABLE's column list has no matching ')': {}",
            head(s)
        )
    })?;
    Ok((s[..open].trim(), &s[open + 1..close]))
}

fn parse_table_body(table: &str, body: &str) -> Result<SchemaTable, String> {
    let mut out = SchemaTable::default();
    let mut inline_pk: Option<String> = None;
    let mut key_clause: Option<(Vec<String>, Vec<String>)> = None;

    for part in split_top_level(body) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let upper = part.to_ascii_uppercase();
        if upper.starts_with("PRIMARY KEY") {
            if key_clause.is_some() {
                return Err(format!("table '{table}' declares two PRIMARY KEY clauses"));
            }
            key_clause = Some(parse_key_clause(table, part)?);
            continue;
        }
        let (name, rest) = part
            .split_once(char::is_whitespace)
            .ok_or_else(|| format!("table '{table}': column definition '{part}' has no type"))?;
        let name = identifier(name);
        let mut rest = rest.trim();
        let mut is_static = false;
        // Trailing modifiers, in either legal spelling.
        loop {
            let upper = rest.to_ascii_uppercase();
            if let Some(stripped) = upper.strip_suffix("PRIMARY KEY") {
                if inline_pk.is_some() {
                    return Err(format!(
                        "table '{table}' declares two inline PRIMARY KEY columns"
                    ));
                }
                inline_pk = Some(name.clone());
                rest = rest[..stripped.len()].trim_end();
                continue;
            }
            if let Some(stripped) = upper.strip_suffix("STATIC") {
                is_static = true;
                rest = rest[..stripped.len()].trim_end();
                continue;
            }
            break;
        }
        if rest.is_empty() {
            return Err(format!("table '{table}': column '{name}' has no type text"));
        }
        out.columns.push(SchemaColumn {
            name,
            cql_type: rest.to_string(),
            is_static,
        });
    }

    match (key_clause, inline_pk) {
        (Some(_), Some(name)) => {
            return Err(format!(
                "table '{table}' declares both a PRIMARY KEY clause and an inline PRIMARY KEY \
                 on column '{name}'"
            ))
        }
        (Some((pk, ck)), None) => {
            out.partition_key = pk;
            out.clustering = ck;
        }
        (None, Some(name)) => out.partition_key = vec![name],
        (None, None) => {
            return Err(format!(
                "table '{table}' declares no PRIMARY KEY — the harness refuses to guess one"
            ))
        }
    }

    for key in out.partition_key.iter().chain(out.clustering.iter()) {
        if !out.columns.iter().any(|c| &c.name == key) {
            return Err(format!(
                "table '{table}': PRIMARY KEY names '{key}', which is not one of its columns"
            ));
        }
    }
    Ok(out)
}

/// `PRIMARY KEY ((a, b), c)` → (`[a, b]`, `[c]`); `PRIMARY KEY (a, b)` → (`[a]`, `[b]`).
fn parse_key_clause(table: &str, part: &str) -> Result<(Vec<String>, Vec<String>), String> {
    let open = part
        .find('(')
        .ok_or_else(|| format!("table '{table}': PRIMARY KEY without a component list"))?;
    let inner = part[open..]
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| format!("table '{table}': PRIMARY KEY list is not parenthesized: {part}"))?;
    let parts = split_top_level(inner);
    let mut parts = parts.into_iter().map(|p| p.trim().to_string());
    let first = parts
        .next()
        .ok_or_else(|| format!("table '{table}': PRIMARY KEY () is empty"))?;
    let clustering: Vec<String> = parts.map(|p| identifier(&p)).collect();
    if let Some(composite) = first.strip_prefix('(').and_then(|s| s.strip_suffix(')')) {
        let partition: Vec<String> = split_top_level(composite)
            .into_iter()
            .map(|p| identifier(&p))
            .collect();
        if partition.is_empty() {
            return Err(format!(
                "table '{table}': PRIMARY KEY's composite partition key is empty"
            ));
        }
        return Ok((partition, clustering));
    }
    Ok((vec![identifier(&first)], clustering))
}

/// Strip `--` line comments, honouring single-quoted strings (schema option maps
/// carry `'class': 'LZ4Compressor'`).
fn strip_comments(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for line in text.lines() {
        let mut in_quote = false;
        let bytes = line.as_bytes();
        let mut cut = line.len();
        let mut i = 0;
        while i < bytes.len() {
            match bytes[i] {
                b'\'' => in_quote = !in_quote,
                b'-' if !in_quote && i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                    cut = i;
                    break;
                }
                _ => {}
            }
            i += 1;
        }
        out.push_str(&line[..cut]);
        out.push('\n');
    }
    out
}

/// Split on `;` outside single quotes.
fn split_statements(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quote = false;
    for ch in text.chars() {
        match ch {
            '\'' => {
                in_quote = !in_quote;
                cur.push(ch);
            }
            ';' if !in_quote => {
                out.push(std::mem::take(&mut cur));
            }
            _ => cur.push(ch),
        }
    }
    out.push(cur);
    out
}

/// Split on commas at depth 0, counting BOTH `(` `)` and `<` `>`: a column list
/// holds `MAP<TEXT, TEXT>` and a key clause holds `((a, b), c)`.
fn split_top_level(inner: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut cur = String::new();
    for ch in inner.chars() {
        match ch {
            '(' | '<' => {
                depth += 1;
                cur.push(ch);
            }
            ')' | '>' => {
                depth -= 1;
                cur.push(ch);
            }
            ',' if depth == 0 => out.push(std::mem::take(&mut cur)),
            _ => cur.push(ch),
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur);
    }
    out
}

/// A CQL identifier: unquoted names are case-insensitive (fold to lowercase), a
/// quoted name keeps its case.
fn identifier(raw: &str) -> String {
    let t = raw.trim();
    match t.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        Some(quoted) => quoted.to_string(),
        None => t.to_ascii_lowercase(),
    }
}

fn head(s: &str) -> String {
    s.chars().take(60).collect()
}
