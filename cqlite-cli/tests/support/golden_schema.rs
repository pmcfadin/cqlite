//! The committed `CREATE TABLE` DDL as the AD2 lane's column/type authority
//! (issue #1491).
//!
//! # Why this module exists
//!
//! Two false-pass paths in the first cut of this lane had the same root cause:
//! the comparison had no statement of what columns a row must carry, or of what
//! CQL type each value is.
//!
//!   * Both sides defaulted an absent column to `null`, so a column CQLite
//!     omitted entirely compared equal to a golden null — and a spurious extra
//!     null column also passed. The "an absent cell renders as `null`" property
//!     was therefore untested by the very cases that exist for it.
//!   * Every numeric-LOOKING text was canonicalized as a number, so a `text`
//!     value `"22201"` compared equal to the JSON number `22201`, and `"00000"`
//!     to `"0"` — type and zero-padding regressions passed silently.
//!
//! Both are answered by the same authority: the committed `CREATE TABLE` in
//! `test-data/schemas/*.cql`. That is doctrine-correct rather than a workaround —
//! the no-heuristics mandate (#28) names the schema as THE metadata source, the
//! CLI under test is already given the same file via `--schema`, and Cassandra
//! itself decides both questions from the DDL.
//!
//! # Independently parsed, on purpose
//!
//! This is a small hand-written DDL reader rather than a call into
//! `cqlite_core`'s schema parser. Using CQLite's parser here would make the
//! oracle share an implementation with the code under test: a parser that
//! dropped a column would drop it from the *expectation* too, and the egress
//! omitting the same column would then pass — exactly the false-pass class this
//! module was written to close (CLAUDE.md, "a CQLite `file:line` is NEVER format
//! authority").
//!
//! It is deliberately narrow and FAILS CLOSED. It understands the subset the
//! committed fixture schemas use — `USE <keyspace>`, `CREATE TYPE`,
//! `CREATE TABLE` (bare or keyspace-qualified), native scalar types, `frozen<>`,
//! `list/set/map/tuple`, UDT references, `STATIC`, inline and
//! trailing `PRIMARY KEY` — and returns `Err` naming the input for anything else
//! (an unknown type name, an unresolvable UDT, a block comment, an unbalanced
//! bracket). A type it cannot name is never guessed at, because a guessed type
//! would silently restore the permissive comparison.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::Path;

/// A CQL type, reduced to the distinctions value comparison needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CqlType {
    /// `int`, `bigint`, `smallint`, `tinyint`, `varint`, `float`, `double`,
    /// `decimal`, `counter`: the only types whose value may be compared
    /// NUMERICALLY, i.e. where a JSON number and its decimal text denote the same
    /// value. The name is kept for diagnostics.
    Numeric(String),
    /// `text`, `varchar`, `ascii`: compared as EXACT strings, so `"00000"` never
    /// equals `"0"` and `"22201"` never equals the number `22201`.
    Text(String),
    /// `boolean`.
    Boolean,
    /// `blob`: `0x…` hex, compared exactly.
    Blob,
    /// `timestamp`: the one scalar with two legitimate spellings (see
    /// `canon_timestamp`).
    Timestamp,
    /// `uuid`, `timeuuid`, `date`, `time`, `duration`, `inet`: opaque text,
    /// compared exactly. The name is kept for diagnostics.
    Opaque(String),
    List(Box<CqlType>),
    Set(Box<CqlType>),
    Map(Box<CqlType>, Box<CqlType>),
    Tuple(Vec<CqlType>),
    Udt(UdtType),
}

/// A user-defined type, fully resolved at parse time (a UDT field that is itself
/// a UDT is expanded, so comparison never has to look a name up).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UdtType {
    pub name: String,
    pub fields: Vec<(String, CqlType)>,
}

impl CqlType {
    /// A short human name for failure messages.
    pub fn describe(&self) -> String {
        match self {
            CqlType::Numeric(n) | CqlType::Text(n) | CqlType::Opaque(n) => n.clone(),
            CqlType::Boolean => "boolean".to_string(),
            CqlType::Blob => "blob".to_string(),
            CqlType::Timestamp => "timestamp".to_string(),
            CqlType::List(e) => format!("list<{}>", e.describe()),
            CqlType::Set(e) => format!("set<{}>", e.describe()),
            CqlType::Map(k, v) => format!("map<{}, {}>", k.describe(), v.describe()),
            CqlType::Tuple(items) => format!(
                "tuple<{}>",
                items
                    .iter()
                    .map(CqlType::describe)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            CqlType::Udt(u) => u.name.clone(),
        }
    }

    /// Is this a collection/UDT, i.e. a type whose value is multi-cell when it is
    /// not frozen?
    pub fn is_complex(&self) -> bool {
        matches!(
            self,
            CqlType::List(_) | CqlType::Set(_) | CqlType::Map(_, _) | CqlType::Udt(_)
        )
    }
}

/// Where a column sits in the primary key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnKind {
    Partition,
    Clustering,
    Static,
    Regular,
}

/// One declared column.
#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub ty: CqlType,
    pub kind: ColumnKind,
    /// `frozen<…>` at the top level: a frozen collection/UDT is stored as ONE
    /// value cell, a non-frozen one is multi-cell.
    pub frozen: bool,
}

impl Column {
    /// A non-frozen collection or UDT: `sstabledump` flattens it into one cell
    /// per element, each carrying a cell path.
    pub fn is_multicell(&self) -> bool {
        !self.frozen && self.ty.is_complex()
    }
}

/// One `CREATE TABLE`, in DDL column order.
#[derive(Clone, Debug)]
pub struct TableSchema {
    pub keyspace: Option<String>,
    pub table: String,
    pub columns: Vec<Column>,
    /// Partition-key columns in KEY order.
    pub partition_key: Vec<String>,
    /// Clustering columns in KEY order.
    pub clustering: Vec<String>,
}

impl TableSchema {
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

/// Parse `file` and return the schema of `table`, or an error naming what could
/// not be read. `table` is matched case-insensitively, as CQL identifiers are.
pub fn load(file: &Path, table: &str) -> Result<TableSchema, String> {
    let text = std::fs::read_to_string(file)
        .map_err(|e| format!("cannot read committed schema {}: {e}", file.display()))?;
    from_ddl(&text, table).map_err(|why| format!("{}: {why}", file.display()))
}

/// Parse DDL text and return the schema of `table`.
pub fn from_ddl(ddl: &str, table: &str) -> Result<TableSchema, String> {
    let ddl = strip_comments(ddl)?;
    let mut udts: BTreeMap<(Option<String>, String), UdtType> = BTreeMap::new();
    let mut found: Option<TableSchema> = None;
    // The keyspace `USE` last put in effect. Every committed fixture schema is
    // written that way — `CREATE KEYSPACE`, `USE <ks>`, then UNQUALIFIED
    // `CREATE TABLE`s — so without reading `USE` this reader answered `keyspace:
    // None` for every table it parsed, and the AD2 case table's declared keyspace
    // was cross-checked against nothing (issue #1491 review round 19, finding Y1).
    let mut in_effect: Option<String> = None;
    let wanted = table.to_ascii_lowercase();
    for statement in statements(&ddl)? {
        let lower = statement.to_ascii_lowercase();
        if let Some(keyspace) = use_target(&statement)? {
            in_effect = Some(keyspace);
            continue;
        }
        if lower.starts_with("create type") {
            let (name, body) = named_body(&statement, "create type")?;
            let (keyspace, name) = split_qualified(&name, in_effect.as_deref());
            let fields = parse_fields(
                &body,
                &TypeScope {
                    declared: &udts,
                    keyspace: keyspace.as_deref(),
                },
            )?;
            // A second `CREATE TYPE` of the same name used to OVERWRITE the first,
            // silently: every table declared before it kept the old field list and
            // every table after it got the new one, so one file could yield two
            // different answers for the same type name and neither would be
            // reported. Refused for the same reason a repeated `CREATE TABLE` is —
            // there is no way to say which declaration is authoritative.
            if udts.contains_key(&(keyspace.clone(), name.clone())) {
                return Err(format!(
                    "type `{name}` is declared more than once — the lane cannot say which \
                     declaration is authoritative for its field types"
                ));
            }
            udts.insert(
                (keyspace, name.clone()),
                UdtType {
                    name: name.clone(),
                    fields,
                },
            );
        } else if lower.starts_with("create table") || lower.starts_with("create columnfamily") {
            let keyword = if lower.starts_with("create table") {
                "create table"
            } else {
                "create columnfamily"
            };
            let (name, body) = named_body(&statement, keyword)?;
            let (keyspace, bare) = split_qualified(&name, in_effect.as_deref());
            if bare != wanted {
                continue;
            }
            if found.is_some() {
                return Err(format!(
                    "table `{table}` is declared more than once — the case cannot say \
                     which declaration is authoritative"
                ));
            }
            let scope = TypeScope {
                declared: &udts,
                keyspace: keyspace.as_deref(),
            };
            found = Some(parse_table(keyspace.clone(), bare, &body, &scope)?);
        }
    }
    found.ok_or_else(|| format!("no `CREATE TABLE {table}` in this schema file"))
}

/// The keyspace a `USE <keyspace>` statement puts in effect, or `None` for any
/// other statement.
///
/// Reads only a BARE identifier, and REFUSES anything else — a quoted
/// (`USE "Ks"`) or otherwise unrecognised target is an error rather than a
/// silently ignored statement, because ignoring it would leave the previous
/// keyspace in effect and attribute the tables below it to the wrong keyspace.
/// Every committed fixture schema uses the bare form.
///
/// A second `USE` is not refused: CQL's own rule — the keyspace in effect is the
/// one the LAST `USE` above the statement named — is exact, and applying it is
/// cheaper than a refusal that would reject a legal file.
fn use_target(statement: &str) -> Result<Option<String>, String> {
    let lower = statement.to_ascii_lowercase();
    let Some(rest) = lower.strip_prefix("use") else {
        return Ok(None);
    };
    // `use` must be a keyword here and not the start of an identifier: statements
    // are whitespace-collapsed, so a `USE` statement is exactly `use <target>`.
    if !rest.is_empty() && !rest.starts_with(' ') {
        return Ok(None);
    }
    let target = rest.trim();
    if target.is_empty()
        || !target
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(format!(
            "`{statement}`: this reader reads only a bare keyspace identifier after \
             `USE`, so it will not guess which keyspace the tables below it declare in"
        ));
    }
    Ok(Some(target.to_string()))
}

/// Split a `CREATE …` name into (keyspace, bare name): the `ks.` prefix when the
/// name carries one, else whatever `USE` last put in effect (`None` when the file
/// has stated no keyspace at all, which the AD2 lane treats as an UNVERIFIABLE
/// declaration rather than as agreement).
fn split_qualified(name: &str, in_effect: Option<&str>) -> (Option<String>, String) {
    match name.split_once('.') {
        Some((keyspace, bare)) => (Some(keyspace.trim().to_string()), bare.trim().to_string()),
        None => (in_effect.map(str::to_string), name.to_string()),
    }
}

/// The UDTs a statement may reference: the ones a `CREATE TYPE` earlier in the
/// same file declared IN THE SAME KEYSPACE.
///
/// Keyed by keyspace because a UDT belongs to one. Two keyspaces may each declare
/// a `person` with different fields, and a flat by-name registry would resolve a
/// table's `person` to whichever was parsed last — the same
/// decided-by-declaration-order defect the duplicate-declaration refusals exist to
/// stop. Keyed this way, a reference that crosses keyspaces does not resolve at
/// all, and `parse_bare_type` reports it as an unknown type rather than silently
/// substituting the other keyspace's fields.
struct TypeScope<'a> {
    declared: &'a BTreeMap<(Option<String>, String), UdtType>,
    keyspace: Option<&'a str>,
}

impl<'a> TypeScope<'a> {
    fn resolve(&self, name: &str) -> Option<&'a UdtType> {
        self.declared
            .get(&(self.keyspace.map(str::to_string), name.to_string()))
    }
}

/// Remove `--` line comments. Block comments are REFUSED rather than skipped:
/// none of the committed schemas uses one, and a half-understood comment syntax
/// could silently swallow a column declaration.
fn strip_comments(ddl: &str) -> Result<String, String> {
    let mut out = String::with_capacity(ddl.len());
    for (i, line) in ddl.lines().enumerate() {
        let mut in_quote = false;
        let mut cut = line.len();
        let bytes = line.as_bytes();
        let mut j = 0;
        while j < bytes.len() {
            match bytes[j] {
                b'\'' => in_quote = !in_quote,
                b'/' if !in_quote && bytes.get(j + 1) == Some(&b'*') => {
                    return Err(format!(
                        "line {}: block comment `/*` is not supported by this reader",
                        i + 1
                    ))
                }
                b'-' if !in_quote && bytes.get(j + 1) == Some(&b'-') => {
                    cut = j;
                    break;
                }
                _ => {}
            }
            j += 1;
        }
        out.push_str(&line[..cut]);
        out.push('\n');
    }
    Ok(out)
}

/// Split into statements on `;` outside quotes and brackets, with whitespace
/// collapsed so the rest of the reader can work on single-line text.
fn statements(ddl: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut depth = 0i32;
    for ch in ddl.chars() {
        match ch {
            '\'' => {
                in_quote = !in_quote;
                current.push(ch);
            }
            '(' | '<' if !in_quote => {
                depth += 1;
                current.push(ch);
            }
            ')' | '>' if !in_quote => {
                depth -= 1;
                if depth < 0 {
                    return Err("unbalanced bracket in DDL".to_string());
                }
                current.push(ch);
            }
            ';' if !in_quote && depth == 0 => {
                out.push(collapse(&current));
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if !collapse(&current).is_empty() {
        out.push(collapse(&current));
    }
    if depth != 0 {
        return Err("unbalanced bracket in DDL".to_string());
    }
    Ok(out.into_iter().filter(|s| !s.is_empty()).collect())
}

fn collapse(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// From `CREATE {TABLE,TYPE} [IF NOT EXISTS] <name> ( <body> ) [WITH …]`, return
/// the lowercased name and the parenthesised body.
fn named_body(statement: &str, keyword: &str) -> Result<(String, String), String> {
    let rest = statement[keyword.len()..].trim_start();
    let rest = match rest.to_ascii_lowercase().strip_prefix("if not exists") {
        Some(_) => rest["if not exists".len()..].trim_start(),
        None => rest,
    };
    let open = rest
        .find('(')
        .ok_or_else(|| format!("`{keyword}` with no `(`: {statement}"))?;
    let name = rest[..open].trim().to_ascii_lowercase();
    if name.is_empty() {
        return Err(format!("`{keyword}` with no name: {statement}"));
    }
    let body = matched(&rest[open..])?;
    Ok((name, body))
}

/// The content of the parenthesised group `s` starts with.
fn matched(s: &str) -> Result<String, String> {
    let mut depth = 0i32;
    let mut in_quote = false;
    for (i, ch) in s.char_indices() {
        match ch {
            '\'' => in_quote = !in_quote,
            '(' if !in_quote => depth += 1,
            ')' if !in_quote => {
                depth -= 1;
                if depth == 0 {
                    return Ok(s[1..i].to_string());
                }
            }
            _ => {}
        }
    }
    Err(format!("unbalanced `(` in: {s}"))
}

/// Split on commas outside quotes, parens AND angle brackets (`map<int, text>`
/// carries a comma that is not an item boundary).
fn split_items(body: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut depth = 0i32;
    let mut in_quote = false;
    for ch in body.chars() {
        match ch {
            '\'' => {
                in_quote = !in_quote;
                current.push(ch);
            }
            '(' | '<' if !in_quote => {
                depth += 1;
                current.push(ch);
            }
            ')' | '>' if !in_quote => {
                depth -= 1;
                if depth < 0 {
                    return Err(format!("unbalanced bracket in: {body}"));
                }
                current.push(ch);
            }
            ',' if !in_quote && depth == 0 => {
                out.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        out.push(current.trim().to_string());
    }
    if depth != 0 {
        return Err(format!("unbalanced bracket in: {body}"));
    }
    Ok(out)
}

/// `CREATE TYPE` fields: `name type` pairs only.
fn parse_fields(body: &str, scope: &TypeScope<'_>) -> Result<Vec<(String, CqlType)>, String> {
    let mut fields = Vec::new();
    for item in split_items(body)? {
        let (name, ty_text) = item
            .split_once(char::is_whitespace)
            .ok_or_else(|| format!("UDT field with no type: `{item}`"))?;
        let (ty, _frozen) = parse_type(ty_text.trim(), scope)?;
        let name = name.trim().to_ascii_lowercase();
        // A repeated field name is refused rather than kept twice: the comparison
        // resolves a field's type with `.find()`, i.e. FIRST wins, while the egress
        // and the golden could each mean the other one — so a duplicate would
        // silently decide the compared type by declaration order.
        if fields.iter().any(|(earlier, _)| *earlier == name) {
            return Err(format!(
                "UDT field `{name}` is declared more than once in `{body}` — the field's \
                 compared type would be decided by declaration order"
            ));
        }
        fields.push((name, ty));
    }
    if fields.is_empty() {
        return Err(format!("UDT with no fields: `{body}`"));
    }
    Ok(fields)
}

fn parse_table(
    keyspace: Option<String>,
    table: String,
    body: &str,
    scope: &TypeScope<'_>,
) -> Result<TableSchema, String> {
    let mut columns: Vec<Column> = Vec::new();
    let mut partition_key: Vec<String> = Vec::new();
    let mut clustering: Vec<String> = Vec::new();
    let mut statics: Vec<String> = Vec::new();
    let mut inline_pk: Option<String> = None;

    for item in split_items(body)? {
        let lower = item.to_ascii_lowercase();
        if lower.starts_with("primary key") {
            let spec = item[..]
                .find('(')
                .map(|i| matched(&item[i..]))
                .transpose()?
                .ok_or_else(|| format!("`PRIMARY KEY` with no `(`: `{item}`"))?;
            let (pk, ck) = parse_key_spec(&spec)?;
            if !partition_key.is_empty() {
                return Err(format!("two PRIMARY KEY clauses in `{table}`"));
            }
            partition_key = pk;
            clustering = ck;
            continue;
        }
        // A column: `name type [STATIC] [PRIMARY KEY]`, in either trailing order.
        let mut rest = item.as_str();
        let mut is_static = false;
        let mut is_inline_pk = false;
        loop {
            let lower = rest.to_ascii_lowercase();
            if let Some(stripped) = lower.strip_suffix("primary key") {
                is_inline_pk = true;
                rest = rest[..stripped.len()].trim_end();
                continue;
            }
            if let Some(stripped) = lower.strip_suffix("static") {
                is_static = true;
                rest = rest[..stripped.len()].trim_end();
                continue;
            }
            break;
        }
        let (name, ty_text) = rest
            .split_once(char::is_whitespace)
            .ok_or_else(|| format!("column with no type: `{item}`"))?;
        let name = name.trim().to_ascii_lowercase();
        let (ty, frozen) = parse_type(ty_text.trim(), scope)?;
        if is_inline_pk {
            if inline_pk.is_some() {
                return Err(format!("two inline PRIMARY KEY columns in `{table}`"));
            }
            inline_pk = Some(name.clone());
        }
        if is_static {
            statics.push(name.clone());
        }
        // A repeated column name is refused rather than kept twice: the comparison
        // iterates `schema.columns` (so the column would be compared — and
        // counted — twice) while `TableSchema::column` resolves a name with
        // `.find()` (so the type used would be the FIRST declaration's). Either
        // half alone makes the compared column set a function of declaration
        // order rather than of the DDL.
        if columns.iter().any(|c| c.name == name) {
            return Err(format!(
                "`{table}` declares the column `{name}` more than once — the compared \
                 column set and its types would depend on declaration order"
            ));
        }
        columns.push(Column {
            name,
            ty,
            kind: ColumnKind::Regular,
            frozen,
        });
    }

    if let Some(pk) = inline_pk {
        if !partition_key.is_empty() {
            return Err(format!(
                "`{table}` has both an inline PRIMARY KEY and a PRIMARY KEY clause"
            ));
        }
        partition_key = vec![pk];
    }
    if partition_key.is_empty() {
        return Err(format!("`{table}` declares no primary key"));
    }
    for name in partition_key.iter().chain(clustering.iter()) {
        if !columns.iter().any(|c| &c.name == name) {
            return Err(format!(
                "`{table}` primary key names `{name}`, which is not a declared column"
            ));
        }
    }
    for column in &mut columns {
        column.kind = if partition_key.contains(&column.name) {
            ColumnKind::Partition
        } else if clustering.contains(&column.name) {
            ColumnKind::Clustering
        } else if statics.contains(&column.name) {
            ColumnKind::Static
        } else {
            ColumnKind::Regular
        };
    }
    Ok(TableSchema {
        keyspace,
        table,
        columns,
        partition_key,
        clustering,
    })
}

/// `pk`, `pk, ck…`, or `(pk1, pk2), ck…`.
fn parse_key_spec(spec: &str) -> Result<(Vec<String>, Vec<String>), String> {
    let items = split_items(spec)?;
    let first = items
        .first()
        .ok_or_else(|| format!("empty PRIMARY KEY spec: `{spec}`"))?;
    let mut partition = Vec::new();
    if first.starts_with('(') {
        for name in split_items(&matched(first)?)? {
            partition.push(name.trim().to_ascii_lowercase());
        }
    } else {
        partition.push(first.trim().to_ascii_lowercase());
    }
    let clustering: Vec<String> = items[1..]
        .iter()
        .map(|n| n.trim().to_ascii_lowercase())
        .collect();
    if partition.is_empty() {
        return Err(format!("PRIMARY KEY with no partition column: `{spec}`"));
    }
    // A repeated key component would be transcribed into the case's `pk`/`ck` and
    // then read TWICE per row when the row key is built, so the same value would
    // appear twice in the pairing key and once in the golden's key array — a key
    // arity mismatch attributed to the golden rather than to the DDL.
    let mut seen: Vec<&String> = Vec::new();
    for name in partition.iter().chain(clustering.iter()) {
        if seen.contains(&name) {
            return Err(format!(
                "PRIMARY KEY names `{name}` more than once: `{spec}`"
            ));
        }
        seen.push(name);
    }
    Ok((partition, clustering))
}

/// Parse a type reference. Returns the type and whether the TOP level was
/// `frozen<…>`.
fn parse_type(text: &str, scope: &TypeScope<'_>) -> Result<(CqlType, bool), String> {
    let trimmed = text.trim();
    let lower = trimmed.to_ascii_lowercase();
    if let Some(rest) = lower.strip_prefix("frozen") {
        if rest.trim_start().starts_with('<') {
            let inner = angle_body(trimmed)?;
            let (ty, _) = parse_type(&inner, scope)?;
            return Ok((ty, true));
        }
    }
    Ok((parse_bare_type(trimmed, scope)?, false))
}

fn parse_bare_type(text: &str, scope: &TypeScope<'_>) -> Result<CqlType, String> {
    let trimmed = text.trim();
    let lower = trimmed.to_ascii_lowercase();
    if let Some(open) = trimmed.find('<') {
        let name = lower[..open].trim().to_string();
        let args = split_items(&angle_body(trimmed)?)?;
        let mut parsed = Vec::new();
        for arg in &args {
            parsed.push(parse_type(arg, scope)?.0);
        }
        return match (name.as_str(), parsed.len()) {
            ("list", 1) => Ok(CqlType::List(Box::new(parsed.remove(0)))),
            ("set", 1) => Ok(CqlType::Set(Box::new(parsed.remove(0)))),
            ("map", 2) => {
                let value = parsed.remove(1);
                let key = parsed.remove(0);
                Ok(CqlType::Map(Box::new(key), Box::new(value)))
            }
            ("tuple", n) if n > 0 => Ok(CqlType::Tuple(parsed)),
            ("frozen", 1) => Ok(parsed.remove(0)),
            _ => Err(format!(
                "unsupported parameterised type `{trimmed}` ({name} with {} argument(s))",
                parsed.len()
            )),
        };
    }
    // Every native scalar name comes from the four censused lists below and
    // NOWHERE else, so a type this reader newly recognises cannot slip past the
    // spelling census that consumes them (roborev job 21 F2).
    let name = lower.as_str();
    if NATIVE_NUMERIC.contains(&name) {
        return Ok(CqlType::Numeric(lower));
    }
    if NATIVE_TEXT.contains(&name) {
        return Ok(CqlType::Text(lower));
    }
    if NATIVE_OPAQUE.contains(&name) {
        return Ok(CqlType::Opaque(lower));
    }
    if let Some((_, ty)) = NATIVE_SINGLETON.iter().find(|(known, _)| *known == name) {
        return Ok(ty.clone());
    }
    match scope.resolve(name) {
        Some(udt) => Ok(CqlType::Udt(udt.clone())),
        // Fail closed: a guessed type restores the permissive comparison this
        // module exists to remove.
        None => Err(format!(
            "unknown type `{trimmed}` — neither a native CQL type this reader \
             knows nor a `CREATE TYPE` declared earlier in the same file and \
             in the same keyspace"
        )),
    }
}

// --- the native scalar type set, as DATA -------------------------------------
//
// These four lists are the SOLE source of the native scalar names
// [`parse_bare_type`] recognises, and they are read back by the per-type spelling
// differential in `golden_csv_container_spelling_tests.rs`, whose census requires
// a case for EVERY name in them. Written as data rather than as match arms for
// exactly that reason: a census over `CqlType` VARIANTS cannot see a missing
// concrete type (any one numeric case satisfies `Numeric`), which is how `counter`
// came to have no spelling case at all (roborev job 21 F2). Add a native type here
// and the census fails until its spelling is established.

/// Names mapping to [`CqlType::Numeric`] — the types whose value may be compared
/// numerically.
pub const NATIVE_NUMERIC: &[&str] = &[
    "int", "bigint", "smallint", "tinyint", "varint", "float", "double", "decimal", "counter",
];

/// Names mapping to [`CqlType::Text`] — compared as exact strings.
pub const NATIVE_TEXT: &[&str] = &["text", "varchar", "ascii"];

/// Names mapping to [`CqlType::Opaque`] — compared as exact text.
pub const NATIVE_OPAQUE: &[&str] = &["uuid", "timeuuid", "date", "time", "duration", "inet"];

/// The names whose variant carries no name of its own, paired with that variant.
///
/// A `(name, variant)` table rather than three match arms so this list is the
/// mapping itself: there is no second spelling of `"boolean"` for the list and the
/// parser to disagree about.
pub const NATIVE_SINGLETON: &[(&str, CqlType)] = &[
    ("boolean", CqlType::Boolean),
    ("blob", CqlType::Blob),
    ("timestamp", CqlType::Timestamp),
];

/// EVERY native scalar name this reader recognises, in the order the four lists
/// declare them.
///
/// The completeness claim is bounded and stated: it is complete with respect to
/// those lists, which [`parse_bare_type`] is the only consumer of, so a name it
/// accepts is necessarily here. It says nothing about types this reader does not
/// implement (a `vector`, say) — those are a hard parse error, not a silent gap.
pub fn native_scalar_decls() -> Vec<&'static str> {
    NATIVE_NUMERIC
        .iter()
        .chain(NATIVE_TEXT)
        .chain(NATIVE_OPAQUE)
        .copied()
        .chain(NATIVE_SINGLETON.iter().map(|(name, _)| *name))
        .collect()
}

/// The content between the first `<` and its match.
fn angle_body(s: &str) -> Result<String, String> {
    let open = s.find('<').ok_or_else(|| format!("no `<` in type `{s}`"))?;
    let mut depth = 0i32;
    for (i, ch) in s[open..].char_indices() {
        match ch {
            '<' => depth += 1,
            '>' => {
                depth -= 1;
                if depth == 0 {
                    return Ok(s[open + 1..open + i].to_string());
                }
            }
            _ => {}
        }
    }
    Err(format!("unbalanced `<` in type `{s}`"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const DDL: &str = r#"
-- a comment mentioning ( unbalanced and a ; semicolon
CREATE KEYSPACE IF NOT EXISTS ks WITH replication = {'class': 'SimpleStrategy'};
USE ks;

CREATE TYPE IF NOT EXISTS address (
    street text,
    city   text,
    zip    text
);

CREATE TYPE IF NOT EXISTS employee (
    name  text,
    home  frozen<address>,
    level int
);

CREATE TABLE IF NOT EXISTS t (
    pk     int,
    bucket text,
    seq    int,
    body   TEXT,
    sdata  text STATIC,
    ms     set<int>,
    fm     FROZEN<MAP<INT, TEXT>>,
    ma     map<text, frozen<address>>,
    e      frozen<employee>,
    tup    tuple<int, text>,
    PRIMARY KEY ((pk, bucket), seq)
) WITH compression = {'enabled': 'false'};

CREATE TABLE IF NOT EXISTS inline (
    id INT PRIMARY KEY,
    v  text
);
"#;

    fn schema(table: &str) -> TableSchema {
        match from_ddl(DDL, table) {
            Ok(s) => s,
            Err(why) => panic!("{table}: {why}"),
        }
    }

    #[test]
    fn the_column_set_and_key_order_come_from_the_ddl() {
        let s = schema("t");
        assert_eq!(
            s.column_names(),
            vec!["pk", "bucket", "seq", "body", "sdata", "ms", "fm", "ma", "e", "tup"]
        );
        assert_eq!(s.partition_key, vec!["pk", "bucket"]);
        assert_eq!(s.clustering, vec!["seq"]);
        assert_eq!(
            s.column("sdata").map(|c| c.kind),
            Some(ColumnKind::Static),
            "a STATIC column must be recognised as static"
        );
        assert_eq!(s.column("body").map(|c| c.kind), Some(ColumnKind::Regular));
        assert_eq!(s.column("pk").map(|c| c.kind), Some(ColumnKind::Partition));
        assert_eq!(
            s.column("seq").map(|c| c.kind),
            Some(ColumnKind::Clustering)
        );

        let inline = schema("inline");
        assert_eq!(inline.partition_key, vec!["id"]);
        assert!(inline.clustering.is_empty());
        assert_eq!(inline.column_names(), vec!["id", "v"]);
    }

    /// The distinction BLOCKER 2 turns on: `text` is not numeric, so a numeric
    /// text is never compared as a number.
    #[test]
    fn text_and_numeric_are_distinct_types() {
        let s = schema("t");
        assert_eq!(
            s.column("body").map(|c| c.ty.clone()),
            Some(CqlType::Text("text".to_string()))
        );
        assert_eq!(
            s.column("seq").map(|c| c.ty.clone()),
            Some(CqlType::Numeric("int".to_string()))
        );
    }

    #[test]
    fn frozen_and_multicell_are_distinguished() {
        let s = schema("t");
        let ms = s.column("ms").expect("ms");
        assert!(ms.is_multicell(), "a non-frozen set is multicell");
        let fm = s.column("fm").expect("fm");
        assert!(!fm.is_multicell(), "a frozen map is one value cell");
        assert_eq!(
            fm.ty,
            CqlType::Map(
                Box::new(CqlType::Numeric("int".to_string())),
                Box::new(CqlType::Text("text".to_string()))
            ),
            "case must not matter and frozen<> must be unwrapped"
        );
        assert!(
            !s.column("body").map(|c| c.is_multicell()).unwrap_or(true),
            "a scalar is never multicell"
        );
    }

    #[test]
    fn a_nested_udt_is_resolved_to_its_fields() {
        let s = schema("t");
        let Some(CqlType::Udt(employee)) = s.column("e").map(|c| c.ty.clone()) else {
            panic!("e should be a UDT");
        };
        assert_eq!(employee.name, "employee");
        let home = employee
            .fields
            .iter()
            .find(|(n, _)| n == "home")
            .map(|(_, t)| t.clone());
        let Some(CqlType::Udt(address)) = home else {
            panic!("employee.home should resolve to the address UDT");
        };
        assert_eq!(
            address.fields,
            vec![
                ("street".to_string(), CqlType::Text("text".to_string())),
                ("city".to_string(), CqlType::Text("text".to_string())),
                // The zip that must NOT be compared numerically.
                ("zip".to_string(), CqlType::Text("text".to_string())),
            ]
        );

        let Some(CqlType::Map(key, value)) = s.column("ma").map(|c| c.ty.clone()) else {
            panic!("ma should be a map");
        };
        assert_eq!(*key, CqlType::Text("text".to_string()));
        assert!(matches!(*value, CqlType::Udt(_)));
    }

    #[test]
    fn a_tuple_keeps_its_positional_types() {
        let s = schema("t");
        assert_eq!(
            s.column("tup").map(|c| c.ty.clone()),
            Some(CqlType::Tuple(vec![
                CqlType::Numeric("int".to_string()),
                CqlType::Text("text".to_string()),
            ]))
        );
    }

    /// Fail closed: an unknown type name is never guessed at, because a guess
    /// would silently restore the permissive comparison.
    #[test]
    fn an_unknown_type_is_an_error_not_a_guess() {
        let ddl = "CREATE TABLE t (id int PRIMARY KEY, v mystery_type);";
        let why = from_ddl(ddl, "t").expect_err("an undeclared type must not parse");
        assert!(why.contains("mystery_type"), "{why}");
    }

    /// A REPEATED declaration is refused rather than silently resolved by
    /// declaration order (issue #1491, the silent-overwrite sweep).
    ///
    /// Each of these four used to be accepted, and each made the compared column
    /// set or a compared TYPE a function of where a name appeared in the file:
    /// a second `CREATE TYPE` overwrote the first (so tables declared before and
    /// after it saw different field lists); a repeated column was kept twice and
    /// compared twice while `TableSchema::column` resolved its type from the FIRST
    /// declaration; a repeated UDT field's type was likewise resolved first-wins;
    /// and a repeated PRIMARY KEY component would be read twice per row while the
    /// golden's key array carried it once.
    #[test]
    fn a_repeated_declaration_is_refused_not_resolved_by_order() {
        let cases: &[(&str, &str, &str)] = &[
            (
                "duplicate CREATE TYPE",
                "CREATE TYPE a (f text); CREATE TYPE a (f int); \
                 CREATE TABLE t (id int PRIMARY KEY, v frozen<a>);",
                "declared more than once",
            ),
            (
                "duplicate column",
                "CREATE TABLE t (id int PRIMARY KEY, v text, v int);",
                "column `v` more than once",
            ),
            (
                "duplicate UDT field",
                "CREATE TYPE a (f text, f int); \
                 CREATE TABLE t (id int PRIMARY KEY, v frozen<a>);",
                "field `f` is declared more than once",
            ),
            (
                "duplicate PRIMARY KEY component",
                "CREATE TABLE t (pk int, ck int, PRIMARY KEY (pk, ck, pk));",
                "names `pk` more than once",
            ),
        ];
        for (what, ddl, expected) in cases {
            let why = from_ddl(ddl, "t")
                .map(|s| s.column_names().join(","))
                .expect_err(&format!("{what} must be refused"));
            assert!(
                why.contains(expected),
                "{what}: the refusal must name what repeated: {why}"
            );
        }
        // The distinct forms of all four are the ordinary shape, so each rule is
        // about the REPETITION and not about the reader.
        let ok = "CREATE TYPE a (f text, g int); CREATE TYPE b (h text); \
                  CREATE TABLE t (pk int, ck int, v frozen<a>, w frozen<b>, \
                  PRIMARY KEY (pk, ck));";
        let schema = from_ddl(ok, "t").expect("distinct declarations parse");
        assert_eq!(schema.column_names(), vec!["pk", "ck", "v", "w"]);
        assert_eq!(schema.partition_key, vec!["pk"]);
        assert_eq!(schema.clustering, vec!["ck"]);
    }

    #[test]
    fn a_missing_table_and_a_block_comment_are_both_errors() {
        let why = from_ddl(DDL, "nosuch").expect_err("a missing table must be an error");
        assert!(why.contains("nosuch"), "{why}");
        let why = from_ddl("/* block */ CREATE TABLE t (id int PRIMARY KEY);", "t")
            .expect_err("a block comment must be refused");
        assert!(why.contains("block comment"), "{why}");
    }

    /// The KEYSPACE a table is declared in, which is what the AD2 case table's
    /// `keyspace` field is cross-checked against (issue #1491 review round 19,
    /// finding Y1). Before this the reader ignored `USE`, so every committed
    /// schema — all of which use `USE <ks>` plus unqualified `CREATE TABLE` —
    /// answered `None`, and the case's declared keyspace was checked against
    /// nothing.
    #[test]
    fn the_keyspace_comes_from_use_or_from_a_qualified_name() {
        // `USE ks` above the `CREATE TABLE`s, which is the committed schemas' shape.
        assert_eq!(schema("t").keyspace.as_deref(), Some("ks"));
        assert_eq!(schema("inline").keyspace.as_deref(), Some("ks"));

        // A qualified name states its own keyspace and OVERRIDES the one in effect.
        let ddl = "USE ks; CREATE TABLE other.q (id int PRIMARY KEY, v text);";
        let parsed = from_ddl(ddl, "q").expect("a qualified CREATE TABLE parses");
        assert_eq!(parsed.keyspace.as_deref(), Some("other"));

        // The LAST `USE` above the statement is the one in effect (CQL's own rule).
        let ddl = "USE a; CREATE TABLE t1 (id int PRIMARY KEY); \
                   USE b; CREATE TABLE t2 (id int PRIMARY KEY);";
        assert_eq!(
            from_ddl(ddl, "t1").expect("t1").keyspace.as_deref(),
            Some("a")
        );
        assert_eq!(
            from_ddl(ddl, "t2").expect("t2").keyspace.as_deref(),
            Some("b")
        );

        // No `USE` and no qualified name: the file states no keyspace, and that is
        // reported as UNKNOWN rather than as agreement with whatever the caller
        // declared — the AD2 lane fails such a case instead of passing it.
        let ddl = "CREATE TABLE t (id int PRIMARY KEY);";
        assert_eq!(from_ddl(ddl, "t").expect("t").keyspace, None);
    }

    /// A `USE` target this narrow reader cannot read is an ERROR, never a silently
    /// ignored statement: ignoring it would leave the PREVIOUS keyspace in effect
    /// and attribute the tables below it to the wrong one.
    #[test]
    fn a_use_target_this_reader_cannot_read_is_an_error() {
        for ddl in [
            "USE \"Ks\"; CREATE TABLE t (id int PRIMARY KEY);",
            "USE ; CREATE TABLE t (id int PRIMARY KEY);",
        ] {
            let why = from_ddl(ddl, "t").expect_err("an unreadable USE must be refused");
            assert!(
                why.contains("bare keyspace identifier"),
                "the refusal must name what it will not guess: {why}"
            );
        }
        // A column or table whose name merely STARTS with `use` is not a `USE`
        // statement, so it must still parse.
        let ddl = "USE ks; CREATE TABLE user_events (id int PRIMARY KEY, useful text);";
        let parsed = from_ddl(ddl, "user_events").expect("`user_events` is not a USE statement");
        assert_eq!(parsed.column_names(), vec!["id", "useful"]);
        assert_eq!(parsed.keyspace.as_deref(), Some("ks"));
    }

    /// A UDT belongs to the keyspace it was declared in, so a reference from
    /// ANOTHER keyspace does not resolve. Fail closed: substituting the other
    /// keyspace's fields would decide a compared type by declaration order, which
    /// is exactly what the duplicate-declaration refusals exist to stop.
    #[test]
    fn a_udt_does_not_resolve_across_keyspaces() {
        let ddl = "USE a; CREATE TYPE p (f text); \
                   USE b; CREATE TABLE t (id int PRIMARY KEY, v frozen<p>);";
        let why = from_ddl(ddl, "t").expect_err("a cross-keyspace UDT must not resolve");
        assert!(why.contains("unknown type `p`"), "{why}");

        // The same-keyspace form is the ordinary shape and parses, so the rule is
        // about the keyspace boundary and not about the reader.
        let ddl = "USE a; CREATE TYPE p (f text); \
                   CREATE TABLE t (id int PRIMARY KEY, v frozen<p>);";
        let parsed = from_ddl(ddl, "t").expect("a same-keyspace UDT resolves");
        assert!(matches!(
            parsed.column("v").map(|c| c.ty.clone()),
            Some(CqlType::Udt(_))
        ));
    }

    /// The committed schemas the AD2 lane actually reads must all parse — the
    /// subject set is derived from the case table's own schema files, so a new
    /// case cannot quietly bypass the reader.
    #[test]
    fn every_committed_schema_this_lane_reads_parses() {
        let root = super::super::datasets_root::repo_root();
        let dir = root.join("test-data/schemas");
        // Three-valued, through the lane's shared probe (issue #1491 review finding
        // V1's sweep): `read_dir(...).filter_map(Result::ok)` drops an entry the
        // filesystem could not describe as if it were not there, and the subject set
        // of this very case is that listing — a silently shortened one would still
        // clear the `parsed > 10` floor below.
        let entries = super::super::fs_probe::dir_entries(&dir)
            .unwrap_or_else(|why| panic!("{why}"))
            .unwrap_or_else(|| panic!("{} does not exist", dir.display()));
        let mut parsed = 0usize;
        for entry in entries {
            let path = entry.path();
            // On the name's BYTES, like every other name test in this lane
            // (`fs_probe::name_ends_with`): `extension().and_then(to_str)` answers
            // `None` for a name that is not valid UTF-8, which would drop a committed
            // schema from THIS CASE'S OWN SUBJECT SET silently — the collapse the
            // three-valued listing above exists to prevent, one line further down.
            if !super::super::fs_probe::name_ends_with(&entry.file_name(), ".cql") {
                continue;
            }
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
            let stripped = match strip_comments(&text) {
                Ok(s) => s,
                // `legacy/` and other out-of-scope files may use syntax this
                // narrow reader refuses; only the tables the lane reads matter,
                // and those are covered by the lane itself.
                Err(_) => continue,
            };
            let Ok(statements) = statements(&stripped) else {
                continue;
            };
            for statement in statements {
                let lower = statement.to_ascii_lowercase();
                if !lower.starts_with("create table") {
                    continue;
                }
                let Ok((name, _)) = named_body(&statement, "create table") else {
                    continue;
                };
                let bare = name.rsplit('.').next().unwrap_or(&name).to_string();
                if let Ok(schema) = from_ddl(&text, &bare) {
                    assert!(
                        !schema.columns.is_empty(),
                        "{}: {bare} parsed with no columns",
                        path.display()
                    );
                    parsed += 1;
                }
            }
        }
        assert!(
            parsed > 10,
            "only {parsed} committed CREATE TABLE statements parsed — the reader has \
             lost its subject set"
        );
    }
}
