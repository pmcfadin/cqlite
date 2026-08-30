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

use super::schema::{Column, ColumnKind, CqlType, TableSchema, UdtType};
use super::{canon_scalar, canon_typed, csv_container, Depth, Egress, Kinding, Row};
use serde_json::{Map, Value};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
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
    /// Declared skip paths that did NOT suppress a divergence in this table's
    /// walk, each with the cause (they agreed, they were never reached, or the
    /// comparison there could not be evaluated). An exclusion that suppresses
    /// nothing holds back coverage that has come back, so the caller must treat a
    /// non-empty list as a failure. See [`SkipPaths`].
    pub stale_skips: Vec<String>,
}

/// What a declared exclusion was OBSERVED to do, over a whole table's walk.
///
/// Ordered by strength: an exclusion that suppressed a real divergence anywhere is
/// applied, whatever happened on the other rows.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Observed {
    /// The path was reached and the comparison there COULD NOT BE DECIDED (the
    /// column was absent from the egress row, or the CSV cell was refused). "I
    /// could not tell" is not "the gap is still real", so it is reported.
    Unresolved(String),
    /// The path was reached and the two sides AGREE — the divergence the exclusion
    /// was declared for is gone, so the exclusion is stale.
    Agreed,
    /// The path was reached and the two sides DIVERGED: the exclusion suppressed a
    /// real divergence, which is the only thing that makes it applied.
    Suppressed,
}

/// Value paths excluded from the comparison, with what each was observed to do.
///
/// A path is fully qualified from the row: `sf` excludes a whole column, `e.home`
/// excludes ONE field of the `frozen<employee>` in column `e` while `e.name` and
/// `e.level` keep being compared. Whole-column granularity alone was too coarse
/// and cost real coverage — skipping `e` for its one divergent inner field left
/// `udt_nested` comparing nothing but its primary key (issue #1491 review finding
/// F5).
///
/// # An exclusion is applied only when it SUPPRESSES a divergence
///
/// Being VISITED is not enough, and treating it as enough is a guard weaker than
/// the property it guards (issue #1491 review finding L1): once CQLite renders the
/// path correctly, a visit-keyed tally still registers a hit, so the column stays
/// excluded forever and the stale-gap check reports the dead exclusion as live —
/// silently preventing the coverage from coming back. So the comparison at an
/// excluded path is still RUN; its result is recorded here and only then
/// discarded. [`Self::stale`] then fails on an exclusion that agreed, was never
/// reached, or could not be evaluated — three distinct causes, each named, and no
/// two of them can be reported for the same path.
pub struct SkipPaths<'a> {
    paths: &'a [&'a str],
    observed: RefCell<BTreeMap<String, Observed>>,
}

impl<'a> SkipPaths<'a> {
    pub fn new(paths: &'a [&'a str]) -> Self {
        Self {
            paths,
            observed: RefCell::new(BTreeMap::new()),
        }
    }

    /// Is this exact path excluded? Records NOTHING — what the exclusion did is
    /// recorded by [`Self::observe`], from the comparison's own outcome.
    fn excludes(&self, path: &str) -> bool {
        self.paths.contains(&path)
    }

    /// Record what the exclusion at `path` was observed to do. The strongest
    /// observation over the table's rows wins, so one divergent row is enough to
    /// keep the exclusion applied and no later agreeing row can retire it.
    fn observe(&self, path: &str, what: Observed) {
        let mut observed = self.observed.borrow_mut();
        match observed.get(path) {
            Some(prev) if *prev >= what => {}
            _ => {
                observed.insert(path.to_string(), what);
            }
        }
    }

    /// Every declared exclusion that did not suppress a divergence, with the cause.
    fn stale(&self) -> Vec<String> {
        let observed = self.observed.borrow();
        self.paths
            .iter()
            .filter_map(|p| match observed.get(*p) {
                Some(Observed::Suppressed) => None,
                Some(Observed::Agreed) => Some(format!(
                    "`{p}` (the two sides AGREE at that path now, so the exclusion \
                     suppresses nothing and is holding back recovered coverage)"
                )),
                Some(Observed::Unresolved(why)) => Some(format!(
                    "`{p}` (the comparison there could not be evaluated: {why} — an \
                     exclusion whose subject cannot be measured is not a measured gap)"
                )),
                None => Some(format!("`{p}` (matched no value in the walk at all)")),
            })
            .collect()
    }
}

/// Everything the walk knows about ONE position in a row's value tree.
///
/// Kept as one value rather than four parallel parameters because all four are
/// threaded through every level identically, and because the child of a position
/// is derived from its parent in exactly two ways — a NAMED step (a UDT field) and
/// a POSITIONAL/keyed one (a collection member, a tuple slot, a map value).
#[derive(Clone)]
struct At<'s, 'p> {
    /// What CSV's empty-field rule keys on (see [`super::Depth`]).
    depth: Depth,
    /// How the GOLDEN spells this value's JSON kind (see [`super::Kinding`] and
    /// [`column_kinding`]).
    kinding: Kinding,
    /// The fully-qualified path from the row, which is how a `SkipPaths` entry can
    /// name one UDT field rather than a whole column.
    path: String,
    skips: &'s SkipPaths<'p>,
}

impl<'s, 'p> At<'s, 'p> {
    /// A whole column's value.
    fn column(name: &str, kinding: Kinding, skips: &'s SkipPaths<'p>) -> Self {
        At {
            depth: Depth::TopLevel,
            kinding,
            path: name.to_string(),
            skips,
        }
    }

    /// One level in, at a NAMED step (a UDT field).
    fn field(&self, step: &str, kinding: Kinding) -> Self {
        let path = if self.path.is_empty() {
            step.to_string()
        } else {
            format!("{}.{step}", self.path)
        };
        At {
            depth: Depth::Inside,
            kinding,
            path,
            skips: self.skips,
        }
    }

    /// One level in, at a POSITIONAL/keyed step (a collection member, a tuple
    /// slot, a map value).
    fn index(&self, index: &str, kinding: Kinding) -> Self {
        At {
            depth: Depth::Inside,
            kinding,
            path: format!("{}[{index}]", self.path),
            skips: self.skips,
        }
    }
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
            // A WHOLE-column exclusion is NOT short-circuited here: the
            // comparison still runs so the exclusion can be observed to suppress
            // a divergence (or not) — see [`SkipPaths`]. What it does suppress is
            // the DIFF and the compared-cell COUNTERS, since a cell whose
            // divergence is discarded was not compared and must not be counted as
            // coverage. A dotted `col.field` entry does not match here; it is
            // observed inside the walk, so the column's other fields keep being
            // compared and counted.
            let excluded_column = skips.excludes(name);
            // The CLI must render EVERY declared column. An omitted one is a
            // divergence, NOT an implicit null: reading it as null is what made
            // the absent-cell property untestable.
            let Some(cv) = c.get(name) else {
                if excluded_column {
                    // An omitted column IS a divergence — the golden carries a
                    // value where the egress row carries nothing — and the
                    // exclusion is what keeps it out of `diffs`, so it suppressed
                    // one. When the column starts being rendered the value
                    // comparison below runs instead, and agreement there is what
                    // retires the exclusion.
                    skips.observe(name, Observed::Suppressed);
                    continue;
                }
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
            let decoded = match csv_decoded(gv, cv, egress, &column.ty, name, &skips) {
                Ok(decoded) => decoded,
                Err(Refusal::Ambiguous(why)) => {
                    report.ambiguous_container_cells += 1;
                    let entry = format!("{name} ({why})");
                    if !report.ambiguity_reasons.contains(&entry) {
                        report.ambiguity_reasons.push(entry);
                    }
                    if excluded_column {
                        skips.observe(
                            name,
                            Observed::Unresolved(format!("the CSV cell was refused: {why}")),
                        );
                    }
                    continue;
                }
                Err(Refusal::Unparseable(why)) => {
                    if excluded_column {
                        // The exclusion is what stops this from being a diff, so
                        // it did suppress a divergence: the CLI's text does not
                        // invert the grammar the declared type states.
                        skips.observe(name, Observed::Suppressed);
                        continue;
                    }
                    report.compared_cells += 1;
                    report.container_cells += 1;
                    report.diffs.push(format!(
                        "row[{key}].{name}: unparseable CSV container: {why}"
                    ));
                    continue;
                }
            };
            let cv = decoded.as_ref().unwrap_or(cv);
            if !excluded_column {
                report.compared_cells += 1;
                if matches!(gv, Value::Array(_) | Value::Object(_)) {
                    report.container_cells += 1;
                }
            }
            let at = At::column(name, column_kinding(column), &skips);
            // `compare_value_at` swallows (and RECORDS) the outcome at an excluded
            // path itself, so an excluded column can never reach `diffs` here.
            if let Err(why) = compare_value_at(gv, cv, egress, &column.ty, &at) {
                report.diffs.push(format!("row[{key}].{name}: {why}"));
            }
        }
    }
    report.stale_skips = skips.stale();
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

/// The [`Kinding`] of a column's value, derived from the committed DDL and from
/// `cassandra-5.0.8 JsonTransformer` (never from CQLite's output).
///
/// `sstabledump` writes a JSON STRING at exactly two kinds of position, so those
/// are the only ones where a numeric golden string may be read as a number:
///
///   * **every partition-key component** — `serializePartitionKey` writes
///     `writeString(keyValidator.getString(...))`, so `pk int = 1` arrives as
///     `"1"`. Read from the DDL's own `PRIMARY KEY` (`ColumnKind::Partition`),
///     not from the caller's transcribed key list;
///   * **a non-frozen collection's cell `path`** — `serializeCell` writes
///     `writeString(ct.nameComparator().getString(...))`. For a multicell SET the
///     path IS the element (`golden_rows` rebuilds the array from the paths), so a
///     `set<int>` golden carries `["-2","-1"]`; for a multicell MAP the path is the
///     key, which [`compare_map`] handles on its own.
///
/// Everything else is written `writeRawValue(type.toJSONString(...))` and keeps its
/// natural JSON kind — clustering values, and every cell VALUE, hence a list's
/// elements, a frozen collection's members and a UDT's fields. `frozen` is read
/// from the DDL (`Column::is_multicell`), so a `frozen<set<int>>` (one value cell,
/// golden `[-2,-1]`) is correctly held to kind equality while the non-frozen
/// `set<int>` beside it is not.
fn column_kinding(column: &Column) -> Kinding {
    let is_partition_key = column.kind == ColumnKind::Partition;
    let is_multicell_set = column.is_multicell() && matches!(column.ty, CqlType::Set(_));
    if is_partition_key || is_multicell_set {
        Kinding::Stringified
    } else {
        Kinding::Natural
    }
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
/// not text (an empty field decodes to `null`, and [`compare_value_at`] is what
/// then names that shape mismatch).
fn csv_decoded(
    gv: &Value,
    cv: &Value,
    egress: Egress,
    ty: &CqlType,
    path: &str,
    skips: &SkipPaths<'_>,
) -> Result<Option<Value>, Refusal> {
    if egress != Egress::Csv || !matches!(gv, Value::Array(_) | Value::Object(_)) {
        return Ok(None);
    }
    if let Some(why) = csv_container::ambiguity(gv, ty) {
        return Err(Refusal::Ambiguous(why));
    }
    let Value::String(text) = cv else {
        return Ok(None);
    };
    // The decoder is given the exclusion set so an EXCLUDED member is left as raw
    // text instead of being required to invert the grammar. Without it a single
    // excluded inner field fails the whole cell, which is what forced
    // `udt_nested`'s exclusion to be whole-column (review finding F5).
    csv_container::decode_at(gv, text, ty, path, &|p: &str| skips.excludes(p))
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

/// The recursive worker.
///
/// The type drives the whole walk: it says which shape each side must have and,
/// at the leaves, which canonicalization applies. Types are threaded through
/// nesting so a `text` map value or UDT field is compared exactly even when its
/// content looks numeric.
///
/// `at` carries the position: its depth, its [`super::Kinding`] and its
/// fully-qualified path, plus the exclusion set (see [`At`]).
fn compare_value_at(
    golden: &Value,
    cli: &Value,
    egress: Egress,
    ty: &CqlType,
    at: &At<'_, '_>,
) -> Result<(), String> {
    if at.skips.excludes(&at.path) {
        // The comparison at an excluded path is RUN and then discarded, because
        // its outcome is the only evidence of whether the declared gap still
        // exists: a divergence means the exclusion suppressed something, agreement
        // means it is stale and must be retired (finding L1). Visiting the path
        // used to be the whole test, which no fix to CQLite could ever falsify.
        let outcome = if compare_value_body(golden, cli, egress, ty, at).is_err() {
            Observed::Suppressed
        } else {
            Observed::Agreed
        };
        at.skips.observe(&at.path, outcome);
        return Ok(());
    }
    compare_value_body(golden, cli, egress, ty, at)
}

/// The comparison itself, with no exclusion check of its own at this level —
/// [`compare_value_at`] owns that, so the body can be run for an excluded path to
/// learn what the exclusion is suppressing. Nested positions still go through
/// [`compare_value_at`], so a deeper exclusion applies normally.
fn compare_value_body(
    golden: &Value,
    cli: &Value,
    egress: Egress,
    ty: &CqlType,
    at: &At<'_, '_>,
) -> Result<(), String> {
    // A column that is absent/null on BOTH sides, whatever its declared shape.
    if matches!(golden, Value::Null) && matches!(cli, Value::Null) {
        return Ok(());
    }
    match ty {
        // set / list / frozen collection: same shape both sides, order-sensitive.
        // Cassandra emits a collection in comparator order and the CLI reads it in
        // storage order, so a reordering IS a divergence, not a normalization.
        //
        // The two kinds differ ONLY in their elements' kinding, which is why they
        // are separate arms: a multicell SET's element is the stringified cell
        // path, a LIST's element is the cell VALUE and so keeps its natural JSON
        // kind.
        CqlType::Set(element) => compare_sequence(golden, cli, egress, ty, element, at.kinding, at),
        CqlType::List(element) => {
            compare_sequence(golden, cli, egress, ty, element, Kinding::Natural, at)
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
                // A tuple is always frozen: the whole value is one cell, so
                // every slot keeps its natural JSON kind.
                let slot = at.index(&i.to_string(), Kinding::Natural);
                compare_value_at(gi, ci, egress, ity, &slot)
                    .map_err(|why| format!("[{i}] {why}"))?;
            }
            Ok(())
        }
        // map: object in the dump, array of {"key","value"} pairs in the CLI (and
        // the CSV decoder produces that same pair spelling).
        CqlType::Map(key_ty, value_ty) => match (golden, cli) {
            (Value::Object(g), Value::Array(c)) => compare_map(g, c, egress, key_ty, value_ty, at),
            _ => Err(shape_error("map", golden, cli, egress)),
        },
        CqlType::Udt(udt) => match golden {
            Value::Object(g) => compare_udt(g, cli, egress, udt, at),
            _ => Err(shape_error(&udt.name, golden, cli, egress)),
        },
        // A scalar type: both sides canonicalized UNDER THAT TYPE, so the numeric
        // rule applies only where the DDL declares a number.
        _ => {
            let g = canon_typed(golden, egress, ty, at.depth, at.kinding)?;
            let c = canon_typed(cli, egress, ty, at.depth, at.kinding)?;
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

/// The shared list/set walk. `element_kinding` is the caller's statement of how
/// the GOLDEN spells this collection's elements (see [`column_kinding`]).
fn compare_sequence(
    golden: &Value,
    cli: &Value,
    egress: Egress,
    ty: &CqlType,
    element: &CqlType,
    element_kinding: Kinding,
    at: &At<'_, '_>,
) -> Result<(), String> {
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
        let member = at.index(&i.to_string(), element_kinding);
        compare_value_at(gi, ci, egress, element, &member).map_err(|why| format!("[{i}] {why}"))?;
    }
    Ok(())
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
fn compare_udt(
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

/// One `{"key":…,"value":…}` entry of the CLI's map/UDT spelling, with the key
/// left as the RAW value.
///
/// Deliberately does NOT stringify the key: doing so applied a text projection
/// before the declared key type could be applied, so a `map<text,…>` golden key
/// `"0"` compared equal to an incorrectly emitted JSON numeric key `0` — defeating
/// the typed comparison in the one place a map most needs it (issue #1491 review
/// finding F2).
fn pair(entry: &Value, egress: Egress) -> Result<(&Value, &Value), String> {
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
    // Both are present — the shape check above requires exactly these two keys —
    // so this is an invariant, stated rather than defaulted: `unwrap_or(&Null)`
    // would compare a structurally broken entry as a null-keyed null-valued one.
    match (object.get("key"), object.get("value")) {
        (Some(key), Some(value)) => Ok((key, value)),
        _ => Err(format!(
            "cli map entry lost its `key`/`value` between the shape check and the read: {}",
            brief(&describe(entry, egress))
        )),
    }
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
    at: &At<'_, '_>,
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
    // A map KEY is `Kinding::Stringified` on BOTH sides and wherever the map
    // appears, frozen or not: the golden renders a map as a JSON OBJECT, and a
    // JSON object's key can only be a string. So the kind carries no information
    // here and the declared key type is what decides equality.
    let canon_key = |v: &Value| -> Result<String, String> {
        canon_typed(v, egress, key_ty, Depth::Inside, Kinding::Stringified)
            .map(|canon| canon.describe())
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
        // A map VALUE is the cell value (`writeRawValue`), so it keeps its natural
        // JSON kind even when the key beside it was stringified.
        let entry = at.index(gk, Kinding::Natural);
        compare_value_at(gv, cv, egress, value_ty, &entry)
            .map_err(|why| format!("[{gk}] {why}"))?;
    }
    Ok(())
}

// ===========================================================================
// Reading the CLI's own egress back
// ===========================================================================

/// Parse `export --format json` output: a JSON array of row objects.
///
/// Parsed STRICTLY — see [`super::strict_json`]. `serde_json::Value`'s own parse
/// last-wins on a duplicate object key, so malformed egress repeating a column (or
/// a UDT field, or the `_type` discriminator) compared EQUAL to the golden
/// whenever the LAST occurrence happened to match, and the spurious one vanished
/// from the shape check and the cell count alike — the JSON half of finding J2,
/// reported as K2.
pub fn cli_json_rows(text: &str) -> Result<Vec<Row>, String> {
    let parsed: Value = super::strict_json::parse(text, "egress")?;
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
    // A repeated header USED to overwrite the earlier column of the same name while
    // building the row map, so egress carrying a duplicate column compared equal to
    // the golden whenever the LAST occurrence happened to match, and the spurious
    // column vanished from both the shape check and the cell count (issue #1491
    // review finding J2).
    for (i, name) in headers.iter().enumerate() {
        if let Some(first) = headers[..i].iter().position(|earlier| earlier == name) {
            return Err(format!(
                "CSV egress header row repeats the column `{name}` (fields {first} and \
                 {i}) — a duplicate header is malformed output, and keeping only one of \
                 the two would hide the other"
            ));
        }
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

/// The `<table>-<uuid>` directory holding this table's SSTable under an ALREADY
/// CHOSEN `sstables/` root, or an error naming that root.
///
/// Choosing the root is the caller's job — see `super::fixture_root`, where a
/// git-committed case is pinned to the checkout copy and only a fetched-corpus case
/// walks the candidate roots by evidence (#1491 finding J1, #3220).
pub fn fixture_dir_in(root: &Path, keyspace: &str, table: &str) -> Result<PathBuf, String> {
    let mut dirs = fixture_dirs_in(root, keyspace, table)?;
    if dirs.is_empty() {
        return Err(format!(
            "no {table}-* directory with a *-Data.db under {}",
            root.join(keyspace).display()
        ));
    }
    Ok(dirs.remove(0))
}

/// EVERY `<table>-<uuid>` directory holding a `*-Data.db` under `root/keyspace`,
/// in sorted order.
///
/// Returned as the whole set, not just the first, so a caller that compares one of
/// them can COUNT the narrowing and declare it instead of picking silently (issue
/// #1491 review finding L3).
pub fn fixture_dirs_in(root: &Path, keyspace: &str, table: &str) -> Result<Vec<PathBuf>, String> {
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
    Ok(matches)
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

/// The golden that describes the fixture's `*-Data.db`, PAIRED BY NAME:
/// `<gen>-Data.db` is described by `<gen>-Data.db.jsonl` and by no other file.
///
/// Two selections used to be silent here, and both could compare a CLI reading of
/// one SSTable against a dump of another (issue #1491 review finding L3):
///
///   * the lexicographically FIRST golden in the directory was taken, so a
///     directory holding `nb-1-…jsonl` next to a `nb-2-big-Data.db` compared the
///     wrong generation's dump — 26 committed fixture directories carry more than
///     one golden, so the shape is real even though no covered case has it today;
///   * a directory holding SEVERAL `*-Data.db` was accepted, and
///     [`stage_single_table`] copies the whole directory, so the CLI reads all of
///     them while one golden describes one. That is not narrowed coverage but an
///     unsound comparison, so it FAILS naming the files rather than being counted.
pub fn golden_path(fixture: &Path) -> Result<PathBuf, String> {
    let mut data_dbs: Vec<PathBuf> = Vec::new();
    let mut goldens: Vec<PathBuf> = Vec::new();
    for entry in std::fs::read_dir(fixture)
        .map_err(|e| format!("cannot read {}: {e}", fixture.display()))?
        .filter_map(Result::ok)
    {
        let path = entry.path();
        match path.file_name().and_then(|n| n.to_str()) {
            Some(name) if name.ends_with("-Data.db.jsonl") => goldens.push(path),
            Some(name) if name.ends_with("-Data.db") => data_dbs.push(path),
            _ => {}
        }
    }
    data_dbs.sort();
    goldens.sort();
    let names = |paths: &[PathBuf]| {
        paths
            .iter()
            .filter_map(|p| p.file_name().and_then(|n| n.to_str()))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let [data_db] = data_dbs.as_slice() else {
        return Err(format!(
            "{} holds {} *-Data.db files ({}) — the whole directory is staged as one \
             table, so the CLI would read all of them while a golden describes one; \
             this lane compares exactly one SSTable per case",
            fixture.display(),
            data_dbs.len(),
            if data_dbs.is_empty() {
                "none".to_string()
            } else {
                names(&data_dbs)
            }
        ));
    };
    let expected = PathBuf::from(format!("{}.jsonl", data_db.display()));
    if !expected.is_file() {
        return Err(format!(
            "no golden {} beside the SSTable it must describe{}",
            expected.display(),
            if goldens.is_empty() {
                String::new()
            } else {
                format!(
                    " (the directory holds {}, which describe other generations)",
                    names(&goldens)
                )
            }
        ));
    }
    Ok(expected)
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
