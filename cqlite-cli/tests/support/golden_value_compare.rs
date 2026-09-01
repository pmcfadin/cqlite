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

use super::schema::{Column, ColumnKind, CqlType, TableSchema};
use super::{
    canon_scalar, canon_typed, container, csv_container, Canon, Depth, Egress, Kinding, Row,
};
// The declared-gap bookkeeping lives with the divergence it books (see [`gap`]).
use gap::{Gap, Observed, SkipPaths, Suppressions};
use serde_json::{Map, Value};
use std::cell::RefCell;
use std::collections::BTreeSet;

/// The outcome of one table × one egress format.
#[derive(Debug, Default)]
pub struct Report {
    /// Human-readable divergences, each naming the row key and the column.
    pub diffs: Vec<String>,
    /// Cells actually value-compared IN FULL. Zero on a non-empty table is a
    /// failure the caller must treat as such (a comparison that compared nothing
    /// is vacuous). A cell in which a REFUSAL or a declared gap discarded part of
    /// the value is not counted — see [`count_cell`].
    pub compared_cells: usize,
    /// How many of [`Self::compared_cells`] were collection/UDT cells. Reported
    /// affirmatively in the census so "containers are covered" is a measurement
    /// rather than an assumption.
    pub container_cells: usize,
    /// Container cells holding at least one REFUSED position, because the
    /// golden's own content cannot survive the unquoted CSV rendering there (see
    /// `csv_container::node_refusal`). Counted, and named in
    /// [`Self::ambiguity_reasons`], so the narrowing is declared at run time
    /// rather than inferred from a silent gap.
    ///
    /// A refusal is decided at the NARROWEST node it destroys, so its reach is
    /// that node and not the cell: every unambiguous sibling and every enclosing
    /// level keeps being compared, and a divergence there is an ordinary diff
    /// (finding N3, at member granularity since finding P2). AT the refused node
    /// itself, exactly two things survive — the bracket frame and the body's
    /// EMPTINESS — and WHICH members the body holds does not; `csv_container`'s
    /// module doc states that residual exactly, having previously implied more
    /// (finding Q1). The CELL is not counted as compared coverage, because only
    /// part of its value was decided.
    pub ambiguous_container_cells: usize,
    /// One deduplicated `path (reason)` entry per refused POSITION — `s (…)` for a
    /// cell's own root node, `sl[0] (…)` for one indistinguishable member of it.
    pub ambiguity_reasons: Vec<String>,
    /// Declared skip paths that did NOT suppress THEIR DECLARED divergence in this
    /// table's walk, each with the cause: they agreed, they were never reached, the
    /// comparison there could not be evaluated, or what diverged there was not the
    /// divergence the gap declares. An exclusion that suppresses nothing either
    /// holds back coverage that has come back or describes the output wrongly, so
    /// the caller must treat a non-empty list as a failure. See [`SkipPaths`] and
    /// [`gap::Divergence`].
    pub stale_skips: Vec<String>,
}

/// The CSV positions this table's walk REFUSED, in walk order, each with its
/// fully-qualified path and cause.
///
/// Recorded at the granularity the walk itself has — per member, per depth — so an
/// indistinguishable NESTED member is refused where it lives while its siblings,
/// its container's member count and its bracket frame keep being compared (issue
/// #1491 review finding P2). Deciding it one node up is what let a golden `[[]]`
/// accept a CLI `[]`.
///
/// A separate channel from the value tree on purpose: a refusal is CONTROL
/// information about a position, and putting it into the decoded `Value` as a
/// sentinel would make it indistinguishable from data the egress could itself
/// produce.
#[derive(Default)]
struct Refusals {
    recorded: RefCell<Vec<(String, String)>>,
}

impl Refusals {
    /// How many refusals have been recorded so far. A caller takes this as a MARK
    /// before a subtree and compares it after, which is how one cell's (or one
    /// excluded path's) refusals are attributed to it.
    fn mark(&self) -> usize {
        self.recorded.borrow().len()
    }

    fn record(&self, path: &str, why: &str) {
        self.recorded
            .borrow_mut()
            .push((path.to_string(), why.to_string()));
    }

    /// The refusals recorded since `mark`, as `path (why)` entries.
    fn since(&self, mark: usize) -> Vec<String> {
        self.recorded
            .borrow()
            .iter()
            .skip(mark)
            .map(|(path, why)| format!("{path} ({why})"))
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
    /// Where a refusal at THIS position is recorded (see [`Refusals`]).
    refusals: &'s Refusals,
    /// Where a suppression by a declared gap is recorded (see [`Suppressions`]).
    suppressions: &'s Suppressions,
    /// The declared gap this position is INSIDE, if any (see [`ActiveGap`]).
    gap: Option<ActiveGap>,
}

/// The declared gap whose subtree the walk is currently inside.
///
/// A gap becomes active at its own path and stays active for every position BELOW
/// it, because that is where its divergence lives: the `set<double>` gap is
/// declared on the column `sf` and its divergence is at `sf[0]`, `sf[5]`, `sf[6]`.
/// A gap declared deeper REPLACES it, so the innermost declaration governs its own
/// subtree.
///
/// The root path travels with it because the root is the only node that records
/// what the gap did — [`Observed`] is keyed by the DECLARED path, and a message
/// naming `sf[6]` would name a path no gap declares.
#[derive(Clone)]
struct ActiveGap {
    root: String,
    divergence: gap::Divergence,
}

impl<'s, 'p> At<'s, 'p> {
    /// A whole column's value.
    fn column(
        name: &str,
        kinding: Kinding,
        skips: &'s SkipPaths<'p>,
        refusals: &'s Refusals,
        suppressions: &'s Suppressions,
    ) -> Self {
        At {
            depth: Depth::TopLevel,
            kinding,
            path: name.to_string(),
            skips,
            refusals,
            suppressions,
            gap: None,
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
            refusals: self.refusals,
            suppressions: self.suppressions,
            gap: self.gap.clone(),
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
            refusals: self.refusals,
            suppressions: self.suppressions,
            gap: self.gap.clone(),
        }
    }

    /// This same position, with the gap DECLARED here made active for its subtree.
    fn under_gap(&self, divergence: gap::Divergence) -> Self {
        At {
            gap: Some(ActiveGap {
                root: self.path.clone(),
                divergence,
            }),
            ..self.clone()
        }
    }

    /// Is this position the ROOT of the active gap — the path the caller declared?
    fn is_gap_root(&self) -> bool {
        self.gap
            .as_ref()
            .is_some_and(|active| active.root == self.path)
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
    skip_columns: &[Gap<'_>],
    egress: Egress,
) -> Report {
    let mut report = Report::default();
    let skips = SkipPaths::new(skip_columns);
    let refusals = Refusals::default();
    let suppressions = Suppressions::default();
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
    // ROW ORDER, before the pairing sort discards it — the same rule
    // [`compare_map`] applies to a map's entries (finding N2), one level up.
    //
    // A CONDITIONAL property, so its preconditions are stated rather than left to
    // be rediscovered (issue #1491 review finding U1, which read this as an
    // unguaranteed assertion — the earlier justification here was "measured 56/56
    // today", which is CQLite's own output standing in for an oracle, exactly what
    // #3042 forbids).
    //
    // NOT guaranteed anywhere as a public contract. No doc comment on `cqlite
    // export`, no user-facing doc and no CLI help text states an export row order;
    // `output_determinism_regression_tests.rs` pins COLUMN order only (the
    // `metadata.columns` sequence, and independence from HashMap iteration), never
    // row order.
    //
    // GOLDEN side — authoritative, non-CQLite:
    //   * `cassandra-5.0.8 io/sstable/format/SortedTableWriter.java:175` throws
    //     unless each appended `DecoratedKey` is strictly greater than the last, so
    //     a Cassandra-written `Data.db` IS in `(token, key)` order on disk;
    //   * within a partition, rows are in clustering order (guide ch.5, "Row
    //     Ordering");
    //   * `cassandra-5.0.8 tools/SSTableExport.java:179` dumps ONE SSTable through
    //     `sstable.getScanner()` and streams the partitions to
    //     `JsonTransformer.toJsonLines` in encounter order, so the golden's LINE
    //     order is that on-disk order.
    //
    // CLI side — a cqlite-core INVARIANT (CQLite source is evidence of what CQLite
    // does, never of what is correct, #3041 — hence a precondition, not a claim
    // about the format): every read path yields `(token, key)` order. The
    // materializing `scan` sorts explicitly, with a STABLE `sort_by`, so rows
    // sharing a partition key keep their file sequence
    // (`reader/data_access/model.rs:434`); cross-generation reconciliation is a
    // k-way TOKEN merge; and the single-generation lazy stream that `export` takes
    // is on-disk order under a RELEASE-ACTIVE guard that logs loudly and falls back
    // to the authoritative scan if a prefix is not `(token, key)`-monotonic
    // (`select_executor/limit_pushdown/mod.rs`, issue #1577).
    //
    // PRECONDITION this lane enforces: exactly ONE Cassandra-written SSTable per
    // case, paired with the golden that names it — `staging::golden_path` FAILS,
    // naming the files, on a fixture directory holding any other number of
    // `*-Data.db` (finding L3). So the reordering this cannot see — a
    // cross-generation merge — cannot reach the comparison.
    //
    // A RED here is therefore one of: CQLite stopped emitting `(token, key)` order
    // (a #1577 invariant violation, not a licensed change); the golden is paired
    // with a different SSTable; or `export` gained a sort, which is a deliberate
    // contract change that must update this pin.
    if let Some(why) = row_order_divergence(&golden, &cli, pk, ck, schema, egress) {
        report.diffs.push(why);
    }
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
            // its DECLARED divergence (or not) — see [`SkipPaths`] and
            // [`gap::Divergence`]. What it suppresses is that one divergence, and
            // with it this cell's compared-cell COUNTERS: a cell part of whose
            // value was discarded is not compared coverage.
            //
            // Does a gap name this WHOLE column? A dotted `col.field` entry does
            // not, so its column keeps being compared and counted; the field is
            // reached inside the walk.
            let column_has_gap = skips.excludes(name);
            // Where this cell's own refusals and gap suppressions start (see
            // [`Refusals::mark`] and [`Suppressions::mark`]).
            let cell_mark = refusals.mark();
            let cell_suppression_mark = suppressions.mark();
            // The CLI must render EVERY declared column. An omitted one is a
            // divergence, NOT an implicit null: reading it as null is what made
            // the absent-cell property untestable.
            //
            // A declared exclusion CANNOT suppress this. A `SkipPaths` entry
            // excludes a VALUE at a path — it says "the two sides disagree about
            // what is rendered there" — and it has no licence to excuse the
            // column not being rendered AT ALL, which is a divergence of the
            // egress SHAPE that every case asserts (the comparator's contract is
            // that every DDL column is rendered). Recording the omission as
            // `Suppressed` is what let the five declared skips hide a regression
            // that dropped their column altogether, while `Observed::Unresolved`'s
            // own documentation already named an absent egress column as the
            // case it cannot measure (issue #1491 review finding P1).
            let Some(cv) = c.get(name) else {
                if shape_seen.insert(format!("missing:{name}")) {
                    report.diffs.push(format!(
                        "row[{key}].{name}: absent from the {egress:?} egress row — the \
                         committed CREATE TABLE {} declares `{name}` ({}), so it must be \
                         rendered (a null cell as `null`, an empty CSV field){}",
                        schema.table,
                        column.ty.describe(),
                        if column_has_gap {
                            " — the declared gap for this path excludes its VALUE, not \
                             the column's PRESENCE"
                        } else {
                            ""
                        }
                    ));
                }
                if column_has_gap {
                    // There is no value at that path to compare, so what the
                    // exclusion suppresses cannot be read off this row: the gap is
                    // UNRESOLVED, never applied. Reported as its own cause by
                    // [`SkipPaths::stale`], which cannot contradict the diff above
                    // — the two say the same thing (the column is missing, and the
                    // gap could not be measured), and both are failures.
                    skips.observe(
                        name,
                        Observed::Unresolved(format!(
                            "the {egress:?} egress row carries no `{name}` column at all"
                        )),
                    );
                }
                continue;
            };
            // Only the GOLDEN may default: `sstabledump` omits a cell that was
            // never written, so a missing golden cell IS the expected null.
            let gv = g.get(name).unwrap_or(&Value::Null);
            // CSV has no types, so a container arrives as one flat text field and
            // has to be decoded back into the golden's shape before comparison.
            let decoded = match csv_decoded(
                gv,
                cv,
                egress,
                &column.ty,
                name,
                &skips,
                column_kinding(column),
            ) {
                Ok(decoded) => decoded,
                Err(why) => {
                    // The CLI's text is not the grammar the declared type states
                    // at all. Reachable ONLY for a column no gap names: an
                    // excluded path's text is never required to invert the grammar
                    // — `csv_container::decode_at` falls back to raw text there
                    // (finding F5) — and that raw text then reaches
                    // [`compare_value_at`], where the gap's DECLARED divergence is
                    // asked of it like any other value. So there is nothing for a
                    // gap to suppress here, and no second copy of the gap rule.
                    report.compared_cells += 1;
                    report.container_cells += 1;
                    report.diffs.push(format!(
                        "row[{key}].{name}: unparseable CSV container: {why}"
                    ));
                    continue;
                }
            };
            let cv = decoded.as_ref().unwrap_or(cv);
            let at = At::column(
                name,
                column_kinding(column),
                &skips,
                &refusals,
                &suppressions,
            );
            // `compare_value_at` suppresses (and RECORDS) the DECLARED divergence
            // at an excluded path; a divergence the gap does not declare reaches
            // `diffs` here like any other.
            let outcome = compare_value_at(gv, cv, egress, &column.ty, &at);
            // The counters are read AFTER the walk, because whether this cell was
            // fully decided is only known once its members have been visited: a
            // refusal — or a declared gap firing — anywhere inside it means a
            // proper PART of the value was compared, which is not container
            // coverage (finding N3, now at member granularity — finding P2).
            count_cell(
                &mut report,
                &refusals,
                cell_mark,
                &suppressions,
                cell_suppression_mark,
                gv,
            );
            if let Err(why) = outcome {
                report.diffs.push(format!("row[{key}].{name}: {why}"));
            }
        }
    }
    report.stale_skips = skips.stale();
    report
}

/// Count ONE cell into the census, once its walk is finished.
///
/// A cell holding at least one REFUSED position is counted as refused and NOT as
/// compared coverage: what was decided there is a proper part of the value, so
/// calling it a compared container cell would overstate the measurement. Every
/// refused position is named (`path (why)`), deduplicated, so the census states
/// the gap at the granularity the walk found it.
///
/// A cell in which a DECLARED GAP suppressed its divergence is not counted either,
/// for the same reason and by the same test — did it fire INSIDE THIS CELL, rather
/// than "is this column named in a gap". That distinction is the census half of
/// review round 17's finding: the `nb_empty_collections` rows whose multi-cell
/// collections are NON-EMPTY are compared in full and now count as coverage, while
/// a whole-cell exclusion is keyed on what actually happened rather than on the
/// declaration. A refusal is still counted and named even under a gap, because a
/// refusal is a property of the golden and the format, not of the exclusion.
fn count_cell(
    report: &mut Report,
    refusals: &Refusals,
    mark: usize,
    suppressions: &Suppressions,
    suppression_mark: usize,
    gv: &Value,
) {
    let refused = refusals.since(mark);
    if !refused.is_empty() {
        report.ambiguous_container_cells += 1;
        for entry in refused {
            if !report.ambiguity_reasons.contains(&entry) {
                report.ambiguity_reasons.push(entry);
            }
        }
        return;
    }
    if suppressions.any_since(suppression_mark) {
        return;
    }
    report.compared_cells += 1;
    if matches!(gv, Value::Array(_) | Value::Object(_)) {
        report.container_cells += 1;
    }
}

/// The first position where the two sides' EMITTED row order differs, or `None`.
///
/// Keyed on the primary key alone (the rows are paired by it), and reported ONCE
/// with the first divergent position rather than per row: a single moved row makes
/// every later position differ, and 900 lines saying so name nothing.
///
/// # The key is TYPED, under the declared key-column types
///
/// Each component goes through `canon_typed` with its `CREATE TABLE` type and with
/// the same ASYMMETRY [`compare_value_at`] applies to a cell value: the GOLDEN's
/// spelling is the one [`column_kinding`] states (`sstabledump` writes every
/// partition-key component with `writeString`, so an `int` key arrives as `"1"`),
/// while the CLI is held to [`Kinding::Natural`] at every position.
///
/// It used to be [`row_message_key`], the PERMISSIVE untyped projection the pairing
/// uses, and that could not see a real reordering (issue #1491 review finding V2).
/// Untyped, `canon_text` reads any numeric-looking string as a number, so two
/// DISTINCT legal `text` keys — `"1"` and `"1.0"` — produce the same key: swapping
/// those two rows was invisible here, and then invisible altogether, because the
/// pairing sort that follows re-sorts both sides by a key that embeds the whole row
/// and hands the comparison a matched pair. A `text` column holding `"1"` beside one
/// holding `"1.0"` is an ordinary table, so this was not an exotic gap.
///
/// A component that cannot be canonicalized under its declared type, or a key column
/// the committed DDL does not declare, is REPORTED rather than swallowed: without a
/// canonical key for every key column there is no order to compare, and a `<reason>`
/// string would meet an identical `<reason>` on the other side and compare equal.
///
/// # The key is the FULL structured value; only the MESSAGE is truncated
///
/// Each component is kept as its `(column, Canon)` pair and compared AS THAT VALUE.
/// It used to be `format!("{name}={}", brief(&canon.describe()))` — the diagnostic
/// rendering — so two DISTINCT keys of equal length sharing their first 120
/// characters collapsed onto ONE key and a reordering of those rows passed unnoticed
/// (issue #1491 review round 24, finding DD1). The corpus reaches that length: the
/// wide-row tables carry multi-hundred-character `text` keys and 4 KiB blobs.
/// [`render_order_key`] applies [`brief`] when the failure is FORMATTED, which is
/// where a truncation belongs — the same rule as `display()`/`to_string_lossy()` in
/// a path (findings W2/L3): fine in a message, never in a decision.
///
/// Only a REORDERING is reported — the same keys in a different sequence. Differing
/// keys are a divergence of a key column's VALUE, which the typed per-row comparison
/// names; see the comment at the check itself for why an order line there would state
/// a cause that is not the cause.
fn row_order_divergence(
    golden: &[&Row],
    cli: &[&Row],
    pk: &[&str],
    ck: &[&str],
    schema: &TableSchema,
    egress: Egress,
) -> Option<String> {
    // `golden_side` selects the kinding, which is a statement about the GOLDEN's
    // spelling alone (see [`column_kinding`] and finding M1).
    let key = |r: &&Row, golden_side: bool| -> Result<Vec<(String, Canon)>, String> {
        let mut parts: Vec<(String, Canon)> = Vec::new();
        for name in pk.iter().chain(ck.iter()) {
            let column = schema.column(name).ok_or_else(|| {
                format!(
                    "the case names key column `{name}`, which the committed CREATE TABLE \
                     {} does not declare",
                    schema.table
                )
            })?;
            let kinding = if golden_side {
                column_kinding(column)
            } else {
                Kinding::Natural
            };
            let value = r.get(*name).unwrap_or(&Value::Null);
            let canon = canon_typed(value, egress, &column.ty, Depth::TopLevel, kinding)
                .map_err(|why| format!("key column `{name}`: {why}"))?;
            parts.push(((*name).to_string(), canon));
        }
        Ok(parts)
    };
    type OrderKey = Vec<(String, Canon)>;
    let keys = |rows: &[&Row], golden_side: bool| -> Result<Vec<OrderKey>, String> {
        rows.iter().map(|r| key(r, golden_side)).collect()
    };
    let (g, c) = match (keys(golden, true), keys(cli, false)) {
        (Ok(g), Ok(c)) => (g, c),
        (Err(why), _) | (_, Err(why)) => {
            return Some(format!(
                "row order: the emitted order cannot be compared — {why}"
            ))
        }
    };
    let at = g.iter().zip(c.iter()).position(|(a, b)| a != b)?;
    // A REORDERING is the SAME keys in a different sequence, and that is the only
    // shape reported here. When the two sides carry DIFFERENT keys the divergence is
    // in a key column's VALUE, not in the order, and the typed comparison below
    // names it per row — so reporting an order line too would state a cause that is
    // not the cause ("the read path stopped emitting `(token, key)` order"), which is
    // the false-divergence class this lane treats as a defect in its own right
    // (finding T1). Measured while making this key typed: an `int` partition key
    // wrongly rendered `"1"` produced a second, spurious `row order:` line beside the
    // spelling diff it really was.
    //
    // It cannot hide a reordering: differing key sets mean some pair disagrees on a
    // key column, which the value comparison reports, so the case still fails.
    let mut g_sorted = g.clone();
    let mut c_sorted = c.clone();
    g_sorted.sort();
    c_sorted.sort();
    if g_sorted != c_sorted {
        return None;
    }
    Some(format!(
        "row order: the {egress:?} egress emits row {at} as `{}` where the golden \
         emits `{}` — both sides walk ONE Cassandra-written SSTable, which is in \
         `(token, key)` order on disk, and the dump emits its partitions in that \
         order (see `compare_rows` for the invariant and its preconditions); so \
         either the read path stopped emitting `(token, key)` order (issue #1577) \
         or this golden describes a different SSTable",
        render_order_key(&c[at]),
        render_order_key(&g[at])
    ))
}

/// One row-order key AS A DIAGNOSTIC: each component's canonical value, truncated
/// by [`brief`].
///
/// The comparison never reads this. [`row_order_divergence`] compares the
/// `(column, Canon)` components themselves, so a truncation here cannot make two
/// distinct keys equal — which is exactly what it did while this rendering WAS the
/// key (finding DD1). The truncation stays in the message on purpose: a wide-row key
/// is multi-kilobyte, and `brief` names the full length it dropped
/// (`…(N chars total)`), so the reader is never told less than that it was cut.
fn render_order_key(key: &[(String, Canon)]) -> String {
    key.iter()
        .map(|(name, canon)| format!("{name}={}", brief(&canon.describe())))
        .collect::<Vec<_>>()
        .join(",")
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

/// Decode a CSV cell whose golden counterpart is a container. `Ok(None)` means
/// no decoding applies — the JSON lane, a scalar column, or a CSV cell that is
/// not text (an empty field decodes to `null`, and [`compare_value_at`] is what
/// then names that shape mismatch).
///
/// The single `Err` is UNPARSEABLE: the CLI's text is not the grammar at all
/// (wrong bracket, unbalanced brackets, a map entry with no `: `). That IS a
/// divergence, so it is reported as one rather than refused.
///
/// NO refusal is decided here, which is the whole of review round 12's finding
/// S1: there used to be a whole-CELL tier for an UNBALANCED bracket, scanned per
/// scalar, and it refused cells whose outer levels were perfectly decidable —
/// balance is a property of the CONCATENATED rendering, not of each scalar. Every
/// refusal is now taken per NODE, by the decoder and the comparator together as
/// they walk (`csv_container::node_refusal`, finding P2), including at the cell's
/// own root node; an imbalance is one way that node's derived question fails. So
/// the decoder is never asked to split a text a CORRECT CLI would render
/// unbalanced: such a node is refused first, and the split is not attempted.
fn csv_decoded(
    gv: &Value,
    cv: &Value,
    egress: Egress,
    ty: &CqlType,
    path: &str,
    skips: &SkipPaths<'_>,
    kinding: Kinding,
) -> Result<Option<Value>, String> {
    if egress != Egress::Csv || !matches!(gv, Value::Array(_) | Value::Object(_)) {
        return Ok(None);
    }
    let Value::String(text) = cv else {
        return Ok(None);
    };
    // The decoder is given the exclusion set so an EXCLUDED member is left as raw
    // text instead of being required to invert the grammar. Without it a single
    // excluded inner field fails the whole cell, which is what forced
    // `udt_nested`'s exclusion to be whole-column (review finding F5).
    // The decoder is entered at the COLUMN's own kinding, which is exactly what
    // `At::column` gives the comparator below, so the two ask `node_refusal` the
    // same question at the same node (the drift this lane's history is made of).
    csv_container::decode_at(gv, text, ty, path, &|p: &str| skips.excludes(p), kinding).map(Some)
}

/// A total, side-independent PAIRING key: the canonical primary key, then the whole
/// canonicalized row as a tie-break so pairing stays deterministic even if a fixture
/// ever carried duplicate keys.
///
/// Deliberately the UNTYPED projection: pairing has to see through `sstabledump`'s
/// two spellings of one value or it pairs the wrong rows (finding T1). The emitted
/// ROW ORDER is a verdict, not a pairing, and is compared under the declared types
/// before this sort runs (see [`row_order_divergence`], finding V2).
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

/// A stable textual description of any value, for the PAIRING sort and diagnostics
/// only — never for a verdict. It reads the untyped projection (see
/// `super::canon_text`), so it collapses distinctions a declared type keeps.
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
///
/// # A DECLARED GAP suppresses the divergence it NAMES, and nothing else
///
/// Inside a declared gap's subtree every node is asked whether the pair there is
/// EXACTLY the divergence the gap declares ([`gap::Divergence`]). If it is, the
/// divergence is suppressed and the gap is recorded as applied. If it is not, the
/// comparison stands and the divergence is reported as an ordinary diff naming the
/// path, the declared divergence and what was seen.
///
/// Until review round 17 the gap swallowed ANY divergence at its path, and
/// `Observed::Suppressed` then dominated table-wide — so each declared gap was a
/// permanent blind spot for its whole column: the empty-collection gaps also
/// suppressed the NON-EMPTY rows of those columns, and `e.home` changing from blob
/// hex to arbitrary text would have passed as the documented gap.
fn compare_value_at(
    golden: &Value,
    cli: &Value,
    egress: Egress,
    ty: &CqlType,
    at: &At<'_, '_>,
) -> Result<(), String> {
    // A gap DECLARED at this exact path becomes active for its whole subtree; an
    // inherited one stays active (see [`ActiveGap`]).
    let entered;
    let at = match at.skips.declared(&at.path) {
        Some(divergence) => {
            entered = at.under_gap(divergence);
            &entered
        }
        None => at,
    };
    let Some(gap) = at.gap.clone() else {
        return compare_value_body(golden, cli, egress, ty, at);
    };
    // A CSV node whose GOLDEN content the flat rendering cannot express is not
    // decided at all — only its bracket frame and its body's EMPTINESS are (see
    // `csv_container::node_refusal`) — so no verdict taken there is a measurement,
    // INCLUDING "this is the declared divergence". The refusal therefore wins over
    // the match, the body records it, and the gap is reported unevaluable: a gap
    // whose subject could not be measured is not a measured gap. That composition
    // is deliberate and conservative — it FAILS the lane rather than minting a
    // suppression out of a partially-decided node — and it is what keeps the
    // earlier rule ("a refusal at ANY depth makes the gap `Unresolved`") true.
    let refused_here = egress == Egress::Csv
        && csv_container::node_refusal(golden, Some(ty), at.kinding).is_some();
    // IS THIS NODE THE DECLARED DIVERGENCE? Asked at every node of the gap's
    // subtree, because that is where the divergence lives: the `set<double>` gap
    // is declared on the column and diverges at three of its seven members.
    if !refused_here
        && gap
            .divergence
            .matched(golden, cli, ty, egress, at.depth, at.kinding)
    {
        at.skips.observe(&gap.root, Observed::Suppressed);
        at.suppressions.record(&gap.root);
        return Ok(());
    }
    // Not the declared divergence, so COMPARE AS NORMAL. The recursion re-enters
    // here for every child with the gap still active, so a divergence deeper down
    // gets the same question asked of it, and anything the gap does not declare
    // travels back up as an ordinary error.
    let refusal_mark = at.refusals.mark();
    let suppression_mark = at.suppressions.mark();
    let outcome = compare_value_body(golden, cli, egress, ty, at);
    if !at.is_gap_root() {
        // Only the gap's ROOT records what the gap did and annotates a surviving
        // divergence: [`Observed`] is keyed by the declared path, and annotating at
        // every enclosing level would repeat the same sentence up the tree.
        return outcome;
    }
    match outcome {
        Ok(()) => {
            if at.suppressions.root_since(suppression_mark, &gap.root) {
                // The declared divergence was found DEEPER in this subtree and
                // recorded there; this node has nothing to add.
                return Ok(());
            }
            // Nothing in the subtree diverged. Two causes, kept apart: a refusal
            // means the comparison could not be decided ("I could not tell" is not
            // "the two sides agree"), and otherwise the two sides really do agree,
            // so the gap is stale and must be retired (finding L1). Visiting the
            // path used to be the whole test, which no fix to CQLite could falsify.
            let observed = match at.refusals.since(refusal_mark).first() {
                Some(refused) => {
                    Observed::Unresolved(format!("the CSV comparison there was refused: {refused}"))
                }
                None => Observed::Agreed,
            };
            at.skips.observe(&gap.root, observed);
            Ok(())
        }
        Err(why) => {
            // A REAL divergence that the declared gap does not describe. Reported
            // as an ordinary diff naming the path, the declared divergence and what
            // was actually seen — which is the whole of review round 17's finding:
            // a gap that suppresses any divergence at its path is a permanent blind
            // spot for its column, and the honesty of the declaration is nominal.
            at.skips.observe(
                &gap.root,
                Observed::Undeclared(format!(
                    "{} — where the gap declares: {}",
                    brief(&why),
                    gap.divergence.declared()
                )),
            );
            Err(format!(
                "{why} — and that is NOT the divergence the declared gap for `{}` stands \
                 for, which is: {}. A declared gap suppresses only the divergence it names",
                gap.root,
                gap.divergence.declared()
            ))
        }
    }
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
    // CSV only: a position whose GOLDEN content the flat rendering cannot express
    // unambiguously (`csv_container::node_refusal`). The refusal is recorded and
    // what survives it is compared — the frame, which the decoder already required
    // at this depth, and the body's EMPTINESS. WHICH members the body holds is
    // NOT compared at this node; that residual is stated exactly in
    // `csv_container`'s module doc (finding Q1).
    //
    // Asked HERE, at every node of the same recursion the comparison walks, which
    // is the whole point of finding P2: the decision is made at the granularity of
    // the comparison, so this node's siblings, its container's member count, its
    // bracket kind and every enclosing level keep being compared. It cannot fire
    // for JSON, which carries its own structure and needs no decoding.
    if egress == Egress::Csv {
        if let Some(why) = csv_container::node_refusal(golden, Some(ty), at.kinding) {
            at.refusals.record(&at.path, &why);
            return csv_container::decidable_despite_node_refusal(golden, cli);
        }
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
        //
        // And only a TOP-LEVEL set's elements can be stringified, which is why the
        // kinding is taken from `at` only there. A stringified set is a MULTICELL
        // one, and a non-frozen collection can only be a whole column; a set
        // NESTED inside another container is frozen, so its elements live in one
        // value cell and keep their natural kind. Propagating the column's kinding
        // inward was harmless only by accident — the dump writes a multicell
        // `set<frozen<set<int>>>`'s cell path as ONE string, so the member fails
        // the array shape check before any kinding applies — and an accident is
        // not a rule.
        CqlType::Set(element) => {
            let element_kinding = match at.depth {
                Depth::TopLevel => at.kinding,
                Depth::Inside => Kinding::Natural,
            };
            compare_sequence(golden, cli, egress, ty, element, element_kinding, at)
        }
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
            Value::Object(g) => udt::compare_udt(g, cli, egress, udt, at),
            _ => Err(shape_error(&udt.name, golden, cli, egress)),
        },
        // A scalar type: both sides canonicalized UNDER THAT TYPE, so the numeric
        // rule applies only where the DDL declares a number — and ASYMMETRICALLY
        // by side, because the two sides are under different constraints.
        //
        // `at.kinding` is a statement about the GOLDEN's spelling alone (see
        // [`column_kinding`]): `sstabledump` writes a partition key and a
        // multicell cell path with `writeString`, so a numeric golden STRING there
        // denotes a number. The CLI's JSON is under no such constraint — it
        // renders a numeric column as a JSON number — so the CLI side is held to
        // `Kinding::Natural`, i.e. to its declared type's JSON kind, at EVERY
        // position.
        //
        // Applying the golden's relaxation to both sides made the mechanism
        // symmetric, so at a stringified position an egress regression that
        // rendered `"id":"1"` for an `int` partition key still compared equal
        // (issue #1491 review finding M1). [`compare_map`] applies the same
        // asymmetry to a map KEY: the golden's object key is stringified by the
        // format, the CLI's `{"key","value"}` key is not (finding N1).
        _ => {
            let g = canon_typed(golden, egress, ty, at.depth, at.kinding)?;
            let c = canon_typed(cli, egress, ty, at.depth, Kinding::Natural)?;
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

/// One `{"key":…,"value":…}` entry of the CLI's map/UDT spelling, with the key
/// left as the RAW value.
///
/// Deliberately does NOT stringify the key: doing so applied a text projection
/// before the declared key type could be applied, so a `map<text,…>` golden key
/// `"0"` compared equal to an incorrectly emitted JSON numeric key `0` — defeating
/// the typed comparison in the one place a map most needs it (issue #1491 review
/// finding F2).
///
/// `pub` so `super::container` reads the SAME entry spelling when it canonicalizes a
/// map (issue #3726): a second `{key,value}` reader would be a second notion of what
/// the egress's map entry is, and the two could then disagree about a malformed one.
pub fn pair(entry: &Value, egress: Egress) -> Result<(&Value, &Value), String> {
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

/// Compare a map: golden object vs the CLI's `{key,value}` pair list, IN EMITTED
/// ORDER, each key canonicalized UNDER THE DECLARED KEY TYPE — so a `map<int,…>`
/// matches the golden's `"-5"` with the CLI's `-5`, while a `map<text,…>` compares
/// its keys exactly AND by JSON kind, so a numeric key `0` does not satisfy the
/// golden's `"0"`.
///
/// # Emitted order, because a map's order is not free
///
/// Cassandra stores a map's entries sorted by the key's comparator — a multicell
/// map's cells are keyed by `CellPath`, and a frozen map is serialized in that
/// same order — and `sstabledump` emits them in that on-disk order, which the
/// golden reader preserves (`serde_json`'s `preserve_order` is on workspace-wide,
/// and a multicell map is rebuilt cell by cell in dump order). A reader walking
/// the same SSTable therefore has no licence to emit a different order, so a
/// reordering is a DIVERGENCE.
///
/// Sorting both sides by canonicalized key before comparing (the previous rule)
/// discarded exactly that: reversing the CLI's entries compared equal, while the
/// CSV decoder's own documentation claimed member order was compared against the
/// golden (issue #1491 review finding N2). The sibling [`compare_sequence`] has
/// always compared list/set members positionally for the same reason, so nothing
/// in this walk is order-insensitive now.
///
/// The golden's keys are JSON object keys, hence always strings; the CLI's keep
/// whatever kind the egress gave them. Both go through `canon_typed(…, key_ty, …)`
/// — the golden's under [`Kinding::Stringified`], the CLI's under
/// [`Kinding::Natural`] — which is what makes the kind comparison possible.
fn compare_map(
    golden: &Map<String, Value>,
    cli: &[Value],
    egress: Egress,
    key_ty: &CqlType,
    value_ty: &CqlType,
    at: &At<'_, '_>,
) -> Result<(), String> {
    // A key canonicalization FAILURE is propagated, never swallowed into the
    // comparison key: a `<reason>` string would still meet an identical
    // `<reason>` on the other side and compare equal.
    //
    // ASYMMETRIC, exactly as for a cell value (finding M1) and for the same
    // reason. A JSON object's key can only be a string, so the GOLDEN's map key
    // is stringified BY THE FORMAT and says nothing about kind: it is read with
    // `Kinding::Stringified`. The CLI is under no such constraint — it spells a
    // map as an ARRAY of `{"key":…,"value":…}` objects, whose `key` keeps the JSON
    // kind of its declared type — so the CLI key is held to `Kinding::Natural`,
    // i.e. to the kind its declared key type implies. Relaxing BOTH sides made a
    // regression from the `map<int,…>` key `-5` to the string `"-5"` compare equal
    // (issue #1491 review finding N1).
    //
    // The key is the `Canon` VALUE, never `Canon::describe()`: `describe` is the
    // DIAGNOSTIC rendering, and a rendering used as a comparison key can only be as
    // faithful as its spelling happens to be — the same class as finding DD1, where
    // the row-order key's `brief(&canon.describe())` made two long distinct keys
    // equal. Here the pair is compared as the structured value and rendered only
    // into the message below.
    let canon_golden_key = |key: &str| -> Result<Canon, String> {
        // WHAT THE GOLDEN'S OBJECT KEY DENOTES is asked of ONE function
        // (`container::golden_map_key_value`), which the canonical model and the CSV
        // rendering also call: for a SCALAR key type it is the key text itself, and
        // for a CONTAINER key type it is that text PARSED, because
        // `cassandra-5.0.8 MapType.toJSONString` writes
        // `keys.toJSONString(kv, protocolVersion)` and only quotes it when it does
        // not already start with `"` — so a container key's object key is exactly the
        // key value's own toJSONString document (issue #3726). Two spellings of that
        // rule would be two notions of what the golden key is.
        let value = container::golden_map_key_value(key, key_ty)?;
        canon_typed(
            &value,
            egress,
            key_ty,
            Depth::Inside,
            container::golden_map_key_kinding(key_ty),
        )
    };
    let canon_cli_key = |v: &Value| -> Result<Canon, String> {
        canon_typed(v, egress, key_ty, Depth::Inside, Kinding::Natural)
    };
    let mut g: Vec<(Canon, &Value)> = Vec::with_capacity(golden.len());
    for (k, v) in golden {
        g.push((canon_golden_key(k)?, v));
    }
    let mut c: Vec<(Canon, &Value)> = Vec::with_capacity(cli.len());
    for entry in cli {
        let (key, value) = pair(entry, egress)?;
        c.push((canon_cli_key(key)?, value));
    }
    if g.len() != c.len() {
        return Err(format!("map size golden {} vs cli {}", g.len(), c.len()));
    }
    for (i, ((gk, gv), (ck, cv))) in g.iter().zip(c.iter()).enumerate() {
        if gk != ck {
            return Err(format!(
                "map key at emitted position {i}: golden {} vs cli {} — a map's \
                 entries are compared in EMITTED order, which is the key-comparator \
                 order both the dump and a reader of the same SSTable see (golden \
                 keys [{}], cli keys [{}])",
                brief(&gk.describe()),
                brief(&ck.describe()),
                keys_of(&g),
                keys_of(&c)
            ));
        }
        // A map VALUE is the cell value (`writeRawValue`), so it keeps its natural
        // JSON kind even when the key beside it was stringified.
        //
        // The PATH takes the key UNTRUNCATED: a declared gap is matched against it by
        // exact string (see [`gap::SkipPaths::declared`]), so truncating it here would
        // silently merge the paths of two long keys — DD1 again, one level down. Only
        // the message prefix is truncated.
        let key_text = gk.describe();
        let entry = at.index(&key_text, Kinding::Natural);
        compare_value_at(gv, cv, egress, value_ty, &entry)
            .map_err(|why| format!("[{}] {why}", brief(&key_text)))?;
    }
    Ok(())
}

/// The canonical keys of one side of a map, in emitted order, for the ordering
/// diagnostic above — a bare "golden X vs cli Y" at position 3 does not say
/// whether the entry is missing, extra or merely moved.
///
/// Each key is rendered through [`brief`] because a map key may be a 4 KiB blob and
/// this line lists EVERY key. That truncation is confined to this message: the keys
/// themselves are compared as `Canon` values (see [`compare_map`] and finding DD1).
fn keys_of(entries: &[(Canon, &Value)]) -> String {
    entries
        .iter()
        .map(|(k, _)| brief(&k.describe()))
        .collect::<Vec<_>>()
        .join(", ")
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
// The UDT comparison
// ===========================================================================
//
// Split into its own file under the campsite rule and reached as `udt::…` from
// `compare_value_at`. A UDT is the one value shape whose member set is fixed by a
// SECOND committed DDL statement (`CREATE TYPE`), which is a different question
// from the structural walk here.

#[path = "golden_value_compare_udt.rs"]
mod udt;

// ===========================================================================
// Declared-gap divergences
// ===========================================================================
//
// Split into its own file under the campsite rule and reached as `gap::…`. WHAT a
// declared gap says the divergence IS — and what the walk observed it to do — is a
// different question from the structural walk here: the declaration is stated from
// the golden and the committed DDL, and this file only asks it. `Observed`,
// `SkipPaths` and `Suppressions` live there with it and are imported at the top.

#[path = "golden_value_compare_gap.rs"]
pub mod gap;

// ===========================================================================
// Fixture staging
// ===========================================================================
//
// Split into its own file under the campsite rule and RE-EXPORTED here, so
// `compare::golden_path` and `compare::stage_single_table` keep naming the same
// items: locating and staging a fixture is a filesystem question, not a
// comparison one. (`fixture_dir_in`/`fixture_dirs_in` are reached as
// `compare::staging::…`, which is where `golden_fixture_root` already names
// them.)

#[path = "golden_fixture_staging.rs"]
pub mod staging;

pub use staging::{golden_path, stage_single_table};

#[cfg(test)]
#[path = "golden_value_compare_tests.rs"]
mod tests;
