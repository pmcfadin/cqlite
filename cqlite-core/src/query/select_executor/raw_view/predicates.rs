//! Predicate-scoping helpers for the raw SSTable view (issue #4222) — split
//! out of `mod.rs` (roborev finding, round 7: a new source file over the
//! gate's 800-line `file-size` ratchet) so the free predicate helpers and
//! their ~250 lines of no-corpus unit tests live separately from the
//! interception/entry-point `impl SelectExecutor` block.
//!
//! [`row_passes_predicates`] is re-exported at the `raw_view` level (see
//! `mod.rs`) so `point.rs`/`scan.rs`'s existing `use super::row_passes_predicates;`
//! keeps working unchanged.

use crate::query::result::QueryRow;
use crate::query::select_ast::WhereExpression;
use crate::query::select_executor::{evaluate_leaf, LeafOutcome};
use crate::query::select_optimizer::SSTablePredicate;
use crate::schema::TableSchema;
use crate::types::Value;
use crate::Result;
use std::collections::HashSet;

/// `true` when `row`'s `row_kind` column is the plain `'row'` case (a live
/// or tombstoned DATA row) rather than a synthetic `partition_tombstone` /
/// `range_tombstone_start` / `range_tombstone_end` row, which carries no
/// clustering columns (or, for a partition-tombstone row, no data columns at
/// all).
fn is_plain_data_row(row: &QueryRow) -> bool {
    row.values.get("row_kind") == Some(&Value::text("row"))
}

/// Count every `Comparison` LEAF in a PUSHABLE position (through `And`/
/// `Parentheses`), or `None` if the tree contains an `Or`/`Not` ANYWHERE —
/// mirroring exactly what `select_optimizer.rs::collect_sstable_predicates`
/// walks.
///
/// Used to detect when the WHERE clause was only PARTIALLY captured by
/// `plan.sstable_predicates` (roborev finding, issue #4222 — round 2 of this
/// same class): `Or`/`Not` are not the only way a comparison silently fails
/// to lower. `select_optimizer.rs::column_comparison_to_predicate` returns
/// `None` — a silent drop even in a perfectly pushable `And`/top-level
/// position — for `!=`, `NotIn`, `Like`, `NotLike`, `NotBetween`, `IsNull`,
/// `IsNotNull`, `Regex`, and any comparison whose right-hand side is not a
/// literal (e.g. a column-to-column comparison). Each SUCCESSFULLY-lowered
/// leaf contributes EXACTLY one `SSTablePredicate`
/// (`column_comparison_to_predicate`'s every `Some` arm), so comparing this
/// count against `plan.sstable_predicates.len()` (once `Or`/`Not` — which
/// would make this `None` — is ruled out) detects ANY dropped leaf, not
/// just the OR/NOT shape: `pk = 1 AND val != 'x'` yields the NON-empty
/// predicate list `[pk = 1]` (1 predicate, 2 leaves) — checking
/// "predicates is empty" alone would miss it.
pub(super) fn count_pushable_comparison_leaves(expr: &WhereExpression) -> Option<usize> {
    match expr {
        WhereExpression::Comparison(_) => Some(1),
        WhereExpression::Or(_) | WhereExpression::Not(_) => None,
        WhereExpression::And(exprs) => {
            let mut total = 0usize;
            for e in exprs {
                total += count_pushable_comparison_leaves(e)?;
            }
            Some(total)
        }
        WhereExpression::Parentheses(inner) => count_pushable_comparison_leaves(inner),
    }
}

/// Every column a predicate must be checked against UNCONDITIONALLY,
/// regardless of `row_kind` — the partition key (handled separately, see
/// below), the source-identity quad, `row_kind` itself, and the
/// partition-/range-deletion columns.
///
/// This is NOT "every column present on every row" (roborev finding, issue
/// #4222 — round 7, correcting a misleading opening sentence in an earlier
/// revision of this doc): `partition_deletion_time`/
/// `partition_deletion_timestamp`/`bound_inclusive`/`range_deletion_time`/
/// `range_deletion_timestamp` are only ever POPULATED on their OWN specific
/// synthetic `row_kind` (never on a plain `row`) — see the second paragraph
/// below. A predicate on `range_deletion_time` still applies unconditionally
/// and therefore DOES drop every plain row via `evaluate_leaf`'s `Unknown`
/// (the column is absent there); it is the ABSENCE OF EXEMPTION that this
/// set decides, never a presence guarantee.
///
/// A predicate naming one of these must be applied to EVERY row, synthetic
/// or not (roborev finding, issue #4222 — round 5 of this same class): the
/// prior split exempted ALL non-partition-key predicates from a synthetic
/// row, which wrongly ALSO exempted `generation`/`sstable`/`format`/
/// `row_kind` and the partition-/range-deletion columns — so
/// `WHERE pk = 2 AND generation = 1` incorrectly included a gen-2
/// partition-tombstone row (`generation` IS present and real on that row;
/// it was just never CHECKED). `partition_deletion_time`/
/// `partition_deletion_timestamp`/`bound_inclusive`/`range_deletion_time`/
/// `range_deletion_timestamp` are only ever POPULATED on their own specific
/// synthetic row_kind (never on a plain `row`), but a predicate against them
/// is still meaningful there and must not be silently skipped either — this
/// set, not `is_plain_data_row`, is what decides "always apply".
///
/// `position` is DELIBERATELY EXCLUDED from this set (roborev finding,
/// issue #4222 — round 6): unlike the other source-identity columns, it is
/// NOT populated consistently across both row producers — `point.rs`
/// resolves it for real via a second index lookup, while `scan.rs` always
/// reports `Value::Null` (the full-scan producer never consults an index
/// per row). A `position` predicate is instead REJECTED OUTRIGHT before
/// either producer runs (see the check in `execute_raw_sstable_view`) —
/// letting it reach this set would apply it "unconditionally" against a
/// column whose very definition differs by internal access path, so the
/// SAME query text (`WHERE pk = 1 AND position = N` vs a position-only
/// predicate that forces a full scan) would silently return different
/// rows depending on a choice (`classify_partition_lookup`) the caller
/// does not control.
///
/// Deliberately NOT in this set (see [`row_passes_predicates`]): the
/// clustering-key / regular-column values and their `_timestamp`/`_ttl`/
/// `_local_deletion_time`/`_tombstone`/`_complex_deletion*` metadata
/// derivatives, and the ROW-level `row_timestamp`/`row_ttl`/
/// `row_local_deletion_time`/`row_tombstone`/`row_deletion_timestamp`
/// quintet — every one of those is populated ONLY on a plain `row_kind =
/// 'row'` row (`row_map.rs`'s `Live`/`Tombstone` arms), so they stay exempt
/// for a synthetic row, matching the spec's "even when the generation holds
/// no live rows" requirement for `partition_tombstone`/`range_tombstone_*`.
fn always_applicable_column_names() -> &'static HashSet<&'static str> {
    static NAMES: std::sync::OnceLock<HashSet<&'static str>> = std::sync::OnceLock::new();
    NAMES.get_or_init(|| {
        [
            "sstable",
            "generation",
            "format",
            "row_kind",
            "partition_deletion_time",
            "partition_deletion_timestamp",
            "bound_inclusive",
            "range_deletion_time",
            "range_deletion_timestamp",
        ]
        .into_iter()
        .collect()
    })
}

/// Split `predicates` into the subset that must apply to EVERY row
/// (partition-key / `token(...)` predicates, plus source-identity/
/// `row_kind`/partition-and-range-deletion predicates —
/// [`always_applicable_column_names`]) and the subset that applies ONLY to
/// a plain data row (`row_kind = 'row'`): clustering-key / regular-column
/// predicates and their metadata derivatives, plus the row-level metadata
/// quintet (roborev finding, issue #4222 — round 5, correcting round 2's
/// over-broad exemption).
///
/// Used by both row producers to apply the CORRECT predicate backstop per
/// row-kind: a synthetic `partition_tombstone`/`range_tombstone_*` row
/// carries no clustering/regular columns to test a data-only predicate
/// against, and the spec requires it to stay visible "even when the
/// generation holds no live rows" — but it DOES carry partition-key values
/// (`insert_pk_values`) and source/row_kind/deletion metadata, so those
/// predicates must still apply to it, or a full scan filtered to one
/// partition/generation would wrongly return every OTHER partition's or
/// generation's tombstone rows too.
pub(super) fn split_predicates_by_key_role<'a>(
    predicates: &'a [SSTablePredicate],
    base_schema: &TableSchema,
) -> (Vec<&'a SSTablePredicate>, Vec<&'a SSTablePredicate>) {
    let pk_names: HashSet<&str> = base_schema
        .partition_keys
        .iter()
        .map(|k| k.name.as_str())
        .collect();
    let always_names = always_applicable_column_names();
    predicates.iter().partition(|p| {
        p.is_token()
            || pk_names.contains(p.column.as_str())
            || always_names.contains(p.column.as_str())
    })
}

/// Apply the raw view's post-scan predicate backstop to one row:
/// `always_predicates` (partition-key/token PLUS source-identity/
/// `row_kind`/partition-and-range-deletion predicates —
/// [`split_predicates_by_key_role`]) apply UNCONDITIONALLY, since every row
/// carries real values for those columns, synthetic or not.
///
/// `data_predicates` (clustering-key/regular-column predicates, their
/// metadata derivatives, and the row-level metadata quintet) exempt a
/// SYNTHETIC row from a predicate ONLY WHEN BOTH the column is a
/// STRUCTURAL data column (its name is NOT a member of `metadata_names` —
/// the set `columns.rs::raw_view_columns` ACTUALLY SYNTHESIZED, never
/// re-derived from a name pattern, roborev finding, issue #4222 — round 9,
/// correcting round 8's own gap: a name-SUFFIX classifier misclassified a
/// REAL base column that happens to be named e.g. `event_timestamp` with
/// no sibling `event` column to trip the collision guard) AND it is
/// genuinely ABSENT from that row's values (round 8's fix: the prior fix
/// before THAT exempted EVERY absent `data_predicates` column, including
/// `val_tombstone`/`row_tombstone`/etc — so `WHERE val_tombstone = 'cell'`
/// wrongly included a `partition_tombstone` row that carries no
/// `val_tombstone` fact AT ALL, an asymmetry with every PLAIN row lacking
/// the column, which `evaluate_leaf`'s `Unknown` correctly rejects). A
/// metadata predicate is now evaluated normally for a synthetic row too —
/// absent ⇒ `Unknown` ⇒ reject, same as a plain row — while a STRUCTURAL
/// data-column predicate (e.g. `ck1`) stays exempt when the row genuinely
/// has no clustering at all (`partition_tombstone`) but is REJECTED when
/// the row DOES carry a real value that mismatches (a
/// `range_tombstone_start`/`_end` bound's clustering component — round 6's
/// original fix, preserved here). A PLAIN row (`row_kind = 'row'`) is
/// NEVER exempted by either rule — ordinary SQL semantics still apply
/// there.
pub(in crate::query::select_executor) fn row_passes_predicates(
    row: &QueryRow,
    always_predicates: &[&SSTablePredicate],
    data_predicates: &[&SSTablePredicate],
    metadata_names: &HashSet<String>,
) -> Result<bool> {
    for p in always_predicates {
        if evaluate_leaf(row, p) != LeafOutcome::True {
            return Ok(false);
        }
    }
    let plain = is_plain_data_row(row);
    for p in data_predicates {
        if !plain
            && !metadata_names.contains(p.column.as_str())
            && !row.values.contains_key(p.column.as_str())
        {
            // A synthetic row that genuinely has no STRUCTURAL value for
            // this column (e.g. `partition_tombstone`'s clustering
            // columns, or an open `Bottom`/`Top` range-tombstone bound's)
            // is exempt from this ONE predicate — it stays visible on that
            // column's account — but a DIFFERENT predicate this same row
            // DOES carry a value for, or a metadata-derivative predicate,
            // is still evaluated normally below.
            continue;
        }
        if evaluate_leaf(row, p) != LeafOutcome::True {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The pure decision behind [`super::SelectExecutor::resolve_raw_view_readers`]'s
/// (see `mod.rs`) cross-keyspace guard (issue #1321's class, applied here
/// for issue #4222) — extracted as a free function (roborev finding, issue
/// #4222 — round 7) so the "genuinely safety-bearing branch" of that method
/// is unit-testable without fabricating a real `SSTableReader`/
/// `StorageEngine` fixture.
///
/// `Err(())` (the guard fires) iff the request was QUALIFIED
/// (`keyspace.table`), the reader map resolved it only via a bare-name
/// FALLBACK (`fully_qualified_match == false`), AND something real was
/// actually found there (`!readers_empty`) — a schema-loaded-but-no-
/// SSTables table (`readers_empty == true`) is a benign zero-row case, not
/// a keyspace-mismatch, and must fall through to `Ok(())` instead.
pub(super) fn cross_keyspace_guard(
    was_qualified: bool,
    fully_qualified_match: bool,
    readers_empty: bool,
) -> std::result::Result<(), ()> {
    if was_qualified && !fully_qualified_match && !readers_empty {
        Err(())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::select_ast::{
        ColumnRef, ComparisonExpression, ComparisonOperator, ComparisonRightSide, SelectExpression,
    };
    use crate::query::select_optimizer::SSTableFilterOp;
    use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn};
    use crate::types::RowKey;
    use std::collections::HashMap;

    fn test_schema() -> TableSchema {
        TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: ClusteringOrder::Asc,
            }],
            columns: vec![
                Column {
                    name: "pk".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "ck".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "val".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: Default::default(),
            dropped_columns: Default::default(),
        }
    }

    fn eq_predicate(column: &str, value: Value) -> SSTablePredicate {
        SSTablePredicate::column(column, SSTableFilterOp::Equal, vec![value])
    }

    /// The exact metadata-derivative name set `raw_view_columns` would
    /// synthesize for [`test_schema`]'s `val` column plus the row-level
    /// quintet — the SAME construction-derived shape `row_passes_predicates`
    /// now takes as an explicit parameter (roborev finding, issue #4222 —
    /// round 9), never a name-suffix guess.
    fn metadata_names_fixture() -> HashSet<String> {
        [
            "val_timestamp",
            "val_ttl",
            "val_local_deletion_time",
            "val_tombstone",
            "row_timestamp",
            "row_ttl",
            "row_local_deletion_time",
            "row_tombstone",
            "row_deletion_timestamp",
        ]
        .into_iter()
        .map(String::from)
        .collect()
    }

    fn row_with(kind: &str, values: &[(&str, Value)]) -> QueryRow {
        let mut map: HashMap<String, Value> = HashMap::new();
        map.insert("row_kind".to_string(), Value::text(kind));
        for (k, v) in values {
            map.insert(k.to_string(), v.clone());
        }
        QueryRow::with_values(RowKey::new(vec![1, 2, 3]), map)
    }

    /// `split_predicates_by_key_role` puts pk/token/source-identity/
    /// row_kind/deletion predicates in the FIRST (always-apply) group, and
    /// clustering-key/regular-column predicates in the SECOND (data-only)
    /// group.
    #[test]
    fn split_predicates_by_key_role_separates_always_from_data() {
        let schema = test_schema();
        let predicates = vec![
            eq_predicate("pk", Value::Integer(1)),
            eq_predicate("generation", Value::BigInt(1)),
            eq_predicate("row_kind", Value::text("row")),
            eq_predicate("partition_deletion_time", Value::BigInt(5)),
            eq_predicate("ck", Value::Integer(2)),
            eq_predicate("val", Value::text("x")),
        ];
        let (always, data) = split_predicates_by_key_role(&predicates, &schema);
        let always_cols: Vec<&str> = always.iter().map(|p| p.column.as_str()).collect();
        let data_cols: Vec<&str> = data.iter().map(|p| p.column.as_str()).collect();
        assert_eq!(
            always_cols,
            vec!["pk", "generation", "row_kind", "partition_deletion_time"]
        );
        assert_eq!(data_cols, vec!["ck", "val"]);
    }

    /// A `partition_tombstone` row carries NO clustering/regular column at
    /// all — a data predicate on `ck` must be EXEMPT (the row stays
    /// visible), matching the spec's "even when the generation holds no
    /// live rows" requirement.
    #[test]
    fn partition_tombstone_row_is_exempt_from_a_data_predicate_it_cannot_carry() {
        let row = row_with("partition_tombstone", &[]);
        let ck_predicate = eq_predicate("ck", Value::Integer(99));
        assert!(
            row_passes_predicates(&row, &[], &[&ck_predicate], &metadata_names_fixture()).unwrap(),
            "a partition_tombstone row has no 'ck' value at all — it must be EXEMPT from a \
             'ck' predicate, not rejected as a mismatch"
        );
    }

    /// Roborev finding (issue #4222, round 5): an ALWAYS predicate (here,
    /// `generation`) is NEVER exempted for a synthetic row — a
    /// partition_tombstone row from generation 2 must be REJECTED by
    /// `generation = 1`, exactly the regression this fix corrects.
    #[test]
    fn partition_tombstone_row_is_still_checked_against_an_always_predicate() {
        let row = row_with("partition_tombstone", &[("generation", Value::BigInt(2))]);
        let gen_predicate = eq_predicate("generation", Value::BigInt(1));
        assert!(
            !row_passes_predicates(&row, &[&gen_predicate], &[], &metadata_names_fixture())
                .unwrap(),
            "a partition_tombstone row's REAL generation value (2) must be checked against \
             an always-predicate (generation = 1) and REJECTED, never exempted"
        );
    }

    /// Roborev finding (issue #4222, round 6): a `range_tombstone_start`/
    /// `_end` row DOES carry a real clustering-component value for an
    /// `Inclusive`/`Exclusive` bound — a data predicate against that column
    /// must be evaluated normally (and can REJECT the row), never exempted
    /// just because the row is non-plain. This is the direct regression
    /// test for the fix that replaced the blanket `is_plain_data_row`
    /// exemption with a per-column presence check.
    #[test]
    fn range_tombstone_row_with_a_real_clustering_value_is_checked_not_exempted() {
        let row = row_with("range_tombstone_start", &[("ck", Value::Integer(2))]);
        let ck_predicate = eq_predicate("ck", Value::Integer(99));
        assert!(
            !row_passes_predicates(&row, &[], &[&ck_predicate], &metadata_names_fixture()).unwrap(),
            "a range_tombstone_start row whose REAL ck value is 2 must be REJECTED by \
             'ck = 99', never wrongly exempted as if it carried no ck value at all"
        );

        // Sanity: the SAME row DOES pass a predicate matching its real value.
        let matching = eq_predicate("ck", Value::Integer(2));
        assert!(
            row_passes_predicates(&row, &[], &[&matching], &metadata_names_fixture()).unwrap(),
            "a range_tombstone_start row must PASS a data predicate its real clustering \
             value genuinely satisfies"
        );
    }

    /// An OPEN range-tombstone bound (`Bottom`/`Top`) carries no clustering
    /// component at all — a data predicate on that column must stay exempt,
    /// same as a partition_tombstone row.
    #[test]
    fn range_tombstone_row_with_an_open_bound_is_exempt_from_a_data_predicate() {
        let row = row_with("range_tombstone_end", &[]);
        let ck_predicate = eq_predicate("ck", Value::Integer(99));
        assert!(
            row_passes_predicates(&row, &[], &[&ck_predicate], &metadata_names_fixture()).unwrap(),
            "an open-bound range_tombstone_end row has no 'ck' value at all — exempt, not \
             rejected"
        );
    }

    /// A PLAIN data row is NEVER exempted from a data predicate by column
    /// absence — ordinary SQL semantics apply: a missing/NULL column fails
    /// an equality predicate, exactly like `evaluate_leaf`'s own
    /// `Unknown`-rejects-like-`False` handling elsewhere.
    #[test]
    fn plain_row_is_never_exempted_even_when_a_data_column_is_absent() {
        let row = row_with("row", &[]);
        let val_predicate = eq_predicate("val", Value::text("x"));
        assert!(
            !row_passes_predicates(&row, &[], &[&val_predicate], &metadata_names_fixture())
                .unwrap(),
            "a plain row missing 'val' must FAIL 'val = x' (SQL NULL semantics), never be \
             silently exempted the way a synthetic row is"
        );
    }

    /// Roborev finding (issue #4222, round 8): a METADATA-DERIVATIVE
    /// predicate (`val_tombstone` here) must NOT be exempted for a
    /// synthetic row just because the row lacks the column — a
    /// `partition_tombstone` row carries no `val_tombstone` fact AT ALL,
    /// so `WHERE val_tombstone = 'cell'` must REJECT it (absent ⇒
    /// `Unknown` ⇒ reject, the same rule a plain row already follows),
    /// never silently include it the way a STRUCTURAL predicate is
    /// exempted.
    #[test]
    fn partition_tombstone_row_is_rejected_by_a_metadata_derivative_predicate_it_lacks() {
        let row = row_with("partition_tombstone", &[]);
        let tombstone_predicate = eq_predicate("val_tombstone", Value::text("cell"));
        assert!(
            !row_passes_predicates(
                &row,
                &[],
                &[&tombstone_predicate],
                &metadata_names_fixture()
            )
            .unwrap(),
            "a partition_tombstone row has no 'val_tombstone' fact at all — it must be \
             REJECTED by 'val_tombstone = cell', never silently included as if it matched"
        );
    }

    /// Same rejection rule applies to the ROW-level metadata quintet
    /// (`row_tombstone` here), not just per-cell derivatives.
    #[test]
    fn partition_tombstone_row_is_rejected_by_a_row_level_metadata_predicate_it_lacks() {
        let row = row_with("partition_tombstone", &[]);
        let row_tombstone_predicate = eq_predicate("row_tombstone", Value::text("row"));
        assert!(
            !row_passes_predicates(
                &row,
                &[],
                &[&row_tombstone_predicate],
                &metadata_names_fixture()
            )
            .unwrap(),
            "a partition_tombstone row has no 'row_tombstone' fact at all — it must be \
             REJECTED by 'row_tombstone = row', never silently included"
        );
    }

    /// Sanity: a STRUCTURAL data-column predicate (`ck`, not a metadata
    /// derivative) is STILL exempted for a `partition_tombstone` row that
    /// genuinely has no clustering at all — round 6's original fix must
    /// stay intact for this class.
    #[test]
    fn partition_tombstone_row_is_still_exempt_from_a_structural_data_predicate() {
        let row = row_with("partition_tombstone", &[]);
        let ck_predicate = eq_predicate("ck", Value::Integer(99));
        assert!(
            row_passes_predicates(&row, &[], &[&ck_predicate], &metadata_names_fixture()).unwrap(),
            "a structural clustering predicate must STILL be exempt for a row with no \
             clustering at all — this is not the class round 8 fixed"
        );
    }

    fn comparison(column: &str, value: Value) -> WhereExpression {
        WhereExpression::Comparison(ComparisonExpression {
            left: SelectExpression::Column(ColumnRef::new(column)),
            operator: ComparisonOperator::Equal,
            right: ComparisonRightSide::Value(SelectExpression::Literal(value)),
        })
    }

    #[test]
    fn count_pushable_comparison_leaves_sums_through_and_and_parentheses() {
        let expr = WhereExpression::Parentheses(Box::new(WhereExpression::And(vec![
            comparison("pk", Value::Integer(1)),
            comparison("ck", Value::Integer(2)),
        ])));
        assert_eq!(count_pushable_comparison_leaves(&expr), Some(2));
    }

    #[test]
    fn count_pushable_comparison_leaves_is_none_for_or() {
        let expr = WhereExpression::Or(vec![
            comparison("pk", Value::Integer(1)),
            comparison("pk", Value::Integer(2)),
        ]);
        assert_eq!(count_pushable_comparison_leaves(&expr), None);
    }

    #[test]
    fn count_pushable_comparison_leaves_is_none_for_not() {
        let expr = WhereExpression::Not(Box::new(comparison("pk", Value::Integer(1))));
        assert_eq!(count_pushable_comparison_leaves(&expr), None);
    }

    #[test]
    fn count_pushable_comparison_leaves_is_none_when_or_is_nested_inside_and() {
        // `pk = 1 AND (ck = 2 OR ck = 3)` — the OR is nested, not top-level,
        // but must still propagate `None` through the enclosing `And`.
        let expr = WhereExpression::And(vec![
            comparison("pk", Value::Integer(1)),
            WhereExpression::Or(vec![
                comparison("ck", Value::Integer(2)),
                comparison("ck", Value::Integer(3)),
            ]),
        ]);
        assert_eq!(count_pushable_comparison_leaves(&expr), None);
    }

    /// Roborev finding (issue #4222, round 7): the ONE genuinely
    /// safety-bearing branch of `resolve_raw_view_readers` (the issue
    /// #1321 cross-keyspace guard) had no test at all. These four cases
    /// cover every combination of its three inputs that changes the
    /// outcome.
    #[test]
    fn cross_keyspace_guard_fires_only_for_a_qualified_fallback_with_real_data() {
        // Qualified + fallback-only + real data found -> the safety
        // violation this guard exists for: REFUSE.
        assert_eq!(cross_keyspace_guard(true, false, false), Err(()));
    }

    #[test]
    fn cross_keyspace_guard_does_not_fire_on_the_benign_empty_carve_out() {
        // Qualified + fallback-only but NOTHING was found: a
        // schema-loaded-but-no-SSTables table is a benign zero-row case,
        // never a keyspace mismatch — must NOT refuse.
        assert_eq!(cross_keyspace_guard(true, false, true), Ok(()));
    }

    #[test]
    fn cross_keyspace_guard_does_not_fire_on_an_exact_qualified_match() {
        // Qualified and resolved via the EXACT `keyspace.table` key — no
        // fallback was involved at all, so there is nothing to guard
        // against.
        assert_eq!(cross_keyspace_guard(true, true, false), Ok(()));
    }

    #[test]
    fn cross_keyspace_guard_does_not_fire_for_an_unqualified_request() {
        // No keyspace was named at all, so there is no cross-keyspace
        // mismatch to detect — matches `fully_qualified_match`'s own
        // contract ("unqualified" is trivially "fully qualified").
        assert_eq!(cross_keyspace_guard(false, false, false), Ok(()));
    }
}
