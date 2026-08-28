//! Pushdown classification: `Exact` only for a predicate the scan applies.

use super::support::*;

// ---------------------------------------------------------------------------
// Pushdown classification — the "never claim Exact for what we do not apply" rule
// ---------------------------------------------------------------------------

#[test]
fn translatable_comparisons_classify_as_exact() {
    let schema = testutil::clustering_schema();
    let filters = vec![
        col("val").eq(lit(30_i32)),
        col("val").lt(lit(30_i32)),
        col("val").gt_eq(lit(30_i32)),
        // Literal-first: must be mirrored to `val > 30`, not `val < 30`.
        lit(30_i32).lt(col("val")),
        col("val").not_eq(lit(30_i32)),
        col("val").is_null(),
        col("val").is_not_null(),
        col("ck").eq(lit("a")),
        col("val").in_list(vec![lit(10_i32), lit(20_i32)], false),
        col("val").between(lit(10_i32), lit(30_i32)),
    ];
    let refs: Vec<&Expr> = filters.iter().collect();
    let verdicts = pushdown::classify(&refs, &schema);
    for (filter, verdict) in filters.iter().zip(verdicts.iter()) {
        assert_eq!(
            *verdict,
            TableProviderFilterPushDown::Exact,
            "expected Exact for {filter:?}"
        );
    }
}

#[test]
fn literal_first_comparison_mirrors_the_operator() {
    let schema = testutil::clustering_schema();
    // `30 < val` must lower to `val > 30`. Asserted through the row sets the two
    // spellings select, so a mirroring bug cannot hide behind a matching verdict.
    let mirrored = pushdown::translate_all(&[lit(30_i32).lt(col("val"))], &schema)
        .expect("mirrored form is translatable");
    let direct = pushdown::translate_all(&[col("val").gt(lit(30_i32))], &schema)
        .expect("direct form is translatable");
    assert_eq!(
        format!("{mirrored:?}"),
        format!("{direct:?}"),
        "`30 < val` must translate identically to `val > 30`"
    );
}

#[test]
fn untranslatable_filters_classify_as_unsupported() {
    let schema = testutil::clustering_schema();
    let filters = [
        // Unknown column: production lowering rejects it, so the claim is refused.
        col("nope").eq(lit(1_i32)),
        // Column-to-column comparison: the scan path evaluates column-vs-literal.
        col("val").eq(col("pk")),
        // A cast can change comparison semantics, and no oracle proves CQLite's
        // coercion matches DataFusion's.
        Expr::Cast(datafusion::logical_expr::Cast::new(
            Box::new(col("val")),
            arrow::datatypes::DataType::Int64,
        ))
        .eq(lit(1_i64)),
        // Operand type that does not coerce to the column's CQL type.
        col("val").eq(lit("not-an-int")),
    ];
    let refs: Vec<&Expr> = filters.iter().collect();
    let verdicts = pushdown::classify(&refs, &schema);
    for (filter, verdict) in filters.iter().zip(verdicts.iter()) {
        assert_eq!(
            *verdict,
            TableProviderFilterPushDown::Unsupported,
            "expected Unsupported for {filter:?}"
        );
    }
}

#[test]
fn pushdown_disabled_reports_every_filter_unsupported() {
    let (_temp, table_dir, schema) = two_generation_fixture();
    let provider =
        CqliteTableProvider::open(schema, table_dir, TEST_BATCH_SIZE, false).expect("provider");
    let filter = col("val").eq(lit(30_i32));
    let verdicts =
        datafusion::catalog::TableProvider::supports_filters_pushdown(&provider, &[&filter])
            .expect("verdicts");
    assert_eq!(verdicts, vec![TableProviderFilterPushDown::Unsupported]);
}
