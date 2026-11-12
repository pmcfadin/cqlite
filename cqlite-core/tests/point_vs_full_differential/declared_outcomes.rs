//! Issue #4256: DECLARED per-case outcomes and the REVERSE-direction axis for the
//! point-vs-full differential.
//!
//! * [`run_non_served`] executes the cases whose `TableCase::outcome` is NOT
//!   `Served`. The expected outcome (`SupportedButUnserved` / `SanctionedFallback`,
//!   with the exact closed `FallbackReason`) is written in the corpus by the author
//!   from the access-path contract and is never inferred from the result.
//! * [`outcome_coverage_violations`] makes dropping an outcome class red.
//! * [`run_reverse`] runs every probed partition and every clustering slice with
//!   `ORDER BY <all clustering columns> DESC` through both paths.
//!
//! Reverse is only meaningful if the DESC result is genuinely the reverse of the ASC
//! one, so each reverse query is anchored three ways: (a) point == full (rows, values
//! AND order, with the engagement + cold-start guards of #4255), (b) the point DESC
//! rows equal the point ASC rows reversed, and (c) at least one partition per case
//! holds >= 2 rows so a single-row partition cannot make direction vacuous.

use cqlite_core::config::ReadPathMode;
use cqlite_core::query::access_path::{AccessPath, FallbackReason};
use cqlite_core::Error;

use super::point_engagement::point_leg_cold;
use super::{assert_point_full_equal, normalize, open_db, DeclaredOutcome, TableCase};
use std::path::Path;

fn expand(template: &str, case: &TableCase) -> String {
    template.replace("{t}", &format!("{}.{}", case.keyspace, case.table))
}

/// Outcome classes every corpus must keep at least one case for.
pub(super) fn outcome_coverage_violations(cases: &[TableCase]) -> Vec<&'static str> {
    let mut missing = Vec::new();
    for (class, present) in [
        (
            "served",
            cases
                .iter()
                .any(|c| matches!(c.outcome, DeclaredOutcome::Served)),
        ),
        (
            "supported_but_unserved",
            cases
                .iter()
                .any(|c| matches!(c.outcome, DeclaredOutcome::SupportedButUnserved { .. })),
        ),
        (
            "sanctioned_fallback",
            cases
                .iter()
                .any(|c| matches!(c.outcome, DeclaredOutcome::SanctionedFallback { .. })),
        ),
    ] {
        if !present {
            missing.push(class);
        }
    }
    missing
}

/// Run a `SupportedButUnserved` / `SanctionedFallback` case. `Served` is handled by
/// the main flow and is rejected here so a mis-route cannot pass silently.
pub(super) async fn run_non_served(
    case: &TableCase,
    root: &Path,
    schema: &Path,
) -> Result<(), String> {
    let full_db = open_db(root, schema, case.keyspace, ReadPathMode::Full).await?;
    match case.outcome {
        DeclaredOutcome::Served => Err(format!(
            "{}.{}: run_non_served called for a Served case",
            case.keyspace, case.table
        )),
        DeclaredOutcome::SanctionedFallback { query, reason } => {
            let q = expand(query, case);
            let auto_db = open_db(root, schema, case.keyspace, ReadPathMode::Auto).await?;
            let auto = auto_db
                .execute(&q)
                .await
                .map_err(|e| format!("auto `{q}` failed: {e}"))?;
            let expected = AccessPath::FallbackFullScan { reason };
            if auto.metadata.access_path.as_ref() != Some(&expected) {
                return Err(format!(
                    "OUTCOME FAIL {}.{}: declared sanctioned fallback {expected:?} for `{q}` but \
                     the engine recorded {:?}",
                    case.keyspace, case.table, auto.metadata.access_path
                ));
            }
            let full = full_db
                .execute(&q)
                .await
                .map_err(|e| format!("full `{q}` failed: {e}"))?;
            let (a, f) = (normalize(&auto.rows), normalize(&full.rows));
            if a.is_empty() || a != f {
                return Err(format!(
                    "OUTCOME FAIL {}.{}: fallback `{q}` must return the SAME non-empty rows as \
                     forced full (auto {} rows, full {} rows)",
                    case.keyspace,
                    case.table,
                    a.len(),
                    f.len()
                ));
            }
            Ok(())
        }
        DeclaredOutcome::SupportedButUnserved { query, reason } => {
            let q = expand(query, case);
            let point_db = open_db(root, schema, case.keyspace, ReadPathMode::Point).await?;
            match point_db.execute(&q).await {
                Err(Error::ForcedReadPathUnavailable {
                    forced: "point",
                    reason: got,
                }) if got == reason_label(reason) => {}
                other => {
                    return Err(format!(
                        "OUTCOME FAIL {}.{}: declared supported-but-unserved \
                         (ForcedReadPathUnavailable point/{}) for `{q}`, got {:?}",
                        case.keyspace,
                        case.table,
                        reason_label(reason),
                        other.map(|r| r.metadata.access_path)
                    ))
                }
            }
            let full = full_db
                .execute(&q)
                .await
                .map_err(|e| format!("full `{q}` failed: {e}"))?;
            if full.rows.is_empty() {
                return Err(format!(
                    "OUTCOME FAIL {}.{}: `{q}` returned no rows under full — the unserved \
                     declaration would be vacuous",
                    case.keyspace, case.table
                ));
            }
            Ok(())
        }
    }
}

fn reason_label(reason: FallbackReason) -> &'static str {
    reason.label()
}

/// The reverse-direction axis for one case. Returns the number of
/// partition/slice comparisons run.
pub(super) async fn run_reverse(
    case: &TableCase,
    order_by: &str,
    point_db: &cqlite_core::Database,
    full_db: &cqlite_core::Database,
    keys: &[i64],
) -> Result<usize, String> {
    let mut comparisons = 0usize;
    let mut multi_row_partitions = 0usize;
    for k in keys {
        let base = format!(
            "SELECT * FROM {}.{} WHERE {} = {k}",
            case.keyspace, case.table, case.pk_column
        );
        let rows = reverse_one(point_db, full_db, &base, order_by).await?;
        comparisons += 1;
        if rows >= 2 {
            multi_row_partitions += 1;
        }
        for (predicate, expected_rows) in case.clustering_slice_predicates {
            let q = format!("{base} AND {predicate}");
            let got = reverse_one(point_db, full_db, &q, order_by).await?;
            if got != *expected_rows {
                return Err(format!(
                    "case {}.{}: reverse `{q} ORDER BY {order_by}` returned {got} rows, slice \
                     must yield exactly {expected_rows}",
                    case.keyspace, case.table
                ));
            }
            comparisons += 1;
        }
    }
    if multi_row_partitions == 0 {
        return Err(format!(
            "case {}.{}: no probed partition holds >= 2 rows, so the reverse direction is \
             vacuous here — declare a fixture with a multi-row partition",
            case.keyspace, case.table
        ));
    }
    Ok(comparisons)
}

/// One query in both directions. Returns the (agreed) row count.
async fn reverse_one(
    point_db: &cqlite_core::Database,
    full_db: &cqlite_core::Database,
    base: &str,
    order_by: &str,
) -> Result<usize, String> {
    let desc_q = format!("{base} ORDER BY {order_by}");
    // (a) point == full, engaged + cold, order included.
    let rows = assert_point_full_equal(point_db, full_db, &desc_q).await?;
    // (b) the DESC point rows are exactly the ASC point rows reversed.
    let (asc, _) = point_leg_cold(point_db, base).await?;
    let (desc, _) = point_leg_cold(point_db, &desc_q).await?;
    let mut asc_rows = normalize(&asc.rows);
    asc_rows.reverse();
    if asc_rows != normalize(&desc.rows) {
        return Err(format!(
            "REVERSE FAIL: `{desc_q}` is not the reverse of `{base}` on the point path \
             (asc reversed: {asc_rows:?}; desc: {:?})",
            normalize(&desc.rows)
        ));
    }
    Ok(rows)
}

#[test]
fn outcome_coverage_flags_a_corpus_missing_a_class() {
    let served_only: Vec<&TableCase> = super::CORPUS
        .iter()
        .filter(|c| matches!(c.outcome, DeclaredOutcome::Served))
        .collect();
    assert!(!served_only.is_empty());
    assert!(
        outcome_coverage_violations(super::CORPUS).is_empty(),
        "the real corpus must cover every outcome class"
    );
    let only_served: Vec<TableCase> = Vec::new();
    // An empty corpus is missing ALL three classes: the check can fail.
    assert_eq!(outcome_coverage_violations(&only_served).len(), 3);
}
