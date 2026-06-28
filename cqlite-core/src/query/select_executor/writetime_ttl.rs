//! WRITETIME() / TTL() projection helpers and the injectable clock.
//!
//! These functions compute the Cassandra-convention output column name and the
//! `Value` produced by a `WRITETIME(col)` / `TTL(col)` call from a row's per-cell
//! metadata. The [`NowSeconds`] clock is injectable so TTL tests are
//! deterministic. [`like_pattern_to_regex`] lives here too as a small projection
//! / predicate text helper.

use super::super::select_ast::{
    SelectClause, SelectExpression, SelectStatement, WriteTimeTtlCall, WriteTimeTtlFunction,
};
use crate::{query::result::QueryRow, types::Value};

/// Clock abstraction for TTL "now" injection.
///
/// This trait exists solely so that tests can inject a deterministic timestamp
/// instead of reading `SystemTime`. The only production implementation is
/// `SystemClock`; tests use `FixedClock`.
pub trait NowSeconds: Send + Sync {
    /// Return the current time as seconds since Unix epoch.
    fn now_seconds(&self) -> i64;
}

/// Production clock: reads from the system wall clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl NowSeconds for SystemClock {
    fn now_seconds(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }
}

/// Test clock: always returns a fixed value.
#[derive(Debug, Clone, Copy)]
pub struct FixedClock(pub i64);

impl NowSeconds for FixedClock {
    fn now_seconds(&self) -> i64 {
        self.0
    }
}

/// Return `true` when the select clause contains at least one `WRITETIME` or
/// `TTL` call — used during planning to set `ProjectionFlags::include_cell_metadata`.
pub(super) fn select_has_writetime_ttl(statement: &SelectStatement) -> bool {
    let exprs = match &statement.select_clause {
        SelectClause::All => return false,
        SelectClause::Columns(e) | SelectClause::Distinct(e) => e,
    };
    exprs
        .iter()
        .any(|e| matches!(e, SelectExpression::WriteTimeTtl(_)))
}

/// Compute the Cassandra-convention output column name for a `WriteTimeTtlCall`.
///
/// - No alias: `writetime(col)` or `ttl(col)` (lowercase, matching Cassandra).
/// - Explicit alias: the alias string, exactly as parsed.
pub(super) fn writetime_ttl_column_name(call: &WriteTimeTtlCall) -> String {
    if let Some(alias) = &call.alias {
        return alias.clone();
    }
    match call.function {
        WriteTimeTtlFunction::WriteTime => format!("writetime({})", call.column),
        WriteTimeTtlFunction::Ttl => format!("ttl({})", call.column),
    }
}

/// Evaluate a `WRITETIME(col)` or `TTL(col)` call against a single `QueryRow`.
///
/// `now_secs` is the current epoch-second used only for TTL subtraction. It
/// **must** be injected by the caller rather than read here so that unit tests
/// can produce deterministic results.
///
/// Return values:
/// - `WRITETIME(col)` → `Value::BigInt(micros)` when metadata exists; `Value::Null` otherwise.
/// - `TTL(col)` → `Value::Integer(remaining_secs)` when the cell has an unexpired TTL;
///   `Value::Null` when no expiration exists **or** the cell has already expired.
pub(super) fn evaluate_writetime_ttl(
    call: &WriteTimeTtlCall,
    row: &QueryRow,
    now_secs: i64,
) -> Value {
    let meta = match row.get_cell_metadata(&call.column) {
        Some(m) => m,
        None => return Value::Null,
    };

    match call.function {
        WriteTimeTtlFunction::WriteTime => Value::BigInt(meta.write_timestamp_micros),
        WriteTimeTtlFunction::Ttl => match &meta.expiration {
            None => Value::Null,
            Some(exp) => {
                let remaining = exp.expires_at_seconds - now_secs;
                if remaining <= 0 {
                    // Cell has already expired — Cassandra returns NULL.
                    Value::Null
                } else {
                    // Safe cast: remaining is in (0, i32::MAX] range for any
                    // realistic TTL (Cassandra caps TTL at 630_720_000 seconds).
                    Value::Integer(remaining.min(i32::MAX as i64) as i32)
                }
            }
        },
    }
}

/// Translate a CQL LIKE pattern (`%`, `_`) into an anchored regex.
pub(super) fn like_pattern_to_regex(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len() + 4);
    out.push('^');
    for ch in pattern.chars() {
        match ch {
            '%' => out.push_str(".*"),
            '_' => out.push('.'),
            _ => out.push(ch),
        }
    }
    out.push('$');
    out
}

#[cfg(test)]
mod tests {
    use super::super::super::select_ast::ColumnRef;
    use super::*;
    use crate::query::result::{CellExpiration, CellWriteMetadata};
    use crate::types::RowKey;

    /// Helper: build a QueryRow with a given column value and optional cell metadata.
    fn row_with_cell_meta(column: &str, value: Value, meta: Option<CellWriteMetadata>) -> QueryRow {
        let mut row = QueryRow::new(RowKey::new(vec![1]));
        row.set(column.to_string(), value);
        if let Some(m) = meta {
            row.insert_cell_metadata(column.to_string(), m);
        }
        row
    }

    /// WRITETIME(col) returns Value::BigInt(micros) when metadata is present.
    #[test]
    fn test_writetime_returns_bigint_when_metadata_present() {
        let write_ts = 1_700_000_000_000_000_i64;
        let row = row_with_cell_meta(
            "name",
            Value::Text("Alice".to_string()),
            Some(CellWriteMetadata {
                write_timestamp_micros: write_ts,
                expiration: None,
            }),
        );

        let call = WriteTimeTtlCall {
            function: WriteTimeTtlFunction::WriteTime,
            column: "name".to_string(),
            alias: None,
        };

        let result = evaluate_writetime_ttl(&call, &row, 0 /* now unused for WRITETIME */);
        assert_eq!(
            result,
            Value::BigInt(write_ts),
            "WRITETIME(col) must return Value::BigInt(micros)"
        );
    }

    /// WRITETIME(col) returns Value::Null when cell metadata is absent.
    #[test]
    fn test_writetime_returns_null_when_no_metadata() {
        // Row has a value for the column but no cell metadata (e.g. partition-key column).
        let row = row_with_cell_meta("id", Value::Integer(1), None);

        let call = WriteTimeTtlCall {
            function: WriteTimeTtlFunction::WriteTime,
            column: "id".to_string(),
            alias: None,
        };

        let result = evaluate_writetime_ttl(&call, &row, 0);
        assert_eq!(
            result,
            Value::Null,
            "WRITETIME(col) must return NULL when no cell metadata is threaded"
        );
    }

    /// TTL(col) returns Value::Integer(remaining) for a live TTL cell.
    #[test]
    fn test_ttl_returns_remaining_seconds_for_live_cell() {
        // Cell was written at epoch 0, TTL = 3600s, expires at epoch 3600.
        // Now = epoch 1000. Remaining = 2600s.
        let now_secs: i64 = 1000;
        let expires_at: i64 = 3600;
        let row = row_with_cell_meta(
            "score",
            Value::Integer(42),
            Some(CellWriteMetadata {
                write_timestamp_micros: 0,
                expiration: Some(CellExpiration {
                    ttl_seconds: 3600,
                    expires_at_seconds: expires_at,
                }),
            }),
        );

        let call = WriteTimeTtlCall {
            function: WriteTimeTtlFunction::Ttl,
            column: "score".to_string(),
            alias: None,
        };

        let result = evaluate_writetime_ttl(&call, &row, now_secs);
        assert_eq!(
            result,
            Value::Integer(2600),
            "TTL(col) must return remaining seconds for a live cell"
        );
    }

    /// TTL(col) returns Value::Null when the cell has no expiration.
    #[test]
    fn test_ttl_returns_null_when_no_expiration() {
        let row = row_with_cell_meta(
            "name",
            Value::Text("Bob".to_string()),
            Some(CellWriteMetadata {
                write_timestamp_micros: 100,
                expiration: None, // no TTL written
            }),
        );

        let call = WriteTimeTtlCall {
            function: WriteTimeTtlFunction::Ttl,
            column: "name".to_string(),
            alias: None,
        };

        let result = evaluate_writetime_ttl(&call, &row, 9999);
        assert_eq!(
            result,
            Value::Null,
            "TTL(col) must return NULL when the cell has no TTL"
        );
    }

    /// TTL(col) returns Value::Null when the cell is expired.
    #[test]
    fn test_ttl_returns_null_for_expired_cell() {
        // Cell expires at epoch 100; now is epoch 200 → expired.
        let row = row_with_cell_meta(
            "token",
            Value::Text("abc".to_string()),
            Some(CellWriteMetadata {
                write_timestamp_micros: 0,
                expiration: Some(CellExpiration {
                    ttl_seconds: 100,
                    expires_at_seconds: 100,
                }),
            }),
        );

        let call = WriteTimeTtlCall {
            function: WriteTimeTtlFunction::Ttl,
            column: "token".to_string(),
            alias: None,
        };

        // now_secs = 200 > expires_at = 100 → expired
        let result = evaluate_writetime_ttl(&call, &row, 200);
        assert_eq!(
            result,
            Value::Null,
            "TTL(col) must return NULL when the cell is expired"
        );
    }

    /// TTL(col) returns Value::Null when cell metadata is entirely absent.
    #[test]
    fn test_ttl_returns_null_when_no_metadata() {
        let row = row_with_cell_meta("x", Value::Integer(7), None);

        let call = WriteTimeTtlCall {
            function: WriteTimeTtlFunction::Ttl,
            column: "x".to_string(),
            alias: None,
        };

        let result = evaluate_writetime_ttl(&call, &row, 1000);
        assert_eq!(result, Value::Null);
    }

    /// Cassandra convention: `writetime(col)` (no alias).
    #[test]
    fn test_writetime_ttl_column_name_no_alias() {
        let wt_call = WriteTimeTtlCall {
            function: WriteTimeTtlFunction::WriteTime,
            column: "name".to_string(),
            alias: None,
        };
        assert_eq!(writetime_ttl_column_name(&wt_call), "writetime(name)");

        let ttl_call = WriteTimeTtlCall {
            function: WriteTimeTtlFunction::Ttl,
            column: "name".to_string(),
            alias: None,
        };
        assert_eq!(writetime_ttl_column_name(&ttl_call), "ttl(name)");
    }

    /// Explicit alias overrides the Cassandra-convention name.
    #[test]
    fn test_writetime_ttl_column_name_with_alias() {
        let call = WriteTimeTtlCall {
            function: WriteTimeTtlFunction::WriteTime,
            column: "score".to_string(),
            alias: Some("wt".to_string()),
        };
        assert_eq!(writetime_ttl_column_name(&call), "wt");
    }

    /// `select_has_writetime_ttl` returns true only when a WriteTimeTtl expression is present.
    #[test]
    fn test_select_has_writetime_ttl_detection() {
        // No WriteTimeTtl → false
        let stmt_no_wt = SelectStatement {
            select_clause: SelectClause::Columns(vec![
                SelectExpression::Column(ColumnRef::new("id")),
                SelectExpression::Column(ColumnRef::new("name")),
            ]),
            from_clause: None,
            where_clause: None,
            group_by: None,
            having_clause: None,
            order_by: None,
            limit: None,
            per_partition_limit: None,
            offset: None,
            allow_filtering: false,
        };
        assert!(!select_has_writetime_ttl(&stmt_no_wt));

        // With WriteTimeTtl → true
        let stmt_wt = SelectStatement {
            select_clause: SelectClause::Columns(vec![
                SelectExpression::Column(ColumnRef::new("id")),
                SelectExpression::WriteTimeTtl(WriteTimeTtlCall {
                    function: WriteTimeTtlFunction::WriteTime,
                    column: "name".to_string(),
                    alias: None,
                }),
            ]),
            from_clause: None,
            where_clause: None,
            group_by: None,
            having_clause: None,
            order_by: None,
            limit: None,
            per_partition_limit: None,
            offset: None,
            allow_filtering: false,
        };
        assert!(select_has_writetime_ttl(&stmt_wt));

        // SELECT * → false (no expression list to inspect)
        let stmt_star = SelectStatement {
            select_clause: SelectClause::All,
            from_clause: None,
            where_clause: None,
            group_by: None,
            having_clause: None,
            order_by: None,
            limit: None,
            per_partition_limit: None,
            offset: None,
            allow_filtering: false,
        };
        assert!(!select_has_writetime_ttl(&stmt_star));
    }
}
