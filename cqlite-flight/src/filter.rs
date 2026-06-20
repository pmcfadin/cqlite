//! Server-side scan filtering: token range, predicate pushdown, projection.
//!
//! Translates a [`FlightTicket`]'s filter fields into a [`ScanSpec`] the producer
//! applies while merging:
//! - **token range** drops whole partitions outside the split's `(start, end]`
//!   range (cross-replica dedup — see `PLAN.md` §2),
//! - **predicates** are evaluated per row via cqlite-core's `evaluate_predicates`
//!   (identical semantics to SELECT), and
//! - **projection** restricts the emitted columns.
//!
//! Predicate operands arrive as JSON and are converted to typed `Value`s using
//! the column's authoritative `CqlType` (no heuristics, issue #28).

use cqlite_core::query::{SSTableFilterOp, SSTablePredicate};
use cqlite_core::schema::{CqlType, TableSchema};
use cqlite_core::types::Value;

use crate::ticket::{token_in_half_open_range, FlightTicket, Predicate, PredicateOp};

/// Errors building a [`ScanSpec`] from a ticket.
#[derive(Debug, thiserror::Error)]
pub enum FilterError {
    /// A predicate referenced a column not present in the schema.
    #[error("predicate column '{0}' is not in the table schema")]
    UnknownColumn(String),
    /// A column's CQL type string could not be parsed.
    #[error("invalid CQL type '{type_str}' for column '{column}': {source}")]
    InvalidType {
        /// Column name.
        column: String,
        /// The offending type string.
        type_str: String,
        /// Underlying parse error.
        source: cqlite_core::Error,
    },
    /// A predicate JSON operand could not be converted to the column's type.
    #[error("predicate on '{column}': {message}")]
    BadOperand {
        /// Column name.
        column: String,
        /// What went wrong.
        message: String,
    },
}

/// Token-range membership test for one split.
#[derive(Debug, Clone, Copy)]
pub struct TokenFilter {
    start: i64,
    end: i64,
    wraparound: bool,
}

impl TokenFilter {
    /// True if `token` falls in this split's `(start, end]` range.
    pub fn contains(&self, token: i64) -> bool {
        token_in_half_open_range(token, self.start, self.end, self.wraparound)
    }

    /// True if an SSTable spanning `[min_token, max_token]` (inclusive of both
    /// endpoints, since they are the smallest and largest partition tokens
    /// actually present) could contain any token in this split's `(start, end]`
    /// range.
    ///
    /// Half-open `(start, end]` semantics: a partition at exactly `start` is
    /// excluded, one at exactly `end` is included. For a non-wrapping range the
    /// spans overlap iff the SSTable reaches past `start` (`max_token > start`)
    /// and starts at or before `end` (`min_token <= end`). A wraparound range is
    /// `(start, MAX] ∪ [MIN, end]`, so overlap holds if the SSTable reaches past
    /// `start` OR starts at or before `end`.
    pub fn overlaps(&self, min_token: i64, max_token: i64) -> bool {
        if self.wraparound {
            max_token > self.start || min_token <= self.end
        } else {
            max_token > self.start && min_token <= self.end
        }
    }
}

/// A fully-resolved scan: what to keep and which columns to emit.
#[derive(Debug, Clone, Default)]
pub struct ScanSpec {
    /// Partition-level token filter; `None` keeps every partition.
    pub token: Option<TokenFilter>,
    /// Row-level predicates (AND-combined); empty keeps every row.
    pub predicates: Vec<SSTablePredicate>,
    /// Column projection; `None` emits all columns.
    pub projection: Option<Vec<String>>,
}

impl ScanSpec {
    /// Build a scan spec from a ticket against its table schema.
    pub fn from_ticket(ticket: &FlightTicket, schema: &TableSchema) -> Result<Self, FilterError> {
        let token = match (ticket.token_start, ticket.token_end) {
            (None, None) => None,
            (start, end) => Some(TokenFilter {
                start: start.unwrap_or(i64::MIN),
                end: end.unwrap_or(i64::MAX),
                wraparound: ticket.wraparound,
            }),
        };

        let predicates = ticket
            .predicates
            .iter()
            .map(|p| to_sstable_predicate(p, schema))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            token,
            predicates,
            projection: ticket.columns.clone(),
        })
    }
}

/// Resolve a column's CQL type from the schema (searches all column lists).
fn column_cql_type(schema: &TableSchema, column: &str) -> Result<CqlType, FilterError> {
    let type_str = schema
        .partition_keys
        .iter()
        .find(|c| c.name == column)
        .map(|c| &c.data_type)
        .or_else(|| {
            schema
                .clustering_keys
                .iter()
                .find(|c| c.name == column)
                .map(|c| &c.data_type)
        })
        .or_else(|| {
            schema
                .columns
                .iter()
                .find(|c| c.name == column)
                .map(|c| &c.data_type)
        })
        .ok_or_else(|| FilterError::UnknownColumn(column.to_string()))?;

    CqlType::parse(type_str).map_err(|source| FilterError::InvalidType {
        column: column.to_string(),
        type_str: type_str.clone(),
        source,
    })
}

/// Convert a ticket predicate to a core `SSTablePredicate`.
fn to_sstable_predicate(
    p: &Predicate,
    schema: &TableSchema,
) -> Result<SSTablePredicate, FilterError> {
    let cql = column_cql_type(schema, &p.column)?;
    let operation = match p.op {
        PredicateOp::Equal => SSTableFilterOp::Equal,
        PredicateOp::In => SSTableFilterOp::In,
        PredicateOp::Gt => SSTableFilterOp::Gt,
        PredicateOp::Gte => SSTableFilterOp::Gte,
        PredicateOp::Lt => SSTableFilterOp::Lt,
        PredicateOp::Lte => SSTableFilterOp::Lte,
        PredicateOp::Prefix => SSTableFilterOp::Prefix,
    };

    // `In` carries a JSON array of operands; everything else a single operand.
    let values = match (&p.op, &p.value) {
        (PredicateOp::In, serde_json::Value::Array(items)) => items
            .iter()
            .map(|v| json_to_value(v, &cql, &p.column))
            .collect::<Result<Vec<_>, _>>()?,
        (PredicateOp::In, _) => {
            return Err(FilterError::BadOperand {
                column: p.column.clone(),
                message: "IN requires a JSON array operand".into(),
            })
        }
        (_, v) => vec![json_to_value(v, &cql, &p.column)?],
    };

    if matches!(p.op, PredicateOp::In) && values.is_empty() {
        return Err(FilterError::BadOperand {
            column: p.column.clone(),
            message: "IN requires at least one value".into(),
        });
    }

    Ok(SSTablePredicate {
        column: p.column.clone(),
        operation,
        values,
    })
}

/// Convert a JSON operand to a typed `Value` using the column's CQL type.
///
/// Supports the scalar types that make sense to push down from a SQL engine.
/// Numeric coercion in `evaluate_predicates` smooths over int/bigint/float
/// differences, so integers map to the natural width.
fn json_to_value(
    json: &serde_json::Value,
    cql: &CqlType,
    column: &str,
) -> Result<Value, FilterError> {
    let bad = |message: String| FilterError::BadOperand {
        column: column.to_string(),
        message,
    };

    if json.is_null() {
        return Err(bad("null is not a valid predicate operand".into()));
    }

    let unwrap_frozen = |t: &CqlType| -> CqlType {
        match t {
            CqlType::Frozen(inner) => (**inner).clone(),
            other => other.clone(),
        }
    };

    match unwrap_frozen(cql) {
        CqlType::TinyInt | CqlType::SmallInt | CqlType::Int => {
            let n = json
                .as_i64()
                .ok_or_else(|| bad(format!("expected integer, got {json}")))?;
            // Avoid silent wrap; numeric coercion in evaluate_predicates handles
            // comparison against the column's actual integer width.
            i32::try_from(n)
                .map(Value::Integer)
                .map_err(|_| bad(format!("integer {n} out of range for an int column")))
        }
        CqlType::BigInt | CqlType::Counter | CqlType::Timestamp => json
            .as_i64()
            .map(Value::BigInt)
            .ok_or_else(|| bad(format!("expected integer, got {json}"))),
        CqlType::Float | CqlType::Double => json
            .as_f64()
            .map(Value::Float)
            .ok_or_else(|| bad(format!("expected number, got {json}"))),
        CqlType::Boolean => json
            .as_bool()
            .map(Value::Boolean)
            .ok_or_else(|| bad(format!("expected boolean, got {json}"))),
        CqlType::Text | CqlType::Ascii | CqlType::Varchar => json
            .as_str()
            .map(|s| Value::Text(s.to_string()))
            .ok_or_else(|| bad(format!("expected string, got {json}"))),
        CqlType::Uuid | CqlType::TimeUuid => {
            let s = json
                .as_str()
                .ok_or_else(|| bad(format!("expected uuid string, got {json}")))?;
            let bytes = parse_uuid(s).ok_or_else(|| bad(format!("invalid uuid '{s}'")))?;
            Ok(Value::Uuid(bytes))
        }
        other => Err(bad(format!(
            "predicate pushdown unsupported for type {other:?}"
        ))),
    }
}

/// Parse a canonical hyphenated UUID string into 16 bytes.
fn parse_uuid(s: &str) -> Option<[u8; 16]> {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut bytes = [0u8; 16];
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{clustering_schema, simple_schema, uuid_schema};
    use serde_json::json;

    fn ticket_with(predicates: Vec<Predicate>) -> FlightTicket {
        FlightTicket {
            keyspace: "flight_ks".into(),
            table: "items".into(),
            predicates,
            ..Default::default()
        }
    }

    #[test]
    fn token_filter_built_from_bounds() {
        let mut t = ticket_with(vec![]);
        t.token_start = Some(-10);
        t.token_end = Some(10);
        let spec = ScanSpec::from_ticket(&t, &simple_schema()).unwrap();
        let tf = spec.token.expect("token filter present");
        assert!(!tf.contains(-10), "start exclusive");
        assert!(tf.contains(0));
        assert!(tf.contains(10), "end inclusive");
        assert!(!tf.contains(11));
    }

    #[test]
    fn overlaps_non_wraparound_boundaries() {
        // Split range (0, 100].
        let tf = TokenFilter {
            start: 0,
            end: 100,
            wraparound: false,
        };
        // Span entirely inside.
        assert!(tf.overlaps(10, 50));
        // Span straddling the start (max past start, min before start).
        assert!(tf.overlaps(-10, 10));
        // Span entirely below: max_token == start is NOT past the exclusive start.
        assert!(
            !tf.overlaps(-50, 0),
            "max_token==start excluded (half-open)"
        );
        assert!(!tf.overlaps(-50, -1));
        // Span entirely above: min_token == end is inclusive (overlaps).
        assert!(tf.overlaps(100, 200), "min_token==end included");
        assert!(!tf.overlaps(101, 200), "min_token>end excluded");
        // Span covering the whole range.
        assert!(tf.overlaps(i64::MIN, i64::MAX));
    }

    #[test]
    fn overlaps_wraparound_boundaries() {
        // Wraparound range (100, -100] = (100, MAX] ∪ [MIN, -100].
        let tf = TokenFilter {
            start: 100,
            end: -100,
            wraparound: true,
        };
        // Span in the high arm (past start).
        assert!(tf.overlaps(150, 200));
        // Span in the low arm (at or below end).
        assert!(tf.overlaps(-300, -200));
        assert!(tf.overlaps(-300, -100), "max_token reaching end inclusive");
        // Span entirely in the excluded middle gap (-99, 100].
        assert!(!tf.overlaps(-50, 50), "middle gap excluded");
        assert!(
            !tf.overlaps(0, 100),
            "max_token==start not past exclusive start"
        );
        // Span touching only the inclusive low end.
        assert!(tf.overlaps(-100, -100));
    }

    #[test]
    fn no_bounds_means_no_token_filter() {
        let spec = ScanSpec::from_ticket(&ticket_with(vec![]), &simple_schema()).unwrap();
        assert!(spec.token.is_none());
    }

    #[test]
    fn int_predicate_translates_with_natural_width() {
        let t = ticket_with(vec![Predicate {
            column: "score".into(),
            op: PredicateOp::Gt,
            value: json!(10),
        }]);
        let spec = ScanSpec::from_ticket(&t, &simple_schema()).unwrap();
        assert_eq!(spec.predicates.len(), 1);
        assert_eq!(spec.predicates[0].column, "score");
        assert!(matches!(spec.predicates[0].operation, SSTableFilterOp::Gt));
        assert_eq!(spec.predicates[0].values, vec![Value::Integer(10)]);
    }

    #[test]
    fn in_predicate_expands_json_array() {
        let t = ticket_with(vec![Predicate {
            column: "name".into(),
            op: PredicateOp::In,
            value: json!(["a", "b"]),
        }]);
        let spec = ScanSpec::from_ticket(&t, &simple_schema()).unwrap();
        assert_eq!(
            spec.predicates[0].values,
            vec![Value::Text("a".into()), Value::Text("b".into())]
        );
    }

    #[test]
    fn uuid_predicate_parses_to_bytes() {
        let t = FlightTicket {
            keyspace: "flight_ks".into(),
            table: "uu".into(),
            predicates: vec![Predicate {
                column: "id".into(),
                op: PredicateOp::Equal,
                value: json!("00000000-0000-0000-0000-000000000001"),
            }],
            ..Default::default()
        };
        let spec = ScanSpec::from_ticket(&t, &uuid_schema()).unwrap();
        let mut expected = [0u8; 16];
        expected[15] = 1;
        assert_eq!(spec.predicates[0].values, vec![Value::Uuid(expected)]);
    }

    #[test]
    fn predicate_on_clustering_column_resolves_type() {
        let t = FlightTicket {
            keyspace: "flight_ks".into(),
            table: "wide".into(),
            predicates: vec![Predicate {
                column: "ck".into(),
                op: PredicateOp::Equal,
                value: json!("a"),
            }],
            ..Default::default()
        };
        let spec = ScanSpec::from_ticket(&t, &clustering_schema()).unwrap();
        assert_eq!(spec.predicates[0].values, vec![Value::Text("a".into())]);
    }

    #[test]
    fn unknown_column_is_rejected() {
        let t = ticket_with(vec![Predicate {
            column: "nope".into(),
            op: PredicateOp::Equal,
            value: json!(1),
        }]);
        let err = ScanSpec::from_ticket(&t, &simple_schema()).unwrap_err();
        assert!(matches!(err, FilterError::UnknownColumn(c) if c == "nope"));
    }

    #[test]
    fn type_mismatch_is_rejected() {
        let t = ticket_with(vec![Predicate {
            column: "score".into(),
            op: PredicateOp::Equal,
            value: json!("not a number"),
        }]);
        let err = ScanSpec::from_ticket(&t, &simple_schema()).unwrap_err();
        assert!(matches!(err, FilterError::BadOperand { .. }));
    }

    #[test]
    fn empty_in_is_rejected() {
        let t = ticket_with(vec![Predicate {
            column: "score".into(),
            op: PredicateOp::In,
            value: json!([]),
        }]);
        let err = ScanSpec::from_ticket(&t, &simple_schema()).unwrap_err();
        assert!(matches!(err, FilterError::BadOperand { .. }));
    }

    #[test]
    fn null_operand_is_rejected() {
        let t = ticket_with(vec![Predicate {
            column: "score".into(),
            op: PredicateOp::Equal,
            value: serde_json::Value::Null,
        }]);
        let err = ScanSpec::from_ticket(&t, &simple_schema()).unwrap_err();
        assert!(matches!(err, FilterError::BadOperand { .. }));
    }

    #[test]
    fn out_of_range_int_is_rejected_not_truncated() {
        let t = ticket_with(vec![Predicate {
            column: "score".into(),
            op: PredicateOp::Equal,
            value: json!(i64::from(i32::MAX) + 1),
        }]);
        let err = ScanSpec::from_ticket(&t, &simple_schema()).unwrap_err();
        assert!(
            matches!(err, FilterError::BadOperand { .. }),
            "must error, not wrap"
        );
    }
}
