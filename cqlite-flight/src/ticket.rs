//! Flight ticket: the request payload a client sends with `DoGet`.
//!
//! A ticket fully describes one scan of one `keyspace.table`: which snapshot to
//! read, the table DDL (so the server can build a `TableSchema` for the merge),
//! an optional token range (for cross-replica dedup — see `PLAN.md` §2), an
//! optional column projection, and zero or more predicates to evaluate
//! server-side.
//!
//! The ticket is intentionally free of any `cqlite-core` types so it can be
//! produced by a non-Rust client (the Java Trino connector) from plain JSON and
//! parsed here without coupling. Predicate values are carried as raw JSON and
//! are only interpreted when predicate evaluation runs (Phase 2).

use serde::{Deserialize, Serialize};

/// Errors produced while (de)serializing a [`FlightTicket`].
#[derive(Debug, thiserror::Error)]
pub enum TicketError {
    /// The ticket bytes were not valid UTF-8 JSON or did not match the schema.
    #[error("invalid flight ticket: {0}")]
    Decode(#[from] serde_json::Error),
    /// A path-bearing field (`keyspace`/`table`/`snapshot`) failed path-safety
    /// validation (issue #1430) — e.g. `../` traversal or an absolute component.
    #[error("invalid flight ticket field: {0}")]
    InvalidField(#[from] crate::pathsafe::PathSafetyError),
}

/// Current ticket wire-format version. Bump when the JSON contract changes in a
/// way the server must distinguish.
///
/// - v1: flat `predicates: Vec<Predicate>` (pure AND of leaves).
/// - v2: adds the recursive [`PredicateExpr`] `filter` tree (issue #834). v1
///   tickets remain accepted; see [`FlightTicket::effective_filter`] for the
///   back-compat rule.
pub const TICKET_VERSION: u8 = 2;

fn default_ticket_version() -> u8 {
    TICKET_VERSION
}

/// A comparison operator for a pushed-down predicate.
///
/// Mirrors the subset of `cqlite_core` `SSTableFilterOp` that makes sense to push
/// from a SQL engine. Translation to the core evaluator happens in Phase 2.
///
/// `#[non_exhaustive]`: this is a cross-language wire contract with the Java Trino
/// connector; new operators can be added without breaking the format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum PredicateOp {
    /// `column = value`
    Equal,
    /// `column IN (values)` — `value` is a JSON array.
    In,
    /// `column > value`
    Gt,
    /// `column >= value`
    Gte,
    /// `column < value`
    Lt,
    /// `column <= value`
    Lte,
    /// `column` starts with `value` (text prefix).
    Prefix,
}

/// A single predicate to evaluate against each emitted row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Predicate {
    /// Column the predicate applies to.
    pub column: String,
    /// Comparison operator.
    pub op: PredicateOp,
    /// Operand, carried as raw JSON until evaluation (Phase 2).
    pub value: serde_json::Value,
}

/// A recursive boolean predicate tree, evaluated server-side with SQL Kleene
/// three-valued logic (issue #834).
///
/// This supersedes the flat [`Predicate`] list, which could only express a pure
/// conjunction of leaves. The tree can express arbitrary nesting of `AND`/`OR`/
/// `NOT` over comparison, `IN`, and `IS NULL` leaves, so cross-column `OR`/`NOT`
/// pushdown becomes representable.
///
/// # Wire format
///
/// Serialized as internally-tagged JSON via `#[serde(tag = "type")]`. The struct
/// variants exist so serde can carry the tag inline (internally-tagged enums
/// cannot represent newtype-over-`Vec`). The exact JSON shapes are:
///
/// ```json
/// {"type":"And","exprs":[ ... ]}
/// {"type":"Or","exprs":[ ... ]}
/// {"type":"Not","expr":{ ... }}
/// {"type":"Compare","column":"c","op":"Equal","value":<json>}
/// {"type":"In","column":"c","values":[<json>, ...]}
/// {"type":"IsNull","column":"c"}
/// ```
///
/// Note that `IN` is its own node, NOT a `Compare` with `op = In`; `Compare`
/// uses only `{Equal, Gt, Gte, Lt, Lte, Prefix}`.
///
/// `#[non_exhaustive]`: this is a cross-language wire contract with the Java
/// Trino connector; new node kinds can be added without breaking the format.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
#[non_exhaustive]
pub enum PredicateExpr {
    /// Logical AND of zero or more sub-expressions. Empty AND is `TRUE`.
    And {
        /// Conjuncts.
        exprs: Vec<PredicateExpr>,
    },
    /// Logical OR of zero or more sub-expressions. Empty OR is `FALSE`.
    Or {
        /// Disjuncts.
        exprs: Vec<PredicateExpr>,
    },
    /// Logical negation (Kleene: `TRUE↔FALSE`, `UNKNOWN` stays `UNKNOWN`).
    Not {
        /// The negated sub-expression.
        expr: Box<PredicateExpr>,
    },
    /// A typed comparison leaf: `column op value`.
    Compare {
        /// Column the comparison applies to.
        column: String,
        /// Comparison operator (never `In`).
        op: PredicateOp,
        /// Operand, carried as raw JSON until lowering.
        value: serde_json::Value,
    },
    /// A membership leaf: `column IN (values)`.
    In {
        /// Column the membership test applies to.
        column: String,
        /// Candidate operands, carried as raw JSON until lowering.
        values: Vec<serde_json::Value>,
    },
    /// A null-test leaf: `column IS NULL`. Always definite (`TRUE`/`FALSE`),
    /// never `UNKNOWN`.
    IsNull {
        /// Column tested for null/absence.
        column: String,
    },
}

/// An aggregate function pushed down to the server (issue #841).
///
/// `Avg` is intentionally absent: the Trino connector decomposes `avg` into
/// `sum` + `count` before building the ticket and recombines them itself, so the
/// server only ever computes these four primitives.
///
/// Serialized by its variant name (`"Count"`, `"Sum"`, `"Min"`, `"Max"`).
///
/// `#[non_exhaustive]`: this is a cross-language wire contract with the Java Trino
/// connector; new functions can be added without breaking the format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum AggFunc {
    /// `count(*)` (when `column` is `None`) or `count(col)` (non-null count).
    Count,
    /// `sum(col)` — integer sources accumulate in checked `i64` (overflow is an
    /// error, matching Trino's non-pushed `sum(bigint)`); float sources in `f64`.
    Sum,
    /// A sum coerced to `f64` regardless of source type, used as the numerator of
    /// a pushed `avg(col)` (issue #902). Unlike [`AggFunc::Sum`] it never overflows
    /// (integer inputs widen to `f64`), so an integer `avg` whose running total
    /// exceeds `i64` succeeds where Trino's 128-bit `avg` also would — instead of
    /// failing the way a checked-`i64` `Sum` would. Always `Float64`, null when the
    /// group has no non-null inputs.
    SumDouble,
    /// `min(col)`.
    Min,
    /// `max(col)`.
    Max,
}

/// One aggregate to compute server-side, plus the name of the partial output
/// column it produces (issue #841).
///
/// `column` is `None` only for `count(*)`. For every other function, and for
/// `count(col)`, `column` names the source column. `output` is the name of the
/// Arrow column the partial value is emitted under.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AggregateSpec {
    /// Which aggregate to compute.
    pub func: AggFunc,
    /// Source column, or `None` for `count(*)`.
    #[serde(default)]
    pub column: Option<String>,
    /// Name of the partial output column.
    pub output: String,
}

/// A pushed-down aggregation: an optional `GROUP BY` plus one or more aggregates
/// (issue #841).
///
/// When carried on a [`FlightTicket`], the server computes PARTIAL aggregates
/// over the surviving (token-pruned, predicate-filtered, tombstone-reconciled,
/// LWW-resolved) rows and emits one partial row per group instead of the full
/// row set. The connector merges partials across ranges/nodes.
///
/// `#[non_exhaustive]`: this is a cross-language wire contract with the Java
/// Trino connector.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Aggregation {
    /// Grouping columns, in output order. Empty means a single global group.
    #[serde(default)]
    pub group_by: Vec<String>,
    /// Aggregates to compute, in output order.
    pub aggregates: Vec<AggregateSpec>,
}

/// The full description of one Flight scan.
///
/// `#[non_exhaustive]`: the JSON form is the contract with the Java Trino
/// connector. Construct in Rust via struct update from [`FlightTicket::default`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct FlightTicket {
    /// Wire-format version (see [`TICKET_VERSION`]).
    #[serde(default = "default_ticket_version")]
    pub version: u8,
    /// Keyspace name.
    pub keyspace: String,
    /// Table name.
    pub table: String,
    /// CQL `CREATE TABLE` DDL — parsed into a `TableSchema` to drive the merge.
    pub ddl: String,
    /// Sidecar snapshot name to read. `None` reads the live data dir (dev only).
    #[serde(default)]
    pub snapshot: Option<String>,
    /// Exclusive lower token bound. See [`FlightTicket::token_in_range`].
    #[serde(default)]
    pub token_start: Option<i64>,
    /// Inclusive upper token bound. See [`FlightTicket::token_in_range`].
    #[serde(default)]
    pub token_end: Option<i64>,
    /// Whether the token range wraps the ring (`token_start > token_end`).
    ///
    /// RETAINED FOR WIRE COMPATIBILITY AND NO LONGER CONSULTED (issue #3634).
    /// Wrapping is DERIVED from `token_start`/`token_end` wherever it is needed —
    /// [`Self::token_in_range`] here, and `ScanSpec::from_ticket` on the producer
    /// side — the way `Range.isWrapAround(left, right)` derives it at
    /// `cassandra-5.0.8` (`left.compareTo(right) >= 0`). A flag that contradicts
    /// its own endpoints describes a range Cassandra cannot express, so honouring
    /// it could only produce a membership rule no Cassandra range has. Old
    /// connectors may keep sending it; it is ignored, never rejected.
    #[serde(default)]
    pub wraparound: bool,
    /// Projection: emit only these columns. `None` emits all columns.
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    /// v1 flat predicates (pure AND of leaves). Retained for back-compat; when
    /// [`Self::filter`] is `None` these are folded into an `And` tree. Empty
    /// means no predicate filtering.
    #[serde(default)]
    pub predicates: Vec<Predicate>,
    /// v2 recursive predicate tree (issue #834). When `Some`, this is the
    /// authoritative filter and [`Self::predicates`] is ignored. See
    /// [`Self::effective_filter`].
    #[serde(default)]
    pub filter: Option<PredicateExpr>,
    /// Aggregation pushdown (issue #841). When `Some`, the producer computes
    /// PARTIAL aggregates over the surviving rows and emits partial rows under
    /// the partial schema instead of full rows. `None` keeps the row path.
    #[serde(default)]
    pub aggregation: Option<Aggregation>,
    /// LIMIT pushdown (issue #2129). When `Some(n)`, the producer emits at most
    /// `n` rows for this ticket, stopping the k-way merge as soon as the cap is
    /// reached — turning `LIMIT k` into bounded per-split work instead of a full
    /// range scan. The cap is applied AFTER token pruning and predicate
    /// filtering, so pruned/filtered-out rows never consume it. Because each
    /// split caps independently, the connector reports `limitGuaranteed = false`
    /// and Trino keeps a global `Limit` above the scan to do the final cut.
    /// Ignored when [`Self::aggregation`] is `Some` (the aggregate already
    /// collapses the row set to partial rows). `None` scans the full range.
    ///
    /// Compat: the field is additive-optional — the ticket struct is NOT
    /// `deny_unknown_fields`, so an old server silently ignores a `limit` from a
    /// new connector (falling back to a correct full scan) and an old connector's
    /// ticket defaults `limit` to `None` on a new server. Both directions are
    /// safe; the only cost of skew is losing the bounding, never wrong rows.
    #[serde(default)]
    pub limit: Option<u64>,
}

impl Default for FlightTicket {
    fn default() -> Self {
        Self {
            version: TICKET_VERSION,
            keyspace: String::new(),
            table: String::new(),
            ddl: String::new(),
            snapshot: None,
            token_start: None,
            token_end: None,
            wraparound: false,
            columns: None,
            predicates: Vec::new(),
            filter: None,
            aggregation: None,
            limit: None,
        }
    }
}

impl FlightTicket {
    /// Parse a ticket from its on-the-wire JSON bytes.
    ///
    /// Rejects tickets whose path-bearing fields (`keyspace`/`table`/`snapshot`)
    /// would traverse out of or replace the server's data directory (issue #1430)
    /// — this is the PRIMARY guard against path-traversal / arbitrary-file
    /// disclosure.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, TicketError> {
        let ticket: Self = serde_json::from_slice(bytes)?;
        ticket.validate()?;
        Ok(ticket)
    }

    /// Validate the path-bearing fields against the path-safety grammar.
    ///
    /// `keyspace`/`table` must be Cassandra unquoted identifiers
    /// (`[A-Za-z0-9_]+`); `snapshot`, when present, additionally allows `-`. This
    /// inherently rejects `.`, `/`, `\`, NUL, and absolute paths, so no field can
    /// escape the data directory when joined as a path component.
    pub fn validate(&self) -> Result<(), TicketError> {
        crate::pathsafe::validate_identifier("keyspace", &self.keyspace)?;
        crate::pathsafe::validate_identifier("table", &self.table)?;
        if let Some(snapshot) = &self.snapshot {
            crate::pathsafe::validate_snapshot(snapshot)?;
        }
        Ok(())
    }

    /// Serialize this ticket to its on-the-wire JSON bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, TicketError> {
        Ok(serde_json::to_vec(self)?)
    }

    /// Resolve the single predicate tree the server should evaluate, reconciling
    /// the v2 `filter` and the v1 `predicates` list.
    ///
    /// Back-compat rule (issue #834):
    /// - If [`Self::filter`] is `Some`, it is authoritative and `predicates` is
    ///   ignored.
    /// - Otherwise, if `predicates` is non-empty, the effective filter is the
    ///   `And` of the leaves: each v1 [`Predicate`] becomes a
    ///   [`PredicateExpr::Compare`], EXCEPT `op == In` which becomes a
    ///   [`PredicateExpr::In`] node. A v1 `In` operand must be a JSON array;
    ///   `ScanSpec::from_ticket` rejects a non-array operand up-front (matching
    ///   the original v1 error), so the singleton-wrapping fallback here is a
    ///   defensive default a validated ticket never reaches.
    /// - If both are empty → `None` (no filtering).
    pub fn effective_filter(&self) -> Option<PredicateExpr> {
        if let Some(filter) = &self.filter {
            return Some(filter.clone());
        }
        if self.predicates.is_empty() {
            return None;
        }
        let exprs = self
            .predicates
            .iter()
            .map(|p| match p.op {
                PredicateOp::In => {
                    let values = match &p.value {
                        serde_json::Value::Array(items) => items.clone(),
                        // A non-array IN operand is treated as a singleton list,
                        // matching how v1 evaluation would have compared it.
                        other => vec![other.clone()],
                    };
                    PredicateExpr::In {
                        column: p.column.clone(),
                        values,
                    }
                }
                op => PredicateExpr::Compare {
                    column: p.column.clone(),
                    op,
                    value: p.value.clone(),
                },
            })
            .collect();
        Some(PredicateExpr::And { exprs })
    }

    /// Does a partition `token` fall inside this ticket's token range?
    ///
    /// Token ranges follow the Cassandra / Sidecar convention of being
    /// half-open as `(start, end]` — exclusive of `start`, inclusive of `end`.
    /// A missing `start` is treated as `i64::MIN` and a missing `end` as
    /// `i64::MAX`; when both are absent there is no range filter and every token
    /// is in range.
    ///
    /// A wraparound range (the segment crossing the ring's min-token boundary,
    /// where `start > end`) keeps tokens that are either above `start` or at/below
    /// `end`.
    pub fn token_in_range(&self, token: i64) -> bool {
        match (self.token_start, self.token_end) {
            (None, None) => true,
            (start, end) => {
                let start = start.unwrap_or(i64::MIN);
                let end = end.unwrap_or(i64::MAX);
                // DERIVED from the normalized endpoints, NOT from `self.wraparound`
                // (issue #3634). This method is public API, and the server filters
                // with a bound whose wrapping is derived
                // (`ScanSpec::from_ticket` -> `TokenFilter` -> `ScanTokenBound`).
                // Trusting the wire flag here would let this method report a
                // DIFFERENT membership than the scan actually applies for a ticket
                // whose flag contradicts its endpoints — and `validate()` accepts
                // such a ticket, because the flag is `#[serde(default)]` and is
                // never checked against the bounds.
                token_in_half_open_range(token, start, end, start >= end)
            }
        }
    }
}

/// Half-open `(start, end]` membership with optional ring wraparound.
///
/// Shared by [`FlightTicket::token_in_range`] and the producer's token filter so
/// the semantics never diverge. A normal range keeps `start < token <= end`; a
/// wraparound range (crossing the ring's min-token boundary) keeps
/// `token > start || token <= end`.
pub fn token_in_half_open_range(token: i64, start: i64, end: i64, wraparound: bool) -> bool {
    // #2228: equal endpoints denote the FULL ring, not the empty set — matching
    // Cassandra's convention where a range `(T, T]` covers every token. The
    // split planner can emit this for single-token topologies. Accept every
    // token here regardless of the `wraparound` flag so no partition is silently
    // dropped (which would make `SELECT *` return 0 rows with no error).
    if start == end {
        return true;
    }
    if wraparound {
        token > start || token <= end
    } else {
        token > start && token <= end
    }
}

#[cfg(test)]
#[path = "ticket_tests.rs"]
mod tests;
