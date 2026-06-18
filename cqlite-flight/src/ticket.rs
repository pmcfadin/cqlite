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
}

/// Current ticket wire-format version. Bump when the JSON contract changes in a
/// way the server must distinguish.
pub const TICKET_VERSION: u8 = 1;

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
    #[serde(default)]
    pub wraparound: bool,
    /// Projection: emit only these columns. `None` emits all columns.
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    /// Predicates to evaluate server-side; empty means no predicate filtering.
    #[serde(default)]
    pub predicates: Vec<Predicate>,
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
        }
    }
}

impl FlightTicket {
    /// Parse a ticket from its on-the-wire JSON bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, TicketError> {
        Ok(serde_json::from_slice(bytes)?)
    }

    /// Serialize this ticket to its on-the-wire JSON bytes.
    pub fn to_bytes(&self) -> Result<Vec<u8>, TicketError> {
        Ok(serde_json::to_vec(self)?)
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
            (start, end) => token_in_half_open_range(
                token,
                start.unwrap_or(i64::MIN),
                end.unwrap_or(i64::MAX),
                self.wraparound,
            ),
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
    if wraparound {
        token > start || token <= end
    } else {
        token > start && token <= end
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn minimal_json() -> Vec<u8> {
        json!({
            "keyspace": "test_basic",
            "table": "simple_table",
            "ddl": "CREATE TABLE test_basic.simple_table (id uuid PRIMARY KEY, name text)"
        })
        .to_string()
        .into_bytes()
    }

    #[test]
    fn parses_minimal_ticket_with_defaults() {
        let t = FlightTicket::from_bytes(&minimal_json()).expect("parse");
        assert_eq!(t.keyspace, "test_basic");
        assert_eq!(t.table, "simple_table");
        assert!(t.ddl.starts_with("CREATE TABLE"));
        // Everything optional defaults sensibly.
        assert_eq!(t.snapshot, None);
        assert_eq!(t.token_start, None);
        assert_eq!(t.token_end, None);
        assert!(!t.wraparound);
        assert_eq!(t.columns, None);
        assert!(t.predicates.is_empty());
    }

    #[test]
    fn round_trips_full_ticket() {
        let ticket = FlightTicket {
            version: TICKET_VERSION,
            keyspace: "ks".into(),
            table: "tbl".into(),
            ddl: "CREATE TABLE ks.tbl (pk int PRIMARY KEY, v int)".into(),
            snapshot: Some("cqlite-abc".into()),
            token_start: Some(-100),
            token_end: Some(100),
            wraparound: false,
            columns: Some(vec!["pk".into(), "v".into()]),
            predicates: vec![Predicate {
                column: "v".into(),
                op: PredicateOp::Gt,
                value: json!(10),
            }],
        };
        let bytes = ticket.to_bytes().expect("serialize");
        let back = FlightTicket::from_bytes(&bytes).expect("parse");
        assert_eq!(ticket, back);
    }

    #[test]
    fn rejects_malformed_json() {
        assert!(FlightTicket::from_bytes(b"not json").is_err());
        // Missing required field `ddl`.
        let missing = json!({"keyspace": "k", "table": "t"}).to_string();
        assert!(FlightTicket::from_bytes(missing.as_bytes()).is_err());
    }

    #[test]
    fn parses_in_predicate_with_array_value() {
        let raw = json!({
            "keyspace": "k", "table": "t", "ddl": "CREATE TABLE k.t (a int PRIMARY KEY)",
            "predicates": [{"column": "a", "op": "In", "value": [1, 2, 3]}]
        })
        .to_string();
        let t = FlightTicket::from_bytes(raw.as_bytes()).expect("parse");
        assert_eq!(t.predicates.len(), 1);
        assert_eq!(t.predicates[0].op, PredicateOp::In);
        assert_eq!(t.predicates[0].value, json!([1, 2, 3]));
    }

    fn ticket_with_range(start: Option<i64>, end: Option<i64>, wrap: bool) -> FlightTicket {
        FlightTicket {
            keyspace: "k".into(),
            table: "t".into(),
            token_start: start,
            token_end: end,
            wraparound: wrap,
            ..Default::default()
        }
    }

    #[test]
    fn no_bounds_accepts_every_token() {
        let t = ticket_with_range(None, None, false);
        assert!(t.token_in_range(i64::MIN));
        assert!(t.token_in_range(0));
        assert!(t.token_in_range(i64::MAX));
    }

    #[test]
    fn normal_range_is_exclusive_start_inclusive_end() {
        let t = ticket_with_range(Some(-100), Some(100), false);
        assert!(!t.token_in_range(-100), "start is exclusive");
        assert!(t.token_in_range(-99));
        assert!(t.token_in_range(0));
        assert!(t.token_in_range(100), "end is inclusive");
        assert!(!t.token_in_range(101));
    }

    #[test]
    fn wraparound_range_accepts_either_side() {
        // Segment crossing the ring boundary: tokens > 100 OR <= -100.
        let t = ticket_with_range(Some(100), Some(-100), true);
        assert!(t.token_in_range(200), "above start");
        assert!(t.token_in_range(-200), "at/below end");
        assert!(!t.token_in_range(0), "the gap is excluded");
        assert!(!t.token_in_range(100), "start still exclusive");
        assert!(t.token_in_range(-100), "end still inclusive");
    }

    #[test]
    fn open_ended_bounds_default_to_min_max() {
        // A defaulted `start` is i64::MIN and stays exclusive, matching the
        // uniform (start, end] convention. Real Murmur3 tokens are never
        // i64::MIN (it is the ring's sentinel), so excluding it loses no data.
        let only_end = ticket_with_range(None, Some(0), false);
        assert!(
            !only_end.token_in_range(i64::MIN),
            "defaulted start is exclusive"
        );
        assert!(only_end.token_in_range(i64::MIN + 1));
        assert!(only_end.token_in_range(0));
        assert!(!only_end.token_in_range(1));

        let only_start = ticket_with_range(Some(0), None, false);
        assert!(!only_start.token_in_range(0));
        assert!(only_start.token_in_range(1));
        assert!(
            only_start.token_in_range(i64::MAX),
            "defaulted end is inclusive"
        );
    }
}
