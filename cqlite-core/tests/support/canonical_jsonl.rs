//! Canonical sstabledump-JSONL comparator (issue #1009, epic #971).
//!
//! This is the **single, reusable** strict comparison lane shared by all of
//! epic #971's CQL-type / schema-evolution parity tests (#1003, #1006, #1007,
//! #1008, and this self-test #1009). It parses BOTH:
//!
//!   * Apache Cassandra `sstabledump` JSONL goldens (the committed
//!     `*-Data.db.jsonl` reference files), and
//!   * CQLite's own sstabledump-shaped JSONL output,
//!
//! into a single **typed canonical record model** and compares two documents
//! positionally, FAILING LOUD with a precise diff on any type/value divergence.
//!
//! ## Why a shared module (not per-test ad-hoc parsing)
//!
//! `cqlite-core/tests/issue_1010_deletion_markers_parity.rs` and
//! `issue_694_writetime_ttl_parity.rs` each re-implement bespoke JSONL parsing
//! that captures only the slice each test needs (deletion markers; writetime/
//! ttl). Epic #971 needs ONE comparator that captures the full canonical row:
//! partition key, clustering key, every cell (name + path + typed value +
//! writetime + ttl + local-deletion-time), and every deletion marker
//! (partition / row / cell / range), so the type-evolution lanes can assert
//! against it without re-deriving the format each time.
//!
//! Tests include it with:
//! ```ignore
//! #[path = "support/canonical_jsonl.rs"]
//! mod canonical_jsonl;
//! ```
//!
//! ## Canonical model & comparison contract
//!
//! The unit of comparison is a [`CanonicalDocument`] = ordered list of
//! [`CanonicalPartition`]s. Order is preserved EXACTLY where Cassandra defines
//! it (partition document order, clustering/row order, cell-path order within a
//! collection, range-bound order). Comparison is positional, never set/count
//! based: a reordering, an over-emission, or an under-emission is a failure
//! reported at the exact index.
//!
//! ### Normalization (formatting-only) vs. semantic difference
//!
//! Formatting-only differences are normalized away BEFORE comparison so they do
//! NOT fail:
//!   * JSON object key ordering is irrelevant (we read into a typed model).
//!   * Whitespace inside JSON arrays/objects (`[34, 99]` vs `[34,99]`) is
//!     irrelevant (serde_json parse).
//!   * Equivalent representations of the *same typed value* are unified:
//!     - integers parsed from JSON numbers vs. numeric strings,
//!     - timestamp strings normalized to epoch-microseconds
//!       (`2025-10-06T01:12:06.060Z` == `2025-10-06T01:12:06.060000Z`),
//!     - trailing-zero fractional seconds collapse.
//!
//! Anything that changes the *typed value*, the writetime, the ttl, the
//! local-deletion-time, the cell path, a key component, or a deletion marker
//! FAILS with a [`Diff`] naming manifest id, fixture path, row key, column
//! path, and expected-vs-actual canonical record.
//!
//! ### No-placeholder / fail-loud contract
//!
//! [`load_golden_document`] ERRORS (never silently passes) when the reference
//! file is missing, empty, malformed JSON, or a recognized placeholder
//! (`"PLACEHOLDER"`, `"TODO"`, `"GENERATED"` sentinel, or a zero-row file where
//! rows are expected). Strict tests must surface this as a failure.

#![allow(dead_code)] // shared module: not every consumer uses every item.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use serde_json::Value as JsonValue;

// ===========================================================================
// Typed canonical value
// ===========================================================================

/// A typed, comparison-stable canonical value decoded from a JSONL cell or key
/// component. Distinct variants never compare equal across types (a `text "5"`
/// is NOT a `bigint 5`). Order is preserved in `List`, `Map`, `Set`, and
/// `Tuple` per Cassandra's persisted ordering.
///
/// `Absent` (the column/value key was missing entirely) is intentionally
/// DISTINCT from `Null` (an explicit JSON `null`) and from an empty collection
/// — masking either with a default would hide a real parity bug.
#[derive(Debug, Clone)]
pub enum CanonicalValue {
    /// The JSON key was absent entirely (e.g. a cell tombstone with no `value`).
    Absent,
    /// An explicit JSON `null`.
    Null,
    Bool(bool),
    /// Any whole-number value (CQL int/bigint/smallint/tinyint/varint/counter).
    /// sstabledump renders all of these as bare JSON integers, so they collapse
    /// into one canonical integer variant; the schema lane that knows the column
    /// type can refine this if it ever needs to distinguish width.
    Int(i128),
    /// A non-integral number (float/double/decimal rendered numerically).
    Float(NormalizedFloat),
    /// Free text / varchar / ascii, and anything sstabledump renders as a quoted
    /// string that is NOT recognized as a timestamp.
    Text(String),
    /// A timestamp value, normalized to epoch-microseconds for equivalence
    /// across fractional-second formatting (`.06Z` vs `.060000Z`). The original
    /// string is retained for diagnostics only and is NOT compared.
    Timestamp { micros: i64, raw: String },
    /// An ordered list (CQL list / frozen list / vector).
    List(Vec<CanonicalValue>),
    /// An ordered set — Cassandra persists set elements sorted, so order is
    /// meaningful and compared positionally.
    Set(Vec<CanonicalValue>),
    /// An ordered map. Cassandra persists map entries sorted by key, so the
    /// (key,value) pairs are compared positionally.
    Map(Vec<(CanonicalValue, CanonicalValue)>),
    /// A tuple / UDT rendered as a JSON object: field order preserved.
    Tuple(Vec<(String, CanonicalValue)>),
}

/// A float wrapper giving deterministic equality (bitwise on the canonical
/// `f64`, with all NaNs unified) so floats can live in a derived-`PartialEq`
/// enum without `f64`'s non-`Eq` surprises tripping the comparator.
#[derive(Debug, Clone)]
pub struct NormalizedFloat(pub f64);

impl PartialEq for NormalizedFloat {
    fn eq(&self, other: &Self) -> bool {
        if self.0.is_nan() && other.0.is_nan() {
            return true;
        }
        self.0.to_bits() == other.0.to_bits()
    }
}

/// Manual equality so [`CanonicalValue::Timestamp`] compares ONLY by its
/// normalized epoch-microseconds (the `raw` string is diagnostic and varies by
/// formatting). All other variants compare structurally. Cross-variant compares
/// are always unequal — type is load-bearing.
impl PartialEq for CanonicalValue {
    fn eq(&self, other: &Self) -> bool {
        use CanonicalValue::*;
        match (self, other) {
            (Absent, Absent) => true,
            (Null, Null) => true,
            (Bool(a), Bool(b)) => a == b,
            (Int(a), Int(b)) => a == b,
            (Float(a), Float(b)) => a == b,
            (Text(a), Text(b)) => a == b,
            (Timestamp { micros: a, .. }, Timestamp { micros: b, .. }) => a == b,
            (List(a), List(b)) => a == b,
            (Set(a), Set(b)) => a == b,
            (Map(a), Map(b)) => a == b,
            (Tuple(a), Tuple(b)) => a == b,
            _ => false,
        }
    }
}

impl CanonicalValue {
    /// Decode a JSON value (sstabledump cell `value`, or a key/path component)
    /// into a canonical value. `column_hint` carries the column name purely for
    /// timestamp recognition heuristics when the schema is unavailable; it is
    /// NEVER used to coerce a type, satisfying the no-heuristics mandate (the
    /// type is taken from the JSON shape, not guessed from the name).
    pub fn from_json(v: &JsonValue) -> Self {
        match v {
            JsonValue::Null => CanonicalValue::Null,
            JsonValue::Bool(b) => CanonicalValue::Bool(*b),
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    CanonicalValue::Int(i as i128)
                } else if let Some(u) = n.as_u64() {
                    CanonicalValue::Int(u as i128)
                } else if let Some(f) = n.as_f64() {
                    CanonicalValue::Float(NormalizedFloat(f))
                } else {
                    // serde_json arbitrary-precision: keep the textual form.
                    CanonicalValue::Text(n.to_string())
                }
            }
            JsonValue::String(s) => {
                // A JSON *string* stays Text. We deliberately do NOT coerce a
                // numeric-looking string (e.g. CQL text/ascii/varchar "5") to
                // Int: sstabledump renders typed integers as bare JSON numbers
                // and text as quoted strings, so the JSON shape already carries
                // the type. Coercing here would mask a real text-vs-numeric
                // parity bug (`text "5"` must NOT equal `int 5`) and violate the
                // typed-comparator contract. Timestamps are the one exception:
                // sstabledump renders them as quoted strings, so we recognize
                // and normalize those to an epoch-µs instant.
                if let Some(micros) = parse_timestamp_micros(s) {
                    CanonicalValue::Timestamp {
                        micros,
                        raw: s.clone(),
                    }
                } else {
                    CanonicalValue::Text(s.clone())
                }
            }
            JsonValue::Array(a) => {
                CanonicalValue::List(a.iter().map(CanonicalValue::from_json).collect())
            }
            JsonValue::Object(o) => CanonicalValue::Tuple(
                o.iter()
                    .map(|(k, v)| (k.clone(), CanonicalValue::from_json(v)))
                    .collect(),
            ),
        }
    }

    /// Decode a partition-key / clustering-key COMPONENT.
    ///
    /// This differs from [`CanonicalValue::from_json`] in exactly one,
    /// schema-aware-but-narrow way: sstabledump renders partition-key components
    /// as quoted JSON *strings* even for numeric key columns (e.g. an `int`
    /// partition key `1` is dumped as `"1"`), whereas CQLite's own JSONL emits
    /// the typed number `1`. For KEY COMPONENTS ONLY we therefore coerce a clean
    /// numeric string to `Int` so the two equivalent key renderings unify.
    ///
    /// This is intentionally NOT applied to cell VALUES (see `from_json`): a CQL
    /// text/ascii/varchar cell `"5"` must stay `Text` and must NOT equal a
    /// numeric `5`. The coercion here is confined to the key path because that is
    /// the only place sstabledump's string-stringification of typed scalars
    /// occurs, so it cannot mask a text-vs-numeric cell-value parity bug.
    pub fn from_json_key(v: &JsonValue) -> Self {
        if let JsonValue::String(s) = v {
            if parse_timestamp_micros(s).is_none() {
                if let Some(i) = parse_strict_i128(s) {
                    return CanonicalValue::Int(i);
                }
            }
        }
        CanonicalValue::from_json(v)
    }

    /// Compact single-line rendering for diff messages.
    fn render(&self) -> String {
        match self {
            CanonicalValue::Absent => "<absent>".to_string(),
            CanonicalValue::Null => "null".to_string(),
            CanonicalValue::Bool(b) => b.to_string(),
            CanonicalValue::Int(i) => i.to_string(),
            CanonicalValue::Float(f) => f.0.to_string(),
            CanonicalValue::Text(s) => format!("{s:?}"),
            CanonicalValue::Timestamp { micros, raw } => format!("ts({micros}µs={raw})"),
            CanonicalValue::List(xs) => format!(
                "[{}]",
                xs.iter().map(|x| x.render()).collect::<Vec<_>>().join(", ")
            ),
            CanonicalValue::Set(xs) => format!(
                "{{{}}}",
                xs.iter().map(|x| x.render()).collect::<Vec<_>>().join(", ")
            ),
            CanonicalValue::Map(kvs) => format!(
                "{{{}}}",
                kvs.iter()
                    .map(|(k, v)| format!("{}: {}", k.render(), v.render()))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            CanonicalValue::Tuple(fs) => format!(
                "({})",
                fs.iter()
                    .map(|(k, v)| format!("{k}={}", v.render()))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

/// Parse a string strictly as a base-10 i128. Returns `None` for anything that
/// is not a clean integer (leading-zero / sign-prefixed / non-digit forms are
/// rejected). Used ONLY by [`CanonicalValue::from_json_key`] to unify
/// sstabledump's string-rendered numeric KEY components with CQLite's typed
/// numbers — never for cell values.
fn parse_strict_i128(s: &str) -> Option<i128> {
    let t = s.trim();
    if t.is_empty() {
        return None;
    }
    let body = t.strip_prefix('-').unwrap_or(t);
    if body.len() > 1 && body.starts_with('0') {
        return None;
    }
    if !body.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    t.parse::<i128>().ok()
}

// ===========================================================================
// Timestamp normalization (ISO-8601 → epoch microseconds)
// ===========================================================================

/// Recognize and normalize an sstabledump timestamp string to
/// epoch-microseconds. Accepts BOTH the ISO-8601 `T` separator
/// (`YYYY-MM-DDTHH:MM:SS[.ffffff]Z`) and the SPACE separator that real
/// `sstabledump` emits in the committed goldens (`YYYY-MM-DD HH:MM:SS[.fff]Z`),
/// so the two equivalent renderings normalize to the same instant. Returns
/// `None` for any string that is not a `Z`-suffixed timestamp, so plain text is
/// left as `Text`.
pub fn parse_timestamp_micros(s: &str) -> Option<i64> {
    let body = s.strip_suffix('Z')?;
    // sstabledump uses a space between date and time; ISO-8601 uses `T`. Accept
    // either so `"2025-10-06 01:12:07.265Z"` == `"2025-10-06T01:12:07.265000Z"`.
    let (date_part, time_part) = body
        .split_once('T')
        .or_else(|| body.split_once(' '))?;

    let mut dp = date_part.splitn(3, '-');
    let year: i64 = dp.next()?.parse().ok()?;
    let month: i64 = dp.next()?.parse().ok()?;
    let day: i64 = dp.next()?.parse().ok()?;
    if dp.next().is_some() {
        return None;
    }

    let (hms, frac) = match time_part.split_once('.') {
        Some((h, f)) => (h, f),
        None => (time_part, ""),
    };
    let mut tp = hms.splitn(3, ':');
    let hour: i64 = tp.next()?.parse().ok()?;
    let minute: i64 = tp.next()?.parse().ok()?;
    let second: i64 = tp.next()?.parse().ok()?;
    if tp.next().is_some() {
        return None;
    }
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    let days = days_since_epoch(year, month, day)?;
    let epoch_seconds = days * 86_400 + hour * 3_600 + minute * 60 + second;
    let frac_micros = if frac.is_empty() {
        0
    } else if frac.bytes().all(|b| b.is_ascii_digit()) {
        // Pad/truncate to exactly 6 digits so ".06" == ".060000".
        format!("{:0<6}", &frac[..frac.len().min(6)])
            .parse::<i64>()
            .ok()?
    } else {
        return None;
    };
    Some(epoch_seconds * 1_000_000 + frac_micros)
}

fn days_since_epoch(year: i64, month: i64, day: i64) -> Option<i64> {
    let a = (14 - month) / 12;
    let y = year + 4800 - a;
    let m = month + 12 * a - 3;
    let jdn = day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045;
    Some(jdn - 2_440_588)
}

// ===========================================================================
// Canonical row / cell / deletion model
// ===========================================================================

/// A single cell decoded from a row's `cells` array.
///
/// Cells are ordered as sstabledump emits them. For collections, sstabledump
/// emits one cell per element with a `path`; this model preserves the path so
/// element identity and ordering are compared positionally.
#[derive(Debug, Clone, PartialEq)]
pub struct CanonicalCell {
    /// Column name.
    pub name: String,
    /// Cell path (collection key / element discriminator). Empty for scalars.
    /// ORDER-SENSITIVE and positional.
    pub path: Vec<CanonicalValue>,
    /// The typed value. `Absent` when the cell carries no `value` (a cell
    /// tombstone or a collection-shell deletion marker).
    pub value: CanonicalValue,
    /// Per-cell writetime (`tstamp`) in epoch-µs, if present.
    pub writetime_micros: Option<i64>,
    /// Per-cell TTL in seconds, if present.
    pub ttl_secs: Option<i64>,
    /// Per-cell deletion: `markedForDeleteAt` µs (from sibling `tstamp`) +
    /// `local_delete_time` seconds (from the `deletion_info` block). Present
    /// only for cell tombstones / collection-shell deletes.
    pub deletion: Option<CellDeletion>,
}

/// A cell-level deletion marker.
#[derive(Debug, Clone, PartialEq)]
pub struct CellDeletion {
    /// markedForDeleteAt, epoch-µs. `None` if sstabledump omitted it.
    pub marked_deleted_micros: Option<i64>,
    /// local_delete_time, epoch-seconds.
    pub local_delete_secs: Option<i64>,
}

/// Per-row liveness info (`liveness_info`): the primary-key writetime + TTL.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct LivenessInfo {
    pub tstamp_micros: Option<i64>,
    pub ttl_secs: Option<i64>,
    pub expires_at_micros: Option<i64>,
}

/// A whole-row / partition / range deletion marker.
#[derive(Debug, Clone, PartialEq)]
pub struct DeletionInfo {
    pub marked_deleted_micros: Option<i64>,
    pub local_delete_secs: Option<i64>,
}

/// A canonical row (`type == "row"` or `type == "static_block"`).
#[derive(Debug, Clone, PartialEq)]
pub struct CanonicalRow {
    /// `"row"` or `"static_block"`.
    pub row_type: String,
    /// Clustering-key components, ORDER-SENSITIVE. Empty for static blocks and
    /// for tables with no clustering key.
    pub clustering: Vec<CanonicalValue>,
    /// Row liveness (PK writetime + ttl). `None` when the row carries none
    /// (e.g. a pure row tombstone).
    pub liveness: Option<LivenessInfo>,
    /// Row-level deletion marker (a row tombstone). Distinct from `None`.
    pub deletion: Option<DeletionInfo>,
    /// Cells in document order.
    pub cells: Vec<CanonicalCell>,
}

/// A range-tombstone bound/boundary entry within a partition's `rows`.
#[derive(Debug, Clone, PartialEq)]
pub struct CanonicalRangeBound {
    /// `"range_tombstone_bound"` or `"range_tombstone_boundary"`.
    pub entry_type: String,
    /// Start half (if present), as (clustering, inclusive, deletion).
    pub start: Option<BoundHalf>,
    /// End half (if present).
    pub end: Option<BoundHalf>,
}

/// One side of a range bound.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundHalf {
    pub clustering: Vec<CanonicalValue>,
    pub inclusive: bool,
    pub marked_deleted_micros: Option<i64>,
    pub local_delete_secs: Option<i64>,
}

/// A canonical partition: key + optional partition tombstone + ordered rows +
/// ordered range bounds.
#[derive(Debug, Clone, PartialEq)]
pub struct CanonicalPartition {
    /// Partition-key components, ORDER-SENSITIVE.
    pub key: Vec<CanonicalValue>,
    /// Partition-level deletion marker. Distinct from `None`.
    pub deletion: Option<DeletionInfo>,
    /// Live rows / static blocks, document order.
    pub rows: Vec<CanonicalRow>,
    /// Range-tombstone bounds/boundaries, document order.
    pub range_bounds: Vec<CanonicalRangeBound>,
}

/// A whole sstabledump document = ordered partitions.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CanonicalDocument {
    pub partitions: Vec<CanonicalPartition>,
}

// ===========================================================================
// Errors
// ===========================================================================

/// Error loading/parsing a golden or CQLite JSONL document. Strict tests must
/// treat any of these as a hard failure (never skip).
#[derive(Debug)]
pub enum CanonicalError {
    /// The reference file does not exist.
    Missing(PathBuf),
    /// The reference file exists but is empty (zero non-blank lines).
    Empty(PathBuf),
    /// A line failed to parse as JSON.
    Malformed { path: PathBuf, line: usize, msg: String },
    /// The reference is a placeholder/sentinel, not real golden data.
    Placeholder { path: PathBuf, marker: String },
    /// Structural problem (a `partition` object missing `key`, etc.).
    Structure { path: PathBuf, line: usize, msg: String },
}

impl fmt::Display for CanonicalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CanonicalError::Missing(p) => {
                write!(f, "canonical JSONL reference MISSING: {}", p.display())
            }
            CanonicalError::Empty(p) => {
                write!(f, "canonical JSONL reference EMPTY (no rows): {}", p.display())
            }
            CanonicalError::Malformed { path, line, msg } => write!(
                f,
                "canonical JSONL reference MALFORMED at {}:{line}: {msg}",
                path.display()
            ),
            CanonicalError::Placeholder { path, marker } => write!(
                f,
                "canonical JSONL reference is a PLACEHOLDER ({marker}), not real golden data: {}",
                path.display()
            ),
            CanonicalError::Structure { path, line, msg } => write!(
                f,
                "canonical JSONL reference STRUCTURE error at {}:{line}: {msg}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for CanonicalError {}

// ===========================================================================
// Loading / parsing
// ===========================================================================

/// Placeholder sentinels that mean "no real golden data here". A reference whose
/// raw content contains one of these (as a whole-file marker or a sentinel
/// partition key) is rejected, never silently passed.
const PLACEHOLDER_MARKERS: &[&str] = &[
    "\"PLACEHOLDER\"",
    "\"__PLACEHOLDER__\"",
    "\"TODO\"",
    "\"GENERATED_PLACEHOLDER\"",
    "PLACEHOLDER_REFERENCE",
];

/// Load and parse an sstabledump-shaped JSONL document from `path`.
///
/// FAILS LOUD (returns `Err`) when:
///   * the file is missing ([`CanonicalError::Missing`]),
///   * the file is empty / all-blank ([`CanonicalError::Empty`]),
///   * any line is not valid JSON ([`CanonicalError::Malformed`]),
///   * the content is a recognized placeholder ([`CanonicalError::Placeholder`]),
///   * a partition object is structurally invalid ([`CanonicalError::Structure`]).
///
/// `expect_rows = true` additionally rejects a document that parses to zero
/// partitions, for lanes where the fixture is known to contain data.
pub fn load_golden_document(path: &Path, expect_rows: bool) -> Result<CanonicalDocument, CanonicalError> {
    if !path.exists() {
        return Err(CanonicalError::Missing(path.to_path_buf()));
    }
    let content = std::fs::read_to_string(path)
        .map_err(|e| CanonicalError::Malformed { path: path.to_path_buf(), line: 0, msg: e.to_string() })?;

    for marker in PLACEHOLDER_MARKERS {
        if content.contains(marker) {
            return Err(CanonicalError::Placeholder {
                path: path.to_path_buf(),
                marker: marker.trim_matches('"').to_string(),
            });
        }
    }

    parse_document_str(&content, path, expect_rows)
}

/// Parse an in-memory JSONL document (one JSON object per non-blank line). Used
/// by [`load_golden_document`] and directly by tests that synthesize a CQLite
/// JSONL document in memory.
pub fn parse_document_str(
    content: &str,
    path: &Path,
    expect_rows: bool,
) -> Result<CanonicalDocument, CanonicalError> {
    let mut partitions = Vec::new();
    let mut nonblank = 0usize;

    for (idx, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        nonblank += 1;
        let lineno = idx + 1;
        let v: JsonValue = serde_json::from_str(line).map_err(|e| CanonicalError::Malformed {
            path: path.to_path_buf(),
            line: lineno,
            msg: e.to_string(),
        })?;
        let partition = parse_partition(&v, path, lineno)?;
        partitions.push(partition);
    }

    if nonblank == 0 {
        return Err(CanonicalError::Empty(path.to_path_buf()));
    }
    if expect_rows && partitions.is_empty() {
        return Err(CanonicalError::Empty(path.to_path_buf()));
    }

    Ok(CanonicalDocument { partitions })
}

fn parse_partition(
    v: &JsonValue,
    path: &Path,
    line: usize,
) -> Result<CanonicalPartition, CanonicalError> {
    let partition = v.get("partition").ok_or_else(|| CanonicalError::Structure {
        path: path.to_path_buf(),
        line,
        msg: "top-level object missing `partition`".to_string(),
    })?;

    let key_arr = partition
        .get("key")
        .and_then(|k| k.as_array())
        .ok_or_else(|| CanonicalError::Structure {
            path: path.to_path_buf(),
            line,
            msg: "`partition` missing array `key`".to_string(),
        })?;
    let key = key_arr.iter().map(CanonicalValue::from_json_key).collect();

    let deletion = partition.get("deletion_info").and_then(parse_deletion_info);

    let mut rows = Vec::new();
    let mut range_bounds = Vec::new();

    if let Some(arr) = v.get("rows").and_then(|r| r.as_array()) {
        for row in arr {
            let rtype = row.get("type").and_then(|t| t.as_str()).unwrap_or("");
            match rtype {
                "row" | "static_block" => rows.push(parse_row(row, rtype)),
                "range_tombstone_bound" | "range_tombstone_boundary" => {
                    range_bounds.push(parse_range_bound(row, rtype));
                }
                other => {
                    return Err(CanonicalError::Structure {
                        path: path.to_path_buf(),
                        line,
                        msg: format!("unrecognized row type {other:?}"),
                    });
                }
            }
        }
    }

    Ok(CanonicalPartition {
        key,
        deletion,
        rows,
        range_bounds,
    })
}

fn parse_row(row: &JsonValue, rtype: &str) -> CanonicalRow {
    let clustering = row
        .get("clustering")
        .and_then(|c| c.as_array())
        .map(|a| a.iter().map(CanonicalValue::from_json_key).collect())
        .unwrap_or_default();

    let liveness = row.get("liveness_info").map(|li| LivenessInfo {
        tstamp_micros: li
            .get("tstamp")
            .and_then(|s| s.as_str())
            .and_then(parse_timestamp_micros),
        ttl_secs: li.get("ttl").and_then(|t| t.as_i64()),
        expires_at_micros: li
            .get("expires_at")
            .and_then(|s| s.as_str())
            .and_then(parse_timestamp_micros),
    });

    let deletion = row.get("deletion_info").and_then(parse_deletion_info);

    let cells = row
        .get("cells")
        .and_then(|c| c.as_array())
        .map(|a| a.iter().map(parse_cell).collect())
        .unwrap_or_default();

    CanonicalRow {
        row_type: rtype.to_string(),
        clustering,
        liveness,
        deletion,
        cells,
    }
}

fn parse_cell(cell: &JsonValue) -> CanonicalCell {
    let name = cell
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or("")
        .to_string();

    let path = cell
        .get("path")
        .and_then(|p| p.as_array())
        .map(|a| a.iter().map(CanonicalValue::from_json).collect())
        .unwrap_or_default();

    // `Absent` (no value key) is distinct from JSON null.
    let value = match cell.get("value") {
        Some(v) => CanonicalValue::from_json(v),
        None => CanonicalValue::Absent,
    };

    let writetime_micros = cell
        .get("tstamp")
        .and_then(|s| s.as_str())
        .and_then(parse_timestamp_micros);

    let ttl_secs = cell.get("ttl").and_then(|t| t.as_i64());

    let deletion = cell.get("deletion_info").map(|di| {
        // A cell tombstone's markedForDeleteAt lives in the sibling `tstamp`
        // (the deletion_info block only carries local_delete_time).
        CellDeletion {
            marked_deleted_micros: writetime_micros.or_else(|| {
                di.get("marked_deleted")
                    .and_then(|s| s.as_str())
                    .and_then(parse_timestamp_micros)
            }),
            local_delete_secs: di
                .get("local_delete_time")
                .and_then(|s| s.as_str())
                .and_then(parse_timestamp_micros)
                .map(|us| us / 1_000_000),
        }
    });

    CanonicalCell {
        name,
        path,
        value,
        writetime_micros,
        ttl_secs,
        deletion,
    }
}

fn parse_range_bound(row: &JsonValue, rtype: &str) -> CanonicalRangeBound {
    let start = row.get("start").map(parse_bound_half);
    let end = row.get("end").map(parse_bound_half);
    // A simple `range_tombstone_bound` may carry the bound at the top level
    // rather than nested under start/end; cover that too.
    let (start, end) = if start.is_none() && end.is_none() {
        // Inspect the bound's own `type` to decide which side it is.
        let bt = row.get("type").and_then(|t| t.as_str()).unwrap_or("");
        let half = parse_bound_half(row);
        // Default to start side; sstabledump distinguishes via clustering.
        if bt.contains("end") {
            (None, Some(half))
        } else {
            (Some(half), None)
        }
    } else {
        (start, end)
    };

    CanonicalRangeBound {
        entry_type: rtype.to_string(),
        start,
        end,
    }
}

fn parse_bound_half(inner: &JsonValue) -> BoundHalf {
    let clustering = inner
        .get("clustering")
        .and_then(|c| c.as_array())
        .map(|a| {
            a.iter()
                .filter(|v| v.as_str() != Some("*"))
                .map(CanonicalValue::from_json_key)
                .collect()
        })
        .unwrap_or_default();

    let bound_type = inner.get("type").and_then(|t| t.as_str()).unwrap_or("");
    let inclusive = bound_type.contains("incl");

    let (marked_deleted_micros, local_delete_secs) = inner
        .get("deletion_info")
        .map(|di| {
            let md = di
                .get("marked_deleted")
                .and_then(|s| s.as_str())
                .and_then(parse_timestamp_micros);
            let ld = di
                .get("local_delete_time")
                .and_then(|s| s.as_str())
                .and_then(parse_timestamp_micros)
                .map(|us| us / 1_000_000);
            (md, ld)
        })
        .unwrap_or((None, None));

    BoundHalf {
        clustering,
        inclusive,
        marked_deleted_micros,
        local_delete_secs,
    }
}

fn parse_deletion_info(di: &JsonValue) -> Option<DeletionInfo> {
    let marked_deleted_micros = di
        .get("marked_deleted")
        .and_then(|s| s.as_str())
        .and_then(parse_timestamp_micros);
    let local_delete_secs = di
        .get("local_delete_time")
        .and_then(|s| s.as_str())
        .and_then(parse_timestamp_micros)
        .map(|us| us / 1_000_000);
    if marked_deleted_micros.is_none() && local_delete_secs.is_none() {
        return None;
    }
    Some(DeletionInfo {
        marked_deleted_micros,
        local_delete_secs,
    })
}

// ===========================================================================
// Diffing
// ===========================================================================

/// A single precise difference between expected (Cassandra) and actual (CQLite).
#[derive(Debug, Clone)]
pub struct Diff {
    /// Manifest id this comparison is gating (for failure attribution).
    pub manifest_id: String,
    /// Fixture path (the golden file or fixture dir) under comparison.
    pub fixture: PathBuf,
    /// Row key locator (partition key + clustering).
    pub row_key: String,
    /// Column path locator (`column[path...]` or `<partition>` / `<row>`).
    pub column_path: String,
    /// Human description of the divergence.
    pub what: String,
    /// Expected canonical rendering.
    pub expected: String,
    /// Actual canonical rendering.
    pub actual: String,
}

impl fmt::Display for Diff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DIFF [manifest={}] fixture={} row_key=[{}] column={}\n    {}\n    expected: {}\n    actual:   {}",
            self.manifest_id,
            self.fixture.display(),
            self.row_key,
            self.column_path,
            self.what,
            self.expected,
            self.actual,
        )
    }
}

/// Context carried through a comparison so every [`Diff`] is fully attributed.
pub struct CompareCtx {
    pub manifest_id: String,
    pub fixture: PathBuf,
}

impl CompareCtx {
    pub fn new(manifest_id: impl Into<String>, fixture: impl Into<PathBuf>) -> Self {
        Self {
            manifest_id: manifest_id.into(),
            fixture: fixture.into(),
        }
    }

    fn diff(
        &self,
        row_key: &str,
        column_path: &str,
        what: impl Into<String>,
        expected: impl Into<String>,
        actual: impl Into<String>,
    ) -> Diff {
        Diff {
            manifest_id: self.manifest_id.clone(),
            fixture: self.fixture.clone(),
            row_key: row_key.to_string(),
            column_path: column_path.to_string(),
            what: what.into(),
            expected: expected.into(),
            actual: actual.into(),
        }
    }
}

fn render_key(key: &[CanonicalValue]) -> String {
    key.iter()
        .map(|v| v.render())
        .collect::<Vec<_>>()
        .join(", ")
}

fn render_row_key(pk: &[CanonicalValue], clustering: &[CanonicalValue]) -> String {
    if clustering.is_empty() {
        format!("pk={}", render_key(pk))
    } else {
        format!("pk={} / ck={}", render_key(pk), render_key(clustering))
    }
}

/// Compare two canonical documents positionally. Returns ALL diffs found (empty
/// == parity). The comparison is strictly index-by-index for partitions, rows,
/// cells, paths, and range bounds; a length divergence at any level is reported
/// as a miss/over-emission at the exact index.
pub fn compare_documents(
    ctx: &CompareCtx,
    expected: &CanonicalDocument,
    actual: &CanonicalDocument,
) -> Vec<Diff> {
    let mut diffs = Vec::new();

    let common = expected.partitions.len().min(actual.partitions.len());
    for i in 0..common {
        compare_partition(ctx, &expected.partitions[i], &actual.partitions[i], &mut diffs);
    }
    for (i, exp) in expected.partitions.iter().enumerate().skip(actual.partitions.len()) {
        diffs.push(ctx.diff(
            &format!("pk={}", render_key(&exp.key)),
            &format!("<partition #{i}>"),
            "partition MISSING from actual (under-emission)",
            format!("partition with {} row(s)", exp.rows.len()),
            "<absent>",
        ));
    }
    for (i, act) in actual.partitions.iter().enumerate().skip(expected.partitions.len()) {
        diffs.push(ctx.diff(
            &format!("pk={}", render_key(&act.key)),
            &format!("<partition #{i}>"),
            "UNEXPECTED partition in actual (over-emission)",
            "<absent>",
            format!("partition with {} row(s)", act.rows.len()),
        ));
    }

    diffs
}

fn compare_partition(
    ctx: &CompareCtx,
    exp: &CanonicalPartition,
    act: &CanonicalPartition,
    diffs: &mut Vec<Diff>,
) {
    let rk = format!("pk={}", render_key(&exp.key));

    if exp.key != act.key {
        diffs.push(ctx.diff(
            &rk,
            "<partition key>",
            "partition key mismatch",
            render_key(&exp.key),
            render_key(&act.key),
        ));
    }

    if exp.deletion != act.deletion {
        diffs.push(ctx.diff(
            &rk,
            "<partition deletion>",
            "partition tombstone mismatch",
            format!("{:?}", exp.deletion),
            format!("{:?}", act.deletion),
        ));
    }

    // Rows: positional.
    let common = exp.rows.len().min(act.rows.len());
    for i in 0..common {
        compare_row(ctx, &exp.key, &exp.rows[i], &act.rows[i], diffs);
    }
    for (i, r) in exp.rows.iter().enumerate().skip(act.rows.len()) {
        diffs.push(ctx.diff(
            &render_row_key(&exp.key, &r.clustering),
            &format!("<row #{i}>"),
            "row MISSING from actual (under-emission)",
            format!("{} with {} cell(s)", r.row_type, r.cells.len()),
            "<absent>",
        ));
    }
    for (i, r) in act.rows.iter().enumerate().skip(exp.rows.len()) {
        diffs.push(ctx.diff(
            &render_row_key(&act.key, &r.clustering),
            &format!("<row #{i}>"),
            "UNEXPECTED row in actual (over-emission)",
            "<absent>",
            format!("{} with {} cell(s)", r.row_type, r.cells.len()),
        ));
    }

    // Range bounds: positional.
    let rb_common = exp.range_bounds.len().min(act.range_bounds.len());
    for i in 0..rb_common {
        if exp.range_bounds[i] != act.range_bounds[i] {
            diffs.push(ctx.diff(
                &rk,
                &format!("<range_bound #{i}>"),
                "range tombstone bound mismatch",
                format!("{:?}", exp.range_bounds[i]),
                format!("{:?}", act.range_bounds[i]),
            ));
        }
    }
    for (i, rb) in exp.range_bounds.iter().enumerate().skip(act.range_bounds.len()) {
        diffs.push(ctx.diff(
            &rk,
            &format!("<range_bound #{i}>"),
            "range bound MISSING from actual",
            format!("{rb:?}"),
            "<absent>",
        ));
    }
    for (i, rb) in act.range_bounds.iter().enumerate().skip(exp.range_bounds.len()) {
        diffs.push(ctx.diff(
            &rk,
            &format!("<range_bound #{i}>"),
            "UNEXPECTED range bound in actual",
            "<absent>",
            format!("{rb:?}"),
        ));
    }
}

fn compare_row(
    ctx: &CompareCtx,
    pk: &[CanonicalValue],
    exp: &CanonicalRow,
    act: &CanonicalRow,
    diffs: &mut Vec<Diff>,
) {
    let rk = render_row_key(pk, &exp.clustering);

    if exp.row_type != act.row_type {
        diffs.push(ctx.diff(&rk, "<row type>", "row type mismatch", &exp.row_type, &act.row_type));
    }
    if exp.clustering != act.clustering {
        diffs.push(ctx.diff(
            &rk,
            "<clustering>",
            "clustering key mismatch",
            render_key(&exp.clustering),
            render_key(&act.clustering),
        ));
    }
    if exp.liveness != act.liveness {
        diffs.push(ctx.diff(
            &rk,
            "<liveness>",
            "row liveness (writetime/ttl) mismatch",
            format!("{:?}", exp.liveness),
            format!("{:?}", act.liveness),
        ));
    }
    if exp.deletion != act.deletion {
        diffs.push(ctx.diff(
            &rk,
            "<row deletion>",
            "row tombstone mismatch",
            format!("{:?}", exp.deletion),
            format!("{:?}", act.deletion),
        ));
    }

    // Cells: positional.
    let common = exp.cells.len().min(act.cells.len());
    for i in 0..common {
        compare_cell(ctx, &rk, &exp.cells[i], &act.cells[i], diffs);
    }
    for (i, c) in exp.cells.iter().enumerate().skip(act.cells.len()) {
        diffs.push(ctx.diff(
            &rk,
            &cell_locator(c),
            format!("cell #{i} MISSING from actual (under-emission)"),
            c.value.render(),
            "<absent>",
        ));
    }
    for (i, c) in act.cells.iter().enumerate().skip(exp.cells.len()) {
        diffs.push(ctx.diff(
            &rk,
            &cell_locator(c),
            format!("UNEXPECTED cell #{i} in actual (over-emission)"),
            "<absent>",
            c.value.render(),
        ));
    }
}

fn cell_locator(c: &CanonicalCell) -> String {
    if c.path.is_empty() {
        c.name.clone()
    } else {
        format!("{}[{}]", c.name, render_key(&c.path))
    }
}

fn compare_cell(
    ctx: &CompareCtx,
    rk: &str,
    exp: &CanonicalCell,
    act: &CanonicalCell,
    diffs: &mut Vec<Diff>,
) {
    let loc = cell_locator(exp);

    if exp.name != act.name {
        diffs.push(ctx.diff(rk, &loc, "cell name mismatch", &exp.name, &act.name));
    }
    if exp.path != act.path {
        diffs.push(ctx.diff(
            rk,
            &loc,
            "cell path mismatch",
            render_key(&exp.path),
            render_key(&act.path),
        ));
    }
    if exp.value != act.value {
        diffs.push(ctx.diff(
            rk,
            &loc,
            "cell value (typed) mismatch",
            exp.value.render(),
            act.value.render(),
        ));
    }
    if exp.writetime_micros != act.writetime_micros {
        diffs.push(ctx.diff(
            rk,
            &loc,
            "cell writetime mismatch",
            format!("{:?}", exp.writetime_micros),
            format!("{:?}", act.writetime_micros),
        ));
    }
    if exp.ttl_secs != act.ttl_secs {
        diffs.push(ctx.diff(
            rk,
            &loc,
            "cell TTL mismatch",
            format!("{:?}", exp.ttl_secs),
            format!("{:?}", act.ttl_secs),
        ));
    }
    if exp.deletion != act.deletion {
        diffs.push(ctx.diff(
            rk,
            &loc,
            "cell deletion (tombstone/local-deletion-time) mismatch",
            format!("{:?}", exp.deletion),
            format!("{:?}", act.deletion),
        ));
    }
}

/// Format all diffs into one human-readable block for a panic message.
pub fn render_diffs(diffs: &[Diff]) -> String {
    let mut s = format!("{} parity difference(s):\n", diffs.len());
    for (i, d) in diffs.iter().enumerate() {
        s.push_str(&format!("  [{i}] {d}\n"));
    }
    s
}

// ===========================================================================
// Manifest-style report (manifest entry: .manifest_report_generation)
// ===========================================================================

/// A per-fixture comparison outcome for the manifest report.
#[derive(Debug, Clone)]
pub struct FixtureReport {
    pub manifest_id: String,
    pub fixture: PathBuf,
    pub partitions: usize,
    pub rows: usize,
    pub cells: usize,
    pub diff_count: usize,
}

/// Build a [`FixtureReport`] from a comparison run.
pub fn build_report(
    manifest_id: &str,
    fixture: &Path,
    doc: &CanonicalDocument,
    diffs: &[Diff],
) -> FixtureReport {
    let rows: usize = doc.partitions.iter().map(|p| p.rows.len()).sum();
    let cells: usize = doc
        .partitions
        .iter()
        .flat_map(|p| &p.rows)
        .map(|r| r.cells.len())
        .sum();
    FixtureReport {
        manifest_id: manifest_id.to_string(),
        fixture: fixture.to_path_buf(),
        partitions: doc.partitions.len(),
        rows,
        cells,
        diff_count: diffs.len(),
    }
}

/// Render a stable, sorted manifest report (markdown-ish) over many fixtures.
/// Sorting by manifest id then fixture path makes the output deterministic for
/// snapshotting (manifest entry `.manifest_report_generation`).
pub fn render_manifest_report(reports: &[FixtureReport]) -> String {
    let mut by_id: BTreeMap<&str, Vec<&FixtureReport>> = BTreeMap::new();
    for r in reports {
        by_id.entry(r.manifest_id.as_str()).or_default().push(r);
    }
    let mut out = String::from("# Canonical JSONL Comparator Report (#1009)\n\n");
    for (id, mut items) in by_id {
        items.sort_by(|a, b| a.fixture.cmp(&b.fixture));
        out.push_str(&format!("## {id}\n"));
        for r in items {
            let status = if r.diff_count == 0 { "MATCH" } else { "FAIL" };
            out.push_str(&format!(
                "- [{}] {} (partitions={}, rows={}, cells={}, diffs={})\n",
                status,
                r.fixture.display(),
                r.partitions,
                r.rows,
                r.cells,
                r.diff_count,
            ));
        }
        out.push('\n');
    }
    out
}

// ===========================================================================
// Fixture discovery helpers (shared so #1003/#1006/#1007/#1008 reuse them)
// ===========================================================================

/// Datasets root from `CQLITE_DATASETS_ROOT`, or `None` when unset/absent.
pub fn datasets_root() -> Option<PathBuf> {
    let p = PathBuf::from(std::env::var("CQLITE_DATASETS_ROOT").ok()?);
    p.exists().then_some(p)
}

/// The `*-Data.db.jsonl` golden inside a fixture directory, ignoring AppleDouble
/// (`._`) sidecars. `None` when none present.
pub fn find_golden_jsonl(dir: &Path) -> Option<PathBuf> {
    for entry in std::fs::read_dir(dir).ok()?.flatten() {
        let name = entry.file_name();
        let n = name.to_str().unwrap_or("");
        if n.ends_with("-Data.db.jsonl") && !n.starts_with("._") {
            return Some(entry.path());
        }
    }
    None
}

/// Locate the fixture directory for `keyspace.table` under the datasets root.
/// Returns `None` only when the datasets root is unset/absent; a present root
/// with no matching dir is a hard `Some(Err)`-style condition the caller asserts.
pub fn fixture_dir(keyspace: &str, table: &str) -> Option<PathBuf> {
    let root = datasets_root()?;
    let ks_dir = root.join("sstables").join(keyspace);
    let entries = std::fs::read_dir(&ks_dir).ok()?;
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let n = name.to_str().unwrap_or("");
        // Directory name is `<table>-<uuid>`; the table name precedes the LAST
        // hyphen group (uuid). Match on the `<table>-` prefix.
        if n == table || n.starts_with(&format!("{table}-")) {
            return Some(path);
        }
    }
    None
}
