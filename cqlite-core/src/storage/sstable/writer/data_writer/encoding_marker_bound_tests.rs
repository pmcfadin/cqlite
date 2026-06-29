//! Issue #1186 roborev MEDIUM — promoted-index marker bound width parity.
//!
//! Oracle: Cassandra `ClusteringBoundOrBoundary.Serializer.serialize`
//! (`ClusteringBoundOrBoundary.java:103-108`) writes
//! `[kind.ordinal()][writeShort(size)][serializeValuesWithoutSize]`, and
//! `serializeValuesWithoutSize` writes NOTHING for `size == 0`. So an open-ended
//! bound (Bottom/Top, size 0) is exactly the 3 bytes `[kind][00][00]`. The pre-fix
//! writer emitted `[kind][00]` (2 bytes) — one byte too few, which misframes the
//! following IndexInfo fields. FAILS-before / PASSES-after.
//!
//! Lives in its own file (included via `#[path]` from `encoding.rs`) so the
//! private-access unit tests don't grow the over-threshold source file (epic #1116).

use super::*;
use crate::schema::{ClusteringColumn, ClusteringOrder, TableSchema};
use std::collections::HashMap;

fn int_ck_schema() -> TableSchema {
    TableSchema {
        keyspace: "ks".into(),
        table: "t".into(),
        partition_keys: vec![],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".into(),
            data_type: "int".into(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// An open-ended (Bottom) START bound: kind INCL_START_BOUND (1) + a 2-byte
/// size short of 0, and NOTHING else. Must be EXACTLY 3 bytes `01 00 00`.
#[test]
fn open_start_bound_is_kind_plus_two_byte_zero_size() {
    let schema = int_ck_schema();
    let bytes = serialize_marker_bound_prefix_for_index(&ClusteringBound::Bottom, true, &schema)
        .expect("empty bound encodes without consulting column types");
    assert_eq!(
        bytes,
        vec![INCL_START_BOUND, 0x00, 0x00],
        "open-ended START bound must be [kind=1][size=0x0000] (3 bytes), \
         matching ClusteringBoundOrBoundary.serializer; got {bytes:02x?}"
    );
    // Regression guard against the pre-fix 2-byte `[kind][00]` form.
    assert_eq!(bytes.len(), 3, "must be 3 bytes, not the pre-fix 2");
}

/// An open-ended (Top) END bound: kind INCL_END_BOUND (6) + size short 0.
#[test]
fn open_end_bound_is_kind_plus_two_byte_zero_size() {
    let schema = int_ck_schema();
    let bytes = serialize_marker_bound_prefix_for_index(&ClusteringBound::Top, false, &schema)
        .expect("empty bound encodes without consulting column types");
    assert_eq!(bytes, vec![INCL_END_BOUND, 0x00, 0x00]);
}

/// The error-fallback path must also emit the 3-byte empty-bound form.
#[test]
fn fallback_empty_bound_is_three_bytes() {
    assert_eq!(
        marker_bound_prefix_for_index(&ClusteringBound::Bottom, true),
        vec![INCL_START_BOUND, 0x00, 0x00]
    );
    assert_eq!(
        marker_bound_prefix_for_index(&ClusteringBound::Top, false),
        vec![INCL_END_BOUND, 0x00, 0x00]
    );
}

/// A non-empty single-`int` inclusive-START bound carries the 2-byte size short
/// BEFORE the values-without-size blob: `[01][00 01][header 00][int 4B]` = 8B.
#[test]
fn nonempty_int_bound_includes_two_byte_size_short() {
    let schema = int_ck_schema();
    let ck = ClusteringKey::single("ck", Value::Integer(0));
    let bytes =
        serialize_marker_bound_prefix_for_index(&ClusteringBound::Inclusive(ck), true, &schema)
            .expect("int bound encodes");
    assert_eq!(
        bytes,
        vec![INCL_START_BOUND, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00],
        "single-int START bound must be [kind][size=1][header][int] (8 bytes) \
         — NOT the 6-byte Clustering.serializer form missing the size short; got {bytes:02x?}"
    );
}
