//! Promoted-index (`IndexInfo`) `ClusteringPrefix` serialization (Issue #1186).
//!
//! Extracted verbatim from `encoding.rs` to keep that file under the campsite-rule
//! file-size threshold (epic #1116). No emitted bytes change. `use super::*`
//! provides the crate imports, the bound-kind constants (`INCL_START_BOUND` etc.,
//! private to `mod.rs`), and the sibling helper `serialize_clustering_prefix_to_vec`
//! (re-exported from `encoding.rs`).
//!
//! Cassandra serializes a promoted-index `firstName`/`lastName` via
//! `ClusteringPrefix.serializer.serialize` (`ClusteringPrefix.java:462-475`), which
//! dispatches on the prefix kind: a row clustering goes through
//! `Clustering.serializer` (`[kind][values-without-size]`); a range-tombstone marker
//! bound goes through `ClusteringBoundOrBoundary.serializer`
//! (`[kind][u16 size][values-without-size]`). The two forms are NOT interchangeable —
//! only the bound form carries the `writeShort(size)`.

use super::*;

/// `ClusteringPrefix.Kind.CLUSTERING.ordinal()` in Cassandra 5.0
/// (`org.apache.cassandra.db.ClusteringPrefix.Kind`). A row's full clustering key
/// (a promoted-index `firstName`/`lastName`) is always kind `CLUSTERING`; range
/// bounds use the other ordinals (e.g. `EXCL_END_INCL_START_BOUNDARY = 2`,
/// `INCL_END_EXCL_START_BOUNDARY = 5`).
pub(crate) const CLUSTERING_PREFIX_KIND_CLUSTERING: u8 = 4;

/// Serialize a `ClusteringKey` as the promoted-index (`IndexInfo`) `ClusteringPrefix`
/// byte sequence (Issue #1186).
///
/// This is **NOT** the same as the Data.db row clustering prefix
/// ([`serialize_clustering_prefix_to_vec`]). Cassandra serializes a Data.db row's
/// clustering via the values-only `Clustering.serializer` (no kind byte), but it
/// serializes a promoted-index `firstName`/`lastName` via
/// `ClusteringPrefix.serializer.serialize`, which prepends a **leading kind byte**
/// (`Kind.ordinal()`). For a full clustering key that kind is always `CLUSTERING`
/// (`= 4`). Format:
///
/// ```text
/// [kind: 1 byte = 0x04 (CLUSTERING)]
/// [header: unsigned VInt]            ← 2 bits per column: 00=present, 10=null
/// [value bytes…]                     ← type-specific bytes for each PRESENT column
/// ```
///
/// For a single `int` clustering this is the Cassandra-exact 6 bytes
/// `04 00 <4-byte big-endian int>`, matching the real
/// `test_big.wide_partition` `Index.db` fixture (verified byte-for-byte).
///
/// Returns `Err` if a clustering column type is unknown (the caller falls back to
/// `[kind, 0x00]` — an empty `Clustering` — in that case).
pub(super) fn serialize_clustering_prefix_for_index(
    clustering_key: &ClusteringKey,
    schema: &TableSchema,
) -> Result<Vec<u8>> {
    let values = serialize_clustering_prefix_to_vec(clustering_key, schema)?;
    let mut buf = Vec::with_capacity(values.len() + 1);
    buf.push(CLUSTERING_PREFIX_KIND_CLUSTERING);
    buf.extend_from_slice(&values);
    Ok(buf)
}

/// The empty-clustering promoted-index `ClusteringPrefix`: a `Clustering` of kind
/// `CLUSTERING` with no columns (Issue #1186). Used for no-clustering rows and
/// range-bound fallbacks where no per-row clustering values are available. Equals
/// `[0x04 (CLUSTERING)][0x00 (empty values header)]`.
pub(super) fn empty_clustering_prefix_for_index() -> Vec<u8> {
    vec![CLUSTERING_PREFIX_KIND_CLUSTERING, 0x00]
}

/// Serialize a range-tombstone **marker** bound as its promoted-index
/// (`IndexInfo`) `ClusteringPrefix` byte sequence (Issue #1186 roborev MEDIUM).
///
/// A marker name uses its **actual bound kind** ordinal, NOT `CLUSTERING`, via
/// Cassandra's `ClusteringBoundOrBoundary.Serializer.serialize`
/// (`ClusteringBoundOrBoundary.java:103-108`) — a **different wire format** from
/// `Clustering.serializer`: kind byte, then a 2-byte big-endian `size` short, then
/// the values-without-size blob, and NOTHING after the size short when `size == 0`
/// (`serializeValuesWithoutSize` is a no-op for an empty bound):
///
/// ```text
/// [kind: 1 byte]   ← INCL_START_BOUND=1 / EXCL_END_BOUND=0 / INCL_END_BOUND=6 / EXCL_START_BOUND=7
/// [size: u16 BE]   ← bound.size() (number of clustering values)
/// [header: VInt]   ← only when size > 0; 2 bits per column: 00=present, 10=null
/// [value bytes…]   ← only when size > 0; type-specific bytes for each PRESENT column
/// ```
///
/// This mirrors the on-disk marker bytes ([`DataWriter::write_range_bound`]) minus
/// the leading `IS_MARKER` flag. An open-ended bound (`Bottom`/`Top`, size 0) is
/// therefore the 3 bytes `[kind][00][00]` — NOT `[kind]` and NOT `[kind][00]`.
/// (Issue #1186 roborev MEDIUM: the pre-fix code emitted the `Clustering.serializer`
/// form — a single `0x00` for empty bounds, no size short for non-empty — never the
/// bound form.) Returns `Err` only if a clustering column type is unknown (the
/// caller falls back via [`marker_bound_prefix_for_index`]).
pub(super) fn serialize_marker_bound_prefix_for_index(
    bound: &ClusteringBound,
    is_open: bool,
    schema: &TableSchema,
) -> Result<Vec<u8>> {
    let (kind, clustering) = marker_bound_kind(bound, is_open);
    let mut buf = Vec::new();
    buf.push(kind);
    match clustering {
        Some(ck) => {
            let size = ck.columns.len();
            if size > u16::MAX as usize {
                return Err(crate::error::Error::InvalidInput(format!(
                    "Range tombstone bound has too many clustering values: {size}"
                )));
            }
            // 2-byte BE size short, then the values-without-size blob.
            buf.extend_from_slice(&(size as u16).to_be_bytes());
            buf.extend_from_slice(&serialize_clustering_prefix_to_vec(ck, schema)?);
        }
        // Empty bound (Bottom/Top): size short = 0, no header/values follow.
        None => buf.extend_from_slice(&0u16.to_be_bytes()),
    }
    Ok(buf)
}

/// The bound-kind-aware empty-prefix fallback for a marker (Issue #1186): the
/// marker's correct `Kind.ordinal()` byte followed by a 2-byte big-endian `size`
/// short of `0` (an empty `ClusteringBound`), matching the bound serializer's
/// no-values form. Used when the marker's clustering values cannot be encoded.
pub(super) fn marker_bound_prefix_for_index(bound: &ClusteringBound, is_open: bool) -> Vec<u8> {
    let (kind, _) = marker_bound_kind(bound, is_open);
    let mut buf = Vec::with_capacity(3);
    buf.push(kind);
    buf.extend_from_slice(&0u16.to_be_bytes());
    buf
}

/// Serialize a range-tombstone **boundary** marker as its promoted-index
/// (`IndexInfo`) `ClusteringPrefix` byte sequence (issue #1220). A boundary uses
/// the same `ClusteringBoundOrBoundary.Serializer` wire form as a bound — a kind
/// byte (2 or 5), a 2-byte big-endian `size` short, then the values-without-size
/// blob — but its kind is the BOUNDARY ordinal and it always carries a concrete
/// clustering value (never the empty `Bottom`/`Top` form). Returns `Err` only if a
/// clustering column type is unknown (the caller falls back via
/// [`boundary_prefix_for_index_fallback`]).
pub(super) fn serialize_boundary_prefix_for_index(
    boundary_kind: u8,
    clustering: &ClusteringKey,
    schema: &TableSchema,
) -> Result<Vec<u8>> {
    let size = clustering.columns.len();
    if size > u16::MAX as usize {
        return Err(crate::error::Error::InvalidInput(format!(
            "Range tombstone boundary has too many clustering values: {size}"
        )));
    }
    let mut buf = Vec::new();
    buf.push(boundary_kind);
    buf.extend_from_slice(&(size as u16).to_be_bytes());
    buf.extend_from_slice(&serialize_clustering_prefix_to_vec(clustering, schema)?);
    Ok(buf)
}

/// Fallback promoted-index prefix for a boundary whose clustering values cannot be
/// encoded: the boundary kind byte followed by a `0` size short (an empty bound
/// form). Mirrors [`marker_bound_prefix_for_index`] for the boundary case.
pub(super) fn boundary_prefix_for_index_fallback(boundary_kind: u8) -> Vec<u8> {
    let mut buf = Vec::with_capacity(3);
    buf.push(boundary_kind);
    buf.extend_from_slice(&0u16.to_be_bytes());
    buf
}

/// Select the `ClusteringPrefix.Kind` ordinal and clustering values for a marker
/// bound — the SINGLE source of truth shared by the serializer and the fallback.
///
/// Mirrors the `(is_open, bound)` match in [`DataWriter::write_range_bound`]
/// exactly so the promoted-index kind byte equals the on-disk marker kind byte.
fn marker_bound_kind(bound: &ClusteringBound, is_open: bool) -> (u8, Option<&ClusteringKey>) {
    match (is_open, bound) {
        (true, ClusteringBound::Inclusive(ck)) => (INCL_START_BOUND, Some(ck)),
        (true, ClusteringBound::Exclusive(ck)) => (EXCL_START_BOUND, Some(ck)),
        (false, ClusteringBound::Inclusive(ck)) => (INCL_END_BOUND, Some(ck)),
        (false, ClusteringBound::Exclusive(ck)) => (EXCL_END_BOUND, Some(ck)),
        (true, ClusteringBound::Bottom | ClusteringBound::Top) => (INCL_START_BOUND, None),
        (false, ClusteringBound::Bottom | ClusteringBound::Top) => (INCL_END_BOUND, None),
    }
}

// Promoted-index marker-bound width parity tests (Issue #1186 roborev MEDIUM).
// Kept in a sibling file (private-access unit tests) so they do not grow this
// source file — split doctrine: epic #1116.
#[cfg(test)]
#[path = "encoding_marker_bound_tests.rs"]
mod marker_bound_prefix_tests;
