//! The two refusals the decode passes raise (design D3, spec R2/R5.2), split out of `components.rs`
//! under the file-size ratchet (#1116).

use super::{FieldProvenance, Refusal, RefusalReason};

pub(super) fn data_corrupt_refusal(detail: impl std::fmt::Display, offset: Option<u64>) -> Refusal {
    Refusal {
        reason: RefusalReason::DataCorrupt,
        remedy: format!(
            "Data.db cannot be trusted as the source of truth: {detail} — remedy: cqlite \
             salvage (issue #4196) to recover a fresh generation from what is still decodable"
        ),
        offset,
    }
}

/// Refuse because re-encoding a partition did not reproduce its ACTUAL
/// on-disk byte span (issue #4197 spec R2), so every promoted-index offset
/// derived from that re-encode is untrustworthy.
///
/// This is deliberately NOT [`RefusalReason::DataCorrupt`]: `Data.db` decoded
/// cleanly and may well be perfectly healthy — what failed is rebuild's
/// ability to REPRODUCE its encoding, overwhelmingly because the
/// whole-SSTable `EncodingStats` baseline could not be recovered from the
/// original `Statistics.db` (see the module doc). Reporting corruption for a
/// healthy file would send the operator to `salvage` for nothing.
pub(super) fn reencode_mismatch_refusal(
    offset: u64,
    on_disk_span: u64,
    reencoded_span: u64,
    baseline_provenance: FieldProvenance,
) -> Refusal {
    Refusal {
        reason: RefusalReason::ReencodeMismatch,
        remedy: format!(
            "the partition at Data.db offset {offset} occupies {on_disk_span} bytes on disk but \
             re-encodes to {reencoded_span} bytes, so every promoted-index offset derived from \
             it would be wrong (EncodingStats baseline provenance: {}) — remedy: restore this \
             generation's original Statistics.db (its SerializationHeader carries the \
             authoritative delta-encoding baseline) and re-run; if it is gone for good, cqlite \
             salvage (issue #4196) can write a fresh, self-consistent generation instead",
            baseline_provenance.manifest_label()
        ),
        offset: Some(offset),
    }
}
