//! Invariant 7 (CompressionInfo chunk table covers `Data.db` exactly) and
//! invariant 8 (`Statistics.db` agrees with a real scan; `--deep` only).

use super::{skip, AuditVerdict};
use crate::parser::header::CassandraVersion;
use crate::platform::Platform;
use crate::storage::sstable::chunk_decompressor::ChunkDecompressor;
use crate::storage::sstable::statistics_reader::StatisticsReader;
use crate::storage::sstable::verify::{CompressionState, VerifyErrorClass, VerifyFinding};
use std::path::Path;
use std::sync::Arc;

fn coverage_gap(detail: String) -> VerifyFinding {
    VerifyFinding::new(
        VerifyErrorClass::CompressionCoverageGap,
        "CompressionInfo.db",
        detail,
    )
}

/// Invariant 7. `data_length` is the LOGICAL (uncompressed) size and the chunk
/// count Cassandra writes is `ceil(data_length / chunk_length)`
/// (cassandra-5.0.8 `CompressionMetadata.Writer.open`, lines 447-449:
/// `dataLength / chunkLength`, plus one when the remainder is non-zero).
/// The final chunk must also decode to exactly the remaining logical bytes.
pub(super) fn check_compression_coverage(
    state: &CompressionState,
    data_path: &Path,
    findings: &mut Vec<VerifyFinding>,
) -> AuditVerdict {
    let info = match state {
        CompressionState::Uncompressed => return skip("no CompressionInfo: uncompressed"),
        CompressionState::Unreadable => return skip(super::BOUNDARY_SOURCE_UNREADABLE),
        CompressionState::Compressed(info) => info,
    };
    let chunk_len = u64::from(info.chunk_length);
    if chunk_len == 0 {
        return skip("CompressionInfo.db declares chunk_length 0; coverage is not computable");
    }
    let expected = info.data_length.div_ceil(chunk_len);
    let declared = info.chunk_offsets.len() as u64;
    if declared != expected {
        findings.push(coverage_gap(format!(
            "CompressionInfo.db declares {declared} chunk(s), but data_length {} with chunk_length \
             {} implies {expected}",
            info.data_length, info.chunk_length
        )));
        return AuditVerdict::Fail;
    }
    if expected == 0 {
        return AuditVerdict::Pass;
    }
    // Last chunk: its decoded length must be the logical remainder.
    let last = (expected - 1) as usize;
    let want = info
        .data_length
        .saturating_sub(chunk_len.saturating_mul(expected - 1));
    let decoded = (|| -> Result<usize, String> {
        let mut file = std::fs::File::open(data_path).map_err(|e| e.to_string())?;
        let mut dec = ChunkDecompressor::new(info.clone(), CassandraVersion::V5_0Release)
            .map_err(|e| e.to_string())?;
        dec.decompress_chunk_by_index(&mut file, last)
            .map(|v| v.len())
            .map_err(|e| e.to_string())
    })();
    match decoded {
        Ok(n) if n as u64 == want => AuditVerdict::Pass,
        Ok(n) => {
            findings.push(coverage_gap(format!(
                "the last chunk (index {last}) decodes to {n} byte(s), but data_length {} implies \
                 {want}",
                info.data_length
            )));
            AuditVerdict::Fail
        }
        Err(why) => {
            findings.push(coverage_gap(format!(
                "the last chunk (index {last}) cannot be decoded, so the chunk table's coverage of \
                 Data.db is not established: {why}"
            )));
            AuditVerdict::Fail
        }
    }
}

/// Invariant 8, `partition_count` half. The declared count is the sum of the
/// `estimatedPartitionSize` histogram buckets (exact: one entry per partition).
/// A declared `0` is not distinguishable from "not decodable" in the parser's
/// contract, so it is unmeasured, never compared. The timestamp / local-deletion
/// half is a DECLARED GAP (see `specs/verify-audit/spec.md` A8).
pub(super) async fn check_statistics_vs_scan(
    stats_path: &Path,
    deep: bool,
    observed: Option<&Result<usize, String>>,
    platform: &Arc<Platform>,
    findings: &mut Vec<VerifyFinding>,
) -> AuditVerdict {
    if !deep {
        return skip("requires --deep");
    }
    if !stats_path.exists() {
        return skip("no Statistics.db");
    }
    let observed = match observed {
        Some(Ok(n)) => *n as u64,
        Some(Err(why)) => return skip(format!("scan failed: {why}")),
        None => return skip("no scan result"),
    };
    let declared = match StatisticsReader::open(stats_path, platform.clone()).await {
        Ok(r) => r.statistics().row_stats.partition_count,
        Err(e) => return skip(format!("Statistics.db unreadable: {e}")),
    };
    if declared == 0 {
        return skip("Statistics.db partition_count is not authoritatively decodable (reads 0)");
    }
    if declared == observed {
        return AuditVerdict::Pass;
    }
    findings.push(VerifyFinding::new(
        VerifyErrorClass::StatisticsScanMismatch,
        "Statistics.db",
        format!(
            "Statistics.db declares {declared} partition(s), but a full scan observed {observed}"
        ),
    ));
    AuditVerdict::Fail
}
