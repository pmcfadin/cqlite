//! Helper functions for [`super::recover::salvage_sstable`] — opening the
//! reader, classifying a component-unreadable refusal, decoding + reconciling
//! ONE partition, building a [`Loss`], and the pre-write ordering check that
//! pre-empts the writer's own hard-Err rejection (design D1-D3). Split out
//! of `recover.rs` (round 15, campsite rule / epic #1116) when that file
//! crossed the ~800-line source threshold — a PURE MOVE, no behavior
//! changed by the split (this is a file THIS PR created, so the
//! `CQLITE_ALLOW_FILE_GROWTH=1` opt-out — reserved for pre-existing files —
//! does not apply to it; split rather than carried under an opt-out).

use super::boundaries::BoundaryEntry;
use super::{Loss, LossClass, Refusal, RefusalReason};
use crate::error::Result;
use crate::schema::TableSchema;
use crate::storage::partition_key_codec::decode_partition_key_columns;
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::{PartitionAtOffsetOutcome, SSTableReader};
use crate::storage::write_engine::merge::{
    KWayMerger, MergeEntry, MergeStep, SSTableRowIterator, SSTableRowIteratorAdapter,
};
use crate::storage::write_engine::mutation::{DecoratedKey, Mutation};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};

/// A run backed by a single already-decoded partition's `MergeEntry`s (the
/// salvage recovery loop's own source, one per partition, per run of
/// [`salvage_sstable`](super::recover::salvage_sstable)) — the same shape
/// the single-partition point-read path (`write_engine::merge::point_read`)
/// uses to feed [`KWayMerger::from_row_iterators`], so a healthy partition
/// reconciles through the IDENTICAL machinery `compact_sstables` uses
/// (design D1, R1).
pub(super) struct SinglePartitionRun {
    entries: VecDeque<MergeEntry>,
}

impl SSTableRowIterator for SinglePartitionRun {
    fn next(&mut self) -> Option<Result<MergeEntry>> {
        self.entries.pop_front().map(Ok)
    }
}

pub(super) async fn open_reader(input: &Path) -> Result<SSTableReader> {
    use crate::config::DiskAccessMode;
    use crate::platform::Platform;
    use crate::Config;
    use std::sync::Arc;

    let mut config = Config::default();
    config.storage.use_mmap = false;
    config.storage.disk_access_mode = DiskAccessMode::Buffered;
    let platform = Arc::new(Platform::new(&config).await?);
    SSTableReader::open(input, &config, platform).await
}

/// `input` names the `Data.db` file itself already (the salvage contract) —
/// this alias exists only so the chunk-preflight call sites read clearly.
pub(super) fn reader_data_path(input: &Path) -> PathBuf {
    input.to_path_buf()
}

/// Build a [`RefusalReason::ComponentUnreadable`] [`Refusal`] (roborev, issue
/// #4196, batched finding b) — `context` names WHAT was being attempted
/// (`"opening the input"`, `"reading the input's repair state
/// (Statistics.db)"`, ...) and `error` is the underlying failure's `Display`,
/// both folded into the remedy so an operator sees the actual cause rather
/// than a generic "refused" with no lead. Unlike
/// [`RefusalReason::BoundarySourceUnreadable`] this does NOT point at
/// `cqlite rebuild --components index` (#4197) — the boundary source was
/// fine here, so that remedy would send an operator at the wrong component;
/// `cqlite verify --mode full` is named instead, to let the operator
/// identify which component is actually damaged before deciding a next step.
pub(super) fn component_unreadable_refusal(
    context: &str,
    error: &dyn std::fmt::Display,
) -> Refusal {
    Refusal {
        reason: RefusalReason::ComponentUnreadable,
        remedy: format!(
            "component unreadable while {context}: {error} — run `cqlite verify --mode full` on \
             this input to identify the damaged component; salvage cannot proceed without it"
        ),
    }
}

/// Decode + reconcile ONE partition. `Ok(Some((key, mutations)))` on success,
/// `Ok(None)` when the partition decoded but reconciled to nothing to write,
/// `Err((class, rows_decoded_before_failure, message))` names a loss.
pub(super) async fn recover_one_partition(
    reader: &SSTableReader,
    entry: &BoundaryEntry,
    end_bound: Option<u64>,
    schema: &TableSchema,
    scan_cancel: &ScanCancel,
) -> std::result::Result<Option<(DecoratedKey, Vec<Mutation>)>, (LossClass, usize, String)> {
    let outcome = reader
        .decode_partition_at_offset_for_salvage(
            entry.data_offset,
            end_bound,
            entry.expected_key.as_deref(),
            Some(schema),
            scan_cancel,
        )
        .await
        .map_err(|e| (LossClass::Decode, 0, e.to_string()))?;

    let rows = match outcome {
        PartitionAtOffsetOutcome::Rows(rows) => rows,
        PartitionAtOffsetOutcome::KeyMismatch => {
            return Err((
                LossClass::KeyMismatch,
                0,
                "decoded key at this offset does not match the boundary source's key for this \
                 slot"
                    .to_string(),
            ));
        }
        PartitionAtOffsetOutcome::DecodeError {
            rows_decoded_before_failure,
            error,
        } => {
            return Err((
                LossClass::Decode,
                rows_decoded_before_failure,
                error.to_string(),
            ));
        }
        PartitionAtOffsetOutcome::Truncated => {
            return Err((
                LossClass::Truncated,
                0,
                "partition's authoritative byte range extends past Data.db's actual end"
                    .to_string(),
            ));
        }
        // roborev, issue #4196, round 17 Low finding: a DISTINCT message
        // from `Truncated`'s — that wording ("extends past Data.db's
        // actual end") is factually false here; the file is intact, and
        // the partition was refused ONLY because its span exceeds the
        // salvage tool's own 128 MiB plausible-partition ceiling
        // (`SALVAGE_MAX_PLAUSIBLE_PARTITION_BYTES`), a documented
        // trade-off, not evidence of damage. `LossClass` has no dedicated
        // variant for this (it is still, ultimately, "not recovered" —
        // `Truncated` is the closest existing class), but the MESSAGE now
        // names the real cause and the actual span width so an operator of
        // a genuinely wide, healthy partition is not told their data is
        // corrupt.
        PartitionAtOffsetOutcome::SpanTooWide { span_bytes } => {
            return Err((
                LossClass::Truncated,
                0,
                format!(
                    "partition's authoritative byte range is {span_bytes} bytes wide, exceeding \
                     this salvage tool's 128 MiB plausible-partition-span ceiling — Data.db \
                     itself is intact; this is a size-based refusal, not evidence of truncation \
                     or corruption"
                ),
            ));
        }
    };

    let mut merge_entries = Vec::with_capacity(rows.len());
    for (idx, row) in rows.into_iter().enumerate() {
        match SSTableRowIteratorAdapter::build_merge_entry(0, row, schema) {
            Ok(me) => merge_entries.push(me),
            Err(e) => return Err((LossClass::Decode, idx, e.to_string())),
        }
    }

    let run = SinglePartitionRun {
        entries: merge_entries.into(),
    };
    let mut merger = KWayMerger::from_row_iterators(vec![Box::new(run)], schema)
        .map_err(|e| (LossClass::Decode, 0, e.to_string()))?;
    let reconciled = merger
        .step()
        .map_err(|e| (LossClass::Decode, 0, e.to_string()))?;
    let (key, entries) = match reconciled {
        MergeStep::Partition { key, rows } => (key, rows),
        MergeStep::Complete => return Ok(None),
    };
    // roborev, issue #4196, round-7 Low finding: `step()` was previously
    // called exactly once and the merger dropped — every row here came from
    // ONE `decode_partition_at_offset_for_salvage` call for ONE boundary
    // slot, so this MUST drain to `Complete` on the next step. A second
    // `Partition` would mean the decoder fabricated rows spanning a
    // partition boundary (e.g. a corrupted `END_OF_PARTITION` marker whose
    // over-consumption still satisfied the compressed branch's `consumed <=
    // end - offset` bound), and silently dropping the merger here would
    // discard that second partition's rows from BOTH the output and the
    // loss manifest — the exact "every partition accounted for" contract
    // this tool exists to uphold.
    match merger.step() {
        Ok(MergeStep::Complete) => {}
        Ok(MergeStep::Partition { .. }) => {
            return Err((
                LossClass::Decode,
                entries.len(),
                "boundary slot decoded rows spanning more than one partition key".to_string(),
            ));
        }
        Err(e) => return Err((LossClass::Decode, entries.len(), e.to_string())),
    }
    if entries.is_empty() {
        return Ok(None);
    }

    let mut mutations = Vec::with_capacity(entries.len());
    for e in entries {
        let m = KWayMerger::merge_entry_to_mutation(e, schema)
            .map_err(|err| (LossClass::Decode, 0, err.to_string()))?;
        mutations.push(m);
    }
    Ok(Some((key, mutations)))
}

pub(super) fn build_loss(
    entry: &BoundaryEntry,
    schema: &TableSchema,
    chunks: Vec<u64>,
    class: LossClass,
    rows_decoded_before_failure: usize,
    message: String,
) -> Loss {
    // roborev, issue #4196: a BTI narrow (`DataOffset`) leaf carries no raw
    // key at all (see `BoundaryEntry::diagnostic_prefix`'s doc) — reporting
    // an empty `key_hex` there gave an operator nothing to locate the slot
    // by. Fall back to the trie's byte-comparable prefix, clearly labelled
    // as such (never presented as the raw key).
    if let Some(key_bytes) = &entry.expected_key {
        // roborev, issue #4196, round-6 Low finding: the Rust `Debug`
        // spelling of the decoded column vector is unstable across refactors
        // and not documented anywhere in the JSON manifest's contract — use
        // `Value`'s own stable `Display` rendering instead, comma-joined
        // `name=value` per column (matches the CLI's own row-value output).
        let key = decode_partition_key_columns(key_bytes, schema).ok().map(
            |cols: Vec<(String, crate::Value)>| {
                cols.iter()
                    .map(|(name, value)| format!("{name}={value}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            },
        );
        Loss {
            key_hex: hex::encode(key_bytes),
            key,
            data_offset: entry.data_offset,
            chunks,
            class,
            rows_decoded_before_failure,
            message,
        }
    } else if let Some(prefix) = &entry.diagnostic_prefix {
        Loss {
            key_hex: hex::encode(prefix),
            key: Some(
                "(BTI trie byte-comparable prefix — the raw key is not carried by the boundary \
                 source for this narrow partition; see Data.db at this offset)"
                    .to_string(),
            ),
            data_offset: entry.data_offset,
            chunks,
            class,
            rows_decoded_before_failure,
            message,
        }
    } else {
        Loss {
            key_hex: String::new(),
            key: None,
            data_offset: entry.data_offset,
            chunks,
            class,
            rows_decoded_before_failure,
            message,
        }
    }
}

/// `true` when `candidate` does not token-sort strictly after
/// `last_written` — mirrors `SSTableWriter::write_partition`'s own ordering
/// check (`writer/mod.rs`: `key.token <= last_token`) so the SAME violation
/// is caught HERE, before ever calling it, and classified as an ordinary
/// `Loss` instead of surfacing as a hard `Err` deep in the write path
/// (roborev, issue #4196, round-9 — three prior rounds independently
/// surfaced the hard-`Err` propagation this prevents). Extracted as a pure
/// function so the comparison is unit-testable directly: end-to-end, this
/// code path is reachable ONLY via a corrupted BTI narrow leaf (no
/// independent key to cross-check — every OTHER corruption class that could
/// produce an out-of-order token is already caught EARLIER, either by
/// `check_strictly_ascending` (offset monotonicity) or by
/// `decode_partition_at_offset_for_salvage`'s own `expected_key` cross-check
/// — see this module's `token_out_of_order` unit tests for the reasoning),
/// which is hard to construct as a real end-to-end fixture; declared here
/// rather than silently left untested.
pub(super) fn token_out_of_order(last_written: Option<i64>, candidate: i64) -> bool {
    last_written.is_some_and(|last| candidate <= last)
}

#[cfg(test)]
mod ordering_tests {
    use super::token_out_of_order;

    /// The common, healthy case: the first partition ever written has
    /// nothing to compare against.
    #[test]
    fn first_partition_is_never_out_of_order() {
        assert!(!token_out_of_order(None, i64::MIN));
        assert!(!token_out_of_order(None, 0));
        assert!(!token_out_of_order(None, i64::MAX));
    }

    /// A strictly-increasing token sequence — the normal case for every
    /// partition after the first — never flags.
    #[test]
    fn strictly_increasing_tokens_pass() {
        assert!(!token_out_of_order(Some(-100), -50));
        assert!(!token_out_of_order(Some(0), 1));
        assert!(!token_out_of_order(Some(i64::MIN), i64::MAX));
    }

    /// A DUPLICATE token — `SSTableWriter::write_partition` rejects `<=`,
    /// not just `<`, so a repeat must flag too (two boundary entries naming
    /// the same effective token, e.g. a Murmur3 hash collision on two
    /// distinct keys — Cassandra's own token-order writer would never
    /// legitimately produce this for the SAME table without an intervening
    /// key, so seeing it here IS the corruption signal).
    #[test]
    fn duplicate_token_is_out_of_order() {
        assert!(token_out_of_order(Some(42), 42));
    }

    /// A DECREASING token — the exact scenario this fix exists for: an
    /// offset-ascending, individually-key-matching boundary source (so
    /// NEITHER `check_strictly_ascending` NOR the per-entry key cross-check
    /// catches it) whose corrupted narrow leaf nonetheless decodes a token
    /// that sorts BEFORE what was already written.
    #[test]
    fn decreasing_token_is_out_of_order() {
        assert!(token_out_of_order(Some(1000), 999));
        assert!(token_out_of_order(Some(0), i64::MIN));
    }
}
