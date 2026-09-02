//! Bounding a single-partition seek's parse input to the target partition's
//! AUTHORITATIVE byte extent (issue #3890; campsite #1116 split of `bti_point.rs`).
//!
//! `bti_decompress_and_parse_target_all` buffers whole COMPRESSION CHUNKS, so the
//! window it hands to the parser routinely overruns the target partition's end by
//! up to one chunk. The parser it calls (`parse_block_emit_windowed`) is a
//! MULTI-partition block walker, so those overrun bytes were walked as if they
//! were more partitions — and because the overrun is by construction a partition
//! truncated at an arbitrary chunk boundary, its first row does not fit the
//! window: the row loop breaks WITHOUT emitting, the outer partition loop advances,
//! and the successor partition's ROW BODY bytes are re-read AS A PARTITION HEADER.
//! Cell decode then fails deep inside garbage (`invalid cell flags 0x37`,
//! `0x32`, …) at an offset inside the SUCCESSOR partition — a real error, reported
//! against the target partition's read, whose only reason for being benign today is
//! that the row loop's `Err` arm swallows it.
//!
//! Cassandra has no such state by construction. `UnfilteredSerializer`
//! (`cassandra-5.0.8`, `src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java:102`)
//! declares `END_OF_PARTITION = 0x01 // Signal the end of the partition. Nothing
//! follows a <flags> field with that flag.`, and `deserializeOne` returns `null` the
//! moment it reads those flags; `SSTableSimpleIterator.CurrentFormatIterator.computeNext()`
//! turns that `null` into `endOfData()`, and `AbstractSSTableIterator` drives its
//! `hasNext` off exactly that deserializer after seeking to `indexEntry.position`.
//! A Cassandra single-partition read therefore has NO concept of a "next
//! partition": it stops at `END_OF_PARTITION` and never reads a successor
//! partition header, so a truncated successor is unreachable.
//!
//! This module restores that property on the CQLite side by slicing the parser's
//! input at the partition's authoritative exclusive end before the walk begins,
//! so there is nothing past the partition for the walker to misread.

use super::super::SSTableReader;
use super::model::table_header_consistent_for_seek;
use crate::storage::sstable::reader::parsing::BufferExtent;
use crate::types::{ScanRow, TableId};
use crate::{Error, Result, RowKey};

impl SSTableReader {
    /// Parse the buffered `window` from `within`, collecting every row of the
    /// target partition.
    ///
    /// Returns `(rows, saw_next_partition)`:
    /// - `rows` — the target partition's row `Value`s (those whose decoded key
    ///   equals `key` and whose table id matches, issue #831 wrong-table guard),
    ///   in on-disk order.
    /// - `saw_next_partition` — `true` iff the parser emitted a fully-decoded row
    ///   whose partition key DIFFERS from `key`, at which point collection stops.
    ///
    /// # The parse input is bounded by the partition's authoritative extent (#3890)
    ///
    /// `partition_end_within` is the AUTHORITATIVE exclusive end of the target
    /// partition expressed as an index into `window` (the successor partition's
    /// start from the BTI trie / BIG index, else `CompressionInfo.data_length`) —
    /// never a value inferred from byte patterns (#28). Everything at or past it
    /// belongs to another partition and is sliced off BEFORE the multi-partition
    /// block walker runs, so the walker cannot reach it. `None` means the extent is
    /// genuinely unknown (the LAST partition of an SSTable with no usable
    /// `CompressionInfo`); no bound is then invented and the whole window is
    /// parsed, exactly as before.
    ///
    /// The bound is clamped to `window.len()`, which is a real case in BOTH
    /// directions: EOF can leave the window SHORTER than the extent (a last
    /// partition's `data_length` bound never reads past EOF), and an engaged #954
    /// clustering-slice narrowing deliberately buffers LESS than the whole
    /// partition (there the clamp makes the bound a no-op and the row loop's
    /// `row_body_window` end stays the operative stop).
    ///
    /// A bound at or below `within` is CORRUPTION, not an empty result: it means
    /// the index's successor offset contradicts the resolved partition offset. It
    /// is reported as a named [`Error::corruption`] rather than silently yielding
    /// zero rows for a present partition.
    ///
    /// # Why the `Break`-on-different-key callback stays
    ///
    /// It is now genuinely defence in depth. The comment it replaces claimed it
    /// already made the chunk overrun harmless; that was wrong, and the way it was
    /// wrong is worth keeping written down: the callback fires only from a row that
    /// FULLY DECODES, while the overrun region is by construction a partition
    /// truncated at an arbitrary chunk boundary — so on the failing fixtures no
    /// callback ever fired, nothing broke out, and the walker went on to misread
    /// row-body bytes as a partition header. With the input sliced at the extent
    /// the callback has nothing left to catch on those fixtures, so it is retained
    /// only for the case where a future caller passes `None` (unknown extent).
    ///
    /// # Clustering-slice window (#954)
    ///
    /// When `row_body_window` is `Some((start_rel, end_rel))` the parse is bounded
    /// to that within-partition byte window (relative to the partition start, i.e.
    /// the `window[within..]` slice domain) so only the clustering slice's
    /// row-index block(s) are decoded. `None` parses the whole partition (the #953
    /// behaviour).
    #[cfg(not(feature = "tombstones"))]
    pub(super) fn bti_collect_partition_rows(
        &self,
        window: &[u8],
        within: usize,
        partition_end_within: Option<usize>,
        row_body_window: Option<(usize, usize)>,
        key: &RowKey,
        table_id: &TableId,
        // Whether the manager resolved this reader by an EXACT fully-qualified
        // `keyspace.table` match (or an unqualified query). When `false` a
        // fully-qualified query reached this reader via the bare-name fallback, so
        // the seek guard keeps STRICT keyspace matching (#1284 review).
        fully_qualified_match: bool,
        schema_opt: Option<&crate::schema::TableSchema>,
        parser: &crate::storage::sstable::reader::parsing::V5CompressedLegacyParser,
    ) -> Result<(Vec<ScanRow>, bool)> {
        let parse_end = partition_end_within.map_or(window.len(), |end| end.min(window.len()));
        if parse_end <= within {
            return Err(Error::corruption(format!(
                "BTI single-partition seek: the target partition's authoritative end ({}, \
                 clamped to {}) does not exceed its start ({}) in a {}-byte window — the \
                 index's successor offset contradicts the resolved partition offset",
                partition_end_within.map_or_else(|| "window end".to_string(), |e| e.to_string()),
                parse_end,
                within,
                window.len()
            )));
        }
        // Issue #3890: the parser sees ONLY the target partition's bytes. Nothing
        // past `parse_end` is another partition's, truncated or otherwise.
        let partition_bytes = &window[within..parse_end];

        let mut rows: Vec<ScanRow> = Vec::new();
        let mut saw_next_partition = false;
        // Clamp the clustering window's end to the available bytes (`usize::MAX`
        // means "to the partition end"); the start is already
        // within-partition-relative, which is the same domain as `partition_bytes`.
        let clamped_window = row_body_window.map(|(start, end)| {
            let avail = partition_bytes.len();
            (start.min(avail), end.min(avail))
        });
        // #3782 x #3890: `Complete`, NOT `Window`. #3782 classifies this reader's
        // buffer as a chunk-covering WINDOW whose truncated tail is a straddle
        // signal — true on `main`, and THIS change removes the fact it rests on.
        // `partition_bytes` is sliced at the partition's authoritative extent
        // above, so it is exactly what `BufferExtent::Complete` documents: "a
        // partition slice already proven fully consumed". No further bytes can
        // arrive to finish a row (the caller pulls every covering chunk BEFORE
        // this call), so a decode failure in here is truncation/corruption —
        // DATA LOSS — and must be reported, not tolerated. Keeping `Window` here
        // would leave a tolerant tail on a buffer with no continuation, i.e. keep
        // silently swallowing the very truncation #3721/#3782 exist to surface.
        // The straddle protocol keeps `Window` at its real site, the chunk-pull
        // loop in `bti_decompress_and_parse_target_all`.
        parser.parse_block_emit_windowed(
            partition_bytes,
            BufferExtent::Complete,
            schema_opt,
            self,
            clamped_window,
            |(tid, entry_key, entry_value)| {
                if entry_key.as_bytes() == key.as_bytes() {
                    // Header-authoritative table consistency: wrong-table rejected
                    // (#831); a keyspace-divergent same-table query is served ONLY
                    // when resolution was an exact fully-qualified match — a
                    // fallback-resolved query keeps strict keyspace matching so it
                    // never returns another keyspace's same-named rows (#1284).
                    if table_header_consistent_for_seek(&tid, table_id, fully_qualified_match) {
                        rows.push(entry_value);
                    }
                    Ok(std::ops::ControlFlow::Continue(()))
                } else {
                    // Defence in depth only (see the doc comment): with the input
                    // sliced at the authoritative extent there is no next-partition
                    // row left in `partition_bytes` to reach this arm.
                    saw_next_partition = true;
                    Ok(std::ops::ControlFlow::Break(()))
                }
            },
        )?;
        Ok((rows, saw_next_partition))
    }
}
