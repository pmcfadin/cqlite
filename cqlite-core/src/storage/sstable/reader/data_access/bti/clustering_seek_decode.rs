//! The clustering-slice seek's decode step, and its ONE fallback (issue #3721).
//!
//! Extracted from [`super::bti`]'s `scan_single_partition_clustering` (campsite
//! rule, epic #1116: that file is over threshold) because the fallback belongs
//! HERE and nowhere below.
//!
//! # Why this is the only place the retry can live
//!
//! A clustering-slice seek applies TWO independent narrowings, both resolved by
//! `resolve_clustering_seek_window` from the authoritative row index:
//!
//! * `decode_end_bound` — the byte extent DECOMPRESSED for the partition, which
//!   for an engaged slice stops at the selected block(s), well before the
//!   partition's authoritative successor offset; and
//! * `row_body_window` — the within-partition row-body extent the parser walks.
//!
//! Both position the cursor from an INDEX rather than by parsing forward from the
//! partition header, so a per-column decode failure under either is ambiguous
//! between "the cursor is not at a row boundary" and "this column is genuinely
//! undecodable" (see `row_decoder::column_decode_error`). Neither the parser nor
//! the per-format decoders below can resolve that ambiguity, and answering with
//! the rows collected so far — the pre-#3721 behaviour — is silent PARTIAL OUTPUT.
//!
//! It also cannot be resolved one level down, and that was MEASURED rather than
//! reasoned: retrying inside `big_decode_clustering_window` /
//! `bti_collect_partition_rows` with only `row_body_window` dropped still reads the
//! NARROWED `decode_end_bound`, so a clean read of the committed
//! `test_big.wide_partition` fixture failed there too (`invalid cell flags 0x63` at
//! offset 342864, an ASCII byte from a `text` value — the truncated extent ends
//! mid-partition, so the walk runs off the end of the bytes it was given).
//!
//! This function holds BOTH narrowings, so it is the only place that can retract
//! BOTH: on [`Error::ColumnDecode`] it re-decodes with the AUTHORITATIVE partition
//! extent and no row-body window — the ordinary full-partition path, which parses
//! forward from the partition header. A misalignment artifact then disappears and a
//! genuine decode failure surfaces from a cursor known to be at a real row
//! boundary. Only the fast path is lost, never rows; the seek is reported as NOT
//! engaged for the retry, so the caller's `AccessPath` stays honest.

use super::super::SSTableReader;
use crate::schema::TableSchema;
use crate::storage::sstable::reader::parsing::row_decoder::column_decode_error;
use crate::storage::sstable::reader::parsing::BufferExtent;
use crate::types::{ScanRow, TableId};
use crate::{Result, RowKey};

/// One decode attempt's outcome: `None` means the seek could not bound the target
/// partition authoritatively and the caller must fall back to a full scan.
type Decoded = Option<Vec<ScanRow>>;

impl SSTableReader {
    /// Decode the target partition for a clustering-slice seek, retracting the
    /// index narrowing on a per-column decode failure (see the module docs).
    ///
    /// Returns `(rows, clustering_engaged)`. `clustering_engaged` is the value the
    /// caller reports as `AccessPath::ClusteringSlice`, and is `false` whenever the
    /// narrowing was retracted — a retried read decoded the whole partition, so
    /// claiming a slice would be untrue.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn decode_clustering_seek_target(
        &self,
        table_id: &TableId,
        partition_key: &[u8],
        key: &RowKey,
        is_bti: bool,
        fully_qualified_match: bool,
        offset: u64,
        // The AUTHORITATIVE partition extent (successor offset / data-section
        // length), i.e. the bound with NO clustering narrowing applied.
        full_end_bound: Option<usize>,
        // The narrowed decode extent chosen by `resolve_clustering_seek_window`.
        decode_end_bound: Option<usize>,
        row_body_window: Option<(usize, usize)>,
        clustering_engaged: bool,
        schema_opt: Option<&crate::schema::TableSchema>,
    ) -> Result<(Decoded, bool)> {
        // A retry is only meaningful when a narrowing was actually applied; without
        // one the first attempt IS the full-partition path and a failure there is
        // attributable to the data, so it propagates.
        let narrowed =
            clustering_engaged || row_body_window.is_some() || (decode_end_bound != full_end_bound);
        match self
            .decode_clustering_seek_attempt(
                table_id,
                partition_key,
                key,
                is_bti,
                fully_qualified_match,
                offset,
                decode_end_bound,
                full_end_bound,
                row_body_window,
                clustering_engaged,
                schema_opt,
            )
            .await
        {
            Ok(rows) => Ok((rows, clustering_engaged)),
            Err(e) if narrowed && column_decode_error::indexed_walk_falls_back(&e) => {
                let rows = self
                    .decode_clustering_seek_attempt(
                        table_id,
                        partition_key,
                        key,
                        is_bti,
                        fully_qualified_match,
                        offset,
                        full_end_bound,
                        full_end_bound,
                        None,
                        false,
                        schema_opt,
                    )
                    .await?;
                Ok((rows, false))
            }
            Err(e) => Err(e),
        }
    }

    /// Decode the rows of a single BIG (`nb`) wide partition bounded to a
    /// clustering-slice block window (Issue #1184). Resolves the partition's
    /// decompressed window, then runs the windowed parser over only the selected
    /// block extent (`row_body_window`), collecting the target partition's rows.
    ///
    /// The target partition is identified by a partition-key-bytes equality check
    /// only — the reader is already scoped to a single table by the manager, so
    /// (exactly as the full-scan `scan().retain(matches_key)` fallback does) no
    /// table-id match is applied. This decodes SSTables whose serialization-header
    /// keyspace/table differ from a fully-qualified query id. Returns `Ok(None)`
    /// when the partition window cannot be resolved (caller falls back to the full
    /// scan).
    async fn big_decode_clustering_window(
        &self,
        partition_key: &[u8],
        offset: u64,
        end_bound: Option<usize>,
        row_body_window: Option<(usize, usize)>,
        schema: Option<&TableSchema>,
    ) -> Result<Option<Vec<(RowKey, ScanRow)>>> {
        let Some((window, within)) = self
            .decompress_partition_window(offset as usize, end_bound)
            .await?
        else {
            return Ok(None);
        };
        let parser = self.build_v5_parser(true);
        let key = RowKey::from(partition_key.to_vec());
        let avail = window.len().saturating_sub(within);
        let clamped = row_body_window.map(|(s, e)| (s.min(avail), e.min(avail)));
        let mut rows: Vec<(RowKey, ScanRow)> = Vec::new();
        // Issue #3721: a per-column decode failure propagates; the retry cannot live
        // here (`end_bound` is already narrowed) — `clustering_seek_decode`.
        //
        // Issue #3782's extent is `Window`, not `Complete`, and the choice is
        // load-bearing rather than incidental. This is a POINT READER decoding a
        // partition window bounded to a clustering-slice block extent, and it sits
        // inside the retraction loop above: the first attempt narrows, and
        // `indexed_walk_falls_back` retracts to the full-partition path. `Window` is
        // exactly the straddle protocol `BufferExtent` documents for point readers —
        // `Complete` would turn the tolerant tail break into a refusal, so the FIRST
        // attempt would hard-fail instead of retracting, breaking a legitimate control
        // flow rather than fixing a defect. `big_promoted.rs` passes `Window` here too,
        // for the same operation.
        parser.parse_block_emit_windowed(
            &window[within..],
            BufferExtent::Window,
            schema,
            self,
            clamped,
            |(_tid, entry_key, entry_value)| {
                if entry_key.as_bytes() == partition_key {
                    if self.filter_tombstone(&entry_value) {
                        rows.push((key.clone(), entry_value));
                    }
                    Ok(std::ops::ControlFlow::Continue(()))
                } else {
                    // First row of the next partition — stop.
                    Ok(std::ops::ControlFlow::Break(()))
                }
            },
        )?;
        Ok(Some(rows))
    }

    /// ONE decode attempt at the supplied bounds. The BIG arm with an engaged
    /// narrowing uses `big_decode_clustering_window` (partition-key-bytes guard,
    /// not the BTI strict table-id match that rejects writer-header SSTables,
    /// issue #1184); everything else uses the chunk-targeted BTI decoder, which is
    /// also the full-partition path (issue #953).
    #[allow(clippy::too_many_arguments)]
    async fn decode_clustering_seek_attempt(
        &self,
        table_id: &TableId,
        partition_key: &[u8],
        key: &RowKey,
        is_bti: bool,
        fully_qualified_match: bool,
        offset: u64,
        end_bound: Option<usize>,
        // Issue #3890: the target PARTITION's authoritative end, i.e. `end_bound`
        // with NO clustering narrowing applied. It bounds the PARSE, while
        // `end_bound` above bounds the DECOMPRESSION — under an engaged #954
        // slice those differ, and parsing to the narrowed one reads another
        // partition's bytes.
        partition_end_bound: Option<usize>,
        row_body_window: Option<(usize, usize)>,
        clustering_engaged: bool,
        schema_opt: Option<&crate::schema::TableSchema>,
    ) -> Result<Decoded> {
        if !is_bti && clustering_engaged {
            if let Some(rows) = self
                .big_decode_clustering_window(
                    partition_key,
                    offset,
                    end_bound,
                    row_body_window,
                    schema_opt,
                )
                .await?
            {
                return Ok(Some(rows.into_iter().map(|(_k, v)| v).collect()));
            }
        }
        // Issue #953: collects EVERY clustering row of the one target partition
        // (bounded by the authoritative successor offset / data-section length), not
        // just the first, and re-verifies the decoded key. `None` means the seek
        // could not bound the partition authoritatively (the LAST partition with an
        // unknown data-section length) — the caller falls back to the full scan.
        //
        // Issue #954: with `row_body_window` set the parse is bounded to the
        // clustering slice's row-index block extent, so only O(slice) rows are
        // decoded; the post-scan backstop trims the block-granularity slack.
        let parser = self.build_v5_parser(true);
        self.bti_decompress_and_parse_target_all(
            offset as usize,
            end_bound,
            partition_end_bound,
            row_body_window,
            key,
            table_id,
            fully_qualified_match,
            schema_opt,
            &parser,
        )
        .await
    }
}
