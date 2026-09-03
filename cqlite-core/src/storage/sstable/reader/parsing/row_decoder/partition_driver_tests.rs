//! Issue #1640 (K1) driver-level tests: the framing skeleton
//! `V5CompressedLegacyParser::drive_partition_sliding` owns, driven through a STUB
//! [`SlidingPartitionPolicy`] over a SYNTHETIC byte buffer — so the contract
//! (buffer-then-flush, no-double-emit on a mid-partition `NeedMore`,
//! marker-`Stop` termination, and the #3782/#3809 failure dispositions) is pinned
//! independently of any real row decode, schema, or on-disk fixture.
//!
//! An external file (`#[path]`) rather than an inline `mod tests`, so
//! `partition_driver.rs` stays under the campsite-rule source limit (epic #1116)
//! while these cases grow.

use super::*;

use super::*;

/// Build a `RowHeader` fixture with only the fields the #932 row-timestamp
/// rule reads populated; everything else is a benign default.
fn hdr(
    timestamp: Option<i64>,
    local_deletion_time: Option<i32>,
    marked_for_delete_at: Option<i64>,
) -> RowHeader {
    RowHeader {
        timestamp,
        ttl: None,
        liveness_expires_at_seconds: None,
        local_deletion_time,
        marked_for_delete_at,
        header_size: 0,
        row_size_vint_len: 0,
        missing_columns_bitmap: None,
        max_data_cell_timestamp: None,
        max_data_cell_expires_at: None,
        has_live_forever_data_cell: false,
        has_deleted_data_cell: false,
    }
}

/// Issue #1640 / #932 lockstep guard.
///
/// Both sliding-window emit paths (streaming-scan timestamps and per-element
/// compaction) now resolve a row's write timestamp through the single
/// [`row_write_timestamp`] helper, so this asserts THE one decision site.
/// On `main` the rule was hand-copied into both loops, making this
/// un-writable as a single-site assertion (the point of K1).
#[test]
fn row_write_timestamp_coexistence_prefers_liveness() {
    // A HAS_DELETION row that ALSO carries a liveness timestamp (surviving
    // cells written strictly AFTER the row deletion): the row timestamp is
    // the liveness ts (2000), NOT the older markedForDeleteAt (1000). Both
    // the user-scan and compaction paths call the same helper, so they agree
    // by construction — a future divergence would fail HERE.
    let coexistence = Some(hdr(Some(2000), Some(5), Some(1000)));
    assert_eq!(row_write_timestamp(&coexistence), 2000);
}

#[test]
fn row_write_timestamp_pure_tombstone_uses_marked_for_delete_at() {
    // A PURE row tombstone (HAS_DELETION, no HAS_TIMESTAMP): fall back to
    // markedForDeleteAt (1000), never epoch 0 (which would lose LWW ordering).
    let pure_tombstone = Some(hdr(None, Some(5), Some(1000)));
    assert_eq!(row_write_timestamp(&pure_tombstone), 1000);
}

#[test]
fn row_write_timestamp_live_row_uses_liveness() {
    // A live row with no deletion: the liveness timestamp.
    let live = Some(hdr(Some(3000), None, None));
    assert_eq!(row_write_timestamp(&live), 3000);
}

#[test]
fn row_write_timestamp_absent_header_is_zero() {
    assert_eq!(row_write_timestamp(&None), 0);
}

#[test]
fn row_write_timestamp_pure_tombstone_without_mfda_promotes_seconds() {
    // Defensive: a deletion recorded with only localDeletionTime (seconds) —
    // promoted to microseconds so ordering stays non-zero and monotonic.
    let odd = Some(hdr(None, Some(7), None));
    assert_eq!(row_write_timestamp(&odd), 7 * 1_000_000);
}

// -----------------------------------------------------------------------
// Driver-level framing tests (issue #1640, roborev K1 test-depth finding).
//
// The correctness-critical logic this refactor centralizes is
// `drive_partition_sliding`'s framing skeleton — the previously-duplicated
// loop whose divergence "manufactures parity regressions". These tests drive
// that skeleton directly with a STUB `SlidingPartitionPolicy` over a SYNTHETIC
// byte buffer, so the framing contract (buffer-then-flush, no-double-emit on a
// mid-partition `NeedMore`, marker-Stop termination) is pinned independently of
// any real row decode, schema, or on-disk fixture.
//
// `write-support` is a DEFAULT feature; the gate is only so the minimal
// `--no-default-features` build (no synthetic-reader writer) still compiles.
// -----------------------------------------------------------------------

/// A synthetic byte the stub treats as exactly ONE data row: it carries
/// neither the END_OF_PARTITION bit (0x01) nor the IS_MARKER bit (0x02), so
/// the driver routes it to `on_data_row`.
#[cfg(feature = "write-support")]
const STUB_ROW_BYTE: u8 = 0xa0;

/// The IS_MARKER flag byte (0x02, END_OF_PARTITION bit clear): the driver
/// routes it to `on_range_marker`.
#[cfg(feature = "write-support")]
const STUB_MARKER_BYTE: u8 = 0x02;

/// A body byte the stub policy answers with a DECODE ERROR (issue #3782),
/// distinct from a byte it simply declines. Not a marker and not
/// END_OF_PARTITION, so the driver routes it to `on_data_row`.
#[cfg(feature = "write-support")]
const STUB_ERR_BYTE: u8 = 0x7C;

/// The text the stub's decode error carries, asserted verbatim so a test proves
/// the POLICY's error reached the caller rather than some other failure.
#[cfg(feature = "write-support")]
const STUB_ERR_TEXT: &str = "stub row decode failure (#3782)";

/// A body byte the stub policy DECLINES with no error
/// ([`DataRowOutcome::Declined`]) — the pre-#3782 tolerant path, kept distinct
/// from [`STUB_ERR_BYTE`] so a test can prove the two are treated differently.
/// Also free of the 0x01/0x02 bits.
#[cfg(feature = "write-support")]
const STUB_DECLINE_BYTE: u8 = 0xa4;

/// A body byte the stub policy REFUSES ([`DataRowOutcome::Refused`], issue
/// #3809): the row DECODED and its content is unrepresentable. Distinct from
/// [`STUB_ERR_BYTE`] so a test can prove the driver treats the two failures
/// differently — the refusal is never tolerated, at any extent. Also free of
/// the 0x01/0x02 bits.
#[cfg(feature = "write-support")]
const STUB_REFUSE_BYTE: u8 = 0x6C;

/// The text the stub's REFUSAL carries — a supplement only. WHICH failure
/// travelled is proved by the error VARIANT, never by this text (#28): the stub
/// raises its refusal as [`Error::InvalidFormat`] where its decode failure is
/// [`Error::Corruption`], so a test that received the wrong one fails on the
/// `matches!` rather than on a string. The variants are the STUB's, chosen for
/// discriminability; the production refusal's own variant choice is argued at
/// `CompactionRowData::require_tombstone_clustering_identity`.
#[cfg(feature = "write-support")]
const STUB_REFUSE_TEXT: &str = "stub row build refusal (#3809)";

/// A carrier row the stub policy buffers into the driver-owned `pending` vec.
#[cfg(feature = "write-support")]
#[derive(Debug, PartialEq, Eq)]
struct StubRow(u8);

/// Test-only [`SlidingPartitionPolicy`] over a synthetic buffer. It exercises
/// the driver's framing skeleton WITHOUT any real row decode: each
/// [`STUB_ROW_BYTE`] is one row (buffered into `pending`, consuming 1 byte),
/// [`STUB_ERR_BYTE`] is a row that FAILS TO DECODE
/// ([`DataRowOutcome::DecodeFailed`], issue #3782), [`STUB_REFUSE_BYTE`] is a
/// row that DECODED but must not be emitted ([`DataRowOutcome::Refused`], issue
/// #3809), any other byte DECLINES with no error, any range-tombstone marker
/// is answered with [`MarkerOutcome::Stop`], and `buffered` records how many
/// rows were pushed into `pending` — so a test can prove a row WAS buffered
/// even when the driver forwards ZERO rows.
#[cfg(feature = "write-support")]
struct StubPolicy {
    /// Count of rows the policy pushed into the driver-owned `pending` vec.
    buffered: usize,
}

#[cfg(feature = "write-support")]
impl SlidingPartitionPolicy for StubPolicy {
    type Row = StubRow;

    fn on_partition_open(
        &mut self,
        _partition_key: RowKey,
        _partition_deletion: Option<(i64, i32)>,
        _schema: &TableSchema,
        _pending: &mut Vec<Self::Row>,
    ) {
        // No synthetic partition-delete row for these framing tests.
    }

    fn on_range_marker(
        &mut self,
        _data: &[u8],
        _offset: usize,
        _schema: &TableSchema,
        _pending: &mut Vec<Self::Row>,
    ) -> MarkerOutcome {
        // Mirror the pre-K1 `break`/`NeedMore` behaviour: a marker the policy
        // cannot represent faithfully terminates the partition.
        MarkerOutcome::Stop
    }

    fn on_data_row(
        &mut self,
        data: &[u8],
        offset: usize,
        _schema: &TableSchema,
        _reader: &crate::storage::sstable::reader::types::SSTableReader,
        _resolution: &RowColumnResolution,
        pending: &mut Vec<Self::Row>,
    ) -> DataRowOutcome {
        match data.get(offset) {
            Some(&b) if b == STUB_ROW_BYTE => {
                pending.push(StubRow(b));
                self.buffered += 1;
                DataRowOutcome::Decoded(offset + 1)
            }
            // A genuine DECODE ERROR (#3782): the driver, not the policy,
            // decides whether to tolerate it.
            Some(&b) if b == STUB_ERR_BYTE => {
                DataRowOutcome::DecodeFailed(Error::corruption(STUB_ERR_TEXT))
            }
            // A REFUSAL (#3809): the row decoded, its content is
            // unrepresentable, and no refill can change that — so the driver
            // must propagate it whatever the extent.
            Some(&b) if b == STUB_REFUSE_BYTE => {
                DataRowOutcome::Refused(Error::invalid_format(STUB_REFUSE_TEXT))
            }
            // Anything else: the policy DECLINES with no error — the driver
            // treats this as end-of-partition on the final chunk, else
            // `NeedMore`, exactly as before #3782.
            _ => DataRowOutcome::Declined,
        }
    }
}

/// A minimal single-partition-key schema `t(pk int, v text)`. The concrete
/// columns are irrelevant to the framing under test (the stub never consults
/// the schema, reader, or resolution), but a valid schema is required to build
/// [`RowColumnResolution`].
#[cfg(feature = "write-support")]
fn stub_schema() -> crate::schema::TableSchema {
    use crate::schema::{Column, KeyColumn, TableSchema};
    let col = |name: &str, ty: &str, nullable: bool| Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable,
        default: None,
        is_static: false,
    };
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_tbl".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![col("pk", "int", false), col("v", "text", true)],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    }
}

/// Build a synthetic **nb**-format partition: a LIVE header (no partition
/// tombstone) followed by `body` bytes. The parser built by
/// [`V5CompressedLegacyParser::new`] uses the nb-compatible gates
/// (`has_uint_deletion_time == false`), so the header is:
/// `flags(1) + key_len(1) + key(1) + nb DeletionTime` where the nb
/// DeletionTime is a 4-byte localDeletionTime (`i32::MAX` == LIVE sentinel) +
/// 8-byte markedForDeleteAt (0). Fixed bytes only — no wall-clock input.
#[cfg(feature = "write-support")]
fn synthetic_partition(body: &[u8]) -> Vec<u8> {
    let mut buf = vec![0x00, 0x01, 0x42]; // flags, key_len=1, key=[0x42]
    buf.extend_from_slice(&i32::MAX.to_be_bytes()); // LIVE localDeletionTime
    buf.extend_from_slice(&0i64.to_be_bytes()); // markedForDeleteAt
    buf.extend_from_slice(body);
    buf
}

/// Drive one synthetic partition through the real `drive_partition_sliding`
/// skeleton with the [`StubPolicy`]. Returns the [`ParseStep`], the number of
/// rows the policy buffered into `pending`, and the rows the driver actually
/// forwarded to the external `emit` closure.
///
/// The `&SSTableReader` is a genuine (dataset-independent) synthetic handle
/// reused from the decoder-lockstep net; its bytes are never consulted here —
/// the stub ignores the reader and resolution entirely.
#[cfg(feature = "write-support")]
async fn drive(data: &[u8], at_final_chunk: bool) -> (ParseStep, usize, Vec<StubRow>) {
    let (step, buffered, collected) = drive_result(data, at_final_chunk).await;
    let step = step.expect("drive_partition_sliding should not error on this input");
    (step, buffered, collected)
}

/// As [`drive`], but hands back the driver's `Result` so a test can assert the
/// #3782 refusal (and the error it carries) rather than unwrapping it.
#[cfg(feature = "write-support")]
async fn drive_result(
    data: &[u8],
    at_final_chunk: bool,
) -> (Result<ParseStep>, usize, Vec<StubRow>) {
    let reader = super::super::decoder_lockstep_tests::open_reader()
        .await
        .expect("write-support synthetic reader is always available");
    let parser =
        V5CompressedLegacyParser::new("test_ks".to_string(), "test_tbl".to_string(), 0, 0, None);
    let schema = stub_schema();
    let mut policy = StubPolicy { buffered: 0 };
    let mut collected: Vec<StubRow> = Vec::new();
    let step = parser.drive_partition_sliding(
        data,
        &schema,
        &reader,
        at_final_chunk,
        &mut policy,
        |row| {
            collected.push(row);
            Ok(std::ops::ControlFlow::Continue(()))
        },
    );
    (step, policy.buffered, collected)
}

/// (a) Issue-#827 no-double-emit invariant: a truncated partition on a
/// NON-final chunk returns `NeedMore` and forwards ZERO rows — *even though*
/// the policy already buffered a row into `pending`. Discarding `pending` on a
/// mid-partition `NeedMore` is what makes a refill-and-re-parse from the
/// partition start safe (a forwarded row here would be duplicated on re-parse).
#[cfg(feature = "write-support")]
#[tokio::test]
async fn truncated_non_final_chunk_buffers_but_emits_zero() {
    // Header + one row byte, no END_OF_PARTITION: the buffer ends mid-partition.
    let data = synthetic_partition(&[STUB_ROW_BYTE]);
    let (step, buffered, collected) = drive(&data, false).await;
    assert_eq!(
        step,
        ParseStep::NeedMore,
        "a mid-partition end-of-buffer on a non-final chunk must request more bytes"
    );
    assert_eq!(
        buffered, 1,
        "the row WAS buffered into the driver-owned pending vec"
    );
    assert!(
        collected.is_empty(),
        "NeedMore must DISCARD pending and forward zero rows so a re-parse cannot \
             double-emit (issue #827)"
    );
}

/// (b) The SAME buffer with `at_final_chunk = true` flushes the buffered row:
/// on the final chunk an end-of-buffer is end-of-partition, so `pending` is
/// forwarded exactly once.
#[cfg(feature = "write-support")]
#[tokio::test]
async fn same_buffer_final_chunk_flushes_pending() {
    let data = synthetic_partition(&[STUB_ROW_BYTE]);
    let (step, buffered, collected) = drive(&data, true).await;
    assert!(
        matches!(step, ParseStep::Emitted(_)),
        "the final chunk treats end-of-buffer as end-of-partition and flushes"
    );
    assert_eq!(buffered, 1, "the same single row is buffered");
    assert_eq!(
        collected,
        vec![StubRow(STUB_ROW_BYTE)],
        "the buffered row is forwarded exactly once on the final chunk"
    );
}

/// (c) A range-tombstone marker the policy answers with `MarkerOutcome::Stop`
/// on a NON-final chunk yields `NeedMore` with NO emission — mirroring the
/// pre-K1 `break`/`NeedMore` terminate-partition behaviour — and discards any
/// rows already buffered before the marker.
#[cfg(feature = "write-support")]
#[tokio::test]
async fn marker_stop_non_final_chunk_needmore_no_emit() {
    // One row, then a marker byte (IS_MARKER set, END_OF_PARTITION clear).
    let data = synthetic_partition(&[STUB_ROW_BYTE, STUB_MARKER_BYTE]);
    let (step, buffered, collected) = drive(&data, false).await;
    assert_eq!(
        step,
        ParseStep::NeedMore,
        "on_range_marker -> Stop on a non-final chunk requests more bytes"
    );
    assert_eq!(buffered, 1, "the pre-marker row was buffered into pending");
    assert!(
        collected.is_empty(),
        "a marker Stop discards pending and forwards nothing"
    );
}

// -----------------------------------------------------------------------
// Row-DISPOSITION cases (issues #3782 and #3809): how the driver disposes of
// each `DataRowOutcome` FAILURE, and at which extent. The two are pinned
// AGAINST EACH OTHER, because the whole point is that they differ:
//
//   outcome        | non-final chunk (BufferExtent::Window) | final chunk
//   ---------------+----------------------------------------+-------------
//   DecodeFailed   | NeedMore (straddling row)              | Err (loss)
//   Refused        | Err                                    | Err
// -----------------------------------------------------------------------

/// (d) Issue #3782, the refusal: a row that FAILS TO DECODE on the FINAL chunk
/// returns the decode error, KIND PRESERVED, instead of flushing a partial
/// partition and reporting `Emitted`. No further bytes can arrive at the final
/// chunk, so the error is truncation/corruption — data loss — never framing.
#[tokio::test]
async fn final_chunk_row_decode_error_is_returned_not_swallowed() {
    // One good row, then a row the policy cannot decode.
    let data = synthetic_partition(&[STUB_ROW_BYTE, STUB_ERR_BYTE]);
    let (step, buffered, collected) = drive_result(&data, true).await;
    let err = match step {
        Err(e) => e,
        Ok(step) => panic!(
            "a decode error at the final chunk must be REFUSED, not flushed as a partial \
                 partition: got {step:?} with {} rows forwarded",
            collected.len()
        ),
    };
    // KIND first: a re-wrap that forwarded the text would satisfy the message
    // check below while destroying the property AC1 is about.
    assert!(
        matches!(err, Error::Corruption(_)),
        "the POLICY's error KIND must reach the caller unchanged, got: {err:?}"
    );
    assert!(
        err.to_string().contains(STUB_ERR_TEXT),
        "the POLICY's error must reach the caller unchanged, got: {err}"
    );
    assert_eq!(buffered, 1, "the pre-error row was buffered");
    assert!(
        collected.is_empty(),
        "a refused partition forwards nothing: {collected:?}"
    );
}

/// (e) Issue #3782, the TOLERANT half — the property that must not regress.
/// The SAME decode error MID-STREAM is an ordinary row straddling the chunk
/// boundary: request more bytes, forward nothing, and never surface an error.
/// Measured over the well-formed corpus this is where 100% of the 614
/// tolerations occur.
#[tokio::test]
async fn non_final_chunk_row_decode_error_still_requests_more_bytes() {
    let data = synthetic_partition(&[STUB_ROW_BYTE, STUB_ERR_BYTE]);
    let (step, buffered, collected) = drive_result(&data, false).await;
    assert!(
        matches!(step, Ok(ParseStep::NeedMore)),
        "a mid-stream decode error must stay tolerant (NeedMore), got {step:?}"
    );
    assert_eq!(buffered, 1, "the pre-error row was buffered");
    assert!(
        collected.is_empty(),
        "NeedMore discards pending so a re-parse cannot double-emit (#827)"
    );
}

/// (f) Issue #3782 did NOT change the DECLINE path: a policy that returns
/// `Ok(None)` still ends the partition on the final chunk and still flushes the
/// rows buffered before it. Only a genuine `Err` refuses.
#[tokio::test]
async fn final_chunk_policy_decline_still_flushes_as_before() {
    let data = synthetic_partition(&[STUB_ROW_BYTE, STUB_DECLINE_BYTE]);
    let (step, buffered, collected) = drive(&data, true).await;
    assert!(
        matches!(step, ParseStep::Emitted(_)),
        "a declined row on the final chunk is end-of-partition, got {step:?}"
    );
    assert_eq!(buffered, 1, "the pre-decline row was buffered");
    assert_eq!(
        collected,
        vec![StubRow(STUB_ROW_BYTE)],
        "the buffered row is still forwarded exactly once"
    );
}

/// (g) Issue #3809, THE case that would otherwise regress silently: a REFUSAL
/// on a NON-FINAL chunk — the disposition every `BufferExtent::Window` entry
/// point gets — must REFUSE, never request more bytes.
///
/// # Premise, asserted rather than assumed
///
/// `BufferExtent` is the block-emit vocabulary and `at_final_chunk` is the
/// driver's; the block-emit entry points translate one into the other with
/// `extent.is_complete()` (`compaction.rs`, `block_emit*.rs`). So
/// `BufferExtent::Window` IS `at_final_chunk == false` here, and the first
/// assertion pins that translation so this test cannot quietly stop covering
/// the extent it names.
///
/// # Oracle
///
/// Not CQLite's prior behaviour — that behaviour is the defect. The rule is
/// derived from what the two failures MEAN: a refusal is raised AFTER the row
/// decoded `Ok`, so it asserts nothing about byte availability, and a short
/// clustering cannot become full-arity however many bytes arrive
/// (`Clustering.java`'s `clustering.size() == types.size()` assert at the
/// pinned `cassandra-5.0.8`, cited in full on
/// `CompactionRowData::require_tombstone_clustering_identity`). `NeedMore` is
/// therefore not a weaker answer, it is an inapplicable one — and on this path
/// it is also the DATA-LOSING one: the caller refills, re-parses, is refused
/// again, and a `Window` consumer that treats `NeedMore` at a truncated tail as
/// end-of-extent drops the rest of the partition and reports `Ok`.
///
/// Sibling case (e) is the control: the SAME driver, the SAME non-final chunk,
/// a `DecodeFailed` instead — which MUST stay tolerant. Without that pairing a
/// blanket "always refuse" would pass here and break every point read.
#[tokio::test]
async fn refusal_on_a_window_extent_refuses_and_never_requests_more_bytes() {
    assert!(
        !BufferExtent::Window.is_complete(),
        "premise: a Window-declaring entry point drives this loop with \
             at_final_chunk == false — if this ever changes, this test no longer \
             covers the Window extent"
    );

    // One good row, then a row the policy REFUSES; at_final_chunk = false.
    let data = synthetic_partition(&[STUB_ROW_BYTE, STUB_REFUSE_BYTE]);
    let (step, buffered, collected) = drive_result(&data, false).await;
    let err = match step {
        Err(e) => e,
        Ok(step) => panic!(
            "a REFUSAL mid-window must be propagated, not converted into a \
                 straddle/refill request: got {step:?} with {} rows forwarded",
            collected.len()
        ),
    };
    // VARIANT, not text (#28): the stub raises its refusal as InvalidFormat and
    // its decode failure as Corruption, so receiving the wrong failure fails
    // HERE rather than passing on a substring.
    assert!(
        matches!(err, Error::InvalidFormat(_)),
        "the POLICY's REFUSAL must reach the caller with its kind intact \
             (a Corruption here would mean the DecodeFailed arm ran instead), \
             got: {err:?}"
    );
    assert!(
        err.to_string().contains(STUB_REFUSE_TEXT),
        "the refusal's own error must reach the caller unchanged, got: {err}"
    );
    assert_eq!(buffered, 1, "the pre-refusal row was buffered");
    assert!(
        collected.is_empty(),
        "a refused partition forwards nothing: {collected:?}"
    );
}

/// (h) The other half of (g): a REFUSAL at the FINAL chunk refuses too, so the
/// disposition is EXTENT-INDEPENDENT. Stated as its own case because "refuses
/// at a Window" alone would still hold if the driver had merely widened
/// #3782's final-chunk rule; the property is that `at_final_chunk` is not
/// consulted for a refusal at all.
#[tokio::test]
async fn refusal_is_refused_at_every_extent_including_a_complete_buffer() {
    assert!(
        BufferExtent::Complete.is_complete(),
        "premise: a Complete-declaring entry point drives with at_final_chunk == true"
    );
    let data = synthetic_partition(&[STUB_ROW_BYTE, STUB_REFUSE_BYTE]);
    let (step, buffered, collected) = drive_result(&data, true).await;
    let err = match step {
        Err(e) => e,
        Ok(step) => panic!("a REFUSAL at a complete buffer must refuse, got {step:?}"),
    };
    assert!(
        matches!(err, Error::InvalidFormat(_)),
        "the refusal's kind must survive at the final chunk too, got: {err:?}"
    );
    assert_eq!(buffered, 1, "the pre-refusal row was buffered");
    assert!(
        collected.is_empty(),
        "a refused partition forwards nothing: {collected:?}"
    );
}
