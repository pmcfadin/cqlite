//! CQL decimal → `Decimal128(38, DECIMAL_FIXED_SCALE)` rescaling for Arrow/Parquet
//! export.
//!
//! Split out of `arrow_convert` (epic #1116 file-size split) so the bounded,
//! fail-closed rescale logic — the fix for the issue #1755 export hang — lives in
//! one small, focused module. `arrow_convert` re-imports [`rescale_decimal`]; the
//! two call sites (the batch and streaming `Decimal128` array builders) are
//! unchanged.

use super::arrow_convert::{
    bigint_to_i128, ArrowConvertError, DECIMAL_FIXED_SCALE, DECIMAL_MAX_PRECISION,
};
use num_bigint::BigInt;
use std::sync::LazyLock;

/// Maximum absolute value representable by `Decimal128(38, …)`: `10^38 − 1`.
///
/// Hoisted to a `LazyLock` so the ~38-digit `BigInt` is allocated ONCE for the
/// process rather than recomputed on every [`rescale_decimal`] call (which runs
/// per decimal cell during Parquet export).
static DECIMAL128_MAX_ABS: LazyLock<BigInt> =
    LazyLock::new(|| BigInt::from(10i64).pow(38u32) - BigInt::from(1i64));

/// Rescale a CQL decimal value to the fixed column scale (`DECIMAL_FIXED_SCALE`).
///
/// Returns the rescaled `i128` value, or an error if:
/// - The input scale exceeds `DECIMAL_FIXED_SCALE` (would require truncation /
///   silent precision loss — fail closed instead of divide-and-truncate).
/// - The rescaled magnitude exceeds 38 decimal digits (overflow of `Decimal128`).
/// - Checked multiplication overflows `i128` when scaling up.
///
/// Issue #1755: the scale can be a GARBAGE value read from a corrupt SSTable on
/// the raw/uncompressed parse path. The exponent of `10^delta` is therefore
/// bounded BEFORE the `BigInt::pow` allocation — an unbounded exponent (from a
/// large-negative garbage scale) previously allocated a multi-hundred-megabyte
/// integer and hung the Parquet export (a non-panic hang `panic=unwind` cannot
/// fix). `execute` / `execute_streaming` never hit this: they materialize the
/// decimal WITHOUT rescaling.
///
/// Follow-up option (not implemented here per owner decision 2026-07-01): derive
/// a per-column target scale from schema / `Statistics.db` metadata so that
/// higher-scale decimals can be represented without loss instead of erroring.
pub(crate) fn rescale_decimal(scale: i32, unscaled: &[u8]) -> Result<i128, ArrowConvertError> {
    if unscaled.is_empty() {
        return Ok(0i128);
    }

    // Fail closed: a scale greater than the fixed target scale can only be
    // reconciled by dividing (truncating toward zero), which silently drops
    // precision from an authoritative export. Error instead — mirror the
    // over-magnitude guard below rather than truncate.
    if scale > DECIMAL_FIXED_SCALE {
        return Err(ArrowConvertError::InvalidValue(format!(
            "decimal scale {scale} exceeds the fixed export scale {DECIMAL_FIXED_SCALE}; \
             refusing to truncate (would lose precision)"
        )));
    }

    // Decode big-endian two's-complement signed integer.
    let bigint = BigInt::from_signed_bytes_be(unscaled);

    // Compute scale delta: positive means we must multiply (scale up). A negative
    // delta (scale > DECIMAL_FIXED_SCALE) is rejected above. Widen to i64 so a
    // corrupt `scale == i32::MIN` cannot overflow the subtraction (issue #1755).
    let delta = DECIMAL_FIXED_SCALE as i64 - scale as i64;

    let rescaled = if delta == 0 || bigint.sign() == num_bigint::Sign::NoSign {
        // A zero unscaled value is zero at every scale — never materialize
        // 10^delta for it (issue #1755).
        bigint
    } else if delta > DECIMAL_MAX_PRECISION as i64 {
        // Fail closed on a corrupt/garbage `scale` (issue #1755): a non-zero value
        // scaled up by more than the Decimal128 precision (38 digits) cannot fit
        // the target type, so reject WITHOUT computing the unbounded `10^delta` —
        // a large negative garbage scale would otherwise drive a multi-gigabyte
        // BigInt allocation and hang the export. Mirrors the over-magnitude guard
        // below, moved ahead of the allocation.
        return Err(ArrowConvertError::InvalidValue(format!(
            "decimal scale {scale} requires rescaling by 10^{delta}, which exceeds \
             Decimal128(38, {DECIMAL_FIXED_SCALE}) range"
        )));
    } else {
        // Scale up: multiply by 10^delta (delta bounded to <= 38 above).
        let factor = BigInt::from(10i64).pow(delta as u32);
        bigint * factor
    };

    // Verify the result fits in Decimal128(38, …). `DECIMAL128_MAX_ABS`
    // (`10^38 − 1`) is computed once via LazyLock rather than per call.
    let abs_rescaled = if rescaled.sign() == num_bigint::Sign::Minus {
        -rescaled.clone()
    } else {
        rescaled.clone()
    };
    if abs_rescaled > *DECIMAL128_MAX_ABS {
        return Err(ArrowConvertError::InvalidValue(format!(
            "Decimal value exceeds Decimal128(38, {DECIMAL_FIXED_SCALE}) range after rescaling"
        )));
    }

    bigint_to_i128(&rescaled)
}

// Issue #1755 regression tests drive the PUBLIC Parquet export surface (batch
// `ParquetWriter::write` and the streaming `StreamingParquetWriter` — the exact
// path the Python `export_parquet` / Node `exportParquet` bindings call) with a
// garbage decimal `scale`. They must TERMINATE with a typed error, never hang on
// the previously-unbounded `10^delta` allocation. Each case runs on a worker
// thread with an `mpsc::recv_timeout` watchdog so a regression manifests as a
// BOUNDED test failure, not a hung suite. Gated on `parquet` so the crate's
// parquet test lanes (`--all-features --lib`, `--features=parquet --lib`)
// exercise them; verified failing-first (4/5 hang) on the pre-fix code.
#[cfg(all(test, feature = "parquet"))]
mod tests {
    use crate::export::parquet::{ParquetExportOptions, ParquetWriter, StreamingParquetWriter};
    use crate::query::{ColumnInfo, QueryMetadata, QueryResult, QueryRow};
    use crate::schema::CqlType;
    use crate::types::DataType;
    use crate::{RowKey, Value};
    use std::collections::HashMap;
    use std::sync::mpsc;
    use std::time::Duration;

    /// A fixed, correct rescale returns in microseconds, so any multi-second run
    /// is the unbounded-`pow` regression.
    const WATCHDOG: Duration = Duration::from_secs(15);

    /// A single `decimal` column whose schema-sourced CQL type routes the export
    /// through the `Decimal128` builder (and thus `rescale_decimal`).
    fn decimal_metadata() -> QueryMetadata {
        QueryMetadata {
            columns: vec![ColumnInfo {
                name: "account_balance".to_string(),
                data_type: DataType::Blob,
                nullable: true,
                position: 0,
                table_name: None,
                cql_type: Some(CqlType::Decimal),
            }],
            ..Default::default()
        }
    }

    fn row_with_decimal(scale: i32, unscaled: Vec<u8>) -> QueryRow {
        let mut values = HashMap::new();
        values.insert(
            "account_balance".to_string(),
            Value::Decimal { scale, unscaled },
        );
        QueryRow::with_values(RowKey::new(vec![1]), values)
    }

    /// Run `f` on a worker thread and fail the test (rather than hang the suite)
    /// if it does not finish within [`WATCHDOG`].
    ///
    /// On a `Timeout` the worker thread is intentionally NOT joined: a
    /// genuinely-hung worker (the regression this watchdog guards) would block
    /// the join forever, defeating the whole point. Leaking the thread is the
    /// acceptable trade-off for a regression watchdog — the test process exits
    /// on panic and reaps it (issue #2145, nit 3). Distinguishing `Disconnected`
    /// (worker panicked, channel dropped) from `Timeout` (true hang) gives a
    /// clearer failure diagnostic (issue #2145, nit 2).
    fn run_bounded<T: Send + 'static>(what: &str, f: impl FnOnce() -> T + Send + 'static) -> T {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(f());
        });
        match rx.recv_timeout(WATCHDOG) {
            Ok(v) => v,
            Err(mpsc::RecvTimeoutError::Disconnected) => panic!(
                "issue #1755: {what} worker thread panicked without producing a result \
                 (channel disconnected)"
            ),
            Err(mpsc::RecvTimeoutError::Timeout) => panic!(
                "issue #1755: {what} did not terminate within {WATCHDOG:?} (hang) — \
                 worker thread intentionally leaked (see run_bounded doc comment)"
            ),
        }
    }

    /// Batch writer must fail closed (typed error), not hang, on a garbage scale.
    #[test]
    fn batch_export_garbage_decimal_scale_errors_not_hangs() {
        let mut result = QueryResult::new();
        result.metadata = decimal_metadata();
        // Large negative garbage scale → unpatched code computed 10^(~1e9).
        result
            .rows
            .push(row_with_decimal(-1_000_000_000, vec![0x01]));

        let is_err = run_bounded("ParquetWriter::write", move || {
            ParquetWriter::write(&result, &ParquetExportOptions::default()).is_err()
        });
        assert!(
            is_err,
            "a garbage decimal scale must produce a typed ParquetExportError, not silent success"
        );
    }

    /// The streaming writer is the EXACT surface `db.export_parquet` /
    /// `exportParquet` drive (new + write_chunk + finalize).
    #[test]
    fn streaming_export_garbage_decimal_scale_errors_not_hangs() {
        let metadata = decimal_metadata();
        let row = row_with_decimal(-1_000_000_000, vec![0x01]);

        let is_err = run_bounded("StreamingParquetWriter export", move || {
            let opts = ParquetExportOptions::default();
            let mut writer = match StreamingParquetWriter::new(Vec::<u8>::new(), &metadata, &opts) {
                Ok(w) => w,
                Err(_) => return true, // schema/init rejected — already fail-closed
            };
            // The write_chunk/finalize → convert_to_arrays → rescale_decimal path
            // is where the unbounded allocation lived. The streaming writer buffers
            // rows and converts a row group at flush time, so the error can surface
            // from EITHER write_chunk or the final flush.
            writer.write_chunk(std::slice::from_ref(&row)).is_err() || writer.finalize().is_err()
        });
        assert!(
            is_err,
            "streaming export must return a typed error on a garbage decimal scale, not hang"
        );
    }

    /// `i32::MIN` scale exercises the widened-to-i64 delta subtraction (no
    /// overflow panic) — still fail-closed, still bounded.
    #[test]
    fn export_i32_min_decimal_scale_is_bounded() {
        let mut result = QueryResult::new();
        result.metadata = decimal_metadata();
        result
            .rows
            .push(row_with_decimal(i32::MIN, vec![0x7f, 0xff]));

        let is_err = run_bounded("ParquetWriter::write (i32::MIN scale)", move || {
            ParquetWriter::write(&result, &ParquetExportOptions::default()).is_err()
        });
        assert!(
            is_err,
            "i32::MIN scale must fail closed without overflow/hang"
        );
    }

    /// Regression guard: a zero unscaled value is zero at ANY scale, so an extreme
    /// scale must NOT error (and must not materialize 10^delta) — it exports fine.
    #[test]
    fn export_zero_unscaled_with_extreme_scale_succeeds() {
        let mut result = QueryResult::new();
        result.metadata = decimal_metadata();
        result
            .rows
            .push(row_with_decimal(-1_000_000_000, vec![0x00]));

        let bytes = run_bounded("ParquetWriter::write (zero unscaled)", move || {
            ParquetWriter::write(&result, &ParquetExportOptions::default())
        })
        .expect("a zero-valued decimal is representable at any scale and must export");
        assert_eq!(&bytes[0..4], b"PAR1", "valid Parquet output expected");
    }

    /// Regression guard: the bound must not break a LEGITIMATE in-range decimal
    /// (scale within the fixed export scale) — it still exports correctly.
    #[test]
    fn export_valid_decimal_still_succeeds() {
        let mut result = QueryResult::new();
        result.metadata = decimal_metadata();
        // 12345 unscaled at scale 2 = 123.45 — well within Decimal128(38, 9).
        result.rows.push(row_with_decimal(2, vec![0x30, 0x39]));

        let bytes = run_bounded("ParquetWriter::write (valid decimal)", move || {
            ParquetWriter::write(&result, &ParquetExportOptions::default())
        })
        .expect("a valid in-range decimal must still export after the #1755 bound");
        assert_eq!(&bytes[0..4], b"PAR1", "valid Parquet output expected");
    }
}
