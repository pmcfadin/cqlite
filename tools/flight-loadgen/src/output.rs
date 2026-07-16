//! JSONL output + terminal-error orchestration (spec: JSONL requirement).
//!
//! The ramp writes one `flight-loadgen.step/v1` line per COMPLETED step. Crucially,
//! [`finalize`] persists whatever records completed BEFORE surfacing any terminal
//! ramp error, so a late-step connect failure (an EXPECTED outcome under saturation,
//! design §(c)) never discards the JSONL of the steps that already succeeded.

use std::io::Write;
use std::path::Path;

use crate::record::StepRecord;

/// Write `records` as JSONL (one object per line) to `out`, or stdout if `None`.
pub fn write_records(records: &[StepRecord], out: Option<&Path>) -> Result<(), String> {
    let mut buf = String::new();
    for rec in records {
        let line = rec
            .to_jsonl()
            .map_err(|e| format!("serializing record: {e}"))?;
        buf.push_str(&line);
        buf.push('\n');
    }
    match out {
        Some(path) => std::fs::write(path, buf.as_bytes())
            .map_err(|e| format!("writing --out {}: {e}", path.display())),
        None => {
            let stdout = std::io::stdout();
            let mut lock = stdout.lock();
            lock.write_all(buf.as_bytes())
                .and_then(|()| lock.flush())
                .map_err(|e| format!("writing stdout: {e}"))
        }
    }
}

/// Persist every completed-step record, THEN surface any terminal ramp error.
///
/// Records are ALWAYS written first: a `ramp_error` (e.g. a connect failure at a
/// later, higher-concurrency step — an expected saturation outcome) must never
/// discard the JSONL already earned by earlier steps. The write itself failing
/// takes precedence (there is no output to trust), otherwise the ramp error, if
/// any, is propagated.
pub fn finalize(
    records: &[StepRecord],
    ramp_error: Option<String>,
    out: Option<&Path>,
) -> Result<(), String> {
    write_records(records, out)?;
    match ramp_error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::StepAgg;

    fn sample_record(step: usize) -> StepRecord {
        StepAgg::new().into_record(
            "r".into(),
            "http://x".into(),
            0,
            42,
            step,
            1 << step,
            "mixed".into(),
            1.0,
        )
    }

    /// The data-loss regression (roborev Medium): when a later step fails, the
    /// records already gathered from prior successful steps MUST still be written
    /// out — finalize persists them, then propagates the terminal error.
    #[test]
    fn finalize_writes_completed_steps_before_propagating_error() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let path = dir.path().join("out.jsonl");
        let records = vec![sample_record(0), sample_record(1)];

        let err = finalize(
            &records,
            Some("ramp stopped at step 2: connect refused".to_string()),
            Some(&path),
        )
        .expect_err("the terminal ramp error must still be surfaced");
        assert!(err.contains("step 2"), "propagates the ramp error: {err}");

        // ...but the two completed steps' JSONL is on disk, NOT discarded.
        let written = std::fs::read_to_string(&path).expect("output file written");
        let lines: Vec<&str> = written.lines().collect();
        assert_eq!(lines.len(), 2, "both completed steps written: {written:?}");
        for (i, line) in lines.iter().enumerate() {
            let v: serde_json::Value = serde_json::from_str(line).expect("valid JSONL line");
            assert_eq!(v["step"], i, "record {i} preserved in order");
        }
    }

    #[test]
    fn finalize_ok_when_no_error() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let path = dir.path().join("out.jsonl");
        finalize(&[sample_record(0)], None, Some(&path)).expect("clean finish");
        let written = std::fs::read_to_string(&path).expect("output file");
        assert_eq!(written.lines().count(), 1);
    }
}
