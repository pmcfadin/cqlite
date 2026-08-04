//! The MEASUREMENT CORPUS pin: the recorded identity of the 4,000,000-row
//! `ws0.events` corpus, as CONSTANTS IN SOURCE (issue #3272, item 9).
//!
//! # What was missing
//!
//! The CI-fixture Arrow-buffer digest (`0xe6eccf8a9ffbca11`, formerly
//! `0xd0014e42e893f87f`) is pinned as a constant in
//! `cqlite-flight/tests/issue_3096_arrow_buffer_digest.rs`, so a lever that moves
//! it reds a test. The MEASUREMENT corpus's figures lived only in an artifact doc
//! (`docs/reports/ws0-3096-artifacts/baseline-2026-08-03.md`) — prose in a report,
//! not a value any code compares against. Their invariance therefore rested
//! entirely on an operator noticing a change while re-reading a markdown table.
//!
//! This module makes them SOURCE. Everything derivable from them is asserted
//! below; the one thing that is not machine-checkable in a gate is stated as such,
//! with the exact operator command that checks it.
//!
//! # What is and is not machine-checked (read this before citing any of it)
//!
//! | pinned value | machine-checked? | by what |
//! |---|---|---|
//! | [`ROWS`], [`PARTITIONS`], [`ROWS_PER_PARTITION`] | **yes** | internal consistency + the committed identity artifact |
//! | [`DATA_DB_SHA256`], [`DATA_DB_BYTES`], [`BYTES_PER_ROW`] | **yes** | the committed `corpus-identity.json` (byte-for-byte agreement with these constants) |
//! | [`ARROW_BUFFER_DIGEST`], [`ARROW_BUFFER_BATCHES`] | **NO** | requires a real 4,000,000-row corpus; see [`OPERATOR_VERIFY_DIGEST`] |
//! | corpus REPRODUCES from the seed at 4M rows | **NO** | requires a ~2.8 GB, minutes-long generation; see [`OPERATOR_VERIFY_CORPUS`] |
//! | the generator is DETERMINISTIC on this code path | **yes**, scaled down | `tests/determinism_byte_compare.rs` (1,000 rows, same `generate()`) |
//!
//! The two `NO` rows are why this is a pinned-constant-plus-procedure rather than
//! a verifying test: a gate component may not write 2.8 GB or run for minutes.
//! What the pin still buys, which prose did not: the values are now in ONE place
//! that a compiler sees, the committed artifact and the source cannot drift apart
//! silently, and an operator re-run has an exact expected value to compare to
//! rather than a table to eyeball.
//!
//! # PERFORMANCE FIXTURE ONLY (issue #3042)
//!
//! Everything here describes a CQLite-written, CQLite-read corpus, which is
//! INVARIANT to a uniform framing/serialization error. No on-disk correctness
//! claim may rest on any constant in this file. See `crate` docs and `identity`.

/// Rows in the measurement corpus.
pub const ROWS: u64 = 4_000_000;

/// Partitions (`ROWS / ROWS_PER_PARTITION`).
pub const PARTITIONS: u64 = 40_000;

/// Rows per partition.
pub const ROWS_PER_PARTITION: u64 = 100;

/// Recorded `Data.db` size in bytes.
pub const DATA_DB_BYTES: u64 = 2_774_760_422;

/// Recorded `Data.db` lowercase-hex `sha256` — the corpus's identity.
///
/// Recorded 2026-08-03 by
/// `ws0-corpus-gen --out /data/ws0-3096` at [`crate::generate::DEFAULT_SEED`], and
/// carried in `docs/reports/ws0-3096-artifacts/corpus-identity.json`. This
/// constant and that artifact are asserted equal by
/// `tests/measurement_corpus_pin.rs`, so neither can move without the other.
pub const DATA_DB_SHA256: &str = "4a903f6fa27c04dbf87a44fddf78615aed73fcd379ecaee6669f6b0d9bbae269";

/// Recorded `DATA_DB_BYTES / ROWS`, to the precision the artifact records.
pub const BYTES_PER_ROW: f64 = 693.6901055;

/// Recorded total bytes across every emitted component.
pub const TOTAL_COMPONENT_BYTES: u64 = 2_779_185_469;

/// Cells per row (the twelve-column `ws0.events` shape).
pub const CELLS_PER_ROW: usize = 12;

/// The Arrow-buffer digest observed over the measurement corpus, at BOTH taps and
/// on BOTH arms (`bypass` == `merge`).
///
/// Recorded in `docs/reports/ws0-3096-artifacts/baseline-2026-08-03.md` §2. NOT
/// machine-checked — see [`OPERATOR_VERIFY_DIGEST`].
pub const ARROW_BUFFER_DIGEST: u64 = 0x0390_bfbb_81a2_3fa1;

/// `RecordBatch`es the digest above was folded over.
pub const ARROW_BUFFER_BATCHES: u64 = 31_250;

/// The `batch_size` [`ARROW_BUFFER_DIGEST`] was measured at, **DERIVED from the
/// recorded batch count, not copied from the artifact's label**.
///
/// # A recorded label that does not survive arithmetic (found pinning this)
///
/// `baseline-2026-08-03.md` §2 labels the measurement-corpus row
/// "measurement corpus (batch 8192)". That label cannot be right:
/// `ROWS / ARROW_BUFFER_BATCHES` = `4_000_000 / 31_250` = **128**, and at
/// `batch_size` 8192 the same 4,000,000 rows would have produced 489 batches, not
/// 31,250. The batch COUNT is an observed quantity the oracle printed; the "8192"
/// is a hand-written label, and `8192` is the batch size of a DIFFERENT test —
/// `issue_3096_framing_subphase.rs`, whose `CQLITE_WS0_BATCH_SIZE=8192` run is the
/// source of the framing-split figures in §3 of the same document. The digest
/// oracle's `BATCH_SIZE` is a compiled-in `const … = 128` with no env override, so
/// its measurement-corpus case could only ever have run at 128.
///
/// So this constant is 128, the two counts are asserted consistent below, and the
/// artifact's label is left as the historical record it is (those artifacts belong
/// to #3096 and are not edited here). An operator re-running [`OPERATOR_VERIFY_DIGEST`]
/// gets 128 because that is what the oracle compiles; had this pin copied the
/// label, the re-run would have "failed" against a batch size nothing ever used.
pub const ARROW_BUFFER_BATCH_SIZE: u64 = 128;

/// The exact command that regenerates the corpus and verifies it reproduces the
/// recorded identity. Minutes, ~2.8 GB — an operator step, never a gate step.
pub const OPERATOR_VERIFY_CORPUS: &str = "\
cargo run --release -p ws0-corpus-gen --bin ws0-corpus-gen -- \\
  --out /data/ws0-3096-verify --progress-every 0 \\
  --verify-against docs/reports/ws0-3096-artifacts/corpus-identity.json
# exits non-zero, naming the divergent component, on ANY field difference";

/// The exact command that re-folds the Arrow-buffer digest over the measurement
/// corpus. Requires a corpus generated by [`OPERATOR_VERIFY_CORPUS`] first.
pub const OPERATOR_VERIFY_DIGEST: &str = "\
CQLITE_WS0_CORPUS_DIR=/data/ws0-3096 \\
  cargo test -p cqlite-flight --test issue_3096_arrow_buffer_digest -- --nocapture
# prints: measurement-corpus arrow-buffer digests = producer 0x… / wire 0x… over
#         4000000 rows in 31250 batches
# both must equal ws0_corpus_gen::measurement_corpus::ARROW_BUFFER_DIGEST";

/// The corpus spec that reproduces the pinned identity.
pub fn spec(out: std::path::PathBuf) -> crate::generate::CorpusSpec {
    let mut s = crate::generate::CorpusSpec::full(out);
    s.rows = ROWS;
    s.rows_per_partition = ROWS_PER_PARTITION;
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pinned quantities must be mutually consistent. A pin that contradicts
    /// itself is not a pin — and one of these assertions is what surfaced the
    /// artifact's "batch 8192" mislabel.
    #[test]
    fn the_pinned_quantities_are_internally_consistent() {
        assert_eq!(
            ROWS,
            PARTITIONS * ROWS_PER_PARTITION,
            "rows must be partitions x rows-per-partition"
        );
        assert_eq!(
            ROWS % ROWS_PER_PARTITION,
            0,
            "the generator REFUSES a non-divisible row count"
        );
        let bpr = DATA_DB_BYTES as f64 / ROWS as f64;
        assert!(
            (bpr - BYTES_PER_ROW).abs() < 1e-6,
            "BYTES_PER_ROW {BYTES_PER_ROW} != DATA_DB_BYTES/ROWS {bpr}"
        );
        assert!(
            TOTAL_COMPONENT_BYTES > DATA_DB_BYTES,
            "the component total must exceed Data.db alone (Index/Summary/Filter/… exist)"
        );
        assert_eq!(
            DATA_DB_SHA256.len(),
            64,
            "a sha256 is 64 hex chars; a short value is a truncated paste, not a digest"
        );
        assert!(
            DATA_DB_SHA256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "the digest must be LOWERCASE hex — an uppercase or non-hex char means the pin \
             was retyped rather than copied"
        );
    }

    /// The batch size is DERIVED, and the derivation is the assertion: `ROWS /
    /// BATCH_SIZE` must equal the batch count the oracle observed, exactly.
    ///
    /// This is the check that refuses the artifact's "(batch 8192)" label. It is
    /// stated as an equality on the OBSERVED count so a future re-pin cannot
    /// record a batch size and a batch count that cannot both be true.
    #[test]
    fn the_batch_size_is_derived_from_the_observed_batch_count() {
        assert_eq!(
            ROWS / ARROW_BUFFER_BATCH_SIZE,
            ARROW_BUFFER_BATCHES,
            "ROWS/{ARROW_BUFFER_BATCH_SIZE} must equal the observed {ARROW_BUFFER_BATCHES} \
             batches"
        );
        assert_eq!(
            ROWS % ARROW_BUFFER_BATCH_SIZE,
            0,
            "a non-divisible batch size would have produced a short final batch and a \
             non-integer relationship to the observed count"
        );
        // NON-VACUITY for the finding above: the label's 8192 is arithmetically
        // impossible for the recorded batch count, and this states it so a future
        // reader does not "correct" the derived 128 back to the label.
        assert_ne!(
            ROWS / 8192,
            ARROW_BUFFER_BATCHES,
            "if batch 8192 DID yield {ARROW_BUFFER_BATCHES} batches the artifact label would \
             be right and this pin wrong — it does not: it yields {}",
            ROWS / 8192
        );
    }

    /// The digest is a real 64-bit value, not a zero/sentinel. A pin of `0` would
    /// compare equal to an uninitialised fold.
    #[test]
    fn the_pinned_digest_is_not_a_sentinel() {
        assert_ne!(
            ARROW_BUFFER_DIGEST, 0,
            "a 0 digest is indistinguishable from unset"
        );
        assert_ne!(
            ARROW_BUFFER_DIGEST, 0xcbf2_9ce4_8422_2325,
            "that is the FNV-1a OFFSET BASIS — the value the fold starts at, i.e. what it \
             returns having folded NOTHING"
        );
    }

    /// The operator procedures must actually name their commands and targets — a
    /// "procedure" that does not say what to run is the documentation equivalent
    /// of a fail-open guard.
    #[test]
    fn the_operator_procedures_name_runnable_commands() {
        assert!(OPERATOR_VERIFY_CORPUS.contains("ws0-corpus-gen"));
        assert!(OPERATOR_VERIFY_CORPUS.contains("--verify-against"));
        assert!(OPERATOR_VERIFY_CORPUS.contains("corpus-identity.json"));
        assert!(OPERATOR_VERIFY_DIGEST.contains("CQLITE_WS0_CORPUS_DIR"));
        assert!(OPERATOR_VERIFY_DIGEST.contains("issue_3096_arrow_buffer_digest"));
        // The digest procedure must state the expected batch count, so an operator
        // comparing output has the DERIVED number in front of them.
        assert!(
            OPERATOR_VERIFY_DIGEST.contains(&ARROW_BUFFER_BATCHES.to_string()),
            "the procedure must state the expected {ARROW_BUFFER_BATCHES} batches"
        );
    }

    /// The spec builder reproduces the pinned shape (so a future edit to
    /// `CorpusSpec::full` cannot silently change what "the measurement corpus" is).
    #[test]
    fn the_spec_builder_matches_the_pinned_shape() {
        let s = spec(std::path::PathBuf::from("/nonexistent"));
        assert_eq!(s.rows, ROWS);
        assert_eq!(s.rows_per_partition, ROWS_PER_PARTITION);
        assert_eq!(s.seed, crate::generate::DEFAULT_SEED);
        assert_eq!(s.rows / s.rows_per_partition, PARTITIONS);
    }
}
