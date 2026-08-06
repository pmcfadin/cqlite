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
//! | [`ARROW_BUFFER_DIGEST`], [`ARROW_BUFFER_BATCHES`] | **NO** | requires a real 4,000,000-row corpus; see [`operator_verify_digest`] |
//! | corpus REPRODUCES from the seed at 4M rows | **NO** | requires a ~2.8 GB, minutes-long generation; see [`operator_verify_corpus`] |
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

/// `sha256` of the emitted `ws0-events.cql` — a MEASUREMENT INPUT (#3272 R2).
///
/// # Why this one is machine-checked WITHOUT a corpus re-run, unlike the digests above
///
/// Every other pinned value here describes 2.8 GB of generated data, so verifying it needs a
/// minutes-long write no gate component may perform. The schema does not: it is
/// `schema::DDL` — a **source constant** — plus the trailing newline `generate` writes. So its
/// digest is derivable from source ALONE, and `measurement_corpus_pin.rs` asserts this constant
/// equals `sha256(DDL + "\n")` in the gate, every run. That makes it the only corpus-identity
/// digest here with a full machine oracle.
///
/// # This did NOT invalidate any pinned constant, and the reason is worth recording
///
/// R2 adds a field to `CorpusIdentity`, which changes the shape of every identity artifact the
/// generator writes — so the obvious worry is that the committed
/// `docs/reports/ws0-3096-artifacts/corpus-identity.json` (recorded 2026-08-03, before this
/// field existed) no longer matches, and that the honest-looking fix is to regenerate it.
/// **That would be the confirmation trap this issue exists to refuse**: re-pinning an artifact
/// to agree with changed output is how a rig stops being able to detect anything.
///
/// It is not necessary, and here is the argument, which rests on evidence rather than on
/// convenience:
///
/// * `DATA_DB_SHA256` and friends are **untouched**. The schema field is metadata ABOUT an
///   auxiliary file; it does not enter `Data.db`, so no recorded corpus digest can move.
/// * The exhaustiveness check reads the **ARTIFACT's** key set and requires every key in it to
///   be compared or explicitly excused. A key the artifact does not yet carry is not a gap in
///   that direction, so the committed artifact stays valid AS THE 2026-08-03 RECORD IT IS.
/// * The DDL constant has **never changed**: measured across all three commits that have ever
///   touched `schema.rs` (`683e717f1`, `f1cd438a9`, `a8dbcfa2e`), `sha256(DDL + "\n")` is
///   `6bdd1d06…` in every one. So this value is not a NEW observation being blessed — it is the
///   digest the corpus was written with all along, now recorded.
///
/// The residual, stated rather than left to be discovered: the committed artifact carries no
/// `schema_sha256`, so it cannot corroborate this constant. The gate oracle is `DDL` itself,
/// which is stronger (it is the input, not a record of it). The next real corpus regeneration
/// will emit the field, and the exhaustiveness check will then require it to be compared —
/// which is the correct time for the artifact to acquire it.
///
/// # That residual had a cost nobody priced, and round 9's F1 is paying it
///
/// "The artifact cannot corroborate this constant" read as a documentation footnote. It was not:
/// round 8 (correctly) made an absent recorded `schema_sha256` a `PartialUnverified` with a
/// NON-ZERO exit, so the two facts together made [`operator_verify_corpus`] — the ONE command
/// every determinism claim rests on — **unable to succeed at all**, against the only artifact it
/// is ever pointed at. An operator who cannot obtain a green stops running the command, which is
/// the same broken-instrument failure as a guard that never fires, arrived at from the other side.
///
/// The fix does not touch the verdict. It uses the fact this doc comment already stated and then
/// filed away: the schema's expected digest needs no artifact, because it is pinned HERE.
/// `SourceOracles` carries THIS CONSTANT and `CorpusIdentity::compare_with_source_oracles` compares
/// the emitted schema against it — so the field is VERIFIED rather than unverified, against a
/// stronger oracle than a recorded value would have been. `PartialUnverified` stays reachable for a
/// field that genuinely has no oracle.
///
/// # IT IS THIS LITERAL, NOT `ddl_file_sha256()` (#3272 round 10, F-C)
///
/// Round 9 wired the oracle as `crate::schema::ddl_file_sha256()`, which is
/// `sha256(DDL + "\n")` — the SAME computation over the SAME `DDL` that produced the emitted
/// schema being checked. The two therefore moved together and agreed for every possible value of
/// `DDL`: a self-comparison that could not fail, and could not see the one thing the arm exists
/// for (a `DDL` edited since the prior corpus was recorded, which is exactly when the prior carries
/// no digest). This constant is a LITERAL and does not move when `DDL` moves, so a DDL change makes
/// the emitted digest DIVERGE from it.
///
/// The pin is not free to drift silently either:
/// `tests/measurement_corpus_pin.rs::the_pinned_schema_digest_is_the_digest_of_the_ddl_that_is_written`
/// asserts it equals `crate::schema::ddl_file_sha256()`, so a DDL change reds the suite and
/// re-pinning is an explicit reviewable act — never a silently relabelled oracle.
pub const SCHEMA_SHA256: &str = "6bdd1d06ad7eb597b3103ace250930b28b19a76aa128bbf2e4170c90406baed0";

/// Recorded total bytes across every emitted component.
pub const TOTAL_COMPONENT_BYTES: u64 = 2_779_185_469;

/// Cells per row (the twelve-column `ws0.events` shape).
pub const CELLS_PER_ROW: usize = 12;

/// The Arrow-buffer digest observed over the measurement corpus, at BOTH taps and
/// on BOTH arms (`bypass` == `merge`).
///
/// Recorded in `docs/reports/ws0-3096-artifacts/baseline-2026-08-03.md` §2. NOT
/// machine-checked — see [`operator_verify_digest`].
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
/// to #3096 and are not edited here). An operator re-running [`operator_verify_digest`]
/// gets 128 because that is what the oracle compiles; had this pin copied the
/// label, the re-run would have "failed" against a batch size nothing ever used.
pub const ARROW_BUFFER_BATCH_SIZE: u64 = 128;

/// The example scratch root used when no concrete one is known. Only ever appears in
/// documentation, never in a command an operator is told to run against a corpus that
/// exists somewhere else — see [`operator_verify_corpus`].
pub const EXAMPLE_VERIFY_ROOT: &str = "/data/ws0-3096-verify";

/// The exact command that regenerates the corpus into `root` and verifies it
/// reproduces the recorded identity. Minutes, ~2.8 GB — an operator step, never a
/// gate step.
///
/// PARAMETERIZED BY ROOT (#3272 review round 4). This used to be a `&'static str`
/// hardcoding `/data/ws0-3096-verify`, and the `#[ignore]`d operator procedure
/// printed it verbatim after generating into `CQLITE_WS0_VERIFY_ROOT` — so the
/// command an operator was handed named a directory the corpus was NOT in. A
/// procedure that cannot verify its own output is worse than none, because it looks
/// like it did.
///
/// # THIS COMMAND COULD NOT SUCCEED, AND THAT WAS A DEFECT OF THE SAME FAMILY (#3272 round 9, F1)
///
/// The artifact it names was recorded 2026-08-03, before [`SCHEMA_SHA256`]'s field existed, and
/// round 8 made an absent field a `PARTIAL` with a NON-ZERO exit. Correct on its own terms — an
/// unverified field must not print `PASS` — but in combination the documented command was
/// **permanently unable to succeed**, over any corpus, however perfectly it reproduced. A command
/// that always fails teaches an operator to stop running it, which loses the whole check.
///
/// It now succeeds on a reproducing corpus, with nothing skipped: the artifact's fields are
/// compared against the artifact, and the emitted schema is verified against the PINNED
/// [`SCHEMA_SHA256`] — a value that does not need the artifact to carry it, and (unlike a digest
/// recomputed from `schema::DDL`, which round 10's F-C removed) one that does not move when `DDL`
/// moves, so the comparison can actually fail.
pub fn operator_verify_corpus(root: &str) -> String {
    format!(
        "cargo run --release -p ws0-corpus-gen --bin ws0-corpus-gen -- \\\n\
         \x20 --out {root} --progress-every 0 \\\n\
         \x20 --verify-against docs/reports/ws0-3096-artifacts/corpus-identity.json\n\
         # exits non-zero, naming the divergent component, on ANY field difference; the emitted\n\
         # schema is verified against the PINNED measurement_corpus::SCHEMA_SHA256, which the\n\
         # 2026-08-03 artifact predates and therefore cannot corroborate"
    )
}

/// Env var carrying the EXPECTED Arrow-buffer digest into the Flight oracle (#3272 round 9, F2).
///
/// See [`operator_verify_digest`] for why the expectation travels through the environment rather
/// than as a `cqlite-flight` dependency on this crate.
pub const EXPECT_DIGEST_ENV: &str = "CQLITE_WS0_EXPECT_ARROW_DIGEST";

/// Env var carrying the EXPECTED `RecordBatch` count into the Flight oracle (#3272 round 9, F2).
pub const EXPECT_BATCHES_ENV: &str = "CQLITE_WS0_EXPECT_ARROW_BATCHES";

/// Env var carrying the EXPECTED row count into the Flight oracle (#3272 round 9, F2).
pub const EXPECT_ROWS_ENV: &str = "CQLITE_WS0_EXPECT_ARROW_ROWS";

/// The exact command that re-folds the Arrow-buffer digest over the corpus at
/// `root` AND COMPARES it against the pinned constants. Requires a corpus generated by
/// [`operator_verify_corpus`] first — and requires it to carry a `corpus-identity.json`, which is
/// why the operator procedure writes one.
///
/// # THE PIN WAS NEVER COMPARED AGAINST AN OBSERVATION (#3272 review round 9, F2)
///
/// This command used to run a test that only checked the two taps AGREED WITH EACH OTHER, and then
/// PRINTED the digest. So the oracle exited successfully if BOTH ARMS DRIFTED TOGETHER to a value
/// different from [`ARROW_BUFFER_DIGEST`] — and this crate's pinned digest and batch count were
/// compared against nothing at all, by anything, ever. A recorded value nobody checks, plus a
/// self-consistency check standing in for an oracle: this issue's own defect class (#3042's
/// lesson), reproduced inside the pin added to fix a neighbouring instance of it.
///
/// # Why the expectation travels through the ENVIRONMENT
///
/// `cqlite-flight` must NOT gain a dev-dependency on `ws0-corpus-gen` — its CI tests stay
/// self-contained via `tests/support/ws0_fixture.rs`, and depending on the corpus generator would
/// pull the whole write path into a Flight test build. So the constants are passed as EXPLICIT
/// VALUES through a validated interface: emitted HERE, FROM the constants themselves (never
/// retyped, so this command cannot state a stale expectation), and parsed fail-closed on the far
/// side — the Flight oracle REFUSES a corpus dir given without expectations, and refuses an
/// expectation it cannot parse, rather than degrading to printing a value nobody compares.
pub fn operator_verify_digest(root: &str) -> String {
    format!(
        "CQLITE_WS0_CORPUS_DIR={root} \\\n\
         \x20 {EXPECT_ROWS_ENV}={ROWS} \\\n\
         \x20 {EXPECT_BATCHES_ENV}={ARROW_BUFFER_BATCHES} \\\n\
         \x20 {EXPECT_DIGEST_ENV}=0x{ARROW_BUFFER_DIGEST:016x} \\\n\
         \x20 cargo test -p cqlite-flight --test issue_3096_arrow_buffer_digest -- --nocapture\n\
         # ASSERTS (it no longer merely PRINTS): {ROWS} rows in {ARROW_BUFFER_BATCHES} batches, and\n\
         # BOTH taps' digests equal 0x{ARROW_BUFFER_DIGEST:016x}. A corpus dir supplied WITHOUT these\n\
         # expectations is REFUSED — the oracle used to check only that the two taps agreed with\n\
         # EACH OTHER, so both arms drifting together exited 0 (#3272 round 9, F2)."
    )
}

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
        let corpus = operator_verify_corpus(EXAMPLE_VERIFY_ROOT);
        let digest = operator_verify_digest(EXAMPLE_VERIFY_ROOT);
        assert!(corpus.contains("ws0-corpus-gen"));
        assert!(corpus.contains("--verify-against"));
        assert!(corpus.contains("corpus-identity.json"));
        assert!(digest.contains("CQLITE_WS0_CORPUS_DIR"));
        assert!(digest.contains("issue_3096_arrow_buffer_digest"));
        // The digest procedure must state the expected batch count, so an operator
        // comparing output has the DERIVED number in front of them.
        assert!(
            digest.contains(&ARROW_BUFFER_BATCHES.to_string()),
            "the procedure must state the expected {ARROW_BUFFER_BATCHES} batches"
        );
    }

    /// THE DIGEST COMMAND MUST SUPPLY THE EXPECTATIONS (#3272 review round 9, F2).
    ///
    /// The Flight oracle only checked that its two taps agreed WITH EACH OTHER and then PRINTED
    /// the digest, so both arms drifting together exited 0 and [`ARROW_BUFFER_DIGEST`] /
    /// [`ARROW_BUFFER_BATCHES`] were compared against a real observation by NOTHING. The oracle now
    /// requires the pinned values and asserts against them — which only helps if the command an
    /// operator is handed actually PASSES them, and passes the values THIS pin holds.
    ///
    /// Asserted on the FORMATTED constants, so a future edit that retyped an expectation into the
    /// command string (the drift a second copy always invites) fails here.
    #[test]
    fn the_digest_procedure_supplies_the_pinned_expectations() {
        let digest = operator_verify_digest(EXAMPLE_VERIFY_ROOT);
        for var in [EXPECT_ROWS_ENV, EXPECT_BATCHES_ENV, EXPECT_DIGEST_ENV] {
            assert!(
                digest.contains(var),
                "the command must set {var}, or the oracle refuses the run: {digest}"
            );
        }
        // The VALUES, not merely the variable names — a command setting `…DIGEST=` to something
        // else would satisfy a name-only check while pinning nothing.
        assert!(
            digest.contains(&format!("{EXPECT_ROWS_ENV}={ROWS}")),
            "the command must pass the pinned {ROWS} rows: {digest}"
        );
        assert!(
            digest.contains(&format!("{EXPECT_BATCHES_ENV}={ARROW_BUFFER_BATCHES}")),
            "the command must pass the pinned {ARROW_BUFFER_BATCHES} batches: {digest}"
        );
        assert!(
            digest.contains(&format!("{EXPECT_DIGEST_ENV}=0x{ARROW_BUFFER_DIGEST:016x}")),
            "the command must pass the pinned digest 0x{ARROW_BUFFER_DIGEST:016x}: {digest}"
        );
        // ...and it must say it ASSERTS rather than prints, because that distinction IS the F2 fix
        // and an operator reading the command decides how much a green is worth.
        assert!(
            digest.contains("ASSERTS"),
            "the command's comment must state that it asserts, not merely prints: {digest}"
        );
    }

    /// BOTH commands must name THE ROOT THEY WERE ASKED ABOUT, and neither may name
    /// the example root when a different one was given (#3272 review round 4).
    ///
    /// This is the whole finding: the printed commands were `&'static str`s
    /// hardcoding `/data/ws0-3096-verify` and `/data/ws0-3096`, while the operator
    /// procedure generated into `CQLITE_WS0_VERIFY_ROOT` — so an operator following
    /// the printed commands verified a directory their corpus was not in. Asserted
    /// in both directions, because "contains the root" alone would pass on a string
    /// that ALSO still carried the hardcoded one.
    #[test]
    fn the_operator_procedures_name_the_root_they_were_given() {
        let root = "/scratch/somewhere-else";
        for cmd in [operator_verify_corpus(root), operator_verify_digest(root)] {
            assert!(cmd.contains(root), "the command must name {root}: {cmd}");
            assert!(
                !cmd.contains("/data/ws0-3096"),
                "the command must NOT name the hardcoded example root (#3272 round 4): {cmd}"
            );
        }
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
