//! Deterministic `ws0.events` row synthesis.
//!
//! Every field of every row is a pure function of `(seed, partition_index,
//! row_index)`, so the corpus is byte-reproducible from the recorded seed alone
//! and is INDEPENDENT of the order partitions happen to be written in (which is
//! Murmur3 token order, decided after the keys exist).
//!
//! The widths below are chosen to land near the **692.70 uncompressed B/row** of
//! the #3026/#3100 Cassandra corpus (`docs/reports/ws0-3026-artifacts/ws0-results/
//! head-to-head-method.md` §1), so the per-row cycle cost is measured against a
//! comparable row weight. The ACHIEVED bytes/row is measured from the emitted
//! `Data.db` and recorded in the corpus identity — never assumed from these
//! constants.

use cqlite_core::storage::write_engine::mutation::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId,
};
use cqlite_core::types::Value;

use crate::schema::{KEYSPACE, TABLE};
use crate::SplitMix64;

/// `blob_a` width in bytes (matches the #3026 corpus profile's `ba96`).
pub const BLOB_A_LEN: usize = 96;
/// `blob_b` width in bytes (matches the #3026 corpus profile's `bb96`).
pub const BLOB_B_LEN: usize = 96;
/// `payload` width in characters.
///
/// The #3026 corpus profile used 375; this generator uses 414 because it must hit
/// the MEASURED target — 692.70 uncompressed B/row — not copy a width from a
/// different (Cassandra) writer's framing. 375 measured 652.99 B/row here. The
/// ACHIEVED value is recomputed from the emitted `Data.db` and recorded in the
/// corpus identity; this constant is an input, never the claim.
pub const PAYLOAD_LEN: usize = 414;

/// Base of the synthetic `event_time` clustering value, in milliseconds since the
/// epoch. A FIXED constant (2023-11-14T22:13:20Z), never wall-clock: a
/// wall-clock-derived value would make the corpus non-reproducible.
pub const EVENT_TIME_BASE_MS: i64 = 1_700_000_000_000;

/// Base write timestamp in MICROseconds. Also fixed — the writer folds it into
/// `Statistics.db` min/max, so a wall-clock value would change the on-disk bytes
/// run to run.
pub const WRITE_TS_BASE_MICROS: i64 = 1_700_000_000_000_000;

/// The `region` label set (a low-cardinality text column, as in the WS0 shape).
const REGIONS: [&str; 6] = [
    "us-east-1",
    "us-west-2",
    "eu-west-1",
    "eu-central-1",
    "ap-south-1",
    "ap-northeast-1",
];

/// The `status` label set.
const STATUSES: [&str; 4] = ["OK", "WARN", "ERROR", "UNKNOWN"];

/// Printable alphabet for `payload`, so a corpus dump stays greppable.
const PAYLOAD_ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz012345";

/// A row could not be synthesized as specified (issue #3272 review).
///
/// There is exactly one variant, and it does not arise while [`PAYLOAD_ALPHABET`] stays
/// ASCII — which is the point. The alternative was a silent substitution
/// (`unwrap_or_else(|_| "a".repeat(PAYLOAD_LEN))`), and a fixture that quietly changes
/// its own CONTENT is strictly worse than one that stops: the substituted corpus would
/// still generate, still be deterministic, still record a digest, and that digest would
/// become the pin every subsequent measurement compared against.
///
/// # It is REACHABLE, and it is driven (issue #3272 review round 2 nit)
///
/// "Unreachable variant" and "variant no input can produce" are different claims, and the
/// round-1 test proved only the weaker one: it CONSTRUCTED the variant by hand and asserted
/// its `Display`, so the new `Result` surface was exercised for its formatting and for
/// nothing else. The construction SITE — the `map_err` in the real synthesis — was never
/// run, which is the half that matters: a variant whose construction site is unexercised
/// can carry the wrong coordinates (`row` where `partition` was meant, an index from the
/// wrong loop) and every test still passes.
///
/// So the payload synthesis takes its alphabet as a PARAMETER
/// ([`synth_payload`]), `row_mutation` passes [`PAYLOAD_ALPHABET`], and
/// [`tests::the_real_synthesis_path_errors_on_a_non_ascii_alphabet`] passes a non-ASCII one
/// — driving the actual code that builds this error, from actually-synthesized bytes, and
/// checking that the coordinates it reports are the ones it was called with. The variant is
/// therefore reachable BY CONSTRUCTION, and what keeps it from arising in production is a
/// pinned constant rather than an inability to occur.
#[derive(Debug)]
pub enum RowSynthError {
    /// The `payload` bytes did not decode as UTF-8, i.e. [`PAYLOAD_ALPHABET`] was
    /// edited to include a non-ASCII byte.
    PayloadNotUtf8 {
        /// Partition index of the offending row.
        partition: u64,
        /// Clustering-row index within the partition.
        row: u64,
        /// The decode failure.
        source: std::string::FromUtf8Error,
    },
}

impl std::fmt::Display for RowSynthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PayloadNotUtf8 {
                partition,
                row,
                source,
            } => write!(
                f,
                "the synthesized `payload` for partition {partition} row {row} is not \
                 valid UTF-8 ({source}). PAYLOAD_ALPHABET must stay ASCII: this used to \
                 substitute {PAYLOAD_LEN} copies of 'a' instead, which changes the \
                 corpus CONTENT on a path whose whole purpose is byte identity — the \
                 corpus would still generate, still be deterministic, and its digest \
                 would silently become the new pin (#3272)"
            ),
        }
    }
}

impl std::error::Error for RowSynthError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::PayloadNotUtf8 { source, .. } => Some(source),
        }
    }
}

/// The partition key text for partition `p`. Fixed width (`p` + 7 digits) so
/// every partition key serializes to the same length, keeping the token
/// distribution and the on-disk key bytes uniform.
pub fn part_id(p: u64) -> String {
    format!("p{p:07}")
}

/// The per-row PRNG, seeded from `(seed, p, r)` by mixing the three through
/// SplitMix64's own avalanche so adjacent rows do not share a stream prefix.
fn row_rng(seed: u64, p: u64, r: u64) -> SplitMix64 {
    let mut mixer = SplitMix64::new(seed ^ p.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    // Advance by a value derived from `r` without a loop over `r` (a per-row loop
    // would make generation O(rows_per_partition^2)).
    let base = mixer.next_u64();
    SplitMix64::new(base ^ r.wrapping_mul(0xD6E8_FEB8_6659_FD93))
}

/// Build the [`Mutation`] for row `r` of partition `p`, or an error.
///
/// One mutation per clustering row: `PRIMARY KEY (part_id, seq, event_time)` with
/// nine non-key columns written as simple cells. `global_row` orders the write
/// timestamps deterministically across the whole corpus.
///
/// # Why this returns a `Result` (issue #3272 review)
///
/// The `payload` synthesis ends in `String::from_utf8(bytes)`, which cannot fail while
/// [`PAYLOAD_ALPHABET`] is ASCII — but it USED to be written
/// `.unwrap_or_else(|_| "a".repeat(PAYLOAD_LEN))`, silently SUBSTITUTING corpus CONTENT
/// on a path whose entire purpose is byte identity. The stated justification was that a
/// future alphabet edit should "degrade to a visibly-wrong-but-valid fixture instead of
/// aborting a 4M-row run", which inverts the priority: a corpus whose payload column is
/// 414 copies of `a` is not visibly wrong, it is *plausible*, and its `Data.db` sha256
/// would become the new pin. Every figure measured against that corpus would then be
/// measured against silently different bytes — the whole failure class #3272 exists to
/// close, in the fixture rather than the reporter.
///
/// So it errors. And erroring is why the signature is a `Result` rather than a panic:
/// this crate's library code carries no `unwrap`/`expect`, and a 4M-row run is exactly
/// the case where an actionable message beats an abort.
/// The `payload` column's bytes, mapped onto `alphabet`, decoded as UTF-8.
///
/// The alphabet is a PARAMETER purely so the error path is REACHABLE and can be driven from
/// actually-synthesized bytes (issue #3272 review round 2 nit) — `row_mutation` always
/// passes [`PAYLOAD_ALPHABET`], and nothing else calls this outside the tests. Making the
/// construction site of [`RowSynthError::PayloadNotUtf8`] executable is the point: a variant
/// built only by hand in a test can report the wrong coordinates forever.
///
/// `alphabet` must be non-empty; an empty one would divide by zero in the modulo. The
/// caller is a pinned constant, so this is an invariant rather than a validated input, and
/// the assertion is a debug one for that reason.
fn synth_payload(
    rng: &mut SplitMix64,
    alphabet: &[u8],
    p: u64,
    r: u64,
) -> Result<String, RowSynthError> {
    debug_assert!(
        !alphabet.is_empty(),
        "the payload alphabet must be non-empty"
    );
    let mut payload = vec![0u8; PAYLOAD_LEN];
    rng.fill(&mut payload);
    for b in payload.iter_mut() {
        *b = alphabet[(*b as usize) % alphabet.len()];
    }
    String::from_utf8(payload).map_err(|e| RowSynthError::PayloadNotUtf8 {
        partition: p,
        row: r,
        source: e,
    })
}

pub fn row_mutation(seed: u64, p: u64, r: u64, global_row: u64) -> Result<Mutation, RowSynthError> {
    let mut rng = row_rng(seed, p, r);

    let mut blob_a = vec![0u8; BLOB_A_LEN];
    rng.fill(&mut blob_a);
    let mut blob_b = vec![0u8; BLOB_B_LEN];
    rng.fill(&mut blob_b);
    let mut device_id = [0u8; 16];
    rng.fill(&mut device_id);

    let metric_a = rng.next_u64() as u32 as i32;
    let metric_b = rng.next_u64() as i64;
    // A finite double with no NaN/Inf: those would be legal CQL but would make an
    // Arrow-buffer digest sensitive to NaN bit patterns for no measurement value.
    let metric_c = (rng.next_u64() % 1_000_000_000) as f64 / 1_000.0;

    let payload = synth_payload(&mut rng, PAYLOAD_ALPHABET, p, r)?;

    let region = REGIONS[rng.below(REGIONS.len() as u64) as usize];
    let status = STATUSES[rng.below(STATUSES.len() as u64) as usize];

    let event_time = EVENT_TIME_BASE_MS + (r as i64) * 1_000;

    Ok(Mutation::new(
        TableId::new(KEYSPACE, TABLE),
        PartitionKey::single("part_id", Value::text(part_id(p))),
        Some(ClusteringKey::new(vec![
            ("seq".to_string(), Value::Integer(r as i32)),
            ("event_time".to_string(), Value::Timestamp(event_time)),
        ])),
        vec![
            write("blob_a", Value::Blob(blob_a.into())),
            write("blob_b", Value::Blob(blob_b.into())),
            write("device_id", Value::Uuid(device_id)),
            write("metric_a", Value::Integer(metric_a)),
            write("metric_b", Value::BigInt(metric_b)),
            write("metric_c", Value::Float(metric_c)),
            write("payload", Value::text(payload)),
            write("region", Value::text(region)),
            write("status", Value::text(status)),
        ],
        WRITE_TS_BASE_MICROS + global_row as i64,
        None,
    ))
}

fn write(column: &str, value: Value) -> CellOperation {
    CellOperation::Write {
        column: column.to_string(),
        value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // For the FULL-payload pin in `the_synth_payload_extraction_did_not_change_the_bytes`
    // (#3272 review round 3 nit): a prefix cannot see a tail-only rng divergence.
    use sha2::{Digest, Sha256};

    /// Row content is a pure function of `(seed, p, r)` — the property the
    /// committed corpus `sha256` rests on. Compared through the debug rendering
    /// so EVERY field participates.
    #[test]
    fn row_content_is_deterministic_in_seed_p_r() {
        let a = format!(
            "{:?}",
            row_mutation(11, 3, 7, 307)
                .expect("ASCII payload")
                .operations
        );
        let b = format!(
            "{:?}",
            row_mutation(11, 3, 7, 307)
                .expect("ASCII payload")
                .operations
        );
        assert_eq!(a, b, "same (seed,p,r) must yield identical cells");
        assert_ne!(
            a,
            format!(
                "{:?}",
                row_mutation(12, 3, 7, 307)
                    .expect("ASCII payload")
                    .operations
            ),
            "a different seed must yield different cells"
        );
        assert_ne!(
            a,
            format!(
                "{:?}",
                row_mutation(11, 4, 7, 407)
                    .expect("ASCII payload")
                    .operations
            ),
            "a different partition must yield different cells"
        );
        assert_ne!(
            a,
            format!(
                "{:?}",
                row_mutation(11, 3, 8, 308)
                    .expect("ASCII payload")
                    .operations
            ),
            "a different clustering row must yield different cells"
        );
    }

    /// Nine non-key cells per row; with the three PK columns that is the
    /// twelve-cell `ws0.events` row shape the WS0 method records.
    #[test]
    fn each_row_writes_nine_non_key_cells() {
        let m = row_mutation(1, 0, 0, 0).expect("ASCII payload");
        assert_eq!(m.operations.len(), 9);
        assert_eq!(crate::schema::COLUMNS.len(), m.operations.len() + 3);
    }

    /// The synthesized widths must actually be the declared ones (a silent width
    /// change would move bytes/row and invalidate the recorded identity).
    #[test]
    fn cell_widths_match_the_declared_constants() {
        let m = row_mutation(5, 5, 5, 5).expect("ASCII payload");
        for op in &m.operations {
            let CellOperation::Write { column, value } = op else {
                panic!("only Write ops are synthesized, got {op:?}");
            };
            match (column.as_str(), value) {
                ("blob_a", Value::Blob(b)) => assert_eq!(b.len(), BLOB_A_LEN),
                ("blob_b", Value::Blob(b)) => assert_eq!(b.len(), BLOB_B_LEN),
                ("payload", Value::Text(t)) => assert_eq!(t.len(), PAYLOAD_LEN),
                ("device_id", Value::Uuid(u)) => assert_eq!(u.len(), 16),
                _ => {}
            }
        }
    }

    /// `metric_c` must be finite for every row in a decent-sized sample: a
    /// NaN/Inf would make the Arrow-buffer digest bit-pattern-sensitive.
    #[test]
    fn metric_c_is_always_finite() {
        for r in 0..500u64 {
            let m = row_mutation(9, r % 7, r, r).expect("ASCII payload");
            for op in &m.operations {
                if let CellOperation::Write {
                    column,
                    value: Value::Float(f),
                } = op
                {
                    assert!(f.is_finite(), "{column} row {r} produced {f}");
                }
            }
        }
    }

    /// Partition keys are fixed-width and unique per index.
    #[test]
    fn part_ids_are_fixed_width_and_unique() {
        assert_eq!(part_id(0), "p0000000");
        assert_eq!(part_id(39_999), "p0039999");
        assert_eq!(part_id(0).len(), part_id(39_999).len());
    }
    /// NON-VACUITY for [`RowSynthError`], driven through the REAL SYNTHESIS PATH
    /// (issue #3272 review round 2 nit).
    ///
    /// The round-1 test CONSTRUCTED the variant by hand and asserted its `Display`, so the
    /// new `Result` surface was proven for its formatting and nothing else — the
    /// construction SITE, the `map_err` inside the synthesis, was never executed. That is
    /// the half that matters: a variant whose construction site is unexercised can carry the
    /// wrong coordinates (`row` where `partition` was meant, an index from the wrong loop)
    /// and every assertion still passes.
    ///
    /// So `synth_payload` takes the alphabet as a parameter and this drives it with a
    /// NON-ASCII one. The bytes are genuinely synthesized, the error is genuinely
    /// constructed by the code that runs in production, and the coordinates it reports are
    /// checked against the ones it was called with.
    ///
    /// The pre-fix code did NOT have an error path at all: `String::from_utf8(payload)
    /// .unwrap_or_else(|_| "a".repeat(PAYLOAD_LEN))` returned a Mutation whose `payload`
    /// column was 414 copies of `a`. That corpus generates, is deterministic, records a
    /// digest — and that digest silently becomes the pin every later measurement is
    /// compared against.
    #[test]
    fn the_real_synthesis_path_errors_on_a_non_ascii_alphabet() {
        // A 2-byte alphabet, both bytes ILLEGAL as standalone UTF-8: 0xFF is never valid,
        // and 0x80 is a continuation byte, so no arrangement of them decodes.
        const NOT_UTF8: &[u8] = &[0xff, 0x80];
        let mut rng = row_rng(11, 17, 42);
        let err = synth_payload(&mut rng, NOT_UTF8, 17, 42)
            .expect_err("a non-UTF-8 alphabet must make the real synthesis path ERROR");

        // THE COORDINATES, read off the error the production code built — the property a
        // hand-constructed variant can never establish.
        let RowSynthError::PayloadNotUtf8 { partition, row, .. } = &err;
        assert_eq!(
            *partition, 17,
            "the error must carry the partition it was given"
        );
        assert_eq!(
            *row, 42,
            "the error must carry the clustering row it was given"
        );

        let msg = err.to_string();
        assert!(msg.contains("partition 17"), "{msg}");
        assert!(msg.contains("row 42"), "{msg}");
        assert!(
            msg.contains("PAYLOAD_ALPHABET"),
            "the message must name the constant to fix: {msg}"
        );
        assert!(
            msg.contains("byte identity"),
            "the message must say WHY a substitution is unacceptable here: {msg}"
        );
        // The underlying decode failure is preserved, not flattened into a string.
        assert!(
            std::error::Error::source(&err).is_some(),
            "the UTF-8 error must be retained as the source"
        );
        // And the SAME call with the production alphabet SUCCEEDS — so the failure above is
        // attributable to the alphabet rather than to anything else about the call.
        let mut rng = row_rng(11, 17, 42);
        assert!(
            synth_payload(&mut rng, PAYLOAD_ALPHABET, 17, 42).is_ok(),
            "the production alphabet must succeed on the identical call"
        );
    }

    /// The refactor that made the error path reachable did NOT change corpus CONTENT.
    ///
    /// Extracting `synth_payload` moved the rng calls behind a function boundary, and the
    /// pinned `Data.db` sha256 (`4a903f6f…`, `measurement_corpus::DATA_DB_SHA256`) is the
    /// digest of the bytes the OLD code produced. A refactor that shifted the payload by one
    /// rng draw would still generate, still be deterministic, still record a digest — and
    /// silently invalidate the pin, which is the failure mode this whole issue exists to
    /// close, arriving via a change made to close it.
    ///
    /// So the payload for one fixed coordinate is pinned against the value MEASURED from the
    /// pre-refactor code. The whole-corpus property remains pinned by
    /// `measurement_corpus_pin.rs`.
    ///
    /// # PROVENANCE, RE-DERIVED (#3272 review round 3 nit)
    ///
    /// The nit was that this constant's provenance is unverifiable from the diff: a value
    /// measured POST-refactor is textually identical to one measured PRE-refactor, and a
    /// circular pin is exactly the class this issue exists to close. So it was re-derived
    /// independently, at the parent commit, and the command and its output are recorded here
    /// so a reader can repeat it rather than trust it:
    ///
    /// ```text
    /// # `c4fb5a8e6` is the commit BEFORE `a69b17ed6` (which extracted synth_payload).
    /// # At c4fb5a8e6 `synth_payload` does not exist: row_mutation inlines the synthesis.
    /// git worktree add --detach /tmp/pre-refactor-wt c4fb5a8e6
    /// # ...append a test to tools/ws0-corpus-gen/src/rows.rs printing the payload for
    /// # row_mutation(11, 17, 42, 1742), then:
    /// cargo test -q -p ws0-corpus-gen --lib pre_refactor_derivation -- --nocapture
    ///
    /// PRE_REFACTOR_FULL_LEN=414
    /// PRE_REFACTOR_PREFIX_48=wuoabdjlxsgeduci1md0l12nhq0a1un2tyif4i10aw052mjx
    /// PRE_REFACTOR_SHA256_OF_FULL=a7f8a27d56cf702e990817c9069d12c01e497f44e4e88c623c1f544205f54f2c
    /// ```
    ///
    /// Both values below are that output. The SHA-256 is pinned ALONGSIDE the prefix
    /// because a prefix is a WEAKER pin than it looks: a change to the rng stream that
    /// happens to leave the first 48 characters intact — a later draw reordered, a
    /// different consumption in the tail — would satisfy the prefix and still invalidate
    /// `DATA_DB_SHA256`. The digest covers all 414 bytes and costs nothing.
    #[test]
    fn the_synth_payload_extraction_did_not_change_the_bytes() {
        /// MEASURED at the pre-refactor parent commit `c4fb5a8e6` — see the derivation
        /// command and output in this test's doc comment.
        const PRE_REFACTOR_PREFIX: &str = "wuoabdjlxsgeduci1md0l12nhq0a1un2tyif4i10aw052mjx";
        /// The sha256 of the WHOLE 414-byte payload, from the same derivation. A prefix
        /// alone would admit a change confined to the tail.
        const PRE_REFACTOR_SHA256: &str =
            "a7f8a27d56cf702e990817c9069d12c01e497f44e4e88c623c1f544205f54f2c";

        let m = row_mutation(11, 17, 42, 1742).expect("the pinned alphabet is ASCII");
        let payload = m
            .operations
            .iter()
            .find_map(|op| match op {
                CellOperation::Write {
                    column,
                    value: Value::Text(t),
                } if column == "payload" => Some(t.clone()),
                _ => None,
            })
            .expect("a payload cell is written");
        let text = String::from_utf8(payload.to_vec()).expect("the payload is ASCII");
        assert_eq!(text.len(), PAYLOAD_LEN);
        assert!(
            text.starts_with(PRE_REFACTOR_PREFIX),
            "extracting synth_payload CHANGED the bytes row_mutation produces. The pinned \
             Data.db digest (measurement_corpus::DATA_DB_SHA256) is the digest of the old \
             bytes, so this refactor would silently invalidate it — the exact failure class \
             #3272 exists to close, arriving via a change made to close it.\n  expected \
             prefix: {PRE_REFACTOR_PREFIX}\n  got:             {}",
            &text[..PRE_REFACTOR_PREFIX.len().min(text.len())]
        );
        // ...and the WHOLE payload, because a prefix cannot see a tail-only divergence
        // (#3272 review round 3 nit). Same derivation, same command, recorded above.
        let mut hasher = Sha256::new();
        hasher.update(text.as_bytes());
        let digest = format!("{:x}", hasher.finalize());
        assert_eq!(
            digest,
            PRE_REFACTOR_SHA256,
            "the payload's first {} characters match the pre-refactor value but its FULL \
             {} bytes do not, so the rng stream diverged somewhere after the prefix — a \
             change the prefix check alone cannot see, and one that still invalidates \
             measurement_corpus::DATA_DB_SHA256.",
            PRE_REFACTOR_PREFIX.len(),
            text.len()
        );
    }

    /// The ACCEPT direction, and the reason the error is unreachable in practice: the
    /// alphabet is ASCII, so every synthesized payload decodes. A future edit that
    /// broke this would fail HERE with a clear cause, before it could reach a 4M-row
    /// run and silently substitute content.
    #[test]
    fn the_payload_alphabet_is_ascii_so_synthesis_cannot_substitute() {
        assert!(
            PAYLOAD_ALPHABET.iter().all(|b| b.is_ascii()),
            "PAYLOAD_ALPHABET must stay ASCII: a non-ASCII byte would make \
             String::from_utf8 fail for real rows, which now ERRORS (it used to \
             substitute {PAYLOAD_LEN} copies of 'a' and change the corpus content)"
        );
        let m = row_mutation(3, 1, 1, 101).expect("an ASCII alphabet cannot fail");
        let payload = m.operations.iter().find_map(|op| match op {
            CellOperation::Write {
                column,
                value: Value::Text(t),
            } if column == "payload" => Some(t.clone()),
            _ => None,
        });
        let payload = payload.expect("a payload cell is written");
        assert_eq!(payload.len(), PAYLOAD_LEN);
        assert!(
            payload.iter().all(|b| PAYLOAD_ALPHABET.contains(b)),
            "every payload byte must come from the pinned alphabet"
        );
        // The substituted value the pre-fix code would have produced must NOT be what a
        // real row looks like — otherwise the substitution would have been undetectable.
        assert_ne!(
            payload,
            "a".repeat(PAYLOAD_LEN),
            "a real payload must differ from the old substitution value"
        );
    }
}
