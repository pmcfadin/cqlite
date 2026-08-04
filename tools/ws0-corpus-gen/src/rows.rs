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

/// Build the [`Mutation`] for row `r` of partition `p`.
///
/// One mutation per clustering row: `PRIMARY KEY (part_id, seq, event_time)` with
/// nine non-key columns written as simple cells. `global_row` orders the write
/// timestamps deterministically across the whole corpus.
pub fn row_mutation(seed: u64, p: u64, r: u64, global_row: u64) -> Mutation {
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

    let mut payload = vec![0u8; PAYLOAD_LEN];
    rng.fill(&mut payload);
    for b in payload.iter_mut() {
        *b = PAYLOAD_ALPHABET[(*b as usize) % PAYLOAD_ALPHABET.len()];
    }
    let payload = String::from_utf8(payload).unwrap_or_else(|_| {
        // Unreachable: every byte was just mapped into a 32-char ASCII alphabet.
        // Kept total rather than panicking so a future alphabet edit degrades to a
        // visibly-wrong-but-valid fixture instead of aborting a 4M-row run.
        "a".repeat(PAYLOAD_LEN)
    });

    let region = REGIONS[rng.below(REGIONS.len() as u64) as usize];
    let status = STATUSES[rng.below(STATUSES.len() as u64) as usize];

    let event_time = EVENT_TIME_BASE_MS + (r as i64) * 1_000;

    Mutation::new(
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
    )
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

    /// Row content is a pure function of `(seed, p, r)` — the property the
    /// committed corpus `sha256` rests on. Compared through the debug rendering
    /// so EVERY field participates.
    #[test]
    fn row_content_is_deterministic_in_seed_p_r() {
        let a = format!("{:?}", row_mutation(11, 3, 7, 307).operations);
        let b = format!("{:?}", row_mutation(11, 3, 7, 307).operations);
        assert_eq!(a, b, "same (seed,p,r) must yield identical cells");
        assert_ne!(
            a,
            format!("{:?}", row_mutation(12, 3, 7, 307).operations),
            "a different seed must yield different cells"
        );
        assert_ne!(
            a,
            format!("{:?}", row_mutation(11, 4, 7, 407).operations),
            "a different partition must yield different cells"
        );
        assert_ne!(
            a,
            format!("{:?}", row_mutation(11, 3, 8, 308).operations),
            "a different clustering row must yield different cells"
        );
    }

    /// Nine non-key cells per row; with the three PK columns that is the
    /// twelve-cell `ws0.events` row shape the WS0 method records.
    #[test]
    fn each_row_writes_nine_non_key_cells() {
        let m = row_mutation(1, 0, 0, 0);
        assert_eq!(m.operations.len(), 9);
        assert_eq!(crate::schema::COLUMNS.len(), m.operations.len() + 3);
    }

    /// The synthesized widths must actually be the declared ones (a silent width
    /// change would move bytes/row and invalidate the recorded identity).
    #[test]
    fn cell_widths_match_the_declared_constants() {
        let m = row_mutation(5, 5, 5, 5);
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
            let m = row_mutation(9, r % 7, r, r);
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
}
