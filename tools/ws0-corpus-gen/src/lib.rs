//! `ws0.events` PERFORMANCE-FIXTURE corpus generator (issue #3096, Phase 0).
//!
//! # What this is
//!
//! The reproduction rig issue #3096 names (`/home/ubuntu/ws0-local/`) does not
//! exist on any delivery box and was never committed, so no number in that issue
//! is reproducible. This crate is the committed replacement: it drives the
//! PRODUCTION [`cqlite_core::storage::sstable::writer::SSTableWriter`] — not a
//! test helper and not a hand-rolled byte emitter — from the pinned `ws0.events`
//! DDL, deterministically from a recorded seed, and records the resulting
//! corpus's OWN identity (`sha256`, row/partition counts, on-disk byte shape)
//! in-tree.
//!
//! # THIS CORPUS IS A PERFORMANCE FIXTURE ONLY — NEVER A CORRECTNESS ORACLE
//!
//! The corpus is **CQLite-written and CQLite-read**. Per issue #3042 that round
//! trip is INVARIANT to a uniform framing/serialization error: both sides make
//! the identical mistake, the round trip closes, and the test stays green while
//! real Cassandra-written data would read wrong. A symmetric round trip cannot
//! detect two defects that cancel — that is not a gap in this generator, it is a
//! property of the construction.
//!
//! Therefore:
//!
//! * **No on-disk framing or encoding correctness claim may rest on this
//!   corpus.** Not row/cell framing, not VInt encoding, not the index or
//!   statistics layout, not compression.
//! * Correctness stays anchored to the **Cassandra-written** fixtures
//!   (`test-data/datasets/`, the `nb`/`da` goldens, the sstabledump JSONL
//!   references) and to the oracles listed in
//!   `openspec/changes/arrow-encode-doget/design.md` §"Correctness pinning stack".
//! * What this corpus IS good for: holding the BYTES CONSTANT across two
//!   measurement arms in one session, at a size (4,000,000 rows) that makes a
//!   per-row cycle cost measurable.
//!
//! # Uncompressed by construction (issue #1406)
//!
//! CQLite's production write surface emits UNCOMPRESSED SSTables only and never
//! a `CompressionInfo.db`. The generator asserts the absence of that component
//! rather than assuming it. The historical #3100 corpus was Cassandra-written and
//! LZ4-compressed, which is why this corpus cannot and does not reproduce that
//! corpus's absolute numbers — see `identity.rs`.

pub mod generate;
pub mod identity;
pub mod rows;
pub mod schema;

/// Deterministic, portable PRNG (SplitMix64).
///
/// Deliberately hand-rolled rather than taken from the `rand` crate: corpus
/// determinism is a COMMITTED property asserted by re-running the generator and
/// comparing `sha256`, so the byte stream must not depend on a dependency's
/// choice of generator or on a version bump changing it. SplitMix64 is fully
/// specified by the three constants below.
#[derive(Debug, Clone)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    /// Seed the generator. Any `u64` (including 0) is a valid seed.
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Next 64 pseudo-random bits.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Fill `buf` with pseudo-random bytes.
    pub fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let word = self.next_u64().to_le_bytes();
            let n = chunk.len();
            chunk.copy_from_slice(&word[..n]);
        }
    }

    /// A value in `0..n` (`n > 0`). Modulo bias is irrelevant here: this picks
    /// fixture labels from small fixed tables, not cryptographic material.
    pub fn below(&mut self, n: u64) -> u64 {
        debug_assert!(n > 0, "below(0) has no valid result");
        self.next_u64() % n.max(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The generator is a pure function of its seed: the same seed replays the
    /// same stream, and different seeds diverge. This is the property the
    /// committed corpus `sha256` rests on.
    #[test]
    fn splitmix64_is_seed_deterministic() {
        let a: Vec<u64> = (0..8)
            .scan(SplitMix64::new(42), |r, _| Some(r.next_u64()))
            .collect();
        let b: Vec<u64> = (0..8)
            .scan(SplitMix64::new(42), |r, _| Some(r.next_u64()))
            .collect();
        let c: Vec<u64> = (0..8)
            .scan(SplitMix64::new(43), |r, _| Some(r.next_u64()))
            .collect();
        assert_eq!(a, b, "the same seed must replay the same stream");
        assert_ne!(a, c, "a different seed must produce a different stream");
        // Pinned first word for seed 42 — a change to the constants (or a switch
        // to a library RNG) would silently invalidate every recorded corpus
        // digest; this catches it at compile-test time instead.
        assert_eq!(a[0], 0xBDD7_3226_2FEB_6E95);
    }

    /// `fill` must cover a non-multiple-of-8 tail (the blob/uuid widths are 96
    /// and 16, but a future width change must not silently leave zeros).
    #[test]
    fn fill_covers_a_ragged_tail() {
        let mut r = SplitMix64::new(7);
        let mut buf = [0u8; 13];
        r.fill(&mut buf);
        assert!(
            buf.iter().any(|b| *b != 0),
            "fill must write the whole buffer"
        );
    }
}
