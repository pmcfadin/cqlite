//! Issue #4497 — property tests for the reconciliation LAWS of the real
//! `KWayMerger::merge_partition_rows`: commutative, associative, idempotent.
//!
//! * [`generators`] — seeded, reusable stream generators (the `#4257` precursor: a
//!   fixed palette of partition/clustering/column slots, a `Clean` profile whose equal-
//!   timestamp ties are RESOLVABLE and a `Wild` profile that also produces the ambiguous
//!   ties). Kept free of assertions so `#4257` can reuse them.
//! * [`laws`] — the three laws over the `Clean` profile, with and without TTL expiry.
//! * [`ties`] — one directed property per equal-timestamp tie class, each resolved
//!   against Cassandra 5.0.8 (`Cells.resolveRegular`, `DeletionTime.supersedes`).
//!
//! The harness config (bounded PR tier, `PROPTEST_CASES` nightly knob, printed seed) is
//! `tests/support/proptest_cfg.rs`, shared with the byte-comparable integration test.

#[path = "../../../../../tests/support/proptest_cfg.rs"]
pub(super) mod proptest_cfg;

mod generators;
mod laws;
mod ties;
