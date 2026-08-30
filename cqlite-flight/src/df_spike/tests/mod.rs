//! Tests for the DataFusion spike (issue #2605).
//!
//! # The oracle these tests use, and why a CQLite-written fixture is legitimate
//!
//! `CLAUDE.md` is emphatic that a CQLite-written + CQLite-read round trip is
//! invariant to a uniform framing error and can never validate an ON-DISK
//! encoding property. That rule does not apply to the property under test here.
//! The property is **arm equivalence**: two EXECUTION engines, reading the SAME
//! bytes through the SAME decoder, must return the same rows in the same order.
//! Both arms share the whole read path, so a decode defect would move both
//! answers identically and could not manufacture a pass — exactly the shape of
//! the `#1918` point-vs-full differential lane and the `#3058` forced-path
//! differential lane, both of which use in-process fixtures for the same reason.
//!
//! The thing these tests exist to stop is a `TableProviderFilterPushDown::Exact`
//! claim for a predicate the scan does not actually apply. That would drop or
//! keep the wrong rows and make the DataFusion arm look FASTER by being WRONG —
//! the one failure mode that would invalidate the whole measurement.
//!
//! `now` is PINNED to a constant (`#2642`: never a wall-clock read), and the
//! fixture carries no TTL, so the read is deterministic.

//!
//! # Layout
//!
//! Split by responsibility (campsite rule, #1116/#1135) — one file per property
//! under test, with the fixtures and helpers they share in [`support`]:
//!
//! * [`support`] — the two-generation fixture, the pinned read clock, and the
//!   row-rendering/arm-driving helpers every file below uses.
//! * [`pushdown`] — `Expr` classification: `Exact` only for what the scan really
//!   applies.
//! * [`provider`] — the `TableProvider` surface: schema, projection, fail-closed
//!   open.
//! * [`equivalence`] — the arm-equivalence oracle, the guard against "faster
//!   because wrong".
//! * [`harness`] — the bench harness's own contract: operand parsing, scenario
//!   and arm identifiers, the SQL each scenario runs, and what a result record
//!   reports.

mod equivalence;
mod harness;
mod provider;
mod pushdown;
mod support;
