//! `flight-loadgen` — a raw `FlightServiceClient` concurrency-ramp load
//! generator for `cqlite-flight` (issue #2418, epic #2313 WS1).
//!
//! This library holds the reusable pieces the `flight-loadgen` binary and its
//! wiring-evidence self-test both drive:
//!
//! - [`shape`] — base-template + seeded shape transforms (deterministic tickets),
//! - [`classify`] — ok / unavailable (#2420 admission shed) / error classification,
//! - [`record`] — the `flight-loadgen.step/v1` JSONL step record + accumulator,
//! - [`client`] — raw `FlightServiceClient` connect + memory-bounded `do_get` drain,
//! - [`ramp`] — the concurrency-ramp engine (per-step worker pool),
//! - [`selftest`] — the in-process ephemeral-port self-test harness.
//!
//! It measures throughput/latency/shedding of the SERVER directly (no Trino, no
//! JDBC, no `cqlite-core` query engine on the client path) — the "server-direct
//! ceiling" underneath the through-Trino floor. It is NOT a correctness oracle.

pub mod classify;
pub mod client;
pub mod ramp;
pub mod record;
pub mod selftest;
pub mod shape;
