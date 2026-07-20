# do_get error taxonomy — fine-grained abort categories + abort-path trace

## Milestone
0.15 (cqlite-trino latency/throughput/operations theme, epic #2403). Design-driven — no parity
oracle; this is an error-taxonomy + tracing design. Scope is **cqlite-flight** (the Rust `do_get`
service), not the connector.

## Why (measured problem)
Source of truth: #2661 v0.15.0 milestone soak, Finding 2.

- **do_get server-side errors run at 0.89%** (8,563 err / 951,762 ok) — zero client-visible,
  load-proportional, concentrated on the busy pod, and the entire volume lands in the single coarse
  category `other`.
- The error is **unprovable in-field**: the category is too coarse to tell a benign abort (a split
  torn down under a streaming reader, a client that hung up) from a genuine internal failure, and
  there is **no log line on the abort path** to attribute it after the fact.
- This noise blocks a clean 0-error stability story for 0.15 and hides any real failure class inside
  the `other` bucket.

### Where the flattening happens (from the code map)
`cqlite.errors.total{cqlite.error.category}` is the counter that carries the `other` label. For
Flight it is fed by `crate::obs::record_status_error(status)` (`cqlite-flight/src/obs.rs`), which
maps the outgoing gRPC `Code` to one of three `cqlite_core::Error` variants
(`not_found | invalid_operation | internal`), and `ErrorCategory::classify()` sends all three to
`ErrorCategory::Other`. So `Aborted`, `Cancelled`, `Unavailable`, `Internal`, `Unknown`, `DataLoss`,
`NotFound`, and `Unimplemented` **all** collapse into `other`.

Two structural problems make this unfixable by "map more codes":
1. The gRPC `Code` is itself lossy — **client-disconnect** (`streaming.rs` stream-drop) and
   **cooperative merge-cancel** (`ProducerError::Cancelled`) both surface as `Code::Aborted`;
   **snapshot/split teardown** surfaces as `Code::Internal`, indistinguishable from a genuine merge
   panic. You cannot recover the abort *class* from the code after the fact.
2. Classifying from the free-text message would violate the no-heuristics mandate (#28).

Therefore classification must be **stamped at the abort site**, where the code that raises the abort
knows exactly why.

## What changes
- **A new bounded attribute `cqlite.flight.abort_reason`** on the `cqlite.errors.total` emission for
  the `flight` subsystem (and mirrored on the abort log/trace event). Its closed value set names
  every known do_get abort path:
  - `superseded_split` — a split/snapshot torn down or retired under a streaming reader (#2452).
  - `client_cancel` — client disconnect / stream dropped before completion; cooperative cancel.
  - `admission_shed` — rejected at max-concurrent-scans capacity (#2420).
  - `snapshot_retired` — the resolved snapshot generation was retired before/while serving (#2452).
  - `internal` — a genuine internal fault (merge/convert/predicate/discovery/panic/egress).
  - `ticket_invalid` — malformed/rejected ticket (already `Code::InvalidArgument`; named for
    completeness so nothing lands unlabeled).
  The reason is **stamped at each abort construction site**, never inferred from the gRPC code or
  message text. `record_status_error` gains a reason-carrying entry point so the site passes the
  authoritative label through.
- **A debug-level structured event on the do_get abort path** carrying enough context to attribute
  in-field: the `abort_reason`, the ticket/split identity, and the snapshot generation — attached at
  the abort site / on the `flight.rpc` span, not just the coarse `%code` + message that exists today.
- **Wiring evidence:** an integration test drives a superseded/cancelled do_get through the public
  Flight service surface (real tonic server, as `do_get_transport_test.rs` does) and asserts the
  specific `cqlite.errors.total{abort_reason=...}` counter increments — not a helper unit test.
- **Adjudication doc:** which reasons are benign (excluded from the error-rate SLI:
  `client_cancel`, `superseded_split`, `snapshot_retired`, `admission_shed`) vs. genuine
  (`internal`). If a genuine failure class remains after attribution, spin out a fix issue
  cross-referenced to #2681.

## Non-goals
- **No change to the connector** or client-visible behavior — this is server-side observability only;
  the 8,563 errors are already zero-client-visible and stay that way.
- **No new SLI wiring / alerting rules** — this change makes attribution *possible*; the field
  verification (report-only) and any alert changes are the next soak round (#2661), noted in the
  acceptance criteria as report-only.
- **No fix for a genuine failure class** discovered inside `other` — if attribution reveals a real
  bug, it is a separate cross-referenced issue, not this change.
- **No unbounded/high-cardinality attributes** — `abort_reason` is a closed enum; ticket/split
  identity and snapshot generation go to the **log/trace event only**, never onto a metric label.

## Doctrine impact
- **No-heuristics (#28):** classification is stamped at the error/abort site from authoritative
  local knowledge; it is never inferred from message text or re-derived from the gRPC code. This is
  the load-bearing design constraint.
- **Wiring-evidence:** done requires the public Flight surface to exercise each new category via an
  end-to-end transport test that asserts the counter increments.
- **Bounded-cardinality metrics:** the new attribute is a closed value set; the existing
  `assert_bounded_attrs` test gains `cqlite.flight.abort_reason` in its allowlist.
