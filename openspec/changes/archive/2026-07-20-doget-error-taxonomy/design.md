# Design — do_get abort taxonomy + abort-path trace

## Problem restated
Every failing do_get is counted on `cqlite.errors.total` with `cqlite.error.category = other` because
the Flight hook `record_status_error(status)` maps the outgoing gRPC `Code` → 3 core `Error`
variants → all `ErrorCategory::Other`. The gRPC code is lossy (disconnect and merge-cancel are both
`Aborted`; teardown and genuine panic are both `Internal`), so the abort *reason* is only knowable at
the site that raises it.

## Chosen approach — stamp an authoritative `abort_reason` at the site

Add a **new bounded OTel attribute `cqlite.flight.abort_reason`** to the `cqlite.errors.total`
emission for `subsystem = flight`, with a closed value enum:

```
superseded_split | client_cancel | admission_shed | snapshot_retired | internal | ticket_invalid
```

Introduce a Rust enum in cqlite-flight (`AbortReason`) with a `&'static str` label, and a
reason-carrying variant of the recording hook:

```
obs::record_do_get_abort(status: &Status, reason: AbortReason, cx: AbortContext)
```

- It emits `cqlite.errors.total += 1` with `{cqlite.error.category = <derived>, cqlite.subsystem =
  "flight", cqlite.flight.abort_reason = reason.label()}`. `error.category` stays a valid closed
  `ErrorCategory` (benign reasons map to `cancelled`; `internal`/`ticket_invalid` keep their existing
  categories) so existing category dashboards do not break — the *new* attribute carries the fine
  detail.
- It emits the abort log/trace event at the level appropriate to the reason (see below), carrying
  `AbortContext { ticket_id, snapshot_generation }`.

Each abort **construction site** passes the authoritative reason:

| Site (file) | Status today | `AbortReason` stamped |
|---|---|---|
| `streaming.rs` stream-drop on `Drop` (client disconnect) | `Aborted` | `client_cancel` |
| `service.rs` `From<ProducerError>` `Cancelled` / `streaming.rs` cooperative cancel | `Aborted` | `client_cancel` |
| `admission.rs` `reject_status()` shed | `Unavailable` | `admission_shed` |
| `service.rs` `warm_error_to_status` for a torn-down/retired snapshot dir | `Internal`/`NotFound` | `superseded_split` / `snapshot_retired` |
| `service.rs`/`streaming.rs` merge/convert/predicate/discovery/panic/egress internal | `Internal` | `internal` |
| `service.rs` `From<TicketError>` | `InvalidArgument` | `ticket_invalid` |

The distinction between `superseded_split` and `snapshot_retired`: the warm-reader path
(`warm/mod.rs`) already knows whether the failure is a *split* being superseded by a newer generation
under an open reader (`superseded_split`) vs. the *snapshot* generation itself being retired before
resolve completes (`snapshot_retired`). The `WarmError` variant carries that distinction; the mapper
stamps the matching reason. Where the warm layer genuinely cannot tell them apart, it stamps the more
specific reason it does know and documents the residual in the adjudication doc — never guesses.

### Logging levels (reason-driven, replacing code-driven)
Today `record_status_error` picks the log level from the gRPC code, so `Unavailable` (a benign
admission shed) logs at ERROR. The abort event uses the **reason**:
- `client_cancel`, `superseded_split`, `snapshot_retired`, `admission_shed` → `debug` (benign,
  expected under load; the acceptance criteria call for debug-level here).
- `internal` → `error` (genuine fault).
- `ticket_invalid` → `warn` (client fault).

### Context carried on the event (NOT on the metric)
`AbortContext { ticket_id: <keyspace/table/generation/split identity>, snapshot_generation }`.
These are potentially unbounded, so they live only on the structured log/trace event and the
`flight.rpc` span fields — never as metric labels. The `flight.rpc` span (`obs.rs`) is extended with
these fields on the abort path only (the happy path stays method-name-only to keep span cardinality
capped).

## Alternatives considered (and why rejected)
1. **Route more gRPC codes to more `ErrorCategory` variants** (e.g. add `Aborted→Cancelled`,
   `Unavailable→new`). Rejected: the code is lossy — disconnect and merge-cancel are both `Aborted`,
   teardown and panic both `Internal`. It cannot separate `superseded_split` from `internal`, which
   is the entire point of the issue.
2. **Classify from the error message text.** Rejected: violates the no-heuristics mandate (#28) —
   message text is not authoritative and is not cardinality-safe.
3. **A brand-new metric per abort reason.** Rejected: proliferates counters, breaks the existing
   `error.category` rollups, and a bounded attribute on the existing counter is the idiomatic OTel
   shape already used elsewhere in the crate.
4. **Add a new `ErrorCategory` variant per reason in cqlite-core.** Rejected: `ErrorCategory` is a
   core, cross-subsystem enum; Flight-specific abort classes belong on a Flight-scoped attribute, not
   in the shared category set. (`Cancelled` already exists and is reused for benign aborts.)

## Wiring evidence plan
Extend the real-transport harness (`tests/do_get_transport_test.rs`, real tonic
`FlightServiceServer` over loopback) with, under `observability-testing`:
- a **client-disconnect** case (drop the decode stream mid-read) asserting
  `cqlite.errors.total{abort_reason="client_cancel"}` increments by 1;
- a **superseded/retired snapshot** case (tear down the snapshot dir under a streaming reader, per
  the existing `warm_hit_after_snapshot_teardown_rebuilds_instead_of_enoent` fixture pattern)
  asserting `abort_reason ∈ {superseded_split, snapshot_retired}` increments;
- an **admission shed** case (drive past `--max-concurrent-scans`) asserting
  `abort_reason="admission_shed"` increments.
Add `cqlite.flight.abort_reason` to `BOUNDED_KEYS` in `metrics_capture_test.rs` so the bounded-attr
assertion covers it.

## Adjudication (recorded in the change)
- **Benign, excluded from the error-rate SLI:** `client_cancel`, `superseded_split`,
  `snapshot_retired`, `admission_shed`.
- **Genuine, counts toward the SLI:** `internal`.
- **Client fault, tracked separately:** `ticket_invalid`.
After this ships, the next soak round (#2661, report-only) attributes the 0.89% by reason and states
the residual genuine-`internal` rate explicitly (target 0). If that residual is non-zero, a fix issue
is spun out cross-referencing #2681.
