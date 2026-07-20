# Tasks — do_get error taxonomy

## Implementation
- [ ] Add `AbortReason` enum (closed set: `superseded_split`, `client_cancel`, `admission_shed`,
  `snapshot_retired`, `internal`, `ticket_invalid`) with `label() -> &'static str` in
  `cqlite-flight/src/obs.rs` (surface: `AbortReason`).
- [ ] Add `AbortContext { ticket_id, snapshot_generation }` and a reason-carrying hook
  `obs::record_do_get_abort(status, reason, cx)` that (a) increments `cqlite.errors.total` with
  `cqlite.subsystem="flight"` + `cqlite.flight.abort_reason=reason.label()` + a valid closed
  `cqlite.error.category`, and (b) emits the structured event at the reason-appropriate level with
  the abort context (surface: `obs::record_do_get_abort`).
- [ ] Stamp the authoritative reason at each abort construction site (do NOT infer from code/message):
  - `streaming.rs` stream-drop (client disconnect) → `client_cancel`
  - `service.rs`/`streaming.rs` cooperative merge-cancel → `client_cancel`
  - `admission.rs` `reject_status()` shed → `admission_shed`
  - `service.rs` `warm_error_to_status` teardown/retire → `superseded_split` / `snapshot_retired`
  - `service.rs`/`streaming.rs` genuine internal → `internal`
  - `service.rs` `From<TicketError>` → `ticket_invalid`
- [ ] Extend the `flight.rpc` span (abort path only) with ticket/split identity + snapshot generation;
  keep the happy path method-name-only (span cardinality cap).
- [ ] Route the abort log level off the reason (benign→debug, internal→error, ticket→warn),
  replacing the code-driven level for the do_get abort path.

## Wiring evidence (public Flight surface)
- [ ] Extend `tests/do_get_transport_test.rs` (real tonic server) under `observability-testing`:
  client-disconnect asserts `cqlite.errors.total{abort_reason="client_cancel"}` += 1.
- [ ] Add a superseded/retired-snapshot transport case asserting
  `abort_reason ∈ {superseded_split, snapshot_retired}` += 1.
- [ ] Add an admission-shed transport case asserting `abort_reason="admission_shed"` += 1.
- [ ] Add `cqlite.flight.abort_reason` to `BOUNDED_KEYS` in `tests/metrics_capture_test.rs`.

## Adjudication + docs
- [ ] Record benign (excluded-from-SLI) vs genuine reasons in the change/proposal; note the
  report-only field-verification follow-up (#2661) and the spin-out-a-fix-issue clause if a residual
  genuine `internal` rate remains.
- [ ] Update any Flight observability doc listing metric attributes to include
  `cqlite.flight.abort_reason`.

## Quality gates
- [ ] `scripts/agent-gate.sh --lite` green each fix round (summary-file redirect).
- [ ] rust-reviewer + roborev on the lite-green diff (review-first).
- [ ] Full `scripts/agent-gate.sh` PASS (gate of record, in flow-closer).
- [ ] C intent audit (spec-auditor) PASS — every requirement satisfied with a public-surface test.
- [ ] Final roborev clean.
- [ ] `openspec archive` on merge.
