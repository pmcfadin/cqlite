## Context

This change encodes the **owner-DECIDED** posture of the 2026-07 read-path performance
audit, `docs/reports/read-path-performance-audit-2026-07-01.md` §Epic F, item **F2**
("Batch the public streaming channel"), verbatim intent:

> the forwarder re-flattens per row into the public channel … one async wake per row
> survives the pipeline whose own docs measured per-row seam costs at 31–42%; expose
> `Vec<Row>` batches (API-additive)

and issue #1592's step-by-step fix. **No new design latitude is taken** — the shape,
the reuse of `BATCH_EMIT_ROWS`, the "no config knob", the API-additive constraint, the
"forward internal batches straight through — do not flatten and re-batch", and the
"reimplement the per-row surface as a thin flattening adapter" all come directly from
the audit/issue.

## The streaming stack (before)

```
run_scan_stream_windowed:  raw chunks → blocking parse → internal Vec batch channel
                           → forwarder FLATTENS batch → per-row tx
SSTableReader::scan_stream → per-row Receiver
SSTableManager::scan_stream→ k-way merge over per-reader per-row streams → per-row out
StorageEngine::scan_stream → per-row Receiver
query engine               → recv() ONE row at a time
```

The internal batch is created (F2's win exists internally) then immediately destroyed
at the forwarder, and every level above sends/receives one row per wake.

## Decisions

1. **`WindowedOut` selects the forwarder arm; the driver is otherwise identical.**
   The windowed driver keeps its single internal `Vec`-batch channel and all of its
   #1143/#1156 invariants (`MAX_INFLIGHT_BATCH_ROWS`, per-chunk flush cadence, terminal
   drain, cancellation, error-flush). Only the forwarder diverges:
   - `WindowedOut::PerRow(tx)` — FLATTEN each `Vec` batch to per-row items (the exact
     prior behavior). Per-row is thus **a thin flattening adapter over the batched
     internal stream** (satisfies issue #1592 step 1's adapter mandate).
   - `WindowedOut::Batched(tx)` — FORWARD each `Vec` batch straight through (one send
     per batch; **do not flatten then re-batch** — satisfies the audit's straight-through
     requirement).
   This keeps ONE machinery and guarantees content parity: flattening the batched arm's
   output equals the per-row arm's output.

2. **Single-generation manager path forwards batches straight through.** For one
   reader (the common single-generation case), `SSTableManager::scan_stream_batched`
   returns the reader's batched receiver directly — no per-row channel is interposed,
   so the wake amortization survives end to end (the real F2 win).

3. **Multi-/zero-generation and `tombstones` reuse the correct per-row path, then
   re-chunk.** These cases already route through cross-generation reconciliation /
   token-ordered k-way merge / materialization in the per-row `scan_stream`. Rather
   than duplicate that ordering logic (risking a divergence bug), the batched surface
   reuses `scan_stream` and re-chunks its output into `BATCH_EMIT_ROWS` batches. This
   trades the straight-through win on the uncommon cross-generation path for
   correctness; a generation-aware fully-batched merge is a deliberate follow-up
   (audit §F). Re-chunk preserves order/content (FIFO) and backpressure (bounded
   channel).

4. **Reuse `BATCH_EMIT_ROWS`; bound the batched channel in batches.** No new tunable.
   The batched channel capacity is `ceil(buffer_size / BATCH_EMIT_ROWS).max(1)` so the
   resident-row budget of the public channel stays ~`buffer_size`, not
   `buffer_size × BATCH_EMIT_ROWS`.

5. **Wire the real consumers.** The query engine's three full-scan consumers
   (`executor::streaming_scan_rows`, `select_executor::streaming` producer,
   `select_executor::stream_agg` fold) consume `scan_stream_batched` and iterate each
   batch. This realizes the wake reduction on the actual read path and provides the
   wired call chain (not a built-but-unwired surface — the recurring audit anti-pattern).

## Backpressure argument

The batched channel is bounded. On the straight-through single-reader path, a stalled
consumer blocks the forwarder's `tx.send(batch).await`, which stops draining the internal
batch channel, which blocks the parse half's `blocking_send`, which stops the parse loop
and the raw-chunk feed, which stops disk reads — the exact end-to-end shape of the per-row
path, just per batch. On the re-chunk path, a stalled consumer blocks the re-chunk task's
`tx.send(batch).await`, which stops draining the per-row source, which applies the same
upstream backpressure the per-row surface already had. Nothing buffers the whole result.

## Risks / trade-offs

- **Re-chunk round-trip on multi-generation reads** (chunk after a per-row merge) — cheap
  Vec push/flush, and the cross-generation path is the uncommon correctness path.
- **Query-consumer loop nesting** — the three consumers gain an inner `for row in batch?`;
  control flow (`continue`/`return`/LIMIT early-stop by dropping the receiver) is preserved.
