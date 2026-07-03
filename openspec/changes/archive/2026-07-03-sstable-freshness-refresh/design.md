# Design: sstable-freshness-refresh

## Decision 1 — Freshness posture per surface (the product call)

**Chosen: explicit-refresh-only for the library handle; document the other two surfaces
as they are.**

| Option | Verdict |
|---|---|
| **(a) Explicit `refresh()` only** — chosen | Deterministic, zero per-query overhead, no surprise result-set changes mid-session, trivially testable. The caller decides when freshness matters. |
| (b) Auto-check per query (dir mtime / generation-set hash) | Rejected for now: adds a stat/readdir syscall tax to every query on the hot path we are actively trying to shrink (#1562), and makes result sets change *between two identical queries* with no user action — surprising for analytical sessions. Viable follow-up as an opt-in `open` option once (a) exists. |
| (c) Filesystem watch (notify/kqueue/inotify) | Rejected: platform-divergent, brings a background thread + event-coalescing complexity into a library, and still needs (a)'s swap machinery underneath. Strictly a follow-up. |

Per-surface contract this change documents:

- **Library handle:** snapshot at `open`; `refresh()` is the only way the set changes.
  In-flight queries are never affected by a concurrent refresh.
- **CLI one-shot:** always fresh (re-open per process). Cold-start cost documented.
- **Flight:** fresh per request; **torn window acknowledged** — the recorded posture for
  the #1477 rewrite is *retry-once-on-vanished-file, then typed error* (rewrite may
  choose to hold `Arc` readers per request via the shared manager instead, which
  subsumes retry; either satisfies the contract page).

## Decision 2 — Swap semantics: diff-and-swap keyed by canonical Data.db path

`SSTableManager::refresh_tables()`:

1. Re-run the same directory discovery `open` uses (TOC/filename-component based — no
   new discovery logic, no heuristics).
2. Under the `table_readers` **write** guard, diff discovered canonical `Data.db` paths
   against held readers per table key:
   - **added** → `SSTableReader::open` (standard path: bloom/Index/Statistics parsed,
     #1626 hard-fail inherited) — opened *before* taking the write guard to keep the
     critical section short; see Decision 3 for failure atomicity.
   - **removed** → drop the `Arc` from the map (readers physically close when the last
     in-flight scan's clone drops).
   - **unchanged** → keep the existing `Arc<SSTableReader>` untouched (warm Index
     HashMap/bloom preserved — refresh of an unchanged 1000-file dir is ~free).
3. Return `RefreshReport { tables_scanned, readers_added, readers_removed }`.

Rejected alternative: rebuild-the-world (`open` a fresh manager, swap wholesale).
Simpler, but re-parses every Index.db (exactly the cost warm sessions exist to avoid),
transiently doubles memory (budget rule), and drops warm state for unchanged files.

## Decision 3 — Failure atomicity: fail-closed, keep old set

All newly discovered generations are opened *first*, outside the write guard. If **any**
open fails, `refresh()` returns that error and performs **no mutation** — the previous
reader set remains live and queries continue against it. Rationale: a partial view
(some new generations visible, one missing) is precisely the torn state this change
exists to eliminate, and silently skipping a corrupt generation would violate the
fail-closed posture #1626 just established.

## Decision 4 — Concurrency model: reuse the existing lock discipline

No new synchronization. Queries already resolve their reader list once under the
`RwLock` read guard and clone `Arc<SSTableReader>`s (per-scan `ScanCursor`s mean no
shared file-position state, #815). Refresh takes the write guard only for the
HashMap diff/swap (opens happen outside it). Consequences, stated in the contract:

- A query started before a refresh completes on the **old** set (correct + consistent).
- A query started after sees the **new** set.
- Two concurrent `refresh()` calls serialize on the write guard; both are correct
  (second becomes a no-op diff).

## Decision 5 — API shape

```rust
// cqlite-core
impl Database {
    /// Re-scan the data directory; apply added/removed SSTable generations atomically.
    pub async fn refresh(&self) -> Result<RefreshReport>;
}
pub struct RefreshReport {
    pub tables_scanned: usize,
    pub readers_added: usize,
    pub readers_removed: usize,
}
```

- Python: `db.refresh() -> RefreshReport` (dataclass-like pyclass; GIL released during
  the async op, same pattern as `execute`). Type stub updated.
- Node: `await db.refresh(): Promise<RefreshReport>` (napi object; TS definition updated).
- Feature gating: lives with the storage layer (not `state_machine`-gated — refresh is
  meaningful for minimal builds too). Minimal-features build must compile (known CI
  gotcha with ungated test items).

## Test strategy notes

- Integration tests use real SSTable binaries; the add-a-generation fixture is created
  by copying an existing corpus generation (or a CQLite-flushed uncompressed one) into a
  temp dir — **never** a synthetic byte blob. 0-rows-on-present-data is a failure.
- The concurrency scenario uses a deliberately slow/streaming scan overlapped with a
  refresh; assertions are on result-set consistency, not timing (no wall-clock races —
  telemetry retro recurring-finding class).
- Multi-generation correctness after refresh (old + new generation reconcile) leans on
  the existing KWayMerger path; the known single-generation reconciliation gap (#1741)
  is orthogonal and NOT masked: the refresh tests assert row *visibility*, not
  tombstone semantics.
