# Proposal: sstable-freshness-refresh (core + bindings + docs)

> Milestone: platform/read-surface hardening (post-M5; feeds the Flight rewrite #1477/AB1).
> Issue: #1749. Routing: **design-driven** — public API surface (`Database`), a product
> posture decision (freshness contract per read surface), and user-facing docs. No
> Cassandra/sstabledump oracle exists for "when should a reader see new files".

## Why

CQLite has three read surfaces with three *accidental* — never decided — behaviors when
the SSTable directory changes underneath them (a Cassandra flush/compaction, or a CQLite
`--flush`, may add or remove generations at any time):

| Surface | Behavior today | Consequence |
|---|---|---|
| Library handle (Python / Node / CLI REPL) | `StorageEngine::open` discovers once; `SSTableManager.table_readers` is never re-scanned | **Stale until reopen** — new generations invisible; compacted-away files held open indefinitely |
| CLI one-shot | Full re-open per process | Fresh, but pays the entire setup cost (incl. full Index.db materialization) every query |
| Arrow Flight server | `MergeProducer` re-lists `*-Data.db` per request (`cqlite-flight/src/producer.rs`) | Fresh per request, but **no isolation within a request**: paths listed at request start; files deleted mid-stream error the stream (torn window) |

Warm long-lived sessions are the recommended way to amortize the 150–500 ms open cost
(#1562 investigation), which makes the library surface's stale-forever behavior the
*default* user experience. Cassandra solves the torn-read side with hardlink snapshots;
CQLite reads in place and has no equivalent. The gap is not any one bug — it is that no
contract exists.

## What Changes

- **A documented per-surface freshness contract** (user docs page): what each surface
  promises about seeing added/removed generations, and what happens when files vanish
  mid-query. The Flight torn-window posture is **recorded as a decision** for the Flight
  rewrite (#1477) to implement — no Flight code changes here.
- **`Database::refresh()`** (async, and via `StorageEngine`/`SSTableManager`): an
  explicit, concurrency-safe re-scan that diffs the directory against the held reader
  set — opens added generations, drops removed ones, **keeps existing `Arc<SSTableReader>`
  instances for unchanged files** (warm state preserved: parsed Index/Statistics/bloom are
  not rebuilt). Returns a `RefreshReport { readers_added, readers_removed, tables_scanned }`.
- **Atomic, fail-closed application:** if any newly discovered generation fails to open
  (e.g. corrupt `Statistics.db` — the #1626 hard-fail is inherited via the standard
  `SSTableReader::open`), `refresh()` returns the error and the previous reader set stays
  in place unchanged. No partial view.
- **Query-consistent snapshots:** each query resolves its reader list once under the
  existing `RwLock` read guard; in-flight scans hold `Arc` clones and complete against
  the pre-refresh set. `refresh()` swaps under the write guard.
- **Bindings wiring:** `db.refresh()` exposed in Python and Node with end-to-end tests
  (public surface exercises the core path — wiring evidence, not helper-only tests).

## Non-goals

- **No filesystem watching / auto-refresh / per-query staleness checks.** Explicit
  refresh only. Auto-refresh is a possible follow-up once the contract exists (recorded
  in the docs page as such).
- **No one-shot CLI changes** (it is already fresh by construction).
- **No Flight implementation changes** — the torn-window posture decision is an input to
  #1477, not built here.
- **No read-your-writes / memtable visibility** — separate product question (open
  NEEDS-YOU item), unchanged by this work.
- **No hardlink/snapshot isolation across refreshes** — queries get consistency via
  `Arc`-held readers, not filesystem snapshots.
- **No REPL `.refresh` command** in this change (trivial follow-up once the API exists).

## Doctrine impact

- User docs: new "Read surfaces and freshness" page; limitations page cross-link. Lands
  in the same change (CLAUDE.md doctrine-currency rule).
- No-heuristics: discovery stays TOC/filename-component based exactly as `open` does
  today; refresh introduces no content sniffing.
- Memory budget: refresh must not double-hold reader state transiently beyond the added
  generations (diff-and-swap, not rebuild-everything).
