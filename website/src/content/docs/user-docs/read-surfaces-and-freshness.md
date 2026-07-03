---
title: Read Surfaces and Freshness
description: What each CQLite read surface promises when the SSTable directory changes underneath it — snapshot-at-open, explicit refresh(), and torn-window behavior.
sidebar:
  label: Read Surfaces and Freshness
  order: 7
---

# Read Surfaces and Freshness

CQLite reads SSTable files in place. While a reader is open, the directory can change
underneath it — a Cassandra flush or compaction, or a CQLite `--flush`, may add or
remove generations at any time. This page states, for each read surface, what it
promises about seeing those changes and what happens when a file is deleted mid-query.

If you keep a long-lived handle open to amortize the open cost, read the
[library handle](#library-handle) section: your handle sees a **snapshot taken at
open** and does not pick up new generations until you call `refresh()`.

## Freshness contract per surface

| Surface | Freshness | Torn window (file removed mid-query) |
|---------|-----------|--------------------------------------|
| [Library handle](#library-handle) (Python / Node / CLI REPL) | Snapshot at `open`; changes apply **only** on an explicit `refresh()` | In-flight queries hold their readers and complete on the pre-refresh set; never torn |
| [CLI one-shot](#cli-one-shot) | Fresh per process — re-discovered every invocation | Not applicable (single short-lived scan) |
| [Arrow Flight](#arrow-flight) | Fresh per request — re-discovered when the request starts | Acknowledged gap; recorded posture below, implemented by the Flight rewrite ([#1477](https://github.com/pmcfadin/cqlite/issues/1477)) |

## Library handle

A `Database` handle (the Python `cqlite.open(...)`, the Node `Database.open(...)`, and
the CLI REPL) discovers the SSTable generations **once, at open**, and holds each
generation's reader for the life of the handle. New generations that appear afterward
are invisible, and generations that are compacted away stay open, until you refresh.

This is deliberate: discovery and open cost O(SSTable count) — parsing every
`Index.db` / `Statistics.db` and building bloom filters — which is why a cold open runs
roughly 150–500 ms on a typical corpus. A long-lived handle exists precisely to pay
that cost once, so re-checking the directory on every query would defeat it. The handle
decides when freshness matters and calls `refresh()`.

### `refresh()`

`refresh()` re-scans the data directory and applies the difference to the held reader
set. Discovery uses the same TOC- and filename-component-based logic as `open` (no
content sniffing).

- **Added** generations are opened and become queryable.
- **Removed** generations stop being queried; their reader is dropped once the last
  in-flight scan releases it.
- **Unchanged** generations keep their existing reader untouched — the parsed
  `Index` map, `Statistics`, and bloom filter are **not** rebuilt. Refreshing a
  directory that has not changed is effectively free, even with thousands of files.

`refresh()` returns a `RefreshReport`:

| Field | Meaning |
|-------|---------|
| `tables_scanned` | Number of tables re-discovered |
| `readers_added` | Generations opened by this refresh |
| `readers_removed` | Generations dropped by this refresh |

```rust
// cqlite-core
let report = db.refresh().await?;
println!("{} added, {} removed", report.readers_added, report.readers_removed);
```

```python
# Python
report = db.refresh()
print(report.readers_added, report.readers_removed)
```

```javascript
// Node
const report = await db.refresh();
console.log(report.readersAdded, report.readersRemoved);
```

### Atomic and fail-closed

`refresh()` opens every newly discovered generation **before** it changes the held set.
If any new generation fails to open — for example a corrupt or truncated
`Statistics.db`, which hard-fails per the
[#1626](https://github.com/pmcfadin/cqlite/issues/1626) posture — `refresh()` returns a
typed error and leaves the previously held reader set **fully unchanged**. There is no
partial view: you never see some new generations while another is silently skipped. A
subsequent query returns exactly the pre-refresh result set.

### In-flight query isolation

Queries resolve their reader set once, at the moment they start, and hold each reader
for the duration of the scan. A `refresh()` running concurrently with a query does not
affect that query:

- A query started **before** a refresh completes against the **pre-refresh** set.
- A query started **after** a refresh sees the **post-refresh** set.

Because readers are reference-counted, an in-flight streaming scan drains to completion
on the generations it started with, even if `refresh()` removes them meanwhile. This is
consistency by held readers, not by filesystem snapshots — CQLite does not hardlink or
copy files.

## CLI one-shot

One-shot invocations (`cqlite --query ...`, `--execute`, `export`, and similar) open,
query, and exit. Each process re-discovers the directory from scratch, so a one-shot
command is **always fresh** — it sees whatever generations exist when it starts.

The trade-off is cold-start cost: every invocation pays the full open cost (O(SSTable
count), roughly 150–500 ms on a typical corpus) before returning a single row. For
repeated queries over the same data, prefer a long-lived handle (Python, Node, or the
REPL) and call [`refresh()`](#refresh) when you need to pick up new generations.

## Arrow Flight

The Arrow Flight server re-lists the table's `*-Data.db` files when a request starts, so
each request is **fresh per request**.

It does not yet isolate a request from concurrent directory changes: the file paths are
resolved at request start, and if a generation is deleted mid-stream (a compaction
removing the file a request is reading), the stream errors. This torn window is a known
gap.

**Recorded posture** (an input to the Flight rewrite,
[#1477](https://github.com/pmcfadin/cqlite/issues/1477); no Flight code changes here):
on a vanished file, **retry once, then return a typed error** rather than a raw I/O
failure. The rewrite may instead hold reference-counted readers per request through the
shared manager — which subsumes the retry by keeping the generation alive for the life
of the request. Either approach satisfies this contract.

## Non-goal: filesystem watching / auto-refresh

CQLite does **not** watch the filesystem, poll directory modification times, or check
for new generations on each query. Freshness on the library handle is explicit: it
changes only when you call `refresh()`.

Auto-refresh — whether an opt-in per-query staleness check or a background filesystem
watcher (inotify/kqueue) — is a **possible follow-up**, not a promise. It is recorded
here so the absence is intentional, not an oversight. An opt-in check would add a
`stat`/`readdir` syscall to the query hot path and could change a result set between two
identical queries with no user action; a watcher would add a platform-divergent
background thread. Both are deferred until the explicit-refresh contract on this page has
proven out.
