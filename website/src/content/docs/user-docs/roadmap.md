---
title: Roadmap
description: Where CQLite is headed — milestones, in-flight epics, and how to influence priorities.
sidebar:
  label: Roadmap
  order: 9
---

# Roadmap

CQLite is at **v0.11.0**. The read path, CLI, output writers (including Parquet),
Python and Node.js bindings, and write support with STCS compaction are
production-ready. This page is what comes next.

The roadmap is community-driven. The fastest way to move something up the list is to
[**open or +1 an issue**](https://github.com/pmcfadin/cqlite/issues) describing your
use case — and to [**star the repo**](https://github.com/pmcfadin/cqlite) so the
project's reach is visible.

_Last reviewed: 2026-06-16 (v0.11.0)._

## Milestones

| Milestone | Status |
|-----------|--------|
| M1 — Core SSTable reading | ✅ Complete |
| M2 — CLI (one-shot + REPL) | ✅ Complete |
| M3 — Output writers (CSV, JSON, Parquet, CQL) | ✅ Complete |
| M4 — Python + Node.js bindings | ✅ Complete |
| M5 — Write support + STCS compaction | ✅ Complete (v0.9.0) |
| M6 — WASM bindings for the browser | 📋 Planned |
| M7 — Performance validation + **v1.0** | 📋 Planned |

## In-flight epics

These are the active workstreams between v0.11.0 and v1.0. Each links to its GitHub
epic with the child tasks.

### Query engine completeness ([#756](https://github.com/pmcfadin/cqlite/issues/756))

`PER PARTITION LIMIT`, static-column tracking, clustering-order (`ASC`/`DESC`)
extraction, and query-plan metadata (`indexes_used`). Closes the gap between CQLite's
`SELECT` support and CQL semantics.

### `WRITETIME()` and `TTL()` in `SELECT` ([#689](https://github.com/pmcfadin/cqlite/issues/689))

Surface per-cell write timestamps and TTLs through the parser, executor, output
formats, and bindings — a common need for debugging and migration workflows.

### Writer format fidelity ([#762](https://github.com/pmcfadin/cqlite/issues/762))

`> 64`-column serialization headers, explicit deletion-time semantics, `DURATION`
comparator parsing, and phase 1 of BTI **index writing** for SSTables CQLite produces.

### Wide-partition & memory performance ([#751](https://github.com/pmcfadin/cqlite/issues/751))

Promoted-index writing for wide partitions, streamed index/merge writers to drop the
in-memory buffer, and BTI-payload Data.db offsets for O(log n) partition seeks. This
also closes the wide-partition scan listed in
[Known Issues](/cqlite/user-docs/known-issues/).

### BTI (`da`) end-to-end read support ([#660](https://github.com/pmcfadin/cqlite/issues/660))

A dedicated read path for trie-indexed SSTables — full trie walk, `ByteComparable`
decode, and Data.db chaining — so `da`-format files are read instead of rejected.
Fixtures and `sstabledump` goldens already ship in the test corpus.

### Delta-scan envelope for CDC-style Parquet ([#696](https://github.com/pmcfadin/cqlite/issues/696))

A streaming `scan_delta` API and `cqlite delta-export` subcommand that emit
upserts, tombstones, and per-cell metadata as Parquet for change-data-capture and
reconciliation pipelines.

## Influencing the roadmap

Priorities follow real-world use. If you need something:

1. **Search** [existing issues](https://github.com/pmcfadin/cqlite/issues) and add a
   👍 or a comment with your use case.
2. **File** a [new issue](https://github.com/pmcfadin/cqlite/issues/new/choose) if it
   is not tracked.
3. **Star** [the repo](https://github.com/pmcfadin/cqlite) — visibility directly
   affects how much time goes into the project.
4. **Contribute** — see [CONTRIBUTING](https://github.com/pmcfadin/cqlite/blob/main/CONTRIBUTING.md)
   and look for `good-first-issue` labels.
</content>
