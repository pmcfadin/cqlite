---
title: Roadmap
description: Where CQLite is headed — milestones, in-flight epics, and how to influence priorities.
sidebar:
  label: Roadmap
  order: 9
---

# Roadmap

CQLite is at **v0.12.0**. The read path, CLI, output writers (including Parquet),
Python and Node.js bindings, and write support with STCS compaction — now with
**byte-for-byte compaction parity against Apache Cassandra**, an Arrow Flight + Trino
connector, canonical BTI (`da`) write/read, and CDC-style delta export — are
production-ready. This page is what comes next.

The roadmap is community-driven. The fastest way to move something up the list is to
[**open or +1 an issue**](https://github.com/pmcfadin/cqlite/issues) describing your
use case — and to [**star the repo**](https://github.com/pmcfadin/cqlite) so the
project's reach is visible.

_Last reviewed: 2026-06-22 (v0.12.0)._

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

These are the active workstreams between v0.12.0 and v1.0. Each links to its GitHub
epic with the child tasks.

> Query-engine completeness (#756), `WRITETIME()`/`TTL()` in `SELECT` (#689), writer
> format fidelity (#762), wide-partition performance (#751), BTI (`da`) end-to-end
> read (#660), and the CDC delta-scan envelope (#696) all **shipped in v0.12.0** —
> see the [changelog](https://github.com/pmcfadin/cqlite/blob/main/CHANGELOG.md).

### Wire storage capabilities into the query path ([#951](https://github.com/pmcfadin/cqlite/issues/951))

CQLite's storage layer already has bloom filters, `Index.db`, the BTI `Partitions.db`
trie, and a point-lookup path — but parts of the CQL query engine still scan every
SSTable instead of using them. This epic audits for that pattern, wires the gaps
(within-SSTable partition seeks, clustering-key pushdown, `IN`/token-range lookups),
and adds regression guards so single-partition reads scale with candidate SSTables,
not total count.

### Read-path performance & I/O backend ([#906](https://github.com/pmcfadin/cqlite/issues/906))

Parallel reads on a single `SSTableReader` (landed) plus a benchmark-first spike on an
io_uring read backend for Linux.

### Compaction byte-parity follow-ups ([#938](https://github.com/pmcfadin/cqlite/issues/938))

Edge cases deferred from the v0.12.0 parity work: range tombstones end-to-end through
the compaction writer, and a handful of writer/reconciliation refinements.

### CLI & bindings polish ([#907](https://github.com/pmcfadin/cqlite/issues/907))

Developer-experience cleanup: export progress/statistics, clearer
unsupported-platform errors in the Node loader, and output-mode tidying.

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
