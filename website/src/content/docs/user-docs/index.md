---
title: User Docs
description: Installation, quick start, CLI reference, query guide, Python and Node.js bindings for CQLite.
sidebar:
  label: Overview
  order: 0
---

# User Docs

Everything you need to install, configure, and use CQLite.

CQLite reads Cassandra 5.0 SSTables directly from disk — no cluster, no JVM, no
daemon required.

## In this section

### Getting started

- [Quick Start](/cqlite/user-docs/quick-start/) — run your first query in under five minutes
- [Installation](/cqlite/user-docs/installation/) — prebuilt binaries, cargo, pip, npm paths for all platforms

### Reference

<!-- TODO(W8): link CLI Reference when merged (W4 issue) -->
**CLI Reference** — all flags, subcommands, and one-shot / REPL / TUI modes *(arriving in W4)*

<!-- TODO(W8): link Output Formats when merged (W4 issue) -->
**Output Formats** — JSON, CSV, Parquet *(arriving in W4)*

<!-- TODO(W8): link Python Bindings when merged (W5 issue) -->
**Python Bindings** — `cqlite-py` API reference *(arriving in W5)*

<!-- TODO(W8): link Node.js Bindings when merged (W5 issue) -->
**Node.js Bindings** — `@cqlite/node` API reference *(arriving in W5)*

### Topics

- [Write Support](/cqlite/user-docs/write-support/) — offline SSTable writing, flush, compaction (M5)
- [Limitations](/cqlite/user-docs/limitations/) — what CQLite can and cannot read (format matrix, known gaps)
- [Troubleshooting](/cqlite/user-docs/troubleshooting/) — common problems and fixes

<!-- TODO(W8): link Use Cases when merged (W9 issue) -->
**Use Cases** — Cassandra sidecar, data science, services, operational scenarios *(arriving in W9)*

## Quick links

- [GitHub Repository](https://github.com/pmcfadin/cqlite)
- [API Reference](/cqlite/api/latest/)
- [SSTable Format Guide](/cqlite/sstable-format/) — 22-chapter deep dive into the on-disk format
