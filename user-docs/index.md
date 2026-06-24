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

- [CLI Reference](/cqlite/user-docs/cli-reference/) — all flags, subcommands, and one-shot / REPL / TUI modes
- [Output Formats](/cqlite/user-docs/output-formats/) — JSON, CSV, Parquet
- [Python Bindings](/cqlite/user-docs/python/) — `cqlite-py` API reference
- [Node.js Bindings](/cqlite/user-docs/nodejs/) — `@cqlite/node` API reference

### Topics

- [Write Support](/cqlite/user-docs/write-support/) — offline SSTable writing, flush, compaction (M5)
- [Observability](/cqlite/user-docs/observability/) — runtime OpenTelemetry traces/metrics, local stack, config, metric catalog
- [Limitations](/cqlite/user-docs/limitations/) — what CQLite can and cannot read (format matrix, known gaps)
- [Troubleshooting](/cqlite/user-docs/troubleshooting/) — common problems and fixes
- [Use Cases](/cqlite/user-docs/use-cases/) — Cassandra sidecar, data science, services, operational scenarios

## Quick links

- [GitHub Repository](https://github.com/pmcfadin/cqlite)
- [API Reference](/cqlite/api/latest/)
- [SSTable Format Guide](/cqlite/sstable-format/) — 22-chapter deep dive into the on-disk format
