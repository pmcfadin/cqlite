---
title: Use Cases
description: When and why to use CQLite — narrative architecture guides for common patterns.
sidebar:
  label: Overview
  order: 0
---

# Use Cases

These pages explain *when* and *why* to reach for CQLite, not just *how* to use its
APIs. Each page covers a concrete pattern, shows what is working today, and is honest
about what is still in progress.

## Pages in this section

| Page | What it covers |
|------|----------------|
| [Lakehouse projections with the Cassandra Sidecar](/cqlite/user-docs/use-cases/sidecar-lakehouse/) | SSTable-to-Parquet pipeline architecture, the delta-semantics caveat, the high-fidelity type mapping, and embedding the Parquet writer |
| [Python: data science and ETL](/cqlite/user-docs/use-cases/python-data-science/) | Offline analytics on snapshots and backups without a cluster; pandas and notebook workflows |
| [Node.js: services and tooling](/cqlite/user-docs/use-cases/nodejs-services/) | Data inspection services, ops dashboards, `executeNative` + streaming patterns |
| [Operational scenarios](/cqlite/user-docs/use-cases/operational/) | Migration validation, test fixtures from production data, backup and snapshot inspection |

## Common thread

All four use cases share the same core property: **no cluster dependency in the read
path.** CQLite reads Cassandra 5.0 SSTables directly from the filesystem. That opens
up workflows that would otherwise require a live Cassandra cluster — offline analytics,
CI fixtures, DR validation, lakehouse projection — while keeping the toolchain simple.

## What CQLite is not (yet)

- Not a real-time replication solution. Reads are per-SSTable batch operations.
- Not a query engine for the data lake. It produces files and rows; Spark, Trino,
  DuckDB, or pandas consume them.
- Not a replacement for the Cassandra query path for production traffic.

See [GitHub Issues](https://github.com/pmcfadin/cqlite/issues) for the roadmap.
