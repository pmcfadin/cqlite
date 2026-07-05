---
title: Releases
description: CQLite release announcements — what shipped in each version and how to upgrade.
sidebar:
  label: Releases
  order: 0
---

# Releases

Release announcements for CQLite. Each page covers what shipped and how to get it.
For the complete, granular change list, see the
[CHANGELOG](https://github.com/pmcfadin/cqlite/blob/main/CHANGELOG.md).

| Version | Date | Highlights |
|---------|------|-----------|
| [v0.13.0](/cqlite/releases/v0-13-0/) | 2026-07-05 | Performance release — read-path + point-read + compressed-chunk wins · byte-bounded results · `Database.refresh()` · 3 breaking changes |
| [v0.12.0](/cqlite/releases/v0-12-0/) | 2026-06-22 | Arrow Flight + Trino connector · CDC delta-export to Parquet · byte-for-byte compaction parity |

<!-- Convention: newest release first. Give each new release page a lower
     sidebar.order than the previous one so it sorts above older releases
     (this index stays at order 0). -->
