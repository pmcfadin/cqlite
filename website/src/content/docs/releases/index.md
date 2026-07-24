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
| [v0.16.1](/cqlite/releases/v0-16-1/) | 2026-07-23 | Reads a second Cassandra on-disk format — CommitLog segment files — via a library API and a `read-commitlog` CLI |
| [v0.16.0](/cqlite/releases/v0-16-0/) | 2026-07-22 | Trino connector completeness — typed collection columns (array/row/map) · weight-balanced split fan-out · `LIMIT`-cancellation hang fixed |
| [v0.15.0](/cqlite/releases/v0-15-0/) | 2026-07-17 | Trino latency/throughput/ops — ~15× warm throughput · admission control · saturation gauges · P0 silent-row-loss fix |
| [v0.14.1](/cqlite/releases/v0-14-1/) | 2026-07-13 | Cold-start parse fix — `Index.db` parsed once per open · 200k-entry index build ~100× faster |
| [v0.14.0](/cqlite/releases/v0-14-0/) | 2026-07-13 | Flight field-readiness — Arrow Flight + Trino read path validated against a live at-scale Cassandra deployment · 2 breaking changes |
| [v0.13.0](/cqlite/releases/v0-13-0/) | 2026-07-05 | Performance release — read-path + point-read + compressed-chunk wins · byte-bounded results · `Database.refresh()` · 3 breaking changes |
| [v0.12.0](/cqlite/releases/v0-12-0/) | 2026-06-22 | Arrow Flight + Trino connector · CDC delta-export to Parquet · byte-for-byte compaction parity |

<!-- Convention: newest release first. Give each new release page a lower
     sidebar.order than the previous one so it sorts above older releases
     (this index stays at order 0). -->
