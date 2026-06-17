---
title: Known Issues
description: Active bugs and sharp edges in the current CQLite release, with issue links — what to watch out for today.
sidebar:
  label: Known Issues
  order: 8
---

# Known Issues

This page tracks **active bugs and sharp edges** in the current release (`v0.11.0`).
It is deliberately short and honest: if something here bites you, you are not doing
it wrong.

- For things CQLite **does not do by design or yet**, see
  [Limitations](/cqlite/user-docs/limitations/).
- For **planned work and milestones**, see [Roadmap](/cqlite/user-docs/roadmap/).
- Hit something not listed here?
  [**Open an issue**](https://github.com/pmcfadin/cqlite/issues/new/choose) — that is
  the single most useful thing you can do for the project.

_Last reviewed: 2026-06-16 (v0.11.0)._

## Correctness

### Set element tombstones may hide a row ([#493](https://github.com/pmcfadin/cqlite/issues/493))

Individual element deletions inside a `set<T>` are not fully surfaced. A row that
contains **only** set-element tombstones may read as empty rather than as present.
This is a narrow edge case — full sets, set replacement, and partition/row/range
tombstones all work correctly. Tracked in
[#493](https://github.com/pmcfadin/cqlite/issues/493).

## Performance

### Wide partitions scan linearly ([#751](https://github.com/pmcfadin/cqlite/issues/751), [#752](https://github.com/pmcfadin/cqlite/issues/752))

SSTables **written** by CQLite emit `promoted_index_length = 0`, so within-partition
seeks fall back to a linear scan.

- Narrow partitions (< 100 rows): no measurable impact.
- Wide partitions (10 000+ rows): O(n) scan within the partition.

Reading SSTables written by Cassandra is unaffected — this only concerns the CQLite
write path. The promoted-index writer and BTI-based O(log n) seeks are on the
[roadmap](/cqlite/user-docs/roadmap/) (epic
[#751](https://github.com/pmcfadin/cqlite/issues/751)).

## Format coverage

### BTI (`da`) SSTables are rejected, not read

BTI/trie-index SSTables (`da-*-bti-*`, opt-in in Cassandra 5.0) are detected and
rejected with a clear error rather than misread. This is by design until the
dedicated BTI read path lands — see
[Limitations](/cqlite/user-docs/limitations/) and roadmap item
[#660](https://github.com/pmcfadin/cqlite/issues/660).

## Contributor / CI

These do not affect users of the published packages, but matter if you build and test
from source.

### Python test suite silently skips tests ([#773](https://github.com/pmcfadin/cqlite/issues/773))

A path-resolution bug in the Python test harness (`CQLITE_DATASETS_ROOT`) can cause
~120 tests to skip silently and mask real failures. If you run `pytest` against the
Python bindings, set the dataset root explicitly:

```bash
export CQLITE_DATASETS_ROOT=$PWD/test-data/datasets
bash test-data/scripts/fetch-datasets.sh
```

Tracked in [#773](https://github.com/pmcfadin/cqlite/issues/773).

### Intermittent CI flake ([#774](https://github.com/pmcfadin/cqlite/issues/774))

`test_row_ttl_uses_row_ttl_cell_flags` byte-scans flag bytes and fails
intermittently in CI. A re-run clears it; it does not indicate a real regression.
Tracked in [#774](https://github.com/pmcfadin/cqlite/issues/774).

## Reporting something new

If you hit a bug — wrong output, a panic, a parsing error, a performance cliff —
please report it. Good reports include:

1. The **Cassandra version** that wrote the SSTable and the file names (e.g.
   `nb-1-big-Data.db`).
2. The **CQL schema** for the table.
3. The exact **command or code** you ran and the output (or a hex dump for parsing
   issues).

[**→ Open an issue**](https://github.com/pmcfadin/cqlite/issues/new/choose)
</content>
</invoke>
