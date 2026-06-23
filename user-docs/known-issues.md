---
title: Known Issues
description: Active bugs and sharp edges in the current CQLite release, with issue links — what to watch out for today.
sidebar:
  label: Known Issues
  order: 8
---

# Known Issues

This page tracks **active bugs and sharp edges** in the current release (`v0.12.0`).
It is deliberately short and honest: if something here bites you, you are not doing
it wrong.

- For things CQLite **does not do by design or yet**, see
  [Limitations](/cqlite/user-docs/limitations/).
- For **planned work and milestones**, see [Roadmap](/cqlite/user-docs/roadmap/).
- Hit something not listed here?
  [**Open an issue**](https://github.com/pmcfadin/cqlite/issues/new/choose) — that is
  the single most useful thing you can do for the project.

_Last reviewed: 2026-06-22 (v0.12.0)._

## Python bindings

### `SET<FROZEN<UDT>>` fails to deserialize ([#804](https://github.com/pmcfadin/cqlite/issues/804))

Querying a table with a `set<frozen<UDT>>` column through the Python bindings raises
a `TypeError`: UDT values are decoded as dicts and the binding tries to place them in
a `frozenset`, but dicts are unhashable. The CLI and Rust reads of the same table are
unaffected. Tracked in [#804](https://github.com/pmcfadin/cqlite/issues/804).

### Concurrent queries can race ([#805](https://github.com/pmcfadin/cqlite/issues/805))

Multiple threads issuing queries against the **same** `Database` object can
intermittently fail with `Column not found: id`. A warm-up query reduces but does not
fully eliminate the race. Workaround: open one `Database` per thread, or serialize
queries through a single thread. Tracked in
[#805](https://github.com/pmcfadin/cqlite/issues/805).

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
