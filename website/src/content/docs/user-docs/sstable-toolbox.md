---
title: SSTable Toolbox
description: Offline verbs for Cassandra 5.0 SSTables — detect, diagnose, compare, recover and export — with their exit codes, read-only guarantees and declared limits.
sidebar:
  label: SSTable Toolbox
  order: 3.5
---

# SSTable Toolbox

CQLite ships a set of offline verbs for working with Cassandra 5.0 SSTable files **without a
cluster**. They are grouped by the question they answer:

| Question | Verb | Writes anything? |
|----------|------|------------------|
| Is it damaged, and where? | [`verify`](#verify--locate-damage-in-one-generation), [`sweep`](#sweep--verify-a-whole-data-directory), `verify --mode audit` | No |
| Where is my key, and what did the engine decide? | [`find`](#find--where-does-a-key-live), [`explain`](#explain--why-did-this-cell-win) | No |
| Is the table healthy and what would compaction reclaim? | [`diagnose`](#diagnose--files-only-health-report) | No |
| Do two copies of a table disagree? | [`diff`](#diff--compare-two-snapshots-of-a-table) | No |
| Get the good data out of a damaged SSTable | [`salvage`](#salvage--recover-whole-partitions), [`rebuild`](#rebuild--regenerate-derived-components) | Only into a separate `--out` directory |
| Move data to a columnar format | [`export --format vortex`](#vortex-export) | A new output file |

Everything here targets **Cassandra 5.0** (`na`/`nb` BIG and `oa`/`da` BTI). Every verb is built on
the same rule as the rest of CQLite: it uses authoritative metadata only (schema, else
`Statistics.db`), never a guess from byte patterns, and it fails closed with a named cause rather
than returning a shorter answer. Flags below are copied from `cqlite <verb> --help`; if your build
disagrees, trust `--help`.

:::note
`salvage`, `rebuild`, `diff`, `find` and `explain` need a build with `write-support`, which is on by
default in the packaged CLI. Vortex export needs the off-by-default `vortex` feature.
:::

## Read-only guarantees

- `verify`, `sweep`, `diagnose`, `find`, `explain` and `diff` never write to, create files in, or
  modify their input directories. `--out` on these verbs selects a **rendering on stdout**, never a
  destination file.
- `salvage` and `rebuild` never modify the input. `Data.db` is opened read-only. `--out` must not
  resolve inside the input tree, and output is published by rename from a staging directory, so a
  crash never leaves a component set that Cassandra could discover as complete (see
  [Crash safety](#crash-safety)).
- `salvage` and `rebuild` write **uncompressed** SSTables only, and never a `CompressionInfo.db`.

## Exit codes at a glance

| Verb | 0 | 1 | 2 | 3 |
|------|---|---|---|---|
| `verify` (quick/full) | all checks pass | usage error | verification failed | — |
| `verify --mode audit` | no `fail` row | usage error | any `fail`, or nothing was measured | — |
| `sweep` | every row `ok` (`degraded` alone does not trip it) | `<data-dir>` missing or not a directory | any row `corrupt` or `unreadable`, or no generation found | — |
| `find` | report complete (including "absent everywhere") | usage error | a generation or CommitLog segment could not be read | — |
| `explain` | trail rendered | usage, schema or table error | a generation cannot be read or rendered | — |
| `diagnose` | any successful report | usage error | — | — |
| `diff` | a report rendered (divergence is a finding) | usage error | unreadable generation, or the result budget was exceeded | — |
| `salvage` | everything recovered and every needed verification ran | usage error | refused, nothing written | output written with losses or a verification gap |
| `rebuild` | every requested component regenerated (or skipped as not applicable) | usage error | refused, nothing written | — |

## `verify` — locate damage in one generation

```bash
cqlite verify <generation-dir> [--mode quick|full|audit] [--out text|json] [--deep]
```

- `quick` checks component presence, `TOC.txt`, `Digest.crc32`, `CompressionInfo.db` and BTI trie
  structure. `full` (the default) adds chunk CRCs, `Statistics.db`/`Summary.db` parsing and a full
  row scan.
- Every finding carries its **location**: component, byte offset, compression chunk, and the
  partitions involved. BTI locations are always `unresolved`.
- `--out json` is stable and intended for CI artifacts.

### Audit mode

`--mode audit` asks a different question: do the components agree with **each other**? It prints a
fixed eight-row checklist, each row `pass`, `fail` (with a finding) or `skip` naming why:

| Row | Checks |
|-----|--------|
| `index-data-identity` | each `Index.db` entry lands on the matching `Data.db` partition header (BIG) |
| `summary-index-correlation` | each `Summary.db` sample resolves to the matching `Index.db` entry (BIG) |
| `bloom-no-false-negative` | `Filter.db` says yes for every key actually present (BIG and BTI) |
| `bti-trie-bounds` | every `Partitions.db` leaf is reachable and in bounds (BTI) |
| `toc-exact-listing` | `TOC.txt` lists exactly the components present |
| `digest-matches-data` | `Digest.crc32` matches `Data.db` |
| `compression-coverage` | the `CompressionInfo.db` chunk table covers exactly `Data.db`'s length |
| `statistics-vs-scan` | `Statistics.db` partition count equals a real scan (`--deep`) |

A skip is never a pass. An audit that **measured nothing** (every row skipped) exits 2 and says
`0 invariants MEASURED`. Without `--deep` the audit never reads a whole partition; `--deep` is a
usage error (exit 1) with any other mode. A present but unparseable `Filter.db` is a `skip` naming
the parse error.

## `sweep` — verify a whole data directory

```bash
cqlite sweep <data-dir> [--mode quick|full|audit] [--out text|json] [--jobs N]
```

Walks `<keyspace>/<table>-<id>/` (exactly two levels; `snapshots/`, `backups/` and secondary-index
subdirectories are not descended into), verifies **every** generation with the same pipeline as
`verify`, and reports one row per generation: `ok`, `degraded`, `corrupt` or `unreadable`.
`degraded` names a CQLite-only detection (for example a `Filter.db` false negative) and never trips
a non-zero exit by itself. A table directory with no readable generation is an `unreadable` row,
never a dropped one. `--jobs` defaults to 1 (sequential) and is clamped to 8; peak memory scales
with it because verification reads a whole `Data.db` into memory.

## `find` — where does a key live

```bash
cqlite --schema ks.cql find <table-dir> <partition-key> [--commitlog <dir>] [--out text|json]
```

`<partition-key>` is a CQL literal (comma-separated for a composite key). For each generation it
reports how the generation answered:

- `hit` with `offset`, `len` (or `to-eof`), `max_writetime` and `partition_deletion`;
- `bloom-negative` (BIG `Filter.db` said no);
- `index-miss` (`Index.db` or the BTI trie said no; also what an absent `Filter.db` degrades to);
- `scanned` (no usable index, so a schema-driven scan decided);
- `UNREADABLE <cause>` for a generation that could not be opened. It is **never** reported as absent.

With `--commitlog`, every mutation in every `*.log` segment touching the key is listed with
segment, byte position, writetime and `unflushed=yes|no|unmeasured(<cause>)`. `unflushed` is a
proof, not a timestamp comparison: `no` only when every cell, marker and deletion of the mutation
is proven present in the SSTables. Shapes the CommitLog decoder does not model (clustered tables,
complex columns, partition deletions) report `unmeasured(<cause>)`, never a guess, and an unreadable
segment is its own named line.

## `explain` — why did this cell win

```bash
cqlite --schema ks.cql --data-dir <data-dir> explain <ks.table> <partition-key> \
  [--clustering <v>]... [--now <epoch|RFC3339>] [--out table|json|csv]
```

`explain` resolves the table like `query` does (`--schema` plus `--data-dir` or `--dataset`) and
replays the reconciliation across **every** generation, showing for each cell which version won and
why (winner, shadowed by timestamp or tombstone, expired, purgeable, dropped column) plus the
tombstones in play. The first line states the clock (`now=...`, marked `(wall clock)` when you did
not pass `--now`) and the generation list. The `winner` set equals what `SELECT *` returns for that
partition at the same `now`.

- `--out` accepts only `table`, `json` and `csv`; `parquet` and `vortex` exit 1.
- `--clustering` filters the **rendering** only; tombstones of the partition still list.
- The same byte budget as the query surface applies (`performance.max_result_bytes`), and exceeding
  it is an error, not a truncated report.
- A key held by no generation is exit 0 with `0 generations hold this key`.

**Declared limit (#4601):** the trail reports **cell decisions only**. Row-level facts such as
whether a primary-key liveness marker survived, was purged or was shadowed, and key-column
shedding, are not traced. For a marker-only row `explain` prints no cell lines even though
`SELECT` returns the row.

`explain` is not `query --explain`, which prints a query plan.

## `diagnose` — files-only health report

```bash
cqlite diagnose <table-dir> [--now <epoch|RFC3339>] [--deep] [--top N] [--out text|json]
```

Reports per-generation facts from `Statistics.db`, `Summary.db` and `Partitions.db` (timestamps,
deletion times, droppable-tombstone estimate, repair state, compression ratio,
fully-expired-at-`now`) and the table's token-range overlap. The default tier **never opens
`Data.db`**. `--deep` adds one bounded scan per generation: partition-size and clustering-width
histograms, `--top N` rankings (default 5), raw tombstone counts and, when `--schema` is given, a
dry-compaction reclaim prediction.

Every number is tagged with its source (`statistics`, `index`, `scan`) or `unmeasured: <cause>`.
Figures are **raw, per generation and unreconciled**. A generation that cannot be read appears in
`unreadable_generations` with its cause rather than failing the report (exit 0 for any successful
report). No schema is needed for the cheap tier.

## `diff` — compare two snapshots of a table

```bash
cqlite --schema ks.tbl.cql diff <dir-a> <dir-b> --table <ks.table> \
  [--partition <key>]... [--now <epoch|RFC3339>] [--out text|json]
```

Compares the same table's **reconciled** state in two flat table directories (live generations
only, never `snapshots/` or `backups/`) and reports, per partition:

- cells as `equal`, `only-a`, `only-b`, `newer-in-a`, `newer-in-b` or `conflict` (same write
  timestamp, different content; both values shown with `resolves_to`, the side Cassandra keeps by
  `Cells.resolveRegular`);
- tombstones held by exactly one side (`only-a`, `only-b`), each with a `resurrection_risk`:
  whether the other side still holds live data the tombstone shadows, qualified `before-gc-grace`
  or `past-gc-grace` at `--now`. `past-gc-grace` is the strict `local_deletion_time < now -
  gc_grace_seconds` rule: the tombstone's holder may purge it, and the other side's data would
  return on the next repair or restore.

A tombstone both sides hold is omitted: the report shows **disagreement only**. The first line
states `now=` and each side's generations. `--partition` (repeatable) restricts the comparison
through the point-read path. `diff <a> <b>` and `diff <b> <a>` are label-swapped mirrors.

`diff` **never writes** to either directory and has no repair or apply mode; the report is input
to a repair, not a repair.

**Declared limits:**

- **Liveness-only rows (#4601).** A row whose only victim is its primary-key liveness marker (no
  cell) reports no resurrection risk, because the marker is not a cell in the reconciliation trail.
- **32 MiB cap.** The result budget is `max_result_bytes`, capped at **32 MiB** so peak memory stays
  under the 128 MiB target. Exceeding it is exit 2 with the same budget error the query surface
  emits, never a truncated report with exit 0.
- **Collections per element.** Cells are keyed by `(clustering, column, cell_path)`, so a map, set
  or list is compared element by element, and an element tombstone shadows only its element.
- **Damaged metadata fails closed.** A generation on either side with a damaged `Statistics.db`
  makes `diff` exit 2 naming `side=a|b` and the path.

## `salvage` — recover whole partitions

```bash
cqlite --schema ks.tbl.cql salvage <Data.db|table-dir> --out <dir> \
  [--table <ks.table>] [--manifest <path>] [--out-format text|json]
```

Recovers every **completely decodable** partition of a damaged SSTable into a fresh uncompressed
generation under `--out`, using the authoritative boundary source (`Index.db` or the `Partitions.db`
trie), never by scanning `Data.db` for a plausible header. A partition is recovered **whole or not
at all**: a later row can carry a tombstone that shadows earlier ones, so writing a prefix would
resurrect deleted data.

- A damaged `Index.db`/`Partitions.db` means boundaries are unknown, so salvage **refuses** and
  writes no `Data.db`; run `cqlite rebuild --components index` first.
- The loss manifest (JSON; one object for a single `Data.db`, an array for a directory) lists what
  could not be saved. `--manifest` is refused if it resolves inside the input, inside the planned
  output generation, or onto an existing SSTable component file.
- `--out` is refused (exit 1) when it resolves inside the input tree.
- The target table is derived from the input directory name (`<table>-<id>`) unless `--table`
  names it; salvage fails closed if neither resolves to exactly one `CREATE TABLE`.
- **Exit 3** means output was written with losses, or with a **verification gap**: an uncompressed
  input with no `CRC.db` has no chunk-CRC loss detection, so `losses: 0` means unmeasured, and the
  manifest names it `ChunkCrcUnavailable`.
- A damaged `Statistics.db` is a classified `ComponentUnreadable` refusal (exit 2) with a manifest.

## `rebuild` — regenerate derived components

```bash
cqlite --schema ks.tbl.cql rebuild <Data.db|table-dir> --components <list> --out <dir> \
  [--table <ks.table>] [--manifest <path>] [--out-format text|json]
```

Regenerates the components you name from a **healthy, unchanged** `Data.db`:
`index`, `summary`, `filter`, `digest`, `toc`, `crc` and `statistics`. There is no default list.
`Data.db` is opened read-only. `--out` receives a complete, self-contained component set: the
regenerated components plus every untouched original, copied verbatim.

- `statistics` is opt-in and **lossy by declaration**: aggregate fields are recomputed from a
  structural scan; `repaired_at`, `pending_repair` and `is_transient` are recovered only when the
  original `Statistics.db` is readable; origin host and compaction ancestry are always lost. The
  manifest states what was lost.
- `Summary.db`'s `min_index_interval` is always recomputed as 128, so a table that ever used a
  different value will not get a byte-identical `Summary.db`.
- A damaged `Data.db` (chunk-CRC failure, or a partition that fails to decode) **refuses**, writes
  nothing, and names `cqlite salvage` as the remedy.
- **Fails closed on a damaged `Statistics.db`.** A present but truncated or checksum-corrupt
  sibling `Statistics.db` stops the rebuild with a named error (exit 2); CQLite does not half-read
  it. Move the damaged file aside to rebuild without it. An unreadable file given as the statistics
  recovery source reports the repair fields as `lost`, never `recovered`.
- `--in-place` **refuses** (exit 1) in this release.
- BTI (`da`) input: `index` is refused as a usage error; `summary` and `crc` are reported
  `skipped_not_applicable`.

### Crash safety

Since 0.19, `salvage` and `rebuild` stage their output under `<out>/.cqlite-publish-<tag>.tmp/`,
fsync it, and publish by rename (`TOC.txt` first, `Data.db` last, directories fsynced). A crash at
any point leaves either nothing Cassandra can discover or a recognisable stage directory that a
re-run refuses by name. The `--manifest` file is published the same way (temp sibling, fsync, rename)
and a failed manifest write, or a full disk while writing `Filter.db`, **fails the run**.

## Vortex export

```bash
cqlite --schema basic-types.cql --data-dir <data-dir> \
  export results.vortex --format vortex --table test_basic.simple_table
```

Writes the reconciled `SELECT` row stream to a [Vortex](https://github.com/spiraldb/vortex)
columnar file through the same Arrow layer as Parquet, so the two formats agree value for value.
It is an **export target only**: it requires a build with the off-by-default `vortex` feature (and
a libclang shared library on the build host), needs a file destination (not stdout), and is rejected
by `query` and `read-sstable`. The writer is fail-closed: a conversion or write error aborts the
export and leaves no partial file. Type mapping, `TIMEUUID` handling and the no-byte-golden note are
on the [Output Formats](/cqlite/user-docs/output-formats/#vortex--vortex-columnar-file-format-export-only)
page.

## Where to go next

- [CLI Reference](/cqlite/user-docs/cli-reference/) — every flag of each verb.
- [Output Formats](/cqlite/user-docs/output-formats/) — Parquet and Vortex.
- [Troubleshooting](/cqlite/user-docs/troubleshooting/) — when a read fails.
