# CQLite v0.18.0 — the SSTable toolbox: detect, salvage and repair damaged SSTables offline

Released: TBD (set on the tag commit)

v0.18.0 adds the first offline tools for **damaged** Cassandra 5.0 SSTables: locate corruption down to
the chunk, offset and partition, sweep a whole data directory, salvage every decodable partition from a
damaged generation, and rebuild derived components from a healthy `Data.db`. It also adds a **raw
SSTable view** for querying unreconciled physical rows, and moves the Arrow stack to 59 and the Rust
floor to 1.95. Like every release, it targets **stock Cassandra 5.0** with no server-side changes.

This note is factual and cites the issues and PRs that shipped. The granular list is the `[v0.18.0]`
section of [`CHANGELOG.md`](../../CHANGELOG.md). Release binaries and the auto-generated GitHub release
body are produced by `release.yml` on the `v0.18.0` tag.

## Headline 1 — the SSTable toolbox (epic [#4192](https://github.com/pmcfadin/cqlite/issues/4192), first slice)

Four verbs ship in this release. They are offline: they read files on disk and never touch a running
node. Every verb has a JSON output mode and a documented exit-code contract, so it can be scripted.

- **`cqlite verify` now says where the damage is**
  ([#4194](https://github.com/pmcfadin/cqlite/issues/4194), PR [#4252](https://github.com/pmcfadin/cqlite/pull/4252)).
  Each finding carries a location: the component, byte offset, compression chunk index, the kind of
  anchor (`damaged_extent` or `declared_record`) and the partitions involved (`resolved`, with a
  truncation count, or `unresolved`). In JSON this is a new, additive `location` key per finding.
- **`cqlite sweep <data-dir> [--mode quick|full] [--out text|json] [--jobs N]`** (same PR) walks a
  Cassandra data directory (`<ks>/<table>-<id>/`), verifies every generation and reports each as
  `ok`, `degraded`, `corrupt` or `unreadable`. Exit code 2 if anything is corrupt or unreadable or no
  generations are found. `--jobs` defaults to 1 and is capped at 8.
- **`cqlite salvage <input> --out <dir>`**
  ([#4196](https://github.com/pmcfadin/cqlite/issues/4196), PR [#4213](https://github.com/pmcfadin/cqlite/pull/4213))
  recovers every fully decodable partition of a damaged BIG or BTI SSTable into a fresh, uncompressed
  generation and writes a JSON **loss manifest** naming what could not be recovered. Partition
  boundaries come from `Index.db`/`Partitions.db`, and a partition is recovered whole or not at all.
  Exit codes: 0 clean, 3 losses or a verification gap, 2 refused, 1 usage error.
- **`cqlite rebuild <input> --components <list> --out <dir>`**
  ([#4197](https://github.com/pmcfadin/cqlite/issues/4197), PR [#4250](https://github.com/pmcfadin/cqlite/pull/4250))
  regenerates derived components (`index`, `summary`, `filter`, `digest`, `toc`, `crc`, and the
  opt-in, declared-lossy `statistics`) from a healthy `Data.db`, which is never modified. It refuses
  with exit 2 when `Data.db` itself is damaged and points you to `salvage`.

**All four ship in the default CLI build** — the release binaries, the Homebrew tap and
`cargo install cqlite-cli`. As of this release `write-support` is a default `cqlite-cli` feature
([#4379](https://github.com/pmcfadin/cqlite/issues/4379)), which also brings `compact` into the
packaged CLI. The write surface is unchanged: CQLite still writes **uncompressed** SSTables only
([#1406](https://github.com/pmcfadin/cqlite/issues/1406)).

Declared limits of this slice:
- `verify` locations on BTI (`da`) SSTables are always `unresolved`
  ([#4337](https://github.com/pmcfadin/cqlite/issues/4337)).
- `salvage` refuses when the partition index itself is damaged; it does not scan `Data.db` for
  partition boundaries.
- `rebuild --in-place` always refuses for now (it depends on the consistency audit,
  [#4195](https://github.com/pmcfadin/cqlite/issues/4195)). On BTI input, `index` is refused and
  `summary`/`crc` report `skipped_not_applicable`. `Summary.db` is written with
  `min_index_interval` 128.
- The remaining verbs of the epic — `explain`, consistency audit, `scrub`, `extract`/`split`,
  tombstone `purge`, `diff`, `convert`, `rekey`, `diagnose`, `find` — are planned for 0.19.

## Headline 2 — the raw SSTable view

**`<ks>.<table>_raw_sstable_data`**
([#4222](https://github.com/pmcfadin/cqlite/issues/4222), PR [#4264](https://github.com/pmcfadin/cqlite/pull/4264))
is a virtual table you query through the existing `SELECT` path, so it works from the CLI, Python,
Node and Arrow Flight. It returns physical rows **before reconciliation** — one row per row per
SSTable generation — so you can see why a read returns what it returns:

- per-column write timestamp, TTL, local deletion time and tombstone flag
  (`<col>_timestamp`, `<col>_ttl`, `<col>_local_deletion_time`, `<col>_tombstone`), plus complex
  deletion info for collections and UDTs;
- row liveness and deletion (`row_timestamp`, `row_ttl`, `row_tombstone`, …), partition and range
  deletions, and `row_kind`;
- the source of each row: `sstable`, `generation`, `format`, `position`.

`ORDER BY`, `DISTINCT`, aggregates and `WHERE` clauses that cannot be pushed down are refused rather
than answered approximately. A real table whose name ends in `_raw_sstable_data` takes precedence. The
view is checked against `sstabledump` on all 22 spec fixtures with no divergence
([#4309](https://github.com/pmcfadin/cqlite/issues/4309), PR [#4312](https://github.com/pmcfadin/cqlite/pull/4312)).

## Also in this release

- **Writer statistics come from the data actually written**
  ([#4246](https://github.com/pmcfadin/cqlite/issues/4246), PR [#4289](https://github.com/pmcfadin/cqlite/pull/4289)).
  Rows dropped because a tombstone shadows them no longer lower the `Statistics.db` minimums or the
  `Data.db` delta-encoding baseline. This fixes a byte-parity divergence from Cassandra in flush,
  compaction and maintenance writes.
- **CommitLog reader hardening** ([#2798](https://github.com/pmcfadin/cqlite/issues/2798),
  PR [#4291](https://github.com/pmcfadin/cqlite/pull/4291)). Malformed descriptor JSON or UTF-8, a
  wrongly typed `compressionClass` and invalid backward sync markers are now rejected, following
  `cassandra-5.0.8`. `cqlite read-commitlog` reports `columns_read` and, in JSON,
  `last_mutation_partial`. A `--limit` that clips output now clears `all_updates_complete`.
- **Trino connector fat jar** ([#2869](https://github.com/pmcfadin/cqlite/issues/2869),
  PR [#4188](https://github.com/pmcfadin/cqlite/pull/4188)). Each release now carries a
  self-contained `cqlite-trino-<version>-all.jar` (plus `.sha256`) as a GitHub Release asset, so you
  can drop one file into a Trino plugin directory. Maven Central still gets the thin jar. Shading also
  fixes gRPC service-file merging, which had dropped gRPC's default load balancer.
- **Arrow 59** ([#4236](https://github.com/pmcfadin/cqlite/issues/4236),
  PR [#4242](https://github.com/pmcfadin/cqlite/pull/4242)): Arrow, Parquet and arrow-flight 53 → 59,
  tonic 0.12 → 0.14, and the non-default `datafusion-spike` feature of `cqlite-flight` from DataFusion
  44 to 55.1. Parquet and Flight output is semantically unchanged.

## Breaking changes

- **Minimum supported Rust version 1.85 → 1.95** (#4236).
- **Rust users of `cqlite-core`'s `arrow` or `parquet` features** now see Arrow 59 types in the public
  API (`export::arrow_size::arrow_payload_bytes`, `ArrowRowAccumulator::to_record_batch`). Upgrade
  your own `arrow` dependency to 59 alongside.
- **`cqlite-core` CommitLog API**: `PartitionUpdate` gains a public field `columns_read: bool`
  (#2798). The struct is not `#[non_exhaustive]`, so code that builds it with a struct literal needs
  the new field.
- **`cqlite-cli` default features** now include `write-support` (#4379). A build that must exclude
  the write surface uses `--no-default-features --features tui,cli-helpers`.

## Known issues and scope notes

- **A scan over an unreadable SSTable still returns `Ok(empty)` rather than an error**
  ([#4159](https://github.com/pmcfadin/cqlite/issues/4159), pre-existing; the fix, PR #4162, did not
  make this release).
- Carried from the 0.17 field run, still open: an unbounded `SELECT count(*)` through the Trino
  `cqlite` catalog can stall ([#4170](https://github.com/pmcfadin/cqlite/issues/4170)); scans are
  served by the `sidecar-uri` host's Flight pod only
  ([#4175](https://github.com/pmcfadin/cqlite/issues/4175)); `timeuuid` maps to `varchar` where the
  stock connector maps `uuid` ([#4173](https://github.com/pmcfadin/cqlite/issues/4173)).
- **Trino connector `cqlite-trino:0.18.0`** has no connector source change since 0.17.0; only its
  packaging changed (the fat jar above).
- The memory-allocator default for `cqlite-flight` is still undecided
  ([#4120](https://github.com/pmcfadin/cqlite/issues/4120)); `jemalloc` stays opt-in.
- Vortex columnar export ([#4237](https://github.com/pmcfadin/cqlite/issues/4237)) moved to 0.19.
