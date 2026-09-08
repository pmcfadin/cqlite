# CQLite v0.17.0 — read correctness and read-path performance against stock Cassandra 5

Released: 2026-09-08

v0.17.0 is the largest CQLite release to date: 97 issues closed across a read-correctness
campaign against Cassandra-written bytes, a measured read-path performance program on the Arrow
Flight server, a unified value/error contract across the Python, Node and CLI surfaces, and the
platform-hygiene epics (config honesty, observability, dead code). It targets **stock Cassandra 5.0**
— no server-side changes — and every parity fix in it is pinned against a Cassandra-written fixture or
the pinned `cassandra-5.0.8` source, never against CQLite's own prior output.

This note is factual and cites the issues/PRs that shipped. The granular list, including every
breaking change with before/after examples and consumer guidance, is the `[v0.17.0]` section of
[`CHANGELOG.md`](../../CHANGELOG.md). Release binaries and the auto-generated GitHub release body are
produced by `release.yml` on the `v0.17.0` tag.

## Headline 1 — read correctness: refuse or decode, never invent

A campaign over the row/cell decoder found and closed a family of silent-wrong-answer defects. The
common shape was a decode error swallowed into a truncated or empty result; the common fix is a
loud, typed error and a fixture-pinned oracle.

- **Silently truncated rows are gone** — row assembly no longer swallows a complex-column decode
  error into a row missing that column and every later one
  ([#3721](https://github.com/pmcfadin/cqlite/issues/3721)); partition-header decode errors are no
  longer hidden behind a byte-resync ([#3928](https://github.com/pmcfadin/cqlite/issues/3928)); the
  index-random-read path keeps the fatal error kind instead of degrading to a sequential fallback
  ([#3782](https://github.com/pmcfadin/cqlite/issues/3782)).
- **P0: misaligned cell offset on point reads and multi-chunk seeks**, hidden behind the swallow
  above on committed fixtures ([#3890](https://github.com/pmcfadin/cqlite/issues/3890)).
- **BTI `Rows.db` row-index root base** was 2 bytes low against Cassandra's `writeWithShortLength`
  framing, masked by a compensating encoder defect; both fixed, pinned against the real Cassandra 5.0
  `da` fixture ([#3002](https://github.com/pmcfadin/cqlite/issues/3002)).
- **Collections and nested types**: an empty `''` map key no longer drops its whole entry
  ([#3747](https://github.com/pmcfadin/cqlite/issues/3747)); an empty fixed-width set member is
  decoded as the EMPTY element instead of dropped ([#4106](https://github.com/pmcfadin/cqlite/issues/4106));
  nested fixed-width elements must have exactly Cassandra's length, not at-least
  ([#3723](https://github.com/pmcfadin/cqlite/issues/3723)); nested tuple/UDT/collection/duration
  consumption is bounds-checked below the first level ([#3778](https://github.com/pmcfadin/cqlite/issues/3778),
  [#3809](https://github.com/pmcfadin/cqlite/issues/3809), [#3811](https://github.com/pmcfadin/cqlite/issues/3811));
  composite (frozen tuple/UDT) collection keys decode through the full value deserializer
  ([#2339](https://github.com/pmcfadin/cqlite/issues/2339)); UDT field values of many types no longer
  decode as opaque `Blob` ([#3722](https://github.com/pmcfadin/cqlite/issues/3722)); the legal
  zero-length buffer is accepted for fixed-width scalars ([#3847](https://github.com/pmcfadin/cqlite/issues/3847))
  and a `blob_len` overflow in the frozen preamble is guarded ([#3848](https://github.com/pmcfadin/cqlite/issues/3848)).
- **Ordering parity**: `inet` and `time` compare as Cassandra's byte order, not as formatted strings
  ([#3790](https://github.com/pmcfadin/cqlite/issues/3790)); the whole-collection writer orders
  `set<time>`/`map<time,…>` elements as Cassandra's `TimeType` does
  ([#3935](https://github.com/pmcfadin/cqlite/issues/3935)).
- **Declaration validity**: `frozen<scalar>` is not declarable CQL and is now refused at both metadata
  entry points instead of decoded by guess ([#4104](https://github.com/pmcfadin/cqlite/issues/4104)),
  which exposed and fixed a **write-path** defect — the `Statistics.db` serialization header now
  carries Cassandra's real `FrozenType(UserType(…))` spelling for a `frozen<UDT>` column instead of
  `BytesType` ([#4158](https://github.com/pmcfadin/cqlite/issues/4158)), verified against a
  Cassandra-written header.
- **CLI JSON parity**: `float` is no longer widened to `f64` ([#3777](https://github.com/pmcfadin/cqlite/issues/3777));
  `decimal`/`varint` render as unquoted JSON numbers as `sstabledump` does
  ([#3644](https://github.com/pmcfadin/cqlite/issues/3644)); a UDT renders its declared fields only,
  with no injected `_type` key ([#3629](https://github.com/pmcfadin/cqlite/issues/3629)).
- **Two new parity lanes** back these: Parquet round-trip value parity
  ([#1490](https://github.com/pmcfadin/cqlite/issues/1490)) and JSON/CSV value parity against the
  `sstabledump` JSONL goldens ([#1491](https://github.com/pmcfadin/cqlite/issues/1491)).

## Headline 2 — read-path performance, measured

The 0.17 throughput program ([#2817](https://github.com/pmcfadin/cqlite/issues/2817)) measured before
it optimised, and every figure below is in a committed report under `docs/reports/`.

- **Flight `do_get` single-SSTable merge bypass** — a scan over one SSTable no longer pays the k-way
  merge/reconciliation it does not need: **3.06× more rows/s per core** on the reference corpus
  ([#3058](https://github.com/pmcfadin/cqlite/issues/3058)). Head-to-head on one physical core against
  stock Cassandra reading the same SSTable: CQLite's bare read path is **1.25×** Cassandra's `count(*)`
  and Flight shipping every row is **1.12×** Cassandra's `SELECT *`
  (`docs/reports/ws0-3100-report.md`), up from 0.29× on the served path in July.
- **Admission default derived from available parallelism** — `--max-concurrent-scans` follows core
  count instead of a constant 64, which cost 7–22% of throughput and 3–42× per-scan p50
  ([#3225](https://github.com/pmcfadin/cqlite/issues/3225)).
- **Byte-bounded Arrow batches and a wired per-stream result budget**
  ([#2825](https://github.com/pmcfadin/cqlite/issues/2825), [#2821](https://github.com/pmcfadin/cqlite/issues/2821)),
  the row-size estimate folded into the build pass ([#3552](https://github.com/pmcfadin/cqlite/issues/3552)),
  and scan-lifetime `madvise` plumbing ([#2824](https://github.com/pmcfadin/cqlite/issues/2824),
  [#3853](https://github.com/pmcfadin/cqlite/issues/3853)).
- **Box ceiling established**: bare scan 2.73M rows/s on 6 physical cores at 93.5% marginal
  efficiency ([#3299](https://github.com/pmcfadin/cqlite/issues/3299)); the served-path target was
  re-baselined to what the priced levers can reach ([#3553](https://github.com/pmcfadin/cqlite/issues/3553)).
- **Allocator**: the glibc malloc lock was measured serialising the second core; jemalloc measured
  +29% at a fixed pin and +61% with a second physical core under `LD_PRELOAD`
  ([#3551](https://github.com/pmcfadin/cqlite/issues/3551)). `cqlite-flight` now builds with a
  linked jemalloc behind the `jemalloc` feature, **off by default**; the shipping default is decided
  by the linked re-measurement in [#4120](https://github.com/pmcfadin/cqlite/issues/4120).
- **Field run on a 3-node Cassandra 5.0.9 cluster** (`docs/2024-09-meetup/REPORT.md`): a full-table
  analytic through CQLite raised the OLTP client's p99 2.21× vs 5.61× for the same query through
  Cassandra's own CQL path; the same Trino SQL over a 22M-row key-value table ran 1.66× faster through
  the `cqlite` catalog than the stock `cassandra` connector; 1.72M rows/s sustained from a single
  Flight pod. Single-pod results — see the known issues below.

## Headline 3 — one value and error contract across bindings

The bindings/FFI audit epics ([#1434](https://github.com/pmcfadin/cqlite/issues/1434),
[#1435](https://github.com/pmcfadin/cqlite/issues/1435), [#1436](https://github.com/pmcfadin/cqlite/issues/1436)):

- **Shared authoritative error table** — Python and Node map the same core error to the same identity
  ([#1451](https://github.com/pmcfadin/cqlite/issues/1451)), with the shared byte-math extracted to
  `cqlite-ffi-common` ([#1452](https://github.com/pmcfadin/cqlite/issues/1452)).
- **3-way golden parity** — the same `SELECT` through Python, Node and the CLI must yield equal
  canonical values, enforced in CI ([#1455](https://github.com/pmcfadin/cqlite/issues/1455)).
- UDT type identity carried out of band (breaking, see below); CQL `decimal` has one rendering in
  Python (breaking); dataset guards fail on present-but-empty roots
  ([#1458](https://github.com/pmcfadin/cqlite/issues/1458)); CI tests the claimed Python ≥3.9 / Node ≥18
  floors ([#1459](https://github.com/pmcfadin/cqlite/issues/1459)); stub-fidelity and exception-path
  leak tests ([#1456](https://github.com/pmcfadin/cqlite/issues/1456), [#1465](https://github.com/pmcfadin/cqlite/issues/1465)).

## Also in this release — platform hygiene epics

- **Correctness/safety landmines** (Epic AG, [#1684](https://github.com/pmcfadin/cqlite/issues/1684)):
  parser abort, unbounded channel, `block_on`, shutdown and data-in-logs classes closed.
- **Config honesty** (Epic AH, partial): ~40 decorative knobs deleted and `Config::validate()` made
  honest ([#1696](https://github.com/pmcfadin/cqlite/issues/1696)); one public `Config`
  ([#1697](https://github.com/pmcfadin/cqlite/issues/1697)); `query.max_execution_time` enforced
  ([#1695](https://github.com/pmcfadin/cqlite/issues/1695)); feature-matrix gate lanes
  ([#1699](https://github.com/pmcfadin/cqlite/issues/1699)). The epic's remainder rolls to 0.18.
- **Observability honesty** (Epic AI, [#1686](https://github.com/pmcfadin/cqlite/issues/1686)): the
  four dead read metrics wired ([#1701](https://github.com/pmcfadin/cqlite/issues/1701)), scan-path
  errors counted, a loud warning when OTel is enabled on a build without it, read-side phase timings.
- **Schema subsystem** (Epic AJ, [#1687](https://github.com/pmcfadin/cqlite/issues/1687)), **dead code +
  public-surface hygiene** (Epic AK, [#1688](https://github.com/pmcfadin/cqlite/issues/1688): `pub mod
  benchmarks` un-exported, never-compiled tests and the orphan schema JSON exporter deleted), and
  **CLI/TUI polish** (Epic AL, [#1689](https://github.com/pmcfadin/cqlite/issues/1689)).

## Breaking changes

Each entry in `CHANGELOG.md` `[v0.17.0]` carries before/after output and consumer guidance.

- `cqlite-core`: `Value` gains `Value::Empty(EmptyValueType)` (exhaustive matches need an arm, #3805);
  `Config.storage` is the single storage config and decorative knobs are deleted (#1696/#1697);
  `ffi_error_contract` moved to `cqlite-ffi-common` (#1451/#1452); the schema JSON exporter and
  `pub mod benchmarks` are gone (#1715/#1712); streaming-scan and single-partition-merger builder
  signatures changed (#2765/#2820 lineage); `ErrorCategory` renames (#1705).
- CLI `--format json`: `decimal`/`varint` unquoted (#3644), no UDT `_type` key (#3629), `float` not
  widened (#3777), `EMPTY` cell-path keys render `""` (#3805).
- Bindings: UDT type identity out of band (#1434 family), Python `decimal` single rendering, Node
  malformed-`inet` is a typed `PARSE` error.
- `cqlite-flight`: `--max-concurrent-scans` default now derives from available parallelism (#3225).

## Known issues and scope notes

- **Trino connector `cqlite-trino:0.17.0`** is content-identical to 0.16.1 (`git log v0.16.1..main --
  trino-connector/` is empty) — a tag-derived version realign, as 0.15.0 was.
- **A scan over an unreadable SSTable still returns `Ok(empty)` rather than an error**
  ([#4159](https://github.com/pmcfadin/cqlite/issues/4159), pre-existing in every prior release,
  fix in flight for 0.18). #4104's refusal surfaces the offending type at `StatisticsReader::open`;
  the scan-level propagation lands with #4159.
- Field-found on the 3-node run, all under investigation for 0.18: an unbounded `SELECT count(*)`
  through the Trino `cqlite` catalog can stall ([#4170](https://github.com/pmcfadin/cqlite/issues/4170));
  scans are served by the `sidecar-uri` host's Flight pod only, so multi-node fan-out is unmeasured
  ([#4175](https://github.com/pmcfadin/cqlite/issues/4175)); `timeuuid` maps to `varchar` where the
  stock connector maps `uuid` ([#4173](https://github.com/pmcfadin/cqlite/issues/4173)).
- **Parity claim scope**: the `required_parity` lane is green on the release commit; the weekly
  `exhaustive_regeneration` lane is red on corpus-inventory drift (new 0.17 fixtures without a
  regeneration recipe, [#3357](https://github.com/pmcfadin/cqlite/issues/3357)), not on a byte
  divergence. No unqualified "same tests as Cassandra" claim is made.
- Memory-allocator default and the served-path measurement program continue in 0.18
  ([#4120](https://github.com/pmcfadin/cqlite/issues/4120), [#2817](https://github.com/pmcfadin/cqlite/issues/2817)).
