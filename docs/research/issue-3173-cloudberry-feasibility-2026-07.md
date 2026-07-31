# CQLite × Apache Cloudberry — feasibility spike (issue #3173)

**Date**: 2026-07-30. **Question** (from #3173): *"Could this project serve as the backend for
https://cloudberry.apache.org/?"* **Method**: four independent research lanes — Cloudberry
architecture/community profile, integration-surface map + prior art, CQLite capability inventory
(repo audit at `f8f7adfcd`, v0.16.1), and a dedicated adversarial "why this is a bad fit" lane —
cross-checked against each other. All load-bearing claims cited; unverifiable ones marked
UNCONFIRMED.

**Bottom line up front**: CQLite cannot sensibly *be* Cloudberry's storage backend, and a native
in-process integration (FDW or table AM) is a bad fit on both the semantics and the cost — argued
in full in §4. But Cloudberry can *consume* CQLite **today, with zero new code**, through two
already-shipping paths: (a) bulk export (`cqlite export` Parquet/CSV → gpfdist/PXF) and (b)
federation via the documented PXF JDBC → Trino chain, which lands on the existing
`in.mcfad:cqlite-trino` connector. The recommended disposition is a docs recipe + a P3 watch-item,
not an engineering project.

---

## 1. What Apache Cloudberry is (facts as of 2026-07-30)

- **Greenplum 7 Beta 3 fork** by HashData Technology (2022, open-sourced 2023), created after
  Broadcom/VMware closed Greenplum's source (05/2024). Apache-2.0. Implemented in **C/C++**
  (~53% C, ~14% C++ — the C++ is largely the GPORCA optimizer).
  [Incubator proposal](https://cwiki.apache.org/confluence/display/INCUBATOR/CloudberryProposal)
- **ASF Incubator podling since 2024-10-11 — NOT graduated** (~21 months in; no graduation vote
  found; maturity phase "community building" across all seven board reports).
  [Status](https://incubator.apache.org/projects/cloudberry.html) ·
  [Clutch](https://incubator.apache.org/clutch/cloudberry.html)
- **Two ASF releases**: 2.0.0-incubating (2025-09-01, introduced the PAX hybrid row-column table
  AM) and 2.1.0-incubating (2026-04-15, UDP2 interconnect, first RPM/DEB packages).
- **Kernel**: shipped releases are **PostgreSQL 14.4**. A PG **16.9** upgrade (~5,730 upstream
  commits, year-long effort) **landed on `main` 2026-06-09** but has not shipped in any release;
  the announcement invites non-production testing only. Driver: PG14 EOL Nov 2026.
  [PG16 blog](https://cloudberry.apache.org/blog/postgresql16-for-apache-cloudberry-202606/)
- **Architecture**: classic Greenplum MPP — coordinator + independent PostgreSQL segment
  instances (no shared disk/memory), motion/interconnect layer (tcp/udpifc/proxy + new UDP2),
  GPORCA cost-based optimizer with PG-planner fallback, 2PC distributed transactions,
  `DISTRIBUTED BY` hash / `RANDOMLY` / `REPLICATED`. Storage AMs: heap, `ao_row`, `ao_column`,
  `pax` (contrib-level, build-time `--enable-pax`).
- **Community**: real but modest and single-vendor-weighted — ~90 distinct commit authors and
  ~833 commits on `main` in 12 months; 34 committers / 25 PPMC; dev@ ran **82 emails in a
  3-month reporting window**; backing from HashData (11/22 initial committers), Yandex Cloud,
  Synx Data Labs. No public adopters page; the only named production users are the sponsors'
  own commercial offerings. The incubation proposal itself flags single-company sponsorship as
  a risk.
- **Roadmap themes**: TLP graduation, annual PG kernel upgrades, vectorized execution, parallel
  ORCA, lakehouse plugins (**Iceberg first** — Cassandra appears nowhere), Kafka FDW, and a
  **Multi-Catalog System proposal**
  ([discussion #1297](https://github.com/apache/cloudberry/discussions/1297), "Under Discussion"
  since 2025-08-07, explicitly names Rust+pgrx as a plugin language, Arrow Flight not mentioned).

## 2. What "backend" could even mean — three readings

The issue's phrasing ("serve as the backend") admits three distinct readings that must not be
conflated:

| Reading | Meaning | Verdict |
|---|---|---|
| **(A) Storage backend** | Cloudberry's own tables physically stored as Cassandra SSTables (a `USING cqlite` table AM) | **No — wrong on the merits**, not just effort (§3.5) |
| **(B) External-data backend** | Cloudberry queries Cassandra SSTables in place, CQLite doing the decode (FDW / PXF / protocol) | Technically plausible in places; fails the cost/demand test (§4) |
| **(C) Loading backend** | CQLite exports, Cloudberry ingests into native storage | **Works today, zero new code** (§3.1) |

One physical constraint dominates every option in (B): **SSTables live on Cassandra nodes' local
disks, not on Cloudberry segment hosts.** CQLite's production topology already solves this by
running the Flight server *where the data is* and shipping Arrow over the wire; any in-process
Cloudberry surface either re-uses that remote-serving design (segments become Flight clients) or
requires shipping SSTable files onto segment hosts — an operational non-starter.

## 3. Surface-by-surface map (thorough — every plausible connection)

### 3.1 Bulk export → gpfdist/PXF/COPY — **works today, XS effort**

`cqlite` already exports **CSV, JSON, Parquet, and CQL** (`cqlite-cli/src/cli.rs` —
`OutputFormat`/`ExportFormat`), plus the CDC-style `delta-export` Parquet envelope. Cloudberry
ingests CSV via `gpfdist://` (parallel across all segments) and Parquet/ORC via PXF from object
stores. Additional supported hooks, all config-only:

- **gpfdist transformations**: a YAML config lets gpfdist invoke an arbitrary external program to
  convert a foreign format to CSV on the fly — a direct, supported hook for `cqlite export`.
- **`CREATE EXTERNAL WEB TABLE ... EXECUTE '<cmd>' ON ALL`**: every segment runs a command and
  consumes its stdout — the cheapest segment-parallel CQLite integration in existence. (Would
  want a `--shard i/N` token-range flag on the CLI, which does not exist today.)

Limitation: this is ETL, not federation — the copy goes stale. But it fully answers "can CQLite
feed Cloudberry": **yes, today, and it always could.**

### 3.2 Federation via PXF JDBC → Trino → cqlite-trino — **works today, config only**

Cloudberry ships PXF ([apache/cloudberry-pxf](https://github.com/apache/cloudberry-pxf), active,
JVM-based, released in lockstep at 2.1.0) with a JDBC connector, and the Greenplum PXF lineage has
a **documented worked example reading from Trino via JDBC**
([Broadcom docs](https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/tanzu-greenplum-platform-extension-framework/7-1/gp-pxf/jdbc_pxf_trino.html)).
CQLite already ships `in.mcfad:cqlite-trino` on Maven Central. So the chain

> **Cloudberry → PXF JDBC → Trino → cqlite-trino → Arrow Flight → CQLite → SSTables**

is buildable this week with **zero new CQLite code**. PXF JDBC even parallelizes
(`PARTITION_BY`/`RANGE`/`INTERVAL` fan a query into N concurrent connections from segment hosts).
Costs: three network hops, two serialization boundaries, a Trino cluster as a hard dependency —
latency will be poor relative to anything native. But it proves the integration end-to-end at
zero engineering risk. UNCONFIRMED: whether apache/cloudberry-pxf retains the Trino example
verbatim from GP PXF 7.x (it is a fork of that code; verify with one run).

### 3.3 Custom PXF connector — the best-fitting *native* abstraction (M–L, 4–8 weeks)

PXF's extension API (Fragmenter / Accessor / Resolver, Java) maps almost 1:1 onto what the Trino
connector already implements: **Fragmenter = token ranges from Cassandra Sidecar**
(`CqliteFlightSplitManager` is structurally a Fragmenter, including the #2237 ring-coverage
guard), **Accessor = a Flight `DoGet` against the fragment's replica host**, **Resolver = Arrow →
PXF fields**. Segment parallelism is PXF's whole reason for existing, so it comes free rather
than hand-rolled. No Cassandra PXF profile exists anywhere in the Greenplum ecosystem — this
would be genuinely novel and upstream-contributable. It is the correct target **if** demand ever
materializes; it is not justified by demand today (§4.3).

### 3.4 Native FDW (C or Rust) — plausible, expensive, and the wrong side of the trade

**Affirmative finding**: Cloudberry FDW scans **do** parallelize across segments —
`mpp_execute 'all segments'` dispatches the ForeignScan to every segment
([Cloudberry FDW dev guide](https://cloudberry.apache.org/docs/developer/write-a-foreign-data-wrapper/)).
But the framework does **no data partitioning for you**: the FDW must self-shard via
`GpIdentity.segindex` / `getgpsegmentCount()`, i.e. every segment must independently and
deterministically derive the same token-ring partitioning — re-implementing the Trino connector's
split manager and ring-coverage guard in C, where a mistake silently duplicates or drops rows.
The default `mpp_execute` is **coordinator-only** — the naive FDW funnels the whole scan through
one node. UNCONFIRMED: whether GPORCA participates in FDW pushdown planning at all (docs silent —
likely a sharp edge).

**The C path** requires first inventing a C ABI CQLite does not have (§4.2). **The Rust path is
blocked today**: Cloudberry's sanctioned pgrx is a community fork
([cloudberry-contrib/pgrx](https://github.com/cloudberry-contrib/pgrx) — **0 stars, last push
2026-01-29, pinned at pgrx ~0.12.7 with `pg14`+`cbdb` features**), while Supabase Wrappers pins
**`pgrx = "=0.16.1"`** exactly; four minor versions of a fast-moving unsafe-bindings API separate
them. Cloudberry's own pgrx docs: single-threaded execution only ("custom threads cannot call
internal database functions"), async "unexplored", build-from-source required, SQL_ASCII/UTF-8
panic hazard. CQLite is a tokio `rt-multi-thread` library with **1,186 `async fn`s**. No one has
shipped a Rust FDW on Cloudberry. Prior Cassandra FDWs
([pgsql-io/cassandra_fdw](https://github.com/pgsql-io/cassandra_fdw) dead 2020,
[rankactive/cassandra-fdw](https://github.com/rankactive/cassandra-fdw) dead 2021) are all
CQL-over-network, none SSTable-based, none MPP-aware — zero reuse value, though it confirms the
SSTable-direct niche is unoccupied.

### 3.5 Table access method (`USING cqlite`) — **ruled out on the merits**

Not just expensive (~44 MPP-aware callbacks, 6+ months) — *wrong*:

1. A table AM implies Cloudberry **owns** the files (writes, vacuums, MVCCs them). SSTables are
   immutable, Cassandra-written, reconciled at read time across generations.
2. The AM API operates on **local files on the segment host**; SSTables are elsewhere.
3. `tuple_insert`/`tuple_delete`/vacuum have no meaningful mapping onto tombstones, TTL, and LWW
   cell reconciliation.
4. A file-level AM would expose the *physical* view — tombstoned, shadowed, expired rows — which
   CQLite's own two-parity-oracles doctrine establishes is the wrong answer for `SELECT`.

Cloudberry's own AM story reinforces this: PAX, its flagship new AM, is **build-time enabled, not
an extension**, and the docs describe no third-party AM path.

### 3.6 Arrow Flight directly — no consumer exists on the other end

No Flight-*client* FDW for PostgreSQL exists anywhere (arrow-flight-sql-postgresql makes PG a
Flight *server*; PG-Strom's `arrow_fdw` reads Arrow *files*). Cloudberry does not speak Flight —
Arrow Flight is not mentioned even in its own multi-catalog proposal. One small engine-agnostic
improvement fell out of this lane regardless of Cloudberry: `cqlite-flight` returns a **single
`FlightEndpoint`** in `GetFlightInfo` (`cqlite-flight/src/service.rs:735`); returning one
endpoint per token-range slice would let *any* parallel consumer (PXF, FDW, ADBC client)
self-shard without a bespoke split manager.

## 4. The case against — why a native integration is a bad fit and a waste of time

This section is deliberately the longest. It was produced by a dedicated adversarial lane and
survived cross-checking against the other three.

### 4.1 Five concrete semantic breaks

An MPP warehouse assumes its storage hands back **settled rows from immutable, shardable
segments**. A Cassandra data directory violates every clause:

1. **Reconciliation cannot shard by file.** A row's value is a fold over *every* SSTable
   generation covering its partition (8-step, order-pinned, parity-critical —
   `merge/reconcile.rs`). You cannot give segment 3 generation-7 and segment 5 generation-9; the
   only correct shard axis is token range, which means **every segment opens every SSTable
   covering its range**: N segments × M generations of open handles, duplicated bloom/summary/
   index probes, duplicated page-cache pressure — for the same total row output. MPP fan-out
   multiplies index work instead of dividing it.
2. **No cross-segment snapshot pin.** Compaction unlinks generations mid-scan. CQLite needed
   snapshot-reuse windows + retire-grace (#2356/#2306) to make this safe *inside one process*;
   across N segment processes starting `BeginForeignScan` at different instants there is no
   shared snapshot object and no place in the FDW API to put one. Building one is a distributed
   lease coordinator, not an FDW.
3. **No shared pinned `now`.** TTL expiry and gc_grace purge are `now`-dependent — CQLite's own
   query-semantics oracle pins `now` for exactly this reason. Two segments initialized 200 ms
   apart silently disagree about which rows are alive.
4. **Distribution keys cannot align.** Cassandra partitions by Murmur3+vnodes; Cloudberry by its
   own hash on declared distribution columns. Every join between a Cassandra foreign table and a
   native table forces a redistribute/broadcast motion — the co-located join, the single biggest
   reason to put data *inside* an MPP warehouse, is structurally unavailable.
5. **Writes are a non-starter.** CQLite's production write surface is uncompressed-only
   (fail-closed, #1406), BIG-index-only, no counters, single-writer `&mut self`,
   fsync-per-write — a fixture/bulk-export path, not a warehouse write path. And
   `ExecForeignInsert/Update/Delete` semantics (row identity, constraints) have no Cassandra
   mapping anyway.

### 4.2 The FFI boundary is the expensive part, and CQLite has no C ABI

Verified against the tree at `f8f7adfcd`: **no `extern "C"`, no `#[no_mangle]`, no
cdylib/staticlib for `cqlite-core`, no header, no cbindgen config, no `cqlite-ffi` crate** — the
two cdylibs (PyO3, napi) are language-runtime ABIs, not C. A Cloudberry extension would need a
new, forever-versioned C ABI over a ~305k-LOC async library. The repo's own research has already
adjudicated this class of work: the #1934 assembled-engine record classifies row/cell
materialization across an FFI boundary as **"architecturally hostile … the #1 risk"**, and the
#2037 Arrow-OLAP research notes a Rust panic crossing into a host process aborts the whole
process — in a Greenplum segment, that takes down the segment and fails the cluster-wide query.
Compare the blast radius of the existing out-of-process path: a CQLite panic kills one Flight
`do_get`, and the Trino connector already has replica-failover tests. Add: PG14→PG16 kernel
migration guarantees ABI rework essentially on delivery; PG14's FDW read API is
tuple-at-a-time (`IterateForeignScan`), discarding the columnar batching the Flight path delivers
for free; and there is no Cloudberry (let alone a multi-segment cluster) anywhere in CQLite's CI,
whose full gate is already the project's throughput ceiling.

### 4.3 Strategic redundancy and the demand test

- **Arrow Flight is already the engine-agnostic surface**; Cloudberry doesn't speak it.
- **Trino already federates** — including *to Greenplum/Cloudberry itself* via its PostgreSQL
  connector. The federation #3173 asks about is achievable today with the arrow pointing the
  other way, at zero cost (§3.2).
- **Cloudberry's own roadmap targets Iceberg**, then Hive/Glue/JDBC/Delta. Cassandra appears
  nowhere in it.
- **The persona test fails**: no identified user runs an incubating Greenplum fork *and* holds
  raw Cassandra 5.0 SSTables *and* is not better served by Trino, Flight, or bulk export. The
  issue is a "what if" filed by the project owner at priority "Low — nice to have", not a user
  request.

### 4.4 Counterparty risk and opportunity cost

Cloudberry is 21 months into incubation with two releases, no graduation vote, single-vendor-
weighted development, mid-kernel-migration, and its sanctioned Rust path is a 0-star fork with
one commit in 2026. CQLite has ~484 open issues, an active 0.16/0.17 program with field users on
rc-pinned images (epic #2817), M6/M7 milestone commitments — and effectively one full-time
contributor. A credible native prototype (C ABI + FDW handler + token sharding + something
resembling snapshot pinning + a 3-segment test cluster) is realistically **4–8 weeks**, i.e.
roughly the remaining 0.16 budget, for zero known users — and it creates a permanent, in-process,
ABI-versioned, MPP-cluster-testing maintenance obligation afterward.

## 5. What survives (the steelman)

1. **Document the two zero-code paths** (§3.1, §3.2) as a recipe — "Query CQLite from
   Greenplum/Cloudberry via PXF JDBC → Trino; bulk-load via Parquet/CSV export." Half a day
   including one verification run. Honest, useful, and the answer a real user should get first.
2. **Multi-`FlightEndpoint` `GetFlightInfo`** (§3.6) — small, Cloudberry-independent server
   improvement that benefits every parallel non-Trino consumer. Worth its own groomed issue on
   its own merits.
3. **P3 watch-item on Cloudberry #1297** (Multi-Catalog System): if it ships a stable plugin API
   with Rust+pgrx as first-class *and* grows an Arrow-shaped ingest path *and* a named user asks,
   the calculus changes — Cloudberry's own layer would then own the marshalling. None of those
   preconditions holds today. Explicit revisit trigger, not a spike.
4. **Demand-gated escalation path**: if a real Cloudberry+Cassandra user appears, answer 1 is the
   recipe; only if it measurably fails a stated latency/throughput requirement does a **PXF
   connector** (§3.3 — the best-fitting native abstraction, ideally after refactoring the Trino
   connector's split/snapshot/ring-validation logic into a shared library) enter grooming. A C/
   Rust FDW and a table AM stay ruled out.

## 6. Recommended disposition for #3173 (owner's call)

- **Do not open an OpenSpec change; do not spike an integration.** Either close with the
  documented alternative, or park at P3/Backlog with the §5.3 trigger recorded.
- Optional cheap child issues if desired: (a) docs recipe (§5.1, docs-only, half a day);
  (b) multi-endpoint `GetFlightInfo` (§5.2, small, engine-agnostic).
- The §4.1 semantic findings are recorded here so the question is not re-litigated from scratch
  in six months.

## Appendix: consolidated UNCONFIRMED list

- Whether apache/cloudberry-pxf retains the GP-PXF JDBC→Trino example verbatim (fork of that
  code; verify with one run).
- Whether GPORCA participates in FDW qual/join/agg pushdown under MPP.
- Cloudberry graduation vote status after May 2026; which release first ships PG 16.9.
- Shipped status of `datalake_fdw` and the Multi-Catalog system (#1297 — discussion only as of
  2026-07-30).
- Whether `cloudberry-contrib/pgrx` will be ported to the PG16 kernel.
- Whether `GP_SEGMENT_ID` is exported into `EXTERNAL WEB TABLE EXECUTE` environments in
  Cloudberry specifically (documented in Greenplum).

## Appendix: primary sources

Cloudberry: [homepage](https://cloudberry.apache.org/) ·
[incubation status](https://incubator.apache.org/projects/cloudberry.html) ·
[clutch](https://incubator.apache.org/clutch/cloudberry.html) ·
[incubation proposal](https://cwiki.apache.org/confluence/display/INCUBATOR/CloudberryProposal) ·
[May 2026 podling report](https://cloudberry.apache.org/blog/apache-cloudberry-incubation-report-202605/) ·
[PG16 upgrade](https://cloudberry.apache.org/blog/postgresql16-for-apache-cloudberry-202606/) ·
[roadmap #868](https://github.com/apache/cloudberry/discussions/868) ·
[multi-catalog #1297](https://github.com/apache/cloudberry/discussions/1297) ·
[FDW dev guide](https://cloudberry.apache.org/docs/developer/write-a-foreign-data-wrapper/) ·
[pgrx guide](https://cloudberry.apache.org/docs/developer/develop-extensions-using-rust/) ·
[cloudberry-contrib/pgrx](https://github.com/cloudberry-contrib/pgrx) ·
[apache/cloudberry-pxf](https://github.com/apache/cloudberry-pxf) ·
[PAX](https://cloudberry.apache.org/docs/operate-with-data/pax-table-format/) ·
[CREATE EXTERNAL TABLE](https://cloudberry.apache.org/docs/sql-stmts/create-external-table/) ·
[gpfdist](https://cloudberry.apache.org/docs/data-loading/load-data-using-gpfdist/).
Greenplum lineage: [FDW/mpp_execute](https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/tanzu-greenplum/7/greenplum-database/admin_guide-external-g-devel-fdw.html) ·
[PXF JDBC→Trino example](https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/tanzu-greenplum-platform-extension-framework/7-1/gp-pxf/jdbc_pxf_trino.html) ·
[PXF JDBC partitioning](https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/tanzu-greenplum-platform-extension-framework/6-10/gp-pxf/jdbc_pxf.html) ·
[gpfdist transforms](https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/tanzu-greenplum/7/greenplum-database/admin_guide-load-topics-transforming-xml-data.html) ·
[CREATE PROTOCOL](https://docs.vmware.com/en/VMware-Greenplum/5/greenplum-database/ref_guide-sql_commands-CREATE_PROTOCOL.html).
Prior art: [pgsql-io/cassandra_fdw](https://github.com/pgsql-io/cassandra_fdw) ·
[rankactive/cassandra-fdw](https://github.com/rankactive/cassandra-fdw) ·
[Supabase Wrappers](https://supabase.github.io/wrappers/) ·
[pgrx upstream](https://github.com/pgcentralfoundation/pgrx) ·
[arrow-flight-sql-postgresql](https://arrow.apache.org/flight-sql-postgresql/) ·
[PG-Strom arrow_fdw](https://heterodb.github.io/pg-strom/arrow_fdw/) ·
[Trino Cassandra connector](https://trino.io/docs/current/connector/cassandra.html).
CQLite internal: `docs/architecture/issue-1934-assembled-engine-research.md` ·
`docs/architecture/issue-2037-arrow-olap-research.md` ·
`docs/architecture/throughput-program-2026-07.md` · `docs/research/phase1-7-trino-question.md` ·
`trino-connector/` (`CqliteFlightSplitManager`) · `cqlite-flight/src/service.rs` ·
`cqlite-cli/src/cli.rs`.
