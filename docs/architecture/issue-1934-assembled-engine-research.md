# The assembled-engine strategy — CQLite as Cassandra's HTAP storage engine (research record)

**Status**: research record for an owner product decision (Seam-1-style). Not a proposal, not
an OpenSpec change. Consolidates: (1) five engine-swap post-mortem investigations (Rocksandra,
MyRocks/MongoDB/InnoDB-family, OrioleDB/zheap/Citus-Columnar/ZedStore, ScyllaDB/YugabyteDB), (2) a
source-verified audit of the `SSTableFormat` pluggability seam, and (3) a product-frame analysis of
how in-flight CQLite epics (#1807, #905, #2037, #941, #1934) compose. Related records:
`docs/architecture/issue-2037-arrow-olap-research.md`,
`docs/architecture/issue-905-compaction-manager-research.md`,
`docs/storage engine/report-2-storage-engine-feasibility.md`.

---

## 1. The question and the constraint

### The analogy, and why it breaks

The pitch under evaluation is "CQLite : Cassandra :: WiredTiger : MongoDB, or MyRocks : MySQL" —
a pluggable storage engine that swaps in for better performance/analytics while the host database's
query surface, replication, and operational tooling stay put. Both reference cases rest on a
precondition CQLite does not have: a **stable, first-class, host-owned storage-engine SPI** that a
third party implements against. MySQL's `handlerton`/`handler` interface predates InnoDB's own
plugin-era hardening; MongoDB shipped an explicit pluggable-storage-engine architecture in 3.0
(2015) specifically to make WiredTiger (and alternatives) possible.

**Cassandra has no such SPI, and the one serious attempt to build one failed.** Instagram's
CASSANDRA-13474/13475/13476 (2017) proposed exactly this: a RocksDB-backed engine plus an upstream
pluggable-storage-engine API modeled explicitly on MySQL/MongoDB. The RocksDB engine shipped and ran
in Instagram production for years; **the abstraction (13475) never landed** — the epic is still
open, unassigned, and the fork went silent in 2019. Committer Jeremiah Jordan's contemporaneous
review comment identifies the proximate cause precisely: an SPI proposed by, and validated by,
exactly one implementation cannot demonstrate its abstraction boundary is drawn correctly, because
there's no second engine to test it against. PMC member Jeff Jirsa's 2021 retrospective is blunter
still: the work was "built (mostly? completely?) by one company for their use case," and died when
that company's priorities moved on — not a rejected proposal, an unforced abandonment. See §2 for
the full case and its lessons.

### The reframe: assembled, not replaced

CQLite's actual strategy is not "implement a StorageEngine SPI Cassandra doesn't have." It is:
**compose the seams that already exist and are already multiply-implemented upstream**, and never
touch the seams that don't exist. Per `report-2-storage-engine-feasibility.md` §1.2 (seam
inventory, verified in source) and §1.3:

**Seams that exist** (host-owned, pluggable today):
- **CEP-11 pluggable memtable** — committed April 2022, explicitly scoped down from
  CASSANDRA-13475's whole-engine ambition "to avoid duplication of the on-disk format, streaming
  and compaction implementations" (CEP-11 design doc). Already multiply-implemented
  (`TrieMemtable`, `SkipListMemtable`, `ShardedSkipListMemtable`) — the precondition Rocksandra's
  SPI lacked.
- **Compaction strategy** — a real, host-owned, multiply-implemented (STCS/LCS/UCS) pluggable
  point; #905's compaction-manager epic builds on it.
- **`SSTableFormat.Factory`** — a genuine `ServiceLoader`-discovered SPI, in-tree since CEP-17, with
  an authoring guide (`SSTable_API.md`) and two shipped implementations (Big, BTI) proving the
  minimal-format surface is small. Unexplored by any in-flight CQLite epic before this record; see
  §3 for the full source-verified audit.
- **Partial commit-log opt-out** — a narrower seam noted in report-2's mechanism map (§1.2), not
  separately re-verified in this record.

**Seams that do not exist** (per report-2 §1.2/§2.3/§3.4, verdict: "Adjacent: yes. Replacement:
no."): the **read-merge path**, **`Tracker`/`View`** (the in-memory view of a table's live
SSTables + memtables), **streaming**, and **repair**. These are not partially pluggable or
pluggable-with-effort — report-2's part-(a) analysis (replacement engine inside Cassandra, §2)
concludes the concrete change inventory required to touch them is a fork, not a seam. CQLite's
strategy is built on never needing to.

### The format-compatibility moat

The property that makes this strategy structurally different from every full-rewrite precedent in
§2 (ScyllaDB, YugabyteDB): **CQLite never writes a foreign on-disk format.** Compaction and flush
output stay byte-parity `nb`/BIG or canonical `da`/BTI SSTables Cassandra itself can read, with or
without CQLite installed. This buys two properties no rewrite case in this record has:

- **Zero-migration adoption.** A table opts in by installing the memtable/compaction plugin; no
  schema change, no re-ingestion, no dual-write verification pipeline (contrast ScyllaDB's
  documented migration process: manual schema recreation, branded sstableloader, dual-write
  verification — a real migration, not a bare format copy, despite the "format-compatible" claim).
- **Rollback = remove the plugin.** If the bet doesn't pay off, uninstalling the plugin returns the
  table to stock Cassandra behavior with no data loss and no re-migration. No post-mortem case in
  §2 has this property as cleanly: Scylla's shadowable-deletion flag makes its format
  compatibility one-way (Cassandra → Scylla only, for schemas with materialized views); Yugabyte's
  DocDB has no bare-format export path back to Cassandra at all.

This is the moat this entire strategy rests its adoption argument on. Everything in §2–§5 exists to
pressure-test whether the engineering path to exploiting it is sound.

---

## 2. Engine-swap post-mortems

### Comparison table

| Case | Seam type | Outcome | One-line verdict |
|---|---|---|---|
| **Rocksandra / CASSANDRA-13474-6** | Proposed new SPI, sole implementation | SPI abandoned (2019); engine ran in prod for years | Sole-sponsor SPIs don't get community sign-off |
| **CEP-11 memtable** | Narrow, scoped-down seam | Shipped (2022), multiply-implemented | Scoping down from 13475 is *why* it succeeded |
| **MyRocks** | Old, stable seam (MySQL handlerton) | Thriving; Percona GA 2018, MariaDB stable 10.3.7 | Old+stable seam + published production number converts adopters |
| **MongoDB pluggable storage** | New seam (3.0), then feature-coupled | Collapsed to WiredTiger-only by 4.2 | Feature coupling (txns, sessions) breaks a seam that isn't architecturally isolated |
| **InnoDB** | Handler API, vendor-consolidated | Became mandatory default (5.5) | Vendor consolidation, not technical merit, secured dominance |
| **Falcon** | New engine, no seam problem — strategic problem | Abandoned pre-release (2009) | Strategically unnecessary the moment the owner consolidated |
| **TokuDB** | Stable seam, upstream churn | Deprecated 2021, removed 2022 | Technically superior + no adopter base + upstream churn = death |
| **PBXT** | Tight coupling to internals, not a seam | Unmaintained, fails test suites | Coupling to internals (not a seam) is unsustainable for a third party |
| **Aria** | Uncontested niche, creator-controlled | Default non-transactional engine since 5.1 | Uncontested niche + forced adoption hardens fast |
| **Spider** | First-party specialized seam | Actively maintained, 100TB+ production | First-party ownership + narrow scope sustains decades |
| **OrioleDB** | Real seam (TAM), still needs core co-patches | In progress, 7+ years, not yet a pure extension | Pluggable ≠ sufficient; budget standing co-evolution with upstream |
| **zheap** | Attempted new heap, no seam existed yet | Stalled/dormant, ~9 years | Deep semantic changes (MVCC) are a systems project, not a plugin |
| **Citus Columnar** | Real seam (TAM), scope matched to it | Shipped 2021, alive today (funded owner) | Honest narrow scope + funded ongoing owner = durable |
| **TimescaleDB Hypercore** | Real seam (TAM), scope matched | Deprecated 2025 for product reasons, not API failure | Even a working plugin dies for product-strategy reasons unrelated to the seam |
| **ZedStore** | Community commitfest submission, committer + vendor backing | Never merged; dead by 2020 | Committer credibility + vendor funding ≠ community mergeability |
| **ScyllaDB** | Full rewrite, format-compatible reads | Success, 10+ years, but format diverged (shadowable-deletion) | Format-compatible reads ≠ write-format compatibility forever |
| **YugabyteDB** | New engine (DocDB), API-only compatibility | YCQL alive but deprioritized; growth is YSQL | API compatibility without semantic compatibility is a false economy |

### Per-case notes

**Rocksandra + CASSANDRA-13474/13475/13476.** Dikang Gu (Instagram) filed 13474 in April 2017
proposing a pluggable storage engine "like MySQL and MongoDB," with a published design doc and a
blog post reporting P99 read latency dropping from 60ms to 20ms and GC stalls from 2.5% to 0.3% on
one production cluster. Twelve sub-tasks were spawned; only "refactor streaming," "refactor
repair," and "refactor write path" were ever resolved — the SPI itself (13475) sits open and
unassigned to this day. Stefan Podkowinski questioned the premise on the mailing list (why build
pluggability before a second engine proves it's needed); Instagram's answer was to ship working
code rather than debate. The decisive governance objection came from Jeremiah Jordan: the SPI
"needs to be implemented with a new engine implementation to go with it" — without a second real,
independent engine, reviewers had no way to know the abstraction boundary was drawn in the right
place rather than shaped around RocksDB's specific semantics. Four years later, Jeff Jirsa's
retrospective on the user-list confirms it: built by one company for their use case, momentum died
when that company's priorities moved elsewhere. The one part of this ambition that *did* eventually
ship was CEP-11's pluggable memtable — deliberately scoped down from 13475's whole-engine swap to
memtables only, precisely to avoid "duplication of the on-disk format, streaming and compaction
implementations."

*Sources*: [CASSANDRA-13474](https://issues.apache.org/jira/browse/CASSANDRA-13474),
[CASSANDRA-13475](https://issues.apache.org/jira/browse/CASSANDRA-13475),
[CASSANDRA-13476](https://issues.apache.org/jira/browse/CASSANDRA-13476),
[design doc](https://docs.google.com/document/d/1suZlvhzgB6NIyBNpM9nxoHxz_Ri7qAm-UEO8v8AIFsc),
[Instagram Engineering blog](https://instagram-engineering.com/open-sourcing-a-10x-reduction-in-apache-cassandra-tail-latency-d64f86b43589),
[dev@cassandra: RocksDB experiment result](https://www.mail-archive.com/dev@cassandra.apache.org//msg11064.html),
[narkive: pluggable storage engine discussion](https://dev.cassandra.apache.narkive.com/SnsveVLp/pluggable-storage-engine-discussion),
[Jeremiah Jordan JIRA comment](https://www.mail-archive.com/commits@cassandra.apache.org/msg181991.html),
[user@cassandra: "What Happened To Alternate Storage And Rocksandra?"](https://www.mail-archive.com/user@cassandra.apache.org/msg61642.html),
[GitHub: Instagram/cassandra rocks_3.0 (archived)](https://github.com/Instagram/cassandra/tree/rocks_3.0),
[CEP-11: Pluggable Memtable Implementations](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=184617682).
*(Note: the GitHub archival date and commit/star counts are from an automated page-fetch summary,
not independently re-verified — likely-accurate but unconfirmed.)*

**MyRocks.** Began as Facebook research (2014) pairing RocksDB with MySQL via MySQL's existing
`handlerton`/`handler` plugin architecture — the same interface InnoDB itself uses, stable since
the InnoDB-as-plugin era, so no core-server fork was required. Facebook's UDB migration (2017) was
zero-downtime (dual-write/verify against InnoDB), and the published payoff was concrete: 62.3%
smaller instance size, ~75% less write amplification (VLDB 2020, Matsunobu et al.). That number is
what converted Percona (GA 5.7.20-19, Jan 2018) and MariaDB (stable 10.3.7) from curious to
committed — not an architecture pitch. MyRocks never achieved full parity: no gap/next-key locking
(forcing `binlog_format=ROW`, ruling out `SERIALIZABLE`), no online DDL at launch, no savepoints, no
foreign keys/spatial/fulltext indexes. These gaps were tolerated for years because the sponsor's own
workload didn't need them, and downstream adopters inherited both the engine and the same
tolerance list.

*Sources*: [Engineering at Meta (2016)](https://engineering.fb.com/2016/08/31/core-infra/myrocks-a-space-and-write-optimized-mysql-database/),
[VLDB 2020 paper](https://www.vldb.org/pvldb/vol13/p3217-matsunobu.pdf),
[MyRocks Getting Started wiki](https://github.com/facebook/mysql-5.6/wiki/Getting-Started-with-MyRocks),
[MyRocks limitations wiki](https://github.com/facebook/mysql-5.6/wiki/MyRocks-limitations),
[gap-lock issue #800](https://github.com/facebook/mysql-5.6/issues/800),
[issue #177](https://github.com/facebook/mysql-5.6/issues/177),
[MySQL 8.0 Reference Manual, pluggable storage architecture](https://dev.mysql.com/doc/refman/8.0/en/pluggable-storage-overview.html),
[Percona blog](https://www.percona.com/blog/myrocks-percona-server-mysql/),
[MariaDB docs](https://mariadb.com/kb/en/differences-between-myrocks-variants/),
[Percona: when to use MyRocks](https://www.percona.com/blog/when-to-use-myrocks-in-mysql/),
[myrocks.io](http://myrocks.io/).

**MongoDB pluggable storage engine.** MongoDB shipped its pluggable API in 3.0 (2015) after
acquiring WiredTiger Inc. (Dec 2014), delivering 7x-10x write throughput and document-level locking
over MMAPv1's collection-lock ceiling; the design allowed multiple engines to coexist in a replica
set with zero-downtime migration. By 3.6, sessions/retryable writes/causal consistency required
significant architectural changes to the storage layer itself, not just engine-level optimization.
MongoDB 4.0's multi-document transactions reached deeper still — Percona called rearchitecting
MongoRocks for transactions "a massive undertaking" and deprecated it (April 2018). The deeper cause
was irreconcilable design philosophy: PerconaFT used pessimistic locking, MongoDB's core used
optimistic locking, requiring Percona to simulate cheap latches in an optimistic-locking world.
MongoDB deprecated MMAPv1 in 4.0, removed it in 4.2, leaving WiredTiger (and Enterprise-only
In-Memory) as the only paths forward. The transferable lesson: **feature coupling through shared
storage layer code kills alternative engines** — an API boundary that isn't architecture-first (it
was retrofit after MMAPv1 shipped) gets redrawn every time a major feature needs deeper access, and
only the engine that's *also* the API's reference implementation (WiredTiger) survives that
redrawing.

*Sources*: [InfoQ, MongoDB 3.0 WiredTiger](https://www.infoq.com/news/2015/02/MongoDB-3.0-WiredTiger-MMS/),
[MongoDB storage engine docs](https://www.mongodb.com/docs/manual/core/storage-engines/),
[DBTA, WiredTiger revolutionized MongoDB](https://www.dbta.com/Columns/MongoDB-Matters/How-WiredTiger-Revolutionized-MongoDB-145510.aspx),
[Percona: MongoRocks deprecation](https://www.percona.com/blog/why-weve-deprecated-mongorocks-in-percona-server-for-mongodb-3-6/),
[Percona: PerconaFT locking incompatibility](https://www.percona.com/blog/mongorocks-deprecating-perconaft-mongodb-optimistic-locking/),
[ScaleGrid, WiredTiger vs MMAPv1](https://scalegrid.io/blog/mongodb-storage-engines/),
[Alibaba Cloud, MongoDB 4.0/4.2 breaking changes](https://www.alibabacloud.com/help/en/mongodb/product-overview/mongodb-versions-and-storage-engines),
[MongoDB, WiredTiger checkpointing](https://www.mongodb.com/docs/manual/core/wiredtiger/),
[MongoDB, replica set oplog](https://www.mongodb.com/docs/manual/core/replica-set-oplog/),
[SolarWinds, why WiredTiger is default](https://orangematter.solarwinds.com/2017/06/14/why-wiredtiger-is-the-default-mongodb-storage-engine/).

**InnoDB.** Survived because Oracle consolidated ownership of both MySQL and InnoDB (Oracle
acquired InnoDB's owner in 2005, then Sun/MySQL in 2010), eliminating any competitive pressure to
build an alternative. Became MySQL's default in 5.5 (2010), replacing MyISAM. Success here is partly
orthogonal to technical merit — it was a licensing/ownership consolidation, not a seam-quality story.

*Source*: [InnoDB — Wikipedia](https://en.wikipedia.org/wiki/InnoDB).

**Falcon.** MySQL AB's answer to Oracle's 2005 InnoDB acquisition — a next-gen transactional engine
for MySQL 6.0. Oracle's subsequent acquisition of Sun (which owned MySQL) eliminated the strategic
rationale entirely; development halted with zero releases. Post-mortems cite management pressure for
performance over correctness, signaling misaligned incentives even before the strategic rug-pull.

*Sources*: [Falcon — Wikipedia](https://en.wikipedia.org/wiki/Falcon_(storage_engine)),
[InfoWorld](https://www.infoworld.com/article/2198360/falcon-to-be-the-major-piece-of-mysql-6-0-2.html),
[Percona design review](https://www.percona.com/blog/falcon-storage-engine-design-review/).

**TokuDB.** Fractal-tree indexing gave 10x lower write amplification and 2-3x compression vs
InnoDB — a real technical win. Died anyway: very little adoption in Percona Server 8.0, and ongoing
MySQL 8.0 upstream churn made each minor-version handler-API compliance update a recurring cost that
outpaced the adopter base paying for it. Deprecation began 8.0.25 (2021), disabled by default 8.0.26
(2021), fully removed 8.0.28 (2022). Percona's own recommendation on deprecation was to migrate to
MyRocks.

*Sources*: [TokuDB — Wikipedia](https://en.wikipedia.org/wiki/TokuDB),
[Percona: when to use MyRocks](https://www.percona.com/blog/when-to-use-myrocks-in-mysql/),
[Percona: TokuDB support changes](https://www.percona.com/blog/tokudb-support-changes-and-future-removal-from-percona-server-for-mysql-8-0/),
[Percona: TokuDB disabled reminder](https://www.percona.com/blog/tokudb-storage-engine-will-be-disabled-by-default-in-percona-server-for-mysql-8-0-26/),
[Percona forums: why deprecated](https://forums.percona.com/t/why-tokudb-deprecated/6866).

**PBXT (PrimeBase XT).** Technically innovative (engine-level replication, BLOB streaming) but
relied heavily on MySQL internal structures and the internal TABLE dictionary — not a seam, a
coupling. As MySQL internals shifted, PBXT broke; when bundled into MariaDB 5.5 source builds it
failed many test suites. No commercial entity had the resources to maintain it against upstream
drift; by the 2010s it was unmaintained and absent from upstream MariaDB binaries. The project's own
blog acknowledged "a marketing problem with no clue how to get a message across."

*Sources*: [Gentoo dev-list, PBXT removal](https://www.mail-archive.com/gentoo-dev@lists.gentoo.org/msg58596.html),
[Launchpad bug, PBXT embedded-server crash](https://bugs.launchpad.net/maria/+bug/439889),
[Flaming Spork, "Where are they now"](https://www.flamingspork.com/blog/2013/04/18/where-are-they-now-mysql-storage-engines/),
[pbxt.blogspot.com](https://pbxt.blogspot.com/).

**Aria.** MariaDB's own core team (the engineers who built MyISAM) built Aria (2007+) as a
crash-safe MyISAM successor, not a head-to-head competitor to InnoDB — the niche (non-transactional,
crash-safe) was uncontested. Compiled into MariaDB 5.1 by default, mandatory at startup, with all
system tables migrated to Aria format — forced adoption hardened it fast, and creator control meant
no buyer's-choice fragmentation.

*Source*: [Aria Storage Engine — MariaDB docs](https://mariadb.com/docs/server/server-usage/storage-engines/aria/aria-storage-engine).

**Spider.** MariaDB's in-house horizontal-sharding engine, in-tree since 5.1, still evolving (v10.11
replaced COMMENT-based config with structured attributes; its own HA feature was recently deprecated
in favor of external tools — disciplined scope management, not neglect). Verified production use:
Tencent Games at 100TB+ across 396 Spider nodes + 2,800 data nodes. Maintained by the distribution's
own engineering team, not a third party gluing onto internals — the PBXT failure mode inverted.

*Sources*: [Spider overview — MariaDB KB](https://mariadb.com/kb/en/spider-storage-engine-overview/),
[Spider case studies — MariaDB docs](https://mariadb.com/docs/server/server-usage/storage-engines/spider/spider-case-studies).

**OrioleDB.** Built on PostgreSQL's Table Access Method (TAM) API (landed PG12, 2019), but founder
Alexander Korotkov (a Postgres core committer) has needed a standing stream of core patches to close
real TAM gaps: no per-index update control (blocking undo-log/WARM techniques), row identifiers
hardwired to an 11-bit block/offset pair (ruling out arbitrary-length TIDs an undo-based design
needs). Real, measured progress — the out-of-tree patch delta shrank from ~5,000 lines (PG14) to
~2,000 lines (PG16), Supabase's own accounting puts it at "~60% already committed to core" — but as
of mid-2026 OrioleDB still ships as a patched Postgres build, not a pure extension; an independent
2026 field survey classifies it "public beta, not production-ready" and the engine "most likely to
discover edge cases in the TAM API that nobody else has hit," precisely because it stresses the API
harder than any other adopter. *(Note: the 5,000→2,000-line and "~60%" figures are vendor-reported
via Supabase's blog, not independently re-verified against raw diffs.)*

*Sources*: [OrioleDB: why Postgres needs a better TAM API](https://www.orioledb.com/blog/better-table-access-methods),
[OrioleDB docs/patch status](https://www.orioledb.com/docs),
[orioledb/postgres patch fork](https://github.com/orioledb/postgres),
[Supabase: next steps for pluggable storage](https://supabase.com/blog/postgres-pluggable-strorage),
[thebuild.com field guide (2026-05-08)](https://thebuild.com/blog/2026/05/08/a-field-guide-to-alternative-storage-engines-for-postgresql/).

**zheap.** EnterpriseDB's in-house undo-log heap replacement, predating/motivating the TAM API
itself, aimed at fixing Postgres's oldest pain point (VACUUM/bloat) via in-place updates. The
EDB GitHub repo went essentially static after 2019; CyberTec later picked it up with third-party
funding but work has been sporadic. The unresolved blockers are concrete and expensive: extra buffer
locking needed before `zheap_lock_tuple` because the standard `heap_lock_tuple` contract doesn't fit
zheap's undo model without an API change or shim; recovery/rollback paths needed substantially more
testing than budgeted. An independent 2026 survey: "the undo-log approach to MVCC is a long,
expensive engineering project" where the distance from compiling to production-ready "is measured in
years of integration work." Governance mattered as much as engineering — zheap was never a
community-owned commitfest effort; it was single-vendor R&D, and stalled when that vendor's
attention moved.

*Sources*: [EnterpriseDB/zheap GitHub](https://github.com/EnterpriseDB/zheap),
[CYBERTEC: zheap current status](https://www.cybertec-postgresql.com/en/postgresql-zheap-current-status/),
[Postgres wiki: Zheap](https://wiki.postgresql.org/wiki/Zheap),
[thebuild.com field guide (2026-05-08)](https://thebuild.com/blog/2026/05/08/a-field-guide-to-alternative-storage-engines-for-postgresql/).

**Citus Columnar.** The clearest "it actually worked" TAM case: shipped in open-source Citus 10
(March 2021) as `CREATE TABLE ... USING columnar`, needing zero patches to Postgres core, and
remains alive today because a funded team (Microsoft, via managed Azure Citus) treats it as a
maintained product surface. The catch: it has stayed **append-only by design for its entire
life** — no UPDATE/DELETE, no space reclamation on rollback, only hash/btree indexes, no tuple
locking — still true as of the Citus 12 docs, tracked as an open feature request rather than a bug.
A sibling case reinforces the lesson from the opposite direction: TimescaleDB's Hypercore TAM (a
btree-based hybrid row/columnar engine) was deprecated in 2.21 and sunset in 2.22 (Sept 2025) — not
because the TAM API failed, but because Timescale's own release notes say the architecture "was not
the right architecture" and their simpler non-TAM columnstore reached parity at lower cost. Even a
technically-working TAM plugin gets killed for product-strategy reasons unrelated to the API.

*Sources*: [Citus 10 release announcement](https://www.citusdata.com/blog/2021/03/05/citus-10-release-open-source-rebalancer-and-columnar-for-postgres/),
[Citus 10 columnar compression](https://www.citusdata.com/blog/2021/03/06/citus-10-columnar-compression-for-postgres/),
[citusdata/citus#4694](https://github.com/citusdata/citus/issues/4694),
[Citus 12.0 table management docs](https://docs.citusdata.com/en/v12.0/admin_guide/table_management.html),
[TimescaleDB 2.22.0 release notes](https://github.com/timescale/timescaledb/releases/tag/2.22.0),
[timescaledb PR #8196](https://github.com/timescale/timescaledb/pull/8196).

**ZedStore.** Began 2019 immediately after TAM committed, led by long-time Postgres committer Heikki
Linnakangas with VMware/Pivotal/Greenplum backing, explicitly aiming for eventual core inclusion. B-
trees as the on-disk structure gave it credible flexibility (column store or compressed row store).
Community discussion around the 2019-2020 patch threads shows real difficulty reaching mergeable
consensus — to the point proponents floated landing it as permanent "Beta"/"Experimental" just to
get it in-tree at all, itself a sign the patch couldn't clear the bar for a normal merge. No visible
development after 2020; an independent 2026 survey classifies it flatly "a dead end, archaeological
only." A core-committer champion and a well-funded sponsor got it *attention*, not *acceptance*.

*Sources*: [VMware Open Source blog](https://blogs.vmware.com/opensource/2020/07/14/zedstore-compressed-columnar-storage-for-postgres/),
[pgsql-hackers thread](https://www.postgresql.org/message-id/CALfoeiuF-m5jg51mJUPm5GN8u396o5sA2AF5N97vTRAEDYac7w@mail.gmail.com),
[postgrespro.com mirror](https://postgrespro.com/list/thread-id/2436236),
[thebuild.com field guide (2026-05-08)](https://thebuild.com/blog/2026/05/08/a-field-guide-to-alternative-storage-engines-for-postgresql/).

**ScyllaDB.** A complete C++/Seastar rewrite (2014-2015) of Cassandra's storage engine, keeping the
wire protocol/format, promising the JVM/GC bottleneck simply can't be tuned away. Genuinely
successful over 10+ years, but the "format-compatible" claim has an asterisk: Scylla extended the
SSTable format with its own `HAS_SHADOWABLE_DELETION_SCYLLA` flag to fix a tombstone-semantics
correctness issue — Cassandra cannot read Scylla-written SSTables carrying that flag, so
compatibility is one-way (Cassandra→Scylla) for any schema with materialized views. The documented
migration process is real migration work (manual schema recreation, Scylla-branded sstableloader,
dual-write verification), not bare format transparency. Ongoing maintenance cost is real and
continuous: 11 years of independently re-implementing every Cassandra format/protocol/compaction/CQL
evolution, visible as a standing stream of parity-gap issues in Scylla's own tracker. Performance won
adoption; compatibility claims managed transition risk, not zero-cost migration.

*Sources*: [ScyllaDB story](https://www.scylladb.com/company/the-scylla-story/),
[ScyllaDB SSTable format docs](https://docs.scylladb.com/manual/stable/architecture/sstable/),
[ScyllaDB Cassandra migration process](https://docs.scylladb.com/manual/stable/operating-scylla/procedures/cassandra-to-scylla-migration-process.html),
[ScyllaDB issue #1969](https://github.com/scylladb/scylladb/issues/1969),
[ScyllaDB issue #20531](https://github.com/scylladb/scylladb/issues/20531).
*(Note: the "11 years, ecosystem doubling" maintenance-cost framing is estimated from issue backlog
and format-divergence instances, not quantified engineering labor hours in public records.)*

**YugabyteDB.** A new engine (DocDB, a custom LSM on RocksDB) exposing Cassandra-like YCQL as one of
two query layers (YSQL/PostgreSQL is the other, and the growth vector). Architectural non-negotiables
break lift-and-shift: Cassandra is AP/eventually-consistent; Yugabyte is CP, all writes committed at
Raft quorum, no tunable consistency knob. Existing Cassandra application code written for
eventual-consistency/LWW patterns doesn't automatically get better on Yugabyte — it must be
rewritten to exploit transactions, or sees no benefit (and may regress). No bare-SSTable import path
exists; the documented migration guide states outright that application data-access logic must be
rewritten. Market reality: most deployments are greenfield YSQL, not Cassandra migrations — YCQL is
maintained but deprioritized (the engineering blog runs 3-4 PostgreSQL posts per 1 YCQL post).
API-level compatibility without storage-semantic compatibility is a false economy: it looks like a
migration path but functions as a values pitch for a different architecture.

*Sources*: [YugabyteDB architecture](https://www.yugabyte.com/blog/yugabyte-db-architecture-diverse-workloads-with-operational-simplicity/),
[YCQL vs Cassandra FAQ](https://docs.yugabyte.com/stable/faq/compatibility/),
[why built by reusing Postgres query layer](https://www.yugabyte.com/blog/why-we-built-yugabytedb-by-reusing-the-postgresql-query-layer/),
[Cassandra migration guide](https://www.yugabyte.com/blog/how-to-migrate-data-from-cassandra-or-mysql-to-yugabyte-db/),
[v2025.1 release](https://docs.yugabyte.com/stable/releases/ybdb-releases/v2025.2/).

### Synthesized lessons

1. **A sole-sponsor, sole-implementation SPI cannot get upstream sign-off — but a seam that is
   already merged and multiply-implemented is safe to build on.** *(Rocksandra vs CEP-11 memtable,
   MyRocks's stable handlerton, Citus Columnar's zero-core-patch TAM use.)* CQLite's dependence on
   CEP-11 memtable and compaction-strategy is on the safe side of this line by construction; a
   hypothetical `SSTableFormat.Factory` dependency inherits the same safety only because Big/BTI
   already prove it multiply-implemented (§3).
2. **Scope down to the seam that's actually pluggable, not the one you wish were pluggable — and
   the smaller the substantive surface, the more durable the bet.** *(CEP-11 vs 13475; Citus
   Columnar's append-only scope vs Hypercore's fuller ambition; Aria's uncontested niche vs
   Falcon's head-on InnoDB competition.)* CQLite's read-mostly, never-touch-write-path posture
   is the same shape of honest narrow scope.
3. **A funded, ongoing owner treating the plugin as a product is necessary for multi-year
   survival — a one-time contribution or single-company R&D project stalls when funding/attention
   moves.** *(TokuDB, PBXT, zheap, Rocksandra all died this way; MyRocks and Citus Columnar
   survived because Meta/Microsoft kept investing.)* This directly bears on §4/§5: whichever posture
   is chosen needs a named, resourced owner past initial ship.
4. **Feature/semantic coupling breaks a seam that isn't architecturally isolated — new host
   features (transactions, sessions, consistency semantics) tend to reach into exactly the layer
   the plugin depends on.** *(MongoDB's txn-driven collapse to WiredTiger-only; the MVCC-model
   mismatch that killed MongoRocks specifically.)* Cassandra's read-merge/`Tracker`/streaming/repair
   being explicitly out of CQLite's seam set (§1) is the direct mitigation — CQLite depends on
   nothing the host is likely to redraw around a new OLTP feature.
5. **"The seam is pluggable" is necessary, not sufficient — some seams need a standing stream of
   host-side co-patches even after they're declared pluggable.** *(OrioleDB's multi-year,
   still-ongoing TAM-gap patch queue.)* This is the central open question for §3: is
   `SSTableFormat.Factory` closer to Citus Columnar (clean, zero-core-patch fit) or OrioleDB
   (real seam, but needs ongoing upstream co-evolution)?
6. **Format/API compatibility claims have an asterisk under pressure — verify what "compatible"
   actually promises before relying on it as a moat.** *(Scylla's one-way shadowable-deletion
   divergence; Yugabyte's API-only compatibility that doesn't survive contact with semantic
   differences.)** This is exactly why §1's format-compatibility moat claim is stated narrowly here
   (byte-parity, host-writable-and-readable SSTables) rather than as a general "Cassandra-compatible"
   claim.
7. **Vendor/owner consolidation is itself a survival strategy, independent of technical merit** —
   *contradicts lesson 3's "funded owner" framing in one respect*: InnoDB won partly by
   *eliminating* alternatives (Oracle owning both sides), not by out-competing them. This case does
   not map cleanly onto Cassandra's ASF governance (no single vendor owns Cassandra the way Oracle
   owns MySQL), but it's a reminder that some engine-swap "successes" are politics, not proof the
   seam-assembly approach itself works.
8. **Committer/vendor backing gets a proposal attention, not acceptance — production proof usually
   has to precede, not follow, the ask for durable upstream trust.** *(ZedStore's core-committer +
   VMware backing still failed to merge; MyRocks/Citus Columnar's production-first, ask-later
   sequencing succeeded.)* Relevant to §5 option C's CEP-ambition path: don't lead with a CEP pitch
   before CQLite has its own production number.

Where cases contradict each other (noted honestly): lesson 3 (funded ownership wins) and lesson 7
(consolidation via elimination wins) point to different mechanisms of success that don't both apply
to Cassandra's governance model — ASF's multi-stakeholder structure resembles MariaDB's Spider/Aria
pattern (in-house, creator-controlled, narrow niche) far more than Oracle's InnoDB consolidation.
Treat lesson 7 as background context, not a template.

---

## 3. The SSTableFormat seam, verified

**Checked-out ref**: `50ddce8455` (`Merge branch 'cassandra-6.0' into trunk`, on branch `trunk`,
`base.version=7.0`). **This is NOT Cassandra 5.0** — CQLite's stated target (CLAUDE.md) is
`cassandra-5.0.0`+. Every load-bearing anchor was cross-checked against the **`cassandra-5.0.8`**
tag via `git show <tag>:<path>` (no working-tree mutation) — the SPI is **present and structurally
identical** on 5.0.8, just at different line numbers (noted inline). Treat trunk anchors as
current-state, 5.0.8 anchors as the version CQLite actually targets.

### 3.1 The full contract a third format must implement

Registration is a real Java `ServiceLoader` SPI, not a hardcoded enum — confirmed by an actual file
on disk: `src/resources/META-INF/services/org.apache.cassandra.io.sstable.format.SSTableFormat$Factory`
lists only `big.BigFormat$BigFormatFactory` and `bti.BtiFormat$BtiFormatFactory` today, but a
third-party jar shipping its own file of that name is auto-discovered by
`DatabaseDescriptor.applySSTableFormats()` — `DatabaseDescriptor.java:1976`
(`ServiceLoader.load(SSTableFormat.Factory.class, ...)`), no Cassandra source edit required. This is
the documented, intentional CEP-17 seam: `src/java/org/apache/cassandra/io/sstable/SSTable_API.md`
(full authoring guide, present in-tree) walks through implementing it.

Contract surface, by layer (file:line, trunk; 5.0.8 line noted in parens where it drifted):

| Layer | Abstract members a minimal format must supply |
|---|---|
| `SSTableFormat<R,W>` interface — `format/SSTableFormat.java:45-105` | `name()`, `getLatestVersion()`, `getVersion(String)`, `getWriterFactory()`, `getReaderFactory()`, `allComponents()`, `primaryComponents()`, `batchComponents()`, `uploadComponents()`, `mutableComponents()`, `generatedOnLoadComponents()`, `getKeyCacheValueSerializer()`, `getScrubber(...)`, `getFormatSpecificMetricsProviders()`, `deleteOrphanedComponents(...)`, `delete(...)` — 16 methods. `AbstractSSTableFormat` (`format/AbstractSSTableFormat.java:20-61`) finalizes `name()/equals/hashCode/toString`, so ~4 come for free. |
| `SSTableFormat.Factory` — `:196-212` | `name()`, `getInstance(Map<String,String>)` — 2. |
| `SSTableReaderFactory<R,B>` — `:107-133` | `builder(Descriptor)`, `loadingBuilder(...)`, `readKeyRange(...)`, `getReaderClass()` — 4. |
| `SSTableWriterFactory<W,B>` — `:135-150` | `builder(Descriptor)`, `estimateSize(...)` — 2. |
| `SSTableReader` abstract class — `format/SSTableReader.java:160-2149` (5.0.8: same class, `SSTableReader.java`) | `cloneWithRestoredStart`, `cloneWithNewStart`, `releaseInMemoryComponents`, `estimatedKeys`, `estimatedKeysForRanges`, `isEstimationInformative`, `getKeySamples`, `getRowIndexEntry` (l.921, the template-method hub — `getPosition`/`getPositionsForBounds`/`getPositionsForRanges`/`getPositionsForFullRange` all funnel through this one abstract call, l.754-830), `keyReader` (×2 overloads), `keyIterator`, `firstKeyBeyond`, `keyAtPositionFromSecondaryIndex`, `mayContainAssumingKeyIsInRange`, `getVerifier`, `Builder.buildInternal` — ~16. Base class also carries a mandatory `protected final FileHandle dfile` field (l.276) used directly by non-overridable `getScanner()`/`getFileDataInput()` — a format cannot opt out of a real `FileHandle` over Data.db. |
| `UnfilteredSource` (implemented by `SSTableReader`, body supplied only in the concrete Big/BTI reader) — `db/rows/UnfilteredSource.java:31-69` | `rowIterator(DecoratedKey, Slices, ColumnFilter, boolean, SSTableReadsListener)` and `partitionIterator(ColumnFilter, DataRange, SSTableReadsListener)` — 2, but the highest-risk 2 (see below): must return live `UnfilteredRowIterator`/`UnfilteredPartitionIterator` Java object graphs, not bytes. Confirmed only concretely implemented in `BigTableReader.java:130,151` and `BtiTableReader.java:370,476` — never in the shared abstract layer. |
| `SSTableWriter` abstract class — `format/SSTableWriter.java:73-613` | `txnProxy()`, `mark()`, `append(UnfilteredRowIterator)` (l.196 — again, object-graph input, not bytes), `getFilePointer()`, `getOnDiskFilePointer()`, `resetAndTruncate()`, `openEarly(Consumer<SSTableReader>)`, `openFinalEarly()`, `openFinal(OpenReason)`, `Builder.getMmappedRegionsCache()`, `Builder.buildInternal(...)` — 11. If built on `SortedTableWriter` (`format/SortedTableWriter.java`, 553 lines, shared template-method base for both Big/BTI) this collapses to `createRowIndexEntry(...)` (l.312) + 3 factory opens (`openDataWriter`/`openIndexWriter`/`openPartitionWriter`, l.547-551) — a much smaller real surface. |
| `SSTableReaderLoadingBuilder<R,B>` — `format/SSTableReaderLoadingBuilder.java:51-156` | `buildKeyReader(TableMetrics)`, `openComponents(B, Owner, boolean, boolean)` — 2. This class also wires the shared `ChunkCache` (l.30,59,67) — every format's loading builder receives `ChunkCache.instance` whether it wants it or not. `SortedTableReaderLoadingBuilder` (extends it, `format/SortedTableReaderLoadingBuilder.java:32-69`) further pre-wires bloom-filter loading (`loadFilter`) and a `FileHandle.Builder` with mmap/chunk-cache options — optional to extend, but the only in-tree examples do. |
| `Version` abstract class — `format/Version.java:34-118` | `isLatestVersion`, `correspondingMessagingVersion`, ~15 `hasXxx()` capability booleans (`hasCommitLogLowerBound`, `hasMaxCompressedLength`, `hasOldBfFormat`, `hasImprovedMinMax`, `hasKeyRange`, ...), `isCompatible()`, `isCompatibleForStreaming()` — ~18, but each is a one-line constant derived from the parsed version-letter — trivial, not substantive work. |
| `KeyCacheValueSerializer<R,T>` — `SSTableFormat.java:187-194` | `skip`, `deserialize`, `serialize` — 3. Only mandatory if the reader opts into `KeyCacheSupport` (optional interface per `SSTable_API.md` §Key cache). |
| `IScrubber` / `IVerifier` — `io/sstable/IScrubber.java`, `IVerifier.java` | `scrub()/close()/getScrubInfo()/scrubWithResult()` and `verify()/close()/getVerifyInfo()` — ~4 each, with `SortedTableScrubber`/`SortedTableVerifier` providing partial shared logic. |

**Rough total**: ~90-100 named abstract/overridable members across the whole hierarchy, but the
*substantive* subset (real per-format logic, not a one-line constant or thin delegation) is closer
to **15-20**: `getRowIndexEntry`, `keyReader`/`keyIterator`, `firstKeyBeyond`, `estimatedKeys`,
`mayContainAssumingKeyIsInRange`, `rowIterator`/`partitionIterator`, `append`/`createRowIndexEntry`,
`openEarly`/`openFinal`, `getVerifier`/scrubber logic, `readKeyRange`. This matches the "two formats
ship, both built on `SortedTable*` shared base classes" reality in-tree — BTI's own reader/writer
files are proof the surface is small enough to have been built twice already inside the project.

### 3.2 Global vs per-table selection; mixed generations; upgradesstables

- **Write selection is GLOBAL, single-valued**: `Config.SSTableConfig.selected_format` (default
  `BigFormat.NAME`) — `config/Config.java:455-459` (5.0.8: `Config.java:373`). No per-table
  equivalent exists for SSTable format (unlike **memtable**, which *is* per-table via
  `CREATE/ALTER TABLE ... WITH memtable = '...'`, `Memtable_API.md:76-91`).
- **Reads coexist across formats/generations by design.** `Descriptor.getFormat()` returns
  `version.format` (`io/sstable/Descriptor.java:212-214`), resolved per physical file from the
  filename token, looked up against the full registered map
  `DatabaseDescriptor.getSSTableFormats()` (`Descriptor.java:306-309`), not the single
  selected-for-writing format. `Descriptor.java:437-438` explicitly rejects an incompatible/unknown
  version string with "you should have run upgradesstables before upgrading" — proving
  mixed-generation coexistence (different versions of the same format, and different formats too)
  is the expected, exercised steady state.
- **`upgradesstables` / `Upgrader`** (`db/compaction/Upgrader.java:72-90`) always builds its output
  writer via `cfs.newSSTableDescriptor(directory)`, resolving to
  `DatabaseDescriptor.getSelectedSSTableFormat().getLatestVersion()`
  (`db/ColumnFamilyStore.java:1017-1019`) — the tool that rewrites any old-format/old-version
  sstable into whatever format is currently globally selected, going through the same
  `CompactionController`/`SSTableRewriter`/format-writer-factory path as normal compaction.

### 3.3 Streaming, repair, backup/snapshot, key-cache/index-summary assumptions

Two structurally distinct paths:

- **Per-partition (mediated) streaming and repair** go through the `UnfilteredSource` contract
  (`rowIterator`/`partitionIterator`), format-agnostic at the API level — but this is exactly where
  the "native reader" question gets hard (below).
- **Entire-sstable zero-copy streaming**
  (`db/streaming/CassandraEntireSSTableStreamWriter.java`, `CassandraEntireSSTableStreamReader.java`)
  is genuinely format-agnostic at the byte level: it enumerates `manifest.components()` and copies
  component files verbatim over the wire; the receiver reconstructs a `Descriptor` from
  `header.version` and opens a `SSTableTxnZeroCopyWriter` via
  `desc.getFormat().getWriterFactory()...` (`CassandraEntireSSTableStreamReader.java:183-200`).
  Gated off only for legacy bloom-filter format / disabled config
  (`CassandraOutgoingFile.java:189-196`) — no explicit peer-format-compatibility check is visible;
  it implicitly assumes the receiving node has the same `SSTableFormat` registered, an operational
  requirement (every node needs the plugin jar), same as BTI itself already requires today in a
  mixed-format cluster.
- **Backup/snapshot** (`SSTableReader.createLinks`, `SSTableReader.java:1154-1206`) hardlinks
  physical component files by `descriptor.fileFor(component)` — pure filesystem operation, no
  format-specific logic needed.
- **Key cache / index summary are explicitly OPTIONAL, opt-in per format**, not baked into the base
  contract: `SSTable_API.md` says a reader implements `IndexSummarySupport`/`KeyCacheSupport` only
  if it uses that mechanism — confirmed structurally: `IndexSummarySupport` usage greps only to the
  `big/` package; BTI implements neither (it uses its own trie `PartitionIndex`/`RowIndexReader`
  instead).
- **Where JVM internals genuinely leak into the contract**: the abstract `SSTableReader` base class
  hard-carries `protected final FileHandle dfile` (`SSTableReader.java:276`), and `FileHandle`
  (`io/util/FileHandle.java:50-500`) is a concrete Cassandra class wrapping a real `ChannelProxy`,
  `MmappedRegionsCache`/`MappedByteBuffer` regions, and `ChunkCache.Buffer`
  (`cache/ChunkCache.java:98`, implementing `Rebufferer.BufferHolder`) — an off-heap page-cache
  abstraction. Non-overridable base methods (`getScanner()`, `getFileDataInput()`, disk-access-mode
  dispatch) call into `dfile` directly. A format cannot substitute a synthetic/fake `FileHandle`
  without either (a) pointing it at a real on-disk file (workable — CQLite already writes real
  files) or (b) reimplementing large parts of `FileHandle`/mmap/chunk-cache machinery.
- **The harder wall is the terminal type of the read path, not the file layer.**
  `rowIterator`/`partitionIterator` must return live `UnfilteredRowIterator`/`UnfilteredPartitionIterator`
  object graphs — Cassandra's in-heap `Row`/`Unfiltered`/`RangeTombstoneMarker`/`Cell`/`DeletionTime`/`ClusteringPrefix`
  object model, consumed downstream by `CompactionIterator`, the query engine's merge-iterators, and
  repair's Merkle-tree hashing. There is no "bytes in, opaque handle out" seam here: a native reader
  must materialize genuine Java objects satisfying these interfaces, cell by cell — either by
  decoding Big/BTI-format bytes in Java (in which case the native engine isn't actually doing the
  reading), or via an FFI/JNI shim constructing each `Row`/`Cell` from Rust-decoded values (workable,
  but pays a per-cell JNI-boundary-crossing tax that undercuts the whole point of delegating to
  native code for decode speed).

### 3.4 Compaction ↔ format interaction

Confirmed format-agnostic at the compaction-strategy level:
- `CompactionManager.java:1829,1870` and `Upgrader.java:78` all obtain the output writer via
  `descriptor.getFormat().getWriterFactory().builder(descriptor)`.
- `CompactionAwareWriter` (`db/compaction/writers/CompactionAwareWriter.java`) — the shared base for
  every compaction-strategy writer (STCS/LCS/splitter/etc.) — holds `ILifecycleTransaction txn`
  (l.66), builds the new `Descriptor` via `cfs.newSSTableDescriptor(...)` (l.238, resolving to the
  globally selected format's latest version, per §3.2), and calls
  `newWriterBuilder(Descriptor).build(txn, cfs)` (l.324-326) — purely through the format's
  `SSTableWriterFactory`. Row data flows through `append(UnfilteredRowIterator partition)`
  (l.147-168) — the same object-graph contract as normal writes.
- **Compaction output CAN be produced by a format-API-conformant writer of any registered
  format** — the mechanism doesn't care whether that writer's internals are pure Java or
  FFI-backed, as long as it correctly implements `append`, `openEarly`/`openFinal`, and the txn
  lifecycle (`Transactional` via `txnProxy()`).
- **A real, in-tree landmine a third format must anticipate**: `CursorCompactor.java:142-146` (a
  newer cursor-based fast-path compactor on this trunk) explicitly gates
  `if (!(DatabaseDescriptor.getSelectedSSTableFormat() instanceof BigFormat)) return false;` with
  the comment `// BTI index writing is not supported yet` — even the in-tree BTI format is silently
  excluded from this optimization and falls back to the standard iterator-based `CompactionTask`
  pipeline. A third format should expect similar `instanceof BigFormat`/`instanceof
  <ConcreteFormat>` fast-path gates scattered around newer optimizations, with graceful (if slower)
  fallback rather than a hard failure — a perf-parity gap, not a correctness one.

### 3.5 Classification and top risks

| Contract area | Classification | Why |
|---|---|---|
| `SSTableFormat`/`Factory` registration, `Components`, `Version` booleans | **thin-shim** | ServiceLoader-based, no core-source edit; `Version` flags are one-line constants; `AbstractSSTableFormat` finalizes the boilerplate. |
| Writer path (`SSTableWriterFactory`, `SortedTableWriter.createRowIndexEntry`, `openDataWriter`/`openIndexWriter`) | **thin-shim to substantial-JVM-work**, depending on target bytes | If CQLite's writer emits genuine byte-for-byte Big/BTI-compatible files (already claimed for compaction — v0.12 changelog), no new format is even needed; stock `BigFormat`/`BtiFormat` readers/writers work unmodified. If a *new* on-disk layout is desired, the JVM side must still implement `createRowIndexEntry`/index-writer glue in Java (or via a JNI writer called per-partition from `SortedTableWriter`'s template hooks) — substantial but bounded JVM work. |
| Reader index/position path (`getRowIndexEntry`, `keyReader`, `keyIterator`, `firstKeyBeyond`, `FileHandle`/`ChunkCache` wiring) | **substantial-JVM-work** | Must wire a real `FileHandle` over a real physical file (workable) but still must implement/adapt Cassandra's `AbstractRowIndexEntry` serialization and chunk-cache-aware buffer access in Java (or via a thin per-call JNI bridge) — non-trivial glue, not conceptually blocked. |
| **Row/partition materialization (`rowIterator`/`partitionIterator` → `UnfilteredRowIterator`) and writer `append(UnfilteredRowIterator)`** | **architecturally hostile** | The contract's terminal type on both read and write is a live Java heap object graph, not bytes. No bytes-in/bytes-out FFI seam exists here — every cell must cross the JNI boundary to become a real Java object (or be decoded in Java, defeating the delegation). This is the #1 risk. |
| Streaming/repair/backup | **thin-shim** | Entire-sstable path is byte-level component copy (format-agnostic); mediated streaming/repair ride the same `UnfilteredSource` contract as normal reads (inherits the row-materialization risk above, not a new one). |
| Scrubber/verifier/key-cache/index-summary | **thin-shim** | All explicitly optional (`SSTable_API.md`); BTI itself implements neither `IndexSummarySupport` nor `KeyCacheSupport`, proving a format can validly skip both. |

**Top 3 risks for a CQLite-delegating format:**

1. **Row/cell object-graph materialization at the FFI boundary** (`rowIterator`/`partitionIterator`/`append`) —
   per-cell JNI round-trips likely erase most of the native-decode speed advantage; this is the one
   part of the contract that cannot be satisfied with a "hand back a buffer" shim.
2. **`instanceof <ConcreteFormat>` fast-path landmines** (confirmed live example:
   `CursorCompactor.java:142`) — newer optimizations get added format-specific before being
   generalized; a third format silently falls back to slower paths rather than failing, but this
   needs active release-to-release monitoring, not an assumption it's handled by the clean
   interface.
3. **Operational fleet consistency**: format selection is a JVM-startup-time `ServiceLoader` +
   global `cassandra.yaml` `sstable.selected_format` value — every node in the cluster needs the
   plugin jar on its classpath to *read* files of that format (entire-sstable streaming and
   mixed-generation directory scans both require it), and there is no schema-propagated per-table
   safety net the way memtable has (`Memtable_API.md:93-97`, which explicitly documents
   fallback-to-default when a node lacks a memtable class).

**Friendlier or harsher than the CEP-11 memtable seam? Friendlier, on balance.** SSTableFormat is a
first-class, ServiceLoader-discovered SPI with an in-tree authoring guide, explicitly modeled as
CEP-17's deliverable — no reflection-based ad hoc class-name wiring was found (unlike memtable's
factory-via-reflection approach, `Memtable_API.md:159-172`). Its contract is more
mechanical/template-method-shaped (`SortedTableWriter`/`SortedTableReaderLoadingBuilder`/
`SortedTableScrubber`/`SortedTableVerifier` all provide substantial shared logic with a handful of
abstract hooks), and two in-tree formats prove the minimal-format surface is genuinely small. **But**
the memtable interface's terminal read type is the *same* `rowIterator`/`partitionIterator`
object-graph contract (`Memtable_API.md:127`; `UnfilteredSource` is explicitly "common data access
interface for sstables **and** memtables," `UnfilteredSource.java:29`) — so the single hardest risk
(row/cell materialization at an FFI boundary) is **identical** across both seams; no amount of
SSTableFormat-side friendliness resolves it. Memtable is **per-table** selectable (finer operational
blast radius, gradual rollout, `Memtable_API.md:99-122`) vs. SSTableFormat's **single global write
selector** — a real friendliness gap in the *other* direction (harsher for SSTableFormat) for staged
production rollout, though reads already tolerate mixed formats/generations per §3.2.

*Marked-inferred, not re-verified this session*: the CEP-11 "package-private constructor" and
"`instanceof TrieMemtable` gate forces `extends`" claims, carried from prior research memory. No
`instanceof TrieMemtable`/`SkipListMemtable`/`ShardedSkipListMemtable` hits were found outside
`db/memtable/` on this trunk checkout — either fixed/refactored since, in a narrower spot the grep
missed, or version-specific. Recommend re-verifying directly if this memtable comparison becomes
load-bearing for a decision.

---

## 4. Composition map

**Scope**: how #1807 (memtable plugin), #905 (compaction manager), #2037 (Arrow/OLAP path), #941
(DataFusion), and #1934 (two-project split) compose into one "assembled storage engine" product.

### The five pieces, in the roles they already occupy

| Piece | Role in the assembly | Status / anchor |
|---|---|---|
| **#1807** | CEP-11 memtable plugin spike — `CqliteMemtable extends TrieMemtable`, on-demand `nb` tail export, no CDC. The write-freshness seam. | OPEN spike; design at `docs/storage engine/memtable-plugin-design.md` |
| **#905** | Compaction-strategy seam. Phase B = pure UCS planner/simulator (unblocked); Phase A′ = productize the offline one-shot compactor (gated on #1537); Phase C = manager daemon (deliberately unfiled — "waits for #1934/WS5 to name the engine product"); Phase D = schema-pull (unfiled). | Epic body + `docs/architecture/issue-905-compaction-manager-research.md` §7 |
| **#2037** | The full OLAP path: #1807's plugin + a node-local CQLite scan engine (k-way LWW merge, Arrow post-merge) + a coordinator OLAP verb + a disposable per-generation Arrow/Parquet cache. Builds on #1807 ("Lineage" line) — it is #1807 *extended*, not a separate plugin. | `docs/architecture/issue-2037-arrow-olap-research.md` §1, §6, §10 |
| **#941** | Coordinator-side query surface: Design A = co-located Flight-backed DataFusion `TableProvider` over Sidecar snapshot manifests; Trino stays MPP scheduler; Design C = materialized-epoch/Iceberg provider (future epic #1914). | Epic body + council doc (commit `d734c44b`) |
| **#1934** | Phase-1 curated engine API (WS1) + query-engine-home decision (WS2) + shared-foundation strategy (WS3) + evict surface code from `storage/` (WS4) + product identity & naming (WS5) + connector graduation (WS6) + ops split cost model (WS7). | Epic body |

The umbrella architecture thesis underneath all five is `report-2-storage-engine-feasibility.md`'s
Verdict: **"Adjacent: yes. Replacement: no."** — CQLite exploits the seams that exist (memtable
CEP-11, `SSTableFormat.Factory`, compaction strategy) and never touches the ones that don't
(read-path merge, `Tracker`/`View`, streaming, repair) (report-2 §1.2 seam inventory, §2.3, §3.4).
Every one of #1807/#905/#2037/#941 lives entirely inside that "adjacent" envelope.

### What composes cleanly

- **#1807 → #2037 is a straight-line extension, not a second plugin.** #2037's architecture diagram
  is #1807's tail-export mechanism with an Arrow read-out bolted on ("Row format until the data
  stops mutating; Arrow from the first immutable moment" — #2037 §1). Ordering constraint: #2037's
  coordinator OLAP verb (its own "WS2" numbering — see the collision note below) can't be fully
  designed until #1807 answers whether a range-bounded `makePartitionIterator` works under the same
  `readOrdering` pin as `getFlushSet` (#2037 §12.7, flagged not settled).
- **#905's tail-dir reuse.** "The tail dir `CqliteTailExporter` fills is exactly what #905's manager
  maintains — same component, second data source" (`issue-905-compaction-manager-research.md` §7).
  #905 Phase C (daemon) and #2037's OLAP path are two independent *consumers* of the same #1807
  artifact, not competing implementations.
- **#941 and #2037 stack, they don't overlap.** Report-2 §3.1: "Flight/Trino is the hot per-node
  read plane for (i) and (ii): same producer/filter/aggregation pipeline... any added row source
  inherits pushdown for free." #941 Design A ships today against the snapshot-only Flight producer;
  #2037's freshness upgrade would transparently deepen the same producer later via the
  already-sketched `max_staleness_ms` `FlightTicket` hint (report-2 §3.1, Tier 1 item 2). No rework
  implied on #941's side if #2037 lands after it.
- **#905's UCS planner as a #1934 exhibit.** "The pure `plan()` trait is a textbook exhibit for the
  Phase-1 curated engine API (no entangled callers to migrate, unlike the query engine)"
  (`issue-905-compaction-manager-research.md` §7) — #905 Phase B is *evidence* for #1934 WS1, not
  just a consumer of it.

### What is redundant or in tension

1. **Design C (#1914) vs #905's major-compaction snapshot.** #905's own research flags this
   directly: "Design C (materialized-epoch provider, #1914) ≈ a #905 major-compaction snapshot —
   reconcile the two designs before #1914 activates, to avoid building 'merge once, serve many'
   twice" (`issue-905-compaction-manager-research.md` §7). A live, unresolved risk of building the
   same capability twice under two different epics.
2. **CDC-tail (report-2 posture ii) vs #2037's no-CDC ArrowMemtable.** Report-2 (2026-07-03)
   recommended CDC-tail as "the Q1 answer that needs zero in-JVM Cassandra change" (report-2 §3,
   Posture ii, Tier 1 item 1). The owner then set **NO CDC** the same day, and #2037 (2026-07-05)
   designed the ArrowMemtable + range-sliced-tail alternative as the replacement freshness path.
   Sequential supersession, not simultaneous redundancy — but report-2's Tier 1 item 1 is now stale
   relative to the frame and nothing formally retires it. #2037's own §12.7 addendum reopens the
   question and concludes "the no-CDC decision survives these numbers" but flags a real gap at
   full-scan scale pending WS7 benching (§12.7, final paragraph).
3. **Three independent "flagship demo for the new engine name" candidates.** #905 Phase B's UCS
   simulator is explicitly called out as "a plausible flagship demo for the new engine-tier product
   name (WS5)" (`issue-905-compaction-manager-research.md` §7). #941's #1912 ("E2E validation —
   DataFusion vs Trino over the same epoch, capstone") occupies the same rhetorical slot. #2037's
   full OLAP-path demo is the same slot again. None of the three epics acknowledges the other two
   are making the identical claim — a genuine product-frame collision, not a technical one.
4. **Workstream-ID collision.** #1934's WS2 = "query-engine home decision" (engine-side vs
   surface-side). #2037's WS2 = "tail live-stream protocol." The two docs cross-reference each other
   while reusing the same short ID for unrelated forks. Currently disambiguated only by
   qualification ("#1934/WS5") rather than a shared registry.

### What is missing

- **`SSTableFormat.Factory` is unexplored by any of the five.** Report-2 places it as the third
  genuinely-pluggable seam (alongside CEP-11 memtable and compaction strategy) but scopes any use to
  a Tier-3 research spike, "not to ship," and only if pure-Java/upstreamable (report-2 §1.2, §4 Tier
  3 item 6, §5 NEEDS-DECISION item 2). None of #1807/#905/#2037/#941/#1934 currently owns or plans
  this seam — §3 of this record is the first source-verified look at it.
- **Repaired-status ingestion (`repairedAt` from Statistics.db).** Report-2 §3.2/§5 item 3 flags
  this as the prerequisite for RF-safe cluster-level dedup (primary-token-range pruning + repaired
  gating), needed by both the Iceberg materializer and any coordinator-level fan-out spanning
  multiple nodes' replicas (#941's cluster view and #2037's coordinator OLAP verb both need it
  eventually). No epic among the five currently owns it.
- **Consistent gating on #1934 WS1.** #905 explicitly places itself "in-repo, behind the #1934
  Phase-1 curated API" (`issue-905-compaction-manager-research.md` §7, "Placement"). #941's #1908
  (`cqlite-datafusion` crate) is only "informed by" WS1 per #1934's own epic body — a softer,
  non-blocking relationship. The asymmetry between "gated on" (#905) and "informed by" (#941) for
  what should be two equally load-bearing external consumers of the same curated API is not
  explained anywhere.
- **A schedule for #1934 itself.** #1934 is Backlog with no owner-assigned timeline. Everything
  chained to WS5 — #905 Phase C, "do NOT mint a published `cqlite-compaction` crates.io name now"
  (`issue-905-compaction-manager-research.md` §7 "Placement"), and by extension any concrete brand
  for #2037's coordinator-facing verb — is indefinitely parked until the owner prioritizes #1934.
- **#1406 (uncompressed writes vs snapshot-hygiene claim) as a cross-cutting compression story.**
  Currently tracked only inside #905 ("Blocks Phase A′ *marketing*, not Phase B" —
  `issue-905-compaction-manager-research.md` §7), but an assembled-engine brand that includes a
  compaction daemon needs one coherent compression posture across write, compact, and OLAP-cache
  paths, not a per-epic caveat.

### Ordering constraints (consolidated)

```
#1934 WS1 (curated engine API)  ──informs──▶ #941 #1908 (cqlite-datafusion)
                                └──gates────▶ #905 Phase B placement (in-repo, unpublished)

#1934 WS5 (engine name)  ──gates──▶ #905 Phase C (daemon)
                          ──gates──▶ any published crate name for #905/#2037's coordinator verb

#1807 (tail export + readOrdering pin verified) ──gates──▶ #2037's coordinator OLAP verb (its own WS2)

#905 Phase A′ ──gates on──▶ #1537 (P0 fix)
#941 #1914 (Design C) ──gates on──▶ #905's major-compaction-snapshot reconciliation (unresolved)
                        ──gates on──▶ #1905–#1912 (Design A) shipping first (epic body: "do not activate until A ships")

#2037's whole architecture ──presupposes──▶ #1934's WS2 working recommendation
                                              ("SQL execution stays engine-side") already resolving as stated
```

The last line is worth stating explicitly: #2037's diagram and #941's framing of DataFusion as "a
leaf scan/table-provider surface, never a second distributed planner" (#941 epic body) both already
assume the answer to #1934 WS2 (query engine lives engine-side) that WS2 hasn't formally resolved
yet. If WS2 comes back differently, both #2037 and #941's shape need rework — a soft pre-commitment
across three epics to an unratified WS2 outcome.

**Anchors used**: `gh issue view 1934/905/941`;
`docs/architecture/issue-905-compaction-manager-research.md` (§7 "Product-direction alignment", §8
"Open decisions"); `docs/architecture/issue-2037-arrow-olap-research.md` (§1 Thesis, §10 workstream
table, §11.4 HTAP precedents, §12.7 S4A′ addendum);
`docs/storage engine/report-2-storage-engine-feasibility.md` (Verdict, §1.2 seam inventory, §2.3,
§3.4, §4, §5 NEEDS-DECISION).

---

## 5. Strategy options

Three postures, mapped to the post-mortem lessons in §2. No recommendation on naming/product — those
are owner calls (§6). Engineering-risk read given per option.

### Option A — Adjacency-only

**Shape**: report-2's verdict (b) as-is — memtable plugin (#1807/#2037) + compaction-strategy
plugin (#905) + node-local Rust scan/merge engine, serving Arrow via Flight/Trino/DataFusion. **No
new in-JVM surface beyond the memtable plugin.** `SSTableFormat.Factory` stays unexplored/unused.

**Mapped lessons**: leans hardest on lesson 2 (scope down to the seam that's actually pluggable) and
lesson 4 (avoid feature/semantic coupling — read-merge/`Tracker`/streaming/repair stay untouched).
Sidesteps lesson 5 entirely (no OrioleDB-style standing co-patch dependency, because no new seam is
being asked of the host).

**Engineering-risk read**: **lowest risk of the three.** No dependency on §3's unresolved
row/cell-materialization wall (the SSTableFormat seam's top risk) because this option never
implements `SSTableFormat`. Main residual risks are internal-to-CQLite: the three compaction-manager
epic tensions in §4 (Design C vs #905 major-compaction snapshot, CDC-tail supersession, WS2/WS5
collision) and the fast-path perf-parity gaps §3.4 shows the *existing* BTI format already
tolerates (a precedent CQLite's own future format-adjacent work would inherit if it ever changes
tack). This option carries the least exposure to §2's failure modes generally — there's no new SPI
being proposed to fail Rocksandra-style, and the "adjacent" envelope is exactly what CEP-11 memtable
itself proved survivable.

### Option B — Assembled engine (add SSTableFormat interop)

**Shape**: Option A's stack, plus wiring `SSTableFormat.Factory` for genuine format-level interop
(e.g., a CQLite-backed reader/writer registered as a third format) — **conditioned on §3's audit
actually classifying the needed slice as thin-enough.** Per §3.5: registration, `Version` booleans,
scrubber/verifier, and (if CQLite's writer stays byte-parity) the writer path are thin-shim to
moderate; the reader index/position path is substantial-JVM-work but not blocked; row/cell
materialization (`rowIterator`/`partitionIterator`/`append`) is **architecturally hostile** and is
the load-bearing unresolved question for this option.

**Mapped lessons**: this is the OrioleDB path most directly (lesson 5) — a real, ServiceLoader-based
seam that nonetheless may need per-release co-evolution or accepted perf-parity gaps
(`instanceof BigFormat` landmines, §3.4). It also risks the MongoDB feature-coupling failure mode
(lesson 4) if the row/cell object-graph wall in §3.3/§3.5 forces CQLite to reach further into
Cassandra internals than the "adjacent" framing intends — the FFI boundary tax on `rowIterator` is
structurally the same kind of problem that broke MongoRocks's optimistic/pessimistic mismatch:
adapting one side's semantics to fit the other's contract at a fundamental layer.

**Engineering-risk read**: **highest technical uncertainty of the three, gated on an unresolved
architectural question.** §3's own top risk (#1, row/cell materialization) is not a scoping problem
to manage — it's an open question of whether a JNI/FFI bridge can cross that boundary without
erasing the native-decode speed advantage the whole strategy is premised on. Until that's answered
(prototype or benchmark), this option's cost is unknowable, not merely large. If the writer side
stays byte-parity (already claimed for compaction) and only registration/scrubbing/version-metadata
are implemented — skipping the hostile row/cell path entirely — this shrinks back toward Option A's
risk profile; the risk is specifically proportional to how much of the reader/writer object-graph
contract gets touched.

### Option C — Assembled engine + upstream CEP ambition

**Shape**: Option B (or A), plus pursuing the missing/incomplete seams as genuine CEP proposals to
Apache Cassandra — echoing #2037's own WS9 ("CEP pitch, optional") and report-2's Tier-3.6 pure-Java
interop spike, explicitly separated from the fork/vendor-only Rust/FFI dual-write path that report-2
says is never upstreamable (report-2 §2.2/§2.3).

**Mapped lessons**: this is the OrioleDB/MyRocks synthesis most directly — lesson 8 (production
proof should precede the upstream ask, not follow it: ZedStore failed with committer+vendor backing
alone; MyRocks/Citus Columnar succeeded production-first) and lesson 1 (a sole-sponsor SPI proposal
doesn't get sign-off — Rocksandra's 13475 is the direct cautionary case for *any* CEP that proposes
a new abstraction validated by only one implementation). It also directly engages lesson 3 (funded,
ongoing ownership) since a CEP effort is a multi-year standing commitment, not a one-time patch.

**Engineering-risk read**: **highest strategic/schedule risk, moderate technical risk beyond
Option B.** The technical risk is bounded by whatever slice of Option B is attempted (same
row/cell-materialization ceiling applies if pursued). The additional risk is process risk unique to
this option: CEPs require community consensus-building on a body with no obligation to prioritize
CQLite's roadmap, and §2's record shows this can consume years with no committed outcome (Rocksandra:
open since 2017; OrioleDB: 7+ years and still not a pure extension; ZedStore: never merged despite a
core-committer champion). This option should not be attempted from a position of "prove the
abstraction via the CEP" — §2 lesson 1 says that fails; it should only be attempted after CQLite has
its own production number in hand (the MyRocks/Citus Columnar sequencing), meaning it's necessarily
gated on Option A or B already running in production somewhere first.

---

## 6. NEEDS-OWNER decisions

Deduped from the product-frame analysis (§4's underlying source):

1. **Product boundary (scope).** Is "the engine" (the new name) the Rust core only, or the Rust
   core + CEP-11 memtable plugin + compaction daemon + DataFusion/Flight/Trino connector tier,
   bundled as one HTAP distribution? Drives WS5's naming calculus, #905 Phase C's timing, and
   #1934 WS6's connector-graduation criteria.
2. **Naming sequencing.** Pull WS5 forward and settle the engine name now — unblocking #905 Phase C
   planning and crate-naming across the whole program — or hold it until WS1–WS4 stabilize the
   technical surface first?
3. **Gating consistency between #905 and #941.** Should #941's #1908 (`cqlite-datafusion` crate) be
   hard-gated on #1934 WS1 landing first, the same way #905 explicitly places itself "behind the
   #1934 Phase-1 curated API" — or is "informed by, not gated on" (the current #1934 language for
   #1908) the intended asymmetry, and why?
4. **Design-C / Phase-A′ reconciliation.** #905's own research flags a live risk of building "merge
   once, serve many" twice (#1914 vs a #905 major-compaction snapshot). Should this reconciliation
   happen as a standalone decision *before* #1914 is ever activated, and who arbitrates it?
5. **Freshness-path retirement.** Report-2's CDC-tail posture (ii) was the Q1 recommendation before
   the no-CDC decision; #2037 is its replacement, with its own §12.7 flagging an unresolved gap at
   full-scan scale pending WS7 benching. Should report-2's CDC-tail Tier-1 recommendation be
   formally retired, or kept on the shelf as a fallback if #2037's numbers don't hold?
6. **Flagship-demo arbitration.** Three epics (#905 Phase B, #941 #1912, #2037) each independently
   frame a piece of themselves as the launch demo for the new engine name. Pick one explicit launch
   narrative, or let all three proceed toward an unnamed brand for now?
7. **CEP ambition level.** Report-2 separates an upstreamable pure-Java `SSTableFormat`/`Memtable`
   path (Tier 3, ASF-contributable) from the Rust/FFI dual-write path (fork/vendor-only, never
   upstreamable — report-2 §2.2/§2.3). Does the product story ever pursue the Tier-3.6 pure-Java
   interop spike as a genuine ASF CEP pitch (echoing #2037's own WS9, "CEP pitch, optional"), or
   stay strictly adjacent/vendor-side and never attempt anything upstreamable beyond the
   already-accepted CEP-11 memtable seam? (Directly the Option C question in §5.)
8. **Ops/versioning scope for WS7.** If the assembled product spans a JVM plugin (versioned against
   Cassandra releases), a Rust crate (its own train), a daemon binary, and DataFusion/Trino
   connectors (separate ecosystems' trains), does WS7's "ops split cost model" need to broaden
   beyond its current repo-split framing to a genuinely multi-runtime, multi-release-train
   versioning matrix — beyond the "5 release trains" #1934's motivation section already counts for
   CQLite's *current* scope (which excludes any JVM-side artifact)?
9. **`SSTableFormat.Factory` adoption call (new, from §3).** Given §3's verified finding that the
   seam is "friendlier than CEP-11 memtable" on registration/versioning but shares memtable's
   identical, unresolved row/cell-materialization wall — does the product pursue Option B (attempt
   format-level interop) at all, and if so, which slice (registration/scrubbing/version metadata
   only, vs. the full reader/writer object-graph path)? This is the concrete decision Option
   B/C in §5 are gated on.

---

## 7. Sources

### Rocksandra / CASSANDRA-13474-6
- [CASSANDRA-13474](https://issues.apache.org/jira/browse/CASSANDRA-13474)
- [CASSANDRA-13475](https://issues.apache.org/jira/browse/CASSANDRA-13475)
- [CASSANDRA-13476](https://issues.apache.org/jira/browse/CASSANDRA-13476)
- [Design doc (Google Docs)](https://docs.google.com/document/d/1suZlvhzgB6NIyBNpM9nxoHxz_Ri7qAm-UEO8v8AIFsc)
- [Instagram Engineering blog](https://instagram-engineering.com/open-sourcing-a-10x-reduction-in-apache-cassandra-tail-latency-d64f86b43589)
- [dev@cassandra: RocksDB experiment result](https://www.mail-archive.com/dev@cassandra.apache.org//msg11064.html)
- [narkive: pluggable storage engine discussion](https://dev.cassandra.apache.narkive.com/SnsveVLp/pluggable-storage-engine-discussion)
- [Jeremiah Jordan JIRA comment](https://www.mail-archive.com/commits@cassandra.apache.org/msg181991.html)
- [user@cassandra: "What Happened To Alternate Storage And Rocksandra?"](https://www.mail-archive.com/user@cassandra.apache.org/msg61642.html)
- [GitHub: Instagram/cassandra rocks_3.0 (archived)](https://github.com/Instagram/cassandra/tree/rocks_3.0)
- [CEP-11: Pluggable Memtable Implementations](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=184617682)

### MyRocks / MongoDB / InnoDB-family (MySQL/MariaDB/Postgres engine-swap set)
- [Engineering at Meta: MyRocks (2016)](https://engineering.fb.com/2016/08/31/core-infra/myrocks-a-space-and-write-optimized-mysql-database/)
- [MyRocks VLDB 2020 paper](https://www.vldb.org/pvldb/vol13/p3217-matsunobu.pdf)
- [MyRocks Getting Started wiki](https://github.com/facebook/mysql-5.6/wiki/Getting-Started-with-MyRocks)
- [MyRocks limitations wiki](https://github.com/facebook/mysql-5.6/wiki/MyRocks-limitations)
- [Gap Lock issue #800](https://github.com/facebook/mysql-5.6/issues/800)
- [Issue #177](https://github.com/facebook/mysql-5.6/issues/177)
- [MySQL 8.0 Reference Manual — Pluggable Storage Engine Architecture](https://dev.mysql.com/doc/refman/8.0/en/pluggable-storage-overview.html)
- [Percona: MyRocks GA blog](https://www.percona.com/blog/myrocks-percona-server-mysql/)
- [MariaDB: MyRocks variant differences](https://mariadb.com/kb/en/differences-between-myrocks-variants/)
- [Percona: when to use MyRocks](https://www.percona.com/blog/when-to-use-myrocks-in-mysql/)
- [myrocks.io](http://myrocks.io/)
- [InfoQ: MongoDB 3.0 WiredTiger](https://www.infoq.com/news/2015/02/MongoDB-3.0-WiredTiger-MMS/)
- [MongoDB storage engine docs](https://www.mongodb.com/docs/manual/core/storage-engines/)
- [DBTA: WiredTiger revolutionized MongoDB](https://www.dbta.com/Columns/MongoDB-Matters/How-WiredTiger-Revolutionized-MongoDB-145510.aspx)
- [Percona: MongoRocks deprecation](https://www.percona.com/blog/why-weve-deprecated-mongorocks-in-percona-server-for-mongodb-3-6/)
- [Percona: PerconaFT locking incompatibility](https://www.percona.com/blog/mongorocks-deprecating-perconaft-mongodb-optimistic-locking/)
- [ScaleGrid: WiredTiger vs MMAPv1](https://scalegrid.io/blog/mongodb-storage-engines/)
- [Alibaba Cloud: MongoDB 4.0/4.2 breaking changes](https://www.alibabacloud.com/help/en/mongodb/product-overview/mongodb-versions-and-storage-engines)
- [MongoDB: WiredTiger checkpointing](https://www.mongodb.com/docs/manual/core/wiredtiger/)
- [MongoDB: replica set oplog](https://www.mongodb.com/docs/manual/core/replica-set-oplog/)
- [SolarWinds: why WiredTiger is default](https://orangematter.solarwinds.com/2017/06/14/why-wiredtiger-is-the-default-mongodb-storage-engine/)
- [InnoDB — Wikipedia](https://en.wikipedia.org/wiki/InnoDB)
- [Falcon — Wikipedia](https://en.wikipedia.org/wiki/Falcon_(storage_engine))
- [InfoWorld: Falcon MySQL 6.0](https://www.infoworld.com/article/2198360/falcon-to-be-the-major-piece-of-mysql-6-0-2.html)
- [Percona: Falcon design review](https://www.percona.com/blog/falcon-storage-engine-design-review/)
- [TokuDB — Wikipedia](https://en.wikipedia.org/wiki/TokuDB)
- [Percona: TokuDB support changes](https://www.percona.com/blog/tokudb-support-changes-and-future-removal-from-percona-server-for-mysql-8-0/)
- [Percona: TokuDB disabled reminder](https://www.percona.com/blog/tokudb-storage-engine-will-be-disabled-by-default-in-percona-server-for-mysql-8-0-26/)
- [Percona forums: why TokuDB deprecated](https://forums.percona.com/t/why-tokudb-deprecated/6866)
- [Gentoo dev-list: PBXT removal](https://www.mail-archive.com/gentoo-dev@lists.gentoo.org/msg58596.html)
- [Launchpad: PBXT embedded-server bug](https://bugs.launchpad.net/maria/+bug/439889)
- [Flaming Spork: "Where are they now"](https://www.flamingspork.com/blog/2013/04/18/where-are-they-now-mysql-storage-engines/)
- [pbxt.blogspot.com](https://pbxt.blogspot.com/)
- [MariaDB: Aria Storage Engine](https://mariadb.com/docs/server/server-usage/storage-engines/aria/aria-storage-engine)
- [MariaDB: Spider overview](https://mariadb.com/kb/en/spider-storage-engine-overview/)
- [MariaDB: Spider case studies](https://mariadb.com/docs/server/server-usage/storage-engines/spider/spider-case-studies)

### OrioleDB / zheap / Citus Columnar / ZedStore (Postgres TAM set)
- [OrioleDB: why Postgres needs a better TAM API](https://www.orioledb.com/blog/better-table-access-methods)
- [OrioleDB docs/patch status](https://www.orioledb.com/docs)
- [orioledb/postgres patch fork](https://github.com/orioledb/postgres)
- [Supabase: next steps for pluggable storage](https://supabase.com/blog/postgres-pluggable-strorage)
- [A Field Guide to Alternative Storage Engines for PostgreSQL (thebuild.com, 2026-05-08)](https://thebuild.com/blog/2026/05/08/a-field-guide-to-alternative-storage-engines-for-postgresql/)
- [EnterpriseDB/zheap GitHub](https://github.com/EnterpriseDB/zheap)
- [CYBERTEC: zheap current status](https://www.cybertec-postgresql.com/en/postgresql-zheap-current-status/)
- [Postgres wiki: Zheap](https://wiki.postgresql.org/wiki/Zheap)
- [Citus 10 release announcement](https://www.citusdata.com/blog/2021/03/05/citus-10-release-open-source-rebalancer-and-columnar-for-postgres/)
- [Citus 10 columnar compression](https://www.citusdata.com/blog/2021/03/06/citus-10-columnar-compression-for-postgres/)
- [citusdata/citus#4694](https://github.com/citusdata/citus/issues/4694)
- [Citus 12.0 table management docs](https://docs.citusdata.com/en/v12.0/admin_guide/table_management.html)
- [TimescaleDB 2.22.0 release notes](https://github.com/timescale/timescaledb/releases/tag/2.22.0)
- [timescaledb PR #8196](https://github.com/timescale/timescaledb/pull/8196)
- [VMware Open Source blog: Zedstore](https://blogs.vmware.com/opensource/2020/07/14/zedstore-compressed-columnar-storage-for-postgres/)
- [pgsql-hackers: Zedstore thread](https://www.postgresql.org/message-id/CALfoeiuF-m5jg51mJUPm5GN8u396o5sA2AF5N97vTRAEDYac7w@mail.gmail.com)
- [postgrespro.com mirror of Zedstore thread](https://postgrespro.com/list/thread-id/2436236)

### ScyllaDB / YugabyteDB (full rewrite set)
- [ScyllaDB story](https://www.scylladb.com/company/the-scylla-story/)
- [ScyllaDB SSTable format docs](https://docs.scylladb.com/manual/stable/architecture/sstable/)
- [ScyllaDB Cassandra migration process](https://docs.scylladb.com/manual/stable/operating-scylla/procedures/cassandra-to-scylla-migration-process.html)
- [ScyllaDB issue #1969](https://github.com/scylladb/scylladb/issues/1969)
- [ScyllaDB issue #20531](https://github.com/scylladb/scylladb/issues/20531)
- [YugabyteDB architecture](https://www.yugabyte.com/blog/yugabyte-db-architecture-diverse-workloads-with-operational-simplicity/)
- [YCQL vs Cassandra FAQ](https://docs.yugabyte.com/stable/faq/compatibility/)
- [Why built by reusing Postgres query layer](https://www.yugabyte.com/blog/why-we-built-yugabytedb-by-reusing-the-postgresql-query-layer/)
- [YugabyteDB Cassandra migration guide](https://www.yugabyte.com/blog/how-to-migrate-data-from-cassandra-or-mysql-to-yugabyte-db/)
- [YugabyteDB v2025.1 release](https://docs.yugabyte.com/stable/releases/ybdb-releases/v2025.2/)
- [Apache Cassandra storage engine docs](https://cassandra.apache.org/doc/4.1/cassandra/architecture/storage_engine.html)
- [Kudu architecture (reference)](https://kudu.apache.org/kudu.pdf)

### Internal anchors
- `docs/storage engine/report-2-storage-engine-feasibility.md` (Verdict, §1.2, §1.3, §2.3, §3.1,
  §3.2, §3.4, §4, §5)
- `docs/architecture/issue-2037-arrow-olap-research.md` (§1, §6, §10, §11.4, §12.7)
- `docs/architecture/issue-905-compaction-manager-research.md` (§7, §8)
- `docs/storage engine/memtable-plugin-design.md`
- Cassandra trunk checkout `50ddce8455` and `cassandra-5.0.8` tag (source-verified in §3): 
  `src/resources/META-INF/services/org.apache.cassandra.io.sstable.format.SSTableFormat$Factory`;
  `src/java/org/apache/cassandra/io/sstable/SSTable_API.md`;
  `src/java/org/apache/cassandra/db/memtable/Memtable_API.md`;
  `config/DatabaseDescriptor.java:1976`; `config/Config.java:455-459` (5.0.8: `Config.java:373`);
  `io/sstable/Descriptor.java:212-214,306-309,437-438`;
  `io/sstable/format/SSTableFormat.java:45-105,107-133,135-150,187-212`;
  `io/sstable/format/AbstractSSTableFormat.java:20-61`;
  `io/sstable/format/SSTableReader.java:160-2149,276,754-830,921,1154-1206`;
  `io/sstable/format/SSTableWriter.java:73-613`;
  `io/sstable/format/SortedTableWriter.java:312,547-551`;
  `io/sstable/format/SSTableReaderLoadingBuilder.java:51-156`;
  `io/sstable/format/SortedTableReaderLoadingBuilder.java:32-69`;
  `io/sstable/format/Version.java:34-118`;
  `db/rows/UnfilteredSource.java:29-69`;
  `io/sstable/format/big/BigTableReader.java:130,151`;
  `io/sstable/format/bti/BtiTableReader.java:370,476`;
  `io/util/FileHandle.java:50-500`; `cache/ChunkCache.java:98`;
  `db/streaming/CassandraEntireSSTableStreamWriter.java`,
  `CassandraEntireSSTableStreamReader.java:183-200`; `db/streaming/CassandraOutgoingFile.java:189-196`;
  `db/compaction/Upgrader.java:72-90,78`; `db/ColumnFamilyStore.java:1017-1019`;
  `db/compaction/CompactionManager.java:1829,1870`;
  `db/compaction/writers/CompactionAwareWriter.java:66,147-168,238,324-326`;
  `db/compaction/CursorCompactor.java:142-146`.
