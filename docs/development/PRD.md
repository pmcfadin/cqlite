# CQLite Product Requirements: Cassandra-Compatible Storage Execution

**Revision:** 2026-09-06 — storage-engine direction. This is a product-document revision, not a software release version.

**Product owner:** Patrick McFadin. **Status:** current product direction and qualification requirements; open architecture choices are identified below. No milestone is complete merely because its design or implementation exists.

This PRD replaces the toolkit-focused milestone sequence for future planning. The [original PRD v0.2](PRD-toolkit-v0.2.md) is preserved unchanged as a historical record. The [research assessment and decision ledger](../reports/2026-09-06-storage-engine-vision-assessment.md) explains the evidence and changes in direction. GitHub tracks execution status; the [changelog](../../CHANGELOG.md) records shipped behavior; release-specific compatibility evidence governs support claims.

## 1. Mission and product thesis

**Mission:** Make expensive Cassandra data work independently executable, reusable, and measurable while preserving Cassandra's storage semantics and operational safety.

**Vision:** A Cassandra-compatible storage engine that shares reconciliation and materialization across analytics, bulk ingestion, and compaction, with incremental integration into Cassandra.

The engine should let an operator prepare data once and reuse that work where the workload warrants it. Analytical queries can scan a protected SSTable set directly or read a reusable materialization. Bulk writers prepare Cassandra-readable files outside the database. Offline compaction reconciles existing files. Later, Cassandra may delegate selected live storage work through a qualified integration boundary.

The product succeeds when those workflows reduce total cost or improve useful capacity within stated correctness, freshness, resource, and foreground-service budgets. Component count, implementation language, isolated throughput, and a second physical representation are not success criteria by themselves.

## 2. Users and primary outcomes

| User | Job to accomplish | Required outcome |
|---|---|---|
| Cassandra operator | Run analytics without unacceptable interference with operational traffic | Measured foreground latency, CPU/I/O, snapshot retention, and freshness within an agreed workload budget |
| Analytics engineer | Query Cassandra data repeatedly through familiar analytical tools | Correct results under an explicit source/consistency contract; reusable materializations when they improve economics |
| Data pipeline engineer | Prepare and ingest bulk data | Bounded-resource generation of portable SSTables; verified Cassandra consumption and end-to-end cost |
| Storage operator | Compact backups, snapshots, or engine-owned files; evaluate compaction policy | Safe offline transformation, understandable plans, and measured space/read/write amplification |
| Engine integrator | Reuse storage execution from a library or service | Stable supported interfaces, explicit capabilities, cancellation, diagnostics, and reproducible validation |

Local inspection, CLI use, Python, Node.js, and export remain supported product surfaces and practical ways to diagnose and exercise the engine. Their historical delivery does not by itself qualify distributed analytics or live compaction offload.

## 3. Product and ownership boundaries

| Layer | Responsibility | Boundary |
|---|---|---|
| CQLite product | CLI, bindings, embedded access, Flight, connectors, conversion, and diagnostics | Preserve supported public behavior while engine internals evolve; no mandatory repository split |
| Shared storage execution | Schema-aware decoding, mutation semantics, reconciliation, writing, compaction execution, and reusable materialization | Independent of CLI/binding/connector implementation details; share semantics and lifecycle where output contracts match |
| Analytical host | Distributed query planning and the host engine's scheduling | Trino retains its MPP responsibilities; a DataFusion experiment must establish its role and benefit without duplicating them |
| Cassandra | Live-write durability, replication, consistency coordination, repair, and authoritative live-file lifecycle | External tools cannot independently replace Cassandra's live file set; native integration requires a separate qualified contract |

The [Mustang proposal](../research/mustang-multi-representation-storage-architecture.md) is the architectural candidate for neutral logical segments, optional representations, publication, and routing. Its name, exact package structure, production columnar format, and native integration are not implicitly ratified by this PRD. New shared storage interfaces must avoid dependencies on product-specific surfaces; extraction should follow actual consumers and preserve compatibility.

**Initial authority:** Cassandra-readable SSTables remain authoritative. New analytical representations are derived and rebuildable. Changing durability authority, commit-log retirement, or native live-file ownership is outside the initial external-engine release.

## 4. Scope and baseline

**Baseline at this revision:** the workspace identifies version 0.16.1 and contains unreleased work. Reading, CLI, exports, Python/Node bindings, writes, reconciliation, offline compaction, Flight, and Trino are implemented. The [parity report](../reports/cassandra-test-parity.md) and [claim manifest](../../test-data/cassandra-parity-manifest.yml) qualify the evidence; broad historical “production-ready” wording is not a substitute for them.

| Workload | Near-term scope | Separate qualification |
|---|---|---|
| Analytics | Protected SSTable reads and reusable current-row analytics epochs | Foreground coexistence, distributed source contract, and end-to-end benefit |
| Materialization / offline compaction | Shared reconciliation of explicit input sets into supported outputs | Publication/recovery, safe purging, compression, and amortized cost |
| Bulk writing | Schema-aware mutation ingestion and external-sort/file-generation workflows | Native Cassandra consumption, resource bounds, and complete generation-to-import economics |
| Compaction policy | Pure metadata planning, explanation, and UCS simulation | Policy invariants and predicted-versus-observed amplification; exact randomized job selection is not required |
| Fresh analytical reads | Selected CEP-11 memtable export spike | Tail lifecycle, pre-/post-flush equivalence, export latency, and write-path impact |
| Live storage offload | Bounded integration research | Cassandra-owned reservation and installation, repair-state handling, and fault-tested recovery |

Target Cassandra 5.0 formats and explicitly qualified version combinations. BIG and BTI capabilities must be reported separately by operation and release; “Cassandra 5+” must not imply support for every future format. Compression reading and production compression writing are separate capabilities. Production SSTable output is currently uncompressed; qualifying compressed output is a requirement of the new offload program, not an existing support claim.

## 5. Product requirements

The following requirements apply to each advertised workload. A capability matrix must identify exclusions and the evidence supporting each promise.

| ID | Requirement | Acceptance evidence |
|---|---|---|
| R1 | **Explicit input and capability contract.** Bind a job/query to source identities, table incarnation, schema, format/version, token coverage, and the time/repair metadata its operation requires. Reject unsupported inputs or expose an explicitly documented partial/export mode; never silently discard data | Supported and unsupported cases through public interfaces; schema/version mismatch and incomplete-input tests |
| R2 | **One reconciliation foundation.** Preserve per-cell timestamps, equal-timestamp rules, TTL, static rows, schema evolution, and supported cell/row/range/partition deletions. Query and compaction policies must be explicit | Independent Cassandra logical oracles; point/full-scan differential checks; appropriate byte oracles. Self-written/read-back fixtures alone do not qualify semantics |
| R3 | **Different output contracts remain distinct.** Mutation-preserving SSTables or per-generation caches retain metadata required for future merges. Current-row epochs describe a fixed source set and TTL reference time | Future-merge tests for SSTable/cache output; fixed-epoch logical tests for current rows; no substitution between these representations without a semantic conversion |
| R4 | **Portable native output.** Bulk and compaction outputs must be consumable by the qualified Cassandra version without CQLite conversion or repackaging | Direct native refresh/import plus query verification on real Cassandra. `sstableloader` is a useful secondary path, not the sole portability proof |
| R5 | **Safe publication and recovery.** Readers see a complete published result or an explicit failure. Preserve required authoritative inputs until replacement is committed. Define durability/acknowledgment and retry behavior for writable jobs | Crash/disk-full/corruption/cancellation tests around publication and retirement; orphan cleanup; retry tests. Use existing offline publication machinery where sufficient |
| R6 | **Bounded resources and terminal failures.** Enforce stated memory, temporary-disk, queue, concurrency, and cancellation budgets, including wide partitions and slow consumers. Producer loss must never appear as successful EOF | Resource measurements across input sizes and fan-in; overload and producer-failure tests through shipped paths; explicit errors when budgets cannot be met |
| R7 | **Visible freshness and consistency.** Expose source/epoch identity and reference time. Preserve the documented single-selected-replica v1 scope; do not imply quorum reads or a global consistent cut. Stale results require an explicit allowed policy | Divergent-replica, snapshot, retry, and TTL-boundary tests; freshness metadata. Maximum event timestamp alone cannot certify complete source coverage |
| R8 | **Useful compaction and materialization economics.** Measure overlap, purge safety, compressed output, transfer, retained inputs, and rebuild cost. One implementation should serve the common “merge once, serve many” work | Offline correctness suite plus repeated-query break-even measurements; compression/import validation; planner prediction versus executed workload |
| R9 | **Bulk ingestion independent of CQL parsing.** Preserve mutation-based input as the primary engine path; CQL is a convenience interface. Support bounded ingestion/sorting, explicit completion, and documented failure/retry semantics | Large unsorted input through public ingestion surfaces, interruption/recovery, duplicate/retry behavior, and native Cassandra consumption |
| R10 | **Safe deployable interfaces.** Published services constrain accessible data and document authentication/authorization and transport requirements. Record per-job progress, failures, resource use, input/output identities, and cleanup state | Deployment tests and operator runbook for the supported topology; enforce access boundaries; verify metrics reflect actual work and failures |

Tombstone purging must satisfy the declared retention/repair policy and protect data in excluded overlapping inputs. If required coverage or safety metadata is unavailable, retain tombstones. Acceptance includes partial-compaction resurrection and GC-boundary tests; this requirement applies to offline compaction as well as native integration.

Counter support must be declared per operation: generating increments and compacting existing counter contexts are different capabilities. Exclude unsupported counter tables from offload and freshness claims until those semantics are independently qualified.

Identical bytes are required only where a specific format/component contract or declared byte oracle requires them. Product-level compaction acceptance requires native readability, logical equivalence, and preservation of merge-affecting metadata. This resolves the conflicting blanket byte-identity wording identified in the research; it does not remove existing byte-level regression gates or broaden parity claims.

## 6. Success metrics and qualification protocol

Before a qualifying run, the product owner and operator must record a **workload acceptance sheet**: source/version/schema, operation and output contract, data shape and overlap, deployment topology, freshness requirement, baseline, resource budget, minimum useful benefit, foreground latency limit, and failure criteria. Thresholds must be numeric and fixed before the run; a milestone cannot pass with those fields unspecified or adjusted after seeing the result.

| Dimension | Required measurement |
|---|---|
| Useful capacity | Completed logical work and box-level aggregate throughput; time to first result and total job/query duration |
| Foreground protection | Cassandra p95/p99 read/write latency, errors, flush/compaction behavior, CPU, and I/O with Cassandra running |
| Total cost | Conversion, reconciliation, compression, transfer, validation/import, retries, extra compute, temporary storage, retained snapshots, and derived storage |
| Reuse | Materialization build/rebuild cost and repeated-query break-even within the declared freshness interval |
| Freshness / correctness | Source coverage, epoch age, export latency, TTL reference time, exact logical result checks, and unsupported-case behavior |
| Capacity under overload | Memory/disk high-water marks, queue time, admission shedding, tail latency, cancellation, and cleanup |

Qualification must cover append-only and overwrite-heavy inputs, realistic overlap and compression, and the formats/types promised for that workload. Declare hardware, physical versus logical cores, cache state, byte basis, repetitions, uncertainty, and applicable fast-path eligibility. Compare equivalent results on the same input; report both equal-hardware and complete-cost implications where offload uses additional resources.

Use the existing [throughput mission](../architecture/0.17-throughput-mission.md) and its measurement → gated change → box-level verification → through-Trino checkpoint. Pre-hardening benchmark caveats require a fresh qualifying baseline. The old 600k/core target and an unqualified “faster than Cassandra” are not acceptance bars. Every relative target must state its equation and denominator; per-core attribution cannot substitute for fleet/deployment capacity.

An isolated microbenchmark can justify a change, but cannot alone qualify operational offload. At least one independent operator pilot must reproduce each advertised production workload's acceptance result before its 1.0 qualification.

## 7. Milestones and dependency gates

The new **SE** identifiers avoid reusing M1–M7 and existing research workstream numbers. Their status at this revision is **qualification pending**; evidence links, not issue counts, determine completion.

| Milestone | User/operator outcome | Exit criteria | Dependencies |
|---|---|---|---|
| **SE1 — Engine contract** | Integrators and operators know exactly what the engine owns and guarantees | Supported workload/capability matrix, authority and output contracts, acceptance sheets, interface ownership, and documented exclusions; unresolved choices have an owner and decision trigger | Starts now; uses existing code and research |
| **SE2 — Shared offline execution** | Safely compact supported inputs and publish reusable analytics output | R1–R6/R8 pass for protected offline inputs; preserve future-merge metadata; at least one qualified production compression codec; fault-tested publication and cleanup; shared reconciliation/materialization across compaction and analytics | SE1 contracts; incremental API extraction only as needed |
| **SE3 — Analytics offload value** | Analytics delivers a measured benefit within a foreground-service and freshness budget | Preregistered workload passes on hardened measurements with Cassandra running; compare live merge and reusable materialization; include overwrite-heavy data, full build/storage costs, and a through-Trino operator pilot | SE1; SE2 for the reusable-output arm. Existing live-path measurement proceeds in parallel |
| **SE4 — Bulk writing and policy tools** | Prepare bulk data efficiently and evaluate compaction plans | **Writer gate:** bounded generation → transfer → native import, supported mutation/type parity, retries, compressed output, and operator cost target. **Planner gate:** pure explain/simulate interfaces, tested UCS density/overlap/shard invariants, and predicted-versus-observed amplification | Writer shares SE2 output qualification. Metadata-only planner research/prototyping can start alongside SE1; neither waits for live installation |
| **SE5 — Qualified freshness** | Opted-in analytical reads include the memtable tail at a stated cost | Selected no-CDC spike passes pre-/post-flush parity, dirty-check/min-interval behavior, write-path/pool safety, concurrent flush/snapshot tests, export latency, and stale-tail retirement | SE1 and qualified read semantics; required only for the stronger tail-freshness promise |
| **SE6 — Bounded Cassandra integration** | Demonstrate one reversible native/offload boundary | Choose one CEP-17 adapter or coordinated-compaction pilot; prove reservation/installation ownership, repair-state constraints, native-boundary cost, failure/retry recovery, mixed-format behavior, and rollback | Qualified underlying SE2/SE4 functionality and an explicit integration design; do not assume a general engine SPI |
| **SE7 — Native lifecycle qualification** | Operators can depend on the claimed Cassandra-integrated scope | Upgrade/restart/repair/streaming/snapshot/bootstrap coverage as applicable; sustained pilot, support matrix and runbooks; necessary host contracts accepted upstream or explicitly maintained as experimental fork scope | SE6; community acceptance is required only for claims of upstream-supported new contracts |

SE1–SE4 define the initial external-engine program. Existing component maturity may discharge parts of those gates, but prior M1–M5 completion does not discharge them automatically. SE5–SE7 can advance selectively after their prerequisite experiments; they are not mandatory dependencies for useful snapshot analytics or offline bulk/compaction work.

**Initial flagship demonstration:** one overwrite-heavy repeated-analytics workload over a protected input set, served through both live reconciliation and reusable materialization. Demonstrate equivalent results, explicit freshness, amortized cost, space overhead, and Cassandra foreground impact. Qualify compressed bulk output and the pure UCS planner alongside this work.

## 8. Release and support contract

External-engine **1.0 requires SE1–SE4**, the independent operator evidence above, stable public error/type/cancellation contracts, installation/upgrade documentation, and green release gates on the exact release commit. A first materialization output may use a qualified existing format; 1.0 does not require Vortex, a continuous Iceberg daemon, or a native Cassandra adapter.

Keep tail-freshness and native integration labeled experimental until their applicable SE5–SE7 gates pass. WASM size or availability and acceptance of a native lifecycle CEP do not gate external-engine 1.0. Release numbers alone must not imply all interfaces and operations have equal maturity.

Maintain a release-specific matrix of Cassandra versions, read/write formats, codecs, types, operations, interfaces, and deployment modes. Link limitations and required rewrite/migration steps to affected versions. Preserve supported CLI, Rust, Python, Node, Flight, and Trino contracts or ship explicit migration guidance.

The [release procedure](RELEASING.md), [parity release checklist](parity-release-checklist.md), [parity tier contracts](parity-ci-tiers.md), and [manifest rules](cassandra-parity-manifest.md) remain the execution authority for checks. This PRD does not waive tests, Clippy, coverage policy, nightly/exhaustive evidence, or review requirements. Byte claims remain scenario-scoped; file parity does not imply Cassandra node-lifecycle parity.

Retain Apache 2.0 licensing and community-oriented development. Pursue upstream collaboration through measured, bounded proposals; neither a donation date nor an Apache design acceptance is promised by this PRD.

## 9. Recorded decisions, experiments, and non-goals

| Topic | Standing decision or requirement | Unresolved / promotion trigger |
|---|---|---|
| Freshness | Owner-selected no-CDC, on-demand dirty-checked CEP-11 export of real tail SSTables | Spike remains unqualified. Live FFI/streaming alternative reconsidered only if measured export behavior cannot meet the chosen requirement |
| DataFusion | July 16 spike-first, promote-on-data decision; Design A is its recorded basis | Role, version alignment, and promotion require the experiment; do not treat a competing remote-fetch draft as the accepted plan |
| Materialization | Unify shared “merge once, serve many” work. Preserve the existing initial Iceberg design's bounded envelope-input scope | Continuous watching, cluster-wide lineage/dedup, repaired gating, REST catalogs, and maintenance are separate increments; initial Iceberg scope excludes tail exports |
| Mustang / Vortex | Leading architectural proposal; SSTable authority and rebuildable derived output are initial requirements | Final branding, package/repository split, Vortex production adoption, and synchronous dual-output policy require explicit decisions and workload evidence |
| Native integration | Preserve Cassandra's authoritative lifecycle until the chosen adapter is qualified | Native routing, atomic multi-representation groups, and coordinated multi-output compaction need additional contracts; no claim that existing plugin APIs provide the complete engine |

Initial non-goals: replacing Cassandra's distributed coordination or commit log; a general drop-in `StorageEngine` SPI; independently authoritative columnar/object-store data; quorum or global-snapshot guarantees for the single-replica analytical path; universal byte identity; an all-purpose SQL engine or a second distributed scheduler inside Trino; every connector/runtime; and browser/WASM delivery as a prerequisite to the engine.

## 10. Risks and decision ownership

| Risk / open decision | Owner | Required resolution |
|---|---|---|
| Semantic divergence between row and derived output | Storage execution maintainers | R2/R3 oracles before routing/representation promotion; scope counter behavior per operation |
| Extra representations cost more than they save | Product owner and pilot operator | Preregistered break-even and foreground budgets; allow one representation when duplication has no benefit |
| Native lifecycle or FFI scope exceeds available seams | Cassandra integration lead and product owner | One bounded SE6 design; pinned-version verification, crash-domain cost, and explicit upstream/fork boundary |
| Product split or naming blocks useful work | Product owner | Resolve interface ownership in SE1; keep repository separation and published names off the experiment's critical path |
| Unsupported watermark/completeness assumptions | Materialization lead | Distinguish source coverage from event-time maxima; prove late/out-of-order input handling before incremental claims |
| Research and shipping status diverge | Release lead | Maintain release evidence and decision lineage; supersede stale recommendations rather than treat old audits as current bug lists |

Detailed design belongs in linked architecture/OpenSpec records. Open choices must not be presented as shipped features or quietly become milestone dependencies. Missing operator thresholds are qualification blockers, not an invitation to invent favorable thresholds after measurement.

## 11. Historical baseline and research map

| Legacy milestone | Historical deliverable | Treatment in this PRD |
|---|---|---|
| M1 | Core reading | Existing foundation; operation-specific evidence still governs support |
| M2 | CLI | Supported interface and engine diagnostic surface |
| M3 | Output writers | Existing export foundation for materialization |
| M4 | Python and Node.js | Supported embedding and workflow surfaces |
| M5 | Writing and STCS compaction | Existing execution foundation; additional offload qualification in SE2/SE4 |
| M6 | WASM | Deferred optional surface; removed from the 1.0 critical path |
| M7 | Performance, size, and 1.0 | Replaced by workload qualification in SE1–SE4 and the release contract above |

Start with the [assessment and coverage ledger](../reports/2026-09-06-storage-engine-vision-assessment.md). Supporting records:

- [Mustang architecture and proposed phases](../research/mustang-multi-representation-storage-architecture.md) and [assembled-engine composition/decisions](../architecture/issue-1934-assembled-engine-research.md).
- [Compaction-manager synthesis](../architecture/issue-905-compaction-manager-research.md), [M5 writer council](../research/M5-Write-Support-Council-Recommendation.md), and [fidelity decision](../garbage-free-compaction-improvements/compaction-fidelity-bar-decision.md).
- [DataFusion council](../architecture/issue-941-datafusion-table-provider-council.md), [promotion decision](../architecture/941-datafusion-decision-brief-2026-07.md), and [Spark/consistency decisions](../architecture/issue-1045-spark-connector-research.md).
- [Memtable design](<../storage engine/memtable-plugin-design.md>), [Iceberg proposal](<../storage engine/proposal.md>), and [Iceberg decisions](<../storage engine/design.md>). The initial Iceberg type/statics/exclusion behavior remains governed by that scoped design, not by an implied general row-image/CDC contract.
- [Overlap measurement](../research/issue-2043-reconcile-overlap-multiplier.md), [throughput mission](../architecture/0.17-throughput-mission.md), and [August performance synthesis](../research/throughput-2026-08-research-synthesis.md).
