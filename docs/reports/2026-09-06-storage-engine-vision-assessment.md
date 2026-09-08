# CQLite / Mustang: storage-engine vision and milestone assessment

Research assessment, 2026-09-06. This memo recommends a direction; it does not ratify drafts, change existing decisions, or update the project board. Repository baseline: `26399bb06`, workspace version `0.16.1`, including unreleased work.

**Assessment**

The research supports a broader direction than a local Cassandra toolkit: a Cassandra-compatible engine for reconciliation, materialization, analytical reads, bulk generation, and eventually delegated storage work. CQLite supplies a working implementation and its user-facing proving environment. The Mustang paper supplies the proposed architectural model: one logical segment, multiple optional representations, explicit publication and lifecycle, and routing based on semantic capabilities before cost.

This is a credible staged program. It is not evidence that a complete alternative engine can already be installed in Cassandra. The strongest initial authority boundary is the one Mustang proposes: Cassandra-readable SSTables remain authoritative; columnar representations are derived and rebuildable. External value can be proved before changing Cassandra's native storage lifecycle.

My earlier toolkit-first assessment missed the strategic center of the research. My subsequent analytics → bulk writer → compaction sequence also missed an explicit existing conclusion: offline compaction and analytics materialization should share a “merge once, serve many” capability, while a pure compaction planner/simulator can proceed in parallel.

**Method and coverage**

The review used three specialist subagents and a root integration/synthesis pass. The entire `docs/` filesystem tree was inventoried: 3,024 files before this memo, including 412 Markdown files and 1,575 Java files. Those are inventory counts, not reading counts. Broad path/content searches located research beyond the named `docs/research/` directory.

| Review lane | Coverage | Limits |
|---|---|---|
| Storage strategy | All 36 Markdown files under `docs/storage engine/` inventoried; top-level reports, design, proposal, spec, tasks, epic, Iceberg decision memo, and memtable design substantively reviewed; Mustang, assembled-engine research, broad format/HTAP surveys, memtable spike, and Sidecar projection decisions reviewed | The 27 underlying Cassandra indexes received heading/summary/correction review, not exhaustive verification of every source-anchor table; external postmortems and bibliography were not all re-audited |
| Performance | 37 primary documents inventoried; mission, throughput program, overlap experiment, August synthesis/surveys, cache decision, WS0/WS3 reports, and important audit conclusions reviewed | Phase-1/Phase-2 packet screened through its conclusions and later adjudication; no benchmark reruns or exhaustive raw-artifact inspection |
| Compaction/writing | Manager draft and source-verified synthesis, fidelity decisions, parity assessment/audit/report, write audit, release checklist, M5 research, and Java cursor applicability research reviewed | Large upstream cursor journal, generated parity table, FFM material, and API examples read selectively; binary JFR and HTML allocation reports not re-analyzed |
| Root synthesis | DataFusion council/design alternatives/promotion decision, Spark research, Trino audit, delta reconciliation, Cloudberry feasibility, milestones, release history, compatibility claims, and selected implementation paths | Integration plans/journals read selectively; no live GitHub board audit, whole-code audit, or new runtime qualification |

Two subagents encountered a usage interruption, then returned their completed syntheses after continuation. No results depend on unfinished agent work. Existing research was classified as an owner decision, proposal, measured report, derived estimate, historical audit, or current implementation evidence. Later explicit corrections outrank earlier sketches. A file's recent Git commit alone does not make its draft an accepted decision.

Selected external verification confirmed the narrow extension boundary: [CEP-11](https://cwiki.apache.org/confluence/spaces/CASSANDRA/pages/184617682/CEP-11%2BPluggable%2Bmemtable%2Bimplementations) explicitly excludes full engine replacement, and [CEP-17](https://issues.apache.org/jira/browse/CASSANDRA-17056) delivered the SSTable format API in Cassandra 5.0. This was not a fresh audit of every external source cited by the repository.

**Decision ledger: preserve what has already been learned**

| Record | Authority and finding | Consequence |
|---|---|---|
| [Sidecar Parquet projections](../architecture/cassandra-sidecar-parquet-projections.md), lines 317–341 | June boundary excluded first-party Iceberg commits and a daemon | Historical boundary; later Iceberg decisions broaden it |
| [Storage-engine feasibility report](<../storage engine/report-2-storage-engine-feasibility.md>), lines 13–29, 94–98 | Source-grounded adjacent-engine recommendation; earlier CDC advice and incorrect version statements have later corrections | Retain the missing-general-SPI finding; do not revive superseded CDC advice or repeat the old CEP-11 version error |
| [Memtable plugin design](<../storage engine/memtable-plugin-design.md>), lines 6–35 | Explicit July 3 owner decision: no CDC; on-demand, dirty-checked exports of real `nb` tail SSTables through CEP-11 | Preserve this as the selected freshness spike; the document is a draft, not shipped-plugin evidence |
| [Iceberg design](<../storage engine/design.md>), line 99; [tasks](<../storage engine/tasks.md>), line 9 | Product choices resolved; implementation tasks remain unchecked | Materialization is deliberately in scope, but not implemented merely because the design exists; first scope excludes memtable-tail exports |
| [Compaction-manager research](../architecture/issue-905-compaction-manager-research.md), lines 118–294 | Planner/executor separation; UCS source research; TOC publication sufficient for offline lifecycle; offline compaction and analytics epochs should converge | Use the synthesis instead of the older transaction-log proposal; pure planning can proceed independently |
| [Assembled-engine research](../architecture/issue-1934-assembled-engine-research.md), lines 605–725, 818–863 | Maps memtable, compaction, OLAP, DataFusion, and engine-boundary work into one program; several owner decisions remain open | Architectural alignment already exists; naming, API ownership, and duplicate materializers are unresolved coordination issues |
| [Spark research](../architecture/issue-1045-spark-connector-research.md), lines 113–150 | Owner-approved Flight-based DSv2 direction and documented single-replica v1 contract; bulk writing excluded from that connector scope | Do not reopen the consistency choice by implication or confuse read-connector scope with bulk-writer strategy |
| [DataFusion decision brief](../architecture/941-datafusion-decision-brief-2026-07.md), lines 3–51 | July 16 owner decision: spike first, promote on data; retains Design A | A competing remote-fetch draft is not automatic authority; embedding DataFusion is not itself a performance milestone |
| [Mustang paper](../research/mustang-multi-representation-storage-architecture.md), lines 6–31, 55–98, 422–476 | July 21 proposal, explicitly not accepted Apache design; neutral storage contracts, SSTable authority, optional derived Vortex, staged integration | Best strategic blueprint; crate extraction, representation choice, and CEP work still require deliberate adoption and proof |
| [Throughput mission](../architecture/0.17-throughput-mission.md), lines 330–353, 624–632 | Diagnostic-gated levers, box-level capacity, field checkpoint; older results carry newly discovered rig caveats | Reuse the existing measurement program; rebaseline with the hardened rig before making deployment claims |

**What is built, and what remains a hypothesis**

The current repository contains native readers, writers, reconciliation, offline compaction, exports, language bindings, Flight, and Trino. The manager research identifies an arbitrary-directory compaction executor independent of `WriteEngine`. This means the proposed engine has a substantial operational core rather than only component sketches.

Selected current-code checks found the workspace still organized as CQLite crates and bindings, without the proposed Mustang/Vortex/DataFusion crate structure. The compaction lane verified that directory-fsync hardening is implemented; release history records fixes to earlier TTL defects. Those historical audit findings must not be presented as current blockers. Production SSTable writing remains explicitly uncompressed in the parity claim contract.

The generated [parity report](cassandra-test-parity.md), lines 32–47, still classifies important compaction/load-path scenarios as partial or smoke evidence. Its scenario-level findings are stronger evidence than broad README language. This review did not regenerate the report or certify a release.

| Capability | Feasibility assessment | Decisive remaining proof |
|---|---|---|
| External analytics on SSTables | Working foundation; nearest operational product | Useful capacity and foreground Cassandra impact on representative, overlapping data |
| Offline compaction plus reusable analytics epochs | Strong architectural fit with substantial executor reuse | Safe publication, compression, rebuildability, freshness policy, and amortized economics |
| Bulk SSTable generation | Existing architecture supports it | Complete generation → transfer → import cost and interoperability on supported cases |
| Memtable freshness | Concrete selected design | Pre-/post-flush equivalence, lifecycle pinning, export latency, and OLTP overhead |
| Derived Vortex representation | Plausible accelerator, unproven Cassandra-specific benefit | Same-data semantic parity and equal-cost comparison against current SSTable and Parquet paths |
| Live compaction offload | Valuable but additional integration work | Cassandra-owned reservation/installation, repair-state handling, failure recovery, and coexistence |
| Full multi-representation native engine | Longer-term research/CEP program | Host lifecycle contracts, native routing, upgrades, repair/streaming, and optional-native packaging |

**Economic evidence and its limits**

The most strategically useful experiment is the [generation-overlap study](../research/issue-2043-reconcile-overlap-multiplier.md), lines 96–145. Holding generation/producer count constant, its synthetic mixed workload costs 3.24 times the disjoint control at five generations and 8.66 times at twenty. Duplicate versions, not file count alone, drive the additional work. This supports pre-reconciliation for repeated analytics. It does not measure the cost of writing, transferring, validating, or installing compacted outputs; purging was disabled, and real field overlap remains unmeasured.

The matched [WS0-3100 report](ws0-3100-report.md) records a 3.06-times eligible Flight fast-path improvement and 1.12-times Cassandra row-serving throughput on one warm, single-SSTable corpus. The later mission explicitly caveats pre-hardening WS0 results: throughput derivation, unread admission counters, and incomplete schema/session identity require remeasurement. Preserve these as historical observations, not an unconditional deployment guarantee.

The later [concurrency study](ws0-3225-report.md), lines 42–145, corrects the censored six-core optimum to 24 scans and shows that excessive admission harms latency and throughput. Cassandra was not running during that test. It informs resource control, not a claim of low OLTP interference.

The [August performance synthesis](../research/throughput-2026-08-research-synthesis.md) favors reducing owned-row intermediates and working-set size over a runtime rewrite. The expected gains are gated projections. Earlier framing/schema-cache optimizations measured zero; the [mission's retired-claim ledger](../architecture/0.17-throughput-mission.md), lines 429–450, explicitly withdraws several prior hypotheses and targets.

The [Arrow OLAP research](../architecture/issue-2037-arrow-olap-research.md), lines 1444–1530, qualifies its cross-scenario latency conclusions because shared constants were inconsistent. The broad Vortex/OpenZL and HTAP surveys establish prior art and possibilities, not measured CQLite speedups.

A useful new acceptance model is workload-specific break-even: over a declared freshness interval, the total cost of materialization plus repeated queries must beat repeated SSTable reconciliation while staying inside storage and foreground-service budgets. This is a proposed evaluation rule, not a result already measured here. It naturally distinguishes high-update, low-query tables from repeatedly scanned, relatively stable tables.

**Recommended milestones**

Retain M1–M5 as historical delivery records. Replace the remaining linear feature sequence with shared foundations, an external-value gate, and a separate Cassandra-integration gate. WASM should not block any of them.

| Milestone | Outcome | Exit evidence |
|---|---|---|
| A. Adopt the engine boundary and semantic contract | One accountable engine program, preserving CQLite as a consumer/proving surface | Decide the CQLite/Mustang boundary; define logical input identity, schema, time, repair state, supported operations, and authority. Distinguish mutation-preserving segments from current-row epochs. Curate only the APIs needed by the next consumers; no mandatory repository split |
| B. Prove shared reconciliation and materialization | One reusable job can compact supported offline SSTables and publish an analytics representation | Equivalent logical results plus preservation of merge-affecting TTL/deletion metadata in SSTable outputs; compressed production output where economics require it; protected input sets; publication and crash tests; TTL/deletion/schema cases; bounded resources; one shared implementation for manager snapshots and analytics epochs |
| C. Prove useful analytics offload | A declared workload benefits enough to deploy | Hardened-rig baseline; box-level completed work; BIG/BTI and append-only/overwrite-heavy cases; live scan versus reusable epoch comparison; total conversion/storage cost; Cassandra running under foreground load; through-Trino checkpoint |
| D. Qualify bulk writing and compaction policy in parallel | External data preparation and metadata-only planning become useful products | Bulk generation → transfer → import parity/cost. UCS planner/simulator uses explicit density/topology inputs and tested policy invariants; compare predicted and observed amplification. Neither lane waits for live compaction installation |
| E. Prove the selected freshness path | Opted-in analytical reads include the memtable tail with a stated cost | Existing no-CDC spike: pre-/post-flush parity, dirty-check behavior, bounded export latency, pool accounting, concurrent flush/snapshot stress, stale-tail retirement. Promote live-stream alternatives only if measured requirements force reconsideration |
| F. Pilot one Cassandra integration boundary | Native integration becomes measurable and reversible | Choose one bounded CEP-17 adapter or coordinated compaction pilot. Prove Cassandra-owned lifecycle, crash/retry safety, repair-state exclusions, native-boundary cost, mixed-format operation, and rollback. Use prototype evidence to specify missing hooks |
| G. Harden the supported engine; advance lifecycle CEPs selectively | A supported distribution with known authority and compatibility boundaries | Sustained operator pilots, release-specific evidence, upgrade/rollback matrix, recovery/repair/streaming tests for claimed scope. Atomic multi-representation publication and native routing advance only after their required contracts are accepted or explicitly owned in a research fork |

Milestones A and the current throughput program should begin together. The metadata-only UCS work can run alongside B. Bulk qualification shares B's writer work but need not wait for C. E is needed for the stronger freshness promise, not for useful snapshot analytics. F should remain bounded; do not fund both broad native-format adoption and live compaction integration before one proves its boundary and value.

Versioning should follow each supported contract. An external engine release can reach 1.0 after its applicable B–D workflows and operational evidence stabilize; it need not wait for a native Cassandra lifecycle CEP. Native adapters should retain a separate experimental/support designation until their F–G gates pass.

**Improvements to the existing plan**

1. Make one team/module own logical reconciliation and materialization. The research already identifies duplicate “merge once, serve many” proposals. Sharing parsers alone is insufficient; share job input identity, output publication, lifecycle, and validation where their semantics actually match.
2. Keep representation semantics explicit. A per-generation cache must preserve the Cassandra mutation envelope. A fully reconciled epoch can expose current rows at a fixed reference time. A current-row epoch cannot silently stand in for a mutation source during later reconciliation.
3. Keep SSTable authority and test fallback. Derived-format absence or corruption should permit a correct SSTable path where the query contract allows it. Commit-log recycling and authoritative input retirement must not depend on an unproven derived representation.
4. Treat compression as shared infrastructure for compaction and bulk writing. An uncompressed replacement can increase disk and network costs relative to compressed inputs. Writer validity alone does not settle offload economics.
5. Treat Vortex as the named candidate, with a measured selection gate. Preserve the neutral representation contract; benchmark against the existing Parquet/Arrow path on the same data. Avoid making every table pay for a second format before its workload warrants it.
6. Preserve local versus distributed consistency boundaries. One selected replica and a fixed file set do not establish a global consistent cut. The documented v1 single-replica choice is an accepted scope, not an implementation defect to obscure or silently strengthen.
7. Use source-coverage watermarks. A maximum mutation timestamp does not, by itself, prove receipt of a complete prefix when timestamps arrive out of order. Track consumed source identities/coverage separately from event-time maxima. This is an assessment inference requiring validation of the materializer contract.
8. Keep policy choice separate from execution safety. The pure UCS planner is an early deliverable; exact randomized Cassandra job selection is not the right correctness bar. Output readability, logical equivalence, safe purging, and lifecycle behavior are.
9. End naming and repository decisions as critical-path blockers. Define ownership and unpublished interfaces now; publish names and split repositories when concrete consumers justify the maintenance cost.
10. Scope counters per operation. Producing external counter increments and merging existing counter contexts are different capabilities. Exclude unsupported counter tables from delegated compaction until context preservation and merge semantics are qualified explicitly.

**Research conflicts to close before promoting the roadmap**

- The June [fidelity decision](../garbage-free-compaction-improvements/compaction-fidelity-bar-decision.md), line 17, makes Cassandra readability and logical equivalence mandatory and identical bytes a non-goal; [byte-parity rules](../compaction/byte-parity-rules.md), line 3, still make a blanket byte requirement. Ratify one product contract while retaining scenario-specific byte oracles.
- The [remote-fetch DataFusion draft](../datafusion-table-provider-design.md) rejects per-node Flight, while the council and later owner decision retain Design A. Mark proposal lineage and authority explicitly; avoid interpreting both as the current plan.
- The [assembled-engine record](../architecture/issue-1934-assembled-engine-research.md), lines 460–479, describes global format selection, while Mustang proposes a single-table canary. The adapter spike must establish how selective adoption works on the pinned Cassandra release.
- The throughput mission computes a per-core “within 1.3-times bare-scan cost” target using division, but describes a box target with multiplication in places. Specify the acceptance equation and unit before a milestone depends on it.
- Older support pages and issue lists are historical. Current claims should be generated from release-specific capability/evidence records. Fixed TTL, directory-fsync, streaming, and snapshot defects must not remain roadmap blockers because an old audit still lists them.

**Recommended next commitment**

Adopt the proposed engine boundary, preserve the current measurement gates, and select one overwrite-heavy repeated-analytics workload as the first full demonstration. Show the same protected input set served through the existing live merge and through a reusable, rebuildable materialization; measure correctness, amortized cost, freshness, storage, and Cassandra foreground impact. In parallel, qualify compressed bulk output and the pure UCS planner. That proves the shared engine's value before asking Cassandra to delegate authoritative live storage lifecycle.
