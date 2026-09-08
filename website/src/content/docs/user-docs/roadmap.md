---
title: Roadmap
description: CQLite's storage-engine direction, qualification milestones, and path to 1.0.
sidebar:
  label: Roadmap
  order: 9
---

# Roadmap

CQLite is developing a Cassandra-compatible storage engine for analytics, reusable
materialization, bulk ingestion, and compaction. The CLI, language bindings, and
query interfaces remain supported product surfaces.

The [Product Requirements](https://github.com/pmcfadin/cqlite/blob/main/docs/development/PRD.md)
define the scope, guarantees, and exit criteria. The
[research assessment](https://github.com/pmcfadin/cqlite/blob/main/docs/reports/2026-09-06-storage-engine-vision-assessment.md)
explains the rationale. GitHub tracks execution; the
[changelog](https://github.com/pmcfadin/cqlite/blob/main/CHANGELOG.md) records shipped behavior.

_Last reviewed: 2026-09-06._

## Qualification milestones

These milestones are **qualification pending**. Existing code and research provide
a foundation; completion requires the PRD's workload and operational evidence.

| Milestone | Outcome |
|-----------|---------|
| SE1 — Engine contract | Explicit capabilities, authority, supported workloads, and measurable acceptance criteria |
| SE2 — Shared offline execution | Safe compaction and reusable analytics materialization, including qualified compressed output |
| SE3 — Analytics offload value | Measured analytics benefit within foreground Cassandra latency, resource, and freshness budgets |
| SE4 — Bulk writing and policy tools | Bounded bulk generation through native Cassandra import, plus compaction planning and simulation |
| SE5 — Qualified freshness | Proven memtable-tail behavior and cost for opted-in analytical reads |
| SE6 — Bounded Cassandra integration | One reversible native-format or coordinated-compaction pilot |
| SE7 — Native lifecycle qualification | Supported integrated behavior backed by recovery, lifecycle, and operator evidence |

Compaction planning can proceed alongside the engine contract. Bulk writing shares
output qualification with offline compaction. Live analytics measurement can
continue while reusable materialization is developed.

## What 1.0 means

External-engine **1.0 requires SE1–SE4**, independent operator evidence, stable public
contracts, and the release checks specified in the PRD. WASM, Vortex adoption, a
continuous Iceberg daemon, and native Cassandra integration are not prerequisites.
Freshness and native integrations retain separate experimental status until their
applicable gates pass.

The initial authority remains Cassandra-readable SSTables. New analytical
representations are derived and rebuildable. The proposed Mustang architecture and
Vortex representation are candidates to validate, not promises of shipped support.

## Historical milestones

M1–M5 delivered core reading, the CLI, output writers, Python/Node bindings, and
writing with STCS compaction. Those accomplishments remain the foundation.

The old M6 (WASM) and M7 (performance/size/1.0) sequence is replaced by the
qualification program above. The
[original PRD](https://github.com/pmcfadin/cqlite/blob/main/docs/development/PRD-toolkit-v0.2.md)
preserves that history. Historical completion does not imply universal compatibility
or qualify every new offload workflow.

## Influencing priorities

[Open an issue](https://github.com/pmcfadin/cqlite/issues/new/choose) describing the
workload, Cassandra version, data shape, freshness requirement, current cost or
latency, and the improvement that would make deployment worthwhile. Operator
workloads and reproducible evidence guide qualification priorities.
