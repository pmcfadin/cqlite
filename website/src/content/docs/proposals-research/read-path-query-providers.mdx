---
title: Read Path & Query Providers
description: "CQLite's proposed direction for read-path provider work: partition reads, Flight/Trino stats, and DataFusion."
sidebar:
  label: Read Path & Query Providers
  order: 5
tableOfContents: false
---

import { Card, CardGrid, LinkCard } from '@astrojs/starlight/components';
import MermaidDiagram from '../../../components/MermaidDiagram.astro';

export const queryProviderMermaid = `flowchart LR
  subgraph Core["Shared read core"]
    direction TB
    Snapshot["Snapshot / data-dir discovery"]
    FastPath["Partition read fast path"]
    Stats["Token-range scoped stats"]
    Snapshot --> FastPath --> Stats
  end

  subgraph Providers["Provider surfaces"]
    direction TB
    Flight["Flight / Trino"]
    DataFusion["DataFusion table provider"]
  end

  Stats --> Flight
  FastPath --> DataFusion`;

export const queryStatsMermaid = `flowchart LR
  Splits["Token-range splits"] --> Replica["One replica per logical range"]
  Replica --> Stats["RF-correct row counts"]
  Stats --> Optimizer["Trino / provider optimizer"]`;

# Read Path & Query Providers

**Status:** Proposed direction.

The query-provider enhancement issues are one family: keep the read core honest
and reusable, then expose it through Flight/Trino and DataFusion without
duplicating Cassandra-specific rules in each consumer.

## Public summary

<CardGrid>
  <Card title="One read core" icon="puzzle">
    Snapshot discovery, partition reads, token pruning, predicate evaluation, and
    statistics should be shared below provider-specific APIs.
  </Card>
  <Card title="Provider adapters" icon="rocket">
    DataFusion should use the same snapshot, split, and predicate model that
    already feeds Flight and Trino.
  </Card>
  <Card title="RF-correct stats" icon="document">
    Optimizer row counts need token-range scope so replicated tables do not look
    larger than the logical data set.
  </Card>
</CardGrid>

## Provider diagram

<MermaidDiagram
  chart={queryProviderMermaid}
  size="compact"
  caption="The proposed direction is a shared read core with provider-specific surfaces above it."
  sourceLabel="Mermaid source for the provider diagram"
/>

## Statistics path

<MermaidDiagram
  chart={queryStatsMermaid}
  size="compact"
  caption="Logical optimizer statistics should follow the same token-range split model used for replica-correct reads."
  sourceLabel="Mermaid source for the statistics diagram"
/>

| Issue | Coverage | Public interpretation |
|-------|----------|-----------------------|
| [#941](https://github.com/pmcfadin/cqlite/issues/941) DataFusion table provider | Direct | Add a DataFusion provider after the reusable read core is stable. |
| [#942](https://github.com/pmcfadin/cqlite/issues/942) partition read path | Direct | Route single-partition reads through the cheaper read path instead of compacting to memory unnecessarily. |
| [#1336](https://github.com/pmcfadin/cqlite/issues/1336) RF-correct optimizer row counts | Direct | Stats must be token-range scoped rather than summed across replicas. |

## Research sources

<CardGrid>
  <LinkCard
    title="Flight / Trino plan"
    description="The existing plan for snapshot reads, token ranges, predicate pushdown, and Trino splits."
    href="https://github.com/pmcfadin/cqlite/blob/main/docs/flight-trino/PLAN.md"
  />
  <LinkCard
    title="Fast analytics design"
    description="The broader Arrow Flight and query-provider design context."
    href="https://github.com/pmcfadin/cqlite/blob/main/docs/plans/2026-06-17-cassandra-fast-analytics-arrow-flight-design.md"
  />
  <LinkCard
    title="Point-read fast path"
    description="Design notes for avoiding unnecessary full merge work on point and partition reads."
    href="https://github.com/pmcfadin/cqlite/blob/main/docs/942-point-read-fast-path-design.md"
  />
  <LinkCard
    title="DataFusion provider council"
    description="Architecture analysis for the DataFusion provider and the shared scan contract."
    href="https://github.com/pmcfadin/cqlite/blob/main/docs/architecture/issue-941-datafusion-table-provider-council.md"
  />
</CardGrid>

Primary issue links:

- [#941 DataFusion table provider](https://github.com/pmcfadin/cqlite/issues/941)
- [#942 partition read path](https://github.com/pmcfadin/cqlite/issues/942)
- [#1336 RF-correct optimizer row counts](https://github.com/pmcfadin/cqlite/issues/1336)
