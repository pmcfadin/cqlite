---
title: Lakehouse Materialization
description: "CQLite's proposed Iceberg materializer path for turning Cassandra delta envelopes into queryable lakehouse tables."
sidebar:
  label: Lakehouse Materialization
  order: 3
tableOfContents: false
---

import { Card, CardGrid, LinkCard } from '@astrojs/starlight/components';
import MermaidDiagram from '../../../components/MermaidDiagram.astro';

export const materializerMermaid = `flowchart LR
  subgraph Extract["Extract"]
    direction TB
    Input["SSTable generations"]
    Delta["Delta envelope stream"]
    Input --> Delta
  end

  subgraph Materialize["Materialize"]
    direction TB
    Fold["Fold engine"]
    Files["Data + delete files"]
    Fold --> Files
  end

  subgraph Publish["Publish"]
    direction TB
    Commit["Delete-aware commit"]
    Engines["Iceberg table + engines"]
    Commit --> Engines
  end

  Delta --> Fold
  Files --> Commit`;

export const materializerDecisionMermaid = `flowchart LR
  Hybrid["HYBRID H1<br/>adopt writers, build commit"]
  Feature["Feature-isolated<br/>iceberg + Arrow 57 island"]
  Later["Follow-ups<br/>daemon, RF dedup, repair gating, REST catalog"]

  Hybrid --> Feature --> Later`;

# Lakehouse Materialization

**Status:** Proposed epic direction.

Delta export produces a faithful change envelope, but it is still a log. The
materializer moves the fold boundary into CQLite: it consumes delta envelopes,
applies Cassandra reconciliation rules, and commits Apache Iceberg v2 snapshots
that query engines can read directly.

## Public summary

<CardGrid>
  <Card title="Consumers read tables" icon="document">
    Trino, DuckDB, Spark, and other engines should query current table state
    without owning Cassandra tombstone and last-write-wins merge logic.
  </Card>
  <Card title="Use delta envelopes" icon="puzzle">
    The materializer consumes the existing `scan_delta` envelope stream, not raw
    SSTable files, so one extraction path owns tombstone fidelity.
  </Card>
  <Card title="Hybrid build/adopt" icon="rocket">
    Adopt iceberg-rust for writers and metadata building blocks, then build the
    delete-aware snapshot commit layer that the released crate does not expose.
  </Card>
</CardGrid>

## Materialization diagram

<MermaidDiagram
  chart={materializerMermaid}
  size="compact"
  caption="CQLite folds delta envelopes into data and delete files, then commits Iceberg snapshots through an embedded catalog."
  sourceLabel="Mermaid source for the materialization diagram"
/>

## Build strategy

<MermaidDiagram
  chart={materializerDecisionMermaid}
  size="compact"
  caption="The proposed child-1 path is hybrid: adopt the Iceberg writer stack, build the delete-aware commit layer, and keep the feature isolated."
  sourceLabel="Mermaid source for the materializer build strategy"
/>

| Decision | Status | Rationale |
|----------|--------|-----------|
| Consume delta envelopes, not raw SSTables | Accepted design | It reuses the tombstone-faithful extraction path and keeps materialization format-agnostic. |
| HYBRID build/adopt path | Accepted direction | iceberg-rust supplies writers and metadata blocks, but not delete-file snapshot commits. |
| Embedded SQLite-backed catalog for child 1 | Accepted direction | It is offline-friendly and keeps REST catalog work out of the first implementation. |
| Feature-isolated Arrow 57 stack | Accepted direction | It avoids forcing a workspace-wide Arrow upgrade. |
| Continuous daemon, RF dedup, repair gating | Deferred | These belong to follow-up children after single-invocation materialization works. |

## Research sources

<CardGrid>
  <LinkCard
    title="Materializer proposal"
    description="The OpenSpec change describing why delta envelopes should become Iceberg tables."
    href="https://github.com/pmcfadin/cqlite/blob/main/docs/storage%20engine/proposal.md"
  />
  <LinkCard
    title="Materializer design"
    description="The design decisions for fold semantics, catalog choice, commit protocol, and feature isolation."
    href="https://github.com/pmcfadin/cqlite/blob/main/docs/storage%20engine/design.md"
  />
  <LinkCard
    title="Iceberg build-vs-adopt memo"
    description="The research-backed verdict for adopting iceberg-rust while building the missing commit layer."
    href="https://github.com/pmcfadin/cqlite/blob/main/docs/storage%20engine/iceberg-oq1-build-vs-adopt.md"
  />
</CardGrid>

Primary source paths:

- `docs/storage engine/proposal.md`
- `docs/storage engine/design.md`
- `docs/storage engine/spec.md`
- `docs/storage engine/tasks.md`
- `docs/storage engine/epic-draft.md`
- `docs/storage engine/iceberg-oq1-build-vs-adopt.md`
- `docs/storage engine/cassandra-index/research-iceberg-oq1.md`
