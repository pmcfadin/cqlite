---
title: Cassandra Seam Inventory
description: "The Cassandra seam map behind CQLite's adjacent analytical-plane direction."
sidebar:
  label: Cassandra Seams
  order: 4
tableOfContents: false
---

import { Card, CardGrid, LinkCard } from '@astrojs/starlight/components';
import MermaidDiagram from '../../../components/MermaidDiagram.astro';

export const seamsMermaid = `flowchart TB
  Open["Usable seams<br/>CEP-11 memtable plugin<br/>SSTable format / components<br/>Compaction strategy<br/>Sidecar discovery + snapshots<br/>CQLite Flight / Trino sidecar"]
  Closed["Hard couplings<br/>CommitLog singleton<br/>Read merge + View snapshot<br/>CFS lifecycle / Tracker<br/>Streaming + repair"]
  Adjacent["Adjacent OLAP plane"]
  Boundary["Do not claim replacement engine"]
  CQLite["CQLite reads SSTables and exported tails"]

  Open --> Adjacent --> CQLite
  Closed --> Boundary --> CQLite

  classDef open fill:#eef5ff,stroke:#2f6fed,color:#23304a;
  classDef closed fill:#fff4ed,stroke:#d56b1f,color:#3b2a1f;
  class Open,Adjacent,CQLite open;
  class Closed,Boundary closed;`;

export const seamsFlowMermaid = `flowchart TB
  WritePath["Cassandra OLTP path<br/>Mutation apply -> CommitLog -> Memtable -> flush lifecycle"]
  SSTables["SSTable components"]
  Tail["Tail gen-* SSTables<br/>export spike"]
  CQLite["CQLite analytical reader"]
  Consumers["Arrow Flight / Trino<br/>Iceberg materializer"]
  Internal["Cassandra local read path<br/>internal memtable + SSTable merge"]

  WritePath --> SSTables --> CQLite --> Consumers
  WritePath -. export spike .-> Tail
  Tail --> CQLite
  WritePath -. internal only .-> Internal`;

# Cassandra Seam Inventory

**Status:** Research-backed guidance.

The seam inventory is the evidence behind the storage-engine direction. Cassandra
has useful storage-related extension points, but not one clean `StorageEngine`
interface. CQLite should use the seams that are already safe - SSTable files,
sidecar discovery, Flight/Trino, and targeted tail export - instead of presenting
itself as a drop-in Cassandra engine replacement.

## Public summary

<CardGrid>
  <Card title="Some seams are real" icon="puzzle">
    Memtable plugins, SSTable format APIs, compaction strategies, and Sidecar
    discovery are meaningful integration points.
  </Card>
  <Card title="The read/lifecycle core is closed" icon="lock">
    Read merge, `Tracker`/`View`, repair, streaming, and commit log lifecycle
    remain Cassandra internals rather than replaceable engine contracts.
  </Card>
  <Card title="Adjacent wins" icon="rocket">
    The public architecture should show CQLite beside Cassandra, reading shared
    storage and exported tails for analytics.
  </Card>
</CardGrid>

## Seam map

<MermaidDiagram
  chart={seamsMermaid}
  size="compact"
  caption="CQLite should build on usable Cassandra seams while avoiding replacement-engine claims around hardwired internals."
  sourceLabel="Mermaid source for the seam map"
/>

## Data ownership flow

<MermaidDiagram
  chart={seamsFlowMermaid}
  size="compact"
  caption="Cassandra owns mutation apply, commit log, memtables, and lifecycle. CQLite enters after durable SSTables or explicit tail export."
  sourceLabel="Mermaid source for the ownership flow"
/>

| Finding | Public interpretation |
|---------|-----------------------|
| Cassandra has no single storage-engine interface | Avoid replacement-engine language. |
| Memtable and SSTable format APIs are useful but bounded | Use them only for targeted integration spikes. |
| Read merge and lifecycle are hardwired | Keep CQLite outside the OLTP read and repair path. |
| Flight/Trino already form the adjacent read plane | Promote the sidecar model as the project direction. |
| Tail export is a narrow bridge | Treat it as freshness work, not a new Cassandra engine. |

## Research sources

<CardGrid>
  <LinkCard
    title="Storage-engine feasibility"
    description="The synthesized verdict for adjacent CQLite versus replacement-engine CQLite."
    href="https://github.com/pmcfadin/cqlite/blob/main/docs/storage%20engine/report-2-storage-engine-feasibility.md"
  />
  <LinkCard
    title="Write path index"
    description="The mutation, commit log, memtable, and flush lifecycle analysis."
    href="https://github.com/pmcfadin/cqlite/blob/main/docs/storage%20engine/cassandra-index/write-path.md"
  />
  <LinkCard
    title="Read path index"
    description="The read merge and View snapshot analysis that limits replacement-engine claims."
    href="https://github.com/pmcfadin/cqlite/blob/main/docs/storage%20engine/cassandra-index/read-path.md"
  />
</CardGrid>

Primary source paths:

- `docs/storage engine/report-2-storage-engine-feasibility.md`
- `docs/storage engine/cassandra-index/write-path.md`
- `docs/storage engine/cassandra-index/read-path.md`
- `docs/storage engine/cassandra-index/config-pluggability-inventory.md`
- `docs/storage engine/cassandra-index/cfs-flush-lifecycle.md`
- `docs/storage engine/cassandra-index/compaction.md`
- `docs/storage engine/cassandra-index/streaming-repair.md`
- `docs/storage engine/cassandra-index/cqlite-flight-trino.md`
