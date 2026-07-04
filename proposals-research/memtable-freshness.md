---
title: Memtable Freshness
description: "CQLite's proposed path for making analytical reads include Cassandra's unflushed memtable tail without replacing Cassandra's write path."
sidebar:
  label: Memtable Freshness
  order: 2
tableOfContents: false
---

import { Card, CardGrid, LinkCard } from '@astrojs/starlight/components';
import MermaidDiagram from '../../../components/MermaidDiagram.astro';

export const freshnessMermaid = `flowchart TB
  Write["Application write"] --> Cassandra["Cassandra commit log + memtable"]
  Cassandra --> Flush["Flush / compaction"]
  Flush --> SSTables["Durable SSTables"]
  SSTables --> CQLite["CQLite SSTable reader"]
  CQLite --> Query["Flight / Trino analytical read"]

  Cassandra -. current gap .-> Invisible["Unflushed tail is not a file"]
  Cassandra -. proposed export .-> Tail["Tail gen-* SSTables"]
  Tail --> Merge["Existing CQLite k-way LWW merge"]
  SSTables --> Merge
  Merge --> FreshQuery["Fresh analytical read"]`;

export const freshnessDecisionMermaid = `flowchart LR
  subgraph Use["Use now / spike"]
    direction TB
    Correct["Flush -> snapshot -> read"]
    Spike["CEP-11 tail export"]
    Correct --> Spike
  end

  subgraph Hold["Hold or avoid"]
    direction TB
    Fallback["CDC / commitlog tail fallback"]
    Avoid["Avoid StorageHook, JVM agent, blind flush"]
    Fallback --> Avoid
  end

  Spike --> Fallback`;

# Memtable Freshness

**Status:** Proposed spike.

Analytical reads are already correct for flushed SSTables. The freshness gap is
the unflushed Cassandra memtable tail: CQLite discovers files, and a live
memtable is not a file. The preferred spike exports the tail as ordinary SSTable
generations so the existing CQLite merge path can read it without a second
correctness model.

## Public summary

<CardGrid>
  <Card title="The merge engine is not the gap" icon="puzzle">
    CQLite already performs the k-way last-write-wins merge over SSTables. The
    missing piece is a trustworthy source for live Cassandra writes before
    flush.
  </Card>
  <Card title="Materialize the tail" icon="document">
    Exporting a live tail as real `gen-*` SSTable components keeps the
    integration path-based and reuses the reader, filters, and Arrow pipeline.
  </Card>
  <Card title="Keep Cassandra in charge" icon="sync">
    Cassandra still owns commit log durability, memtable lifecycle, flush, and
    compaction. CQLite only reads the durable foundation plus the exported tail.
  </Card>
</CardGrid>

## Freshness diagram

<MermaidDiagram
  chart={freshnessMermaid}
  size="compact"
  caption="Freshness becomes a source-enumeration problem: include exported tail generations alongside flushed SSTables, then let the existing merge path reconcile them."
  sourceLabel="Mermaid source for the freshness diagram"
/>

## Decision map

<MermaidDiagram
  chart={freshnessDecisionMermaid}
  size="compact"
  caption="The preferred spike is a CEP-11 memtable export path that produces normal SSTable generations. CDC remains a useful fallback, while StorageHook and forced-flush postures should not steer the first implementation."
  sourceLabel="Mermaid source for the freshness decision map"
/>

| Decision | Status | Rationale |
|----------|--------|-----------|
| Export live tail as SSTable generations | Proposed spike | It preserves CQLite's existing reader and merge semantics. |
| Flush then snapshot | Accepted baseline | It is correct today, but freshness is bounded by flush timing. |
| CDC or commitlog tail into CQLite | Deferred fallback | It avoids in-JVM code but accepts bounded lag and CDC operational cost. |
| StorageHook as the freshness seam | Rejected for now | It wraps per-SSTable iterator creation and cannot see live memtables. |
| Tiny memtables or blind flush cadence | Rejected for now | It trades OLTP health for analytics freshness. |

## Research sources

<CardGrid>
  <LinkCard
    title="Memtable freshness report"
    description="The option analysis for exposing Cassandra's unflushed tail to CQLite analytical reads."
    href="https://github.com/pmcfadin/cqlite/blob/main/docs/storage%20engine/report-1-memtable-freshness.md"
  />
  <LinkCard
    title="CEP-11 plugin design"
    description="The spike design for exporting a Cassandra memtable tail as real SSTable generations."
    href="https://github.com/pmcfadin/cqlite/blob/main/docs/storage%20engine/memtable-plugin-design.md"
  />
  <LinkCard
    title="CQLite tail seam map"
    description="The CQLite-side inventory for adding a tail directory without rewriting the merge engine."
    href="https://github.com/pmcfadin/cqlite/blob/main/docs/storage%20engine/cassandra-index/research-cqlite-tail-seam.md"
  />
</CardGrid>

Primary source paths:

- `docs/storage engine/report-1-memtable-freshness.md`
- `docs/storage engine/memtable-plugin-design.md`
- `docs/storage engine/cassandra-index/research-cqlite-tail-seam.md`
- `docs/storage engine/cassandra-index/research-export-format.md`
- `docs/storage engine/cassandra-index/memtable-api.md`
