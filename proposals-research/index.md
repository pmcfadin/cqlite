---
title: Proposals and Research
description: Official forward-looking CQLite proposals, design direction, and research-backed architecture notes.
sidebar:
  label: Overview
  order: 0
---

import { Card, CardGrid, LinkCard } from '@astrojs/starlight/components';

# Proposals and Research

This section collects official forward-looking CQLite proposals. It is not a raw
research notebook. Pages here summarize the direction the project is prepared to
stand behind, then link to the deeper source material for readers who want the
full trail.

## Status legend

| Status | Meaning |
|--------|---------|
| Accepted direction | The architectural direction is the project plan, though implementation may still be staged. |
| Proposed spike | The approach is specific enough to evaluate or prototype, but not yet committed as product behavior. |
| Deferred | The idea remains documented, but is not part of the first implementation pass. |
| Rejected for now | The option has enough downside that it should not steer near-term work. |

## Research topics

<CardGrid>
  <LinkCard
    title="Storage Engine Direction"
    description="Why CQLite should sit beside Cassandra as an analytical plane over SSTables, instead of replacing Cassandra's internal storage engine."
    href="/cqlite/proposals-research/storage-engine/"
  />
  <LinkCard
    title="Memtable Freshness"
    description="How the project should close the gap between flushed SSTables and Cassandra's live unflushed tail."
    href="/cqlite/proposals-research/memtable-freshness/"
  />
  <LinkCard
    title="Lakehouse Materialization"
    description="How delta envelopes become queryable Iceberg tables without forcing every consumer to own merge logic."
    href="/cqlite/proposals-research/iceberg-materializer/"
  />
  <LinkCard
    title="Cassandra Seam Inventory"
    description="The seam map behind the adjacent-OLAP decision: what Cassandra lets us plug into, and what remains hardwired."
    href="/cqlite/proposals-research/cassandra-seams/"
  />
  <LinkCard
    title="Read Path & Query Providers"
    description="How the Flight, Trino, DataFusion, and partition-read enhancement issues fit into a single query-provider direction."
    href="/cqlite/proposals-research/read-path-query-providers/"
  />
  <LinkCard
    title="Compaction & Maintenance"
    description="How standalone compaction, explicit gc-before / now-sec inputs, and maintenance scheduling fit together."
    href="/cqlite/proposals-research/compaction-maintenance/"
  />
</CardGrid>

## How to read this section

<CardGrid>
  <Card title="Public first" icon="document">
    Each page leads with the decision, trade-off, and current status before linking
    to raw notes.
  </Card>
  <Card title="Research-backed" icon="magnifier">
    Source reports stay linked so readers can inspect the evidence and the
    discarded paths.
  </Card>
  <Card title="Diagram-led" icon="puzzle">
    Diagrams explain architecture and data movement with hand-authored labels.
    Generated images are used only for non-critical conceptual visuals.
  </Card>
</CardGrid>
