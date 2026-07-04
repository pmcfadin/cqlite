---
title: Compaction & Maintenance
description: "CQLite's documented direction for standalone compaction, deterministic compaction inputs, and maintenance scheduling."
sidebar:
  label: Compaction & Maintenance
  order: 6
tableOfContents: false
---

import { Card, CardGrid, LinkCard } from '@astrojs/starlight/components';
import MermaidDiagram from '../../../components/MermaidDiagram.astro';

export const compactionMermaid = `flowchart LR
  Snapshot["Offline snapshot / CQLite-native table"]
  Planner["Pure compaction planner"]
  Executor["K-way merge executor"]
  Manager["Standalone manager"]
  Output["Valid Cassandra 5 SSTables"]

  Snapshot --> Planner --> Executor --> Output
  Planner --> Manager
  Manager --> Executor`;

export const deterministicInputsMermaid = `flowchart LR
  Schema["schema.cql gc_grace"]
  GcBefore["explicit gc-before"]
  NowSec["explicit now-sec"]
  Merge["deterministic merge"]
  Tests["parity harness"]

  Schema --> Merge
  GcBefore --> Merge
  NowSec --> Merge
  Merge --> Tests`;

# Compaction & Maintenance

**Status:** Proposed direction.

The compaction enhancement issues have enough design depth to stand as public
proposal pages. The direction is an offline, deterministic maintenance plane:
Cassandra still owns live data directories, while CQLite can compact snapshots,
backups, and CQLite-native table directories with explicit inputs and testable
parity.

## Public summary

<CardGrid>
  <Card title="Standalone manager" icon="sync">
    The manager schedules compaction work over snapshots, backups, or
    CQLite-native tables without modifying live Cassandra-owned directories.
  </Card>
  <Card title="Deterministic inputs" icon="document">
    Compaction should receive explicit time inputs such as `gc-before` and
    `now-sec`, instead of depending on ambient wall-clock behavior.
  </Card>
  <Card title="Parity first" icon="puzzle">
    The executor remains a k-way merge path with Cassandra-compatible output and
    byte/semantic parity checks.
  </Card>
</CardGrid>

## Maintenance model

<MermaidDiagram
  chart={compactionMermaid}
  size="compact"
  caption="The documented design separates planning, execution, and long-running management so each layer can be tested independently."
  sourceLabel="Mermaid source for the compaction maintenance model"
/>

## Deterministic merge inputs

<MermaidDiagram
  chart={deterministicInputsMermaid}
  size="compact"
  caption="Compaction correctness depends on explicit schema and time inputs that can be replayed in the parity harness."
  sourceLabel="Mermaid source for deterministic compaction inputs"
/>

| Issue | Coverage | Public interpretation |
|-------|----------|-----------------------|
| [#905](https://github.com/pmcfadin/cqlite/issues/905) compaction manager | Direct | Build a standalone manager around a pure planner and bounded executor. |
| [#1536](https://github.com/pmcfadin/cqlite/issues/1536) `cqlite compact --now-sec` default | Direct | Keep TTL expiry behavior explicit and deterministic for compaction parity. |

## Research sources

<CardGrid>
  <LinkCard
    title="Compaction manager design"
    description="The main design for the standalone planner, executor, manager, crash-safety, and GC policy."
    href="https://github.com/pmcfadin/cqlite/blob/main/docs/compaction-manager-design.md"
  />
  <LinkCard
    title="Compaction parity harness design"
    description="The design trail for explicit gc-before and now-sec inputs in the compact command."
    href="https://github.com/pmcfadin/cqlite/blob/main/docs/plans/2026-06-18-compaction-parity-harness-design.md"
  />
  <LinkCard
    title="Read-path performance audit"
    description="Related audit notes on merge inputs, gc-before, now-sec, and documentation honesty."
    href="https://github.com/pmcfadin/cqlite/blob/main/docs/reports/write-path-performance-audit-2026-07-01.md"
  />
</CardGrid>

Primary issue links:

- [#905 compaction manager](https://github.com/pmcfadin/cqlite/issues/905)
- [#1536 compact now-sec contract](https://github.com/pmcfadin/cqlite/issues/1536)
