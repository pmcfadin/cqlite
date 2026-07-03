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

## Featured proposal

<CardGrid>
  <LinkCard
    title="Storage Engine Direction"
    description="Why CQLite should sit beside Cassandra as an analytical plane over SSTables, instead of replacing Cassandra's internal storage engine."
    href="/cqlite/proposals-research/storage-engine/"
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
