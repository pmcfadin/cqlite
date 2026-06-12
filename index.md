---
title: CQLite
description: Local Apache Cassandra SSTable access without cluster dependencies
template: splash
hero:
  tagline: Read Cassandra 5.0 SSTable files locally — no cluster, no JVM, no dependencies.
  actions:
    - text: Get Started
      link: /cqlite/user-docs/
      icon: right-arrow
    - text: View on GitHub
      link: https://github.com/pmcfadin/cqlite
      icon: external
      variant: minimal
---

import { Card, CardGrid, LinkCard } from '@astrojs/starlight/components';

## What is CQLite?

CQLite is a Rust library for reading Apache Cassandra 5.0 SSTable files directly — no Cassandra cluster required. It provides a CQL query interface over local data files, making it ideal for offline analysis, migrations, testing, and backup inspection.

## Key Features

<CardGrid>
  <Card title="No cluster required" icon="rocket">
    Read Cassandra SSTables directly from disk. No JVM, no cluster, no configuration overhead.
  </Card>
  <Card title="CQL query interface" icon="magnifier">
    Execute familiar CQL SELECT queries against local SSTable files.
  </Card>
  <Card title="Multiple bindings" icon="puzzle">
    Available as a Rust library, Python package, and Node.js module.
  </Card>
  <Card title="Multiple output formats" icon="document">
    Export data as JSON, CSV, or Parquet for downstream processing.
  </Card>
</CardGrid>

## Documentation Sections

<CardGrid>
  <LinkCard
    title="User Docs"
    description="Installation, quick start, CLI reference, query guide, Python and Node.js bindings."
    href="/cqlite/user-docs/"
  />
  <LinkCard
    title="SSTable Format Guide"
    description="22-chapter deep dive into the Cassandra 5.0 SSTable binary format."
    href="/cqlite/sstable-format/"
  />
  <LinkCard
    title="For Agents: Using CQLite"
    description="Task-oriented recipes for AI agents integrating with or automating CQLite."
    href="/cqlite/agents-using/"
  />
  <LinkCard
    title="For Agents: Developing CQLite"
    description="Contributor doctrine, gate contracts, and development workflows for AI agents working on CQLite itself."
    href="/cqlite/agents-developing/"
  />
</CardGrid>

## API Reference

Rustdoc for `cqlite-core` is available at [/cqlite/api/latest/](/cqlite/api/latest/).
Per-release API docs are at `/cqlite/api/<version>/`.
