---
title: "For Agents: Using CQLite"
description: Task-oriented recipes for AI agents integrating with or automating CQLite.
sidebar:
  label: Overview
  order: 0
---

# For Agents: Using CQLite

This section provides terse, copy-pasteable, machine-verifiable recipes for AI
agents that integrate with CQLite — as a CLI tool, Rust library, Python package,
or Node.js module.

> **Content arriving in W6.** This placeholder marks the section structure.
> Full recipe pages — each covering one task with exact commands, expected output
> shapes, exit codes, and failure modes — will be published as part of issue W6
> in epic #733.

## What you'll find here (W6 onwards)

Each page covers exactly one task:

- **Open a database and run a SELECT** — CLI and all binding APIs
- **Filter with WHERE clauses** — supported predicates and operators
- **Export to JSON/CSV/Parquet** — `--out` flag usage and output shapes
- **Read collections** — lists, sets, maps in all bindings
- **Read UDTs** — user-defined types
- **Streaming large result sets** — Node.js and Python streaming APIs
- **Write mutations** — INSERT/UPDATE via write support
- **Error codes and recovery** — all error codes, categories, and retry patterns
- **Verify output against sstabledump** — parity validation workflow
- **Troubleshooting** — common failures and fixes

## Design principles

Every recipe in this section is:

1. **Verifiable** — commands run against the real test datasets
2. **Complete** — includes setup, execution, expected output, and teardown
3. **Terse** — no prose that agents must skip over; code first
4. **Honest about limitations** — documents what doesn't work and why

## Error code reference

The following error codes are thrown by CQLite bindings (full table in W6):

| Code | Category | Description |
|------|----------|-------------|
| `IO` | System | I/O errors |
| `SCHEMA` | Schema | Schema/table errors |
| `QUERY` | Query | Query execution errors |
| `PARSE` | Data | Binary format parsing errors |
| `CONFIG` | Configuration | Configuration errors |
| `STORAGE` | Storage | Storage engine errors |
| `NOT_FOUND` | NotFound | Resource not found |
| `INVALID_INPUT` | Logic | Invalid operation or state |
