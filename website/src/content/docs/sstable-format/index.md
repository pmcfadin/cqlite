---
title: SSTable Format Guide
description: 22-chapter deep dive into the Cassandra 5.0 SSTable binary format.
sidebar:
  label: Overview
  order: 0
---

# SSTable Format Guide

This section publishes CQLite's definitive guide to the Apache Cassandra 5.0
SSTable binary format — 22 chapters plus appendices, audited against
Cassandra 5.0.8.

> **Content arriving in W2.** This placeholder marks the section structure.
> The full 22-chapter guide (currently in `docs/sstables-definitive-guide/`)
> will be published as part of issue W2 in epic #733.

## What you'll find here (W2 onwards)

- **Chapter 1–4**: SSTable overview, file components, versioning
- **Chapter 5**: Data.db format — rows, flags, V5CompressedLegacy
- **Chapter 6**: Index.db and Summary.db — partition lookups
- **Chapters 7–16**: Collections, UDTs, tombstones, bloom filters, statistics, compression
- **Chapter 17**: BTI formats — trie indexes
- **Chapters 18–22**: Advanced topics
- **Appendix B**: Encoding cheat sheet — VInt, flags
- **Appendix F**: Known limitations

## Source

The format guide source lives in `docs/sstables-definitive-guide/` in the
[CQLite repository](https://github.com/pmcfadin/cqlite). It is the single source
of truth for Cassandra 5.0 SSTable format documentation in this project.
