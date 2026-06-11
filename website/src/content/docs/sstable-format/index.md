---
title: "SSTable Format Guide"
description: "22-chapter deep dive into the Cassandra 5.0 SSTable binary format, audited against Cassandra 5.0.8."
sidebar:
  label: "Overview"
  order: 0
---

# SSTable Format Guide

A 22-chapter reference for the Apache Cassandra 5.0 SSTable binary format, audited against Cassandra 5.0.8 as part of CQLite epic #598.

> Source of truth: [`docs/sstables-definitive-guide/`](https://github.com/pmcfadin/cqlite/tree/main/docs/sstables-definitive-guide/) — pages below are generated at build time by `website/scripts/sync-format-guide.mjs`.

## Chapters

- [What Are SSTables?](/cqlite/sstable-format/ch01/)
- [Anatomy of an SSTable](/cqlite/sstable-format/ch02/)
- [Disk and IO Model](/cqlite/sstable-format/ch03/)
- [From CQL to Disk](/cqlite/sstable-format/ch04/)
- [Data.db Format](/cqlite/sstable-format/ch05/)
- [Index.db and Summary.db](/cqlite/sstable-format/ch06/)
- [Filter.db (Bloom)](/cqlite/sstable-format/ch07/)
- [Statistics.db](/cqlite/sstable-format/ch08/)
- [CompressionInfo.db and Chunking](/cqlite/sstable-format/ch09/)
- [Point Reads and Slices](/cqlite/sstable-format/ch10/)
- [Merging, Tombstones, and Shadowing](/cqlite/sstable-format/ch11/)
- [Caching and OS Interaction](/cqlite/sstable-format/ch12/)
- [Secondary Indexes (2i)](/cqlite/sstable-format/ch13/)
- [Storage-Attached Index (SAI)](/cqlite/sstable-format/ch14/)
- [Compaction Strategies](/cqlite/sstable-format/ch15/)
- [SSTable Lifecycle and Maintenance](/cqlite/sstable-format/ch16/)
- [BTI (Big Trie-Indexed) Formats](/cqlite/sstable-format/ch17/)
- [Repair, Streaming, and Bootstrap (Overview)](/cqlite/sstable-format/ch18/)
- [Example reference listing sourced from canonical datasets (names illustrative)](/cqlite/sstable-format/ch19/)
- [Checksums and Integrity](/cqlite/sstable-format/ch20/)
- [How to find a row by key (flow card)](/cqlite/sstable-format/ch21/)
- [Versioning and Format Matrix (quick reference)](/cqlite/sstable-format/ch22/)

## Appendices

- [Appendix A -- CQL->SSTable Type Mapping](/cqlite/sstable-format/appendix-a/)
- [Appendix B — On-Disk Encodings Cheat Sheet](/cqlite/sstable-format/appendix-b/)
- [Appendix C — Reference Walkthroughs with Code](/cqlite/sstable-format/appendix-c/)
- [Appendix D — Tools & Workflows](/cqlite/sstable-format/appendix-d/)
- [Appendix E — Glossary](/cqlite/sstable-format/appendix-e/)
- [Appendix F — Known Limitations](/cqlite/sstable-format/appendix-f/)
- [Appendix G: Cassandra 5.0 Compression Chunk Formats](/cqlite/sstable-format/appendix-g/)
