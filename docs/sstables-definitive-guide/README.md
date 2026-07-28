# SSTables: The Definitive Guide

This directory contains the evolving manuscript for our SSTables reference in the style of an O'Reilly "Definitive Guide". Start with `OUTLINE.md`.

- `OUTLINE.md`: chapter plan and structure
- `REFERENCES.md`: authoritative code links and reading list
- `OPEN_QUESTIONS.md`: scope decisions and unresolved items
- `diagrams/`: source diagrams (Mermaid/SVG)
- `tables/`: tabular references (CSV/MD)
- `references/`: cached citations and permalinks

Contributions: keep chapters under ~300-600 lines; split when larger.

## Adding a new chapter or appendix — it is NOT published until registered

Creating a file under `chapters/` does **not** publish it to the docs site. The published set is the
hardcoded `CHAPTERS` array in `website/scripts/sync-format-guide.mjs`; a chapter absent from it is
simply never synced. Four appendices were silently unpublished this way (issue #3006).

To publish a new chapter or appendix:

1. Add a `{ file, order, prefix }` entry to the `CHAPTERS` array in
   `website/scripts/sync-format-guide.mjs`.
2. Add it to the chapter/appendix list below, so the guide's own front door matches the site.

If a file must deliberately stay off the site, register it in that script's
`UNPUBLISHED_BY_DESIGN` map with a reason instead.

Either way you will be told: the sync script runs as the website `prebuild` and **exits 1** on an
unregistered chapter (as well as on a stale exclusion, a doubly-listed file, a missing source, or a
duplicate prefix), so an unregistered chapter now fails the build rather than vanishing quietly. Do
not work around the guard — register the chapter.

## Chapters

- 01 — [What Are SSTables?](chapters/01-what-are-sstables.md)
- 02 — [Anatomy of an SSTable](chapters/02-anatomy-of-an-sstable.md)
- 03 — [Disk and IO Model](chapters/03-disk-and-io-model.md)
- 04 — [From CQL to Disk](chapters/04-from-cql-to-disk.md)
- 05 — [Data.db Format](chapters/05-data-db-format.md)
- 06 — [Index.db and Summary.db](chapters/06-index-and-summary.md)
- 07 — [Bloom Filter](chapters/07-bloom-filter.md)
- 08 — [Statistics.db](chapters/08-statistics-db.md)
- 09 — [CompressionInfo.db and Chunking](chapters/09-compressioninfo-and-chunking.md)
- 10 — [Point Reads and Slices](chapters/10-point-reads-and-slices.md)
- 11 — [Merging, Tombstones, and Shadowing](chapters/11-merging-tombstones-and-shadowing.md)
- 12 — [Caching and OS Interaction](chapters/12-caching-and-os-interaction.md)
- 13 — [Secondary Indexes (2i)](chapters/13-secondary-indexes.md)
- 14 — [Storage-Attached Indexes (SAI)](chapters/14-storage-attached-indexes-sai.md)
- 15 — [Compaction Strategies](chapters/15-compaction-strategies.md)
- 16 — [SSTable Lifecycle and Maintenance](chapters/16-sstable-lifecycle-and-maintenance.md)
- 17 — [BTI Formats](chapters/17-bti-formats.md)
- 18 — [Repair, Streaming, and Bootstrap](chapters/18-repair-streaming-bootstrap.md)
- 19 — [Incremental Backups and Snapshots](chapters/19-incremental-backups-and-snapshots.md)
- 20 — [Checksums and Integrity](chapters/20-checksums-and-integrity.md)

## Appendices

- Appendix A — [CQL→SSTable Type Mapping](chapters/appendix-a-type-mapping.md)
- Appendix B — [On-Disk Encodings Cheat Sheet](chapters/appendix-b-encodings-cheat-sheet.md)
- Appendix C — [Reference Walkthroughs with Code](chapters/appendix-c-walkthroughs.md)
- Appendix D — [Tools & Workflows](chapters/appendix-d-tools-and-workflows.md)
- Appendix E — [Glossary](chapters/appendix-e-glossary.md)
- Appendix F — [Known Limitations](chapters/appendix-f-known-limitations.md)
- Appendix G — [Cassandra 5.0 Compression Chunk Formats](chapters/appendix-g-compression-chunk-formats.md)

