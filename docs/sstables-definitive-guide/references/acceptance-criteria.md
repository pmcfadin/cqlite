# Acceptance Criteria — SSTables: The Definitive Guide

This document lists the per-chapter acceptance bullets used for drafting and review. It complements the embedded criteria in `OUTLINE.md` and the conventions in `STYLE_GUIDE.md`.

## Part I — Foundations

### 1. What Are SSTables?
- Explains LSM, memtables, WAL, and immutability in relation to SSTables
- Includes 1+ Cassandra 5.0.0 permalink
- Shows at least one trimmed example using `test-data/datasets/test_basic`
- Ends with Key Takeaways and pinned References

### 2. Anatomy of an SSTable
- Describes component roles and TOC invariants; includes `.mmd` component diagram
- Pins ≥2 Cassandra 5.0.0 links (e.g., `Descriptor`, `StatsMetadata`, `IndexSummary`)
- Demonstrates directory naming with a tiny real listing from `test_basic` (trimmed)
- Notes 3.x/4.x differences in a sidebar when material

### 3. Disk and IO Model
- Explains chunking and checksum flow with a small `CompressionInfo.db` example or summary
- Compares mmapped vs buffered IO trade-offs; references `CompressionMetadata` and related classes
- Includes ≥1 Cassandra 5.0.0 permalink
- Key Takeaways cover random vs sequential impacts

## Part II — The Write Path in Detail

### 4. From CQL to Disk
- `.mmd` flush pipeline diagram and 5–10 line pseudocode
- Pins ≥2 Cassandra 5.0.0 links (`SSTableWriter`, memtable classes)
- Tiny `sstabledump` or log excerpt from `test_basic` (trimmed)
- Sidebar for version differences if applicable

### 5. Data.db Format
- Minimal annotated row layout example from `test_basic` (trimmed)
- Short encoding snippet (vint/varint or cell flags) + ≥1 Cassandra link
- Optionally cross-link to Appendix C for implementation walkthroughs
- Deletion/TTL semantics summarized succinctly

### 6. Index.db and Summary.db
- Explains index entry structure and summary sampling; one trimmed `sstabledump` excerpt
- Pins ≥2 Cassandra links (e.g., `IndexSummary`)
- Notes promoted index behavior; binary search path
- Takeaways on latency vs memory trade-offs

### 7. Filter.db (Bloom)
- Describes Bloom parameters and expected FPR; references Cassandra `BloomFilter`
- Shows short-circuit behavior; contrasts with promoted index
- Pins ≥1 Cassandra link
- Includes one small numeric FPR example

### 8. Statistics.db
- Identifies key `StatsMetadata` fields with a tiny printed subset (trimmed)
- Pins ≥1 Cassandra link
- Explains compaction/read heuristic interactions
- Ends with Takeaways

### 9. CompressionInfo.db and Chunking
- Presents a small chunk map example (trimmed) and checksum note
- Pins ≥1 Cassandra link (`CompressionMetadata`)
- States chunk size guidance for random vs scan
- References related diagram (Ch. 4 or 10)

## Part III — The Read Path in Detail

### 10. Point Reads and Slices
- `.mmd` decision tree diagram for Bloom→Index→Summary→Data
- Pins ≥2 Cassandra links
- Minimal trimmed example illustrating the flow on `test_basic`
- Summarizes point vs slice behaviors

### 11. Merging, Tombstones, and Shadowing
- `.mmd` tombstone timeline diagram with caption
- Pins ≥2 Cassandra links (rows/tombstone classes)
- Tiny example showing shadowing across two SSTables (trimmed)
- Bullet list of reconciliation rules

### 12. Caching and OS Interaction
- Clarifies historical caches vs current realities; links to 5.0 docs/code
- Short comparison of mmapped vs buffered vs async (bullets/table)
- Pins ≥1 Cassandra link
- Practical defaults in Takeaways

## Part IV — Indexing

### 13. Secondary Indexes (2i)
- Describes 2i storage artifacts and query flow at a high level
- Pins ≥1 Cassandra link
- Tiny example demonstrating filtering behavior
- Succinct limitations/trade-offs

### 14. Storage-Attached Indexes (SAI)
- `.mmd` diagrams for SAI file layout and query flow; vector coverage required
- Pins ≥2 Cassandra links under `index.sai.*`
- Small example for one numeric/text and one vector query (trimmed)
- Lifecycle interactions with SSTables summarized

## Part V — Compaction and Lifecycle

### 15. Compaction Strategies
- Comparison table for STCS/LCS/TWCS; UCS in sidebar
- Pins ≥2 Cassandra links
- Tombstone purging rules and overlap implications
- Takeaways on when to use which strategy

### 16. SSTable Lifecycle and Maintenance
- Lists lifecycle ops with one trimmed tool output
- Pins ≥2 Cassandra links and references TOC invariants
- Anticorruption component checklist
- Cross-links to Ch. 18

## Part VI — Advanced Topics

### 17. BTI (B-Tree/Trie Indexed) Formats
- Contrasts BTI with big/mc/mm using 3–5 bullets
- Pins ≥2 Cassandra BTI links
- Tiny figure/example of index layout differences
- Read amplification impact summarized

### 18. Repair, Streaming, and Bootstrap (Overview)
- High-level processes and artifacts; not a deep ops guide
- Pins ≥1 Cassandra link and cross-links to related chapters
- Minimal sequence-of-steps figure or bullets
- Takeaways on when these occur

### 19. Incremental Backups and Snapshots
- Tiny directory listing for snapshot/backup layout (trimmed)
- Pins ≥1 Cassandra link
- Brief restore considerations and pitfalls
- References section present

### 20. Checksums and Integrity
- Demonstrates checksum verification with a tiny example (trimmed)
- Pins ≥1 Cassandra link
- Explains `Digest.crc32` and component checksum interactions
- Ends with Takeaways

## Part VII — Developer’s Appendix

### A. CQL→SSTable Type Mapping
- Mapping tables under `tables/` and referenced in text
- Pins ≥1 Cassandra link per major type group where relevant
- Cross-references schema encoding chapters
- Validated against `test_basic`

### B. On-Disk Encodings Cheat Sheet
- Concise tables/bullets for encodings with ≥1 pinned link
- ≤1 page; highly scannable

### C. Reference Walkthroughs with Code
- At least one end-to-end `Data.db` read walkthrough
- Pinned links for Cassandra; cross-link here from core chapters for implementation details
- Trimmed `sstabledump` excerpts where helpful

### D. Tools & Workflows
- Minimal trimmed command outputs with captions
- Safety notes and references to lifecycle chapter
- Pinned links to `org.apache.cassandra.tools`

### E. Glossary
- Defines all introduced terms; cross-links back to first use
- Concise entries (1–2 lines each)
- References included when semantics mirror Cassandra source


