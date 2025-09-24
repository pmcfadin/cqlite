# SSTables: The Definitive Guide (Apache Cassandra)

> Working outline. Modeled after O'Reilly "Definitive Guide" series. Primary scope is Cassandra 5.0; 3.x and 4.x are covered for historical context and via sidebars when significant differences arise.

## Part I — Foundations
1. What Are SSTables?
   - LSM-tree recap, memtables, WAL, immutable files
   - Role of SSTables in Cassandra read/write path
   - Evolution of formats: big → mc/md → mm → oa/ob; BTI (B-Tree/Trie indexed)
   - Directory layout and naming (`{prefix}-{gen}-{format}-{Component}.db`, TOC)
   - Acceptance criteria:
     - Explains LSM, memtables, WAL, and immutability in relation to SSTables
     - Includes 1+ Cassandra 5.0.0 permalink (e.g., `SSTableReader`, `SSTableWriter`, or `Descriptor`)
     - Provides at least one trimmed example using `test-data/datasets/test_basic`
     - Ends with Key Takeaways and pinned References per style guide
2. Anatomy of an SSTable
   - Components: Data.db, Index.db, Summary.db, Filter.db, Statistics.db, CompressionInfo.db, Digest.crc32, TOC.txt
   - Versioning, feature flags, format change log
   - How schema and CQL types map to on-disk encodings
   - Acceptance criteria:
     - Shows component roles and TOC invariants; includes `.mmd` component relationship diagram committed under `diagrams/`
     - Pins at least 2 Cassandra 5.0.0 links (e.g., `Descriptor`, `StatsMetadata`, `IndexSummary`)
     - Demonstrates directory naming with a tiny, real listing from `test_basic` (trimmed)
     - Notes 3.x/4.x differences in a sidebar when materially different
3. Disk and IO Model
   - Block layout, compression chunks, checksums
   - Memory mapping vs buffered IO, page cache effects
   - Bloom filters and negative lookups
   - Acceptance criteria:
     - Explains chunking and checksum flow with a small `CompressionInfo.db` excerpt or summary
     - Compares mmapped vs buffered IO trade-offs; references `CompressionMetadata` and relevant classes
     - Includes 1+ Cassandra 5.0.0 permalink
     - Key Takeaways summarize impacts on random vs sequential reads

## Part II — The Write Path in Detail
4. From CQL to Disk
   - CQL mutation → partition + clustering keys, cells, tombstones
   - Memtable insertions, WAL appends
   - Flush: building Data.db, Index.db, Summary.db, Stats, Filter, CompressionInfo
   - Pseudocode: flush pipeline and component writers
   - Acceptance criteria:
     - Includes `.mmd` flush pipeline diagram (committed) and 5–10 line pseudocode of flush steps
     - Pins 2+ Cassandra 5.0.0 links (e.g., `SSTableWriter`, memtable classes)
     - Shows a tiny `sstabledump` or log excerpt from `test_basic` illustrating a flush artifact (trimmed)
     - Calls out version differences in a sidebar if applicable
5. Data.db Format
   - Partition header, row/cluster layout, cells and cell flags
   - Value encodings, varints/vints, collection/UDT serialization
   - Row deletions, range tombstones, expiring/TTL cells
   - Acceptance criteria:
     - Presents a minimal annotated row layout example from `test_basic` (trimmed)
     - Includes a short encoding snippet (vint/varint or cell flags) and pins 1+ Cassandra 5.0.0 link
     - Optionally cross-link to Appendix C for implementation walkthroughs
     - Ends with a concise table or bullets summarizing deletion/TTL semantics
6. Index.db and Summary.db
   - Partition key to Data.db position, promoted index
   - Summary sampling and binary search path
   - Token range iteration
   - Acceptance criteria:
     - Explains index entry structure and summary sampling; includes one trimmed `sstabledump` excerpt
     - Pins 2+ Cassandra 5.0.0 links (e.g., `IndexSummary`)
     - Notes how summary guides binary search and when promoted index applies
     - Provides Key Takeaways on latency vs memory trade-offs
7. Filter.db (Bloom)
   - Hashing, false positive rate, configuration
   - Interaction with promoted index and Summary
   - Acceptance criteria:
     - Describes Bloom parameters and expected FPR; references Cassandra `BloomFilter`
     - Shows when Bloom short-circuits reads; contrasts with promoted index
     - Pins 1+ Cassandra 5.0.0 link
     - Includes one small numeric example of FPR impact
8. Statistics.db
   - Histograms, min/max timestamps, sstable-level metadata
   - Repair/level metadata, origins for compaction
   - Acceptance criteria:
     - Identifies key StatsMetadata fields with a tiny printed subset (trimmed)
     - Pins 1+ Cassandra 5.0.0 link (`StatsMetadata`)
     - Explains how stats inform compaction and read heuristics
     - Ends with Key Takeaways
9. CompressionInfo.db and Chunking
   - Algorithms, chunk sizes, offsets map, checksums
   - Impact on random reads and scans
   - Acceptance criteria:
     - Presents a small chunk map example (trimmed) and checksum note
     - Pins 1+ Cassandra 5.0.0 link (`CompressionMetadata`)
     - States guidance on chunk size effects (random vs scan)
     - References related diagram from Ch. 4 or 10 as applicable

## Part III — The Read Path in Detail
10. Point Reads and Slices
    - Key lookup flow: Bloom → Index → Summary → Data read
    - Slice/partition range reads; short reads vs wide partitions
    - Read-before-write, read repair context (not executed by SSTable but affects patterns)
    - Acceptance criteria:
      - Includes `.mmd` read-path decision tree diagram (committed)
      - Pins 2+ Cassandra 5.0.0 links (read command classes, `IndexSummary`)
      - Shows a minimal, trimmed example illustrating Bloom→Index→Summary flow on `test_basic`
      - Summarizes behavior for point vs slice reads
11. Merging, Tombstones, and Shadowing
    - Tombstone semantics: partition/row/cell, range tombstones
    - Shadowing across SSTables; reconciliation rules
    - TTL expiry and gc_grace_seconds
    - Acceptance criteria:
      - Provides a `.mmd` tombstone timeline diagram with captions
      - Pins 2+ Cassandra 5.0.0 links (rows/tombstone classes)
      - Includes a tiny example showing shadowing across two SSTables (trimmed)
      - Lists reconciliation rules as concise bullets
12. Caching and OS Interaction
    - Key cache/row cache (historical), page cache realities
    - Read-ahead, direct IO, mmapped vs async
    - Acceptance criteria:
      - Clarifies historical caches vs current realities; links to 5.0 docs/code
      - Presents a short comparison (bullets or small table) of mmapped vs buffered vs async
      - Pins 1+ Cassandra 5.0.0 link
      - Ends with Key Takeaways focusing on practical defaults

## Part IV — Indexing
13. Secondary Indexes (2i)
    - Per-table local indexes, storage layout and query flow
    - Read/write path impact; coordination and filtering
    - Acceptance criteria:
      - Describes 2i storage artifacts and query flow at a high level
      - Pins 1+ Cassandra 5.0.0 link
      - Provides a tiny example demonstrating filtering behavior
      - Calls out limitations and trade-offs succinctly
14. Storage-Attached Indexes (SAI)
    - SAI data structures (posting lists, terms, numeric trees, vector segments)
    - File layout and lifecycle with SSTables
    - Read path integration; range, LIKE, and vector similarity queries
    - Acceptance criteria:
      - Includes `.mmd` diagrams for SAI file layout and query flow (vector coverage required)
      - Pins 2+ Cassandra 5.0.0 links under `index.sai.*`
      - Shows a small illustrative example (trimmed) for one numeric/text and one vector query
      - Summarizes lifecycle interactions with SSTables

## Part V — Compaction and Lifecycle
15. Compaction Strategies
    - STCS, LCS, TWCS; UCS (sidebar)
    - Tombstone purging, sstable density and overlap
    - Acceptance criteria:
      - Provides a comparison table for STCS/LCS/TWCS; UCS covered in a sidebar
      - Pins 2+ Cassandra 5.0.0 links
      - Articulates tombstone purging rules and overlap implications
      - Ends with Key Takeaways on when to use each strategy
16. SSTable Lifecycle and Maintenance
    - Scrub, verify, upgrade, level compaction metadata
    - Orphaned components, TOC invariants, anticorruption checks
    - Repair and streaming (overview placement; see Ch. 18)
    - Acceptance criteria:
      - Lists lifecycle operations with one trimmed tool output (e.g., `sstablemetadata`)
      - Pins 2+ Cassandra 5.0.0 links and references TOC invariants
      - Provides an anticorruption checklist for components
      - Cross-links to Ch. 18 for repair/streaming

## Part VI — Advanced Topics
17. BTI (B-Tree/Trie Indexed) Formats
    - Motivation, structure, differences from big/mc/mm
    - Impact on read amplification, index layout
    - Acceptance criteria:
      - Contrasts BTI with big/mc/mm using 3–5 bullets
      - Pins 2+ Cassandra 5.0.0 BTI links
      - Includes a tiny example or figure illustrating index layout differences
      - Summarizes impact on read amplification
18. Repair, Streaming, and Bootstrap (Overview)
    - Why they exist; how SSTables are shipped between nodes
    - Relationship to read/write paths already covered
    - Acceptance criteria:
      - Outlines processes and artifacts at a high level; no deep ops guide
      - Pins 1+ Cassandra 5.0.0 link and cross-links to relevant chapters
      - Provides a minimal sequence-of-steps figure or bullets
      - Ends with Key Takeaways on when these occur
19. Incremental Backups and Snapshots
    - Hardlinks, directory structure, restore considerations
    - Acceptance criteria:
      - Shows a tiny directory listing illustrating snapshot/backup layout (trimmed)
      - Pins 1+ Cassandra 5.0.0 link
      - Notes restore considerations and pitfalls briefly
      - Includes References section
20. Checksums and Integrity
    - CRC32 and chunk checksums
    - Digest files and verification workflows
    - Acceptance criteria:
      - Demonstrates checksum verification with a tiny example (trimmed)
      - Pins 1+ Cassandra 5.0.0 link
      - Explains Digest.crc32 role and interactions with component checksums
      - Ends with Key Takeaways

## Part VII — Developer’s Appendix (CQLite + Cassandra Source)
A. CQL→SSTable Type Mapping
   - Table mapping for primitive/collection/UDT types
   - Acceptance criteria:
     - Adds mapping tables under `tables/` and references them
     - Pins 1+ Cassandra 5.0.0 link per major type group where relevant
     - Cross-references schema encoding chapters
     - Validated against `test_basic` examples
B. On-Disk Encodings Cheat Sheet
   - vint/varint, header bits, cell flags
   - Acceptance criteria:
     - Provides concise tables/bullets for encodings with 1+ pinned link
     - Optionally cross-link to Appendix C for implementation walkthroughs
     - Keeps under 1 page of content; highly scannable
C. Reference Walkthroughs with Code
   - Cassandra classes and paths to study for each component
   - CQLite modules and tests exercising those paths
   - Acceptance criteria:
     - Includes at least one end-to-end walkthrough for `Data.db` read
     - Uses pinned links for Cassandra 5.0.0; optionally cross-link to Appendix C for implementation
     - Trimmed outputs from `sstabledump` where helpful
D. Tools & Workflows
   - sstabledump, sstablemetadata, sstablescrub; example invocations
   - Acceptance criteria:
     - Shows minimal, trimmed command outputs with captions
     - Includes safety notes and references to lifecycle chapter
     - Pinned links to tool sources under `org.apache.cassandra.tools`
E. Glossary
   - Terms and abbreviations
   - Acceptance criteria:
     - Defines all chapter-introduced terms; cross-links back to first use
     - Keeps entries concise (1–2 lines each)
     - Contains references where definitions reflect Cassandra source semantics

---

Planned Diagrams & Tables
- SSTable component relationship diagram (Data/Index/Summary/Filter/Stats/CompressionInfo/TOC)
- Write flush pipeline (steps and artifacts)
- Read path decision tree (Bloom→Index→Summary→Data)
- Tombstone timeline and shadowing examples
- Compaction strategy comparison table (STCS/LCS/TWCS; UCS sidebar)
- Type mapping tables (CQL→on-disk)
- SAI file layout and query flow diagrams (incl. vector)
- Diagrams authored in Mermaid; commit `.mmd` sources. SVG export optional later.

Scope & Version Notes
- Primary: Cassandra 5.0; sidebars call out significant 3.x/4.x differences
- Include BTI as a dedicated chapter

Contribution Guidance
- Keep chapters < ~500 lines each (split when needed)
- Include minimal runnable examples or `sstabledump` snippets
- Cite Cassandra source (class/package + permalink)
- Use the `test_basic` dataset from `test-data/` for canonical examples
