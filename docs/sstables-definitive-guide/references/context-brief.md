# Context Brief — SSTables: The Definitive Guide

This brief captures shared context, assumptions, and open deltas to align all contributors before drafting.

## Scope and Version Baseline
- Primary scope: Apache Cassandra 5.0 (not 5.1). Older versions (3.x/4.x) appear as sidebars when materially different.
- Indexing: Storage-Attached Indexes (SAI) coverage includes vector indexing in 5.0.
- Compaction: STCS/LCS/TWCS covered in main text; UCS is treated as a sidebar.
- Out of scope: encryption at rest, key management, operations-tuning guidance.

## Key Policies
- Pin upstream citations to GitHub permalinks under the `cassandra-5.0.0` tag (or a commit SHA when necessary).
- Embed only short upstream code excerpts (<30 lines) and include a permalink to the full source.
- Diagrams authored in Mermaid (`.mmd`) and committed; SVG export is optional for now.
- Canonical examples must use `test-data/datasets/test_basic`.

## Architecture at a Glance (5.0)
- Multi-file SSTable components stored alongside a TOC:
  - `Data.db`, `Index.db`, `Summary.db`, `Filter.db` (Bloom), `Statistics.db`, `CompressionInfo.db`, `Digest.crc32`, `TOC.txt`.
- File naming pattern: `{prefix}-{generation}-{format}-{Component}.db`; components are distinct files (Index/Bloom not embedded in Data.db).

## Read/Write Path (Essentials)
- Write: CQL mutation → Memtable + WAL → Flush builds per-component files (Data, Index, Summary, Stats, Filter, CompressionInfo).
- Read: Point/range reads typically flow Bloom → Index → Summary → Data block(s); merge logic reconciles tombstones/TTLs across SSTables.

## Data, Examples, and Tools
- Use `sstabledump`/`sstablemetadata` excerpts trimmed for clarity; annotate outputs.
- Validate examples against CQLite readers when possible.

## Style and Structure
- Each chapter includes: summary, "learn" bullets, core content, sidebars/callouts, key takeaways, references.
- Keep chapters ≤ ~500 lines; split where warranted.

## Open Deltas (to resolve early)
- Confirm exact permalink scheme for Cassandra 5.0: prefer `cassandra-5.0.0` tag (naming verification) vs branch/tag variants.
- BTI exemplar dataset: choose/build a small canonical workload to illustrate BTI differences (wide partition and clustering variety?).
- SAI vector coverage: enumerate concrete classes/files to cite (segment writers/readers, query ops) for 5.0.
- Chapter file layout: confirm `chapters/` directory and naming convention (e.g., `01-what-are-sstables.md`).

## Inputs Consulted
- `docs/sstables-definitive-guide/OUTLINE.md`
- `docs/sstables-definitive-guide/STYLE_GUIDE.md`
- `docs/sstables-definitive-guide/REFERENCES.md`
- `docs/sstables-definitive-guide/OPEN_QUESTIONS.md`
- `docs/sstable_component_architecture_research.md`
- `docs/sstable_component_path_review.md`
- `docs/technical/architecture.md`

## Handoff
- Feeds: T1 (Source Map and Pinning) and T2 (Outline Acceptance Criteria).
- Keep this file updated with any policy changes and resolved deltas.
