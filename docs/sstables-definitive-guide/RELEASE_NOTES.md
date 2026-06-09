# Release Notes — SSTables: The Definitive Guide

This document captures distribution notes for the Cassandra-focused guide. Keep CQLite mentions minimal; prioritize Cassandra correctness and pinned upstream references.

## 2026-06-09 — Source-verification audit vs Apache Cassandra 5.0.8

Full claim-by-claim audit of all guide chapters against a cassandra-5.0.8 source checkout
(epic #598, PRs #609–#618 + this PR #619).

### Summary of changes

- All permalinks re-pinned from cassandra-5.0.0 to cassandra-5.0.8 across REFERENCES.md,
  STYLE_GUIDE.md, source-map.md, context-brief.md, and both appendixes.
- Major corrections applied (via individual audit PRs):
  - Cell flag bits 3/4 corrected to USE_ROW_TIMESTAMP / USE_ROW_TTL
  - Unsigned VInt encoding confirmed for temporal deltas
  - Filter.db layout verified: `hashCount` (4B) then `wordCount` (4B) then raw bitset bytes;
    field labels were previously swapped
  - Inline per-chunk CRC32 clarified: 4-byte big-endian u32 appended in Data.db after each
    compressed chunk; CompressionInfo.db holds only chunk start offsets
  - Default chunk length confirmed as 16 KiB (not a different value)
  - Version letter identifiers: `oa`/`da` replace the former `V5_0NewBig` placeholder names
  - BIG Index.db entry format corrected: u16 BE key length + raw key bytes + vint position;
    no 0x0010 marker, no MD5 digest — the bytes `0010` are the key length itself
  - BTI clarified as "Big Trie-Indexed" format family
- New coverage added: SAI vector on-disk format, zero-copy streaming, UCS compaction strategy,
  manifest.json component.
- Retracted: "Header CRC32 Validation (Legacy/BIG only)" section removed from Appendix C
  (no BIG-family component prepends a 4-byte CRC32 header prefix; the per-chunk CRC lives
  inline in NB Data.db only).
- Moved class paths corrected:
  - `SSTableReader` → `io/sstable/format/SSTableReader.java`
  - `SSTableWriter` → `io/sstable/format/SSTableWriter.java`
  - `IndexSummary` → `io/sstable/indexsummary/IndexSummary.java`
  - `IndexSummaryBuilder` → `io/sstable/indexsummary/IndexSummaryBuilder.java`
  - `SSTableDump` (nonexistent) → `tools/SSTableExport.java`
  - `SSTableMetadata` (nonexistent) → `tools/SSTableMetadataViewer.java`

---

## v0.1 (Draft for Review)

### Highlights
- Complete chapter set (01–20) plus Appendices A–E
- Cassandra 5.0 baseline with sidebars for 3.x/4.x where materially different
- Mermaid diagrams committed; SVGs generated for publication
- References pinned to `cassandra-5.0.0` in `REFERENCES.md`

### Artifacts
- `docs/sstables-definitive-guide/dist/sstables-definitive-guide.pdf`
- `docs/sstables-definitive-guide/dist/sstables-definitive-guide.html`

### Validation Steps
1. Render diagrams to SVG:
   ```bash
   cd docs/sstables-definitive-guide/diagrams
   for f in *.mmd; do base="${f%.mmd}"; mmdc -i "$f" -o "${base}.svg" --backgroundColor white --scale 1.2; done
   ```
2. Build PDF and HTML (see `PUBLISHING.md` for options):
   ```bash
   pandoc docs/sstables-definitive-guide/chapters/*.md -o docs/sstables-definitive-guide/dist/sstables-definitive-guide.pdf --from gfm --pdf-engine=xelatex --toc --toc-depth=2 --resource-path=.:docs/sstables-definitive-guide -V geometry:margin=1in -V mainfont="Helvetica Neue" -V monofont="Menlo" -V colorlinks=true --metadata title="SSTables: The Definitive Guide (Apache Cassandra)"
   pandoc docs/sstables-definitive-guide/chapters/*.md -o docs/sstables-definitive-guide/dist/sstables-definitive-guide.html --from gfm --toc --toc-depth=2 --resource-path=.:docs/sstables-definitive-guide
   ```
3. Open PDF and HTML; verify:
   - Table of contents matches chapter ordering
   - Diagrams render with correct labels
   - Code blocks and tables wrap cleanly; no overflow
   - Citations resolve and are pinned to `cassandra-5.0.0`
4. Run repository quality gates:
   ```bash
   just check
   cargo test --workspace --all-features
   ```

### Known Issues
- Large LaTeX distribution may be required; consider `typst` alternative later
- Some diagrams may benefit from manual sizing for print layout

### Next
- Add `just book-pdf` and `just book-html` convenience recipes
- Explore mdBook multi-page site + downloadable PDF
