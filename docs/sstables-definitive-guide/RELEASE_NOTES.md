# Release Notes — SSTables: The Definitive Guide

This document captures distribution notes for the Cassandra-focused guide. Keep CQLite mentions minimal; prioritize Cassandra correctness and pinned upstream references.

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
