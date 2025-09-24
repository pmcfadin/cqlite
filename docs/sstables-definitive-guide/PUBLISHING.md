# Publishing (HTML/PDF)

Cassandra-first manuscript export plan for "SSTables: The Definitive Guide". Diagrams are authored in Mermaid (`.mmd`) and committed; SVG export is performed at build time.

## Goals
- Single-file PDF suitable for distribution
- Single-file HTML for web preview
- Reproducible, CLI-only process on macOS/Linux

## Prerequisites
- Pandoc (document converter)
- TeX engine for PDF (XeLaTeX recommended)
- Mermaid CLI for diagram rendering

On macOS (Homebrew):

```bash
brew install pandoc
brew install --cask mactex-no-gui   # or: brew install basictex && sudo tlmgr update --self && sudo tlmgr install xetex
brew install node
npm install -g @mermaid-js/mermaid-cli
```

## 1) Render diagrams (Mermaid .mmd → .svg)
Generate SVGs alongside `.mmd` sources (idempotent):

```bash
cd docs/sstables-definitive-guide/diagrams
for f in *.mmd; do
  base="${f%.mmd}";
  mmdc -i "$f" -o "${base}.svg" --backgroundColor white --scale 1.2;
done
```

Notes:
- Chapters should reference the `.svg` outputs when targeting PDF/HTML. If chapters embed Mermaid code blocks instead, use the Pandoc mermaid filter (see Option B) rather than pre-rendering.

## 2) Build the book (Option A: Pre-rendered SVGs)
Uses pre-rendered SVGs; simplest and fastest.

```bash
pandoc \
  docs/sstables-definitive-guide/chapters/*.md \
  -o docs/sstables-definitive-guide/dist/sstables-definitive-guide.pdf \
  --from gfm \
  --pdf-engine=xelatex \
  --toc --toc-depth=2 \
  --resource-path=.:docs/sstables-definitive-guide \
  -V geometry:margin=1in \
  -V mainfont="Helvetica Neue" -V monofont="Menlo" \
  -V colorlinks=true \
  --metadata title="SSTables: The Definitive Guide (Apache Cassandra)"
```

HTML export:

```bash
pandoc \
  docs/sstables-definitive-guide/chapters/*.md \
  -o docs/sstables-definitive-guide/dist/sstables-definitive-guide.html \
  --from gfm \
  --toc --toc-depth=2 \
  --resource-path=.:docs/sstables-definitive-guide
```

## 2B) Build the book (Option B: Render Mermaid on the fly)
If chapters contain Mermaid code blocks instead of image references, use a Pandoc filter:

```bash
pipx install pandoc-mermaid  # or: pip install --user pandoc-mermaid
pandoc \
  docs/sstables-definitive-guide/chapters/*.md \
  -o docs/sstables-definitive-guide/dist/sstables-definitive-guide.pdf \
  --from gfm \
  --pdf-engine=xelatex \
  --toc --toc-depth=2 \
  --filter pandoc-mermaid
```

## Structure and Ordering
Files are numbered (`01-..`, `02-..`, …, `appendix-..`) so globbing `chapters/*.md` preserves chapter order. Appendices are included automatically by the glob.

## Nice-PDF Touches (optional)
- Cover/title: provide a minimal front-matter file and include with `--include-before-body=docs/sstables-definitive-guide/cover.md`.
- Fonts: adjust with `-V mainfont`, `-V monofont` to match your system fonts.
- Code highlighting: pick `--highlight-style=tango|pygments|breezedark`.
- Links: keep `-V colorlinks=true` for readable hyperlinks.

## Validation Checklist
- Run diagram render; confirm `.svg` exists for each `.mmd` in `diagrams/`.
- Build HTML and PDF; open both and spot-check diagrams, tables, and code blocks.
- Verify citations are pinned to `cassandra-5.0.0` (see `REFERENCES.md`).
- Ensure Cassandra-first terminology per `STYLE_GUIDE.md`.

## Distribution
- Place artifacts under `docs/sstables-definitive-guide/dist/`:
  - `sstables-definitive-guide.pdf`
  - `sstables-definitive-guide.html`
- Attach the PDF to the release on your hosting platform.

## Future Enhancements
- Add a `just book-pdf` and `just book-html` recipe to wrap the commands above.
- Consider mdBook for a multi-page HTML version plus `mdbook-pdf` for print.
- Evaluate `typst` for faster, GUI-free PDF generation if LaTeX install is heavy.
