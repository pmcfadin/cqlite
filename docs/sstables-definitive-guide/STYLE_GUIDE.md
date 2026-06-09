# Style Guide for "SSTables: The Definitive Guide"

This guide ensures consistency across chapters and contributors.

## Chapter Structure
- Title, one-paragraph summary, and "In this chapter you will learn" bullets (3–6 items)
- Body organized by H2/H3 sections; avoid deep nesting beyond H3
- Sidebars for version differences (3.x/4.x vs 5.0), labeled "Sidebar: Version Differences"
- Callouts for key ideas:
  - Note (background), Tip (practical advice), Warning (pitfalls), Reference (source links)
- End with:
  - Key Takeaways (3–6 bullets)
  - References (stable permalinks to Cassandra code/docs)
  - Cross-links to related chapters

## Writing Style
- Prefer active voice; precise, concise, technical tone
- Define terms on first use; add to Glossary
- Use consistent terminology: SSTable component names as `Data.db`, `Index.db`, etc.
- Terminology normalization:
  - "Bloom filter" (lowercase "filter" in running text); component file is `Filter.db`
  - "Storage-Attached Index (SAI)" (hyphenated) on first use; thereafter "SAI"
  - Component names spelled exactly: `Statistics.db`, `CompressionInfo.db`, `Digest.crc32`, `TOC.txt`
- Versioning: default to Cassandra 5.0; call out older versions only when materially different
- Keep paragraphs short; leverage lists for procedures and concepts

## Code and Pseudocode
- Use short, focused snippets (<30 lines); elide with `// ...`
- Add brief captions before code blocks stating intent
- For existing repo code, use code references with start:end:filepath format
- For upstream Cassandra code, embed short excerpts (<30 lines) with a permalink to full source
- For new pseudocode, prefer language-agnostic style or annotated Rust-like pseudocode

## Diagrams and Tables
- Author in Mermaid; commit `.mmd` files only for now (SVG optional later)
- Place in `diagrams/` and reference relatively from chapters
- Provide alt-text and a one-line caption
- Tables go in `tables/` when large; small tables inline in Markdown

## Citations and Sources
- Always include at least one Cassandra source link per major claim
- Pin links to specific git tags/commits; default to `cassandra-5.0.8` tag
- Cassandra-first policy: keep CQLite references to a minimum. Avoid embedding CQLite code in core chapters; when useful, add a brief cross-link to Appendix C instead of in-body excerpts.

## Examples and Datasets
- Use `test-data/datasets/test_basic` for canonical examples
- Include `sstabledump` excerpts where illustrative; keep outputs trimmed and annotated

## Length and Splitting
- Aim for ≤500 lines per chapter file; split into `chapter-name/part-1.md`, etc., when longer

## Formatting Conventions
- Use backticks for files/dirs/classes/functions
- Use American English spelling
- Avoid trailing whitespace; wrap at ~100 columns

## Topic-Specific Conventions
- Compaction: cover STCS/LCS/TWCS in main text; keep UCS as a sidebar
- SAI: include vector indexing where relevant (segments, queries)

## Review Checklist (for PRs)
- Chapter structure followed (summary, learn bullets, takeaways)
- Diagrams included (Mermaid `.mmd`) if referenced
- At least one code reference and one external citation
- Version differences called out in sidebars where applicable
- Examples use `test_basic` dataset
