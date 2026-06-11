# Documentation Website Design

**Date**: 2026-06-11
**Status**: Validated (brainstorm with project owner)
**Target**: Ships with v0.11.0 as a parallel epic lane

## Problem

CQLite's documentation is rich but scattered and repo-bound: a 22-chapter SSTable
format guide, duplicated quick-starts in `docs/user-guides/`, API references spread
across loose files in `docs/`, and contributor knowledge living only in `CLAUDE.md`
and `.claude/skills/`. There is no public website beyond per-tag rustdoc on GitHub
Pages. Two audiences are unserved:

1. **Users** (humans) who need install/CLI/bindings/troubleshooting docs in a
   browsable site.
2. **Agents** (AI), in two distinct tracks:
   - agents *using* CQLite as a tool or library, who need terse, copy-pasteable,
     machine-verifiable recipes;
   - agents *developing* CQLite, who need the contributor doctrine (gate contract,
     no-heuristics mandate, validation workflow) in public, citable form.

## Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Agent audiences | Both tracks, separate sections | Distinct needs: "use CQLite" vs "develop CQLite" |
| Format guide | Publish in full | Unique asset; freshly audited to cassandra-5.0.8 (epic #598) |
| Tooling | Astro Starlight | Multi-section sidebars, built-in search, community plugins for llms.txt and raw-markdown endpoints |
| Sequencing | Parallel epic, ships with v0.11.0 | Docs work doesn't contend with code-epic files or reviewers |
| Hosting | GitHub Pages, existing URL | `pmcfadin.github.io/cqlite` already enabled; site takes root, rustdoc moves to `/api/<tag>/` |
| Versioning | Latest release only, version badge | YAGNI; pinned-version needs covered by rustdoc under `/api/<tag>/` |

## Architecture

One Starlight site in `website/` at the repo root, deployed to
`pmcfadin.github.io/cqlite/` by a new `docs-site.yml` workflow publishing to the
`gh-pages` branch root with `keep_files: true` (coexists with rustdoc trees).
`api-docs.yml` keeps its tag trigger; its publish path changes to `/api/<tag>/`
(and `/api/latest/`), with redirect stubs at the old per-tag URLs so links in past
release notes keep working.

Content source of truth stays in `docs/`; the site builds from it (symlinks
preferred over a sync step to avoid drift — final call in W1 implementation).

### Sections

1. **User Docs** — install, quick start, CLI reference, query guide, output
   formats (JSON/CSV/Parquet), Python bindings, Node bindings, write support,
   troubleshooting. Consolidates `docs/user-guides/` and scattered API docs.
2. **SSTable Format Guide** — the 22 chapters + appendices, near-verbatim;
   work is frontmatter, link rewriting, and SVG diagram embedding.
3. **For Agents: Using CQLite** — ~10 recipe pages, one task each: exact
   command/code, expected output shape, exit codes, failure modes. Written
   against the real test datasets so every example is verifiable. Error-code
   table generalized from the Node bindings docs.
4. **For Agents: Developing CQLite** — ~6 pages distilled from `CLAUDE.md`,
   `.claude/skills/`, and the orchestration doctrine (#719): gate contract,
   no-heuristics mandate, test-data fetching, key source paths, sstabledump
   validation playbook. `CLAUDE.md` then slims to pointers at these pages.

### Agent plumbing

- `llms.txt` at site root: section map with one-line descriptions.
- `llms-full.txt`: full-content variant.
- Every page also served as raw markdown at `<page-url>.md`.
- CI check: every published page appears in `llms.txt`; raw endpoints resolve.

## Content plan

Mostly moves and consolidation; new writing is concentrated in the agent tracks.

- Merge `quick-start.md`, `QUICK_START_GUIDE.md`, `UAT_QUICK_START.md` into one
  quick start; archive the losers.
- CLI reference rebuilt from `--help` output + verified command examples (one-shot,
  REPL, TUI, write subcommands, `--out` precedence) so it documents the actual binary.
- Bindings pages adapted from existing Python/Node material.
- User-facing limitations page ("What CQLite can and can't read") twinned from
  `appendix-f-known-limitations.md` — honest about da/BTI being unsupported and
  the v1 collection-tombstone gap (#493).

## CI and quality gates

Separate workflow from code gates — a docs break never blocks a code PR and
vice versa.

1. **Link check**: internal links/anchors fail the build (Starlight native);
   external links checked weekly via lychee, not per-PR.
2. **Example verification**: commands in the "Using CQLite" recipe pages are
   extracted into a smoke script run against the real test datasets in CI; a
   documented command that stops producing the documented output shape fails
   the build. Same philosophy as sstabledump parity, applied to docs.
3. **llms.txt validation** as above.

Local loop: `npm run dev` in `website/`; `scripts/docs-site-check.sh` mirrors the
CI checks (agent-gate pattern).

## Epic breakdown

| ID | Task | Size | Blocked by |
|----|------|------|-----------|
| W1 | Scaffold Starlight site, deploy workflow, rustdoc → `/api/` + redirects | M | — |
| W2 | Publish Format Guide section | M | W1 |
| W3 | User docs consolidation | M | W1 |
| W4 | CLI reference + output formats pages | M | W1 |
| W5 | Bindings pages (Python, Node) | S | W1 |
| W6 | Agent track "Using CQLite" recipes + example smoke script | L | W4 |
| W7 | Agent track "Developing CQLite" + CLAUDE.md slimming | M | W1 |
| W8 | Agent plumbing: llms.txt, llms-full.txt, raw endpoints, validation | S | W2, W3, W6, W7 |

W2–W5 and W7 are mutually independent; up to five builders can run concurrently
after W1 lands.

**Ship gate for v0.11.0**: site live with all four sections, CI checks green,
README pointing at the site.

## Out of scope (v1)

- Versioned docs (rustdoc per tag covers it)
- PR preview deploys (GitHub Pages legacy mode doesn't support them)
- Custom domain (can be added later without breaking links)
- Per-PR external-link checking (weekly lychee instead)
