# Agent Work Prompt — SSTables: The Definitive Guide

This file is the agentic runbook for producing an O’Reilly-style, code-backed reference on Cassandra SSTables while reinforcing CQLite’s goal to be the most accurate and efficient reader/writer.

## Mission (read-first)
- Produce a definitive, comprehensible reference on SSTables (Cassandra 5.0 baseline).
- Keep explanations grounded in source code: cite Cassandra 5.0.0 permalinks and CQLite modules.
- Favor clarity and minimal examples over verbosity; teach the read/write path and file formats.

## Scope & Constraints
- Baseline: Cassandra 5.0 (sidebars for 3.x/4.x when materially different).
- SAI coverage includes vector indexing.
- Compaction: STCS/LCS/TWCS in main text; UCS in a sidebar.
- Diagrams: Mermaid `.mmd` committed; SVG export optional later.
- Canonical data for examples: `test-data/datasets/test_basic`.
- Upstream code snippets: short (<30 lines) with a permalink to full source.

## Authoritative References (load into context)
- `docs/sstables-definitive-guide/OUTLINE.md`
- `docs/sstables-definitive-guide/STYLE_GUIDE.md`
- `docs/sstables-definitive-guide/REFERENCES.md`
- `docs/sstables-definitive-guide/OPEN_QUESTIONS.md`
- Supporting internal research: `docs/sstable_component_architecture_research.md`, `docs/sstable_component_path_review.md`, `docs/technical/architecture.md`, `docs/user-guides/*`

## Operating Rules (quality gates)
- Pin all upstream citations to `cassandra-5.0.0` (or SHA) and include class/package.
- Keep chapters ≤ ~500 lines; split if needed. Follow `STYLE_GUIDE.md`.
- Each chapter includes: summary, “learn” bullets, body, sidebars/callouts, key takeaways, references.
- All diagrams have alt-text and captions; `.mmd` committed under `diagrams/`.
- Run `just check` and `cargo test --workspace --all-features` before PR.

## Definition of Done (per chapter)
- Meets style guide; has at least one Cassandra code permalink and one CQLite code reference.
- Includes required diagrams/tables where indicated by the outline.
- Examples validated on `test_basic` with small, trimmed outputs.
- Tech review and editorial pass approved.

---

## Sequential Work Plan (dependent tasks with handoffs)

- [x] T0: Initialize Context and Working Notes (Owner: Research)
  - Inputs: Files in Authoritative References
  - Actions: Read all; extract assumptions; confirm scope decisions
  - Outputs: `references/context-brief.md` (assumptions, open deltas), updated `OPEN_QUESTIONS.md` if gaps
  - Handoff: Share `context-brief.md` with all subsequent tasks

- [x] T1: Source Map and Pinning (Owner: Research)
  - Inputs: `REFERENCES.md`, Cassandra 5.0.0 repository
  - Actions: Pin missing references to `cassandra-5.0.0`; map classes per component (Data/Index/Summary/Filter/Stats/CompressionInfo, BTI, SAI incl. vector)
  - Outputs: `references/source-map.md` (table of component → class/files + permalinks); PR to update `REFERENCES.md` if needed
  - Handoff: `source-map.md` feeds all drafting tasks

- [x] T2: Lock Outline + Acceptance Criteria (Owner: Leads)
  - Inputs: `OUTLINE.md`, `context-brief.md`
  - Actions: For each chapter, add 3–6 acceptance bullets (learning goals + required artifacts)
  - Outputs: Updated `OUTLINE.md` with acceptance bullets; `references/acceptance-criteria.md`
  - Handoff: Criteria guide chapter skeletons and reviews

- [x] T3: Chapter Skeletons Pass (Owner: Authors)
  - Inputs: Outline + acceptance criteria
  - Actions: Create per-chapter files with headings, summary, “learn” bullets, placeholders for diagrams/code refs
  - Outputs: `chapters/XX-title.md` files scaffolded per `STYLE_GUIDE.md`; stub `.mmd` diagrams in `diagrams/`
  - Handoff: Skeletons become targets for drafting passes

- [x] T4: Part I Draft (Ch. 1–3) (Owner: Authors, Diagrams, Reviewers)
  - Inputs: Skeletons, `source-map.md`
  - Actions: Write content; add minimal code refs and diagrams; add sidebars for 3.x/4.x deltas
  - Outputs: Drafted Ch.1–3 with `.mmd` diagrams; references section with pinned links
  - Gates: `just check` clean; reviewer sign-off
  - Handoff: Lessons learned appended to `context-brief.md`

- [x] T5: Part II Draft (Ch. 4–9: Write Path + Components) (Owner: Authors, Diagrams)
  - Inputs: Prior drafts, `source-map.md`
  - Actions: Deep dives for Data/Index/Summary/Filter/Stats/CompressionInfo; pseudocode for flush pipeline
  - Outputs: Drafted Ch.4–9; diagrams for flush pipeline and component relations
  - Gates: Examples validated on `test_basic`; citations pinned

- [x] T6: Part III Draft (Ch. 10–12: Read Path & OS) (Owner: Authors)
  - Inputs: Prior drafts
  - Actions: Read flow (Bloom→Index→Summary→Data), slices/ranges, caching/OS interactions
  - Outputs: Drafted Ch.10–12; decision tree diagram
  - Gates: Reviewer approval; trimmed `sstabledump` excerpts

- [x] T7: Indexing Draft (Ch. 13–14: 2i and SAI incl. Vector) (Owner: Authors)
  - Inputs: Prior drafts, SAI classes
  - Actions: Explain storage layout, query flow; include vector indexing coverage
  - Outputs: Drafted Ch.13–14; SAI file layout and query flow diagrams
  - Gates: Pinned SAI permalinks (5.0.0); examples coherent and small

- [x] T8: Compaction & Lifecycle Draft (Ch. 15–16) (Owner: Authors)
  - Inputs: Prior drafts
  - Actions: STCS/LCS/TWCS main; UCS as sidebar; maintenance and anticorruption checks
  - Outputs: Drafted Ch.15–16; compaction comparison table; UCS sidebar
  - Gates: Keep concise; not a deep operations guide

- [x] T9: Advanced Topics Draft (Ch. 17–20) (Owner: Authors)
  - Inputs: Prior drafts
  - Actions: BTI differences, repair/streaming overview, backups/snapshots, checksums/integrity
  - Outputs: Drafted Ch.17–20 with required diagrams/tables
  - Gates: Pin references; maintain clarity

- [x] T10: Appendices (A–E) (Owner: Authors)
  - Inputs: All prior work
  - Actions: Type mapping tables, encoding cheat sheet, code walkthroughs, tools, glossary
  - Outputs: Appendices A–E complete; tables under `tables/`
  - Gates: Tables accurate; examples verified

- [x] T11: Cross-Chapter Consistency Pass (Owner: Editors)
  - Inputs: All chapters
  - Actions: Normalize terminology, file/component names, style and captions
  - Outputs: Consistency fixes; updated `STYLE_GUIDE.md` if norms changed
  - Gates: No contradictions across chapters

- [x] T12: Quality & Build (Owner: Engineering)
  - Inputs: Repository state
  - Actions: link checks; image/diagram presence
  - Outputs: Green checks; issues filed/fixed

- [x] T13: Publication Prep (Owner: Editors)
  - Inputs: Final chapters
  - Actions: Ensure `README.md` indexes chapters; optional HTML/PDF export plan captured
  - Outputs: Book index updated; release notes and validation steps

---

## Team Roles (assign per task)
- Research, Authors, Diagrams, Engineering, Reviewers, Editors.
- Each task’s Outputs must be attached in PR and referenced in the description.

## Execution Notes for Agents
- Keep commits focused (Conventional Commits). Include validation steps and permalinks in PRs.
- When a task completes, paste Outputs into the next task’s Inputs section in the PR to preserve context.
- Ask/record open issues in `OPEN_QUESTIONS.md`; resolve before drafting where feasible.
