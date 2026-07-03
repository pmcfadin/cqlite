# Proposals and Research Section Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the approved official **Proposals and Research** section to the CQLite website, first scoped to the storage-engine direction.

**Architecture:** Add a new Starlight sidebar section under `website/src/content/docs/proposals-research/`. The landing page is a curated hub; the storage-engine page is a compact diagram-led official proposal that links back to source research. A deterministic validation script guards the expected section, content, links, and generated concept image.

**Tech Stack:** Astro Starlight, MDX content, Node.js validation script, Imagegen 2 raster concept asset, Mermaid code blocks for precise diagrams.

---

### Task 1: Add Website Contract Test

**Files:**
- Create: `website/scripts/validate-proposals-research.mjs`
- Modify: `website/package.json`

- [ ] **Step 1: Create the validation script**

Create `website/scripts/validate-proposals-research.mjs` with checks for:

```javascript
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

const root = new URL('..', import.meta.url).pathname;

const checks = [
  {
    name: 'sidebar has Proposals and Research section',
    file: 'astro.config.mjs',
    includes: ["label: 'Proposals and Research'", "directory: 'proposals-research'"],
  },
  {
    name: 'home page links to proposals hub',
    file: 'src/content/docs/index.mdx',
    includes: ['title="Proposals and Research"', 'href="/cqlite/proposals-research/"'],
  },
  {
    name: 'proposals hub exists and features storage engine direction',
    file: 'src/content/docs/proposals-research/index.mdx',
    includes: ['title: Proposals and Research', 'Storage Engine Direction', '/cqlite/proposals-research/storage-engine/'],
  },
  {
    name: 'storage-engine proposal page exists with approved parallel-plane model',
    file: 'src/content/docs/proposals-research/storage-engine.mdx',
    includes: [
      'title: Storage Engine Direction',
      'Cassandra OLTP Plane',
      'SSTable Foundation',
      'OLAP Plane',
      'storageEngineConcept',
      'docs/storage engine/report-2-storage-engine-feasibility.md',
      '```mermaid',
    ],
  },
  {
    name: 'generated concept image is present',
    file: 'src/assets/storage-engine-parallel-planes.png',
    binary: true,
  },
];

let failed = false;

for (const check of checks) {
  const path = join(root, check.file);
  if (!existsSync(path)) {
    console.error(`FAIL ${check.name}: missing ${check.file}`);
    failed = true;
    continue;
  }

  if (check.binary) {
    console.log(`PASS ${check.name}`);
    continue;
  }

  const contents = readFileSync(path, 'utf8');
  for (const expected of check.includes) {
    if (!contents.includes(expected)) {
      console.error(`FAIL ${check.name}: ${check.file} does not include ${JSON.stringify(expected)}`);
      failed = true;
    }
  }

  if (!failed) {
    console.log(`PASS ${check.name}`);
  }
}

if (failed) {
  process.exit(1);
}
```

- [ ] **Step 2: Add npm script**

Add this entry to `website/package.json`:

```json
"validate-proposals-research": "node scripts/validate-proposals-research.mjs"
```

- [ ] **Step 3: Verify RED**

Run:

```bash
npm run validate-proposals-research
```

Expected: FAIL because `proposals-research` pages, sidebar entry, homepage card, and generated image do not exist yet.

### Task 2: Generate and Save Concept Image

**Files:**
- Create: `website/src/assets/storage-engine-parallel-planes.png`

- [ ] **Step 1: Generate Imagegen 2 concept visual**

Use Imagegen 2 with this prompt:

```text
Use case: scientific-educational
Asset type: website concept visual
Primary request: A polished abstract technical illustration showing two parallel data-processing planes over a shared storage foundation.
Scene/backdrop: clean light documentation-style background, no dark theme.
Subject: left side suggests an operational database/write plane, right side suggests an analytical query/lakehouse plane, bottom suggests a shared durable file/storage foundation.
Style/medium: crisp modern technical illustration, subtle depth, professional open-source documentation style.
Composition/framing: wide landscape banner with generous whitespace; visual separation between left, bottom, and right areas.
Color palette: restrained blue, teal, graphite, and white; avoid purple-dominant gradients.
Text: no text, no letters, no numbers.
Constraints: do not include logos, brand names, product names, screenshots, watermarks, or readable labels.
Avoid: dense tiny details, text-like marks, fantasy imagery, people, photorealistic server rooms.
```

- [ ] **Step 2: Copy selected output into the website**

Copy the selected generated image to:

```text
website/src/assets/storage-engine-parallel-planes.png
```

### Task 3: Add Proposals and Research Content

**Files:**
- Create: `website/src/content/docs/proposals-research/index.mdx`
- Create: `website/src/content/docs/proposals-research/storage-engine.mdx`
- Modify: `website/src/content/docs/index.mdx`
- Modify: `website/astro.config.mjs`

- [ ] **Step 1: Add sidebar entry**

Modify `website/astro.config.mjs` so the Starlight sidebar includes:

```javascript
{
  label: 'Proposals and Research',
  autogenerate: { directory: 'proposals-research' },
},
```

- [ ] **Step 2: Add homepage card**

Add a `LinkCard` to `website/src/content/docs/index.mdx`:

```mdx
<LinkCard
  title="Proposals and Research"
  description="Official forward-looking proposals, design direction, and research-backed architecture notes."
  href="/cqlite/proposals-research/"
/>
```

- [ ] **Step 3: Add hub page**

Create `website/src/content/docs/proposals-research/index.mdx` with frontmatter, status legend, and a featured link to `/cqlite/proposals-research/storage-engine/`.

- [ ] **Step 4: Add storage-engine proposal page**

Create `website/src/content/docs/proposals-research/storage-engine.mdx` with:

- Imported `storageEngineConcept` image.
- Public summary.
- Parallel-plane architecture diagram.
- Data-path diagram.
- Decision map.
- Research source links to `docs/storage engine/`.

### Task 4: Verify and Commit

**Files:**
- All files from Tasks 1-3

- [ ] **Step 1: Verify GREEN contract**

Run:

```bash
npm run validate-proposals-research
```

Expected: PASS for every section.

- [ ] **Step 2: Build website**

Run:

```bash
npm run build
```

Expected: Astro build succeeds and Starlight link validator reports no broken internal links.

- [ ] **Step 3: Review git diff**

Run:

```bash
git diff --stat
git diff --name-status
```

Expected: only website files, the generated asset, this plan, and any intentional spec/design files are changed.

- [ ] **Step 4: Commit**

Run:

```bash
git add docs/superpowers/plans/2026-07-03-proposals-research-section.md website/astro.config.mjs website/package.json website/scripts/validate-proposals-research.mjs website/src/assets/storage-engine-parallel-planes.png website/src/content/docs/index.mdx website/src/content/docs/proposals-research/index.mdx website/src/content/docs/proposals-research/storage-engine.mdx
git commit -m "docs: add proposals research website section"
```

## Self-Review

Spec coverage:

- New official section: Task 3.
- Storage-engine-only first launch: Task 3.
- Parallel OLTP/OLAP diagram: Task 3.
- Imagegen 2 concept visual: Task 2.
- Deterministic diagrams for labels: Task 3.
- Validation: Task 4.

Placeholder scan:

- No TODO, TBD, or deferred implementation placeholders.

Scope check:

- This plan does not implement storage-engine code and does not migrate unrelated proposal threads.
