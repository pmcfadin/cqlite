#!/usr/bin/env node
/**
 * validate-agent-plumbing.mjs
 *
 * Post-build validation: checks that every published HTML page:
 *   1. Appears in dist/llms.txt (or dist/llms-full.txt as fallback)
 *   2. Has a corresponding raw .md endpoint in dist/
 *
 * This script inspects dist/ only — no live HTTP requests.
 * Exit code 0 = all checks pass; 1 = one or more missing pages.
 *
 * Usage:
 *   node website/scripts/validate-agent-plumbing.mjs
 *   (called by scripts/docs-site-check.sh and docs-site.yml)
 *
 * What counts as a "published page":
 *   Any dist/**\/index.html that is not:
 *   - 404.html
 *   - Inside _astro/ or pagefind/ (build artifacts)
 *
 * llms.txt coverage:
 *   The starlight-llms-txt plugin writes page URLs into llms-full.txt
 *   (one per section, full content). We check that the relative URL path
 *   of each HTML page appears somewhere in llms-full.txt.
 *   The top-level llms.txt is a section map; checking llms-full.txt gives
 *   stronger per-page coverage.
 *
 * Raw .md endpoint:
 *   dist/user-docs/cli-reference/index.html  →  dist/user-docs/cli-reference.md
 *   dist/user-docs/index.html               →  dist/user-docs/index.md
 *   dist/index.html                         →  dist/index.md
 */

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const WEBSITE_DIR = path.resolve(__dirname, '..');
const DIST_DIR = path.join(WEBSITE_DIR, 'dist');

// ── Helpers ───────────────────────────────────────────────────────────────────

function abort(msg) {
  console.error(`[validate-agent-plumbing] ERROR: ${msg}`);
  process.exit(1);
}

function collectHtmlPages(dir, baseDir) {
  baseDir = baseDir ?? dir;
  const pages = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    // Skip build artefact directories
    if (entry.isDirectory()) {
      if (entry.name === '_astro' || entry.name === 'pagefind') continue;
      pages.push(...collectHtmlPages(fullPath, baseDir));
    } else if (entry.isFile() && entry.name === 'index.html') {
      const rel = path.relative(baseDir, dir); // e.g. "user-docs/cli-reference"
      pages.push(rel === '' ? '/' : '/' + rel + '/');
    }
  }
  return pages;
}

// ── Pre-flight ─────────────────────────────────────────────────────────────────

if (!fs.existsSync(DIST_DIR)) {
  abort(`dist/ not found at ${DIST_DIR}. Run npm run build first.`);
}

const llmsFullPath = path.join(DIST_DIR, 'llms-full.txt');
const llmsTxtPath  = path.join(DIST_DIR, 'llms.txt');

if (!fs.existsSync(llmsFullPath)) {
  abort('dist/llms-full.txt not found. The starlight-llms-txt plugin may not be installed or the build failed.');
}
if (!fs.existsSync(llmsTxtPath)) {
  abort('dist/llms.txt not found. The starlight-llms-txt plugin may not be installed or the build failed.');
}

// ── Collect published pages ────────────────────────────────────────────────────

// Collect all index.html paths, excluding 404
const rawPages = collectHtmlPages(DIST_DIR);
// Filter out the 404 page (it lives at dist/404.html, not dist/*/index.html, so
// it never appears in rawPages; guard anyway)
const pages = rawPages.filter(p => !p.includes('404'));

console.log(`[validate-agent-plumbing] Found ${pages.length} published pages.`);

// ── Check 1: llms.txt coverage ────────────────────────────────────────────────

const llmsFullContent = fs.readFileSync(llmsFullPath, 'utf-8');

const missingFromLlms = [];
for (const page of pages) {
  // The plugin writes URLs like /cqlite/user-docs/cli-reference/ — strip the
  // trailing slash to match both forms, and match the path segment.
  // We look for the page path (without leading slash) anywhere in the file.
  const searchPath = page.replace(/^\//, '').replace(/\/$/, '');
  // Accept both /cqlite/<path>/ and /cqlite/<path> (with or without trailing slash)
  const pattern = `/${searchPath}`;
  if (!llmsFullContent.includes(pattern)) {
    missingFromLlms.push(page);
  }
}

// ── Check 2: Raw .md endpoint coverage ────────────────────────────────────────

const missingRawMd = [];
for (const page of pages) {
  // Map page URL to expected .md file in dist/
  // /  →  dist/index.md
  // /user-docs/  →  dist/user-docs/index.md
  // /user-docs/cli-reference/  →  dist/user-docs/cli-reference.md

  let relPath;
  if (page === '/') {
    relPath = 'index.md';
  } else {
    // Remove leading and trailing slashes → "user-docs/cli-reference"
    const stripped = page.replace(/^\//, '').replace(/\/$/, '');
    const parts = stripped.split('/');
    // If the last segment is "index" (shouldn't happen with Starlight), treat as dir
    const last = parts[parts.length - 1];
    if (last === '') {
      // Trailing slash after join — shouldn't happen but guard
      relPath = stripped + 'index.md';
    } else {
      // The page at /a/b/ has its HTML at dist/a/b/index.html
      // The raw endpoint is dist/a/b.md  (NOT dist/a/b/index.md)
      // However index pages (/a/) → dist/a/index.md
      // Detect index pages: the directory itself has an index.html AND the
      // parent directory also has an index.html (i.e. page is a section overview)
      // In Starlight, every directory gets an index page, so we use:
      //   /user-docs/        →  dist/user-docs/index.md       (section index)
      //   /user-docs/python/ →  dist/user-docs/python.md      (leaf page)
      // The index pages have their parent as the content dir (e.g. user-docs/index.md)
      // Leaf pages have a matching filename (e.g. user-docs/python.md)
      // We detect index pages by checking if the path ends with a known section root.
      // Simpler: check if a dist/<stripped>/index.md exists; if so it's an index page.
      // Actually we just check both paths and use whichever exists.

      // The emit-raw-markdown.mjs script writes:
      //   for index.md source files  → dist/<parent>/index.md
      //   for other source files     → dist/<parent>/<slug>.md
      //
      // So the raw endpoint for:
      //   page /user-docs/cli-reference/  →  dist/user-docs/cli-reference.md
      //   page /user-docs/               →  dist/user-docs/index.md
      //
      // We detect the index case by checking if the LAST segment in the URL
      // path (before trailing slash) is the same as the parent. But actually
      // the simplest signal is: does dist/<stripped>/index.md exist?

      const indexMdPath = path.join(DIST_DIR, stripped, 'index.md');
      const leafMdPath  = path.join(DIST_DIR, stripped + '.md');
      // Leaf path is used for non-index pages
      if (fs.existsSync(indexMdPath)) {
        relPath = stripped + '/index.md';
      } else {
        relPath = stripped + '.md';
      }
    }
  }

  const mdPath = path.join(DIST_DIR, relPath);
  if (!fs.existsSync(mdPath)) {
    missingRawMd.push({ page, expected: relPath });
  }
}

// ── Report ─────────────────────────────────────────────────────────────────────

let failed = false;

if (missingFromLlms.length > 0) {
  failed = true;
  console.error('\n[validate-agent-plumbing] FAIL: The following pages are missing from llms-full.txt:');
  for (const p of missingFromLlms) {
    console.error(`  MISSING FROM llms-full.txt: ${p}`);
  }
}

if (missingRawMd.length > 0) {
  failed = true;
  console.error('\n[validate-agent-plumbing] FAIL: The following pages have no raw .md endpoint in dist/:');
  for (const { page, expected } of missingRawMd) {
    console.error(`  MISSING RAW ENDPOINT: ${page}  (expected dist/${expected})`);
  }
}

if (failed) {
  console.error('\n[validate-agent-plumbing] FAILED — see errors above.');
  process.exit(1);
} else {
  console.log(`[validate-agent-plumbing] PASS: all ${pages.length} pages are covered by llms-full.txt and have raw .md endpoints.`);
  process.exit(0);
}
