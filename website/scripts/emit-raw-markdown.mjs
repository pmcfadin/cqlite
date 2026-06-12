#!/usr/bin/env node
/**
 * emit-raw-markdown.mjs
 *
 * Post-build step: copies the source Markdown (and MDX) files for every
 * published page into the Astro dist/ directory so that each page is also
 * reachable as raw Markdown at <page-url>.md.
 *
 * Mapping rule:
 *   src/content/docs/<slug>.md   →  dist/<slug>.md
 *   src/content/docs/<slug>.mdx  →  dist/<slug>.md   (extension normalised)
 *
 * Index files follow the same rule:
 *   src/content/docs/user-docs/index.md  →  dist/user-docs/index.md
 *   (served at /cqlite/user-docs/index.md — matches the page URL + ".md")
 *
 * For the site root:
 *   src/content/docs/index.mdx  →  dist/index.md
 *
 * The format-guide pages are generated at build time by sync-format-guide.mjs
 * and written to src/content/docs/sstable-format/<prefix>.md (gitignored).
 * They exist on disk when this script runs (postbuild), so they are picked up
 * by the glob scan automatically — no special handling needed.
 *
 * Usage:
 *   node website/scripts/emit-raw-markdown.mjs
 *   (called automatically by the "postbuild" npm hook)
 */

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const WEBSITE_DIR = path.resolve(__dirname, '..');
const CONTENT_DIR = path.join(WEBSITE_DIR, 'src', 'content', 'docs');
const DIST_DIR = path.join(WEBSITE_DIR, 'dist');

/**
 * Recursively collect all .md and .mdx files under a directory.
 * Returns an array of absolute file paths.
 */
function collectMarkdownFiles(dir) {
  const results = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...collectMarkdownFiles(fullPath));
    } else if (entry.isFile() && (entry.name.endsWith('.md') || entry.name.endsWith('.mdx'))) {
      results.push(fullPath);
    }
  }
  return results;
}

console.log('[emit-raw-markdown] Starting...');

if (!fs.existsSync(DIST_DIR)) {
  console.error(`[emit-raw-markdown] ERROR: dist/ not found at ${DIST_DIR}`);
  console.error('[emit-raw-markdown] Run npm run build before this script.');
  process.exit(1);
}

const sourceFiles = collectMarkdownFiles(CONTENT_DIR);
let copied = 0;
let skipped = 0;

for (const srcPath of sourceFiles) {
  // Compute the relative path from the content root
  const relFromContent = path.relative(CONTENT_DIR, srcPath);

  // Normalise extension: .mdx → .md
  const relNormalized = relFromContent.replace(/\.mdx$/, '.md');

  // The dist path mirrors the content path but lives under dist/
  const destPath = path.join(DIST_DIR, relNormalized);

  // Only copy if the corresponding HTML page exists in dist
  // (confirms the page was actually built, not a draft or excluded page).
  // The HTML is either at dist/<slug>/index.html (directory pages)
  // or dist/<slug>.html (flat pages, rare in Starlight).
  // For index.md files: dist/<parent>/index.html or dist/index.html at root.

  const relDir = path.dirname(relNormalized);          // e.g. "user-docs"
  const relBase = path.basename(relNormalized, '.md'); // e.g. "cli-reference"

  let htmlPath;
  if (relBase === 'index') {
    // index pages → dist/<parent>/index.html
    htmlPath = path.join(DIST_DIR, relDir, 'index.html');
  } else {
    // regular pages → dist/<parent>/<slug>/index.html
    htmlPath = path.join(DIST_DIR, relDir, relBase, 'index.html');
  }

  if (!fs.existsSync(htmlPath)) {
    // Page was not built (draft, excluded, or no matching route). Skip.
    skipped++;
    continue;
  }

  // Ensure destination directory exists
  fs.mkdirSync(path.dirname(destPath), { recursive: true });

  // Copy the source markdown to dist
  fs.copyFileSync(srcPath, destPath);
  copied++;
}

console.log(
  `[emit-raw-markdown] Done: ${copied} raw .md endpoints written, ${skipped} source files skipped (no matching built page).`
);
