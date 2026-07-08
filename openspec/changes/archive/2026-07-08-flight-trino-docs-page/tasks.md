# Tasks — Flight/Trino user-docs page

## 1. Author the page
- [x] 1.1 Create `website/src/content/docs/user-docs/flight-trino.md` with Starlight frontmatter
      (`title`, `description`, `sidebar.label`, `sidebar.order`). Surface: the site nav (User Docs group).
- [x] 1.2 Write the "What it is" + compaction-on-read model section (co-located, on-the-fly merge,
      flushed-SSTables-only, snapshot-aware, `SELECT` parity). Source: `cqlite-flight/README.md`.
- [x] 1.3 Write the "Run the container" section — unauthenticated `docker pull`/`docker run` against
      `ghcr.io/pmcfadin/cqlite-flight`, read-only mount, `--data-dir`, `:8815`.
- [x] 1.4 Write the "Client / ticket API" section — ticket contract (keyspace/table/ddl + optional
      snapshot/token-range/columns/predicates), read-only RPC surface, runnable PyArrow example.
- [x] 1.5 Write the "Trino connector" section — Sidecar node/token-range discovery, one-split-per-replica,
      plugin install (`./gradlew installPlugin` / Maven), catalog config incl. `cqlite.read-mode`
      snapshot-vs-live. Source: `trino-connector/README.md`.
- [x] 1.6 Link back to the authoritative crate READMEs for exhaustive flag/property detail.

## 2. Discoverability
- [x] 2.1 Add a cross-link to the new page from `website/src/content/docs/user-docs/use-cases/sidecar-lakehouse.md`
      (and optionally the README's Arrow Flight section) so the page is not orphaned.

## 3. Verify (correctness gate for this change)
- [x] 3.1 `cd website && npm run build` passes, including `starlightLinksValidator` — no broken internal
      link/anchor. This is the gate of record for a docs-only change (the Rust `agent-gate.sh` does not
      exercise the site).
- [x] 3.2 Manual read-through against `cqlite-flight/README.md` + `trino-connector/README.md`: verify no
      contradictions (image name, `:8815`, ticket fields, `read-mode` default, install path).
- [x] 3.3 Confirm the diff touches only `website/` (+ the one cross-linked page) — no `src/` / product code.

## 4. Pipeline close-out (run by flow-closer)
- [x] 4.1 Site build gate (docs-only diff → the substantive check): `npm run build` EXIT=0, `✓ All internal links are valid.`
- [x] 4.2 C intent audit — all 6 requirements satisfied with the rendered page as evidence (PASS).
- [x] 4.3 Final roborev pass on the branch (`--base origin/main`): 3 doc-accuracy blockers fixed over 2 rounds, converged clean.
- [x] 4.4 Merge on green (PR #2181, squash be06c31e) + `flow-finalize` (archive the change, sync specs, remove worktree/branch, close #2115).
