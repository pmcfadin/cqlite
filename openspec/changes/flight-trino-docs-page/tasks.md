# Tasks — Flight/Trino user-docs page

## 1. Author the page
- [ ] 1.1 Create `website/src/content/docs/user-docs/flight-trino.md` with Starlight frontmatter
      (`title`, `description`, `sidebar.label`, `sidebar.order`). Surface: the site nav (User Docs group).
- [ ] 1.2 Write the "What it is" + compaction-on-read model section (co-located, on-the-fly merge,
      flushed-SSTables-only, snapshot-aware, `SELECT` parity). Source: `cqlite-flight/README.md`.
- [ ] 1.3 Write the "Run the container" section — unauthenticated `docker pull`/`docker run` against
      `ghcr.io/pmcfadin/cqlite-flight`, read-only mount, `--data-dir`, `:8815`.
- [ ] 1.4 Write the "Client / ticket API" section — ticket contract (keyspace/table/ddl + optional
      snapshot/token-range/columns/predicates), read-only RPC surface, runnable PyArrow example.
- [ ] 1.5 Write the "Trino connector" section — Sidecar node/token-range discovery, one-split-per-replica,
      plugin install (`./gradlew installPlugin` / Maven), catalog config incl. `cqlite.read-mode`
      snapshot-vs-live. Source: `trino-connector/README.md`.
- [ ] 1.6 Link back to the authoritative crate READMEs for exhaustive flag/property detail.

## 2. Discoverability
- [ ] 2.1 Add a cross-link to the new page from `website/src/content/docs/user-docs/use-cases/sidecar-lakehouse.md`
      (and optionally the README's Arrow Flight section) so the page is not orphaned.

## 3. Verify (correctness gate for this change)
- [ ] 3.1 `cd website && npm run build` passes, including `starlightLinksValidator` — no broken internal
      link/anchor. This is the gate of record for a docs-only change (the Rust `agent-gate.sh` does not
      exercise the site).
- [ ] 3.2 Manual read-through against `cqlite-flight/README.md` + `trino-connector/README.md`: verify no
      contradictions (image name, `:8815`, ticket fields, `read-mode` default, install path).
- [ ] 3.3 Confirm the diff touches only `website/` (+ the one cross-linked page) — no `src/` / product code.

## 4. Pipeline close-out (run by flow-closer)
- [ ] 4.1 Full `scripts/agent-gate.sh` (expected: docs-only diff → PASS; site build is the substantive check).
- [ ] 4.2 C intent audit — `spec-auditor` anchored to `openspec/changes/flight-trino-docs-page/specs/**`
      (every requirement `satisfied` with the page/build as evidence).
- [ ] 4.3 Final roborev pass on the branch (`--base origin/main`).
- [ ] 4.4 Merge on green + `flow-finalize` (archive the change, sync specs, remove worktree/branch, close #2115).
