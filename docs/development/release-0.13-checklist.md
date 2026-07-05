# Release 0.13.0 — Preparation Checklist

> **Status:** PREP (started 2026-07-04). This is the tracking spine for the 0.13
> release. It inventories every supporting file, its current state, and who does
> what. Check boxes on the **release commit / RC tag**, not on a stale branch.
>
> Release theme (draft): **performance + correctness-honesty** on top of v0.12.0's
> byte-for-byte compaction parity — read-path constant-factor wins (Epic E, C2),
> Node bindings throughput, and no-heuristics correctness fixes.
>
> Companion gate for public parity claims: [`parity-release-checklist.md`](./parity-release-checklist.md).

## Supporting-file inventory

| File | Current | Needed for 0.13 | Owner |
|---|---|---|---|
| `CHANGELOG.md` | `[Unreleased]` populated (this prep) | Rename `[Unreleased]` → `[v0.13.0] - <DATE>` on release commit; final owner curation of DRAFT entries | owner curate → worker rename |
| `Cargo.toml` (workspace) | `0.12.0` (2 sites: `[workspace.package]` + one crate) | Bump → `0.13.0` | worker (gated) |
| `bindings/python/pyproject.toml` | `0.12.0` | Bump → `0.13.0` | worker (gated) |
| `bindings/node/package.json` | `0.12.0` | Bump → `0.13.0` | worker (gated) |
| `README.md` | ~5 refs to `v0.12.0` (status line + milestone table + notes) | Update to `v0.13.0`, refresh feature summary | worker |
| `CLAUDE.md` | "Status: v0.12.0 (Jun 2026)" + M-milestone note | Bump status line to v0.13.0 + theme | worker |
| `docs/development/parity-release-checklist.md` | exists | Copy into release issue; check every box on release commit | release mgr |
| `docs/reports/cassandra-test-parity.md` | derived artifact | Confirm not stale (`cargo run -p cassandra-parity -- report --check` exits 0) | worker |
| Release notes / announcement | none | Draft from CHANGELOG `[v0.13.0]` | owner |

## Version-bump sites (single source of truth check)

All must move `0.12.0` → `0.13.0` together (mixing minor versions breaks trait
wiring per the workspace note):

- [ ] `Cargo.toml` — `[workspace.package] version` (line ~28) and the crate at line ~50
- [ ] `bindings/python/pyproject.toml` — `version` (line ~7)
- [ ] `bindings/node/package.json` — `"version"` (line ~3)
- [ ] Grep sweep after bump: `grep -rn '0\.12\.0' --include=*.toml --include=*.json --include=*.md .` returns only historical CHANGELOG/README entries, nothing live.

> ⚠️ These are **gated code changes** — they run through a worker + full
> `scripts/agent-gate.sh`, not hand-edited on the release commit without the gate.

## CHANGELOG finalization

- [ ] Owner reviews/trims the DRAFT `### Fixed` and `### Performance` entries
      (curated from the merge log since v0.12.0; marked with DRAFT HTML comments).
- [ ] Confirm the three BREAKING entries are complete and migration notes are correct
      (#1450 Python duration/time, #1710 schema honesty, #283 CLI YAML removal).
- [ ] Rename `## [Unreleased]` → `## [v0.13.0] - <DATE>` and open a fresh empty
      `## [Unreleased]` above it.

## Required green gates (release commit)

- [ ] Full `scripts/agent-gate.sh` PASS on the release commit (paste the SUMMARY block).
- [ ] **main is green on minimal/single-compression builds** — blocker `#1873` (P0) /
      `#1866` (P1) (`scan_merge.rs` dead-code under `-D warnings`). **Nothing ships over
      a red main.**
- [ ] CI-hygiene standing-red cleared or knowingly waived: `#1803` (python-bindings
      flaky venv), `#1928` (Python `test_abort_safety`), `#1896` (10 CLI failures),
      `#1860`/`#1859` (panic-not-skip), `#1776` (load-flake in gate).
- [ ] Parity gates green per [`parity-release-checklist.md`](./parity-release-checklist.md)
      (manifest lint, tier-contract-check, `required_parity`, recent `nightly_docker`,
      `exhaustive_regeneration` for the RC).

## Publish workflows (verify triggers before tagging)

- [ ] `.github/workflows/release.yml` — main release lane
- [ ] `.github/workflows/python-release.yml` — PyPI wheels (maturin `--profile release-unwind`, #1440)
- [ ] `.github/workflows/node-release.yml` — npm `@cqlite/node`
- [ ] `.github/workflows/trino-publish.yml` — Trino/Flight connector
- [ ] Tag `v0.13.0` created on the green release commit (no local tags exist yet;
      confirm the tag/versioning convention the publish lanes expect).

## Open in-flight lanes to resolve before the cut (as of 2026-07-04)

- **PR #1890** (#1582 D6 byte-budget) — owner-parked: `#1582` slug collision awaiting
  adjudication + red minimal build + ~21 commits behind. Decide in-or-out of 0.13.
- **PR #1936** (#1892 gate `--delta`) — parked; roborev left 1 Med + 2 Low. Tooling,
  not user-facing — likely out of the release-notes scope regardless.
- Active builds (no PR yet): #1625, #1632, #1763(P0). Cold/likely-abandoned locks to
  reap: #1750(P0), #1819(P1), #1585.

## NEEDS-YOU

1. **Manager board access / operating mode** — this remote session can't reach the
   Projects-v2 `Status` field, so Ready can't be fed from here (Path A: no label dispatch).
2. **#1582 collision adjudication** (PR #1890) — in or out of 0.13?
3. **CHANGELOG DRAFT curation** — confirm the release theme framing and trim entries.
