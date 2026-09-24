# Tasks — vortex-export (issue #4237)

Ordered. Group 0 is premises, 1 the shared batch producer, 2 the core writer, 3 the CLI wiring, 4
gate wiring, 5 docs, 6 the endgame. Commit after every group (#3042 work-loss insurance). `--lite`
after every fix round; ONE full gate in `flow-closer`.

## 0. Premises — re-confirm in the worktree, STOP and report if false

- [ ] 0.1 Confirm `origin/main` HEAD has the arrow 59 rev merged (#4236/#4242): `arrow = { version
      = "59", ... }`, `rust-version = "1.95"` in root `Cargo.toml`. (Already verified at
      `79db00db7` during activation — re-verify after rebasing onto latest `origin/main` before
      implementation starts, since #4236 landed only 3 days before this activation.)
- [ ] 0.2 Confirm `vortex` and `vortex-arrow` publish MSRV/version compatibility still matches the
      design doc's pin (`=0.86.1`) — `cargo info vortex@0.86.1` or crates.io, since a few days may
      have passed since the 2026-09-18 verification.
- [ ] 0.3 Confirm `arrow_convert::rows_to_record_batch` / `rows_to_record_batch_with_schema`
      signatures (surface: `cqlite-core/src/export/arrow_convert.rs`) — the shared producer (D1)
      wraps these; note their exact return type (`RecordBatch`) and error type
      (`ArrowConvertError`) before writing the adapter.
- [ ] 0.4 Confirm the exact `ExportFormat` match sites and their current arm bodies in
      `cqlite-cli/src/cli.rs`, `commands/export.rs`, `commands/export_sstable.rs`,
      `commands/read_sstable.rs` (already located during spec drafting: `export.rs` lines
      ~111/192/411 hold `Parquet` arms; `export_sstable.rs` lines ~75-83; `read_sstable.rs` line
      ~140 the rejection). Re-confirm line numbers are stable after rebase.
- [ ] 0.5 Confirm the 33 fixture table list and dataset layout (`test-data/scripts/fetch-datasets.sh
      --verify-only`) so R9's per-table loop has a concrete source of table names.

## 1. Shared Arrow batch producer — surface: `cqlite_core::export` (D1)

- [ ] 1.1 Add the shared `RecordBatch`-per-chunk adapter (naming per D1) in
      `cqlite-core/src/export/` (new small file or a function added to `arrow_convert.rs` if it
      stays under the campsite line threshold) that both the Parquet and Vortex call sites use.
      Parquet's existing internal batch construction is refactored to call it — no change to
      `arrow_convert`'s conversion logic itself.
- [ ] 1.2 Unit test: R1.1 (both writers receive the same batch), R1.2 (no Vortex-specific CQL→Arrow
      `match`).
- [ ] 1.3 `--lite`; commit.

## 2. Core Vortex writer — surface: `cqlite_core::export::vortex` (R2–R5)

- [ ] 2.1 `cqlite-core/Cargo.toml`: `vortex = ["arrow", "dep:vortex"]` feature; `vortex` optional
      dependency with `default-features = false, features = ["vortex-file"]` (vortex-cloud,
      vortex-tensor OFF), pinned `=0.86.1`.
- [ ] 2.2 `cqlite-core/src/export/vortex.rs`: `VortexExportOptions`, `VortexExportError`
      (`thiserror`, batch-index + column context per R4.2), `write_vortex(...)` consuming the
      shared producer's batches through
      `session.arrow().from_arrow_record_batch(...)` → `session.write_options().write(...)`.
      Chunked shape mirrors `create_streaming_parquet_writer`/`write_chunk`/`finalize` (one batch
      resident at a time).
- [ ] 2.3 `pub mod vortex;` declared unconditionally at `#[cfg(feature = "vortex")]` in
      `export/mod.rs` per R5.2 / the pub-surface guard (#1712) — verify no inner
      `#![cfg(feature = "vortex")]` duplicate inside `vortex.rs`.
- [ ] 2.4 Fail-closed path: write-to-temp-then-rename (or delete-on-error), tested per R4.1.
- [ ] 2.5 UUID extension unit test (R2.1) at the writer level if feasible without a full fixture
      read-back (a synthetic batch with `arrow.uuid` metadata is enough for THIS unit test; R9's CLI
      differential is the fixture-backed confirmation).
- [ ] 2.6 `--lite`; commit.

## 3. CLI wiring — surface: `cqlite-cli` (R6–R8)

- [ ] 3.1 `cqlite-cli/Cargo.toml`: `vortex = ["cqlite-core/vortex"]` feature (forwarding only, no
      bundling into `state_machine` or any other default-adjacent feature — deliberate, see
      proposal's Non-goals framing: strictly opt-in like `parquet`).
- [ ] 3.2 `ExportFormat::Vortex` added to the enum in `cli.rs` (`Display` arm too); every existing
      `match format { ... }` in `export.rs`, `export_sstable.rs`, `read_sstable.rs` gains an
      explicit arm — confirm the compiler enforces exhaustiveness (no `_ =>` anywhere in those
      matches) as the design's own check.
- [ ] 3.3 `export.rs`: `ExportFormat::Vortex` arm — require `--out`/`--output` (R6.2), call the
      shared producer + `write_vortex`.
- [ ] 3.4 `export_sstable.rs`: `ExportFormat::Vortex` arm on the `export_sstable` LIBRARY function
      (not a CLI verb — lead ruling 2026-09-24, R7), mirroring the `Parquet` arm shape.
- [ ] 3.5 `read_sstable.rs`: reject `vortex` with the R8.1 message shape.
- [ ] 3.6 CLI-level tests: R6.1, R6.2, R8.1 (`cqlite-cli/tests/`, named `--test` targets with
      `required-features = ["vortex"]` or `["parquet", "vortex"]` as each needs). R7.1 is a
      LIBRARY-level test instead (`test_export_sstable_to_vortex` in
      `export_integration_tests.rs`), mirroring `test_export_sstable_to_parquet` exactly.
- [ ] 3.7 `--lite`; commit.

## 4. Cross-format differential + gate wiring (R9–R11, design.md D3)

- [ ] 4.1 `cqlite-cli/tests/issue_4237_vortex_parquet_differential.rs`,
      `required-features = ["parquet", "vortex"]`: loop over the 33 fixture tables (R9.1), export
      both formats, read both back to Arrow, assert full-column-set equality both directions
      (#3890 shape — reuse the `issue_3890_point_read_column_parity_sweep.rs` pattern for asserting
      both directions, not its subject matter). Split into a sibling
      `issue_4237_vortex_type_coverage.rs` for the per-CQL-type-family cases (R2.2/R2.3) if the
      first file approaches the ~1500-line campsite threshold.
- [ ] 4.2 `scripts/agent-gate.sh` (owner Seam-1 ruling 2026-09-23: slim gate wiring — see design.md
      D3): add `feature-iso-vortex` (R11.1) as **compile-only**, mirroring `feature-iso-parquet`'s
      existing form exactly (`--no-run`, no execution), and `vortex-parquet-differential` (R11.2) —
      the SOLE new executing component, running both `cargo test -p cqlite-core --features
      parquet,vortex --lib` (writer unit tests) and the CLI-level fixture-required differential
      target — to `COMPONENTS=(...)`, `DATASET_COMPONENTS`, and the dispatch `case` — following the
      exact pattern of the #1699 / #3453 prior additions (grep those issues' landed diffs for the
      touch-point list: `COMPONENTS`, dispatch `case`, cost-model `case`
      (`printf 'cargo'`/`'libtest'`), and the summary-line emission).
- [ ] 4.3 Verify `features-load-bearing` recognizes `vortex` as load-bearing (R11.3) — it has real
      `cfg` sites once 2.3/3.2 land; no special-casing needed if the guard's derivation is followed
      correctly.
- [ ] 4.4 Verify `dep-duplicates` after adding the `vortex` dependency —
      `bash scripts/ci/check-dep-duplicates.sh --regenerate` ONLY if a genuine new duplicate
      appears that the baseline should record; otherwise expect `PASS` given #4236 already
      unified the arrow tree at 59.2.
- [ ] 4.5 `--lite`; commit.

## 5. Docs (R12)

- [ ] 5.1 `website/src/content/docs/user-docs/output-formats.md`: new `## \`vortex\`` section
      (R12.1).
- [ ] 5.2 `website/src/content/docs/user-docs/cli-reference.md`: `--format vortex` entry.
- [ ] 5.3 `docs/development/dev-cookbook.md`: `--features vortex` build/test recipe (R12.2).
- [ ] 5.4 `README.md` formats table: add Vortex row.
- [ ] 5.5 Release notes draft entry naming Vortex export + arrow 59 + Rust 1.95 (the 0.18 release
      notes file, once it exists / is being assembled — coordinate with whichever issue owns the
      0.18 notes doc rather than creating a new one here).
- [ ] 5.6 `--lite`; commit.

## 6. Endgame

- [ ] 6.1 Open PR.
- [ ] 6.2 `rust-reviewer` + `roborev-review.sh --agent claude-code --model claude-opus-5` on the
      lite-green diff (review-first, before the full gate).
- [ ] 6.3 Fix rounds: `--lite` re-cert + diff-scoped targets only — never a full gate per round.
- [ ] 6.4 Hand to `flow-closer`: rebase → ONE full gate of record
      (`AGENT_GATE_SUMMARY_FILE=/tmp/gate-summary.txt bash scripts/agent-gate.sh`) → C intent audit
      (`spec-auditor` against `openspec/changes/vortex-export/specs/**`) → roborev LAST → arm
      `gh pr merge --auto --squash --delete-branch` → `flow-finalize` (archive the OpenSpec change,
      remove the worktree, close #4237).
