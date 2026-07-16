# Tasks — xtask static audit for no-unbounded-materialization (#2012)

One branch `issue-2012-oom-audit-xtask`, staged commits. Each task names the surface it exercises and
carries a test. Anchors are `main`-relative and will drift; re-grep before editing. Seam 1 (owner
approval of proposal + design, incl. the four open forks in design.md §"Open forks") precedes all
implementation.

## Stage 0 — crate skeleton + self-test fixtures (red first)
- [ ] 0.1 Add the `xtask` crate: `[workspace] members` entry, `Cargo.toml` (deps `syn` full+visit,
  `quote`, `walkdir`, `toml`; no cqlite deps), `main.rs` with a `clap`-free arg parse for
  `oom-audit [--enforce]`. Surface: `cargo run -p xtask -- oom-audit`. (oom-audit-xtask)
- [ ] 0.2 Add self-test fixtures under `xtask/tests/fixtures/`: `violating.rs.txt` (unbounded
  `collect::<Vec<_>>()` on a scan-shaped fn) and `bounded.rs.txt` (same body with a `ResultBudget`/
  `.take(limit)`). Fixture-driven unit test asserts violating→finding, bounded→none. (oom-audit-xtask)

## Stage 1 — the STREAM_RETURNS_VEC rule (design §B)
- [ ] 1.1 Implement the `syn`-visitor rule: per in-scope fn, detect the collect/push shape over a
  row/partition/cell iterator with no in-scope bound; conservative type-fragment allowlist; fire only
  when shape + iterator-type are syntactically visible. Unit-pinned by the Stage-0 fixtures + the
  renamed-helper scenario. Surface: `xtask oom-audit` finding set. (oom-audit-xtask)
- [ ] 1.2 Path-scope enforcement: only `.rs` under the committed scope roots are parsed/reported;
  out-of-scope file with the shape → no finding (scenario). (oom-audit-xtask)

## Stage 2 — allowlist machinery (design §C)
- [ ] 2.1 Content-fingerprint (quote-normalized token hash) + TOML allowlist load/validate: suppress on
  fingerprint match; FAIL on orphan / missing `issue` / missing `justification` / past `expiry`.
  Unit-pinned per scenario. Surface: the allowlist TOML + `oom-audit`. (oom-audit-xtask)

## Stage 3 — modes + seeding (design §D, §E)
- [ ] 3.1 report-only (exit 0 always) vs `--enforce` (non-zero on any failing condition); self-test
  fixture drives the enforce exit-code scenarios. (oom-audit-xtask)
- [ ] 3.2 Run report-only over the v1 scope; triage every hit; seed the committed allowlist for
  reviewed-sound sites (issue + justification each). Report clean before Stage 4. (oom-audit-xtask)

## Stage 4 — gate wiring + docs (design §D)
- [ ] 4.1 Add `run_oom_audit` (SKIP-aware, `delivery-telemetry` template) to `scripts/agent-gate.sh`:
  `COMPONENTS` entry, dispatch `case`, NOT in `DATASET_COMPONENTS`; SKIP if `xtask` can't build, FAIL on
  violation. Self-test the SKIP + FAIL paths (`scripts/tests/`). (oom-audit-xtask)
- [ ] 4.2 Flip the component to `--enforce`; confirm green over the seeded allowlist. (oom-audit-xtask)
- [ ] 4.3 Docs: one-line `oom-audit` row in the CLAUDE.md gate table + `docs/development/gate-ops.md`
  (or the gate-contract website page); note the allowlist policy (issue + justification mandatory,
  expiry per fork F-expiry). (oom-audit-xtask)

## Stage 5 — gate + close
- [ ] 5.1 `scripts/agent-gate.sh` full PASS (SUMMARY recorded; `oom-audit` PASS). rust-reviewer +
  roborev on the lite-green diff; C PASS (every requirement satisfied by a self-test/public-surface
  test). (oom-audit-xtask)
- [ ] 5.2 `openspec archive`. (oom-audit-xtask)
