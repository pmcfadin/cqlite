# Tasks — parser fuzz crate (H1 safety net)

## 1. Feature-gated fuzz-support surface on cqlite-core
- [ ] 1.1 Add a `fuzz` feature to `cqlite-core/Cargo.toml` (no default deps beyond what the wrappers need). Surface: `cqlite-core` `[features] fuzz`.
- [ ] 1.2 Add `#[cfg(feature = "fuzz")] #[doc(hidden)] pub mod fuzz_support` re-exporting `parser::vint::{parse_vint, parse_vuint, parse_vint_length}` and thin driver wrappers: `fuzz_decode_value(type_str, bytes)`, `fuzz_block_emit(block)` (fixed `test_basic.simple_table` schema), `fuzz_bti_traverse(bytes)` (in-memory `Cursor`), `fuzz_cql_type(s)`, plus `schema::cql_parser::{parse_create_table, cql_type_to_type_id}`. No `unwrap()`/`expect()`; wrappers return `crate::Result<_>`. Surface: `cqlite_core::fuzz_support`.
- [ ] 1.3 Verify the default (no-`fuzz`) public API is unchanged: `cargo build -p cqlite-core` exposes no `fuzz_support`.

## 2. cargo-fuzz crate at fuzz/, excluded from the workspace
- [ ] 2.1 `cargo fuzz init` at repo root → `fuzz/` crate; add `fuzz` to root `Cargo.toml` `[workspace] exclude`. Confirm `cargo build --workspace` does NOT build `fuzz/`.
- [ ] 2.2 `fuzz/Cargo.toml` depends on `cqlite-core = { path = "../cqlite-core", features = ["fuzz", "all-compression"] }` + `libfuzzer-sys`.

## 3. Five fuzz targets (the tests) — never panic/hang/OOM
- [ ] 3.1 `fuzz_targets/fuzz_vint.rs` — feed bytes to `parse_vint`/`parse_vuint`/`parse_vint_length`; ignore `Result`. No panic on any input.
- [ ] 3.2 `fuzz_targets/fuzz_value_decode.rs` — for each type in a fixed list (every scalar + `list<int>`, `set<text>`, `map<text,int>`, tuple, `frozen<list<list<int>>>`) call `fuzz_decode_value(type, bytes)`; ignore `Result`.
- [ ] 3.3 `fuzz_targets/fuzz_block_emit.rs` — feed bytes as a decompressed block to `fuzz_block_emit`; ignore `Result`.
- [ ] 3.4 `fuzz_targets/fuzz_bti.rs` — feed bytes to `fuzz_bti_traverse` (Cursor-backed node decode + DFS); ignore `Result`. Must not seek past EOF into a panic.
- [ ] 3.5 `fuzz_targets/fuzz_schema_parse.rs` — feed bytes as a UTF-8-lossy string to `parse_create_table` / `fuzz_cql_type` / `cql_type_to_type_id`; deep nesting returns `Err` via the #1690 depth guard, never stack-overflows. (Queued from #1690 / PR #1739.)
- [ ] 3.6 Confirm no target body contains `assert!`/`unwrap()`/`expect()` that can panic on a valid `Err`.

## 4. Seed corpora (committed, real-derived, small)
- [ ] 4.1 Copy small real component bytes from `test-data/datasets/sstables/` into `fuzz/corpus/<target>/` (Data.db chunk → block_emit; BTI component → bti; short byte slices → vint/value_decode; `CREATE TABLE` strings → schema_parse). Keep each seed a few KB.
- [ ] 4.2 Commit the corpus (force-add if the source is gitignored); verify present in a fresh `git worktree add --detach HEAD` checkout.

## 5. CI + local smoke, isolated from the stable gate
- [ ] 5.1 `.github/workflows/fuzz.yml`: PR smoke (each target `-max_total_time≈30-60 -rss_limit_mb=2048 -timeout=25`) + nightly `schedule`/`workflow_dispatch` long-run; install nightly + cargo-fuzz; upload crash reproducer artifact on failure. Style follows existing workflows.
- [ ] 5.2 Local bounded smoke helper (e.g. `fuzz/smoke.sh`) running the same bounded invocation; NOT wired into `scripts/agent-gate.sh` (needs nightly).
- [ ] 5.3 Run each target locally on nightly for a bounded time; if a crash is found, file a SEPARATE bug issue with the reproducer (do not silently patch here unless a one-line guard).

## 6. Doctrine
- [ ] 6.1 Add a short "Fuzzing" note to CLAUDE.md (where `fuzz/` lives, how to run a target, that it is nightly + out-of-gate) and mirror on the `agents-developing/` site source.

## 7. Quality gates
- [ ] 7.1 `scripts/agent-gate.sh` PASS (paste AGENT-GATE SUMMARY). Run with `CQLITE_DATASETS_ROOT` at the main repo's `test-data/datasets`; confirm the gate is byte-unaffected by the excluded fuzz crate.
- [ ] 7.2 `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features` clean; no `unwrap()`/`expect()` in cqlite-core library code (fuzz-support wrappers included).
- [ ] 7.3 Intent audit **C** (spec-auditor anchored to `openspec/changes/parser-fuzz-crate/specs/**`) PASS.
- [ ] 7.4 roborev (`--agent codex --base origin/main`) clean.
