# Repository Guidelines

## Project Structure & Module Organization
CQLite is a Rust workspace anchored by `Cargo.toml` with crates: `cqlite-core` (engine), `cqlite-cli` (tooling), and `cqlite-ffi`/`cqlite-wasm` for bindings. Examples and scripts live under `examples/` and `tools/`. Integration fixtures sit in `tests/`, with reusable assets in `tests/fixtures` and larger Cassandra reference sets in `test-data/` and `real_cassandra5_data/`. Docs, design notes, and architecture records live in `docs/`.

## Build, Test, and Development Commands
Prefer the `just` recipes: `just build` compiles all targets, `just test` runs the workspace suite, and `just check` enforces fmt, Clippy, and security audit gates. Use `just wasm` or `just ffi` when editing those bindings. For quick loops, run `cargo build --workspace`, `cargo test --package <crate>`, or `cargo run --package cqlite-cli -- parse <sstable>`. `just watch` keeps a rebuild loop running.

## Coding Style & Naming Conventions
Formatting is governed by `.rustfmt.toml` (4 spaces, 100 columns). Follow Rust idioms: `snake_case` modules, `CamelCase` types, lower_snake identifiers. Keep imports explicit, document tricky flows, and gate experimental APIs with features. Run `cargo fmt --all` and ensure `cargo clippy --all-targets --all-features` is clean; several lints are `deny` in the workspace manifest.

## Testing Guidelines
Unit tests should reside near the code they cover; cross-crate, regression, and data-driven tests belong in the `tests/` workspace crate (fixtures documented in `tests/README.md`). Name new files `*_test.rs` to match existing conventions. Execute `cargo test --workspace --all-features` before submitting, lean on `just test-core` or similar recipes while iterating, and collect coverage with `just test-coverage` when changes materially affect parsing paths.

## Commit & Pull Request Guidelines
Commits follow a Conventional Commit prefix (`feat:`, `fix:`, `chore:`) with optional issue references (`#123`). Keep changes focused and include docs or schema updates when behaviour shifts. Before requesting review, run `just check` plus any targeted tests, describe the validation steps in the PR body, and attach logs or screenshots if CLI output changes.

## Security & Data Handling
Use the bundled Cassandra datasets for local validation; sanitize any external SSTables before sharing. Generated fixtures should stay in `test-data/` with short provenance notes. When touching FFI or WASM code paths, run `just ffi` or `just wasm` in a clean shell and inspect outputs for unintended symbols or credentials.
