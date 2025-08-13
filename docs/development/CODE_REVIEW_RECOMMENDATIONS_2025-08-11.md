### Code Review Recommendations — 2025-08-11

- **Clarity first**: Prefer descriptive names, early returns, guard clauses; avoid deep nesting (>3 levels).
- **Lints and warnings**: Keep the tree warning-free. Run clippy with `clippy::all`, `clippy::pedantic` as guidance; fix or justify.
- **Error handling**: No `unwrap`/`expect` in library code. Use `thiserror` for typed errors, add context on edges with `anyhow`.
- **API boundaries**: Public types and functions must have docs; keep APIs minimal, stable, and feature-gated where appropriate.
- **Concurrency/async**: Avoid blocking in async paths; validate `Send`/`Sync` and `!Send` boundaries; scope Tokio features precisely.
- **Testing**: Require unit + integration; add property tests where feasible; tests must be deterministic and fast.
- **Performance**: Use `criterion` for hot paths; watch allocations, clones, and unnecessary sync; measure before optimizing.
- **Dependencies**: Minimize and pin via workspace; limit features; run `cargo audit`/`cargo deny`; prefer well-maintained crates.
- **Logging/telemetry**: Prefer `tracing` with structured fields; avoid noisy logs; centralize initialization.
- **CI gates**: Enforce `fmt --check`, `clippy -D warnings`, `test --all-features`, audit/deny, and coverage on critical paths.

### Project-specific recommendations

- **Root crate cleanup**: Either formalize a top-level crate (add `[package]` and features to gate re-exports) or remove `src/lib.rs` at the repository root to avoid confusion.
- **Unify editions**: Ensure all crates, including `tools/*`, inherit `edition.workspace = true` (Rust 2024) and consistent `rust-version`.
- **Toolchain pinning**: Add `rust-toolchain.toml` with stable channel and components `clippy`/`rustfmt`; include `wasm32-unknown-unknown` target.
- **Formatter/Linter**: Add `.rustfmt.toml`; enforce `cargo fmt --all --check` and `cargo clippy --all-targets --all-features -D warnings` in CI.
- **Lint sections fix**: Keep rustc lints under `[workspace.lints.rust]` and clippy lints under `[workspace.lints.clippy]`; avoid mixing keys like `dead_code` under clippy.
- **Dependency unification**: Use workspace deps consistently (e.g., `clap = 4.5`, `chrono = 0.4`). Remove duplicate direct versions in member crates; prefer `workspace = true`.
- **Tokio features**: Avoid `tokio` "full" at workspace level; enable only per-crate features actually used.
- **Compression features**: Make compression crates optional and feature-gated (`lz4_flex`, `snap`, `flate2`, `zstd`), with clear default feature sets.
- **Logging**: Prefer `tracing` across crates; capture `log` via `tracing-log` if needed; avoid double-initialization with `env_logger`.
- **FFI header generation**: Add `build.rs` in `cqlite-ffi` to run `cbindgen` and emit headers, or remove the build-dependency and provide a script.
- **Licensing polish**: For dual-license, include `LICENSE-MIT` and `LICENSE-APACHE` alongside `license = "MIT OR Apache-2.0"`.
- **Security/Compliance**: Add `cargo-audit` and `cargo-deny` to CI; maintain `deny.toml` for licensing and vulnerability policy.
- **WASM packaging**: Add `package.metadata.wasm-pack` and CI job to run `wasm-pack build --target web` for `cqlite-wasm`.

