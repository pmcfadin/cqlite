# `format-validator` — a WIRED library plus four manual dev BINARIES

This crate is **split-status**, and the split is the whole point of this file. Verified by census in
issue #1716 (epic #1688, audit finding AK5):

| target | status |
|---|---|
| the **library** (`format_validator`) | **WIRED — do not remove** |
| the four **binaries** (`hex-analyzer`, `format-checker`, `deviation-detector`, `benchmark-validator`) | **manual dev tools, NOT CI-wired** |

## The library is wired — three live consumers

1. **`tests/format-compatibility`** (package `format-compatibility-tests`) path-depends on this
   crate, and `tests/format-compatibility/tests/oa_format_compliance.rs` uses
   `format_validator::format_constants` and `format_validator::utils`. That package is its own
   **full agent-gate component** (`format-compat`), so this library is on the gate's critical path.
2. **`scripts/tests/test_agent_gate_summary.sh`** uses `tools/format-validator/src/lib.rs` as the
   fixture for the gate's path → package **"owners"** resolution (which reads `cargo metadata`).
3. **`xtask/src/oom_audit/scope.rs`** asserts on a `tools/format-validator/` path.

**Therefore this crate must stay a workspace member and must NOT be added to the workspace
`exclude` list** — doing so breaks all three. See the `default-members` comment in the root
`Cargo.toml`.

## The four binaries are not wired

No CI workflow, no script and no live doc invokes any of them. So the crate is **not** in
`default-members`: a bare `cargo build` at the repo root no longer compiles the four binaries,
while the library still builds as `format-compatibility-tests`' dependency — which is exactly the
coverage the `format-compat` gate component needs.

```bash
cargo run -p format-validator --bin hex-analyzer -- --help    # or format-checker,
cargo build -p format-validator --bins                        # deviation-detector,
cargo test  -p format-validator                               # benchmark-validator
```

The crate is still a workspace **member**, so the gate's
`cargo clippy --workspace --all-targets --all-features` lints every target here under
`-D warnings` — dropping out of `default-members` costs build time, never lint coverage. (Note the
crate-level `#![allow(clippy::all)]` in `src/lib.rs`, an old "EMERGENCY M1 FIX", suppresses most of
that within the lib; unrelated to #1716 and left alone.)

## What the binaries do

- **`hex-analyzer`** — format-aware hex dump of an SSTable component (magic numbers, structures,
  data layout).
- **`format-checker`** — format compliance check of an SSTable file.
- **`deviation-detector`** — flags deviations/anomalies against the expected Cassandra 5+ layout.
- **`benchmark-validator`** — benchmarks the validation operations themselves.

## Before you delete anything

The library is wired — deleting it breaks the gate. The binaries are retained deliberately: issue
#1716 permits deletion only for a tool that **duplicates a live gate check entirely**, and these
four are not wired into any gate lane. Batch-deletion of dead tooling is epic #1688 decision #9 —
take it there, not here.
