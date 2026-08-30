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
`exclude` list** — doing so breaks all three.

## The four binaries are not wired

No CI workflow, no script and no live doc invokes any of them. They are built only by an explicit
`--workspace`/`--all-targets` (notably the gate's clippy) or by the `-p` commands below — a bare
`cargo build` at the repo root compiles only the root `cqlite` package, so it never built them.
Measured on 16 cores with dependencies warm, these four binaries cost **51.1 s** of a
`cargo build --workspace --all-targets`, which is 82% of all three unwired crates' combined 62.6 s
(issue #1716). Deleting or feature-gating them is proposed as a follow-up there; #1716's own rules
do not authorize it (they permit deletion only for a tool that duplicates a live gate check).

```bash
cargo run -p format-validator --bin hex-analyzer -- --help    # or format-checker,
cargo build -p format-validator --bins                        # deviation-detector,
cargo test  -p format-validator                               # benchmark-validator
```

The crate is a workspace **member**, so the gate's
`cargo clippy --workspace --all-targets --all-features` lints every target here under
`-D warnings`. (Note the
crate-level `#![allow(clippy::all)]` in `src/lib.rs`, an old "EMERGENCY M1 FIX", suppresses most of
that within the lib; unrelated to #1716 and left alone.)

## What the binaries do

- **`hex-analyzer`** — format-aware hex dump of an SSTable component (magic numbers, structures,
  data layout).
- **`format-checker`** — format compliance check of an SSTable file.
- **`deviation-detector`** — flags deviations/anomalies against the expected Cassandra 5+ layout.
- **`benchmark-validator`** — benchmarks the validation operations themselves.

## Its tests run only when you touch it — and one was wrong for years

No CI job or gate component runs workspace-wide tests, so this crate's unit tests did not execute on
unrelated changes. But the agent gate's `--lite` blast-radius maps a touched path to its package, so
**editing anything in this directory (this README included) makes `--lite` run
`cargo test -p format-validator --lib`**.

On #1716 that happened for the first time and `utils::tests::test_hex_dump_formatting` **failed**:
it asserted `dump.contains("48656c6c6f")`, an unseparated hex run that `format_hex_dump` can never
emit for any input — it produces a conventional `hexdump -C` layout:

```text
00000000: 48 65 6c 6c 6f 2c 20 57  6f 72 6c 64 21 20 54 68  |Hello, World! Th|
```

The **test** was wrong, not the formatter, so the expectation was corrected (and tightened to also
pin the address prefix and the post-gap half of the line). Run `cargo test -p format-validator`
before touching anything here.

## Before you delete anything

The library is wired — deleting it breaks the gate. The binaries are retained deliberately: issue
#1716 permits deletion only for a tool that **duplicates a live gate check entirely**, and these
four are not wired into any gate lane. Batch-deletion of dead tooling is epic #1688 decision #9 —
take it there, not here.
