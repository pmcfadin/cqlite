# Design — parser fuzz crate (H1 safety net)

## Context

The decode entry points the targets must reach have mixed visibility from outside `cqlite-core`:

| Target | Entry point(s) | Reachable from an external crate today? |
|--------|----------------|------------------------------------------|
| `fuzz_vint` | `parser::parse_vint` / `parse_vuint` / `parse_vint_length` | **Yes** — `pub mod parser` + `pub use vint::*` |
| `fuzz_value_decode` | schema-typed value decode (`parse_value_with_comparator` is `pub`; the per-type `parse_*_value` are `pub(crate)`) | **No** — the module chain / items are `pub(crate)` |
| `fuzz_block_emit` | `parse_block_emit` (`.../row_decoder/block_emit.rs`) | **No** — `mod row_decoder` is private |
| `fuzz_bti` | `iterate_partitions_in_bti_file` + `node_decode` | **No** — module chain is private |
| `fuzz_schema_parse` | `schema::cql_parser::{parse_create_table, cql_type_to_type_id}`; `cql_type` is `fn` (private) | Partial |

So the crate cannot simply `use cqlite_core::…` for four of the five targets.

## Decision 1 — reach internals via a feature-gated `fuzz` surface, not by widening the public API

Add a `fuzz` **feature** to `cqlite-core` that gates a single `#[doc(hidden)] pub mod fuzz_support`
(only compiled under `--features fuzz`). It re-exports the exact entry points each target needs, e.g.:

```rust
#[cfg(feature = "fuzz")]
#[doc(hidden)]
pub mod fuzz_support {
    pub use crate::parser::vint::{parse_vint, parse_vint_length, parse_vuint};
    // thin wrappers that decode arbitrary bytes at a fixed set of types / one fixed schema:
    pub fn fuzz_decode_value(type_str: &str, bytes: &[u8]) -> crate::Result<crate::types::Value>;
    pub fn fuzz_block_emit(block: &[u8]) -> crate::Result<()>;   // fixed simple_table schema
    pub fn fuzz_bti_traverse(bytes: &[u8]) -> crate::Result<()>; // in-memory Cursor over bytes
    pub use crate::schema::cql_parser::{cql_type_to_type_id, parse_create_table};
    pub fn fuzz_cql_type(s: &str) -> crate::Result<()>;          // exercises the private `cql_type`
}
```

**Why:** the default public API and every gate/CI build stay byte-identical (the module does not exist
without `--features fuzz`). The thin wrappers keep the internal parsers `pub(crate)` — we expose
*fuzz drivers*, not the parsers themselves. This is the smallest, most honest surface: the wrapper is
the documented "public surface" that exercises the internal decode (wiring-evidence), and the fuzz
target is the end-to-end test of it.

**Alternatives rejected:**
- *Make every parser `pub`* — permanently widens the API for a test-only need; violates the campsite
  intent and risks accidental external reliance on unstable internals.
- *Put the targets inside `cqlite-core` as `#[test]`s driven by `arbitrary`* — not real fuzzing (no
  coverage-guided mutation, no `-rss_limit_mb`/`-timeout` hang/OOM enforcement); the issue asks for a
  cargo-fuzz crate specifically.

## Decision 2 — the crate lives at `fuzz/`, excluded from the workspace

`cargo fuzz init` creates `fuzz/` with its own `Cargo.toml` (its own lockfile-independent build,
`cargo-fuzz` metadata, `[[bin]]` per target). Add `fuzz` to the root `Cargo.toml`
`[workspace] exclude` (cargo-fuzz usually does this, but assert it) so `cargo build`/`clippy`/the gate
never compile it. The crate depends on `cqlite-core = { path = "../cqlite-core", features = ["fuzz", "all-compression"] }`.
Fuzzing requires **nightly** (`-Z sanitizer` / libFuzzer); the stable gate never touches it.

## Decision 3 — the contract is a harness invariant, asserted by construction

Each `fuzz_target!(|data: &[u8]| { … })` body calls the entry point and **ignores the `Result`** (both
`Ok` and `Err` are acceptable). The invariant "never panic" is enforced by libFuzzer aborting on any
panic/abort; "never hang" by `-timeout=<n>`; "never OOM" by `-rss_limit_mb=<n>`. The target body must
contain **no `assert!`/`unwrap()`/`expect()`** that could itself panic on a valid `Err` — a decode
error is a *pass*, not a finding. `fuzz_value_decode` iterates its fixed type list; `fuzz_schema_parse`
feeds the bytes as a UTF-8-lossy string (arbitrary strings, not just valid UTF-8).

## Decision 4 — seed corpora from real, tiny component files

Copy small real files from `test-data/datasets/sstables/` into `fuzz/corpus/<target>/`:
a Data.db chunk for `fuzz_block_emit`, a BTI/Rows/Partitions component for `fuzz_bti`, a `Statistics.db`
or short byte slices for `fuzz_vint`/`fuzz_value_decode`, and a handful of `CREATE TABLE` strings for
`fuzz_schema_parse`. Keep each seed small (the corpus is committed) — a few KB per target, not whole
datasets. Force-add if any candidate is gitignored.

## Decision 5 — CI: bounded PR smoke + nightly long-run, isolated workflow

New `.github/workflows/fuzz.yml` (style follows the existing gate/CI workflows):
- **PR smoke** (`pull_request`): install nightly + `cargo-fuzz`, run each target
  `-max_total_time=<~30-60s> -rss_limit_mb=2048 -timeout=25`. Bounded so PRs never hang. A crash fails
  the job with the reproducer as an artifact.
- **Nightly** (`schedule` + `workflow_dispatch`): a long per-target budget; uploads any crash artifact.

A local, gate-independent smoke script (e.g. `fuzz/smoke.sh` or a `just`/`make` recipe) runs the same
bounded invocation for humans. This is *not* wired into `scripts/agent-gate.sh` (needs nightly; keeps
the stable gate untouched) — the gate still fully builds `cqlite-core --features fuzz` implicitly only
if someone opts in; by default the gate is unchanged.

## Risk / edge cases

- **`fuzz_bti` needs a `Read + Seek`** — wrap `&[u8]` in `std::io::Cursor`; the traversal must not seek
  past EOF into a panic (bounded by `Err`).
- **`fuzz_schema_parse` arbitrary strings** — must generalize #1690's max-depth-32 guard: deeply nested
  `frozen<frozen<…>>` returns `Err`, never stack-overflows.
- **False "hang"** on a pathological-but-legit input — tune `-timeout`/`-rss_limit_mb`; a genuine hang
  is a finding to file, not to paper over.
- The fuzz-support wrappers are the *only* new public-ish surface; they are `#[doc(hidden)]` and
  feature-gated, so they never appear in normal docs or the stable API.
