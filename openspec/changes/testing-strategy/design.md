# Design: CQLite testing strategy (issue #4259)

## Context

Four constraints shape every decision below:

1. **nextest is process-per-test.** The gate's `core-tests` component runs under `cargo nextest run`
   (`.config/nextest.toml`, #1737). Any "build once, share across rows" mechanism that lives in process
   memory (`LazyLock`, `OnceLock`) rebuilds in every test process and shares nothing.
2. **Every `tests/*.rs` is its own linked binary.** 417 in `cqlite-core/tests/` alone. Link time and
   `target/debug/deps` growth are a real, already-observed cost (CLAUDE.md `--lite` row).
3. **Features must be load-bearing** (#1698) and the dep-duplicates ratchet (#1700) watches new
   dependencies — prefer small in-repo code over new crates.
4. **Owner rulings (2026-09-22):** contracts now / generator later; new-work rule + measure before
   migrating; mechanize only cheap, low-false-positive checks; randomized oracle = self-consistency
   nightly + Cassandra weekly; adoption must not back up CI.

## Layer model

| Layer | Question it answers | Tier | Enforcement |
|---|---|---|---|
| Matrix | Does every relevant axis combination behave? | PR | Review + case floors |
| Named anchor | Does an invariant the compare cannot see hold? | PR | Review (header states reason) |
| Differential | Does the new path equal the old path — and did it actually run? | PR | **Type** (cannot compile without outcome) + runtime engagement assert |
| Randomized | What did no human think to write? | Nightly / weekly | Harness; issues, never merge blocks |
| Tier + budget | What may each lane cost? | All | **Gate FAIL** on budget breach |

## Decisions

### D1 — Tiers are nextest profiles, selected by binary-name prefix

**Chosen:** `.config/nextest.toml` gains `pr` (= today's default behaviour), `nightly`, `weekly`
profiles. Tier membership is by **test-binary name prefix**: `burn_*` binaries are excluded from the
`pr` profile's default filterset and included by `nightly`/`weekly`; weekly-only cases additionally
require `CQLITE_TIER=weekly`. The randomized harness refuses to draw a fresh seed unless `CQLITE_TIER`
is `nightly` or `weekly`.

**Beat:** (a) a committed per-target registry file — 775 entries to maintain, drifts; (b) `#[ignore]` —
already overloaded in this repo for perf asserts (`perf-gate-allow`) and invisible in counts;
(c) cargo features — would need to be load-bearing and multiply the clippy matrix.

### D2 — Fixture-once via a content-addressed disk cache

**Chosen:** `tests/support/fixture_cache.rs` builds a fixture for a **fixture-axis tuple**
(format, compression, shape, generator version) into
`$CARGO_TARGET_TMPDIR/cqlite-fixtures/<sha256(tuple)>/`, built in a sibling temp dir and published with
an atomic `rename`; concurrent builders race harmlessly (loser discards). Rows open the directory
read-only. The key includes a generator version constant so a change to fixture construction
invalidates the cache instead of reading stale bytes.

**Beat:** (a) `LazyLock` per binary — defeated by nextest (constraint 1); (b) one `#[test]` that loops
all rows — loses per-row reporting, retries and filtering in nextest; (c) committing generated fixtures
— repo bloat, and CQLite-written fixtures are not an oracle anyway (#3042).

### D3 — Matrix rows: pairwise generator in-repo, named full-product pairs

**Chosen:** `tests/support/matrix.rs` provides a deterministic greedy 2-wise covering-array builder
(IPOG-style, a few hundred lines, no dependency) plus a `full_product(axis_a, axis_b)` escape. Illegal
combinations are pruned by a predicate in the builder, never in the test body. Each matrix module header
names its full-product pairs; the builder's own tests prove every pair is covered. Rows are materialised
as a generated case list (one nextest test per row via a macro over the list) asserted non-empty with a committed case floor.

**Beat:** a covering-array crate (new dep, ratchet); full Cartesian product (thousands of rows).

**Initial named interacting pairs** (from evidence): direction × format; direction × compression
(#1293); clustering-slice × wide partition (BTI `Rows.db`, #3002).

### D4 — The differential contract is a type

**Chosen:** `tests/support/differential.rs` defines

```rust
pub enum ExpectedOutcome {
    Served,                       // new path engaged on every probe
    SupportedUnserved,            // accepted, but nothing for the new path to open
    GatedFallback(FallbackReason) // sanctioned carve-out, exact reason
}
pub struct DifferentialCase { /* fixture, query, */ pub expected: ExpectedOutcome }
```

with no `Default` and no constructor that omits `expected`, so a case cannot compile without a declared
outcome. The runner (1) invalidates process-global read caches (`GlobalKeyOffsetCache::invalidate_all`,
already public) before the new-path leg, (2) runs new path then old path, (3) reads
`access_path::last()` / `metadata.access_path` after each new-path query and asserts it matches the
declared outcome — a `Served` case with a non-targeted path FAILs naming table + key.

**Beat:** doc convention (not enforceable); auto-detecting the outcome from the result (turns a silent
fallback green — exactly the failure class).

### D5 — Non-vacuousness via a debug-only fault seam

**Chosen:** a debug-assertions-only env seam (precedent: `CQLITE_TTL_NOW_OVERRIDE_SECS`,
`now_clock.rs`) that perturbs one decoded cell on the path under test (e.g. `CQLITE_FAULT_INJECT=
point_read:first_cell`). Each differential suite has one meta-test that sets it and asserts the suite's
**own** check fails (the runner returns `Result`; the meta-test asserts `Err` from the real runner on real fixtures, not a synthetic diff). Release builds
compile the seam out.

**Beat:** `cargo-mutants` in the gate (slow, non-goal); a feature flag (must be load-bearing, widens the
clippy matrix); comparator-only self-tests (what exists today — proves the compare, not the path).

### D6 — Budgets are measured, reported and enforced by the gate

**Chosen:** the gate derives per-binary wall time from nextest's JUnit output and prints a
`test-budget:` SUMMARY line (tier total, top-5 slowest binaries, budget). Budgets are committed in
`scripts/ci/test-budgets.txt`, initialised from a measured baseline + 15% headroom (task 5). Over budget
→ FAIL naming the binary and the remedies (move rows to nightly; shard; use `fixture_cache`). Raising a
budget is a reviewed diff to that file, never an env override.

**Beat:** relying on nextest `slow-timeout` (a hang guard, not a budget); advisory-only reporting (the
owner's concern is growth, and advisory lines are ignored).

### D7 — Consolidation is measured before it is adopted

**Chosen:** a measurement task merges a sample of ~20 `issue_*` targets into one binary
(`tests/regressions/main.rs` + modules) on a branch, and records cold + warm build time, link time,
`target/` size and nextest wall time before/after, on the gate box. Migration proceeds (as its own
issue) only if the extrapolated PR-tier saving is ≥10% of `core-tests` wall-clock or ≥20% of
`target/debug/deps` size. Otherwise the new-work rule stands alone.

### D8 — Randomized tier contract (generator deferred)

This change fixes only the **contract** the #4257 generator must satisfy: seed printed on failure;
data seed and query seed split; explicit write timestamps from the seed; pinned `now`; no TTL in
replayed histories; high `gc_grace`; nightly = CQLite self-consistency (write→read, point vs full,
1-vs-N generations); weekly = replay a sample through Cassandra 5.0 in the existing Docker lane; a
failure files an issue with seed + shrunk repro and never gates a merge; the shrunk repro lands as a
committed regression test and the seed joins a checked-in fixed-seed array run in the PR tier.

## Cost analysis (owner constraint)

| Change | PR-tier effect |
|---|---|
| Engagement + outcome assertions | ~0 (assertions on existing queries) |
| Cold new-path leg | seconds (small fixtures read cold) |
| One fault meta-test per suite | one extra case per suite |
| Reverse × format × compression rows | tens of rows on cached fixtures |
| Fixture cache | **negative** — rebuilds eliminated |
| Budget line | **caps** growth; fails loud |
| Randomized burn | **0** — nightly/weekly only; PR runs only committed fixed seeds |
| Consolidation (if adopted) | **negative** — fewer links |

## Risks

- **Fixture cache staleness** → generator-version constant in the key; a stale read is a test bug, and
  the cache lives under `CARGO_TARGET_TMPDIR`, wiped by `cargo clean`.
- **Budget flapping on a loaded box** → budgets measured on the gate box with headroom; the line names
  the box's CPU budget (#2640) so a misfire is diagnosable.
- **Fault seam leaking into release** → `cfg(debug_assertions)` gate plus a test asserting the env var
  is ignored in a release-profile build.
- **Doctrine sprawl** → the doctrine page has a hard target of ~400 lines; incident narratives stay in
  linked issues, not the page.
