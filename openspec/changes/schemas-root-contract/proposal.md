# Proposal: separate the committed schemas root from the fetched datasets root (issues #3148, #3131)

**Milestone:** maintenance (test-fixture / gate contract) · **Priority:** P1 ·
**Routing:** design-driven (a fixture-discovery and gate-preflight **contract** change — which env var
owns which root, and what the preflight promises; there is no on-disk oracle to pin) ·
**Issues:** #3148 (gate preflight does not validate the schemas root), #3131 (no single
`CQLITE_DATASETS_ROOT` works on the fleet) ·
**Related:** #2078 (the fail-closed corpus preflight this extends), #2878 (the sibling
`fetch-datasets.sh` `rm -rf` defect — deliberately NOT in scope), #3095 / PR #3141 (the delivery whose
triage was nearly misattributed), #3130 / #3127 ("the verification was the broken part" siblings).

## Why

Two roots with different natures were conflated:

| Root | Nature | Owner | Present in a bare checkout? |
|------|--------|-------|-----------------------------|
| `test-data/datasets` | **fetched, relocatable** binary corpus | `CQLITE_DATASETS_ROOT` | no (fetched) |
| `test-data/schemas` | **committed source** — 23 files incl. `legacy/`, `udts/` | the checkout | **always** |

Four independently-written call sites derived the *schemas* root from the *datasets* root by climbing
`..`:

```
cqlite-core/benches/fixtures/mod.rs:73-75   datasets_root().join("../schemas")
cqlite-core/tests/dead_cache_delete_tests.rs:604   root.join("../schemas").join(schema_file)
cqlite-core/tests/observability_correctness.rs:548 datasets_root().join("../schemas").join(...)
cqlite-cli/benches/export_csv.rs:124              datasets_root().join("../schemas").join(SCHEMA_FILE)
```

Three consequences, all observed on real runs:

1. **No single root satisfies both halves (#3131).** `/data/datasets` (where `fetch-datasets.sh`
   caches and extracts on this fleet) holds ~155 `-Data.db` but `/data` has no `schemas/`. The
   CLAUDE.md-documented `<repo>/test-data/datasets` has the sibling but only ~30 committed byte-parity
   references and **zero** `test_basic` fixtures. Pointing at the former clears the preflight and then
   fails 5-8 tests with `Path does not exist: /data/datasets/../schemas/basic-types.cql`.

2. **The gate's preflight validated only half the fixture contract (#3148).**
   `grep -c schemas scripts/agent-gate.sh` was **0**. `_fixture_status()` counted
   `$CQLITE_DATASETS_ROOT/sstables/test_basic/*-Data.db` and reported that as *fixture readiness* —
   `STATUS: OK` — then ~8 minutes of build later `core-tests` and `memory-budget` failed on opaque
   missing-`.cql` panics. That is **worse than having no preflight**: with no preflight an agent hitting
   five `.cql` panics investigates its environment; with `STATUS: OK` already recorded it reads
   "fixtures verified" and suspects its own diff. On #3095 / PR #3141 an environmental failure was
   nearly attributed to a read-path change it had nothing to do with. A verification step that produces
   false confidence actively degrades attribution.

3. **`join("..")` is not a lexical parent at the syscall level.** The kernel resolves `datasets/..`
   against the **symlink target's** parent. So `ln -s <checkout>/test-data/datasets /data/datasets`
   makes `/data/datasets/../schemas` resolve to `<checkout>/test-data/schemas`, while a real
   `/data/datasets` directory resolves to `/data/schemas`. Two visually identical layouts, opposite
   outcomes, and no message explaining why — #3148's "symlink trap".

Compounding it, **`fetch-datasets.sh` exits 0 having named no actionable root** when the cache is warm:
its sole output was `Dataset <asset> (tag <tag>) already present in <root>; skipping download`. A green
fetch was therefore not evidence that any particular tree gained fixtures, so the documented remedy
silently failed to remedy (#3131 item 2).

And the tempting shortcut — `AGENT_GATE_ALLOW_MISSING_FIXTURES=1` — buys a green by letting dataset
components SKIP: a vacuous PASS, exactly what #2078 exists to prevent. The wrong fix is also the easy one.

## What Changes

1. **The schemas root is resolved CHECKOUT-RELATIVE, never from `CQLITE_DATASETS_ROOT`** — #3148's
   proposed fix 4, taken as the owner's design decision (#3148 AC (h)). A checkout always holds these
   files, so the failure mode becomes **structurally impossible** and the symlink trap **disappears**
   rather than being papered over: the change leaves ZERO `..` climbing from the datasets root, so
   there is nothing left to mis-resolve. An explicit `CQLITE_SCHEMAS_ROOT` override is honored when set,
   non-empty and readable, for out-of-tree runs.

2. **One shared resolution file, `test-data/support/fixture_roots.rs`**, pulled into each consuming
   target with `#[path = …] mod fixture_roots;` (the pattern already used for
   `#[path = "../benches/fixtures/mod.rs"]`). All four historical sites migrate to it; no open-coded
   `join("../schemas")` expression remains. `schema_path()` verifies readability and panics naming the
   resolved **absolute** path, how the root was chosen, and the remedy.

3. **The three divergent `datasets_root()` copies are reconciled** into one implementation exposing two
   *documented* shapes — `datasets_root()` (infallible, checkout fallback) and
   `datasets_root_if_present()` (`Option`, env-var-only, no fallback, for SKIP-gated tests) —
   preserving today's observable per-test behavior exactly.

4. **The FULL gate's preflight validates the schemas root**, checking **readability of the specific
   `.cql` files the components consume** (not directory existence), and FAILs CLOSED with
   `missing-schemas: FAIL-CLOSED (#3148)` — textually distinct from #2078's `missing-fixtures:` — plus a
   remedy naming the exact expected absolute path. A positive `schemas: N/N …` line is stamped on
   success so a pasted SUMMARY shows the check RAN. `--lite`/`--only` stay lenient. **No opt-out**: the
   fetched corpus is legitimately absent sometimes; committed source in a checkout never is.

5. **A positive-control self-test** (`scripts/tests/test_agent_gate_schemas_preflight.sh`, wired into
   `tooling-tests`) proves the preflight **REJECTS** a schemas-less and a present-but-incomplete root.
   The #3148 gap survived precisely because `STATUS: OK` was only ever observed on the happy path.

6. **`fetch-datasets.sh` never exits 0 leaving an unusable root**: both exit paths re-verify the
   content at the extraction target and print the exact
   `export CQLITE_DATASETS_ROOT=<absolute path>` line the run guarantees, plus a NOTE when that differs
   from the checkout default. A new non-mutating `--verify-only` mode makes the failure path
   exercisable — a check observable only when passing is not a check.

## Non-goals

- **#2878 is NOT in scope.** The `rm -rf "${DATASET_ROOT}"` and the
  `[ -n "${CI:-}" ] || return 0` short-circuit in `restore_ci_tracked_dataset_files` are deliberately
  untouched; a self-test case asserts both remain verbatim so the boundary cannot be silently crossed.
- **Not relocating the corpus.** #3131 item 1 offered (a) make the fetch populate the repo tree or
  (b) ship `schemas/` alongside the extracted corpus. This change makes **both moot**: with the
  schemas decoupled, ANY corpus root works, so `/data/datasets` is self-sufficient by construction and
  no data is duplicated. Recorded as a deliberate supersession, not an omission.
- **Not converting the other ad-hoc `CQLITE_DATASETS_ROOT` readers.** ~50 `cqlite-core/tests/**` and
  `src/**` inline suites resolve the corpus themselves. #3148 names three copies; touching fifty is a
  different change with a different risk profile.
- **Not unifying the per-keyspace ad-hoc corpus checks** (`test_compactionparity`,
  `test_compaction_tombstone_ttl` at `scripts/agent-gate.sh:4238`, `:4295`) — noted as adjacent
  inconsistency in #3148's scope note and explicitly left out.
- **Not a docs change in this deliverable.** CLAUDE.md and the `agents-developing/test-data` page are
  updated on the same branch by a separate pass; the facts they must state are listed in `design.md`.
- **No library surface, no on-disk format work, no no-heuristics implications.** Nothing touches
  `cqlite-core`'s public API, the decode path, the bindings, or the <128MB budget. The shared file is
  test/bench support and is never compiled into the library.

## Impact

- **New:** `test-data/support/fixture_roots.rs` (the single roots contract),
  `scripts/tests/test_agent_gate_schemas_preflight.sh` (16 assertions, hermetic).
- **Modified:** `cqlite-core/benches/fixtures/mod.rs`, `cqlite-core/tests/dead_cache_delete_tests.rs`,
  `cqlite-core/tests/observability_correctness.rs`, `cqlite-cli/benches/export_csv.rs`,
  `scripts/agent-gate.sh` (schemas preflight + `--preflight-schemas` hook + SUMMARY line +
  `tooling-tests` wiring), `test-data/scripts/fetch-datasets.sh`.
- **Gate:** a new FULL-gate fail-closed cause (`missing-schemas:`) and a new positive SUMMARY line
  (`schemas:`). `--lite`/`--only`/`--delta` behavior unchanged.
- **Operators:** `CQLITE_DATASETS_ROOT` alone is now sufficient on every layout; `CQLITE_SCHEMAS_ROOT`
  is a new, optional out-of-tree override.
