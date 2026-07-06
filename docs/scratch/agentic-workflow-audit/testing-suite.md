# CQLite Test Suite Inventory

## Inventory

### Test surfaces by framework

| Surface | Framework | Count | Location |
|---------|-----------|-------|----------|
| **cqlite-core tests** | Rust (tokio, integration) | ~1,275 test cases | `cqlite-core/tests/*.rs` (367 files) |
| **cqlite-cli tests** | Rust (integration) | ~427 test cases | `cqlite-cli/tests/*.rs` (105 files) |
| **Python bindings** | pytest | ~401 test functions | `bindings/python/tests/*.py` (25 modules) |
| **Node.js bindings** | Jest | ~496 test blocks (describe/it/test) | `bindings/node/__test__/*.test.js` (13 files) |
| **sstabledump-validator** | Rust | 4 files | `tools/sstabledump-validator/tests/*.rs` |
| **cassandra-parity** | Rust | varies | `tools/cassandra-parity/tests/*.rs` |

**Total production test count**: ~2,600 Rust tests, ~400 Python tests, ~500 Node tests (3,500+ total).

### Test data organization

- **Committed test data**: `test-data/datasets/sstables/<keyspace>/<table-uuid>/`
- **JSONL goldens**: Stored as `*-Data.db.jsonl` per table (format-agnostic: `nb-*`, `oa-*`, `da-*` prefixes)
- **Schemas**: `test-data/schemas/*.cql` (basic-types, collections, time-series, wide-rows, oa-test)
- **Gitignore**: Binary `*-Data.db` files (fetched via `bash test-data/scripts/fetch-datasets.sh`)
- **Committed corpus classification**: In `test-data/corpus-coverage-policy.md` (dynamic discovery prevents silent drops)

---

## How it works

### Parity testing architecture

1. **Discovery (Issue #1229)**: Dynamic corpus enumeration walking committed git-tracked files under `sstables/`, NOT hand-typed counts. An untracked WIP keyspace on disk is ignored; a newly-committed keyspace is auto-in-scope unless listed in skip-set.

2. **JSONL validation**:
   - Python test `test_parity.py` discovers tables dynamically via `corpus.py`
   - Compares row counts + cell values against `*-Data.db.jsonl` goldens
   - Same JSONL golden used by Node parity tests and smoke-test-all-tables.sh

3. **Smoke test** (`test-data/scripts/smoke-test-all-tables.sh`):
   - Bash script iterating every committed table via CLI `read-sstable` 
   - Mirrors Python/Node skip-set + pending-set exactly
   - Fails on 0 rows if Data.db is present (absence → skip, not fail)

4. **Test classification**:
   - **Enforced**: Run through the reader; failures fail the suite
   - **Skip-pending**: Discovered + listed explicitly (zero-live-row fixtures, missing schema, future binaries)
   - **Skip-keyspace**: Entirely excluded (write-parity fixtures, system*, compaction harness fixtures)

### Silent-pass prevention

**Problem**: Historical fixture drops would silently inflate coverage % (e.g., 8 unexecuted keyspaces reported as 100% coverage).

**Solution**: Three-layer fail-closed policy (Issue #1229, #1230, #1319):

1. **Explicit allowlist** (`IN_SCOPE_KEYSPACES` in `corpus.py`): Keyspace must be named OR it trips an integrity guard (`unclassified_keyspaces()`).

2. **Environment-gated strict mode**:
   - Default local: missing Data.db → skip (lenient)
   - CI via `CQLITE_REQUIRE_FIXTURES=1` or `CQLITE_PARITY_REQUIRE_DATASETS=1`: missing dataset → **hard failure** (fail-closed)

3. **Committed-corpus filter** (Issue #1319): Skip-set classifies the **committed** git-tracked corpus (via single `git ls-files -z`), not live-disk enumeration. An untracked WIP table is neither enforced nor flagged; a committed table missing its golden is still flagged loudly.

### Fixture classification consistency

All three harnesses mirror the same skip-sets in three places:
- `smoke-test-all-tables.sh` (`SKIP_KEYSPACE_NAMES`, `SKIP_PENDING_KEYSPACES`)
- `bindings/python/tests/corpus.py` (`SKIP_KEYSPACES`, `SKIP_PENDING_KEYSPACES`)
- `bindings/node/__test__/parity-utils.js` (`SKIP_KEYSPACES`, `SKIP_PENDING_KEYSPACES`)

Mismatch between any two is an audit failure.

---

## Measured/observed costs

### Test duration (from CLAUDE.md / memory)

- **agent-gate.sh full run**: 12–25 min (optimized via sccache, nextest, 2-lane parallelism in #1825/#1737)
- **agent-gate.sh --lite**: ~1–5 min (fmt + file-size + scoped workspace clippy + blast-radius tests)
- **Python full suite**: varies (slow tests marked with `@pytest.mark.slow`; RUN_SLOW_TESTS=1 enables CLI parity)
- **Node full suite**: varies (13 test files, ~500 test blocks)

### Build artifacts

- Rust tests compile ~367 + 105 = 472 integration test files (many fixture/data files, not all are code)
- Python fixtures via conftest.py: centralized in `bindings/python/tests/conftest.py` (Issue #330)
- CLI binary cached per session by Python fixture (`check_prerequisites` module-scoped)

### Dataset size

- Published asset (CI): subset ship without `*-Data.db` binaries; JSONL goldens always present
- Local full dataset: ~43GB (target disk budget per MEMORY.md notes)

---

## Friction points

1. **JSONL golden freshness**: Derived artifacts (not regenerated per PR). Semantic merge race risk — a squash-merge can leave goldens stale vs merged base. Mitigated by `parity-report-heal` push-to-main job (issue #1338), but requires `PARITY_HEAL_TOKEN` secret to open regen PR.

2. **Multi-harness skip-set sync**: Same skip-set lives in bash/Python/JS; no DRY enforcement. A maintainer must update all three places or one harness drifts (audit failure, not caught by tests themselves).

3. **Dynamic discovery rooted to source tree, not CQLITE_DATASETS_ROOT**: Git-tracked-ness computed against repo's `test-data/` (correct for WIP filter), but disk enumeration uses live `CQLITE_DATASETS_ROOT`. Confusing when datasets root is a different checkout.

4. **Slow tests and performance gates**: Python and Node suites have `@pytest.mark.slow` but no explicit max-duration budget. CLI parity tests are slow (compile + full query → JSON roundtrip) and only run under `RUN_SLOW_TESTS=1` (issue #331).

5. **Silent 0-row results**: A table with Data.db present but 0 rows in the JSONL golden fails the parity check (correct), but the error message can be ambiguous if the JSONL is itself stale (no golden-staleness audit).

6. **Flaky test guards underspecified**: `@pytest.mark.xfail` used for expected failures in `test_abort_safety.py` (panic-unwind firewall, issue #1437) and `test_parity.py` (one unknown case); no formalized policy for wall-clock-dependent tests. CLAUDE.md forbids wall-clock races ("never assert against a window captured at a different instant").

7. **Dataset subset inference fragile**: CI ships a subset of binaries; a locally-enforced table skips on `Data.db` absence, but no explicit "this table is local-only" annotation. A moved/renamed binary silently triggers skips instead of failing loudly.

---

## Open questions

1. **Golden staleness audit**: Who/when regenerates JSONL goldens when corpus SSTables change? Is there a periodic audit or CI lane that checks all goldens are fresh?

2. **Skip-set synchronization**: Is there a linter or CI check that enforces all three skip-set copies (bash/Python/JS) stay identical?

3. **Execution latency for bindings**: What's the measured tail latency for a single Python test vs a Rust test? Are Node tests slower than Python?

4. **Flaky test rate**: Is there a dashboard or metric for test flake rate? The CLAUDE.md forbids wall-clock races but doesn't list known flaky tests.

5. **Dataset versioning**: How is the dataset asset versioned? A concurrent session can commit WIP fixtures not yet in the published asset. How does CI decide which version to test?

6. **Test coverage gaps**: The memory notes issue #1229 "8 committed keyspaces were covered by zero comprehensive test while everything reported 100%". Post-fix, is coverage now audited per-keyspace or just by dynamic enumeration?
