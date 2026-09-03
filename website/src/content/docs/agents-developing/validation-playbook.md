---
title: sstabledump Validation Playbook
description: JSONL golden files, parity tests, and smoke-test-all-tables — how CQLite validates parsing correctness against Cassandra's reference output.
sidebar:
  label: Validation playbook
  order: 6
---

CQLite validates parsing correctness by comparing output against `sstabledump` — the
Cassandra tool that produces authoritative JSON from SSTable files. Golden JSONL files
are committed alongside the binary SSTables so CI can run parity checks without a live
Cassandra cluster.

## Two oracles: physical-dump parity vs query-semantics parity

There are **two independent parity oracles**, and they answer different questions.
Confusing them hides read-time-reconciliation bugs (issue #1741/#1742):

| | **Physical-dump parity** | **Query-semantics parity** |
|---|---|---|
| Oracle | `*-Data.db.jsonl` (sstabledump `-l`) | `test-data/query-semantics-oracle.json` |
| What it captures | Every cell **physically on disk** — *including* tombstones, deleted rows, and expired-but-uncompacted TTL cells | The **post-reconciliation result set** a real Cassandra `SELECT` returns |
| Question answered | "Did CQLite parse the bytes the same as Cassandra wrote them?" | "Does a CQLite `SELECT` return what a Cassandra `SELECT` returns?" |
| TTL / tombstone rows | Present (enumerated) | Absent (shadowed / expired away) |
| Test lane | the seven `golden_path_*` integration tests, Python/Node parity | `cqlite-core/tests/query_semantics_oracle_parity.rs` (gate component `query-semantics-oracle`) |

**Why both are needed.** A physical dump enumerates shadowed/expired rows, so a
row-count/value comparison against it **structurally cannot catch a read-time
reconciliation bug**: when CQLite fails to apply partition deletions, range
tombstones, or TTL expiry, *both* sides still contain those rows and physical parity
passes green — while a real `SELECT` diverges. `test_basic.ttl_test_table`'s golden
holds all 100 expired rows; CQLite returned all 100; physical parity was green; a real
Cassandra `SELECT` returns 0. That gap is exactly what the query-semantics oracle
closes.

The query-semantics oracle records, per fixture, the canonical `SELECT` and its
expected result set evaluated at a **pinned `now`** (`pinned_now_secs`, epoch seconds)
so TTL expiry is deterministic — never wall-clock-flaky. The reader honors the pin via
the debug-only `CQLITE_TTL_NOW_OVERRIDE_SECS` seam (`now_clock.rs`). Each case carries a
non-empty `expected_rows` and re-asserts the committed golden's physical row count, so a
0-row or unreconciled result fails loudly (anti-empty-pass) and the divergence
(physical count > semantic count) is visible in the test output.

Run it directly:

```bash
env CQLITE_REQUIRE_FIXTURES=1 CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo test -p cqlite-core --features "state_machine cli-helpers" \
  --test query_semantics_oracle_parity -- --nocapture
```

`CQLITE_REQUIRE_FIXTURES=1` (which the gate component sets) makes an absent/empty
fixture a **hard failure**; without it, a minimal checkout that lacks the committed
`test_compaction_tombstone_ttl` fixtures SKIPs loudly.

### Point-vs-full differential lane (CQLite-vs-CQLite, issue #1918)

A third lane complements the two oracles above by comparing CQLite to *itself* across
its two read access paths. `cqlite-core/tests/point_vs_full_differential.rs` runs each
point-read-eligible corpus query under forced `CQLITE_READ_PATH=point` (a
partition-targeted lookup) and forced `CQLITE_READ_PATH=full` (a full scan +
reconciliation) — via the `QueryConfig::forced_read_path` knob — and asserts the two
paths return byte-identical rows/values/**order** at a **pinned `now`**
(`CQLITE_TTL_NOW_OVERRIDE_SECS`). The corpus deliberately includes multi-generation,
tombstone, and TTL fixtures — the reconciliation classes #1741 hid — so a divergence
between the point and full paths (invisible to a physical dump, which retains the
shadowed rows on both sides) fails the lane and names the diverging query. Same
fail-closed/SKIP contract as the query-semantics oracle:

```bash
env CQLITE_REQUIRE_FIXTURES=1 CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo test -p cqlite-core --features "state_machine cli-helpers" \
  --test point_vs_full_differential -- --nocapture
```

The `CQLITE_READ_PATH` knob is a **test/debug** control (not a perf recommendation);
`point` fails closed rather than silently full-scanning. See the CLI reference for the
user-facing knob docs.

#### Fixture roots resolve per TABLE, and committed cases must RUN (issue #3220)

A dataset lane that picks its corpus root by **keyspace** can pass without ever running.
Three lanes did exactly that (`point_vs_full_differential`, `query_semantics_oracle_parity`,
`read_path_forcing_e2e`): each selected the first candidate root whose `<keyspace>/` was a
directory and then **committed to it with no fallback**, although what each needs is a
specific **table**. On a machine whose `CQLITE_DATASETS_ROOT` corpus held `test_da/` but
not the git-committed `test_da/multiclustering_table-*`, the env root won, the table was
declared absent, and the #3032 multi-component clustering case **skipped silently** behind
a green suite.

Two rules now close it, and both apply to any new dataset lane:

* **Resolve TABLE-granularly.** `cqlite-core/tests/support/datasets_root.rs` —
  `sstables_root_for_table(keyspace, table)` walks *every* candidate root
  (`CQLITE_DATASETS_ROOT`, then the checkout's committed corpus) and returns the first that
  actually carries `<keyspace>/<table>-*/…-Data.db`. Presence is judged on a real `*-Data.db`
  component, never on directory existence: the repo commits JSONL sidecars for fixtures whose
  binaries are gitignored. Schemas keep resolving checkout-relative through
  `test-data/support/fixture_roots.rs` (#3148) — never by climbing `..` from the datasets root.
* **Assert per CASE, not suite-wide.** `assert!(ran > 0)` over a whole corpus cannot see one
  case skipping behind its siblings. Every case whose binaries are **committed to git** carries
  `must_run: true` and is fail-closed **unconditionally** (with or without
  `CQLITE_REQUIRE_FIXTURES`, because `core-tests` does not set it); under
  `CQLITE_REQUIRE_FIXTURES=1` *every* case must run, matching the query-semantics oracle's
  per-case `assert!(skipped.is_empty())`.

**Resolve by evidence, not by preference — and why no ordering can be fixed here.** Neither root is
a superset of the other. Measured on a fleet box, `/data/datasets` holds 144 `*-Data.db` across 122
tables but lacks the one git-committed `test_da/multiclustering_table`; the checkout holds only its
31 committed parity references, which include it. Env-first loses that committed fixture,
checkout-first loses 92 fetched tables — so a *preference ordering* is wrong for one set of tables
whichever way you point it, and only "walk every candidate, take the first that actually holds THIS
table's `*-Data.db`" is right for both. That supersedes issue **#3104**'s suggested "prefer the
already-exported `CQLITE_DATASETS_ROOT`" fix **for the lanes on this resolver**. #3104 stays **open**
for the work the resolver does not reach: the whole-corpus `#2078` preflight fail-close, a diagnostic
naming the found `Data.db` count and the overridden candidate root, the `--lite`
implausibly-small-corpus warning, and the CLAUDE.md / flow-* / `agents-developing/test-data` text
that still tells agents to override the exported root with the checkout path.

Defense in depth: the agent-gate `bti-multiclustering` component also runs
`point_vs_full_differential`, so the committed BTI fixtures are covered by a fail-closed gate
component rather than by `core-tests` alone.

#### Second axis: 1 generation vs N generations (issue #3129)

The point-vs-full comparison above holds the **generation count fixed**, so both of its
arms route through the same reconciliation kernel and a disagreement between the
*single-generation* read path and the *cross-generation* merge kernel
(`generation_merge.rs`) reproduces identically on both arms — the lane stays green while a
real `SELECT`'s answer depends on the table's compaction state. That is a fourth blind
spot alongside physical-dump parity, query-semantics parity, and the self-round-trip class.

`cqlite-core/tests/point_vs_full_differential/one_vs_n_generation.rs` (a submodule of the
same test target) closes it: for each corpus fixture that holds **exactly one
Cassandra-written generation**, it materializes two temp trees from those same bytes — one
generation, and the same generation copied N ≥ 2 times under distinct generation numbers —
then requires identical rows/values/**order** from both trees for the full scan, every
per-partition read under both forced read-path modes, and the multi-key `IN`, at the same
pinned `now`. N identical copies reconcile to exactly one copy under Cassandra's rules, so
any inequality is a merge-kernel (or single-gen) defect. Anti-vacuity: each case pins the
exact full-scan row count and partition count, both arms' generation counts are re-scanned
after materialization (so the axis can never degenerate to 1-vs-1), and a source fixture
that stops holding exactly one generation FAILs.

Two properties of this axis are easy to get wrong:

* **It probes partitions that return NOTHING, on purpose.** Discovery runs
  `SELECT <pk> FROM …`, so a fully-deleted partition yields no key and would never be point-read
  — leaving the seek/merge path untested for exactly the deleted-partition phantom-row shape the
  axis exists to catch. Each case therefore declares `empty_probe_keys` (deleted or absent
  partitions), asserted to be undiscoverable AND to return zero rows on both arms in both modes.
* **It covers structural merge, not precedence.** The N copies are byte-identical, so every
  cross-generation comparison is a *tie*: interleaving, ordering, dedup, static injection and
  tombstone application are exercised, but "a newer generation's tombstone shadows an older
  generation's live row" is not. That asymmetric class belongs to the real 2-generation
  Cassandra fixtures (`test_tomb.resurrection_gc0`, `skipped_partition_delete`) and the
  Cassandra-oracle lanes.

**A quarantine needs a release signal.** Shapes that already diverge for a tracked defect are
marked `known_divergent` with a reason that MUST cite its issue (`#<number>`, asserted — a
waiver with no cited issue is not a waiver). They are excluded from the enforcing lane, but they
are NOT parked in an `#[ignore]`d reproducer: an ignored test is a ratchet that never releases,
since the gate never runs it and nothing ever reports that the defect got fixed. Instead
`one_vs_n_generation_quarantine_still_diverges` runs in the normal test run and pins the
*expected divergence*: each quarantined case must STILL diverge, and the moment one starts
agreeing the test fails with instructions to flip `known_divergent` to `None`. Only an error
carrying the divergence marker counts — a harness/fixture error is reported separately, so a
broken harness can never masquerade as "still broken":

```bash
env CQLITE_REQUIRE_FIXTURES=1 CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo test -p cqlite-core --features "state_machine cli-helpers" \
  --test point_vs_full_differential -- one_vs_n_generation_quarantine_still_diverges --nocapture
```

Generalizing: when a test must be excluded because of a known defect, prefer an
**expected-failure pin that fails when the defect disappears** over `#[ignore]`. The former
self-releases; the latter is green whether the code is broken or fixed.

### The self-round-trip blind spot (CQLite-written + CQLite-read, issue #3042)

Both lanes above compare CQLite against an *external* oracle or against its own two read
paths. A third class of test compares CQLite only to itself in a single direction — write
with CQLite, read back with CQLite, assert the values survived — and it carries a blind spot
severe enough to name explicitly:

> **A CQLite-written + CQLite-read round-trip test is INVARIANT to a uniform
> framing/serialization error.**

The writer and the reader share the same encoding assumption. If that assumption is wrong,
*both sides make the identical mistake*, the round-trip still closes, and the test is green —
while real Cassandra-written data reads wrong, and CQLite-written data is unreadable by
Cassandra. The test validates **self-consistency**, which is not the property that matters.
It therefore can **never** substitute for a Cassandra-written fixture on any on-disk framing
or encoding property.

**The concrete instance (issue #3002).** The only arity-2 BTI test,
`cqlite-core/tests/issue_908_bti_canonical_write.rs`, is CQLite-written and CQLite-read and
asserts only ordering/structure. It stayed green across a defect pair that cancelled:

1. `resolve_rows_db_entry` computed the signed root-delta base as `RowsOffset + key_length` —
   **2 bytes low**, because Cassandra 5.0 captures `basePosition` *after*
   `writeWithShortLength`, i.e. `RowsOffset + 2 + key_length`.
2. The OSS50 clustering-bound encoders emitted the `0x40 NEXT_COMPONENT` byte only *between*
   components, while `ClusteringComparator.ByteComparableClustering` emits it before **each**
   component including the first.

The 2-low root pointed at exactly the subtree the un-prefixed bounds were keyed for, so the
two errors masked each other perfectly. Fixing *either alone* regresses BTI clustering reads —
which is precisely why a symmetric test cannot see the pair. It is also why the fixture matters
independently: the wrong root landed on the root's only child only because that fixture's child
node happened to be 2 bytes wide (see the fixture-must-vary rule in
`.claude/skills/test-data-management/SKILL.md`).

**What caught it:** `cqlite-core/tests/issue_3002_bti_rows_root_base.rs`, pinned against the
real Cassandra 5.0 `da` fixture (`test_da/wide_table`), with every expectation derived from
Cassandra's own writer/reader source — never from CQLite's prior behavior. That last clause is
the load-bearing one: an expectation reverse-engineered from current CQLite output re-encodes
the bug as the specification.

**Rule.** For any on-disk framing/encoding property, the oracle must be **Cassandra-written
bytes or Cassandra source**, never CQLite's own output. A round-trip test is a useful
regression net for internal invariants; it is not parity evidence.

### The projected-column blind spot (point-vs-scan on a SUBSET of columns, issue #3890)

Every blind spot above is about which **oracle** you compare against. This one is about **how
much of the row** you compare, and it is the cheapest of the five to reintroduce:

> **A point-read test that compares a SUBSET of columns against the scan cannot see a
> TRUNCATED point row.**

`assert_point_equals_scan` (`cqlite-core/tests/issue_1573_readat_positional.rs`) projected
`id` plus one named column and ran `SELECT id, <col>` on both legs. `SELECT id, name` decodes
the first two cells and stops, so a point read whose *later* cells failed to decode compared
equal on exactly the two columns under comparison — and stayed green for years.

Two properties turn "under-tested" into "invisible":

1. **The failure does not propagate.** A cell-decode error inside the row loop is swallowed —
   `cqlite-core/src/storage/sstable/reader/parsing/row_decoder/row_data.rs` logs it at `debug`
   and `break`s out of the column loop (removing that swallow is issue #3721). So the query
   returns `Ok`.
2. **The failure has no positive signal.** The cells after the failure point are simply
   **absent** from the row's `values` map. A `get(col)` comparison over the columns you named
   cannot notice a column you did not name.

**Rule.** A point/seek-vs-scan comparison runs `SELECT *` and asserts **both directions** of
the column set — no scan column absent from the point row, no point column the scan lacks —
and names the missing column in the failure message. Row counts and whole-row fingerprint
*sets* are not substitutes: neither says, per row, which column went missing.

**The corpus-wide instance** is
`cqlite-core/tests/issue_3890_point_read_column_parity_sweep.rs`: it derives its table set at
run time from `test-data/schemas/**/*.cql` intersected with the `*-Data.db` components on
disk (never a curated list), sweeps every table it can express, and prints by name every
table it cannot — `no-schema`, `unrenderable-key`, `empty` — because a lane that omits
coverage silently is indistinguishable from one that covers it.

**And its bound is measured, not chosen** — a bound tight enough to cost nothing can be tight
enough to miss most of what it exists to catch, so when you cap a sweep, measure what the cap
EXCLUDES. Raising that sweep's per-table cap from 4 to 32 multiplied what it detects several
times over while leaving the whole run under a second (962 targeted reads over 101 tables, a
runtime anyone can reproduce with `cargo test`).

**A cap is only as reproducible as its SELECTION, and that is the subtler half.** The first
version stopped collecting after 32 distinct keys in *scan* order and sorted afterwards, while
its own doc said "the first 32 in sorted order" — so which keys got swept moved with scan
ordering. Fixing it to collect every distinct key first and apply the cap in sorted order
*changed the detection count outright*, from the same corpus against the same defect. Neither
count was wrong; they were counts over different samples. If you quote what a cap detects,
quote the selection with it.

**The detection figures themselves live in that test's module header, not here, and the reason
is a rule in its own right.** Measuring a guard's detection power requires instrumenting the
swallowed error AND reverting the fix — detection is only meaningful against the defect
PRESENT (the "membership is not detection" rule). So the numbers are not reproducible from
committed source, and a figure in doctrine that nobody can re-derive from the repo is worse
than no figure: it is what stops the next person looking. Keep such numbers beside the code
they describe, with the command, the cap value and the revert step written out.

## Golden JSONL files

Every Data.db in the dataset has a companion `.jsonl` file containing
`sstabledump -l` output (one JSON object per line, one line per row):

```
test-data/datasets/sstables/
└── test_basic/
    └── simple_table-<hash>/
        ├── nb-1-big-Data.db         ← binary (not in git; fetch separately)
        └── nb-1-big-Data.db.jsonl   ← sstabledump golden (committed to git)
```

The JSONL files ARE in git. You can run parity tests without fetching binary SSTables,
but you need the binaries to run the parser itself.

## Smoke test: all tables

```bash
# Runs CLI against every table, checks exit codes and row counts
bash test-data/scripts/smoke-test-all-tables.sh
```

The gate runs this against a freshly built debug binary:

```bash
cargo build --package cqlite-cli --bin cqlite
CQLITE_CLI="$PWD/target/debug/cqlite" bash test-data/scripts/smoke-test-all-tables.sh
```

Using a freshly built binary prevents the failure mode where a stale release binary
(from a previous run) passes smoke while current code is broken. This was an actual
failure caught in the first full gate run.

**Expected output**: 33/33 tables PASS; the 3 da/BTI tables are SKIP-PENDING.

## Integration parity tests (Rust)

The seven CI-enforced integration tests compare parsed rows against the JSONL goldens:

```bash
cargo test --package cqlite-integration-tests \
  --test golden_path_scan_operations_tests \
  --test golden_path_get_operations_tests \
  --test golden_path_partition_lookup_tests \
  --test golden_path_summary_index_integration_tests \
  --test chunked_data_reader_direct_test \
  --test comprehensive_component_integration_tests \
  --test fixture_specific_integration_tests
```

These are the same seven targets the gate's `integration-tests` component runs. Run
them individually when a specific area fails.

## Python parity tests

```bash
# All 33 tables: row count + value-level parity
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  pytest bindings/python/tests/test_parity.py -v

# Python vs CLI output equivalence
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  pytest bindings/python/tests/test_cli_parity.py -v
```

`test_parity.py` has three test classes:
- `TestRowCountParity` — row count per keyspace (33/33 must pass)
- `TestValueParity` — cell-level comparison for representative tables
- `TestE2ESummary` — asserts all 33 tables pass (explicit failure if count drops)

**Known xfail**: none as of Dec 2025. Prior xfails (`static_columns_table` #480,
`typed_collections_table` #481) are resolved. Issue #493 (set element tombstones)
is tracked as out-of-scope for v0.9.1.

## Node.js parity tests

```bash
# Requires CQLITE_DATASETS_ROOT
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets npm run test:parity --prefix bindings/node
```

39 parity tests in `bindings/node/__test__/parity.test.js`. Uses JSONL utilities in
`parity-utils.js` for parsing and type normalization.

## Manual parity check workflow

When investigating a single table:

```bash
# 1. Parse with cqlite CLI
cargo run --package cqlite-cli -- \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables/test_basic/simple_table-<hash> \
  --query "SELECT * FROM test_basic.simple_table" \
  --out json > /tmp/cqlite.json

# 2. Reference is the JSONL golden
cat test-data/datasets/sstables/test_basic/simple_table-<hash>/nb-1-big-Data.db.jsonl \
  | jq -s '.' > /tmp/reference.json

# 3. Normalize and diff
jq -S '.' /tmp/cqlite.json > /tmp/cqlite-sorted.json
jq -S '.' /tmp/reference.json > /tmp/ref-sorted.json
diff /tmp/ref-sorted.json /tmp/cqlite-sorted.json
```

Type differences between sstabledump JSON and CQLite JSON are expected and documented:
Python uses native types (v0.13 mapping: `timestamp`→`datetime`, `uuid`→`UUID`,
`blob`→`bytes`, `time`→`int` ns since midnight, `duration`→`cqlite.Duration` — see the
v0.13 Migration Guide, `docs/development/v0.13-migration-guide.md`); CLI uses JSON
strings. Normalization is needed for comparison — see
`bindings/python/tests/test_cli_parity.py` for the normalization logic.

## Adding a new table to parity coverage

1. Add schema to `test-data/schemas/` and `schemas/core.list`
2. Regenerate: `bash test-data/scripts/regenerate-datasets.sh`
3. JSONL golden is generated automatically during regeneration
4. Add table to the relevant parity test file
5. Run gate: `scripts/agent-gate.sh`

## Fail-closed guards vs non-deterministically-regenerated sources

A fail-closed guard must key on something the source actually reproduces. CI regenerates
clean SSTables from a live `cassandra:5.0.2` container on every run, and several
components are **not byte-reproducible** between runs:

- the **BTI (`da`) trie** — `Partitions.db` / `Rows.db`
- **`Statistics.db`** — embeds wall-clock, host, and repair metadata

**Rule (L1):** when a source is regenerated non-deterministically, gate on the
**semantic verdict**, not on a whole-file byte identity. For corruption/verify corpora,
assert that *CQLite detects the corruption the same way Cassandra does on whatever bytes
are present* (the parity TEST) — never bind a captured verdict to the `sha256` of the
whole fixture. Keep the *authoring* check (empty/missing verdict) fail-closed; that part
IS reproducible. Per-component / full-directory binding is the proper future form
(tracked in #1294).

Why this matters: a whole-file-sha guard added in #1236 passed locally every run (the
local gate uses pre-existing, non-regenerated datasets) but tripped in CI on a
*different* regenerated fixture each round (first the BTI trie, then
`statistics_db_header_damage`) — three CI-failure root-cause cycles *after* the local
gate reported PASS. Origin: flow-meta #1310.

## Row count 0 — silent-pass trap

If parity tests pass but show 0 rows, `CQLITE_DATASETS_ROOT` is unset or points to a
directory without binary Data.db files. The tests return empty results (not an error)
when no files are found. This is the failure mode the gate's dataset preflight prevents.

```bash
# Verify data is present
find "$CQLITE_DATASETS_ROOT/sstables" -name "*-Data.db" | wc -l
# Must be > 0 (should be 33+ for the full corpus)
```

## Fuzzing the parser (issue #1614)

The parser decodes untrusted bytes, so a standing **cargo-fuzz / libFuzzer** harness at
`fuzz/` proves it never panics/hangs/OOMs on arbitrary input (every input yields `Ok` or
`Err`). The crate is its own workspace and is **excluded from the main workspace**, so the
stable `scripts/agent-gate.sh` and every default `cargo` build are unaffected — fuzzing
needs **nightly** Rust and is out of the stable gate. Five targets (`fuzz_vint`,
`fuzz_value_decode`, `fuzz_block_emit`, `fuzz_bti`, `fuzz_schema_parse`) reach
`cqlite-core` internals via the feature-gated `#[doc(hidden)] cqlite_core::fuzz_support`
module (`--features fuzz`), which leaves the default public API unchanged. Run one target
with `cd fuzz && cargo +nightly fuzz run fuzz_vint -- -max_total_time=45 -rss_limit_mb=2048
-timeout=25` (or all via `fuzz/smoke.sh`); `.github/workflows/fuzz.yml` runs a bounded PR
smoke lane plus a nightly long-run, uploading any crash reproducer as an artifact. A crash
is a **success** for the net — it is filed as its own bug issue with the reproducer, not
silently patched.

## Field round validation (live-cluster reporting standard, issue #2399)

The oracles above validate correctness locally/in CI against fixtures. A separate,
complementary standard covers the **live 3-node field validation round** run against a
real Cassandra + Flight + Trino deployment (the round tracker channel, e.g. #2367):
`docs/development/round-validation-metrics.md` — a 14-point checklist (A correctness, B
hang/liveness — both pass/fail GATE items; C throughput, D hygiene — tracked numbers),
pre-filled with the round-9 baseline. That round gate is a live-cluster verdict, distinct
from `scripts/agent-gate.sh` above. New round trackers seed from
`.github/ISSUE_TEMPLATE/round-tracker.yml`.
