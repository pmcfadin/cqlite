# Differential Compaction Harness (Epic #817 / issue #819)

A differential harness that validates CQLite's compaction output against a
reference compaction of the **same logical data**, using the three-tier fidelity
bar defined in issue #818.

It lives alongside the existing parity tooling (`tools/sstabledump-validator/`,
`tools/cqlite-validator/`) as a Rust integration test:

- **Test:** `cqlite-core/tests/issue_819_differential_compaction.rs`

## What it does

The harness drives CQLite's one-shot compactor
(`cqlite_core::storage::write_engine::merge::compact_sstables`) and the k-way
merge read path (`KWayMerger`) to produce, read back, and compare compaction
outputs. Comparison is done at three tiers:

| Tier | Name | Role | Gate? |
|------|------|------|-------|
| Tier 2 | Logical merge equivalence | Walk both outputs partition-by-partition / cell-by-cell; assert identical surviving tuples carrying all **OBSERVABLE** read/merge-affecting metadata (see caveat below) | **YES** |
| Tier 1 | Real-node load-path validity | generation/naming, TOC completeness, `Digest.crc32` matches `Data.db`, component completeness, partition+clustering ordering | **YES** |
| Tier 3 | Raw-byte diff (Data.db/Statistics.db/Index.db) | per-component byte-offset diff, printed for debugging | **NO — never gates** |

### The Tier-2 canonical tuple

Two outputs are Tier-2 equivalent iff their **ordered** lists of canonical
tuples are identical. Each tuple carries, per (partition, clustering) coordinate:

- partition token + raw partition-key bytes,
- clustering key (stable byte form) — `None` for partition-level / non-clustering,
- row **kind** (live row vs row tombstone; static/RT-marker reserved),
- row-deletion metadata `(markedForDeleteAt_micros, localDeletionTime_secs)`,
- per surviving cell: column id, **raw value bytes**, write timestamp, TTL.

The comparison is value-byte-exact (not rendered-string), so encoding
regressions (e.g. a value or timestamp encoded with the wrong width) are caught.

#### Observability caveat (Tier-2 is observable-only for expiring / complex cells)

The merge read model (`CellData` in `merge.rs`) carries only `column, value,
timestamp, ttl`. It has **no per-cell local-deletion-time** and **no complex-cell
path**, so two outputs that differ ONLY in an expiring cell's local-deletion-time,
or ONLY in the per-path layout of a collection / non-frozen UDT, would compare
**equal** here (finding #823). When such cells are present, the harness does NOT
claim full Tier-2 equivalence over them: `ObservabilityCaveats` detects them and
the success message downgrades to **OBSERVABLE-ONLY** equivalence rather than
asserting a gate it cannot enforce. The row tombstone's local-deletion-time *is*
observable (`RowData::Tombstone { local_deletion_time }`) and *is* compared.

## What runs by default vs. env-gated

### Default (`cargo test`) — NO Cassandra needed

1. `differential_two_generation_self_consistency` — compact N inputs (gen-1),
   Tier-1 validate, then **re-compact gen-1's own output (gen-2)** and assert
   Tier-2 equivalence between gen-1 and gen-2 (AC2 / finding #2: catches
   write-side defects only the next merge observes). Tier-3 byte diff printed.
2. `differential_two_independent_paths_fixture` — fixture fallback standing in
   for the Cassandra reference: compact the same inputs through two independent
   CQLite code paths (`compact_sstables` vs. a manually-driven
   `KWayMerger` + `SSTableWriter`) and assert Tier-2 equivalence + Tier-1
   validity on both.
3. `differential_input_merge_vs_output_fidelity_live_cells` — non-ignored
   input→output fidelity gate for the **no-tombstone path**: a fixture of plain
   live cells only (no row/cell tombstones, no TTLs, no complex columns) is
   compacted, read back, and asserted equal to the tuples obtained by merging the
   SAME inputs directly. This closes the hole where the `#[ignore]`d
   `differential_input_merge_write_fidelity` (cell-tombstone fixture) left the
   default gate unable to catch a *stable* write corruption on surviving live
   data. The fixture is asserted to have NO observability caveats, so this is a
   full observable-field equivalence gate (not a downgraded one).

### Env-gated (skipped by default)

4. `differential_vs_live_cassandra_env_gated` — runs only when
   `CQLITE_DIFFERENTIAL_CASSANDRA=1`. It compacts the same inputs with CQLite,
   then compares (Tier-1/2/3) against an operator-supplied Cassandra-compacted
   reference. The reference must be **exactly one** compacted SSTable, supplied
   via either of:

   - `CQLITE_DIFFERENTIAL_REFERENCE_DATA` — full path to the single
     `nb-*-big-Data.db` (explicit single Data.db path; takes precedence), or
   - `CQLITE_DIFFERENTIAL_REFERENCE_DIR` — a directory that must resolve to
     **exactly one** `nb-*-big-Data.db` (zero or multiple is a hard error).

   Requiring exactly one reference prevents a false pass where CQLite would
   otherwise be compared against a logical merge of several uncompacted reference
   tables. Booting a Cassandra 5.0 Docker image is slow, so this is opt-in
   exactly like the other Cassandra e2e tooling in `test-data/scripts/`. Without
   the flag it returns immediately.

### Pinned regressions (`#[ignore]`d — real defects this harness surfaced)

These are runnable on demand and document genuine, still-open defects in the
compaction round-trip. They are `#[ignore]`d so the default run stays green
*without fabricating a pass*:

- `differential_row_tombstone_wide_partition_regression` — a clustering-table
  **row tombstone**, after compaction, is mis-decoded on the next read (the
  per-row tombstone marker resurfaces as a partition-level tombstone with the
  wrong deletion timestamp and corrupts subsequent partition framing).
- `differential_input_merge_write_fidelity` — a row carrying a **cell
  tombstone** has its live sibling cells' write timestamps rewritten to the
  row's max timestamp instead of preserving the original write timestamp.

Both belong to the writer/reader work of epic #817; when fixed, remove the
`#[ignore]`.

## How "pass" maps to the #818 gate

A run **passes** iff, for every comparison performed:

- **Tier 2** logical equivalence holds (identical surviving tuples), AND
- **Tier 1** load-path validity holds (output is loadable by a live node).

Tier-3 byte diffs are printed for debugging and **never** decide pass/fail.

## How to run

Default (no Cassandra):

```bash
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo test --package cqlite-core --features write-support \
  --test issue_819_differential_compaction -- --nocapture
```

Run the pinned regressions on demand:

```bash
cargo test --package cqlite-core --features write-support \
  --test issue_819_differential_compaction -- --ignored --nocapture
```

Live-Cassandra differential (slow; requires Docker + a Cassandra-compacted
reference — **exactly one** compacted SSTable — for the SAME logical data):

```bash
# 1) Produce a Cassandra-compacted SSTable for the same logical data using the
#    project's Docker tooling under test-data/scripts/, then point the harness at
#    it via EITHER env var:

# Option A — explicit single Data.db path (takes precedence):
env CQLITE_DIFFERENTIAL_CASSANDRA=1 \
    CQLITE_DIFFERENTIAL_REFERENCE_DATA=/path/to/nb-1-big-Data.db \
    CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo test --package cqlite-core --features write-support \
  --test issue_819_differential_compaction \
  -- --nocapture differential_vs_live_cassandra_env_gated

# Option B — directory that must contain exactly one nb-*-big-Data.db:
env CQLITE_DIFFERENTIAL_CASSANDRA=1 \
    CQLITE_DIFFERENTIAL_REFERENCE_DIR=/path/to/cassandra/compacted/sstable/dir \
    CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo test --package cqlite-core --features write-support \
  --test issue_819_differential_compaction \
  -- --nocapture differential_vs_live_cassandra_env_gated
```

## Where it lives

- Harness test + comparators: `cqlite-core/tests/issue_819_differential_compaction.rs`
- Existing parity tooling it sits beside: `tools/sstabledump-validator/`,
  `tools/cqlite-validator/`
- Compaction mechanics it models on: `cqlite-core/tests/compaction_integration.rs`,
  `cqlite-core/tests/compact_command.rs`
