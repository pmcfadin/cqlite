# Issue #3847 — corpus before/after census

Acceptance criterion: *"run the 144-file corpus before/after census (a table that
gains rows is the expected direction here, and must be explained)."*

**This is a RE-RUN at the final sha (roborev job 99, finding 1).** The first census
measured its AFTER leg at `13401947e`, and six production commits landed after it —
including the marshal-resolver changes of jobs 97/98, which are exactly the code a
non-regression claim has to cover. Worse, its BEFORE leg was `b2132fb2c`, which
predates this branch's rebase: the merge-base is now `fb889e227`, so **both** legs
were stale and re-running only the AFTER leg would have compared against a base that
no longer exists. A measurement artifact is a fact about ONE TREE, and every commit
after it widens the gap between what it claims and what it measured.

## Method

Subject set **DISCOVERED ON DISK**, never a hard-coded count:
`find $CQLITE_DATASETS_ROOT -name '*-Data.db'` → **155 files**
(`CQLITE_DATASETS_ROOT=/data/datasets`, the root
`test-data/scripts/fetch-datasets.sh --verify-only` names; the criterion's "144" is a
count from an earlier corpus pin). Per file, two observables:

1. the **row count**, `jq 'length'` over `cqlite read-sstable <Data.db> --format json`;
2. a **hash of the whole rendered JSON** (`sha256sum`, first 16 hex) — so a changed
   VALUE at an unchanged row count is visible, which a row count alone cannot see.

Two `cqlite-cli` debug binaries, one per tree, both run from one scratch cwd (the CLI
writes a `cqlite.db` beside itself, which must not land in either worktree):

| leg | tree | commit |
|---|---|---|
| BEFORE | `git worktree --detach` at `merge-base(HEAD, origin/main)` | `d659de8fc` |
| AFTER | this branch | `412fcd92e` |

Only `docs/round-artifacts/**` changes after `412fcd92e` (this file and its two TSVs),
so the AFTER leg covers every code commit on the branch.

## Result: byte-for-byte identical

```
files=155  decoded=141  no-output=10  corruption-fixtures=4  total-rows=21000
diff BEFORE AFTER  ->  0 lines
```

No table gained rows, none lost rows, and **no rendered output changed** — the two
censuses are identical on all 155 files, hashes included.

### Three states, counted honestly

The previous artifact reported `decoded=151 ... zero-row=0 error=4`, which folded ten
files that **emit no output at all** into "decoded". That overstated the result, so the
accounting here is three-way:

- **141 files decode to a row count**, totalling 21000 rows, **none of them 0** —
  this is the non-vacuity evidence, measured rather than asserted. A corpus-less root
  would have produced 0 files or 0 rows and is distinguishable from this.
- **10 files emit nothing** (`ERROR` in the TSVs, rendered-JSON hash
  `e3b0c44298fc1c14` = sha256 of empty input): the `system` / `system_schema`
  metadata tables (`sstable_activity`, `sstable_activity_v2`, `aggregates`,
  `column_masks`, `functions`, `triggers`, `types`, `views`) plus two tombstone-only
  test tables (`test_tomb/skipped_partition_delete` `nb-2`,
  `test_types/ct_deleted_counter_shadowing` `nb-2`). Identical on both legs.
- **4 deliberately corrupted fixtures** under `corruption/test_comp_corrupt/`
  (`compression_info_bad_offset`, `data_db_bit_flip`, `data_db_truncation`,
  `uncompressed_data_bit_flip`). They fail identically on both legs: this change
  neither repaired nor broke a corruption fixture.

**This run does not separate "no output" from "non-zero exit"** — the label is one
`ERROR` covering both — because the two are distinguished by a return code this
harness did not capture per file. The prior run's `UNPARSEABLE` / `ERROR(rc=5)` split
is preserved in git history if that distinction is ever needed. Stated rather than
papered over: what is established here is per-file OUTPUT equality between the two
trees, which is the non-regression property, not a diagnosis of the 14.

### A base-vs-head census is INVARIANT to `main` advancing — measured, not argued

This census was run twice: once at base `fb889e227` / head `6a3b1c1d5`, then again after
a rebase at base `d659de8fc` / head `412fcd92e` — **27 `main` commits apart**, one of
which (#3644, `29d0ae533`) changed CLI JSON egress for `decimal` and `varint`. Both
TSVs came out **byte-identical** across the two runs.

That is the empirical form of the reason this artifact does not need re-running every
time `main` moves: the comparison is BASE vs HEAD, so a change on `main` moves **both**
legs and cancels. What invalidates it is a change to **this branch's own diff** — which
is exactly what happened the first time (the AFTER leg predated six of this branch's
own commits) and is the only staleness that matters. Stated as a measurement rather
than an argument because a property nobody measured is a property nobody has.

### The hash column is not comparable to the PREVIOUS artifact

The recorded hashes differ from the superseded run's for the same files (e.g.
`bti_partitions_footer_flip`: `de6511d8a9b3c22f` here, `6ec78e40b8498b30` before) at
an unchanged row count. That is expected and is the reason the re-run was necessary:
the baseline moved from `b2132fb2c` to `fb889e227`, so `main` changed the rendering in
between. **Within this run both legs agree exactly**, which is the only comparison a
non-regression claim rests on. Cross-run hash comparison is meaningless once the base
has moved.

Reproducibility spot-checked after the fact: the two `da-2-bti` rows were re-measured
against a freshly rebuilt CLI and reproduced their recorded count and hash exactly.

## Why "no change" is the CORRECT result here, stated rather than assumed

The acceptance criterion anticipated a table GAINING rows. It did not happen, and the
reason is that the widened accepted set is **not exercised by this corpus**: no fixture
carries a zero-length component for a fixed-width scalar (a zero-length
frozen-collection element, tuple/UDT component or UDT field). The widening is therefore
a **latent-correctness** fix — it makes CQLite read a shape Cassandra can legitimately
write, and which this corpus happens not to contain — and its evidence is the unit
oracle (`raw_value/fixed_width.rs`'s pinned `cassandra-5.0.8` `deserialize` table and
the 28 cases in the two `issue_3847_empty_fixed_width_tests.rs` files), **not** the
census.

What the census DOES establish is the thing that could have gone wrong: a widening of a
width guard could have admitted garbage that previously errored, changed a decoded
value, or shifted a row count. It did none of those. **This is not a regression.**
