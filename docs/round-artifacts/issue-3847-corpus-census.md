# Issue #3847 — corpus before/after census

Acceptance criterion: *"run the 144-file corpus before/after census (a table that
gains rows is the expected direction here, and must be explained)."*

## Method

Subject set **DISCOVERED ON DISK**, never a hard-coded count (CLAUDE.md #1229):
`find $CQLITE_DATASETS_ROOT -name '*-Data.db'` → **155 files** on this box
(`CQLITE_DATASETS_ROOT=/data/datasets`, the root
`test-data/scripts/fetch-datasets.sh --verify-only` names; the criterion's "144"
is a count from an earlier corpus pin). Per file, two observables:

1. the **row count**, `jq 'length'` over `cqlite read-sstable <Data.db> --format json`;
2. a **hash of the whole rendered JSON** (`sha256sum`, first 16 hex) — so a
   changed VALUE at an unchanged row count is visible, which a row count alone
   cannot see.

Two `cqlite-cli` debug binaries, one per tree:

| leg | tree | commit |
|---|---|---|
| BEFORE | `git worktree --detach` at `merge-base(HEAD, origin/main)` | `b2132fb2c` |
| AFTER | this branch | `13401947e` |

Both legs ran from one scratch cwd (the CLI writes a `cqlite.db` beside itself,
which must not land in either worktree).

## Result: byte-for-byte identical, and NOT vacuous

```
files=155  decoded=151  zero-row=0  error=4  total-rows=21000
diff BEFORE AFTER  ->  0 lines
```

- **No table gained rows, no table lost rows, and no rendered output changed** —
  the two censuses are identical on every one of the 155 files, hashes included.
- The 4 `ERROR(rc=5)` files are the DELIBERATELY corrupted fixtures
  (`corruption/test_comp_corrupt/{compression_info_bad_offset,data_db_bit_flip,`
  `data_db_truncation,uncompressed_data_bit_flip}`). They fail identically on both
  legs: this change neither repaired nor broke a corruption fixture.
- **Non-vacuity is measured, not asserted**: 151 files decoded, **zero** of them
  to 0 rows, 21000 rows total. A corpus-less root would have produced 0 files or
  0 rows and is distinguishable from this.

## Why "no change" is the CORRECT result here, stated rather than assumed

The acceptance criterion anticipated a table GAINING rows. It did not happen, and
the reason is that the widened accepted set is **not exercised by this corpus**:
no fixture carries a zero-length component for a fixed-width scalar (a
zero-length frozen-collection element, tuple/UDT component or UDT field). The
widening is therefore a **latent-correctness fix** — it makes CQLite read a shape
Cassandra can legitimately write, and which this corpus happens not to contain —
and its evidence is the unit oracle (`raw_value/fixed_width.rs`'s pinned
`cassandra-5.0.8` `deserialize` table, and the 20 cases in the two
`issue_3847_empty_fixed_width_tests.rs` files), not the census.

What the census DOES establish, which is the thing that could have gone wrong: the
change is not a REGRESSION. A widening of a width guard could have admitted
garbage that previously errored, changed a decoded value, or shifted a
consumption offset and truncated a row — `row_data.rs` `break`s its column loop
on a failing column, so a mis-wired guard presents as a quietly truncated row
rather than a failure. Identical hashes over 21000 rows is the measurement that
rules that out.

## Declared limits of this census

- It reads the **corpus present on this box** (155 `*-Data.db`). A fixture set
  containing a zero-length fixed-width component would move numbers here; none is
  known to exist, and generating one is a WRITE-side exercise this change does not
  attempt.
- `read-sstable --format json` renders values as strings, so the hash is over
  CQLite's rendering, not over typed values. It cannot distinguish two values with
  one rendering — sufficient for a regression check, not an oracle for
  correctness (which is what the Cassandra-pinned unit cases are for).
