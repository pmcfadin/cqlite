# issue #3790 — `inet` / `time` multicell-collection ordering fixture

**Cassandra-written** SSTable pinning the ON-DISK ordering of `inet` and `time`
values used as non-frozen collection elements and map keys, plus the composite
(frozen tuple) case. This file is **the oracle**: the "observed on-disk order"
tables below are what Cassandra 5.0.2 actually wrote, read back out of the
committed `sstabledump` golden, and they are what an ordering test must assert.

- Schema (with the per-column rationale): `test-data/schemas/issue-3790-comparator-ordering.cql`
- Generator: `test-data/scripts/generate-issue-3790-comparator-ordering.sh`
- Backs issue #3790 (acceptance criteria 1–4).

## What is here

```
test-data/datasets/sstables/                     <- the checkout's committed corpus root
└── test_comparator_order/                       <- keyspace
    └── collection_order-3479a500a65e11f1895d413585556a46/      <- table dir
        ├── nb-1-big-Data.db            (701 B)   Cassandra 5.0 `nb` BIG, UNCOMPRESSED
        ├── nb-1-big-Data.db.jsonl      (4.0 KB)  sstabledump golden — the physical-dump oracle
        ├── nb-1-big-Statistics.db      + .db.txt (sstablemetadata rendering)
        ├── nb-1-big-{Index,Summary,Filter,CRC}.db
        ├── nb-1-big-TOC.txt
        └── nb-1-big-Digest.crc32
```

Keyspace `test_comparator_order`, table `collection_order`. **No
`CompressionInfo.db`** — asserted by the generator, not assumed. Total committed
size **52 KB** on disk (~13.4 KB of file content).

## How a test resolves it

It sits in the **checkout's committed corpus**. The #3790 tests resolve it
**checkout-relative and deliberately do NOT use the TABLE-granular resolver**
(roborev job 70): `sstables_root_for_table` searches `$CQLITE_DATASETS_ROOT`
BEFORE the checkout, so an out-of-tree corpus carrying a
`test_comparator_order/collection_order-*` would SUBSTITUTE this oracle — and a
single external fixture is unambiguous inside its own root, so counting
candidates there cannot detect it either. A committed oracle must not consult an
external corpus root at all:

```rust
let ks = checkout_test_data_dir()
    .join("datasets")
    .join("sstables")
    .join("test_comparator_order");
// then require EXACTLY ONE `collection_order-<32 hex>` carrying both a
// *-Data.db and its *-Data.db.jsonl; ambiguity is a hard failure.
```

`sstables_root_for_table` walks EVERY candidate root (the `CQLITE_DATASETS_ROOT`
corpus first, then this checkout) and picks the one that actually carries
`<keyspace>/<table>-*/…-Data.db`, judged by a real `*-Data.db` and never by
directory existence. That is what makes a committed fixture findable on a fleet
box whose `CQLITE_DATASETS_ROOT` (e.g. `/data/datasets`) does not contain it —
the #3220 defect was a resolver that selected by KEYSPACE and then declared the
table absent. This fixture is git-committed, so per #3220 it is **`must_run`**: a
consumer must fail closed if it is not found, never skip.

### Corpus classification — READ THIS BEFORE ADDING A CONSUMER

Living under `test-data/datasets/sstables/` is not free. Per
`test-data/corpus-coverage-policy.md` every committed keyspace there is
**classified** — in-scope/enforced, skip-pending, or in the documented skip-set —
and an *unclassified* one **reds** the enumeration guard in each of the three
comprehensive harnesses.

`test_comparator_order` is classified as a **skip-set (parity-fixture)**
keyspace, the same category as `test_writeparity`, `test_signed_coll` and
`test_compaction_tombstone_ttl`: a fixture whose subject is one pinned property,
validated by a dedicated Rust test rather than by the comprehensive read-parity
corpus. Two reasons, and the second is the load-bearing one:

1. its subject is an ordering property that only the #3790 test knows how to
   assert; a row-count smoke pass over it would prove nothing about ordering; and
2. the comprehensive corpus is a **merge gate**, and this fixture exists because
   the ordering it pins is (or was) WRONG — enrolling a known-divergent fixture
   as enforced would red every lane's gate for a defect the fixture is
   documenting, not regressing.

The skip-set must be stated **identically in four places** (policy + three
harnesses), and all four are done:

- [x] `test-data/corpus-coverage-policy.md` — skip-set table row
- [x] `test-data/scripts/smoke-test-all-tables.sh` — `SKIP_KEYSPACE_NAMES` + reason
- [x] `bindings/python/tests/corpus.py` — `SKIP_KEYSPACES`
- [x] `bindings/node/__test__/parity-utils.js` — `SKIP_KEYSPACES`

### The enrollment is MEASURED, not assumed — and the before/after is the evidence

The question "does committing a keyspace here actually red the enumeration
guards, or is that only plausible?" was answered by running the guards' own
assertion bodies against the same inputs they use, once **before** the binding
entries existed and once **after**:

| guard | root | before enrollment | after enrollment |
|---|---|---|---|
| `corpus.py::unclassified_keyspaces` (asserted by `test_parity.py::TestCoverageSummary::test_every_discovered_keyspace_is_classified`) | checkout `test-data/datasets/sstables` | **RED** — `['test_comparator_order']` | GREEN — `[]` |
| same | `/data/datasets/sstables` (fleet corpus) | GREEN — keyspace not present there | GREEN |
| `parity-utils.js::unclassifiedKeyspaces` (asserted by `parity.test.js`) | both roots | n/a (entry already present when measured) | GREEN — `[]` |

Two facts worth keeping:

1. **The RED is real, and it is the DEFAULT.** `scripts/agent-gate.sh:928` resolves
   `CQLITE_DATASETS_ROOT="${CQLITE_DATASETS_ROOT:-$REPO_ROOT/test-data/datasets}"`,
   so on any box that does not export the variable the gate points the binding
   suites at **this checkout** — where the keyspace is present with 10 tracked
   files. `corpus.py`'s zero-tracked-files exemption (#1319) therefore does NOT
   apply to it. Without the two binding entries this fixture would be a latent,
   environment-dependent red: green wherever `CQLITE_DATASETS_ROOT` names a
   corpus that lacks the keyspace, red wherever it does not.
2. **The measurement is the guard's assertion body, not the full suite.**
   `bindings/python/tests/conftest.py` imports `cqlite`, and the Node suite
   imports the native module, so running the real pytest/jest cases needs the
   maturin/napi build this lane does not have. The predicates were evaluated
   directly, with `DATASETS`/`global.testPaths.SSTABLES_DIR` computed exactly as
   `conftest.py` and `setup.js` compute them, and those two lines are the entire
   content of both tests.

## Exactly how it was produced

```bash
bash test-data/scripts/generate-issue-3790-comparator-ordering.sh
```

- Image: `cassandra:5.0.2`, digest
  `sha256:9945dafdc759800f1e129ee871e45c9d3aa304fb5149148bde8685ae9812b81b`
  (pulled locally; no network fetch needed).
- The script starts a container named `cqlite-issue3790-cmporder` (fail-closed if
  that name already exists — a peer lane's Cassandra is never touched), applies
  the committed schema with `cqlsh -f`, runs the two `INSERT`s below via
  `cqlsh -k test_comparator_order -e`, then `nodetool flush test_comparator_order`,
  tar-streams `/var/lib/cassandra/data/test_comparator_order` out, and generates
  the golden with `sstabledump <Data.db> -l` and the `.txt` with
  `sstablemetadata`, both inside the same image.
- Asserted by the generator: exactly ONE `Data.db` (single flush), NO
  `CompressionInfo.db`, and the golden mentions all five columns and all eight
  ordering-bearing literals.

### The values inserted

Both statements carry `USING TIMESTAMP 1000`. **No TTL and no
`default_time_to_live`** (explicitly `0`), so this fixture cannot time-bomb. The
literals are written in an order that is **neither** the byte order **nor** the
string order, so no observed on-disk order can be an artefact of insertion
sequence.

Row `id = 1` — the full case:

| column | value |
|---|---|
| `inet_set` | `{'192.168.0.1', '9.0.0.1', 'fe80::1', '10.0.0.2', '::1', '2001:db8::1'}` |
| `inet_map` | `{'192.168.0.1':'v4-private', '9.0.0.1':'v4-nine', 'fe80::1':'v6-linklocal', '10.0.0.2':'v4-ten', '::1':'v6-loopback', '2001:db8::1':'v6-doc'}` |
| `time_set` | `{'12:00:00.000000000', '00:00:09.000000000', '23:59:59.999999999', '00:00:00.000000000', '00:00:10.000000000'}` |
| `time_map` | `{'12:00:00.000000000':'t-noon', '00:00:09.000000000':'t-nine-sec', '23:59:59.999999999':'t-max', '00:00:00.000000000':'t-midnight', '00:00:10.000000000':'t-ten-sec'}` |
| `pair_set` | `{('192.168.0.1','12:00:00.000000000'), ('9.0.0.1','23:59:59.999999999'), ('10.0.0.2','00:00:09.000000000'), ('2001:db8::1','00:00:10.000000000'), ('10.0.0.2','00:00:00.000000000')}` |

Row `id = 2` — the minimal falsifying pair:

| column | value |
|---|---|
| `inet_set` | `{'10.0.0.2', '9.0.0.1'}` |
| `inet_map` | `{'10.0.0.2':'pair-ten', '9.0.0.1':'pair-nine'}` |
| `time_set` | `{'00:00:10.000000000', '00:00:09.000000000'}` |
| `time_map` | `{'00:00:10.000000000':'pair-ten-sec', '00:00:09.000000000':'pair-nine-sec'}` |
| `pair_set` | `{('10.0.0.2','00:00:09.000000000'), ('9.0.0.1','00:00:10.000000000')}` |

## THE ORACLE — observed on-disk ordering

Read out of the committed golden `nb-1-big-Data.db.jsonl` (cell `path` entries in
file order, i.e. the order Cassandra wrote the cells of each complex column).
`sstabledump` renders an inet cell path as text and escapes `:` inside a tuple
path as `\:`; the **byte** column below is the serialized value Cassandra
actually compares (`InetAddressType`/`TimeType` are both
`ComparisonType.BYTE_ORDER`, verified at tag `cassandra-5.0.8`).

### `inet_set` and `inet_map`, row `id = 1` — identical order in both columns

| # | value | golden `path` | serialized bytes (hex) | len |
|---|---|---|---|---|
| 1 | `::1`         | `0:0:0:0:0:0:0:1`      | `00000000000000000000000000000001` | 16 |
| 2 | `9.0.0.1`     | `9.0.0.1`              | `09000001`                         | 4 |
| 3 | `10.0.0.2`    | `10.0.0.2`             | `0a000002`                         | 4 |
| 4 | `2001:db8::1` | `2001:db8:0:0:0:0:0:1` | `20010db8000000000000000000000001` | 16 |
| 5 | `192.168.0.1` | `192.168.0.1`          | `c0a80001`                         | 4 |
| 6 | `fe80::1`     | `fe80:0:0:0:0:0:0:1`   | `fe800000000000000000000000000001` | 16 |

`inet_map` values follow the same order: `v6-loopback`, `v4-nine`, `v4-ten`,
`v6-doc`, `v4-private`, `v6-linklocal`.

That is **unsigned byte-wise order** — first bytes `0x00 < 0x09 < 0x0a < 0x20 <
0xc0 < 0xfe` — and it is NOT the order CQLite's formatted-string comparison
produces, which is:

```
::1  <  10.0.0.2  <  192.168.0.1  <  2001:db8::1  <  9.0.0.1  <  fe80::1
```

(`"0000:..." < "10.0.0.2" < "192.168.0.1" < "2001:0db8:..." < "9.0.0.1" <
"fe80:..."` under `types.rs::fmt_inet`, which renders IPv4 dotted-decimal and
IPv6 as eight zero-padded hex groups).

**`9.0.0.1` is at position 2 on disk and at position 5 by string.** Three
independent falsifying pairs, inverting in both the v4/v4 and the v4/v6
direction, so no single coincidence can satisfy them all:

| pair | Cassandra (bytes) | CQLite (string) |
|---|---|---|
| `9.0.0.1` vs `10.0.0.2`    | `9.0.0.1` first (`0x09 < 0x0a`) | `10.0.0.2` first (`'1' < '9'`) |
| `2001:db8::1` vs `192.168.0.1` | `2001:db8::1` first (`0x20 < 0xc0`) | `192.168.0.1` first (`'1' < '2'`) |
| `9.0.0.1` vs `2001:db8::1` | `9.0.0.1` first (`0x09 < 0x20`) | `2001:db8::1` first (`'2' < '9'`) |

Row `id = 2` is the same property in two elements: on disk `9.0.0.1` then
`10.0.0.2` (`pair-nine` then `pair-ten`); by string the reverse.

### `time_set` and `time_map`, row `id = 1` — identical order in both columns

| # | golden `path` | nanoseconds since midnight | 8-byte big-endian (hex) |
|---|---|---|---|
| 1 | `00:00:00.000000000` | `0`              | `0000000000000000` |
| 2 | `00:00:09.000000000` | `9000000000`     | `0000000218711a00` |
| 3 | `00:00:10.000000000` | `10000000000`    | `00000002540be400` |
| 4 | `12:00:00.000000000` | `43200000000000` | `0000274a48a78000` |
| 5 | `23:59:59.999999999` | `86399999999999` | `00004e94914effff` |

Those five hex values are not hand-derived: they are the 8-byte components
CQLite read back out of this fixture's own `pair_set` tuple elements (see the
read-back section below), cross-checked against `"%016x" % nanos`. An earlier
draft of this table carried three hand-computed values that were WRONG; an
oracle with wrong bytes in it is worse than no oracle, so derive them from the
fixture, never by hand.

`time_map` values follow the same order: `t-midnight`, `t-nine-sec`, `t-ten-sec`,
`t-noon`, `t-max`. Row `id = 2`: `00:00:09` then `00:00:10` (`pair-nine-sec` then
`pair-ten-sec`).

**HONEST SCOPE — read this before writing a `time` assertion.** CQLite renders
`time` as `TIME(HH:MM:SS.nnnnnnnnn)` (`types.rs::fmt_time`), which is fixed-width
and zero-padded, so over the whole valid range `0..=86399999999999` the string
order and the nanosecond order **coincide**. There is therefore **no `time` value
pair that falsifies the current formatted-string implementation**, and this
fixture does not claim one. What the `time` columns do provide:

1. a **value-order pin** over Cassandra-written bytes, so a future
   re-implementation that breaks nanosecond ordering reds; and
2. a **falsifier for a decimal-nanosecond string comparison** — the other
   plausible wrong implementation: `9000000000 < 10000000000` numerically while
   `"9000000000" > "10000000000"` lexicographically. That is exactly why
   `00:00:09` and `00:00:10` are in the value set.

Anyone reporting this fixture as catching a `time` string-ordering bug would be
overclaiming; `inet` is the column that catches the live defect.

### `pair_set` — the composite case (`SET<FROZEN<TUPLE<INET, TIME>>>`)

Row `id = 1`, golden `path` in file order (`\:` is sstabledump's escaping of a
`:` inside the tuple path, and the tuple's two components are separated by an
unescaped `:`):

| # | tuple | golden `path` |
|---|---|---|
| 1 | (`9.0.0.1`, `23:59:59.999999999`)     | `9.0.0.1:23\:59\:59.999999999` |
| 2 | (`10.0.0.2`, `00:00:00.000000000`)    | `10.0.0.2:00\:00\:00.000000000` |
| 3 | (`10.0.0.2`, `00:00:09.000000000`)    | `10.0.0.2:00\:00\:09.000000000` |
| 4 | (`2001:db8::1`, `00:00:10.000000000`) | `2001\:db8\:0\:0\:0\:0\:0\:1:00\:00\:10.000000000` |
| 5 | (`192.168.0.1`, `12:00:00.000000000`) | `192.168.0.1:12\:00\:00.000000000` |

Two facts make this the composite oracle #3790 AC4 asks for:

- The **first** component decides between distinct inets, in the same byte order
  as above (`9.0.0.1` before `10.0.0.2` before `2001:db8::1` before
  `192.168.0.1`) — so a tuple *containing* an `inet` inherits the defect, which is
  the scalar-leaf delegation the issue names.
- Entries 2 and 3 share the inet `10.0.0.2` and differ **only** in the `time`
  component, and they are ordered `00:00:00` before `00:00:09`. So the second
  component's comparator is load-bearing here and not merely carried along.

Row `id = 2`: `9.0.0.1:00\:00\:10...` then `10.0.0.2:00\:00\:09...` — the first
component decides, and it decides against the time components' order, which
would be reversed. A comparator that (wrongly) compared the rendered *whole tuple
path* as one string would put `10.0.0.2...` first.

## Residual: the golden is NOT byte-reproducible

Assigning a whole non-frozen collection — which is what an `INSERT` of a
collection column does — makes Cassandra write a **complex-column deletion**
ahead of the cells, one per collection (five per row here):

```json
{"name":"inet_map","deletion_info":{"marked_deleted":"1970-01-01T00:00:00.000999Z",
                                    "local_delete_time":"2026-09-01T23:38:17Z"}}
```

`marked_deleted` is pinned by `USING TIMESTAMP 1000` (timestamp − 1 = 999 µs), but
`local_delete_time` is a wall clock (`nowInSeconds`) that no CQL clause can pin,
so **that one field differs on every regeneration**. The same residual
`issue-3504-udt-collision.cql` and `issue-3630-row-collision.cql` record for
their tombstones.

Do not byte-compare this golden across regenerations: compare the cell **paths**
(which is what the ordering oracle is about) and the values, or normalise
`local_delete_time` away first. The tombstone is **kept rather than avoided** — it
is what a real `INSERT` into a non-frozen collection produces, so removing it
(by populating with `UPDATE ... SET col = col + {...}`, which writes no complex
deletion) would buy reproducibility at the cost of the shape users actually have
on disk.

## Verified to READ

`stat`-ing files proves nothing; the reader is the judge. Confirmed with the
committed binaries in place:

```
cargo run -p cqlite-cli --bin cqlite -- read-sstable \
  test-data/datasets/sstables/test_comparator_order/collection_order-*/nb-1-big-Data.db \
  --format json
```

Result, on the committed fixture with this branch's working tree (the
`ComparatorType::compare` fix `ec6335bcd` already applied — see the caveat
below, which is why that does not make this a *proof* of the fix):

- exit `0`, **2 entries** (`Displayed 2 entries (total: 2, skipped: 0)`) — a
  non-zero row count with the collection contents intact, so this is not the
  silent 0-rows-when-present failure. Re-verified from a fresh
  `git worktree add --detach HEAD`, not from the dirty tree that produced it.
- `inet_set` came back as
  `{::1, 9.0.0.1, 10.0.0.2, 2001:db8::1, 192.168.0.1, fe80::1}` and `inet_map` as
  `{::1: v6-loopback, 9.0.0.1: v4-nine, 10.0.0.2: v4-ten, 2001:db8::1: v6-doc,
  192.168.0.1: v4-private, fe80::1: v6-linklocal}` — i.e. **the on-disk byte
  order above**. `time_set`/`time_map` likewise.
- `pair_set` elements came back as **undecoded tuple blobs**
  (`0x00000004090000010000000800004e94914effff`, …) on this path — a
  4-byte-length-prefixed inet component followed by an 8-byte-length-prefixed
  time component, which is where the verified `time` hex above comes from.

**WHAT THIS READ-BACK DOES AND DOES NOT ESTABLISH.** It establishes that the
committed bytes PARSE and yield both rows with all five collections populated —
a liveness check on the fixture, which is the only claim made for it here. It
does **not** establish that the ordering fix works, for two independent reasons,
and a test author should treat both as things to determine rather than assume:

1. the observation was taken with the fix already applied, so a correct result is
   consistent with both a fixed comparator and a path that never calls one; and
2. a single-SSTable dump emits cells in the order it read them off disk, so if
   this path does not re-order, it would print the correct sequence with or
   without the fix.

Which entrypoint actually consults `ComparatorType::compare` for a multicell
collection (and `compare_composite`'s scalar-leaf delegation for `pair_set`) is
a question about CQLite's read path, not about this fixture; establish it before
choosing where to assert, or the assertion may pass vacuously. The ordering
ORACLE — the tables above — is independent of that choice: it comes from
Cassandra-written bytes and from `cassandra-5.0.8` source, never from CQLite's
own output.
