# Design — byte-bounded Arrow egress batches (issue #2825, T4/M11)

## Context

Both egress build sites accumulate the **same** concrete type and flush through
the **same** converter, which makes a single shared cap mechanism natural:

| | buffer decl | push | row-cap trip | flush |
|---|---|---|---|---|
| `producer.rs` | `:888` `let mut buffer: Vec<QueryRow>` | `:949` | `:951` | `:1222` `fn flush_buffer(&self, buffer: &mut Vec<QueryRow>) -> Result<RecordBatch, ProducerError>` |
| `producer_stream.rs` | `:118` `let mut buffer: Vec<QueryRow>` | `:204` | `:206` | `:87` `fn flush(&self, buffer: &mut Vec<QueryRow>) -> Result<RecordBatch, ProducerError>` |

The buffered element is `cqlite_core::query::QueryRow`
(`cqlite-core/src/query/result.rs:67`):

```rust
pub struct QueryRow {
    pub values: HashMap<Arc<str>, Value>,
    pub key: RowKey,
    pub metadata: RowMetadata,
    pub cell_metadata: Option<HashMap<String, CellWriteMetadata>>,
}
```

Conversion is `rows_to_record_batch(columns: &[ColumnInfo], rows: &[QueryRow])`
(`cqlite-core/src/export/arrow_convert.rs:197`) → `transpose_columns`
(`export/arrow_columnar.rs:59`, borrowing `Option<&Value>` per cell) →
`convert_column_to_array` (`arrow_convert.rs:1322`), which dispatches on
`ColumnInfo::cql_type`. Only `values` is read; `key`, `metadata` and
`cell_metadata` never reach the batch.

Variable-width Arrow outputs (the wide-row case that matters): `Text`/`Ascii`/
`Varchar`/`Duration`/`Inet`/`Custom` → `Utf8`; `Blob` → `Binary`; `List`/`Set` →
`ListArray`; `Map` → `MapArray`; `Tuple`/`Udt` → `StructArray`. Fixed-width:
`Uuid`/`TimeUuid` → `FixedSizeBinary(16)`, `Decimal`/`Varint` → `Decimal128`,
`Date`/`Time`/`Counter`/numerics → fixed primitives.

The decisions below each state alternatives; §Recommended package selects one
coherent set for Seam-1 approval. **§(b) and §(c) each surface a contradiction with
the figures carried in issue #2825 and in the M11 manifest line; both are called
out explicitly because they change the arithmetic downstream (#2821) depends on.**

---

## (a) Where the byte decision is made: post-batch vs. pre-batch accumulation

- **Post-batch, via `RecordBatch::get_array_memory_size()`.** The existing
  measurement seam (`streaming.rs:647`) uses exactly this. But it can only be read
  *after* `rows_to_record_batch` has already allocated and copied every value —
  discovering a 512 MiB batch is oversized after paying for it is not a cap, it is
  a report. It would also force a re-split (build, measure, discard, rebuild),
  turning the "~1.0–1.1× throughput" budget into a guaranteed regression.
- **Pre-batch, via a running per-row estimate.** Maintain a `usize` accumulator
  next to `buffer`; on each `buffer.push(row)` add
  `estimate_arrow_row_bytes(&self.columns, &row)`; trip the flush when the
  accumulator would exceed the cap. Costs one extra pass over the row's values —
  the same asymptotic order the conversion already pays, and on the *narrow* path
  the accumulator never trips, so the cost is a few adds per row.
- **Hybrid: estimate to decide, `get_array_memory_size()` to verify in tests.**
  Production uses the estimate; the test suite asserts the realised batch against
  the estimate so the two can never silently diverge.

**Recommendation: pre-batch accumulation, with `get_array_memory_size()` retained
only as the test-side oracle** (the hybrid). This is the only option that bounds
allocation rather than reporting it.

## (b) The cap's currency — payload bytes vs. Arrow buffer capacity

**This is the crux, and it contradicts the naive reading of the acceptance
criteria.** `get_array_memory_size()` sums Arrow `Buffer::capacity()`, not
buffer length. `arrow_convert.rs:666/686` build variable-width columns with
`StringArray::from(Vec<Option<&str>>)` / `BinaryArray::from(Vec<Option<&[u8]>>)`,
which route through arrow-53's `FromIterator` → `MutableBuffer::new(0)` +
`reserve`, and `MutableBuffer::reserve` grows to
`max(round_upto_multiple_of_64(required), capacity * 2)` — **power-of-two
doubling from zero**. Reported memory therefore exceeds payload by a factor that
depends on where the payload lands relative to the next power of two, approaching
2× in the worst case.

Measured directly against this tree's arrow 53 (throwaway probe, not committed):

| shape | payload bytes | `get_array_memory_size()` | ratio |
|---|---:|---:|---:|
| `BinaryArray` 8192 × 300 B | 2,457,600 | 4,227,256 | **1.720** |
| `BinaryArray` 8192 × 180 B | 1,474,560 | 2,130,104 | **1.445** |
| `BinaryArray` 512 × 8192 B | 4,194,304 | 4,196,536 | 1.001 |
| `BinaryArray` 100 × 64 KiB | 6,553,600 | 8,389,176 | 1.280 |
| `StringArray` 8192 × 290 B | 2,375,680 | 4,227,256 | **1.779** |
| `StringArray` 8192 × 20 B | 163,840 | 295,096 | **1.801** |

Options:

- **Cap on `get_array_memory_size()` (capacity currency).** Matches the quantity
  `streaming.rs:647` already meters and the quantity #2821 would naturally budget.
  But it is **not computable before the batch exists** — it is a property of an
  allocator's growth policy, not of the data — so it cannot be the trigger (§a).
  It is also jumpy: the same payload can report 1.0× or 1.8× depending on
  power-of-two proximity, making a cap on it non-monotonic in row count.
- **Cap on payload bytes (buffer lengths).** Estimable from the rows in hand,
  monotonic in row count, and stable across arrow versions and allocator policy.
  The cost is that the guarantee must be *stated in payload terms*, with the
  capacity relationship published as a separate, named factor.

**Recommendation: the cap is normatively defined over Arrow *payload* bytes**,
with a published companion constant `BATCH_BYTES_CAPACITY_FACTOR = 2` and a
per-column fixed slack, so any consumer (notably #2821) can convert:
`worst_case_get_array_memory_size ≤ 2 × cap + slack`.

**Contradiction recorded (for the owner at Seam 1).** The task framing states
"a 4 MiB batch cap + an 8 MiB egress ceiling = **12 MiB**, inside B4 ≤16Mi, with
headroom." That arithmetic is only true in *payload* currency. `streaming.rs:647`
— the seam #2821 will extend — meters **capacity**. In capacity currency a 4 MiB
payload cap is worst-case ≈ 8 MiB, so `8 MiB ceiling + 8 MiB batch = 16 MiB` —
**exactly B4, with zero headroom**, not 12 MiB with headroom. Three ways out, for
the owner to pick:

1. **Keep the 4 MiB payload default; #2821 budgets in capacity currency** and sets
   its ceiling to ≈6 MiB so `6 + 8 = 14 MiB < 16Mi`. This change publishes the
   factor; #2821 does the arithmetic. *(Recommended — keeps #2825's default as
   decided and localises the correction to the dependent.)*
2. **Drop the default to 2 MiB payload** (worst-case ≈4 MiB capacity), restoring
   the literal "4 MiB batch + 8 MiB ceiling = 12 MiB" figure in capacity currency.
   Safe against every *measured* narrow shape in-tree (§c) but only ~1.4× above the
   field-model narrow batch, which is thinner headroom than the criterion "no
   throughput regression on narrow rows" deserves.
3. **Have #2821 meter payload bytes too**, converting `streaming.rs:647`'s
   accounting. Cleanest currency-wise but changes an existing metric's meaning —
   out of scope here and a breaking observability change.

This change is written to **option 1**. Options 2 and 3 are recorded so the Seam-1
decision is on the record rather than implied.

## (c) The default — is 4 MiB safe for narrow rows?

The criterion "no throughput regression on narrow rows" reduces to: *on narrow
shapes the row-cap must still trip first*, i.e. `8192 × narrow_row_bytes < cap`.

**Contradiction recorded.** The "~300 B/row → ~2.4 MB batch" figure in the task
framing is arithmetically right (`8192 × 300 = 2,457,600 B = 2.34 MiB`) but the
300 B/row input is the weakest number in the research corpus and is internally
contradicted. It derives from `docs/research/phase1-6-parallelism.md:261`, itself
a throughput *ratio* (`phase0-scan-cost-breakdown-2026-07.md:74`: 880 MB ÷ 3 M
rows = 293 B/row) — not a measured row width. The same corpus records **~82 B/row
uncompressed on disk** (`docs/research/phase1-3-linux-io.md:87`), and an 82 B
on-disk row cannot become a 293 B Arrow row (Arrow adds ~8 B of offsets over raw
text; the SSTable *adds* framing). `docs/research/phase1-5-transport-ingest.md:195`
independently models the same shape at **~180 B/row → 1.47 MB/batch**. The
generator that would settle it (`gen_bigtable`) is not in this tree.

The **in-repo** narrow shapes, measured, not modelled:

| narrow shape | bytes/row | full 8192-row batch | headroom to 4 MiB |
|---|---:|---:|---:|
| `test_fixtures.rs:51` `KEYVALUE_ROWS` (`k1`/`1`) | ~11 | — (3 rows exist) | — |
| `issue_1494` fixture `k{i:06}`/`v{i}` — **measured `get_array_memory_size` of a real 8192-row 2-col batch: 196,976 B** | ~20 | ~192 KiB | **~22×** |
| `many_partition_fixture` (`streaming_tests.rs:16`, `id int`/`name text` 1 char/`score int`) | ~13 | ~107 KiB | ~39× |
| field model, `phase1-5:195` | ~180 | 1.47 MB | **~2.9×** |
| the contested 300 B/row figure | 300 | 2.34 MiB | 1.7× |

Options: `1 MiB` (binds on the field model — rejected), `2 MiB` (§b option 2),
`4 MiB` (row-cap trips first under every estimate, including the contested one),
`8 MiB` (halves the B4 headroom for no narrow-path benefit).

**Recommendation: `DEFAULT_MAX_BATCH_BYTES = 4 * 1024 * 1024`.** The decided
default holds: even at the pessimistic 300 B/row the narrow row-cap still trips
first in payload currency (2.34 MiB < 4 MiB), and at the honest ≤180 B/row the
margin is ~2.9×. Two caveats to carry into the docs: (i) at the contested 300 B/row
the *capacity* reading of a full narrow batch is already 4,227,256 B — above 4 MiB
— which is precisely why the cap must be payload-denominated (§b), and (ii) gRPC's
default inbound limit is 4 MB and the Java client sets no `maxInboundMessageSize`
(`phase1-5:196-199`), so a cap set *at* 4 MiB of payload leaves the client's own
default limit no framing headroom — a note for the connector, not a blocker here
(IPC wire bytes ≠ payload bytes, and Flight's own client default already exceeds it).

## (d) The estimator — what to reuse, and why nothing existing is conservative

Three per-`Value` estimators exist. **None is conservative for this purpose as-is.**

1. `Value::size_estimate` — `cqlite-core/src/types.rs:930`, `pub`, recursive
   per-variant. Models the **serialized** size: adds a vint length prefix per
   variable-length value and counts UDT `type_name`/`keyspace`/field-name strings.
   For a short `Text` it adds a **1-byte** vint where Arrow spends **4 B of offset
   + a validity bit** → it *under-counts* a narrow text cell by ~3 B. Also uses
   plain `+`, not `saturating_add`, and has no recursion-depth cap.
2. `crate::memory::estimate_value_size` — `cqlite-core/src/memory/mod.rs:89`,
   `pub(crate)` and `#[cfg(feature = "state_machine")]`. Pure content bytes, no
   overhead at all (`Text(s) => s.len()`) → under-counts every variable-width cell
   by the full 4 B offset + validity bit.
3. `Memtable::estimate_value_size` — `storage/write_engine/memtable.rs:227`,
   private. The hardened one (iterative `SmallVec` worklist, `MAX_ESTIMATE_NODES`
   cap, fails closed to `usize::MAX`, all `saturating_add`) but unreachable and,
   like (2), content-only.

`estimate_query_row_bytes` (`cqlite-core/src/query/result_budget.rs:32`) is
`pub(crate)` in a `pub(crate) mod` (`query/mod.rs:29`), has exactly one call site
(`result_budget.rs:46`), and simply sums (2) over `row.values` plus
`row.key.as_bytes().len()`.

**Blast radius of exposing it:** small in edit count — the fn *and* `mod
result_budget` must both become `pub` (or a `pub use` added in `query/mod.rs`),
and the new public symbol must carry `#[cfg(feature = "state_machine")]` because
its callee is gated (`cqlite-flight` gets `state_machine` transitively via
`cqlite-core/features = ["arrow"]`, `cqlite-core/Cargo.toml:364`, so the flight
build is fine, but an ungated `pub` breaks `--no-default-features`). **But the
edit size is not the objection.** `estimate_query_row_bytes` is the wrong seam
on three counts: it is *Arrow-blind* (takes no `&[ColumnInfo]`, so it cannot know
which columns are projected or which CQL types become variable-width Arrow
arrays), it adds `row.key` bytes that never enter the batch, and it models no
structural overhead — so its error is signed **negative** exactly where it
matters. Exposing an under-estimator as the foundation of #2821's bound would
make that bound void.

Options: (i) expose `estimate_query_row_bytes` and add a fudge factor in
`cqlite-flight` — rejected, an unnamed fudge factor is not a contract;
(ii) write the estimator in `cqlite-flight` — rejected, the CQL→Arrow type mapping
lives in `export/`, and duplicating it there guarantees drift when
`convert_column_to_array` gains a type; (iii) a new estimator in `cqlite-core/src/export/`
beside the converter that owns the mapping.

**Recommendation: (iii).** A new module `cqlite-core/src/export/arrow_size.rs`
(new file — `arrow_convert.rs` is 2,596 lines, already far over the campsite
threshold), exporting

```rust
#[cfg(feature = "arrow")]
pub fn estimate_arrow_row_bytes(columns: &[ColumnInfo], row: &QueryRow) -> usize
```

re-exported from `export/mod.rs` next to `rows_to_record_batch`. It walks
`columns` (never the whole `values` map), resolves each cell the same way
`transpose_columns` does, and for each cell adds:

- the **content bytes** of the value (reusing `memory::estimate_value_size`'s
  per-variant shape, hardened like the memtable estimator: iterative, node-capped,
  `saturating_add` throughout, fails closed to a large value rather than panicking);
- a per-cell **structural addend** `ARROW_CELL_OVERHEAD_BYTES` covering the
  offsets entry (4 B for `Utf8`/`Binary`/`List`/`Map`), the buffer's trailing
  `n+1`-th offsets entry, and the validity bit (rounded up to 1 B) — the term
  every existing estimator omits;
- for collection/struct columns, a per-**element** structural addend, because a
  `ListArray` pays child offsets + child validity per element, not per row;
- a single per-**column** residual `ARROW_COLUMN_SLACK_BYTES` for array nodes
  that correspond to no slot (the flat `MapArray`'s always-present empty `Utf8`
  children). Charged per column, never per slot, so the estimate cannot inflate
  with a cell's element count (#2825 review B3).

Conservatism is a **contract, not an aspiration**: the spec requires a property
test over a corpus of shapes asserting
`Σ estimate_arrow_row_bytes(...) ≥ payload_bytes(rows_to_record_batch(...))`,
where `payload_bytes` sums buffer *lengths* (recursively over child data), so a
future type addition that the estimator does not know about fails the test rather
than silently under-counting. `result_budget.rs` is left untouched — no visibility
change, no blast radius.

## (e) Liveness: the one-row floor

If a single row's estimate exceeds the whole cap, "flush before the cap is
exceeded" would flush an empty buffer forever. The rule is therefore ordered:
**push first, then test** — a batch is cut when the buffer is non-empty *and* the
accumulated estimate has reached the cap. Consequences, all specified: a
2 GiB-blob row yields a one-row batch (never dropped); `--max-batch-bytes 0` and
`--max-batch-bytes 1` both degrade to one row per batch rather than hanging (the
same clamp posture as `batch_size.max(1)` at `producer.rs:422` / `service.rs:327`);
and the `i32::MAX` `checked_value_bytes` guard remains the fail-closed backstop for
the single row that cannot be represented at all.

## (f) Library-vs-binary posture for the knob

`CqliteFlightService::new` is deliberately **unconstrained** for admission
(`service.rs:304-316`: `new()` → `with_admission(.., Admission::unconstrained())`),
so library embedders keep pre-#2420 behaviour and only the binary opts in.

The byte-cap is different in kind: an unbounded batch is a *memory-safety* hazard,
not a policy choice, and the 4 MiB default is a no-op on every narrow shape. Making
it opt-in would leave every library embedder exposed to the very hazard the change
exists to close, and would leave #2821 unable to state a bound for the library path.

**Recommendation: the byte-cap defaults ON for every construction path**, including
`CqliteFlightService::new` and `MergeProducer::new`/`with_spec`. An embedder that
genuinely wants the old behaviour sets `usize::MAX`. This divergence from the
admission precedent is deliberate and is recorded here rather than discovered later.

## Recommended package (for Seam-1 approval)

1. Pre-batch accumulation; `get_array_memory_size()` is the **test oracle only** §(a).
2. Cap denominated in **Arrow payload bytes**, with `BATCH_BYTES_CAPACITY_FACTOR = 2`
   published so consumers can convert to capacity §(b). **#2821's 12 MiB arithmetic
   is revised to `6 MiB ceiling + 8 MiB worst-case batch = 14 MiB < 16Mi`** — a
   TARGET for #2821 to enforce, not a bound this change puts in force: until that
   per-stream byte ceiling exists, `do_get` residency stays COUNT-bounded at ~7
   batches (`~56 MiB` worst case), because `streaming.rs`'s
   `get_array_memory_size()` reading feeds metrics only.
3. `DEFAULT_MAX_BATCH_BYTES = 4 MiB` — the decided default, verified to leave the
   narrow row-cap binding under every available estimate §(c).
4. New `cqlite_core::export::estimate_arrow_row_bytes`; `result_budget.rs`
   untouched; conservatism enforced by a property test §(d).
5. Test-then-push one-row floor (revised from push-then-test in the #2825 review
   round, B1: the cut is decided BEFORE the crossing row is appended, bounding a
   batch at `max(cap, widest_row_payload)` rather than `cap + widest_row`);
   `0`/`1` clamp to one row per batch §(e).
6. Cap on by default on every construction path §(f).
7. `--max-batch-bytes` / `CQLITE_MAX_BATCH_BYTES` / `DEFAULT_MAX_BATCH_BYTES`,
   mirroring `admission.rs:43/51` + `main.rs:43`, echoed in the `main.rs:109-116`
   startup log.

## Placement and file-size (campsite rule, epic #1116)

Already over threshold and therefore to receive **minimal** edits (a field, a
parameter, an accumulator — all logic in new modules):
`cqlite-flight/src/producer.rs` (3,330), `cqlite-flight/src/service.rs` (2,036),
`cqlite-core/src/export/arrow_convert.rs` (2,596). Adding even a field grows them,
so the `file-size` ratchet will trip; the implementer re-runs with
`CQLITE_ALLOW_FILE_GROWTH=1` and leaves a note linking #1116, per CLAUDE.md.
`cqlite-flight/src/streaming.rs` (768/800) has ~35 lines of headroom — **do not**
put byte accounting there. New code lands in
`cqlite-core/src/export/arrow_size.rs` and `cqlite-flight/src/batch_bytes.rs`
(+ a `#[path]` sibling `batch_bytes_tests.rs`, the `admission.rs`/`admission_tests.rs`
precedent). `cqlite-flight/src/streaming_tests.rs` is at 1,336/1,500 — the wide-row
fixture and its tests go in the new sibling test file, not there.

## References

- Issue #2825 (T4 byte-bounded batch sizing); epic #2817; dependent issue #2821
  (streaming `do_get` result-budget wiring gap).
- `docs/architecture/throughput-program-2026-07.md` §4 lever T4 (`:158`), §7 M11
  (`:383-387`), B4 definition (`:41`).
- `docs/research/phase2-verify-transport.md` (T4 SURVIVES-minor),
  `docs/research/phase2-verify-parallelism.md:94-100` (the 57,344 correction).
- Precedent: `openspec/changes/archive/2026-07-14-flight-admission-control/`
  (const + env + clap + service-field knob chain).
