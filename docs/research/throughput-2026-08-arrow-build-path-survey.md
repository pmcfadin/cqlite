# Lane 3 — Row→Arrow build cost in production Rust/Arrow systems

**2026-08-04, owner-commissioned research survey (lane 3 of 3).** Synthesis + verdicts:
`throughput-2026-08-research-synthesis.md`. Claims tagged **[MEASURED]** (published number),
**[SOURCE]** (read from arrow-rs source/API docs — a fact about the code), or **[FOLKLORE]**
(repeated, no primary measurement). Our numbers (1.52 µs/row Flight overhead, 313 ns/row IPC
framing, ~1.2 µs/row unattributed build) treated as ground truth.

## 1. arrow-rs builder economics

- **`finish()` does not retain capacity** — it is `mem::take(&mut self.buffer)`; the builder gets a
  new empty buffer **[SOURCE]**
  ([arrow-buffer builder](https://raw.githubusercontent.com/apache/arrow-rs/main/arrow-buffer/src/builder/mod.rs)).
  This kills the "reuse the builder object across batches" idea **[FOLKLORE]** — builders are
  reusable after `finish()` ([ARROW-4075](https://issues.apache.org/jira/browse/ARROW-4075)) but
  reuse buys the struct, not the allocation. Actionable form: **re-apply `with_capacity` every
  batch** from a known row count + byte estimate.
- **`BufferBuilder::append()` calls `reserve(1)` per element** **[SOURCE]** — a capacity branch per
  value; bulk paths (`append_slice`, `append_n`, `Extend`) amortize it. Capacity accessors added so
  downstreams can measure allocation slack ([#10342](http://www.mail-archive.com/commits@arrow.apache.org/msg63672.html)).
- **`GenericByteBuilder::with_capacity(item_capacity, data_capacity)`** takes offsets AND value
  bytes; `append_value` takes `impl AsRef<[u8]>` — **borrowed bytes are accepted; no owned
  `String`/`Vec<u8>` required**. Panics at i32 offset overflow (2 GB/array) **[SOURCE]**
  ([docs](https://docs.rs/arrow-array/latest/arrow_array/builder/struct.GenericByteBuilder.html)).
- **Nested builders are the documented dispatch trap**: `StructBuilder` stores children as
  `Box<dyn ArrayBuilder>`; `field_builder::<T>(i)` is a **runtime downcast**; `len()` checks only
  the first child. `List<Struct<…>>` downcasts through `ListBuilder<Box<dyn ArrayBuilder>>`
  **[SOURCE]** ([docs](https://docs.rs/arrow-array/latest/arrow_array/builder/struct.StructBuilder.html)).
  Per-cell downcast is the pathological shape.
- **View builders offer real zero-copy**: `GenericByteViewBuilder::append_block(Buffer)` +
  `try_append_view(block, offset, len)`; values ≤ ~12 B inlined into the 16-byte view; block growth
  8 KiB → 2 MiB ([#6136](https://github.com/apache/arrow-rs/pull/6136)) **[SOURCE]**
  ([docs](https://docs.rs/arrow-array/latest/arrow_array/builder/struct.GenericByteViewBuilder.html)).
- **StringView payoff**: 20–200% on string-heavy ClickBench; Parquet→`StringViewArray` 287.81 µs vs
  `StringArray` 345.03 µs (~1.2×) by reusing the source page buffer **[MEASURED]**
  ([DataFusion blog](https://datafusion.apache.org/blog/2024/09/13/string-view-german-style-strings-part-1/)).

## 2. Row→columnar in the wild

- **`arrow-row` is the opposite direction** (Arrow → comparable byte rows for sort/group)
  **[SOURCE]** ([docs](https://docs.rs/arrow-row/latest/arrow_row/)). Do not mine it.
- **`arrow-avro` is the closest published analogue** (row-oriented source → Arrow, in-tree,
  benchmarked). Design: per-field `Codec` decided once at schema-bind, then decode "column-by-column
  straight into Arrow builders," explicitly avoiding "deserializ[ing] one record at a time into
  native Rust values … then build[ing] Arrow arrays from those values" (extra allocations,
  cache-unfriendly) **[SOURCE]**. Measured against exactly that row-object path (`apache-avro`):
  **1 M rows 267.21 ms → 27.91 ms = 9.57×**; 10 K rows 2.60 → 0.24 ms = 10.8× **[MEASURED]**
  ([announcement](https://arrow.apache.org/blog/2025/10/23/introducing-arrow-avro/)). Caveat:
  bench schema field count undisclosed — per-field normalization is an estimate.
- **arrow-json / Arroyo**: two-pass tape (simdjson-style); each column decoder gets a position
  array, processing "in a tight, efficient loop, and only needing to downcast the array a single
  time." 0.396–5.108 µs/record vs Jackson 0.517–11.73 **[MEASURED]**
  ([Arroyo](https://www.arroyo.dev/blog/fast-arrow-json-decoding/)); raw-reader rewrite ~2.5× over
  the `serde_json::Value` intermediate ([#3479](https://github.com/apache/arrow-rs/pull/3479)).
- **ConnectorX (VLDB 2022)**: >85% of `pandas.read_sql` is client-side (~40% deserialization / ~40%
  dataframe conversion) **[MEASURED]**; peak memory 4× final frame. Fixes = pre-allocate destination
  from metadata, stream small batches, write each converted cell directly into its final slot,
  compile-time type-conversion codegen. String ablation ~doubles the 16-column lineitem load
  **[MEASURED]** ([paper](https://www.vldb.org/pvldb/vol15/p2994-wang.pdf),
  [repo](https://github.com/sfu-db/connector-x)).
- **ADBC Postgres**: binary wire → Arrow "without ever materialising rows in between" **[SOURCE]**
  ([docs](https://arrow.apache.org/adbc/current/driver/postgresql.html)).
- **DataFusion's cautionary tale**: `Vec<ScalarValue>` (owned per-value enums — structurally our
  `Value`) documented as ~16 B/field overhead + heap indirection, degrading with column count
  **[SOURCE]** ([#1708](https://github.com/apache/datafusion/issues/1708)); doctrine is "prefer
  Arrow compute kernels over `ScalarValue`."

## 3. Arrow Flight serving costs

- `FlightDataEncoderBuilder` default max flight data size **2 MiB** (size estimate approximate)
  **[SOURCE]** ([docs](https://docs.rs/arrow-flight/latest/arrow_flight/encode/struct.FlightDataEncoderBuilder.html)).
- **Splitting is cheap and at our batch size probably a no-op**: `split_batch_for_grpc_response`
  sums buffer sizes once per input batch and emits zero-copy `batch.slice()`s; a 2 MiB-targeted
  batch yields `n_batches == 1`, one slice **[SOURCE]**
  ([encode.rs](https://arrow.apache.org/rust/src/arrow_flight/encode.rs.html)). **This
  independently explains #3096's measured ZERO on the framing lever — there was no work to
  remove.** (Iterator-not-Vec micro-opt: [#10126](http://www.mail-archive.com/commits@arrow.apache.org/msg62674.html).)
- **One `dictionary_tracker` per stream** — dictionaries not re-sent per batch by default
  **[SOURCE]**. Only bites if emitting `DictionaryArray`s.
- **The one unavoidable copy is tonic's**: default codec copies `data_body` into its buffer — a
  memcpy of the whole IPC body; non-contiguous-buffer proposal open, unimplemented **[SOURCE]**
  ([tonic #1558](https://github.com/hyperium/tonic/issues/1558)). Plausibly a chunk of our
  313 ns/row framing; not removable from our side.
- Upstream sizing guidance is row-count-based (DataFusion `batch_size` 8192 + `coalesce_batches`)
  **[SOURCE]** ([configs](https://datafusion.apache.org/user-guide/configs.html)). Flight itself is
  not the ceiling: DoGet measured to ~6000 MB/s, ~95% of link bandwidth **[MEASURED]**
  ([arXiv 2204.03032](https://arxiv.org/abs/2204.03032)).

## 4. Anti-pattern audit

| Anti-pattern | What fast implementations do | Evidence |
|---|---|---|
| `match value_enum` per cell | Bind (source type → Arrow type) once per column at schema-bind; monomorphized/codegen appender per column | arrow-avro `Codec`; ConnectorX macro DSL **[SOURCE]** |
| Downcast `Box<dyn ArrayBuilder>` per cell | Downcast once per column per batch, then a tight loop | arrow-json/Arroyo **[SOURCE]** |
| Build owned `String`/`Vec<u8>` then append | Append `&str`/`&[u8]` borrowed from source (`append_value: AsRef`), or `append_block` + view for true zero-copy | arrow-rs API; StringView **[MEASURED]** |
| Row-major outer loop over cells | Column-major inner loop (row-major outer OK *if* per-column appender resolved outside it) | arrow-avro **[SOURCE]** |
| Grow builders from zero each batch | `with_capacity(rows, est_bytes)` per batch (capacity NOT retained across `finish()`) | arrow-buffer source **[SOURCE]** |

## 5. Realistic expectations — is our 1.2 µs/row 2× or 10× off?

1. **arrow-avro**: 267 ns/row row-object path (~67 ns/field if the 4-field bench shape) →
   **~1.07 µs/row estimated at 16 columns**. Our 1.2 µs/row lands almost exactly on the measured
   row-object-materializing path scaled to our width — the single most informative data point here.
2. **arrow-json/Arroyo**: 0.4–0.6 µs/record for ~5–10 field records *including parsing raw JSON*.
   We are not parsing and cost 2–3× that.
3. **ConnectorX**: 16-column lineitem, single stream = **2.6 µs/row for the entire pipeline** (wire
   read + deserialize + convert + write). Our 1.2 µs/row is under half a full pipeline while doing
   only the last stage.

**Judgement: ~1.2 µs/row is ~4–10× off achievable (150–400 ns/row for 15–20 mixed columns).** That
takes total Flight overhead to ~0.5–0.7 µs/row, with IPC framing (313 ns/row) as the floor and
dominant term.

## Ranked candidates for the `do_get` build path

1. **Resolve type dispatch + builder downcast once per column per batch; build column-major.**
   2–4× on the ~1.2 µs/row region (confidence medium-high — arrow-avro 9.6×, arrow-json ~2.5×
   against structurally identical baselines). Pitfalls: nested types keep per-cell downcasts in
   naive ports; the win is the tight per-column loop, not the tape. Pre-validate: perf self-time in
   builder vtable thunks / `field_builder` / `Value` match arms (<10% of build region ⇒ mis-aimed);
   or an afternoon criterion bench building one RecordBatch two ways from a pre-materialized row set.
2. **Append borrowed bytes; stop materializing owned `String`/`Vec<u8>` for text/blob cells.**
   1.3–2× on string-heavy schemas, ~0 on all-primitive (confidence medium — ConnectorX ablation
   ~2×; gain depends on unmeasured var-len fraction). Pitfalls: the aggressive `Utf8View` form
   **changes the wire schema** — connector/client compatibility is an owner decision; conservative
   borrowed-`&str` form has no wire impact, try first; `StringBuilder` 2 GB offset panic at large
   batches. Pre-validate: allocations/row via dhat/counting allocator (≈ var-len column count ⇒
   confirmed), + var-len bytes/row histogram per table. Digest oracle makes byte-identity free.
3. **Per-batch `with_capacity` pre-sizing from a running per-column byte EWMA.** 5–15% (confidence
   medium; listed third because cheap, not big). Pitfall: the folklore "hoist builders and reuse"
   does nothing (`finish()` = `mem::take`); undersized data_capacity = geometric realloc + memcpy;
   oversized inflates RSS vs the 128 MB target / B4 budget. Pre-validate: realloc count per builder
   per batch via capacity accessors; ≤1–2 ⇒ drop this entirely.

**Explicitly do NOT pursue:** further Flight framing tuning (mechanism-explained zero: one size
sum + single zero-copy slice at 2 MiB; residual is tonic's upstream memcpy) and schema caching
(emitted once per stream — nothing recomputed).
