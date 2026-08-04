# Design: Arrow encode on the Flight `do_get` data plane (issue #3096)

## Context

### What the row actually costs today

Every citation below was verified against the worktree at branch point; the three that were wrong in
the issue's recon notes are corrected here.

**Per row, before any Arrow buffer is touched, the column set is hashed three times.**

1. `build_row_from_scan_cached` (`cqlite-core/src/query/select_executor/row_build.rs:227`) allocates
   `HashMap<Arc<str>, Value>` with `HashMap::with_capacity(cells.len() + pk_hint)` at **:246** and
   inserts each cell at **:259** via `row_values.insert(name, col_value.into_owned())` — one
   allocation + N SipHash inserts + N `Value::into_owned()` compactions (`types.rs:1234`) per row.
2. `estimate_arrow_row_bytes` (`cqlite-core/src/export/arrow_size.rs:251`) probes that map back out
   at **:254** — `row.values.get(col.name.as_str())` — once per column per row, called from the
   batching decision at `producer_stream.rs:351`.
3. `transpose_columns` (`cqlite-core/src/export/arrow_columnar.rs:59`) probes it a THIRD time at
   **:87** — `name_to_indices.get(name.as_ref())` inside the per-cell loop — after allocating
   `n_cols` separate `vec![None; n_rows]` vectors at **:79-80**.

So a 12-column row (the `ws0.events` shape) pays ~36 string hashes and 1 map allocation for data the
scan already produced in column order.

**Then every scalar builder materializes an intermediate vector.** `arrow_convert.rs:1395-1917`
holds the flat builders; each ends in a `.collect::<Result<Vec<Option<T>>, _>>()?` that is then
handed to `XArray::from(values)`:

| line | builder | type |
|---|---|---|
| 1407 | `build_boolean_array` | `boolean` |
| 1461 | `build_int32_array` | `int` |
| 1487 | `build_int64_array` | `bigint`/`counter` |
| 1524 | `build_float64_array` | `double` |
| 1555 | `build_string_array` | `text` (borrowed `&str` — pointer vector, not a data copy) |
| 1596 | `build_binary_array` | `blob` (borrowed `&[u8]` — same) |

**Then the batch is rebuilt twice more downstream.** `rows_to_record_batch` (`arrow_convert.rs:197`)
re-derives the Arrow schema on EVERY call — `build_arrow_schema(columns)` at **:201**, re-wrapped in
a fresh `Arc::new(schema)` at **:203**. And `encode_do_get` (`cqlite-flight/src/streaming.rs:594`)
builds the encoder with defaults only:

```rust
599     let encoded = FlightDataEncoderBuilder::new()
600         .with_schema(schema_ref)
601         .build(batch_stream)
```

There is **no** `.with_dictionary_handling(...)` and **no** `.with_max_flight_data_size(...)`
anywhere in `cqlite-flight/src` (verified by grep), so both come from arrow-flight 53.4.1 defaults:
`DictionaryHandling::Hydrate` (`encode.rs:176`, enum at `:468-488`) and
`GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES = 2097152` (**`encode.rs:166`**, not `:613`). `Hydrate` routes
every batch through `hydrate_dictionaries` (definition **`encode.rs:685`**, call site `:379`), which
is a full `RecordBatch::try_new_with_options` rebuild — on a schema with **no dictionary columns**,
a pure per-batch tax. And our `DEFAULT_MAX_BATCH_BYTES = 4 * 1024 * 1024` (`batch_bytes.rs:154`) is
exactly 2x the encoder's target, so **every** batch we hand it is re-sliced and framed twice.

### The observability blind spot this change must close first

`StreamSubPhase::Encode` (`egress_flush.rs:116-119`, not `:114-118`) times **only**
`self.flush_buffer(buffer)` — the Arrow array build. It deliberately excludes the reserve park above
and the gRPC write below. It therefore does **not** cover `encode_do_get`'s IPC framing, dictionary
hydration, or re-slicing at all. Levers 4 and 5 target work that the existing timer cannot see, so
attributing them requires either an added sub-phase around the encoder stream or external `perf`
attribution. **This is a task, not a footnote:** without it, "lever 4 helped" is unfalsifiable.

### Why the rig has to be rebuilt before anything else

`/home/ubuntu/ws0-local/` does not exist on this box (`/home/ubuntu` holds only `projects` and
`workspace`), and PR #3103 committed only `.rs` + OpenSpec files. The #3026 harness that produced
the historical numbers is committed only as a *document*
(`docs/reports/ws0-3026-artifacts/ws0-cqlite/scan-harness/`) whose `Cargo.toml` points at
`/home/ubuntu/workspace/wt-3026/cqlite-core` — a dead path. The #3100 corpus was Cassandra-written
and LZ4-compressed (`corpus-geometry.txt`: `chunk_length 16384`, ratio 3.5353x), which CQLite's
uncompressed-only write surface (#1406) cannot reproduce. **So no number in issue #3096 is currently
reproducible on the delivery box**, and the pre-change baseline must be re-established locally
before any lever can be judged. The box is a 16-vCPU Intel Xeon Platinum 8488C (8 physical cores, 2
threads/core) / 30 GB with `/usr/bin/perf` and `/usr/bin/taskset` present, so it can host it.

## Recommended design

### Phase 0 (BLOCKING) — commit the rig, re-baseline locally

1. **Corpus generator** (`tools/ws0-corpus-gen`, a new workspace member under the existing `tools/*`
   glob; `tools/flight-loadgen` is the precedent for a committed measurement tool). Drives the
   production `SSTableWriter` — not a hand-rolled byte emitter — from the pinned `ws0.events` DDL.
   4,000,000 rows = 40,000 partitions x 100 rows, partitions emitted in **Murmur3 token order**,
   uncompressed, deterministic from a recorded seed. Records `sha256` + row/byte shape in-tree.
2. **Measurement scripts** (`scripts/perf/` or alongside the tool): `perf stat -C <sibling-pair>`,
   never `-p`; `taskset` to a pair read from `thread_siblings_list` and **failing closed** if they
   are not siblings; median of 3 with spread; setup-subtracted cycles/row; warm and cold as separate
   runs and separate claims; both arms (`bare scan` via `execute_streaming`, `do_get` via a real
   loopback Flight transport) in ONE session on ONE pinned pair.
3. **The in-repo Arrow-buffer digest oracle.** A `RecordBatch`-buffer fold (values + validity, in
   column order, plus row count) run over the whole `do_get` stream, asserted equal across
   `CQLITE_FLIGHT_MERGE_PATH=bypass|merge` at a pinned `now` and equal before/after every lever. It
   lives in `cqlite-flight/tests/` next to `issue_3058_forced_path_differential.rs` and shares that
   file's `PROBE_LOCK` discipline (the env vars are process-global).
4. **Extend the sub-phase timing** so the arrow-flight encoder stream is attributable, or record
   explicitly that levers 4/5 are attributed by `perf` alone.
5. **Re-measure the pre-change ratio** on the regenerated corpus. That number — not 210,192 /
   312,155 — is this change's baseline.

### The ranked levers, in landing order

Each lands and is measured **individually** against the Phase-0 baseline before the next starts.

| # | Lever | Why here | Risk |
|---|---|---|---|
| **1** | **Column-major build straight from the scan row carrier**, skipping the per-row `HashMap<Arc<str>, Value>` (`row_build.rs:246`) | Largest single item: removes 1 map allocation/row, N `into_owned()`/row, and **both** downstream hash passes (`arrow_columnar.rs:87`, `arrow_size.rs:254`) at once. The scan already yields cells in schema order; the map is pure re-derivation. | **High** — a second row-emit shape. Gated by the digest + the forced-path differential + the semantics oracles. |
| **2** | **Delete the intermediate `Vec<Option<T>>` in the scalar builders** (`arrow_convert.rs:1407,1461,1487,1524,1555,1596`); append into `PrimitiveBuilder::with_capacity(n_rows)` with its own null bitmap | Halves the touch count on the 6 fixed-width columns of the corpus shape. Independent of #1 and mechanically checkable. | Low |
| **3** | **Fold `estimate_arrow_row_bytes` into the transpose/append pass** (`arrow_size.rs:251`) | Removes the third hash pass and one full row traversal per batch. Must preserve `Σ estimate >= arrow_payload_bytes` (`arrow_size_tests.rs` over `arrow_shape_corpus.rs`) — the batching cut and the egress credit reservation both depend on it. | Medium (invariant-bearing) |
| **4** | **Align the batch cap with the encoder**: `DEFAULT_MAX_BATCH_BYTES` 4 MiB (`batch_bytes.rs:154`) → the encoder's 2 MiB `GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES` (`encode.rs:166`), or raise the encoder's limit to match ours | Today every batch is re-sliced and framed twice. Essentially one line. | Near zero — but the `batch_bytes.rs:137-153` narrow-shape table must be re-derived, since halving the cap moves where the byte-cap starts binding. |
| **5** | **Stop the per-batch dictionary rebuild** at `streaming.rs:599` — `DictionaryHandling::Resend`, or hydrate once | The schema has no dictionary columns, so `hydrate_dictionaries` (`encode.rs:685`) is a pure per-batch `RecordBatch` rebuild. | Low, but wire-visible: the `do_get_transport_test.rs` byte golden is the guard. Must be verified against a Trino/JDBC client shape, not only our own decoder. |
| **6** | **Cache the Arrow `Schema`** instead of rebuilding at `arrow_convert.rs:201-203` | One `build_arrow_schema` + `Arc::new` per batch, not per call. Trivially safe. | Near zero |
| **7** | *(stretch, strictly behind #1)* **Borrow text/blob from the decoded chunk** instead of `into_owned()` compaction (`row_build.rs:259`, `types.rs:1234`) | The 675 B/row copy figure is mostly here. | **Highest** — needs a chunk-retention story that does not break the <128MB bound. May be dropped without failing the change. |

**Ordering rationale.** 4 and 6 are near-free and should land first as a cheap floor — except that
attributing them needs the Phase-0 timing fix, so they follow it. 2 and 3 are contained and
mechanical. 1 is the largest item and the highest risk, so it lands with the digest oracle already
proven on the earlier levers. 7 is deliberately last and optional.

**What this beat.** (a) *Replace the CQL→Arrow converter wholesale with a bespoke encoder* —
rejected: it discards `arrow_size`'s estimator invariant, the shape corpus, and the egress credit
contract in one step, and makes any negative result uninterpretable. (b) *Chase the 675 B/row copy
first (lever 7 as lever 1)* — rejected: it is the highest-risk item and its benefit is bounded by the
hash/alloc overhead sitting in front of it; do the cheap structural work first and re-price it.
(c) *Tune the allocator (#3028) instead* — rejected as out of scope here and explicitly held for
re-pricing; lever 1 removes the allocation rather than making it cheaper, which is the #3058
precedent ("delete the path that builds it, do not tune the map"). (d) *Batch-level parallel encode*
— rejected: it changes the per-core denominator the whole 0.17 program is measured in, and would
make this number incomparable with #3023/#3058.

### Correctness pinning stack

| Guard | What it catches |
|---|---|
| In-repo Arrow-buffer digest (new) | any change to emitted buffers, offsets or null bitmaps — the only guard that sees a builder defect |
| `issue_3058_forced_path_differential.rs` | bypass-vs-merge row-set/value/order divergence |
| `do_get_transport_test.rs` wire-frame byte golden (`:252-253`) | a framing/metadata change from levers 4/5 |
| `arrow_size_tests.rs` over `arrow_shape_corpus.rs` | `Σ estimate >= realized payload` — lever 3's invariant |
| `batch_bytes_tests::the_capacity_bound_holds_over_the_shared_shape_corpus` | the egress credit reservation bound after lever 3/4 |
| `query_semantics_flight_parity.rs` + core `query_semantics_oracle_parity.rs` at pinned `now` | read-time reconciliation, unchanged |
| `StreamSubPhase::Encode` + the added encoder-stream phase | per-lever attribution inside the process |

### Explicitly NOT a correctness oracle

The committed corpus is **CQLite-written and CQLite-read**. Per issue #3042 that round trip is
INVARIANT to a uniform framing/serialization error — both sides make the identical mistake and the
test stays green. It is therefore a **performance fixture only**, and the generator's own
documentation must say so. No on-disk framing or encoding correctness claim may rest on it;
correctness stays anchored to the Cassandra-written fixtures (`test_compaction_tombstone_ttl`, the
`da`/`nb` goldens) and the oracles above.

## Owner decisions needed at Seam 1

1. **Where the committed rig lives.** Recommendation: `tools/ws0-corpus-gen` (new workspace member
   under the existing `tools/*` glob) + scripts under `scripts/perf/` + a method doc in
   `docs/reports/`. The alternative — a `docs/reports/` payload like #3026's — is what produced the
   dead `Cargo.toml` this change is cleaning up.
2. **Whether the file-size split of `arrow_convert.rs` (2,596 lines) lands in THIS change or a
   precursor.** Recommendation: a mechanical split-only precursor commit inside this PR, before any
   behavioral lever, so the ratchet never forces `CQLITE_ALLOW_FILE_GROWTH=1` and so the levers'
   diffs stay reviewable.
3. **Lever 5's blast radius.** `DictionaryHandling::Resend` is wire-visible. Recommendation: land it
   only if a real Trino/JDBC client round-trip is verified, not just our own `FlightRecordBatchStream`
   decoder; otherwise prefer hydrating once and reusing.
4. **How far to go if lever 4 + 6 alone reach the ratio.** Recommendation: STOP and report — the kill
   criterion cuts both ways, and unneeded risk on levers 1/7 is not justified by a met target.
