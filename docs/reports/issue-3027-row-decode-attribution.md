# Issue #3027 (WS1) — row-decode CPU: function-level attribution

Parent: #3023 (T1 exploration). Program epic: #2817. Companion WS: #3028.

**Headline: the WS1 premise is partially refuted.** Row decode is the largest *CPU* consumer, but it is
**traffic-light**. The memory traffic — the term that binds first, per the #3023 roofline — lives in the
`QueryRow` **materialize → walk → free** lifecycle, not in the decoder. Cutting decode CPU would move
CPU% without moving the bandwidth roofline, which is precisely the failure mode the #3023 reporting
contract warns about.

The stopping condition (row decode < 10% of on-CPU **and** >= 400k rows/s/phys-core warm) was **not met**,
and no fix in this issue moved the number. Both candidate fixes measured are **null results**, reported
as such.

---

## 1. Measurement environment (differs from #2818 — read before comparing)

| | #2818 ground truth | This report |
|---|---|---|
| CPU | i4i (Ice Lake) | **Intel Xeon Platinum 8488C (Sapphire Rapids), 8 physical / 16 logical** |
| Corpus | LZ4 compressed, `chunk_length=16384`, server-direct | `test_basic.simple_table`, 999 rows, 647 B/row on disk |
| Driver | `cqlite-flight` server-direct + loadgen | `cqlite-core/benches/read.rs` (criterion) |

**Docker is unavailable on this box**, so a Cassandra-generated corpus of controlled shape could not be
produced; every `test-data/scripts/generate-*.sh` path needs it. The largest available fixture is 999
rows. Absolute rows/s therefore is **not** directly comparable to #2818's server-direct figure — but the
*bucket proportions* reproduce it closely (§3), which is what licenses the attribution below.

Runs are pinned with `taskset -c 2` (core 2's HT sibling is core 10, left idle) and warm by construction
(criterion re-runs the same query thousands of times).

### Tooling gotchas found here (add to the #3023 shared list)

- **DRAM-level PMU counters are not virtualized on this EC2 instance.** `longest_lat_cache.miss`,
  `offcore_requests.*`, `mem_load_retired.l3_miss` and `cache-references` all read **exactly 0**.
  `uncore_imc/cas_count_read/` does not exist. The one working memory counter is **`l2_lines_in.all`**.
  All "memory traffic" below is therefore **L2-fill traffic (L1D+L1I miss traffic, includes prefetch),
  x 64 B/line** — *not* DRAM traffic. It is the right metric for comparing how much data movement a code
  path causes, and it is measurable; it is not the roofline's DRAM term.
- `perf` needs `kernel.perf_event_paranoid` lowered (default 4 on this image blocks everything).
- Datasets live at **`/data/datasets`**, not the in-tree `test-data/datasets` — use the
  `export CQLITE_DATASETS_ROOT=…` line `fetch-datasets.sh` prints. **RETIRED (#3131/#3148):** at the
  time of this run `benches/fixtures/mod.rs` also resolved schemas as
  `$CQLITE_DATASETS_ROOT/../schemas`, so a `/data/schemas` symlink was needed. That is no longer true
  and **the symlink must not be created**: the schemas root is resolved checkout-relative, so
  `CQLITE_DATASETS_ROOT` alone is sufficient.
- **Do not pin the streaming bench to one core for a throughput number.** `read/scan_partition_dense_stream`
  under `taskset -c 2` spends 5.27 s of 12.43 s in `sys` — a multi-threaded tokio pipeline forced onto one
  core measures the scheduler, not the read path. Unpinned, `native_queued_spin_lock_slowpath` is 5.96%
  of the profile. Absolute streaming throughput here is a **scheduling artifact** and is not reported as
  a finding.

---

## 2. Baseline (clean, idle box, +-0.2% run-to-run)

| Bench | rows/s / phys-core, warm | cycles/row | L2-fill traffic/row |
|---|--:|--:|--:|
| `read/full_scan` (materializing, `db.execute`) | **314.77 k** | **11,283** | **12,456 B** |
| `read/scan_partition_dense_stream` (`execute_streaming`) | 157.90 k | (confounded — see above) | |

Derivation: `perf stat` over `--profile-time 12` gave 43.52e9 cycles and 750.68e6 `l2_lines_in.all` over
3.857e6 rows, at 3.499 GHz, IPC 2.27.

**11,283 cycles/row vs #2818's 12,597** — this box reproduces the ground-truth shape within ~10%, despite
the different CPU and corpus. That is the licence for the attribution.

Note the amplification: **12,456 B of L2-fill traffic per row for a 647 B row (~19x)**.

---

## 3. Bucket-level attribution — corroborates #2818

Flat `perf record -F 1999` over `read/full_scan`, single-threaded.

| Bucket | This report | #2818 | |
|---|--:|--:|---|
| Row/cell decode | **22.1%** | 26.7% | corroborated |
| Allocator (malloc/free/drop_glue) | **20.0%** | 19.6% | corroborated |
| LZ4 decompress | 0.9% | 0.5% | corroborated |
| Map build + SipHash | 7.7% | (inside decode) | newly separated |
| Result-budget estimator | 6.4% | not present | **materializing path only** |

Decode bucket = `parse_row_data_with_offset_impl` 9.64 + `parse_cell_value_schema_order` 6.55 +
`decode_scalar_cell_value` 3.06 + `parse_block` 1.22 + `parse_row_metadata` 0.87 +
`read_vint_length_prefixed_bytes` 0.74.

Allocator bucket = `malloc` 6.41 + unnamed libc 6.02 + `drop_glue::<QueryRow>` 4.63 + `cfree` 2.04 +
`drop_glue::<Value>` 0.89.

---

## 4. The headline: cycles vs memory traffic diverge

Sampling **on the memory event itself** (`perf record -e l2_lines_in.all -c 20000`) attributes traffic
per function — measured, not inferred from CPU share.

| Function | cycles % | cycles/row | **traffic %** | **B/row** | traffic:cycles |
|---|--:|--:|--:|--:|--:|
| `parse_row_data_with_offset_impl` | **9.64** | 1,088 | 4.20 | 523 | **0.44** |
| `parse_cell_value_schema_order` | 6.55 | 739 | 3.35 | 417 | 0.51 |
| `decode_scalar_cell_value` | 3.06 | 345 | 4.22 | 526 | 1.38 |
| `memory::estimate_value_size` | 6.35 | 716 | **11.22** | **1,398** | **1.77** |
| `drop_glue::<QueryRow>` | 4.63 | 522 | **7.62** | **949** | **1.65** |
| `Value::into_owned` | 6.22 | 702 | **7.59** | **945** | 1.22 |
| `malloc` | 6.41 | 723 | 4.23 | 527 | 0.66 |
| `sip::Hasher::write` | 2.72 | 307 | 2.45 | 305 | 0.90 |
| `hashbrown insert` | 2.74 | 309 | 2.39 | 298 | 0.87 |
| `RandomState::hash_one` | 2.22 | 250 | 1.76 | 219 | 0.79 |
| `build_row_from_scan_cached` | 3.08 | 348 | 1.58 | 197 | 0.51 |

**Read this table before proposing a WS1 fix.**

- The **decoder is compute-bound**: `parse_row_data_with_offset_impl` is the #1 CPU consumer but only #6
  in traffic (ratio 0.44). Making it faster buys CPU% and moves the roofline barely at all.
- The **`QueryRow` lifecycle is traffic-bound**: `estimate_value_size` + `drop_glue::<QueryRow>` +
  `Value::into_owned` = **26.4% of all L2-fill traffic** but only 17.2% of cycles, i.e. **3,292 B/row**
  of the 12,456 B/row total. All three are pure *materialization overhead* — none of them decodes
  anything. `estimate_value_size` alone is **1,398 B/row**, which independently reproduces the #3023
  brief's "~1,400 B of the ~4,408 B/row is the `Value` materialize step."

**Consequence for T1.** A WS1 that cuts decode CPU cannot reach 600k rows/s/phys-core, because the term
that binds is the materialize/free traffic, not the decode compute. The lever that moves the roofline is
T2's "avoid materializing `Value` at all — borrow from the decompressed chunk." This is a
program-level redirect, offered in the same spirit as #2818 Arm 2's flat read histogram.

---

## 5. Findings by scope item

### 5.1 `estimate_value_size` — 6.35% CPU / 11.22% traffic, **materializing path only**

`query/result_budget.rs:36` sums `memory::estimate_value_size` over **every value of every row** of the
fully-materialized result set, to enforce `max_result_bytes` (#1582). It is a second full pointer-chasing
pass over data that has just gone cold — hence the 1.77 traffic:cycles ratio, the worst in the profile.

**`cqlite-flight` never calls the result budget** (verified: no reference to `result_budget`,
`estimate_query_row_bytes` or `enforce_materialized_rows` anywhere in `cqlite-flight/src/`). So this cost
is absent from the Flight streaming path #2818 measured, and it is **not** part of the 26.7% streaming
decode bucket. It is real for CLI/embedded/materializing consumers.

The Flight path has its own analogue: `estimate_arrow_row_bytes` is called **per row** at
`producer.rs:1032` / `producer_stream.rs:326` and re-does an `N x M` by-name `HashMap` probe
(`arrow_size.rs:253`) that `transpose_columns` exists to eliminate.

### 5.2 Per-partition rebuild of scan-invariant state (streaming path)

`RowColumnResolution::build` is called **once per partition** (`partition_driver.rs:179`, and the code
comment says so). It depends only on `(schema, reader.header)` — both invariant for the entire scan. Per
column it allocates two `String`s via `to_lowercase()` on two *different* strings: `CellKind::from_type`
lowercases the schema type (`cell_kind.rs:72`) and `is_complex_column` lowercases the header marshal type
(`udt.rs:1216`).

On `simple_table` (999 **one-row** partitions, ~20 columns) that is ~40 `String` allocations + case-folds
**per row**. The cluster measured **17.4% of on-CPU** on the pinned streaming profile
(`build` 2.86 + iterator 2.60 + `CellKind::from_type` 1.80 + `to_lowercase` 2.99 + `is_complex_column` 1.41
+ `Vec<ColumnToParse>::from_iter` 1.40 + `drop_glue` 1.32 + `hash_one::<&String>` 1.50 + `make_hash::<str>`
1.49).

**Caveat that must travel with this number:** 1 row/partition is the *pathological* shape. The cost
amortizes linearly as rows/partition rises, and #2818 did not record the production rows/partition. On
the materializing path (which uses `block_emit`, per *block*) this cluster does not appear in the profile
at all. **This is a gap in the #3023 shared context worth closing before anyone sizes the fix.**

Not landed here: with no Docker there is no realistic-shape corpus on this box, so the fix's true value
cannot be measured — only modeled. Landing a core-decode refactor on a modeled benefit would violate the
#3023 contract. Filed as a follow-up instead, with this measurement attached.

This is the residue of #1635 (J1), which removed the per-**cell** `to_lowercase`; the
per-column-per-partition one survived it.

### 5.3 Redundant UTF-8 validation — CONFIRMED, exactly 2x per text cell

Every text cell on a Flight SELECT is UTF-8-validated twice:

1. `cell_value_scalar.rs:72` — `std::str::from_utf8` at decode; result discarded, bytes stored via
   `borrow_active`. **Necessary** (establishes the `Value::Text` invariant) and does not copy.
2. `arrow_convert.rs:1547` (strict), `:1569` (lossy), `:654` (collection element) — re-validated only to
   satisfy `StringArray::from`'s `&str` API, then `memcpy`d straight back into the Arrow buffer. The
   `str`-ness is **never inspected**.

`Value::Text`'s UTF-8 invariant is documented as established at construction (`types.rs:88-90`,
`types.rs:1189-1201`), so the second validation is provably redundant. Removing it requires
`from_utf8_unchecked`, i.e. `unsafe` on a hostile-input path — **not recommended** without the fuzzing
epic (#1614) covering it. Recorded, not actioned.

Null result: no path validates the same bytes 3+ times, and no decoded cell's `str` is genuinely
inspected as text (no `.chars()` anywhere in the row-decode or Arrow-encode call graph).

### 5.4 Per-cell re-parsing

Confirmed instances (all `file:line` verified):

- `raw_value.rs:131` — `to_lowercase()` + ~30-arm string ladder **per collection element**. The surviving
  instance of the anti-pattern #1635 removed for scalars.
- `read_assembly.rs:155` — `Arc::from(cell.column.as_str())` **per cell per row** on the Flight merge
  path, defeating the #1334 interning contract (the name was supposed to be interned once).
- `complex_column.rs:1124` — unconditional `to_vec()` of the cell path per collection element, discarded
  for lists on the user-facing read.
- `row_framing.rs:941` — `peek_partition_boundary` runs after **every** row and walks the header prefix
  twice; at a partition boundary the same header bytes are structurally walked **4x**.
- `mod.rs:776` — `row_has_non_key_cell` computed unconditionally. **See §6.1 — this one is a null result.**

Null result: no "compute serialized length by re-running the parser" anywhere on this path, and the
scalar path's generic re-dispatch was genuinely eliminated by #1635.

### 5.5 `Value::into_owned` — the dominant allocation term

`types.rs:1250-1261`: for any payload <= `RETENTION_SLACK` (4096 B) that is uniquely owned, `into_owned`
performs an **unconditional** `Bytes::copy_from_slice` — a heap allocation + memcpy — even when the
payload is already tight. The rationale (documented in-place) is that `capacity()` cannot see backing
*behind* the payload offset. For a ~690 B row **every** byte-carrying cell is Tier 1, so every one is
copied. The repo's own ratchet encodes this as `PER_CELL_ALLOCS = 1`
(`row_build_alloc_budget_test.rs:73`, rationale at `:52-54`).

Ledger for one medium row (30 cols, ~690 B), decompressed chunk -> finished `QueryRow`: **~16-17 heap
allocations and ~12 memcpys**, of which ~10-11 are `into_owned` alone. This is T2 territory (borrow from
the chunk) and is the single largest lever on the traffic term.

---

## 6. Candidate fixes attempted — both null results

### 6.1 Lazy `row_has_non_key_cell` — NULL RESULT, hypothesis refuted

`build_display_row` (`row_decoder/mod.rs:776`) computed `has_data_cell` unconditionally although it is
consumed only by `row_tombstone.is_some() && !has_data_cell`. Moving the call into the short-circuiting
condition is provably behaviour-identical (the function is pure over `cells`/`schema`, neither mutated in
between).

| | baseline | with fix (2 runs) |
|---|--:|--:|
| `read/full_scan` | 314.77 k | 317.70 k / 313.49 k |
| `read/scan_partition_dense_stream` | 157.90 k | 157.62 k / 157.32 k |

**No measurable change.** Mechanism: `.any()` **short-circuits on the first non-key cell**, which for a
live row is typically the first cell examined — so the real cost is ~1 string comparison per row, not the
~90 an `O(cells x (n_pk + n_ck))` reading suggests. **Reverted; not shipped.** A one-line change with no
measured benefit is churn.

### 6.2 #2901 FxHashMap end-to-end A/B — INCONCLUSIVE, instrument too noisy

Attempted a real A/B by swapping `QueryRow.values` to `rustc_hash::FxHashMap` and re-running
`benches/row_build_bench.rs`. **The result is not reportable**, for two compounding reasons:

1. **Run-to-run noise exceeds the effect.** Two runs of the *identical* SipHash binary differ by 6.8% at
   `columns_32` and **26% at `columns_64`**; the `hashonly_*` control group — which the swap cannot
   affect — moved 8-25% between runs. N=8/16 are tight (~2-3%); N=32/64 are not. The noise tracks memory
   footprint, so the larger arms are contention-sensitive.
2. **My own measurements were self-contaminated.** Part of the A/B ran while a subagent was executing
   `cargo clippy` and tests **in the same worktree**. That is the #1930 one-worker-per-machine rule being
   violated from the inside — a lead running measurements while its own subagent builds. The subagent,
   measuring on a quiet detached worktree, recorded <1.6% spread and `columns_32 = 774 k`, matching my one
   clean run (771 k) and exposing my contaminated 612 k reading as the outlier.

**Process lesson (worth propagating):** a measurement session must own the box exclusively, subagents
included. "One worker per machine" is not only about gate concurrency.

---

## 7. #2901 (L5 FxHashMap) — disposition

The issue required all three objections answered. Result: **one answered, one refuted, one stands.**

**(a) Measured benefit — ANSWERED.** SipHash is measurable and larger than the unmeasured 1.04x
projection:
- In situ, `read/full_scan`: `sip::Hasher::write` 2.72% + `RandomState::hash_one` 2.22% = **4.94% of
  on-CPU (557 cycles/row)**; with the `hashbrown` insert it is 7.68%.
- Streaming profile: SipHash symbols total **8.12%** of on-CPU.
- Isolated instrument (`row_build/hashonly_*` vs `columns_*`, clean run): map construction + SipHash is
  **57-80% of the entire row-conversion cost**. **This is NOT the upside of a hasher swap** — it bounds
  map construction *and* hashing together (`with_capacity`, the hashbrown probe/insert and the map dealloc
  are all inside it, and a hasher swap removes none of them). Only the SipHash sub-term is addressable
  that way, and the in-situ 4.94% above is the honest bound on it.

**(b) Non-breaking surface — REFUTED BY CONSTRUCTION.** The issue said *"scope the swap so the map type
does not appear in any `pub` signature... If it cannot be made non-breaking, say so and stop."* It cannot:

- `QueryRow.values` is a **`pub` field** whose type *is* the map (`query/result.rs:77`).
- `QueryRow::with_interned_values(key, values: HashMap<Arc<str>, Value>)` is a **`pub fn`** taking it as a
  parameter (`query/result.rs:595`).

A newtype does not help: `Deref` cannot bridge two `HashMap`s with different `S`. Compiling the swap
surfaced **6 construction sites in `cqlite-core` alone** before the ripple reaches `cqlite-flight` and
`cqlite-cli`, reproducing #1883's finding exactly. Per the issue's own instruction: **stop.**

**(c) Untrusted keys — OBJECTION STANDS.** The claim to test was "the keys are column names from the
schema, not user data." On the default read path the schema is reconstructed from the file's
`Statistics.db` serialization header, so for a hostile SSTable the attacker supplies the key strings. I
searched for a bound on the header column count and **found none** — the only nearby limit is a
per-row cell-count check (`row_cell_state_machine.rs:710`, `column_count > 1000`) on a different path,
which does not bound schema width. Without such a bound, FxHash's trivial collidability gives O(N^2)
per-row insert on attacker-chosen colliding column names. The `rustc-hash` invariant in
`cqlite-core/Cargo.toml` ("NOT for maps exposed to untrusted string keys", #1590 E8) therefore still
applies.

**Recommendation (owner decision — not taken unilaterally).** Do **not** land L5 as specified. Either
close #2901, or re-scope it to the shape that dominates it on both axes: the decoder already emits cells
**positionally in schema order**, and `arrow_columnar.rs` exists to transpose the map back to columnar.
A positional `Vec` + a shared name->index table would remove the per-row hashing **and** the per-row map
allocation **and** sidestep hash-DoS entirely — strictly better than FxHashMap. That is a public-surface
change and is T2-aligned; it belongs with the "avoid materializing `Value`" work, not here.

---

## 8. What shipped

- `cqlite-core/benches/row_build_bench.rs` — the row-conversion instrument #2901 named as its blocker
  ("#1883 ran no row-conversion benchmark, so the ~1.04x remained a projection"). Groups
  `row_build/columns_{8,16,32,64}` drive the real public `build_row_from_scan_cached`;
  `row_build/hashonly_{N}` isolates map construction + SipHash. Carries a compile-time pin
  (`ValuesMap` + `hashonly_map_type_is_pinned_to_query_row`) so that changing `QueryRow.values`' hasher
  **breaks the bench build** rather than silently making the two groups measure different hashers.
  Known limitation, now recorded in the bench's own module doc: run-to-run spread at N=32/64 is 7-26%
  (§6.2); trust N=8/16 and treat the large arms as indicative until #3048 stabilises it.
- This report.
- No production-code change: both candidate fixes were null results and were reverted.

## 9. Acceptance criteria

| Criterion | Status |
|---|---|
| Function-level attribution with cycles/row **and** bytes-traffic/row | **Met** (§4) |
| Each candidate fix measured before/after in those units | **Met** — both null (§6) |
| Stopping condition: decode < 10% on-CPU **and** >= 400k rows/s/phys-core | **NOT met** (22.1%, 314.8 k) |
| #2901 lands non-breaking, or closed with reason recorded | **Recorded** (§7); close/re-scope is an owner call |
| No regression on the #1883 alloc ratchet | **Met** — no production code changed |
</content>
