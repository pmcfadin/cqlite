# arrow-encode-doget — delta for arrow-encode-doget (issue #3096)

## ADDED Requirements

### Requirement: Acceptance is a same-session RATIO to the bare scan, reported as rows/s AND cycles/row
Acceptance SHALL be `flight_do_get_rows_per_sec >= bare_scan_rows_per_sec / 1.3`, both arms measured in the SAME session, on the SAME box, over the SAME bytes, warm, per **physical** core. The issue's absolutes (`>= ~240,100` rows/s against a `312,155` rows/s bare scan) were corpus- and machine-bound — the regenerated corpus is CQLite-written and uncompressed and cannot reproduce them — so they SHALL NOT be encoded as thresholds nor restated as reproduced. Every claim SHALL be reported as **rows/s AND cycles/row**, setup-subtracted, median of at least 3 runs with the spread stated, never as a CPU-share. A fall in the share of CPU attributed to Arrow encode with rows/s unmoved is a **FAIL** (the #2877 shape). The pre-change ratio SHALL be re-measured on the regenerated corpus before any lever lands, so the delta is self-contained.

#### Scenario: The ratio is measured in one session over identical bytes
- **WHEN** the bare scan (`execute_streaming`) and Flight `do_get` are driven over the committed corpus's identical bytes in one session on the same pinned cores, median of at least 3 runs
- **THEN** `do_get` rows/s is at least the same-session bare-scan rows/s divided by 1.3, and both surfaces are reported as rows/s AND cycles/row with the observed spread

#### Scenario: A CPU-share shift with unmoved rows/s is a FAIL
- **WHEN** a change reduces the percentage of cycles attributed to Arrow encode while same-session rows/s and cycles/row are unmoved beyond the measured spread
- **THEN** the result is recorded as a FAIL and acceptance is NOT met — a profile shift alone is never evidence of a throughput gain

#### Scenario: The prior absolutes are never restated as reproduced
- **WHEN** the results are written up
- **THEN** they state that `240,100` / `312,155` rows/s were bound to the #3058 corpus and machine, report this change's own same-session pair instead, and present the ratio — not the absolutes — as the pass condition

### Requirement: The measurement honors both known traps, and warm and cold stay separate claims
The measurement SHALL use CPU-wide `perf stat -C <cpu-list>` and SHALL NEVER use per-process `perf stat -p` (measured >2x observer cost on this workload). The workload SHALL be pinned with `taskset` to a **verified** physical-core sibling pair read from `/sys/devices/system/cpu/cpu*/topology/thread_siblings_list`, never assumed from a CPU number, and the pinned list SHALL appear in the recorded method. Warm and cold SHALL be reported as SEPARATE claims, never blended. Setup (corpus open, reader warm-up, server start) SHALL be subtracted from the cycles/row denominator, and the row denominator SHALL be printed alongside every figure.

#### Scenario: The recorded method names the counter mode and the pinned CPUs
- **WHEN** the committed measurement script and recorded method are inspected
- **THEN** they invoke `perf stat -C` with an explicit CPU list, contain no `perf stat -p` invocation, and record the `taskset` mask actually used

#### Scenario: The pinned pair is verified siblings, not assumed
- **WHEN** the script selects the cores to pin
- **THEN** it reads `thread_siblings_list` and fails closed if the requested pair is not a sibling pair of one physical core, rather than silently measuring two different cores

#### Scenario: Warm and cold are reported separately
- **WHEN** the results are reported
- **THEN** the warm figure (this issue's owned claim) and the cold figure each appear with their own rows/s and cycles/row, and no averaged or blended number is presented

### Requirement: Output invariance is proven by an IN-REPO Arrow-buffer digest oracle
An output-invariance oracle SHALL live in the repository and SHALL be computed over the **Arrow buffers** of every `RecordBatch` the `do_get` data plane emits — value and validity buffers in column order, folded with the batch's row count — not over `QueryRow` values, so a defect in the Arrow builders themselves is visible to it. It SHALL assert the digest, total row count, and cells-per-row are IDENTICAL between the `bypass` and `merge` arms over the same bytes at a PINNED `now` (via `CQLITE_FLIGHT_MERGE_PATH`), and IDENTICAL before and after every lever this change lands. The external digest `0x0a2a390223bde6aa` named in the issue exists nowhere in this repository, SHALL NOT be cited as evidence, and SHALL be replaced by an in-repo digest re-pinned on the regenerated corpus and recorded in-tree. The change SHALL additionally keep green `cqlite-flight/tests/issue_3058_forced_path_differential.rs` (bypass-vs-merge row-set equality), `cqlite-flight/tests/do_get_transport_test.rs` (the wire-frame byte golden, metadata-order guard at `:252-253`), and the estimator invariance tests `cqlite-core/src/export/arrow_size_tests.rs` over `arrow_shape_corpus.rs`.

#### Scenario: The digest is arm-invariant over the same bytes
- **WHEN** `do_get` is run over the same corpus and ticket at a PINNED `now` under `CQLITE_FLIGHT_MERGE_PATH=bypass` and under `=merge`
- **THEN** the Arrow-buffer digest, the row count, and cells-per-row are identical across the two arms, and the test FAILS if any differs

#### Scenario: The digest is arm-invariant before and after each lever
- **WHEN** each optimization lever lands, against the digest pinned on the pre-change binary over the regenerated corpus
- **THEN** the recomputed digest equals the pinned value, and a lever that changes it is reverted or its divergence is explained as an intended, separately-specified output change — never absorbed silently

#### Scenario: The digest sees the Arrow buffers, not the row values
- **WHEN** the oracle's implementation is inspected
- **THEN** it folds `RecordBatch` column buffers (values + validity) and row counts, so an incorrect Arrow builder, offset, or null bitmap changes it — a digest computed from `QueryRow` values alone would not, and is not sufficient

#### Scenario: The estimator invariant survives folding the size pass into the build pass
- **WHEN** the per-row size estimate is folded into the transpose/append pass
- **THEN** `Σ estimate_arrow_row_bytes >= realized arrow payload bytes` still holds for every shape in `arrow_shape_corpus.rs`, and the byte-cap batching decision (`producer_stream.rs:351-358`) cuts on the same rows it cuts on today

### Requirement: The corpus generator and measurement scripts are COMMITTED and runnable from a clean checkout
The rig named in the issue (`/home/ubuntu/ws0-local/`) does not exist on the delivery box and was never committed, so this change SHALL reconstitute and COMMIT it. A corpus generator SHALL drive `cqlite-core`'s production `SSTableWriter` from the pinned schema `ws0.events(part_id text, seq int, event_time timestamp, blob_a blob, blob_b blob, device_id uuid, metric_a int, metric_b bigint, metric_c double, payload text, region text, status text)` with `PRIMARY KEY (part_id, seq, event_time)` (`docs/reports/ws0-3026-artifacts/ws0-corpus/schema-as-created.cql`, `docs/reports/ws0-3100-artifacts/ws0-h2h/schemas/ws0-events.cql`), emitting 4,000,000 rows as 40,000 partitions x 100 rows, partitions written in **Murmur3 token order**, **UNCOMPRESSED** (write surface is uncompressed-only, #1406), deterministic from a recorded seed. The corpus SHALL be pinned by its OWN recorded `sha256` plus row count and on-disk byte shape, recorded in-tree at generation time; it will NOT equal `0185909de6da0de839e75defe8b7113f502001017db3b5312d7ed6fd3312f0b1` and that digest SHALL NOT be asserted. Measurement scripts SHALL be committed alongside it and SHALL run end to end from a clean checkout with no path outside the repository and no uncommitted helper. Because the corpus is CQLite-written and CQLite-read it is INVARIANT to a uniform framing/serialization error (issue #3042): it SHALL be documented as a **performance fixture only** and SHALL NEVER be used as a correctness oracle for on-disk framing or encoding, which stay anchored to the Cassandra-written fixtures and the oracles above.

#### Scenario: A clean checkout can regenerate and measure
- **WHEN** the committed generator and measurement scripts are run per the committed method on a fresh clone with `perf` and `taskset`
- **THEN** they produce the corpus and both arms' numbers without referencing any uncommitted path, and no step depends on `/home/ubuntu/ws0-local/` or any other absent rig

#### Scenario: The corpus is pinned by its own recorded shape and is deterministic and non-vacuous
- **WHEN** the generator is run twice from the recorded seed and the corpus identity is checked
- **THEN** it produces a byte-identical `Data.db` both times with exactly 4,000,000 rows across 40,000 partitions in Murmur3 token order and no `CompressionInfo.db`; the check compares against the `sha256`, row count and byte shape RECORDED for this change (stating they differ from `0185909de6da…` by construction) and fails closed on a mismatch; and any run observing zero rows exits non-zero rather than reporting a measurement

#### Scenario: The corpus is documented as not a correctness oracle
- **WHEN** the generator and its documentation are inspected
- **THEN** they state explicitly that a CQLite-written + CQLite-read corpus proves self-consistency only (#3042), and no on-disk framing or encoding correctness claim rests on it

### Requirement: The kill criterion is binding, and a correctly reported negative result satisfies this change
If attacking Arrow encode does not move same-session `do_get` rows/s materially — beyond the measured run-to-run spread — the work SHALL STOP. The negative result SHALL be posted with rows/s AND cycles/row, warm and cold separately, and no further lever SHALL be stacked on an unexplained result. **A negative result, correctly measured and correctly reported, IS a satisfying outcome of this change** and SHALL NOT be treated as a failed delivery, reworked into a passing narrative, or padded with additional levers to reach the ratio. Each lever SHALL be attributed INDIVIDUALLY against the same-session baseline before the next is stacked, so a cumulative number can never hide a lever that cost throughput.

#### Scenario: An immaterial gain stops the work
- **WHEN** a landed, correct, wired lever's same-session `do_get` rows/s does not move beyond the measured spread
- **THEN** the work STOPS, the negative result is posted with rows/s and cycles/row for warm and cold, and the attribution question is re-opened rather than more levers stacked

#### Scenario: A negative result is a satisfying outcome
- **WHEN** the change is audited against this specification after a negative measurement
- **THEN** a posted, correctly-measured negative result with the rig committed satisfies this requirement, and the absence of a throughput gain alone is not an unmet requirement

#### Scenario: Each lever is attributed individually
- **WHEN** more than one lever lands
- **THEN** each is measured on its own against the same-session baseline and its individual rows/s and cycles/row delta is reported, so a regressing lever cannot be masked by the cumulative figure

### Requirement: The Arrow encode work is wired from the public `do_get` surface and preserves the existing contracts
Every optimization SHALL be reached through the public Flight `do_get` RPC — wiring evidence is the named surface (`cqlite-flight/src/service.rs:553 do_get`), the call chain `producer_stream.rs:219 drive_row_source` → `egress_flush.rs:83 flush_credited` → `producer.rs:1132 flush_buffer` → `cqlite-core/src/export/arrow_convert.rs:197 rows_to_record_batch`, and an end-to-end test over that RPC; helper-only unit tests are NOT sufficient. No encode or decode decision may be inferred from byte patterns (no-heuristics, #28) — the column set and its types come from the ticket schema. Library code SHALL contain no `unwrap()`/`expect()` and SHALL be clean under `RUSTFLAGS="-D warnings"`. The byte-bounded batching contract (`DEFAULT_MAX_BATCH_BYTES`, `batch_bytes.rs:154`), the egress credit reservation, and cancellation SHALL be preserved. The file-size campsite rule applies to every file touched; the already-oversized `cqlite-core/src/export/arrow_convert.rs` (2,596 lines) SHALL be split by responsibility rather than grown (epic #1116).

#### Scenario: The gain is observable end to end from do_get
- **WHEN** the throughput claim is made
- **THEN** it is measured over the public `do_get` RPC on a real transport, not over `rows_to_record_batch` in isolation, and an end-to-end test exercises the changed path from that RPC

#### Scenario: The byte-bounded batching contract still holds
- **WHEN** a `do_get` with an explicit `max_batch_bytes` is drained after the levers land
- **THEN** no emitted batch exceeds the cap, the credit reservation is still trued up downward from `get_array_memory_size()`, and mid-stream cancellation still terminates the stream

#### Scenario: Touched files respect the campsite rule
- **WHEN** the diff is gated
- **THEN** the `file-size` ratchet passes without `CQLITE_ALLOW_FILE_GROWTH=1`, and any growth in `arrow_convert.rs` is instead delivered as a split by responsibility
