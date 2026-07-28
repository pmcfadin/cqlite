# Tasks: flight-single-sstable-bypass (issue #3058)

> **Seam-1 owner decisions SETTLED** (implementation landed against them: warm route only;
> ratio closure on a locally generated corpus; `CQLITE_FLIGHT_MERGE_PATH` is a PERMANENT
> documented seam). Historic note follows.
>
> **(historic) Seam-1 owner decisions** (see `design.md` §"Owner decisions needed at Seam 1"):
> (1) warm route only vs warm + cold `MergeInput::Paths`; (2) how to re-establish the WS0
> measurement assets — **`/home/ubuntu/ws0/**` does not exist on this box** (verified: `/home/ubuntu`
> contains only `workspace`), so the pass condition is not verifiable until the corpus + drivers are
> recovered or regenerated and re-baselined; (3) whether `CQLITE_FLIGHT_MERGE_PATH` is a permanent
> documented seam or test-cfg-only. **Do not start section 2 before these are answered.**
>
> Binding constraints carried from the acceptance contract: PASS is an EXTERNAL rows/s number, not a
> CPU-share shift (§1); correctness is proven by the semantic oracles + differentials, not by
> benchmarks (§2); NO allocator work and no #3060/#3061 (§3); the kill criterion is binding (§4);
> warm and cold reported separately (§5).

## 0. Re-establish the measurement baseline (BLOCKING — gates the pass condition)
- [ ] Report to the owner: `/home/ubuntu/ws0/ws0-corpus/rerun.sh`, `/home/ubuntu/ws0/ws0-h2h/`, and
      `/home/ubuntu/ws0/ws0-results/head-to-head-method.md` are **ABSENT** on this box. The box is a
      16-vCPU Intel Xeon Platinum 8488C / 30 GB with `/usr/bin/perf` and `/usr/bin/taskset`
      available, so it can host the measurement once the assets exist.
- [ ] Recover the assets, or regenerate the corpus and verify
      `Data.db sha256 == 22d9ae224b439b2176c287a59eee6a7d1f08b4f1fafc4d2198b3da50cdce922c`
      (3,999,890 rows, LZ4 `chunk_length=16384`, single `nb` SSTable after flush + `nodetool compact`).
      A corpus that does not reproduce that digest is NOT a like-for-like comparison — say so
      explicitly rather than comparing against WS0's numbers.
- [ ] Re-baseline the PRE-change Flight `do_get` and bare-scan numbers on THIS box (warm, per
      physical core, pinned, CPU-wide counters, median of ≥3, spread reported). Never compare a
      post-change number on this box against a WS0 number measured elsewhere.

## 1. Predicate + branch skeleton (surface: `cqlite-flight/src/producer_warm.rs`)
- [x] Add the conjunctive, fail-closed bypass predicate at the full-scan decision point
      (`producer_warm.rs:110`, AFTER the `is_aggregating()` early return at `:52`, AFTER
      `prune_readers` at `:56`, AFTER the point-read route at `:75`): post-prune count == 1 AND
      `schema.dropped_columns.is_empty()` AND forced-path override != merge. No byte/size/statistics
      inference (#28).
- [x] Add the forced-path override (`CQLITE_FLIGHT_MERGE_PATH=bypass|merge`, unset = automatic),
      mirroring `CQLITE_READ_PATH`; read it once per request, not per row.
- [x] Add the path-taken observation seam (merger-construction + reconcile-entry counters/probe) the
      AC #1 test asserts on — an explicit marker, never a timing inference.
- [x] Unit tests: predicate is 1-source-only; 2 sources → merge arm; post-prune (not pre-prune) count
      is used; non-empty `dropped_columns` → merge arm; forced `merge` → merge arm; aggregate never
      reaches the branch.

## 2. Single-source row source + adapter (surface: `cqlite-flight/src/producer_stream.rs`, new adapter module)
- [x] Factor the drive loop's ROW SOURCE behind a small internal trait so `drive_merge_over`
      (`producer_stream.rs:165`) keeps ownership of batching, `max_batch_bytes` (`batch_bytes.rs`),
      `CancelFlag` polling, `ScanProgress`, and the `on_merger_built` phase-boundary fire — do NOT
      duplicate the loop.
- [x] Implement the fast source over `SSTableReader::scan_stream_batched_admitted`
      (`reader/data_access/sequential.rs:539`), passing the ScanSpec token bound so the
      Summary-guided walk still prunes (#2412/#2413), and the request's admission context (#2420).
- [x] Implement the `(RowKey, ScanRow) → QueryRow` adapter by REUSING
      `cqlite_core::query::build_row_from_scan_cached` (`select_executor/row_build.rs:227`) with a
      caller-owned `PartitionKeyCache` — the same adapter the 367,760 rows/s bare scan uses
      (`select_executor/streaming.rs:142,227,308`). Do not write a second adapter.
- [x] Thread the request's `now_secs` into the fast path's shadow/TTL clock explicitly (the merge arm
      does this at `producer_warm.rs:115-117`); assert it is not an ambient wall-clock read, or the
      pinned-`now` oracles cannot be honored.
- [x] Assert the fast path's parser is built with `read_shadowing = true`
      (`scan_stream_windowed.rs:748-751`) rather than assuming it.
- [x] Verify the fast path decodes with `want_cell_metadata == false` and constructs zero
      `CellWriteMetadata` maps (AC #2) — by not entering `CompactionPolicy::on_data_row`, not by
      tuning it.
- [x] Confirm predicate pushdown, projection, and the UDT registry still apply on the fast arm
      (`filter.rs` application point unchanged).

## 3. Path-taken pins (surface: `cqlite-flight/tests/`)
- [x] AC #1: an e2e `do_get` over a real single-SSTable fixture asserts merger-construction count == 0
      AND reconcile-entry count == 0 — the test FAILS if the merge path is taken. Not a throughput
      assertion.
- [x] AC #4 (inverse): an e2e `do_get` over a ≥2-overlapping-SSTable fixture asserts the merger IS
      constructed and reconcile entries are non-zero.
- [x] Assert emitted `QueryRow.cell_metadata` is still `None` on both arms (no consumer-visible change).

## 4. Correctness pins (surfaces: `cqlite-flight/tests/`, `cqlite-core/tests/`, `test-data/`)
- [x] **Forced-path differential** (the primary proof for gap (b)): run the SAME single-SSTable
      fixture through `do_get` under `CQLITE_FLIGHT_MERGE_PATH=bypass` and `=merge` at a PINNED
      `now`; assert identical rows, values, and order. Cover: partition deletion, range tombstone,
      row deletion, cell tombstone, expired-TTL cell, live-TTL cell, static column, and an
      `UPDATE`-written row with no PK liveness marker under both `SELECT *` and a PK-only projection
      (`producer.rs:1199-1220`, #2374/#2789).
- [x] **New ≥2-overlapping-SSTable Flight fixture** — built IN-PROCESS by the write engine in
      `cqlite-flight/tests/issue_3058_bypass_path_taken.rs` (two generations; the later one
      overwrites a value and deletes a row), asserting the merger IS entered AND the rows
      reconcile. No gitignored binary is added, so nothing needs `git add -f`. The
      `test-data/query-semantics-oracle.json` multi-generation CASE is NOT added (it needs a
      Cassandra-generated fixture; the in-process pin covers the Flight surface today).
      (historic) Every committed
      `test_compaction_tombstone_ttl` dir holds exactly ONE `nb-3-big-Data.db` (verified for
      `rt_cross_gen`, `ttl_expired_live`, `shadow_row_delete`), so the bypass would otherwise remove
      the ONLY oracle coverage of the multi-generation merge on the Flight surface. Generate a
      2-generation overlapping fixture (later generation overwrites a value and deletes a row) and
      add its post-reconciliation result set to `test-data/query-semantics-oracle.json` at the pinned
      `now`. `git add -f` any gitignored reference binary and verify against a fresh
      `git worktree add --detach HEAD`, not the dirty tree.
- [x] Run both semantic oracles green: gate components `query-semantics-oracle`
      (`cqlite-core/tests/query_semantics_oracle_parity.rs`) and `flight-query-semantics-oracle`
      (`cqlite-flight/tests/query_semantics_flight_parity.rs`).
- [x] Run `cqlite-core/tests/point_vs_full_differential.rs` (#1918) green.
- [x] Run the #2988 multi-generation pins green and unchanged:
      `issue_1579_streaming_multigen_memory`, `issue_1579_streaming_multigen_order`,
      `issue_957_streaming_materializing_parity`, `issue_2096_seeking_point_merge_parity`, and the
      `step_streaming_matches_step_for_*` oracles (`write_engine/merge/streaming.rs:1103,1149,1196,1230`).
- [ ] AC #5: full-scan the WS0 corpus through `do_get` and assert **3,999,890 rows, 12 cells/row,
      digest `0x4903ffa446163c4b`** — byte-identical output pre- and post-change.

## 5. Measurement (AC #3 — the PASS CONDITION; correctness must be green FIRST)
- [ ] Method, both traps honored and RECORDED in the report:
      **CPU-wide `perf stat -C <cpu-list>`, never `perf stat -p`** (per-process costs >2x here:
      163K vs 360K rows/s, ~540K context switches), and **`taskset` pinning is mandatory**
      (unpinned 18.74 s vs pinned 11.16 s; 1.98M vs 310K voluntary context switches).
- [ ] Warm, per **physical** core, median of ≥3, spread reported. Report **rows/s AND cycles/row AND
      bytes-of-memory-traffic/row** — never CPU-share (#3023 reporting contract).
- [ ] Report the cold number as a SEPARATE claim (§5); do not blend. #3068 owns the cold/IO side.
- [ ] Compare against: bare scan 367,760 rows/s (reference), Cassandra 212,981 rows/s (must beat),
      pre-change Flight 61,151 rows/s. Target **≥ ~280,000 rows/s/phys-core**.
- [ ] **KILL CRITERION (binding):** if rows/s does not move materially, STOP — post the negative
      result with rows/s + cycles/row (warm and cold), do NOT stack further levers, and re-open the
      attribution question (Arrow encode is the leading candidate: 59% of cycles / 37% of throughput
      / 675 B/row copied). A merge-CPU%-fell-but-rows/s-flat result is a FAIL, not a partial pass.

## 6. Scope-fence check before review
- [x] No allocator work (#3028) and no `RowColumnResolution` hoist (#3047) in this diff — they are
      held for re-pricing against the post-change profile.
- [x] No #3060 (mid-stream shutdown spin) and no #3061 (double mmap / RSS) in this diff.
- [x] No Arrow-encode changes and no mpsc-handoff changes in this diff.
- [x] Pre-roborev self-check: no-heuristics (the predicate is authoritative), no
      `manual_range_contains`, no integer overflow/saturation, no wall-clock threshold in a
      correctness test path, no gitignored reference binary left unadded.

## 7. Docs / doctrine
- [x] Document the forced-path override alongside `CQLITE_READ_PATH` in
      `docs/development/dev-cookbook.md` (if owner decision 3 makes it permanent).
- [ ] Update `docs/architecture/throughput-program-2026-07.md` with the measured post-change profile
      and note that #3028 / #3047 must be re-priced against it.

## 8. Gate + review + sign-off
- [x] `scripts/agent-gate.sh --lite` green each fix round (summary-file redirect; never raw stdout).
- [ ] `rust-reviewer` + roborev on the lite-green diff (review-first).
- [ ] Full gate ONCE via `flow-closer` (includes `query-semantics-oracle` and
      `flight-query-semantics-oracle`).
- [ ] **C (spec-auditor)** anchored to `openspec/changes/flight-single-sstable-bypass/specs/**`:
      every requirement `satisfied` with a public-surface (`do_get`) test as evidence.
- [ ] roborev clean (blockers fixed pre-merge; nits batched to a follow-up issue).
- [ ] `openspec validate flight-single-sstable-bypass --strict` clean; `openspec archive` after merge.

## 9. Residuals recorded at implementation time (issue #3058)

- **AC #3 / §5 measurement NOT performed in this change.** No local corpus was generated and no
  `perf stat -C` / `taskset` run was made; the ratio-closure claim and the kill criterion are still
  OWED. The wiring, correctness pins and the `CQLITE_FLIGHT_MERGE_PATH` seam needed to run it are in
  place.
- **AC #5 WS0 digest (3,999,890 rows, 12 cells/row, `0x4903ffa446163c4b`) NOT reproduced** — the WS0
  corpus is absent from this machine (recorded as owed, never claimed as verified).
- **STATIC columns are excluded from the bypass, fail-closed.** Measured on real Cassandra bytes,
  the two arms disagree in opposite directions (merge emits a `ck = null` static row and injects no
  static value into clustering rows; the single-generation decoder injects statics correctly but
  emits NOTHING for a static-ONLY partition). Both diverge from Cassandra; reconciling them changes
  the core read path and is a follow-up. Spec R5's "static-cell injection" clause is therefore
  UNREACHABLE on the fast arm today.
- **Gap (b) residual**: the marker-less `UPDATE t SET v=? WHERE pk=? AND ck=?` row shape is covered
  only in its EXPIRED-marker form (`ttl_expired_live`, under `SELECT *` and a PK-only projection).
  No committed fixture holds a marker-less CLUSTERING row and CQLite's writer cannot emit one, so
  that exact byte shape needs a Cassandra-generated fixture (owed).
- **Two pre-existing defects found while differentiating** (both reproduce with
  `CQLITE_FLIGHT_MERGE_PATH=merge`, i.e. on pre-#3058 code, and are out of scope here): a
  CQLite-written simple CELL TOMBSTONE surfaces as a raw `Value::Tombstone` that the Arrow encoder
  rejects on BOTH arms; and the merged-read assembler fails closed (#2339) on a composite-keyed
  `set<frozen<UDT>>`, so that column cannot be served by the merge arm at all.
- **File-size ratchet**: seven already-over-threshold core files grew by 1–28 lines each
  (parameter/probe plumbing); acknowledged with `CQLITE_ALLOW_FILE_GROWTH=1` (epic #1116). The new
  flight code lives in NEW modules (`bypass.rs`, `row_source.rs`) and `producer_stream.rs` was split
  back under the target.
