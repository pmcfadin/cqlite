# Tasks: arrow-encode-doget (issue #3096)

> **Seam-1 owner decisions pending** (see `design.md` §"Owner decisions needed at Seam 1"):
> (1) where the committed rig lives; (2) whether the `arrow_convert.rs` file-size split is a
> precursor commit in this PR; (3) lever 5's wire blast radius; (4) whether to stop early if the
> cheap levers already reach the ratio. **Do not start section 2 before these are answered.**
>
> **Owner-approved scope carried in (attended decision 2026-08-03):** the reproduction rig named in
> the issue (`/home/ubuntu/ws0-local/`) does NOT exist and was never committed, so this change ALSO
> reconstitutes and COMMITS the corpus generator, the measurement scripts, and an in-repo digest
> oracle. AC1 is re-pinned to its RATIO form; the absolutes 240,100 / 312,155 rows/s are NOT
> thresholds.
>
> Binding constraints: acceptance is an external rows/s ratio, never a CPU-share shift (AC2); the
> two measurement traps are mandatory (AC3); output invariance is proven by the in-repo digest, not
> the absent `0x0a2a390223bde6aa` (AC4); warm and cold stay separate (AC5); the kill criterion is
> binding and a correctly reported negative result IS a satisfying outcome (AC6).

## 0. Commit the rig and re-baseline (BLOCKING — gates every acceptance claim)
- [ ] Record in the PR that `/home/ubuntu/ws0-local/` is absent, that `docs/reports/ws0-3026-artifacts/
      ws0-cqlite/scan-harness/Cargo.toml` points at the dead path `/home/ubuntu/workspace/wt-3026/
      cqlite-core`, and that the #3100 corpus is Cassandra-written + LZ4 — so no #3096 number is
      currently reproducible on this box.
- [ ] **Corpus generator** (surface: new `tools/ws0-corpus-gen` binary): drives the production
      `cqlite_core` `SSTableWriter` from the pinned `ws0.events` DDL
      (`docs/reports/ws0-3100-artifacts/ws0-h2h/schemas/ws0-events.cql`); 4,000,000 rows as
      40,000 partitions x 100 rows; partitions emitted in Murmur3 token order; **uncompressed**
      (assert no `CompressionInfo.db` is written — #1406); deterministic from a recorded seed.
- [ ] Generator self-checks: re-running with the same seed produces a byte-identical `Data.db`;
      writing/observing 0 rows exits non-zero; the emitted row/partition counts are asserted, not
      assumed.
- [ ] Record the corpus identity in-tree: `sha256`, row count, cells/row, on-disk bytes, bytes/row.
      State explicitly that it differs from #3058's `0185909de6da…` by construction and that the
      old digest is NOT asserted.
- [ ] Document the corpus as a **PERFORMANCE FIXTURE ONLY** in the generator's own docs — a
      CQLite-written + CQLite-read corpus is invariant to a uniform framing error (#3042) and is
      never a correctness oracle for on-disk framing.
- [ ] **Measurement scripts** (surface: `scripts/perf/…` + method doc under `docs/reports/`):
      CPU-wide `perf stat -C <cpu-list>`, **no** `perf stat -p` anywhere; `taskset` pinning to a
      pair read from `/sys/devices/system/cpu/cpu*/topology/thread_siblings_list` that **fails
      closed** if the pair is not siblings of one physical core; median of 3 with spread; setup
      subtracted from the cycles/row denominator; row denominator printed with every figure.
- [ ] Both arms in ONE session on ONE pinned pair: bare scan via `execute_streaming`, Flight
      `do_get` over a real loopback transport. Warm and cold as separate runs, reported separately.
- [ ] **In-repo Arrow-buffer digest oracle** (surface: `cqlite-flight/tests/`): folds each emitted
      `RecordBatch`'s value + validity buffers in column order plus row count; asserts equality
      across `CQLITE_FLIGHT_MERGE_PATH=bypass|merge` at a PINNED `now`, plus row count and
      cells/row. Shares `PROBE_LOCK` with `issue_3058_forced_path_differential.rs` (process-global
      env). One `#[test]`, cases in a list.
- [ ] Close the attribution blind spot: `StreamSubPhase::Encode` (`egress_flush.rs:116-119`) times
      only `flush_buffer`, NOT `encode_do_get`'s IPC framing. Add a sub-phase around the encoder
      stream, or record explicitly that levers 4/5 are attributed by `perf` alone.
- [ ] **Re-baseline pre-change** on the regenerated corpus: bare scan and `do_get` (bypass arm)
      rows/s + cycles/row, warm and cold. This pair, not 210,192 / 312,155, is the baseline.

## 1. File-size precursor (surface: `cqlite-core/src/export/arrow_convert.rs`)
- [ ] Split `arrow_convert.rs` (2,596 lines, ~4x the ~800 target) by responsibility before any
      behavioral lever — builders / schema / conversion entry — so the `file-size` ratchet never
      forces `CQLITE_ALLOW_FILE_GROWTH=1`. Mechanical, no behavior change; digest unchanged.
- [ ] Check `cqlite-flight/src/producer.rs` (3,243 lines) for the same problem before touching it.

## 2. Lever 4 + 6 — the cheap floor (surfaces: `cqlite-flight/src/batch_bytes.rs`, `streaming.rs`, `cqlite-core/src/export/arrow_convert.rs`)
- [ ] Lever 4: align `DEFAULT_MAX_BATCH_BYTES` (`batch_bytes.rs:154`, 4 MiB) with arrow-flight's
      `GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES` (2 MiB, `encode.rs:166`) — or raise the encoder's limit —
      so a batch is not re-sliced and framed twice. Re-derive the narrow-shape table at
      `batch_bytes.rs:137-153`; halving the cap moves where the byte-cap starts binding.
- [ ] Lever 6: cache the Arrow `Schema` instead of rebuilding it per batch at
      `arrow_convert.rs:201-203`.
- [ ] Measure each individually against the Phase-0 baseline; report rows/s AND cycles/row.
- [ ] `--lite` + the digest oracle + `issue_2825_max_batch_bytes_e2e.rs` after each.

## 3. Lever 2 + 3 — builder and estimator passes (surfaces: `arrow_convert.rs`, `arrow_size.rs`, `arrow_columnar.rs`)
- [ ] Lever 2: replace the intermediate `Vec<Option<T>>` in each scalar builder
      (`arrow_convert.rs:1407,1461,1487,1524,1555,1596`) with `PrimitiveBuilder::with_capacity(n_rows)`
      appends + an explicit null bitmap. Note `:1555`/`:1596` hold BORROWED `&str`/`&[u8]` — pointer
      vectors, not data copies — so their gain is smaller; measure, do not assume.
- [ ] Lever 3: fold `estimate_arrow_row_bytes` (`arrow_size.rs:251`, per-cell map probe at `:254`)
      into the transpose/append pass, removing the third hash pass per row.
- [ ] Prove the invariant survives: `arrow_size_tests.rs` (`Σ estimate >= realized payload` over
      `arrow_shape_corpus.rs`) and `batch_bytes_tests::the_capacity_bound_holds_over_the_shared_shape_corpus`
      both green, and the byte-cap cut at `producer_stream.rs:351-358` cuts on the same rows.
- [ ] Measure each individually; digest unchanged.

## 4. Lever 1 — column-major build from the scan row carrier (surfaces: `cqlite-core/src/query/select_executor/row_build.rs`, `cqlite-flight/src/producer.rs`)
- [ ] Add a column-major emit seam alongside `build_row_from_scan_cached` (`row_build.rs:227`) that
      writes cells positionally from the authoritative schema, skipping the per-row
      `HashMap<Arc<str>, Value>` (`:246`) and the two downstream hash probes
      (`arrow_columnar.rs:87`, `arrow_size.rs:254`).
- [ ] Column identity SHALL come from the ticket schema positionally — never inferred from bytes
      (#28). No `unwrap()`/`expect()`; clean under `RUSTFLAGS="-D warnings"`.
- [ ] Keep the `QueryRow` path intact for every non-`do_get` consumer; do not fork the adapter.
- [ ] Prove output invariance: the digest oracle, `issue_3058_forced_path_differential.rs`,
      `query_semantics_flight_parity.rs` + core `query_semantics_oracle_parity.rs` at pinned `now`,
      and `do_get_transport_test.rs`'s wire-frame golden.
- [ ] Measure individually. If lever 1 does not move rows/s beyond the spread, STOP — see §6.

## 5. Lever 5 + 7 — optional, gated (surfaces: `cqlite-flight/src/streaming.rs`, `row_build.rs`)
- [ ] Lever 5 (only per the Seam-1 decision): `DictionaryHandling::Resend` or hydrate-once at
      `streaming.rs:599`, since the schema has no dictionary columns and `hydrate_dictionaries`
      (`encode.rs:685`) is a pure per-batch `RecordBatch` rebuild. Verify against a real Trino/JDBC
      client shape, not only `FlightRecordBatchStream`; the byte golden is the guard.
- [ ] Lever 7 (stretch, strictly behind lever 1): borrow text/blob from the decoded chunk instead of
      `into_owned()` (`row_build.rs:259`, `types.rs:1234`). Requires a chunk-retention story that
      holds the <128MB bound. **May be dropped without failing the change** — record the decision.

## 6. Acceptance measurement + the kill criterion (correctness must be green FIRST)
- [ ] Final same-session run: bare scan and `do_get`, same box, same pinned sibling pair, same
      bytes, median of 3, warm and cold separately, rows/s AND cycles/row, spread stated.
- [ ] PASS = `do_get rows/s >= bare_scan rows/s / 1.3` in that same session. Never a CPU-share
      claim; a fall in "% cycles in Arrow encode" with unmoved rows/s is a **FAIL** (#2877 shape).
- [ ] Report each lever's individual delta, including any lever that cost throughput.
- [ ] **Kill criterion:** if the levers do not move rows/s materially, STOP — post the negative
      result (rows/s + cycles/row, warm and cold), do not stack further levers, re-open attribution.
      A correctly measured, correctly reported negative result CLOSES this change satisfactorily.
- [ ] State plainly what remains OWED: the WS0 absolutes, the stock-Cassandra leg (no Cassandra on
      this box), and the compressed-corpus shape (#1406). Never restate them as reproduced.

## 7. Scope-fence check before review
- [ ] No allocator work (#3028) and no `RowColumnResolution` hoist (#3047) smuggled in — flag them
      for re-pricing instead.
- [ ] No mpsc handoff-limiter change; no cold-route (#3068) change.
- [ ] No query-result change: digest, differential, wire golden, semantics oracles all unchanged.
- [ ] No new public CLI/Python/Node surface; Flight wire format unchanged (or lever 5's change is
      explicitly owner-approved and byte-pinned).

## 8. Docs / doctrine
- [ ] Commit the method doc (traps, pinning, denominators, warm/cold separation) with the scripts.
- [ ] Re-price `docs/architecture/throughput-program-2026-07.md` and the #3028 / #3047 estimates
      against the post-change profile — or record that the kill criterion fired and they stand.
- [ ] Update `CLAUDE.md` / the website `agents-developing/` page only if a user-facing or workflow
      rule actually changed; verify a publish by grepping the served page for new content, never by
      HTTP 200.

## 9. Gate + review + sign-off
- [ ] `--lite` every fix round (summary-file redirect); never a full gate per round.
- [ ] `rust-reviewer` + `bash scripts/flow/roborev-review.sh --agent <agent> --model <model>
      --repo /home/ubuntu/projects/cqlite-wt/issue-3096` on the lite-green diff, BEFORE the full gate.
- [ ] Open the PR; hand the endgame to `flow-closer`: ONE full `scripts/agent-gate.sh` → `C`
      (spec-auditor against `openspec/changes/arrow-encode-doget/specs/**`) → final roborev →
      `gh pr merge --auto --squash --delete-branch` on green → `flow-finalize`.
- [ ] Paste the full `AGENT-GATE SUMMARY` (verify `RESULT: PASS|FAIL` and `tree-integrity:`) and the
      `ROBOREV REVIEW SUMMARY` in the PR.
