# Tasks: keyed-access-distribution-probe (issue #2827)

> Design decided in `design.md`. **Scope:** build a bounded partition repeat-access instrument
> (fixed-cardinality buckets `1 | 2 | 3-4 | 5-8 | 9-16 | 17+`, fixed 3 MiB memory, measured
> working-set bytes) plus a committed decision procedure. **NOT in scope:** a synthetic Zipf round, a
> hit-ratio-vs-skew curve, a field skew number, the go/no-go verdict, or any decoded-partition cache.
> Issue #2827's **AC2 is NOT SATISFIED** by this change — see the AC map at the top of
> `specs/partition-access-distribution/spec.md`. The owner's instruction that the issue stop calling
> itself the gate is **recorded, not executed** (retitle/re-scope is an owner action).

## 1. Catalog + attribute registration (surface: `cqlite_core::observability::catalog`)
- [x] Add the four metric constants to `cqlite-core/src/observability/catalog.rs` with doc comments in
      the existing house style (name, instrument, unit, bounded attributes):
      `READ_PARTITION_ACCESS_DISTINCT_PARTITIONS` (`cqlite.read.partition_access.distinct_partitions`,
      counter, `{partition}`), `READ_PARTITION_ACCESS_ACCESSES` (`…accesses`, counter, `1`),
      `READ_PARTITION_ACCESS_BYTES` (`…bytes`, counter, `By`),
      `READ_PARTITION_ACCESS_SAMPLE_DENOMINATOR` (`…sample_denominator`, gauge, `1`).
- [x] Add the two bounded attribute keys to `catalog::attr` beside `LOOKUP_ROUTE` (`catalog.rs:82`):
      `REPEAT_BUCKET = "cqlite.read.repeat_bucket"` (closed set `1|2|3-4|5-8|9-16|17+`) and
      `SIZE_SOURCE = "cqlite.read.size_source"` (closed set `index|successor_gap|unavailable` —
      **amended by the R1 rider below**; originally written `index|unavailable`). Document both value
      sets in the doc comment — this is what the docs generator and the reviewer read.
- [x] Register all four in `ALL_METRICS` (`catalog.rs:907`) in read-path order.
- [x] Add four `MetricDoc` entries to `cqlite-core/src/observability/operator_docs_annotations.rs`
      (`kind`, `unit`, `summary`, `attributes`, `interpretation`, `round_item`). Generation is
      fail-closed on a missing annotation (`operator_docs.rs:16-19`, `:116`) — an omission fails the
      build, not review.

## 2. The recorder (surface: `cqlite_core::observability::partition_access`) — TDD, tests first
- [x] New module `cqlite-core/src/observability/partition_access.rs`, exported from
      `observability/mod.rs` beside `catalog`/`config`. Keep it under the ~800-line source target
      (campsite rule); if the emit path grows, split emit from counting rather than growing the file.
- [x] Write the failing tests first in `cqlite-core/tests/issue_2827_partition_access_histogram.rs`:
      bucket boundaries at 1/2/3/4/8/9/16/17, `accesses` per bucket = sum of member counts,
      emit-exactly-once + reset on close, empty-window silence, ≤25 distinct series, and the
      attribute-key allowlist assertion.
- [x] Implement the counting table: open-addressed, linear probing, `SLOTS = 1 << 17`, entry =
      `key_hash: AtomicU64` + `bytes: AtomicU64` + `count: AtomicU32` + `flags: AtomicU8` + padding =
      24 B ⇒ **exactly 3 MiB**. `RwLock` read-locked on the hot path (relaxed atomics on the slot),
      write-locked only for downsample and window close. Allocate **lazily** on first enable.
- [x] Implement adaptive hash-prefix downsampling at load factor 0.75: increment `k`, one linear pass
      dropping non-matching entries, `sample_denominator = 2^k`. Cap `k` at 20 and mark the window
      non-census at the cap. **No recency/frequency/arrival-order eviction** — `design.md` D4 records
      why those bias toward "go".
- [x] Implement the window: tumbling; closes on duration (default 60 s) **or** access count (default
      5,000,000) **or** an explicit public `close_window()`. Every correctness test drives
      `close_window()` — no test asserts on elapsed wall time (`roborev-lints` / #2642).
- [x] Implement the runtime gate `CQLITE_PARTITION_ACCESS_PROBE` (default OFF): `OnceLock`-cached env
      read plus a programmatic override that wins, mirroring `CQLITE_READ_PATH`
      (`cqlite-core/src/query/select_executor/forcing.rs:42`, `:78-82`). Disabled ⇒ one relaxed atomic
      load, zero allocation, zero emission.
- [x] Verify the hash is over the **raw partition-key bytes** the point path already holds
      (`partition_lookup.rs:52-57`), not the Murmur3 token — the BIG point path
      (`partition_lookup.rs:63-66`) does not compute a token and must not be made to.

## 3. Byte weighting + the BTI fail-closed path (surface: `record_partition_access`)
- [x] Write the failing tests in `cqlite-core/tests/issue_2827_partition_access_bytes.rs`:
      max-not-sum semantics over repeated accesses; **per the R1 rider below**, an access whose
      extent is measured lands under `size_source="successor_gap"` (BOTH formats — no Cassandra 5.0
      index records a size) and an unmeasurable one under `size_source="unavailable"` with **zero**
      bytes; a mixed window shows both series. (Originally written as a BIG `index` /
      BTI `unavailable` split, which the false-premise finding superseded.)
- [x] Thread the byte weight from the per-SSTable resolution sites into the open logical access:
      `PartitionLoc.data_size` (`cqlite-core/src/storage/cache/global_key_offset.rs:76-81`) summed
      across resolved SSTables for that one access.
- [x] Implement the entry rule `bytes = max(bytes, this_access_bytes)` and the sticky
      `size_source = unavailable` flag, set whenever **any** resolved SSTable reported no size
      (BTI: `global_key_offset.rs:72-74`, `:94-100`; bare-offset return at `partition_lookup.rs:433`).
- [x] Assert in code and in test that **no size is ever estimated** — no successor-offset
      interpolation, no nominal default. Unavailable is reported, not filled in (`design.md` D6; this
      is the #28 no-heuristics boundary for this change).
- [x] Make the BTI test resolve its fixture per TABLE via
      `cqlite-core/tests/support/datasets_root.rs::sstables_root_for_table` and assert **per case**
      (`must_run` for committed fixtures) — never a suite-wide `assert!(ran > 0)` (#3220).

> **RESOLVED — owner ruling, 2026-08-06: option (b), the successor gap, APPROVED and
> ruled SCOPE-PRESERVING** (a mechanism fix under the existing Seam-1 approval, since
> the approved deliverable was "measured working-set bytes, verdict falls out of the
> first real window" and (b) is the only option that keeps that true).
>
> The finding it resolves: design D6 assumed the BIG `Index.db` supplies a
> per-partition size. It does not — a Cassandra 5.0 BIG index entry is
> `[key][data_offset vint][promoted_index_len vint][promoted_index]`
> (`docs/sstables-definitive-guide/chapters/06-index-and-summary.md`; written by
> `BigTableWriter.createRowIndexEntry` at tag `cassandra-5.0.8`) with no size field,
> and the BTI trie resolves an offset only. Extents are therefore MEASURED as the
> successor gap. Four binding riders, all delivered:
>
> - **R1** — `size_source` gains a distinct third value `successor_gap`; `unavailable`
>   still fails closed at zero bytes.
> - **R2** — `design.md` D6/D3 amended in this same change, cited to format authority
>   only, plus the false `PartitionLoc` doc comment corrected.
> - **R3** — both honest costs recorded in the amendment: the last partition bounds to
>   the uncompressed data-section length, and the extents are uncompressed offsets
>   (the correct decode-multiplier input; write surface is uncompressed-only, #1406).
> - **R4** — the transposed `5-8`/`9-16` figures fixed in the SPEC text.
>
> **Verified, not assumed:** the successor offset IS reachable at the LOGICAL
> point-read boundary. Both weight resolvers already consult reader-level state there
> (the same granularity as the B4 key-offset cache), and D2 itself assigns byte
> weights to the per-SSTable resolution while keeping COUNTING logical. **The probe
> did not move to a per-SSTable site.**

## 4. Wiring at the logical point-read boundary (wiring evidence)
- [x] Call `record_partition_access` once per logical partition at the core targeted path:
      `classify_partition_lookup` yielding `Targeted`/`MultiTargeted`
      (`cqlite-core/src/query/select_executor/lookup.rs:92`), at its consumers
      `streaming.rs:107` and `stream_agg.rs:196`.
- [x] Call it once per key of the returned `PointReadPlan` at the Flight point path
      (`cqlite-flight/src/producer_point.rs:83` `point_read_keys`).
- [x] Do **NOT** count at the per-SSTable probe sites in
      `cqlite-core/src/storage/sstable/reader/partition_lookup.rs` (`:84`, `:128`, `:152`, `:349`,
      `:410`, `:436`) — they supply byte weights only. Add a comment at each naming the D2 reason
      (per-SSTable counting multiplies repeats by the generation count and manufactures
      concentration), so the next reader does not "fix" it. **Comment placement note:**
      `partition_lookup.rs` (833 lines) is already over the campsite ratchet, so the D2
      rationale is recorded instead in the three wiring files that DO carry the decision —
      `storage/partition_access_weight.rs`, `write_engine/merge/point_read.rs`
      (`KeySizeNote` / `PointAccessRecording`) and `producer_warm.rs` — plus amended D6.
- [x] Add a regression test that a partition present in several generations registers exactly **one**
      access for one logical read.
- [x] End-to-end wiring test `cqlite-flight/tests/issue_2827_partition_access_e2e.rs`
      (`#![cfg(feature = "observability-testing")]`, own test binary — the capture harness installs a
      process-global meter provider; mirror the header rationale in
      `cqlite-flight/tests/metrics_capture_test.rs:1-32`): repeated keyed `do_get` point reads with a
      full-PK equality `FlightTicket::filter` (`cqlite-flight/src/ticket.rs:259`) over the
      `cassandra_easy_stress.keyvalue` fixture (`cqlite-flight/src/test_fixtures.rs:40-53`, feature
      `test-util`, `cqlite-flight/src/lib.rs:72-74`), asserting the recovered histogram **and** that
      the reads reported `streaming_partition_lookup` (`cqlite-core/src/query/access_path.rs:126`) —
      never bare `partition_lookup` (`:122`), which this route does not emit.

## 5. Known-input validation (the oracle-legitimacy requirement)
- [x] Add the census recovery cases to
      `cqlite-core/tests/issue_2827_partition_access_histogram.rs`: a known skewed sequence
      (10 partitions × 20 accesses, 100 × 3, 1000 × 1) recovered **exactly**, and a uniform sequence
      (5,000 × 1) that reports everything in bucket `1`.
- [x] Add a forced-downsample case asserting the bucket **fractions** survive, `sample_denominator > 1`,
      and no survivor's count is under-recorded.
- [x] Derive every expected value from the input sequence by arithmetic that does **not** pass through
      the instrument, so the test can fail.
- [x] Put the oracle-legitimacy note in the test file header, in its own words: a synthetic input is a
      legitimate oracle for a claim about the **instrument** and an illegitimate one for a claim about
      the **world** (CLAUDE.md round-trip invariance #3042 / two parity oracles #1742). Assert **no**
      hit ratio, cache size or skew parameter anywhere in this file.

## 6. The decision procedure (surface: a committed research note)
- [x] Write `docs/research/decoded-partition-cache-decision.md` covering, in order: inputs; the single
      assumption (decode multiplier `m`, cited to the Phase-0 wire estimate at
      `docs/research/phase2-verify-caching.md:221-222` and labelled an assumption); the four refusal
      conditions checked first; the closed-form clairvoyant ceiling
      `H_max(C) = [Σ_taken (a_b − n_b) + f·(a_last − n_last)] / A` with buckets ordered by
      `accesses/bytes` and `C/m` on-disk bytes filled greedily; the recommended threshold
      `H_max(128 MiB) ≥ 0.50` **labelled an owner-settable parameter**, with its Arm-1 arithmetic
      (#2818: k-way merge 3.2% on-CPU vs decompress+CRC ~23%); and the tumbling-window bias with its
      conservative direction.
- [x] Include a worked example computed from the validation test's known distribution, labelled a
      **self-check, never a field result** (refusal condition 4).
- [x] State the scope paragraph: this note is the procedure, not the verdict; the verdict awaits a real
      keyed workload; AC2 is unmet.

## 7. Docs, catalog pages and the bundled correction
- [x] Regenerate the operator pages: `cargo run -p cqlite-core --example gen_operator_metrics_doc` →
      `docs/reports/flight-metrics-reference.md` + `website/src/content/docs/agents-using/flight-metrics-reference.md`
      (`operator_docs.rs:35`, `:40`). Do not hand-edit either file.
- [x] Hand-edit `docs/observability/configuration.md`: add the four metric rows to the read-path table
      and the two attribute value-set rows to the bounded-attribute table.
- [x] **Correction (same change, because it is this change's premise):** fix
      `docs/observability/configuration.md:215` and the instrument description at
      `cqlite-core/src/observability/otel.rs:384` — `cqlite.read.partition_lookup.total` is keyed by
      `cqlite.read.lookup_route` (`catalog.rs:82`, `:283`; emission at `partition_lookup.rs:87`,
      `:156`, `:353`, `:414`, `:440`), **not** `cqlite.query.access_path`.
- [x] **Correction:** add `streaming_partition_lookup` and `metadata_partition_lookup` to the
      documented `cqlite.query.access_path` value set at `docs/observability/configuration.md:298`
      (both emitted by `cqlite-core/src/query/access_path.rs:125-126`).
- [x] Confirm the corrections change **documentation and description strings only** — no attribute on
      an existing metric added, removed or renamed.
- [x] Update `docs/architecture/throughput-program-2026-07.md` M13 (`:344`, `:504`) to record the
      re-scope: instrument + procedure delivered, field number and go/no-go not delivered.
- [x] Doctrine check: no CLAUDE.md rule changes (no new gate component, no new agent-facing workflow).
      If review disagrees, update CLAUDE.md **and** the website `agents-developing/` page in this same
      change, and accept publication by grepping the served page for a new distinctive phrase — never
      by HTTP 200.

## 8. Honesty clause, carried into every artifact
- [x] Verify the identical claim appears in `proposal.md`, `design.md`,
      `specs/partition-access-distribution/spec.md` and
      `docs/research/decoded-partition-cache-decision.md`: **instrument + procedure delivered; field
      number and go/no-go NOT delivered; AC2 not satisfied, not waived; blocked by the absence of a
      field keyed workload with captured concentration
      (`docs/research/phase2-verify-caching.md:214-216`).**
- [x] Verify no artifact calls any output a measured field skew, the go/no-go, or a gate, and that no
      artifact contains a hit-ratio-vs-skew curve.
- [x] Record the owner instruction ("the issue must stop calling itself a gate") and that
      retitle/re-scope is an **owner action not taken here**; raise it as a NEEDS-YOU item on the PR.
      **Done by the owner:** the issue is retitled *"Keyed access-distribution probe: instrument +
      decision procedure (verdict lands with the first real keyed workload)"*, and no artifact in
      this change carries the "gate" framing (verified: the only occurrences are negations).

## 9. Verification and delivery
- [ ] `bash scripts/agent-gate.sh --lite` green on every fix round (summary-file redirect;
      `AGENT_GATE_SUMMARY_FILE=/tmp/gate-2827-lite.txt`), plus the diff-relevant targets:
      `cargo test -p cqlite-core --test issue_2827_partition_access_histogram`,
      `--test issue_2827_partition_access_bytes`, and
      `cargo test -p cqlite-flight --features observability-testing --test issue_2827_partition_access_e2e`.
- [ ] `cargo test -p cqlite-core --features observability-testing` for the capture-harness cases;
      `cargo tree`-based `observability_no_otel_default.rs` still green (default build links no OTel).
- [ ] rust-reviewer + `bash scripts/flow/roborev-review.sh --agent codex --model gpt-5.6-sol
      --repo <abs worktree path> --base origin/main` on the lite-green diff, **before** the first full
      gate (review-first). Push first — an unpushed commit is an empty-diff cause. Retain only the
      `==== ROBOREV REVIEW SUMMARY ====` block. Any non-PASS terminal `RESULT` (including
      `NOTHING-TO-REVIEW`) is a blocked merge.
- [ ] Open the PR; hand the endgame to `flow-closer`: **ONE** full `bash scripts/agent-gate.sh`
      (summary-file redirect; verify `RESULT: PASS` **and** `tree-integrity:` **and** a matching
      `run-id:`) → `spec-auditor` C intent audit against this spec → final roborev → arm
      `gh pr merge --auto --squash --delete-branch` after `scripts/flow/premerge-assert.sh` → finalize.
- [ ] Gate components that must be green because this change touches them: `operator-metrics-doc`,
      `kit-dashboard-drift`, `core-tests`, `roborev-lints`, `file-size`, `minimal-build`.
- [ ] File the follow-ups at merge, each linked to #2827: **F1** keyed load mode for
      `tools/flight-loadgen` (a `KeyedZipf` shape setting a full-PK equality `FlightTicket::filter`
      **and** key-derived Murmur3 bounds, asserted on `streaming_partition_lookup`; amends
      `tools/flight-loadgen/README.md:68-71`); **F2** automated cross-language Murmur3 parity test
      (`cassandra_murmur3.rs:488-513` ↔ `Murmur3TokenTest.java:120-128`, hand-copied per its Javadoc
      `:14-16`); **F3** measure the decode multiplier `m`; **F4** exact BTI partition size via
      successor offset (only if exact, never as an estimate).
- [ ] Batch any roborev **nits** into ONE linked follow-up issue at merge time; fix every **blocker**
      pre-merge with a `--lite` re-cert per round.
