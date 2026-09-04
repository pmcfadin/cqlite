# Issue #3853 slice 1 — reader-scoped scan-lifetime madvise seam (IMPLEMENTATION SPEC)

TDD. Commit after every meaningful unit (a killed subagent loses uncommitted work).
Iterate with `scripts/agent-gate.sh --lite` ONLY. Do NOT run a full gate (two live P0 gate
blockers, #4029/#4032, red `tooling-tests` on main; not ours, not to be patched).

## What ships in slice 1

A reader-scoped scan-lifetime seam. NOT a policy flip. `PrefetchMode::Auto` keeps issuing NO
madvise (#1143) — do not touch that arm.

### 1. New module `cqlite-core/src/storage/sstable/reader/scan_lifetime.rs`

```rust
pub(crate) struct ScanLifetime { /* None inner == seam DISABLED */ }
pub(crate) struct ScanLifetimeGuard { /* Send + 'static: holds Option<Arc<ScanLifetime>> */ }
```

- `ScanLifetime::disabled() -> Arc<Self>`
- `ScanLifetime::for_scan_mapping(mmap: Arc<memmap2::Mmap>) -> Arc<Self>` (unix only; on
  non-unix always disabled)
- `fn begin(self: &Arc<Self>) -> ScanLifetimeGuard`
- `Drop for ScanLifetimeGuard`

Semantics, exactly:
- An in-flight count guarded by a `std::sync::Mutex<u32>`. `begin` increments; when the count
  goes **0 -> 1** issue `mmap.advise(Advice::WillNeed)`. Guard drop decrements; when the count
  goes **1 -> 0** issue `unsafe { mmap.unchecked_advise(UncheckedAdvice::DontNeed) }`.
- **The advice is issued WHILE HOLDING THE LOCK**, deliberately. An `AtomicUsize` leaves a
  window in which a 1->0 DONTNEED lands after a concurrent 0->1 WILLNEED and zaps a mapping the
  new scan is reading — issue #3853 constraint 3 forbids exactly that. Document that this is
  NOT a reintroduction of the #815 scan mutex: #815 removed a lock held for a scan's whole
  duration that serialized scans; this lock is taken twice per SCAN (begin + end), never per
  row/block/page, and covers only the counter transition and the one syscall whose ordering
  must be serialized with it. Say that in the module docs, with #815 named.
- Nesting is FINE and needs no exemption plumbing: several entry points delegate to each other
  (`iterate_all_partitions` -> `_cancellable` -> `_via_full_index` -> sequential fallback), so an
  inner guard just raises the count and only the OUTERMOST end releases. Contrast this with
  `scan_admission`'s `Exempt` (that one had a hold-and-wait deadlock; a counter cannot).
- Advise failures are non-fatal `tracing::debug!`, same posture as the existing sites.

`unsafe` justification, which MUST be in a `// SAFETY:` comment at the call site:
`memmap2::MmapOptions::map` maps `PROT_READ | MAP_SHARED` over a file (verified in
memmap2-0.9.11 `src/unix.rs:245-257`) and this reader's contract is that the file is immutable
for the reader's lifetime. For a **shared, file-backed** mapping `MADV_DONTNEED` discards
resident pages and the next touch repopulates from the up-to-date file contents — the same
bytes. memmap2 puts `DontNeed` behind `UncheckedAdvice` because for **anonymous/private**
mappings repopulation is zero-fill, which would change observable content under a live borrow;
that case cannot arise here. So no borrow can observe a change, and outstanding zero-copy
borrows into the mapping (`value_borrow.rs`) stay valid. State that the cost of a stale borrow
is a refault, never a content change.

Observability, **unconditionally `pub`, not cfg-gated** (per CLAUDE.md #3522: a crate-level
cfg-gated integration target executes in NO gate component, so a `work-counters`-gated
assertion would be coverage that never runs). Two `AtomicU64`s per reader, incremented once per
scan-start/scan-end transition — not per row — so the cost is nil and honest:
- `pub fn scan_lifetime_advice_counts(&self) -> (u64, u64)` on `SSTableReader` -> (willneed, dontneed)
- `pub fn scan_lifetime_in_flight(&self) -> u32`
Count ATTEMPTS (increment even if the syscall errors) and say so in the doc comment, or count
successes and say that — pick one and make the doc comment true.

### 2. Reader open wiring (`reader/mod.rs`, `backend_resolve.rs`)

- `mmap_advice_for` currently maps `PrefetchMode::WillNeed => Some(Advice::WillNeed)` and
  `build_block_sources` applies it at OPEN. Move the **WillNeed** case to scan start: at open it
  must issue NOTHING. `Sequential` keeps its open-time advice (explicit drop-behind opt-in,
  out of scope); `Off`/`Auto` unchanged (None). Keep `mmap_advice_for`'s existing unit tests
  meaningful — if you change its signature/contract, update `reader/tests.rs:626-645` and say
  why in the test.
- Build the reader's `scan_lifetime` field after `point_source` is built, and enable it ONLY when
  ALL of:
  1. `ScanSource::Mapped(mmap)` (mmap backend), and
  2. the resolved prefetch mode is `PrefetchMode::WillNeed`, and
  3. **the point plane does not share the scan mapping** — test it with
     `Arc::ptr_eq(&point_mmap, mmap)`. `point_read_mmap` returns `scan_mmap.clone()` when
     `file_size < POINT_MMAP_MADV_RANDOM_MIN_BYTES` (8 MiB) or when the dedicated map/advice
     failed, so `ptr_eq` is an exact test needing no signature change. Same Arc => seam
     DISABLED (issue #3853 AC bullet 3 + constraint 3: below the threshold `point_source` IS
     the scan mapping, so releasing would degrade the point plane).
  Otherwise `ScanLifetime::disabled()`.
- Document in the field's doc comment that the SCAN mapping is also aliased by
  `self.file` (the legacy positioned helper) and `scan_positional_source` (#2876), and that
  those are scan-plane/integrity users for whom a refault is content-safe — the guarantee the
  gate buys is specifically that the #2210 `MADV_RANDOM` **point** mapping is a different
  allocation.

### 3. Wire the guard at every scan entry point (twelve sites, three shapes)

Shape A — materializing async fns: `let _scan = self.begin_scan();` at the top.
1. `data_access/sequential.rs` `scan_inner`
2. `partition_lookup.rs` `iterate_all_partitions`
3. `partition_lookup.rs` `iterate_all_partitions_cancellable`
4. `data_access/compaction.rs` `iterate_all_partitions_for_compaction`
5. `data_access/full_index_scan.rs` `iterate_all_partitions_via_full_index`
6. `data_access/bti.rs` `bti_scan_with_metadata`
7. `data_access/bti.rs` `bti_scan_with_metadata_cancellable`

Shape B — callback walks: same, at the top.
8. `data_access/summary_scan/mod.rs` `stream_partitions_summary_guided`
9. `data_access/summary_scan/mod.rs` `stream_partitions_summary_guided_compaction`
10. `data_access/summary_scan/mod.rs` `stream_all_partitions_for_query`
11. `data_access/full_index_stream.rs` `stream_all_partitions_via_full_index`
12. `data_access/full_index_stream.rs` `stream_all_partitions_cancellable`

Shape C — spawned task + channel: the guard goes INSIDE the spawned task body, on the line
next to the existing `_admission` RAII guard, so its lifetime is the task's:
13. `data_access/per_row_scan_stream.rs` `run_scan_stream` (~line 165)
14. `data_access/batched_scan_stream.rs` `run_scan_stream_batched` (~line 197)

(The issue says "nine entry points"; that count is #2824's grouping. Wire every site listed
here — and if you find a scan entry point NOT in this list, wire it and NAME it in your report.
`stream_bti_scan` is reached from shape C and is nested; a guard there is harmless but say
which you chose and why.)

Compaction/merge force buffered I/O, so their sites are no-ops in practice — wire them anyway
(uniformity; a future backend change must not silently skip them) and say so in a comment.

## Tests — write them FIRST, and make each one fail for the right reason before fixing

New: `cqlite-core/tests/issue_3853_scan_lifetime_advice.rs` (integration, default features so
it actually EXECUTES in the gate's `core-tests`) plus in-crate unit tests in the new module.

Required properties:
1. **Unit, no I/O**: 0->1 issues exactly one WILLNEED; nested begins issue none; the count
   returning to 0 issues exactly one DONTNEED; a second scan afterwards issues a second
   WILLNEED. `disabled()` issues nothing ever.
2. **Unit, concurrency**: N threads each begin/hold/end overlapping; assert WILLNEED == 1 and
   DONTNEED == 1 and `in_flight() == 0` at the end, i.e. no release while a scan was live.
   Make the overlap deterministic (barriers), never a sleep race — CLAUDE.md forbids
   wall-clock threshold asserts in correctness tests.
3. **`Database::open` issues NO advice** — open a reader (mmap + `PrefetchMode::WillNeed`,
   file >= 8 MiB) and assert `(0, 0)` before any scan. This is the issue's headline AC and the
   defect #2824 was reverted over.
4. **One property per entry point**: for each wired site reachable from a real fixture, run it
   and assert counts go (1, 1) and `in_flight()` returns to 0. Where a site is not reachable
   from the committed corpus, say so in the test file rather than silently omitting it — a
   DECLARED gap, listed in one place, printed by the test.
5. **Point-plane exclusivity**: a sub-8-MiB mmap reader at `PrefetchMode::WillNeed` records
   `(0, 0)` for a full scan (seam disabled because `point_source` IS the scan mapping).
6. **Error paths release**: a scan that returns `Err` and a cancelled scan both return
   `in_flight()` to 0 (guard drop, not a success-path decrement).
7. `PrefetchMode::Auto` records `(0, 0)` — the #1143 prohibition, asserted here too.

Fixture roots: use `cqlite-core/tests/support/datasets_root.rs::sstables_root_for_table`
(per-TABLE resolution, #3220) — never a keyspace-level `is_dir()` and never a suite-wide
`assert!(ran > 0)`. Committed fixtures are `must_run`. Note `/data/datasets` here is 24 MB, so
an >= 8 MiB `Data.db` may not exist in the corpus: if so, build the large-file cases from a
temp file you create in the test (a sparse/zero-filled file large enough to cross the 8 MiB
threshold is enough to exercise the ADVICE plumbing, since the property under test is which
syscalls fire, not what the bytes decode to) — and if you do that, assert the reader really
took the mmap backend, or the test proves nothing.

Must stay green (run them):
- `cqlite-core/tests/issue_1143_mmap_prefetch_tail_guard.rs`
- `cargo bench --bench concurrent_scan` `scaling_floors` perf gate (constraint 3)

## Hard prohibitions
- **No page-cache-eviction claim anywhere.** `MADV_DONTNEED` on a file-backed mapping is an
  **RSS** control; the pages stay in page cache (`madvise(2)`). #2824's AC2 got this wrong —
  do not inherit its wording. Grep your own diff for "page cache" before you finish.
- No `MADV_SEQUENTIAL` for `Auto`, ever (#1143).
- No `unwrap()`/`expect()` in library code. `RUSTFLAGS="-D warnings"` clean.
- File-size campsite rule: new code goes in the NEW module, do not grow an over-threshold file.
- Do not touch `scan_admission.rs` semantics.

## Deliverable
Write your terminal verdict to `/data/lanes/lane-3853/.lane/slice1-verdict.md`:
what you wired (site by site), the `--lite` SUMMARY verdict, test names + results, every
DECLARED gap, every residual you knowingly left, and anything in the spec above you found to be
WRONG about the code (say so plainly — the spec was written from a read, not from a build).
