# Phase 2 — Adversarial verification of the Phase-1 IO packet (`phase1-3-linux-io.md`)

**Date:** 2026-07-21 · **Status:** research (uncommitted) · **Scope:** READ-ONLY (code + git verify)

The IO packet's one surviving code lever is the **fadvise family** (`SEQUENTIAL` + `WILLNEED` at
open, `DONTNEED` post-scan): S-cost, portable `cfg`-gate, sold as serving **B4 cold-start**. This
packet attacks that lever at file:line. **Verdict up front: the lever as written is DEAD CODE on the
field path and must be RESPEC'd to `madvise(WILLNEED)` under Auto-mmap. Its B4 justification is
largely stale. The LZ4/NVMe arithmetic is sound (if anything conservative).**

---

## Ruling 1 — fadvise vs madvise: **RESPEC (the lever as specified is dead code on the field path)**

The Phase-1 packet flags its own contradiction and then resolves it the wrong way. Verified chain:

**a) The field path runs `Auto`.** No override anywhere in the Flight server or CLI:
`rg disk_access_mode|DiskAccessMode|prefetch|CQLITE_USE_MMAP|CQLITE_DISK_ACCESS` over
`cqlite-flight/src` + `cqlite-cli/src` returns **nothing** (the only hit is an unrelated `prefetch`
comment in `streaming.rs`). The easy-db-lab kits set no disk-access/mmap/prefetch config either.
`disk_access_mode` and `prefetch` both default to `Auto` — confirmed by the round-trip test at
`cqlite-core/src/config.rs:997-998` (`assert_eq!(config.disk_access_mode, DiskAccessMode::Auto)`,
`PrefetchMode::Auto`).

**b) `Auto` resolves to `Mmap` for every field SSTable.** `resolve_disk_access_mode`
(`cqlite-core/src/storage/sstable/reader/mod.rs:245-282`): a file `< mmap_min_size_bytes` → Buffered;
`> memory_fraction × RAM` → Direct; else **Mmap**. The lower bound is
**`default_mmap_min_size_bytes()` = one page = 4096 bytes** (`config.rs:207-209`), and the Direct
threshold on a 32 GiB i4i.xlarge is **16 GiB**. So **any real Data.db (≥ 4 KiB, < 16 GiB) → Mmap.**
The buffered backend is essentially *never* selected on the field scan path.

> This falsifies the Phase-1 packet's own lever-table row (`phase1-3-linux-io.md:203`): *"keep
> buffered as the scan default (it already is — mmap is opt-in)."* **Backwards.** Under `Auto` the
> scan default **is mmap** for any file ≥ 4 KiB; buffered is the sub-page exception. This single
> factual error is the root of the mis-specified lever.

**c) Mmap reads never touch a file descriptor.** `BlockSource::Mapped(MmapCursor)`
(`reader/source.rs:51,91,98,265`) serves every read from the `Arc<Mmap>` memory slice via
`poll_read` on the cursor; the positional path uses `MmapReadAt` (a slice), not `read_at.rs`'s
`FileExt::read_at`. There are **zero `read()`/`pread()` syscalls** on the mmap scan path.

**d) `posix_fadvise` acts on a file-descriptor's page cache for `read()`/`pread()` — it has *no
effect* on mmap'd region access.** The mmap analogue is `madvise`. Therefore **the fadvise
SEQUENTIAL/WILLNEED/DONTNEED lever as specified would never fire on the field path** — it is only
reachable when a Buffered or Direct backend is explicitly selected, which the default `Auto` never
does for a field SSTable.

**The respec (precise):**

- **The correct S-lever for the field is `madvise(MADV_WILLNEED)` at open on the Auto-mmap scan
  mapping — not `posix_fadvise`.** The machinery is **already built and wired**:
  `mmap.advise(Advice::WillNeed)` at `reader/mod.rs:1052-1053`, driven by `PrefetchMode::WillNeed`
  in `mmap_advice_for` (`reader/mod.rs:316-328`). It is simply **not selected by `Auto`** —
  `mmap_advice_for` returns `None` for `Auto`/`Off`. The lever is a *policy flip* (teach `Auto` to
  issue `WILLNEED` at open, or add a distinct open-time WILLNEED pass), not new IO code. (`#2210`
  already proved the pattern: a dedicated `MADV_RANDOM` mapping for point reads at
  `reader/mod.rs:1167`.)
- **`posix_fadvise` is correct *only if* a buffered/direct backend is explicitly configured**
  (non-default; e.g. an operator sets `disk_access_mode=buffered` or a >16 GiB file forces Direct).
  Keep it as a `cfg`-gated hint **behind a backend check**, not on the default scan path. As the
  primary field lever it is dead code — **do not ship it as "the gap."**
- **`DONTNEED` post-scan** has the same split: mmap → `madvise(MADV_DONTNEED)`; buffered →
  `fadvise(DONTNEED)`. Spec it against the *actual* backend, not unconditionally as fadvise.

Net: the lever **survives, but respec'd** — "issue the already-built `madvise(WILLNEED)` under
Auto-mmap at open (+ `MADV_DONTNEED` post-scan-once); `posix_fadvise` only under an explicit
buffered/direct backend." The Phase-1 framing ("`posix_fadvise` is the one absent, additive lever")
is wrong for the default backend.

---

## Ruling 2 — the #1143 story: **MEASURED REGRESSION, and it constrains the respec**

`git show 5495dd649` (`perf(#1143): … drop MADV_SEQUENTIAL drop-behind on Auto prefetch`, PR #1347)
+ the doc comment at `reader/mod.rs:303-314`:

- **It was a measured regression, not unfinished work.** `MADV_SEQUENTIAL` couples aggressive
  read-ahead with **drop-behind** (pages evicted as the scan passes). Under **concurrent write
  load** the eviction meant overlapping scans re-faulted just-dropped pages as **synchronous major
  page faults on the tokio worker thread**, and the read-side **p99 tail regressed ~2×**. There is a
  dedicated 313-line guard: `cqlite-core/tests/issue_1143_mmap_prefetch_tail_guard.rs`.

- **Why this does NOT block the respec — the hazard is specific to `SEQUENTIAL`.** `MADV_WILLNEED`
  does **async read-ahead of the range with no drop-behind** — it never evicts. So issuing
  `WILLNEED` under `Auto` (the respec in Ruling 1) does **not** reintroduce the #1143 tail; only
  re-enabling `SEQUENTIAL` would. Any respec that reaches for `SEQUENTIAL` (or an
  eviction-on-advance `DONTNEED` *during* a scan) must re-answer the concurrent-write drop-behind
  question and re-run the #1143 tail guard. A one-shot `MADV_DONTNEED` issued *after* a genuine
  scan-once (not during) is the safe half, exactly as the Phase-1 packet's DONTNEED row argues —
  that reasoning holds, it just has to be spelled `madvise`, not `fadvise`.

---

## Ruling 3 — B4 cold-start justification: **WEAK / LARGELY STALE**

B4 (cold ≤ 3 s) is open-time latency — faulting Summary / Statistics / Index + the first data chunk.
The Phase-1 lever sells WILLNEED-at-open as turning "a cold-start's serial fault chain into one async
prefetch." That serial fault chain **has already been engineered away**:

- **`#2385/#2395` (commit `02984d3ab`)**: *"retire redundant SSTableIndex from BIG open — parse-once,
  kill O(N²) cold-start."*
- **`#2412` (commit `5a0ef023f`)**: *"lazy Summary-guided BIG index — O(summary) open."* Confirmed in
  `index_reader/lazy.rs` + `reader/component_loading.rs`: open now reads only the **small Summary.db**,
  not the full Index.db.
- **Field evidence (R11b, MEMORY):** cold-start field PASS with **"cold parses ZERO"**, 0 OOMKills at
  80 threads. Cold-start is already meeting the bar in the field.

So the "Summary/Index/first-chunk serial fault chain" the lever claims to shorten is already
**O(summary)-short and empirically not the bottleneck**. WILLNEED-at-open could still *marginally*
overlap the small summary + first-chunk faults with query setup, but it is **not the B4 saver the
Phase-1 packet frames it as** — that headline rationale is superseded by #2385 + #2412 and by the
field measurement. **Downgrade** the B4 line from "directly serves B4 ≤ 3 s" to "B4 already met in
field post-#2412; WILLNEED-at-open is a marginal, speculative cold-p99 hedge, not a B4 fix." This
removes the lever's main urgency claim; it stands only as a cheap page-cache hint, priced S but
low-value, and should sit **behind a re-measurement** the same way `io_uring` does.

---

## Ruling 4 — LZ4 ~1.5 GB/s/core + 8× NVMe headroom: **SOUND, mildly conservative**

- **`lz4_flex` safe-decode ~1.5 GB/s/core.** Reasonable-to-conservative. Reference C LZ4 decompresses
  ~4.5–5 GB/s/core; `lz4_flex` safe-decode (bounds-checked, the codebase's deliberate no-`unsafe`
  choice — `compression.rs:259`, `Cargo.toml` `safe-decode`) benches competitively but pays the
  checks, so ~1.5 GB/s is a fair pessimistic anchor. The doc's own halving to 0.75 GB/s only doubles
  the result to ~0.2–0.8 % CPU. No correction — the decompress-is-a-low-single-digit-% conclusion is
  robust, and the honest §2c caveat (relative share ~doubles to ~1–1.6 % once the CPU coordination
  cost is removed) is correct.
- **8× NVMe headroom arithmetic checks out and is conservative.** 600 k rows/s × (500 B ÷ 2.5) =
  120 MB/s vs an i4i.xlarge Nitro SSD's ≥ 1 GB/s sequential read → ~8×. The floor is understated:
  i4i.xlarge NVMe sequential read is typically **2–4+ GB/s**, so true headroom is ~16–30×. The
  conclusion — **cold IO bandwidth never binds in the 100 k–600 k rows/s envelope** — is if anything
  stronger than stated.
- **Minor:** the 2.5× text compression ratio is a fair mid-point; better field ratios (3–4×) only
  shrink disk bytes and widen headroom. No change needed. Row-width band (82/250/500 B) is a modeled
  guess (the packet admits `easy_cass_stress.keyvalue` width isn't pinned) but does not affect the
  binding-constraint conclusion.

---

## Bottom line for the program

1. **The IO packet's surviving lever is mis-targeted.** As written (`posix_fadvise` on "the buffered
   cold scan") it is **dead code** — the field runs `Auto` → **Mmap** (mmap_min_size = 4 KiB, no fd
   reads), and fadvise cannot touch mmap'd pages. **RESPEC to: flip `Auto` to issue the already-built
   `madvise(MADV_WILLNEED)` at open** (`reader/mod.rs:1052`, currently gated off by
   `PrefetchMode::Auto → None`); add `MADV_DONTNEED` post-scan-once; keep `posix_fadvise` only as a
   backend-gated hint for an explicitly-configured buffered/direct mode. This is a **policy flip on
   built machinery**, not new IO code, which further lowers its cost — but also its novelty.
2. **#1143 was a measured ~2× p99 regression** from `MADV_SEQUENTIAL` drop-behind under concurrent
   writes; **`WILLNEED` sidesteps it** (no drop-behind), so the respec is safe — but any reach for
   `SEQUENTIAL`/in-scan `DONTNEED` must re-run `issue_1143_mmap_prefetch_tail_guard.rs`.
3. **The B4 justification is largely stale** — #2385 + #2412 made open O(summary) and R11b field
   cold-start already passes ("cold parses zero"). Reprice the lever from "B4 saver" to "cheap,
   low-value cold-p99 hedge, defer behind a re-measurement."
4. **The LZ4/NVMe arithmetic is sound and conservative** — no corrections; IO bandwidth does not bind
   in-envelope, arguably by a wider margin than the packet claims.
