# Proposal: `madvise(WILLNEED)` under Auto-mmap + `MADV_DONTNEED` post-scan-once (issue #2824)

**Milestone:** 0.17 scan-path throughput program (epic #2817, manifest item M10) · **Priority:** P2 ·
**Routing:** **design-driven** — this is a *policy* change on already-built machinery, and the deliverable
is a contract plus a recorded measurement, not a parse fix with an external oracle. The one place an
oracle exists (does the advice reach the plane the hot scan reads through?) is used below, and it
**falsified the issue's own framing once already** — see "The RE-SPEC, and why it no longer applies".
· **Issue:** #2824 · **Refs:** #1143 (the `MADV_SEQUENTIAL` prohibition), #2210 (the `MADV_RANDOM`
point plane), #2876 (the scan positional plane), #2412, #2605/PR #3446, #2818 (M0)

## Why

Cold page-in is measured at **60.17 us/row — 3x all producer CPU combined, and 98% of wall-time
variance** (#2605 / PR #3446). The 2026-08-30 adjudication funded this lever on that number: it is the
only funded lever touching the largest wall-clock term.

The machinery is already built and simply gated off. `mmap_advice_for`
(`cqlite-core/src/storage/sstable/reader/backend_resolve.rs:184`) maps `PrefetchMode::Auto -> None`, so
the default configuration issues **no** `madvise` at all on the scan mapping. Turning it on is a
one-arm change to a `match`; the work in this change is establishing that the flip is *safe* and
*reaches the right plane*, and measuring it.

## The RE-SPEC, and why it no longer applies

The owner's RE-SPEC on #2824 held that the flip "advises a mapping the hot scan path barely touches" —
that the field's scan read partition bodies through the dedicated `MADV_RANDOM` point mapping, not
through the scan mmap. **That was true when written and is no longer true.**

**#2876 landed.** Post-#2876, `scan_positional_source`
(`cqlite-core/src/storage/sstable/reader/mod.rs:~343`) reuses *the same* never-`MADV_RANDOM` `Arc<Mmap>`
that `ScanSource::Mapped` holds — precisely the mapping `build_block_sources` advises
(`reader/mod.rs:832`). The Summary-guided walk and the windowed scan feed read through that plane. So
the advice now lands on the hot scan plane by construction, and #2876's own source comment says a claim
scoped to `BlockSource` "describes nothing that a real scan does" — the inverse of which is that a claim
scoped to *this* mapping describes exactly what a scan does.

The RE-SPEC's second condition is also discharged: it asked to "re-measure after #2876 lands and M0
(#2818) re-measures". Both are closed, and the 2026-08-30 adjudication (FUNDED, P3 -> P2, sealed, board
Ready) postdates the RE-SPEC.

Its two standing prohibitions are **carried unchanged**: no `MADV_SEQUENTIAL`, and
`issue_1143_mmap_prefetch_tail_guard.rs` stays green.

## #1143 is not reintroduced, and the guard is retargeted rather than deleted

#1143 was a ~2x p99 tail regression caused by `MADV_SEQUENTIAL`, whose harm is **drop-behind**: the
kernel aggressively evicts pages behind the read cursor, evicting hot pages under concurrent write load.
`MADV_WILLNEED` has **no drop-behind semantics** — it queues asynchronous read-ahead and nothing else.
The two advices are not interchangeable and the #1143 mechanism does not transfer.

The durable #1143 pin is the **unit** assert `mmap_advice_for(PrefetchMode::Auto) == None`
(`cqlite-core/src/storage/sstable/reader/tests.rs:626`); the integration test's latency comparison is
**observational only, by its own header** (`issue_1143_mmap_prefetch_tail_guard.rs:18-29` — it logs
p50/p99 ratios and asserts nothing on timing, because a co-scheduled pause makes a ratio-vs-ratio assert
flake). That unit assert states the *implementation of the day*, not the invariant. The invariant #1143
actually needs is **`Auto` never yields `Sequential`**. This change **retargets** the assert to that
invariant — it is never deleted, and the integration guard runs unchanged.

## `MADV_DONTNEED` does not evict page cache — the claim is bounded, not the mechanism

AC2 of the issue reads: "`MADV_DONTNEED` is issued post-scan-once **so a full-ring scan does not leave
the page cache warm** past its usefulness (B4 peak hygiene)."

The mechanism does not deliver that property. Per `madvise(2)`, after `MADV_DONTNEED` on a **file-backed**
mapping, subsequent accesses "result in **repopulating the memory contents from the up-to-date contents of
the underlying mapped file** (for shared file mappings …)". It zaps the process's PTEs and frees **RSS**;
the pages remain in page cache. The call that evicts page cache is `posix_fadvise(POSIX_FADV_DONTNEED)`,
which this issue explicitly scopes to the buffered/direct backends only.

This change therefore **implements the mechanism exactly as specified and states the claim accurately**:
`MADV_DONTNEED` post-scan-once bounds **peak RSS** by dropping the scan mapping's resident PTEs. No
requirement, comment, or PR text in this change may claim page-cache eviction on the mmap plane. If
page-cache eviction on the mmap plane is the property wanted, it is a different lever and is filed
separately rather than smuggled in here.

Raised on the issue thread as REQ-2824-01 item (2), with this as the stated default.

## What this change does NOT do

- **No `MADV_SEQUENTIAL`**, ever, on any plane (#1143).
- **No `posix_fadvise` on the mmap path.** The fadvise lever stays behind the explicit buffered/direct
  backends, exactly as the issue scopes it.
- **No new size threshold invented.** #2210's `POINT_MMAP_MADV_RANDOM_MIN_BYTES` is measurement-derived;
  this change adds no unmeasured heuristic to sit beside it (no-heuristics mandate, #28).
- **No i4i measurement.** See below.

## Measurement, and the residual this change ships with

AC1 asks for cold-p99 on a **cold i4i scan**. This lane runs on a **c7i.4xlarge** with local NVMe and no
route to an i4i rig, so that number is **not obtainable here**. What is delivered instead:

- a cold-vs-warm A/B on this box's local NVMe over the committed `ws0.events` performance fixture
  (`tools/ws0-corpus-gen`, ~2.8 GB), `drop_caches` between arms, baseline vs. patched;
- the #1143 integration guard green;
- the warm arm checked for regression.

The **i4i magnitude is an explicit residual**, recorded in the PR and filed as a follow-up for the rig
lane. Raised on the issue thread as REQ-2824-01 item (1), with this as the stated default.

The fixture is **CQLite-written and CQLite-read** and is a **performance fixture only, never a
correctness oracle** (#3042) — it holds the bytes constant across two measurement arms in one session
and nothing more. It is also **uncompressed** (#1406), which is noted where the result is reported.
