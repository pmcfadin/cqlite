# sstable-read-io — delta for madvise-willneed-dontneed (issue #2824)

**Architecture note (read this first).** An mmap-backed `SSTableReader` holds up to **two** mappings of
one `Data.db`, and which one a given read goes through is the whole subject of this delta:

| mapping | advised | who reads through it |
|---|---|---|
| the **scan** mapping (`ScanSource::Mapped`) | `mmap_advice_for(prefetch)` at open (`reader/mod.rs:832`) | `scan_positional_source` (the Summary-guided walk + windowed scan feed, #2876) **and** `BlockSource::Mapped` |
| the **point** mapping (`point_read_mmap`) | `MADV_RANDOM`, files >= 8 MiB only (#2210) | point lookups on files >= 8 MiB |

Below 8 MiB there is no second mapping and the point path shares the scan one (#2210's measured
decision). So for any file large enough for read-ahead policy to matter, **the advised mapping is the
scan plane and only the scan plane**.

`mmap_advice_for` (`backend_resolve.rs:184`) currently maps `PrefetchMode::Auto -> None`, so the default
configuration issues no `madvise` on that plane at all. This delta flips that one arm and adds a
post-scan-once counterpart.

**Acceptance-criterion → requirement map** (issue #2824):

| AC | Requirement(s) | Slice |
|---|---|---|
| AC1 — cold-p99 improves, no warm regression, no #1143 reintroduction | ADDED *`PrefetchMode::Auto` issues `MADV_WILLNEED` on the scan mapping*, ADDED *`Auto` never yields `MADV_SEQUENTIAL`*, ADDED *The policy flip is recorded against a cold-vs-warm measurement*; the **i4i magnitude** is an explicit residual, see the proposal | 1 |
| AC2 — `MADV_DONTNEED` post-scan-once, B4 peak hygiene | — see ADDED *Slice 1 ships no `MADV_DONTNEED` and says so*; the claim boundary that governs it is ADDED *The post-scan release claims RSS hygiene and never page-cache eviction* | **2** |

**This delta is SLICE 1 of 2.** It ships AC1 — the `WILLNEED` policy flip — and deliberately ships no
`MADV_DONTNEED`. The reason is recorded in the proposal and raised on the issue thread as REQ-2824-02:
AC2 has no single post-scan seam (nine independent scan entry points in three shapes), requires new
reader-scoped in-flight state because concurrent scans on one reader are a supported and tested property
(#815), requires `unsafe` because memmap2 puts `DontNeed` in `UncheckedAdvice`, and — implemented
unconditionally — is a plausible source of exactly the warm regression AC1 forbids. Slice 2 is filed
separately.

## ADDED Requirements

### Requirement: `PrefetchMode::Auto` issues `MADV_WILLNEED` on the scan mapping

`mmap_advice_for` SHALL return `MADV_WILLNEED` for `PrefetchMode::Auto`, so that the default
configuration advises the scan mapping for asynchronous read-ahead at open. `PrefetchMode::Off` SHALL
continue to return no advice, and `Sequential`/`WillNeed` SHALL continue to return their named advices.

The advice SHALL be issued on the mapping held by `ScanSource::Mapped` — the same mapping
`scan_positional_source` reuses (#2876) — and SHALL NOT be issued on the dedicated `MADV_RANDOM` point
mapping built by `point_read_mmap` (#2210).

A failed `madvise` SHALL remain non-fatal and SHALL be logged, matching the established posture at the
existing advise sites: opening an SSTable never fails because the kernel declined an advisory hint.

#### Scenario: The default configuration advises the scan plane
- **GIVEN** a reader opened with `PrefetchMode::Auto` on a Unix host with the mmap backend resolved
- **WHEN** the scan mapping is built
- **THEN** `MADV_WILLNEED` is issued on that mapping

#### Scenario: Prefetch off still issues nothing
- **GIVEN** `PrefetchMode::Off`
- **THEN** `mmap_advice_for` returns no advice and no `madvise` is issued on the scan mapping

#### Scenario: A refused advise does not fail the open
- **GIVEN** a mapping on which `madvise` returns an error
- **WHEN** the reader is opened under `PrefetchMode::Auto`
- **THEN** the open succeeds and the failure is logged

### Requirement: `Auto` never yields `MADV_SEQUENTIAL`

`mmap_advice_for` SHALL NOT return `MADV_SEQUENTIAL` for `PrefetchMode::Auto` under any configuration.

This is the durable form of the issue #1143 protection. #1143 was a ~2x p99 tail regression whose
mechanism is `MADV_SEQUENTIAL`'s **drop-behind**: pages behind the read cursor are aggressively evicted,
which evicts hot pages under concurrent write load. `MADV_WILLNEED` queues asynchronous read-ahead and
has no drop-behind semantics, so it does not carry that mechanism.

The pre-existing unit assert `mmap_advice_for(PrefetchMode::Auto) == None` states the *implementation of
the day* rather than the invariant, and SHALL be **retargeted** to this requirement — never deleted. The
`issue_1143_mmap_prefetch_tail_guard.rs` integration guard SHALL continue to run unchanged; note its
latency comparison is observational only, by its own header, so the unit assert is the load-bearing pin.

#### Scenario: The #1143 invariant holds under every prefetch mode
- **GIVEN** any `PrefetchMode` value
- **WHEN** it is `Auto`
- **THEN** `mmap_advice_for` does not return `MADV_SEQUENTIAL`

#### Scenario: An explicit opt-in to Sequential is still honoured
- **GIVEN** `PrefetchMode::Sequential`, which is an explicit caller opt-in and not a default
- **THEN** `mmap_advice_for` returns `MADV_SEQUENTIAL`

### Requirement: Slice 1 ships no `MADV_DONTNEED` and says so

This change SHALL NOT issue `MADV_DONTNEED` on any mapping, and SHALL NOT claim to satisfy AC2.

The reader SHALL be left with no partial or dormant scan-lifetime accounting introduced for it: a
half-built refcount that no seam decrements is worse than none, because it reads as coverage. Slice 2
adds the accounting and the release together, with `benches/concurrent_scan.rs`'s `scaling_floors` perf
gate re-run as part of it.

#### Scenario: No `MADV_DONTNEED` is issued on any plane
- **GIVEN** a reader opened under any `PrefetchMode`
- **WHEN** any scan over it completes
- **THEN** no `MADV_DONTNEED` is issued

### Requirement: The post-scan release claims RSS hygiene and never page-cache eviction

No requirement, source comment, log line, artifact, or PR text produced by this change — **or by slice
2** — SHALL claim that `MADV_DONTNEED` evicts page cache, drops the page cache, or prevents a scan from
"leaving the page cache warm". The requirement is stated here, in slice 1, because slice 1 is where the
finding was made and where the issue's AC2 wording would otherwise be inherited unexamined.

Per `madvise(2)`, after `MADV_DONTNEED` on a **file-backed** mapping subsequent accesses repopulate "from
the up-to-date contents of the underlying mapped file"; the call frees the process's resident PTEs and
therefore **RSS**, and the pages remain in page cache. Page-cache eviction is `posix_fadvise`
(`POSIX_FADV_DONTNEED`), which issue #2824 explicitly scopes to the buffered and direct backends only.

The sanctioned wording is that a completed scan-once **bounds peak RSS by releasing the scan mapping's
resident pages**. This mirrors the `claim.blocked.compressed_sstable_writes` boundary (#1406): the
mechanism ships, and the claim is stated at the size the mechanism actually supports.

#### Scenario: No page-cache-eviction claim is made
- **GIVEN** the source, comments, and artifacts introduced by this change
- **WHEN** they describe the post-scan `MADV_DONTNEED`
- **THEN** they describe it as releasing resident pages / bounding RSS, and never as evicting page cache

### Requirement: `posix_fadvise` stays off the mmap path

This change SHALL NOT introduce any `posix_fadvise` call on the mmap backend. The fadvise lever SHALL
remain reachable only behind an explicitly-resolved buffered or direct backend, as issue #2824 scopes it.

#### Scenario: The mmap backend issues no fadvise
- **GIVEN** a reader whose resolved backend is mmap
- **THEN** no `posix_fadvise` is issued by the read path

### Requirement: The policy flip is recorded against a cold-vs-warm measurement

The change SHALL ship a recorded cold-vs-warm A/B of the baseline against the patched binary over a
fixed corpus, with the page cache dropped before each cold arm, and SHALL record the **host** the
measurement ran on.

The measurement harness SHALL **fail closed** when it cannot drop the page cache, rather than running a
warm pass labelled cold: a cold arm that silently ran warm is indistinguishable in the output from a real
one, which is the false-certification shape this repository refuses elsewhere.

Where the measuring host is not an i4i, the recorded artifact and the PR SHALL state that AC1's i4i
magnitude is **UNMEASURED** and name it as a residual. A non-i4i result SHALL NOT be reported as
satisfying AC1's i4i clause.

#### Scenario: A host that cannot drop the page cache refuses to measure
- **GIVEN** a host on which dropping the page cache is not permitted
- **WHEN** the harness is run
- **THEN** it exits non-zero without producing a measurement

#### Scenario: A non-i4i measurement names its own residual
- **GIVEN** a measurement recorded on a host that is not an i4i
- **THEN** the artifact records the instance type and states that the i4i magnitude is unmeasured
