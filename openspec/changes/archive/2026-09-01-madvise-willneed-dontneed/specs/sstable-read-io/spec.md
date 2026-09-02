# sstable-read-io — delta for madvise-willneed-dontneed (issue #2824)

**This delta adds NO behavioural requirement and changes no code path.** Issue #2824's lever was built,
measured and rejected (lead ruling on REQ-2824-03); `PrefetchMode::Auto` still issues no `madvise`. What
is recorded here is what the investigation established, so the next attempt starts from it rather than
rediscovering it.

**Acceptance-criterion → disposition map** (issue #2824):

| AC | Disposition |
|---|---|
| AC1 — cold-p99 improves on a cold i4i scan, no warm regression, no #1143 reintroduction | **NOT MET, and re-scoped.** The flip is a confirmed regression vector at `Database::open`; the benefit is undemonstrable on EBS. Recorded by ADDED *`PrefetchMode::Auto` does not advise the scan mapping at open* and ADDED *A read-ahead measurement must record the device it measured* |
| AC2 — `MADV_DONTNEED` post-scan-once, B4 peak hygiene | **NOT MET, and its rationale is false as written.** Recorded by ADDED *`MADV_DONTNEED` on a file-backed mapping is an RSS control, never a page-cache control* |

## ADDED Requirements

### Requirement: `PrefetchMode::Auto` does not advise the scan mapping at open

`mmap_advice_for` SHALL continue to return no advice for `PrefetchMode::Auto`, and no advice SHALL be
issued on the scan mapping at reader-open time under the default configuration.

This is **not** a restatement of #1143. #1143 forbids `MADV_SEQUENTIAL` for its drop-behind. This
requirement forbids *any* whole-mapping advice **at open**, for a different and independent reason:
`SSTableManager::new` opens **every** SSTable under the data directory
(`storage/sstable/manager_open.rs:61` -> `:300`, depth 3) on the `Database::open` path, so open-time
advice is issued for every table of every keyspace before a single query is seen, and a point-lookup-only
workload pays it in full. Reader-open is not evidence of intent to scan.

The constraint is scoped and has an exit: it holds **until** a scan-lifetime seam exists that can issue
advice when a scan actually begins. That plumbing is filed as **issue #3853**. An explicit
`PrefetchMode::WillNeed` or `Sequential` remains a caller opt-in and is unaffected — the requirement is
about the default.

#### Scenario: The default configuration issues no advice at open
- **GIVEN** a reader opened under `PrefetchMode::Auto` with the mmap backend resolved
- **WHEN** the scan mapping is built
- **THEN** no `madvise` is issued on it

#### Scenario: An explicit opt-in is still honoured
- **GIVEN** `PrefetchMode::WillNeed` or `PrefetchMode::Sequential`, set deliberately by a caller
- **THEN** the corresponding advice is issued

### Requirement: `MADV_DONTNEED` on a file-backed mapping is an RSS control, never a page-cache control

No requirement, source comment, log line, artifact, issue body or PR text in this repository SHALL claim
that `MADV_DONTNEED` evicts page cache, drops the page cache, or prevents a scan from "leaving the page
cache warm".

Per `madvise(2)`, after `MADV_DONTNEED` on a file-backed mapping subsequent accesses repopulate "from the
up-to-date contents of the underlying mapped file". It frees the process's resident PTEs and therefore
RSS; the pages remain in page cache. Page-cache eviction is `posix_fadvise(POSIX_FADV_DONTNEED)`.

The sanctioned wording is that releasing a scan mapping **bounds peak RSS**. This mirrors the
`claim.blocked.compressed_sstable_writes` boundary (#1406): state the claim at the size the mechanism
supports. Issue #2824's own AC2 states it larger, which is why this is written down rather than left to
be re-derived.

#### Scenario: A successor change inherits the boundary, not the wording
- **GIVEN** a change that adds a post-scan `MADV_DONTNEED`
- **THEN** it describes the effect as releasing resident pages / bounding RSS, never as evicting page cache

### Requirement: A read-ahead measurement must record the device it measured

Any A/B recorded in this repository that claims a read-ahead or prefetch effect SHALL record the storage
device backing the corpus, its model, and its `read_ahead_kb`, and SHALL fail closed rather than record
an unmeasured value for them.

A read-ahead result is uninterpretable without the device: on the EBS volume used for #2824
(132 MB/s measured, 128 KiB window) the default window already saturates the device, so the measurement
had no headroom to show an effect in either direction and a null result carried no information. Reported
without the device, that same null would have read as "the lever does not work".

The harness SHALL also fail closed when it cannot drop the page cache, since a warm run labelled cold is
indistinguishable in the output from a real one.

#### Scenario: An unmeasurable device is declared, not defaulted
- **GIVEN** a host where the corpus device model or read-ahead window cannot be read
- **THEN** the artifact says so explicitly and marks the result unattributed

#### Scenario: A host that cannot drop the page cache refuses to measure
- **GIVEN** a host on which dropping the page cache is not permitted
- **THEN** the harness exits non-zero without producing a measurement
