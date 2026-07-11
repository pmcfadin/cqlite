# flight-warm-handles Specification

## Purpose
TBD - created by archiving change flight-warm-handles. Update Purpose after archive.
## Requirements
### Requirement: Warm parsed state SHALL be keyed on generation identity, valid across snapshot dirs

The warm-handle cache SHALL key parsed SSTable state (Index/Summary/Statistics/bloom) and parsed
schema on the logical table plus the set of SSTable generations present, using an inode-stable
generation identity (device+inode of each `Data.db`, cross-checked with the parsed generation
number) so that the same underlying files reached through a different snapshot hardlink directory
resolve to the same cache entry. The cache key SHALL NOT be the resolved directory path, and SHALL
NOT be a TTL/time bucket.

#### Scenario: Same generations reached through two snapshot dirs are one warm entry

- **GIVEN** a table whose per-query snapshot mode hardlinks the same `Data.db` inodes into a fresh
  `snapshots/<name>/` directory per request
- **WHEN** two `do_get` requests for the same table resolve to two different snapshot directories
  over the identical inode set
- **THEN** both resolve to the same warm cache entry, the second request records a warm **hit**,
  and it performs **zero** SSTable reader-open and zero Index/Summary/Statistics/bloom parse.

#### Scenario: A path-keyed or TTL-keyed cache would miss here (regression guard)

- **GIVEN** the same two-snapshot-dir sequence over identical inodes
- **WHEN** the warm handle is queried
- **THEN** the cache entry is selected by generation identity, not by the (differing) directory
  paths — a path key would report a miss on the second request, and this scenario asserts a hit.

### Requirement: A per-request staleness probe SHALL decide warm-hit vs rebuild with zero staleness window

The service SHALL, on every request, probe the current SSTable generation set for the resolved table
(an authoritative directory listing / generation enumeration) and compare it to the warm handle's
cached set. When the set is unchanged the request SHALL take a warm hit and skip all reader-open and
Index/Summary/Statistics/bloom parse; when the set has changed the request SHALL rebuild. The probe
SHALL read ground truth (a listing/manifest), never infer freshness from filesystem timing,
mtime-only, or any non-authoritative heuristic (#28), so that a flush/compaction is visible on the
next request with no staleness window.

#### Scenario: Unchanged generation set is a warm hit with zero parse

- **GIVEN** a warm handle whose cached generation set matches the on-disk set
- **WHEN** a second identical `do_get` executes
- **THEN** the staleness probe reports "unchanged," the request serves from warm state, and a
  work-done probe shows zero reader-open and zero Index/Summary/Statistics/bloom parse for that
  request.

#### Scenario: A newly added generation is visible on the next request

- **GIVEN** a warm handle and a subsequent flush that adds a new SSTable generation on disk
- **WHEN** the next `do_get` executes
- **THEN** the staleness probe reports "changed," the rebuild adds exactly the new generation
  (unchanged generations keep their warm parsed state), and the result reflects the new data — with
  no staleness window and no wall-clock/TTL delay.

### Requirement: The snapshot manifest.json SHALL provide a fast-path probe without a readdir

In snapshot mode the service SHALL use the snapshot's `manifest.json` as a fast-path staleness
probe: when the manifest is byte-identical to the manifest cached for that generation set, the
service SHALL take the warm hit without performing a directory `read_dir`. The manifest fast path
SHALL be an optimization only — its result SHALL be equivalent to the authoritative
generation-set probe, never a weaker freshness guarantee.

#### Scenario: Matching manifest skips the directory listing

- **GIVEN** a snapshot-mode warm handle whose cached `manifest.json` matches the snapshot on disk
- **WHEN** a subsequent request for the same snapshot executes
- **THEN** the request takes a warm hit via the manifest fast path and performs no `read_dir` of the
  snapshot directory, returning byte-identical batches to the first request.

#### Scenario: Absent or unparsable manifest falls back to the authoritative probe

- **GIVEN** a snapshot with no `manifest.json` or an unreadable one
- **WHEN** a request executes
- **THEN** the service SHALL fall back to the authoritative generation-set listing probe (never a
  stale warm hit) and produce the correct answer.

### Requirement: Refresh SHALL be fail-closed — the old warm set stays intact on any error

The warm-handle rebuild SHALL be atomic and fail-closed, mirroring `Database::refresh()` (#1749):
every added generation SHALL be opened before the warm set is swapped, so if any added generation
fails to open (e.g. a corrupt `Statistics.db`, #1626) the rebuild SHALL return the typed error and
leave the previously warm set fully intact — no partial view. A probe error SHALL be treated as
"changed" (forcing a full re-resolve), never as a stale warm hit. In-flight requests SHALL hold
their own `Arc` reader clones and complete against the pre-swap set, unaffected by a concurrent
rebuild.

#### Scenario: A corrupt added generation leaves the warm set unchanged

- **GIVEN** a warm handle serving a valid generation set and a newly added generation whose
  `Statistics.db` is corrupt
- **WHEN** a request triggers a rebuild that tries to open the new generation
- **THEN** the rebuild returns the typed error, the previously warm set is still served intact on
  the next request, and no partial/half-applied generation set is ever exposed.

#### Scenario: An in-flight request is isolated from a concurrent rebuild

- **GIVEN** a request streaming from the warm set while a concurrent rebuild swaps in a new
  generation set
- **WHEN** both proceed
- **THEN** the in-flight request completes against its pre-swap `Arc` reader clones (same rows it
  would have returned), and the next request sees the post-rebuild set.

### Requirement: Warm state SHALL stay within the memory budget via LRU eviction

The warm-handle cache SHALL bound its footprint by an explicit byte budget inside the <128MB
discipline, accounting the parsed-state footprint (per-generation Index/Summary/Statistics/bloom plus
parsed schema) explicitly, and SHALL evict least-recently-used (table, generation) entries when the
budget would be exceeded. A generation removed on disk (dropped by a rebuild) SHALL be evicted
immediately regardless of LRU age.

#### Scenario: Exceeding the budget evicts the least-recently-used entry

- **GIVEN** a warm cache at its byte budget with N distinct (table, generation) entries
- **WHEN** a request warms a new (table, generation) that would exceed the budget
- **THEN** the least-recently-used entry is evicted (recorded as an **evict**), the accounted
  footprint stays within the budget, and an evicted entry re-parses correctly on its next request.

#### Scenario: A removed generation is evicted immediately

- **GIVEN** a warm generation that a rebuild finds no longer on disk
- **WHEN** the rebuild applies
- **THEN** that generation's warm state is dropped immediately (independent of LRU age) and its
  reader closes once the last in-flight `Arc` clone is dropped.

### Requirement: The warm handle SHALL expose hit/miss/evict/refresh-outcome metrics

The service SHALL emit bounded observability counters for warm-cache **hit**, **miss**, **evict**,
and **refresh-outcome** (unchanged / rebuilt-delta / fail-closed-retained), riding the existing
observability contract with no new configuration knob, environment variable, or ticket field. These
counters SHALL be sufficient for the #2289 harness and #1494 bench suite to prove warm behavior.

#### Scenario: A warm hit and a rebuild are distinguishable in metrics

- **GIVEN** observability enabled
- **WHEN** one request warms the cache (miss + build) and a second identical request hits it, then a
  flush forces a third request to rebuild
- **THEN** the counters show exactly one miss, one hit, and one refresh-outcome=rebuilt-delta, with
  no new knob introduced to surface them.

### Requirement: Cancellation discipline SHALL hold through the probe and rebuild paths

The staleness probe and any rebuild (reader opens) SHALL be cooperatively cancellable under the
#2264/#1473 discipline: a pre-cancelled request SHALL perform zero probe and zero rebuild work, and
`Error::Cancelled` SHALL be surfaced by variant (not by racing a flag). No cancellation guarantee
that holds on the cold path SHALL weaken on the warm path.

#### Scenario: A pre-cancelled request does zero warm-path work

- **GIVEN** a request whose cancel flag is already set
- **WHEN** it reaches the warm handle
- **THEN** it performs zero staleness-probe and zero rebuild work and surfaces the distinct
  `Cancelled` variant without masking it as another error.

#### Scenario: A disconnect mid-rebuild leaves the warm set intact

- **GIVEN** a rebuild in progress when the client disconnects (cancel flag set)
- **WHEN** cancellation is observed
- **THEN** the rebuild stops early, the previously warm set is left intact (fail-closed), and no
  partial generation set is exposed.

### Requirement: Bench evidence SHALL show ~zero parse cost on a repeated unchanged query

The change SHALL be validated with before/after bench evidence on the #2289 local harness (point
read, LIMIT, full scan on a ≥100k-partition table) and the #1494 bench suite: a second identical
query on an unchanged generation set SHALL show approximately zero parse cost (schema parse +
directory resolve + reader-open/Index/Summary/Statistics/bloom parse elided) relative to the cold
first query, with the warm-hit counter incrementing.

#### Scenario: Repeated point read on unchanged data pays no parse cost

- **GIVEN** the #2289 harness with a warm handle and unchanged generation set
- **WHEN** the same `WHERE pk = X` point read runs a second time
- **THEN** the measured per-request schema-parse + resolve + reader-open/index/summary/bloom parse
  cost is approximately zero versus the cold run, the warm-hit counter increments, and the returned
  rows are byte-identical to the cold run.

