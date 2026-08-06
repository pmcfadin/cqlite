# partition-access-distribution — delta for keyed-access-distribution-probe (issue #2827)

**Read this first.** This change delivers **an instrument and a decision procedure**. It does **NOT**
deliver a measured field skew number and it does **NOT** decide the 64–128 MiB decoded-partition cache
go/no-go. The reason is not a shortcut: **no field keyed workload with captured concentration exists**
— the only one on record is ~0.9 qps with no reported hot-set concentration
(`docs/research/phase2-verify-caching.md:214-216`), and there is no Zipf/skew measurement of the field
keyed workload anywhere in `docs/` (`:212-213`). Nothing produced by this change may be cited as
"measured field skew".

**Why the originally-filed method is not implemented.** `cqlite.read.partition_lookup.total` carries
only bounded attributes (`docs/observability/configuration.md:215`;
`cqlite-core/src/observability/catalog.rs:283`), and the catalog forbids the alternative outright:
"Unbounded values (raw error messages, **partition keys**, full query text) are **NEVER** attached as
attributes or span fields" (`docs/observability/configuration.md:304-305`). Per-key skew is not
reconstructable from those counters. **Why the obvious replacement is not implemented either:** a
synthetic-Zipf sensitivity curve is a circular oracle — its answer is a function of the distribution
we chose — and the owner rejected it on the thread. See `design.md` D8 for the governing rule.

**Owner instruction, recorded and NOT executed.** The owner's standing instruction — *"the issue must
stop calling itself a gate"*, with a request to retitle/re-scope the deliverable as an **input** rather
than the go/no-go — is recorded here so no artifact re-inherits the "gate" framing. Retitling or
re-scoping a GitHub issue is an **owner action**; the worker does not take it unilaterally.

**Acceptance-criterion → requirement map** (issue #2827's three ACs):

| AC | Verdict | Requirement(s) |
|----|---------|----------------|
| 1 — reports the **hot-set concentration (skew/Zipf)** for the field keyed workload at A2-scale qps | **PARTIAL — the instrument that reports it is delivered; the field number is NOT.** The concentration shape is delivered as a bounded bucket histogram (the owner's replacement for a Zipf parameter, `design.md` D3); it reports whatever workload runs with the probe enabled. No field workload exists to run it against, so no field number ships. NOT waived. | ADDED *A bounded partition repeat-access histogram reports the access-concentration shape without per-key attributes*; *Repeat counting uses fixed memory independent of partition count*; *The measurement window is tumbling, closes deterministically, and emits exactly once*; *The instrument is wired into the logical point-read boundary and is zero-cost when disabled*; *The instrument's recovery of a known input distribution is verified* |
| 2 — **decides** whether a 64–128 MiB decoded-partition cache clears a useful hit ratio (the go/no-go for the K-A build) | **NOT SATISFIED by this change.** Not waived, not deferred to another issue: **satisfiable on the first real keyed workload run with the probe enabled**, because the procedure that turns a closed window into the verdict ships here. The blocker is the absence of a field workload, not the absence of analysis. | ADDED *Distinct-partition working-set bytes are MEASURED, and an unknown extent fails closed*; *A committed decision procedure converts a closed window into a go/no-go, and refuses when it cannot* — these deliver the **procedure**, not the verdict |
| 3 — standalone, **decoupled from #2037** (the measurement proceeds without the owner-gated cache build) | **SATISFIED.** No cache is built, sized, wired or benchmarked; nothing in this change references or depends on #2037's surface. | All requirements below (the property is negative — evidenced by the absence of a cache dependency, pinned by the scope scenario in *The change records what it does not deliver*) |

## ADDED Requirements

### Requirement: A bounded partition repeat-access histogram reports the access-concentration shape without per-key attributes

The read path SHALL expose an instrument that, over a measurement window, records how many times each
distinct partition was accessed and reports the resulting distribution as a **fixed-cardinality bucket
histogram** over exactly the buckets `1`, `2`, `3-4`, `5-8`, `9-16`, `17+`.

The instrument SHALL emit the following catalog metrics, and no others under this namespace:

| Metric | Instrument | Unit | Bounded attributes |
|---|---|---|---|
| `cqlite.read.partition_access.distinct_partitions` | counter | `{partition}` | `cqlite.read.repeat_bucket`, `cqlite.read.size_source` |
| `cqlite.read.partition_access.accesses` | counter | `1` | `cqlite.read.repeat_bucket` |
| `cqlite.read.partition_access.bytes` | counter | `By` | `cqlite.read.repeat_bucket` |
| `cqlite.read.partition_access.sample_denominator` | gauge | `1` | (none) |
| `cqlite.read.partition_access.dropped_accesses` | counter | `1` | (none) |
| `cqlite.read.partition_access.window_dropped_accesses` | gauge | `1` | (none) |
| `cqlite.read.partition_access.sampling_floor` | gauge | `1` | (none) |

`cqlite.read.repeat_bucket` SHALL be a closed set of exactly the six bucket labels above.
`cqlite.read.size_source` SHALL be a closed set of exactly `index`, `successor_gap` and
`unavailable` (amended 2026-08-06 by owner ruling; see `design.md` D6 and rider R1). A consumer SHALL
be able to distinguish an extent MEASURED as the partition's successor gap from one an index reported
directly, and both from a genuinely unknown one — so the provenance is its own label, never folded
into `index`.

No partition key, key hash, key prefix, key length, token, or any other per-key-derived value SHALL be
attached as an attribute or span field on any of these metrics, in any build configuration. This is
the binding constraint from `docs/observability/configuration.md:304-305` and
`cqlite-core/src/observability/catalog.rs:19-24`, and it is the reason the histogram exists in this
form rather than as a per-key series.

The existing `cqlite.read.partition_lookup.total` SHALL NOT gain, lose or rename an attribute.

**Evidence:** `cqlite-core/tests/issue_2827_partition_access_histogram.rs` (bucket boundary cases,
closed attribute sets, and an assertion that every emitted attribute key is one of the two declared
keys); `cqlite-flight/tests/issue_2827_partition_access_e2e.rs` (emitted-series readback through the
`observability-testing` capture harness).

#### Scenario: Access counts land in the correct bucket at every boundary
- **GIVEN** the probe is enabled and a window is open
- **WHEN** eight distinct partitions are accessed exactly 1, 2, 3, 4, 8, 9, 16 and 17 times respectively and the window is closed
- **THEN** `cqlite.read.partition_access.distinct_partitions` reports exactly one partition in each of `1`, `2`, `5-8` and `17+`, and exactly two partitions in each of `3-4` and `9-16` — counts `3` and `4` fall in `3-4`, count `8` alone falls in `5-8`, and counts `9` and `16` both fall in `9-16` (corrected 2026-08-06: the original text transposed the `5-8` and `9-16` figures, contradicting its own accesses arithmetic below)
- **AND** `cqlite.read.partition_access.accesses` for each bucket equals the sum of the access counts of the partitions in it (so `1` reports 1, `2` reports 2, `3-4` reports 3+4=7, `5-8` reports 8, `9-16` reports 9+16=25 and `17+` reports 17)

#### Scenario: No emitted attribute is derived from a partition key
- **GIVEN** a closed window over accesses to partitions with distinct, long, high-entropy keys
- **WHEN** the emitted series are read back through the observability capture harness
- **THEN** the only attribute keys present on any `cqlite.read.partition_access.*` series are `cqlite.read.repeat_bucket` and `cqlite.read.size_source`
- **AND** every observed `cqlite.read.repeat_bucket` value is one of the six declared labels and every `cqlite.read.size_source` value is `index`, `successor_gap` or `unavailable`
- **AND** the total number of distinct series across the seven metrics does not exceed 34 (`6 x 3 + 6 + 6 + 1 + 1 + 1 + 1`), regardless of how many distinct partitions were accessed

### Requirement: Repeat counting uses fixed memory independent of partition count

The counting structure SHALL occupy a **fixed** number of bytes that does not grow with the number of
distinct partitions accessed, the access rate, or the window length. The bound SHALL be stated in the
code and SHALL be at most 4 MiB. Nothing SHALL be allocated for the instrument while it is disabled.

When the number of distinct partitions in a window would exceed the structure's capacity, the
recorder SHALL reduce the admitted key space by **hash-prefix downsampling** — an admission predicate
that is a function of the key hash alone and therefore statistically independent of a key's access
frequency — and SHALL NOT evict entries by recency, by frequency, or by arrival order. The admitted
sample's per-key counts SHALL remain exact across a downsample (a key admitted at prefix width `k` is
admitted at every `k' < k`, so a survivor never loses counts recorded before the pass).

The scale in force at window close SHALL be published as
`cqlite.read.partition_access.sample_denominator`, so a consumer can distinguish a census
(`denominator = 1`) from a sample.

Accesses the recorder could NOT seat in the table SHALL be counted and published, and whether the
window reached the prefix cap SHALL be published, so a consumer reading the emitted series ALONE can
tell a lossy or floored window from a clean one.

Satisfying that clause requires **per-window reset semantics**, so the loss signals SHALL be:

| Series | Instrument | Semantics |
|---|---|---|
| `cqlite.read.partition_access.dropped_accesses` | counter | CUMULATIVE — "has this process ever lost input". Alertable, but it cannot answer "was THIS window clean": once it increments it reads non-zero for the life of the process. |
| `cqlite.read.partition_access.window_dropped_accesses` | gauge | Accesses the LAST CLOSED window could not seat. Reset every window. |
| `cqlite.read.partition_access.sampling_floor` | gauge | `1` when the last closed window reached the prefix cap, else `0`. Reset every window. |

Both GAUGES SHALL be emitted on **every** closed window, including when their value is zero, so an
absent series is never ambiguous between "clean" and "not emitted". A window SHALL be reportable as
clean exactly when both gauges read `0`. A probe-cluster failure
to seat a key SHALL widen the sample rather than drop the key: only keys not already in the table
can fail to seat, so dropping them would suppress the singleton bucket and OVERSTATE concentration.

The prefix width SHALL be capped, and a window that reaches the cap SHALL be marked non-census so the
decision procedure refuses it (see *A committed decision procedure…*).

A window entry SHALL be identified by `(keyspace, table, raw partition-key bytes)`.
One recorder serves every table, so a key-only identity would merge the same key in
two tables into a single entry — reporting two singletons as one repeat and pricing
it at the larger of the two extents, both of which overstate the case for a cache.

**Rationale that makes this a correctness requirement, not a resource one:** recency- or
arrival-ordered eviction retains high-frequency keys preferentially, which under-counts the singleton
bucket and **overstates** concentration — the direction that makes a cache look better than it is. See
`design.md` D4.

**Evidence:** `cqlite-core/tests/issue_2827_partition_access_histogram.rs` (fixed-footprint assertion;
downsample correctness; unbiased-fraction recovery under forced downsampling).

#### Scenario: Memory does not grow with the number of distinct partitions
- **GIVEN** the probe is enabled
- **WHEN** one window records accesses to 1,000 distinct partitions and another records accesses to 500,000 distinct partitions
- **THEN** the recorder's reported footprint is identical in both cases and equals the declared fixed bound
- **AND** with the probe disabled the recorder reports a footprint of zero and has allocated no table

#### Scenario: Downsampling preserves the bucket fractions and declares its scale
- **GIVEN** a controlled access sequence over enough distinct partitions to force at least one downsample, with a known bucket distribution
- **WHEN** the window is closed
- **THEN** `cqlite.read.partition_access.sample_denominator` reports a value greater than 1
- **AND** the per-bucket share of `distinct_partitions` matches the known distribution's shares within a stated tolerance
- **AND** no admitted partition's recorded access count is lower than the number of accesses it actually received

#### Scenario: A clean window is distinguishable from a lossy one by the emitted series alone
- **GIVEN** a closed window in which every access was seated and the prefix cap was not reached
- **WHEN** the emitted series are read back through the observability capture harness
- **THEN** `cqlite.read.partition_access.window_dropped_accesses` and `cqlite.read.partition_access.sampling_floor` are both PRESENT and both read `0`
- **AND** for a window that did lose accesses, the per-window gauge, the cumulative `dropped_accesses` counter and `sampling_floor` all report the loss
- **AND** the clean verdict does not depend on the cumulative counter, which stays non-zero for the life of the process once it has incremented

#### Scenario: A window that exhausts the sampling floor is marked non-census
- **GIVEN** a window driven past the prefix-width cap
- **WHEN** the window closes
- **THEN** the window is reported as non-census
- **AND** the decision procedure applied to it returns a refusal, not a hit-ratio number

### Requirement: The measurement window is tumbling, closes deterministically, and emits exactly once

The window SHALL be **tumbling**: on close the recorder buckets every live entry, emits each of the
series once, and resets the structure to empty with the sampling scale reset.

The window SHALL close on the first of: a configured wall-clock duration, a configured recorded-access
count, or an explicit programmatic `close_window()` call. `close_window()` SHALL be part of the public
surface **so that tests can close a window without depending on elapsed time** — no correctness test
in this change may assert on wall-clock (CLAUDE.md's mechanized wall-clock-race lint, `roborev-lints` /
#2642).

A window in which zero accesses were recorded SHALL emit nothing. A positive verdict requires an
affirmative measurement; a `0/0` emission is a series with no subject.

The tumbling window's known bias SHALL be recorded in the decision procedure: a partition accessed on
both sides of a boundary is split into two lower-repeat entries, so the histogram **understates**
concentration. The direction is conservative for a go/no-go and is accepted for that reason, not
argued away.

**Evidence:** `cqlite-core/tests/issue_2827_partition_access_histogram.rs` (emit-exactly-once,
reset-on-close, empty-window-silence).

#### Scenario: Closing a window emits once and resets
- **GIVEN** an open window with recorded accesses
- **WHEN** `close_window()` is called
- **THEN** each of the emitted series is emitted exactly once for that window
- **AND** a subsequent `close_window()` with no intervening accesses emits nothing
- **AND** the next window's histogram contains no partition from the previous window unless it was accessed again

#### Scenario: An empty window is silent
- **GIVEN** the probe is enabled and no partition access has been recorded
- **WHEN** `close_window()` is called
- **THEN** no `cqlite.read.partition_access.*` series is emitted at all

### Requirement: Distinct-partition working-set bytes are MEASURED, and an unknown extent fails closed

> **AMENDED 2026-08-06 by owner ruling** (scope-preserving mechanism fix under the existing Seam-1
> approval). The original text required the weight to come from `PartitionLoc.data_size`, asserting
> BIG resolved it. **No Cassandra 5.0 index format records a per-partition size**: a BIG index entry is
> `[key][data_offset vint][promoted_index_len vint][promoted_index]`
> (`docs/sstables-definitive-guide/chapters/06-index-and-summary.md`, "Index.db Entry Format"; written
> by `BigTableWriter.createRowIndexEntry` at tag `cassandra-5.0.8`) and the BTI `Partitions.db` trie
> resolves an offset only. See `design.md` D6.

Each recorded access SHALL carry the partition's **on-disk extent, MEASURED** as the successor gap
`[data_offset, successor_offset)`, summed across the SSTables resolved for that access. This is
authoritative index-LAYOUT metadata — the same bound the single-partition seek path already uses to
size its decompression window — not an estimate. The LAST partition of an SSTable, which has no
successor, SHALL bound instead to the authoritative UNCOMPRESSED data-section length
(`CompressionInfo.db`'s `data_length` when compressed, else the `Data.db` file length).

A window entry SHALL retain the **maximum** weight observed for that partition, never a running sum,
because the working set is defined over **distinct** partitions: ten accesses to one partition
contribute its bytes once.

`cqlite.read.partition_access.bytes` per bucket SHALL therefore be the sum of distinct-partition
on-disk bytes in that bucket. This is the measurement that reduces the working-set question from two
unknowns (concentration x size) to one (the decode multiplier).

Measured bytes SHALL be reported under the distinct `size_source = successor_gap`, never as `index`.
Where an access mixes provenances the **weakest** SHALL be reported.

**An UNKNOWN extent SHALL fail closed and SHALL NOT silently under-report.** An access for which
**any** resolved SSTable yielded no authoritative extent — no index available, no data-section length,
a fail-safe whole-file scan that resolves no per-partition layout, or a resolution error — SHALL:

- set a sticky `size_source = unavailable` flag on that window entry,
- contribute **zero** bytes to `cqlite.read.partition_access.bytes`, and
- be counted under `cqlite.read.partition_access.distinct_partitions{size_source="unavailable"}`.

A size SHALL NOT be estimated, interpolated by proportion, or defaulted to a nominal value. The
`unavailable` fraction SHALL be readable from the emitted series alone, and the decision procedure
SHALL refuse any window whose `unavailable` fraction is non-zero. That condition tests
**incompleteness, not provenance**: a fully gap-measured window is complete and SHALL be priced.

**Evidence:** `cqlite-core/tests/issue_2827_partition_access_bytes.rs` (max-not-sum semantics;
weakest-provenance mixing; a gap-measured census window is priced rather than refused; and both a
real BIG (`nb`) and a real BTI (`da`) committed fixture priced end-to-end from their measured gaps,
per-case fail-closed on fixture resolution (#3220)).

#### Scenario: Repeated accesses to one partition count its bytes once
- **GIVEN** a partition of known on-disk extent accessed ten times in one window
- **WHEN** the window closes
- **THEN** it appears once in `distinct_partitions` under the `9-16` bucket
- **AND** `cqlite.read.partition_access.bytes` for that bucket increases by that partition's extent exactly once, not ten times

#### Scenario: A partition's extent is measured from its successor and labelled as measured
- **GIVEN** a Cassandra-written fixture of either format, whose index records no partition size
- **WHEN** a partition is read and the window closes
- **THEN** it is counted under `distinct_partitions{size_source="successor_gap"}`, never `{size_source="index"}`
- **AND** `cqlite.read.partition_access.bytes` for its bucket is non-zero
- **AND** the window's `unavailable` fraction is zero, so the decision procedure prices it rather than refusing it

#### Scenario: An unmeasurable extent is marked unavailable and contributes no bytes
- **GIVEN** an access for which no authoritative extent can be resolved
- **WHEN** the window closes
- **THEN** it is counted under `distinct_partitions{size_source="unavailable"}`
- **AND** it contributes zero to `cqlite.read.partition_access.bytes`
- **AND** no non-zero size is recorded for it from any source

#### Scenario: A mixed window makes its incompleteness visible
- **GIVEN** a window containing both extent-measured and unmeasurable partition accesses
- **WHEN** the window closes
- **THEN** both `size_source="successor_gap"` and `size_source="unavailable"` series are present with non-zero values
- **AND** the decision procedure applied to that window returns a refusal naming the non-zero `unavailable` fraction, rather than a hit-ratio number computed from the partial byte total

### Requirement: The instrument is wired into the logical point-read boundary and is zero-cost when disabled

Accesses SHALL be recorded once per **logical partition read**, at the core targeted read path
(`cqlite-core/src/query/select_executor/lookup.rs:92` `classify_partition_lookup` yielding
`Targeted`/`MultiTargeted`, consumed at `streaming.rs:107` and `stream_agg.rs:196`) and at the Flight
point path (`cqlite-flight/src/producer_point.rs:83` `point_read_keys`).

Accesses SHALL NOT be counted at the per-SSTable probe sites in
`cqlite-core/src/storage/sstable/reader/partition_lookup.rs`. Counting there would multiply every
partition's repeat count by the number of generations holding the key, shifting the histogram right
and **manufacturing concentration the workload does not have** — a bias toward "go". Those sites
supply byte weights only.

The probe SHALL be **off by default** at runtime, controlled by `CQLITE_PARTITION_ACCESS_PROBE` read
once into a `OnceLock` with a programmatic override taking precedence, mirroring the `CQLITE_READ_PATH`
pattern at `cqlite-core/src/query/select_executor/forcing.rs:42`, `:78-82`. When disabled the hot path
SHALL perform at most one relaxed atomic load, SHALL allocate nothing, and SHALL emit nothing.

Metric **export** SHALL remain gated by the existing `observability` feature, with call sites always
compiled and helpers compiling to no-ops when the feature is off
(`cqlite-core/src/observability/mod.rs:49-54`). A default `cqlite-core` build SHALL continue to link no
OpenTelemetry crates.

**Known coverage limitation (recorded, not waived):**
`StorageEngine::scan_partition_with_cell_metadata` (the WRITETIME/TTL-projection point
read) is a logical point read that is NOT recorded, so its accesses are invisible to
the histogram. The direction is conservative — those partitions are under-counted,
which **understates** concentration — but a workload whose keyed traffic is
predominantly WRITETIME/TTL projections is measured badly and its window must not be
used for the decision.

**Wiring evidence (a public surface + a call chain + an end-to-end test):** public surface
`cqlite_core::observability::partition_access`; call chain *point-read boundary →
`record_partition_access` → window close → `obs::add_counter`*; end-to-end test
`cqlite-flight/tests/issue_2827_partition_access_e2e.rs`, which drives repeated keyed point reads
through a real `CqliteFlightService` `do_get` and reads back the emitted series. Green unit tests on
the recorder alone are not sufficient.

#### Scenario: Repeated keyed point reads through do_get produce the histogram
- **GIVEN** a running `CqliteFlightService` over a fixture table, with the probe enabled and the `observability-testing` capture harness installed
- **WHEN** a client issues point-read `do_get` calls whose tickets carry a full-PK equality `filter`, hitting one partition five times and four other partitions once each, and the window is then closed
- **THEN** the emitted `distinct_partitions` series reports one partition in `5-8` and four in `1`
- **AND** the reads report access path `streaming_partition_lookup`, confirming the instrument sat on the keyed route rather than a degraded scan

#### Scenario: A partition present in several SSTables is one access, not several
- **GIVEN** a table whose generations all contain the same partition key
- **WHEN** that partition is read once through the logical point path
- **THEN** the recorder registers exactly one access for it, regardless of how many per-SSTable probes the read performed
- **AND** its byte weight is the sum of the per-SSTable MEASURED extents (successor gaps) resolved for that one access — no Cassandra 5.0 index records a `data_size`

#### Scenario: Disabled by default, and costless when disabled
- **GIVEN** a process with `CQLITE_PARTITION_ACCESS_PROBE` unset and no programmatic override
- **WHEN** a keyed read workload runs
- **THEN** no `cqlite.read.partition_access.*` series is emitted, the recorder has allocated no counting table, and query results are unchanged
- **AND** a default-feature `cqlite-core` build links no OpenTelemetry crate

### Requirement: The instrument's recovery of a known input distribution is verified

An in-repo test SHALL drive **known** access distributions — at minimum a uniform one and a skewed one
with a hand-computed expected bucket histogram — through the instrumented surface, and SHALL assert
that the recovered histogram matches the known concentration, exactly where the window is a census and
within a stated tolerance where it is a sample.

The expected values SHALL be derived from the input sequence by arithmetic that does not pass through
the instrument, so the test is capable of failing.

**The oracle's legitimacy SHALL be recorded with the test, and so SHALL its limit.** A synthetic input
is a legitimate oracle for a claim about the **instrument** ("it recovers a distribution I control")
and an **illegitimate** oracle for a claim about the **world** ("the field workload is skewed"). This
is the same asymmetry CLAUDE.md records for round-trip invariance (#3042) — where both sides share an
assumption, the artefact closes on itself and validates nothing — and for the two parity oracles
(#1742), where an oracle blind to the property under test is decoration. Accordingly this test SHALL
NOT assert any hit ratio, cache size, or skew parameter, and its output SHALL NOT be cited anywhere as
evidence about a real workload.

**Evidence:** `cqlite-core/tests/issue_2827_partition_access_histogram.rs`.

#### Scenario: A known skewed distribution is recovered exactly
- **GIVEN** a constructed access sequence in which 10 partitions are accessed 20 times each, 100 are accessed 3 times each and 1,000 once each
- **WHEN** the sequence is driven through the recorder and the window closed as a census
- **THEN** `distinct_partitions` reports exactly 1,000 in `1`, 100 in `3-4` and 10 in `17+`, and zero in `2`, `5-8` and `9-16`
- **AND** `accesses` reports 1,000, 300 and 200 for those buckets respectively

#### Scenario: A uniform distribution is not reported as concentrated
- **GIVEN** a constructed sequence accessing 5,000 distinct partitions exactly once each
- **WHEN** the window is closed as a census
- **THEN** all 5,000 partitions are reported in bucket `1` and every other bucket is zero
- **AND** the test asserts nothing about a hit ratio or a cache size

### Requirement: A committed decision procedure converts a closed window into a go/no-go, and refuses when it cannot

A committed note at `docs/research/decoded-partition-cache-decision.md` SHALL state the complete
procedure for turning one closed window into the 64 MiB / 128 MiB decoded-partition-cache verdict,
such that no further analysis round is required once a real workload is instrumented.

It SHALL state, at minimum:

- the inputs (per-bucket `distinct_partitions`, `accesses`, `bytes`; `sample_denominator`; the
  `unavailable` fraction) and the single remaining **assumption**, the decode multiplier `m`, cited to
  its provenance as a Phase-0 wire estimate (`docs/research/phase2-verify-caching.md:221-222`) and
  labelled an assumption, not a measurement;
- the closed-form clairvoyant hit-ratio ceiling at a decoded budget `C`, ordering buckets by access
  density `accesses / bytes` and filling `C / m` of on-disk bytes greedily, charging one compulsory
  miss per selected distinct partition;
- that the result is an **upper bound**, so a low value is a sound **no-go** while a high value is
  necessary but not sufficient for a "go";
- the **refusal conditions**, each of which yields *no answer* rather than a default verdict: a
  non-zero `unavailable` fraction, accesses the recorder could not seat at all, a window at the
  sampling floor, a window that is a SAMPLE rather than a census (its per-bucket bytes are
  sample-domain totals, so filling a real budget against them overstates what fits), a window below
  a stated minimum access count, and a window generated by synthetic or self-generated load;
- a **recommended go threshold with its arithmetic**, explicitly labelled an owner-settable parameter;
- the tumbling-window bias and its conservative direction (understates concentration).

**Evidence:** the committed note, plus a worked example in it computed from the validation test's
known distribution (labelled a self-check, never a field result).

#### Scenario: The procedure refuses a window it cannot price
- **GIVEN** a closed window with a non-zero `size_source="unavailable"` fraction
- **WHEN** the procedure is applied
- **THEN** it returns a refusal that names the unpriceable fraction
- **AND** it does not produce a hit-ratio number from the partial byte total

#### Scenario: The procedure yields a verdict from a complete census window
- **GIVEN** a closed census window with `sample_denominator = 1`, a zero `unavailable` fraction, and an access count above the stated minimum
- **WHEN** the procedure is applied at 64 MiB and at 128 MiB with the cited decode multiplier
- **THEN** it yields a hit-ratio ceiling for each budget and a go/no-go against the recorded threshold
- **AND** the verdict states that the ceiling is clairvoyant, so a "go" is a licence to simulate LRU against the captured window rather than a licence to build

### Requirement: The change records what it does not deliver

The change SHALL state, in `proposal.md`, `design.md`, this spec and the committed decision note, that
it delivers **the instrument and the procedure, not the field number**, and that issue #2827's original
AC2 is **not satisfied** by it — not waived, not deferred to another issue, but satisfiable on the
first real keyed workload run with the probe enabled, blocked only by the absence of such a workload.

The change SHALL record the owner's standing instruction that the issue must stop calling itself the
gate, together with the fact that **retitling or re-scoping the issue is an owner action the worker
does not take**.

No artifact in this change SHALL describe any output of this change as a measured field skew, as the
go/no-go, or as a gate. No artifact SHALL contain a hit-ratio-vs-skew sensitivity curve or any
hit-ratio number derived from a distribution chosen by this change.

The change SHALL remain decoupled from #2037: it SHALL NOT build, size, wire, configure or benchmark a
decoded-partition cache, and SHALL NOT introduce a dependency on that work.

**Evidence:** `proposal.md` (Scope reset, Non-goals), `design.md` D1/D8/D10, this requirement, and the
decision note's own scope paragraph.

#### Scenario: The scope statement is present and consistent across artifacts
- **GIVEN** the change's four artifacts and the committed decision note
- **WHEN** they are read for the AC2 claim
- **THEN** each states that the field number and the go/no-go are not delivered, and gives the same reason (no field keyed workload with captured concentration exists)
- **AND** none of them describes any output of this change as measured field skew, as the go/no-go, or as a gate

#### Scenario: No cache is built or depended on
- **GIVEN** the change's full diff
- **WHEN** it is inspected for cache work
- **THEN** it contains no decoded-partition cache, no cache sizing configuration, and no reference that makes this change depend on #2037

### Requirement: The metric catalog, the operator docs and the published attribute tables record the new metrics correctly

The seven metrics SHALL be registered as constants in `cqlite-core/src/observability/catalog.rs` and
listed in `ALL_METRICS` (`:907`), and each SHALL carry an operator annotation in
`operator_docs_annotations.rs` — the generator is fail-closed, so an unannotated catalog metric cannot
ship (`cqlite-core/src/observability/operator_docs.rs:16-19`, `:116`). The two new attribute keys SHALL
be added to `catalog::attr` with their closed value sets documented.

The generated operator pages `docs/reports/flight-metrics-reference.md` and
`website/src/content/docs/agents-using/flight-metrics-reference.md` (`operator_docs.rs:35`, `:40`) SHALL
be regenerated in the same change, and `docs/observability/configuration.md` SHALL gain the seven metric
rows and the two attribute value-set rows. The `operator-metrics-doc` and `kit-dashboard-drift` agent-gate
components SHALL pass (`scripts/agent-gate.sh:2033`).

**The stale documentation of the metric this change's premise is about SHALL be corrected in the same
change.** `docs/observability/configuration.md:215` and the OTel instrument description at
`cqlite-core/src/observability/otel.rs:384` both state that `cqlite.read.partition_lookup.total` is
keyed by `cqlite.query.access_path`; the authoritative catalog doc (`catalog.rs:283`) and every
emission site (`cqlite-core/src/storage/sstable/reader/partition_lookup.rs:87`, `:156`, `:353`, `:414`,
`:440`) attach `cqlite.read.lookup_route` (`catalog.rs:82`). The documented value set for
`cqlite.query.access_path` at `docs/observability/configuration.md:298` SHALL likewise be corrected to
include `streaming_partition_lookup` and `metadata_partition_lookup`, both emitted by
`cqlite-core/src/query/access_path.rs:125-126` — the omission of `streaming_partition_lookup` is the
documented source of the "assert on bare `partition_lookup`" mistake, since a plain full-PK equality
point read reports `streaming_partition_lookup` (`:126`), never `partition_lookup` (`:122`).

This correction SHALL be documentation and description text only: no attribute on an existing metric
may be added, removed or renamed by it.

**Evidence:** the regenerated committed pages plus the `operator-metrics-doc` and
`kit-dashboard-drift` gate components; `cqlite-core/tests/issue_2827_partition_access_histogram.rs`
asserts the emitted attribute keys of `cqlite.read.partition_lookup.total` match the corrected
documentation.

#### Scenario: A new metric cannot ship undocumented
- **GIVEN** the seven metrics added to `ALL_METRICS`
- **WHEN** the operator-metrics doc generator runs
- **THEN** it succeeds only because each has an operator annotation, and the regenerated pages contain all seven with their units and bounded attribute sets
- **AND** the `operator-metrics-doc` gate component reports no drift

#### Scenario: The corrected attribute documentation matches what the code emits
- **GIVEN** the corrected `docs/observability/configuration.md` row for `cqlite.read.partition_lookup.total`
- **WHEN** a point read emits that counter and the series is read back
- **THEN** its attribute keys are exactly `cqlite.result`, `cqlite.read.lookup_route` and `cqlite.sstable.format`, matching the corrected row
- **AND** the documented `cqlite.query.access_path` value set contains every label `AccessPath::label()` can return
