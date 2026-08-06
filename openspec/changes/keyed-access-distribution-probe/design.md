# Design: bounded partition repeat-access instrument (issue #2827)

## Context

Issue #2827 was filed to *measure* the field keyed hot-set concentration and *decide* whether a
64–128 MiB decoded-partition cache is worth building. Four owner comments on the thread established
that the filed method cannot work and that the obvious replacement is circular. This design records
the third option the owner named — a bounded repeat-access histogram — and the smallest set of
decisions that turn it from "a histogram" into "a decision procedure that fires on the first real
workload".

Three facts frame every decision below.

1. **No per-key attribute is permissible.** `docs/observability/configuration.md:304-305`:
   "Unbounded values (raw error messages, **partition keys**, full query text) are **NEVER** attached
   as attributes or span fields." Mirrored in code at
   `cqlite-core/src/observability/catalog.rs:19-24`. So the instrument must summarise *inside the
   process* and emit only bounded buckets.
2. **No field workload exists to measure.** `docs/research/phase2-verify-caching.md:214-216` — the
   only field keyed loadtest on record is ~0.9 qps with no captured concentration. This is why the
   deliverable is the instrument, not the number.
3. **The one thing we must not build is a synthetic answer.** The owner's rejection of the
   sensitivity-curve plan is a doctrine statement, not a preference — see D8.

## Recommended design

### D1 — Scope: build the instrument and the decision procedure; do NOT run a synthetic field round

**Decision.** #2827 delivers (a) a bounded partition repeat-access instrument, (b) measured
working-set bytes, (c) a validation test against a known input distribution, and (d) a written
decision procedure. It does **not** deliver a skew number or the go/no-go.

**Alternatives considered.**

- *(A1) Build the synthetic Zipf sweep as originally scoped in the first thread comment.* **Rejected**
  by the owner: "It converts a *measurement* into an *owner judgment about an assumed skew* … A
  sensitivity curve restates that conditional with more decimal places." The output would be a
  function of a distribution we selected; `phase2-verify-caching.md:220-227` already records that
  conditional, so the curve adds decimal places, not knowledge. Building it anyway would also have
  cost the F1 loadgen work first (see D9), for an output already ruled non-decisive.
- *(A2) Do nothing until a field keyed workload exists.* **Rejected.** The instrument is the thing
  that makes such a workload informative when it appears; without it, the first real keyed round
  produces latency numbers and no distribution, exactly as #2866 did. Cheap now, expensive to
  retrofit under a live round.
- *(A3) Instrument AND run the synthetic round, labelling the curve carefully.* **Rejected.** A
  carefully-labelled synthetic curve is still read as a measurement by the next reader — which is
  precisely the owner's stated concern ("a green sensitivity curve will later be read as 'measured
  field skew' by someone who did not read this thread"). Publishing the number is the hazard; the
  label is not a control.

**Consequence, stated rather than hidden:** issue #2827's AC2 is **unmet** by this change. See the AC
table in `specs/partition-access-distribution/spec.md`, D10, and the honesty clause in every artifact.

### D2 — The instrument sits at the LOGICAL point-read boundary, not at the per-SSTable probe sites

**Decision.** One access is recorded per **logical partition read** — one call at the core targeted
path (`cqlite-core/src/query/select_executor/lookup.rs:92` `classify_partition_lookup` returning
`Targeted`/`MultiTargeted`, consumed at `streaming.rs:107` and `stream_agg.rs:196`) and one at the
Flight point path (`cqlite-flight/src/producer_point.rs:83` `point_read_keys`, one record per key in
the returned `PointReadPlan`). The per-SSTable probe sites in
`cqlite-core/src/storage/sstable/reader/partition_lookup.rs` (`:84`, `:128`, `:152`, `:349`, `:410`,
`:436`) report **byte sizes** into the open access, but do **not** count accesses.

**Why this is a correctness decision, not a convenience one.** With STCS a live partition is present
in *k* SSTables at once. Counting at the probe sites multiplies every partition's repeat count by
approximately *k* — which shifts the entire histogram to the right and **manufactures concentration
that the workload does not have**. A uniform workload over a 4-generation table would report every
partition in the `3-4` bucket instead of the `1` bucket, and the decision procedure would read that
as a hot set. The bias direction is the dangerous one (it makes the cache look good), so the cheaper
wiring is disqualified outright.

The cache being sized is a **decoded-partition** cache, whose unit is the reconciled logical
partition — so the logical boundary is also the semantically correct one: it counts exactly the
events such a cache would serve.

**Known coverage limitation, recorded rather than left implicit.**
`StorageEngine::scan_partition_with_cell_metadata` — the WRITETIME/TTL-projection
sibling of `scan_partition` (`SELECT WRITETIME(col) … WHERE pk = ?`) — is also a
logical point read, and it is **NOT** recorded. Those accesses are therefore invisible
to the histogram. The direction is conservative: a partition read only through the
metadata path is under-counted, which **understates** concentration and so understates
the case for a cache. It is a gap in coverage, not a bias toward "go", and a workload
whose keyed traffic is predominantly WRITETIME/TTL projections would be measured
badly — such a window should not be used for the decision. Closing it is a
one-wrapper change on the same pattern as the three recorded boundaries.

**Alternative considered.** *Count at the probe sites and divide by an SSTable count.* Rejected: the
divisor varies per partition (a key may be in 1 or 7 generations) and is not knowable at the probe
site, so the correction would itself be an estimate — a heuristic in the sense CLAUDE.md's #28
mandate exists to prevent.

### D3 — Metric shape: SEVEN counters/gauges, two new bounded attributes, 34 series

**Decision.**

| Metric | Instrument | Unit | Bounded attributes |
|---|---|---|---|
| `cqlite.read.partition_access.distinct_partitions` | counter | `{partition}` | `cqlite.read.repeat_bucket`, `cqlite.read.size_source` |
| `cqlite.read.partition_access.accesses` | counter | `1` | `cqlite.read.repeat_bucket` |
| `cqlite.read.partition_access.bytes` | counter | `By` | `cqlite.read.repeat_bucket` |
| `cqlite.read.partition_access.sample_denominator` | gauge | `1` | (none) |
| `cqlite.read.partition_access.dropped_accesses` | counter | `1` | (none) |
| `cqlite.read.partition_access.window_dropped_accesses` | gauge | `1` | (none) |
| `cqlite.read.partition_access.sampling_floor` | gauge | `1` | (none) |

The last three are the window's **trustworthiness** signals (added after roborev
rounds 2–3): accesses the recorder could not seat, cumulatively and per window, and
whether the window hit the sampling cap. Two gauges rather than one counter because a
cumulative counter cannot answer "was THIS window clean" — once it increments it
reads non-zero for the life of the process. A window is clean exactly when both
gauges read `0`; both are emitted on every closed window, including at zero.

New attribute keys, both closed sets:

- `cqlite.read.repeat_bucket` ∈ { `1`, `2`, `3-4`, `5-8`, `9-16`, `17+` } — exactly the owner's six
  buckets, verbatim.
- `cqlite.read.size_source` ∈ { `index`, `successor_gap`, `unavailable` } — a closed
  set of THREE values (amended D6 / rider R1).

Total series: `6 × 3 + 6 + 6 + 1 + 1 + 1 + 1` = **34**, fixed forever (`size_source`
carries three values, not two — see D6 — and the three trustworthiness signals are
unlabelled scalars). Compare the existing bounded sets:
`cqlite.read.partition_lookup.total` carries three attributes (`catalog.rs:283`) and
`cqlite.rpc.phase.duration` carries a five-value closed phase set — this is the same order.

**Naming.** The briefing's working name was `cqlite.read.partition_access.repeat_count`. Rejected on
the catalog's own convention: "counters describe a monotonically increasing total; **their name
reflects the thing being counted**" (`catalog.rs:14-15`). The thing counted is *distinct partitions*,
not repeats, so a counter called `repeat_count` whose value is a partition count would be a
mis-named series on a dashboard. The chosen names each say what their value is.

**Why `accesses` is emitted per bucket and not just as a total.** Emitting, per bucket, the *sum of
the repeat counts of the partitions in that bucket* removes the within-bucket mean from the
hit-ratio math entirely (D7): the bound becomes a point value rather than an interval, and the
open-ended `17+` bucket stops being unbounded. This is one extra series for a materially stronger
output.

**Why `cqlite.sstable.format` is NOT carried.** Per D2 the unit is a *logical* partition access, which
may span a BIG and a BTI generation in the same read; attaching a format would require picking one
arbitrarily. (Amended D6 removes the original secondary rationale — `size_source` does NOT stand in
for the format, because neither format records a size and both are measured the same way.)

**Composition with existing attributes.** These are new series, not new attributes on existing
metrics: `cqlite.read.partition_lookup.total` is left exactly as it is. Adding `repeat_bucket` to it
was considered and rejected — that counter is emitted once per *probe*, so it is subject to the D2
inflation, and adding a 6-value attribute would multiply its existing 3-attribute series count.

### D4 — Counting structure: one 3 MiB open-addressed table with adaptive hash-prefix downsampling

**Decision.** A single lazily-allocated, open-addressed (linear-probing) table:

- `SLOTS = 1 << 17` = 131,072
- entry = `key_hash: AtomicU64` (8 B) + `bytes: AtomicU64` (8 B) + `count: AtomicU32` (4 B) +
  `flags: AtomicU8` (1 B) + 3 B padding = **24 B**
- **total = 131,072 × 24 B = 3,145,728 B = exactly 3 MiB, fixed** — independent of partition count,
  of qps, and of window length.

The table lives behind an `RwLock`: the hot path takes the **read** lock and mutates its slot with
relaxed atomics (no exclusive lock, no allocation, no per-access `String`); the **write** lock is
taken only on a downsample pass and on window close, both rare.

`key_hash` is a 64-bit hash of the **raw partition-key bytes** the read path already holds (the
`PartitionKey::to_bytes` form documented at `partition_lookup.rs:52-57`), not the Murmur3 token.
Murmur3 was considered because it is already computed on some paths
(`cqlite-core/src/query/select_executor/predicate.rs:58`, `scan_merge.rs:170`) — but it is **not**
computed on the BIG point path, whose lookup takes raw key bytes
(`partition_lookup.rs:63-66`), so using it would mean computing a token that path does not need. A
cheap non-cryptographic 64-bit hash of the same bytes is strictly less work and is only ever used for
slot addressing and identity within a window.

**Entry identity is TABLE-SCOPED (roborev round 3).** The hash covers
`(keyspace, table, raw key bytes)`, not the key bytes alone. `global()` is ONE
process-wide recorder shared by every table, so a key-only identity merges the same
key in two tables into one entry — and a tenant/user id shared across tables is
ordinary, not a rare collision. The merge is biased twice toward "build the cache":
two singletons become a `count = 2` entry (`accesses − distinct` reports a hit where
the truth is none), and the entry keeps the MAXIMUM byte weight rather than the sum,
so it is under-priced and ranks EARLIER by access density. Both call sites already
held the table identity, so this costs nothing on the hot path.

**Overflow handling: adaptive hash-prefix downsampling, never eviction.** When occupancy reaches a
load factor of 0.75 (98,304 entries) the recorder takes the write lock, increments `k`, and drops
every entry whose `key_hash` does not satisfy the new `k`-bit prefix predicate — one linear pass that
halves occupancy — and doubles `sample_denominator = 2^k`. Thereafter only keys satisfying the
predicate are admitted.

**Why this and not the obvious alternatives.**

- *(B1) LRU eviction when full.* **Rejected, and the bias is the reason.** Evicting cold entries
  keeps hot ones, so the surviving population is enriched in high-repeat keys: the singleton bucket
  is under-counted and the histogram **overstates concentration** — the direction that makes the
  cache look better than it is. A go/no-go instrument must not be biased toward "go".
- *(B2) Drop new keys when full (first-come admission).* **Rejected, same direction.** Hot keys
  arrive early by construction (they are accessed often), so late-dropped keys are disproportionately
  cold — again understating the singleton bucket.
- *(B3) Count-Min Sketch.* **Rejected on capability, not size.** CMS answers "what is the count of
  *this* key"; it cannot enumerate the key population, and this instrument needs the *distribution of
  counts over distinct keys*. There is no way to produce the bucket histogram from a CMS without a
  separate key list — i.e. without the thing CMS was chosen to avoid.
- *(B4) Unbounded `HashMap`.* **Rejected**: 1.93 M partitions/node
  (`phase2-verify-caching.md:212`, `:224`) at ~48 B/entry is ~90 MiB against a <128 MB whole-process
  budget, and it is unbounded in principle.

**Why hash-prefix sampling is unbiased where B1/B2 are not.** The admission predicate is a function
of the key hash alone — it is statistically independent of the key's access frequency. So the
admitted set is a uniform random sample of the *distinct* partitions touched in the window, and each
admitted key's count is **exact** (every access to an admitted key is counted, from window start,
because the predicate is monotone: a key admitted under `k` is admitted under all `k' < k`, so
downsampling only ever removes keys, never invalidates a survivor's count). Scaling
`distinct_partitions` and `bytes` by `2^k` therefore gives unbiased estimates, and the bucket
*fractions* — which are what the decision procedure consumes — are unbiased with no scaling at all.

**Failing to seat a key is never a drop (roborev round 2).** The probe bound is 64 slots, while the
expected longest cluster near a 0.75 load factor is several hundred — so a new key can fail to seat
well BELOW the load factor, where the occupancy-gated widen loop never fires. Dropping it there is
biased in the same direction eviction is: an existing entry is always within 64 probes of its home,
so only NEW keys are lost, the singleton bucket is suppressed and concentration is OVERSTATED. The
recorder therefore widens (unbiased, frequency-independent) and retries; a key that still cannot be
seated at the cap is counted and PUBLISHED as a dropped access, and the decision procedure refuses
any window reporting one.

**Sampling floor.** `k` is capped at 20. At `k = 20` the sample is 1/1,048,576 of the key space,
which over a 1.93 M-partition corpus admits ~2 keys; a window that reaches the cap is
statistically worthless, so the recorder marks it non-census and the decision procedure refuses it
(D7). In practice `k` stabilises: a window whose distinct accessed set is under ~98k never
downsamples at all.

**Memory bound, stated once:** 0 bytes when the probe is off (lazy allocation); **3 MiB exactly**
when on; no term in partition count, qps, window length, or `k`.

### D5 — Window semantics: tumbling, closed deterministically, emitted exactly once

**Decision.** A **tumbling** window, closed on whichever comes first:

- a wall-clock duration (default 60 s), or
- an access-count bound (default 5,000,000 recorded accesses), or
- an explicit `close_window()` call.

On close, under the write lock: walk the table, bucket every live entry by its `count`, emit the four
series once, zero the table, reset `k` to 0. Emission is **exactly once per closed window**; a window
with zero accesses emits nothing (a `0/0` emission would be a series with no subject — the same
"affirmative measurement" rule CLAUDE.md records for the roborev wrapper's `prompt-content:` key).

**Why tumbling rather than a sliding two-generation window.** A sliding window (current + previous
generation, rotated) costs **double the memory** (6 MiB) and materially more code, to fix a bias whose
direction is *conservative*: a partition accessed on both sides of a boundary is recorded as two
lower-repeat entries rather than one high-repeat entry, so a tumbling window **understates**
concentration. Understating concentration makes the cache look *worse*, which is the safe direction
for a go/no-go — a "go" produced despite this bias is a stronger signal, and a "no-go" is the one
verdict this instrument can produce cheaply and wrongly only in the direction of not building
something. The bias is bounded by the ratio of window length to inter-access interval and shrinks
with window length; 60 s at A2-scale qps is thousands of accesses per hot key.

**Why the access-count bound exists alongside the duration.** At A2 scale (≥1,000 qps/pod) a 60 s
window is ~60,000 accesses, comfortably inside the table. The count bound only fires on a workload
far above that, and its purpose is to close the window *before* downsampling degrades the sample
rather than after.

**Why `close_window()` is public.** So tests can close a window **deterministically** and never
assert on elapsed wall time. CLAUDE.md mechanizes a `--lite` lint against wall-clock threshold
asserts in the correctness test path (`roborev-lints` / #2642); the deterministic hook is what keeps
every test in this change on the right side of it. The wall-clock trigger is a property of the
*instrument*, exercised only by an explicitly-`#[ignore]`d timing test if at all.

### D6 — Byte weighting is MEASURED as the successor gap; a genuinely unknown extent fails closed

> **AMENDED 2026-08-06 by owner ruling** (escalation from implementation, accepted in
> full and ruled **scope-preserving** — a mechanism fix under the existing Seam-1
> approval, not a renegotiation, because the approved deliverable was "measured
> working-set bytes, verdict falls out of the first real window" and this is the only
> mechanism that keeps that true). The original D6 rested on a **false premise about
> the on-disk format** and is superseded below. Its rejection of the successor gap
> died with that premise.

**The premise that was false.** The original text asserted that "BIG (`Index.db`)
resolves both fields" and that only BTI lacked a size. **Neither Cassandra 5.0 index
format records a per-partition byte size.** A BIG index entry is

```
[key][data_offset: unsigned vint][promoted_index_len: unsigned vint][promoted_index]
```

— position and promoted-index length, no size field
(`docs/sstables-definitive-guide/chapters/06-index-and-summary.md`, "Index.db Entry
Format"; written by `BigTableWriter.createRowIndexEntry` at tag `cassandra-5.0.8`).
The BTI `Partitions.db` trie resolves an offset only. Had the original D6 shipped, the
`size_source` attribute would have had exactly ONE reachable value, the byte counter
would have been permanently zero, and the decision procedure would have refused every
real window on refusal condition 1 — i.e. the "measured working-set bytes" half of D1
would have shipped dead, leaving the decode multiplier as one of TWO unknowns again.

**Decision.** Each logical access carries a byte weight that is **MEASURED** as the
partition's on-disk extent:

- **The successor gap**, `[data_offset, successor_offset)`, summed across the SSTables
  resolved for that access. This is authoritative index-LAYOUT metadata, not an
  estimate: it is the same bound the single-partition seek path already uses to size
  its decompression window, resolved per format by one strict-ceiling trie walk (BTI)
  or the minimum `Index.db` offset strictly greater than the target (BIG).
- **The last partition** in an SSTable has no successor, and bounds instead to the
  authoritative UNCOMPRESSED data-section length: `CompressionInfo.db`'s `data_length`
  when compressed, else the `Data.db` file length (for an uncompressed SSTable the file
  IS the data section).
- The window entry stores `bytes = max(bytes, this_access_bytes)`, not a sum — the
  working set is defined over **distinct** partitions, so ten accesses to one partition
  contribute its size once. `max` is exact because partition extents are immutable
  within a generation set, and it is robust to an access that resolved fewer
  generations than another.

**Provenance is a distinct, truthful label (rider R1).** `cqlite.read.size_source` is a
closed set of **three** values, not two:

| value | meaning |
|---|---|
| `index` | every resolved SSTable reported a size directly in its index metadata. Unreachable for Cassandra-written SSTables; retained so a producer that genuinely knows a size is never forced to report a measured one. |
| `successor_gap` | the extent was MEASURED as above. |
| `unavailable` | at least one resolved SSTable yielded no authoritative extent. Counted as a partition, contributes **ZERO** bytes. |

A reader must always be able to distinguish measured-from-gap from
index-supplied and from genuinely-unknown, so the provenance is its own label rather
than being folded into `index`. Where an access mixes provenances the **weakest** wins
(`successor_gap` over `index`, `unavailable` over both): a total is only as
well-founded as its weakest component.

**What still fails closed.** `unavailable` is set when the extent is genuinely
unknowable — no index available, no data-section length, a fail-safe whole-file scan
that resolves no per-partition layout, or a resolution error. Such an entry contributes
zero bytes, is counted under
`distinct_partitions{size_source="unavailable"}`, and makes the byte total's
incompleteness visible **as a ratio**. The decision procedure refuses any window with a
non-zero `unavailable` fraction. That condition tests **incompleteness, not
provenance**: a fully gap-measured window is complete and is priced, which is the whole
point of this amendment. A size is still never estimated, interpolated by
proportion, or defaulted to a nominal value (no-heuristics, #28).

**Two honest costs, recorded rather than buried (rider R3).**

1. **The last partition's extent is bounded by the data-section length, not by a
   successor.** It is authoritative, but it is a different measurement from the others:
   it includes any trailing data-section bytes after that partition. One partition per
   SSTable per window is affected. The alternative — reporting the last partition
   `unavailable` — would refuse every window containing it, which is strictly worse.
2. **These are UNCOMPRESSED offsets, so the extent is an uncompressed on-disk size.**
   That is the *correct* input for the decision procedure, whose decode multiplier `m`
   converts on-disk bytes to decoded bytes; applying `m` to a compressed size would
   compound two ratios. It also means a compressed table's measured bytes exceed its
   file bytes, which is a property of the measurement, not an error — and the
   production write surface is uncompressed-only anyway (#1406).

**Placement is unchanged (verified, not assumed).** The extent is resolved through
reader-level APIs at the **logical** point-read boundary, exactly where the byte
weights were always going to come from — D2 says the per-SSTable sites "report byte
sizes into the open access, but do **not** count accesses". Counting stays once per
logical read; nothing moved to a per-SSTable site.

**Alternative still rejected.** *Skip unpriceable accesses entirely.* This would make
the histogram itself wrong (they are real accesses), not merely the byte total.

### D7 — The decision procedure is a committed document, and it refuses more often than it answers

**Decision.** `docs/research/decoded-partition-cache-decision.md` states the procedure completely, so
that any closed window plus one assumption yields the verdict with no further analysis round.

**Inputs.** For each bucket `b` ∈ {1, 2, 3-4, 5-8, 9-16, 17+}: `n_b` (distinct partitions), `a_b`
(accesses attributable to `b`), `B_b` (distinct on-disk bytes). Plus `sample_denominator = 2^k`, the
`unavailable` fraction, and one assumption: the decode multiplier `m` (Phase-0 wire estimate ≈ 3.5×,
`phase2-verify-caching.md:221-222` — explicitly the only assumption left).

**Refusal conditions (checked FIRST; each is a NO ANSWER, never a default verdict).**

1. `unavailable` fraction > 0 → the byte total is incomplete by an unknown amount. Refuse.
2. `k` reached the cap (20) → the sample is statistically worthless. Refuse.
3. Total accesses `A = Σ a_b` below a stated minimum (10,000) → the window is not a workload. Refuse.
4. The window came from a synthetic or self-generated load → **not a field measurement**; the output
   may be recorded as an instrument self-check and may **never** be cited as the go/no-go (D8).

**The bound.** Buckets are ordered by access density `a_b / B_b` (accesses per on-disk byte),
descending. Fill the budget greedily: with a decoded budget `C` (64 or 128 MiB) the on-disk bytes
that fit are `C / m`. Taking buckets whole while they fit and the last one fractionally by byte
share `f`, the clairvoyant (Belady) hit-ratio estimate is

```
H_max(C) = [ Σ_{fully-taken b} (a_b − n_b)  +  f · (a_last − n_last) ] / A
```

— each selected partition's first access in the window is compulsory (hence `− n_b`), and every
subsequent access hits.

**Corrected after review (owner ruling, Option B):** this is **not** an upper bound. `H_max` is an ESTIMATE UNDER A STATED RANKING HEURISTIC, not a ceiling. Buckets are
ordered by `accesses / bytes`, but the quantity a cache actually serves is
`(accesses − distinct) / bytes`, so a bucket of large HOT partitions can be outranked
by dense small SINGLETONS that serve nothing once admitted — and the greedy fill then
spends the budget on them. The error from THIS defect was measured at ≈0.10 maximum
observed. Other mechanisms (the fractional final-bucket take, and the coverage
limitations of the instrument itself) push independently, so **the total error can
bias in EITHER direction** — do not treat a low value as automatically safe.

Tracked as **issue #3340**, and the ordering constraint is part of the procedure:
**#3340 MUST land before any go/no-go verdict is derived from a real production
window.** Until it does, a value near the threshold decides nothing.
The clairvoyance assumption remains optimistic, so a *high* `H_max` is at most a licence to simulate
LRU against the captured window.

**Recommended go threshold (an OWNER-SETTABLE parameter, recorded as such).** `H_max(128 MiB) ≥ 0.50`.
Rationale, from the owner's own Arm-1 repricing (#2818, quoted in the thread): a decoded-partition
cache targets decode/merge work, and k-way merge measured **3.2%** of on-CPU while LZ4 decompress +
CRC measured **~23%**. A cache with a *ceiling* below 50% on a ≤~3% work share cannot move the
end-to-end number by more than ~1.5% — under the round harness's noise floor. Below the threshold the
verdict is a no-go INDICATION (see the #3340 correction above — it is not sound on its own); at or above it the verdict is "worth a real LRU simulation against the
captured window", not an automatic build.

**Why a threshold is named at all,** given it is the owner's call: an unnamed threshold means the
first person to run the procedure re-litigates it, which is the "owner judgment about an assumed
parameter" failure this change exists to remove. Naming a default with its arithmetic, and labelling
it owner-settable, is the smallest form that still terminates.

### D8 — Why a synthetic input is a legitimate oracle for the instrument and an illegitimate one for the field claim

This is the distinction the whole change turns on, so it is stated as a rule.

**The rule.** A synthetic input is a legitimate oracle for a claim about the **instrument**, and an
illegitimate oracle for a claim about the **world**.

- Validation test (**legitimate**): *"Given an access sequence I constructed, the instrument reports
  the bucket histogram that sequence has."* The expected value is derived from the input by
  arithmetic that does not pass through the instrument, so the test can fail. It is measuring the
  instrument, and the instrument is the thing we built.
- Sensitivity curve (**illegitimate**): *"Given a Zipf distribution I chose, the cache would hit
  70%."* The claim is about a field workload; the evidence is about a parameter I selected. Nothing
  in the artefact can fail, because the input was never in question.

**Why this is CLAUDE.md doctrine and not a local preference.** CLAUDE.md records the same asymmetry
twice:

- *Two parity oracles (#1742)*: a physical-dump oracle "CANNOT catch a read-time-reconciliation bug
  (both sides keep the shadowed rows → green while a real `SELECT` diverges)". The oracle must be
  chosen for the property under test; an oracle that cannot see the property is decoration.
- *Round-trip invariance (#3042)*: "a CQLite-WRITTEN + CQLite-READ round-trip test is INVARIANT to a
  uniform framing/serialization error … Both sides make the *identical* mistake, so the round-trip
  closes and the test stays green." A synthetic-skew sensitivity curve is structurally the same
  object: our assumption about the workload goes in, our conclusion about the workload comes out, and
  the loop closes on itself. CLAUDE.md's remedy there — "for any on-disk framing/encoding property,
  the oracle must be **Cassandra-written bytes**, never CQLite's own output" — generalises here to:
  *for any claim about a workload's access distribution, the oracle must be a real workload, never a
  distribution we generated.*

**Consequence, enforced in the artifacts.** The validation test's assertions are on *recovery of a
known input*, never on a hit ratio. The decision procedure's refusal condition #4 forbids citing a
synthetic window as the go/no-go. And the honesty clause (D10) states that the field number is
undelivered, so no reader has to reconstruct this argument to know what they are holding.

### D9 — The keyed load driver is a follow-up (F1), and this change does not depend on it

**Decision.** `tools/flight-loadgen` gains no keyed mode here. The instrument is validated
hermetically (D8) and wired end-to-end through the existing in-process rig — `serve_fixture()` at
`tools/flight-loadgen/src/selftest.rs:47` stands up a real `CqliteFlightService` on `127.0.0.1:0`
(`:66`) over a 1-SSTable `cassandra_easy_stress.keyvalue` fixture built from the committed constants
in `cqlite-flight/src/test_fixtures.rs:40-53` (feature `test-util`, `cqlite-flight/src/lib.rs:72-74`)
and drives the ordinary `run_ramp` engine (`selftest.rs:123`) — plus the
`observability-testing` capture harness pattern established by
`cqlite-flight/tests/metrics_capture_test.rs`.

**Why F1 is a separate issue rather than part of this one.** Three reasons, in order of weight:

1. **It is needed by more than this probe.** Every future keyed-latency measurement needs it; folding
   it into #2827 hides a general capability inside a probe's scope.
2. **It is where the subtle mechanism lives, and that deserves its own review.** Tokens do not route
   — `detect_route(filter, schema)` (`cqlite-flight/src/point_read.rs:67`) decides the point route
   from the typed predicate tree plus the schema partition key alone, and token bounds are pruning
   applied *afterwards* (`cqlite-flight/src/producer_point.rs:130-135`; see the comment at
   `:186-193`). A ticket with correct key-derived Murmur3 bounds and **no `filter`** still resolves to
   `full_scan`, and the failure is silent: correct rows at scan latency, the bimodal p50 17 ms /
   p99 34 s symptom #2866 measured. The correct assertion is on
   **`streaming_partition_lookup`** (`cqlite-core/src/query/access_path.rs:126`) for a plain full-PK
   equality, or `multi_partition_lookup` (`:123`) for an IN-list — never bare `partition_lookup`
   (`:122`), which this route does not emit. Entangling that with an observability change makes one
   PR carry two independent correctness stories.
3. **This change does not need it.** Nothing in D1–D8 requires a keyed load generator; requiring one
   would make an instrument's delivery depend on a load tool's delivery for no gain.

**F2 (cross-language Murmur3 parity)** is likewise deferred and likewise not a dependency: this
change computes no tokens (D4 hashes raw key bytes). It is proposed because the coupling is real —
the Java golden vectors at `Murmur3TokenTest.java:120-128` are, per their own Javadoc (`:14-16`),
copied verbatim from `cqlite-core/src/util/cassandra_murmur3.rs:488-513`, and nothing mechanically
keeps them in step. F1 would make that coupling load-bearing (a client computing tokens from keys),
which is the moment it should be closed.

### D10 — The honesty clause, and the owner action this change does not take

**Decision.** Every artifact in this change states, in its own words and without hedging:

> #2827 as re-scoped delivers **the instrument and the procedure, not the field number**. Its original
> AC2 — "decides whether a 64–128 MiB decoded-partition cache clears a useful hit ratio" — is **not
> satisfied by this change**. It becomes satisfiable on the first real keyed workload run with the
> probe enabled. The reason it cannot be satisfied here is that **no field keyed workload with
> captured concentration exists** (`docs/research/phase2-verify-caching.md:214-216`).
>
> **Scope:** that holds for BTI and for BIG whose `Index.db` is already resident. The probe
> will not materialize an index to get an answer (it would defeat #2412's lazy open and change
> the process memory profile), so a Summary-guided BIG window is REFUSED rather than priced —
> as are a non-census window and one with a non-zero `unavailable` fraction. All three fail
> SAFE (a refusal is never a false "go"), but the FIRST window may well be refused.

**Owner instruction recorded, not executed.** The owner's standing instruction from the thread — *"the
issue must stop calling itself a gate"*, with the request to retitle/re-scope to reflect that the
output is an **input** rather than the go/no-go — is recorded verbatim in the spec and surfaced as a
NEEDS-YOU item. It is **not** acted on: CLAUDE.md reserves retitling and re-scoping an issue to the
owner. Recording it prevents the artifacts from silently re-inheriting the "gate" framing while the
issue title still carries it.

**Why the clause is a requirement and not a note.** Because the failure mode is a reader six months
out, not a reviewer today. The owner named it precisely: "a green sensitivity curve will later be read
as 'measured field skew' by someone who did not read this thread." The same hazard applies to a green
instrument: a merged, tested, documented probe reads as "we measured the skew" unless every artifact
says otherwise. So it is an auditable requirement with a scenario, not a paragraph in a proposal.

### D11 — Doctrine and catalog registration in the same change, including a bundled correction

**Decision.** The seven metrics are registered in `cqlite-core/src/observability/catalog.rs` (constants
+ `ALL_METRICS` at `:907`) and annotated in `operator_docs_annotations.rs`. The generator is
**fail-closed** — "a metric present in `ALL_METRICS` with no operator annotation" is rejected
(`cqlite-core/src/observability/operator_docs.rs:16-19`, `:116`) — so registration cannot ship
undocumented. Instruments are constructed in `otel.rs` alongside the existing read counters
(`:381-385`) and dispatched in `add_counter` (`:751`). The generated pages
`docs/reports/flight-metrics-reference.md` and
`website/src/content/docs/agents-using/flight-metrics-reference.md` (`operator_docs.rs:35`, `:40`) are
regenerated by `cargo run -p cqlite-core --example gen_operator_metrics_doc`; the
`operator-metrics-doc` and `kit-dashboard-drift` gate components (`scripts/agent-gate.sh:2033`) fail
closed on drift. `docs/observability/configuration.md` is hand-maintained and gains the seven rows plus
the two new attribute value-sets.

**Bundled correction, in scope because it is this change's own premise.** The proposal's central
factual claim is about `cqlite.read.partition_lookup.total`'s attribute set, and the tree states it
three ways:

| Source | Claimed attributes |
|---|---|
| `docs/observability/configuration.md:215` | `cqlite.result`, **`cqlite.query.access_path`**, `cqlite.sstable.format` |
| `cqlite-core/src/observability/otel.rs:384` (instrument description) | "keyed by {result, **access_path**}" |
| `catalog.rs:283` + every emission site (`partition_lookup.rs:87`, `:156`, `:353`, `:414`, `:440`) | `cqlite.result`, **`cqlite.read.lookup_route`** (`catalog.rs:82`), `cqlite.sstable.format` |

The code is authoritative; the doc and the instrument description are stale. Shipping a change whose
premise quotes the stale row without fixing it would leave the next reader with the same wrong fact.
Related and fixed in the same pass: `docs/observability/configuration.md:298` documents the
`cqlite.query.access_path` value set as `full_scan, partition_lookup, multi_partition_lookup,
clustering_slice, fallback_full_scan`, omitting **`streaming_partition_lookup`** and
**`metadata_partition_lookup`**, both of which `cqlite-core/src/query/access_path.rs:125-126` emits —
and `streaming_partition_lookup` is precisely the label a correct keyed point read reports, so the
omission is the documented source of the "assert on `partition_lookup`" mistake the owner flagged.

This correction is **documentation and a description string only** — no attribute is added, removed
or renamed on any existing metric, so no dashboard or alert changes behaviour.

## Doctrine compliance notes

- **No-heuristics (#28):** the instrument consumes values the read path already resolved from
  authoritative metadata (`PartitionLoc.data_size`, the resolved key bytes). Where a value is genuinely
  unknown (BTI size) it records `unavailable` rather than inferring one — D6. Nothing is inferred from
  byte content.
- **Memory target (<128 MB):** 0 when off, +3 MiB fixed when on — D4.
- **Zero-cost when off:** two independent gates. Compile-time, the `observability` feature (call sites
  always compiled, helpers no-op — `cqlite-core/src/observability/mod.rs:49-54`; the dependency
  isolation is itself guarded by `cqlite-core/tests/observability_no_otel_default.rs`). Runtime,
  `CQLITE_PARTITION_ACCESS_PROBE` default-off via a `OnceLock` plus a programmatic override, mirroring
  `CQLITE_READ_PATH` (`cqlite-core/src/query/select_executor/forcing.rs:42`, `:78-82`).
- **Wiring evidence:** the public surface is `cqlite_core::observability::partition_access`; the call
  chain is `select_executor` targeted path / Flight `point_read_keys` → `record_partition_access` →
  window close → `obs::add_counter`; the end-to-end test is a Flight `do_get` point-read test under
  `observability-testing` that reads back the emitted series.
- **Wall-clock in tests:** every correctness assertion drives `close_window()` explicitly; no test
  asserts on elapsed time (CLAUDE.md's `roborev-lints` / #2642 class) — D5.
- **File size (campsite rule):** the recorder is a new module
  (`cqlite-core/src/observability/partition_access.rs`); `catalog.rs`, `otel.rs` and
  `operator_docs_annotations.rs` grow by table entries only. `operator_docs_annotations.rs` and
  `catalog.rs` are already large — if either crosses its ratchet, split by responsibility per the
  campsite rule rather than setting `CQLITE_ALLOW_FILE_GROWTH=1` without a note.

## Follow-ups (named here, deliberately not built here)

- **F1 — keyed load mode for `tools/flight-loadgen`** (D9). Requires amending
  `tools/flight-loadgen/README.md:68-71`.
- **F2 — automated cross-language Murmur3 parity test** (D9).
- **F3 — measure the decode multiplier `m`**, removing the last assumption from D7's procedure.
- ~~**F4 — BTI partition size via successor offset**~~ — **DELIVERED in this change** by the
  amended D6 (owner ruling, 2026-08-06), for BOTH formats rather than BTI alone, because the
  premise that BIG had a size was false. It is exact, not an estimate: the successor offset is the
  partition's exclusive end in the index's own layout.
