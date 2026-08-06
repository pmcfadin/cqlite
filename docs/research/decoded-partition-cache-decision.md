# Decision procedure: is a 64–128 MiB decoded-partition cache worth building?

**Status:** the PROCEDURE, not the verdict · **Issue:** #2827 · **Related:** #2037 (the
owner-gated cache build this stays decoupled from), #2059/#1570 (the key→offset cache the
extents come from), #2818 (the Arm-1 CPU decomposition that prices the lever), #2866 (the
bench round whose keyed gap is why no field distribution exists), #3330 (F1, keyed loadgen)

## Read this first — what is delivered, and what is not

This note plus the bounded partition access-distribution probe deliver **the instrument and
the procedure, not the field number**. Issue #2827's original AC2 — "decides whether a
64–128 MiB decoded-partition cache clears a useful hit ratio" — is **NOT satisfied**. Not
waived, and not deferred to another issue: it becomes satisfiable on **the first real keyed
workload run with the probe enabled**, because everything needed to turn one closed window
into the verdict is written down below.

**Scope of that promise, stated honestly.** It holds for BTI, and for BIG whose `Index.db` is
already resident. It does **not** hold universally: the probe will not materialize an index to
get an answer — doing so would defeat the lazy Summary-guided open of #2412 and permanently
change the process memory profile — so under a Summary-guided BIG read a window is REFUSED
rather than priced. Two other refusals bite for the same reason: a non-census window, and a
non-zero `unavailable` fraction. All three fail in the SAFE direction (a refusal is never a
false "go"), but an operator should expect that the FIRST window may be refused, and that
obtaining a priceable one can take a shorter window, a resident index, or both.

The reason it cannot be satisfied here is not a shortcut. **No field keyed workload with
captured concentration exists.** The only keyed loadtest on record is ~0.9 qps aggregate,
~30 rows/s, with no reported hot-set concentration — three orders of magnitude below the
A2 ≥1,000 qps/pod target the cache is being justified against
(`docs/research/phase2-verify-caching.md:214-216`), and there is no Zipf/skew measurement of
the field keyed workload anywhere in `docs/` (`:212-213`).

**Nothing produced by the probe or by this note may be cited as a measured field skew, as
the go/no-go, or as a gate.** There is deliberately no hit-ratio-vs-skew sensitivity curve
here: a curve over a distribution *we* chose converts a measurement into an owner judgment
about an assumed skew, and `phase2-verify-caching.md:220-227` already records that
conditional. Refusal condition 6 below (synthetic or self-generated load) exists to enforce that.

## Inputs

One CLOSED window of the probe (`CQLITE_PARTITION_ACCESS_PROBE=1`), taken from a
[`WindowSummary`] — **not** read off a dashboard.
`distinct_partitions`, `accesses` and `bytes` are CUMULATIVE COUNTERS, so a collector read
of them after N closed windows is the sum over all N; see "How to run it" below for why that
produces a false no-go. The series column names where each input appears when exactly one
window has closed, which is the only case in which the two agree:

| symbol | series (single-window read only) | meaning |
|---|---|---|
| `n_b` | `cqlite.read.partition_access.distinct_partitions{repeat_bucket=b}` | distinct partitions in bucket `b` |
| `a_b` | `cqlite.read.partition_access.accesses{repeat_bucket=b}` | accesses attributable to bucket `b` (the sum of its members' repeat counts) |
| `B_b` | `cqlite.read.partition_access.bytes{repeat_bucket=b}` | distinct-partition **on-disk** bytes in bucket `b` |
| `2^k` | `cqlite.read.partition_access.sample_denominator` | sampling scale; `1` = census |
| — | `distinct_partitions{size_source="unavailable"}` | partitions whose extent could not be measured |
| — | `window_dropped_accesses`, `sampling_floor` | per-window GAUGES; these two are safe to read off a dashboard at any time |

Buckets `b` are exactly `1`, `2`, `3-4`, `5-8`, `9-16`, `17+`. `A = Σ a_b`.

### Where `B_b` comes from, and the one assumption

`B_b` is **measured**, not assumed. Each partition's on-disk extent is its successor gap
`[data_offset, successor_offset)` — authoritative index-layout metadata, the same bound the
single-partition seek uses to size its decompression window — with the last partition of an
SSTable bounding to the authoritative uncompressed data-section length. Measured extents are
labelled `size_source="successor_gap"`; `"index"` is reserved for a producer that genuinely
records a size (no Cassandra 5.0 index format does).

Two properties of that measurement, stated so nobody has to rediscover them:

- **The extents are UNCOMPRESSED offsets.** This is the correct domain: `m` below converts
  on-disk bytes to decoded bytes, and applying it to a *compressed* size would compound two
  ratios. A compressed table's measured bytes therefore exceed its file bytes; that is the
  measurement working, not an error. The production write surface is uncompressed-only
  anyway (#1406).
- **The last partition per SSTable includes any trailing data-section bytes**, since it is
  bounded by the section length rather than by a successor. One partition per SSTable per
  window.

**The single remaining ASSUMPTION is the decode multiplier `m`** — decoded bytes per on-disk
byte. Use **`m ≈ 3.5`**, whose provenance is the Phase-0 **wire estimate** at
`docs/research/phase2-verify-caching.md:221-222`. It is **an assumption, not a measurement**,
and it is labelled as such everywhere it appears. Measuring it is follow-up **F3**; until
then, report the verdict with the `m` you used and re-run the arithmetic at `m = 2` and
`m = 5` to see whether the verdict is even sensitive to it.

## Step 1 — the refusal conditions, checked FIRST

Each yields **no answer**, never a default verdict. Check every one before computing anything.
They are in the order the shipped evaluator applies them, so a window failing several is
diagnosed by the most fundamental.

0. **An out-of-domain input.** The decode multiplier must be finite and `> 0`; the go
   threshold must be finite and in `[0.0, 1.0]`. **Refuse** otherwise — a zero multiplier
   makes the on-disk budget infinite, so every bucket "fits" and the procedure reports a
   maximal hit ratio that clears any threshold: a false GO rather than a visible error.
1. **A non-zero `unavailable` fraction** (`distinct_partitions{size_source="unavailable"}`).
   The byte total is incomplete by an unknown amount, so any budget-filling arithmetic
   overstates what fits. **Refuse**, naming the fraction. This tests *incompleteness*, not
   provenance: a window whose bytes are entirely `successor_gap` is complete and proceeds.
2. **A non-zero `window_dropped_accesses`.** The recorder could not seat some accesses in its
   table, so the histogram is missing input — and only keys NOT already in the table can be
   dropped, which suppresses the singleton bucket and OVERSTATES concentration. **Refuse.**
3. **`sampling_floor = 1`.** The window reached the sampling-prefix cap; the surviving sample
   is statistically worthless. **Refuse.**
4. **`sample_denominator > 1`.** The window is a SAMPLE, not a census. Its per-bucket bytes
   are sample-domain totals, so filling a real budget against them prices the whole budget
   against `1/2^k` of the working set — a false "go". **Refuse.** Remedy: shorten the window
   (`CQLITE_PARTITION_ACCESS_WINDOW_SECS` / `..._WINDOW_ACCESSES`) until the distinct set fits
   the ~98,304-slot table, and re-measure.
5. **`A` below 10,000 accesses.** This is not a workload. **Refuse.**
6. **The window came from synthetic or self-generated load** — a test, an in-repo rig, a
   loadgen run. Its output may be recorded as an **instrument self-check** and may **NEVER**
   be cited as the go/no-go, because the answer would be a function of a distribution we
   chose. **Refuse.**
7. **No priced bytes at all.** Nothing to fill a budget with. **Refuse.**

A window is trustworthy on the emitted series alone exactly when
`window_dropped_accesses = 0`, `sampling_floor = 0` and `sample_denominator = 1`.

Scaling a sample by `2^k` was rejected in favour of refusing it (condition 4,
`sample_denominator > 1`): scaling
yields an unbiased point estimate of the population totals but says nothing about its
variance, and this procedure's output is a go/no-go rather than an interval — so a scaled
verdict would read exactly as authoritative as a census one while resting on an
extrapolation the instrument cannot bound.

A window that survives every condition is priceable. Under sampling (`2^k > 1`) the bucket *fractions* remain unbiased with no correction — the
admission predicate is a function of the key hash alone, hence independent of a key's access
frequency — so a sampled window is still worth reading for SHAPE. It is not priceable: absolute
`n_b` and `B_b` are sample-domain totals, which is why such a window is refused above rather
than scaled.

## Step 2 — the hit-ratio estimate (NOT a ceiling — see below)

Order buckets by **access density** `a_b / B_b` (accesses per on-disk byte), descending: the
best bytes to spend the budget on are the ones serving the most accesses. With a decoded
budget `C` (64 MiB or 128 MiB), the on-disk bytes that fit are `C / m`. Fill greedily, taking
buckets whole while they fit and the last one fractionally by byte share `f`:

```
H_max(C) = [ Σ_{fully-taken b} (a_b − n_b)  +  f · (a_last − n_last) ] / A
```

Each selected partition's **first** access in the window is compulsory (hence `− n_b`); every
subsequent access hits.

**This is NOT an upper bound.** `H_max` is an ESTIMATE UNDER A STATED RANKING HEURISTIC, not a ceiling. Buckets are
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

What remains true: the clairvoyance assumption is optimistic (a real LRU cache does
worse than a Belady one), so a **high** `H_max` is at most a licence to simulate LRU
against the captured window, never a licence to build.

**Known bias, accepted rather than argued away.** The measurement window is *tumbling*, so a
partition accessed on both sides of a boundary is recorded as two lower-repeat entries
instead of one high-repeat entry. The histogram therefore **understates** concentration and
`H_max` is, if anything, pessimistic. That is the safe direction for a go/no-go: a "go"
produced *despite* the bias is a stronger signal, and the bias shrinks with window length
(60 s at A2-scale qps is thousands of accesses per hot key).

## Step 3 — the verdict

**Recommended threshold: `H_max(128 MiB) ≥ 0.50`. This is an OWNER-SETTABLE PARAMETER,
recorded as such**, not a derived constant.

Its arithmetic, so the next reader does not re-litigate it: a decoded-partition cache targets
decode/merge work, and the Arm-1 CPU decomposition (#2818) measured **k-way merge at 3.2% of
on-CPU** against **LZ4 decompress + CRC at ~23%**. A cache whose *estimated hit ratio* is below 50% on a
≤~3% work share cannot move the end-to-end number by more than ~1.5%, which is under the
round harness's noise floor.

- `H_max(128 MiB) < threshold` → a **no-go INDICATION**, not a sound no-go: per the #3340
  correction above the estimate can err in either direction, so a value near the threshold
  decides nothing until #3340 lands.
- `H_max(128 MiB) ≥ threshold` → **worth a real LRU simulation against the captured
  window.** Not an automatic build; #2037 stays owner-gated.

Report both budgets (64 MiB and 128 MiB): if 64 MiB is already close to 128 MiB, the working
set is small and the cheaper cache is the one to model.

## Worked example — an INSTRUMENT SELF-CHECK, never a field result

Computed from the validation test's known, constructed distribution
(`cqlite-core/tests/issue_2827_partition_access_bytes.rs`). **Refusal condition 6 (synthetic or self-generated load) applies to
it**: this is here to show the arithmetic, and it is *not* evidence about any real workload.

Constructed window: 600 partitions accessed 20× each and 10,000 accessed once each, every
partition 1,024 on-disk bytes, all extents measured, census.

```
A     = 600·20 + 10_000            = 22_000 accesses
17+ : n=600,    a=12_000, B=614_400 B     density = 0.01953 acc/byte
1   : n=10_000, a=10_000, B=10_240_000 B  density = 0.00098 acc/byte

on-disk budget at C=128 MiB, m=3.5:  134_217_728 / 3.5 = 38_347_922 B
  both buckets fit (10_854_400 B total), so both are taken whole
served = (12_000 − 600) + (10_000 − 10_000) = 11_400
H_max(128 MiB) = 11_400 / 22_000 = 0.518
```

0.518 ≥ 0.50, so on a *real* window this shape would say "worth simulating LRU". Note how
little the singleton bucket contributes (zero hits for 94% of the bytes) — that is the whole
reason the bucket histogram is the right instrument: it separates the bytes worth caching
from the bytes that only dilute the budget.

## How to run it

```bash
# 1. Enable the probe on the process serving the real keyed workload. Default is OFF.
export CQLITE_PARTITION_ACCESS_PROBE=1

# 1b. If step 4 refuses the window as a non-census SAMPLE, shorten the window until
#     the distinct partition set fits the ~98,304-slot counting table. This is the
#     operator-side remedy and needs no code change.
export CQLITE_PARTITION_ACCESS_WINDOW_SECS=10        # default 60
export CQLITE_PARTITION_ACCESS_WINDOW_ACCESSES=50000 # default 5,000,000

# 2. Let a real workload run. The window is tumbling (60 s or 5,000,000 accesses,
#    whichever first); an operator can also close one deterministically in-process.

# 3. Price ONE window. Use the in-process evaluator, which takes a single
#    `WindowSummary`:
#
#      cqlite_core::observability::partition_access::{close_window, decision};
#      if let Some(w) = close_window() {
#          match decision::evaluate(&w, decision::WindowSource::Field,
#                                   128 * 1024 * 1024,
#                                   decision::ASSUMED_DECODE_MULTIPLIER) { .. }
#      }
```

**Do NOT price the bucketed series off a dashboard.**
`distinct_partitions`, `accesses` and `bytes` are COUNTERS: a collector read of them
on a process that has closed more than one window is the SUM OVER ALL WINDOWS since
start. Distinct partitions and bytes are then double-counted while the budget `C`
stays fixed, so `H_max` collapses roughly as `1/N` — a **false no-go**. Either price
one window in-process as above, or read the dashboard only on a run that closed
exactly one window.

What the dashboard IS for is the trustworthiness check, which is per-window by
construction and needs no arithmetic:

```text
cqlite.read.partition_access.sample_denominator      # 1 = census
cqlite.read.partition_access.window_dropped_accesses # per-window; 0 = nothing lost
cqlite.read.partition_access.sampling_floor          # per-window; 0 = not floored
cqlite.read.partition_access.dropped_accesses        # CUMULATIVE — "ever lost input",
                                                     # NOT the per-window signal
```

A window is trustworthy exactly when the first three read `1`, `0`, `0`. Reading the
cumulative counter as the per-window signal mis-refuses every window after the first
loss.

Costs, stated in full so enabling it needs no further investigation.

**Off (the default): zero.** No allocation at all — the counting table is allocated lazily
on first use — and the hot path is one relaxed atomic load.

**On, the instrument's own footprint: exactly 3 MiB, fixed** — no growth term in partition
count, in qps, in window length, or in the sampling scale.

**On, what pricing costs the read path** — this is NOT zero, and it is not covered by the
3 MiB above:

- Per access, per candidate generation, the probe resolves the partition's extent. BTI pays
  O(trie depth) — one strict-ceiling walk. **BIG pays O(partition count)**: the successor
  offset is the minimum `Index.db` `data_offset` strictly greater than the target, taken over
  every entry. On a million-partition table across `k` generations that is ~`k` million
  iterations per point read.
- The probe **will not materialize an `Index.db`** to get an answer — doing so would defeat
  the lazy Summary-guided open (#2412) and permanently add resident index bytes to the
  process. So a BIG generation whose index is not already resident is reported
  `size_source = unavailable`, and the window is refused rather than priced.

Both are why the probe is default-OFF and is meant to be switched on for a measurement
window, not left on.

The procedure is also available as code — `cqlite_core::observability::partition_access::decision`
— so a captured window can be priced without re-deriving any of the above. It implements
exactly the refusal conditions and the `H_max` estimate in this note.

## Known coverage limitation of the instrument

**Formerly a coverage gap, now closed.** The WRITETIME/TTL projection point read
(`scan_partition_with_cell_metadata`) IS recorded, like every other logical point read.
It was omitted at first on the reasoning that the omission was "conservative"; that
reasoning was **wrong**, and the correction is worth stating because the failure mode is
counter-intuitive. An unrecorded access leaves the **denominator** as well as the
numerator, so dropping a workload's metadata singletons while keeping its repeat traffic
*raises* `H_max`: 1M metadata singletons beside 100 partitions read 100 times each measure
≈0.99 against a true ≈0.0098 — a confident false "go", not a safe under-count. Nothing
needs to be discounted for it now.

What DOES still limit priceability is listed under the refusal conditions above (an
unmeasurable extent, a sample rather than a census, a lossy window) and in the scope note
at the top: a BIG generation whose `Index.db` is not resident cannot be priced.

## Known accuracy limitations of the measurement

Two, and they push in OPPOSITE directions, so neither cancels the other. Read them
alongside the #3340 ranking caveat in Step 2 before acting on any value.

- **The generation set is captured just before the read, not shared with it, so a
  generation created in between is priced by neither** — a flush, or a compaction
  output, landing in that window. The affected partition is UNDER-priced, and
  under-pricing bytes lets more buckets fit the budget, so this **RAISES `H_max`**:
  a bias in the UNSAFE direction. It is narrow (bounded by the gap between two
  snapshot acquisitions on one read) but it is not zero, and closing it properly
  requires threading the read's own snapshot out of the SSTable manager.
- **The tumbling-window boundary splits a partition accessed on both sides into two
  lower-repeat entries**, which UNDERSTATES concentration and so LOWERS `H_max` — the
  safe direction, accepted for that reason. It shrinks as the window lengthens.

## What this note does NOT do

- It does not report a field skew. See the scope statement at the top.
- It does not build, size, wire, configure or benchmark a decoded-partition cache. #2827
  stays decoupled from #2037.
- It contains no hit-ratio-vs-skew sensitivity curve, and no hit-ratio number derived from a
  distribution chosen by this change other than the explicitly-labelled self-check above.
