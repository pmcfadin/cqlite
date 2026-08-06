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
conditional. Refusal condition 4 below exists to enforce that.

## Inputs

One CLOSED window of the probe (`CQLITE_PARTITION_ACCESS_PROBE=1`), read from the emitted
series or from `WindowSummary`:

| symbol | series | meaning |
|---|---|---|
| `n_b` | `cqlite.read.partition_access.distinct_partitions{repeat_bucket=b}` | distinct partitions in bucket `b` |
| `a_b` | `cqlite.read.partition_access.accesses{repeat_bucket=b}` | accesses attributable to bucket `b` (the sum of its members' repeat counts) |
| `B_b` | `cqlite.read.partition_access.bytes{repeat_bucket=b}` | distinct-partition **on-disk** bytes in bucket `b` |
| `2^k` | `cqlite.read.partition_access.sample_denominator` | sampling scale; `1` = census |
| — | `distinct_partitions{size_source="unavailable"}` | partitions whose extent could not be measured |

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

Each yields **no answer**, never a default verdict. Check all four before computing anything.

1. **A non-zero `unavailable` fraction.** The byte total is incomplete by an unknown amount,
   so any budget-filling arithmetic overstates what fits. **Refuse**, naming the fraction.
   This tests *incompleteness*, not provenance: a window whose bytes are entirely
   `successor_gap` is complete and proceeds.
2. **The window reached the sampling floor** (`sample_denominator` at the cap, reported as
   non-census). The surviving sample is statistically worthless. **Refuse.**
3. **`A` below 10,000 accesses.** This is not a workload. **Refuse.**
4. **The window came from synthetic or self-generated load** — a test, an in-repo rig, a
   loadgen run. Its output may be recorded as an **instrument self-check** and may **NEVER**
   be cited as the go/no-go, because the answer would be a function of a distribution we
   chose. **Refuse.**

The procedure additionally REFUSES a window that is not a census
(`sample_denominator > 1`) and one reporting a non-zero `dropped_accesses`. Both are
consequences of the conditions already stated rather than new policy: a sample's
per-bucket bytes are sample-domain totals, so filling a real budget against them
overstates what fits; and only keys not already in the table can be dropped, so a
loss suppresses the singleton bucket and overstates concentration. Scaling a sample
by `2^k` was rejected in favour of refusing: it yields a point estimate whose
variance this instrument cannot bound, and the output is a go/no-go rather than an
interval.

A window that survives every condition is priceable. Under sampling (`2^k > 1`) the bucket *fractions* remain unbiased with no correction — the
admission predicate is a function of the key hash alone, hence independent of a key's access
frequency — so a sampled window is still worth reading for SHAPE. It is not priceable: absolute
`n_b` and `B_b` are sample-domain totals, which is why such a window is refused above rather
than scaled.

## Step 2 — the clairvoyant hit-ratio ceiling

Order buckets by **access density** `a_b / B_b` (accesses per on-disk byte), descending: the
best bytes to spend the budget on are the ones serving the most accesses. With a decoded
budget `C` (64 MiB or 128 MiB), the on-disk bytes that fit are `C / m`. Fill greedily, taking
buckets whole while they fit and the last one fractionally by byte share `f`:

```
H_max(C) = [ Σ_{fully-taken b} (a_b − n_b)  +  f · (a_last − n_last) ] / A
```

Each selected partition's **first** access in the window is compulsory (hence `− n_b`); every
subsequent access hits.

**This is an UPPER bound.** It assumes a clairvoyant (Belady) cache that already holds
exactly the right partitions. A real LRU cache does strictly worse. So the asymmetry is
load-bearing:

- a **low** `H_max` is a **sound no-go** — no cache policy can beat the ceiling;
- a **high** `H_max` is **necessary but not sufficient** for a "go". It is a licence to
  simulate LRU against the captured window, **not** a licence to build.

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
on-CPU** against **LZ4 decompress + CRC at ~23%**. A cache whose *ceiling* is below 50% on a
≤~3% work share cannot move the end-to-end number by more than ~1.5%, which is under the
round harness's noise floor.

- `H_max(128 MiB) < threshold` → **sound NO-GO.** Record it and close the question.
- `H_max(128 MiB) ≥ threshold` → **worth a real LRU simulation against the captured
  window.** Not an automatic build; #2037 stays owner-gated.

Report both budgets (64 MiB and 128 MiB): if 64 MiB is already close to 128 MiB, the working
set is small and the cheaper cache is the one to model.

## Worked example — an INSTRUMENT SELF-CHECK, never a field result

Computed from the validation test's known, constructed distribution
(`cqlite-core/tests/issue_2827_partition_access_bytes.rs`). **Refusal condition 4 applies to
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

# 2. Let a real workload run. The window is tumbling (60 s or 5,000,000 accesses,
#    whichever first); an operator can also close one deterministically in-process.

# 3. Read the four series off the dashboard / OTLP collector:
#      cqlite.read.partition_access.distinct_partitions{repeat_bucket,size_source}
#      cqlite.read.partition_access.accesses{repeat_bucket}
#      cqlite.read.partition_access.bytes{repeat_bucket}
#      cqlite.read.partition_access.sample_denominator
#      cqlite.read.partition_access.dropped_accesses    (0 on a healthy window)
#      cqlite.read.partition_access.sampling_floor      (0 on a healthy window)
```

Costs, so enabling it needs no further investigation: **zero bytes and one relaxed atomic
load when off** (the counting table is allocated lazily on first use), **exactly 3 MiB fixed**
when on — with no growth term in partition count, in qps, in window length, or in the
sampling scale.

The procedure is also available as code — `cqlite_core::observability::partition_access::decision`
— so a captured window can be priced without re-deriving any of the above. It implements
exactly the four refusal conditions and the ceiling in this note.

## Known coverage limitation of the instrument

`SELECT WRITETIME(col) / TTL(col) … WHERE pk = ?` takes a separate metadata point-read
boundary that the probe does **not** record, so those accesses never reach the
histogram. The direction is conservative — the affected partitions are under-counted,
which understates concentration — but if the workload you instrumented is
predominantly WRITETIME/TTL projections, **do not use its window for the decision**:
the measurement would be of the traffic that happens to be visible, not of the
workload.

## What this note does NOT do

- It does not report a field skew. See the scope statement at the top.
- It does not build, size, wire, configure or benchmark a decoded-partition cache. #2827
  stays decoupled from #2037.
- It contains no hit-ratio-vs-skew sensitivity curve, and no hit-ratio number derived from a
  distribution chosen by this change other than the explicitly-labelled self-check above.
