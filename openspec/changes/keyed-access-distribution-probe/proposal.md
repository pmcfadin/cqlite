# Proposal: Bounded partition repeat-access instrument + a written cache-sizing decision procedure (issue #2827)

**Milestone:** 0.17 (epic #2817, throughput program, manifest item M13 —
`docs/architecture/throughput-program-2026-07.md:344`, `:504`) · **Priority:** P2 ·
**Routing:** design-driven (a new observability surface + a decision procedure; there is no on-disk
oracle for "what shape is a workload's access distribution", so this is latitude, not parity work) ·
**Issue:** #2827 ·
**Related:** #2037 (owner-gated cache build — this change stays decoupled from it, per AC3),
#2059 / #1570 (the global key→offset cache whose `PartitionLoc` supplies the byte weights),
#2412 (Summary-guided BIG point path the instrument sits behind), #2866 (the direct-flight bench round
whose keyed gap is why no field distribution exists), #2818 (Arm-1 CPU decomposition that reprices the
lever).

## Scope reset — read this first

This change ships **the instrument and the decision procedure**. It does **NOT** ship a field
skew number, and it does **NOT** decide the 64–128 MiB decoded-cache go/no-go.

Issue #2827's AC2 ("decides whether a 64–128MiB decoded-partition cache clears a useful hit ratio")
is therefore **NOT SATISFIED by this change** — see the AC table at the top of
`specs/partition-access-distribution/spec.md`. It is not waived and not deferred to a different
issue: it becomes **satisfiable on the first real workload that runs with the probe enabled**, and
the reason it cannot be satisfied here is stated plainly below. No artifact in this change may claim
otherwise.

**Owner instruction carried forward (issue comment, 2026-07-26):** *"the issue must stop calling
itself a gate."* Retitling / re-scoping the GitHub issue is an **owner action**; the worker does not
take it unilaterally (CLAUDE.md: *never change an issue's scope/title … without the owner*). The
instruction is recorded here and in `specs/…/spec.md` so the artifacts do not silently re-inherit the
"gate" framing, and it is surfaced as a NEEDS-YOU item at merge.

## Why

### 1. The method the issue was filed with is dead, and it was verified dead

`cqlite.read.partition_lookup.total` carries only **bounded** attributes. The published catalog row
(`docs/observability/configuration.md:215`) lists `cqlite.result`, `cqlite.query.access_path`,
`cqlite.sstable.format`; the code's authoritative doc (`cqlite-core/src/observability/catalog.rs:283`)
and every emission site (`cqlite-core/src/storage/sstable/reader/partition_lookup.rs:82-90` and its five siblings at
`:128`, `:152`, `:349`, `:410`, `:436`) actually attach
`cqlite.result` / `cqlite.read.lookup_route` / `cqlite.sstable.format`. Either way there is **no
per-partition label**, and the catalog forbids one outright:

> "Unbounded values (raw error messages, **partition keys**, full query text) are **NEVER** attached
> as attributes or span fields." — `docs/observability/configuration.md:304-305`
> (mirrored in code at `cqlite-core/src/observability/catalog.rs:19-24`)

A Zipf/skew parameter is not reconstructable from a hit/miss-by-route counter. The framing the issue
was filed with cannot produce its own acceptance criterion.

### 2. There is no field data source, so the obvious replacement is circular

The only field keyed loadtest on record is **~0.9 qps aggregate, ~30 rows/s, with no reported hot-set
concentration** — three orders of magnitude below the A2 ≥1,000 qps/pod target the cache is being
justified against (`docs/research/phase2-verify-caching.md:214-216`). There is no Zipf/skew
measurement of the field keyed workload anywhere in `docs/` (`:212-213`).

The replacement first proposed — drive a synthetic Zipf sweep and publish a hit-ratio-vs-skew curve —
was rejected by the owner in the same thread, and the reason generalises:

> "A curve reading 's=1.2 → 70% hit at 128 MiB; uniform → 3%' decides nothing unless we know where
> the real workload sits. It converts a *measurement* into an *owner judgment about an assumed skew*
> … A sensitivity curve restates that conditional with more decimal places."

That is a **circular oracle**: the answer is a function of a distribution we chose. It is the same
defect class CLAUDE.md names for CQLite-written/CQLite-read round-trip tests — *both sides make the
identical assumption, so the artefact closes on itself and validates nothing anyone cares about.*
`phase2-verify-caching.md:220-227` already records the conditional ("the multiplier is real IF the
skew is real, and we don't know the skew"); a synthetic curve would restate it, not close it.

### 3. The owner named the non-circular alternative, and it is cheap

> "Instrument a **bounded histogram of repeat-access counts per partition** (buckets 1, 2, 3–4, 5–8,
> 9–16, 17+) over a sliding window. That yields the concentration shape — which is all the
> cache-sizing math needs — with fixed cardinality and no key labels, fully inside the existing
> observability rules. Small code change, and it turns this issue back into a measurement rather than
> a parameter sweep."

This change builds exactly that, plus the two things that make its output *decisive* rather than
merely interesting: **measured working-set bytes** (so only the decode multiplier remains an
assumption) and a **written-down closed-form decision procedure** (so the go/no-go falls out of the
first real window with no further rig round).

### 4. Nothing in the tree does any of this today

Verified by search across the workspace:

- **No decoded-partition cache exists.** The two real caches are `DecompressedChunkCache`
  (`cqlite-core/src/storage/cache/mod.rs:135`) and `GlobalKeyOffsetCache`
  (`cqlite-core/src/storage/cache/global_key_offset.rs:329`, 64 MiB default at `:213`).
  `PartitionKeyCache` (`cqlite-core/src/query/select_executor/row_build.rs:125`) is a size-1 memo of
  the last partition's decoded key columns, not a hot-set cache.
- **No measurement of decoded partition size.** The ~3.5× decoded/on-disk ratio at
  `docs/research/phase2-verify-caching.md:221-222` is an explicitly-labelled Phase-0 *wire estimate*.
- **No Zipf generator, no cache simulator, no hit-ratio-vs-skew machinery** anywhere
  (`grep -rn 'zipf\|count_min\|CountMin'` over the workspace returns nothing outside an unrelated
  `hyperloglog` format-string in `cqlite-cli/tests/compatibility/src/format_detective.rs:119`).
- **`tools/flight-loadgen` cannot drive a keyed workload.** `Shape::Point`
  (`tools/flight-loadgen/src/shape.rs:191-202`) draws a seeded random `i64` from the full ring and
  sets `token_start`/`token_end` only — it never sets `filter`, so its distribution is **uniform by
  construction**, the one distribution that makes a hot-set study vacuous. Its own README says so at
  `tools/flight-loadgen/README.md:68-71`.

## What Changes

1. **A fixed-cardinality partition repeat-access histogram on the read path.** Four new catalog
   metrics under `cqlite.read.partition_access.*`, carrying a new bounded attribute
   `cqlite.read.repeat_bucket` over exactly the owner's six buckets `1 | 2 | 3-4 | 5-8 | 9-16 | 17+`
   and a new bounded `cqlite.read.size_source` ∈ `index | unavailable`. **No per-key attribute**, ever —
   `docs/observability/configuration.md:304-305` is the binding constraint, and 6 × 2 = 12 series per
   counter is the whole cardinality budget. Composition with the existing bounded attributes is
   spelled out in `design.md` D3 (the instrument deliberately does **not** carry
   `cqlite.sstable.format`, because it counts *logical* partition accesses, not per-SSTable probes).

2. **Fixed-memory repeat counting over a tumbling window.** One lazily-allocated open-addressed table
   of `2^17` slots × 24 B = **exactly 3 MiB, fixed**, independent of partition count (the field scale
   is 1.93 M partitions/node, `docs/research/phase2-verify-caching.md:212`, `:224`) and independent of
   qps. Overflow is handled by **adaptive hash-prefix downsampling** (halve the admitted key space,
   keep exact counts for survivors, double a declared `sample_denominator`), never by eviction —
   design rationale and the bias analysis for both the sampling and the window boundary are in
   `design.md` D4/D5. The probe is **OFF by default** at runtime (`CQLITE_PARTITION_ACCESS_PROBE`,
   mirroring the `CQLITE_READ_PATH` `OnceLock`-plus-programmatic-override pattern at
   `cqlite-core/src/query/select_executor/forcing.rs:42`, `:78-82`) and its *export* is gated by the
   existing `observability` feature, whose call sites are always compiled and no-op when off
   (`cqlite-core/src/observability/mod.rs:49-54`).

3. **Byte weighting, so the working set is measured rather than assumed.** Distinct-partition on-disk
   bytes accumulate per bucket from `PartitionLoc.data_size`
   (`cqlite-core/src/storage/cache/global_key_offset.rs:76-81`). This turns the working-set question
   from two unknowns (skew × size) into one (the decode multiplier).
   **BTI fails closed, it does not under-report:** BTI trie resolution stores `data_size = 0`
   (`global_key_offset.rs:72-74`, `:94-100`; the BTI lookup returns an offset with no size at
   `partition_lookup.rs:433` (`Ok(Some(header.data_position))`, no size)), so a BTI-resolved access is recorded as
   `size_source=unavailable` and contributes **zero** bytes while still being counted as a
   partition — making the byte total's incompleteness *visible as a ratio* rather than silently
   absorbed. The decision procedure REFUSES to emit a go/no-go from a window with a non-zero
   `unavailable` fraction.

4. **Validation against a KNOWN input distribution.** An in-repo test drives a controlled access
   sequence (a uniform one and a skewed one) through the instrumented surface and asserts the
   recovered bucket histogram matches the known concentration exactly. `design.md` D8 argues why a
   synthetic input is a **legitimate** oracle for *this* claim (we are validating that the instrument
   recovers a distribution we control — a property of the instrument) and **illegitimate** for a claim
   about field skew (a property of the world), citing CLAUDE.md's two-parity-oracles and
   round-trip-invariance doctrine.

5. **The decision procedure, written down** in `docs/research/decoded-partition-cache-decision.md`:
   given the bucket table + measured bytes + a decode multiplier `m`, the closed-form
   clairvoyant/LRU hit-ratio bound at 64 and 128 MiB, the refusal conditions, and the recommended
   go/no-go threshold (an owner-settable parameter, recorded as such). The point is that the moment
   any real workload is instrumented, the go/no-go **falls out** — no further rig round, no further
   issue.

6. **The honesty clause in every artifact.** #2827 as re-scoped delivers the instrument and the
   procedure, not the field number; original AC2 is unmet; the owner's "stop calling itself the gate"
   instruction is recorded and left as an owner action.

7. **Doctrine and catalog in the same change.** The new metrics are registered in
   `cqlite-core/src/observability/catalog.rs` (`ALL_METRICS`) and annotated in
   `operator_docs_annotations.rs` — the generator is **fail-closed**, so an unannotated metric cannot
   ship (`cqlite-core/src/observability/operator_docs.rs:16-19`, `:116`) — and the published tables
   `docs/observability/configuration.md`, `docs/reports/flight-metrics-reference.md` and
   `website/src/content/docs/agents-using/flight-metrics-reference.md` are regenerated
   (`operator_docs.rs:35`, `:40`; gate components `operator-metrics-doc` and `kit-dashboard-drift`,
   `scripts/agent-gate.sh:2033`).
   **Bundled correction, in the same change:** `docs/observability/configuration.md:215` and the OTel
   instrument description at `cqlite-core/src/observability/otel.rs:384` both claim
   `cqlite.read.partition_lookup.total` is keyed by `cqlite.query.access_path`; the code emits
   `cqlite.read.lookup_route` (`catalog.rs:82`, `:283`; `partition_lookup.rs:87`). Since this change's
   entire premise is a claim about that metric's attribute set, shipping the corrected fact is in
   scope. `docs/observability/configuration.md:298` likewise omits `streaming_partition_lookup` and
   `metadata_partition_lookup` from the documented `cqlite.query.access_path` value set that
   `cqlite-core/src/query/access_path.rs:126` and `:125` emit.

## Non-goals

- **Not a synthetic field round.** No Zipf sweep, no hit-ratio-vs-skew curve, no sensitivity table.
  The owner's critique is that such a curve is an owner judgment wearing a measurement's clothes;
  building one anyway would be building the thing that was rejected.
- **Not the field skew number, and not the go/no-go.** AC1 is delivered only as *the instrument that
  reports it*; AC2 is **not delivered at all**. Nothing here may be cited as "measured field skew".
- **Not the decoded-partition cache.** No cache is built, sized, wired or benchmarked. #2827 stays
  decoupled from #2037 (AC3), and the K-A build stays owner-gated.
- **Not a keyed load mode for `tools/flight-loadgen`.** Tracked as follow-up **F1**, filed as issue #3330.
- **Not a cross-language Murmur3 parity test.** Proposed as follow-up **F2** below.
- **Not a decoded-size measurement.** The decode multiplier `m` stays an input to the procedure with
  a cited provenance (the Phase-0 wire estimate at `phase2-verify-caching.md:221-222`), explicitly
  labelled the one remaining assumption. Measuring it is out of scope and named as **F3**.
- **Not retitling or re-scoping issue #2827.** Owner action; recorded, not taken.
- **No on-disk format work, no decode-path change, no no-heuristics surface.** The instrument reads
  values the read path already resolved; it infers nothing from bytes.

## Follow-ups (stated, not specified here)

- **F1 — a keyed load mode for `tools/flight-loadgen` (issue #3330).** A `KeyedZipf`-style `Shape` over an
  operator-supplied partition-key corpus that sets a **full-PK equality `FlightTicket::filter`**
  (`cqlite-flight/src/ticket.rs:259`) *and* key-derived Murmur3 token bounds (`:240`, `:243`) as a
  consistent accompaniment, asserted to emit `streaming_partition_lookup`
  (`cqlite-core/src/query/access_path.rs:126`). The mechanism note matters and is easy to get wrong:
  **tokens do not route; the filter does.** `detect_route(filter, schema)`
  (`cqlite-flight/src/point_read.rs:67`) decides the point route from the typed predicate tree plus
  the schema partition key alone, and token bounds are a *pruning guard applied afterwards*
  (`cqlite-flight/src/producer_point.rs:130-135`, and the comment at `:186-193`), so a ticket with
  correct token bounds and no `filter` still reports `full_scan` — correct rows at scan latency, the
  silent bimodal p50 17 ms / p99 34 s failure #2866 measured. Needed for **all** future keyed-latency
  work, not only this probe; it is also the natural end-to-end driver for exercising this
  instrument at load. Requires amending `tools/flight-loadgen/README.md:68-71`, which currently
  states the `point` shape is a request-setup/admission-cost proxy and that "a true keyed read needs
  a partition-key corpus (a follow-up)".
- **F2 — an automated cross-language Murmur3 parity test** (unfiled). A shared golden-vector file consumed by
  both suites, closing the hand-copy coupling: the Java vectors at
  `trino-connector/src/test/java/in/mcfad/cqlite/flight/Murmur3TokenTest.java:120-128` are, by their
  own Javadoc (`:14-16`), "copied verbatim from the Rust unit tests" at
  `cqlite-core/src/util/cassandra_murmur3.rs:488-513`. They agree today
  (`('hello',42) → 7666157718303755816`, `('world',99) → -4641306270390207264`) but nothing
  mechanically keeps them agreeing.
- **F3 — measure the decode multiplier** (unfiled). Replace the Phase-0 ~3.5× wire estimate with a measured
  decoded-bytes-per-on-disk-byte ratio on real fixtures, removing the last assumption from the
  decision procedure.

## Impact

- **New public surface:** `cqlite_core::observability::partition_access` — a recorder with an
  enable/disable control, a `record_partition_access` entry point, and a deterministic
  `close_window()` used by tests and by an operator-triggered flush.
- **Call sites:** the two **logical** point-read boundaries — the core `select_executor` targeted
  path (`cqlite-core/src/query/select_executor/lookup.rs:92`, consumed at `streaming.rs:107` and
  `stream_agg.rs:196`) and the Flight point path (`cqlite-flight/src/producer_point.rs:83` `point_read_keys` → `drive_merge`). Per-SSTable probe sites are deliberately **not** call sites; see
  `design.md` D2.
- **Catalog / docs:** `catalog.rs` (4 metric constants, 2 attribute constants, `ALL_METRICS`),
  `operator_docs_annotations.rs` (4 `MetricDoc` entries — fail-closed, mandatory), `otel.rs`
  (instrument construction + `add_counter`/`record_gauge` dispatch), regenerated
  `docs/reports/flight-metrics-reference.md` + `website/src/content/docs/agents-using/flight-metrics-reference.md`,
  hand-edited `docs/observability/configuration.md` (new rows + the two corrections above).
- **New research note:** `docs/research/decoded-partition-cache-decision.md`.
- **Memory budget (<128 MB):** **zero when off** (the table is allocated lazily on first enable);
  **+3 MiB fixed** when on, with no growth term in partition count, qps, or window length.
- **No-heuristics mandate (#28):** unaffected. The instrument records values the read path already
  resolved from authoritative metadata; it never infers a type, format or size from byte content, and
  where the size is genuinely unknown (BTI) it records `unavailable` rather than estimating one.
- **Public binding surfaces (Python/Node/CLI):** untouched. No CLI flag, no binding API.
- **Write path, compaction, on-disk format:** untouched.
