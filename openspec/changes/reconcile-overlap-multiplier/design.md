# Design: reconcile-overlap-multiplier (issue #2043 / M9)

## D1 — Measure at the public `KWayMerger` surface, not a new seam on the private reconcile fn

**Chosen.** Drive the bench through the already-`pub` constructors —
`KWayMerger::new_from_readers` (`merge/from_readers.rs:302`) with `with_now_secs`
(`merge/mod.rs:2622`) — over **k** real flushed generations, timing the full drain.

**What it beat.** Exposing `reconcile_cluster_with_overlap_counted` (`merge/mod.rs:4175`, private;
its wrappers at :4120/:4145 are `#[cfg(test)]` and therefore invisible to a `benches/` integration
target) as `#[doc(hidden)] pub`.

**Why.** Two reasons, the second decisive:

1. It would add a public surface purely to bench it — the change is explicitly a no-production-change
   spike, and a new `pub` item on the hot reconcile path invites exactly the review finding we would
   have to argue away.
2. **It would measure the wrong quantity.** The §3 term being tightened is a *whole-scan* derate: how
   per-row cost grows when a cluster spans k generations. That growth includes `BinaryHeap` refill
   (`refill_heap`, `mod.rs:2979`), cluster assembly, and `MergeEntry` construction — all of which scale
   with k — not only the `ReconcileState` pipeline. Isolating the reconcile call would understate the
   multiplier and produce a number that cannot be substituted into §3.

**Attribution without a private seam.** The reconcile path already counts overlap and purges
(`reconcile_cluster_with_overlap_counted`'s `PurgeCounts` out-param). The bench reports
**collisions-per-row alongside ns/row**, so cost growth is attributable to real collision density
rather than merely to input size — recovering most of what a micro-seam would have given us.

## D2 — Pin `now` through the API, never the env var (correctness trap)

**Chosen.** `KWayMerger::with_now_secs(Some(PINNED_NOW_SECS))`.

**The trap.** The read-path seam `CQLITE_TTL_NOW_OVERRIDE_SECS`
(`reader/parsing/row_decoder/now_clock.rs:61`) is `#[cfg(debug_assertions)]`. `cargo bench` builds
**release**, where that seam compiles out entirely and an absent/invalid value **silently falls back
to wall clock**. A bench pinned that way would silently drift: TTL cells would expire or not depending
on when the bench ran, so the expiring-TTL arm's ns/row would be irreproducible while still looking
green. In-repo precedent for the same choice: `cqlite-core/tests/issue_1849_multigen_tombstone_ttl_shadow.rs:49`
notes it uses the API specifically so behavior is identical in debug and release.

`None` is a strict no-op (no expiry), so the no-TTL arms pass `None` rather than a far-future pin —
keeping the TTL machinery genuinely out of those measurements.

## D3 — Fixture: generalize the existing same-key multi-generation builder

**Chosen.** Add a **k-parameterized** builder to the shared bench fixtures
(`cqlite-core/benches/fixtures/mod.rs`), built on the existing `open_write_engine()` (:340) +
`seeded_rng()` (:40) so it is deterministic and needs no vendored data.

`benches/compaction.rs::build_tombstone_heavy` (:307) already writes the *same* `(pk, ck)` in every
generation with ascending timestamps — precisely the overlap shape — but at a fixed `L0_SSTABLES`.
The generalization is the k parameter plus a collision-mix selector; no new I/O machinery.

**Matrix:** k ∈ {1, 2, 5, 10, 20} × mix ∈ {`disjoint`, `lww_overwrite`, `tombstone`, `ttl_expiring`,
`field_blend`}. k=1 anchors the curve against the published ~2.0 µs/row singleton figure — if the
`disjoint`/k=1 arm does not land near it, the harness is wrong and the run is void (see spec).

**Why synthetic and not `CQLITE_DATASETS_ROOT`.** Controlled k is the entire independent variable, and
the vendored corpus is single-generation — it cannot supply k>1 at all. The bench therefore synthesizes
and does **not** depend on the fetched datasets. This is a deliberate departure from the read benches;
it does not weaken the "never 0-row-pass" rule, which the spec instead enforces as a positive row-count
assertion per arm.

## D4 — Advisory perf-gate registration only

**Chosen.** Register the new bench IDs under `advisory_benches` in `cqlite-core/benches/perf-gate.json`
(the file's own contract: advisory entries "document cost, not block merges"), and add **no**
`threshold_pct` entry.

**Why.** A measurement instrument whose numbers are expected to move with fixture tuning would produce
false CI blocks. It also keeps us clear of the known review-blocker class: wall-clock thresholds
asserted in the correctness path.

## D5 — The field point is an assumption, and it is labeled as one

**Chosen.** Ship the measured curve + an STCS-derived expected-k band, and mark the field multiplier
**assumption-not-measurement** wherever it feeds §3. The record states the k the tightened derate
assumes, so a later M0 (#2818) rig measurement can substitute a real k without re-deriving anything.

**What it beat.** Blocking #2043 on M0/#2818 until the rig reports field cluster shape.

**Why.** M0 is unscheduled, and M7 (#2822) is blocked on *this* issue — blocking here stalls a chain
for a term we can already bound. The curve is the durable artifact; only the point selection is
provisional. **L3's disposition is therefore resolved conditionally:** the record gives the cost ratio
and the eligibility arithmetic, and states which k-band makes `P2:stage2`'s ~1.20× versus
`P2:row-engine`'s ~1.03–1.08× correct — so the moment field k is known, the disposition follows
without further measurement.

**This is the design point most worth the owner's pushback**; the issue's acceptance says "resolves the
L3 disposition," and what is deliverable locally is a conditional resolution.

## Risk

The box is currently under a concurrent full gate (load avg ~25–70). Benchmark numbers taken under
that load are worthless. The spec requires the run to record load average and reject results taken
above a stated ceiling — measurement runs wait for a quiesced machine.
