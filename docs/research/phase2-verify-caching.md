# Phase 2 — Adversarial verification of the caching packet (`phase1-4-caching.md`)

**Date:** 2026-07-21 · **Status:** research (uncommitted) · **Agent:** Phase-2 adversarial verifier ·
**Method:** READ-ONLY — code inspection (`cqlite-core/src`, `cqlite-flight/src`,
`trino-connector/src`), issue reads (`gh`), and cross-check against the sibling Phase-1 packets
(`phase1-1`, `phase1-2`, `phase1-3`, `phase1-5`, `phase1-6`) and MEMORY field rounds. No builds, no
writes, no GitHub writes.

Target under attack: `docs/research/phase1-4-caching.md`. Anchor: `phase0-scan-cost-breakdown-2026-07.md`.

---

## 0. Verdict summary

| # | Packet claim | Verdict | Severity ruling |
|---|--------------|---------|-----------------|
| 1 | **B4 idle-≤16 Mi "unmet by construction" (384 MiB of undrainable cache)** | **WRONG as stated — two independent errors** | S-D idle-drain is **P3 hardening**, NOT a P1 B4 gate. The real (peak) hazard is the 256 MiB chunk-cache default, a config retune. |
| 2 | **#2561 (BTI chunk-straddle whole-file fallback) — "fix #2561 first, K-B/K-C partially broken"** | **STALE — the BTI bug is already fixed on `main`** (landed in #2059's PR #2554). | **NOT a 0.17 slot.** P3 traceability-close; BIG-path follow-up is a verify task, not a live landmine. |
| 3 | **#2165 (wire scan decode through ChunkSource) — caching wants it wired, row-engine demoted it** | **Both right for different workloads** — no real conflict. | **Re-scope, don't close.** Keep for decode-plane maintainability; do NOT credit it a scan multiplier; the *cache* value is keyed/repeated-range only. |
| 4 | **K-A/K-D decoded-partition cache — "biggest keyed lever, ~1.5–3×"; don't file standalone (reconcile with #2037)** | **Lever direction is sound; the hit-rate model is UNSUPPORTED by field data.** #2037 coupling is **half-right**. | Real A2 lever, but **gate it on a measured keyed access distribution first**. Decouple the *measurement* from #2037; keep the *build* reconciled. |
| 5 | **Snapshot-reuse-window tuning is an owner freshness call** | **CONFIRMED correct.** | Lands on NEEDS-OWNER, not an autonomous filing. |

---

## 1. THE B4 IDLE-DRAIN CLAIM — the headline is wrong twice over

The packet's gating claim (lines 30–37, 182–184):

> "three shipped resident caches declare 256 MiB + 64 MiB + 64 MiB = 384 MiB … none drain at idle …
> **The idle-≤16 Mi half of B4 is therefore not met by construction today**, and every new cache
> worsens it."

**This is wrong on two independent grounds. It should not gate any lever.**

### Error 1 — B4 has no "idle" clause. "≤16 Mi" is the per-query working-set target.

I checked the ratified B4 definition against every place it is stated. **The caching packet is the
only one of the eight Phase-1 documents that reads "≤16 Mi" as an idle-pod ceiling.** Every sibling
reads it as a *per-query working-set* target:

- `phase1-6-parallelism.md:258` — "**B4 = ≤3 s / ≤512Mi pod / ≤16Mi (per-query working set target)**."
- `phase1-6-parallelism.md:276` — "a single stream already consumes ~15 MB, essentially the whole
  ≤16Mi **per-query** target on its own — the ≤16Mi number is only satisfiable at concurrency 1 or
  with a smaller batch_size."
- `phase1-5-transport-ingest.md:263,352` — "the `≤512 Mi`/`≤16 Mi` B4 envelope"; "worker heap ≤512 Mi
  / ≤16 Mi **[per-query]**."
- `phase1-3-linux-io.md:24,118` — the *third* B4 number the IO agent tracks is "**cold-start latency
  B4 ≤ 3 s**", not an idle byte count.
- MEMORY (ratified 2026-07-15): "**B4 ≤3s/≤512Mi/≤16Mi**" — three numbers: latency, pod peak,
  per-query working set. No "idle".

A full-tree grep for an idle memory target (`idle` + `mem/16/rss/drain/pod`, outside `phase1-4`)
returns **zero** hits in `docs/architecture` or `docs/reports`. The only other "idle" usages are the
#2037 research ("no background CPU when idle" — a *CPU* property) and the telemetry schema. **There is
no idle-memory clause in B4 to violate.** The packet reified a target that does not exist and then
declared it "unmet by construction."

The three shipped caches (256 + 64 + 64) are **shared amortized infrastructure counted against the
≤512 Mi PEAK**, not against the ≤16 Mi per-query number. The per-query number governs Arrow-egress
buffers per stream (`batch_size × channel-depth`), which is exactly what `phase1-6` sizes admission
against — nothing to do with resident caches.

### Error 2 — 384 MiB is a DECLARED CAP, not resident memory. Lazy caches sit near-zero.

Even under a (nonexistent) idle-memory clause, the "holds 384 MiB indefinitely" premise is false. All
three caches are **lazily populated, byte-bounded LRUs** — resident bytes = *touched* working set,
capped at the budget, never the budget itself:

- **Chunk cache** (`chunk_source.rs:80-149`): `chunk()` = `cache.get` → on miss decompress →
  `cache.insert`. Only chunks actually read are ever inserted.
- **Critically, the scan path inserts NOTHING.** Per #2165 itself, `iterate_all_partitions` /
  `sequential_scan` decode **bypasses `ChunkSource` entirely** (they still use the legacy `self.file`
  + `compression_reader` model). So the packet's own worked example — "a pod that serves **one scan**
  and goes quiet" — populates **~0 bytes** of the 256 MiB chunk cache, because a scan never touches
  B1. The scenario the packet uses to motivate the crisis is the one scenario where the chunk cache
  stays empty.
- **Global key cache** (64 MiB): populated only by *point-read* index resolution; a scan-only pod
  holds ~0 here too.
- **Warm registry** (64 MiB): holds `Arc<SSTableReader>` = open FDs + parsed Summary/lazy-BIG-index —
  tens of KB to low-MB per generation, a handful of generations. Nowhere near 64 MiB resident for the
  R12 2-generation corpus.

So a genuinely idle pod after a scan holds a few MB (warm readers), not 384 MiB. The "no drain" code
observation is **factually true** (I confirmed: no TTL/decay/idle-sweep on `DecompressedChunkCache`
or `GlobalKeyOffsetCache`; the `cache_ttl` hits in the tree are unrelated legacy caches —
`cql/config.rs`, `schema_discovery.rs`, `sstable_data_manager.rs`), but "no drain" ≠ "384 MiB
resident at idle."

### Field evidence — the pod PEAK is met in practice, and idle was never measured because it isn't a target

- **R11b field (MEMORY, `#2367`): "0 OOMKills @80thr."** Pods run at the 512 Mi limit and did not
  OOMKill under 80-thread overload → the ≤512 Mi PEAK is satisfied in the field *with all three
  caches live*. This is the constraint that actually exists, and it passes.
- `round-validation-metrics.md` B7/B8 measure **cancellation reclaim** and **CPU/memory trajectory
  under load** — not idle. R7's "leaked to 5.5 GB" was a genuine *leak bug* (since fixed), unrelated
  to cache retention policy. There is no idle-memory measurement in any round because **idle memory is
  not a round criterion.**
- The `cqlite.proc.rss_bytes` gauge (`flight-metrics-reference.md:52`) exists precisely to watch peak
  ("a level nearing the memory limit is the OOMKill risk") — a *peak* watch, not an idle watch.

### The one real, correctly-stated hazard hiding inside the wrong claim

The packet is right about **one** thing, but it is a **peak** problem, not an idle one: the library
default `block_cache.max_size = max_memory / 4 = 256 MiB` (`config.rs:299`, from a 1 GB default
`max_memory`). Under B4's real binding constraint — `phase1-6`'s "~20 concurrent streams ×
15–20 MB/stream ≈ 300–680 MB" — a **filled** 256 MiB chunk cache **plus** concurrent stream
working sets can breach ≤512 Mi. That is the K-C "retune the 256 MiB default down for the 512 Mi
Flight/Trino deployment" recommendation, and it is **legitimate** — but note (a) it only bites when the
chunk cache is actually *full* (sustained point-read load, not idle, and never on the scan path that
doesn't populate it), and (b) I could not find an explicit `block_cache` override in `cqlite-flight/src`,
so the deployment likely does inherit the 256 MiB library default — worth confirming before the retune.

### Severity ruling: S-D idle-drain is **P3 hardening**, not a P1 B4 gate

- The packet's "S-D is not optional — it is the B4 gate" (line 134, 170–171, 183–184) is **downgraded
  to P3**. There is no idle clause to gate, the caches don't hold 384 MiB idle, and the field meets
  the real (peak) ceiling today. An idle-drain sweeper is *preventive hardening* (bound worst-case
  resident bytes on a long-lived multi-tenant pod, reduce FD retention per #2013) — nice to have, and
  it must stay off the hot path (#2316), but it **unblocks nothing** and gates no other lever.
- The **peak retune (K-C: smaller `block_cache.max_size` for the pod)** is the item with real B4
  standing, and it is a **config change, P2 at most**, not new code.
- **Correction for the packet:** strike "idle-≤16 Mi unmet by construction" and "S-D is the B4 gate."
  Replace with: "The 256 MiB chunk-cache default is oversized for a 512 Mi pod under concurrency
  (peak, not idle); retune it. Idle-drain is optional P3 hardening."

---

## 2. #2561 — the bug is REAL but already FIXED on `main`; not a 0.17 slot

**Is it real?** Yes. The root cause is genuine: the BTI point-read decode of a partition straddling a
chunk boundary trusted "emit closure fired = complete partition," emitted a garbage entry off the
truncated tail, returned `found=None`, and fell through to a whole-file `scan_for_key` — violating the
#1572 "present-key `get()` never sequential-scans" invariant. It is a correctness-adjacent **perf**
hole (wrong path, but the offset was correct; it degraded a point read to a full scan on ~7 straddling
partitions).

**Is it open?** The issue is OPEN, but the **fix already landed on `main`.** Per the issue body and
confirmed in code: commit `0ebb43381` merged via PR #2554 (#2059). `bti_point.rs:186` now documents
and enforces the `emitted_our_key` gate ("parse returns Ok AND the emit closure fired for the QUERIED
key"), and the regression test `present_key_get_does_not_sequential_scan` was strengthened to check
every key (0/80 fail post-fix, was deterministic-fail pre-fix). **The BTI path — K-C/K-B's cached
point-read path — is not broken on `main`.**

**The only open remainder** is the issue's own follow-up: "confirm the BIG-format equivalent
(`big_point.rs`) doesn't have the same closure-fired-trusted-as-complete pattern." I inspected
`big_point.rs`: it does **not** share the pattern. The BIG point path reasons about **block-map
completeness** and falls to `scan_for_key` only for a genuinely-ambiguous partial map (`big_point.rs:
46,103-171`) — a *bounded, by-design, #1572-sanctioned* fallback ("rare and acceptable"
false-positive with `SCAN_FOR_KEY_CALLS` observable), not a truncated-tail decode bug. So the BIG
follow-up looks like a **no-op verify**, not a live gap.

**Disposition ruling:**
- **NOT worth a 0.17 correctness/perf slot** — the landmine (BTI present-key full-scan) is already
  disarmed on `main`.
- Re-scope #2561 to a **P3 traceability-close**: add a one-shot BIG-path assertion test (present-key
  `get()` across a straddling BIG partition asserts `SCAN_FOR_KEY_CALLS` delta == 0 for a truly-present
  key) to nail down the follow-up, then close. This can ride the #2565 nit batch, not a standalone slot.
- **Correction for the packet:** "fix #2561 first, K-B/K-C are partially broken on straddling
  partitions" is **stale** — strike it. The caches are correct on `main`; only a verify test remains.

---

## 3. #2165 — both packets are right; it is a maintainability item, not a lever either way

The apparent conflict: `phase1-1` (row-engine) demotes #2165 — "Stage 2 is only 9.7% *and* this is
decode-plane *consolidation*, not an optimization; expected perf-neutral; worthless as a throughput
lever (keep only for maintainability)." `phase1-4` (caching) wants it wired — "S-A: wire B1 into the
sequential-scan path … ~1.3–1.5× on repeated-range scans."

**There is no real contradiction — they are measuring different things:**

- `phase1-1` is correct that **routing decode through ChunkSource buys ~0 as a decode/parse speedup**
  (Stage 2 is 9.7%, and consolidation doesn't make decode faster).
- `phase1-4` is correct that the **side effect** of routing through ChunkSource is that the scan path
  gains **access to the B1 chunk cache**, which today it entirely bypasses — and *that* pays back
  decompress+IO **only when the same token range is re-scanned within a freshness window** (repeated
  dashboards, multi-split overlap), which is a real field pattern through Trino. It is **worthless for
  a one-off scan** (hit rate ≈ 0) and pays only on repeated ranges — exactly what `phase1-4`'s own
  table says (line 126: "1.0× one-off; ~1.3–1.5× on repeated-range scans").

So: **#2165 as a decode-speed lever = worthless (phase1-1 right). #2165 as a cache-enablement for
repeated-range scans = modest field-only win (phase1-4 right).** Both correct; the packets just
attach it to different mechanisms.

**Ruling on #2165's 0.17 disposition — re-scope, do NOT close:**
- **Keep it** — it has independent standing as **decode-plane consolidation** (its stated acceptance:
  `chunk_decode_single_plane.rs` excludes only non-query paths; today two query-reachable sites still
  use the legacy IO model). That maintainability value is real and outlives this program.
- **Do NOT credit it a scan throughput multiplier.** In the 0.17 program it is neither a per-stream nor
  a utilization lever for a one-off scan.
- **If wired, the payoff is a keyed/repeated-range cache win, and it MUST ship with scan-resistant
  admission (Lever K-G)** or a full scan will evict the keyed hot set from B1 — turning a keyed win
  into a keyed regression. Wire-for-consolidation is safe; wire-and-populate-B1-from-scans is only safe
  behind scan-resistance.
- Net: **keep as-is (maintainability), P3; not a throughput lever; its cache value is contingent on
  K-G.** The two packets should cross-reference rather than appear to disagree.

---

## 4. Decoded-partition cache (K-A/K-D) — right direction, unsupported hit-rate model, half-right #2037 coupling

**The lever direction is sound.** A repeated point read today re-parses + re-materializes the
partition body even when its chunk is resident in B1 (Phase-0 stages 2+3 ≈ 14% CPU, plus the alloc +
SipHash they drive). A decoded cache collapses a hot-set point read to a clone. This is the one
genuinely *new* keyed lever, and the packet is right that it is the biggest keyed gap.

**But the A2 hit-rate model is asserted, not measured — this is the weak point.** The packet's
`~1.5–3× on the hot keyed set` rests entirely on "the field's dashboards hit a small partition subset
repeatedly (the classic keyed workload)" (line 140). I searched for any field evidence of the keyed
**access distribution** and found **none**:

- There is **no Zipf/skew measurement** of the field keyed workload anywhere in `docs/`. "1.93 M
  partitions/node" is the *corpus size*, not an access distribution.
- The only field keyed loadtest on record (`round-validation-metrics.md` C9) is **~0.9 qps aggregate,
  ~30 rows/s, warm point-read + LIMIT 5/100** — three orders of magnitude below the **A2 ≥1,000
  qps/pod** target the cache is being justified against, and with no reported hot-set concentration.
- The cache-effectiveness gauges exist (`cqlite.cache.key.*`, `cqlite.warm.cache.*`) but I found no
  captured field values establishing a hot-set hit ratio.

**So the working-set question the attack asks — "does a 64–128 MiB decoded cache hold enough to
matter?" — cannot be answered from current data.** With decoded rows at ~3.5× on-disk size (Phase-0
wire ratio), a 64 MiB decoded budget holds ~18 MiB-on-disk-equivalent of hot partitions. Whether that
covers the field hot set is **entirely dependent on a skew we have not measured.** If the keyed access
is near-uniform over 1.93 M partitions (no evidence either way), the hit rate is ~0 and the cache is
dead weight under B4 peak. If it is strongly Zipf (plausible for dashboards, but *unproven*), 64 MiB
could carry a high hit ratio. **The multiplier is real IF the skew is real, and we don't know the skew.**

**Invalidation vs the 3 s reuse window — this part actually works.** K-D's snapshot-scoped design is
correct-by-construction: a Cassandra snapshot is an immutable point-in-time hardlink set, so a bucket
keyed by generation identity (the #2059 inode-stable identity, `#2345/#2383`) is valid until the
generation set changes; "snapshot rolled → drop the bucket" is a clean invalidation and gives a
natural drain. The 3 s connector reuse window (`DEFAULT_SNAPSHOT_REUSE_WINDOW_MILLIS = 3_000`,
confirmed in `CqliteFlightConfig.java:84`) bounds staleness; within a window the generation identity
is stable, so decoded entries stay valid. Invalidation is **not** the risk here — the **hit rate** is.

**The #2037 coupling is half-right.** The packet says "reconcile with #2037 WS6 (per-generation Arrow
cache) or it triples surface; do not file standalone" (lines 145, 174–176). Assessment:
- **Right** that the *build artifact* overlaps: #2037's disposable per-generation Arrow segment cache
  ("columnar per-generation Arrow segments exist only as a cache, invalidated exactly at generation
  death") is structurally the same idea as K-A/K-D at a different serialization (Arrow vs decoded
  rows). Building both independently *would* triple surface. The `issue-2037-arrow-olap-research.md`
  postures (S3/S4A disposable-per-generation cache) are literally this mechanism.
- **Wrong to let it hostage the A2 lever indefinitely.** #2037 is an **owner-gated exploration epic**
  (P2, children Backlog by design, no schedule). Coupling K-A's *delivery* to #2037's *promotion*
  means a real, cheap-to-prototype A2 lever waits on a large architectural decision that may never
  land. **Decouple the measurement from the build:**
  1. **File the cheap, decoupled precursor now (P2, 0.17-eligible, standalone):** *instrument the
     field keyed workload's partition access distribution* (a hot-set-concentration probe over the
     existing `cqlite.read.partition_lookup.*` counters, or a one-shot key-frequency capture during a
     field round). This is a few counters, no #2037 dependency, and it is the **gate** that decides
     whether K-A is worth anything. It answers the unmeasured-skew question the whole lever rests on.
  2. **Keep the decoded/Arrow *cache implementation* reconciled with #2037 WS6** — do not build two
     per-generation decoded caches. When the skew data justifies it, build it once, as the shared
     substrate #2037 also consumes.
- Net **decoded-cache verdict:** sound lever, **do not build blind.** Gate on a measured keyed access
  distribution (standalone P2 probe, decoupled from #2037). Keep the cache *implementation* reconciled
  with #2037 so it isn't built twice. Do not let the owner-gated exploration indefinitely hostage the
  measurement.

---

## 5. Snapshot-reuse-window tuning — CONFIRMED an owner freshness call → NEEDS-OWNER

The packet routes S-B/K-F (lengthen the 3 s snapshot-reuse window) as "freshness contract — data up to
window-old; owner call (mirrors #2305 flush-on-snapshot decision)." **Confirmed correct:**

- The window (`DEFAULT_SNAPSHOT_REUSE_WINDOW_MILLIS = 3_000`, `CqliteFlightConfig.java:84`) directly
  sets **maximum data staleness** — lengthening it trades freshness for fewer snapshot-create
  fan-outs and higher warm/B1/key hit rates. That is a **product/data-semantics tradeoff**, not an
  engineering optimization.
- It mirrors the standing **#2305 owner decision** (flush-on-snapshot semantics, "snapshot-mode
  semantics unchanged … about parse-cost, not read semantics") — the freshness contract is
  explicitly owner-owned per MEMORY.
- The knob **already exists** (`cqlite.snapshot-reuse-window-ms`), so this is a *default-value* /
  *recommended-setting* decision, not code.

→ **Lands on NEEDS-OWNER. Not an autonomous filing.** An agent must not change the default staleness
window; it surfaces the tradeoff (cheapest keyed lift in the program vs freshness cost) and lets the
owner set the number.

---

## 6. NEEDS-OWNER list (product / freshness / scope decisions this verification surfaces)

1. **Snapshot-reuse-window default** (§5) — freshness vs hit-rate/fan-out tradeoff. The knob exists
   (`cqlite.snapshot-reuse-window-ms`, default 3 s); the *value* is a data-staleness product call.
   Mirrors #2305. Cheapest keyed lever in the program, but owner-gated.
2. **Decoded-cache (K-A) vs #2037 sequencing** (§4) — the *measurement precursor* (keyed
   access-distribution probe) can proceed standalone P2, but the **cache build** overlaps the
   owner-gated #2037 ArrowMemtable WS6 per-generation cache. Owner decides whether K-A ships as its own
   thing (reconciled implementation) or waits for #2037 promotion. Recommendation: unblock the
   measurement now; keep the build reconciled.
3. **B4 "≤16 Mi" definition, on record** (§1) — worth an owner one-liner confirming the ratified
   reading is **per-query working set**, not idle-pod memory, so no future agent re-derives the
   phantom idle gate. (Every sibling packet already reads it this way; this just pins it.)

## 7. Corrections the caching packet (`phase1-4`) should absorb

- **Strike** "idle-≤16 Mi unmet by construction" and "S-D is the B4 gate." B4 has no idle clause;
  384 MiB is a cap not a resident figure; the field meets the ≤512 Mi peak (R11b 0 OOMKills @80thr).
  **Downgrade S-D to P3 preventive hardening.** Keep the **peak retune (K-C, smaller
  `block_cache.max_size`)** as the item with real (peak) B4 standing — a config change, P2.
- **Strike** "fix #2561 first, K-B/K-C partially broken." The BTI bug is fixed on `main` (PR #2554);
  only a BIG-path verify test remains. #2561 → P3 traceability-close, ride the #2565 nit batch.
- **Reconcile #2165 wording with `phase1-1`:** it is a maintainability/consolidation item (P3), not a
  scan throughput lever; its cache value is keyed/repeated-range only and contingent on K-G
  scan-resistance. No disagreement between the packets once framed this way.
- **Gate K-A on a measured keyed access distribution** before claiming its ~1.5–3× — the hot-set
  assumption is currently unmeasured (field keyed loadtest on record is ~0.9 qps with no captured
  skew). File the access-distribution probe standalone; decouple it from #2037.

**File path:** `/Users/patrickmcfadin/local_projects/cqlite/docs/research/phase2-verify-caching.md`
(uncommitted per instructions).
