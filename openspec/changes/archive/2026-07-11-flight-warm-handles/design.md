# Design — cqlite-flight warm-handle service (#2310)

## Context

Static analysis (anchors `main`-relative, WILL drift — re-grep before editing):

- **Stateless per request.** `CqliteFlightService` holds only `data_dir` + `batch_size`
  (`cqlite-flight/src/service.rs:119-126`). `build_producer` (`service.rs:145`) calls
  `parse_schema` → `parse_cql_schema` (`service.rs:138`) on **every** RPC, then constructs a fresh
  `MergeProducer`. `do_get_setup` (`service.rs:479`) resolves `DirSource` and lists paths per
  request inside `spawn_blocking`.
- **Directory resolve per request.** `DirSource::resolve` (`cqlite-flight/src/producer.rs:146`)
  chooses the live `<data>/<ks>/<table>[-<uuid>]` dir or the `snapshots/<name>/` hardlink set, with
  in-`data_dir` path safety (#1430). `data_paths` (`producer.rs:194`) `read_dir`s and
  `generation_of` (`producer.rs:237`) parses the generation from each `Data.db` file name; paths
  sort newest-generation-first (`producer.rs:230`). `prune_paths_cancellable` reads each
  `Summary.db` for the token-span prune.
- **Generation number is already parsed** (`generation_of`, `producer.rs:237`) — but nothing keys
  cached state on it; every request re-opens readers from scratch.
- **Core already solved warm freshness.** `Database::refresh()` (`cqlite-core/src/lib.rs:425`) →
  `SSTableManager::refresh_tables` (`cqlite-core/src/storage/sstable/refresh.rs`): re-runs the same
  filename/TOC discovery `open` used (no content sniffing, no heuristics), diffs canonical `Data.db`
  paths against the held reader set, opens **added** generations, drops **removed**, and keeps
  **unchanged** generations' warm parsed Index/Statistics/bloom state (not rebuilt). Application is
  **atomic + fail-closed** (all adds open before the write guard; any open failure mutates nothing —
  #1626 corrupt-`Statistics.db` hard-fail inherited). In-flight queries hold `Arc` reader clones and
  complete against the pre-refresh set. A dedicated refresh mutex serializes concurrent refreshes;
  it is NEVER taken on a query path.
- **Cancellation contract (#2264/#1473).** `MergeProducer::produce_cancellable` (`producer.rs:495`)
  polls a `CancelFlag` between partition steps; a pre-cancel yields ZERO steps.

The gap: core's warm-set/refresh machinery lives on `Database`, but the flight producer never opens
a `Database` — it constructs `DirSource` + `MergeProducer` per request and never retains parsed
state. This epic gives flight an equivalent warm layer keyed on generation identity.

---

## Decision 1 — Cache key = generation identity, not directory path

**Options considered.**

- **(a) Key by resolved directory path.** Cache keyed on the `DirSource` dir.
  - Con: snapshot mode creates a **new hardlinked dir per query** — a path key misses on every
    snapshot even though the underlying inodes (and thus all parsed state) are identical. Defeats
    the whole epic in the mode the field actually runs.
- **(b) TTL-keyed / time-bucketed.** Key by `(table, coarse time bucket)`.
  - Con: introduces a staleness window (a flush inside the bucket is invisible) — a correctness
    tradeoff, and still misses across snapshot dirs within a bucket for unrelated reasons.
- **(c, RECOMMENDED) Key by (table, SSTable generation set) with inode-stable identity.** The cache
  key for a warm handle is the logical table plus the **set of SSTable generations** present, where
  each generation's identity is inode-stable (device+inode of its `Data.db`, cross-checked with the
  parsed generation number) so the SAME files reached through a fresh snapshot hardlink dir resolve
  to the SAME key.

**Recommendation: (c).** Generation identity is what actually determines the validity of parsed
Index/Summary/Statistics/bloom state — two snapshot dirs that hardlink the same inodes describe the
same bytes, so their parsed state is interchangeable. Keying on inode-stable generation identity
(not the ephemeral snapshot path) is the only key that gives a warm hit across per-query snapshot
dirs while never serving state for bytes that changed. Live-mode reuse falls out for free: a live
dir whose generation set is unchanged between requests hits the same key.

---

## Decision 2 — THE refresh trigger (the epic's named Seam-1 decision)

The epic lists candidates: (a) cheap per-request staleness probe (dir listing / generation-set
diff); (b) snapshot-`manifest.json` diff fast path; (c) TTL; (d) external signal.

**Options considered.**

- **(a) Per-request generation-set probe.** On every request, list the resolved dir (or otherwise
  enumerate the generation set) and compare to the warm handle's cached set. Unchanged set → warm
  hit (no reader-open, no parse); changed set → rebuild only the delta (adopt Decision 3).
  - Pro: **zero staleness window** — a directory listing is authoritative ground truth for "what
    generations exist right now," so a flush/compaction is visible on the very next request. **No
    heuristics** — it reads the truth, it does not infer it. The listing (a readdir + name parse)
    is cheap relative to the reader-open/Index/Summary/Statistics/bloom parse it guards.
  - Con: still one `read_dir` per request (far cheaper than the parse it saves, but non-zero).
- **(b) Snapshot `manifest.json` diff fast path.** In snapshot mode the snapshot already carries a
  `manifest.json` enumerating its files; when the manifest is byte-identical to the cached one for
  that generation set, skip the `read_dir` entirely and take the warm hit.
  - Pro: turns the probe into a single small-file stat/read in the common snapshot path.
  - Con: only applies in snapshot mode; a manifest is a fast path, not the correctness backbone.
- **(c) TTL.** Serve warm for N seconds, then force a rebuild.
  - Con: **staleness window is a correctness tradeoff** — a flush inside the TTL is invisible.
    Rejected: this epic must be zero-staleness-regression (epic acceptance).
- **(d) External signal / fs-watching.** inotify/FSEvents or a control-plane refresh RPC.
  - Con: fs-watching is **platform-dependent and heuristic** (coalesced/dropped events, hardlink
    blind spots) — a no-heuristics violation. An external signal-only design has no fallback when
    the signal is missed and reintroduces a staleness window.

**Recommendation: (a) as the correctness backbone + (b) as the snapshot fast path.** The
per-request generation-set probe is authoritative and zero-window; the `manifest.json` diff avoids
the `read_dir` when the manifest matches in snapshot mode. **Reject TTL** (staleness window) and
**reject fs-watching** (heuristic, platform-dependent). This keeps the no-heuristics mandate: the
trigger is an explicit documented contract that reads ground truth every request.

**Fail-closed (mirrors #1749).** A **probe error** (dir unreadable, manifest unparsable) is treated
as **"changed" → full re-resolve** (never a stale warm hit). A **refresh/rebuild error** (an added
generation fails to open — e.g. corrupt `Statistics.db`, #1626) leaves the **previously warm set
fully intact** and surfaces the typed error — no partial view, exactly as `refresh_tables` does.

---

## Decision 3 — Reuse core's refresh contract vs new machinery

**Options considered.**

- **(a) New flight-local warm-set + diff/swap machinery.** Reimplement add/drop/keep diffing,
  atomic swap, and Arc isolation inside cqlite-flight.
  - Con: duplicates the exact semantics `refresh_tables` already proves (atomic, fail-closed, Arc
    isolation, concurrent-refresh serialization, #1626 hard-fail); a second implementation drifts
    from the first and re-litigates settled correctness.
- **(b, RECOMMENDED) Adopt/adapt core's refresh contract.** Model the flight warm handle on
  `Database::refresh()` / `SSTableManager::refresh_tables` (#1749): the warm handle holds `Arc`
  reader (and parsed-schema) clones; a rebuild diffs the probed generation set against the held set,
  opens only **added** generations, drops **removed**, keeps **unchanged** parsed state, and swaps
  atomically under a write guard while in-flight requests keep their pre-swap `Arc` clones. Where
  flight's per-request `DirSource`/`MergeProducer` shape differs from core's `Database`, adapt the
  seam (a warm handle that hands a `MergeProducer` a pre-resolved, pre-parsed path/reader set)
  rather than reinvent the diff/swap.

**Recommendation: (b).** The freshness/atomicity/isolation properties this epic needs are exactly
#1749's; reusing that contract inherits its tests and its no-heuristics discovery, and keeps a
single source of truth for "how a warm CQLite reader set refreshes."

---

## Decision 4 — Memory budget + eviction

**Options considered.**

- **(a) Unbounded cache.** Keep every table/generation ever parsed.
  - Con: violates the <128MB discipline on a many-table server; unbounded growth.
- **(b, RECOMMENDED) LRU by (table, generation) with explicit accounting.** Bound warm state by an
  explicit byte budget inside the <128MB discipline; evict least-recently-used (table, generation)
  entries when the budget is exceeded. Account the parsed-state footprint explicitly (per-generation
  Index/Summary/Statistics/bloom + parsed schema), not by proxy. A generation dropped by a rebuild
  (removed on disk) is evicted immediately regardless of LRU age.

**Recommendation: (b).** LRU by (table, generation) matches the cache key (Decision 1) and the
refresh unit (Decision 3), giving natural, explainable eviction. Explicit accounting keeps the
budget auditable and lets the metrics surface report it.

**Metrics surface (feeds #2289).** Bounded, catalog-attribute counters: warm **hit**, **miss**,
**evict**, and **refresh-outcome** (unchanged / rebuilt-delta / fail-closed-retained). These ride
the existing observability contract (no new knob); the #2289 harness and #1494 bench suite consume
them to prove the second identical query on an unchanged generation pays ~0 parse cost.

---

## Decision 5 — Cancellation through cached/refresh paths

**Recommendation: the #2264/#1473 discipline holds unchanged through the cache and refresh paths.**
Both the **staleness probe** and any **rebuild** (reader opens) poll the same `CancelFlag`
cooperatively and map `Error::Cancelled` by variant (not by racing a flag): a pre-cancelled request
does ZERO probe/rebuild work, and a client disconnect mid-rebuild stops it without corrupting the
warm set (the fail-closed rule keeps the prior set intact). A warm hit is trivially cancellable (it
does no I/O beyond the probe). No cancellation guarantee that holds on the cold path may weaken on
the warm path.

---

## Modes, budgets, cancellation (summary)

- **Snapshot vs live:** both use the warm handle keyed on generation identity (Decision 1). Snapshot
  mode additionally gets the `manifest.json` fast path (Decision 2b). Per-query snapshots and
  flush-on-snapshot semantics are unchanged (#2305).
- **Budgets:** the byte/row result budget, LIMIT, and #2230 wide-partition bounds are enforced by
  the same `drive_merge` sink — the warm handle changes only *how readers are obtained*, never how
  rows are produced.
- **Cancellation:** Decision 5.

## Test strategy (parity is the deliverable)

- **Cross-snapshot warm-hit:** two `do_get`s for the same table land in two different snapshot
  hardlink dirs over the same inodes; assert the second is a warm hit (metrics: hit++, zero
  reader-open/parse) and returns byte-identical batches to the first.
- **Staleness visibility:** add a generation on disk (simulate a flush), then re-request; assert the
  probe reports "changed," the rebuild adds exactly the new generation, and the result reflects it —
  zero staleness window.
- **Fail-closed rebuild:** an added generation with a corrupt `Statistics.db` (#1626) → the rebuild
  returns the typed error and the previously warm set is still served intact.
- **Memory budget + eviction:** drive enough distinct (table, generation) entries to exceed the
  budget; assert LRU eviction (evict++), footprint stays within the <128MB discipline, and an
  evicted entry re-parses correctly on next request.
- **Cancellation:** a pre-cancelled request does zero probe/rebuild work; a disconnect mid-rebuild
  leaves the warm set intact.
- **Bench evidence (#2289 + #1494):** second identical query on an unchanged generation shows ~0
  parse cost (schema parse + resolve + reader-open/index/summary/bloom parse elided), measured on
  the #2289 harness point-read / LIMIT / full-scan runs.

## Open questions for Seam 1

1. **Warm-handle seam shape:** a flight-owned `WarmTableRegistry` that hands `MergeProducer` a
   pre-resolved reader set, vs opening a core `Database` per table and calling `refresh()`.
   (Recommendation: the former — flight's `DirSource`/`ScanSpec`/token-prune shape differs from
   `Database`'s query surface; adapt the #1749 contract rather than route through `Database`.)
2. **Byte budget value + source:** a fixed named default inside <128MB vs a configurable ceiling.
   (Recommendation: a fixed named default this epic, no new user knob per Non-goals; revisit only if
   the field needs it.)
3. **Schema-cache scope:** cache the parsed schema per (table, ticket-DDL hash) alongside the
   generation set, or per table only. (Recommendation: per (table, DDL) — the DDL rides the ticket
   and can in principle change; keying on it keeps the cache authoritative.)
