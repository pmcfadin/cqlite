# WS3 — BTI (`da`) read-path structural analysis — CQLite #3029

> ## What this report is, and is not
>
> **It is NOT a performance profile. It contains ZERO measurements of our own** — no rows/s, no
> cycles/row, no histogram, no flame graph. Not one number here was produced by running CQLite.
>
> **The profile is not runnable on this box, and the reason is not #3002.** #3029 was gated on "#3002
> merges"; it merged (PR #3044). The real blocker is a **corpus that had not been commissioned**: the
> largest BTI object reachable anywhere here is a **28,129-byte `Data.db` holding 900 rows in 3
> partitions** (§6). Profiling it would measure process startup. No BTI-capable throughput-scale
> generator exists, and `docker` — which every committed BTI generator requires — is absent (§6.3).
> That prerequisite is now **owned by its own issue, #3234** (*"Commission a profileable BTI (`da`)
> perf corpus + author `gen-perf-corpus-bti.sh`"*), and **WS3's measurement arm — scope items 1, 4 and
> 5 — holds behind it**. The fixture is **shared with WS4 (#3030)**, per this theme's "one fixture,
> three consumers" rule, so it is commissioned once and consumed three times (§6.5).
>
> **What it IS: the structural analysis the profile's own scope items 2 and 3 asked for**, from code
> alone. That needs no corpus, it turned up two P1 defects, and it changes what the eventual
> measurement must compare (§8).
>
> **A null result reported honestly is the deliverable** — #2818 Arm 2's standard, and #3023's
> reporting contract clause 3 (*"A null result is a result… Do not manufacture a win"*). The
> alternative, a rows/s figure off a 900-row fixture, would be a fabricated baseline all future BTI
> work then gets measured against.

Every factual claim carries a `file:line` or a named artefact. **CQLite `file:line` citations are
evidence of *what CQLite does*, which is precisely the subject; none is offered as authority for what
is *correct*.**

---

## 1. Findings ledger

| # | Finding | Scope item it answers | Verdict | Filed as |
|--:|---|---|---|---|
| 1 | The clustering-seek plane still reads `Data.db` through the `MADV_RANDOM` point mmap — #2876's split missed it, in **both** formats (§3) | **item 2** ("does #2876's plane split cover BTI?") | **Defect, from code** — it does not cover it, and the gap is not BTI-specific | **#3230**, P1 |
| 2 | BTI index components (`Partitions.db`, `Rows.db`) are **resident-at-open**; trie traversal is a slice index, **zero-syscall** (§4) | **item 3** ("does trie traversal add small random reads?") | **Hypothesis refuted** — false by construction. Real cost is two eager whole-file reads at OPEN (open-latency, not throughput) | — no defect; recorded here only |
| 3 | BTI readers are **structurally excluded** from the Flight single-source bypass arm; on BIG that arm is worth 3.06× (§5) | unscoped/discovered | **Defect, from code**; verified CONFIRMED and **incidental**, not deliberate (§5.3) | **#3233**, P1 |
| 4 | No profileable BTI corpus exists on this box, and none can be generated here (no `docker`, no JDK, no BTI scale generator) (§6) | blocks **items 1, 4, 5** (the measurement arm) | **Blocker** — this is why the report contains no measurement | **#3234**, prerequisite (shared with WS4 #3030) |
| 5 | #3029's **AC 3** premise "BTI materially below BIG = defect" is unsound: 238k/253k is a **bypass-arm** number BTI cannot reach at all (§7.1) | corrects **item 4** | **Strike the premise.** The profile's job is to *attribute* the gap, not to assume a defect | recorded here only |
| 6 | #3029's shared context still carries **T1 = 600k rows/s/phys-core**, withdrawn upstream by #3023 (§7.2) | corrects **items 1, 4** | **Strike the number.** WS3's ACs bake in a withdrawn target | recorded here only |
| 7 | BTI **full** scan calls `stitch_all_chunks`, materializing the entire decompressed data section into one `Vec<u8>` — **O(file) resident** (§3.3) | unscoped/discovered (adjacent to #3061) | Structural note, from code — invisible on a 900-row fixture, load-bearing at profiling scale | **recorded here only, not filed** |

Consequences that change what the eventual measurement must do: findings 3, 5 and 6 (§7.1, §8.2);
finding 4 is the gating prerequisite (§6, now **#3234**).

---

## 2. Machine and environment

| | |
|---|---|
| CPU | Intel(R) Xeon(R) Platinum 8488C — **8 physical / 16 logical**, SMT on, 1 socket |
| RAM / disk | 30 GB / **266 GB free on `/data`** (295 GB NVMe) |
| Kernel | `6.17.0-1019-aws` |
| `perf` / `bpftrace` / `iostat` | present (`/usr/bin/…`) |
| `offcputime-bpfcc` / `runqlat-bpfcc` | present (`/usr/sbin/…`) |
| `samply` | **absent** — and must not be used regardless (§8.3) |
| `docker` | **ABSENT** — the binding constraint on corpus generation (§6.3) |
| Java / JDK / `cassandra-stress` | **absent** (`/usr/lib/jvm` does not exist; no `cassandra-stress` on disk) |
| `python3` `rust_demangler` | **absent** — #3217 trap 9 (`ws0-3217-report.md:217`): its absence degrades the Part-B parsers *quietly* |
| `kernel.perf_event_paranoid` | currently **4** — #3217 trap 8 (`ws0-3217-report.md:214-216`): it reverts on its own schedule, so re-assert `-1` and assert it took, before **every** capture |

**Comparability.** This is the **same machine class** as the box that produced the BIG head-to-head
numbers quoted below: `ws0-3217-report.md:3-7` records the identical CPU/topology/kernel and states
outright it is the "same machine class as #3100's `c7i.4xlarge`, which is what makes the S=1 control
a direct comparison rather than an analogy." Two deltas remain: **storage** (NVMe `/data` here vs
#3100's **EBS gp3 root**) and a different kernel line from #3100's `7.0.0-1009-aws`.

It is, however, **not** the `i4i` that produced the **238k** figure #3029 quotes — that came from
#2818 (`i4i cold-vs-warm server-direct samply profile`), per #3029's own shared-context block. So a
future WS3 measurement must be a **same-box head-to-head with BIG re-measured locally**; a
BTI-here-vs-238k-there quotient would divide two different machines.

---

## 3. Finding 1 — the clustering-seek plane reads through the `MADV_RANDOM` point mmap

**Filed as #3230 (clustering-seek plane on the advised mapping), P1.** This answers #3029 scope item
2 ("does #2876's plane split cover BTI?"): **it does not — and the gap is not BTI-specific.**

### 3.1 The two planes

| Plane | Field | Built at | Mapping |
|---|---|---|---|
| point | `point_source` (`reader/types.rs:277`) | `reader/mod.rs:576-600` | dedicated 2nd mmap advised `MADV_RANDOM` |
| scan | `scan_positional_source` (`reader/types.rs:290`) | `reader/mod.rs:608-613` | `Arc` clone of the scan mmap |

`MADV_RANDOM` is applied at exactly one site: `point_mmap.advise(memmap2::Advice::Random)`,
`reader/mod.rs:1208`, inside `point_read_mmap` (`reader/mod.rs:1197-1233`).

**Threshold.** The split is gated on `POINT_MMAP_MADV_RANDOM_MIN_BYTES = 8 MiB`
(`reader/mod.rs:348`, rationale `:330-346`). Below it, `point_read_mmap` returns
`scan_mmap.clone()` (`reader/mod.rs:1232`) — **both planes become the same mapping**, so on every
fixture in the repo an A/B between them is structurally zero. This is a hard floor on any future
measurement (§8.1).

*Precision:* "only one mapping is ever advised" holds **under the default `PrefetchMode::Auto` only** —
`mmap_advice_for` (`reader/mod.rs:316-328`) returns `None` for `Auto`/`Off` (`:321`), but
`Sequential`/`WillNeed` (`:325-326`) *do* advise the scan mapping.

### 3.2 Where #2876 landed the split — and where it did not

#2876 repointed only the walks that thread a caller-supplied source:
`summary_scan/mod.rs:315`, `full_index_scan.rs:240`, `full_index_stream.rs:281`,
`scan_stream_windowed_read.rs:130,166,301,340,389`, `scan_stream_windowed_decode.rs:98`.

The clustering-seek plane is not among them:

```
storage/sstable/mod.rs:1572  scan_partition_clustering        (public entry)
  → :1686                    scan_single_partition_clustering
data_access/bti.rs:99        scan_single_partition_clustering
  → :161                     resolve_clustering_seek_window   (unified, big_promoted.rs:225)
      → bti.rs:302           bti_clustering_row_window        (in-memory; see §4)
  → :212                     bti_decompress_and_parse_target_all
data_access/bti_point.rs:488 bti_decompress_and_parse_target_all
  → :581, :617               bti_pull_decompressed_chunk  ← FORWARD, SEQUENTIAL chunk walk
data_access/bti_point.rs:688 bti_pull_decompressed_chunk
  → :713                     ChunkSource::new(self.point_source.as_ref(), …, NS_BTI_CHUNK, …)
```

Both loops walk the partition's chunks **forward and sequentially** (`:575-603` buffers the header;
`:615-622` extends to the authoritative partition end) — the single access shape `MADV_RANDOM`
readahead suppression is exactly wrong for. The exclusion is even documented in prose:
`data_access/compressed_offset.rs:21-22` — "(`bti_point.rs` / `big_promoted.rs` read `point_source`
directly and do not route through here.)"

**BIG leaks identically**, so this is *the clustering-seek plane, both formats*, not a BTI bug:
`big_promoted.rs:524-525` passes `self.point_source.as_ref()` into `compressed_partition_window`,
and `:588-593` reads the uncompressed arm off `point_source` (`:585-587` documents the choice as
deliberate for a "POINT path").

### 3.3 Correct by design, and NOT affected

| Path | Site | Plane | Verdict |
|---|---|---|---|
| BTI point lookup | `bti_point.rs:57` (`bti_point_lookup`), reads at `:157` | `point_source` | correct — genuinely scattered |
| BTI **full** scan | `bti.rs:536` (`bti_scan_with_metadata`) → `:550` `new_scan_cursor` → `data_access/mod.rs:1001-1006` → `scan_source.open` → `ScanCursor{BlockSource}` (`source.rs:196-203`) | unadvised scan mmap | correct |

**Adjacent structural note — ledger finding 7, recorded here only, NOT filed.** That same BTI full
scan calls `stitch_all_chunks` (`bti.rs:562`, `data_access/mod.rs:418`), materializing the **entire
decompressed data section into one `Vec<u8>`** before parsing — invisible on a 900-row fixture,
**O(file) resident** at profiling scale. It sits in the neighbourhood of **#3061** (peak-RSS vs the
<128 MB target) rather than of #3230, and is deliberately left **unfiled** pending a measurement that
would price it — filing an unpriced memory claim is the same error as publishing an unmeasured
throughput one. Any future BTI run must state which of the two BTI scan shapes it measured; their
memory profiles differ.

---

## 4. Finding 2 — BTI index components are resident-at-open; trie traversal is zero-syscall

#3029 scope item 3 asks: *"Does trie node traversal add small random reads on top?"* **No — it
cannot.** The question is false by construction.

| Component | Load | Result |
|---|---|---|
| `Partitions.db` | `tokio::fs::read` — `reader/mod.rs:652` | whole file → `Arc<Vec<u8>>` (`:681`), stored `:964` |
| `Rows.db` | `tokio::fs::read` — `reader/mod.rs:670` | whole file → `Arc<Vec<u8>>` (`:681`) |

Both are read **whole, at open**, in the one BTI-specific branch of `open` (`reader/mod.rs:642-685`);
either missing is a hard `Error::UnsupportedFormat` (`:652-661`, `:670-679`). Not mmapped, not
`pread`, no chunk cache, no `madvise`.

A trie node fetch is therefore a **slice index, not a read**: `bti_clustering_row_window`
(`bti.rs:302`) walks `partitions_db.as_slice()` (`:326`) and `rows_db.as_slice()` (`:344`, into
`resolve_rows_db_entry`, `bti/parser/rows.rs:433`) entirely in memory.

**Where the cost actually is: two eager whole-file reads at OPEN** — an **open-latency** cost
belonging to the O(summary)-open conversation (#2412 lazy Summary-guided BIG index), not the
throughput one. BIG got a lazy open; nobody has asked BTI the equivalent question.

### 4.1 Cache topology on the BTI read path

Three namespaces exist (`data_access/mod.rs:129-131`), folded into the cache key so
differently-granular sites cannot alias (`:123-128`):

| Namespace | Const | Used by | Key granularity |
|---|---|---|---|
| `NS_BTI_CHUNK` | `data_access/mod.rs:130` | BTI seek chunk pull (`bti_point.rs:719`) | absolute **chunk index**, one compression chunk |
| `NS_BIG_POINT` | `data_access/mod.rs:129` | BIG point read (`get_cached_data`, `:661-675`) | `(block_offset, size)` — `size` as the aux discriminant, `:670-674` |
| `NS_WINDOWED_CHUNK` | `data_access/mod.rs:131` | windowed scan (`scan_stream_windowed_decode.rs:45,104`) | chunk index |

`DecompressedChunkCache` under `NS_BTI_CHUNK` is therefore the only cache the BTI *seek* path
consults; the BTI index tries are not cached because they are already resident.

---

## 5. Finding 3 — BTI is structurally denied the Flight bypass arm

**Filed as #3233 (BTI denied the Flight bypass arm), P1.**

### 5.1 The predicate

```rust
// reader/data_access/summary_scan/query_rows.rs:381-382
pub fn supports_streaming_query_scan(&self) -> bool {
    self.index_reader.is_some() && self.bti_partitions_db.is_none()
}
```

Doc comment `:373-374` states the intent: "an `Index.db`, not a BTI `Partitions.db`." For a `da`
reader **both conjuncts fail**:

| Conjunct | Field | For `da` | Why |
|---|---|---|---|
| `index_reader.is_some()` | `types.rs:323` | always `None` | `load_index_reader` derives its path solely from a sibling `*-Index.db` (`component_loading.rs:181`) and returns `Absent` on `NotFound` (`:211-214`); wired `reader/mod.rs:781-790` |
| `bti_partitions_db.is_none()` | `types.rs:418` | always `Some` | set for every `da` reader (`reader/mod.rs:642`, `:681`, stored `:964`) |

**Verified on the fixtures:** `find test-data/datasets/sstables/test_da -name '*Index.db' -o -name
'*Summary.db'` returns **0**; `da-2-bti-TOC.txt` lists exactly `Data.db, Statistics.db,
Digest.crc32, TOC.txt, CompressionInfo.db, Filter.db, Partitions.db, Rows.db`. BTI ships neither
component **by format**, so this is not a fixture accident.

### 5.2 The consequence in Flight

`cqlite-flight/src/bypass.rs:270-272` turns the failed predicate into
`BypassReason::ReaderUnsupported` (variant doc `:135-137`). `producer_warm.rs:126-131` computes the
reason, `:132` gates on `reason.is_selected()`, and a non-selected reason falls through
unconditionally to `KWayMerger::new_from_readers` (`:162-167`).

`CQLITE_FLIGHT_MERGE_PATH=bypass` **cannot rescue it**: the module contract (`bypass.rs:56-59`) is
that `bypass` "requests the fast path but NEVER overrides a correctness precondition," and the
predicate (`:204-273`) checks `supports_streaming_query_scan()` unconditionally, after the
forced-path branch at `:210-211`.

### 5.3 Verification outcome — **CONFIRMED, and INCIDENTAL** (not deliberate)

The deliberate-vs-incidental question was verified before publishing, because the answers carry
opposite consequences: a *deliberate* exclusion is a documented constraint to respect, an *incidental*
one is a defect to remove. **Outcome: CONFIRMED, and incidental — filed as #3233.** Evidence:

1. **The #3058 design never mentions BTI.** Grepping both OpenSpec locations for `bti` /
   `Partitions.db` / `da`: `openspec/changes/archive/2026-07-29-flight-single-sstable-bypass/`
   (`proposal.md`, `design.md`, `tasks.md`, `specs/`) → **0 hits**;
   `openspec/specs/flight-single-sstable-bypass/spec.md` (30,903 B) → **0 hits**. A constraint nobody
   wrote down in the design that created the predicate was not chosen there.
2. **Core labels the dependency implicit and undocumented, in its own words.**
   `parsing/block_entries.rs:119-130` marks itself a **"KNOWN FAIL-OPEN SEAM — issue #3108"** and
   states the `da` route "is unreachable from the single-source query path today only because
   `supports_streaming_query_scan()` refuses BTI readers — an **implicit, undocumented dependency**,"
   which **#3108** owns making explicit. The same comment records a second BTI gap:
   `run_scan_stream_batched` "lacks the BTI dispatch its siblings have — issue #3109."

**The counterfactual, stated so the question is not re-opened.** Had the exclusion been *deliberate*,
its home would be the pipeline-unification design — **#3231** (*"collapse the fragmented Flight
`do_get` read pipeline into a single copy-minimizing path"*) — as a documented constraint on which
readers the single-source path may serve. #3231 does discuss BTI, but only in the opposite direction:
it lists divergent "BIG vs BTI dispatch" as *fragmentation to be collapsed* and names #3109 as "the
first path-unification step." No document anywhere states that BTI must not take the arm. So the
exclusion is a side effect of reusing a BIG-shaped component predicate (`Index.db` presence) as a
proxy for "can stream", and #3233 is a defect report rather than a request to change a decision.

### 5.4 Arm selection is **HALF**-observable — the reason is discarded, the arm is not

The stronger claim ("arm selection is unobservable") is **false**; this report does not make it.

- **The *reason* IS discarded.** `BypassReason` has exactly one production consumer, `is_selected()`
  (`bypass.rs:192-197`), called at `producer_warm.rs:132`; the value is never logged, exported, or
  attached to a span. An operator seeing the merge arm cannot distinguish "BTI reader" from "2 sources"
  from "multicell schema" — the observability gap #3233 records.
- **The *arm* IS observable.** `read_path_probe`'s merger-built counter
  (`write_engine/merge/from_readers.rs:416`, fired at `producer_warm.rs:150,168` — both arms, same
  phase boundary) exposes which arm ran, and existing differential tests assert on exactly that signal.
  The module contract says so (`bypass.rs:60-61`).

So: **half**-observable. WS3's measurement can already prove which arm its BTI run took, and must
report it (§8.2); what it cannot get, without #3233, is *why*.

### 5.5 What the denied arm is worth — on BIG

From `docs/reports/ws0-3100-report.md` (§2, tables at `:63-81`, analysis `:86`):

| Basis | bypass | merge | ratio |
|---|--:|--:|--:|
| per **physical core** (`-c 2,10`), `:68-69` | **252,999 rows/s** (22,111 cyc/row) | 82,639 rows/s (72,382 cyc/row) | **3.06×** |
| per **hardware thread** (`-c 2`), `:78-79` | **234,842 rows/s** (14,883 cyc/row) | 71,974 rows/s (49,863 cyc/row) | **3.26×** |

**Limitation, stated explicitly: these are BIG-only.** No BTI measurement of *either* arm exists.
The 3.06× is what the arm is worth on `nb`; whether BTI would realize the same, more, or less is
**unmeasured**, and this report does not project it.

---

## 6. The blocker — no profileable BTI corpus, and none generable here

**Now owned by #3234** (*"Commission a profileable BTI (`da`) perf corpus + author
`gen-perf-corpus-bti.sh` — prerequisite blocking WS3 (#3029) and WS4 (#3030)"*). This section is that
issue's evidence base: §6.1–6.3 are why it exists, §6.4 is what it does **not** need to build, and
§6.5 is its work breakdown. **WS3's measurement arm (scope items 1, 4, 5) holds behind #3234**; the
structural arm (items 2, 3 → §3, §4) is delivered here and needed nothing from it.

### 6.1 Every BTI object reachable on this box

`ls -la` under `/data/datasets/sstables/test_da/` **and** the worktree's own
`test-data/datasets/sstables/test_da/`; partition/row counts from the committed `*-Data.db.jsonl`
sstabledump references.

| Fixture | `Data.db` | `Rows.db` | partitions / rows | in `/data/datasets`? |
|---|--:|--:|--:|:--:|
| `wide_table` | **28,129 B** | 760 B | 3 / **900** | yes |
| `multiclustering_table` | 10,660 B | 618 B | 3 / **468** | **NO** |
| `simple_table` | 168 B | **0 B** | — | yes |
| `ttl_table` | 76 B | **0 B** | — | yes |
| `collection_table` | 160 B | **0 B** | — | yes |

Largest BTI `Data.db` in existence here: **28 KB / 900 rows**. Three of five fixtures have an **empty
`Rows.db`** — no partition exceeded `column_index_size`, so they exercise no row index at all.
**`multiclustering_table` is invisible to the standard datasets root**: it is committed in the
checkout (`test_da` binaries are tracked, not gitignored — `git check-ignore` reports "NOT ignored")
via commit `eb2d7b2` (PR #3210), but the fetched `/data/datasets` pin predates it.

### 6.2 No BIG corpus for a same-box comparison either

`/data/ws0`, `/home/ubuntu/ws0`, `/home/ubuntu/corpus-3068` are all **absent**. The largest
`*Data.db` anywhere under `/data/datasets` is **647,164 B** (`test_basic/simple_table`), across 155
files. #3217's own 784 MB `nb-16-big` corpus was generated on *this* box and has since been deleted.
**So a same-box BIG-vs-BTI head-to-head requires regenerating BOTH.**

### 6.3 Why neither can be generated here

| Requirement | State |
|---|---|
| `docker` | **absent** — and *every* committed generator is docker-driven: `gen-perf-corpus-3068.sh:68` (`DOCKER="${DOCKER:-sudo -n docker}"`, used `:315-400`), `gen-multiclustering-bti.sh:106-130`, `gen-wide-bti.sh` (19 docker call sites) |
| JDK + `cassandra-stress` | **absent** — so #3217's non-docker tarball recipe (`ws0-3217-report.md:114-120`) cannot run as-is either |
| a BTI **scale** generator | **does not exist.** `gen-perf-corpus-3068.sh` is the only throughput-scale recipe (12M + 1.2M rows, `:62-64`) and is **BIG-only**: no `selected_format` flip anywhere, `nb-*` hardcoded in both its documented output layout (`:31-32`) and its publish step (`:399-400`), and arity-1 `PRIMARY KEY (pk, ck)` (`:217`, `:271`) |
| the only BTI generators | `gen-wide-bti.sh` / `gen-multiclustering-bti.sh` — both build a **`cqlsh` INSERT-per-row `.cql` file**, the approach `gen-perf-corpus-3068.sh:19-21` explicitly calls "hopeless at 12M rows," and default to 300 rows/partition (`gen-wide-bti.sh:21`) |

### 6.4 The harness is NOT a blocker

#3217 committed a **retained, format-agnostic** harness at
`docs/reports/ws0-3217-artifacts/harness/` (13 files incl. `sweep.sh`, `profile-oncpu.sh`,
`profile-offcpu.sh`, `selftest.sh`, `corpus-basis.py`; provenance `ws0-3217-report.md:173-179`).
Grepping it for `nb-` returns **no files** — nothing in it is BIG-specific. `perf`, `bpftrace`,
`iostat`, `offcputime-bpfcc` and `runqlat-bpfcc` are all installed (§2).

### 6.5 What commissioning a profileable BTI corpus requires — the #3234 work breakdown

1. Install `docker` (or a JDK + Cassandra 5.0.8 tarball for a #3217-style non-container run).
2. Fork `gen-perf-corpus-3068.sh` and graft in the BTI flip from `gen-multiclustering-bti.sh:110-113`
   — `storage_compatibility_mode: NONE` + `sstable: selected_format: bti` + an explicit
   `column_index_size` — keeping that script's fail-closed `grep` verification (`:117-120`), since a
   silently-unapplied edit yields a thin `Rows.db` and a fixture that profiles nothing.
3. Replace the hardcoded `nb-*` globs (`:31-32`, `:399-400`) with `da-*`; add `Partitions.db` /
   `Rows.db` to the published component set.
4. Widen the schema to a **compound `PRIMARY KEY (pk, bucket, seq)`** so `Rows.db` carries
   multi-component clustering (today's generators are arity-1) — the same shape #3002 AC-9 and #3030
   (WS4) need.
5. Budget **hours of generation** and **~30 GiB transient disk** (`:62-64` project ~8.6 GB + ~5 GB
   `Data.db`; `:176` enforces a free-space floor for "data + 2× compact headroom"). 266 GB free is
   ample.

**One fixture, three consumers.** The geometry above is not WS3-specific: the same compound-clustering
`da` corpus is what **WS4 (#3030, wide-row ≥4 KB profile)** needs, and what #3002's AC-9 asked for at
correctness scale. That is why #3234 is a standalone prerequisite rather than a step inside WS3 —
commission it once, consume it three times. Corollary for sequencing: **#3234 lands before either WS3's
or WS4's measurement arm can start**, and neither should fork its own generator.

---

## 7. Corrections to #3029's own text — TWO UNSOUND AC PREMISES, STRIKE BOTH

§7.1 and §7.2 are recorded prominently and on purpose: both premises are still live in the issue text,
so the next worker would otherwise inherit them. Neither is a quibble — one would send a bug hunt
after an already-filed defect, the other scores WS3 against a number its own umbrella has withdrawn.
§7.3 collects two lesser pointer errors.

### 7.1 STRIKE the premise "BTI materially below BIG = defect" (AC 3)

#3029's AC 3 reads that a BTI rows/s figure "materially below BIG's 238k" is **"a defect, not a tuning
gap."** That is **unsound**, for a reason that has nothing to do with how fast BTI is:

- **238k/253k is a BYPASS-ARM number.** The 252,999 rows/s/phys-core headline (and the 234,842
  1-hw-thread figure) is Flight `do_get` on the #3058 single-source bypass arm
  (`ws0-3100-report.md:68`, `:78`); the 238k #3029 quotes is #2818's i4i bypass measurement.
- **BTI cannot reach that arm at all** — not slowly, not at all. `supports_streaming_query_scan()`
  fails on **both** conjuncts for every `da` reader, by format (§5.1), so a BTI run is confined to the
  merge arm (**#3233**). Comparing a merge-arm number against a bypass-arm number measures #3233 and
  nothing else, and #3233 is already filed.
- **The sound same-arm comparison is BTI-merge-arm vs BIG-MERGE-ARM = 82,639 rows/s/phys-core**
  (`ws0-3100-report.md:69`), **not** 252,999 (§8.2 tabulates the valid and invalid pairings).

**Reframed job for the profile — attribution, never assumption.** A measured BTI-vs-BIG gap is not
self-interpreting. WS3's task is to **attribute** it across three candidate causes, each of which has
its own already-identified owner:

| Candidate cause | Owner | How the profile separates it |
|---|---|---|
| **Merge-arm routing** — BTI denied the bypass arm | **#3233** | compare arm-to-arm (BTI-merge vs BIG-merge); the arm is observable (§5.4), so disclose it |
| **The clustering-seek plane** on the `MADV_RANDOM` mapping | **#3230** | present in BIG too, so it does **not** explain a BTI-vs-BIG *delta*; isolate with a >8 MiB corpus (§8.1) and the block-read size histogram |
| **Genuine BTI-specific cost** — trie/`Rows.db` resolution, `stitch_all_chunks` residency, eager open reads | unowned; §3.3, §4 bound the candidates | residual after the first two are accounted for, attributed per-phase (#2819 timers) |

Only a residual that survives that decomposition is a new BTI defect. **No gap may be called a defect
before it is attributed** — and a gap explained entirely by #3233 or #3230 is a re-report, not a
finding.

### 7.2 STRIKE the target "T1 = 600k rows/s per physical core" — WITHDRAWN upstream

#3029's shared-context block still asserts "**T1 = 600k rows/s per PHYSICAL core, warm** (2.5× today's
measured 238k)". Umbrella **#3023**'s current title reads:

> *"[UMBRELLA] 0.17 scan-path exploration: TARGET = Flight `do_get` within ~1.3× of bare scan (~280k
> rows/s/phys core); **T1=600k WITHDRAWN as underivable**."*

Said plainly: **WS3's ACs bake in a withdrawn number.** 600k was retired as underivable, and the live
target is *relative* — Flight `do_get` within ~1.3× of bare scan, ~280k rows/s/phys-core. Any WS3
measurement that reports "x% of T1" is scoring itself against a target that no longer exists; it
should report against the ~1.3×-of-bare-scan bound instead, which also requires measuring bare scan on
the same box in the same run.

### 7.3 Two lesser pointer errors

**(a) The AC-9 fixture did not land in PR #3044.** A claim comment on #3029 states "#3002 …merged
2026-07-28 via PR #3044." PR #3044 (`fix(bti): correct Rows.db row-index root base + OSS50 leading
NEXT_COMPONENT`) merged the #3002 **code fix**. #3002's **AC-9 fixture** — the
multi-component-clustering `da` fixture WS3 and WS4 were told to share — landed separately under
**#3032 / PR #3210**, commit `eb2d7b2`, the sole commit touching both
`test-data/schemas/multiclustering-table-bti.cql` and the fixture directory. And it does **nothing
for a profile**: 468 rows in 3 partitions, a correctness fixture (§6.1).

**(b) Two minor pointer errors.** #3029's Notes cite the Cassandra source as
`/Users/pmcfadin/projects/cassandra`; no clone exists on this box and `CQLITE_CASSANDRA_REPO` is
unset — per `CLAUDE.md`, read through the pinned ref (`git show cassandra-5.0.8:<path>`), never a
working tree. Its scope-item-2 chain (`scan_single_partition_clustering` →
`resolve_clustering_seek_window` → `bti_clustering_row_window` → `resolve_rows_db_entry`) **is
correct** (`bti.rs:99` → `:161`/`big_promoted.rs:225` → `bti.rs:302` → `rows.rs:433`) — but it names
the **index-resolution** leg, which §4 shows is entirely in-memory. The leg that touches the advised
mapping is the separate byte-fetch leg (`bti.rs:212` → `bti_point.rs:488`), which #3029 never names.

---

## 8. What the eventual measurement must do

### 8.1 Clear the 8 MiB threshold — non-negotiable

`POINT_MMAP_MADV_RANDOM_MIN_BYTES = 8 MiB` (`reader/mod.rs:348`). A `Data.db` below it makes
`point_read_mmap` return `scan_mmap.clone()` (`:1232`), so **both planes are the same mapping** and
any point-vs-scan A/B — including any before/after for #3230 — measures literally nothing. The corpus
must exceed 8 MiB by a wide margin (§6.5 targets multi-GB; `:330-346` notes the win is unambiguous by
4 MiB).

### 8.2 Compare arm-to-arm, and disclose the arm

Because of #3233, the honest headline pairs are:

| Comparison | Basis | Valid? |
|---|---|:--|
| BTI-merge vs **BIG-merge** (82,639 rows/s/phys-core, `ws0-3100-report.md:69`) | same arm, same box | ✅ the sound one |
| BTI-merge vs **BIG-bypass** (252,999, `:68`) | different arms | ❌ measures #3233, already filed (§7.1) |
| BTI-here vs **238k** (#2818, i4i) | different machines **and** different arms | ❌ (§2, §7.1) |
| BTI-anything vs **T1 = 600k** | target withdrawn upstream | ❌ (§7.2) — score against ~1.3× of bare scan instead |

Re-measure BIG **on this box, alongside BTI, on a geometry-matched corpus**, and state which arm each
number came from — and which BTI scan shape was measured, the whole-section `bti_scan_with_metadata`
(§3.3) or the clustering seek.

### 8.3 The #2818 instrument set — with two amendments

| Instrument | Use |
|---|---|
| `bpftrace` on `block:block_rq_complete` | block-read size histogram (bcc `xfsslower` does not compile here — `ws0-cassandra-baseline-2026-07-27.md:846`) |
| `iostat -x` | `rareq-sz`, `%util`, `r_await` |
| `/proc/<pid>/io` | `read_bytes`, `rchar`, `syscr` — **reported separately, NEVER divided** (`ws0-cassandra-baseline-2026-07-27.md:768`: the quotient "produced a bogus" result); `read_bytes` also proves warm residency (`ws0-3217-artifacts/partC/POST-TO-2817.md:13`) |
| `perf record -F 999` | CPU decomposition — **with frame pointers, NOT `--call-graph=dwarf`** |
| `samply` | **never** |
| #2819 `stream_*` sub-phase timers | phase attribution |

Two amendments to what #3029's scope item 1 prescribes:

1. **Not `--call-graph=dwarf`.** #3029 asks for `perf record -F 999 --call-graph=dwarf`. That **hangs
   past 120 s** against this binary — recorded twice (`ws0-3217-report.md:168` and trap 6 at `:211`)
   and earlier at `ws0-cassandra-baseline-2026-07-27.md:848`. Build with frame pointers instead,
   exactly as #3217 did (`ws0-3217-report.md:162-165`): `CARGO_PROFILE_RELEASE_STRIP=none
   CARGO_PROFILE_RELEASE_DEBUG=true RUSTFLAGS="-C force-frame-pointers=yes"`.
2. **`samply` records 0 samples on these kernels**
   (`ws0-3026-artifacts/ws0-results/head-to-head-method.md:105`,
   `ws0-cassandra-baseline-2026-07-27.md:845`) — and it is not installed here anyway.

Inherit #3217's ten traps wholesale (`ws0-3217-report.md:181-226`). The first three each produce an
empty off-CPU profile that "reads identically to 'the mpsc handoff is innocent'" (`:183-186`) — the
same false-negative shape a BTI profile would take. Highest-risk here today: `perf_event_paranoid` is
**4** (trap 8), BPF collectors need `sudo` regardless of the sysctl (trap 1), sched tracepoints need
`sudo` independently (trap 7), and `rust_demangler` is **absent** (trap 9, degrades quietly). §2
records all four.

### 8.4 Report per the #3023 contract — against the LIVE target

cycles/row **and** bytes-of-memory-traffic/row; rows/s **per physical core, warm** as the headline
with cold separate; every multiplier measured, not modelled; findings posted to umbrella #3023. The
target to score against is #3023's current one — **Flight `do_get` within ~1.3× of bare scan (~280k
rows/s/phys-core)** — never the withdrawn T1 = 600k (§7.2), which means bare scan must be measured in
the same run on the same box.

---

## 9. Acceptance criteria — status

| AC | Status | Where |
|---|---|---|
| **1.** BTI's first read-path profile exists, #2818-comparable instrument coverage | ❌ **HELD behind #3234** — no profileable corpus (§6) | §6 |
| **2.** Evidence-backed answer on whether #2876's split covers BTI; any defect filed **separately** | ✅ **answered: it does not** — filed as **#3230**, both formats | §3 |
| **3.** Does trie traversal add small random reads? | ✅ **answered: no** — resident-at-open, zero-syscall; hypothesis refuted | §4 |
| **4.** BTI rows/s/phys-core next to BIG's 238k | ❌ **HELD behind #3234**; and the comparison as specified is **unsound** — the premise is struck (§7.1) and the target it cites is withdrawn (§7.2) | §6, §7.1, §7.2 |
| **5.** BTI roofline note (cycles/row, traffic/row) | ❌ **HELD behind #3234** — requires measurement | §6 |
| *(unscoped, found en route)* BTI excluded from the Flight bypass arm | ✅ filed as **#3233**; verified CONFIRMED + incidental | §5 |
| *(unscoped, found en route)* BTI full scan materializes the whole data section | ⚠️ recorded here only, **not filed** — unpriced | §3.3 |

Scope items **2 and 3 are delivered**; items **1, 4 and 5 — the measurement arm — hold behind #3234**,
whose fixture is shared with WS4 (#3030).

## 10. Notes and deviations

- **No production code was changed and no fix is proposed here.** Both defects are filed as their own
  P1 issues (#3230, #3233), per #3029's instruction to file separately rather than bury; the corpus
  prerequisite is filed as **#3234**.
- **Nothing was measured.** Every number is quoted from a committed artefact (`ws0-3100-report.md`,
  `ws0-3217-report.md`, `ws0-cassandra-baseline-2026-07-27.md`) or read off the filesystem with
  `ls`/`find`/`lscpu`/`uname`. **This report claims no measurement of its own, anywhere.**
- **Ordering.** #3230 and #3233 are code-only and need no corpus, so both can proceed while **#3234**
  is funded. Their *validation* needs a >8 MiB corpus (§8.1), which makes #3234 the gating item for
  all of WS3's measurement arm — and, per #3029's own note, for WS4 (#3030) too. **#3234 first, then
  re-open the profile.**
- **One finding is deliberately unfiled**: the `stitch_all_chunks` whole-section materialization
  (§3.3). It is real and read off the code, but pricing it needs the same corpus #3234 will produce,
  and an unmeasured memory claim is the same category of error this report exists to avoid.
