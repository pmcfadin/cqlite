# Audit #952 — Unwired Storage Capabilities (Epic #951)

Systematic audit of storage-layer prune/seek/index/bloom capabilities that exist
and are tested but are **not reachable from the CQL query engine**
(`cqlite-core/src/query/`). This is the failure class behind #949 (bloom filters /
Index.db / BTI trie / `get()` were never called by `SELECT`).

All paths and line numbers are from the worktree at commit on
`worktree-epic-951-wire-storage-query`. **No code was modified by this audit.**

---

## 1. Public call chain (ground truth)

### Query entry points

| Entry point | File:line | Routes SELECT to |
|---|---|---|
| `QueryEngine::execute` | `cqlite-core/src/query/engine.rs:139` | `execute_select_query` (non-simple SELECT) → **`SelectExecutor::execute`** (engine.rs:117); *simple id lookups* and non-SELECT fall through to **legacy `QueryExecutor::execute`** (engine.rs:163) |
| `QueryEngine::execute_streaming` | `cqlite-core/src/query/engine.rs:205` | **`SelectExecutor::execute_streaming`** (engine.rs:212) |
| `QueryEngine::execute_with_params` | `cqlite-core/src/query/engine.rs:262` | **ignores `_params`**, calls `self.execute(cql)` (string passthrough) |
| `QueryEngine::execute_prepared` | `cqlite-core/src/query/engine.rs:290` | `PreparedQuery::execute` → **legacy `QueryExecutor::execute`** (`prepared.rs:98`) |

There are **two distinct SELECT execution paths** with different access-path behavior:

- **`SelectExecutor`** (`select_executor.rs`) — the modern path. Used by
  `execute_select_query` and `execute_streaming`. This is where the #949 fast path lives.
- **Legacy `QueryExecutor`** (`executor.rs`) — used by simple-id-lookup SELECTs, all
  prepared SELECTs, and `execute_with_params`. It always issues full
  `storage.scan(table, None, None, None, None)` (executor.rs:249, executor.rs:460) and
  per-row `storage.get` for index/PK lookups (executor.rs:260, executor.rs:293).

### Storage scan/get surface used by the query layer

| Manager method | File:line | Called from query? |
|---|---|---|
| `SSTableManager::scan` | `storage/sstable/mod.rs:749` | Yes — `select_executor.rs:1381`, `executor.rs:249`, `executor.rs:460` |
| `SSTableManager::scan_stream` | `storage/sstable/mod.rs:1437` | Yes — `select_executor.rs:1215` |
| `SSTableManager::scan_with_cell_metadata` | `storage/sstable/mod.rs:1304` | Yes — `select_executor.rs:1331` |
| `SSTableManager::scan_partition` | `storage/sstable/mod.rs:972` | Yes — `select_executor.rs:1170`, `select_executor.rs:1377` (#949 fast path) |
| `SSTableManager::get` | `storage/sstable/mod.rs:680` | Only from legacy `executor.rs:260/293` (not `select_executor`) |

**Key structural finding:** `scan_partition` (the #949 fast path) prunes candidate
SSTables via `might_contain_partition` (bloom + BTI presence) at `mod.rs:988`, but then
calls **`reader.scan(...)` + `retain(matches_key)`** at `mod.rs:1036-1037`. It does
**not** perform a within-SSTable seek — the BTI/Index byte-offset seek
(`lookup_partition_via_bti_trie`, `lookup_partition_with_index`, `scan_for_key`) is only
reachable through `SSTableReader::get` (data_access.rs:192), which the modern SELECT path
never calls. So the partition fast path prunes *which files* but still linearly scans the
*whole* admitted file. This is exactly the #953 (within-SSTable seek) gap.

---

## 2. Summary table — storage capabilities vs wiring

Legend for wiring status:
- **WIRED** — on the live CQL SELECT path.
- **PARTIAL** — reachable from one SELECT surface but not all (e.g. fast path only via
  the modern executor, bypassed by prepared / params / simple-id).
- **UNWIRED-INTENDED** — exists + tested, should be reachable from a SELECT but isn't.
- **INTERNAL** — intentionally an internal building block; not meant to be a query entry.

| Capability | File:line | Wiring status | Intended caller surface | Covered by |
|---|---|---|---|---|
| `SSTableReader::might_contain_partition` (bloom + BTI presence prune) | `reader/partition_lookup.rs:165` | **PARTIAL** | `scan_partition` candidate prune — reached via modern SELECT only | #962 (apply fast path across all SELECT surfaces) |
| `BloomFilter::might_contain` | `sstable/bloom.rs:121` | **PARTIAL** | via `might_contain_partition` (line 174) and `reader.get` (data_access.rs:209) and `partition_lookup.rs:371/397` | #953, #962 |
| `SSTableManager::scan_partition` (file-level prune) | `sstable/mod.rs:972` | **PARTIAL** | `WHERE pk = ?` fast path; only modern `SelectExecutor`, not prepared/params/simple-id | #962 |
| `lookup_partition_via_bti_trie` (BTI byte-offset seek) | `reader/partition_lookup.rs:80` | **UNWIRED-INTENDED** | within-SSTable partition seek; only reached via `reader.get`→`data_access.rs:279`, which SELECT never calls | #953 |
| `lookup_partition_with_index` (Index.db/Summary seek) | `reader/partition_lookup.rs:26` | **UNWIRED-INTENDED** | within-SSTable partition seek for non-BTI; called only by `partition_lookup` internals / docs refs | #953 |
| `SSTableReader::scan_for_key` (targeted single-partition scan) | `reader/data_access.rs` (counter at :252) | **UNWIRED-INTENDED** | fall-back seek inside `reader.get`; SELECT never enters `reader.get` | #953 |
| `SSTableReader::get` / `SSTableManager::get` | `data_access.rs:192` / `mod.rs:680` | **UNWIRED-INTENDED** (for modern SELECT) | point read; only legacy `executor.rs` uses it | #953 / #962 |
| `IndexReader::lookup_partition` (digest → entry) | `sstable/index_reader.rs:174` | **INTERNAL** (reached via `partition_lookup.rs:34/191`) | building block for `lookup_partition_with_index` | #953 (indirect) |
| `Index::find_entry` | `sstable/index.rs:120` | **INTERNAL** (reached via `data_access.rs:216` inside `reader.get`) | building block for point read | #953 (indirect) |
| `BtiReader::lookup_partition` (clustering-aware partition seek) | `bti/parser.rs:2179` | **UNWIRED-INTENDED** | partition seek returning payload ref for clustering descent | #953 / #954 |
| `BtiReader::lookup_row` (within-partition clustering seek) | `bti/parser.rs:2330` | **UNWIRED-INTENDED** | clustering-key predicate pushdown (`WHERE pk=? AND ck >/=/< ?`) | #954 |
| `select_row_index_blocks_for_range` (clustering range → row-index blocks) | `bti/parser.rs:1970` | **UNWIRED-INTENDED** | clustering range pruning; only referenced from `bti/` tests + `bti::lookup_row` internals | #954 |
| `lookup_raw_key_in_bti_partitions_db` | `bti/parser.rs:1056` | **INTERNAL** (reached via `partition_lookup.rs:91`) | low-level BTI Partitions.db walk for `lookup_partition_via_bti_trie` | #953 (indirect) |
| `lookup_partition_in_bti_file` | `bti/parser.rs:940` | **INTERNAL** (only writer round-trip tests + docs) | low-level BTI file walk | #953 (indirect) |
| `SummaryReader::find_entry_for_position` | `sstable/summary_reader.rs:164` | **UNWIRED-INTENDED** | Summary.db → Index.db narrowing for partition seek | #953 |
| `SummaryReader::get_entry_at` | `sstable/summary_reader.rs:185` | **UNWIRED-INTENDED** | Summary.db indexed access for seek | #953 |
| `Directory::get_secondary_index` / `get_secondary_indexes` | `sstable/directory/mod.rs:131/136` | **UNWIRED-INTENDED** | secondary-index-aware access path | new gap (see §3) |
| `get_with_spec_readers` / `get_with_schema_context` | `reader/partition_lookup.rs:364/389` | **UNWIRED-INTENDED** | schema-context point read; no non-test callers | #953 |
| `execute_with_params` parameter binding | `engine.rs:262` | **UNWIRED-INTENDED** | bound-param SELECT | #961 |
| Prepared SELECT → modern fast path | `prepared.rs:98` (uses legacy `executor`) | **UNWIRED-INTENDED** | prepared SELECTs bypass `SelectExecutor` entirely → no fast path | #961 + #962 |
| `select_parser` unquoted/typed literals (UUID, blob `0x…`) | `query/select_parser.rs` (no UUID/hex handling found) | **UNWIRED-INTENDED** | `WHERE pk = <uuid>` literal in WHERE | #956 |

---

## 3. UNWIRED-BUT-INTENDED items mapped to epic children

- **Within-SSTable partition seek** — `lookup_partition_via_bti_trie`,
  `lookup_partition_with_index`, `scan_for_key`, `reader/manager get`,
  `SummaryReader::find_entry_for_position`/`get_entry_at`, `get_with_spec_readers`,
  `get_with_schema_context`, `BtiReader::lookup_partition`: **#953 (B)**. These are the
  byte-offset seek primitives that `scan_partition` does not yet call; today it
  full-scans the admitted file.
- **Clustering-key predicate pushdown** — `BtiReader::lookup_row`,
  `select_row_index_blocks_for_range`: **#954 (C)**.
- **IN / token-range multi-partition lookups** — currently `full_partition_key_lookup`
  (`select_executor.rs:418`) only handles a single fully-equal partition key; IN/token
  fan-out to multiple `scan_partition`/seek calls is unbuilt: **#955 (D)**.
- **Parser typed/unquoted literals** — `select_parser` has no UUID or `0x…` blob literal
  handling, so `WHERE pk = <uuid>` never reaches the equal-predicate fast path: **#956 (E)**.
- **Streaming vs materializing reconciliation parity** — `scan_partition` reconciles
  generations via k-way merge (mod.rs:1009, write-support) while `scan_stream`
  (select_executor.rs:1215) does per-key merge; these can diverge: **#957 (F)**.
- **`execute_with_params` / prepared param binding** — engine.rs:262 ignores `_params`;
  prepared SELECTs route through the legacy executor (prepared.rs:98) and never bind into
  the modern fast path: **#961 (I)** (binding) + **#962 (J)** (route prepared through the
  fast path).
- **Apply fast path across ALL SELECT surfaces** — the #949 fast path is only on
  `SelectExecutor`; simple-id-lookup SELECTs, prepared SELECTs, and
  `execute_with_params` bypass it entirely: **#962 (J)**.

### Genuinely NEW gaps (not covered by any listed child)

1. **Secondary index access path** — `Directory::get_secondary_index` /
   `get_secondary_indexes` (`sstable/directory/mod.rs:131/136`) expose discovered
   secondary indexes, but **no query-layer code references them**. None of the listed
   children (#953–#962) cover wiring a `WHERE <non-pk-col> = ?` SELECT to a secondary
   index access path; the legacy executor's "index" branch (executor.rs:1011 comment)
   only calls `storage.get` by key, not an actual `.db` secondary index. **Recommend a new
   child issue** (or explicit out-of-scope note) for secondary-index-backed SELECT.
2. **Within-SSTable seek not invoked by the file-level fast path** — strictly this is the
   substance of #953, but worth calling out as a discrete regression risk: a future change
   could "tick the #949 box" (file prune via `might_contain_partition`) while leaving the
   linear `reader.scan` in place. The CI check in §4 must assert the *seek* primitives are
   called, not just `scan_partition`. Treat as a **test/assertion requirement layered onto
   #953 + #958/#960**, not a separate feature.

---

## 4. Recommended static-grep CI check

Goal: fail CI when a storage seek/prune primitive exists with non-test callers only
inside `storage/`, i.e. it is never referenced from `cqlite-core/src/query/`. This catches
the #949 regression class (a primitive silently drifting out of the query path).

Wire as a new script `scripts/check-storage-wiring.sh`, invoked from
`scripts/agent-gate.sh` (add a step before the summary block). It is `rg`-only, needs no
build, and runs in well under a second.

```bash
#!/usr/bin/env bash
# scripts/check-storage-wiring.sh — fail if a storage seek/prune primitive that should be
# reachable from the CQL query engine has no reference under cqlite-core/src/query/.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

# Primitives that MUST be reachable (directly or transitively) from the query layer.
# Keep this list in sync with docs/audits/952-unwired-storage-capabilities.md §2.
REQUIRED_IN_QUERY=(
  scan_partition
  might_contain_partition
)

# Primitives that, once #953/#954/#962 land, must appear somewhere on the query→storage
# call chain. Until then they are tracked as "known unwired" (warn, do not fail).
SEEK_PRIMITIVES=(
  lookup_partition_via_bti_trie
  lookup_partition_with_index
  scan_for_key
)

fail=0
for sym in "${REQUIRED_IN_QUERY[@]}"; do
  if ! rg -q "\b${sym}\b" cqlite-core/src/query; then
    echo "WIRING-FAIL: '${sym}' is no longer referenced from cqlite-core/src/query/ (regression of #949)"
    fail=1
  fi
done

for sym in "${SEEK_PRIMITIVES[@]}"; do
  if rg -q "\b${sym}\b" cqlite-core/src/query; then
    echo "WIRING-OK: seek primitive '${sym}' now reachable from query layer"
  else
    echo "WIRING-TODO: seek primitive '${sym}' still unwired from query layer (see #953/#962)"
  fi
done

exit "$fail"
```

The two verbatim ranking-grep commands the audit itself runs (and which the script
encodes) are:

```bash
# (a) enumerate storage seek/prune/index/bloom primitives
rg -n 'pub (async )?fn (lookup_|bti_|get_with_|might_contain|find_entry|scan_for_key|.*index.*|.*bloom.*)' cqlite-core/src/storage

# (b) for each symbol, assert query-layer reachability (count > 0 == wired)
rg -l '\bscan_partition\b' cqlite-core/src/query
rg -l '\bmight_contain_partition\b' cqlite-core/src/query
```

Recommended wiring point: append the `check-storage-wiring.sh` invocation to
`scripts/agent-gate.sh` so it is part of the canonical pre-PR gate and its result joins
the machine-checkable summary block. (Functional proof that the *seek* primitives actually
run belongs in #958's work counters / #960's access-path assertions — the grep is a cheap
structural tripwire, not a substitute.)

---

## 5. Intentionally internal (not query entry points)

| Symbol | File:line | Rationale |
|---|---|---|
| `IndexReader::lookup_partition` | `sstable/index_reader.rs:174` | Low-level Index.db digest→entry lookup; correctly consumed only by `partition_lookup.rs` (the seek wrapper). Query layer should call the wrapper, not this. |
| `Index::find_entry` | `sstable/index.rs:120` | Building block inside `reader.get` (`data_access.rs:216`); not a query entry. |
| `lookup_raw_key_in_bti_partitions_db` | `bti/parser.rs:1056` | Raw BTI Partitions.db walk; consumed by `lookup_partition_via_bti_trie` (`partition_lookup.rs:91`). |
| `lookup_partition_in_bti_file` | `bti/parser.rs:940` | Low-level BTI file walk; used by writer round-trip tests and as a building block. |
| `compressed_chunk_offset` | `sstable/compression_info.rs:206` | Compression chunk math; internal to chunked readers. |
| `parse_index_header` | `sstable/header_spec.rs:724` | Header parsing primitive. |
| `add_partition_row_index` / `add_entry` / `with_sink` / `write_partition_with_index_blocks` | various writer files | Write-path builders; not read/query surface. |

---

## 6. Dependency understanding — confirmation/correction

Your stated dependency model is **confirmed and refined**:

- **#953 (within-SSTable seek) is prerequisite for #954 (clustering) and #962 (fast path)** —
  CONFIRMED. `scan_partition` currently file-prunes then linearly scans (mod.rs:1036);
  #954's clustering pushdown (`BtiReader::lookup_row`, `select_row_index_blocks_for_range`)
  and #962's "fast path everywhere" both presuppose the within-SSTable byte-offset seek
  that #953 wires (`lookup_partition_via_bti_trie` / `lookup_partition_with_index` /
  `scan_for_key`).
- **#956 (parser UUID/typed literals) is prerequisite for #961 (params)** — CONFIRMED with a
  nuance: #956 unblocks *literal* `WHERE pk = <uuid>` in the equal-predicate path that
  `full_partition_key_lookup` (select_executor.rs:418) consumes; #961 unblocks *bound*
  params. They share the same downstream equal-predicate fast path, so #956 should land
  first so #961's binding has a working literal/value path to feed.
- **#960 (access-path exposure) + #958 (work counters) are infra for testing #953/#962** —
  CONFIRMED. The only existing functional probe today is the test-only
  `scan_for_key_call_count` (`data_access.rs:252`); #958's counters and #960's access-path
  reporting are what let #953/#962 *prove* the seek ran rather than a silent full scan.

Added correction worth flagging to the epic: **prepared SELECTs and `execute_with_params`
route through the legacy `QueryExecutor` (prepared.rs:98, engine.rs:264), not
`SelectExecutor`** — so #962's "apply fast path across all SELECT surfaces" is not merely
adding the fast path in more places; it requires *unifying* prepared/param SELECTs onto the
modern `SelectExecutor` (or porting the fast path into the legacy executor). This is a
larger structural change than #962's title implies and should be called out as a #961↔#962
coupling.
