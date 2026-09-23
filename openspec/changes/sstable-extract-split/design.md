# Design — sstable-extract-split (issue #4199)

## D1. `extract`, reconciled mode: reuse the point-read merger unchanged

`build_single_partition_merger(paths, keys, schema, scan_cancel) -> Result<Option<KWayMerger>>`
(`cqlite-core/src/storage/write_engine/merge/point_read.rs:246`) already does exactly what
reconciled `extract` needs: given a set of raw partition keys and every candidate SSTable path, it
opens a `KWayMerger` scoped to just those keys, reconciled across generations, **no purge** — the
same engine `query`'s point-read path and the Flight producer's point route use, so extract's output
is provably the same reconciliation Cassandra-equivalent reads already perform (the #1918
point-vs-full differential lane exists precisely to keep this path honest).

```
selection (Key | TokenRange | KeySet)
      │
      ▼  resolve to a concrete key list (D1.1)
build_single_partition_merger(all_generation_paths, keys, schema, cancel)
      │  KWayMerger, reconciled, no purge
      ▼
loop { merger.step() } → MergeStep::Partition{rows,..} → SSTableWriter::write_partition
      ▼
ONE output generation, `verify --mode full` self-audit, manifest
```

This is structurally the SAME merge-step loop `compact_sstables` already drives
(`merge/mod.rs:1350`, `MergeStep::Complete`/`MergeStep::Partition`), just fed a key-scoped merger
instead of a whole-table one and writing to a single fresh generation rather than folding N inputs
into one. No new merge engine.

### D1.1 Resolving a `Selection` to a concrete key list

- `--partition <key>`: the key literal is CQL-encoded through the same schema-driven key-literal
  parser `query`'s `WHERE pk = <literal>` and salvage/rebuild's `--table` disambiguation already
  use. One key.
- `--keys-file <f>`: one key per non-blank line, using the SAME literal encoding as `--partition`.
  **A composite (multi-column) partition key is written as `,`-separated literals on one line**
  (e.g. `tenant_id=42,region='us-east'` — named-column form, not positional, so a file surviving a
  schema column reorder still parses correctly and a typo names the offending column rather than
  silently binding to the wrong position). This is flagged to the owner below as the one CLI
  ergonomics choice this design makes without an existing convention to anchor it to.
- `--token-range <a>,<b>`: **(a, b] — left-exclusive, right-inclusive**, matching Cassandra's own
  token-range convention (`nodetool describering`/`ring` output, and how vnode ranges are
  represented at the ring boundary to avoid double-counting the wrap point). Resolving this to a
  key list walks the boundary source of EVERY input generation (the same walk D3 below performs for
  `split`, reused here across N generations instead of one), collecting every partition whose
  `cassandra_murmur3_token(raw_key)` falls in `(a, b]`, deduped across generations by raw key bytes
  (two generations can both hold the same live partition). This is a full boundary-source scan
  bounded by the input's partition count, not by the width of the token range — a two-partition
  range over a million-partition table costs the same walk as a whole-table range. Documented as a
  known cost, not optimized in this slice (Non-goals).
- Every raw key resolved by any of the three modes that turns out to be present in **zero** input
  generations is collected and reported BY NAME; `--keys-file` with any such key exits 3 (acceptance
  criterion 3). `--partition`/`--token-range` finding nothing is reported the same way (an empty
  `TokenRange` match is not usage error — an operator giving an empty/wrong range must be told
  "0 partitions matched", not get a silently-empty successful output).

## D2. `extract --raw`: no merger, per-generation copy

`--raw` skips `build_single_partition_merger` entirely. For each input generation independently:
walk that generation's OWN boundary source, filter to the resolved selection (same D1.1 key list,
but membership is now per-generation — a key present in generation 3 but not generation 7 produces
output only from generation 3), and for each match call the SAME decode-at-offset primitive salvage
already drives — `decode_partition_at_offset_for_salvage`
(`cqlite-core/src/storage/write_engine/salvage/recover_helpers.rs`) — which already: validates the
chunk CRC the partition's byte range intersects, decodes structurally through the compaction-row
decoder, and returns the partition WHOLE (salvage's D2 partition-atomicity guarantee: a mid-partition
decode failure yields nothing, never a truncated prefix). Extract reuses that guarantee as-is; it
adds no new decode path.

**Unlike salvage, a decode/CRC failure on a REQUESTED partition is a refusal, not a recorded loss and
a continue.** Extract is not advertised as a corruption-recovery tool (that is what `salvage` #4196
and `rebuild` #4197 exist for) — an operator who typed `--partition X` and gets back a manifest
saying "X was lost" instead of the actual reason they cannot open the resulting output has been
handed a worse tool than a clear refusal naming the offset and pointing at `salvage`/`rebuild`. This
is a deliberate, narrower failure contract than #4196's (D5 below), and it is the one place this
design seriously considered mirroring salvage's skip-and-report policy instead — flagged to the
owner.

Each generation that had at least one matched, successfully-decoded partition gets ONE output
generation (same generation number as the source, in a distinct output dir — the salvage D4
convention); a generation with zero matches is silently absent from the output (not an error — a
key legitimately living in only 2 of a table's 5 generations should not force 3 empty outputs).

## D3. `split`: sequential boundary walk, N (or byte-bounded) output writers, no merge

`split` operates on exactly ONE input generation (a bare `Data.db` path, or a table dir that
resolves to exactly one generation — more than one is a usage error naming every generation found
and asking for an explicit `Data.db` path, since "split the whole table" is not this verb's job).

```
boundary source walk, ascending token order (the format's own invariant)
      │  (key, data_offset) in file order == ascending Murmur3 token order
      ▼
for each partition:
   decode_partition_at_offset_for_salvage(...)  ──► Ok(rows) ──► current_writer.write_partition
                                                 └──► Err(e)  ──► REFUSE whole run (D2's contract)
   if boundary crossed (D3.1) → finalize current_writer, verify --mode full it, open next writer
      ▼
finalize + verify the LAST writer
      ▼
SplitReport { parts: [{ generation, token_range: (min,max], partitions, rows }, ...] }
```

No `KWayMerger` is involved: a single healthy SSTable generation has no duplicate partition keys to
reconcile (Cassandra's own on-disk invariant), so `split` is a straight sequential copy driven by the
boundary walk, identical in spirit to `extract --raw`'s per-partition decode-and-write but rolling
across N writers instead of 1. This keeps split from introducing a second merge engine into the
epic — it is, on purpose, the simplest of the four REPAIR verbs' core loops.

### D3.1 Where a part rolls over

- `--parts N`: the boundary source is walked ONCE up front to get the total partition count `T`
  (cheap — boundary entries are small; the same up-front count `rebuild`'s premise work already
  established Index.db enumeration supports). Target size per part is `ceil(T / N)` partitions;
  the writer rolls to the next part once its own partition count reaches that target, UNLESS doing
  so would leave a later part with zero partitions (the last part absorbs any remainder rather than
  the boundary walk under-filling early parts and starving the tail — asserted by the "union ==
  source, N parts, none empty" test).
- `--max-bytes B`: rolls over once the CURRENT writer's accumulated Data.db byte span (tracked from
  each partition's own byte extent in the boundary source, the same measurement rebuild's D1 byte-
  extent walk performs) reaches or exceeds `B`. The roll happens at the next partition BOUNDARY at
  or past `B`, never mid-partition — a part can therefore exceed `B` by up to one partition's width,
  documented in `--help` and the manifest (`target_max_bytes` vs. each part's actual `bytes`).
- Either way, the LAST partition emitted by the boundary walk always closes the LAST part — there is
  no separate "did we reach the end" check to get wrong.

### D3.2 Disjoint, ascending token ranges — by construction, asserted anyway

Because the boundary walk yields partitions in the same ascending-token file order the format
itself guarantees (Index.db entries / the `Partitions.db` trie are written in that order by every
Cassandra/CQLite writer), and a part is exactly a contiguous slice of that walk, disjointness and
ascending order are structural, not a policy this code enforces by comparison. The acceptance test
still asserts it explicitly per part (`min_token(part[i+1]) > max_token(part[i])`) rather than
trusting the construction silently — the same "assert the property, don't just assume the mechanism
that should produce it" discipline #3890's point-vs-full sweep applies to a different structural
guarantee.

## D4. Manifests

### Extract
```json
{ "input": "<table-dir>", "output": "<dir>", "mode": "reconciled|raw",
  "selection": { "kind": "partition|token-range|keys-file", "detail": "…" },
  "generations_written": [ { "generation": 12, "partitions": 1, "rows": 42 } ],
  "not_found": [ "<key literal or hex>" ],
  "refused": null | { "reason": "partition-decode-failed|boundary-source-unreadable",
                       "generation": 12, "data_offset": 88192, "remedy": "cqlite salvage (#4196)" },
  "now": "<RFC3339>", "cqlite_version": "…" }
```

### Split
```json
{ "input": "<Data.db>", "output": "<dir>", "boundary": { "kind": "parts", "value": 4 },
  "parts": [ { "generation": 1, "min_token": -9223372036854775808, "max_token": -3074457345618258603,
               "partitions": 301, "rows": 8842, "bytes": 4194304, "verify": "pass" }, ... ],
  "source_partitions": 1204, "source_rows": 35211,
  "refused": null | { "reason": "multiple-generations|partition-decode-failed|part-verify-failed",
                       "detail": "…" },
  "now": "<RFC3339>", "cqlite_version": "…" }
```

## D5. Failure contract

| Situation | Behaviour | Exit |
|---|---|---|
| every selected partition recovered (extract) / every part verify-clean (split) | output written, manifest `refused: null` | 0 |
| `--keys-file` names a key absent from every generation | output written for the keys that DID resolve; `not_found` lists the rest by name | 3 |
| a selected/enumerated partition fails to decode or fails its chunk CRC | REFUSE the whole run: nothing written under `--out`; manifest names the offset + generation, remedy = `salvage`/`rebuild` | 2 |
| boundary source (Index.db/Partitions.db) unreadable | REFUSE, remedy `cqlite rebuild --components index` (#4197) | 2 |
| a produced `split` part fails its own `verify --mode full` self-audit | REFUSE: nothing published under `--out` (temp-then-publish, the salvage/rebuild `--out` convention) | 2 |
| `split` input resolves to more than one generation, or `--partition`/`--token-range`/`--keys-file` given together, or `--out` non-empty, or `--schema` unresolved | usage error | 1 |

No exit-3 "partial success" path exists for a DECODE failure the way salvage has one — D2 explains
why extract/split refuse instead of skip-and-continue. Exit 3 here means only "the request named
something that was never there" (a `not_found` key), never "something was there and could not be
read cleanly."

## D6. Oracles (all Cassandra-written; never CQLite alone, #3042)

| Case | Fixture | Expected result derived by |
|---|---|---|
| extract, reconciled, single generation | any committed `test_basic`/`test_da` table | `SELECT *` for the key under `CQLITE_READ_PATH=point` and `=full` (#1918), both-directions column compare (#3890) |
| extract, reconciled, key spans multiple generations (an update in gen 2 of a row first written in gen 1) | `test_timeseries` or a synthesized two-flush fixture over a committed schema | reconciled output matches Cassandra's own compacted view of the same two SSTables (byte-for-byte compaction parity, the existing #12x parity harness) restricted to the one key |
| extract `--raw` | any committed multi-generation table | each output generation's dump for the key equals that INPUT generation's own `sstabledump` JSONL golden for the key, tombstones included — never the reconciled view |
| `--keys-file` with an absent key | a keys file mixing present + synthetic-absent keys against a committed table | `not_found` names exactly the absent ones; present keys' output is dump-equal to source |
| split, `--parts N` | a committed table restaged into ONE generation via `compact` | `union(parts)` row-for-row and dump-for-dump equals the staged source; N parts, each `verify --mode full` clean, ranges disjoint+ascending, Statistics per part match a fold over just that part's rows |
| split, `--max-bytes` | same staged source | same union/verify/range assertions; each part's `bytes` is within one partition's width of `--max-bytes` (except possibly the last) |
| decode failure during extract/split | `corrupt_byte_fixture.rs`'s `ClusteringTextLiteral` mutation (the same fixture #3782/salvage reuse) targeted at the requested partition | refusal names the exact offset the mutation targeted; exit 2; `--out` empty |

## D7. CLI shape, campsite, and the shared write guard

`cqlite-cli/src/commands/extract.rs`, `cqlite-cli/src/commands/split.rs`, wired like
`salvage.rs`/`rebuild.rs`; `Commands::Extract`/`Commands::Split` in `cli_types.rs`. Schema resolution
and `--table` disambiguation reuse `write.rs`'s existing helpers (`load_compaction_table_schema_for_table`)
unchanged.

**The destructive-path guard is promoted, not re-derived.** `commands/salvage/write_guard.rs`
(`WriteGuard`, `resolve_write_target`) already encodes the three round-23 findings (F1: a planned
output path can be judged before it exists and then land on a real write; F4: a symlink can point
inside the input; F5: `--out` inside the input tree self-poisons a re-run) as ONE resolve-then-check
helper. This change moves it to `cqlite-cli/src/commands/write_guard.rs` (crate-visible, `pub(crate)`)
with `salvage`'s own `use` updated to the new path — a pure relocation, not a rewrite, so salvage's
existing 600+ lines of tests keep proving the same guard. `extract`/`split` then build their own
`WriteGuard` instances (protecting the input directory/file and each verb's own planned output) from
the shared module instead of writing a fourth copy of F1/F4/F5. This is task 0.4, done FIRST, so the
guard used by both new verbs from day one is the hardened one, not a naive first draft that repeats
the discovery.

New core module is new files only (`storage/write_engine/extract_split/`: `mod.rs`,
`selection.rs` (D1.1), `raw_copy.rs` (D2), `split.rs` (D3)), so the file-size ratchet is not engaged
by construction; `cli_types.rs` growth is two variants.
