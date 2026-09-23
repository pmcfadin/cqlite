# Design — sstable-scrub (issue #4198)

## D0. Source read for this design

Read directly at `cassandra-5.0.8` (never a CQLite `file:line`, #3041):
- `src/java/org/apache/cassandra/io/sstable/IScrubber.java` — the options contract
  (`checkData`, `reinsertOverflowedTTLRows`, `skipCorrupted`) and `ScrubResult`
  (`goodPartitions`, `badPartitions`, `emptyPartitions`).
- `src/java/org/apache/cassandra/io/sstable/format/SortedTableScrubber.java` — the format-agnostic
  scrub loop: `tryAppend`, `OrderCheckerIterator` (row order), `saveOutOfOrderPartition` (partition
  order), `writeOutOfOrderPartitions` (the sidecar), `throwIfCannotContinue` (the abort predicate),
  `FixNegativeLocalDeletionTimeIterator` / `rebuildTimestamptsForOverflowedRows`
  (`--reinsert-overflowed-ttl`).
- `src/java/org/apache/cassandra/io/sstable/format/big/BigTableScrubber.java` — BIG-specific
  index-assisted positional recovery and the counter/2i override of `throwIfCannotContinue`.
- `src/java/org/apache/cassandra/tools/StandaloneScrubber.java` — the CLI options
  (`-s/--skip-corrupted`, `-n/--no-validate`, `-r/--reinsert-overflowed-ttl`, `-m/--manifest-check`,
  the deprecated `-e/--header-fix`), the pre-scrub snapshot, and the in-place obsolete-and-replace.
- `src/java/org/apache/cassandra/io/sstable/format/big/BigFormat.java` /
  `.../bti/BtiFormat.java` — `hasUintDeletionTime` version gating (D6).

## D1. The pipeline: compact_sstables + an order gate, salvage's loss machinery for `--skip-corrupted`

```
every generation of <table-dir>
        │
        ▼
KWayMerger (compact_sstables path, purge_safe = --purge, same gc_before/purge contract as `compact --major`)
        │  (per source, streaming; unchanged from compact)
        ▼
NEW: order gate — as each partition (and, within it, each row) is pulled off the merge heap,
     assert its key/clustering compares >= the previous one under the schema's own comparator
        │                                              │
        │ in order                                     │ NOT in order
        ▼                                              ▼
   primary writer (SSTableWriter,             sidecar accumulator (sorted, same
   same generation number as compact)          comparator) ──► written as
        │                                       <out>/<generation>-outoforder
        ▼                                              (Cassandra: `writeOutOfOrderPartitions`)
ScrubReport { generations[], purge, dry_run, losses[]?, out_of_order[]?, ttl_rewrites[]? }
```

- **Order gate, not order re-sort.** Cassandra's `Scrubber` does not silently re-sort a whole
  partition or table into place — `OrderCheckerIterator` stops returning rows from the primary
  iterator the moment order breaks, and everything after that point (within that partition, and
  every subsequent out-of-order partition) is collected into `outOfOrder` and written separately,
  IN ORDER, to its own SSTable. CQLite's gate mirrors this at the merge-heap level: `KWayMerger`
  already emits partitions/rows in comparator order from ITS OWN perspective (heap routing, see
  `merge/mod.rs`'s module doc) — the gate compares the DECODED source order (as the row physically
  sat in its generation, before the heap re-routes it) against the merge's chosen order, which is
  exactly what "out of order in the underlying SSTable" means. A row/partition that is out of order
  ONLY because two DIFFERENT generations disagree (normal LWW reconciliation) is not a scrub finding
  — it is what compaction is for; the gate keys off deviation from sorted order WITHIN a single
  source's own physical stream, i.e. exactly Cassandra's shape (`prevKey.compareTo(key) > 0` and
  `comparator.compare(next, previous) < 0`, both intra-source comparisons in the source text above).

- **`--skip-corrupted` calls #4196, does not reimplement it.** When the order gate or the merge's
  own decode hits a partition that fails to decode, `--skip-corrupted` routes it through
  `salvage::decode_partition_at_offset_for_salvage` (same function, same `LossClass` enum:
  `chunk-crc | decode | key-mismatch | truncated`) rather than a second, scrub-owned decode path.
  Two implementations of "does this partition survive" is exactly the drift #4196's design
  explicitly avoided by sharing one `LossClass`; scrub reuses the SAME module, not a fork of it.
  Without `--skip-corrupted`, a corrupted partition is a REFUSAL (see D3/D4 — this is scrub's own
  fail-closed choice, and D4 states plainly that it is NOT what Cassandra's default does for a
  regular table).

## D2. Out-of-order fixture and the sidecar's own byte oracle

The committed fixture is a documented, fixed-offset byte mutation of a real Cassandra-written
Index.db (BIG) that swaps two partition-index entries — no bytes in Data.db change, only the ORDER
in which the boundary source names them, so the fixture is deterministic and minimal (one swap, one
partition pair). Expected outcome is derived purely from the healthy original's Index/`sstabledump`:
the two swapped partitions are exactly the sidecar's contents, everything else is exactly the main
output. A second, SEPARATE fixture (or a documented deferral — see the tasks' premise step) covers
intra-partition row order (CASSANDRA-12127's shape: two rows within one partition swapped) — this is
a genuinely different code path (`OrderCheckerIterator` vs `saveOutOfOrderPartition`) and Cassandra
tests it as a distinct case; scrub's spec requires it be NAMED even if this slice defers the fixture
(see NEEDS-YOU in the report).

## D3. Failure contract

| Situation | Behaviour | Exit |
|---|---|---|
| every generation already in order, nothing corrupted, no overflowed-TTL rows | primary output(s) only; report has empty `out_of_order`, `losses`, `ttl_rewrites` | 0 |
| out-of-order material found | primary output(s) + `-outoforder` sidecar(s); report names every diverted partition/row | 3 |
| `--skip-corrupted` given, some partitions lost | primary output(s) + report naming every loss (same shape as salvage's `losses[]`) | 3 |
| `--skip-corrupted` given, `--reinsert-overflowed-ttl` given, N rows rewritten | primary output(s); report names the rewrite count | 3 |
| a corrupted partition found and `--skip-corrupted` NOT given | REFUSE: nothing written under `--out`; report names the partition, its offset, and that `--skip-corrupted` is the remedy | 2 |
| input includes a counter table | REFUSE unconditionally (Non-goals) — citing the write-engine's existing counter restriction, never attempting a partial scrub | 2 |
| boundary source unreadable for a generation | REFUSE that generation (same `boundary-source-unreadable` class + `rebuild` remedy as salvage); scrub does not invent a second refusal vocabulary | 2 |
| `--out` non-empty, table dir has no generations, schema unresolved | usage error, nothing written | 1 |
| `--dry-run` | never writes under `--out`; exit code is what the WET run would have produced (R7 requires the two to match field-for-field, exit code included) | matches wet run |

Never a shorter output presented as success: exit `0` is reachable only when nothing needed
reporting, mirroring the #4196 "affirmative zero" doctrine (`out_of_order`/`losses`/`ttl_rewrites`
are each an affirmative, sized list — never a bare unmeasured `0`).

## D4. Parity table — what matches Cassandra's `Scrubber`/`StandaloneScrubber`, what deliberately doesn't

This is the deliverable the epic asked this proposal to spell out explicitly.

| Cassandra behaviour | CQLite `scrub` | Match? |
|---|---|---|
| Out-of-order partitions/rows diverted to a separate, sorted output SSTable | Same: `<out>/<gen>-outoforder`, sorted | **Yes** |
| `-r/--reinsert-overflowed-ttl`: rewrite rows with `localDeletionTime == INVALID_DELETION_TIME` (CASSANDRA-14092) to the 2038 cap, timestamp+1 | Same rewrite rule, applied only where the FORMAT can even have the bug — see D6 | **Yes, version-gated identically to Cassandra's own `hasUIntDeletionTime()`** |
| `-s/--skip-corrupted`: without it, abort scrub entirely on the FIRST corrupted partition — **but ONLY for counter tables** (`isCommutative`); a REGULAR table's default (flag absent) is to SKIP the corrupted partition, count it in `badPartitions`, and CONTINUE — never abort, never refuse the whole run | Without `--skip-corrupted`: **REFUSE the whole run** on ANY corrupted partition, regular or not | **NO — deliberate divergence, NEEDS-YOU.** See below. |
| `-s` for a 2i (secondary index) table: ALWAYS abort regardless of the flag, telling the operator to rebuild the index instead | N/A — CQLite's write engine has no secondary-index maintenance surface | **N/A, not applicable** |
| Index-assisted positional recovery: on a data-file read error, `BigTableScrubber` retries once from the Index's recorded byte position for that same partition before giving up | Not reproduced as a distinct retry step — scrub's decode-at-offset (via salvage) reads directly at the boundary-source-named offset in the first place, so there is no "data file drifted from the index" retry to perform; the boundary source IS the position of record from the start (same principle #4196's D1 already established: "the boundary source is authoritative") | **Behaviourally equivalent outcome (a corrupted partition is still classified and skipped/refused), different mechanism — not a gap** |
| Duplicate-row merging within a partition (2.x-migration `RowMergingSSTableIterator`, CASSANDRA-12144) | Out of scope — CQLite targets 5.0-native SSTables only (`na`+/`nb`/`da`); a pre-`na` migration artifact cannot exist in an in-scope input | **N/A by version floor** |
| `-n/--no-validate`: skip Cassandra's extra per-cell column-validator pass during scrub (default: validate) | No opt-out — CQLite's schema-aware decode IS the validation; there is no cheaper decode path to fall back to | **N/A, no CQLite analogue (Non-goals)** |
| `-m/--manifest-check`: repair a Leveled Compaction Strategy manifest | Not implemented — CQLite's offline tools do not model a live LCS manifest | **N/A (Non-goals)** |
| Pre-scrub snapshot, then obsolete (delete) originals and replace with scrub output, in place | Never touches or deletes input; always writes to `--out` (epic #4192 binding constraint) | **NO — deliberate, doctrine-driven divergence (Non-goals), not a gap** |
| No dry-run mode exists at all | `--dry-run` computes and reports everything, writes nothing | **CQLite-only addition, not a Cassandra behaviour to match** |

### The counter-table / default-abort divergence (NEEDS-YOU)

Cassandra's actual default (flag absent, regular table) is **skip-and-continue**, not abort. Issue
#4198's acceptance criteria, as groomed, state the opposite ("without the flag: refuse, exit 2,
nothing written — matches Cassandra `Scrubber` default") — that phrasing is **not accurate** against
the source read for this design; it is accurate only for the counter-table case, which CQLite's
write engine cannot round-trip anyway (Non-goals) and MUST therefore refuse regardless of the flag.
Two ways to resolve this, both defensible, and the choice is the owner's:

- **(a) Keep CQLite's stricter default (this proposal's D3/D4 above): refuse the whole run on ANY
  corrupted partition unless `--skip-corrupted` is given**, for every table shape, not only counters.
  Rationale: CQLite's `--skip-corrupted` is destructive-and-silent-by-omission in exactly the way
  #28 (no-heuristics) and the epic's "fail closed, name what's wrong" doctrine push against a
  default that quietly drops data; an operator who wants Cassandra's permissive default can pass
  the flag every time. This is what the acceptance criteria, as literally read, describe — treat
  the AC's Cassandra citation as aspirational/mistaken rather than binding.
- **(b) Match Cassandra's actual default: skip-and-continue for a regular table without the flag**,
  and reserve refusal for the (CQLite-unreachable) counter case. Rationale: fidelity to the cited
  oracle, and consistency with `salvage`, which ALREADY drops undecodable partitions unconditionally
  — a scrub whose default is stricter than salvage's is a second, different default philosophy
  in the same epic for the same underlying loss class.

This proposal recommends **(a)** — keep the strict default — because CQLite's own doctrine (fail
closed, name what's wrong, #4159 class) already generally sets a HIGHER bar than Cassandra's
historical defaults elsewhere in this epic (e.g. #4196 salvage's own refusal-on-boundary-loss is
already stricter than anything Cassandra does), and because `--skip-corrupted` costs the operator
nothing to pass when they mean it. But this is a product posture call, not a format fact, and is
listed as this proposal's #1 open decision.

## D5. `--reinsert-overflowed-ttl` version gating (D6 in the source read)

`FixNegativeLocalDeletionTimeIterator` only fires when
`!sstableVersion.hasUIntDeletionTime()`. That predicate is:

- **BTI (`BtiFormat.BtiVersion.hasUIntDeletionTime()`): hard-coded `true`, unconditionally.** CQLite
  supports only BTI `da`. So for every BTI input scrub will ever see, `--reinsert-overflowed-ttl` is
  a **structural no-op** — there is no overflow to fix because the format cannot represent one.
- **BIG (`BigFormat.BigVersion.hasUintDeletionTime`): `version.compareTo("oa") >= 0`.** CQLite
  supports BIG `na`/`nb`, both lexically less than `oa` (`'n' < 'o'`), so `hasUIntDeletionTime()` is
  **`false` for every BIG version CQLite supports** — meaning CASSANDRA-14092's overflow IS
  representable, and `--reinsert-overflowed-ttl` is meaningful for every in-scope BIG fixture.

**Consequence for the spec:** the BTI arm of R5 (reinsert-overflowed-ttl) is a NAMED no-op
assertion (fixture has a BTI table with no rewrite possible; asserted: 0 rows rewritten, exit
matches "nothing to report"), never silently skipped as "not applicable" — an untested no-op reads
identically to an untested real path, which is exactly the affirmative-zero lesson #4196 already
learned the hard way (D3 there). The BIG arm is where the real rewrite logic is exercised, using the
`issue_1011_ttl_local_deletion_parity` fixture family the issue already names.

## D6. Report schema (stable; CLI text is a rendering of it)

```json
{ "table_dir": "<path>", "output": "<dir>", "dry_run": false, "purge": false,
  "skip_corrupted": true, "reinsert_overflowed_ttl": false,
  "generations": [
    { "generation": 12, "format": "nb", "compressed_input": true,
      "partitions": { "total": 400, "written": 398 },
      "out_of_order": { "partitions": 1, "rows": 0,
                         "sidecar": "<out>/12-outoforder/nb-12-big-Data.db" },
      "losses": [ { "key_hex": "…", "data_offset": 88192, "chunks": [12],
                     "class": "chunk-crc", "message": "…" } ],
      "ttl_rewrites": 0,
      "refused": null } ],
  "now": "<RFC3339>", "cqlite_version": "…" }
```

Field ORDER is the serialization order of `ScrubReport`
(`cqlite-core/src/storage/write_engine/scrub/mod.rs`); every field is verified against a real report
emitted by the compiled binary, not read off the struct in isolation (the #4196 C-audit found four
manifest-schema drifts this way — the same discipline applies here from the first PR, not added in
round 20).

## D7. Oracles (all Cassandra-written; expectations derived from the format, never from CQLite)

| Case | Fixture | Expected outcome derived by |
|---|---|---|
| Healthy multi-generation table, no `--purge` | `test_basic`/`test_da` table, ≥2 generations | byte-identical to `compact_sstables(purge_safe=false)` over the same generations |
| Out-of-order partitions (BIG) | committed fixed-offset Index.db entry swap over a Cassandra-written original | swapped pair named by the clean source's Index; everything else unchanged |
| `--skip-corrupted` on `test_comp_corrupt/data_db_bit_flip` | same fixture #4196 uses | loss set field-for-field equal to `salvage`'s loss set for the same input |
| `--reinsert-overflowed-ttl`, BIG `na`/`nb` | `issue_1011_ttl_local_deletion_parity` family | `cassandra-5.0.8` `FixNegativeLocalDeletionTimeIterator`/`hasUintDeletionTime` (D5) |
| `--reinsert-overflowed-ttl`, BTI `da` | any committed `test_da` table | asserted no-op (D5) — `BtiFormat.hasUIntDeletionTime() == true` |
| Counter-table input | a schema fixture with a `counter` column | refused unconditionally, citing the write engine's existing counter restriction |
| `--dry-run` vs wet run | any of the above | recursive sha256 of `--out`'s parent unchanged after `--dry-run`; report equal field-for-field to the wet run's report (dry_run flag excepted) |

## D8. CLI shape and campsite

`cqlite-cli/src/commands/scrub/` (new directory, mirroring `salvage/`'s file-size discipline —
new files only). `Commands::Scrub` in `cli_types.rs`. Reuses `compact`'s schema/generation-discovery
helpers and salvage's `write_guard`/path-resolution helper (R7.8's ONE guard, not a second one) for
`--out`/`--manifest`, since scrub has the identical destructive-path-argument hazard salvage's
round-23 findings covered — those findings are import, not rediscovery.
