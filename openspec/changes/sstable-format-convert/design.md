# Design — sstable-format-convert (issue #4202)

## D1. Why not `compact_sstables`: it has no format-preservation knob

`merge/mod.rs::compact_sstables`/`compact_sstables_with_registry` (`input_paths: Vec<PathBuf>,
output_dir, schema, generation, gc_before_secs, now_secs, purge_safe`) takes no target-format
parameter at all. Salvage's own tasks.md (#4196, task 2.3) discovered this directly: "`compact_sstables`
has no format-preservation knob — it always emits BIG output regardless of input format," which is
why salvage's BTI healthy-parity test could not use `compact_sstables` as its byte-for-byte oracle
and fell back to a content-parity (decode-and-compare) check instead.

`convert` therefore cannot be a thin wrapper around `compact_sstables`. It is a new, small function
that does what `compact_sstables` does internally — build a `KWayMerger` over exactly ONE input path
(same "one input = a closed, self-contained compaction" framing salvage's R1 and `explain`'s D3
establish), drive it `purge_safe(false)`, `gc_before_secs: None`, `now_secs: None` so nothing is
purged and nothing is shadowed beyond ordinary same-partition reconciliation — but writes through
`SSTableWriter::with_format(output_dir, generation, schema, capacity, target_format)` with
`target_format` an EXPLICIT parameter, independent of the input's own family. This is the one
combination no existing writing verb exercises: salvage's `with_format` call always matches the
SOURCE family (BIG in, BIG out; BTI in, BTI out); `compact`'s hardcodes BIG. `convert` is the first
verb where target format is a free variable.

```
input generation (ONE, either family)
      │  full decode via the existing single-input KWayMerger construction
      │  purge_safe(false), gc_before_secs: None, now_secs: None  (nothing purged, nothing dropped)
      ▼
Vec<Mutation> / CompactionRow stream  (the SAME format-independent intermediate
                                        representation compact_sstables and salvage both
                                        already produce before handing rows to a writer)
      ▼
SSTableWriter::with_format(output_dir, generation, schema, capacity, TARGET format)
      ▼
ConvertReport { input_format, output_format, generation, partitions, rows, statistics_carried[],
                statistics_not_representable[] }
```

Premise to re-confirm in the worktree (tasks.md 0.1): that the merge/reconcile pipeline's row
representation is genuinely format-independent all the way to the writer boundary — i.e. nothing
upstream of `SSTableWriter::write_partition` inspects or assumes the INPUT's format when producing
the row stream the writer consumes. `salvage`'s `merge_entry_to_mutation` → `write_partition` path
already crosses this boundary once per partition for the SAME-family case; `convert` is betting nothing
in that path is family-conditional. If task 0.1 finds otherwise, that is a STOP-and-report premise
failure, not a design change to paper over silently.

## D2. Statistics field accounting (proposal.md AC3)

Enumerated from `StatisticsMetadata` (`writer/stats_writer/metadata.rs`) — every field, and its
representability in each direction:

| Field | BIG (`nb`) | BTI (`da`) | Convert behavior |
|---|---|---|---|
| `min_timestamp` / `max_timestamp` | yes | yes | preserved, tested equal both directions |
| `min_local_deletion_time` / `max_local_deletion_time` | yes | yes | preserved, tested equal |
| `min_ttl` / `max_ttl` | yes | yes | preserved, tested equal |
| `partition_count` / `row_count` / `column_count` / `total_rows_size` | yes | yes | preserved, tested equal |
| `tombstone_histogram` | yes | yes | preserved (same `StreamingTombstoneHistogramBuilder` shape either format); tested equal |
| `first_key` / `last_key` | yes | yes (`da` `StatsMetadata.firstKey`/`lastKey`, `hasKeyRange`) | preserved, tested equal |
| `has_partition_level_deletions` | **NOT representable** — the metadata.rs doc states plainly: "Ignored by the legacy BIG (nb/oa) STATS body, which never serialises this field" | yes (`da` `StatsMetadata.hasPartitionLevelDeletionsPresenceMarker`, cassandra-5.0.0 `StatsMetadata.StatsMetadataSerializer.serialize` line 495) | BTI→BIG: field is DROPPED, named in `statistics_not_representable` with the source citation above. BIG→BTI: computed fresh from the decoded partition stream (the writer already does this on every write — confirm in task 0.2, not re-derived by `convert` itself) |
| `repaired_at` / `pending_repair` / `is_transient` | yes | yes | preserved via the SAME `set_repair_state` mechanism `salvage`'s D4 already uses, read from source Statistics when readable |
| `estimated_partition_size` / `estimated_cell_count` | yes (own seed shape, EH(155)/EH(118) per #1327) | yes | preserved: BOTH formats use the identical seed shapes (#1327 finding 1 is about partition-size vs cell-count seeds, not a BIG-vs-BTI difference) — verified in task 0.3, not assumed |

**Compaction ancestry** (the issue body's "compaction ancestry where the format has it"): confirmed
ABSENT from `StatisticsMetadata` entirely — neither format carries an ancestors field in the `na`+`nb`
/ `da` shapes CQLite targets (not documented anywhere in `docs/sstables-definitive-guide/`, and no
`ancestors`-named field exists in `writer/stats_writer/metadata.rs`). The parenthetical resolves to
"neither format has it" — stated plainly in R3's scenario rather than silently ignored, since a
reader could otherwise read the issue's phrasing as promising a field that does not exist in either
format CQLite writes.

## D3. Oracles — two different claims need two different oracle classes

**AC1 (reconciliation preservation)** is an internal-consistency claim: did `convert` alter or drop
any cell, tombstone, or row while changing format? The oracle is CQLite's own already-parity-proven
no-purge single-input merge (the SAME mechanism `salvage`'s R1 uses as ITS oracle) — legitimate here
because the claim under test is "conversion preserves what reconciliation already proved it preserves
for salvage," not a claim about Cassandra's on-disk byte layout.

**AC2 (BTI structural correctness)** is a framing/encoding claim, and #3042's round-trip-invariance
lesson applies directly: a CQLite-written-then-CQLite-read BTI file can be invariant to a uniform
framing defect that cancels between writer and reader (`issue_908_bti_canonical_write.rs` is
EXACTLY this shape — CQLite-written, CQLite-read, asserts only ordering/structure — and it did not
catch #3002's `Rows.db` row-index root base being 2 bytes low, because a compensating encoder defect
canceled it). `convert`'s BTI output MUST therefore be validated the way `issue_3002_bti_rows_root_base.rs`
validates: against Cassandra-written `da` bytes / `cassandra-5.0.8` writer source, not merely against
CQLite's own reader reading its own writer's output. Concretely: `bti-multiclustering` gate
component coverage, PLUS a `convert`-specific structural assertion in the same family as
`issue_3002_bti_rows_root_base.rs` (row-index root base, `NEXT_COMPONENT` framing) run against
`convert`'s actual output, not assumed inherited from the writer's existing unit tests.

**The BIG round-trip target is NOT byte-identical to the untouched source.** Salvage's own round-4
finding (#4196): a healthy zero-clustering-column uncompressed table's salvage output diverged from
`compact_sstables`'s by exactly a re-encoding width difference (20410 vs 19803 bytes, Data.db),
traced to the clustering-column-COUNT (not compression) as the isolating variable — both sides decode
to IDENTICAL rows, so it is not a correctness defect, but it means AC1's "byte-equal to a direct
no-purge compaction of the source" oracle must be checked knowing this quirk can recur for the SAME
shape of table. `convert`'s round-trip test suite deliberately includes a zero-clustering-column
fixture (`test_basic.uncompressed_table`, the exact fixture that triggered it for salvage) so this
change either reproduces the SAME already-diagnosed divergence (expected, content-parity fallback per
salvage's established pattern) or is clean — either way, known and asserted, not silently different
in this new code path.

## D4. Shared write guard — ordering with #4199

`commands/salvage/write_guard.rs` exists TODAY only inside `salvage`'s own module tree
(`pub(super)`-scoped). `sstable-extract-split` (#4199, branch `issue-4199-extract-split`) proposes
relocating it to `cqlite-cli/src/commands/write_guard.rs` (crate-visible `pub(crate)`) as task 0.4 of
ITS OWN tasks.md — at spec time #4199 is itself still spec-review, not implemented (`git log
origin/main..issue-4199-extract-split` shows exactly one commit, the OpenSpec proposal itself).

Two orderings are both valid; task 0.4 below checks which is true at implementation time rather than
assuming:
- **#4199 lands first**: `convert` adopts `cqlite-cli/src/commands/write_guard.rs` directly — pure
  reuse, no relocation work in this change.
- **#4202 lands first**: `convert` performs the SAME relocation #4199's design.md D7 describes
  (`commands/salvage/write_guard.rs` → `cqlite-cli/src/commands/write_guard.rs`, salvage's `use`
  updated, its 600+ lines of existing guard tests unchanged) as ITS OWN task 0.4, so whichever of
  `extract`/`split`/`convert` needs the guard next finds it already shared. Either way, the guard
  itself is moved exactly ONCE — not re-derived a third time (salvage already paid for its
  discovery; #4199 and #4202 must not each write their own copy).

`convert`'s own `WriteGuard` instances protect: the input table directory (read-only, never
written), and the run's own planned `--out/<keyspace>/<table>/` generation directory — the same two
protected-set entries `salvage` registers, per the shared module's existing `INPUT_LABEL`/
`OUTPUT_LABEL` shape.

## D5. CLI shape and campsite

`cqlite-cli/src/commands/convert.rs`, wired like `salvage.rs`; `Commands::Convert` in
`cli_types.rs`. Schema resolution reuses `write.rs`'s existing helpers
(`load_compaction_table_schema_for_table`) unchanged. New core module is new files only
(`storage/write_engine/format_convert/`), so the file-size ratchet is not engaged by construction,
matching salvage's D7 precedent; `cli_types.rs` growth is one variant.

`--format big|bti` is REQUIRED (no default — converting to "whatever the input already is" is a
no-op an operator should not reach for by typo); `--compression` defaults to `none` and is the ONLY
accepted value today (#1406), refusing `Error::UnsupportedFormat` for anything else with a `--help`
line naming the boundary, matching `salvage`'s and `compact`'s existing precedent text.
