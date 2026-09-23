# Design — sstable-rekey (issue #4203)

## D1. The premise, and why it makes this a copy, not a rewrite

Every REPAIR/MOVE sibling so far (`salvage`, `rebuild`, `extract`/`split`) exists because SOME part
of an SSTable's bytes needs to be regenerated or reconciled. Rekey is the odd one out: once the
premise is confirmed (proposal.md — table id lives ONLY in the directory name, in every format
CQLite reads), there is nothing inside any component file that needs to change. The core operation
is:

```
for each generation directory under <table-dir>:
    for each component file (Data.db, Index.db|Partitions.db+Rows.db, Statistics.db, Summary.db,
                              Filter.db, CompressionInfo.db (if present), Digest.crc32, CRC.db (if
                              present, BIG uncompressed), TOC.txt):
        stream-copy the file, byte-for-byte, into
        <out>/<table_name>-<new_id_hex>/<same filename>
verify --mode full the new directory before declaring success
```

No `SSTableReader`/`SSTableWriter`/`KWayMerger` involvement — this is `std::io::copy` per file, the
same primitive `salvage`'s own module doc contrasts itself against ("never by scanning Data.db bytes
for a plausible header" — rekey doesn't even open Data.db). The one piece of NEW logic is deciding
the target directory name and validating the id (D2), plus the self-audit (D3).

**This is deliberately NOT reusing `compact_sstables`/`build_single_partition_merger`/
`decode_partition_at_offset_for_salvage`** the way `extract`/`split` (#4199) do — those exist to
change or select DATA; rekey changes neither the data nor its physical layout, only its address. A
generation whose components fail to even OPEN structurally (a pre-`na` version, a corrupt TOC) is
still copyable as bytes, but rekey does not silently copy garbage: D3's self-audit is what catches
that, not a decode pass during the copy.

## D2. Deriving and validating the target directory name

- **Table name is unchanged.** `extract_table_name` (`storage/sstable/mod.rs`, delegating to
  `snapshot_path::extract_table_name`) already derives it from the SOURCE directory; rekey reuses it
  read-only and never accepts an operator-supplied table name (the issue's CLI shape has no such
  flag — only `--table-id`).
- **`--table-id <uuid>` accepts either form.** Standard dashed (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`,
  36 chars — matches `query/parser.rs`'s existing `is_uuid_literal`/`parse_uuid_literal` shape,
  reused or mirrored rather than reimplemented) or bare 32 lowercase hex chars (the raw directory
  suffix form, `discovery/scanner.rs`'s `has_cassandra_table_uuid_suffix` shape). Either is
  normalized to the 32-lowercase-hex directory form before use. A value that is neither (wrong
  length, uppercase, non-hex, malformed dash positions) is a usage error naming what was expected.
- **Target directory:** `<out>/<table_name>-<normalized_id>/`. `--out` must be empty going in (the
  salvage/rebuild convention); the target subdirectory therefore cannot collide with anything already
  there.
- **The new id must differ from the source's own id** — rekeying to the SAME id is almost certainly
  an operator mistake (nothing changes, but it silently "succeeds" and produces a directory
  indistinguishable from a plain copy of the input under a different `--out`). Refused as a usage
  error naming the id the source already has, with an escape hatch (`--allow-same-id`) for the
  legitimate case (an operator using rekey purely as a verified-copy tool). Flagged as a UX choice,
  not load-bearing to the acceptance criteria — reasonable to drop `--allow-same-id` and just always
  refuse a same-id request if the owner prefers a smaller surface.

## D3. Self-audit before publish (same discipline as every REPAIR/MOVE sibling)

Rekey writes to a temp path under `--out` first (`<out>/.rekey-tmp-<table_name>-<new_id>/`), then:

1. Runs `verify --mode full` against the temp directory.
2. On PASS: atomically renames the temp directory to its final
   `<out>/<table_name>-<new_id>/` name (one `rename(2)`, the same publication-barrier discipline
   `TocWriter`'s doc comment already establishes for a normal flush — the directory either fully
   exists under its real name or not at all, never partially).
3. On FAIL (a source component was already corrupt, or the copy itself was interrupted/truncated):
   REFUSE, remove the temp directory, nothing published under `--out`.

This is cheap here specifically because rekey changes nothing structural — a verify failure after a
byte-for-byte copy is strong evidence the SOURCE was already unhealthy (rekey is not a repair tool;
`salvage`/`rebuild` are), so the refusal message names the failing component and points there.

## D4. `--version`: refuse on any actual change, never partially implement (proposal.md's boundary)

```
requested = --version value, or None (defaults to "whatever the source already is")
source_version = the version marker read from the source's own component filenames
                  (e.g. "nb" from "nb-1-big-Data.db"; "da" from a Partitions.db-bearing generation)
if requested is None or requested == source_version:
    proceed with the D1 copy
else:
    REFUSE: "cqlite rekey does not rewrite formats; cqlite convert (#4202) converts
             <source_version> -> <requested>, then rekey the result"
```

A table directory holding MULTIPLE generations at DIFFERENT source versions (a table mid-upgrade)
is refused as a usage error naming every distinct version found — rekey's `--version` check needs
exactly one source version to compare `requested` against, and silently copying a mixed-version tree
under one new id while claiming a single `--version` was honored would misrepresent what happened.

## D5. Manifest

```json
{ "input": "<table-dir>", "output": "<dir>", "table_name": "…",
  "source_table_id": "<32-hex>", "new_table_id": "<32-hex>",
  "generations": [ { "generation": 1, "version": "nb", "components": ["Data.db", "Index.db", …],
                      "bytes_copied": 4194304 } ],
  "verify": "pass",
  "refused": null | { "reason": "version-change-requested|verify-failed|mixed-source-versions|
                                  same-table-id|malformed-table-id",
                       "detail": "…", "remedy": "cqlite convert (#4202)" | null },
  "now": "<RFC3339>", "cqlite_version": "…" }
```

## D6. Failure contract

| Situation | Behaviour | Exit |
|---|---|---|
| every generation copied and the new directory verify-clean | published under `--out`; `refused: null` | 0 |
| `--version` requests an actual format change | REFUSE before any copy; remedy names `cqlite convert` (#4202) | 2 |
| the copied output fails `verify --mode full` | REFUSE: temp dir removed, nothing published | 2 |
| pre-`na` source version | REFUSE via inherited `BigVersionGates`/`BtiVersionGates::from_version` `Error::UnsupportedVersion`, never a rekey-specific reimplementation of the floor | 2 |
| a table directory mixes multiple source versions across generations, with `--version` given | usage error naming every version found | 1 |
| malformed `--table-id`, new id equals source id (without `--allow-same-id`), `--out` non-empty, input dir has no recognizable table-id suffix | usage error | 1 |

## D7. Oracles (all Cassandra-written or Cassandra-read; never CQLite alone, #3042)

| Case | Fixture | Expected result derived by |
|---|---|---|
| rekey, single generation, BIG | any committed `test_basic` table | output dump under the new id equals the source's OWN sstabledump golden (bytes are literally identical, so this is a strong but expected pass — the point is proving nothing besides the directory name changed) |
| rekey, BTI | any committed `test_da` table | same, BTI-side |
| rekey, multi-generation | a committed multi-generation table | every generation copied, `Statistics.db` etc. per generation byte-identical to source (sha256, not just dump-equal) |
| Cassandra-side readback (AC2) | rekeyed output placed at Cassandra's own `data/<ks>/<table>-<new_id>/` after a matching `DROP`+`CREATE TABLE` assigning that exact id (or a Cassandra test harness that can pin a table id), via `e2e-cassandra-readback.sh` | Cassandra's own `sstableloader`/restart-and-query path accepts the directory and returns the same rows `SELECT *` returned against the ORIGINAL id — the authority that no internal cross-check against the old id exists anywhere Cassandra itself reads |
| pre-`na` input | any legacy-version fixture available in the corpus (or a synthesized directory with an `ma`/`me`-prefixed filename) | `Error::UnsupportedVersion` with `floor: "na"`, matching `BigVersionGates::from_version`'s existing behavior verbatim |
| `--version` requesting a real change | a committed `nb` table, `--version da` | refusal names `cqlite convert` (#4202); nothing published |

## D8. CLI shape, campsite, and the shared write guard

`cqlite-cli/src/commands/rekey.rs`, wired like `salvage.rs`/`rebuild.rs`; `Commands::Rekey` in
`cli_types.rs`. `--table-id` parsing (D2) lives beside the command, reusing (not duplicating) the
hyphen-stripping shape `query/parser.rs::parse_uuid_literal` already established for a CQL `uuid`
literal — if that function is not `pub(crate)`-reachable, this change makes it so rather than
hand-rolling a second hex/dash parser.

**Write guard**: `--out`/`--manifest` are validated through `commands/write_guard.rs`
(`WriteGuard::new`, `resolve_write_target`) — the module #4199 (`sstable-extract-split`) promotes out
of `commands/salvage/write_guard.rs`. **This is a hard dependency**: rekey's tasks.md group 0 starts
by confirming that module exists at the crate-visible path before writing any rekey CLI code: if
#4199 has not yet landed, this change either waits or (task 0, fallback) promotes the guard itself
using the identical relocation #4199's design already specifies, so the two changes converge on one
copy of the guard rather than each writing their own.

New core module is new files only (`storage/write_engine/rekey/`: `mod.rs`, `table_id.rs` (D2),
`copy.rs` (D1/D3)), so the file-size ratchet is not engaged by construction; `cli_types.rs` growth is
one variant.
