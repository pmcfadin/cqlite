# consistency-audit — issue #4195 (epic #4192, DETECT slice)

**Milestone:** unmilestoned (owner convention; board `Ready` → claimed). **Priority:** P2.
**Routing:** design-driven (OpenSpec + Seam 1) — extending `cqlite verify`'s report/mode shape and
choosing the PASS/FAIL/SKIP invariant-checklist contract have real latitude. The underlying
detection is oracle-bound: every invariant this change checks is derived from the format's own
authoritative components (`Index.db`, `Summary.db`, `Filter.db`, `CompressionInfo.db`, the BTI
`Partitions.db`/`Rows.db` tries, `Statistics.db`) against each other or against `Data.db`, and
correctness is checked against Cassandra-written healthy fixtures plus a committed byte-mutated
corpus, per fixture where available a captured `sstableverify --extended` verdict (issue #1236
pattern).

## Why

`cqlite verify` (epic #970) already checks that each component is **internally** well-formed
(`Index.db` parses, `CompressionInfo.db` offsets are in-bounds, the BTI tries walk). It does not
check that components **agree with each other**: an `Index.db` entry can parse cleanly yet point at
the wrong byte in `Data.db`; a `Summary.db` sample can name an `Index.db` position that no longer
holds the key it claims; `CompressionInfo.db`'s chunk table can be internally consistent yet fail to
account for the last bytes of `Data.db`; `Statistics.db`'s declared min/max timestamp can silently
drift from what a real scan finds. None of that is corruption of any *one* file — it is disagreement
between files, and today's `verify` cannot see it. Owner ruling 2026-09-09: this is the DETECT
program's second cross-component slice (pairs with #4194's per-finding location detail).

**A direct read of `cqlite-core/src/storage/sstable/verify.rs` (2761 lines) during this activation
found that several of the issue's eight invariants are already substantially implemented** under
`VerifyMode::Full`, which changes this change's real scope:

| # | Invariant (issue text) | Status found in `verify.rs` |
|---|---|---|
| 1 | Index.db entry position → Data.db partition header, key match (BIG) | **Gap.** `check_big_index` validates only that `Index.db` itself parses; nothing cross-checks its positions against `Data.db`. |
| 2 | Summary.db sample key exists in Index.db at the recorded position | **Gap.** `check_summary` only confirms `Summary.db` parses. |
| 3 | Bloom filter has no false negative for any key in Data.db | **Half-done.** `check_filter_false_negatives` (issue #1398) already does this for **BIG**, probing `Filter.db` against `Index.db`'s authoritative key set. It is explicitly BIG-only today (comment: "BTI is immune... bloom bypassed for the trie" — true for the READ path, but Cassandra still writes `Filter.db` for `da` SSTables, confirmed present in the `bti_partitions_footer_flip` fixture directory). |
| 4 | BTI: every trie leaf reachable + payload offset in-bounds/lands on that key; Rows.db roots in-bounds | **Mostly done.** `check_bti_structure` + `bti_partition_identity_mismatch` (issue #1103) already resolve every leaf's payload to a raw key and cross-check it by IDENTITY against a Data.db scan; `Rows.db` root/offset bounds are already checked (`BtiTrieCorrupt`). **One explicit gap**: a `DataOffset` leaf's position is never bounds-checked against `Data.db`'s length directly — today it is only caught indirectly, as a scan error, if and when a FULL scan runs. |
| 5 | TOC.txt lists exactly the files on disk (extra AND missing) | **Done.** `check_toc_and_presence` already checks both directions. |
| 6 | Digest.crc32 == CRC32(Data.db) | **Done.** `check_digest`. |
| 7 | CompressionInfo chunk table covers exactly Data.db's length | **Gap.** `check_compression_info` bounds-checks every offset (`< data_len`) but never confirms the chunk table's declared chunk count actually *tiles* `data_length`/`Data.db`'s real size — a short chunk table (missing a final chunk) passes today. |
| 8 | Statistics min/max timestamp, min/max LDT, partition count vs a full scan | **Gap.** `check_statistics` only validates that `Statistics.db` parses; nothing cross-checks its declared values against a real scan. |

This is not a green-field build: it is four genuinely new cross-component checks (1, 2, 7, 8), one
format extension of an existing check (3, to BTI), one bounded-scope addition to an already-strong
check (4's `DataOffset` bounds gap), and two invariants (5, 6) that are already correct and need only
to be included in the new mode's checklist.

## What changes

**Library (`cqlite-core`).** A new `VerifyMode::Audit` (third mode, alongside `Quick`/`Full`) runs a
closed set of 8 named invariant checks and reports each as `Pass | Fail | Skip(<cause>)` — additive
to (not a replacement of) the existing `findings: Vec<VerifyFinding>` contract: every `Fail` row also
pushes a `VerifyFinding` (new `VerifyErrorClass` variants: `IndexPositionKeyMismatch`,
`SummarySampleMismatch`, `BtiDataOffsetOutOfBounds`, `CompressionCoverageGap`,
`StatisticsScanMismatch`; invariants 3/5/6 reuse `FilterFalseNegative`/`MissingComponent`/
`DigestMismatch`, already defined) so existing report consumers see the same detail they always have.
`VerifyReport` gains `pub audit: Option<AuditSummary>` (populated only when `mode == Audit`), where
`AuditSummary` carries exactly 8 named rows, always — a row that could not run names why
(`Skip("no CompressionInfo: uncompressed")`, `Skip("boundary source unreadable")`, `Skip("requires
--deep")`), never a silently absent row. A `--deep` flag (valid only combined with `--mode audit`;
a usage error otherwise) additionally runs invariant 8, which needs a full decode pass to compute
observed min/max timestamp, min/max local-deletion-time and partition count for comparison against
`Statistics.db`'s declared values — the one invariant this change cannot check without reading whole
partitions, so it is the only one gated behind an explicit opt-in. Every other invariant (1, 2, 3, 4,
5, 6, 7) is designed to read only `Index.db`, `Summary.db`, `Filter.db`, `TOC.txt`, `Digest.crc32`,
`CompressionInfo.db`, the BTI index components, and — for invariant 1 only — the `Data.db` **partition
headers** the index names (via the existing point-read header-read path, never a full partition
decode), matching the issue's runtime-bound acceptance criterion.

**CLI (`cqlite-cli`).** `cqlite verify --mode audit <dir> [--deep] --out json|text` renders the 8-row
checklist (existing report shape extended, never reshaped, matching #4194's convention). Exit `2` on
any `Fail`; exit `2` (not `0`) on an all-`Skip` audit, naming `audit: 0 invariants MEASURED`
(affirmative-zero doctrine — a vacuous audit must never look like a clean one).

## Extends `cqlite verify` — not a new verb

This change extends `cqlite verify` with a third `--mode` value rather than adding a new verb
(`cqlite audit`, or similar). Recommendation, not treated as an open product question, because the
code evidence above makes it a low-ambiguity engineering call: five of the eight invariants are
*already* checks living inside `verify.rs`'s existing pipeline (`ComponentSet`, `findings: Vec<...>`,
the QUICK/FULL dispatch), sharing its report struct, its CLI rendering, and its exit-code convention.
A new verb would either duplicate that entire pipeline or awkwardly re-invoke `verify` internally for
no benefit — and the issue's own acceptance criteria already write the invocation as `cqlite verify
--mode audit`. The precedent is direct: #4194 (corruption-locator, PR #4252, in flight) extends the
same `VerifyReport`/`VerifyFinding` shape additively rather than inventing a new report type, and
adds a genuinely NEW verb (`sweep`) only for something `verify` structurally cannot do (walk a whole
data directory) — audit has no such structural mismatch with `verify`'s existing one-generation
scope. This is still surfaced to the owner at Seam 1 as a recommendation with rationale (not left
silent), consistent with epic #4192's own open NEEDS-YOU #1 ("separate verbs vs a namespace") — but
it is not blocking: the epic's UX question is about the family of tool verbs as a whole (`salvage`,
`sweep`, `scrub`, …), not about whether THIS invariant checklist belongs inside `verify`.

## What this change must establish

1. **Every invariant check is oracle-bound to the format's own authoritative components** — no
   invariant is checked by re-deriving CQLite's own prior behavior (#3042 doctrine); expected
   outcomes for the new corruption fixtures are computed by the test from the CLEAN source.
2. **A `Skip` is never a silent pass.** Every row that could not run — an absent optional component,
   an unhealthy boundary source, `--deep` not supplied for invariant 8 — is `Skip(<cause>)`, and a
   report where every row is `Skip` is a FAILING exit code naming the affirmative-zero condition.
3. **Fail-closed on damaged boundary sources**, mirroring #4194's D2: an invariant whose own
   authoritative source (e.g. `Index.db` for invariant 1) is itself corrupt reports `Skip("boundary
   source unreadable")`, not a guess built on unreliable data.
4. **The existing `VerifyMode::Quick`/`Full` behavior, and the existing `findings`/report fields, are
   completely unchanged** — `Audit` is additive, matching #4194's own "additive, never reshaped"
   convention on the same struct.
5. **Runtime bound**: `--mode audit` without `--deep` never reads a whole partition — invariant 1
   reads only the partition HEADER bytes the index names (via the existing point-read header path),
   never a full row decode.
6. **`sstableverify --extended` parity** recorded per fixture, per the manifest's existing
   `cassandra_verdict`/`verdict_parity` convention, for the invariants Cassandra's own tool also
   checks (digest, chunk CRC coverage) — divergent verdicts recorded, not silently reconciled (the
   #1236 pattern already used throughout `corruption-manifest.yml`).
7. **Wiring evidence from the binary**, both formats where the invariant applies, exercised through
   `cqlite verify --mode audit` (and `--deep`).

## Non-goals

- Fixing anything the audit finds — the sweep (#4194, in flight) already calls per-generation
  `verify` for its own severity rows; this change only adds invariants to what a single `verify` call
  can report, it never writes.
- Repair, salvage, or rebuild of any kind (#4196, #4197).
- Corruption **location** (#4194) — this change's findings carry `location: None`, same as most
  existing FULL-mode findings; wiring `Location` onto the five new `VerifyErrorClass` variants is a
  natural #4194-style follow-up, not attempted here (`Location` resolution needs its own boundary-
  source-health story per finding class, which this change's fail-closed `Skip` handles differently).
- A files-only performance report (#4204) — invariant 8's `--deep` pass exists to check *correctness*
  of `Statistics.db`'s declared values, not to report throughput.
- Extending invariant 3 (bloom) or invariant 7 (compression coverage) to a corpus fixture family that
  does not exist in the committed/fetched corpus for a given format — where a combination is
  genuinely absent (e.g. no compressed BTI fixture), this change documents it as a declared gap
  rather than manufacturing one.

## Impact statements (openspec rules)

- **No-heuristics (#28):** every invariant reads only the format's own decoded, authoritative
  structures (`Index.db` entries, `Summary.db` entries, the BTI trie's resolved leaves,
  `CompressionInfo.db`'s chunk table, a real row/header decode) — never a byte-pattern scan over
  `Data.db`, mirroring #4194's D6/L3 "no header hunting" guard and its mechanized
  `test_*_no_resync_scan.sh` gate check.
- **Uncompressed-write claim boundary (#1406):** not implicated — this change is entirely read-only
  (`verify` writes nothing); no interaction with the write surface.
- **Cassandra 5.0 only:** every fixture and every check operates on `na`/`nb` BIG and `da` BTI only,
  consistent with `BigVersionGates`/`BtiVersionGates`.
- **Public surfaces:** `cqlite-core`'s `verify` module gains `VerifyMode::Audit`, `AuditSummary`,
  `AuditRow`, `AuditVerdict`, and five new `VerifyErrorClass` variants (additive — no existing variant
  removed or renumbered). New logic lives in a new sibling module (`verify_audit`, or a small
  directory under it if it exceeds the campsite target), declared next to the existing `pub mod
  verify;`, matching #4194's precedent for keeping `verify.rs` itself (already 2761 lines, well over
  the ~800-line target) from growing further than the field + call-site wiring requires. CLI gains
  one new `VerifyModeArg::Audit` value and one new `--deep` flag on the existing `Verify` subcommand;
  no new subcommand. Python/Node untouched.
- **<128 MB:** invariants 1–7 hold at most one `Index.db`/`Summary.db`/`Filter.db`/
  `CompressionInfo.db`/BTI-index in memory at a time (all already-bounded reads the existing checks
  already perform) plus per-header reads for invariant 1, never a whole `Data.db`. Invariant 8
  (`--deep` only) performs one full decode pass — the SAME bound `VerifyMode::Full`'s existing row
  scan already has today; it adds no new unbounded structure.
- **Gate:** the new test target (`cqlite-core/tests/issue_4195_component_audit.rs`) must be named in
  the gate's `core-tests` component list (#3522); confirm during tasks whether it is auto-covered by
  the existing glob (matching #4194's own confirmed auto-coverage) or needs an explicit addition.
  Corruption `.db` binaries are gitignored; corpus tests skip-clean when absent, FAIL when
  present-but-wrong, hard-required under `CQLITE_REQUIRE_FIXTURES=1` (#1094 doctrine).
