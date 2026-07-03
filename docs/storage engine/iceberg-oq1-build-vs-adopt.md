# OQ1 Final Memo: BUILD vs ADOPT for iceberg-rust write support

**Change:** `add-iceberg-materializer` (epic child 1) · **Answers:** OQ1
(`design.md:77-78`, `epic-draft.md:47`) · **Date:** 2026-07-03 · all pins as of this date.
**Evidence base:** the verified synthesis
`docs/storage engine/cassandra-index/research-iceberg-oq1.md` (every claim below carries
its primary-source citation through; no new research).

## 1. Verdict

**HYBRID (H1).** Adopt `apache/iceberg-rust` **0.9.1** (crates.io, published
2026-05-06, `research-iceberg-oq1.md:55`) for Iceberg types, the data/delete-file
writers, FileIO, manifest/manifest-list encoding, and the SQL catalog; **build only the
delete-aware snapshot+commit layer ourselves** (~the scope of upstream PRs
[#1987](https://github.com/apache/iceberg-rust/pull/1987) and
[#1882](https://github.com/apache/iceberg-rust/pull/1882)). The one-line why: as of
2026-07-03 **neither the released crate (0.9.1) nor `main` exposes any transaction
action that can commit equality/position delete files into a snapshot** — the 0.9.1
`transaction/` module has append-only actions and `main` adds only
`expire_snapshots`/`update_schema`; both upstream delete-commit attempts (#1882, #1987)
closed stale unmerged (`research-iceberg-oq1.md:25-33`). Our spec's tombstone
requirements (`spec.md:5-43`) make delete commits non-negotiable, so pure ADOPT is
blocked; pure BUILD would hand-roll the correctness-critical Avro manifest encoding that
iceberg-rust already exposes publicly (`research-iceberg-oq1.md:41-49,180-188`). H1 is
the smallest net-new surface (~one `commit.rs` + tests) and deletes cleanly once
upstream lands `row_delta` (`research-iceberg-oq1.md:194-206`).

## 2. Evidence table — iceberg-rust 0.9.1 today vs what we build

All rows pinned to iceberg 0.9.1 (tag `v0.9.1`) + `main` fetched 2026-07-03; sources are
the synthesis ledger rows cited per line.

| Capability | iceberg-rust today (0.9.1) | What we'd build | Source |
|---|---|---|---|
| Data-file append/commit | **Works**: `Transaction::fast_append().add_data_files(...)` + `DataFileWriter` | Nothing | `research-iceberg-oq1.md:94-100`; docs.rs `iceberg/0.9.1` transaction |
| Equality-delete file write | **Works**: `EqualityDeleteFileWriter{,Builder}`, `EqualityDeleteWriterConfig` | Nothing (file emission) | `research-iceberg-oq1.md:39` (B1); docs.rs `iceberg/0.9.1/.../base_writer/equality_delete_writer` |
| Position-delete file write | **Works**: position-delete writer + `DataContentType` covers pos-deletes | Nothing (file emission) | `research-iceberg-oq1.md:40` (B2) |
| Delete-file snapshot **COMMIT** | **Missing** — no `row_delta`/`overwrite`/`rewrite_files` action in 0.9.1 or `main`; PRs #1882 (RowDeltaAction, closed stale 2026-03-06 "too large to review") and #1987 (closed stale 2026-04-08) both dead | **We build this** — delete-aware snapshot producer + catalog `update_table` CAS commit, against public `spec::` blocks; ~300–600 LOC + tests | `research-iceberg-oq1.md:25-33,196-206`; github.com/apache/iceberg-rust/pull/1882, /pull/1987 |
| Manifest / manifest-list encoding | **Works and public**: `spec::ManifestWriter`, `ManifestListWriter`, `DataFile{Builder}`, `DataContentType::EqualityDeletes`, `Snapshot`, `TableMetadata` | Nothing — this is exactly what we adopt instead of hand-rolling Avro | `research-iceberg-oq1.md:41-49` (B3) |
| Filesystem catalog | **Does not exist** — apache ships `iceberg-catalog-{rest,hms,glue,s3tables,memory,sql}` only; `StaticTable` is read-only | Either nothing (use SQL catalog) or a self-emitted `version-hint.text` filesystem layout — SD-catalog decision, §6 | `research-iceberg-oq1.md:64-65` (D1, D2) |
| Embedded catalog alternative | **Exists**: `iceberg-catalog-sql` 0.9.1, ASF-official (publisher liurenjie1024), SQLite-backable, commit-capable (commit body PARTIALly verified) | Small wiring only | `research-iceberg-oq1.md:66` (D3); crates.io `iceberg-catalog-sql` |
| FileIO local `file://` | **Works** (opendal-backed) | Nothing | `research-iceberg-oq1.md:67` (D4) |
| Custom snapshot properties (`cqlite.generations`, `cqlite.delta-horizon-micros`, `cqlite.lineage`) | Snapshot summary is public in `spec::`; the *append* action sets properties, but our delete-bearing commits go through **our** commit layer | We populate summary props inside our built `commit.rs` — under our control by construction | `research-iceberg-oq1.md:41,168-171,199-201`; `design.md:35-48` |
| Arrow compatibility | iceberg 0.9.1 pins **arrow/parquet 57.1** (`v0.9.1` root Cargo.toml); CQLite pins **arrow/parquet 53** (`Cargo.toml:149-150`, lock at 53.4.1). Majors already coexist in our lock (53 + 54) but **types don't interop** across majors | Feature-isolated arrow-57 batch construction inside `export/iceberg/` (option b) — SD-arrow decision, §6 | `research-iceberg-oq1.md:58,85-86,118-134` (C4, F1, F2) |

## 3. What changes in the drafted design/spec (verdict ≠ D5's assumption)

- **D5 (Catalog: filesystem first) — must change.** `design.md:59-65` and
  `proposal.md:32` assume an "iceberg-rust filesystem catalog"; **apache ships none**
  (`research-iceberg-oq1.md:64`). D5 becomes either (1) apache `iceberg-catalog-sql`
  (SQLite-backed, embedded, ASF-official — recommended) or (2) self-emitted
  `version-hint.text` filesystem metadata (no catalog dep, we own atomic swap). D5's
  wording "filesystem/Hadoop-style catalog only" (`proposal.md:32`) must be rewritten to
  the chosen option; see SD-catalog in §6 (`research-iceberg-oq1.md:250-261`).
- **D3 (Commit protocol / exactly-once) — ownership flips from delegated to built.**
  `design.md:35-48` implicitly assumed iceberg-rust performs the commit and we decorate
  it. Under H1 **the delete-aware snapshot assembly and the catalog `update_table` CAS
  are OUR code**: idempotency (`cqlite.generations` set-membership), snapshot-property
  stamping, and crash-safety-via-atomic-swap are mechanics we implement in the built
  commit layer, not behavior we inherit (`research-iceberg-oq1.md:196-206`). The D3
  *requirements* stand unchanged; the *mechanism* paragraph needs rewording to name our
  commit layer as the implementer.
- **Spec "Feature-flag claim boundary" (`spec.md:146-163`) — dependency list gets
  concrete.** The `iceberg` feature now isolates three named deps: `iceberg = "0.9.1"`,
  `iceberg-catalog-sql = "0.9.1"` (if SD-catalog → option 1), and an **arrow-57 stack
  used only inside `export/iceberg/`** (`research-iceberg-oq1.md:126-130`). The
  requirement's default-build scenarios are unaffected; the impact note in
  `proposal.md:40-42` should enumerate these pins.
- **D6 module tree (`design.md:67-73`) — `commit.rs` is re-scoped.** In
  `export/iceberg/{mod.rs, fold.rs, deletes.rs, commit.rs, lineage.rs, schema.rs}`,
  `commit.rs` is no longer thin glue over a library transaction: it **is the built
  delete-aware commit layer** (snapshot producer + manifest-list assembly via adopted
  `spec::` writers + CAS commit + retry). Budget it as the M-sized named task (§5).
- **`cqlite-core/src/export/mod.rs:29-36` boundary comment — must be reversed in
  child 1.** It currently declares "Committing those files to lakehouse table formats
  (Iceberg, Delta) … deliberately out of scope for this crate"
  (`research-iceberg-oq1.md:87,274-278`). This change reverses that boundary; child 1
  must update the comment (and its
  `docs/architecture/cassandra-sidecar-parquet-projections.md` reference) so code and
  feature don't contradict.

## 4. Updated dependency status

- **#1728 / #1729 (authoritative Statistics.db): CLOSED 2026-07-02** via PR #1732
  (#1728) and PR #1730 (#1729) (`research-iceberg-oq1.md:267-269`). `epic-draft.md:44-45`
  ("blocks child 1's watermark requirement (fail-closed until landed)") and
  `proposal.md:53-55` (Hard dependency, "fails closed without them") are **stale** —
  reword both to "satisfied by (landed 2026-07-02)". The spec's fail-closed placeholder
  requirement (`spec.md:86-107`) **stays** as defense-in-depth against pre-#1729 writer
  output in the wild; it is no longer a schedule blocker.
- **Tail-export orthogonality.** The memtable-plugin design
  (`docs/storage engine/memtable-plugin-design.md`) has CQLite exporting real `nb`
  SSTables into a tail directory; that is orthogonal to this epic. The drafts are silent
  on it (`research-iceberg-oq1.md:279-280`) — add one explicit out-of-scope line to
  `proposal.md`: *the materializer consumes only real flushed/compacted SSTable
  generations (via delta envelopes, D1); tail-export generations are out of scope for
  child 1.*

## 5. Effort tiers + recommended child-1 scoping

Tiers from `research-iceberg-oq1.md:232-240`:

| Path | Tier | Note |
|---|---|---|
| ADOPT-pure (released transaction layer) | **BLOCKED** | No delete-commit action in 0.9.1 or `main` (A1–A3); append-only would be S/M but fails every tombstone scenario in `spec.md:21-43` |
| **HYBRID H1 (recommended)** | **M–L** | S adopt-wiring (writers/spec/catalog) + M built commit layer (~PR#1987 scope, ~300–600 LOC + tests) + M arrow-57 bridge (option b) + M reference-merge parity harness (spec-required on any path, `spec.md:109-122`) |
| JanKaul sub-option (H3) | **M if spike passes** | JanKaul `iceberg-rust` 0.10.0 (2026-03-26, sole maintainer, ~8.1k LOC, 32k downloads vs apache's 37-contributor ~97k) has a true filesystem catalog + "Equality deletes ✅" — but that checkmark is the **read** column; **MERGE/DELETE write+commit is UNVERIFIED** (E4) and would need a 1-day spike. Dependency risk material; fallback only (`research-iceberg-oq1.md:74-76,136-145,214-217`) |
| BUILD-pure (own metadata/manifests) | **L–XL** | Hand-rolled Avro manifests + sequence-number inheritance + metadata JSON; only oracle is "a real Iceberg engine reads it back" — highest spec-compliance risk (`research-iceberg-oq1.md:149-188`) |

(H2 — vendored #1882 fork — is L with an ongoing fork-maintenance tax and is dominated
by H1, which uses only public API; `research-iceberg-oq1.md:208-212`.)

**Child-1 scoping.** Everything in the drafted change stays in
`add-iceberg-materializer`; two scope edits:

1. **Spike task 1.1 is dead as drafted.** OQ1 was "spike task inside child 1"
   (`epic-draft.md:47`, `design.md:77-78`); this memo answers it. Delete the
   build-vs-adopt spike. The optional 1-day **JanKaul MERGE-write spike** (E4) survives
   only as a de-prioritized fallback probe — do not gate child 1 on it.
2. **The commit layer becomes a named task**, replacing the spike: *"build the
   delete-aware snapshot+commit layer (`export/iceberg/commit.rs`): snapshot producer
   referencing eq/pos-delete manifests via `spec::ManifestWriter`/`ManifestListWriter`,
   `cqlite.*` summary properties, catalog `update_table` CAS with optimistic-retry;
   delete-scope/sequence-number semantics tested against a real Iceberg reader."*
   Add a tracking note: when upstream lands `row_delta` (revival of #1882/#1987 scope),
   delete our layer and call theirs (`research-iceberg-oq1.md:204-206`).

Remaining child-1 tasks (fold engine per D2, schema mapping per Epic #673, lineage per
D4, CLI `materialize` subcommand, DuckDB reference-merge parity harness) are unchanged
by the verdict.

## 6. NEEDS-DECISION (genuine product calls only)

- **SD-arrow** — arrow-major strategy for the iceberg feature. Options: (a) workspace
  upgrade 53→57 (clean but L/XL; ripples `arrow-flight`/Trino connector, parquet writer,
  `arrow_convert`, duckdb dev-dep); (b) feature-isolated arrow-57 **inside
  `export/iceberg/` only**, building arrow-57 batches directly from CQLite `Value`s —
  two arrow majors already coexist in our lock (53.4.1 + 54.2.1,
  `research-iceberg-oq1.md:86`). **Lead recommendation: (b)**; flag (a) as a separate
  change, don't bundle (`research-iceberg-oq1.md:244-249`).
- **SD-catalog** — what D5 becomes, since apache ships no filesystem catalog. Options:
  (1) apache `iceberg-catalog-sql` (SQLite-backed, embedded, ASF-official,
  commit-capable); (2) JanKaul `iceberg-file-catalog` (matches D5 verbatim, unofficial
  single-maintainer ecosystem); (3) self-emitted `version-hint.text` filesystem
  metadata (no catalog dep, we own atomic-swap/concurrency). **Lead recommendation:
  option 1**, keep option 3 as the no-dependency fallback
  (`research-iceberg-oq1.md:250-261`).
- **OQ2** (still open, `design.md:79-82`, `epic-draft.md:60-61`) — tables whose
  clustering columns include types Iceberg disallows as identifier fields: degrade to
  position deletes for those tables, or fail closed with a named error? No lead
  recommendation recorded.
- **OQ3** (still open, `design.md:83-84`, `epic-draft.md:62`) — static-column
  materialization shape: denormalized per-row values (simple, redundant) vs companion
  table (normalized, two commits). No lead recommendation recorded.

---

*Pins: iceberg 0.9.1 (arrow/parquet 57.1) + `main` @2026-07-03; iceberg-catalog-sql
0.9.1; JanKaul iceberg-rust 0.10.0 (2026-03-26); CQLite arrow/parquet 53.4.1
(`Cargo.toml:149-150`). All upstream claims verified in
`research-iceberg-oq1.md` against the `apache/iceberg-rust` `v0.9.1` tag, `main`,
docs.rs 0.9.1, and crates.io on 2026-07-03.*
