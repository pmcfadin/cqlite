# Research: OQ1 — BUILD vs ADOPT for iceberg-rust write support

**Change:** `add-iceberg-materializer` (epic child 1) · **Open question:** OQ1
**Tier:** verification (primary-source confirmation of Phase-1 Haiku fan-out)
**Date:** 2026-07-03 · **Pins as of this date.**

> Every load-bearing Phase-1 claim below was re-checked against a primary source
> (crates.io API, docs.rs API pages, the actual `apache/iceberg-rust` source tree
> on the `v0.9.1` tag **and** `main`, merged/closed PR pages). Blog posts were used
> only for release-note dates and to resolve the RisingWave mechanism, never as the
> sole authority for an API claim.

---

## 1. Verified findings ledger

Verdict key: **CONFIRMED** (primary source agrees) · **CORRECTED** (Haiku wrong, real
value given) · **ENRICHED** (Haiku right but incomplete) · **PARTIAL** (confirmed in
part, one sub-claim unverified) · **UNVERIFIED** (could not confirm from primary source).

### A. The critical claim — delete-file COMMIT path

| # | Claim (Phase-1) | Verdict | Primary source | Pin |
|---|---|---|---|---|
| A1 | The released transaction/commit API cannot commit delete files into a snapshot | **CONFIRMED + ENRICHED** | `v0.9.1` `crates/iceberg/src/transaction/` contains only `action.rs, append.rs, mod.rs, snapshot.rs, sort_order.rs, update_location.rs, update_properties.rs, update_statistics.rs, upgrade_format_version.rs` — **no `row_delta.rs`, `overwrite.rs`, `rewrite_files.rs`**. `main` (fetched 2026-07-03) adds only `expire_snapshots.rs` + `update_schema.rs` — **still no delete-commit action**. docs.rs `iceberg/0.9.1/…/transaction` public surface = `Transaction`, `ActionCommit`, `ApplyTransactionAction` + `fast_append()`/append only. | iceberg 0.9.1; main @2026-07-03 |
| A2 | PR #1987 "add delete file support to SnapshotProducer" closed unmerged 2026-04-08 (stale) | **CONFIRMED** | github.com/apache/iceberg-rust/pull/1987 — Closed, not merged, auto-stale 2026-04-08; reviewer CTTY flagged overlap with #1606 + a planned DML epic | — |
| A3 | (Haiku missed this) A second, more complete delete-commit attempt existed | **NEW (ENRICHES A1)** | PR **#1882** "feat: Add deletion support with RowDeltaAction" (EmilLindfors, opened 2025-11-22) — adds `transaction/row_delta.rs`, position + equality + deletion-vector commit, 18 unit + 5 e2e tests. **Closed stale 2026-03-06** after maintainer liurenjie1024: *"This pr is too large to review, please split them first."* Never split/relanded. | — |

**Net A-finding:** As of **2026-07-03**, neither the released crate (0.9.1) nor `main`
exposes any public API to commit equality/position **delete files** into an Iceberg
snapshot. Two separate attempts (#1882, #1987) both died as stale. The materializer
spec **requires** row/range/partition tombstones → delete files, so a pure ADOPT of the
transaction layer is **not possible today**.

### B. Writer + building-block availability (the good news)

| # | Claim | Verdict | Primary source | Pin |
|---|---|---|---|---|
| B1 | `EqualityDeleteWriter` shipped in 0.9.1 | **CONFIRMED** | docs.rs `iceberg/0.9.1/…/writer/base_writer/equality_delete_writer` → `EqualityDeleteFileWriter`, `EqualityDeleteFileWriterBuilder`, `EqualityDeleteWriterConfig` | 0.9.1 |
| B2 | `PositionDeleteFileWriter` shipped | **CONFIRMED** | docs.rs writer module + `DataContentType` enum ("data, equality deletes, or position deletes") | 0.9.1 |
| B3 | Manifest/metadata building blocks are public | **CONFIRMED (key enabler)** | docs.rs `iceberg/0.9.1/iceberg/spec/` → `ManifestWriter`, `ManifestListWriter`, `Manifest`, `ManifestEntry`, `DataFile`, `DataFileBuilder`, `DataContentType` (data/eq-delete/pos-delete), `Snapshot`, `TableMetadata`, `FormatVersion` all **public** | 0.9.1 |
| B4 | Delete-file *reads* (both eq + pos on one FileScanTask) landed 0.8.0 | **CONFIRMED** | 0.8.0 release blog (iceberg.apache.org) — reads only; orthogonal to the commit gap | 0.8.0 |

**Net B-finding:** everything needed to *build* a delete-aware commit exists as public
API — writers emit the delete files, `spec::ManifestWriter`/`ManifestListWriter` +
`DataContentType::EqualityDeletes` encode the manifests, `TableMetadata`/`Snapshot`
model the metadata. **Only the high-level action that atomically assembles a
delete-referencing snapshot and swaps metadata is missing** — i.e. exactly PR #1882/#1987's
scope. This is what makes HYBRID cheap (see §4).

### C. Versions & dates

| # | Claim | Verdict | Source | Correction |
|---|---|---|---|---|
| C1 | iceberg 0.9.1 latest, ~2026-05-06 | **CONFIRMED** | crates.io API `iceberg` → `0.9.1` `created_at 2026-05-06T18:54Z` | — |
| C2 | 0.9.0 = 2026-03-10 | **CORRECTED** | crates.io → `0.9.0` published **2026-03-19** | Haiku off by ~9 days |
| C3 | 0.8.0 = 2026-02-02 | **CORRECTED** | crates.io → `0.8.0` published **2026-01-19** | Haiku off by ~2 weeks |
| C4 | iceberg 0.9.1 depends on arrow/parquet **57.1** | **CONFIRMED** | `v0.9.1` root `Cargo.toml` `[workspace.dependencies]`: `arrow-array/schema/select/string = "57.1"`, `parquet = "57.1"` | — |

### D. Catalogs

| # | Claim | Verdict | Source | Notes |
|---|---|---|---|---|
| D1 | No apache filesystem/Hadoop catalog exists | **CONFIRMED (via crate inventory)** | apache ships `iceberg-catalog-{rest,hms,glue,s3tables,memory,sql}` only; no `-filesystem`/`-hadoop`. Consistent with `StaticTable` read-only + upstream Hadoop-catalog-deprecation direction | The specific "maintainers rejected it in Discussion #1246" I could **not** open directly → that sub-claim is **UNVERIFIED** but consistent with all evidence |
| D2 | `StaticTable` is read-only (cannot commit) | **CONFIRMED** | Corroborated across apache docs + Hadoop-catalog dev@ thread ("add StaticTable … read only") | — |
| D3 | apache `iceberg-catalog-sql` exists & is commit-capable for embedded/SQLite | **PARTIAL** | crates.io `iceberg-catalog-sql` **0.9.1**, repo `apache/iceberg-rust`, publisher liurenjie1024, desc "Apache Iceberg Rust Sql Catalog". **Exists and is apache-official** (Haiku was over-skeptical → CORRECTED). `update_table`/commit *body* not read from source → completeness **PARTIAL**. Moot for deletes: even a fully-committing catalog can't help while the **transaction layer has no delete action** (A1) | 0.9.1 |
| D4 | FileIO supports local `file://` via opendal | **CONFIRMED** | apache + JanKaul both build FileIO on opendal; `file://` supported | — |

### E. Ecosystem

| # | Claim | Verdict | Source | Notes |
|---|---|---|---|---|
| E1 | icelake archived, folded into apache/iceberg-rust | **CONFIRMED** | RisingWave "replace icelake with iceberg-rust" tracking (#17548) + release notes | — |
| E2 | JanKaul `iceberg-rust` crate v0.10.0, updated 2026-03, unofficial single-maintainer | **CONFIRMED** | crates.io `iceberg-rust` → **0.10.0**, published **2026-03-26**, sole publisher Jan Kaul, ~8,100 LOC / 32 files, 32,429 downloads, Apache-2.0 | — |
| E3 | JanKaul crate has a filesystem + sql catalog story | **CONFIRMED** | Ecosystem crates `iceberg-file-catalog` (true filesystem catalog) + `iceberg-sql-catalog` (both JanKaul) + `iceberg-glue-catalog` | Directly satisfies design D5 "filesystem first" — see H3 |
| E4 | JanKaul crate can **write AND commit** equality deletes (incl. DataFusion MERGE) | **UNVERIFIED** | Repo feature table shows "Equality deletes ✅" but that is the **read** column; DataFusion `INSERT INTO` confirmed, **MERGE/DELETE write path not confirmed from primary source**. Needs a 1-day spike before it can change the verdict | — |
| E5 | RisingWave writes equality deletes from Rust in production | **CONFIRMED — resolves the Lane1↔Lane3 contradiction** | RisingWave rewrote its connector in Rust and **contributed the writer building blocks to apache/iceberg-rust** (#17548), but **commits via its own custom logstore** ("idempotent commits … all commits are replayable and verifiable", RisingWave eq-delete blog), i.e. its **own commit coordinator**, NOT a released `tx.row_delta()` (which doesn't exist). Production Rust eq-delete writers roll their own commit glue — exactly the HYBRID shape | — |
| E6 | PyIceberg cannot write equality deletes (COW only); Java has mature eq-delete writers | **UNVERIFIED (low risk)** | Consistent with ecosystem; not independently re-confirmed. Not load-bearing for OQ1 | — |
| E7 | Iceberg v3: position deletes → deletion vectors; equality deletes still valid for CDC | **UNVERIFIED (low risk)** | Directionally corroborated (PR #1882 carried Puffin/deletion-vector support). Note: our materializer targets **v2**; a future v3 move is a follow-up, not a blocker | — |

### F. CQLite local pins (spot-check)

| # | Claim | Verdict | Source | Notes |
|---|---|---|---|---|
| F1 | CQLite pins `arrow = "53"`, `parquet = "53"` | **CONFIRMED** | `Cargo.toml:149-150` → `arrow = { version = "53" }`, `parquet = { version = "53", features=["arrow","snap","zstd"] }`; `Cargo.lock` → `arrow 53.4.1`, `parquet 53.4.1` | — |
| F2 | (arrow-major coexistence is theoretical) | **ENRICHED** | `Cargo.lock` **already contains `arrow 54.2.1` alongside `arrow 53.4.1`** — a transitive dep already pulls a second arrow major. Two arrow majors in one binary is **already a fact of this tree**, not a hypothetical | — |
| F3 | `export/mod.rs` documents Iceberg committing as out-of-scope | **CONFIRMED** | `cqlite-core/src/export/mod.rs:29-36` "External-committer boundary … Committing those files to lakehouse table formats (Iceberg, Delta) … is deliberately out of scope for this crate." **Must be updated in child 1** (this change reverses that boundary) | — |

---

## 2. ADOPT-path inventory (apache/iceberg-rust 0.9.1)

**Works today (zero net-new):**
- Iceberg schema + `TableMetadata` v2 modeling (`spec::` public).
- Parquet **data-file** writing via `DataFileWriter` / DataFusion `IcebergTableWriter`.
- **Equality-delete file** writing (`EqualityDeleteFileWriter`, B1) and position-delete
  file writing (B2) — the delete *files* themselves.
- Manifest / manifest-list encoding (`ManifestWriter`, `ManifestListWriter`, B3) incl.
  `DataContentType::EqualityDeletes`.
- Append-only commits (`Transaction::fast_append().add_data_files(...)`).
- FileIO on local `file://` (D4).
- Persistent, commit-capable **SQL catalog** (`iceberg-catalog-sql`, SQLite-backable, D3).

**Missing for our use (hard blockers for the spec's tombstone requirements):**
1. **Delete-file commit** — no `row_delta`/`overwrite` action anywhere in released or
   `main` transaction API (A1–A3). This is the whole point of the materializer; without
   it upserts-only would commit but every tombstone scenario in `spec.md`
   (row/range/partition-tombstone, equal-ts delete-vs-live) **cannot be satisfied by ADOPT**.
2. **Filesystem/Hadoop catalog** — absent (D1). Closest apache option is the SQL catalog
   (a **design change to D5**, see §5 SD-catalog).

**Cost to fill each gap on the ADOPT path:**
- Gap 1 (delete commit): revive PR #1882/#1987 upstream — **not in our control**; both
  died on review-bandwidth, no committed timeline. Depending on "upstream lands it" is an
  unbounded schedule risk. → pushes us to HYBRID (§4).
- Gap 2 (catalog): pick `iceberg-catalog-sql` (small wiring) or JanKaul `iceberg-file-catalog`.

**arrow-57-vs-53 implications:** iceberg 0.9.1 = arrow/parquet **57.1** (C4); CQLite =
arrow/parquet **53** (F1). Arrow majors are additive crates and **already coexist** in
our lock (53 + 54, F2), so *compiling* both is fine. But **types do not interop**: a
`RecordBatch` from arrow-53 (our `DeltaParquetWriter`/`arrow_convert` output) **cannot** be
handed to iceberg-rust's arrow-57 `DataFileWriter`. Options:
- **(a) Upgrade the whole workspace to arrow 57** — clean single-version, but ripples
  through `arrow-flight` (53→57, the Flight/Trino connector), the parquet writer,
  `arrow_convert`, and the `duckdb` dev-dep. **L/XL, separate change.**
- **(b) Feature-isolated arrow-57 in the `iceberg` module only** — build arrow-57
  `RecordBatch`es directly from CQLite `Value`s inside `export/iceberg/`, never sharing an
  arrow-53 batch across the boundary. Duplicates a thin slice of `arrow_convert` at
  arrow-57; two arrow majors in the feature-on build (already precedented). **M, contained
  by the `iceberg` feature — recommended.**
- **(c) Arrow-IPC bridge** (arrow-53 batch → IPC bytes → arrow-57 batch) — works, but
  per-batch serialize/deserialize cost and awkward. Not recommended.
- **(d) BUILD path** sidesteps arrow-57 entirely (reuse our arrow-53 parquet writer, emit
  metadata ourselves) — see §3.

### 2b. JanKaul sub-option (ADOPT-unofficial) + dependency-risk assessment
JanKaul `iceberg-rust` 0.10.0 (E2) advertises a **fuller** write + catalog story:
`iceberg-file-catalog` (true filesystem catalog — matches D5 as written), `iceberg-sql-catalog`,
`datafusion_iceberg` with `INSERT`, and an "Equality deletes ✅" row. **If** a spike (E4)
confirms it can **write + commit** equality deletes (MERGE/DELETE-FROM), it could deliver
both gaps in one dependency. **Dependency risk (material):** single maintainer, unofficial,
~8.1k LOC, 32k downloads (vs apache official, 37-contributor cadence, ~97k downloads),
arrow version unconfirmed, no ASF governance/security process. For a CQLite feature-gated
optional path this is **acceptable only as a spike-gated fallback**, not the primary
dependency. Recommendation: do not stake the epic on it; keep as H3 (§4).

---

## 3. BUILD-path inventory (emit Iceberg v2 ourselves)

We already own: an arrow-53 **Parquet writer** (`export/delta_parquet.rs`), column
stats/bounds, and CQL→Arrow schema mapping (`delta_schema`, `arrow_convert`, Epic #673).
BUILD means writing the **metadata/manifest layer** ourselves. Concrete deliverables, with
the governing Iceberg **v2 spec** sections:

1. **Table metadata JSON** (`vN.metadata.json`): `format-version:2`, schema (Iceberg
   schema from CQL/arrow map), partition-spec (unpartitioned v1), sort-order,
   `current-snapshot-id`, `snapshots`, `snapshot-log`, `metadata-log`,
   `last-sequence-number`, properties. — spec §*Table Metadata*.
2. **Manifest files (Avro)**: one `ManifestEntry` per data/delete file — `status`
   (added/existing/deleted), `sequence_number`, `DataFile{ content: 0 data | 1 pos-del |
   2 eq-del, file_path, file_format=PARQUET, record_count, file_size_in_bytes,
   column_sizes/value_counts/null_counts/lower_bounds/upper_bounds, equality_ids (eq-del),
   partition }`. — spec §*Manifests*.
3. **Manifest list (Avro)**: `manifest_file` entries (path, added/existing/deleted
   file+row counts, `sequence_number`, `min/max_sequence_number`, partition summaries). —
   spec §*Manifest Lists*.
4. **Snapshot object**: `snapshot-id`, `parent-snapshot-id`, `sequence-number`,
   `timestamp-ms`, `manifest-list` path, `summary{ operation: append|overwrite|delete,
   added/deleted-* counts, our `cqlite.generations` / `cqlite.delta-horizon-micros` /
   `cqlite.lineage` }`, `schema-id`. — spec §*Snapshots*.
5. **Sequence-number assignment & delete-scope semantics** (the subtle part): monotonic
   per commit; an **equality-delete file applies to data files with sequence number <
   its own** (Iceberg row-delta inheritance). Getting this wrong silently deletes too much
   or too little — must be tested against a real Iceberg reader. — spec §*Sequence Numbers*
   / *Scan Planning / delete file applicability*.
6. **Atomic version swap**: write `v(N+1).metadata.json`, then atomically advance the
   pointer — filesystem: `version-hint.text` + rename; or a catalog `update_table` CAS —
   with optimistic-concurrency retry. — spec §*File System Tables* / *Catalog*.
7. **Avro encoding** of (2)+(3) against the **exact** Iceberg manifest Avro schemas
   (`apache-avro` crate). This is the bulk of net-new BUILD code — and precisely what
   iceberg-rust's public `spec::ManifestWriter` already does correctly, which is why pure
   BUILD is dominated by HYBRID (§4).

**BUILD correctness risk:** hand-rolling manifest Avro + sequence-number inheritance +
metadata JSON is high-fidelity, easy-to-get-subtly-wrong spec work; our only oracle would
be "a real Iceberg engine reads it back correctly." Higher risk than borrowing upstream's
already-tested encoders.

---

## 4. HYBRID options

**H1 — adopt writers/spec/FileIO/catalog, build only the delete-aware commit layer
(RECOMMENDED).**
Use iceberg-rust 0.9.1 for: schema/`TableMetadata`/`Snapshot` types, `DataFileWriter` +
`EqualityDeleteFileWriter` (the delete *files*), `spec::ManifestWriter`/`ManifestListWriter`
+ `DataContentType::EqualityDeletes` (manifest encoding — the correctness-critical part we
do **not** want to hand-roll), FileIO, and `iceberg-catalog-sql`. **Build only** the
~PR#1882/#1987-shaped piece: assemble a snapshot that references the delete manifests,
populate summary props, and commit via the catalog's `update_table` CAS. This is exactly
what RisingWave does with its own coordinator (E5) and what both closed PRs implemented —
we implement it once, against the **public** building blocks (B3), on our side of the
boundary. Net-new ≈ one `commit.rs` (delete-aware snapshot producer) + tests. When upstream
finally lands `row_delta`, we delete our layer and call theirs. **Smallest net-new surface,
rides upstream for encoding, no fork to maintain.**

**H2 — adopt + vendored patch reviving PR #1882 (RowDeltaAction).**
Carry #1882 as a git patch/fork. Risk: #1882 was closed *specifically* for being "too
large," so it'll need splitting/rebasing against a fast-moving `main`; a vendored fork of a
0.x crate is ongoing maintenance cost. Inferior to H1 (which needs no fork — it only uses
public API).

**H3 — adopt JanKaul iceberg-rust (spike-gated fallback).**
If the E4 spike confirms write+commit of equality deletes via `datafusion_iceberg`, JanKaul
delivers both gaps + a real filesystem catalog in one dep. Gated by the dependency-risk
assessment in §2b — **fallback only**, not primary.

---

## 5. Draft verdict

**Recommendation: HYBRID H1** — adopt apache/iceberg-rust 0.9.1 for types, writers, FileIO,
manifest encoding, and catalog; **build the delete-aware snapshot+commit layer ourselves**
against the public `spec::` building blocks. Pure ADOPT is **impossible today** for the
tombstone requirements (A1–A3, no delete-commit action in released *or* `main`, both
attempts dead-stale). Pure BUILD needlessly re-implements correctness-critical Avro/manifest
encoding that iceberg-rust already exposes publicly (B3). H1 satisfies every `spec.md`
tombstone scenario, keeps net-new code to ~one commit module + parity harness, and cleanly
hands back to upstream once `row_delta` lands.

**Effort tiers:**

| Path | Tier | Note |
|---|---|---|
| Pure ADOPT (transaction layer) | **BLOCKED** | delete-commit action absent; append-only ADOPT would be S/M but fails the spec's tombstone requirements |
| **HYBRID H1** (recommended) | **M–L** | S wiring (adopt writers/spec/catalog) + M delete-aware commit layer (~PR#1987 scope, ~300–600 LOC + tests) + M arrow-57 bridge (option b) + M reference-merge parity harness (spec-required on any path) |
| HYBRID H2 (vendored #1882 fork) | **L** | + ongoing fork-maintenance tax |
| HYBRID H3 (JanKaul) | **M if spike passes** | + single-maintainer dependency risk |
| Pure BUILD (own metadata+manifests) | **L–XL** | + hand-rolled Avro/manifest/sequence-number encoders + own correctness oracle (highest spec-compliance risk) |

### Named sub-decisions for the owner

- **SD-arrow (arrow-upgrade question).** Do **not** upgrade the workspace to arrow 57 for
  this change. Use **option (b)**: feature-isolated arrow-57 inside `export/iceberg/`,
  building arrow-57 batches directly from CQLite values (no arrow-53↔57 batch sharing).
  Two arrow majors already coexist in the lock (F2). A full workspace arrow 53→57 upgrade
  (option a) is a legitimate but **separate** L/XL change (ripples `arrow-flight`, parquet,
  duckdb dev-dep) — flag, don't bundle.
- **SD-catalog (D5 change).** Design D5 says "filesystem/Hadoop catalog first," but **apache
  has no filesystem catalog** (D1). Pick one, and update D5 accordingly:
  1. **apache `iceberg-catalog-sql`** (SQLite-backed, embedded, persistent, commit-capable,
     ASF-official) — recommended: closest to "local, no external service" while staying on
     the official crate. **This is a real change to D5's wording** ("filesystem" → "embedded
     SQL/SQLite catalog").
  2. JanKaul `iceberg-file-catalog` — a *true* filesystem catalog matching D5 verbatim, but
     ties us to the unofficial ecosystem (§2b risk).
  3. BUILD path emits `version-hint.text` filesystem layout ourselves — fully filesystem,
     no catalog dep, but we own atomic-swap/concurrency.
  Recommendation: **SD-catalog → option 1** (apache SQL catalog), keep option 3 as a
  no-dependency fallback.

---

## 6. Freshness notes for the epic filing (Lane 5 + this pass)

- Draft issue refs verified: **#1728 CLOSED** 2026-07-02 (PR #1732), **#1729 CLOSED**
  2026-07-02 (PR #1730), **#1388 CLOSED** 2026-07-02. All 16 draft issue references remain
  valid.
- **`epic-draft.md`** line 46 `"(fail-closed until landed)"` re: #1729/#1728 is now **stale**
  — those blockers **landed** 2026-07-02. Update the dependency line to past-tense/resolved.
- **`proposal.md` "Dependencies → Hard"** (#1729/#1728) likewise now satisfied; reword from
  "blocks" to "satisfied by (landed 2026-07-02)."
- **`cqlite-core/src/export/mod.rs:29-36`** carries an explicit *"External-committer
  boundary … committing to Iceberg/Delta … deliberately out of scope"* comment (F3). This
  epic **reverses that boundary** — child 1 must update/replace that doc comment (and its
  `docs/architecture/cassandra-sidecar-parquet-projections.md` reference) so the code
  boundary and the new feature don't contradict each other.
- Drafts are silent on **tail exports**; recommend an explicit out-of-scope line in
  `proposal.md` (per Lane 5).
- **OQ1 now answered** (this doc): verdict **HYBRID H1**; feeds two owner decisions —
  **SD-arrow** (feature-isolated arrow-57, no workspace upgrade) and **SD-catalog** (change
  D5 from "filesystem" to apache SQL/SQLite catalog). Both belong on the epic's "NEEDS YOU"
  list before child 1 activates.

---

*All API/source claims verified against `apache/iceberg-rust` `v0.9.1` tag + `main` tree,
docs.rs 0.9.1, and crates.io on 2026-07-03. Version pins: iceberg 0.9.1 (arrow/parquet
57.1); CQLite arrow/parquet 53.4.1; JanKaul iceberg-rust 0.10.0.*
