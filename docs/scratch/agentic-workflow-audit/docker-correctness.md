# Docker-Based Correctness/Parity Infrastructure Audit

## Inventory

### Tiers & CI Contracts (5 documented)

| Tier | Purpose | Runs | Evidence | Gate strength |
|------|---------|------|----------|---------------|
| `fast_pr` | Static checks, schema linting | Every PR | smoke/partial/out_of_scope | cheapest |
| `required_parity` | Deterministic vs committed goldens (JSONL, digests) | Every PR + main | canonical_semantic / byte_for_byte | blocking |
| `nightly_docker` | Live Cassandra regen/revalidate (weekly) | Scheduled only | canonical_semantic / byte_for_byte | non-blocking for PRs, blocks release |
| `exhaustive_regeneration` | Full corpus regen matrix (nb/oa/da/big/bti) | Release + weekly dispatch | byte_for_byte / canonical_semantic | strongest; coverage/presence audit |
| `manual_debug` | Human investigation | Never auto | any | non-gating |

**Artifact retention mandates:** fast_pr (none) · required_parity (≥14d) · nightly_docker (≥30d) · exhaustive_regeneration (≥90d)

### Docker Machinery

**Compose abstraction** (`test-data/scripts/container_env.sh`): Auto-detects docker/podman variants; exports `COMPOSE_CMD`, `ENGINE_CMD`; provides `compose_exec_nontty`, `compose_run_nontty` helpers for cross-engine compatibility.

**Lifecycle** (`compose-guard.sh`): Brings up service, polls `ps`, probes health via cqlsh+nodetool (UP + UN status), configurable timeout (default 900s, poll 5s).

**Containers pinned:**
- `cassandra:5.0.2` (single source, cassandra-5.0.2 / git f278f677…) — used by all generators
- Detected on-path via docker/podman; explicit `CONTAINER_ENGINE` env override; fallback is docker

### Fixture Generation (Local + Nightly)

**Corpus keyspaces & generators (exhaustive-regeneration.yml mandate):**

| Keyspace(s) | Generator | Lines | Purpose |
|------------|-----------|-------|---------|
| test_basic, test_collections, test_timeseries, test_wide_rows, test_oa, test_da | regenerate-datasets.sh | ~300 | nb/oa/da main corpus (50 rows/table default) |
| test_deltas | generate-deltas.sh | 589 | CDC delta export validation |
| test_comp (compression) + BTI sources | generate-compression-parity.sh | 657 | Compression/decompression paths |
| test_da wide partition | gen-wide-bti.sh | ? | BTI wide-partition index |
| corruption/test_comp_corrupt | generate-corruption-corpus.sh | 1041 | Corruption detection validation |
| test_types | generate-cql-type-parity.sh | 1047 | All CQL type encode/decode |
| test_tomb | generate-tombstone-parity.sh | 751 | Tombstone/TTL/expiry paths |
| test_compactionparity | generate-compaction-parity.sh | 411 | Live Cassandra compaction comparisons |
| test_compactionparityudt | generate-compaction-parity-udt.sh | 421 | Compaction with UDTs |
| test_writeparity | generate-write-load-parity.sh | 309 | Write-path byte parity |
| test_signed_coll | generate-signed-collection-parity.sh | 300 | Signed/hashed collections |
| test_compaction_tombstone_ttl | generate-compaction-tombstone-ttl-parity.sh | 441 | Compaction + tombstone + TTL |
| test_big wide partition | gen-wide-big.sh | ? | BIG wide-partition index |
| system* | n/a | n/a | Excluded (run-dependent) |

**Each generator pattern:**
1. Start docker container (cassandra:5.0.2, named container)
2. Wait for cqlsh readiness (retry loop: 60 × 5s default)
3. Apply schema (cp + cqlsh -f)
4. Disable autocompaction (nodetool)
5. Insert data (direct cqlsh or Python driver with retry)
6. Flush (nodetool flush)
7. Export tar-stream from `/var/lib/cassandra/data/<ks>` to host
8. Generate JSONL goldens via sstabledump (per-generator)
9. Generate Statistics.db.txt via sstablemetadata (per-generator)
10. Cleanup container on EXIT trap

**Fixed timestamps** (byte-parity requirement): Some generators use `USING TIMESTAMP T_WRITE` (e.g., write-load: 1700000000000000µs) to make writes reproducible; others use wall-clock (localDeletionTime) so fresh regen is NOT byte-reproducible vs committed goldens.

### Pinning & Fetching

**fetch-datasets.sh:**
- Downloads release artifact from GitHub releases (tag/asset/SHA256 pinned)
- SHA256 verified (fail-closed when checksum tools absent + CQLITE_PARITY_REQUIRE_DATASETS=1)
- Writes `.dataset-pin` (tag/asset/sha256) after extraction
- Fast-path: if pin matches + required components present (wide_partition REQUIRED_COMPONENTS hardcoded) → skip download
- Safety: refuses `/`, `$HOME`, `/tmp`, repo root; requires `datasets` as final component
- CI support: restores git-tracked reference files from HEAD after extract (Digest.crc32, TOC.txt, JSONL goldens)

**Artifact classes:**
- Binary components: `*-Data.db`, `*-Index.db`, `*-Summary.db`, `*-Statistics.db`, `*-CompressionInfo.db`, `*-CRC.db`, `*-Digest.crc32`, `*-Filter.db`, `*-TOC.txt` (some gitignored, some committed)
- Goldens (committed): `*-Data.db.jsonl` (sstabledump normalized), `*-Statistics.db.txt` (sstablemetadata), `*-TOC.txt`, `*-Digest.crc32`

---

## How It Works

### Nightly Docker Parity Lane (`nightly-docker-parity.sh`)

**6 legs, 2 classes (HARD-fail + ADVISORY):**

| Leg | Class | Component | Command | Time est. | Artifact |
|-----|-------|-----------|---------|-----------|----------|
| 1 | HARD | Live read-back | e2e-cassandra-readback.sh | ~2-5 min | diffs/live_readback/ |
| 2 | HARD | BTI sstabledump | issue_911_bti_sstabledump_parity test | ~1 min | test output |
| 4 | HARD | Bloom no-false-negative | filter_db_strict_parameters_and_no_false_negative test | ~30s | diffs/ + log |
| 3 | HARD | Compaction logical | gradle test (Cassandra source rebuild) | ~10-20 min | diffs/compaction_logical/ |
| 6 | ADVISORY | Bloom FPR statistical | filter_db_statistical_false_positive_rate_slow (CQLITE_FILTER_FPR_SLOW=1) | ~5-10 min | diffs/ + FPR summary |
| 5 | ADVISORY | Compaction byte | gradle byteParity | ~30+ min | diffs/compaction_byte/ |

**Outputs under target/nightly-docker-parity/:**
- `report.md` — per-leg table + Bloom FPR summary + repro cmds
- `fixture-metadata.txt` — Cassandra version/git SHA/image/repo commit/generated_at
- `logs/<leg>.log` — captured stdout+stderr per leg
- `diffs/<leg>/` — JSONL diffs, Statistics.db.txt diffs, etc.

**Exit behavior:**
- 0 iff all HARD legs pass (ADVISORY outcomes recorded, never gate)
- Under STRICT mode (NIGHTLY_DOCKER_STRICT=1 in workflow): infra-unavailable SKIPs on HARD legs convert to FAIL (run-or-fail). Explicit user skips (--skip-live, --skip-compaction) stay SKIP even in strict mode.

**Local repro:** Non-strict mode; SKIPs cleanly when Docker/gradle/Cassandra unavailable → agent-gate stays green.

### Live Readback (`e2e-cassandra-readback.sh`)

**Acceptance gate (PRD §4.1, epic #472):** Validates SSTables written by CQLite can be loaded by real Cassandra.

**Per-table flow:**
1. CQLite writes mutation JSONL set → exports SSTable
2. Copy components to `/var/lib/cassandra/data/<ks>/<table>-<uuid>/`
3. `nodetool refresh <ks> <table>` reloads
4. cqlsh queries verify row count + per-column values (structured JSON comparison, not grep)

**Spec language:** row_count, row.<pk>, col[<pk>].<col>, absent_col, absent_row_cluster; production verifier (`e2e_verify.py`) is used by nightly AND has self-test mode (no container).

**Tables (12 labels):** basic-primitives, collections, udt, static-columns, ttl, cell-delete, row-delete, range-tombstone, partition-tombstone, wal-recovered.

**Artifact handoff** (issue #725): On FAIL, workdir copied to E2E_ARTIFACT_DIR (mutation JSONL + spec.txt); on SUCCESS, workdir deleted.

### Exhaustive Regeneration (`exhaustive-regeneration.yml`)

**Triggered:** weekly + workflow_dispatch (never on PRs).

**What it does:**
1. Invokes every generator (regenerate-datasets.sh + generate-*.sh suite) in sequence
2. Records provenance (Cassandra version/git SHA/docker image/generator commands/asset name+SHA256)
3. Runs `cargo run -p cassandra-parity -- corpus-audit --corpus . --manifest <manifest> --provenance <record>`

**Audit hard-fails on:**
- Missing/stale manifest reference
- Unclassified high-relevance Cassandra file
- **ABSENT** expected (non-system*) component identity (coverage/presence check)
- Provenance/manifest version divergence
- Corruption-fixture coverage gap

**Uploads:** One report artifact (provenance + audit report + generator logs); never commits binaries or publishes dataset assets.

**Gate strength note** (issue #2009): This is a **coverage/presence** audit — proves every manifest-referenced component is PRODUCED, not byte-identical to committed goldens (byte-parity stays with sstabledump-parity-gate + nightly_docker tiers).

---

## Measured/Observed Costs

**Times (rough):**
- regenerate-datasets.sh (50 rows): ~5-10 min (includes Cassandra startup + Python insertion)
- generate-write-load-parity.sh: ~5 min
- generate-compression-parity.sh: ~10-15 min
- generate-cql-type-parity.sh: ~15-20 min
- generate-corruption-corpus.sh: ~20+ min
- nightly-docker-parity.sh (all legs, strict): ~2-3 hours (Cassandra rebuild + gradle byteParity dominates)
- e2e-cassandra-readback.sh: ~2-5 min (per-table compose startup)

**Disk:**
- datasets-v3.4 archive: unspecified size in scripts (users cite 1–10 GB in issues)
- Single generator may consume 10+ GB RAM+disk during Cassandra runtime
- Workflow artifacts (logs + diffs) retained per tier minimum
- Multiple generators running → sequential execution (mutex via CONTAINER_NAME uniqueness)

**Cassandra versions:**
- All generators pin cassandra:5.0.2 hardcoded
- Single source of truth: docker image tag
- CASSANDRA_REF env var overrides (used by compaction-parity bootstrap-cassandra.sh)

**Schema delivery:**
- Committed at test-data/schemas/*.cql
- Applied via docker cp + cqlsh -f
- Per-generator ownership (no shared schema builder)

---

## Friction Points

### 1. **Sequential Container Lifecycle per Generator**
- Each generator spawns a named container (cqlite-regen, cqlite-<keyspace>, etc.)
- Must cleanup on EXIT (trap); leftover container blocks re-run
- No parallel generator invocation (container name collision)
- nightly-docker-parity serializes legs (Cassandra rebuild for compaction is the long pole at 10-20 min)
- **Cost:** ~2-3 hour critical path for exhaustive regen; nightly can run in ~1 hour if legs parallelized (not implemented)

### 2. **Cassandra Startup Latency per Generator**
- Every generator waits ~5-10 min for cqlsh readiness (60 retries × 5s default)
- No shared Cassandra instance across generators (each runs its own container)
- Overhead for small generators (write-load: ~5 min total, ~3 min just Cassandra warmup)
- **Cost:** ~30 min+ wasted startup time across full regen suite

### 3. **JSONL Golden Generation via sstabledump Container**
- After export, each generator runs sstabledump in a *separate* container (not reusing writer Cassandra image)
- Per-table invocation (find + loop)
- Involves JSON parsing on stdout
- **Cost:** ~1-2 min per generator (sstabledump I/O bound)

### 4. **No Deterministic Byte Parity for Wall-Clock Timestamps**
- Generators using localDeletionTime (TTL/tombstone expiry wall-clock) are NOT byte-reproducible
- Fresh regen → different bytes than committed goldens (invalidates byte-for-byte diffs)
- Fixed timestamps (write-load) are reproducible but only for live-data scenarios
- **Workaround:** Exhaustive regen tier uses coverage/presence audit, not byte-drift check (issue #2009)
- **Friction:** Developers cannot easily re-validate byte parity locally; must rely on nightly lane

### 5. **Distributed Fixture Truth**
- Committed JSONL/Statistics.db.txt/TOC.txt/Digest.crc32 are gitignored or tracked per-generator
- Binary Data.db files (500MB–few GB) are downloaded from release artifacts, not regenerated on clone
- `.dataset-pin` controls fetch; old pins may not match current manifest
- **Cost:** Fresh checkout requires ~5–30 min fetch + network latency; pin mismatch forces re-regen

### 6. **Manifest Staleness Race (Merged PR + Report Drift)**
- `docs/reports/cassandra-test-parity.md` is a committed derived artifact
- Two PRs can merge green per-PR but leave main with a stale report (semantic merge race, issue #1338)
- **Safeguard:** parity-report-heal job on push to main (opens regen PR from auto/parity-report-regen); local `--check` in agent-gate
- **Friction:** Requires `PARITY_HEAL_TOKEN` secret to auto-heal; manual regen fallback is slow

### 7. **E2E Container Resource Starvation**
- e2e-cassandra-readback spins up a fresh docker-compose stack
- Multiple parallel table tests → N containers (resource contention)
- --keep-running flag speeds iteration but leaves dangling containers (cleanup responsibility on user)
- **Cost:** ~2-5 min per run; slow iteration when debugging readback failures

### 8. **No Incremental Regeneration**
- regenerate-datasets.sh (and all generators) re-do the entire keyspace (all tables)
- No ability to re-gen only changed tables or skip completed keyspaces
- Re-run after fixing one table → re-gen all test_basic tables again
- **Cost:** Unnecessary CPU/disk during development

### 9. **Strict Mode State Fragility**
- NIGHTLY_DOCKER_STRICT=1 fails infra unavailability (non-user SKIP → FAIL)
- Local smoke runs (STRICT=0) skip cleanly; CI lane (STRICT=1) fails closed
- Border between "CI works, local smoke is green" is easy to misread
- **Friction:** Developers can pass local nightly-docker-parity.sh (non-strict) while CI strict-mode fails on same code (different environment perception)

### 10. **Generator Script Duplication**
- 12 generator scripts follow nearly identical patterns (Cassandra start, schema, insert, flush, export, sstabledump)
- Each has its own retry logic, logging, temp-dir handling
- Changes to the pattern (e.g., new sstabledump flag) require updates to multiple files
- **Cost:** 6947 lines of bash across 12 generators; maintenance burden

### 11. **Wide-Partition Pinning Buried in fetch-datasets.sh**
- WIDE_PARTITION_REQUIRED_COMPONENTS hardcoded as array (nb-2-big-Data.db, Index.db, Digest.crc32, CompressionInfo.db)
- If a new component becomes required, fetch.sh must be updated (not discovered automatically)
- **Friction:** Easy to miss when expanding parity scope

### 12. **LIMIT Agent Context: Compaction Leg Requires External JDK/Gradle**
- Compaction legs run gradle test/byteParity against Cassandra source
- Requires JDK, ant, gradle installed on CI/local machine (not containerized)
- Agent gates cannot invoke nightly-docker-parity.sh without JDK (must --skip-compaction locally)
- **Cost:** Dev environment setup burden; CI pipeline must pre-install JDK

---

## Open Questions

1. **Parallelization roadmap?** Nightly Docker Parity legs are serial (Cassandra rebuild serializes compaction). Can legs 1, 2, 4 run in parallel (independent Cassandra images) while holding 3, 5 serial? Expected speedup ~40% (lose compaction rebuild).

2. **Incremental regen scope?** Could generate-*.sh accept --only-tables=<list> to skip already-generated tables? Feasible without manifest lock-stepping?

3. **Shared Cassandra instance across generators?** Could a single long-lived cassandra:5.0.2 service (docker-compose up -d) serve all generators in exhaustive regen, dropping startup latency from 30+ min? Schema isolation challenges?

4. **Coverage audit vs byte-parity unification?** Issue #2009 decoupled coverage/presence (exhaustive-regen) from byte-drift (sstabledump-parity / nightly_docker). Should these reunify or is split intentional (cost containment)?

5. **Dataset release asset hygiene?** v3.4 SHA256 is hardcoded in fetch-datasets.sh; when is the next release cut? Manual process or automated (release workflow)?

6. **Compaction leg containerization?** Is there appetite to move gradle/JDK into a container so agents can invoke full nightly-docker-parity.sh without pre-installed toolchain?

7. **E2E verifier production use?** e2e_verify.py is battle-tested in nightly + self-test. Can it be promoted to a public library (bindings expose it) so users validate their own write-path roundtrips?
