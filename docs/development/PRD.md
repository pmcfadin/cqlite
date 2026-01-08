# CQLite – Concise Product Requirements Document (v0.2)

## 1 · Mission & Vision

* **Mission**  Lower friction to Cassandra data by providing a fast, safe Rust library for local SSTable operations—fully aligned with Apache community values.
* **Vision**  Become the de‑facto community standard for reading Cassandra 5+ SSTables, usable from CLI, Python, Node.js, and WASM.

---

## 2 · Functional Scope (Must‑Have for v1.0)

| Area                    | Requirements                                                                                                                                                                                                                                     |     |                                                                    |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --- | ------------------------------------------------------------------ |
| **Core Reading**        | • 100 % Cassandra 5 SSTable format support (data, TOC, index, stats)<br>• All CQL types incl. collections & UDTs<br>• Compression: LZ4, Snappy, Deflate<br>• Zero‑copy deserialization into **provided schema** (schema passed in, not inferred) |     |                                                                    |
| **CLI (`cqlite`)**      | • **One‑shot mode**: `--schema`, `--data-dir`, optional `--query` & \`--out {json                                                                                                                                                                | csv | parquet}\`<br>• **REPL mode**: interactive attach / query / export |
| **Output Formats**      | JSON, CSV, Parquet (pluggable writers)                                                                                                                                                                                                           |     |                                                                    |
| **Language Bindings**   | Typed APIs for Python (async), Node.js (TS defs), WASM (IndexedDB)                                                                                                                                                                               |     |                                                                    |
| ~~Writing (Post-MVP)~~  | ~~Generate Cassandra 5 SSTables~~ REMOVED - CQLite is read-only                                                                                                                                                                                                                     |     |                                                                    |
| **Performance Targets** | Set after functional parity; goal: *faster than native Cassandra bulk tools*                                                                                                                                                                     |     |                                                                    |

---

## 3 · Architecture & Separation of Concerns

```
cqlite-core/        # Pure Rust crate
├── sstable_rw/     # read/write, compression, checksums
├── schema/         # type system, validation
└── query/          # minimal SQL parser & executor (optional)

cli/                # REPL + one-shot wrapper (uses core)
bindings/
  ├── python/
  ├── nodejs/
  └── wasm/
tests/              # shared fixtures (Cassandra 5 SSTables)
```

* **Rule**: Core never depends on CLI or bindings.
* **Async‑first**, **type‑safe**, zero‑copy IO.

---

## 4 · Milestones

| #      | Deliverable                | Key Exit Criteria                                                                         |
| ------ | -------------------------- | ----------------------------------------------------------------------------------------- |
| **M1** | **Core Reading Library**   | Reads any Cassandra 5 SSTable; all CQL/UDT types; compression OK; tiered coverage (see Section 5.1) |
| **M2** | **CLI (REPL + one‑shot)**  | Human can query & verify data from disk; basic `SELECT … WHERE …`                         |
| **M3** | **Output Writers**         | JSON, CSV, Parquet export work end‑to‑end via CLI                                         |
| **M4** | **Language Bindings**      | `pip install cqlite`, `npm i cqlite`; CI wheels & native modules                          |
| **M5** | ~~Write Support~~          | ~~Generates valid Cassandra 5 SSTables~~ REMOVED (Issues #175, #176)                                                   |
| **M6** | **Perf & Size Validation** | Benchmarks > native bulk tools; WASM < 2 MB; publish v1.0 release                         |

> **Revision Note (Dec 2025)**: M1 coverage target revised from flat 95% to tiered targets (90%/80%/70%/50%) based on module criticality per Issue #204. M5 (Write Support) permanently removed - CQLite is a read-only library (Issues #175, #176, #23, #12).


---

## 5 · Testing Strategy

| Layer       | Tests                                              | Tooling                            |
| ----------- | -------------------------------------------------- | ---------------------------------- |
| Core        | Unit + property‑based for type/format edge cases   | Rust `cargo test`, `proptest`      |
| CLI         | Integration & snapshot tests for commands/output   | `assert_cmd`, `insta`              |
| Bindings    | Language‑specific unit + FFI smoke tests           | `pytest`, `jest`, web‑worker tests |
| Integration | End‑to‑end: read → export → read‑back              | GitHub Actions matrix              |
| CI/CD       | PR lint, fmt, unit, integration; codecov gate 75 % | GitHub Actions                     |

### 5.1 · Tiered Coverage Targets

Coverage targets are tiered by module criticality rather than flat percentages:

| Tier | Line Coverage | Branch Coverage | Modules |
|------|---------------|-----------------|---------|
| **Critical** | 90%+ | 80%+ | `parser/`, `storage/sstable/reader/`, `storage/sstable/reader/parsing/` |
| **Important** | 80%+ | 70%+ | `query/`, `schema/`, `types/`, `cql/`, `discovery/` |
| **Supporting** | 70%+ | 60%+ | `memory/`, `platform/`, `storage/sstable/directory/`, `storage/sstable/bti/` |
| **Utilities** | 50%+ | 40%+ | `benchmarks/`, `testing/` |

**Aggregate Target**: 75% overall (weighted by module size), enforced via codecov gate.

**Rationale**: Tiered coverage focuses testing effort on critical code paths (parser, storage) where bugs cause data corruption, while allowing pragmatic coverage for utilities and platform abstractions.

---

## 6 · Release & Distribution

* **CI**: GitHub Actions builds artifacts per tag.
* **Packages**:

  * Cargo crate (`cqlite-core`)
  * PyPI wheels (`cqlite`) for macOS/Linux/Win
  * npm (`cqlite`) pre‑builds + WASM bundle
  * Homebrew & Linuxbrew taps for CLI
* **Versioning**: SemVer; v0.x during feature development, v1.0 at M6 completion.

---

## 7 · Community & Governance (Snapshot)

* Apache 2.0 license from day 1; CLA + DCO required.
* Public GitHub project board, weekly community call.
* Donation path: engage Cassandra PMC by M4, IP clearance by M6.

---

## 8 · Risks & Mitigations (Top 3)

| Risk                   | Impact                       | Mitigation                                            |
| ---------------------- | ---------------------------- | ----------------------------------------------------- |
| Cassandra format churn | Read/write breakage          | Modular format adapters + test corpus per release     |
| WASM memory limits     | Feature gaps in browser env. | IndexedDB chunked IO + streaming deserialization      |
| External PR quality    | Project instability          | Strict CI gates, contributor guide, mandatory reviews |

---

## 9 · Acceptance / “Definition of Done”

1. **Functional parity** – read SSTables for all Cassandra 5+ formats, CLI & bindings pass all tests.
2. **Performance** – demonstrably faster bulk reads than Cassandra native tools.
3. **Coverage quality** – tiered targets met (Critical: 90%+, Important: 80%+, Supporting: 70%+).
4. **Size** – WASM bundle ≤ 2 MB compressed.
5. **Community** – ≥ 10 active contributors, docs & governance ready for ASF.
6. **Release** – v1.0 tagged, packages in Cargo, PyPI, npm; announcement blog post.
