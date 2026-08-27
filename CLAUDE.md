# CLAUDE.md

Guidance for Claude Code when working with CQLite. This file is loaded into every agent context —
it holds the **rules and pointers**; recipes and examples live in `docs/development/dev-cookbook.md`.

## Project Overview

CQLite is a Rust library for local Apache Cassandra SSTable access — it reads (and writes)
Cassandra 5.0 data files without cluster dependencies.

**Status**: v0.14.x (Jul 2026). M1–M5 complete (core reading, CLI, output writers, Python +
Node.js bindings, write support + STCS compaction); v0.12 delivered byte-for-byte compaction parity
vs Apache Cassandra, Arrow Flight + Trino connector, canonical BTI (`da`) write/read, CDC-style
delta-export; v0.13 added read-path speedups, byte-bounded result budgets, and no-heuristics fixes.
**0.15 is in progress** — the cqlite-trino latency/throughput/operations theme (epic #2403). Headline
shipped since 0.14: lazy Summary-guided BIG index (O(summary) open, bounded point intervals,
summary-guided scans — #2412), Flight admission control (`--max-concurrent-scans`, #2420),
connector snapshot reuse per (keyspace,table) (#2356, connector 0.14.3), row-granular streaming for
point-read/warm/full-scan merges (#2423/#2230), and a GitHub-enforced merge gate (#2433). Next: M6
(WASM bindings), M7 (perf validation + v1.0).

## Documentation

- **SSTable format (single source of truth)**: `docs/sstables-definitive-guide/README.md` —
  Ch.5 Data.db, Ch.6 Index.db/Summary.db, Ch.17 BTI, App.B encoding cheat sheet, App.F known limitations
- **Agent doctrine (canonical site)**: https://pmcfadin.github.io/cqlite/agents-developing/ —
  [gate contract](https://pmcfadin.github.io/cqlite/agents-developing/gate-contract/),
  [no-heuristics](https://pmcfadin.github.io/cqlite/agents-developing/no-heuristics/),
  [test data](https://pmcfadin.github.io/cqlite/agents-developing/test-data/),
  [source map](https://pmcfadin.github.io/cqlite/agents-developing/source-map/),
  [validation playbook](https://pmcfadin.github.io/cqlite/agents-developing/validation-playbook/),
  [format debugging](https://pmcfadin.github.io/cqlite/agents-developing/format-debugging/),
  [spec-driven audit](https://pmcfadin.github.io/cqlite/agents-developing/spec-driven-audit/),
  [delivery pipeline](https://pmcfadin.github.io/cqlite/agents-developing/delivery-pipeline/),
  [roborev findings](https://pmcfadin.github.io/cqlite/agents-developing/roborev-findings/)
- **Gate deep mechanics** (sccache tuning, concurrency caps, disk hygiene, `--delta` internals):
  `docs/development/gate-ops.md`
- **CI toolchain policy** (issue #1990): `docs/development/ci-toolchain-policy.md` — workflows honor
  `rust-toolchain.toml`; one advisory `future-rust-canary.yml` lane tracks latest stable; coverage
  tools install prebuilt.
- **Parity CI tiers**: `docs/development/parity-ci-tiers.md` (tier contracts; the
  `exhaustive_regeneration` tier = weekly `exhaustive-regeneration.yml`, #1026) +
  `docs/development/parity-release-checklist.md` (gates public parity claims).
  `docs/reports/cassandra-test-parity.md` is a **committed derived artifact** of
  `test-data/cassandra-parity-manifest.yml` (#1338): the SKIP-aware `parity-report` gate component
  catches local staleness; the `parity-report-heal` job in `cassandra-parity.yml` self-heals
  merge races via a regen PR (needs `PARITY_HEAL_TOKEN`; SKIPs with a notice if absent).
- **Command cookbook** (CLI usage/modes, bindings build/test/examples, write support, delta-export,
  profiling, feature-flag builds, fuzz runs): `docs/development/dev-cookbook.md`
- Historical investigations: `docs/archive/issues/INDEX.md`; pass rates: `test-data/validation-matrix.md`

## Available Skills (auto-invoked)

Skills in `.claude/skills/` activate automatically when relevant:

| Skill | Use Case |
|-------|----------|
| `sstable-parsing` | Binary format parsing, hex dumps, compression |
| `cql-type-system` | CQL type deserialization |
| `rust-patterns` | Zero-copy, async I/O, memory efficiency |
| `rust-skills` | General idiomatic Rust (265 rules); invoke with `/rust-skills` |
| `ci-cd-validation` | Tiered gate loop (lite iterate, full once), CI monitoring, merge-on-green |
| `test-data-management` | Test SSTable generation, validation |

**Delivery pipeline skills**: `flow-groom` → `flow-activate` → `flow-implement` → `flow-address` →
`flow-finalize`, plus `flow-board` (claim board + next thing) and **`/drive-issue <N>`** (drive ONE
named issue to merged: worker persona + `github-coord-worker` comms + a self-rearming cron that
re-checks the issue for lead answers while blocked). See
`docs/development/pm-operating-loop.md`. (`start-epic`/`pm-status` are deprecated pointers → flow-*.)

## Available Subagents

Subagents in `.claude/agents/` — **always pass an explicit accessible `model` on spawn** (the pinned
frontmatter model may be inaccessible):

| Agent | Purpose |
|-------|---------|
| `flow-lead` | Delivery lead/PM — drives the flow-* pipeline, sequences the specialists |
| `flow-closer` | Per-issue endgame owner — ONE full gate → C → final roborev → merge-on-green → finalize, in its own disposable context (#2084) |
| `sstable-developer` | SSTable implementation, format debugging |
| `rust-reviewer` | Read-only Rust code review, quality enforcement |
| `test-validator` | Test execution, sstabledump parity, failure triage |
| `spec-auditor` | Intent audit (C) — impl vs OpenSpec/issue acceptance criteria |
| `coverage-reviewer` | Test-quality review (meaningful, not just present) |
| `compaction-parity-auditor` | Write/compaction byte-parity gap audit vs Cassandra |

## The Agent Gate — the only run that counts (issue #719)

`scripts/agent-gate.sh` is THE pre-PR gate. Its `==== AGENT-GATE SUMMARY ====` block is the verdict;
ad-hoc cargo runs never count. `scripts/agent-gate.sh --list` shows the component set.

| Mode | Command | Use |
|------|---------|-----|
| **Full** — the gate of record | `scripts/agent-gate.sh` | ONCE per issue, immediately pre-merge, inside `flow-closer`. fmt, clippy `-D warnings`, core/integration/write/CLI tests, `oom-audit` (SKIP-aware structural no-unbounded-materialization audit, #2012), `pub-surface` (cqlite-core public-API snapshot guard, #1712), minimal-features build, smoke. Emits `AGENT-GATE SUMMARY`. |
| **Lite** (#1821, ~1–5 min) | `scripts/agent-gate.sh --lite` | EVERY fix round. file-size + fmt + scoped clippy + blast-radius tests (touched package `--lib` + diff's new `--test` targets, mapped from `git diff origin/main...HEAD`; defaults to `cqlite-core --lib` when no rust package is in the diff). Emits a DISTINCT `AGENT-GATE LITE SUMMARY` (MODE: lite) — can NEVER be pasted as the full SUMMARY. |
| **Delta** (#1892) | `scripts/agent-gate.sh --delta <anchor-sha> --anchor-run-id <id>` (or `--anchor-summary-file <path>`) | Re-certify a post-full-PASS polish round whose diff is ONLY executable tests/docs (rust test code, python/node binding tests against an already-built module, `scripts/tests/*.sh`, `*.md`; #2081). FAILs CLOSED on anything else (src, scripts, workflows, `Cargo.*`, config, test-data, unbuilt node module) — never builds, never passes vacuously. Emits a DISTINCT `AGENT-GATE DELTA SUMMARY` naming the anchor + a `delta-executors:` line; record BOTH it AND the anchor's full SUMMARY in the PR. NOT the gate of record. |

**Required invocation — summary-file redirect, never raw stdout (issues #1175/#2079), full AND lite:**

```bash
AGENT_GATE_SUMMARY_FILE=/tmp/gate-summary.txt bash scripts/agent-gate.sh > gate.log 2>&1 < /dev/null
cat /tmp/gate-summary.txt   # the SUMMARY block is the ONLY gate text an agent retains; NEVER read gate.log
```

- Prefer `run_in_background` (or a long timeout) so a subagent never idle-waits and gets
  watchdog-killed (#1855). A queued gate ≠ hung gate: under load it prints `waiting for gate slot`.
- **Completion probe = `grep -qE 'RESULT: (PASS|FAIL)'` — `INCOMPLETE` is a liveness placeholder, NOT
  a verdict (#3041; mechanism follow-up #2908).** The gate writes
  `RESULT: INCOMPLETE (gate did not finish)` into the summary file **at launch** (EXIT-trap sentinel,
  before the #1825 slot is even granted) and only overwrites it with `PASS`/`FAIL` at the terminal
  emit. So a bare `grep -q` on the bare `RESULT:` token fires the instant the gate starts and would let an agent accept
  a **just-launched or still-queued** gate as its gate of record — a verdict that does not exist.
  Anchor every poll (agents, skills, docs, helper scripts) on `PASS|FAIL`; a sentinel-only summary
  means "still running, died, or queued", never certified.
- **A GENUINELY PROSE diff cannot change the compiled binary — so a test failure in its full gate
  is BY DEFINITION pre-existing on `main` or a flake, and the correct response is CITE-AND-WAIVE
  (#3042).** The waiver's precondition is that the diff touches no compiled input (no `src`, no
  `Cargo.*`, no build script, no workflow, no test-data). **That qualifier is a path-shape test, and a
  path shape is not evidence — DON'T JUDGE IT, RUN IT (#3250):**
  ```bash
  git diff --name-only origin/main...HEAD | bash scripts/ci/classify-docs-only.sh   # exit 0 = prose
  ```
  A non-zero exit means the waiver does not apply, full stop. **The falsifying case is not
  hypothetical**: this repository ships measurement harnesses under `docs/reports/*-artifacts/` **by
  convention**, so a #3222-shaped diff contains `src/main.rs` **and** `Cargo.toml` under
  `docs/reports/ws0-3026-artifacts/ws0-cqlite/scan-harness/` — it satisfies "no `src`, no `Cargo.*`"
  **textually** while being false **materially**, and an agent correctly following the old wording
  waives a red that is genuinely theirs. (Read "docs-only" here the same way roborev doctrine does — a
  **code-free census**, never a `docs/` path prefix; the classifier above is the gate-side spelling of
  the same idea, and `scripts/tests/test_classify_docs_only.sh` pins it.) **NEVER patch source to turn such a
  gate green** — that is a real change smuggled in under a docs diff, certified by nothing, and it
  masks the actual main-red. Instead: (1) confirm the diff really is non-compiling-input, with the
  classifier above rather than by eye; (2) identify the failure as a known main-red issue or a
  known flake — reproduce it on a clean `origin/main` checkout if it is not already filed, and FILE it
  if it is not; (3) record the waiver in the PR body naming the failing component and the issue number
  it belongs to. A waiver with no cited issue is not a waiver — it is an unexplained red. Conversely,
  if ANY compiled input is in the diff the waiver is void: the failure is presumed yours until proven
  otherwise.
- Defaults if `AGENT_GATE_SUMMARY_FILE` unset (per-checkout; give concurrent gates in ONE checkout
  unique paths): `.agent-gate-summary.txt` / `.agent-gate-lite-summary.txt` / `.agent-gate-delta-summary.txt`.
  **Nested exception (#2874):** a gate started with `AGENT_GATE_PARENT_RUN_ID` in its env (i.e. spawned
  by an enclosing gate) and no explicit `AGENT_GATE_SUMMARY_FILE` defaults to its OWN
  `$LOG_DIR/summary-primary.txt` (never the checkout default) and stamps `nested-under: <parent-run-id>`, so a
  nested/self-test sub-gate can never clobber the parent's summary. A mid-run summary clobber (foreign
  run-id) is caught at the next component boundary — and at the terminal emit — with a named
  `summary-integrity: FAIL` line + `RESULT: FAIL`, never a bare INCOMPLETE. **No-clobber + reader
  contract (#2874):** when the contended path is found holding a FOREIGN `run-id` (a live peer owns
  it) the gate does NOT rewrite that path; it publishes its own FAIL verdict to a non-clobbering
  sibling `<summary-file>.integrity-fail.<run-id>` + the `logs:` bundle (+ stdout/stderr) and exits
  non-zero, deliberately leaving the peer's block on the pinned path. A reader therefore MUST treat
  the process EXIT CODE as primary and MUST verify the `run-id:` line matches the run it launched
  before trusting a pinned-path block — a mismatched/foreign `run-id` block (even `RESULT: PASS`) is a
  peer's, not yours; on a mismatch, read the `.integrity-fail.<run-id>` sibling / `logs:` bundle instead.
- clippy is scoped per-package (#1844): whole workspace `-D warnings` but skips the source-built
  DuckDB amalgamation (cqlite-cli `duckdb-tests`) + OTel stack (`observability`/
  `observability-testing`); parquet/arrow stay linted. `CQLITE_CLIPPY_FULL=1` (nightly `gate.yml`)
  runs the full matrix.
- The FULL gate FAILs CLOSED on **either half** of the fixture contract; `--lite`/`--only` stay
  lenient for both.
  - Fetched corpus absent (#2078): `missing-fixtures: FAIL-CLOSED (#2078)`, remedy
    `bash test-data/scripts/fetch-datasets.sh`; `AGENT_GATE_ALLOW_MISSING_FIXTURES=1` opts out
    visibly (`missing-fixtures: OPT-OUT (...)`).
  - Committed CQL schemas unreachable (#3148): `missing-schemas: FAIL-CLOSED (#3148)` — textually
    distinct from #2078's marker, with two causes, an unreadable `test-data/schemas/*.cql` or a
    **rejected relative `CQLITE_SCHEMAS_ROOT`**, each carrying its own remedy line. Success stamps a
    positive `schemas: N/N canonical .cql readable under <root> (<source>)` line, so a pasted SUMMARY
    shows the check RAN. **There is deliberately NO opt-out env var, and none may be added**:
    committed source in a checkout is never legitimately absent, so an escape hatch could only buy a
    vacuous green.
- **A run whose worktree mutates MID-RUN cannot certify (#2926).** Every mode captures a tree
  identity at start, re-verifies it at each component boundary + the terminal emit, and FAILs closed
  with `tree-integrity: FAIL (tree-mutated-midrun; head <a>→<b>; changed: …)`. Every SUMMARY carries
  `tree-start:`/`tree-end:`/`tree-integrity:`, so **closers verify `tree-integrity:` alongside
  `RESULT:`** — a mutated-mid-run block is not a certification and cannot be pasted as one. The
  block's `commit:`/`dirty:` are derived from that verified capture, never a fresh emit-time git
  read. No env var bypasses it; remedy is to re-run on a stable tree (don't edit a worktree while
  its gate runs).
- Every SUMMARY carries an `accelerators:` line (sccache/nextest/lane state, plus a `mold=` token and
  a `perf=` profiling-capability token on Linux hosts, #2859/#3249) — degradation there is
  actionable, not noise. `perf=paranoid-<N>`/`kptr-restricted` means THIS BOX CANNOT BE PROFILED (a
  PERMISSION verdict, not a missing capability): re-run `bash scripts/bootstrap-agent-machine.sh
  --yes`, which installs + verifies `/etc/sysctl.d/99-cqlite-perf.conf`. Self-test:
  `bash scripts/tests/test_agent_gate_summary.sh`.

## Core Commands

```bash
cargo build
cargo test --package cqlite-core            # needs CQLITE_DATASETS_ROOT exported — see "Test Data"
env RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features   # CI mode
cargo fmt
bash test-data/scripts/smoke-test-all-tables.sh
bash test-data/scripts/fetch-datasets.sh    # fetch real SSTable binaries; USE the export line it prints
bash test-data/scripts/fetch-datasets.sh --verify-only   # is my root usable? mutates nothing
```

Everything else (CLI usage/modes/output precedence, Python/Node build + test + examples, write
support, delta-export, profiling, feature-flag builds, fuzz runs): `docs/development/dev-cookbook.md`.

## Workspace Structure

```
cqlite-core/     # Core library (SSTable parsing, query engine)
                 #   storage/commitlog/ — Cassandra 5.0 CommitLog segment reader (#2389),
                 #   sibling of storage/sstable/ and storage/write_engine/ (Cassandra's
                 #   CommitLog, NOT CQLite's own write_engine::wal)
cqlite-cli/      # Command-line interface
bindings/python/ # Python bindings (PyO3) — M4 complete
bindings/node/   # Node.js bindings (napi-rs) — Phase 3 complete
test-data/       # Real Cassandra 5.0 SSTables for testing
tools/           # sstabledump-validator, format-validator
fuzz/            # cargo-fuzz crate — own workspace, EXCLUDED from the main one
```

**Planned (M6)**: `bindings/wasm/`. Full source map (parsers, writers, query engine, bindings
layout, binding structure trees):
[source map](https://pmcfadin.github.io/cqlite/agents-developing/source-map/) +
`docs/development/dev-cookbook.md`.

## Development Standards

### No-heuristics mandate (issue #28)
Authoritative metadata only — schema, else `Statistics.db`. No type guessing. Schema-aware decoding
when schema present. Legacy heuristics live only behind the opt-in `legacy-heuristics` feature flag.
Doctrine: [no-heuristics](https://pmcfadin.github.io/cqlite/agents-developing/no-heuristics/).

### Supported formats (version floor)
CQLite targets Cassandra 5.0 — `na`+/`nb` BIG and `oa`/`da` BTI in scope; pre-`na` (`ma`–`me`,
Cassandra 3.x) is out of scope and SHALL NOT be introduced, supported, or reviewed for correctness
(reviewers incl. roborev).
Enforced in code: `BigVersionGates::from_version` rejects `< na`, `BtiVersionGates::from_version`
rejects non-`da` (`Error::UnsupportedVersion`); `SSTableReader::open` propagates. Do not re-litigate
pre-`na` "regressions."

### Write surface: UNCOMPRESSED SSTables only (claim boundary, issue #1406)
The production write surface (flush + compaction via `SSTableWriter`) emits **uncompressed**
SSTables and never a `CompressionInfo.db`. The compressed-write building blocks
(`CompressedDataWriter`, `CompressionInfoWriter`) are built but **UNWIRED** — fixture-synthesis
only, zero Cassandra-side parity coverage. Fail-closed in code: configuring compressed production
writing returns `Error::UnsupportedFormat`. Do NOT claim CQLite emits compressed SSTables (manifest:
`claim.blocked.compressed_sstable_writes`; safe wording `claim.safe.uncompressed_sstable_writes`).
Wiring them (posture a) is issue #1406.

### Public API of `cqlite-core` (snapshot guard, issue #1712)
A change to `cqlite-core`'s public API must regenerate `cqlite-core/pub-surface.snapshot` **in the
same commit** (`bash scripts/ci/check-pub-surface.sh --regenerate`); the full gate's `pub-surface`
component FAILs otherwise. An unexpected diff there is a **semver decision, not a formatting
chore** — say in the PR body why the surface moved. The guard also asserts the crate root tells the
truth: an unconditional, non-`#[doc(hidden)]` `pub mod NAME;` must actually be present in the
default-feature surface, so a cfg gate can never again hide inside a module file while the crate
root advertises the module. **Stated boundary**: granularity is item PATHS, kinds,
associated-item NAMES (methods, enum variants, public fields, associated consts/types) and
`pub use` re-export edges — never SIGNATURES, generics or bounds, and trait/synthetic/blanket impl
members are excluded by design because they move on any dependency bump.

### Code quality
- `RUSTFLAGS="-D warnings"` must pass; no `unwrap()`/`expect()` in library code; `thiserror` for errors
- Memory target: <128MB for large files

### File size (campsite rule)
Keep files small — agentic context cost scales with file size. Targets (total lines, inline tests
included): source `~800`, test files `~1500`. The gate's `file-size` ratchet FAILs if your change
grows an over-threshold `.rs` file (or pushes one over). Touching an over-threshold file → split it
by responsibility (source: epic #1116; tests: #1135). Genuinely out of scope → re-run with
`CQLITE_ALLOW_FILE_GROWTH=1` and leave a note linking #1116/#1135.

### Testing
- Integration tests use real SSTable data only; validate against `sstabledump` output via JSONL
  reference files —
  [validation playbook](https://pmcfadin.github.io/cqlite/agents-developing/validation-playbook/)
- Never let a dataset-dependent test pass on an empty dataset (0-rows-when-present = failure)
- **Resolve fixture roots per TABLE, and assert per CASE (issue #3220)**: a lane that picks its
  corpus root by KEYSPACE (`root.join(keyspace).is_dir()`) and commits to it can pass without ever
  running — a `CQLITE_DATASETS_ROOT` holding `test_da/` but not the git-committed
  `test_da/multiclustering_table-*` made the #3032 case skip silently behind a green suite. Use
  `cqlite-core/tests/support/datasets_root.rs::sstables_root_for_table`, which walks EVERY candidate
  root (env, then checkout) for that table's `*-Data.db`. And never terminate a corpus loop with a
  suite-wide `assert!(ran > 0)`: it cannot see one case skipping behind its siblings — assert per
  case (committed fixtures = `must_run`, fail-closed unconditionally).
  Resolve by EVIDENCE, never by a preference ordering: neither root is a superset — a fleet
  `/data/datasets` measured 144 `*-Data.db` over 122 tables yet lacks the one committed
  `test_da/multiclustering_table`, which the checkout's 31 parity references carry — so *any* fixed
  env-first/checkout-first rule picks wrong for one set of tables. That dissolves #3104's "prefer
  the already-exported root" fix for the lanes on this resolver; **#3104 stays open** for what the
  resolver does not reach (whole-corpus `#2078` preflight, count-naming diagnostics, `--lite`
  small-corpus warning, and the doctrine text still telling agents to override the exported root).
- **Two parity oracles (issue #1742)**: *physical-dump parity* (the `*-Data.db.jsonl` sstabledump
  goldens) enumerates every on-disk cell INCLUDING tombstones/deleted/expired-TTL rows, so it CANNOT
  catch a read-time-reconciliation bug (both sides keep the shadowed rows → green while a real
  `SELECT` diverges). *Query-semantics parity* (`test-data/query-semantics-oracle.json`, gate
  component `query-semantics-oracle`, test `query_semantics_oracle_parity.rs`) records the
  post-reconciliation result set of a canonical `SELECT` at a PINNED `now` (never wall-clock). Add
  the correct oracle for the property under test; correctness of `SELECT` output needs the semantic one.
  The CQLite-vs-CQLite complement is the *point-vs-full differential lane* (issue #1918,
  `cqlite-core/tests/point_vs_full_differential.rs`): it runs the same point-eligible query under
  forced `CQLITE_READ_PATH=point` and `=full` and asserts identical rows/values/order at a PINNED
  `now` — catching a divergence between the two read paths that a physical dump cannot see.
- **Third blind spot: a CQLite-WRITTEN + CQLite-READ round-trip test is INVARIANT to a uniform
  framing/serialization error (issue #3042).** Both sides make the *identical* mistake, so the
  round-trip closes and the test stays green while real Cassandra-written data reads wrong — and,
  symmetrically, CQLite-written data is unreadable by Cassandra. Such a test can **never** substitute
  for a Cassandra-written fixture; it validates self-consistency, which is not the property anyone
  cares about. Concrete instance: the only arity-2 BTI test,
  `cqlite-core/tests/issue_908_bti_canonical_write.rs`, is CQLite-written and CQLite-read and asserts
  only ordering/structure, so it is invariant to exactly the framing defect of **#3002 (BTI `Rows.db`
  row-index root base 2 bytes low — missing the `writeWithShortLength` 2-byte prefix, masked by a
  compensating encoder defect that omitted the leading `0x40 NEXT_COMPONENT`)**. Two defects that
  cancel are undetectable by a symmetric test *by construction*. The oracle that caught it is
  `cqlite-core/tests/issue_3002_bti_rows_root_base.rs`, asserting against the real Cassandra 5.0 `da`
  fixture with every expectation derived from Cassandra's writer/reader source — never from CQLite's
  prior behavior. Rule: for any on-disk framing/encoding property, the oracle must be
  **Cassandra-written bytes** (or Cassandra source), never CQLite's own output. Long form:
  [validation playbook](https://pmcfadin.github.io/cqlite/agents-developing/validation-playbook/).

### Fuzzing (issue #1614)
`fuzz/` is a cargo-fuzz/libFuzzer crate in its own workspace, excluded from the main one — the gate
and default builds never compile it; fuzzing needs nightly and is out of the stable gate. Five
targets prove the parser never panics/hangs/OOMs on arbitrary bytes. CI: `fuzz.yml` (PR smoke +
nightly long-run); crashes are filed as bug issues. Run commands: `docs/development/dev-cookbook.md`.

## Test Data

Location: `test-data/datasets/sstables/` — keyspaces `test_basic` (8), `test_collections` (8),
`test_timeseries` (9), `test_wide_rows` (8). **Pass rate: 100% (33/33, Dec 2025).**

The repo ships only JSONL reference files; fetch real binaries with
`bash test-data/scripts/fetch-datasets.sh`, then export **the exact
`export CQLITE_DATASETS_ROOT=<abs>` line that script prints** — it names the only root that run
guarantees, and on a fleet box it is often a machine-local root (e.g. `/data/datasets`), NOT
`$PWD/test-data/datasets`. The printed line beats any root remembered from this file. The script
rejects every unrecognized argument (exit 2) because its default path is destructive
(`rm -rf` on the dataset root); `--verify-only` probes a root without mutating anything, `--help`
lists the flags. `--verify-only` also **reports** (never repairs) git-tracked fixtures a
SIGKILLed fetch left deleted: it names them, prints the exact `git restore` one-liner and exits
non-zero — distinct from the generic "does not hold a usable dataset corpus", and distinct again
from `NO SUBJECT` (out-of-repo root) and `COULD NOT MEASURE` (census untakeable) (#3310).

**`CQLITE_DATASETS_ROOT` alone is sufficient on every layout (#3131/#3148)** — the corpus root needs
no `schemas` sibling. The CQL schema fixtures (`test-data/schemas`, 23 committed files incl.
`legacy/` + `udts/`) are **committed source resolved checkout-relative** (anchored on the
workspace-root `Cargo.toml`), never derived from `CQLITE_DATASETS_ROOT`. `CQLITE_SCHEMAS_ROOT` is an
optional out-of-tree override and **MUST be absolute**: a relative value is rejected fail-closed by
both the gate and the tests, because the gate resolves it against the repo root while cargo resolves
it against each package dir — so it would certify one schemas root while the tests read another.
Without Data.db files, query tests pass but return 0 rows. Dataset pins:
[test data](https://pmcfadin.github.io/cqlite/agents-developing/test-data/).

## Feature Flags

Default (cqlite-core): `all-compression` (LZ4, Snappy, Deflate, Zstd), `state_machine`,
`write-support` (#558). Non-default: `cli-helpers` (#249), `parquet` (#682), `delta-scan` /
`delta-export` (#696/#705), `legacy-heuristics` (opt-in pre-5.0 heuristic fallbacks, #28), `metrics`,
`experimental` (gates `Database::flush()`/`compact()`, the INSERT executor path, the schema JSON
exporter, bloom-filter tests (#65), and the unimplemented `Storage::put`/`delete` stubs (#175)). Build
recipes: `docs/development/dev-cookbook.md`.

## Troubleshooting

- **Missing test data / 0 rows**: `bash test-data/scripts/fetch-datasets.sh`, then export the
  `CQLITE_DATASETS_ROOT=` line it prints — NOT `$PWD/test-data/datasets`, which on a fleet box is a
  corpus-less root the fetch never populates. `--verify-only` re-checks an existing root
  non-destructively. No `schemas` sibling is needed (#3131).
- **Clippy failures**: run with `RUSTFLAGS="-D warnings"` to match CI
- **Parsing issues**: `docs/sstables-definitive-guide/chapters/appendix-f-known-limitations.md`
- **Python bindings**: Rust 1.85+, Python 3.9+, `pip install maturin`, then
  `cd bindings/python && maturin develop --profile dev`

## Resources

- **Definitive Guide**: `docs/sstables-definitive-guide/`
- **Agent developer docs**: https://pmcfadin.github.io/cqlite/agents-developing/
- **Issues**: https://github.com/pmcfadin/cqlite/issues
- **Cassandra source — read it at the PINNED TAG, never a working tree (#3041)**: CQLite targets the
  Cassandra **5.0** on-disk format, so the authority is a `cassandra-5.0.8` tag read:
  ```bash
  git show cassandra-5.0.8:src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java
  ```
  Browse the same pin at https://github.com/apache/cassandra/tree/cassandra-5.0.8. A **local clone is
  OPTIONAL and BRANCH-SENSITIVE**: a checkout may sit on `trunk`/`6.0-alpha`/any non-5.0 line, whose code
  is NOT the 5.0 format and yields confidently-wrong answers, so read through the tag ref
  (`git -C <clone> show cassandra-5.0.8:<path>`) — never the checked-out files. There is no guaranteed
  clone path on any machine; `$CQLITE_CASSANDRA_REPO` names one when a tool needs it.

### Format authority — a CQLite `file:line` is NEVER format authority (#3041)
Citing CQLite's own code to justify CQLite's behavior is **circular reasoning**. Format authority is, in
order: (1) the pinned `cassandra-5.0.8` Cassandra source, (2) `sstabledump` output, (3)
`docs/sstables-definitive-guide/`. A CQLite source line is evidence of *what CQLite does*, never of
*what is correct*.

## Agent-Team Conventions

- **Implementers commit after each meaningful unit of work — this is WORK-LOSS insurance, not just
  review hygiene (#3042).** Reviews landing while context is fresh is the smaller half. The larger
  half: a subagent starved of CPU (a co-scheduled gate, a heavy sibling lane) is killed by the **600s
  stall watchdog** and **loses every uncommitted change** — 3 agents lost all their work this way in a
  single session. A commit is the only thing that survives the kill; the harness re-invoke starts from
  the last commit, not the last edit. So commit early and often, before any long-running or
  CPU-contended step, even mid-refactor and even when the unit feels too small to review.
- Stay within your assigned issue's scope; flag cross-cutting changes to the lead instead of editing
  another teammate's files.
- An issue is "done" only when tests pass, coverage meets threshold, roborev is clean, and both the
  spec-auditor and coverage-reviewer sign off.

### The implement loop (#1821/#2084/#2086/#2087/#2088) — ONE design, review before gate, gate once

```
implement (TDD) → --lite each fix round (summary-file redirect)
  → rust-reviewer + roborev on the lite-green diff   (review-first, DEFAULT)
  → fix rounds: --lite re-cert + diff-scoped targets  (NEVER a full gate per round)
  → open PR
  → flow-closer { FULL gate ONCE → C → final roborev → merge-on-green → finalize }
```

- **Review-first (#2086)**: review BEFORE the first full gate so the ONE gate certifies
  already-reviewed code. Skip ONLY for a genuinely mechanical diff (no `pub`-item change AND single
  call site AND no new surface). When in doubt, review.
- **roborev invocation — `scripts/flow/roborev-review.sh` is the ONLY sanctioned call, and it requires
  BOTH `--agent` and `--model` (#2964/#2433/#3037).**
  `bash scripts/flow/roborev-review.sh --agent <agent> --model <model> [--repo <abs-path>] [--base <ref>] [--log <path>]`
  — codex is `--agent codex --model gpt-5.6-sol`; Claude is `--agent claude-code --model claude-opus-5`.
  `--repo` defaults to the toplevel of `$PWD` (resolved absolute), `--base` to `origin/main`. Retain ONLY
  its `==== ROBOREV REVIEW SUMMARY ====` block (header deliberately distinct from all three
  `AGENT-GATE *SUMMARY` blocks so neither can be pasted as the other), never the transcript — that goes
  to the `log:` path named in the block. Exit `0` PASS / `1` FAIL / `3` NOTHING-TO-REVIEW / `2` usage
  error; **any** non-PASS terminal `RESULT` — `NOTHING-TO-REVIEW` included — is a failed review round and
  a blocked merge, never "roborev clean". Four rules: **(1)** the NON-SANCTIONED direct forms are
  `--branch` **WITHOUT** an explicit `--repo` (from a worktree it resolves against the ROOT checkout),
  the two-positional commit-range form (its range base is git's EMPTY TREE), and a SINGLE-SHA review (it
  covers ONE COMMIT, certifying a multi-commit branch from its last commit alone). `--repo` is what makes
  `--branch` correct, so the wrapper reviews the RANGE `--branch --base <base> --repo <abs>` — measured
  5/5 census code files delivered, vs 3/5 for the other two. **(2)** The **reviewed RANGE must be VERIFIED
  against `<base>...HEAD`** — the wrapper asserts BOTH endpoints from the **job record's structured
  fields** (`roborev list/show --json`; `git_ref` is `<base40>..<head40>`, echoed in `reviewed-sha:`
  beside a `job-record:` completeness key), with the stdout `Enqueued job <N> for <sha>` line DEMOTED to
  the job-id carrier: for a range review it names only the BASE, so an unavailable record FAILs rather
  than falling back to prose that verifies nothing. A range that does not match, a SINGLE-COMMIT record
  (even one equal to HEAD), or a base-equal scope **aborts the round** — base-equality is the signature of
  the worktree bug. **(3)** `"contains no code changes to review"` on a
  NON-EMPTY diff is a **HARD FAIL**, never a pass. **(4)** A docs-only (code-free) diff **cannot be
  roborev-certified at all** — and "docs-only" means a **CODE-FREE CENSUS as the wrapper classifies it,
  NEVER a `docs/` path prefix** (#3229). The mechanism, stated correctly: **roborev drops exactly what
  its configured `exclude_patterns` pathspecs match — it makes NO code/non-code judgement.** The measured
  22-markdown-absent / 5-code-present split happened because `*.md` is CONFIGURED, not because the
  reviewer recognised prose, so for prose-only the constructed diff is genuinely EMPTY and that verdict is
  a truthful report of an empty input, not a malfunction. The wrapper's
  deterministic pre-enqueue `code-free:` check fails it before any review is enqueued, and
  `prompt-content:` therefore asserts the CODE subset of the census (an unretrievable prompt FAILs — there
  is no passing `UNAVAILABLE` there). The sanctioned substitute is
  primary-source verification recorded in the PR (e.g. `git show cassandra-5.0.8:<path>`), and no
  docs-only change may ever record "roborev clean".
  **The same mechanism cuts the other way, and did**: a configured `docs/**` discarded 33 EXECUTABLE
  measurement-harness files on PR #3222 — the `docs/reports/*-artifacts/` harnesses this repo ships **by
  convention are reviewed CODE**, so a PR carrying them is NOT a docs-only change and MUST be
  roborev-certified. The deny-list is now narrowed to `*.md` plus artifact extensions **scoped to
  artifact-bearing DIRECTORIES** (measured after the narrowing: 71 `docs/` executables reach the reviewer,
  0 markdown does, and nothing outside `docs/` is newly excluded). **NOTHING PREDICTS THE EXCLUSION SET
  PRE-ENQUEUE.** A `census-exclusion:` key that did — a bash port of roborev's `git.FormatExcludeArgs` over
  a TOML parse of three config sources — was built on #3229 and **REMOVED by owner ruling, deferred to
  #3283**: its false-PASS count was *increasing* across review rounds (1, 1, 2, 3), and two of the last
  round's three defects lived in code the two preceding fix rounds had just introduced. **A guard with
  known documented false-PASSes is worse than no guard, because it invites reliance it cannot support.**
  So a path the reviewer did not receive surfaces AFTER the review, under `prompt-content:`, fail-closed,
  with a cause that names the symptom rather than the mechanism — **if `prompt-content:` FAILs, suspect
  `.roborev.toml` first.** The class-level lesson, recorded for #3283: **a port is a second
  implementation, and a second implementation's correctness is only knowable by differential testing
  against the original** — the oracle re-derived Go's trim rules in bash and was tested against a *model*
  of Go, not against Go, so its NBSP divergence (Go's `unicode.IsSpace` trims U+00A0; bash trims do not)
  was unfindable by care. The narrowing's asymmetry is deliberate — **noise, never blindness** — but that claim is SCOPED, and the
  scope is the whole content of it: it holds for **inert dumps** (`.txt`/`.log`/`.err`), where exclusion
  costs only **noise** (a new artifact *directory* is re-admitted to review prompts, a token cost, while
  the swallow direction can only ever fail loudly). For a **code-bearing format**
  (`.json`/`.html`/`.svg`) exclusion is **BLINDNESS**, because such a file can be **functional
  configuration under any path**. So exclusion of code-bearing formats **MUST be scoped by directory,
  never by extension alone**. **This asymmetry was first written unqualified and THIS CHANGE falsified
  it (#3229):** an extension sweep across ALL of `docs/` was retired because `docs/**/*.json` hid
  `docs/observability/grafana/dashboards/cqlite-overview.json` — the gate's own `kit-dashboard-drift`
  component guards that dashboard, so the extension-wide form hid from the reviewer a file the gate
  treats as correctness-bearing — from the reviewer's diff *and* classified it code-free, i.e.
  unreviewable by construction; `docs/reports/delivery-telemetry.schema.json` went the same way. The
  durable generalisation: **an extension describes a FORMAT; a directory records an INTENT** — someone
  decided that tree holds artifacts — so a directory is the better proxy for "generated". So the
  patterns are `<artifact-dir-glob>/**/*.<ext>` over exactly four directories
  (`docs/reports/*-artifacts/`, `docs/round-artifacts/`, `docs/**/jfr-reports/`,
  `docs/sstables-definitive-guide/diagrams/`) and everything else under `docs/` is **reviewed**. Still
  extension-scoped *within* each directory, never a blanket `<dir>/**` — those directories hold the
  executable harnesses that ARE the census `docs/**` swallowed. The census-side mirror
  (`CODE_FREE_ARTIFACT_EXTENSIONS` / `CODE_FREE_ARTIFACT_DIR_GLOBS`) and the committed `.roborev.toml` are
  the same fact written twice and are **maintained BY HAND** — add an extension or a directory in both, in
  one edit. There is deliberately **no automated drift assert**: the one that existed depended on the
  removed TOML parser and went with it, so drift surfaces the slow way, as a `prompt-content:` FAIL on
  someone's report PR, until #3283 lands a guard whose own correctness is establishable. That gap is a
  **known reduction in coverage**, accepted, not argued away.
  **The verdict split follows ONE rule — apply it to any call of this shape without asking: FAIL where
  the author can act; NOTICE where only the information is actionable; never silence.** `NOTICE` stays
  outside the wrapper's failing-capable scan (`FAIL|FINDINGS|ERROR|INCONSISTENT`) because `vacuity-tier1:`
  needs it as an advisory.
  **NEITHER HALF OF ROBOREV'S EXCLUSION SET IS MODELLED (#3283 configured, #3278 compiled-in).** Beyond
  `exclude_patterns`, roborev appends a hard-coded lockfile/cache deny-list (`**/Cargo.lock`, `**/go.sum`,
  `**/pnpm-lock.yaml`, `**/.cache/**`, …) that no configuration can switch off. Modelling either half was
  built and then **DELETED on #3229**, and **subtraction cannot introduce a false PASS** — with nothing
  predicted, nothing is excused. So the residual, stated rather than left to be rediscovered: **a path
  roborev excludes by either half is silently dropped from the reviewer's diff, nothing names it
  pre-enqueue, and `prompt-content:` FAILs on its absence.** That **fails CLOSED** — the cost is a
  diagnostic whose stated cause names the symptom, not the mechanism. `prompt-content:` accordingly expects
  **every** census code path and subtracts nothing: no key is licensed to tell another which paths to skip.
  Also: **`prompt-content:` never prints a `0/0` PASS** — a key with no subject has no verdict to give.
  **`prompt-content:` ASKS ONE QUESTION, AND THERE IS NO DELIVERY CLASSIFIER (#3312, owner ruling (4)).** Are
  the census **CODE** paths present in the prompt the reviewer was sent? **Present ⇒ PASS. Absent ⇒ FAIL,
  unconditionally**, whatever caused it. The wrapper used to infer HOW roborev delivered the diff — inlined,
  or by a path to a **transient** `.roborev/roborev-snapshot-<id>/` file it deletes before `--wait` returns,
  or the delegated tier that ships neither and tells the reviewer to run git itself — and that inference
  produced **four consecutive High-severity false verdicts, in both directions**: a header set consulted
  before an oversize marker (a delegated review PASSing on repository-quoted headers), a candidate outliving
  its block, a real inline delivery under an unrecognised heading producing no evidence, and a block opener
  keyed on heading text that roborev treats as caller **data**. The instances differed; **the cause did not** —
  roborev's prompt embeds repository-controlled content (project guidelines/`AGENTS.md`, additional context,
  previous-review bodies) at column zero, indistinguishable from roborev's own text, so structure inferred
  from it is spoofable both ways. **No terminating marker exists** (the only structural one was roborev's
  fenced diff, and repository content can contain fences too), so the owner deleted the inference rather than
  patch a fifth instance. Gone with it: block detection, heading parsing, fence evidence, `mixed-delivery`,
  candidate lifetime, the snapshot/delegated distinction, the lexical path binding, snapshot-path extraction,
  the three `snapshot-*` keys, this key's `NOTICE` verdict and its exemption — so the **affirmation backstop
  again has no per-key escape hatch**. Every one of the four Highs is now *unexpressible*.
  **THE ACCEPTED COST, stated because it is real: a snapshot-delivered diff and a vacuous review that
  received NOTHING are IDENTICAL to the machine** — neither has census paths in its prompt, so both FAIL.
  What distinguishes them is a **human plus the review's token accounting** (genuine: 398k–649k input /
  314k–554k cached; vacuous baseline ~18.7k / 0). That trade was chosen over a machine guessing from
  injectable text.
  **THE ABSENCE WAIVER — the break-glass, its four constraints, and why the documentation is not the
  credential (#3312 job 23).** The **OWNER or the coordination LEAD** may excuse an absence FAIL with a
  **dedicated, column-zero line** of a PR comment:
  `roborev-waive: prompt-content-absent base=<40-hex> head=<40-hex> job=<id> reason=<why>`.
  **(a)** Human-authorized, never self-applied: a worker or closer may post **one** REQUEST comment —
  carrying the token accounting — and may never waive its own PR. **(b) Bound to the WHOLE REVIEW SCOPE**,
  not just the head: **base AND head AND job**, all required and all verified, because the authorizer
  judged **one** review — so a push, a different base *or a re-run* each need a fresh waiver, and one
  persistent comment can no longer excuse a later **vacuous** review at the same head. **(c)** It excuses
  the **ABSENCE verdict ONLY** — never any other cause — and the block still records what was absent, the
  authorizer, the bound scope and the reason, under a **distinct `WAIVED` token** (so nobody grepping
  `prompt-content: PASS` reads a waived run as certified) beside a `waiver:` key that names the state even
  when nothing was granted (`NONE`/`STALE`/`MALFORMED`/`UNAVAILABLE`, each leaving the FAIL). **(d)** The
  request carries the token accounting and the authorizer checks it.
  **AND THE LOOP HAS TO CLOSE, WHICH IT DID NOT (#3312 job 24).** The waiver binds a JOB, but the
  operator only learns the job id — and the token accounting — from the FINISHED run, and re-running the
  wrapper to apply a fresh waiver **enqueues a different job**, so the waiver was instantly `STALE`. As
  first built the break-glass was a **dead letter**: no sequence of actions got a legitimate absence past
  the gate. The fix is **not** to loosen the binding (dropping `job=` reopens the hole where one comment
  waives a later *vacuous* review at the same base+head) but to add
  **`--recheck-job <id>`**: re-decide THAT job's verdict, enqueueing nothing. The job is named
  **explicitly**, never resolved from base+head, or a re-run could inherit a waiver written for a
  different review. **A recheck inherits nothing**: `sha-assert` re-compares the record's `git_ref`
  against this base and head, the record's own review text becomes the transcript so
  `review-completed`, both vacuity tiers and `findings` are re-asserted from it (no review text ⇒ empty
  transcript ⇒ fail-closed), and `roborev-exit` reports `SKIP` rather than claiming an exit status for a
  process that never ran. The block declares **`MODE: recheck (job <id> …; NO review was enqueued)`** and
  **`recheck-of: <id>`** as its first keys — the way the gate declares `MODE: lite` — so a recheck PASS is
  legitimate but can never be pasted as evidence of a *fresh* review. Demonstrated end to end: absence
  FAIL → waiver naming that base/head/job → recheck ⇒ `WAIVED` + `RESULT: PASS`, with zero reviewer
  invocations; and a recheck of a *different* job stays `STALE`.
  **The marker is decided by ONE anchored pattern, and the reason is trimmed BEFORE it is judged**, so
  field order and value boundaries are enforced and `reason=TODO ` / whitespace-only reasons are refused
  like their untrimmed forms — per-field extraction had enforced neither.
  **AND THREE LAYERS STOP THE ARTIFACT BECOMING THE CREDENTIAL — the sharpest instance of this issue's
  recurring shape.** The first version accepted the marker *anywhere* inside a comment whose newlines had
  been flattened, and the absence diagnostic **printed a complete marker carrying the live sha** — so
  pasting the summary block into a PR comment, *the documented practice throughout this repo*, authorized
  the next run (RED-verified: the pasted block produced `prompt-content: WAIVED … RESULT: PASS`). A quoted
  example or a waiver *request* self-granted the same way. It is the same defect as prose inside a diff
  naming its own oracle, which is why the census matcher is column-zero anchored. Now: **(1)** comment line
  boundaries are preserved and the marker must **BE** the line — an indented, `>`-quoted, bulleted or
  mid-sentence copy is inert; **(2)** placeholder reasons are refused (an unsubstituted `<…>`, or a bare
  `why`/`todo`/`tbd` — `claim.sh`'s rule), so a pasted **template** reads `MALFORMED`; **(3)** no emitted
  diagnostic carries **any part** of the marker — not even the prefix — and points at `--help` instead.
  **THE UMBRELLA LESSON OF THIS ISSUE, and the most durable thing in it: CONTROL AND DATA MUST NOT SHARE A
  CHANNEL WHEN THE DATA IS ATTACKER-CONTROLLED (#3312).** Four separate High-severity defects were the same
  shape, and each individual fix worked while the family kept regenerating — because the shape was never
  named in one place. The instances, in the order they were found:
  1. **Prose inside a diff naming its own oracle.** A census path quoted in the reviewed text could satisfy
     the check that the reviewer *received* that path — which is why the matcher is anchored at COLUMN ZERO
     (every unified-diff body line carries a leading `+`/`-`/space/`@`/`\`, so body content cannot pose as
     a header).
  2. **The wrapper's own diagnostic printing a valid waiver marker** — an artifact that DESCRIBED the escape
     hatch BECAME it, because summary blocks get pasted into PR comments as a matter of course. Fixed by
     three layers: an anchored dedicated line, placeholder-reason refusal, and a diagnostic that emits no
     part of the marker at all.
  3. **Repository text reproducing roborev's delivery-block markers.** Delivery mode was inferred from
     prompt text that embeds project guidelines / `AGENTS.md`, so repo content could move a review into an
     uncertified mode in either direction. No terminating marker existed, so the owner deleted the
     *classifier* rather than patch a fifth instance of it.
  4. **A comment body forging its own author record.** Comments were flattened into one stream with an
     in-band author delimiter, so an unauthorized commenter could name an allowlisted login inside their own
     body and defeat the allowlist with one control character. Fixed by parsing `gh --json` STRUCTURALLY —
     author and body stay separate FIELDS of one object — so there is no delimiter to forge.
  **The generalisation to apply elsewhere:** when a decision is made from a stream that carries both your
  markers and someone else's payload, the fix is to REMOVE the shared channel (structured data, a separate
  field, a distinct file), not to choose a rarer delimiter — a rarer delimiter is still forgeable, and each
  narrowing only postpones the next instance. Where the channel genuinely cannot be separated, anchor the
  control tokens somewhere the payload provably cannot reach (column zero of a diff), and say in code that
  this is what the anchor is for.
  **THE FIFTH VARIATION, AND HOW THE CLASS WAS FINALLY CLOSED (#3312 job 29): AN AUTHORIZATION MUST BE THE
  SOLE NONBLANK CONTENT OF ITS PR COMMENT.** Leading/trailing blank lines are fine; anything else — prose, a
  code fence, a quote, an HTML tag, a second sentence — means the comment is **not** an authorization.
  **FOUR RECOGNISERS WERE TRIED AND SUPERSEDED**, each correct about the case in front of it, and they are
  named here so nobody reintroduces Markdown parsing thinking it was an oversight:
  (1) accept the marker **anywhere** in the comment ⇒ a quoted example granted;
  (2) require it to **be its own line** (column-zero anchor) ⇒ defeated indented, `>`-quoted, bulleted and
  mid-sentence copies, but not fences;
  (3) **skip fenced regions** ⇒ a fence preserves column zero, so a quoted example inside one granted;
  (4) **track fence open/close state** ⇒ a ```` ```bash ```` line *inside* a fence is CONTENT, not a closing
  delimiter, so the state desynchronised and a later marker granted — and HTML `<pre>`/`<code>` was never
  covered at all.
  Every one asked *"is this line DATA or CONTROL?"* of a grammar the **comment author controls**, which has
  unbounded ways to say "this is data" — so the list of recognisers never closes. **That is this issue's own
  umbrella lesson applied to itself: remove the shared channel, do not pick a rarer delimiter.** Parsing
  Markdown to separate data from control *is* sharing a channel. The sole-content rule removes it and is
  decidable **without parsing anything**: no quoting construct can be the only thing in a comment, because
  every quoting construct requires additional content.
  **Cost, and why it is arguably an improvement:** the authorizer posts a comment containing only the marker
  and puts commentary in a **separate** comment — the token accounting already lives inside `reason=`, so
  nothing is lost, and an authorization *should* be a clean unambiguous act rather than a sentence buried in
  prose. **A comment with other content is ignored silently, not reported malformed:** someone documenting
  the form (this repository's own threads do) never attempted an authorization, and `MALFORMED` would be a
  false accusation reprinted on every later run. A **marker-only** comment with bad fields is still
  `MALFORMED`. The `NONE` cause teaches both rules — sole content **and** top-level.
  **THE WAIVER'S THREAT MODEL, STATED WITH ITS LIMITS (#3312) — and the triage rule that goes with it.**
  Five consecutive review rounds landed in this one authorization path (marker anchoring, scope binding,
  author authorization, the parse channel, the enforcer path). Every fix was right, and the pattern predicts
  more — so the boundary is recorded to stop the next finding being patched instead of triaged.
  **A HOSTILE INVOKER IS OUT OF SCOPE, by construction.** Whoever runs the wrapper can edit it, replace the
  scanner file beside it, shadow `gh`/`python3` on `PATH`, or skip the wrapper and hand-write a
  `==== ROBOREV REVIEW SUMMARY ==== … RESULT: PASS` block into the PR. **No check inside a process defends
  against the party that controls the process**, and pretending otherwise is the false-assurance shape this
  issue exists to remove. The merge gate's real protection against a hostile *worker* is the **audit trail
  plus a human reading the PR**, not the wrapper.
  **WHAT IT DOES DEFEND, which is why the layers were worth building:** (1) **parties who do not control the
  invocation** — this is a public repository, anyone can comment, and a failing block *prints* base/head/job,
  so the allowlist + anchored marker + structured author association are what stop a stranger; (2)
  **accident and drift**, the larger category in practice — a pasted block, a quoted example, a stale waiver
  riding to a later review, a re-run inheriting an authorization meant for another job, an unsubstituted
  placeholder. Every fix in this path landed in (1) or (2).
  **THE TRIAGE RULE:** *"the INVOKER can bypass this"* ⇒ **out of model — record it, do not patch it**;
  *"a NON-INVOKER can bypass this"* or *"this can be bypassed BY ACCIDENT"* ⇒ **defect**. Same-host actors
  able to write these scripts or roborev's database are **invoker-class**, not third parties.
  **CHEAP HARDENING REMAINS WORTH IT** where an invoker could reach the same end another way: dropping the
  scanner-path env override cost nothing and closes contexts where the environment is influenced while
  files are not (a workflow injecting a variable). "Theoretically redundant" never justifies leaving a hole
  a non-invoker or an accident can walk through.
  **TWO RESIDUALS INSIDE THE MODEL, named rather than implied:** the marker is read from **top-level PR
  comments only**, so one posted inside a review body or a review-thread reply is silently not applied (the
  run reports `waiver: NONE` and the FAIL stands — fail-closed, but it reads as "my waiver was ignored");
  and **an authorized human can authorize carelessly** — pre-authorizing a job id, or waiving without
  checking the token accounting. Nothing mechanical detects either; the control is the permanent,
  attributable comment, which is why a substantive reason is required and recorded verbatim.
  **AND A SECOND, EQUALLY TRANSFERABLE RULE FROM THE SAME ISSUE: THE CONSTRAINED PARTY MUST NOT CHOOSE ITS
  OWN ENFORCER (#3312 job 27).** Hardening a check while leaving its *invocation* configurable moves the hole
  rather than closing it. Concretely: the waiver allowlist was deliberately hard-coded and asserted
  non-env-derived — *"an override is settable by the party the allowlist constrains"* — and then the **scanner
  that enforces that allowlist** was made env-settable (`WAIVER_SCAN_TOOL`), so an invoker could point it at a
  script printing `state=granted` and pass with **no authorized comment anywhere**. The protection had moved
  outward and been left open. The enforcer is now resolved from the wrapper's own directory with no override
  and no `${…:-…}` fallback, and the structural assert covers the **invocation** as well as the value.
  **Corollary for tests:** a case needing a different enforcer **substitutes the artifact** in its own scratch
  copy of the tree — never a path variable. A test-only seam is one more thing a real invoker can set, so the
  harness assert forbids reintroducing one (with the needle split so the guard cannot match its own line).
  **WHO MAY GRANT: AN EXPLICIT AUTHOR ALLOWLIST (#3312 job 25) — and the correction that produced it.**
  The comment author used to be *recorded but never authorized*, so on this **public** repository ANY
  commenter could copy the `base`/`head`/`job` values out of the failing block (they are printed in it)
  and make the merge gate pass. The residual had been written as *"we cannot distinguish the owner from
  the worker on a shared `GH_TOKEN`"*, which conflated **cannot enforce perfectly** with **cannot enforce
  at all** — so absence of a perfect check became absence of ANY check, the same permissive shape this
  issue is about. Now: the author must be on `ROBOREV_WAIVER_AUTHORS`, hard-coded in
  `roborev-review-oracles.sh` — **not** a config file and **not** env-overridable, because an override is
  settable by the party it constrains and one visible location keeps "who may grant" inside the diff a
  reviewer already reads. A well-formed marker naming this exact review from a non-allowlisted author
  reports **`waiver: UNAUTHORIZED (...)`** — distinct from `MALFORMED`, because the marker was fine and
  the author was not.
  **THE RESIDUAL SURVIVES, NARROWED TO WHAT IS TRUE:** the worker, the closer and the owner all post
  through the SAME login on this fleet, so nothing here can tell **which allowlisted human** posted a
  comment. "Only the owner or the coordination lead may GRANT; a worker may only REQUEST" is therefore
  **process-enforced with an audit trail** at that level — never a claim that authorship is unverifiable
  in general. **An unenforceable claim gets scoped to what is enforceable, never dropped whole.**
  **The hang and race classes are NOT REACHABLE because nothing is read — that is weaker than "fixed", and only
  it is true.** The predicate-family rule survives as **doctrine, not code** (the helper and its lint were
  deleted with the probes they served, since a lint with an empty subject set greens vacuously): **every
  `test`/`[` file predicate is two-valued, so it must collapse "cannot tell" onto one of its answers — and it
  always picks the permissive one.** If a filesystem probe ever returns to that code, this rule obligates the
  three-valued helper (`verified-absent` / `present` / `unreadable`) to return with it.
  **That is ONE SHAPE, found repeatedly on #3229, so it is now a RULE: a positive verdict requires an
  AFFIRMATIVE MEASUREMENT.** The shape is *a multi-state signal where only the BAD states are tested, so
  every unknown/unmeasured state inherits the PERMISSIVE branch* — a three-state signal took the permissive
  excusal path; an `UNAVAILABLE` corroboration state reached a `PASS` and **enqueued** (the code's own
  comment said the binary was the only oracle that could tell "no key recognised" from "nothing
  configured", then never required it to have *answered*); a `${end:-$start}` default degraded a failed
  `awk` bound to a 1-line scan. Those instances lived in a subsystem since deleted; **the shape is the
  lesson, and it was never theirs** — it was in the wrapper's own terminal verdict scan, which predates
  them all. So: never derive a pass from the ABSENCE of a bad signal; where an oracle is the SOLE evidence
  for a claim and could not be consulted the verdict is NON-PASSING and its text names what was
  unverifiable; key a permissive branch on the AFFIRMATIVE value (`= OK`), never on `!= <bad>`; and where a
  signal genuinely SHOULD be permissive, record the reason IN CODE at the branch. The wrapper's verdict
  scan is therefore a CLOSED grammar (unrecognised value ⇒ FAIL) plus a backstop that no PASS may carry a
  verdict-carrying key that is not affirmatively `PASS` — a `SKIP` means the check never ran, which is the
  vacuous pass itself. **Both are RETAINED after the oracle that surfaced them was deleted**, because they
  are properties of every remaining key, and leaving the terminal verdict permissive again would leave the
  wrapper worse than we found it. **And the closure must not itself be a prefix test**: `PASS*` accepts
  `PASSthisNeverRan` and `PASS-MEASUREMENT-DID-NOT-HAPPEN`, i.e. the guard against unplanned values would
  check a *spelling* rather than a *state* — the same shape one level down. So each value is reduced to its
  **verdict TOKEN** (up to the first space) and matched **EXACTLY**.
  **Paths are normalised ONCE, at the census, and that boundary is the fix for SIX blockers (#3229).**
  Rounds 2–4 of review produced six, and every one was a path-normalisation defect in a *different*
  consumer, because normalisation was scattered. Now the census reads `git diff --numstat -z` (and the
  survivor set `--name-only -z`), so paths arrive **RAW**, and RAW is the single representation used for
  classification, comparison and display; the one quoted-path decoder survives for the reviewer's prompt
  alone, with exactly one caller — the canonical matcher `roborev_diff_header_has_path`, which every
  consumer must ask rather than parsing headers itself. It reads every shape git emits: unquoted,
  **space-bearing** (`diff --git a/a b.txt b/a b.txt` — this repo tracks 40 space-bearing paths under
  `docs/`), **C-quoted** (`diff --git "a/\303\251.txt" "b/…"`), and the **MIXED** shape a rename produces
  (`diff --git a/<ascii> "b/<quoted>"`). Two measured costs of getting this wrong, in both directions: the
  census classifying a *quoted* spelling read `docs/é notes.md` as extension `md"` and called PROSE **code**,
  so the configured `*.md` legitimately removed it from the reviewer's diff while `prompt-content:`
  demanded it there ⇒ a **false FAIL** on an ordinary docs+code branch (reproduced against the tracked
  `docs/research/CQLite Writes (M5) — …md`); and a
  newline-delimited path set with `grep -Fxq` membership made a path's first line "prove" its presence ⇒ a
  genuine **false PASS**. A key that reds on correct input is the key agents learn to waive; a key that
  greens on absent input is worse. The invariant is asserted **structurally** in
  `scripts/tests/test_roborev_review_guard.sh` (no path-reading `git diff` without `-z`; the decoder called
  only from the matcher), because behavioural cases only cover the shapes someone already thought of.
  **A `.roborev.toml` change cannot certify itself (#3229) — three properties, one generalization:**
  **(1)** roborev's daemon binds a repository by its **`repos.root_path`** and reads **that ROOT
  checkout's** `.roborev.toml` — a *worktree* `.roborev.toml` edit is **invisible** to it, so under
  1:1:1:1 the file you edited is not the file your review applies. **(2)** The daemon **snapshots config
  at start**, so an edit needs a **daemon restart** to take effect. **(3) Generalized: any PR whose
  subject is a config the daemon (or a gate) reads from root cannot certify itself** — the same shape as
  `required` evaluating the aggregator and registry from the PR's **BASE** ref (below). Plan the
  demonstration for **after** the merge. Both (1) and (2) have cost real rounds: (1) produced a
  since-removed key's `PASS (7/7 survive)` about a config roborev never read, caught only by the
  pre-existing `prompt-content: FAIL (1/7 absent)` — **defence in depth paid out in the direction nobody
  plans for, and it is why `prompt-content:` is the layer that stayed**; (2) made #3234 measure `exclude_patterns` as having
  "no observable effect" (its single daemon restart preceded every config edit and never followed one).
  The durable lesson from that pairing: when the newer, cleverer guard and the older, dumber one disagree,
  **the one that measures what actually happened wins** — which is why the descope kept `prompt-content:`
  and dropped the predictor.
  Push first: an unpushed implementation commit is
  itself an empty-diff cause, and the wrapper asserts the push and FAILs otherwise. **Why:** FOUR
  confirmed paths make roborev report clean having reviewed NOTHING (or only part), and a vacuous pass is
  TEXTUALLY IDENTICAL to a genuine one — (T1) from a worktree, `--branch` without `--repo` resolves
  against the ROOT checkout (normally on `main`) and enqueues the BASE commit: enqueued `39900e4db`
  (= origin/main) while branch HEAD was `4e7ab591e`; (T2) the two-positional range form anchors the range
  at git's EMPTY TREE (`4b825dc6…`); (T3) a diff every path of which the configured
  `exclude_patterns` match is SILENTLY DISCARDED even with the right SHA and the right `--repo` — a
  code-free diff by default, and under a mis-scoped pattern like `docs/**` an EXECUTABLE one too — so
  **SHA verification alone is insufficient**; (T4) a single-SHA review covers
  ONE COMMIT — a PARTIAL review whose enqueued sha EQUALS HEAD, so no sha check can see it (this is the
  form #2964's own AC2 asked for; the wrapper implements the AC's intent instead).
  Token accounting is the tell: genuine reviews
  398k–649k input / 314k–554k cached / 5.0k–6.3k output over ~2m30s, vs the vacuous baseline 18.7k input
  / 0 cached / 53–56 output in 8s. Real cost: on #2950 two vacuous runs "passed"; re-run correctly
  against the real SHA, the SAME diff produced TWO REAL BLOCKERS. 1:1:1:1 puts EVERY issue in a worktree
  and `flow-closer`'s final pass is a MERGE GATE — so this could merge unreviewed code fleet-wide.
  Reviewer-selection trap: `--agent claude-code` alone still inherits `review_model = 'gpt-5.6-sol'` from
  `.roborev.toml` (the repo pin overrides your global `~/.roborev/config.toml`) — an OpenAI model name
  Claude cannot serve, which fails as a silent review failure that looks like an outage; historically
  mirrored (codex-on-a-ChatGPT-account hard-`400 'opus' model is not supported`). Hence the wrapper
  enforces both. `gpt-5.6-sol` is **codex's own built-in default, not a config pin** — there is no
  `~/.codex/config.toml` on the worker boxes; the bare `codex` default moved `gpt-5.5` → `gpt-5.6-sol` in
  the 0.142.5 → 0.145.0 upgrade, so a version bump can silently move it again. `codex --version` + a bare
  `codex exec` header is how you check what it actually resolves to.
- **Scoping a review (`exclude_patterns`) is a ROOT-checkout operation (#3229/#3234).** The daemon binds
  the repo via `repos.root_path` and reads the **ROOT checkout's** `.roborev.toml`, so editing it inside a
  worktree is a silent no-op that looks exactly like "`exclude_patterns` doesn't work" — and `roborev
  config get` answers differently depending on cwd. Edit the root checkout's file and restart the daemon.
- **flow-closer (#2084/#2668)**: the full gate, the final roborev pass, and the merge run inside the
  disposable `flow-closer` subagent — the lead retains only its terminal packet (verdict, PR URL,
  summary-file path, ≤10 lines residual), never gate stdout or review churn. The closer has **no
  `Agent` tool**, so **C is spawned by the lead at the closer's `NEEDS-SPAWN` request** (the closer
  stops, emits a `NEEDS-SPAWN {role: spec-auditor, …}` packet, and the lead spawns `spec-auditor`
  then re-invokes with the verdict; a src-design fix respawns `sstable-developer` the same way).
  Before arming `gh pr merge --auto` the closer runs the scripted pre-merge assert
  `scripts/flow/premerge-assert.sh <pr> <certified-sha>` (#2456) — refusing to merge unless the PR
  head still equals the certified SHA — and re-reads comments for a fresh `HOLD:` order. With `--auto`
  armed, GitHub lands the PR on the `required` check going green (#2667); no CI busy-wait.
- **Severity triage (#2088, rubric `docs/development/roborev-severity.md`)**: roborev **blockers**
  are fixed pre-merge — each re-triggers `fix → --lite (+ any diff-relevant parity/integration
  target) → re-review` (#2087). **Nits** never trigger
  a re-verify round: batch all of a PR's nits into ONE linked follow-up issue at merge time. When in
  doubt, blocker. Every pre-roborev self-check class below is BLOCKER by definition.
- **Post-gate polish (#1892)**: after a full PASS at `X`, a test/docs-only diff `X..Y` re-certifies
  with `--delta` (fail-closed; see gate table above), never a repeat full gate. The nightly
  `gate.yml` deep-check re-runs the FULL gate on `main` as the standing backstop.
- `--lite` NEVER replaces the full gate — the full `AGENT-GATE SUMMARY` is the only run that counts.

### Pre-roborev self-check (common findings to pre-empt)
`roborev_findings` is the #1 recurring delivery cost. Full guidance:
https://pmcfadin.github.io/cqlite/agents-developing/roborev-findings/
Three of these classes are now **mechanized as `--lite` lints** (#2656) — the `roborev-lints`
gate component (GHA injection via `scripts/ci/check-workflow-injection.sh` + the #2642
wall-clock guard) plus clippy's `manual_range_contains` — so a reintroduction FAILs the fast
loop, not a review round. The rest stay hand-checked (no low-false-positive static signal).
- **GitHub Actions injection** — never interpolate `${{ inputs.* }}`/step outputs into `run:`;
  allowlist-validate fail-closed before any secret step, pass via quoted env var. MECHANIZED
  (`roborev-lints`): an attacker-controlled `${{ }}` context inlined in `run:` FAILs `--lite`;
  mark a provably-safe line `injection-lint-allow`.
- **clippy `manual_range_contains`** — write `(a..=b).contains(&x)`. MECHANIZED (clippy).
- **Integer overflow/saturation** — use `num_bigint::BigInt` for unscaled decimal math; compare
  signs/adjusted-exponents first; never materialize `10^scale` with unbounded exponent.
- **Float ordering vs Java** — `total_cmp` ≠ `Float/Double.compare`; use an explicit comparator
  (NaN last, `-0.0 < +0.0`) when matching Cassandra.
- **Wall-clock races in tests** — capture the time window to cover ALL sampled operations.
  MECHANIZED (`roborev-lints`/`tooling-tests`, #2642): a wall-clock threshold assert in the
  correctness test path FAILs; mark a deliberate `#[ignore]`d perf assert `perf-gate-allow`.
- **No-heuristics violations** — never infer type/behavior from byte patterns.
- **Gitignored reference binaries** — `git add -f` tiny parity references; verify against a fresh
  `git worktree add --detach HEAD`, not the dirty tree.

### Spec-driven work (OpenSpec)
- OpenSpec is the front door for **design-driven** work (bindings/M6, query-engine surface, CLI/REPL
  UX, perf/M7, process). **Oracle-driven** bug fixes (SSTable parsing, compaction/tombstone parity,
  type decode) stay a GitHub issue + pinned parity test — no OpenSpec change.
- Merge flow (design-driven): `apply → gate → C (intent audit) → roborev → merge → archive`. **C** =
  `spec-auditor` anchored to `openspec/changes/<name>/specs/**`, after the gate is green. **B**
  (optional, `roborev-design-review-branch`) escalates when C reports `partial`, high stakes, or
  doctrine is touched.
- Done = gate PASS + **C PASS** (every requirement `satisfied` with a public-surface test as
  evidence; `unmet`/uncovered/unjustified-`partial` blocks merge) + roborev clean → `openspec archive`.
- superpowers are *techniques*; OpenSpec is the *artifact system* — the proposal/design/tasks ARE
  the plan. See [spec-driven audit](https://pmcfadin.github.io/cqlite/agents-developing/spec-driven-audit/).

### Wiring evidence
A feature is done only when its public surface exercises it — a named surface + call chain + an
end-to-end test. Green helper-only unit tests are not sufficient.

### Delivery pipeline (flow-lead)
- **`flow-lead`** orchestrates (opt-in: `claude --agent flow-lead`; plain sessions are default Claude) — it spawns
  and sequences the specialists + roborev + the gate, and writes no production code. Verbs:
  `flow-groom` → `flow-activate` (**Seam 1**: owner approves spec + design) → `flow-implement` (the
  implement loop above) → `flow-address` → `flow-finalize`; `flow-board` = status + the single next
  thing. Doctrine: [delivery pipeline](https://pmcfadin.github.io/cqlite/agents-developing/delivery-pipeline/).
- **1:1:1:1**: one issue ↔ one worktree/branch `issue-<N>-<slug>` (branched from `origin/main`) ↔
  one OpenSpec change `<slug>` ↔ one PR. Worktrees lack gitignored Data.db binaries — point
  `CQLITE_DATASETS_ROOT` at the root the fetch's printed export line names (often machine-local, e.g.
  `/data/datasets`), which is not necessarily the main repo's `test-data/datasets`. The committed CQL
  schemas need no env var: they resolve from the worktree's own checkout (#3148).
- **Board = sole dispatch authority (Path A, #1886)**: the GitHub Project `Status` field
  (`Backlog/Ready/In Progress/In Review/Done`); exactly one `P0`–`P3` per issue. New issues auto-land
  at `Backlog`. Empty Ready column = no work ready → STOP. Board unreachable (auth/scope) → STOP and
  fix auth; never label-dispatch.
- **How to READ the board — always `--query`, never an unfiltered page (#3055)**: the fresh board read
  the claim protocol requires is a **server-side filtered** `item-list`. This board is 900+ items, and
  an UNFILTERED `gh project item-list` **silently truncates** at the page limit — it returns a partial
  column with no error, which has produced wrong "nothing is Ready" / "issue not on board" reads.
  Filtered, it is exact, ~1.6 s, and cheaper than the GraphQL `projectItems` path:

  ```bash
  gh project item-list 1 --owner pmcfadin --query "status:Ready"         --format json -L 100 \
    --jq '.items[]|"\(.content.number)\t\(.content.title)"'
  gh project item-list 1 --owner pmcfadin --query 'status:"In Progress"' --format json -L 100
  gh project item-list 1 --owner pmcfadin --query 'status:"In Review"'   --format json -L 100
  ```

  `--query` takes GitHub Projects filter syntax (`-status:Done`, `assignee:<login>`, combinations);
  quote multi-word option names. Do NOT reach for GraphQL to work around truncation — filter instead.
  Corollary: a board read and the `status:*` labels **will disagree** by design (below) — when they do,
  the filtered board read wins, always.
- **`status:*` labels = an ENFORCED read-mirror of board Status, for DISCOVERY only (#2855)**: the
  `project-board-sync.yml` workflow is the *single writer*, deriving each OPEN issue's label from its
  board Status (Ready→`status:ready`, In Progress→`status:in-progress`, In Review→`status:in-review`,
  Backlog/Done→none) on the 30-min sweep + on issue events, and a drift-detector FAILs the run on any
  disagreement. So the label is now *trustworthy* for **cheap server-side candidate discovery**
  (`gh issue list --state open --label status:ready --json number,title` — no issue bodies, no board
  pagination). It is NEVER the dispatch/claim authority: it is eventually-consistent (≤30-min lag), so
  it only NARROWS candidates — the claim ref + a fresh board read at claim time remain the sole
  double-work arbiter. **The lag is real and routinely bites**: measured 2026-07-27, the label said
  `status:ready` for three issues the board had at In Progress / In Review / In Review, while two
  freshly-promoted P0s had no label yet — so a label-only read simultaneously offered work already
  three stages in AND hid the two highest-priority items. Reporting board state from labels is a
  correctness bug, not a shortcut. flow-* skills no longer write the board-derived labels (they set
  board Status only; the mirror follows); `status:spec-review`/`status:addressing` stay transient skill-managed
  sub-markers the mirror does not touch.
- **Claim protocol (cross-machine, #2665)**: THE lock is the slugless fixed-name ref
  `refs/claims/issue-<N>`, acquired via `bash scripts/flow/claim.sh claim <N>` — an atomic unique
  root-commit push that git arbitrates server-side, so a model-chosen slug or an identical-SHA base
  can no longer double-claim (the #1632 slug-pair + identical-SHA-no-op hazards are closed). The
  `issue-<N>-<slug>` branch is now **PR plumbing, NOT the lock**. Acquire the claim ref FIRST, then
  worktree+branch; set assignee + `Status=In Progress`. `claim.sh verify <N>` confirms you hold it;
  adopting a reaped claim = `claim.sh adopt <N> --expect <old-sha>` (compare-and-swap, so a
  resurrected original holder loses the lease immediately — #2467/#2499); **resuming an issue whose
  `issue-<N>-*` branch outlived its claim ref** (released/reaped/parked claim, or a
  merged-but-undeleted branch) =
  `claim.sh adopt <N> --expect none --reason resume-legacy-branch-lock:branch-outlived-claim` (#2945) —
  git's empty lease, so the create is still server-arbitrated (a machine actually holding the ref keeps
  it, `ADOPT-LOST`) and the claim commit records who took it AND why (a `--reason` with nothing
  recordable in it, a bare placeholder like `why`/`todo`/`tbd`, or one still carrying an
  **unsubstituted `<…>`** — a copied template such as `--reason resume-legacy-branch-lock:<branch>` —
  is a usage error, not a silent `reason=unspecified`/`reason=why`; `--actor` is fail-closed the same
  way, since an unrecordable actor would alias two identities onto one holder). That is the ONLY sanctioned
  way past `reason=legacy-branch-lock`; never hand-craft a claim commit. It is deliberately **NOT
  auto-advertised**: the refusal DIAGNOSES the lane (`reason=legacy-branch-lock detail=<branches>
  claim-ref=free resume=documented-procedure`) and points here, but prints **no runnable command** —
  a printed line gets executed literally, and an older-fleet worker holds only the BRANCH (so the
  empty-lease adopt WOULD succeed against a live lane). Before resuming, CONFIRM the lane is
  abandoned with the same test `flow-board`'s reaper uses — `claim-heartbeat.sh should-reap
  <machine>` (age > 4h AND no open PR AND pid-dead-if-local) plus board `Status` and the branch/PR
  author. `claim.sh release <N>`
  deletes the ref (refuses under an open PR without `--force`). Maintain the liveness heartbeat
  (`scripts/flow/claim-heartbeat.sh beat <N>`, refreshed at claim + every stage transition);
  `flow-board` reaps deterministically (age > 4h AND no open PR) (#2089).
  **The lock is a plain `git push`, so git — not just `gh` — must be authenticated (#2942).** They
  are separate credential paths: an authenticated `gh` with an unwired git fails every claim with
  `fatal: could not read Username`, and `claim.sh` now calls that `ERROR reason=auth (NOT
  retryable)` instead of the old misleading `reason=infra (transient — retry)` — do not retry it,
  fix the box (`gh auth setup-git`, or `bash scripts/bootstrap-agent-machine.sh --yes`, which also
  probes board access functionally rather than trusting the `project` scope string). The three
  worker-environment deltas and the messages that identify them: `docs/development/fleet-runbook.md`.
- **Supervisor-authored machine claim + CI reaper (#2655/#2499)**: liveness is now MECHANISM-driven,
  not prose. `worker-supervisor.sh` stamps `refs/machine-claims/<machine>` (issue+supervisor-PID+ts)
  via `claim-heartbeat.sh stamp` at every spawn, refreshes it each iteration, and clears it on a
  clean exit (`reap`, which REFUSES when the issue still has an open PR — an unfinished endgame stays
  owned for adoption, never orphaned). This namespace is distinct from `claim.sh`'s per-issue lock
  `refs/claims/issue-<N>`. `claim-heartbeat.sh should-reap <machine> [secs]` is the single, fail-safe
  reap predicate (exit 0 = reap, 1 = keep, 2 = no ref): reap ONLY on age > threshold (4h) AND no open
  PR AND (pid-dead, when the claim is local — a foreign machine's PID is unknowable). It KEEPS on a
  fresh ref, an open PR, a live local PID, or an unparseable age; a `gh`/network hiccup in the
  open-PR probe assumes an open PR (keeps). The `project-board-sync` 30-min cron runs a `reap-claims`
  job that applies this predicate server-side and flips a freed board item back to Ready with a
  traceable comment. **`PROJECTS_TOKEN` absence now FAILS the workflow loudly (`::error::`)** — a
  persistent red run is the alert, replacing the old silent green `::notice::` no-op. The scheduled
  board sweep only backlogs a null-status issue once it is past a 10-min auto-add grace window, so it
  no longer races the built-in Auto-add's default-status write.
- **One worker per machine (#1930)**: one lead/worker session owns a box; it fans out subagents but
  keeps to **one full gate at a time** — enforced mechanically (#2640): `bootstrap-agent-machine.sh`
  pins `CQLITE_GATE_MAX_CONCURRENCY=1` (the #1825 cap admits one gate; the per-gate core budget then
  gives it full cores), and every gate derives `CARGO_BUILD_JOBS` + nextest `--test-threads` from its
  slot count and runs under `taskpolicy -c utility`/`nice`, so no manual `pgrep`-serialization is
  needed. It pre-claims by checking the `refs/claims/issue-<N>` ref (`claim.sh status <N>`) AND any legacy
  `issue-<N>-*` branch. Multiple independent sessions → separate
  machines, each claim-protocol-gated; NEVER N bare leads without the protocol. Unattended runs:
  `scripts/local/worker-supervisor.sh` (#2090) recycles ONE worker process per issue (hard context
  bound = process exit; the worker writes `.worker-last-iteration.json` then EXITs — never a second
  issue per session), with flock single-instance + preflight + crash-loop breaker + budgets + ntfy
  (`docs/development/fleet-runbook.md`).
- **Park-and-resume — never block on a question unattended (#2666)**: `AskUserQuestion` (and any
  interactive prompt) is **attended-sessions-only**. In an unattended worker session, hitting Seam 1 (an
  unapproved spec) or a genuine mid-run owner decision is NOT a wait — the worker **parks**: post ONE
  structured question comment (options + recommendation + default), add the `needs-decision` label, write
  a `blocked` marker with `reason: seam1-approval|needs-decision` (+ optional one-line `question`), and
  EXIT, releasing the machine. The supervisor judges this `parked-on-owner` (never toward the crash
  breaker), pages the owner once, and moves to the next Ready issue; a stuck-on-a-prompt worker is detected
  mid-iteration (log-tail watchdog) and paged as `stuck-on-question`, also never toward the breaker. A
  `needs-decision` issue resumes only on a strictly-newer owner reply (worker reads the answer, clears the
  label); a durable `resume-dont-ask` label is a standing Seam-1 seal `flow-implement` honors in place of asking.
- **Inter-issue reset (#2085)**: after each `flow-finalize` the lead drops ALL prior-issue context
  (board renders, gate summaries, roborev findings, PR bodies, Seam-1 spec renders) and re-hydrates
  the next item from **board + disk alone**. Seam-1 spec bodies are not retained after approval —
  `spec-auditor` re-reads them from `openspec/changes/<slug>/`. Durable lessons → `MEMORY.md` /
  `process_improvements.md`, never the live window.
- Spawn subagents with an explicit accessible model (e.g. opus).
- **Telemetry**: `flow-finalize` stamps one record per delivery cycle (issue, pr) into
  `docs/reports/delivery-telemetry.jsonl` (schema `docs/reports/delivery-telemetry.schema.json`)
  via `scripts/delivery-telemetry.py record` — a reopened issue that ships more than once
  legitimately gets one record per shipped PR, so retro aggregation by issue treats such
  multi-cycle issues as multiple deliveries, not one (issue #2314). Records hold authoritative
  data only (a counter not observed is an error, never a fabricated 0). On a cadence the manager
  runs `retro` and files a deduped `flow-meta` issue. The SKIP-aware `delivery-telemetry` gate
  component covers the tool. Doctrine: `docs/development/pm-operating-loop.md`.
  - **Stamp via a PR-in-worktree, never a direct push (#2433 branch protection).** `main` blocks
    direct pushes (PR required for every commit, `enforce_admins=true`), so the ledger line CANNOT be
    pushed to `main` directly. `flow-finalize`/`flow-closer` stamp by: (1) `git worktree add` a
    `telemetry-<N>` branch off `origin/main` — **never `git checkout` in the shared root** (a closer
    that switched root to a `telemetry-*` branch and died stranded root off `main`, breaking every
    session); (2) `scripts/delivery-telemetry.py record` — note it writes to the SCRIPT's repo ledger
    (root checkout), NOT `$PWD`, so move/verify the line lands in the telemetry worktree's ledger and
    leave root clean; (3) commit + push the branch + open a telemetry-only PR that merges once its own
    `required` check is green. The ledger is a hot append-only file: on a rebase conflict, **keep ALL
    lines** (main's ledger + your new record), never drop a peer's line. Do NOT block the code merge on
    the telemetry PR — return its number as residual if its CI is still pending.
- **Keep doctrine current in the same change** — user-facing or workflow changes update CLAUDE.md
  and the website `agents-developing/` page as part of the change.
  - **Acceptance step: a publish is verified by the NEW CONTENT being served, never by HTTP 200
    (#3042).** A green deploy plus a `200` proves the site is up, not that your change is live: the CDN
    can keep serving the **previous** page for roughly **3 minutes** afterward (observed twice — two
    successive `curl`s returned stale content after a successful deploy). Grep the response for a
    distinctive string your change introduced, and re-check after a wait if it is absent:
    ```bash
    curl -sS https://pmcfadin.github.io/cqlite/agents-developing/<page>/ | grep -c '<new phrase>'
    ```
    A `0` means not-yet-published (or not published) — not a failure to report immediately, but never
    bank it as done. For a NEW SSTable-guide chapter there is a second, separate requirement: it must
    be registered in `CHAPTERS` (`docs/sstables-definitive-guide/README.md`).

## Product-Manager Behavior (lead)

- The lead acts as product manager: track epics and issues, prioritize, keep work moving.
- **Autonomy — arm `--auto`, GitHub merges on green (default, #2667)**: the moment **local
  certification** is met — local gate PASS + **C** PASS (design-driven) + roborev clean — workers (and
  the lead) **arm auto-merge on their own PR** via `gh pr merge --auto --squash --delete-branch`
  (after the pre-merge SHA assert + `HOLD` re-read), then finalize. GitHub owns the CI-green wait and
  lands the PR the instant the `required` check passes — **never `ScheduleWakeup`-poll a PR's own CI**.
  Branch protection enforces the `required` check for admins too (`enforce_admins`), so `--auto` can
  never land against an unchecked head and bypass is impossible; a known-flake red gets
  `gh run rerun --failed`, never a bypass. This enforcement is load-bearing: if branch-protection
  settings change, this doc governs catching it (#2433). **`gh pr merge --auto` is the ONLY sanctioned
  merge — REST `PUT repos/OWNER/REPO/pulls/N/merge` is ABSOLUTELY FORBIDDEN (#3055)**: it merges
  *immediately*, bypassing the required-check wait branch protection exists to enforce, so it is never a
  GraphQL-throttle fallback. `--auto` is set-once/idempotent — on a throttle, **sleep and retry the same
  arm**. (The comment-post and PR-create REST fallbacks remain fine; only merge is forbidden.) **What a green `required` now covers
  (#2910)**: `required` is no longer only its own steps — it also polls the PR head's sibling check
  runs and **fails closed** on any tier declared in `.github/ci-gating-tiers.yml` that is failed,
  still pending at the aggregation deadline (60 min default), or **absent** (absence is an error,
  never "not applicable" — a registered tier always emits its context, reporting inapplicability as
  an explicit success). So arming `--auto` before the tiers finish stays correct: GitHub releases the
  merge on `required` going green, and `required` cannot go green until every registered tier has
  reported success. A **diff that mandates a tier runs it with or without the tier's `ci:*` label**,
  so **no step of the flow asks a worker to decide which tiers are out of band or to apply a label**.
  Adding a `pull_request` workflow without enrolling it in the registry (as a tier or an
  annotated exemption) reds `required`. Residual: a tier re-run **after** `required` is already green
  cannot be retracted by a finished job — **re-run the tier, then re-run `required`**, in that order.
  Break-glass is per-tier only (`ci:waive:<tier-id>`, owner action) and can excuse an absent or
  pending tier, **never** a failed one — applying it takes effect **without a re-run** (the
  aggregator re-reads live labels each poll) and **without restarting `pr-gate-core`** (label events
  queue rather than cancel, and skip the core, reusing the result already recorded for that head
  sha). A waiver is **bound to the head sha it was applied for**: a label survives a push, so after
  you push again it no longer short-circuits — the tier is polled and a failure it reports still reds
  the gate; **remove and re-apply the label** to waive the new head. Two further properties worth knowing: `required` evaluates the aggregator **and the registry
  from the PR's BASE ref**, so a registry/aggregator change lands only after it merges (rename a
  tier's context in a separate PR, or waive it) — the **same shape** as roborev reading
  `exclude_patterns` from the repo **root path** and snapshotting it at daemon start (#3229, above);
  generalized, **any PR whose subject is a config a daemon or gate reads from root cannot certify
  itself**, so plan its demonstration for after the merge; and a tier's mandate covers everything that reaches
  it at runtime — for Flight that includes `cqlite-core/**`, `test-data/**` and the Cargo manifests,
  so core-touching PRs run the Flight e2e tier. Finalize runs in-session when the required
  check is already green at arm time, else on a later wake confirming `state=MERGED`. Do NOT
  wait for the owner. Seam 1
  (spec approval) is the only standing human gate. Escalate and **hold the merge** ONLY for: a
  genuine design-call roborev finding, a scope/product question, an unmet/uncovered requirement, or
  work outside the issue — and obey any `HOLD: merge after #N` order.
- Autonomous GitHub writes within limits: comments; status labels; assign/reassign. Closing a
  fully-done non-epic issue with a merged linked PR (+ closing comment) is allowed.
- Never close an epic, change an issue's scope/title, or make a product decision (ambiguous scope,
  conflicting requirements, tradeoffs) without the owner — collect under a "NEEDS YOU" list.
- Every issue/PR number carries a brief description (`#1081 (multicell UDT)`, never bare `#1081`).
- Make every write traceable with a short comment.
