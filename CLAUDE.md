# CLAUDE.md

Guidance for Claude Code working on CQLite. It holds the rules and pointers; recipes live in
`docs/development/dev-cookbook.md`, operator detail in `docs/development/` (linked below).
Each rule says what, why, how it is checked, and one issue to read.

## Project overview

CQLite is a Rust library for local Apache Cassandra SSTable access: it reads (and writes) Cassandra 5.0
data files without a cluster. Single Linux dev box; the multi-machine fleet was retired 2026-11-01.

**Status** (Nov 2026): v0.18.0 shipped as the SSTable toolbox release (`cqlite verify`/`sweep` #4194,
`salvage` #4196, `rebuild` #4197, raw SSTable view #4222, arrow 59 / DataFusion 55.1 with Rust floor 1.95
#4236). Milestone 0.19: the rest of epic #4192 (explain / audit / scrub / diff / diagnose), Vortex export
#4237, memtable freshness. Later: M6 (WASM bindings), M7 (perf validation, v1.0).

## Where things are

- SSTable format, single source of truth: `docs/sstables-definitive-guide/README.md` (Ch.5 Data.db, Ch.6
  Index/Summary, Ch.17 BTI, App.B encodings, App.F known limitations).
- Agent doctrine: https://pmcfadin.github.io/cqlite/agents-developing/ (gate contract, no-heuristics, test
  data, source map, validation playbook, format debugging, spec-driven audit, delivery pipeline, roborev
  findings).
- Operator docs in `docs/development/`: `gate-ops.md` (gate modes, host, component contracts),
  `roborev-contract.md`, `roborev-severity.md`, `pm-operating-loop.md` (pipeline, claim/board, review and PM
  rules), `merge-gate.md`, `dev-cookbook.md` (CLI, bindings, write support, profiling, feature builds, fuzz),
  `test-oracles.md`, `ci-toolchain-policy.md`, `parity-ci-tiers.md`, `parity-release-checklist.md`.
- Committed derived artifact: `docs/reports/cassandra-test-parity.md` (from
  `test-data/cassandra-parity-manifest.yml`; the `parity-report` gate component catches staleness).
- History: `docs/archive/issues/INDEX.md`, `test-data/validation-matrix.md`. The retired fleet
  (`docs/development/fleet-runbook.md`) is legacy; do not route work there.

## Workspace

```
cqlite-core/     # library: SSTable parsing, query engine, write path, storage/commitlog/ (Cassandra's CommitLog)
cqlite-cli/      # CLI
bindings/python/ # PyO3      bindings/node/ # napi-rs      (bindings/wasm/ planned, M6)
test-data/       # real Cassandra 5.0 SSTables + JSONL oracles
tools/           # 7 crates, each with a recorded disposition pinned by scripts/tests/test_tools_crate_disposition.sh (#1716)
fuzz/            # cargo-fuzz crate, own workspace, never built by the gate (#1614)
```

Source map: the website `source-map` page. Do not add `default-members` (#1716; see gate-ops.md).

## Core commands

```bash
cargo build
cargo test --package cqlite-core            # needs CQLITE_DATASETS_ROOT, see Test data
env RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features   # CI mode
cargo fmt
bash test-data/scripts/smoke-test-all-tables.sh
bash test-data/scripts/fetch-datasets.sh    # prints the export line to use
bash test-data/scripts/fetch-datasets.sh --verify-only   # probes a root, mutates nothing
```

Python bindings: Rust 1.95+, Python 3.9+, `pip install maturin`, then
`cd bindings/python && maturin develop --profile dev`. Everything else: `dev-cookbook.md`.

## The agent gate

`scripts/agent-gate.sh` is the pre-PR gate; its `==== AGENT-GATE SUMMARY ====` block is the verdict and
ad-hoc cargo runs never count. The local full gate is the gate of record for now (a CI coverage audit is
pending; do not claim CI is the gate of record). Modes (`--lite` each fix round, full once, `--delta`,
`--recertify`, `--only` as a diagnostic) and component contracts are in `docs/development/gate-ops.md`.

```bash
AGENT_GATE_SUMMARY_FILE=/tmp/gate-summary.txt bash scripts/agent-gate.sh > gate.log 2>&1 < /dev/null
cat /tmp/gate-summary.txt   # the SUMMARY block is the only gate text to retain
```

Read the summary file, not `gate.log`; keep it outside the worktree (#4171); run in the background so the
session isn't killed waiting. A queued gate is not a hung gate.

- **Full gate once, on Linux.** Run it once per issue right before merge. macOS is for `--lite`/targeted
  runs and its full summary never certifies (#4225). Hand-checked: check the host before citing a run.
- **Rebase first.** A gate script behind `origin/main` cannot certify. Checked by the gate.
- **Completion and verdict are separate.** Probe completion with the terminal-token grammar for the mode
  you ran (published only in gate-ops.md), then read your component's own line, never the terminal token:
  `PARTIAL` describes the run, not your component. Checked by the gate-summary self-tests.
- **Tree integrity and run-id.** A worktree that mutates mid-run cannot certify. Verify `tree-integrity:`,
  `dirty: no`, and that `run-id:` is the run you launched; a foreign block is a peer's, even at PASS.
- **Run it detached** (`scripts/flow/gate-detached.sh`) or it dies with the session cgroup.
- **Prose-only diffs.** A prose diff cannot change the binary, so a test failure there is pre-existing or
  a flake; cite an issue and never patch source to green the gate. Run
  `git diff --name-only origin/main...HEAD | bash scripts/ci/classify-docs-only.sh` (exit 0 = prose).
- **`--only` never certifies**, and compiling a feature is not covering it (#1699): component lines name
  the feature matrix they ran. A census reports `0 RECOGNISED`, never a bare `0`.

## Development standards

- **No heuristics (#28).** Authoritative metadata only: schema, else `Statistics.db`; never infer types or
  behavior from byte patterns. Legacy heuristics live behind the opt-in `legacy-heuristics` feature. Review
  catches violations. Doctrine: website `no-heuristics`.
- **Version floor.** Cassandra 5.0: `na`/`nb` BIG and `oa`/`da` BTI. Pre-`na` (3.x `ma`–`me`) is out of scope
  and is not to be added or reviewed, roborev included. `BigVersionGates::from_version` and
  `BtiVersionGates::from_version` reject the rest with `Error::UnsupportedVersion`. Do not re-litigate
  pre-`na` "regressions."
- **Uncompressed writes only (#1406).** Flush and compaction via `SSTableWriter` emit uncompressed
  SSTables and never `CompressionInfo.db`; `CompressedDataWriter`/`CompressionInfoWriter` are unwired
  (fixture synthesis, no Cassandra parity). Configuring compressed production writes returns
  `Error::UnsupportedFormat`. Never claim compressed output (manifest `claim.blocked.compressed_sstable_writes`,
  safe wording `claim.safe.uncompressed_sstable_writes`).
- **Crate root tells the truth (#1712).** A top-level `pub mod NAME;` in `cqlite-core/src/lib.rs` must not
  be switched off by an inner `#![cfg(...)]`; hoist the cfg to the declaration. The gate's `pub-surface`
  component checks only that; nothing detects public-API changes yet (#3366).
- **Code quality.** `-D warnings` passes; no `unwrap()`/`expect()` in library code; `thiserror` for errors;
  under 128 MB for large files. Checked by clippy and the `oom-audit` component.
- **File size (#1116, #1135).** Source ~800 lines, test files ~1500, because agent context cost scales
  with file size. The `file-size` ratchet fails if you grow an over-threshold `.rs` file; split it by
  responsibility. Out of scope: `CQLITE_ALLOW_FILE_GROWTH=1` (exactly `1`; reports `OPT-OUT`) plus a note
  linking #1116/#1135.
- **Every feature is load-bearing (#1698).** Declared features must change something (cfg site, optional
  dep, dep feature, `required-features`); delete dead flags and their mentions in one diff. Checked by
  `features-load-bearing`. Default features: `all-compression`, `state_machine`, `write-support`; the rest
  are in `dev-cookbook.md`.
- **Wiring evidence.** A feature is done only when a public surface exercises it end to end; helper-only
  unit tests are not enough. Hand-checked in review.
- **Format authority (#3041).** Read Cassandra at the pinned `cassandra-5.0.8` tag, never a working tree
  (a local clone may sit on `trunk` and give confidently wrong answers):
  `git show cassandra-5.0.8:src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java`
  (`$CQLITE_CASSANDRA_REPO` names a clone if a tool needs one). Authority order: pinned Cassandra source,
  `sstabledump`, the definitive guide. A CQLite `file:line` shows what CQLite does, never what is correct.
  Hand-checked in review.

## Testing

- **Real data only.** Integration tests use real SSTables validated against `sstabledump` JSONL (website
  `validation-playbook`). A dataset-dependent test must never pass on an empty dataset (0 rows when data is
  present is a failure). Hand-checked.
- **Fixture roots per table, assertions per case (#3220).** Use
  `cqlite-core/tests/support/datasets_root.rs::sstables_root_for_table`; never pick a root by keyspace or a
  fixed env-first/checkout-first order, and never end a corpus loop with a suite-wide `assert!(ran > 0)`;
  committed fixtures are `must_run`, fail-closed per case.
- **Pick the oracle that can see the defect** (each is green on some real bug; long form in `test-oracles.md`):
  1. Physical-dump parity keeps shadowed rows, so it misses read-time reconciliation bugs. Use
     query-semantics parity (`test-data/query-semantics-oracle.json`, pinned `now`) for `SELECT` correctness
     (#1742); `point_vs_full_differential.rs` covers CQLite-vs-CQLite read paths (#1918).
  2. CQLite-written and CQLite-read round trips can't see a framing error both sides share. For on-disk
     encodings the oracle is Cassandra-written bytes or Cassandra source (#3042, #3002).
  3. Per-surface oracles can pass while Python, Node and the CLI disagree. `bindings/parity/` compares them
     in CI only, not merge-gating (#1455).
  4. Column-subset comparisons miss a truncated point row. Point/seek-vs-scan tests use `SELECT *` and assert
     the column set both ways (#3890).
- **Fuzzing (#1614).** `fuzz/` needs nightly, is outside the gate; CI `fuzz.yml` runs a PR smoke and a
  nightly. Commands: `dev-cookbook.md`.

## Test data

`test-data/datasets/sstables/` holds keyspaces `test_basic`, `test_collections`, `test_timeseries`,
`test_wide_rows` (33 tables, all passing as of Dec 2025). The repo ships only JSONL references; run
`bash test-data/scripts/fetch-datasets.sh` and export the exact `CQLITE_DATASETS_ROOT=<abs>` line it prints.
That root beats any path remembered from docs. `CQLITE_DATASETS_ROOT` alone suffices (schemas are committed,
#3131); `CQLITE_SCHEMAS_ROOT` is an optional override and must be absolute. Without Data.db files, query
tests pass with 0 rows. Details (`--verify-only` outputs, why absolute): `gate-ops.md`; pins: website
`test-data`. Parsing problems: `docs/sstables-definitive-guide/chapters/appendix-f-known-limitations.md`.

## Delivery

Issue → worktree `issue-<N>-<slug>` from `origin/main` → PR. Claim and board mechanics, review rules, the
pre-roborev self-check and PM behavior: `docs/development/pm-operating-loop.md`. Skills `flow-groom` →
`flow-activate` → `flow-implement` → `flow-address` → `flow-finalize`, `flow-board` and `/drive-issue <N>`
follow it; `flow-lead` is an optional persona.

- **Commit often (#3042).** Commit after each meaningful unit, before any long or CPU-heavy step: a starved
  agent is killed by the 600 s stall watchdog and loses every uncommitted change (3 agents did in one
  session). Hand-checked.
- **Specs only for public surfaces.** OpenSpec is required for a new or changed public surface (public API,
  CLI verb/flag, binding surface, on-disk format behavior). Process, tooling, refactors and oracle-driven
  fixes are a GitHub issue with acceptance criteria (and a pinned parity test for fixes). For OpenSpec
  work the owner approves spec + design first (Seam 1); a C intent audit by `spec-auditor` follows the
  gate, and the change is archived after merge. See website `spec-driven-audit`.
- **Endgame checklist** (anyone follows it; `flow-closer` automates it): rebase → full gate of record →
  C (OpenSpec work) → roborev last → `scripts/flow/premerge-assert.sh <pr> <sha>` → arm
  `gh pr merge --auto --squash --delete-branch` → finalize. Roborev changes no bytes but a rebase does, so a
  later rebase voids the roborev round (#3752); checked by `premerge-assert`.
- **Merge only with `--auto`.** REST `PUT …/pulls/N/merge` merges immediately and bypasses the
  required-check wait, so it is never a fallback (retry the arm on a throttle). Don't poll a PR's CI.
  Obey `HOLD:` comments. Checked by branch protection and `premerge-assert`. `merge-gate.md`.
- **Roborev only via `scripts/flow/roborev-review.sh`** with both `--agent` and `--model`; push first. Any
  non-PASS terminal result blocks the merge, `NOTHING-TO-REVIEW` included. Read `roborev-contract.md`
  before your first round. Checked by the wrapper's exit code (0 PASS, 1 FAIL, 3 nothing, 2 usage).
- **Severity.** Blockers are fixed pre-merge; nits go in one linked follow-up. When in doubt, blocker
  (`roborev-severity.md`).
- **Subagents** always get an explicit accessible `model`. Roster: `.claude/agents/` (`sstable-developer`,
  `rust-reviewer`, `test-validator`, `spec-auditor`, `coverage-reviewer`, `compaction-parity-auditor`,
  `flow-closer`, `flow-lead`). Skills in `.claude/skills/` load on demand (`sstable-parsing`,
  `cql-type-system`, `rust-patterns`, `rust-skills`, `ci-cd-validation`, `test-data-management`).
- **Owner decisions.** Don't close an epic, change an issue's scope or title, or make a product call without
  the owner. Unattended sessions park (post one question, label `needs-decision`, exit) rather than block.
- Number every issue/PR with a short description (`#1081 (multicell UDT)`).
