# CLAUDE.md

Guidance for Claude Code when working with CQLite. This file is loaded into every agent context —
it holds the **rules and pointers**; recipes and examples live in `docs/development/dev-cookbook.md`.

## Project Overview

CQLite is a Rust library for local Apache Cassandra SSTable access — it reads (and writes)
Cassandra 5.0 data files without cluster dependencies.

**Status** (Sep 2026): **v0.17.0 released 2026-09-08**; **0.18 in progress = the SSTable toolbox
release** — the whole of epic #4192 (offline detect / salvage / repair / fix / move / diagnose tools
for damaged SSTables), plus raw SSTable view #4222 and Vortex export #4237. Shipped toward 0.18
so far: `cqlite salvage` (#4196), the macOS gate fixes (#4215), and the arrow 59 / DataFusion 55.1
rev (#4236, which raised the Rust floor to 1.95). Milestone 0.19 is memtable freshness. Earlier
releases delivered core reading, CLI, output writers, Python + Node bindings, write support + STCS
compaction with byte-for-byte parity vs Cassandra, Arrow Flight + the Trino connector, BTI (`da`)
read/write, delta-export, and the CommitLog reader. Later: M6 (WASM bindings), M7 (perf validation
+ v1.0).

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
| **Full** — the gate of record | `scripts/agent-gate.sh` | ONCE per issue, immediately pre-merge, inside `flow-closer`, **on Linux** (see *Gate host* below). Runs the whole component set: fmt, clippy `-D warnings`, core/integration/write/CLI tests, the feature-matrix and binding lanes, `oom-audit`, `pub-surface`, `features-load-bearing`, advisory `dep-duplicates`, `all-features-check`, minimal build, smoke. Tests run at the TARGET granularity each component names, never whole packages — several crates' tests execute nowhere (#3522; record: `scripts/tests/workspace-test-disposition.txt`). Per-component contracts: `docs/development/gate-ops.md` § *Component contracts*. Emits `AGENT-GATE SUMMARY`. |
| **Lite** (#1821 — cost is a FUNCTION of the diff; see the measured bands) | `scripts/agent-gate.sh --lite` | EVERY fix round. file-size + fmt + clippy + roborev-lints + diff-scoped tests. **Cost is NOT proportional to the diff**: clippy is the full per-package matrix every time (a no-op warm, 16–24 min cold), and a `cqlite-core/src/` diff also compile-checks every direct dependent's test targets (median ~20 min, much more under peer load). A narrow warm diff is the ~1.4 min floor, not the norm. `--lite` is exempt from the gate-slot cap and has no admission check yet (#3763). Mechanism + measured bands: `docs/development/gate-ops.md`. Emits a DISTINCT `AGENT-GATE LITE SUMMARY` (MODE: lite) — can NEVER be pasted as the full SUMMARY. |
| **Delta** (#1892) | `scripts/agent-gate.sh --delta <anchor-sha> --anchor-run-id <id>` (or `--anchor-summary-file <path>`) | Re-certify a post-full-PASS polish round whose diff is ONLY executable tests/docs (rust test code, python/node binding tests against an already-built module, `scripts/tests/*.sh`, `*.md`; #2081). FAILs CLOSED on anything else (src, scripts, workflows, `Cargo.*`, config, test-data, unbuilt node module) — never builds, never passes vacuously. Emits a DISTINCT `AGENT-GATE DELTA SUMMARY` naming the anchor + a `delta-executors:` line; record BOTH it AND the anchor's full SUMMARY in the PR. NOT the gate of record. |

**Required invocation — summary-file redirect, never raw stdout, full AND lite:**

```bash
AGENT_GATE_SUMMARY_FILE=/tmp/gate-summary.txt bash scripts/agent-gate.sh > gate.log 2>&1 < /dev/null
cat /tmp/gate-summary.txt   # the SUMMARY block is the ONLY gate text an agent retains
```

Read the summary file, not `gate.log`. Prefer `run_in_background` (or a long timeout) so a subagent
never idle-waits into a watchdog kill. A queued gate ≠ a hung gate.

The merge-blocking rules — each one FAILs closed, and the full mechanism for every one of them is in
`docs/development/gate-ops.md`:

- **Completion and verdict are two assertions.** Probe completion with the terminal-token grammar
  for the mode you ran — full, `--lite`/`--only`, and `--delta` each have their own, published ONLY
  in `docs/development/gate-ops.md` (this file deliberately quotes none of them, so it cannot teach
  a partial set). `INCOMPLETE` is a liveness placeholder written at launch, not a verdict. And read
  a **component's OWN line**, never the terminal token — `PARTIAL` says the *run* was partial, not
  that your component failed.
- **A gate script behind `origin/main` cannot certify.** Rebase before the gate of record.
- **A worktree that mutates mid-run cannot certify.** Verify `tree-integrity:` and `dirty: no`
  alongside `RESULT:`, and verify the `run-id:` matches the run you launched — a foreign `run-id`
  block is a peer's, even at `PASS`.
- **Run it detached from your session's cgroup**, or it dies with the session.
- **A genuinely prose diff cannot change the compiled binary**, so a test failure in its gate is
  pre-existing or a flake. Don't judge the path shape — run the classifier, and cite an issue:
  ```bash
  git diff --name-only origin/main...HEAD | bash scripts/ci/classify-docs-only.sh   # exit 0 = prose
  ```
  Never patch source to turn such a gate green.
- **`--only` is a diagnostic, NEVER the gate of record.** It is lenient by construction.
- **Compiling a feature is not covering it**; every component line names the feature matrix it ran.
- **Affirmative zero**: a census reports `0 RECOGNISED`, never a bare `0` — an unmeasured check and a
  clean one must not read alike.

### Gate host: Linux certifies, macOS never does (#4225)

**Owner ruling, 2026-09-13: the gate of record runs on Linux.** macOS is a development host —
`--lite`, `--only` and targeted `cargo test` runs are fine there, but a full-gate SUMMARY produced on
macOS never certifies a merge, even at `RESULT: PASS`. Why: a 110-file tooling self-test sweep went
36/110 non-PASS on Darwin, and several of those are Linux-only by construction (`/proc`-based lane
locks, `flock(1)`, the `systemd-run` cgroup detach that `scripts/flow/gate-detached.sh` requires —
it exits 69 on a host without it). **Nothing enforces this mechanically**: `agent-gate.sh` will
print a full SUMMARY on a Mac. Closers and leads check the host before they cite a run. Route a gate
of record to a fleet Linux box (`docs/development/fleet-runbook.md`, the `remote-gate` skill).

Self-test: `bash scripts/tests/test_agent_gate_summary.sh`.

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
tools/           # 7 crates, each with a RECORDED disposition (WIRED / UNWIRED / MIXED) pinned by
                 #   scripts/tests/test_tools_crate_disposition.sh (#1716). A NEW tools/ crate
                 #   must be classified there or the gate FAILs. The label says whether
                 #   something INVOKES the crate, not whether its tests run — most don't (#3522).
                 #   Details: docs/development/gate-ops.md § tools/ crates.
fuzz/            # cargo-fuzz crate — own workspace, EXCLUDED from the main one
```

**Do not add `default-members` (#1716).** This workspace has a root package, so a bare
`cargo build` already builds only `cqlite`; an explicit list would *expand* it to every member.

**Expect latent test failures the first time you touch a long-unwired crate.** No gate component
runs workspace-wide tests, so most `tools/` crates' tests run only when `--lite` maps your diff to
that package. Failures it surfaces are pre-existing, but your diff is what runs them, so they are
yours to fix. Details: `docs/development/gate-ops.md` § *tools/ crates*.

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

### Crate root must tell the truth (`cqlite-core`, issue #1712)
The full gate's `pub-surface` component checks one thing: a top-level `pub mod NAME;` in
`cqlite-core/src/lib.rs` must not be switched off by an inner `#![cfg(...)]` in `NAME`'s own file.
If it is, hoist the cfg to the declaration. **It does not detect public-API changes — nothing in
this repo does** (the principled route is #3366). Contract: `docs/development/gate-ops.md`.

### Code quality
- `RUSTFLAGS="-D warnings"` must pass; no `unwrap()`/`expect()` in library code; `thiserror` for errors
- Memory target: <128MB for large files

### File size (campsite rule)
Keep files small — agentic context cost scales with file size. Targets (total lines, inline tests
included): source `~800`, test files `~1500`. The gate's `file-size` ratchet FAILs if your change
grows an over-threshold `.rs` file (or pushes one over). Touching an over-threshold file → split it
by responsibility (source: epic #1116; tests: #1135). Genuinely out of scope → re-run with
`CQLITE_ALLOW_FILE_GROWTH=1` and leave a note linking #1116/#1135. The override is visible: the component reports `OPT-OUT`, not `PASS`, and only for the value
exactly `1` (any other value still FAILs). File names go to `file-size.log`, never the SUMMARY row
(#3402). Contract: `docs/development/gate-ops.md`.

### Testing
- Integration tests use real SSTable data only; validate against `sstabledump` output via JSONL
  reference files —
  [validation playbook](https://pmcfadin.github.io/cqlite/agents-developing/validation-playbook/)
- Never let a dataset-dependent test pass on an empty dataset (0-rows-when-present = failure)
- **Resolve fixture roots per TABLE, assert per CASE (#3220).** Use
  `cqlite-core/tests/support/datasets_root.rs::sstables_root_for_table`; never pick a root by
  keyspace or by a fixed env-first/checkout-first order, and never end a corpus loop with a
  suite-wide `assert!(ran > 0)` — committed fixtures are `must_run`, fail-closed per case.
- **Pick the oracle that can see your defect.** Each one below is green on some real bug:
  1. *Physical-dump parity* (sstabledump JSONL) keeps shadowed rows, so it can't catch a read-time
     reconciliation bug. For `SELECT` correctness use *query-semantics parity*
     (`test-data/query-semantics-oracle.json`, pinned `now`) (#1742). The point-vs-full
     differential (`point_vs_full_differential.rs`) covers CQLite-vs-CQLite read paths (#1918).
  2. *CQLite-written + CQLite-read round trips* can't see a framing error both sides share. For any
     on-disk encoding property, the oracle is Cassandra-written bytes or Cassandra source (#3042,
     #3002).
  3. *Per-surface oracles* can each pass while Python, Node and the CLI disagree. The cross-surface
     differential (`bindings/parity/`) runs in CI only and is not merge-gating (#1455).
  4. *Column-subset comparisons* can't see a truncated point row. Point/seek-vs-scan tests use
     `SELECT *` and assert the column set in both directions (#3890).
  Long form, with every declared gap: `docs/development/test-oracles.md`.

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
`experimental` (gates `Database::flush()`/`compact()`, the INSERT executor path, bloom-filter tests
(#65), and the unimplemented `Storage::put`/`delete` stubs (#175)). Build recipes:
`docs/development/dev-cookbook.md`.

**Every declared feature must be LOAD-BEARING (#1698)** — the full gate's
`features-load-bearing` component FAILs on a feature that changes nothing (no cfg site, optional
dep, dep feature, or `required-features` entry in its closure). Being listed in a clippy feature
list, a workflow or a doc table is not an effect, so deleting a dead flag means removing those
mentions in the same diff. No bypass. Contract: `docs/development/gate-ops.md`.

## Troubleshooting

- **Missing test data / 0 rows**: `bash test-data/scripts/fetch-datasets.sh`, then export the
  `CQLITE_DATASETS_ROOT=` line it prints — NOT `$PWD/test-data/datasets`, which on a fleet box is a
  corpus-less root the fetch never populates. `--verify-only` re-checks an existing root
  non-destructively. No `schemas` sibling is needed (#3131).
- **Clippy failures**: run with `RUSTFLAGS="-D warnings"` to match CI
- **Parsing issues**: `docs/sstables-definitive-guide/chapters/appendix-f-known-limitations.md`
- **Python bindings**: Rust 1.95+, Python 3.9+, `pip install maturin`, then
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
  → flow-closer { rebase → FULL gate ONCE → C → ROBOREV LAST → premerge-assert → arm → finalize }
```

- **ROBOREV LAST, and a later rebase VOIDS the roborev round.** Endgame order is **rebase → gate of
  record → C → roborev → `premerge-assert` → arm**. A roborev round changes no bytes, so reviewing
  after gating is free; a rebase changes bytes, so it invalidates both. If you rebase, you re-review.
- **A lead stops a merge by converting the PR to a draft**, not by comment alone.
- **Review-first**: review BEFORE the first full gate so the ONE gate certifies already-reviewed
  code. Skip only for a genuinely mechanical diff. When in doubt, review.
- **roborev — `scripts/flow/roborev-review.sh` is the ONLY sanctioned call, and it requires BOTH
  `--agent` and `--model`.** Push first; an unpushed commit is itself an empty-diff cause. Retain only
  the `==== ROBOREV REVIEW SUMMARY ====` block. Exit `0` PASS / `1` FAIL / `3` NOTHING-TO-REVIEW /
  `2` usage; **any non-PASS terminal `RESULT` is a blocked merge**, never "roborev clean". A
  code-free diff **cannot be roborev-certified at all** — the substitute is primary-source
  verification recorded in the PR. `docs/reports/*-artifacts/` harnesses are reviewed CODE. If
  `prompt-content:` FAILs, suspect `.roborev.toml` first, then **#3252** (a large diff is delivered
  by snapshot pointer and FAILs with the worktree-bug signature).
  Everything else — the four vacuous-pass traps and their token accounting, the absence waiver and
  its constraints, exclusion-set mechanics, the control/data channel lesson — is in
  `docs/development/roborev-contract.md`. Read it before your first roborev round on an issue.
- **Scoping a review (`exclude_patterns`) is a ROOT-checkout operation.** The daemon binds the repo
  via `repos.root_path`, reads that checkout's `.roborev.toml`, and snapshots it at start — editing it
  in a worktree is a silent no-op. Generalized: **any PR whose subject is a config a daemon or gate
  reads from root cannot certify itself.** Plan the demonstration for after the merge.
- **flow-closer** owns the endgame in its own disposable context and returns only a terminal packet.
  It has no `Agent` tool, so **C is spawned by the lead at the closer's `NEEDS-SPAWN` request**.
  Before arming it runs `scripts/flow/premerge-assert.sh <pr> <certified-sha>` and re-reads comments
  for a fresh `HOLD:`. Mechanism: `docs/development/pm-operating-loop.md`.
- **Severity triage** (rubric `docs/development/roborev-severity.md`): **blockers** are fixed
  pre-merge, each re-triggering `fix → --lite → re-review`. **Nits** never trigger a re-verify round —
  batch them into ONE linked follow-up at merge time. When in doubt, blocker.
- **Post-gate polish**: after a full PASS at `X`, a test/docs-only diff `X..Y` re-certifies with
  `--delta`, never a repeat full gate.
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
- **Cargo-output parses keyed on literal status text** — route through `_ansi_stripped_log`,
  read by redirection not a pipe (#3400). NOT mechanized: the lint written for this was
  descoped for an increasing false-PASS count (see the gate section above); mechanization is
  deferred to #3499, so this one is hand-checked.
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
- **`flow-lead`** orchestrates (`claude --agent flow-lead`) and writes no production code. Verbs:
  `flow-groom` → `flow-activate` (**Seam 1**: owner approves spec + design) → `flow-implement` →
  `flow-address` → `flow-finalize`; `flow-board` = status + the single next thing.
- **1:1:1:1**: one issue ↔ one worktree/branch `issue-<N>-<slug>` (branched from `origin/main`) ↔ one
  OpenSpec change ↔ one PR. Worktrees lack gitignored Data.db binaries — point `CQLITE_DATASETS_ROOT`
  at the root the fetch's printed export line names. Committed CQL schemas need no env var.
- **Board = sole dispatch authority**: the Project `Status` field; exactly one `P0`–`P3` per issue.
  Empty Ready column = STOP. Board unreachable = STOP and fix auth; never label-dispatch.
- **Read the board with `--query`, never an unfiltered page.** This board is 900+ items and an
  unfiltered `gh project item-list` **silently truncates** — a partial column with no error:
  ```bash
  gh project item-list 1 --owner pmcfadin --query "status:Ready" --format json -L 100 \
    --jq '.items[]|"\(.content.number)\t\(.content.title)"'
  ```
  `status:*` labels are an enforced read-mirror for **discovery only** and lag up to 30 min. When a
  board read and the labels disagree, the filtered board read wins, always.
- **Claim protocol**: THE lock is the per-lane claim ref, acquired via
  `bash scripts/flow/claim.sh claim <N>` — an atomic unique root-commit push git arbitrates
  server-side. The branch is PR plumbing, NOT the lock. Claim FIRST, then worktree+branch; set
  assignee + `Status=In Progress`; `claim.sh verify <N>` confirms. Maintain the heartbeat at claim and
  every stage transition. **The lock is a plain `git push`, so git — not just `gh` — must be
  authenticated**: an authenticated `gh` with unwired git fails every claim as
  `ERROR reason=auth (NOT retryable)`. Fix the box, don't retry.
- **Park, never block, when unattended.** `AskUserQuestion` is attended-sessions-only. Hitting Seam 1
  or a real owner decision means: post ONE structured question, add `needs-decision`, write a
  `blocked` marker, and EXIT, releasing the machine. A `resume-dont-ask` label is a standing Seam-1
  seal.
- **Inter-issue reset**: after each `flow-finalize` drop ALL prior-issue context and re-hydrate from
  **board + disk alone**. Durable lessons go to memory, never the live window.
- Spawn subagents with an explicit accessible model.
- **Telemetry**: `flow-finalize` stamps one record per shipped PR into
  `docs/reports/delivery-telemetry.jsonl` via `scripts/delivery-telemetry.py record`. Authoritative
  data only — a counter not observed is an error, never a fabricated 0. **Stamp via a
  PR-in-worktree, never a direct push, and never `git checkout` in the shared root.** The ledger is
  append-only: on a rebase conflict **keep ALL lines**.

Lease semantics, the reap predicate, adoption, the supervisor machine claim, the CI reaper and the
worker-environment deltas are in `docs/development/fleet-runbook.md`.

## Product-Manager Behavior (lead)

- The lead acts as product manager: track epics and issues, prioritize, keep work moving.
- **Autonomy — arm `--auto`, GitHub merges on green (default).** The moment local certification is met
  — gate PASS + **C** PASS (design-driven) + roborev clean, or for a code-free diff the recorded
  primary-source substitute — arm `gh pr merge --auto --squash --delete-branch` (after
  `premerge-assert` and a `HOLD` re-read), then finalize. GitHub owns the CI-green wait; **never
  `ScheduleWakeup`-poll a PR's own CI**. **`gh pr merge --auto` is the ONLY sanctioned merge** — REST
  `PUT .../pulls/N/merge` merges immediately, bypassing the required-check wait, so it is never a
  throttle fallback (on a throttle, retry the same idempotent arm). Seam 1 is the only standing human
  gate; do not wait for the owner otherwise. What `required` aggregates, the
  `.github/ci-gating-tiers.yml` registry and the per-tier `ci:waive:<tier-id>` break-glass are in
  `docs/development/merge-gate.md`.
- Escalate and **hold the merge** ONLY for: a design-call roborev finding, a scope/product question,
  an unmet requirement, or work outside the issue — and obey any `HOLD: merge after #N`.
- Autonomous GitHub writes within limits: comments; status labels; assign/reassign. Closing a
  fully-done non-epic issue with a merged linked PR (+ closing comment) is allowed.
- Never close an epic, change an issue's scope/title, or make a product decision (ambiguous scope,
  conflicting requirements, tradeoffs) without the owner — collect under a "NEEDS YOU" list.
- Every issue/PR number carries a brief description (`#1081 (multicell UDT)`, never bare `#1081`).
- Make every write traceable with a short comment.
