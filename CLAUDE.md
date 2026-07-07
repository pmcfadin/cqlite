# CLAUDE.md

Guidance for Claude Code when working with CQLite. This file is loaded into every agent context —
it holds the **rules and pointers**; recipes and examples live in `docs/development/dev-cookbook.md`.

## Project Overview

CQLite is a Rust library for local Apache Cassandra SSTable access — it reads (and writes)
Cassandra 5.0 data files without cluster dependencies.

**Status**: v0.13.0 (Jul 2026) — the performance release. M1–M5 complete (core reading, CLI,
output writers, Python + Node.js bindings, write support + STCS compaction). v0.12 delivered
byte-for-byte compaction parity vs Apache Cassandra, Arrow Flight + Trino connector, canonical BTI
(`da`) write/read, CDC-style delta-export. v0.13 adds read-path speedups, byte-bounded result
budgets, `Database::refresh()`, and no-heuristics correctness fixes. Next: M6 (WASM bindings),
M7 (perf validation + v1.0).

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
`flow-finalize`, plus `flow-board` (claim board + next thing). See
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
| **Full** — the gate of record | `scripts/agent-gate.sh` | ONCE per issue, immediately pre-merge, inside `flow-closer`. fmt, clippy `-D warnings`, core/integration/write/CLI tests, minimal-features build, smoke. Emits `AGENT-GATE SUMMARY`. |
| **Lite** (#1821, ~1–5 min) | `scripts/agent-gate.sh --lite` | EVERY fix round. file-size + fmt + scoped clippy + blast-radius tests (touched package `--lib` + diff's new `--test` targets, mapped from `git diff origin/main...HEAD`; defaults to `cqlite-core --lib` when no rust package is in the diff). Emits a DISTINCT `AGENT-GATE LITE SUMMARY` (MODE: lite) — can NEVER be pasted as the full SUMMARY. |
| **Delta** (#1892) | `scripts/agent-gate.sh --delta <anchor-sha> --anchor-run-id <id>` (or `--anchor-summary-file <path>`) | Re-certify a post-full-PASS polish round whose diff is ONLY executable tests/docs (rust test code, python/node binding tests against an already-built module, `scripts/tests/*.sh`, `*.md`; #2081). FAILs CLOSED on anything else (src, scripts, workflows, `Cargo.*`, config, test-data, unbuilt node module) — never builds, never passes vacuously. Emits a DISTINCT `AGENT-GATE DELTA SUMMARY` naming the anchor + a `delta-executors:` line; record BOTH it AND the anchor's full SUMMARY in the PR. NOT the gate of record. |

**Required invocation — summary-file redirect, never raw stdout (issues #1175/#2079), full AND lite:**

```bash
AGENT_GATE_SUMMARY_FILE=/tmp/gate-summary.txt bash scripts/agent-gate.sh > gate.log 2>&1 < /dev/null
cat /tmp/gate-summary.txt   # the SUMMARY block is the ONLY gate text an agent retains; NEVER read gate.log
```

- Prefer `run_in_background` (or a long timeout) so a subagent never idle-waits and gets
  watchdog-killed (#1855). A queued gate ≠ hung gate: under load it prints `waiting for gate slot`.
- Defaults if `AGENT_GATE_SUMMARY_FILE` unset (per-checkout; give concurrent gates in ONE checkout
  unique paths): `.agent-gate-summary.txt` / `.agent-gate-lite-summary.txt` / `.agent-gate-delta-summary.txt`.
- clippy is scoped per-package (#1844): whole workspace `-D warnings` but skips the source-built
  DuckDB amalgamation (cqlite-cli `duckdb-tests`) + OTel stack (`observability`/
  `observability-testing`); parquet/arrow stay linted. `CQLITE_CLIPPY_FULL=1` (nightly `gate.yml`)
  runs the full matrix.
- The FULL gate FAILs CLOSED when the fetched validation corpus is absent (#2078), stamping
  `missing-fixtures: FAIL-CLOSED (#2078)`; `AGENT_GATE_ALLOW_MISSING_FIXTURES=1` opts out visibly
  (`missing-fixtures: OPT-OUT (...)`). Remedy: `bash test-data/scripts/fetch-datasets.sh`.
  `--lite`/`--only` stay lenient.
- Every SUMMARY carries an `accelerators:` line (sccache/nextest/lane state) — degradation there is
  actionable, not noise. Self-test: `bash scripts/tests/test_agent_gate_summary.sh`.

## Core Commands

```bash
cargo build
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test --package cqlite-core
env RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features   # CI mode
cargo fmt
bash test-data/scripts/smoke-test-all-tables.sh
bash test-data/scripts/fetch-datasets.sh    # fetch real SSTable binaries (required for integration tests)
```

Everything else (CLI usage/modes/output precedence, Python/Node build + test + examples, write
support, delta-export, profiling, feature-flag builds, fuzz runs): `docs/development/dev-cookbook.md`.

## Workspace Structure

```
cqlite-core/     # Core library (SSTable parsing, query engine)
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
when schema present. Legacy heuristics live only behind the opt-in `experimental` feature flag.
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

### Fuzzing (issue #1614)
`fuzz/` is a cargo-fuzz/libFuzzer crate in its own workspace, excluded from the main one — the gate
and default builds never compile it; fuzzing needs nightly and is out of the stable gate. Five
targets prove the parser never panics/hangs/OOMs on arbitrary bytes. CI: `fuzz.yml` (PR smoke +
nightly long-run); crashes are filed as bug issues. Run commands: `docs/development/dev-cookbook.md`.

## Test Data

Location: `test-data/datasets/sstables/` — keyspaces `test_basic` (8), `test_collections` (8),
`test_timeseries` (9), `test_wide_rows` (8). **Pass rate: 100% (33/33, Dec 2025).**

The repo ships only JSONL reference files; fetch real binaries with
`bash test-data/scripts/fetch-datasets.sh` and set `CQLITE_DATASETS_ROOT=$PWD/test-data/datasets`.
Without Data.db files, query tests pass but return 0 rows. Dataset pins:
[test data](https://pmcfadin.github.io/cqlite/agents-developing/test-data/).

## Feature Flags

Default (cqlite-core): `all-compression` (LZ4, Snappy, Deflate, Zstd), `state_machine`.
Non-default: `cli-helpers` (#249), `parquet` (#682), `delta-scan` / `delta-export` (#696/#705),
`metrics`, `experimental`. Build recipes: `docs/development/dev-cookbook.md`.

## Troubleshooting

- **Missing test data / 0 rows**: `export CQLITE_DATASETS_ROOT=$PWD/test-data/datasets` +
  `bash test-data/scripts/fetch-datasets.sh`
- **Clippy failures**: run with `RUSTFLAGS="-D warnings"` to match CI
- **Parsing issues**: `docs/sstables-definitive-guide/chapters/appendix-f-known-limitations.md`
- **Python bindings**: Rust 1.85+, Python 3.9+, `pip install maturin`, then
  `cd bindings/python && maturin develop --profile dev`

## Resources

- **Definitive Guide**: `docs/sstables-definitive-guide/`
- **Agent developer docs**: https://pmcfadin.github.io/cqlite/agents-developing/
- **Issues**: https://github.com/pmcfadin/cqlite/issues
- **Cassandra source**: `~/local_projects/cassandra` (local) /
  https://github.com/apache/cassandra/tree/cassandra-5.0.0

## Agent-Team Conventions

- Implementers commit after each meaningful unit of work so reviews land while context is fresh.
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
- **flow-closer (#2084)**: the full gate, C, the final roborev pass, and the merge run inside the
  disposable `flow-closer` subagent — the lead retains only its terminal packet (verdict, PR URL,
  summary-file path, ≤10 lines residual), never gate stdout or review churn.
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
- **GitHub Actions injection** — never interpolate `${{ inputs.* }}`/step outputs into `run:`;
  allowlist-validate fail-closed before any secret step, pass via quoted env var.
- **clippy `manual_range_contains`** — write `(a..=b).contains(&x)`.
- **Integer overflow/saturation** — use `num_bigint::BigInt` for unscaled decimal math; compare
  signs/adjusted-exponents first; never materialize `10^scale` with unbounded exponent.
- **Float ordering vs Java** — `total_cmp` ≠ `Float/Double.compare`; use an explicit comparator
  (NaN last, `-0.0 < +0.0`) when matching Cassandra.
- **Wall-clock races in tests** — capture the time window to cover ALL sampled operations.
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
- **`flow-lead`** orchestrates (the repo's default agent; `claude --agent flow-lead`) — it spawns
  and sequences the specialists + roborev + the gate, and writes no production code. Verbs:
  `flow-groom` → `flow-activate` (**Seam 1**: owner approves spec + design) → `flow-implement` (the
  implement loop above) → `flow-address` → `flow-finalize`; `flow-board` = status + the single next
  thing. Doctrine: [delivery pipeline](https://pmcfadin.github.io/cqlite/agents-developing/delivery-pipeline/).
- **1:1:1:1**: one issue ↔ one worktree/branch `issue-<N>-<slug>` (branched from `origin/main`) ↔
  one OpenSpec change `<slug>` ↔ one PR. Worktrees lack gitignored Data.db binaries — point
  `CQLITE_DATASETS_ROOT` at the main repo's `test-data/datasets`.
- **Board = sole dispatch authority (Path A, #1886)**: the GitHub Project `Status` field
  (`Backlog/Ready/In Progress/In Review/Done`); exactly one `P0`–`P3` per issue. `status:*` labels
  are decorative, NEVER a selection source. New issues auto-land at `Backlog`. Empty Ready column =
  no work ready → STOP. Board unreachable (auth/scope) → STOP and fix auth; never label-dispatch.
- **Claim protocol (cross-machine)**: claim = push the `issue-<N>-<slug>` branch to origin (THE
  lock — assignee alone is not), set assignee + `Status=In Progress`, then re-read and proceed only
  if you hold the branch. Maintain the liveness heartbeat
  (`scripts/flow/claim-heartbeat.sh beat <N>`, refreshed at claim + every stage transition);
  `flow-board` reaps deterministically (age > 4h AND no open PR) (#2089).
- **One worker per machine (#1930)**: one lead/worker session owns a box; it fans out subagents but
  **serializes its own full gates** (the #1825 machine cap is a backstop, not a license) and
  pre-claims by checking for ANY `issue-<N>-*` branch. Multiple independent sessions → separate
  machines, each claim-protocol-gated; NEVER N bare leads without the protocol. Unattended runs:
  `scripts/local/worker-supervisor.sh` (#2090) recycles ONE worker process per issue (hard context
  bound = process exit; the worker writes `.worker-last-iteration.json` then EXITs — never a second
  issue per session), with flock single-instance + preflight + crash-loop breaker + budgets + ntfy
  (`docs/development/fleet-runbook.md`).
- **Inter-issue reset (#2085)**: after each `flow-finalize` the lead drops ALL prior-issue context
  (board renders, gate summaries, roborev findings, PR bodies, Seam-1 spec renders) and re-hydrates
  the next item from **board + disk alone**. Seam-1 spec bodies are not retained after approval —
  `spec-auditor` re-reads them from `openspec/changes/<slug>/`. Durable lessons → `MEMORY.md` /
  `process_improvements.md`, never the live window.
- Spawn subagents with an explicit accessible model (e.g. opus).
- **Telemetry**: `flow-finalize` stamps one record per issue into
  `docs/reports/delivery-telemetry.jsonl` (schema `docs/reports/delivery-telemetry.schema.json`)
  via `scripts/delivery-telemetry.py record` — authoritative
  data only (a counter not observed is an error, never a fabricated 0). On a cadence the manager
  runs `retro` and files a deduped `flow-meta` issue. The SKIP-aware `delivery-telemetry` gate
  component covers the tool. Doctrine: `docs/development/pm-operating-loop.md`.
- **Keep doctrine current in the same change** — user-facing or workflow changes update CLAUDE.md
  and the website `agents-developing/` page as part of the change.

## Product-Manager Behavior (lead)

- The lead acts as product manager: track epics and issues, prioritize, keep work moving.
- **Autonomy — auto-merge on green (default)**: workers (and the lead) **merge their own PR** the
  moment the quality bar is met — gate PASS + **C** PASS (design-driven) + roborev clean — via
  `gh pr merge --squash --delete-branch`, then `flow-finalize`. Do NOT wait for the owner. Seam 1
  (spec approval) is the only standing human gate. Escalate and **hold the merge** ONLY for: a
  genuine design-call roborev finding, a scope/product question, an unmet/uncovered requirement, or
  work outside the issue — and obey any `HOLD: merge after #N` order.
- Autonomous GitHub writes within limits: comments; status labels; assign/reassign. Closing a
  fully-done non-epic issue with a merged linked PR (+ closing comment) is allowed.
- Never close an epic, change an issue's scope/title, or make a product decision (ambiguous scope,
  conflicting requirements, tradeoffs) without the owner — collect under a "NEEDS YOU" list.
- Every issue/PR number carries a brief description (`#1081 (multicell UDT)`, never bare `#1081`).
- Make every write traceable with a short comment.
