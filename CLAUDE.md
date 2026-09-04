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
| **Full** — the gate of record | `scripts/agent-gate.sh` | ONCE per issue, immediately pre-merge, inside `flow-closer`. fmt, clippy `-D warnings`, core/integration/write/CLI tests **at the TARGET granularity each component names, NEVER whole packages** (#3522: `cli-tests` runs 35 of 45 `--test` targets and passes no `--lib`/`--bins`, so `cqlite-cli`'s 255 lib/bin unit tests execute nowhere; `integration-tests` COMPILES `cqlite-integration-tests` (`--no-run`) then runs 6 named targets, leaving its lib's 206 tests and 13 bins unexecuted — per-member record: `scripts/tests/workspace-test-disposition.txt`), `oom-audit` (SKIP-aware structural no-unbounded-materialization audit, #2012), `pub-surface` (cqlite-core crate-root declaration-consistency guard, #1712), **`dep-duplicates`** (the ADVISORY duplicate-dependency ratchet, #1700: measures `cargo tree -d --workspace --target all` — never the bare form, which reads the ROOT PACKAGE only, and never without `--target all`, since `cargo tree` otherwise measures the HOST target and a COMMITTED baseline would then mean different things on a Linux lane and a macOS one — against the committed baseline `scripts/ci/dep-duplicates-baseline.txt`, regenerated by the one documented `bash scripts/ci/check-dep-duplicates.sh --regenerate`. The probe is run READ-ONLY, `--locked --offline`: without `--locked` cargo UPDATES the TRACKED `Cargo.lock` whenever it decides the manifests need it, and a component that rewrites a tracked file mid-run trips #2926's mid-run tree-mutation check — an ADVISORY component that may never FAIL reddening the gate of record from a mutation it caused itself; a failure under either flag is UNMEASURABLE ⇒ SKIP, never an unlocked/online retry, which would restore that mutability silently. It emits **no FAIL at all, by mandate**: an increase is PASS plus a loud, textually distinct `ADVISORY-INCREASE` block naming the delta AND the crates responsible, because a legitimate new dependency can add a duplicate no local decision can collapse and `[patch]`/upstream-fighting pins are out of scope. It also cannot pass VACUOUSLY: PASS is keyed on THREE affirmative signals together — the guard's own `verdict` line PLUS its `probe … INVOKED` line (cargo really ran) and its `MEASURED …` line (a census was really published), because a verdict alone is reachable from a stale, replayed or hard-coded log and once permitted the self-contradictory `PASS [never reached …]` — a clean run reads `0 INCREASE RECOGNISED` rather than a bare `0`, the measurement parser is a CLOSED grammar in which every line must match a recognised shape (record / indented-or-tree-branch continuation in EITHER cargo charset / the exact `[dev-dependencies]`|`[build-dependencies]` pair) and anything else at column zero — punctuation included — is refused rather than skipped, and every unmeasured state — no cargo, **no `timeout(1)` accepting `-k` with which to BOUND the probe** (the probe is then not run at all: an unbounded `cargo tree` could hang the gate, and a missing capability must not inherit the permissive branch), `cargo tree` non-zero or timed out, output the parser does not recognise, a missing or ungrammatical baseline, an unexpected exit status, a zero exit with NO verdict line, or a verdict unaccompanied by the probe/MEASURED lines — is a **SKIP NAMING THE CAUSE**. `cargo tree` is a metadata probe, so the component reads no corpus and is not in `DATASET_COMPONENTS`; its class is `indirect:` with the driver's reach recorded from the guard's own `probe … INVOKED (rc N)` line, never from the terminal status), minimal-features build, the **feature-matrix lanes** (#1699: `flight-tests` EXECUTES cqlite-flight's UNIT suite (`--lib --bins`) and prints a run-time census naming the 42 integration targets it does NOT run, why, and who does (#3384); `legacy-heuristics` builds AND RUNS the feature's gated tests at its own feature set; `feature-iso-parquet`/`feature-iso-delta-scan` hold `parquet` and `delta-scan` in MUTUAL isolation, each without the other, never `--all-features` — `feature-iso-parquet` still COMPILE-ONLY (`--lib --no-run`), while `feature-iso-delta-scan` **EXECUTES** its `--lib` suite plus a run-time-DERIVED set of crate-level `delta-scan`-gated `--test` targets under the zero-tests guards and, on the full gate, `CQLITE_REQUIRE_FIXTURES=1`. Its per-target fixture-AWARENESS scan was DESCOPED by lead ruling on #3725 after seven rounds found seven holes in it — source-text matching cannot decide whether a lookup is executable; #3789 owns declared per-target posture, #3725), the **binding lanes** (#3522: `binding-rust-tests` EXECUTES `cqlite-ffi-common` (ALL targets) and `cqlite-node` (`--lib`), whose Rust tests previously ran NOWHERE, and never SKIPs — it needs nothing beyond cargo; `node-bindings` runs the WHOLE jest suite, not 1 of 27 files), `all-features-check` (#3453: `cargo check` + `cargo clippy -D warnings`, both at `-p cqlite-core --all-features --all-targets` — the ONLY component that enables the OTLP stack; never SKIPs), smoke. Emits `AGENT-GATE SUMMARY`. |
| **Lite** (#1821 — cost is a FUNCTION of the diff; see the measured bands) | `scripts/agent-gate.sh --lite` | EVERY fix round. file-size + fmt + clippy + roborev-lints + blast-radius tests. **Two cost drivers, and only ONE of them scales with your diff.** (1) **`clippy` is NOT diff-scoped** — `--lite` dispatches the IDENTICAL `run_clippy` the full gate does (`run_component clippy run_clippy`, `scripts/agent-gate.sh:17233` vs `:18220`), i.e. the #1844 **per-package scoped-workspace** matrix at `:9357`, and `run_clippy` never reads the diff. (The whole-workspace `--all-features --all-targets` form is the `CQLITE_CLIPPY_FULL=1` path only — do not read the scoped matrix as that one.) So every `--lite` pays clippy IN FULL whatever the diff: measured over 188 completed lite runs it is a no-op warm, 2–7 min part-warm, and **16–24 min cold**. (2) **`scoped-tests` is diff-scoped, and has a fan-out leg the old wording omitted entirely**: it RUNS the touched package's `--lib` + the diff's new `--test` targets (owners by longest-prefix path match over `cargo metadata`, from `merge-base(HEAD, <base>)...HEAD` where `<base>` is the FIRST of `origin/main` → `main` → `origin/master` → `master` that resolves (`:16870`), **plus `git diff HEAD` — the uncommitted diff over TRACKED files only, untracked excluded**; defaults to `cqlite-core --lib` when no rust package is in the diff) — **and when a changed path is under `cqlite-core/src/` it ALSO runs `cargo test -p <pkg> --all-targets --no-run` for every workspace member that DIRECTLY DECLARES a dependency on `cqlite-core` (the `--no-deps` metadata edge) and owns a `--test` target (#2658: COMPILE-CHECKED, never run).** That leg — NOT "touched packages", which consult no dependency edge at all — is why a core-src diff annotates 9–11 package sets, and its `--all-targets` is what balloons `target/debug/deps` (+18 GB in a single round — reported by another lane in #3763/#3764, not measured here). **Measured bands** (completed runs, one fleet box): a **narrow, WARM-clippy** diff is **median 1.4 min** (n=43) — so the `~1–5 min` this row used to claim is that case exactly, a FLOOR and not a range. **The bands are marginal over DIFFERENT subsets and do not compose**: a 1.4 min run is by construction one that paid no cold clippy, so you cannot add the cold-clippy band to it — read each as what its own population measured. A **`cqlite-core/src/`** diff is **median 20 min, range 3.8–43 min** (n=20), and lane-3612 **reports** (not measured here) **up to ~104 min under peer load** in #3764. `cqlite-core/tests/**` does NOT trigger the fan-out; `cqlite-core/src/**` does. **`--lite` is EXEMPT from the #1825 gate-slot cap** (as are `--delta`/`--only`) — it runs outside slot arbitration entirely, so on a shared box its build competes with a peer's gate of record for disk and CPU with nothing arbitrating it. **There is NO admission check for `--lite` today and #3763 owns that gap** — do not read this row as instructing you to apply one. Emits a DISTINCT `AGENT-GATE LITE SUMMARY` (MODE: lite) — can NEVER be pasted as the full SUMMARY. |
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

- **Completion and verdict are two assertions.** Probe completion with
  `grep -qE 'RESULT: (PASS|FAIL)'`; `INCOMPLETE` is a liveness placeholder written at launch, not a
  verdict. And read a **component's OWN line**, never the terminal token — `PARTIAL` says the *run*
  was partial, not that your component failed.
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
tools/           # 7 crates, each with a RECORDED disposition in one of THREE
                 #   categories, pinned by the gate guard
                 #   scripts/tests/test_tools_crate_disposition.sh (#1716).
                 #   These labels say whether something INVOKES the crate —
                 #   usually its BINARY — and NOT whether its TESTS execute
                 #   (#3522). Of the WIRED four only ws0-corpus-gen's tests run
                 #   in the gate (tooling-tests); cassandra-parity (25+9),
                 #   sstabledump-validator (17+2) and flight-loadgen (21) have
                 #   tests that execute NOWHERE, as does MIXED format-validator
                 #   (8). Per-member record, with the label AND the class:
                 #   scripts/tests/workspace-test-disposition.txt.
                 #   WIRED   — cassandra-parity, flight-loadgen,
                 #             sstabledump-validator, ws0-corpus-gen.
                 #   UNWIRED — nothing runs them AND nothing depends on them:
                 #             cqlite-validator, memory-safety-runner. Each needs
                 #             a README saying it is NOT CI-wired.
                 #   MIXED   — format-validator: its 4 BINS are orphaned but its
                 #             LIB is WIRED (tests/format-compatibility = the
                 #             gate's `format-compat` component). Its README must
                 #             name BOTH halves, and the crate must stay a
                 #             workspace member — never `exclude` it.
                 #   A NEW tools/ crate must be classified there or the gate FAILs.
                 #   That guard is deliberately SMALL: it checks a disposition
                 #   was RECORDED and LABELED, not that the record is TRUE, and it
                 #   is per-CRATE (an orphaned bin added to a WIRED crate passes
                 #   unchanged). It needs no cargo/python3/network. A
                 #   cargo-derived cross-check that verified truth was built and
                 #   REMOVED (#1716) — 11 review findings landed in it and none in
                 #   the list/README part, and its scratch workspaces sat outside
                 #   the repo so they did not inherit rust-toolchain.toml, making a
                 #   MANDATORY gate component host-toolchain-dependent. Doing it
                 #   properly is its own issue under epic #1688.
fuzz/            # cargo-fuzz crate — own workspace, EXCLUDED from the main one
```

**A bare `cargo build` here already builds only the ROOT package — do not "optimize" it with
`default-members` (#1716).** This workspace has a root package (`cqlite`), and cargo's default for
`default-members` in that case is **that package alone** ("all members" is the default only for a
VIRTUAL workspace). Verified: `cargo tree --depth 0` at the root resolves to `cqlite` and nothing
else. So adding an explicit `default-members` list would **expand** the bare build from 1 package to
14 — the opposite of the intent, and the trap #1716 was originally written around ("these crates are
compiled by every workspace build" was false). The `tools/` crates are compiled only by an explicit
`--workspace`/`--all-targets` (the gate's clippy) or `-p`. So those crates stay fully linted under
`-D warnings` no matter their disposition.

**Their unit tests, though, run ONLY when your diff touches their package (#1716).** No CI job and
no gate component runs workspace-wide tests, so an untouched `tools/` crate's tests execute only
where something names its package explicitly — `ws0-corpus-gen` under the gate's `tooling-tests`, and
`cassandra-parity` in the path-filtered, `required`-exempt `cassandra-parity.yml`; for every other
`tools/` crate they never execute (#3522). But `--lite`'s blast-radius maps a touched path to its
package and runs that package's `--lib` tests. Consequence, found the hard way on #1716: editing
only `tools/format-validator/README.md` made `--lite` run that crate's tests **for the first time**,
and one failed —
`test_hex_dump_formatting` asserted an unseparated `"48656c6c6f"` against a `hexdump -C`-style
formatter that emits `48 65 6c 6c 6f`, an expectation that could never hold for any input. **Expect
latent failures the first time you touch a long-unwired crate**; they are pre-existing, not yours,
but they are yours to fix because your diff is what runs them.

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
The full gate's `pub-surface` component (`scripts/ci/check-pub-surface.sh`) asserts ONE property,
answered entirely from source: an unconditional, non-`#[doc(hidden)]` top-level `pub mod NAME;` in
`cqlite-core/src/lib.rs` must not be gated by an inner `#![cfg(...)]` inside `NAME`'s own file. The
defect it exists for: `pub mod benchmarks;` read as shipped public API for months while an inner
`#![cfg(feature = "benchmarks")]` in `benchmarks/mod.rs` configured it out of every default build.
Both facts are source and each is a BOUNDED read — the declaration's attributes structurally from
`lib.rs`, and the module file's PROLOGUE (rustc-verified to hold every inner attribute a module
has). It **REFUSES rather than guess**: a `pub mod` shape it does not recognise, a module file
resolving to neither/both legal paths, an unreadable module file, a block comment in a prologue or
an inner attribute it cannot classify are each a named FAIL. Remedy is always the same — hoist the
gate to the declaration site.

**PUBLIC-API DRIFT DETECTION IS NOT PART OF IT.** There is no `pub-surface.snapshot` and no
`--regenerate`: the rustdoc-derived snapshot half was **removed deliberately** (#1712) because five
review findings were one defect class — a scanner that had to find declarations anywhere in
arbitrary source, an unbounded parsing problem that cannot abstain. So **nothing in this repo
currently detects a public-API change**, and a green `pub-surface` must never be read as one; the
principled route (reachability from rustc's own dep-info) is **issue #3366**.

### Code quality
- `RUSTFLAGS="-D warnings"` must pass; no `unwrap()`/`expect()` in library code; `thiserror` for errors
- Memory target: <128MB for large files

### File size (campsite rule)
Keep files small — agentic context cost scales with file size. Targets (total lines, inline tests
included): source `~800`, test files `~1500`. The gate's `file-size` ratchet FAILs if your change
grows an over-threshold `.rs` file (or pushes one over). Touching an over-threshold file → split it
by responsibility (source: epic #1116; tests: #1135). Genuinely out of scope → re-run with
`CQLITE_ALLOW_FILE_GROWTH=1` and leave a note linking #1116/#1135. **That override is now
VISIBLE in the SUMMARY, and the component's status token says so (#3402):**
`file-size: OPT-OUT (0s)  [no-cargo] — CQLITE_ALLOW_FILE_GROWTH=1 (ratchet NOT enforced); N
over-threshold file(s) grown — see <logdir>/file-size.log`. `PASS` means the check RAN
and was SATISFIED; it never means the check was switched off — a bare `file-size: PASS` under an
engaged override was indistinguishable from a genuine one, so the disclosure depended on the
author remembering to write it in the PR body. `OPT-OUT` is NON-FAILING (only an exact `FAIL`
sets `OVERALL=FAIL`), so an acknowledged growth still reaches `RESULT: PASS` — it is now merely
impossible to hide. It is emitted ONLY for the value **exactly `1`**: a value SET BUT NOT `1`
(`0`, `true`, `yes`) is not an opt-out, stays a ratchet violation and FAILs, because a
permissive branch keyed on `!= <bad>` would let a typo waive the ratchet. **The row carries NO
repository content**: the file NAMES live in `file-size.log` (#3401) and, for a reviewer, in the
PR diff itself. Rendering them inline was tried and REMOVED — it was the optional half of #3402
("ideally naming the files") and produced THREE of that PR's seven review findings, one per
round, each a different way of mangling a filename (a `: ` split recovering a path from a
display string; substitution inside a path containing `RESULT:`; `,` joining, making
`src/a.rs,b.rs` indistinguishable from two files). **Remove the mechanism rather than carve it a
fourth time** (#3229's ruling); escaping only moves the argument to the escape grammar (#3312).
So the one boundary rendering these details (`_status_detail`) takes GATE-AUTHORED text only —
fixed wording plus computed values — strips `[:cntrl:]` under `LC_ALL=C`, and WITHHOLDS any
value carrying the completion probe's `RESULT:` token rather than rewriting it, because a
rewrite would name something that does not exist. If you add a component that needs repository
content in its detail, re-introduce a trusted/untrusted split deliberately; do not smuggle it
through the gate-authored field.

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
- **Fourth blind spot: EVERY oracle above is PER-SURFACE, so three surfaces can each be green against
  their own oracle while DISAGREEING WITH EACH OTHER (issue #1455).** Python, Node and the CLI are three
  independent windows onto one SSTable, and each was checked only against its own reference — Python
  against the CLI (`test_cli_parity.py`), Node against the sstabledump JSONL goldens
  (`parity-utils.js`), the CLI against nothing else. Those two normalizers **do not share an oracle, a
  canonical form, or even a comparison direction** (blob canonicalizes to a `"0x…"` STRING on the Python
  side and to a `Buffer` on the Node side; timestamp to a millisecond-truncated string vs a `Date` with a
  ±1 ms tolerance; Node has **no duration rule at all**), so both can pass while a user querying one table
  three ways gets three answers. The cross-surface differential is
  `bindings/parity/` + `bindings/python/tests/test_cross_binding_parity.py`: ONE `SELECT`, all three
  surfaces, canonical JSON, deep-equal per row. **The canonical form is implemented TWICE by construction
  (`canonical.py` / `canonical.mjs`) and the two are DIFFERENTIALLY PINNED** against a shared
  `canonical-vectors.json` — a second implementation's agreement is only knowable by testing it, never by
  care. **SEVEN DECLARED gaps, printed IN FULL at run time from one `DECLARED_GAPS` tuple — because a
  lane that omits coverage silently is indistinguishable from one that covers it, and a README nobody
  opens is not a declaration**: (1) `tuple` vs `list` is UNDETECTABLE here — Node and the CLI both emit
  a plain array and only Python has a distinct type, so it is canonicalized as a plain array; (2) **no
  `varint` column exists anywhere in `test-data/schemas/*.cql`**, so that rule is pinned by
  `canonical-vectors.json` alone and by no fixture; (3) UDT columns are REFUSED by the canonicalizer
  rather than compared, and no fixture uses one; (4) non-finite floats are a real 3-way asymmetry
  (Python `nan` / Node `NaN` / CLI JSON `null`, `cqlite-cli/src/output/json.rs:156-161`) and are avoided
  rather than reconciled; (5) a column absent from one leg is compared as `null`, so the harness cannot
  tell *omitted* from *null* — and the omitting leg is **NODE** (`bindings/node/src/row.rs:130` skips a
  metadata column with no value, while `bindings/python/src/result.rs:447` null-FILLS a shared row
  shape; the first draft of this harness blamed Python, which is backwards); (6) **A UNIFORM
  `cqlite-core` DEFECT IS INVISIBLE TO IT — all three legs read the SAME core, so agreement here is
  agreement about CQLite, not about Cassandra.** That is #3042's round-trip-invariance lesson one level
  up: a differential between SURFACES over a shared engine can only find *surface* divergence, and it
  never substitutes for a Cassandra-written oracle; (7) the 3-way comparison runs in **CI only**.
  **THAT LAST ONE MEANS THIS HARNESS IS NOT MERGE-GATING.** No local gate component can run it — the
  gate runs pytest with `RUN_SLOW_TESTS=0` and builds neither the Node native module nor a release
  `cqlite-cli` — so it lives in `python-ci.yml`'s `cross-binding-parity` job, which is
  `required`-exempt AND in the heavy `ci:bindings-full` tier, i.e. on a routine unlabeled PR it does not
  run at all. A cross-binding divergence can therefore still merge; the `.github/ci-gating-tiers.yml`
  exemption NAMES that residual rather than implying coverage it does not have (#3493). Marking the test
  `@pytest.mark.slow` is deliberate and not an oversight: unmarked, the gate's `python-bindings`
  component would instantiate the `cli_binary` fixture and add a full release `cqlite-cli` build to
  EVERY lane's full gate. **And the fixture-skip route is a defect this harness reproduced inside its own
  first draft, caught in review**: `conftest.py`'s `cli_binary` fixture `pytest.skip`s on build failure
  and is NOT strict-aware, and the CI job invokes only this one file — whose other non-slow tests pass,
  so #1230's session floor never fires. All three parity cases would have skipped and
  `cross-binding-parity` would have reported SUCCESS having compared nothing. The parity lane therefore
  wraps that fixture and `pytest.fail`s under strict mode, and both data tables carry committed **case
  floors** (minimum fixture/vector/refusal counts plus required names and CQL kinds), since an emptied
  table otherwise yields an empty parametrize that pytest reports as one skipped placeholder — #3544's
  case-floor lesson, one directory over.
- **Fifth blind spot: a point-read test that compares a SUBSET of columns against the scan cannot see
  a TRUNCATED point row (issue #3890).** The four above are about which ORACLE you compare against;
  this one is about how much of the row you compare. `assert_point_equals_scan`
  (`cqlite-core/tests/issue_1573_readat_positional.rs`) projected `id` plus ONE named column, and
  `SELECT id, name` decodes the first two cells and stops — so a point read whose LATER cells failed to
  decode compared equal on exactly the columns being compared, for years. Two properties make it
  invisible rather than merely under-tested: a failed cell decode inside the row loop is SWALLOWED
  (`row_decoder/row_data.rs` logs at `debug` and `break`s — #3721 is removing that), so nothing
  propagates; and the missing cells are simply ABSENT from the row's map, so a `get(col)` comparison
  over the columns you named can never notice them. **Rule: a point/seek-vs-scan comparison uses
  `SELECT *` and asserts BOTH directions of the column set** — no scan column absent from the point
  row, no point column the scan lacks — and reports the missing column BY NAME. The corpus-wide
  instance is `cqlite-core/tests/issue_3890_point_read_column_parity_sweep.rs`. **Two rules about
  its per-table key cap, both of which cost a review round: a bound tight enough to cost nothing
  can be tight enough to miss most of what it exists to catch, so measure what your cap EXCLUDES;
  and a cap's detection figure is only meaningful alongside its SELECTION** — capping in scan order
  and sorting afterwards samples different keys than capping over the sorted set, and that alone
  moved the same measurement. **No figure is quoted here on purpose: measuring a guard's detection
  power needs the swallow instrumented AND the fix reverted, so it is not reproducible from
  committed source, and a number nobody can re-derive from the repo is what stops the next person
  looking.** That target's module header carries the numbers with the exact recipe — commands, cap
  values, and how the fix is reverted so detection is measured against the defect PRESENT.

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

**Every declared feature must be LOAD-BEARING, and the full gate's
`features-load-bearing` component enforces it (#1698).**
`scripts/ci/check-features-load-bearing.sh` derives every feature declared by every
workspace member from `cargo metadata --no-deps` (never a textual manifest sweep — cargo
SYNTHESISES implicit features from optional deps that no `[features]` block contains, and
`find`ing manifests reaches non-members cargo never builds) and asserts each one changes
something: it, or something in its feature CLOSURE, has a cfg reference site in its
DECLARING package's sources, enables an optional dependency, enables a feature of an
external dependency, or is named in a target's `required-features`. **CREDIT FLOWS UP FROM
EFFECTS, NEVER DOWN FROM A PARENT** — a leaf named only by an aggregator is DEAD, which is
exactly how four `test-*` leaves survived for months behind `test-infrastructure`, while
the legitimate `all-compression` stays green through its four dep-pulling leaves. **Being
ENUMERATED is not an effect**: the gate's own clippy feature lists, a workflow
`--features` argument and a doc table all NAME features without enabling anything, so
deleting a dead flag means cleaning those enumerations IN THE SAME DIFF. Only `default` is
exempt (cargo defines its meaning; an empty `default = []` is legitimate). Fail-closed on
every derivation failure, and there is deliberately **no bypass flag and no env opt-out** —
a dead flag is always deletable, so an escape hatch could only buy a vacuous green.
The component's prerequisites are **cargo AND python3, both mandatory and declared**
(cargo metadata is the only source of truth for the feature set; python3 is the reader
that parses its JSON and lexes Rust) — absent either it FAILs with a named remedy and
never SKIPs, while its self-test in the SKIP-aware `tooling-tests` component SKIPs loudly
on a python3-less box, so the never-SKIPping lane is not folded into a SKIP-aware one
(#3522). Its `cargo metadata` runs `--locked`, so a mandatory component can never rewrite
`Cargo.lock` mid-gate and trip the tree-integrity check (#2926). The guard's claim is
SCOPED and printed: no false FAIL for a gate in a RECOGNISED spelling (`#[cfg]`,
`#![cfg]`, `cfg!`, `cfg_attr` condition and tail, whitespace and string escapes handled),
explicitly INCOMPLETE, with the escape routes and the two NOT-SEEN spellings (a
macro-expanded feature name, a runtime-built build-script env key) enumerated in the
line — an absolute soundness claim was tried and retracted after six rounds of witnesses.
Deleted by #1698: `events`, `ci_zero_tolerance` (5 manifests), the four
`test-infrastructure` leaves, `sstable-writer`, cqlite-cli `interactive` (it sat in
`default`), `cqlite-core/unit-tests-only` (the cqlite-integration-tests feature of the
same name keeps its 25 cfg sites) and `wasm` with its three wasm32-only deps — the 27
`cfg(target_arch = "wasm32")` sites stay, they gate on target, not on that feature.

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
