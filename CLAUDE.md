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
| **Full** — the gate of record | `scripts/agent-gate.sh` | ONCE per issue, immediately pre-merge, inside `flow-closer`. fmt, clippy `-D warnings`, core/integration/write/CLI tests **at the TARGET granularity each component names, NEVER whole packages** (#3522: `cli-tests` runs 35 of 45 `--test` targets and passes no `--lib`/`--bins`, so `cqlite-cli`'s 255 lib/bin unit tests execute nowhere; `integration-tests` COMPILES `cqlite-integration-tests` (`--no-run`) then runs 6 named targets, leaving its lib's 206 tests and 13 bins unexecuted — per-member record: `scripts/tests/workspace-test-disposition.txt`), `oom-audit` (SKIP-aware structural no-unbounded-materialization audit, #2012), `pub-surface` (cqlite-core crate-root declaration-consistency guard, #1712), minimal-features build, the **feature-matrix lanes** (#1699: `flight-tests` EXECUTES cqlite-flight's UNIT suite (`--lib --bins`) and prints a run-time census naming the 42 integration targets it does NOT run, why, and who does (#3384); `legacy-heuristics` builds AND RUNS the feature's gated tests at its own feature set; `feature-iso-parquet`/`feature-iso-delta-scan` compile `parquet` and `delta-scan` in MUTUAL isolation, each without the other, never `--all-features`), the **binding lanes** (#3522: `binding-rust-tests` EXECUTES `cqlite-ffi-common` (ALL targets) and `cqlite-node` (`--lib`), whose Rust tests previously ran NOWHERE, and never SKIPs — it needs nothing beyond cargo; `node-bindings` runs the WHOLE jest suite, not 1 of 27 files), `all-features-check` (#3453: `cargo check` + `cargo clippy -D warnings`, both at `-p cqlite-core --all-features --all-targets` — the ONLY component that enables the OTLP stack; never SKIPs), smoke. Emits `AGENT-GATE SUMMARY`. |
| **Lite** (#1821 — cost is a FUNCTION of the diff; see the measured bands) | `scripts/agent-gate.sh --lite` | EVERY fix round. file-size + fmt + clippy + roborev-lints + blast-radius tests. **Two cost drivers, and only ONE of them scales with your diff.** (1) **`clippy` is NOT diff-scoped** — `--lite` dispatches the IDENTICAL `run_clippy` the full gate does (`run_component clippy run_clippy`, `scripts/agent-gate.sh:17233` vs `:18220`), i.e. the #1844 **per-package scoped-workspace** matrix at `:9357`, and `run_clippy` never reads the diff. (The whole-workspace `--all-features --all-targets` form is the `CQLITE_CLIPPY_FULL=1` path only — do not read the scoped matrix as that one.) So every `--lite` pays clippy IN FULL whatever the diff: measured over 188 completed lite runs it is a no-op warm, 2–7 min part-warm, and **16–24 min cold**. (2) **`scoped-tests` is diff-scoped, and has a fan-out leg the old wording omitted entirely**: it RUNS the touched package's `--lib` + the diff's new `--test` targets (owners by longest-prefix path match over `cargo metadata`, from `merge-base(HEAD, <base>)...HEAD` where `<base>` is the FIRST of `origin/main` → `main` → `origin/master` → `master` that resolves (`:16870`), **plus `git diff HEAD` — the uncommitted diff over TRACKED files only, untracked excluded**; defaults to `cqlite-core --lib` when no rust package is in the diff) — **and when a changed path is under `cqlite-core/src/` it ALSO runs `cargo test -p <pkg> --all-targets --no-run` for every workspace member that DIRECTLY DECLARES a dependency on `cqlite-core` (the `--no-deps` metadata edge) and owns a `--test` target (#2658: COMPILE-CHECKED, never run).** That leg — NOT "touched packages", which consult no dependency edge at all — is why a core-src diff annotates 9–11 package sets, and its `--all-targets` is what balloons `target/debug/deps` (+18 GB in a single round — reported by another lane in #3763/#3764, not measured here). **Measured bands** (completed runs, one fleet box): a **narrow, WARM-clippy** diff is **median 1.4 min** (n=43) — so the `~1–5 min` this row used to claim is that case exactly, a FLOOR and not a range. **The bands are marginal over DIFFERENT subsets and do not compose**: a 1.4 min run is by construction one that paid no cold clippy, so you cannot add the cold-clippy band to it — read each as what its own population measured. A **`cqlite-core/src/`** diff is **median 20 min, range 3.8–43 min** (n=20), and lane-3612 **reports** (not measured here) **up to ~104 min under peer load** in #3764. `cqlite-core/tests/**` does NOT trigger the fan-out; `cqlite-core/src/**` does. **`--lite` is EXEMPT from the #1825 gate-slot cap** (as are `--delta`/`--only`) — it runs outside slot arbitration entirely, so on a shared box its build competes with a peer's gate of record for disk and CPU with nothing arbitrating it. **There is NO admission check for `--lite` today and #3763 owns that gap** — do not read this row as instructing you to apply one. Emits a DISTINCT `AGENT-GATE LITE SUMMARY` (MODE: lite) — can NEVER be pasted as the full SUMMARY. |
| **Delta** (#1892) | `scripts/agent-gate.sh --delta <anchor-sha> --anchor-run-id <id>` (or `--anchor-summary-file <path>`) | Re-certify a post-full-PASS polish round whose diff is ONLY executable tests/docs (rust test code, python/node binding tests against an already-built module, `scripts/tests/*.sh`, `*.md`; #2081). FAILs CLOSED on anything else (src, scripts, workflows, `Cargo.*`, config, test-data, unbuilt node module) — never builds, never passes vacuously. Emits a DISTINCT `AGENT-GATE DELTA SUMMARY` naming the anchor + a `delta-executors:` line; record BOTH it AND the anchor's full SUMMARY in the PR. NOT the gate of record. |

**Compiling a feature is not covering it (#1699).** The scoped clippy matrix enables ~30 cqlite-core features
at once under `--all-targets`, so a feature can be *test-compiled* on every full gate and have **executed
nothing** — and a combined feature set is exactly what MASKS cross-feature coupling (an item gated on feature
A referencing feature B's items compiles fine while both are on). Measured, not argued: turning EXECUTION on
for `legacy-heuristics` surfaced 4 tests that had never run once, two of which assert behaviour CQLite
deliberately does not support (#3372 five `not yet implemented` stubs behind the flag; #3374 filler-byte mock
`Statistics.db` plus pre-`na` `mc-` names); and `flight-tests` surfaced **14 cqlite-flight targets that
execute NOWHERE** — not locally, not in CI — because their module-level
`#![cfg(feature = "observability-testing")]` is off in every lane that runs them (#3375), a gap #2910's tier
aggregation cannot see because the tier *runs* and silently executes 0 tests. So when you add a feature flag,
ask which lane **executes** it, not which lane compiles it; if the answer is none, the feature is uncovered
however green the gate looks. `experimental` is **one** remaining instance (#3373) and NOT the only
one: in `cqlite-core` the crate-level-gated integration targets for `delta-scan` (13) and
`observability-testing` (14) are named by no `--test` in the gate and execute ZERO tests at
`core-tests`' feature set, as do 3 of the 5 `dhat-heap` ones; the `delta_scan` module's own 39 lib
tests run in no gate component either (`feature-iso-delta-scan` is `--lib --no-run`), only in the
`required`-exempt `ci.yml` (#3522 audit).
**AND THE SAME REASONING RUNS AT PACKAGE GRANULARITY, WHICH IS WHERE IT WAS COSTLIEST (#3522).**
`cargo clippy --workspace --all-targets` compiles EVERY workspace member on every full gate, so a
whole CRATE can be built by every run and execute nothing — and it reads as covered precisely
because the workspace builds clean. Measured: `cqlite-ffi-common` appeared **zero times** in
`scripts/**` and `.github/workflows/**` (37 unit tests + `tests/dependency_boundary.rs` +
`tests/error_contract_table.rs`, executed by nothing anywhere), and `cqlite-node`'s 53 Rust unit
tests were in the same hole because `node-bindings` runs jest against the BUILT ARTIFACT and never
`cargo test`. Both now run in `binding-rust-tests`. Two design rules came out of it. **A
never-SKIPping lane must not be folded into a SKIP-aware one**: `node-bindings` correctly SKIPs
without node/npm, and putting cqlite-node's *Rust* tests behind that SKIP would be a coverage hole
wearing a SKIP's clothes — so the Rust lane depends on nothing beyond cargo and never SKIPs. **And
enrolling a lane in `DATASET_COMPONENTS` is not enough to stop a corpus-dependent suite skipping**:
the widened `node-bindings` also exports `CQLITE_REQUIRE_FIXTURES=1` on the full gate, which buys ONE
named setup failure instead of 14 separate `beforeAll` throws and closes `parity.test.js`'s `test.skip`
placeholder — the one corpus-conditional path in that suite that would pass silently. (An earlier draft
of this paragraph said those suites `describe.skip`; **measured, none does** — the repo's Node
convention THROWS. A false rationale in a gate log is worse than none, because it is what stops the
next person looking.) The durable question is the same one shape up: for each workspace member, **which component
EXECUTES it** — recorded, member by member, in `scripts/tests/workspace-test-disposition.txt`
(`EXECUTED`/`PARTIAL`/`NOT-EXECUTED`, a closed label set enforced under `tooling-tests`), so a new
crate cannot join the unexecuted set unannounced. Each record also carries a CLASS — `silent` (no
committed doctrine claims it is covered) vs `contradicts-doctrine` (doctrine says it is and it is not)
— coupled to the label (`EXECUTED` ⇔ `no-gap`), because a gap our own doctrine denies is a false
certification and not a backlog item. That census records completeness and labeling, **not
truth** — deliberately, on #1716's precedent.
Two corollaries the lanes are built on. **Derive, never curate**: both executing lanes compute their subject
set from committed source at run time — `legacy-heuristics` its `--test` targets (from cargo metadata plus a
module closure, so a manifest-gated or directory-style target is not missed) and its allowed-zero set, and
`flight-tests` its unit-target set from cargo metadata — so a new gated file is picked up and a feature joining
`default` shrinks the excusal set with no gate edit. A failed derivation is a FAIL naming the derivation, never
a fallback to "nothing enabled", which would silently excuse every gated target. **And a narrowed lane
DECLARES the narrowing at run time**: `flight-tests` prints what it does not execute on every run, because a
lane that omits coverage silently is indistinguishable from one that covers it — the same reason this whole
component set exists. `legacy-heuristics` declares a second, subtler narrowing the same way: a test target can
reach a child module through a cfg the derivation does not evaluate (`#[cfg(all(feature = …))] #[path = …] mod
support;` on a shared helper — 3 such targets in `cqlite-core` today), and the closure used to follow that child
while DISCARDING the attribute gating it, so a gated test inside counted as executable while an ungated sibling
kept the target non-zero and the co-required census reported **no gap**. Such a subtree is now reported as a
`DECLARED GAP` with a `cfg-gated-subtree gaps: N RECOGNISED` census line that states its own non-exhaustiveness and is affirmative at `0` — **`0 RECOGNISED`, never a bare `0`**, because a bare zero in a gate log reads as a verified all-clear from a scan that is documented as incomplete. Deliberately **declared, not
fatal**: failing the lane on it was tried and reverted, because those helpers are correct code and **a lane that
reds on correct input is the lane agents learn to waive**. The `UNRESOLVED` half stays fail-closed — an
incomplete source set is permissive everywhere, an unevaluated one is merely unattributable. And
**a lane in `--list` is not a lane that works**: `feature-iso-parquet` reports `PASS (0s)` warm, so presence
proves nothing. `scripts/tests/test_agent_gate_feature_matrix_lanes.sh` (opt-in) plants each lane's
incident-class break in a throwaway `git worktree` and requires the lane to red **and** to NAME the planted
symbol — a bare red is not evidence either, since an unrelated breakage produces an identical exit code and
SUMMARY line. `scripts/tests/test_agent_gate_binding_rust_lanes.sh` does the same for the #3522 binding
lanes, and adds the case the failing-assertion plants cannot reach: one that cfg's a unit suite OUT, so it
compiles, runs **zero** tests and exits 0 — the only plant that exercises the non-zero-count half of
`check_unittest_targets_ran`.

**A CI exemption that defers to a local gate component is only as true as that component's SCOPE
(#3493).** `.github/ci-gating-tiers.yml` excuses a workflow from `required` by naming the local
component that supposedly owns its merge-gating half — and nothing checks that the named component
actually covers it. Measured instance, since FIXED by #3522/#3574: the `node-ci.yml` exemption read
*"the merge-gating half is the local gate's node-bindings component"* while `node-bindings` ran ONE
of the Node suite's 27 test files (`npx jest write-readback-content`, narrowed for speed under
#1255), so **26 files were gated by neither side** — and a deterministic export-surface red sat on
`main` for ~2 days across 4 Node contexts without blocking a merge. Its sibling is the control:
`python-bindings` runs the whole pytest suite, so the identically-worded Python exemption was true.
This is the **circular-deferral** shape #3544 records for `ci-minimal-features.yml` — each side's
coverage justified by the other's, the content exercised by neither, **with a documented rationale
on both sides explaining why that is fine** — and it is a confirmed family, not a one-off. Two rules
follow. **Narrowing a component for speed is a CHANGE TO A MERGE GATE**: if a registry exemption
names it, correct that entry in the same diff or the exemption silently becomes false. And **when
you widen or narrow, measure first** — the #1255 narrowing outlived its own premise (the widened
component measures **138s**, dominated by the `release-unwind` LTO build it already paid), so the
speed argument that justified the hole had stopped being true long before anyone re-checked it.

**AND "DOES EVERY TEST RUN" IS NOT "IS THE CORPUS COMPLETE" (#3493).** #3522's census answers the
first; it cannot answer the second, and neither can its per-suite guard. The Node parity cases
**derive their table set FROM DISK**, so a partial extraction is green BY OMISSION: every suite
runs, every suite does real work, and the missing tables are simply never enumerated. Hence
`test-data/scripts/check-dataset-manifest.sh`, paired with `npm test` in `node-bindings`, asserting
that every expected table is present AND usable. Measured against the real binding, on an otherwise
intact generation: a **zero-length `CompressionInfo.db` or `Statistics.db` makes `SELECT` return 0
ROWS silently** (not an error — the "0-rows-when-present" failure this repo says must never pass),
and a second generation whose `Data.db` is well-formed garbage makes the reader throw. A
completeness check proves files are present and usable AS FILES; it cannot prove they parse — that
is the reader's job, and no amount of `stat`ing substitutes for it.

**A GREEN FULL GATE DOES NOT SUBSUME `pr-gate-core` (#3453).** The two check sets overlap; neither
contains the other, and this is structural, not a backlog item. The gate runs lanes CI cannot
(`arrow-parity-guard` names a `#![cfg(feature = "arrow")]` integration target that pr-gate's `--lib
--all-features` compiles no path to), and pr-gate runs a lane the gate does not: `cargo test -p
cqlite-core --lib --all-features` EXECUTES cqlite-core's unit suite with the OTLP stack ON, which no
gate component executes. **MEASURED ON `main`, NOT CITED FROM AN INCIDENT: the gate of record
DISCOVERS 3562 cqlite-core `--lib` tests (`--features cli-helpers`); pr-gate-core discovers 3782
(`--all-features`) — so 220 lib tests execute in CI and NOWHERE in the gate of record.** #3382's own
fix pin (`a_stats_only_name_cannot_create_an_instrument_through_the_emit_path`) is one the gate cannot
even list (`-- --list` finds 0 vs 1). That is how PR #3382 earned a 31/31 gate PASS without executing
the test pinning its own fix — the issue was filed around one instance; the standing gap is 220 tests
wide. `all-features-check` now closes the **compile/lint half** — a type error or a
`-D warnings` lint under `#[cfg(feature = "observability")]` reds the gate of record — and
**deliberately not the runtime half**: it executes NONE of those 220, so an order-dependent defect like #3382's
(a process-wide `OnceLock<Instruments>` poisoned by whichever test binds the global meter to a no-op
provider first, invisible to `#[serial_test::serial]` grouping) STILL fails only in CI. Note the
tests in question are gated on `observability-testing`, not `observability`. So never read a green
SUMMARY as a prediction about `required`; a red CI check on a green-gate PR is an ordinary event.

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
- **A gate launched in-session dies with its session's CGROUP, and no detach idiom saves it — run it
  with `scripts/flow/gate-detached.sh` and poll `scripts/gate-liveness.sh` (#3473).** Every process an
  agent session spawns inherits the session's `tmux-spawn-<uuid>.scope`, which carries
  `KillMode=control-group` + `SendSIGKILL=yes`: stopping it signals **every task in the cgroup**.
  Cgroup membership is inherited across `fork` and **cannot** be shed by `nohup`, `setsid`, closing
  fds or being reparented to init — measured, both directions, on an equivalent cgroup, where the
  victim died leaving **no signal record at all** (the field symptom of a traceless kill). A subagent
  gets its OWN pane scope. **What is NOT true, and was tested: an agent FINISHING does not tear its
  scope down** — a killed subagent's tickers kept running, orphaned, because systemd releases a scope
  only when its LAST process exits, so a long gate holds its own scope open and outlives the agent
  that launched it. The exposure is to pane/session teardowns (a supervisor recycle, `kill-pane`,
  logout), not to your turn ending. **#3473's "~10 minute ceiling" does not exist**: six instrumented
  tickers (plain `nohup`, `setsid`, renamed argv, harness-background, and two launched by a subagent)
  each ran the full **2400s with zero signals** and self-terminated. The 600s stall watchdog is
  **untested, NOT cleared** — the attempt to induce a stall failed, because this harness version
  **backgrounds** an over-timeout foreground call instead of killing it (the blocker completed
  unmolested, exit 0, after its full 700s). The cgroup mechanism explains
  the lead's `ssh` + `nohup` control completing on the same box and sha (an ssh login gets its own
  `session-N.scope`), but **AC2 landed as a PARTIAL: a sufficient, demonstrated mechanism with
  alternatives ruled out — NOT a confirmed diagnosis** of the original deaths, whose correlation with
  ~10 minutes nothing measured here explains. So **"lanes cannot run a full gate" is RETRACTED** — a
  lane can, detached. `gate-detached.sh` forwards the caller's environment **except the three
  build-flag variables it deliberately drops** — `RUSTFLAGS`, `CARGO_ENCODED_RUSTFLAGS`,
  `RUSTDOCFLAGS`, named in the launch banner's `SKIPPED` list, because `systemd-run --scope`
  inherits the caller's shell and a non-empty `RUSTFLAGS` SUPPRESSES cargo's managed
  `target.rustflags` block, so a lane that exported it once poisons every detached gate it starts
  (that contamination reddened a clean tree and halted the fleet on a P0 that did not exist).
  Everything else is carried across (a transient systemd unit inherits **none** of it, and an
  allowlist of remembered variables fails silently), and it **refuses with exit 69** where it
  cannot deliver a separate cgroup, rather than falling back to a session-scoped launch the caller
  would believe was protected.
  **And the killed-vs-running ambiguity is now answerable without `ps` on the box**: the gate beats
  `<summary-file>.heartbeat` every 20s for as long as it lives (the startup sentinel names the path),
  and `gate-liveness.sh <summary-file> [--run-id <id>]` reports `COMPLETE`(0) / `RUNNING`(2) /
  `STALLED`(3) / `UNKNOWN`(4). Pass `--run-id` whenever you know it — a peer's beat in the same
  checkout otherwise answers about the peer's gate (#2874). A **missing** beat is `UNKNOWN`, never
  `STALLED`, and there is deliberately **no env var** to widen the staleness window or disable the
  beat. **`STALLED` means "no liveness published", NOT "the process is dead"** — a death claim
  (`REAPED`) was built and then DESCOPED after four review rounds each found another way it was
  unsound (a beater can die under a live gate; the reader's `/proc` is not the gate's; two boxes can
  share a hostname), because proving a process dead means proving a negative about a machine you may
  not be on. The replacement needs no process inspection and so is correct on every host, macOS
  included: the gate relaunches its beater at each component boundary, so a live gate whose beater
  alone died recovers to `RUNNING` within one component — re-read before acting, and treat a
  still-`STALLED` run as gone only after **longer than the LONGEST COMPONENT OF YOUR OWN RUN,
  derived from the component table in your own SUMMARY** — never from a constant in prose. The
  figure previously written here, "~850s", was understated by 2.4x (`tooling-tests` measured
  **2073s** on #3473's gate of record #4), and acting on an understated bound makes a closer declare
  a LIVE gate gone and relaunch it, putting two gates on one summary path.
  Full record: `docs/development/lane-gate-execution.md`.
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
- **A gate parser must be colour-immune AT THE PARSE SITE (#3400).** 18 workflows set
  `CARGO_TERM_COLOR: always` (incl. the nightly `gate.yml`) plus `scripts/local/pre-merge.sh`, and
  **colour SURVIVES redirection to a file** (measured: 25 ESC bytes vs 0) — the gate's own mandated
  `> gate.log 2>&1` capture is coloured too, so this is not a tty-only artifact. Cargo colours the
  STATUS WORD and emits the reset immediately after it (`Running<ESC>[0m tests/foo.rs`), so a
  pattern anchored on the status word alone survives while one spanning `<status> <payload>` — the
  literal `Running tests/`, or `warning:` — matches NOTHING. **It breaks BOTH ways, and neither is
  safe**: the cli-tests zero-tests guard reported OK having judged no target at all (a vacuous PASS,
  live on `main` for months, fixed by #1699); the declared-vs-observed reconciliation reported EVERY
  declared target unobserved on a healthy run (a false RED, fixed by #3400). Conversely
  `test result:` / `running N tests` are libtest's, and cargo does not pass `--color` through to the
  harness, so they carry no escapes — safe for a reason that is NOT in the code, which is why this
  is a lint and not a comment. Route every cargo-output parse
  through `_ansi_stripped_log` and read by **redirection, never a pipe** (a piped `while read` runs
  in a subshell and its verdict is discarded — a second, independent silent pass). **This rule is
  DOCTRINE and is NOT mechanically enforced.** A structural lint over the parse sites was built on
  #3400 and **descoped**: its own false-PASS count rose across review rounds (2, 2, 3) and two of
  the last round's three defects were inside the two preceding fix rounds — the same shape, and the
  same ruling, as #3229's removed `census-exclusion:` key, because a guard with known documented
  false-PASSes is worse than no guard, since it invites reliance it cannot support. Mechanization is
  deferred to **#3499**; until it lands, this is a review-time rule, and the standing coverage is
  behavioural (`scripts/tests/test_cargo_output_parsers.sh`, in `tooling-tests`), which pins the
  defect against real code rather than predicting it from source shape — it EXTRACTS each guard from
  the shipped `agent-gate.sh` and runs it, so unrouting one reds the suite instead of greening it.
  `CARGO_TERM_COLOR=never` at the invocation is belt, not the fix; `gate.yml` KEEPS
  `always` — colour is a presentation property of a log for humans, and moving correctness into a
  workflow file 18 files from the parse is a worse coupling than the one being removed.
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
- **A gate script BEHIND `origin/main` cannot certify (#3544).** `agent-gate.sh` is read from
  the tree under test, so a branch cut before a component-set expansion runs the OLD script and
  reports a true `N/N nonpass=0` while being **silent about every component added since**
  (measured: PR #3467's gate would have certified 31 of 35). Merge-cleanliness cannot see it
  (`git merge-tree` returns CLEAN — the skew is semantic), and `required` cannot backstop it:
  `.github/ci-gating-tiers.yml` exempts the CI feature-matrix lane *because the local gate owns
  it*, so each side's coverage is justified by the other's and the component is exercised by
  neither. At the mode dispatch — before the #1825 slot and any component — every mode compares
  its component **SET** (never a line count or blob hash) against a
  baseline **fetched in that same invocation** (a remote-tracking ref is a *cached observable*;
  a stale one returns "no skew" against a superseded `main`), and stamps `component-set:` into
  every SUMMARY: `PASS (36/36 vs origin/main <sha40> via the committed manifest)` — affirmative,
  **naming its baseline sha AND how the baseline was read**; `FAIL-CLOSED (#3544) — this tree is BEHIND …; MISSING: <names>` (remedy: `git fetch
  origin && git rebase origin/main`); `DECLARED (#3544) — this branch REMOVES <names>` when
  `origin/main` IS an ancestor of `HEAD` **AND the components are absent at `HEAD` too** — **loud,
  not fatal**, because the author has nothing to rebase and a guard that reds on correct input
  is the guard agents learn to waive. **ANCESTRY ALONE IS NOT PROVENANCE, and trusting it was a
  reproduced false PASS**: `is origin/main reachable from HEAD?` is not `did this branch's
  committed diff remove the component?`, so deleting one component from the WORKING COPY alone
  yielded a non-fatal `DECLARED` in a certifying mode (a full gate would have certified 35 of 36)
  under a line that asserted committed provenance for an uncommitted edit. A removal still
  PRESENT at `HEAD` is therefore its own fail-closed `UNCOMMITTED` verdict (remedy: commit or
  restore — never rebase), measured against `HEAD`'s OWN component set rather than the proxy "is
  the tree dirty" (which would red every mid-edit branch and still prove nothing on a clean-but-
  stale one); an **uncommitted ADDITION still PASSes**, because extra components are never skew.
  **A CHECK MUST BE INSIDE THE WINDOW IT CERTIFIES — NOT BEFORE IT, NOT AFTER THE HARM (roborev
  job 290).** The mirror of this issue's earlier ruling ("a check placed AFTER the harmful effect
  can only report it"), and the same family as the two sha-equality failures. The component-set
  pre-flight ran BEFORE `acquire_gate_slot`, and `_tree_recapture_after_slot` then RESET the
  certification window to the tree present when the slot was granted — so an edit made WHILE QUEUED
  became the new starting tree under a STALE `component-set:` verdict. The recapture is deliberate
  and stays; the pre-flight is **repeated inside the window** (the earlier call is kept, because it
  is what stops an uncertifiable run from queueing or compiling at all). **Second half, one
  asymmetry down:** the LOCAL manifest is verified against the LOCAL declaration every run, so it is
  a checked claim — while HEAD's manifest was TRUSTED, letting a stale one at HEAD excuse an
  uncommitted removal as `DECLARED`. Provenance now reads HEAD's committed `COMPONENTS`
  **declaration** as data and does not consult HEAD's manifest at all: **remove the second source
  rather than reconcile it.**
  **AND EVERY INPUT THE CHECK REASONS ABOUT MUST BE INSIDE THAT WINDOW TOO (roborev jobs 292–294) —
  BUT "IS THE CODE I AM EXECUTING THE CODE I CERTIFY" IS NOT ANSWERABLE FROM INSIDE THE RUNNING
  PROCESS, AND IS SPLIT OUT TO #3705.** Being inside the window yourself is not enough if you compare
  against a snapshot taken outside it: `COMPONENTS` is an array bash loaded before the queue, so a
  script that GAINED a component while queued was validated against the OLD array (292); and change a
  component's **executor function** while queued and the recaptured tree becomes the certification
  window while the process keeps running the definitions it loaded before it (293). **The RULE stands.
  The MECHANISM built for it does not**: a whole-file startup digest of `$GATE_SELF` (with the field
  comparisons demoted to the message) is REMOVED, because **bash parses a script INCREMENTALLY** — the
  digest is taken only after thousands of lines are already parsed, so an atomic replace before that
  point leaves bash executing the OLD inode while the digest reads the NEW path (294). Answering the
  question needs a **bootstrap/re-exec handshake**, i.e. a change to how `agent-gate.sh` STARTS UP,
  and it cannot ride inside a component-set comparison. **Five consecutive rounds landed in that one
  mechanism (290/292/293/294) while #3544's own property produced one finding in five** — the standing
  signal to SPLIT rather than carve the same place again. What stays here: job 290's REPEAT of the
  pre-flight after the slot is granted (cheap, and it makes the component-set verdict current with
  respect to the recaptured tree), and job 285's MANIFEST mode validation. The gate-script symlink
  refusal went to #3705 with the check it belonged to; a `gate-script-*` kind no longer exists.
  **A SYMLINK IS A BLOB, AND A GRAFT OUTLIVES `--no-replace-objects` (roborev job 285).** Two
  false-green routes, both closed by moving rather than flagging. (1) The presence probe accepted
  every `blob`, but a symlink IS one — the difference is the MODE (`120000`) — so the two halves of
  the manifest check read DIFFERENT DOCUMENTS: the working-tree validation FOLLOWS the link and
  sees a full manifest while `git show <rev>:<path>` prints the link's TARGET TEXT, so
  `agent-gate.components -> fmt` validated locally and published a ONE-COMPONENT baseline. The mode
  is now validated on both halves. (2) `$GIT_DIR/info/grafts` rewrites parentage and
  `--no-replace-objects` does **not** disable it (measured: no → YES → YES across
  before-graft/plain/`--no-replace-objects`), so on the object-REUSE path — where ancestry still ran
  in the live repository — a graft could reclassify missing components from fatal `BEHIND` to
  non-fatal `DECLARED`. **Ancestry now runs in the isolated repository on BOTH paths**, live objects
  exposed only through an alternate; the reuse path keeps what it was for (no fetch, no transfer)
  and loses only a `mktemp`+`git init`. **The pattern the owner named while ruling on it: every
  live-repository read preserved for speed has turned into a route** (round 16's partial-clone lazy
  fetch, now grafts) — so a third finding there should remove the reuse optimisation rather than
  carve it again. **And a test-suite lesson from the same round: a span-replacing edit silently
  deleted FOUR cases and the suite reported `failed: 0` at 102 instead of 105 for a whole round —
  a green tally over a shrunken suite is #3544's own subject inside its own test file. That suite
  now asserts a CASE FLOOR**, the idiom `test_agent_gate_summary.sh` already used.
  **THE SHARED OBJECT STORE IS TRUSTED, NOT VERIFIED — DECLARED IN THE EMITTED LINE, AND OWNED BY
  #3746 (roborev job 311; lead ruling on `REQ-3544-OBJTRUST`).** Git does not rehash a packed
  object against the id it was asked for on an ordinary read, and on this fleet **every lane on a
  box is a worktree of ONE shared `.git`** (measured: `/data/lanes/repo/.git/objects` for
  lane-3544, lane-3473 and lane-3629 alike), so a peer lane planting a forged pack/index can make
  a canonical sha resolve to a shortened manifest — a **false PASS**, and a NON-INVOKER route,
  hence a defect. **The recorded "a third finding here should REMOVE the reuse optimisation"
  ruling does NOT dispose of it, because removal does not CLOSE it:** the ancestry walk and the
  provenance leg read HEAD's **committed** content, which has no source other than that store —
  the working tree cannot substitute, since `UNCOMMITTED` exists precisely to compare against what
  is committed — so a forged HEAD object still turns `UNCOMMITTED` (fatal) into `DECLARED`
  (non-fatal) after removal, while charging every `--lite` round for the privilege (measured
  2026-08-31: **3.41 s / 93 MB** full, **3.58 s / 45 MB** at `--depth=1` — shallow is NOT cheaper,
  it still ships the tip's whole tree). **A permanent tax for a half-closure is the guard agents
  learn to waive**, and a bounded re-hash of the consumed objects is the FOURTH carve in this
  family. So the boundary is **DECLARED**: every baseline-bearing verdict line ends by naming the
  object provenance (`REUSED` from the shared store / `FETCHED` from the canonical remote / `NOT
  RECORDED`) plus `store TRUSTED, not verified (#3746)`. **A check that claims nothing false is
  worth more than one claiming a closure it does not deliver** — the same move the roborev
  waiver's threat model makes where a dependency cannot be removed. The declaration is folded into
  the ONE `src_note` suffix eleven printf arms already consume, never appended per-arm, and the
  self-test pins it as a **closed set of three renderings** by string equality: pinning one
  literal would red on correct input (which clause fires depends on whether this box's store
  already held the commit), pinning nothing would let a wording pass delete it. **#3746 may
  conclude the subject is not this pre-flight at all but the infrastructure decision that lanes
  share an object store** — a peer able to plant objects there is a hazard to every gate on the
  box, not to one component-set comparison.
  **STOP RENDERING THE VALUE, DO NOT SANITISE IT AGAIN — AND A FIX THAT ADDS A RESOURCE INHERITS
  THAT RESOURCE'S LIFETIME BUGS (roborev job 282).** Two closures. (1) The rejected-origin
  diagnostic was the FIFTH finding in one family — raw URL rendered (227) → redacted but not
  flattened (234) → flattened but not redacted (239) → scheme-only redaction (264) → **query
  strings verbatim and multi-`@` authorities redacted only to the first `@`** (282). Every fix
  improved the sanitiser, which is the "rarer delimiter" the mechanism ruling warns against, so the
  URL is **no longer published**: the diagnostic names the AXIS it was rejected on, plus the
  normalised identity **only when that identity is itself grammatically clean** (a
  `…/repo?token=SECRET` normalises to a value CARRYING the query, so the shape is checked rather
  than assumed). Two self-inflicted defects on the way, both worth knowing: a fall-through printed
  `${v%%:*}` which with no colon **is the whole value**, reproducing the finding; and reducing the
  NORMALISER's output instead of the RENDERED text made every local path normalise identically, so
  a canonical identity pinned to a local path matched **any** local path — **the normalised value
  is a COMPARISON KEY, not a diagnostic string.** (2) Round 17's own fix created the owned
  supervisor and never registered it with the signal path — the third instance of one family
  (round 9 register-before-create, round 14 clean-up-on-signals), i.e. **fixing a resource-lifetime
  bug added a resource with the same bug**. Any owned child is now registered the moment it exists
  and cleared the moment it is reaped, and cleanup reaps it BEFORE deleting the files it could
  otherwise recreate.
  **NEVER SIGNAL A PROCESS GROUP YOU NO LONGER OWN — AND OWNERSHIP ENDS AT REAP, NOT AT EXIT
  (roborev job 279).** The bounded runner's watchdog arm backgrounded the COMMAND, so the pgid was
  the command's pid, and after TERM + a 1s grace it sent an unconditional `kill -KILL -$pid` — by
  which time bash may already have REAPED the leader, releasing that id. On a four-lane box the
  group that inherits it is most likely **a peer lane's gate** (this repo has the incident: a
  pattern-based `pkill` killed a peer's gate at component 28 of 30). The leader is now a
  **supervisor kept alive on purpose** — it runs the command, records the status to a file with a
  completeness marker, then parks (bounded at `secs+5` so a SIGKILLed gate leaves nothing) — so
  every signal targets an id we still hold. Two things fall out: a successful call now reaps its
  STRAY descendants, and **the race itself cannot be tested** (pid reuse is not controllable), so
  the coverage is the observable before/after difference plus a **structural** assert of the
  ownership invariant, labelled as such rather than dressed up as behavioural. Related, from the
  same round: `[ -z "$(find …)" ]` collapses "the scan FAILED" onto "no match" — a three-valued
  signal read two-valued — and this repo LINTS for that shape (`1699-find-tristate`).
  **THE ALLOWLIST HAS TO REACH THE SITES A LATER CHANGE ADDS (roborev job 276).** The migrated
  object reads ran under a bare `env`, inheriting the caller's environment — the round-13 hole
  re-opened at the NEW sites, not a new route: an inherited `GIT_DIR` points a read at another
  repository, and `GIT_CONFIG_COUNT`/`GIT_CONFIG_PARAMETERS` injects a promisor or an `insteadOf`.
  Every git call in the pre-flight now runs under `env -i` + the ONE allowlist, with only
  location-specific values (the alternate) layered on top — **including the STATE probes**, since
  injected config could have made a real partial clone look non-partial and re-opened the fast
  path. Two corrections came with it: **a config file does NOT keep a URL out of every argv** —
  git passes the configured URL to a transport HELPER, whose command line then carries the token —
  so a credential-bearing origin is now **refused** (userinfo must be absent or exactly `git`;
  refusing ALL userinfo red the standard `ssh://git@github.com/…`, a false FAIL on correct input — **that ssh example is now moot, since job 296 refuses ssh forms outright; what KEEPS the rule is CI's `https://x-access-token:<TOKEN>@github.com/…`**);
  and **a specified control must be required to have WORKED** — the `chmod 600 … || true` on the
  isolated config is now fail-closed with the resulting mode VERIFIED (`find -perm 600`, since
  `stat` is GNU-vs-BSD incompatible), because "chmod exited 0" and "the file is 0600" are
  different claims.
  **AND A LOCAL READ CAN BE A NETWORK OPERATION (roborev job 268).** In a PARTIAL clone,
  `ls-tree`/`show`/`cat-file` answer a missing object by fetching it from the **promisor remote**,
  under the live repository's **local config** — so `url.*.insteadOf` plus an enabled external
  protocol executes a remote helper, and the lazy fetch writes objects into the shared store. That
  was the THIRD route of one family (`insteadOf` on the fetch, `ext::` on the transfer hop, the
  promisor), and per-call-site suppression had failed each time — so **every baseline/HEAD object
  read and the ancestry walk moved INTO the isolated scratch repository**, with the lane's object
  directory supplied as an alternate (pure object storage: no config, hence no promisor, no
  `insteadOf`, nothing for a helper to be invoked from; a missing object there is a named refusal).
  Ancestry compares against **HEAD resolved to a sha in the checkout**, because inside the scratch
  the ref `HEAD` means the SCRATCH's own unborn HEAD. **The fast path is gated on the clone not
  being partial** (`_component_set_is_partial`, three-valued, UNKNOWN ⇒ treated as partial: the
  conservative branch costs a fetch, not correctness), because that path reads the baseline in the
  live repository — and `cat-file -e` cannot even probe presence there: measured with
  `GIT_NO_LAZY_FETCH=1` set, it answered 0 for a blob whose `show` then FAILED, since it answers
  about PROMISED objects. `GIT_NO_LAZY_FETCH=1` is carried as a **belt, not the control** (git ≥ 2.36;
  an unset variable does nothing silently, which is exactly why it cannot be the control).
  **UNTRUSTED REPOSITORY STATE IS BIGGER THAN CONFIG (roborev job 264).** Closing git's *config*
  sources and treating "untrusted repository state" as closed with them left three holes, and the
  shape of the error is the recurring one — one axis closed, space declared done. **(1) Replacement
  refs**: `refs/replace/<sha>` transparently substitutes another commit, so the pre-flight reported
  the CANONICAL sha while reading a FORGED, smaller manifest, and PASSed — the worst pairing, since
  the audit trail looks right. Now `GIT_NO_REPLACE_OBJECTS=1` in the allowlist plus
  `--no-replace-objects` on every lane-local object read. **(2) The transfer hop could EXECUTE**:
  `git fetch` in the LIVE repository reads its LOCAL config (only the *environment* is sanitisable
  — a `.git/config` is a file), so a local `url.*.insteadOf` + `protocol.ext.allow=always` rewrote
  the scratch path to an `ext::` helper and ran commands DURING the fetch, before the sha
  comparison that was meant to make the hop "untrusted but safe". **A check placed AFTER a harmful effect can only REPORT it, never PREVENT it — so if the harm is
  EXECUTION, the control must be that the execution cannot be REACHED, not that its result is
  detected** (lead ruling, round 14; the sha-equality assert sat downstream of the fetch it was
  meant to validate). The corollary for tests: assert UNREACHABILITY, with a positive control
  proving the attack executes in a plain repository, or the green means nothing. A protocol allowlist is not expressible either
  (`-c protocol.allow=never` loses to a more specific local `protocol.<name>.allow=always`, and
  the helper-name space is whatever `git-remote-*` is on PATH). So there is **no import at all**:
  the scratch object store is made visible via `GIT_ALTERNATE_OBJECT_DIRECTORIES` — an object
  SOURCE, not a transport — and NOTHING is written into the shared `.git` (no pack, no ref, no
  `FETCH_HEAD`). Safe for the reason the transport was not: every read is BY A SHA whose provenance
  is the isolated chain, and objects are **content-addressed**. `baseline-transfer-mismatch` and the
  private-ref machinery are gone with it — the class is ELIMINATED, not detected. **(3) The scp-form
  leak, third instance of one family** (raw → flattened-not-redacted → scheme-only redaction):
  `TOKEN@github.com:owner/repo` was canonical because the normaliser dropped userinfo before
  comparing, and an ssh error then echoed it into the SUMMARY. Fixed by **narrowing what is
  accepted** (scp userinfo must be exactly `git`) rather than widening the scrubber again — though
  the scrubber covers scp form too, since a REJECTED value is still rendered. **(4) Cleanup on
  SIGNALS**, the second axis of round 9's "cleanup registration precedes resource creation": bash
  runs no EXIT trap for a signal with its default disposition, so INT/TERM/HUP now have handlers,
  installed before the resources exist, saving and restoring the caller's.
  **THE ISOLATED HOP'S ENVIRONMENT IS AN ALLOWLIST, AND THE OBJECTS ARE FETCHED ONLY WHEN
  ABSENT (roborev job 258).** Neutralising `GIT_CONFIG_GLOBAL`/`GIT_CONFIG_SYSTEM` and stopping
  there left the "isolated" hop inheriting `GIT_CONFIG_COUNT`/`KEY_*`/`VALUE_*`,
  `GIT_CONFIG_PARAMETERS` and `GIT_TEMPLATE_DIR` — each measured to redirect a fetch via
  `url.<attacker>.insteadOf`, the template by seeding the *new* repo's own LOCAL config. A HOP 1
  redirect is worse than a hop 2 one: the sha the isolated hop observes and the commit
  transferred in then BOTH come from the attacker, so the equality assert compares two values
  that AGREE and emits a **false PASS**. That was **enumerating one axis and declaring the space
  enumerated** — so every isolated git call now runs under **`env -i` plus an allowlist**
  (`ADMIT` what git needs to REACH and AUTHENTICATE to the remote, each entry carrying its
  reason; `CLEAR` everything that can change WHAT it fetches or WHAT it runs), which makes new
  git environment variables cleared BY DEFAULT rather than needing to be discovered. Lane-local
  reads are deliberately NOT wrapped: the only value needing provenance is the SHA, and
  everything addressed by it is **content-addressed**. **And the baseline sha now comes from a
  ref ORACLE (`git ls-remote`), with objects fetched only when this repository lacks that
  commit** — measured 3.74 s / **92 MB of full history per invocation** → 0.51 s / no transfer;
  `--filter=blob:none` was rejected on measurement (it leaves the manifest blob absent exactly
  when `main` changed it, failing a correct tree). The ref value is still read live, which is
  what "fetched in THIS invocation" is about; the oracle's output is remote-controlled text and
  is VALIDATED (`baseline-ref-unparsable`), never merely parsed.
  **THE BASELINE IS READ AS DATA; NOTHING FETCHED IS EVER EXECUTED (REQ-3544-01, lead ruling).**
  The first design derived the baseline set by extracting `origin/main:scripts/agent-gate.sh` and
  RUNNING it (`bash <fetched> --list`). **Six of that mechanism's seven High-severity findings
  traced to that one decision**, and its three fixes each moved the hole one layer outward (a
  symbolic remote name ⇒ the validated URL ⇒ the URL in `argv`) — the signature of a **shared
  channel between data and control**, where the standing ruling (#3312) is to REMOVE the channel,
  not to choose a rarer delimiter. So: the branch side is the **in-process `COMPONENTS` array**
  (what this run will actually dispatch), and the baseline side is **`git show
  <sha>:scripts/agent-gate.components`** — a committed DATA manifest, parsed under a CLOSED
  grammar (one name per line; blank lines and `#` comments skipped; anything else, INCLUDING a
  name with leading/trailing whitespace, is a NAMED refusal — a parser that trims is a parser
  that guesses). **What this CONVERTS the six findings into, rather than eliminating:** a
  redirected or hostile baseline now yields a **wrong component list, which the comparison itself
  detects**, instead of arbitrary code execution with the developer's credentials. Everything
  built for the old mechanism is **KEPT as defence in depth** — identity/transport/host/path
  pinning, the isolated fetch (URL written into a 0600 config by a shell builtin so it never
  enters `argv`), the verified transfer hop, the mode-dependent bound, shallow ancestry, the
  redact+flatten detail path. **The local manifest is ASSERTED against the running array on every
  run** (`manifest-missing`/`-garbage`/`-stale`, fail-closed, ORDER included), and that assert is
  what makes a manifest baseline trustworthy at all: without it the file is an unverified claim,
  and a branch that grew `COMPONENTS` without regenerating the manifest would — once merged —
  leave `main`'s manifest SHORT, so every later branch would compare against a too-small baseline
  and silently excuse real skew. Regenerate with `{ sed -n -e '/^[^#]/q' -e p
  scripts/agent-gate.components; scripts/agent-gate.sh --list; }` and commit it.
  **One TRANSITIONAL fallback, also data-only, and it is UNREACHABLE BY ASSERTION rather than by
  reasoning:** the baseline's tree is **probed first**, as its own step, with **three** outcomes —
  `present` ⇒ the manifest and NOTHING ELSE (every failure of that read is an ERROR; the textual
  path is a **hard refusal** here), `verified-absent` ⇒ the gate script's **single-line top-level
  `COMPONENTS=(…)` declaration extracted AS TEXT** (never executed), `could-not-tell` ⇒ **REFUSE**
  (`baseline-probe-unmeasured`). "The fallback is self-limiting" was true and **not enough**: that
  is a property someone reasoned about and nothing measured, so a refactor or a deleted manifest
  would silently re-enable the brittle path — a pass derived from the ABSENCE of a bad signal.
  **`git show` cannot answer the question**: its non-zero exit conflates "no such path" with "bad
  object" with "unreadable repository", so absence is never inferred from it; `git ls-tree <rev> --
  <path>` separates them affirmatively (rc 0 + an entry / rc 0 + NO entry / rc ≠ 0), and a non-blob
  entry is its own refusal. The payoff is **mechanical expiry instead of trust**: once the manifest
  is on `main` every baseline measures `present`, so path 2 is dead code that any attempt to enter
  ERRORS. The extractor refuses loudly on any shape it does not recognise and **NAMES it** ("is not
  a SINGLE-LINE literal"), so a future reflow on `main` — which would refuse for **every branch at
  once**, fail-closed rather than a false green — surfaces as that sentence and not as a mystery.
  Every baseline-bearing verdict line ends by naming its baseline source, so use of the fallback is
  visible rather than inferred.
  And `origin` must **NAME the canonical upstream**, HOST INCLUDED
  (`github.com/pmcfadin/cqlite`, one hard-coded literal, EXACT equality after normalising the
  spellings git accepts — scheme forms, scp-like, userinfo, an ssh port, `www.`, `.git`, case):
  `origin` merely EXISTING made `git remote set-url origin <anything>` a git-config-shaped
  opt-out, and it fires BY ACCIDENT in the fork workflow, where a contributor's fork `main` is a
  stale baseline stamped `PASS`. **An OWNER/REPO-only match is NOT enough, and "err toward
  accepting an ambiguous host" was WRONG here** — it accepted `evil.example/pmcfadin/cqlite` and,
  needing no hostile host at all, ANY LOCAL PATH ending in those two segments — which, while the
  pre-flight still RAN the baseline's copy of the gate, admitted arbitrary code and not merely a
  wrong baseline (identity and execution were one concern, not two). Under REQ-3544-01 what a
  loose identity buys is a baseline of unknown PROVENANCE, from which no PASS may be derived, so
  the check stays exactly as strict — as defence in depth rather than as the only thing standing
  between a re-pointed remote and code execution. Anything unverifiable from the string (an ssh alias, a mirror, a local
  path, `file://`, a look-alike host) is a NAMED non-PASS, as is a URL-less `origin`. **And the URL grammar is CLOSED AXIS BY AXIS, because three rounds were "too permissive" in
  a NEW place each time** (no host; host but no transport; then `http://`/`git://` accepted):
  transport (**`https://` ONLY since #3544/job 296** — `http://` and `git://` authenticate
  nothing, so an on-path impersonator supplies the objects this run certifies against; when the
  rule was written those objects were EXECUTED, which is why it was a High. **`ssh://`,
  `git+ssh://`, `ssh+git://` and scp-form `git@host:path` WERE accepted and are now REFUSED
  (`ssh-transport:<form>`)**: the isolated environment must admit `HOME` for key and
  `known_hosts` discovery, so OpenSSH still honours **`~/.ssh/config`**, where a
  `Host github.com` rule rewrites `HostName` or runs `ProxyCommand`/`Match exec`. That is a
  redirected baseline AND arbitrary execution behind a URL string that passes the identity check.
  It is IN MODEL because HOME IS SHARED — every lane runs as one user with a writable
  `/home/ubuntu`, so the planter is a PEER LANE, not the invoker (an invoker editing their own
  config is out of model; a non-invoker route is a defect — same shape as #3617). It was met by
  **DESCOPE, not hardening**, under the standing ruling on this family: a bounded residual was
  unavailable because `ProxyCommand` EXECUTES, and the usual mitigation — a redirected baseline
  degrades to a wrong component list the comparison detects — does not reach a harm that lands
  during TRANSPORT, before any comparison. Measured cost: nil, every lane and CI already use
  https; an ssh-form checkout now fails closed with the remedy named), host,
  port (default only), path, and userinfo (ACCEPTED — GitHub Actions writes
  `https://x-access-token:<TOKEN>@github.com/…`, so rejecting it would red a legitimate CI
  checkout — and therefore REDACTED everywhere it is rendered, since SUMMARY blocks get pasted
  into PR comments). Each axis has one stated rule beside the check; a new variant would be a
  change to git's URL syntax, not a gap. **The baseline is fetched into a PRIVATE per-run
  `refs/worktree/…` ref, never `FETCH_HEAD`**: `--refmap=` removed the shared *tracking* write
  and left `FETCH_HEAD`, which is itself one shared mutable file a concurrent fetch overwrites
  between the fetch and the read — the run would then compare against a commit it never fetched. **And `--is-ancestor`'s rc 1 is itself three-valued**: in a SHALLOW clone it
  also means "the connecting history is absent", so rc 1 is definitive only in a repo PROVEN
  complete (`unknown` shallowness ⇒ INDETERMINATE) — otherwise a legitimate committed removal
  in a shallow checkout reds as BEHIND. **Corollary
  for tests**: hermetic fixtures use local origins, so they SUBSTITUTE THE ARTIFACT — one shared
  helper rewrites the canonical literal in the fixture's own scratch copy of the gate and verifies
  the pin took (`scripts/tests/lib/agent-gate-canonical-pin.sh`) — never a settable seam. The
  first design let local paths through so the fixtures would work, i.e. **the test hook and the
  vulnerability were the same fact**; and the check REGRESSED three suites whose local origins it
  rejected (`test_agent_gate_delta.sh`'s two real `--delta` fixtures stopped at the pre-flight
  instead of reaching their REFUSED paths — a `tooling-tests` FAIL invisible to `--lite`). Or `FAIL-CLOSED … baseline NOT measured (<kind>)` for a
  failed fetch/absent `origin`/an empty or ungrammatical baseline manifest/a baseline declaration
  that cannot be read as text/an unreadable baseline-or-`HEAD` set/**a host on which the probe
  cannot be BOUNDED** (in which case the fetch is not run at all — an unbounded fetch
  could hang `--lite` on a stall or an auth prompt, and a missing capability must not inherit
  the permissive branch) — **never a SKIP and never a fallback to an empty baseline**, which
  would excuse every branch. A branch-only
  component is NOT skew. Fail-closed in the **certifying** modes (full, `--delta`); `--lite`
  and `--only` stamp the same line `ADVISORY-*` and cannot fail on it. **No opt-out env var,
  and none may be added** — rebasing is always available, so an escape hatch could only buy a
  vacuous green.
- **A run whose worktree mutates MID-RUN cannot certify (#2926).** Every mode captures a tree
  identity at start, re-verifies it at each component boundary + the terminal emit, and FAILs closed
  with `tree-integrity: FAIL (tree-mutated-midrun; head <a>→<b>; changed: …)`. Every SUMMARY carries
  `tree-start:`/`tree-end:`/`tree-integrity:`, so **closers verify `tree-integrity:` alongside
  `RESULT:`** — a mutated-mid-run block is not a certification and cannot be pasted as one. The
  block's `commit:`/`dirty:` are derived from that verified capture, never a fresh emit-time git
  read. No env var bypasses it; remedy is to re-run on a stable tree (don't edit a worktree while
  its gate runs).
- **Every component line NAMES the feature matrix it ran, in ALL THREE modes (#3453).**
  `core-tests: PASS (412s)  [test cqlite-core --features cli-helpers]` — read as
  `<subcommand> <scope> <features>`, one entry per distinct invocation, `xN` for repeats. A bare
  `PASS (412s)` could not distinguish a run that certified the OTLP stack from one that never
  enabled it, which is this issue's whole subject. It is **DERIVED, never curated**: `cargo` and `env` are shell FUNCTIONS in the gate, so a
  matrix is described from the REAL argv about to execute. **AND IT RECORDS EXECUTION, NOT INTENT.**
  The eight components whose cargo calls live in a single-quoted `bash -c` body (core-tests'
  nextest branch, memory-budget, integration-tests, write-tests, cli-tests,
  compaction-byte-parity, minimal-build, smoke) first declared their sets in the PARENT, before the
  child ran — so `cli-tests: FAIL` named BOTH of its feature sets even when Pass 1, or the
  fail-closed target derivation above it, died before Pass 2 started, and write-tests claimed the
  same set `x3` after failing on the first of three `&&`-chained passes. **A failure summary that
  claims an invocation which never occurred is affirmatively false, and strictly worse than
  silence** — it is what stops the next person looking. Each body now calls the EXPLICIT recorder
  `_fm_observe_child` on the line immediately BEFORE each cargo command, from the same hoisted
  package/feature variables the argv is built from, so a short-circuit records nothing later. The
  cargo/env INTERCEPTORS stay deliberately NOT `export -f`-ed — exporting an interceptor would make
  every bash DESCENDANT record, so `tooling-tests` (which runs nested agent-gate self-tests) would
  attribute a nested run's cargo to itself — while `_fm_observe_child`, which intercepts nothing and
  fires only where a body calls it by name, IS exported (with the gate's own `_fm_describe_cargo`,
  so there is no second formatter to drift). It **never renders blank**: `[UNDECLARED]` (cargo
  expected, nothing observed), `[no-cargo]`, `[via <driver>: feature set NOT observed]`,
  `[cargo not observable: <why>]`, or a named SKIP / FAILed-before-its-first-cargo /
  never-reached-its-driver state; a long list abbreviates as `33:a,b,c,+30 more`, never
  a silent truncation. **A driver we cannot see is NAMED, not guessed** — `python-bindings`,
  `node-bindings` and the `--lite` scoped-tests PYTHON TIER (whose maturin build runs in a child
  process) render `via <driver>: feature set NOT observed`, ADDITIVELY beside the rust sets a mixed
  diff also observes (`[test cqlite-core --features cli-helpers | via maturin: feature set NOT
  observed]`): "nobody said" and "known to be indirect, therefore unobservable" are different facts
  and only one of them is a defect.
  **AND THE CLASS DECIDES WHAT MAY BE CLAIMED — three rules, from one family of findings (roborev
  job 273).** (1) A component whose cargo runs ONLY IN A CHILD PROCESS is **never class `cargo`**:
  the interceptors are unexported by design, so `cargo` means "observable in this shell (or
  self-recorded from a `bash -c` body)". `tooling-tests` was declared `cargo` while its only cargo
  runs inside ~60 nested test scripts, so a PASS read `[UNDECLARED]` and a FAIL could claim it
  "FAILed before its first cargo invocation" after a child `cargo build` really ran — hence the
  fourth class `unobservable:<why>`, which asserts NOTHING in either direction and takes no
  SKIP/FAIL note. (2) An `indirect:<driver>` component must **RECORD whether its driver was
  REACHED, from an explicit signal** (a build-verify rc, or a recorder call on the line before the
  driver runs) — never inferred from the terminal status: `python-bindings` can die in venv/pip
  before maturin and `node-bindings` in `npm ci` before `npm run build`, and both used to claim an
  unobserved cargo run. An indirect component with NO record renders `UNDECLARED` **naming the
  driver** — a visible recording gap, not a claim. (3) The misclassification is now
  **MECHANICALLY DETECTABLE**, because the census that missed (1) READ THE TABLE instead of
  exercising it: every `cargo`-class component is RUN under `--only` with a recording shim `cargo`
  and an `UNDECLARED` annotation is a FAIL, while a component that cannot be exercised without
  recursion (`tooling-tests` runs the guard) must be declared non-`cargo` — also a FAIL.
  **Observation beats declaration** — a component
  declared `no-cargo` that IS observed running cargo renders the observed sets plus
  `!declared-no-cargo`, so a mis-declaration self-corrects. Guard (hermetic, in
  `tooling-tests`): `scripts/tests/test_agent_gate_feature_matrix_annotation.sh` — every
  `COMPONENTS` name must resolve to a declared class (a new component cannot join with a blank
  matrix), all six emit sites must route through the one renderer, the DECLARED matrix of each
  `bash -c` component must equal the argv that ACTUALLY EXECUTED under a recording PATH-shim
  `cargo` (described through the gate's own `_fm_describe_cargo`, never re-derived), and the same
  differential is re-run under a FAILING shim, where each body must name exactly the one invocation
  it reached — with the short-circuit proved by measurement (strictly fewer invocations than the
  passing run) rather than assumed.
- **Every SUMMARY's `cpu-budget:` line says WHERE the slot cap came from (#3414):**
  `max-concurrency=N(pinned|default|invalid|clamped)`, the same idiom as `build-jobs=N(derived|caller)`
  beside it. `pinned` = a valid `CQLITE_GATE_MAX_CONCURRENCY` >= 1 used verbatim; `default` = the var
  is UNSET so N is the #1825 formula; `invalid` = it was EMPTY or non-numeric and was silently
  discarded for the formula; `clamped` = it was a valid integer < 1 and was silently raised to 1.
  Read `N(default)` on a fleet box as **the pin is not provisioned** — `3` and `3 because nothing set
  it` are different operational facts, and the second one is what ran unseen for months. `invalid`
  and `clamped` exist because `${VAR:-dflt}` cannot tell unset from set-empty (`${VAR+set}` can), so a
  mis-set variable was textually identical to a healthy defaulted box.
  **THE REMEDY DIFFERS BY TOKEN, and getting that wrong sends an operator in a circle.** A
  `default` box has NO pin line, so `bash scripts/bootstrap-agent-machine.sh --fix-gate-pin`
  (or `--yes`) persists one. An `invalid`/`clamped` box ALREADY HAS the line, with a bad value —
  and bootstrap deliberately never rewrites an existing value (a box running >1 gate on purpose
  must not be clobbered), so re-running it is a **silent no-op**: fix the VALUE in
  `/etc/environment` by hand. Bootstrap says the same thing at the same fork, as
  `gate-pin: NOT-HONOURED`.
- **Every component line states WHAT IT VERIFIED, not just how long it took — and a component
  that verified NOTHING cannot report PASS (#3625).** `PASS (0s)` was indistinguishable, in a
  pasted block, from a component that did nothing. A duration is a PROXY for work; a COUNT is
  the work. So `_fm_summary_line` now appends a census suffix — `{verified: 3562 tests passed}`,
  `{verified: 2 test binaries built/verified}`, `{no census — <declared reason>}`,
  `{census NOT-MEASURED: <reason>}` — plus ONE aggregate `census:` line per block. **The
  measured oracle behind it, and the answer to the issue's two-run comparison: cargo caches
  COMPILATION, never test EXECUTION** — a WARM `cargo test` re-prints `test result: ok. N
  passed` and a WARM `cargo test --no-run` still prints one `Executable ` line per binary — so
  those `0s` lanes DID re-verify their subjects and the count was in the log all along; nothing
  put it in the SUMMARY. A `libtest`/`compile`/`both` lane whose measured subject count is ZERO
  is recorded as **`VACUOUS`**, a fourth component-status token beside PASS/FAIL/SKIP, and it
  fails the run. **That required making every aggregation AFFIRMATIVE**: `[ "$st" = FAIL ] &&
  OVERALL=FAIL` failed only the ONE named bad token, so every other value — an unrecognised
  token, an empty result file, VACUOUS itself — took the permissive branch; `_status_is_nonfailing`
  is now a closed set (PASS, SKIP) and everything else fails. Two states are DECLARED and
  deliberately NON-FATAL, because a lane that reds on correct input is the lane agents learn to
  waive: `NOT-MEASURED` (an unreadable log, a failed ANSI strip, an unrecognised driver report)
  and `gap:<reason>` (14 components today — fmt, clippy, all-features-check, the shell/python
  guards, smoke, tooling-tests — each PRINTING its reason every run). Neither is ever read as
  verified: the aggregate line counts them separately and always as `N RECOGNISED`, never a bare
  `N`, and it DECLARES its own non-exhaustiveness, because the gap set is curated. One asymmetry
  worth knowing: for a cargo lane the subject markers are cargo's OWN guaranteed output, so their
  absence really does mean nothing ran — but for `indirect:<driver>` (python-bindings/pytest,
  node-bindings/jest) an ABSENT tally is `NOT-MEASURED`, since a third-party report format is not
  ours and its absence is a measurement failure, not proof of vacuity. **#3400 HAS A SECOND
  DIMENSION, AND QUIET IS IT**: that rule is about a cargo-output parse keyed on a
  PRESENTATION property, and an anchor can be perfectly colour-immune while still depending
  on another one. `CARGO_TERM_QUIET=true` in the environment, or `[term] quiet = true` in any
  `.cargo/config.toml`, suppresses EVERY cargo status line — measured: a
  `cargo test --lib --no-run` under quiet emits a COMPLETELY EMPTY log — while leaving
  libtest's `running N tests`/`test result:` untouched. Neither is visible at the call site,
  so a box carrying either would have made `feature-iso-parquet` and `minimal-build` measure
  a *zero* `Executable` count and read VACUOUS on every gate, fleet-wide, on correct input.
  The fix is THREE-VALUED, not an env override (#3400 records that moving correctness into a
  setting far from the parse is the worse coupling): the tally reports
  `<Executable lines> <cargo status lines>`, and a log with **no cargo status output at all**
  is `NOT-MEASURED (suppressed)` while only a log that demonstrably carries status output
  *and* zero `Executable` lines is a real `ZERO`. Generalise: **"the marker is absent" and
  "the marker could not have been printed" are different facts, and a fatal state may only
  be derived from the first.** Declaration site:
  `_census_kind` (a CLOSED set; an undeclared component is a named FAIL, so a new component
  cannot join with a blank census) — **and that guarantee is only as strong as WHERE the
  state is judged**: the verdict coupling used to return every non-`PASS` status untouched,
  so `UNDECLARED` was not fatal when the component SKIPped, i.e. the completeness rule failed
  exactly on a NEW component that SKIPs on the box where it is first run. The census RECORD
  is now judged before the run's status (an unsound record is a fact about the TABLE, not
  about this run), and only then does the status decide. **BUT A STATIC DECLARATION IS NOT
  ALWAYS POSSIBLE, AND
  ASSUMING IT WAS COST A HIGH**: `scoped-tests` was declared `both`, and a diff confined to
  `bindings/python/**` dispatches NO cargo at all (`classify_scoped_plan` diverts `cqlite-py`
  and the `cqlite-core` fallback is deliberately guarded on `python_diff -eq 0`), so its log
  holds only maturin + pytest output and the lane measured ZERO — reddening a CORRECT `--lite`
  fix round and a CORRECT `--delta`, a certifying mode. A lane whose SUBJECT DEPENDS ON WHAT
  THE RUN ROUTED TO gets the `runtime:<why>` kind and writes its own record from the same
  routing variables the dispatch was made from; "no executable subject was dispatched" is an
  affirmative `NOT-APPLICABLE`, never `VACUOUS`. **The general rule: before declaring a lane's
  subject, ask whether the lane always HAS that subject — a kind that is right for the common
  route and wrong for a rarer one is a guard that reds on correct input.** **Its domain is WIDER than `COMPONENTS`, and getting that
  wrong was measured, not theorised**: a name reaches a component line from `COMPONENTS`, from a
  `NAMES+=("<literal>")` append in the `run_delta_*` helpers, AND from a `record_result
  "<literal>"` call — the #2926 `tree-selftest` hook is the third kind, and enumerating only the
  first two rendered its row `FAIL` in a real self-test block. Guard: `scripts/tests/test_agent_gate_census.sh`
  (`tooling-tests`), which plants a no-op in a real component under `--only`, requires the block
  to NAME it, and carries a positive control on the same lane differing in ONE property.
  **ADDING A STATUS TOKEN INVALIDATES EVERY HARD-CODED STATUS-SET LITERAL, including the ones in
  the test suites**: three `(PASS|FAIL|SKIP)` alternations survived `VACUOUS`'s arrival, and the
  failure direction is the nasty one — such a pattern stops SEEING exactly the rows that report a
  component verified nothing (one of them then REDDENED A CORRECT boundary block, because a
  sibling count did see them). `test_agent_gate_census.sh` case R1 is the standing sweep; its
  needle is deliberately SPLIT so the guard cannot match its own source, and case R2 proves it
  discriminates the bare three from the roborev block's longer verdict vocabulary, which
  legitimately begins with the same tokens.
  **Two lessons from its review worth carrying elsewhere. (1) A "present-and-zero" tally has more
  than one spelling, and keying on the GOOD word misses all the others**: the pytest reader matched
  `N passed`, so every terminal summary reporting zero passed WITHOUT that word — `61 skipped in
  1.20s`, `1 xfailed in …`, `2 deselected in …`, `3 errors in …` — fell into the ABSENT branch,
  which is `NOT-MEASURED` and therefore PRESERVES `PASS`. A suite whose every test was skipped is
  the vacuous pass this mechanism exists to catch, so RECOGNISE THE SUMMARY LINE FIRST (an outcome
  pair from the driver's own closed vocabulary **plus** a duration tail) and read the count off it
  second. **(2) A near-miss in a FORMAT STRING can hide an entire emit path from a uniformity
  guard**: #3453's B1 grepped for the literal `printf '%-18s %s (%s)'` while the tree-integrity
  BOUNDARY printer spelled its format `(%ss)` — one character — so a whole mode rendered component
  rows with neither annotation and the guard reported zero bypasses. The needle is now the `%-18s`
  NAME FIELD (comment-blind), whose only legitimate occurrence is the renderer's own definition.
  Generalise: **when you assert "everything goes through ONE X", key the assert on the narrowest
  thing that MAKES it an X, never on a whole literal a caller can spell differently** — and
  re-derive the emit-site set from the code rather than from a count someone wrote in a report.
  **(3) A LABEL MAY NAME A STATUS ONLY IF IT WAS DERIVED FROM THE OBSERVED STATUS — this issue
  produced FOUR findings of that one shape** (a progress line printing `PASS` beside a
  `VACUOUS` SUMMARY; a FAILing `gap:` component counted as `DECLARED-GAP` rather than
  not-applicable; `NOT-APPLICABLE` labelled `(SKIP/FAIL)` on a row that PASSes, once the
  `runtime:` route made that pair reachable; and the `ZERO` STATE counted under a heading
  reading `VACUOUS`, a STATUS word, which a shipping mode already contradicted by emitting
  `fmt: VACUOUS (0s)` beside `0 VACUOUS`). The root was structural — the aggregate took
  component NAMES and no statuses, so every status word in it *had* to be an assumption about
  which statuses reach a given state. It takes name/STATUS pairs now, the state buckets carry
  no status word, and the two status-derived figures are counted from the status. **Ask of
  every label: is this word derived from the state I am rendering, or from an assumption about
  which states get here?** And prefer *deriving* the qualifier to deleting it — `(did not
  PASS)` carries real information when it is true. **(4) THE SAME ROOT APPEARED A THIRD TIME, IN
  THE RENDER-TIME FALLBACK, AND THE ANSWER WAS CONVERGENCE RATHER THAN A SIXTH PATCH.**
  `_census_measure` (verdict time) and `_census_record` (render time) answer the same question —
  what is the truthful census state for (component, status)? — and answered it differently for
  five rounds, because the fallback *took no status* and dispatched on kind alone: a gap-declared
  component that CRASHED before `record_result` rendered its GAP reason. Both now delegate to one
  `_census_classify`, with exactly one declared asymmetry (only the measurer may read the
  component log), and `test_agent_gate_census.sh` case S1 drives BOTH over the same 64-cell
  (kind × status × sidecar) matrix requiring identical output wherever the log is not needed —
  because **a second implementation's agreement is only knowable by testing it**. Generalised:
  when two functions answer one question, converging them and pinning the agreement ends the
  class; patching the sixth label does not. **(5) AND A COUNT OF INPUTS IS STILL A PROXY** — the
  delta `node-tests` lane censused *the number of changed files it selected*, which is this
  doctrine's own premise ("a duration is a proxy for work; a count IS the work") violated inside
  its implementation, and wrong in BOTH directions at once: jest EXITS 0 when every selected test
  is skipped, so an all-skipped run reported a confident count and kept its PASS, while a changed
  HELPER runs the WHOLE suite and was censused as one file. The subject must be what the DRIVER
  reports it did, so that lane is `indirect:jest` like `node-bindings` — one tally, not two. **The
  sibling question is answered AT THE DECLARATION rather than left for the next reviewer**:
  `shell-selftests` keeps "scripts executed" because `_run_shell_selftest_files` invokes every file
  unconditionally (selected == executed, which is exactly what was NOT true of jest) and because no
  uniform per-script assertion tally exists to prefer; its residual — a script that runs and
  asserts nothing — is declared there too.
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
  a blocked merge, never "roborev clean". **"ROBOREV CLEAN" MEANS NO UNADDRESSED FINDINGS, NOT "THE TOOL
  PRINTED ZERO" (#3626)** — a LEAD-DEFERRED finding is re-reported by every later round, so the (correct,
  unwaivable) affirmative-`NONE` rule below blocked such a merge FOREVER; the route past it is a
  `roborev-defer: findings` authorization reported as `findings: DEFERRED (…)`, never `NONE`, and every
  OTHER non-PASS verdict still blocks exactly as before. Four rules: **(1)** the NON-SANCTIONED direct forms are
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
  the worktree bug. **The expected RANGE BASE is the MERGE-BASE, never the base ref's TIP (#3392)**:
  `<base>...HEAD` *is* `merge-base(<base>, HEAD)..HEAD`, so an assert that expected the tip FAILED
  DETERMINISTICALLY on a CORRECT review of any branch whose `main` had advanced past its branch point —
  i.e. almost every branch not just rebased. It was misdiagnosed as a race **twice** (the falsifying
  control: `origin/main` recorded before AND after a failing round, unmoved). The tip is still read, for
  the T1 root-checkout signature alone, and the block now prints an informational
  `assert-base: <merge-base> (merge-base of <base> and HEAD; <base> tip <sha>)` so the two can never be
  confused in a pasted block. The absence waiver's `base=` field is bound to that same merge-base —
  copy it from `assert-base:`, not from `base:`. **(3)** `"contains no code changes to review"` on a
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
  **REQUEST A WAIVER ONLY WHEN THE HEAD IS FINAL — pushed, conflict-free, post-gate, and REVIEWED AT
  THAT SHA (#3460).** The binding above is `base` AND `head` AND `job`, each compared for EXACT
  EQUALITY and each against a DIFFERENT value: `head` against the run's own `HEAD_SHA`
  (`git rev-parse HEAD`, assigned ONCE before mode dispatch — NO path derives head from the job record,
  `--recheck-job` included), `base` against `RANGE_BASE_SHA`, which is the **MERGE-BASE and NOT the base
  ref's tip** (#3392 — copy it from the block's `assert-base:` line, NEVER from `base:`), and `job`
  against this run's job id. So **any commit landing after the request makes the grant unapplicable** —
  `waiver: STALE`, and the FAIL stands. The order is therefore: push every local commit → rebase or
  resolve until the PR is not `CONFLICTING` → gate of record → **a roborev confirmation pass ON THAT
  FINAL SHA** → only then request the waiver, naming THAT round's triple. **`--recheck-job` is not an
  escape**: #3392 stabilised the BASE comparison against a moving `main`, and nothing can make a HEAD
  binding survive a genuine content change. **The confirmation pass is the step that gets skipped, and
  skipping it has its own failure shape**: #3367's gated sha had never been reviewed at all (round 25
  reviewed `6f5fc2b7c` and two commits landed after it), and #2605 was the same — so the final sha needs
  its OWN round, and THAT round's job id is the one the waiver must name. **THE TRAP CATCHES A LANE
  DOING THE CAREFUL THING, which is why it is written down rather than left to judgement**: the absence
  diagnostic prints `base … head … job …`, those values are CORRECT at the moment it prints them, and
  **the failing block itself says nothing about a push invalidating them** — only `--help` does ("a
  push, a different base or a re-run each need a fresh one"), which is not where a lane reading a FAIL
  is looking. So copying the verified triple straight into a request is simultaneously the obvious
  action and the wrong one whenever anything is still going to move the head. Measured cost: THREE
  independent lanes on ONE day (2026-08-28) — #1705/PR #3382 (grant received, then a conflict with
  just-merged #1701 had to be resolved), #1699/PR #3403 (the triple was exact and the PR was
  `CONFLICTING` at the same moment), #3248/PR #3455 (fixes committed but unpushed, AND `CONFLICTING`) —
  each spending an authorization on code that would not merge, and asking the authorizer to judge a
  review that no longer described the diff. **DO NOT LOOSEN THE BINDING TO MAKE THIS EASIER**: all three
  instances are the binding WORKING, and a waiver riding to a later review is the hole #3312 exists to
  close. **TWO OF THE FOUR CONDITIONS ARE MECHANIZED AND TWO ARE NOT — KNOW WHICH.** *Pushed* is:
  `push-assert` FAILs the round before any review is enqueued (`FAIL (unpushed commits)` when the remote
  branch exists and local is ahead; `FAIL (branch absent on remote <remote>)` when it was never pushed —
  four spellings, one verdict). *Reviewed-at-that-sha* is: `sha-assert` FAILs when the record's range
  head ≠ local `HEAD`, and the head binding catches it again at waiver time. But **`mergeable` /
  `CONFLICTING` appears NOWHERE** in the wrapper, the waiver scanner or `premerge-assert.sh`, and
  **NOTHING correlates the reviewed sha with a gate of record** — so a pushed, reviewed,
  still-`CONFLICTING` head passes every check and yields a triple that dies on the rebase. Mechanizing
  those two, and splitting the staleness VERDICT TOKEN (`STALE` for all three causes today, though its
  DETAIL already names the diverged field and both values) into `head moved` / `base moved` /
  `job mismatch`, is **#3827** — whose demonstration is **circular rather than impossible**: every
  sanctioned invocation is the branch's own `scripts/flow/roborev-review.sh`, so the round reviewing a
  wrapper change RUNS the changed wrapper (the same property #3544 records for `agent-gate.sh`, and
  NOT the read-from-root self-certification bar of #3229).
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
  them all. **AND `findings:` WAS THE SAME SHAPE, ONE KEY OVER (#3564).** `findings:` is not one of the
  six affirmation keys — its affirmative value is `NONE`, not `PASS` — and it was documented as merely
  CORROBORATING, which read as "guarded elsewhere" when it was guarded NOWHERE: `PRESENT` is in the
  closed grammar's NON-FAILING set, so the only thing failing a findings-bearing run was the
  NEIGHBOURING key `roborev-exit: FINDINGS (exit 1)`. On `--recheck-job` **no reviewer runs**, so
  `roborev-exit` is legitimately `SKIP` — and the run emitted `findings: PRESENT (3)` beside
  `RESULT: PASS`, a **false PASS in a merge gate** (measured on #3473 round 3), on the ONE path an
  authorized waiver must travel, letting a waiver scoped to `prompt-content` ABSENCE excuse findings
  nobody excused. Now a would-be PASS requires `findings:` to reduce token-exactly to `NONE` **in every
  mode including recheck**, and that requirement is **NOT waivable**. Fixed in the verdict scan and
  deliberately NOT in `roborev-exit`: `SKIP` is the TRUE statement about a recheck, and making a key
  claim a failure it never observed trades one false statement for another. Second half, the part that
  keeps the break-glass alive: a recheck of a record with no structured `verdict` field used to read
  `UNKNOWN` (its branch was keyed on the reviewer's exit code, and there is no reviewer), which would
  have false-FAILed EVERY clean recheck — so a recheck now re-asserts findings from the record's own
  review text — but ONLY in the direction prose can actually evidence. **PROSE CAN EVIDENCE FINDINGS;
  IT CANNOT EVIDENCE CLEANLINESS**, so a marker in a findings block yields `PRESENT` while its ABSENCE
  yields `UNKNOWN`, never `NONE`. `NONE` is reachable only from the record's STRUCTURED `verdict`
  letter. **Two review rounds each found a review SHAPE the previous recogniser missed** — a HEADERLESS
  findings review (no `Findings` heading, which `review-completed` deliberately accepts), then a
  findings BLOCK with no recognised severity marker — and the class provably does not close, because
  `review-completed` accepts a bare `## Summary` heading as a completed review: a findings review whose
  findings are prose is then INDISTINGUISHABLE from a clean one, whose real text is
  `No issues found.\n\nSummary: …` with no `Findings` heading either. That is #3312's lesson applied
  one directory over: **REMOVE THE CHANNEL, do not pick a rarer delimiter** — a recogniser over
  author-controlled prose never closes. **And it costs nothing, measured rather than assumed**:
  `roborev show --json` SYNTHESISES the verdict letter from the `reviews.verdict_bool` column for every
  observed record (`P` clean / `F` findings; `review_jobs` has no verdict column), so a real clean
  recheck takes the structured path and the break-glass is intact, and the verdict-less branch is
  defensive for a payload shape nothing observed emits. **The generalisation to carry elsewhere: DELEGATING A KEY'S FAILURE TO ITS NEIGHBOUR IS A
  LATENT FALSE PASS** — the coupling is invisible while one event populates both keys and evaporates in
  the first mode where it does not, so ask of every key *what fails the run if THIS key alone goes bad*.
  **And a fail-closed argument for a `${VAR:-default}` is only valid for the consumers that existed when
  it was written**: the `block_marker_count` `:-0` was audited as strict because `NONE` was the STRICT
  direction for `vacuity-tier1:`, and a new consumer for which `NONE` is PERMISSIVE inverted it silently
  — no default can fix that (`0` and *unmeasurable* are one value). **The resolution is not a better
  default or a second signal but a REMOVED CONSUMER**: `NONE` is unreachable from a marker count at all
  (only the structured verdict yields it), so nothing derives a permissive verdict from that `0` and the
  original argument holds unchanged. An intermediate version of this fix DID add a separate
  `block_measured` flag; it went away with the prose reconstruction it guarded, and this sentence
  described it for one round after it was deleted — caught by the C audit. **A doctrine line naming a
  mechanism is a claim about code, and it decays exactly like a comment: re-grep the symbol.**
  **AND THE UNWAIVABLE RULE MADE ONE MERGE UNOBTAINABLE, WHICH IS ITS OWN DEFECT CLASS (#3626).** #3586's
  requirement is right, and it interacted with a fact nobody designed for: **roborev re-reports a
  LEAD-DEFERRED finding on every later round.** So once a lead defers a finding — as a nit, as a batched
  follow-up, or by explicit ruling — `findings: PRESENT (n)` persists, `RESULT` stays `FAIL`, and *"any
  non-PASS terminal RESULT is a blocked merge"* blocks that merge **forever**: neither escape hatch applies
  (the absence waiver excuses `prompt-content` ABSENCE only, by design, and a correct `--recheck-job` of a
  findings-bearing job re-reports the same `FAIL`). Measured on PR #3572 job 262: two findings, **ZERO
  new** — both already filed (#3602, #3613) and both already lead-deferred — 5.9M input / 5.7M cached
  tokens, every deterministic key PASS, and the merge required an out-of-band lead comment. The lane the
  fix protects is the one that behaved CORRECTLY: it refused to arm `--auto` over a `FAIL`, refused to fix
  the deferrals to manufacture a green, refused a waiver that does not apply, and asked the owner instead.
  **A rule that punishes the correct behaviour will not survive contact.** So *"roborev clean"* is
  redefined as **NO UNADDRESSED FINDINGS**, and the distinction is made MECHANICAL rather than a matter of
  lead memory: a **second marker**, `roborev-defer: findings issues=<N>[,<N>...] count=<n> base=<40-hex>
  head=<40-hex> job=<id> reason=<why>`, travels the **absence waiver's channel** (top-level PR comment,
  column-zero, **sole nonblank content**, hard-coded `ROBOREV_WAIVER_AUTHORS` allowlist, structured
  `gh --json` author parsing, applied via `--recheck-job`, placeholder reasons refused, no part of the
  marker in any diagnostic) and inherits those rules **BY CALL** — the same scanner, kind selected
  explicitly — never by copy, because a second implementation of a channel rule is a second place for it
  to diverge and a divergence there is an authorization bypass. **There is deliberately NO flag, NO file
  in the worktree and NO env var**, each of which would hand the constrained party the power to satisfy
  its own constraint (#3312's corollary). **The match is AFFIRMATIVE, which is what makes this a match
  and not a mute button**: `count=` must EQUAL the observed findings count and `issues=` must be
  non-empty, so a PRE-AUTHORIZATION written before the findings were read fails on a mismatch, and **any
  new finding at the same head raises the observed count and fails** — that is how the UNDEFERRED set is
  computed without a per-finding identity roborev's prose does not provide, and **no such identity is
  reconstructed from that prose** (the class #3564 closed by REMOVING prose reconstruction stays closed).
  `issues=` records that the finding is **TRACKED**, and THE ISSUE-STATE LEG is what enforces it:
  each number must be an **OPEN** GitHub issue, asked **FOUR-VALUED** — only a payload affirmatively
  naming that number **and an OPEN state** is `present` and may grant; an issue GitHub answers does not
  exist is `ISSUE-ABSENT`; an issue GitHub answers is CLOSED is `ISSUE-CLOSED`; an issue whose existence
  could NOT BE ASKED (no `gh`, no auth, a network/API failure, an unparseable payload, or any diagnostic
  that does not say the issue is missing) is `ISSUE-UNVERIFIABLE`. They are **textually distinct**
  because they are different operator actions ("that issue number is wrong" / "that issue is closed" /
  "this box cannot reach GitHub"), and **`gh issue view` EXITS 1 FOR BOTH THE FIRST AND THE LAST**
  (measured, gh 2.98.0) — so an exit-code-only test is the two-valued predicate that always picks
  the permissive answer and would grant over issues nobody confirmed exist. Unrecognised ⇒ could-not-ask,
  and a could-not-ask is NEVER read as verified.
  **AND "RETRIEVABLE" WAS NOT ENOUGH, WHICH IS WHY THE CHECK IS STRONGER THAN THE CONDITION THAT ASKED
  FOR IT (#3626 round 3).** `gh issue view` returns the number and **exits 0 for a CLOSED issue**, so a
  number-only test made "the finding is tracked" satisfiable by an issue closed as a duplicate three
  weeks ago: `present` ⇒ `GRANTED` ⇒ `RESULT: PASS`, the finding permanently untracked while the block
  asserted it was filed. The condition said *retrievable* and closed-is-retrievable satisfies the letter
  — but three separate statements of this leg claim it enforces **not-dropped**, so the claim was made
  TRUE rather than weakened to match a weaker implementation. **The generalisable ruling: when the
  implementation satisfies the LETTER of a condition and contradicts the PROPERTY every statement of it
  claims, strengthen the implementation — do not narrow three claims.** A false refusal here is
  recoverable (reopen it, or file a fresh tracking issue) and is the fail-closed direction.
  **The disposition backstop COUNTS VERIFICATIONS PERFORMED; it does not test the string.** It was
  `[ -z "$ISSUES" ]` — a non-emptiness test standing in for a verification test — and `ISSUES=","`
  passes it, splits into ZERO words, runs the loop body never, and returns with the state still
  `granted`: a `DEFERRED` ⇒ `PASS` with not one `gh issue view` executed. Unreachable only because the
  `issues=` PATTERN forbade that value, i.e. **exactly the upstream dependency a backstop must not
  have**. Now the count of verifications must EQUAL the count of declared comma-separated fields.
  **A PR-BODY LINK WAS ALSO REQUIRED, AND THAT LEG IS DELETED — DO NOT REINSTATE IT (#3626, lead
  ruling).** An earlier revision demanded each number also appear as a local, visible `#N` in the PR
  BODY (`PR-UNLINKED` otherwise), with recognisers for `owner/repo#N`, `#Nsuffix`, fences, code spans and
  HTML comments. **The reason it is gone is NOT the bypasses: a PR body is EDITABLE AT ANY TIME BY ANYONE
  WITH WRITE ACCESS, WITH NO PER-EDIT ATTRIBUTION, while a top-level comment is PERMANENT AND
  ATTRIBUTABLE.** So it was the WEAKER artifact and would stay weaker **even if Markdown parsed
  trivially** — an authorization the constrained party can silently rewrite after it is granted evidences
  nothing; the recogniser problem was a SYMPTOM. The requirement's own wording invited it, too: "name
  where the finding went" invited a PROSE SCAN when the property wanted is that the finding is TRACKED.
  The census, kept because it is the evidence the class does not close (Markdown-handling references in
  that one predicate went **0 → 11** over two rounds): round 1 closed five shapes (cross-repository,
  `#Nsuffix`, fenced block, HTML comment, single-backtick span); round 2 found **two more** — a
  multi-backtick span and an explicit `[#N](url)` link — with GFM autolinks, reference-style links, raw
  HTML, entity refs and nested emphasis unhandled by any generation and the 4-space indent already a
  declared residual. #3312 (*remove the shared channel, do not pick a rarer delimiter*) and #3229's owner
  ruling (*a guard with known documented false-PASSes is worse than no guard*) both apply, and
  **subtraction cannot introduce a false PASS**: with nothing predicted about the body, nothing is
  excused by it. Any future strengthening must come from an **immutable or attributed** artifact (a
  structured GitHub relation, or the authorization comment itself), never from parsing the mutable body
  of the PR under review. It reports
  a **distinct token** — `findings: DEFERRED (<n>, issues=#…, authorized @<login>, job <id>)`, **NEVER
  `NONE`** (which stays reachable only from the record's structured verdict letter, so nobody grepping
  `findings: NONE` reads a deferred run as clean) — beside a `deferral:` key that speaks even when
  nothing was granted (`NONE`/`STALE`/`MALFORMED`/`UNAUTHORIZED`/`COUNT-MISMATCH`/`ISSUE-ABSENT`/
  `ISSUE-CLOSED`/`ISSUE-UNVERIFIABLE`/`UNAVAILABLE`, each leaving the FAIL). A marker **attempt** is the
  stem plus whitespace **or end-of-line**, so a marker-only comment that is exactly the stem is
  `MALFORMED`, never a fail-quiet `NONE`. Three field rules, both kinds, one parser: `base=`/`head=` are
  **exactly 40 hex** (an abbreviated sha is `MALFORMED`, never `STALE` — it names THIS review in a
  spelling the form forbids, and an authorizer sent to re-check *which review* finds nothing wrong); a
  recorded `reason` keeps its internal whitespace **VERBATIM** (only the BLOCK boundary renders a
  control character as a visible escape, because the property required is one line per value, not
  collapsed whitespace); and a `reason` may **not contain either marker stem** — refused, not escaped,
  since **the structural assert covers the CODE while a RUNTIME value can inject what no source scan
  sees, so an invariant over OUTPUT needs a check on the OUTPUT PATH**. **AND THAT RULE IS OVER EVERY
  EMITTED VALUE, NOT OVER THE `reason` — fixing the field and not the class cost a review round
  (roborev job 230).** The reason is the field an authorizer CHOOSES, so refusing it removes that class;
  a keyword also arrives through fields nobody chooses — an unauthorized commenter's **GitHub login**
  (which `UNAUTHORIZED` must report to say who was refused), **`gh issue view`'s stdout/stderr** (which
  reach `deferral:` as an `ISSUE-UNVERIFIABLE` cause), the allowlist, and whatever a future key
  interpolates. So each process neutralises the keywords at its **ONE emit boundary** (`safe_value` in
  the scanner; `roborev_safe_line` in the wrapper, already the gate for every block value and every
  DETAILS line), never per interpolation site — a per-site escape is a list to keep complete. There the
  value is **REDACTED, not refused**: it is an identity or a diagnostic the run must still report.
  Only where the keyword is **not continued by another letter** — a longer word is a different word,
  exactly as `roborev-defer: findingsfoo` is — because the scanner's own FILE NAME embeds a keyword and
  is printed by the fail-closed `waiver: UNAVAILABLE (… tool: <path>)` cause an operator has to read.
  It is **display-only, which is the whole safety argument**: every authorization decision is made on
  the RAW value before any renderer runs, so two boundaries can only redact differently, never grant —
  acceptable where two marker PARSERS would not be, since a parser decides and a renderer does not.
  Deliberately **not** a security layer: a login admits letters, digits and hyphens and NOT colons or
  spaces, so it cannot hold a full stem, and an emitted line begins `deferral: UNAUTHORIZED (`, which
  the sole-content rule refuses. **`findings: UNKNOWN` and `SKIP` are NOT
  deferrable in any mode**: those states were never ESTABLISHED, and a pass may not rest on a state that
  could not be read. **The two authorizations stay SEPARATELY SCOPED and neither falls back to the
  other** — an absence waiver confers no authority over `findings:`, a findings deferral none over
  `prompt-content:` — because collapsing them would let a delivery-artifact waiver excuse a real defect;
  a run may legitimately carry both, each under its own key. `DEFERRED` is a value of the **closed**
  verdict grammar, non-failing **only** on the single coupled granted state that the grammar scan and the
  findings gate both read — one state, not two, so they cannot drift into two opinions about whether one
  authorization was granted. **AND THAT ADMISSION IS CONFINED TO ONE KEY, `findings:`, BY CONSTRUCTION**
  (roborev job 225): the scan carries each key's NAME beside its value and admits the token for
  `findings` alone, and the deterministic-key affirmation backstop carries **no** `DEFERRED` arm and
  reads the state not at all. The confinement was first left resting on an ABSENCE — no other key
  happened to emit the token — which is #3564's lesson verbatim, so ask of every key *what fails the run
  if THIS key alone goes bad*. The contrast with the absence waiver is the reason: **a waiver authorizes a
  PROPERTY** (an absence) that only one key can ever report, so its provenance IS the whole test and it is
  correctly not key-scoped; **a deferral authorizes a NAMED SET OF FINDINGS** and says nothing about
  whether the reviewer's diff arrived or the reviewed range matched, so an unconfined admission would let
  ONE authorization excuse a check NOBODY authorized. Relatedly, **no emitted diagnostic reproduces any
  part of either marker — not even its prefix**: the MALFORMED detail used to quote the whole required
  form and is interpolated into the summary key, so the block printed a fillable authorization beside a
  live base/head/job while a comment beside the interpolation asserted it never did. So: never derive a pass from the ABSENCE of a bad signal; where an oracle is the SOLE evidence
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
  `scripts/flow/premerge-assert.sh <pr> <certified-sha> <gate-of-record-summary> [<delta-summary>]`
  (#2456/#3465) — refusing to merge unless the PR head still equals the certified SHA **AND** a gate
  of record exists for it — and re-reads comments for a fresh `HOLD:` order. **The third argument is
  REQUIRED, and that is the #3465 mechanism**: verifying the head against a *claimed* certified sha never verified that a
  certified sha EXISTS. **Two distinct escapes, one mechanism.** #3408 = **no gate at all** (merged on
  22 `--lite` PASSes and not one full `scripts/agent-gate.sh` run, because nothing in the merge path
  ever asked for the block). #3616 = **a real gate, someone else's** — a closer located its run dir by
  RECENCY (`ls -t /tmp/agent-gate.*`), read a PEER LANE's dir, saw 33 of 37 components PASS and was
  about to merge #3616 on PR #3580's verdict; the count, the dir and the timestamps were all real, and
  only the `run-id:` line exposed it, read by a human. With 14000-27000 stale run dirs per box and up
  to 4 concurrent gates, recency picks a peer ROUTINELY. **That second class is what the
  `commit:`+`tree-start:` binding refuses**: a peer's summary carries the OTHER PR's branch head, so
  requiring both abbreviations to match the certified sha converts "a human might notice the `run-id:`
  line" into a mechanical refusal at the merge point — the sha compare is the guard, not bookkeeping.
  The script now requires the summary file to hold exactly ONE
  `==== AGENT-GATE SUMMARY ====` block (whole-line-anchored; `--lite`/`--delta` headers are distinct
  and refused by name, as is a second or unterminated block) with `RESULT: PASS` and
  `tree-integrity: PASS` compared **token-exactly** (`INCOMPLETE` is the launch sentinel, not a
  verdict — #3041; a mutated-mid-run tree is not a certification — #2926), and with BOTH `commit:`
  (7 hex) and `tree-start:` (12 hex) prefix-matching the certified sha **at each value's own width**
  — a non-hex placeholder REFUSES rather than being skipped. It cannot verify `run-id:` (it did not
  launch the gate — #2874's reader contract needs the launcher) and it cannot prove the summary came
  from a real run rather than a hand-written file: a **hostile invoker is out of the threat model**;
  what this closes is **accident and drift** — a diligent worker with no step in its path telling it
  the gate of record was never run. `dirty:` is REPORTED in the success line **and ENFORCED** (#3648): the gate of record's
  block — and, in Case B, the delta block's too — must read `dirty: no`, matched AFFIRMATIVELY, so an
  absent or unrecognised value REFUSES rather than being read as clean. A `dirty: yes` run certified the
  sha PLUS uncommitted tracked edits (the capture is `--exclude-standard`, so never a gitignored log) and
  `commit:`/`tree-start:` cannot see the difference. There is deliberately NO opt-out — a dirty tree is
  always re-gateable, so an override could only buy a vacuous green. **The FOURTH argument is optional and is the ONLY way a `--delta`
  re-cert can certify a merge** — because #1892 *mandates* `--delta`, "never a repeat full gate", for a
  test/docs-only diff on top of a full PASS at anchor `X`, and mandates that the PR record BOTH blocks.
  A 3-arg-only guard therefore red on correct, doctrine-mandated input, which is the guard agents learn
  to waive. So: **Case A (3 args)** the full block's `commit:`/`tree-start:` must cover the certified
  sha; **Case B (4 args)** the third argument is the ANCHOR's full PASS (its sha need NOT be the
  certified sha) and the fourth must be one `==== AGENT-GATE DELTA SUMMARY ====` block with
  `MODE: delta` (asserted affirmatively — the inverse of Case A's belt), `RESULT: PASS`,
  `tree-integrity: PASS`, a `delta-anchor:` naming exactly that anchor (an `(UNRESOLVED)` anchor
  refuses), and its OWN `commit:`/`tree-start:` at the certified sha. Either way a full-gate PASS must
  EXIST and the merged tree is covered — directly, or by an anchored delta re-cert on top of it. A
  block carrying `nested-under:` (#2874) is refused outright: a nested sub-gate runs at the SAME tree,
  so the sha binding provably cannot see it.
  **What a `PREMERGE: OK` does NOT prove (#3650) — it says so itself, on a `PREMERGE: SCOPE` line.**
  It proves the diff is unchanged since certification and that a full gate PASSed on **that exact
  tree**. It does NOT prove the change was certified against the `main` it will join: a squash-merge
  composes the diff with main's CURRENT tip, so for any PR whose base is behind main **the certified
  tree and the merged tree are different objects**. Measured on #3358/PR #3362: base `2bde26a7c` with
  main 10 commits ahead, whose head gate FAILed `core-tests` only because a known flake's fix
  (`5e08db201`, #3514) was on main and absent from that base — the benign direction; the malign one is
  a PASS at a stale head hiding an interaction with something that landed in between. A gate on the
  MERGE RESULT is **#3650 SLICE 2** and is still not implemented here. Report the verdict as "gate of
  record verified at `<sha>`", never "certified against main".
  **What #3650 SLICE 1 DID add — a non-blocking BASE-STALENESS ADVISORY, which is information and
  not a verdict.** `scripts/flow/base-staleness.sh` (runnable by hand — it is the mechanization of
  the standing triage question *"is the fix for this red already on main and merely absent from my
  base?"*) reports `N` commits behind the **merge-base** with `origin/main` (never the base ref's
  tip — #3392) and `M` of those touching the diff's **blast radius**, which is
  *(paths the diff touches) + (a hard-coded gate-global set)* — content that can change ANY gate's
  verdict regardless of the diff (`.config/nextest.toml`, the toolchain pin, the Cargo manifests,
  `scripts/agent-gate.sh`, `scripts/ci/**`, **`scripts/tests/**`**, `cqlite-core/tests/support/**`,
  `test-data/**`, `.github/workflows/**`). That set is **one NAMED, COMMITTED list
  (`GATE_GLOBAL_PATTERNS`) with no env override**, never an inline glob: an override is
  settable by the party it constrains, *"which paths stale my certification"* is exactly what a
  lane wanting to skip a re-gate would widen, and the next person adding a shared test-support
  directory has to be able to FIND the list. **Membership asserts ONE predicate** — *content here can
  change a gate's verdict INDEPENDENTLY OF THE DIFF* — not "is important" or "is shared"; to add an
  entry, state which gate COMPONENT it can flip and how you MEASURED its selectivity.
  `scripts/tests/**` is in the set because the gate does not merely READ that roster, it EXECUTES it
  (`tooling-tests` runs ~16 of them), so one commit touching one of those files reds EVERY lane's
  full gate — the predicate verbatim — and it was measured before being added (28 → 37 of 107, 9
  commits staling only because of it), while `deny.toml` and the loose `scripts/*.sh` helpers were
  measured and NOT added because they fire zero times. **And the list is DECLARED NON-CLOSED in the
  output**: it is a curated, measured list of RECOGNISED gate-global content, so a gate-global path
  absent from it is a false negative — declared as gap 2 of 2 beside the dependency-closure gap,
  because declaring one gap while having two affirms a completeness the list does not have.
  **The two path sources are RENAME-SYMMETRIC by construction, and that is a FAIL-OPEN if broken.**
  The diff side is porcelain (`git diff`), which honours `diff.renames` (git default TRUE since 2.9)
  and reports a rename's DESTINATION ONLY; the commit side is plumbing (`git diff-tree`), which
  rename-detects only under an explicit `-M`. Unpinned, a PR that renames a path — routine here, the
  campsite rule makes splits normal — loses the OLD path, a commit behind editing it matches NEITHER
  half, and the scan reports `blast-radius 0 RECOGNISED` on a genuinely stale base. `diff.relative`
  is the same class and is worse because the INVOKER controls it: set, porcelain run from a
  subdirectory strips the prefix, making the count a function of cwd. Both are pinned off on the
  porcelain call; **do NOT add `-M` to the `diff-tree` call**, which would reintroduce the asymmetry
  from the other direction. `premerge-assert.sh` prints the finding on
  `PREMERGE: ADVISORY` lines and **can never fail on it** — an absent, failing or `UNMEASURED`
  advisory is REPORTED and is not fatal in slice 1 — and the three `PREMERGE: SCOPE` lines are
  RETAINED, because slice 1 does not close the gap they disclose. Three properties to carry:
  **(1)** the output is ANCHORED so it cannot be pasted or grepped as a certification. **The
  absolute form of this property was FALSIFIED BY REVIEW and the correction is recorded rather than
  softened**: it read *"no `PASS`, no `OK`, no `RESULT:` in any run"*, which is impossible because the
  advisory prints repository-controlled paths VERBATIM — `test-data/**` is gate-global and the tracked
  path `test-data/scripts/CI_SMOKE_TEST_USAGE.md` contains `OK`; three tracked paths do today, and the
  test asserting the absolute form passed only because the sampled run's matched set happened to
  exclude them, a test passing for the wrong reason. What holds instead: **every** output line, stdout
  AND stderr, begins with `BASE-STALENESS: `; every dynamic field is CONTROL-CHARACTER SANITIZED
  (git PERMITS NEWLINES IN PATHS, and unsanitized such a path emits a line with NO prefix, breaking the
  anchor everything rests on) while otherwise printing the path verbatim, because masking it would
  mangle it for the reader — #3312's rule is to anchor or remove the channel, never to pick a rarer
  delimiter; the verdict appears ONLY on a `verdict ` line carrying a token from the closed set
  {`STALE-RECOGNISED`, `NO-STALENESS-RECOGNISED`, `UNMEASURED`}, prose going on `verdict-detail` lines;
  and the script's own STATIC TEMPLATE TEXT carries none of the three tokens, asserted STRUCTURALLY
  over the source file, which is provable where a claim about one sample run is not. **Declared
  residual: a repository path CAN contain a reserved substring and the advisory prints it — the anchor
  is what makes that harmless.** The no-finding verdict is `NO-STALENESS-RECOGNISED` (a *scan result*,
  never `FRESH`/`CLEAN`); **(2)** `M = 0` prints
  `0 RECOGNISED`, never a bare `0`, and every run prints its own `NON-EXHAUSTIVE` lines, because the
  blast radius is **not a dependency closure** — a commit changing an item the diff CALLS while
  touching neither the diff's paths nor a gate-global path is reported as NOT staling, a real
  false-negative class that is declared, filed, and not closed; **(3)** exit `4` is
  `STALE-RECOGNISED`, `5` is `UNMEASURED`, and **a consumer MUST treat `5`/`UNMEASURED` as STALE,
  never as fresh** — the standing rule against deriving a pass from the absence of a bad signal.
  The definition was chosen BY MEASUREMENT against the case that produced the issue
  (`docs/round-artifacts/issue-3650-blast-radius-measurements.md`): on PR #3362 the culprit commit
  and the diff share **no path**, so path intersection alone would call that certification fresh
  exactly when it was not, while intersection + gate-global fires on 37 of 107 commits behind (35%)
  — measured at `origin/main` `b1e8598a2`, subject `4bc6b913a`, the sha quoted because `behind` is a
  function of where main was — leaving 65% of the churn non-staling. The run NAMES the culprit
  (`matched 5e08db201 gate-global .config/nextest.toml`), so the detection is attributable rather
  than a coincidence on a count — and the count is reported BY THE SCRIPT, which is the authority
  for it; a number quoted in prose here decays exactly like a comment. With
  `--auto` armed, GitHub lands the PR on the `required` check going green (#2667); no CI busy-wait.
- **Severity triage (#2088, rubric `docs/development/roborev-severity.md`)**: roborev **blockers**
  are fixed pre-merge — each re-triggers `fix → --lite (+ any diff-relevant parity/integration
  target) → re-review` (#2087). **Nits** never trigger
  a re-verify round: batch all of a PR's nits into ONE linked follow-up issue at merge time. When in
  doubt, blocker. Every pre-roborev self-check class below is BLOCKER by definition.
  **Scripts get a capped loop (#3893):** roborev on `scripts/**`, `.claude/**`, `.github/**` and
  measurement-harness code (`docs/reports/*-artifacts/**`) is capped at **TWO rounds**; round-3 findings
  are DISPOSED — one linked follow-up issue per PR, `roborev-defer` marker on the merits — not fixed,
  UNLESS a finding is a **hang** or a **false verdict** (those two classes are exempt from every
  convergence rule). Bash has no compiler, so each fix round seeds the next; measured 22/25/32 findings
  over 7–12 rounds on three harness PRs in one day, most in the prior round's own fix. Tests and the full
  gate still apply; only the review loop is capped.
  **A DEFERRED finding still has to get PAST roborev, and since #3626 that is mechanical rather than a
  matter of lead memory**: roborev re-reports a deferred finding on every later round, so batching nits
  into a follow-up issue does not by itself make `findings:` read `NONE`. The lead records the deferral
  with a `roborev-defer: findings` PR comment naming the filed issue numbers and the observed count, and
  applies it with `--recheck-job`; the run then reports `findings: DEFERRED (…)` (never `NONE`) and may
  reach `PASS`. See the roborev-invocation bullet above for the marker and its constraints.
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
- **PRODUCT FIRST — the dispatch queue is for the release, not the tooling (owner ruling 2026-09-01,
  #3893).** Measured that night: Ready held 9 product items vs 38 delivery-tooling items; 13 of the day's
  new issues were tooling; three bash-harness PRs ran 22/25/32 roborev findings, most in the previous
  round's fix. Tooling had reached Ready with equal standing and starved the release lane. Rules:
  **(1)** a worker pulling from Ready takes a **release-milestoned** item (currently `0.17`) whenever one
  is Ready; delivery-tooling (gate, roborev, claim, bootstrap, fleet, telemetry, coord) is taken only when
  no product item is Ready or the tooling item is BLOCKING under (2). **(2)** A tooling issue reaches
  Ready ONLY if it (a) caused a **false PASS** or the merge of bad code, (b) **blocked a lane > 1 h**, or
  (c) **recurred twice** — cite which in the issue body. Everything else lands `Backlog` (or is a one-line
  doctrine note, or nothing). "Well-scoped" is no longer sufficient; the lead enforces this at triage
  and does not promote tooling on scope alone. **(3)** Tooling is **feature-complete for the release**: a
  tooling change needs a (2)-justification. **(4)** Finish in-flight tooling PRs on their merits; do not
  feed the pipeline. **(5)** Retro metric: product share of merged PRs, target ≥ 70 % (≈ 45 % when ruled).
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
  **The per-lane STATE MARKER is ownership-stamped too, and its session axis is deliberately NOT
  fail-closed (#3822).** `/drive-issue`'s durable `.drive-issue-state.md` used to be prose with no
  writer, no reader and no ownership stamp, so a session rehydrating in a shared or REUSED worktree
  adopted a peer's plan wholesale. It is now written and read ONLY through
  `scripts/flow/drive-issue-state.sh` (`write` / `verify` / `adopt --reason <why>` / `show`;
  `--help` is the contract), which stamps issue, machine, worktree, session, the SESSION's pid + its
  start window, and actor into a **bounded prologue** — an exact first-line sentinel, `key: value`
  lines at column zero, an exact end sentinel — so identity is never grepped out of the free-form
  body (#3312: remove the shared channel; a body line reproducing a sentinel at column zero is
  REFUSED at write time, and a duplicate sentinel is its own read-time refusal). **The axis split is
  the load-bearing decision and the thing a future agent will otherwise undo:** `issue`, `machine`
  and `worktree` are FAIL-CLOSED and each refusal NAMES ITS AXIS, because they are stable across a
  legitimate resume and distinct across lanes. `session` is RECORDED but not fail-closed, because
  the marker's *intended consumer* is the Delta 3 cron re-invoke — a NEW `CLAUDE_CODE_SESSION_ID` in
  the SAME lane on the SAME issue — so verifying it literally would red EVERY correct resume, and a
  guard that reds on correct input is the guard agents learn to waive. A session difference alone is
  resolved by the LIVENESS of the recorded writer, three-valued on `dead-lanes`' precedent: provably
  GONE ⇒ `ADOPTABLE` and `verify` STILL exits non-zero (adoption is an explicit `adopt` gesture that
  records the prior session, never an implicit inheritance); provably ALIVE ⇒ `LIVE-PEER`, which
  **`adopt` also refuses** (an adopt that ignores liveness is a mute button for the whole guard);
  UNMEASURABLE ⇒ `LIVENESS-UNKNOWN`, refused — a positive verdict requires an affirmative
  measurement. **The pid recorded is the SESSION's (`CLAUDE_PID`), and there is deliberately NO `$$`
  fallback**: `$$` is the transient bash that exits immediately, so recording it would make a LIVE
  peer read as DEAD seconds later — the exact false-permissive this closes. PID reuse is defeated by
  requiring the live pid's start window to still intersect the recorded one, measured by the
  three-valued primitives now SHARED with `claim-heartbeat.sh` in
  `scripts/flow/lib/process-liveness.sh` (one definition, sourced by both: a second implementation
  of those review rounds is a second place to lose them). `machine` is the same notion `claim.sh`
  records — same env var, same default, same sanitizer — and the agreement is pinned BEHAVIOURALLY
  by `scripts/tests/test_drive_issue_state.sh`, which extracts claim.sh's own definition and
  compares, rather than by care. **`write` MUST succeed over an UNSTAMPED marker, and that is a
  correctness requirement rather than a convenience**: every lane holds an unstamped marker on
  rollout BY DEFINITION, so refusing it made the whole marker path a dead letter fleet-wide while
  the refusal text named the very command that refused — the #3312-job-24 shape, where a
  break-glass ships that no sequence of actions can reach. An unstamped marker asserts NO
  ownership, so refusing protects no identifiable party; its body is DISCARDED (never carried
  forward) and the discard is ANNOUNCED. A MALFORMED/DUPLICATE-SENTINEL marker gets no such
  exception — it CLAIMS an identity that merely cannot be READ, which may be a live peer's — so a
  human moves it aside and the lane then takes the ABSENT path. **Generalised, and pinned by a
  DERIVED test case rather than per-verdict prose: no refusal text may name a subcommand of the
  script that returns the SAME refusal in that state** — including a two-step remedy, because the
  readers of these texts run printed commands literally. Naming no mechanical remedy is fine
  (`FOREIGN-*` say "escalate"); naming one that refuses is the defect, and that case caught two
  further instances in the same round it was written.
  **Three later corrections, one shape: a guarantee that stops short of what a consumer
  actually reads.** (1) A fatal start-up failure printed an ANCHORED line and no
  `verdict <TOKEN>`, so every caller the doctrine tells to `case` on the token fell through
  every arm — the prefix is contract (a), the token is contract (c), and **(a) does not imply
  (c)**; every exit now carries a token. (2) The anchor covers the EXTERNAL commands too: a
  native `mktemp:`/`mv:` line has no prefix at all, so each call site either CAPTURES that text
  into the anchored message or suppresses it, and a cleanup command carries `|| true` because a
  failing command in a bash EXIT trap under `set -e` aborts the trap **and replaces the exit
  status** (measured: a broken `rm` turned a legitimate `WRITTEN`(0) into an unexplained
  non-zero). Same sweep, worse defect: a failing `date` committed a stamp with an EMPTY
  required field — `set -e` cannot catch it, since the writer is called as `if ! write_marker`,
  which suppresses `set -e` for its whole subtree — so the assembled bytes are now checked for
  FIELD COMPLETENESS, not just sentinels, or the lane bricks itself. (3) The ADOPTION
  PROVENANCE (`prior-session`, `prior-session-pid`, `prior-ts`, `adopt-reason`) is DURABLE
  STATE: an ordinary `write` preserves it exactly as it preserves stage/request-id/pr/branch,
  because a mandatory, validated `--reason` that the next stage update erases is no audit
  record — and carrying a field forward requires READING it, so all four are parsed under the
  same duplicate-key refusal as the identity keys. (4) And the marker CLASSIFIER read `grep`
  two-valued: an unperformable scan counted as ZERO sentinels, so a DISPLACED stamp classified
  as `legacy` and `write`'s migration path — the ONE branch licensed to discard a marker —
  overwrote what may be a LIVE PEER's state. Every sentinel/field scan here is now three-valued
  and an unmeasurable classification is its own `ERROR` class every caller refuses on, which is
  CLAUDE.md's standing rule (a positive verdict requires an AFFIRMATIVE MEASUREMENT; never
  derive a pass from the ABSENCE of a bad signal) applied where its permissive branch DELETES
  DATA. (5) Correction (1) reached ONE exit path, and the SIGNAL traps and USAGE errors still
  exited with no token — the fix not reaching every site of its own class — so contract (c) is
  now enforced by a FLAG rather than by reviewing each `exit`: `verdict()` records that it
  fired, an EXIT-trap backstop emits `ERROR` for any path that would leave with none, `USAGE`
  joins the closed set, and the signal handlers pick their one token from an explicit
  COMMIT_PHASE (`ERROR` before the atomic rename, the run's own success token after it,
  DEFERRED across it). That phase is only OBSERVABLE because the rename moved OUT of a `$( )`:
  measured on bash 5.2, a trapped signal arriving while the shell waits for a COMMAND
  SUBSTITUTION inside a FUNCTION is DISCARDED — the trap never runs — while the same signal
  during a plain command is delivered normally. (6) Same shape once more, at the SHELL's own
  diagnostics: correction (2) captured 21 EXTERNAL commands' stderr and left `shift`'s, which
  bash prints UNPREFIXED under `shift_verbose`/POSIX mode — both reachable from the inherited
  `BASHOPTS`, so by a caller and not only by the invoker. Every `shift` now validates `$#`
  FIRST. **The transferable rule from (4)-(6): when a round's fix names a site, sweep the
  CLASS and add coverage driven by a TABLE, or the next round finds the same defect one exit
  path over** — which is what happened here three times running. That table immediately paid
  out on a site none of the three findings named: a FAILED REDIRECTION is a native diagnostic
  too, and bash applies redirections LEFT TO RIGHT, so `: >>"$lock" 2>/dev/null` printed its own
  unprefixed `Permission denied` before stderr was diverted — the suppressor must come FIRST.
  (7) The marker BODY is copied BY BYTE OFFSET, never by a line-oriented tool: `awk '{print}'`
  ALWAYS terminates the record it prints, so a body whose last line had no newline GAINED one on
  every carry-forward write (measured 19 → 20 bytes) under a header promising byte-for-byte
  preservation — and the case meant to catch that extracted through awk TOO, so **a verification
  sharing the defect's blind spot is not a verification**; the test helper now reads a `grep -b`
  byte offset and copies with `tail -c`, a different mechanism from the script's, with the
  retired helper kept as the positive control. `$( )` capture is the same class (it strips
  trailing newlines), which is why body bytes are streamed to a redirected stdout and never
  captured into a variable. (8) Contract (c)'s own enforcer had a window inside it: `verdict()`
  committed the "already emitted" flag BEFORE printing the line, so a signal landing between the
  two made the handler AND the EXIT backstop both stay silent and the run exited with NO token —
  over a possibly-committed write. **A flag that says a side effect HAPPENED must be set AFTER
  the side effect, and the gap made unobservable**: the emission is now a signal-deferred
  critical section (print, then commit, then deliver anything that arrived), there is exactly ONE
  site that prints a verdict line and ONE that sets the flag, and the race is pinned by a
  structural order assert plus a signal PLANTED at the window in a scratch copy — with the
  pre-fix ordering kept as the positive control, because a race cannot be pinned by a timed test.
  (9) **An identity is recorded and compared LOSSLESSLY or the run REFUSES, naming its axis** —
  the third instance of H1's family (unmeasurable axis committed as a placeholder; then the
  worktree axis; now a MEASURABLE identity committed LOSSILY). The shared `sanitize_field`
  maps space to `-`, collapses runs and TRUNCATES at 120, so `CLAIM_MACHINE='build box'`
  recorded `build-box` and a genuinely different `build-box` verified as OWNED — and two names
  sharing a 120-char prefix were one owner. Enforced at the USE SITE by requiring
  `sanitize_field(v) == v` (one comparison covering charset AND length, needing no second copy
  of the sanitizer's rules), never in the sanitizer, which stays pinned against claim.sh's own
  definition — that agreement case now compares the two EXTRACTED functions over a TABLE, since
  a lossy value no longer reaches a marker to be read back out of. It covers `machine` and
  `session` (an EQUAL session id is OWNED outright, so a lossy one aliases two sessions);
  `worktree` was already verbatim; `actor`, the durable fields and the `prior-*` provenance are
  DECLARED lossy and deliberately not refused, because nothing COMPARES them so a collision
  cannot grant ownership. Same round, two more: a supplied body is **READ, never stat-gated** —
  an `[ -s ]` before the copy let a source deleted or truncated in between commit an EMPTY body
  under `WRITTEN`, so the caller's file is snapshotted ONCE and every later step reads those
  bytes (a check before the act can only describe a file that no longer has to be the one acted
  on); and an **unrecognised prologue key is PRESERVED**, because accepting it "for forward
  compatibility" while the rewrite path dropped it made an OLDER script silently DELETE a field
  a NEWER one introduced — preserve beats refuse, which would brick every touched lane on a
  fleet mid-rollout.
  (10) **An `-e` existence probe is not an existence probe: it FOLLOWS the link, so a DANGLING
  symlink at the marker path classified `absent` — the ONE class licensed to replace a file — and
  `write` silently destroyed a link someone placed.** Existence is now `-e` **or** `-L`, the `-L`
  test runs BEFORE `-f` (which also follows), and EVERY symlink, dangling or not, takes the
  existing `not-regular` refusal; the class was swept to the script-owned lock sidecar, where
  `: >>"$lock"` would have created a file OUTSIDE the lane. **And an axis guard must run before
  anything that DERIVES A PATH OR TAKES A LOCK**: `adopt` locked first, so an unmeasurable
  worktree yielded a generic "not writable" instead of the published `axis=worktree` refusal —
  the same input answered differently by subcommand. Both were UNTESTED because the axis matrix
  named `write verify show` by hand, so it is now DERIVED from the dispatch table and a
  subcommand with no declared arguments REDS rather than joining uncovered.
  (11) **A `-L`-only refusal is a type rule with one row: the class is EVERY non-regular type,
  and the FIFO is the one that HANGS.** Clause (10) taught the lock sidecar to refuse a symlink
  and left FIFO, socket, device and directory accepted — the third consecutive round whose fix
  reached one member of its own class. `: >>"$lock"` on a **FIFO BLOCKS INDEFINITELY** waiting
  for a reader (measured: `timeout 10` ⇒ rc 124, **no verdict line at all**), which is the worst
  available breach of contract (c) — not a wrong verdict but NO verdict, forever, in a lane
  nobody is watching; a device would serialize NOTHING while appearing to succeed. So the rule
  is stated over the TYPE — only `absent` or `regular` may be opened, everything else including
  a type the probe cannot NAME is one refusal reporting what the entry actually is — and the
  symlink check is FOLDED IN rather than kept beside it, because two checks are two messages to
  keep true. The same sweep covers `--body-file`, where `-r` is TRUE for a FIFO and the `cat`
  then blocks (and `/dev/zero` streams without end into the snapshot: a filled disk, not a
  hang), but there the question is what the path **RESOLVES to**, since a symlink to a real
  notes file is the caller's own ordinary artifact — **the same probe answering two different
  questions would be a defect either way.** The probes are pure `test` builtins: they STAT and
  never OPEN, so probing cannot itself block. Where a probe CANNOT close the gap it is replaced
  rather than added to: the `mv`-diagnostic file was a redirection into the derived name
  `$tmp.err`, and a stat before a redirect only describes a file that no longer has to be the
  one opened — so it is now `mktemp`, which creates with `O_EXCL` and therefore cannot open a
  pre-planted entry of any type. **And a test whose subject is a HANG must be BOUNDED and must
  assert rc != 124 explicitly**: unbounded, a regression does not fail the suite, it hangs it,
  and the thing that notices is the gate's stall watchdog minutes later.
  (12) **EXTRACTING A SHARED LIBRARY MOVES A `source` INTO A SCRIPT THAT NEVER HAD ONE, AND
  THE GUARD WRITTEN FOR IT WAS WEAKER THAN THE ONE THIS SAME CHANGE HAD ALREADY WRITTEN NEXT
  DOOR.** Pulling the liveness predicates out of `claim-heartbeat.sh` into
  `lib/process-liveness.sh` gave that script its first `.` — a NEW open, hence a new exposure —
  and it was guarded `-r` only, while `drive-issue-state.sh`'s guard on the SAME library
  already required `-f` as well. A FIFO there passed `-r` and the `.` BLOCKED FOREVER
  (measured: `timeout 10` ⇒ rc 124, **no output at all**), in the script the fleet reaper runs
  unattended. So: **an extraction's DEDUPLICATION is not complete until the GUARDS around the
  new dependency are deduplicated too** — the second call site is where the review rounds get
  lost, exactly as the predicates themselves would have been. `-f` is the whole class in one
  predicate (false for FIFO, socket, device and directory alike) and FOLLOWS a symlink on
  purpose, since a symlinked checkout is a legitimate layout.
  **The lock is a plain `git push`, so git — not just `gh` — must be authenticated (#2942).** They
  are separate credential paths: an authenticated `gh` with an unwired git fails every claim with
  `fatal: could not read Username`, and `claim.sh` now calls that `ERROR reason=auth (NOT
  retryable)` instead of the old misleading `reason=infra (transient — retry)` — do not retry it,
  fix the box (`gh auth setup-git`, or `bash scripts/bootstrap-agent-machine.sh --yes`, which also
  probes board access functionally rather than trusting the `project` scope string). Since #3369 the
  same script MEASURES git push capability by **performing the push** — a throwaway
  `refs/claims/smoke-<commit-sha>` create/read-back/delete via `claim.sh smoke` — rather than trusting a
  credential-helper answer or a green `git ls-remote`, and reports it three-valued
  (`git-push: VERIFIED` / `FAILED` / `UNMEASURED`); an unmeasurable result is UNKNOWN, never `ok`.
  `--fix-credentials` wires the credential path only (no toolchain installs) and `--strict` turns any
  warning into exit 1, which is what `.agent-ami/profile.yaml`'s `verify.run` uses. The three
  worker-environment deltas and the messages that identify them: `docs/development/fleet-runbook.md`.
- **Supervisor-authored machine claim + CI reaper (#2655/#2499)**: liveness is now MECHANISM-driven,
  not prose. `worker-supervisor.sh` stamps `refs/lane-claims/<machine>/<issue>` (issue+supervisor-PID+ts)
  via `claim-heartbeat.sh stamp` at every spawn, refreshes it each iteration, and clears it on a
  clean exit (`reap`, which REFUSES when the issue still has an open PR — an unfinished endgame stays
  owned for adoption, never orphaned). **The ref is PER LANE — `refs/lane-claims/<machine>/<issue>`
  — since #3393's ruling**; the old per-machine `refs/machine-claims/<machine>` is legacy and is
  still *read* by `list-claims`, `dead-lanes` and the CI reaper purely so a pre-ruling ref gets
  drained (an un-enumerated claim ref pins its board item at In Progress indefinitely). `reap` takes
  `<machine> [lane-id] [expected_sha]`. **`should-reap` has TWO forms and a two-argument call is
  ALWAYS the legacy one** — `should-reap <machine> [threshold_secs]` acts on the legacy ref, and a
  lane needs all three, `should-reap <machine> <issue> <threshold_secs>`. The grammar is deliberately
  unambiguous rather than positional-guessing, so `should-reap <box> <issue>` reads the issue number
  as a THRESHOLD and answers about the legacy ref (#3393 round 21: this doc previously advertised
  `<machine> [issue]`, which is that trap written down). This namespace is distinct from `claim.sh`'s per-issue lock
  `refs/claims/issue-<N>`. `claim-heartbeat.sh should-reap` (both forms above) is the single, fail-safe
  reap predicate (exit 0 = reap, 1 = keep, 2 = no ref): reap ONLY on age > threshold (4h) AND no open
  PR AND (pid-dead, when the claim is local — a foreign machine's PID is unknowable). It KEEPS on a
  fresh ref, an open PR, a live local PID, or an unparseable age; a `gh`/network hiccup in the
  open-PR probe assumes an open PR (keeps).
  **`should-reap` is a REAP GATE, not a liveness monitor, and the difference cost three lanes
  (#3393)**: it consults the PID only AFTER age > 4h, so a worker the kernel OOM-killed a minute ago
  is indistinguishable from a healthy one for four hours — and even then the answer is an exit code
  nobody watches, and nothing reported three silent lane deaths — each leaving a clean worktree, a
  held claim and an open PR. `claim-heartbeat.sh dead-lanes` answers the other question, "is anything
  dead RIGHT NOW", and inverts BOTH of the reaper's conservative guards on purpose: **no age gate** (a
  fresh claim with a dead PID *is* the shape of an OOM kill) and **an open PR does not suppress the
  report** (for the reaper an open PR means KEEP; for a report it is the most urgent row on the page
  — a dead process holding an in-flight endgame, annotated `open-pr=yes`, still never reaped).
  It is a REPORT: it deletes no ref and moves no board item. **`claim-heartbeat.sh dead-lanes --help`
  is the authoritative contract** — it is in the same file as the code and cannot drift from it, so
  read it rather than this summary when the exact verdict set matters (this paragraph drifted once
  already). In outline: verdicts are MULTI-valued because a PID is only checkable on the machine that
  owns the claim — two `DEAD-*` verdicts (`DEAD-NO-PROCESS`, which covers a zombie, and
  `DEAD-PID-REUSED`) and a family of `UNKNOWN-*` ones (`FOREIGN`, `NO-PID`, `STATE`, `IDENTITY`,
  `PROBE`, `UNREADABLE`). Exit `3` = a dead lane was reported (both `DEAD-*` verdicts); exit `1` = none was
  reported — which also covers zero claim refs, an all-foreign run and a failed listing.
  **This slice is POSITIVE-DETECTION ONLY and never exits 0** (#3393 split ruling, 2026-08-29): act
  on `3`, and never read `1` as a clean bill of health. A sound clean verdict IS possible on per-lane
  refs — the masking that made exit 0 a lie is gone, since a surviving sibling now stamps a different
  ref — but it was split out rather than shipped, because the fail-open defect family (five
  instances: a failed probe read as a negative answer) clustered in that exit-0 path and it is the
  value a cron reads. Restoring it is tracked separately, carrying the family census forward.
  **AND ON THIS FLEET IT ANSWERED ABOUT THE EMPTY SET — SUPERVISOR FLEETS ONLY, DESCOPED by owner
  ruling 2026-09-01 on #3548 (option C; completes #3393).** The subject set is `refs/lane-claims/*`
  (+ legacy `refs/machine-claims/*`) and its only IN-TREE CALLER that creates or refreshes them is `worker-supervisor.sh` (`stamp` is public and can be called directly), so on
  this supervisor-less `/drive-issue` fleet it had nothing to report when measured (persisted or
  manually `stamp`ed refs can still produce rows) — and **exit 1 still means "nothing was reported",
  never a clean bill of health.** The populated `refs/claims/issue-<N>` and `refs/heartbeats/<machine>`
  are deliberately NOT read (measured: a transient claiming-shell pid; single-slot-per-machine
  masking), and AC4 survives as a counterfactual — were a later change ever to read a non-refreshing
  carrier, a stale pid there must abstain, never yield `DEAD-*`. **Everything else — the measurement,
  what liveness here rests on, and both board signatures (NEITHER of which is a verdict) — is stated
  ONCE in `docs/development/fleet-runbook.md` → *Lane liveness on a supervisor-less `/drive-issue`
  fleet*.** Seven review rounds on #3548 were propagation failures of duplicated prose, so it is not
  restated anywhere else.
  **Not covered, by construction**: #3393 AC3's "worktree present, tmux session absent" test is
  unimplementable in committed tooling because the lane-directory layout and tmux session naming
  exist NOWHERE in this repo — a tool guessing at them would report nothing on any differently-named
  machine, a vacuous green in a watchdog's clothes. **Diagnostic order for a box that stops answering
  is in `docs/development/fleet-runbook.md`** and starts with `dmesg` for an OOM kill *before*
  concluding the instance is broken, because reading that symptom as a broken box already cost one
  healthy machine.
  The `project-board-sync` 30-min cron runs a `reap-claims`
  job that applies this predicate server-side and flips a freed board item back to Ready with a
  traceable comment. **`PROJECTS_TOKEN` absence now FAILS the workflow loudly (`::error::`)** — a
  persistent red run is the alert, replacing the old silent green `::notice::` no-op. The scheduled
  board sweep only backlogs a null-status issue once it is past a 10-min auto-add grace window, so it
  no longer races the built-in Auto-add's default-status write.
- **~~One worker per machine (#1930)~~ RETRACTED — MULTIPLE LANES PER MACHINE IS THE STANDING MODEL
  (#3393, owner ruling 2026-08-28).** The invariant was false in practice all day (the fleet runs up
  to 4 lanes per box on standing instruction) and leaving it written is what caused the defect it
  was supposed to prevent: `refs/machine-claims/<machine>` was designed one-ref-per-machine *because*
  of this text, so several lanes on one box overwrote each other's claim and `dead-lanes` could report
  at most one — which is why two of #3393's three silent lane deaths, both on one host, were
  structurally invisible. Claims are now **per LANE**:
  `refs/lane-claims/<machine>/<issue>` (a new namespace, because git forbids a ref being both a file
  and a directory, and `<machine>-<issue>` is ambiguous when machine names contain dashes). Read
  "one worker per machine" nowhere as a design constraint; design for N lanes per box.
  **What DOES still hold — one full gate at a time per machine**, which is a resource
  bound and not a worker-count invariant — enforced mechanically (#2640): `bootstrap-agent-machine.sh`
  pins `CQLITE_GATE_MAX_CONCURRENCY=1` (the #1825 cap admits one gate; the per-gate core budget then
  gives it full cores), and every gate derives `CARGO_BUILD_JOBS` + nextest `--test-threads` from its
  slot count and runs under `taskpolicy -c utility`/`nice`, so no manual `pgrep`-serialization is
  needed. **A PIN THAT IS PRESENT IS NOT A PIN THAT IS IN EFFECT (#3414).** That pin lived in
  `~/.bashrc` on all three boxes and was in effect on NONE of them: stock Ubuntu `.bashrc` opens
  with `case $- in *i*) ;; *) return;; esac`, and a gate is launched non-interactively, so every
  gate resolved N from the #1825 formula (`--slots 3` on 16 cores) while `grep` said the pin was
  installed — one gate queued 1h31m and was killed with no verdict, and a measurement lane was
  given an isolation guarantee the semaphore was not providing. Bootstrap now persists to
  `/etc/environment` (read by PAM at session creation, no interactivity guard; bash itself never
  reads it, login shell or not) and takes its verdict from an AFFIRMATIVE PROBE — it SCRUBS its own
  inherited value — **and `BASH_ENV`/`ENV` with it, since a non-interactive bash SOURCES `$BASH_ENV`
  and that file can re-export the variable just scrubbed** — and reads it back out of a fresh,
  profile-free session, in the same posture as `git-push:`. FIVE verdicts ship and only the first is
  an `[ok]`: **`VERIFIED`** (the system-wide file sets a value, a fresh session sees THAT SAME
  value, and the gate honours it), **`NOT-SYSTEM-WIDE`** (the session sees a value the file does not
  set — a sudo- or user-specific override, so ordinary sessions get something else),
  **`NOT-HONOURED`** (visible, but the gate discards or clamps it — fix the VALUE, not the
  presence), **`FAILED`** (not visible), **`UNMEASURED`** (the probe could not run, the gate could
  not be consulted, or the file could not be read/parsed), **`OPT-OUT`/`SKIPPED`**. **`VERIFIED` IS THE ONLY `[ok]`, AND A
  NON-LINUX HOST DOES NOT GET ONE.** An earlier form emitted `ok "NOT-APPLICABLE"` there — the
  mechanism is Linux/`pam_env`-specific, so macOS was scoped OUT rather than supported — and that
  reasoning is still right about the MECHANISM and was wrong about the VERDICT: an `[ok]` is what
  `--strict` reads, so it **certified an unpinned host**, which is the false-certification shape this
  whole section exists to remove. A non-Linux host whose session shows a value now reports
  **`UNMEASURED`** (there is no PAM-read system-wide file to correlate against, so a machine-wide pin
  cannot be told apart from a user-scoped one), and the per-run authority there is the gate's own
  `cpu-budget:` token. **Scoping a platform out is not the same as passing it**, and `NOT-APPLICABLE`
  is emitted nowhere in the script today. **`VERIFIED` IS SCOPED AND SAYS SO**: it
  measures a PAM-created (sudo) session, so a gate launched from a systemd unit or a container
  entrypoint — no PAM in its ancestry, so `/etc/environment` never applies — is NOT covered, and the
  authoritative per-run confirmation stays that gate's own `cpu-budget: max-concurrency=N(pinned)`
  token. The generalisation,
  which is the same one #3369 landed one section over: **presence in a config file and visibility to
  the process that reads it are different facts, and only the second one is a verdict** — so never
  certify a setting by re-reading the file you just wrote, and never let an INHERITED value answer a
  question about a PERSISTED one (bootstrap runs inside an already-pinned session, so an unscrubbed
  probe would have certified the exact failure it exists to catch). It pre-claims by checking the `refs/claims/issue-<N>` ref (`claim.sh status <N>`) AND any legacy
  `issue-<N>-*` branch. Multiple sessions on ONE machine are now expected, each
  claim-protocol-gated; NEVER N bare sessions without the protocol — and note the claim ref is a
  hard control only *cross-machine* (git arbitrates the push). Locally it is advisory: a session
  that never consults it can still walk into an occupied lane directory, which happened
  (#3436). Unattended runs:
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
  multi-cycle issues as multiple deliveries, not one (issue #2314). An issue that ships one or more
  SLICES while **deliberately staying OPEN** gets one record per shipped PR too, stamped with
  `--slice` and carrying `closed_at: null` (cycle time then bounded by the PR's `mergedAt`), which
  `retro` reports as its own class rather than as completed issues (#3550). `--slice` asserts the issue
  was open **when the PR merged**, which current state cannot decide (GitHub auto-closes AFTER the
  merge), so since #3559 it is decided by replaying the issue's own **timeline** to the PR's
  `mergedAt` — the only record that can place a close/reopen relative to the merge. The rule is a
  **conjunction**: slice ⟺ the issue was OPEN at `mergedAt` **AND** this PR closes NOTHING. Both
  halves are permanent: open-at-`mergedAt` alone never becomes sufficient, because the auto-close is
  recorded *after* the merge, so an ordinary completed delivery whose PR declares `Closes #N` was
  **also** literally open at `mergedAt` and only `closingIssuesReferences` tells the two apart (a
  slice PR closes NOTHING). So `--slice` is now ACCEPTED for an issue that has since been closed or
  reopened — the three owed #3393 records (#3407/#3429/#3467) were what this unblocked — and REFUSED
  when the LAST `closed`/`reopened` event STRICTLY BEFORE `mergedAt` is a `closed` (the last one decides — a close then reopen before the merge is ACCEPTED; that delivery COMPLETED the
  issue; a later reopen does not change it) or when the PR declares the close. **`--slice` is an
  operator ASSERTION and the tool refuses it wherever it can be DISPROVED**; where it cannot be, the
  assertion stands — a completed delivery whose PR omits `Closes #N` and whose issue is closed by
  hand later is observationally identical to a genuine slice completed later by another PR, and the
  difference is intent (doctrine bounds it: `flow-implement` mandates `Closes #<N>` in every PR body).
  Closing an issue to satisfy the tool, or hand-appending past the validator, are both FORBIDDEN. Records hold authoritative
  data only (a counter not observed is an error, never a fabricated 0; a delivery with no full gate
  of record is `gate: not-run` + `gate_runs: 0`, coupled both ways and reported by `retro` as its own
  ungated class — #3448). On a cadence the manager
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
