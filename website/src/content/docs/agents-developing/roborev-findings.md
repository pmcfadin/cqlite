---
title: Common roborev findings and how to pre-empt them
description: The recurring roborev finding classes — and the one-line fix pattern for each — so implementations land clean and reviews converge in fewer rounds. (Issue #1245)
sidebar:
  label: Pre-roborev self-check
  order: 9
---

`roborev_findings` is the #1 recurring delivery cost in the pipeline telemetry retro
(`docs/reports/delivery-telemetry.jsonl`). Most rounds are spent re-litigating the same
handful of finding classes. Scan your diff against this checklist **before** reporting an
implementation done — every one pre-empted is a review round saved.

This mirrors the **Pre-roborev self-check** section in `CLAUDE.md`. Keep both in sync.

## Which classes are mechanized in `--lite` (issue #2656)

Several of these delivery costs now FAIL in the fast `scripts/agent-gate.sh --lite` loop
(component `roborev-lints`) — and the full gate — so you no longer spend a review round on them:

| Class | Mechanized by | Where it runs |
|-------|---------------|---------------|
| GitHub Actions command injection | `scripts/ci/check-workflow-injection.sh` — flags an *attacker-controlled* `${{ }}` context (issue/PR title/body, `github.head_ref`, commit message, …) inlined into a `run:` shell | `roborev-lints` (`--lite` + full) |
| clippy `manual_range_contains` | `cargo clippy -D warnings` | `clippy` (`--lite` + full) |
| Wall-clock races in tests | `scripts/tests/check-no-wallclock-asserts.sh` (#2642) | `roborev-lints` (`--lite`) + `tooling-tests` (full) |
| Vacuous roborev reviews (a "clean" verdict that reviewed nothing) | `scripts/tests/test_roborev_review_guard.sh` (#2964) — hermetic regression check over every vacuity trigger of `scripts/flow/roborev-review.sh` | `roborev-lints` (`--lite`) + `tooling-tests` (full) |
| A configured `exclude_patterns` that would swallow census CODE (the PR #3222 class) | `scripts/tests/test_roborev_review_guard.sh` (#3229) — the `(cx*)` cases drive the wrapper's pre-enqueue `census-exclusion:` check against fixture-owned `.roborev.toml` files | `roborev-lints` (`--lite`) + `tooling-tests` (full) |

The other classes below (integer/decimal overflow, float ordering, no-heuristics,
process-global counters, gitignored references) are **not mechanized**: they are semantic
or structural, with no low-false-positive static signal (a gitignored-references lint would
false-positive on the intentionally-fetched dataset corpus). Walk them by hand.

**Escape hatches** (deliberate, reviewer-visible, one-line rationale required): the injection
lint honours `injection-lint-allow` on the offending `run:` line or the line above it; the
wall-clock guard honours `perf-gate-allow`.

## The recurring finding classes

### GitHub Actions command injection
User- or dispatch-controlled input (`${{ inputs.* }}`, `${{ steps.*.outputs.* }}`)
interpolated directly into a `run:` shell — worst in a step that holds secrets in `env`.

**Fix:** allowlist-validate the value fail-closed *before* any secret step, then pass it
through a quoted env var; never inline `${{ }}` in `run:`.

**Mechanized (#2656):** `scripts/ci/check-workflow-injection.sh` (gate component
`roborev-lints`, in `--lite` and the full gate) FAILs on an *attacker-controlled* `${{ }}`
context inlined into `run:`. It scopes to the known attacker-supplied contexts (issue/PR
title/body, `github.head_ref`, commit message, `workflow_run.head_branch`, …) so it does not
false-positive on benign `${{ env.* }}` / `${{ inputs.* }}` / `head.sha` interpolations. If a
context is provably not attacker-controlled in that workflow's triggers, mark the line
`injection-lint-allow` with a rationale.

```yaml
# Not allowed — injection sink
- run: ./gradlew publish -Pversion=${{ inputs.version }}

# Allowed — validate fail-closed, then quoted env var
- env:
    VERSION: ${{ inputs.version }}
  run: |
    [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "bad version"; exit 1; }
    ./gradlew publish -Pversion="$VERSION"
```

### clippy `manual_range_contains`
`x >= a && x <= b` fails under `RUSTFLAGS="-D warnings"`.

**Fix:** `(a..=b).contains(&x)`.

### Integer overflow / saturation
Decoding into `i128` or a fixed width and saturating (decimal unscaled values, scale math)
silently loses data; materializing `10^scale` with an unbounded exponent is a DoS/OOM risk.

**Fix:** use `num_bigint::BigInt` (already a dependency) and bound the computation —
compare signs and adjusted exponents *before* computing any large power of ten.

### Float ordering vs Java
Rust `total_cmp` does not match Java `Float.compare` / `Double.compare`: Rust orders
negative NaN first, Java sorts NaN last; signed-zero handling also differs.

**Fix:** when matching Cassandra ordering, use an explicit comparator — NaN last,
`-0.0 < +0.0`.

### Wall-clock races in tests
Asserting a value sampled at one instant against a window captured at a different instant
flakes on one-second boundaries.

**Fix:** capture the time window so it covers *all* sampled operations (sample the bounds
around the whole block, not per-call). If the assertion is really a *perf* signal, convert it to a
recorded metric (`eprintln!`) that belongs to the benchmark lane rather than the correctness gate —
that is how #2369's `collection_benchmarks` wall-clock bounds were retired.

### Process-global work counters under thread-parallel tests
A test that asserts a **delta** on a process-global counter (an `AtomicU64` incremented deep in the
read/scan path) flakes under CI's thread-parallel `cargo test` — unrelated concurrent tests bump the
same counter between the before/after reads. `#[serial(tag)]` only serializes same-tag tests, so an
untagged sibling still contaminates the delta. Local per-process runners (nextest) never reproduce it.

**Fix (structural):** scope the measurement to the current thread — a `#[cfg(test)]` thread-local
scope guard (the `StreamWalkScope` pattern, #2428; `index_probes` follow-up #2451) that reads only
its own thread's increments, contamination-proof by construction. Production builds keep the plain
atomic. Serial tags on the counter then become redundant.

### No-heuristics violations
Inferring a type or behaviour from byte patterns instead of authoritative metadata.

**Fix:** decode from schema or `Statistics.db` metadata only. See the
[no-heuristics mandate](/cqlite/agents-developing/no-heuristics/).

### Gitignored reference binaries / dirty-tree gate
Byte-parity tests silently **SKIP** in a clean checkout because their `.db` references are
gitignored — so a gate that "passed" against your dirty working tree proves nothing.

**Fix:** force-add the tiny reference binaries (`git add -f`) and verify the test against a
fresh `git worktree add --detach HEAD`, never the dirty tree.

## How to use this

1. Before handing an issue off, diff your branch against `origin/main` and walk this list.
2. Fix matches up front rather than waiting for roborev to flag them.
3. Then run `scripts/agent-gate.sh` and request review through the sanctioned wrapper below — see
   also the [gate contract](/cqlite/agents-developing/gate-contract/).

## The only sanctioned invocation is `scripts/flow/roborev-review.sh` (issue #2964)

```bash
bash scripts/flow/roborev-review.sh --agent <agent> --model <model> \
  [--repo <abs-path>] [--base <ref>] [--log <path>]
```

`--repo` defaults to the toplevel of `$PWD` resolved absolute; `--base` defaults to `origin/main`.
Retain ONLY the wrapper's single `==== ROBOREV REVIEW SUMMARY ====` block — never the raw transcript,
which is written to the `log:` path named in the block. That header is deliberately distinct from all
three `AGENT-GATE *SUMMARY` blocks, so a review verdict can never be pasted as a gate verdict nor the
reverse. Exit codes: `0` PASS, `1` FAIL, `3` NOTHING-TO-REVIEW, `2` usage error. **Any** non-PASS
terminal `RESULT` — `NOTHING-TO-REVIEW` included — is a failed review round and a blocked merge, never
"roborev clean".

### The four rules

1. **The wrapper is the only sanctioned roborev invocation.** Three direct-CLI forms are
   **NON-SANCTIONED**: `--branch` **WITHOUT an explicit `--repo`** (from a worktree it resolves against the
   ROOT checkout), the two-positional commit-range form (`roborev review <sha-a> <sha-b>`, whose range base
   is git's EMPTY TREE), and a single-SHA review (`roborev review <sha>`, which **reviews ONE COMMIT, not
   the branch**). Measured on a 17-commit branch with a 27-file census: `--branch --base <base> --repo
   <abs>` delivered **5/5** census code files to the reviewer, the other two **3/5**. So `--repo` is what
   makes `--branch` correct — the defect was always the missing `--repo`, never `--branch` itself — and the
   wrapper invokes that range form.
2. **The reviewed RANGE must be VERIFIED against `<base>...HEAD`.** The wrapper asserts **both endpoints**
   from the **job record's structured fields** (read via `roborev list --json` / `roborev show --json`:
   `git_ref` is `<base40>..<head40>`, reported in `reviewed-sha:` beside a `job-record:` completeness key),
   and demotes the stdout `Enqueued job <N> for <sha>` announcement to the **carrier of the job id** — for
   a range review it names only the range BASE, so when the record is unavailable the run FAILS rather than
   falling back to prose that verifies nothing. A tool's structured record
   is a stronger source than its human-readable prose — the same principle that moved the push assert
   off the local `origin/<branch>` mirror ref onto `git ls-remote`. A range that does not match, a
   **single-commit record even when it equals HEAD**, or a scope that *equals the base ref*, **aborts the
   round**; base-equality is the signature of the
   worktree bug below. Also push first — an unpushed implementation commit is itself an empty-diff
   cause, and the wrapper asserts the push and FAILs otherwise. Which fields are asserted is the
   wrapper's business — see its `--help`.
3. **`"contains no code changes to review"` on a NON-EMPTY diff is a HARD FAIL**, never a pass. The
   wrapper judges the reviewer's claim against a *locally computed* `git` diff census, so a reviewer
   asserting the opposite of a census we measured ourselves has demonstrably not reviewed the change.
4. **A docs-only (code-free) diff cannot be roborev-certified at all** — where **"docs-only" means a
   CODE-FREE CENSUS as the wrapper classifies it, never a `docs/` path prefix** (issue #3229).
   The mechanism is measured, and it is *not* a code/non-code judgement: **roborev drops exactly what its
   configured `exclude_patterns` pathspecs match.** Of a 27-file census — 22 markdown, 5 code — the prompt
   carried headers for exactly the 5 code files **because `*.md` is configured**, not because the reviewer
   recognised prose. So for a prose-only diff the constructed diff
   is genuinely EMPTY and the verdict is a *truthful report of an empty input*, not a reviewer malfunction.
   Re-running cannot help; the wrapper's deterministic
   pre-enqueue `code-free:` check fails it before any review is enqueued, rather than matching reviewer
   prose after the fact. The same mechanism is why `prompt-content:` asserts the **CODE subset** of the
   census — and why an unretrievable prompt is a `FAIL` there, never a passing `UNAVAILABLE`. The sanctioned substitute is primary-source verification recorded in the PR (for a docs
   change describing the on-disk format, `git show cassandra-5.0.8:<path>`). **No docs-only change may
   ever record "roborev clean."**

   **The same mechanism cuts the other way, and did.** A configured `docs/**` discarded **33 executable**
   measurement-harness files on PR #3222 — a 136-path code census reaching the reviewer as an empty prompt
   (`prompt-content: FAIL (136/136 code census paths absent)`, 15,443 in / 89 out). The
   `docs/reports/*-artifacts/` measurement harnesses this repo ships **by convention are executable code
   that IS reviewed**, so a PR carrying them is **not** a docs-only change and must be roborev-certified
   like any other code change. Two things now hold that line: `exclude_patterns` is a narrowed
   **prose/artifact deny-list** (`*.md` plus artifact extensions scoped to artifact-bearing
   **directories** — never a blanket `docs/**`), and the wrapper's pre-enqueue **`census-exclusion:`** key
   — immediately after `code-free:`
   in the block's fixed order — reconciles the CODE census against the *effective* exclusion set by
   porting roborev's own pathspec construction and letting **git** do the matching, FAILing closed and
   naming both the swallowed paths and the pattern responsible.

   That deny-list leans deliberately one way — **noise, never blindness** — and the claim is *scoped*, not
   timeless. It holds for **inert dumps** (`.txt`/`.log`/`.err`), where exclusion costs only **noise**: a
   *new* artifact **directory** (or a new artifact extension inside one of the four below) is silently
   re-admitted to review prompts, which costs tokens; the check can
   only ever FAIL in the opposite direction, where a configured pattern would swallow census code. For a
   **code-bearing format** (`.json`/`.html`/`.svg`) exclusion is **blindness**, because such a file can be
   **functional configuration under any path** — so exclusion of code-bearing formats **must be scoped by
   directory, never by extension alone**. The claim was originally written unqualified, and #3229 falsified
   it with a file this repo already guards; the section below records which one. The durable
   generalisation is worth keeping past this issue: **an extension describes a format; a directory records
   an intent** — someone decided that tree holds artifacts — which makes a directory the better proxy for
   "generated".

   #### Why the exclusions are scoped to DIRECTORIES, not extensions across `docs/`

   The intermediate form — `docs/**/*.txt`, `docs/**/*.json`, … — **did not satisfy the claim above**, and
   #3229 retired it. The asymmetry holds for `.txt`/`.log`/`.err` run dumps, which carry nothing but
   output. It does **not** hold for `.json`/`.html`/`.svg`, which carry *functional configuration*: for a
   code-bearing format, exclusion is **blindness**, not noise. Two live cases falsified it:

   - `docs/observability/grafana/dashboards/cqlite-overview.json` — a dashboard the **full agent gate
     guards with its own `kit-dashboard-drift` component**, so the repo already treats it as
     correctness-bearing. Under `docs/**/*.json` a PR editing it was dropped from the reviewer's diff *and*
     classified code-free: unreviewable by construction, in both directions at once.
   - `docs/reports/delivery-telemetry.schema.json` — the schema governing the delivery ledger, hidden the
     same way.

   So every artifact pattern is now `<artifact-dir-glob>/**/*.<ext>` over exactly four directories:

   | directory glob | what it holds |
   |---|---|
   | `docs/reports/*-artifacts/` | per-issue measurement artifacts (the #3229 convention) |
   | `docs/round-artifacts/` | soak/round measurement output |
   | `docs/**/jfr-reports/` | JFR profiling output |
   | `docs/sstables-definitive-guide/diagrams/` | generated diagram renders |

   Everything else under `docs/` is **reviewed**. Measured when the change landed: 672 tracked `docs/`
   files carry an artifact extension, 667 sit inside those four directories and stay excluded, and the 5
   that do not are now delivered to the reviewer.

   It stays **extension-scoped within** each directory — never a blanket `<dir>/**` — because these
   directories deliberately hold executable code beside their output: 63 tracked
   `.sh`/`.py`/`.rs`/`.c`/`.bt`/`.cql`/`.yaml`/`.toml` files under `docs/reports/*-artifacts/` alone.
   Those harnesses *are* the 136-path census `docs/**` swallowed on PR #3222, so a blanket directory
   exclude would reintroduce this issue's original defect.

   When you add a pattern to `.roborev.toml`, add the extension to `CODE_FREE_ARTIFACT_EXTENSIONS` (or the
   directory to `CODE_FREE_ARTIFACT_DIR_GLOBS`) in
   `scripts/flow/roborev-review-oracles.sh` in the same edit. That mirror is **asserted structurally**:
   `scripts/tests/test_roborev_review_guard.sh` derives the expected pattern set from those constants and
   compares it for **set equality** against the committed `.roborev.toml`, so a one-sided edit FAILs
   `--lite` rather than showing up later as a puzzling `census-exclusion:` failure on someone else's
   report PR. And **never write a trailing slash**:
   roborev trims it *before* deciding anchoring, so `docs/` resolves RECURSIVE (`**/docs`) — the opposite
   of root-anchored `docs/**` — and `census-exclusion:` FAILs on that form unconditionally.

   #### The effective exclusion set is FOUR things, not one

   `exclude_patterns` in the file you are looking at is only a quarter of it. The set is the UNION of the
   `--repo` checkout's `.roborev.toml`, the **ROOT checkout's** `.roborev.toml` (see the ordering property
   below), the global `~/.roborev/config.toml`, **and roborev's own compiled-in lockfile/cache deny-list**
   — `**/Cargo.lock`, `**/go.sum`, `**/package-lock.json`, `**/.beads/**`, `**/.cache/**` and 19 more, 24
   patterns pinned to **v0.61.2** and extracted from the executable itself. Every `census-exclusion:`
   value names WHICH source is responsible for each swallowed path, and ends with
   `built-in-set: OK|DIVERGED|UNAVAILABLE`.

   The two halves reach git by **different mechanisms**, and the pathspec **count** is part of the
   contract. A *configured* pattern is a user pattern that `git.FormatExcludeArgs` anchors and expands into
   **two** pathspecs, `<body>` and `<body>/**`. A *built-in* is not a user pattern at all: it is a
   **pre-formatted pathspec constant appended to git's argv verbatim**, contributing **exactly one**
   pathspec, never re-anchored and never given a `/**` sibling. That was established from the v0.61.2
   binary rather than assumed — the `:(exclude,glob)` prefix sits *inside* each of the 24 string literals,
   which Go's length-ordered rodata packing proves is not linker coincidence (equal-length runs at deltas
   of exactly `15 + len(pattern)`), and only **two** bare `:(exclude,glob)` constants exist among the 26
   total occurrences, so there is no shared prefix for 24 patterns to be formatted against. Corroborating
   shape: the *directory* built-ins carry `/**` hand-written into the literal (`**/.beads/**`) while the
   *file* built-ins do not (`**/Cargo.lock`) — nobody writes `/**` onto a string about to be handed to a
   formatter that appends it.

   Getting that count wrong is a false PASS **in either direction**, which is why it is pinned both ways.
   Running built-ins through the formatter invented a `**/Cargo.lock/**` exclusion roborev never applies;
   **over**-modelling the exclusion set makes `prompt-content:` *excuse* paths from coverage. Dropping the
   sibling from *configured* patterns is the mirror-image error: it reports paths as SURVIVING that roborev
   really drops. Re-verify both on every roborev upgrade — the same obligation the pin itself carries.

   #### The verdict rule — apply it to any call of this shape, without asking

   > **FAIL where the author can act; NOTICE where only the information is actionable; never silence.**

   This is **one** rule, not three ad-hoc calls, and it is what decides `census-exclusion:`'s three
   outcomes:

   | Cause | Verdict | Why |
   |---|---|---|
   | A **configured** pattern swallows census CODE | **FAIL** | The remedy is a one-token edit to a **named** file. Act before paying for a review round. |
   | A **pinned built-in** swallows **SOME** census CODE | **NOTICE** | There is **no** remedy: the deny-list is compiled in, with no opt-out and no negation form. A guard that fires on a legitimate change (a routine `Cargo.lock` touch) with **no available fix** is the guard that gets **disabled** — which is how #3229 happened. So: paths named loudly in the value line, run proceeds, and `prompt-content:` is told not to expect them. |
   | A **pinned built-in** swallows the **WHOLE** code census | **FAIL**, pre-enqueue | Nothing reaches the reviewer, so a verdict on an **EMPTY prompt** certifies nothing. Not an exception to the row above — the **same** rule reaching a case that row does not decide, and it *does* have a remedy: the one `code-free:` already prescribes (verify another way; record primary-source verification in the PR). |
   | The **live built-in set diverges from the pin** | **FAIL** | This one *does* have a remedy — re-extract, update the pin, and **judge** the new built-in. It is a **mechanism** change, which the version pin exists to catch rather than absorb: a NOTICE here would silently swallow an upgrade that began excluding `*.rs` or `scripts/**`, with the failure looking like normal operation. |

   "Never silence" is the load-bearing third clause. An **unobservable** built-in set (no roborev on
   PATH, an unreadable binary) reads `built-in-set: UNAVAILABLE` **in the value line** — never as an
   unstated assumption of agreement. `NOTICE*` sits deliberately outside the wrapper's failing-capable
   scan (`FAIL*|FINDINGS*|ERROR*|INCONSISTENT*`), so a NOTICE cannot red `RESULT:`; both FAIL forms can.

   The **TOTAL vs PARTIAL** boundary is the whole distinction, and it was measured rather than
   theorised. Left as a NOTICE, a hermetic `Cargo.lock` + `README.md` fixture produced
   `census-exclusion: NOTICE (0/1 survive)`, `prompt-content: PASS (0/0 code census paths present)` and
   `RESULT: PASS`, exit 0 — **a vacuous pass textually identical to a genuine one**, on which
   `flow-closer` would arm `--auto`. Its trigger is ordinary: any dependency-bump branch whose only
   non-prose file is a lockfile. `code-free:` does not catch it (a `.lock` extension classifies as
   CODE), tier 1 greps a phrase the reviewer need not emit, and tier 2 is `UNAVAILABLE`.

   Two follow-throughs on `prompt-content:`, the wrapper's strongest deterministic anti-vacuity key:

   - **A `0/0` is never a pass.** With no census path left to look for, the key has no subject and so no
     verdict to give: it FAILs, and it can never print `PASS (0/0 …)`.
   - **Paths are normalised ONCE, at the census — one boundary, and it is the fix for SIX blockers.**
     Rounds 2–4 of review on #3229 produced six blockers and **every one was a path-normalisation defect
     in a different consumer**, because normalisation was scattered: the census did not normalise at all,
     `census-exclusion:` unquoted at one point, `prompt-content:` did something else again. Patch the
     reported consumer and the next round finds the next one. So:

     - the census reads `git diff --numstat -z` and the survivor set `--name-only -z`, so paths arrive
       **RAW**, and RAW is the single representation used for classification, comparison and display;
     - the one quoted-path decoder survives only for text we did **not** get from git plumbing — the
       reviewer's prompt — with exactly **one caller**, the canonical matcher
       `roborev_diff_header_has_path`. Every consumer asks that matcher instead of parsing headers;
     - it reads every shape git emits: unquoted, **space-bearing** (`diff --git a/a b.txt b/a b.txt`),
       **C-quoted** (`diff --git "a/\303\251.txt" "b/…"`), and the **MIXED** shape
       (`diff --git a/<ascii> "b/<quoted>"`) that occurs **only on renames** — and our census runs
       `--no-renames` while the reviewer's diff has rename detection ON;
     - the invariant is asserted **structurally** (no path-reading `git diff` without `-z`; the decoder
       called only from the matcher; no header regex or delimiter-based membership anywhere else), because
       a behavioural case can only cover the shapes someone already thought of.

     Both failure directions were measured. False FAIL: classifying a *quoted* spelling read
     `docs/é notes.md` as extension `md"`, so PROSE counted as **code** and the configured `*.md` reported a
     swallow ⇒ `census-exclusion: FAIL` **pre-enqueue** on an ordinary docs+code branch (reproduced against
     the tracked `docs/research/CQLite Writes (M5) — Analysis & Recommended Paths.md`); a `[^ ]+` header
     regex likewise gave `census-exclusion: PASS (2/2 survive)` beside `prompt-content: FAIL (1/2 absent)`.
     False PASS: a newline-delimited path set probed with `grep -Fxq` turned `a<LF>b.rs` into the
     alternatives {`a`, `b.rs`}, so a prompt naming only `a` reported `PASS (2/2 present)` for a file the
     reviewer never received. A key that reds on correct input is the key agents learn to waive; a key that
     greens on absent input is worse. This repo tracks 40 space-bearing paths under `docs/`, including the
     directory `docs/storage engine/`.

   #### A `.roborev.toml` change cannot certify itself — three properties, one generalization

   1. **roborev's daemon reads `exclude_patterns` from the repo ROOT PATH.** It binds a repository by its
      `repos.root_path` and resolves the config from **that** checkout — so a **worktree**
      `.roborev.toml` edit is **invisible** to it. Under 1:1:1:1 the file you edited is not the file your
      review applies.
   2. **The daemon snapshots config at start.** An edit needs a **daemon restart** to take effect.
   3. **Generalized — state it this way:** *any PR whose subject is a config the daemon (or a gate) reads
      from root cannot certify itself.* Plan the demonstration for **after** the merge.

   (3) is the **same shape** as the `required`-check property in CLAUDE.md — `required` evaluates the
   aggregator **and the registry** from the PR's **BASE** ref, so a registry change lands only after it
   merges. Recognising the shape is the transferable part.

   Both (1) and (2) have already cost real rounds, so they are not theoretical:

   - (1) produced `census-exclusion: PASS (7/7 code census paths survive)` about a config roborev never
     read. It was caught **only** by the *pre-existing* `prompt-content: FAIL (1/7 code census paths
     absent)`. **That is the strongest argument in the change for keeping both layers**, and it paid out
     in the direction nobody plans for: the **older** guard caught the **newer** one certifying the wrong
     input. Defence in depth is not about the new check protecting you from old bugs.
   - (2) made a separate investigation (#3234) measure `exclude_patterns` as having *no observable
     effect* — a null result produced entirely by its single daemon restart happening to precede every
     config edit it made and never follow one.

   #### A test that blesses a vacuous verdict is worse than an unguarded path

   Two cases in this repo's own regression suite (`cx5b`/`cx5c`) asserted
   `census-exclusion: PASS (no exclusion patterns configured)` while leaving the binary corroboration
   unavailable — i.e. they **locked in** the exact green a guard emits when it has silently failed to
   recognise a configured key. An unguarded path is merely unprotected; a test like that **consumes the
   review budget that would otherwise have found the bug**, and converts "nobody checked" into "we
   checked and it was fine". When you add a case whose expected value is a PASS, ask what state the
   system is in when that PASS is *wrong*, and make the fixture distinguish the two.

### Why: a vacuous roborev pass is textually identical to a genuine clean pass

**Four** confirmed trigger paths make roborev report clean **without having reviewed anything** (or having
reviewed only part), and at the top level ("No issues found") a vacuous verdict reads exactly like a real
one:

- **T1 — worktree + `--branch` without `--repo`.** Worktrees are not in `roborev repo list`, and
  `roborev repo` has no `add` subcommand (repos self-register on first use), so `--branch` resolves against
  the **ROOT checkout** — which normally sits on `main` — and enqueues the BASE commit. Observed: enqueued
  `39900e4db` (= `origin/main`) while the branch HEAD was `4e7ab591e`; jobs 4649/4651/4653/4655/4657 all
  enqueued `origin/main`. Adding an explicit `--repo <abs>` FIXES this form: it then reports "17 commits
  since origin/main" and delivers every census code file — which is why the sanctioned invocation uses it.
- **T2 — the two-positional commit-range form** anchors the reviewed range at git's **EMPTY-TREE** hash
  (`4b825dc6…..<head40>`) rather than at the base you named, delivering 3 of 5 census code files.
- **T3 — a diff whose every path the configured `exclude_patterns` match is silently discarded** even on
  a correctly targeted run: right SHA, right
  `--repo`, and still *"No issues found. Summary: The provided diff contains no code changes to
  review."* Reproducible (jobs 4658/4659). **This one passes the SHA check, so SHA verification alone is
  insufficient** — hence rules 3 and 4. The mechanism is rule 4's: the configured pathspecs remove those
  paths before the diff is constructed, so there is genuinely nothing to review. By default that is
  prose; under a mis-scoped pattern it was **executable code** (PR #3222), which is why
  `census-exclusion:` now computes the same fact pre-enqueue.
- **T4 — a single-SHA review covers ONE COMMIT.** `roborev review <sha>` enqueues `git_ref = <head40>` —
  *correct*, and still partial: 3 of 5 census code files reached the prompt on a 17-commit branch. Every
  sha-equality check passes while the reviewer saw only the last commit, so this is a PARTIAL review
  reported as a complete one, invisible to any SHA check. It is also the form #2964's own AC2 prescribed;
  the wrapper implements that AC's *intent* — the reviewed content must match the requested range.

Token accounting is the tell: genuine reviews run 398k–649k input / 314k–554k cached / 5.0k–6.3k output
over 2m25s–2m45s, while the vacuous baseline is 18.7k input / 0 cached / 53–56 output in 8s (a
known-empty diff: 17,333 input / 21 output). The wrapper uses this only to **fail closed** — it can
never turn a failure into a pass.

The real cost, measured: on #2950 two vacuous runs "passed"; re-run correctly against the real SHA, the
**same diff produced two real blockers** that would otherwise have shipped. Because 1:1:1:1 puts every
issue in a worktree, and `flow-closer`'s final roborev pass is a **merge gate**, this could merge
unreviewed code fleet-wide.

### Live worktree probe (documented, not gate-run)

The hermetic regression check cannot prove the real external binary honours `--repo`. From a real
`issue-<N>-*` worktree whose commit is pushed, while the root checkout sits on `main`, run the wrapper and
confirm the summary block's `sha-assert: PASS` beside a **`reviewed-sha:` RANGE** of the form
`<base40>..<head40>` whose **HEAD endpoint is the worktree branch's HEAD** and whose base is `origin/main`.
Because the sanctioned invocation reviews a range, `reviewed-sha` is **not** a bare sha — do not test it for
equality with `head-sha`; compare the range's head endpoint. A `reviewed-sha` that is `origin/main` alone
means the explicit `--repo` did not defeat the root-checkout resolution. It stays out of the gate because
it needs network and a live reviewer, and it should be re-run after any roborev version bump.

## Pass BOTH agent and model — the wrapper requires it

`.roborev.toml` on `main` pins `agent`/`review_agent = 'codex'` and
`model`/`review_model = 'gpt-5.6-sol'`. That repo-local pin **overrides** whatever your global
`~/.roborev/config.toml` sets, so it is the value that actually runs — and worktrees inherit `main`'s
pinned config. The wrapper therefore **requires both** options and treats one alone as a usage error
(exit `2`), because supplying one alone silently inherits the other from that pin:

```bash
# codex (the repo default reviewer)
bash scripts/flow/roborev-review.sh --agent codex --model gpt-5.6-sol

# the Claude reviewer — override BOTH
bash scripts/flow/roborev-review.sh --agent claude-code --model claude-opus-5
```

`--agent claude-code` **alone** still inherits `review_model = 'gpt-5.6-sol'` from config — an OpenAI
model name Claude cannot serve — which surfaces as a silent review failure that looks like a backend
outage rather than a config mismatch. (Historically the pin ran the other way, and codex on a ChatGPT
account rejected the inherited Anthropic name with a hard `400 'opus' model is not supported`; it is the
same trap, mirrored.) The explicit `--model` is the reliable override on every checkout.

### `gpt-5.6-sol` is codex's default, not a config pin

There is **no `~/.codex/config.toml`** on the worker boxes — `gpt-5.6-sol` is simply what the bare
`codex` binary resolves to. That default moved `gpt-5.5` → `gpt-5.6-sol` across the 0.142.5 → 0.145.0
upgrade, so a future codex version bump can silently move it again and leave `.roborev.toml` pinning a
model the installed CLI no longer serves. Check what is actually in effect with `codex --version` and the
model line in a bare `codex exec` header, rather than assuming a config file holds it.

## Verifying an update to this page is actually published

A green deploy plus an HTTP `200` proves the site is up, **not** that your change is live — the CDN can
keep serving the previous page for roughly **3 minutes** after a successful deploy. Accept a doctrine
publish by grepping the served page for a distinctive phrase the change introduced, and re-check after a
wait if it is absent:

```bash
curl -sS https://pmcfadin.github.io/cqlite/agents-developing/roborev-findings/ \
  | grep -c 'a vacuous roborev pass is textually identical'
```

A `0` means not-yet-published — never bank it as done.
