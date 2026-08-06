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
| Executable harness files under `docs/` being classified as prose (the PR #3222 class) | `scripts/tests/test_roborev_review_guard.sh` (#3229) — the `(cx*)` cases drive the census classification and the `prompt-content:` match. **NOT mechanized: whether the configuration would swallow them.** No guard predicts roborev's exclusion set; that is deferred to #3283 | `roborev-lints` (`--lite`) + `tooling-tests` (full) |

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
   census — and why an unretrievable prompt is a `FAIL` there, never a passing `UNAVAILABLE`.

   **Snapshot-delivered diffs are detected and reported, and nothing is read (issue #3312, owner ruling C⁗).**
   A large diff is not inlined: roborev writes it to a **transient** `.roborev/roborev-snapshot-<id>/` file,
   names it in the prompt, and deletes it before `roborev review --wait` returns, so the prompt carries **zero**
   `diff --git` headers — which made the original check false-FAIL every large review. Certifying that mode
   required trusting a copy of a vanishing file, and **seven review rounds found eleven false-PASS vectors** in
   the machinery built to make the copy trustworthy; once that was retired, every remaining defect was in the
   code that merely **touched the filesystem** to digest it. Four destinations were ruled in turn (A-bounded →
   categorical + C‴ → C‴ → C⁗), and **C⁗ is the floor**: in snapshot mode `prompt-content:` reports a
   **`NOTICE`**, and the block records `snapshot-path:` (the path *as the prompt stated it*),
   `snapshot-containment:` (a **`lexical`** statement about that string — no filesystem access at all) and
   `snapshot-expected:` (the census code subset expected, not asserted). **A snapshot-mode PASS does not assert
   that the reviewer received the census paths** — inspect the diff, or re-review a smaller range, if you need
   that. **Inline mode is unchanged and still FAILs on an absent census path.** A stated path that is relative,
   dot-segmented, outside the repository prefix, or not shaped like a snapshot file is a **named FAIL**, as is
   roborev's delegated-inspection tier (which names no snapshot path at all). **The hang and race classes are
   not reachable because nothing is read** — a weaker claim than "fixed", and the only true one.

   **And the invariant that bounds it: inline census verification must not be suppressible by any
   repository-controlled content (job 18).** Once nothing is read, a lexically valid but NONEXISTENT snapshot
   path cannot be refuted — so an instruction *injected* into the prompt would flip an inline review to the
   exempted NOTICE and skip the census check. The column-zero anchor did not cover it: it was designed against
   diff-BODY lines (each carries a leading `+`/`-`/space/`@`), while an injected prompt SECTION — an `AGENTS.md`
   guidelines block is the concrete example — sits at column zero exactly like roborev's own text. RED-verified
   before the fix: a prompt missing a census path reached `RESULT: PASS`. It is now double-locked — an
   instruction counts **only inside roborev's own diff-delivery block** (its `### …Diff…` heading, after its own
   `(Diff too large` notice; the heading is matched *tolerantly*, because it is data in roborev's own template
   and pinning the observed `### Combined Diff` spelling would false-FAIL a default-`### Diff` review), **and** a
   prompt carrying BOTH inline headers and a delivery instruction — which roborev never emits — is a named
   `mixed-delivery` FAIL. Scope: this defends against silent tooling failure, staleness and prompt-content
   injection, *not* against an adversary with write access to the repo, who can rewrite the wrapper itself.

   **The scoping is block-local on both sides, and the last block wins.** The delivery-*mode* decision reads only
   `diff --git` headers seen **inside** a delivery block — what an inline delivery actually looks like — never the
   global header collection (which still feeds census certification unchanged): consulting it made a *legitimate*
   snapshot review FAIL whenever repository instructions merely **quoted** a diff header, this issue's own
   false-FAIL in a new shape. Each block opener discards the previous block's candidates, so only the **final**
   delivery block is selected. The header evidence stays prompt-wide on purpose: under a strictly same-block rule,
   a genuine inline delivery followed by an injected trailer would present a final block with an instruction and
   no headers, resolve to the exempted NOTICE, and skip census certification — the #3222 class excused by
   repository content.

   **The irreducible residual, stated as a property.** Delivery mode is inferred from prompt **text**, and
   roborev's prompt embeds repository-controlled content (project guidelines/`AGENTS.md`, additional context,
   previous-review bodies) at column zero exactly like roborev's own text. There is **no structural marker**
   separating the generated delivery block from injected text that reproduces it, so **no amount of further
   text-scoping closes this**. Concretely: a prompt with **no inline delivery** whose repository content
   reproduces a delivery block, an oversize notice and a lexically valid snapshot path **obtains a `NOTICE` where
   a `FAIL` was due**. It is **bounded, not new** — snapshot mode is uncertified by C⁗, so repository content can
   move such a review *into the already-accepted uncovered envelope*, widening access to an accepted gap rather
   than opening a class; where an inline delivery is present the `mixed-delivery` lock fails closed, and where its
   headers cover the census the run is certified inline and snapshot mode is never consulted. **Closing it
   requires an out-of-band delivery-mode signal roborev measurably does not expose** — no delivery field, no
   digest, no size, and `review_jobs.diff_content`/`patch` present in the schema but empty for every job. So it is
   **disclosed, not fixed.**

   **The general lesson worth carrying elsewhere** is the predicate family that surfaced three times on the way
   (`! -f`, then `! -e`, then `! -e` again): **every `test`/`[` file predicate is two-valued, so it must collapse
   "cannot tell" onto one of its answers — and it always picks the permissive one.** The three-valued helper that
   fixed it was deleted with the probes it served (a lint with an empty subject set greens vacuously, which is
   the very shape it existed to catch), so this rule is the durable artifact: if a filesystem probe returns to
   that code, `verified-absent` / `present` / `unreadable` returns with it.

   The sanctioned substitute is primary-source verification recorded in the PR (for a docs
   change describing the on-disk format, `git show cassandra-5.0.8:<path>`). **No docs-only change may
   ever record "roborev clean."**

   **The same mechanism cuts the other way, and did.** A configured `docs/**` discarded **33 executable**
   measurement-harness files on PR #3222 — a 136-path code census reaching the reviewer as an empty prompt
   (`prompt-content: FAIL (136/136 code census paths absent)`, 15,443 in / 89 out). The
   `docs/reports/*-artifacts/` measurement harnesses this repo ships **by convention are executable code
   that IS reviewed**, so a PR carrying them is **not** a docs-only change and must be roborev-certified
   like any other code change. **One** thing holds that line: `exclude_patterns` is a narrowed
   **prose/artifact deny-list** (`*.md` plus artifact extensions scoped to artifact-bearing
   **directories** — never a blanket `docs/**`). Measured after the narrowing: 71 `docs/` executables reach
   the reviewer, 0 markdown does, and nothing outside `docs/` is newly excluded.

   **The GATE had the same bug, and it is the same definition (#3250).** `scripts/ci/classify-docs-only.sh`
   — which decides whether `pr-gate-core`, the compute half of `required`, runs at all — classified every
   path under `docs/` as documentation on the prefix alone, so the same three PRs reported `required` green
   in 13–16 s having compiled nothing. It now answers only on an affirmative allowlist match and imports
   this subsystem's artifact declaration (`CODE_FREE_ARTIFACT_EXTENSIONS`, `CODE_FREE_ARTIFACT_DIR_GLOBS`,
   `roborev_path_in_artifact_dir`) rather than restating it, so the review-side and gate-side definitions of
   "docs-only" are one fact. The gate-side rule — including how to WAIVE a red on a genuinely prose diff, by
   RUNNING that classifier instead of judging a path shape — is in
   [gate contract → CITE-AND-WAIVE](/cqlite/agents-developing/gate-contract/).

   **NOTHING PREDICTS THE EXCLUSION SET PRE-ENQUEUE, and that is a deliberate, recorded reduction in
   coverage (#3283).** A `census-exclusion:` key that did — a bash port of roborev's own pathspec
   construction (`git.FormatExcludeArgs`) over a TOML parse of three configuration sources — was built on
   #3229 and **REMOVED by owner ruling**. Its false-PASS count across review rounds was *increasing*
   (1, 1, 2, 3), and two of the last round's three defects lived in code the two preceding fix rounds had
   just introduced: a surface where fixes add defects of the class they close. **A guard with known
   documented false-PASSes is worse than no guard, because it invites reliance it cannot support.**
   Subtraction, by contrast, cannot add a false-PASS.

   So a path the reviewer did not receive surfaces **after** the review, under `prompt-content:`,
   fail-closed, with a cause that names the symptom ("the reviewer did not receive this path") rather than
   the mechanism. **If `prompt-content:` FAILs, suspect `.roborev.toml` first.**

   **The class-level root cause, recorded for #3283: a port is a second implementation, and a second
   implementation's correctness is only knowable by differential testing against the original.** The
   removed oracle re-derived Go's `TrimSpace`/`TrimRight`/`TrimLeft` rules in bash and was tested against a
   *model* of Go, not against Go — which is why its NBSP divergence (Go's `unicode.IsSpace` trims U+00A0;
   the bash trims did not) was unfindable by care rather than by differential test. #3283 must either test
   differentially against the real binary or not predict at all.

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
   directory to `CODE_FREE_ARTIFACT_DIR_GLOBS`) in `scripts/flow/roborev-review-oracles.sh` in the same
   edit. That mirror is **maintained by hand, and there is no automated drift assert** — the one that
   existed depended on the removed TOML parser and went with it (#3283). A one-sided edit therefore
   surfaces the slow way, as a `prompt-content:` failure on someone else's report PR. And **never write a
   trailing slash**: roborev trims it *before* deciding anchoring, so `docs/` resolves RECURSIVE
   (`**/docs`) — the opposite of root-anchored `docs/**` — and nothing now catches that inversion before a
   review is paid for.

   #### Neither half of roborev's exclusion set is modelled (#3283, #3278)

   `exclude_patterns` in the file you are looking at is not the whole exclusion set. The effective set is
   the union of the `--repo` checkout's `.roborev.toml`, the **ROOT checkout's** (see the ordering property
   below) and the global `~/.roborev/config.toml` — plus a compiled-in lockfile/cache deny-list roborev
   appends itself (`**/Cargo.lock`, `**/go.sum`, `**/pnpm-lock.yaml`, `**/package-lock.json`,
   `**/.beads/**`, `**/.cache/**` and ~18 more) that no configuration can switch off.

   **Nothing in the wrapper models any of it.** The configured half is deferred to **#3283**, the
   compiled-in half to **#3278**. Both attempts were built on #3229 and deleted, for the same reason: they
   produced false-PASSes faster than review rounds could close them, and **subtraction cannot introduce a
   false PASS** — with nothing predicted, nothing is excused.

   The residual, stated rather than left to be rediscovered: **a path roborev excludes — by configuration
   or by built-in — is silently dropped from the reviewer's diff, nothing names it pre-enqueue, and
   `prompt-content:` FAILs on its absence.** It **fails CLOSED** — never a vacuous green, never a merge on
   unreviewed code. The cost is a **diagnostic**: the stated cause names the symptom ("the reviewer never
   received their diffs", which is true) rather than the mechanism.

   #### The verdict rule — apply it to any call of this shape, without asking

   > **FAIL where the author can act; NOTICE where only the information is actionable; never silence.**

   This is **one** rule, not a set of ad-hoc calls: it decides any verdict-shaped call, including ones this
   wrapper has not met yet. `NOTICE` sits outside the wrapper's failing-capable scan
   (`FAIL|FINDINGS|ERROR|INCONSISTENT`), because `vacuity-tier1:` emits it as a documented advisory.

   **And no key is exempt from the affirmation backstop.** One key was formerly allowed a `NOTICE` there —
   the backstop's single per-key escape hatch — while a remedy-less swallow was a measurement with a stated
   residual. With that subject deleted the exemption went with it: all **six** deterministic keys
   (`push-assert:`, `census-check:`, `code-free:`, `sha-assert:`, `review-completed:`, `prompt-content:`)
   must be affirmatively `PASS`, no exceptions. A structural assert reads the backstop's own `case` body and
   requires exactly ONE exempting arm, so no hatch can be reintroduced.

   **`prompt-content:` expects EVERY census code path and subtracts nothing.** No key is licensed to tell
   another which paths to skip; a path the reviewer really did not receive FAILs. (And it never prints a
   `0/0` PASS: a key with no subject has no verdict to give.)

   **It was never one bug — it is ONE SHAPE, found repeatedly on #3229, so it is now a rule:
   *a positive verdict requires an AFFIRMATIVE MEASUREMENT.*** The shape is *a multi-state signal where
   only the BAD states are tested, so every unknown or unmeasured state inherits the PERMISSIVE branch*:

   | # | signal | states | tested | what the unmeasured state did |
   |---|---|---|---|---|
   | 1 | a built-in-set signal (a since-DELETED subsystem, #3278) | OK / DIVERGED / UNAVAILABLE | `= DIVERGED`, `!= DIVERGED` | took the permissive **excusal** path — coverage excused on a model that could not be verified |
   | 2 | a corroboration signal (a since-DELETED subsystem, #3283) | OK / DRIFT / NOTICE / **UNAVAILABLE (initial)** | `= DRIFT`, `= NOTICE` | reached a `PASS` claiming nothing was configured, and **enqueued** a review |
   | 3 | an `awk` line bound | a number / empty | a `${end:-$start}` default | degraded a failed measurement to a **1-line scan**, in which the absence-assert reads `ok` |
   | 4 | the wrapper's **own verdict scan** — the oldest instance, and the one that outlived the others | 4 failing tokens / 8 non-failing / anything else | four failing **prefixes** | let every unplanned value, `''` included, fall through to `RESULT: PASS` |

   Instances 1–3 lived in subsystems since deleted; **instance 4 is why the rule survived the deletion.**
   The shape was never theirs — it was in the terminal verdict, which predates all of them, and leaving that
   permissive again would have left the wrapper worse than we found it. Instance 2 is the sharpest: the
   code's own comment three lines above said the binary is the ONLY oracle
   that can tell "our parser recognised no key" from "nothing is configured" — and then never required that
   oracle to have **answered**. So the rule, applicable well beyond this wrapper:

   - Never derive a pass from the **absence** of a bad signal.
   - Where an oracle is the **sole** evidence for a claim and could not be consulted, the verdict is
     **non-passing**, and its text distinguishes *"we could not check"* from *"nothing was wrong"* — naming
     what was unverifiable and what would have verified it.
   - Key a permissive branch on the **affirmative** value (`= OK`), never on `!= <bad>`, so an unknown state
     fails closed.
   - Where a signal genuinely **should** be permissive, record the reason **in code** at the branch, so the
     next reader need not re-derive it and the next edit cannot silently widen it.

   The wrapper's own **verdict scan** was the same shape at its most consequential point — four failing
   prefixes tested, everything else falling through to `RESULT: PASS`. Its non-failing set is now an
   **allow-list** (an unrecognised value, an empty string included, FAILs and names itself), plus a backstop
   that a PASS may not carry a verdict-carrying key that is not affirmatively `PASS`: a `SKIP` there means
   the check **never ran**, which is the vacuous pass itself. Un-backstopped, an early-returning
   `prompt-content:` passed a run with the strongest anti-vacuity key having measured nothing.

   **And a closure must not itself be a prefix test — the shape recurses one level down.** Written as
   `PASS*|SKIP*|…`, the allow-list accepts `PASSthisNeverRan` and `PASS-MEASUREMENT-DID-NOT-HAPPEN`: the
   guard against unplanned values checks a **spelling** rather than a **state**, which is the same error it
   was written to remove. So each value is reduced to its **verdict TOKEN** — everything up to the first
   space, well defined because every documented value is either the bare token or `TOKEN (detail…)` — and
   that token is compared **EXACTLY**. Tightening is safe in both arms: a `FAILED (…)` variant no longer
   matches the failing arm by prefix either, and lands in the `*)` that also fails. Two mutation cases pin
   it, and a mutation restoring the globs makes **both** mutants reach `RESULT: PASS` — which is how you
   know the asserts bite.

   The **TOTAL vs PARTIAL** boundary is the whole distinction, and it was measured rather than
   theorised. Left as a NOTICE, a hermetic `Cargo.lock` + `README.md` fixture produced a since-removed
   key's `NOTICE (0/1 survive)`, `prompt-content: PASS (0/0 code census paths present)` and
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
     a since-deleted consumer unquoted at one point, `prompt-content:` did something else again. Patch the
     reported consumer and the next round finds the next one. So:

     - the census reads `git diff --numstat -z`, so paths arrive **RAW**, and RAW is the single
       representation used for classification, comparison and display;
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
     `docs/é notes.md` as extension `md"`, so PROSE counted as **code** — and the configured `*.md`
     legitimately removed it from the reviewer's diff while `prompt-content:` demanded it there, a **false
     FAIL** on an ordinary docs+code branch (reproduced against the tracked
     `docs/research/CQLite Writes (M5) — Analysis & Recommended Paths.md`); a `[^ ]+` header regex likewise
     made a since-removed key report `PASS (2/2 survive)` beside `prompt-content: FAIL (1/2 absent)`.
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

   - (1) produced a since-removed key's `PASS (7/7 code census paths survive)` about a config roborev never
     read. It was caught **only** by the *pre-existing* `prompt-content: FAIL (1/7 code census paths
     absent)` — defence in depth paying out in the direction nobody plans for: the **older** guard caught
     the **newer** one certifying the wrong input. The durable lesson, and the one the descope acted on:
     **when the newer, cleverer guard and the older, dumber one disagree, the one that measures what
     actually happened wins.** `prompt-content:` is the layer that stayed.
   - (2) made a separate investigation (#3234) measure `exclude_patterns` as having *no observable
     effect* — a null result produced entirely by its single daemon restart happening to precede every
     config edit it made and never follow one.

   #### A test that blesses a vacuous verdict is worse than an unguarded path

   Two cases in this repo's own regression suite asserted a since-removed key's
   `PASS (no exclusion patterns configured)` while leaving the binary corroboration
   unavailable — i.e. they **locked in** the exact green a guard emits when it has silently failed to
   recognise a configured key. (Both cases are gone with the key, but the lesson is not.) An unguarded path
   is merely unprotected; a test like that **consumes the
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
  prose; under a mis-scoped pattern it was **executable code** (PR #3222), which is why the configuration
  is now a narrowed prose/artifact deny-list — and why, with nothing predicting that set pre-enqueue
  (#3283), a `prompt-content:` FAIL should send you to `.roborev.toml` first.
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
