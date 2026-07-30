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
4. **A docs-only (code-free) diff cannot be roborev-certified at all.** The mechanism is measured:
   roborev **excludes non-code paths from the diff it builds** (of a 27-file census — 22 markdown, 5 code —
   the prompt carried headers for exactly the 5 code files), so for a prose-only diff the constructed diff
   is genuinely EMPTY and the verdict is a *truthful report of an empty input*, not a reviewer malfunction.
   Re-running cannot help; the wrapper's deterministic
   pre-enqueue `code-free:` check fails it before any review is enqueued, rather than matching reviewer
   prose after the fact. The same mechanism is why `prompt-content:` asserts the **CODE subset** of the
   census — and why an unretrievable prompt is a `FAIL` there, never a passing `UNAVAILABLE`. The sanctioned substitute is primary-source verification recorded in the PR (for a docs
   change describing the on-disk format, `git show cassandra-5.0.8:<path>`). **No docs-only change may
   ever record "roborev clean."**

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
- **T3 — a code-free diff is silently discarded** even on a correctly targeted run: right SHA, right
  `--repo`, and still *"No issues found. Summary: The provided diff contains no code changes to
  review."* Reproducible (jobs 4658/4659). **This one passes the SHA check, so SHA verification alone is
  insufficient** — hence rules 3 and 4. The mechanism is rule 4's: non-code paths never reach the
  constructed diff, so there is genuinely nothing to review.
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
