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
| GNU-only shell constructs in a macOS-sensitive script (`sed -i EXPR`, `xargs -r`, `stat -c`, `readlink -f`, `grep -P`, `timeout`, bash-4 constructs, …) | `scripts/tests/test_roborev_guard_portability.sh` (#3296) — an enumerated construct table, every rule carrying a positive control, plus a behavioural BSD-shim differential. **Its SUBJECT SET is enumerated, not derived, and a pass says nothing about a file outside it** — so since #3756 the run PRINTS its scope: every scanned path, plus a measured `unscanned: N of M tracked scripts/**/*.sh` line (`NOT MEASURED` when the census cannot be taken). Scanned today: the roborev wrapper + guard, this lint itself, and `scripts/bootstrap-agent-machine.sh` + its suite | `roborev-lints` (`--lite` + full) |
| Executable harness files under `docs/` being classified as prose (the PR #3222 class) | `scripts/tests/test_roborev_review_guard.sh` (#3229) — the `(cx*)` cases drive the census classification and the `prompt-content:` match. **NOT mechanized: whether the configuration would swallow them.** No guard predicts roborev's exclusion set; that is deferred to #3283 | `roborev-lints` (`--lite`) + `tooling-tests` (full) |

The other classes below (integer/decimal overflow, float ordering, no-heuristics,
process-global counters, gitignored references, colour-blind cargo-output parses) are
**not mechanized**: they are semantic or structural, with no low-false-positive static
signal (a gitignored-references lint would false-positive on the intentionally-fetched
dataset corpus). Walk them by hand.

One of them was mechanized and then **deliberately un-mechanized**: a structural lint over
the cargo-output parse sites was built on #3400 and **descoped** because its own false-PASS
count rose across review rounds (2, 2, 3) and two of the last round's three defects were
inside the two preceding fix rounds. That is the same ruling as #3229's removed
`census-exclusion:` key — **a guard with known documented false-PASSes is worse than no
guard, because it invites reliance it cannot support.** Mechanization is deferred to #3499.
The standing coverage is behavioural, not structural:
`scripts/tests/test_cargo_output_parsers.sh` (in `tooling-tests`) EXTRACTS each parser from
the shipped `scripts/agent-gate.sh` and runs it against real coloured cargo bytes, so it
pins the defect against real code rather than predicting it from source shape.

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

### Cargo-output parses keyed on literal status text
A guard that greps a `cargo test` log for a literal like `Running tests/` or `warning:` is
**inert under colour**. 18 workflows set `CARGO_TERM_COLOR: always` — including the nightly
FULL `gate.yml` — as does `scripts/local/pre-merge.sh`, and **colour survives redirection to
a file**, so the gate's own mandated `> gate.log 2>&1` capture is coloured too. This is not a
tty-only artifact.

Cargo colours the **status word** and emits the reset immediately after it
(`Running<ESC>[0m tests/foo.rs`), so a pattern anchored on the status word alone survives
while one spanning `<status> <payload>` matches **nothing**. It breaks both ways and neither
is safe: the cli-tests zero-tests guard reported OK having judged no target at all (a vacuous
PASS, live on `main` for months, fixed by #1699), and the declared-vs-observed reconciliation
reported every declared target unobserved on a perfectly healthy run (a false RED, fixed by
#3400).

`test result:` and `running N tests` are libtest's own output and carry no escapes — cargo
does not pass `--color` through to the harness — so parses keyed on those are safe **for a
reason that is not visible at the parse site**. That inherited-correctness coupling is exactly
what left the zero-tests guard inert, which is why the rule is applied uniformly rather than
only where it is load-bearing.

**Fix:** route every cargo-output parse through `_ansi_stripped_log` and read the result by
**redirection, never a pipe** — a piped `while read` runs in a subshell, so its accumulated
verdict is discarded on exit and the guard passes silently for a second, independent reason.
`CARGO_TERM_COLOR=never` at the invocation is **belt, not the fix**; `gate.yml` keeps `always`
on purpose, because colour is a presentation property of a log read by humans and moving
correctness into a workflow file 18 files away from the parse is a worse coupling than the one
being removed.

### No-heuristics violations
Inferring a type or behaviour from byte patterns instead of authoritative metadata.

**Fix:** decode from schema or `Statistics.db` metadata only. See the
[no-heuristics mandate](/cqlite/agents-developing/no-heuristics/).

### Gitignored reference binaries / dirty-tree gate
Byte-parity tests silently **SKIP** in a clean checkout because their `.db` references are
gitignored — so a gate that "passed" against your dirty working tree proves nothing.

**Fix:** force-add the tiny reference binaries (`git add -f`) and verify the test against a
fresh `git worktree add --detach HEAD`, never the dirty tree.

### A permitted shape found ANYWHERE, standing in for the line BEING that shape (issue #3367)

**The single most repeated finding class measured to date: seven of 29 findings in one PR, then
three more of the same family.** Every instance is a guard that asks *"does a permitted construct
appear somewhere on this line?"* when the question is *"IS this line that construct?"*

Measured instances, all from one guard, each found after the previous was fixed:

| the check | what slipped through |
|---|---|
| `case $line in *"bash $ref"*)` | `grep bash "$WRAPPER"` — a scan that merely mentions the word |
| one occurrence per line assumed | `bash "$W"; grep -c x "$W"` — the invocation carried a scan in with it |
| `${seg%%=*}` for the assigned name | `_saved=x; subject="$W"` — the FIRST `=` named an allowlisted variable |
| a substring of the declaration text | `grep -E "a|b" "$_saved"` — the line quoted the declaration and exempted itself |
| marker matched anywhere on the line | `# see 'marker' above` — prose naming the escape hatch became it |
| marker matched as a prefix | `marker-allowance` — a longer word starting with the marker granted |
| exclusion by substring | `_saved=/decoy # _wr_fixture` — naming an excluded token exempted a real mutation |

**Fix:** decide the property of the WHOLE line, not the presence of a token in it. Anchor at column
zero, match the trimmed line EXACTLY, count occurrences rather than assuming one, and reduce a value
to its token before comparing. Where a fixture or diagnostic must mention the guarded name,
**compose it at runtime** (`"$_alias"`, `'wrap''per'`) so the file never contains the literal —
that removes the need for the exclusion that keeps becoming the hole.

### A claim about a SET, where the set does not close (issue #3367)

The sibling class. A guard enumerates the constructs it knows about; review finds the one it missed;
the fix adds it; repeat. Measured: `$(...)` but not backticks; `-n` but not `-ng`/`-g -n`;
`${VAR:=}` but not `${VAR:+}`/`${VAR:-}`; braced but not unbraced expansions; `NAME=` but not
`export`/`declare`/`+=`/`printf -v`/`read` — **that last one three separate times, on three
different variables.**

**The terminating move is to remove the decision, not to complete the list.** What actually worked,
in the order it was found:

- **Hand the property to the shell.** `readonly` refuses every assignment syntax, including the ones
  you did not think of. `readonly` on an *unset* name forbids the variable existing at all — but
  **`unset` first**, because `readonly` on an inherited exported value freezes that value instead.
- **Use a `local`, not a global.** A function-local cannot be assigned from outside, so the whole
  "who may write this?" question is unexpressible rather than policed.
- **Delete the mutable thing.** If a global exists only so two cases can repoint it, pass a parameter
  instead and the guard has no subject.
- **Choose needles that cannot occur in what you must exempt.** Scanning for prose-shaped strings
  forced a help-text exemption, and six rounds went into deciding which heredoc lines execute. Keying
  only on code identifiers — impossible in English — deleted the exemption, the filter, and the family.

**And the general limit, worth knowing before you start:** a lexical scanner cannot decide whether a
shell line reads a variable, because the name need not appear in the line —
`export WRAP''PER; sh -c 'grep "$WRAP''PER"'` reads it, and banning `eval` does not close it
(`bash -c`, `source`, `.`, `xargs sh -c`). If your guard needs that decision, redesign so it does not.

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

**And "roborev clean" means NO UNADDRESSED FINDINGS, not "the tool printed zero" (issue #3626).** A
lead-deferred finding is re-reported by every later round, so the (correct, unwaivable)
affirmative-`NONE` requirement below blocked such a merge **forever**. The route past it is a
`roborev-defer: findings` authorization on the absence waiver's channel, reported as
`findings: DEFERRED (…)` — never `NONE` — beside its own `deferral:` key. Every **other** non-PASS
verdict still blocks exactly as before, and `findings: UNKNOWN`/`SKIP` are **not** deferrable. Full
mechanism below, under *"the unwaivable rule made one merge unobtainable"*.

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
   round**. The expected **range base is the MERGE-BASE** of `<base>` and HEAD, never the base ref's
   **tip** — `<base>...HEAD` *is* `merge-base(<base>, HEAD)..HEAD`, and an assert that expected the tip
   failed deterministically on a correct review of any branch whose `main` had advanced past its branch
   point (#3392, misdiagnosed as a race twice). The tip is still read for the root-checkout signature
   alone, and the block prints an informational
   `assert-base: <merge-base> (merge-base of <base> and HEAD; <base> tip <sha>)`; the absence waiver's
   `base=` field binds to that merge-base, so copy it from `assert-base:`. base-equality is the signature of the
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

   **`prompt-content:` asks one question, and there is no delivery classifier (issue #3312, owner ruling (4)).**
   Are the census **code** paths present in the prompt the reviewer was sent? Present is a PASS; **absent is a
   FAIL, unconditionally**, whatever caused it. The wrapper used to infer *how* roborev delivered the diff —
   inlined, or by a path to a transient `.roborev/roborev-snapshot-<id>/` file it deletes before `--wait`
   returns, or the delegated tier that ships neither and tells the reviewer to run git itself. That inference
   produced **four consecutive High-severity false verdicts, in both directions**, and the instances differed
   while the cause did not: roborev's prompt embeds repository-controlled content (project guidelines /
   `AGENTS.md`, additional context, previous-review bodies) at column zero, indistinguishable from roborev's own
   text, so structure read out of it is spoofable both ways. No terminating marker exists — the only structural
   one was roborev's fenced diff, and repository content can contain fences too — so the owner deleted the
   inference instead of patching a fifth instance. Block detection, heading parsing, fence evidence,
   `mixed-delivery`, candidate lifetime, the snapshot/delegated distinction, the lexical path binding, snapshot
   path extraction, the three `snapshot-*` keys and this key's `NOTICE` exemption are all gone, so the
   affirmation backstop again has **no per-key escape hatch** and all four Highs are unexpressible.

   **The accepted cost, stated because it is real: a snapshot-delivered diff and a vacuous review that received
   nothing are identical to the machine.** Neither has census paths in its prompt, so both FAIL. What
   distinguishes them is a **human plus the review's token accounting** (genuine: 398k–649k input / 314k–554k
   cached; vacuous baseline ~18.7k / 0). That trade was chosen deliberately over a machine guessing from
   injectable text.

   **That cost is true at review time, and false after the fact only for a *human* reading the stored
   record — it stays true of the *machine*, and must (#3654).** The prompt roborev *sent* is **retained in the job record** and retrievable later —
   `roborev show <id> --prompt` — even though the snapshot file it names is transient and already deleted,
   and a delivery-by-path prompt says so in its own words under `### Combined Diff`. That is **the direct
   artifact** — roborev's *actual prompt* rather than a statistic about it.

   **It is not self-authenticating, and the trust properties run the opposite way from the obvious
   reading.** roborev's prompt **embeds repository-controlled content** at positions indistinguishable from
   roborev's own text, so a reviewed branch can carry text *mimicking* that delivery wording and an
   authorizer would read it as roborev's — a human in the loop is not a channel separation, it is the same
   shared channel with a slower parser. So the prompt is read for the **structural fact** it reports, never
   as proof of its own provenance. The **token accounting is daemon-recorded but not independent, and
   it does not establish delivery either**: the *record* is authentic (the branch cannot rewrite it), but
   the counts measure **the prompt**, and the prompt embeds repository-controlled content — so a branch
   influences their *magnitude* without forging anything. That bites exactly where the counts are used:
   the vacuous baseline is ~18.7k input / 0 cached, so padding non-diff prompt content can make a review
   that received **no diff** look token-rich. **Neither signal establishes provenance, and the two are not
   independent** — both are functions of the same repository-influenced prompt. **Which evidence a waiver
   should rest on is an open question, tracked as
   [#3826](https://github.com/pmcfadin/cqlite/issues/3826)** — nothing here recommends one signal over the
   other, or any ordering between them. **It resurrects nothing of the deleted delivery classifier, and that distinction is
   load-bearing rather than a caveat:** the classifier read injectable prompt text *at decision time* to
   produce an **automated verdict**, while this is a **human** reading a **stored record** as evidence for a
   **hand-granted** waiver, so the direct **parser** exploit is gone: nothing in the wrapper parses the
   prompt for delivery mode, and nothing may be added that does. **That is all it buys** — the human is
   *in* the path, not outside it, so spoofed repository-controlled prompt text can still mislead an
   authorizer into issuing the marker, and the marker is what makes `--recheck-job` pass. That exposure is
   [#3826](https://github.com/pmcfadin/cqlite/issues/3826)'s subject and is not settled here.

   **And `job=` is daemon-scoped, which nobody had written down (#3654).** Each fleet box runs its own
   roborev daemon with its own database and its own sequential ids, so **two boxes can legitimately present
   the same id for different reviews** — measured: `job=265` on two lanes 50 minutes apart, different
   ranges, branches and token counts, both correct — and the coordination lead read the repetition as a
   collision and **withheld a valid waiver**. The failure is symmetric and the other direction is worse: a
   lead who therefore treats `job=` as uninformative discards the one field binding an authorization to a
   *review* rather than to a *range*. So **verify the record's `git_ref`, never the id alone**:

   ```bash
   # git_ref / status / token_usage are NESTED under .job on this payload:
   roborev show <id> --json | jq '.job | {id, git_ref, branch, status, token_usage}'
   # `roborev list` defaults its branch filter to the --repo path's CURRENT HEAD, not to the
   # branch your shell is on — so name --branch when that checkout is not on the job's branch.
   # --limit defaults to 50 (measured, v0.61.2): RAISE it until the job appears, or until the
   # returned row count stops growing. An empty result at a still-growing limit is UNMEASURED.
   roborev list --json --limit 200 --repo <abs> --branch <branch> | jq '.[] | select(.id==<id>) | {id, git_ref, branch}'
   ```

   **Read `.job`, never a `show` payload's top-level `id`.** That field is the *review* row's own
   sequence and need not be the job you asked for — measured over ten records, asking for 9 returns
   `id=8` with `job_id=9` and `job.id=9` — so a top-level jq manufactures exactly the "is this the right
   review?" doubt the check exists to remove. The wrapper is unaffected (`find_job` matches
   `id`/`job_id`/`job` and then **prefers the object carrying `git_ref`**, so it lands on the job row
   either way): this is a trap for the human running the check by hand, not a live false `STALE`. For the
   same reason, do not reach for the top-level `uuid` as a machine proxy: it identifies the **review row**,
   not the daemon, so it answers a different question however it renders. (It would not render blank in any
   case — the writer emits no line at all for an empty value, so the key would read `NOT RECORDED`.)

   **A local row count is not evidence of uniqueness.** `roborev list … | jq '[.[] | select(.id==N)] |
   length'` returns `1` whether or not another box holds that id — and `0` when the row fell outside the
   `--limit` window, so the count says nothing about the window it was taken over — because `list` only
   ever sees the **local** daemon — one more probe whose output is **identical under the two states it claims to separate** (the
   `RESULT: INCOMPLETE` launch sentinel read as a verdict; a gate run dir found by `ls -t`;
   `mergeable: MERGEABLE` on a marker-bearing merge commit). Run on both `job=265` lanes it gave the right
   answer for a reason that did not hold.

   **What the `git_ref` check settles is scoped to one daemon; the rest is not claimed.** It settles
   that the id names the review you think it does *on this daemon*. It does **not** settle the cross-box
   case: two daemons can hold the same id for the **same `git_ref` range**, so a waiver authorized
   against machine A's review can be accepted by `--recheck-job` against machine B's different review of
   that range — and **no local lookup can detect it**, because `roborev list` only ever sees the local
   daemon while the marker travels through GitHub. Same-range cross-daemon collisions therefore remain
   **unprotected**: declared here, not closed here.

   **Whether the block should name the issuing daemon — and the cross-box question that comes with it,
   since the marker travels through GitHub while `--recheck-job` reads the *local* daemon — is tracked as
   [#3825](https://github.com/pmcfadin/cqlite/issues/3825), together with the marker-grammar question it
   raises.**

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
   **REQUEST A WAIVER ONLY WHEN THE HEAD IS FINAL — pushed, conflict-free, post-gate, and reviewed AT
   THAT SHA (issue #3460).** The binding above is `base` AND `head` AND `job`, each compared for **exact
   equality** and each against a **different** value:

   - `head` against the run's own `HEAD_SHA` (`git rev-parse HEAD`, assigned **once** before mode
     dispatch — no path derives head from the job record, `--recheck-job` included);
   - `base` against `RANGE_BASE_SHA`, which is the **merge-base and *not* the base ref's tip** (#3392) —
     copy it from the block's `assert-base:` line, **never** from `base:`;
   - `job` against this run's job id.

   So **any commit landing after the request makes the grant unapplicable** — the run reports
   `waiver: STALE` and the FAIL stands. The order is therefore:

   1. **push** every local commit;
   2. **rebase or resolve** until the PR is no longer `CONFLICTING`;
   3. **gate of record**;
   4. a **roborev confirmation pass on that final sha**;
   5. *then* request the waiver, naming **that** round's `base`/`head`/`job`.

   **`--recheck-job` is not an escape.** #3392 stabilised the *base* comparison against a moving `main`,
   and nothing can make a *head* binding survive a genuine content change — that is what the binding is
   for.

   **The confirmation pass is the step that gets skipped, and skipping it has its own failure shape.**
   #3367 found its gated sha had never been reviewed at all: round 25 reviewed `6f5fc2b7c` and two
   commits landed after it; #2605 hit the same shape. The final sha needs its **own** review round, and
   *that* round's job id is the one the waiver must name.

   **The trap catches a lane doing the careful thing, which is why it is written down rather than left
   to judgement.** The absence diagnostic prints `base … head … job …`, and those values are *correct at
   the moment it prints them*. **The failing block itself says nothing about a push invalidating them** —
   only `--help` does ("a push, a different base or a re-run each need a fresh one"), which is not where
   a lane reading a FAIL is looking. So copying the verified triple straight into a request is
   simultaneously the obvious action and the wrong one whenever anything is still going to move the
   head. Measured cost: **three independent lanes on one day** (2026-08-28) — #1705/PR #3382 (grant
   received, then a conflict with just-merged #1701 had to be resolved), #1699/PR #3403 (the triple was
   exact and the PR was `CONFLICTING` at the same moment), #3248/PR #3455 (fixes committed but unpushed,
   **and** `CONFLICTING`) — each spending an authorization on code that would not merge, and asking the
   authorizer to judge a review that no longer described the diff.

   **Do not loosen the binding to make this easier.** All three instances are the binding *working*, and
   a waiver riding to a later review is the hole #3312 exists to close.

   **Two of the four conditions are mechanized and two are not — know which.**

   | condition | mechanized? | what enforces it |
   |---|---|---|
   | pushed | **yes** | `push-assert` fails the round *before any review is enqueued* — `FAIL (unpushed commits)` when the remote branch exists and local is ahead, `FAIL (branch absent on remote <remote>)` when it was never pushed |
   | reviewed at that sha | **yes** | `sha-assert` fails when the record's range head ≠ local `HEAD`; the head binding catches it again at waiver time |
   | conflict-free | **no** | `mergeable` / `CONFLICTING` appears **nowhere** in the wrapper, the waiver scanner or `premerge-assert.sh` |
   | post-gate | **no** | nothing correlates the reviewed sha with a gate of record |

   So a pushed, reviewed, still-`CONFLICTING` head passes every check and yields a triple that dies on
   the rebase. Mechanizing those two, and splitting the staleness **verdict token** (`STALE` for all
   three causes today, though its *detail* already names the diverged field and both values) into
   `head moved` / `base moved` / `job mismatch`, is **issue #3827** — whose demonstration is *circular
   rather than impossible*: every sanctioned invocation is the branch's own
   `scripts/flow/roborev-review.sh`, so the round that reviews a wrapper change runs the **changed**
   wrapper (the property #3544 records for `agent-gate.sh` — not the read-from-root self-certification
   bar of #3229).

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
   **certified** head sha, it carries a reason, and all of it is recorded in the summary block.

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

   **AND `findings:` GATES THE VERDICT ON ITS OWN — the affirmation backstop's six keys were never the
   whole story (#3564).** `findings:` is not one of the six (its affirmative value is `NONE`, not `PASS`, so
   it cannot satisfy that loop's uniform test), and it was described as merely *corroborating* — which read
   as "guarded elsewhere" when it was guarded **nowhere**. `PRESENT` is in the closed grammar's
   **non-failing** set, so the only thing failing a findings-bearing run was the NEIGHBOURING key
   `roborev-exit: FINDINGS (exit 1)`. That coupling held for a fresh review and broke exactly where it
   mattered most: on `--recheck-job` **no reviewer process runs**, so `roborev-exit` is legitimately `SKIP`,
   and with the failing signal gone the run emitted

   ```
   findings:     PRESENT (3)
   roborev-exit: SKIP (recheck: no reviewer ran in this invocation; job 160 re-decided from its record)
   RESULT:       PASS
   ```

   — a **false PASS in a merge gate**, measured on #3473's round-3 recovery. And it landed on the one path
   an authorized waiver must travel (a re-run enqueues a different job and stales the waiver), so a waiver
   scoped to `prompt-content` **absence only** could carry a findings failure nobody excused.

   Now: on any would-be `RESULT: PASS`, `findings:` must reduce (token-exact) to `NONE`, **in every mode
   including recheck**, and the requirement is **not waivable**. The fix is in the verdict scan and
   deliberately **not** in `roborev-exit`, because `SKIP` is the TRUE statement about a recheck — making that
   key claim a failure it never observed would trade one false statement for another. Its second half is
   easy to miss and is the part that keeps the break-glass alive: a recheck of a record with **no structured
   `verdict` field** used to fall through to a branch keyed on the reviewer's exit code and read `UNKNOWN`,
   so the gate alone would have false-FAILed **every clean recheck**; a recheck now re-asserts findings from
   the record's own review text (the transcript in that mode), scoped to the findings block, and reports
   `UNKNOWN` — never `NONE` — whenever that reconstruction cannot support a positive claim.

   **And that reconstruction was itself unsound — two review rounds proved it, and the fix was to
   DELETE it rather than patch it a third time.** Round 1: `review-completed` deliberately ACCEPTS a
   **headerless** findings review (a bare `**Severity**:` line, `[High]`, `Medium:`), for which the
   findings-block extraction finds nothing — so "0 markers in the block" meant *"no block was found"*,
   not *"no findings"*, and read as an affirmative `NONE`. Round 2: a `## Findings` block whose findings
   carry **no recognised severity marker** leaves the block non-empty and the count at zero, defeating
   the round-1 fix too.

   **The class provably does not close.** `review-completed` accepts a bare `## Summary` heading as a
   completed review, so a findings review whose findings are prose — no `Findings` heading, no severity
   marker — is a *valid* completed review reporting findings, and it is textually indistinguishable from
   a clean one, whose real text is `No issues found.\n\nSummary: …` with no `Findings` heading either.
   Any recogniser over that prose admits some findings-bearing shape. This is **#3312's umbrella lesson
   applied one directory over: remove the channel, do not pick a rarer delimiter** — and the wrapper's
   own facts tool already said it, that the structured field "must win wherever it exists" and a
   transcript regex is "a prose heuristic".

   So the direction is **asymmetric and permanent**: a severity marker inside a findings block is
   positive evidence **of findings** (`PRESENT`); its absence is **not** evidence of cleanliness
   (`UNKNOWN`). **`NONE` is reachable only from the structured `verdict` letter.** And that costs
   nothing, **measured rather than assumed**: `roborev show --json` synthesises that letter from the
   `reviews.verdict_bool` column for **every** observed record — `P` for a clean review (job 154,
   `verdict_bool=1`), `F` for a findings-bearing one (job 162, `verdict_bool=0`) — while the
   `review_jobs` table has no verdict column at all. A real recheck of a clean job therefore takes the
   structured path and still PASSes, so the #3312 break-glass is intact, and the verdict-less branch is
   **defensive**, for a payload shape no observed record produces. Making a defensive path fail closed
   is free; making it guess is how a merge gate passes over live findings.

   **The unwaivable rule made one merge unobtainable, which is its own defect class (issue #3626).**
   #3586's requirement is right, and it interacted with a fact nobody designed for: **roborev re-reports a
   lead-deferred finding on every later round.** So once a lead defers a finding — as a nit, as a batched
   follow-up, or by explicit ruling — `findings: PRESENT (n)` persists, `RESULT` stays `FAIL`, and *"any
   non-PASS terminal RESULT is a blocked merge"* blocks that merge **forever**. Neither escape hatch
   applies: the absence waiver excuses `prompt-content` **absence only**, by design, and a correct
   `--recheck-job` of a findings-bearing job re-reports the same `FAIL`. Measured on PR #3572 job 262: two
   findings, **zero new** — both already filed (#3602, #3613) and both already lead-deferred — 5,937,937
   input / 5,703,168 cached tokens (the largest of 21 rounds), every deterministic key PASS, and the merge
   required an out-of-band lead comment.

   The lane the fix protects is the one that behaved **correctly**: it refused to arm `--auto` over a
   `FAIL`, refused to fix the deferrals to manufacture a green, refused a waiver that does not apply, and
   asked the owner instead. **A rule that punishes the correct behaviour will not survive contact.**

   So *"roborev clean"* is redefined as **NO UNADDRESSED FINDINGS**, and the distinction is made
   mechanical rather than a matter of lead memory. A **second marker** travels the absence waiver's
   channel:

   ```
   roborev-defer: findings issues=<N>[,<N>...] count=<n> base=<40-hex> head=<40-hex> job=<id> reason=<why>
   ```

   applied with `--recheck-job <id>`, which enqueues nothing. Everything about the channel is
   **inherited by call, never copied** — the same scanner, with the kind selected explicitly — because a
   second implementation of a channel rule is a second place for it to diverge, and a divergence in a
   channel rule is an authorization bypass: sole nonblank content of a **top-level** PR comment, an author
   on the hard-coded `ROBOREV_WAIVER_AUTHORS` allowlist, author association parsed **structurally** from
   `gh --json`, placeholder reasons refused, and **no part of the marker in any emitted diagnostic** (the
   form lives only in `--help`, because summary blocks get pasted into PR comments as a matter of course).

   **There is deliberately no flag, no file in the worktree and no environment variable.** Each would hand
   the constrained party the power to satisfy its own constraint — #3312's corollary, *the constrained
   party must not choose its own enforcer* — and a worker could then clear its own findings. That absence
   is asserted **structurally** in the guard suite, because behavioural cases only cover the channels
   someone already thought of.

   **The match is affirmative, which is what makes this a match and not a mute button.** `count=` must
   **equal** the observed findings count and `issues=` must be non-empty. A job is a completed review and
   its findings do not change, so `job=` already fixes the finding *set*; the count equality is the
   affirmative evidence that the findings the authorizer judged are the findings this run observed. Two
   consequences, both deliberate: a **pre-authorization** written before the findings were read fails on a
   count mismatch instead of passing silently, and **any new finding at the same head** raises the observed
   count and fails. That is how the *undeferred* set is computed without a per-finding identity — which
   roborev's prose does not provide, and **none is reconstructed from that prose**: the class closed above
   by *removing* prose reconstruction stays closed.

   **`issues=` records that the finding is TRACKED, and the issue-state leg is what enforces it.**
   Each number must be an **OPEN** GitHub issue, asked **four-valued**: only a payload
   affirmatively naming that number **and an OPEN state** is `present` and permits a grant; an issue
   GitHub answers does not exist is `ISSUE-ABSENT`; an issue GitHub answers is CLOSED is
   `ISSUE-CLOSED`; and an issue whose existence could **not be asked** — no `gh`, no auth, a
   network/API failure, an unparseable payload, or **any diagnostic that does not say the issue is
   missing** — is `ISSUE-UNVERIFIABLE`. The non-granting states are **textually distinct** because
   they are different operator actions: *"that issue number is wrong"* / *"that issue is closed"* /
   *"this box cannot reach GitHub"*. **`gh issue view` exits 1 for both** (measured on gh 2.98.0: `GraphQL: Could not resolve to
   an issue or pull request with the number of N.` versus `HTTP 401: Bad credentials`), so an
   exit-code-only test is exactly the two-valued predicate that always picks the permissive answer — it
   would grant a deferral over issues **nobody confirmed exist**. The verdict therefore comes from the
   diagnostic, anything unrecognised is a could-not-ask, and a could-not-ask is **never** read as
   verified.

   **AND "RETRIEVABLE" WAS NOT ENOUGH — THE CHECK IS DELIBERATELY STRONGER THAN THE CONDITION THAT
   ASKED FOR IT (#3626 round 3).** `gh issue view` returns the number and **exits 0 for a CLOSED
   issue**, so a number-only test made *"the finding is tracked"* satisfiable by an issue closed as a
   duplicate three weeks ago: `present` ⇒ `GRANTED` ⇒ `RESULT: PASS`, the finding permanently untracked
   while the block asserted it was filed. The lead's condition said *retrievable*, and
   closed-is-retrievable satisfies the letter — but the call site, the scanner and the spec all state the
   stronger **not-dropped** claim, and a closed-as-duplicate issue means the finding IS dropped. So the
   claim was made TRUE rather than three claims weakened to match a weaker implementation. **The
   generalisable ruling: when an implementation satisfies the LETTER of a condition while contradicting
   the PROPERTY every statement of it claims, strengthen the implementation.** A false refusal is
   recoverable (reopen the issue, or file a fresh tracking issue and re-authorize) and is the fail-closed
   direction.

   **The disposition backstop counts VERIFICATIONS PERFORMED; it does not test the string.** It was
   `[ -z "$ISSUES" ]` — a non-emptiness test standing in for a verification test — and `ISSUES=","`
   passes it, splits into **zero words**, never runs the loop body, and returns with the state still
   `granted`: `findings: DEFERRED` and `RESULT: PASS` with not one `gh issue view` executed. It was
   unreachable only because the `issues=` *pattern* forbade that value, which is **precisely the upstream
   dependency a backstop must not have**. The count of verifications must now EQUAL the count of declared
   comma-separated fields.

   **Three field rules, both marker kinds, one parser.** `base=`/`head=` are **exactly 40 hex** — an
   abbreviated sha is `MALFORMED`, never `STALE`, because it names *this* review in a spelling the form
   forbids and an authorizer sent to re-check *which review* they named would find nothing wrong with it.
   A recorded `reason` keeps its internal whitespace **verbatim**; only the **block boundary** renders a
   control character as a visible escape, because the property required is one line per value, not
   collapsed whitespace. And a `reason` may **not contain either marker stem** — refused rather than
   escaped, since an authorizer has no legitimate need for one. That last one carries its own lesson:
   **the structural assert covers the CODE, while a RUNTIME value can inject what no source scan sees —
   an invariant over OUTPUT needs a check on the OUTPUT PATH.**

   **AND THE RULE IS OVER EVERY EMITTED VALUE, NOT OVER THE `reason` FIELD (roborev job 230).** Fixing
   the field and leaving the class cost a review round — the same guarantee one field over. The `reason`
   is the field an authorizer *chooses*, so refusing it removes that class outright; but a marker keyword
   also arrives through fields nobody chooses: an unauthorized commenter's **GitHub login** (which
   `UNAUTHORIZED` has to report in order to say who was refused), **`gh issue view`'s stdout and
   stderr** (which reach `deferral:` as an `ISSUE-UNVERIFIABLE` cause), the allowlist, and whatever a
   future key interpolates. So each of the two processes neutralises the keywords at its **one emit
   boundary** — `safe_value` in the structured scanner, `roborev_safe_line` in the wrapper (already the
   single gate for every block value and every DETAILS line) — and never per interpolation site, because
   **a per-site escape is a list to keep complete**. There, unlike the reason, the value is **redacted
   rather than refused**: it is an identity or a diagnostic the run must still report.

   Only where the keyword is **not continued by another letter**, because **a longer word is a different
   word** — the rule the parser already applies to `roborev-defer: findingsfoo`. That boundary is
   load-bearing, not cosmetic: the scanner's own file name embeds a keyword and is printed by the
   fail-closed `waiver: UNAVAILABLE (… tool: <path>)` cause an operator must read to fix the state, and a
   blanket substring redaction turns it into a diagnostic nobody can act on. Declared residual: a
   keyword *inside* a longer word is left alone — it carries no marker **form**.

   Two boundaries rather than one is safe because the transformation is **display-only**: every
   authorization decision (allowlist, scope, count, retrievability) is made on the **raw** value before
   any renderer runs, so a divergence between the two spellings can only redact differently — never
   grant. **That is why the same rule at two emit boundaries is fine where two marker PARSERS would not
   be: a parser decides, a renderer does not.** And it is deliberately **not** a security layer: a
   GitHub login admits letters, digits and hyphens and not colons or spaces, so it can contain
   `roborev-defer` but never a full stem, and an emitted line begins `deferral: UNAUTHORIZED (`, which
   the sole-content rule refuses. Spec conformance and invariant coverage — nothing more.

   **A PR-BODY LINK WAS ALSO REQUIRED, AND THAT LEG WAS DELETED (#3626, lead ruling).** An earlier
   revision demanded each `issues=` number also appear as a local, visible `#N` in the **PR body**
   (`PR-UNLINKED` otherwise), with recognisers for cross-repository references, alphanumeric suffixes,
   fenced blocks, code spans and HTML comments. It is gone, and **the reason is not the bypasses**:

   > **A PR body is editable at any time by anyone with write access, with NO per-edit attribution. A
   > top-level comment is permanent and attributable.**

   So the body-link leg was the **weaker artifact**, and it would stay weaker **even if Markdown parsed
   trivially** — an authorization the constrained party can silently rewrite after it is granted
   evidences nothing. The Markdown-recogniser problem was a **symptom**, not the cause. The requirement's
   own wording invited it: *"name where the finding went"* invited a **prose scan**, when the property
   actually wanted is that the finding is **TRACKED**. The census is kept because it is the evidence the
   class does not close — Markdown-handling references in that one predicate went **0 → 11** across two
   review rounds:

   | shape | round | status when the leg was deleted |
   |---|---|---|
   | `other/repo#3602` cross-repository | R1 | closed |
   | `#3602suffix` | R1 | closed |
   | fenced code block | R1 | closed |
   | `<!-- #3602 -->` HTML comment | R1 | closed |
   | `` `#3602` `` single-backtick span | R1 | closed |
   | ``` ``#3602`` ``` multi-backtick span | **R2** | **ACCEPTED (bypass)** |
   | `[#3602](https://example.com)` explicit link | **R2** | **ACCEPTED (bypass)** |
   | 4-space indented code block | — | ACCEPTED (declared residual) |
   | GFM autolinks, `[#N][ref]`, raw HTML, entity refs, nested emphasis | — | unhandled by any generation |

   #3312 (*remove the shared channel, do not pick a rarer delimiter*) and #3229's owner ruling (*a guard
   with known documented false-PASSes is worse than no guard, because it invites reliance it cannot
   support*) both apply. **Subtraction cannot introduce a false PASS**: with nothing predicted about the
   body, nothing is excused by it. Any future strengthening of the disposition must come from an
   **immutable or attributed** artifact — a structured GitHub relation, or the authorization comment
   itself — never from parsing the mutable body of the PR under review. **Reinstating a body scan is
   reinstating generation three.**

   **What it reports** is a distinct token, and never `NONE`:

   ```
   findings: DEFERRED (2, issues=#3602,#3613, authorized @<login>, job 262)
   deferral: GRANTED (author=@<login> issues=3602,3613 count=2 scope=base=<…> head=<…> job=262 reason=<…>)
   RESULT: PASS
   ```

   `NONE` stays reachable **only** from the record's structured `verdict` letter, so nobody grepping
   `findings: NONE` reads a deferred run as a clean review. The `deferral:` key states its own state even
   when nothing was granted — `NONE` / `STALE` / `MALFORMED` / `UNAUTHORIZED` / `COUNT-MISMATCH` /
   `ISSUE-ABSENT` / `ISSUE-CLOSED` / `ISSUE-UNVERIFIABLE` / `UNAVAILABLE`, every one leaving the FAIL — because *"your
   marker names the wrong job"* and *"there is no marker"* are different operator actions and a bare FAIL
   distinguishes neither. Per #3312's own finding, a **marker-only** comment with bad fields is
   `MALFORMED` while a comment carrying the marker **plus other content** is ignored **silently**
   (`NONE`): someone documenting the form never attempted an authorization, and a false accusation
   reprinted on every later run is worse than silence. A marker **attempt** is the stem followed by
   whitespace **or the end of the line**, so a marker-only comment that is exactly the stem
   (`roborev-defer: findings`) is `MALFORMED` and not a fail-quiet `NONE` — an author told "no
   authorization exists" re-reads syntax they typed and concludes the mechanism is broken.

   **`findings: UNKNOWN` and `findings: SKIP` are not deferrable, in any mode.** Those values mean the
   findings state was never *established* — we cannot count what we cannot see, so a deferral over one
   would be precisely a pass resting on a state we could not read. Only an affirmatively measured
   `PRESENT (n)` is deferrable, and a bare `PRESENT` with no count is refused for the same reason.

   **The two authorizations stay separately scoped and neither falls back to the other.** Distinct marker
   keywords, distinct summary keys (`waiver:` / `deferral:`), distinct verdict tokens (`WAIVED` /
   `DEFERRED`). An absence waiver confers **no** authority over `findings:`; a findings deferral confers
   **none** over `prompt-content:`. A run may legitimately carry both, each granted on its own marker and
   reported under its own key. Collapsing them would let a delivery-artifact waiver excuse a real defect —
   which is exactly the false PASS #3564 removed.

   `DEFERRED` is a value of the **closed** verdict grammar, non-failing **only** on the single coupled
   granted state that the grammar scan and the `findings:` gate **both** read — one state, not two, so
   they cannot drift into two opinions about whether one authorization was granted. Re-deriving the
   provenance per gate would *be* that drift.

   **And that admission is confined to one key, `findings:`, by construction** (roborev job 225). The
   scan carries each key's **name** beside its value and admits the token for `findings` alone; the
   deterministic-key affirmation backstop carries **no** `DEFERRED` arm and does not read the state at
   all. It was first written unconfined, on the reasoning that provenance is what matters and a
   key-scoped test has to be re-argued whenever a key is added — a real argument, and wrong here. **A
   waiver authorizes a *property*** (an absence) that only one key can ever report, so its provenance is
   the whole test and it is correctly not key-scoped. **A deferral authorizes a *named set of
   findings*** and says nothing about whether the reviewer's diff arrived, whether the push landed or
   whether the reviewed range matched — so an unconfined admission let **one** authorization excuse a
   check **nobody** authorized, and the only thing standing between it and a false PASS was that no other
   key *happened* to emit the token. That is lesson (1) below, verbatim.

   Relatedly, **no emitted diagnostic reproduces any part of either marker — not even its prefix.** The
   `MALFORMED` detail used to quote the whole required form, and that detail is interpolated into the
   `waiver:`/`deferral:` summary key: the block printed a complete, fillable authorization beside a live
   base/head/job, while a comment two lines from the interpolation asserted it never did. One fix in the
   shared scanner closed it for **both** kinds, because both details come from the one structured parse.
   The absence is now asserted against **every** diagnostic-emitting case rather than only the `NONE`
   state, where it held trivially — **a property asserted only where it cannot fail is not asserted.**

   **Two transferable lessons.** (1) *Delegating a key's failure to its neighbour is a latent false PASS* —
   the coupling is invisible while both keys are populated by the same event and evaporates in the first mode
   where they are not. Ask of every key: **what fails the run if this key alone goes bad?** (2) *A
   fail-closed argument for a default is only valid for the consumers that existed when it was written.* The
   `block_marker_count` `:-0` default was audited as fail-closed because `NONE` was the STRICT direction for
   `vacuity-tier1:`. Adding a consumer for which `NONE` is the PERMISSIVE direction silently inverted it, and
   no choice of default can fix that — `0` and *unmeasurable* are the same value. **The resolution is not a
   better default or a second signal but a REMOVED CONSUMER**: `NONE` is unreachable from a marker count at
   all (only the structured verdict letter yields it), so nothing derives a permissive verdict from that `0`
   and the original fail-closed argument holds unchanged. Re-derive such an argument whenever you add a
   consumer — and note that an intermediate version of this very fix added a separate `block_measured` flag,
   which went away with the prose reconstruction it guarded while this paragraph went on citing it for a
   round (caught by the C intent audit). **A doctrine line naming a mechanism is a claim about code and
   decays exactly like a comment: re-grep the symbol.**

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
`<base40>..<head40>` whose **HEAD endpoint is the worktree branch's HEAD** and whose base is
`git merge-base origin/main HEAD` — **not** `git rev-parse origin/main`, which is the ref's tip and equals
the merge-base only while the branch is not behind (#3392). The block's `assert-base:` key names both.
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
