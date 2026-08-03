# Design: narrow the `docs/` review exclusion and reconcile the census against it (issue #3229)

## Context

The delivery pipeline treats "roborev clean" as a merge condition. `.roborev.toml` currently sets
`exclude_patterns = ['docs/**', '*.md']`, which discards every path under `docs/` from the diff roborev
constructs — including the measurement harnesses the repo ships **by convention** under
`docs/reports/*-artifacts/`. On PR #3222 that produced an empty prompt for a 136-file code census; the
#2964 wrapper FAILed the round, so nothing unreviewed merged, but the class of PR became
non-certifiable. This design records how the exclusion is narrowed, and how the wrapper is made to
notice if it is ever wrong again.

### What is actually known about `exclude_patterns` (and how)

`roborev` is an **external, stripped Go binary** — `roborev v0.61.2` at `/usr/local/bin/roborev`, no
source available — so its behaviour was established by symbol inspection, an empirical replay, and finally
by **DISASSEMBLING the responsible function** (next section). Three findings from the observational phase
are load-bearing for everything below; the disassembly then made the mechanism exact.

1. **It is git pathspec, not a bespoke matcher.** The binary carries the symbols
   `git.FormatExcludeArgs` and `config.ResolveExcludePatterns`, and both string constants
   `:(exclude,glob)` and `:(exclude,glob)**/` are present. So the semantics are git wildmatch with
   `WM_PATHNAME`: anchored at the repository root, and `*` does not cross `/`. That is why the fix can be
   **verified with git itself** rather than with a re-implementation (see "Defence in depth").
2. **A slash-less pattern is applied recursively.** Replaying **21 real reviews** recorded in
   `~/.roborev/reviews.db` against their own `git_ref` ranges and diffing the census against the prompt
   actually sent, the only paths ever dropped were **25 paths, every one a `.md`** — including
   `.claude/agents/*.md`, `openspec/**/*.md`, `website/**/*.md` and top-level `CLAUDE.md`. `docs/**`
   cannot explain any of those, so `*.md` must be normalised to `**/*.md`. Corroborated locally:
   `git ls-files -- ':(exclude,glob)*.md'` leaves 1393 `.md` files (only the 11 top-level ones are
   excluded) whereas `':(exclude,glob)**/*.md'` leaves 0 — matching the observed behaviour, and
   confirming `docs/**/*.md` is valid supported syntax.
3. **Nothing but `.md` has ever been excluded.** In the same replay every non-`.md` file reached its
   prompt — `.github/workflows/*.yml`, `scripts/*.sh`, `scripts/flow/*.py`, `.rs`. Combined with (2),
   **`*.md` alone already excludes all ~1404 tracked `.md` files repo-wide**, and `docs/**` is the *sole*
   cause of executables under `docs/` being discarded. Deleting nothing but `docs/**` would already fix
   AC1 — at a token cost this design refuses to pay (see the rejected alternatives).

### RESOLVED: the exact algorithm, recovered by disassembly

The one question the observational phase could not answer — whether a pattern CONTAINING a `/` is passed
verbatim or is ALSO `**/`-prefixed — is **no longer open**. `git.FormatExcludeArgs` was recovered from the
stripped binary by parsing `.gopclntab` for symbols (real text base `0x401000`) and disassembling it. It is
eight lines:

```go
p = strings.TrimSpace(pattern)
p = strings.TrimRight(p, "/")
if p == "" { continue }
b0 = p[0]                       // read BEFORE TrimLeft
p = strings.TrimLeft(p, "/")
if p == "" { continue }
if b0 == '/' || strings.Index(p, "/") >= 0 {
    prefix = ":(exclude,glob)"       // verbatim, ROOT-ANCHORED
} else {
    prefix = ":(exclude,glob)**/"    // RECURSIVE
}
out = append(out, prefix+p, prefix+p+"/**")   // TWO pathspecs per pattern
```

It is on the **real diff path**, not a dead helper: callers are `git.GetDiffCtx`, `GetDiffLimitedCtx`,
`GetRangeDiffCtx`, `GetRangeDiffLimitedCtx`, `GetDirtyDiff`, and
`prompt.(*Builder).buildSinglePrompt` / `buildRangePrompt` / `resolveExcludes`.

**R1 — a slash-containing pattern is VERBATIM and ROOT-ANCHORED.** `docs/**/*.json` does **not** match
`website/src/content/docs/c.json`. The residual resolves in the *narrow* direction, which is the blast
radius this design wanted: the docs-scoped deny-list is exactly scoped and hides nothing elsewhere in the
tree.

**R2 — every pattern silently emits TWO pathspecs**, `prefix+p` and `prefix+p+"/**"`. That is how a bare
directory name excludes its whole subtree, and any port that emits only the first would under-count
exclusions (i.e. miss a swallow).

**R3 — the trailing-slash INVERSION, and it is the real trap.** `TrimRight(p, "/")` runs *before* the
contains-`/` test, so `docs/` → `docs` → slash-less → `**/docs` + `**/docs/**` = **RECURSIVE**, while
`docs/**` keeps its slash and stays root-anchored. **`docs/` and `docs/**` behave OPPOSITELY**, and the
trailing-slash form would also swallow `website/src/content/docs/**`. A trailing slash is a *silent
widening* of unbounded depth that reads like a harmless tidy-up.

**Decision: a trailing-slash pattern is a FAIL, not a NOTICE.** It is a configuration defect knowable
from the config alone, with no diff needed; the widening it causes is depth-unbounded and invisible in
every summary block that would otherwise read `PASS`; the remedy is a one-token edit (`docs/**`, or a
deliberate slash-less `docs` if recursion really is wanted); and a NOTICE in a block agents skim is
exactly how the original `docs/**` survived for months. Fail-closed also matches the key's existing
contract — the value grammar has no non-failing state that blocks anything, so a NOTICE could not stop
the very edit that reintroduces the bug. The cost is honest and accepted: because the check is
diff-independent here, a bad config edit reds *every* review round fleet-wide until it is fixed — which is
the intended pressure, not a side effect.

**R4 — a leading `/` root-anchors an otherwise-recursive slash-less name.** `/README.md` →
`:(exclude,glob)README.md` (root only) vs `README.md` → `:(exclude,glob)**/README.md` (any depth). It is
the ONLY way to root-anchor a slash-less name, and it is why `b0` is captured *before* `TrimLeft`.

**R5 — no negation / re-include capability, VERIFIED at the instruction level.** `FormatExcludeArgs`
performs only TrimSpace / TrimRight / TrimLeft / `Index`; there is no `!` handling and no re-include path.
This **upgrades "an allow-list is not expressible" from a working assumption to a verified fact**, which
retroactively proves the deny-list of D1 was *forced*, not a preference. Empty-after-trim patterns are
silently skipped.

**R6 — the model closes against the observations.** Today's `['docs/**', '*.md']` resolves to
root-anchored `docs/**` + `docs/**/**` and recursive `**/*.md` + `**/*.md/**`. That reproduces the 21-review
replay *exactly*: only `.md` ever dropped, at arbitrary depth, repo-wide, and never a non-`.md`. No
residual inconsistency remains between the disassembly and the measurements.

**R7 — the check must REPLICATE, not query.** No roborev flag prints the resolved pathspecs (`review
--help` has no `--dry-run`; `-v` is global-only), so the resolved set is simply not obtainable from the
tool. `config.ResolveExcludePatterns` / `loadRepoExcludePatterns` merge the global
`~/.roborev/config.toml` with the repo `.roborev.toml` (global is currently `[]`), which confirms the union
read. Two adjacent mechanisms are deliberately NOT in scope and must not be conflated: `max_prompt_size`,
`exclude_branches` and commit-message exclusion (`IsCommitMessageExcluded`) are separate keys, and
`git.EnsureLocalExcludePattern` / `infoExcludePath` writes `.git/info/exclude` — a different mechanism
entirely.

**Maintenance obligation.** All of the above is pinned to **`roborev v0.61.2`**. An upgrade could change
`FormatExcludeArgs`, which would silently invalidate the port while every summary block still read `PASS`,
so the version is recorded beside the ported code and re-verification on upgrade is a stated obligation
(the same discipline the #2964 live probe already carries for version bumps).

### What lives under `docs/`, measured

1103 tracked files under `docs/`, by extension: md 356, txt 272, json 135, err 66, log 53, jsonl 46,
**sh 32**, png 23, svg 22, **py 22**, gz 18, pdf 10, mmd 10, html 9, yml 4, yaml 3, jfr 3, csv 3, c 3,
**bt 3**, extensionless 3, cql 2, toml 1, tex 1, rs 1, diff 1. Of those, 578 are under
`docs/reports/*-artifacts/**`: txt 246, json 133, err 66, log 53, jsonl 45, sh 30, py 21, gz 18, svg 12,
md 7, csv 3, c 3, bt 3, extensionless 3, yaml 2, cql 2, toml 1, rs 1, diff 1. The three extensionless
files are **compiled binaries** (`ws0-readbw`, `ws0-stream`, `offcputime-bigmap`) — correctly not code
to review.

Read that histogram as the budget: the ~570 raw-output files (`txt`/`json`/`err`/`log`/`jsonl`) are what
makes a blanket un-exclusion unaffordable, and the ~60 `sh`/`py`/`bt`/`c`/`rs`/`toml`/`cql`/`yml` files
are what the reviewer must see.

## Recommended design

### D1 — the config: a prose/artifact deny-list, not a blanket path glob (AC1)

`exclude_patterns` becomes: **`*.md` kept unchanged** (it already performs all prose exclusion,
repo-wide), and `docs/**` replaced by **docs-scoped exclusions of the non-code artifact extensions
only** — at minimum the high-count raw-output and binary/image classes from the histogram:

```
'*.md',
'docs/**/*.txt', 'docs/**/*.json', 'docs/**/*.jsonl', 'docs/**/*.log', 'docs/**/*.err',
'docs/**/*.csv', 'docs/**/*.png', 'docs/**/*.svg', 'docs/**/*.gz', 'docs/**/*.pdf',
'docs/**/*.jfr', 'docs/**/*.html', 'docs/**/*.mmd', 'docs/**/*.tex', 'docs/**/*.diff'
```

(`docs/**/*.x` matches `docs/a.x` as well as `docs/a/b/c.x` — git's `**/` matches zero or more
components.) The consequence is that `.py`, `.sh`, `.bt`, `.c`, `.rs`, `.toml`, `.cql`, `.yml`, `.yaml`
under `docs/` are **reviewed**, which is AC1. Each of these patterns contains an interior `/`, so by R1 it
is **root-anchored** — the scope is exactly `docs/`, and nothing under `website/src/content/docs/` or any
other nested `docs` directory is affected. **None of them may be written with a trailing slash** (R3): a
`docs/` form would invert to recursive and re-widen the blast radius, which is why the wrapper FAILs on a
trailing-slash pattern.

**A deny-list is forced, not preferred — and this is now VERIFIED, not assumed.** `git.FormatExcludeArgs`
does only TrimSpace / TrimRight / TrimLeft / `Index`: there is no `!` handling and no re-include path at
the instruction level, and git pathspec supports none inside `:(exclude)` either. "Review these extensions
and nothing else" is therefore **not expressible**; the only lever is narrower excludes.

**The deny-list's known weakness, stated up front.** A *new* artifact extension appearing under `docs/`
is silently re-admitted to review prompts. That is a **token-cost** problem, never a correctness one:
the failure direction of a deny-list miss is "the reviewer sees noise", and D2's check is what
guarantees it can never invert into "the reviewer silently sees nothing". The maintenance signal is the
same check: adding a pattern to the config without adding the extension to the wrapper's declared
artifact set FAILs loudly at the next review round (see D2's residual table).

Three extensionless compiled binaries under `docs/` are not matched by an extension deny-list. They
remain classified non-code by the census (extensionless under a declared prose directory), and git
renders them as `Binary files … differ`, so the token cost is bounded; they are not worth a
by-name exclusion, and this is recorded rather than fixed.

**Operational risk to record:** `.roborev.toml` is a machine-managed file (`roborev config set` rewrites
it, comments and all). A rewrite that drops or reorders the list would silently restore the blind spot —
which is precisely why the fix cannot be a config edit alone.

#### Alternatives rejected

| Alternative | Why it was rejected |
|---|---|
| **(a) Drop `docs/**` entirely, keep only `*.md`** | Correct on AC1 and the *simplest* change — but it admits ~570 raw run-output files (`txt`/`json`/`err`/`log`/`jsonl`) plus binary/image blobs into review prompts. A genuine review already runs 398k–649k input tokens; a report PR's artifact tree would blow past the prompt budget (roborev's own `max_prompt_size` fallback switches to *file paths only*, i.e. a degraded review) for zero review value. |
| **(b) Relocate the harnesses out of `docs/`** | **Explicitly ruled out by the owner in #3229.** Shipping a harness beside the report it produced is the convention and it stays. |
| **(c) Global (slash-less) exclusion of `*.txt`, `*.json`, …** | Per the recursive-normalisation finding, a slash-less pattern applies **repo-wide**. That would newly hide real config and data files elsewhere in the tree (`test-data/**/*.json`, workflow-adjacent JSON, fixtures) from review — a genuine regression traded for a shorter pattern list. |
| **(d) Ask roborev for an allow-list / negation** | Not expressible today (see above) and it is an upstream feature request on a binary we do not control. A worthwhile upstream ask; not this change. |

### D2 — defence in depth: reconcile the census against the effective exclusion set with git (AC3, AC4)

This is the key idea, and the disassembly makes it **exact rather than approximate**. A new **pre-enqueue**
check, under its own greppable key **`census-exclusion:`**, does *not* re-implement glob matching and does
*not* trust the reviewer's narration. It **ports roborev's pathspec construction and lets git do the
matching**:

1. Read the effective `exclude_patterns` (repo ∪ global, per `ResolveExcludePatterns`).
2. **Construct the pathspecs as an exact port of `git.FormatExcludeArgs`** (the eight lines above):
   trim; skip if empty; capture `b0` BEFORE `TrimLeft`; root-anchored `:(exclude,glob)<p>` when `b0 == '/'`
   or `<p>` contains `/`, else recursive `:(exclude,glob)**/<p>`; and emit **both** `<p>` and `<p>/**`.
   FAIL on a trailing-slash pattern (R3's inversion) rather than silently resolving it recursively.
3. Ask git which census paths survive:
   `git diff --name-only -z --no-renames <base>...HEAD -- <pathspecs>`.
4. **Swallowed set = the census's CODE paths − the survivors.** Non-empty ⇒ **FAIL, before anything is
   enqueued**, naming the swallowed paths and the pattern that ate each.

The split of labour matters: **construction** is a port of a fully specified 8-line function, and
**matching** is delegated to git, which is the same matcher roborev delegates to. Neither half is guessed,
so the reconciliation of AC4 is exact — a materially stronger position than the earlier
"evaluate-both-readings and fail on either" stance, which the disassembly showed would have been not
conservative but simply **WRONG**: a census path under `website/src/content/docs/` would have been reported
swallowed while roborev in fact delivers it, i.e. a false FAIL on legitimate report PRs. Because
construction is a port, the pinned version (`roborev v0.61.2`) is recorded beside it and re-verification on
upgrade is a maintenance obligation.

`--no-renames` is passed because the census itself is computed with `--no-renames`; without it the two
path sets would not be comparable. The read is **NUL-safe** (`-z`, bash arrays, no word splitting): one
tracked path under `docs/` contains a literal double quote and spaces
(`docs/research/CQLite Writes (M5) — Analysis & Recommended Paths.md`), and `git diff --name-only`
without `-z` would emit it *quoted*, which would silently never match a census entry.

**Why this satisfies AC4 by construction.** The wrapper stops *asserting* what roborev excludes in a
prose comment and starts *computing* it with the same tool roborev uses. The two classifications are
still independent (they must be — an oracle that derives one from the other could never detect a
config regression), but they are compared on every run, so a second silent divergence of this shape is
impossible. The declared residual, both directions:

| Divergence | Detected as | Consequence |
|---|---|---|
| Config excludes a path the census calls CODE | `census-exclusion: FAIL` naming the path + pattern, pre-enqueue | Blocked round; **the defect this issue is about** |
| Census calls a path non-code that the config does *not* exclude | Not a failure | The file is sent to the reviewer; **noise only**, bounded by the artifact set's size |

The census's docs-scoped artifact set and the config's docs-scoped deny-list must therefore agree on the
artifact extensions, or the *first* row fires on every legitimate report PR: the census's prose set is
`md markdown mdx txt rst adoc`, so `.json`, `.jsonl`, `.log`, `.err`, `.csv`, `.svg` … currently count
as **CODE** and would be reported swallowed. The design therefore declares **one** artifact-extension
constant in `scripts/flow/roborev-review-oracles.sh` (alongside the existing
`CODE_FREE_EXTENSIONS` / `CODE_FREE_EXTENSIONLESS_PREFIXES`), used to classify census paths **under the
declared prose directories** as non-code, and mirrored by the config's docs-scoped patterns. The check
remains non-vacuous because its verdict is computed from **what the config file actually says**, so
restoring `docs/**` — or any hand edit, or a `roborev config set` rewrite — still FAILs even though the
constant is untouched.

**Reading the effective set: files, not the binary.** The check parses `.roborev.toml` (repo) and
`~/.roborev/config.toml` (global) directly, rather than shelling out to `roborev config get`. Two
reasons: it keeps the check **hermetic and stub-testable** (the existing suite drives a stub `roborev`
via `STUB_*` env vars and must be able to vary the *config*, not the binary), and it avoids an ordering
change — `command -v roborev` is validated *after* the census, and a binary-dependent check would have
to be moved before it. Tradeoffs accepted and mitigated:

- The parser must handle **table scoping** (`exclude_patterns` under `[ci]` is not the top-level key —
  today's file has the real key above the first table) and the single-line array form the file uses. To
  keep the parser minimal the committed value stays a **single-line array**; the parser must still
  refuse to guess.
- **Brittle-TOML risk is answered by failing closed, not by best-effort.** A key present whose value is
  not a parseable bracketed array ⇒ `FAIL (exclusion set unreadable: …)`. A genuinely absent key or
  absent config file ⇒ `PASS (no exclusion patterns configured)`, because "no exclusions" cannot swallow
  anything. "We could not tell" is never aliased to "nothing is excluded".
- The **global/repo merge is a UNION**, per `config.ResolveExcludePatterns` / `loadRepoExcludePatterns`
  (today the global list is `[]`, so the repo list is the whole effective set). This is no longer a
  fail-closed guess; the check takes the union because that is what the tool does.
- When `roborev` *is* invocable, the parsed set is **corroborated** against
  `roborev config get exclude_patterns` (which prints the comma-joined repo value). A pattern the binary
  reports that our parse lacks is `FAIL (exclusion set drift: …)` — that direction could hide a swallow.
  The reverse is a NOTICE. When the binary is absent the corroboration reports `UNAVAILABLE` and the
  verdict stands on the file parse, so hermeticity is preserved.

**Rejected alternatives for the check:**

- **Hand-roll wildmatch in bash.** A second, independent implementation of `WM_PATHNAME` semantics
  (`**` vs `*`, anchoring, character classes) is exactly the kind of near-miss re-implementation that
  produced this bug in the first place — a comment asserting behaviour instead of measuring it. git is
  already installed, already the authority, and already how roborev does it.
- **Parse/trust `roborev config get` output alone.** It prints the configured value, not the resolved
  pathspecs, and it makes the check non-hermetic (it needs the real binary) — so the regression tests
  that must pin this behaviour could not run in `--lite`. Demoted to corroboration.
- **Wait for `prompt-content:` to catch it after the fact.** That is today's behaviour: it works, but it
  pays for a full review round to learn a fact that is knowable deterministically before the enqueue,
  and it reports the failure under a key whose meaning is "the reviewer did not get the files", not
  "your config ate them". AC3 explicitly asks for the distinct, greppable, pre-enqueue reason.

### D3 — placement in the wrapper (surfaces and the four registration points)

The oracle function lands in **`scripts/flow/roborev-review-oracles.sh`** (227 lines), beside
`roborev_census` and the `code-free:` check whose classification it must share. This keeps
`scripts/flow/roborev-review.sh` — already **798 lines**, against the ~800 campsite target — from
growing past it: the wrapper gains only the call site plus registration.

The call slots in **after `roborev_census` and the `code-free:` check, before the checks-file validation
and the enqueue**, and requires touching **four** places or it is decorative:

1. state initialisation (the key defaults to an explicit `SKIP`, never blank);
2. `emit_summary()`'s FIXED key order — `census-exclusion:` between `code-free:` and `job-record:`;
3. the call site itself;
4. the **verdict-scan failing-capable key list** — a key absent from that list cannot fail the run, so
   omitting it would ship a check whose FAIL is cosmetic. The scan already fails on values beginning
   `FAIL`, `FINDINGS`, `ERROR`, `INCONSISTENT`, so the value grammar reuses `FAIL`/`PASS`/`SKIP`/`NOTICE`
   with no new scan vocabulary.

Plus `usage()` documentation in the wrapper's two help regions.

**Alternative considered:** a fourth sourced file (`roborev-review-config.sh`) alongside the oracles and
checks files. Rejected for now — the oracles file has room, the new code shares the classification
constants it would otherwise have to import, and every additional sourced file adds another
missing-or-truncated fail-closed validation surface. Revisit if the oracles file approaches the ~800-line
target.

### D4 — the demonstration is a recorded live probe, not an assertion (AC2)

AC2 is satisfied by **running** the sanctioned wrapper, not by reasoning about it:

```
bash scripts/flow/roborev-review.sh --agent codex --model gpt-5.6-sol --repo <abs> --base origin/main
```

against a PR #3222-shaped diff — executables under `docs/reports/*-artifacts/` — with the narrowed
config in place. Recorded in the PR body: the `census:` counts, the `code-free:` and
`census-exclusion:` lines, the `prompt-content:` line (expected `PASS (<n>/<n> code census paths
present)`), and the **input / cached / output token counts** from the job record, which must sit in the
**genuine-review band (398k–649k in / 5.0k–6.3k out, minutes of wall time)** rather than the **vacuous
baseline (~18.7k in / 0 cached / 53–56 out, ~8s)** — the signature PR #3222 actually produced was
15,443 in / 89 out.

The probe diff **must also include a file under a nested `docs` directory** (e.g. under
`website/src/content/docs/`) with one of the deny-listed extensions — no longer to *resolve* an ambiguity
(the disassembly did that) but as the **end-to-end confirmation of R1**: because a pattern with an interior
`/` is root-anchored, that nested path must still be DELIVERED to the reviewer. Presence confirms the port
live; **absence would falsify it** and is a blocking finding, not an acceptable outcome to record — the
pattern list and the check's construction both rest on R1 being right.

The probe needs the network and a live reviewer, so like the #2964 worktree probe it is **documented and
recorded, never gate-run**.

### D5 — hermetic regression tests (AC5)

`scripts/tests/test_roborev_review_guard.sh` (1638 lines, 77 cases, fully hermetic: a stub `roborev`
written first on `PATH`, `STUB_*`-driven, with a hermeticity meta-assert and no network/cargo/real
reviewer) gains cases in its existing `== case (<id>): <desc> ==` style, continuing the code-free
family's `(c2*)` lettering. The fixture helper gains the ability to write the work repo's **own
`.roborev.toml`**, which is what makes a config-regression case expressible at all. Minimum coverage:

1. **executables under `docs/`** (`.py`/`.sh`/`.bt` under `docs/reports/x-artifacts/`) ⇒ `code-free: PASS`,
   `census-exclusion: PASS`, review **IS** enqueued;
2. **prose-only under `docs/`** ⇒ `code-free: FAIL` and `assert_never_enqueued` — the #2964 behaviour is
   preserved, not loosened, and this is the case that proves the change did not trade one blind spot for
   the opposite one;
3. **a config that would swallow the census** (`exclude_patterns` restored to `['docs/**','*.md']`) ⇒
   `census-exclusion: FAIL` naming the swallowed paths, `RESULT: FAIL`, `assert_never_enqueued`;
4. **key order** — `census-exclusion:` appears exactly once, between `code-free:` and `job-record:`
   (`assert_one_block` + the order assert);
5. **unreadable vs absent config** — a malformed value FAILs; an absent key/file PASSes as "no exclusion
   patterns configured";
6. **a path with spaces and a literal quote** survives the comparison (the NUL-safety regression);
7. **corroboration `UNAVAILABLE`** when the stub does not answer `config get`, and the drift FAIL when it
   reports a pattern the parse lacks;
8. **the ported construction itself, case by case** (R1–R5) — a slash-containing pattern leaves a NESTED
   `docs`-directory census path SURVIVING (the false-FAIL regression the earlier both-readings design would
   have shipped); a bare directory name excludes its subtree through the `<p>/**` sibling; `/README.md`
   excludes only the root file while `README.md` excludes at any depth; a **trailing-slash** pattern FAILs
   naming the inversion; a whitespace-only pattern is skipped, not treated as match-everything.

The suite runs under the `roborev-lints` gate component, which is in **both** `COMPONENTS` and
`LITE_COMPONENTS` — so a regression FAILs the fast loop rather than costing a review round. Its tally
line stays `GUARD-TEST RESULT: …`, distinct from every gate/wrapper verdict.

### D6 — doctrine, in the same change (AC6)

The false claim to be retired is "roborev **EXCLUDES non-code paths from the diff it builds**". It is
false in both halves: the exclusion is a **configured git pathspec set**, not a code/non-code judgement,
and under `docs/**` it excluded *code* — which is the whole issue. Doctrine is amended to state the true
mechanism, to name the `docs/reports/*-artifacts/` harness convention explicitly, and to stop implying
that everything under `docs/` is code-free.

Every surface that repeats the claim is corrected in the same change, or the doctrine drifts against
itself: `CLAUDE.md` (roborev rule 4 and its T3 sentence, plus the docs-only/CITE-AND-WAIVE region),
`website/src/content/docs/agents-developing/roborev-findings.md` (rule 4 and its T3 paragraph),
`website/src/content/docs/agents-developing/delivery-pipeline.md`, `.claude/agents/flow-lead.md`,
`.claude/agents/flow-closer.md`, `.claude/skills/flow-implement/SKILL.md`, the header comments of all
three `scripts/flow/roborev-review*.sh` files (including the `roborev_check_prompt_content()` comment
that states the now-falsified claim outright), and this capability's own requirements in
`openspec/specs/roborev-review-guard/spec.md`.

The website deploys from `.github/workflows/docs-site.yml` (`Docs Site`, `deploy` job via
`peaceiris/actions-gh-pages@v4`); PRs build and link-check but do **not** deploy. So publication is
accepted **after merge**, by fetching the page and grepping for a distinctive phrase introduced by this
change — never by an HTTP 200, since the CDN can serve the previous page for ~3 minutes after a
successful deploy.

### D7 — the backfill ruling is recorded, and the decision stays the owner's (AC7) — RULED: accept as-is

AC7 asks for a *recorded decision* about the already-merged, never-reviewed harness code from #3026 /
#3100 / #3217, not for a particular decision. The requirement is therefore written as "the change
records the owner's ruling **and its reason**", with the scenario covering **both** branches — a
retroactive review pass (the natural mechanism: run the sanctioned wrapper over a range or a
reconstructed branch containing those paths, now that they are reviewable) or explicit
acceptance-as-is with the reason stated (e.g. #3222's harness already had a full adversarial hand review
recorded in the PR). Leaving it unaddressed is the only failing outcome.

#### THE RULING (owner, 2026-08-03): ACCEPT AS-IS. No retroactive review pass.

Recorded here in full, because an unrecorded "we decided it was fine" is indistinguishable from nobody
having looked. The reasoning, not merely the verdict:

1. **The exposure is BOUNDED by what the code is.** Every affected file is a *measurement harness* —
   a Part A/B driver, an off-CPU classifier, a demangler, a counter parser, a summarisation tool. None of
   it ships in the library, none of it is imported by `cqlite-core`/`cqlite-cli`/the bindings, and none of
   it runs in CI or the agent gate. A defect in it can corrupt a *report's numbers*; it cannot corrupt a
   release, a user's data, or a gate verdict. That is a materially different risk class from unreviewed
   library code, and it is the reason the decision can go this way at all.
2. **The largest tranche ALREADY had a full adversarial pass.** #3222's 34 executables were hand-reviewed
   file by file when the wrapper refused to certify them, and that review is recorded in the PR. It found
   **no blockers**, and it *did* find real defects — a 4th silent-failure instance where every driver log
   fabricated `rc=0` (`$(…)` resets `$?`) plus two provenance defects — all fixed before the PR merged. So
   the biggest slice of the exposure is not unreviewed; it is reviewed by a *more* expensive mechanism
   than roborev.
3. **The class cannot recur silently, which is what a backfill would actually be buying.** The value of a
   retroactive pass is mostly the assurance that the *next* one will not slip through. That assurance now
   comes from a mechanism instead: `exclude_patterns` no longer contains a blanket directory glob, the
   pre-enqueue `census-exclusion:` check FAILs closed (naming the paths and the pattern) if it ever does
   again, and the `(cx*)` hermetic cases fail the `--lite` loop on a regression. A backfill adds no part
   of that.
4. **Retroactively reviewing code whose outputs are already banked buys audit theatre, not safety.** The
   reports those harnesses produced are merged and have already been acted on. A finding now would not
   un-bank a number; it would produce a comment on a historical artifact. Spending review rounds — and
   a reviewer's attention — on that instead of on live code is a worse allocation, and pretending
   otherwise would be the dishonest part.

**What would change this ruling:** any of that harness code being promoted into a shipped path (a gate
component, a CI step, an imported module). At that moment it stops being a measurement artifact and
inherits the review obligation of the surface it joins. That is a rule about *promotion*, not about
history, and it is the standing follow-up this ruling leaves behind.

## Follow-ups (named here, deliberately not fixed here)

- **`scripts/ci/classify-docs-only.sh` has the same defect in the correctness gate.** `is_docs_file()`
  classifies with a blanket `case "$path" in docs/*) return 0`, so a PR touching only
  `docs/reports/*-artifacts/*.sh` is classified **docs-only** and short-circuits `pr-gate-core` to green.
  Same shape — "a path glob swallows executables under `docs/`" — but in the gate that decides whether
  the code is *tested*, not merely reviewed. Its test is `scripts/tests/test_classify_docs_only.sh`. To be
  **filed as its own issue** during this change; entangling a review-gate config change with a
  correctness-gate change in one PR would make both harder to certify.
- **Upstream ask on roborev:** an allow-list / negation form for `exclude_patterns`, and a non-zero exit
  when the constructed diff is empty because everything was excluded.
- **Re-verify the port on any roborev upgrade.** The construction is a port of `git.FormatExcludeArgs` as
  it exists in **`roborev v0.61.2`**; an upstream change to that function would silently invalidate the
  check while every summary block still read `PASS`. The version is recorded beside the ported code, and a
  version bump obliges re-disassembly (or an upstream source read, if one becomes available) before the
  check is trusted again — the same discipline #2964's live probe carries for version bumps.
- **Deny-list drift watch:** a new artifact extension under `docs/` is re-admitted as review noise until
  the pattern list is extended (bounded, non-correctness — see D1).

## Doctrine compliance notes

- **No-heuristics mandate (#28):** unaffected. The mandate governs inferring on-disk TYPE/format from
  byte content in the SSTable read path. The new check infers nothing: it asks git to apply configured
  pathspecs and compares two path sets, and its only possible action is to fail closed.
- **Format authority:** not applicable — no on-disk format surface is touched.
- **Campsite rule:** the new logic lands in the 227-line oracles file specifically so the 798-line
  wrapper does not cross the ~800-line target; the 1638-line test file is over the ~1500 test target
  already, so its growth is expected to be flagged and either split by responsibility or run with
  `CQLITE_ALLOW_FILE_GROWTH=1` and a note linking #1135.
- **Wiring evidence:** the public surface is `scripts/flow/roborev-review.sh` — the sanctioned
  invocation every flow-\* skill and agent calls. The end-to-end evidence is the recorded live probe
  (D4) plus the hermetic cases that drive the real wrapper end-to-end via its own summary block (D5).
