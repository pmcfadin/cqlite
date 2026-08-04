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

### D1 — the config: artifact exclusions scoped to artifact-bearing DIRECTORIES (AC1)

`exclude_patterns` becomes: **`*.md` kept unchanged** (it already performs all prose exclusion,
repo-wide), and `docs/**` replaced by the **intersection of a non-code artifact extension and an
artifact-bearing directory** — `<artifact-dir-glob>/**/*.<ext>` over exactly four directory globs:

| directory glob | what it holds | artifact files (measured) |
|---|---|---|
| `docs/reports/*-artifacts` | per-issue measurement artifacts (the #3229 convention) | 577 |
| `docs/round-artifacts` | soak/round measurement output | 53 |
| `docs/**/jfr-reports` | JFR profiling output | 7 |
| `docs/sstables-definitive-guide/diagrams` | generated diagram renders | 30 |

crossed with the high-count raw-output and binary/image classes from the histogram: `txt json jsonl log
err csv png svg gz pdf jfr html mmd tex diff` — 4 × 15 + `*.md` = **61 patterns**.

(`<dir>/**/*.x` matches `<dir>/a.x` as well as `<dir>/a/b/c.x` — git's `**/` matches zero or more
components.) The consequence is that `.py`, `.sh`, `.bt`, `.c`, `.rs`, `.toml`, `.cql`, `.yml`, `.yaml`
under `docs/` are **reviewed**, which is AC1. Each of these patterns contains an interior `/`, so by R1 it
is **root-anchored** — nothing under `website/src/content/docs/` or any
other nested `docs` directory is affected. **None of them may be written with a trailing slash** (R3): a
`docs/` form would invert to recursive and re-widen the blast radius, which is why the wrapper FAILs on a
trailing-slash pattern.

#### D1a — why DIRECTORY-scoped, and why the intermediate `docs/**/*.<ext>` form was retired

The first revision of this change wrote the artifact patterns as `docs/**/*.<ext>` — an extension sweep
across **all** of `docs/`. That form does **not** satisfy this change's own stated asymmetry ("noise, never
blindness"), and round 6 retired it. **The asymmetry was originally written unqualified; this change
falsified it, and the corrected form is scoped:** it is sound for **inert dumps** — `.txt`/`.log`/`.err`
run output — where exclusion costs only **noise**. It is **false** for **code-bearing formats**
`.json`/`.html`/`.svg`, which can be *functional configuration under any path*, and for those exclusion is
**blindness**, not noise. Hence the rule the corrected form yields: exclusion of a code-bearing format must
be **scoped by directory, never by extension alone**. Two live cases in this
repository prove it:

- **`docs/observability/grafana/dashboards/cqlite-overview.json`** — guarded by the **full agent gate's own
  `kit-dashboard-drift` component**, so the repository already treats it as correctness-bearing. Under
  `docs/**/*.json` a PR editing it was dropped from the reviewer's diff **and** classified code-free: the
  gate says "this is correctness-bearing" while the review path says "there is nothing here to review".
- **`docs/reports/delivery-telemetry.schema.json`** — the schema governing the delivery ledger, hidden the
  same way.

Measured on this branch: 672 tracked `docs/` files carry an artifact extension; **667** lie inside the four
directories above and remain excluded, and the **5** that do not are now delivered to the reviewer (the two
above, `docs/reports/delivery-telemetry.jsonl`, and two `sstables-definitive-guide` artifacts that sit
beside the prose rather than in `diagrams/`). With the scoping in place the asymmetry is **true as
written**: a *new artifact directory* is silently re-admitted to **review** (noise, a token cost), and
functional configuration under `docs/` can no longer be hidden (no blindness).

The doctrine surfaces state the corrected asymmetry **together with the counterexample that falsified it**
— naming `cqlite-overview.json` and the `kit-dashboard-drift` component that guards it — rather than as a
timeless claim, because a line that carries its own counterexample is much harder to re-break than one that
reads as always having been true. The generalisable part, worth more than this configuration: **an extension
describes a FORMAT; a directory records an INTENT** — someone decided that tree holds artifacts. That makes
a directory the better proxy for "generated", which is why the scoping is directory-first everywhere here.

**It stays extension-scoped WITHIN each directory — never a blanket `<dir>/**`.** That was the tempting
simplification and it would reintroduce this issue's original defect: these directories deliberately hold
executable code beside their output. Measured: **63** tracked `.sh`/`.py`/`.rs`/`.c`/`.bt`/`.cql`/`.yaml`/
`.toml` files under `docs/reports/*-artifacts/` alone, plus a `.py` under `docs/round-artifacts/`. Those
harnesses *are* the 136-path census `docs/**` swallowed on PR #3222.

The census-side mirror follows the same shape (`CODE_FREE_ARTIFACT_EXTENSIONS` ∩
`CODE_FREE_ARTIFACT_DIR_GLOBS`), matched **component-wise** to git's `:(glob)` semantics rather than with a
shell `case` — bash's `*` crosses `/`, so `docs/reports/*-artifacts/*` would also match
`docs/reports/a/b-artifacts/x`, which git's `*` does not. The globs are held in an **array**: as a
space-separated string, unquoted iteration pathname-expands them against `$PWD`, silently reducing the
classification to "the directories that exist in this checkout" (measured while writing it —
`docs/**/jfr-reports` collapsed to the single existing one and `docs/jfr-reports/a.html` stopped matching).
Because the two representations are one fact written twice, the regression suite **derives** the expected
pattern set from the constants and asserts **set equality** against the committed `.roborev.toml` using the
wrapper's own TOML parser, rejecting both retired forms off the *parsed* set (never a file-wide grep — this
very file documents the forms it retired).

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
| **(a) Drop `docs/**` entirely, keep only `*.md`** | Correct on AC1 and the *simplest* change — but it admits ~570 raw run-output files (`txt`/`json`/`err`/`log`/`jsonl`) plus binary/image blobs into review prompts. A genuine review on a large diff already runs several hundred thousand input tokens; a report PR's artifact tree would blow past the prompt budget (roborev's own `max_prompt_size` fallback switches to *file paths only*, i.e. a degraded review) for zero review value. |
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

#### D2a — WHICH `.roborev.toml`: the ROOT checkout's, not just the worktree's (blocker, measured)

The first implementation read `$REPO/.roborev.toml` and nothing else, and that produced a **false PASS on
this change's own branch**. roborev's daemon binds a repository by its **`repos.root_path`** — the ROOT
checkout — and reads THAT checkout's config. Under 1:1:1:1 every issue runs in a linked worktree, so
`$REPO` is `…/cqlite-wt/issue-N` while roborev is reading `…/workspace/repo/.roborev.toml`. Measured on
this branch:

| Source | `exclude_patterns` | What the check said |
|---|---|---|
| worktree `…/cqlite-wt/issue-3229/.roborev.toml` | the narrowed 16-pattern set | `census-exclusion: PASS (7/7 survive)` |
| root `…/workspace/repo/.roborev.toml` | `['docs/**', '*.md']` (pre-change) | what roborev **actually applied** |
| the real review | — | `prompt-content: FAIL (1/7 code census paths absent)` |

Replaying the ported construction with the OLD set reproduced exactly the 6 files present in the real
prompt (6/6), while the NEW set predicted 7 — so the port was right and the *input* was wrong. The
corroboration could not catch it either, because `roborev config get` was run with `cd "$REPO"` and
`config get` resolves the repo config **relative to its CWD**: from the worktree it answers the new set,
from the root checkout `docs/**,*.md`.

The fix evaluates **both** repository files plus the global one as a UNION, and fails if **any** would
swallow a census code path. Deliberately *not* "pick the one roborev prefers": which file wins is an
internal roborev detail, and betting on it is how a false PASS gets reintroduced on the next upgrade. The
root checkout is resolved from git — `rev-parse --path-format=absolute --git-common-dir` (a linked
worktree's `--git-dir` is `<root>/.git/worktrees/<name>`, its `--git-common-dir` is `<root>/.git`), with a
relative-path fallback for git < 2.31 where `--path-format` does not exist, and
`git worktree list --porcelain` (whose first entry is the MAIN worktree) as a last resort for a
non-standard `$GIT_DIR` name. If none of those answer, the check **FAILs closed**: reading one file and
reporting a PASS about it is the defect being fixed, so "we could not tell which file roborev reads" must
never degrade to "we read the one we could". When `$REPO` *is* the root checkout, `_rx_root` is emptied so
the single file is never double-reported. Corroboration now runs `roborev config get` from **every**
checkout whose config was read.

Because three sources are now in play, **every value line names the source** (`worktree-config` /
`root-config` / `repo-config` / `global-config`): "excluded by `docs/**`" does not tell
an operator which file to edit. The FAIL details additionally enumerate every source path read, and a
worktree run states explicitly that a narrowed worktree config does **not** override the root one.

#### D2a-bis — the arbitration Blocker A settled, and what it saved

Blocker A did more than fix a false PASS: it closed the **live existential risk to this entire change**.

Issue **#3234** had independently measured that `exclude_patterns` has **"no observable effect"** — a null
result. If true, AC1's narrowing would have been cosmetic and AC3 would have been guarding a mechanism
that does not apply, i.e. the change would have had no subject. The owner had ranked hypothesis **H2**:
*config resolves from the primary checkout, not the worktree.*

Both halves turned out to be operative, and they were found from opposite directions:

| Half | Established by |
|---|---|
| The **mechanism** — `exclude_patterns` really is applied, as `FormatExcludeArgs` pathspecs | this change: the disassembly + the 21-review replay (every dropped path a `.md` at any depth, no non-`.md` ever dropped) |
| The **ordering** — the daemon reads the ROOT path's config and snapshots it at start | #3234, whose single daemon restart happened to precede every config edit it made and never follow one; and this change's Blocker A, from the other side |

**Conclusion, recorded plainly: `exclude_patterns` WORKS.** #3234's null result was a
**worktree-config artifact**, not a broken mechanism. So AC1 is a genuine fix and AC3 guards a mechanism
that really applies. Two workers reached the same property from opposite ends, which is stronger evidence
than either alone.

#### D2a-ter — the PRE-EXISTING guard caught the NEW guard. Keep both layers.

The detail worth not smoothing over: `census-exclusion:` — the check added **by this change** — reported
`PASS (7/7 code census paths survive)` about a config roborev never read. What caught it was
`prompt-content: FAIL (1/7 code census paths absent)`: the **older** guard, already in the wrapper.

This is the strongest argument in the change for keeping both layers, and it is strong precisely because
it paid out in **the direction nobody plans for**. Defence in depth is usually justified as "the new,
sharper check will catch what the old one misses". Here the new check was the wrong one, and the crude
after-the-fact check — the one whose whole cost is that it only fires *after* a review round is paid for —
was the thing standing between a broken guard and a fleet-wide green. A layer is worth keeping not because
it is better than the other, but because its failure modes are **uncorrelated** with the other's.

Corollary, also worth recording: `prompt-content:` remains valuable even though `census-exclusion:` now
computes the same fact earlier and more cheaply. The cheap early check can be wrong about its INPUT; the
expensive late check reads what actually happened.

#### D2b — roborev's BUILT-INS are DELIBERATELY NOT MODELLED, and the residual is documented (AC4's SECOND BRANCH)

**The fact, and it is still true.** `exclude_patterns` is not the whole exclusion set. The binary ALWAYS
appends a hard-coded lockfile/cache deny-list — `**/Cargo.lock`, `**/cargo.lock`, `**/go.sum`,
`**/pnpm-lock.yaml`, `**/package-lock.json`, `**/packages.lock.json`, `**/yarn.lock`, `**/bun.lock`,
`**/bun.lockb`, `**/pdm.lock`, `**/uv.lock`, `**/poetry.lock`, `**/Pipfile.lock`, `**/Gemfile.lock`,
`**/composer.lock`, `**/flake.lock`, `**/mix.lock`, `**/pubspec.lock`, `**/Podfile.lock`,
`**/Package.resolved`, `**/.beads/**`, `**/.cache/**`, `**/.gocache/**`, `**/.kata.local.toml` — to the
pathspecs it hands git, with no configuration switch, no opt-out and no negation form. A census path one of
those eats is exactly as invisible to the reviewer as a configured swallow.

**The decision (owner-ruled, #3229; deferred in full to #3278): do not model it here.** This check
reconciles the census against the effective **`exclude_patterns`** — the configured half, the half every
one of #3229's seven ACs is about, and the half a repository can actually edit. The deny-list is the
binary's internal default.

**Why the modelling was removed rather than fixed once more.** It was built, and then four consecutive
review rounds each found a **false-PASS inside that subsystem alone**:

| round | the false-PASS | mechanism |
|---|---|---|
| jobs 30 | a phantom `/**` sibling on a pre-formatted pathspec constant | over-modelled the exclusion set, so a covered path was EXCUSED from `prompt-content:` |
| jobs 31 | an unbounded substring presence test | `**/Cargo.lock` matched inside `**/Cargo.lock.bak`, so an equal-length rename read `OK` |
| jobs 32 | a three-state `built-in-set:` tested as two | `UNAVAILABLE` took the permissive branch and the excusal happened on an unverified model |
| jobs 33-H1 | the excusal machinery itself | coverage excused on a model that could not be verified |

Four for four, in a subsystem no acceptance criterion reaches — while the *retained* oracle (census vs
`exclude_patterns`) is what every AC is written against. **And subtraction cannot introduce a false PASS**:
with no built-in in `_rx_patterns` this check only ever models FEWER exclusions than roborev applies, so it
can never tell `prompt-content:` to skip a path. Every failure direction the deletion opens is a FAIL.

**Job-33 H1 is therefore DELETED, not waived.** The excusal mechanism it was a finding about no longer
exists, so there is **no subject remaining** for the finding to apply to. That distinction matters because a
High finding that vanishes because its subject was removed reads *identically* to a waived one unless it is
written down: nothing was excused, judged acceptable, or deferred-with-the-defect-still-present — the code
the finding described is gone, and what it would have guarded is now recorded as a residual under #3278.

**THE RESIDUAL, STATED — AC4's SECOND BRANCH.** AC4 reads: *"Either the wrapper's extension-based
classification and roborev's path-glob exclusion agree on what 'code' means, **or the residual disagreement
is documented with the exact cases where it persists**."* This change takes the second branch. **This is not
a reduction in AC coverage** — AC4 is a disjunction, and the second branch is met by naming the exact case,
pinning it by test, and showing it fails closed.

**The exact cases where it persists.** A diff whose code-census paths include a path the compiled-in
deny-list excludes: `Cargo.lock`, `go.sum`, `pnpm-lock.yaml`, `Gemfile.lock`, `package-lock.json`, or
anything under `.cache/`, `.beads/`, `.gocache/`. Such a path is **silently dropped from the reviewer's
diff, and the guard does not model it**. Per key:

| key | what it reports | why |
|---|---|---|
| `census-exclusion:` | **PASS**, the path SURVIVING | no CONFIGURED pattern excludes it, and the built-in is not in the set |
| `prompt-content:` | **FAIL**, naming the path | it expects the path in the prompt and it is not there |

So the residual surfaces as a `prompt-content:` **FAIL whose stated cause names the symptom** ("the reviewer
never received their diffs" — which is TRUE) **rather than the mechanism**. It is a **diagnostic** gap, and
it **fails CLOSED**: never a vacuous green, never a merge on unreviewed code. The degenerate sub-case (a
dependency bump whose only code-census path is a lockfile) also FAILs, under `prompt-content:` instead of
pre-enqueue.

**Pinned, with a both-directions control** (`cx30` / `cx30b`), because a declared residual that is not
pinned is indistinguishable from an unnoticed one: `cx30` runs a #3096-shaped fixture
(`docs/reports/ws0-3096-artifacts/*.json` + `Cargo.lock` + `.rs`) against a prompt carrying only the `.rs`
file and asserts `census-exclusion: PASS (2/2 …)` + `prompt-content: FAIL (1/2 …)` + `RESULT: FAIL`;
`cx30b` changes ONE variable — the prompt carries the lockfile — and PASSes. Without `cx30b`, `cx30`'s FAIL
could come from the fixture, the config or the census rather than from the absence it names.

**Measured consequence for #3096.** #3096 is held `HOLD: merge after #3229` partly on **F1**, the built-in
mis-modelling behind its `Cargo.lock` residual, and its worker was told to expect a numeric
`prompt-content: PASS (n/n)`. F1 lived inside the deleted subsystem, so that expectation no longer holds:
measured on the hermetic #3096-shaped fixture, a #3096-shaped diff now yields **`prompt-content: FAIL`**
(option **(iii)**), not a numeric PASS and not a PASS-with-residual. Re-anchoring #3096 is the owner's call,
not this change's.

##### D2b-i — THE RULE, and why this key has no `NOTICE` value

> **FAIL where the author can act; NOTICE where only the information is actionable; never silence.**

On `census-exclusion:`'s subject the rule resolves to a **single** call: a CONFIGURED pattern (worktree,
root or global) swallowing census CODE ⇒ **FAIL**, pre-enqueue, because the remedy is a one-token edit to a
NAMED file and the author can act before paying for a review round.

**There is deliberately NO `NOTICE` value for this key.** A NOTICE is the right value for a swallow with
**no** remedy, and the only such swallow is by the deny-list this check does not model. With every pattern
in play CONFIGURED and therefore EDITABLE, every swallow the check can see is actionable — so the verdict is
`PASS` or `FAIL` and nothing between. A **TOTAL** swallow needs no branch of its own either: it is a
configured swallow, so it FAILs, and the value carries `<m>/<n>` so `m == n` shows on its face.

"Never silence" is satisfied one level up, by the documented residual: it is stated in `--help`, in the
oracles file's header, in the delta spec and here — not left to be rediscovered.

**And the affirmation backstop lost its one exemption, which makes it stricter.** `census-exclusion:` was
allowed a `NOTICE` in the wrapper's verdict-affirmation backstop — its single per-key escape hatch — because
a remedy-less built-in swallow was a measurement with a stated residual. With that subject deleted the key
cannot emit `NOTICE`, so the exemption is gone: all seven deterministic keys must now be affirmatively
`PASS`, with no exception. A structural assert reads the backstop's own `case` body and requires **exactly
one** exempting arm (the affirmative `PASS*)` one), so no per-key hatch can be reintroduced.

###### Round 10: it was never one bug — it is ONE SHAPE, and the third instance is why it is now a rule

Round 9 fixed `built-in-set: UNAVAILABLE` taking the permissive excusal path. Round 10's review found the
SAME defect one key over, and the round before had fixed a third instance in the test harness. Written out
together they are indistinguishable:

| # | signal | states | which were tested | what the UNMEASURED state did |
|---|---|---|---|---|
| 1 | `_rx_builtin_state` | OK / DIVERGED / UNAVAILABLE | `= DIVERGED`, `!= DIVERGED` | `UNAVAILABLE` took the permissive **excusal** path — coverage excused on an unverified model |
| 2 | `_rx_corroboration` | OK / DRIFT / NOTICE / **UNAVAILABLE (initial)** | `= DRIFT`, `= NOTICE` | `UNAVAILABLE` reached `PASS (no exclusion patterns configured; …)` and **enqueued** a review |
| 3 | `_census_end` (suite) | a line number / empty | `-` (a `${:-$start}` default) | a failed `awk` degraded to a **1-line scan**, in which the absence-assert reads `ok` |

**The shape: a multi-state signal where only the BAD states are tested, so every unknown or unmeasured
state inherits the PERMISSIVE branch.** Instance 2 is the sharpest, because the code's own comment states
the correct principle three lines above the defect — *"our parser recognised no key" is NOT "nothing is
configured", and here the binary is the only oracle that can tell them apart* — and then never requires that
oracle to have **answered**; `--help` and `roborev_toml_exclude_patterns` had *already documented* the
corroboration requirement, so the code was contradicting its own contract. That is #3229 reintroduced under
the key written to prevent it, for the second time.

A third point fix is the wrong response, so round 10 does four things instead:

1. **The rule, stated in the spec and in the wrapper's own doc block** (not only in a commit message):
   *a positive verdict requires an affirmative measurement*; an oracle that is the SOLE evidence for a claim
   and could not be consulted yields a NON-PASSING verdict whose text distinguishes "we could not check"
   from "nothing was wrong"; a permissive branch is keyed on the AFFIRMATIVE value, never on the absence of
   a bad one; and where a signal genuinely SHOULD be permissive, the reason is recorded IN CODE at the branch.
2. **The instance fixed** — `FAIL (exclusion set UNCORROBORATED: …)`, a third textually distinct form beside
   `unreadable` and `no exclusion patterns configured`, scoped to the EMPTY parse. With patterns parsed the
   silence stays permissive *on purpose*, because there the patterns themselves are the measurement (each
   resolved through the port and matched by git) and corroboration only cross-checks them; failing there
   would red every run on a box whose roborev lacks the subcommand. `cx1` now pins that permissive branch
   deliberately, so the scoping is asserted in both directions (an over-broad fix reds 153 asserts).
3. **The shape closed where it is structural** — the wrapper's final verdict scan WAS the same shape at its
   most consequential point: four failing prefixes tested, everything else falling through to
   `finish PASS 0`. The non-failing set is now an ALLOW-LIST with a failing fallback (`cx28`), and a PASS
   additionally requires every one of the seven verdict-carrying keys to be affirmatively `PASS` — closing
   the neighbouring case where a value is IN the grammar and non-failing but is not a MEASUREMENT, namely
   the initial `SKIP` of a check that never ran (`cx29`; un-backstopped, an early-returning
   `prompt-content:` PASSED the run with the strongest anti-vacuity key having measured nothing).
4. **Every state variable and `${VAR:-default}` in the three flow scripts audited**, and each
   deliberately-permissive one annotated with its reason: `vacuity-tier1: UNAVAILABLE` (no summary region =
   no claim to judge; a genuine not-applicable, and the deterministic keys cover the condition), the
   `${HOME:-}` global-config guard (roborev resolves it from HOME too, so the mechanism agrees), the
   announce-count default (diagnostic-only). Two more were verified to fall the STRICT way and now say so:
   `block_marker_count`'s `:-0` (0 markers ⇒ `findings: NONE` ⇒ tier 1 HARD FAILs, whereas `PRESENT` is the
   permissive value) and the built-in literal count's `|| n=0` (0 ⇒ `UNAVAILABLE`, blessing withheld).
   `_rx_found` — the parser's "did I see the key at all" flag — was MEASURED AND NEVER READ; it now feeds the
   UNCORROBORATED diagnostic, which is the only thing that can distinguish an absent key from an empty array
   for the operator, while deliberately NOT gating the verdict (an empty array in one file cannot rule out an
   unrecognised key spelling in another).

One latent instance was reported and deliberately not changed at the time, because it sat in the
built-in-set machinery whose fate was an open owner decision: `roborev_builtin_state_details` switched on
`_rx_builtin_state` with arms for `OK` and `UNAVAILABLE` and **no `*)`**. That decision has since been made
— the whole subsystem is deleted (D2b) — so the instance is **gone with its subject**, not carried forward.
It is recorded here only because it is the fourth sighting of the same shape, and the shape is the finding.

###### The symmetric-mirror lesson the deleted subsystem taught, kept because it generalises

Worth keeping even though the code is gone, because the failure mode is one of this repo's named blind
spots reproduced in shell, and it applies to ANY constant this suite mirrors.
`ROBOREV_BUILTIN_EXCLUDES` was first written as a space-separated STRING and iterated unquoted, so bash
**pathname-expanded** it: `**/package-lock.json` became the repo-relative `website/package-lock.json`
(without `globstar`, `**` behaves as `*`). The presence check then looked for
`:(exclude,glob)website/package-lock.json` in the binary, did not find it, and reported

```
census-exclusion: FAIL (roborev built-in exclude set DIVERGED from the pinned v0.61.2 set:
  pinned pattern(s) no longer present in the binary: website/package-lock.json)
```

— a false FAIL on every run. This is the same hazard the corroboration code already warns about for
`roborev config get` output ("NEVER an unquoted `for item in $out`, which would PATHNAME-EXPAND a pattern
like `*.md` against `$PWD`"); the fix is a bash **array**, which removes it structurally instead of relying
on remembering to quote.

**Why the hermetic suite did not catch it.** The regression suite mirrors the pinned set in its own
constant so it can plant literals into a stub binary — and that mirror was *also* a space-separated string
iterated unquoted. **Both sides made the identical mistake**, so the planted literals and the presence
check agreed with each other and `built-in-set: OK` passed. That is #3042's rule (a symmetric
producer/consumer test is invariant to a uniform error, and two defects that cancel are undetectable *by
construction*) reproduced in shell rather than in SSTable framing. What exposed it was the only asymmetric
oracle available: running the check against the **real roborev binary**, which carries the pattern
verbatim. **The generalisable rule, which survives the deletion:** a hermetic suite that MIRRORS a
production constant is a symmetric oracle, so it cannot catch an error both sides make — the mirror needs an
ASYMMETRIC check (here, the real binary) or a structural assert against the production file itself. `cx24`,
which asserts the census/config mirror **structurally against the committed `.roborev.toml`** rather than
against a second hand-maintained copy, is that rule applied to the mirror this change RETAINS.

##### D2b-iii — `prompt-content:` expects EVERY code census path, and subtracts nothing

There is **no subtraction and no excusal**. No key is licensed to tell `prompt-content:` which census code
paths to skip: a CONFIGURED swallow FAILs pre-enqueue and never reaches it, and roborev's compiled-in
deny-list is not modelled, so nothing computes a "known absence" set at all. A path the reviewer really did
not receive therefore FAILs — the fail-closed direction, and the residual documented in D2b is exactly what
lands here.

That is a deliberate reversal of the earlier design, which handed the built-in-excluded set down and had
`prompt-content:` report `(+<n> not expected: …)`. The excusal was the mechanism behind two of the four
false-PASSes in D2b's table (it excused coverage on a model that could not be verified), and it is the ONE
place in the wrapper where one key could weaken another. Removing it means the guard may now report a FAIL
whose stated cause is imprecise; it can no longer report a PASS that covered less than it claimed.

**And the floor stays: a `0/0` is never a pass.** With no subtraction, `census_total == 0` means the census
had no CODE path at all — which `code-free:` already FAILs pre-enqueue — so the branch is unreachable
through the normal ordering. It is kept anyway, as a **structural** backstop that does not depend on an
upstream check still being there, because `PASS (0/0 code census paths present)` is textually
indistinguishable from a genuine pass. Its regression case (`cx21`) therefore drives the function
**directly** — a test that could only be written against the current control flow would evaporate with the
next refactor.

##### D2b-iv — ONE canonical path-normalisation boundary (the fix for six blockers, not two)

**The pattern is the finding.** Rounds 2, 3 and 4 of review produced **six blockers and every one was a
path-normalisation defect** — in a different consumer each time: the oracle compared paths from the wrong
config source; a total exclusion swallow certified an empty prompt; `prompt-content:` could not parse
space-bearing or C-quoted headers; the **census classified a C-quoted path by its quoted spelling**; rename
and mixed-quoted headers were unreachable; a newline-delimited path set turned one path into two grep
alternatives. The root cause was structural: **normalisation was scattered.** `roborev_census` did not
normalise at all, `census-exclusion:` unquoted at one point, `prompt-content:` did something different again.
Patch the reported consumer and the next round finds the next consumer. So the design changes the shape of
the problem instead of the symptom.

**THE BOUNDARY.** Paths are normalised **once, at the census**, by asking git for them **NUL-delimited**:

| Source | Rendering of `docs/é.sh` | Rendering of `docs/a b.sh` |
|---|---|---|
| the census (`git diff --numstat -z`) | `docs/é.sh` (RAW) | `docs/a b.sh` (RAW) |
| the survivor set (`git diff --name-only -z`) | `docs/é.sh` (RAW) | `docs/a b.sh` (RAW) |
| the prompt's diff header (produced by roborev, not by us) | `diff --git "a/docs/\303\251.sh" "b/…"` | `diff --git a/docs/a b.sh b/docs/a b.sh` |

With `-z` there is **no quoted spelling to reconcile** on any git-sourced path: `census_paths` /
`census_code_paths` / the survivor map all hold the same RAW bytes, and RAW is
the single representation used for classification, comparison **and** display. `census-exclusion:` became a
direct byte comparison; the census's extension/prefix tests now see `md`, not `md"`. Records are read with
`read -r -d ''`, so a path containing a NEWLINE survives — something a line-oriented read cannot do at all.

The only text that still arrives quoted is text **we did not get from git plumbing**: the reviewer's prompt.
So `roborev_unquote_path` survives with exactly **one caller**, `roborev_diff_header_has_path`, and that
matcher is the **only** way any consumer may ask "is this census path in the prompt?".

**THE MATCHER, and why each shape is decidable.** A quoted side is unambiguous (a C-quoted body holds no
unescaped `"`, so the first unescaped one ends the token, spaces and all); an unquoted side holds no `"` at
all when git wrote it. That yields four shapes, three of them exactly parseable:

1. `diff --git "a/<q>" "b/<q>"` — both quoted: both sides decoded exactly.
2. `diff --git "a/<q>" b/<raw>` and `diff --git a/<raw> "b/<q>"` — **MIXED**, emitted when only one side
   needs quoting. This occurs **only on renames**, which is why a both-sides-quoted parse never reached it
   and both census sides were reported absent. (Confirmed: `--no-renames` is absent from the roborev
   binary's strings, so the reviewer's diff has rename detection ON while our census splits renames.)
3. `diff --git a/<raw> b/<raw>` — genuinely ambiguous when a name carries a space (`a/x y b/z w` has several
   readings), and **no reading of the header LINE can settle it**.

**AMBIGUITY IS RESOLVED FROM EVIDENCE, NOT FROM POSITION (round 5, blocker 1).** The first attempt at shape 3
tested each **position** the wanted path could occupy — `case $rest in "a/$want b/"*)` and friends. That is a
**PREFIX test**, and it produced a genuine **FALSE PASS in the merge gate**: with a tracked file named
`foo b/x`, the header `diff --git a/foo b/x b/foo b/x` has `a/foo b/` as a prefix, so the *unrelated* census
path `foo` read as PRESENT. Reproduced:
`roborev_diff_header_has_path 'diff --git a/foo b/x b/foo b/x' foo` → present. The code carried a comment
asserting this was impossible ("can only ever match a header that LITERALLY carries `a/<want> b/` … widens
nothing unsound"); the claim was false — `a/<want> b/` occurs as a **prefix of a different path's header** —
and the comment is corrected in place rather than deleted, because the next reader relies on it.

**Failing closed was NOT an option.** With renames ON the header ambiguity is irreducible
(`a/foo b/(bar b/foo b/bar)` is a legal reading), so refusing to decide would red **every space-bearing
header** and reintroduce the round-3/4 false FAILs pinned by `cx6c`, `cx6g`, `cx6h`. Instead: **git does not
leave renames ambiguous elsewhere in the diff.** For a rename or copy it always emits
`rename from <path>` / `rename to <path>` (with `similarity index`) immediately after the header — one path
per line, C-quoted when needed, so each is exactly decidable. And for a non-rename the two header paths are
**identical**, which disambiguates by itself. Hence the resolution order:

| # | evidence | ruling |
|---|----------|--------|
| 0 | the header's own `rename from`/`rename to` (or `copy from`/`copy to`) lines | AUTHORITATIVE — the header line is not consulted at all |
| 4a | otherwise, some valid `a/<A> b/<B>` split has **A == B** | the header is a NON-rename (git always writes rename lines for a rename), so ONLY the equal reading counts |
| 4b | otherwise (no equal split, no rename lines) | it can only be a rename whose rename lines did not reach us: any valid split counts — the **declared residual** |

Every candidate split is *enumerated*, and `$want` is **byte-compared**, never used as a pattern, so a path
containing `*`, `?` or `[` matches literally. On the reproduction, 4a fires (`foo b/x` == `foo b/x`) and `foo`
is correctly ABSENT; on `cx6g`'s space-bearing rename no split is equal, so 4b keeps it PRESENT.

**The residual is declared, not implied.** 4b is permissive: a `$want` that is one side of *some* valid split
reads PRESENT even if the producer meant another split. It is reachable only for a header that (i) carries a
space, (ii) names two DIFFERENT paths — i.e. is a rename/copy — and (iii) arrived **without** the rename lines
git always writes. For git's own output it is unreachable, and it is the price of not re-breaking `cx6g`/`cx6h`.

**Consequence for the boundary: the matcher's INPUT widened, the boundary did not move.** Resolving from the
rename lines means considering the lines *following* a header, so header collection moved into the oracles
file beside the matcher (`roborev_collect_prompt_headers`, awk-based so a multi-megabyte prompt is not read in
a bash loop, with the extended-header run BOUNDED so a `rename from` in the reviewer's prose or a diff body
line is never attributed to a header). The canonical-normalisation invariant is preserved exactly:
`roborev_unquote_path` still has one caller, and `roborev-review-checks.sh` still performs no unquoting and
holds no header-shape knowledge — it now carries three parallel arrays through instead of one.

Membership is decided **per header, in bash** — no regex, no path-set file, no `grep -Fxq` over
newline-delimited paths. That is what closes the newline false PASS: with census `{a, a<LF>b.rs}` and a
prompt naming only `a`, the old set-and-`grep` mechanism reported `PASS (2/2 present)` because a multi-line
pattern is a list of alternatives. The path is now either named by a header or reported ABSENT.

**The false-FAIL direction is the dangerous one here.** `prompt-content:` is the wrapper's strongest
deterministic anti-vacuity key; a key that reds on correct input is the key agents learn to waive, and a
waived `prompt-content:` defeats the entire purpose of this change. Symmetrically, `census-exclusion:`
false-FAILing **pre-enqueue** blocks a review outright: the quoted-prose misclassification made any PR
touching the tracked file `docs/research/CQLite Writes (M5) — Analysis & Recommended Paths.md` alongside code
FAIL with `census-exclusion: FAIL (1/2 code census paths excluded: … by '*.md' [repo-config])`. Reachability
is not theoretical: the repository tracks that file, **40 space-bearing paths under `docs/`** including the
directory `docs/storage engine/`, and this change *promotes* `docs/reports/*-artifacts/**` executables to
CODE census paths.

**THE INVARIANT IS PINNED STRUCTURALLY, because that is what stops round 5.** Behavioural cases can only
cover the shapes someone thought of — and each round proved someone had not. The guard suite therefore
asserts the boundary itself: every path-reading `git diff` carries `-z`; the census does not normalise inside
its classification loop and reads NUL-terminated records; the quoted-path decoder is defined once and called
**only** from inside `roborev_diff_header_has_path`; and the three retired mechanisms (a `[^ ]+` header
regex, the `.promptpaths` set file, `grep -Fxq` membership) are absent from the executable lines of the
consumer. Each assert was verified to FAIL under a deliberate mutation, so it is a live check rather than
decoration.

**Test-quality consequence.** The case that named this behaviour (`cx6`) asserted only `census-exclusion:`
and so reported two `ok`s while `prompt-content:` false-FAILed and the run terminated `RESULT: FAIL` — 565
asserts green over a broken key. *A case that passes while the behaviour it names is broken is worse than no
case, because it is read as coverage.* Every hostile-path case now asserts `RESULT:` **and**
`prompt-content:`, and the suite's stub JSON-escapes the prompt so a quote-bearing prompt cannot degrade the
job record and mask the comparison.

#### D2c — an EMPTY parse must be corroborated, not trusted (blocker, measured)

The first implementation returned `PASS (no exclusion patterns configured)` and `return 0` **before**
corroboration ever ran. So "our parser recognised no key" was aliased to "nothing is configured" — the
precise epistemic error the rest of the check refuses, and worse than the unreadable case, because it
reads green.

It is reachable in the real world. Measured against roborev v0.61.2: a config containing the **quoted key**
`"exclude_patterns" = ['docs/**', '*.md']` — valid TOML, the *same* key — is honoured (`roborev config get`
answers `docs/**,*.md`), while the bare-key pattern match skipped the line entirely. The guard then
reported "nothing configured" and enqueued a review from which every `docs/reports/*-artifacts/**`
executable was silently dropped: **#3229 reintroduced under the key whose whole job is preventing it.**

Two changes, and both are needed:

1. **Corroboration runs unconditionally, before every early return.** Parsed-nothing while the binary
   reports something is DRIFT → FAIL, with a detail naming the state for what it is. Where the parse is
   empty this cross-check is the *only* oracle, so skipping it there was skipping it exactly where it
   mattered. A binary that answers with an **empty list** is an ANSWER and corroborates; only a binary
   that answers nowhere is `UNAVAILABLE`.
2. **The quoted key spellings are accepted** (`"exclude_patterns"`, `'exclude_patterns'`). On its own this
   is insufficient — any other unenumerated-yet-honoured spelling would disable the guard again — which is
   why (1) is the load-bearing half and (2) merely moves the common case from the backstop to the primary
   path.

The regression suite's `cx5b`/`cx5c` previously **locked in** the un-corroborated PASS (both left the stub's
`config get` unsupported), so a green suite blessed a self-disabled guard; they now supply an
answering-but-empty binary and assert `corroboration: OK`. `cx5d` pins the drift direction directly.

**A test that blesses a vacuous verdict is WORSE than an unguarded path.** This is worth stating as a rule,
not just as a fix. An unguarded path is merely unprotected — everyone can see there is no check. A test
asserting `PASS (no exclusion patterns configured)` for the exact state a silently self-disabled guard
produces is actively harmful twice over: it **consumes the review budget** that would otherwise have looked
at that path, and it converts "nobody checked" into "we checked and it was fine" — which is the one
statement that stops anyone looking again. When adding a case whose expected value is a PASS, ask *what
state the system is in when that PASS is wrong*, and make the fixture distinguish the two.

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
  absent config file ⇒ `PASS (no exclusion patterns configured; …)` **once the binary has confirmed it**
  (D2c), because "no exclusions" cannot swallow anything but "we did not recognise the key" can hide
  everything. "We could not tell" is never aliased to "nothing is excluded". The same rule governs
  **escapes**: an unknown or untranslated backslash escape inside a basic string is REFUSED, not
  swallowed — `"a\tb"` is `a<TAB>b`, and quietly yielding `atb` would compare a pattern *different* from
  the one roborev applies, which is the exact failure mode the check exists to catch.
- **Path normalisation avoids command substitution.** `$(…)` strips trailing newlines, so a tracked path
  ending in a `\012` escape would come back a byte short — mis-comparing against the `-z` survivor set and
  able to COLLIDE with a genuinely shorter sibling. The un-quoting helper returns through a named global and
  expands octal escapes with `printf -v`.
- The **global/repo merge is a UNION**, per `config.ResolveExcludePatterns` / `loadRepoExcludePatterns`
  (today the global list is `[]`, so the repo list is the whole effective set). This is no longer a
  fail-closed guess; the check takes the union because that is what the tool does.
- When `roborev` *is* invocable, the parsed set is **corroborated** against
  `roborev config get exclude_patterns` (which prints the comma-joined repo value). A pattern the binary
  reports that our parse lacks is `FAIL (exclusion set drift: …)` — that direction could hide a swallow.
  The reverse is a NOTICE. When the binary is absent the corroboration reports `UNAVAILABLE` and the
  verdict stands on the file parse, so hermeticity is preserved.
- **Parsing that answer must not MUTATE the patterns it reports** (round 5, blocker 3 / #3260 item 1). The
  first version deleted every bracket in the string (`${out//[/}` + `${out//]/}`) to shed a bracketed list.
  That also destroys a glob **character class** inside a pattern: `src/[Tt]est.rs` came back `src/Ttest.rs`,
  matched nothing in the parsed set, and reported `corroboration: DRIFT` ⇒ a **pre-enqueue FAIL on a
  correct configuration**. Only a **verified outer container** is stripped now (leading `[` *with* trailing
  `]`, after trimming), and quoting only at each item's own edges. The direction was fail-closed, so it
  could never certify unreviewed code — but by this design's own rule, *a guard that reds a legitimate
  configuration is the guard that gets disabled*, which is how #3229 happened.

#### D2d — no path may reach a summary value un-neutralised (round 5, blocker 2)

The block is **line-oriented** and it is what every reader keeps: `flow-closer`, the flow-* skills and this
repo's own guard suite retain only the block and grep it by `^<key>: ` / `^RESULT: ` to decide whether a
merge proceeds. Diff-derived text reaches those values — `census-exclusion:` names each swallowed census
path, the NOTICE/FAIL details name paths and the pattern responsible — and **a census path is
attacker-controlled**: it is whatever a PR chose to track. A filename carrying a NEWLINE therefore let a
value **span lines** and introduce arbitrary `key:` lines, up to a forged `RESULT: PASS`, into the block
whose entire purpose is to be trusted. Measured on the mutant: **3 `RESULT:` lines, one of them
`RESULT: PASS`**, plus a forged `prompt-content: PASS`. Configured `exclude_patterns` are the same input
class (a TOML basic string `"a\nb"` decodes to a real newline, and a branch may carry its own
`.roborev.toml`).

**Fixed at the single emit boundary, not per interpolation site.** `emit_kv` (every block value) and
`finish` (every detail line) run their text through `roborev_safe_line`, which renders control characters as
visible escapes (`\n`, `\r`, `\t`, else `\ooo`). A per-site escape is a list to keep complete — the next
value to grow a path interpolation reopens the hole silently — so the property is **total** and holds for
keys that do not exist yet. Four structural asserts pin the boundary itself (every `emit_summary` value goes
through `emit_kv`; all 23 keys do; `emit_kv` neutralises; `finish` neutralises and no longer bulk-prints
`"${DETAILS[@]}"`), each verified to FAIL under mutation.

Two deliberate choices:

- **Quotes, backslashes and spaces are left intact.** The block names swallowed paths **by their real
  bytes** — `docs/…/odd "q" name.sh` must still read as itself, pinned by `cx6b` — and no non-control byte
  can begin a line or a `key:`. Escaping more would trade a real diagnostic for no extra safety.
- **The rendering is not reversible, and that residual is declared.** A path holding the two literal bytes
  `\` `n` renders the same as one holding a newline. The guarantee is exactly *no value spans a line and no
  `key:` can be introduced*; a caller wanting exact bytes reads them from git, not from a summary block.
  Neutralising also never costs the operator the diagnosis: the path is still **named**, on one line.

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

### D4 — the demonstration is a recorded POST-MERGE run, and the primary evidence is a real PR (AC2)

AC2 is satisfied by **running** the sanctioned wrapper, not by reasoning about it. But it cannot be run
against *this* change's own review, and the reason is the ordering property of D2a taken one step further.

#### Why it cannot be pre-merge — the deadlock

roborev resolves `exclude_patterns` from the **ROOT checkout** and **snapshots it at daemon start**. So
while this change is unmerged, the set applied to its own review is the root checkout's pre-change blanket
`['docs/**', '*.md']`. The original plan committed an **executable under root `docs/`**
(`probe-census-exclusion.sh`) precisely so the PR would be a #3222-shaped demonstration of its own fix.
Under the old set that executable is swallowed, so the new check FAILs — **correctly**:

```
census-exclusion: FAIL (1/7 code census paths excluded:
  docs/reports/3229-artifacts/probe-census-exclusion.sh by 'docs/**' [root-config])
```

That is not a bug to route around; it is the guard working. But it makes a pre-merge self-demonstration a
**deadlock, not a test**: *the specimen that proves the fix is the specimen the unfixed configuration eats.*
So the executable is removed from the branch and the procedure kept as committed prose
(`docs/reports/3229-artifacts/live-probe-procedure.md`). The requirement is **rescheduled, not dropped** —
and the reason is recorded, because a quietly weakened acceptance criterion is indistinguishable from one
that was never met.

`website/src/content/docs/reports/_3229-artifacts/_3229-root-anchoring-probe.json` **stays** on the branch:
a `.json` under a *nested* `docs/reports/*-artifacts/` directory is not swallowed by the configured set
(root anchoring), so it does not deadlock and is live evidence either way.

##### A probe is only evidence while it DISCRIMINATES (round 9)

The probe's evidence is its **survival**, and survival only means something if the two candidate readings
of a configured pattern **disagree** about the path. At its original location
(`website/src/content/docs/_3229-root-anchoring-probe.json`) the discriminating pattern was the
pre-round-6 `docs/**/*.json`: `**/docs/**/*.json` (the incorrect `**/`-prefixed reading) matched it,
`docs/**/*.json` (root-anchored) did not. **Round 6's ⑦a directory-scoping deleted that pattern**, and
measurement with the guard's own port then showed the old path SURVIVING under *both* readings — i.e. the
probe had become **vacuous evidence**: it could no longer fail, so its passing said nothing. Vacuous
evidence is worse than absent evidence, because it reads exactly like the real thing.

The probe was therefore relocated to a path a *currently configured* pattern discriminates:
`docs/reports/*-artifacts/**/*.json` matches
`website/src/content/docs/reports/_3229-artifacts/_3229-root-anchoring-probe.json` under the
`**/`-prefixed reading (`**/docs/reports/*-artifacts/**/*.json`) and NOT under the root-anchored one. Both
path segments that Astro would otherwise pick up are underscore-prefixed, and `_3229-artifacts` still
matches `*-artifacts`, so the file is inert to the website build AND discriminating.

**How that was established, and how it must be re-established.** With the guard's OWN port —
`roborev_format_exclude_args` over the real `.roborev.toml`, then the same
`git diff --name-only -z --no-renames <base>...HEAD -- <exclude pathspecs>` survivor query the check runs —
executed twice: once with the correct pathspecs and once with every configured pattern forced to the
`**/`-prefixed form. A **both-directions control** ran alongside: `docs/reports/3229-artifacts/*.json`
must be EXCLUDED (proving the harness can see an exclusion) and `scripts/flow/*.sh` must SURVIVE (proving
it can see a survival). Do **not** substitute hand-rolled `git ls-files` + `:(exclude,glob)` pathspecs:
measured on this issue, `git ls-files -- 'website/' ':(exclude,glob)*.md'` returned **0 of 95** files, and
an exclude pathspec combined with a *literal file* pathspec returns 0 unconditionally — either answer
would have manufactured a configuration defect that does not exist. **Any future change to
`exclude_patterns` must re-run this discrimination check**, because deleting a pattern can silently
retire the probe.

#### The primary evidence is a real PR, not the probe

A probe is written to pass. The first post-merge PR that *happens* to carry an executable under `docs/`
proves the fix on a diff **nobody shaped for it**, which is strictly better evidence and costs nothing
extra — #3234 ships harnesses now, #3096's successor will, #3249's artifacts may.

- **AC2's record** = that PR's `census:` + `census-exclusion:` + `prompt-content:` lines, posted to #3229.
- **The committed procedure** = the documented **fallback**, if no such PR arrives promptly or its evidence
  is ambiguous.

The PASS condition is `census-exclusion: PASS` **together with**
`prompt-content: PASS (<n>/<n> code census paths present)`: the first says the configuration would not
swallow the executables, the second says the reviewer actually received them. Neither alone suffices —
which is the same defence-in-depth point D2a-ter makes from the other side.

#### The named trigger — an unowned post-merge obligation is not an obligation

Post-merge intentions decay, and this project has the receipts: **#3232** existed only as prose in #3100's
close; **#3103** shipped while its producer stayed uncommitted, after which three separate issues rebuilt a
corpus. So the obligation carries mechanism:

1. On merge, **#3229 goes to `In Review`, NOT `Done`** — `Done` auto-closes the issue and the obligation
   would vanish with it.
2. The PR is finalized and delivery telemetry stamped as usual; neither waits on the demonstration.
3. #3229 flips to **`Done` only once the AC2 evidence is posted** on the issue.
4. If the demonstration has not happened **within a few days**, it is **filed as a tracked issue** — never
   left to live in a comment thread.

#### Reading the tokens: the mechanism's thresholds, not a memorised band

Earlier drafts of this design enshrined a **398k–649k input** "genuine-review band". That is wrong as a
threshold and has been corrected. Judge against the wrapper's own thresholds, which are what the verdict is
actually computed from:

- **`input` ≥ `ROBOREV_VACUITY_MIN_INPUT_TOKENS` (25,000)** — anchored on the *highest observed vacuous
  run* (18,801), with headroom. Below it, tier 2 FAILs.
- **`cached` > 0** — a vacuous run measured exactly 0 cached.
- **`output` is ADVISORY ONLY, never a failure condition** — and the reason is decisive: a genuine **clean**
  review emits roughly **20–60** output tokens, *indistinguishable* from the vacuous baseline's 53–56.
  Output therefore cannot be a realness test on its own, in either direction. Already documented at
  `scripts/flow/roborev-review-checks.sh:328`.

398k–649k is cited **only as observed on large diffs**. It is diff-size dependent, and an absolute floor
drawn from large-diff observations would **falsely flag legitimate small diffs**: a real, substantive round
measured during this change was `input=118514 cached=88320 output=5954` on a ~90k-character prompt, with two
substantive findings citing real code — unambiguously genuine, and far below that band.

The vacuous **signature** to recognise is a shape, not a magnitude: input below the 25k floor, `cached == 0`,
a few dozen output tokens in seconds. PR #3222 measured 15,443 in / 89 out beside
`prompt-content: FAIL (136/136 code census paths absent)`.

The demonstration needs the network and a live reviewer, so like the #2964 worktree probe it is
**documented and recorded, never gate-run**.

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
9. **the DECLARED RESIDUAL, with a both-directions control** (D2b) — `cx30`: a #3096-shaped census
   (`docs/reports/ws0-3096-artifacts/*.json` + `Cargo.lock` + `.rs`) against a prompt carrying only the
   `.rs` file ⇒ `census-exclusion: PASS (2/2 …)` (the lockfile SURVIVES, because the built-in is not
   modelled), `prompt-content: FAIL (1/2 …)` naming `Cargo.lock`, `RESULT: FAIL`, and no "not expected"
   clause anywhere; `cx30b`: the same census with the lockfile IN the prompt ⇒ `prompt-content: PASS (2/2)`,
   `RESULT: PASS`, review enqueued. `cx30b` is not optional: without it `cx30`'s FAIL could come from the
   fixture, the config or the census rather than from the absence it names, and a declared residual would be
   indistinguishable from an unnoticed one;
10. **the `0/0` floor**, driven **directly** against `roborev_check_prompt_content` in the real files (the
    state is unreachable through the wrapper because `code-free:` FAILs first) — asserts the refusal value
    AND the *absence* of any `PASS (0/0` form;
11. **every diff-header shape git emits** (D2b-iv) — a space-bearing directory (`docs/storage engine/`) and a
    non-ASCII, octal-escaped name (`é.sh`) each ⇒ `prompt-content: PASS` and `RESULT: PASS`. Plus the
    test-quality rule these cases exist to enforce: **a hostile-path case asserts the terminal `RESULT:` and
    `prompt-content:`, never one intermediate key alone**, and the stub emits VALID JSON for a quote-bearing
    prompt so the record cannot degrade and mask the comparison.
12. **the header-ambiguity resolution, both directions** (D2b-iv, round 5) — `cx6l`: a tracked `foo b/x`
    beside a `foo`, with a prompt naming only the former ⇒ `prompt-content: FAIL (1/2 … absent)`, so a
    PREFIX reading can never stand in for a delivery; `cx6m`: an ambiguous rename header
    (`a/p b/x b/p b/x`, whose *equal* split is not the true one) **plus** its `rename from`/`rename to`
    lines ⇒ `PASS (2/2 present)`; `cx6n`: **the same header with those lines removed** ⇒
    `FAIL (2/2 … absent)`, which is what proves `cx6m` is carried by the rename lines and not by the
    permissive positional fallback. Plus structural asserts that the retired prefix test is gone, that the
    from/to resolution is still there, and that the consumer does no `diff --git` scanning of its own.
13. **verdict forgery by filename** (D2d) — `cx6p`: a docs/ harness `.sh` whose NAME carries newlines plus
    `RESULT: PASS` and `prompt-content: PASS` lines, swallowed by `docs/**` so it lands in the
    `census-exclusion:` value *and* the detail lines ⇒ **exactly one** `RESULT:` line, no `RESULT: PASS`
    and no forged `prompt-content: PASS` anywhere, and the path still NAMED with its newlines escaped on
    one line. A dedicated `assert_one_result_line` helper is required: `assert_verdict` reads
    `^RESULT: ` | `tail -1` and is therefore BLIND to an injected verdict line above the real one — only a
    count can see it.
14. **a glob character class in a corroborated pattern** (D2c) — `cx7c`: the stub answers the BRACKETED
    `['*.md', 'src/[Tt]est.rs']` ⇒ `corroboration: OK`, no `FAIL (exclusion set drift`, so one case pins
    both halves (the container IS stripped, the class inside a pattern is NOT).

15. **the CLOSED verdict grammar and the affirmation backstop** (round 10, and the owner ruling) —
    `cx28`: a per-check key holding a value outside the documented grammar ⇒ FAIL naming itself, so an
    unplanned value cannot inherit the non-failing branch; `cx29`: a check that returns before assigning its
    key ⇒ FAIL, so an un-run check cannot ride to PASS on its initial `SKIP`. Both run patched COPIES of the
    three flow scripts, each with the UNPATCHED copy shown to PASS first — an assert that a copy FAILs is
    otherwise satisfied by a copy that failed because it was copied wrong, which is a probe failing in the
    direction that looks like success. Plus four **structural** asserts read from the scan and backstop
    STATEMENTS (never a file-wide grep, which the summary block's own emit line would satisfy): the
    failing-capable set is exactly `FAIL*|FINDINGS*|ERROR*|INCONSISTENT*`; the positive arm exists and its
    `*)` sets `failed=1`; all seven deterministic keys are named in the backstop; and the backstop carries
    **exactly one** exempting arm — the affirmative `PASS*)` one — so no per-key escape hatch can be
    reintroduced now that `census-exclusion:`'s `NOTICE` exemption is gone.

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
