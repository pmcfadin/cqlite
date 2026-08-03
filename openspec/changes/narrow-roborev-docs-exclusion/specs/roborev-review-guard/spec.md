# roborev-review-guard — delta for narrow-roborev-docs-exclusion (issue #3229)

**Architecture note (read this first).** #2964 established the guard as **DETERMINISTIC-PRIMARY**: the
checks that carry the verdict are judged against data the wrapper obtains ITSELF. This delta extends
that principle to the one input the wrapper previously took on FAITH — **roborev's own exclusion
configuration**. The wrapper asserted in a prose comment that roborev "excludes non-code paths"; the
configured set actually excluded `docs/**`, i.e. **code**, and on PR #3222 that discarded 33 executable
harness files (a 136-path code census reduced to an EMPTY prompt: `prompt-content: FAIL (136/136 code
census paths absent)`, 15,443 input / 89 output tokens against the vacuous baseline). So this delta
(a) narrows the configured exclusion set to prose and non-code artifacts, and (b) makes the wrapper
COMPUTE roborev's exclusion view with git and fail closed pre-enqueue when it would swallow census code,
instead of asserting it.

**Acceptance-criterion → requirement map** (issue #3229's numbered ACs):

| AC | Requirement(s) |
|----|----------------|
| 1 — the exclusion is narrowed so executable files under `docs/` are reviewed | ADDED *The review-diff exclusion set excludes prose and non-code artifacts, never executable code* |
| 2 — demonstrated on the real PR #3222 shape, recorded not asserted | ADDED *A recorded live probe demonstrates the narrowed exclusion on the real harness-PR shape* |
| 3 — the wrapper FAILs loudly, pre-enqueue, when the config would swallow the census | ADDED *The wrapper fails closed before enqueuing when the effective exclusion set would swallow census code*; MODIFIED *The wrapper emits a machine-greppable summary block with a terminal verdict* (the new key in the fixed order) |
| 4 — the two classifiers are reconciled, or the residual is made explicit | ADDED *The exclusion view is computed with git from the effective configuration, and the residual divergence is declared*; MODIFIED *A code-free census is a deterministic failure before any review is enqueued* |
| 5 — a hermetic regression test pins the behaviour | MODIFIED *A hermetic regression check pins every vacuity trigger and is wired into the agent gate* |
| 6 — doctrine updated in the same change, publication verified by served content | MODIFIED *Doctrine records the roborev rules, including the measured invocation matrix* |
| 7 — the backfill decision is recorded | ADDED *The backfill ruling for already-merged, never-reviewed harness code is recorded* |

**Mechanism note — how the exclusion semantics were established.** `roborev` is an external **stripped
Go binary** (`roborev v0.61.2`, `/usr/local/bin/roborev`) with no source available, so its behaviour is
stated here only where it was **measured**: `exclude_patterns` is implemented as git pathspec
(`:(exclude,glob)`, symbols `git.FormatExcludeArgs` / `config.ResolveExcludePatterns`), i.e. git
wildmatch with `WM_PATHNAME` — anchored at the repository root, `*` not crossing `/`. Replaying 21 real
reviews from `~/.roborev/reviews.db` against their recorded `git_ref` ranges, the ONLY paths ever dropped
from a prompt were 25 paths, EVERY ONE a `.md` — including `.claude/agents/*.md`, `openspec/**/*.md`,
`website/**/*.md` and top-level `CLAUDE.md` — which `docs/**` cannot explain, so a **slash-less pattern
is applied recursively** (normalised to `**/<pattern>`). Every non-`.md` path in that replay reached its
prompt. The pathspec CONSTRUCTION is no longer inferred at all: `git.FormatExcludeArgs` was recovered by
DISASSEMBLING the stripped binary (symbols via `.gopclntab`, text base `0x401000`) and is fully specified
in the requirement below — a pattern with an interior or leading `/` is ROOT-ANCHORED and passed verbatim,
a slash-less pattern is `**/`-prefixed (recursive), every pattern emits BOTH `<p>` and `<p>/**`, and a
TRAILING slash is trimmed before the anchoring test so `docs/` and `docs/**` behave OPPOSITELY. The
absence of any negation / re-include capability is likewise now a VERIFIED fact at the instruction level,
not an assumption. The construction is pinned to `roborev v0.61.2`.

## ADDED Requirements

### Requirement: The review-diff exclusion set excludes prose and non-code artifacts, never executable code
The repository's roborev exclusion configuration (`.roborev.toml`'s `exclude_patterns`) SHALL be a
**prose/artifact deny-list**, and SHALL NOT contain any pattern that excludes a path merely because of
the directory it lives in. Specifically it SHALL NOT contain `docs/**` or any equivalent blanket
directory glob, and it SHALL NOT exclude any of the executable/config-as-code extensions the repository
ships under `docs/` — at minimum `.py`, `.sh`, `.bt`, `.c`, `.rs`, `.toml`, `.cql`, `.yml`, `.yaml`.
Measurement harnesses committed under `docs/reports/*-artifacts/` are a repository CONVENTION, so this
is a standing property of the configuration, not a one-time edit.

Prose exclusion SHALL be retained: `*.md` stays, and it is SUFFICIENT for prose — because a slash-less
pattern is applied recursively, `*.md` alone already excludes every tracked `.md` file repo-wide
(measured: ~1404 files; `git ls-files -- ':(exclude,glob)*.md'` leaves 1393 while
`':(exclude,glob)**/*.md'` leaves 0, matching the observed drops). Non-code ARTIFACT exclusion SHALL be
expressed **docs-scoped** (`docs/**/*.<ext>`), covering at minimum the high-volume raw-output and
binary/image classes measured under `docs/`: `txt`, `json`, `jsonl`, `log`, `err`, `csv`, `png`, `svg`,
`gz`, `pdf`, `jfr`, `html`, `mmd`, `tex`, `diff`.

A deny-list SHALL be used because an allow-list is **NOT EXPRESSIBLE** — now a VERIFIED fact rather than a
working assumption: `git.FormatExcludeArgs`, read at the instruction level, performs only
TrimSpace/TrimRight/TrimLeft/`Index` and has no negation or re-include handling whatsoever (and git
pathspec supports none inside `:(exclude)`), so "review these extensions" cannot be written. The deny-list's known weakness SHALL be recorded rather than papered over — a NEW artifact
extension appearing under `docs/` is re-admitted to review prompts — and that weakness SHALL remain a
TOKEN-COST issue only, never a correctness one, which is what the pre-enqueue reconciliation check
guarantees. Globally-scoped (slash-less) exclusion of artifact extensions SHALL NOT be used, because it
would apply repo-wide and hide real configuration and data files outside `docs/` from review.

#### Scenario: An executable committed under a report's artifact directory is reviewed
- **GIVEN** the narrowed `exclude_patterns` and a diff containing `docs/reports/ws0-3217-artifacts/harness/run.sh`, `.../classify.py` and `.../offcpu.bt`
- **WHEN** roborev builds the review diff for that range
- **THEN** all three paths are present in the prompt actually sent, and no pattern in the effective set excludes them

#### Scenario: The configuration contains no blanket directory glob
- **WHEN** `.roborev.toml`'s `exclude_patterns` is inspected after this change
- **THEN** it contains neither `docs/**` nor any other pattern that excludes a path solely by its directory, every docs-scoped pattern names a specific non-code file extension, and `*.md` is still present

#### Scenario: Prose is still excluded, and repo-wide
- **GIVEN** a diff containing `docs/reports/ws0-3217-report.md`, `openspec/changes/x/proposal.md` and `CLAUDE.md`
- **WHEN** roborev builds the review diff
- **THEN** all three markdown paths are absent from the prompt, established by the single `*.md` pattern without any `docs/`-scoped markdown pattern being required

#### Scenario: Artifact extensions are excluded only under docs/, not repo-wide
- **GIVEN** a diff containing both `docs/reports/x-artifacts/partA-run/counters.json` and `test-data/cassandra-parity-manifest.json`-class configuration/data JSON outside `docs/`
- **WHEN** roborev builds the review diff
- **THEN** the artifact JSON under `docs/` is excluded while the non-`docs/` JSON is still delivered to the reviewer, so narrowing the exclusion did not create a new blind spot elsewhere in the tree

### Requirement: The wrapper fails closed before enqueuing when the effective exclusion set would swallow census code
The wrapper SHALL evaluate its own CODE census against the **effective** roborev exclusion set and SHALL
FAIL, **before any review is enqueued**, when any code census path would be excluded. The verdict SHALL
be published under its own distinct greppable key **`census-exclusion:`** — never as a generic
`prompt-content:` failure discovered after a review has been paid for. The check SHALL run after the
census and `code-free:` classification and before the enqueue, and it SHALL be registered in the
summary block's verdict scan so its FAIL actually fails the run (a key absent from the failing-capable
set would be decorative).

The value grammar SHALL be:

- `PASS (<k>/<n> code census paths survive the effective exclusion set; corroboration: <state>)`
- `PASS (no exclusion patterns configured)` — an absent config file or a genuinely empty pattern list
  cannot swallow anything
- `FAIL (<m>/<n> code census paths excluded: <path> by '<pattern>'[, …])` — naming the swallowed paths
  and the pattern that excluded each, capped at the first 10 with `(+<r> more)` so the block stays compact
- `FAIL (exclusion set unreadable: <cause>)`
- `FAIL (trailing-slash pattern '<p>/' resolves RECURSIVE (**/<p>), opposite to '<p>/**' — drop the
  trailing slash deliberately or write '<p>/**')`
- `FAIL (exclusion set drift: '<pattern>' reported by roborev config get is absent from the parsed set)`
- `SKIP (<cause>)` when the step was not reached

An UNREADABLE configuration SHALL fail closed and SHALL be DISTINGUISHABLE from an absent one: a key
present whose value is not a parseable pattern array is `FAIL (exclusion set unreadable: …)`, while an
absent key or absent config file is the `PASS (no exclusion patterns configured)` form. "We could not
tell" SHALL NEVER be aliased to "nothing is excluded".

The check SHALL be NUL-safe (`-z` / array handling, no word-splitting on filenames), because the
repository tracks a path under `docs/` containing spaces and a literal double quote, which
`git diff --name-only` without `-z` emits QUOTED and which would therefore silently never match its
census entry — a false PASS in exactly the direction this check exists to close.

#### Scenario: A restored blanket docs glob fails the round before any review is enqueued
- **GIVEN** a pushed branch whose census contains `docs/reports/x-artifacts/harness/run.sh` and `.../classify.py`, and a repository configuration whose `exclude_patterns` is `['docs/**', '*.md']`
- **WHEN** the wrapper runs
- **THEN** `census-exclusion:` reads `FAIL (2/2 code census paths excluded: docs/reports/x-artifacts/harness/run.sh by 'docs/**', docs/reports/x-artifacts/harness/classify.py by 'docs/**')`, NO review is enqueued, the terminal `RESULT:` is `FAIL`, and the exit code is non-zero

#### Scenario: The narrowed configuration passes and the review proceeds
- **GIVEN** the same census under the narrowed `exclude_patterns`
- **WHEN** the wrapper runs
- **THEN** `census-exclusion:` reads `PASS (2/2 code census paths survive the effective exclusion set; corroboration: …)` and the review IS enqueued

#### Scenario: An unparseable exclusion set fails closed and is distinct from an absent one
- **GIVEN** a repository configuration whose `exclude_patterns` key is present but whose value is not a parseable pattern array, and separately a repository with no `exclude_patterns` key at all
- **WHEN** the wrapper evaluates each
- **THEN** the first reads `FAIL (exclusion set unreadable: <cause>)` with no review enqueued, and the second reads `PASS (no exclusion patterns configured)` and proceeds — so a parse failure can never present as "nothing is excluded"

#### Scenario: A path containing spaces and a quote is compared correctly
- **GIVEN** a census whose code paths include a filename containing spaces and a literal double quote
- **WHEN** the wrapper compares the census against the survivors of the exclusion pathspecs
- **THEN** the comparison is performed over NUL-delimited paths so the path matches its census entry exactly, and the verdict reflects the real exclusion state rather than a quoting artefact

#### Scenario: The failure names its own cause, not the reviewer's
- **GIVEN** a configuration that would swallow census code
- **WHEN** the summary block is read by an agent deciding what to fix
- **THEN** the failing key is `census-exclusion:` and its message names the configuration pattern responsible, so the reader is not sent to investigate `prompt-content:` or the reviewer's behaviour for a defect that is entirely in configuration

### Requirement: The exclusion view is computed with git from the effective configuration, and the residual divergence is declared
The wrapper SHALL NOT re-implement glob matching and SHALL NOT trust reviewer narration to learn what was
excluded. It SHALL REPRODUCE roborev's mechanism with **git itself**: read the effective
`exclude_patterns`, construct the git pathspecs roborev constructs, and obtain the surviving paths from
`git diff --name-only -z --no-renames <base>...HEAD -- <pathspecs>`; the swallowed set is the census's
CODE paths MINUS those survivors. `--no-renames` SHALL be passed because the census is computed with
`--no-renames`, so without it the two path sets would not be comparable.

**Pathspec construction SHALL be an EXACT PORT of roborev's `git.FormatExcludeArgs`**, which is fully
specified — recovered by disassembling the stripped binary (symbols via `.gopclntab`, text base
`0x401000`) and confirmed to be on the real diff path from its callers (`git.GetDiffCtx`,
`GetDiffLimitedCtx`, `GetRangeDiffCtx`, `GetRangeDiffLimitedCtx`, `GetDirtyDiff`, and
`prompt.(*Builder).buildSinglePrompt` / `buildRangePrompt` / `resolveExcludes`):

```
p  = TrimSpace(pattern); p = TrimRight(p, "/"); if p == "" -> skip
b0 = p[0]                       # read BEFORE TrimLeft
p  = TrimLeft(p, "/");          if p == "" -> skip
prefix = (b0 == '/' || p contains "/") ? ":(exclude,glob)" : ":(exclude,glob)**/"
emit   prefix+p   AND   prefix+p+"/**"
```

Four consequences SHALL be replicated, not approximated:

1. **A pattern containing an interior `/` is passed VERBATIM and is ROOT-ANCHORED.** `docs/**/*.json`
   therefore does NOT match `website/src/content/docs/c.json`. The check SHALL report such a nested path
   as SURVIVING. Evaluating both a verbatim and a `**/`-prefixed reading and failing on either is
   FORBIDDEN: it is not conservative but WRONG, and would emit false `census-exclusion: FAIL`s on
   legitimate report PRs.
2. **Every pattern emits TWO pathspecs**, `<p>` and `<p>/**`, which is how a bare directory name excludes
   recursively.
3. **A TRAILING SLASH INVERTS the anchoring.** `TrimRight(p, "/")` runs BEFORE the contains-`/` test, so
   `docs/` becomes `docs`, is treated as slash-less, and resolves RECURSIVE (`**/docs` + `**/docs/**`) —
   the OPPOSITE of `docs/**`, which stays root-anchored. Because that is a SILENT WIDENING of unbounded
   depth that a future tidy-up would reintroduce, a trailing-slash pattern in the effective set SHALL be
   a loud **FAIL** naming the inversion, independent of whether it currently swallows a census path.
4. **A LEADING `/` root-anchors an otherwise-recursive slash-less name**: `/README.md` resolves to
   `README.md` (root only) while `README.md` resolves to `**/README.md`. This is the ONLY way to
   root-anchor a slash-less name and SHALL be replicated.

Empty-after-trim patterns SHALL be skipped silently, as the algorithm does. The algorithm SHALL be
recorded against the PINNED version it was derived from — **`roborev v0.61.2`** — and re-verification on
any roborev upgrade SHALL be a stated maintenance obligation, since an upstream change to
`FormatExcludeArgs` would silently invalidate the port. `git.EnsureLocalExcludePattern` /
`.git/info/exclude` is a DISTINCT mechanism and SHALL NOT be conflated with `exclude_patterns`.

The check SHALL REPLICATE the algorithm rather than QUERY the binary, because **no roborev flag prints
the resolved pathspecs** — `review` has no `--dry-run` and `-v` is a global-only flag — so the resolved
set is not obtainable from the tool at all.

The effective set SHALL be read from the configuration FILES, not from the `roborev` binary, so the check
stays hermetic and stub-testable and so no reordering of the wrapper's existing `command -v roborev`
validation is required. The parse SHALL respect TOML table scoping (a same-named key inside a `[table]`
is NOT the top-level key) and SHALL fail closed rather than guess.

**THREE config sources SHALL be read, and a swallow in ANY of them SHALL FAIL.** roborev's daemon binds a
repository by its `repos.root_path` — the **ROOT checkout** — and reads THAT checkout's `.roborev.toml`.
Under 1:1:1:1 the wrapper's `$REPO` is a LINKED WORKTREE, so reading only `$REPO/.roborev.toml` certifies
a file roborev may never consult: on this change's own branch that produced
`census-exclusion: PASS (7/7 survive)` from the worktree's narrowed set while the real review applied the
root checkout's blanket `['docs/**','*.md']` and returned
`prompt-content: FAIL (1/7 code census paths absent)`. The sources SHALL therefore be
(a) `$REPO/.roborev.toml`, (b) the ROOT checkout's `.roborev.toml` when `$REPO` is a linked worktree, and
(c) the global `~/.roborev/config.toml`, combined as a UNION — which is also what
`config.ResolveExcludePatterns` / `loadRepoExcludePatterns` do. Which of (a) and (b) a given roborev build
PREFERS is an internal detail the check SHALL NOT bet on; the union is the only reading that cannot
produce a false PASS in either direction. The root checkout SHALL be resolved from git
(`rev-parse --path-format=absolute --git-common-dir`, with a relative-path fallback for git older than
2.31 and `git worktree list --porcelain` as a last resort) and SHALL FAIL CLOSED when none of those
answer — reading one file and reporting a PASS about it is the defect, not the remedy. When `$REPO` IS
the root checkout there is only ONE repository file and it SHALL NOT be double-reported.

**Every FAIL and PASS value SHALL name WHICH source each pattern came from.** With more than one config
file in play, "excluded by `docs/**`" is not an actionable instruction; the swallowed-path list, the
trailing-slash FAIL and the resolved-pathspec listing SHALL each carry a source tag, and a FAIL SHALL
additionally enumerate every source path it read.

**roborev's own BUILT-IN excludes SHALL be modelled too.** `exclude_patterns` is not the whole exclusion
set: the binary ALWAYS appends a hard-coded lockfile/cache deny-list (extracted from the pinned v0.61.2
executable as literal `:(exclude,glob)**/…` pathspecs — the `Cargo.lock`/`go.sum`/`package-lock.json`/…
family plus `**/.beads/**`, `**/.cache/**`, `**/.gocache/**`, `**/.kata.local.toml`), with no
configuration switch. A census path one of those eats is exactly as invisible to the reviewer as a
configured swallow, so it SHALL be evaluated in the same reconciliation, and it SHALL be messaged
DISTINCTLY from a configured pattern ("excluded by a roborev built-in" ≠ "excluded by your config"). This
list SHALL carry the same re-verify-on-upgrade obligation as the ported algorithm.

**THE VERDICT SHALL FOLLOW ONE RULE, STATED IN DOCTRINE VERBATIM:**

> **FAIL where the author can act; NOTICE where only the information is actionable; never silence.**

This is deliberately ONE rule rather than three ad-hoc calls, and doctrine SHALL present it as such
(CLAUDE.md, `roborev-findings.md`, `design.md`), so a future call of this shape is decided by the rule
instead of re-litigated. It resolves the three cases:

1. A **CONFIGURED** pattern (worktree, root or global) swallowing census CODE ⇒ **FAIL**. The remedy is a
   one-token edit to a NAMED file, available before any review round is paid for.
2. A **PINNED BUILT-IN** swallowing census CODE ⇒ **NOTICE**, non-failing. There is NO remedy: the
   deny-list is compiled into the binary with no opt-out and no negation form. FAILing would permanently
   red a ROUTINE, legitimate change class (a `Cargo.lock` touch) against a check its author cannot
   possibly satisfy — and **a guard that fires on correct input with no available fix is the guard that
   gets disabled**, which is how #3229 happened. The NOTICE SHALL still name the paths and the responsible
   built-in IN THE VALUE LINE (not merely in a detail), SHALL state that a clean verdict does not cover
   them, and the run SHALL proceed.
3. The **LIVE built-in set DIVERGING from the pinned set** ⇒ **FAIL**. This case DOES have a remedy —
   re-extract, update the pin, and judge the new built-in — and it is a MECHANISM change, which the
   v0.61.2 pin already obliges the project to catch on upgrade rather than assume away. A NOTICE here
   would silently absorb an upgrade that began excluding `*.rs` or `scripts/**`, with the failure looking
   like normal operation: precisely the blindness this change exists to close. This FAIL SHALL be
   DIFF-INDEPENDENT, like the trailing-slash FAIL.

**The pinned pattern list SHALL be held in a form that cannot be PATHNAME-EXPANDED** (a bash array, not a
space-separated string iterated unquoted). This is not style: `**/package-lock.json` expands to the
repo-relative `website/package-lock.json`, which then reads as a pinned pattern having vanished from the
binary and FAILs every run. The regression suite's mirrored copy SHALL be held the same way — when both
sides were strings they made the IDENTICAL mistake, agreed with each other, and a green suite blessed a
check that FAILed against the real binary, which is the symmetric-oracle blindness of #3042 in shell.

**Divergence SHALL be OBSERVED, not assumed**, by reading the roborev executable: each pinned pattern
looked for as a FIXED string `:(exclude,glob)<pattern>` (which names removals exactly), plus a PINNED
COUNT of `:(exclude,glob)` literals (which detects additions numerically). A blind full-set
re-extraction SHALL NOT be used as the basis of a FAIL: Go string literals are concatenated without
terminators, and a naive scan of this very binary yields truncations, junk-suffixed hits and a phantom
pattern that is really a bare prefix constant — a FAIL built on that would red every run. The residual
(a new pattern having a pinned one as a prefix) SHALL be declared.

**"Never silence" SHALL be mechanized.** Every `census-exclusion:` value SHALL end with
`built-in-set: OK|DIVERGED|UNAVAILABLE`. When the set cannot be observed at all — `roborev` absent from
PATH, an unreadable target, or a target carrying zero `:(exclude,glob)` literals (which is the hermetic
suite's own state) — the value SHALL read `UNAVAILABLE` and SHALL be NEITHER a failure NOR a blessing.
An unobservable set SHALL NEVER be reported as, or silently treated as, agreement.

**PRECEDENCE.** Both FAIL causes outrank the NOTICE, and EVERY cause present SHALL be named in the value
line — the actionable half must never be hidden behind the unactionable one, in either direction.

**`NOTICE*` SHALL NOT be failing-capable** and both FAIL forms SHALL be. The wrapper's single verdict scan
fails a run whose value begins `FAIL`, `FINDINGS`, `ERROR` or `INCONSISTENT`; that correspondence SHALL be
asserted STRUCTURALLY against the scan itself, because a value reading NOTICE while `RESULT:` goes FAIL
(or a FAIL that does not red the run) is the decorative-key defect mirrored.

**A TOTAL built-in swallow SHALL FAIL; only a PARTIAL one is a NOTICE.** When EVERY code census path is
dropped by a pinned built-in — so the reviewer would receive an EMPTY prompt — `census-exclusion:` SHALL
FAIL **before the enqueue**, with the value naming the empty diff and the detail carrying the same remedy
`code-free:` prescribes (this diff cannot be roborev-certified at all; verify the paths another way and
record primary-source verification in the pull request). This SHALL NOT be read as an exception to the
pinned-built-in NOTICE ruling above: it is the SAME rule — *FAIL where the author can act; NOTICE where
only the information is actionable; **never silence*** — reaching a case that ruling does not cover. A
partial swallow leaves a diff the reviewer can genuinely review and an unactionable remainder, so it stays
information; a total swallow leaves NOTHING, and a verdict on an empty prompt certifies nothing — the very
condition `code-free:` already FAILs pre-enqueue for a prose-only census, arrived at through the exclusion
set instead of through classification. The boundary is TOTAL vs PARTIAL and nothing else, and BOTH sides
of it SHALL be pinned by the regression suite so neither can drift into the other.

The consequence of NOT drawing this boundary is MEASURED, not hypothetical (hermetic fixture: a
`Cargo.lock` bump beside a `README.md` edit): `census-exclusion: NOTICE (0/1 … survive)` let
`prompt-content:` report `PASS (0/0 code census paths present)` and the block terminate `RESULT: PASS`
with exit 0 — a VACUOUS pass TEXTUALLY IDENTICAL to a genuine one, on which `flow-closer` would arm
`--auto` for an unreviewed diff. Any dependency-bump branch whose only non-prose file is a lockfile
(`Cargo.lock`, `go.sum`, `pnpm-lock.yaml`) reaches it, and `code-free:` does NOT catch it because a
`.lock` extension classifies as CODE. Neither vacuity tier catches it either: tier 1 greps a literal
phrase the reviewer need not emit, and tier 2 is `UNAVAILABLE` with no token payload.

**A KNOWN built-in absence SHALL NOT be re-reported by `prompt-content:` as a discovery.** The set of
census code paths a pinned built-in drops SHALL be handed to `prompt-content:`, which SHALL subtract them
and SHALL say so in its value. Their absence from the prompt is a deterministic property of roborev's
compiled-in mechanism, already reported non-fatally under `census-exclusion:`; asserting their presence
would move the same unfixable red one key down. The subtraction SHALL be scoped to BUILT-IN swallows
only — a configured swallow FAILs pre-enqueue and can therefore never be masked by it.

When `roborev` IS invocable the parsed set SHALL be CORROBORATED against
`roborev config get exclude_patterns`, run from **every** checkout whose config was read (that command
resolves the repo config relative to its CWD, so asking only from `$REPO` reproduces the same blind spot
inside the corroboration). A pattern the binary reports that the parse LACKS SHALL be
`FAIL (exclusion set drift: …)` because that direction can hide a swallow, the reverse direction SHALL be
a non-failing NOTICE, and a binary that answers NOWHERE SHALL report the corroboration as `UNAVAILABLE`
without failing. A binary that answers with an EMPTY list is an ANSWER, not an absence, and SHALL
corroborate rather than degrade to `UNAVAILABLE`.

**Corroboration SHALL run on EVERY path, including when the parse found NO configured pattern.** An empty
parse SHALL NOT be reported as "no exclusion patterns configured" until the binary has confirmed it:
"our parser recognised no key" is not "nothing is configured", and where the parse is empty this
cross-check is the ONLY oracle available. The parse SHALL additionally accept the QUOTED TOML key
spellings `"exclude_patterns"` and `'exclude_patterns'`, which are the same key and ARE honoured by
roborev (measured on v0.61.2) — but accepting them is NOT sufficient on its own, because any other
unenumerated-yet-honoured spelling would silently disable the guard; the corroboration is what covers
that residual. A parse that found nothing while the binary reports at least one pattern SHALL be DRIFT →
FAIL, and the failure text SHALL say that this state is issue #3229 reintroduced under the key meant to
prevent it.

An UNKNOWN or UNTRANSLATED backslash escape inside a TOML basic-string pattern SHALL be REFUSED fail-closed
rather than have its backslash swallowed: `"a\tb"` is `a<TAB>b`, and silently yielding `atb` would compare a
pattern DIFFERENT from the one roborev applies — the very failure mode this check exists to detect.

Census paths SHALL be normalised for comparison WITHOUT command substitution, which strips trailing
newlines: a tracked path ending in a `\012` escape would otherwise lose a byte and both mis-compare
against the `-z` survivor set and risk COLLIDING with a shorter sibling path.

The two classifications SHALL remain INDEPENDENT — the census's extension-based classification and the
configured pathspec set — because deriving either from the other would make the comparison vacuous and
unable to detect a configuration regression. They SHALL be kept in AGREEMENT on the docs-scoped artifact
extensions by declaring that extension set ONCE in the wrapper's oracles file, used to classify census
paths under the declared prose directories as non-code and mirrored by the configuration's docs-scoped
patterns. The residual divergence SHALL be DECLARED in both directions:

1. configuration excludes a path the census calls CODE ⇒ `census-exclusion: FAIL`, pre-enqueue — the
   defect class this requirement exists to prevent;
2. the census calls a path non-code that the configuration does NOT exclude ⇒ not a failure; the file is
   delivered to the reviewer as bounded NOISE.

#### Scenario: The exclusion view is obtained from git, not from a re-implemented matcher
- **WHEN** the reconciliation check is inspected
- **THEN** it determines survivors by invoking git with `:(exclude,glob)` pathspecs derived from the configured patterns, and contains no independent wildmatch/glob implementation whose semantics could drift from git's

#### Scenario: A slash-containing pattern is root-anchored, so a nested docs path survives
- **GIVEN** the configured pattern `docs/**/*.json` and a census code path `website/src/content/docs/c.json`
- **WHEN** the check constructs the pathspecs and evaluates the census
- **THEN** the pattern is emitted VERBATIM as `:(exclude,glob)docs/**/*.json` (plus its `/**` sibling), the nested path is reported as SURVIVING rather than swallowed, and `census-exclusion:` does NOT FAIL — a both-interpretations reading that failed here would be a false FAIL on a legitimate report PR

#### Scenario: Each pattern contributes both its own pathspec and its `/**` sibling
- **GIVEN** a configured pattern naming a bare directory, for example `build`
- **WHEN** the check constructs the pathspecs
- **THEN** it emits BOTH `:(exclude,glob)**/build` and `:(exclude,glob)**/build/**`, so a directory-name pattern is recognised as excluding the directory's whole subtree and the check's survivor set matches roborev's

#### Scenario: A trailing-slash pattern is recognised as recursive and FAILs loudly
- **GIVEN** an effective exclusion set containing `docs/` (a trailing slash) rather than `docs/**`
- **WHEN** the check evaluates the set
- **THEN** it recognises that the trailing slash is trimmed BEFORE the contains-`/` test so the pattern resolves RECURSIVE (`**/docs` + `**/docs/**`) — the OPPOSITE of `docs/**` — and it emits a FAIL naming the inversion and the remedy, independently of whether that pattern currently swallows any census path

#### Scenario: A leading-slash pattern is root-anchored despite having no interior slash
- **GIVEN** the configured patterns `/README.md` and `README.md`
- **WHEN** the check constructs the pathspecs for each
- **THEN** the first yields `:(exclude,glob)README.md` (root only) and the second yields `:(exclude,glob)**/README.md` (any depth), so a census path `docs/dev/README.md` is swallowed only under the second

#### Scenario: The replication reproduces the observed behaviour of the pre-change configuration
- **GIVEN** the pre-change effective set `['docs/**', '*.md']`
- **WHEN** the check constructs its pathspecs and they are applied to the repository
- **THEN** `docs/**` is root-anchored while `*.md` is recursive, which reproduces the measured behaviour of the 21 replayed reviews — every dropped path was a `.md` at arbitrary depth repo-wide, and no non-`.md` path was ever dropped — so the port is demonstrably faithful rather than merely plausible

#### Scenario: The construction is pinned to a roborev version and re-verified on upgrade
- **WHEN** the reconciliation check and this change's design record are inspected
- **THEN** both name `roborev v0.61.2` as the version whose `git.FormatExcludeArgs` the construction ports, and state that a roborev upgrade requires re-verifying the algorithm before the check can be trusted — because an upstream change to it would silently invalidate the port while every summary block still read `PASS`

#### Scenario: The check runs without the roborev binary and reports the corroboration state
- **GIVEN** an environment in which `roborev` is not invocable
- **WHEN** the wrapper runs the reconciliation check
- **THEN** the verdict is computed from the configuration files alone and the value records the corroboration as `UNAVAILABLE`, so the check is fully exercisable in the hermetic regression suite

#### Scenario: A pattern the binary reports but the parse missed is drift, and fails
- **GIVEN** an invocable `roborev` whose `config get exclude_patterns` reports a pattern absent from the wrapper's parsed set
- **WHEN** the check corroborates
- **THEN** it reads `FAIL (exclusion set drift: '<pattern>' reported by roborev config get is absent from the parsed set)` and no review is enqueued, because an unparsed pattern could be excluding census code invisibly

#### Scenario: From a linked worktree, the ROOT checkout's configuration is evaluated
- **GIVEN** `$REPO` is a linked worktree whose `.roborev.toml` carries the narrowed set, while the ROOT checkout backing it still carries `['docs/**', '*.md']`, and the census contains executables under `docs/`
- **WHEN** the wrapper runs the reconciliation check
- **THEN** `census-exclusion:` FAILs naming those executables, attributes each to `docs/**` with a `root-config` source tag, enumerates both repository config paths it read, and states that roborev binds the repository by its `repos.root_path` so a narrowed worktree config does NOT override the root one — and no review is enqueued

#### Scenario: The same worktree layout passes once both configurations are narrowed
- **GIVEN** the same linked-worktree layout with the narrowed set in BOTH the worktree and the root checkout
- **WHEN** the wrapper runs
- **THEN** every `docs/` executable is reported SURVIVING, the review IS enqueued, and no pattern is double-reported — so the two-source read is a correctness fix, not a blanket refusal to review from a worktree

#### Scenario: The root checkout cannot be resolved, so the check fails closed
- **GIVEN** an environment in which neither `git rev-parse --git-common-dir` nor `git worktree list --porcelain` names a usable root checkout for `$REPO`
- **WHEN** the wrapper runs the reconciliation check
- **THEN** it FAILs closed saying the exclusion set roborev will apply is UNKNOWN, rather than reading `$REPO/.roborev.toml` alone and reporting a PASS about a file roborev may never consult

#### Scenario: A pinned built-in exclude is a non-failing NOTICE that still names the path
- **GIVEN** a census containing `Cargo.lock` (which the census classifies as CODE) beside a `.rs` file, under a configuration that excludes neither
- **WHEN** the wrapper runs the reconciliation check
- **THEN** `census-exclusion:` reads `NOTICE`, names `Cargo.lock` as excluded by `**/Cargo.lock` with a `roborev-builtin` source tag IN THE VALUE LINE, states that there is NOTHING TO FIX in any config file, states that a clean verdict does not cover that path, does NOT blame the operator's configuration, reports the `.rs` file as surviving — and the review IS enqueued with `RESULT:` not FAIL on that account

#### Scenario: A TOTAL built-in swallow FAILs pre-enqueue, because nothing would reach the reviewer
- **GIVEN** a census whose only non-prose file is a lockfile (a `Cargo.lock` bump beside a `README.md` edit), so `code-free:` PASSes because a `.lock` extension classifies as CODE, and whose single CODE path is dropped by the built-in `**/Cargo.lock`
- **WHEN** the wrapper runs the reconciliation check
- **THEN** `census-exclusion:` reads `FAIL (0/1 code census paths survive …; ALL 1 code census path(s) excluded by a roborev built-in, so the reviewer would receive an EMPTY diff: …)`, NOT a NOTICE; the detail states the diff cannot be roborev-certified at all and prescribes primary-source verification recorded in the pull request; no review is enqueued; `prompt-content:` reads `SKIP`; and the terminal `RESULT:` is `FAIL`

#### Scenario: The total-swallow FAIL is the NOTICE rule applied consistently, not an exception to it
- **WHEN** the total-swallow failure detail is read
- **THEN** it states that a PARTIAL built-in swallow stays a NOTICE and that this FAIL is the rule "FAIL where the author can act; NOTICE where only the information is actionable; never silence" applied consistently rather than an exception to it — so a reader cannot conclude the pinned-built-in NOTICE ruling was reversed

#### Scenario: The same lockfile beside surviving code is still only a NOTICE
- **GIVEN** the same built-in-excluded `Cargo.lock` in a census that ALSO carries a surviving `.rs` file
- **WHEN** the wrapper runs the reconciliation check
- **THEN** `census-exclusion:` reads `NOTICE (1/2 code census paths survive …)`, the review IS enqueued, the terminal `RESULT:` is not FAIL on that account, and the total-swallow wording (`EMPTY diff`) is absent — so the boundary is TOTAL vs PARTIAL and nothing else

#### Scenario: A known built-in absence is not re-reported by prompt-content
- **GIVEN** the same run, whose prompt therefore carries the `.rs` file but not `Cargo.lock`
- **WHEN** `prompt-content:` evaluates
- **THEN** it PASSes over the reduced set and records the subtraction explicitly (`+<n> not expected: excluded by a roborev built-in`), rather than FAILing on an absence that `census-exclusion:` already reported and that has no remedy under either key

#### Scenario: A live built-in set matching the pin is reported OK and corroborated
- **GIVEN** a roborev executable whose built-in deny-list matches the pinned set exactly
- **WHEN** the wrapper runs the reconciliation check
- **THEN** the value ends `built-in-set: OK`, the detail states the pin is corroborated rather than assumed, and a pinned-built-in swallow in the same run is still only a NOTICE

#### Scenario: An ADDED built-in fails, because that divergence has a remedy
- **GIVEN** a roborev executable carrying one MORE `:(exclude,glob)` literal than the pinned count — the shape of an upgrade that began excluding source — and a census that touches no lockfile at all
- **WHEN** the wrapper runs the reconciliation check
- **THEN** `census-exclusion:` FAILs with `roborev built-in exclude set DIVERGED from the pinned v0.61.2 set`, quantifies the delta against the pinned literal count, explains that this direction HAS a remedy and is therefore a FAIL rather than a NOTICE, names the concrete silent-absorption risk (`*.rs` / `scripts/**`), states that the FAIL is about the mechanism rather than this diff, and enqueues no review

#### Scenario: A REMOVED pinned built-in fails, naming the missing pattern
- **GIVEN** a roborev executable from which one pinned built-in pattern is absent
- **WHEN** the wrapper runs the reconciliation check
- **THEN** it FAILs naming that pattern as no longer present in the binary, reports the count delta, and points at the re-extract-and-re-pin remedy — because a vanished pattern makes the model OVER-exclude, producing a false FAIL, which is the direction that gets a guard bypassed

#### Scenario: An unobservable built-in set is UNAVAILABLE, neither failing nor blessing
- **GIVEN** an environment where the roborev target carries no `:(exclude,glob)` literals (a wrapper, a shim, or the regression suite's stub)
- **WHEN** the wrapper runs the reconciliation check
- **THEN** the value ends `built-in-set: UNAVAILABLE`, a detail states that this is deliberately NEITHER a failure NOR a blessing, and the run's verdict is unaffected by it — so the hermetic suite stays fully exercisable and an unobserved set is never silently read as agreement

#### Scenario: A configured swallow and a built-in divergence both name their cause
- **GIVEN** a run in which a configured pattern swallows census code AND the live built-in set has diverged from the pin
- **WHEN** the wrapper runs the reconciliation check
- **THEN** the single value line names the configured swallow AND the divergence, the configured cause keeps its own remedy detail, and no review is enqueued — the actionable half is never hidden behind the other

#### Scenario: The pinned pattern list cannot be pathname-expanded
- **WHEN** the oracles file is inspected
- **THEN** `ROBOREV_BUILTIN_EXCLUDES` is declared as a bash array, is never iterated unquoted, and the pinned `:(exclude,glob)` literal count is present — so a pattern like `**/package-lock.json` can never glob into a repo-relative path and be reported as a vanished built-in

#### Scenario: NOTICE is not failing-capable and both FAIL forms are
- **WHEN** the wrapper's verdict scan is inspected directly, rather than inferred from a case's exit code
- **THEN** its failing-capable set is exactly `FAIL*|FINDINGS*|ERROR*|INCONSISTENT*`, `NOTICE*` is absent from it, and `census-exclusion:` still participates in the scan — so a NOTICE cannot red the run while a configured swallow still does

#### Scenario: An empty parse is corroborated by the binary, never assumed
- **GIVEN** a configuration file the parser recognises no `exclude_patterns` key in, and an invocable `roborev` whose `config get exclude_patterns` reports `docs/**`
- **WHEN** the wrapper runs the reconciliation check
- **THEN** it FAILs as drift, says explicitly that the parse found no configured pattern while the binary reports at least one, names that state as issue #3229 reintroduced under the key meant to prevent it, never emits a `census-exclusion: PASS`, and enqueues no review

#### Scenario: A genuinely absent key passes only with the binary's confirmation
- **GIVEN** a configuration file with no `exclude_patterns` key and an invocable `roborev` that ANSWERS with an EMPTY list
- **WHEN** the wrapper runs the reconciliation check
- **THEN** the value reads `PASS (no exclusion patterns configured; …)` with the corroboration recorded as `OK` and the roborev built-in excludes still named, counted and reconciled — so "nothing is configured" can never be read as "nothing is excluded"

#### Scenario: A quoted TOML key spelling is parsed, not skipped
- **GIVEN** a configuration whose key is written `"exclude_patterns"` (or `'exclude_patterns'`) with a value that would swallow census code
- **WHEN** the wrapper runs the reconciliation check
- **THEN** the swallow is named directly by the primary reconciliation — not merely caught by the drift backstop — and the value is never `PASS (no exclusion patterns configured…)`

#### Scenario: An unknown TOML escape is refused rather than swallowed
- **GIVEN** a configured pattern written as a TOML basic string containing an escape TOML does not define
- **WHEN** the check parses it
- **THEN** it FAILs closed as an unreadable exclusion set naming the escape, explains that dropping the backslash would compare a different pattern than roborev applies, and enqueues no review

#### Scenario: The declared residual direction is noise, never a swallow
- **GIVEN** a census path the wrapper classifies as a non-code artifact which the configuration does NOT exclude
- **WHEN** the wrapper runs
- **THEN** no key fails on account of it, the path is simply delivered to the reviewer, and the documented residual states that this direction can only add review noise while the opposite direction is always a pre-enqueue FAIL

### Requirement: A recorded live probe demonstrates the narrowed exclusion, POST-MERGE, on a real harness PR
The change SHALL be demonstrated by a **recorded live run** — run, not asserted — of the sanctioned wrapper
against a diff of the shape that failed: executable harness files under `docs/reports/*-artifacts/`.

**THE DEMONSTRATION IS NECESSARILY POST-MERGE, AND THE REQUIREMENT SHALL SAY WHY.** roborev's daemon binds
a repository by its `repos.root_path` and resolves `exclude_patterns` from the **ROOT checkout**, and it
**snapshots that config at daemon start**. Therefore the narrowed set CANNOT apply to this change's own
review: while the change is unmerged the root checkout still carries the blanket `['docs/**', '*.md']`. A
committed **executable under root `docs/`** — the original self-demonstrating specimen — is consequently
swallowed, making `census-exclusion:` FAIL **correctly** and permanently until merge. A pre-merge
self-demonstration is therefore a **deadlock, not a test**: the specimen that proves the fix is the
specimen the unfixed configuration eats. The executable SHALL NOT be committed under root `docs/`; the
requirement is **rescheduled, not dropped**, and the reason SHALL be recorded rather than the requirement
quietly weakened.

**THE PRIMARY EVIDENCE SHALL BE A REAL PR, NOT A SYNTHETIC PROBE.** The first post-merge pull request that
happens to carry an executable under `docs/` demonstrates this end to end at no extra cost, and is
**strictly better** evidence than a probe written to pass, because it proves the fix on a diff **nobody
shaped for it**. AC2's record SHALL therefore be that PR's `census:` + `census-exclusion:` +
`prompt-content:` evidence posted to the issue; the committed probe **procedure** is the documented
**FALLBACK**, for when no such PR arrives promptly or its evidence is ambiguous.

**THE OBLIGATION SHALL CARRY A NAMED TRIGGER**, because an unowned post-merge obligation is not an
obligation: (a) on merge the issue SHALL move to **`In Review`, NOT `Done`** — `Done` auto-closes it and
the obligation would vanish with it; (b) the PR SHALL be finalized and delivery telemetry stamped
regardless, neither waiting on the demonstration; (c) the issue SHALL flip to `Done` ONLY once the AC2
evidence is posted; (d) if the demonstration has not happened within a few days it SHALL be **filed as a
tracked issue**, never left to live in a comment thread.

The recorded evidence SHALL carry: the `census:` counts, the `code-free:` and `census-exclusion:` lines,
the `prompt-content:` line, and the input / cached / output token counts from the job record. Its PASS
condition SHALL be `census-exclusion: PASS` TOGETHER WITH
`prompt-content: PASS (<n>/<n> code census paths present)` — the first says the configuration would not
swallow the executables, the second says the reviewer actually received them, and neither alone suffices.

**TOKEN COUNTS SHALL BE JUDGED AGAINST THE MECHANISM'S THRESHOLDS, NOT A MEMORISED BAND.** The thresholds
are the wrapper's own: `input` at or above `ROBOREV_VACUITY_MIN_INPUT_TOKENS` (**25,000**, anchored on the
HIGHEST observed vacuous run, 18,801), `cached` greater than zero, and **`output` ADVISORY ONLY, never a
failure condition**. The reason output can never be a realness test on its own SHALL be stated: a genuine
**clean** review emits roughly **20–60** output tokens, which is INDISTINGUISHABLE from the vacuous
baseline's 53–56 — already documented at `scripts/flow/roborev-review-checks.sh:328`. The figures
398k–649k input / 314k–554k cached / 5.0k–6.3k output SHALL be cited ONLY as **observed on large diffs**
and SHALL NOT be enshrined as a threshold: they are diff-size dependent, and a real substantive round
measured during this change was `input=118514 cached=88320 output=5954` on a ~90k-character prompt with
two substantive findings citing real code — unambiguously genuine and far below that band, so an absolute
floor set from large-diff observations would falsely flag legitimate small diffs. The vacuous SIGNATURE to
recognise is the SHAPE: input below the 25k floor, `cached == 0`, a few dozen output tokens in seconds
(PR #3222 measured 15,443 in / 89 out beside `prompt-content: FAIL (136/136 code census paths absent)`).

The demonstration diff SHALL additionally include a file under a NESTED `docs` directory (for example
under `website/src/content/docs/`) carrying one of the deny-listed artifact extensions, as an END-TO-END
CONFIRMATION of the disassembly-derived prediction: because a pattern with an interior `/` is
root-anchored, that nested path SHALL still be DELIVERED to the reviewer. Its absence from the prompt
would falsify the recovered algorithm and SHALL be treated as a blocking finding. That file SHALL be
committed on this branch, because — unlike an executable under root `docs/` — it survives under BOTH the
old and the new configuration and therefore does not deadlock.

Because the demonstration needs the network and a live reviewer, it SHALL be documented and recorded
rather than executed by the agent gate.

#### Scenario: The recorded evidence shows the code census present and a genuine token signature
- **GIVEN** the narrowed exclusion configuration in effect on the ROOT checkout, and a branch whose diff carries executables under `docs/reports/*-artifacts/`
- **WHEN** the sanctioned wrapper is run against it and the result recorded on the issue
- **THEN** the record shows `census-exclusion: PASS`, `prompt-content: PASS (<n>/<n> code census paths present)`, and a token triple judged against the wrapper's own floors (input at or above 25,000, cached greater than zero, output advisory) rather than against a memorised large-diff band

#### Scenario: The reason the demonstration cannot be pre-merge is recorded, not the requirement weakened
- **WHEN** the change is inspected for AC2
- **THEN** it records that roborev reads `exclude_patterns` from the repo root path and snapshots it at daemon start, that a committed executable under root `docs/` therefore makes `census-exclusion:` FAIL correctly until merge, and that the demonstration is consequently rescheduled to post-merge — and it carries no executable under `docs/reports/3229-artifacts/`

#### Scenario: A real post-merge PR is the primary evidence and the probe is the fallback
- **WHEN** the AC2 record is inspected
- **THEN** it names the first post-merge PR carrying an executable under `docs/` as the primary evidence — better than a probe written to pass, because the diff was not shaped for the test — and positions the committed procedure as the documented fallback

#### Scenario: The post-merge obligation has a named trigger rather than a comment thread
- **WHEN** the change's tasks and delta spec are inspected
- **THEN** they state that the issue moves to `In Review` and not `Done` on merge, that the PR finalizes and telemetry stamps regardless, that `Done` waits on the posted AC2 evidence, and that an undelivered demonstration is filed as a tracked issue within a few days

#### Scenario: Output tokens are never a realness test on their own
- **WHEN** the token guidance is inspected
- **THEN** it states that a genuine clean review's output count (roughly 20–60) is indistinguishable from the vacuous baseline's 53–56, that output is therefore advisory only, and it cites 398k–649k input solely as observed on large diffs rather than as a threshold

#### Scenario: The demonstration confirms the disassembly-derived root anchoring end to end
- **GIVEN** a diff that includes a deny-listed artifact extension under a nested `docs` directory such as `website/src/content/docs/`
- **WHEN** the prompt actually sent is inspected
- **THEN** that nested path IS present in the prompt — confirming live that a pattern containing an interior `/` is root-anchored as the recovered `git.FormatExcludeArgs` specifies — and its absence would instead falsify the port and block the change rather than being recorded as an acceptable outcome

#### Scenario: The demonstration is recorded evidence, not an assertion
- **WHEN** the pull request and issue are reviewed for AC2
- **THEN** they carry the actual summary-block lines and token counts from a real run, and a statement that the narrowed configuration "should" work is NOT accepted in their place

#### Scenario: The live probe is not a gate component
- **WHEN** the agent gate's component set is inspected
- **THEN** the live probe is not among its components, and its procedure plus expected summary-block values are documented instead

### Requirement: The backfill ruling for already-merged, never-reviewed harness code is recorded
The change SHALL RECORD the owner's ruling on the already-merged, never-reviewed harness code shipped
under `docs/reports/*-artifacts/` by #3026, #3100 and #3217, **together with its reason**. Either ruling
is acceptable — a retroactive review pass now that those paths are reviewable, or explicit
acceptance-as-is — and leaving the question unaddressed SHALL be the only failing outcome. The DECISION
is the owner's and SHALL NOT be made by the implementer; this requirement governs only that the decision
and its reason are recorded in a durable place (the change's artifacts and the pull request), so a later
reader can tell that the exposure was considered rather than missed.

Where the ruling is a retroactive review, the record SHALL name the mechanism used (the sanctioned
wrapper over a range or reconstructed branch containing those paths) and its outcome. Where the ruling is
acceptance-as-is, the record SHALL name the reason — for example that #3222's harness already received a
full adversarial hand review recorded in its pull request, which found no blockers.

#### Scenario: A retroactive review ruling is recorded with its mechanism and outcome
- **GIVEN** the owner rules that the already-merged harness code gets a retroactive review pass
- **WHEN** the change is finalised
- **THEN** the record names the sanctioned-wrapper invocation used, the paths covered, and the outcome, so the ruling is auditable rather than a claim

#### Scenario: An acceptance-as-is ruling is recorded with its reason
- **GIVEN** the owner rules that the already-merged harness code is accepted as-is
- **WHEN** the change is finalised
- **THEN** the record states that ruling and the reason for it, and does not leave the reader to infer that the exposure was simply forgotten

#### Scenario: Silence on the backfill question fails the change
- **WHEN** the change's artifacts and pull request are inspected for the backfill question
- **THEN** the absence of any recorded ruling is a failure of this requirement, independently of whether the configuration and wrapper changes are complete

## MODIFIED Requirements

### Requirement: The reviewer must demonstrably have received the census's own code files
The wrapper SHALL assert, under its own greppable key `prompt-content:`, that the **CODE subset** of the
census's changed file paths appears in the prompt ACTUALLY SENT to the reviewer, retrieved from the job
record (the structured `prompt` field, else the reviewer's own prompt-retrieval command). This check
SHALL be DETERMINISTIC and THRESHOLD-FREE: it catches "the reviewer never received the diff", the half of
the defect space that a verdict-text comparison cannot see.

**The code subset — not every census path — is what SHALL be required present**, because **roborev drops
exactly what its configured `exclude_patterns` pathspecs match — it makes NO code/non-code judgement**
(measured: on a census of 22 markdown + 5 code files the prompt carried `diff --git` headers for exactly
the 5 code files, because `*.md` is CONFIGURED). Requiring all 27 would false-FAIL
every branch that touches documentation, which is most of them. The code subset is the right subset only
while the configured set is a prose/artifact deny-list MIRRORING the census classification, and that
correspondence SHALL NOT be assumed — `census-exclusion:` computes it with git pre-enqueue (#3229).

**EVERY code path SHALL be checked** — there SHALL be NO sampling cap. A sampled subset was a hole: a
partial prompt naming just the sampled files passed. Matching SHALL be against the prompt's actual
`diff --git` HEADER paths, never a bare substring (a substring is satisfied by any incidental mention,
including this wrapper quoting a path in its own comments), and the header path set SHALL be collected
from **BOTH sides** of each header and compared WHOLE-LINE: the census runs `--no-renames` (a rename is
two paths) while the reviewer's diff may have rename detection ON (one `a/old b/new` header), so
same-path-only matching FALSELY REJECTED every review containing a detected rename. Collecting both sides
reconciles the two rename behaviours WITHOUT weakening exact-header strictness to a substring test.

**PATHS SHALL BE COMPARED NORMALISED, AND EVERY HEADER SHAPE GIT EMITS SHALL BE RECOGNISED (#3229).** The
census is built from `git diff --numstat`, which C-QUOTES a path containing a double quote, a backslash or
a non-ASCII byte; the prompt's headers may carry the raw spelling, the C-quoted spelling
(`diff --git "a/\303\251.txt" "b/\303\251.txt"`), or an unquoted spelling containing SPACES
(`diff --git a/a b.txt b/a b.txt`). BOTH sides SHALL therefore be normalised through the same
quoted-path decoder `census-exclusion:` already uses, and a space-bearing path — which the
`a/<x> b/<y>` header form cannot be split on unambiguously — SHALL be matched by probing the LITERAL
header line the census path would produce. Accepting only `^diff --git a/[^ ]+ b/[^ ]+$`, and comparing a
C-quoted census path against unquoted captures, FALSE-FAILED both shapes (MEASURED: a census whose two
code paths both survived the exclusion set reported `census-exclusion: PASS (2/2 survive)` beside
`prompt-content: FAIL (1/2 absent)`, `RESULT: FAIL`). That direction is the DANGEROUS one for this key
specifically: it is the wrapper's strongest deterministic anti-vacuity signal, so a key that reds on
correct input is the key agents learn to waive. Reachability is not theoretical — the repository already
tracks 40 space-bearing paths under `docs/`, including the directory `docs/storage engine/`, and this
change promotes `docs/reports/*-artifacts/**` executables to CODE census paths.

**A `0/0` SHALL NEVER BE A PASS.** When no code census path is left to look for — every one of them
dropped from the diff roborev builds — this key has no subject and SHALL NOT report PASS; it SHALL FAIL,
naming the reason. `PASS (0/0 code census paths present)` is textually indistinguishable from a genuine
pass while the reviewer received an EMPTY prompt, which is precisely the vacuity this capability exists to
prevent. This is belt-and-braces behind the pre-enqueue total-swallow FAIL in `census-exclusion:`: the
condition is unreachable through the normal flow, and SHALL remain refused here anyway so that removing
the upstream FAIL cannot silently restore a vacuous PASS.

The value set SHALL be exactly:

- `PASS (<n>/<n> code census paths present)` — every code path found, optionally suffixed
  `(+<b> not expected: excluded by a roborev built-in — see census-exclusion:)`;
- `FAIL (<k>/<n> code census paths absent from the prompt)` — `<k>` MISSING of `<n>` checked, naming the
  missing paths (first ten). Note the two values carry the SAME denominator `<n>` but OPPOSITE numerator
  senses (present on PASS, absent on FAIL), so a grep-based reader SHALL read the value word, never the
  ratio alone;
- `FAIL (no code census path was checkable — a 0/0 is never a pass)`;
- `FAIL (prompt unretrievable — no evidence any diff was delivered)`;
- `SKIP` — the step was never reached.

**An unretrievable (empty or whitespace-only) prompt SHALL FAIL.** There SHALL be no non-failing
`UNAVAILABLE` value for this key: with a NON-EMPTY code census an unretrievable prompt means there is NO
authoritative evidence the reviewer received any diff, and a PASS resting on that contradicts this
capability's entire purpose. It is also not an always-red risk — the prompt is measurably retrievable
from the job record's `prompt` field AND from the reviewer's `show <job> --prompt` command, so an empty
one is a real anomaly.

#### Scenario: A prompt that does not mention the census's code files is a hard failure
- **GIVEN** a pushed branch with a non-empty code census whose review returns a clean verdict with healthy token accounting
- **WHEN** the prompt actually sent to the reviewer mentions none of the census's code file paths
- **THEN** `prompt-content:` reads `FAIL (<k>/<n> code census paths absent from the prompt)`, the message names the missing paths and states that a prompt that does not mention the census's files cannot have reviewed them, and the terminal `RESULT:` is `FAIL`

#### Scenario: An unretrievable prompt FAILS rather than passing on no evidence
- **GIVEN** a job for which the prompt cannot be retrieved from either the job record's `prompt` field or the reviewer's prompt-retrieval command, while the code census is non-empty
- **WHEN** the wrapper evaluates prompt content
- **THEN** `prompt-content:` reads `FAIL (prompt unretrievable — no evidence any diff was delivered)`, the message names both retrieval attempts and the number of code files that went unverified, and the terminal `RESULT:` is `FAIL`

#### Scenario: A prompt carrying the census's code files passes and reports its coverage
- **WHEN** every code census path appears on either side of a `diff --git` header in the prompt
- **THEN** `prompt-content:` reads `PASS (<n>/<n> code census paths present)`, so a reader can see the coverage rather than trusting a bare PASS

#### Scenario: A detected rename in the reviewer's diff is not a false rejection
- **GIVEN** a census computed with `--no-renames` that lists a rename as two paths (`main.rs` deleted, `renamed.rs` added), and a prompt whose diff has rename detection ON and carries the single header `diff --git a/main.rs b/renamed.rs`
- **WHEN** the wrapper evaluates prompt content
- **THEN** both census paths count as covered, `prompt-content:` reads `PASS (2/2 code census paths present)`, and the exact-header match is NOT weakened to a substring test to achieve it

#### Scenario: Every code path is checked, with no sampling cap
- **GIVEN** a census with many code paths
- **WHEN** the wrapper evaluates prompt content
- **THEN** it requires EVERY code census path to be present, so a prompt naming only a sampled subset cannot pass

#### Scenario: A census path carrying spaces and a literal quote is not a false failure
- **GIVEN** a census whose code paths include a filename with spaces and a literal double quote, which `git diff --numstat` therefore C-QUOTES, and a prompt carrying that path in its raw spelling
- **WHEN** the wrapper evaluates prompt content
- **THEN** both sides are normalised through the quoted-path decoder, `prompt-content:` reads `PASS (2/2 code census paths present)`, and the terminal `RESULT:` is `PASS` — the verdict itself is asserted, not just the `census-exclusion:` key

#### Scenario: A space-bearing directory in a code path is matched by its literal header line
- **GIVEN** a code census path under a directory containing a space (the repository tracks `docs/storage engine/`), whose diff header is therefore `diff --git a/docs/storage engine/probe.sh b/docs/storage engine/probe.sh`
- **WHEN** the wrapper evaluates prompt content
- **THEN** the path counts as present, `prompt-content:` reads `PASS`, and the ambiguity is resolved by probing the literal header line rather than by relaxing the match to a substring

#### Scenario: A non-ASCII code path is matched through the C-quoted header shape
- **GIVEN** a code census path with a non-ASCII name, which git renders as `"docs/reports/x-artifacts/\303\251.sh"` in the census and as `diff --git "a/docs/reports/x-artifacts/\303\251.sh" "b/…"` in the prompt
- **WHEN** the wrapper evaluates prompt content
- **THEN** both spellings decode to the same raw bytes and compare equal, `prompt-content:` reads `PASS`, and no octal-escaped path is reported absent

#### Scenario: A zero-subject prompt-content refuses to report a pass
- **GIVEN** a state in which every code census path has been dropped from the diff roborev builds, so no path remains to be checked
- **WHEN** the check evaluates
- **THEN** it reads `FAIL (no code census path was checkable — a 0/0 is never a pass)` with a detail explaining that a `0/0` PASS would be indistinguishable from a genuine one, and it NEVER emits `PASS (0/0 code census paths present)`

### Requirement: A code-free census is a deterministic failure before any review is enqueued
Because roborev structurally discards a code-free diff, a census consisting ENTIRELY of
documentation/specification prose SHALL be a DETERMINISTIC FAIL under its own greppable key
`code-free:`, evaluated from the wrapper's OWN census classification **before any review is enqueued**,
with no reviewer prose involved. No docs-only change SHALL record "roborev clean", and the sanctioned
substitute SHALL be verification against primary sources recorded in the pull request.

The MECHANISM is measured, not inferred, and it SHALL be stated CORRECTLY: **roborev drops from the diff
it constructs exactly the paths matched by its CONFIGURED `exclude_patterns`, applied as git pathspec
exclusions** — it makes no code/non-code judgement of its own. On a 27-file census (22 markdown + 5 code)
the prompt carried headers for exactly the 5 code files because the configured set excluded `*.md`, not
because the reviewer recognised prose. The earlier wording of this requirement — "roborev EXCLUDES
non-code paths" — is FALSIFIED and SHALL NOT be restored: under the configured `docs/**` the same
mechanism discarded 33 executable harness files on PR #3222 (`prompt-content: FAIL (136/136 code census
paths absent)`), i.e. it excluded CODE. So for a diff every path of which the configured set excludes, the
constructed diff is genuinely EMPTY and the reviewer's "contains no code changes to review" is a TRUTHFUL
report of an empty input rather than a reviewer malfunction. That is precisely why the correct response is
a DETERMINISTIC pre-enqueue FAIL computed from our own census — the reviewer is not misbehaving and no
amount of re-running or re-prompting will change the outcome — and why the census is ALSO reconciled
against the effective exclusion set under `census-exclusion:`, so a configured pattern that would swallow
CODE fails before the enqueue instead of masquerading as a code-free diff.

Classification SHALL be by file EXTENSION against a declared prose-extension set, plus a declared
docs-scoped ARTIFACT-extension set mirroring the configuration's docs-scoped exclusions (raw run output
and binary/image blobs under the declared prose directories), with a path assist limited to EXTENSIONLESS
files under those directories. A file with an executable/config-as-code extension anywhere in the tree —
including `docs/foo.py`, `docs/reports/*-artifacts/**/*.sh`, `*.bt` and `.github/workflows/*.yml` — SHALL
count as CODE, so neither the check nor the configuration may treat a program as documentation merely
because it lives under `docs/`. `code-free:` SHALL NEVER be satisfied by the presence of a directory
prefix alone.

This requirement is deliberately STRONGER than a prose-matched detection: an earlier revision computed
the same classification and used it only for attribution wording, which let a docs-only diff reach
`RESULT: PASS` whenever the reviewer's verdict happened not to carry the vacuity phrase.

#### Scenario: A markdown-only census fails deterministically before a review is enqueued
- **GIVEN** a pushed branch whose census against the base is entirely markdown
- **WHEN** the wrapper runs
- **THEN** `code-free:` reads `FAIL (code-free census: <n>/<n> files are documentation/specification text)`, NO review is enqueued, the terminal `RESULT:` is `FAIL`, and the message directs the author to primary-source verification in the PR instead of "roborev clean"

#### Scenario: A code-free census fails even when the review returns clean with healthy accounting
- **GIVEN** a docs-only census and a reviewer that would return "No issues found" with genuine-looking token accounting
- **WHEN** the wrapper runs
- **THEN** the outcome is still `RESULT: FAIL` attributed to `code-free:`, because the failure is a property of the census the wrapper measured and never a bet on the reviewer admitting it

#### Scenario: A workflow YAML or a script under a prose directory is CODE, not documentation
- **GIVEN** a census consisting only of `.github/workflows/ci.yml`, and separately a census mixing one markdown file with one `.rs` file
- **WHEN** the wrapper classifies each census
- **THEN** neither is classified code-free, `code-free:` reads `PASS` for both, and the review proceeds — so a false code-free classification cannot manufacture a false FAIL

#### Scenario: The sanctioned substitute for a docs-only change is primary-source verification
- **GIVEN** a docs-only change that cannot be roborev-certified
- **WHEN** the change is prepared for merge
- **THEN** doctrine directs the author to record primary-source verification in the pull request (for example reading the pinned Cassandra source at the `cassandra-5.0.8` tag that the documentation describes) instead of recording "roborev clean"

#### Scenario: A measurement harness under a report's artifact directory is CODE, not documentation
- **GIVEN** a census consisting of `docs/reports/ws0-3217-artifacts/harness/partA.sh`, `.../classify.py` and `.../offcpu.bt` alongside `docs/reports/ws0-3217-report.md`
- **WHEN** the wrapper classifies the census
- **THEN** the three executables count as CODE, `code-free:` reads `PASS`, the review IS enqueued, and no `docs/` path prefix contributes to a code-free classification

#### Scenario: A docs artifact tree with no executables is still code-free
- **GIVEN** a census consisting only of markdown plus declared docs-scoped artifacts (`.txt`, `.json`, `.log`, `.err`, `.jsonl` under `docs/reports/*-artifacts/`)
- **WHEN** the wrapper classifies the census
- **THEN** `code-free:` FAILs deterministically with no review enqueued, because every path in it is one the configured exclusion set removes — so the narrowing did not trade the old blind spot for a vacuous review of a diff roborev would empty


### Requirement: The wrapper emits a machine-greppable summary block with a terminal verdict
The wrapper SHALL emit a single compact `==== ROBOREV REVIEW SUMMARY ====` block on every **VERDICT**
exit path — a pass, any failed check, or an empty census — carrying one field per line, in a FIXED
order that is part of the contract, under the greppable keys: `repo:`, `branch:`, `base:`, `head-sha:`,
`reviewed-sha:`, `job:`, `model:`, `census:`, `tokens:`, `push-assert:`, `census-check:`, `code-free:`,
`census-exclusion:`, `job-record:`, `sha-assert:`, `review-completed:`, `prompt-content:`,
`vacuity-tier1:`, `vacuity-tier2:`, `findings:`, `roborev-exit:`, `log:`, and a terminal
`RESULT: PASS|FAIL|NOTHING-TO-REVIEW`. `census-exclusion:` SHALL sit immediately after `code-free:`,
mirroring its pre-enqueue evaluation order, and SHALL appear EXACTLY ONCE.
`reviewed-sha:` SHALL carry the reviewed RANGE `<base40>..<head40>` on a normal run (a single sha only
when the record reports one, and `-` when it is unverifiable), so a reader SHALL NOT expect a bare sha
there.

Every per-check key SHALL participate in ONE verdict scan in which a value beginning `FAIL`, `FINDINGS`,
`ERROR` or `INCONSISTENT` fails the run, and `PASS*`, `SKIP`, `UNAVAILABLE`, `NOTICE*` and `DEGRADED*`
never do. `DEGRADED` is non-failing BY DESIGN and only ever appears on `job-record:`, whose consequences
are published by the dependent asserts under their own keys. A per-check key whose
step was never reached SHALL carry an explicit `SKIP` rather than a blank, so an unreached check can
never read as a pass. The block's name SHALL be distinct from the agent gate's summary block names so
neither can be pasted as the other. The wrapper SHALL exit non-zero on any outcome other than PASS, and
SHALL be usable such that a caller retains ONLY this block and never the raw review transcript (which
SHALL be written to the log path named in the block's `log:` field). An unexpected mid-run abort SHALL
still emit the block with `RESULT: FAIL` rather than terminate silently.

A **USAGE ERROR is NOT a verdict.** When a required option is missing or invalid (notably `--agent`
without `--model`, or the reverse), the wrapper SHALL emit **NO summary block at all**: it SHALL print a
loud `ERROR:` line naming the missing or invalid option and SHALL exit with the dedicated usage code
`2`, before any repository identity is resolved and before anything is enqueued. This omission is
DELIBERATE and SHALL NOT be "fixed" by emitting a block: the three `RESULT:` values are reserved for the
three real outcomes, so a `RESULT:` line for a run that never happened would ALIAS a usage error onto a
genuine verdict — precisely the indistinguishability this capability exists to eliminate. The `--help`
path (exit `0`) is likewise not a verdict and SHALL emit no block.

#### Scenario: Every verdict run emits exactly one block with a terminal RESULT
- **WHEN** the wrapper finishes on a verdict path (pass, any failed check, or an empty census)
- **THEN** it emits exactly one `==== ROBOREV REVIEW SUMMARY ====` block whose last line is `RESULT:` followed by exactly one of `PASS`, `FAIL`, or `NOTHING-TO-REVIEW`

#### Scenario: The block carries every per-check key in the contracted order
- **WHEN** a review was enqueued and completed
- **THEN** the block carries `repo:`, `branch:`, `base:`, `head-sha:`, `reviewed-sha:`, `job:`, `model:`, `census:`, `tokens:`, `push-assert:`, `census-check:`, `code-free:`, `census-exclusion:`, `job-record:`, `sha-assert:`, `review-completed:`, `prompt-content:`, `vacuity-tier1:`, `vacuity-tier2:`, `findings:`, `roborev-exit:` and `log:` in that order, ahead of the terminal `RESULT:`

#### Scenario: The new exclusion key is registered in the verdict scan, not decorative
- **GIVEN** a run whose only failing key is `census-exclusion:`
- **WHEN** the terminal verdict is computed by the single scan over the per-check keys
- **THEN** the run is `RESULT: FAIL` with a non-zero exit — so a key added to the block but omitted from the failing-capable key set (a FAIL that changes nothing) is a defect this scenario forbids

#### Scenario: An unreached exclusion check reads SKIP, never blank
- **GIVEN** a run that fails at `push-assert:` before the census is classified
- **WHEN** the block is emitted
- **THEN** `census-exclusion:` carries an explicit `SKIP (<cause>)` rather than a blank value, so an unreached check can never read as a pass

#### Scenario: One scan over the per-check keys computes the verdict
- **GIVEN** a block in which exactly one per-check key carries a value beginning `FAIL`, `FINDINGS`, `ERROR` or `INCONSISTENT` while every other reads `PASS*`, `SKIP`, `UNAVAILABLE`, `NOTICE*` or `DEGRADED*`
- **WHEN** the terminal verdict is computed
- **THEN** the run is `RESULT: FAIL` and the failing key names the cause, and a `NOTICE`, `DEGRADED`, `UNAVAILABLE` or `SKIP` value never contributes a failure

#### Scenario: The reviewed scope is reported as a range
- **WHEN** a normal run's block is read
- **THEN** `reviewed-sha:` carries `<base40>..<head40>` rather than a bare sha, so any consumer comparing it for equality with `head-sha:` SHALL compare the range's HEAD endpoint instead

#### Scenario: A usage error emits no block and exits with its own distinct code
- **GIVEN** an invocation supplying `--agent` but not `--model` (or `--model` but not `--agent`)
- **WHEN** the wrapper runs
- **THEN** it prints an `ERROR:` line naming the missing option, emits NO `==== ROBOREV REVIEW SUMMARY ====` block and NO `RESULT:` line at all, enqueues nothing, and exits `2` — a code distinct from PASS (`0`), FAIL (`1`), and NOTHING-TO-REVIEW (`3`), so a usage error can never be read as any of the three verdicts

#### Scenario: An unexpected abort still emits a block
- **GIVEN** a run that dies mid-flight after the review was enqueued, before reaching a verdict
- **WHEN** the process exits
- **THEN** it still emits exactly one block with `RESULT: FAIL` and a line reporting the unexpected termination, so a run that died without a verdict never looks like a run that was never made

#### Scenario: The block cannot be confused with an agent-gate summary
- **WHEN** the block is compared with the agent gate's `AGENT-GATE SUMMARY`, `AGENT-GATE LITE SUMMARY`, and `AGENT-GATE DELTA SUMMARY` blocks
- **THEN** its header is distinct from all three, so a roborev summary can never be pasted as a gate verdict nor a gate summary recorded as a review verdict

#### Scenario: A non-PASS outcome exits non-zero
- **WHEN** the terminal `RESULT:` is `FAIL` or `NOTHING-TO-REVIEW`
- **THEN** the wrapper's process exit code is non-zero


### Requirement: A hermetic regression check pins every vacuity trigger and is wired into the agent gate
A regression check SHALL exercise the wrapper hermetically — using a stub `roborev` on `PATH` that
replays recorded real outputs, with no network, no live reviewer, no dataset corpus and no cargo — and
SHALL assert that the wrapper:

(a) FAILs when the reviewed sha equals the base ref, naming the base; (b) FAILs when the reviewed scope
does not match the census range at either endpoint; (c) FAILs a cleanliness vacuity claim against a
non-empty code census — INCLUDING one whose sentence sits under a `## Summary` HEADING — and does NOT
fail a findings-bearing or out-of-summary mention of the same phrase; (d) FAILs the vacuous token
signature, and pins the input floor at its exact declared value; (e) FAILs an unpushed branch, a branch
absent from the remote, a stale-mirror/deleted-remote branch, and an `ls-remote` failure attributed to
infra/auth — including under the fleet's NARROW fetch refspec, where the branch IS pushed and the
assert must PASS; (f) PASSes a genuine review with a matching range and healthy accounting, asserting the
SANCTIONED ARGV itself (`--branch` PAIRED with an explicit absolute `--repo`, both reviewer flags, and
neither two positionals nor a single positional sha); (g) reports
`NOTHING-TO-REVIEW` rather than PASS on a genuinely empty census, and FAILs (never
`NOTHING-TO-REVIEW`) on an unresolvable base or a failed `git diff`; (h) FAILs a code-free census
deterministically while NOT classifying a workflow YAML or a mixed census as code-free; (i) FAILs when
the job never completed, when the provider returned a model-mismatch error, and when the job status is
not `done`; (j) FAILs when the prompt actually sent omits the census's code paths AND when the prompt is
UNRETRIEVABLE, and PASSes a census whose rename appears in the prompt as a single two-sided
`diff --git a/old b/new` header; (k) distinguishes `FINDINGS` from `ERROR` on a non-zero reviewer exit,
and FAILs both `INCONSISTENT` findings states (a clean structured verdict, and a zero exit, each beside
in-block severity markers); (l) evaluates token accounting against the REAL doubly-encoded payload shape,
accepts the documented field aliases, and FAILs a present-but-unparseable payload as drift; (m) FAILs
closed when EITHER sourced helper — the oracles file or the per-review-checks file — is missing or
truncated, with no review enqueued; (n) refuses a SINGLE-COMMIT job record even when it equals branch
HEAD; and (o) pins the job-record read: `PASS` on a complete record, `PASS` when the required fields live
in the NESTED job row of a `show --json` payload whose outer review row lacks them, and `DEGRADED` plus
`sha-assert: FAIL (job record unavailable …)` when no source answers.

The check SHALL additionally pin the exclusion reconciliation, which requires the fixture helper to write
the work repository's OWN roborev configuration (without that capability a configuration regression is not
expressible at all, which is why the defect could ship): (p) a fixture diff of EXECUTABLES under
`docs/reports/*-artifacts/` (`.py`, `.sh`, `.bt`) under the narrowed configuration yields `code-free: PASS`
and `census-exclusion: PASS` and IS enqueued; (q) a PROSE-ONLY diff under `docs/` still yields
`code-free: FAIL` with NO review enqueued, so the narrowing did not invert the guard; (r) a configuration
whose `exclude_patterns` WOULD swallow census code — notably a restored `['docs/**', '*.md']` — yields
`census-exclusion: FAIL` NAMING the swallowed paths, `RESULT: FAIL` and NO review enqueued; (s) an
`exclude_patterns` key present with an unparseable value FAILs as `exclusion set unreadable` while an
absent key/configuration file reads `PASS (no exclusion patterns configured)`; (t) a census path containing
SPACES and a literal double quote is compared correctly (the NUL-safety regression, which a
non-`-z` comparison would silently mis-handle as a false PASS); (u) the corroboration states — a stub
that does not answer `config get` reports `UNAVAILABLE` without failing, and one reporting a pattern absent
from the parsed set FAILs as `exclusion set drift`; and (v) the ported `FormatExcludeArgs` construction
itself — a slash-containing pattern leaves a NESTED `docs`-directory census path SURVIVING (no false FAIL),
a bare directory name excludes its whole subtree via the `<p>/**` sibling pathspec, a leading-`/` pattern
excludes only the root-level path while its slash-less twin excludes at any depth, a TRAILING-slash pattern
FAILs naming the recursive inversion, and an empty-after-trim pattern is skipped rather than treated as a
match-everything.

The check SHALL additionally pin the TOTAL/PARTIAL built-in boundary and the header-shape normalisation:
(w) a LOCKFILE-ONLY census (a `Cargo.lock` bump beside prose) yields `code-free: PASS` and
`census-exclusion: FAIL` naming the EMPTY diff, with NO review enqueued, no `NOTICE` value, and no
`prompt-content: PASS (0/0 …)` anywhere; (x) the SAME lockfile beside a surviving `.rs` file still yields
the `NOTICE` and IS enqueued, so the boundary is TOTAL vs PARTIAL and the pinned-built-in ruling is
demonstrably intact; (y) `prompt-content:` refuses to report a pass when no census path is checkable, driven
DIRECTLY against the function so the assertion survives the upstream pre-enqueue FAIL that makes the state
unreachable through the wrapper; and (z) a code census path containing SPACES, one under a space-bearing
DIRECTORY, and one with a NON-ASCII (octal-escaped) name each yield `prompt-content: PASS` and
`RESULT: PASS`.

**Every hostile-path or hostile-verdict case SHALL assert the terminal `RESULT:` and, where the path
reaches the reviewer, `prompt-content:` — not one intermediate key alone.** A case that asserted only
`census-exclusion:` reported two passes while the SAME fixture false-FAILed `prompt-content:` and the run
terminated `RESULT: FAIL`: a case that passes while the behaviour it names is broken is worse than no case,
because it is read as coverage. The suite's stub SHALL emit a VALID JSON job record for a prompt containing
double quotes, so a quote-bearing prompt cannot degrade the record and mask the very comparison the case
exists to pin.

The check SHALL also pin the block's key ORDER — including `census-exclusion:` appearing EXACTLY ONCE
immediately after `code-free:` — the distinctness of its header from all three
agent-gate summary headers, the usage-error path emitting no block, and hermeticity itself. It SHALL be
registered in the agent gate's shell-tooling component set such that it runs in the fast `--lite` loop
as well as the full gate, so a regression FAILs the fast loop rather than costing a review round. The
check SHALL contain no wall-clock threshold assertion in its correctness path, and SHALL report a loud
SKIP rather than a silent pass when an optional prerequisite for a subset of cases is unavailable.

#### Scenario: Every trigger class is asserted against the block's own keys
- **WHEN** the regression check runs
- **THEN** it asserts each of the classes (a) through (z) above against the wrapper's terminal `RESULT`, its per-check key values and its exit code, and it reports an explicit pass/fail tally so a partial run cannot read as a pass

#### Scenario: The total-swallow and partial-swallow cases are both pinned
- **GIVEN** two hermetic fixtures under the narrowed configuration — one whose census is a `Cargo.lock` bump beside a prose edit, one whose census is the same lockfile beside a `.rs` file
- **WHEN** the regression check runs the wrapper against each
- **THEN** the first reports `census-exclusion: FAIL` naming the EMPTY diff with `RESULT: FAIL` and nothing enqueued, the second reports `census-exclusion: NOTICE` with `RESULT: PASS` and IS enqueued, and neither can drift into the other without failing the fast loop

#### Scenario: A hostile-path case asserts the verdict, not one intermediate key
- **WHEN** the suite's hostile-path cases (spaces, a literal quote, a space-bearing directory, a non-ASCII name) are inspected
- **THEN** each asserts the terminal `RESULT:` and the `prompt-content:` value in addition to `census-exclusion:`, and the stub emits a VALID JSON record for a quote-bearing prompt so the record cannot degrade and mask the comparison

#### Scenario: The zero-subject refusal is driven directly against the check
- **GIVEN** that the pre-enqueue total-swallow FAIL makes a zero-subject `prompt-content:` unreachable through the wrapper
- **WHEN** the regression check exercises the check function directly, in the real files, with every census code path built-in-excluded
- **THEN** it asserts `FAIL (no code census path was checkable — a 0/0 is never a pass)` and asserts the ABSENCE of any `PASS (0/0` form, so removing the upstream FAIL cannot silently restore the vacuous pass

#### Scenario: Executables under a docs artifact directory are enqueued, prose under docs is not
- **GIVEN** two hermetic fixtures under the narrowed configuration — one whose diff is `.py`/`.sh`/`.bt` files under `docs/reports/x-artifacts/`, one whose diff is only markdown under `docs/`
- **WHEN** the regression check runs the wrapper against each
- **THEN** the first reports `code-free: PASS`, `census-exclusion: PASS` and IS enqueued, while the second reports `code-free: FAIL` and is asserted never enqueued

#### Scenario: A configuration regression is caught by the fixture's own roborev configuration
- **GIVEN** a hermetic fixture that writes its own `.roborev.toml` with `exclude_patterns = ['docs/**', '*.md']` and a census of executables under `docs/`
- **WHEN** the regression check runs the wrapper
- **THEN** `census-exclusion:` FAILs naming the swallowed paths, the terminal `RESULT:` is `FAIL`, nothing is enqueued, and the case is expressible precisely because the fixture can supply its own configuration

#### Scenario: The ported pathspec construction is pinned case by case
- **GIVEN** hermetic fixtures configuring, separately, `docs/**/*.json` with a census path under a nested `docs` directory, a bare directory name, `/README.md` versus `README.md`, `docs/` with a trailing slash, and a whitespace-only pattern
- **WHEN** the regression check runs the wrapper against each
- **THEN** the nested path is reported SURVIVING, the bare directory name excludes its whole subtree, the leading-`/` form excludes only the root-level path while its slash-less twin excludes at any depth, the trailing-slash form FAILs naming the recursive inversion, and the whitespace-only pattern is skipped — so a future edit that "simplifies" the construction away from `FormatExcludeArgs` FAILs the fast loop

#### Scenario: The exclusion cases stay hermetic
- **WHEN** the new cases run on a machine with no network access and no real roborev binary installed
- **THEN** they complete using the stub reviewer, the fixture's own git repository and the fixture's own configuration file, with the corroboration reported `UNAVAILABLE` rather than causing a failure or a skip

#### Scenario: Every wrapper invocation in the suite redirects HOME
- **GIVEN** the reconciliation check reads the GLOBAL `$HOME/.roborev/config.toml` into the effective set
- **WHEN** the regression suite is inspected for invocations of the wrapper, including the hand-rolled ones that do not go through the shared runner
- **THEN** every one of them redirects `HOME` to the throwaway fixture home, so a host whose real global config carries a pattern cannot make a case fail on `census-exclusion:` before its own assertion is ever reached

#### Scenario: No case blesses a guard that has silently self-disabled
- **WHEN** the cases that expect `PASS (no exclusion patterns configured…)` are inspected
- **THEN** each of them supplies a binary that ANSWERS with an empty list and asserts the corroboration is `OK`, so no case in the suite records a green verdict for the state a guard reaches when it fails to recognise a configured key

#### Scenario: The tally line cannot be mistaken for a gate or wrapper verdict
- **WHEN** the regression check finishes
- **THEN** its tally line reports the passed/failed counts under its own distinct heading and does NOT begin with the `RESULT:` token, which belongs to the agent gate's summary contract and to the wrapper's own block

#### Scenario: The check is hermetic
- **WHEN** the regression check runs on a machine with no network access and no real roborev binary installed
- **THEN** it still runs to completion using the stub reviewer and throwaway git fixtures, requiring no dataset corpus, no cargo, no live reviewer and no network

#### Scenario: A regression fails the fast loop
- **GIVEN** a change that removes or weakens one of the wrapper's asserts
- **WHEN** `scripts/agent-gate.sh --lite` runs
- **THEN** the component that hosts the regression check FAILs, so the fast loop catches the regression rather than a later review round

#### Scenario: The check also runs in the full gate
- **WHEN** the full `scripts/agent-gate.sh` runs
- **THEN** the regression check executes as part of the shell-tooling component set and a failure FAILs that component and the run


### Requirement: Doctrine records the roborev rules, including the measured invocation matrix
CLAUDE.md's roborev-invocation guidance and the published `agents-developing/roborev-findings` page
SHALL both state, in this same change: (a) the wrapper is the only sanctioned roborev invocation;
(b) the reviewed SCOPE must be verified against the census range (branch HEAD included);
(c) a "contains no code changes to review" verdict on a non-empty diff is a HARD FAIL, never a pass; and
(d) a docs-only diff cannot be roborev-certified. Both SHALL also record the wrapper's exit-code contract
and that ANY non-PASS terminal `RESULT` — `NOTHING-TO-REVIEW` included — is a failed round and a blocked
merge. The `roborev-findings` page SHALL additionally carry the new guard in its "mechanized in `--lite`"
table, since a mechanized class that is not listed there will be hand-checked forever. The published page
SHALL be accepted by confirming the NEW CONTENT is served — not by an HTTP 200 — because the CDN can
serve the previous page for minutes after a successful deploy.

**THREE MEASURED CORRECTIONS SHALL land on EVERY surface that states the rule** — CLAUDE.md,
`website/.../agents-developing/roborev-findings.md`, `website/.../agents-developing/delivery-pipeline.md`,
`docs/development/pm-operating-loop.md`, `docs/development/agent-machine-setup.md` — because the earlier
wording FORBIDS the form now known to be correct:

1. **`--repo` is what makes `--branch` correct from a worktree.** The non-sanctioned form is therefore
   `--branch` **WITHOUT** an explicit `--repo` (it resolves against the ROOT checkout, normally on the
   base branch) — NOT `--branch` as such. Any absolute "bare `--branch` is non-sanctioned" claim SHALL be
   narrowed accordingly wherever it appears.
2. **The single-SHA form reviews ONE COMMIT, not the branch** — a FOURTH vacuity class (a PARTIAL review
   reported as a complete one) on every multi-commit branch. It SHALL be named non-sanctioned alongside
   the two-positional form (whose range base is git's EMPTY-TREE hash).
3. **roborev drops exactly the paths its CONFIGURED `exclude_patterns` match, applied as git pathspec
   exclusions — it makes NO code/non-code judgement.** The earlier claim that roborev "EXCLUDES non-code
   paths from the diff it builds" is **FALSIFIED and SHALL NOT be restated anywhere**: under the
   configured `docs/**` the same mechanism discarded 33 EXECUTABLE harness files on PR #3222
   (`prompt-content: FAIL (136/136 code census paths absent)`, 15,443 input / 89 output tokens). Doctrine
   SHALL state the configured-pathspec mechanism, that a markdown-only diff is empty because `*.md` is
   configured (not because the reviewer recognised prose), that the wrapper's `prompt-content:` check
   covers the CODE subset of the census, and that the deterministic pre-enqueue `code-free:` FAIL plus the
   `census-exclusion:` reconciliation are the correct responses.

**Doctrine SHALL NOT imply that everything under `docs/` is code-free.** Every surface stating the
docs-only rule SHALL be amended in this same change to (a) name the `docs/reports/*-artifacts/` harness
convention EXPLICITLY as executable code that IS reviewed and that a PR carrying it is NOT a docs-only
change, (b) state that "docs-only" means a code-free CENSUS as the wrapper classifies it, never a
directory prefix, and (c) name `census-exclusion:` as the pre-enqueue key that FAILs when the configured
exclusion set would swallow census code. The surfaces SHALL include, beyond the two AC4 surfaces:
`website/.../agents-developing/delivery-pipeline.md`, `.claude/agents/flow-lead.md`,
`.claude/agents/flow-closer.md`, `.claude/skills/flow-implement/SKILL.md`, and the header comments of all
three `scripts/flow/roborev-review*.sh` files — including the `roborev_check_prompt_content()` comment
that states the falsified claim outright. A surface left un-amended is doctrine drift against itself, and
this requirement is not satisfied while any copy still asserts the falsified mechanism.

Where doctrine documents the summary block it SHALL carry the `job-record:` key, the `census-exclusion:`
key in its contracted position immediately after `code-free:`, and the corrected `prompt-content:` values
(an unretrievable prompt FAILS; there is no non-failing `UNAVAILABLE` for that key). Where doctrine
documents the live probe it SHALL state the expectation in the RANGE form — the `reviewed-sha:` range's
HEAD endpoint equals the worktree HEAD and its base equals the base ref — never as `reviewed-sha`
equalling the worktree HEAD.

#### Scenario: Doctrine states the verdict rule verbatim, as one rule
- **WHEN** CLAUDE.md, `website/src/content/docs/agents-developing/roborev-findings.md` and this change's `design.md` are inspected
- **THEN** each carries the sentence "FAIL where the author can act; NOTICE where only the information is actionable; never silence." verbatim, and each presents it as ONE rule resolving the configured-pattern FAIL, the pinned-built-in NOTICE and the built-in-divergence FAIL — rather than as three independent judgements a future editor would have to re-derive

#### Scenario: Doctrine records the three config-ordering properties and their generalization
- **WHEN** CLAUDE.md and `roborev-findings.md` are inspected beside the existing note that `required` evaluates the aggregator and registry from the PR's BASE ref
- **THEN** both state that roborev's daemon reads `exclude_patterns` from the repo ROOT PATH so a worktree edit is invisible to it, that the daemon snapshots config at start so an edit needs a restart, that BOTH have already cost real rounds, and that the generalization is "any PR whose subject is a config the daemon (or a gate) reads from root cannot certify itself" — explicitly noted as the same shape as the BASE-ref property

#### Scenario: Doctrine records that the PRE-EXISTING guard caught the NEW guard
- **WHEN** the defence-in-depth rationale in `roborev-findings.md` and `design.md` is inspected
- **THEN** it records that `prompt-content:` — the older check — caught the newly added `census-exclusion:` certifying a config roborev never read, and states this as the strongest argument in the change for keeping both layers, explicitly because it paid out in the direction nobody plans for: the NEW layer was the wrong one

#### Scenario: Doctrine records that a test blessing a vacuous verdict is worse than an unguarded path
- **WHEN** the doctrine page is inspected
- **THEN** it records that the two regression cases which locked in an un-corroborated "no exclusion patterns configured" PASS were worse than having no case at all, because such a test consumes the review budget that would otherwise have found the bug and converts "nobody checked" into "we checked and it was fine"

#### Scenario: Both AC4 doctrine surfaces carry all four rules
- **WHEN** CLAUDE.md and `website/src/content/docs/agents-developing/roborev-findings.md` are inspected after this change
- **THEN** both state that the wrapper is the only sanctioned invocation, that the reviewed scope must be verified against the census range, that a "contains no code changes to review" verdict on a non-empty diff is a HARD FAIL, and that a docs-only diff cannot be roborev-certified

#### Scenario: Every rule-stating surface carries the three measured corrections
- **WHEN** CLAUDE.md, `roborev-findings.md`, `delivery-pipeline.md`, `docs/development/pm-operating-loop.md` and `docs/development/agent-machine-setup.md` are inspected
- **THEN** none of them still forbids `--branch` unconditionally (each names the non-sanctioned form as `--branch` WITHOUT an explicit `--repo`), each names the single-SHA form as a partial review, and the roborev-findings page records that roborev drops exactly the paths its configured `exclude_patterns` match rather than making a code/non-code judgement

#### Scenario: No surface still claims roborev excludes non-code paths
- **WHEN** CLAUDE.md, `roborev-findings.md`, `delivery-pipeline.md`, `.claude/agents/flow-lead.md`, `.claude/agents/flow-closer.md`, `.claude/skills/flow-implement/SKILL.md` and the three `scripts/flow/roborev-review*.sh` header comments (including `roborev_check_prompt_content()`'s) are grepped for the falsified claim
- **THEN** no copy remains, each instead states the configured-pathspec mechanism, and the falsified wording appears nowhere in the tree

#### Scenario: Doctrine names the harness convention as reviewed code
- **WHEN** the docs-only rule is read on CLAUDE.md and the `roborev-findings` page after this change
- **THEN** both name `docs/reports/*-artifacts/` measurement harnesses explicitly as executable code that IS reviewed, state that "docs-only" means a code-free census rather than a `docs/` path prefix, and name `census-exclusion:` as the pre-enqueue key that FAILs when the configured exclusion set would swallow census code

#### Scenario: The live-probe expectation is stated in the range form
- **WHEN** the doctrine page's live worktree probe section is inspected
- **THEN** it asks the reader to confirm the `reviewed-sha:` RANGE — its HEAD endpoint equal to the worktree branch HEAD and its base equal to the base ref — rather than a `reviewed-sha` equal to the worktree HEAD, which the range value can never satisfy

#### Scenario: The mechanized-in-lite table lists the new guard
- **WHEN** the `roborev-findings` page's table of classes mechanized in `--lite` is inspected
- **THEN** it carries a row for the vacuous-review class naming the hermetic regression check and the components it runs in

#### Scenario: Publication is accepted by the served content, not a status code
- **WHEN** the published `agents-developing/roborev-findings` page is verified after deployment
- **THEN** acceptance is established by fetching the page and matching a distinctive phrase introduced by this change, and an HTTP 200 without that phrase is treated as not-yet-published rather than as done

