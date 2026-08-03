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

The effective set SHALL be read from the configuration FILES (the repository `.roborev.toml` and the
global `~/.roborev/config.toml`), not from the `roborev` binary, so the check stays hermetic and
stub-testable and so no reordering of the wrapper's existing `command -v roborev` validation is required.
The parse SHALL respect TOML table scoping (a same-named key inside a `[table]` is NOT the top-level key)
and SHALL fail closed rather than guess. Repository and global patterns SHALL be combined as a UNION,
which is what `config.ResolveExcludePatterns` / `loadRepoExcludePatterns` do (the global list is
currently empty, so the repository list is today's whole effective set). When `roborev` IS invocable
the parsed set SHALL be CORROBORATED against `roborev config get exclude_patterns`; a pattern the binary
reports that the parse LACKS SHALL be `FAIL (exclusion set drift: …)` because that direction can hide a
swallow, the reverse direction SHALL be a non-failing NOTICE, and an absent binary SHALL report the
corroboration as `UNAVAILABLE` without failing.

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

#### Scenario: The declared residual direction is noise, never a swallow
- **GIVEN** a census path the wrapper classifies as a non-code artifact which the configuration does NOT exclude
- **WHEN** the wrapper runs
- **THEN** no key fails on account of it, the path is simply delivered to the reviewer, and the documented residual states that this direction can only add review noise while the opposite direction is always a pre-enqueue FAIL

### Requirement: A recorded live probe demonstrates the narrowed exclusion on the real harness-PR shape
The change SHALL include a **recorded live probe** — run, not asserted — of the sanctioned wrapper against
a diff of the shape that failed: executable harness files under `docs/reports/*-artifacts/`. The probe
SHALL be executed with the sanctioned invocation (`--agent codex --model gpt-5.6-sol` with an explicit
absolute `--repo`), and its RECORD in the pull request SHALL carry: the `census:` counts, the
`code-free:` and `census-exclusion:` lines, the `prompt-content:` line, and the input / cached / output
token counts from the job record.

The probe's PASS condition SHALL be `prompt-content: PASS (<n>/<n> code census paths present)` together
with a token signature in the **genuine-review band** (398k–649k input, 5.0k–6.3k output, minutes of wall
time) rather than the **vacuous baseline** (~18.7k input, 0 cached, 53–56 output, ~8s). The measured
failure being repaired is PR #3222's `prompt-content: FAIL (136/136 code census paths absent)` at 15,443
input / 89 output, so a signature near that baseline SHALL be read as the defect persisting, whatever the
verdict text says.

The probe diff SHALL additionally include a file under a NESTED `docs` directory (for example under
`website/src/content/docs/`) carrying one of the deny-listed artifact extensions, as an END-TO-END
CONFIRMATION of the disassembly-derived prediction: because a pattern with an interior `/` is
root-anchored, that nested path SHALL still be DELIVERED to the reviewer. Its absence from the prompt
would falsify the recovered algorithm and SHALL be treated as a blocking finding — the pattern list and
the check's construction both depend on the port being correct. Because the probe needs the network and a
live reviewer, it SHALL be documented and recorded rather than executed by the agent gate.

#### Scenario: The recorded probe shows the code census present and a genuine token signature
- **GIVEN** the narrowed exclusion configuration and a branch whose diff is executables under `docs/reports/*-artifacts/`
- **WHEN** the sanctioned wrapper is run against it and the result recorded in the pull request
- **THEN** the record shows `census-exclusion: PASS`, `prompt-content: PASS (<n>/<n> code census paths present)`, and input/cached/output token counts inside the genuine-review band rather than the vacuous baseline

#### Scenario: The probe confirms the disassembly-derived root anchoring end to end
- **GIVEN** a probe diff that includes a deny-listed artifact extension under a nested `docs` directory such as `website/src/content/docs/`
- **WHEN** the prompt actually sent is inspected
- **THEN** that nested path IS present in the prompt — confirming live that a pattern containing an interior `/` is root-anchored as the recovered `git.FormatExcludeArgs` specifies — and its absence would instead falsify the port and block the change rather than being recorded as an acceptable outcome

#### Scenario: The demonstration is recorded evidence, not an assertion
- **WHEN** the pull request is reviewed for AC2
- **THEN** it carries the actual summary-block lines and token counts from a real run, and a statement that the narrowed configuration "should" work is NOT accepted in their place

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

The check SHALL also pin the block's key ORDER — including `census-exclusion:` appearing EXACTLY ONCE
immediately after `code-free:` — the distinctness of its header from all three
agent-gate summary headers, the usage-error path emitting no block, and hermeticity itself. It SHALL be
registered in the agent gate's shell-tooling component set such that it runs in the fast `--lite` loop
as well as the full gate, so a regression FAILs the fast loop rather than costing a review round. The
check SHALL contain no wall-clock threshold assertion in its correctness path, and SHALL report a loud
SKIP rather than a silent pass when an optional prerequisite for a subset of cases is unavailable.

#### Scenario: Every trigger class is asserted against the block's own keys
- **WHEN** the regression check runs
- **THEN** it asserts each of the classes (a) through (u) above against the wrapper's terminal `RESULT`, its per-check key values and its exit code, and it reports an explicit pass/fail tally so a partial run cannot read as a pass

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

