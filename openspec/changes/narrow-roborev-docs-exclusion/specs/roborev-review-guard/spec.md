# roborev-review-guard — delta for narrow-roborev-docs-exclusion (issue #3229)

**Architecture note (read this first).** #2964 established the guard as **DETERMINISTIC-PRIMARY**: the
checks that carry the verdict are judged against data the wrapper obtains ITSELF. The wrapper asserted in
a prose comment that roborev "excludes non-code paths"; the configured set actually excluded `docs/**`,
i.e. **code**, and on PR #3222 that discarded 33 executable harness files (a 136-path code census reduced
to an EMPTY prompt: `prompt-content: FAIL (136/136 code census paths absent)`, 15,443 input / 89 output
tokens against the vacuous baseline).

**What this delta does, after the owner's descope ruling:** it **narrows the configured exclusion set** to
prose and non-code artifacts — measured at 71 `docs/` executables reaching the reviewer, 0 markdown, and
nothing outside `docs/` newly excluded — and it strengthens the wrapper's TERMINAL VERDICT so no key's
value can reach `RESULT: PASS` without an affirmative measurement. It does **NOT** predict roborev's
effective exclusion set. A pre-enqueue oracle that did was built here and REMOVED; see
*DEFERRED Requirements* below. A path the reviewer does not receive still fails CLOSED — under
`prompt-content:`, after the review round rather than before it.

**Acceptance-criterion → requirement map** (issue #3229's numbered ACs):

| AC | Requirement(s) |
|----|----------------|
| 1 — the exclusion is narrowed so executable files under `docs/` are reviewed | ADDED *The review-diff exclusion set excludes prose and non-code artifacts, never executable code* |
| 2 — demonstrated on the real PR #3222 shape, recorded not asserted | ADDED *A recorded live probe demonstrates the narrowed exclusion on the real harness-PR shape* |
| 3 — the wrapper FAILs loudly, pre-enqueue, when the config would swallow the census | **DEFERRED to issue #3283** — owner ruling. The requirement that carried it is REMOVED from this delta with its implementation; see *DEFERRED Requirements* below. NOT satisfied, NOT waived. |
| 4 — the two classifiers are reconciled, **or the residual disagreement is documented with the exact cases where it persists** | **DEFERRED to issue #3283** — owner ruling, same removal. NOT satisfied through either branch, NOT waived; see *DEFERRED Requirements* below. |
| 5 — a hermetic regression test pins the behaviour | MODIFIED *A hermetic regression check pins every vacuity trigger and is wired into the agent gate* |
| 6 — doctrine updated in the same change, publication verified by served content | MODIFIED *Doctrine records the roborev rules, including the measured invocation matrix* |
| 7 — the backfill decision is recorded | ADDED *The backfill ruling for already-merged, never-reviewed harness code is recorded* |

**Mechanism note — how the exclusion semantics were established. RECORDED FOR #3283; NO CODE IN THIS
CHANGE RELIES ON IT.** The findings below are primary-source measurement and are kept because #3283 will
need them. They no longer describe anything this change implements: the port they specified is deleted. `roborev` is an external **stripped
Go binary** (`roborev v0.61.2`, `/usr/local/bin/roborev`) with no source available, so its behaviour is
stated here only where it was **measured**: `exclude_patterns` is implemented as git pathspec
(`:(exclude,glob)`, symbols `git.FormatExcludeArgs` / `config.ResolveExcludePatterns`), i.e. git
wildmatch with `WM_PATHNAME` — anchored at the repository root, `*` not crossing `/`. Replaying 21 real
reviews from `~/.roborev/reviews.db` against their recorded `git_ref` ranges, the ONLY paths ever dropped
from a prompt were 25 paths, EVERY ONE a `.md` — including `.claude/agents/*.md`, `openspec/**/*.md`,
`website/**/*.md` and top-level `CLAUDE.md` — which `docs/**` cannot explain, so a **slash-less pattern
is applied recursively** (normalised to `**/<pattern>`). Every non-`.md` path in that replay reached its
prompt. The pathspec CONSTRUCTION is no longer inferred at all: `git.FormatExcludeArgs` was recovered by
DISASSEMBLING the stripped binary (symbols via `.gopclntab`, text base `0x401000`) — a pattern with an
interior or leading `/` is ROOT-ANCHORED and passed verbatim,
a slash-less pattern is `**/`-prefixed (recursive), every pattern emits BOTH `<p>` and `<p>/**`, and a
TRAILING slash is trimmed before the anchoring test so `docs/` and `docs/**` behave OPPOSITELY. The
absence of any negation / re-include capability is likewise a VERIFIED fact at the instruction level, not
an assumption. The construction is pinned to `roborev v0.61.2`. **A caveat #3283 must honour:** reading
those instructions is not the same as reproducing them. The removed port re-derived Go's
`TrimSpace`/`TrimRight`/`TrimLeft` in bash and diverged on U+00A0, because it was tested against a MODEL
of Go rather than against Go.

## DEFERRED Requirements (AC3 and AC4 → issue #3283)

**AC3 and AC4 of issue #3229 are DEFERRED, not satisfied and not waived.** The two requirements that
carried them — *The wrapper fails closed before enqueuing when the effective exclusion set would swallow
census code* and *The exclusion view is computed with git from the effective configuration, and the
residual divergence is declared* — are **REMOVED from this delta together with their implementation**,
by owner ruling. They were ADDED requirements, so removing them means they never enter
`openspec/specs/`; nothing is left in a half-satisfied state, and a C audit of this change assesses
**four** ACs (1, 5, 6, 7), not six with two failing.

**Why.** The removed subsystem was a pre-enqueue oracle that PREDICTED roborev's effective exclusion set:
a bash port of `git.FormatExcludeArgs` over a TOML parse of three configuration sources, reporting under
a `census-exclusion:` summary key. Across review rounds 8–11 its false-PASS count was **increasing**
(1, 1, 2, **3**), and **two of round 11's three defects lived in code the two preceding fix rounds had
just introduced** — a surface on which fixes add defects of the class they close. **A guard with known
documented false-PASSes is worse than no guard, because it invites reliance it cannot support.**
Subtraction, by contrast, cannot add a false-PASS: with nothing predicted, nothing is excused.

**This IS a reduction in coverage.** It is an acceptable one, and it is stated plainly rather than
argued away. (An earlier draft argued the opposite about a residual under AC4's SECOND BRANCH, on the
ground that a disjunction met by its other branch loses nothing. AC4 now has NO satisfied branch, so that
reasoning is VOID and SHALL NOT be carried over — here the requirements are withdrawn outright, and no
statement anywhere in this change may argue THIS removal out of being a coverage reduction.)

**The absent coverage, named in one line:** there is no automated guard against a future `.roborev.toml`
re-broadening; the regression it would catch is a hand edit to a version-controlled file on `main`, and
AC6's doctrine names the hazard in prose.

**What still stands in its place.** AC1's narrowed `.roborev.toml` is the fix for the defect #3229 was
filed for, and it is measured: 71 `docs/` executables reach the reviewer, 0 markdown does, and nothing
outside `docs/` is newly excluded. A path the reviewer does not receive still FAILs, fail-closed, under
`prompt-content:` — after the review round rather than before it, with a cause that names the symptom
("the reviewer did not receive this path") rather than the mechanism. That diagnostic gap is the whole
residual.

**The class-level root cause, recorded for #3283.** **A port is a second implementation, and a second
implementation's correctness is only knowable by differential testing against the original.** The removed
oracle re-derived Go's `TrimSpace`/`TrimRight`/`TrimLeft` rules in bash and was tested against a *model*
of Go, not against Go — which is why the NBSP divergence (Go's `unicode.IsSpace` trims U+00A0; bash's
`${v## }`-style trims do not) was unfindable by care. #3283 must either test differentially against the
real binary or not predict at all.



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
`':(exclude,glob)**/*.md'` leaves 0, matching the observed drops).

Non-code ARTIFACT exclusion SHALL be scoped to ARTIFACT-BEARING DIRECTORIES, expressed
`<artifact-dir-glob>/**/*.<ext>`, covering at minimum the high-volume raw-output and binary/image classes
measured under `docs/` — `txt`, `json`, `jsonl`, `log`, `err`, `csv`, `png`, `svg`, `gz`, `pdf`, `jfr`,
`html`, `mmd`, `tex`, `diff` — over exactly these directory globs: `docs/reports/*-artifacts`,
`docs/round-artifacts`, `docs/**/jfr-reports`, `docs/sstables-definitive-guide/diagrams`.

An extension sweep across ALL of `docs/` (`docs/**/*.<ext>`) SHALL NOT be used, and this is a CORRECTNESS
requirement, not a preference: it hides FUNCTIONAL CONFIGURATION, not merely artifacts. The falsifying
cases are `docs/observability/grafana/dashboards/cqlite-overview.json` — guarded by the full agent gate's
own `kit-dashboard-drift` component, so the repository already treats it as correctness-bearing, yet a PR
editing it was BOTH dropped from the reviewer's diff AND classified code-free, i.e. unreviewable by
construction — and `docs/reports/delivery-telemetry.schema.json`, the schema governing the delivery
ledger. Everything under `docs/` outside those four directories SHALL be REVIEWED. (Measured: of 672
tracked `docs/` files carrying an artifact extension, 667 lie inside the four directories and remain
excluded; the 5 that do not are delivered to the reviewer.)

A BLANKET directory exclude (`<artifact-dir-glob>/**`) SHALL NOT be used either: these directories
deliberately hold EXECUTABLE code beside their output — 63 tracked `.sh`/`.py`/`.rs`/`.c`/`.bt`/`.cql`/
`.yaml`/`.toml` files under `docs/reports/*-artifacts/` alone, plus a `.py` under
`docs/round-artifacts/` — and those harnesses ARE the census `docs/**` swallowed. The exclusion SHALL
therefore remain the INTERSECTION of an artifact extension and an artifact directory.

A deny-list SHALL be used because an allow-list is **NOT EXPRESSIBLE** — now a VERIFIED fact rather than a
working assumption: `git.FormatExcludeArgs`, read at the instruction level, performs only
TrimSpace/TrimRight/TrimLeft/`Index` and has no negation or re-include handling whatsoever (and git
pathspec supports none inside `:(exclude)`), so "review these extensions" cannot be written. The
deny-list's known weakness SHALL be recorded rather than papered over — a NEW artifact DIRECTORY, or a new
artifact extension inside one of the four, is re-admitted to review prompts — and that weakness SHALL
remain a TOKEN-COST issue only, never a correctness one. With the directory scoping above, the stated
asymmetry **"noise, never blindness" SHALL be true as written**: the leak direction costs tokens, and no
pattern reaches outside a directory whose whole purpose is committed run output, so functional
configuration under `docs/` cannot be hidden. That asymmetry SHALL be recorded as SCOPED, not timeless: it
holds for **inert dumps** (`.txt`/`.log`/`.err`), where exclusion costs only noise, and it does NOT hold
for **code-bearing formats** (`.json`/`.html`/`.svg`), for which exclusion is **blindness** because such a
file can be functional configuration under any path. Exclusion of a code-bearing format SHALL therefore be
scoped by DIRECTORY and SHALL NOT be scoped by extension alone. The record SHALL name the falsifying file:
the claim was first written unqualified, and `docs/**/*.json` hid
`docs/observability/grafana/dashboards/cqlite-overview.json`, which the agent gate's own
`kit-dashboard-drift` component guards — so the extension-wide form hid from the reviewer a file the gate
treats as correctness-bearing. The generalisable rule SHALL be stated with it: an extension describes a
FORMAT, whereas a directory records an INTENT (someone decided that tree holds artifacts), which makes a
directory the better proxy for "generated". Globally-scoped (slash-less) exclusion of artifact
extensions SHALL NOT be used, because it would apply repo-wide and hide real configuration and data files
outside `docs/` from review.

#### Scenario: Functional configuration under `docs/` is classified CODE, not artifact
- **GIVEN** a diff containing `docs/observability/grafana/dashboards/cqlite-overview.json`, `docs/reports/delivery-telemetry.schema.json` and `docs/reports/x-artifacts/a.txt`
- **WHEN** the census classifies them
- **THEN** both configuration files are CODE census paths, the change is NOT classified code-free, and only the artifact under the artifact directory is classified non-code — so the narrowing neither hides functional config nor degenerates into reviewing every artifact
- **AND** the configured pattern set contains no pattern that would exclude either configuration file, verified by inspecting `.roborev.toml`

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

### Requirement: File paths are normalised ONCE, at the census, and every consumer uses the normalised form
Every path the wrapper reasons about SHALL be normalised at **exactly one boundary — the census** — and the
**RAW bytes SHALL be the single internal representation** used for classification, comparison and display.
No other consumer SHALL normalise, unquote, or re-derive a path spelling.

**THE MECHANISM.** Paths SHALL be obtained from git **NUL-delimited** (`git diff --numstat -z`,
`git diff --name-only -z`), so they arrive RAW and no unquoting step exists to get wrong; the census
records SHALL be read with a NUL record separator, so a path containing a NEWLINE survives intact. Where a
path spelling arrives from a producer we do NOT control — the reviewer's prompt, whose `diff --git` headers
are C-quoted by roborev's own `git diff` — it SHALL be normalised by the **single** quoted-path decoder, at
the **single** call site that needs it: the canonical header matcher. A consumer SHALL ask that matcher
whether a header names a path; it SHALL NOT parse header shapes, build a path SET, or perform delimiter-based
membership of its own.

**WHY THIS IS A REQUIREMENT AND NOT AN IMPLEMENTATION DETAIL.** Scattered normalisation produced a BLOCKER
IN EVERY REVIEW ROUND of this change — six in total, all the same defect class in a different consumer:
the (since-removed) exclusion oracle compared paths from the wrong config source; a total exclusion swallow
certified an empty prompt;
`prompt-content:` could not parse space-bearing or C-quoted headers; the **census classified a C-quoted path
by its QUOTED spelling** (`docs/é notes.md` read as extension `md"` and prefix `"docs/`, so PROSE counted as
CODE — and a CODE census path that the configured `*.md` removes from the diff roborev builds is exactly a
`prompt-content:` FAIL on an ordinary docs+code branch; REPRODUCED against the repository's own tracked
`docs/research/CQLite Writes (M5) — Analysis & Recommended Paths.md`); rename and MIXED-quoted headers were
unreachable; and a newline-delimited path set turned a newline-bearing path into grep ALTERNATIVES, so its
first line "proved" its presence — a genuine FALSE PASS. Patching the reported consumer each round is
demonstrably a losing strategy: the invariant, not the symptom, is what SHALL be pinned.

**THE HEADER SHAPES the canonical matcher SHALL recognise**, because git emits all of them:
`diff --git a/<raw> b/<raw>` (including SPACE-bearing, which no regex can split unambiguously),
`diff --git "a/<q>" "b/<q>"` (both quoted), and — **only on renames, which is why it was unreachable** —
the MIXED shapes `diff --git "a/<q>" b/<raw>` and `diff --git a/<raw> "b/<q>"`, emitted when only one side
needs quoting. Since our census runs `--no-renames` while the reviewer's diff has rename detection ON, a
rename SHALL be counted as covered when a single header names either census side.

**AMBIGUITY SHALL BE RESOLVED FROM EVIDENCE, NEVER POSITIONALLY.** A space-bearing `diff --git` header
LINE is **irreducibly ambiguous**: `diff --git a/foo b/x b/foo b/x` reads both as the non-rename of a file
named `foo b/x` and as a rename of `foo` to `x b/foo b/x`, and with renames ON both are legal. The matcher
SHALL therefore decide membership in this order, and SHALL NOT substitute a positional or PREFIX test for
any earlier step:

1. **The header's own `rename from` / `rename to` (and `copy from` / `copy to`) lines**, when the prompt
   carried them. git ALWAYS writes them for a rename or copy — one path per line, C-quoted when needed,
   hence exactly decidable — so they are authoritative and the header line SHALL NOT be consulted at all.
   Because these lines FOLLOW the header, header collection SHALL be part of the matcher's boundary
   (the consumer SHALL still know nothing about header shapes), and the extended-header run SHALL be
   BOUNDED so a `rename from` in the reviewer's prose or a diff body line is never attributed to a header.
2. **Equality of the two header sides**, otherwise. Absent rename/copy lines the header is a NON-rename,
   whose two paths are IDENTICAL, so ONLY a split whose `a/` and `b/` sides are EQUAL SHALL be accepted.
3. **Positional enumeration**, last, and ONLY for a header that has no equal split and no rename/copy
   lines — i.e. one that can only be a rename whose rename lines did not reach us.

**A FALSE PASS HERE IS A FALSE PASS IN THE MERGE GATE**, which is why the ordering is a requirement.
MEASURED: with a bare prefix test (`case $rest in "a/<want> b/"*`), a repository tracking a file named
`foo b/x` made the UNRELATED census path `foo` read as PRESENT — `a/foo b/` is a PREFIX of that file's own
header — so `prompt-content:`, the strongest anti-vacuity key the wrapper has, certified delivery of a file
the reviewer never received. The matcher SHALL NOT fail closed on an ambiguous header either: ambiguity is
irreducible, so refusing to decide would red EVERY space-bearing header and reintroduce the false-FAIL
defects this capability already fixed. **Any residual permissiveness SHALL be DECLARED** — step 3 is
permissive, is reachable only for a header that carries a space, names two DIFFERENT paths and arrived
WITHOUT the rename lines git always writes (so unreachable for git's own output), and that boundedness
SHALL be stated at the code, not left implicit. A comment asserting that a permissive step is safe SHALL be
correct or absent: a false safety claim is worse than none, because the next reader relies on it.

#### Scenario: A space-bearing header does not prove an unrelated census path
- **GIVEN** a census containing a file named `foo b/x` beside one named `foo`, and a prompt whose only header is `diff --git a/foo b/x b/foo b/x`
- **WHEN** the wrapper evaluates prompt content
- **THEN** `foo` is reported ABSENT — `prompt-content: FAIL (1/2 code census paths absent from the prompt)` — because a split whose two sides are EQUAL exists, so the header is a non-rename naming only `foo b/x`, and a prefix reading SHALL NOT stand in for a delivery

#### Scenario: An ambiguous rename header is resolved by its rename from/to lines
- **GIVEN** a rename whose header (`diff --git a/p b/x b/p b/x`) admits an EQUAL split that is NOT the true one, and a prompt carrying that header together with the `rename from p` / `rename to x b/p b/x` lines git writes
- **WHEN** the wrapper evaluates prompt content
- **THEN** both census sides count as covered and `prompt-content:` reads `PASS (2/2 code census paths present)`, resolved from the rename lines rather than from the header

#### Scenario: The same header without its rename lines cannot prove either side
- **GIVEN** the same census and the same header with the `rename from` / `rename to` lines REMOVED
- **WHEN** the wrapper evaluates prompt content
- **THEN** both sides are reported ABSENT — `prompt-content: FAIL (2/2 code census paths absent from the prompt)` — so the passing verdict above rests on the rename lines and not on a permissive positional reading

**THE INVARIANT SHALL BE ASSERTED STRUCTURALLY**, not merely by behavioural cases: the hermetic regression
check SHALL fail when a path-reading `git diff` lacks `-z`, when the census normalises inside its own
classification loop, when the quoted-path decoder is defined more than once or called from outside the
canonical matcher, or when a consumer reintroduces header-regex parsing or delimiter-based path membership.
A behavioural case can only cover the shapes someone thought of; a structural assert covers the next
consumer nobody has written yet.

#### Scenario: A non-ASCII prose path is classified by its raw bytes, not its quoted spelling
- **GIVEN** a census containing a non-ASCII documentation path (which a non-`-z` `git diff --numstat` would render C-quoted) beside a real code file, and a configuration excluding `*.md`
- **WHEN** the wrapper classifies the census and evaluates prompt content
- **THEN** the documentation path is classified NON-code, only the code file is a CODE census path, `prompt-content:` reads `PASS (1/1 code census paths present)`, and the terminal `RESULT:` is `PASS` — the ordinary docs+code branch is never false-FAILed

#### Scenario: A non-ASCII docs artifact is classified by its raw bytes too
- **GIVEN** a census containing a non-ASCII docs-scoped artifact (`docs/reports/*-artifacts/é.json`) beside a code file, with the artifact's extension in the configured docs-scoped deny-list
- **WHEN** the wrapper classifies the census
- **THEN** the artifact is classified NON-code by its RAW bytes rather than its quoted spelling, only the code file is a CODE census path, `code-free:` reads `PASS`, and the artifact is never demanded of the prompt roborev's configured exclusions remove it from

#### Scenario: A rename whose BOTH names carry a space is matched
- **GIVEN** a census that splits a rename into two paths, both of which contain a space, and a prompt carrying the single header `diff --git a/docs/storage engine/old probe.sh b/docs/storage engine/new probe.sh`
- **WHEN** the wrapper evaluates prompt content
- **THEN** both census sides count as covered, `prompt-content:` reads `PASS (2/2 code census paths present)`, and the match is decided per header by the canonical matcher rather than by any regex

#### Scenario: A MIXED-quoted rename header, where only one side needs quoting, is matched
- **GIVEN** a rename from an ASCII name to a non-ASCII one, for which git emits `diff --git a/<ascii> "b/<quoted>"`
- **WHEN** the wrapper evaluates prompt content
- **THEN** both census sides count as covered and `prompt-content:` reads `PASS (2/2 code census paths present)` — a shape that occurs only on renames SHALL NOT be structurally unreachable

#### Scenario: A newline-bearing census path cannot be proved present by its first line
- **GIVEN** a census containing a path with a literal newline (`a<LF>b.rs`) beside a path equal to its first line (`a`), and a prompt whose only header names `a`
- **WHEN** the wrapper evaluates prompt content
- **THEN** the newline-bearing path is reported ABSENT — `prompt-content: FAIL (1/2 code census paths absent from the prompt)` — because membership is decided per header with no delimiter, never by a line-oriented pattern match that would treat the two lines as alternatives

#### Scenario: The same newline-bearing path counts as present when its header IS in the prompt
- **GIVEN** the same census and a prompt additionally carrying the C-quoted header git emits for that path
- **WHEN** the wrapper evaluates prompt content
- **THEN** it reads `PASS (2/2 code census paths present)`, so the absent verdict above is a real measurement and not a blanket "newline ⇒ absent" rule

#### Scenario: The boundary is pinned structurally, so a new consumer cannot re-scatter it
- **GIVEN** the hermetic regression check
- **WHEN** a path-reading `git diff` loses its `-z`, or a second consumer calls the quoted-path decoder outside the canonical matcher
- **THEN** the check FAILs with a message naming the offending file and mechanism, so the regression is caught by the fast `--lite` loop rather than by a review round

### Requirement: A recorded live probe demonstrates the narrowed exclusion, POST-MERGE, on a real harness PR
The change SHALL be demonstrated by a **recorded live run** — run, not asserted — of the sanctioned wrapper
against a diff of the shape that failed: executable harness files under `docs/reports/*-artifacts/`.

**THE DEMONSTRATION IS NECESSARILY POST-MERGE, AND THE REQUIREMENT SHALL SAY WHY.** roborev's daemon binds
a repository by its `repos.root_path` and resolves `exclude_patterns` from the **ROOT checkout**, and it
**snapshots that config at daemon start**. Therefore the narrowed set CANNOT apply to this change's own
review: while the change is unmerged the root checkout still carries the blanket `['docs/**', '*.md']`. A
committed **executable under root `docs/`** — the original self-demonstrating specimen — is consequently
dropped from the review of its own change, so `prompt-content:` would FAIL **correctly** and permanently
until merge (the reviewer really did not receive the file). A pre-merge
self-demonstration is therefore a **deadlock, not a test**: the specimen that proves the fix is the
specimen the unfixed configuration eats. The executable SHALL NOT be committed under root `docs/`; the
requirement is **rescheduled, not dropped**, and the reason SHALL be recorded rather than the requirement
quietly weakened.

**THE PRIMARY EVIDENCE SHALL BE A REAL PR, NOT A SYNTHETIC PROBE.** The first post-merge pull request that
happens to carry an executable under `docs/` demonstrates this end to end at no extra cost, and is
**strictly better** evidence than a probe written to pass, because it proves the fix on a diff **nobody
shaped for it**. AC2's record SHALL therefore be that PR's `census:` + `code-free:` +
`prompt-content:` evidence posted to the issue; the committed probe **procedure** is the documented
**FALLBACK**, for when no such PR arrives promptly or its evidence is ambiguous.

**THE OBLIGATION SHALL CARRY A NAMED TRIGGER**, because an unowned post-merge obligation is not an
obligation: (a) on merge the issue SHALL move to **`In Review`, NOT `Done`** — `Done` auto-closes it and
the obligation would vanish with it; (b) the PR SHALL be finalized and delivery telemetry stamped
regardless, neither waiting on the demonstration; (c) the issue SHALL flip to `Done` ONLY once the AC2
evidence is posted; (d) if the demonstration has not happened within a few days it SHALL be **filed as a
tracked issue**, never left to live in a comment thread.

The recorded evidence SHALL carry: the `census:` counts, the `code-free:` line, the `prompt-content:` line,
and the input / cached / output token counts from the job record. Its PASS
condition SHALL be `code-free: PASS` TOGETHER WITH
`prompt-content: PASS (<n>/<n> code census paths present)` and a genuine token signature — the first says
the wrapper's own census classified the executables as CODE, the second says the reviewer actually received
them, the third says a real review happened, and no one of the three alone suffices. `prompt-content:`
carries the whole weight of the exclusion question: it is measured AFTER the review round, from the prompt
the reviewer was actually given, and nothing predicts the exclusion set before the enqueue (deferred to
issue #3283).

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
CONFIRMATION of the disassembly-derived reading of roborev's own pathspec construction: because a pattern
with an interior `/` is root-anchored, that nested path SHALL still be DELIVERED to the reviewer. Its
absence from the prompt would falsify that reading — on which the SHAPE of the committed deny-list rests —
and SHALL be treated as a blocking finding. That file SHALL be
committed on this branch, because — unlike an executable under root `docs/` — it survives under BOTH the
old and the new configuration and therefore does not deadlock.

Because the demonstration needs the network and a live reviewer, it SHALL be documented and recorded
rather than executed by the agent gate.

#### Scenario: The recorded evidence shows the code census present and a genuine token signature
- **GIVEN** the narrowed exclusion configuration in effect on the ROOT checkout, and a branch whose diff carries executables under `docs/reports/*-artifacts/`
- **WHEN** the sanctioned wrapper is run against it and the result recorded on the issue
- **THEN** the record shows `code-free: PASS`, `prompt-content: PASS (<n>/<n> code census paths present)`, and a token triple judged against the wrapper's own floors (input at or above 25,000, cached greater than zero, output advisory) rather than against a memorised large-diff band

#### Scenario: The reason the demonstration cannot be pre-merge is recorded, not the requirement weakened
- **WHEN** the change is inspected for AC2
- **THEN** it records that roborev reads `exclude_patterns` from the repo root path and snapshots it at daemon start, that a committed executable under root `docs/` is therefore dropped from the review of its own change so `prompt-content:` FAILs correctly until merge, and that the demonstration is consequently rescheduled to post-merge — and it carries no executable under `docs/reports/3229-artifacts/`

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
- **THEN** that nested path IS present in the prompt — confirming live that a pattern containing an interior `/` is root-anchored, as the disassembly of roborev's own `git.FormatExcludeArgs` established — and its absence would instead falsify the reading the committed deny-list's shape rests on and block the change rather than being recorded as an acceptable outcome

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
correspondence is NOT predicted anywhere pre-enqueue (an oracle that did was built here and REMOVED —
deferred to issue #3283). A broken correspondence therefore surfaces HERE, after the review round, as a
`prompt-content:` FAIL naming the paths the reviewer never received — which is why a FAIL of this key
means "suspect `.roborev.toml` first".

**EVERY code path SHALL be checked** — there SHALL be NO sampling cap. A sampled subset was a hole: a
partial prompt naming just the sampled files passed. Matching SHALL be against the prompt's actual
`diff --git` HEADER paths, never a bare substring (a substring is satisfied by any incidental mention,
including this wrapper quoting a path in its own comments), and the header path set SHALL be collected
from **BOTH sides** of each header and compared WHOLE-LINE: the census runs `--no-renames` (a rename is
two paths) while the reviewer's diff may have rename detection ON (one `a/old b/new` header), so
same-path-only matching FALSELY REJECTED every review containing a detected rename. Collecting both sides
reconciles the two rename behaviours WITHOUT weakening exact-header strictness to a substring test.

**PATHS SHALL BE COMPARED IN THE NORMALISED (RAW) FORM ESTABLISHED AT THE CENSUS, AND EVERY HEADER SHAPE
GIT EMITS SHALL BE RECOGNISED (#3229).** This key SHALL perform NO normalisation of its own: census paths
reach it RAW (the census reads `git diff --numstat -z`), and membership SHALL be decided **per `diff --git`
header, by the single canonical matcher** specified under *File paths are normalised ONCE, at the census* —
which recognises the raw, SPACE-bearing, C-quoted and MIXED-quoted shapes. This key SHALL NOT build a path
SET, apply a header regex, or perform delimiter-based membership: a `[^ ]+` regex cannot split a
space-bearing header, a both-sides-quoted parse cannot read a rename's mixed header, and a
newline-delimited set makes a newline-bearing path's first line "prove" its presence. Accepting only
`^diff --git a/[^ ]+ b/[^ ]+$`, and comparing a
C-quoted census path against unquoted captures, FALSE-FAILED both shapes (MEASURED: a census whose two
code paths were both OUTSIDE the configured exclusion set, and both present in the prompt, nevertheless
reported `prompt-content: FAIL (1/2 absent)`, `RESULT: FAIL`). That direction is the DANGEROUS one for this key
specifically: it is the wrapper's strongest deterministic anti-vacuity signal, so a key that reds on
correct input is the key agents learn to waive. Reachability is not theoretical — the repository already
tracks 40 space-bearing paths under `docs/`, including the directory `docs/storage engine/`, and this
change promotes `docs/reports/*-artifacts/**` executables to CODE census paths.

**A `0/0` SHALL NEVER BE A PASS.** When no code census path is left to look for — every one of them
dropped from the diff roborev builds — this key has no subject and SHALL NOT report PASS; it SHALL FAIL,
naming the reason. `PASS (0/0 code census paths present)` is textually indistinguishable from a genuine
pass while the reviewer received an EMPTY prompt, which is precisely the vacuity this capability exists to
prevent. This is belt-and-braces behind the pre-enqueue `code-free:` FAIL: the condition is unreachable through the normal
flow, and SHALL remain refused here anyway so that a change to the upstream ordering cannot silently
restore a vacuous PASS.

The value set SHALL be exactly:

- `PASS (<n>/<n> code census paths present)` — every code path found. There SHALL be NO "not expected"
  suffix and NO subtraction: no key is licensed to tell this one which census code paths to skip, so a path
  the reviewer did not receive FAILs (see the residual named under *DEFERRED Requirements*);
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
- **GIVEN** a census whose code paths include a filename with spaces and a literal double quote, and a prompt carrying that path in an UNQUOTED header (a producer that is not git)
- **WHEN** the wrapper evaluates prompt content
- **THEN** the canonical matcher recognises the path positionally in that header, `prompt-content:` reads `PASS (2/2 code census paths present)`, and the terminal `RESULT:` is `PASS` — the verdict itself is asserted, never one intermediate key alone

#### Scenario: The same path in the header shape git REALLY emits for a quote is matched
- **GIVEN** the same census path and the header git actually writes for it, with the whole side C-quoted and the inner quotes ESCAPED (`diff --git "a/…odd \"q\" name.sh" "b/…"`)
- **WHEN** the wrapper evaluates prompt content
- **THEN** the escaped-quote spelling decodes to the census's raw bytes and counts as present, so the raw and quoted readings are both pinned rather than one being assumed to follow from the other

#### Scenario: A space-bearing directory in a code path is matched positionally
- **GIVEN** a code census path under a directory containing a space (the repository tracks `docs/storage engine/`), whose diff header is therefore `diff --git a/docs/storage engine/probe.sh b/docs/storage engine/probe.sh`
- **WHEN** the wrapper evaluates prompt content
- **THEN** the path counts as present, `prompt-content:` reads `PASS`, and the ambiguity is resolved by testing the positions the path could occupy in that header — never by relaxing the match to a substring

#### Scenario: A non-ASCII code path is matched through the C-quoted header shape
- **GIVEN** a code census path with a non-ASCII name, which the census records RAW and the prompt carries as `diff --git "a/docs/reports/x-artifacts/\303\251.sh" "b/…"`
- **WHEN** the wrapper evaluates prompt content
- **THEN** the canonical matcher decodes the quoted header to the same raw bytes, they compare equal, `prompt-content:` reads `PASS`, and no octal-escaped path is reported absent

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
amount of re-running or re-prompting will change the outcome. This key measures the census ONLY; it does
NOT predict which of those paths the configured exclusion set will remove (an oracle that did was built
here and REMOVED — deferred to issue #3283). A configured pattern that would swallow CODE therefore fails
AFTER the review round, under `prompt-content:`, rather than before the enqueue.

Classification SHALL be by file EXTENSION against a declared prose-extension set, plus the INTERSECTION of
a declared ARTIFACT-extension set and a declared set of ARTIFACT-BEARING DIRECTORY GLOBS, mirroring the
configuration's `<artifact-dir-glob>/**/*.<ext>` exclusions (raw run output and binary/image blobs
committed inside a directory whose purpose is committed run output), with a path assist limited to
EXTENSIONLESS files under the declared prose directories. An artifact EXTENSION alone SHALL NOT make a file
non-code: a `.json` outside those directories — notably `docs/observability/**` — is functional
configuration and SHALL count as CODE. The directory-glob match SHALL follow git `:(glob)` component
semantics (`*` matches within one path component, `**` matches zero or more components), so a shell-style
match whose `*` crosses `/` (which would classify `docs/reports/a/b-artifacts/x.json` as an artifact) is
FORBIDDEN, and the declared globs SHALL be held in a form that cannot be PATHNAME-EXPANDED against the
current directory (they contain `*`; an unquoted string iteration silently reduces them to the directories
that happen to exist in the checkout).

**THE MIRROR IS ONE FACT IN TWO REPRESENTATIONS, MAINTAINED BY HAND, AND THAT SHALL BE DECLARED AT THE
CODE.** The classification constants and `.roborev.toml`'s `exclude_patterns` are the SAME FACT WRITTEN
TWICE, and a one-sided edit is the standing hazard: it surfaces as a `prompt-content:` FAIL on an unrelated
report PR, a whole review round away from its cause. Both representations SHALL therefore be edited
TOGETHER, and each SHALL carry a comment saying so and naming its twin.

**There is NO automated drift assert, and that SHALL be recorded as a KNOWN GAP rather than left to be
discovered.** One existed briefly — it re-derived the expected pattern set from the constants and asserted
set equality against the committed `.roborev.toml` — and it was REMOVED with the exclusion-modelling
subsystem it read the file through (a bash TOML parser over three config sources), because that subsystem
produced false-PASSes faster than review rounds could close them. Closing the gap with a guard whose own
correctness is establishable is deferred to issue **#3283**. Until then drift surfaces the slow way, under
`prompt-content:`, and the declaration SHALL name that path so the FAIL is diagnosable.

A file with an executable/config-as-code extension anywhere in the tree —
including `docs/foo.py`, `docs/reports/*-artifacts/**/*.sh`, `*.bt` and `.github/workflows/*.yml` — SHALL
count as CODE, so neither the check nor the configuration may treat a program as documentation merely
because it lives under `docs/`. `code-free:` SHALL NEVER be satisfied by the presence of a directory
prefix alone.

This requirement is deliberately STRONGER than a prose-matched detection: an earlier revision computed
the same classification and used it only for attribution wording, which let a docs-only diff reach
`RESULT: PASS` whenever the reviewer's verdict happened not to carry the vacuity phrase.

#### Scenario: The census/configuration mirror is declared, with its missing assert named as a gap
- **GIVEN** the declared artifact-extension set, the declared artifact-directory globs and the committed `.roborev.toml`
- **WHEN** the two representations are inspected
- **THEN** they agree exactly over a NON-EMPTY set, the configured set carries neither a blanket `docs/**` nor any `docs/**/*.<ext>` sweep, each side carries a comment naming its twin and requiring a single joint edit, and the ABSENCE of an automated drift assert is recorded at the code as a known gap deferred to #3283 — together with the path a one-sided edit takes instead (a `prompt-content:` FAIL on an unrelated report PR)

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
`job-record:`, `sha-assert:`, `review-completed:`, `prompt-content:`,
`vacuity-tier1:`, `vacuity-tier2:`, `findings:`, `roborev-exit:`, `log:`, and a terminal
`RESULT: PASS|FAIL|NOTHING-TO-REVIEW` — **TWENTY-TWO keys in all**, counting the terminal `RESULT:`. Each
SHALL appear EXACTLY ONCE, and `code-free:` SHALL sit immediately after `census-check:`, mirroring its
pre-enqueue evaluation order.
`reviewed-sha:` SHALL carry the reviewed RANGE `<base40>..<head40>` on a normal run (a single sha only
when the record reports one, and `-` when it is unverifiable), so a reader SHALL NOT expect a bare sha
there.

Every per-check key SHALL participate in ONE verdict scan in which a value whose VERDICT TOKEN is `FAIL`,
`FINDINGS`, `ERROR` or `INCONSISTENT` fails the run, and `PASS`, `SKIP`, `UNAVAILABLE`, `NOTICE` and
`DEGRADED` never do. `DEGRADED` is non-failing BY DESIGN and only ever appears on `job-record:`, whose consequences
are published by the dependent asserts under their own keys. A per-check key whose
step was never reached SHALL carry an explicit `SKIP` rather than a blank, so an unreached check can
never read as a pass.

**THE VERDICT GRAMMAR SHALL BE CLOSED, AND THE NON-FAILING SET SHALL BE AN ALLOW-LIST.** Testing only the
FAILING prefixes and letting everything else fall through to the pass is the same defect shape as the three
above, at the wrapper's single most consequential decision point: a value nobody planned — an EMPTY string
because a check aborted before assigning, a state a future check introduces, a typo — would inherit the
non-failing branch and reach `RESULT: PASS`. A value matching NEITHER the failing set NOR the documented
non-failing set (`PASS`, `SKIP`, `NOTICE`, `UNAVAILABLE`, `DEGRADED`, and `findings:`'s own `NONE`,
`PRESENT`, `UNKNOWN`) SHALL therefore be an UNRECOGNISED VERDICT that FAILS the run and NAMES itself and
the reason. The failing-token scan SHALL be preserved as its own statement so the structural
assert pinning `NOTICE` outside the failing set keeps reading the statement it was written against.

**BOTH THE GRAMMAR SCAN AND THE AFFIRMATION BACKSTOP SHALL MATCH ON THE VERDICT TOKEN — the value up to
its FIRST SPACE — COMPARED EXACTLY, NEVER AS A PREFIX GLOB.** Every documented value is either a bare
token (`PASS`, `SKIP`, `UNAVAILABLE`) or `TOKEN (detail…)`, so the token is well defined for all of them,
and anything ELSE glued to a token is UNRECOGNISED and fails closed. A `PASS*` prefix glob would accept
`PASSthisNeverRan` and `PASS-MEASUREMENT-DID-NOT-HAPPEN` as affirmative passes: the closure would then be
checking a SPELLING rather than a STATE, and the backstop against unmeasured keys would itself be
satisfiable by a value that measured nothing. Exact-token matching is strictly STRONGER in BOTH arms — a
`FAILED (…)` variant no longer matches the failing arm by prefix either; it lands in the unrecognised arm,
which also fails — so nothing becomes permissive by tightening.

**AND A PASS SHALL REQUIRE EVERY VERDICT-CARRYING KEY TO HAVE AFFIRMATIVELY PASSED.** The **six**
deterministic keys — `push-assert:`, `census-check:`, `code-free:`, `sha-assert:`,
`review-completed:`, `prompt-content:` — SHALL each read `PASS` on a passing run, with **NO exemption for
any key** and no exemption mechanism. (One existed briefly, for a key allowed a `NOTICE` because a
remedy-less swallow was a measurement with a stated residual; that key and its exemption are both gone —
#3283/#3278 — leaving the backstop UNIFORM, which is stricter, never weaker.) `vacuity-tier1:`,
`vacuity-tier2:` and `findings:` are deliberately EXCLUDED, being corroborators with documented non-`PASS`
values. This closes the case NEIGHBOURING the grammar check: a value that is IN the grammar and
non-failing but is not a MEASUREMENT — `SKIP` above all, which means the check NEVER RAN. Validating that
the sourced checks file DEFINES its five functions proves they exist, NOT that each reached its
assignment; a check that returns early leaves its key at the initial `SKIP`, and the run then passed with a
key that measured nothing — textually identical to a genuine pass. The backstop SHALL be evaluated only on
a run that would otherwise PASS, so an already-failing run's actionable cause is not buried under a
structural one, and its message SHALL say that the cause is a defect in the wrapper rather than in the
branch under review.

#### Scenario: An unrecognised verdict value fails the run instead of inheriting the pass
- **GIVEN** a run in which one per-check key holds a value outside the documented grammar (the observable signature of a check that aborted before assigning, or that introduced a new state)
- **WHEN** the verdict scan runs
- **THEN** the run FAILs, the offending value is named under its own diagnostic, and the value is still emitted in the block rather than being silently normalised
- **AND** the hermetic suite proves this on a PATCHED COPY of the flow scripts, having FIRST shown the UNPATCHED copy reaching `PASS` on the same fixture and verified that the patch really changed the file — otherwise a copy that failed because it was copied wrong would satisfy the assert

#### Scenario: A check that never ran cannot ride to PASS on its initial SKIP
- **GIVEN** a run in which a verdict-carrying check returns before assigning its key, leaving the initial `SKIP`, and in which no other key fails
- **WHEN** the verdict is computed
- **THEN** the run FAILs, naming the key and its non-affirmative value, stating that a non-failing value which is not a measurement is the vacuous pass itself, and directing the reader at the wrapper rather than at the branch under review

#### Scenario: A value that merely BEGINS with a recognised token is unrecognised, not a pass
- **GIVEN** a run in which one verdict-carrying key holds a NEAR-PREFIX value — `PASSthisNeverRan` (a token glued to more characters with no separator) or `PASS-MEASUREMENT-DID-NOT-HAPPEN` (a token followed by a hyphenated state name) — and in which no other key fails
- **WHEN** the verdict scan and the affirmation backstop run
- **THEN** the run FAILs in BOTH arms because the verdict TOKEN (the value up to its first space) is compared EXACTLY rather than as a `PASS*` glob, the offending value is NAMED, and it is still EMITTED in the block rather than normalised away — so the closure cannot be satisfied by a spelling that measured nothing

The block's name SHALL be distinct from the agent gate's summary block names so
neither can be pasted as the other. The wrapper SHALL exit non-zero on any outcome other than PASS, and
SHALL be usable such that a caller retains ONLY this block and never the raw review transcript (which
SHALL be written to the log path named in the block's `log:` field). An unexpected mid-run abort SHALL
still emit the block with `RESULT: FAIL` rather than terminate silently.

**NO PATH SHALL REACH A SUMMARY VALUE UN-NEUTRALISED.** The block is LINE-ORIENTED and safety-critical:
every reader retains only the block and greps it by `^<key>: ` / `^RESULT: ` to decide whether a merge
proceeds. Diff-derived text reaches those values — `prompt-content:` names each code census path ABSENT
from the prompt, and the accompanying detail lines name those paths — and a census path is
**ATTACKER-CONTROLLED**: it is whatever a pull request chose to track. Every value the block emits, **and every detail line printed alongside it**,
SHALL therefore be neutralised so that a value can never span lines nor introduce a `key:` at line start:
control characters SHALL be rendered as visible escapes (or the path C-quoted). Quotes, backslashes and
spaces MAY be left intact, since the block names paths by their real bytes and no non-control
byte can start a line.

The neutralisation SHALL be enforced at the **single emit boundary**, not per interpolation site — a
per-site escape is a list to keep complete, and the next value that grows a path interpolation would
silently reopen the hole — and that boundary SHALL be asserted **structurally**, so a value emitted by any
other route FAILs the fast loop. The rendering is NOT required to be reversible; the guarantee is exactly
"no value spans a line and no `key:` can be introduced", and this residual SHALL be declared rather than
implied.

#### Scenario: A filename cannot forge a summary key or the verdict
- **GIVEN** a census path whose FILENAME carries newlines followed by a `RESULT: PASS` line and a `prompt-content: PASS` line, ABSENT from the prompt the reviewer received so that it is named in the `prompt-content:` value and in the detail lines
- **WHEN** the block is emitted
- **THEN** the output carries EXACTLY ONE `RESULT:` line (the wrapper's real `RESULT: FAIL`), no `RESULT: PASS` and no forged `prompt-content: PASS` anywhere, and the missing path is still NAMED — on one line, with its newlines shown as visible escapes — so neutralising never costs the operator the diagnosis

#### Scenario: The neutralisation boundary is pinned structurally
- **GIVEN** the hermetic regression check
- **WHEN** a block value is emitted by any route that bypasses the neutralising boundary, or the detail lines are bulk-printed again
- **THEN** the check FAILs naming the offending emit, so a future key that interpolates a path cannot silently reopen the injection

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
- **THEN** the block carries `repo:`, `branch:`, `base:`, `head-sha:`, `reviewed-sha:`, `job:`, `model:`, `census:`, `tokens:`, `push-assert:`, `census-check:`, `code-free:`, `job-record:`, `sha-assert:`, `review-completed:`, `prompt-content:`, `vacuity-tier1:`, `vacuity-tier2:`, `findings:`, `roborev-exit:` and `log:` in that order, ahead of the terminal `RESULT:` — twenty-two keys in all, each exactly once

#### Scenario: An unreached check reads SKIP, never blank
- **GIVEN** a run that fails at `push-assert:` before the census is classified
- **WHEN** the block is emitted
- **THEN** `code-free:` carries an explicit `SKIP (<cause>)` rather than a blank value, so an unreached check can never read as a pass — and, because the affirmation backstop admits only an exact `PASS`, that `SKIP` could not have ridden to a verdict either

#### Scenario: One scan over the per-check keys computes the verdict
- **GIVEN** a block in which exactly one per-check key carries a value whose verdict token is `FAIL`, `FINDINGS`, `ERROR` or `INCONSISTENT` while every other reads `PASS`, `SKIP`, `UNAVAILABLE`, `NOTICE` or `DEGRADED`
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

The check SHALL additionally pin the DOCS-CENSUS CLASSIFICATION, which is the half of the narrowing that
the wrapper itself decides: (p) a fixture diff of EXECUTABLES under `docs/reports/*-artifacts/`
(`.py`, `.sh`, `.bt`) yields `code-free: PASS` and IS enqueued; (q) a PROSE-ONLY diff under `docs/` still
yields `code-free: FAIL` with NO review enqueued, so the narrowing did not invert the guard; and (r) a
census path containing SPACES and a literal double quote is compared correctly (the NUL-safety regression,
which a non-`-z` comparison would silently mis-handle as a false PASS). It SHALL NOT pin any PREDICTION of
roborev's effective exclusion set: the cases that did — a configured-swallow FAIL, the unparseable/absent
exclusion-set forms, exclusion-set drift, the binary-corroboration states, the ported pathspec
construction, the three-config-source union, the trailing-slash inversion and the built-in lockfile
residual — are REMOVED with the oracle they exercised (deferred to issue #3283), and the fixture helper
consequently no longer writes a `.roborev.toml` into a fixture nor stubs `roborev config get`, because
nothing reads either one. The REMOVAL ITSELF SHALL be pinned structurally, since a half-deletion is its own
failure mode: the suite SHALL assert that the deleted key is absent from the verdict-scan key list (it
would otherwise hold a permanently EMPTY value that the closed grammar reds on every run), that the summary
block no longer emits it — so the removal is visible in the OUTPUT contract and not merely in the source —
and that each deleted function has NO live reference left in any of the three flow scripts.

The check SHALL additionally pin the DECLARED RESIDUAL and the header-shape normalisation:
(s) a #3096-shaped census (`docs/reports/ws0-3096-artifacts/*.json` + a `Cargo.lock` change + a `.rs` file)
against a prompt carrying only the `.rs` file yields
`prompt-content: FAIL (1/2 …)` naming `Cargo.lock`, with `RESULT: FAIL` and no "not expected" clause
anywhere; (t) the SAME census against a prompt that DOES carry the lockfile yields
`prompt-content: PASS (2/2 …)` and `RESULT: PASS`, so (s)'s FAIL is attributable to the prompt's contents
and to nothing else — the both-directions control without which a declared residual is indistinguishable
from an unnoticed one; (u) `prompt-content:` refuses to report a pass when no census path is checkable, driven
DIRECTLY against the function so the assertion survives the upstream pre-enqueue FAIL that makes the state
unreachable through the wrapper; and (v) a code census path containing SPACES, one under a space-bearing
DIRECTORY, and one with a NON-ASCII (octal-escaped) name each yield `prompt-content: PASS` and
`RESULT: PASS`.

The check SHALL additionally pin the CLOSED VERDICT GRAMMAR and the affirmation backstop, which are
properties of the wrapper's own decision point rather than of any fixture: (w) a per-check key holding a
value outside the documented grammar FAILs the run and is named; (x) a verdict-carrying check that
returns before assigning its key FAILs the run rather than passing on its initial `SKIP`; and (y) the two
NEAR-PREFIX mutants — `PASSthisNeverRan` and `PASS-MEASUREMENT-DID-NOT-HAPPEN` — are UNRECOGNISED and FAIL
in both arms, so neither the grammar scan nor the backstop can be satisfied by a value that merely BEGINS
with a recognised token. Because none of these states is reachable through a fixture, all
SHALL be exercised against a PATCHED COPY of the three flow
scripts, and the copy SHALL be shown to reach `PASS` UNPATCHED on the same fixture — with the patch
verified to have really changed the file — before any assertion is believed: an assert that a copy FAILs
is otherwise satisfied by a copy that failed because it was copied wrong, which is a probe failing in the
direction that looks like success. They SHALL ALSO be pinned STRUCTURALLY against the scan statement (that
the positive arm exists, that its fallback sets the failure flag, that both arms match the verdict TOKEN
exactly rather than by prefix, and that the backstop names all SIX
deterministic keys with no exemption), because a behavioural case cannot see a future edit that deletes the arm for a key it
does not exercise.

The suite SHALL report its own pass/fail tally, which at this change's completion stands at **477**
assertions passed and 0 failed.

**Every hostile-path or hostile-verdict case SHALL assert the terminal `RESULT:` and, where the path
reaches the reviewer, `prompt-content:` — not one intermediate key alone.** A case that asserted only an
intermediate pre-enqueue key reported two passes while the SAME fixture false-FAILed `prompt-content:` and the run
terminated `RESULT: FAIL`: a case that passes while the behaviour it names is broken is worse than no case,
because it is read as coverage. The suite's stub SHALL emit a VALID JSON job record for a prompt containing
double quotes, so a quote-bearing prompt cannot degrade the record and mask the very comparison the case
exists to pin.

The check SHALL also pin the block's key ORDER — all twenty-two keys, each appearing EXACTLY ONCE, with
`code-free:` immediately after `census-check:` — the distinctness of its header from all three
agent-gate summary headers, the usage-error path emitting no block, and hermeticity itself. It SHALL be
registered in the agent gate's shell-tooling component set such that it runs in the fast `--lite` loop
as well as the full gate, so a regression FAILs the fast loop rather than costing a review round. The
check SHALL contain no wall-clock threshold assertion in its correctness path, and SHALL report a loud
SKIP rather than a silent pass when an optional prerequisite for a subset of cases is unavailable.

#### Scenario: Every trigger class is asserted against the block's own keys
- **WHEN** the regression check runs
- **THEN** it asserts each of the classes (a) through (y) above against the wrapper's terminal `RESULT`, its per-check key values and its exit code, and it reports an explicit pass/fail tally (477 passed, 0 failed) so a partial run cannot read as a pass

#### Scenario: The total-swallow and partial-swallow cases are both pinned
- **GIVEN** two hermetic fixtures under the narrowed configuration — one whose census is a `Cargo.lock` bump beside a prose edit, one whose census is the same lockfile beside a `.rs` file
- **WHEN** the regression check runs the wrapper against each
- **THEN** the first reports `prompt-content: FAIL` naming `Cargo.lock` with `RESULT: FAIL`, the second reports `prompt-content: PASS (2/2 …)` with `RESULT: PASS` and IS enqueued, and neither can drift into the other without failing the fast loop

#### Scenario: A hostile-path case asserts the verdict, not one intermediate key
- **WHEN** the suite's hostile-path cases (spaces, a literal quote, a space-bearing directory, a non-ASCII name) are inspected
- **THEN** each asserts the terminal `RESULT:` alongside the `prompt-content:` value rather than an intermediate key alone, and the stub emits a VALID JSON record for a quote-bearing prompt so the record cannot degrade and mask the comparison

#### Scenario: The zero-subject refusal is driven directly against the check
- **GIVEN** that the pre-enqueue `code-free:` FAIL makes a zero-subject `prompt-content:` unreachable through the wrapper
- **WHEN** the regression check exercises the check function directly, in the real files, with an EMPTY code census
- **THEN** it asserts `FAIL (no code census path was checkable — a 0/0 is never a pass)` and asserts the ABSENCE of any `PASS (0/0` form, so removing the upstream FAIL cannot silently restore the vacuous pass

#### Scenario: Executables under a docs artifact directory are enqueued, prose under docs is not
- **GIVEN** two hermetic fixtures — one whose diff is `.py`/`.sh`/`.bt` files under `docs/reports/x-artifacts/`, one whose diff is only markdown under `docs/`
- **WHEN** the regression check runs the wrapper against each
- **THEN** the first reports `code-free: PASS` and IS enqueued, while the second reports `code-free: FAIL` and is asserted never enqueued

#### Scenario: The suite neither configures nor stubs an exclusion prediction
- **WHEN** the regression suite is inspected after the oracle's removal
- **THEN** no fixture writes a `.roborev.toml`, no stub answers `roborev config get`, and no case asserts a predicted exclusion set — because nothing in the wrapper reads any of them, and a fixture pinning a behaviour no code has is read as coverage while covering nothing

#### Scenario: The deletion is pinned so a half-removal cannot ship
- **WHEN** the regression suite's structural asserts run against the three flow scripts
- **THEN** the deleted key is absent from the verdict-scan key list, the summary block emits no such key, and every deleted function has no live reference — so a partial deletion (a key left in the scan holding a permanently empty value, which the closed grammar would red on every run) FAILs the fast loop instead of the field

#### Scenario: The near-prefix mutants are pinned as cases, not left to the grammar's wording
- **WHEN** the suite's verdict-grammar cases are inspected
- **THEN** they include the two near-prefix mutants (`PASSthisNeverRan` and `PASS-MEASUREMENT-DID-NOT-HAPPEN`), each asserted to FAIL the run, be NAMED, and still appear in the block, and a structural assert additionally pins that both arms reduce a value to its verdict token before comparing rather than matching a `PASS*` glob

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
   covers the CODE subset of the census, and that the deterministic pre-enqueue `code-free:` FAIL is the
   correct response to a code-free census. Doctrine SHALL FURTHER state that **NOTHING predicts roborev's
   effective exclusion set pre-enqueue** — the oracle that did was built under #3229 and removed, deferred
   to **#3283**, with the built-in (unconfigured) patterns deferred to **#3278** — and that a
   `prompt-content:` FAIL therefore means **"suspect `.roborev.toml` first"**: the reviewer did not receive
   a path the census called code, and a configured pattern is the likeliest reason.

**Doctrine SHALL NOT imply that everything under `docs/` is code-free.** Every surface stating the
docs-only rule SHALL be amended in this same change to (a) name the `docs/reports/*-artifacts/` harness
convention EXPLICITLY as executable code that IS reviewed and that a PR carrying it is NOT a docs-only
change, (b) state that "docs-only" means a code-free CENSUS as the wrapper classifies it, never a
directory prefix, and (c) name `prompt-content:` as the key that FAILs — after the review round, since
nothing predicts the exclusion set before it — when a configured pattern swallows census code, so its FAIL
reads as "suspect `.roborev.toml` first". The surfaces SHALL include, beyond the two doctrine surfaces
(`CLAUDE.md` and the website `agents-developing/roborev-findings/` page):
`website/.../agents-developing/delivery-pipeline.md`, `.claude/agents/flow-lead.md`,
`.claude/agents/flow-closer.md`, `.claude/skills/flow-implement/SKILL.md`, and the header comments of all
three `scripts/flow/roborev-review*.sh` files — including the `roborev_check_prompt_content()` comment
that states the falsified claim outright. A surface left un-amended is doctrine drift against itself, and
this requirement is not satisfied while any copy still asserts the falsified mechanism.

Where doctrine documents the summary block it SHALL carry the `job-record:` key, NO `census-exclusion:`
key, the exact-token verdict grammar (the value up to its first space, matched exactly) with its
SIX-key affirmation backstop and no per-key exemption, and the corrected `prompt-content:` values
(an unretrievable prompt FAILS; there is no non-failing `UNAVAILABLE` for that key). Where doctrine
documents the live probe it SHALL state the expectation in the RANGE form — the `reviewed-sha:` range's
HEAD endpoint equals the worktree HEAD and its base equals the base ref — never as `reviewed-sha`
equalling the worktree HEAD.

#### Scenario: Doctrine states the verdict rule verbatim, as one rule
- **WHEN** CLAUDE.md, `website/src/content/docs/agents-developing/roborev-findings.md` and this change's `design.md` are inspected
- **THEN** each carries the sentence "FAIL where the author can act; NOTICE where only the information is actionable; never silence." verbatim, and each presents it as ONE rule rather than as independent judgements a future editor would have to re-derive — and each records that the affirmation backstop grants NO `NOTICE` exemption to ANY of its six keys, the single exemption that briefly existed having been removed along with the key it was written for (deferred to #3283, its remedy-less residual to #3278)

#### Scenario: Doctrine records the three config-ordering properties and their generalization
- **WHEN** CLAUDE.md and `roborev-findings.md` are inspected beside the existing note that `required` evaluates the aggregator and registry from the PR's BASE ref
- **THEN** both state that roborev's daemon reads `exclude_patterns` from the repo ROOT PATH so a worktree edit is invisible to it, that the daemon snapshots config at start so an edit needs a restart, that BOTH have already cost real rounds, and that the generalization is "any PR whose subject is a config the daemon (or a gate) reads from root cannot certify itself" — explicitly noted as the same shape as the BASE-ref property

#### Scenario: Doctrine records that the PRE-EXISTING guard caught the NEW guard
- **WHEN** the defence-in-depth rationale in `roborev-findings.md` and `design.md` is inspected
- **THEN** it records that `prompt-content:` — the older check — caught the then-new `census-exclusion:` oracle (since REMOVED, deferred to #3283) certifying a config roborev never read, and states this as the change's strongest argument for keeping the measured layer, explicitly because it paid out in the direction nobody plans for: the NEW layer was the wrong one, and it is the layer that went

#### Scenario: Doctrine records that a test blessing a vacuous verdict is worse than an unguarded path
- **WHEN** the doctrine page is inspected
- **THEN** it records that the two regression cases which locked in an un-corroborated "no exclusion patterns configured" PASS (both since deleted with the oracle they exercised) were worse than having no case at all, because such a test consumes the review budget that would otherwise have found the bug and converts "nobody checked" into "we checked and it was fine"

#### Scenario: Both doctrine surfaces carry all four rules
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
- **THEN** both name `docs/reports/*-artifacts/` measurement harnesses explicitly as executable code that IS reviewed, state that "docs-only" means a code-free census rather than a `docs/` path prefix, state that NOTHING predicts roborev's exclusion set pre-enqueue (deferred to #3283, its built-in patterns to #3278), and name `prompt-content:` as the key whose FAIL means "suspect `.roborev.toml` first"

#### Scenario: The live-probe expectation is stated in the range form
- **WHEN** the doctrine page's live worktree probe section is inspected
- **THEN** it asks the reader to confirm the `reviewed-sha:` RANGE — its HEAD endpoint equal to the worktree branch HEAD and its base equal to the base ref — rather than a `reviewed-sha` equal to the worktree HEAD, which the range value can never satisfy

#### Scenario: The mechanized-in-lite table lists the new guard
- **WHEN** the `roborev-findings` page's table of classes mechanized in `--lite` is inspected
- **THEN** it carries a row for the vacuous-review class naming the hermetic regression check and the components it runs in

#### Scenario: Publication is accepted by the served content, not a status code
- **WHEN** the published `agents-developing/roborev-findings` page is verified after deployment
- **THEN** acceptance is established by fetching the page and matching a distinctive phrase introduced by this change, and an HTTP 200 without that phrase is treated as not-yet-published rather than as done

