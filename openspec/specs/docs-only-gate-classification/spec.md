# docs-only-gate-classification Specification

## Purpose
Defines how `scripts/ci/classify-docs-only.sh` decides whether a changed set is documentation, and
therefore whether `pr-gate-core` — the compute half of the `required` branch-protection check — runs.
Created by archiving change narrow-docs-only-classifier (issue #3250).

## Requirements
### Requirement: A path under `docs/` is documentation only on an affirmative allowlist match

`scripts/ci/classify-docs-only.sh` SHALL NOT classify a path as documentation on the strength of a `docs/`
path prefix. A path under `docs/` SHALL be classified documentation ONLY when it affirmatively matches a
named allowlist layer, and SHALL otherwise force the full path (verdict `full`, exit 1). There SHALL be no
permissive default anywhere in the `docs/` classification: a positive verdict requires a positive match, so
every unmeasured, unrecognized or unassigned case inherits `full`.

Specifically, under `docs/` the classifier SHALL force the full path for **every** executable and
config-as-code extension the repository ships there — at minimum `.sh`, `.py`, `.bt`, `.c`, `.rs`, `.toml`,
`.cql`, `.yml`, `.yaml` — for **any unrecognized extension**, and for **any path with no extension at all**.

The extensionless rule SHALL be unconditional and SHALL NOT consult git's executable bit, even though the
sibling subsystem (#3229) does. Four reasons, each of which SHALL remain true of the implementation: the
classifier's input is a path list carrying no mode information; consulting a mode would require the
classifier to read a repository and two resolvable refs, destroying its purity and importing an
"unmeasurable mode" failure mode; a `chmod -x` must not be able to move a program into the documentation
class, because here the consequence is an ungated merge; and the measured cost is zero — all three tracked
extensionless files under `docs/` are mode 100755 harness binaries, and no tracked extensionless prose file
exists under `docs/`.

A changed set SHALL be classified `full` when **any** of its paths is non-documentation, independent of that
path's position in the set, and the classifier SHALL retain its existing fail-closed treatment of an
empty/ambiguous changed set. The sensitive-directory escape (`.github/`, `scripts/`, `test-data/` force the
full path regardless of extension) SHALL be retained unchanged.

#### Scenario: An executable under a report artifact directory forces the full gate
- **GIVEN** a changed set consisting of `docs/reports/ws0-3217-artifacts/harness/common.sh`
- **WHEN** the classifier runs
- **THEN** the verdict is `full` and the exit status is 1
- **AND** the same holds for `docs/reports/ws0-3026-artifacts/ws0-h2h/cas-scan.py` and for
  `docs/reports/ws0-3026-artifacts/ws0-corpus/trace-scan.bt`, each on its own

#### Scenario: A docs-hosted Cargo crate forces the full gate
- **GIVEN** a changed set of `docs/reports/ws0-3026-artifacts/ws0-cqlite/scan-harness/Cargo.toml` and
  `docs/reports/ws0-3026-artifacts/ws0-cqlite/scan-harness/src/main.rs`
- **WHEN** the classifier runs
- **THEN** the verdict is `full` and the exit status is 1
- **AND** each of the two paths alone also yields `full`, so neither the manifest nor the source depends on
  the other being present to be caught

#### Scenario: One executable hidden among many prose files is caught, in either order
- **GIVEN** a changed set of 20 `*.md` files under `docs/reports/ws0-3217-artifacts/` with
  `docs/reports/ws0-3217-artifacts/harness/emit-point.py` as the **last** entry
- **WHEN** the classifier runs
- **THEN** the verdict is `full` and the exit status is 1
- **AND** the same set with the `.py` as the **first** entry also yields `full`, so the verdict is
  order-independent — this is the real PR #3222 shape, where 34 executables sat among 154 prose paths

#### Scenario: An extensionless path under `docs/` forces the full gate without consulting its mode
- **GIVEN** a changed set of `docs/reports/ws0-3026-artifacts/ws0-results/ws0-readbw`
- **WHEN** the classifier runs
- **THEN** the verdict is `full` and the exit status is 1
- **AND** the verdict is unchanged when the same path is not executable in the checkout, and the classifier
  invokes no git command, so the result cannot depend on a repository, a ref, or a mode lookup

#### Scenario: An unrecognized extension under `docs/` fails closed
- **GIVEN** changed sets of `docs/tools/run.rb`, `docs/tools/query.jq` and `docs/tools/build.mjs`
- **WHEN** the classifier runs on each
- **THEN** every verdict is `full` and every exit status is 1
- **AND** no deny-list of executable extensions appears anywhere in the classifier, so an extension nobody
  anticipated is documentation-by-default in neither direction

#### Scenario: The sensitive-directory escape is unchanged
- **GIVEN** changed sets of `.github/README.md`, `scripts/notes.md`, `test-data/README.md` and
  `docs/ok.md` + `.github/CONTRIBUTING.md`
- **WHEN** the classifier runs on each
- **THEN** every verdict is `full`, exactly as before this change

### Requirement: Prose, images, legal text and inert report artifacts under `docs/` still short-circuit

The classifier SHALL keep returning documentation (verdict `docs-only`, exit 0) for a changed set consisting
only of prose, images, legal text, and report artifacts, so the ~14-minute correctness core does not become
the standing cost of a documentation or WS0 **report** PR that carries no code.

Under `docs/`, documentation SHALL comprise exactly three layers, disjoint by extension so their order
cannot change any answer:

1. **Prose, images and legal text at any depth** — `.md` and `.markdown`; the classifier's existing image
   allowlist `.png .jpg .jpeg .gif .svg .webp .ico`; and `LICENSE`/`LICENSE.*`/`NOTICE`/`NOTICE.*`. This
   layer is the classifier's **own** path semantics and SHALL be unchanged by this change, including its
   behaviour outside `docs/`.
2. **Inert report artifacts at any depth under `docs/`** — the inert bucket of the imported artifact
   extension set (at minimum `txt jsonl log err csv gz pdf jfr mmd tex diff`).
3. **Code-bearing report artifacts inside an artifact-bearing directory only** — see the directory-scoping
   requirement below.

The prose extension set SHALL NOT be imported from #3229. Its `CODE_FREE_EXTENSIONS`
(`md markdown mdx txt rst adoc`) applies repo-wide, and a `.txt` outside `docs/` can be a golden test
fixture whose edit genuinely changes a test outcome — so importing it as a global prose layer would open a
new gate hole in the change that closes this one. `.txt` reaches documentation status under `docs/` through
layer 2 and nowhere else.

This change SHALL move **no** tracked prose or image file from the fast path to the full path. That is a
measurable property of the tracked tree, not an aspiration.

#### Scenario: A prose, image and legal changed set short-circuits
- **GIVEN** a changed set of `docs/development/dev-cookbook.md`, `docs/img/diagram.png`, `docs/img/x.svg`,
  `README.md`, `CHANGELOG.markdown` and `LICENSE`
- **WHEN** the classifier runs
- **THEN** the verdict is `docs-only` and the exit status is 0
- **AND** every one of these assertions passed before this change too, so the fast path for prose is
  demonstrably preserved rather than re-derived

#### Scenario: A WS0 report PR carrying only prose and inert artifacts short-circuits
- **GIVEN** a changed set of `docs/reports/ws0-3217-artifacts/README.md`,
  `docs/reports/ws0-3217-artifacts/results/run.txt`, `.../results/points.jsonl`,
  `.../results/summary.csv`, `.../results/driver.log`, `.../results/driver.err`,
  `.../results/curve.png` and `.../results/profile.json`
- **WHEN** the classifier runs
- **THEN** the verdict is `docs-only` and the exit status is 0 — a genuine report PR keeps its
  seconds-long green

#### Scenario: Inert artifact extensions are documentation anywhere under `docs/`
- **GIVEN** changed sets of `docs/reports/delivery-telemetry.jsonl`,
  `docs/sstables-definitive-guide/pandoc-header.tex` and
  `docs/sstables-definitive-guide/statistics-db-annotated-dump.txt`, none of which lies inside an
  artifact-bearing directory
- **WHEN** the classifier runs on each
- **THEN** every verdict is `docs-only` and every exit status is 0
- **AND** the delivery-ledger case is the reason the inert bucket exists: a purely directory-scoped rule
  would force the ~14-minute core on every `flow-finalize` telemetry PR — one per delivery cycle — for a
  one-line append that no gate step reads

#### Scenario: A photo extension outside the imported artifact set is still documentation
- **GIVEN** a changed set of `docs/img/photo.jpg`, `docs/img/anim.gif` and `docs/img/favicon.ico`
- **WHEN** the classifier runs
- **THEN** the verdict is `docs-only` and the exit status is 0 — none of these extensions is in the imported
  artifact set, so the image layer must remain authoritative for paths under `docs/`, and a prose PR
  carrying a photo does not fail closed

#### Scenario: A prose file living next to an executable does not rescue the set
- **GIVEN** a changed set of `docs/reports/ws0-3217-artifacts/README.md` and
  `docs/reports/ws0-3217-artifacts/harness/parse-runqlat.py`
- **WHEN** the classifier runs
- **THEN** the verdict is `full` and the exit status is 1

### Requirement: Code-bearing artifact formats under `docs/` are documentation only inside an artifact-bearing directory

The classifier SHALL treat a **code-bearing** artifact format (at minimum `.json` and `.html`) as
documentation ONLY when the path lies strictly beneath one of the imported artifact-bearing directory globs,
and SHALL otherwise force the full path. Scoping such a format by extension alone across all of `docs/`
SHALL NOT be used, and this is a correctness requirement rather than a preference: an extension describes a
FORMAT, whereas a directory records an INTENT, and a code-bearing file can be functional configuration
under any path.

The two falsifying cases SHALL both force the full path:
`docs/observability/grafana/dashboards/cqlite-overview.json`, which the full agent gate's own
`kit-dashboard-drift` component guards — so the repository already treats it as correctness-bearing — and
`docs/reports/delivery-telemetry.schema.json`, the schema governing the delivery ledger.

The directory test SHALL be the imported component-wise matcher, NOT a bash `case` glob: a `case` pattern's
`*` crosses `/`, so `docs/reports/*-artifacts/*` would also match `docs/reports/a/b-artifacts/x`, which
git's `:(glob)` `*` does not. The classifier SHALL agree with the pathspec the sibling subsystem actually
configures rather than approximate it.

#### Scenario: Functional configuration under `docs/` forces the full gate
- **GIVEN** a changed set of `docs/observability/grafana/dashboards/cqlite-overview.json`
- **WHEN** the classifier runs
- **THEN** the verdict is `full` and the exit status is 1
- **AND** the same holds for `docs/reports/delivery-telemetry.schema.json` alone, and for a set pairing
  either file with any number of `*.md` files

#### Scenario: The same extension inside an artifact directory short-circuits
- **GIVEN** changed sets of `docs/reports/ws0-3217-artifacts/results/profile.json`,
  `docs/round-artifacts/soak/report.html` and
  `docs/sstables-definitive-guide/diagrams/partition-layout.svg`
- **WHEN** the classifier runs on each
- **THEN** every verdict is `docs-only` and every exit status is 0 — the fast path for genuine report
  artifacts is intact, so the directory scoping narrows the hole without widening the gate for reports

#### Scenario: The directory test does not let a glob cross a path separator
- **GIVEN** a changed set of `docs/reports/a/b-artifacts/x.json`, which a `case` glob
  `docs/reports/*-artifacts/*` would match but git's `:(glob)` `*` would not
- **WHEN** the classifier runs
- **THEN** the verdict is `full` and the exit status is 1
- **AND** `docs/reports/ws0-3217-artifacts/x.json` — a genuine single-component match — yields `docs-only`,
  so the assertion distinguishes the matcher's semantics rather than merely observing a `full`

#### Scenario: A nested artifact directory is matched where the configured glob says it is
- **GIVEN** changed sets of `docs/observability/jfr-reports/run.html` (matching the `docs/**/jfr-reports`
  glob at depth) and `docs/round-artifacts/2026-08/deep/nested/out.json`
- **WHEN** the classifier runs on each
- **THEN** every verdict is `docs-only`, because `**` matches zero or more path components and a match is
  "strictly beneath" the directory

### Requirement: The artifact declaration is imported from its single declaration and its use fails closed

The classifier SHALL obtain the docs-scoped artifact extension set, the artifact-bearing directory globs,
and the directory matcher by **importing** the single declaration in
`scripts/flow/roborev-review-oracles.sh` (`CODE_FREE_ARTIFACT_EXTENSIONS`,
`CODE_FREE_ARTIFACT_DIR_GLOBS`, `roborev_path_in_artifact_dir`), and SHALL NOT restate any of them. The
classifier's source SHALL contain no literal copy of the extension list and no literal copy of any directory
glob.

The import SHALL be resolved **relative to the classifier's own location**, never relative to the working
directory, so it is correct from any cwd and so the declaration can be mutated in a temporary tree to prove
the classifier actually follows it.

The import SHALL fail closed. When the declaring file is absent or fails to source, or when
`CODE_FREE_ARTIFACT_EXTENSIONS` is empty or `CODE_FREE_ARTIFACT_DIR_GLOBS` has no elements after sourcing,
the classifier SHALL classify **every** path under `docs/` as `full` and SHALL print a named reason. It
SHALL NOT degrade to a prose-only allowlist and continue, because that would make an infra fault produce a
*more permissive* gate.

Every extension in the imported set SHALL be assigned to exactly one bucket — **inert** (documentation at
any depth under `docs/`), **code-bearing** (documentation only inside an artifact directory), or
**answered-by-the-image-layer** (`png`, `svg`, which the classifier's own image allowlist decides first and
which this change does not alter). The buckets SHALL partition the imported set exactly: their union equals
it and they are pairwise disjoint. An imported extension assigned to no bucket SHALL classify `full` at
runtime AND SHALL fail the self-test with a greppable reason naming the extension and issue #3250. This is
the mechanical disagreement check AC5 requires, aimed at the bucketing — the only place drift remains
possible once the list itself is imported.

The coupling this import creates SHALL be recorded at the import site and honoured on upgrade. Its scope is
exact: this classifier depends on the declaration's **content**, and NOT on `roborev v0.61.2`'s
`git.FormatExcludeArgs` pathspec semantics — it feeds no pathspec to roborev and reads no `.roborev.toml`.
What it inherits is the consequence: the declaration mirrors `.roborev.toml`, whose correctness was
established against that pinned version, so after any roborev upgrade the declaration may move — and it now
moves the **correctness gate** as well as the reviewer. The re-verify-on-upgrade obligation SHALL therefore
be recorded as covering the gate, with the bucket-partition assert as its mechanical half.

#### Scenario: The classifier holds no second copy of the imported declaration
- **GIVEN** the classifier's source
- **WHEN** it is searched for the imported artifact extension list and for each of the four directory globs
  as literal text
- **THEN** none is found, and the only reference to them is through the imported names

#### Scenario: A mutation of the real declaration moves the classifier's verdict
- **GIVEN** a temporary tree holding a copy of the classifier and a copy of the **real**
  `scripts/flow/roborev-review-oracles.sh` whose declaration is mutated — a synthetic inert extension added,
  and one directory glob removed
- **WHEN** the classifier in that tree is run on a path bearing the synthetic extension inside an artifact
  directory, and on a code-bearing artifact under the removed glob
- **THEN** the first yields `docs-only` and the second yields `full`, both **changed** from their verdicts
  against the unmutated declaration
- **AND** a classifier carrying its own hardcoded copy of the lists would return the unchanged verdicts and
  therefore fail this scenario — which is what makes it a test of the import rather than a mirror of the
  constant

#### Scenario: A missing or empty declaration fails closed
- **GIVEN** a temporary tree in which the declaring file is absent, and a second in which it defines
  `CODE_FREE_ARTIFACT_EXTENSIONS=""` and an empty `CODE_FREE_ARTIFACT_DIR_GLOBS`
- **WHEN** the classifier is run in each on `docs/reports/ws0-3217-artifacts/results/run.txt` — a path that
  is `docs-only` under the real declaration
- **THEN** both yield `full` with exit status 1 and a printed reason naming the absent or empty declaration
- **AND** neither yields `docs-only` for any path under `docs/`, so no infra fault can loosen the gate

#### Scenario: The buckets partition the imported set exactly
- **GIVEN** the imported `CODE_FREE_ARTIFACT_EXTENSIONS`, read from the real declaration and not from a
  fixture copy
- **WHEN** the self-test compares it against the union of the classifier's inert, code-bearing and
  image-layer buckets
- **THEN** the union equals the imported set, the buckets are pairwise disjoint, and the assertion passes
- **AND** when a synthetic extension is added to the declaration in a temporary tree, the self-test FAILs
  with a message naming that extension and issue #3250, and the classifier classifies a path bearing it as
  `full`

#### Scenario: Sourcing the declaration has no side effects on the classifier
- **GIVEN** the classifier running under `set -euo pipefail`
- **WHEN** it sources the declaration
- **THEN** nothing is written to stdout or stderr by the sourcing itself, the shell option set is unchanged,
  no environment variable is required, and the classifier's own stdout remains exactly the one-word verdict

### Requirement: One canonical decision point classifies a `docs/` path

The classification of a path under `docs/` SHALL happen at exactly ONE place in the classifier — a single
named function reached by a single dispatch from `is_docs_file()`. `is_docs_file()` SHALL contain no `case`
arm that returns documentation for a `docs/`-prefixed pattern, and no second site anywhere in the file SHALL
decide a `docs/` path's class. This is a structural requirement, not a stylistic one: the sibling change
(#3229) spent three review rounds patching one path-normalisation consumer at a time before fixing the
boundary, and a per-`case`-arm fix here leaves the next `docs/`-shaped arm one edit away.

The self-test SHALL pin this structurally, and the structural assert SHALL be mutation-tested: a
reintroduced blanket `docs/*) return 0` arm, or a second `docs/` decision site, SHALL make the self-test
FAIL.

#### Scenario: A reintroduced blanket prefix arm fails the self-test
- **GIVEN** a temporary copy of the classifier with `docs/*) return 0 ;;` reintroduced into `is_docs_file()`
- **WHEN** the self-test's structural assertion runs against that copy
- **THEN** it FAILs with a message naming the offending arm
- **AND** the unmodified classifier passes the same assertion, so the assert is proven to bite rather than
  merely to exist

#### Scenario: A second decision site fails the self-test
- **GIVEN** a temporary copy of the classifier in which a second function also returns documentation for a
  path under `docs/`
- **WHEN** the self-test's structural assertion runs against that copy
- **THEN** it FAILs, naming the second site

#### Scenario: The classifier's external contract is unchanged
- **GIVEN** the classifier
- **WHEN** it is run on any changed set
- **THEN** it still reads a newline-delimited path list on stdin, still prints exactly one word
  (`docs-only` or `full`) to stdout with the human-readable reason on stderr, still exits 0 for
  documentation and 1 for the full path, and still forces the full path on an empty or blank-only set

### Requirement: The classifier reads raw repo-relative paths and fail-closes on any other spelling

The changed-file list SHALL reach the classifier as **raw** repo-relative paths.
`.github/workflows/pr-gate.yml` SHALL compute it with path quoting disabled
(`git -c core.quotePath=false diff --name-only`), so a legitimately non-ASCII prose path is classified on
its real extension rather than on an artefact of its spelling. The repository tracks space-bearing and
non-ASCII paths under `docs/` today, so this is a live case rather than a hypothetical one.

The classifier SHALL fail closed on any path spelling it cannot read as a raw repo-relative path — in
particular a C-quoted spelling, recognisable by a leading `"` — classifying it `full` with a named reason.
A path containing a control character remains unrepresentable in a newline-delimited stream and is closed by
this rule rather than by assuming it cannot occur.

This is the sibling change's lesson applied once rather than six times: #3229 produced six review blockers
that were all path-normalisation defects in a different consumer each, including a census that read
`docs/é notes.md` as extension `md"`. Path interpretation SHALL therefore have exactly one boundary in this
classifier.

#### Scenario: A space-bearing path is classified on its real extension
- **GIVEN** a changed set containing `docs/research/CQLite Writes (M5) — notes.md` and a second containing
  `docs/reports/ws0-3217-artifacts/harness/run all.sh`
- **WHEN** the classifier runs on each
- **THEN** the first yields `docs-only` and the second yields `full`, so a space neither smuggles code
  through nor penalises prose

#### Scenario: A C-quoted spelling fails closed
- **GIVEN** a changed set containing the literal spelling `"docs/\303\251-notes.md"`, quotes included
- **WHEN** the classifier runs
- **THEN** the verdict is `full` with exit status 1 and a reason naming the unreadable spelling
- **AND** the same path in raw form (`docs/é-notes.md`) yields `docs-only`, proving the quoted case fails on
  the spelling rather than on the extension

#### Scenario: The workflow feeds unquoted paths
- **GIVEN** `.github/workflows/pr-gate.yml`
- **WHEN** its `Classify docs-only diff` step is inspected
- **THEN** the changed-file list is produced with `core.quotePath=false`, and the step is otherwise
  unchanged: no `paths`/`paths-ignore` filter on the trigger, the classify step carries no `if:`, every
  heavy step is still gated `!= 'true'`, a docs-only branch step still exists so the required status always
  reports, and the `required` job still runs with `if: always()`, still depends on the core job, and still
  aggregates the sibling tiers

### Requirement: A hermetic self-test pins the classification behaviourally and the boundary structurally

`scripts/tests/test_classify_docs_only.sh` SHALL cover the new behaviour in its existing style — pure
shell, no cargo, Docker, datasets or network, `assert_docs_only`/`assert_full` helpers, one line per case —
and SHALL retain every existing assertion and its Ruby-based `pr-gate.yml` contract assertion. The additions
SHALL be **additive**, so the suite rebases cleanly over concurrent work on the sibling suite
`scripts/tests/test_roborev_review_guard.sh`.

Coverage SHALL include, at minimum: `.sh`, `.py` and `.bt` executables under `docs/reports/*-artifacts/`;
the docs-hosted `Cargo.toml` and `src/main.rs`; an extensionless mode-100755 harness path; one executable
hidden among many prose files in both first and last position; the two functional-config `.json` files
forcing `full`; the same extensions inside an artifact directory short-circuiting; an unrecognized extension
under `docs/` failing closed; and the negative cases proving prose plus inert `.json`/`.txt` artifacts still
short-circuit.

The suite SHALL NOT mirror the imported declaration. Where it needs the artifact extension set it SHALL read
the **real** declaration, and it SHALL additionally carry the structural and mutation assertions required
above (no literal copy of the lists; no second `docs/` decision site; a mutated declaration moves the
verdict; the buckets partition the imported set). A symmetric copy of a production constant is not a test:
it shares any defect in the original, so both sides agree and the suite is green while the gate is broken.

The suite SHALL continue to run in the full gate's `tooling-tests` component, so a regression FAILs the gate
of record rather than surfacing on a later PR.

#### Scenario: The suite fails when the classifier regresses to the prefix verdict
- **GIVEN** a classifier reverted to the blanket `docs/*) return 0`
- **WHEN** `bash scripts/tests/test_classify_docs_only.sh` runs
- **THEN** it exits non-zero, and the failing assertions include both a behavioural case (an executable
  under `docs/` reported `docs-only`) and the structural case (the reintroduced arm)

#### Scenario: Every pre-existing assertion still passes
- **GIVEN** the amended suite
- **WHEN** it runs against the amended classifier
- **THEN** the 23 assertions that existed before this change all pass unchanged, and the reported
  `FAIL` count is 0

#### Scenario: The suite is wired into the gate
- **GIVEN** `scripts/agent-gate.sh`
- **WHEN** the `tooling-tests` component runs
- **THEN** it executes `scripts/tests/test_classify_docs_only.sh`, and a failure of that script FAILs the
  component and therefore the full gate

#### Scenario: The suite is hermetic
- **GIVEN** a machine with no datasets root exported, no network, and no cargo invocation permitted
- **WHEN** the suite runs
- **THEN** it completes and reports its PASS/FAIL counts, using only shell built-ins, `git` reads of the
  local checkout, and the optional Ruby workflow assertion which skips with an `info` line when Ruby is
  absent

### Requirement: The behaviour is demonstrated on the real merged-PR shapes and recorded

The change SHALL record a demonstration against the **real** file lists of the merged, never-gated PRs
rather than only against synthetic fixtures. The 188-path changed-file list of **PR #3222**, obtained from
`gh api repos/pmcfadin/cqlite/pulls/3222/files` (both pages), replayed through the amended classifier SHALL
yield `full`; a prose-only WS0 diff — the same PR's paths with its 34 executables removed — SHALL yield
`docs-only`. The same replay SHALL be recorded for PR #3081 and PR #3216.

This SHALL be recorded evidence, not a test assertion: the input is a historical API response, so making it
a gate assertion would either pin a network call or commit a 188-path fixture that ages. The record SHALL
name the verdict, the exit status, the path count, and the count of paths responsible for the `full`
verdict, so a reader can tell the demonstration ran from the numbers rather than from a claim that it did.

#### Scenario: The real PR #3222 changed set is classified `full`
- **GIVEN** the 188 paths of PR #3222, every one under `docs/`, of which 34 are executables or
  config-as-code
- **WHEN** they are fed to the amended classifier
- **THEN** the verdict is `full` with exit status 1, and the recorded evidence names the first offending
  path and the count of offending paths
- **AND** the pre-change classifier is recorded as having returned `docs-only` on the same input, so the
  demonstration shows a changed verdict rather than an unexplained one

#### Scenario: A prose-only WS0 diff is still classified `docs-only`
- **GIVEN** the same PR #3222 path list with its 34 executable and config-as-code paths removed
- **WHEN** it is fed to the amended classifier
- **THEN** the verdict is `docs-only` with exit status 0, demonstrating on real data that the narrowing did
  not cost report PRs their fast path

#### Scenario: The demonstration is recorded, not asserted
- **GIVEN** the change's artifacts
- **WHEN** they are inspected for the demonstration
- **THEN** the verdicts, counts and commands are recorded in the change and in the PR body, and no gate
  assertion depends on a network call or on a committed copy of the API response

### Requirement: Doctrine scopes the CITE-AND-WAIVE waiver to a genuinely prose diff

CLAUDE.md's #3042 CITE-AND-WAIVE paragraph SHALL be narrowed so it no longer implies that everything under
`docs/` is non-compiled input. It SHALL name the `docs/reports/*-artifacts/` measurement-harness convention
explicitly, SHALL scope the waiver to a diff that is **genuinely prose**, and SHALL name
`scripts/ci/classify-docs-only.sh` as the mechanical test of that — so an agent can run the question instead
of judging it. The falsifying case SHALL be named: a #3222-shaped diff contains `src/main.rs` and
`Cargo.toml` under `docs/reports/ws0-3026-artifacts/ws0-cqlite/scan-harness/`, so the existing qualifier
("no `src`, no `Cargo.*`") is satisfied textually while being false materially, and an agent correctly
following the old text waives a red that is genuinely theirs.

Everything else about the rule SHALL be retained: a waiver still requires a cited issue, and the clause
voiding the waiver when any compiled input is in the diff SHALL be unchanged.

The matching published doctrine SHALL be updated in the same change. `CITE-AND-WAIVE` currently appears
nowhere on the site, so the rule SHALL be added to the gate doctrine page
(`website/src/content/docs/agents-developing/gate-contract.md`, which already owns the gate contract and the
`--delta` test/docs-only re-certification section) and SHALL be cross-referenced from
`roborev-findings.md`'s existing code-free-census definition, so the review-side and gate-side definitions
of "docs-only" cannot drift apart again.

Publication SHALL be accepted by the **new content being served** — grepping the served page for a
distinctive phrase the change introduces — and SHALL NOT be accepted on an HTTP 200 or a green deploy. A
zero count SHALL be reported as not-yet-published and re-checked, never banked as done.

#### Scenario: The narrowed rule names the convention and the mechanical test
- **GIVEN** the amended CLAUDE.md paragraph
- **WHEN** it is read
- **THEN** it names `docs/reports/*-artifacts/`, scopes the waiver to a genuinely prose diff, names
  `scripts/ci/classify-docs-only.sh` as the test, retains the cited-issue requirement, and retains the
  compiled-input-voids-the-waiver clause

#### Scenario: The published page carries the same rule
- **GIVEN** `website/src/content/docs/agents-developing/gate-contract.md` after the change
- **WHEN** it is read
- **THEN** it states the narrowed waiver rule, and `roborev-findings.md` cross-references it so the two
  definitions of "docs-only" are linked rather than independently maintained

#### Scenario: Publication is verified by served content
- **GIVEN** the deployed site after the change merges
- **WHEN** `curl -sS https://pmcfadin.github.io/cqlite/agents-developing/gate-contract/ | grep -c '<the new
  phrase>'` is run
- **THEN** the count is non-zero and that is recorded as the acceptance evidence
- **AND** a zero count is reported as not-yet-published and re-checked after a wait, and an HTTP 200 alone
  is never accepted as evidence

### Requirement: The backfill ruling for the three merged, never-gated PRs is recorded

The change SHALL record the owner's ruling, **with its reason**, on the three already-merged, never-gated
PRs — #3081 (issue #3026), #3216 (issue #3100) and #3222 (issue #3217): either a retroactive run of the
core gate's step set over that harness code, or explicit acceptance as-is with the reason stated. A decision
either way satisfies this requirement; **silence is the only failing outcome**. The ruling SHALL be
coordinated with #3229's AC7, which asks the same question about the same three PRs from the review side, so
one ruling covers both.

The record SHALL carry the evidence that bounds the exposure, so a future reader can tell the decision was
informed rather than assumed: `docs/reports/ws0-3026-artifacts/ws0-cqlite/scan-harness` is **not** a
workspace member (verified via `cargo metadata --no-deps`, 16 packages, none under `docs/`), so no
`pr-gate-core` step would have compiled, formatted or linted it even had the gate run; and none of the other
skipped steps reads any path in those diffs, which makes a retroactive run equivalent to running the core on
`main` at those commits.

The record SHALL also carry the ruling's **condition of change**: any of that harness code being promoted
into a shipped path — a gate component, a CI step, an imported module — ends the exemption, because at that
moment it inherits the obligations of the surface it joins.

#### Scenario: An acceptance-as-is ruling is recorded with its reason
- **GIVEN** the owner rules to accept the three PRs as-is
- **WHEN** the change is inspected
- **THEN** the ruling, its date, its reason and its condition of change are recorded in the change, and the
  bounding evidence (non-membership in the workspace; no skipped step reads those paths) is recorded with it

#### Scenario: A retroactive-run ruling is recorded with its outcome
- **GIVEN** the owner instead rules for a retroactive run of the core gate's step set
- **WHEN** the change is inspected
- **THEN** the commands run, the commit each was run against, and the result of each are recorded, together
  with the statement of what the run does and does not establish

#### Scenario: Silence on the backfill question fails the change
- **GIVEN** a change in which neither ruling is recorded
- **WHEN** the requirement is audited
- **THEN** it is unmet — the requirement is satisfied by the record, never by the absence of a complaint
