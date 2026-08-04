# roborev-review-guard Specification

## Purpose
TBD - created by archiving change roborev-vacuous-review-guard. Update Purpose after archive.
## Requirements
### Requirement: A PASS requires positive evidence that a review completed
The wrapper SHALL NOT report `RESULT: PASS` unless it holds POSITIVE evidence that a review actually
completed, recorded under its own greppable key `review-completed:`. The absence of a vacuity phrase
SHALL NEVER be treated as evidence that a review happened. The positive evidence SHALL be:

- the job record's structured `status` field, which SHALL NOT be a value other than `done` — a status
  present and not `done` is `FAIL (job status '<s>' is not done)`; and
- a **terminal verdict marker** in the transcript, drawn from a declared ALLOW-LIST **built from the REAL
  measured transcript**, not from invented shapes: a Findings heading (`## Review Findings`), a
  `**Severity**:` line, a Summary heading or label, or the bracketed/`Medium:` severity shapes other
  agents emit. No marker ⇒ `FAIL (no terminal verdict marker)`. The allow-list SHALL be measured because
  an invented one REJECTED a GENUINE codex review — the false-FAIL direction that gets a guard bypassed —
  and it SHALL remain a closed allow-list because everything that is NOT a review (a still-waiting job, a
  provider 400, a failed job) matches none of these shapes.

An unreadable transcript SHALL be `FAIL (transcript unreadable)`. When the structured `status` is
UNAVAILABLE the check MAY still pass on the transcript marker alone, and SHALL then record a NOTICE
naming that as the weaker of the two signals — an unavailable status SHALL NEVER be silently treated as
`done`. This requirement exists because the reproduced defect was exactly the inverse inference: a
transcript showing only an unfinished job, a provider `400 … model is not supported` outage, or a job
whose status was `failed` each contained NO vacuous phrase and therefore reached `RESULT: PASS`.

#### Scenario: A job that never finished cannot reach PASS
- **GIVEN** a pushed branch with a non-empty code census whose transcript shows only that the wrapper was waiting for the job to complete, with no terminal verdict marker
- **WHEN** the wrapper runs
- **THEN** `review-completed:` reads `FAIL (no terminal verdict marker)`, the terminal `RESULT:` is `FAIL`, and the run is NOT reportable as "roborev clean"

#### Scenario: A provider model-mismatch outage cannot reach PASS
- **GIVEN** a review whose transcript carries a provider error (for example the #2433/#3037 `400 … model is not supported` mismatch) and no terminal verdict marker
- **WHEN** the wrapper runs
- **THEN** `review-completed:` FAILs, the terminal `RESULT:` is `FAIL`, and the failure message states that the absence of a vacuous phrase is never evidence that a review happened

#### Scenario: A non-done job status fails closed
- **GIVEN** a job whose structured `status` is `failed`
- **WHEN** the wrapper evaluates completion
- **THEN** `review-completed:` reads `FAIL (job status 'failed' is not done)` and the terminal `RESULT:` is `FAIL`

#### Scenario: A completed review with a terminal verdict marker passes the completion check
- **GIVEN** a job whose structured `status` is `done` and whose transcript carries a terminal verdict marker from the allow-list
- **WHEN** the wrapper evaluates completion
- **THEN** `review-completed:` reads `PASS`, and an unavailable `status` alongside a present marker instead records a NOTICE naming the weaker signal rather than a silent pass

### Requirement: The locally computed diff census is the authoritative oracle
The wrapper SHALL compute a **local diff census** — the files changed and lines added/removed for
`<base>...HEAD`, obtained from `git` in the target repository — and SHALL treat that locally computed
census as the authoritative statement of what must be reviewed. Every downstream judgement SHALL be
made against that census and never against the reviewer's own report of what it saw. The census SHALL
be reported under `census:` and its own verdict under `census-check:`.

Rename detection SHALL be disabled, so every census entry is a REAL path (a rename-composite path such
as `dir/{old => new}.rs` is not a path and could never be located in the reviewer's prompt). The
consequence — a rename counts as TWO census paths while the reviewer's diff may render it as ONE two-sided
header — SHALL be reconciled in `prompt-content:`, never by re-enabling rename detection here.

The census SHALL be partitioned into a CODE subset and a non-code subset by the classification below.
`census:` SHALL report the TOTAL (`<N> file(s), +<A>/-<D>`, covering both subsets, since that is what
changed), while `code-free:` is decided by the non-code count equalling the total and `prompt-content:` is
asserted over the CODE subset alone.

An unmeasurable census SHALL fail closed and SHALL be DISTINGUISHABLE from an empty one:
`FAIL (base '<ref>' unresolvable)` when the base ref does not resolve to a commit, and
`FAIL (git diff failed)` when the diff command itself exits non-zero. Neither SHALL be aliased to
`FAIL (empty census)` / `NOTHING-TO-REVIEW`, because "we could not tell" is not "there is nothing to
review". The wrapper SHALL NOT fetch on the caller's behalf to repair an unresolvable base.

#### Scenario: An unresolvable base ref fails closed rather than reporting nothing to review
- **GIVEN** a clone whose `origin/main` mirror ref does not resolve (a narrow fetch refspec that has never fetched it)
- **WHEN** the wrapper runs with the default base
- **THEN** `census-check:` reads `FAIL (base 'origin/main' unresolvable)`, the terminal `RESULT:` is `FAIL` (not `NOTHING-TO-REVIEW`), no review is enqueued, and the message states that an unresolvable base is "we cannot tell", never "there is nothing to review"

#### Scenario: A failed git diff is not "genuinely empty"
- **GIVEN** a repository in which `git diff --numstat -z --no-renames <base>...HEAD` exits non-zero
- **WHEN** the wrapper computes the census
- **THEN** `census-check:` reads `FAIL (git diff failed)`, the message reproduces what git said, and the outcome is `RESULT: FAIL` rather than `NOTHING-TO-REVIEW`

#### Scenario: The census is the oracle every later judgement is measured against
- **WHEN** the census is non-empty
- **THEN** `census:` reports the file count and the added/removed line totals, and the code-free, prompt-content and vacuity checks all state their verdicts relative to that census rather than to anything the reviewer reports

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

The value set SHALL be exactly:

- `PASS (<n>/<n> code census paths present)` — every code path found;
- `FAIL (<k>/<n> code census paths absent from the prompt)` — `<k>` MISSING of `<n>` checked, naming the
  missing paths (first ten). Note the two values carry the SAME denominator `<n>` but OPPOSITE numerator
  senses (present on PASS, absent on FAIL), so a grep-based reader SHALL read the value word, never the
  ratio alone;
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
because the reviewer recognised prose. The earlier wording — "roborev excludes non-code paths" — is
FALSIFIED and SHALL NOT be restored: under a configured `docs/**` the same mechanism discarded 33
executable harness files on PR #3222, i.e. it excluded CODE.
So for a diff every path of which the configured set excludes, the constructed diff is genuinely EMPTY, and
the reviewer's "contains no code
changes to review" is a TRUTHFUL report of an empty input rather than a reviewer malfunction. That is
precisely why the correct response is a DETERMINISTIC pre-enqueue FAIL computed from our own census — the
reviewer is not misbehaving and no amount of re-running or re-prompting will change the outcome — and why
the census is ALSO reconciled against the effective exclusion set under `census-exclusion:` (#3229), so a
configured pattern that would swallow CODE fails before the enqueue instead of masquerading as a code-free
diff. A `docs/` path PREFIX SHALL NEVER on its own satisfy `code-free:`: the `docs/reports/*-artifacts/`
measurement harnesses are executable CODE.

Classification SHALL be by file EXTENSION against a declared prose-extension set, with a path assist
limited to EXTENSIONLESS files under declared prose directories. A file with a code-ish extension
anywhere in the tree — including `docs/foo.py` and `.github/workflows/*.yml` — SHALL count as CODE, so
the check cannot false-FAIL a code change that merely lives in a documentation directory.

Under a declared prose directory an EXTENSIONLESS path SHALL count as CODE **iff git RECORDS it
EXECUTABLE AT EITHER ENDPOINT of the census range**, the mode read from the tree and never from
the filesystem. The prose PREFIX alone SHALL NOT decide it: that made every extensionless path under
`docs/` non-code, so it never entered the code census and `prompt-content:` made no claim about it at all —
while the configured exclusions do not remove it, so it does reach the reviewer.

The endpoint test SHALL be a **logical OR over the HEAD tree and the BASE tree**, never an ordered scan
that stops at the first endpoint holding a record. All four combinations SHALL follow from that one rule:
present at both (including a MODE CHANGE in either direction — a `chmod -x` SHALL NOT reclassify a script
as prose), HEAD only, BASE only (classified by the mode it HAD, since removing an executable is a code
change whose review must be asserted), and neither (unmeasurable ⇒ non-executable, no error). An ordered
scan is a FALSE-PASS mechanism, not an optimisation: it classified `100755`@BASE → `100644`@HEAD as
NON-CODE, so the path left the code census and `prompt-content: PASS (n/n)` was silent about it. That
property SHALL hold BY CONSTRUCTION — a range-blind per-endpoint lookup, an endpoint list complete before
the fold, and a fold with no `break`/`continue`/`return` whose single post-loop `return` yields an
accumulator — and the SHAPE SHALL be asserted structurally, with the assert itself controlled against
mutants that violate it.

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

### Requirement: A vacuous verdict claim against a non-empty census fails, gated on the findings state
The wrapper SHALL compare the reviewer's own verdict text against the non-empty census under the
greppable key `vacuity-tier1:`, and a vacuity claim there SHALL be AUTHORITATIVE — a HARD FAIL that
blocks the merge, not a note. Two properties SHALL bound the match so it cannot false-FAIL a genuine
review:

1. **ANCHORING TO THE WHOLE SUMMARY BLOCK.** The match SHALL be confined to the verdict/summary region,
   and that region SHALL be the whole summary **BLOCK** — from a `Summary` HEADING (`## Summary`) or a
   `Summary:` label ANYWHERE on a line, through to the next heading or EOF — never merely the lines that
   themselves CONTAIN `Summary:`. This is a REQUIRED strengthening, not a stylistic detail: the real
   reviewer format is a heading followed by blank line and prose, so a line-only region missed the prose
   entirely and a vacuous clean review whose "no code changes" sentence sat UNDER the heading reported
   **PASS** — the exact defect this capability exists to stop. The block form is a strict SUPERSET of the
   line form, so the older single-line `No issues found. Summary: …` shape stays covered. A transcript
   with no such region SHALL read `UNAVAILABLE` (a non-failing degraded value; the deterministic checks
   still govern, and this tier can never rescue another key's FAIL).
2. **GATING ON the findings state** (the `findings:` key below):
   - `findings: NONE` — the reviewer is CLAIMING CLEANLINESS, so the phrase is a vacuity claim about a
     census we measured as non-empty ⇒ `FAIL (vacuous verdict vs non-empty census)`.
   - `findings: UNKNOWN` — the state is unknowable ⇒ HARD FAIL as well. An unknowable findings state
     SHALL NEVER DISARM this check; fail-closed is the correct direction. `INCONSISTENT` (below) is
     likewise neither `PRESENT` nor `NONE` and SHALL NOT exempt this check.
   - `findings: PRESENT` (with or without a count) — the reviewer demonstrably analysed the diff and
     produced findings, so the phrase is discussion ⇒ an advisory
     `NOTICE (phrase present in a findings-bearing review)` that does NOT fail the run.

The gating and anchoring SHALL be recorded as an EVIDENCED relaxation, not silent drift: the
unanchored, ungated form false-FAILed a genuine findings-bearing review that merely QUOTED the phrase
(this change's own diff carries the phrase in five or more files), and the systemic cost of a false
FAIL is agents learning to WAIVE tier-1 FAILs — which restores the original defect wholesale.

#### Scenario: A cleanliness claim of no code changes against a code census is a hard failure
- **GIVEN** a pushed branch whose census against the base is non-empty and contains code
- **WHEN** the review's summary states the diff contains no code changes to review and the review reports NO findings
- **THEN** `vacuity-tier1:` reads `FAIL (vacuous verdict vs non-empty census)`, the message prints the census and states that the reviewer's claim contradicts a fact the wrapper measured itself, the terminal `RESULT:` is `FAIL`, and the run is NOT reportable as "roborev clean"

#### Scenario: An UNKNOWN findings state does not disarm the check
- **GIVEN** a run whose findings state is `UNKNOWN` because the reviewer errored
- **WHEN** the verdict region carries the vacuity phrase
- **THEN** `vacuity-tier1:` still reads `FAIL (vacuous verdict vs non-empty census)` and the message states that an unknowable findings state is treated as claiming cleanliness because fail-closed is the correct direction

#### Scenario: A findings-bearing review that quotes the phrase is a NOTICE, not a failure
- **GIVEN** a review that reported findings and whose summary discusses the phrase "no code changes"
- **WHEN** the wrapper evaluates tier 1
- **THEN** `vacuity-tier1:` reads `NOTICE (phrase present in a findings-bearing review)`, the NOTICE explains that the review demonstrably analysed the diff, and the NOTICE does not fail the run

#### Scenario: The match is anchored to the verdict region, not the whole transcript
- **GIVEN** a clean review whose finding bodies or quoted material mention "no code changes" while its own summary does not
- **WHEN** the wrapper evaluates tier 1
- **THEN** `vacuity-tier1:` reads `PASS`, because an unanchored match would false-FAIL a genuine review and teach agents to waive the check

#### Scenario: A vacuity claim under a `## Summary` HEADING is caught
- **GIVEN** a clean review reporting no findings whose transcript carries `## Summary` followed by a blank line and then the sentence claiming the diff contains no code changes to review, against a non-empty code census
- **WHEN** the wrapper evaluates tier 1
- **THEN** `vacuity-tier1:` reads `FAIL (vacuous verdict vs non-empty census)` and the terminal `RESULT:` is `FAIL` — a region restricted to lines containing `Summary:` would have reported PASS on exactly this transcript

### Requirement: Token accounting corroborates the deterministic checks and may only fail closed
Token accounting SHALL be a CORROBORATING signal under `vacuity-tier2:` whose only permitted effect is
to FAIL CLOSED — it SHALL NEVER cause a run to pass, SHALL NEVER relax another check's FAIL, and SHALL
NEVER be the sole thing standing between the pipeline and a vacuous pass.

Extraction SHALL distinguish THREE states and SHALL be reported so a reader can tell them apart:

- **absent** — the job record carries no token accounting at all ⇒ `UNAVAILABLE`, a visible
  degraded-signal notice, never a silent skip.
- **parsed** — counts readable ⇒ the thresholds are evaluated.
- **present but unparseable** — a token field IS present yet no documented field alias resolves to a
  number ⇒ `FAIL (token accounting present but unparseable — drift)`. This SHALL be a FAIL and SHALL
  NOT be downgraded to a notice, because that is exactly how the tier was SILENTLY DISARMED: the real
  payload double-encodes its token container as a JSON STRING and names the output count
  `total_output_tokens`, so reading it as a nested object with `output_tokens` yielded no counts, the
  tier reported a non-failing `UNAVAILABLE` on EVERY real run, and a guard that silently was not there
  certified runs whose true counts were the vacuous baseline. The remedy named in the failure message
  SHALL be to add the new field alias, never to waive the check.

The FAIL conditions on parsed counts SHALL be: an input count below the named input floor, OR a cached
input count of zero. Each SHALL print the OBSERVED value beside the named constant that tripped.
Thresholds SHALL be named constants declared with the measured evidence cited beside them.

Two calibration decisions SHALL be recorded with their evidence, so each is a documented decision
rather than silent drift:

- The input floor SHALL be anchored on the measured VACUOUS CEILING, not on the genuine band, because
  the genuine band scales with diff size: **25,000** sits above the highest observed vacuous run
  (18,801) and below the smallest observed genuine run (67,387). The originally specified 50,000 would
  have false-FAILed that genuine run's size class, and an always-red guard is the failure mode that
  gets a guard bypassed.
- An output-token floor SHALL NOT be a FAIL condition; it MAY be reported as an advisory NOTICE only.
  A genuine CLEAN review and a vacuous one emit near-identical output counts (both are "No issues
  found" plus one sentence; the vacuous baseline measured 21–56), so the counts COLLIDE and any output
  floor would false-FAIL precisely the case that matters most — a real review that is legitimately
  clean.
- A `cached_input_tokens == 0` FAIL SHALL be retained with its false-positive caveat documented (a
  genuinely cold cache can report zero); it is an accepted trade in the fail-closed direction, made
  affordable by the deterministic checks now carrying the verdict.

Wall-clock duration SHALL NOT be asserted (host-dependent, #2642).

#### Scenario: The vacuous token signature against a non-empty census fails loudly
- **GIVEN** a pushed branch with a non-empty code census whose job reports the measured vacuous accounting (input ≈18k, cached 0)
- **WHEN** the wrapper evaluates tier 2
- **THEN** `vacuity-tier2:` reads `FAIL (vacuous token signature)`, each trip prints the observed value beside the named threshold constant, and the terminal `RESULT:` is `FAIL`

#### Scenario: Token accounting present but unparseable is failed as drift
- **GIVEN** a job whose token container is present but whose count fields match none of the documented aliases
- **WHEN** the wrapper evaluates tier 2
- **THEN** `vacuity-tier2:` reads `FAIL (token accounting present but unparseable — drift)`, the terminal `RESULT:` is `FAIL`, and the message names the extractor's alias sets as the fix and says not to waive it

#### Scenario: The real doubly-encoded payload shape is decoded and the tier actually evaluates
- **GIVEN** a job whose token container is a JSON-ENCODED STRING carrying `total_output_tokens`, with the measured small-but-genuine counts (67,387 input / 43,520 cached / 2,232 output)
- **WHEN** the wrapper evaluates tier 2
- **THEN** the counts appear on the `tokens:` line and `vacuity-tier2:` reads `PASS` — it is not reported as `UNAVAILABLE`, which is what a single decode produced on every real run

#### Scenario: A low output count never fails a genuine clean review
- **GIVEN** a genuine review with healthy input and cached counts whose output count is below the advisory output constant
- **WHEN** the wrapper evaluates tier 2
- **THEN** `vacuity-tier2:` reads `PASS`, and the low output count is reported only as an advisory NOTICE that states output tokens cannot discriminate a genuine clean review from a vacuous one

#### Scenario: Absent token accounting degrades visibly and never rescues a failing run
- **GIVEN** a roborev build whose job record carries no token accounting at all
- **WHEN** the wrapper evaluates a run whose deterministic checks pass
- **THEN** `vacuity-tier2:` reads `UNAVAILABLE` with an explicit degraded-signal notice, the deterministic checks still govern the verdict, and the unavailability alone neither fails the run nor turns any other check's FAIL into a PASS

### Requirement: A genuinely empty census reports NOTHING-TO-REVIEW, never a pass
When the local diff census for `<base>...HEAD` is genuinely empty, the wrapper SHALL NOT invoke a review
and SHALL exit with a DISTINCT `NOTHING-TO-REVIEW` status — a non-zero exit code distinct from the
failure exit code — that is explicitly NOT a pass. A `NOTHING-TO-REVIEW` outcome SHALL NOT be recordable
as "roborev clean" by any caller.

#### Scenario: An empty census yields NOTHING-TO-REVIEW rather than PASS
- **GIVEN** a pushed branch whose diff against the base is genuinely empty (no files changed)
- **WHEN** the wrapper runs
- **THEN** it does not enqueue a review, its summary block terminates in `RESULT: NOTHING-TO-REVIEW`, and it exits with a non-zero code distinct from the FAIL exit code

#### Scenario: NOTHING-TO-REVIEW is distinguishable from PASS by exit code alone
- **WHEN** a caller inspects only the wrapper's exit status
- **THEN** the PASS, FAIL, and NOTHING-TO-REVIEW outcomes are three distinct exit codes, so a caller can never mistake "there was nothing to review" for "it was reviewed and clean"

### Requirement: The sanctioned invocation reviews the census RANGE with an explicit repository path
The wrapper SHALL invoke roborev over the **CENSUS RANGE** with an **EXPLICIT ABSOLUTE `--repo`**, i.e.
`roborev review --branch --base <base> --repo <abs> --agent <a> --model <m> --wait`, which was MEASURED to
enqueue `git_ref = <base40>..<head40>` and to deliver every code file of the census to the reviewer (5/5
in the matrix at the top of this delta). The reviewed scope SHALL therefore be exactly what the census
measured — the property AC2 was reaching for.

Three forms SHALL NEVER be used, each for a MEASURED reason:

- **`--branch` WITHOUT an explicit `--repo`** — from an unregistered worktree it resolves against the ROOT
  checkout, which normally sits on the base branch. `--repo` is what makes `--branch` correct from a
  worktree, so the prohibition is on the MISSING `--repo`, NOT on `--branch` itself.
- **the two-positional commit-range form** (`<base> <head>`) — measured to anchor the range at git's
  EMPTY-TREE hash (`4b825dc6…`) and to deliver only 3/5 code files.
- **a single-SHA review** (`<sha>`) — measured to enqueue `git_ref = <head40>` and deliver 3/5 code files:
  it reviews ONE COMMIT, so on any multi-commit branch it certifies the branch from its last commit alone.

The wrapper SHALL require BOTH the reviewer agent and the reviewer model to be supplied, refusing to run
with only one of them. An option supplied with an EMPTY value SHALL be a usage error rather than a silent
fallback to the default, because a `--repo ""` falling back to `$PWD` is exactly how a caller reviews a
repository it did not name. `--repo` SHALL be resolved to an ABSOLUTE path (roborev must never receive a
relative one) and SHALL always be passed explicitly — the wrapper SHALL never let roborev infer the
repository from `$PWD`, because that inference IS the original defect.

#### Scenario: The invocation names the census range and an absolute repo path
- **WHEN** the wrapper invokes roborev
- **THEN** the command line is `review --branch --base <base> --repo <abs-repo> --agent <a> --model <m> --wait` — `--branch` PAIRED with an explicit absolute `--repo`, and carrying neither two positional commit arguments nor a single positional sha

#### Scenario: The three non-sanctioned forms are never emitted
- **WHEN** the wrapper's invocation is inspected
- **THEN** it never invokes `--branch` without an explicit `--repo`, never passes two positional commits (whose range base was measured to be git's empty-tree hash), and never passes a single positional sha (which reviews one commit only)

#### Scenario: Supplying only an agent or only a model is a usage error
- **GIVEN** an invocation that supplies a reviewer agent but no reviewer model (which would inherit a mismatched model from the repository's roborev config and fail as a silent-looking review outage)
- **WHEN** the wrapper runs
- **THEN** it refuses with a non-zero exit and a message naming the missing option, before any review is enqueued

#### Scenario: An empty option value is a usage error, not a default
- **GIVEN** an invocation that passes an option with an empty value (for example an empty `--repo`)
- **WHEN** the wrapper parses its arguments
- **THEN** it refuses with the usage exit code and a message stating that an empty value is never a default, and nothing is enqueued

### Requirement: The reviewed RANGE is asserted against the census range using the job record as the oracle
The wrapper SHALL assert the reviewed scope under the greppable key `sha-assert:`, using the **job
record's structured `git_ref`** as the oracle — recorded by roborev itself, compared full-sha to full-sha,
case-normalised. Because the sanctioned invocation reviews a RANGE, `git_ref` is normally
`<base40>..<head40>` and **BOTH ENDPOINTS SHALL be asserted** against the census range (`reviewed-sha:`
SHALL report that range verbatim). This is strictly STRONGER than the single-sha equality it replaces: it
proves the reviewed scope neither stops short of the branch tip nor starts somewhere other than the
census base.

The value set SHALL be:

- `PASS` — range head == branch HEAD AND range base == the resolved base sha.
- `FAIL (reviewed range does not match <base>...HEAD)` — either endpoint disagrees, with the message
  naming WHICH: a range BASE of git's empty-tree hash SHALL be named as the signature of the
  non-sanctioned two-positional form, and a range HEAD short of branch HEAD SHALL be named as a reviewed
  scope that stops short of the tip.
- `FAIL (single-commit record, not the census range)` — the record reports a SINGLE commit **even when it
  EQUALS branch HEAD**. This SHALL fail closed: a single-commit review covers one commit, and because
  `prompt-content` matches PATHS, a review of only the last of several commits touching the same file
  passes every path check while the earlier changes go unreviewed. The sanctioned invocation always
  records a range, so a single sha means something else ran.
- `FAIL (reviewed-sha does not match head-sha)` — a single-commit record that is not branch HEAD;
  attributed when it equals the base ref (the signature of `--branch` resolved against the ROOT checkout)
  and otherwise named as matching neither endpoint.
- `FAIL (job record unavailable — reviewed range unverifiable)` — no `git_ref` after the bounded read.
  This SHALL FAIL rather than fall back to prose: for a RANGE review the stdout announcement names only
  the range BASE, so it cannot establish that branch HEAD was reviewed at all, and a fallback to it would
  be a check that verifies nothing.
- `FAIL (no parseable enqueue announcement)` / `FAIL (unparseable enqueue announcement)` /
  `FAIL (roborev not on PATH)` / `SKIP`.

The stdout announcement SHALL be DEMOTED to the **carrier of the job id** — it SHALL NOT be an oracle for
the reviewed scope — while remaining load-bearing enough to fail closed, because every structured query
needs that id: an ABSENT announcement SHALL be `FAIL (no parseable enqueue announcement)` and a malformed
one (a non-numeric job id, or a sha shorter than the declared 7-hex-char floor) SHALL be
`FAIL (unparseable enqueue announcement)` — never a skipped check. Parsing SHALL be defensive:
case-normalised before matching, both fields validated before use, and when several announcements are
present the LAST one SHALL be the effective enqueue with the multiplicity recorded as a NOTICE.

#### Scenario: A matching range satisfies the assert
- **WHEN** the job record's `git_ref` is `<base40>..<head40>` whose head equals branch HEAD and whose base equals the resolved base sha
- **THEN** `sha-assert:` reads `PASS` and `reviewed-sha:` reports the full `<base40>..<head40>` range beside `head-sha:`

#### Scenario: A range whose endpoints do not match the census range fails closed
- **GIVEN** a job record whose reviewed range disagrees with the census range at either endpoint
- **WHEN** the wrapper asserts the reviewed range
- **THEN** `sha-assert:` reads `FAIL (reviewed range does not match <base>...HEAD)`, the message names the offending endpoint(s) and the expected values, an empty-tree range base is named as the two-positional-form signature, and the terminal `RESULT:` is `FAIL`

#### Scenario: A single-commit record is refused even when it equals branch HEAD
- **GIVEN** a job record whose `git_ref` is a single sha equal to branch HEAD
- **WHEN** the wrapper asserts the reviewed range
- **THEN** `sha-assert:` reads `FAIL (single-commit record, not the census range)` and the message explains that a single-commit review covers one commit only, so path-based checks cannot see the earlier commits' changes

#### Scenario: A reviewed sha equal to the base ref aborts and names the base
- **GIVEN** a worktree branch whose HEAD differs from its base `origin/main`
- **WHEN** the job record reports the base ref as the reviewed commit
- **THEN** the wrapper exits non-zero with `RESULT: FAIL`, and the message states that the reviewed sha equals the base ref, that NO branch change was reviewed, and that base-equality is the signature of a `--branch` review resolved against the ROOT checkout

#### Scenario: An unavailable job record fails closed rather than falling back to the announcement
- **GIVEN** a job whose record still carries no `git_ref` after the bounded read
- **WHEN** the wrapper asserts the reviewed range
- **THEN** `sha-assert:` reads `FAIL (job record unavailable — reviewed range unverifiable)`, the message states that the announcement names only the range BASE and therefore cannot establish that branch HEAD was reviewed, and the run does not report a pass

#### Scenario: A missing or malformed enqueue announcement fails closed
- **WHEN** the transcript contains no parseable enqueue announcement, or one whose job id is non-numeric or whose sha is shorter than the declared 7-hex-char floor
- **THEN** `sha-assert:` reads `FAIL (no parseable enqueue announcement)` or `FAIL (unparseable enqueue announcement)` respectively, the reviewed scope is treated as unverifiable, and the run does not report a pass

#### Scenario: The announcement carries only the job id, and its multiplicity is recorded
- **GIVEN** a transcript carrying two enqueue announcements, the last naming job `4656`
- **WHEN** the wrapper parses it
- **THEN** `job:` reads `4656`, a NOTICE records that two announcements were present and that the last is the effective enqueue, and the announced sha is used for no scope judgement

### Requirement: The job record is read from whichever source answers, and its completeness is reported
Four asserts (`sha-assert`, `review-completed`, `findings`, `vacuity-tier2`) and the `model:` line depend
on the structured job record, so the wrapper SHALL read it explicitly and SHALL report what it got under
its own greppable key `job-record:`, with the value set:

- `PASS` — the required fields (`git_ref`, a terminal `status`) AND token accounting are all present.
- `PASS (no token accounting in the record)` — the required fields are present while token accounting is
  absent. Token accounting SHALL be DESIRABLE, not required: a build may legitimately report none, and
  spending the whole bound waiting for it would cost the bound on every such run, so it SHALL get one
  grace poll and then be accepted.
- `DEGRADED (incomplete after <n>s: <missing fields>)` — the record could not be completed. This value
  SHALL be NON-FAILING **and** SHALL NOT silently weaken anything: each dependent assert SHALL publish its
  own verdict under its own key (notably `sha-assert: FAIL (job record unavailable — reviewed range
  unverifiable)`), so the consequence is always visible where it applies.
- `SKIP` — no parseable announcement, so there was no job id to query.

**TWO SOURCES OF DIFFERENT SHAPE SHALL both be consulted, and a source SHALL count only when it yields
the fields the asserts require.** Measured: `roborev show <job> --json` returns the **REVIEW** row, which
answers to the same id and carries `prompt`/`agent` but NO `git_ref`, `status` or `token_usage`, and
NESTS the JOB row under a `job` key; `roborev list --json` returns the JOB row directly. Accepting the
first payload that merely PARSED therefore returned the poorer row, and the record looked permanently
incomplete. The extractor SHALL prefer an id match that actually carries `git_ref` (so the nested job row
is a first-class source), falling back to the first id match only when none does, and a payload with no id
echoed back SHALL be accepted ONLY when it is a single top-level object — for a list or a nested
collection the first object carrying `git_ref` may be an UNRELATED or EARLIER job, which would falsely
certify the job just enqueued.

With the nested job row read as a first-class source the record is **complete in ONE read**. The bounded
poll (5 attempts at 1s) is therefore a **SANITY RETRY, not a wait for asynchronous durability** — an
earlier diagnosis of an async write was a MISDIAGNOSIS of the wrong-row read, and SHALL NOT be restated as
the reason. Its two knobs SHALL be overridable for test timing only, and shortening them SHALL only ever
be able to make the record MORE likely to read `DEGRADED` — the fail-closed direction.

#### Scenario: A complete record reads PASS
- **GIVEN** a job whose record carries `git_ref`, a terminal `status` and readable token accounting
- **WHEN** the wrapper reads the record
- **THEN** `job-record:` reads `PASS` and the dependent asserts evaluate against the structured fields

#### Scenario: The nested job row is used rather than the outer review row
- **GIVEN** a `show --json` payload whose top-level REVIEW row answers to the job id but carries none of the required fields, while the JOB row nested under its `job` key carries all of them
- **WHEN** the wrapper reads the record
- **THEN** `job-record:` reads `PASS` — the id match that actually carries `git_ref` wins, so a record that is in fact complete is never reported as incomplete

#### Scenario: An unreadable record is DEGRADED, and every dependent assert says so itself
- **GIVEN** a job whose record never yields `git_ref` or `status`
- **WHEN** the wrapper finishes the bounded read
- **THEN** `job-record:` reads `DEGRADED (incomplete after <n>s: …)` naming the missing fields, that value alone does not fail the run, and `sha-assert:` independently reads `FAIL (job record unavailable — reviewed range unverifiable)` so the run still cannot pass

#### Scenario: A record without token accounting still passes, explicitly
- **GIVEN** a job whose record carries the required fields but no token accounting at all
- **WHEN** the wrapper reads the record
- **THEN** `job-record:` reads `PASS (no token accounting in the record)` and `vacuity-tier2:` separately reports its own `UNAVAILABLE` degraded-signal notice

### Requirement: A model substitution is surfaced, never silent
The wrapper SHALL report the model the job actually ran under the greppable key `model:`, and SHALL
surface a difference between the requested model and the model the job ran as a LOUD NOTICE naming both
values. It SHALL NOT be a FAIL: roborev legitimately canonicalises/resolves a model alias, so a
mismatch is not by itself evidence of a bad review, and an always-red guard is the failure mode that
gets a guard bypassed (a failure mode this change hit twice). When the job record carries no model
field, the line SHALL say so explicitly rather than implying confirmation.

#### Scenario: A substituted model is reported loudly without failing the run
- **GIVEN** a job whose requested model differs from the model it ran
- **WHEN** the wrapper emits its block
- **THEN** `model:` names the model that ran and marks it as SUBSTITUTED, naming the requested model, a NOTICE tells the operator to confirm the substituted model is acceptable for a merge-gating review, and the substitution alone does not fail the run

#### Scenario: A matching model is reported plainly and an absent one is marked unconfirmed
- **WHEN** the job's model equals the requested model
- **THEN** `model:` reports it plainly; and when the job record carries no model field at all, the line marks the value UNCONFIRMED rather than presenting it as confirmed

### Requirement: The branch is asserted pushed by asking the REMOTE, never a local mirror ref
Before enqueuing a review the wrapper SHALL assert, under `push-assert:`, that the branch exists on its
remote and that the remote tip equals local HEAD — with `git ls-remote` (the REMOTE itself) as the
AUTHORITATIVE oracle, compared full-sha. There SHALL be NO local mirror-ref (`refs/remotes/<remote>/<branch>`)
fast path. Two evidenced reasons:

- CQLite clones carry a NARROW fetch refspec (`+refs/heads/main:refs/remotes/origin/main`), so a feature
  branch's mirror ref is NEVER created however often the branch is pushed — a mirror-based assert
  false-FAILed 100% of the fleet, which would have made the only sanctioned invocation unusable and
  pushed agents back to the bare `--branch` form this wrapper exists to replace.
- A CACHED mirror ref survives a force-push or an outright deletion of the remote branch, so it can
  equal HEAD while the remote no longer has the commit — enqueueing a review of a commit the reviewer
  cannot fetch, which is itself a vacuous-review setup.

The remote SHALL be taken from the branch's configured upstream, falling back to `origin`, never
hard-coded. Failure causes SHALL be DISTINCT and correctly attributed: `FAIL (detached HEAD)`,
`FAIL (ls-remote failed: infra/auth)` (an unknown remote state — explicitly NOT "never pushed", since
`git` and `gh` are separate credential paths), `FAIL (branch absent on remote <remote>)`, and
`FAIL (unpushed commits)` naming the unpushed commits (or the divergence when local HEAD is not a
descendant of the remote tip). Every one of these SHALL happen BEFORE a review is enqueued.

#### Scenario: A pushed branch under the fleet's narrow fetch refspec passes
- **GIVEN** a clone whose fetch refspec only mirrors `main`, so `refs/remotes/origin/<branch>` never exists, and whose feature branch IS pushed
- **WHEN** the wrapper runs
- **THEN** `push-assert:` reads `PASS` because the assert asked the remote via `git ls-remote`, and the run proceeds to enqueue a review

#### Scenario: A stale mirror ref equal to HEAD does not satisfy the assert
- **GIVEN** a branch whose local mirror ref equals HEAD but whose branch has been DELETED from the remote
- **WHEN** the wrapper runs
- **THEN** `push-assert:` reads `FAIL (branch absent on remote <remote>)` and no review is enqueued, because a cached local proxy is never authority for what the remote has

#### Scenario: An unpushed or behind branch fails before a review is enqueued
- **GIVEN** a branch that has never been pushed, and separately a branch whose remote tip is behind HEAD
- **WHEN** the wrapper runs
- **THEN** it exits non-zero with `RESULT: FAIL` — `FAIL (branch absent on remote <remote>)` in the first case and `FAIL (unpushed commits)` naming the unpushed commit(s) in the second — and no review job is enqueued

#### Scenario: An ls-remote failure is attributed to infra/auth, not to being unpushed
- **GIVEN** a remote that cannot be reached or read
- **WHEN** the push assert runs
- **THEN** `push-assert:` reads `FAIL (ls-remote failed: infra/auth)`, the message reproduces what git said and states this is NOT evidence the branch is unpushed (naming the separate `git`/`gh` credential paths), and the run fails closed on the unknown remote state

#### Scenario: A detached HEAD fails before anything is enqueued
- **GIVEN** a repository on a detached HEAD
- **WHEN** the wrapper runs
- **THEN** `push-assert:` reads `FAIL (detached HEAD)`, the message says to check out the issue branch, and no review is enqueued

### Requirement: A findings-bearing review is distinguished from a reviewer error, both under their own greppable keys
The wrapper SHALL report the findings state under its own greppable key `findings:` (`NONE`,
`PRESENT`, `PRESENT (<n>)`, `UNKNOWN`, `INCONSISTENT (verdict clean, <n> findings marker(s))`,
`INCONSISTENT (exit 0, <n> findings marker(s))`, or `SKIP`) and the reviewer process's own status under
`roborev-exit:`, and SHALL DISTINGUISH the two non-zero causes: `FINDINGS (exit <N>)` when the review
RAN and reported findings, versus `ERROR (exit <N>)` when the reviewer itself failed. The authority for
which occurred SHALL be the job record's structured `status`, falling back to the completion evidence.

**The PRESENT/NONE decision SHALL be derived from the STRUCTURED `verdict` field**, not from prose over the
whole transcript. Tier 1 is GATED on this answer, so a regex over the entire output was a real weakness:
an incidental or QUOTED severity token such as `[Low]` anywhere in the output set `findings: PRESENT`,
which then EXEMPTED a genuinely vacuous "no code changes" verdict from tier 1's hard failure. Where no
structured verdict exists the wrapper SHALL fall back to the reviewer's EXIT CODE, and prose SHALL be
consulted only inside the FINDINGS BLOCK (from a `Findings` heading/label to a LINE-INITIAL `Summary`
heading/label). The `<n>` COUNT SHALL remain BEST-EFFORT prose parsing of severity markers within that
block and SHALL be reported for a human, never used as an authority; the PRESENT/NONE/INCONSISTENT
distinction is the load-bearing part.

**A CONTRADICTION SHALL FAIL.** A structured verdict of "clean" (or, absent one, a zero exit) while the
findings block DOES carry severity markers SHALL be `INCONSISTENT (verdict clean, <n> findings marker(s))`
or `INCONSISTENT (exit 0, <n> findings marker(s))` respectively. Both SHALL fail the run, and being
NEITHER `PRESENT` nor `NONE` neither of them SHALL exempt tier 1 either.

Both cause the terminal `RESULT: FAIL` — a review with open findings is not "roborev clean" — but the
attribution SHALL be correct, because roborev exits NON-ZERO WHEN IT REPORTS FINDINGS, and calling that
a reviewer malfunction is dangerous in the OPPOSITE direction from the vacuity defect: an agent told
the reviewer broke will RETRY or BYPASS instead of FIXING the findings. The `FINDINGS` message SHALL
therefore say the review is genuine, that the reviewer did not malfunction, and that the findings must
be triaged and fixed; the `ERROR` message SHALL name it as an infra condition and point at the daemon,
credentials and transcript. A zero exit SHALL read `roborev-exit: PASS`, and a failure before the
reviewer ran SHALL read `SKIP`.

A prose detail line alone SHALL NOT satisfy this requirement: because a caller retains ONLY the summary
block and reads it by grepping the per-check keys, without these keys a reader sees every per-check key
reading `PASS` beside a `RESULT: FAIL` and cannot attribute the failure.

#### Scenario: A non-zero exit with a completed review is FINDINGS, not a malfunction
- **GIVEN** a pushed branch with a non-empty census whose job status is `done` and whose reviewer process exited non-zero after reporting findings
- **WHEN** the wrapper emits its block
- **THEN** `roborev-exit:` reads `FINDINGS (exit <N>)`, `findings:` reads `PRESENT` (with a count when countable), the terminal `RESULT:` is `FAIL`, and the message states the review is genuine, tells the operator to triage and fix the findings, and says not to retry or bypass the reviewer

#### Scenario: A non-zero exit with a job that did not complete is an ERROR
- **GIVEN** a reviewer process that exited non-zero on a job whose status is not `done`
- **WHEN** the wrapper emits its block
- **THEN** `roborev-exit:` reads `ERROR (exit <N>)`, `findings:` reads `UNKNOWN`, the message names it an infra condition pointing at the daemon/credentials/transcript, and the terminal `RESULT:` is `FAIL`

#### Scenario: A zero exit records the key as PASS and never rescues another check
- **WHEN** the reviewer process exits zero
- **THEN** `roborev-exit:` reads `PASS`, `findings:` reads `NONE` (or `PRESENT (<n>)` when severity markers are present), and that key alone never turns any other check's FAIL into a pass

#### Scenario: A clean verdict beside findings markers is INCONSISTENT and fails
- **GIVEN** a job whose structured `verdict` says the review was clean while its findings block carries one severity marker
- **WHEN** the wrapper evaluates the findings state
- **THEN** `findings:` reads `INCONSISTENT (verdict clean, 1 findings marker(s))`, the terminal `RESULT:` is `FAIL`, the message states that one of the two must be wrong, and the value does not exempt tier 1

#### Scenario: A zero exit beside findings markers, with no structured verdict, is INCONSISTENT
- **GIVEN** a reviewer that exited 0 while the findings block carries a severity marker, and a job record with no structured verdict to arbitrate
- **WHEN** the wrapper evaluates the findings state
- **THEN** `findings:` reads `INCONSISTENT (exit 0, 1 findings marker(s))` and the terminal `RESULT:` is `FAIL`

#### Scenario: A quoted severity token outside the findings block does not manufacture PRESENT
- **GIVEN** a transcript that mentions a severity token in prose outside the findings block
- **WHEN** the wrapper derives the findings state
- **THEN** the state comes from the structured verdict (or the exit code), the out-of-block mention does not set `PRESENT`, and it therefore cannot exempt a vacuity claim from tier 1

#### Scenario: A pre-invocation failure leaves the reviewer's status SKIPped, not passed
- **GIVEN** a run that fails its push assert or census before the reviewer process is started
- **WHEN** the wrapper emits its block
- **THEN** `roborev-exit:` reads `SKIP`, which can never be mistaken for a pass

### Requirement: The wrapper emits a machine-greppable summary block with a terminal verdict
The wrapper SHALL emit a single compact `==== ROBOREV REVIEW SUMMARY ====` block on every **VERDICT**
exit path — a pass, any failed check, or an empty census — carrying one field per line, in a FIXED
order that is part of the contract, under the greppable keys: `repo:`, `branch:`, `base:`, `head-sha:`,
`reviewed-sha:`, `job:`, `model:`, `census:`, `tokens:`, `push-assert:`, `census-check:`, `code-free:`,
`census-exclusion:`, `job-record:`, `sha-assert:`, `review-completed:`, `prompt-content:`,
`vacuity-tier1:`, `vacuity-tier2:`,
`findings:`, `roborev-exit:`, `log:`, and a terminal `RESULT: PASS|FAIL|NOTHING-TO-REVIEW`.
`census-exclusion:` SHALL sit immediately after `code-free:`, mirroring its pre-enqueue evaluation order,
and SHALL appear EXACTLY ONCE.
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

### Requirement: The wrapper fails closed when any of its own sourced helpers is unavailable
The implementation SHALL be **FIVE files** — the wrapper, TWO sourced shell helpers (the local oracles:
push assert + census/code-free; and the per-review checks: review-completed, prompt-content, findings,
both vacuity tiers), a python job-facts extractor, and the hermetic regression check — and for **BOTH**
sourced helpers a MISSING or TRUNCATED file SHALL FAIL CLOSED with a named cause rather than silently
reducing its checks to no-ops. An absent helper would leave every key it owns reading `SKIP`/`PASS` beside
a `RESULT: PASS`, which is a worse failure than any this guard was built to catch: the completeness test
SHALL therefore be that each REQUIRED FUNCTION the file must define is actually defined, not merely that
the file exists.

**Both helpers SHALL be validated BEFORE the review is invoked**, so a broken installation costs no review
(the checks helper's functions are only CALLED later, once the job facts exist). Helper paths SHALL be
resolved relative to the wrapper's OWN file location, never `$PWD`, because the wrapper is invoked from
arbitrary worktrees. Likewise, an absent `roborev` binary, an unresolvable HEAD, and any other precondition
failure SHALL fail closed with a named cause and SHALL NOT report a pass.

#### Scenario: A missing or truncated oracles helper fails closed
- **GIVEN** a checkout in which the sourced oracles helper is missing, and separately one in which it is present but truncated so it does not define both oracle functions
- **WHEN** the wrapper runs
- **THEN** both cases exit non-zero with `RESULT: FAIL` and a message naming the helper and stating that the push assert and the census cannot run, and neither reports a pass with those checks silently disabled

#### Scenario: A missing or truncated checks helper fails closed before any review is enqueued
- **GIVEN** a checkout in which the sourced per-review-checks helper is missing, and separately one truncated so one of its five required functions is undefined
- **WHEN** the wrapper runs
- **THEN** both exit non-zero with `RESULT: FAIL`, the message names the helper and the specific missing function, NO review is enqueued (the validation happens before the invocation, so a broken install costs no review), and neither reports a pass with those five checks silently disabled

#### Scenario: An absent roborev binary or an unresolvable HEAD fails closed
- **GIVEN** a PATH with no `roborev` binary, and separately a repository with no commits
- **WHEN** the wrapper runs
- **THEN** each exits non-zero with `RESULT: FAIL` naming the cause (the absent binary; no commit to review), and no review is enqueued

### Requirement: Every roborev call site and doctrine surface routes through the sanctioned wrapper
Every roborev invocation documented anywhere in the delivery pipeline's agent surfaces, commands,
skills and doctrine SHALL be expressed as a call to `scripts/flow/roborev-review.sh`, and NO surface SHALL
document a DIRECT `roborev` CLI invocation as sanctioned — specifically the flag-only `--branch` form (i.e.
without an explicit `--repo`) and the two-positional commit-range form SHALL be marked NON-SANCTIONED
wherever they are named. Because the wrapper's INTERNAL invocation form is the wrapper's own business, a
call site SHALL NOT prescribe the arguments the WRAPPER passes to roborev (it may mark a direct-CLI form
non-sanctioned; it may not specify the sanctioned one). The corrected, measured statement of which forms
are sanctioned lives on the doctrine surfaces enumerated in the next requirement, so a later change to the
wrapper's internal form can never leave sixteen surfaces stale. **The migrated set is SIXTEEN surfaces** — thirteen under `.claude/**`
plus three non-`.claude` doctrine surfaces — carrying THREE different obligations, because some of them
contain no roborev invocation at all and an obligation to "invoke the wrapper" would be unsatisfiable
for those. (Two further surfaces, CLAUDE.md and the published `roborev-findings` page, are covered by
the doctrine requirement below, for eighteen surfaces referencing the wrapper in total.)

**(a) Invocation sites (9)** — surfaces whose documented procedure runs or prescribes the wrapper. Each
SHALL express its roborev step as a call to `scripts/flow/roborev-review.sh`, SHALL pass BOTH the
reviewer agent and the reviewer model, and SHALL NOT instruct a bare `roborev review --branch` nor the
two-positional commit-range form. They subdivide by what the surface itself does:

- **Review-round sites (4)** — they run a review round in-line: `.claude/skills/flow-implement/SKILL.md`
  (review-first, the primary call site), `.claude/agents/flow-closer.md` (the final merge-gating
  confirmation pass), `.claude/skills/flow-address/SKILL.md` (the post-comment re-review), and
  `.claude/commands/worker.md` (the fleet's UNATTENDED entry point, which runs the implement loop's
  review-first step itself). Each SHALL ADDITIONALLY state that the branch is pushed BEFORE the review
  is requested, and SHALL treat ANY non-PASS terminal `RESULT` — `NOTHING-TO-REVIEW` INCLUDED — as a
  failed review round and a blocked merge, never as "roborev clean".
- **Prescribing sites (5)** — they name the wrapper as the invocation to be used without running a
  round in-line: `.claude/agents/flow-lead.md` (the stage table and the roborev doctrine bullet),
  `.claude/skills/ci-cd-validation/SKILL.md` and `.claude/skills/ci-cd-validation/merge-process.md`
  (the merge-readiness definition), `.claude/skills/flow-activate/SKILL.md` (the roborev step of the
  `tasks.md` it authors), and `.claude/commands/manager.md` (which defines what "roborev clean" means
  for the workers it dispatches). Each SHALL name the wrapper as the ONLY sanctioned invocation, and any
  merge-readiness or finalizability rule it states SHALL require a terminal `RESULT: PASS` and SHALL
  NOT accept `NOTHING-TO-REVIEW` or `FAIL`.

**(b) Non-invoking surfaces (4)** — surfaces that reference roborev (the `roborev-lints` gate
component, the pre-roborev self-check pointer, the telemetry `--roborev-findings` counter) but contain
NO roborev invocation: `.claude/skills/flow-finalize/SKILL.md`, `.claude/agents/rust-reviewer.md`,
`.claude/agents/sstable-developer.md`, `.claude/agents/test-validator.md`. Each SHALL state explicitly
that it never invokes roborev directly, SHALL point at `scripts/flow/roborev-review.sh` as the only
sanctioned invocation, and SHALL NOT contradict any of the four doctrine rules (wrapper-only; verify
the reviewed SHA; a "contains no code changes to review" verdict on a non-empty diff is a HARD FAIL; a
docs-only diff cannot be roborev-certified). `.claude/agents/rust-reviewer.md` SHALL ADDITIONALLY
require that a diff reintroducing a bare `roborev review --branch` or the two-positional range form is
flagged as a **BLOCKER**.

**(c) Non-`.claude` doctrine surfaces (3)** — the fleet-facing prose that prescribes how roborev is
run: `website/src/content/docs/agents-developing/delivery-pipeline.md`,
`docs/development/pm-operating-loop.md`, `docs/development/agent-machine-setup.md`. Each SHALL name the
wrapper as the only sanctioned invocation with BOTH flags required, SHALL state push-first, and SHALL
state that any non-PASS terminal `RESULT` (`NOTHING-TO-REVIEW` included) is a failed round and a blocked
merge. These are NOT optional extras: each previously carried the INVERSE instruction — "roborev follows
this machine's configured agent … run it with no `--agent`/`--model` flags", with
`delivery-pipeline.md` calling explicit agent/model "never doctrine" — which directly contradicts the
amended CLAUDE.md rule, so leaving them unmigrated would leave the fleet's published guidance
prescribing the very invocation this change forbids.

No surface in any class SHALL document a bare `--branch` or two-positional-range roborev invocation as
sanctioned. A historical quotation of a superseded command in design/spec prose SHALL be marked as
historical so it cannot be copied as guidance. (`.claude/hooks/issue-gate.sh` is deliberately NOT in
this set: it documents that no hook path runs roborev at all (#2671) and contains no invocation to
migrate.)

#### Scenario: All sixteen migrated surfaces route through the wrapper and none documents a bare --branch invocation
- **WHEN** the sixteen migrated surfaces — the thirteen under `.claude/**` (`skills/flow-implement`, `agents/flow-closer`, `skills/flow-address`, `commands/worker`, `agents/flow-lead`, `skills/ci-cd-validation/SKILL.md`, `skills/ci-cd-validation/merge-process.md`, `skills/flow-activate`, `commands/manager`, `skills/flow-finalize`, `agents/rust-reviewer`, `agents/sstable-developer`, `agents/test-validator`) plus `website/src/content/docs/agents-developing/delivery-pipeline.md`, `docs/development/pm-operating-loop.md` and `docs/development/agent-machine-setup.md` — are inspected for roborev invocations
- **THEN** every one of them names `scripts/flow/roborev-review.sh` as the sanctioned invocation, each of the nine class-(a) sites expresses its roborev step as a wrapper call passing both the reviewer agent and the reviewer model, none instructs a bare `roborev review --branch` invocation or the two-positional commit-range form as sanctioned, and the bare `--branch` form is explicitly marked non-sanctioned wherever it appears

#### Scenario: Each review-round site states push-first and treats any non-PASS RESULT as a failed round
- **WHEN** `.claude/skills/flow-implement/SKILL.md`, `.claude/agents/flow-closer.md`, `.claude/skills/flow-address/SKILL.md` and `.claude/commands/worker.md` are inspected
- **THEN** each states that the branch is pushed before the review is requested, and each states that any non-PASS terminal `RESULT` — `NOTHING-TO-REVIEW` included — is a failed review round and a blocked merge rather than "roborev clean"

#### Scenario: Each prescribing site names the wrapper and requires RESULT PASS for readiness
- **WHEN** `.claude/agents/flow-lead.md`, `.claude/skills/ci-cd-validation/SKILL.md`, `.claude/skills/ci-cd-validation/merge-process.md`, `.claude/skills/flow-activate/SKILL.md` and `.claude/commands/manager.md` are inspected
- **THEN** each names `scripts/flow/roborev-review.sh` as the only sanctioned invocation with both flags, and every merge-readiness or finalizability rule any of them states requires a terminal `RESULT: PASS` and rejects both `NOTHING-TO-REVIEW` and `FAIL`

#### Scenario: Each non-invoking surface says so and points at the wrapper
- **GIVEN** the four class-(b) surfaces, whose only roborev references are the `roborev-lints` gate component, the pre-roborev self-check pointer, and the telemetry `--roborev-findings` counter
- **WHEN** `.claude/skills/flow-finalize/SKILL.md`, `.claude/agents/rust-reviewer.md`, `.claude/agents/sstable-developer.md`, and `.claude/agents/test-validator.md` are inspected
- **THEN** each states that it never invokes roborev directly, each points at `scripts/flow/roborev-review.sh` as the only sanctioned invocation, none contradicts any of the four doctrine rules, and `.claude/agents/rust-reviewer.md` additionally requires flagging a reintroduced bare `--branch` or two-positional range form as a BLOCKER

#### Scenario: The three non-.claude doctrine surfaces no longer prescribe the inverse rule
- **GIVEN** that `website/src/content/docs/agents-developing/delivery-pipeline.md`, `docs/development/pm-operating-loop.md` and `docs/development/agent-machine-setup.md` previously instructed running roborev with the machine's configured agent and NO `--agent`/`--model` flags, with one of them calling explicit agent/model "never doctrine"
- **WHEN** they are inspected after this change
- **THEN** none of them still carries that instruction, each names the wrapper with both flags required and push-first, and each states that any non-PASS terminal `RESULT` (`NOTHING-TO-REVIEW` included) is a failed round and a blocked merge

#### Scenario: The merge-gating confirmation pass routes through the wrapper
- **GIVEN** the `flow-closer` agent's final roborev confirmation pass, whose verdict gates arming auto-merge
- **WHEN** that step is inspected
- **THEN** it invokes the sanctioned wrapper and treats a non-PASS terminal `RESULT` (including `NOTHING-TO-REVIEW`) as a blocked merge rather than a clean review

#### Scenario: Both agent and model remain required at every invocation site
- **WHEN** each class-(a) invocation site is inspected
- **THEN** it passes both the reviewer agent and the reviewer model, preserving the documented trap that supplying only one inherits a mismatched model from the repository roborev config and fails as a silent-looking review outage

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
   exclusions — it makes NO code/non-code judgement.** The earlier claim that roborev "excludes non-code
   paths from the diff it builds" is **FALSIFIED and SHALL NOT be restated anywhere**: under a configured
   `docs/**` the same mechanism discarded 33 EXECUTABLE harness files on PR #3222
   (`prompt-content: FAIL (136/136 code census paths absent)`, 15,443 input / 89 output tokens). So for a
   markdown-only diff the constructed diff is genuinely EMPTY — because `*.md` is CONFIGURED — and
   "contains no code changes to review" is a TRUTHFUL report of an
   empty input rather than a reviewer malfunction. Doctrine SHALL state that mechanism, that the
   wrapper's `prompt-content:` check therefore covers the CODE subset of the census, and that the
   deterministic pre-enqueue `code-free:` FAIL plus the `census-exclusion:` reconciliation are the correct
   responses.

**Doctrine SHALL NOT imply that everything under `docs/` is code-free** (#3229). Every surface stating the
docs-only rule SHALL name the `docs/reports/*-artifacts/` harness convention EXPLICITLY as executable code
that IS reviewed, SHALL state that "docs-only" means a code-free CENSUS rather than a directory prefix, and
SHALL name `census-exclusion:` as the pre-enqueue key that FAILs when the configured exclusion set would
swallow census code. Beyond CLAUDE.md and the `roborev-findings` page the surfaces SHALL include
`website/.../agents-developing/delivery-pipeline.md`, `.claude/agents/flow-lead.md`,
`.claude/agents/flow-closer.md`, `.claude/skills/flow-implement/SKILL.md`, and the header comments of all
three `scripts/flow/roborev-review*.sh` files.

Where doctrine documents the summary block it SHALL carry the `job-record:` key, the `census-exclusion:`
key in its contracted position immediately after `code-free:`, and the corrected
`prompt-content:` values (an unretrievable prompt FAILS; there is no non-failing `UNAVAILABLE` for that
key). Where doctrine documents the live probe it SHALL state the expectation in the RANGE form — the
`reviewed-sha:` range's HEAD endpoint equals the worktree HEAD and its base equals the base ref — never as
`reviewed-sha` equalling the worktree HEAD.

#### Scenario: Both AC4 doctrine surfaces carry all four rules
- **WHEN** CLAUDE.md and `website/src/content/docs/agents-developing/roborev-findings.md` are inspected after this change
- **THEN** both state that the wrapper is the only sanctioned invocation, that the reviewed scope must be verified against the census range, that a "contains no code changes to review" verdict on a non-empty diff is a HARD FAIL, and that a docs-only diff cannot be roborev-certified

#### Scenario: Every rule-stating surface carries the three measured corrections
- **WHEN** CLAUDE.md, `roborev-findings.md`, `delivery-pipeline.md`, `docs/development/pm-operating-loop.md` and `docs/development/agent-machine-setup.md` are inspected
- **THEN** none of them still forbids `--branch` unconditionally (each names the non-sanctioned form as `--branch` WITHOUT an explicit `--repo`), each names the single-SHA form as a partial review, and the roborev-findings page records that roborev drops exactly the paths its configured `exclude_patterns` match rather than making a code/non-code judgement

#### Scenario: The live-probe expectation is stated in the range form
- **WHEN** the doctrine page's live worktree probe section is inspected
- **THEN** it asks the reader to confirm the `reviewed-sha:` RANGE — its HEAD endpoint equal to the worktree branch HEAD and its base equal to the base ref — rather than a `reviewed-sha` equal to the worktree HEAD, which the range value can never satisfy

#### Scenario: The mechanized-in-lite table lists the new guard
- **WHEN** the `roborev-findings` page's table of classes mechanized in `--lite` is inspected
- **THEN** it carries a row for the vacuous-review class naming the hermetic regression check and the components it runs in

#### Scenario: Publication is accepted by the served content, not a status code
- **WHEN** the published `agents-developing/roborev-findings` page is verified after deployment
- **THEN** acceptance is established by fetching the page and matching a distinctive phrase introduced by this change, and an HTTP 200 without that phrase is treated as not-yet-published rather than as done

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

The check SHALL also pin the block's key ORDER, the distinctness of its header from all three
agent-gate summary headers, the usage-error path emitting no block, and hermeticity itself. It SHALL be
registered in the agent gate's shell-tooling component set such that it runs in the fast `--lite` loop
as well as the full gate, so a regression FAILs the fast loop rather than costing a review round. The
check SHALL contain no wall-clock threshold assertion in its correctness path, and SHALL report a loud
SKIP rather than a silent pass when an optional prerequisite for a subset of cases is unavailable.

#### Scenario: Every trigger class is asserted against the block's own keys
- **WHEN** the regression check runs
- **THEN** it asserts each of the classes (a) through (o) above against the wrapper's terminal `RESULT`, its per-check key values and its exit code, and it reports an explicit pass/fail tally so a partial run cannot read as a pass

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

### Requirement: A documented live worktree probe proves the worktree's HEAD is what gets reviewed
The change SHALL include a documented live probe, runnable against the real roborev binary from inside a
real issue worktree, that proves a worktree-launched review reviews the WORKTREE's HEAD rather than the
root checkout's commit. The probe SHALL be documented rather than executed by the gate, because it
requires network access and a live reviewer. Its procedure and expected summary-block values SHALL live
in the wrapper's own `--help` output (so the two cannot drift apart) and in the doctrine page, and SHALL
include re-running it after any roborev version bump.

The probe's PASS condition SHALL be that the reviewed scope **covers the worktree HEAD**: with the
sanctioned range invocation, `reviewed-sha:` is `<base40>..<head40>`, so the assertion is on the range's
HEAD ENDPOINT equalling the worktree branch HEAD (and `sha-assert: PASS`, which the wrapper only reaches
when BOTH endpoints match). A `reviewed-sha` that is the base ref alone means the explicit-`--repo`
invocation did not defeat the root-checkout resolution.

#### Scenario: The probe establishes that the reviewed range covers the worktree HEAD
- **GIVEN** a real issue worktree, on its own branch, with its implementation commit pushed, while the root checkout sits on `main`
- **WHEN** the documented probe runs the wrapper from inside that worktree
- **THEN** the block reports `sha-assert: PASS` with a `reviewed-sha:` range whose HEAD endpoint is the worktree branch's HEAD sha and which is not the base ref alone, demonstrating that the explicit-`--repo` invocation defeats the root-checkout resolution trigger

#### Scenario: The probe is documented, not gate-run
- **WHEN** the agent gate's component set is inspected
- **THEN** the live probe is not among its components, and the probe's procedure and expected summary-block values are recorded in the wrapper's `--help` usage documentation and the doctrine page instead

