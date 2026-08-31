# roborev-review-guard — delta for roborev-deferred-findings (issue #3626)

**Architecture note (read this first).** `scripts/flow/roborev-review.sh` is the only sanctioned
roborev invocation, and its `==== ROBOREV REVIEW SUMMARY ====` block is the verdict a merge rests on.
Since **#3586** a would-be `PASS` requires `findings:` to reduce token-exactly to `NONE` **in every
mode including `--recheck-job`**, and that requirement is *not waivable* — correctly, because
delegating a key's failure to its neighbour is a latent false pass. The consequence nobody designed
for: a **lead-deferred** finding is re-reported by every later round, so `findings: PRESENT (n)`
persists, `RESULT` stays `FAIL`, and the doctrine rule *"any non-PASS terminal `RESULT` … is a blocked
merge"* blocks the merge **forever**. Measured on PR #3572 job 262: two findings, **zero new**, both
already filed (#3602, #3613) and both already lead-deferred, 5.9M input tokens, every deterministic
key PASS — and the merge required an out-of-band lead comment. This delta makes the deferral
mechanical, on the absence waiver's channel, without giving the constrained party the power to
satisfy its own constraint.

**Acceptance-criterion → requirement map** (issue #3626 body "What to fix" + "Constraints on any
fix", owner ruling 2026-08-30T23:59:52Z, lead brief 2026-08-31T00:47:54Z):

| AC / ruling item | Requirement(s) |
|---|---|
| Define "roborev clean" as NO UNADDRESSED FINDINGS, not "the tool printed zero" | ADDED *An affirmatively matched, authorized deferral reports DEFERRED and gates the verdict on the undeferred set* |
| Wrapper accepts a manifest of specific issue numbers | ADDED *A findings deferral is authorized only by a marker on the absence waiver's channel* |
| `findings: DEFERRED(n)` distinct from `PRESENT(n)`, never `NONE` | ADDED *An affirmatively matched, authorized deferral reports DEFERRED and gates the verdict on the undeferred set* |
| Manifest recorded in the PR and attributable | ADDED *A findings deferral is authorized only by a marker on the absence waiver's channel*; ADDED *The deferral state is reported under its own key, including when nothing was granted* |
| Scoped to named issue numbers, never a blanket switch | ADDED *A findings deferral is authorized only by a marker on the absence waiver's channel* |
| Each deferral affirmatively matched to a filed issue; absence of a match = PRESENT (#3586) | ADDED *A deferral is affirmatively matched by count and by disposition, never by absence* |
| `prompt-content` and `findings` stay separately scoped | ADDED *The absence waiver and the findings deferral are separately scoped and may not substitute for one another* |
| Lead property 1 — per-finding, not per-run | ADDED *A deferral is affirmatively matched by count and by disposition, never by absence* |
| Lead property 2 — a distinct verdict token | ADDED *An affirmatively matched, authorized deferral reports DEFERRED and gates the verdict on the undeferred set* |
| Lead property 3 — it must name where the finding went | ADDED *A deferral is affirmatively matched by count and by disposition, never by absence* |
| Lead property 4 — the gate of record is unaffected | ADDED *The deferral mechanism is confined to the roborev verdict* |
| The wrapper cannot certify itself; demonstration post-merge | ADDED *Every deferral state is pinned hermetically, and the live path is demonstrated post-merge* |
| Doctrine obligation (CLAUDE.md) | ADDED *Doctrine states what "roborev clean" now means and how a deferral is authorized* |

## ADDED Requirements

### Requirement: A findings deferral is authorized only by a marker on the absence waiver's channel

The wrapper SHALL recognise a findings deferral **only** from a **dedicated, column-zero line** that
is the **sole nonblank content** of a **top-level pull-request comment**, of exactly the form:

```
roborev-defer: findings issues=<N>[,<N>...] count=<n> base=<40-hex> head=<40-hex> job=<id> reason=<why>
```

Every field SHALL be required, the field order SHALL be enforced by one anchored pattern, and the
`reason` SHALL be trimmed **before** it is judged, so that `reason=TODO ` and whitespace-only reasons
are refused exactly as their untrimmed forms are. A `reason` that is a bare placeholder
(`why`/`todo`/`tbd`) or that still carries an **unsubstituted `<…>`** SHALL be refused.

The wrapper SHALL NOT accept a deferral from any channel the reviewed party can write in its own
name. Specifically there SHALL be **no** command-line flag, **no** file in the worktree or
repository, and **no** environment variable by which a deferral can be asserted, and
`scripts/tests/test_roborev_review_guard.sh` SHALL assert this **structurally** — behavioural cases
cover only the channels someone already thought of.

The comment author SHALL be required to appear in the hard-coded `ROBOREV_WAIVER_AUTHORS` allowlist,
which SHALL NOT be environment-overridable and SHALL NOT be read from a configuration file. The
author association SHALL be obtained by parsing `gh --json` **structurally**, so that author and body
remain separate fields of one object and there is no in-band delimiter for a comment body to forge.

The scanner that enforces this SHALL be resolved from the wrapper's own directory with **no override
and no `${…:-…}` fallback** — the constrained party must not choose its own enforcer. A test needing
a different enforcer SHALL substitute the artifact in its own scratch copy of the tree, never a path
variable, and the harness SHALL assert that no test-only seam has been reintroduced.

**No emitted diagnostic SHALL carry any part of the marker** — not even its prefix — because summary
blocks are pasted into PR comments as a matter of course in this repository, and an artifact that
describes the escape hatch must not become it. Diagnostics SHALL point at `--help` instead.

A **marker-only** comment with bad or missing fields SHALL be reported `MALFORMED`. A comment
containing the marker **plus other content** SHALL be ignored **silently** (reported `NONE`), never
`MALFORMED`: someone documenting the form never attempted an authorization, and a false accusation
reprinted on every later run is worse than silence.

#### Scenario: A well-formed marker from an allowlisted author, sole content of a top-level comment
- **WHEN** `--recheck-job <id>` runs against a findings-bearing job whose PR carries such a comment naming this base, head, job, an issue list and the observed count
- **THEN** the deferral is granted, and the run reports `deferral: GRANTED (…)`

#### Scenario: The same marker from a non-allowlisted author
- **WHEN** the marker is well-formed and names this exact review, but its author is not on the allowlist
- **THEN** the run reports `deferral: UNAUTHORIZED (…)`, distinct from `MALFORMED` because the marker was fine and the author was not, and the FAIL stands

#### Scenario: The marker is not the sole nonblank content of its comment
- **WHEN** the marker appears indented, `>`-quoted, bulleted, mid-sentence, inside a fenced block, inside an HTML `<pre>`/`<code>` element, or beside any other prose
- **THEN** no authorization is recognised, the run reports `deferral: NONE (…)` teaching both the sole-content and top-level rules, and the FAIL stands

#### Scenario: A deferral is attempted through a flag, a file, or an environment variable
- **WHEN** any such input asserts a deferral
- **THEN** no deferral is granted, and `scripts/tests/test_roborev_review_guard.sh` fails if such a channel exists in the wrapper

#### Scenario: A diagnostic is pasted back into the PR as a comment
- **WHEN** a failing run's summary block or diagnostic text is posted as a PR comment
- **THEN** it contains no part of the deferral marker, so it authorizes nothing on any later run

### Requirement: A deferral is affirmatively matched by count and by disposition, never by absence

A deferral SHALL be granted only on **affirmative** evidence. The wrapper SHALL NOT derive a grant
from the absence of a contrary signal.

**Scope binding.** The marker's `base`, `head` **and** `job` SHALL all be verified against the review
under decision, exactly as the absence waiver's are: `base` SHALL be the **merge-base** of the base
ref and `HEAD` (never the base ref's tip — the assert that expected the tip failed deterministically
on correct reviews of any branch whose base had advanced), `head` SHALL be the branch head, and
`job` SHALL be the specific job whose verdict is being decided. A push, a different base, or a re-run
SHALL each require a fresh authorization. The job SHALL be named **explicitly** and SHALL NOT be
resolved from base+head, or a re-run could inherit an authorization written for a different review.

**Count matching.** The marker's `count=<n>` SHALL equal the **observed** findings count, and
`issues=` SHALL be non-empty. A mismatch SHALL leave the run FAILing under
`deferral: COUNT-MISMATCH (…)`. This is what makes the match affirmative rather than permissive: a
marker written before its job's findings were read, and any **new** finding arriving at the same
head, each raise or lower the observed count and therefore fail.

**Disposition.** Each issue number in `issues=` SHALL be an issue that (a) is **retrievable**, and
(b) is **referenced from the pull-request body**. An unretrievable issue SHALL leave the run FAILing
under `deferral: ISSUE-UNRESOLVABLE (…)`; an issue absent from the PR body SHALL leave it FAILing
under `deferral: PR-UNLINKED (…)`. A deferral without a linked issue is a dropped finding.

The PR body is authored by the **worker** — the party the disposition requirement constrains — so the
reference SHALL be recognised only as a **LOCAL** issue reference bounded on **both** sides by a
non-token, non-repository-qualifier character, and only where it appears in **visible** body content.
A cross-repository reference (`owner/repo#N`), a reference carrying an alphanumeric suffix (`#Nsuffix`)
and a reference appearing **only** inside a fenced code block, an inline code span or an HTML comment
SHALL each leave the run FAILing under `deferral: PR-UNLINKED (…)`. Every ambiguity SHALL resolve
toward **not referenced**: the remedy for a false `PR-UNLINKED` is one line in the PR body, whereas the
opposite error lets a deferred finding be dropped with no recorded disposition.

**Non-deferrable states.** `findings: UNKNOWN` and `findings: SKIP` SHALL NOT be deferrable in any
mode. Those values mean the findings state was never **established**, and a pass may not rest on a
state that could not be read; only an affirmatively measured `PRESENT (n)` SHALL be deferrable. The
wrapper SHALL NOT reconstruct a per-finding identity from the review's prose in order to match it
against an issue number — that is a recogniser over author-controlled text, the class closed by
removing prose reconstruction, and it SHALL NOT be reopened.

#### Scenario: The observed count exceeds the authorized count
- **WHEN** a granted-shaped marker declares `count=2` and the job reports three findings
- **THEN** the run reports `deferral: COUNT-MISMATCH (…)`, `findings:` remains `PRESENT (3)`, and `RESULT: FAIL`

#### Scenario: A new finding arrives at the same head under an existing authorization
- **WHEN** a later job at the same base and head reports one finding more than the authorization covers
- **THEN** the marker's `job=` no longer names this job, so nothing is granted and the FAIL stands

#### Scenario: The authorization names an issue that is not linked from the PR body
- **WHEN** `issues=` names a retrievable issue that the PR body does not reference
- **THEN** the run reports `deferral: PR-UNLINKED (…)` and `RESULT: FAIL`

#### Scenario: The PR body references the issue only in another repository, with a suffix, or in inert content
- **WHEN** `issues=` names retrievable issues and the PR body references them only as `owner/repo#N`, as `#Nsuffix`, inside a fenced code block, inside an inline code span, or inside an HTML comment
- **THEN** the run reports `deferral: PR-UNLINKED (…)` naming those issues and `RESULT: FAIL` — the constrained party may not satisfy its own disposition constraint with a reference to something else or with content the reader does not see as a link

#### Scenario: The PR body references the issue in ordinary visible prose
- **WHEN** `issues=` names retrievable issues and the PR body references each as a bare `#N` in visible text (parenthesised or sentence-final included), beside unrelated inert content
- **THEN** the disposition half is satisfied, so a granted, matching marker still reports `deferral: GRANTED (…)`, `findings: DEFERRED (…)` and `RESULT: PASS`

#### Scenario: The authorization names an unretrievable issue
- **WHEN** an `issues=` number cannot be retrieved
- **THEN** the run reports `deferral: ISSUE-UNRESOLVABLE (…)` and `RESULT: FAIL` — the unretrievable case fails closed rather than being skipped

#### Scenario: A findings state that was never established
- **WHEN** `findings:` reads `UNKNOWN` or `SKIP` and a granted-shaped marker is present
- **THEN** no deferral applies and `RESULT: FAIL`

#### Scenario: The base is asserted against the merge-base, not the base ref's tip
- **WHEN** the base ref has advanced past the branch point of a correct review
- **THEN** the scope assert still matches, because the expected base is the merge-base

### Requirement: An affirmatively matched, authorized deferral reports DEFERRED and gates the verdict on the undeferred set

When a deferral is granted and affirmatively matched, `findings:` SHALL report a **distinct token**
of the form `DEFERRED (<n>, issues=#<N>[,#<N>...], authorized @<login>, job <id>)`, and the terminal
verdict SHALL be gated on the **undeferred** set only, so `RESULT: PASS` becomes reachable.

`findings:` SHALL **never** report `NONE` on account of a deferral. `NONE` SHALL remain reachable
**only** from the job record's structured `verdict` letter, so that nobody grepping
`findings: NONE` — or `findings: PASS`-shaped text — reads a deferred run as a clean review.

`DEFERRED` SHALL be a value of the wrapper's **closed** verdict grammar: it SHALL be non-failing
**only** when the deferral oracle affirmatively granted, and an unrecognised value SHALL continue to
FAIL. Each value SHALL be reduced to its verdict **token** (up to the first space) and matched
**exactly**, never by prefix — `PASS*`-style prefix acceptance checks a spelling rather than a state.
The admission SHALL be **confined to the `findings:` key by key name**: the verdict scan SHALL carry
each key's NAME beside its value, and `DEFERRED` SHALL be non-failing for `findings` and for no other
key — this is the mechanism by which "the deferral SHALL NOT be readable by, or applicable to, any
check other than the wrapper's `findings:` key" (below) is realised, rather than resting on the
accident that no other key emits the token. The affirmation backstop (no `PASS` may carry a
verdict-carrying key that is not affirmatively passing) covers the six DETERMINISTIC keys, none of
which is `findings:`, and SHALL therefore carry **no** `DEFERRED` arm and SHALL NOT read the coupled
state at all; a deterministic key holding the token SHALL fail in the verdict scan, by key name, with
its own diagnostic. (An earlier draft required the backstop to be EXTENDED with a provenance-gated,
key-agnostic `DEFERRED` arm, by analogy with the absence waiver's. The analogy does not hold: a
waiver authorizes a PROPERTY — an absence — that only one key can ever report, whereas a deferral
authorizes a NAMED SET OF FINDINGS and confers no information about any other check. That draft
contradicted the confinement requirement below, and the confinement governs.)

The deferral SHALL be honoured **only** on `--recheck-job <id>`, which enqueues nothing: the operator
learns the job id **and** the findings only from the finished run, and re-running the wrapper to
apply a fresh authorization would enqueue a different job and stale it instantly. The block SHALL
continue to declare `MODE: recheck (…; NO review was enqueued)` and `recheck-of: <id>` as its first
keys, so a deferred `PASS` can never be pasted as evidence of a fresh clean review.

#### Scenario: A granted, matched deferral on a findings-bearing recheck
- **WHEN** `--recheck-job <id>` decides a job reporting two findings, both authorized by a matching marker
- **THEN** `findings:` reads `DEFERRED (2, issues=#…, authorized @…, job <id>)`, `deferral:` reads `GRANTED (…)`, and `RESULT: PASS`

#### Scenario: A deferred run is not greppable as clean
- **WHEN** a deferred `PASS` block is searched for `findings: NONE`
- **THEN** it does not match, because a deferral never yields `NONE`

#### Scenario: An unrecognised findings value
- **WHEN** `findings:` carries a value outside the closed grammar
- **THEN** `RESULT: FAIL`

#### Scenario: A verdict token is matched exactly, not by prefix
- **WHEN** a key's value begins with `DEFERRED` but is a different token (for example `DEFERREDX`)
- **THEN** it is not accepted as the `DEFERRED` state and `RESULT: FAIL`

#### Scenario: A deferral offered outside recheck mode
- **WHEN** a fresh review is enqueued while a matching marker exists
- **THEN** no deferral is applied to that fresh run

### Requirement: The absence waiver and the findings deferral are separately scoped and may not substitute for one another

The two authorizations SHALL remain separate mechanisms: distinct marker keywords
(`roborev-waive: prompt-content-absent` and `roborev-defer: findings`), distinct summary keys
(`waiver:` and `deferral:`), and distinct verdict tokens (`WAIVED` and `DEFERRED`). Neither SHALL be
read as, or fall back to, the other.

An absence waiver SHALL confer **no** authority over `findings:`, and a findings deferral SHALL
confer **no** authority over `prompt-content:`. A run may legitimately carry both, each granted on its
own marker and reported under its own key. Collapsing them would let a delivery-artifact waiver
excuse a real defect.

#### Scenario: An absence waiver is present and findings are reported
- **WHEN** a run carries a granted `roborev-waive: prompt-content-absent` and `findings: PRESENT (2)` with no deferral marker
- **THEN** `prompt-content:` reads `WAIVED`, `findings:` remains `PRESENT (2)`, and `RESULT: FAIL`

#### Scenario: A findings deferral is present and the prompt content is absent
- **WHEN** a run carries a granted `roborev-defer: findings` and `prompt-content:` is absent with no absence waiver
- **THEN** `findings:` reads `DEFERRED (…)`, `prompt-content:` FAILs, and `RESULT: FAIL`

### Requirement: The deferral state is reported under its own key, including when nothing was granted

The block SHALL carry a `deferral:` key whenever the findings branch had a deferral to look for, and
it SHALL state its own state even when nothing was granted, with one cause per distinguishable
operator action: `GRANTED` / `NONE` / `STALE` / `MALFORMED` / `UNAUTHORIZED` / `COUNT-MISMATCH` /
`ISSUE-UNRESOLVABLE` / `PR-UNLINKED` / `UNAVAILABLE`. Every non-`GRANTED` value SHALL leave the
existing FAIL in place.

A `GRANTED` record SHALL name the authorizing author, the issue numbers, the count, the bound scope
(base, head, job) and the reason **verbatim**, so that the disposition of every deferred finding is
legible from a pasted block alone and the authorization is permanently attributable.

The `NONE` cause SHALL teach both channel rules — **sole nonblank content** and **top-level comment**
— because a marker posted inside a review body or a review-thread reply is silently not applied, and
the run must not read as though the authorization was arbitrarily ignored.

An **unavailable** comment listing SHALL be reported `UNAVAILABLE` and SHALL leave the FAIL: where an
oracle is the sole evidence for a claim and could not be consulted, the verdict is non-passing and
its text names what was unverifiable.

#### Scenario: No marker exists at all
- **WHEN** a findings-bearing recheck runs on a PR with no deferral marker
- **THEN** `deferral:` reads `NONE (…)` naming both the sole-content and top-level rules, and `RESULT: FAIL`

#### Scenario: The comment listing cannot be retrieved
- **WHEN** the PR comment listing is unavailable
- **THEN** `deferral:` reads `UNAVAILABLE (…)` and `RESULT: FAIL`

#### Scenario: A marker naming a different job
- **WHEN** a well-formed marker names a job other than the one under decision
- **THEN** `deferral:` reads `STALE (…)` and `RESULT: FAIL`

#### Scenario: A granted record is legible from the block alone
- **WHEN** a deferral is granted
- **THEN** `deferral: GRANTED (…)` names the author, the issue numbers, the count, base, head, job, and the verbatim reason

### Requirement: The deferral mechanism is confined to the roborev verdict

The change SHALL affect **only** the roborev wrapper's verdict. `scripts/agent-gate.sh` SHALL NOT be
modified, and no gate component's behaviour SHALL change: three lanes are live on that file. The
deferral SHALL NOT be readable by, or applicable to, any check other than the wrapper's `findings:`
key, and SHALL NOT become a general "override any check" mechanism.

#### Scenario: The gate of record is unaffected
- **WHEN** this change is applied
- **THEN** `scripts/agent-gate.sh` is unmodified and no gate component consumes a deferral marker

#### Scenario: The deferral cannot excuse another check
- **WHEN** a granted deferral is present and a wrapper key other than `findings:` fails
- **THEN** that failure stands and `RESULT: FAIL`

### Requirement: Every deferral state is pinned hermetically, and the live path is demonstrated post-merge

`scripts/tests/test_roborev_review_guard.sh` — already executed by the agent gate's `tooling-tests`
component — SHALL gain a case for **every** state named above: the grant, and each of `NONE`,
`STALE`, `MALFORMED`, `UNAUTHORIZED`, `COUNT-MISMATCH`, `ISSUE-UNRESOLVABLE`, `PR-UNLINKED`,
`UNAVAILABLE`, plus the non-deferrable `UNKNOWN`/`SKIP` states, the sole-content refusals (indented,
quoted, bulleted, mid-sentence, fenced, HTML-wrapped), the diagnostic-is-not-a-credential property,
and the separate-scoping pair. Each case SHALL plant its artifacts in its **own scratch copy of the
tree**.

Because a pull request whose subject is how the wrapper reads authorizations **cannot certify
itself**, the live demonstration SHALL be planned and recorded **post-merge**, and the pull-request
body SHALL say so. A hermetic pass SHALL NOT be recorded as evidence that the live path works.

#### Scenario: A refusal state regresses
- **WHEN** any refusal state is weakened so that it grants
- **THEN** `scripts/tests/test_roborev_review_guard.sh` fails, and with it the gate's `tooling-tests` component

#### Scenario: The self-certification boundary is stated
- **WHEN** the pull request is opened
- **THEN** its body states that the wrapper cannot certify itself and that the live demonstration is post-merge

### Requirement: Doctrine states what "roborev clean" now means and how a deferral is authorized

`CLAUDE.md` and the website's `agents-developing/roborev-findings/` page SHALL be updated **in this
change** to state that **"roborev clean" means NO UNADDRESSED FINDINGS**, not that the tool printed
zero; that a lead-deferred finding is authorized by the `roborev-defer: findings` marker on the
absence waiver's channel and reported as `DEFERRED`, never `NONE`; that the two authorizations are
separately scoped; and that `UNKNOWN`/`SKIP` are not deferrable. The doctrine SHALL retain the
existing rule that any other non-`PASS` terminal `RESULT` is a blocked merge.

#### Scenario: Doctrine no longer states an unobtainable rule
- **WHEN** an agent reads the roborev doctrine after this change
- **THEN** it finds the deferral route stated, so a lead-deferred finding no longer requires an out-of-band authorization to merge
