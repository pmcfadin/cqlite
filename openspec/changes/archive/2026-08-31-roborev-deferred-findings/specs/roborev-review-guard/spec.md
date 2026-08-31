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

A marker **ATTEMPT** SHALL be recognised as the marker's **stem followed by whitespace OR by the end
of the line** — never the stem plus a mandatory trailing space. A marker-only comment reading exactly
the stem (`roborev-defer: findings`, `roborev-waive: prompt-content-absent`) is a truncated
authorization someone plainly meant to write, so it SHALL be `MALFORMED` and SHALL NOT be reported as
if no authorization existed: a **fail-quiet on an attempted authorization** sends the author to
re-read syntax they typed correctly and to conclude the mechanism is broken. The token boundary SHALL
still be tested rather than dropped, so a different word (`roborev-defer: findingsfoo`) is not an
attempt. This rule SHALL hold for **both** marker kinds, expressed once and inherited by call.

#### Scenario: A marker-only comment that is the bare stem
- **WHEN** the sole nonblank content of a top-level comment from an allowlisted author is exactly the marker's stem, with or without a trailing newline, for either marker kind
- **THEN** the run reports that kind's `MALFORMED` state and `RESULT: FAIL`, and never reports `NONE`

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

**Disposition.** Each issue number in `issues=` SHALL be an **OPEN** issue GitHub confirms, and that
check SHALL be **four-valued**: an issue GitHub answers does not exist SHALL leave the run FAILing
under `deferral: ISSUE-ABSENT (…)`; an issue GitHub answers is **CLOSED** SHALL leave the run FAILing
under `deferral: ISSUE-CLOSED (…)`; an issue whose existence could **not be asked** — no `gh`,
no auth, a network or API failure, an unparseable payload, or any diagnostic that does not say the
issue is missing — SHALL leave the run FAILing under `deferral: ISSUE-UNVERIFIABLE (…)`, each a
**textually distinct** state. A could-not-ask SHALL NEVER be read as verified-present, and only a
payload affirmatively naming that issue's number **and an OPEN state** SHALL count as present. `gh
issue view` exits 1 for BOTH a missing issue and an auth failure (measured on gh 2.98.0), so the two
SHALL NOT be distinguished by exit code; where they cannot be told apart, the verdict SHALL be the
could-not-ask. The non-granting states are separate because they are **different operator actions**
("that issue number is wrong" / "that issue is closed" / "this box cannot reach GitHub"). A deferral
naming an issue that does not exist is a dropped finding wearing a link.

**OPEN IS DELIBERATELY STRONGER THAN "RETRIEVABLE", AND THE STRENGTHENING IS THE POINT.** The lead's
literal condition said *retrievable*, and a CLOSED issue is retrievable: `gh issue view` returns its
number and exits 0. So a number-only test made "the finding is tracked" satisfiable by an issue closed
as a duplicate three weeks ago — `present` ⇒ `GRANTED` ⇒ `RESULT: PASS`, the finding permanently
untracked while the block asserted it was filed. Every other statement of this leg, here and in the
implementation, claims it enforces **not-dropped**; a closed-as-duplicate issue means the finding IS
dropped, so the claim is made TRUE rather than three statements of it weakened to match a weaker
implementation. A false refusal is recoverable — reopen the issue, or file a fresh tracking issue and
re-authorize with its number — and is the fail-closed direction.

**The disposition backstop SHALL be AFFIRMATIVE.** It SHALL count the verifications actually
**performed** and require that count to EQUAL the number of **declared** `issues=` fields; it SHALL
NOT test the issue-list string for non-emptiness. A non-emptiness test is satisfiable by a list the
split does not traverse (`,` splits into ZERO words), which leaves a grant standing with no `gh issue
view` executed at all — and its unreachability depends on the `issues=` **pattern** still forbidding
that value, which is precisely the upstream dependency a backstop must not have.

**SUPERSEDED — the PR-body reference requirement is REMOVED (lead ruling, option A).** This
requirement previously ALSO demanded that each `issues=` number be **referenced from the
pull-request body** as a local, visible `#N`, with `deferral: PR-UNLINKED (…)` otherwise, and it
carried scenarios for cross-repository references, alphanumeric suffixes, fenced blocks, code spans,
HTML comments and a declared 4-space-indent residual. **That leg is deleted, not weakened**, and it
SHALL NOT be reinstated. The requirement text is superseded **in place** rather than removed silently,
because the reason is the durable part:

1. **THE ARTIFACT WAS THE WRONG ONE.** A PR body is **editable at any time by anyone with write
   access, with NO per-edit attribution**. A top-level comment is **permanent and attributable**. So
   the body-link leg was the **weaker** artifact of the two, and it would stay weaker **even if
   Markdown parsed trivially**: an authorization that the constrained party can silently rewrite after
   it is granted evidences nothing. The Markdown-recogniser problem was a **symptom**, not the cause.
2. **THE WORDING INVITED THE MISTAKE.** "Name where the finding went" invited a **prose scan**, when
   the property actually wanted is that the finding is **TRACKED** — which retrievability enforces and
   a sentence in a body never did.
3. **THE RECOGNISER CLASS DID NOT CLOSE.** Markdown-handling references in the one predicate went
   **0 → 11** across two review rounds. Round 1 closed five shapes (cross-repository, `#Nsuffix`,
   fenced block, HTML comment, single-backtick span); round 2 then found **two more** — a
   multi-backtick span ``` ``#3602`` ``` and an explicit link `[#3602](https://example.com)` — with
   GFM autolinks, reference-style links, raw HTML, entity references and nested emphasis unhandled by
   any generation, and the 4-space-indent case already a declared residual. Per #3312 (*remove the
   shared channel, do not pick a rarer delimiter*) and #3229's owner ruling (*a guard with known
   documented false-PASSes is worse than no guard, because it invites reliance it cannot support*),
   the leg is removed.

**Subtraction cannot introduce a false PASS**: with nothing predicted about the PR body, nothing is
excused by it. The property is now carried by three legs — (1) the marker **names** the issue numbers,
on the permanent, attributable, allowlisted comment channel; (2) each named issue must be
an **OPEN** issue, four-valued as above, which is the leg that enforces **not-dropped**; (3) the
summary block **records** the numbers, the count, the scope and the reason verbatim. Any future
strengthening of the disposition SHALL come from an **immutable or attributed** artifact, never from
parsing the mutable body of the pull request under review.

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

#### Scenario: The authorization names an issue GitHub says does not exist
- **WHEN** an `issues=` number is answered by GitHub as not existing in this repository
- **THEN** the run reports `deferral: ISSUE-ABSENT (…)` and `RESULT: FAIL` — an issue that does not exist fails closed rather than being skipped, and the remedy named is the marker or the missing issue

#### Scenario: The authorization names an issue GitHub says is CLOSED
- **WHEN** an `issues=` number is answered by GitHub as existing but `CLOSED`, every other part of the authorization being perfect
- **THEN** the run reports `deferral: ISSUE-CLOSED (…)` and `RESULT: FAIL`, textually distinct from both `ISSUE-ABSENT` and `ISSUE-UNVERIFIABLE`, naming the issue, its state, and the recoverable remedy (reopen it, or file a fresh tracking issue and re-authorize) — a closed issue tracks nothing, so a deferral to one is the finding being dropped with a link attached

#### Scenario: A granted deferral declares more issue fields than were verified
- **WHEN** the disposition leg is reached with an issue list whose comma-separated fields do not each traverse to a verification (for example a comma-only list, or one carrying an empty field)
- **THEN** the run reports `deferral: UNAVAILABLE (…)` naming how many fields were declared and how many were affirmatively verified, and `RESULT: FAIL` — a grant requires as many verifications as declared fields, never merely a non-empty string

#### Scenario: The existence of a named issue could not be asked
- **WHEN** `gh issue view` fails with a diagnostic that does **not** say the issue is missing (for example `HTTP 401: Bad credentials`), every other part of the authorization being perfect
- **THEN** the run reports `deferral: ISSUE-UNVERIFIABLE (…)` and `RESULT: FAIL`, textually distinct from `ISSUE-ABSENT`, carrying the diagnostic and directing the operator at the network rather than at the marker — a could-not-ask is never read as verified

#### Scenario: The PR body is not consulted at all
- **WHEN** a granted, matching, count-equal authorization names retrievable issues and the pull-request body mentions none of them
- **THEN** the run still reports `deferral: GRANTED (…)`, `findings: DEFERRED (…)` and `RESULT: PASS` — the body is evidence for nothing, because it is editable without attribution (see the superseded requirement above)

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
`ISSUE-ABSENT` / `ISSUE-CLOSED` / `ISSUE-UNVERIFIABLE` / `UNAVAILABLE`. Every non-`GRANTED` value
SHALL leave the existing FAIL in place.

A `GRANTED` record SHALL name the authorizing author, the issue numbers, the count, the bound scope
(base, head, job) and the reason **verbatim**, so that the disposition of every deferred finding is
legible from a pasted block alone and the authorization is permanently attributable.

**Verbatim means verbatim.** The structured scanner SHALL NOT rewrite internal whitespace in a
recorded value: repeated spaces and tabs SHALL survive byte-for-byte. The only transformation
permitted is at the **block boundary**, which SHALL render a control character as a **visible**
escape so that no value can span lines — a value occupying one line is the property actually required,
and whitespace collapsing is not it.

**A reason SHALL NOT contain either marker stem** (`roborev-waive`, `roborev-defer`). A granted reason
is interpolated into the summary block, and no emitted diagnostic may carry any part of a marker form.
It is REFUSED rather than escaped, because an authorizer has no legitimate need for one; the structural
assert covers the *code*, while a *runtime* reason can inject what no source scan sees — an invariant
over OUTPUT needs a check on the OUTPUT PATH.

**AND THE RULE IS OVER *EVERY* EMITTED VALUE, NOT OVER THE `reason` FIELD.** The reason is the field
an authorizer chooses, so refusing it removes that class outright; but a marker keyword can reach a
diagnostic through fields nobody chooses — the **GitHub login** of an unauthorized commenter (which
`UNAUTHORIZED` must report in order to say who was refused), **`gh issue view`'s stdout and stderr**
(which reach `deferral:` as an `ISSUE-UNVERIFIABLE` cause), the allowlist, and any value a future key
interpolates. So each of the two processes SHALL neutralise the keywords at its **one emit boundary** —
the structured scanner where every `key=value` leaves it, and the wrapper where every block value and
every DETAILS line is already rendered — and SHALL NOT do it per interpolation site, which is a list to
keep complete. There, unlike the reason, the value is **REDACTED rather than refused**: it is an
identity or a diagnostic the run must still report.

The keyword SHALL be neutralised only where it is **not continued by another letter**: a longer word is
a different word — the same rule the parser already applies to `roborev-defer: findingsfoo` — and the
boundary is load-bearing, because the scanner's own file name embeds a keyword and is printed by the
fail-closed `waiver: UNAVAILABLE (… tool: <path>)` cause that an operator must read to act. A value
carrying a keyword inside a longer word is a **declared residual**: it carries no marker *form*.

This is spec conformance and invariant coverage, **not** a security layer, and SHALL NOT become one: a
GitHub login admits letters, digits and hyphens and not colons or spaces, so it cannot hold a full
stem, and an emitted line begins `deferral: UNAUTHORIZED (`, which the sole-content rule refuses. It is
safe as a **display-only** transformation precisely because every authorization decision — allowlist,
scope, count, retrievability — SHALL be made on the **raw** value before any renderer runs; so the two
boundaries can only ever redact differently, never grant.

**Both markers' `base=`/`head=` fields SHALL be exactly 40 hex.** An abbreviated sha SHALL report
`MALFORMED`, never `STALE`: it names THIS review in a spelling the form does not permit, and an
authorizer sent to re-check *which review* they named will find nothing wrong with it. The rule holds
for both kinds together — they share one parser, and a field rule that holds for one marker and not
the other is a divergence in a channel rule.

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

#### Scenario: An unauthorized author's login carries a marker keyword
- **WHEN** the sole nonblank content of a top-level comment is a well-formed marker of either kind naming this review, from a NON-allowlisted author whose GitHub login contains `roborev-waive` or `roborev-defer`
- **THEN** the state is `UNAUTHORIZED`, the emitted cause names the author with the keyword redacted and the rest of the login intact, no emitted diagnostic carries any part of a marker form, and `RESULT: FAIL`

#### Scenario: A `gh` diagnostic carries a marker keyword
- **WHEN** `gh issue view` fails with a diagnostic containing a marker keyword
- **THEN** `deferral: ISSUE-UNVERIFIABLE (…)` still quotes the diagnostic it could not interpret, with the keyword redacted by the wrapper's own emit boundary

#### Scenario: An ordinary value is untouched
- **WHEN** a deferral is granted by an allowlisted author with an ordinary login
- **THEN** the block records the author verbatim and no value is redacted

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
`STALE`, `MALFORMED`, `UNAUTHORIZED`, `COUNT-MISMATCH`, `ISSUE-ABSENT`, `ISSUE-CLOSED`,
`ISSUE-UNVERIFIABLE`,
`UNAVAILABLE`, plus the non-deferrable `UNKNOWN`/`SKIP` states, the sole-content refusals (indented,
quoted, bulleted, mid-sentence, fenced, HTML-wrapped), the diagnostic-is-not-a-credential property,
the affirmative-count backstop, the abbreviated-sha `MALFORMED` verdict for **both** marker kinds,
the verbatim recording of a reason carrying repeated spaces and a tab, the refusal of a stem-bearing
reason for **both** kinds, the **redaction of a keyword-bearing GitHub login for both kinds** and of a
keyword-bearing `gh` diagnostic (the wrapper boundary, which the login cases do not reach), the
**preservation of the scanner's own file name** in the `UNAVAILABLE` cause, and the separate-scoping
pair. Every case whose subject is a leg that can
GRANT SHALL carry a **planted-defect contrast** — the naive form of that leg, applied to a scratch
copy, granting the fixture the real code refuses, with the unpatched copy's refusal measured FIRST. Each case SHALL plant its artifacts in its **own scratch copy of the
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
