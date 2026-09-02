# roborev-review-guard — delta for roborev-waiver-misplaced (issue #3759)

**Architecture note (read this first).** `scripts/flow/roborev-review.sh` is the only sanctioned
roborev invocation, and its `==== ROBOREV REVIEW SUMMARY ====` block is the verdict a merge rests on.
Two human authorizations can excuse one of its keys, each on the same tightly-constrained channel: the
**absence waiver** (`roborev-waive: prompt-content-absent`, #3312) and the **findings deferral**
(`roborev-defer: findings`, #3626). Both are read from **top-level pull-request comments only**. A
marker posted anywhere else is silently not applied, and the resulting `NONE` is **textually
indistinguishable from a refused or never-posted authorization**.

Measured: for PR #3710 the coordination lead granted **both** markers — field-perfect, verified
base/head/job, each the sole nonblank content of its own comment, from an allowlisted author — on
**issue #3544**, the thread where that lane's coordination happened all day. `PR #3710 comments:
roborev-waive: 0, roborev-defer: 0` / `issue #3544: both markers present, correct in every field`. So
`--recheck-job 317` would have reported `waiver: NONE` / `deferral: NONE`, the `prompt-content` FAIL
would have stood, and the lane's natural reading of `NONE` is *"the lead refused"*. **Position 1 of a
six-PR serial queue idled ~8 hours and blocked five lanes.** Two lanes diagnosed it independently, and
`lane-3544` explicitly **declined to copy the markers onto the PR itself** — on a shared login it could
have made its own merge gate pass, and correctly identified that as the exact bypass the channel exists
to prevent.

CLAUDE.md's recorded residual names two misplacement locations — *"inside a review body or a
review-thread reply"* — and **does not name the linked ISSUE thread**, which is the **most probable**
of the three, because that is where lane/lead coordination lives.

This delta adds a **distinct, NON-GRANTING** state, `MISPLACED`, for **both** kinds. `NONE` today
covers "never granted", "granted and misplaced" and "granted for a different scope", and the operator
action differs for each: a multi-state reality collapsed onto two slots where **the discarded value is
the diagnostic one**. It is a **diagnosability** change and not a loosening — nothing here permits a
marker on an issue thread to grant anything, and the author allowlist, the sole-nonblank-content rule,
the column-zero anchor and the base+head+job binding are all untouched.

The new state is **additive to two existing requirements' cause sets** (`waiver:` under *The reviewer
must demonstrably have received the census's own code files*, and *The deferral state is reported under
its own key, including when nothing was granted*) and changes no existing behaviour, so this delta is
stated entirely as ADDED requirements, as #3626's was. It needs **no change to the closed verdict
grammar**: `waiver:` and `deferral:` are informational keys, outside the verdict scan and outside the
affirmation loop, so a new value there cannot make anything pass by itself.

**Acceptance-criterion → requirement map** (issue #3759 body "Proposed" items 1–3 + "Scope note", lead
brief 2026-09-01 items R1–R8):

| AC / ruling item | Requirement(s) |
|---|---|
| Proposed 2 — a distinct diagnosable state, `waiver: MISPLACED (found on linked issue #N, not the PR)` (R1) | ADDED *A misplaced authorization is reported under a distinct, non-granting MISPLACED state, for both kinds* |
| Proposed 2 / Scope note — `MISPLACED` must NOT grant; fail-closed either way (R2) | ADDED *MISPLACED grants nothing, and no channel rule is loosened to produce it* |
| R3 — escalation only from `none`, and only from an issue-side marker that would have granted | ADDED *The escalation is only from `none`, and only from a marker the channel would have accepted* |
| R4 — the linked issue is resolved structurally, never by parsing the PR body | ADDED *The linked issue is resolved from the structured GitHub relation, never from the pull-request body* |
| R5 — the probe declares its own non-exhaustiveness; never fails the run; never looks complete | ADDED *The probe is best-effort, cannot change any verdict, and declares what it did and did not check* |
| R6 — emit-boundary safety unchanged and re-asserted | ADDED *Every new diagnostic rides the existing single emit boundary and carries no part of either marker* |
| Proposed 1 + 3 — doctrine names the linked issue as the most probable misplacement; lead-side verification procedure (R7) | ADDED *Doctrine and the in-source residuals name the linked-issue thread and the new state* |
| R8 — behavioural coverage in the gate-executed guard suite | ADDED *Every MISPLACED and NONE rendering is pinned hermetically, and the live path is demonstrated post-merge* |

## ADDED Requirements

### Requirement: A misplaced authorization is reported under a distinct, non-granting MISPLACED state, for both kinds

The wrapper SHALL recognise a new state, **`misplaced`**, for **both** authorization kinds — the
absence waiver and the findings deferral — and SHALL report it as `waiver: MISPLACED (…)` /
`deferral: MISPLACED (…)`.

**When it is looked for.** When, and **only** when, the pull-request-side scan for a kind returns state
`none`, the wrapper SHALL additionally scan the **top-level comments of the pull request's linked
issue(s)**, using **the same scanner**, **the same marker kind**, and **the same** `base`, `head`,
`job` and author-allowlist arguments (and, for the deferral, the same observed findings count). If that
scan returns `granted`, the state SHALL become `misplaced`; otherwise it SHALL remain `none`.

**One enforcer, inherited by call.** The scanner SHALL NOT be duplicated, forked or given a
thread-specific variant, and `scripts/flow/roborev-waiver-scan.py` SHALL be **unmodified** by this
change. It is already **thread-agnostic** — it consumes
`{"comments":[{"author":{"login":…},"body":…}]}` on standard input and knows nothing about pull
requests — and this is **measured, not assumed**: on issue #3626,
`gh issue view <N> --json comments` emits `{"comments":[{"author":{"login":…},"body":…}]}`,
**byte-identical in shape** to what `gh pr view --json comments` emits. So the sole-nonblank-content
rule, the column-zero anchor, the structured author association, the allowlist, the field grammar, the
placeholder refusal and the scope binding are all inherited **by call**.

*A second implementation of the marker grammar would be a second place for it to diverge, and a
divergence in an AUTHORIZATION grammar is a bypass* (#3626's *reuse, do not reinvent* ruling). This
SHALL hold even for a variant that only had to recognise a marker well enough to print a diagnostic: a
second grammar's agreement with the first is knowable only by testing it, never by care. **The
measurement is what licenses the reuse** — had the payloads differed in shape, the options would have
been a translation layer (a new component in an authorization path, needing its own review) or a second
scanner (forbidden above), never an assumption — so the shape is recorded here, and a `gh` release that
changes it SHALL fail against a written expectation rather than silently yield an empty comments array,
which would read as *"no marker there"* and resurrect the indistinguishable `NONE` this change removes.

**The scanner SHALL NOT emit `misplaced`.** Thread identity is the **caller's** knowledge: the scanner
cannot know which thread its input came from, and telling it would mean adding a provenance argument to
the one component whose inputs must stay fixed. The state SHALL therefore be assigned by the shell
caller in `roborev-review-oracles.sh`, leaving the scanner's contract exactly as it is — *given these
comments, does an authorization for this review exist in them?*

**What the report SHALL contain.** The `MISPLACED` value SHALL name (1) the **issue number** the
marker was found on, (2) that it **grants nothing and the failing verdict stands**, and (3) the
**remedy**: re-post the **identical marker** as a **top-level comment on the pull request**. Each of
the two report arms SHALL be **dedicated**, not a fall-through: the generic arm that uppercases an
unhandled state would render a syntactically correct `MISPLACED (…)` and no remedy, and this state's
entire value is its remedy.

**`misplaced` SHALL be added to both lookup functions' recognised-state lists** — as a **belt**, not as
a route. The probe assigns after that validation today, so no list change is strictly required; the
entry exists so that a future refactor routing the probe result through the validation cannot rewrite
an accurate diagnostic into a generic `unavailable`, re-collapsing the very state this change splits
out. Those lists are **recognition** lists, not granting lists, and membership SHALL confer nothing.

#### Scenario: A would-have-granted waiver marker sits on the linked issue and nothing is on the PR
- **WHEN** a run whose `prompt-content` census paths are absent finds no `roborev-waive:` marker on the PR, and the PR's linked issue #N carries one that is well-formed, sole content of a top-level comment, from an allowlisted author, naming this base, head and job
- **THEN** the run reports `waiver: MISPLACED (…)` naming issue #N and the remedy of re-posting it as a top-level PR comment, `prompt-content:` still reads `FAIL`, and `RESULT: FAIL`

#### Scenario: A would-have-granted deferral marker sits on the linked issue
- **WHEN** a `--recheck-job` over an affirmatively measured `findings: PRESENT (n)` finds no `roborev-defer:` marker on the PR, and the PR's linked issue #N carries one naming this base, head, job and count from an allowlisted author
- **THEN** the run reports `deferral: MISPLACED (…)` naming issue #N and the remedy, `findings:` still reads `PRESENT (n)` — never `DEFERRED` and never `NONE` — and `RESULT: FAIL`

#### Scenario: The scanner is not duplicated and does not learn about threads
- **WHEN** this change is applied
- **THEN** `scripts/flow/roborev-waiver-scan.py` is unmodified, no second scanner exists, the scanner emits no `misplaced` state, and the issue-side call passes the same kind, base, head, job and allowlist as the PR-side call

#### Scenario: The report arm is dedicated rather than a fall-through
- **WHEN** the state is `misplaced` for either kind
- **THEN** the emitted value carries the issue number, the non-granting statement and the remedy, rather than the generic uppercased state with only a raw detail

### Requirement: MISPLACED grants nothing, and no channel rule is loosened to produce it

`MISPLACED` SHALL be a **diagnostic state only**. It SHALL NOT grant, SHALL NOT partially grant, and
SHALL NOT grant with a notice. The `prompt-content:` FAIL and the `findings:` FAIL SHALL stand exactly
as they stand today, and the terminal `RESULT` SHALL be unchanged by the presence of a misplaced
marker.

**Nothing about the channel SHALL be loosened**: not the author allowlist, not the
sole-nonblank-content rule, not the column-zero anchor, not the structured `gh --json` author
association, not the placeholder-reason refusal, and not the `base` + `head` + `job` binding (nor the
deferral's `count=` match and issue-disposition legs). The security property **only a marker on the
pull request grants** SHALL be preserved exactly.

**The granting gates SHALL remain the two token-exact equalities** —
`[ "$ROBOREV_WAIVER_STATE" = "granted" ]` and `[ "$ROBOREV_DEFERRAL_STATE" = "granted" ]` — and no
branch anywhere SHALL treat `misplaced` as granting. Because `waiver:` and `deferral:` are
**informational** keys, outside the closed verdict grammar and outside the affirmation loop, the new
value additionally **cannot** make anything pass by itself; **no grammar entry is required and none
SHALL be added**, since adding one would be the first step toward a value with verdict weight.

**A structural test SHALL assert that no granting path is reachable from `misplaced`**, alongside the
behavioural cases. Both are required and neither substitutes for the other: a behavioural case covers
only the fixtures someone thought of, and a structural assert cannot see a granting path built some
other way.

#### Scenario: A misplaced waiver does not waive
- **WHEN** the waiver state is `misplaced`
- **THEN** `prompt-content:` never reads `WAIVED`, the absence FAIL stands, and `RESULT: FAIL`

#### Scenario: A misplaced deferral does not defer
- **WHEN** the deferral state is `misplaced`
- **THEN** `findings:` never reads `DEFERRED`, the findings FAIL stands, and `RESULT: FAIL`

#### Scenario: No granting path is reachable from the new state
- **WHEN** `scripts/tests/test_roborev_review_guard.sh` runs
- **THEN** it asserts structurally that the only granting gates are the two token-exact `= "granted"` comparisons and that `misplaced` appears in no granting branch, and it fails if either becomes false

#### Scenario: The closed verdict grammar is unchanged
- **WHEN** this change is applied
- **THEN** no value is added to the verdict grammar's recognised set and no key that carries a verdict can report `MISPLACED`

### Requirement: The escalation is only from `none`, and only from a marker the channel would have accepted

**`MISPLACED` SHALL mean exactly one operator action** — *re-post the identical marker as a top-level
comment on the pull request.* Both halves of this requirement exist to keep that true.

**Only from `none`.** A pull-request-side `stale`, `malformed`, `unauthorized`, `count-mismatch` or
`unavailable` SHALL NEVER be overwritten. Each is already specific, already actionable and already
correct — *"your marker names a different review"*, *"a field is wrong"*, *"this login may not grant"*,
*"re-triage, the counts differ"*, *"the oracle could not be consulted"* — and replacing one with
`MISPLACED` would substitute a vaguer diagnosis for a precise one and send the operator to move a
comment that still would not grant. `none` is the only state carrying no information, and it is the
only state the probe may refine. The probe SHALL NOT EVEN BE PERFORMED for the other states: a network
call whose result is discarded is latency plus a future footgun.

**Only from an issue-side `granted`.** An issue-side marker that is itself stale, malformed or
unauthorized SHALL NOT produce `MISPLACED`; the state SHALL stay `none`. Such a marker is a
**different** defect that happens to be on a different thread, and re-posting it would not help —
reporting `MISPLACED` for it makes the run FAIL after the operator followed the remedy, which spends
the diagnostic's credibility. The escalation condition SHALL therefore be exactly *"this marker WOULD
have been accepted by the channel had it been on the pull request"*.

**What "accepted by the channel" means SHALL be stated in the rendering, not glossed.** The probe asks
the **scanner's** verdict — every property decidable from the comment itself: shape, sole content,
column-zero anchor, author allowlist, field grammar, reason substance, the base/head/job binding, and
for a deferral the `count=` match against the observed count. It SHALL NOT run the deferral's
**network disposition leg** (each `issues=` number's four-valued open-issue check) issue-side. That is
a **declared scoping**, sound because (1) `MISPLACED` grants nothing, so the worst case is advice one
step short of complete rather than a pass; (2) the remedy is identical either way — a deferral naming a
closed issue on the wrong thread must still be moved to the pull request, where the disposition leg
then runs and reports its own precise `ISSUE-CLOSED` / `ISSUE-ABSENT` / `ISSUE-UNVERIFIABLE`; and (3) it
would add one network call per declared issue per probed thread on a purely diagnostic path. The
rendering SHALL therefore claim *"would have been accepted by the channel"* and **not** *"would have
granted"*, and SHALL name that the disposition legs still apply once the marker is on the pull request.
**A diagnostic that overstates what it measured is what stops the next person looking.**

#### Scenario: A PR-side STALE marker is not overwritten by a perfect issue-side marker
- **WHEN** the PR carries a well-formed marker naming a different job, and the linked issue carries one naming THIS review exactly
- **THEN** the state remains `STALE` with its own cause, is never reported `MISPLACED`, and — measured against the `gh` invocation log — no linked-issue probe call was made

#### Scenario: A stale issue-side marker leaves the state at NONE
- **WHEN** nothing is on the PR and the linked issue carries a marker naming a different base, head or job
- **THEN** the state stays `NONE` with the probe's *checked* declaration, and `MISPLACED` is not reported

#### Scenario: A malformed issue-side marker leaves the state at NONE
- **WHEN** nothing is on the PR and the linked issue carries a marker with a missing field, an abbreviated sha or a placeholder reason
- **THEN** the state stays `NONE` with the probe's *checked* declaration

#### Scenario: An unauthorized issue-side author leaves the state at NONE
- **WHEN** nothing is on the PR and the linked issue carries a field-perfect marker from an author outside `ROBOREV_WAIVER_AUTHORS`
- **THEN** the state stays `NONE` — the allowlist applies identically on both threads, and a stranger's comment on a public issue thread cannot even produce a diagnostic that names it as an authorization

#### Scenario: The rendering does not claim more than the probe measured
- **WHEN** a deferral is reported `MISPLACED`
- **THEN** the value states that the marker would have been accepted by the channel and that the issue-disposition legs still apply once it is re-posted on the PR, rather than asserting it would have granted

### Requirement: The linked issue is resolved from the structured GitHub relation, never from the pull-request body

The linked issue(s) SHALL be resolved from `gh pr view --json closingIssuesReferences` — the structured
GitHub relation. The pull-request **body** SHALL NOT be read, scanned, or consulted for this or any
other purpose.

**This is a standing ruling, not a preference.** #3626 **deleted** a PR-body link requirement because
*a pull-request body is editable at any time by anyone with write access, with no per-edit attribution,
while a top-level comment is permanent and attributable* — so the body was the **weaker artifact**, and
would stay weaker **even if Markdown parsed trivially**. Its recogniser class provably did not close
(Markdown-handling references in one predicate went 0 → 11 across two rounds, with a multi-backtick
span and an explicit `[#N](url)` link accepted at deletion time). Per #3312, *remove the shared channel,
do not pick a rarer delimiter.* Reinstating a body scan **for any purpose** is reinstating a deleted
generation, and this requirement SHALL be read as forbidding it.

**The mutable-derived caveat SHALL be declared at the call site, not only in design notes.**
`closingIssuesReferences` is derived from the body's closing keywords and is therefore itself mutable by
anyone with write access. That is acceptable **here and only here**, for one precise reason: **the
result grants nothing** — it selects *which thread to print a diagnostic about*. The worst outcome from
a re-pointed relation is a diagnostic naming the wrong issue, or none, and the run FAILs either way.
**The moment any consumer downstream of this relation could grant, the argument evaporates and the
relation must go with it**; that boundary SHALL be written in the source beside the call, because a
future edit adding a granting consumer reads the code before it reads a design document.

**THE RESOLVER SHALL BE A SEPARATE, LATER CALL, AND THE GRANTING PATH'S PAYLOAD SHALL NOT CHANGE
SHAPE.** The existing single `gh pr view --json comments` call SHALL remain **exactly as it is**. The
relation SHALL NOT be folded into it as
`gh pr view --json comments,closingIssuesReferences`, and the existing call sites SHALL NOT be
restructured around a richer payload. Two reasons, the first decisive:

1. **The payload an AUTHORIZATION is decided from must not change shape as a side effect of adding a
   DIAGNOSTIC.** That document is the scanner's input, and the reason the scanner is safe to reuse
   unmodified is precisely that its input shape is fixed and measured. Widening it hands every consumer
   a document with an extra top-level key for a feature that grants nothing — and a refactor of the
   granting call sites turns a review of a diagnostic into a review of the grant.
2. **The probe SHALL be reachable only from a branch that has already failed to grant.** Fetching the
   relation up front makes its data available on every path including the granted one, so reachability
   would rest on where an `if` sits rather than on the data not existing. Issued as a separate, later
   call on the `none` branch alone, the ordering is structural: on any other state the call is **not
   made**, not merely ignored — which is also the only version a `gh` invocation-log assert can
   measure.

The extra round-trip on a failing run is the accepted cost; the path it sits on has already determined
that the run FAILs.

Each issue number returned SHALL be validated **affirmatively as digits** before it is used or emitted;
a value that is not a number SHALL be a could-not-check cause and SHALL NOT be interpolated raw.

**A CLOSING REFERENCE'S NUMBER IS SCOPED TO ITS OWN REPOSITORY, AND ONLY A SAME-REPOSITORY REFERENCE
SHALL BE PROBED.** `gh issue view <N>` resolves a number in the **current** repository, so following a
**cross-repository** reference would read a different issue that merely shares that number — which can
produce a **false `MISPLACED` naming a thread that never carried a marker**, worse than reporting
nothing, or miss the real one. The probe SHALL therefore establish which repository `gh` resolves here
(by asking `gh`, not by deriving it) and SHALL probe a reference **only** when that reference's own
repository, read from the relation payload, **equals** it — compared case-insensitively, because
GitHub owner and repository names are case-insensitive and a case-sensitive compare would skip a
same-repository reference.

A cross-repository reference SHALL be an **explicit, DECLARED skip**, counted and named in the
rendering, never a silent one. Following it by URL is deliberately **not** done: the reference derives
from the mutable pull-request body, so honouring it would widen what this call can be pointed at on a
path whose entire justification is that it grants nothing and only selects a thread to name; the
incident this mechanism exists for is a same-repository coordination thread; and skipping is the
fail-closed direction, whose worst case is a `NONE` that declares the skip.

**BOTH COMPONENTS OF A REFERENCE'S IDENTITY SHALL BE HELD TO THE SAME CONSTRAINT AS THE CURRENT
REPOSITORY, FROM ONE SHARED GRAMMAR.** The owner login and the repository name SHALL each satisfy the
same character grammar the current-repository identity satisfies, and that grammar SHALL be defined
**exactly once** and consumed by both checks. Two parallel validators of one grammar drift, and the
drift here is not benign: an identity that passes a weaker check but matches nothing compares unequal
to every reference, which renders as a **confident cross-repository declared skip** derived from an
identity nobody established. An identity failing this grammar SHALL be **could-not-check**, never a
declared skip.

**"In another repository" and "cannot tell which repository" are different facts and SHALL NOT render
alike.** A reference whose repository cannot be read from the payload, and a current repository that
could not be established at all, SHALL each be a **could-not-check** cause — never a declared skip
(which would assert something nobody established) and never a probe (which is the false-`MISPLACED`
route).

**Several linked issues** SHALL each be probed, **in the order GitHub returns them** (not a sort — any
sort is a policy nobody asked for), **bounded** by a named constant, reporting the **first** thread
carrying a matching marker. The bound exists because the probe is a diagnostic and must not become an
unbounded fan-out of network calls on a failing run; when the declared set exceeds it, the rendering
SHALL say so.

#### Scenario: The relation is the only source of the linked issue
- **WHEN** the probe resolves which issue to check
- **THEN** it uses `gh pr view --json closingIssuesReferences`, and no `#N` scan of the PR body and no `--json body` read exists anywhere in the wrapper or its sourced files

#### Scenario: A PR body mentioning an issue that is not linked
- **WHEN** the PR body mentions `#N` as prose with no closing keyword, so GitHub declares no linked issue, and issue #N carries a field-perfect marker
- **THEN** no thread is probed, the state stays `NONE` with the declared *no linked issue* rendering, and the marker on #N is not found — the relation, not the prose, is what bounds and attributes the probe

#### Scenario: Several linked issues, the first match reported
- **WHEN** the PR declares two linked issues and the second carries the matching marker
- **THEN** both are probed in GitHub's order and the state is `MISPLACED` naming the second

#### Scenario: A cross-repository closing reference is not probed, and the skip is declared
- **WHEN** the only declared closing reference is `other-owner/other-repo#N`, and issue `#N` **in this repository** carries a marker that would have been accepted
- **THEN** no thread is probed for it, `MISPLACED` is not reported, and the value declares how many cross-repository references were deliberately not probed and why

#### Scenario: The same number in this repository is probed
- **WHEN** the identical number is declared as a **same-repository** closing reference
- **THEN** it is probed and reported `MISPLACED`, so the skip above is shown to turn on the reference's repository and not on the probe having stopped working

#### Scenario: Owner and repository names are compared case-insensitively
- **WHEN** the current repository and the reference's repository differ only in letter case
- **THEN** they are one repository, the reference is probed, and no skip is declared

#### Scenario: A reference identity that fails the name grammar is not a cross-repository skip
- **WHEN** a reference's repository object carries an owner login or a name that does not satisfy the shared owner/name grammar
- **THEN** the reference is could-not-check, not a declared cross-repository skip, and the grammar it was judged against is the same one, defined once, that the current-repository identity is judged against

#### Scenario: A newline-bearing identity fails the grammar
- **WHEN** a reference's repository name or owner login ends with a newline
- **THEN** it fails the shared grammar and is could-not-check — never a cross-repository declared skip, which is what an anchor admitting a trailing newline produced, since the surviving newline made a same-repository reference compare unequal to everything

#### Scenario: A reference whose repository cannot be established
- **WHEN** a relation entry carries a usable number but no readable repository
- **THEN** the outcome is a could-not-check naming an unestablished repository, it is not counted as a cross-repository skip, and it is not probed

#### Scenario: The current repository cannot be established
- **WHEN** the call resolving the current repository fails, or answers with something that is not an owner/name pair
- **THEN** the outcome is a could-not-check naming that resolution, and no reference is probed

#### Scenario: A non-numeric value in the relation payload
- **WHEN** the relation payload carries a value that is not a number
- **THEN** it is not used and not interpolated raw, and the outcome is a could-not-check cause

#### Scenario: The granting call is unchanged and the resolver is a separate call
- **WHEN** this change is applied
- **THEN** the existing `gh pr view --json comments` invocation is unchanged, no invocation requests `comments,closingIssuesReferences` together, and the relation is fetched by its own later call

#### Scenario: The relation is never fetched on a state that already granted or already diagnosed
- **WHEN** the PR-side scan returns `granted`, `unauthorized`, `stale`, `malformed`, `count-mismatch` or `unavailable`
- **THEN** the `gh` invocation log shows no `closingIssuesReferences` call and no linked-issue comment call for that run

#### Scenario: The relation resolves the incident's own linked issue
- **WHEN** the resolver runs against a pull request whose declared closing reference is the coordination issue the marker was posted on, as measured on PR #3710 → issue #3544
- **THEN** that issue is the thread probed, so the misplacement that produced this change is detectable by the mechanism this change specifies

### Requirement: The probe is best-effort, cannot change any verdict, and declares what it did and did not check

The probe SHALL be **best-effort**. A missing `gh`, an unusable scanner, no linked issue, an API error,
an unparseable payload, or a thread whose comments cannot be read SHALL each leave the state exactly
where the pull-request-side scan left it — at `none`. The probe SHALL NEVER make a run pass and SHALL
NEVER make a run fail on its own, and its helper SHALL NEVER return non-zero or exit: a two-valued
return would re-import the very collapse this change exists to remove, so every failure SHALL be a
**state with a cause**.

**BUT `NONE` SHALL NEVER BE SILENTLY AMBIGUOUS ABOUT WHETHER THE PROBE RAN.** *A lane that omits
coverage silently is indistinguishable from one that covers it* — the reason the gate prints
`0 RECOGNISED` rather than a bare `0` and declares its gaps rather than implying completeness. The
`none` report SHALL therefore carry the probe's declaration, from a **closed set of renderings**:

1. **checked** — *"linked issue #N checked: no matching marker there either"*. Emitted **only when
   every probed thread was read successfully**.
2. **partially checked** — *"linked issues #A,#B checked — N of M declared examined, probe bounded at
   N, R never looked at"*. The unprobed remainder is named, never implied.
3. **no subject** — *"no linked issue is declared on this PR, so no linked-issue thread was checked"*.
4. **could not check** — *"the linked-issue thread could NOT be checked: `<cause>`"*, naming the cause.

**A RENDERING THAT READ NOTHING SHALL NEVER CLAIM ANYTHING ABOUT CONTENT.** Whether any thread was
read SHALL be decided **independently of** whether the bound cut references off. When no thread was
read the outcome SHALL say so, and SHALL carry **neither** the *no matching marker* clause — a claim
about content nobody looked at — **nor** the declared read limit, which states what a *read*
establishes and therefore presupposes one. Two forms: if references remain **unexamined** the outcome
is **could not check**, naming that no thread was read and how much was never looked at (it is not
*no subject*, which would assert that none is declared, and emphatically not *checked*); if nothing was
cut off, every declared reference was a declared skip and the outcome is the *no subject* rendering
naming that reason.

A **mixed outcome** — one thread read, another unavailable — SHALL take rendering 4 and name **both**
halves; it SHALL NOT take rendering 1. *A partial scan reported as a complete one is worse than an
admitted failure, because it is the version nobody re-checks.*

**THE UNEXAMINED REMAINDER SHALL BE NAMED WHEREVER IT EXISTS, NOT ONLY ON RENDERING 1's SIBLING.**
The bound clause SHALL be appended to **whichever** rendering fires, including rendering 4. A read
that fails inside the bounded prefix while declared references remain unexamined SHALL report **both**
gaps in one value — what could not be read, **and** how many declared references were never looked at.
Reporting only the first is the same defect as rendering 4's own reason for existing: a value claiming
more completeness than the probe achieved.

**EVERY INPUT ON THE PROBE PATH SHALL BE VALIDATED AFFIRMATIVELY, AND A ZERO EXIT SHALL NOT BE READ
AS A SUCCESSFUL READ — ENFORCED BY A SINGLE VALIDATED READ, NOT BY A CHECK AT EACH CALL SITE.** Three
successive reviews found this same collapse at a different input each time (3, then 1, then 2
findings), and the third round's second finding was inside code that round had just **added while
fixing the class**. Per this repository's standing ruling for that shape — defects landing inside the
preceding fix rounds, several rounds inside one mechanism — the response SHALL be to **restructure**,
not to validate one more site.

Therefore: every payload read and every authorization scan on this path SHALL go through **one**
validated-read helper, which returns a **three-valued** result — `ok`, `could-not-check(<cause>)`,
`refused(<cause>)` — and which is the **only** place that judges (a) that a payload is a top-level
JSON **object**, (b) that the field it needs is a **list**, (c) that the scanner's exit status was
zero, and (d) that the scanner's returned `state=` is in its **closed** recognition set. `ok` is the
only outcome from which any conclusion may be drawn; the other two are non-granting and carry a cause.
No caller on this path SHALL read a payload, invoke the scanner, or extract the scanner's `state=` any
other way, and a **structural** test SHALL assert that — because what four rounds of site fixes failed
to achieve is making the **next** input structurally unable to join the unvalidated set.

**AND WHAT THE VALIDATED READ ESTABLISHES SHALL BE STATED WITH ITS LIMIT, NOT IMPLIED.** An `ok`
establishes the **container** — a top-level object whose named field is a list — the **exit status**,
and the **closed-grammar state**. It does **NOT** distinguish a malformed **element** inside an
otherwise well-formed list: a comment entry that is not an object, or whose `body` is not a string, is
**skipped by the scanner**, so a list of such entries reads as a thread carrying no authorization.

**That limit SHALL be DECLARED and SHALL NOT be closed here.** The scanner is reused **unmodified** by
design and its element-skipping is correct for its own contract; a caller that re-validated every
element and its field types would be **re-implementing the scanner parse** — a second implementation
of the marker path, whose correctness is knowable only by differential testing against the first, and
the precise hazard this design rejected at the outset. Closing it would trade a bounded, stated
limitation for an unbounded one. The limit SHALL therefore be stated **in the rendering of every
thread reported as read**, not only in a comment: an affirmative declared limit beats a silent one,
which is this requirement's whole point. It SHALL NOT be attached to a rendering that claims no read.
The harm ceiling is a diagnostic one step less precise than it sounds; it can never be a wrongly
granted authorization, because nothing on this path can grant.

**THE TWO GRANTING LOOKUPS SHALL USE THE SAME READ.** This is not confined to the probe: the
pull-request payload that decides an **authorization** SHALL be validated by the same helper. A
**An EMPTY payload — `gh` exiting zero with nothing on standard output — is an unreadable payload and
SHALL take the same path as every other**: no caller SHALL short-circuit on it, because that reports
"there is no authorization" over comments nobody read. A
malformed or empty pull-request payload SHALL make the lookup state **`unavailable`** — the value that says the
oracle could not be consulted — and, because the probe runs only from `none`, SHALL therefore not be
probed at all. *(Recorded honestly: the missing validation on that granting read predates this change
— on `main` a malformed payload already yields `none` rather than `unavailable`. This change did not
introduce it; it added a new consequence to it, and repairs it.)*

Two consequences that are **not** obvious and SHALL be honoured explicitly, and both now live inside
that one helper. (1) The reused scanner
coerces a valid-JSON but malformed payload — `{}`, `{"comments": null}`, `{"comments": {}}` — to an
**empty comment list** and exits **0**. That is correct for the scanner's own contract and the scanner
SHALL NOT be changed for it; the **caller** SHALL validate the payload as a JSON **object** carrying a
`comments` **list** before drawing any conclusion, and every other shape SHALL make that thread a
could-not-check, never a successfully read one. (2) The scanner's returned `state=` SHALL be matched
against its **closed** recognition set before it is trusted; an **empty**, absent or never-judged state
SHALL make that thread a could-not-check. The permissive branch SHALL be keyed on **affirmative
membership**, never on `!= granted`.

**AN UNREADABLE RELATION PAYLOAD IS NOT AN EMPTY RELATION.** Rendering 3 — *"no linked issue is
declared on this PR"* — is an **affirmative claim about the pull request**, and it SHALL be reachable
**only** from a payload that was affirmatively read as a JSON **object** carrying a
`closingIssuesReferences` **list**. An unparseable payload, a non-object top level, a **missing**
`closingIssuesReferences` key, an explicit `null`, and a non-list value SHALL each take rendering 4.
`gh pr view --json closingIssuesReferences` always returns the key it was asked for, so its absence is
a broken payload; coercing any of these to zero declared references would derive an ANSWER from
something nobody could read, which is the permissive-branch-inherits-the-unknown-state shape this
whole mechanism is written against.

The existing `NONE` teaching text — that the marker must be the **sole nonblank content** of a
**top-level** comment — SHALL be **retained**; the declaration is additional, not a replacement. An
unrecognised rendering SHALL NOT exist: the set is closed, and a new outcome requires deciding what it
means before it can be printed.

#### Scenario: The probe ran and found nothing
- **WHEN** no marker is on the PR and the single linked issue's comments were read and carry none
- **THEN** the state is `NONE` and its value names the *checked* rendering identifying that issue, so a reader can tell the most probable misplacement was ruled out

#### Scenario: No linked issue is declared
- **WHEN** no marker is on the PR and the PR declares no linked issue
- **THEN** the state is `NONE` and its value names the *no linked issue* rendering, so the absence of a check is stated rather than looking like a completed one

#### Scenario: The probe could not be performed
- **WHEN** no marker is on the PR and the linked-issue comment read fails (no `gh`, an API error, or an unparseable payload)
- **THEN** the state is `NONE`, its value names the *could not check* rendering with the cause, the run still FAILs on the underlying key, and the probe failure itself neither fails nor rescues anything

#### Scenario: An unreadable relation payload is not reported as an empty relation
- **WHEN** the relation payload is unparseable, is not a JSON object, omits the `closingIssuesReferences` key, carries an explicit `null`, or carries a non-list value
- **THEN** each shape takes the *could not check* rendering naming the broken payload, and none of them reports *"no linked issue is declared on this PR"*

#### Scenario: Every read on the path goes through the one validated helper
- **WHEN** `scripts/tests/test_roborev_review_guard.sh` runs
- **THEN** it asserts structurally that every scanner invocation and every `state=` extraction lies inside the validated-read helper, that every payload-shape predicate lies inside the one shape validator, that both granting lookups and the probe route through them, and it fails if any call site reads a payload or the scanner directly

#### Scenario: The bound is exhausted by skips before any thread is read
- **WHEN** cross-repository references exhaust the probe bound before a declared **same-repository** reference is reached, so no thread is read at all
- **THEN** the outcome is *could not check* stating that no thread was read and naming both the unexamined remainder and the skips — and it carries neither a *no matching marker* claim, nor a *checked* rendering over an empty read list, nor the declared read limit, nor the *no linked issue* rendering, since one **is** declared and merely unexamined

#### Scenario: A read thread declares what "read" does not establish
- **WHEN** a probed thread's payload is a well-formed comments **list** whose **elements** are malformed — not objects, or with non-string bodies — so the scanner skips them and reports no authorization
- **THEN** the thread is reported as read, and the value declares its own non-exhaustiveness: that a thread counts as read when its payload was a comments list the scanner accepted, and that a malformed entry inside an otherwise well-formed list is skipped by the scanner and is not distinguished here

#### Scenario: The declared limit is absent where no read is claimed
- **WHEN** a rendering claims no thread was read at all
- **THEN** it carries no read limit, so the declaration cannot itself imply a read

#### Scenario: An empty payload from a successful gh is unavailable, not none
- **WHEN** `gh` exits zero with nothing on standard output, for either kind
- **THEN** the lookup state is `unavailable` naming the empty payload, never `none`, and no linked-issue probe is performed

#### Scenario: A malformed pull-request payload does not grant and is not probed
- **WHEN** the pull-request comments payload parses but is not an object carrying a `comments` list
- **THEN** the lookup state is `unavailable` naming the cause — never `none`, which would assert that no authorization exists over comments nobody read — and no linked-issue probe is performed

#### Scenario: A valid-JSON but malformed comments payload is not a read thread
- **WHEN** a probed thread's comments payload parses but is not an object carrying a `comments` list — `{}`, `{"comments": null}`, `{"comments": {}}`, or a non-list value — so the scanner reduces it to zero comments and exits 0
- **THEN** that thread takes the *could not check* rendering naming the payload, is never reported as checked, and the scanner file is unchanged

#### Scenario: An unrecognised or empty scanner state is not a read thread
- **WHEN** the scanner returns an empty `state=`, no `state=` line, or a state outside its recognition set
- **THEN** that thread takes the *could not check* rendering naming the unrecognised state, and is never counted as successfully checked

#### Scenario: The current repository identity is a single owner/name pair
- **WHEN** the repository resolution answers with something that is not exactly one `owner/name` pair — `x/`, `/x`, `a/b/c`, or a value carrying whitespace
- **THEN** the outcome is a could-not-check, and in particular the references are **not** reported as a cross-repository declared skip, which would be an answer about the pull request derived from an identity nobody established

#### Scenario: One thread read, another unavailable
- **WHEN** two linked issues are declared, the first is read with no match, and the second's comments cannot be retrieved
- **THEN** the value takes the *could not check* rendering naming both what was read and what was not, and never the *checked* rendering

#### Scenario: A thread is unreadable inside the bound while declared references remain unexamined
- **WHEN** the declared set exceeds the bound and one thread inside the probed prefix cannot be read
- **THEN** the *could not check* rendering names **both** the unreadable thread and how many declared references were never examined, in one value

#### Scenario: More linked issues than the bound
- **WHEN** the declared linked-issue set exceeds the probe bound and no match is found in the probed prefix
- **THEN** the value names how many were declared, how many were probed and the bound, so the unprobed remainder is visible

### Requirement: Every new diagnostic rides the existing single emit boundary and carries no part of either marker

The new detail strings interpolate externally-sourced values — a **runtime issue number** from
GitHub's structured payload and a **`gh` diagnostic** which is arbitrary remote text. Every one of them
SHALL pass through the **existing single emit boundary** for its process — `roborev_safe_line` in the
wrapper, `safe_value` in the scanner — and SHALL NOT be escaped, redacted or sanitised per
interpolation site. *A per-site escape is a list to keep complete*, and the class was fixed once
already by moving the neutralisation to the one boundary rather than to the field that happened to
carry it.

**No emitted diagnostic SHALL carry any part of either marker stem** (`roborev-waive`,
`roborev-defer`), nor a fillable field skeleton, because summary blocks are pasted into pull-request
comments as a matter of course in this repository and an artifact that describes the escape hatch must
not become it. The exact form SHALL remain in `--help` only. The new cases SHALL be run through the
existing `assert_no_marker_form` helper, which is attached to **every** diagnostic-emitting case —
*a property asserted only where it cannot fail is not asserted*, which is exactly how the MALFORMED
detail leaked the whole marker form for a whole release while a nearby comment denied it.

Values SHALL remain one line per value: a control character in a remote diagnostic SHALL be rendered as
a **visible escape** at the boundary, so no value can span lines and the block SHALL still carry
exactly one `RESULT:` line.

#### Scenario: A misplaced diagnostic is pasted back into the PR as a comment
- **WHEN** a run reporting `waiver: MISPLACED` or `deferral: MISPLACED` has its block posted as a PR comment
- **THEN** it contains no part of either marker stem and no field skeleton, so it authorizes nothing on any later run

#### Scenario: A gh diagnostic on the probe path carries a marker keyword
- **WHEN** the linked-issue comment read fails with a diagnostic containing `roborev-waive` or `roborev-defer`
- **THEN** the *could not check* cause still quotes the diagnostic, with the keyword redacted by the wrapper's own emit boundary and no marker form emitted

#### Scenario: A remote diagnostic containing a control character
- **WHEN** the probe's cause text carries a newline or other control character
- **THEN** it is rendered as a visible escape, the value occupies one line, and the block carries exactly one `RESULT:` line

#### Scenario: Every new diagnostic-emitting case is asserted
- **WHEN** `scripts/tests/test_roborev_review_guard.sh` runs
- **THEN** `assert_no_marker_form` is applied to both `MISPLACED` arms and to all four `NONE` renderings, not only to the case where the property holds trivially

### Requirement: Doctrine and the in-source residuals name the linked-issue thread and the new state

`CLAUDE.md` SHALL be updated **in this change**, in **both** places that record the residual — the
absence waiver's and the findings deferral's, which carry the same sentence — to state that the
**linked ISSUE thread** is the **MOST PROBABLE** misplacement, because that is where lane/lead
coordination lives, and to record the new **`MISPLACED`** state: that it names the issue the marker was
found on, that it **grants nothing**, and that the FAIL stands. *A residual corrected in one of two
places is a residual that reads as correct in the other.*

The **same two "RESIDUALS" comment blocks in `scripts/flow/roborev-review-oracles.sh`** SHALL receive
the same correction, and `--help` SHALL be corrected where it states the residual (its
*"THE COMMENT MUST BE TOP-LEVEL"* bullet names only a review body and a review-thread reply). These are
the artifacts an implementer actually reads; leaving them stale is how the doctrine gap regenerates.
`MISPLACED (…)` SHALL be added to the documented value sets of both the `waiver` and `deferral` keys,
marked non-granting and informational.

Doctrine SHALL also record the **lead-side procedure** (issue item 3): after posting either marker,
verify with `gh pr view <PR> --json comments` that the marker line is on the **pull request** — *a
grant is only granted once it is readable by the scanner that reads it.*

The two locations that remain unread for granting purposes — a **review body** and a
**review-thread reply** — SHALL still be named, since the probe does not read those either; the linked
issue is added to the list as the most probable, not substituted for them.

The website `agents-developing/roborev-findings/` page SHALL carry the same content. **Publication
verification is POST-MERGE and cannot be performed in this change**: the site is served from `main`, so
grepping the served page for a distinctive new phrase before this branch merges could only ever report
`0`, which is precisely the false signal the *"never by HTTP 200"* rule exists to prevent. The phrase to
grep SHALL be recorded in the pull-request body.

#### Scenario: An agent reads the residual after this change
- **WHEN** an agent or lead reads the waiver or deferral residual in `CLAUDE.md`
- **THEN** it names the linked-issue thread as the most probable misplacement alongside a review body and a review-thread reply, and records that a misplaced marker is reported `MISPLACED` and grants nothing

#### Scenario: The in-source residuals match the doctrine
- **WHEN** an implementer reads the two RESIDUALS comment blocks in `roborev-review-oracles.sh` and the `--help` output
- **THEN** all three name the linked-issue thread and the `MISPLACED` state, so no artifact still states the superseded two-location residual

#### Scenario: The lead-side verification step is recorded
- **WHEN** a lead posts either marker
- **THEN** doctrine directs them to verify with `gh pr view <PR> --json comments` that the line is on the PR

### Requirement: Every MISPLACED and NONE rendering is pinned hermetically, and the live path is demonstrated post-merge

`scripts/tests/test_roborev_review_guard.sh` — already executed by the agent gate's `tooling-tests`
component — SHALL gain behavioural cases covering, at minimum:

- a would-have-granted **waiver** marker on the linked issue with nothing on the PR ⇒
  `waiver: MISPLACED` naming that issue, with the run still FAILing;
- the same for **`roborev-defer:`** ⇒ `deferral: MISPLACED`, with `findings:` unchanged and the FAIL
  standing;
- a **stale**, a **malformed** and an **unauthorized** issue-side marker ⇒ state stays `NONE`;
- a **PR-side `stale`** with a perfect issue-side marker ⇒ stays `STALE`, is not overwritten, **and no
  probe call was made** (asserted against the `gh` invocation log, not assumed);
- **no linked issue** ⇒ `NONE` with the declared *no linked issue* rendering;
- the probe **unable to run** ⇒ `NONE` with the declared *could not check* rendering and its cause, the
  run still FAILing; plus the **partial-read** case ⇒ *could not check* naming both halves;
- **more linked issues than the bound** ⇒ the rendering declares declared/probed/bound;
- a **positive control** that `MISPLACED` reaches **no granting path**, for both kinds, paired with the
  **structural** assert of R2;
- `assert_no_marker_form` on **every** new diagnostic-emitting case, plus a keyword-bearing `gh`
  diagnostic on the probe path.

The `gh` test double SHALL be extended for the two new calls —
`pr view --json closingIssuesReferences` and `issue view <N> --json comments` — with the linked-issue
list **defaulting to EMPTY**, so a case that wants a probe has to **say so**. That is the fail-closed
direction and it stops a case passing because the double happened to be permissive about a question the
wrapper asks.

Every case SHALL plant its artifacts in its **own scratch copy of the tree**, **never** a path variable
or an environment seam: *a test-only seam is one more thing a real invoker can set*, and the harness
already asserts that none has been reintroduced. The suite SHALL additionally assert **structurally**
that `roborev-waiver-scan.py` is unmodified, that no consumer of `closingIssuesReferences` feeds a
granting branch, that **no invocation requests `comments` and `closingIssuesReferences` in one call**
and the pre-existing `--json comments` invocation is unchanged, that no pull-request **body** read was
reintroduced, and that `scripts/agent-gate.sh` is unmodified.

Each case whose subject is the **escalation rule** SHALL carry a **planted-mutant contrast** — the
naive form (probe on every state, or escalate on any issue-side marker) applied to a scratch copy,
producing the outcome the real code refuses — because a case that passes against both the real code and
its naive form measures nothing.

Because a pull request whose subject is how the wrapper reads authorizations **cannot certify itself**,
the live demonstration SHALL be planned and recorded **post-merge**, and the pull-request body SHALL
say so. A hermetic pass SHALL NOT be recorded as evidence that the live probe path works.

#### Scenario: The escalation rule regresses
- **WHEN** the escalation is loosened to fire from a state other than `none`, or from an issue-side marker the channel would not have accepted
- **THEN** `scripts/tests/test_roborev_review_guard.sh` fails, and with it the gate's `tooling-tests` component

#### Scenario: The linked-issue fixture defaults to absent
- **WHEN** a case does not declare a linked issue
- **THEN** the test double reports none, so no case can pass because the double was permissive about a question the wrapper asks

#### Scenario: A case is planted rather than seamed
- **WHEN** a case needs a different scanner, wrapper or oracle behaviour
- **THEN** it substitutes the artifact in its own scratch copy of the tree, and the harness fails if a test-only path variable or environment seam is reintroduced

#### Scenario: The self-certification boundary is stated
- **WHEN** the pull request is opened
- **THEN** its body states that the wrapper cannot certify itself, that the live probe demonstration is post-merge, and that `MISPLACED` grants nothing
