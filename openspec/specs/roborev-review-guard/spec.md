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

The census range's BASE SHALL be the **MERGE-BASE** of the base ref and HEAD, resolved ONCE and reused:
`<base>...HEAD` is by definition `merge-base(<base>, HEAD)..HEAD`, so the base ref's TIP is NOT the base of
the range under review and SHALL NOT be used as one. The wrapper SHALL carry the two as SEPARATE named
values — the range base (used by the census, by `sha-assert:` and by the absence-waiver scope) and the base
ref's tip (used only where the tip itself is the subject, i.e. the ROOT-checkout signature in
`sha-assert:`) — and the census diff SHALL be pinned to the resolved range base, so one read of a moving
mirror ref cannot leave the census measuring one range while the assert expects another.

An unmeasurable census SHALL fail closed and SHALL be DISTINGUISHABLE from an empty one:
`FAIL (base '<ref>' unresolvable)` when the base ref does not resolve to a commit,
`FAIL (no merge-base between '<ref>' and HEAD)` when the two have no common ancestor,
`FAIL (merge-base of '<ref>' and HEAD unusable)` when the merge-base command succeeds without yielding
exactly one 40-hex sha, and `FAIL (git diff failed)` when the diff command itself exits non-zero. None
SHALL be aliased to `FAIL (empty census)` / `NOTHING-TO-REVIEW`, because "we could not tell" is not "there
is nothing to review". An unresolvable merge-base SHALL NOT be degraded to the base ref's tip or to an
empty value — a permissive branch here SHALL be keyed on the AFFIRMATIVE value (one 40-hex sha), never on
the absence of a non-zero exit. The wrapper SHALL NOT fetch on the caller's behalf to repair an
unresolvable base.

#### Scenario: An unresolvable merge-base fails closed before anything is enqueued
- **GIVEN** a repository whose base ref and HEAD have NO common ancestor (unrelated histories)
- **WHEN** the wrapper computes the census
- **THEN** `census-check:` reads `FAIL (no merge-base between '<ref>' and HEAD)`, the message states that this is explicitly NOT `NOTHING-TO-REVIEW` and that the base is deliberately not degraded to the tip, no review is enqueued, and the terminal `RESULT:` is `FAIL`

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
configuration and SHALL count as CODE.

**THE EXTENSIONLESS RULE SHALL BE THE EXECUTABLE BIT AT EITHER ENDPOINT, NOT THE PREFIX.** Under a declared
prose directory an EXTENSIONLESS path SHALL count as CODE **if and only if** git RECORDS it EXECUTABLE **at
EITHER ENDPOINT of the census range** (`<base>...HEAD`); a non-executable
extensionless path there SHALL stay non-code, which is the only case the prefix assist exists for
(`docs/LICENSE`, `openspec/NOTES`, a `.claude/CODEOWNERS`). The prefix ALONE SHALL NOT decide it: a bare
prefix test made every extensionless path under `docs/` non-code, so it never entered the code census and
`prompt-content:` made **no claim whatsoever** about it — while the narrowed configuration excludes only
`*.md` globally plus the artifact intersection above, so such a path is NOT excluded and DOES reach the
reviewer. That left the guard silent on exactly the class AC2's post-merge trigger names, and on a REAL
tracked file: of this repo's three extensionless `docs/` executables (all mode 100755),
`docs/reports/ws0-3217-artifacts/partB-run/offcputime-bigmap` is a **379-line Python script**, i.e. genuinely
reviewable source that no key asserted was delivered (the other two, `ws0-readbw` and `ws0-stream`, are ELF
binaries, for which git still emits a `diff --git` header so the check remains satisfiable).

The mode SHALL be read from GIT'S TREE and SHALL NOT be read from the filesystem with `test -x`. A census
path need not be
checked out at all (a deletion has no file to stat, so a filesystem probe would answer a different question
with a plausible value), the recorded mode is what the diff and therefore the prompt carry, and under
`core.fileMode=false` the working bits are not authoritative. The lookup SHALL use `:(literal)` pathspec
magic (a tracked name may contain `*`, `?` or `[`) and SHALL classify the same RAW path the census holds, so
the single normalisation boundary is unchanged. An absent path SHALL classify non-code without erroring.

**THE RANGE TEST SHALL BE A DISJUNCTION OVER BOTH ENDPOINTS, AND SHALL NOT BE AN ORDERED SCAN.** The mode
SHALL be collected from the HEAD tree AND the BASE tree, and "executable" SHALL be their **logical OR**;
the result SHALL NOT depend on which endpoint is consulted first, and no endpoint SHALL be skippable on the
strength of what another endpoint answered. All four endpoint combinations SHALL be handled by that one
rule: present at BOTH (including a MODE CHANGE in either direction — a `chmod -x` SHALL NOT reclassify a
script as prose, and a `chmod +x` SHALL make an existing file CODE), present at HEAD only (added), present
at BASE only (deleted, which SHALL be classified by the mode it HAD, since removing an executable is a code
change whose review must be asserted), and present at NEITHER (which SHALL classify non-executable without
erroring, because a SUCCESSFUL lookup returning no record is a real measurement of absence — see the
tri-valued requirement below, which forbids confusing it with a FAILED lookup). "Whichever endpoint answers
first" SHALL be treated as a defect, not an
optimisation: an ordered scan that returned on the first endpoint holding a record classified `100755`@BASE
→ `100644`@HEAD as NON-CODE, dropping the path from the code census so `prompt-content:` reported
`PASS (n/n)` while making **no claim** about it — a false PASS, and a contradiction of the rule's own
premise that the census subject is the RANGE.

**THAT PROPERTY SHALL HOLD BY CONSTRUCTION, AND THE SHAPE SHALL BE ASSERTED, NOT ONLY THE BEHAVIOUR.** The
per-endpoint mode lookup SHALL be a SEPARATE function that names no endpoint (range-blind, so it cannot
express a precedence), the endpoint list SHALL be produced complete before the fold begins, and the fold
SHALL contain no `break`, no `continue` and no `return` — its single `return` SHALL be its last statement,
returning an accumulator — so that "skip an endpoint" is UNEXPRESSIBLE rather than merely unintended. A
STRUCTURAL test SHALL assert that shape independently of the loop's spelling, and SHALL itself be
controlled against mutants that violate it (an injected early exit, and the prior ordered-scan
implementation verbatim) so a shape assert that could not fire is not mistaken for one that holds.

**ANY PREDICATE FEEDING A SAFETY DECISION SHALL BE TRI-VALUED — yes / no / COULD-NOT-MEASURE — AND THE
UNMEASURABLE CASE SHALL FAIL CLOSED.** A BOOLEAN CANNOT EXPRESS UNCERTAINTY, so it is forced to fold "I could
not tell" onto one of its two values, and the value it folds onto is invariably the PERMISSIVE one ("nothing
wrong", "nothing to review"). This is a rule about the SHAPE of the predicate, not about any one call site,
and it holds at EVERY level: this is the NINTH instance of the class on this change, and the previous round's
remedy made the FOLD order-independent BY CONSTRUCTION while leaving the LEAF two-valued — it therefore
proved the right property ONE LEVEL TOO HIGH, since an order-independent fold over a predicate that has
already discarded the distinction cannot recover it. A fourth point patch would not have ended it.

Concretely, for the mode lookup: a `git ls-tree` that SUCCEEDED and returned NO RECORD (the path is
genuinely absent at that ref) and a `git ls-tree` that FAILED (`$REPO` is not a repository, the ref does not
resolve to a tree, a corrupt object) SHALL be DISTINGUISHED — the first is a MEASUREMENT and SHALL classify
NOT-EXECUTABLE, the second is UNMEASURABLE and SHALL NOT. The three states SHALL join on the TOTAL order
`NOT-EXEC < UNMEASURABLE < EXEC` by MAXIMUM, which keeps the join associative, commutative and idempotent so
order-independence is a property of the LATTICE rather than of the loop: EXECUTABLE SHALL dominate
UNMEASURABLE (the rule is a disjunction, so positive evidence at one endpoint settles it — no "yes" from
another endpoint can un-satisfy a disjunction), UNMEASURABLE SHALL dominate NOT-EXECUTABLE ("executable at
NEITHER endpoint" is a claim about EVERY endpoint, which one unmeasured endpoint leaves unfounded), and an
endpoint set that yielded nothing SHALL join to UNMEASURABLE rather than to the permissive bottom.
NOT-EXECUTABLE SHALL therefore be reachable ONLY from a positive measurement at EVERY endpoint.

When any endpoint a classification depends on is UNMEASURABLE the run SHALL FAIL CLOSED on `census-check:`
before any review is enqueued, naming the PATH, the endpoint REF(s) that could not be measured and git's own
message, worded so that *"could not check"* CANNOT read as *"nothing was wrong"* — the same wording
discipline the unresolvable base and the failed `git diff` already carry. It SHALL NOT be spent as a non-code
classification (which would drop the path from the code census, leave `prompt-content:` asserting nothing
about it, and print a green summary) and SHALL NOT be spent as `code-free:`/NOTHING-TO-REVIEW (which would
report the diff as prose). The per-endpoint predicate SHALL be renamed whenever its value set changes this
way, so that a surviving boolean call site — where `if` would silently re-collapse the third state — fails
as a "command not found" rather than as a permissive answer.

The mode lookup SHALL NOT emit spurious output on its own stderr. `git ls-tree -z` captured through a
command substitution makes the shell warn `ignored null byte in input` on EVERY call, which is per-call
noise able to MASK a real warning; since only the leading MODE field is read — first, space-terminated, and
always one of git's literal mode constants — the NUL-delimited form is unnecessary for a single-path
lookup and SHALL NOT be used there. Cleanliness SHALL be asserted by measuring that a batch of
classifications writes NOTHING to stderr.

The directory-glob match SHALL follow git `:(glob)` component
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

- `PASS` — range head == branch HEAD AND range base == the resolved RANGE BASE, i.e. the MERGE-BASE of
  the base ref and HEAD. It SHALL NOT be the base ref's TIP: the reviewed range is merge-base-relative, so
  comparing its base endpoint against the tip made a CORRECT review FAIL deterministically for every branch
  whose base ref had advanced since the branch point (issue #3392).
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
  attributed when it equals the base ref's TIP (the signature of `--branch` resolved against the ROOT
  checkout, which enqueues that tip), attributed distinctly when it equals the MERGE-BASE (the branch point
  itself, so every commit under review is a descendant of it), and otherwise named as matching neither
  endpoint. Both equalities SHALL FAIL, and the message SHALL name WHICH one matched — when the branch is
  not behind its base the two are the same commit and the message SHALL say so.
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
- **WHEN** the job record's `git_ref` is `<base40>..<head40>` whose head equals branch HEAD and whose base equals the resolved range base (the merge-base of the base ref and HEAD)
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

#### Scenario: A correct review of a branch whose base ref has advanced PASSes
- **GIVEN** a branch whose HEAD is unchanged while its base ref has advanced past the branch point, so `merge-base(<base>, HEAD)` and `rev-parse <base>` are DIFFERENT commits
- **WHEN** the job record's `git_ref` is `<merge-base>..<branch HEAD>` — the range roborev actually reviews
- **THEN** `sha-assert:` reads `PASS`, `assert-base:` names the merge-base with the base ref's tip beside it, and the terminal `RESULT:` is `PASS`

#### Scenario: With the base ref advanced, a tip-anchored range and a stale head still FAIL
- **GIVEN** the same branch, whose base ref has advanced past the branch point
- **WHEN** the job record's `git_ref` anchors the range at the base ref's TIP, or at git's empty tree, or reports a head short of the branch tip, or is a single sha equal to the tip or to the merge-base
- **THEN** `sha-assert:` FAILs in every one of those cases with the endpoint-naming diagnostics, so the merge-base comparison is not a loosening of what the assert catches

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
structured verdict exists **on a FRESH review** the wrapper SHALL fall back to the reviewer's EXIT CODE,
and prose SHALL be consulted only inside the FINDINGS BLOCK (from a `Findings` heading/label to a
LINE-INITIAL `Summary` heading/label).

**ON A `--recheck-job` THE EXIT-CODE FALLBACK DOES NOT EXIST, AND PROSE SHALL NOT ESTABLISH CLEANLINESS
(#3564).** A recheck runs no reviewer, so `roborev-exit:` is legitimately `SKIP` and cannot arbitrate
anything. `NONE` SHALL therefore be reachable from the record's STRUCTURED `verdict` letter ALONE. Where a
recheck's record carries no such letter, the findings block MAY establish `PRESENT` (a severity marker is
positive evidence) but its ABSENCE SHALL be `UNKNOWN`, never `NONE` — the two directions are asymmetric
because `review-completed:` accepts a bare `## Summary` heading as a completed review, so a findings review
whose findings are prose is INDISTINGUISHABLE from a clean one. `UNKNOWN` fails closed. This is a DEFENSIVE
path: every observed record carries the synthesised letter, so a real recheck of a clean job takes the
structured path and still PASSes, leaving the #3312 absence waiver's only route intact. The `<n>` COUNT SHALL remain BEST-EFFORT prose parsing of severity markers within that
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
`reviewed-sha:`, `assert-base:`, `job:`, `model:`, `census:`, `tokens:`, `push-assert:`, `census-check:`,
`code-free:`, `job-record:`, `sha-assert:`, `review-completed:`, `prompt-content:`,
`vacuity-tier1:`, `vacuity-tier2:`, `findings:`, `roborev-exit:`, `log:`, and a terminal
`RESULT: PASS|FAIL|NOTHING-TO-REVIEW` — **TWENTY-THREE keys in all**, counting the terminal `RESULT:`. Each
SHALL appear EXACTLY ONCE, and `code-free:` SHALL sit immediately after `census-check:`, mirroring its
pre-enqueue evaluation order.
`assert-base:` SHALL be INFORMATIONAL — outside the verdict scan and outside the affirmation backstop,
exactly like `census:`, `tokens:` and `waiver:` — and SHALL state the sha the range assert compared against
together with the base ref's TIP, so a reader of a pasted block can tell a merge-base from a ref tip
instead of inferring which one `base:` meant (issue #3392).
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
`vacuity-tier2:` are deliberately EXCLUDED, being corroborators with documented non-`PASS`
values. **`findings:` IS NOT EXCLUDED (#3564):** it carries its OWN affirmative gate — a passing run's
`findings:` SHALL reduce token-exactly to `NONE`, in EVERY mode, and that requirement SHALL NOT be
waivable (the absence waiver excuses `prompt-content:` absence only). It is listed separately from the six
because it is not merely required to be non-failing: `PRESENT`, `INCONSISTENT`, `UNKNOWN` and `SKIP` each
fail, so a run can never PASS beside open or unestablished findings. This closes the case NEIGHBOURING the grammar check: a value that is IN the grammar and
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
- **THEN** the block carries `repo:`, `branch:`, `base:`, `head-sha:`, `reviewed-sha:`, `assert-base:`, `job:`, `model:`, `census:`, `tokens:`, `push-assert:`, `census-check:`, `code-free:`, `job-record:`, `sha-assert:`, `review-completed:`, `prompt-content:`, `vacuity-tier1:`, `vacuity-tier2:`, `findings:`, `roborev-exit:` and `log:` in that order, ahead of the terminal `RESULT:` — twenty-three keys in all, each exactly once

#### Scenario: The block states which base the range assert compared against
- **WHEN** a normal run's block is read
- **THEN** `assert-base:` carries the resolved range base (the merge-base of the base ref and HEAD) together with the base ref's tip, and it is informational — it appears in neither the verdict-grammar scan nor the affirmation backstop, so it can never make a run pass or fail on its own

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
HEAD endpoint equals the worktree HEAD and its base equals `git merge-base <base> HEAD` (NOT
`git rev-parse <base>`, which is the ref's tip and equals the merge-base only while the branch is not
behind — #3392) — never as `reviewed-sha` equalling the worktree HEAD.

**Scenario attribution note (#3229).** The *Both AC6 doctrine surfaces carry all four rules* scenario
below was authored as *Both **AC4** doctrine surfaces …* under #2964, where the doctrine criterion was
that change's AC4. Under #3229 the doctrine criterion is **AC6**, and #3229's own AC3/AC4 are **DEFERRED
to issue #3283**. The scenario is therefore RE-ATTRIBUTED to AC6 rather than left carrying a number that
now names a deferred criterion: the assertion is true and satisfied on `main` today, so filing it under a
deferred AC would invite a future reader — most likely whoever picks up #3283 — to read "AC4 is deferred"
as "AC4's scenarios are not yet satisfied" and then either re-derive an assertion that already holds or
delete it as AC4 residue. The assertion is unchanged; only its criterion number is corrected.

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

#### Scenario: Both AC6 doctrine surfaces carry all four rules
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
- **THEN** it asks the reader to confirm the `reviewed-sha:` RANGE — its HEAD endpoint equal to the worktree branch HEAD and its base equal to `git merge-base <base> HEAD` — rather than a `reviewed-sha` equal to the worktree HEAD, which the range value can never satisfy

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

(a) FAILs when the reviewed sha equals the base ref — its tip and its merge-base with HEAD alike, each named distinctly; (b) FAILs when the reviewed scope
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

The check SHALL also pin the block's key ORDER — all twenty-three keys, each appearing EXACTLY ONCE, with
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

#### Scenario: An EXTENSIONLESS executable under `docs/` is a code census path the prompt must carry
- **GIVEN** a census containing `docs/reports/x-artifacts/ws0-results/ws0-readbw` recorded mode 100755, a non-executable `docs/reports/x-artifacts/harness/plain.sh`, `docs/reports/x-report.md` and a non-executable extensionless `docs/NOTICE`
- **WHEN** the census classifies them and `prompt-content:` runs against a prompt carrying `diff --git` headers for the extensionless executable and the `.sh`
- **THEN** `code-free:` reads `PASS`, `prompt-content:` reads `PASS (2/2 code census paths present)` — so the extensionless executable IS a subject and the `.md` and extensionless non-executable are NOT, established by the count rather than by inspection
- **AND** a code EXTENSION still outranks the mode: the non-executable `.sh` is CODE

#### Scenario: The same extensionless executable ABSENT from the prompt is a named FAIL
- **GIVEN** the same census
- **WHEN** the prompt carries a header for the `.sh` only
- **THEN** `prompt-content:` reads `FAIL (1/2 code census paths absent from the prompt)`, a DETAILS line NAMES `docs/reports/x-artifacts/ws0-results/ws0-readbw`, and the terminal `RESULT:` is `FAIL` — the assertion that did not exist while the path was not a subject at all

#### Scenario: The same extensionless path NON-EXECUTABLE is still non-code
- **GIVEN** a census containing `docs/reports/x-artifacts/ws0-results/ws0-readbw` recorded mode 100644 and one `.rs` file
- **WHEN** `prompt-content:` runs against a prompt carrying a header for the `.rs` file only
- **THEN** it reads `PASS (1/1 code census paths present)` and the terminal `RESULT:` is `PASS` — same name, same directory, only the recorded mode differs, so the discriminator is demonstrably the mode

#### Scenario: A DELETED extensionless executable is classified from the BASE tree
- **GIVEN** a branch that DELETES `docs/reports/x-artifacts/ws0-results/ws0-readbw`, which was recorded 100755 at the base and is absent from both HEAD and the working tree
- **WHEN** the census classifies it
- **THEN** it is a CODE census path, `code-free:` reads `PASS`, and `prompt-content:` reads `PASS (1/1 code census paths present)` against the deletion's own `diff --git` header — a filesystem `test -x` could not have reached this verdict, since there is no file to stat

#### Scenario: An extensionless path that LOSES the executable bit is still CODE
- **GIVEN** a branch whose only change to `docs/reports/x-artifacts/ws0-results/ws0-readbw` is a PURE mode change from 100755 at the base to 100644 at HEAD — so the path is present at BOTH endpoints, and neither HEAD nor the working tree records the bit
- **WHEN** the census classifies it, beside a `docs/reports/x-report.md`
- **THEN** it is a CODE census path, `code-free:` reads `PASS` (never `FAIL`), and `prompt-content:` reads `PASS (1/1 code census paths present)` — a `chmod -x` does not turn a script into prose, and the BASE tree is the only source that can say so
- **AND** with the path ABSENT from the prompt, `prompt-content:` reads `FAIL (1/1 code census paths absent from the prompt)` and a DETAILS line NAMES it — so the path is a SUBJECT the guard can miss, which is what an ordered scan removed

#### Scenario: An extensionless path that GAINS the executable bit is CODE
- **GIVEN** a branch whose only change to `docs/reports/x-artifacts/ws0-results/ws0-readbw` is a PURE mode change from 100644 at the base to 100755 at HEAD
- **WHEN** the census classifies it
- **THEN** it is a CODE census path and `prompt-content:` reads `PASS (1/1 code census paths present)` — the mirror direction, so neither endpoint outranks the other

#### Scenario: Every endpoint combination is classified by the one disjunction
- **GIVEN** a single repository whose range carries one path per combination: executable at both endpoints; non-executable at both; added executable; added non-executable; deleted executable; executable at BASE only via `chmod -x`; executable at HEAD only via `chmod +x`; an executable whose NAME contains `[`, `*` and `?`; and a path present at NEITHER endpoint
- **WHEN** each is classified
- **THEN** exactly the paths executable at one or both endpoints read CODE, the both-non-executable, added-non-executable and absent-from-both paths read NON-CODE, and the glob-metacharacter name is answered for itself rather than for a wildcard match
- **AND** consulting only HEAD SHALL make the deleted-executable and `chmod -x` paths read NON-CODE, and consulting only BASE SHALL make the added-executable and `chmod +x` paths read NON-CODE — measured as mutants, so the either-endpoint assertions are demonstrably load-bearing in both directions and the two endpoints are demonstrably not redundant
- **AND** the batch of classifications SHALL write nothing to stderr

#### Scenario: A FAILED mode lookup is UNMEASURABLE, never a measured "not executable"
- **GIVEN** a path git records mode 100755 at BOTH endpoints of the range, and a repository in which `git ls-tree` itself FAILS (the repository is unreadable), so nothing about the path can be measured
- **WHEN** it is classified
- **THEN** the classification reads UNMEASURABLE — a THIRD outcome distinguishable from both CODE and NON-CODE — and git's own failure message is recorded against each endpoint ref
- **AND** in the SAME repository read normally, the same path reads CODE and a non-executable sibling reads NON-CODE, so the fault is demonstrably the lookup and not the classifier
- **AND** a path that a SUCCESSFUL `ls-tree` reports NO RECORD for reads NON-CODE, not UNMEASURABLE — a successful lookup returning nothing is a measurement of absence, and the added/deleted combinations depend on it
- **AND** reverting the per-endpoint predicate to its two-valued form (`|| return 1`) SHALL make every UNMEASURABLE case read NON-CODE — measured as a mutant, with a control showing the mutant still classifies a readable repository correctly, so the tri-value assertions are demonstrably load-bearing

#### Scenario: The lattice resolves a SINGLE unmeasurable endpoint in both directions
- **GIVEN** a range whose HEAD endpoint is measurable and whose BASE endpoint is not
- **WHEN** a path recorded EXECUTABLE at HEAD is classified
- **THEN** it reads CODE — EXECUTABLE dominates UNMEASURABLE, because the rule is a disjunction and positive evidence at one endpoint cannot be un-satisfied by another endpoint's answer
- **AND WHEN** a path recorded NON-EXECUTABLE at HEAD is classified, it reads UNMEASURABLE — NOT-EXECUTABLE does NOT dominate, because "executable at NEITHER endpoint" is a claim about EVERY endpoint
- **SO THAT** neither a fail-open implementation (which would read NON-CODE for the second) nor a fail-closed-on-everything one (which would read UNMEASURABLE for the first) satisfies the pair

#### Scenario: An unmeasurable mode FAILS the run closed and names the path
- **GIVEN** a census of four files under `docs/` of which exactly two are extensionless (one recorded 100755, one 100644) and a `git ls-tree` that FAILS for every invocation while every other git operation succeeds
- **WHEN** the wrapper runs
- **THEN** `census-check:` reads `FAIL (recorded mode unmeasurable for 2 of 4 census paths)`, the terminal `RESULT:` is `FAIL`, and NO review is enqueued
- **AND** a DETAILS line NAMES the genuinely executable path together with the endpoint ref(s) that could not be measured and git's own message
- **AND** the summary NEVER reads `RESULT: PASS`, NEVER reads `census-check: PASS`, NEVER reads `code-free: FAIL` (an unmeasurable mode is not a docs-only diff) and NEVER reads `RESULT: NOTHING-TO-REVIEW`
- **AND** the SAME fixture with the lookup working reaches `RESULT: PASS` with `census-check: PASS` and DOES enqueue — one variable, so the FAIL is demonstrably the unmeasurable mode and not a broken harness

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

**THE RELATION SHALL BE READ BEFORE THE REPOSITORY IS RESOLVED, AND AN EMPTY RELATION SHALL BE
ANSWERED WITHOUT RESOLVING IT AT ALL.** The repository identity exists only to classify references, so
a pull request declaring **none** SHALL reach the definitive *no linked issue* outcome without that
call being made — not merely without its result being used. Resolving first made a **failure of a call
the answer never needed** report *"could not be checked"* where a definitive answer existed: a
could-not-tell reported **where an answer exists**, which is the inverse of the collapse this path is
built against and equally wrong, because it hides a completed check behind an apparent infrastructure
fault.

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

**AND "DEFINED ONCE" SHALL MEAN "INTERPRETED IDENTICALLY", WHICH IS A TESTED PROPERTY AND NOT A
CONSEQUENCE OF SHARING A STRING.** The grammar is consumed by two engines — a POSIX bracket expression
in the shell and Python's `re` — and a **range** inside a bracket expression is resolved by the
**locale's collation** in the first and by **codepoint** in the second. Measured: under
`LC_ALL=en_US.utf8` the shell matches `é`, `ǅ`, `İ` and `ß` against `^[A-Za-z0-9._-]+$` while Python
refuses all four. A malformed current-repository identity could therefore pass the shell check and
fail the reference check, and a **same-repository** reference would be reported as a cross-repository
declared skip — the probe silently declining to examine the one thread that matters.

The grammar SHALL therefore be **enumerated rather than ranged**, so no collation is consulted by
either consumer, and a test SHALL assert **both** that it contains no range **and** that the two
consumers agree on a corpus including non-ASCII inputs, in every locale the host provides, declaring
its own non-exhaustiveness where the host offers no locale that could expose a divergence. *A second
implementation is correct only insofar as it is differentially tested against the first, and sharing
one definition does not exempt it — which is what this finding proved. A documented invariant that is
untrue is worse than one that was never claimed.*

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

#### Scenario: The one name grammar means the same thing to both of its consumers
- **WHEN** the shared owner/name grammar is evaluated by the shell and by Python over the same corpus of ASCII-valid, ASCII-invalid and non-ASCII inputs, in every locale the host provides
- **THEN** the two agree on every input, and the grammar contains no character range — a ranged class is resolved by locale collation in one engine and by codepoint in the other, which lets a same-repository reference be misreported as a cross-repository skip

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

#### Scenario: No linked issue, and the repository resolution fails
- **WHEN** a pull request declares no closing reference and the call that resolves the current repository would fail
- **THEN** the outcome is the definitive *no linked issue is declared* result, not a could-not-check, and the repository resolution is not performed at all

#### Scenario: With references to classify, a failed repository resolution is reported
- **WHEN** a pull request declares a closing reference and the repository resolution fails
- **THEN** the outcome is a could-not-check naming that resolution, so the ordering above is shown to be conditional rather than the resolution having been removed

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

#### Scenario: The documented output-state contracts are complete
- **WHEN** either lookup's header comment enumerates the states it can emit
- **THEN** every state the function can actually assign is listed — `misplaced` included — and the test reads the contract's own value lines rather than the surrounding prose, so a paragraph mentioning a state cannot satisfy a check about the contract

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
