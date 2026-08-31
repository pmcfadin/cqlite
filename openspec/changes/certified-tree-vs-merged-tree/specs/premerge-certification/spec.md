# premerge-certification — delta for certified-tree-vs-merged-tree (issue #3650, SLICE 1)

**Architecture note (read this first).** `scripts/flow/premerge-assert.sh` is the merge-point guard. Since
#3465 (PR #3646) it proves two things: the PR head still equals the certified sha, and a full
`AGENT-GATE SUMMARY` with `RESULT: PASS` and `tree-integrity: PASS` exists whose `commit:`/`tree-start:`
cover that sha. It does **not** prove the diff was certified against the `main` it will join — a
squash-merge composes the diff with main's *current* tip, so for any stale-based PR **the certified tree
and the merged tree are different objects**. Its own output says so, on three `PREMERGE: SCOPE` lines
naming #3650.

**This delta is SLICE 1 of 2 and changes NO verdict.** It ships the *information source* the owner
sequenced first: a non-blocking advisory reporting how far a PR's base is behind `origin/main` and how much
of that churn lands in the diff's blast radius. Slice 2 — a merge-result gate mode and the fail-closed
enforcement that consumes this — is filed separately, and the #3465 scope disclaimer is **retained**
until it lands.

**Acceptance-criterion → requirement map** (issue #3650):

| AC | Requirement(s) | Slice |
|---|---|---|
| AC1 — owner decides the freshness bound | *Satisfied outside this spec* by the owner ruling of 2026-08-30T23:59:56Z (blast-radius staleness); mechanized by ADDED *The blast radius is path intersection ∪ a declared gate-global set* | 1 |
| AC2 — gate of record runnable against the merge result, distinguishable summary | — | **2** |
| AC3 — `premerge-assert` requires merge-result certification, disclaimer updated | ADDED *Slice 1 changes no verdict and retains the #3465 scope disclaimer* (the retention half) | **2** (requirement half) |
| AC4 — a stale-based PR with a head PASS that fails against main is refused | — | **2** |
| AC5 — the advisory is non-blocking and cannot become a false certification | ADDED *The advisory cannot be read as a certification*, ADDED *An unmeasurable scan is UNMEASURED and is fail-closed by contract*, ADDED *Slice 1 changes no verdict …* | 1 |
| AC6 — mutation-checked, per #3465's precedent | ADDED *The advisory's blast-radius definition is mutation-checked against the motivating case* | 1 |

## ADDED Requirements

### Requirement: The base-staleness advisory reports N commits behind and M in the blast radius

`scripts/flow/base-staleness.sh` SHALL, for a branch in the current checkout, report the number of commits
`N` on `origin/main` that are not reachable from the branch's **merge-base** with `origin/main`, and the
number `M` of those commits that touch the diff's blast radius. It SHALL print the merge-base it used, the
`origin/main` sha it measured, and that sha's commit date, so the measurement is attributable and its own
staleness is visible. It SHALL NOT fetch, mutate any ref, or write in the repository.

#### Scenario: A stale base with churn in the blast radius is reported as stale
- **GIVEN** a branch whose merge-base with `origin/main` is behind by at least one commit
- **AND** at least one of those commits touches a path in the diff's blast radius
- **WHEN** `base-staleness.sh` is run for that branch
- **THEN** it prints `behind <N> commits` with `N` equal to `git rev-list --count <merge-base>..origin/main`
- **AND** it prints a blast-radius count `M >= 1`
- **AND** its verdict line reads `STALE-RECOGNISED`
- **AND** it exits `4`

#### Scenario: An up-to-date base reports zero behind without claiming freshness
- **GIVEN** a branch whose merge-base with `origin/main` equals `origin/main`
- **WHEN** the advisory is run
- **THEN** it prints `behind 0 commits`
- **AND** its verdict line reads `NO-STALENESS-RECOGNISED` and NOT `FRESH` or `CLEAN`
- **AND** it exits `0`

#### Scenario: The base is the merge-base, never the base ref's tip
- **GIVEN** a branch whose `origin/main` has advanced past its branch point
- **WHEN** the advisory is run
- **THEN** the reported base is `git merge-base origin/main HEAD`
- **AND** the printed base sha is that merge-base, labelled as such

*Verified by:* `scripts/tests/test_base_staleness.sh` (behind-count, verdict-token, merge-base cases).

### Requirement: The blast radius is path intersection ∪ a declared gate-global set

The blast radius of a diff SHALL be the union of (a) the paths the diff itself touches and (b) a
**gate-global** path set — content that can change any gate's verdict regardless of the diff. The
gate-global set SHALL be hard-coded in one location in the script with **no environment override**, since
an override is settable by the party it constrains.

#### Scenario: A commit sharing no path with the diff still stales it via the gate-global set
- **GIVEN** a diff touching only paths under `cqlite-core/src/**` and `cqlite-core/tests/issue_*.rs`
- **AND** a commit behind the base touching `.config/nextest.toml` and no path in the diff
- **WHEN** the advisory is run
- **THEN** the verdict is `STALE-RECOGNISED`
- **AND** the output names the gate-global path that matched

#### Scenario: Unrelated churn does not stale a certification
- **GIVEN** commits behind the base touching neither the diff's paths nor any gate-global path
- **WHEN** the advisory is run
- **THEN** those commits are counted in `N` but NOT in `M`
- **AND** the verdict is `NO-STALENESS-RECOGNISED`

Every entry of the gate-global set SHALL be pinned by a test that reds when that entry alone is
removed. The subject set SHALL be DERIVED at run time rather than curated, and SHALL be reconciled
against a second, independent committed declaration of the same list — a single derivation cannot pin an
entry, since removing one removes its own probe.

#### Scenario: Removing any single gate-global entry reds the suite
- **GIVEN** a copy of the script with exactly one `GATE_GLOBAL_PATTERNS` entry deleted
- **WHEN** the test suite runs against it
- **THEN** it reports at least one failure, naming the entry

*Verified by:* `scripts/tests/test_base_staleness.sh` (gate-global match case; unrelated-churn case;
derived per-entry pin with the two-oracle reconciliation).
*Rationale (measured):* `docs/round-artifacts/issue-3650-blast-radius-measurements.md` — on PR #3362 the
culprit commit and the diff share no path, so intersection alone is unsound; intersection ∪ gate-global
fires on 37 of 107 commits behind (35%) as shipped — measured with the script at `origin/main`
`b1e8598a2`, subject `4bc6b913a` — so unrelated churn still does not stale. The count is reported by
the script, which is the authority for it, and it is quoted WITH the `origin/main` sha because
`behind` is a function of where main was.

The gate-global set SHALL include `scripts/tests/**`: the gate does not only read that roster, it
EXECUTES it (`tooling-tests` runs ~16 `scripts/tests/*.sh`), so a commit touching only one of them reds
every lane's full gate regardless of the diff. Measured: it takes the fired count from 28 to 37 of 107,
9 commits staling only because of it.

The gate-global set is **DECLARED NON-CLOSED**: it is a curated, measured list of RECOGNISED gate-global
content, not an enumeration of all of it, and the output SHALL declare that as a second gap alongside the
dependency-closure gap.

The two path sources SHALL be rename-symmetric and root-relative on both sides. The porcelain
`git diff --name-only -z` SHALL be invoked with `diff.renames` and `diff.relative` pinned OFF, because
porcelain honours both (git's `diff.renames` default is true since 2.9) while the plumbing `git diff-tree`
rename-detects only under an explicit `-M`. Unpinned, a PR that renames a path loses the OLD path from
the diff set and a commit behind editing it matches neither half — a FAIL-OPEN reporting
`blast-radius 0 RECOGNISED` on a genuinely stale base; and `diff.relative` would make the count a
function of the invoker's cwd.

#### Scenario: A renamed path in the diff still stales via a commit editing the OLD path
- **GIVEN** a diff that renames `<old>` to `<new>` and a commit behind the base editing `<old>`
- **WHEN** the advisory is run
- **THEN** the verdict is `STALE-RECOGNISED` and the matched line names `<old>`
- **AND** the same case reports `NO-STALENESS-RECOGNISED` against a copy with the porcelain pin removed

#### Scenario: `diff.relative` and the invoker's cwd cannot change the count
- **GIVEN** a repository with `diff.relative=true` and a stale path intersection
- **WHEN** the advisory is run with cwd in a subdirectory
- **THEN** the verdict is `STALE-RECOGNISED` and the matched path is root-relative

### Requirement: The advisory cannot be read as a certification

**The absolute substring form of this requirement was FALSIFIED BY REVIEW (roborev job 233, F2) and is
recorded as CHANGED, not quietly softened.** It read: *"the advisory's output SHALL NOT contain the tokens
`PASS`, `OK`, or `RESULT:` in any run"*. That is unachievable: the advisory prints repository-controlled
paths **verbatim** in its dynamic fields. `test-data/**` is gate-global and the tracked path
`test-data/scripts/CI_SMOKE_TEST_USAGE.md` contains `OK`, so a commit touching it emits `OK` on a
`matched` line; three tracked paths contain `OK` today. The verifying test passed only because the sampled
run's matched set happened to exclude them — a test passing for the wrong reason.

The **ANCHORED** form replaces it. The advisory SHALL satisfy all four:

1. EVERY output line, on **stdout and stderr**, SHALL begin with `BASE-STALENESS: ` — a prefix distinct
   from the `AGENT-GATE *SUMMARY`, `ROBOREV REVIEW SUMMARY` and `PREMERGE:` block vocabularies.
2. Every **dynamic field** SHALL be control-character sanitized (newline, CR, other C0, DEL → a visible
   escape) so requirement 1 cannot be broken by repository- or caller-controlled data. Git permits
   newlines in paths; unsanitized, such a path emits a line with no prefix at all. The field SHALL
   otherwise be printed **verbatim** — masking a reserved substring would mangle the path for the reader.
3. The verdict SHALL appear **only** on a `BASE-STALENESS: verdict ` line, carrying a token from the
   **closed set** {`STALE-RECOGNISED`, `NO-STALENESS-RECOGNISED`, `UNMEASURED`}; continuation prose SHALL
   use `verdict-detail` lines.
4. The script's own **static template text** SHALL contain none of `PASS`, `OK`, `RESULT:`, asserted
   **structurally over the source file** — a provable property, unlike a claim about one sample run.

**DECLARED RESIDUAL:** a repository path may contain a reserved substring and the advisory will print it.
The anchor is what makes that harmless.

A blast-radius count of zero SHALL be printed as
`0 RECOGNISED`, never as a bare `0`. Every run SHALL print its own non-exhaustiveness: the scan is path
intersection ∪ gate-global and is **not** a dependency closure, so a commit changing an item the diff
calls without touching the diff's paths or a gate-global path is reported as not staling; and the
gate-global list itself is declared non-closed.

#### Scenario: Every output line of every case is anchored
- **GIVEN** the stale, non-stale, unmeasurable AND usage-error cases, and paths containing `OK`, `PASS`,
  a space and a newline
- **WHEN** the advisory is run
- **THEN** every nonempty output line, stdout and stderr, begins with `BASE-STALENESS: `
- **AND** every `verdict ` line carries a token from the closed set
- **AND** the assertion is evaluated after the LAST case, over the accumulated output of all of them

#### Scenario: The script's own static text carries no foreign verdict token
- **GIVEN** the shipped `scripts/flow/base-staleness.sh` with whole-line comments stripped
- **WHEN** the remaining text is searched for `PASS`, `OK` and `RESULT:`
- **THEN** none occurs

#### Scenario: A zero blast radius is affirmative, not bare
- **GIVEN** a run in which no commit behind the base touches the blast radius
- **WHEN** the advisory prints the blast-radius line
- **THEN** it reads `0 RECOGNISED` and names the scan's scope
- **AND** the same run prints the `NON-EXHAUSTIVE` statement

*Verified by:* `scripts/tests/test_base_staleness.sh` (reserved-substring/space/newline fixture case;
whole-suite anchor + closed-verdict-set case; structural template case; sanitizer planted-mutant case;
`0 RECOGNISED` case; non-exhaustiveness-present case).

### Requirement: An unmeasurable scan is UNMEASURED and is fail-closed by contract

Where the scan cannot be performed — `origin/main` absent, no merge-base between the branch and
`origin/main`, an unresolvable subject rev, or the failure of a git invocation **feeding the measurement**
(`rev-parse` of either ref, `merge-base`, `rev-list`, `diff`, `diff-tree`) — the advisory SHALL report
`UNMEASURED` with a cause and exit `5`. It SHALL NOT report `0`, `NO-STALENESS-RECOGNISED`, or exit `0`.
The script header and this spec SHALL state that a consumer MUST treat `UNMEASURED` as stale, never as
fresh.

**ONE GIT CALL IS EXCEPTED, AND THE EXCEPTION IS SCOPED HERE, WHERE THE CLAIM IS MADE.** This
requirement previously read "or a git invocation failing", an unqualified absolute the implementation
deliberately violates: the **informational** commit date of `origin/main` (`git log -1 --format=%cI`)
SHALL degrade to the literal `DATE-UNAVAILABLE` in that one field and SHALL NOT make the run
`UNMEASURED`. It feeds neither `N` nor `M`, so a fully measured scan stays measured; injecting the
verdict token into a measured run would false-positive a slice-2 consumer grepping `UNMEASURED`; and
escalating a cosmetic informational field to a non-verdict would red the tool on correct input, which is
the guard agents learn to waive. An absolute the code violates is the same defect class as the falsified
vocabulary guarantee this change already had to restate, so it is scoped rather than left to be
rediscovered (#3650 review B3).

#### Scenario: A missing origin/main is unmeasurable, not clean
- **GIVEN** a checkout with no `origin/main` ref
- **WHEN** the advisory is run
- **THEN** the verdict line reads `UNMEASURED` and names the cause
- **AND** it exits `5`
- **AND** the output contains neither `NO-STALENESS-RECOGNISED` nor a bare blast-radius `0`

#### Scenario: No merge-base is unmeasurable
- **GIVEN** a branch with no merge-base with `origin/main`
- **WHEN** the advisory is run
- **THEN** the verdict is `UNMEASURED` and it exits `5`

#### Scenario: An unavailable informational commit date does NOT make a measured scan unmeasurable
- **GIVEN** a measurable checkout whose `origin/main` commit date cannot be read
- **WHEN** the advisory is run
- **THEN** that field reads `DATE-UNAVAILABLE`
- **AND** the verdict is the measured one, not `UNMEASURED`

#### Scenario: The no-fetch guarantee is measured, and an unguaranteeable one is unmeasurable
- **GIVEN** a git that does not honour `GIT_NO_LAZY_FETCH` (older than 2.36, or a version string that
  cannot be parsed)
- **AND** a repository that is a partial/promisor clone, or whose promisor state cannot be determined
- **WHEN** the advisory is run
- **THEN** it performs no object access at all
- **AND** the verdict is `UNMEASURED`, naming the git version and the promisor state
- **AND** it exits `5`

#### Scenario: A non-promisor repository is not burdened with a git version floor
- **GIVEN** a git that does not honour `GIT_NO_LAZY_FETCH`
- **AND** a repository affirmatively determined NOT to be a partial/promisor clone
- **WHEN** the advisory is run
- **THEN** the scan proceeds and reports its measured verdict
- **AND** a `lazy-fetch-guard` line names the version field, that the variable is not honoured, and the
  affirmative `promisor NO` state

*Verified by:* `scripts/tests/test_base_staleness.sh` (missing-ref case; no-merge-base case; the
"exit 5 is never exit 0" assertion; the lazy-fetch-guard cases, which fixture a promisor clone and an
old/unparseable `git --version` through a `PATH` shim).

### Requirement: Slice 1 changes no verdict and retains the #3465 scope disclaimer

`premerge-assert.sh` SHALL print the advisory's finding on `PREMERGE: ADVISORY` lines and SHALL NOT alter
its exit code or any refusal on account of it. An advisory that fails to run, is absent, or reports
`UNMEASURED` SHALL be reported on those lines and SHALL NOT be fatal in slice 1.

The advisory SHALL be invoked under a **time bound whose enforcement does not depend on the child
cooperating**: the bound SHALL carry a SIGKILL escalation (`--kill-after`), because plain
`timeout <secs>` sends SIGTERM and then waits, so a child that traps or ignores TERM keeps the merge
critical path blocked indefinitely and the advertised bound bounds nothing (#3650 review R1). The
runner SHALL be resolved as `timeout` then `gtimeout` — GNU coreutils installs its timeout as
`gtimeout` on stock macOS, which this repo supports (#3650 review R2) — and each candidate's
`--kill-after` support SHALL be PROBED rather than assumed, following
`_gate_notify_bounded_timeout` in `scripts/lib/gate-notify.sh`. A candidate that rejects the flag
SHALL count as no bounding tool at all, since an escapable bound is not a bound.

Where no such runner is available the advisory SHALL NOT be run at all: the unavailability
SHALL be reported on a `PREMERGE: ADVISORY` line naming the missing bound, and the exit code SHALL be
unaffected. An UNBOUNDED child on the merge critical path is the hang the bound exists to prevent, so the
bound SHALL NOT silently degrade (#3650 review B1). The diagnostic SHALL name what the code actually
accepts: a message recommending `gtimeout` while the code resolves only `timeout` is the defect R2
records. The three existing
`PREMERGE: SCOPE` lines naming #3650 SHALL be retained, since slice 1 does not close the gap they
disclose; they MAY be extended to point at the advisory.

#### Scenario: A stale-based PR is still merged, with the advisory printed
- **GIVEN** a PR whose head equals the certified sha and whose gate of record is a full `RESULT: PASS`
- **AND** whose base is behind with churn in its blast radius
- **WHEN** `premerge-assert.sh` runs
- **THEN** it exits `0` with `PREMERGE: OK`
- **AND** its output carries `PREMERGE: ADVISORY` lines reporting `STALE-RECOGNISED`
- **AND** its output still carries the `PREMERGE: SCOPE` lines and the literal `#3650`

#### Scenario: A broken advisory cannot fail the assert in slice 1
- **GIVEN** an otherwise-passing `premerge-assert.sh` invocation
- **AND** an advisory that is absent or exits non-zero for any reason
- **WHEN** `premerge-assert.sh` runs
- **THEN** it still exits `0`
- **AND** the advisory's unavailability is reported on a `PREMERGE: ADVISORY` line

#### Scenario: The bound is never silently dropped
- **GIVEN** an otherwise-passing `premerge-assert.sh` invocation and an advisory that hangs
- **WHEN** a supported timeout runner is available
- **THEN** the advisory is bounded, its timeout is reported with its exit code, and the assert exits `0`
- **WHEN** no supported timeout runner is available
- **THEN** the advisory is NOT executed, the missing bound is named on a `PREMERGE: ADVISORY` line, and
  the assert exits `0` with its `PREMERGE: SCOPE` lines intact

#### Scenario: The bound holds against a child that ignores SIGTERM
- **GIVEN** an otherwise-passing `premerge-assert.sh` invocation
- **AND** an advisory that installs `trap '' TERM` and then sleeps
- **WHEN** `premerge-assert.sh` runs
- **THEN** the advisory is killed rather than awaited, its exit is reported on a `PREMERGE: ADVISORY`
  line, and the assert exits `0`

*Verified by:* `scripts/tests/test_premerge_assert.sh` (advisory-printed case; broken-advisory
non-fatal case; extended Case 39 for the retained SCOPE wording; Case 41b's BOUND cases — one
hanging stub bounded through a `timeout` shim that records the requested bound AND whether
`--kill-after` was passed, the same stub NOT EXECUTED with no supported runner on PATH, and a
TERM-IGNORING stub delegated to the real runner, each asserting the exit code and the SCOPE lines
survive; Case 41c's shipped-wiring case, which runs the shipped artifacts against a **3-commit
synthetic repository** whose own HEAD is the certified sha — so it is bounded by construction,
identical on every host, and pins MEASURED advisory values (`behind 1 commits`,
`blast-radius 1 RECOGNISED`, `verdict STALE-RECOGNISED`) rather than only the advisory's prefix;
Case 41d, which asserts behaviourally AND structurally that no OTHER case invokes the shipped
artifact, so an ambient-checkout scan cannot return).

### Requirement: The advisory's blast-radius definition is mutation-checked against the motivating case

The test suite SHALL contain a pinned regression case reproducing PR #3362's shape — a diff sharing no path
with a commit behind the base that touches a gate-global path — asserting `STALE-RECOGNISED`. That case
SHALL red if the gate-global half of the definition is removed. The suite SHALL additionally plant that
defect in a copy of the script and assert the suite catches it, so the guard is observed to fire rather
than assumed to.

#### Scenario: Removing the gate-global set reds the suite
- **GIVEN** a copy of `base-staleness.sh` with the gate-global set emptied
- **WHEN** the test suite is run against that copy
- **THEN** the motivating-case test FAILS
- **AND** the failure names the blast-radius definition

#### Scenario: The suite is wired into the gate
- **GIVEN** the full `scripts/agent-gate.sh`
- **WHEN** the `tooling-tests` component runs
- **THEN** `scripts/tests/test_base_staleness.sh` is executed
- **AND** a failing assertion in it makes the full gate FAIL

*Verified by:* `scripts/tests/test_base_staleness.sh` (planted-mutant case) and its registration in
`run_tooling_tests` in `scripts/agent-gate.sh`.

### Requirement: Doctrine states what the advisory does and does not prove

`CLAUDE.md`'s `premerge-assert` section, the script header, `.claude/agents/flow-closer.md` and
`.claude/skills/flow-address/SKILL.md` SHALL describe the advisory, its non-blocking status in slice 1,
the `UNMEASURED`-is-stale consumer contract, and the declared non-exhaustiveness of the blast radius. They
SHALL continue to state that a `PREMERGE: OK` does not prove certification against current `main`.

#### Scenario: Doctrine does not overclaim
- **GIVEN** the doctrine text after this change
- **WHEN** a reader looks for what a `PREMERGE: OK` proves
- **THEN** it still states the merge-result gap is open and names slice 2's issue
- **AND** it does not describe the advisory as a certification or as blocking

*Verified by:* the doctrine diff, and `scripts/tests/test_premerge_assert.sh` Case 39 pinning the retained
`PREMERGE: SCOPE` / `#3650` / `does NOT prove` literals.
