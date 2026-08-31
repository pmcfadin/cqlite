# Design: base-staleness advisory (slice 1 of issue #3650)

Decisions that bind the implementation. Every one is a choice with a rejected alternative, because the
issue's own warning is that a mechanism here can be *satisfied and wrong*.

## D1 — Blast radius = path intersection ∪ a hard-coded gate-global list

**Decided by measurement, against the case that produced the issue** (derivation:
`docs/round-artifacts/issue-3650-blast-radius-measurements.md`).

- *Rejected: path intersection alone.* On PR #3362 the culprit commit `5e08db201` and the PR's diff share
  **no path**, so its `M = 0` branch calls a certification fresh exactly when it is not. Unsound in the
  malign direction.
- *Rejected: any churn behind the base.* 107 of 107 commits — this is the shape the owner's ruling exists
  to refuse, and it forces re-gate loops on a repo where `main` moves 12× in 4 hours.
- **Adopted: intersection ∪ gate-global.** **37 of 107 (35%)** as shipped, measured at `origin/main`
  **`b1e8598a2`** with subject `4bc6b913a`, leaving 65% of churn non-staling. Catches the motivating
  case, and names it (`matched 5e08db201 gate-global .config/nextest.toml`), so the detection is
  attributable and not a coincidence on a count. *This figure has moved twice under measurement and
  the history is kept rather than tidied: an early hand count said 22 of 107 (21%) using root-only
  `Cargo.*` and excluding the diff's own paths; the script itself then reported 28 of 107 (26%); and
  review added `scripts/tests/**` to the set (D1b below), taking it to 37. `behind` is a function of
  where `origin/main` was, so every figure here is quoted WITH that sha — a bare percentage reads as
  a defect the moment main moves. The script reports its own count and is the authority for it:*
  `bash scripts/flow/base-staleness.sh 4bc6b913a6afc63d2fe7f234152da9b03ea03a89`

### D1a — What membership ASSERTS, and how to add an entry

The predicate is not "is important" or "is shared". It is: **content at this path can change a gate's
verdict INDEPENDENTLY OF THE DIFF UNDER TEST** — a commit touching only it can flip any lane's full gate
while that lane's own diff is unchanged.

```
.config/nextest.toml   rust-toolchain.toml   **/Cargo.toml   **/Cargo.lock
scripts/agent-gate.sh  scripts/ci/**   scripts/tests/**
cqlite-core/tests/support/**   test-data/**   .github/workflows/**
```

It is **one NAMED, COMMITTED list in one place (`GATE_GLOBAL_PATTERNS`), hard-coded in the script, with no
env override** — not an inline glob, because the next person adding a shared test-support directory has to
be able to FIND it. Rationale for the no-override half is #3312's second rule: an override is settable by
the party it constrains, and *"which paths stale my certification"* is precisely what a lane wanting to
skip a re-gate would widen. To add an entry: add one line, and state in the commit message which gate
COMPONENT it can flip and how you MEASURED its selectivity over the commits behind a real base.

### D1b — `scripts/tests/**` is in the set, measured (review finding)

`scripts/agent-gate.sh` was listed but the roster it *executes* was not: `tooling-tests` runs ~16
`scripts/tests/*.sh`, so a commit touching only e.g. `scripts/tests/test_worker_supervisor.sh` reds
**every** lane's full gate regardless of the diff — the membership predicate verbatim — and was reported
as not staling. Measured with the script over the 107-commit base (subject `4bc6b913a`, `origin/main`
`b1e8598a2`): **28 → 37 of 107**, i.e. **9** commits behind stale only because of it. Two neighbours were
measured and **deliberately not added** because they fire **zero** times there — `deny.toml`, and the 14
loose `scripts/*.sh` helpers enumerated as exact entries (both leave the count at 37). An entry that has
never fired buys only false positives.

### D1c — The list is DECLARED NON-CLOSED, in the output

The set is a curated, measured list of **recognised** gate-global content, never an enumeration of all of
it, so a gate-global path absent from it is a false negative reported as *not staling*. The output declares
this as **gap 2 of 2** on every run, beside the dependency-closure gap. The earlier text named only the
closure gap, which affirmed a completeness the list does not have.

### D1d — The two path sources must be rename-symmetric and root-relative (review finding)

The blast radius compares a **porcelain** path set (`git diff --name-only -z <merge-base>...<subject>`)
against a **plumbing** one (`git diff-tree` per commit behind), and the two answer differently under
default config. Measured (git 2.43.0, `git mv src/foo.rs src/foo_renamed.rs` plus an edit):

| call | result |
|---|---|
| `git diff --name-only` (porcelain, `diff.renames` default **true** since 2.9) | **destination only** |
| `git diff-tree`, default | **both** paths |
| `git diff-tree -c diff.renames=true` (forced) | **both** — plumbing ignores the config |
| `git diff-tree -M` (explicit) | destination only |
| porcelain with `diff.relative=true`, run from a subdirectory | `foo_renamed.rs` — **no `src/` prefix** |

Asymmetric, a PR that renames a file — routine here, the campsite rule makes splits normal — loses the OLD
path from the diff set, so a commit behind that edited the old path matches **neither** half and the scan
reports `blast-radius 0 RECOGNISED` on a genuinely stale base. **A fail-open.** `diff.relative` is the same
class and is the live hazard because the **invoker** controls it: it makes `M` a function of cwd.

**Decision:** pin **both off on the porcelain call** (`-c diff.renames=false -c diff.relative=false`). The
plumbing call needs no pin — it does not rename-detect without an explicit `-M`, and the comment says so
rather than claiming the config reaches it (a claim that would be false and would rot). The plumbing-side
risk runs the **other** way: adding `-M` to `diff-tree` to "improve" the commit scan would reintroduce the
asymmetry from the opposite direction. **Do not add it.**

## D2 — The verdict vocabulary is chosen so the advisory CANNOT be read as a certification

AC5 says the advisory must not itself become a false certification. That is a property of its **words**,
not of its intent, because this repo's failure mode is someone grepping a token.

**THE ABSOLUTE FORM OF THIS DECISION WAS FALSIFIED BY REVIEW (roborev job 233, F2) AND IS RECORDED AS
CHANGED, NOT QUIETLY SOFTENED.** It read *"no `PASS`, no `OK`, no `RESULT:` anywhere in its output"*. That
is false: the advisory prints repository-controlled paths **verbatim** in its dynamic fields, and a path
may contain any of those substrings. Confirmed against this tree — `test-data/**` is gate-global and the
tracked path `test-data/scripts/CI_SMOKE_TEST_USAGE.md` contains `OK` (inside `SMOKE`), so a commit
touching it emits `OK` on a `matched` line; three tracked paths contain `OK` today. The AC5 test passed
only because the sampled run's matched set happened to exclude them — **a test passing for the wrong
reason**. Worse, git **permits newlines in paths**, so an unsanitized matched path could emit a line with
no prefix at all, breaking the very anchor everything else rests on.

**The ANCHORED form replaces it** (#3312's rule: anchor or remove the channel, never pick a rarer
delimiter — and masking a path would also mangle it for the reader):

- **(a)** EVERY output line, stdout **and** stderr, begins with `BASE-STALENESS: `. Its prefix is distinct
  from `AGENT-GATE *SUMMARY`, `ROBOREV REVIEW SUMMARY` and `PREMERGE:`, so no line of this output can be
  mistaken for a line of theirs.
- **(b)** Every dynamic field is **control-character sanitized** (newline, CR, other C0, DEL → a visible
  `\n`/`\r`/`\xNN` escape), so (a) cannot be broken by repository- or caller-controlled data. The path is
  otherwise kept **verbatim**.
- **(c)** The verdict appears **only** on a `BASE-STALENESS: verdict ` line, its token from the **closed
  set** {`STALE-RECOGNISED`, `NO-STALENESS-RECOGNISED`, `UNMEASURED`}; continuation prose goes on
  `verdict-detail` lines.
- **(d)** The script's own **static template text** carries none of `PASS`, `OK`, `RESULT:` — asserted
  **structurally over the source file**, which is provable, unlike a claim about one sample run.
- **DECLARED RESIDUAL:** a repository path *can* contain a reserved substring, and when it does the
  advisory prints it. The anchor is what makes that harmless.
- The no-finding verdict is **`NO-STALENESS-RECOGNISED`**, never `FRESH` and never `CLEAN`. It names a
  *scan result*, not a *state of the world* — the distinction the whole issue turns on.
- `M = 0` prints **`0 RECOGNISED`**, never a bare `0`. Precedent: `cfg-gated-subtree gaps: N RECOGNISED`
  (CLAUDE.md), for the identical reason — a bare zero in a log reads as a verified all-clear from a scan
  documented as incomplete.
- Every run prints its own **`NON-EXHAUSTIVE`** lines. Not a footnote in docs; in the output, every time,
  because the output is what gets pasted.

## D3 — Exit codes give slice 2 a machine-readable signal, and UNMEASURED is fail-closed BY CONTRACT

| exit | meaning |
|---|---|
| 0 | `NO-STALENESS-RECOGNISED` — scan completed, affirmatively measured, nothing recognised |
| 4 | `STALE-RECOGNISED` — at least one commit behind the base touches the blast radius |
| 5 | `UNMEASURED` — the scan could not be performed (missing ref, no merge-base, unresolvable subject, or the failure of a git call **feeding the measurement**: `rev-parse`/`merge-base`/`rev-list`/`diff`/`diff-tree`) |
| 3 | usage error |

**The contract, written now so slice 2 cannot walk into the fail-open hole:** a consumer MUST treat
**`5`/`UNMEASURED` as STALE**, never as fresh. This is CLAUDE.md's standing rule — *never derive a pass
from the absence of a bad signal; where the sole oracle could not be consulted the verdict is
non-passing*. It is stated in the script header, in the spec, and asserted by a test, because the shape
that keeps recurring in this repo is a multi-state signal whose unmeasured state inherits the permissive
branch.

Distinct codes `4`/`5` are used precisely because `premerge-assert.sh` already owns `0`/`2`/`3`; reusing
one would make the advisory indistinguishable from its caller's verdicts.

## D4 — The base is the MERGE-BASE, never the base ref's tip

`N` = `git rev-list --count $(git merge-base origin/main HEAD)..origin/main`.

#3392 is the recorded cost of getting this wrong: an assert that expected the base ref's *tip* FAILed
deterministically on every correct review of a branch whose `main` had advanced, and was misdiagnosed as a
race **twice**. Same trap, same shape, so the same resolution — and the script **prints the merge-base it
used**, so the two can never be confused in a pasted block.

## D5 — The advisory does not fetch, and says which `origin/main` it measured

A verifier with a side effect is a worse verifier. The script reads `origin/main` as it finds it and
**prints that sha and its commit date**, so a reader can see whether the measurement itself is stale. A
missing `origin/main` is `UNMEASURED` (exit 5), never a silent zero. Doctrine tells the caller to fetch
first — `premerge-assert`'s existing guidance already says `git fetch && git rebase origin/main` before
every gate.

**`GIT_NO_LAZY_FETCH=1` is what makes the no-fetch claim TRUE rather than intended (#3650 review B2).**
There is no fetch command in the script, but in a **partial/promisor** clone plain object access fetches
over the network and **writes a packfile into the repository** — `rev-list`, `diff`, `diff-tree` and `log`
all do. Measured on git 2.43.0 against a `--filter=tree:0` clone: the diff call alone took the object
store from 4 files to 12. The variable is exported once, before any object access; a missing object then
fails its git call, which every call site already routes to `UNMEASURED`.

**AND EVERY SCRATCH READ IS A CHECKED OPEN, BECAUSE `done <"$file"` IS A FAIL-OPEN (#3650 review R6 F2).**
The NUL-separated git output is written to files under the scratch dir and read back by redirection (never
`$( )`, which discards NULs). An unchecked `done <"$file"` does two things when the file cannot be opened,
and both are the shapes this design refuses: bash emits an **unprefixed** diagnostic — breaking D2a's
anchor from a line no `sane` call can reach — and the loop body **never runs**, so the path set reads as
empty, `M` is undercounted and the verdict lands on the permissive `NO-STALENESS-RECOGNISED`. All three
reads (`diff-paths`, `commit-paths`, `behind-commits`) therefore open their file explicitly on a numbered
fd, check the open, and route a failure to `UNMEASURED` naming the file and what it held. The open is
wrapped in a brace group so the suppression applies to the shell's own redirect diagnostic: redirections
are processed left to right, so `exec 3<"$f" 2>/dev/null` prints the diagnostic *before* the suppression
takes effect (measured on bash 5.2), and `exec 2>/dev/null 3<"$f"` would silence the script's stderr for
the rest of the run; a brace group does not fork, so the fd persists.

**AMBIENT GIT STATE IS PINNED IN ONE PLACE, AND `refs/replace/*` JOINED THE LIST (#3650 review R6 F1).**
`GIT_NO_LAZY_FETCH` is one instance of a family: repository- or invoker-controlled git state that changes
what `merge-base`, `rev-list`, `diff` or `diff-tree` REPORT *without failing*, so the answer is confidently
wrong — and always in the permissive direction, a false `NO-STALENESS-RECOGNISED`. Replacement refs are
honoured by all four of those commands, so one local `git replace` can rewrite the ancestry the scan walks
or hide a blast-radius path. Measured (git 2.43.0, a synthetic fixture): with the commit that touches the
gate-global `.config/nextest.toml` replaced by a substitute carrying its parent's tree, `diff-tree`
reported no paths and the scan emitted `blast-radius 0 RECOGNISED` / `NO-STALENESS-RECOGNISED`, while the
same run with `GIT_NO_REPLACE_OBJECTS=1` exported reported the path and `STALE-RECOGNISED` — `behind` was
`1` either way, so nothing else in the output betrayed the substitution. Unlike the lazy-fetch variable it
needs no version measurement (every git with replacement refs honours it). The pins were in three separate
places, each with its own rationale, so the *set* was visible nowhere; they are now enumerated in one
comment block at the exports — `GIT_NO_LAZY_FETCH`, `GIT_NO_REPLACE_OBJECTS`, and a pointer to the
per-invocation porcelain `diff.renames`/`diff.relative` pins, which must stay at their call site because
the plumbing side has to remain unpinned for rename symmetry. None is settable by the caller (#3312).

**And the scratch location is the other half of the same claim, which is only as strong as its ORDERING
and its SUBJECT (#3650 review job 239).** `mktemp -d` honours `TMPDIR`, so a `TMPDIR` inside the checkout
makes this script write in the repository. Two properties, both learned the hard way:

* **Validate BEFORE creating.** The first version created the scratch dir and *then* rejected an
  in-repository location — so the no-write contract was already violated in exactly the case the check
  exists to prevent, and a SIGKILL between the two stranded the directory in the checkout. The requested
  root is now canonicalized (`cd`+`pwd -P`, no `realpath` dependency) and refused before `mktemp` runs;
  the created dir is revalidated afterwards (symlink swap, unexpected `mktemp` resolution) and removed
  before routing to `UNMEASURED`.
* **Check BOTH repository roots.** A work-tree-only check is blind in this fleet's standard
  configuration: every lane is a `git worktree`, so `--git-common-dir` is *always* outside the lane's
  toplevel (measured on lane-3650: toplevel `/data/lanes/lane-3650`, common dir
  `/data/lanes/repo/.git`). A `TMPDIR` there writes into state every lane on the box shares. An
  unresolvable root is `UNMEASURED`, never a fallback to whichever one resolved — a check that silently
  narrows its own subject is the permissive-branch shape this issue refuses.

The ordering half is **not observable by a `find` after the run**: `mktemp -d` creates an empty
*directory* and the EXIT trap removes it on the unmeasured path too, so create-then-check leaves no
residue once the process has exited (verified — the pre-fix script passes a directory-inclusive
snapshot). The test therefore observes the create itself, via a `PATH` shim recording every `mktemp`
invocation, and asserts the reject path invokes it **zero** times, with a non-vacuity assert that a
measuring run invokes the shim exactly once.

**And the "any failed git invocation ⇒ `UNMEASURED`" claim is SCOPED, not absolute (#3650 review B3).**
The calls feeding the measurement (`rev-parse` of either ref, `merge-base`, `rev-list`, `diff`,
`diff-tree`) yield `UNMEASURED`. The **informational** `origin/main` commit date is explicitly excepted
and degrades to `DATE-UNAVAILABLE`: it feeds neither `N` nor `M`, injecting the verdict token into a
fully measured run would false-positive a slice-2 consumer grepping `UNMEASURED`, and escalating a
cosmetic field to a non-verdict would red the tool on correct input. The exception is stated in all three
places the claim is made — script header, spec, here — because an absolute the code deliberately violates
is the defect regardless of which side is right.

## D6 — Slice 1 changes NO verdict, and the #3465 disclaimer is RETAINED

`premerge-assert.sh` gains `PREMERGE: ADVISORY` lines and nothing else: it cannot fail on the advisory, and
an advisory that errors or is absent is reported, not fatal. The three existing `PREMERGE: SCOPE` lines
naming #3650 **stay** — slice 1 does not close the gap they disclose, and removing them would be the
overclaim the lead named. They are extended by one line pointing at the advisory.

`scripts/tests/test_premerge_assert.sh` Case 39 pins the literal `PREMERGE: SCOPE`, `#3650` and
`does NOT prove` on both success paths; the new lines are additive so that case keeps holding, and it is
extended rather than replaced.

## D7 — Home: a sourced helper next to `premerge-assert.sh`, following the roborev model

`scripts/flow/base-staleness.sh` is a **standalone executable** (so it is usable by hand for triage, which
is the standing fleet rule this mechanizes) that `premerge-assert.sh` invokes. Resolved from the caller's
own directory with **no env override** — #3312's enforcer rule, and it costs nothing here.

*Rejected: inlining into `premerge-assert.sh`.* It is 691 lines already, and the triage use ("is the fix
for this red already on main and merely absent from my base?") wants a command a human can run on its own.

## D8 — Mutation check, per #3465's precedent

`scripts/tests/test_base_staleness.sh`, wired into the existing **`tooling-tests`** component (which
already runs `test_premerge_assert.sh`). Beyond ordinary cases it carries:

1. **The motivating case as a pinned regression fixture** — a synthetic repo reproducing PR #3362's shape
   (a diff sharing no path with a commit behind that touches `.config/nextest.toml`), asserting
   `STALE-RECOGNISED`. **It reds if the gate-global set is removed**, which is the mutation that matters.
2. **A planted-mutant case** following `scripts/tests/test_ws0_perf_invocation_lint.sh:812-830`: copy the
   script, plant the narrow-definition defect, assert the suite catches it — so the guard is shown to fire
   rather than assumed to.
3. **The anchored-output cases** (D2 as revised). Fixtures whose matched paths contain `OK`, `PASS`, a
   space and a **newline**; a whole-suite assertion, accumulated across **every** case and evaluated after
   the **last** one, that every nonempty output line (stdout and stderr) carries the prefix and that every
   `verdict ` token is from the closed set; a structural assertion over the script source for (d); and a
   planted mutant reducing the sanitizer to a pass-through, which must break the anchor. `M = 0` never
   appears as a bare `0`. *The predecessor of these ran MID-SUITE, so it inspected Cases 2–6 only and never
   saw the `UNMEASURED`, usage or mutant runs, and its prefix check recorded success on both branches while
   asking the wrong question.*

   **THEIR POSITION IS NO LONGER MAINTAINED BY HAND, BECAUSE THAT FAILED THREE TIMES (#3650 review R6
   F3).** Round 2 found them running mid-suite; round 3 moved them to the end; round 5 appended new cases
   *after* them and silently shrank their coverage again — invisibly, because the assertions still passed
   on the subset they happened to see. A manually placed check cannot hold, so the position is replaced by
   two mechanisms: they run **only from `finish`, the EXIT trap and the single exit path** (nothing can
   execute after an EXIT trap, so a case appended anywhere — including the file's last line — is
   inspected, and there is deliberately no explicit call at the end, which would be a position again);
   and a **count reconciliation** is the actual guard — `record_out` counts recorded runs, the checks
   record the count they inspected, and `finish` reds unless they ran exactly once and inspected exactly
   that many. Verified by contrast: an anchor-violating case appended at the file's very end is invisible
   under the previous structure (209 passed, 0 failed, exit 0) and reds under this one (exit 1, naming the
   appended case); a re-introduced mid-suite invocation reds with `ran 2 times, not once`; and with the
   `finish` call removed as well, the shortfall branch reds naming `16 run(s) were never evaluated`.
5. **A DERIVED per-entry pin for the gate-global set, with TWO oracles reconciled fail-closed.** A
   mutation sweep found 8 of the 10 entries silently deletable with the suite green — Case 11 empties the
   WHOLE list, so nothing pinned an individual entry, and the two that did red were covered only
   incidentally. Deriving the subject set from the script ALONE cannot fix that *by construction*: drop an
   entry and its probe disappears with it (the oracle shares a source with its subject). So the subject
   set is the **union** of the script's `GATE_GLOBAL_PATTERNS` and the **independent** committed
   declaration in D1a above, the two are reconciled by name in both directions, and an unreadable second
   oracle is a FAIL rather than a fallback to the first. A new entry is probed for free (derive, never
   curate); a one-sided deletion reds twice. Measured: dropping any single entry now reds; emptying the
   list reds hard; the control is green.
   **DECLARED LIMIT — a COORDINATED deletion from BOTH oracles in one diff is NOT caught, and is
   green (measured: 116 passed / 0 failed).** Two oracles that both live in this repository cannot
   detect an edit that moves both, so this pin defends against *one-sided drift* — a rebase, a
   cleanup, a partial edit — and not against a deliberate two-sided change. That residual is stated
   rather than implied, because "a one-sided deletion still reds" reads to a hurried reviewer as
   "deletions are caught", which is exactly the affirming-a-completeness-we-do-not-have shape this
   whole change exists to close. The control on a two-sided edit is the diff review: both hunks are
   visible in the same PR, which is the point of keeping oracle B a committed declaration rather than
   a generated artifact.
6. **Rename/relative symmetry cases** (D1d): a PR that renames a path plus a commit behind editing the OLD
   path must be `STALE-RECOGNISED`, and it reds when the porcelain pin is removed; likewise
   `diff.relative=true` with cwd in a subdirectory.
4. **An `UNMEASURED`-is-not-`0` case**: a git failure yields exit 5, never exit 0.
7. **The lazy-fetch-guard cases** (D5 as revised, #3650 review R5 F1): a REAL `--filter=tree:0 --no-local`
   promisor clone plus a `git` shim that both reports an old version AND unsets `GIT_NO_LAZY_FETCH` —
   simulating an old git faithfully takes both halves, since reporting the version alone leaves the real
   git still honouring the variable and the object-store assertion would pass for the wrong reason. The
   primary assertion is an ARTIFACT ON DISK: the object-store file count across the run. Measured
   contrast — the pre-fix script took it from 4 files to 8 (a packfile written into the repository) and
   reported `NO-STALENESS-RECOGNISED`; the fixed script leaves it at 4 and reports `UNMEASURED`. Both
   config sources and both object-store marker shapes are fixtured separately, the 2.36 floor is pinned
   from both sides (2.36.0 honoured, 2.35.9 not), an unparseable version is pinned as UNSUPPORTED, and a
   failing `git config` probe is pinned as `UNKNOWN` rather than "absent".

**AND THE SUITE'S OWN HERMETICITY IS NOW ASSERTED, NOT CLAIMED (#3650 review R5 F2).**
`scripts/tests/test_premerge_assert.sh` had a header saying "fast + hermetic" while `run()` invoked the
SHIPPED `premerge-assert.sh`, which invokes the SHIPPED `base-staleness.sh`, which read the **ambient
checkout** — so 13 success-path cases ran repeated repository-dependent scans, and on a stock macOS the
suite's own runner shim DISCARDED the bound, so they ran unbounded. Measured cost of that dependency on
this lane: ~0.03 s per scan on a freshly-rebased base but 0.43 s on a base 110 commits behind, i.e. ~5.6 s
of ambient-dependent work in one suite run, growing with the lane's staleness. `run()` now invokes a
scratch copy beside an IMMEDIATE advisory stub, and the ONE wiring case uses the synthetic repository
described in the spec. The defect was INVISIBLE — every assertion was green while 13 cases scanned the
surrounding repository — so Case 41d asserts both halves: behaviourally (an ordinary case carries the
neutral stub's line and NOT the shipped advisory's `NON-EXHAUSTIVE` block, which every real run prints)
and STRUCTURALLY (exactly ONE invocation of the shipped artifact exists in the file, with the needle split
so the guard cannot match its own line). Verified by planting the regression: restoring `run()` to the
shipped artifact reds all three assertions.

## D9 — Cost

Git plumbing: one `rev-list`, then one `diff-tree -r -z --name-only` per commit behind (**not** `show`,
which this text used to name — plumbing is what keeps the commit side rename-symmetric with the porcelain
diff side, per D1d). Re-measured 2026-08-31 on this lane at **0.43 s** warm for a base **110** commits
behind (the earlier **≈1.5 s** figure was taken on a cold object cache; both are recorded rather than one
overwriting the other, since a bare figure in committed prose reads as a defect the moment it moves). It is **not** added to `--lite` and adds nothing to the full gate's critical path
beyond `tooling-tests`' own suite. A pathological N is bounded by reporting and continuing, never by
silently truncating the scan (a truncated scan would be an `UNMEASURED`, per D3).

## D10 — This change CAN certify itself, unlike its neighbours

`premerge-assert.sh` and `base-staleness.sh` are read from the **checkout**, not from a PR's base ref, so
the demonstration can run on this PR. Stated because the neighbouring class cannot — `required` evaluates
the aggregator and registry from the PR's **base** ref (#2910), and roborev reads `exclude_patterns` from
the **root** checkout at daemon start (#3229) — and three lanes were caught by that class on the night
this issue was filed. Not a claim that every part is self-demonstrating: the *enforcement* it feeds is
slice 2's, and slice 2's own demonstration is planned there.
