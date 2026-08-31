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
| 5 | `UNMEASURED` — the scan could not be performed (missing ref, no merge-base, git failure) |
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
6. **Rename/relative symmetry cases** (D1d): a PR that renames a path plus a commit behind editing the OLD
   path must be `STALE-RECOGNISED`, and it reds when the porcelain pin is removed; likewise
   `diff.relative=true` with cwd in a subdirectory.
4. **An `UNMEASURED`-is-not-`0` case**: a git failure yields exit 5, never exit 0.

## D9 — Cost

Git plumbing: one `rev-list`, then one `show --name-only` per commit behind. Measured on the 107-commit
case at **≈1.5 s** warm. It is **not** added to `--lite` and adds nothing to the full gate's critical path
beyond `tooling-tests`' own suite. A pathological N is bounded by reporting and continuing, never by
silently truncating the scan (a truncated scan would be an `UNMEASURED`, per D3).

## D10 — This change CAN certify itself, unlike its neighbours

`premerge-assert.sh` and `base-staleness.sh` are read from the **checkout**, not from a PR's base ref, so
the demonstration can run on this PR. Stated because the neighbouring class cannot — `required` evaluates
the aggregator and registry from the PR's **base** ref (#2910), and roborev reads `exclude_patterns` from
the **root** checkout at daemon start (#3229) — and three lanes were caught by that class on the night
this issue was filed. Not a claim that every part is self-demonstrating: the *enforcement* it feeds is
slice 2's, and slice 2's own demonstration is planned there.
