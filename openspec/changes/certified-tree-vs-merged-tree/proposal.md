# Proposal: The certified tree is not the merged tree — base-staleness advisory, then merge-result certification (issue #3650)

**Milestone:** maintenance / delivery-pipeline hygiene · **Priority:** P2 · **Routing:** **design-driven** —
there is no external oracle for *"what must a merge gate certify"*; the deliverable is a contract plus
recorded measurement. The one place an oracle exists (does definition D detect the case that produced the
issue?) is used below, and it **falsified the first definition**. · **Issue:** #3650 ·
**Predecessor:** #3465 (PR #3646, merged 2026-08-31 01:12Z) · **Refs:** #3358, #3362, #3514, #3616, #3646

## Why

`premerge-assert.sh` gained a real requirement in #3465: a full gate of record must exist and must have
PASSed on the exact tree being merged. It closed *"nothing requires a gate."* It did not close
**"the gate certified the wrong tree."**

A squash-merge composes the PR's diff with **current** `main`, not with the base the branch was written
against. So for any stale-based PR the tree a full gate certifies **will never exist**. Two facts are
needed and #3465 shipped one:

| fact | tree | mechanism | status |
|---|---|---|---|
| the diff has not moved since certification | PR head | head assert (`commit:`/`tree-start:`) | **landed (#3646)** |
| the diff was certified against the main it will join | `origin/main` + diff | merge-result gate | **this issue** |

Measured on PR #3362: base `2bde26a7c`, **107** commits behind `origin/main`. Its gate at the PR head
FAILed `core-tests` solely because a known-flake fix (`5e08db201`, #3514) is on main and absent from that
base — two `git merge-base --is-ancestor` calls settle it. That was the **benign** direction. The malign
direction is a **PASS at a stale head** hiding an interaction with something that landed in between: a
head-matching assert accepts it, stamps it as the gate of record, and the merge composes two things never
tested together. *Satisfied and wrong* is worse than a visible gap, and it is the same shape as the peer
summary that nearly merged #3616 on #3580's verdict.

**Owner ruling (2026-08-30T23:59:56Z), Seam 1, taken as given and not re-decided here:** the freshness
bound is **blast-radius staleness**. A certification stales only when commits landed behind the PR's base
touch files in the diff's blast radius; unrelated churn does not stale it, so hot-repo lanes (`main` moved
12× in 4 hours on the night this was filed) are not forced into re-gate loops. Sequencing, also the
owner's: **land the non-blocking advisory first**; the enforcement builds on the same information source.

### The measurement that shaped the design, because it falsified the obvious definition

Committed at `docs/round-artifacts/issue-3650-blast-radius-measurements.md`. Against PR #3362 — the case
that produced this issue — the culprit commit and the PR's diff **share no path**, so under
*"blast radius = the paths the diff touches"* the culprit is **not in the blast radius of the PR it
broke**. That definition's `M = 0` branch would call the certification fresh and skip the re-gate that was
actually required. (Under it PR #3362 does print `M = 2`; the culprit is not among those two, so that
number is a coincidence and not a detection. The unsoundness is in the `M = 0` branch.)

The interaction ran through content that changes **any** gate's verdict regardless of the diff — the
nextest config and a shared test-support module. So blast radius here means path intersection **∪ a
gate-global set**, and that widening is what makes the ruling non-vacuous:

| definition | of 107 commits behind, how many stale the certification | share |
|---|---|---|
| any churn (the shape the ruling rejects) | 107 | 100% |
| **path-intersection ∪ gate-global (adopted, as shipped)** | **28** | **26%** |
| path-intersection alone (unsound) | 2 | 2% |

74% of the churn on an 8-day-old base still does not stale a certification, and the motivating case is
detected **and named** in the output (`matched 5e08db201 gate-global .config/nextest.toml`) rather than
merely counted. (An earlier draft of this proposal said 28→22 / 26%→21%: a hand measurement over
root-only `Cargo.*` that excluded the diff's own paths. Attributed row-by-row in the measurements
artifact; the script reports its own count and is the authority.)

## What Changes

**This change is SLICE 1 of 2, and says which one it ships.** Slice 1 is the non-blocking advisory — the
piece the lead judged *"worth more than the enforcement itself"* and the owner sequenced first. It ships
the information source; slice 2 consumes it.

### Slice 1 — this change

1. **A new `scripts/flow/base-staleness.sh`**: given a branch/PR, report **`N` commits behind
   `origin/main`** and **`M` of those touching the diff's blast radius**, blast radius being path
   intersection ∪ a single declared gate-global list.
2. **Non-blocking by construction, not by intent.** Its vocabulary contains no `PASS`, no `OK`, no
   `RESULT:` — the three tokens every other verdict-bearing artifact in this repo uses — so its output
   cannot be pasted or grepped as a certification. It exits `0` whether or not it finds staleness;
   *unmeasurable* is `UNMEASURED`, never a permissive `0`.
3. **`M = 0` prints `0 RECOGNISED`, never a bare `0`**, and every run states its own non-exhaustiveness
   (it is not a dependency closure). Precedent: `cfg-gated-subtree gaps: N RECOGNISED`, for the same
   reason — a bare zero in a log reads as a verified all-clear from a scan documented as incomplete.
4. **`premerge-assert.sh` prints the advisory as a `PREMERGE: ADVISORY` line and never fails on it.**
   Slice 1 changes no verdict. The #3465 scope disclaimer is **retained**, because slice 1 does not close it.
5. Tests in `scripts/tests/test_base_staleness.sh`, wired to a gate component, **mutation-checked** per
   #3465's precedent: the motivating case is a pinned regression case that reds if the gate-global set is
   removed.

### Slice 2 — filed, not smuggled in

A `--merge-result` gate mode (gates `origin/main` + the diff in a scratch worktree, stamping a
**distinguishable** summary that names the main tip it composed against), and `premerge-assert.sh`
requiring that certification fail-closed when the advisory says stale, with the #3465 disclaimer then
updated or removed. Filed as its own issue at slice 1's merge, and this issue **stays open** — slice 1's
delivery-telemetry record is stamped `--slice` with `closed_at: null` (#3550/#3559), not closed to satisfy
a validator.

## Non-goals

- **A dependency-closure blast radius.** A commit changing a Rust item the diff *calls*, touching neither
  the diff's paths nor a gate-global path, can still change the verdict and is reported as not staling.
  Declared on every run, filed separately; it needs rustc dep-info as its information source (the route
  #3366 takes for the public-API question). Faking it with a heuristic import scan is refused.
- **Re-deciding the freshness bound.** Owner-ruled; this change mechanizes it.
- **Bounding the freshness of the merge-result gate itself** (a merge-result gate against a main that has
  since moved is stale the same way). Slice 2's problem, and named there.
- **Enforcement in slice 1.** No verdict changes. Deliberate: an enforcement that certifies the wrong tree
  while claiming to close this issue is the vacuous-pass shape one level up.
- **Defending against a hostile invoker.** Out of the threat model, per #3312's triage rule: whoever runs
  these scripts can edit them. What is defended is **accident and drift**.

## Impact

- **No-heuristics mandate:** untouched (no decode path). The advisory's own honesty rule is the same
  doctrine one level up: authoritative measurement, never inference — which is exactly why a
  heuristic dependency scan is a non-goal rather than a shortcut.
- **Public binding surfaces (Python/Node/CLI):** none. Delivery tooling only.
- **<128MB memory budget:** unaffected; no library code changes.
- **Gate cost:** the advisory is git plumbing over the commits behind the base (`rev-list` +
  `show --name-only`). Cost measured and reported in `design.md`, and it is **not** added to `--lite`.
- **Self-certification:** `premerge-assert.sh` is read from the *checkout*, not from a PR's base ref, so
  unlike a `required`-registry change this one **can** exercise itself on its own PR. Stated because the
  neighbouring class (#2910 registry, #3229 roborev config) cannot, and three lanes were caught by it.
