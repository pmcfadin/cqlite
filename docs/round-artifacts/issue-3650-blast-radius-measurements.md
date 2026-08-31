# Issue #3650 — blast-radius definition, measured against the motivating case

All figures taken on box1 `/data/lanes/lane-3650`, 2026-08-31, against the repo at
`origin/main` = `b1e8598a2`. Every command is quoted so the numbers can be re-derived.

## The subject

PR #3362 (issue #3358, BTI `da` token bound) — the case that produced this issue.

```
base       2bde26a7cf3a0a43b33b6cbfef4510781d434351   (2026-08-22)
head       4bc6b913a6afc63d2fe7f234152da9b03ea03a89
merge-base 2bde26a7cf3a0a43b33b6cbfef4510781d434351   (= base; branch never rebased)
commits behind origin/main: 107        # git rev-list --count 2bde26a7c..origin/main
```

Its diff touches exactly two paths:

```
cqlite-core/src/storage/sstable/reader/data_access/summary_scan/mod.rs
cqlite-core/tests/issue_3358_bti_query_token_bound.rs
```

The commit whose absence from that base caused the `core-tests` red is `5e08db201` (#3514,
a known-flake fix). Confirmed with the two ancestry calls the standing triage rule names:

```
git merge-base --is-ancestor 5e08db201 origin/main   -> on main:  YES
git merge-base --is-ancestor 5e08db201 2bde26a7c     -> in base:  NO
```

`5e08db201` touches:

```
.config/nextest.toml
cqlite-core/tests/issue_2316_merge_thread_budget.rs
cqlite-core/tests/issue_2370_concurrent_merge_thread_budget.rs
cqlite-core/tests/issue_3514_psi_report_contract.rs
cqlite-core/tests/support/os_thread_budget.rs
docs/development/gate-ops.md
```

## Finding 1 — PATH INTERSECTION IS EMPTY ON THE MOTIVATING CASE

```
comm -12 <PR3362 diff paths> <5e08db201 paths>   -> (empty)
```

The culprit commit and the PR's diff share **no path**. So "blast radius = the paths the
diff touches" does not contain the thing that actually changed the gate's verdict. A
definition that stops there is unsound in the direction that matters: its `M = 0` branch
would declare a certification **fresh** precisely when it was not, and the enforcement built
on it would skip the re-gate.

**A coincidence worth stating, because it would otherwise look like a pass.** Under the
narrow definition PR #3362 reports `M = 2` — two of the 107 commits do touch its two paths
— so the advisory would have printed a non-zero number on this PR. That number is **not a
detection of this defect**: the culprit is not among those two. Reading the narrow
definition as vindicated by its output on this case is the error; the unsoundness lives in
the `M = 0` branch, not in this sample's value.

## Finding 2 — WHY it is empty, and the definition that follows

The interaction was not through the PR's own code. It was through **content that can change
any gate's verdict regardless of the diff**: the nextest configuration and a shared test
support module. So for a *gate-verdict* freshness question the blast radius is not "paths my
diff touches" but "paths whose content can change my gate's verdict", which additionally
includes a **gate-global** set.

Candidate gate-global set (the exact one measured below):

```
.config/nextest.toml   rust-toolchain.toml   Cargo.toml   Cargo.lock
scripts/agent-gate.sh  scripts/ci/**  cqlite-core/tests/support/**
test-data/**           .github/workflows/**
```

It contains both `.config/nextest.toml` and `cqlite-core/tests/support/os_thread_budget.rs`,
so it **catches `5e08db201`** — the motivating case is detected.

## Finding 3 — the widened definition is SELECTIVE, not "any churn"

The owner's ruling exists to keep hot-repo lanes out of a re-gate loop, so the widened set is
only legitimate if it stays far from "every commit stales everything". Measured over all 107
commits behind that base:

| definition | commits that stale the certification | share |
|---|---|---|
| any churn (the shape the ruling rejects) | 107 | 100% |
| **AS SHIPPED — diff paths \u222a gate-global (`**/Cargo.*`)** | **28** | **26%** |
| gate-global only, `**/Cargo.*` (any member's manifest) | 26 | 24% |
| gate-global only, root-`Cargo.*` (first hand measurement) | 22 | 21% |
| path-intersection with PR #3362's 2 paths, alone | 2 | 2% |

**The shipped number is 28, and the four rows above are shown because the first draft of this
document published 22 and that number was stale within one implementation round.** The delta is
fully attributed, not hand-waved: `+4` from spelling the manifest patterns `**/Cargo.toml` /
`**/Cargo.lock` rather than root-only (any workspace member's manifest moves any gate's verdict, so
the superset is correct and can only ever *add* staleness — it cannot manufacture a false
`NO-STALENESS-RECOGNISED`), and `+2` from the union with the diff's own paths, which the
gate-global-only rows exclude by construction. Re-derive with
`bash scripts/flow/base-staleness.sh 4bc6b913a6afc63d2fe7f234152da9b03ea03a89` — the script reports
its own count, and **the script is the authority here, not this file**.

**74% of the churn on an 8-day-old base does not stale a certification** under the shipped
definition. The ruling's purpose survives — this is nowhere near "every commit stales everything" —
and the motivating case is caught **for the right reason**: the shipped run names it explicitly,
`matched 5e08db201 gate-global .config/nextest.toml`, so the detection is attributable to the
culprit rather than coincidental on a count.

## What is still NOT covered, declared rather than implied

Path intersection ∪ gate-global is **not** a dependency closure. A commit that changes a
Rust item my diff *calls* — without touching my paths and without touching a gate-global
path — can still change my verdict and will be reported as not staling. That is a real
false-negative class, it is not closed here, and it is why the advisory declares its own
non-exhaustiveness on every run and why `M = 0` prints as `0 RECOGNISED`, never a bare `0`.
A dependency-closure blast radius needs a different information source (rustc dep-info, as
#3366 proposes for the public-API question) and belongs in its own issue.
