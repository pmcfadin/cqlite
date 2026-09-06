# Design: scoped clippy for `--lite`

## Decision 0 — the scope is `blast_radius ∪ FLOOR`, and `FLOOR` exists because of #1893

A pure blast-radius scope is unsound here, and the reason is written in the source rather than
inferable from the design.  `scripts/agent-gate.sh:11214` records invariant **#1893**: `--lite`'s
python tier classifies a venv/pip/maturin toolchain failure as **SKIP** rather than FAIL, and it is
safe to do so *only because* `run_clippy` still COMPILES `cqlite-py` in the same lite run.  Drop
`cqlite-py` from the lint set on a diff that does not touch it, and a broken `bindings/python/src`
sails through a lite run that reports SKIP for the tier that would have caught it.

So the scope is a union with a declared floor, `FLOOR` is defined in exactly one place, and every
entry carries the issue that put it there.  `cqlite-py` is the founding entry.

**This is a constraint on the cost win, and it is stated rather than hidden.**  `FLOOR` is linted on
every lite run regardless of diff, so its cost is unavoidable and belongs in the baseline measurement
as its own row.  A floor that grows without discipline reconstructs the whole-workspace matrix one
justified entry at a time.  Hence the rule: an entry needs a named reason at the definition site, or
it does not go on the list.

## Decision 1 — reuse the `scoped-tests` package set rather than derive a second one

`--lite` already computes a package set from the diff for `scoped-tests`.  Deriving a second set for
clippy would create two answers to "what does this diff affect," and they would drift.

The set is taken from the existing derivation verbatim, including the #2658 direct-dependent fan-out
for a `cqlite-core/src/` path.  Clippy lints that set.  If the two ever need to differ, that is a
separate decision with its own rationale, not a silent divergence.

**Rationale for including the fan-out set in clippy scope:** a `cqlite-core` change can introduce a
lint failure in a dependent without touching that dependent's files.  Dropping the fan-out would make
scoped lite miss exactly the class the fan-out was added for.  The cost is that a core-src diff is
barely narrowed, which figure 2 of the proposal's measurement reports honestly.

## Decision 2 — the feature flags come from the existing four-stage matrix, per package

The scoped run must not quietly lint a package at a different feature set than the whole-workspace
matrix would.  A package linted at narrower features can pass where the gate of record fails, which
converts a cost fix into a false green.

`run_clippy` at `scripts/agent-gate.sh:11193` is four stages, and stages 2 and 3 carry **explicit
per-package feature strings** for `cqlite-core` and `cqlite-cli` (both excluding the OpenTelemetry
stack, whose drift guard is the nightly `CQLITE_CLIPPY_FULL=1` pass, #2662).  Stage 1 is the workspace
minus five packages at `--all-features`; stage 4 is `cqlite-flight` plus the bindings at defaults.

So scoping selects **which** stages run and **which** packages within stage 1, and changes nothing
about the feature strings.  This is a filter over an existing matrix, not a new invocation.  A
consequence worth stating: stage 1 excludes `cqlite-core`, `cqlite-cli`, `cqlite-flight`,
`cqlite-py`, and `cqlite-node` precisely so `--all-features` never activates their duckdb or otel
features, so the filter must preserve that exclusion set rather than re-deriving it from the scope.

## Decision 3 — the disclosure line carries counts and a log pointer, never file or package names

`#3402` and `#3401` settled this for `file-size`, and the reasoning transfers.  Rendering repository
content inline produced three review findings in three rounds, one per attempt at mangling a name.
The ruling from #3229 applies: remove the mechanism rather than carve it a fourth time.

So the SUMMARY line carries gate-authored text plus computed integers, and the package names live in
`clippy.log`.  The `_status_detail` boundary already strips `[:cntrl:]` under `LC_ALL=C` and
withholds any value carrying the completion probe's `RESULT:` token; the scoped line goes through it
unchanged.

## Decision 4 — a failed derivation SKIPs, and the SKIP names the cause

Three states must be distinguishable in the summary:

| State | Line |
|---|---|
| Scoped run, clean | `clippy: PASS [scoped] — N of M packages CHECKED; K NOT CHECKED` |
| Scoped run, clean, nothing excluded | `clippy: PASS [scoped] — M of M packages CHECKED; 0 NOT-CHECKED RECOGNISED` |
| Derivation unmeasurable | `clippy: SKIP — blast-radius derivation failed (<named cause>); scope unknown` |

The third must never be a `PASS`.  A permissive branch on an unmeasurable scope is how a check that
ran against zero packages reports success.  `cargo metadata` failing, a path resolving to no
workspace member, or a base ref that does not resolve are each a named cause.

## Decision 5 — the regression test is mutation-checked, and the mutations are named

Wired into `tooling-tests`.  Four mutations, each of which the test must red on:

1. Scoped set replaced by the empty set.  Catches the vacuous branch.
2. Disclosure line removed from the summary.  Catches a silent narrowing.
3. Fan-out set dropped from the scope for a `cqlite-core/src/` path.  Catches the dependent-breakage
   regression that Decision 1 exists to prevent.
4. **`cqlite-py` removed from `FLOOR`.**  Catches the #1893 regression, which is the one mutation a
   reviewer reading only the diff would not think to look for, because the invariant it breaks lives
   in a different component's SKIP logic.

A test that passes against the broken implementation pins nothing.  Each mutation is applied and
reverted in the test's own scratch copy, never in the checkout, so the run cannot perturb the tree
and trip #2926's mid-run mutation check.

## Decision 6 — `CLAUDE.md` is corrected in the same diff

The Lite row currently documents "clippy is NOT diff-scoped" and gives the bands that follow from it.
On merge that text describes the old behaviour.  Doctrine that contradicts the code is worse than
absent doctrine, because an agent reads it and plans around a cost that no longer exists.

The corrected row states the scoped default, keeps the `CQLITE_CLIPPY_FULL=1` escape, and cites the
new measurement artifact for the bands rather than restating them.

## What is not designed here

The `--all-targets --no-run` fan-out (slice 2) shares Decision 1's package set and will reopen the
question of whether compile-checking dependents belongs in a fast loop at all.  That question is
deliberately not answered here.  Slice 1 changes what clippy lints; it changes nothing about what
gets compiled.

## Cited authorities

All line numbers below were verified against `scripts/agent-gate.sh` at the time of writing.
`CLAUDE.md`'s Lite row cites `:17233`, `:18220`, and `:9357` for the same facts; those have drifted
and are corrected by this change.

- `run_clippy` definition, four-stage whole-workspace matrix: `scripts/agent-gate.sh:11193`.
- `CQLITE_CLIPPY_FULL=1` whole-workspace escape: `:11194`.
- Nightly `clippy-full` backstop that justifies the existing feature narrowing: `:11186-11192`, #2662.
- The #1893 `cqlite-py` compile-backstop invariant: `:11214`.
- `run_lite` and its call to the shared `run_clippy`: `:20532` and `:20546`.
- `LITE_COMPONENTS`: `:6315`.
- Core-src direct-dependent fan-out: #2658.
- Summary-detail trust boundary and the remove-rather-than-carve ruling: #3402, #3401, #3229, #3312.
- Mid-run tree mutation invalidates a run: #2926.
- Mutation-checked regression precedent: #3465.
