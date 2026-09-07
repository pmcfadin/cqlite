# Proposal: `--lite` pays clippy IN FULL whatever the diff — scope it to the blast radius it already computes

**Milestone:** maintenance / delivery-pipeline cost · **Priority:** P1 · **Routing:** **design-driven** —
there is no external oracle for *"what must a fast pre-review check cover"*.  The deliverable is a
contract plus a measurement.  One half of it does have an oracle and it is used below: does a scoped
clippy still red on the defect class it exists to catch?  · **Intent:** `openspec/intent/active/ai-native-sdlc.md`
(slice 1 of 7) · **Refs:** #1821 (lite mode), #1844 (per-package scoped-workspace clippy), #2658
(core-src fan-out), #3763/#3764 (lite admission and disk), #3499 (deferred lint mechanization)

## Why

`--lite` runs on **every fix round**.  Its stated purpose is a cost that is a function of the diff.
One of its two cost drivers is not.

`run_clippy` is defined once, at `scripts/agent-gate.sh:11193`, and `run_lite` calls that same
function (`run_component clippy run_clippy`, `scripts/agent-gate.sh:20546`).  It is a four-stage
**whole-workspace** matrix and **it never reads the diff**.  `LITE_COMPONENTS` at `:6315` lists
`clippy` unconditionally.  Measured over 188 completed lite runs, per `CLAUDE.md`: a no-op warm,
**2-7 minutes part-warm**, **16-24 minutes cold**.

**Correction to doctrine, recorded rather than quietly fixed.**  `CLAUDE.md`'s Lite row cites
`:17233` for the lite dispatch and `:18220` for the full one, framed as two sites.  Both line numbers
have drifted and the framing is wrong: there is one definition and one shared call.  The substance of
the claim survives verification; the citations do not.  `CLAUDE.md` is corrected in this change.

`run_lite`'s own text disagrees with itself and is corrected too: the function comment at `:20529`
says "FULL-workspace clippy" while the banner it echoes at `:20539` says "scoped workspace clippy."

### The precedent this change extends

`run_clippy` is **already** a narrowing, and its justification is already the one used below.  Its
header at `:11186-11192` records that the historical `--workspace --all-targets --all-features` pass
runs nightly in a dedicated parallel `clippy-full` job in `gate.yml` (#2662), so the per-package
matrix in the gate of record is a deliberate reduction backed by a within-24h backstop.  Scoping
`--lite` by blast radius is the same argument one step further, with the **gate of record** as the
backstop rather than a nightly lane.  This is not a new kind of concession.

The other driver, `scoped-tests`, *is* diff-scoped.  So `--lite` already derives a package set from
the diff: the touched package by longest-prefix path match over `cargo metadata` from
`merge-base(HEAD, <base>)...HEAD` plus `git diff HEAD` over tracked files, defaulting to
`cqlite-core --lib` when no Rust package is in the diff, and for a `cqlite-core/src/` path also every
workspace member that directly declares a dependency on `cqlite-core` and owns a `--test` target
(#2658).

That set exists, is computed on every lite run, and clippy ignores it.

### What the cost buys, and what it does not

A round is not one lite run.  Delivery telemetry over 590 records: **71.4%** of PRs needed rework at
a mean of **3.32 rounds**.  So the part-warm-to-cold clippy leg is paid roughly three times per
change on top of the round that first reached lite-green.

The full gate is unaffected by this change and remains the gate of record (#719).  Whatever a scoped
lite does not check, the full gate checks once, before merge, at the unscoped matrix.  That is the
property that makes narrowing lite defensible at all, and it is why this change does not touch
`:18220`.

### The measurement that constrains the design

**To be recorded at `docs/round-artifacts/lite-clippy-scope-measurements.md` before the spec is
claimed satisfied.**  Three figures, each on this repo at a named `origin/main` sha:

1. For a narrow diff (one file, one non-core package): elapsed clippy seconds unscoped versus scoped,
   cold and part-warm, and the package count in each.
2. For a `cqlite-core/src/` diff: the same, where the scoped set includes the #2658 fan-out and is
   therefore expected to be most of the workspace.  **If scoping saves nothing on this class, the
   proposal says so rather than claiming a win it does not have.**
3. The discriminating case: a clippy violation planted in a package **outside** the blast radius.
   Scoped lite must not FAIL on it, must NAME it as unchecked, and the full gate must still FAIL.

Figure 2 is the one that can falsify the value of this change for the class that hurts most.  It is
measured and reported either way.

## What Changes

1. **`run_clippy` gains a scoped mode, taken only by the `--lite` call site.**  It takes the package
   set `scoped-tests` already derives, unions it with a declared floor (item 2), and lints exactly
   that union at the same feature flags the existing four-stage matrix uses for each package.  The
   full gate's call to `run_clippy` is untouched and keeps the whole-workspace matrix.

2. **A declared floor of always-linted packages, because one of them is load-bearing for a SKIP.**
   `scripts/agent-gate.sh:11214` records invariant **#1893**: `cqlite-py` must stay in the linted set
   because `--lite`'s python tier classifies a venv/pip/maturin toolchain failure as **SKIP** rather
   than FAIL, and this clippy pass is the compile backstop that makes that SKIP safe.  A blast-radius
   scope alone would drop `cqlite-py` from most diffs and convert a safe SKIP into a silent hole.

   So the scoped set is `blast_radius ∪ FLOOR`, `FLOOR` is a named list in one place, and `cqlite-py`
   is on it with #1893 cited at the definition.  A future floor entry needs the same treatment: a
   named reason, or it does not go on the list.

3. **The narrowing is disclosed in the LITE SUMMARY, and it is not a bare number.**  The component
   line names how many packages were checked, how many were not, and where the names are:

   ```
   clippy: PASS (0s) [scoped] — 3 of 14 packages CHECKED; 11 NOT CHECKED (named in <logdir>/clippy.log)
   ```

   `0 NOT CHECKED` is written `0 NOT-CHECKED RECOGNISED`, never a bare `0`, per the affirmative-zero
   doctrine.  A reader must not be able to confuse a full-coverage lite run with a narrowed one.

4. **An empty or unmeasurable package set is not a permissive branch.**  If the blast-radius
   derivation fails for any reason, scoped clippy **SKIPs naming the cause** and does not fall back
   to lint-nothing.  Because `FLOOR` is non-empty by construction, a set that comes back empty is
   itself evidence the derivation is broken, and it is treated as such rather than obeyed.

5. **`--delta` and `--only` are unchanged.**  `--delta` builds nothing by construction and `--only`
   is a diagnostic that is lenient by design.  `--only clippy` therefore keeps the whole-workspace
   matrix, which is what a diagnostic should do.

6. **A discriminating regression test in `scripts/tests/test_agent_gate_lite_clippy_scope.sh`,
   wired to `tooling-tests`.**  Four mutations, each of which must red it, named in `design.md`.
   Mutation-checked per #3465's precedent: a test that passes against both the fixed and the broken
   implementation pins nothing.

## Non-goals

- **Scoping the FULL gate's clippy.**  The gate of record keeps the whole-workspace matrix.  That is
  the whole reason lite may narrow.
- **Re-deciding the #1893 python SKIP.**  This change preserves that invariant via `FLOOR`.  Whether
  a toolchain failure should SKIP at all is a separate question and is not reopened here.
- **Removing the four-stage feature narrowing inside `run_clippy`.**  That reduction is #2662's, is
  backed by the nightly `clippy-full` job, and is orthogonal to package scoping.
- **A dependency-closure blast radius.**  A commit changing an item the diff *calls*, in a package
  neither touched nor a direct dependent, is not in the set.  Declared on every run.  The sound route
  is rustc dep-info, which is #3366.
- **An admission check or slot cap for `--lite`.**  #3763 owns that gap.  This change reduces what a
  lite run costs; it does not arbitrate who runs one.
- **The `cqlite-core/src/` `--all-targets --no-run` fan-out leg.**  That is the reported +18 GB in a
  single round and it is slice 2, filed separately.  Conflating them would make figure 2 above
  unreadable, because two changes would be moving the same number.
- **Removing the `CQLITE_CLIPPY_FULL=1` path.**  It stays as the explicit whole-workspace escape and
  is not the scoped default's fallback.
- **A bypass flag for scoped clippy.**  Deliberately absent.  There is no legitimate reason to want a
  lite run that lints nothing and says PASS.

## Impact

- **No-heuristics mandate:** untouched.  No decode path.  The honesty rule is the same doctrine one
  level up — the scope comes from `cargo metadata`, which is authoritative, never from a textual
  guess at which packages a diff affects.
- **Public binding surfaces (Python/Node/CLI):** none.  Delivery tooling only.
- **<128 MB memory budget:** unaffected.  No library code changes.
- **Gate cost:** this is the change's subject.  The full gate's cost is unchanged by construction;
  lite's cost is the measurement above.
- **Self-certification:** `agent-gate.sh` is read from the checkout, so this change **can** exercise
  itself on its own PR, and must.  Its own lite run should print the scoped disclosure line.  Stated
  because the neighbouring class (a `required`-registry change, a `.roborev.toml` change) cannot, and
  three lanes have been caught by assuming otherwise.
- **Doctrine text:** `CLAUDE.md`'s Lite row currently states that clippy is not diff-scoped and gives
  the bands for that behaviour.  That text becomes wrong on merge and is updated in the same change,
  not left for a follow-up.
