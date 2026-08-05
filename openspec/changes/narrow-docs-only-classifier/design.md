# Design: narrow the docs-only correctness-gate classifier (issue #3250)

## Context

`scripts/ci/classify-docs-only.sh` decides whether `pr-gate-core` — the compute half of the `required`
branch-protection check — runs at all. It returns docs-only for any path under `docs/`, extension-blind.
Since this repository ships measurement harnesses under `docs/reports/*-artifacts/` by convention, three
merged PRs (#3222, #3081, #3216) reported `required` green in 13–16 seconds having compiled and tested
nothing. #2910's tier aggregation was checked and does **not** mitigate it: `flight-ci.yml:148` leaves
`docs/**` deliberately unmandated, so the one registered tier emits not-applicable success.

This is the same bug **class** as #3229 — a path glob that swallows executables under `docs/` — in the
**correctness gate** rather than the reviewer. #3229 merged first, deliberately, so that the docs-scoped
artifact declaration exists in exactly one place before a second consumer appears.

**Owner rulings at Seam 1 (2026-08-04), in force and not re-litigated below:**

- **D2(a)** — directory-scope the code-bearing formats (`json`, `html`). Where AC3's literal text and
  CLAUDE.md's *"an extension describes a FORMAT; a directory records an INTENT"* rule disagree, the
  doctrine wins; AC3's negative case (prose + artifacts still short-circuits) is delivered against the
  **inert** extensions instead. See D3b.
- **D3(a)** — add `core.quotePath=false` to `pr-gate.yml`'s existing `git diff --name-only` in the
  `Classify docs-only diff` step; do **not** switch to `-z`. See D5.
- **AC7 / D1(a)** — no retroactive core-gate run; one ruling covers #3229 AC7 and #3250 AC7. Recorded
  verbatim with its evidence in **D7.RULING**.

Two constraints from #3229's post-mortem govern this design and are treated as given:

- **Fix the BOUNDARY, not the sites.** #3229 burned three review rounds patching one path-normalisation
  consumer at a time. Here: ONE decision point, pinned structurally.
- **A symmetric mirror of a production constant is not a test.** A test that copies the constant shares any
  defect in it, so both sides agree and the suite is green while broken — #3042's blindness in shell.

---

## D1 — the decision point: one function, an affirmative allowlist, a closed grammar

`is_docs_file()` keeps its existing shape and its sensitive-directory escape
(`.github/`, `scripts/`, `test-data/` ⇒ `full`). The blanket `docs/*) return 0` arm is replaced by a
**single dispatch**:

```
is_docs_file(path):
  1. sensitive dirs (.github/ scripts/ test-data/)              -> full     [unchanged]
  2. path is not a raw repo-relative spelling (begins with `"`)  -> full     [new, D5]
  3. path is under docs/  -> return docs_path_is_documentation(path)         [the ONE dispatch]
  4. global allowlist: *.md|*.markdown | images | LICENSE*|NOTICE* -> docs   [unchanged]
  5. otherwise                                                   -> full    [unchanged]
```

`docs_path_is_documentation()` is the **only** place a path under `docs/` is classified, and it classifies
by **layer**, each layer an affirmative match:

| layer | rule | source of the rule |
|---|---|---|
| **L1 prose / image / legal** | `.md`, `.markdown`; `.png .jpg .jpeg .gif .svg .webp .ico`; `LICENSE*`, `NOTICE*` — at any depth | the classifier's **own** path semantics, unchanged from today |
| **L2 inert report artifact** | extension in the *inert* bucket of the imported `CODE_FREE_ARTIFACT_EXTENSIONS` — at any depth under `docs/` | **imported** (#3229), bucketed here (D3b) |
| **L3 code-bearing artifact** | extension in the *code-bearing* bucket **AND** `roborev_path_in_artifact_dir(path)` | **imported** (#3229), scoped by directory (D3a) |
| **fail-closed** | anything else: unknown extension, **no** extension, unassigned imported extension | AC1 |

Ordering is presentational only: the layers are a disjunction over disjoint extension sets, so permuting
them cannot change an answer. That is stated because #3229's own endpoint fold had to make the same claim.

**Why a function rather than more `case` arms.** The defect is not that one arm is wrong; it is that a
verdict is reachable from a *path shape* instead of from a *named class*. Adding arms leaves the next
`docs/`-shaped arm one edit away. A single dispatch makes "classify a `docs/` path" a thing with one
address, which is what the structural assert can then pin (D6).

**Closed grammar, not "absence of a bad signal".** `docs_path_is_documentation` returns docs-only only from
an L1/L2/L3 match. There is no `else return 0`, no "not obviously code ⇒ prose". This is #3229's
affirmative-measurement rule applied at the leaf: a positive verdict requires a positive match.

### Alternatives rejected

- **Add an executable-extension deny-list under `docs/`** (`case docs/*.sh|docs/*.py|…) return 1`). This is
  the "patch the sites" shape, and it is **fail-OPEN**: the next extension someone commits under `docs/`
  (`.rb`, `.jq`, `.mjs`, a Dockerfile) is documentation by default. AC1 mandates fail-closed on an
  unrecognized extension, which a deny-list cannot deliver.
- **Consult the git executable bit**, as #3229's census does for extensionless paths. Rejected on evidence,
  see D3d.
- **Drop `docs/` from the classifier entirely** so `docs/x.md` falls through to the global `*.md` arm. This
  loses L2/L3 (report artifacts) and would force the full gate on every WS0 report PR — a direct AC2
  violation, and the standing 14-minute cost the issue forbids.

---

## D2 — AC5: DIRECT IMPORT, not the drift-check substitute. The evidence.

AC5 requires importing #3229's artifact declaration and never restating it; if a shared constant is
*genuinely not expressible* across `scripts/ci/` and `scripts/flow/`, the required substitute is a
mechanical check that FAILs when the two lists disagree, with a greppable reason. **Import is expressible.**
Measured, not assumed:

1. **The declaring file is designed to be sourced and says so.** `scripts/flow/roborev-review-oracles.sh`'s
   header reads *"SOURCED, never executed"*, and the declaration's own comment reads: *"THE SINGLE
   DECLARATION: it is imported, never redeclared (`scripts/ci/classify-docs-only.sh` will import it —
   issue #3250)."* This change is the import #3229 provisioned for.
2. **Sourcing has no side effects.** Probed under `set -euo pipefail`: exit 0, **zero bytes** on stdout and
   stderr, shell option string unchanged (`ehuBc` → `ehuBc`), no environment variable required, 11 function
   definitions and the constants defined. The file contains **no** `set`/`shopt` line, so it cannot mutate
   the caller's options — which matters because the classifier runs under `set -euo pipefail`.
3. **The path exists in CI.** `pr-gate.yml`'s only checkout is `actions/checkout@v5` with `fetch-depth: 0`
   — the whole repository, so `scripts/flow/roborev-review-oracles.sh` is present in the `pr-gate-core`
   workspace. The classifier is also run from the repo root by the gate's `tooling-tests` component and
   standalone by developers.
4. **The directory matcher comes with it.** `roborev_path_in_artifact_dir` is a pure function over its
   argument plus `CODE_FREE_ARTIFACT_DIR_GLOBS`; it needs none of the wrapper's state. Verified in the
   probe: `docs/reports/ws0-3217-artifacts/harness/common.sh` ⇒ inside;
   `docs/observability/grafana/dashboards/cqlite-overview.json` ⇒ outside. Re-implementing it would be the
   redeclaration AC5 forbids, one level up — and its component-wise matching is load-bearing (a bash `case`
   glob crosses `/`, git's `:(glob)` `*` does not).
5. **`CODE_FREE_ARTIFACT_DIR_GLOBS` is an array, and that is why it must be imported rather than copied.**
   Its values contain `*`; iterating an unquoted string form pathname-expands them against `$PWD`, which
   silently degrades the classification to "the directories that happen to exist in this checkout". The
   declaration documents this as a measured hazard. A copy would have to re-earn that.

**So the design takes (a) direct sourcing.** Hermeticity is preserved: the import is a local file read, no
network, no cargo, no toolchain, and the self-test stays pure shell.

**The failure direction is closed, because an import can fail.** The classifier verifies **after** sourcing
that the file existed, sourced successfully, and that `CODE_FREE_ARTIFACT_EXTENSIONS` is non-empty and
`CODE_FREE_ARTIFACT_DIR_GLOBS` has ≥1 element. Any failure ⇒ **every** path under `docs/` classifies `full`
and the classifier prints a named reason. It never degrades to L1-only-and-carry-on, because that would be
a *more permissive* gate produced by an infra fault — the shape #3229 named as "an unmeasurable input
reaching a permissive branch".

**The import is resolved relative to the script's own location**
(`$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../flow/roborev-review-oracles.sh`), never relative to
`$PWD`. Two reasons: the classifier is invoked from varying working directories, and location-relative
resolution is what makes the mutation test of D6 possible — a temp tree with a mutated declaration must be
able to change the verdict.

**AC5's drift-check substitute is still delivered, aimed at where drift can now occur.** With the list
imported, the list cannot drift. What *can* drift is the **bucketing**: #3229 may add an extension to
`CODE_FREE_ARTIFACT_EXTENSIONS`, and this classifier must decide whether the new extension is inert or
code-bearing. So the self-test asserts the buckets **partition the imported set exactly** — union equals
the imported set, buckets pairwise disjoint — and an extension in neither FAILs with a greppable reason
naming the extension and this issue. That is a mechanical disagreement check, and it fails **closed**: an
unassigned extension also classifies `full` at runtime.

---

## D3 — the subtlety: are #3229's answer set and this classifier's the same?

**No, and conflating them would be the next drift.** They ask different questions:

- **#3229 asks:** *is this path CODE for the purpose of REVIEW?* — i.e. must a reviewer see it. Cost of
  getting it wrong toward "artifact": lost review coverage. Toward "code": tokens.
- **#3250 asks:** *does this path require the CORRECTNESS GATE?* — i.e. must fmt/clippy/build/tests/oracle
  and the policy checks run. Cost toward "docs": a merged, never-compiled change. Toward "code": ~14 minutes.

They **share exactly one sub-answer**: *what counts as committed run output beside a report*. That is
precisely what `CODE_FREE_ARTIFACT_EXTENSIONS` × `CODE_FREE_ARTIFACT_DIR_GLOBS` denotes, and it is the only
thing imported. Everything else is this classifier's own path semantics. Stated as a table, because the
owner's constraint is that the boundary be *stated*, not left implicit:

| element | shared mechanism (imported) | this classifier's own semantics |
|---|---|---|
| the artifact extension set | ✅ `CODE_FREE_ARTIFACT_EXTENSIONS` | — |
| the artifact directory globs + matcher | ✅ `CODE_FREE_ARTIFACT_DIR_GLOBS`, `roborev_path_in_artifact_dir` | — |
| inert vs code-bearing bucketing of that set | — | ✅ D3b, derived from CLAUDE.md's asymmetry rule |
| prose extensions | — | ✅ `.md`/`.markdown` only (D3e) |
| images + `LICENSE*`/`NOTICE*` | — | ✅ unchanged, repo-wide (D3c) |
| extensionless paths | ❌ deliberately **not** adopted | ✅ unconditional `full` (D3d) |
| sensitive-dir escape (`.github/`, `scripts/`, `test-data/`) | — | ✅ unchanged; roborev has no analogue |

### D3a — where the answer sets legitimately COINCIDE

For every executable/config-as-code extension the repo ships under `docs/` — `.sh .py .bt .c .rs .toml
.cql .yml .yaml` — both subsystems answer **code**: none of them is in
`CODE_FREE_ARTIFACT_EXTENSIONS`. And both fail closed toward "code". So importing the artifact set is
**safe for the gate**: it is a set of things the reviewer-side already judged non-code, and the gate's
question is a *stricter* one on the same material.

### D3b — where they legitimately DIFFER: directory scoping applies to code-bearing formats only

CLAUDE.md already rules on this, and the rule is doctrine rather than a preference: *"exclusion of
code-bearing formats MUST be scoped by directory, never by extension alone"*, because *"an extension
describes a FORMAT; a directory records an INTENT"* — and the falsifying case is
`docs/observability/grafana/dashboards/cqlite-overview.json`, which the **full agent gate's own
`kit-dashboard-drift` component guards**. A classifier that calls that file documentation tells the gate to
skip on a file the gate itself treats as correctness-bearing.

The asymmetry is also **scoped** by doctrine: it holds for *inert dumps*, where a mistake costs only noise.
Applied here:

- **inert bucket** (`txt jsonl log err csv gz pdf jfr mmd tex diff`) — docs-only **anywhere** under
  `docs/`. Nothing in `pr-gate-core` reads a run dump, and no gate component treats one as a contract.
- **code-bearing bucket** (`json html`) — docs-only **only inside** an artifact directory.
- **`png svg`** — answered by L1 (the pre-existing image layer) and therefore assigned to neither bucket;
  see D3c.

Measured against the tracked tree, which is what makes this concrete rather than theoretical: of **808**
tracked `docs/` files carrying an imported artifact extension, **803 lie inside the four artifact
directories** and **5 do not**. Each of the 5 is assigned deliberately:

| path | verdict | why |
|---|---|---|
| `docs/observability/grafana/dashboards/cqlite-overview.json` | **full** | functional config; guarded by the gate's own `kit-dashboard-drift` |
| `docs/reports/delivery-telemetry.schema.json` | **full** | the schema governing the delivery ledger |
| `docs/reports/delivery-telemetry.jsonl` | **docs-only** | inert append-only ledger; the per-delivery telemetry PR keeps its seconds-long green |
| `docs/sstables-definitive-guide/pandoc-header.tex` | **docs-only** | inert typesetting fragment |
| `docs/sstables-definitive-guide/statistics-db-annotated-dump.txt` | **docs-only** | inert dump |

The `delivery-telemetry.jsonl` row is why the buckets exist at all. A purely directory-scoped rule — the
straightforward "mirror #3229 exactly" design — would force the ~14-minute core on **every**
`flow-finalize` telemetry PR, one per delivery cycle, for a one-line append no gate step reads. That is a
recurring cost with no correctness return, and it is exactly the over-narrowing AC2 forbids. Note this is a
case where **#3229's answer is `code` and the gate's is `docs`, and both are right**: a ledger line's
provenance is worth a reviewer's eye and is not worth a compiler's.

### D3c — `.svg` and `.png`: an untouched layer, and a recorded divergence

The classifier's existing image allowlist (`.png .jpg .jpeg .gif .svg .webp .ico`, repo-wide) is
**unchanged by this change**, and it answers `docs/img/x.svg` before the artifact layers are consulted.
#3229 classifies `.svg` as code-bearing and directory-scopes it. Both are correct for their own question:
roborev's concern is a script embedded in an SVG reaching a *reviewer*, whereas **no `pr-gate-core` step
reads an image of any kind**. Narrowing `.svg` here would (i) contradict AC2's explicit listing of `.svg`
and `.png` as still-short-circuiting, (ii) alter the classifier's behaviour for `.svg` **outside** `docs/`
as well, and (iii) buy **zero** files: the measurement above shows no tracked `.svg` under `docs/` outside
the four artifact directories. So the divergence is recorded rather than harmonised, and `.svg`/`.png` are
assigned to the third bucket ("answered by L1") so the partition assert still covers the imported set.

`.jpg`/`.gif`/`.webp`/`.ico` are **not** in the imported set at all, which is exactly why L1 must remain
authoritative for paths under `docs/`: without it, `docs/img/photo.jpg` would fail closed to `full` and a
prose PR carrying a photo would pay the full core. That is a real trap in the "let the artifact layers
decide everything under `docs/`" design, and it is why the layers are a disjunction with L1 inside it.

### D3d — the executable bit is deliberately NOT adopted

#3229 classifies an **extensionless** path under a prose prefix as non-code iff git records it
non-executable at **both** endpoints of the review range. This classifier does **not** adopt that rule, and
the reason is not tidiness:

1. **AC1 mandates the opposite**: extensionless under `docs/` ⇒ `full`, unconditionally.
2. **The classifier's input carries no mode.** Its contract is a newline-delimited path list on stdin
   (`git diff --name-only` in `pr-gate.yml`). Adopting the mode rule would mean the classifier running
   `git ls-tree` itself — turning a pure, hermetic, self-testable function into one that needs a repository
   and two resolvable refs, with #3229's own tri-valued "unmeasurable" hazard imported along with it.
3. **A mode flip must not decide gating.** `chmod -x` does not turn a program into prose (#3229 round-13
   found exactly that defect on its own rule), and here the consequence would be an ungated merge.
4. **The cost is zero.** All **3** tracked extensionless files under `docs/` are mode 100755 harness
   binaries (`ws0-readbw`, `ws0-stream`, `offcputime-bigmap`) — every one of which *should* force the full
   path. There is no tracked extensionless prose file under `docs/` to penalise.

So: shared mechanism = the artifact intersection. **Not** shared = the exec-bit rule, the prose extension
set, the image/legal layer, and the sensitive-dir escape.

### D3e — the prose set is NOT imported

#3229's `CODE_FREE_EXTENSIONS` is `md markdown mdx txt rst adoc`. Importing it as the gate's global prose
layer was considered and rejected: it applies **repo-wide**, so `.txt`/`.rst` would become docs-only
outside `docs/` too — and a `.txt` outside `docs/` can be a golden fixture whose edit genuinely changes a
test's outcome. That would open a **new** hole in a change whose purpose is to close one. `.txt` is reached
under `docs/` via L2 instead, which is where AC2 wants it and nowhere else.

---

## D4 — the roborev v0.61.2 pin: what is inherited, and what is not

Stated precisely, because conflating the shared mechanism with this classifier's own semantics is the drift
the owner named:

- **NOT inherited:** the pin's *subject*. `roborev v0.61.2 git.FormatExcludeArgs` describes how roborev
  turns a configured pattern into git pathspecs (interior-`/` ⇒ root-anchored verbatim; slash-less ⇒
  `**/`-prefixed recursive; trailing slash inverts the anchoring; two pathspecs per pattern). **This
  classifier feeds no pathspec to roborev, reads no `.roborev.toml`, and depends on none of those
  semantics.** Its own matching is `roborev_path_in_artifact_dir`'s component-wise walk, which is
  deterministic bash over a committed array.
- **INHERITED:** the pin's *consequence for the declaration's content*. The declaration exists to agree
  with `.roborev.toml`, whose correctness was established against v0.61.2. If a roborev upgrade changes
  `FormatExcludeArgs`, `.roborev.toml` may be re-derived and the declaration may move with it — and the
  declaration now moves **the correctness gate** as well as the reviewer. So the re-verify-on-upgrade
  obligation gains a clause: after any roborev upgrade, re-verify that the artifact extension/directory
  declaration still means what the **gate** needs, and re-run `test_classify_docs_only.sh`, whose bucket
  partition assert is the mechanical half of that check.
- **A second consumer changes the declaration's status.** Before this change, editing
  `CODE_FREE_ARTIFACT_EXTENSIONS` affected review coverage. After it, the same edit can change which PRs
  skip the correctness gate. That is recorded at the classifier's import site and in the spec, because it
  is the kind of coupling that is obvious for one release and invisible afterwards.

---

## D5 — the raw-path boundary (#3229's six-blocker lesson, applied once)

`pr-gate.yml` produces the changed-file list with `git diff --name-only`, which **C-quotes** any path
containing non-ASCII or control bytes (`core.quotePath` defaults on). This repository tracks 40
space-bearing paths under `docs/` and prose files with em dashes in their names. Consequences today, and
under the new design:

- A quoted path like `"docs/\303\251.md"` has apparent extension `md"`. Under the old blanket `docs/*` it
  did not even match (the spelling begins with `"`), so it already forced `full` — no regression, but no
  correctness either: the verdict came from an accident of spelling.
- Under the new design the extension is what decides, so an accidental read of `md"` — or worse, a future
  reader "fixing" the quoting by stripping quotes and re-splitting — is the exact class of defect that cost
  #3229 six blockers across six different consumers.

So the boundary is fixed **once**, in both directions:

1. `pr-gate.yml` computes the list with `git -c core.quotePath=false diff --name-only`, so a legitimately
   non-ASCII prose path arrives **raw** and is classified on its real extension. (`-z` is *not* used: it
   would change the classifier's stdin contract from newline- to NUL-delimited, a larger and unnecessary
   change. `core.quotePath=false` is the minimal fix for the actual failure mode.)
2. The classifier **fail-closes** on any path spelling it cannot read as raw — one beginning with `"` — with
   a named reason. Control characters (a literal newline in a path) are still quoted by git even with
   `quotePath=false`, and remain unrepresentable in a newline-delimited stream; that residual is closed by
   fail-closing rather than by pretending it cannot happen.

Nothing else about `pr-gate.yml` changes: no trigger filter, no step-gating change, no `required` change.
The suite's existing Ruby contract assertion continues to pin all of that.

---

## D6 — pinning the boundary: structural asserts and a real mutation

Behavioural cases only cover shapes someone already thought of, so the suite gains asserts about the
classifier's **structure** and about the **real declaration**:

1. **No second decision site.** No `case` arm anywhere in the classifier may return docs-only for a
   `docs/`-prefixed pattern; the only `docs/` mention outside the dispatch is the dispatch's own guard. A
   reintroduced `docs/*) return 0` FAILs.
2. **No literal copy of the imported lists.** The classifier's source must not contain the artifact
   extension list nor any of the four directory globs as literals. This is the AC5 assert that bites if
   someone "simplifies" the import away.
3. **A mutation of the REAL declaration changes the verdict.** The test copies the classifier and the
   *real* `roborev-review-oracles.sh` into a temp tree, mutates the copy's declaration (adds a synthetic
   extension; removes a directory glob), and asserts the classifier's verdict follows. If the classifier
   ignored the declaration — the symmetric-mirror failure — the verdicts would not move and the test FAILs.
   This is why the import is location-relative (D2).
4. **The buckets partition the imported set** exactly, read from the real declaration, never from a mirror.
5. **The verdicts themselves** stay behavioural, in the existing `assert_docs_only`/`assert_full` style, so
   the suite reads as one thing.

**D6a — how the declaration-mutation scenario is delivered, and why it is two-sided.** The scenario as
written ("a synthetic inert extension added [upstream] ⇒ `docs-only`") cannot hold together with AC1's
fail-closed rule, because an imported extension this classifier has **not** bucketed must classify `full`
(otherwise a new upstream extension would become documentation by default — the fail-OPEN shape AC1
forbids). Both properties are therefore delivered, as three measured sub-cases against a temp tree holding
a copy of the classifier beside a copy of the **real** declaration:

| mutation | path | verdict | proves |
|---|---|---|---|
| `txt` **removed** from the declaration | `docs/reports/ws0-3217-artifacts/results/run.txt` | `docs-only` → **`full`** | the verdict follows the declaration's content |
| one directory glob **removed** | `docs/round-artifacts/soak/report.html` | `docs-only` → **`full`** | the verdict follows the declared globs, through the imported matcher |
| synthetic `zzz` added **upstream only** | `.../out.zzz` | stays **`full`**, and the partition assert FAILs naming `zzz` + `#3250` | AC1's fail-closed rule for an unbucketed imported extension |
| synthetic `zzz` added **upstream AND bucketed here** | `.../out.zzz` | `full` → **`docs-only`** | the scenario's letter: a declared+assigned extension is documentation |
| synthetic `zzz` **bucketed here only** | `.../out.zzz` | stays **`full`** | the DECLARATION is authoritative — a classifier carrying its own hardcoded list would answer `docs-only` here and fail |

The last row is the discriminator that makes this a test of the *import* rather than a mirror of the
constant: a hardcoded classifier passes rows 1–4 by accident and fails row 5 by construction.

The suite is **additive**: existing asserts and the Ruby workflow-contract block are preserved, so it
rebases cleanly over #3296, which is editing the *sibling* suite `test_roborev_review_guard.sh` — a
different file. Should the two lanes ever touch the same lines, the standing instruction is to raise
`coord:needs-attention` rather than race.

---

## D7 — AC7: the backfill ruling is the OWNER'S. Recommendation and the evidence.

AC7 asks for a **recorded decision** about the three already-merged, never-gated PRs (#3081/#3026,
#3216/#3100, #3222/#3217): a retroactive core-gate run over that harness code, or explicit acceptance with
the reason. Silence fails; either decision passes. **This change does not decide it** — it records what the
owner rules, with the reason.

**The evidence that bounds the exposure, verified rather than asserted:**

1. **The docs-hosted crate is not a workspace member.** `cargo metadata --no-deps` lists 16 packages;
   `docs/reports/ws0-3026-artifacts/ws0-cqlite/scan-harness` is **not** among them, and the root
   `[workspace] members` list does not reach under `docs/`. So `cargo fmt --all`, `cargo clippy -p
   cqlite-core`, `cargo build -p cqlite-core --all-features` and the lib tests would **not have compiled or
   formatted a single byte of it** even had they run.
2. **None of the other skipped steps reads any path in those diffs.** Workflow-policy validation reads
   `.github/workflows/**`; dataset-pin agreement, release-version agreement, the Dockerfile Rust-pin
   lockstep, `Cargo.lock` freshness and the #2644 oracle read workspace manifests, `test-data/**` and
   `cqlite-core`. A `docs/`-only diff touches none of them.
3. **Therefore a retroactive `pr-gate-core` run over those three merge commits is equivalent to running it
   on `main` at those commits** — it would report on `main`'s health, not on the harnesses. And `main` has
   been re-certified nightly since by `gate.yml`'s full-gate deep check.
4. **#3229's own AC7 ruling already covers the same three PRs, and its reasoning transfers.** Owner,
   2026-08-03: *accept as-is, no retroactive pass*, on the grounds that every affected file is a measurement
   harness that ships nowhere, is imported by nothing, and *"does not run in CI or the agent gate"* — the
   gate-side claim, stated in that ruling. Its named condition also transfers verbatim: the ruling changes
   the moment any of that harness code is **promoted** into a shipped path (a gate component, a CI step, an
   imported module), at which point it inherits the obligations of the surface it joins.

**Recommendation: accept as-is, no retroactive core-gate run** — one ruling covering both #3229 AC7 and
#3250 AC7 — with the promotion condition recorded. **Safe default if the owner prefers not to rule:** run
`pr-gate-core`'s step set once against `main` at the newest of the three merge commits and record the
result, which is cheap, harmless, and honest about proving only `main`'s health at that point. Either way
the ruling and its reason are recorded in this change before it is done; the requirement is satisfied by
the record, not by a particular verdict.

### D7.RULING — the owner's decision, as given

> **Ruling: NO retroactive core-gate run over PRs #3081 (issue #3026), #3216 (issue #3100) and
> #3222 (issue #3217). Accept as-is.**
>
> - **Date / authority:** 2026-08-04, owner decision taken at Seam 1 of this change (issue #3250).
> - **Scope:** ONE ruling covers **both** #3229 AC7 (the review side) and #3250 AC7 (the gate side) —
>   the same three PRs, asked from two directions, answered once.
> - **Reason:** every affected file is a measurement harness that ships nowhere and is imported by
>   nothing, so a retroactive run would report on `main`'s health rather than on the harnesses. The
>   exposure is bounded by the evidence below, and `main` has been re-certified nightly since by
>   `gate.yml`'s full-gate deep check.
> - **Condition of change, verbatim:** *the exemption ends the moment harness code is promoted into a
>   shipped path.* At that moment it inherits the obligations of the surface it joins (a gate
>   component, a CI step, an imported module), and the question is re-opened for that code.

**Bounding evidence, re-verified in this change rather than inherited as prose:**

1. **The docs-hosted crate is not a workspace member.** `cargo metadata --no-deps --format-version 1`
   run at this change's HEAD lists **16 packages** — `cassandra-parity`, `cqlite`, `cqlite-cli`,
   `cqlite-core`, `cqlite-examples`, `cqlite-flight`, `cqlite-integration-tests`, `cqlite-node`,
   `cqlite-py`, `cqlite-validator`, `flight-loadgen`, `format-compatibility-tests`, `format-validator`,
   `memory-safety-runner`, `sstabledump-validator`, `xtask` — and **none** of their manifest paths lies
   under `docs/`. `docs/reports/ws0-3026-artifacts/ws0-cqlite/scan-harness` is **absent** from the
   package set, so `cargo fmt --all`, `cargo clippy -p cqlite-core`,
   `cargo build -p cqlite-core --all-features` and the lib tests would not have compiled, formatted or
   linted a single byte of it even had the core run.
2. **No skipped `pr-gate-core` step reads any path in those three diffs.** Workflow-policy validation
   reads `.github/workflows/**`; dataset-pin agreement, release-version agreement, the Dockerfile
   Rust-pin lockstep, `Cargo.lock` freshness and the #2644 oracle read workspace manifests,
   `test-data/**` and `cqlite-core`. The measured diffs are 188 / 269 / 197 paths (D10), of which
   **exactly one** (`process_improvements.md`, in #3081) is outside `docs/` and it is prose.
3. **Therefore a retroactive run is equivalent to running the core on `main` at those commits** — which
   is the substance of the ruling, not an excuse appended to it.

---

## D8 — AC6: doctrine, in the same change

CLAUDE.md's #3042 paragraph currently reasons from "a markdown/docs-only diff cannot change the compiled
binary" with the qualifying test "no `src`, no `Cargo.*`, no build script, no workflow, no test-data". A
#3222-shaped diff **contains** `src/main.rs` and `Cargo.toml` under
`docs/reports/ws0-3026-artifacts/ws0-cqlite/scan-harness/`, so the qualifier is satisfied *textually* while
being false *materially*. The narrowing:

- names the `docs/reports/*-artifacts/` convention explicitly, as the reason a `docs/` path prefix is not
  evidence of non-compiled input;
- scopes the waiver to a **genuinely prose** diff, and makes `scripts/ci/classify-docs-only.sh` the
  mechanical test of that — an agent can now *run* the question rather than judge it, which is the same
  move CLAUDE.md already makes for the roborev census ("a code-free census, never a `docs/` path prefix");
- keeps everything else about CITE-AND-WAIVE intact: the waiver still requires a cited issue, and the
  "compiled input in the diff ⇒ the waiver is void" clause is unchanged.

**Website half.** `CITE-AND-WAIVE` appears in `CLAUDE.md` and **nowhere on the site** today (verified by
grep across `website/` and `docs/`). The gate doctrine page
`website/src/content/docs/agents-developing/gate-contract.md` is the correct home — it already owns "what a
green `required` covers", the component list, and the `--delta` test/docs-only re-certification section — so
the narrowed rule is added there, with a cross-reference from `roborev-findings.md`'s existing code-free
census definition so the review-side and gate-side definitions of "docs-only" cannot drift apart again.
**Publication is accepted by grepping the served page for a distinctive new phrase**, never by HTTP 200; a
`0` count means not-yet-published (the CDN has been observed serving stale content for ~3 minutes after a
successful deploy) and is re-checked, never banked.

---

## D10 — AC4: the recorded demonstration on the real merged-PR shapes

Evidence, **not** a gate assertion: the input is a historical API response, so asserting it would either
pin a network call or commit a fixture that ages. No fixture is committed and no gate assertion depends on
the network. Replayed 2026-08-05 at this change's HEAD.

**How it was obtained and replayed** (per PR `<N>` ∈ {3222, 3081, 3216}):

```bash
for page in 1 2 3; do
  gh api "repos/pmcfadin/cqlite/pulls/<N>/files?per_page=100&page=$page" --jq '.[].filename'
done > pr-<N>.txt                                   # every page, so nothing is silently truncated
bash scripts/ci/classify-docs-only.sh < pr-<N>.txt   # amended  (POST)
git show origin/main:scripts/ci/classify-docs-only.sh > pre.sh
bash pre.sh                       < pr-<N>.txt       # pre-change (PRE)
# per-path census: each path fed alone, so the offending set is measured, not inferred
```

| PR | paths | PRE verdict | POST verdict | offending | first offending path |
|---|---|---|---|---|---|
| **#3222** (issue #3217) | **188** | `docs-only` / **exit 0** | `full` / **exit 1** | **35** | `docs/reports/ws0-3217-artifacts/harness/classify-offcpu.py` |
| **#3081** (issue #3026) | **269** | `docs-only` / **exit 0** | `full` / **exit 1** | **30** | `docs/reports/ws0-3026-artifacts/ws0-corpus/.gen-p200000-pl375x6-ba96x3-bb96x16-cl2x10.yaml` |
| **#3216** (issue #3100) | **197** | `docs-only` / **exit 0** | `full` / **exit 1** | **1** | `docs/reports/ws0-3100-artifacts/ws0-h2h/schemas/ws0-events.cql` |

**The verdict CHANGED on every one of the three**, which is the point of recording the PRE column: the
`full` is attributable to this change and not to an unexplained difference in the input.

**The prose-only replay — the fast path is intact on real data.** The same lists with the offending paths
removed:

| PR | remaining paths | verdict |
|---|---|---|
| #3222 | **153** | `docs-only` / **exit 0** |
| #3081 | **239** | `docs-only` / **exit 0** |
| #3216 | **196** | `docs-only` / **exit 0** |

**What the offending sets are made of** (measured per path, so a false positive would show up here as
prose or as an inert artifact — none does):

- **#3222 — 35 paths: 17 `.sh`, 15 `.py`, 2 `.bt`, 1 extensionless** (`partB-run/offcputime-bigmap`).
  **The count reconciles with the issue's "34" rather than contradicting it:** 34 is the
  extension-bearing executable count (17+15+2); the 35th is the extensionless harness binary, which AC1
  forces to `full` unconditionally and without a mode lookup. The measured number is recorded as
  measured.
- **#3081 — 30 paths: 13 `.sh`, 6 `.py`, 3 `.c`, 2 extensionless (`ws0-readbw`, `ws0-stream`), 2 `.yaml`,
  1 `.toml`, 1 `.rs`, 1 `.cql`, 1 `.bt`.** The `.toml` + `.rs` pair is
  `ws0-cqlite/scan-harness/Cargo.toml` and `.../src/main.rs` — the docs-hosted crate that is the
  falsifying case for the old CITE-AND-WAIVE qualifier (D8).
- **#3216 — 1 path: 1 `.cql`** (a schema-as-created fixture). One path is enough: a set is `full` when
  ANY member is, so #3216's 13-second green rested on a single unclassified file.

**One path in the three diffs is outside `docs/`**: `process_improvements.md` (#3081), prose, classified
documentation by the pre-existing global `*.md` arm, unchanged by this change.

## D9 — what this change deliberately does not do

- It does not model roborev's exclusion set (#3283) or its compiled-in built-ins (#3278). It consumes a
  committed declaration; it predicts nothing, so it cannot false-PASS about anything.
- It does not touch `.roborev.toml`. A pattern added there without a matching declaration edit remains
  #3229's known, accepted gap; this change's partition assert now makes the *declaration* side of that
  mirror louder, which is a side benefit and not a claim to have closed #3283.
- It does not relocate a harness, filter the workflow trigger, touch the tier registry, or widen the gate
  for prose.

## Follow-ups (named, not fixed here)

- **A `pr-gate-core` step that covers docs-hosted harness crates.** Nothing compiles
  `docs/reports/*-artifacts/**/Cargo.toml` today; the fix in this change makes such a PR *pay* the core, but
  the core still does not build the harness. Whether it should is a separate scope question (and D7's
  promotion condition is the doctrine hook for it).
- **`.github/ci-gating-tiers.yml` has one registered tier.** Out of scope here (#2910 works as designed),
  but the measurement in this change's proposal is the concrete record of what that buys on a `docs/` PR.
