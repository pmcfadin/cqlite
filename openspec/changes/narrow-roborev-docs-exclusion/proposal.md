# Proposal: Narrow roborev's `docs/` exclusion so executable code under `docs/` is reviewed (issue #3229)

**Milestone:** maintenance (agent-team automation / delivery pipeline) · **Priority:** P1 ·
**Routing:** design-driven (review-gate configuration + the agent-facing rule about what can be
certified — no external oracle; the fix is a config contract plus doctrine, plus a strengthening of the
wrapper's terminal verdict) · **Issue:** #3229 ·
**Related:** #2964 (the sanctioned roborev wrapper and its vacuity traps — the mechanism that caught
this), #3217 / PR #3222 (where it surfaced; 33 unreviewed executables), #3026 and #3100 (prior
measurement PRs that shipped harness code under `docs/` the same way), **#3283** (the deferred
exclusion-set oracle — AC3 and AC4), **#3278** (roborev's compiled-in built-in deny-list, separate,
still open, still unmodelled).

## Scope after the owner's DESCOPE ruling (2026-08-04) — read this first

This change ships **AC1, AC2, AC5, AC6 and AC7**. **AC3 and AC4 are DEFERRED to issue #3283** — not
satisfied, not waived, not unmet: deferred, with their implementation removed from this change.

The deferred half was a pre-enqueue oracle in `scripts/flow/roborev-review-oracles.sh` that **predicted
roborev's effective exclusion set** (a bash port of roborev's Go `git.FormatExcludeArgs`, a TOML array
parser, a three-source config union of worktree/root/global `.roborev.toml` + `~/.roborev/config.toml`, a
`roborev config get` corroboration oracle, a trailing-slash FAIL and a survivor computation) and reported
under a summary key `census-exclusion:`. It is **deleted in full**. The ruling's reasoning:

- The false-PASS blockers review found inside that oracle were **INCREASING** across rounds 8→11
  (1, 1, 2, **3**), and **two of round 11's three defects lived in code the two preceding fix rounds had
  just introduced** — a surface where the fixes were adding defects of the very class they closed.
- **A guard with known documented false-PASSes is worse than no guard, because it invites reliance it
  cannot support.**
- **Subtraction cannot add a false-PASS.** Removing the prediction leaves the wrapper modelling *nothing*
  about roborev's exclusions, so no key can tell another key to excuse a path; every failure direction the
  deletion opens is a FAIL, under `prompt-content:`, after the review round rather than before it.
- Three known-but-unfixed defects in it — a corroboration gate, an NBSP `TrimSpace` port divergence, and
  prefix-glob holes — are removed **with their subject**, not fixed.

**This IS a reduction in coverage, and it is an acceptable one.** Naming the absent coverage exactly:
*there is no automated guard against a future `.roborev.toml` re-broadening; the regression it would catch
is a hand edit to a version-controlled file on `main`, and AC6's doctrine names the hazard in prose.*

## Why

`.roborev.toml` set `exclude_patterns = ['docs/**', '*.md']`. That setting is **path-based**, so
`docs/**` discarded *everything* under `docs/` — including programs. WS0-style measurement harnesses
live under `docs/reports/*-artifacts/` **by repo convention**, so *every* such PR carried executable
code that no automated reviewer ever saw.

**Measured on PR #3222** (#3217's measurement study): 100% of the diff was under `docs/`, and it
carried **33 executable scripts** (`.py` / `.sh` / `.bt`) — the Part A/B drivers, the off-CPU
classifier, the demangler, the counter parsers, the corpus-basis and summarisation tools. The
sanctioned wrapper returned `RESULT: FAIL` with `prompt-content: FAIL (136/136 code census paths
absent)`, both vacuity tiers red, and a token signature of **15,443 in / 89 out** against a documented
vacuous baseline of ~18.7k in / 0 cached / 53–56 out. roborev had built an **empty prompt**. (Token
figures like 398k–649k in are *observed on large diffs*, never thresholds: the wrapper's actual floor is
`ROBOREV_VACUITY_MIN_INPUT_TOKENS = 25000` with `cached > 0`, and **output is advisory only** because a
genuine *clean* review emits 20–60 output tokens — indistinguishable from a vacuous one.)

The wrapper catching it is a **detection, not a fix**. The standing outcome was "this class of PR can
never be roborev-certified", and the compensating control — a hand-run adversarial review of every
executable — is not a process to rely on. (On #3222 that hand review found no blockers but did find a
fourth silent-failure instance and two provenance defects; the owner accepted it for that PR
explicitly **not** as a standing process.)

### Settled: `exclude_patterns` WORKS — the existential risk to this change is closed

This change had one way to be worthless. Issue **#3234** had independently measured that
`exclude_patterns` has **"no observable effect"** — a null result. If that were true, AC1's narrowing would
be cosmetic. The owner had ranked hypothesis **H2**: *config resolves from the primary checkout, not the
worktree.*

Both halves turned out to be operative, established from opposite directions:

- **The mechanism half** — this change: the disassembly of `git.FormatExcludeArgs` plus a 21-review replay
  in which every dropped path was a `.md` at arbitrary depth and no non-`.md` was ever dropped. The
  exclusion is real.
- **The ordering half** — #3234, independently: its single daemon restart happened to **precede every
  config edit it made and never follow one**. Since roborev resolves `exclude_patterns` from the repo
  **ROOT path** and **snapshots it at daemon start**, its edits could not have taken effect. This change
  hit the same property from the other side.

**Conclusion: `exclude_patterns` works. #3234's null result was a worktree-config artifact, not a broken
mechanism.** So AC1 is a genuine fix. Two workers reaching the same property from opposite ends is stronger
evidence than either alone, and the arbitration is recorded here rather than left implicit.

**Recorded for #3283 — no code in this change relies on it.** How the exclusion semantics were established
(symbol inspection, the 21-review `reviews.db` replay, the disassembly of `git.FormatExcludeArgs`, the
root-path/daemon-snapshot ordering) is kept in `design.md` as measured primary-source knowledge, because
#3283 will need it. It no longer describes anything this change implements. With one caveat #3283 must
honour: **reading those instructions is not the same as reproducing them** — the removed port re-derived
Go's trim rules in bash and diverged on U+00A0, because it was tested against a *model* of Go rather than
against Go.

What remains materially true and load-bearing for AC1: **`*.md` alone already excludes all ~1404 tracked
`.md` files repo-wide**, so the blanket `docs/**` bought nothing that `*.md` did not already provide for
prose, and it cost the review of every program the repo ships beside a report.

## What Changes

1. **The exclusion is narrowed to a prose/artifact deny-list (AC1) — the fix the issue was filed for.**
   `*.md` stays (it already does all prose exclusion, repo-wide). The blanket `docs/**` is replaced by
   **docs-scoped exclusions of the non-code artifact extensions only**, each scoped to an
   artifact-bearing DIRECTORY (`<artifact-dir-glob>/**/*.<ext>`) — the raw run output and binary/image
   blobs that make a report's artifact directory expensive (`txt json jsonl log err csv png svg gz pdf jfr
   html mmd tex diff`) — so `.py` / `.sh` / `.bt` / `.c` / `.rs` / `.toml` / `.cql` / `.yml` / `.yaml`
   under `docs/` become reviewable. A deny-list is **forced**, not chosen: `exclude_patterns` has no
   negation/allow-list support (git pathspec has none inside `:(exclude)`), so "review these extensions"
   is not expressible. **Measured on the shipped value: 72 `docs/` executables now reach the reviewer, 0
   markdown does, and nothing outside `docs/` is newly excluded.**
2. **AC3 and AC4 are DEFERRED to #3283.** The pre-enqueue prediction of roborev's effective exclusion set,
   and with it the reconciliation of the wrapper's extension-based census against roborev's path-glob
   exclusion, are removed from this change. See the scope note above for the ruling and its reasoning.
   **A guard with known documented false-PASSes is worse than no guard, because it invites reliance it
   cannot support.** A path the reviewer does not receive still fails **closed** — under `prompt-content:`,
   after the review round rather than before it. roborev's compiled-in built-in deny-list stays a separate,
   still-open, still-unmodelled thing under **#3278**.
3. **The canonical path-normalisation boundary is retained (AC5's structural half).** `git diff … -z`
   everywhere, RAW paths as the single internal representation, ONE unquoter (`roborev_unquote_path`)
   called from exactly ONE matcher (`roborev_diff_header_has_path`), and `prompt-content:` as the consumer
   that reads it. Six review blockers were all path-normalisation defects in a different consumer each
   time; the boundary is what closes the shape rather than the symptom, and it is pinned by structural
   asserts.
4. **The verdict-grammar closure and the affirmative-measurement backstop are RETAINED and STRENGTHENED.**
   Both now match the **VERDICT TOKEN — the value up to its first space — EXACTLY, never as a prefix
   glob**: a `PASS*` glob accepted `PASSthisNeverRan` and `PASS-MEASUREMENT-DID-NOT-HAPPEN`, so the closure
   was checking a *spelling* rather than a *state*. The affirmation backstop names **six** deterministic
   keys with **no per-key exemption** (the one exemption that existed belonged to the removed key and went
   with it, which is stricter). Two new guard cases `cx28b`/`cx28c` pin the near-prefix mutants: a mutation
   reverting to prefix globs makes both mutants reach `RESULT: PASS`, proving the asserts bite. The
   retained-ness is deliberate — the permissive verdict scan was a **pre-existing** defect this change's
   sweep found, not something the removed oracle introduced, and leaving the wrapper worse than we found
   it would be a bad trade.
5. **A recorded live demonstration (AC2) — POST-MERGE, with a named trigger.** The demonstration cannot be
   pre-merge: roborev resolves `exclude_patterns` from the **repo ROOT path** and snapshots it at daemon
   start, so the narrowed set does not apply to this change's own review. The **primary evidence is the
   first post-merge PR that happens to carry an executable under `docs/`** (strictly better than a probe
   written to pass: the diff was not shaped for it); the committed procedure in
   `docs/reports/3229-artifacts/live-probe-procedure.md` is the fallback. The obligation is held by
   mechanism, not goodwill: on merge the issue goes to **`In Review`, not `Done`**, and flips to `Done`
   only once the evidence is posted; undelivered within a few days, it is filed as a tracked issue.
   `website/src/content/docs/reports/_3229-artifacts/_3229-root-anchoring-probe.json` stays on the branch
   as a root-anchoring specimen: a deny-listed extension under a *nested* `docs/reports/*-artifacts/`
   directory is still delivered to the reviewer, because a pattern containing an interior `/` is passed
   verbatim.
6. **Hermetic regression tests (AC5).** `scripts/tests/test_roborev_review_guard.sh` keeps and extends its
   `(cx*)` family in its existing style: executables under `docs/` yield a PASS-eligible census, are not
   code-free, and REACH the reviewer's prompt (measured against the prompt actually sent); a prose-only
   diff under `docs/` still reports `code-free: FAIL` and is never enqueued; every hostile-path shape git
   emits is covered; and `cx28`/`cx28b`/`cx28c`/`cx29` pin the exact-token verdict grammar and the
   affirmation backstop. The suite deliberately contains **no** case asserting what roborev's exclusion set
   would do to a given census, because no code predicts it any more — and the fixtures no longer supply a
   `.roborev.toml` at all, since an inert input that reads as load-bearing is the same class of misleading
   test.
7. **Doctrine in the same change (AC6).** CLAUDE.md's roborev rule 4 and the published
   `agents-developing/roborev-findings` page are amended so neither implies that *everything* under
   `docs/` is code-free; the `docs/reports/*-artifacts/` harness convention is named explicitly; the
   true mechanism (configured git pathspec exclusions, not a code/non-code judgement by the reviewer) is
   stated; and the re-broadening hazard is named in prose, since it is what now stands in for the deferred
   automated guard. Every drifting copy of the old claim across the wrapper's own comments, the `.claude/`
   agent/skill surfaces, `delivery-pipeline.md` and the `roborev-review-guard` spec is corrected in the
   same change. Publication is accepted by **grepping the served page for a distinctive new phrase**,
   never by HTTP 200.
8. **The backfill ruling is recorded (AC7).** Whether the already-merged, never-reviewed harness code
   from #3026 / #3100 / #3217 gets a retroactive review pass or is explicitly accepted as-is **with the
   reason** is recorded in the change. **RULED (owner, 2026-08-03): accept as-is, no retroactive pass**;
   the four-part reasoning is in `design.md` D7.

## Non-goals

- **Not predicting roborev's exclusion set.** Deferred to **#3283** by owner ruling (see the scope note).
  Nothing in this change models what roborev excludes — neither the configured patterns nor the
  compiled-in built-ins (**#3278**).
- **Not relocating measurement harnesses out of `docs/reports/*-artifacts/`.** The owner has ruled: the
  convention of shipping a harness beside the report it produced stays. No move is proposed here, and
  none is smuggled in as tidying.
- **Not broadening the reviewer's diet to prose.** `*.md` and the other prose extensions stay excluded.
  The point is to stop excluding programs, not to start reviewing documentation text.
- **Not touching the four vacuity/SHA traps (T1–T4) or the `--agent`/`--model` enforcement.** Those work
  and are not in question; this change strengthens the terminal verdict *around* them and reuses their
  summary-block and exit-code contracts otherwise unchanged.
- **Not re-doing #3222's substitute review.** It happened, found no blockers, and is recorded in the PR.
- **Not patching or forking `roborev`.** It is an external stripped binary. This change configures it
  correctly.
- **Not fixing `scripts/ci/classify-docs-only.sh`.** Its `docs/*` classification has the *same* defect
  in the correctness gate (a PR touching only `docs/reports/*-artifacts/*.sh` is classified docs-only and
  short-circuits `pr-gate-core`). It is named in `design.md` and filed as a **follow-up**, deliberately
  out of scope here so a review-gate change and a correctness-gate change are not entangled in one PR.
- **No Rust code, no library surface, no on-disk format work.** Nothing touches `cqlite-core`, the
  bindings, the CLI, the no-heuristics decode path, or the <128MB memory budget.

## Impact

- **Config:** `.roborev.toml` — `exclude_patterns` replaced by the directory-scoped prose/artifact
  deny-list.
- **Wrapper:** `scripts/flow/roborev-review-oracles.sh` keeps the census, the docs-scoped artifact
  extension/directory constants and the path-normalisation boundary, and **loses** the exclusion-prediction
  oracle in full (the port, the TOML array parser, the three-source union, the corroboration oracle, the
  trailing-slash FAIL, the survivor computation). `scripts/flow/roborev-review.sh` loses that key's state,
  its emit line, its call site and its verdict-scan registration, and gains the **exact-token** verdict
  grammar + affirmation backstop. `scripts/flow/roborev-review-checks.sh` keeps `prompt-content:`
  expecting **every** code census path, with no subtraction and no excusal.
- **Summary block:** **22 keys** (down from 23 — the removed key is gone from the fixed order, and its
  absence is asserted structurally so the removal is visible in the OUTPUT contract, not only in the
  source). No key is added.
- **Tests:** `scripts/tests/test_roborev_review_guard.sh` (hermetic; stub `roborev` first on `PATH`, no
  network / cargo / real reviewer) — **477 passing assertions** (down from 644 with the oracle's case
  families removed, plus the new `cx28b`/`cx28c`). It runs in the `roborev-lints` gate component, which is
  in **both** the full and `--lite` component sets, so a regression FAILs the fast loop.
- **Doctrine surfaces:** `CLAUDE.md` (roborev rule 4 + the T3 sentence + the mechanized-lints table
  region), `website/src/content/docs/agents-developing/roborev-findings.md`,
  `website/src/content/docs/agents-developing/delivery-pipeline.md`, `.claude/agents/flow-lead.md`,
  `.claude/agents/flow-closer.md`, `.claude/skills/flow-implement/SKILL.md`, the three
  `scripts/flow/roborev-review*.sh` header comments, and
  `openspec/specs/roborev-review-guard/spec.md` (via this change's delta).
- **No-heuristics mandate (#28):** unaffected — that mandate governs on-disk TYPE/format inference in
  the SSTable read path. Nothing here infers anything; the change is a configuration narrowing plus a
  strictly stricter verdict scan.
- **Public binding surfaces (Python/Node/CLI), memory budget:** untouched.
- **Deferred, with issues:** the exclusion-set oracle → **#3283**; roborev's compiled-in built-in
  deny-list → **#3278**.
- **Follow-up filed, not fixed here:** `scripts/ci/classify-docs-only.sh` `is_docs_file()`'s blanket
  `docs/*` case (same defect class, correctness gate rather than reviewer).
