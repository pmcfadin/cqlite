# Proposal: Narrow roborev's `docs/` exclusion so executable code under `docs/` is reviewed (issue #3229)

**Milestone:** maintenance (agent-team automation / delivery pipeline) · **Priority:** P1 ·
**Routing:** design-driven (review-gate configuration + the agent-facing rule about what can be
certified — no external oracle; the fix is a config contract plus a new fail-closed wrapper check plus
doctrine) · **Issue:** #3229 ·
**Related:** #2964 (the sanctioned roborev wrapper and its vacuity traps — the mechanism that caught
this), #3217 / PR #3222 (where it surfaced; 33 unreviewed executables), #3026 and #3100 (prior
measurement PRs that shipped harness code under `docs/` the same way).

## Why

`.roborev.toml` sets `exclude_patterns = ['docs/**', '*.md']`. That setting is **path-based**, so
`docs/**` discards *everything* under `docs/` — including programs. WS0-style measurement harnesses
live under `docs/reports/*-artifacts/` **by repo convention**, so *every* such PR carries executable
code that no automated reviewer ever sees.

**Measured on PR #3222** (#3217's measurement study): 100% of the diff was under `docs/`, and it
carried **33 executable scripts** (`.py` / `.sh` / `.bt`) — the Part A/B drivers, the off-CPU
classifier, the demangler, the counter parsers, the corpus-basis and summarisation tools. The
sanctioned wrapper returned `RESULT: FAIL` with `prompt-content: FAIL (136/136 code census paths
absent)`, both vacuity tiers red, and a token signature of **15,443 in / 89 out** against a documented
vacuous baseline of ~18.7k in / 0 cached / 53–56 out (a genuine review on this repo runs 398k–649k in /
5.0k–6.3k out). roborev had built an **empty prompt**.

The wrapper catching it is a **detection, not a fix**. The standing outcome is "this class of PR can
never be roborev-certified", and the compensating control — a hand-run adversarial review of every
executable — is not a process to rely on. (On #3222 that hand review found no blockers but did find a
fourth silent-failure instance and two provenance defects; the owner accepted it for that PR
explicitly **not** as a standing process.)

**The mechanism is a disagreement between two classifiers.** The wrapper's pre-enqueue `code-free:`
check is **extension-based** (`roborev_census` in `scripts/flow/roborev-review-oracles.sh` classifies
per file by extension plus a non-code path-prefix list), so it correctly saw 136 code files and
enqueued. roborev's `exclude_patterns` is **path-glob based**, so it then discarded all 136. Same diff,
two answers, and nothing in the wrapper compares the two.

**What the exclusion actually does** was established by symbol inspection plus an empirical replay,
because `roborev` is an external stripped Go binary (`roborev v0.61.2`, `/usr/local/bin/roborev`) with
no source available:

- `exclude_patterns` is implemented as **git pathspec** exclusion (`:(exclude,glob)`), so the
  semantics are git wildmatch with `WM_PATHNAME`: anchored at the repo root, `*` does not cross `/`.
  The construction function `git.FormatExcludeArgs` was subsequently **disassembled** out of the stripped
  binary and is now fully specified (see `design.md`): a pattern with an interior or leading `/` is
  root-anchored and passed verbatim, a slash-less pattern is `**/`-prefixed, every pattern emits both
  `<p>` and `<p>/**`, a trailing slash is trimmed BEFORE the anchoring test (so `docs/` and `docs/**`
  behave OPPOSITELY), and there is **no negation/re-include capability at all** — which is why the fix
  must be a deny-list.
- Replaying **21 real reviews** from `~/.roborev/reviews.db` against their recorded `git_ref` ranges,
  the ONLY paths ever dropped from a prompt were **25 paths, every one a `.md`** — including
  `.claude/agents/*.md`, `openspec/**/*.md`, `website/**/*.md` and `CLAUDE.md`. `docs/**` cannot
  explain those, so a **slash-less pattern is applied recursively** (normalised to `**/<pattern>`).
- Therefore **`*.md` alone already excludes all ~1404 tracked `.md` files repo-wide**, and `docs/**` is
  the *sole* cause of executables under `docs/` being discarded. Every non-`.md` file in that replay
  was present in its prompt, including `.github/workflows/*.yml`, `scripts/*.sh`, `scripts/flow/*.py`
  and `.rs`.

So the blanket `docs/**` buys nothing that `*.md` does not already provide for prose, and it costs the
review of every program the repo ships beside a report. It is fleet-wide and recurring, not a one-off.

## What Changes

1. **The exclusion is narrowed to a prose/artifact deny-list (AC1).** `*.md` stays (it already does all
   prose exclusion, repo-wide). The blanket `docs/**` is replaced by **docs-scoped exclusions of the
   non-code artifact extensions only** — the raw run output and binary/image blobs that make a report's
   artifact directory expensive (`txt json jsonl log err csv png svg gz pdf jfr html mmd tex diff`) —
   so `.py` / `.sh` / `.bt` / `.c` / `.rs` / `.toml` / `.cql` / `.yml` / `.yaml` under `docs/` become
   reviewable. A deny-list is **forced**, not chosen: `exclude_patterns` has no negation/allow-list
   support (git pathspec has none inside `:(exclude)`), so "review these extensions" is not expressible.
2. **A new fail-closed pre-enqueue check, `census-exclusion:` (AC3).** The wrapper reads the effective
   `exclude_patterns`, converts each pattern to the git pathspec roborev would build, and asks **git**
   which of its own census's CODE paths survive. A non-empty swallowed set is a **FAIL before the review
   is enqueued**, naming the swallowed paths under its own distinct greppable key — never a generic
   `prompt-content:` failure discovered after a review has been paid for.
3. **The two classifiers are reconciled by construction (AC4).** The check does not re-implement a glob
   matcher and does not trust the reviewer's narration: it reproduces roborev's mechanism with git
   itself. The census's docs-scoped artifact classification and the config's docs-scoped deny-list are
   declared **once** in the wrapper, and the one residual disagreement direction that remains is
   declared and shown to be noise-only, never a swallow.
4. **A recorded live demonstration (AC2).** The sanctioned wrapper is run against a PR
   #3222-shaped diff (executables under `docs/reports/*-artifacts/`) and the census counts, the
   `prompt-content:` line and the input/cached/output token counts are **recorded** in the PR — showing
   the genuine-review band rather than the vacuous baseline. The probe also CONFIRMS end to end the
   root-anchoring the disassembly established — a deny-listed extension under `website/src/content/docs/`
   is still delivered to the reviewer, because a pattern containing an interior `/` is passed verbatim.
5. **Hermetic regression tests (AC5).** `scripts/tests/test_roborev_review_guard.sh` gains cases in its
   existing style: executables under `docs/` yield a PASS-eligible census and ARE enqueued; a prose-only
   diff under `docs/` still reports `code-free: FAIL` and is never enqueued; a config that WOULD swallow
   census code (e.g. a restored `docs/**`) yields `census-exclusion: FAIL` naming the swallowed paths,
   `RESULT: FAIL` and no enqueue; and the new key appears exactly once in the fixed key order.
6. **Doctrine in the same change (AC6).** CLAUDE.md's roborev rule 4 and the published
   `agents-developing/roborev-findings` page are amended so neither implies that *everything* under
   `docs/` is code-free; the `docs/reports/*-artifacts/` harness convention is named explicitly; the
   true mechanism (configured git pathspec exclusions, not a code/non-code judgement by the reviewer) is
   stated. Every drifting copy of the old claim across the wrapper's own comments, the `.claude/`
   agent/skill surfaces, `delivery-pipeline.md` and the `roborev-review-guard` spec is corrected in the
   same change. Publication is accepted by **grepping the served page for a distinctive new phrase**,
   never by HTTP 200.
7. **The backfill ruling is recorded (AC7).** Whether the already-merged, never-reviewed harness code
   from #3026 / #3100 / #3217 gets a retroactive review pass or is explicitly accepted as-is **with the
   reason** is recorded in the change. The ruling is the owner's; the requirement is that it is recorded
   either way.

## Non-goals

- **Not relocating measurement harnesses out of `docs/reports/*-artifacts/`.** The owner has ruled: the
  convention of shipping a harness beside the report it produced stays. No move is proposed here, and
  none is smuggled in as tidying.
- **Not broadening the reviewer's diet to prose.** `*.md` and the other prose extensions stay excluded.
  The point is to stop excluding programs, not to start reviewing documentation text.
- **Not touching the four vacuity/SHA traps (T1–T4) or the `--agent`/`--model` enforcement.** Those work
  and are not in question; this change adds a check *beside* them and reuses their summary-block and
  exit-code contracts unchanged.
- **Not re-doing #3222's substitute review.** It happened, found no blockers, and is recorded in the PR.
- **Not patching or forking `roborev`.** It is an external stripped binary. This change configures it
  correctly and verifies the configuration from our side.
- **Not fixing `scripts/ci/classify-docs-only.sh`.** Its `docs/*` classification has the *same* defect
  in the correctness gate (a PR touching only `docs/reports/*-artifacts/*.sh` is classified docs-only and
  short-circuits `pr-gate-core`). It is named in `design.md` and filed as a **follow-up**, deliberately
  out of scope here so a review-gate change and a correctness-gate change are not entangled in one PR.
- **No Rust code, no library surface, no on-disk format work.** Nothing touches `cqlite-core`, the
  bindings, the CLI, the no-heuristics decode path, or the <128MB memory budget.

## Impact

- **Config:** `.roborev.toml` — `exclude_patterns` replaced by the prose/artifact deny-list.
- **Wrapper:** `scripts/flow/roborev-review-oracles.sh` gains the exclusion-reconciliation oracle and the
  declared docs-artifact extension set; `scripts/flow/roborev-review.sh` gains only a call site plus its
  four registration points (state init, `emit_summary()` key order, the call, and the verdict-scan
  failing-capable key list — a key absent from that last list is decorative), plus `usage()` text.
- **Summary block:** one new key, `census-exclusion:`, inserted between `code-free:` and `job-record:` in
  the FIXED key order. Consumers that assert the order (the hermetic check, doctrine renderings) update
  in the same change.
- **Tests:** `scripts/tests/test_roborev_review_guard.sh` (hermetic; stub `roborev` first on `PATH`, no
  network / cargo / real reviewer) gains the new cases and a fixture mode able to supply its own
  `.roborev.toml`. It runs in the `roborev-lints` gate component, which is in **both** the full and
  `--lite` component sets, so a regression FAILs the fast loop.
- **Doctrine surfaces:** `CLAUDE.md` (roborev rule 4 + the T3 sentence + the mechanized-lints table
  region), `website/src/content/docs/agents-developing/roborev-findings.md`,
  `website/src/content/docs/agents-developing/delivery-pipeline.md`, `.claude/agents/flow-lead.md`,
  `.claude/agents/flow-closer.md`, `.claude/skills/flow-implement/SKILL.md`, the three
  `scripts/flow/roborev-review*.sh` header comments, and
  `openspec/specs/roborev-review-guard/spec.md` (via this change's delta).
- **No-heuristics mandate (#28):** unaffected — that mandate governs on-disk TYPE/format inference in
  the SSTable read path. The new check is deterministic: it asks git to apply the configured pathspecs
  and compares path sets.
- **Public binding surfaces (Python/Node/CLI), memory budget:** untouched.
- **Follow-up filed, not fixed here:** `scripts/ci/classify-docs-only.sh` `is_docs_file()`'s blanket
  `docs/*` case (same defect class, correctness gate rather than reviewer).
