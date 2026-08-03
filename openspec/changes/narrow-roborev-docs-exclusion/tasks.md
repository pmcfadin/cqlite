# Tasks: narrow-roborev-docs-exclusion (issue #3229)

> Design decided in `design.md`: narrow `exclude_patterns` from a blanket `docs/**` to a **prose/artifact
> deny-list** (an allow-list is not expressible), and give the wrapper a **pre-enqueue
> `census-exclusion:` check that reproduces roborev's exclusion mechanism with git** rather than asserting
> it in a comment. AC→requirement map is at the top of `specs/roborev-review-guard/spec.md`.
> Owner-facing decision outstanding: the AC7 backfill ruling (task 7).

## 1. Resolve the one unknown before finalising the pattern list (surface: recorded probe evidence)
- [ ] Run the sanctioned wrapper once against a diff containing a deny-listed artifact extension under a
      NESTED `docs` directory (`website/src/content/docs/...`) plus one under top-level `docs/`, and read
      the prompt actually sent to determine whether a pattern CONTAINING a `/` is passed verbatim
      (`:(exclude,glob)docs/**`) or ALSO `**/`-prefixed. Record the answer in the PR.
- [ ] If `**/`-prefixed: record the nested-`docs/` exclusion (notably `website/src/content/docs/**`) as a
      named known residual in `design.md` and the PR. Do NOT silently accept it.

## 2. Narrow the exclusion configuration (surface: `.roborev.toml`)
- [ ] Replace `exclude_patterns = ['docs/**', '*.md']` with `*.md` (unchanged — it already excludes every
      tracked `.md` repo-wide) plus docs-scoped artifact patterns for at minimum
      `txt json jsonl log err csv png svg gz pdf jfr html mmd tex diff`.
- [ ] Keep the value a SINGLE-LINE array so the wrapper's parser stays minimal, and verify by
      `git ls-files -- <the pathspec forms>` that no `.py`/`.sh`/`.bt`/`.c`/`.rs`/`.toml`/`.cql`/`.yml`/
      `.yaml` path under `docs/` is matched, and that no path OUTSIDE `docs/` newly becomes excluded.
- [ ] Note in the PR that `.roborev.toml` is machine-managed (`roborev config set` rewrites it), so the
      list must be re-checked after any `roborev config` write — the `census-exclusion:` check is the
      standing detector.

## 3. The pre-enqueue reconciliation oracle (surface: `scripts/flow/roborev-review-oracles.sh`)
- [ ] Declare the docs-scoped ARTIFACT extension set beside the existing `CODE_FREE_EXTENSIONS` /
      `CODE_FREE_EXTENSIONLESS_PREFIXES` constants, and use it in the per-file census classification so
      the census and the configuration agree on artifacts (without it, every legitimate report PR would
      trip the new check on its `.json`/`.log`/`.err` artifacts).
- [ ] Add the effective-exclusion-set reader: parse `.roborev.toml` and `~/.roborev/config.toml`
      directly (NOT via the binary, so the check stays hermetic and no `command -v roborev` reorder is
      needed), respecting TOML table scoping; UNION the two lists (fail-closed direction).
- [ ] Fail closed on an unreadable set (`FAIL (exclusion set unreadable: <cause>)`) and pass explicitly on
      a genuinely absent one (`PASS (no exclusion patterns configured)`) — never alias the two.
- [ ] Add `roborev_check_census_exclusion()`: build `:(exclude,glob)` pathspecs (slash-less ⇒ `**/`-prefixed;
      slash-containing ⇒ BOTH interpretations, union of exclusions), get survivors from
      `git diff --name-only -z --no-renames <base>...HEAD -- <pathspecs>`, and compute
      swallowed = census CODE paths − survivors.
- [ ] Emit the pinned value grammar, naming each swallowed path and the pattern that ate it, capped at 10
      with `(+<r> more)`.
- [ ] Be NUL-safe end to end (`-z`, bash arrays, no word splitting) — the repo tracks a `docs/` path with
      spaces and a literal double quote that a non-`-z` read would emit quoted and never match.
- [ ] Corroborate against `roborev config get exclude_patterns` when the binary is invocable: a pattern it
      reports that the parse LACKS ⇒ `FAIL (exclusion set drift: …)`; the reverse ⇒ NOTICE; binary absent ⇒
      `UNAVAILABLE`, never a failure.

## 4. Wire it into the wrapper — all four registration points (surface: `scripts/flow/roborev-review.sh`)
- [ ] Initialise the `census-exclusion` state to an explicit `SKIP` (never blank).
- [ ] Insert `census-exclusion:` into `emit_summary()`'s FIXED key order, immediately after `code-free:`,
      exactly once.
- [ ] Call the check after `roborev_census` / `code-free:` and BEFORE the checks-file validation and the
      enqueue, so a swallowing configuration costs no review.
- [ ] Add the key to the **verdict-scan failing-capable key list** — omitting it ships a check whose FAIL
      is decorative.
- [ ] Document the key, its value grammar and its position in both `usage()` regions.

## 5. Hermetic regression cases (surface: `scripts/tests/test_roborev_review_guard.sh`)
- [ ] Extend `make_fixture` so a fixture can write the work repo's OWN `.roborev.toml` (without this, a
      configuration regression is not expressible — which is why this defect could ship), plus a
      `docs-executables` fixture mode (`.py`/`.sh`/`.bt` under `docs/reports/x-artifacts/`).
- [ ] Case: executables under `docs/` + narrowed config ⇒ `code-free: PASS`, `census-exclusion: PASS`, IS
      enqueued.
- [ ] Case: prose-only under `docs/` ⇒ `code-free: FAIL`, `assert_never_enqueued` (the guard is not
      inverted).
- [ ] Case: docs artifacts only (`.txt`/`.json`/`.log`/`.err`) ⇒ still `code-free: FAIL`, never enqueued.
- [ ] Case: config restored to `['docs/**','*.md']` ⇒ `census-exclusion: FAIL` NAMING the swallowed paths,
      `RESULT: FAIL`, `assert_never_enqueued`.
- [ ] Case: unparseable `exclude_patterns` value ⇒ `FAIL (exclusion set unreadable: …)`; absent key/file ⇒
      `PASS (no exclusion patterns configured)`.
- [ ] Case: a census path with spaces + a literal double quote compares correctly (NUL-safety).
- [ ] Case: corroboration `UNAVAILABLE` with a stub that does not answer `config get`; drift FAIL with one
      that reports an unparsed pattern.
- [ ] Case: key order — `census-exclusion:` exactly once, immediately after `code-free:`
      (`assert_one_block` + order assert); and an unreached run carries `SKIP`.
- [ ] Keep every new case hermetic (stub `roborev` first on `PATH`, `STUB_*`-driven, no network/cargo/real
      reviewer) and keep the hermeticity meta-assert green. If the file crosses the ~1500-line test target,
      split by responsibility or re-run with `CQLITE_ALLOW_FILE_GROWTH=1` and a note linking #1135.
- [ ] Confirm the suite still runs in the `roborev-lints` component (present in BOTH the full and `--lite`
      component sets), so a regression FAILs the fast loop.

## 6. Doctrine, in this same change
- [ ] `CLAUDE.md` roborev rule 4 (+ its T3 sentence and the docs-only/CITE-AND-WAIVE region): retire the
      falsified "roborev EXCLUDES non-code paths" claim, state the configured-pathspec mechanism, name the
      `docs/reports/*-artifacts/` harness convention as reviewed code, define docs-only as a code-free
      CENSUS (never a path prefix), and add `census-exclusion:` to the documented key order.
- [ ] `website/src/content/docs/agents-developing/roborev-findings.md`: same corrections, plus the
      summary-block key list and the mechanized-in-`--lite` table row.
- [ ] Propagate to every drifting copy: `website/.../agents-developing/delivery-pipeline.md`,
      `.claude/agents/flow-lead.md`, `.claude/agents/flow-closer.md`,
      `.claude/skills/flow-implement/SKILL.md`, and the header comments of all three
      `scripts/flow/roborev-review*.sh` files — including `roborev_check_prompt_content()`'s comment, which
      states the falsified claim outright.
- [ ] Grep the whole tree for the falsified wording afterwards and confirm zero remaining copies.
- [ ] After merge, verify publication by grepping the SERVED page for a distinctive new phrase (never an
      HTTP 200); re-check after ~3 minutes if absent (CDN staleness).

## 7. AC2 demonstration and the AC7 ruling
- [ ] Run the sanctioned wrapper (`--agent codex --model gpt-5.6-sol`, explicit absolute `--repo`) against
      a PR #3222-shaped diff and RECORD in the PR: `census:` counts, `code-free:`, `census-exclusion:`,
      `prompt-content:`, and input/cached/output tokens — expecting the genuine-review band
      (398k–649k in / 5.0k–6.3k out) and NOT the vacuous baseline (~18.7k in / 0 cached / 53–56 out;
      #3222 itself measured 15,443 in / 89 out).
- [ ] Ask the owner for the AC7 backfill ruling on #3026 / #3100 / #3217 and RECORD it with its reason
      (retroactive review pass — naming mechanism, paths and outcome; or acceptance-as-is with the reason).
      Park rather than block if the session is unattended: one structured question comment,
      `needs-decision`, `blocked` marker, exit.

## 8. Follow-up to file (not fixed here)
- [ ] File an issue for `scripts/ci/classify-docs-only.sh` `is_docs_file()`'s blanket `case "$path" in
      docs/*)` — the same "path glob swallows executables under `docs/`" defect in the CORRECTNESS gate: a
      PR touching only `docs/reports/*-artifacts/*.sh` is classified docs-only and short-circuits
      `pr-gate-core` to green. Test surface `scripts/tests/test_classify_docs_only.sh`. Link it from this
      change's PR.
- [ ] Note the upstream ask (allow-list/negation for `exclude_patterns`; non-zero exit when everything is
      excluded) in the PR as a non-blocking follow-up.

## 9. Certification
- [ ] `--lite` green each fix round (summary-file redirect), then `rust-reviewer` + `roborev` on the
      lite-green diff (the diff contains code — shell + config — so it IS roborev-certifiable and MUST be).
- [ ] Open the PR; hand the endgame to `flow-closer`: the ONE full `scripts/agent-gate.sh` run of record
      (`AGENT-GATE SUMMARY`, `RESULT: PASS`, `tree-integrity:` verified) → `spec-auditor` C intent audit
      against these specs → final roborev pass → `gh pr merge --auto --squash --delete-branch` after
      `scripts/flow/premerge-assert.sh`.
- [ ] After merge: verify the published doctrine page by served content, then `flow-finalize` (archive this
      change, stamp delivery telemetry via a telemetry worktree PR).
