# Tasks: narrow-roborev-docs-exclusion (issue #3229)

> Design decided in `design.md`: narrow `exclude_patterns` from a blanket `docs/**` to a **prose/artifact
> deny-list** (an allow-list is not expressible), and give the wrapper a **pre-enqueue
> `census-exclusion:` check that reproduces roborev's exclusion mechanism with git** rather than asserting
> it in a comment. AC→requirement map is at the top of `specs/roborev-review-guard/spec.md`.
> AC7 backfill ruling: **RULED by the owner (2026-08-03) — accept as-is, no retroactive pass**;
> the ruling and its four-part reasoning are recorded in `design.md` D7 and
> `docs/reports/3229-artifacts/README.md`.

## 1. Pin the resolved algorithm as the contract (surface: `design.md`, the ported code's comment)
- [x] Record `git.FormatExcludeArgs` verbatim (the 8-line form in `design.md`), its recovery method
      (`.gopclntab` symbol parsing of the stripped binary, text base `0x401000`) and its caller list
      (`git.GetDiffCtx`, `GetDiffLimitedCtx`, `GetRangeDiffCtx`, `GetRangeDiffLimitedCtx`, `GetDirtyDiff`,
      `prompt.(*Builder).buildSinglePrompt` / `buildRangePrompt` / `resolveExcludes`) beside the port, so a
      future reader can re-derive it.
- [x] Pin the derivation to **`roborev v0.61.2`** in both `design.md` and the ported code's header comment,
      and state the maintenance obligation: a roborev upgrade requires re-verifying the algorithm before the
      check is trusted (an upstream change would silently invalidate it while blocks still read `PASS`).
- [x] Do NOT conflate the adjacent mechanisms: `max_prompt_size`, `exclude_branches`,
      `IsCommitMessageExcluded`, and `git.EnsureLocalExcludePattern` (`.git/info/exclude`) are separate.

## 2. Narrow the exclusion configuration (surface: `.roborev.toml`)
- [x] Replace `exclude_patterns = ['docs/**', '*.md']` with `*.md` (unchanged — it already excludes every
      tracked `.md` repo-wide) plus docs-scoped artifact patterns for at minimum
      `txt json jsonl log err csv png svg gz pdf jfr html mmd tex diff`.
- [x] Keep the value a SINGLE-LINE array so the wrapper's parser stays minimal, write every docs-scoped
      pattern WITH an interior `/` and WITHOUT a trailing slash (root-anchored per R1; a trailing slash would
      invert to recursive per R3), and verify by `git ls-files -- <the pathspec forms>` that no
      `.py`/`.sh`/`.bt`/`.c`/`.rs`/`.toml`/`.cql`/`.yml`/`.yaml` path under `docs/` is matched, that no path
      OUTSIDE `docs/` newly becomes excluded, and that `website/src/content/docs/**` is untouched.
- [x] Note in the PR that `.roborev.toml` is machine-managed (`roborev config set` rewrites it), so the
      list must be re-checked after any `roborev config` write — the `census-exclusion:` check is the
      standing detector.

## 3. The pre-enqueue reconciliation oracle (surface: `scripts/flow/roborev-review-oracles.sh`)
- [x] Declare the docs-scoped ARTIFACT extension set beside the existing `CODE_FREE_EXTENSIONS` /
      `CODE_FREE_EXTENSIONLESS_PREFIXES` constants, and use it in the per-file census classification so
      the census and the configuration agree on artifacts (without it, every legitimate report PR would
      trip the new check on its `.json`/`.log`/`.err` artifacts).
- [x] Add the effective-exclusion-set reader: parse `.roborev.toml` and `~/.roborev/config.toml`
      directly (NOT via the binary — no roborev flag prints the resolved pathspecs, and file parsing keeps
      the check hermetic with no `command -v roborev` reorder), respecting TOML table scoping; UNION the two
      lists, matching `config.ResolveExcludePatterns` / `loadRepoExcludePatterns`.
- [x] Fail closed on an unreadable set (`FAIL (exclusion set unreadable: <cause>)`) and pass explicitly on
      a genuinely absent one (`PASS (no exclusion patterns configured)`) — never alias the two.
- [x] Add `roborev_check_census_exclusion()` with pathspec construction as an EXACT PORT of
      `git.FormatExcludeArgs`: TrimSpace → TrimRight `/` → skip if empty → capture `b0` BEFORE TrimLeft →
      TrimLeft `/` → skip if empty → root-anchored `:(exclude,glob)<p>` when `b0=='/'` OR `<p>` contains
      `/`, else recursive `:(exclude,glob)**/<p>` → emit BOTH `<p>` and `<p>/**`. Then get survivors from
      `git diff --name-only -z --no-renames <base>...HEAD -- <pathspecs>` and compute
      swallowed = census CODE paths − survivors. Do NOT evaluate two interpretations of a slash-containing
      pattern — that would false-FAIL on nested-`docs/` census paths roborev actually delivers.
- [x] FAIL on a TRAILING-SLASH pattern with the inversion named (`docs/` resolves recursive `**/docs` +
      `**/docs/**`, the opposite of `docs/**`), independent of whether it currently swallows anything.
- [x] Emit the pinned value grammar, naming each swallowed path and the pattern that ate it, capped at 10
      with `(+<r> more)`.
- [x] Be NUL-safe end to end (`-z`, bash arrays, no word splitting) — the repo tracks a `docs/` path with
      spaces and a literal double quote that a non-`-z` read would emit quoted and never match.
- [x] Corroborate against `roborev config get exclude_patterns` when the binary is invocable: a pattern it
      reports that the parse LACKS ⇒ `FAIL (exclusion set drift: …)`; the reverse ⇒ NOTICE; binary absent ⇒
      `UNAVAILABLE`, never a failure.

## 4. Wire it into the wrapper — all four registration points (surface: `scripts/flow/roborev-review.sh`)
- [x] Initialise the `census-exclusion` state to an explicit `SKIP` (never blank).
- [x] Insert `census-exclusion:` into `emit_summary()`'s FIXED key order, immediately after `code-free:`,
      exactly once.
- [x] Call the check after `roborev_census` / `code-free:` and BEFORE the checks-file validation and the
      enqueue, so a swallowing configuration costs no review.
- [x] Add the key to the **verdict-scan failing-capable key list** — omitting it ships a check whose FAIL
      is decorative.
- [x] Document the key, its value grammar and its position in both `usage()` regions.

## 5. Hermetic regression cases (surface: `scripts/tests/test_roborev_review_guard.sh`)
- [x] Extend `make_fixture` so a fixture can write the work repo's OWN `.roborev.toml` (without this, a
      configuration regression is not expressible — which is why this defect could ship), plus a
      `docs-executables` fixture mode (`.py`/`.sh`/`.bt` under `docs/reports/x-artifacts/`).
- [x] Case: executables under `docs/` + narrowed config ⇒ `code-free: PASS`, `census-exclusion: PASS`, IS
      enqueued.
- [x] Case: prose-only under `docs/` ⇒ `code-free: FAIL`, `assert_never_enqueued` (the guard is not
      inverted).
- [x] Case: docs artifacts only (`.txt`/`.json`/`.log`/`.err`) ⇒ still `code-free: FAIL`, never enqueued.
- [x] Case: config restored to `['docs/**','*.md']` ⇒ `census-exclusion: FAIL` NAMING the swallowed paths,
      `RESULT: FAIL`, `assert_never_enqueued`.
- [x] Case: unparseable `exclude_patterns` value ⇒ `FAIL (exclusion set unreadable: …)`; absent key/file ⇒
      `PASS (no exclusion patterns configured)`.
- [x] Case: a census path with spaces + a literal double quote compares correctly (NUL-safety).
- [x] Case: corroboration `UNAVAILABLE` with a stub that does not answer `config get`; drift FAIL with one
      that reports an unparsed pattern.
- [x] Case: key order — `census-exclusion:` exactly once, immediately after `code-free:`
      (`assert_one_block` + order assert); and an unreached run carries `SKIP`.
- [x] Cases pinning the ported construction: `docs/**/*.json` leaves a `website/src/content/docs/c.json`
      census path SURVIVING (no false FAIL); a bare directory name excludes its whole subtree via the
      `<p>/**` sibling; `/README.md` excludes only the root file while `README.md` excludes at any depth; a
      TRAILING-slash `docs/` FAILs naming the inversion; a whitespace-only pattern is skipped.
- [x] Case: replication fidelity against the pre-change set `['docs/**','*.md']` — root-anchored `docs/**`
      plus recursive `**/*.md`, reproducing the 21-review replay (only `.md`, any depth, repo-wide).
- [x] Keep every new case hermetic (stub `roborev` first on `PATH`, `STUB_*`-driven, no network/cargo/real
      reviewer) and keep the hermeticity meta-assert green. If the file crosses the ~1500-line test target,
      split by responsibility or re-run with `CQLITE_ALLOW_FILE_GROWTH=1` and a note linking #1135.
- [x] Confirm the suite still runs in the `roborev-lints` component (present in BOTH the full and `--lite`
      component sets), so a regression FAILs the fast loop.

## 6. Doctrine, in this same change
- [x] `CLAUDE.md` roborev rule 4 (+ its T3 sentence and the docs-only/CITE-AND-WAIVE region): retire the
      falsified "roborev EXCLUDES non-code paths" claim, state the configured-pathspec mechanism, name the
      `docs/reports/*-artifacts/` harness convention as reviewed code, define docs-only as a code-free
      CENSUS (never a path prefix), and add `census-exclusion:` to the documented key order.
- [x] `website/src/content/docs/agents-developing/roborev-findings.md`: same corrections, plus the
      summary-block key list and the mechanized-in-`--lite` table row.
- [x] Propagate to every drifting copy: `website/.../agents-developing/delivery-pipeline.md`,
      `.claude/agents/flow-lead.md`, `.claude/agents/flow-closer.md`,
      `.claude/skills/flow-implement/SKILL.md`, and the header comments of all three
      `scripts/flow/roborev-review*.sh` files — including `roborev_check_prompt_content()`'s comment, which
      states the falsified claim outright.
- [x] Grep the whole tree for the falsified wording afterwards and confirm zero remaining copies.
- [ ] After merge, verify publication by grepping the SERVED page for a distinctive new phrase (never an
      HTTP 200); re-check after ~3 minutes if absent (CDN staleness).

## 7. AC2 demonstration and the AC7 ruling
- [ ] Run the sanctioned wrapper (`--agent codex --model gpt-5.6-sol`, explicit absolute `--repo`) against
      a PR #3222-shaped diff and RECORD in the PR: `census:` counts, `code-free:`, `census-exclusion:`,
      `prompt-content:`, and input/cached/output tokens — expecting the genuine-review band
      (398k–649k in / 5.0k–6.3k out) and NOT the vacuous baseline (~18.7k in / 0 cached / 53–56 out;
      #3222 itself measured 15,443 in / 89 out).
- [ ] Include in that same probe diff a deny-listed artifact extension under `website/src/content/docs/` and
      confirm it IS present in the prompt — the end-to-end confirmation of root anchoring. Its ABSENCE
      falsifies the port and blocks the change; it is not an acceptable outcome to merely record.
- [x] Ask the owner for the AC7 backfill ruling on #3026 / #3100 / #3217 and RECORD it with its reason
      (retroactive review pass — naming mechanism, paths and outcome; or acceptance-as-is with the reason).
      Park rather than block if the session is unattended: one structured question comment,
      `needs-decision`, `blocked` marker, exit.

## 8. Follow-up to file (not fixed here)
- [x] File an issue for `scripts/ci/classify-docs-only.sh` `is_docs_file()`'s blanket `case "$path" in
      docs/*)` — the same "path glob swallows executables under `docs/`" defect in the CORRECTNESS gate: a
      PR touching only `docs/reports/*-artifacts/*.sh` is classified docs-only and short-circuits
      `pr-gate-core` to green. Test surface `scripts/tests/test_classify_docs_only.sh`. Link it from this
      change's PR.
- [x] Note the upstream ask (allow-list/negation for `exclude_patterns`; non-zero exit when everything is
      excluded) in the PR as a non-blocking follow-up. (Recorded in `design.md` "Follow-ups"; restate in
      the PR body.)

## 9. Certification
- [ ] `--lite` green each fix round (summary-file redirect) — DONE, see the PR — then `rust-reviewer` + `roborev` on the
      lite-green diff (the diff contains code — shell + config — so it IS roborev-certifiable and MUST be).
- [ ] Open the PR; hand the endgame to `flow-closer`: the ONE full `scripts/agent-gate.sh` run of record
      (`AGENT-GATE SUMMARY`, `RESULT: PASS`, `tree-integrity:` verified) → `spec-auditor` C intent audit
      against these specs → final roborev pass → `gh pr merge --auto --squash --delete-branch` after
      `scripts/flow/premerge-assert.sh`.
- [ ] After merge: verify the published doctrine page by served content, then `flow-finalize` (archive this
      change, stamp delivery telemetry via a telemetry worktree PR).
