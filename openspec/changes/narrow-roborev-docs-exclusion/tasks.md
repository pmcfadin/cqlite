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
      tracked `.md` repo-wide) plus artifact patterns for at minimum
      `txt json jsonl log err csv png svg gz pdf jfr html mmd tex diff`, each scoped to an ARTIFACT-BEARING
      DIRECTORY as `<artifact-dir-glob>/**/*.<ext>` — **not** swept across all of `docs/`, which hid
      functional config (round 6, §8f / design D1a).
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

## 7. AC2 demonstration (POST-MERGE) and the AC7 ruling
- [x] RECORD why the demonstration cannot be pre-merge, rather than weakening the criterion: roborev
      resolves `exclude_patterns` from the repo ROOT path and SNAPSHOTS it at daemon start, so the narrowed
      set does not apply to this change's own review. A committed executable under root `docs/` therefore
      makes `census-exclusion:` FAIL *correctly* until merge — a DEADLOCK, not a test. `git rm`'d
      `probe-census-exclusion.sh`; procedure kept as prose in
      `docs/reports/3229-artifacts/live-probe-procedure.md`.
- [x] KEEP `website/src/content/docs/_3229-root-anchoring-probe.json` on the branch — a `.json` under a
      NESTED `docs` directory survives under BOTH the old and the new configuration (root anchoring), so it
      does not deadlock and is live evidence either way. Its ABSENCE from a prompt would falsify the port
      and block; it is not an acceptable outcome to merely record.
- [x] Correct the token guidance everywhere it appears: judge against the MECHANISM's thresholds
      (`ROBOREV_VACUITY_MIN_INPUT_TOKENS = 25000`, anchored on the highest observed vacuous run of 18,801;
      `cached > 0`; **output ADVISORY ONLY**, because a genuine CLEAN review emits 20–60 output tokens —
      indistinguishable from the vacuous baseline's 53–56, per
      `scripts/flow/roborev-review-checks.sh:328`). Cite 398k–649k ONLY as "observed on large diffs", never
      as a threshold: a real substantive round measured `input=118514 cached=88320 output=5954` on a ~90k
      prompt with two findings citing real code, far below that band, so an absolute floor drawn from
      large-diff observations would falsely flag legitimate small diffs.
- [ ] **POST-MERGE, PRIMARY EVIDENCE:** take the first post-merge PR that carries an executable under
      `docs/` (#3234 ships harnesses now; #3096's successor will; #3249's artifacts may) and post its
      `census:` + `census-exclusion: PASS` + `prompt-content: PASS (<n>/<n>)` lines to #3229. Better
      evidence than a probe written to pass, because the diff was not shaped for it. FALLBACK: the
      documented procedure.
- [ ] **NAMED TRIGGER — an unowned post-merge obligation is not an obligation** (#3232 existed only as
      prose in #3100's close; #3103 shipped uncommitted and three issues then rebuilt a corpus):
      on merge #3229 goes to **`In Review`, NOT `Done`** (`Done` auto-closes it); the PR is finalized and
      telemetry stamped regardless; #3229 flips to `Done` ONLY once the AC2 evidence is posted; if the
      demonstration has not happened within a few days, FILE IT as a tracked issue — never leave it in a
      comment thread.
- [ ] Before running it post-merge: update the ROOT checkout AND **restart the roborev daemon** (it
      snapshots config at start; the one observed had 4d15h uptime).
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

## 8b. Blocker round from the first sanctioned roborev pass (two false-PASS classes)
The first sanctioned round returned `RESULT: FAIL` with `prompt-content: FAIL (1/7 code census paths
absent)` while `census-exclusion:` said `PASS (7/7 survive)` — i.e. the new guard itself reported green
about a swallow it exists to catch. Two independent root causes, both false PASSes:
- [x] **A — the wrong config file.** The oracle read `$REPO/.roborev.toml` (the WORKTREE) while roborev's
      daemon binds the repo by `repos.root_path` (the ROOT checkout) and reads THAT file. Fix: resolve the
      root checkout from git (`--path-format=absolute --git-common-dir`, relative fallback for git < 2.31,
      `worktree list --porcelain` last resort, FAIL CLOSED if none answer), evaluate ALL config sources as
      a UNION and fail on a swallow in ANY, corroborate with `roborev config get` run from EVERY checkout
      read, and NAME the source file in every FAIL/PASS value (incl. the trailing-slash FAIL).
- [x] **B — corroboration skipped exactly where it is the only oracle.** `_rx_found=0` returned
      `PASS (no exclusion patterns configured)` BEFORE corroboration ran, aliasing "our parser recognised
      no key" to "nothing is configured". Verified live on v0.61.2: a QUOTED key `"exclude_patterns"` IS
      honoured while the bare-key match skipped it. Fix: corroborate unconditionally and before every early
      return (parsed-none-while-binary-reports-some ⇒ DRIFT → FAIL), accept the quoted key spellings, and a
      binary answering with an EMPTY list corroborates rather than degrading to `UNAVAILABLE`.
- [x] Model roborev's BUILT-IN excludes (the hard-coded lockfile/cache deny-list extracted from the pinned
      v0.61.2 binary) in the same reconciliation, messaged DISTINCTLY from a configured pattern.
- [x] Refuse an unknown/untranslated TOML basic-string escape fail-closed instead of swallowing the
      backslash; return un-quoted paths through a named global so `$(…)` cannot strip a trailing newline
      byte; declare `line` local in `roborev_check_census_exclusion`.
- [x] Redirect `HOME` in the two hand-rolled wrapper invocations (t7, t9) that bypassed `run_wrapper`'s
      fixture home — both now reach `census-exclusion`, which reads the GLOBAL config, so a host global with
      a pattern would have reded them on the wrong key.
- [x] New hermetic cases: `cx5d` (empty parse + binary reports ⇒ DRIFT FAIL, never enqueued), `cx5e`/`cx5f`
      (quoted / single-quoted key spellings parsed), `cx5g` (unknown TOML escape refused), `cx18` (linked
      worktree: blanket ROOT config caught, attributed `[root-config]`), `cx18b` (its PASS complement),
      `cx19` (`Cargo.lock` swallowed by a built-in, messaged apart), `cx19b` (structural: the built-in
      constant + its re-extract obligation). `cx5b`/`cx5c` no longer bless an un-corroborated PASS.
- [x] Specify all of it — the multi-source rule, the built-in excludes, the corroborate-on-empty-parse
      rule — in the delta spec and `design.md` (§D2a/D2b/D2c), not only in code.
- [x] EXPECTED CONSEQUENCE, recorded not suppressed: with A fixed, `census-exclusion:` now correctly FAILs
      on this very branch, because the ROOT checkout's `.roborev.toml` still carries the blanket `docs/**`
      and the diff contains `docs/reports/3229-artifacts/probe-census-exclusion.sh`. Observed verbatim:
      `census-exclusion: FAIL (1/7 code census paths excluded:
      docs/reports/3229-artifacts/probe-census-exclusion.sh by 'docs/**' [root-config])`. That FAIL is the
      guard working. The narrowed config only takes effect on the root checkout once this PR MERGES, so the
      pre-merge roborev round on this branch is unavoidably scoped by the old set — a scheduling
      consequence for the owner, never a reason to weaken the check, special-case the probe, or hand-edit
      the root checkout's config.

## 8c. Owner rulings applied after the first sanctioned roborev round
- [x] **Built-in verdict SPLIT** — neither bare FAIL nor bare NOTICE. One rule, stated verbatim in
      CLAUDE.md, `roborev-findings.md`, `design.md` and `--help`: **FAIL where the author can act; NOTICE
      where only the information is actionable; never silence.** A pinned built-in swallowing census code is
      a NOTICE (no remedy exists — compiled in, no opt-out, no negation form; and a guard that fires on a
      routine `Cargo.lock` touch with no available fix is the guard that gets DISABLED, which is how #3229
      happened). The live built-in set DIVERGING from the pinned 24 is a FAIL (that HAS a remedy —
      re-extract, re-pin, judge the new built-in — and it is a MECHANISM change the version pin exists to
      catch; a NOTICE would silently absorb an upgrade that began excluding `*.rs` or `scripts/**`).
- [x] Observe the live set from the binary by two RELIABLE signals, not a blind re-extraction (Go literals
      are concatenated with no terminators — a naive scan of this binary yields truncations, junk-suffixed
      hits and a phantom `**/git` that is really the bare prefix constant): fixed-string presence per pinned
      pattern names REMOVALS exactly, and a pinned COUNT of `:(exclude,glob)` literals (26 = 24 patterns + 2
      prefix constants) detects ADDITIONS numerically. Residual declared.
- [x] "Never silence" mechanized: every value ends `built-in-set: OK|DIVERGED|UNAVAILABLE`; an unobservable
      set (no roborev, unreadable, or a stub with zero literals — the hermetic suite's state) is
      UNAVAILABLE and is explicitly NEITHER a failure NOR a blessing.
- [x] Precedence: both FAIL causes outrank the NOTICE and EVERY cause present is named.
- [x] CONFIRMED by reading the wrapper's verdict scan directly (and asserted structurally against it):
      failing-capable set is exactly `FAIL*|FINDINGS*|ERROR*|INCONSISTENT*`; `NOTICE*` is absent from it;
      `$CENSUS_EXCLUSION` still participates, so a configured swallow still reds `RESULT:`.
- [x] Follow-through so the unfixable red is not merely moved one key down: `prompt-content:` subtracts the
      built-in-excluded set and says so in its value. Scoped to BUILT-IN swallows only — a configured
      swallow FAILs pre-enqueue, so it can never be masked.
- [x] Tests: `cx19` keeps the NOTICE (+ review enqueued, `RESULT:` not FAIL, and the UNAVAILABLE
      "neither failure nor blessing" assertion); `cx19d` (pin MATCHED ⇒ `built-in-set: OK`, corroborated);
      `cx19e` (an ADDED `**/*.rs` ⇒ FAIL, diff-independent, never enqueued); `cx19f` (a REMOVED pinned
      pattern ⇒ FAIL naming it); `cx19g` (configured swallow AND divergence ⇒ both named); `cx19b`'s
      structural assertions kept.
- [x] **Doctrine — three properties, one generalization**, recorded in CLAUDE.md and
      `roborev-findings.md` beside the existing BASE-ref note: (1) roborev's daemon reads
      `exclude_patterns` from the repo ROOT PATH, so a worktree edit is INVISIBLE to it; (2) the daemon
      SNAPSHOTS config at start, so an edit needs a RESTART; (3) generalized — **any PR whose subject is a
      config the daemon (or a gate) reads from root cannot certify itself**. Both (1) and (2) have cost real
      rounds, and the write-up says so.
- [x] RECORD, not smooth over: **`prompt-content:` — the PRE-EXISTING guard — caught the NEW guard**
      certifying a config roborev never used. Kept as the strongest argument in the change for keeping both
      layers, explicitly because it paid out in the direction nobody plans for (the NEW layer was the wrong
      one). In `design.md` (D2a-ter) and the doctrine rationale.
- [x] RECORD the cross-worker arbitration in `proposal.md` + `design.md` (D2a-bis): #3234 measured
      `exclude_patterns` as having "no observable effect"; the owner ranked H2 (config resolves from the
      primary checkout). Our disassembly + 21-review replay proved the MECHANISM half, #3234 independently
      supplied the ORDERING half (its single daemon restart preceded every config edit and never followed
      one). Both operative. **Conclusion: `exclude_patterns` WORKS — #3234's null result was a
      worktree-config artifact, so AC1 is a genuine fix and AC3 guards a mechanism that really applies.**
      This was the live existential risk to the change and it is now closed.
- [x] RECORD that a test which BLESSES a vacuous verdict (`cx5b`/`cx5c` locking in the un-corroborated
      PASS) is WORSE than an unguarded path: it consumes the review budget that would otherwise have found
      the bug, and converts "nobody checked" into "we checked and it was fine".

## 8b. ONE canonical path-normalisation boundary (round 4 — the pattern behind SIX blockers)

- [x] **Normalise ONCE, at the census.** `roborev_census` reads `git diff --numstat -z --no-renames` and
      parses NUL-terminated records (`read -r -d ''`), so paths arrive RAW and a newline-bearing path
      survives. RAW is the SINGLE internal representation for classification, comparison and display.
- [x] **Every consumer audited, not just the two reported**: the census classification loop (was reading
      the QUOTED extension — `md"`/`json"` — so PROSE counted as CODE ⇒ false pre-enqueue
      `census-exclusion: FAIL` under `*.md`); `census-exclusion:`'s survivor comparison (dropped its own
      unquote — both sides are `-z` now); `CENSUS_BUILTIN_EXCLUDED` + the `prompt-content:` subtraction;
      `prompt-content:` membership; the wrapper's `--help`/key documentation.
- [x] **ONE matcher for prompt headers**: `roborev_diff_header_has_path` (in the oracles file, beside the
      boundary) is the only way to ask whether a header names a path, and `roborev_unquote_path` has exactly
      one caller — it. It reads every shape git emits, including the MIXED-quoted rename header
      (`diff --git a/<ascii> "b/<quoted>"`), which occurs only on renames and was structurally unreachable.
- [x] **The retired mechanisms are gone**: the `[^ ]+` header regex, the `.promptpaths` path-set file, and
      `grep -Fxq` membership over newline-delimited paths (which reported a genuine FALSE PASS —
      `PASS (2/2 present)` for census `{a, a<LF>b.rs}` against a prompt naming only `a`).
- [x] Tests: `cx6e` (non-ASCII PROSE ⇒ non-code, no false swallow), `cx6f` (non-ASCII docs ARTIFACT),
      `cx6g` (rename with a space in BOTH names), `cx6h` (MIXED-quoted rename), `cx6i` (newline path
      reported ABSENT), `cx6j` (the same path PRESENT when its header is there), `cx6k` (the escaped-quote
      header shape git really emits). The new fixtures are PROSE/artifact on purpose: the only pre-existing
      non-ASCII fixture is a `.sh`, i.e. CODE *by accident*, which is why nothing covered this.
- [x] **STRUCTURAL asserts pin the boundary** (this is what stops round 5): every path-reading `git diff`
      carries `-z`; the census does not normalise in its loop and reads NUL records; the decoder is defined
      once and called only from the canonical matcher; the three retired mechanisms are absent from
      executable lines. Each verified to FAIL under a deliberate mutation.

## 8c. Round 5 blockers: evidence-based ambiguity, output neutralisation, non-mutating parse

- [x] **The header matcher resolves ambiguity from EVIDENCE, never positionally** (blocker 1, High — a FALSE
      PASS in `prompt-content:`, i.e. in the merge gate). REPRODUCED:
      `roborev_diff_header_has_path 'diff --git a/foo b/x b/foo b/x' foo` returned PRESENT, because
      `case $rest in "a/$want b/"*)` is a PREFIX test and `a/foo b/` prefixes the header of a file named
      `foo b/x`. Resolution order: (0) the header's own `rename from`/`rename to` (and `copy from`/`copy to`)
      lines — git ALWAYS writes them, one exact path per line; (4a) else accept ONLY a split whose two sides
      are EQUAL, because absent rename lines the header is a non-rename; (4b) else — no equal split, no
      rename lines — any valid split, DECLARED as the bounded residual (unreachable for git's own output).
      NOT fixed by failing closed: the ambiguity is irreducible with renames ON, so that would red every
      space-bearing header and re-break `cx6c`/`cx6g`/`cx6h`.
- [x] **The stale comment claiming the prefix test was safe is CORRECTED, not deleted** — a false safety
      claim is worse than none, because the next reader relies on it.
- [x] Header collection moved to the oracles file (`roborev_collect_prompt_headers`, awk, extended-header run
      BOUNDED) because the matcher now needs the lines FOLLOWING a header. The canonical boundary is kept,
      not moved: the matcher's INPUT widened, `roborev_unquote_path` still has one caller, and the consumer
      still holds no header-shape knowledge.
- [x] **No path reaches a summary value un-neutralised** (blocker 2, Medium — injection). A newline-bearing
      filename made a value SPAN LINES and inject keys, up to a forged `RESULT: PASS` (measured on the
      mutant: 3 `RESULT:` lines). Fixed CENTRALLY at the emit boundary — `emit_kv` for every block value,
      `finish` for every DETAILS line, both via `roborev_safe_line` (control characters ⇒ visible escapes).
      Per-site escaping was rejected: it is a list to keep complete. Quotes/backslashes/spaces stay intact so
      the block still names paths by their real bytes (`cx6b`); non-reversibility is the declared residual.
- [x] **The corroboration parse does not MUTATE reported patterns** (blocker 3, Low; #3260 item 1). Only a
      VERIFIED outer `[…]` container is stripped — `${out//[/}` destroyed a glob class (`src/[Tt]est.rs` ⇒
      `src/Ttest.rs`) and reported `corroboration: DRIFT`, a pre-enqueue FAIL on a CORRECT config.
- [x] Tests: `cx6l` (prefix reading cannot prove an unrelated path), `cx6m` (rename lines resolve an
      ambiguous header), `cx6n` (the SAME header without them proves neither side — so `cx6m` rests on the
      rename lines), `cx6p` (a filename cannot forge a key or the verdict; new `assert_one_result_line`
      helper, because `assert_verdict` reads `tail -1` and is blind to an injected line above the real one),
      `cx7c` (a bracketed answer carrying a glob class corroborates OK).
- [x] STRUCTURAL asserts for all three: the retired prefix test is absent from the matcher, the from/to
      resolution and the equal-split rule are present, the consumer does no `diff --git` scanning, every
      `emit_summary` value goes through `emit_kv` (all 23 keys), `emit_kv`/`finish` neutralise, and
      `"${DETAILS[@]}"` is no longer bulk-printed. Every new assert verified to FAIL under a deliberate
      mutation of its own fix, and the whole `cx6*` hostile-path family re-run green.

## 8f. Round-6 roborev blockers (job 30 — certified genuine: `prompt-content: PASS (6/6)`, 261k in / 183k cached / 10.4k out)
- [x] **A built-in exclude contributes ONE verbatim pathspec, not two** (blocker 1). The port emitted
      `<p>` **and** `<p>/**` for built-ins too, manufacturing `:(exclude,glob)**/Cargo.lock/**` — an
      exclusion roborev never applies. OVER-modelling is a false PASS: a path wrongly believed excluded is
      SUBTRACTED from `prompt-content:` coverage. **Established from the v0.61.2 binary, not inferred**
      (see D2b-0): the `:(exclude,glob)` prefix is inside each of the 24 string literals, proven
      non-coincidental by Go's length-ordered rodata packing (equal-length runs at deltas of exactly
      `15 + len(pattern)`), and only 2 bare prefix constants exist among 26 occurrences — so built-ins are
      pre-formatted constants appended verbatim and never reach `FormatExcludeArgs`. Built-ins now bypass
      the port entirely; CONFIGURED patterns keep both pathspecs. Blame lookup and the pathspec listing are
      driven off the same `_rx_owner_single` flag so no output advertises an exclusion git was not asked for.
- [x] **Artifact exclusions are scoped to artifact-bearing DIRECTORIES** (blocker 2, owner-approved posture).
      The `docs/**/*.<ext>` sweep hid FUNCTIONAL CONFIG, falsifying "noise, never blindness" for code-bearing
      formats: `docs/observability/grafana/dashboards/cqlite-overview.json` is guarded by the gate's own
      `kit-dashboard-drift` component yet was dropped from the diff AND classified code-free, and
      `docs/reports/delivery-telemetry.schema.json` went the same way. Now
      `<artifact-dir-glob>/**/*.<ext>` over 4 directories × 15 extensions + `*.md` = 61 patterns. Verified
      with `git ls-files`: 672 tracked `docs/` artifact-extension files → **667 still excluded** (incl. all
      **577** under `docs/reports/*-artifacts/`), **5 now reviewed** (both functional-config files + the
      telemetry ledger + 2 guide artifacts), and **63** harness code files under `docs/reports/*-artifacts/`
      still reviewed — so no blanket `<dir>/**` and no #3229 regression.
- [x] Census mirror follows the same shape (`CODE_FREE_ARTIFACT_DIR_GLOBS` ∩
      `CODE_FREE_ARTIFACT_EXTENSIONS`), matched COMPONENT-WISE to git `:(glob)` semantics rather than with a
      bash `case` (whose `*` crosses `/` and would match `docs/reports/a/b-artifacts/x`). Held in an ARRAY:
      as a space-separated string, unquoted iteration pathname-expanded the globs against `$PWD`, silently
      reducing the classification to "the directories that exist in this checkout" — caught by its own probe.
- [x] **STRUCTURAL mirror assert** (#3260 item 2, previously unguarded): `cx24` derives the expected pattern
      set from the constants and asserts SET EQUALITY against the committed `.roborev.toml` via the wrapper's
      OWN TOML parser, refuses a vacuous empty-set comparison, and rejects both retired forms off the PARSED
      set — never a file-wide grep, since `.roborev.toml` documents the forms it retired (that exact
      over-broad grep was caught red-handed on first run).
- [x] Tests: `cx22` (a tracked DIRECTORY named `Cargo.lock` — both keys asserted, incl. the false-PASS
      `prompt-content` excusal), `cx22b` (the built-in still eats the real FILE, so the fix did not
      UNDER-model), `cx23` (functional config is CODE and survives while a real artifact stays non-code),
      `cx23b` (the retired form reproduced, swallowing the dashboard — without it `cx23` would pass under
      either config), `cx24` (the mirror). `NARROWED_PATTERNS` moved to the shipped directory-scoped shape;
      the old value kept as `DOCS_WIDE_EXT_PATTERNS` to drive `cx23b`.
- [x] Every new assert MUTATION-TESTED both directions in a scratch copy — 6 mutations, each RED, restore
      green: M1 restore the phantom sibling (5 `cx22` asserts RED), M2 under-model configured patterns
      (`cx9` RED), M3 revert the config to docs-wide (3 `cx24` asserts RED, exact both-sides diff), M4
      one-sided constant drift (2 `cx24` RED), M5 revert the classification to the bare `docs/` prefix (6
      asserts across `cx23`/`cx23b` RED), M6 dir-globs back to an unquoted string (9 asserts across
      `cx3`/`cx6f`/`cx17`/`cx24` RED). Named regression set re-run green: `cx1`, `cx3`, `cx6`, `cx6c`–`cx6n`,
      `cx6p`, `cx7c`, `cx8`, `cx11`, `cx13`, `cx18`, `cx19*`, `cx20*`, `cx21`. Suite: 681 asserts, 0 failed.

## 8g. Round 7 — the `built-in-set:` self-check needed a right boundary and a version gate
- [x] REPRODUCED the false PASS on the real binary: patching `/usr/local/bin/roborev`'s length-28 run to
      `:(exclude,glob)**/Cargo.lock.bak:(exclude,glob)**/cargo.lock:(exclude,glob)**/flake.lock` (4 bytes
      borrowed from the preceding string, size unchanged) left the literal count at 26/26 and the missing
      list EMPTY ⇒ `built-in-set: OK` on a set that had moved. Unbounded substring presence + a bare count
      is sound in the REMOVAL direction only.
- [x] Fix, both halves: (A) `ROBOREV_PINNED_VERSION` is a machine-checked constant and the check asks the
      SAME file it read the literals from (`roborev version`) — a mismatch is DIVERGENCE ⇒ FAIL naming
      observed vs pinned and the re-verify obligation. (B) a RIGHT BOUNDARY derived from the blob's
      length-bucket adjacency: Go packs rodata in length order with no terminator and each bucket is ONE
      contiguous run, so per bucket of k members exactly k-1 must be immediately followed by another
      `:(exclude,glob)` literal. Derived from the pinned list alone — no foreign successor bytes, no
      within-bucket order — so a rebuild that merely permutes a bucket cannot false-FAIL.
- [x] Grammar preserved: `OK` | `UNAVAILABLE` | a `FAIL` naming what diverged. An unreadable version is
      `UNAVAILABLE` (withholds only the blessing); an OBSERVED divergence still FAILs without it. A bucket
      holding a MISSING member skips the adjacency arithmetic, so a removal still FAILs naming the pattern.
- [x] Tests: `cx25` (the equal-length tamper FAILs, bucket + unbounded member named, and the two
      pre-existing signals asserted SILENT), `cx25b` (the untampered control reads OK), `cx26` (version
      mismatch alone FAILs), `cx26b` (unreadable version ⇒ UNAVAILABLE, never OK), `cx26c` (a removal still
      FAILs without a readable version). The stub plants the MEASURED contiguous length-bucket runs, with
      `guard_assert_run_mirror_agrees` keeping the two harness mirrors on the same 24 patterns.
- [x] MUTATION-TESTED both directions in a scratch copy — 3 mutations, each RED, restore green: M1 disable
      the boundary check (9 asserts RED, `cx25` reproduces `RESULT: PASS` + `built-in-set: OK` on the
      tampered set and even enqueues the review), M2 disable the version gate (12 asserts RED across
      `cx26`/`cx26b`), M3 plant the literals one-per-line (19 asserts RED across `cx19d`/`cx25`, i.e. the
      harness mirror is load-bearing). Verified on REAL binaries too: pristine ⇒ OK, `Cargo.lock.bak` tamper
      ⇒ DIVERGED, a blanked `**/go.sum` prefix ⇒ DIVERGED naming it missing, a patched version string ⇒
      DIVERGED naming v0.99.9 vs v0.61.2, no binary ⇒ UNAVAILABLE. Named regression set re-run green.
      Suite: 718 asserts, 0 failed.

## 9. Certification
- [ ] `--lite` green each fix round (summary-file redirect) — DONE, see the PR — then `rust-reviewer` + `roborev` on the
      lite-green diff (the diff contains code — shell + config — so it IS roborev-certifiable and MUST be).
- [ ] Open the PR; hand the endgame to `flow-closer`: the ONE full `scripts/agent-gate.sh` run of record
      (`AGENT-GATE SUMMARY`, `RESULT: PASS`, `tree-integrity:` verified) → `spec-auditor` C intent audit
      against these specs → final roborev pass → `gh pr merge --auto --squash --delete-branch` after
      `scripts/flow/premerge-assert.sh`.
- [ ] After merge: verify the published doctrine page by served content (grep for a distinctive new
      phrase, never an HTTP 200 — the CDN can serve the previous page for ~3 minutes), then `flow-finalize`
      (archive this change, stamp delivery telemetry via a telemetry worktree PR).
- [ ] After merge: set #3229 to **`In Review`, NOT `Done`** — the AC2 demonstration is still outstanding and
      `Done` would auto-close the issue and lose the obligation. Flip to `Done` only once the evidence is
      posted (see §7).
