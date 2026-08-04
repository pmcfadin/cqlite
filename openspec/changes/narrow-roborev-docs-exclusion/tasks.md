# Tasks: narrow-roborev-docs-exclusion (issue #3229)

> Design decided in `design.md`. **As shipped, after the owner's DESCOPE ruling of 2026-08-04 (§8j):**
> narrow `exclude_patterns` from a blanket `docs/**` to a directory-scoped **prose/artifact deny-list**
> (an allow-list is not expressible), retain the canonical path-normalisation boundary, and STRENGTHEN the
> wrapper's terminal verdict grammar + affirmation backstop to exact-token matching. The pre-enqueue oracle
> that PREDICTED roborev's effective exclusion set is **DELETED and DEFERRED to #3283** — with it, **AC3 and
> AC4 are `deferred`, not satisfied and not waived**. roborev's compiled-in built-in deny-list is **#3278**.
> AC→requirement map is at the top of `specs/roborev-review-guard/spec.md`.
> AC7 backfill ruling: **RULED by the owner (2026-08-03) — accept as-is, no retroactive pass**;
> the ruling and its four-part reasoning are recorded in `design.md` D10 and
> `docs/reports/3229-artifacts/README.md`.

## 1. Record the mechanism knowledge as research, not as a contract this change implements
- [x] Record `git.FormatExcludeArgs` verbatim (the 8-line form in `design.md`), its recovery method
      (`.gopclntab` symbol parsing of the stripped binary, text base `0x401000`) and its caller list
      (`git.GetDiffCtx`, `GetDiffLimitedCtx`, `GetRangeDiffCtx`, `GetRangeDiffLimitedCtx`, `GetDirtyDiff`,
      `prompt.(*Builder).buildSinglePrompt` / `buildRangePrompt` / `resolveExcludes`), so a future reader
      can re-derive it.
- [x] CORRECTED BY §8j: this knowledge is now labelled **RECORDED FOR #3283 — no code in this change relies
      on it**. The ported code it was pinned "beside" no longer exists. `roborev v0.61.2` stays recorded as
      the version the disassembly was taken from, and the re-verify-on-upgrade obligation moves to #3283
      along with the port.
- [x] Add the caveat that governs the whole record: **reading the instructions is not the same as
      reproducing them.** The removed port re-derived Go's `TrimSpace`/`TrimRight`/`TrimLeft` in bash and
      diverged on U+00A0 (Go's `unicode.IsSpace` trims it, bash trims do not) because it was tested against
      a MODEL of Go rather than against Go. State the class-level root cause for #3283: **a port is a second
      implementation, and a second implementation's correctness is only knowable by differential testing
      against the original.**
- [x] Do NOT conflate the adjacent mechanisms: `max_prompt_size`, `exclude_branches`,
      `IsCommitMessageExcluded`, and `git.EnsureLocalExcludePattern` (`.git/info/exclude`) are separate.

## 2. Narrow the exclusion configuration (surface: `.roborev.toml`) — AC1, SHIPPED
- [x] Replace `exclude_patterns = ['docs/**', '*.md']` with `*.md` (unchanged — it already excludes every
      tracked `.md` repo-wide) plus artifact patterns for at minimum
      `txt json jsonl log err csv png svg gz pdf jfr html mmd tex diff`, each scoped to an ARTIFACT-BEARING
      DIRECTORY as `<artifact-dir-glob>/**/*.<ext>` — **not** swept across all of `docs/`, which hid
      functional config (round 6, §8f / design D1a).
- [x] Keep the value a SINGLE-LINE array, write every docs-scoped pattern WITH an interior `/` and WITHOUT
      a trailing slash (root-anchored per R1; a trailing slash would invert to recursive per R3), and verify
      by `git ls-files -- <the pathspec forms>` that no `.py`/`.sh`/`.bt`/`.c`/`.rs`/`.toml`/`.cql`/`.yml`/
      `.yaml` path under `docs/` is matched, that no path OUTSIDE `docs/` newly becomes excluded, and that
      `website/src/content/docs/**` is untouched. MEASURED on the final tree: **71 `docs/` executables
      reach the reviewer, 0 markdown does, nothing outside `docs/` is newly excluded.**
- [x] CORRECTED BY §8j: `.roborev.toml` is machine-managed (`roborev config set` rewrites it), so the list
      must be re-checked after any `roborev config` write — and there is **no automated detector for a
      re-broadening any more**. The standing controls are the file's own comment block (`docs/**` MUST NOT
      be reintroduced; never a trailing slash), AC6's doctrine, and `prompt-content:` failing closed on the
      consequence after the review round. Restoring a pre-enqueue detector is **#3283**.

## 3. The pre-enqueue reconciliation oracle — DEFERRED to #3283 (AC3, AC4)
> The items below were implemented and then **DELETED IN FULL** by the owner ruling of 2026-08-04 (§8j).
> They are kept, unchecked and explained, because #3283 inherits exactly this task list.
- [x] Declare the docs-scoped ARTIFACT extension/directory constants beside the existing
      `CODE_FREE_EXTENSIONS` / `CODE_FREE_EXTENSIONLESS_PREFIXES` and use them in the per-file census
      classification. **RETAINED** — the census half survives the descope and is what `code-free:` and
      `prompt-content:` classify against.
- [ ] **DEFERRED to #3283** — the effective-exclusion-set reader (parse the worktree/root `.roborev.toml`
      and `~/.roborev/config.toml`, respect TOML table scoping, UNION the lists per
      `config.ResolveExcludePatterns` / `loadRepoExcludePatterns`). Built here; deleted by §8j.
- [ ] **DEFERRED to #3283** — fail closed on an unreadable set, pass explicitly on a genuinely absent one,
      never alias the two. Built here; deleted by §8j.
- [ ] **DEFERRED to #3283** — `roborev_check_census_exclusion()` with pathspec construction as an EXACT PORT
      of `git.FormatExcludeArgs`, the `git diff --name-only -z --no-renames <base>...HEAD -- <pathspecs>`
      survivor query and `swallowed = census CODE paths − survivors`. Built here; deleted by §8j. #3283 must
      not re-land it without a **differential** harness against the real binary (§1's caveat).
- [ ] **DEFERRED to #3283** — FAIL on a TRAILING-SLASH pattern with the inversion named. Built here; deleted
      by §8j. The rule itself survives as documentation in `.roborev.toml` and `design.md` R3.
- [ ] **DEFERRED to #3283** — the value grammar naming each swallowed path and the pattern that ate it,
      capped at 10 with `(+<r> more)`. Built here; deleted by §8j.
- [x] Be NUL-safe end to end (`-z`, bash arrays, no word splitting) — the repo tracks a `docs/` path with
      spaces and a literal double quote that a non-`-z` read would emit quoted and never match.
      **RETAINED** — this is the canonical path-normalisation boundary (§8d, design D3), which survives.
- [ ] **DEFERRED to #3283** — corroborate the parse against `roborev config get exclude_patterns`. Built
      here; deleted by §8j. Its known-unfixed gate defect (a positive verdict reachable without an
      affirmative measurement) is removed WITH the subject, not waived.

## 4. Wrapper registration for the oracle — REMOVED with its subject (AC3)
- [ ] **DEFERRED to #3283** — the key's state initialisation, its position in `emit_summary()`'s FIXED key
      order, its call site before the checks-file validation and the enqueue, its entry in the verdict-scan
      failing-capable key list, and its `usage()` documentation. All four registration points were wired and
      are now removed; **the summary block is back to 22 keys**.
- [x] PIN THE REMOVAL, both directions: the deleted key is absent from the verdict-scan key list **and**
      from the emit line (so the removal is visible in the OUTPUT contract, not only in the source) and from
      `--help`, and the deleted functions are asserted absent by name. A removal that is not pinned is a
      removal that comes back.

## 5. Hermetic regression cases (surface: `scripts/tests/test_roborev_review_guard.sh`) — AC5
- [x] `docs-executables` fixture mode (`.py`/`.sh`/`.bt` under `docs/reports/x-artifacts/`).
- [x] Case: executables under `docs/` ⇒ `code-free: PASS`, `prompt-content: PASS`, IS enqueued (`cx1`).
- [x] Case: prose-only under `docs/` ⇒ `code-free: FAIL`, `assert_never_enqueued` (the guard is not
      inverted) (`cx2`).
- [x] Case: docs artifacts only (`.txt`/`.json`/`.log`/`.err`) ⇒ still `code-free: FAIL`, never enqueued
      (`cx3`).
- [ ] **DEFERRED to #3283** — the config-regression cases: a restored `['docs/**','*.md']` ⇒ a pre-enqueue
      FAIL naming the swallowed paths; an unparseable value ⇒ unreadable-FAIL; an absent key/file ⇒ an
      explicit pass; the corroboration `UNAVAILABLE`/DRIFT pair; the ported-construction cases (R1–R5); the
      replication-fidelity case against the pre-change set; the linked-worktree root-config case. All were
      written and PASSED; all are deleted with their subject. **`make_fixture` no longer writes a
      `.roborev.toml` at all** — nothing reads one, and an inert input that reads as load-bearing is the same
      class of misleading test (§8c, and design D2b lesson 3).
- [x] Case: a census path with spaces + a literal double quote compares correctly (NUL-safety), plus every
      other diff-header shape git emits (`cx6`, `cx6c`–`cx6k`).
- [x] Cases: the header-ambiguity resolution in both directions (`cx6l`/`cx6m`/`cx6n`) and verdict forgery
      by filename (`cx6p`).
- [x] Case: the `0/0` floor driven DIRECTLY against `roborev_check_prompt_content` (`cx21`).
- [x] Cases: the CLOSED verdict grammar and the affirmation backstop — `cx28` (a value outside the grammar
      FAILs), **`cx28b`/`cx28c` (NEW: the near-prefix mutants `PASSthisNeverRan` and
      `PASS-MEASUREMENT-DID-NOT-HAPPEN` FAIL, so the closure tests a STATE and not a spelling)**, `cx29` (an
      un-run check cannot ride to PASS on its initial `SKIP`), plus the structural asserts on the scan and
      backstop statements.
- [x] Keep every case hermetic (stub `roborev` first on `PATH`, `STUB_*`-driven, no network/cargo/real
      reviewer) and keep the hermeticity meta-assert green. The file is over the ~1500-line test target, so
      growth is flagged and run with `CQLITE_ALLOW_FILE_GROWTH=1` and a note linking #1135.
- [x] Confirm the suite still runs in the `roborev-lints` component (present in BOTH the full and `--lite`
      component sets), so a regression FAILs the fast loop. Tally after the descope: **477 passed, 0
      failed** (from 644).

## 6. Doctrine, in this same change — AC6
- [x] `CLAUDE.md` roborev rule 4 (+ its T3 sentence and the docs-only/CITE-AND-WAIVE region): retire the
      falsified "roborev EXCLUDES non-code paths" claim, state the configured-pathspec mechanism, name the
      `docs/reports/*-artifacts/` harness convention as reviewed code, and define docs-only as a code-free
      CENSUS (never a path prefix).
- [x] CORRECTED BY §8j: doctrine documents the **22-key** block order — the key an earlier round added is
      removed, so no doctrine surface may list it. And doctrine now carries the **re-broadening hazard in
      prose**, because that is what stands in place of the deferred automated guard: `docs/**` must not be
      reintroduced, a trailing slash inverts to recursive, and `roborev config set` can rewrite the file.
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
      set does not apply to this change's own review. A committed executable under root `docs/` is therefore
      swallowed by the OLD set until merge — a DEADLOCK, not a test. `git rm`'d the probe executable;
      procedure kept as prose in `docs/reports/3229-artifacts/live-probe-procedure.md`.
- [x] KEEP `website/src/content/docs/reports/_3229-artifacts/_3229-root-anchoring-probe.json` on the branch
      — a `.json` under a NESTED `docs/reports/*-artifacts/` directory is not swallowed by the configured
      set (root anchoring), so it does not deadlock and is live evidence either way. Its ABSENCE from a
      prompt would falsify the root-anchoring finding and block; it is not an acceptable outcome to merely
      record.
- [x] Round 9: RELOCATE the probe so it DISCRIMINATES again. At its old path
      (`website/src/content/docs/_3229-root-anchoring-probe.json`) it discriminated against the pre-round-6
      `docs/**/*.json`; ⑦a's directory-scoping removed that pattern, after which NO configured pattern
      matched the old path under EITHER reading — it survived unconditionally, which is vacuous evidence,
      not weak evidence. The new path is matched by `**/docs/reports/*-artifacts/**/*.json` (incorrect
      reading) and not by `docs/reports/*-artifacts/**/*.json` (correct reading). Established with the
      then-existing port plus the same survivor query, both directions, against a must-be-EXCLUDED and a
      must-SURVIVE control — never hand-rolled `git ls-files` pathspecs, which measurably answer 0-of-95 on
      this repo. NOTE after §8j: that measurement harness was the deleted oracle, so **re-establishing the
      discrimination after a future pattern change now needs the procedure written out in `design.md` D2b
      lesson 6**, not a function call.
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
      `census:` + `prompt-content: PASS (<n>/<n>)` lines to #3229. (The pre-enqueue line an earlier revision
      of this task expected is gone with its key; `prompt-content:` is the line that says the reviewer
      actually RECEIVED the executables, which is the property AC2 is about.) Better evidence than a probe
      written to pass, because the diff was not shaped for it. FALLBACK: the documented procedure.
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
      **RULED (owner, 2026-08-03): ACCEPT AS-IS, no retroactive pass**, with the four-part reasoning in
      `design.md` D10. One amendment forced by §8j: reason 3 originally read "the class cannot recur
      silently" on the strength of the pre-enqueue detector; with that detector deferred, the honest form is
      "the configuration is fixed and the consequence still fails closed under `prompt-content:`". The
      ruling itself is unchanged, and so is its promotion clause: harness code promoted into a shipped path
      (a gate component, a CI step, an imported module) inherits the review obligation of the surface it
      joins.

## 8. Follow-up to file (not fixed here)
- [x] File an issue for `scripts/ci/classify-docs-only.sh` `is_docs_file()`'s blanket `case "$path" in
      docs/*)` — the same "path glob swallows executables under `docs/`" defect in the CORRECTNESS gate: a
      PR touching only `docs/reports/*-artifacts/*.sh` is classified docs-only and short-circuits
      `pr-gate-core` to green. Test surface `scripts/tests/test_classify_docs_only.sh`. Link it from this
      change's PR.
- [x] Note the upstream ask in the PR as a non-blocking follow-up: an allow-list/negation form for
      `exclude_patterns`; **a way to PRINT the resolved exclude pathspecs** (which would retire #3283's
      whole port-and-differential problem); and a non-zero exit when everything is excluded.

---

> **§8b–§8i are HISTORY.** They record the review rounds that happened. Where an item's subject was the
> exclusion oracle, that subject was **DELETED** on 2026-08-04 (§8j): read those items as "this work was
> done and has since been removed with its subject", never as claims about the shipped code. Items about the
> census, the path-normalisation boundary, the summary-block emit boundary and the verdict grammar describe
> **retained** code.

## 8b. Blocker round from the first sanctioned roborev pass (two false-PASS classes) — subject since DELETED
The first sanctioned round returned `RESULT: FAIL` with `prompt-content: FAIL (1/7 code census paths
absent)` while the oracle said its census-vs-config check had PASSed — i.e. the new guard itself reported
green about a swallow it existed to catch. Two independent root causes, both false PASSes:
- [x] **A — the wrong config file.** The oracle read `$REPO/.roborev.toml` (the WORKTREE) while roborev's
      daemon binds the repo by `repos.root_path` (the ROOT checkout) and reads THAT file. Fixed at the time
      by resolving the root checkout from git, evaluating ALL config sources as a UNION, failing on a swallow
      in ANY, corroborating from EVERY checkout read, and NAMING the source file in every value. **All of it
      deleted by §8j; the root-path/daemon-snapshot FINDING is retained in `design.md` and doctrine.**
- [x] **B — corroboration skipped exactly where it is the only oracle.** An empty parse returned a pass
      BEFORE corroboration ran, aliasing "our parser recognised no key" to "nothing is configured". Verified
      live on v0.61.2: a QUOTED key `"exclude_patterns"` IS honoured while the bare-key match skipped it.
      Fixed at the time; **deleted by §8j**, and the LESSON is retained as design D2b lesson 5, which #3283
      must honour.
- [x] Refuse an unknown/untranslated TOML basic-string escape fail-closed; return un-quoted paths through a
      named global so `$(…)` cannot strip a trailing newline byte. The TOML half went with the parser; the
      named-global un-quoting is **retained** (it is part of the path boundary).
- [x] Redirect `HOME` in the two hand-rolled wrapper invocations (t7, t9) that bypassed `run_wrapper`'s
      fixture home. Retained as hygiene; the global-config read that motivated it is gone.
- [x] New hermetic cases for all of the above (`cx5*`, `cx18*`). **Deleted by §8j** together with the code
      they drove.
- [x] Specify the multi-source rule and the corroborate-on-empty-parse rule in the delta spec and
      `design.md`. **Both requirements are REMOVED from the delta by §8j** and belong to #3283.
- [x] EXPECTED CONSEQUENCE, recorded not suppressed: with A fixed, the oracle correctly FAILed on this very
      branch, because the ROOT checkout's `.roborev.toml` still carried the blanket `docs/**` and the diff
      contained a probe executable under `docs/`. That FAIL was the guard working, and it is what identified
      the AC2 deadlock (§7). The narrowed config only takes effect on the root checkout once this PR MERGES —
      a scheduling consequence for the owner, never a reason to hand-edit the root checkout's config.

## 8c. Owner rulings applied after the first sanctioned roborev round
- [x] **The unifying verdict rule**, stated verbatim in CLAUDE.md, `roborev-findings.md`, `design.md` and
      `--help`: **FAIL where the author can act; NOTICE where only the information is actionable; never
      silence.** It resolved the deleted key to a single call (a CONFIGURED swallow ⇒ FAIL, pre-enqueue, so
      the key had no `NOTICE` value); with that key gone the rule stands as general doctrine.
- [x] CONFIRMED by reading the wrapper's verdict scan directly (and asserted structurally against it):
      the failing-capable set is exactly `FAIL|FINDINGS|ERROR|INCONSISTENT` and `NOTICE` is absent from it
      (`vacuity-tier1:` needs it as an advisory). **Retained and strengthened by §8j to exact-token
      matching.**
- [x] **Doctrine — three properties, one generalization**, recorded in CLAUDE.md and
      `roborev-findings.md` beside the existing BASE-ref note: (1) roborev's daemon reads
      `exclude_patterns` from the repo ROOT PATH, so a worktree edit is INVISIBLE to it; (2) the daemon
      SNAPSHOTS config at start, so an edit needs a RESTART; (3) generalized — **any PR whose subject is a
      config the daemon (or a gate) reads from root cannot certify itself**. Both (1) and (2) have cost real
      rounds, and the write-up says so. **Retained.**
- [x] RECORD, not smooth over: **`prompt-content:` — the PRE-EXISTING guard — caught the NEW guard**
      certifying a config roborev never used. Kept as the argument for uncorrelated layers, explicitly
      because it paid out in the direction nobody plans for (the NEW layer was the wrong one). It is also,
      in hindsight, the first datum of the trajectory §8j acted on. In `design.md` D2b lesson 4.
- [x] RECORD the cross-worker arbitration in `proposal.md` + `design.md`: #3234 measured `exclude_patterns`
      as having "no observable effect"; the owner ranked H2 (config resolves from the primary checkout). The
      disassembly + 21-review replay proved the MECHANISM half, #3234 independently supplied the ORDERING
      half. **Conclusion: `exclude_patterns` WORKS — #3234's null result was a worktree-config artifact, so
      AC1 is a genuine fix.** This was the live existential risk to the change and it is closed. **Retained.**
- [x] RECORD that a test which BLESSES a vacuous verdict is WORSE than an unguarded path: it consumes the
      review budget that would otherwise have found the bug, and converts "nobody checked" into "we checked
      and it was fine". **Retained as design D2b lesson 3 — and applied by §8j, which deleted the fixtures'
      `.roborev.toml` support so no inert input reads as load-bearing.**

## 8d. ONE canonical path-normalisation boundary (round 4 — the pattern behind SIX blockers) — RETAINED
- [x] **Normalise ONCE, at the census.** `roborev_census` reads `git diff --numstat -z --no-renames` and
      parses NUL-terminated records (`read -r -d ''`), so paths arrive RAW and a newline-bearing path
      survives. RAW is the SINGLE internal representation for classification, comparison and display.
- [x] **Every consumer audited, not just the two reported**: the census classification loop (was reading
      the QUOTED extension — `md"`/`json"` — so PROSE counted as CODE ⇒ a false pre-enqueue FAIL under
      `*.md`); the oracle's survivor comparison (both sides `-z`; the oracle itself is gone, the invariant is
      not); `prompt-content:` membership; the wrapper's `--help`/key documentation.
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
- [x] **STRUCTURAL asserts pin the boundary**: every path-reading `git diff` carries `-z`; the census does
      not normalise in its loop and reads NUL records; the decoder is defined once and called only from the
      canonical matcher; the three retired mechanisms are absent from executable lines. Each verified to
      FAIL under a deliberate mutation.

## 8e. Round 5 blockers: evidence-based ambiguity, output neutralisation, non-mutating parse
- [x] **The header matcher resolves ambiguity from EVIDENCE, never positionally** (blocker 1, High — a FALSE
      PASS in `prompt-content:`, i.e. in the merge gate). REPRODUCED:
      `roborev_diff_header_has_path 'diff --git a/foo b/x b/foo b/x' foo` returned PRESENT, because
      `case $rest in "a/$want b/"*)` is a PREFIX test and `a/foo b/` prefixes the header of a file named
      `foo b/x`. Resolution order: (0) the header's own `rename from`/`rename to` (and `copy from`/`copy to`)
      lines — git ALWAYS writes them, one exact path per line; (4a) else accept ONLY a split whose two sides
      are EQUAL; (4b) else any valid split, DECLARED as the bounded residual (unreachable for git's own
      output). NOT fixed by failing closed: the ambiguity is irreducible with renames ON. **RETAINED.**
- [x] **The stale comment claiming the prefix test was safe is CORRECTED, not deleted** — a false safety
      claim is worse than none, because the next reader relies on it.
- [x] Header collection moved to the oracles file (`roborev_collect_prompt_headers`, awk, extended-header run
      BOUNDED) because the matcher now needs the lines FOLLOWING a header. The canonical boundary is kept,
      not moved: the matcher's INPUT widened, `roborev_unquote_path` still has one caller, and the consumer
      still holds no header-shape knowledge.
- [x] **No path reaches a summary value un-neutralised** (blocker 2, Medium — injection). A newline-bearing
      filename made a value SPAN LINES and inject keys, up to a forged `RESULT: PASS` (measured on the
      mutant: 3 `RESULT:` lines). Fixed CENTRALLY at the emit boundary — `emit_kv` for every block value,
      `finish` for every DETAILS line, both via `roborev_safe_line`. Per-site escaping was rejected: it is a
      list to keep complete. Quotes/backslashes/spaces stay intact so the block still names paths by their
      real bytes (`cx6b`); non-reversibility is the declared residual. **RETAINED** — the structural assert
      now covers all **22** keys.
- [x] **The corroboration parse does not MUTATE reported patterns** (blocker 3, Low; #3260 item 1). Only a
      VERIFIED outer `[…]` container was stripped — `${out//[/}` destroyed a glob class (`src/[Tt]est.rs` ⇒
      `src/Ttest.rs`) and reported drift, a pre-enqueue FAIL on a CORRECT config. **Fix deleted with the
      corroboration oracle by §8j**; the lesson (*a guard that reds a legitimate configuration is the guard
      that gets disabled*) is retained in `design.md` and is a #3283 constraint.
- [x] Tests: `cx6l`, `cx6m`, `cx6n`, `cx6p` (retained), `cx7c` (deleted with the corroboration oracle).
- [x] STRUCTURAL asserts: the retired prefix test is absent from the matcher, the from/to resolution and the
      equal-split rule are present, the consumer does no `diff --git` scanning, every `emit_summary` value
      goes through `emit_kv`, `emit_kv`/`finish` neutralise, and `"${DETAILS[@]}"` is no longer
      bulk-printed. Every assert verified to FAIL under a deliberate mutation of its own fix.

## 8f. Round-6 roborev blockers (job 30 — certified genuine: `prompt-content: PASS (6/6)`, 261k in / 183k cached / 10.4k out)
- [x] **Artifact exclusions are scoped to artifact-bearing DIRECTORIES** (blocker 2, owner-approved posture).
      The `docs/**/*.<ext>` sweep hid FUNCTIONAL CONFIG, falsifying "noise, never blindness" for code-bearing
      formats: `docs/observability/grafana/dashboards/cqlite-overview.json` is guarded by the gate's own
      `kit-dashboard-drift` component yet was dropped from the diff AND classified code-free, and
      `docs/reports/delivery-telemetry.schema.json` went the same way. Now
      `<artifact-dir-glob>/**/*.<ext>` over 4 directories × 15 extensions + `*.md` = 61 patterns. Verified
      with `git ls-files`: 672 tracked `docs/` artifact-extension files → **667 still excluded** (incl. all
      **577** under `docs/reports/*-artifacts/`), **5 now reviewed**, and **63** harness code files under
      `docs/reports/*-artifacts/` still reviewed — so no blanket `<dir>/**` and no #3229 regression.
      **SHIPPED — this is AC1's value.**
- [x] Census mirror follows the same shape (`CODE_FREE_ARTIFACT_DIR_GLOBS` ∩
      `CODE_FREE_ARTIFACT_EXTENSIONS`), matched COMPONENT-WISE to git `:(glob)` semantics rather than with a
      bash `case` (whose `*` crosses `/`). Held in an ARRAY: as a space-separated string, unquoted iteration
      pathname-expanded the globs against `$PWD`, silently reducing the classification to "the directories
      that exist in this checkout". **RETAINED.**
- [x] The STRUCTURAL mirror assert (#3260 item 2) derived the expected pattern set from the constants and
      asserted SET EQUALITY against the committed `.roborev.toml` via the wrapper's OWN TOML parser.
      **DELETED by §8j with that parser** — a one-sided edit of the census/config pair is no longer caught
      mechanically. Failure direction is bounded (review noise, then a fail-closed `prompt-content:`); the
      remedy is the config comment's instruction to edit both in one commit; restoring the assert is #3283.
      Recorded as a residual in `design.md` D1a rather than left implicit.
- [x] Tests: `cx23`/`cx23b`/`cx24` covered the scoping and the mirror. **Deleted by §8j** (each required a
      fixture-supplied `.roborev.toml`); the shipped constants they pinned are still exercised by `cx3`
      and `cx6f`.
- [x] Every new assert MUTATION-TESTED both directions in a scratch copy at the time — 6 mutations, each
      RED, restore green.

## 8i. Round 11 — the built-in-exclude modelling is DELETED (owner ruling; deferred to #3278)
- [x] **Ruled option A: remove the roborev built-in-exclude modelling entirely.** Rationale, recorded because
      a deletion needs one: four consecutive review rounds (jobs 30, 31, 32, 33-H1) found four false-PASSes
      **all inside that subsystem**; **no acceptance criterion reaches it**; and **subtraction cannot
      introduce a false PASS**.
- [x] DELETED from the oracles file: the built-in constants, the pinned-version observation with its rodata
      length-bucket boundary, the state-details helper, the `built-in-set:` grammar, the excusal machinery,
      the built-in branch of the port and the owner-arity flag.
- [x] DELETED from `scripts/flow/roborev-review.sh`: the built-in excusal state, and the affirmation
      backstop's per-key `NOTICE` exemption — its **only** escape hatch. All deterministic keys must be
      affirmatively `PASS`, which is STRICTER. A structural assert reads the backstop's own `case` body and
      requires exactly ONE exempting arm, so no hatch can be reintroduced. **RETAINED and strengthened by
      §8j** (exact-token matching; six keys).
- [x] DELETED from `scripts/flow/roborev-review-checks.sh`: the built-in subtraction and `prompt-content:`'s
      `(+<n> not expected: …)` clause. **`prompt-content:` now expects EVERY census code path; no key can
      tell it which to skip. RETAINED.**
- [x] DELETED from the suite: the built-in case families, the pinned-set mirrors and their agreement check,
      the built-in stub and its `version` subcommand, and the lockfile fixture modes.
- [x] **AC4's SECOND BRANCH — SUPERSEDED BY §8j.** This round argued that AC4 was satisfied through its
      second branch (*"…or the residual disagreement is documented with the exact cases where it
      persists"*), the residual being roborev's compiled-in deny-list, documented and pinned by `cx30`/
      `cx30b` and failing closed. **That argument is VOID**: it rested on the disjunction's FIRST branch
      being in place — a live reconciliation of the census against the configured set, with the built-in as
      the only declared gap. §8j deleted that reconciliation, so no branch remains to satisfy and **AC4 is
      `deferred` to #3283**. The `cx30`/`cx30b` pair is deleted with it. The built-in residual itself stands,
      is documented in `design.md` D2a, still fails closed under `prompt-content:`, and is tracked as
      **#3278**.
- [x] **Job-33 H1 is DELETED, not waived.** The excusal mechanism it was a finding about no longer exists, so
      **no subject remains**. Nothing was excused or judged acceptable-with-the-defect-present; deferred to
      **#3278**. Written down because a High finding that vanishes with its subject reads identically to a
      waiver unless it is said explicitly.
- [x] MEASURED for #3096, and reported without adjustment: a #3096-shaped diff now yields
      **`prompt-content: FAIL`** — option **(iii)**, not the numeric `PASS (n/n)` that worker was told to
      expect, because **F1 lived inside the deleted subsystem**. Re-anchoring #3096 is the owner's call.
- [x] MUTATION-TESTED both directions in a scratch copy — 6 mutations, each RED, restore green at 644/0 (the
      tally at the time; §8j took it to 477/0).

## 8j. TERMINAL ROUND — DESCOPE (owner ruling, 2026-08-04): the exclusion oracle is DELETED, AC3/AC4 deferred to #3283
- [x] **Ruled: delete the pre-enqueue oracle that PREDICTED roborev's effective exclusion set, in full.**
      Removed: the bash port of `git.FormatExcludeArgs`, the TOML array parser, the three-source config union
      (worktree/root `.roborev.toml` + global `~/.roborev/config.toml`), the `roborev config get`
      corroboration oracle, the trailing-slash FAIL, the survivor computation, the summary key and its value
      grammar, the state variable, the call site, the verdict-scan registration, the `--help` text, every
      case family that drove it, and the fixtures' ability to supply a `.roborev.toml`.
- [x] **RECORD THE REASONING, because a deletion needs one.** (1) The false-PASS blockers review found
      *inside that oracle* were **INCREASING** across rounds 8→11 — **1, 1, 2, 3** — and **two of round 11's
      three defects lived in code the two preceding fix rounds had just introduced**: a surface where the
      fixes add defects of the class they close. (2) **A guard with known documented false-PASSes is worse
      than no guard, because it invites reliance it cannot support.** (3) **Subtraction cannot add a
      false-PASS** — with nothing modelling roborev's exclusions, no key can excuse a path, and every failure
      direction the deletion opens is a FAIL under `prompt-content:`, after the review round rather than
      before it.
- [x] **Three known-but-unfixed defects removed WITH their subject, not fixed**: a corroboration gate that
      could reach a positive verdict without an affirmative measurement; an **NBSP `TrimSpace` port
      divergence** (Go's `unicode.IsSpace` trims U+00A0, bash trims do not); and prefix-glob holes around the
      key. Nothing was excused, waived, or judged acceptable-with-the-defect-present.
- [x] **RECORD THE CLASS-LEVEL ROOT CAUSE FOR #3283: a port is a second implementation, and a second
      implementation's correctness is only knowable by differential testing against the original.** The
      oracle was tested against a MODEL of Go, not against Go, which is why the NBSP divergence was
      **unfindable by care**. #3283 must either obtain the resolved pathspecs from roborev itself or stand up
      a differential harness against the real binary before any verdict rests on a port.
- [x] **AC3 and AC4 are `deferred` to #3283 — not satisfied, not waived, not unmet.** The two ADDED
      requirements that carried them are REMOVED from the delta spec together with their implementation, so
      nothing lands in `openspec/specs/` half-satisfied and a C audit assesses **four** ACs (1, 5, 6, 7).
- [x] **State the cost plainly: this IS a reduction in coverage, and an acceptable one.** The absent
      coverage, named: *no automated guard against a future `.roborev.toml` re-broadening; the regression it
      would catch is a hand edit to a version-controlled file on `main`, and AC6's doctrine names the hazard
      in prose.* Recorded in `proposal.md`, `design.md` D0 and the delta spec — never as "no reduction".
- [x] **RETAIN and STRENGTHEN the verdict-grammar closure and the affirmation backstop.** Both now match the
      **VERDICT TOKEN — the value up to its first space — EXACTLY, never as a prefix glob**: a `PASS*` glob
      accepted `PASSthisNeverRan` and `PASS-MEASUREMENT-DID-NOT-HAPPEN`, so the closure was checking a
      SPELLING rather than a STATE. The backstop names **six** deterministic keys with **no per-key
      exemption**. Justification for retaining rather than deleting with the oracle: the permissive verdict
      scan was a **PRE-EXISTING** defect this change's sweep found, not something the oracle introduced, and
      leaving the wrapper worse than we found it would be a bad trade — deleting the closures would itself
      re-admit a false-PASS, which the descope's own principle forbids. Decision record in `design.md` D4.
- [x] NEW cases `cx28b`/`cx28c` pin the near-prefix mutants, plus structural asserts that the scan extracts
      a verdict token and carries **no** `TOKEN*` glob anywhere. NEGATIVE CONTROL: a mutation reverting to
      prefix globs makes **both** mutants reach `RESULT: PASS`, which is what proves the asserts bite.
- [x] Summary block: **23 → 22 keys**. Suite: **644 → 477 asserts, 0 failed**. The removal is pinned in the
      OUTPUT contract (the key absent from the verdict-scan list AND from the emit line AND from `--help`)
      and by name for each deleted function.
- [x] Sweep the change's own artifacts so none describes the deleted oracle as shipped: `proposal.md`
      (AC1+AC2+AC5+AC6+AC7 delivered, AC3/AC4 deferred), `design.md` (the mechanism knowledge relabelled
      **RECORDED FOR #3283 — no code in this change relies on it**, with the "reading ≠ reproducing" caveat;
      D2 rewritten as a removal record; D4's decision record added), this file, and the delta spec.
- [x] Cite **#3283** for the deferred exclusion oracle and **#3278** for roborev's compiled-in built-in
      deny-list (a separate, still-open, still-unmodelled thing) everywhere either is referenced.

## 8k. Round-11 roborev blocker — the EXTENSIONLESS class the guard made no claim about
- [x] **An extensionless path under a prose prefix is CODE iff git records it EXECUTABLE.**
      `CODE_FREE_EXTENSIONLESS_PREFIXES` was applied as a bare prefix test, so EVERY extensionless path under
      `docs/`/`openspec/`/`website/`/`.claude/` classified non-code and never entered `census_code_paths` —
      while the narrowed `exclude_patterns` exclude only `*.md` globally plus the artifact intersection, so
      such a path is NOT excluded and DOES reach the reviewer. `prompt-content:` was therefore SILENT on
      exactly AC2's trigger class ("the first post-merge PR carrying an executable under `docs/`") whenever
      the executable has no extension, and a `PASS (n/n)` was a true statement about a subject set that had
      dropped the file in question. **SHIPPED** in `roborev_path_exec_state` + the census's extensionless
      branch.
- [x] **The mode is read from GIT'S TREE, never `test -x`**: `ls-tree`, with `:(literal)` pathspec magic
      because a tracked name may contain `*`/`?`/`[`, and on the same RAW path the census holds so the single
      normalisation boundary is unchanged. `core.fileMode=false` also makes working bits non-authoritative.
- [x] **The range test is a DISJUNCTION over BOTH endpoints, not an ordered scan.** The first revision read
      "HEAD, falling back to BASE" and `return`ed on the first ref yielding a record, so a path present at
      HEAD **never reached BASE**: a pure `chmod -x` (`100755`@BASE → `100644`@HEAD) classified NON-CODE and
      left `census_code_paths`, making `prompt-content: PASS (n/n)` silent about it — a false PASS, and a
      contradiction of the rule's own premise that the census subject is the RANGE. Reproduced on a
      two-commit fixture with a still-executable control. Deletion was already right, so the defect was
      precisely the MODE-CHANGE case, which no then-existing fixture could produce (all four had the path at
      exactly ONE endpoint). **FIXED BY CONSTRUCTION**, not by moving the `return`: the endpoint list is
      produced complete before the fold; the fold body has no `return`/`break`/`continue` and only ORs into a
      monotone accumulator, so the sole `return` is post-loop; and the per-endpoint lookup
      (`_roborev_mode_exec_state_at`) is range-BLIND, so no precedence is expressible. All four combinations —
      both / HEAD-only / BASE-only / neither — fall out of the one rule.
- [x] **The `-z` NUL warning is gone**: the single-path `ls-tree` lookup captured `-z` output through
      `$(...)`, so bash warned `ignored null byte in input` on EVERY call — harmless (only the terminating
      NUL is lost) but per-call stderr noise able to mask a real warning. Safe to drop because the PATH FIELD
      IS NEVER READ; only the leading MODE is, and it is first, space-terminated and one of git's literal
      mode constants. A `-z`-only mutant reds exactly one assert (the stderr-cleanliness one), which is the
      evidence the removal is behaviour-neutral.
- [x] **The design's measurement was WRONG and is corrected**: the three extensionless `docs/` files were
      recorded as "compiled binaries … correctly not code to review". `file(1)`:
      `docs/reports/ws0-3217-artifacts/partB-run/offcputime-bigmap` is a **379-line Python script**; only
      `ws0-readbw`/`ws0-stream` are ELF. All three are 100755. Re-measured on the final tree through the real
      census: docs/ executables classified CODE **46/49 → 49/49**, docs/ code paths **75 → 78**, delta =
      exactly those three paths.
- [x] Tests `cx3a`–`cx3d`: the extensionless executable is CODE and EXPECTED (2/2 of a 4-file census, which
      also proves the `.md` and an extensionless 100644 `NOTICE` are still non-code and a non-executable
      `.sh` is still CODE); the same path ABSENT from the prompt is a FAIL that NAMES it; the same path at
      mode **100644** is still non-code (one-variable control); a DELETED extensionless executable is
      classified from the BASE tree. Fixtures set BOTH the on-disk bit and the index mode, and
      `assert_tracked_mode` reads the recorded mode back from the tree. Mutation-tested in both directions:
      reverting to the prefix-only rule reds 9 asserts (cx3a/cx3b/cx3d), and swapping the tree read for
      `test -x` reds 4 (cx3d only) — so each half of the fix is load-bearing.
- [x] Tests `cx3e`–`cx3j`: the ENDPOINT-COMBINATION axis, which `cx3a`–`cx3d` structurally could not reach
      (each of their fixtures has the path at exactly one endpoint, so a single-endpoint implementation
      passed all four). `cx3e` a PURE `chmod -x` present at BOTH endpoints is CODE and EXPECTED, with the
      working-tree bit asserted clear so only the BASE tree can answer; `cx3f` the same path absent from the
      prompt is a FAIL that NAMES it; `cx3g` the mirror `chmod +x`; `cx3h` the full matrix as a direct unit
      probe over one repo carrying every combination (both / HEAD-only / BASE-only / neither, both
      mode-change directions, a `glob[x]*?-exec` name proving `:(literal)` still holds, and
      `absent-everywhere`, which is reachable ONLY by direct probe) plus an assert that nine classifications
      write NOTHING to stderr; `cx3i` in-test mutation of the SEMANTICS in both directions, each with a KEPT
      case proving the mutant lost one endpoint rather than the whole function, and a restored-green diff;
      `cx3j` a shape-AGNOSTIC structural assert (no `break`/`continue`, exactly one `return`, last),
      controlled against an injected `return 0`, an injected `break`, the ROUND-12 shape verbatim, and an
      absent-function probe that must read `NOT-FOUND` so a rename cannot pass vacuously. Whole-suite
      mutation: ordered scan restored ⇒ **19 RED**; consult-only-HEAD ⇒ **19 RED**; consult-only-BASE ⇒
      **17 RED**; unmutated ⇒ **551/551**. Assertion count 501 → 551.

## 8l. Round-14 roborev blocker — the LEAF was still two-valued, so it collapsed uncertainty
- [x] **THE CLASS-LEVEL RULE, which is the durable part: any predicate feeding a safety decision must be
      TRI-VALUED — yes / no / could-not-measure — because a boolean CANNOT express uncertainty and will
      therefore collapse it onto the PERMISSIVE side.** This was the **ninth** instance of "could not measure"
      rendered as "nothing wrong" on this change (after `built-in-set: UNAVAILABLE`,
      `corroboration: UNAVAILABLE`, the fail-open `${_census_end:-$_census_start}`, the permissive verdict
      scan, and the measurement failures). The instructive part is the **LEVEL-SHIFT**: §8k's remedy fixed the
      **fold** (order-independence, by construction) and left the **leaf** two-valued, so it proved the right
      property **ONE LEVEL TOO HIGH** — an order-independent fold over a predicate that has already discarded
      the distinction cannot recover it, which is why a fourth point patch on the fold would not have ended
      the series. Recorded in `design.md`, the delta spec and the main spec.
- [x] **The defect**: `record=$(git ls-tree …) || return 1` gave a FAILED lookup the SAME value as a measured
      non-executable. Reproduced with controls — valid repo ⇒ CODE; `REPO` not a git repo (every `ls-tree`
      fails) ⇒ **NON-CODE for a genuinely executable file**; bogus `BASE_SHA` + valid HEAD ⇒ CODE (so the
      monotone OR did bound the blast radius to the both-endpoints-unmeasurable case).
- [x] **The leaf is now tri-valued** (`_roborev_mode_exec_state_at`, exit status = state): 0 EXEC, 1 NOT-EXEC,
      2 UNMEASURABLE. The critical distinction is INSIDE the failure handling — `ls-tree` **succeeded with no
      record** is a REAL MEASUREMENT of absence (the added/deleted case, §8k's endpoint matrix) and stays
      NOT-EXEC, while `ls-tree` **failed** (not a repo, unresolvable ref, corrupt object) is UNMEASURABLE.
      git's own message is captured rather than discarded, so the condition is actionable.
- [x] **The lattice**: `NOT-EXEC < UNMEASURABLE < EXEC`, joined by MAXIMUM. Total order ⇒ associative,
      commutative, idempotent ⇒ order-independence is a property of the LATTICE, keeping §8k's guarantee
      intact one level down. EXEC dominates UNMEASURABLE **soundly** (a disjunction settled by positive
      evidence cannot be un-satisfied by another endpoint); UNMEASURABLE dominates NOT-EXEC ("exec at NEITHER"
      is a claim about EVERY endpoint); the accumulator STARTS at UNMEASURABLE so an endpoint set that yielded
      nothing cannot answer "prose". NOT-EXEC — the only permissive state — is now reachable only from a
      positive measurement at EVERY endpoint. The fold keeps §8k's shape: complete endpoint list up front, no
      `return`/`break`/`continue` in the body, monotone accumulators, single post-loop `return` (`cx3j` green).
- [x] **Unmeasurable FAILS CLOSED on `census-check:`** pre-enqueue, naming the path, the endpoint ref(s) and
      git's message, worded so "could not check" cannot read as "nothing was wrong" — the same discipline the
      unresolvable base and the failed `git diff` already carry. Never spent as a non-code classification, and
      never as `code-free:`/NOTHING-TO-REVIEW (an infra fault is not a docs-only diff).
- [x] **Both functions RENAMED** (`_roborev_mode_is_exec_at` → `_roborev_mode_exec_state_at`,
      `roborev_path_is_executable` → `roborev_path_exec_state`), because `if <boolean-call>` collapses states 1
      and 2 back into "false": a surviving boolean call site now breaks as a "command not found" instead of
      answering permissively. The `cx3h` probe was likewise widened to THREE outcome words — a boolean probe
      over a tri-valued function would have printed the defect as the expected answer.
- [x] Tests `cx3k`/`cx3k-mut`/`cx3l`: the leaf probed directly through the real oracles file (valid+exec ⇒
      CODE; valid+prose ⇒ NON-CODE, the minimality control; `ls-tree`-succeeded-with-no-record ⇒
      measured-absent, classification unchanged; both endpoints unmeasurable for a genuinely executable path
      ⇒ UNMEASURABLE carrying git's message; ONE endpoint unmeasurable pinned in BOTH sub-directions — exec
      ⇒ CODE, non-exec ⇒ UNMEASURABLE — so neither a fail-open nor a fail-closed-on-everything
      implementation satisfies the pair); the two-valued leaf restored verbatim as a mutant, flipping every
      unmeasurable row to NON-CODE, with a not-uniformly-broken control and a restored-green re-measure; and
      the end-to-end consequence through the wrapper's summary block, fault-injecting a failing `git ls-tree`
      with a PATH shim (its ONLY caller is the leaf, so nothing else in the run is perturbed) with the
      no-shim control run FIRST and shown to reach PASS + enqueue. Whole-suite mutation: leaf reverted to
      two-valued ⇒ **16 RED**; unmutated ⇒ **581/581**. Assertion count 551 → 581. The `49/49` docs/
      executable measurement was re-run on the final tree and still discriminates (**46/49** with the
      executable bit ignored).
- [ ] **DEFERRED to #3260 (item: the OTHER direction of the same mirror)**: `txt rst adoc mdx markdown` sit
      in `CODE_FREE_EXTENSIONS` while `.roborev.toml` excludes only `*.md` globally (plus `docs/**/*.txt`
      inside artifact dirs), so a `.rst`/`.txt`-only diff is called code-free although roborev would deliver
      it — an over-strict FALSE-FAIL. NOT fixed here, and deliberately not on the census side: removing those
      extensions would let a prose-only change reach `RESULT: PASS` and record "roborev clean", violating
      CLAUDE.md rule 4 — trading a conservative false-FAIL for a doctrine-breaking false-PASS. The sound fix
      is on the CONFIG side (add the patterns so the two halves agree), which is out of this round's scope.

## 9. Certification
- [ ] `--lite` green each fix round (summary-file redirect) — DONE, see the PR — then `rust-reviewer` +
      `roborev` on the lite-green diff (the diff contains code — shell + config — so it IS
      roborev-certifiable and MUST be).
- [ ] Open the PR; hand the endgame to `flow-closer`: the ONE full `scripts/agent-gate.sh` run of record
      (`AGENT-GATE SUMMARY`, `RESULT: PASS`, `tree-integrity:` verified) → `spec-auditor` C intent audit
      against these specs (**four** ACs: 1, 5, 6, 7 — AC3/AC4 are deferred, see §8j) → final roborev pass →
      `gh pr merge --auto --squash --delete-branch` after `scripts/flow/premerge-assert.sh`.
- [ ] After merge: verify the published doctrine page by served content (grep for a distinctive new
      phrase, never an HTTP 200 — the CDN can serve the previous page for ~3 minutes), then `flow-finalize`
      (archive this change, stamp delivery telemetry via a telemetry worktree PR).
- [ ] After merge: set #3229 to **`In Review`, NOT `Done`** — the AC2 demonstration is still outstanding and
      `Done` would auto-close the issue and lose the obligation. Flip to `Done` only once the evidence is
      posted (see §7).
- [ ] Confirm the two deferral issues exist and are linked from the PR: **#3283** (pre-enqueue exclusion-set
      guard — AC3 + AC4, inheriting §1's research record and §8j's differential-testing constraint) and
      **#3278** (roborev's compiled-in built-in deny-list).
