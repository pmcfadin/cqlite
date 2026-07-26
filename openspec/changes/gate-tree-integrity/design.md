# Design — gate-tree-integrity (issue #2926)

## 1. What is actually broken (code-verified)

| Claim | Evidence in `scripts/agent-gate.sh` @ `c76f4ef` |
|---|---|
| `commit:` is stamped at summary-write time | :4888 (full), :3703 (`--lite`), :3923 (`--delta`) |
| Nothing captures tree state at start | the ONLY other `git rev-parse` uses are :3818 (`--delta` anchor), :4926–4927 (push signal), :3256/:3356 (base-ref probes) |
| The `dirty:` flag is `git status --porcelain` at the same late moment | same three lines |
| Components derive scope from git **mid-run** | `file-size` :3266–3268, `--lite` blast radius :3365–3366, `--delta` classification :3881–3882 |
| There is already a component-boundary chokepoint to hook | `record_result` :2251–2256 calls `_assert_summary_integrity` |
| There is already a lane-aware (MAIN vs SIDE) fail-closed pattern to mirror | `_assert_summary_integrity` :1985, `_apply_integrity_marker` :2051, `_emit_terminal_summary` :2077 |

The #2874 `summary-integrity` machinery is the exact structural precedent: a per-run identity
(`run-id`), a boundary check, a lane-aware marker for backgrounded components, and a terminal
re-check. This change adds a second identity — of the **tree** rather than of the **summary file** —
through the same three hook points. That is the cheapest correct shape and it keeps one mental model.

## 2. Remedy choice: fail-closed identity stamp (not snapshot, not lock)

The issue offered three remedies.

- **Snapshot / gate a detached copy.** Strongest isolation, but: the gate compiles into a shared
  `target/` whose paths are baked into artifacts, the datasets root and `REPO_ROOT`-relative
  component paths (`docs/reports/…`, `test-data/…`) assume the real checkout, and a copy of a
  built repo is tens of GB. It also *hides* the collision instead of reporting it — the fixer's
  commit still lands in a worktree whose gate result no longer describes it. Rejected as this
  change's remedy.
- **Advisory worktree lock.** Prevents the second writer, but only if the second writer takes the
  lock. The realistic second writer is a human or an agent running `git commit`, which will never
  consult a gate lock. It also converts a rare hazard into a routine hard block on a normal pipeline
  shape (closer + fixer). Rejected.
- **Fail-closed identity stamp (chosen).** Makes the corruption *self-evident in the artifact* —
  precisely the property whose absence caused the incident. It costs ~35 ms per run, cannot be
  defeated by an uncooperative writer, and composes with the existing #2874 mechanism. It does not
  prevent the collision; the doctrine update (§7) and #1930 cover prevention.

## 3. The digest — chosen mechanism, and why the naive one fails

### 3.1 The trap the guard must not fall into

`git status --porcelain` alone is **provably blind** to the dominant case. Measured in this checkout:

```
append "A\n" to README.md      porcelain sha256 = c8094453…   diff-based digest = 2ca89bd8…
append "B\n" to README.md      porcelain sha256 = c8094453…   diff-based digest = 47f1c403…
                                                ^^ IDENTICAL              ^^ CHANGED
```

The porcelain listing is ` M README.md` in both states. Editing a file that is *already* modified —
the single most likely shape of a mid-run review fix — leaves the porcelain byte-identical. A guard
built on it would be another can't-fail guard.

### 3.2 Chosen mechanism: a per-path content manifest, digested

Capture is one function, `_tree_identity <out-manifest>`, producing a NUL-framed manifest and its
`sha256`:

```
H <full-head-sha-or-"unborn">
T <path> <blob-sha | DELETED> <mode>      # one line per uncommitted tracked change
U <path> <blob-sha | SIZE:<n>:MTIME:<ns>> # one line per untracked, non-ignored file
```

- **Tracked side**: `git --no-optional-locks diff --name-only -z HEAD --` enumerates every path that
  differs from HEAD in the index *or* the working tree (content, mode, add, delete). Each surviving
  path's **working-tree bytes** are hashed with `git hash-object --stdin-paths` (a single batched
  process, **no `-w`** so nothing is written to the object database); paths that no longer exist are
  recorded `DELETED`.
- **Untracked side**: `git --no-optional-locks ls-files --others --exclude-standard -z`, hashed the
  same way. `--exclude-standard` is what makes `.gitignore` the exclusion set (§5).
- Both lists are ordered with `LC_ALL=C sort -z`; the manifest is fully NUL-framed so a path
  containing a newline cannot forge a line.
- `digest = sha256(manifest)`; the manifest itself is retained at `$LOG_DIR/tree-identity.{start,end}`
  so a mismatch can **name the paths that changed** (`comm`/`diff` of the two manifests) instead of
  reporting an opaque hash difference.

**Why this catches the identical-porcelain case**: the manifest carries the *content hash of the
file's current bytes*, not its status letter. Appending to an already-modified file changes the
`T <path> <blob-sha>` line while `git status` output is unchanged. Verified empirically above and,
for the untracked side, by adding then appending to a new untracked file (three distinct digests:
absent → added → content changed).

**Why the digest only has to be stable *within one run*.** Both captures happen in one process
invocation on one machine with one git binary and one config. No cross-run, cross-machine or
cross-git-version stability is required, which removes every portability objection to using git's
own plumbing output as the input to the hash.

### 3.3 Alternatives considered and rejected

| Candidate | Verdict |
|---|---|
| `git status --porcelain` (± `--untracked-files=all`) | **Rejected** — provably blind to §3.1 (measured). |
| Hash of `git diff --binary HEAD` patch text | Close second; equally content-sensitive and marginally cheaper (~4 ms), but an opaque patch blob cannot name the changed paths on failure, and it says nothing about untracked files. Its per-path successor (§3.2) is strictly more useful for the same order of cost. |
| `git ls-files -s` (+ index blob shas) | **Rejected** — reflects the **index**, so an unstaged working-tree edit is invisible. Making it correct requires hashing all 4,346 tracked files. |
| Temporary index + `git add -A` + `git write-tree` | **Rejected** — correct, but **228 ms** measured per capture (~13× §3.2) *and* it writes blob/tree objects into the object database, which git worktrees **share with the root checkout**. A guard that mutates the user's repo to check that the user's repo did not mutate is the wrong trade, even though index/worktree are untouched. |
| `find -newer` / mtime sweep | **Rejected** — build tools touch mtimes constantly; false positives, and it is a wall-clock-shaped signal (#2642 territory). |

### 3.4 Cost (measured, this checkout: 4,346 tracked files, warm page cache)

| Operation | Time |
|---|---|
| `git diff --binary HEAD \| sha256sum` (clean tree) | 4 ms |
| Full `_tree_identity` shape (HEAD + tracked names + hash-object batch + untracked enumeration) | **17 ms** |
| Temp-index `git add -A` + `git write-tree` (rejected alternative) | 228 ms |

Two captures (start + terminal) ≈ **35 ms**. With a capture at every `record_result` boundary
(~30 components) ≈ **0.5 s** on a 40–60 minute full gate — under 0.03 %. On a 1–5 minute `--lite`
run, ≈ 0.1 s. Cost is dominated by lstat-ing tracked files; it grows with the size of the
*uncommitted* diff (usually small), not with repository or `target/` size.

### 3.5 Side-effect freedom

Every git invocation is prefixed `git --no-optional-locks`. Without it, `git diff`/`git status` may
refresh and **rewrite `$GIT_DIR/index`** — which would (a) perturb the user's index, violating the
stated constraint, and (b) race the ~8 concurrent SIDE-lane subshells that will each call the
capture at their own component boundary. `git hash-object` is invoked **without `-w`**: hashes are
computed, nothing is written. No temporary index, no `git add`, no object-database write, no
working-tree write. The only files the guard creates are the two manifests under `$LOG_DIR`
(already a per-run `mktemp -d`, outside the repo).

## 4. Mode coverage: full, `--lite`, `--delta`, `--only` — all guarded

**Recommendation: guard all four.** Reasoning per mode:

- **Full gate** — the gate of record; the incident's mode. Non-negotiable.
- **`--lite`** — *more* scope-sensitive than the full gate, not less: `run_scoped_tests` derives its
  blast radius from `git diff --name-only "$base"...HEAD` **and** `git diff --name-only HEAD` at
  :3365–3366, i.e. mid-run. A commit landing between summary-path resolution and that call changes
  which packages and which `--test` targets run. And `--lite` is the mode that runs *during* the
  fix rounds, i.e. in the exact window where a second agent is editing. A ~0.1 s cost on a 1–5 min
  run is not a reason to leave the hole open.
- **`--delta`** — its entire premise is "`anchor..HEAD` is executable-tests/docs only", classified
  from `git diff --name-only --no-renames "$anchor_sha" HEAD` at :3881. If HEAD moves mid-run, the
  set that was classified is not the set that was executed, and the fail-closed classification is
  void. `--delta` is also the mode most likely to be running while someone keeps polishing.
- **`--only` (PARTIAL)** — shares the full-gate code path; guarding it is free and a PARTIAL block
  is still pasted into PRs.

**Exempt** (they never emit a certification of a real tree, and several exit before `LOG_DIR` is
created at :1620): `--list` (:1474), `--python-build-verify` (:1569), `--emit-summary-selftest`
(:2130 already stamps the synthetic `commit: selftest branch: selftest dirty: no`), the `--lite`
aggregation self-test (:4225, same synthetic stamp) and the concurrency-stub mode (:4200). These
stamp `tree-start: selftest` / `tree-end: selftest` so the block shape stays uniform and
`test_agent_gate_summary.sh` can assert on the lines' presence without a git dependency.

## 5. Exclusions — `.gitignore` does almost all of it

The exclusion set is `--exclude-standard` (the repo's own ignore rules) plus a two-item explicit
list. Audited coverage of everything the gate itself writes into the checkout:

| Gate-written path | Already excluded by |
|---|---|
| `target/**` (all cargo/nextest/criterion/profiling output, incl. `target/profiling/history.jsonl`) | `.gitignore: target/` |
| `gate.log` and any caller redirect log | `.gitignore: *.log` |
| `.agent-gate-summary.txt`, `-lite-`, `-delta-`, and `*.integrity-fail.*` siblings | `.gitignore` root-scoped rules |
| `test-data/.tmp-parity-manifest-mutated*` (parity-report self-test fixture) | `.gitignore` explicit rule |
| `test-data/scripts/smoke-test-all-tables-results/`, `test-data/output/` | `.gitignore` |
| `bindings/python/{target,build,dist,*.egg-info,__pycache__,.venv}`, `pytest_output.txt`, `*.so` | root + `bindings/python/.gitignore` |
| `bindings/node/{target,node_modules,*.node,index.js,index.d.ts}` | root + `bindings/node/.gitignore` |
| `trino-connector/{build,.gradle}`, `compaction-parity/{build,.gradle}` | their `.gitignore`s |
| fetched dataset binaries (`*.db`, `test-data/datasets/.dataset-pin`) | `.gitignore` |
| `$LOG_DIR` (`/tmp/agent-gate.XXXXXX`), sccache dir, `CARGO_HOME` | outside `$REPO_ROOT` |

Explicit additions (deliberately tiny — an over-broad exclusion re-opens the hole):

1. **`$SUMMARY_FILE` and `$SUMMARY_FILE.integrity-fail.*`, only when they resolve under
   `$REPO_ROOT`.** The three default paths are already gitignored, but a caller may pin a *relative*
   `AGENT_GATE_SUMMARY_FILE` (resolved against `$INVOCATION_CWD` at :1678–1681), landing an
   untracked, non-ignored file in the repo that the gate writes twice by contract. Scope: exactly
   the paths this run declares it will write — not a glob.
2. **`Cargo.lock` is a named non-fatal class, not an exclusion.** The gate runs cargo **without
   `--locked`/`--frozen` (verified: neither flag appears anywhere in the script)**, so the first
   cargo component may legitimately re-resolve a stale lockfile — a tracked-file mutation caused by
   the gate itself. Making that a hard FAIL would break real runs. Making it invisible would hide a
   dependency-version change from the certification. Chosen middle: if the start→end manifest diff
   contains **only** `Cargo.lock` (or a nested `*/Cargo.lock`), the run stamps
   `tree-integrity: PASS (lockfile-settled: Cargo.lock <sha-before>→<sha-after>)` and proceeds; if
   `Cargo.lock` changed **alongside** anything else, the whole run FAILs as a mutation. Both digests
   are stamped either way, so the residual hole ("someone hand-edits only `Cargo.lock` mid-run") is
   visible in the artifact rather than silent. **Follow-up (separate issue): add `--locked` to the
   gate's cargo invocations and delete this carve-out** — that change alters failure modes across
   every component and does not belong here.

Explicitly **NOT** excluded, and why: `docs/**` and `*.md` (a docs edit mid-run is a real mutation,
and `--delta` re-certifies exactly those files), `test-data/**` beyond the ignore rules (a fixture
swap mid-run is the nastiest possible mutation), `openspec/**`, `scripts/**`, `.github/**`.

**Residual limitation to state in the doctrine**: gitignored *inputs* — chiefly the fetched
`test-data/datasets/**` SSTable binaries — are outside the digest. Their stability is covered by the
existing `datasets: N Data.db files` stamp and the `ci-pins:` line, not by this guard.

## 6. Escape hatch — recommendation: **none**

Argued **against**, and the codebase supplies the argument:

- The guard's whole value is that a certification artifact cannot lie. An env var that converts a
  known-lying artifact back into `RESULT: PASS` reinstates the exact failure mode #2926 exists to
  close. A certification guard with a bypass is worth approximately what the bypass costs to type.
- The precedent named in the issue is a **warning, not a model**: `CQLITE_ALLOW_FILE_GROWTH` is an
  opt-out that is **never stamped into the SUMMARY** — it prints only into the `file-size`
  component's stdout (:3301–3303) and `SUMMARY_META` never learns about it. (Contrast
  `AGENT_GATE_ALLOW_MISSING_FIXTURES`, which *is* stamped via `MISSING_FIXTURES_MARKER` at
  :1187/:4896 — that is the shape any future opt-out must copy.)
- The legitimate need behind an escape hatch is "my mid-run edit was irrelevant to the components
  that already ran". That is not knowable — component→file attribution does not exist — and the
  remedy is already the cheapest sound one: re-run on a stable tree. A mixed-tree run has to be
  re-run to mean anything regardless of what an env var says.
- Failing closed here cannot deadlock delivery: the mutation is under the operator's control, and
  the FAIL names the offending paths.

One knob **is** specified, and it is a performance knob rather than a bypass:
`AGENT_GATE_TREE_HASH_CAP_BYTES` (default 8 MiB) caps per-file content hashing of *untracked* files;
above the cap the manifest records `SIZE:<n>:MTIME:<ns>` instead of a blob sha. It can never turn a
detected mutation green — only weaken detection for a single oversized untracked blob — and any
non-default value, plus any use of the fallback, is stamped in the SUMMARY
(`tree-hash-cap: <bytes> (<k> file(s) recorded by size+mtime)`).

## 7. Interaction with the `INCOMPLETE` placeholder (#2908)

The startup sentinel at :1760–1769 writes `RESULT: INCOMPLETE (gate did not finish)` as a liveness
marker. Composition rules:

1. `tree-mutated-midrun` is a **verdict**, never a liveness state: it emits `RESULT: FAIL` plus
   `tree-integrity: FAIL (…)`. A live gate that detects the mutation knows the outcome
   determinately, so INCOMPLETE would be strictly less informative.
2. The sentinel **gains** `tree-start: <sha> dirty: <y/n> digest: <d12>` (it does not gain a
   `tree-end:` line — there is no end yet). A gate killed mid-run therefore leaves an artifact that
   still records the tree it began on, which is precisely the forensic evidence the #2916 incident
   lacked.
3. **Do not regress #2908.** #2908's concern is that `grep -q 'RESULT:'` false-positives on the
   placeholder; the corrected predicate is `grep -qE 'RESULT: (PASS|FAIL)'`. Constraint on this
   change: **no new line may contain the token `RESULT:`** — in particular the `tree-integrity:`
   reason text must never embed it, and the mutation reason must not be phrased as
   `RESULT: mutated`. Both predicates (buggy and corrected) therefore behave exactly as they do
   today.
4. **A `.running` sentinel is the cleaner joint fix** for #2908's poll hazard — a
   `$SUMMARY_FILE.running` file created next to the summary at startup and removed at terminal emit
   makes "is the gate finished?" a file-existence question with no parsing at all, and it would
   naturally carry the `tree-start:` identity for pollers. **That belongs to #2908, not here.** This
   change is designed to compose with it: the tree lines live inside the SUMMARY block, and the
   start identity is written in exactly one place (`$LOG_DIR/tree-identity.start` + the sentinel),
   so a later `.running` file can reuse it without a second capture.

## 8. Detection points and lane-awareness

Three hooks, mirroring #2874 exactly:

1. **Start** — immediately after summary-path resolution / sentinel write (:1760–1769), before
   `run_lite` (:4243), `run_delta` (:4250) and `acquire_gate_slot` (:4257) so all guarded modes are
   covered by one capture. Stored as `$LOG_DIR/tree-identity.start` + `TREE_START_DIGEST`.
2. **Component boundary** — inside `record_result` (:2251), alongside `_assert_summary_integrity`.
   On the MAIN foreground lane a mismatch stops the run immediately with the named FAIL block (the
   ~1 h saving). Off the foreground lane (`[ "${BASHPID:-$$}" != "$$" ]`, the SIDE-lane subshells) it
   **must not** `emit_summary`/`exit`; it appends to a marker file
   (`$LOG_DIR/tree-integrity.fail`) that a post-drain `_apply_tree_integrity_marker` converts into
   `OVERALL=FAIL` + the named terminal line — the same append-safe pattern as
   `$LOG_DIR/summary-integrity.fail` (:1998–2003, :2051–2064).
3. **Terminal** — a final capture immediately before `_emit_terminal_summary` (:4912 / :3718 /
   `run_delta`'s emit), which is the authoritative check: a mutation landing after the last
   `record_result` is still caught. This is the one that can never be skipped.

Ordering with the existing guard: `summary-integrity` (who owns the artifact) is evaluated first,
then `tree-integrity` (what the artifact describes); if both fire, both lines appear and `RESULT`
is `FAIL` once.

## 9. Test design (discriminating by construction)

New `scripts/tests/test_agent_gate_tree_integrity.sh`, run by `tooling-tests`. It builds throwaway
git repos with per-run `mktemp -d …XXXXXX` (hermeticity rules from #2874) and drives the gate through
a fast path (`--only fmt` on the temp repo, and direct invocation of the capture function via the
existing self-test seam style) so it costs seconds, not an hour.

Cases, and what each discriminates:

| Case | Discriminates against |
|---|---|
| **A** mutate the tree mid-run (commit landing between two component boundaries) → no `RESULT: PASS`, named `tree-integrity: FAIL (tree-mutated-midrun …)`, non-zero exit | the bug itself |
| **B** control: identical harness, no mutation → `RESULT: PASS`, `tree-integrity: PASS` | a hardwired-FAIL "guard" |
| **C** append to an **already-modified** tracked file (porcelain listing byte-identical before/after) → detected | the naive porcelain implementation (§3.1) |
| **D** untracked file added, then its content changed, then removed → each state a distinct digest | a tracked-only digest |
| **E** churn-only run: writes under `target/`, a `*.log`, and the default summary path → digest unchanged, `RESULT: PASS` | an over-tight exclusion set that self-trips |
| **F** capture idempotence: two captures of an unchanged tree → identical digest; `$GIT_DIR/index` mtime+content and `git status` output unchanged across captures | index/worktree perturbation (§3.5) |
| **G** `--lite` and `--delta` mutated-mid-run → neither certifies | mode gaps (§4) |

A and B together are the discrimination proof: **remove the guard and A fails; hardwire the guard to
FAIL and B fails.** No test-only bypass seam is introduced to "prove" the guard can fail — such a
seam would itself be the escape hatch §6 rejects.

**No wall-clock assertions (#2642).** All assertions are on captured identities (digests, SHAs,
manifest contents, the presence of named lines) — never on elapsed time. The mid-run mutation is
sequenced by a **rendezvous on the gate's own artifacts** (wait for the first
`$LOG_DIR/<component>.result` to appear, then mutate), not by `sleep N`; the harness's outer
bounded-wait deadline is a test-harness timeout, not a correctness threshold, and is documented as
such so `scripts/tests/check-no-wallclock-asserts.sh` / the `roborev-lints` component's intent is
respected.

## 10. Doctrine updates (same change)

- `website/src/content/docs/agents-developing/gate-contract.md`: add `tree-start:`/`tree-end:`/
  `tree-integrity:` to the machine-checkable block (both the full and PARTIAL renderings at
  :357/:378), and a short "a mid-run tree mutation invalidates the run" section stating that
  `tree-integrity: PASS` is part of what a closer must read before trusting a summary.
- `CLAUDE.md` gate section: one line in the gate table's notes — a full/lite/delta run whose tree
  changes mid-run FAILs with `tree-mutated-midrun` and cannot be pasted as a certification; closers
  verify `tree-integrity:` alongside `RESULT:`.
- `docs/development/gate-ops.md`: operational note on the closer+fixer overlap shape (#1582/#1930)
  and how to recover (re-run on a stable tree; the FAIL names the changed paths).
