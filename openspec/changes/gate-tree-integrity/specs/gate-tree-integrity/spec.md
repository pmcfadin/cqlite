# gate-tree-integrity — a gate run whose worktree mutates mid-run SHALL NOT certify

## ADDED Requirements

### Requirement: The gate SHALL capture a tree identity at start and re-verify it before emitting a verdict

`scripts/agent-gate.sh` SHALL capture a **tree identity** — the HEAD commit SHA, the dirty flag, and
a content-sensitive digest of the working tree — once at gate start (before any component runs), at
every component boundary (`record_result`), and once immediately before the terminal summary emit.
When the start identity and the identity observed at any later capture differ, the run SHALL NOT
emit `RESULT: PASS`: it SHALL emit `RESULT: FAIL` with a named `tree-integrity: FAIL
(tree-mutated-midrun; …)` line and exit non-zero. The named line SHALL report the start and end HEAD
SHAs and SHALL name the paths whose content changed (derived from the retained start/end manifests),
truncated with an explicit count when numerous.

#### Scenario: an unmutated full run certifies normally
- **WHEN** a gate runs to completion and no path inside the digest scope changes
- **THEN** the SUMMARY SHALL carry `tree-integrity: PASS` with equal `tree-start:`/`tree-end:` digests
- **AND** the run's `RESULT` SHALL be decided by its components exactly as before this change

#### Scenario: a commit landing mid-run prevents RESULT: PASS
- **WHEN** a new commit lands in the gate's worktree after the start capture and every component
  otherwise passes
- **THEN** the SUMMARY SHALL carry `RESULT: FAIL` and a `tree-integrity: FAIL (tree-mutated-midrun; …)` line
- **AND** the exit status SHALL be non-zero

#### Scenario: the failure line names the changed paths and both HEAD SHAs
- **WHEN** a mid-run mutation is detected
- **THEN** the named line SHALL report the start HEAD SHA, the end HEAD SHA, and the paths whose
  manifest entries differ (with an explicit count when the list is truncated)

#### Scenario: a mutation landing after the last component boundary is still caught at terminal emit
- **WHEN** the tree is mutated after the final `record_result` call but before the summary is written
- **THEN** the terminal capture SHALL detect the mismatch and the emitted block SHALL be a named
  `tree-integrity: FAIL` with `RESULT: FAIL`

### Requirement: The digest SHALL detect a content change whose porcelain status line is unchanged

The tree-identity digest SHALL be computed over a per-path manifest that records the **content hash
of each path's current bytes**, never over `git status --porcelain` output alone. The manifest SHALL
cover (a) every path that differs from HEAD in the index or the working tree, hashed from its
working-tree bytes (recording deletions and file-mode changes), and (b) every untracked,
non-ignored path, hashed the same way. Path ordering SHALL be deterministic and the manifest SHALL
be NUL-framed so that a path containing a newline cannot forge a manifest line.

#### Scenario: appending to an already-modified tracked file is detected
- **GIVEN** a tracked file that is already modified relative to HEAD, so `git status --porcelain`
  lists it as ` M <path>`
- **WHEN** further content is appended to that same file, leaving the porcelain listing byte-identical
- **THEN** the tree-identity digest SHALL differ from the digest captured before the append

#### Scenario: a file-mode change with unchanged content is detected
- **WHEN** a tracked file's executable bit is flipped without changing its bytes
- **THEN** the manifest entry for that path SHALL change and the digest SHALL differ

#### Scenario: deleting a tracked file mid-run is detected
- **WHEN** a tracked file is removed from the working tree
- **THEN** the manifest SHALL record that path as deleted and the digest SHALL differ

#### Scenario: the untracked-file lifecycle is tracked as a CONTENT identity
- **WHEN** an untracked, non-ignored file is created, then appended to, then deleted
- **THEN** the digest after the creation SHALL differ from the baseline digest, and the digest after
  the append SHALL differ from both
- **AND** the digest after the deletion SHALL RETURN TO the baseline digest — the identity is a
  content identity, not a counter, and removing the file genuinely restores the earlier tree
- **AND** removing an untracked file that was PRESENT at the baseline capture SHALL change the
  digest, which is the property that makes an untracked-file deletion mid-run detectable

#### Scenario: a capture that cannot be validated fails closed instead of comparing equal
- **GIVEN** the hashing tool exits non-zero, or the manifest write fails or is truncated, so the
  capture produces no usable digest
- **WHEN** the gate reaches any boundary or the terminal emit
- **THEN** the run SHALL NOT emit `RESULT: PASS`: it SHALL emit `RESULT: FAIL` with a named
  `tree-integrity: FAIL` line stating the tree cannot be proven unchanged, and exit non-zero
- **AND** this SHALL hold whether or not the tree was actually mutated, and SHALL NOT be reported as
  the "no git worktree" SKIP

#### Scenario: the comparison covers the whole identity, not the digest alone
- **WHEN** a later capture's HEAD sha, dirty flag or digest differs from the start capture's
- **THEN** the identities SHALL be treated as different — a block SHALL never stamp
  `tree-integrity: PASS` while its own `tree-start:`/`tree-end:` lines disagree

#### Scenario: an unchanged tree hashes identically on two consecutive captures
- **WHEN** the tree identity is captured twice with no intervening change inside the digest scope
- **THEN** both captures SHALL produce byte-identical manifests and an identical digest

### Requirement: Capturing the tree identity SHALL NOT perturb the repository

Every git invocation used for capture SHALL pass `--no-optional-locks`, and content hashing SHALL
use `git hash-object` **without** `-w`. The capture SHALL NOT write to the index, the working tree,
or the object database, and SHALL NOT create a temporary index. The only files it may create are
the start/end manifests inside the run's own `mktemp` log directory. Capture SHALL be safe to call
concurrently from the backgrounded SIDE-lane subshells.

#### Scenario: index and working tree are byte-identical across a capture
- **WHEN** a tree-identity capture runs against a dirty checkout
- **THEN** the git index file content and `git status --porcelain` output SHALL be unchanged
  afterwards, and no working-tree file SHALL be created, modified or removed

#### Scenario: no object-database write occurs during capture
- **WHEN** a capture hashes modified tracked files and untracked files
- **THEN** the repository's object database SHALL gain no new objects (hashes are computed, not stored)

#### Scenario: concurrent captures from parallel component lanes do not race the index
- **WHEN** several backgrounded SIDE-lane components reach their boundaries at the same time and each
  captures the tree identity
- **THEN** every capture SHALL succeed and none SHALL write or refresh the index

### Requirement: Every SUMMARY block SHALL carry start and end tree provenance

The SUMMARY block SHALL carry a `tree-start:` line (HEAD short-sha, dirty flag, digest prefix) and a
`tree-end:` line of the same shape, plus a `tree-integrity:` verdict line, in the full gate,
`--lite`, `--delta` and `--only` modes. The startup `INCOMPLETE` sentinel SHALL carry the
`tree-start:` line (and no `tree-end:` line), so a killed run still records the tree it began on.
The synthetic emission modes (`--emit-summary-selftest`, the `--lite` aggregation self-test) SHALL
stamp a `selftest` identity so the block shape is uniform without a git dependency. No line added by
this capability SHALL contain the token `RESULT:`.

#### Scenario: a full-gate summary carries tree-start, tree-end and tree-integrity
- **WHEN** a full gate emits its terminal SUMMARY
- **THEN** the block SHALL contain a `tree-start:`, a `tree-end:` and a `tree-integrity:` line

#### Scenario: lite and delta summaries carry the same three lines
- **WHEN** `--lite` or `--delta` emits its distinctly-marked summary block
- **THEN** that block SHALL also contain `tree-start:`, `tree-end:` and `tree-integrity:` lines

#### Scenario: the startup INCOMPLETE sentinel carries tree-start only
- **WHEN** the startup sentinel is written before any component runs
- **THEN** it SHALL contain the `tree-start:` line and no `tree-end:` line
- **AND** its terminal line SHALL remain `RESULT: INCOMPLETE (gate did not finish)`

#### Scenario: selftest emission modes stamp a synthetic identity
- **WHEN** `--emit-summary-selftest` or the lite aggregation self-test emits a representative block
- **THEN** its `tree-start:`/`tree-end:` lines SHALL be the synthetic `selftest` form, consistent with
  the existing synthetic `commit: selftest` stamp, and SHALL require no git state

#### Scenario: the RESULT poll predicates are unaffected by the new lines
- **WHEN** a caller polls a summary file with `grep -q 'RESULT:'` or `grep -qE 'RESULT: (PASS|FAIL)'`
- **THEN** the added lines SHALL NOT match either predicate, so issue #2908's separate concern is
  neither fixed nor regressed by this change

### Requirement: A detected mutation SHALL be a verdict, never an INCOMPLETE and never a lost SIDE-lane signal

`tree-mutated-midrun` SHALL surface as `RESULT: FAIL`, never as `RESULT: INCOMPLETE` (which remains
a liveness placeholder, not a verdict, per issue #2908). A mutation detected on the MAIN foreground
lane SHALL stop the run at that boundary with the named line intact. A mutation detected inside a
backgrounded SIDE-lane subshell SHALL NOT emit a summary or exit from the subshell; it SHALL append
to a marker file that a post-drain check converts into `OVERALL=FAIL` plus the named terminal line,
so the detection can never be lost to a false-green. When both the existing `summary-integrity`
guard and this guard fire, both named lines SHALL appear and the result SHALL be a single `FAIL`.

#### Scenario: MAIN-lane detection stops the run with a named FAIL
- **WHEN** a foreground component's boundary capture detects a mutation
- **THEN** the gate SHALL stop there, emit the named `tree-integrity: FAIL` block with `RESULT: FAIL`,
  and exit non-zero without running the remaining components

#### Scenario: SIDE-lane detection survives the drain and forces a terminal FAIL
- **WHEN** a backgrounded SIDE-lane component detects the mutation
- **THEN** it SHALL record a marker instead of emitting, and after the lanes drain the terminal block
  SHALL carry the named `tree-integrity: FAIL` line with `RESULT: FAIL` and a non-zero exit

#### Scenario: a mutated run never reports INCOMPLETE as its verdict
- **WHEN** a live gate detects a mid-run mutation
- **THEN** the emitted terminal line SHALL be `RESULT: FAIL`, never `RESULT: INCOMPLETE`

#### Scenario: summary-integrity and tree-integrity failures coexist in one block
- **WHEN** a run's summary file is clobbered by a foreign run-id AND its tree mutates
- **THEN** the emitted block SHALL carry both named lines and a single `RESULT: FAIL`

### Requirement: The guard SHALL apply to --lite, --delta and --only, and SHALL exempt only non-certifying modes

`--lite`, `--delta` and `--only` SHALL be guarded by the same capture-and-compare mechanism as the
full gate, because each derives part of its own scope from git state read mid-run (lite blast-radius
selection, delta fail-closed path classification, the `file-size` ratchet base). `--list`,
`--python-build-verify` and the concurrency-stub mode SHALL remain exempt (they exit before the run
is established and never emit a certification of a real tree).

#### Scenario: a lite run whose HEAD moves mid-run cannot report a lite PASS
- **WHEN** a commit lands during a `--lite` run
- **THEN** the LITE summary SHALL carry `tree-integrity: FAIL (tree-mutated-midrun; …)` with
  `RESULT: FAIL` and a non-zero exit

#### Scenario: a delta run whose HEAD moves mid-run cannot re-certify
- **WHEN** HEAD moves during a `--delta` run after its anchor classification
- **THEN** the DELTA summary SHALL carry the named `tree-integrity: FAIL` line with `RESULT: FAIL`,
  and SHALL NOT report a successful re-certification

#### Scenario: an --only PARTIAL run whose tree mutates fails instead of reporting PARTIAL
- **WHEN** the tree mutates during an `--only` run
- **THEN** the block SHALL carry the named `tree-integrity: FAIL` line and `RESULT: FAIL`, not `PARTIAL`

#### Scenario: the full gate's certification window begins when its slot is granted
- **GIVEN** the machine-wide concurrency cap can hold a full gate in `waiting for gate slot` for the
  length of another run, during which it has executed nothing and certifies nothing
- **WHEN** the worktree is edited while the gate is queued and then left untouched once work begins
- **THEN** the full gate SHALL (re-)capture its start identity immediately after the slot is granted
  and SHALL certify normally — the queue SHALL NOT be inside the guarded window
- **AND** `--lite` and `--delta`, which never queue and exit before that point, SHALL keep the
  capture taken before their own first component

#### Scenario: helper modes remain exempt and unchanged
- **WHEN** `--list`, `--python-build-verify` or the concurrency-stub mode is invoked
- **THEN** no tree-identity capture SHALL be required and their behaviour and output SHALL be unchanged

### Requirement: The digest SHALL exclude only paths the gate itself writes

Exclusion SHALL be driven by the repository's own ignore rules (`--exclude-standard`) plus exactly
two explicit carve-outs: (1) the run's own summary file and its `integrity-fail` siblings when they
resolve under the repository root, and (2) `Cargo.lock`, which is a **named non-fatal class** rather
than an exclusion — when the start→end manifest difference consists solely of a lockfile, the run
SHALL stamp `tree-integrity: PASS (lockfile-settled: …)` naming the before/after hashes and proceed;
when a lockfile changed alongside any other path, the run SHALL FAIL as a mutation. No broader
exclusion (for example `docs/**`, `*.md`, or `test-data/**` beyond the ignore rules) SHALL be
introduced. Ignored *inputs* such as the fetched dataset binaries are outside the digest and SHALL be
documented as a stated limitation covered by the existing `datasets:` and `ci-pins:` stamps.

#### Scenario: cargo, log and summary churn during a normal run does not trip the guard
- **WHEN** a run writes into `target/`, writes a `*.log`, and rewrites its default summary path
- **THEN** the start and end digests SHALL be equal and the run SHALL be free to certify

#### Scenario: a caller-pinned in-repo summary path does not trip the guard
- **WHEN** `AGENT_GATE_SUMMARY_FILE` resolves to a non-ignored path under the repository root
- **THEN** that path and its `integrity-fail` siblings SHALL be excluded from the digest, and no other
  untracked path SHALL be excluded on their account

#### Scenario: the carve-out matches however the in-repo path is spelled
- **GIVEN** git reports only normalized repo-root-relative paths
- **WHEN** the caller pins the summary path in a non-canonical form — relative, `./…`, containing
  `..`, or absolute — that still resolves under the repository root
- **THEN** the path SHALL be canonicalized before comparison so the carve-out matches, because the
  gate creates that file only AFTER the start capture and a missed carve-out is a guaranteed false FAIL
- **AND** a path resolving outside the repository root SHALL simply not be excluded

#### Scenario: a tab inside a changed path cannot corrupt the lockfile classification
- **WHEN** a changed path contains a TAB character (for example an untracked file whose name begins
  `Cargo.lock<TAB>`)
- **THEN** the failure report SHALL name the whole path and the non-fatal lockfile class SHALL NOT
  fire on a fragment of it — such a change SHALL be a normal fatal mutation

#### Scenario: every settled lockfile is named in the stamp
- **WHEN** more than one lockfile is re-resolved by the gate's own cargo invocations and nothing else differs
- **THEN** the `lockfile-settled` stamp SHALL name every changed lockfile with its before/after hash

#### Scenario: a lockfile settled by the gate's own cargo resolution is stamped, not fatal
- **WHEN** the only difference between the start and end manifests is `Cargo.lock`
- **THEN** the SUMMARY SHALL carry `tree-integrity: PASS (lockfile-settled: …)` naming the before and
  after hashes, and the run SHALL be free to certify

#### Scenario: a lockfile change accompanied by any other change is fatal
- **WHEN** `Cargo.lock` and at least one other in-scope path differ between the start and end manifests
- **THEN** the run SHALL FAIL with the named `tree-mutated-midrun` line listing all changed paths

#### Scenario: a mid-run edit to a docs or test-data file still trips the guard
- **WHEN** a tracked file under `docs/` or `test-data/` is edited mid-run
- **THEN** the digest SHALL differ and the run SHALL FAIL — such paths SHALL NOT be excluded

### Requirement: The guard SHALL have no bypass

No environment variable, flag, or configuration SHALL convert a detected mid-run mutation into
`RESULT: PASS`. The only supported knob SHALL be a per-file content-hash cap for untracked files
(`AGENT_GATE_TREE_HASH_CAP_BYTES`, default 8 MiB) which weakens detection for a single oversized
untracked blob only, never suppresses a detected mutation, and SHALL be stamped in the SUMMARY
whenever it is set to a non-default value or whenever the size+mtime fallback is used.

#### Scenario: no environment variable turns a mutated run green
- **WHEN** a mutated run is executed with any combination of the gate's documented environment
  variables set
- **THEN** the run SHALL still emit the named `tree-integrity: FAIL` line with `RESULT: FAIL`

#### Scenario: the hash cap is stamped when non-default or when the fallback is used
- **WHEN** `AGENT_GATE_TREE_HASH_CAP_BYTES` is set to a non-default value, or any untracked file
  exceeds the cap and is recorded by size and mtime
- **THEN** the SUMMARY SHALL carry a `tree-hash-cap:` line naming the cap and the number of files
  recorded by the fallback

#### Scenario: the hash cap cannot suppress a detected mutation
- **WHEN** the cap is set arbitrarily low and a tracked file is edited mid-run
- **THEN** the mutation SHALL still be detected and the run SHALL FAIL (the cap applies only to
  untracked-file content hashing)

### Requirement: A discriminating regression test SHALL pin the behaviour inside tooling-tests

`scripts/tests/test_agent_gate_tree_integrity.sh` SHALL be executed by the `tooling-tests` component
and SHALL contain both a mutated case (the gate does not certify) and an unmutated control case (the
gate certifies), so that removing the guard fails the first and hardwiring it to FAIL fails the
second. It SHALL cover the porcelain-identical content-change case explicitly. All fixtures SHALL be
per-run `mktemp` namespaces with terminal `XXXXXX` templates and traps that remove only paths the
run created. Assertions SHALL be on captured identity values (digests, SHAs, manifest contents,
named summary lines) and SHALL NOT assert on wall-clock durations; mid-run mutation SHALL be
sequenced by a rendezvous on the gate's own per-component result artifacts rather than by a fixed
sleep.

#### Scenario: the mutated case asserts the gate does not certify
- **WHEN** the self-test mutates the gate's worktree mid-run
- **THEN** it SHALL assert the emitted block contains the named `tree-integrity: FAIL` line, contains
  no `RESULT: PASS`, and that the gate exited non-zero

#### Scenario: the unmutated control case asserts the gate still certifies
- **WHEN** the identical harness runs with no mutation
- **THEN** it SHALL assert `tree-integrity: PASS` and a certifying `RESULT`, proving the guard is not
  hardwired to fail

#### Scenario: the porcelain-identical content change case is covered
- **WHEN** the self-test appends to an already-modified tracked file mid-run
- **THEN** it SHALL assert both that `git status --porcelain` output is unchanged and that the run
  did not certify

#### Scenario: the mutating self-test hooks refuse a live checkout
- **GIVEN** the hooks append to — and optionally commit into — `$REPO_ROOT`
- **WHEN** they are invoked against a checkout that does not carry the disposable-fixture marker file
- **THEN** the gate SHALL refuse with a non-zero exit before writing anything, leaving the working
  tree and HEAD untouched
- **AND** with the marker present the same invocation SHALL run and still fail closed on the mutation

#### Scenario: the self-test is hermetic and leaves no shared fixtures
- **WHEN** two instances of the self-test run concurrently in one checkout
- **THEN** both SHALL pass and neither SHALL remove or overwrite the other's fixtures

#### Scenario: no assertion depends on wall-clock duration
- **WHEN** the self-test sequences its mid-run mutation
- **THEN** it SHALL rendezvous on the gate's per-component result artifacts, and its assertions SHALL
  reference captured identities only — no elapsed-time threshold SHALL appear in the correctness path

### Requirement: Doctrine SHALL state that a mid-run tree mutation invalidates the run

`CLAUDE.md`'s gate section, the `agents-developing/gate-contract` page, and
`docs/development/gate-ops.md` SHALL state, in the same change, that a run whose worktree mutates
mid-run FAILs with `tree-mutated-midrun` and cannot be pasted as a certification, and that a closer
MUST read `tree-integrity:` alongside `RESULT:` before trusting a summary. The gate-contract page's
machine-checkable block renderings SHALL show the new lines.

#### Scenario: the gate-contract page documents the new lines and the closer's check
- **WHEN** a reader consults the gate-contract page's machine-checkable summary block
- **THEN** the rendering SHALL include `tree-start:`, `tree-end:` and `tree-integrity:`, and the page
  SHALL state that a mid-run tree mutation invalidates the run

#### Scenario: CLAUDE.md states the invalidation rule in the gate section
- **WHEN** an agent reads CLAUDE.md's gate section
- **THEN** it SHALL find the statement that a mutated-mid-run gate FAILs with `tree-mutated-midrun`
  and that closers verify `tree-integrity:` alongside `RESULT:`

#### Scenario: gate-ops documents recovery from a mutated run
- **WHEN** an operator hits a `tree-mutated-midrun` FAIL
- **THEN** `docs/development/gate-ops.md` SHALL describe the closer+fixer overlap shape and the
  remedy (re-run on a stable tree; the FAIL names the changed paths)
