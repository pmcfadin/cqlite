# gate-tree-integrity Specification

## Purpose
TBD - created by archiving change gate-tree-integrity. Update Purpose after archive.
## Requirements
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

#### Scenario: a manifest truncated after the header is rejected, not compared
- **GIVEN** a manifest write is truncated part-way (for example `$TMPDIR` fills during a long run),
  leaving a file whose surviving prefix is byte-identical to another capture's prefix
- **WHEN** the capture validates its own output
- **THEN** the manifest SHALL carry a trailing record stating the number of body records, and a
  manifest whose first record is not the header, whose last record is not that trailer, or whose
  trailer count disagrees with the records actually read SHALL be REJECTED
- **AND** the two truncated captures SHALL therefore never compare equal, so a mutation to a path
  recorded after the truncation point cannot pass as `tree-integrity: PASS`

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

The block's existing `commit:` line — its short sha and its `dirty:` flag — SHALL be derived from
the VERIFIED terminal capture, never from a fresh `git rev-parse`/`git status` executed at emit
time. When no validated terminal capture exists, the line SHALL say so explicitly rather than name a
sha, and the run SHALL already be failing closed. When the block is published because a mid-run
mutation was DETECTED — by ANY detection path: a component boundary, a SIDE-lane marker consumed
after the lanes drain, or the terminal capture — `commit:` SHALL instead name the VERIFIED START
identity — the identity the run actually executed against — explicitly labelled as such, with the
post-mutation observation carried separately on an equally explicitly labelled `tree-end:` line.
This rule is a property of the DETECTION, never of the path that made it: every path SHALL apply the
identical labelling, and the labelling SHALL be implemented once so a newly added detection path
cannot publish an unlabelled post-mutation identity.

The two labels are CONTRACT TEXT, not paraphrasable intent: a triager who reads only this block must
be unable to mistake either tree for the other, so the wording is pinned here verbatim. On a
mutation-detected block, `commit:` SHALL end with exactly:

```
 (VERIFIED START — the identity this run executed against; the tree MUTATED mid-run, see tree-end: for the post-mutation observation)
```

and `tree-end:` SHALL end with exactly:

```
 (POST-MUTATION observation — NOT the identity this run executed against)
```

(The em dashes are literal U+2014.) When no validated terminal capture exists the line SHALL read
exactly `commit: unverified branch: <branch> dirty: unverified`.

Every path that emits a terminal block SHALL render its tree provenance through the shared tree
renderers, including the internal self-test hooks: no emission path SHALL hand-assemble a block
without the tree lines, and no block SHALL carry a duplicated set of them.

#### Scenario: a HEAD move between the terminal capture and the emit is never certified
- **GIVEN** the terminal capture has been taken and verified
- **WHEN** HEAD moves before the block is written
- **THEN** the emitted `commit:` SHALL name the sha the capture verified, SHALL NOT name the moved
  HEAD, and the `dirty:` flag SHALL be the capture's — no block SHALL ever certify a sha the guard
  did not verify
- **AND** when no validated terminal capture exists, `commit:` SHALL read `unverified`

#### Scenario: a mutation-detected block names the identity the run executed against
- **GIVEN** a mutation detection publishes the ONE block a triager reads after a mid-run mutation,
  and the post-mutation identity is not one this run certified anything against
- **WHEN** that block is emitted, whichever detection path produced it
- **THEN** its `commit:` line SHALL name the VERIFIED START sha and dirty flag and SHALL carry the
  `(VERIFIED START — …)` suffix pinned verbatim above
- **AND** the post-mutation identity SHALL appear only on the `tree-end:` line, carrying verbatim
  the suffix `(POST-MUTATION observation — NOT the identity this run executed against)`, so the two
  can never be read as the same thing

#### Scenario: a mutation detected by the TERMINAL capture is labelled identically
- **GIVEN** the tree mutates AFTER the last component boundary — the window that covers all of
  `--lite` and `--delta` and the tail of every full gate — so the TERMINAL capture is what detects it
- **WHEN** the terminal block is emitted
- **THEN** `commit:` SHALL name the VERIFIED START identity with the `(VERIFIED START — …)` suffix
  and SHALL NOT name the post-mutation sha, and `tree-end:` SHALL carry the post-mutation identity
  with the `(POST-MUTATION observation — …)` suffix — byte-for-byte the same two labels a boundary
  detection publishes
- **AND** the same SHALL hold for a mutation detected on a SIDE lane and applied from its marker
  after the lanes drain

#### Scenario: every emission path carries the tree lines exactly once
- **WHEN** any terminal block is emitted, including one published by an internal self-test hook or
  by the no-clobber summary-integrity publish path
- **THEN** that block SHALL carry `tree-start:`, `tree-end:` and `tree-integrity:` lines, exactly one
  of each

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
- **WHEN** a caller polls a summary file with the **known-buggy** bare-`RESULT:`-token match (it also
  matches the start-of-run `RESULT: INCOMPLETE` placeholder — the #2908/#3041 defect; do NOT copy it) or
  the **correct** `grep -qE 'RESULT: (PASS|FAIL)'` (or its `--only` counterpart, which additionally
  accepts a `PARTIAL` verdict token — #3750)
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

#### Scenario: the boundary FAIL block carries the same provenance as any other terminal block
- **GIVEN** a block published at a component boundary is the ONE block a reader reaches after a
  mid-run mutation, so it is exactly where full provenance is needed
- **WHEN** a MAIN-lane boundary detection publishes its block
- **THEN** that block SHALL carry the standard terminal provenance — `commit:`, `datasets:`
  (when the run has established it), `ci-pins:`, `accelerators:`, `cpu-budget:`, the
  `summary-integrity:` line when one is set, the tree lines, and the per-component verdict
  table for the components that recorded a result — in the terminal block's own order and row
  format, plus `detected-after-component:` and a count of how far the run got
- **AND** that table SHALL cover the component set the RUNNING MODE dispatches (`--lite` and
  `--delta` run components the full gate's set does not contain), and no recorded verdict SHALL be
  omitted from the table or from the count
- **AND** assembling it SHALL take no capture, so the component-named verdict line can never be
  overwritten by a lazily-triggered terminal capture

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

#### Scenario: a transient git failure at the slot grant does not disarm a live guard
- **GIVEN** a start capture has already succeeded, so the guard is armed
- **WHEN** the re-capture at the slot grant cannot consult git at all (a concurrent prune/gc, a
  stuttering network mount)
- **THEN** the run SHALL RETAIN the pre-queue capture and stay guarded — it SHALL NOT be downgraded
  to `tree-integrity: SKIP`, which is reserved for a capture attempt finding no git worktree
- **AND** the retained capture SHALL still detect a real mid-run mutation, and the retention SHALL be
  disclosed on the `tree-start:` line

#### Scenario: an unvalidatable re-capture at the slot grant restores the validated pre-queue capture
- **GIVEN** a fully validated pre-queue identity exists in the run's state and on disk
- **WHEN** the re-capture at the slot grant RUNS but cannot be validated (a hash-tool or disk blip)
- **THEN** the run SHALL restore that pre-queue capture — both the identity and the on-disk manifest,
  which the failed re-capture may have overwritten — and stay guarded against the strictly wider
  pre-queue window, rather than failing a run closed for a transient failure
- **AND** the restored capture SHALL still detect a real mid-run mutation AND name the changed path
- **AND** when nothing trustworthy can be restored, the run SHALL stay FAIL-CLOSED
- **AND** an unvalidatable FIRST capture SHALL remain FAIL-CLOSED: there is no validated identity to
  fall back to, and git demonstrably exists (a genuinely non-git tree yields the no-worktree SKIP)

#### Scenario: a transient git failure at the FIRST capture does not leave the run unguarded
- **GIVEN** the very first capture reports "no git worktree", which a transient `git rev-parse`
  failure at process start is indistinguishable from
- **WHEN** the run reaches the slot grant
- **THEN** the capture SHALL be RE-ATTEMPTED there and the guard SHALL be ARMED if it succeeds, so a
  single blip at second 0 cannot yield `tree-integrity: SKIP` with `RESULT: PASS` for the whole run
- **AND** the `tree-start:` line SHALL disclose that the identity was captured at the slot grant
- **AND** a guard armed by that re-attempt SHALL still detect a real mid-run mutation
- **AND** a genuinely non-git tree SHALL fail the re-attempt and keep the plain no-worktree `SKIP`

#### Scenario: an unguarded mode discloses a worktree that reappears by the terminal capture
- **GIVEN** `--lite` and `--delta` exit before the slot grant and therefore have no re-arm point
- **WHEN** a run's first capture found no git worktree but the terminal capture finds one
- **THEN** the `tree-integrity: SKIP` line SHALL state that the start capture failed transiently and
  that the run proves nothing about the tree, so that SKIP can never be read as "there was nothing
  to check"

#### Scenario: helper modes remain exempt and unchanged
- **WHEN** `--list`, `--python-build-verify` or the concurrency-stub mode is invoked
- **THEN** no tree-identity capture SHALL be required and their behaviour and output SHALL be unchanged

### Requirement: The digest SHALL exclude only paths the gate itself writes

Exclusion SHALL be driven by the repository's own ignore rules (`--exclude-standard`) plus exactly
three explicit carve-outs, each of which is an artifact THIS RUN writes: (1) the run's own summary
file and its `integrity-fail` siblings when they resolve under the repository root, (1a) the run's
own stdout/stderr redirect target, when the platform can name the file descriptor's target and it
resolves to a regular file under the repository root — and where it cannot be named, the guard SHALL
stay armed and the failure text SHALL name that possibility rather than exclude anything on a
guess — and (2) `Cargo.lock`, which is a **named non-fatal class** rather
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

#### Scenario: a run does not trip the guard on its own stdout redirect target
- **GIVEN** the documented invocation redirects the run's output into the checkout, and a caller may
  redirect to a path the repository's ignore rules do not cover
- **WHEN** the run's stdout and/or stderr resolves to a regular file under the repository root
- **THEN** exactly that file SHALL be excluded from the digest and the run SHALL be free to certify
- **AND** no other untracked path SHALL be excluded on its account — a file the run's own components
  create mid-run SHALL still FAIL as a mutation and SHALL still be named

#### Scenario: the carve-out matches however the in-repo path is spelled
- **GIVEN** git reports only normalized repo-root-relative paths
- **WHEN** the caller pins the summary path in a non-canonical form — relative, `./…`, containing
  `..`, or absolute — that still resolves under the repository root
- **THEN** the path SHALL be canonicalized before comparison so the carve-out matches, because the
  gate creates that file only AFTER the start capture and a missed carve-out is a guaranteed false FAIL
- **AND** a path resolving outside the repository root SHALL simply not be excluded
- **AND** canonicalization SHALL succeed when the pinned path's parent directory does not exist yet,
  so the carve-out cannot be silently disarmed by a directory the gate has not created

#### Scenario: a space inside a changed path cannot be read as two paths
- **GIVEN** the `changed:` list and the `lockfile-settled:` detail are SPACE-JOINED, and they are the
  one artifact a triager reads after a mid-run mutation
- **WHEN** a changed path contains a SPACE (for example `two words.txt`)
- **THEN** the rendered list SHALL escape it (`two\swords.txt`) so a single path can never be read as
  two separate paths
- **AND** the escape SHALL be the same backslash family the manifest's `.report` view uses — `\\` for a
  literal backslash, `\n` for a newline, `\t` for a tab, `\s` for a space — never a second convention,
  and the backslash SHALL be escaped so the family is injective

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

#### Scenario: the lockfile class is admitted on the record, never on the path spelling
- **GIVEN** matching on the path alone would give the non-fatal class to any file whose name ends
  `/Cargo.lock`
- **WHEN** the changed path is UNTRACKED (an untracked `Cargo.lock` appearing mid-run), or its end
  record is not a real blob hash (the lockfile was deleted or replaced by a non-file), or the path is
  not a blob in the commit the run started on
- **THEN** the change SHALL fall through to the fatal class and the run SHALL FAIL
- **AND** a path that merely ends in `Cargo.lock` without being one (for example `notCargo.lock` or
  `deps/vendored-Cargo.lock`) SHALL likewise be a normal fatal mutation
- **AND** the dominant legitimate case — a TRACKED lockfile that is clean at the start capture and
  re-resolved by the gate's own cargo — SHALL still be stamped `lockfile-settled`

#### Scenario: the blob-id rule accepts both object-id lengths
- **GIVEN** `git hash-object` yields 40 hex characters in a SHA-1 repository and 64 in a SHA-256 one
- **WHEN** the carve-out tests whether a lockfile's end record is a real blob id
- **THEN** it SHALL accept either length through the SAME shared rule the capture digest uses, so the
  carve-out is reachable on a SHA-256 repository instead of failing every lockfile settle spuriously
- **AND** the admission SHALL stay closed there: an untracked mid-run `…/Cargo.lock` on a SHA-256
  repository SHALL still be fatal

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
whenever it is set to a non-default value or whenever the size+mtime fallback is used. The knob
SHALL be FLOORED: a value below the floor (notably `0` and `1`, at which EVERY untracked file — not
one oversized blob — would fall back to size+mtime) SHALL be clamped to the floor, and a
non-numeric or out-of-range value SHALL fall back to the default; every such normalization SHALL be
stamped.

The mtime-resolution disclosure is CONTRACT TEXT, not paraphrasable intent — an artifact that hides
a weaker platform guarantee is worse than one that never claimed it — so the wording is pinned here
verbatim. Whenever the size+mtime fallback is in force AND the host's probed mtime resolution is
coarser than nanoseconds, the `tree-hash-cap:` line SHALL append exactly one of:

```
; mtime resolution: WHOLE SECONDS on this host — a same-size rewrite within one second is NOT detected
; mtime resolution: UNAVAILABLE on this host — those records are size-only
```

— the first when the host's `stat` records whole seconds only, the second when no `stat` flavour
works. (The em dashes are literal U+2014.) When the resolution is sub-second, or when the fallback
is not in force, no such suffix SHALL be appended.

#### Scenario: no environment variable turns a mutated run green
- **WHEN** a mutated run is executed with any combination of the gate's documented environment
  variables set
- **THEN** the run SHALL still emit the named `tree-integrity: FAIL` line with `RESULT: FAIL`

#### Scenario: the hash cap is stamped when non-default or when the fallback is used
- **WHEN** `AGENT_GATE_TREE_HASH_CAP_BYTES` is set to a non-default value, or any untracked file
  exceeds the cap and is recorded by size and mtime
- **THEN** the SUMMARY SHALL carry a `tree-hash-cap:` line naming the cap and the number of FILES
  recorded by the fallback — a single oversized file present for the whole run SHALL be reported
  once, however many captures the run took
- **AND** every emitted block SHALL carry that line when it applies, including the block published
  by a component-boundary FAIL, where the capture is weakest and the disclosure matters most
- **AND** when the fallback is in force the line SHALL also state the mtime RESOLUTION the host
  actually offers whenever it is coarser than nanoseconds, so a weaker platform guarantee is
  disclosed rather than implied to be at parity

#### Scenario: a superseded capture's cap disclosure is cleared, never left standing
- **GIVEN** the full gate re-captures the start identity when its slot is granted, and the fallback
  count is re-derived from that authoritative capture
- **WHEN** the pre-queue capture engaged the size+mtime fallback and the re-capture does not
- **THEN** the `tree-hash-cap:` line SHALL NOT be published, so no block advertises a weakened
  capture that is not in effect

#### Scenario: the hash cap cannot suppress a detected mutation
- **WHEN** the cap is set arbitrarily low and a tracked file is edited mid-run
- **THEN** the mutation SHALL still be detected and the run SHALL FAIL (the cap applies only to
  untracked-file content hashing)

#### Scenario: a sub-floor cap cannot weaken detection for ordinary untracked files
- **GIVEN** a cap of `0` or `1` would push every untracked file onto the size+mtime record
- **WHEN** an untracked file is rewritten mid-run with different bytes of the SAME length and its
  mtime restored — the one edit a size+mtime record cannot see — with the cap set that low
- **THEN** the cap SHALL have been clamped to the floor, the file SHALL have been content-hashed, the
  mutation SHALL be detected and named, and the emitted block SHALL disclose the clamp

### Requirement: The guard SHALL behave identically on a BSD/macOS host

macOS is a first-class gate host (the gate carries a Darwin wrapper branch, a BSD `stat` branch and a
macOS `/bin/bash` 3.2 floor), so the tree-integrity code SHALL use no GNU-only construct. Specifically:
the changed-path parser SHALL handle both `comm -3` columns by field arithmetic inside `awk`, never by
a GNU-only `sed 's/^\t//'`; and any flag that is not universally available (`sort -z`, `stat -c`,
BSD `stat`'s fractional-seconds datum) SHALL be PROBED once with a portable fallback rather than
assumed. Separately — and NOT as a portability rule, since `awk -v` is POSIX — a value handed to
`awk` SHALL be passed through the environment rather than `awk -v`, whose escape-sequence processing
would un-escape the very tabs the report view escapes; the enforcing lint SHALL report that hazard in
its own words rather than as a GNU-only construct.

#### Scenario: the changed-path list is correct on a host whose sed does not honour \t
- **WHEN** a mid-run mutation is detected on a host whose `sed` treats `\t` as a literal `t`
- **THEN** the named FAIL line SHALL report the changed PATH, never the record's mode field, and the
  lockfile classification — which keys on those paths — SHALL be unaffected

#### Scenario: a report path is found by its escaped spelling
- **WHEN** a record's path contains a tab or a newline and is therefore escaped in the `.report` view
- **THEN** looking that record up by its escaped spelling SHALL find it, and looking up a raw or
  absent spelling SHALL find nothing

#### Scenario: a sort(1) without -z cannot silently empty the manifest
- **GIVEN** an unsupported flag makes `sort` emit nothing, which would leave the capture enumerating
  ZERO paths on a dirty tree — a silent FAIL-OPEN in which both captures agree
- **WHEN** the guard runs on a host whose `sort` rejects `-z`
- **THEN** the capture SHALL fall back to git's own deterministic path ordering, SHALL still enumerate
  the changed paths, and a mutated run SHALL still FAIL while a clean run still certifies

#### Scenario: the size+mtime fallback records a real mtime through the BSD stat interface
- **WHEN** an oversized untracked file is recorded by size+mtime on a host whose `stat` offers only
  the BSD `-f` interface
- **THEN** the record SHALL carry a numeric mtime, not `unknown`

#### Scenario: sub-second mtime resolution is used where the BSD host offers it, and disclosed where it does not
- **GIVEN** GNU `stat` records nanoseconds while a BSD `stat` limited to `%m` records whole seconds,
  so a same-size rewrite landing inside one second would be invisible to the fallback on that host
- **WHEN** the host's `stat` offers the fractional-seconds datum
- **THEN** the guard SHALL use it and the record SHALL carry a sub-second mtime, closing the gap
- **AND WHEN** it does not, the `tree-hash-cap:` line SHALL append the `WHOLE SECONDS` suffix
  pinned verbatim above (or the `UNAVAILABLE … size-only` one, when no `stat` flavour works), so
  the artifact never implies a guarantee the platform did not give

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

#### Scenario: the wiring proof is per-call-site and blind to indentation
- **GIVEN** a structural check that keys on a call site's INDENTATION is vacuous for any site written
  at a different indentation (a check that cannot fail is the defect it replaced)
- **WHEN** the self-test asserts that the terminal capture is wired into every certifying emit path
- **THEN** the assertion SHALL match on the CALL FORM with leading whitespace and trailing comments
  stripped, SHALL assert the emit-site and explicit-capture inventories of each certifying function,
  and SHALL be PROVED discriminating by deleting each call site in turn — addressed by its ORDINAL
  within the function — in a scratch copy, including the terminal site of `run_delta`

#### Scenario: the truncated changed-path list is covered by a test and a control
- **WHEN** more than five paths differ between the start and end manifests
- **THEN** the named line SHALL list five paths followed by an explicit `(+N more)` remainder, and at
  or below five paths SHALL list them all with no remainder marker

#### Scenario: the self-test names its own prerequisites
- **GIVEN** the slot-grant cases need `scripts/lib/gate_slot_daemon.py` beside `scripts/agent-gate.sh`
- **WHEN** the self-test runs from a copy that lacks it
- **THEN** it SHALL report that missing prerequisite by name rather than failing the slot-grant cases
  for an unexplained reason

#### Scenario: the self-test is hermetic and leaves no shared fixtures
- **WHEN** two instances of the self-test run concurrently in one checkout
- **THEN** both SHALL pass and neither SHALL remove or overwrite the other's fixtures

#### Scenario: no assertion depends on wall-clock duration
- **WHEN** the self-test sequences its mid-run mutation
- **THEN** it SHALL rendezvous on the gate's per-component result artifacts, and its assertions SHALL
  reference captured identities only — no elapsed-time threshold SHALL appear in the correctness path

#### Scenario: the suite covers a BSD/macOS host, behaviourally and statically
- **GIVEN** the guard's first Linux-only construct shipped because the suite had no macOS path at all
- **WHEN** `scripts/tests/test_agent_gate_tree_portability.sh` runs inside `tooling-tests`
- **THEN** it SHALL re-run the parsing, classification and mtime paths against PATH shims that
  reproduce the BSD divergences (a `sed` that does not honour `\t`, a `stat` offering only `-f`, a
  `sort` that rejects `-z`) with `AGENT_GATE_TEST_OS=Darwin` forcing the host-family branches
- **AND** it SHALL statically lint EVERY tree-integrity function for the GNU-only construct classes,
  allowlisting only the flags the code PROBES for, so a reintroduction on a path the shims do not
  execute fails too
- **AND** each lint rule SHALL be proved discriminating by a mutant that it catches, with a portable
  control body that it does not flag
- **AND** the set of functions it lints SHALL be DERIVED from the gate rather than maintained by
  hand, so a tree-integrity helper added by a later change is covered by that change; this SHALL be
  proved by a mutant that adds a helper carrying a banned construct and asserts the lint catches it

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

