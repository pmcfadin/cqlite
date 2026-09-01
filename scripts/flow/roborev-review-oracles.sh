#!/usr/bin/env bash
# roborev-review-oracles.sh — the LOCAL oracles behind roborev-review.sh (#2964/#3229).
#
# SOURCED, never executed: it defines `roborev_push_assert`, `roborev_census` and the
# path-normalisation/prompt-matching helpers (`roborev_unquote_path`,
# `roborev_collect_prompt_headers`, `roborev_diff_header_has_path`), which run inside the
# wrapper's scope and use its state (REPO, BRANCH, HEAD_SHA, BASE, DETAILS, the
# summary-state variables) and its `finish` function.
#
# Why these live together, and apart from the wrapper: they are the change's
# whole thesis in code — every claim is judged against data WE obtain, never against
# a proxy or against the reviewer's prose. Each learned that the hard way:
#   * push-assert read the local `refs/remotes/<remote>/<branch>` mirror, which a
#     narrow fetch refspec never creates, so it false-FAILed 100% of the fleet. It
#     now asks the REMOTE via `git ls-remote`.
#   * the census discarded `git diff`'s exit status, so a FAILED diff rendered as
#     "0 files, genuinely empty" — asserting a measurement that never happened.
#   * `prompt-content:` used to be satisfied by the reviewer's prose; it now matches the
#     CODE census against the `diff --git` headers of the prompt the reviewer was actually
#     given, through the single normalisation boundary documented below.
# Keeping them in one sourced file also keeps the wrapper inside the campsite size
# guidance (issue #1116's spirit; the gate's ratchet covers .rs only).
#
# Self-test: scripts/tests/test_roborev_review_guard.sh (which also asserts the
# wrapper FAILS CLOSED when this file is missing — a silently absent oracles file
# turning both checks into no-ops would be the worst regression possible here).

# shellcheck disable=SC2034
# ^ every variable assigned in this file (PUSH_ASSERT, BASE_TIP_SHA, RANGE_BASE_SHA,
#   CENSUS_CHECK, CODE_FREE, census_*) is READ by the sourcing wrapper, which
#   shellcheck cannot see when it lints this fragment standalone. Lint the pair with
#   `shellcheck -x scripts/flow/roborev-review.sh` to resolve them across the
#   source boundary.

# Code-free (non-code) census classification. A census consisting ENTIRELY of prose
# is STRUCTURALLY DISCARDED by roborev (trigger 3), so it is a DETERMINISTIC FAIL
# condition in its own right under the `code-free:` key — never a bet on the
# reviewer's prose happening to admit it (which is what the previous revision did:
# it computed this classification and then used it only for wording).
#
# EXTENSION-BASED, with a narrow path assist. An earlier revision treated every file
# under `docs/`, `.github/` or `.claude/` as non-code, which misclassifies
# `docs/foo.py` and a workflow `.yml` — and now that code-free is a FAIL condition, a
# false code-free classification is a FALSE FAIL. So the test is the file EXTENSION,
# and the path assist covers only EXTENSIONLESS files under a prose directory
# (`docs/LICENSE`, `openspec/NOTES`). Anything with a code-ish extension anywhere —
# including `.github/workflows/*.yml` — counts as CODE and does not trip this key.
CODE_FREE_EXTENSIONS="md markdown mdx txt rst adoc"
# THE PROSE PREFIXES, and the RULE that applies under them: an extensionless path under one
# of these directories is CODE **iff git records it EXECUTABLE AT EITHER ENDPOINT of the
# census range** (`${BASE}...HEAD`) — non-code only when it is executable at NEITHER.
#
# EITHER endpoint, because the census SUBJECT is the range, not a snapshot. Asking only HEAD
# would miss a deleted executable; asking only BASE would miss an added one; asking "whichever
# endpoint answers first" misses a MODE CHANGE, and did (#3229 round-13 blocker: `100755`@BASE
# → `100644`@HEAD read non-code, because the HEAD record ended the scan before BASE was ever
# consulted — and a `chmod -x` does not turn a script into prose). The rule is a DISJUNCTION
# over both endpoints; see `roborev_path_exec_state` for how the shape now makes skipping
# one unexpressible.
#
# AND THERE IS A THIRD ANSWER (#3229 round-14 blocker): **could not measure**. `git ls-tree` can
# FAIL — an unreadable repository, a ref that does not resolve to a tree, a corrupt object — and
# the round-13 leaf wrote `|| return 1`, giving a FAILED lookup the SAME value as a measured
# non-executable. So a file the repo genuinely runs classified as prose, silently, on an infra
# fault. Non-code is now reachable ONLY from a positive measurement at every endpoint; an
# unmeasurable one FAILs `census-check:` closed and names the path and the ref.
#
# The prefix ALONE was the bug (#3229 round-11 blocker). Under a bare prefix test EVERY
# extensionless path under `docs/` classified non-code, so it was dropped from
# `census_code_paths` and `prompt-content:` made NO CLAIM about it — while the narrowed
# `exclude_patterns` exclude only `*.md` globally plus docs-scoped ARTIFACT EXTENSIONS, so an
# extensionless file is NOT excluded and genuinely DOES reach the reviewer. The guard was
# therefore silent on precisely the class this issue's AC2 names — "the first post-merge PR
# carrying an executable under `docs/`" — whenever that executable has no extension. Three
# such files are tracked TODAY, all mode 100755:
#   docs/reports/ws0-3026-artifacts/ws0-results/ws0-readbw
#   docs/reports/ws0-3026-artifacts/ws0-results/ws0-stream
#   docs/reports/ws0-3217-artifacts/partB-run/offcputime-bigmap
# The EXECUTABLE BIT is the discriminator because it is EVIDENCE rather than a name guess
# (the no-heuristics posture): a file git records as executable is something the repo intends
# to RUN, which is what an extensionless harness script is. A non-executable extensionless
# file under a prose directory stays non-code — which is the only thing this prefix list was
# ever for (`docs/LICENSE`, `openspec/NOTES`, a `CODEOWNERS` under `.claude/`).
CODE_FREE_EXTENSIONLESS_PREFIXES="openspec/ docs/ website/ .claude/"

# The DOCS-SCOPED ARTIFACT extension set (issue #3229) — raw run output and
# binary/image blobs a report commits beside itself. THE SINGLE DECLARATION: it is
# imported, never redeclared (`scripts/ci/classify-docs-only.sh` will import it —
# issue #3250).
#
# It exists because the census and `.roborev.toml`'s artifact deny-list must AGREE
# on what an artifact is. The prose set above is only `md markdown mdx txt rst adoc`,
# so `.json`/`.jsonl`/`.log`/`.err`/`.csv`/`.svg` in an artifact directory would otherwise
# count as CODE while the configuration excludes them — and `prompt-content:` would then
# FAIL on every legitimate report PR, expecting in the prompt a file the configuration had
# already removed.
#
# SCOPED TO ARTIFACT-BEARING DIRECTORIES, NOT TO `docs/` AS A WHOLE (#3229 round-6
# blocker 2). The intermediate form was a bare `docs/` PREFIX test mirroring
# `docs/**/*.<ext>`, and it classified FUNCTIONAL CONFIG as an artifact: the gate's own
# `kit-dashboard-drift` component guards
# `docs/observability/grafana/dashboards/cqlite-overview.json`, yet a PR editing it was
# both dropped from the reviewer's diff and counted code-free — unreviewable by
# construction. `docs/reports/delivery-telemetry.schema.json` was hidden the same way. A
# path is an artifact only when its extension is in the set below AND it sits under one of
# the four directory globs — the exact intersection `.roborev.toml` configures.
#
# ONE MIRROR, TWO REPRESENTATIONS, MAINTAINED BY HAND. These constants and
# `.roborev.toml`'s `exclude_patterns` are the same fact written twice, and a one-sided
# edit is the standing hazard (#3260 item 2). Add an extension or a directory HERE and
# THERE in one edit.
#
# THERE IS NO AUTOMATED DRIFT ASSERT, and that is a KNOWN GAP, not an oversight (#3283).
# One existed briefly: it re-derived the expected pattern set from these constants and
# asserted set equality against the committed `.roborev.toml`. It was removed with the rest
# of the exclusion-modelling subsystem it depended on (a bash TOML parser over three config
# sources), because that subsystem produced false-PASSes faster than review rounds could
# close them. Drift between this list and `.roborev.toml` therefore surfaces the slow way —
# as a `prompt-content:` FAIL on someone's report PR — until #3283 lands a guard whose own
# correctness is establishable.
CODE_FREE_ARTIFACT_EXTENSIONS="txt json jsonl log err csv png svg gz pdf jfr html mmd tex diff"
# The DIRECTORY GLOBS, in git `:(glob)` pathspec spelling: `*` matches within a single
# path component, `**` matches zero or more components. The configured pattern for each is
# `<glob>/**/*.<ext>` — never a blanket `<glob>/**`, because these directories hold
# EXECUTABLE harness code beside their output and swallowing it is precisely #3229.
#
# AN ARRAY, NOT A SPACE-SEPARATED STRING, and that is load-bearing: these values CONTAIN
# `*`, so iterating an unquoted string would PATHNAME-EXPAND them against `$PWD`.
# Measured while writing this: run from
# the repo root, `docs/reports/*-artifacts` collapsed to the four directories that happen
# to exist today and `docs/**/jfr-reports` to the single existing one, so
# `docs/jfr-reports/a.html` stopped matching — the classification silently became "the
# directories present in this checkout" instead of "the configured globs". An array
# removes the hazard structurally rather than by remembering to quote.
CODE_FREE_ARTIFACT_DIR_GLOBS=(
  'docs/reports/*-artifacts'
  'docs/round-artifacts'
  'docs/**/jfr-reports'
  'docs/sstables-definitive-guide/diagrams'
)
# roborev_path_in_artifact_dir <path>: 0 when `<path>` lies STRICTLY BENEATH a directory
# matching one of `CODE_FREE_ARTIFACT_DIR_GLOBS`, else 1.
#
# Component-wise on purpose. The obvious `case "$path" in docs/round-artifacts/*)` would
# be wrong for the two globbed entries: bash's `case` lets `*` cross `/`, so
# `docs/reports/*-artifacts/*` would also match `docs/reports/a/b-artifacts/x` — which
# git's `:(glob)` `*` does NOT. Matching one component at a time keeps the shell's `*`
# inside a slash-free string, so it cannot cross a separator, and `**` is handled
# explicitly as "zero or more components". That makes this function agree with the
# pathspec git is actually given rather than approximate it.
roborev_path_in_artifact_dir() {
  local path="$1" glob
  local -a _pc=() _gc=()
  IFS='/' read -r -a _pc <<<"$path"
  for glob in "${CODE_FREE_ARTIFACT_DIR_GLOBS[@]}"; do
    _gc=()
    IFS='/' read -r -a _gc <<<"$glob"
    if _rx_dirglob_match 0 0; then return 0; fi
  done
  return 1
}

# _rx_dirglob_match <glob-index> <path-index>: do the glob components `_gc[gi..]` match
# path components `_pc[pi..]` while leaving AT LEAST ONE path component unconsumed? The
# leftover is what makes this "under the directory" rather than "is the directory" — a
# file named exactly `docs/round-artifacts` is not inside it.
_rx_dirglob_match() {
  local gi="$1" pi="$2" k
  if [ "$gi" -ge "${#_gc[@]}" ]; then
    [ "$pi" -lt "${#_pc[@]}" ] && return 0
    return 1
  fi
  if [ "${_gc[$gi]}" = '**' ]; then
    for ((k = pi; k <= ${#_pc[@]}; k++)); do
      if _rx_dirglob_match "$((gi + 1))" "$k"; then return 0; fi
    done
    return 1
  fi
  [ "$pi" -lt "${#_pc[@]}" ] || return 1
  # UNQUOTED on the right so the component is treated as a PATTERN; the left side is a
  # single path component and therefore slash-free, so `*` cannot cross a separator.
  case "${_pc[$pi]}" in
    ${_gc[$gi]}) _rx_dirglob_match "$((gi + 1))" "$((pi + 1))" && return 0 ;;
  esac
  return 1
}

# ROBOREV_RANGE_ENDPOINT_REFS: the refs that ARE the census range's endpoints, named once so
# neither caller nor reader can disagree about what "the range" means. HEAD is the right-hand
# endpoint; `RANGE_BASE_SHA` — the MERGE-BASE, which is what `<base>...HEAD` actually diffs
# against, NOT the base ref's tip (#3392) — is the left. The order is presentational ONLY —
# the fold below is a DISJUNCTION, so permuting this array cannot change any answer.
roborev_range_endpoint_refs() {
  local ref
  for ref in HEAD "${RANGE_BASE_SHA:-}"; do
    [ -n "$ref" ] && printf '%s\n' "$ref"
  done
}

# ===================== THE CLASS-LEVEL RULE THIS LEAF EXISTS TO OBEY =====================
# ANY PREDICATE FEEDING A SAFETY DECISION MUST BE TRI-VALUED — yes / no / could-not-measure.
# A BOOLEAN CANNOT EXPRESS UNCERTAINTY, so it is forced to fold "I could not tell" onto one of
# its two values, and the value it folds onto is always the PERMISSIVE one ("nothing wrong",
# "nothing to review"). That is not a bug in any one call site; it is the shape.
#
# This is the NINTH instance of "could not measure" rendered as "nothing wrong" on this change
# (after `built-in-set: UNAVAILABLE`, `corroboration: UNAVAILABLE`, the fail-open
# `${_census_end:-$_census_start}`, the permissive verdict scan, and the measurement failures),
# and the LEVEL-SHIFT is why another point patch would not have ended it: round 13 made the
# FOLD below order-independent BY CONSTRUCTION while leaving this LEAF two-valued, so it proved
# the right property ONE LEVEL TOO HIGH. An order-independent fold over a predicate that has
# already discarded the distinction cannot recover it.
#
# _roborev_mode_exec_state_at <ref> <path>: the recorded mode STATE of `<path>` in the tree at
# `<ref>`. The EXIT STATUS *is* the state, and there are THREE of them:
#   0  EXEC          — `git ls-tree` SUCCEEDED and records mode 100755.
#   1  NOT-EXEC      — `git ls-tree` SUCCEEDED and records some other mode, OR SUCCEEDED and
#                      returned NO RECORD. BOTH ARE REAL MEASUREMENTS. A successful `ls-tree`
#                      with empty output means the path is genuinely ABSENT at that ref — the
#                      added/deleted case the endpoint matrix already covers — and it is
#                      perfectly fine. It must NEVER be conflated with the state below.
#   2  UNMEASURABLE  — `git ls-tree` FAILED (`$REPO` is not a repository, the ref does not
#                      resolve to a tree, a corrupt/unreadable object). NOTHING was measured, so
#                      there is no answer of any kind for this endpoint.
# The distinction between 1-by-empty-output and 2 is the whole fix: the previous revision wrote
# `... || return 1`, which gave a FAILED lookup the SAME value as a measured non-executable.
#
# THE RENAME IS LOAD-BEARING. The old name was `_roborev_mode_is_exec_at` and it was called as
# `if _roborev_mode_is_exec_at ...`, where `if` collapses 1 and 2 back into "false". A surviving
# boolean call site would silently reintroduce exactly this defect, so the name such a call site
# would use no longer exists — the breakage is a syntax-visible "command not found", not a
# permissive answer.
#
# DELIBERATELY IGNORANT OF THE RANGE. It answers for ONE endpoint and cannot express an
# ordering, a precedence or a short-circuit, because it has no second endpoint to skip. All
# range semantics live in the fold below, in one place, so there is exactly one thing to get
# right instead of one per ref.
#
# THE MODE COMES FROM GIT'S TREE, NEVER FROM `test -x` ON THE FILESYSTEM (#3229), for three
# reasons in ascending order of how badly the filesystem answer fails:
#   1. The census subject is the RANGE `${BASE}...HEAD`, and a path in that range need not
#      exist in the working tree at all — a path the diff DELETES has no file to stat, so
#      `test -x` would answer a DIFFERENT question with a plausible-looking value.
#   2. The recorded mode is the one the diff — and therefore the reviewer's prompt — carries
#      (`new file mode 100755`); it comes out of the tree, not out of the checkout.
#   3. A checkout's bits are not authoritative anyway: under `core.fileMode=false`, or on a
#      filesystem that cannot hold the bit, the working file diverges from the tree while git
#      keeps honouring the recorded mode.
#
# `:(literal)` pathspec magic is load-bearing: nothing stops a repo tracking a name
# containing `*`, `?` or `[`, and a bare pathspec would read those as WILDCARDS — answering
# for a different file, or for several. It is fed the RAW `$path` the census carries, so the
# single normalisation boundary is untouched.
#
# NO `-z` (#3229): `-z` made every call emit `warning: command substitution: ignored null
# byte in input` on stderr — per-call noise that can MASK a real warning. Dropping it is safe
# precisely because the PATH FIELD IS NEVER READ: only the leading MODE is, and that field is
# first, space-terminated, and always one of git's four literal mode constants. Without `-z`
# git C-QUOTES an odd name, which keeps a newline-bearing path on ONE line — if anything
# harder to confuse than the raw form. An absent path is a SILENT empty result (not an
# error), so content is tested, not just git's exit status — and now the two are DISTINGUISHED
# rather than both meaning "not executable".
#
# git's OWN message is captured (not discarded to `/dev/null`) into
# `ROBOREV_EXEC_MEASURE_STDERR`, because "unmeasurable" is only actionable when the operator is
# told WHY. It is set on EVERY call, so a stale message from an earlier call can never be
# attributed to this one.
ROBOREV_EXEC_MEASURE_STDERR=""
_roborev_mode_exec_state_at() {
  local ref="$1" path="$2" record mode errfile rc
  ROBOREV_EXEC_MEASURE_STDERR=""
  if [ -z "$ref" ]; then
    # UNMEASURABLE, not NOT-EXEC: an empty ref names no tree, so nothing was read. Unreachable
    # from the fold (the endpoint producer drops empties), and fail-closed anyway.
    ROBOREV_EXEC_MEASURE_STDERR="empty ref: no tree to read"
    return 2
  fi
  errfile=$(mktemp "${TMPDIR:-/tmp}/roborev-exec-state.XXXXXX") || return 2
  rc=0
  record=$(git -C "$REPO" ls-tree "$ref" -- ":(literal)$path" 2>"$errfile") || rc=$?
  if [ "$rc" -ne 0 ]; then
    ROBOREV_EXEC_MEASURE_STDERR=$(head -1 "$errfile" 2>/dev/null || printf '')
    [ -n "$ROBOREV_EXEC_MEASURE_STDERR" ] || ROBOREV_EXEC_MEASURE_STDERR="git ls-tree exited $rc with no message"
    rm -f "$errfile"
    return 2
  fi
  rm -f "$errfile"
  # SUCCEEDED with no record => the path is genuinely ABSENT at this ref. A MEASUREMENT.
  [ -n "$record" ] || return 1
  mode="${record%% *}"
  if [ "$mode" = 100755 ]; then return 0; fi
  return 1
}

# roborev_path_exec_state <path>: the mode state of `<path>` ACROSS THE CENSUS RANGE, joined from
# the per-endpoint states. Exit status is the state, on the SAME three-valued scale as the leaf:
#   0  EXEC          — git records it executable at at least one endpoint  -> CODE
#   1  NOT-EXEC      — MEASURED non-executable (or measured absent) at EVERY endpoint -> prose
#   2  UNMEASURABLE  — no endpoint said EXEC and at least one endpoint could not be measured
#                      (or no endpoint was consulted at all) -> the caller MUST fail closed;
#                      `ROBOREV_EXEC_UNMEASURABLE_REFS` names which refs, and why.
# Used only for the extensionless-under-a-prose-prefix decision above.
#
# THE LATTICE, and why it is the safe one. Three states, TOTALLY ordered
#       NOT-EXEC  <  UNMEASURABLE  <  EXEC
# and the join is the MAXIMUM under that order. Because the order is total, the join is
# associative, commutative and idempotent — so order-independence is a property OF THE LATTICE,
# not of the loop, which is what keeps round 13's by-construction guarantee intact now that the
# accumulator carries three states instead of a boolean.
#   * EXEC dominates everything, UNMEASURABLE included, and that is SOUND rather than
#     convenient: the rule is a DISJUNCTION over the endpoints, so positive evidence at ONE
#     endpoint already settles it — whatever an unmeasurable endpoint would have said could only
#     be another "yes", and no "yes" can un-satisfy a disjunction. This is a conclusion drawn
#     from a real measurement, never a guess about the endpoint that failed.
#   * UNMEASURABLE dominates NOT-EXEC because "executable at NEITHER endpoint" is a claim about
#     EVERY endpoint, and one unmeasured endpoint leaves it unfounded.
#   * NOT-EXEC is therefore the ONLY state that can reach the permissive classification (prose,
#     dropped from `census_code_paths`, asserted about by nothing) — and reaching it now
#     requires a POSITIVE measurement at every single endpoint.
#   * The accumulator STARTS at UNMEASURABLE, not at the lattice bottom: an endpoint set that
#     yielded nothing measured nothing, so it must not answer "prose". (Unreachable today —
#     `roborev_range_endpoint_refs` always yields HEAD — closed by construction rather than by
#     relying on that, because a fail-open reachable only by a future edit is the same class of
#     defect this whole function is fixing.)
#
# EITHER ENDPOINT, AND THE DISJUNCTION IS THE WHOLE RULE (#3229 round-13 blocker). The census
# subject is the RANGE, so a path is a code path if it is an executable ANYWHERE in that range:
# both endpoints belong to the reviewed change and neither outranks the other. The four cases
# this must get right, all of them by the same single rule:
#   present at both, exec at either  -> CODE. Includes a MODE CHANGE in EITHER direction: a
#       `chmod -x` does not turn a script into prose, and a `chmod +x` of an existing file is
#       an executable entering the range.
#   present at HEAD only (added)     -> CODE iff added executable.
#   present at BASE only (deleted)   -> CODE iff it WAS executable. Fail-closed: removing an
#       executable is a code change whose review must be asserted, and a pure deletion still
#       carries a `diff --git` header for `prompt-content:` to find.
#   present at neither               -> NOT-EXEC, and that is a MEASUREMENT: `ls-tree` succeeded
#       at both endpoints and reported no record, i.e. git positively says the path is not there.
#       Do not confuse it with UNMEASURABLE, where `ls-tree` itself failed and git said nothing
#       at all. (Unreachable for a real census path, which by construction exists at an
#       endpoint — it is reachable only by the direct unit probe.)
#
# BY CONSTRUCTION, NOT BY CARE. The previous revision was an ordered scan that `return`ed on
# the FIRST ref yielding a record, so a path recorded 100644 at HEAD NEVER REACHED BASE and a
# `chmod -x` silently dropped it from `census_code_paths` — `prompt-content:` then asserted
# NOTHING about it while claiming `PASS (n/n)`. Moving that `return` would have fixed the one
# case and left the shape that produced it. So the shape changed instead, and three properties
# now make skipping an endpoint UNEXPRESSIBLE here rather than merely unintended:
#   1. The endpoint list is produced by `roborev_range_endpoint_refs`, complete before the
#      fold starts — the loop cannot be cut short by a ref it has not looked at yet.
#   2. THE LOOP BODY HAS NO `return`, NO `break` AND NO `continue`. It can only ever OR into
#      the accumulators, so control flow cannot leave the fold early; the function's sole
#      `return` is after the loop, once every endpoint has been consulted. (Pinned
#      STRUCTURALLY by the guard test, so a future edit cannot reintroduce an early exit.)
#   3. The per-endpoint predicate is a separate, range-blind function, so there is no
#      "first"/"then" for a reader or an editor to get wrong — only a set to fold.
# The three-state accumulator is carried as THREE MONOTONE FLAGS rather than one running
# maximum, so each is a plain OR (idempotent, commutative) and the lattice join is applied ONCE,
# after the loop, as a fixed precedence. Same guarantee, and nothing inside the loop can lower a
# flag.
#
# ROBOREV_EXEC_UNMEASURABLE_REFS: reset on every call, then one entry per endpoint whose mode
# could not be measured, spelled `<ref>: <git's own message>`. A SET — its order is
# presentational, exactly like the endpoint list's.
ROBOREV_EXEC_UNMEASURABLE_REFS=()
roborev_path_exec_state() {
  local path="$1" ref st consulted=0 saw_exec=0 saw_unmeasurable=0 state
  ROBOREV_EXEC_UNMEASURABLE_REFS=()
  while IFS= read -r ref; do
    # NO early exit of any kind — see property 2 above. Every endpoint is consulted on every
    # call, and every accumulator is a monotone OR, so the result cannot depend on the order.
    consulted=1
    st=0
    _roborev_mode_exec_state_at "$ref" "$path" || st=$?
    if [ "$st" -eq 0 ]; then saw_exec=1; fi
    # ANY status that is neither of the two MEASURED ones counts as UNMEASURABLE, so there is no
    # unhandled bucket for a future fourth state to fall through permissively.
    if [ "$st" -ne 0 ] && [ "$st" -ne 1 ]; then
      saw_unmeasurable=1
      ROBOREV_EXEC_UNMEASURABLE_REFS+=("$ref: ${ROBOREV_EXEC_MEASURE_STDERR:-git ls-tree failed with no message}")
    fi
  done < <(roborev_range_endpoint_refs)
  # THE JOIN, once, as the lattice's fixed precedence: EXEC > UNMEASURABLE > NOT-EXEC, over an
  # accumulator that starts at UNMEASURABLE (nothing consulted == nothing measured).
  state=2
  if [ "$consulted" -eq 1 ]; then state=1; fi
  if [ "$saw_unmeasurable" -eq 1 ]; then state=2; fi
  if [ "$saw_exec" -eq 1 ]; then state=0; fi
  return "$state"
}

# NO EXCLUSION SET IS MODELLED HERE AT ALL (issues #3283 / #3278).
#
# THE FACT, and it remains true: roborev drops exactly what its exclusion pathspecs match,
# and those come from TWO halves — the repo/global `exclude_patterns` configuration, and a
# hard-coded lockfile/cache deny-list the binary appends (`**/Cargo.lock`, `**/go.sum`,
# `**/pnpm-lock.yaml`, `**/.cache/**`, …) with no configuration switch and no opt-out.
# roborev makes NO code/non-code judgement of its own; the earlier prose comment that
# credited it with one was FALSIFIED by PR #3222, where the then-configured `docs/**`
# discarded 33 EXECUTABLE harness files.
#
# WHAT THIS FILE DOES ABOUT IT: nothing predictive. The remedy #3229 shipped is the
# NARROWED configuration in `.roborev.toml` itself (artifact extensions inside
# artifact-bearing directories, never a blanket `docs/**`) plus the census/`prompt-content:`
# pair, which measures what the reviewer ACTUALLY received rather than predicting what it
# should have. An oracle that predicted the effective exclusion set — a bash port of
# roborev's `git.FormatExcludeArgs` plus a TOML parser over three config sources — was
# built here and REMOVED by owner ruling: four consecutive review rounds found false-PASSes
# inside it at an INCREASING rate (1, 1, 2, 3), and two of the last round's three defects
# lived in code the two preceding fix rounds had just introduced. A guard with known
# documented false-PASSes is worse than no guard, because it invites reliance it cannot
# support. Re-attempting it is issue #3283; modelling the binary's built-ins is #3278.
#
# THE RESULTING RESIDUAL, stated rather than hidden: a path roborev excludes (by either
# half) is not named as such anywhere pre-enqueue. It surfaces AFTER the review as a
# `prompt-content:` FAIL — "the reviewer did not receive this path", which is TRUE but
# attributes it to nothing. That is a DIAGNOSTIC gap and it fails CLOSED; it is never a
# vacuous green, and never a claim that a path WAS delivered.


# roborev_push_assert: sets PUSH_ASSERT (and REMOTE/REMOTE_SHA); calls `finish` on
# failure, so it never returns when the branch is not pushed.
roborev_push_assert() {
  # --- step 2: push assert (AC3) — before the census, so the operator gets the ---
  # --- actionable cause ("push your commits") rather than a downstream vacuity ---
  #
  # THE ORACLE IS THE REMOTE, NOT THE LOCAL MIRROR (#2964 follow-up). The obvious
  # implementation — read `refs/remotes/<remote>/<branch>` — is WRONG on this fleet
  # and was a 100%-reproducible false FAIL: CQLite clones carry a NARROW fetch
  # refspec
  #     remote.origin.fetch = +refs/heads/main:refs/remotes/origin/main
  # so `refs/remotes/origin/*` only ever holds `origin/main` (+ `origin/HEAD`). A
  # remote-tracking ref for a feature branch is NEVER created there, no matter how
  # many times the branch is pushed — so "mirror ref absent" says NOTHING about
  # whether the branch is pushed. `git ls-remote` asks the REMOTE, which is the
  # authoritative answer, exactly as the census asks `git` locally rather than
  # believing the reviewer's prose. A local proxy is never authority.
  if [ "$BRANCH" = "HEAD" ]; then
    PUSH_ASSERT="FAIL (detached HEAD)"
    DETAILS+=("ERROR: push-assert: $REPO is on a detached HEAD, so there is no branch to assert against. Check out the issue branch. No review was enqueued.")
    finish FAIL 1
  fi

  # Prefer the branch's CONFIGURED upstream remote; fall back to `origin`.
  REMOTE=$(git -C "$REPO" config --get "branch.$BRANCH.remote" 2>/dev/null || printf '')
  [ -n "$REMOTE" ] || REMOTE=origin

  # NO MIRROR-REF FAST PATH. An earlier revision short-circuited when
  # `refs/remotes/<remote>/<branch>` happened to equal HEAD; that is unsound, and the
  # reviewer flagged it. A CACHED mirror ref survives a force-push or an outright
  # DELETION of the remote branch, so it can equal HEAD while the remote no longer
  # has the commit at all — the wrapper would then enqueue a review for a commit the
  # reviewer cannot fetch, which is precisely a vacuous-review setup. `ls-remote`
  # costs ~1s, and not trusting local proxies is this wrapper's whole point.
  set +e
  LS_OUT=$(git -C "$REPO" ls-remote --heads "$REMOTE" "$BRANCH" 2>&1)
  LS_RC=$?
  set -e
  if [ "$LS_RC" -ne 0 ]; then
    # NOT "never pushed" — a misattributed cause sends people to fix the wrong
    # thing. `git` and `gh` are SEPARATE credential paths (#2942): an
    # authenticated `gh` with an unwired git fails every remote read here.
    PUSH_ASSERT="FAIL (ls-remote failed: infra/auth)"
    DETAILS+=("ERROR: push-assert: 'git ls-remote --heads $REMOTE $BRANCH' exited $LS_RC, so the remote state is UNKNOWN. This is an INFRA/AUTH condition, NOT evidence that the branch is unpushed — do not 'fix' it by pushing. git and gh are separate credential paths (#2942): check network reachability and git's own credentials ('gh auth setup-git'). git said:")
    while IFS= read -r line; do
      [ -n "$line" ] || continue
      DETAILS+=("  $line")
    done <<<"$LS_OUT"
    DETAILS+=("ERROR: push-assert: failing closed on an unknown remote state. No review was enqueued.")
    finish FAIL 1
  fi
  REMOTE_SHA=$(printf '%s\n' "$LS_OUT" | awk -v ref="refs/heads/$BRANCH" '$2 == ref { print $1; exit }')
  if [ -z "$REMOTE_SHA" ]; then
    PUSH_ASSERT="FAIL (branch absent on remote $REMOTE)"
    DETAILS+=("ERROR: push-assert: the remote '$REMOTE' has no branch '$BRANCH' (authoritative: git ls-remote) — this branch has never been pushed. The reviewer can only see what the remote has, so an unpushed branch is itself an empty-diff cause. Push it, then re-run. No review was enqueued.")
    finish FAIL 1
  fi
  if [ "$REMOTE_SHA" != "$HEAD_SHA" ]; then
    PUSH_ASSERT="FAIL (unpushed commits)"
    DETAILS+=("ERROR: push-assert: $REMOTE/$BRANCH is at $REMOTE_SHA (authoritative: git ls-remote) but local HEAD is $HEAD_SHA.")
    unpushed=""
    if git -C "$REPO" cat-file -e "${REMOTE_SHA}^{commit}" 2>/dev/null; then
      unpushed=$(git -C "$REPO" log --oneline "$REMOTE_SHA..HEAD" 2>/dev/null || printf '')
    fi
    if [ -n "$unpushed" ]; then
      DETAILS+=("ERROR: push-assert: unpushed commit(s):")
      while IFS= read -r line; do
        [ -n "$line" ] || continue
        DETAILS+=("  $line")
      done <<<"$unpushed"
    else
      DETAILS+=("ERROR: push-assert: local HEAD is not a descendant of the remote tip (or the remote tip is not present locally) — the branch has diverged; reconcile before reviewing.")
    fi
    DETAILS+=("ERROR: push-assert: push the branch before reviewing. No review was enqueued.")
    finish FAIL 1
  fi
  PUSH_ASSERT="PASS"
}

# roborev_census: sets BASE_TIP_SHA, RANGE_BASE_SHA, CENSUS, CENSUS_CHECK, CODE_FREE and the
# census_* / census_paths state; calls `finish` on failure or an empty census.
roborev_census() {
  # --- step 3: the local diff census — THE ORACLE -------------------------------
  # `<base>` (default `origin/main`) IS a local mirror ref, so it can be stale or —
  # on a narrow-refspec clone that has never fetched — absent. Fail CLOSED: an
  # unresolvable base must never be allowed to produce an empty census, which would
  # surface as NOTHING-TO-REVIEW and read as "nothing to look at" rather than "we
  # could not tell". No implicit `git fetch` is performed on the caller's behalf.
  BASE_TIP_SHA=""
  RANGE_BASE_SHA=""
  if ! BASE_TIP_SHA=$(git -C "$REPO" rev-parse --verify --quiet "${BASE}^{commit}"); then
    CENSUS_CHECK="FAIL (base '$BASE' unresolvable)"
    DETAILS+=("ERROR: census: base ref '$BASE' does not resolve to a commit in $REPO, so the census — and therefore every vacuity judgement — would be unfounded. This is a FAIL, explicitly NOT a NOTHING-TO-REVIEW: an unresolvable base is 'we cannot tell', never 'there is nothing to review'. If '$BASE' is a remote-tracking ref, this clone may have a narrow fetch refspec or have never fetched it; fetch it yourself (the wrapper never fetches behind your back) and re-run. No review was enqueued.")
    finish FAIL 1
  fi

  # ===== THE RANGE BASE IS THE MERGE-BASE, NEVER THE BASE REF'S TIP (#3392) =====
  # `<base>...HEAD` — the THREE-DOT form the census measures — is
  # `merge-base(<base>, HEAD)..HEAD`. It is NOT `<base-tip>..HEAD`, and the two differ for
  # every branch whose `<base>` has advanced since the branch point, i.e. for almost every
  # branch that has not just been rebased. roborev reviews the same three-dot range and its
  # job record's `git_ref` therefore carries `<merge-base>..<head>`.
  #
  # Before this, ONE name (`BASE_SHA`) held the TIP and was used both as this range's left
  # endpoint and as `sha-assert`'s expected range base, so the assert compared a
  # merge-base-relative reviewed range against the tip and FAILED DETERMINISTICALLY on a
  # CORRECT review — an abort that costs the review's tokens and reads exactly like a vacuous
  # review. Two different definitions of "base" under one name is the whole defect, so the
  # name is retired: `RANGE_BASE_SHA` is the base OF THE RANGE UNDER REVIEW (what the census
  # diffs from, what the assert expects, what the absence waiver is bound to) and
  # `BASE_TIP_SHA` is the base REF's tip, used only where the tip itself is the subject (the
  # root-checkout T1 diagnostic in the wrapper).
  #
  # RESOLVED FROM `$BASE_TIP_SHA`, NOT FROM `$BASE` AGAIN, and the census diff below is then
  # pinned to `$RANGE_BASE_SHA`: one read of the moving ref, one range, used by the census,
  # the assert and the waiver scope. That is what closes the SECOND-ORDER race the deterministic
  # bug was masking — a mirror ref that advances mid-run can otherwise leave the census
  # measuring one range while the assert expects another, and each would look correct alone.
  #
  # FAIL CLOSED, affirmatively. Unrelated histories (no common ancestor) exit non-zero with no
  # output; a broken/absent object store can fail in other ways. Either way the range under
  # review is UNKNOWN, and an unknown range must never degrade to the tip or to an empty value —
  # the tip is the very thing that produced the defect, and an empty expected base would make
  # `sha-assert` compare against nothing. So the permissive branch is keyed on the AFFIRMATIVE
  # value: a single 40-hex sha and nothing else (a multi-line `--all`-style answer, an empty
  # answer, or anything non-hex is a FAIL, because we could not tell WHICH base was reviewed).
  # Ordered BEFORE any review is enqueued, like every other census check, so a failure here
  # costs no review tokens.
  local mergebase_err="$LOG.mergebase.err"
  if ! RANGE_BASE_SHA=$(git -C "$REPO" merge-base "$BASE_TIP_SHA" HEAD 2>"$mergebase_err"); then
    CENSUS_CHECK="FAIL (no merge-base between '$BASE' and HEAD)"
    DETAILS+=("ERROR: census: 'git merge-base $BASE_TIP_SHA HEAD' failed in $REPO, so the BASE OF THE RANGE UNDER REVIEW is unknown and neither the census ('${BASE}...HEAD' is merge-base..HEAD) nor sha-assert would have anything sound to measure against. This is a FAIL, explicitly NOT a NOTHING-TO-REVIEW: an unresolvable merge-base is 'we cannot tell', never 'there is nothing to review'. The usual cause is that '$BASE' and HEAD have NO common ancestor (unrelated histories). It is deliberately NOT degraded to the tip of '$BASE': asserting a merge-base-relative reviewed range against a tip is the defect this resolution exists to fix. No review was enqueued. git said:")
    while IFS= read -r line; do
      [ -n "$line" ] || continue
      DETAILS+=("  $line")
    done <"$mergebase_err"
    finish FAIL 1
  fi
  # The AFFIRMATIVE form test: `range_base_form` only ever becomes `40-hex` by a positive
  # measurement (hex-only AND exactly 40 bytes), and the branch below keys on THAT value rather
  # than on "not one of the bad shapes" — a shape nobody anticipated therefore fails closed.
  # `*[!0-9a-f]*` catches a NEWLINE too, so a multi-line answer (several merge bases) cannot
  # slip through as a sha with trailing noise.
  local range_base_form="unusable"
  case "$RANGE_BASE_SHA" in
    *[!0-9a-f]*) ;;
    ?*) [ "${#RANGE_BASE_SHA}" -eq 40 ] && range_base_form="40-hex" ;;
  esac
  if [ "$range_base_form" != "40-hex" ]; then
    CENSUS_CHECK="FAIL (merge-base of '$BASE' and HEAD unusable)"
    DETAILS+=("ERROR: census: 'git merge-base $BASE_TIP_SHA HEAD' succeeded but did not yield exactly one 40-hex commit sha, so the base of the range under review is not established. A PASS here would rest on the ABSENCE of a non-zero exit rather than on a measurement, so it fails closed. What git returned, rendered between markers: [$RANGE_BASE_SHA]. No review was enqueued.")
    finish FAIL 1
  fi

  # `--no-renames` on purpose: with rename detection a renamed file is reported as the
  # composite path `dir/{old => new}.rs`, which is not a real path and so can never be
  # found in the reviewer's prompt (the prompt-content check below matches literal
  # paths). Splitting a rename into its delete+add pair keeps every census path a real
  # one, at the cost of counting it as two files.
  # A FAILED `git diff` must NEVER alias to "genuinely empty". Discarding the exit
  # status here would assert a measurement that never happened — the same epistemic
  # error as reading a mirror ref instead of the remote — and it would surface as
  # NOTHING-TO-REVIEW, i.e. "there is nothing to look at" instead of "we could not
  # tell". Capture the status and fail CLOSED on it.
  #
  # ============== THE CANONICAL PATH-NORMALISATION BOUNDARY (#3229) ==============
  # `-z` IS LOAD-BEARING, and this is the ONE place path normalisation happens.
  #
  # Without it git C-QUOTES any path containing a double quote, a backslash or a
  # non-ASCII byte (`"docs/\303\251 notes.md"`) — and EVERY consumer downstream then has
  # to remember to unquote before it classifies or compares. Three rounds of blockers
  # came from exactly that: the classification below read the extension of a QUOTED
  # spelling (`md"`, `json"`) and called PROSE code (#3229 round 4 F1), while
  # `prompt-content:` unquoted at a different point in a different way. Patching one
  # consumer per round is a losing game.
  #
  # So: `-z` makes the paths arrive RAW, `census_paths` / `census_code_paths` hold the RAW
  # bytes, and the RAW form is the SINGLE internal representation for classification,
  # comparison AND display. No consumer unquotes — `roborev_unquote_path` survives ONLY
  # for text we do NOT get from git plumbing (the reviewer's prompt, whose `diff --git`
  # headers are quoted by the producer), and it is called from exactly one place:
  # `roborev_diff_header_has_path`. `scripts/tests/test_roborev_review_guard.sh` asserts
  # that structurally, so a new consumer that re-implements unquoting FAILs `--lite`.
  #
  # `--numstat -z` emits one NUL-TERMINATED RECORD per file, `<add>\t<del>\t<path>`
  # (the rename form, `<add>\t<del>\tNUL<old>NUL<new>`, cannot occur under
  # `--no-renames`). Records are read with `read -d ''`, so a path containing a NEWLINE
  # survives intact — which a line-oriented read cannot do at all.
  local numstat_file="$LOG.numstat"
  set +e
  # `"$RANGE_BASE_SHA" HEAD` (two-dot) IS `"${BASE}...HEAD"` (three-dot) BY DEFINITION, with
  # the moving ref read exactly ONCE — see the merge-base resolution above. Every diagnostic
  # that says `${BASE}...HEAD` names this same range; the spelling here is pinned, the prose
  # there is the reader-facing name for it.
  git -C "$REPO" diff --numstat -z --no-renames "$RANGE_BASE_SHA" HEAD \
    >"$numstat_file" 2>"$numstat_file.err"
  DIFF_RC=$?
  set -e
  if [ "$DIFF_RC" -ne 0 ]; then
    CENSUS_CHECK="FAIL (git diff failed)"
    DETAILS+=("ERROR: census: 'git diff --numstat -z --no-renames $RANGE_BASE_SHA HEAD' (that is, ${BASE}...HEAD) exited $DIFF_RC in $REPO, so the census was never measured. This is a FAIL, explicitly NOT a NOTHING-TO-REVIEW — an unmeasurable diff is 'we cannot tell', never 'there is nothing to review'. git said:")
    while IFS= read -r line; do
      [ -n "$line" ] || continue
      DETAILS+=("  $line")
    done <"$numstat_file.err"
    finish FAIL 1
  fi

  census_files=0
  census_added=0
  census_deleted=0
  census_non_code_files=0
  census_paths=()
  census_unmeasurable_paths=()
  census_unmeasurable_detail=()
  local record add del path exec_state unmeasurable_row
  while IFS= read -r -d '' record; do
    [ -n "$record" ] || continue
    # Split the record by hand: the PATH is everything after the second TAB and may itself
    # contain a newline, so `read -r add del path` (which splits on IFS per LINE) cannot be
    # used here — the whole point of `-z` is that the path is not line-delimited.
    add="${record%%$'\t'*}"
    path="${record#*$'\t'}"
    del="${path%%$'\t'*}"
    path="${path#*$'\t'}"
    [ -n "$path" ] || continue
    if [ "$add" = "-" ]; then add=0; fi
    if [ "$del" = "-" ]; then del=0; fi
    census_files=$((census_files + 1))
    census_paths+=("$path")
    census_added=$((census_added + add))
    census_deleted=$((census_deleted + del))
    # Non-code classification: a documented prose EXTENSION, an ARTIFACT extension inside
    # an ARTIFACT-BEARING DIRECTORY (#3229 — mirroring the configured
    # `<artifact-dir>/**/*.<ext>` deny-list), or an EXTENSIONLESS file under a documented
    # prose directory. Anything else — including `docs/foo.py`,
    # `docs/reports/*-artifacts/**/*.sh`, `*.bt` and `.github/workflows/*.yml` — is CODE.
    # A `docs/` path PREFIX never makes a file non-code on its own, and since round 6 it
    # does not make an artifact EXTENSION non-code either: `docs/observability/**/*.json`
    # is functional config the gate's own `kit-dashboard-drift` component guards, so it is
    # CODE and must reach the reviewer.
    #
    # CLASSIFIED ON THE RAW PATH (#3229): `$path` came out of `--numstat -z`, so it is
    # never C-quoted and the extension/prefix tests below see the real bytes. Reading a
    # QUOTED spelling here is what made `docs/é notes.md` (extension `md"`, prefix
    # `"docs/`) classify as CODE, so `prompt-content:` demanded it in a prompt from which
    # the configured `*.md` had already removed it — a false FAIL.
    file_non_code=0
    ext=""
    case "$path" in *.*) ext="${path##*.}" ;; esac
    if [ -n "$ext" ]; then
      # shellcheck disable=SC2086 # deliberate split of the space-separated constant
      for candidate in $CODE_FREE_EXTENSIONS; do
        if [ "$ext" = "$candidate" ]; then file_non_code=1; fi
      done
      if [ "$file_non_code" -eq 0 ]; then
        artifact_dir=0
        if roborev_path_in_artifact_dir "$path"; then artifact_dir=1; fi
        if [ "$artifact_dir" -eq 1 ]; then
          # shellcheck disable=SC2086 # deliberate split of the space-separated constant
          for candidate in $CODE_FREE_ARTIFACT_EXTENSIONS; do
            if [ "$ext" = "$candidate" ]; then file_non_code=1; fi
          done
        fi
      fi
    else
      # EXTENSIONLESS under a prose prefix: non-code only when git MEASURED it NON-EXECUTABLE
      # AT EVERY ENDPOINT of the range. The prefix alone used to decide it, which made every
      # extensionless path under `docs/` a path `prompt-content:` asserted NOTHING about; the
      # ordered single-endpoint scan that replaced it did the same to a MODE CHANGE; and the
      # two-valued leaf under that scan's replacement did the same whenever `git ls-tree` itself
      # FAILED — see the rule documented at `CODE_FREE_EXTENSIONLESS_PREFIXES`. The mode is read
      # from the tree, on this same RAW `$path`, so the classification boundary is unchanged.
      #
      # THREE STATES, THREE DESTINATIONS — and only ONE of them is the permissive one:
      #   0 EXEC          -> CODE (fall through; `file_non_code` stays 0)
      #   1 NOT-EXEC      -> prose. A POSITIVE measurement at every endpoint.
      #   2 UNMEASURABLE  -> NEITHER. Recorded here and failed closed after the loop, because a
      #                     path whose mode was never measured must not be spent as prose: that
      #                     would drop it from `census_code_paths`, leave `prompt-content:`
      #                     asserting nothing about it, and print a green summary meaning
      #                     "nothing was wrong" when what happened is "nothing was checked".
      # shellcheck disable=SC2086 # deliberate split of the space-separated constant
      for prefix in $CODE_FREE_EXTENSIONLESS_PREFIXES; do
        case "$path" in
          "$prefix"*)
            exec_state=0
            roborev_path_exec_state "$path" || exec_state=$?
            if [ "$exec_state" -eq 1 ]; then file_non_code=1; fi
            if [ "$exec_state" -ne 0 ] && [ "$exec_state" -ne 1 ]; then
              census_unmeasurable_paths+=("$path")
              census_unmeasurable_detail+=("$path @ ${ROBOREV_EXEC_UNMEASURABLE_REFS[*]:-<no endpoint was consulted>}")
            fi
            ;;
        esac
      done
    fi
    if [ "$file_non_code" -eq 1 ]; then
    census_non_code_files=$((census_non_code_files + 1))
  else
    # The CODE subset is the part of the census we expect the reviewer to be sent:
    # roborev drops exactly what its exclusion pathspecs match — it makes NO code/non-code
    # judgement — and this repo's configured set is a prose/artifact deny-list mirroring the
    # classification above. That correspondence is NOT verified pre-enqueue (see the
    # exclusion note near the top of this file, and #3283); it is checked AFTER the fact by
    # `prompt-content:`, against the prompt the reviewer actually received.
    census_code_paths+=("$path")
  fi
  done <"$numstat_file"

  census_noun="files"
  if [ "$census_files" -eq 1 ]; then census_noun="file"; fi
  CENSUS="$census_files $census_noun, +$census_added/-$census_deleted"

  if [ "$census_files" -eq 0 ]; then
    CENSUS_CHECK="FAIL (empty census)"
    DETAILS+=("NOTHING-TO-REVIEW: the local diff census for '${BASE}...HEAD' is genuinely empty (0 files changed), so no review was enqueued. This is explicitly NOT a pass and MUST NOT be recorded as \"roborev clean\".")
    finish NOTHING-TO-REVIEW 3
  fi

  # --- step 3a: UNMEASURABLE MODE = FAIL CLOSED (#3229) -------------------------
  # Reached when `git ls-tree` itself FAILED at every endpoint a path's classification depends
  # on, so this run has NO measurement of whether that path is harness CODE or prose. It is
  # deliberately not allowed to fall through to either side. Same wording discipline as the
  # unresolvable base and the failed `git diff` above, and for the same reason: "we could not
  # check" must never be able to read as "nothing was wrong", and it is textually
  # indistinguishable from it unless it is said out loud. Surfaced on `census-check:` because
  # that key already fails closed on an unmeasurable census and is affirmation-checked.
  if [ "${#census_unmeasurable_paths[@]}" -gt 0 ]; then
    CENSUS_CHECK="FAIL (recorded mode unmeasurable for ${#census_unmeasurable_paths[@]} of $census_files census paths)"
    DETAILS+=("ERROR: census: the executable-bit classification could NOT BE MEASURED for ${#census_unmeasurable_paths[@]} extensionless census path(s): 'git ls-tree' FAILED in $REPO at every endpoint of '${BASE}...HEAD' that the decision needs, so this run does not know whether they are harness CODE or prose. This is a FAIL, explicitly NOT a quiet non-code classification and explicitly NOT a NOTHING-TO-REVIEW — an unmeasurable mode is 'we cannot tell', never 'there is nothing wrong'. Each row below names the PATH, then the endpoint REF(s) that could not be measured and git's own message:")
    for unmeasurable_row in "${census_unmeasurable_detail[@]}"; do
      DETAILS+=("  $unmeasurable_row")
    done
    DETAILS+=("ERROR: census: failing closed on an unmeasurable mode. No review was enqueued. This is an INFRA condition in the checkout being reviewed (an unreadable repository, a ref that does not resolve to a tree, a corrupt object), NOT a defect in the branch under review — repair the checkout and re-run.")
    finish FAIL 1
  fi
  CENSUS_CHECK="PASS"

  # --- step 3b: code-free census — a DETERMINISTIC FAIL, before any review ------
  # roborev structurally DISCARDS a code-free diff, so such a diff cannot be certified
  # by roborev at all (this change's own spec requirement, and CLAUDE.md rule 4). That
  # is a property of OUR census, measured locally — it must not depend on the reviewer
  # admitting it in prose, which is what the previous revision bet on.
  #
  # The MECHANISM, stated correctly (#3229): roborev drops exactly the paths its
  # CONFIGURED `exclude_patterns` match, applied as git pathspec exclusions — it makes
  # no code/non-code judgement. A markdown-only diff arrives EMPTY because `*.md` is
  # configured, not because the reviewer recognised prose; so its
  # "contains no code changes to review" is a TRUTHFUL report of an empty input, and
  # re-running or re-prompting can never change it. The earlier claim — that roborev
  # filtered out non-code paths by a judgement of its own — is FALSIFIED: under the
  # configured `docs/**` the very same mechanism discarded 33 EXECUTABLE files on PR
  # #3222, i.e. it excluded CODE. That is why the configuration was NARROWED (#3229) and why
  # `prompt-content:` reconciles the code census against the prompt actually delivered
  # rather than trusting the correspondence.
  if [ "$census_non_code_files" -eq "$census_files" ]; then
    CODE_FREE="FAIL (code-free census: $census_non_code_files/$census_files files are documentation/specification text)"
    DETAILS+=("ERROR: code-free: every file in the census ($CENSUS for ${BASE}...HEAD) is documentation/specification prose, and roborev STRUCTURALLY DISCARDS a code-free diff — so this diff CANNOT be certified by roborev at all, whatever verdict it returns. The sanctioned substitute is primary-source verification recorded in the PR (for example 'git show cassandra-5.0.8:<path>' for the source the docs describe). A docs-only change must NEVER record \"roborev clean\".")
    DETAILS+=("ERROR: code-free: no review was enqueued, because a passing verdict on this diff would be meaningless.")
    finish FAIL 1
  fi
  CODE_FREE="PASS"
  }

# roborev_unquote_path <token>: render a git C-QUOTED path token back to its RAW bytes.
#
# THE ONE UNQUOTING IMPLEMENTATION, and it has exactly ONE caller:
# `roborev_diff_header_has_path`. Everything the wrapper gets from git PLUMBING is read
# with `-z` and is therefore already raw (the census's `--numstat -z`), so no census path is
# ever quoted and nothing on those paths needs this function. What DOES arrive quoted is text produced by SOMEONE
# ELSE: the reviewer's prompt, whose `diff --git` headers are written by roborev's own
# `git diff` with quoting ON and no `-z` available to us.
#
# That division is the #3229 canonical boundary: normalise once where the bytes enter,
# keep RAW as the single internal representation, and never let a second consumer grow
# its own unquoting. The guard suite asserts the single-caller property structurally,
# because "one more consumer normalises slightly differently" is what produced a blocker
# in each of rounds 2, 3 and 4.
#
# The paths this exists for are real: this repo tracks
# `docs/research/CQLite Writes (M5) — Analysis & Recommended Paths.md` and 40
# space-bearing paths under `docs/`.
#
# IT RETURNS THROUGH A NAMED GLOBAL (`_rx_unquoted`), NEVER THROUGH `$(...)`. Command
# substitution STRIPS every trailing newline, so a tracked path ending in a `\012`
# escape (`weird\n`) would come back a byte short — and a short path both mis-compares
# against the `-z` census set AND can COLLIDE with a sibling that really is the
# shorter name. `printf -v` is used for the octal expansion for the same reason.
roborev_unquote_path() {
  local p="$1" out="" i n ch oct
  _rx_unquoted="$p"
  case "$p" in
    '"'*'"') ;;
    *) return 0 ;;
  esac
  p="${p:1:${#p}-2}"
  n=${#p}
  i=0
  # A HAND-ROLLED scan, deliberately: `printf '%b'` does NOT expand `\"` (it is not an
  # `echo -e` escape), so it would leave the backslash in place — which is exactly the
  # mis-comparison this helper exists to prevent. Git's `quote_c_style` emits only
  # \a \b \f \n \r \t \v \" \\ and \nnn octal, all of which are handled here.
  while [ "$i" -lt "$n" ]; do
    ch="${p:$i:1}"
    if [ "$ch" != '\' ]; then
      out+="$ch"
      i=$((i + 1))
      continue
    fi
    i=$((i + 1))
    ch="${p:$i:1}"
    case "$ch" in
      '"' | '\' | '/') out+="$ch"; i=$((i + 1)) ;;
      a) out+=$'\a'; i=$((i + 1)) ;;
      b) out+=$'\b'; i=$((i + 1)) ;;
      f) out+=$'\f'; i=$((i + 1)) ;;
      n) out+=$'\n'; i=$((i + 1)) ;;
      r) out+=$'\r'; i=$((i + 1)) ;;
      t) out+=$'\t'; i=$((i + 1)) ;;
      v) out+=$'\v'; i=$((i + 1)) ;;
      [0-7]) printf -v oct '%b' "\\0${p:$i:3}"; out+="$oct"; i=$((i + 3)) ;;
      *) out+="$ch"; i=$((i + 1)) ;;
    esac
  done
  _rx_unquoted="$out"
}

# _rx_take_quoted_token <string>: the string STARTS with a `"`; split off the complete
# C-quoted token (quotes included) into `_rx_qtok` and the remainder into `_rx_rest`.
# Returns 1 when the token is unterminated (a malformed header — never guessed at).
#
# The scan is what makes a quoted side UNAMBIGUOUS: a C-quoted body never contains an
# unescaped `"`, so the first one not preceded by a backslash ends the token, whatever
# spaces the path carries.
_rx_take_quoted_token() {
  local s="$1" i=1 n ch
  n=${#s}
  _rx_qtok=""
  _rx_rest=""
  while [ "$i" -lt "$n" ]; do
    ch="${s:$i:1}"
    if [ "$ch" = '\' ]; then
      i=$((i + 2))
      continue
    fi
    if [ "$ch" = '"' ]; then
      _rx_qtok="${s:0:i+1}"
      _rx_rest="${s:i+1}"
      return 0
    fi
    i=$((i + 1))
  done
  return 1
}

# roborev_collect_prompt_headers <prompt-file>: read the prompt's `diff --git` headers
# TOGETHER WITH the `rename from` / `rename to` (and `copy from` / `copy to`) lines that
# DISAMBIGUATE them, into three parallel arrays `_rx_hdrs` / `_rx_hdr_from` / `_rx_hdr_to`.
#
# WHY THE FOLLOWING LINES ARE PART OF THE INPUT (#3229 round 5, blocker 1). A `diff --git`
# header alone is IRREDUCIBLY AMBIGUOUS once a path may contain a space: `a/foo b/x b/foo b/x`
# is both the non-rename of a file named `foo b/x` and a rename of `foo` to `x b/foo b/x`,
# and no amount of care applied to the header LINE can tell them apart. Git, however, does
# not leave renames ambiguous ELSEWHERE: for a rename or copy it always writes
# `rename from <path>` / `rename to <path>` immediately after the header — ONE path per
# line, C-quoted when needed, so each is exactly decidable. Those lines are the authority
# the matcher resolves against; see `roborev_diff_header_has_path`.
#
# The extended-header run is BOUNDED: only the lines git emits between `diff --git` and the
# `index`/`---`/`@@` body count, so a `rename from` sitting in the reviewer's PROSE (or in a
# diff BODY line, which always begins with a space/`+`/`-`/`@`/`\`) is never attributed to a
# header. `awk`, not a bash line loop: a prompt is megabytes.
#
# Reading the headers LINE-ORIENTED is sound HERE, and only here: git quotes a
# newline-bearing path inside a `diff --git` header and inside a `rename from`/`rename to`
# line, so each is genuinely one line. Git PLUMBING output gets `-z` instead (see the
# census).
roborev_collect_prompt_headers() {
  local f="$1" _h _f _t
  _rx_hdrs=()
  _rx_hdr_from=()
  _rx_hdr_to=()
  # THE ONLY FILE THIS EVER READS IS THE WRAPPER'S OWN PROMPT FILE, written by this run beside its transcript
  # — under C⁗ no snapshot is ever read, so this function is never pointed at a path roborev controls. Its
  # falsity yields an EMPTY header set, which is the fail-closed direction (every census path then reads
  # absent) rather than an absence claim about anything.
  [ -f "$f" ] || return 0
  while IFS= read -r _h && IFS= read -r _f && IFS= read -r _t; do
    _rx_hdrs+=("$_h")
    _rx_hdr_from+=("$_f")
    _rx_hdr_to+=("$_t")
  done < <(LC_ALL=C awk '
      function flush() { if (h != "") { print h; print f; print t } }
      /^diff --git / { flush(); h = $0; f = ""; t = ""; ext = 1; next }
      ext && /^rename from / { f = substr($0, 13); next }
      ext && /^rename to /   { t = substr($0, 11); next }
      ext && /^copy from /   { f = substr($0, 11); next }
      ext && /^copy to /     { t = substr($0, 9);  next }
      ext && /^(similarity index |dissimilarity index |old mode |new mode |new file mode |deleted file mode |index )/ { next }
      { ext = 0 }
      END { flush() }
    ' "$f" 2>/dev/null)
  return 0
}

# roborev_diff_header_has_path <diff-git-header-line> <RAW census path> [<from-path-token>]
#   [<to-path-token>]: true when the header names that path on EITHER side.
#
# The two optional trailing arguments are the header's OWN `rename from`/`rename to`
# (or `copy from`/`copy to`) path tokens, exactly as they appear in the prompt (still
# C-quoted if git quoted them, and WITHOUT any `a/`/`b/` prefix — git does not write one on
# those lines). `roborev_collect_prompt_headers` supplies them.
#
# THE ONLY WAY a consumer may ask "is this census path in the prompt?" (#3229 round 4,
# blockers F2 + F3). It is defined HERE, beside the boundary it belongs to, because the
# previous arrangement — a regex path-set built in `roborev-review-checks.sh` and probed
# with `grep -Fxq` over a NEWLINE-delimited file — was wrong in three independent ways,
# each of which had to be found by a separate review round:
#
#   * `^diff --git a/[^ ]+ b/[^ ]+$` cannot split a SPACE-bearing header, and there is no
#     regex that can: `a/x y b/z w` has several readings. So no regex is used at all.
#   * requiring BOTH sides quoted missed the MIXED shape `diff --git a/ascii "b/quoted"`,
#     which git emits when only one side needs quoting — a shape that occurs ONLY on
#     renames, and renames are ON in the reviewer's diff while our census runs
#     `--no-renames`. Both were therefore reported ABSENT: a false FAIL on the strongest
#     anti-vacuity key the wrapper has.
#   * a NEWLINE-delimited path set + `grep -Fxq` turns a path containing a newline into
#     ALTERNATIVES, so `a` present "proved" `a<LF>b.rs` present — a false PASS. Membership
#     is decided here, in bash, per header, with no delimiter anywhere.
#
# RESOLUTION ORDER — ambiguity is resolved from EVIDENCE, never positionally (#3229
# round 5, blocker 1):
#   0. the `rename from` / `rename to` lines, when the prompt carried them. AUTHORITATIVE
#      and exact: one path per line, C-quoted when needed. The header is not consulted at
#      all, because those lines ARE the header's two paths.
#   1. `diff --git "a/<q>" "b/<q>"`  — both quoted: both sides parsed exactly.
#   2. `diff --git "a/<q>" b/<raw>`  — a-side quoted, b-side literal to end of line.
#   3. `diff --git a/<raw> "b/<q>"`  — an UNQUOTED side never contains a `"` (git would
#      have quoted it), so the FIRST `"` begins the b-side token.
#   4. `diff --git a/<raw> b/<raw>`  — no side is parseable as a quoted token, and NO
#      rename/copy lines were supplied. EVERY valid split of the line into `a/<A> b/<B>`
#      is enumerated (candidates only; `$want` is byte-compared, never used as a pattern,
#      so a path containing `*`/`?`/`[` is matched literally), and then:
#        4a. if SOME split has A == B, the header is a NON-RENAME — git ALWAYS accompanies
#            a rename/copy with `rename from`/`rename to`, and none were supplied — so
#            ONLY the equal reading is accepted. This is what closes the FALSE PASS below.
#        4b. if NO split has A == B, the line can only be a rename/copy whose
#            `rename from`/`rename to` lines did NOT reach us, so any valid split counts.
#            See THE DECLARED RESIDUAL.
#
# THE FALSE PASS 4a CLOSES, measured: `roborev_diff_header_has_path 'diff --git a/foo b/x
# b/foo b/x' foo` used to return PRESENT. The old test `case $rest in "a/$want b/"*)` is a
# PREFIX test, and `a/foo b/` is a prefix of that header — so a repo tracking a file named
# `foo b/x` made the UNRELATED census path `foo` read as delivered. That is a false PASS in
# `prompt-content:`, the strongest anti-vacuity key the wrapper has and the exact mechanism
# that certifies "the reviewer received the code". The comment that used to sit at shape 3's
# fall-through asserted this was impossible ("can only ever match a header that LITERALLY
# carries `a/<want> b/` … widens nothing unsound"). It was WRONG: `a/<want> b/` occurs as a
# PREFIX of a DIFFERENT path's header. The reasoning is corrected here rather than deleted,
# because a false safety claim in a comment is worse than no comment.
#
# WHY NOT FAIL CLOSED ON AN AMBIGUOUS HEADER. Because with renames ON the header ambiguity
# is IRREDUCIBLE — `a/foo b/(bar b/foo b/bar)` is a legal reading — so refusing to decide
# would red EVERY space-bearing header and reintroduce the false-FAIL blockers of rounds 3
# and 4, pinned by cases (cx6c), (cx6g) and (cx6h). A guard that fires on correct input is
# the guard that gets disabled.
#
# THE DECLARED RESIDUAL, bounded and stated rather than implied: branch 4b is PERMISSIVE.
# When no split is equal and no rename/copy lines were supplied, a `$want` that happens to
# be one side of SOME valid split reads as PRESENT even if the producer meant a different
# split. It is reachable only for a header that (i) carries a space, (ii) has two DIFFERENT
# paths, i.e. is a rename/copy, and (iii) arrived WITHOUT the rename/copy lines git always
# writes — so for git's own output it is unreachable, and it is the price of keeping
# (cx6g)/(cx6h) green. Anything git emits resolves at step 0 or by equality.
roborev_diff_header_has_path() {
  local hdr="$1" want="$2" from_tok="${3:-}" to_tok="${4:-}" rest a_raw b_raw
  case "$hdr" in 'diff --git '*) ;; *) return 1 ;; esac
  rest="${hdr#diff --git }"

  # step 0: git's own disambiguation. Both lines must be present — one alone does not
  # identify the pair, so a truncated run falls through to the header shapes instead.
  if [ -n "$from_tok" ] && [ -n "$to_tok" ]; then
    roborev_unquote_path "$from_tok"
    [ "$_rx_unquoted" != "$want" ] || return 0
    roborev_unquote_path "$to_tok"
    [ "$_rx_unquoted" != "$want" ] || return 0
    return 1
  fi

  if [ "${rest:0:1}" = '"' ]; then
    _rx_take_quoted_token "$rest" || return 1
    roborev_unquote_path "$_rx_qtok"
    a_raw="${_rx_unquoted#a/}"
    [ "$a_raw" != "$want" ] || return 0
    rest="${_rx_rest# }"
    if [ "${rest:0:1}" = '"' ]; then
      _rx_take_quoted_token "$rest" || return 1
      roborev_unquote_path "$_rx_qtok"
      b_raw="${_rx_unquoted#b/}"
    else
      b_raw="${rest#b/}"
    fi
    [ "$b_raw" = "$want" ] && return 0
    return 1
  fi

  case "$rest" in
    *'"'*)
      # shape 3: unquoted a-side (git would have quoted one holding a `"`), quoted b-side.
      a_raw="${rest%%\"*}"
      a_raw="${a_raw% }"
      a_raw="${a_raw#a/}"
      [ "$a_raw" != "$want" ] || return 0
      roborev_unquote_path "\"${rest#*\"}"
      b_raw="${_rx_unquoted#b/}"
      [ "$b_raw" = "$want" ] && return 0
      # NO early `return 1`: fall through to shape 4. A `"` in the line does not PROVE a
      # quoted b-side — a producer that emits a quote-bearing path UNQUOTED (git quotes it;
      # not every diff we are handed is git's own) yields a header whose only reading is by
      # split enumeration. Case (cx6) is exactly that shape, and it resolves at 4a by
      # equality.
      ;;
  esac

  # shape 4: no side is parseable as a quoted token and no rename/copy lines were supplied.
  # Enumerate EVERY valid split of the line into `a/<A> b/<B>` and prefer the EQUAL reading
  # (4a) over any unequal one (4b) — see the RESOLUTION ORDER above for why that is the
  # sound direction and where the residual is.
  case "$rest" in 'a/'*) ;; *) return 1 ;; esac
  local body="${rest#a/}" n i a_cand b_cand
  local eq_seen=0 eq_match=0 any_match=0
  n=${#body}
  for ((i = 0; i + 3 <= n; i++)); do
    [ "${body:$i:3}" = ' b/' ] || continue
    a_cand="${body:0:$i}"
    b_cand="${body:$((i + 3))}"
    if [ "$a_cand" = "$b_cand" ]; then
      eq_seen=1
      [ "$a_cand" != "$want" ] || eq_match=1
    elif [ "$a_cand" = "$want" ] || [ "$b_cand" = "$want" ]; then
      any_match=1
    fi
  done
  if [ "$eq_seen" -eq 1 ]; then
    # 4a: a non-rename. Only the equal reading is admissible.
    [ "$eq_match" -eq 1 ] && return 0
    return 1
  fi
  # 4b: a rename/copy whose `rename from`/`rename to` lines did not reach us.
  [ "$any_match" -eq 1 ] && return 0
  return 1
}

# ===================== THE ABSENCE WAIVER (issue #3312, owner ruling (4)) =====================
# WHY THIS EXISTS, AND WHAT IT REPLACED. `prompt-content:` asks exactly ONE question — are the census
# CODE paths present in the prompt the reviewer was sent? PRESENT is a PASS; ABSENT is a FAIL,
# unconditionally. There is deliberately NO CLASSIFIER: the wrapper used to infer HOW roborev
# delivered the diff (inline / snapshot path / delegated tier) from the prompt TEXT, and four
# consecutive review rounds each found a High-severity false verdict in that inference — a header set
# consulted before an oversize marker, a candidate outliving its block, a delivery under an
# unrecognised heading producing no evidence, and a block opener keyed on heading text that roborev
# treats as caller DATA. The instances differed; the cause did not. roborev's prompt EMBEDS
# repository-controlled content at column zero, so structure inferred from that text is spoofable in
# both directions, and no marker was found that terminates the sequence: the only structural one was
# roborev's fenced diff, and repository content can contain fences too. The owner therefore ruled the
# inference out of existence rather than patching its fifth instance.
#
# THE COST THAT RULING ACCEPTS, stated plainly: a review whose diff roborev delivered BY PATH has no
# census paths in its prompt, so it now FAILs exactly like a review that received nothing at all.
# **To the machine those two are IDENTICAL.** That is not a limitation to be engineered away here; it
# is the trade being chosen over a machine guessing from injectable text. What distinguishes them is a
# HUMAN plus the review's token accounting (genuine reviews measured 398k–649k input / 314k–554k
# cached; the vacuous baseline is ~18.7k input / 0 cached), which is why the escape hatch is a
# human-authorized waiver and not another inference.
#
# ===================== THE WAIVER'S THREAT MODEL, WITH ITS LIMITS (#3312) =====================
# STATED because five consecutive review rounds landed in this authorization path, and the next finding of
# the form "the invoker can bypass this" needs to be TRIAGED rather than patched.
#
# A HOSTILE INVOKER CANNOT BE DEFENDED AGAINST HERE, AT ALL. Whoever runs this wrapper can edit it,
# replace the scanner file beside it, shadow `gh` or `python3` on PATH, or skip the wrapper entirely and
# hand-write a `==== ROBOREV REVIEW SUMMARY ==== … RESULT: PASS` block into the PR. No check inside a
# process defends against the party that controls the process, and claiming otherwise would be the exact
# false-assurance shape this issue exists to remove. The merge gate's real protection against a hostile
# WORKER is the audit trail plus a human reading the PR — not this code.
#
# WHAT IT DOES DEFEND, which is what makes it worth having:
#   (1) PARTIES WHO DO NOT CONTROL THE INVOCATION. On a public repository anyone can comment on a PR, and
#       the base/head/job values are printed in the failing block; the allowlist, the anchored marker and
#       the structured (non-forgeable) author association are what stop a stranger granting a waiver.
#   (2) ACCIDENT AND DRIFT — the larger category in practice. A pasted summary block, a quoted example, a
#       stale waiver riding to a later review, a re-run inheriting an authorization written for a different
#       job, a placeholder reason left unsubstituted. Every fix in this path landed in (1) or (2).
#
# THE TRIAGE RULE THAT FOLLOWS, so a future round does not spend itself here:
#   * "the INVOKER can bypass this"      -> OUT OF MODEL. Record it; do not patch it.
#   * "a NON-INVOKER can bypass this"    -> DEFECT.
#   * "this can be bypassed BY ACCIDENT" -> DEFECT.
# Same-host actors that can write the roborev database or the scripts are invoker-class, not third parties.
#
# CHEAP HARDENING IS STILL WORTH IT even where an invoker could reach the same end another way: removing
# the scanner-path env override cost nothing, removed a footgun, and closes contexts where the environment
# is influenced while files are not (a workflow injecting a variable). "Theoretically redundant" is not a
# reason to leave a hole that a non-invoker or an accident can walk through.
#
# TWO RESIDUALS INSIDE THE MODEL, named rather than implied:
#   * THE MARKER IS READ FROM TOP-LEVEL PR COMMENTS ONLY (`gh pr view --json comments`), AND THE MOST
#     PROBABLE MISPLACEMENT IS THE LINKED ISSUE THREAD (#3759). Three locations are not read: a marker
#     posted on the PR's LINKED ISSUE — the likeliest, because that is where lane/lead coordination
#     lives — inside a REVIEW body, or as a review-thread reply. None of them applies, and the FAIL
#     stands. MEASURED: for PR #3710 both authorizations were granted, field-perfect, on issue #3544,
#     and the run reported `waiver: NONE` / `deferral: NONE` — textually identical to "the lead
#     refused" and to "nobody posted one". Position 1 of a six-PR queue idled ~8 hours.
#     SINCE #3759 THE LINKED-ISSUE CASE IS DIAGNOSED, NOT GRANTED: when the PR-side scan returns
#     `none`, the PR's linked issue(s) are scanned with the SAME scanner and the SAME scope, and a
#     marker there that WOULD have been accepted by the channel is reported `waiver: MISPLACED (found
#     on linked issue #N …)` naming the issue and the remedy. `MISPLACED` GRANTS NOTHING — not
#     partially, not with a notice — and the FAIL stands; only a marker on the PULL REQUEST grants,
#     and moving it there is a HUMAN act by the authorizer. A `none` verdict now also DECLARES whether
#     the probe ran, so "checked and not there" and "never checked" can never read alike.
#     LEAD-SIDE PROCEDURE, the other half of the fix: after posting either marker, verify with
#     `gh pr view <PR> --json comments` that the line is ON THE PR. A grant is only granted once it is
#     readable by the scanner that reads it.
#   * AN AUTHORIZED HUMAN CAN AUTHORIZE CARELESSLY — pre-authorizing a job id, or waiving without checking
#     the token accounting. Nothing here can detect that; the control is the permanent, attributable
#     comment, which is why the reason is required and recorded verbatim.

# ===== WHO MAY GRANT: AN EXPLICIT AUTHOR ALLOWLIST (roborev job 25) =====
# THE HOLE THIS CLOSES, and it is the permissive shape this whole issue is about: the comment author was
# RECORDED but never AUTHORIZED, so on a PUBLIC repository ANY commenter could copy the base/head/job
# values out of the failing block — they are printed in it — and make the merge gate pass. The residual
# had been written as "we cannot distinguish the owner from the worker on a shared GH_TOKEN", which
# conflated **cannot enforce perfectly** with **cannot enforce at all**, and so absence of a perfect
# check became absence of ANY check.
#
# HARD-CODED HERE, NOT IN A CONFIG FILE, and deliberately with NO env override:
#   * one visible location, in the file whose review this mechanism is part of — a reviewer reading the
#     waiver code sees who may grant, in the same diff;
#   * an env override (or a path the caller names) would be settable by whoever invokes the wrapper,
#     i.e. by the very party the allowlist exists to constrain;
#   * a separate committed config buys nothing here — it is equally repo-controlled — while adding a
#     read path with its own absent/unreadable failure modes to get wrong.
# Changing it is a code change: edit this list, and the diff is reviewed like any other.
#
# WHAT IT DOES NOT DO, stated because the previous over-broad disclaimer is what invited the hole: it
# stops third parties. It does NOT distinguish WHICH allowlisted human posted the comment — on this fleet
# the worker, the closer and the owner all post through the same login — so "only the owner or the
# coordination lead may GRANT, a worker may only REQUEST" remains a process obligation with an audit
# trail, now enforced to the level of "an allowlisted human", not to the level of "that specific human".
ROBOREV_WAIVER_AUTHORS="pmcfadin"
# ===== THE ENFORCER IS RESOLVED FROM THIS FILE'S OWN DIRECTORY, NEVER FROM THE ENVIRONMENT =====
# (roborev job 27.) This used to carry a `${WAIVER_SCAN_TOOL:-…}` override, which handed the same hole
# outward that the allowlist itself closes: THE CONSTRAINED PARTY MUST NOT CHOOSE ITS OWN ENFORCER.
# Hardening a check while leaving its INVOCATION configurable moves the hole rather than closing it — an
# invoker could point this at a script printing `state=granted` and turn an absent prompt into a PASS with
# no authorized comment anywhere. There is deliberately no override, no fallback and no `${…:-…}` here, and
# a structural assert fails if one reappears. A test that needs a different scanner SUBSTITUTES THE FILE in
# its own scratch copy of `scripts/flow/` — replacing the artifact, not redirecting the path — so the
# production resolution stays single, literal and unreachable from any invocation.
WAIVER_SCAN_TOOL="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/roborev-waiver-scan.py"

# ===== NO SHELL-SIDE AUTHORIZATION HELPER (roborev job 229; #3312 job 26/27) =====
# A `roborev_waiver_author_allowed <login>` shell predicate used to sit here. It had ZERO callers
# repo-wide — the decision moved into the structured scanner at #3312 job 26, where author and body stay
# separate FIELDS of one object so no body can forge its author — and it is DELETED rather than left
# dormant. A live-looking allowlist check next to `ROBOREV_WAIVER_AUTHORS` is an invitation: a future
# edit calling it instead of the scanner would reintroduce a SECOND, shell-side authorization path, and
# two implementations of an authorization rule drift, and a drift in an authorization rule is a bypass.
# The allowlist is still expressed once, above, and consumed only by the scanner it is passed to.


# ============ THE LINKED-ISSUE MISPLACEMENT PROBE (issue #3759) ============
# WHY IT EXISTS, measured rather than imagined. For PR #3710 the coordination lead granted BOTH
# authorizations — field-perfect, correct base/head/job, each the sole nonblank content of its own
# top-level comment, from an allowlisted author — on ISSUE #3544, the thread where that lane's
# coordination had happened all day. The wrapper reads PR comments only, so it reported
# `waiver: NONE` / `deferral: NONE`, which is TEXTUALLY IDENTICAL to "the lead refused" and to
# "nobody ever posted one". Position 1 of a six-PR serial queue idled ~8 hours and blocked five
# lanes. The channel behaved exactly as designed; the DIAGNOSTIC could not tell the operator which
# of three very different situations they were in.
#
# WHAT THIS ADDS, AND WHAT IT DELIBERATELY DOES NOT. It adds one DIAGNOSTIC state, `misplaced`. It
# GRANTS NOTHING — not partially, not with a notice — and no channel rule is loosened to produce
# it: the allowlist, the sole-nonblank-content rule, the column-zero anchor, the structured author
# association, the placeholder refusal and the base+head+job binding are all untouched, and the
# security property ONLY A MARKER ON THE PULL REQUEST GRANTS is preserved exactly. Copying a marker
# from an issue onto the PR is a HUMAN act by the authorizer; this code only tells them to do it.
#
# ONE ENFORCER, INHERITED BY CALL. `roborev-waiver-scan.py` is unmodified and is passed the SAME
# kind, base, head, job, allowlist (and observed count) as the PR-side call. It is already
# thread-agnostic — it consumes `{"comments":[{"author":{"login":…},"body":…}]}` on stdin and knows
# nothing about pull requests — and `gh issue view <N> --json comments` emits that shape
# BYTE-IDENTICALLY to `gh pr view --json comments` (measured live on issue #3626, 2026-09-01). That
# measurement is what LICENSES the reuse: had the shapes differed, the options would have been a
# translation layer (a new component inside an authorization path) or a second scanner — and a
# second implementation of a marker grammar is a second place for it to diverge, which in an
# AUTHORIZATION grammar is a bypass (#3626's "reuse, do not reinvent" ruling). The scanner is never
# told which thread its input came from: thread identity is the CALLER's knowledge, so the
# `misplaced` state is assigned HERE and the scanner's contract stays exactly "given these
# comments, does an authorization for this review exist in them?".
#
# NEVER RETURNS NON-ZERO AND NEVER EXITS. A two-valued return would re-import the very collapse
# this change exists to remove, so every failure is a STATE WITH A CAUSE, from a CLOSED set of four
# outcomes: `misplaced` / `checked` / `no-subject` / `could-not-check`. A partial read — one thread
# read, another unavailable — is `could-not-check` naming BOTH halves and is NEVER `checked`: a
# partial scan reported as a complete one is worse than an admitted failure, because it is the
# version nobody re-checks.
#
# Sets, and never returns non-zero:
#   ROBOREV_PROBE_OUTCOME  misplaced | checked | no-subject | could-not-check
#   ROBOREV_PROBE_ISSUE    the issue number, on `misplaced` only
#   ROBOREV_PROBE_DETAIL   the rendering for this outcome, from the closed set above

# THE BOUND. The probe is a DIAGNOSTIC on a path that has already determined the run FAILs, so it
# must not become an unbounded fan-out of network calls; when the declared set exceeds this, the
# rendering says so rather than leaving the unprobed remainder silent. Named, not inline, because
# the number appears in the rendering the operator reads.
ROBOREV_LINKED_ISSUE_PROBE_MAX=3

roborev_linked_issue_marker_probe() { # <kind> <base> <head> <job> [<observed-findings-count>]
  local kind="$1" base="$2" head="$3" job="$4" observed="${5:-}"
  local rel_errfile rel_json rel_errtext numbers declared probed=0
  local issue read_ok=() unread=() comments result state scan_rc
  local issue_errfile issue_errtext
  ROBOREV_PROBE_OUTCOME="could-not-check"
  ROBOREV_PROBE_ISSUE=""
  ROBOREV_PROBE_DETAIL="the linked-issue thread could NOT be checked: the probe was never asked"
  if ! command -v gh >/dev/null 2>&1; then
    ROBOREV_PROBE_DETAIL="the linked-issue thread could NOT be checked: 'gh' is not on PATH"
    return 0
  fi
  if ! command -v python3 >/dev/null 2>&1 || [ ! -f "$WAIVER_SCAN_TOOL" ]; then
    ROBOREV_PROBE_DETAIL="the linked-issue thread could NOT be checked: the structured authorization scanner is unusable, and a marker is NEVER recognised from a flattened text stream — not even to print a diagnostic, because a second recogniser is a second place for the grammar to diverge"
    return 0
  fi
  if ! rel_errfile="$(mktemp 2>/dev/null)"; then
    ROBOREV_PROBE_DETAIL="the linked-issue thread could NOT be checked: no temporary file could be created to capture the 'gh' diagnostic"
    return 0
  fi
  # ===== THE LINKED ISSUE COMES FROM THE STRUCTURED RELATION, NEVER FROM THE PR BODY (#3626) =====
  # #3626 DELETED a PR-body link requirement, and not because Markdown is hard to parse: A PULL-REQUEST
  # BODY IS EDITABLE AT ANY TIME BY ANYONE WITH WRITE ACCESS, WITH NO PER-EDIT ATTRIBUTION, while a
  # top-level comment is permanent and attributable — so the body was the WEAKER ARTIFACT and would
  # stay weaker even if Markdown parsed trivially. Reinstating a body scan FOR ANY PURPOSE would be
  # reinstating a deleted generation, so this reads the relation and the guard suite asserts that no
  # `--json body` read and no `#N` prose scan came back.
  #
  # ===== THE MUTABLE-DERIVED BOUNDARY, WRITTEN HERE BECAUSE THE NEXT EDIT READS THE CODE FIRST =====
  # `closingIssuesReferences` is itself derived from the body's closing keywords, so it is ALSO
  # mutable by anyone with write access. That is acceptable HERE AND ONLY HERE for one precise
  # reason: THE RESULT GRANTS NOTHING. It selects WHICH THREAD TO PRINT A DIAGNOSTIC ABOUT. The worst
  # outcome from a re-pointed relation is a diagnostic naming the wrong issue, or naming none, and the
  # run FAILs either way. THE MOMENT ANY CONSUMER DOWNSTREAM OF THIS RELATION COULD GRANT, THIS
  # ARGUMENT EVAPORATES AND THE RELATION MUST GO WITH IT.
  #
  # ===== A SEPARATE, LATER CALL — THE GRANTING PAYLOAD DOES NOT CHANGE SHAPE (#3759 R4) =====
  # This is deliberately NOT folded into the caller's `gh pr view --json comments` as
  # `--json comments,closingIssuesReferences`. Two reasons, the first decisive. (1) THE PAYLOAD AN
  # AUTHORIZATION IS DECIDED FROM MUST NOT CHANGE SHAPE AS A SIDE EFFECT OF ADDING A DIAGNOSTIC: that
  # document is the scanner's input, and the fixed, measured shape is exactly what licenses reusing
  # the scanner unmodified. (2) THE PROBE MUST BE REACHABLE ONLY FROM A BRANCH THAT HAS ALREADY
  # FAILED TO GRANT: fetching the relation up front would make its data available on every path
  # including the granted one, so reachability would rest on where an `if` sits rather than on the
  # data not existing. Issued as its own later call, the ordering is STRUCTURAL — on any other state
  # the call is NOT MADE, not merely ignored, which is also the only version an invocation-log assert
  # can measure. The extra round-trip on a failing run is the accepted cost.
  if ! rel_json="$(cd "$REPO" && gh pr view --json closingIssuesReferences 2>"$rel_errfile")"; then
    rel_errtext="$(tr -d '\r' <"$rel_errfile" | tr '\n' ' ')"
    rm -f "$rel_errfile"
    ROBOREV_PROBE_DETAIL="the linked-issue thread could NOT be checked: 'gh pr view --json closingIssuesReferences' failed (${rel_errtext:-no diagnostic was produced})"
    return 0
  fi
  rm -f "$rel_errfile"
  # ===== EVERY NUMBER IS VALIDATED AFFIRMATIVELY, AND A NON-NUMBER IS NEVER INTERPOLATED RAW =====
  # The payload is remote text. The python leg reduces each entry to DIGITS or to the fixed token
  # `NON-NUMERIC` — it never echoes an unrecognised value — and the shell then re-tests each token
  # for digits itself. Two affirmative tests rather than one because the shell is what interpolates
  # into an emitted diagnostic, and a value that reaches a renderer must have been judged by the
  # process that renders it.
  if ! numbers="$(printf '%s' "$rel_json" | python3 -c '
import json, sys
try:
    data = json.load(sys.stdin)
except ValueError:
    sys.exit(1)
refs = data.get("closingIssuesReferences") if isinstance(data, dict) else None
if refs is None:
    refs = []
if not isinstance(refs, list):
    sys.exit(1)
for ref in refs:
    n = ref.get("number") if isinstance(ref, dict) else None
    if isinstance(n, bool):
        n = None
    if isinstance(n, int):
        sys.stdout.write("%d\n" % n)
    elif isinstance(n, str) and n.isdigit() and n:
        sys.stdout.write("%s\n" % n)
    else:
        # NEVER the raw value: an unrecognised entry is reported as a KIND, not as text.
        sys.stdout.write("NON-NUMERIC\n")
' 2>/dev/null)"; then
    ROBOREV_PROBE_DETAIL="the linked-issue thread could NOT be checked: the closingIssuesReferences payload could not be parsed"
    return 0
  fi
  declared=0
  for issue in $numbers; do declared=$(( declared + 1 )); done
  if [ "$declared" -eq 0 ]; then
    ROBOREV_PROBE_OUTCOME="no-subject"
    ROBOREV_PROBE_DETAIL="no linked issue is declared on this PR, so no linked-issue thread was checked"
    return 0
  fi
  # PROBED IN GITHUB'S RETURNED ORDER — not sorted. Any sort is a policy nobody asked for, and the
  # order GitHub returns is the only one attributable to something outside this code.
  for issue in $numbers; do
    [ "$probed" -lt "$ROBOREV_LINKED_ISSUE_PROBE_MAX" ] || break
    case "$issue" in
      ''|*[!0-9]*)
        probed=$(( probed + 1 ))
        unread+=("an entry that is not an issue number")
        continue
        ;;
    esac
    probed=$(( probed + 1 ))
    if ! issue_errfile="$(mktemp 2>/dev/null)"; then
      unread+=("#$issue (no temporary file could be created to capture the 'gh' diagnostic)")
      continue
    fi
    if ! comments="$(cd "$REPO" && gh issue view "$issue" --json comments 2>"$issue_errfile")"; then
      issue_errtext="$(tr -d '\r' <"$issue_errfile" | tr '\n' ' ')"
      rm -f "$issue_errfile"
      unread+=("#$issue (${issue_errtext:-no diagnostic was produced})")
      continue
    fi
    rm -f "$issue_errfile"
    # THE SAME SCANNER, THE SAME KIND, THE SAME SCOPE, THE SAME ALLOWLIST. No new argument, no new
    # grammar, no thread parameter — see the header. The deferral additionally passes the SAME
    # observed count, so `count=` is matched identically on both threads.
    if [ -n "$observed" ]; then
      result="$(printf '%s' "$comments" | python3 "$WAIVER_SCAN_TOOL" "$kind" "$base" "$head" "$job" "$ROBOREV_WAIVER_AUTHORS" "$observed" 2>/dev/null)" && scan_rc=0 || scan_rc=1
    else
      result="$(printf '%s' "$comments" | python3 "$WAIVER_SCAN_TOOL" "$kind" "$base" "$head" "$job" "$ROBOREV_WAIVER_AUTHORS" 2>/dev/null)" && scan_rc=0 || scan_rc=1
    fi
    if [ "$scan_rc" -ne 0 ]; then
      unread+=("#$issue (its comments payload could not be parsed)")
      continue
    fi
    state="$(printf '%s\n' "$result" | sed -n 's/^state=//p' | head -1)"
    read_ok+=("#$issue")
    # ===== ESCALATION ONLY FROM AN ISSUE-SIDE `granted`, KEYED ON THE AFFIRMATIVE VALUE =====
    # An issue-side marker that is itself stale, malformed or unauthorized is a DIFFERENT defect that
    # happens to be on a different thread, and re-posting it would not help — reporting MISPLACED for
    # it would make the run FAIL after the operator followed the remedy, which spends the
    # diagnostic's credibility. `MISPLACED` must mean EXACTLY ONE operator action: re-post the
    # identical marker as a top-level PR comment. So the condition is exactly "this marker WOULD have
    # been accepted by the channel had it been on the pull request", and every other state — including
    # one this code has never judged — leaves the outcome alone rather than inheriting an escalating
    # branch.
    if [ "$state" = "granted" ]; then
      ROBOREV_PROBE_OUTCOME="misplaced"
      ROBOREV_PROBE_ISSUE="$issue"
      # ===== THE RENDERING CLAIMS WHAT THE PROBE MEASURED, AND NOT ONE STEP MORE (#3759 R3) =====
      # "would have been ACCEPTED BY THE CHANNEL", never "would have GRANTED". The probe asks the
      # SCANNER's verdict — every property decidable from the comment itself: shape, sole content,
      # column-zero anchor, allowlist, field grammar, reason substance, the base/head/job binding, and
      # for a deferral the `count=` match. It deliberately does NOT run the deferral's NETWORK
      # DISPOSITION LEG (each `issues=` number's four-valued open-issue check) issue-side. That
      # scoping is sound because (1) MISPLACED grants nothing, so the worst case is advice one step
      # short of complete rather than a pass; (2) the remedy is identical either way — a deferral
      # naming a closed issue on the wrong thread must STILL be moved to the PR, where the disposition
      # leg then runs and reports its own precise ISSUE-CLOSED / ISSUE-ABSENT / ISSUE-UNVERIFIABLE;
      # and (3) it would add one network call per declared issue per probed thread on a purely
      # diagnostic path. A DIAGNOSTIC THAT OVERSTATES WHAT IT MEASURED IS WHAT STOPS THE NEXT PERSON
      # LOOKING, so the claim is stated at its true strength and the remaining legs are named.
      ROBOREV_PROBE_DETAIL="an authorization for THIS review (this base, head and job) is on LINKED ISSUE #$issue, not on the pull request — it would have been ACCEPTED BY THE CHANNEL there (shape, sole nonblank content, top-level, allowlisted author, fields and scope all check out)"
      if [ -n "$observed" ]; then
        ROBOREV_PROBE_DETAIL="$ROBOREV_PROBE_DETAIL, and its count= matches the $observed observed finding(s); the issue-disposition legs (every issues= number must be an OPEN issue GitHub confirms) are NOT run issue-side and still apply once it is on the PR"
      fi
      return 0
    fi
  done
  # ===== A PARTIAL READ IS `could-not-check` NAMING BOTH HALVES, NEVER `checked` =====
  if [ "${#unread[@]}" -gt 0 ]; then
    ROBOREV_PROBE_OUTCOME="could-not-check"
    ROBOREV_PROBE_DETAIL="the linked-issue thread could NOT be checked: read with no matching marker: ${read_ok[*]:-none}; NOT read: $(printf '%s; ' "${unread[@]}")"
    return 0
  fi
  if [ "$probed" -lt "$declared" ]; then
    # THE UNPROBED REMAINDER IS NAMED, NEVER IMPLIED — the same reason the gate prints
    # `0 RECOGNISED` rather than a bare `0`. A lane that omits coverage silently is
    # indistinguishable from one that covers it.
    ROBOREV_PROBE_OUTCOME="checked"
    ROBOREV_PROBE_DETAIL="linked issues ${read_ok[*]} checked — $probed of $declared declared, probe bounded at $ROBOREV_LINKED_ISSUE_PROBE_MAX: no matching marker"
    return 0
  fi
  ROBOREV_PROBE_OUTCOME="checked"
  if [ "$declared" -eq 1 ]; then
    ROBOREV_PROBE_DETAIL="linked issue ${read_ok[*]} checked: no matching marker there either"
  else
    ROBOREV_PROBE_DETAIL="linked issues ${read_ok[*]} checked: no matching marker there either"
  fi
  return 0
}

# roborev_absence_waiver_lookup <base-sha> <head-sha> <job-id>: does the PR for this branch carry a
# waiver for THIS REVIEW? Sets, and never returns non-zero:
#   ROBOREV_WAIVER_STATE   granted | stale | malformed | none | unavailable
#   ROBOREV_WAIVER_AUTHOR / _SCOPE / _REASON / _DETAIL
#
# THE MARKER — a DEDICATED LINE of a PR comment, anchored at column zero, all four fields required:
#     roborev-waive: prompt-content-absent base=<40-hex> head=<40-hex> job=<id> reason=<why>
#
# ===== WHY IT IS ANCHORED, AND WHY THE DIAGNOSTIC NEVER PRINTS A COMPLETE ONE (roborev job 23) =====
# THE DEFECT THIS CLOSES, which is the sharpest instance of a shape this issue keeps producing: AN
# ARTIFACT THAT DESCRIBES THE ESCAPE HATCH BECAME THE ESCAPE HATCH. Detection used to accept the marker
# ANYWHERE inside a comment whose newlines had been flattened, and the absence-FAIL diagnostic printed a
# complete marker carrying the live sha — so pasting the summary block into a PR comment, which is the
# documented practice throughout this repo, silently authorized the next run. A quoted example or a
# waiver REQUEST self-granted the same way. It is the same defect as prose inside a diff naming its own
# oracle, which is why the column-zero anchor exists on the census matcher.
#
# THREE INDEPENDENT LAYERS, because these blocks get pasted routinely:
#   (1) LINE BOUNDARIES ARE PRESERVED and the marker must BE the line — no leading whitespace, no
#       quoting prefix, nothing before it. An indented, `>`-quoted or mid-sentence copy cannot match.
#   (2) PLACEHOLDER REASONS ARE REFUSED: empty, an unsubstituted `<…>`, or one of the bare placeholders
#       `claim.sh` already refuses (`why`/`todo`/`tbd`/…). A pasted TEMPLATE therefore reads MALFORMED.
#   (3) THE DIAGNOSTIC EMITS NO VALID MARKER AT ALL — it points the requester at `--help`. Layers 1 and 2
#       make a pasted block harmless; layer 3 means the block never carries a live credential to begin with.
#
# ===== AND THE WAIVER IS BOUND TO THE WHOLE REVIEW SCOPE, NOT JUST THE HEAD (roborev job 23) =====
# Binding `head` alone let ONE persistent comment waive a LATER, different review at the same head — a
# vacuous re-run, or a review against a different base with a different census. The authorizer's judgment
# under constraint (d) was about a SPECIFIC review and its token accounting, so the waiver may not outlive
# it: `base`, `head` and `job` are ALL required and ALL verified, and a marker missing any field is
# MALFORMED rather than granted.
#
# ===== AUTHORSHIP: ENFORCED TO "AN ALLOWLISTED HUMAN", PROCESS-ENFORCED BEYOND THAT =====
# WHAT IS MECHANIZED: the author must be on the explicit allowlist above, the marker must be a dedicated
# anchored line on the PR, it must name the certified base, head AND job, and it must carry a substantive
# reason — and the author, the scope, the reason and the absent paths all land in the summary block.
#
# WHAT REMAINS PROCESS-ENFORCED WITH AN AUDIT TRAIL, and this is the WHOLE residual now: on this fleet the
# worker, the closer and the owner all post through the SAME GitHub login, so this code cannot tell WHICH
# ALLOWLISTED HUMAN posted a given comment. The ruling that only the OWNER or the coordination LEAD may
# GRANT — a worker or closer may only REQUEST — therefore rests on process and on the comment being
# permanently attributable, NOT on a mechanical check. That is narrower than the disclaimer this once
# carried ("authorship cannot be verified at all"), and the narrowing matters: the over-broad version is
# what justified having no author check whatsoever, which let ANY commenter on a public repository grant a
# waiver (job 25). An unenforceable claim must be scoped to what is actually true, never dropped whole.
#
# FAIL-CLOSED EVERYWHERE: no `gh`, no PR, a `gh` error, a marker for another scope, a placeholder reason,
# a missing field — every one of them leaves the absence FAILing, under its own named state.
roborev_absence_waiver_lookup() {
  local base="$1" head="$2" job="$3" json result
  ROBOREV_WAIVER_STATE="none"
  ROBOREV_WAIVER_AUTHOR=""
  ROBOREV_WAIVER_SCOPE=""
  ROBOREV_WAIVER_REASON=""
  ROBOREV_WAIVER_DETAIL=""
  if [ -z "$base" ] || [ -z "$head" ] || [ -z "$job" ] || [ "$job" = "-" ]; then
    ROBOREV_WAIVER_STATE="unavailable"
    ROBOREV_WAIVER_DETAIL="this run has no complete review scope (base='$base' head='$head' job='$job') for a waiver to be bound to"
    return 0
  fi
  if ! command -v gh >/dev/null 2>&1; then
    ROBOREV_WAIVER_STATE="unavailable"
    ROBOREV_WAIVER_DETAIL="'gh' is not on PATH, so no PR comment could be read"
    return 0
  fi
  if ! command -v python3 >/dev/null 2>&1 || [ ! -f "$WAIVER_SCAN_TOOL" ]; then
    ROBOREV_WAIVER_STATE="unavailable"
    ROBOREV_WAIVER_DETAIL="the structured waiver scanner is unusable (python3 present: $(command -v python3 >/dev/null 2>&1 && printf yes || printf no); tool: $WAIVER_SCAN_TOOL) — a waiver is NEVER decided from a flattened text stream, so this fails closed rather than falling back to line parsing"
    return 0
  fi
  # ===== ONE `gh` CALL, RAW JSON, DECIDED STRUCTURALLY (#3312 job 26) =====
  # `--json comments` WITHOUT `--jq`: the author and the body must stay SEPARATE FIELDS of the same
  # object all the way to the decision. The previous form asked `jq` to flatten them into one text
  # stream with an in-band author record, and a comment body is attacker-controlled on a public
  # repository — so a body could carry its own author line and be attributed to an allowlisted login,
  # defeating the allowlist entirely. CONTROL AND DATA MUST NOT SHARE A CHANNEL WHEN THE DATA IS
  # ATTACKER-CONTROLLED; the fix removes the delimiter rather than choosing a rarer one.
  #
  # The `gh` FAILURE IS A STATE, never a silent empty result: it exits non-zero when there is no PR for
  # the branch, when auth is missing and when the API errors, and all three mean no waiver could be
  # established — which keeps the absence FAILing.
  if ! json=$(cd "$REPO" && gh pr view --json comments 2>/dev/null); then
    ROBOREV_WAIVER_STATE="unavailable"
    ROBOREV_WAIVER_DETAIL="'gh pr view --json comments' failed (no PR for this branch, no auth, or an API error), so no waiver could be read"
    return 0
  fi
  [ -n "$json" ] || return 0
  # The scanner owns the WHOLE decision — shape, scope, reason and authorization — so this shell never
  # associates an author with a body. Its output is `key=value` lines with whitespace-collapsed values,
  # the same shape `roborev-job-facts.py` emits, so a free-text reason cannot introduce a second channel.
  # THE MARKER KIND IS NAMED EXPLICITLY (#3626). The scanner now decides TWO authorizations — the
  # absence waiver and the findings deferral — and each call selects exactly one, so neither can ever
  # read the other's marker: an absence waiver confers no authority over `findings:` and a findings
  # deferral confers none over `prompt-content:`. There is no default kind, deliberately: a default
  # would be the one thing that could make a call read a marker its caller did not ask for.
  if ! result=$(printf '%s' "$json" | python3 "$WAIVER_SCAN_TOOL" prompt-content-absent "$base" "$head" "$job" "$ROBOREV_WAIVER_AUTHORS" 2>/dev/null); then
    ROBOREV_WAIVER_STATE="unavailable"
    ROBOREV_WAIVER_DETAIL="the PR comments could not be parsed as JSON, so no waiver could be established"
    return 0
  fi
  ROBOREV_WAIVER_STATE=$(printf '%s\n' "$result" | sed -n 's/^state=//p' | head -1)
  ROBOREV_WAIVER_AUTHOR=$(printf '%s\n' "$result" | sed -n 's/^author=//p' | head -1)
  ROBOREV_WAIVER_SCOPE=$(printf '%s\n' "$result" | sed -n 's/^scope=//p' | head -1)
  ROBOREV_WAIVER_REASON=$(printf '%s\n' "$result" | sed -n 's/^reason=//p' | head -1)
  ROBOREV_WAIVER_DETAIL=$(printf '%s\n' "$result" | sed -n 's/^detail=//p' | head -1)
  # A STATE THIS CODE HAS NEVER JUDGED IS NOT A PASS: an unrecognised (or empty) verdict from the
  # scanner fails closed instead of inheriting the permissive path.
  case "$ROBOREV_WAIVER_STATE" in
    # `misplaced` IS IN THIS LIST AS A BELT, NOT AS A ROUTE (#3759). The scanner never emits it —
    # thread identity is the caller's knowledge — and the probe below assigns it AFTER this
    # validation, so no list entry is strictly required today. It is here so that a future refactor
    # routing the probe's result through this validation cannot rewrite an accurate diagnostic into a
    # generic `unavailable`, re-collapsing the very state this change splits out. THIS IS A
    # RECOGNITION LIST, NOT A GRANTING LIST: membership confers nothing, and the only granting gate
    # anywhere is the token-exact `[ "$ROBOREV_WAIVER_STATE" = "granted" ]` in the checks file.
    granted|unauthorized|stale|malformed|none|misplaced) ;;
    *)
      ROBOREV_WAIVER_DETAIL="the waiver scanner returned the unrecognised state '$ROBOREV_WAIVER_STATE'; failing closed"
      ROBOREV_WAIVER_STATE="unavailable"
      ;;
  esac
  # ===== THE LINKED-ISSUE PROBE: ONLY FROM `none`, AND IT GRANTS NOTHING (#3759) =====
  # ONLY FROM `none`, and the probe is NOT EVEN PERFORMED for the other states. A PR-side `stale`,
  # `malformed`, `unauthorized` or `unavailable` is already specific, already actionable and already
  # correct — "your marker names a different review", "a field is wrong", "this login may not grant",
  # "the oracle could not be consulted" — and replacing one with MISPLACED would substitute a vaguer
  # diagnosis for a precise one and send the operator to move a comment that still would not grant.
  # `none` is the only state carrying no information, so it is the only one the probe may refine.
  # Not calling it (rather than calling and discarding) is what makes the reachability STRUCTURAL and
  # measurable from an invocation log; a network call whose result is discarded is latency plus a
  # future footgun.
  if [ "$ROBOREV_WAIVER_STATE" = "none" ]; then
    roborev_linked_issue_marker_probe prompt-content-absent "$base" "$head" "$job"
    if [ "$ROBOREV_PROBE_OUTCOME" = "misplaced" ]; then
      # KEYED ON THE AFFIRMATIVE VALUE: only the one outcome that means "a marker the channel would
      # have accepted is on the wrong thread" changes the state. Every other outcome — including one
      # this code has never judged — leaves it at `none` and merely records what the probe did.
      ROBOREV_WAIVER_STATE="misplaced"
    fi
    ROBOREV_WAIVER_DETAIL="$ROBOREV_PROBE_DETAIL"
  fi
  return 0
}

# ============== THE FINDINGS DEFERRAL (issue #3626) ==============
# WHY THIS EXISTS. Since #3586 a would-be PASS requires `findings:` to reduce token-exactly to `NONE`,
# in every mode including `--recheck-job`, and that requirement is correctly NOT waivable. The
# consequence nobody designed for: a LEAD-DEFERRED finding is re-reported by every later round, so
# `findings: PRESENT (n)` persists, `RESULT` stays `FAIL`, and the doctrine rule "any non-PASS terminal
# RESULT is a blocked merge" blocks the merge FOREVER. Measured on PR #3572 job 262: two findings, ZERO
# new, both already filed (#3602, #3613) and both already lead-deferred, 5.9M input tokens, every
# deterministic key PASS — and the merge required an out-of-band lead comment. The tooling was behaving
# properly; the doctrine was unobtainable, and a rule that punishes the correct behaviour (#3515's lane
# refused to manufacture a green and asked instead) will not survive contact.
#
# So "roborev clean" is redefined as NO UNADDRESSED FINDINGS rather than "the tool printed zero", and
# the distinction is made MECHANICAL instead of resting on lead memory.
#
# ===== THE TRAP, AND WHY THE AUTHORIZATION LIVES IN A PR COMMENT =====
# The obvious fix — let a lane mark findings deferred so the tool passes — HANDS THE CONSTRAINED PARTY
# THE POWER TO SATISFY ITS OWN CONSTRAINT. That is the shape #3312 spent four High-severity rounds on,
# and its corollary is binding here: THE CONSTRAINED PARTY MUST NOT CHOOSE ITS OWN ENFORCER. A
# `--defer-finding` flag, a deferral file in the worktree and an env var are therefore all
# non-starters — a worker could clear its own findings. The authorization must live somewhere the
# worker cannot write in its own name, so it travels the ABSENCE WAIVER'S CHANNEL: a top-level PR
# comment whose SOLE NONBLANK CONTENT is one anchored marker line, from an author on the hard-coded
# allowlist above, associated with its body STRUCTURALLY by `gh --json`.
#
# REUSE, DO NOT REINVENT: the channel rules are inherited BY CALL (the same scanner, selected by kind),
# never by copy. Five recogniser generations were superseded before that class closed; a second
# implementation of it would be a second place for it to diverge, and a divergence is a bypass.
#
# ===== SEPARATELY SCOPED FROM THE WAIVER, AND THAT SEPARATION IS THE POINT =====
# Distinct marker keywords, distinct summary keys (`waiver:` / `deferral:`), distinct verdict tokens
# (`WAIVED` / `DEFERRED`). Neither reads the other's marker and neither falls back to the other. A
# delivery-artifact waiver may never excuse a real defect, and a findings deferral may never excuse an
# absent prompt. A run may legitimately carry both, each granted on its own marker.
#
# ===== WHAT IS AFFIRMATIVE HERE, AND WHAT IS NOT DEFERRABLE AT ALL =====
# `count=` must EQUAL the observed findings count and `issues=` must be non-empty, so a grant rests on
# a measurement rather than on the absence of a contrary signal (#3586). `findings: UNKNOWN` and
# `findings: SKIP` are NOT deferrable in any mode: those values mean the findings state was never
# ESTABLISHED, and a pass may not rest on a state that could not be read. And nothing here
# reconstructs per-finding identity from the review's prose — that is a recogniser over
# author-controlled text, the class #3564 closed by REMOVING prose reconstruction, and it stays closed.
#
# THE THREAT MODEL AND THE TRIAGE RULE ARE THE WAIVER'S, VERBATIM (see above): a hostile INVOKER is out
# of model by construction; what this defends is (1) parties who do not control the invocation — this
# is a public repository and a failing block PRINTS base/head/job — and (2) accident and drift, the
# larger category. "The invoker can bypass this" => record it, do not patch it. "A non-invoker can
# bypass this", or "this can be bypassed BY ACCIDENT" => defect.
#
# RESIDUALS, named rather than implied: the marker is read from TOP-LEVEL PR COMMENTS ONLY, AND THE
# MOST PROBABLE MISPLACEMENT IS THE PR'S LINKED ISSUE THREAD (#3759) — that is where lane/lead
# coordination lives — followed by a review body and a review-thread reply. None of the three is read,
# so a marker there is silently not applied (the run reports `deferral: NONE` and the FAIL stands —
# fail-closed, but it reads as "my authorization was ignored"). MEASURED on PR #3710, where both
# authorizations were granted field-perfect on issue #3544 and neither applied. SINCE #3759 THE
# LINKED-ISSUE CASE IS DIAGNOSED, NOT GRANTED: on a `none`, the PR's linked issue(s) are scanned with
# the SAME scanner and scope, and a marker there that WOULD have been accepted by the channel reports
# `deferral: MISPLACED (found on linked issue #N …)` with the remedy. MISPLACED GRANTS NOTHING and the
# FAIL stands — only a marker on the PULL REQUEST grants — and a `none` now DECLARES whether the probe
# ran. LEAD-SIDE PROCEDURE: after posting the marker, verify with `gh pr view <PR> --json comments`
# that the line is ON THE PR; a grant is only granted once it is readable by the scanner that reads it.
# And an authorized human can authorize carelessly — pre-authorizing a job, or deferring without
# checking that the findings really are the tracked ones. Nothing mechanical detects either; the
# control is the permanent, attributable comment, which is why a substantive reason is required and
# recorded verbatim.

# ===== RETRIEVABILITY IS THREE-VALUED: PRESENT / ABSENT / COULD-NOT-ASK (#3626, lead condition 1) =====
# This is the LOAD-BEARING leg of a deferral's disposition — the PR-body link check it used to share
# that job with was DELETED, not patched (the full census and the reason live at the deleted site in
# `roborev-waiver-scan.py`; in one line: a PR body is editable by anyone with write access with no
# per-edit attribution, so it was the weaker artifact even before its Markdown recognisers leaked). A
# load-bearing leg MUST NOT collapse "cannot tell" onto an answer.
#
# `gh issue view` EXITS 1 FOR BOTH A MISSING ISSUE AND AN AUTH/NETWORK FAILURE — measured on gh 2.98.0:
#   not found : `GraphQL: Could not resolve to an issue or pull request with the number of N.`  exit 1
#   no auth   : `HTTP 401: Bad credentials (https://api.github.com/graphql)`                    exit 1
# so an exit-code-only test is exactly the two-valued predicate that always picks the permissive
# answer. The verdict therefore comes from the DIAGNOSTIC, and EVERY unrecognised diagnostic is
# COULD-NOT-ASK: both non-present states are non-granting, and what differs is the OPERATOR ACTION
# ("that issue number is wrong" vs "this box cannot reach GitHub"), so guessing between them buys
# nothing and guessing wrong sends the operator to fix the wrong thing.
#
# ===== AND A CLOSED ISSUE IS NOT A DISPOSITION (#3626 round 3) =====
# `gh issue view` answers, and exits 0, for a CLOSED issue, so a number test alone made "the finding
# is tracked" satisfiable by an issue closed as a duplicate three weeks ago — the finding permanently
# untracked while the block asserted it was filed. That is the exact thing this leg exists to prevent,
# so `state` is read alongside `number` and only an OPEN issue may grant.
#
# THIS IS DELIBERATELY STRONGER THAN THE LEAD'S LITERAL CONDITION, which said RETRIEVABLE, and a closed
# issue is retrievable. The claim made at the call site, in the scanner and in the spec is the stronger
# "enforces NOT-DROPPED", and a closed-as-duplicate issue means the finding IS dropped — so the claim is
# made true rather than three statements of it weakened to match a weaker implementation. A false refusal
# here is recoverable (reopen the issue, or file a fresh tracking issue and re-authorize) and is the
# fail-closed direction.
#
# Sets ROBOREV_ISSUE_STATE = present | closed | absent | unverifiable, and ROBOREV_ISSUE_DETAIL for the
# three non-granting ones. It never returns non-zero, because a two-valued RETURN would re-import the
# very collapse this function exists to remove.
roborev_issue_retrievability() {
  local issue="$1" out errfile errtext folded ok num state
  ROBOREV_ISSUE_STATE="unverifiable"
  ROBOREV_ISSUE_DETAIL="'gh issue view $issue' was never asked"
  if ! command -v gh >/dev/null 2>&1; then
    ROBOREV_ISSUE_DETAIL="'gh' is not on PATH, so whether issue #$issue exists could not be asked"
    return 0
  fi
  if ! errfile="$(mktemp 2>/dev/null)"; then
    ROBOREV_ISSUE_DETAIL="no temporary file could be created to capture the 'gh' diagnostic, so a missing issue could not be told apart from a could-not-ask — the two must never read alike, so this is the could-not-ask"
    return 0
  fi
  # `--jq` lets `gh` do the JSON parse and yields ONE line, `<number> <state>`, so the AFFIRMATIVE test
  # is an exact string comparison instead of a regex over JSON. (The COMMENTS payload is deliberately
  # NOT read through `--jq`: there, author and body must stay separate FIELDS of one object so no body
  # can forge its author. Here there is no association to preserve — only two scalars.)
  if out="$(cd "$REPO" && gh issue view "$issue" --json number,state --jq '"\(.number) \(.state)"' 2>"$errfile")"; then
    ok=1
  else
    ok=0
  fi
  errtext="$(tr -d '\r' <"$errfile" | tr '\n' ' ')"
  rm -f "$errfile"
  folded="$(printf '%s' "$errtext" | tr '[:upper:]' '[:lower:]')"
  if [ "$ok" -eq 1 ]; then
    # THE PERMISSIVE BRANCH IS KEYED ON THE AFFIRMATIVE PAIR: the payload must name THIS issue AND say
    # it is OPEN. Anything else — a different number, a missing or `null` state, a state this code has
    # never judged — takes a non-granting branch rather than inheriting the permissive one.
    if [ "$out" = "$issue OPEN" ]; then
      ROBOREV_ISSUE_STATE="present"
      ROBOREV_ISSUE_DETAIL=""
      return 0
    fi
    num="${out%% *}"
    state="${out#* }"
    if [ "$num" = "$issue" ] && [ "$state" = "CLOSED" ]; then
      ROBOREV_ISSUE_STATE="closed"
      ROBOREV_ISSUE_DETAIL="GitHub answered that issue #$issue is CLOSED"
      return 0
    fi
    ROBOREV_ISSUE_STATE="unverifiable"
    ROBOREV_ISSUE_DETAIL="'gh issue view $issue' succeeded but returned '$out' rather than that issue's number and an OPEN state, so its availability to track a deferred finding was never AFFIRMATIVELY established"
    return 0
  fi
  case "$folded" in
    *"could not resolve to an issue"*)
      ROBOREV_ISSUE_STATE="absent"
      ROBOREV_ISSUE_DETAIL="GitHub answered that issue #$issue DOES NOT EXIST in this repository ($errtext)"
      ;;
    *)
      ROBOREV_ISSUE_STATE="unverifiable"
      ROBOREV_ISSUE_DETAIL="'gh issue view $issue' failed WITHOUT answering that the issue does not exist (${errtext:-no diagnostic was produced}), so whether #$issue exists is UNKNOWN — this is a could-not-ask, not an answer"
      ;;
  esac
  return 0
}

# roborev_findings_deferral_lookup <base-sha> <head-sha> <job-id> <observed-findings-count>:
# does the PR for this branch carry a findings deferral for THIS REVIEW, covering exactly this many
# findings, with every named issue an OPEN issue GitHub confirms? Sets, and never returns non-zero:
#   ROBOREV_DEFERRAL_STATE   granted | unauthorized | stale | malformed | none | count-mismatch |
#                            issue-absent | issue-closed | issue-unverifiable | unavailable
#   ROBOREV_DEFERRAL_AUTHOR / _SCOPE / _REASON / _DETAIL / _ISSUES / _COUNT
#
# FAIL-CLOSED EVERYWHERE: no `gh`, no PR, a `gh` error, an unusable scanner, a marker for another
# scope, a count that does not match, an issue GitHub says does not exist, an issue whose existence
# could not be ASKED, a placeholder reason, a missing field — every one of them leaves the findings
# FAILing, under its own named state, because "your marker names the wrong job" and "there is no
# marker" are different operator actions and a bare FAIL distinguishes neither.
roborev_findings_deferral_lookup() {
  local base="$1" head="$2" job="$3" observed="$4" json result issue
  local declared_issues verified_issues
  ROBOREV_DEFERRAL_STATE="none"
  ROBOREV_DEFERRAL_AUTHOR=""
  ROBOREV_DEFERRAL_SCOPE=""
  ROBOREV_DEFERRAL_REASON=""
  ROBOREV_DEFERRAL_DETAIL=""
  ROBOREV_DEFERRAL_ISSUES=""
  ROBOREV_DEFERRAL_COUNT=""
  if [ -z "$base" ] || [ -z "$head" ] || [ -z "$job" ] || [ "$job" = "-" ]; then
    ROBOREV_DEFERRAL_STATE="unavailable"
    ROBOREV_DEFERRAL_DETAIL="this run has no complete review scope (base='$base' head='$head' job='$job') for a deferral to be bound to"
    return 0
  fi
  # THE OBSERVED COUNT IS THE AFFIRMATIVE HALF OF THE BINDING, so an unmeasurable one is a state, never
  # a default: with no measured count there is nothing for `count=` to match and a grant would rest on
  # exactly the absence #3586 forbids.
  case "$observed" in
    ''|*[!0-9]*)
      ROBOREV_DEFERRAL_STATE="unavailable"
      ROBOREV_DEFERRAL_DETAIL="the observed findings count is not a measured number ('$observed'), so the marker's count= has nothing to be matched against; only an affirmatively measured 'PRESENT (n)' is deferrable"
      return 0
      ;;
  esac
  if ! command -v gh >/dev/null 2>&1; then
    ROBOREV_DEFERRAL_STATE="unavailable"
    ROBOREV_DEFERRAL_DETAIL="'gh' is not on PATH, so no PR comment could be read"
    return 0
  fi
  if ! command -v python3 >/dev/null 2>&1 || [ ! -f "$WAIVER_SCAN_TOOL" ]; then
    ROBOREV_DEFERRAL_STATE="unavailable"
    ROBOREV_DEFERRAL_DETAIL="the structured authorization scanner is unusable (python3 present: $(command -v python3 >/dev/null 2>&1 && printf yes || printf no); tool: $WAIVER_SCAN_TOOL) — an authorization is NEVER decided from a flattened text stream, so this fails closed rather than falling back to line parsing"
    return 0
  fi
  # ONE `gh` CALL, RAW JSON, DECIDED STRUCTURALLY — AND `comments` IS THE WHOLE PAYLOAD (#3626).
  # `body` was fetched here for a PR-body link check that has been DELETED rather than patched: a PR
  # body is editable at any time by anyone with write access with NO per-edit attribution, while a
  # top-level comment is permanent and attributable, so the body was the weaker artifact and is now
  # evidence for nothing (the full bypass census is at the deleted site in `roborev-waiver-scan.py`).
  # No `--jq`: author and body must stay SEPARATE FIELDS of the same object all the way to the
  # decision, so nothing inside a body can change whose comment it is.
  if ! json=$(cd "$REPO" && gh pr view --json comments 2>/dev/null); then
    ROBOREV_DEFERRAL_STATE="unavailable"
    ROBOREV_DEFERRAL_DETAIL="'gh pr view --json comments' failed (no PR for this branch, no auth, or an API error), so no deferral could be read"
    return 0
  fi
  [ -n "$json" ] || return 0
  if ! result=$(printf '%s' "$json" | python3 "$WAIVER_SCAN_TOOL" findings-deferral "$base" "$head" "$job" "$ROBOREV_WAIVER_AUTHORS" "$observed" 2>/dev/null); then
    ROBOREV_DEFERRAL_STATE="unavailable"
    ROBOREV_DEFERRAL_DETAIL="the PR payload could not be parsed as JSON, so no deferral could be established"
    return 0
  fi
  ROBOREV_DEFERRAL_STATE=$(printf '%s\n' "$result" | sed -n 's/^state=//p' | head -1)
  ROBOREV_DEFERRAL_AUTHOR=$(printf '%s\n' "$result" | sed -n 's/^author=//p' | head -1)
  ROBOREV_DEFERRAL_SCOPE=$(printf '%s\n' "$result" | sed -n 's/^scope=//p' | head -1)
  ROBOREV_DEFERRAL_REASON=$(printf '%s\n' "$result" | sed -n 's/^reason=//p' | head -1)
  ROBOREV_DEFERRAL_DETAIL=$(printf '%s\n' "$result" | sed -n 's/^detail=//p' | head -1)
  ROBOREV_DEFERRAL_ISSUES=$(printf '%s\n' "$result" | sed -n 's/^issues=//p' | head -1)
  ROBOREV_DEFERRAL_COUNT=$(printf '%s\n' "$result" | sed -n 's/^count=//p' | head -1)
  # A STATE THIS CODE HAS NEVER JUDGED IS NOT A GRANT: an unrecognised (or empty) verdict from the
  # scanner fails closed instead of inheriting the permissive path.
  case "$ROBOREV_DEFERRAL_STATE" in
    # `unavailable` is RETAINED here as a pass-through, though the scanner emits no such state today
    # (the one it had reported a payload with no readable PR body, and the PR body is no longer read at
    # all — #3626). A scanner that ever reports its own unavailability keeps its PRECISE cause instead
    # of being rewritten to a generic "unrecognised state", which is the diagnostic-quality failure
    # this case exists to avoid; nothing becomes permissive either way, because `unavailable` is
    # non-granting on both paths. `pr-unlinked` is GONE with the check that produced it.
    # `misplaced` IS A BELT HERE FOR THE SAME REASON AS ON THE WAIVER (#3759): the scanner never
    # emits it, the probe below assigns it after this validation, and the entry exists so a future
    # refactor routing the probe through this validation cannot rewrite an accurate diagnostic into a
    # generic `unavailable`. A RECOGNITION LIST, NOT A GRANTING LIST — the only granting gate is the
    # token-exact `= "granted"` comparison on the line after this `case`.
    granted|unauthorized|stale|malformed|none|count-mismatch|unavailable|misplaced) ;;
    *)
      ROBOREV_DEFERRAL_DETAIL="the deferral scanner returned the unrecognised state '$ROBOREV_DEFERRAL_STATE'; failing closed"
      ROBOREV_DEFERRAL_STATE="unavailable"
      return 0
      ;;
  esac
  # ===== THE LINKED-ISSUE PROBE: ONLY FROM `none`, AND IT GRANTS NOTHING (#3759) =====
  # Identical rule and identical reasoning to the waiver's (see the block in
  # `roborev_absence_waiver_lookup`), served by the SAME helper — two copies of a probe over an
  # authorization channel would be two places for it to diverge. The observed findings count is
  # passed through UNCHANGED, so `count=` is matched issue-side exactly as it is PR-side; what is
  # deliberately NOT run issue-side is the network disposition leg, and the rendering says so.
  if [ "$ROBOREV_DEFERRAL_STATE" = "none" ]; then
    roborev_linked_issue_marker_probe findings-deferral "$base" "$head" "$job" "$observed"
    if [ "$ROBOREV_PROBE_OUTCOME" = "misplaced" ]; then
      ROBOREV_DEFERRAL_STATE="misplaced"
    fi
    ROBOREV_DEFERRAL_DETAIL="$ROBOREV_PROBE_DETAIL"
  fi
  # The disposition legs below are unreachable from `none`/`misplaced` — this comparison is the one
  # granting gate, and it is left EXACTLY as it was: nothing about the probe widens it.
  [ "$ROBOREV_DEFERRAL_STATE" = "granted" ] || return 0
  # ===== DISPOSITION: EACH DEFERRED ISSUE MUST BE AN OPEN, FILED ISSUE — AND THAT IS THE WHOLE OF IT =
  # This is the ONE leg that enforces NOT-DROPPED, since the PR-body scan was removed (#3626). It needs
  # a network read, so it lives here rather than in the structured scanner. It is THREE-VALUED: an
  # issue GitHub says does not exist and an issue whose existence could not be ASKED are separate
  # states with separate causes, because they are different operator actions — and neither is ever read
  # as verified. A deferral pointing at nothing is a dropped finding wearing a link.
  # ONE GRANT IS UNDONE BY ANY FAILURE — the loop cannot leave a partial grant standing, because the
  # state is overwritten before the first failure returns.
  # ===== THE BACKSTOP COUNTS VERIFICATIONS PERFORMED — IT DOES NOT TEST THE STRING (#3626 round 3) ====
  # It used to be `[ -z "$ROBOREV_DEFERRAL_ISSUES" ]`, i.e. a NON-EMPTINESS test standing in for a
  # VERIFICATION test, which is the fail-open shape this whole file is written against: never derive a
  # pass from the absence of a bad signal. Reproduced directly — `ROBOREV_DEFERRAL_ISSUES=","` passes
  # `[ -z ]`, `${//,/ }` yields `" "`, the UNQUOTED expansion splits into ZERO WORDS, the loop body
  # never runs, and the function returns with the state still `granted`: `findings: DEFERRED` and
  # `RESULT: PASS` with not one `gh issue view` executed and the block recording `issues=,`.
  #
  # That value is unreachable TODAY only because the marker pattern's `issues=([0-9]+(?:,[0-9]+)*)`
  # forbids it — precisely the upstream dependency a backstop must not have. So the test is
  # AFFIRMATIVE: count the verifications actually performed and require that count to EQUAL the number
  # of DECLARED comma-separated fields. Any list the split does not traverse one-for-one is then a
  # mismatch and fails closed, whatever a future loosening of `issues=` lets through. Measured, so the
  # claim is not broader than the check: `,` (declared 2, traversed 0) and `3602,,3613` (declared 3,
  # traversed 2) are refused, and so is a whitespace-only string. `3602, 3613` is NOT refused and does
  # not need to be — one comma declares 2 fields and the split yields 2 words, so each number is still
  # put through retrievability individually, which is the property. This clause is spelled out because
  # an inaccurate comment on a security-relevant check is what stops the next reader looking.
  declared_issues=$(( $(printf '%s' "$ROBOREV_DEFERRAL_ISSUES" | tr -cd ',' | wc -c) + 1 ))
  verified_issues=0
  # The precise diagnostic for the empty list is kept, because "granted with no issue list at all" and
  # "granted with a list the split could not traverse" send the operator to different places. It is no
  # longer what enforces the property: the count equality below catches the empty list too.
  if [ -z "$ROBOREV_DEFERRAL_ISSUES" ]; then
    ROBOREV_DEFERRAL_STATE="unavailable"
    ROBOREV_DEFERRAL_DETAIL="the scanner granted without an issue list, which cannot happen through the marker pattern; failing closed rather than granting a deferral with no recorded disposition"
    return 0
  fi
  # The list is comma-separated integers (the marker pattern guarantees the shape), so the commas are
  # replaced with spaces and the default IFS does the split — no global IFS to save and restore, which
  # is one fewer thing to leave broken on an early return.
  for issue in ${ROBOREV_DEFERRAL_ISSUES//,/ }; do
    roborev_issue_retrievability "$issue"
    # KEYED ON THE AFFIRMATIVE VALUE: only `present` continues. Every other value — including one this
    # code has never judged — takes a non-granting branch, so an unplanned state cannot inherit the
    # permissive path. ONE GRANT IS UNDONE BY ANY FAILURE: the state is overwritten before the return,
    # so the loop can never leave a partial grant standing.
    case "$ROBOREV_ISSUE_STATE" in
      present) verified_issues=$(( verified_issues + 1 )) ;;
      closed)
        ROBOREV_DEFERRAL_STATE="issue-closed"
        ROBOREV_DEFERRAL_DETAIL="$ROBOREV_ISSUE_DETAIL — so a deferred finding is tracked by an issue nobody will look at again, which is the finding being DROPPED with a link attached. This is stricter than 'retrievable' on purpose: reopen #$issue, or file a fresh tracking issue and re-authorize with its number"
        return 0
        ;;
      absent)
        ROBOREV_DEFERRAL_STATE="issue-absent"
        ROBOREV_DEFERRAL_DETAIL="$ROBOREV_ISSUE_DETAIL — so the disposition of a deferred finding rests on nothing. A deferral must name a FILED issue; fail-closed rather than deferring into a void. Check the number in the marker, file the issue, then re-authorize"
        return 0
        ;;
      *)
        ROBOREV_DEFERRAL_STATE="issue-unverifiable"
        ROBOREV_DEFERRAL_DETAIL="$ROBOREV_ISSUE_DETAIL — and a deferral may not be granted on an UNVERIFIED disposition: 'the issue does not exist' and 'this box could not ask GitHub' are different operator actions, and only the first is an answer. This one is reported separately BECAUSE it is not an answer — fix the ability to reach GitHub (auth, network, rate limit) and re-run; do NOT change the marker"
        return 0
        ;;
    esac
  done
  # THE AFFIRMATIVE REQUIREMENT ITSELF. Reached only when every traversed issue verified `present`, so
  # this is the one remaining way a grant can stand: as many verifications as declared fields.
  if [ "$verified_issues" -ne "$declared_issues" ]; then
    ROBOREV_DEFERRAL_STATE="unavailable"
    ROBOREV_DEFERRAL_DETAIL="the deferral declares $declared_issues issue field(s) in '$ROBOREV_DEFERRAL_ISSUES' but only $verified_issues were AFFIRMATIVELY verified as open and filed, so at least one declared field was never checked; failing closed rather than granting on an unverified disposition"
    return 0
  fi
  return 0
}
