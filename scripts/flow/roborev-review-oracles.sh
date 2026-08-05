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
# ^ every variable assigned in this file (PUSH_ASSERT, BASE_SHA, CENSUS_CHECK,
#   CODE_FREE, census_*) is READ by the sourcing wrapper, which
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
# endpoint; `BASE_SHA` the left. The order is presentational ONLY — the fold below is a
# DISJUNCTION, so permuting this array cannot change any answer.
roborev_range_endpoint_refs() {
  local ref
  for ref in HEAD "${BASE_SHA:-}"; do
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

# roborev_census: sets BASE_SHA, CENSUS, CENSUS_CHECK, CODE_FREE and the
# census_* / census_paths state; calls `finish` on failure or an empty census.
roborev_census() {
  # --- step 3: the local diff census — THE ORACLE -------------------------------
  # `<base>` (default `origin/main`) IS a local mirror ref, so it can be stale or —
  # on a narrow-refspec clone that has never fetched — absent. Fail CLOSED: an
  # unresolvable base must never be allowed to produce an empty census, which would
  # surface as NOTHING-TO-REVIEW and read as "nothing to look at" rather than "we
  # could not tell". No implicit `git fetch` is performed on the caller's behalf.
  BASE_SHA=""
  if ! BASE_SHA=$(git -C "$REPO" rev-parse --verify --quiet "${BASE}^{commit}"); then
    CENSUS_CHECK="FAIL (base '$BASE' unresolvable)"
    DETAILS+=("ERROR: census: base ref '$BASE' does not resolve to a commit in $REPO, so the census — and therefore every vacuity judgement — would be unfounded. This is a FAIL, explicitly NOT a NOTHING-TO-REVIEW: an unresolvable base is 'we cannot tell', never 'there is nothing to review'. If '$BASE' is a remote-tracking ref, this clone may have a narrow fetch refspec or have never fetched it; fetch it yourself (the wrapper never fetches behind your back) and re-run. No review was enqueued.")
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
  git -C "$REPO" diff --numstat -z --no-renames "${BASE}...HEAD" \
    >"$numstat_file" 2>"$numstat_file.err"
  DIFF_RC=$?
  set -e
  if [ "$DIFF_RC" -ne 0 ]; then
    CENSUS_CHECK="FAIL (git diff failed)"
    DETAILS+=("ERROR: census: 'git diff --numstat -z --no-renames ${BASE}...HEAD' exited $DIFF_RC in $REPO, so the census was never measured. This is a FAIL, explicitly NOT a NOTHING-TO-REVIEW — an unmeasurable diff is 'we cannot tell', never 'there is nothing to review'. git said:")
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
  # observability-justified: every caller has already established this file's state — the wrapper's own
  # prompt file, or a snapshot/capture path that `_roborev_regular_readable_state` reported PRESENT. This
  # test is a defensive no-op guard, and its falsity yields an EMPTY header set, which is the fail-closed
  # direction (every census path then reads absent) rather than an absence claim about the path.
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

# ===================== THE SECOND DIFF-DELIVERY MODE (issue #3312) =====================
# roborev delivers the diff to the reviewer in ONE OF TWO ways, and modelling only the first
# made `prompt-content:` FALSE-FAIL every large review. When the diff is big it is NOT inlined:
# roborev writes it to a TRANSIENT snapshot file under `<repo>/.roborev/roborev-snapshot-<id>/`
# and the prompt ends with an instruction to read it. MEASURED (job 6836 — 23 files, +6561/-1;
# 1.47M input / 1.35M cached / 6.1k output; 4 findings with real `file:line`):
#
#     ### Combined Diff
#
#     (Diff too large to include inline)
#
#     The full diff has been written to a file for review.
#     Read the diff from: `/Users/.../issue-3272/.roborev/roborev-snapshot-157393586/roborev-snapshot-content.diff`
#
#     Review the actual diff before writing findings.
#
# That prompt carries ZERO `diff --git` headers, so the check reported
# `FAIL (21/21 code census paths absent from the prompt)` on a review the reviewer demonstrably
# performed — a red no fix can clear, on exactly the diffs that most need reviewing, which is
# the documented way a guard gets waived. And `prompt-content:` is the layer #3229 deliberately
# KEPT when the pre-enqueue predictor was deleted.
#
# THE FIX IS NOT "AN EMPTY PROMPT IS A PASS" — that would reopen T3 (a silently discarded diff)
# and the `0/0` false pass. It is: FOLLOW THE DIFF TO WHERE IT ACTUALLY IS. Same census, same
# canonical matcher, same "no subtraction and no excusal"; only the SOURCE of the headers moves.
# Every new edge therefore fails CLOSED, with its own cause text, because an oracle that could
# not be consulted is a NON-PASSING verdict whose text says what was unverifiable.
#
# WHY THE EXTRACTION LIVES HERE, beside the header collector: "where the diff actually is" is
# prompt-shape knowledge, the same class as "how far the extended-header run extends". The
# checks file must not grow a second idea of it (asserted structurally).

# ============ THE SNAPSHOT IS GONE BEFORE THE WRAPPER CAN READ IT (#3312) ============
# MEASURED LIVE, on this fleet, with the real binary — and it falsifies the obvious design. Job 3,
# 12 files / +3311, 232,820-byte snapshot, 541,812 input / 472,576 cached tokens, review-completed
# PASS, both vacuity tiers PASS: roborev wrote
# `<repo>/.roborev/roborev-snapshot-898764941/roborev-snapshot-content.diff` (9 `diff --git`
# headers, exactly the 9 code census paths) — and DELETED the whole directory when the review
# finished, i.e. BEFORE `roborev review --wait` returned to us. Reading the path after the review
# therefore reports `cleaned-up` on EVERY genuine snapshot-mode review: a differently-named FAIL
# is not a fix, because the merge is still blocked on exactly the large diffs the issue is about.
#
# THE DIFF IS NOT RETRIEVABLE AFTER THE FACT. `roborev show <job>` has `--prompt` and `--json` and
# NO `--diff` (checked against the installed CLI's own help), and the prompt only NAMES the path.
#
# SO IT IS CAPTURED WHILE THE REVIEW RUNS: a watcher started before `roborev review` copies each
# `<repo>/.roborev/roborev-snapshot-*/roborev-snapshot-content.diff` it sees into the wrapper's own
# diagnostics directory, keyed by the SNAPSHOT DIRECTORY ID. After the review, the check resolves
# the id from the path the prompt names and reads OUR COPY of exactly that snapshot.
#
# WHY THIS IS NOT A WEAKENING. The copy is (a) taken from INSIDE the reviewed repository, (b) taken
# DURING this run, by us, (c) VALIDATED AT CAPTURE TIME and matched afterwards against the exact
# relative path the reviewed job's OWN prompt names. Nothing is reconstructed: we never compute the
# diff ourselves. That distinction is the whole point — `git diff` output of our own would make
# `prompt-content:` compare our census against our own diff, which always agrees, i.e. exactly the
# vacuous pass this key exists to prevent. A capture that does not happen is not excused either:
# with no live file and no capture the verdict is still `cleaned-up`, fail-closed.
#
# ===== PROVENANCE IS ESTABLISHED AT CAPTURE TIME, NOT AT VALIDATION TIME (roborev job 6) =====
# Two blockers, and they were two halves of ONE seam: capture provenance was established too LATE
# and keyed too LOOSELY. Both were false-PASS vectors in the guard whose only job is preventing
# false passes, so both are closed here rather than argued down.
#
# (1) THE CAPTURE-TIME PATH RUNS EARLIER AND THEREFORE WINS. `-f` and `cp` both FOLLOW symlinks, so
#     a symlink at `<repo>/.roborev/roborev-snapshot-<id>/roborev-snapshot-content.diff` (or a
#     symlinked `.roborev`/`roborev-snapshot-<id>` component) made the watcher copy an OUTSIDE file
#     into the capture dir. The validation-time `-L` refusal added earlier cannot see it: roborev
#     deletes the original first, so by then there is no path left to inspect and the capture is
#     simply trusted. So the watcher now validates BEFORE it reads — no symlink at the file OR at
#     any component it descends, a regular file, and a physically-resolved directory that really is
#     `<repo>/.roborev/<id>` — and RECORDS that validation beside the capture. The consumer requires
#     the record AFFIRMATIVELY: a capture with no validation record is REJECTED, never trusted,
#     because "no recorded failure" is not a measurement.
#
# (2) A CAPTURE IS KEYED BY ITS FULL VALIDATED RELATIVE PATH, not by the directory id. The binding
#     accepts any file beneath a snapshot directory while the watcher only ever copies the canonical
#     `roborev-snapshot-content.diff`, so an id-only lookup let a prompt naming a DIFFERENT (missing)
#     file in that directory be answered with the canonical sibling's capture — certifying a diff the
#     reviewer was never instructed to read. The validation record therefore stores the exact
#     relative path captured, and the consumer requires it to EQUAL the relative path the prompt
#     names. (Chosen over hard-coding the expected filename + depth in the binding: the PROMPT is the
#     authority on which file the reviewer was pointed at, and an equality test against what we
#     actually copied cannot drift with roborev's naming, while a pinned filename would both duplicate
#     that authority and false-FAIL if roborev ever renames its snapshot file.)
#
# ============ C‴: SNAPSHOT MODE IS OBSERVED AND REPORTED, NOT CERTIFIED ============
# THE HISTORY, because the shape of this code is the conclusion of it. Certifying a snapshot-delivered
# review meant trusting a copy of a file roborev deletes before it returns, and making that copy
# trustworthy took a capture watcher, private per-run publication directories, sequenced publications,
# a three-way digest agreement, an identity record and whole-path matching — across seven review rounds
# that found ELEVEN false-PASS vectors in that machinery, the last of which (a stale publication
# remaining eligible after a failed re-capture) fired the pre-registered exit.
#
# THE OWNER'S RULING: snapshot mode drops to a `NOTICE`, and the wrapper's own summary block records
# what it observed — the snapshot PATH, its DIGEST, and the census code-path subset it EXPECTED — so the
# information is preserved and actionable by a human closer even though nothing is certified from it.
#
# WHAT THAT DELETES, and why each deletion is safe: **nothing derives a verdict from an observation any
# more**, so everything that existed to make an observation trustworthy stops being load-bearing. Gone:
# the copy itself, the private capture directory (and its containment test and cleanup), staging,
# sequenced publications and their selection, the identity/`meta` record, whole-path capture matching,
# the reuse key, and the pre/post/staged digest agreement. A stale or missed observation is now a
# REPORTING IMPRECISION, not a false certification — which is exactly what made those parts expensive.
#
# WHAT SURVIVES, deliberately:
#   * the three-way path-state helper (`verified-absent`/`present`/`unreadable`) and its lint. The
#     predicate family is a general lesson, it is cheap, and it keeps "cannot tell" out of every answer.
#   * the SAFETY half of the path checks: absolute, no `.`/`..` segments, inside the reviewed repository,
#     no symlink at any traversed component. Those exist so the wrapper never READS something it should
#     not, which is independent of whether it certifies anything.
#   * "never silence": an unobserved snapshot yields a NOTICE that NAMES what could not be observed.
#
# The observer therefore only ever READS. It never copies, never publishes, and holds no state a
# consumer could mistake for evidence.
ROBOREV_SNAPSHOT_OBSERVE_FILE=""
ROBOREV_SNAPSHOT_OBSERVE_PID=""
_ROBOREV_SNAPSHOT_OBSERVE_POLL_SECS=${ROBOREV_SNAPSHOT_OBSERVE_POLL_SECS:-1}

# roborev_snapshot_observe_start <observation-file>: begin observing. Never fatal — an observer that
# cannot start leaves the NOTICE saying the snapshot was not observed, which is the honest outcome.
roborev_snapshot_observe_start() {
  ROBOREV_SNAPSHOT_OBSERVE_FILE=""
  ROBOREV_SNAPSHOT_OBSERVE_PID=""
  : >"$1" 2>/dev/null || return 0
  ROBOREV_SNAPSHOT_OBSERVE_FILE="$1"
  _roborev_snapshot_observe_loop "$REPO" "$1" &
  ROBOREV_SNAPSHOT_OBSERVE_PID=$!
  return 0
}

# roborev_snapshot_observe_stop: stop the observer. TERM asks it to finish the current iteration; the
# bounded escalation to KILL is guarded, because "the observer already exited" is its EXPECTED failure
# mode and an unguarded `kill` as the last command of an AND-OR list aborts the wrapper under `set -e`.
roborev_snapshot_observe_stop() {
  local waited=0
  [ -n "${ROBOREV_SNAPSHOT_OBSERVE_PID:-}" ] || return 0
  kill -TERM "$ROBOREV_SNAPSHOT_OBSERVE_PID" 2>/dev/null || :
  while [ "$waited" -lt 50 ]; do
    kill -0 "$ROBOREV_SNAPSHOT_OBSERVE_PID" 2>/dev/null || break
    waited=$((waited + 1))
    sleep 0.1 2>/dev/null || sleep 1
  done
  if kill -0 "$ROBOREV_SNAPSHOT_OBSERVE_PID" 2>/dev/null; then
    kill -KILL "$ROBOREV_SNAPSHOT_OBSERVE_PID" 2>/dev/null || :
  fi
  wait "$ROBOREV_SNAPSHOT_OBSERVE_PID" 2>/dev/null || :
  ROBOREV_SNAPSHOT_OBSERVE_PID=""
  return 0
}

# _roborev_snapshot_safe_to_read <repo> <snapshot-dir-id> <file>: may the wrapper READ this file? This
# is the SAFETY half of the retired capture validation and nothing more: every traversed component must
# be observable and not a symlink, the file must be a readable regular file, and its directory must
# physically be `<repo>/.roborev/<id>`. It answers "may we look", never "is this evidence".
_roborev_snapshot_safe_to_read() {
  local repo="$1" id="$2" f="$3" dirphys _sr_walk
  [ -n "$repo" ] && [ -n "$id" ] && [ -n "$f" ] || return 1
  for _sr_walk in "$repo/.roborev" "$repo/.roborev/$id" "$f"; do
    _roborev_path_state "$_sr_walk" || :
    [ "$_rx_path_state" = "present" ] || return 1
    # observability-justified: `-L` is asked only of a component the helper reported PRESENT, so its
    # falsity is a measurement about type rather than an inference from a failed lookup.
    [ ! -L "$_sr_walk" ] || return 1
  done
  _roborev_regular_readable_state "$f" || return 1
  dirphys=$(cd "${f%/*}" 2>/dev/null && pwd -P) || return 1
  [ "$dirphys" = "$repo/.roborev/$id" ] || return 1
  return 0
}

# The observation record is one line per observation, `<path>\t<digest>\t<bytes>`, appended whenever the
# digest changes; the LAST line for a path is what the block reports. Append-only and read-only-once, so
# there is no publication, no selection and no staleness rule to get wrong — a superseded line is simply
# an earlier observation of the same path, and the reader takes the last.
_roborev_snapshot_observe_loop() {
  local repo="$1" out="$2" f id last=""
  _rx_observe_stop=0
  trap '_rx_observe_stop=1' TERM
  while : ; do
    for f in "$repo"/.roborev/roborev-snapshot-*/roborev-snapshot-content.diff; do
      id="${f%/*}"
      id="${id##*/}"
      _roborev_snapshot_safe_to_read "$repo" "$id" "$f" || continue
      _roborev_file_digest "$f" || continue
      [ "$_rx_digest" != "$last" ] || continue
      last="$_rx_digest"
      printf '%s\t%s\t%s\n' "$f" "$_rx_digest" "$(wc -c <"$f" 2>/dev/null | tr -d '[:space:]')" \
        >>"$out" 2>/dev/null || :
    done
    [ "$_rx_observe_stop" -eq 0 ] || return 0
    sleep "$_ROBOREV_SNAPSHOT_OBSERVE_POLL_SECS" 2>/dev/null || sleep 1
    [ "$_rx_observe_stop" -eq 0 ] || return 0
  done
}

# roborev_snapshot_observation_for <path>: the digest and size the observer recorded for `<path>`, into
# `_rx_obs_digest` / `_rx_obs_bytes`. Exit 1 when the observer recorded nothing for it — which the
# caller reports as "not observed", never as a fact about the diff.
roborev_snapshot_observation_for() {
  local want="$1" line p d b
  _rx_obs_digest=""
  _rx_obs_bytes=""
  [ -n "${ROBOREV_SNAPSHOT_OBSERVE_FILE:-}" ] || return 1
  # observability-justified: the observation file is the wrapper's OWN, created by this run beside its
  # transcript. Its absence yields "not observed", which is the honest report rather than a claim.
  [ -f "$ROBOREV_SNAPSHOT_OBSERVE_FILE" ] || return 1
  while IFS= read -r line; do
    p="${line%%$'\t'*}"
    [ "$p" = "$want" ] || continue
    d="${line#*$'\t'}"
    b="${d#*$'\t'}"
    d="${d%%$'\t'*}"
    _rx_obs_digest="$d"
    _rx_obs_bytes="$b"
  done <"$ROBOREV_SNAPSHOT_OBSERVE_FILE"
  [ -n "$_rx_obs_digest" ] || return 1
  return 0
}

# roborev_prompt_snapshot_paths <prompt-file>: the DISTINCT snapshot diff paths the prompt
# instructs the reviewer to read, into `_rx_snap_paths`, plus `_rx_snap_unparseable` — how many
# instruction lines carried NO readable path. That second count is not diagnostics: an
# instruction line we could not read is an UNKNOWN source, so the caller fails closed on it even
# when another line did parse. Counting only the good ones would be the "only the bad states are
# tested" shape with the roles swapped — a partially-read instruction block excused by its
# readable half.
#
# ANCHORED AT COLUMN ZERO, and that is load-bearing rather than tidy. Every line of a unified
# diff BODY carries a leading `+`, `-`, ` `, `@` or `\`, so an instruction quoted INSIDE the
# reviewed change — this repo's own docs and tests quote it — can never pose as roborev's own
# instruction. Without the anchor a branch could name the file its own review is judged against.
#
# TWO INSTRUCTION SPELLINGS, both READ OUT OF THE INSTALLED BINARY rather than inferred from one
# transcript (roborev v0.61.2, `strings`):
#     Read the diff from: `%s`          the full form, beside `roborev-snapshot-content.diff`
#     (Diff too large; read `%s`.)      the compact form emitted under a tight prompt budget
# Accepting both can only ADD coverage: whatever the compact form's `%s` turns out to be, the
# token still has to pass `roborev_snapshot_path_binding` before anything is read, so a token
# that is not a bound snapshot path FAILs closed exactly as it does today. The DECLARED
# UNCERTAINTY: the compact form's `%s` was measured as a format string, not observed on a live
# prompt, so if it names a COMMAND rather than a path the binding reports `not-absolute` — a
# fail-closed verdict under a cause that would then be worth renaming (see the residual noted
# under `none` in roborev-review-checks.sh).
#
# LINE-ORIENTED, soundly: the instruction is one line and a path on it cannot contain a newline,
# so there is nothing `-z` could add. (Git PLUMBING output still gets `-z` — see the census.)
# The path is taken RAW, exactly as the prompt spells it: it is not a git-quoted token and the
# single normalisation boundary is untouched.
roborev_prompt_snapshot_paths() {
  local f="$1" row seen p q
  _rx_snap_paths=()
  _rx_snap_unparseable=0
  _rx_snap_oversize_markers=0
  # observability-justified: `$f` is the wrapper's OWN prompt file, written by this run beside its
  # transcript. Its falsity yields no snapshot paths, which the caller reports as the `none` state — a
  # fail-closed measurement about the prompt, not a claim about a roborev file.
  [ -f "$f" ] || return 0
  while IFS= read -r row; do
    [ -n "$row" ] || continue
    case "$row" in
      OVERSIZE) _rx_snap_oversize_markers=$((_rx_snap_oversize_markers + 1)); continue ;;
      UNPARSEABLE) _rx_snap_unparseable=$((_rx_snap_unparseable + 1)); continue ;;
    esac
    p="${row#PATH	}"
    [ -n "$p" ] || { _rx_snap_unparseable=$((_rx_snap_unparseable + 1)); continue; }
    seen=0
    for q in ${_rx_snap_paths[@]+"${_rx_snap_paths[@]}"}; do
      [ "$q" = "$p" ] && seen=1
    done
    [ "$seen" -eq 1 ] || _rx_snap_paths+=("$p")
  done < <(LC_ALL=C awk '
      # THE INSTRUCTION LINES, both spellings, each anchored at COLUMN ZERO (index(...) == 1).
      index($0, "Read the diff from:") == 1 || index($0, "(Diff too large; read ") == 1 {
        line = $0
        sub(/\r$/, "", line)
        s = index(line, "`")
        if (s == 0) { print "UNPARSEABLE"; next }
        rest = substr(line, s + 1)
        e = 0
        for (i = length(rest); i >= 1; i--) if (substr(rest, i, 1) == "`") { e = i; break }
        if (e <= 1) { print "UNPARSEABLE"; next }
        printf "PATH\t%s\n", substr(rest, 1, e - 1)
        next
      }
      # THE OTHER OVERSIZE TIERS, reported so the caller can say WHICH mode it is looking at
      # rather than only "the paths are absent". Measured in the same binary: the
      # `codex_*_fallback_*` and `generic_*_fallback` templates open with a `(Diff too large`
      # line and then ask the reviewer to run git commands ITSELF — no snapshot file exists, so
      # nothing local can establish what the reviewer saw. Counted, never excused.
      index($0, "(Diff too large") == 1 { print "OVERSIZE" }
    ' "$f" 2>/dev/null)
  return 0
}

# _roborev_resolve_existing_ancestor <path>: `<path>` with its deepest EXISTING ancestor
# directory replaced by that directory's PHYSICAL path (`cd`+`pwd -P`), into `_rx_resolved`.
#
# Why not `realpath`/`readlink -f`: neither is portable (BSD `readlink` has no `-f`), and both
# need the path to exist — while the whole point here is that a snapshot file may ALREADY BE
# GONE. Resolving the deepest ancestor that does exist makes the containment test below immune
# to a symlinked parent (a `/tmp` -> `/private/tmp` box, a symlinked worktree root) without
# requiring the file itself, and it removes any `..` an existing prefix contained. `..` in the
# still-unresolved remainder is rejected separately, by component, in the binding below.
_roborev_resolve_existing_ancestor() {
  local p="$1" dir="$1" rest="" base phys
  _rx_resolved="$p"
  while : ; do
    # observability-justified: this walk exists ONLY to find a directory whose PHYSICAL path can be read
    # for the containment test. An unobservable component makes the walk continue to a shallower
    # ancestor, which can only ever make containment STRICTER; the components inside the repo are
    # separately required to be observable by `roborev_snapshot_path_binding`, which is where an
    # unreadable one is reported.
    if [ -d "$dir" ]; then
      phys=$(cd "$dir" 2>/dev/null && pwd -P) || phys=""
      if [ -n "$phys" ]; then
        _rx_resolved="${phys%/}$rest"
        return 0
      fi
    fi
    [ "$dir" != "/" ] || return 0
    base="${dir##*/}"
    rest="/$base$rest"
    dir="${dir%/*}"
    [ -n "$dir" ] || dir="/"
  done
}

# roborev_snapshot_path_binding <path>: is `<path>` a roborev snapshot belonging to THE REVIEW
# WE RAN? Exit status is the state; `_rx_snap_bound_path` carries the resolved path and
# `_rx_snap_bind_state` the state token:
#   0  ok             — absolute, inside `$REPO`, and under `$REPO/.roborev/roborev-snapshot-<id>/`
#   1  not-absolute   — a relative path would resolve against THIS PROCESS's cwd, not the repo
#   2  foreign-repo   — outside the reviewed repository entirely
#   3  unbound-job    — inside the repo but not a roborev snapshot directory for this review
#
# THE BINDING IS THE POINT, and it has two halves. PROVENANCE: the path is read ONLY from the
# prompt of the job whose record `job-record:`/`sha-assert:` already verified — nothing here
# ever SEARCHES the filesystem for a snapshot, so a leftover directory cannot volunteer itself.
# SHAPE: even so, the named path must be inside the reviewed repo and must sit under that repo's
# own `.roborev/roborev-snapshot-<id>/`, so a path from another checkout (the reviewed change's
# prose naming an absolute path, a stale worktree) cannot become the oracle for this review.
#
# COMPONENT-WISE, never a `case` glob: bash's `*` crosses `/`, so `.roborev/roborev-snapshot-*/*`
# would also accept `.roborev/roborev-snapshot-x/../../elsewhere/f.diff`. Splitting the relative
# path into components makes `..`, `.` and an empty component unexpressible as "inside".
#
# THE DECLARED RESIDUAL: this pins roborev's CURRENT snapshot layout. If a future build writes
# its snapshots elsewhere, this FAILs closed with `unbound-job` naming the path — a DRIFT FAIL
# costing one re-run plus a one-line update here, chosen over the alternative (accept any path
# the prompt names), which would hand the strongest anti-vacuity key an oracle nobody bound.
roborev_snapshot_path_binding() {
  local p="$1" rel repo_prefix orig_prefix orig_prefix_phys walk i
  local -a parts=() orig_parts=()
  _rx_snap_bind_state=""
  _rx_snap_bind_detail=""
  _rx_snap_bound_path="$p"
  case "$p" in
    /*) ;;
    *) _rx_snap_bind_state="not-absolute"
       _rx_snap_bind_detail="a relative path would resolve against this process's working directory rather than the reviewed repository"
       return 1 ;;
  esac
  # VALIDATE BEFORE YOU NORMALISE (roborev job 12): physical resolution erases `..` segments and
  # directory symlinks, which is exactly what these checks exist to find, so the original spelling is
  # checked first and resolution is used ONLY for the containment test below.
  IFS='/' read -r -a orig_parts <<<"$p"
  for ((i = 0; i < ${#orig_parts[@]}; i++)); do
    case "${orig_parts[$i]}" in
      '.'|'..')
        _rx_snap_bind_state="dot-segment"
        _rx_snap_bind_detail="the path contains a '${orig_parts[$i]}' segment, which resolution would erase — it names one directory and lands in another"
        return 1
        ;;
    esac
  done
  _roborev_resolve_existing_ancestor "$p"
  _rx_snap_bound_path="$_rx_resolved"
  repo_prefix="${REPO%/}"
  case "$_rx_snap_bound_path/" in
    "$repo_prefix"/*) ;;
    *) _rx_snap_bind_state="foreign-repo"
       _rx_snap_bind_detail="'$_rx_snap_bound_path' is not under the reviewed repository '$repo_prefix'"
       return 1 ;;
  esac
  rel="${_rx_snap_bound_path#"$repo_prefix"/}"
  IFS='/' read -r -a parts <<<"$rel"
  orig_prefix="$p"
  for ((i = 0; i < ${#parts[@]}; i++)); do orig_prefix="${orig_prefix%/*}"; done
  [ -n "$orig_prefix" ] || orig_prefix="/"
  orig_prefix_phys=$(cd "$orig_prefix" 2>/dev/null && pwd -P) || orig_prefix_phys=""
  if [ "${orig_prefix_phys%/}" != "$repo_prefix" ]; then
    _rx_snap_bind_state="unresolvable-prefix"
    _rx_snap_bind_detail="the path's own prefix '$orig_prefix' does not resolve to the repository root ('${orig_prefix_phys:-<unresolvable>}')"
    return 1
  fi
  # NO SYMLINK AT ANY COMPONENT WE WOULD TRAVERSE, tested on the ORIGINAL spelling, top down, with
  # observability asked FIRST so "cannot tell" is never read as "not a symlink".
  walk="$orig_prefix"
  for ((i = 0; i < ${#parts[@]}; i++)); do
    walk="$walk/${parts[$i]}"
    _roborev_path_state "$walk" || :
    if [ "$_rx_path_state" = "unreadable" ]; then
      _rx_snap_bind_state="unreadable-component"
      _rx_snap_bind_detail="the component '$walk' could not be observed — ${_rx_path_why:-the lookup did not answer}"
      return 1
    fi
    # observability-justified: `-L` is asked only where the component is PRESENT or verified-absent, so
    # its falsity is a measurement about type rather than a guess about existence.
    if [ -L "$walk" ]; then
      _rx_snap_bind_state="symlinked"
      _rx_snap_bind_detail="the component '$walk' is a SYMLINK, so the path is not the file it appears to name"
      return 1
    fi
  done
  _rx_snap_bind_state="ok"
  return 0
}

# roborev_collect_review_diff_headers <prompt-file>: collect the `diff --git` headers of the diff
# the reviewer was ACTUALLY GIVEN — inline in the prompt, in the snapshot file the prompt names,
# or (the union of) both — into the same three parallel arrays `roborev_collect_prompt_headers`
# fills, and report WHERE they came from.
#
# THE SINGLE SOURCE OF TRUTH IS THE STATE VARIABLE, never this function's exit status: it always
# returns 0, so a caller cannot accidentally treat "no failure signalled" as "a source was
# measured". `ROBOREV_DIFF_SOURCE_STATE` is one of the AFFIRMATIVE states
#   inline    the prompt carried the diff inline; no snapshot was named
#   snapshot  the diff came from the snapshot file the prompt names
#   both      both sources carried headers; the sets are UNIONED (no header is dropped)
#   none      NEITHER source exists. A MEASUREMENT, not an excusal — see the caller.
# or a FAILURE state naming what was wrong with the named snapshot
#   not-absolute / foreign-repo / unbound-job / cleaned-up / missing / unreadable / empty /
#   no-headers / ambiguous / unparseable-path
# `ROBOREV_DIFF_SOURCE_DETAIL` carries the ERROR line for a failure state (each distinct, each
# naming the path), and `ROBOREV_DIFF_SOURCE_PATH` the snapshot path when one was named.
# The caller keys on the AFFIRMATIVE names and fails closed on everything else, so a state added
# here without a decision there cannot inherit a permissive branch.
#
# COLLECT DURING THE RUN, because the snapshot directory is TRANSIENT: `.roborev/roborev-snapshot-<id>/`
# is removed minutes after the review, and "already cleaned up" is its own explicit cause.
#
# A NAMED-BUT-UNUSABLE SNAPSHOT FAILS EVEN WHEN THE PROMPT ALSO CARRIED INLINE HEADERS, and that
# is deliberate: roborev told the reviewer to read the snapshot, so whatever is inline is by
# construction PARTIAL. Falling back to it would let a fraction of the diff certify the whole.
# THE DIFF-SOURCE RESOLVER, after C‴. It answers ONE question — did the prompt deliver the diff inline,
# by snapshot path, or not at all — and for a snapshot it records what could be OBSERVED about it. It no
# longer reads the snapshot's headers, unions header sets, selects among captures or matches identities,
# because no verdict is derived from any of that any more.
#
# THE AFFIRMATIVE STATES, unchanged in spirit and now shorter:
#   inline    the prompt carried `diff --git` headers and named no snapshot  -> certified as before.
#   snapshot  the prompt named a snapshot path                               -> NOTICE, observed+reported.
#   none      neither                                                        -> the census match FAILs.
# There is no failure state left in this function: a snapshot path that is unusable, unsafe to read or
# unobservable is still `snapshot`, with `ROBOREV_SNAPSHOT_UNOBSERVED_WHY` naming what stopped us. That is
# the C‴ trade written down: those conditions used to be FAIL causes because a certification depended on
# them, and nothing depends on them now.
ROBOREV_DIFF_SOURCE_STATE=""
ROBOREV_SNAPSHOT_PATH=""
ROBOREV_SNAPSHOT_DIGEST=""
ROBOREV_SNAPSHOT_BYTES=""
ROBOREV_SNAPSHOT_UNOBSERVED_WHY=""
roborev_collect_review_diff_headers() {
  local prompt="$1" snap_path
  ROBOREV_DIFF_SOURCE_STATE=""
  ROBOREV_SNAPSHOT_PATH=""
  ROBOREV_SNAPSHOT_DIGEST=""
  ROBOREV_SNAPSHOT_BYTES=""
  ROBOREV_SNAPSHOT_UNOBSERVED_WHY=""

  roborev_collect_prompt_headers "$prompt"
  roborev_prompt_snapshot_paths "$prompt"

  if [ "${#_rx_snap_paths[@]}" -eq 0 ] && [ "${_rx_snap_unparseable:-0}" -eq 0 ]; then
    if [ "${#_rx_hdrs[@]}" -gt 0 ]; then
      ROBOREV_DIFF_SOURCE_STATE="inline"
    else
      ROBOREV_DIFF_SOURCE_STATE="none"
    fi
    return 0
  fi

  # SNAPSHOT MODE. Everything below only ever REPORTS.
  ROBOREV_DIFF_SOURCE_STATE="snapshot"
  if [ "${#_rx_snap_paths[@]}" -eq 0 ]; then
    ROBOREV_SNAPSHOT_UNOBSERVED_WHY="the prompt carries roborev's snapshot instruction but no backtick-delimited path could be read from it (${_rx_snap_unparseable:-0} such line(s))"
    return 0
  fi
  if [ "${#_rx_snap_paths[@]}" -gt 1 ]; then
    ROBOREV_SNAPSHOT_UNOBSERVED_WHY="the prompt names ${#_rx_snap_paths[@]} different snapshot diff paths, so which one this review was given is undecidable: ${_rx_snap_paths[*]}"
    return 0
  fi
  snap_path="${_rx_snap_paths[0]}"
  ROBOREV_SNAPSHOT_PATH="$snap_path"
  # THE SAFETY CHECKS SURVIVE C‴ (see the note above): they decide whether the wrapper may READ this
  # path at all, which is independent of certification. A refusal here means "not observed", with the
  # reason, never a silent omission.
  roborev_snapshot_path_binding "$snap_path" || :
  if [ "${_rx_snap_bind_state:-}" != ok ]; then
    ROBOREV_SNAPSHOT_UNOBSERVED_WHY="the path is not safe to read as this repository's snapshot (${_rx_snap_bind_state:-unknown}): ${_rx_snap_bind_detail:-no detail}"
    return 0
  fi
  ROBOREV_SNAPSHOT_PATH="$_rx_snap_bound_path"
  # PREFER WHAT THE OBSERVER SAW WHILE THE REVIEW RAN, because roborev deletes the snapshot before
  # `--wait` returns; fall back to digesting the live file when it is somehow still there.
  if roborev_snapshot_observation_for "$_rx_snap_bound_path"; then
    ROBOREV_SNAPSHOT_DIGEST="$_rx_obs_digest"
    ROBOREV_SNAPSHOT_BYTES="$_rx_obs_bytes"
    return 0
  fi
  _roborev_regular_readable_state "$_rx_snap_bound_path" || :
  case "$_rx_path_state" in
    present)
      if _roborev_file_digest "$_rx_snap_bound_path"; then
        ROBOREV_SNAPSHOT_DIGEST="$_rx_digest"
        ROBOREV_SNAPSHOT_BYTES=$(wc -c <"$_rx_snap_bound_path" 2>/dev/null | tr -d '[:space:]')
        return 0
      fi
      ROBOREV_SNAPSHOT_UNOBSERVED_WHY="the snapshot is still present but no digest could be taken of it (no sha256sum/shasum/cksum?)"
      ;;
    verified-absent)
      ROBOREV_SNAPSHOT_UNOBSERVED_WHY="the snapshot was already deleted by roborev before this check ran, and the observer recorded nothing for it while the review was running"
      ;;
    *)
      ROBOREV_SNAPSHOT_UNOBSERVED_WHY="the snapshot could not be observed: ${_rx_path_why:-the lookup did not answer}"
      ;;
  esac
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
