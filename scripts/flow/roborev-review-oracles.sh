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
# over both endpoints; see `roborev_path_is_executable` for how the shape now makes skipping
# one unexpressible.
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

# _roborev_mode_is_exec_at <ref> <path>: 0 iff the tree at `<ref>` records `<path>` with the
# executable bit; 1 for recorded-non-executable, absent-at-that-ref, or unreadable-ref.
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
# error), so content is tested, not just git's exit status.
_roborev_mode_is_exec_at() {
  local ref="$1" path="$2" record mode
  [ -n "$ref" ] || return 1
  record=$(git -C "$REPO" ls-tree "$ref" -- ":(literal)$path" 2>/dev/null) || return 1
  [ -n "$record" ] || return 1
  mode="${record%% *}"
  [ "$mode" = 100755 ]
}

# roborev_path_is_executable <path>: 0 when git records `<path>` executable at EITHER ENDPOINT
# of the census range, 1 only when it is executable at NEITHER (recorded non-executable at
# both, absent from both, or unmeasurable). Used only for the extensionless-under-a-prose-
# prefix decision above.
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
#   present at neither               -> non-executable (unmeasurable; unreachable for a real
#       census path, which by construction exists at an endpoint).
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
#      the accumulator, so control flow cannot leave the fold early; the function's sole
#      `return` is after the loop, once every endpoint has been consulted. (Pinned
#      STRUCTURALLY by the guard test, so a future edit cannot reintroduce an early exit.)
#   3. The per-endpoint predicate is a separate, range-blind function, so there is no
#      "first"/"then" for a reader or an editor to get wrong — only a set to fold.
roborev_path_is_executable() {
  local path="$1" ref exec_at_any=1
  while IFS= read -r ref; do
    # NO early exit of any kind — see property 2 above. Every endpoint is consulted on every
    # call, and the accumulator is monotone, so the result cannot depend on the order.
    if _roborev_mode_is_exec_at "$ref" "$path"; then exec_at_any=0; fi
  done < <(roborev_range_endpoint_refs)
  return "$exec_at_any"
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
  local record add del path
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
      # EXTENSIONLESS under a prose prefix: non-code only when git records it NON-EXECUTABLE
      # AT BOTH ENDPOINTS of the range. The prefix alone used to decide it, which made every
      # extensionless path under `docs/` a path `prompt-content:` asserted NOTHING about; the
      # ordered single-endpoint scan that replaced it did the same to a MODE CHANGE — see the
      # rule documented at `CODE_FREE_EXTENSIONLESS_PREFIXES`. The mode is read from the tree,
      # on this same RAW `$path`, so the classification boundary is unchanged.
      # shellcheck disable=SC2086 # deliberate split of the space-separated constant
      for prefix in $CODE_FREE_EXTENSIONLESS_PREFIXES; do
        case "$path" in
          "$prefix"*)
            if ! roborev_path_is_executable "$path"; then file_non_code=1; fi
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
