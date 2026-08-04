#!/usr/bin/env bash
# roborev-review-oracles.sh — the LOCAL oracles behind roborev-review.sh (#2964/#3229).
#
# SOURCED, never executed: it defines `roborev_push_assert`, `roborev_census` and
# `roborev_check_census_exclusion`, which run inside the wrapper's scope and use its
# state (REPO, BRANCH, HEAD_SHA, BASE, DETAILS, the summary-state variables) and its
# `finish` function.
#
# Why these live together, and apart from the wrapper: they are the change's
# whole thesis in code — every claim is judged against data WE obtain, never against
# a proxy or against the reviewer's prose. All three learned that the hard way:
#   * push-assert read the local `refs/remotes/<remote>/<branch>` mirror, which a
#     narrow fetch refspec never creates, so it false-FAILed 100% of the fleet. It
#     now asks the REMOTE via `git ls-remote`.
#   * the census discarded `git diff`'s exit status, so a FAILED diff rendered as
#     "0 files, genuinely empty" — asserting a measurement that never happened.
#   * the exclusion reconciliation (#3229) replaces a PROSE COMMENT that credited
#     roborev with a code/non-code judgement it does not make. The truth is narrower:
#     **roborev drops exactly what its configured `exclude_patterns` pathspecs match.**
#     Under the configured `docs/**` that discarded 33 EXECUTABLE harness files on
#     PR #3222. The claim is now COMPUTED with git, pre-enqueue, not asserted.
# Keeping them in one sourced file also keeps the wrapper inside the campsite size
# guidance (issue #1116's spirit; the gate's ratchet covers .rs only).
#
# Self-test: scripts/tests/test_roborev_review_guard.sh (which also asserts the
# wrapper FAILS CLOSED when this file is missing — a silently absent oracles file
# turning both checks into no-ops would be the worst regression possible here).

# shellcheck disable=SC2034
# ^ every variable assigned in this file (PUSH_ASSERT, BASE_SHA, CENSUS_CHECK,
#   CODE_FREE, CENSUS_EXCLUSION, census_*) is READ by the sourcing wrapper, which
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
CODE_FREE_EXTENSIONLESS_PREFIXES="openspec/ docs/ website/ .claude/"

# The DOCS-SCOPED ARTIFACT extension set (issue #3229) — raw run output and
# binary/image blobs a report commits beside itself. THE SINGLE DECLARATION: it is
# imported, never redeclared (`scripts/ci/classify-docs-only.sh` will import it —
# issue #3250).
#
# It exists because the census and `.roborev.toml`'s artifact deny-list must AGREE
# on what an artifact is. The prose set above is only `md markdown mdx txt rst adoc`,
# so `.json`/`.jsonl`/`.log`/`.err`/`.csv`/`.svg` in an artifact directory would otherwise
# count as CODE while the configuration excludes them — and `census-exclusion:` would FAIL
# on every legitimate report PR.
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
# ONE MIRROR, TWO REPRESENTATIONS, ASSERTED STRUCTURALLY. These constants and
# `.roborev.toml`'s `exclude_patterns` are the same fact written twice, and a one-sided
# edit is the standing hazard (#3260 item 2). `scripts/tests/test_roborev_review_guard.sh`
# therefore DERIVES the expected pattern set from the constants below and asserts SET
# EQUALITY against the committed `.roborev.toml`, so drift in either direction FAILs
# `--lite` instead of appearing later as a mystifying `census-exclusion:` failure on
# someone else's report PR. Add an extension or a directory HERE and THERE in one edit.
#
# The two classifications stay INDEPENDENT at RUNTIME (extension+dir here, pathspec
# there); this constant only keeps them in agreement on artifacts. A configuration
# regression is still caught, because the verdict is computed from what the config FILE
# says, not from this list.
CODE_FREE_ARTIFACT_EXTENSIONS="txt json jsonl log err csv png svg gz pdf jfr html mmd tex diff"
# The DIRECTORY GLOBS, in git `:(glob)` pathspec spelling: `*` matches within a single
# path component, `**` matches zero or more components. The configured pattern for each is
# `<glob>/**/*.<ext>` — never a blanket `<glob>/**`, because these directories hold
# EXECUTABLE harness code beside their output and swallowing it is precisely #3229.
#
# AN ARRAY, NOT A SPACE-SEPARATED STRING, for the same load-bearing reason
# `ROBOREV_BUILTIN_EXCLUDES` is one: these values CONTAIN `*`, so iterating an unquoted
# string would PATHNAME-EXPAND them against `$PWD`. Measured while writing this: run from
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
# The PROSE pattern the configuration carries alongside them (slash-less ⇒ RECURSIVE,
# repo-wide), named here so the structural mirror assert can account for every configured
# pattern rather than ignoring the ones it does not generate.
CODE_FREE_PROSE_PATTERN="*.md"

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

# ROBOREV'S BUILT-IN EXCLUDES — PINNED TO `roborev v0.61.2` (issue #3229).
#
# `exclude_patterns` is NOT the whole exclusion set. The binary ALWAYS appends a
# hard-coded lockfile/cache deny-list to the pathspecs it hands git, with no
# configuration switch and no way to opt out. Modelling only the configured half is the
# SAME false-PASS class as reading the wrong config file: today a PR touching
# `Cargo.lock` has it silently dropped from the reviewer's diff while a
# `census-exclusion:` that ignored the built-ins would report it SURVIVING.
#
# RECOVERED FROM THE BINARY, not from documentation or memory: these are the literal
# `:(exclude,glob)`-prefixed pathspec strings present in the v0.61.2 executable
# (`strings -a <bin> | grep -o ':(exclude,glob)[^ ]*'`).
#
# THE MECHANISM, ESTABLISHED FROM THE v0.61.2 BINARY (#3229 round-6 blocker 1) —
# **built-ins are PRE-FORMATTED pathspec CONSTANTS appended to git's argv VERBATIM; they
# do NOT pass through `git.FormatExcludeArgs`, so each contributes EXACTLY ONE pathspec
# and never acquires the `/**` sibling a configured pattern does.** Three independent
# measurements against `/usr/local/bin/roborev`, all reproducible:
#   1. THE PREFIX IS INSIDE THE LITERAL. Each of the 24 is present as the CONTIGUOUS byte
#      string `:(exclude,glob)<pattern>`. A pattern destined for the formatter would be
#      stored BARE (the formatter is what prepends `:(exclude,glob)`), so a baked-in
#      prefix means the string is already a finished git argument.
#   2. LENGTH-BUCKET PACKING PROVES (1) IS NOT LINKER COINCIDENCE. Go's linker packs the
#      rodata string blob in LENGTH order with no terminators, and these 24 pack
#      back-to-back in runs of EQUAL total length INCLUDING the 15-byte prefix — e.g. at
#      one offset `:(exclude,glob)**/bun.lock`, `…**/pdm.lock`, `…**/mix.lock` sit at
#      deltas of exactly 26 = 15 + 11, and `…**/Pipfile.lock`/`Gemfile.lock`/
#      `pubspec.lock`/`Podfile.lock` at deltas of exactly 30. Adjacency in a
#      length-sorted blob is only possible if the prefix is part of the string being
#      sorted.
#   3. ONLY TWO BARE `:(exclude,glob)` CONSTANTS EXIST, and Go deduplicates identical
#      literals — so there is no shared prefix constant for 24 patterns to be formatted
#      against. `:(exclude,glob)` counts 26 occurrences total = the 24 finished pathspecs
#      + the 2 runtime prefix constants the formatter concatenates for CONFIGURED
#      patterns (`:(exclude,glob)` root-anchored, `:(exclude,glob)**/` recursive).
# CORROBORATING SHAPE: the DIRECTORY built-ins carry `/**` HAND-WRITTEN into the literal
# (`**/.beads/**`, `**/.cache/**`, `**/.gocache/**`) while the FILE built-ins do not
# (`**/Cargo.lock`) — nobody writes `/**` onto a string about to be handed to a formatter
# that appends it. And no `:(exclude,glob)**/Cargo.lock/**`-style sibling literal exists
# for ANY file built-in (measured: 0 hits for all 8 probed).
#
# WHY IT MATTERS, i.e. why this is not trivia: `roborev_format_exclude_args` used to run
# built-ins through the port, which manufactured a phantom `:(exclude,glob)**/Cargo.lock/**`
# — an exclusion roborev never applies. OVER-modelling the exclusion set drops paths from
# `prompt-content` coverage, which is the FALSE-PASS direction. UNDER-modelling it (the
# mirror-image error of "fixing" a configured pattern down to one pathspec) would report
# paths as SURVIVING that roborev really drops — also a false PASS. Both directions are
# wrong; the pathspec count must match the mechanism per input class, and it now does.
#
# MAINTENANCE OBLIGATION, identical to the port's: a roborev UPGRADE requires
# RE-EXTRACTING this list before the check is trusted. An upstream addition would
# silently widen the real exclusion set while every summary block still read `PASS`.
#
# MESSAGED DISTINCTLY, deliberately: a built-in swallow is NOT a defect in this repo's
# configuration and editing `.roborev.toml` cannot fix it, so the two causes must never
# share a remedy line.
#
# DECLARED AS AN ARRAY, NOT A SPACE-SEPARATED STRING, and that is load-bearing: iterating
# an unquoted string performs PATHNAME EXPANSION, so `**/package-lock.json` silently
# became the repo-relative `website/package-lock.json` — which then read as "a pinned
# pattern is no longer present in the binary" and FAILed every run. (Caught by this very
# check running against the real binary. The identical hazard is called out for
# `roborev config get` parsing below; an array removes it structurally rather than by
# remembering to quote.)
ROBOREV_BUILTIN_EXCLUDES=(
  '**/.beads/**' '**/.cache/**' '**/.gocache/**' '**/.kata.local.toml'
  '**/Cargo.lock' '**/cargo.lock' '**/Gemfile.lock' '**/Package.resolved'
  '**/Pipfile.lock' '**/Podfile.lock' '**/bun.lock' '**/bun.lockb'
  '**/composer.lock' '**/flake.lock' '**/go.sum' '**/mix.lock'
  '**/package-lock.json' '**/packages.lock.json' '**/pdm.lock' '**/pnpm-lock.yaml'
  '**/poetry.lock' '**/pubspec.lock' '**/uv.lock' '**/yarn.lock'
)
ROBOREV_BUILTIN_SRC_LABEL="roborev-builtin"
# The TOTAL number of `:(exclude,glob)` literals the pinned v0.61.2 executable carries:
# the 24 patterns above PLUS the 2 bare PREFIX CONSTANTS the algorithm concatenates
# (`:(exclude,glob)` for root-anchored, `:(exclude,glob)**/` for recursive). 24 + 2 = 26.
# This count is what makes an ADDED built-in observable: see
# `roborev_observe_builtin_excludes` for why a count is used rather than a blind
# full-set re-extraction.
ROBOREV_BUILTIN_PATHSPEC_LITERALS=26
# THE VERSION EVERYTHING ABOVE IS PINNED TO, as a MACHINE-CHECKED value rather than only
# prose (#3229 round-7 blocker). Both this deny-list AND the `git.FormatExcludeArgs` port
# were derived from THIS build; the standing obligation on an upgrade is to RE-VERIFY both
# before the check is trusted. `roborev_observe_builtin_excludes` therefore ASKS the
# executable which version it is (`roborev version` prints `roborev v0.61.2`; there is no
# `--version` flag) and treats a mismatch as DIVERGENCE — not as a workaround for a
# missing right boundary, but because "the pin holds" is exactly the claim the version
# answers. A version that cannot be read is UNAVAILABLE, never a blessing.
ROBOREV_PINNED_VERSION="v0.61.2"

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
  # `census-exclusion:` and `prompt-content:` each unquoted at a different point in a
  # different way. Patching one consumer per round is a losing game.
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
    # `"docs/`) classify as CODE and false-FAIL `census-exclusion:` under `*.md`.
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
      # shellcheck disable=SC2086 # deliberate split of the space-separated constant
      for prefix in $CODE_FREE_EXTENSIONLESS_PREFIXES; do
        case "$path" in "$prefix"*) file_non_code=1 ;; esac
      done
    fi
    if [ "$file_non_code" -eq 1 ]; then
    census_non_code_files=$((census_non_code_files + 1))
  else
    # The CODE subset is the part of the census we expect the reviewer to be sent:
    # roborev drops exactly what its configured `exclude_patterns` pathspecs match —
    # it makes NO code/non-code judgement — and this repo's configured set is a
    # prose/artifact deny-list mirroring the classification above. That correspondence
    # is not assumed: `roborev_check_census_exclusion` COMPUTES it with git before any
    # review is enqueued (#3229).
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
  # #3222, i.e. it excluded CODE. Which is why
  # `census-exclusion:` below reconciles the census against the configured set instead
  # of trusting the correspondence.
  if [ "$census_non_code_files" -eq "$census_files" ]; then
    CODE_FREE="FAIL (code-free census: $census_non_code_files/$census_files files are documentation/specification text)"
    DETAILS+=("ERROR: code-free: every file in the census ($CENSUS for ${BASE}...HEAD) is documentation/specification prose, and roborev STRUCTURALLY DISCARDS a code-free diff — so this diff CANNOT be certified by roborev at all, whatever verdict it returns. The sanctioned substitute is primary-source verification recorded in the PR (for example 'git show cassandra-5.0.8:<path>' for the source the docs describe). A docs-only change must NEVER record \"roborev clean\".")
    DETAILS+=("ERROR: code-free: no review was enqueued, because a passing verdict on this diff would be meaningless.")
    finish FAIL 1
  fi
  CODE_FREE="PASS"
  }

# =============================================================================
# step 3c: census-exclusion — reconcile the CODE census against the EFFECTIVE
# roborev exclusion set, with git, BEFORE anything is enqueued (issue #3229).
# =============================================================================
#
# WHY IT EXISTS. The wrapper used to ASSERT, in a prose comment, that roborev filtered
# the diff by a code/non-code judgement of its own. It does not, and never did.
# **roborev drops exactly what its configured `exclude_patterns` pathspecs match.** With `exclude_patterns = ['docs/**', '*.md']` that
# discarded 33 EXECUTABLE harness files on PR #3222: a 136-path code census reached
# the reviewer as an EMPTY prompt (`prompt-content: FAIL (136/136 code census paths
# absent)`, 15,443 input / 89 output tokens). `prompt-content:` caught it — AFTER a
# review round had been paid for, and under a key that says "the reviewer did not get
# the files" rather than "your configuration ate them". This check computes the same
# fact deterministically, pre-enqueue, under its own key.
#
# THE SPLIT OF LABOUR. Pathspec CONSTRUCTION is an exact PORT of roborev's
# `git.FormatExcludeArgs`; MATCHING is delegated to git, the same matcher roborev
# delegates to. Neither half is a hand-rolled wildmatch — a second near-miss
# implementation of `WM_PATHNAME` semantics is precisely the class of error this
# check exists to catch.
#
# ============================ THE PORTED ALGORITHM ============================
# PINNED TO `roborev v0.61.2`. Recovered by DISASSEMBLING the stripped Go binary
# (symbols via `.gopclntab`, real text base 0x401000). On the real diff path — callers
# are `git.GetDiffCtx`, `GetDiffLimitedCtx`, `GetRangeDiffCtx`, `GetRangeDiffLimitedCtx`,
# `GetDirtyDiff`, and `prompt.(*Builder).buildSinglePrompt` / `buildRangePrompt` /
# `resolveExcludes`. Verbatim:
#
#     p  = strings.TrimSpace(pattern)
#     p  = strings.TrimRight(p, "/")
#     if p == "" { continue }
#     b0 = p[0]                       // read BEFORE TrimLeft
#     p  = strings.TrimLeft(p, "/")
#     if p == "" { continue }
#     if b0 == '/' || strings.Index(p, "/") >= 0 {
#         prefix = ":(exclude,glob)"       // verbatim — ROOT-ANCHORED
#     } else {
#         prefix = ":(exclude,glob)**/"    // RECURSIVE
#     }
#     out = append(out, prefix+p, prefix+p+"/**")   // TWO pathspecs per pattern
#
# The four consequences are REPLICATED, not approximated:
#   R1 an interior `/` ⇒ VERBATIM + ROOT-ANCHORED. `docs/**/*.json` does NOT match
#      `website/src/content/docs/c.json`, so such a path is reported SURVIVING.
#      Evaluating both a verbatim AND a `**/`-prefixed reading and failing on either
#      is FORBIDDEN — not conservative but WRONG, a false FAIL on report PRs.
#   R2 EVERY pattern emits TWO pathspecs, `<p>` and `<p>/**`. That is how a bare
#      directory name excludes its whole subtree; emitting only the first would MISS
#      a swallow.
#   R3 A TRAILING SLASH INVERTS the anchoring, because TrimRight runs BEFORE the
#      contains-`/` test: `docs/` ⇒ `**/docs` + `**/docs/**` (RECURSIVE), the
#      OPPOSITE of root-anchored `docs/**`. A silent widening of unbounded depth that
#      reads like a tidy-up, so it is a loud FAIL here, diff-independently.
#   R4 A LEADING `/` root-anchors an otherwise-recursive slash-less name:
#      `/README.md` ⇒ `README.md` (root only) vs `README.md` ⇒ `**/README.md`.
#   R5 There is NO negation / re-include form at the instruction level, which is why
#      the configuration must be a deny-list (an allow-list is not expressible).
#
# MAINTENANCE OBLIGATION: a roborev UPGRADE requires RE-VERIFYING this algorithm
# before the check is trusted. An upstream change to `FormatExcludeArgs` would
# silently invalidate the port while every summary block still read `PASS`.
#
# NOT THE SAME MECHANISM, do not conflate: `max_prompt_size`, `exclude_branches` /
# `excluded_branches`, commit-message exclusion (`IsCommitMessageExcluded`), and
# `git.EnsureLocalExcludePattern` (which writes `.git/info/exclude`).
#
# WHY THE FILES AND NOT THE BINARY. No roborev flag prints the resolved pathspecs
# (`review` has no `--dry-run`; `-v` is global-only), so the resolved set cannot be
# obtained from the tool at all. Reading `.roborev.toml` + `~/.roborev/config.toml`
# also keeps the check HERMETIC (the regression suite must vary the CONFIG, not the
# binary) and needs no reordering of the wrapper's `command -v roborev` validation.
# The two lists are UNIONed, matching `config.ResolveExcludePatterns` /
# `loadRepoExcludePatterns`. When `roborev` IS invocable the parse is CORROBORATED
# against `roborev config get exclude_patterns`.
#
# THE DECLARED RESIDUAL, both directions:
#   1. the configuration excludes a path the census calls CODE  ⇒ FAIL, pre-enqueue.
#      This is the defect class the check exists to prevent.
#   2. the census calls a path non-code that the configuration does NOT exclude
#      ⇒ NEVER a failure. The file is simply delivered to the reviewer: bounded
#      NOISE, never blindness.

# roborev_unquote_path <token>: render a git C-QUOTED path token back to its RAW bytes.
#
# THE ONE UNQUOTING IMPLEMENTATION, and it has exactly ONE caller:
# `roborev_diff_header_has_path`. Everything the wrapper gets from git PLUMBING is read
# with `-z` and is therefore already raw (the census's `--numstat -z`, the exclusion
# check's `--name-only -z`), so no census/survivor path is ever quoted and nothing on
# those paths needs this function. What DOES arrive quoted is text produced by SOMEONE
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
# against the `-z` survivor set AND can COLLIDE with a sibling that really is the
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

# _rx_has_slash <string>: true when the string contains a '/'.
_rx_has_slash() { case "$1" in */*) return 0 ;; esac; return 1; }

# roborev_parse_toml_array <value>: append the single-line TOML array's string items
# to `_rx_patterns` (each tagged into `_rx_sources` with `$_rx_src_label`, so a later
# FAIL can name WHICH file the pattern came from — with up to three config sources in
# play, "some file excludes this" is not an actionable message). FAIL CLOSED (return 1
# with `_rx_error` set) rather than guess — "we could not tell" must NEVER be aliased to
# "nothing is excluded".
roborev_parse_toml_array() {
  local v="$1" i=0 n ch q item
  case "$v" in
    '['*) ;;
    *) _rx_error="the exclude_patterns value is not a bracketed array (got '${v:0:60}')"; return 1 ;;
  esac
  v="${v#[}"
  n=${#v}
  while [ "$i" -lt "$n" ]; do
    ch="${v:$i:1}"
    case "$ch" in
      ' '|$'\t'|,) i=$((i + 1)); continue ;;
      ']') return 0 ;;
      "'"|'"') q="$ch"; i=$((i + 1)) ;;
      *)
        _rx_error="unexpected character '$ch' in the exclude_patterns array — only a SINGLE-LINE array of quoted strings is supported (multi-line arrays and bare values are refused rather than guessed)"
        return 1
        ;;
    esac
    item=""
    while :; do
      if [ "$i" -ge "$n" ]; then
        _rx_error="unterminated $q-quoted pattern in the exclude_patterns array"
        return 1
      fi
      ch="${v:$i:1}"
      if [ "$ch" = "$q" ]; then i=$((i + 1)); break; fi
      # TOML basic strings ("...") take backslash escapes; literal strings ('...') do not.
      #
      # AN UNKNOWN ESCAPE IS REFUSED, NOT SWALLOWED. The previous revision took the
      # character after the backslash VERBATIM, so `"a\tb"` yielded the 3-byte `atb`
      # while roborev's TOML decoder yields `a<TAB>b` — a pattern SILENTLY DIFFERENT
      # from the one actually applied, which is the whole failure mode this check
      # exists to prevent. Translate the escapes TOML defines and fail closed on
      # anything else, `\u`/`\U` included (an approximated code point is still a
      # different pattern; write the literal character, or use a 'literal string').
      if [ "$q" = '"' ] && [ "$ch" = '\' ]; then
        i=$((i + 1))
        if [ "$i" -ge "$n" ]; then
          _rx_error="trailing backslash escape in the exclude_patterns array"
          return 1
        fi
        ch="${v:$i:1}"
        case "$ch" in
          '"') ch='"' ;;
          '\') ch='\' ;;
          b) ch=$'\b' ;;
          f) ch=$'\f' ;;
          n) ch=$'\n' ;;
          r) ch=$'\r' ;;
          t) ch=$'\t' ;;
          *)
            _rx_error="unknown escape '\\$ch' inside a \"basic string\" exclude_patterns entry — TOML defines only \\\" \\\\ \\b \\f \\n \\r \\t \\uXXXX \\UXXXXXXXX, and dropping the backslash would compare a DIFFERENT pattern than roborev applies (\"a\\tb\" is a<TAB>b, not atb). Refusing to guess: write the literal character, or use a 'single-quoted literal string'"
            return 1
            ;;
        esac
      fi
      item+="$ch"
      i=$((i + 1))
    done
    _rx_patterns+=("$item")
    _rx_sources+=("$_rx_src_label")
  done
  _rx_error="unterminated exclude_patterns array — the committed value must be a SINGLE-LINE array"
  return 1
}

# roborev_toml_exclude_patterns <file> <source-label>: read the file's TOP-LEVEL
# `exclude_patterns` key. TABLE SCOPING is respected — a same-named key under `[ci]`,
# `[review]`, ... is NOT the top-level key, and this repo's real file has both a
# top-level key and several tables below it. Absent file / absent key contribute
# nothing (which is only a PASS once the BINARY has corroborated that nothing is
# configured — see `roborev_check_census_exclusion`); an unparseable value is an error.
#
# KEY SPELLINGS. TOML admits a bare key AND a quoted key: `exclude_patterns`,
# `"exclude_patterns"` and `'exclude_patterns'` are the SAME key, and roborev honours
# all three (measured against v0.61.2: a file carrying `"exclude_patterns" = ['docs/**',
# '*.md']` makes `roborev config get exclude_patterns` answer `docs/**,*.md`). A parser
# that matched only the bare spelling therefore skipped the line, reported "nothing is
# configured", and silently disabled this guard under the very key whose job is to keep
# it armed. Recognising the quoted forms is still not sufficient on its own — the
# binary corroboration is what covers any spelling not enumerated here.
roborev_toml_exclude_patterns() {
  local file="$1" line t value
  local _rx_src_label="${2:-$file}"
  [ -e "$file" ] || return 0
  if [ ! -r "$file" ]; then
    _rx_error="'$file' exists but is not readable"
    return 0
  fi
  local in_table=0
  while IFS= read -r line || [ -n "$line" ]; do
    t="${line#"${line%%[![:space:]]*}"}"
    case "$t" in
      '' | '#'*) continue ;;
      '['*) in_table=1; continue ;;
    esac
    [ "$in_table" -eq 0 ] || continue
    case "$t" in
      exclude_patterns[[:space:]]*=* | exclude_patterns=*) ;;
      '"exclude_patterns"'[[:space:]]*=* | '"exclude_patterns"'=*) ;;
      "'exclude_patterns'"[[:space:]]*=* | "'exclude_patterns'"=*) ;;
      *) continue ;;
    esac
    _rx_found=1
    value="${t#*=}"
    value="${value#"${value%%[![:space:]]*}"}"
    roborev_parse_toml_array "$value" || return 0
  done <"$file"
  return 0
}

# roborev_resolve_root_checkout <repo>: sets `_rx_root` to the ROOT (main) checkout
# backing `<repo>` — EMPTY when `<repo>` IS that checkout — or sets `_rx_root_error`
# when it cannot be determined at all.
#
# WHY THIS EXISTS (the false PASS it closes, #3229). Under 1:1:1:1 every issue runs in a
# LINKED WORKTREE, so `$REPO` is `.../cqlite-wt/issue-N` — but roborev's daemon binds
# the repository by its `repos.root_path`, which is the ROOT checkout
# (`.../workspace/repo`), and reads THAT checkout's `.roborev.toml`. Reading only
# `$REPO/.roborev.toml` therefore certified a config roborev never consulted: on this
# very branch the worktree carried the narrowed set (7/7 "survive") while the root
# checkout still carried the blanket `['docs/**','*.md']` that the real review actually
# applied (`prompt-content: FAIL (1/7 code census paths absent)`). Corroborating with
# `roborev config get` did not catch it, because that ran from the same wrong cwd.
#
# BOTH FILES ARE EVALUATED, and a swallow in EITHER is a FAIL. Which of the two a given
# roborev build prefers is an internal detail we must not bet on; the union is the only
# reading that cannot produce a false PASS in either direction. When the two are the
# SAME checkout `_rx_root` is emptied so the single file is never double-reported.
roborev_resolve_root_checkout() {
  local repo="$1" gcd="" rc=0 first="" rp_repo rp_root
  _rx_root=""
  _rx_root_error=""
  # PRIMARY: the COMMON git dir. In a linked worktree `--git-dir` is
  # `<root>/.git/worktrees/<name>` while `--git-common-dir` is `<root>/.git`.
  set +e
  gcd=$(git -C "$repo" rev-parse --path-format=absolute --git-common-dir 2>/dev/null)
  rc=$?
  set -e
  if [ "$rc" -ne 0 ] || [ -z "$gcd" ]; then
    # `--path-format` needs git >= 2.31. Without it `--git-common-dir` may answer a
    # path RELATIVE to `$repo` — resolve it rather than reading one file and hoping.
    set +e
    gcd=$(git -C "$repo" rev-parse --git-common-dir 2>/dev/null)
    rc=$?
    set -e
    if [ "$rc" -eq 0 ] && [ -n "$gcd" ]; then
      case "$gcd" in /*) ;; *) gcd="$repo/$gcd" ;; esac
    else
      gcd=""
    fi
  fi
  case "$gcd" in
    */.git) _rx_root="${gcd%/.git}" ;;
    *) _rx_root="" ;;
  esac
  if [ -z "$_rx_root" ] || [ ! -d "$_rx_root" ]; then
    # LAST RESORT: `git worktree list --porcelain` names the MAIN worktree FIRST. This
    # also covers a non-standard `$GIT_DIR` name, where the `*/.git` munge above cannot
    # apply.
    set +e
    first=$(git -C "$repo" worktree list --porcelain 2>/dev/null | sed -n '1s/^worktree //p')
    set -e
    if [ -n "$first" ] && [ -d "$first" ]; then
      _rx_root="$first"
    else
      _rx_root=""
      _rx_root_error="neither 'git rev-parse --git-common-dir' nor 'git worktree list --porcelain' named a usable root checkout for '$repo'"
      return 0
    fi
  fi
  # Normalise BOTH sides before comparing: a symlinked or trailing-slash spelling of one
  # and the same checkout must not read as two sources (it would double-report every
  # pattern and name the wrong remedy).
  rp_repo=$(cd "$repo" 2>/dev/null && pwd -P) || rp_repo="$repo"
  rp_root=$(cd "$_rx_root" 2>/dev/null && pwd -P) || rp_root="$_rx_root"
  if [ "$rp_repo" = "$rp_root" ]; then _rx_root=""; fi
  return 0
}

# roborev_format_exclude_args: THE PORT (see the header block above). Fills
# `_rx_pathspecs` (what git is asked) plus the parallel `_rx_owner_pattern` /
# `_rx_owner_body` / `_rx_owner_src` / `_rx_owner_single` (so a FAIL can name the pattern
# responsible for each path AND the file it came from, and reproduce the SAME pathspec
# count), and sets `_rx_trailing` when a CONFIGURED pattern carries the R3 trailing slash.
#
# TWO INPUT CLASSES, TWO MECHANISMS — and conflating them OVER-MODELS the real exclusion
# set (#3229 round-6 blocker 1):
#   * a CONFIGURED pattern is a USER pattern that roborev pushes through
#     `git.FormatExcludeArgs`, which anchors it and emits TWO pathspecs, `<body>` and
#     `<body>/**`. Both are reproduced here.
#   * a BUILT-IN is NOT a user pattern. It is a PRE-FORMATTED pathspec CONSTANT that the
#     binary appends to git's argv VERBATIM, so it contributes exactly ONE pathspec and
#     is never re-anchored and never given a `/**` sibling. Emitting a sibling for it
#     invented an exclusion roborev does not apply (`:(exclude,glob)**/Cargo.lock/**`),
#     and over-exclusion drops paths from `prompt-content` coverage — the FALSE-PASS
#     direction this whole check exists to close.
#
# ESTABLISHED FROM THE v0.61.2 BINARY, not inferred (see ROBOREV_BUILTIN_EXCLUDES above
# for the full evidence): each built-in is a SINGLE Go string literal that already
# CONTAINS the `:(exclude,glob)` prefix. RE-VERIFY ON EVERY roborev UPGRADE — the same
# obligation the pin itself carries.
roborev_format_exclude_args() {
  local pattern p spaced b0 prefix pidx=-1 psrc
  _rx_pathspecs=()
  _rx_owner_pattern=()
  _rx_owner_body=()
  _rx_owner_src=()
  _rx_owner_single=()
  _rx_trailing=""
  for pattern in "${_rx_patterns[@]}"; do
    pidx=$((pidx + 1))
    psrc="${_rx_sources[$pidx]}"
    # A BUILT-IN bypasses the port ENTIRELY: appended verbatim, ONE pathspec, no
    # trimming, no anchoring test, no `/**` sibling. Deliberately BEFORE the
    # normalisation below rather than a flag threaded through it — the mechanism is "the
    # binary does not call the formatter on these", and the code should say exactly that.
    if [ "$psrc" = "$ROBOREV_BUILTIN_SRC_LABEL" ]; then
      _rx_pathspecs+=(":(exclude,glob)$pattern")
      _rx_owner_pattern+=("$pattern")
      _rx_owner_body+=("$pattern")
      _rx_owner_src+=("$psrc")
      _rx_owner_single+=(1)
      continue
    fi
    # strings.TrimSpace
    p="${pattern#"${pattern%%[![:space:]]*}"}"
    p="${p%"${p##*[![:space:]]}"}"
    spaced="$p"
    # strings.TrimRight(p, "/")
    while case "$p" in */) true ;; *) false ;; esac; do p="${p%/}"; done
    # Empty after trimming ⇒ SKIPPED SILENTLY, exactly as the algorithm does. It is
    # emphatically NOT a match-everything.
    [ -n "$p" ] || continue
    b0="${p:0:1}"
    # strings.TrimLeft(p, "/")
    while case "$p" in /*) true ;; *) false ;; esac; do p="${p#/}"; done
    [ -n "$p" ] || continue
    if [ "$b0" = "/" ] || _rx_has_slash "$p"; then
      prefix=""            # ROOT-ANCHORED (R1/R4)
    else
      prefix="**/"         # RECURSIVE
    fi
    # R3: the trailing slash was already trimmed above, BEFORE the anchoring test —
    # so recording it here is what makes the inversion visible instead of silent.
    #
    # It names the SOURCE FILE too: with a worktree config, a root-checkout config and a
    # global config all in play, "a trailing slash somewhere" is not an editable
    # instruction. (A built-in carries no trailing slash, and is not editable at all, so
    # it can never be the subject of this FAIL.)
    if [ -z "$_rx_trailing" ] && [ "$psrc" != "$ROBOREV_BUILTIN_SRC_LABEL" ] &&
      [ "$spaced" != "$p" ] && [ "${spaced%/}" != "$spaced" ]; then
      if [ "$prefix" = "**/" ]; then
        _rx_trailing="FAIL (trailing-slash pattern '$p/' from $psrc resolves RECURSIVE (**/$p), opposite to '$p/**' — drop the trailing slash deliberately or write '$p/**')"
      else
        # Still a FAIL, unconditionally (the trailing slash is never load-bearing and
        # a one-character edit away from the inversion above), but do not misreport
        # the resolution: this form stayed root-anchored.
        _rx_trailing="FAIL (trailing-slash pattern '$p/' from $psrc is trimmed before the anchoring test, so the slash is at best redundant and one edit away from inverting to RECURSIVE — write '$p' or '$p/**')"
      fi
    fi
    _rx_pathspecs+=(":(exclude,glob)$prefix$p" ":(exclude,glob)$prefix$p/**")
    _rx_owner_pattern+=("$pattern")
    _rx_owner_body+=("$prefix$p")
    _rx_owner_src+=("$psrc")
    _rx_owner_single+=(0)
  done
}

# roborev_observe_builtin_excludes: compare the LIVE built-in deny-list against the
# PINNED 24. Sets `_rx_builtin_state` = OK | DIVERGED | UNAVAILABLE, plus
# `_rx_builtin_missing` (pinned patterns the binary no longer carries) and
# `_rx_builtin_count` / `_rx_builtin_count_note`.
#
# WHY THIS IS A SEPARATE, FAILING CHECK. A swallow by a built-in that IS in the pinned
# set has no remedy, so it is information only (a NOTICE). A DIVERGENCE between the live
# set and the pin is the opposite: it HAS a remedy — re-extract, update the pin, and
# decide whether the new built-in is acceptable — and it is a MECHANISM change, which the
# v0.61.2 pin already obliges us to catch on upgrade rather than assume away. If an
# upgrade added a built-in matching `*.rs` or `scripts/**`, a bare NOTICE would absorb it
# silently, the failure would look like normal operation, and we would be blind again:
# exactly the class this issue exists to close. So divergence FAILs.
#
# WHAT `built-in-set:` VERIFIES, AND WHY AN UNBOUNDED SUBSTRING TEST IS NOT ENOUGH.
# Go string literals are concatenated into one rodata blob with NO terminators, so a scan
# for `:(exclude,glob)<something>` cannot reliably delimit each pattern — MEASURED on this
# very binary, a naive extraction yields truncations (`**/.be`, `**/f`, `**/mix.l`),
# junk-suffixed hits (`**/.cache/**add…`, `**/go.sumBinary file…`) and, worst, a phantom
# `**/git` that is really the bare RECURSIVE PREFIX constant followed by an unrelated
# string. Basing a FAIL on that would red every run. So the check is built from FOUR
# signals, each individually reliable:
#
#   1. REMOVALS, named exactly: each pinned pattern is looked for as a FIXED string
#      `:(exclude,glob)<pattern>`. Hit or no hit — no delimiting required.
#   2. ADDITIONS, detected numerically: the COUNT of `:(exclude,glob)` literals. Any
#      added built-in adds one. The count cannot say WHICH pattern appeared, and it also
#      moves if roborev introduces an unrelated `:(exclude,glob)` string — but that is
#      still a mechanism change in precisely this area, with precisely this remedy, so
#      reporting it is correct rather than a false alarm.
#   3. THE VERSION THE PIN IS FOR (`ROBOREV_PINNED_VERSION`), asked of the executable
#      itself. Every fact modelled in this file — the 24 patterns, their arity, the ported
#      `git.FormatExcludeArgs` — was derived from the v0.61.2 build, and an upgrade
#      obliges a re-verification of ALL of it. So a version mismatch IS the divergence the
#      pin encodes, reported with the observed and pinned versions named. This is the
#      GENERAL signal: any upgrade or rebuild moves it, whatever it did to the patterns.
#   4. A RIGHT BOUNDARY for (1), from the blob's LENGTH-BUCKET ADJACENCY.
#
# (4) EXISTS BECAUSE (1) ALONE IS A FALSE-PASS (#3229 round-7 blocker, REPRODUCED). A
# fixed-string search for `:(exclude,glob)<pattern>` has an exact LEFT boundary (the
# 15-byte prefix) and NO RIGHT ONE, so `**/Cargo.lock` matches INSIDE
# `**/Cargo.lock.bak`. Combined with (2) being a bare count, an equal-length substitution
# is INVISIBLE: patching the v0.61.2 binary so its bucket-28 run reads
# `:(exclude,glob)**/Cargo.lock.bak:(exclude,glob)**/cargo.lock:(exclude,glob)**/flake.lock`
# (4 bytes taken from the preceding string, file size unchanged) leaves the count at
# EXACTLY 26 and the missing list EMPTY — verdict `built-in-set: OK` while the modelled
# exclusion set no longer matches reality. Measured, not hypothesised.
#
# THE BOUNDARY, AND WHY IT IS DERIVED RATHER THAN PINNED. Go's linker packs the rodata
# string blob in LENGTH order, so the 24 finished pathspec literals fall into LENGTH
# BUCKETS, and MEASURED against v0.61.2 each bucket is stored as ONE CONTIGUOUS RUN: the
# 12 buckets are (total literal length → members) 24→go.sum; 25→uv.lock;
# 26→bun/pdm/mix.lock; 27→yarn.lock/bun.lockb/.beads/.cache; 28→Cargo/cargo/flake.lock;
# 29→poetry.lock/.gocache; 30→Pipfile/Gemfile/pubspec/Podfile.lock; 31→composer.lock;
# 32→pnpm-lock.yaml; 34→Package.resolved/.kata.local.toml; 35→package-lock.json;
# 36→packages.lock.json. Inside a run, the byte AFTER a literal is the `:` of the NEXT
# literal — a genuine right boundary. So the invariant checked is, per bucket of k
# members: EXACTLY k-1 of them are immediately followed by another `:(exclude,glob)`
# literal, and exactly one (the run's last) is not. That is DERIVED FROM THE PINNED
# PATTERN LIST ALONE (group by length; expect k-1) — it pins NO foreign bytes and NO
# within-bucket ORDER, so it cannot false-FAIL on a rebuild that merely permutes a bucket.
# Summed over the 12 buckets: 12 bounded, 12 run-enders. The tamper above moves bucket 28
# from 2 bounded to 1, and FAILs naming the bucket and the unbounded member.
#
# DECLARED RESIDUALS, both narrowed by (3):
#   * the LAST literal of each of the 12 runs has no derivable right boundary — its
#     successor is an unrelated Go string, and pinning those bytes would pin foreign,
#     build-specific data whose drift would FALSE-FAIL a correct binary (the failure mode
#     that gets a guard disabled). An in-place equal-length extension of a run-ENDER
#     inside v0.61.2 therefore stays invisible; any upstream change that really did it
#     would move the version (3) or the count (2).
#   * a NEW pattern that has a PINNED one as a prefix is still not NAMED by (1) — it is
#     detected, as an addition, by (2) and/or as a broken run by (4).
#
# UNAVAILABLE, never FAIL, when the set cannot be observed — `roborev` absent from PATH,
# an unreadable target, a target carrying ZERO `:(exclude,glob)` literals (a wrapper
# script rather than the Go executable), or a target that will not report its version.
# That is what keeps the hermetic suite, which puts a shell stub on PATH, fully
# exercisable: "we could not look" is never a failure and never a blessing. Note the
# asymmetry that keeps this from self-disabling: an unreadable version withholds only the
# OK BLESSING — a positively OBSERVED divergence (1/2/4) still FAILs without it.
roborev_observe_builtin_excludes() {
  _rx_builtin_state="UNAVAILABLE"
  _rx_builtin_missing=()
  _rx_builtin_count=""
  _rx_builtin_count_note=""
  _rx_builtin_version=""
  _rx_builtin_version_note=""
  _rx_builtin_bucket_note=""
  local bin="" p n lit len ver want got k
  bin=$(command -v roborev 2>/dev/null) || bin=""
  { [ -n "$bin" ] && [ -f "$bin" ] && [ -r "$bin" ]; } || return 0
  set +e
  n=$(LC_ALL=C grep -a -o ':(exclude,glob)' "$bin" 2>/dev/null | wc -l | tr -d '[:space:]')
  set -e
  [ -n "${n:-}" ] || n=0
  # ZERO literals ⇒ this is not the Go executable (a stub, a wrapper, a shim). Not
  # observable, so not a verdict in either direction.
  [ "$n" -gt 0 ] || return 0
  _rx_builtin_count="$n"

  # --- (1) presence AND (4) its right boundary, bucketed by literal length -----------
  # `b_bounded` counts the members whose literal is IMMEDIATELY FOLLOWED by another
  # `:(exclude,glob)` literal. A bounded hit PROVES presence, so the presence-only grep
  # runs on the else branch — the two signals are one pass over the pinned list.
  local -A b_k=() b_bounded=() b_members=() b_bounded_names=() b_unbounded_names=() b_missing=()
  for p in "${ROBOREV_BUILTIN_EXCLUDES[@]}"; do
    lit=":(exclude,glob)$p"
    len=${#lit}
    b_k[$len]=$((${b_k[$len]:-0} + 1))
    b_members[$len]="${b_members[$len]:-}${b_members[$len]:+ }$p"
    if LC_ALL=C grep -qFa "$lit:(exclude,glob)" "$bin"; then
      b_bounded[$len]=$((${b_bounded[$len]:-0} + 1))
      b_bounded_names[$len]="${b_bounded_names[$len]:-}${b_bounded_names[$len]:+, }$p"
    elif LC_ALL=C grep -qFa "$lit" "$bin"; then
      b_unbounded_names[$len]="${b_unbounded_names[$len]:-}${b_unbounded_names[$len]:+, }$p"
    else
      _rx_builtin_missing+=("$p")
      b_missing[$len]=1
    fi
  done
  local -a b_lens=()
  mapfile -t b_lens < <(printf '%s\n' "${!b_k[@]}" | sort -n)
  for len in ${b_lens[@]+"${b_lens[@]}"}; do
    # A bucket with a MISSING member has had its run broken BY that removal, so its
    # arithmetic is a CONSEQUENCE of a divergence already named exactly by (1). Reporting
    # both would bury the actionable name under derived noise.
    [ -z "${b_missing[$len]:-}" ] || continue
    k="${b_k[$len]}"
    want=$((k - 1))
    got="${b_bounded[$len]:-0}"
    [ "$got" -ne "$want" ] || continue
    _rx_builtin_bucket_note="${_rx_builtin_bucket_note:-}${_rx_builtin_bucket_note:+; }literal length $len [${b_members[$len]}]: $got of $k bounded on the right by an adjacent ':(exclude,glob)' literal, pinned $want (a length bucket is stored as ONE run, so exactly one member ends it); bounded: ${b_bounded_names[$len]:-<none>}; UNBOUNDED: ${b_unbounded_names[$len]:-<none>}"
  done
  if [ -n "$_rx_builtin_bucket_note" ]; then
    _rx_builtin_bucket_note="pinned pattern(s) present but NOT right-bounded as pinned (an unbounded substring hit also matches a LONGER pattern, e.g. '**/Cargo.lock' inside '**/Cargo.lock.bak'): $_rx_builtin_bucket_note"
  fi

  # --- (2) additions, numerically ----------------------------------------------------
  if [ "$n" -ne "$ROBOREV_BUILTIN_PATHSPEC_LITERALS" ]; then
    _rx_builtin_count_note="observed $n ':(exclude,glob)' literal(s), pinned $ROBOREV_BUILTIN_PATHSPEC_LITERALS (= 24 built-in patterns + 2 prefix constants)"
  fi

  # --- (3) the version the pin is FOR ------------------------------------------------
  # Asked of `$bin` — the SAME file the literals were read from, never a bare `roborev`
  # that PATH might resolve elsewhere. `timeout` when available: this check must not be
  # the thing that hangs a review round. `version`, not `--version`: the latter does not
  # exist.
  set +e
  if command -v timeout >/dev/null 2>&1; then
    ver=$(timeout 20 "$bin" version 2>/dev/null | head -n 1)
  else
    ver=$("$bin" version 2>/dev/null | head -n 1)
  fi
  set -e
  if [[ "${ver:-}" =~ (^|[[:space:]])(v[0-9]+\.[0-9]+\.[0-9]+[0-9A-Za-z.+-]*)([[:space:]]|$) ]]; then
    _rx_builtin_version="${BASH_REMATCH[2]}"
  fi
  if [ -n "$_rx_builtin_version" ] && [ "$_rx_builtin_version" != "$ROBOREV_PINNED_VERSION" ]; then
    _rx_builtin_version_note="the executable reports $_rx_builtin_version, but every fact modelled here was derived from $ROBOREV_PINNED_VERSION"
  fi

  if [ "${#_rx_builtin_missing[@]}" -gt 0 ] || [ -n "$_rx_builtin_count_note" ] ||
    [ -n "$_rx_builtin_bucket_note" ] || [ -n "$_rx_builtin_version_note" ]; then
    _rx_builtin_state="DIVERGED"
  elif [ -z "$_rx_builtin_version" ]; then
    # Everything observable AGREED, but the version — the thing the pin is TO — could not
    # be read, so the pin is unconfirmed. Withhold the blessing, never invent a failure.
    _rx_builtin_state="UNAVAILABLE"
  else
    _rx_builtin_state="OK"
  fi
  return 0
}

# roborev_builtin_state_details: record the built-in observation on a NON-FAILING path.
# "Never silence" is the third clause of the rule: an UNAVAILABLE observation must be
# stated, because an unstated one reads as agreement — and agreement is exactly what we
# have no evidence for.
roborev_builtin_state_details() {
  case "$_rx_builtin_state" in
    OK)
      DETAILS+=("NOTICE: census-exclusion: built-in-set: OK — the live roborev built-in exclude set MATCHES the pinned v0.61.2 set. The pin is corroborated, not assumed, on FOUR observations: the executable reports $_rx_builtin_version (the build every fact here was derived from, so the pin's re-verify-on-upgrade obligation is satisfied for this run); every pinned pattern is present as the literal ':(exclude,glob)<pattern>'; every pattern is RIGHT-BOUNDED by the pinned length-bucket adjacency, so a presence hit cannot be a substring of a LONGER pattern; and $_rx_builtin_count ':(exclude,glob)' literal(s) are present, exactly as pinned. The right-boundary half is load-bearing: an unbounded substring test alone reported OK for a binary whose '**/Cargo.lock' had been replaced by '**/Cargo.lock.bak' at equal length (count 26/26, missing 0).")
      ;;
    UNAVAILABLE)
      DETAILS+=("NOTICE: census-exclusion: built-in-set: UNAVAILABLE — the live roborev built-in deny-list could not be confirmed against the pinned v0.61.2 set ('roborev' absent from PATH, unreadable, a wrapper/stub carrying no ':(exclude,glob)' literals, or a target that would not report its version), so whether it still matches is UNKNOWN. This is deliberately NEITHER a failure NOR a blessing: 'we could not look' is reported in the value line so it cannot be mistaken for agreement. It withholds only the OK blessing — an OBSERVED divergence (a missing pattern, a broken adjacency run, a changed literal count) still FAILs without a readable version. It ALSO withholds the EXCUSAL (#3229 round-9 F1): no census code path is excused from 'prompt-content:' on a model that could not be verified, because the excusal asserts that a path's absence is a DETERMINISTIC property of the pinned mechanism — a claim an unverified mechanism cannot support.")
      ;;
  esac
}

# roborev_corroborate_exclude_patterns: the cross-check against the BINARY.
# A pattern the binary reports that our parse LACKS is a FAIL (that direction can hide
# a swallow); the reverse is a NOTICE; a binary that answers NOWHERE is `UNAVAILABLE`
# and never a failure — which is what keeps the whole check hermetically testable.
#
# IT IS RUN ON EVERY PATH, INCLUDING WHEN THE PARSE FOUND NOTHING (#3229). The previous
# revision returned `PASS (no exclusion patterns configured)` BEFORE calling this — so
# "our parser recognised no key" was aliased to "nothing is configured", the exact
# epistemic error the rest of this file refuses. Measured against roborev v0.61.2: a
# config carrying the QUOTED key `"exclude_patterns" = ['docs/**','*.md']` is HONOURED by
# the binary while the old bare-key match skipped the line — the guard then reported a
# green "nothing configured" and enqueued a review from which every
# `docs/reports/*-artifacts/**` executable was silently dropped. That is #3229
# reintroduced under the key whose whole job is preventing it. Where the parse is empty
# this cross-check is the ONLY oracle, so it must run precisely there.
#
# IT ASKS FROM BOTH CHECKOUTS, for the reason `roborev_resolve_root_checkout` documents:
# `roborev config get` resolves the repo config relative to its CWD, so asking only from
# `$REPO` reproduces Blocker A's blind spot inside the corroboration itself.
#
# The comparison is against the CONFIGURED subset of `_rx_patterns` only: `config get`
# reports configuration, never roborev's built-in lockfile/cache excludes, so including
# those would make every run report a permanent NOTICE.
roborev_corroborate_exclude_patterns() {
  _rx_corroboration="UNAVAILABLE"
  _rx_drift=""
  _rx_drift_cwd=""
  command -v roborev >/dev/null 2>&1 || return 0
  local out rc item known missing_here=0 extra_here=0 p idx answered=0 j
  local -a cwds=("$REPO") reported=() reported_cwd=()
  if [ -n "$_rx_root" ]; then cwds+=("$_rx_root"); fi
  for ((idx = 0; idx < ${#cwds[@]}; idx++)); do
    set +e
    out=$(cd "${cwds[$idx]}" && roborev config get exclude_patterns 2>/dev/null)
    rc=$?
    set -e
    # rc==0 is AUTHORITATIVE even when the output is EMPTY: "the binary says nothing is
    # configured" is corroboration, and treating it as UNAVAILABLE (as the previous
    # revision did) throws away the only evidence available on the empty-parse path.
    [ "$rc" -eq 0 ] || continue
    answered=1
    # The binary prints the configured value (comma-joined, possibly bracketed/quoted).
    out="${out#*=}"
    # STRIP ONLY A VERIFIED OUTER `[…]` CONTAINER (#3229 round 5, blocker 3 / #3260 item
    # 1). The previous revision deleted EVERY `[` and `]` in the string (`${out//[/}` +
    # `${out//]/}`), which DESTROYS a glob CHARACTER CLASS inside a pattern:
    # `src/[Tt]est.rs` came back as `src/Ttest.rs`, matched nothing in the parsed set,
    # and reported `corroboration: DRIFT` ⇒ a pre-enqueue FAIL on a CORRECT
    # configuration. That direction is fail-closed and loud, so it could never certify
    # unreviewed code — but a guard that reds a legitimate config is the guard that gets
    # disabled, which is how #3229 happened in the first place.
    # Trim first, so the container test sees the brackets at the string's edges.
    out="${out#"${out%%[![:space:]]*}"}"
    out="${out%"${out##*[![:space:]]}"}"
    case "$out" in
      '['*']') out="${out#\[}"; out="${out%\]}" ;;
    esac
    # `read -a` with IFS=',' — NEVER an unquoted `for item in $out`, which would
    # PATHNAME-EXPAND a pattern like `*.md` against $PWD.
    local -a reported_raw=()
    IFS=',' read -r -a reported_raw <<<"$out"
    for item in ${reported_raw[@]+"${reported_raw[@]}"}; do
      item="${item#"${item%%[![:space:]]*}"}"
      item="${item%"${item##*[![:space:]]}"}"
      item="${item#\'}"; item="${item%\'}"
      item="${item#\"}"; item="${item%\"}"
      [ -n "$item" ] || continue
      reported+=("$item")
      reported_cwd+=("${cwds[$idx]}")
    done
  done
  [ "$answered" -eq 1 ] || return 0
  for ((j = 0; j < ${#reported[@]}; j++)); do
    item="${reported[$j]}"
    known=0
    for ((idx = 0; idx < ${#_rx_patterns[@]}; idx++)); do
      [ "${_rx_sources[$idx]}" != "$ROBOREV_BUILTIN_SRC_LABEL" ] || continue
      [ "${_rx_patterns[$idx]}" != "$item" ] || known=1
    done
    if [ "$known" -eq 0 ]; then
      missing_here=1
      _rx_drift="$item"
      _rx_drift_cwd="${reported_cwd[$j]}"
    fi
  done
  for ((idx = 0; idx < ${#_rx_patterns[@]}; idx++)); do
    [ "${_rx_sources[$idx]}" != "$ROBOREV_BUILTIN_SRC_LABEL" ] || continue
    p="${_rx_patterns[$idx]}"
    known=0
    for item in ${reported[@]+"${reported[@]}"}; do
      [ "$p" != "$item" ] || known=1
    done
    [ "$known" -eq 1 ] || extra_here=1
  done
  if [ "$missing_here" -eq 1 ]; then
    _rx_corroboration="DRIFT"
  elif [ "$extra_here" -eq 1 ]; then
    _rx_corroboration="NOTICE"
  else
    _rx_corroboration="OK"
  fi
  return 0
}

# roborev_check_census_exclusion: sets CENSUS_EXCLUSION; calls `finish` on failure, so
# it never returns when the configured set would swallow census code.
roborev_check_census_exclusion() {
  local repo_cfg="$REPO/.roborev.toml"
  local global_cfg="${HOME:-}/.roborev/config.toml"
  local -a _rx_patterns=() _rx_sources=() _rx_pathspecs=() _rx_owner_pattern=() \
    _rx_owner_body=() _rx_owner_src=() _rx_owner_single=()
  local _rx_error="" _rx_found=0 _rx_trailing="" _rx_corroboration="UNAVAILABLE" \
    _rx_drift="" _rx_drift_cwd="" _rx_unquoted="" _rx_root="" _rx_root_error="" \
    _rx_src_label="" _rx_builtin_state="UNAVAILABLE" _rx_builtin_count="" \
    _rx_builtin_count_note="" _rx_builtin_version="" _rx_builtin_version_note="" \
    _rx_builtin_bucket_note=""
  local -a _rx_builtin_missing=()
  local line

  # --- the CONFIG SOURCES, all of them ------------------------------------------
  # THREE, not one: the worktree's own `.roborev.toml`, the ROOT checkout's (the file
  # roborev's daemon actually binds through `repos.root_path` — see
  # `roborev_resolve_root_checkout`) and the global one. Short SOURCE TAGS accompany
  # every pattern into the FAIL/PASS text, because with more than one file in play
  # "excluded by 'docs/**'" does not tell an operator which file to edit.
  roborev_resolve_root_checkout "$REPO"
  if [ -n "$_rx_root_error" ]; then
    CENSUS_EXCLUSION="FAIL (exclusion set unreadable: the ROOT checkout backing '$REPO' could not be resolved)"
    DETAILS+=("ERROR: census-exclusion: roborev binds a repository by its 'repos.root_path' — the ROOT checkout — and reads THAT checkout's .roborev.toml, so with the root unresolvable the exclusion set roborev will actually apply is UNKNOWN. Failing closed rather than reading only '$repo_cfg' and reporting a PASS about a file roborev may never consult. Cause: $_rx_root_error")
    DETAILS+=("ERROR: census-exclusion: no review was enqueued. Re-run from a normal checkout or linked worktree (an exotic \$GIT_DIR layout is not supported by this check).")
    finish FAIL 1
  fi
  local root_cfg=""
  local repo_tag="repo-config" root_tag="root-config" global_tag="global-config"
  if [ -n "$_rx_root" ]; then
    root_cfg="$_rx_root/.roborev.toml"
    repo_tag="worktree-config"
  fi

  _rx_src_label="$repo_tag"
  roborev_toml_exclude_patterns "$repo_cfg" "$repo_tag"
  if [ -z "$_rx_error" ] && [ -n "$root_cfg" ]; then
    roborev_toml_exclude_patterns "$root_cfg" "$root_tag"
  fi
  if [ -z "$_rx_error" ] && [ -n "${HOME:-}" ]; then
    roborev_toml_exclude_patterns "$global_cfg" "$global_tag"
  fi
  local sources_line="$repo_tag='$repo_cfg'"
  if [ -n "$root_cfg" ]; then sources_line="$sources_line UNION $root_tag='$root_cfg'"; fi
  sources_line="$sources_line UNION $global_tag='$global_cfg'"
  if [ -n "$_rx_error" ]; then
    CENSUS_EXCLUSION="FAIL (exclusion set unreadable: $_rx_error)"
    DETAILS+=("ERROR: census-exclusion: the effective roborev exclusion set could not be read, so whether it would swallow this census's CODE paths is UNKNOWN. Failing closed — 'we could not tell' is never 'nothing is excluded'. Sources: $sources_line. Cause: $_rx_error")
    DETAILS+=("ERROR: census-exclusion: fix the exclude_patterns value (a single-line array of quoted patterns) and re-run. No review was enqueued.")
    finish FAIL 1
  fi
  local n_configured=${#_rx_patterns[@]}

  # --- roborev's OWN built-in excludes, folded into the SAME evaluation ----------
  # They are not configurable and not reported by `config get`, but git applies them
  # exactly like the configured ones, so a census path they eat is just as invisible to
  # the reviewer. Tagged distinctly so the remedy text can differ.
  local _builtin
  for _builtin in "${ROBOREV_BUILTIN_EXCLUDES[@]}"; do
    _rx_patterns+=("$_builtin")
    _rx_sources+=("$ROBOREV_BUILTIN_SRC_LABEL")
  done
  local n_builtin=$((${#_rx_patterns[@]} - n_configured))

  # --- is the LIVE built-in set still the one we pinned? -------------------------
  # Observed here, BEFORE any verdict, so a mechanism change is reported even on a run
  # whose census nothing swallows. `UNAVAILABLE` is recorded and carried into the value
  # line rather than being silently treated as agreement.
  roborev_observe_builtin_excludes

  # --- THE EXCUSAL REQUIRES A VERIFIED MODEL (#3229 round-9 blocker F1) -----------
  # `_rx_builtin_state` has THREE values (OK | UNAVAILABLE | DIVERGED), and the round-8
  # revision tested only `= DIVERGED` / `!= DIVERGED` — a three-state signal tested as
  # two. `UNAVAILABLE` therefore took the PERMISSIVE path: it populated
  # `CENSUS_BUILTIN_EXCLUDED` and told `prompt-content:` NOT to expect those paths, so
  # coverage was EXCUSED on an unverified model while the block read PASS. REPRODUCED: a
  # shim carrying all 24 literals correctly right-bounded but refusing to report its
  # `version` yields `state=UNAVAILABLE, missing=0` — and the excusal still happened.
  # That is "we could not check" rendered as "nothing was wrong", inside the guard whose
  # entire purpose is preventing exactly that.
  #
  # SO THE PERMISSIVE BEHAVIOUR IS KEYED ON THE POSITIVE STATE, never on "not the
  # negative one". Two distinct decisions, deliberately separated:
  #   * whether a built-in swallow can be a NOTICE rather than a FAIL — that turns on
  #     `!= DIVERGED`, and correctly so: only a POSITIVELY OBSERVED divergence is an
  #     actionable mechanism change. An unobservable binary (`roborev` absent from PATH —
  #     the hermetic suite's normal condition) must NOT red every run; failing there would
  #     be the self-disabling guard this change keeps refusing to build.
  #   * whether that swallow EXCUSES the paths from `prompt-content:` — that requires
  #     `= OK`. The excusal is a claim ABOUT THE MECHANISM ("their absence is
  #     deterministic"), and an unverified mechanism cannot support it. Withholding it
  #     fails CLOSED on the excusal WITHOUT failing the run: `prompt-content:` simply goes
  #     on to expect every census code path, and FAILs if the reviewer really did not get
  #     one.
  local builtin_excusal=WITHHELD
  [ "$_rx_builtin_state" != OK ] || builtin_excusal=GRANTED

  # --- CORROBORATION, unconditionally and BEFORE any early return ----------------
  # Especially when `n_configured` is 0: "our parser recognised no key" is NOT "nothing
  # is configured", and here the binary is the only oracle that can tell them apart.
  roborev_corroborate_exclude_patterns
  if [ "$_rx_corroboration" = DRIFT ]; then
    CENSUS_EXCLUSION="FAIL (exclusion set drift: '$_rx_drift' reported by roborev config get is absent from the parsed set)"
    DETAILS+=("ERROR: census-exclusion: 'roborev config get exclude_patterns' run from '$_rx_drift_cwd' reports a pattern ('$_rx_drift') that this wrapper's parse of $sources_line did not see, so the effective set is WIDER than the set just reconciled — an unparsed pattern could be excluding census code invisibly. Failing closed. Bring the configuration back to a single-line array of quoted patterns the parser reads, or fix the parser. No review was enqueued.")
    if [ "$n_configured" -eq 0 ]; then
      DETAILS+=("ERROR: census-exclusion: the parse found NO configured pattern at all while the binary reports at least one — so the guard would otherwise have reported 'no exclusion patterns configured' and enqueued a review against a diff roborev silently narrows. That is issue #3229 reintroduced under the key meant to prevent it, which is why an empty parse is corroborated instead of trusted.")
    fi
    finish FAIL 1
  fi
  if [ "$_rx_corroboration" = NOTICE ]; then
    DETAILS+=("NOTICE: census-exclusion: this wrapper parsed pattern(s) that 'roborev config get exclude_patterns' did not report. That direction can only make the reconciliation STRICTER than reality, so it is a NOTICE, not a failure.")
  fi

  roborev_format_exclude_args
  if [ -n "$_rx_trailing" ]; then
    # DIFF-INDEPENDENT, by decision (#3229 R3): a trailing slash is a configuration
    # defect knowable from the configuration alone, its widening is depth-unbounded and
    # invisible in a block that would otherwise read PASS, and a NOTICE in a block
    # agents skim is exactly how the original `docs/**` survived for months.
    CENSUS_EXCLUSION="$_rx_trailing"
    DETAILS+=("ERROR: census-exclusion: roborev's git.FormatExcludeArgs trims a trailing '/' BEFORE deciding whether the pattern is root-anchored, so 'x/' and 'x/**' behave OPPOSITELY — 'x/' becomes the slash-less 'x' and resolves to the RECURSIVE pathspecs ':(exclude,glob)**/x' + ':(exclude,glob)**/x/**', matching every 'x' directory at ANY depth. This FAIL is deliberately independent of whether the pattern currently swallows a census path: the widening is unbounded and silent.")
    DETAILS+=("ERROR: census-exclusion: no review was enqueued. The pattern's SOURCE is named in the value line above; edit that file ($sources_line) to remove the trailing slash.")
    finish FAIL 1
  fi

  # How the PASS line describes the CONFIGURED half. The built-in half is always
  # evaluated, so "nothing is configured" must never read as "nothing is excluded".
  local n_conf_effective=0 idx
  for ((idx = 0; idx < ${#_rx_owner_src[@]}; idx++)); do
    [ "${_rx_owner_src[$idx]}" != "$ROBOREV_BUILTIN_SRC_LABEL" ] || continue
    n_conf_effective=$((n_conf_effective + 1))
  done
  # `pass_prefix` leads the PASS parenthetical when the CONFIGURED half contributes
  # nothing, so "nothing is configured" can never be mistaken for "nothing is excluded" —
  # the built-in half is still named, counted and reconciled.
  local pass_prefix="" survive_of="the effective exclusion set"
  if [ "$n_configured" -eq 0 ]; then
    pass_prefix="no exclusion patterns configured; "
    survive_of="the $n_builtin roborev v0.61.2 built-in exclude(s)"
  elif [ "$n_conf_effective" -eq 0 ]; then
    pass_prefix="$n_configured configured pattern(s), all empty after trimming; "
    survive_of="the $n_builtin roborev v0.61.2 built-in exclude(s)"
  fi

  # --- the built-in DIVERGENCE fragment, computed once ---------------------------
  # Assembled here so that whichever verdict wins below can carry it: a divergence must
  # be reported even on a run that swallows nothing, and must not be hidden by (or hide)
  # a coexisting configured swallow.
  local builtin_div="" builtin_div_missing=""
  if [ "$_rx_builtin_state" = DIVERGED ]; then
    # VERSION first: it is the GENERAL signal, and it reframes every other fragment — on a
    # different build the patterns, the count and the port all need re-deriving, so a
    # reader must not diagnose a moved pattern before knowing the binary moved.
    if [ -n "$_rx_builtin_version_note" ]; then
      builtin_div="$_rx_builtin_version_note"
    fi
    if [ "${#_rx_builtin_missing[@]}" -gt 0 ]; then
      builtin_div_missing=$(
        IFS=,
        printf '%s' "${_rx_builtin_missing[*]}"
      )
      [ -z "$builtin_div" ] || builtin_div="$builtin_div; "
      builtin_div="${builtin_div}pinned pattern(s) no longer present in the binary: ${builtin_div_missing//,/, }"
    fi
    if [ -n "$_rx_builtin_bucket_note" ]; then
      [ -z "$builtin_div" ] || builtin_div="$builtin_div; "
      builtin_div="$builtin_div$_rx_builtin_bucket_note"
    fi
    if [ -n "$_rx_builtin_count_note" ]; then
      [ -z "$builtin_div" ] || builtin_div="$builtin_div; "
      builtin_div="$builtin_div$_rx_builtin_count_note"
    fi
  fi

  local n_code=${#census_code_paths[@]}
  local -a swallowed=()
  local -A _rx_blame=() _rx_blame_src=()
  local n_builtin_hits=0 n_config_hits=0 m=0 joined="" path
  local surv_file="$LOG.exclusion"
  local i=0 body="" matched="" shown=0

  if [ "$n_code" -gt 0 ]; then
  # MATCHING IS GIT'S JOB. `--no-renames` matches the census's own diff, or the two
  # path sets would not be comparable. `-z` for NUL-safety.
  local rc=0
  set +e
  git -C "$REPO" diff --name-only -z --no-renames "${BASE}...HEAD" -- "${_rx_pathspecs[@]}" \
    >"$surv_file" 2>"$surv_file.err"
  rc=$?
  set -e
  if [ "$rc" -ne 0 ]; then
    CENSUS_EXCLUSION="FAIL (exclusion set unreadable: git rejected the constructed pathspecs — exit $rc)"
    DETAILS+=("ERROR: census-exclusion: 'git diff --name-only -z --no-renames ${BASE}...HEAD -- <${#_rx_pathspecs[@]} exclude pathspecs>' exited $rc, so the surviving path set was never measured. Failing closed. git said:")
    while IFS= read -r line; do
      [ -n "$line" ] || continue
      DETAILS+=("  $line")
    done <"$surv_file.err"
    finish FAIL 1
  fi

  local -A _rx_survivor=()
  local sp
  while IFS= read -r -d '' sp; do
    [ -n "$sp" ] || continue
    _rx_survivor["$sp"]=1
  done <"$surv_file"

  # BOTH SIDES ARE ALREADY RAW (#3229 canonical boundary): the census comes from
  # `--numstat -z` and the survivors from `--name-only -z`, so this is a direct byte
  # comparison with no unquoting step to get wrong. This consumer used to unquote here —
  # one of three consumers each doing it at a different point, which is the arrangement
  # that produced a blocker per review round.
  for path in "${census_code_paths[@]}"; do
    if [ -z "${_rx_survivor[$path]:-}" ]; then
      swallowed+=("$path")
    fi
  done

  if [ "${#swallowed[@]}" -gt 0 ]; then
  # --- name each swallowed path AND the pattern that ate it ----------------------
  # Attribution asks git once per pattern, using the POSITIVE form of the SAME two
  # pathspecs, so the blame is computed by the same matcher rather than guessed. Only
  # when something was swallowed, so the common case stays one git call.
  for ((i = 0; i < ${#_rx_owner_body[@]}; i++)); do
    body="${_rx_owner_body[$i]}"
    # The POSITIVE form must be the SAME pathspec set the exclusion used, or blame
    # attributes a path to a pattern that did not eat it. A BUILT-IN contributes ONE
    # verbatim pathspec (no `/**` sibling — see roborev_format_exclude_args), so asking
    # for a sibling here would blame `**/Cargo.lock` for a `Cargo.lock/` subtree the
    # real exclusion never touched.
    local -a _blame_spec=(":(glob)$body")
    if [ "${_rx_owner_single[$i]}" -ne 1 ]; then _blame_spec+=(":(glob)$body/**"); fi
    set +e
    git -C "$REPO" diff --name-only -z --no-renames "${BASE}...HEAD" \
      -- "${_blame_spec[@]}" >"$surv_file.blame" 2>/dev/null
    set -e
    while IFS= read -r -d '' matched; do
      [ -n "$matched" ] || continue
      if [ -z "${_rx_blame[$matched]:-}" ]; then
        _rx_blame["$matched"]="${_rx_owner_pattern[$i]}"
        _rx_blame_src["$matched"]="${_rx_owner_src[$i]}"
      fi
    done <"$surv_file.blame"
  done

  for path in "${swallowed[@]}"; do
    if [ "${_rx_blame_src[$path]:-}" = "$ROBOREV_BUILTIN_SRC_LABEL" ]; then
      n_builtin_hits=$((n_builtin_hits + 1))
    else
      n_config_hits=$((n_config_hits + 1))
    fi
  done
  m=${#swallowed[@]}
  for path in "${swallowed[@]}"; do
    [ "$shown" -lt 10 ] || break
    [ -z "$joined" ] || joined="$joined, "
    joined="$joined$path by '${_rx_blame[$path]:-<unattributed>}' [${_rx_blame_src[$path]:-<unattributed>}]"
    shown=$((shown + 1))
  done
  if [ "$m" -gt "$shown" ]; then joined="$joined (+$((m - shown)) more)"; fi
  fi
  fi   # end: n_code > 0

  # ===================== THE ONE DECISION POINT =====================================
  # THE UNIFYING RULE, applied here and stated in doctrine:
  #   **FAIL where the author can act; NOTICE where only the information is actionable;
  #   never silence.**
  #
  # It resolves three calls that would otherwise be three ad-hoc judgements:
  #   - a CONFIGURED pattern swallowing census code ⇒ FAIL. The remedy is a one-token
  #     edit to a named file; the author can act before paying for a review round.
  #   - a PINNED built-in swallowing census code ⇒ NOTICE. There is NO remedy: the
  #     deny-list is compiled into the binary, with no configuration switch and no
  #     negation form (R5). Failing would red a ROUTINE `Cargo.lock` touch against a
  #     check its author cannot possibly satisfy — and **a guard that fires on correct
  #     input with no available fix is the guard that gets disabled**, which is how
  #     #3229 happened. So: named loudly in the VALUE line, run proceeds.
  #   - the LIVE built-in set DIVERGING from the pinned 24 ⇒ FAIL. This one DOES have a
  #     remedy (re-extract, update the pin, judge the new built-in), and it is a
  #     MECHANISM change, which the v0.61.2 pin already obliges us to catch on upgrade
  #     rather than assume away. A bare NOTICE here would silently absorb an upgrade that
  #     started excluding `*.rs` or `scripts/**`, with the failure looking like normal
  #     operation: the exact blindness this issue exists to close.
  # "Never silence" is the third clause and it is load-bearing: an unobservable built-in
  # set is `UNAVAILABLE` IN THE VALUE LINE, never an unstated assumption of agreement.
  #
  # PRECEDENCE: both FAIL causes outrank the NOTICE, and EVERY cause present is named —
  # the actionable half must never be hidden behind the unactionable one, in either
  # direction.
  #
  # `NOTICE*` is deliberately OUTSIDE the wrapper's failing-capable verdict scan
  # (`FAIL*|FINDINGS*|ERROR*|INCONSISTENT*`), so a NOTICE cannot red `RESULT:`; both FAIL
  # forms below start with `FAIL`, so they can. That correspondence is asserted
  # structurally in the regression suite, against the scan itself.
  local builtin_state_clause="built-in-set: $_rx_builtin_state"
  if [ "$_rx_builtin_state" != DIVERGED ] && [ "$n_config_hits" -eq 0 ]; then
    if [ "$m" -eq 0 ]; then
      local corr_clause="corroboration: $_rx_corroboration"
      [ "$n_code" -gt 0 ] || corr_clause="corroboration: SKIP (no code paths)"
      CENSUS_EXCLUSION="PASS (${pass_prefix}$n_code/$n_code code census paths survive $survive_of; $corr_clause; $builtin_state_clause)"
      roborev_builtin_state_details
      return 0
    fi
    # --- the TOTAL swallow: nothing reaches the reviewer at all ⇒ FAIL --------------
    # A PARTIAL built-in swallow is a NOTICE (above/below): some of the diff still reaches
    # the reviewer, the remainder has no remedy, and failing would red a routine
    # `Cargo.lock` touch — the ruling this change deliberately made.
    #
    # THIS IS NOT A CONTRADICTION OF THAT RULING; it is the SAME rule — "FAIL where the
    # author can act; NOTICE where only the information is actionable; NEVER SILENCE" —
    # applied consistently to a case the NOTICE ruling does not cover. When EVERY code
    # census path is swallowed the reviewer receives an EMPTY prompt, so a returned "no
    # issues found" certifies NOTHING: it is the very same condition `code-free:` already
    # FAILs pre-enqueue for a prose-only census, arrived at by the exclusion set rather
    # than by classification. Reporting a NOTICE here would let `prompt-content:` go on to
    # print `PASS (0/0 code census paths present)` and the block to read `RESULT: PASS` —
    # a VACUOUS pass TEXTUALLY IDENTICAL to a genuine one, on which `flow-closer` would
    # arm `--auto`. MEASURED (#3229 round 3, hermetic fixture `Cargo.lock` + `README.md`):
    # `census-exclusion: NOTICE (0/1 ... survive)` ⇒ `prompt-content: PASS (0/0 ...)` ⇒
    # `RESULT: PASS`, exit 0. Any dependency-bump branch whose only non-prose file is a
    # `Cargo.lock` / `go.sum` / `pnpm-lock.yaml` reaches it, and `code-free:` does NOT
    # catch it because a `.lock` extension classifies as CODE.
    # The remedy is the ACTIONABLE one code-free already prescribes — verify another way
    # and record it — so this FAILs, and it FAILs BEFORE the enqueue: a review of an empty
    # prompt costs a round and certifies nothing.
    if [ "$m" -ge "$n_code" ]; then
      CENSUS_EXCLUSION="FAIL (${pass_prefix}0/$n_code code census paths survive $survive_of; ALL $n_code code census path(s) excluded by a roborev built-in, so the reviewer would receive an EMPTY diff: $joined; corroboration: $_rx_corroboration; $builtin_state_clause)"
      DETAILS+=("ERROR: census-exclusion: EVERY ONE of the $n_code CODE path(s) in this census is dropped from the diff roborev builds by a ROBOREV BUILT-IN exclude (source tag '$ROBOREV_BUILTIN_SRC_LABEL', pinned to v0.61.2: the hard-coded lockfile/cache deny-list), so NOTHING would reach the reviewer. A verdict on an EMPTY prompt certifies nothing, and this diff therefore CANNOT be roborev-certified at all — exactly as a code-free (prose-only) census cannot. The sanctioned substitute is primary-source verification recorded in the PR; this change must NEVER record \"roborev clean\".")
      DETAILS+=("ERROR: census-exclusion: a PARTIAL built-in swallow stays a NOTICE — some of the diff still reaches the reviewer and the rest has no remedy. A TOTAL one FAILs because it is the same condition 'code-free:' already fails pre-enqueue: this is the rule 'FAIL where the author can act; NOTICE where only the information is actionable; never silence' applied consistently, not an exception to it. The actionable remedy is the one code-free prescribes — verify these path(s) another way and record it.")
      DETAILS+=("ERROR: census-exclusion: path(s) the reviewer would NOT receive, each with the built-in responsible:")
      for path in "${swallowed[@]}"; do
        DETAILS+=("  $path  <=  '${_rx_blame[$path]:-<unattributed>}'  [${_rx_blame_src[$path]:-<unattributed>}]")
      done
      DETAILS+=("ERROR: census-exclusion: no review was enqueued — an exclusion set that swallows the WHOLE code census is knowable BEFORE the enqueue, so it costs no review round.")
      roborev_builtin_state_details
      finish FAIL 1
    fi
    # The EXCUSAL clause, and the two detail sentences that depend on it. GRANTED keeps
    # the round-8 wording verbatim; WITHHELD says so in the VALUE LINE, in words that read
    # as neither a clean PASS nor as a DIVERGED set — because the state is neither.
    local excusal_clause="" pin_clause=", and the live built-in set still MATCHES that pin" \
      expect_clause="'prompt-content:' will not expect them either, because their absence is a deterministic property of roborev's mechanism rather than evidence about the reviewer."
    if [ "$builtin_excusal" != GRANTED ]; then
      excusal_clause="excusal WITHHELD: the built-in model is NOT VERIFIED (built-in-set: $_rx_builtin_state), so NO path is excused and 'prompt-content:' still expects all $n_code code census path(s); "
      pin_clause=""
      expect_clause="'prompt-content:' WILL still expect them, because the excusal — the claim that their absence is a DETERMINISTIC property of roborev's mechanism — rests on the live built-in set actually being the pinned one, and that could NOT be verified on this run (built-in-set: $_rx_builtin_state). 'We could not check' is never 'nothing was wrong', so coverage is not excused on an unverified model: if the reviewer really did not receive one of these path(s), 'prompt-content:' FAILs, which is the correct fail-closed outcome here."
    fi
    CENSUS_EXCLUSION="NOTICE (${pass_prefix}$((n_code - m))/$n_code code census paths survive $survive_of; $m code census path(s) excluded by a roborev built-in: $joined; ${excusal_clause}corroboration: $_rx_corroboration; $builtin_state_clause)"
    DETAILS+=("NOTICE: census-exclusion: $m of the $n_code CODE path(s) in this census are dropped from the diff roborev builds by a ROBOREV BUILT-IN exclude (source tag '$ROBOREV_BUILTIN_SRC_LABEL', pinned to v0.61.2: the hard-coded lockfile/cache deny-list)$pin_clause. NO configured pattern is responsible, so there is NOTHING TO FIX in '$repo_cfg' or any other config file — the deny-list is compiled into the binary and has no opt-out. Under the rule 'FAIL where the author can act; NOTICE where only the information is actionable; never silence', this is a NOTICE: a check that fires on a legitimate change (a routine Cargo.lock touch) with no remedy available is a check that gets disabled, which is how #3229 happened.")
    DETAILS+=("NOTICE: census-exclusion: path(s) the reviewer will NOT receive, each with the built-in responsible:")
    for path in "${swallowed[@]}"; do
      DETAILS+=("  $path  <=  '${_rx_blame[$path]:-<unattributed>}'  [${_rx_blame_src[$path]:-<unattributed>}]")
    done
    DETAILS+=("NOTICE: census-exclusion: a clean verdict on this review does NOT cover those path(s) — verify them some other way (primary sources, a regenerate-and-diff, or by reviewing them outside roborev). $expect_clause")
    roborev_builtin_state_details
    # Hand the set to `prompt-content:` so it does not re-report a KNOWN absence as a
    # discovery. TWO conditions, both required:
    #   * the swallow is by a BUILT-IN, not a configured pattern (a configured swallow
    #     FAILs below and never reaches here); and
    #   * the built-in model is VERIFIED (`built-in-set: OK`) — see the excusal block
    #     above. On `UNAVAILABLE` the set stays EMPTY, so `prompt-content:` evaluates
    #     every census code path normally and fails closed if one is really absent.
    if [ "$builtin_excusal" = GRANTED ]; then
      CENSUS_BUILTIN_EXCLUDED=("${swallowed[@]}")
    fi
    return 0
  fi

  # --- a FAIL: a configured swallow, a built-in divergence, or both --------------
  local fail_value=""
  if [ "$n_config_hits" -gt 0 ] || [ "$m" -gt 0 ]; then
    fail_value="$m/$n_code code census paths excluded: $joined"
  fi
  if [ "$_rx_builtin_state" = DIVERGED ]; then
    [ -z "$fail_value" ] || fail_value="$fail_value; ALSO "
    fail_value="${fail_value}roborev built-in exclude set DIVERGED from the pinned v0.61.2 set: $builtin_div"
  fi
  # The built-in state rides on THIS value too (#3229 round-9 F1 audit). This is the ONE
  # reconciliation verdict that used to omit it, which made the documented contract
  # ("every value ends with built-in-set: OK|DIVERGED|UNAVAILABLE") false for exactly the
  # branch where a configured swallow coexists with an UNVERIFIED built-in model — the
  # state stayed silent, and "never silence" is the third clause of the rule, not a
  # PASS-only courtesy.
  CENSUS_EXCLUSION="FAIL ($fail_value; $builtin_state_clause)"

  if [ "$_rx_builtin_state" = DIVERGED ]; then
    DETAILS+=("ERROR: census-exclusion: the LIVE roborev built-in exclude set no longer matches the set PINNED to v0.61.2 in ROBOREV_BUILTIN_EXCLUDES. Divergence: $builtin_div. This is a MECHANISM change, and unlike a pinned built-in it HAS a remedy, which is why it FAILs rather than reporting a NOTICE: re-extract the deny-list from the binary (LC_ALL=C grep -a -o ':(exclude,glob)[^ ]*' \"\$(command -v roborev)\"), update ROBOREV_BUILTIN_EXCLUDES and ROBOREV_BUILTIN_PATHSPEC_LITERALS, and JUDGE the new built-in — an upgrade that started excluding '*.rs' or 'scripts/**' would otherwise be absorbed silently while the block still read green. Re-verify the ported git.FormatExcludeArgs in the same pass.")
    if [ -n "$_rx_builtin_version_note" ]; then
      DETAILS+=("ERROR: census-exclusion: the executable is NOT the build this model was derived from — it reports $_rx_builtin_version, pinned $ROBOREV_PINNED_VERSION. That alone is the divergence: the 24 built-in patterns, their one-pathspec arity AND the ported git.FormatExcludeArgs were all read out of the $ROBOREV_PINNED_VERSION binary, so on any other build EVERY one of them is unverified and the check would be reconciling this diff against a mechanism that is no longer in play. Re-verify all three against the new binary and move ROBOREV_PINNED_VERSION in the same commit — this is the standing re-verify-on-upgrade obligation the pin has always carried, now enforced rather than remembered.")
    fi
    if [ -n "$_rx_builtin_bucket_note" ]; then
      DETAILS+=("ERROR: census-exclusion: a pinned pattern is PRESENT but no longer RIGHT-BOUNDED, which is a divergence a bare presence test cannot see. Go packs the rodata string blob in LENGTH order with no terminator, so a fixed-string search for ':(exclude,glob)<pattern>' has an exact LEFT boundary and NO right one: '**/Cargo.lock' matches inside '**/Cargo.lock.bak'. The boundary used instead is the blob's own structure — each LENGTH BUCKET is stored as ONE contiguous run, so exactly k-1 of a k-member bucket must be immediately followed by another ':(exclude,glob)' literal. A bucket that no longer satisfies that either gained a longer pattern sharing a pinned prefix or had one renamed at equal length; the count (2) and the missing list (1) are both blind to it. Re-extract the deny-list, re-derive the buckets, and JUDGE what the new literal excludes before re-pinning.")
    fi
  fi
  if [ "$m" -eq 0 ]; then
    DETAILS+=("ERROR: census-exclusion: no census code path is currently swallowed — this FAIL is about the MECHANISM having moved under us, not about this diff. It is deliberately diff-independent for the same reason the trailing-slash FAIL is.")
    DETAILS+=("ERROR: census-exclusion: no review was enqueued — the exclusion set roborev will apply is no longer the one this check models, so a reconciliation against it certifies nothing.")
    finish FAIL 1
  fi

  DETAILS+=("ERROR: census-exclusion: the EFFECTIVE roborev exclusion set would remove $m of the $n_code CODE path(s) in this census from the diff roborev builds, so the reviewer would never see them and a clean verdict would be VACUOUS for those files. roborev drops exactly what its pathspecs match — it makes NO code/non-code judgement — so this is a CONFIGURATION defect (or a roborev built-in), not a reviewer one; do NOT go looking at prompt-content or the reviewer.")
  DETAILS+=("ERROR: census-exclusion: config sources read, ALL of them, and a swallow in ANY is a FAIL (which file a given roborev build prefers is an internal detail this check must not bet on): $sources_line.")
  if [ -n "$root_cfg" ]; then
    DETAILS+=("ERROR: census-exclusion: '$REPO' is a LINKED WORKTREE. roborev's daemon binds the repository by its 'repos.root_path' — the ROOT checkout '$_rx_root' — so '$root_cfg' is the file its reviews actually apply. A narrowed worktree config does NOT override it; edit the file the value line names.")
  fi
  if [ "$n_config_hits" -gt 0 ]; then
    DETAILS+=("ERROR: census-exclusion: $n_config_hits of the $m swallowed path(s) were excluded by YOUR CONFIGURATION — editable, and the remedy is to narrow the responsible pattern in the file its source tag names.")
  fi
  if [ "$n_builtin_hits" -gt 0 ]; then
    local soften_because="the $n_config_hits configured swallow(s) above are actionable"
    if [ "$n_config_hits" -eq 0 ]; then
      soften_because="the built-in set has DIVERGED from the pin, which IS actionable"
    fi
    DETAILS+=("ERROR: census-exclusion: $n_builtin_hits of the $m swallowed path(s) were excluded by a ROBOREV BUILT-IN (source tag '$ROBOREV_BUILTIN_SRC_LABEL', pinned to v0.61.2: the hard-coded lockfile/cache deny-list). That direction ALONE would be a NOTICE (it has no remedy — the deny-list is compiled in), but it does NOT soften this FAIL: $soften_because, and the FAIL wins. Every cause is named so the actionable half is not hidden behind the unactionable one. Those built-in path(s) will not reach the reviewer even after you fix what IS fixable; verify them some other way.")
  fi
  DETAILS+=("ERROR: census-exclusion: swallowed path(s), each with the pattern responsible and its source:")
  for path in "${swallowed[@]}"; do
    DETAILS+=("  $path  <=  '${_rx_blame[$path]:-<unattributed>}'  [${_rx_blame_src[$path]:-<unattributed>}]")
  done
  DETAILS+=("ERROR: census-exclusion: the $n_configured configured pattern(s) resolved to these git pathspecs (an exact port of roborev v0.61.2's git.FormatExcludeArgs — a pattern with an interior or leading '/' is ROOT-ANCHORED and verbatim, a slash-less pattern is '**/'-prefixed and RECURSIVE, and every pattern emits BOTH itself and its '/**' sibling):")
  for ((i = 0; i < ${#_rx_owner_body[@]}; i++)); do
    [ "${_rx_owner_src[$i]}" != "$ROBOREV_BUILTIN_SRC_LABEL" ] || continue
    DETAILS+=("  [${_rx_owner_src[$i]}] :(exclude,glob)${_rx_owner_body[$i]}")
    # The sibling is printed only when the pattern actually HAS one. Driven off the same
    # `_rx_owner_single` flag the pathspecs and the blame are, so this listing cannot
    # advertise an exclusion git was never asked for.
    [ "${_rx_owner_single[$i]}" -eq 1 ] ||
      DETAILS+=("  [${_rx_owner_src[$i]}] :(exclude,glob)${_rx_owner_body[$i]}/**")
  done
  DETAILS+=("ERROR: census-exclusion: PLUS $n_builtin roborev v0.61.2 built-in exclude(s) — the hard-coded lockfile/cache deny-list (${ROBOREV_BUILTIN_EXCLUDES[*]}) — which are always applied and are not configurable.")
  DETAILS+=("ERROR: census-exclusion: no review was enqueued — a swallowing exclusion set is knowable BEFORE the enqueue, so it costs no review round.")
  finish FAIL 1
}
