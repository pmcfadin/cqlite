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
# It exists because the census and `.roborev.toml`'s docs-scoped deny-list must AGREE
# on what an artifact is. The prose set above is only `md markdown mdx txt rst adoc`,
# so `.json`/`.jsonl`/`.log`/`.err`/`.csv`/`.svg` under `docs/` would otherwise count as
# CODE while the configuration excludes them — and `census-exclusion:` would FAIL on
# every legitimate report PR. It MIRRORS the configured `docs/**/*.<ext>` patterns; add
# an extension here in the same edit that adds it there.
#
# Scoped to `docs/` ONLY (not the wider prose-directory list): the configuration's
# deny-list is root-anchored at `docs/`, so a `.json` under `website/src/content/docs/`
# is CODE to the census AND delivered to the reviewer — the two views agree.
#
# The two classifications stay INDEPENDENT (extension-vs-pathspec); this constant only
# keeps them in agreement on artifacts. A configuration regression is still caught,
# because the verdict is computed from what the config FILE says, not from this list.
CODE_FREE_ARTIFACT_EXTENSIONS="txt json jsonl log err csv png svg gz pdf jfr html mmd tex diff"
CODE_FREE_ARTIFACT_PREFIXES="docs/"

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
  set +e
  NUMSTAT=$(git -C "$REPO" diff --numstat --no-renames "${BASE}...HEAD" 2>&1)
  DIFF_RC=$?
  set -e
  if [ "$DIFF_RC" -ne 0 ]; then
    CENSUS_CHECK="FAIL (git diff failed)"
    DETAILS+=("ERROR: census: 'git diff --numstat --no-renames ${BASE}...HEAD' exited $DIFF_RC in $REPO, so the census was never measured. This is a FAIL, explicitly NOT a NOTHING-TO-REVIEW — an unmeasurable diff is 'we cannot tell', never 'there is nothing to review'. git said:")
    while IFS= read -r line; do
      [ -n "$line" ] || continue
      DETAILS+=("  $line")
    done <<<"$NUMSTAT"
    finish FAIL 1
  fi

  census_files=0
  census_added=0
  census_deleted=0
  census_non_code_files=0
  census_paths=()
  while IFS=$'\t' read -r add del path; do
    [ -n "${path:-}" ] || continue
    if [ "$add" = "-" ]; then add=0; fi
    if [ "$del" = "-" ]; then del=0; fi
    census_files=$((census_files + 1))
    census_paths+=("$path")
    census_added=$((census_added + add))
    census_deleted=$((census_deleted + del))
    # Non-code classification: a documented prose EXTENSION, a docs-scoped ARTIFACT
    # extension (#3229 — mirroring the configured `docs/**/*.<ext>` deny-list), or an
    # EXTENSIONLESS file under a documented prose directory. Anything else — including
    # `docs/foo.py`, `docs/reports/*-artifacts/**/*.sh`, `*.bt` and
    # `.github/workflows/*.yml` — is CODE. A `docs/` path PREFIX never makes a file
    # non-code on its own.
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
        # shellcheck disable=SC2086 # deliberate split of the space-separated constant
        for prefix in $CODE_FREE_ARTIFACT_PREFIXES; do
          case "$path" in "$prefix"*) artifact_dir=1 ;; esac
        done
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
  done <<<"$NUMSTAT"

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

# roborev_unquote_path <path>: render a `git diff --numstat` path back to the RAW
# bytes that `-z` output carries.
#
# NUL-SAFETY, both halves. Survivors are read with `-z` (raw, never quoted, safe for
# spaces and newlines); the census is built from `--numstat` WITHOUT `-z`, which
# C-QUOTES any path containing a double quote, a backslash or a non-ASCII byte — this
# repo tracks exactly such a path,
# `docs/research/CQLite Writes (M5) — Analysis & Recommended Paths.md`. Comparing the
# two renderings directly would mismatch on precisely those paths, so normalise here.
roborev_unquote_path() {
  local p="$1" out="" i n ch
  case "$p" in
    '"'*'"') ;;
    *) printf '%s' "$p"; return 0 ;;
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
      [0-7]) out+=$(printf '%b' "\\0${p:$i:3}"); i=$((i + 3)) ;;
      *) out+="$ch"; i=$((i + 1)) ;;
    esac
  done
  printf '%s' "$out"
}

# _rx_has_slash <string>: true when the string contains a '/'.
_rx_has_slash() { case "$1" in */*) return 0 ;; esac; return 1; }

# roborev_parse_toml_array <value>: append the single-line TOML array's string items
# to `_rx_patterns`. FAIL CLOSED (return 1 with `_rx_error` set) rather than guess —
# "we could not tell" must NEVER be aliased to "nothing is excluded".
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
      if [ "$q" = '"' ] && [ "$ch" = '\' ]; then
        i=$((i + 1))
        if [ "$i" -ge "$n" ]; then
          _rx_error="trailing backslash escape in the exclude_patterns array"
          return 1
        fi
        ch="${v:$i:1}"
      fi
      item+="$ch"
      i=$((i + 1))
    done
    _rx_patterns+=("$item")
  done
  _rx_error="unterminated exclude_patterns array — the committed value must be a SINGLE-LINE array"
  return 1
}

# roborev_toml_exclude_patterns <file>: read the file's TOP-LEVEL `exclude_patterns`
# key. TABLE SCOPING is respected — a same-named key under `[ci]`, `[review]`, ... is
# NOT the top-level key, and this repo's real file has both a top-level key and
# several tables below it. Absent file / absent key contribute nothing (which is a
# PASS: nothing configured cannot swallow anything); an unparseable value is an error.
roborev_toml_exclude_patterns() {
  local file="$1" line t value
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
      *) continue ;;
    esac
    _rx_found=1
    value="${t#*=}"
    value="${value#"${value%%[![:space:]]*}"}"
    roborev_parse_toml_array "$value" || return 0
  done <"$file"
  return 0
}

# roborev_format_exclude_args: THE PORT (see the header block above). Fills
# `_rx_pathspecs` (what git is asked) plus the parallel `_rx_owner_pattern` /
# `_rx_owner_body` (so a FAIL can name the pattern responsible for each path), and
# sets `_rx_trailing` when a pattern carries the R3 trailing slash.
roborev_format_exclude_args() {
  local pattern p spaced b0 prefix
  _rx_pathspecs=()
  _rx_owner_pattern=()
  _rx_owner_body=()
  _rx_trailing=""
  for pattern in "${_rx_patterns[@]}"; do
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
    if [ -z "$_rx_trailing" ] && [ "$spaced" != "$p" ] && [ "${spaced%/}" != "$spaced" ]; then
      if [ "$prefix" = "**/" ]; then
        _rx_trailing="FAIL (trailing-slash pattern '$p/' resolves RECURSIVE (**/$p), opposite to '$p/**' — drop the trailing slash deliberately or write '$p/**')"
      else
        # Still a FAIL, unconditionally (the trailing slash is never load-bearing and
        # a one-character edit away from the inversion above), but do not misreport
        # the resolution: this form stayed root-anchored.
        _rx_trailing="FAIL (trailing-slash pattern '$p/' is trimmed before the anchoring test, so the slash is at best redundant and one edit away from inverting to RECURSIVE — write '$p' or '$p/**')"
      fi
    fi
    _rx_pathspecs+=(":(exclude,glob)$prefix$p" ":(exclude,glob)$prefix$p/**")
    _rx_owner_pattern+=("$pattern")
    _rx_owner_body+=("$prefix$p")
  done
}

# roborev_corroborate_exclude_patterns: OPTIONAL cross-check against the binary.
# A pattern the binary reports that our parse LACKS is a FAIL (that direction can hide
# a swallow); the reverse is a NOTICE; an uninvocable binary is `UNAVAILABLE` and never
# a failure — which is what keeps the whole check hermetically testable.
roborev_corroborate_exclude_patterns() {
  _rx_corroboration="UNAVAILABLE"
  _rx_drift=""
  command -v roborev >/dev/null 2>&1 || return 0
  local out rc reported item known missing_here=0 extra_here=0 p
  set +e
  out=$(cd "$REPO" && roborev config get exclude_patterns 2>/dev/null)
  rc=$?
  set -e
  { [ "$rc" -eq 0 ] && [ -n "$out" ]; } || return 0
  # The binary prints the configured value (comma-joined, possibly bracketed/quoted).
  out="${out#*=}"
  out="${out//[/}"
  out="${out//]/}"
  # `read -a` with IFS=',' — NEVER an unquoted `for item in $out`, which would
  # PATHNAME-EXPAND a pattern like `*.md` against $PWD.
  local -a reported_raw=()
  IFS=',' read -r -a reported_raw <<<"$out"
  reported=()
  for item in ${reported_raw[@]+"${reported_raw[@]}"}; do
    item="${item#"${item%%[![:space:]]*}"}"
    item="${item%"${item##*[![:space:]]}"}"
    item="${item#\'}"; item="${item%\'}"
    item="${item#\"}"; item="${item%\"}"
    [ -n "$item" ] || continue
    reported+=("$item")
  done
  for item in ${reported[@]+"${reported[@]}"}; do
    known=0
    for p in "${_rx_patterns[@]}"; do
      [ "$p" != "$item" ] || known=1
    done
    if [ "$known" -eq 0 ]; then
      missing_here=1
      _rx_drift="$item"
    fi
  done
  for p in "${_rx_patterns[@]}"; do
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
  local -a _rx_patterns=() _rx_pathspecs=() _rx_owner_pattern=() _rx_owner_body=()
  local _rx_error="" _rx_found=0 _rx_trailing="" _rx_corroboration="UNAVAILABLE" _rx_drift=""

  roborev_toml_exclude_patterns "$repo_cfg"
  if [ -z "$_rx_error" ] && [ -n "${HOME:-}" ]; then
    roborev_toml_exclude_patterns "$global_cfg"
  fi
  if [ -n "$_rx_error" ]; then
    CENSUS_EXCLUSION="FAIL (exclusion set unreadable: $_rx_error)"
    DETAILS+=("ERROR: census-exclusion: the effective roborev exclusion set could not be read, so whether it would swallow this census's CODE paths is UNKNOWN. Failing closed — 'we could not tell' is never 'nothing is excluded'. Sources: '$repo_cfg' (repo) UNION '$global_cfg' (global). Cause: $_rx_error")
    DETAILS+=("ERROR: census-exclusion: fix the exclude_patterns value (a single-line array of quoted patterns) and re-run. No review was enqueued.")
    finish FAIL 1
  fi
  if [ "$_rx_found" -eq 0 ] || [ "${#_rx_patterns[@]}" -eq 0 ]; then
    # An absent key/file or a genuinely empty list cannot swallow anything. This is
    # TEXTUALLY DISTINCT from the unreadable FAIL above, on purpose.
    CENSUS_EXCLUSION="PASS (no exclusion patterns configured)"
    return 0
  fi

  roborev_format_exclude_args
  if [ -n "$_rx_trailing" ]; then
    # DIFF-INDEPENDENT, by decision (#3229 R3): a trailing slash is a configuration
    # defect knowable from the configuration alone, its widening is depth-unbounded and
    # invisible in a block that would otherwise read PASS, and a NOTICE in a block
    # agents skim is exactly how the original `docs/**` survived for months.
    CENSUS_EXCLUSION="$_rx_trailing"
    DETAILS+=("ERROR: census-exclusion: roborev's git.FormatExcludeArgs trims a trailing '/' BEFORE deciding whether the pattern is root-anchored, so 'x/' and 'x/**' behave OPPOSITELY — 'x/' becomes the slash-less 'x' and resolves to the RECURSIVE pathspecs ':(exclude,glob)**/x' + ':(exclude,glob)**/x/**', matching every 'x' directory at ANY depth. This FAIL is deliberately independent of whether the pattern currently swallows a census path: the widening is unbounded and silent.")
    DETAILS+=("ERROR: census-exclusion: no review was enqueued. Edit '$repo_cfg' (or the global config) to remove the trailing slash.")
    finish FAIL 1
  fi
  if [ "${#_rx_pathspecs[@]}" -eq 0 ]; then
    CENSUS_EXCLUSION="PASS (${#_rx_patterns[@]} configured pattern(s), all empty after trimming — nothing is excluded)"
    return 0
  fi

  local n_code=${#census_code_paths[@]}
  if [ "$n_code" -eq 0 ]; then
    CENSUS_EXCLUSION="PASS (0/0 code census paths survive the effective exclusion set; corroboration: SKIP (no code paths))"
    return 0
  fi

  # MATCHING IS GIT'S JOB. `--no-renames` matches the census's own diff, or the two
  # path sets would not be comparable. `-z` for NUL-safety.
  local surv_file="$LOG.exclusion"
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

  local -a swallowed=()
  local path raw
  for path in "${census_code_paths[@]}"; do
    raw=$(roborev_unquote_path "$path")
    if [ -z "${_rx_survivor[$raw]:-}" ]; then
      swallowed+=("$raw")
    fi
  done

  if [ "${#swallowed[@]}" -eq 0 ]; then
    roborev_corroborate_exclude_patterns
    if [ "$_rx_corroboration" = DRIFT ]; then
      CENSUS_EXCLUSION="FAIL (exclusion set drift: '$_rx_drift' reported by roborev config get is absent from the parsed set)"
      DETAILS+=("ERROR: census-exclusion: 'roborev config get exclude_patterns' reports a pattern ('$_rx_drift') that this wrapper's parse of '$repo_cfg' did not see, so the effective set is WIDER than the set just reconciled — an unparsed pattern could be excluding census code invisibly. Failing closed. Bring the configuration back to a single-line array of quoted patterns the parser reads, or fix the parser. No review was enqueued.")
      finish FAIL 1
    fi
    if [ "$_rx_corroboration" = NOTICE ]; then
      DETAILS+=("NOTICE: census-exclusion: this wrapper parsed pattern(s) that 'roborev config get exclude_patterns' did not report. That direction can only make the reconciliation STRICTER than reality, so it is a NOTICE, not a failure.")
    fi
    CENSUS_EXCLUSION="PASS ($n_code/$n_code code census paths survive the effective exclusion set; corroboration: $_rx_corroboration)"
    return 0
  fi

  # --- the FAIL path: name each swallowed path AND the pattern that ate it -------
  # Attribution asks git once per pattern, using the POSITIVE form of the SAME two
  # pathspecs, so the blame is computed by the same matcher rather than guessed. Only
  # on the failure path, so the common case stays one git call.
  local -A _rx_blame=()
  local i body matched
  for ((i = 0; i < ${#_rx_owner_body[@]}; i++)); do
    body="${_rx_owner_body[$i]}"
    set +e
    git -C "$REPO" diff --name-only -z --no-renames "${BASE}...HEAD" \
      -- ":(glob)$body" ":(glob)$body/**" >"$surv_file.blame" 2>/dev/null
    set -e
    while IFS= read -r -d '' matched; do
      [ -n "$matched" ] || continue
      [ -n "${_rx_blame[$matched]:-}" ] || _rx_blame["$matched"]="${_rx_owner_pattern[$i]}"
    done <"$surv_file.blame"
  done

  local m=${#swallowed[@]} shown=0 joined=""
  for path in "${swallowed[@]}"; do
    [ "$shown" -lt 10 ] || break
    [ -z "$joined" ] || joined="$joined, "
    joined="$joined$path by '${_rx_blame[$path]:-<unattributed>}'"
    shown=$((shown + 1))
  done
  if [ "$m" -gt "$shown" ]; then joined="$joined (+$((m - shown)) more)"; fi
  CENSUS_EXCLUSION="FAIL ($m/$n_code code census paths excluded: $joined)"

  DETAILS+=("ERROR: census-exclusion: the EFFECTIVE roborev exclusion set would remove $m of the $n_code CODE path(s) in this census from the diff roborev builds, so the reviewer would never see them and a clean verdict would be VACUOUS for those files. roborev drops exactly what its configured pathspecs match — it makes NO code/non-code judgement — so this is a CONFIGURATION defect, not a reviewer one. Fix '$repo_cfg' (or the global '$global_cfg'); do NOT go looking at prompt-content or the reviewer.")
  DETAILS+=("ERROR: census-exclusion: swallowed path(s), each with the pattern responsible:")
  for path in "${swallowed[@]}"; do
    DETAILS+=("  $path  <=  '${_rx_blame[$path]:-<unattributed>}'")
  done
  DETAILS+=("ERROR: census-exclusion: the ${#_rx_patterns[@]} configured pattern(s) resolved to these git pathspecs (an exact port of roborev v0.61.2's git.FormatExcludeArgs — a pattern with an interior or leading '/' is ROOT-ANCHORED and verbatim, a slash-less pattern is '**/'-prefixed and RECURSIVE, and every pattern emits BOTH itself and its '/**' sibling):")
  for sp in "${_rx_pathspecs[@]}"; do
    DETAILS+=("  $sp")
  done
  DETAILS+=("ERROR: census-exclusion: no review was enqueued — a swallowing configuration is knowable BEFORE the enqueue, so it costs no review round.")
  finish FAIL 1
}
