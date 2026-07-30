#!/usr/bin/env bash
# roborev-review-oracles.sh — the two LOCAL oracles behind roborev-review.sh (#2964).
#
# SOURCED, never executed: it defines `roborev_push_assert` and `roborev_census`,
# which run inside the wrapper's scope and use its state (REPO, BRANCH, HEAD_SHA,
# BASE, DETAILS, the summary-state variables) and its `finish` function.
#
# Why these two live together, and apart from the wrapper: they are the change's
# whole thesis in code — every claim is judged against data WE obtain, never against
# a proxy or against the reviewer's prose. Both learned that the hard way:
#   * push-assert read the local `refs/remotes/<remote>/<branch>` mirror, which a
#     narrow fetch refspec never creates, so it false-FAILed 100% of the fleet. It
#     now asks the REMOTE via `git ls-remote`.
#   * the census discarded `git diff`'s exit status, so a FAILED diff rendered as
#     "0 files, genuinely empty" — asserting a measurement that never happened.
# Keeping them in one sourced file also keeps the wrapper inside the campsite size
# guidance (issue #1116's spirit; the gate's ratchet covers .rs only).
#
# Self-test: scripts/tests/test_roborev_review_guard.sh (which also asserts the
# wrapper FAILS CLOSED when this file is missing — a silently absent oracles file
# turning both checks into no-ops would be the worst regression possible here).

# shellcheck disable=SC2034
# ^ every variable assigned in this file (PUSH_ASSERT, BASE_SHA, CENSUS_CHECK,
#   CODE_FREE, census_*) is READ by the sourcing wrapper, which shellcheck cannot see
#   when it lints this fragment standalone. Lint the pair with `shellcheck -x
#   scripts/flow/roborev-review.sh` to resolve them across the source boundary.

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
    # Non-code classification: a documented prose EXTENSION, or an EXTENSIONLESS file
    # under a documented prose directory. Anything else — including `docs/foo.py` and
    # `.github/workflows/*.yml` — is code.
    file_non_code=0
    ext=""
    case "$path" in *.*) ext="${path##*.}" ;; esac
    if [ -n "$ext" ]; then
      # shellcheck disable=SC2086 # deliberate split of the space-separated constant
      for candidate in $CODE_FREE_EXTENSIONS; do
        if [ "$ext" = "$candidate" ]; then file_non_code=1; fi
      done
    else
      # shellcheck disable=SC2086 # deliberate split of the space-separated constant
      for prefix in $CODE_FREE_EXTENSIONLESS_PREFIXES; do
        case "$path" in "$prefix"*) file_non_code=1 ;; esac
      done
    fi
    if [ "$file_non_code" -eq 1 ]; then census_non_code_files=$((census_non_code_files + 1)); fi
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
  if [ "$census_non_code_files" -eq "$census_files" ]; then
    CODE_FREE="FAIL (code-free census: $census_non_code_files/$census_files files are documentation/specification text)"
    DETAILS+=("ERROR: code-free: every file in the census ($CENSUS for ${BASE}...HEAD) is documentation/specification prose, and roborev STRUCTURALLY DISCARDS a code-free diff — so this diff CANNOT be certified by roborev at all, whatever verdict it returns. The sanctioned substitute is primary-source verification recorded in the PR (for example 'git show cassandra-5.0.8:<path>' for the source the docs describe). A docs-only change must NEVER record \"roborev clean\".")
    DETAILS+=("ERROR: code-free: no review was enqueued, because a passing verdict on this diff would be meaningless.")
    finish FAIL 1
  fi
  CODE_FREE="PASS"
  CODE_FREE="PASS"
}
