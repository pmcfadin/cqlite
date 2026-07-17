#!/usr/bin/env bash
#
# claim.sh — atomic, issue-scoped claim lock via a slugless fixed-name git ref
# (issue #2665, epic #2664 Phase-1 audit FM2).
#
# WHY THIS EXISTS
# ---------------
# The old claim lock arbitrated a ref NAME derived from a model-chosen slug, so
# two sessions could push `issue-<N>-<different-slug>` and BOTH pushes succeed
# (field: #1632 slug pair). Two sessions branching the SAME `origin/main` tip
# pushed an IDENTICAL SHA, and git reported "Everything up-to-date" to the
# loser — so both thought they won. This script replaces the slug-named lock
# with a single FIXED-name ref per issue:
#
#   refs/claims/issue-<N>
#
# The slug survives only as worktree/PR naming — NEVER as the lock. Because the
# claim commit is a UNIQUE root commit (no parent, empty tree, a per-invocation
# nonce in its message), no two claimants ever compute the same SHA, and a root
# commit can never be a fast-forward of an existing ref. So a plain `git push`
# (no --force) to the fixed-name ref is the whole arbiter:
#
#   - ref absent  -> the push CREATES it            -> we won
#   - ref present -> the push is REJECTED non-ff    -> someone else holds it
#
# git's server-side ref update decides every race; the win/lose verdict is
# purely the push result, re-confirmed by a post-push `ls-remote`.
#
# ADOPTION (resume of a reaped claim) uses compare-and-swap:
#   git push --force-with-lease=refs/claims/issue-<N>:<old-sha> origin <new>:refs/claims/issue-<N>
# so a resurrected ORIGINAL holder loses the fast-forward / lease on its next
# push and detects the loss immediately (fixes the #2467/#2499 two-writer race).
#
# LEGACY GUARD (mixed-fleet safety): older workers still branch-lock with a
# `refs/heads/issue-<N>-*` branch. `claim` refuses if any such branch exists on
# origin (treat the issue as already-claimed) UNLESS every such branch tip is
# our OWN claim (re-entrancy). New claims never create these branches — the
# `issue-<N>-<slug>` branch is now PR plumbing pushed separately, never the lock.
#
# REF LAYOUT
#   refs/claims/issue-<N>
#     ONE ref per issue. Its commit is a ROOT commit (no parent) pointing at the
#     empty tree; its ONLY payload is the commit message:
#       "claim issue=<N> machine=<machine> pid=<pid> actor=<actor> ts=<ISO8601 UTC> nonce=<nonce>"
#     `verify`/`status` parse identity FROM THE MESSAGE (the pushed record),
#     never the commit date. The author/committer date is stamped to `ts` so the
#     commit metadata agrees with the message.
#
# REMOTE REQUIREMENT (verified)
#   origin MUST accept pushes to the `refs/claims/*` namespace. Confirmed working
#   on github.com/pmcfadin/cqlite on 2026-07-17 (created, ls-remote'd, and deleted
#   refs/claims/smoke-test). When adopting a NEW remote or host, run the one-time
#   preflight `claim.sh smoke` FIRST — a host that restricts custom ref namespaces
#   makes the whole claim mechanism unusable, and that must be caught up front.
#
# SUBCOMMANDS
#   claim  <N> [--actor <id>]                 acquire the lock (CLAIM HELD / CLAIM LOST / CLAIM ERROR)
#   verify <N> [--actor <id>]                 exit 0 iff we hold it (this machine+actor)
#   adopt  <N> --expect <old-sha> [--actor <id>]  compare-and-swap the ref (adoption/resume)
#   release <N> [--force]                     delete the ref (refuses under an open PR w/o --force)
#   status [<N>]                              render claim ref(s) with holder + age
#   smoke                                     one-time preflight: prove refs/claims/* is pushable on origin
#
# IDENTITY
#   machine  CLAIM_MACHINE (default `hostname -s`) — tests override to simulate
#            multiple machines from one clone.
#   actor    --actor <id>, else CLAIM_ACTOR, else "flow" — a sub-machine role.
#   The holder identity that `verify` matches is machine+actor.
#
# ENV
#   CLAIM_REMOTE   origin remote name (default: origin)
#   CLAIM_MACHINE  override machine identity (default: hostname -s)
#   CLAIM_ACTOR    default actor when --actor is omitted (default: flow)
#
# CONSTRAINTS
#   macOS bash 3.2 compatible (no associative arrays, no readarray/mapfile).
#   `set -euo pipefail`, shellcheck-clean. gh is used ONLY by `release` (open-PR
#   guard) and degrades LOUDLY if gh is absent. All informative output is a
#   single line prefixed `CLAIM:`.
#
# EXIT CODES
#   0  success (CLAIM HELD, VERIFY-OK, ADOPTED, RELEASED, SMOKE-OK, status render)
#   2  lost / refused (CLAIM LOST, VERIFY-FAIL, ADOPT-LOST, RELEASE-REFUSED)
#   1  infra / git / gh failure — retryable, NOT a race-loss (CLAIM ERROR reason=infra,
#      SMOKE-FAIL). `claim` NEVER reports LOST when nobody holds the ref: a failed
#      push whose re-read finds the ref absent, or an ls-remote that itself errors,
#      is ERROR (exit 1) so the caller retries rather than skipping the item.
#   64 usage error
#
# ---END-HELP---
set -euo pipefail

prog="$(basename "$0")"

die_usage() { echo "$prog: $*" >&2; exit 64; }
note()      { echo "[claim] $*" >&2; }
emit()      { echo "CLAIM: $*"; }

REMOTE="${CLAIM_REMOTE:-origin}"

print_help() {
  awk 'NR>=2 && /^# ---END-HELP---/{exit} NR>=2 {sub(/^# ?/,""); print}' "$0"
}

# require_numeric_issue <value> <subcommand>
require_numeric_issue() {
  case "${1:-}" in
    *[!0-9]* | '') die_usage "$2 requires a numeric issue number (got '${1:-<none>}')" ;;
  esac
}

# ts_to_epoch <ISO8601 UTC ts> — portable across GNU date and BSD/macOS date.
ts_to_epoch() {
  local ts="$1" epoch
  if epoch=$(date -u -d "$ts" +%s 2>/dev/null); then
    printf '%s\n' "$epoch"; return 0
  fi
  if epoch=$(date -u -j -f '%Y-%m-%dT%H:%M:%SZ' "$ts" +%s 2>/dev/null); then
    printf '%s\n' "$epoch"; return 0
  fi
  return 1
}

# humanize_age <seconds> — coarse, deterministic bucket (s/m/h/d).
humanize_age() {
  local s="$1"
  [ "$s" -lt 0 ] 2>/dev/null && s=0
  if   [ "$s" -lt 60 ];    then printf '%ss\n' "$s"
  elif [ "$s" -lt 3600 ];  then printf '%sm\n' "$((s / 60))"
  elif [ "$s" -lt 86400 ]; then printf '%sh\n' "$((s / 3600))"
  else                          printf '%sd\n' "$((s / 86400))"
  fi
}

this_machine() { printf '%s\n' "${CLAIM_MACHINE:-$(hostname -s)}"; }

# msg_field <message> <key> — extract "<key>=<value>" (value is a non-space run).
msg_field() {
  printf '%s' "$1" | sed -n "s/.*${2}=\\([^ ]*\\).*/\\1/p" | head -1
}

# remote_claim_sha <N> — SHA of refs/claims/issue-<N> on origin ("" if absent).
# Best-effort: an ls-remote failure yields "" (not a hard error) so callers that
# only need "is there a holder" never trip `set -e`; use remote_claim_lookup when
# the infra-vs-absent distinction matters.
remote_claim_sha() {
  git ls-remote "$REMOTE" "refs/claims/issue-$1" 2>/dev/null | awk '{print $1}' | head -1 || true
}

# remote_claim_lookup <N> — like remote_claim_sha but distinguishes an infra
# failure (ls-remote itself errors) from a legitimately-absent ref. Sets the
# global REPLY_SHA (the holder sha, or "" if the ref is absent) and returns:
#   0  ls-remote SUCCEEDED (REPLY_SHA is "" when the ref does not exist)
#   1  ls-remote FAILED (remote unreachable / auth — an infra error, NOT a race)
REPLY_SHA=""
remote_claim_lookup() {
  local out
  if ! out="$(git ls-remote "$REMOTE" "refs/claims/issue-$1" 2>/dev/null)"; then
    REPLY_SHA=""
    return 1
  fi
  REPLY_SHA="$(printf '%s' "$out" | awk '{print $1}' | head -1)"
  return 0
}

# build_claim_commit <N> <actor> — create a UNIQUE root commit (empty tree, no
# parent) and print its SHA. The nonce guarantees distinct SHAs even for two
# claimants at the same base in the same second.
build_claim_commit() {
  local issue="$1" actor="$2"
  local machine pid ts nonce message empty_tree
  machine="$(this_machine)"
  pid="$$"
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  nonce="${pid}-${RANDOM}-${RANDOM}-$(date -u +%s)"
  message="claim issue=${issue} machine=${machine} pid=${pid} actor=${actor} ts=${ts} nonce=${nonce}"
  empty_tree="$(git hash-object -t tree --stdin </dev/null)"
  GIT_AUTHOR_NAME="cqlite-claim" GIT_AUTHOR_EMAIL="claim@cqlite.local" \
    GIT_COMMITTER_NAME="cqlite-claim" GIT_COMMITTER_EMAIL="claim@cqlite.local" \
    GIT_AUTHOR_DATE="$ts" GIT_COMMITTER_DATE="$ts" \
    git commit-tree "$empty_tree" -m "$message"
}

# holder_is_us <sha> <actor> — 0 iff the claim commit was authored by this
# machine+actor (identity match), reading the pushed message.
holder_is_us() {
  local sha="$1" actor="$2" msg h_machine h_actor
  msg="$(git log -1 --format=%B "$sha" 2>/dev/null || true)"
  [ -n "$msg" ] || return 1
  h_machine="$(msg_field "$msg" machine)"
  h_actor="$(msg_field "$msg" actor)"
  [ "$h_machine" = "$(this_machine)" ] && [ "$h_actor" = "$actor" ]
}

# fetch_claim <N> — ensure the claim object is present locally; no-op on absence.
fetch_claim() {
  git fetch "$REMOTE" "refs/claims/issue-$1" >/dev/null 2>&1 || true
}

# holder_desc <sha> — "machine=<m> actor=<a>" for a held ref (best effort).
holder_desc() {
  local msg
  msg="$(git log -1 --format=%B "$1" 2>/dev/null || true)"
  printf 'machine=%s actor=%s' \
    "$(msg_field "$msg" machine)" "$(msg_field "$msg" actor)"
}

# holder_token <sha-or-empty> — the holder field for an emit line. A real sha
# renders "holder-machine=<m> actor=<a>"; an EMPTY read renders an explicit
# "holder=unknown" — never fall back to our OWN commit (which would falsely name
# us as the holder on a lost/empty race).
holder_token() {
  if [ -z "$1" ]; then
    printf 'holder=unknown'
  else
    printf 'holder-%s' "$(holder_desc "$1")"
  fi
}

# legacy_lock_blocks <N> — 0 (blocks) iff a refs/heads/issue-<N>-* branch exists
# on origin whose tip is NOT our own claim. Re-entrancy: all-ours -> no block.
legacy_lock_blocks() {
  local issue="$1" actor="$2" raw ref sha found_other=0 found_any=0
  raw="$(git ls-remote --heads "$REMOTE" "issue-${issue}-*" 2>/dev/null || true)"
  [ -n "$raw" ] || return 1
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    found_any=1
    sha="$(printf '%s' "$line" | awk '{print $1}')"
    ref="$(printf '%s' "$line" | awk '{print $2}')"
    git fetch "$REMOTE" "$ref" >/dev/null 2>&1 || true
    if ! holder_is_us "$sha" "$actor"; then
      found_other=1
    fi
  done <<EOF
$raw
EOF
  [ "$found_any" -eq 1 ] && [ "$found_other" -eq 1 ]
}

# ---------------------------------------------------------------------------
cmd_claim() {
  local issue="" actor="${CLAIM_ACTOR:-flow}"
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --actor) [ "$#" -ge 2 ] || die_usage "--actor requires a value"; actor="$2"; shift 2 ;;
      -*) die_usage "claim: unknown flag $1" ;;
      *) [ -z "$issue" ] || die_usage "claim: unexpected argument $1"; issue="$1"; shift ;;
    esac
  done
  require_numeric_issue "$issue" claim

  # Pre-check: an existing claim ref that is OURS is re-entrant (idempotent win).
  local existing
  existing="$(remote_claim_sha "$issue")"
  if [ -n "$existing" ]; then
    fetch_claim "$issue"
    if holder_is_us "$existing" "$actor"; then
      emit "HELD issue=$issue ref=refs/claims/issue-$issue sha=$existing $(holder_desc "$existing") (re-entrant)"
      return 0
    fi
    emit "LOST issue=$issue ref=refs/claims/issue-$issue sha=$existing $(holder_token "$existing")"
    return 2
  fi

  # Legacy branch-lock guard (mixed fleet). Re-entrancy: our own branch is fine.
  if legacy_lock_blocks "$issue" "$actor"; then
    emit "LOST issue=$issue reason=legacy-branch-lock detail=refs/heads/issue-${issue}-* exists on $REMOTE"
    return 2
  fi

  # Build our unique claim commit and attempt the atomic create.
  local sha
  sha="$(build_claim_commit "$issue" "$actor")"
  if git push "$REMOTE" "${sha}:refs/claims/issue-${issue}" >/dev/null 2>&1; then
    : # push accepted — confirm below.
  else
    # Push failed. Distinguish a genuine race-loss (another holder present) from
    # an infra failure (remote unreachable, or a push error with NO holder) — a
    # LOST verdict must NEVER be emitted when nobody actually holds the ref.
    if ! remote_claim_lookup "$issue"; then
      emit "ERROR issue=$issue reason=infra detail=push-failed-and-ls-remote-unreachable-on-$REMOTE (transient — retry)"
      return 1
    fi
    local now="$REPLY_SHA"
    if [ -z "$now" ]; then
      # Push was rejected yet the ref is absent: not a lost race — a push/infra
      # error. Fail as ERROR (exit 1, retryable), never a bogus LOST.
      emit "ERROR issue=$issue reason=infra detail=push-rejected-but-ref-absent-on-$REMOTE (transient — retry)"
      return 1
    fi
    fetch_claim "$issue"
    if holder_is_us "$now" "$actor"; then
      emit "HELD issue=$issue ref=refs/claims/issue-$issue sha=$now $(holder_desc "$now") (re-entrant)"
      return 0
    fi
    emit "LOST issue=$issue ref=refs/claims/issue-$issue sha=$now $(holder_token "$now")"
    return 2
  fi

  # Post-push re-read: the ref MUST be our SHA (a TOCTOU winner could differ).
  local confirmed
  confirmed="$(remote_claim_sha "$issue")"
  if [ "$confirmed" = "$sha" ]; then
    emit "HELD issue=$issue ref=refs/claims/issue-$issue sha=$sha machine=$(this_machine) actor=$actor"
    return 0
  fi
  fetch_claim "$issue"
  emit "LOST issue=$issue ref=refs/claims/issue-$issue sha=${confirmed:-<gone>} $(holder_token "$confirmed")"
  return 2
}

# ---------------------------------------------------------------------------
cmd_verify() {
  local issue="" actor="${CLAIM_ACTOR:-flow}"
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --actor) [ "$#" -ge 2 ] || die_usage "--actor requires a value"; actor="$2"; shift 2 ;;
      -*) die_usage "verify: unknown flag $1" ;;
      *) [ -z "$issue" ] || die_usage "verify: unexpected argument $1"; issue="$1"; shift ;;
    esac
  done
  require_numeric_issue "$issue" verify

  local sha
  sha="$(remote_claim_sha "$issue")"
  if [ -z "$sha" ]; then
    emit "VERIFY-FAIL issue=$issue reason=no-claim-ref"
    return 2
  fi
  fetch_claim "$issue"
  if holder_is_us "$sha" "$actor"; then
    emit "VERIFY-OK issue=$issue ref=refs/claims/issue-$issue sha=$sha $(holder_desc "$sha")"
    return 0
  fi
  emit "VERIFY-FAIL issue=$issue ref=refs/claims/issue-$issue sha=$sha holder-$(holder_desc "$sha") wanted-machine=$(this_machine) wanted-actor=$actor"
  return 2
}

# ---------------------------------------------------------------------------
cmd_adopt() {
  local issue="" actor="${CLAIM_ACTOR:-flow}" expect=""
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --actor)  [ "$#" -ge 2 ] || die_usage "--actor requires a value";  actor="$2";  shift 2 ;;
      --expect) [ "$#" -ge 2 ] || die_usage "--expect requires a value"; expect="$2"; shift 2 ;;
      -*) die_usage "adopt: unknown flag $1" ;;
      *) [ -z "$issue" ] || die_usage "adopt: unexpected argument $1"; issue="$1"; shift ;;
    esac
  done
  require_numeric_issue "$issue" adopt
  [ -n "$expect" ] || die_usage "adopt requires --expect <old-sha>"

  local sha
  sha="$(build_claim_commit "$issue" "$actor")"
  # Compare-and-swap: replace ONLY if origin is still at <old-sha>.
  if git push --force-with-lease="refs/claims/issue-${issue}:${expect}" \
        "$REMOTE" "${sha}:refs/claims/issue-${issue}" >/dev/null 2>&1; then
    local confirmed
    confirmed="$(remote_claim_sha "$issue")"
    if [ "$confirmed" = "$sha" ]; then
      emit "ADOPTED issue=$issue ref=refs/claims/issue-$issue sha=$sha machine=$(this_machine) actor=$actor from=$expect"
      return 0
    fi
  fi
  local now
  now="$(remote_claim_sha "$issue")"
  fetch_claim "$issue"
  emit "ADOPT-LOST issue=$issue ref=refs/claims/issue-$issue expected=$expect actual=${now:-<gone>} $(holder_token "$now")"
  return 2
}

# ---------------------------------------------------------------------------
# open_pr_count <N> — number of open PRs whose HEAD BRANCH is this issue's
# (`issue-<N>` exact, or the `issue-<N>-<slug>` PR-plumbing prefix). Prints -1
# and a loud note when gh is unavailable/errors (fail loud, never silently
# pretend "0 open"). Deliberately NOT a free-text `--search` on "issue-<N>":
# body-text matching false-positives (#2665 vs #266) and false-negatives (a PR
# that never mentions the issue in prose) — the head-branch name is the exact,
# structural link.
open_pr_count() {
  local issue="$1" refs name count=0
  if ! command -v gh >/dev/null 2>&1; then
    note "gh not found — cannot check for an open PR on issue #$issue"
    printf '%s\n' -1
    return 0
  fi
  if ! refs="$(gh pr list --state open --json headRefName --jq '.[].headRefName' 2>/dev/null)"; then
    note "gh pr list failed — cannot check for an open PR on issue #$issue"
    printf '%s\n' -1
    return 0
  fi
  while IFS= read -r name; do
    [ -n "$name" ] || continue
    case "$name" in
      "issue-${issue}" | "issue-${issue}-"*) count=$((count + 1)) ;;
    esac
  done <<EOF
$refs
EOF
  printf '%s\n' "$count"
}

cmd_release() {
  local issue="" force=0
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --force) force=1; shift ;;
      -*) die_usage "release: unknown flag $1" ;;
      *) [ -z "$issue" ] || die_usage "release: unexpected argument $1"; issue="$1"; shift ;;
    esac
  done
  require_numeric_issue "$issue" release

  local sha
  sha="$(remote_claim_sha "$issue")"
  if [ -z "$sha" ]; then
    emit "RELEASED issue=$issue ref=refs/claims/issue-$issue (already absent)"
    return 0
  fi

  if [ "$force" -eq 0 ]; then
    local prs
    prs="$(open_pr_count "$issue")"
    if [ "$prs" = "-1" ]; then
      emit "RELEASE-REFUSED issue=$issue reason=open-pr-check-unavailable (gh missing/failed; re-run with --force to override)"
      return 2
    fi
    if [ "$prs" -gt 0 ]; then
      emit "RELEASE-REFUSED issue=$issue reason=open-pr open-prs=$prs (orphan-endgame hazard; use --force to override)"
      return 2
    fi
  fi

  if git push "$REMOTE" --delete "refs/claims/issue-${issue}" >/dev/null 2>&1; then
    emit "RELEASED issue=$issue ref=refs/claims/issue-$issue sha=$sha"
    return 0
  fi
  emit "RELEASE-REFUSED issue=$issue reason=delete-failed sha=$sha"
  return 1
}

# ---------------------------------------------------------------------------
cmd_status() {
  local only="${1:-}"
  [ -z "$only" ] || require_numeric_issue "$only" status

  local pattern raw now_epoch
  if [ -n "$only" ]; then pattern="refs/claims/issue-$only"; else pattern="refs/claims/*"; fi
  raw="$(git ls-remote "$REMOTE" "$pattern" 2>/dev/null || true)"
  now_epoch="$(date -u +%s)"

  if [ -z "$raw" ]; then
    emit "STATUS none pattern=$pattern remote=$REMOTE"
    return 0
  fi

  while IFS= read -r line; do
    [ -n "$line" ] || continue
    local sha ref issue msg machine actor ts age
    sha="$(printf '%s' "$line" | awk '{print $1}')"
    ref="$(printf '%s' "$line" | awk '{print $2}')"
    issue="${ref#refs/claims/issue-}"
    git fetch "$REMOTE" "$ref" >/dev/null 2>&1 || true
    msg="$(git log -1 --format=%B "$sha" 2>/dev/null || true)"
    machine="$(msg_field "$msg" machine)"; [ -n "$machine" ] || machine="?"
    actor="$(msg_field "$msg" actor)";     [ -n "$actor" ]   || actor="?"
    ts="$(msg_field "$msg" ts)";           [ -n "$ts" ]      || ts="?"
    if [ "$ts" != "?" ] && epoch="$(ts_to_epoch "$ts" 2>/dev/null)"; then
      age="$(humanize_age "$((now_epoch - epoch))")"
    else
      age="unknown"
    fi
    emit "STATUS issue=$issue ref=$ref sha=$sha machine=$machine actor=$actor ts=$ts age=$age"
  done <<EOF
$raw
EOF
  return 0
}

# ---------------------------------------------------------------------------
# cmd_smoke — ONE-TIME preflight for a new remote/host: prove that origin accepts
# a push to the `refs/claims/*` namespace (create + ls-remote + delete a throwaway
# `refs/claims/smoke-<nonce>` ref). Some managed Git hosts restrict custom ref
# namespaces; if this fails, the whole claim mechanism is unusable on that remote
# and MUST be caught before the fleet relies on it. NOT part of the hermetic test
# suite — it mutates the REAL origin. (Verified on github.com/pmcfadin/cqlite
# 2026-07-17: refs/claims/* is pushable.)
cmd_smoke() {
  local nonce ref sha seen
  nonce="$$-${RANDOM}-$(date -u +%s)"
  ref="refs/claims/smoke-${nonce}"
  note "smoke preflight: does $REMOTE accept a push to refs/claims/* ? (ref=$ref)"
  sha="$(build_claim_commit "smoke" "smoke")" || { emit "SMOKE-FAIL remote=$REMOTE reason=commit-build"; return 1; }
  if ! git push "$REMOTE" "${sha}:${ref}" >/dev/null 2>&1; then
    emit "SMOKE-FAIL remote=$REMOTE ref=$ref reason=push-rejected (does $REMOTE permit the refs/claims/* namespace?)"
    return 1
  fi
  seen="$(git ls-remote "$REMOTE" "$ref" 2>/dev/null | awk '{print $1}' | head -1)"
  # Always clean up the throwaway ref, whatever the ls-remote said.
  git push "$REMOTE" --delete "$ref" >/dev/null 2>&1 || note "WARNING: could not delete $ref on $REMOTE — remove it manually"
  if [ "$seen" = "$sha" ]; then
    emit "SMOKE-OK remote=$REMOTE namespace=refs/claims/* (create + ls-remote + delete verified)"
    return 0
  fi
  emit "SMOKE-FAIL remote=$REMOTE ref=$ref reason=ls-remote-mismatch seen=${seen:-<none>} expected=$sha"
  return 1
}

# ---------------------------------------------------------------------------
SUBCOMMAND="${1:-}"
case "$SUBCOMMAND" in
  claim)   shift; cmd_claim   "$@" ;;
  verify)  shift; cmd_verify  "$@" ;;
  adopt)   shift; cmd_adopt   "$@" ;;
  release) shift; cmd_release "$@" ;;
  status)  shift; cmd_status  "${1:-}" ;;
  smoke)   shift; cmd_smoke ;;
  -h | --help) print_help ;;
  "") die_usage "a subcommand is required: claim <N> | verify <N> | adopt <N> --expect <sha> | release <N> [--force] | status [<N>] | smoke" ;;
  *)  die_usage "unknown subcommand: $SUBCOMMAND (expected claim|verify|adopt|release|status|smoke)" ;;
esac
