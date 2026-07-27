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
# RESUME OF A FREE REF (`adopt <N> --expect none --reason <why>`, issue #2945) is
# the same mechanism with git's EMPTY LEASE — `--force-with-lease=<ref>:` means
# "the ref MUST NOT exist", i.e. the update carries the all-zero old value, so the
# remote creates it only when nobody holds it and rejects every racing claimant.
# It is the sanctioned, non-`--force` way past the LEGACY GUARD below when an
# `issue-<N>-*` branch outlived its claim ref (released/reaped/parked claim, or a
# merged-but-undeleted branch). `--reason` is REQUIRED there: the claim commit
# records who took it AND why, so a resume is auditable rather than a hand-crafted
# push. It is NOT a bypass — git still arbitrates, so a machine actively holding
# the claim ref keeps it (ADOPT-LOST, exit 2).
#
# LEGACY GUARD (mixed-fleet safety): older workers still branch-lock with a
# `refs/heads/issue-<N>-*` branch. `claim` refuses if any such branch exists on
# origin (treat the issue as already-claimed) and names the resume command above
# in its refusal — but ONLY when the lane is DEMONSTRABLY ORPHANED per
# `hatch_liveness`: zero open PRs AND every matching branch tip older than
# claim-heartbeat.sh's reap threshold AND no fresh machine-claim/heartbeat ref naming
# the issue. An older-fleet worker holds only the BRANCH, so `claim-ref=free` is true
# while it works — and because a PR is opened LATE in this pipeline, "no open PR" is
# also true for most of its life, which is why the branch-tip age and the liveness
# refs (the PRE-PR window) are part of the test. Otherwise — a live signal OR any
# signal that could not be READ — the refusal prints `remediation=withheld <signals>`
# instead: fail closed, because the readers run printed remediations literally (#2945).
# The advertised command carries a CONCRETE `--reason resume-legacy-branch-lock:<branch>`
# rather than a `<why>` placeholder, since it will be run verbatim.
# There is NO tip-based re-entrancy exemption: a work branch is
# cut from origin/main and carries ordinary commits, so its tip NEVER carries the
# machine=/actor= claim trailers — the old "all-ours -> no block" escape hatch was
# unreachable dead code (#2945). New claims never create these branches — the
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
#   adopt  <N> --expect <old-sha>|none [--reason <why>] [--actor <id>]
#                                             compare-and-swap the ref (adoption/resume).
#                                             --expect <old-sha>: the ref must still be <old-sha>.
#                                             A hex value MUST be a full object name (40/64 hex);
#                                             a truncated sha is a usage error, never a lost race.
#                                             --expect none:      the ref must NOT exist (empty
#                                             lease) — the resume path for an issue whose
#                                             `issue-<N>-*` branch outlived its claim; --reason
#                                             is REQUIRED and is recorded in the claim commit.
#                                             An EMPTY --expect '' is a usage error on purpose,
#                                             as is a --reason with nothing recordable in it
#                                             ('   ', '---', '…') or one that records as a bare
#                                             PLACEHOLDER ('why', 'todo', 'tbd', 'xxx', …, the
#                                             shape a verbatim-run `--reason <why>` produces):
#                                             the record must say WHY.
#                                             RE-ENTRANT: if the ref is already held by THIS
#                                             machine+actor, adopt reports ADOPTED (re-entrant)
#                                             exit 0 — a retry after a confirm-read blip must
#                                             never abandon an issue we still hold. In CAS mode
#                                             that verdict names BOTH shas
#                                             (re-entrant, lease-mismatch expected=/actual=), so a
#                                             VIOLATED compare-and-swap is never reported as a
#                                             satisfied one.
#   release <N> [--force] [--actor <id>]      delete the ref; without --force requires holder identity
#                                             (machine+actor) + no open PR, and deletes via CAS lease.
#                                             --force = reaper/adopt: unconditional delete.
#   status [<N>]                              render claim ref(s) with holder + age
#   smoke                                     one-time preflight: prove refs/claims/* is pushable on origin
#
# IDENTITY
#   machine  CLAIM_MACHINE (default `hostname -s`) — the holder identity that
#            `verify`/`release` match against, and the arbiter of re-entrancy.
#            A fleet whose boxes DO NOT have unique short hostnames (cloud images,
#            containers, cloned VMs all reporting the same `hostname -s`) MUST set
#            CLAIM_MACHINE to a UNIQUE value per box — otherwise two machines share
#            one identity and each treats the other's claim as its own (false
#            re-entrancy / cross-release). Tests also use it to simulate multiple
#            machines from one clone.
#   actor    --actor <id>, else CLAIM_ACTOR, else "flow" — a sub-machine role.
#            SANITIZED to one token before it is written OR compared (see
#            sanitize_field): the actor is part of the holder identity and lands in
#            the same commit message the identity parser reads, so an unsanitized
#            actor (e.g. `--actor 'flow machine=other'`) could forge a holder and win
#            false re-entrancy on someone else's claim (#2945).
#   The holder identity that `verify`/`release` match is machine+actor.
#
# ENV
#   CLAIM_REMOTE   origin remote name (default: origin)
#   CLAIM_MACHINE  override machine identity (default: hostname -s) — set it UNIQUELY
#                  per box on fleets with non-unique short hostnames (see IDENTITY)
#   CLAIM_ACTOR    default actor when --actor is omitted (default: flow)
#
# CONSTRAINTS
#   macOS bash 3.2 compatible (no associative arrays, no readarray/mapfile).
#   `set -euo pipefail`, shellcheck-clean. All informative output is a single line
#   prefixed `CLAIM:` (notes/degradations go to stderr).
#   gh is consulted by exactly TWO paths, both via `open_pr_count`, both degrading
#   LOUDLY (stderr note + a `-1` count) when gh is absent/errors: `release` without
#   --force (the open-PR guard) and `claim`'s LEGACY-BRANCH refusal (one
#   `gh pr list --limit 1000` per refusal, as hatch_liveness's endgame signal, where
#   `-1` WITHHOLDS the advertised remediation). Nothing else touches gh, and claim/
#   adopt/release ARBITRATION never depends on it — gh only bounds how loud a refusal
#   may be. That refusal also shells out to the sibling `claim-heartbeat.sh
#   reap-threshold` (single source of the 4h staleness threshold); an unreadable
#   answer withholds rather than defaulting.
#
# EXIT CODES
#   0  success (CLAIM HELD, VERIFY-OK, ADOPTED, RELEASED, SMOKE-OK, status render)
#   2  lost / refused (CLAIM LOST, VERIFY-FAIL, ADOPT-LOST, RELEASE-REFUSED)
#   1  infra / git / gh failure — retryable, NOT a race-loss (CLAIM ERROR reason=infra,
#      SMOKE-FAIL). EVERY remote-reading subcommand (claim/verify/adopt/release/status)
#      maps an ls-remote/push/delete failure to ERROR (exit 1), so a network blip never
#      makes a worker conclude it LOST/does-not-hold/RELEASED. `claim` also never reports
#      LOST when nobody holds the ref (a failed push whose re-read finds it absent).
#      ALSO exit 1: `CLAIM ERROR reason=auth` — a push git could not AUTHENTICATE
#      (issue #2942). Same exit code (callers keep treating 1 as "not a race"), but the
#      verdict is explicitly NOT retryable: retrying cannot fix a missing credential.
#   64 usage error
#
# ---END-HELP---
set -euo pipefail

prog="$(basename "$0")"

die_usage() { echo "$prog: $*" >&2; exit 64; }
note()      { echo "[claim] $*" >&2; }
emit()      { echo "CLAIM: $*"; }

# emit_infra <line> — a transient infrastructure failure (ls-remote/push/delete
# unreachable): a retryable ERROR, NEVER a lost/absent verdict. Callers pair it
# with `return 1` per the header exit-code contract, so a network blip never makes
# a worker conclude it lost ownership.
emit_infra() { emit "ERROR reason=infra $* (transient — retry)"; }

# emit_auth <line> — a push git could not AUTHENTICATE (issue #2942). This is a
# MACHINE-CONFIGURATION fault, not a blip: it cannot self-clear, so it must never
# wear the `transient — retry` wording. Observed: a box with an authenticated `gh`
# but no git credential helper failed every claim push and was reported as
# `reason=infra ... (transient — retry)`, sending the worker into a retry loop on an
# operation that can never succeed. Callers pair this with `return 1` (same exit code
# as infra — still "not a race-loss" — but the text names the fix, not a retry).
# The remediation is deliberately fixed text: git's raw stderr is NEVER echoed,
# because a remote URL can carry an embedded token.
emit_auth() {
  emit "ERROR reason=auth $* (NOT retryable — git cannot authenticate to $REMOTE; fix credentials: 'gh auth setup-git' or 'bash scripts/bootstrap-agent-machine.sh --yes', then re-run)"
}

# git_stderr_is_auth <captured-stderr> — 0 iff the text carries the signature of a
# CREDENTIAL/AUTHORIZATION failure rather than a network/outage one. Deliberately
# conservative: anything unrecognized stays an infra (retryable) verdict, so the
# #2665 contract can only ever be narrowed by a signature we positively identify.
# Every pattern below is unambiguously credential-shaped. Two candidates were
# deliberately REJECTED because they are emitted by non-credential faults too, and
# misclassifying a transient as permanent is the one direction #2665 says never to
# move: `403 Forbidden` (a proxy or edge outage returns it; GitHub's rate-limit text
# is `HTTP 403`, so nothing real is lost) and `remote: Repository not found` (seen
# during brief degradations and in the window right after a token rotation).
git_stderr_is_auth() {
  case "$1" in
    *"could not read Username"*      | *"could not read Password"*        | \
    *"Authentication failed"*        | *"authentication failed"*          | \
    *"terminal prompts disabled"*    | *"Invalid username or token"*      | \
    *"Invalid username or password"* | *"Permission denied (publickey)"*  | \
    *"Permission to "*" denied"*     | *"Write access to repository not granted"* | \
    *"Support for password authentication was removed"* | *"401 Unauthorized"*)
      return 0 ;;
  esac
  return 1
}

REMOTE="${CLAIM_REMOTE:-origin}"

# Never let a remote operation block on an interactive credential prompt: an
# unattended worker would hang forever instead of failing with a diagnosable
# verdict (issue #2942). With prompts disabled git fails fast and its stderr
# carries the `could not read Username` signature emit_auth keys on.
export GIT_TERMINAL_PROMPT=0

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
#
# EXACT-KEY, FIRST-MATCH, ANCHORED PER TOKEN (identity-forgery hardening, #2945).
# The old implementation was `sed "s/.*<key>=\([^ ]*\).*/\1/"`: the leading `.*` is
# GREEDY, so the LAST occurrence won and any key was matched as a SUBSTRING. Since
# free-text `--reason`/`--actor` values are appended to the very message this parser
# reads, a value carrying `machine=<other>` could FORGE holder identity — and holder
# identity is what gates re-entrancy, verify, and release. Fixing that at the parser
# is the root fix: split the message on whitespace, compare each token's key for
# EQUALITY (so neither `holder-machine=` nor an appended `machine=` in a later field
# can answer for `machine`), and return the FIRST match — the trailers this script
# writes itself, which always precede any user-supplied field. Sanitizing the
# user-controlled fields (see sanitize_field) is the second, independent layer.
msg_field() {
  local msg="$1" key="$2" tok glob_was_off=0
  # Word-splitting needs the tokens UNGLOBBED: a forged message may contain '*'.
  case "$-" in *f*) glob_was_off=1 ;; esac
  set -f
  # shellcheck disable=SC2086  # deliberate word-split of the claim message
  for tok in $msg; do
    case "$tok" in
      "$key"=*)
        printf '%s\n' "${tok#*=}"
        [ "$glob_was_off" = 1 ] || set +f
        return 0
        ;;
    esac
  done
  [ "$glob_was_off" = 1 ] || set +f
  return 0
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

# sanitize_field <text> — collapse a free-text value into ONE parseable token.
# Applied to EVERY user-controlled field that lands in the claim message: --reason
# AND --actor (the latter is part of the holder identity, so an unsanitized actor
# was itself a forgery vector, #2945). The message is parsed as
# `<key>=<non-space-run>`, so a value containing spaces would be truncated at the
# first space. Keeps [A-Za-z0-9._:/#-] — note '=' is NOT kept, so a value can never
# introduce a new `key=` pair — maps every other run (spaces, newlines, quotes,
# shell metacharacters) to a single '-', trims leading/trailing '-', caps at 120
# chars, and never prints an empty token.
#
# TRIM ORDER IS PART OF THE CONTRACT: the 120-char cut happens BEFORE the final
# trim, because trimming first and truncating after can re-introduce the very
# trailing separator the trim promised to remove (a reason whose 120th byte is a
# '-' recorded as `…foo-`, #2945 review). Collapse -> trim -> cut -> re-trim.
#
# LC_ALL=C on BOTH tr and sed is load-bearing: BSD/macOS `tr` aborts with
# "Illegal byte sequence" on non-ASCII input under a UTF-8 locale, and a `--reason`
# with an em dash is a likely invocation in this repo. Under `set -euo pipefail`
# that failure inside a command substitution killed the whole script — no `CLAIM:`
# line at all and a bogus exit 1 the contract reads as "retryable".
sanitize_field() {
  local s
  s="$(printf '%s' "${1:-}" | LC_ALL=C tr -c 'A-Za-z0-9._:/#-' '-' | LC_ALL=C sed -e 's/--*/-/g' -e 's/^-//' -e 's/-$//')"
  s="$(LC_ALL=C printf '%.120s' "$s")"
  s="${s%-}"   # re-trim: the cut may have landed exactly on a separator
  [ -n "$s" ] || s="unspecified"
  printf '%s\n' "$s"
}

# resolve_actor <raw> — the actor identity, sanitized to one token. Sanitizing at
# the ARG BOUNDARY (every subcommand, before any comparison) keeps the written
# record and the identity match on exactly the same value.
resolve_actor() { sanitize_field "${1:-}"; }

# build_claim_commit <N> <actor> [extra-fields] — create a UNIQUE root commit
# (empty tree, no parent) and print its SHA. The nonce guarantees distinct SHAs
# even for two claimants at the same base in the same second. <extra-fields> is
# an already-sanitized, space-separated `key=value` run appended to the message
# (e.g. `mode=empty-lease reason=<why>` for an adopt-resume record).
build_claim_commit() {
  local issue="$1" actor="$2" extra="${3:-}"
  local machine pid ts nonce message empty_tree
  machine="$(this_machine)"
  pid="$$"
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  nonce="${pid}-${RANDOM}-${RANDOM}-$(date -u +%s)"
  message="claim issue=${issue} machine=${machine} pid=${pid} actor=${actor} ts=${ts} nonce=${nonce}"
  [ -z "$extra" ] || message="${message} ${extra}"
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

# legacy_branch_scan <N> — enumerate the older fleet's branch lock
# `refs/heads/issue-<N>-*` on origin (which is ALSO what a merged-but-undeleted PR
# branch leaves behind). Sets the global LEGACY_BRANCHES to a comma-separated list
# of matching ref NAMES ("" when there are none) and returns:
#   0  enumeration SUCCEEDED — LEGACY_BRANCHES is authoritative ("" == genuinely none)
#   1  ls-remote FAILED — the answer is UNKNOWN, never "none" (issue #2677 item 2:
#      the old code mapped an outage to "no branches", i.e. an all-clear). Callers
#      map this to ERROR reason=infra (exit 1), matching every other remote read.
# There is deliberately NO "is this branch ours" test: a work branch is cut from
# origin/main and carries ordinary commits, so its tip NEVER carries the claim
# machine=/actor= trailers, which made the old `holder_is_us`-based "all-ours -> no
# block" re-entrancy unreachable (#2945). Re-entrancy for a branch you own is now
# expressed explicitly by `adopt <N> --expect none --reason <why>`, arbitrated by
# the claim ref itself rather than guessed from a commit message.
LEGACY_BRANCHES=""
legacy_branch_scan() {
  local issue="$1" raw line ref
  LEGACY_BRANCHES=""
  if ! raw="$(git ls-remote --heads "$REMOTE" "issue-${issue}-*" 2>/dev/null)"; then
    return 1
  fi
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    ref="$(printf '%s' "$line" | awk '{print $2}')"
    [ -n "$ref" ] || continue
    if [ -z "$LEGACY_BRANCHES" ]; then
      LEGACY_BRANCHES="$ref"
    else
      LEGACY_BRANCHES="${LEGACY_BRANCHES},${ref}"
    fi
  done <<< "$raw"
  return 0
}

# reap_threshold_secs — "how old is stale enough to hand away", in seconds, read
# from claim-heartbeat.sh (`reap-threshold`) so the fleet keeps exactly ONE
# definition of the 4h staleness threshold instead of a second copy here that could
# drift from the documented one (#2945 review). Prints the seconds (exit 0), or
# returns 1 when the sibling script is missing/answers non-numerically — an
# UNREADABLE signal, which callers must treat as "withhold", never as a default.
reap_threshold_secs() {
  local hb out
  hb="$(dirname -- "${BASH_SOURCE[0]}")/claim-heartbeat.sh"
  [ -f "$hb" ] || return 1
  out="$(bash "$hb" reap-threshold 2>/dev/null)" || return 1
  case "$out" in
    '' | *[!0-9]*) return 1 ;;
  esac
  printf '%s\n' "$out"
}

# hatch_liveness <N> <threshold-secs> — may the copy-pasteable empty-lease resume be
# ADVERTISED for this issue, i.e. is its lane demonstrably ORPHANED?
#
# The open-PR signal ALONE does not answer this (#2945 review). In this pipeline the
# PR is opened LATE (implement + review happen first), so an older-fleet worker that
# is actively implementing has ZERO open PRs for most of its life while holding only
# the BRANCH — and `claim-ref=free` is true for it. Advertising the hatch there is the
# two-writer outcome the guard exists to prevent, and the readers run printed
# remediations literally. So the PRE-PR window needs its own liveness evidence:
#
#   1. open PRs == 0                       (the ENDGAME signal; -1 = unreadable)
#   2. EVERY matching issue-<N>-* branch tip is OLDER than <threshold> (the same
#      staleness threshold `claim-heartbeat.sh should-reap` uses) — a worker mid-
#      implementation pushes commits, so a FRESH tip means somebody is on it
#   3. NO refs/machine-claims/* or refs/heartbeats/* ref FRESHLY names this issue
#      (the supervisor-authored liveness proof of #2655; a ref older than the
#      threshold is itself reapable and does not withhold)
#
# ANY signal that cannot be READ withholds — an unreachable remote, an unparseable
# commit date and a missing threshold are all "we could not prove nobody is working
# this", never an all-clear. Sets HATCH_SIGNALS (a single-line key=value run for the
# refusal) and returns 0 = orphaned (advertise), 1 = withhold.
HATCH_SIGNALS=""
hatch_liveness() {
  local issue="$1" threshold="$2"
  local prs now raw line refname msg tip_ct age min_age="" ref_issue ref_ts ref_epoch

  prs="$(open_pr_count "$issue")"
  HATCH_SIGNALS="open-prs=$prs"
  [ "$prs" = "0" ] || return 1

  now="$(date -u +%s)"
  while IFS= read -r refname; do
    [ -n "$refname" ] || continue
    if ! git fetch "$REMOTE" "$refname" >/dev/null 2>&1 \
       || ! tip_ct="$(git log -1 --format=%ct FETCH_HEAD 2>/dev/null)" \
       || [ -z "$tip_ct" ] || [ -n "${tip_ct//[0-9]/}" ]; then
      HATCH_SIGNALS="$HATCH_SIGNALS branch-tip=unreadable:${refname}"
      return 1
    fi
    age=$((now - tip_ct))
    [ "$age" -ge 0 ] || age=0
    if [ -z "$min_age" ] || [ "$age" -lt "$min_age" ]; then min_age="$age"; fi
  done <<< "$(printf '%s' "$LEGACY_BRANCHES" | tr ',' '\n')"
  if [ -z "$min_age" ]; then
    HATCH_SIGNALS="$HATCH_SIGNALS branch-tip=unreadable"
    return 1
  fi
  HATCH_SIGNALS="$HATCH_SIGNALS newest-branch-tip=$(humanize_age "$min_age") stale-after=$(humanize_age "$threshold")"
  [ "$min_age" -gt "$threshold" ] || return 1

  if ! raw="$(git ls-remote "$REMOTE" 'refs/machine-claims/*' 'refs/heartbeats/*' 2>/dev/null)"; then
    HATCH_SIGNALS="$HATCH_SIGNALS liveness-refs=unreadable"
    return 1
  fi
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    refname="$(printf '%s' "$line" | awk '{print $2}')"
    [ -n "$refname" ] || continue
    if ! git fetch "$REMOTE" "$refname" >/dev/null 2>&1 \
       || ! msg="$(git log -1 --format=%B FETCH_HEAD 2>/dev/null)" || [ -z "$msg" ]; then
      HATCH_SIGNALS="$HATCH_SIGNALS liveness-refs=unreadable:${refname}"
      return 1
    fi
    ref_issue="$(msg_field "$msg" issue)"
    if [ -z "$ref_issue" ]; then
      HATCH_SIGNALS="$HATCH_SIGNALS liveness-refs=unreadable:${refname}"
      return 1
    fi
    [ "$ref_issue" = "$issue" ] || continue
    # This ref names OUR issue. Fresh -> a worker is on it. Unparseable ts -> we
    # cannot age it out, so it withholds (fail closed) exactly like should-reap
    # refuses to reap on an unknown age.
    ref_ts="$(msg_field "$msg" ts)"
    if [ -z "$ref_ts" ] || ! ref_epoch="$(ts_to_epoch "$ref_ts")"; then
      HATCH_SIGNALS="$HATCH_SIGNALS liveness-ref=${refname}:unparseable-ts"
      return 1
    fi
    age=$((now - ref_epoch))
    [ "$age" -ge 0 ] || age=0
    if [ "$age" -le "$threshold" ]; then
      HATCH_SIGNALS="$HATCH_SIGNALS liveness-ref=${refname}:age=$(humanize_age "$age")"
      return 1
    fi
  done <<< "$raw"

  HATCH_SIGNALS="$HATCH_SIGNALS liveness-refs=none-naming-$issue"
  return 0
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
  actor="$(resolve_actor "$actor")"

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

  # Legacy branch-lock guard (mixed fleet). An `issue-<N>-*` branch on origin is
  # treated as an older worker's branch lock. The refusal NAMES the sanctioned
  # resume command (#2945): a bare LOST is what previously sent workers into
  # hand-crafted claim-commit pushes. An enumeration OUTAGE is UNKNOWN, not an
  # all-clear (#2677 item 2) — it maps to ERROR infra (retryable), never a claim
  # granted on an unread guard.
  if ! legacy_branch_scan "$issue"; then
    emit_infra "issue=$issue detail=legacy-branch-ls-remote-unreachable-on-$REMOTE (cannot tell 'no legacy branch' from an outage)"
    return 1
  fi
  if [ -n "$LEGACY_BRANCHES" ]; then
    # The refusal must be ACTIONABLE without being DANGEROUS (#2945 review). "git
    # rejects it if any machine holds the ref" does NOT cover the case this guard
    # exists for: an OLDER-fleet worker locks with the BRANCH and holds no claim ref,
    # so `claim-ref=free` is true for it and the advertised empty-lease adopt WOULD
    # succeed — a second machine on an actively-worked issue. Prose hedging ("if it
    # is YOURS") is not a control: the readers are agents that run printed
    # remediations literally. So the copy-pasteable command is printed ONLY when
    # hatch_liveness proves the lane ORPHANED across ALL its signals — no open PR
    # (endgame), every branch tip staler than the reap threshold (the PRE-PR window,
    # where an actively-implementing legacy worker has no PR at all), and no fresh
    # machine-claim/heartbeat ref naming this issue. Any unreadable signal withholds:
    # fail closed, never advertise a hand-away on evidence we could not read.
    # The printed --reason is a CONCRETE, self-describing default, not a `<why>`
    # placeholder: run verbatim, `<why>` recorded as `reason=why` — as uninformative
    # as the no-reason case the audit gate rejects (cmd_adopt now refuses that token).
    local remedy threshold hatch_branch
    if ! threshold="$(reap_threshold_secs)"; then
      remedy="remediation=withheld liveness=threshold-unreadable (could not read the staleness threshold from claim-heartbeat.sh reap-threshold, so the lane cannot be shown orphaned; fix the checkout, then see 'bash scripts/flow/claim.sh -h')"
    elif hatch_liveness "$issue" "$threshold"; then
      hatch_branch="${LEGACY_BRANCHES%%,*}"
      hatch_branch="${hatch_branch#refs/heads/}"
      remedy="remediation='bash scripts/flow/claim.sh adopt $issue --expect none --reason resume-legacy-branch-lock:${hatch_branch}' $HATCH_SIGNALS (orphaned lane: no open PR, every branch tip staler than the reap threshold, no fresh liveness ref for this issue — an older-fleet branch lock left behind, a parked/reaped resume, or a merged-but-undeleted branch; the quoted empty-lease adopt takes the FREE claim ref atomically — git rejects it if any machine holds the claim ref)"
    else
      remedy="remediation=withheld $HATCH_SIGNALS (this lane may be LIVE or is unproven: an open PR (or an unreadable -1 PR list), a branch tip fresher than the reap threshold — an older-fleet worker mid-implementation holds only the BRANCH and has NO PR yet — or a fresh machine-claim/heartbeat ref naming this issue. An empty-lease adopt WOULD succeed there and create a SECOND writer; confirm ownership via the board and the branch/PR author first, then see 'bash scripts/flow/claim.sh -h')"
    fi
    emit "LOST issue=$issue reason=legacy-branch-lock detail=$LEGACY_BRANCHES exists on $REMOTE claim-ref=free $remedy"
    return 2
  fi

  # Build our unique claim commit and attempt the atomic create.
  local sha push_err
  sha="$(build_claim_commit "$issue" "$actor")"
  # Capture the push's stderr (stdout discarded) so a CREDENTIAL failure can be told
  # apart from a race-loss and from a genuine transient (issue #2942). The captured
  # text is only ever CLASSIFIED, never emitted — a remote URL can embed a token.
  if push_err="$(git push "$REMOTE" "${sha}:refs/claims/issue-${issue}" 2>&1 >/dev/null)"; then
    : # push accepted — confirm below.
  else
    # Auth is checked FIRST and independently of the re-read: on a public repo an
    # unauthenticated box still ls-remotes fine (so the ref reads as absent and the
    # old code called it a transient), while on a private repo BOTH fail. Either way
    # the cause is the same permanent, non-retryable credential fault.
    if git_stderr_is_auth "$push_err"; then
      emit_auth "issue=$issue detail=claim-push-unauthenticated ref=refs/claims/issue-$issue"
      return 1
    fi
    # Push failed. Distinguish a genuine race-loss (another holder present) from
    # an infra failure (remote unreachable, or a push error with NO holder) — a
    # LOST verdict must NEVER be emitted when nobody actually holds the ref.
    if ! remote_claim_lookup "$issue"; then
      emit_infra "issue=$issue detail=push-failed-and-ls-remote-unreachable-on-$REMOTE"
      return 1
    fi
    local now="$REPLY_SHA"
    if [ -z "$now" ]; then
      # Push was rejected yet the ref is absent: not a lost race — a push/infra
      # error. Fail as ERROR (exit 1, retryable), never a bogus LOST.
      emit_infra "issue=$issue detail=push-rejected-but-ref-absent-on-$REMOTE"
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
  # Confirm via the infra-AWARE lookup — an ls-remote failure right after a
  # SUCCESSFUL push is infra (retryable, exit 1), NEVER a false LOST on a claim we
  # actually won. Only a successfully-read non-matching sha is a genuine LOST.
  if ! remote_claim_lookup "$issue"; then
    emit_infra "issue=$issue detail=push-succeeded-but-confirm-ls-remote-unreachable-on-$REMOTE"
    return 1
  fi
  local confirmed="$REPLY_SHA"
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
  actor="$(resolve_actor "$actor")"

  # ls-remote failure is INFRA (retryable), not "you don't hold it" — never let a
  # network blip make a worker conclude it lost ownership.
  if ! remote_claim_lookup "$issue"; then
    emit_infra "issue=$issue detail=ls-remote-unreachable-on-$REMOTE"
    return 1
  fi
  local sha="$REPLY_SHA"
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
  local issue="" actor="${CLAIM_ACTOR:-flow}" expect="" reason="" mode="cas"
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --actor)  [ "$#" -ge 2 ] || die_usage "--actor requires a value";  actor="$2";  shift 2 ;;
      --expect) [ "$#" -ge 2 ] || die_usage "--expect requires a value"; expect="$2"; shift 2 ;;
      --reason) [ "$#" -ge 2 ] || die_usage "--reason requires a value"; reason="$2"; shift 2 ;;
      -*) die_usage "adopt: unknown flag $1" ;;
      *) [ -z "$issue" ] || die_usage "adopt: unexpected argument $1"; issue="$1"; shift ;;
    esac
  done
  require_numeric_issue "$issue" adopt
  actor="$(resolve_actor "$actor")"
  # An EMPTY --expect stays a USAGE ERROR (fail closed): an unset shell variable
  # expanding to "" must NEVER silently turn a compare-and-swap into a create.
  # The empty lease is opt-in via the explicit literal `none`.
  [ -n "$expect" ] || die_usage "adopt requires --expect <old-sha> (CAS against a HELD ref) or --expect none (empty lease: the claim ref must NOT exist — the sanctioned resume when an issue-<N>-* branch outlived its claim, #2945). An empty --expect '' is rejected on purpose."
  # A hex --expect must be a FULL object name (40 hex sha1 / 64 hex sha256). Length
  # was unchecked (#2945 review), and both failure shapes MISREPORT a caller bug:
  # a TRUNCATED sha (`--expect abc123`, e.g. a bad `cut` in a caller) builds a lease
  # git cannot resolve, so the push fails and the confirm read reports ADOPT-LOST —
  # a race-loss verdict for a usage error — while a SHORT all-zero value (`--expect 0`)
  # slipped into the all-zero branch below and silently became a CREATE instead of a
  # compare-and-swap. This file is otherwise rigorous about separating usage / infra /
  # lost-race, so both are now usage errors (exit 64).
  case "$expect" in
    none) mode="empty" ;;
    *[!0-9a-fA-F]*) die_usage "adopt: --expect takes a hex sha or the literal 'none' (got '$expect')" ;;
    *)
      case "${#expect}" in
        40 | 64) : ;;
        *) die_usage "adopt: --expect needs a FULL object name — 40 hex (sha1) or 64 hex (sha256) — or the literal 'none' (got '$expect', ${#expect} chars). A truncated sha cannot be resolved as a lease and would read as a lost race." ;;
      esac
      ;;
  esac
  # An ALL-ZERO expected value (at full object-name length) is git's own "the ref must
  # not exist", so it carries exactly the `none` intent — route it through the same
  # AUDITED path (--reason required) instead of leaving a quiet create-with-no-record.
  # Verified against the real origin: `--expect 000…0` does create the ref, so it
  # cannot stay unaudited.
  case "$expect" in
    *[!0]*) : ;;
    *) mode="empty" ;;
  esac

  # VALIDATE THE RECORDED TOKEN, NOT THE RAW TEXT (#2945 review). The audit value
  # is what lands in the claim commit, so gating on `[ -n "$reason" ]` let '   ',
  # '---', '…' or an expansion like "$UNSET_VAR " through and recorded them as
  # `reason=unspecified` — indistinguishable from supplying no reason at all, which
  # defeats the "record WHY" requirement. Sanitize FIRST, then require a token that
  # actually says something: not the `unspecified` sentinel sanitize_field falls back
  # to (so a literal `--reason unspecified` is refused too — it records nothing), and
  # at least 3 recordable characters. Same fail-closed direction as `--expect ''`.
  local extra="" reason_token=""
  if [ -n "$reason" ]; then
    reason_token="$(sanitize_field "$reason")"
    if [ "$reason_token" = "unspecified" ] || [ "${#reason_token}" -lt 3 ]; then
      die_usage "adopt: --reason must carry at least 3 recordable characters ([A-Za-z0-9._:/#-]); '$reason' records as '$reason_token', which is indistinguishable from no reason at all"
    fi
    # PLACEHOLDER TOKENS ARE REFUSED (#2945 review). A doc/help line that shows
    # `--reason <why>` is run VERBATIM by the readers of these commands, and `<why>`
    # sanitizes to `why` — 3 recordable chars, so the length gate passes and the
    # record says `reason=why`: exactly as uninformative as the no-reason case this
    # gate exists to reject. So the sentinel set is refused BY NAME too (the printed
    # remediation now carries a concrete self-describing default instead, see
    # cmd_claim). Case-insensitive; the list is the placeholder vocabulary that shows
    # up in help text and templates, not an attempt at judging prose quality.
    case "$(printf '%s' "$reason_token" | LC_ALL=C tr 'A-Z' 'a-z')" in
      why | reason | todo | tbd | tba | xxx | xxxx | placeholder | fixme | none | foo | bar | baz | n/a)
        die_usage "adopt: --reason '$reason' records as the PLACEHOLDER '$reason_token' — as uninformative as no reason at all. Say what the resume IS, e.g. --reason resume-legacy-branch-lock:issue-$issue-<slug> or --reason 'reaped claim, board says Ready'"
        ;;
    esac
  fi
  if [ "$mode" = "empty" ]; then
    # The resume is a judgement call, so it MUST be self-documenting: the claim
    # commit records who took it (machine/actor/ts) AND why (reason).
    [ -n "$reason_token" ] || die_usage "adopt --expect none requires --reason <why> (the resume is recorded in the claim commit: who took it AND why)"
    extra="mode=empty-lease reason=$reason_token"
  elif [ -n "$reason_token" ]; then
    extra="mode=cas reason=$reason_token"
  fi

  local sha lease adopt_err=""
  sha="$(build_claim_commit "$issue" "$actor" "$extra")"
  if [ "$mode" = "empty" ]; then
    # EMPTY LEASE: a lease of "<ref>:" (no expected value) means "the ref must NOT
    # exist" — the ref update carries the all-zero old value, so the REMOTE creates
    # it only when nobody holds it and rejects every racing claimant. Same single
    # arbiter as `claim`, no --force, so two machines racing a resume still yield
    # exactly one winner.
    lease="refs/claims/issue-${issue}:"
  else
    # Compare-and-swap: replace the ref ONLY if origin is still at <old-sha>.
    lease="refs/claims/issue-${issue}:${expect}"
  fi
  # We ignore the push exit here and let the infra-AWARE confirm read below decide
  # — mirroring cmd_claim exactly, so a lease-mismatch, a TOCTOU, and an infra blip
  # are told apart by the READ, never by the push's opaque non-zero.
  adopt_err="$(git push --force-with-lease="$lease" \
        "$REMOTE" "${sha}:refs/claims/issue-${issue}" 2>&1 >/dev/null)" || true
  # ...with ONE exception (issue #2942): a CREDENTIAL failure is not something the
  # read can diagnose. On a public repo the confirm read succeeds and would report
  # ADOPT-LOST — blaming the lease for what is a broken machine. A lease mismatch
  # says "stale info", never an auth signature, so this cannot swallow a real CAS loss.
  # No `[ -n "$adopt_err" ]` pre-guard: it was redundant (an empty string matches no
  # signature) and it read as if empty stderr were a meaningful state to skip on.
  if git_stderr_is_auth "$adopt_err"; then
    emit_auth "issue=$issue detail=adopt-cas-push-unauthenticated ref=refs/claims/issue-$issue"
    return 1
  fi

  # Confirm via the infra-AWARE lookup: a lookup failure is infra (retryable,
  # exit 1), NEVER a false ADOPT-LOST on a claim we actually landed. If the read
  # SHA equals OUR new sha, we hold it → ADOPTED, in every path.
  if ! remote_claim_lookup "$issue"; then
    emit_infra "issue=$issue detail=adopt-cas-and-confirm-ls-remote-unreachable-on-$REMOTE"
    return 1
  fi
  local now="$REPLY_SHA"
  local record=""
  [ -z "$extra" ] || record=" $extra"
  if [ "$now" = "$sha" ]; then
    emit "ADOPTED issue=$issue ref=refs/claims/issue-$issue sha=$sha machine=$(this_machine) actor=$actor from=$expect$record"
    return 0
  fi
  if [ -z "$now" ] && [ "$mode" = "empty" ]; then
    # The empty-lease create did not land AND the ref is absent: nobody holds it,
    # so this is NOT a lost race — it is a push/infra error (retryable, exit 1),
    # exactly as `claim` treats a rejected push over an absent ref.
    emit_infra "issue=$issue detail=adopt-empty-lease-rejected-but-ref-absent-on-$REMOTE (nobody holds it — not a lost race)"
    return 1
  fi
  fetch_claim "$issue"
  # RE-ENTRANCY (#2945): the ref is not our NEW sha, but it may already be OURS —
  # the documented remedy for the `ERROR reason=infra` paths above is "retry", and a
  # retry builds a FRESH claim commit whose empty-lease push is then correctly
  # rejected (the ref now exists, because the first attempt's push DID land and only
  # its confirm read failed). Without this check that lands on ADOPT-LOST — exit 2,
  # which workers read as "you did not win, take the next item" — so a machine
  # abandons an issue it demonstrably owns WHILE STILL HOLDING the claim ref, and
  # nobody else can take it either. `cmd_claim` has had this identity check on both
  # of its failure paths from the start; `adopt` is now the ONLY way past the legacy
  # guard, so it needs the same idempotence.
  # In CAS mode the same situation ALSO covers a VIOLATED compare-and-swap: the ref
  # is at some Y != our --expect X, and Y happens to be ours. We still hold it (so
  # exit 2 would abandon it), but reporting a plain `ADOPTED … from=X` would print a
  # value the ref never had and make a FAILED CAS indistinguishable from a satisfied
  # one. So the CAS path gets its OWN verdict naming BOTH shas (#2945 review).
  if [ -n "$now" ] && holder_is_us "$now" "$actor"; then
    if [ "$mode" = "empty" ]; then
      emit "ADOPTED issue=$issue ref=refs/claims/issue-$issue sha=$now $(holder_desc "$now") from=$expect (re-entrant)"
    else
      emit "ADOPTED issue=$issue ref=refs/claims/issue-$issue sha=$now $(holder_desc "$now") (re-entrant, lease-mismatch expected=$expect actual=$now — we DO hold the ref, but the compare-and-swap precondition did NOT hold)"
    fi
    return 0
  fi
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
  # --limit 1000: gh's default page is 30; under-counting here would let a claim
  # be released out from under an open PR — the exact orphan-endgame hazard.
  if ! refs="$(gh pr list --state open --limit 1000 --json headRefName --jq '.[].headRefName' 2>/dev/null)"; then
    note "gh pr list failed — cannot check for an open PR on issue #$issue"
    printf '%s\n' -1
    return 0
  fi
  while IFS= read -r name; do
    [ -n "$name" ] || continue
    case "$name" in
      "issue-${issue}" | "issue-${issue}-"*) count=$((count + 1)) ;;
    esac
  done <<< "$refs"
  printf '%s\n' "$count"
}

cmd_release() {
  local issue="" force=0 actor="${CLAIM_ACTOR:-flow}"
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --force) force=1; shift ;;
      --actor) [ "$#" -ge 2 ] || die_usage "--actor requires a value"; actor="$2"; shift 2 ;;
      -*) die_usage "release: unknown flag $1" ;;
      *) [ -z "$issue" ] || die_usage "release: unexpected argument $1"; issue="$1"; shift ;;
    esac
  done
  require_numeric_issue "$issue" release
  actor="$(resolve_actor "$actor")"

  # ls-remote failure is INFRA (retryable), not "already absent" — never delete-
  # or claim-absent on a network blip.
  if ! remote_claim_lookup "$issue"; then
    emit_infra "issue=$issue detail=ls-remote-unreachable-on-$REMOTE"
    return 1
  fi
  local sha="$REPLY_SHA"
  if [ -z "$sha" ]; then
    emit "RELEASED issue=$issue ref=refs/claims/issue-$issue (already absent)"
    return 0
  fi

  if [ "$force" -eq 0 ]; then
    # A non-forced release is a HOLDER releasing its OWN finished claim. You may
    # not release a ref you do not hold — that is the reaper's job, and the reaper
    # uses --force (which skips BOTH this identity gate and the open-PR guard).
    fetch_claim "$issue"
    if ! holder_is_us "$sha" "$actor"; then
      emit "RELEASE-REFUSED issue=$issue reason=not-holder sha=$sha holder-$(holder_desc "$sha") wanted-machine=$(this_machine) wanted-actor=$actor (only the holder may release without --force)"
      return 2
    fi
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
    # Compare-and-swap delete: remove the ref ONLY if it is still at <sha> we just
    # read and own — a ref that changed under us (adopted/reaped) fails the lease.
    local rel_err
    if rel_err="$(git push "$REMOTE" --force-with-lease="refs/claims/issue-${issue}:${sha}" \
          ":refs/claims/issue-${issue}" 2>&1 >/dev/null)"; then
      emit "RELEASED issue=$issue ref=refs/claims/issue-$issue sha=$sha (cas)"
      return 0
    fi
    # CAS delete failed: an unauthenticated push is a permanent machine fault, not
    # something a retry fixes (#2942); anything else is the ref changing under us OR
    # an unreachable remote — a retryable ERROR (exit 1), never a silent success.
    if git_stderr_is_auth "$rel_err"; then
      emit_auth "issue=$issue detail=release-cas-delete-unauthenticated sha=$sha"
      return 1
    fi
    emit_infra "issue=$issue detail=cas-delete-failed-on-$REMOTE (ref changed or remote unreachable) sha=$sha"
    return 1
  fi

  # --force: reaper/adopt semantics — unconditional delete, no identity/PR gate.
  local force_err
  if force_err="$(git push "$REMOTE" --delete "refs/claims/issue-${issue}" 2>&1 >/dev/null)"; then
    emit "RELEASED issue=$issue ref=refs/claims/issue-$issue sha=$sha (force)"
    return 0
  fi
  if git_stderr_is_auth "$force_err"; then
    emit_auth "issue=$issue detail=release-force-delete-unauthenticated sha=$sha"
    return 1
  fi
  emit_infra "issue=$issue detail=delete-failed-on-$REMOTE sha=$sha"
  return 1
}

# ---------------------------------------------------------------------------
cmd_status() {
  local only="${1:-}"
  [ -z "$only" ] || require_numeric_issue "$only" status

  local pattern raw now_epoch
  if [ -n "$only" ]; then pattern="refs/claims/issue-$only"; else pattern="refs/claims/*"; fi
  # ls-remote failure is INFRA (retryable), not "no claims" — an unreachable
  # origin must NOT render as an empty board.
  if ! raw="$(git ls-remote "$REMOTE" "$pattern" 2>/dev/null)"; then
    emit_infra "detail=ls-remote-unreachable-on-$REMOTE pattern=$pattern"
    return 1
  fi
  now_epoch="$(date -u +%s)"

  if [ -z "$raw" ]; then
    emit "STATUS none pattern=$pattern remote=$REMOTE"
    return 0
  fi

  while IFS= read -r line; do
    [ -n "$line" ] || continue
    local sha ref issue msg machine actor ts age epoch reason record
    sha="$(printf '%s' "$line" | awk '{print $1}')"
    ref="$(printf '%s' "$line" | awk '{print $2}')"
    # Only issue claim refs are rendered — skip stray refs under refs/claims/*
    # that are NOT issue claims (e.g. a leftover `refs/claims/smoke-<nonce>` from
    # an interrupted preflight), so they never masquerade as an issue row.
    case "$ref" in
      refs/claims/issue-*) : ;;
      *) continue ;;
    esac
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
    # An adopt-resume records why it took the ref (#2945); surface it so a reader
    # of the board sees the justification, not just the holder.
    reason="$(msg_field "$msg" reason)"
    record=""
    [ -z "$reason" ] || record=" reason=$reason"
    emit "STATUS issue=$issue ref=$ref sha=$sha machine=$machine actor=$actor ts=$ts age=$age$record"
  done <<< "$raw"
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
  local smoke_err
  if ! smoke_err="$(git push "$REMOTE" "${sha}:${ref}" 2>&1 >/dev/null)"; then
    # An unauthenticated git is the #1 reason this preflight fails on a fresh box,
    # and blaming the ref namespace sends the operator hunting the wrong thing
    # (#2942) — so name the credential fault when the stderr says so.
    if git_stderr_is_auth "$smoke_err"; then
      emit "SMOKE-FAIL remote=$REMOTE ref=$ref reason=auth (git cannot authenticate — NOT a namespace restriction; fix with 'gh auth setup-git' or 'bash scripts/bootstrap-agent-machine.sh --yes')"
      return 1
    fi
    emit "SMOKE-FAIL remote=$REMOTE ref=$ref reason=push-rejected (does $REMOTE permit the refs/claims/* namespace?)"
    return 1
  fi
  # `|| true`: an ls-remote failure here must NOT abort before the cleanup delete
  # below (a stranded smoke ref is the worst outcome). A "" seen → SMOKE-FAIL.
  seen="$(git ls-remote "$REMOTE" "$ref" 2>/dev/null | awk '{print $1}' | head -1 || true)"
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
  "") die_usage "a subcommand is required: claim <N> | verify <N> | adopt <N> --expect <sha>|none [--reason <why>] | release <N> [--force] | status [<N>] | smoke" ;;
  *)  die_usage "unknown subcommand: $SUBCOMMAND (expected claim|verify|adopt|release|status|smoke)" ;;
esac
