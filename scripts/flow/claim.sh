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
# IT IS DELIBERATELY NOT AUTO-ADVERTISED (owner decision, #2945). The refusal below
# DIAGNOSES the lane (which branch blocks it, and that the claim ref itself is free)
# and POINTS AT THE DOCUMENTED PROCEDURE — it never prints a copy-pasteable resume
# command. The readers are agents that run printed remediations LITERALLY, so a
# printed command is a hand-away, and deciding whether a lane is truly abandoned
# needs signals this script cannot read soundly (three successive attempts at such a
# liveness probe each shipped a new way to hand an ACTIVELY-WORKED lane to a second
# writer). The abandonment test lives where it already has the inputs:
# `flow-board`'s reaper criteria / `claim-heartbeat.sh should-reap`, plus the board
# Status and the branch/PR author.
#
# LEGACY GUARD (mixed-fleet safety): older workers still branch-lock with a
# `refs/heads/issue-<N>-*` branch. `claim` refuses if any such branch exists on
# origin (treat the issue as already-claimed), naming the blocking branch(es) and
# `claim-ref=free` — a diagnosis that used to be missing, and without which a worker
# cannot tell this refusal from a genuinely held claim.
# THAT REFUSAL IS SPLIT IN TWO BY NAME (#3436 AC6), because two states reach it with
# OPPOSITE remedies: an ABANDONED PEER lane (`reason=legacy-branch-lock`, unchanged
# wording and unchanged remedy) and OUR OWN branch resumed after a legitimate release
# (`reason=released-then-resumed`), where the abandoned-lane procedure is exactly the
# wrong advice. The split is decided by MACHINE-LOCAL evidence (lane-lock SELF, a live
# local lane-lock holder, or a local worktree on this issue's branch) and is fail-closed
# toward the generic verdict. BOTH still refuse, both still exit 2, and NEITHER prints a
# runnable resume command.
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
#                                             Every HELD line also carries a
#                                             `lane-lock=<state>` WARNING field read from
#                                             the machine-local lane-directory lock
#                                             (scripts/flow/lane-lock.sh, #3436 AC5):
#                                             free / no-lane-dir / occupied-alive /
#                                             occupied-unknown-<verdict> /
#                                             reclaimable-<DEAD-verdict> /
#                                             unmeasured(<cause>). An occupied state also
#                                             NAMES the occupant on stderr. It is a report
#                                             and NOTHING ELSE: it never changes the claim
#                                             verdict or the exit code.
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
#                                             as is a SUPPLIED --reason '' (on BOTH the empty-lease
#                                             and the CAS path) or one with nothing recordable in it
#                                             ('   ', '---', '…'), one that records as a bare
#                                             PLACEHOLDER ('why', 'todo', 'tbd', 'xxx', …, the
#                                             shape a verbatim-run `--reason <why>` produces),
#                                             or one still carrying an UNSUBSTITUTED '<…>'
#                                             anywhere in it (a copied template such as
#                                             `--reason resume-legacy-branch-lock:<branch>`
#                                             sanitizes to a non-sentinel token, so it is
#                                             rejected on the RAW text, before sanitization):
#                                             the record must say WHY.
#                                             RE-ENTRANT: if the ref is already held by THIS
#                                             machine+actor, adopt reports ADOPTED (re-entrant)
#                                             exit 0 — a retry after a confirm-read blip must
#                                             never abandon an issue we still hold. In CAS mode,
#                                             when the ref sits at some OTHER sha of ours, that
#                                             verdict names BOTH shas (re-entrant, lease-mismatch
#                                             expected=/actual=), so a VIOLATED compare-and-swap is
#                                             never reported as a satisfied one; when the ref is
#                                             still at --expect (the precondition HELD, only our
#                                             new commit did not land) it is the plain re-entrant
#                                             verdict — no mismatch that did not happen.
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
#            false re-entrancy on someone else's claim (#2945). It must also RECORD
#            something: an actor with fewer than 3 recordable characters (or the bare
#            `unspecified` sentinel) is a usage error (exit 64) — otherwise two
#            distinct-but-unrecordable actors ('***' and '???') aliased onto ONE
#            identity, letting the second satisfy the holder gate for a claim it does
#            not own (the opposite of --reason's fail-closed direction).
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
#   gh is consulted by exactly ONE path, via `open_pr_count`, degrading LOUDLY
#   (stderr note + a `-1` count) when gh is absent/errors: `release` without --force
#   (the open-PR guard). Nothing else touches gh — `claim` (including its
#   LEGACY-BRANCH refusal), `adopt` and `release --force` never run it, so no
#   arbitration or refusal text depends on a GitHub read. THAT IS STILL TRUE AND IS
#   NOT WIDENED by the lane-lock report below.
#   ONE non-gh shell-out was added (#3436): `claim` runs the sibling
#   `scripts/flow/lane-lock.sh` — `probe` for its `lane-lock=` warning field, and
#   `verify` for the released-then-resumed evidence. Both are LOCAL, READ-ONLY (the
#   probe writes no record, takes no lock and creates no directory), network-free and
#   gh-free, they are bounded by `timeout` when it is available, and EVERY failure
#   mode of either is NON-FATAL: it degrades to `lane-lock=unmeasured(<cause>)` /
#   "no evidence" and can never change a claim verdict or exit code. The path is
#   resolved from this script's own directory with no env override (#3312: the
#   constrained party must not choose its own enforcer).
#
# EXIT CODES
#   0  success (CLAIM HELD, VERIFY-OK, ADOPTED, RELEASED, SMOKE-OK, status render)
#   2  lost / refused (CLAIM LOST, VERIFY-FAIL, ADOPT-LOST, RELEASE-REFUSED)
#   1  infra / git / gh failure — retryable, NOT a race-loss (CLAIM ERROR reason=infra,
#      SMOKE-FAIL). EVERY remote-reading subcommand (claim/verify/adopt/release/status)
#      maps an ls-remote/push/delete failure to ERROR (exit 1), so a network blip never
#      makes a worker conclude it LOST/does-not-hold/RELEASED. `claim` also never reports
#      LOST when nobody holds the ref — whether the push was REJECTED or ACCEPTED, a
#      re-read that finds the ref absent is infra (a reaper delete can land in that
#      window; either way the lane is FREE, so exit 2 would be a lie), and
#      and NO subcommand reports LOST/ADOPT-LOST/VERIFY-FAIL/not-holder when the holder
#      commit's message is UNREADABLE (`detail=holder-commit-unreadable`): the best-effort
#      fetch may simply not have landed the object, so the holder is unknown — possibly US.
#      `adopt` in CAS mode also never reports ADOPT-LOST while the ref still sits at
#      exactly `--expect` (`detail=adopt-cas-rejected-but-ref-unchanged`): the lease
#      precondition HELD, so the push failed for some other reason — no race happened.
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

# emit_unreadable_holder <issue> <sha> — the claim commit's message could NOT be read
# (holder_identity state 2). The holder is UNKNOWN — possibly US — so EVERY caller maps
# it to this ONE retryable infra verdict, never to LOST / ADOPT-LOST / VERIFY-FAIL /
# not-holder. Shared on purpose: four separate review rounds found this same
# "an unread signal was reported as abandon/does-not-hold" shape in a different caller,
# so the wording and the verdict live in exactly one place (#2945 review).
emit_unreadable_holder() {
  emit_infra "issue=$1 ref=refs/claims/issue-$1 sha=$2 detail=holder-commit-unreadable (the claim object did not fetch — the holder is UNKNOWN, possibly us; NOT a lost race and NOT a 'you do not hold it' verdict)"
}

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

# this_machine — the machine identity, SANITIZED to one token (#2945 review). `machine=`
# is the FIRST identity token in the claim message AND the value `verify`/non-forced
# `release` compare against, and it was interpolated RAW — the one user-controlled field
# sanitize_field's contract missed. Two consequences, both reachable through an env var:
#   - CLAIM_MACHINE="build box" wrote `machine=build box pid=…`, so the parser returned
#     `build` while the comparison used `build box` — the HOLDER could never verify or
#     non-force-release its OWN claim: a permanently stuck ref needing --force.
#   - a value carrying `actor=` shifted the recorded actor — the same forgery class closed
#     for --reason/--actor.
# Sanitizing HERE (one definition) rather than at each use guarantees the WRITTEN and the
# COMPARED token are always the same value. A machine identity must still be UNIQUE per
# box (see IDENTITY): sanitization makes it parseable, not distinct.
this_machine() { sanitize_field "${CLAIM_MACHINE:-$(hostname -s)}"; }

# resolve_actor <raw> — the actor identity, sanitized to one token. Sanitizing at
# the ARG BOUNDARY (every subcommand, before any comparison) keeps the written
# record and the identity match on exactly the same value.
#
# IT MUST RECORD SOMETHING (#2945 review) — the same fail-closed gate `--reason`
# carries, for a stronger reason: the actor is part of the HOLDER IDENTITY that gates
# re-entrancy, `verify` and non-forced `release`. Letting an unrecordable value fall
# back to the `unspecified` sentinel ALIASED two distinct actors onto ONE identity:
# `claim N --actor '***'` recorded `actor=unspecified`, and a later
# `release N --actor '???'` resolved to `unspecified` too — satisfying the holder gate
# for a claim it does not own. So an actor that records as the sentinel (including a
# literal `--actor unspecified`, which records nothing either) or carries fewer than 3
# recordable characters is a USAGE ERROR, never a silent coercion.
resolve_actor() {
  local tok
  tok="$(sanitize_field "${1:-}")"
  if [ "$tok" = "unspecified" ] || [ "${#tok}" -lt 3 ]; then
    die_usage "--actor must carry at least 3 recordable characters ([A-Za-z0-9._:/#-]); '${1:-}' records as '$tok', and the actor is part of the HOLDER IDENTITY — two unrecordable actors must never alias onto one identity"
  fi
  printf '%s\n' "$tok"
}

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

# holder_identity <sha> <actor> — classify the holder of a claim commit by READING
# its pushed message. THREE outcomes, deliberately distinct (#2945 review):
#   0  the holder is US (this machine+actor) — re-entrant
#   1  the holder is SOMEONE ELSE (the message was read; the identity differs)
#   2  UNREADABLE — the commit object/message could not be read at all (fetch_claim
#      swallows its errors, so a transient fetch failure over an object we do not have
#      locally lands here), so the holder is UNKNOWN — possibly US.
# Collapsing 2 into 1 is the "an unread signal must never mean abandon" bug: callers
# fall through to LOST/ADOPT-LOST/VERIFY-FAIL/not-holder (exit 2), which workers read as
# "you do not own this, move on". A machine would then drop an issue whose claim ref it
# still holds, and nobody else could take it either (the ref is held) — a permanent
# stall, whose tell is the empty `holder-machine= actor=` render. EVERY caller maps 2 to
# `emit_unreadable_holder` + return 1 (retryable), like every other unread remote signal
# here — and there is deliberately NO boolean wrapper around this function: the earlier
# `holder_is_us` helper re-collapsed 2 into "someone else" for `verify` and `release`,
# which is exactly how that contract was shipped false once already (#2945 review).
holder_identity() {
  local sha="$1" actor="$2" msg h_machine h_actor
  msg="$(git log -1 --format=%B "$sha" 2>/dev/null || true)"
  [ -n "$msg" ] || return 2
  h_machine="$(msg_field "$msg" machine)"
  h_actor="$(msg_field "$msg" actor)"
  [ "$h_machine" = "$(this_machine)" ] && [ "$h_actor" = "$actor" ] && return 0
  return 1
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
# machine=/actor= trailers, which made the old identity-based "all-ours -> no block"
# re-entrancy unreachable (#2945). Re-entrancy for a branch you own is now
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


# ---------------------------------------------------------------------------
# LANE-LOCK REPORT (issue #3436 AC5) — the two locks finally say hello.
#
# `refs/claims/issue-<N>` is a HARD control CROSS-machine (git arbitrates the
# push server-side) and a purely ADVISORY one LOCALLY: a second session on the
# same box that never runs this script simply walks into the lane directory.
# Measured 2026-08-28 — two Claude sessions worked ONE issue in ONE worktree on
# ONE box for ~20 minutes, and the only thing that noticed was agent-gate.sh's
# `tree-integrity`, by accident. `scripts/flow/lane-lock.sh` is the
# machine-local half of the fix; the helpers below let `claim` REPORT that
# half's state on its verdict line, so a claim GRANTED over an occupied lane
# directory is visible at the moment it is granted (#3436 AC5).
#
# IT IS A WARNING. IT MUST NEVER CHANGE THE CLAIM VERDICT OR THE EXIT CODE.
# A granted claim stays granted; a lost claim stays lost. The claim's arbiter is
# git's ref update and nothing else, so NOTHING derived from the lane lock may
# turn a HELD into anything else. Consequently EVERY failure mode of the probe
# (script absent/unreadable, non-zero exit, timeout, output this parser does not
# recognise) lands on `lane-lock=unmeasured(<cause>)` and the claim proceeds.
#
# THE LIVENESS SPLIT IS CONSUMED, NEVER RE-IMPLEMENTED. lane-lock.sh owns the
# closed verdict set (only DEAD-* permits reclaim; every UNKNOWN-* refuses); a
# second copy of that judgement here would be a second, weaker arbiter of the
# same fact — the shape #2665 exists to remove. So this code maps verdicts, and
# an unrecognised verdict maps to an OCCUPIED state, never to a free one.
#
# `probe` is the right entry point and the only one used for the report: it is
# READ-ONLY (no record, no mutex, no log, no mkdir) and exits 0 even for an
# occupied lane, because occupancy is DATA to a probe rather than an error.
LANE_LOCK_SH=""
_lane_lock_dir="$(cd -- "$(dirname -- "$0")" >/dev/null 2>&1 && pwd)" || _lane_lock_dir=""
[ -z "$_lane_lock_dir" ] || LANE_LOCK_SH="$_lane_lock_dir/lane-lock.sh"
unset _lane_lock_dir
# Resolved from THIS script's own directory with NO env override, on #3312's
# ruling: the constrained party must not choose its own enforcer. An override
# would let a caller point the report at a script that always answers "free".

LANE_LOCK_PROBED=""            # memo: the issue already probed this run
LANE_LOCK_STATE=""             # the `lane-lock=` field value
LANE_LOCK_DIR=""               # lane directory AS LANE-LOCK.SH RESOLVED IT
LANE_LOCK_LIVENESS=""          # the probe's verbatim liveness verdict
LANE_LOCK_HOLDER=""            # occupant description for the stderr note (AC2)
LANE_LOCK_HOLDER_MACHINE=""
LANE_LOCK_OUR_MACHINE=""       # lane-lock.sh's OWN idea of this machine

# lane_lock_available — 0 iff the sibling script is present and readable. It is
# invoked as `bash <path>`, so the executable bit is deliberately not required:
# a checkout that lost the mode bit should still get the report.
lane_lock_available() {
  [ -n "$LANE_LOCK_SH" ] && [ -f "$LANE_LOCK_SH" ] && [ -r "$LANE_LOCK_SH" ]
}

# lane_lock_probe <N> — populate the globals above. ALWAYS returns 0: a probe
# failure is a degraded REPORT, never a claim failure. Memoized per issue so the
# four HELD emit sites cost one probe.
lane_lock_probe() {
  local issue="$1" rc=0 out="" line="" verdict=""
  if [ "$LANE_LOCK_PROBED" = "$issue" ]; then return 0; fi
  LANE_LOCK_PROBED="$issue"
  LANE_LOCK_STATE=""; LANE_LOCK_DIR=""; LANE_LOCK_LIVENESS=""
  LANE_LOCK_HOLDER=""; LANE_LOCK_HOLDER_MACHINE=""; LANE_LOCK_OUR_MACHINE=""

  if ! lane_lock_available; then
    LANE_LOCK_STATE="unmeasured(lane-lock-script-unreadable)"
    note "lane-lock: NOT measured — ${LANE_LOCK_SH:-<unresolved path>} is absent or unreadable. The claim verdict below is unaffected."
    return 0
  fi
  # BOUNDED: a hung probe must never hang a claim. `timeout` is used when present
  # and skipped when absent — a missing timeout is not a reason to skip the
  # report, and the probe reads local files only (no git, no gh, no network).
  if command -v timeout >/dev/null 2>&1; then
    out="$(timeout 20 bash "$LANE_LOCK_SH" probe "$issue" 2>/dev/null)" || rc=$?
  else
    out="$(bash "$LANE_LOCK_SH" probe "$issue" 2>/dev/null)" || rc=$?
  fi
  if [ "$rc" -ne 0 ]; then
    LANE_LOCK_STATE="unmeasured(probe-exit-$rc)"
    note "lane-lock: NOT measured — 'lane-lock.sh probe $issue' exited $rc. The claim verdict below is unaffected."
    return 0
  fi
  line="$(printf '%s\n' "$out" | grep '^LANE-LOCK: ' | head -1)" || line=""
  if [ -z "$line" ]; then
    LANE_LOCK_STATE="unmeasured(probe-output-unrecognised)"
    note "lane-lock: NOT measured — the probe printed no 'LANE-LOCK:' line. The claim verdict below is unaffected."
    return 0
  fi
  verdict="$(printf '%s\n' "${line#LANE-LOCK: }" | awk '{print $1}')" || verdict=""
  LANE_LOCK_DIR="$(msg_field "$line" lane-dir)"
  LANE_LOCK_LIVENESS="$(msg_field "$line" liveness)"
  # lane-lock.sh's own machine token, taken from the `our-token` it printed, so
  # the holder-vs-us comparison below is apples-to-apples. CLAIM_MACHINE is a
  # DIFFERENT namespace's identity (and tests deliberately override it), so it is
  # never used to answer a question about the lane lock.
  LANE_LOCK_OUR_MACHINE="$(printf '%s' "$(msg_field "$line" our-token)" | cut -d: -f1)"

  case "$verdict" in
    FREE)
      # A lane DIRECTORY that does not exist yet is the ordinary case for a fresh
      # claim and must read as unremarkable, not as a warning. An empty lane-dir
      # field means the measurement did not happen, which is not "free".
      if [ -z "$LANE_LOCK_DIR" ]; then
        LANE_LOCK_STATE="unmeasured(probe-named-no-lane-dir)"
      elif [ -d "$LANE_LOCK_DIR" ]; then
        LANE_LOCK_STATE="free"
      else
        LANE_LOCK_STATE="no-lane-dir"
      fi
      ;;
    HELD)
      local hpid hactor hts hage
      LANE_LOCK_HOLDER_MACHINE="$(msg_field "$line" holder-machine)"
      hpid="$(msg_field "$line" holder-pid)"
      hactor="$(msg_field "$line" holder-actor)"
      hts="$(msg_field "$line" acquired-ts)"
      hage="$(msg_field "$line" age)"
      # NAME THE OCCUPANT (#3436 AC2): a collision diagnosed generically sends
      # the reader to the wrong problem.
      LANE_LOCK_HOLDER="lane-dir=${LANE_LOCK_DIR:-unknown} liveness=${LANE_LOCK_LIVENESS:-unstated} holder-machine=${LANE_LOCK_HOLDER_MACHINE:-unknown} holder-actor=${hactor:-unknown} holder-pid=${hpid:-unknown} acquired-ts=${hts:-unknown} age=${hage:-unknown}"
      case "${LANE_LOCK_LIVENESS:-}" in
        ALIVE|SELF)  LANE_LOCK_STATE="occupied-alive" ;;
        DEAD-*)      LANE_LOCK_STATE="reclaimable-$(sanitize_field "$LANE_LOCK_LIVENESS")" ;;
        UNKNOWN-*)   LANE_LOCK_STATE="occupied-unknown-$(sanitize_field "$LANE_LOCK_LIVENESS")" ;;
        # Fail closed toward OCCUPIED: a verdict word this mapping does not know
        # is precisely the case where inheriting the permissive branch would hide
        # the collision (CLAUDE.md's affirmative-measurement rule).
        *)           LANE_LOCK_STATE="occupied-unknown-$(sanitize_field "${LANE_LOCK_LIVENESS:-unstated}")" ;;
      esac
      ;;
    *)
      LANE_LOCK_STATE="unmeasured(probe-verdict-$(sanitize_field "${verdict:-none}"))"
      ;;
  esac

  # SELF is broken out of the occupied note (and only the note): `probe` DOES
  # report it when the record's token is ours, and telling a session "if that
  # occupant is not you..." about its own lock is a warning that trains readers to
  # ignore the warning. The STATE stays occupied-alive — the lane is occupied, and
  # by a live process; who it is belongs in the text, not in the classification.
  case "$LANE_LOCK_STATE:${LANE_LOCK_LIVENESS:-}" in
    occupied-alive:SELF)
      note "lane-lock: the lane directory is held by THIS session (liveness=SELF) — $LANE_LOCK_HOLDER"
      ;;
  esac
  case "$LANE_LOCK_STATE" in
    occupied-*)
      [ "${LANE_LOCK_LIVENESS:-}" != "SELF" ] || return 0
      note "lane-lock: the lane DIRECTORY is ALREADY OCCUPIED — $LANE_LOCK_HOLDER"
      note "lane-lock: this is a WARNING ONLY; the claim verdict below is unaffected. If that occupant is not you, do NOT write in that lane directory — two sessions in one worktree is #3436, and 'git add -A' there launders one session's work into the other's commit."
      ;;
    reclaimable-*)
      note "lane-lock: a lane lock RECORD exists but its holder is gone ($LANE_LOCK_LIVENESS) — $LANE_LOCK_HOLDER"
      note "lane-lock: 'lane-lock.sh acquire' reclaims a dead holder itself; the claim verdict below is unaffected."
      ;;
  esac
  return 0
}

# lane_lock_field <N> — the `lane-lock=<state>` token for a verdict line.
lane_lock_field() {
  lane_lock_probe "$1"
  printf 'lane-lock=%s' "${LANE_LOCK_STATE:-unmeasured(not-probed)}"
}

# lane_local_evidence <N> — does THIS BOX already occupy that lane? Sets
# LANE_LOCAL_EVIDENCE to the token that established it, or "" for none.
#
# It answers ONE question for ONE consumer: the released-then-resumed refusal
# (#3436 AC6) below. Evidence, strongest first:
#   (a) lane-lock verify exits 0        -> THIS VERY SESSION holds the lane lock
#   (b) probe says ALIVE and the holder machine is lane-lock.sh's own machine
#                                       -> a live LOCAL process owns the lane
#   (c) a git worktree at the lane dir whose HEAD branch is issue-<N>-*
#                                       -> the lane predates this lock (the case
#                                          that makes the feature useful on day
#                                          one, since a session that resumed
#                                          before this lock existed never took it)
#
# FAIL-CLOSED TOWARD "NO EVIDENCE". Every unread signal — script missing, probe
# error, git failure, unreadable directory — yields "", which keeps the GENERIC
# `legacy-branch-lock` verdict. Direction matters and is the whole design: the
# released-then-resumed text sends the reader down the RE-ACQUIRE path, and doing
# that for a lane a LIVE PEER owns hands a second writer an actively-worked lane
# — the inverse hazard, and strictly worse than an over-generic diagnosis.
LANE_LOCAL_EVIDENCE=""
lane_local_evidence() {
  local issue="$1" rc=0 branch=""
  LANE_LOCAL_EVIDENCE=""
  lane_lock_available || return 0
  # Probe FIRST, unconditionally, even though (a) can answer without it: the
  # verdict line that consumes this evidence also reports `lane-lock=<state>`, and
  # returning early on (a) left that field reading `unmeasured(not-probed)` — a
  # measurement that did happen, reported as one that did not.
  lane_lock_probe "$issue"

  # (a) SELF — the only subcommand that can answer "this very session".
  if command -v timeout >/dev/null 2>&1; then
    timeout 20 bash "$LANE_LOCK_SH" verify "$issue" >/dev/null 2>&1 || rc=$?
  else
    bash "$LANE_LOCK_SH" verify "$issue" >/dev/null 2>&1 || rc=$?
  fi
  if [ "$rc" -eq 0 ]; then
    LANE_LOCAL_EVIDENCE="lane-lock-self"
    return 0
  fi

  # (b) a LIVE process on THIS machine holds it. `ALIVE` already implies the
  # holder is local (lane-lock.sh answers UNKNOWN-FOREIGN for another machine),
  # but the machine tokens are compared anyway so the claim rests on a field that
  # was actually read rather than on an implication.
  if [ "${LANE_LOCK_LIVENESS:-}" = "ALIVE" ] \
     && [ -n "$LANE_LOCK_HOLDER_MACHINE" ] && [ -n "$LANE_LOCK_OUR_MACHINE" ] \
     && [ "$LANE_LOCK_HOLDER_MACHINE" = "$LANE_LOCK_OUR_MACHINE" ]; then
    LANE_LOCAL_EVIDENCE="lane-lock-alive-local"
    return 0
  fi

  # (c) the pre-lock case: a local worktree on this issue's branch.
  if [ -n "$LANE_LOCK_DIR" ] && [ -d "$LANE_LOCK_DIR" ]; then
    branch="$(git -C "$LANE_LOCK_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null)" || branch=""
    case "$branch" in
      "issue-${issue}-"*) LANE_LOCAL_EVIDENCE="lane-worktree-branch:$(sanitize_field "$branch")" ;;
    esac
  fi
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
    local hrc=0
    holder_identity "$existing" "$actor" || hrc=$?
    if [ "$hrc" -eq 0 ]; then
      # AC5 (#3436): a WARNING field only. lane_lock_field NEVER changes the
      # verdict or the exit code — a granted claim stays granted.
      emit "HELD issue=$issue ref=refs/claims/issue-$issue sha=$existing $(holder_desc "$existing") $(lane_lock_field "$issue") (re-entrant)"
      return 0
    fi
    # UNREADABLE holder metadata is UNKNOWN, never "someone else" (#2945 review): the
    # fetch is best-effort, so a transient failure leaves the claim object absent and
    # the message unreadable — including when the holder is US.
    if [ "$hrc" -eq 2 ]; then
      emit_unreadable_holder "$issue" "$existing"
      return 1
    fi
    emit "LOST issue=$issue ref=refs/claims/issue-$issue sha=$existing $(holder_token "$existing")"
    return 2
  fi

  # Legacy branch-lock guard (mixed fleet). An `issue-<N>-*` branch on origin is
  # treated as an older worker's branch lock. An enumeration OUTAGE is UNKNOWN, not
  # an all-clear (#2677 item 2) — it maps to ERROR infra (retryable), never a claim
  # granted on an unread guard.
  if ! legacy_branch_scan "$issue"; then
    emit_infra "issue=$issue detail=legacy-branch-ls-remote-unreachable-on-$REMOTE (cannot tell 'no legacy branch' from an outage)"
    return 1
  fi
  if [ -n "$LEGACY_BRANCHES" ]; then
    # DIAGNOSE, DO NOT HAND OVER (owner decision, #2945). The refusal names the
    # blocking branch(es) and says the claim REF is free — the diagnosis a bare LOST
    # was missing, and the reason workers previously concluded the issue was a dead
    # end and hand-crafted claim commits. It deliberately prints NO runnable resume
    # command: the readers are agents that execute printed remediations literally, and
    # an older-fleet worker locks with the BRANCH while holding no claim ref, so a
    # printed empty-lease adopt WOULD succeed against an actively-worked lane. Deciding
    # abandonment needs liveness inputs this script cannot read soundly (three
    # successive in-script probes each shipped a fresh way to hand away a live lane),
    # so the pointer goes to the tools that own that judgement.
    # AC6 (#3436): SPLIT THE REFUSAL, because the two states have OPPOSITE
    # remedies and the old single verdict sent every reader to the wrong one.
    #   - an ABANDONED PEER lane  -> confirm abandonment, then resume it
    #   - OUR OWN branch, resumed after a legitimate release -> re-take the lock
    # Measured (#3393, 2026-08-29): a slice shipped, the claim ref was released
    # correctly and the board went back to Ready — all proper finalize behaviour —
    # and then work resumed on the SAME branch for 20+ commits holding no claim,
    # while the board advertised the issue as available. `claim` refused with
    # reason=legacy-branch-lock and pointed at the abandoned-lane procedure
    # (should-reap / board Status / branch author), none of which applies when the
    # lane is YOURS and live. The evidence is MACHINE-LOCAL and fail-closed toward
    # the generic verdict (see lane_local_evidence).
    lane_local_evidence "$issue"
    if [ -n "$LANE_LOCAL_EVIDENCE" ]; then
      emit "LOST issue=$issue reason=released-then-resumed detail=$LEGACY_BRANCHES exists on $REMOTE claim-ref=free resume=documented-procedure lane-evidence=$LANE_LOCAL_EVIDENCE lane-lock=${LANE_LOCK_STATE:-unmeasured(not-probed)} (THIS MACHINE ALREADY OCCUPIES THAT LANE, so the branch above is almost certainly YOUR OWN, resumed after a legitimate release: finalize released the claim ref and no step re-took it when work restarted (#3436 AC6). This is NOT a stale lock and NOT an abandoned peer lane, so the ABANDONED-LANE PROCEDURE DOES NOT APPLY — do not run claim-heartbeat.sh should-reap, and do not read the board Status or the branch author as evidence of abandonment: those describe a lane nobody is in, and someone is in this one. REMEDY: re-check ownership with this script's 'verify' subcommand FIRST; if you do not hold the ref, take the documented empty-lease resume path — the compare-and-swap adoption that records WHO took it and WHY — spelled out in the claim protocol (CLAUDE.md) and in 'bash scripts/flow/claim.sh -h'. It is deliberately NOT printed here as a runnable line (#2945): the readers are agents that run printed remediations literally, and this diagnosis rests on local evidence that can be wrong. Never hand-craft a claim commit)"
      return 2
    fi
    emit "LOST issue=$issue reason=legacy-branch-lock detail=$LEGACY_BRANCHES exists on $REMOTE claim-ref=free resume=documented-procedure (the claim REF is FREE — this is an older worker's BRANCH lock, not a held claim. A sanctioned resume exists and is documented in the claim protocol (CLAUDE.md) and in 'bash scripts/flow/claim.sh -h'; it is intentionally NOT printed here as a runnable line. CONFIRM the lane is abandoned FIRST — flow-board's reaper criteria via claim-heartbeat.sh should-reap, the board Status, and the branch/PR author — then follow that documented procedure. Never resume a lane a live worker owns, and never hand-craft a claim commit)"
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
    local hrc2=0
    holder_identity "$now" "$actor" || hrc2=$?
    if [ "$hrc2" -eq 0 ]; then
      # AC5 (#3436): warning field only — see the pre-check site above.
      emit "HELD issue=$issue ref=refs/claims/issue-$issue sha=$now $(holder_desc "$now") $(lane_lock_field "$issue") (re-entrant)"
      return 0
    fi
    # Same rule as the pre-check: unreadable holder metadata is retryable infra, not a
    # LOST verdict on a ref that may well be ours.
    if [ "$hrc2" -eq 2 ]; then
      emit_unreadable_holder "$issue" "$now"
      return 1
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
    # AC5 (#3436): warning field only — see the pre-check site above.
    emit "HELD issue=$issue ref=refs/claims/issue-$issue sha=$sha machine=$(this_machine) actor=$actor $(lane_lock_field "$issue")"
    return 0
  fi
  if [ -z "$confirmed" ]; then
    # The push was ACCEPTED yet the confirm read finds the ref ABSENT — realistically a
    # reaper's force-delete landing in this window. NOBODY HOLDS IT, so this is not a
    # lost race: the SAME rule, and the same shape, as the rejected-push sibling above
    # (`push-rejected-but-ref-absent`) — ERROR (exit 1, retryable), never a LOST verdict
    # rendering `sha=<gone> holder=unknown`, which a worker reads as "you did not win,
    # take the next item" and so walks away from a FREE lane (#2945 review).
    emit_infra "issue=$issue detail=push-accepted-but-ref-absent-on-confirm-on-$REMOTE (nobody holds it — not a lost race)"
    return 1
  fi
  fetch_claim "$issue"
  # A TOCTOU winner's sha. Route it through the SAME three-outcome identity read as the
  # rejected-push sibling above and as the pre-check, so all of this file's exit-2 sites
  # read as ONE rule: a commit of OURS is a re-entrant HELD (we do hold the ref), an
  # UNREADABLE commit is retryable infra (the holder is UNKNOWN — possibly us — and
  # `fetch_claim` is best effort AND only ever fetches THIS issue's ref, which the landed
  # push left at our own sha, so a TOCTOU winner's object may simply not be here;
  # falling straight through rendered the tell-tale empty `holder-machine= actor=` on a
  # LOST, which the header rule at the top of this file forbids), and ONLY a
  # successfully-read FOREIGN holder is a genuine LOST.
  local crc=0
  holder_identity "$confirmed" "$actor" || crc=$?
  if [ "$crc" -eq 0 ]; then
    # AC5 (#3436): warning field only — see the pre-check site above.
    emit "HELD issue=$issue ref=refs/claims/issue-$issue sha=$confirmed $(holder_desc "$confirmed") $(lane_lock_field "$issue") (re-entrant)"
    return 0
  fi
  if [ "$crc" -eq 2 ]; then
    emit_unreadable_holder "$issue" "$confirmed"
    return 1
  fi
  emit "LOST issue=$issue ref=refs/claims/issue-$issue sha=$confirmed $(holder_token "$confirmed")"
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
  # THREE outcomes, never two (#2945 review): an UNREADABLE holder commit must not be
  # reported as "you do not hold it". `fetch_claim` is best effort, so a transient failure
  # leaves the object absent and the message empty EVEN WHEN THE HOLDER IS US — and a
  # `VERIFY-FAIL … holder-machine= actor=` exit 2 is precisely the verdict that makes a
  # worker abandon an issue it still holds.
  local vrc=0
  holder_identity "$sha" "$actor" || vrc=$?
  if [ "$vrc" -eq 0 ]; then
    emit "VERIFY-OK issue=$issue ref=refs/claims/issue-$issue sha=$sha $(holder_desc "$sha")"
    return 0
  fi
  if [ "$vrc" -eq 2 ]; then
    emit_unreadable_holder "$issue" "$sha"
    return 1
  fi
  emit "VERIFY-FAIL issue=$issue ref=refs/claims/issue-$issue sha=$sha holder-$(holder_desc "$sha") wanted-machine=$(this_machine) wanted-actor=$actor"
  return 2
}

# ---------------------------------------------------------------------------
cmd_adopt() {
  local issue="" actor="${CLAIM_ACTOR:-flow}" expect="" reason="" reason_given=0 mode="cas"
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --actor)  [ "$#" -ge 2 ] || die_usage "--actor requires a value";  actor="$2";  shift 2 ;;
      --expect) [ "$#" -ge 2 ] || die_usage "--expect requires a value"; expect="$2"; shift 2 ;;
      # reason_given records the FLAG's presence, so a supplied-but-empty value cannot
      # skip validation (see the gate below).
      --reason) [ "$#" -ge 2 ] || die_usage "--reason requires a value"; reason="$2"; reason_given=1; shift 2 ;;
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
  # NORMALIZE THE CASE of a hex lease value. git resolves an object name
  # case-insensitively, so `--expect <UPPERCASE sha>` satisfies the lease and the push
  # lands — but every comparison below (`now = expect`) is a STRING compare against
  # ls-remote's lowercase output, so an uppercase value made a satisfied precondition
  # read as a violated one. Normalizing here keeps the arbiter and the verdict agreed.
  if [ "$mode" = "cas" ]; then
    expect="$(printf '%s' "$expect" | LC_ALL=C tr 'A-F' 'a-f')"
  fi

  # VALIDATE THE RECORDED TOKEN, NOT THE RAW TEXT (#2945 review). The audit value
  # is what lands in the claim commit, so gating on `[ -n "$reason" ]` let '   ',
  # '---', '…' or an expansion like "$UNSET_VAR " through and recorded them as
  # `reason=unspecified` — indistinguishable from supplying no reason at all, which
  # defeats the "record WHY" requirement. Sanitize FIRST, then require a token that
  # actually says something: not the `unspecified` sentinel sanitize_field falls back
  # to (so a literal `--reason unspecified` is refused too — it records nothing), and
  # at least 3 recordable characters. Same fail-closed direction as `--expect ''`.
  #
  # AND THE GATE KEYS ON "WAS THE FLAG SUPPLIED", NOT "IS THE RAW TEXT NON-EMPTY"
  # (#2945 review). Guarding it on `[ -n "$reason" ]` silently EXEMPTED `--reason ""` —
  # the classic `--reason "$WHY"` with WHY unset — on the CAS path: the adoption recorded
  # no `reason=` at all, while '   ', '---' and 'x' were exit 64. That was the last
  # asymmetry in an otherwise fail-closed argument surface, and it is the shape most
  # likely to happen by accident. A SUPPLIED-but-empty reason is now exit 64 on BOTH
  # paths (on --expect none the required-reason gate below caught it already).
  local extra="" reason_token=""
  if [ "$reason_given" -eq 1 ]; then
    # AN UNSUBSTITUTED TEMPLATE IS REFUSED BEFORE SANITIZATION (#2945 review). The
    # placeholder gate below only sees the SANITIZED token, so it caught a bare `<why>`
    # but not a template inside a longer value: the documented
    # `--reason resume-legacy-branch-lock:<branch>` sanitizes to
    # `resume-legacy-branch-lock:-branch` — not a sentinel, so it was ACCEPTED and
    # recorded an unresolved placeholder as the audit reason. These commands are read by
    # agents that run printed text LITERALLY, which is the whole premise of this change,
    # so any surviving `<…>` in the RAW value is a caller bug, not a reason.
    case "$reason" in
      *'<'*'>'*)
        die_usage "adopt: --reason '$reason' still carries an UNSUBSTITUTED placeholder (<…>) — substitute it. e.g. --reason resume-legacy-branch-lock:branch-outlived-claim or --reason 'reaped claim, board says Ready'"
        ;;
    esac
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
        die_usage "adopt: --reason '$reason' records as the PLACEHOLDER '$reason_token' — as uninformative as no reason at all. Say what the resume IS, e.g. --reason resume-legacy-branch-lock:issue-$issue-branch-outlived-claim or --reason 'reaped claim, board says Ready'"
        ;;
    esac
  fi
  if [ "$mode" = "empty" ]; then
    # The resume is a judgement call, so it MUST be self-documenting: the claim
    # commit records who took it (machine/actor/ts) AND why (reason).
    [ -n "$reason_token" ] || die_usage "adopt --expect none requires a --reason saying what the resume IS (it is recorded in the claim commit next to who took it), e.g. --reason resume-legacy-branch-lock:issue-$issue-branch-outlived-claim"
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
  # …but ONLY a GENUINE divergence gets the mismatch wording (#2945 review): when the
  # ref still sits at exactly our --expect value the compare-and-swap precondition DID
  # hold (only our own new commit failed to land), so `lease-mismatch expected=X
  # actual=X` named a divergence that never happened. That case takes the plain
  # re-entrant verdict, whose `from=` is the value the ref really has.
  # …and an UNREADABLE holder commit is UNKNOWN, never a foreign holder (#2945 review):
  # `fetch_claim` swallows its errors, so a transient fetch failure (disk full, partial
  # outage, ref-advertisement hiccup) leaves the object absent and the message empty —
  # even when `now` is OUR OWN claim commit. Falling through to ADOPT-LOST there made a
  # machine abandon an issue it still held the ref for (nobody else can take it either:
  # the ref is held), and rendered the tell-tale empty `holder-machine= actor=`. Treat it
  # as retryable infra, exactly like every other unread signal in this file.
  local arc=0
  if [ -n "$now" ]; then
    holder_identity "$now" "$actor" || arc=$?
    if [ "$arc" -eq 2 ]; then
      emit_unreadable_holder "$issue" "$now"
      return 1
    fi
  fi
  if [ -n "$now" ] && [ "$arc" -eq 0 ]; then
    if [ "$mode" = "empty" ] || [ "$now" = "$expect" ]; then
      emit "ADOPTED issue=$issue ref=refs/claims/issue-$issue sha=$now $(holder_desc "$now") from=$expect (re-entrant)"
    else
      emit "ADOPTED issue=$issue ref=refs/claims/issue-$issue sha=$now $(holder_desc "$now") (re-entrant, lease-mismatch expected=$expect actual=$now — we DO hold the ref, but the compare-and-swap precondition did NOT hold)"
    fi
    return 0
  fi
  # A CAS whose push did not land WHILE THE REF IS STILL EXACTLY AT `--expect` did not
  # lose a race: the lease precondition HELD, so the push failed for some other reason
  # (transient network, an unrecognized auth signature, a server-side hook). `ADOPT-LOST
  # … expected=X actual=X` is self-contradictory on its face, and it makes a worker move
  # on from a claim that is still adoptable and UNTAKEN. So it is retryable infra —
  # regardless of who the (readable) holder is. The empty-lease sibling above
  # (`rejected-but-ref-absent`) and the re-entrant `now == expect` case already had this
  # treatment; the FOREIGN-holder variant was the one missed (#2945 review).
  if [ "$mode" = "cas" ] && [ -n "$now" ] && [ "$now" = "$expect" ]; then
    emit_infra "issue=$issue ref=refs/claims/issue-$issue sha=$now expected=$expect detail=adopt-cas-rejected-but-ref-unchanged (the lease precondition still HOLDS — the push failed for another reason; NOT a lost race) $(holder_token "$now")"
    return 1
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
    # Same three-outcome rule as `verify` (#2945 review): an UNREADABLE holder commit is
    # UNKNOWN, so `reason=not-holder` (exit 2) would refuse the TRUE holder its own
    # release over a best-effort fetch blip — and the refusal's remedy (`--force`) is the
    # one thing a holder must not need for its own claim.
    local hrc3=0
    holder_identity "$sha" "$actor" || hrc3=$?
    if [ "$hrc3" -eq 2 ]; then
      emit_unreadable_holder "$issue" "$sha"
      return 1
    fi
    if [ "$hrc3" -ne 0 ]; then
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
    # that are NOT issue claims (e.g. a leftover `refs/claims/smoke-<commit-sha>` from
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
# `refs/claims/smoke-<commit-sha>` ref). Some managed Git hosts restrict custom ref
# namespaces; if this fails, the whole claim mechanism is unusable on that remote
# and MUST be caught before the fleet relies on it. ALL THREE steps are part of the
# verdict (#3369): a cleanup delete that does not succeed leaves delete capability
# UNPROVEN, and `release` deletes refs/claims/issue-<N> — that is
# `reason=cleanup-unverified`, never a SMOKE-OK with a stderr warning. The reason code
# names the OBSERVATION (a nonzero exit); it attributes no cause, because one exit status
# cannot tell a deletion policy from a network drop. NOT part of the hermetic test
# suite — it mutates the REAL origin. (Verified on github.com/pmcfadin/cqlite
# 2026-07-17: refs/claims/* is pushable.)
cmd_smoke() {
  local ref sha seen
  # THE REF NAME IS DERIVED FROM THE COMMIT SHA, never from an ad-hoc nonce (#3369
  # review). It used to be `$$-${RANDOM}-$(date -u +%s)`: a pid, ONE 15-bit $RANDOM and a
  # second-resolution timestamp. Bash seeds $RANDOM from pid+time, so on identically
  # provisioned machines booting simultaneously — literally this issue's subject, a fleet
  # launched from ONE AMI — all three components are CORRELATED rather than independent.
  # Since every bootstrap now runs this probe against the SHARED origin, a collision
  # presents as a spurious push rejection => `git-push: FAILED` => `--strict` refusing a
  # healthy box, the same failure class as this issue's earlier blockers.
  #
  # `build_claim_commit` is already called here and its message carries machine + pid +
  # TWO $RANDOMs + timestamp, so the commit object is content-addressed over strictly
  # more entropy than the old nonce had — and two runs that differ in any of those fields
  # cannot share a sha. The `refs/claims/smoke-` PREFIX is load-bearing (the docs, the
  # `git ls-remote origin 'refs/claims/smoke-*'` cleanup command, and cmd_status's
  # "never an issue claim" skip all key on it) and is kept.
  sha="$(build_claim_commit "smoke" "smoke")" || { emit "SMOKE-FAIL remote=$REMOTE reason=commit-build"; return 1; }
  ref="refs/claims/smoke-${sha}"
  note "smoke preflight: does $REMOTE accept a push to refs/claims/* ? (ref=$ref)"
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
  # Always clean up the throwaway ref, whatever the ls-remote said — and RECORD whether
  # the cleanup worked. It used to be `|| note "WARNING: ..."`, after which SMOKE-OK was
  # emitted UNCONDITIONALLY with the text "(create + ls-remote + delete verified)" — a
  # verdict claiming more than it measured (#3369). Two costs: a caller (bootstrap's
  # push-capability probe) read SMOKE-OK as proof of the whole cycle and passed a machine
  # that had just STRANDED a ref on the shared origin; and the diagnosis was a `note` on
  # stderr, invisible to any caller capturing the verdict.
  local delete_ok=1
  git push "$REMOTE" --delete "$ref" >/dev/null 2>&1 || delete_ok=0
  if [ "$seen" != "$sha" ]; then
    # The readback failed — but the cleanup delete was already attempted above, and if
    # that ALSO failed, returning here used to suppress it entirely: the operator got a
    # mismatch verdict with no hint that a ref might be sitting on the shared origin
    # (#3369 review). Same reason code, one appended field — not a fourth variant.
    # Deliberately "UNKNOWN" rather than "STRANDED": the readback says the ref is not
    # there and the delete says it could not be removed, so the two signals disagree and
    # neither may be asserted. That is the same three-valued discipline this whole probe
    # is built on.
    local mismatch_extra=""
    [ "$delete_ok" = 0 ] && mismatch_extra=" cleanup-delete=FAILED (whether $ref exists on $REMOTE is UNKNOWN — check 'git ls-remote $REMOTE $ref' and remove it with 'git push $REMOTE --delete $ref' if present)"
    emit "SMOKE-FAIL remote=$REMOTE ref=$ref reason=ls-remote-mismatch seen=${seen:-<none>} expected=$sha$mismatch_extra"
    return 1
  fi
  if [ "$delete_ok" = 0 ]; then
    # DELETE CAPABILITY IS REQUIRED BY THE CLAIM PROTOCOL, not a tidiness nicety:
    # `claim.sh release` deletes refs/claims/issue-<N>, and the reaper depends on it. So a
    # cleanup delete that did not succeed is a FAIL, never a SMOKE-OK with a stderr note.
    #
    # BUT THE REASON CODE STATES THE OBSERVATION, NOT A CAUSE (#3369 review). The first
    # cut called this `delete-rejected` and blamed the remote's ref-deletion policy — a
    # definite causal verdict inferred from ONE bit (a nonzero exit), which a transient
    # network drop or a post-readback auth failure produces identically. That is the
    # affirmative-measurement violation this whole change exists to remove, committed by
    # the change itself. Distinguishing the causes would mean re-deriving them from git's
    # stderr text, which is the same guessing shape one level down. So: what is KNOWN is
    # that the delete exited nonzero, and therefore that delete capability is UNPROVEN
    # and the ref's existence UNKNOWN. Nothing else is claimed.
    emit "SMOKE-FAIL remote=$REMOTE ref=$ref reason=cleanup-unverified (the cleanup delete exited NONZERO — no cause is attributed. Whether $ref still exists on $REMOTE is UNKNOWN: check with 'git ls-remote $REMOTE $ref' and remove it with 'git push $REMOTE --delete $ref' if present. Delete capability is therefore UNPROVEN, and 'claim.sh release' deletes refs/claims/issue-<N>, so this namespace is NOT confirmed usable for claims.)"
    return 1
  fi
  emit "SMOKE-OK remote=$REMOTE namespace=refs/claims/* (create + ls-remote + delete verified)"
  return 0
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
