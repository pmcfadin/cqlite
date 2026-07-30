#!/usr/bin/env bash
# Regression check for issue #2964: scripts/flow/roborev-review.sh must FAIL
# CLOSED on every recorded vacuous-review trigger, and must never let one be
# reported as "roborev clean".
#
# The wrapper's verdict gates a merge (flow-implement's review-first pass and
# flow-closer's confirmation pass before arming `gh pr merge --auto`), so a
# weakened assert in it means the pipeline merges unreviewed code with no red
# anywhere. This check pins all seven triggers so a regression FAILs the fast
# `--lite` loop instead of costing a review round.
#
# HERMETIC BY CONSTRUCTION: no network, no real roborev, no dataset corpus, no
# cargo. A STUB `roborev` placed FIRST on PATH replays the recorded real outputs
# (enqueue lines, verdict text, `show --json` token payloads) against throwaway
# `git init` fixtures that carry their own local bare "origin" so the push assert
# can be exercised in both directions. Everything lives under one temp dir removed
# on EXIT. No wall-clock threshold assert anywhere in the correctness path (#2642).
#
# Run standalone:   bash scripts/tests/test_roborev_review_guard.sh
# Or via the gate:  scripts/agent-gate.sh --lite   (roborev-lints component)
#                   scripts/agent-gate.sh          (roborev-lints + tooling-tests)
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
WRAPPER="$SCRIPT_DIR/../flow/roborev-review.sh"
BLOCK_HEADER="==== ROBOREV REVIEW SUMMARY ===="

if [ ! -f "$WRAPPER" ]; then
  printf 'FAIL - wrapper not found at %s\n' "$WRAPPER"
  exit 1
fi

# Shorten the wrapper's job-record poll bound: a TIMING knob only — fewer polls can
# only make the record MORE likely to be reported DEGRADED (the fail-closed
# direction), so it cannot weaken anything this suite asserts.
export ROBOREV_JOB_RECORD_POLL_ATTEMPTS=2
export ROBOREV_JOB_RECORD_POLL_INTERVAL_SECS=1

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/roborev-guard-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

# ---------------------------------------------------------------------------
# The stub reviewer: first on PATH, driven entirely by STUB_* env vars, replaying
# the RECORDED SHAPE of the real binary's payload — notably `token_usage`, which is
# a JSON-ENCODED STRING (decoded twice) carrying `total_output_tokens`, not a nested
# object with `output_tokens`. Getting that shape wrong is exactly how tier 2 was
# permanently UNAVAILABLE, so the stub reproduces it verbatim.
#   STUB_ANNOUNCE_SHA    sha to announce; empty => emit NO enqueue announcement
#   STUB_JOB             job id in the announcement / json payloads
#   STUB_VERDICT         verdict text written to the transcript
#   STUB_TOKEN_USAGE     pre-escaped body of the token_usage JSON string, or NONE
#   STUB_REVIEW_RC       exit code of `roborev review`
#   STUB_STATUS          job status field (done / failed / ...)
#   STUB_GIT_REF         job git_ref field (the structured reviewed sha)
#   STUB_MODEL           job model field
#   STUB_REQUESTED_MODEL job requested_model field
#   STUB_PROMPT          prompt text (plain, no quotes/backslashes)
#   STUB_VERDICT_FIELD   the job record's structured `verdict` letter (P/F), '' to omit
#   STUB_PAYLOAD_JOB     the id INSIDE the payload (differs from the announced job to
#                        pin the narrowed ID-less fallback in roborev-job-facts.py)
#   STUB_RECORD_BLANK_FOR the first N record reads return an empty record, so the
#                        wrapper's bounded poll for the ASYNCHRONOUSLY written record
#                        can be exercised (counter kept in $STUB_INVOKED.reads)
#   STUB_HAS_TOKEN_DATA  emit a has_token_data field with this value (true/false)
#   STUB_SHOW_JSON       `none` => `show --json` returns null, forcing the
#                        `list --json` fallback path
#   STUB_INVOKED         file the stub appends its argv to (empty => never run)
# ---------------------------------------------------------------------------
stubbin="$tmp/bin"
mkdir -p "$stubbin"
cat >"$stubbin/roborev" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
cmd="${1:-}"
shift || true

emit_job_object() {
  local usage="" extra=""
  if [ -n "${STUB_HAS_TOKEN_DATA:-}" ]; then
    extra=",\"has_token_data\":${STUB_HAS_TOKEN_DATA}"
  fi
  if [ "${STUB_TOKEN_USAGE:-NONE}" != "NONE" ]; then
    usage=",\"token_usage\":\"${STUB_TOKEN_USAGE}\""
  fi
  local git_ref="${STUB_GIT_REF:-${STUB_ANNOUNCE_SHA:-}}"
  if [ "${STUB_GIT_REF:-}" = none ]; then git_ref=""; fi
  if [ -n "${STUB_VERDICT_FIELD:-}" ]; then
    extra="$extra,\"verdict\":\"${STUB_VERDICT_FIELD}\""
  fi
  printf '{"id":%s,"git_ref":"%s","status":"%s","model":"%s","requested_model":"%s","prompt":"%s"%s}' \
    "${STUB_PAYLOAD_JOB:-${STUB_JOB:-4600}}" \
    "$git_ref" \
    "${STUB_STATUS:-done}" \
    "${STUB_MODEL:-gpt-5.6-sol}" \
    "${STUB_REQUESTED_MODEL:-gpt-5.6-sol}" \
    "${STUB_PROMPT:-}" \
    "$usage$extra"
}

# record_read_blank: true while the first STUB_RECORD_BLANK_FOR record reads should
# come back empty, replaying the real daemon's asynchronous record write.
record_read_blank() {
  local want="${STUB_RECORD_BLANK_FOR:-0}" seen=0 counter="$STUB_INVOKED.reads"
  [ "$want" -gt 0 ] || return 1
  [ ! -f "$counter" ] || seen=$(cat "$counter")
  seen=$((seen + 1))
  printf '%s' "$seen" >"$counter"
  [ "$seen" -le "$want" ]
}

case "$cmd" in
  review)
    printf 'review %s\n' "$*" >>"$STUB_INVOKED"
    if [ -n "${STUB_ANNOUNCE_SHA:-}" ]; then
      printf 'Enqueued job %s for %s\n' "${STUB_JOB:-4600}" "$STUB_ANNOUNCE_SHA"
    fi
    printf '%s\n' "${STUB_VERDICT:-No issues found}"
    exit "${STUB_REVIEW_RC:-0}"
    ;;
  show)
    case " $* " in
      *" --prompt "*) printf '%s\n' "${STUB_PROMPT:-}"; exit 0 ;;
    esac
    record_read_blank && { printf 'null\n'; exit 0; }
    [ "${STUB_SHOW_JSON:-object}" != none ] || { printf 'null\n'; exit 0; }
    emit_job_object; printf '\n'
    exit 0
    ;;
  list)
    record_read_blank && { printf 'null\n'; exit 0; }
    printf '['; emit_job_object; printf ']\n'
    exit 0
    ;;
  *) printf 'stub: unsupported roborev subcommand: %s\n' "$cmd" >&2; exit 64 ;;
esac
STUB
chmod +x "$stubbin/roborev"

# The structured-payload cases need python3 (the wrapper decodes the doubly-encoded
# token_usage with it). Loud SKIP, never a silent pass, matching the gate's other
# SKIP-aware guards.
HAVE_PYTHON3=1
if ! command -v python3 >/dev/null 2>&1; then
  HAVE_PYTHON3=0
  printf 'SKIP - no python3: the structured-payload cases (tokens, prompt-content, git_ref, model) cannot run\n'
fi

# ---------------------------------------------------------------------------
# Fixture: a work repo with a local bare origin. `git push` updates the
# remote-tracking ref, which is exactly what the wrapper's push assert reads.
# ---------------------------------------------------------------------------
git_q() { git -C "$1" -c user.email=t@example.invalid -c user.name=Tester -c commit.gpgsign=false "${@:2}"; }

# Modes:
#   pushed          wide refspec, feature pushed (mirror ref present -> fast path)
#   unpushed        wide refspec, feature never pushed
#   empty           wide refspec, feature == main, pushed
#   docs-only       wide refspec, markdown-only change, pushed (code-free census)
#   mixed           one markdown + one .rs file (NOT code-free)
#   two-code-commits  two commits, each touching a DIFFERENT .rs file
#   workflow-yaml   only .github/workflows/ci.yml (a .yml extension is CODE, so this
#                   must NOT be classified code-free)
#   narrow          NARROW refspec (+refs/heads/main:refs/remotes/origin/main) —
#                   feature IS pushed but no refs/remotes/origin/feature ever
#                   exists. This is THE fleet's real configuration, under which a
#                   mirror-ref push assert false-FAILs unconditionally.
#   narrow-behind   narrow refspec, pushed, then one extra LOCAL commit
#   narrow-upstream narrow refspec on a remote named `upstream` with
#                   branch.feature.remote=upstream (hardcoding `origin` breaks)
#   unreachable     pushed, mirror ref removed, remote URL repointed at a missing
#                   path so `git ls-remote` FAILS (infra/auth condition)
#   no-base         pushed, then refs/remotes/origin/main deleted so the default
#                   --base origin/main cannot resolve
#   deleted-remote  pushed (mirror ref present and EQUAL to HEAD), then the branch
#                   DELETED from the bare origin — a stale proxy that would still
#                   satisfy a mirror-ref fast path
make_fixture() { # make_fixture <name> <mode> -> prints work dir
  # NOTE: separate statements on purpose — `local` is a builtin, so ALL of its
  # arguments are expanded before any assignment takes effect; `local a=$1 b=$a`
  # would read an unset `a` and abort under `set -u`.
  local name mode root work remote narrow
  name="$1"
  mode="$2"
  root="$tmp/$name"
  work="$root/work"
  remote=origin
  narrow=0
  case "$mode" in
    narrow|narrow-behind) narrow=1 ;;
    narrow-upstream) narrow=1; remote=upstream ;;
  esac

  mkdir -p "$root"
  git init -q --bare "$root/origin.git"
  git init -q -b main "$work"
  printf 'base\n' >"$work/README.md"
  printf 'fn main() {}\n' >"$work/main.rs"
  git_q "$work" add README.md main.rs
  git_q "$work" commit -q -m base
  git_q "$work" remote add "$remote" "$root/origin.git"
  if [ "$narrow" -eq 1 ]; then
    git_q "$work" config "remote.$remote.fetch" "+refs/heads/main:refs/remotes/$remote/main"
  fi
  git_q "$work" push -q "$remote" main

  git_q "$work" checkout -q -b feature main
  case "$mode" in
    empty) : ;;
    docs-only)
      printf 'doc line\ndoc line 2\n' >>"$work/README.md"
      printf '# spec\n' >"$work/NOTES.md"
      git_q "$work" add README.md NOTES.md
      git_q "$work" commit -q -m 'docs only'
      ;;
    two-code-commits)
      # Two commits touching DIFFERENT code files: a single-commit review would cover
      # only the second, which is the partial-review vacuity class.
      printf 'fn alpha() {}\n' >"$work/alpha.rs"
      git_q "$work" add alpha.rs
      git_q "$work" commit -q -m 'first code commit'
      printf 'fn beta() {}\n' >"$work/beta.rs"
      git_q "$work" add beta.rs
      git_q "$work" commit -q -m 'second code commit'
      ;;
    mixed)
      printf 'doc line\n' >>"$work/README.md"
      printf 'fn helper() {}\n' >>"$work/main.rs"
      git_q "$work" add README.md main.rs
      git_q "$work" commit -q -m 'docs plus code'
      ;;
    workflow-yaml)
      mkdir -p "$work/.github/workflows"
      printf 'name: ci\non: push\n' >"$work/.github/workflows/ci.yml"
      git_q "$work" add .github/workflows/ci.yml
      git_q "$work" commit -q -m 'workflow only'
      ;;
    *)
      printf 'fn helper() {}\n' >>"$work/main.rs"
      git_q "$work" add main.rs
      git_q "$work" commit -q -m 'code change'
      ;;
  esac
  if [ "$mode" != unpushed ]; then
    git_q "$work" push -q "$remote" feature
  fi

  case "$mode" in
    narrow-upstream)
      git_q "$work" config branch.feature.remote "$remote"
      ;;
    narrow-behind)
      printf 'fn later() {}\n' >>"$work/main.rs"
      git_q "$work" add main.rs
      git_q "$work" commit -q -m 'unpushed narrow follow-up'
      ;;
    unreachable)
      git_q "$work" update-ref -d "refs/remotes/origin/feature" 2>/dev/null || true
      git_q "$work" remote set-url origin "$root/absent-remote.git"
      ;;
    no-base)
      git_q "$work" update-ref -d refs/remotes/origin/main
      ;;
    detached)
      git_q "$work" checkout -q --detach HEAD
      ;;
    orphan-base)
      # An unrelated root commit: `git diff <orphan>...HEAD` has NO merge base and
      # therefore FAILS, which must never render as "census: 0 files".
      git_q "$work" checkout -q --orphan unrelated
      git_q "$work" rm -q -rf . >/dev/null 2>&1 || true
      printf 'unrelated\n' >"$work/UNRELATED.txt"
      git_q "$work" add UNRELATED.txt
      git_q "$work" commit -q -m 'unrelated root'
      git_q "$work" checkout -q feature
      ;;
    deleted-remote)
      # The mirror ref stays, EQUAL to HEAD, while the branch is deleted from the
      # remote: a cached proxy that still "proves" a push that no longer exists.
      git -C "$root/origin.git" update-ref -d refs/heads/feature
      ;;
  esac
  printf '%s' "$work"
}

# Fixture-integrity guard: a narrow-refspec fixture that accidentally grew a
# feature mirror ref would silently stop testing the condition it exists for.
assert_no_mirror_ref() { # assert_no_mirror_ref <label> <work> <remote>
  if git -C "$2" rev-parse --verify --quiet "refs/remotes/$3/feature" >/dev/null; then
    bad "$1: fixture is not narrow — refs/remotes/$3/feature exists"
  else
    ok "$1: no refs/remotes/$3/feature mirror ref (narrow refspec reproduced)"
  fi
}

# range_ref <work>: the git_ref shape a RANGE review records, "<base40>..<head40>".
range_ref() { printf '%s..%s' "$(git -C "$1" rev-parse "${2:-origin/main}")" "$(git -C "$1" rev-parse HEAD)"; }

CASE_N=0
OUT=""
RC=0
INVOKED=""

run_wrapper() { # run_wrapper <work-dir> [extra wrapper args...]
  local work="$1"; shift
  # Fail loudly on a broken fixture rather than letting the wrapper fall back to
  # $PWD (which would silently run every assert against the REAL repo).
  if [ -z "$work" ] || [ ! -d "$work/.git" ]; then
    bad "fixture setup failed: '$work' is not a git work tree"
    OUT="$tmp/empty-out.txt"; : >"$OUT"; INVOKED="$tmp/empty-invoked.txt"; : >"$INVOKED"; RC=99
    return 0
  fi
  CASE_N=$((CASE_N + 1))
  OUT="$tmp/out-$CASE_N.txt"
  INVOKED="$tmp/invoked-$CASE_N.txt"
  : >"$INVOKED"
  # The sanctioned invocation reviews the RANGE <base>..HEAD, so the job record's
  # git_ref is "<base40>..<head40>". Default the stub to the correct range unless the
  # case pinned git_ref itself (or asked for it to be absent with `none`).
  if [ -z "${STUB_GIT_REF:-}" ]; then STUB_GIT_REF=$(range_ref "$work"); fi
  STUB_INVOKED="$INVOKED" PATH="$stubbin:$PATH" \
    bash "$WRAPPER" --repo "$work" --agent codex --model gpt-5.6-sol \
    --log "$tmp/transcript-$CASE_N.txt" "$@" >"$OUT" 2>&1
  RC=$?
}

assert_verdict() { # assert_verdict <label> <expected RESULT> <expected rc>
  local label="$1" want="$2" want_rc="$3" got
  got=$(grep -E '^RESULT: ' "$OUT" | tail -1 || printf '<none>')
  if [ "$got" = "RESULT: $want" ] && [ "$RC" -eq "$want_rc" ]; then
    ok "$label: $got (exit $RC)"
  else
    bad "$label: expected 'RESULT: $want' + exit $want_rc, got '$got' + exit $RC"
    printf -- '------- captured -------\n'; cat "$OUT"; printf -- '------------------------\n'
  fi
}

assert_says() { # assert_says <label> <extended-regex>
  if grep -qE -- "$2" "$OUT"; then
    ok "$1"
  else
    bad "$1: pattern '$2' not found in output"
    printf -- '------- captured -------\n'; cat "$OUT"; printf -- '------------------------\n'
  fi
}

assert_lacks() { # assert_lacks <label> <extended-regex>
  if grep -qE -- "$2" "$OUT"; then
    bad "$1: output unexpectedly matched '$2'"
  else
    ok "$1"
  fi
}

assert_one_block() { # assert_one_block <label>
  local n
  n=$(grep -cF "$BLOCK_HEADER" "$OUT" || true)
  if [ "$n" -eq 1 ]; then ok "$1: exactly one summary block"; else bad "$1: $n summary blocks (want 1)"; fi
}

assert_never_enqueued() { # assert_never_enqueued <label>
  if [ -s "$INVOKED" ]; then
    bad "$1: a review WAS enqueued (stub invoked: $(cat "$INVOKED"))"
  else
    ok "$1: no review was enqueued"
  fi
}

# Recorded token_usage payloads, PRE-ESCAPED for embedding as a JSON *string* value
# (the real payload double-encodes it). The small-genuine and vacuous numbers are the
# measured ones from issue #2964; the boundary pair pins the input floor exactly.
TOKENS_GENUINE_LARGE='{\"input_tokens\":505625,\"cached_input_tokens\":387328,\"total_output_tokens\":6332,\"usage_source\":\"job_log_turn_completed\"}'
TOKENS_GENUINE_SMALL='{\"input_tokens\":67387,\"cached_input_tokens\":43520,\"total_output_tokens\":2232,\"usage_source\":\"job_log_turn_completed\",\"thread_id\":\"tid\",\"event_offset\":2366}'
TOKENS_VACUOUS='{\"input_tokens\":18700,\"cached_input_tokens\":0,\"total_output_tokens\":53,\"usage_source\":\"job_log_turn_completed\"}'
TOKENS_CLEAN_LOW_OUTPUT='{\"input_tokens\":67387,\"cached_input_tokens\":43520,\"total_output_tokens\":40,\"usage_source\":\"job_log_turn_completed\"}'
TOKENS_INPUT_BELOW_FLOOR='{\"input_tokens\":24999,\"cached_input_tokens\":9000,\"total_output_tokens\":2000,\"usage_source\":\"job_log_turn_completed\"}'
TOKENS_INPUT_AT_FLOOR='{\"input_tokens\":25000,\"cached_input_tokens\":9000,\"total_output_tokens\":2000,\"usage_source\":\"job_log_turn_completed\"}'
TOKENS_OLD_FIELD_NAMES='{\"input_tokens\":505625,\"cached_input_tokens\":387328,\"output_tokens\":6332}'

# A prompt that mentions every path any fixture touches, and one that mentions none.
PROMPT_WITH_PATHS='Review the following change. diff --git a/main.rs b/main.rs @@ fn helper() {} @@ diff --git a/README.md b/README.md diff --git a/NOTES.md b/NOTES.md'
PROMPT_WITHOUT_PATHS='Please review the change on this branch. (no diff was attached to this prompt)'

export STUB_JOB=4656
export STUB_VERDICT=$'No issues found.\nSummary: reviewed the diff; no issues found.'
export STUB_TOKEN_USAGE="$TOKENS_GENUINE_LARGE"
export STUB_PROMPT="$PROMPT_WITH_PATHS"
export STUB_STATUS="done"
export STUB_GIT_REF=''
export STUB_MODEL=gpt-5.6-sol
export STUB_REQUESTED_MODEL=gpt-5.6-sol
export STUB_SHOW_JSON=object
export STUB_HAS_TOKEN_DATA=''
export STUB_VERDICT_FIELD=''
export STUB_RECORD_BLANK_FOR=0
export STUB_PAYLOAD_JOB=''
export STUB_REVIEW_RC=0
export STUB_ANNOUNCE_SHA=''

reset_stub() {
  STUB_JOB=4656
  STUB_VERDICT=$'No issues found.\nSummary: reviewed the diff; no issues found.'
  STUB_TOKEN_USAGE="$TOKENS_GENUINE_LARGE"
  STUB_PROMPT="$PROMPT_WITH_PATHS"
  STUB_REVIEW_RC=0
  STUB_ANNOUNCE_SHA=''
  STUB_STATUS="done"
  STUB_GIT_REF=''
  STUB_MODEL=gpt-5.6-sol
  STUB_REQUESTED_MODEL=gpt-5.6-sol
  STUB_SHOW_JSON=object
  STUB_HAS_TOKEN_DATA=''
  STUB_VERDICT_FIELD=''
  STUB_RECORD_BLANK_FOR=0
  STUB_PAYLOAD_JOB=''
}

printf '== case (a): enqueued sha == base ref ==\n'
reset_stub
work=$(make_fixture case_a pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
STUB_GIT_REF=$(git -C "$work" rev-parse origin/main)
run_wrapper "$work"
assert_verdict 'case (a)' FAIL 1
assert_says 'case (a) names the base ref' "EQUALS the base ref 'origin/main'"
assert_says 'case (a) sha-assert FAIL' '^sha-assert: FAIL'
assert_one_block 'case (a)'

printf '== case (b): enqueued sha is neither endpoint ==\n'
reset_stub
work=$(make_fixture case_b pushed)
STUB_ANNOUNCE_SHA='0000000000000000000000000000000000000abc'
STUB_GIT_REF='0000000000000000000000000000000000000abc'
run_wrapper "$work"
assert_verdict 'case (b)' FAIL 1
assert_says 'case (b) names neither-endpoint' 'matches NEITHER endpoint'
assert_says 'case (b) prints the reviewed sha beside expected head' "git_ref '0000000000000000000000000000000000000abc' does not equal branch HEAD"

printf '== case (c): findings NONE + a vacuity claim on a CODE census = HARD FAIL ==\n'
reset_stub
# Tier 1 is AUTHORITATIVE again (round 4) but GATED on `findings:`. With findings NONE
# the reviewer is CLAIMING CLEANLINESS, so a "no code changes" summary against a
# census we measured as non-empty is trigger T3 and must block the merge.
work=$(make_fixture case_c pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_VERDICT=$'No issues found.\nSummary: the diff contains no code changes to review.'
run_wrapper "$work"
assert_verdict 'case (c)' FAIL 1
assert_says 'case (c) tier1 FAILs authoritatively' '^vacuity-tier1: FAIL \(vacuous verdict vs non-empty census\)$'
assert_says 'case (c) names the claiming-cleanliness gate' 'reported NO findings \(findings: NONE\)'
assert_says 'case (c) prints the census' '^census: [0-9]+ files?, \+[0-9]+/-[0-9]+$'
assert_says 'case (c) sha-assert still PASS' '^sha-assert: PASS$'
assert_says 'case (c) findings are NONE' '^findings: NONE$'

printf '== case (c1c): findings UNKNOWN + the phrase FAILs (fail-closed on unknown) ==\n'
reset_stub
# The findings state is UNKNOWN when the reviewer errored. An unparseable/unknowable
# findings state must never DISARM tier 1, so UNKNOWN is treated as claiming cleanliness.
work=$(make_fixture case_c1c pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_VERDICT=$'No issues found.\nSummary: the diff contains no code changes to review.'
STUB_STATUS=failed
STUB_REVIEW_RC=3
run_wrapper "$work"
STUB_REVIEW_RC=0
STUB_STATUS="done"
assert_verdict 'case (c1c)' FAIL 1
assert_says 'case (c1c) findings are UNKNOWN' '^findings: UNKNOWN$'
assert_says 'case (c1c) tier1 still FAILs' '^vacuity-tier1: FAIL \(vacuous verdict vs non-empty census\)$'
assert_says 'case (c1c) says fail-closed is the correct direction' 'treated as claiming cleanliness'

printf '== case (c1b): the tier-1 match is anchored to the verdict/summary region ==\n'
reset_stub
# A genuine findings-bearing review that QUOTES the phrase in a finding body must not
# be flagged: the match only looks at the Summary line.
work=$(make_fixture case_c1b pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_VERDICT=$'## Findings\n[Medium] the tier-1 regex matches the literal phrase no code changes anywhere in the transcript\n[Low] naming nit\n## Summary: 2 findings; the guard says no code changes too broadly.'
STUB_REVIEW_RC=1
run_wrapper "$work"
STUB_REVIEW_RC=0
assert_says 'case (c1b) tier1 does not FAIL a findings-bearing review' '^vacuity-tier1: NOTICE \(phrase present in a findings-bearing review\)$'
assert_says 'case (c1b) the gate names the findings evidence' 'the review reported findings'
assert_lacks 'case (c1b) tier1 never FAILs here' '^vacuity-tier1: FAIL'
assert_says 'case (c1b) the run fails only for the findings' '^roborev-exit: FINDINGS \(exit 1\)$'


printf '== case (c1d): a CLEAN review quoting the phrase OUTSIDE its summary PASSes ==\n'
reset_stub
# This is what the verdict/summary ANCHORING buys, and the findings gate cannot cover
# it: findings NONE (so the gate would fail it) yet the phrase appears only in a body
# note, not in the review's conclusion. Unanchored matching fails this run; anchored
# matching correctly passes it.
work=$(make_fixture case_c1d pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_VERDICT=$'No issues found.\nNote: the guard under review matches the literal phrase no code changes.\nSummary: reviewed 1 file; nothing to report.'
run_wrapper "$work"
assert_verdict 'case (c1d)' PASS 0
assert_says 'case (c1d) tier1 PASSes on an out-of-region mention' '^vacuity-tier1: PASS$'
assert_says 'case (c1d) findings are NONE' '^findings: NONE$'
assert_lacks 'case (c1d) tier1 does not FAIL' '^vacuity-tier1: FAIL'

printf '== case (c2): a code-free (docs-only) census FAILs deterministically ==\n'
reset_stub
# roborev structurally DISCARDS a code-free diff, so such a diff cannot be certified
# at all. That is a property of OUR census, so it must not depend on the reviewer
# admitting it: the previous revision computed the classification and used it only for
# wording, and a docs-only census reached RESULT: PASS.
work=$(make_fixture case_c2 docs-only)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (c2)' FAIL 1
assert_says 'case (c2) code-free is its own FAIL key' '^code-free: FAIL \(code-free census: 2/2 files are documentation/specification text\)$'
assert_says 'case (c2) names the structural discard' 'roborev STRUCTURALLY DISCARDS a code-free diff'
assert_says 'case (c2) points at primary-source verification' 'primary-source verification recorded in the PR'
assert_never_enqueued 'case (c2)'
assert_says 'case (c2) later checks are SKIPped, not passed' '^review-completed: SKIP$'

printf '== case (c2b): a code-free census FAILs even with a clean verdict and healthy tokens ==\n'
reset_stub
work=$(make_fixture case_c2b docs-only)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_TOKEN_USAGE="$TOKENS_GENUINE_LARGE"
run_wrapper "$work"
assert_verdict 'case (c2b)' FAIL 1
assert_lacks 'case (c2b) never reports a pass' '^RESULT: PASS$'

printf '== case (c2c): a mixed census is NOT code-free ==\n'
reset_stub
work=$(make_fixture case_c2c mixed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (c2c)' PASS 0
assert_says 'case (c2c) code-free PASSes when any file is code' '^code-free: PASS$'

printf '== case (c2d): a workflow .yml census is CODE, not documentation ==\n'
reset_stub
# The classification is EXTENSION-based: an earlier revision treated everything under
# .github/ (and docs/) as non-code, which would make this a FALSE code-free FAIL now
# that code-free fails the run.
work=$(make_fixture case_c2d workflow-yaml)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff: diff --git a/.github/workflows/ci.yml b/.github/workflows/ci.yml'
run_wrapper "$work"
assert_verdict 'case (c2d)' PASS 0
assert_says 'case (c2d) a .yml file is not classified documentation' '^code-free: PASS$'

printf '== case (d): vacuous token signature vs non-empty census ==\n'
reset_stub
work=$(make_fixture case_d pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_VERDICT=$'No issues found.\nSummary: reviewed the diff; no issues found.'
STUB_TOKEN_USAGE="$TOKENS_VACUOUS"
run_wrapper "$work"
assert_verdict 'case (d)' FAIL 1
assert_says 'case (d) tier2 FAIL' '^vacuity-tier2: FAIL'
assert_says 'case (d) tier1 unaffected' '^vacuity-tier1: PASS$'
assert_says 'case (d) code-free PASSes on a code census' '^code-free: PASS$'
assert_says 'case (d) prints observed input vs named constant' 'observed input=18700 < ROBOREV_VACUITY_MIN_INPUT_TOKENS=25000'
assert_says 'case (d) prints observed cached vs the zero clause' 'observed cached=0 == 0'
assert_says 'case (d) output floor is ADVISORY, not a failure condition' 'advisory \(NOT a failure condition\): observed output=53'
assert_says 'case (d) tokens line carries the triple' '^tokens: input=18700 cached=0 output=53$'

printf '== case (e): unpushed branch ==\n'
reset_stub
work=$(make_fixture case_e unpushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_TOKEN_USAGE="$TOKENS_GENUINE_LARGE"
run_wrapper "$work"
assert_verdict 'case (e)' FAIL 1
assert_says 'case (e) push-assert FAIL' '^push-assert: FAIL'
assert_says 'case (e) names the missing remote branch authoritatively' "the remote 'origin' has no branch 'feature' \(authoritative: git ls-remote\)"
assert_never_enqueued 'case (e)'

printf '== case (e2): remote behind HEAD names the unpushed commits ==\n'
reset_stub
work=$(make_fixture case_e2 pushed)
printf 'fn later() {}\n' >>"$work/main.rs"
git_q "$work" add main.rs
git_q "$work" commit -q -m 'unpushed follow-up'
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (e2)' FAIL 1
assert_says 'case (e2) lists the unpushed commit' 'unpushed follow-up'
assert_never_enqueued 'case (e2)'

printf '== case (k): NARROW fetch refspec — pushed, but no feature mirror ref ==\n'
reset_stub
# THE regression case (#2964 follow-up blocker): CQLite clones set
# remote.origin.fetch = +refs/heads/main:refs/remotes/origin/main, so
# refs/remotes/origin/<feature> is NEVER created however often the branch is
# pushed. A mirror-ref push assert false-FAILs here 100% of the time, which would
# make the only sanctioned roborev invocation unusable fleet-wide and push agents
# back to the bare --branch form. The remote (git ls-remote) is the authority.
work=$(make_fixture case_k narrow)
assert_no_mirror_ref 'case (k)' "$work" origin
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_VERDICT=$'No issues found.\nSummary: reviewed the diff; no issues found.'
STUB_TOKEN_USAGE="$TOKENS_GENUINE_LARGE"
run_wrapper "$work"
assert_verdict 'case (k)' PASS 0
assert_says 'case (k) push-assert PASS despite the absent mirror ref' '^push-assert: PASS$'
assert_says 'case (k) the run proceeded to a real review' '^sha-assert: PASS$'

printf '== case (k2): narrow refspec, pushed but behind HEAD ==\n'
reset_stub
work=$(make_fixture case_k2 narrow-behind)
assert_no_mirror_ref 'case (k2)' "$work" origin
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (k2)' FAIL 1
assert_says 'case (k2) push-assert FAIL (unpushed commits)' '^push-assert: FAIL \(unpushed commits\)$'
assert_says 'case (k2) names the unpushed commit' 'unpushed narrow follow-up'
assert_says 'case (k2) cites ls-remote as the authority' 'authoritative: git ls-remote'
assert_never_enqueued 'case (k2)'

printf '== case (k3): the remote is derived from the branch upstream, not hardcoded ==\n'
reset_stub
work=$(make_fixture case_k3 narrow-upstream)
assert_no_mirror_ref 'case (k3)' "$work" upstream
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_GIT_REF=$(range_ref "$work" upstream/main)
run_wrapper "$work" --base upstream/main
assert_verdict 'case (k3)' PASS 0
assert_says 'case (k3) push-assert PASS against the configured upstream' '^push-assert: PASS$'

printf '== case (k4): ls-remote failure is an infra/auth FAIL, not "never pushed" ==\n'
reset_stub
work=$(make_fixture case_k4 unreachable)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (k4)' FAIL 1
assert_says 'case (k4) push-assert FAIL names the ls-remote failure' '^push-assert: FAIL \(ls-remote failed: infra/auth\)$'
assert_says 'case (k4) attributes it to infra/auth' 'INFRA/AUTH condition, NOT evidence that the branch is unpushed'
assert_says 'case (k4) points at the separate git credential path' 'gh auth setup-git'
assert_lacks 'case (k4) does NOT claim the branch was never pushed' 'has never been pushed'
assert_never_enqueued 'case (k4)'

printf '== case (k5): an unresolvable --base FAILs closed, never NOTHING-TO-REVIEW ==\n'
reset_stub
work=$(make_fixture case_k5 no-base)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (k5)' FAIL 1
assert_says 'case (k5) census-check FAIL names the unresolvable base' "^census-check: FAIL \(base 'origin/main' unresolvable\)$"
assert_says 'case (k5) push-assert still PASS' '^push-assert: PASS$'
assert_says 'case (k5) says it is not NOTHING-TO-REVIEW' "explicitly NOT a NOTHING-TO-REVIEW"
assert_lacks 'case (k5) is not NOTHING-TO-REVIEW' '^RESULT: NOTHING-TO-REVIEW$'
assert_never_enqueued 'case (k5)'

printf '== case (r): mirror ref equals HEAD but the remote branch was DELETED ==\n'
reset_stub
# The reviewer flagged the mirror-ref fast path as a Medium, correctly: a CACHED
# origin/<branch> survives a force-push or a deletion, so it can equal HEAD while
# the remote no longer has the commit — enqueueing a review for a commit the
# reviewer cannot fetch, i.e. a vacuous-review setup. There is no fast path now.
work=$(make_fixture case_r deleted-remote)
if [ "$(git -C "$work" rev-parse --verify --quiet refs/remotes/origin/feature)" = "$(git -C "$work" rev-parse HEAD)" ]; then
  ok 'case (r): fixture has a stale mirror ref equal to HEAD'
else
  bad 'case (r): fixture did not retain a mirror ref equal to HEAD'
fi
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (r)' FAIL 1
assert_says 'case (r) push-assert FAIL despite the stale mirror ref' '^push-assert: FAIL \(branch absent on remote origin\)$'
assert_says 'case (r) cites ls-remote as the authority' 'authoritative: git ls-remote'
assert_never_enqueued 'case (r)'

if [ "$HAVE_PYTHON3" -eq 1 ]; then
printf '== case (l): tier 2 EVALUATES against the real token_usage payload shape ==\n'
reset_stub
# The regression that made tier 2 dead code: token_usage is a JSON-ENCODED STRING
# (decode twice) whose output field is total_output_tokens. These are the EXACT
# numbers measured on the real job (67387 / 43520 / 2232).
work=$(make_fixture case_l pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_TOKEN_USAGE="$TOKENS_GENUINE_SMALL"
run_wrapper "$work"
assert_verdict 'case (l)' PASS 0
assert_says 'case (l) the doubly-encoded token_usage was decoded' '^tokens: input=67387 cached=43520 output=2232$'
assert_says 'case (l) tier 2 actually evaluated' '^vacuity-tier2: PASS$'
assert_lacks 'case (l) tier 2 is NOT permanently unavailable' '^vacuity-tier2: UNAVAILABLE$'

printf '== case (l2): the legacy output_tokens field name is still accepted ==\n'
reset_stub
work=$(make_fixture case_l2 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_TOKEN_USAGE="$TOKENS_OLD_FIELD_NAMES"
run_wrapper "$work"
assert_verdict 'case (l2)' PASS 0
assert_says 'case (l2) output_tokens read as a fallback' '^tokens: input=505625 cached=387328 output=6332$'

printf '== case (l3): the input floor is pinned at exactly 25000 ==\n'
reset_stub
work=$(make_fixture case_l3 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_TOKEN_USAGE="$TOKENS_INPUT_BELOW_FLOOR"
run_wrapper "$work"
assert_verdict 'case (l3) below the floor' FAIL 1
assert_says 'case (l3) names the floor and the observation' 'observed input=24999 < ROBOREV_VACUITY_MIN_INPUT_TOKENS=25000'
reset_stub
work=$(make_fixture case_l3b pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_TOKEN_USAGE="$TOKENS_INPUT_AT_FLOOR"
run_wrapper "$work"
assert_verdict 'case (l3) at the floor' PASS 0

printf '== case (l4): a low output count NEVER fails a genuine clean review ==\n'
reset_stub
# A genuine CLEAN review and a vacuous one emit near-identical output counts, so an
# output floor would false-FAIL exactly the case we care most about.
work=$(make_fixture case_l4 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_TOKEN_USAGE="$TOKENS_CLEAN_LOW_OUTPUT"
run_wrapper "$work"
assert_verdict 'case (l4)' PASS 0
assert_says 'case (l4) tier 2 still PASSes' '^vacuity-tier2: PASS$'
assert_says 'case (l4) the low output is reported as advisory only' 'advisory \(NOT a failure condition\): observed output=40'

printf '== case (m): show --json unavailable falls back to list --json ==\n'
reset_stub
work=$(make_fixture case_m pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_TOKEN_USAGE="$TOKENS_GENUINE_SMALL"
STUB_SHOW_JSON=none
run_wrapper "$work"
assert_verdict 'case (m)' PASS 0
assert_says 'case (m) accounting came from the list fallback' '^tokens: input=67387 cached=43520 output=2232$'

printf '== case (n): a prompt without the census paths is a HARD FAIL ==\n'
reset_stub
# The deterministic complement of tier 1: tier 1 catches "the reviewer got the diff
# and discarded it"; this catches "the reviewer never got the diff at all".
work=$(make_fixture case_n pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT="$PROMPT_WITHOUT_PATHS"
run_wrapper "$work"
assert_verdict 'case (n)' FAIL 1
assert_says 'case (n) prompt-content FAIL counts the absent paths' '^prompt-content: FAIL \(1/1 code census paths absent from the prompt\)$'
assert_says 'case (n) names the missing path' '^  main\.rs$'
assert_says 'case (n) says the reviewer never received the diffs' 'the reviewer never received their diffs'
assert_says 'case (n) every other check passed' '^vacuity-tier1: PASS$'
assert_says 'case (n) review-completed still PASS' '^review-completed: PASS$'

printf '== case (n2): an unretrievable prompt degrades visibly, never a silent skip ==\n'
reset_stub
work=$(make_fixture case_n2 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT=''
run_wrapper "$work"
assert_verdict 'case (n2)' PASS 0
assert_says 'case (n2) prompt-content UNAVAILABLE' '^prompt-content: UNAVAILABLE$'
assert_says 'case (n2) degraded-signal wording' 'DEGRADED SIGNAL, never a silent skip'

printf '== case (n3): prompt-content PASS reports the coverage it checked ==\n'
reset_stub
work=$(make_fixture case_n3 mixed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_says 'case (n3) prompt-content counts only the CODE subset' '^prompt-content: PASS \(1/1 code census paths present\)$'

printf '== case (o): the structured git_ref is the sha oracle, stdout is a cross-check ==\n'
reset_stub
# stdout announces the right sha while the job record says the base was reviewed:
# the structured field must win, and the disagreement must be recorded.
work=$(make_fixture case_o pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
# stdout announced the range BASE (normal for a range review, and it verifies nothing
# about HEAD), while the record says the reviewed range STOPPED at the base. Only the
# structured range can catch that the branch tip was never reviewed.
STUB_GIT_REF="$(git -C "$work" rev-parse origin/main)..$(git -C "$work" rev-parse origin/main)"
run_wrapper "$work"
assert_verdict 'case (o)' FAIL 1
assert_says 'case (o) sha-assert FAIL from the structured range' '^sha-assert: FAIL \(reviewed range does not match origin/main\.\.\.HEAD\)$'
assert_says 'case (o) names the short-of-tip range head' 'the reviewed scope stops short of the branch tip'
assert_says 'case (o) reviewed-sha reports the structured range' "^reviewed-sha: $(git -C "$work" rev-parse origin/main)\.\.$(git -C "$work" rev-parse origin/main)\$"

printf '== case (p): a model substitution is surfaced loudly ==\n'
reset_stub
work=$(make_fixture case_p pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_MODEL=gpt-5.6-mini
STUB_REQUESTED_MODEL=gpt-5.6-sol
run_wrapper "$work"
assert_verdict 'case (p)' PASS 0
assert_says 'case (p) the model line marks the substitution' "^model: gpt-5\.6-mini \(SUBSTITUTED — requested 'gpt-5\.6-sol'\)\$"
assert_says 'case (p) a loud NOTICE explains it' 'a MODEL SUBSTITUTION'

printf '== case (p2): a matching model is reported plainly ==\n'
reset_stub
work=$(make_fixture case_p2 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (p2)' PASS 0
assert_says 'case (p2) model line is plain' '^model: gpt-5\.6-sol$'
assert_lacks 'case (p2) no substitution notice' 'SUBSTITUTED'
fi  # HAVE_PYTHON3

printf '== case (f): genuine review with matching sha + healthy accounting ==\n'
reset_stub
work=$(make_fixture case_f pushed)
head_sha=$(git -C "$work" rev-parse HEAD)
STUB_ANNOUNCE_SHA="$head_sha"
STUB_VERDICT=$'No issues found.\nSummary: reviewed the diff; no issues found.'
STUB_TOKEN_USAGE="$TOKENS_GENUINE_LARGE"
run_wrapper "$work"
assert_verdict 'case (f)' PASS 0
for line in 'push-assert: PASS' 'census-check: PASS' 'sha-assert: PASS' 'vacuity-tier1: PASS' 'vacuity-tier2: PASS'; do
  assert_says "case (f) $line" "^$line\$"
done
assert_says 'case (f) reviewed-sha reports the RANGE' "^reviewed-sha: $(git -C "$work" rev-parse origin/main)\.\.$head_sha\$"
assert_says 'case (f) job printed' '^job: 4656$'
assert_says 'case (f) log path named' '^log: .+transcript-'
assert_one_block 'case (f)'
# The sanctioned invocation form: explicit HEAD sha, explicit ABSOLUTE --repo,
# --wait, both --agent and --model — and never --branch, never two positionals.
if grep -qF -- "review --branch --base origin/main --repo $work --agent codex --model gpt-5.6-sol --wait" "$INVOKED"; then
  ok 'case (f): invoked over the census RANGE with an explicit absolute --repo + --wait'
else
  bad "case (f): unexpected invocation form: $(cat "$INVOKED")"
fi
# `--branch` is correct ONLY with an explicit --repo (without it, it resolves against
# the ROOT checkout); the two-positional range form must never appear.
if grep -qF -- '--branch' "$INVOKED" && grep -qF -- "--repo $work" "$INVOKED"; then
  ok 'case (f): --branch is paired with an explicit absolute --repo'
else
  bad "case (f): --branch/--repo pairing missing: $(cat "$INVOKED")"
fi
if grep -qE -- 'review [0-9a-f]{7,40} [0-9a-f]{7,40}' "$INVOKED"; then
  bad 'case (f): the two-positional commit-range form was used'
else
  ok 'case (f): no two-positional commit-range form'
fi

printf '== case (g): genuinely empty census ==\n'
reset_stub
work=$(make_fixture case_g empty)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (g)' NOTHING-TO-REVIEW 3
assert_says 'case (g) census is zero files' '^census: 0 files, \+0/-0$'
assert_says 'case (g) says it is not a pass' 'NOT a pass'
assert_lacks 'case (g) is not a PASS' '^RESULT: PASS$'
assert_never_enqueued 'case (g)'

printf '== case (h): missing / unparseable enqueue announcement ==\n'
reset_stub
work=$(make_fixture case_h pushed)
STUB_ANNOUNCE_SHA=''
STUB_VERDICT='Review complete. (no enqueue line printed)'
run_wrapper "$work"
assert_verdict 'case (h)' FAIL 1
assert_says 'case (h) sha-assert FAIL (unverifiable)' '^sha-assert: FAIL \(no parseable enqueue announcement\)$'
assert_says 'case (h) reviewed-sha unknown' '^reviewed-sha: -$'

printf '== case (i): unavailable token accounting degrades visibly ==\n'
reset_stub
work=$(make_fixture case_i pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_VERDICT='No issues found. Summary: the diff contains no code changes to review.'
STUB_TOKEN_USAGE=NONE
run_wrapper "$work"
assert_verdict 'case (i)' FAIL 1
assert_says 'case (i) tokens UNAVAILABLE' '^tokens: UNAVAILABLE$'
assert_says 'case (i) tier2 UNAVAILABLE' '^vacuity-tier2: UNAVAILABLE$'
assert_says 'case (i) degraded notice, not a skip' 'never a silent skip'
assert_says 'case (i) tier1 governs even with tier 2 unavailable' '^vacuity-tier1: FAIL'
assert_lacks 'case (i) UNAVAILABLE never upgrades to PASS' '^RESULT: PASS$'

printf '== case (i2): unavailable accounting alone does not fail an otherwise clean review ==\n'
reset_stub
work=$(make_fixture case_i2 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_VERDICT=$'No issues found.\nSummary: reviewed the diff; no issues found.'
STUB_TOKEN_USAGE=NONE
run_wrapper "$work"
assert_verdict 'case (i2)' PASS 0
assert_says 'case (i2) tier2 UNAVAILABLE' '^vacuity-tier2: UNAVAILABLE$'

printf '== case (j): exit non-zero WITH a completed review = FINDINGS, not a malfunction ==\n'
reset_stub
# roborev exits non-zero when the review REPORTS FINDINGS. Calling that a reviewer
# malfunction is dangerous in the opposite direction from the vacuity bug: an agent
# told the reviewer broke retries or bypasses instead of FIXING THE FINDINGS.
work=$(make_fixture case_j pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_VERDICT=$'Review complete.\nFindings:\n[Medium] scripts/flow/roborev-review.sh:350 the fast path bypasses the authoritative remote check\nSummary: 1 finding.'
STUB_STATUS="done"
STUB_REVIEW_RC=1
run_wrapper "$work"
STUB_REVIEW_RC=0
STUB_VERDICT=$'No issues found.\nSummary: reviewed the diff; no issues found.'
assert_verdict 'case (j)' FAIL 1
assert_says 'case (j) roborev-exit is FINDINGS with the observed code' '^roborev-exit: FINDINGS \(exit 1\)$'
assert_says 'case (j) findings are PRESENT and counted' '^findings: PRESENT \(1\)$'
assert_says 'case (j) says the review is GENUINE' 'The review is GENUINE'
assert_says 'case (j) directs the reader to fix, not retry' 'TRIAGE AND FIX the findings'
assert_lacks 'case (j) does not call it a reviewer malfunction' 'reviewer malfunction — |ERROR \(exit 1\)'
# Every OTHER per-check key passes here — that is the point of the key: without it a
# reader retaining only the block would see all-PASS beside RESULT: FAIL and be
# unable to attribute the failure.
for line in 'push-assert: PASS' 'census-check: PASS' 'sha-assert: PASS' 'vacuity-tier1: PASS' 'vacuity-tier2: PASS'; do
  assert_says "case (j) $line" "^$line\$"
done
assert_lacks 'case (j) is not a PASS' '^RESULT: PASS$'

printf '== case (j1b): exit non-zero with a FAILED job = ERROR (infra), not FINDINGS ==\n'
reset_stub
work=$(make_fixture case_j1b pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_VERDICT='agent invocation failed: connection refused'
STUB_STATUS=failed
STUB_REVIEW_RC=2
run_wrapper "$work"
STUB_REVIEW_RC=0
STUB_STATUS="done"
STUB_VERDICT=$'No issues found.\nSummary: reviewed the diff; no issues found.'
assert_verdict 'case (j1b)' FAIL 1
assert_says 'case (j1b) roborev-exit is ERROR with the observed code' '^roborev-exit: ERROR \(exit 2\)$'
assert_says 'case (j1b) findings are UNKNOWN' '^findings: UNKNOWN$'
assert_says 'case (j1b) attributes it to the reviewer itself' 'The REVIEWER itself failed'
assert_says 'case (j1b) names it an infra condition' 'this is an infra condition, not a findings outcome'
assert_lacks 'case (j1b) is not reported as FINDINGS' '^roborev-exit: FINDINGS'

printf '== case (j2): a zero roborev exit records roborev-exit: PASS ==\n'
reset_stub
work=$(make_fixture case_j2 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (j2)' PASS 0
assert_says 'case (j2) roborev-exit PASS' '^roborev-exit: PASS$'
assert_says 'case (j2) findings NONE' '^findings: NONE$'
# Key ORDER is part of the contract: roborev-exit sits between vacuity-tier2 and log.
if [ "$(grep -nE '^(vacuity-tier2|roborev-exit|log):' "$OUT" | cut -d: -f2 | paste -sd,)" = "vacuity-tier2,roborev-exit,log" ]; then
  ok 'case (j2): roborev-exit is positioned between vacuity-tier2 and log'
else
  bad "case (j2): unexpected key order: $(grep -nE '^(vacuity-tier2|roborev-exit|log):' "$OUT" | cut -d: -f2 | paste -sd,)"
fi

printf '== case (j3): a pre-invocation failure leaves roborev-exit: SKIP ==\n'
reset_stub
work=$(make_fixture case_j3 unpushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (j3)' FAIL 1
assert_says 'case (j3) roborev-exit SKIP (the process never ran)' '^roborev-exit: SKIP$'
assert_never_enqueued 'case (j3)'

printf '== case (s1): a job that never finished must NOT reach PASS ==\n'
reset_stub
# B1, the worst defect: there was no POSITIVE "a review happened" assert. Absence of
# a vacuous phrase was treated as proof one occurred, so a still-waiting job passed.
work=$(make_fixture case_s1 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_VERDICT='Waiting for job 4656 to complete...'
run_wrapper "$work"
assert_verdict 'case (s1)' FAIL 1
assert_says 'case (s1) review-completed FAILs on the missing verdict marker' '^review-completed: FAIL \(no terminal verdict marker\)$'
assert_says 'case (s1) roborev-exit is still PASS (exit 0)' '^roborev-exit: PASS$'
assert_says 'case (s1) explains that absence is not evidence' 'none of them is a review'
assert_lacks 'case (s1) never reports a pass' '^RESULT: PASS$'

printf '== case (s2): the #2433/#3037 model-mismatch 400 must NOT reach PASS ==\n'
reset_stub
work=$(make_fixture case_s2 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_VERDICT='Error: 400 the requested model is not supported for this account. Review aborted.'
run_wrapper "$work"
assert_verdict 'case (s2)' FAIL 1
assert_says 'case (s2) review-completed FAILs' '^review-completed: FAIL \(no terminal verdict marker\)$'
assert_says 'case (s2) names the silent-outage class' '#2433/#3037 model-mismatch 400'

printf '== case (s3): a failed job status must NOT reach PASS ==\n'
reset_stub
work=$(make_fixture case_s3 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_VERDICT='Job 4656 status: failed (provider timeout). No review was produced.'
STUB_STATUS=failed
run_wrapper "$work"
assert_verdict 'case (s3)' FAIL 1
assert_says 'case (s3) review-completed FAILs on the job status' "^review-completed: FAIL \(job status 'failed' is not done\)\$"
assert_says 'case (s3) says nothing was certified' 'nothing was certified'

printf '== case (t1): the REAL announcement shape (abbreviated range base) parses ==\n'
reset_stub
# A range review announces "Enqueued job N for <abbreviated RANGE BASE>", so the
# announcement carries the JOB ID and nothing verifiable about HEAD; the structured
# range is what verifies the scope. The parse must still accept the short form.
work=$(make_fixture case_t1 pushed)
base_sha=$(git -C "$work" rev-parse origin/main)
STUB_ANNOUNCE_SHA="${base_sha:0:9}"
run_wrapper "$work"
assert_verdict 'case (t1)' PASS 0
assert_says 'case (t1) the abbreviated announcement was parsed' '^job: 4656$'
assert_says 'case (t1) the structured range verified the scope' '^sha-assert: PASS$'

printf '== case (t2): an UPPERCASE announcement is normalised, not fed on as garbage ==\n'
reset_stub
work=$(make_fixture case_t2 pushed)
base_sha=$(git -C "$work" rev-parse origin/main)
STUB_ANNOUNCE_SHA=$(printf '%s' "${base_sha:0:9}" | tr 'a-f' 'A-F')
run_wrapper "$work"
assert_verdict 'case (t2)' PASS 0
assert_says 'case (t2) the upper-case announcement was parsed' '^job: 4656$'
assert_says 'case (t2) sha-assert PASSes' '^sha-assert: PASS$'

printf '== case (t3): a 4-hex-char announcement is too short to verify ==\n'
reset_stub
work=$(make_fixture case_t3 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD | cut -c1-4)
# The range git_ref is left VALID on purpose, so the ONLY thing that can fail this run
# is the announcement's 7-hex-char floor.
run_wrapper "$work"
assert_verdict 'case (t3)' FAIL 1
assert_says 'case (t3) the 7-char floor rejects it' '^sha-assert: FAIL \(no parseable enqueue announcement\)$'
assert_says 'case (t3) mentions the 7-hex-char floor' 'at least 7 hex chars'

printf '== case (t4): with two announcements the LAST one is the effective enqueue ==\n'
reset_stub
work=$(make_fixture case_t4 pushed)
head_sha=$(git -C "$work" rev-parse HEAD)
base_sha=$(git -C "$work" rev-parse origin/main)
STUB_ANNOUNCE_SHA="$base_sha"
STUB_JOB=4655
STUB_VERDICT=$'superseded; retrying\nEnqueued job 4656 for '"$base_sha"$'\nNo issues found.\nSummary: reviewed the diff; no issues found.'
run_wrapper "$work"
assert_verdict 'case (t4)' PASS 0
assert_says 'case (t4) the multiplicity is recorded' 'the transcript carries 2 enqueue announcements'
assert_says 'case (t4) the LAST announcement supplied the job id' '^job: 4656$'
assert_lacks 'case (t4) the FIRST job id was not used' '^job: 4655$'

printf '== case (t5): a detached HEAD FAILs before anything is enqueued ==\n'
reset_stub
work=$(make_fixture case_t5 detached)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (t5)' FAIL 1
assert_says 'case (t5) push-assert names the detached HEAD' '^push-assert: FAIL \(detached HEAD\)$'
assert_never_enqueued 'case (t5)'

printf '== case (t6): a census whose git diff FAILS is not "genuinely empty" ==\n'
reset_stub
work=$(make_fixture case_t6 orphan-base)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work" --base unrelated
assert_verdict 'case (t6)' FAIL 1
assert_says 'case (t6) the diff failure is named' '^census-check: FAIL \(git diff failed\)$'
assert_says 'case (t6) it is explicitly not NOTHING-TO-REVIEW' 'explicitly NOT a NOTHING-TO-REVIEW'
assert_lacks 'case (t6) never reports NOTHING-TO-REVIEW' '^RESULT: NOTHING-TO-REVIEW$'
assert_never_enqueued 'case (t6)'

printf '== case (t7): a repository with no commits FAILs closed ==\n'
reset_stub
mkdir -p "$tmp/case_t7"
git init -q -b main "$tmp/case_t7/work"
CASE_N=$((CASE_N + 1))
OUT="$tmp/out-$CASE_N.txt"
INVOKED="$tmp/invoked-$CASE_N.txt"
: >"$INVOKED"
STUB_INVOKED="$INVOKED" PATH="$stubbin:$PATH" bash "$WRAPPER" --repo "$tmp/case_t7/work" \
  --agent codex --model gpt-5.6-sol --log "$tmp/transcript-$CASE_N.txt" >"$OUT" 2>&1
RC=$?
assert_verdict 'case (t7)' FAIL 1
assert_says 'case (t7) names the missing commit' 'there is no commit to review'
assert_never_enqueued 'case (t7)'

printf '== case (t8): roborev absent from PATH FAILs closed ==\n'
reset_stub
work=$(make_fixture case_t8 pushed)
nobin="$tmp/nobin"
mkdir -p "$nobin"
missing_tool=0
for tool in git sed grep awk tr wc head tail cut mkdir dirname basename python3 cat bash; do
  tool_path=$(command -v "$tool" 2>/dev/null || printf '')
  if [ -n "$tool_path" ]; then ln -sf "$tool_path" "$nobin/$tool"; else missing_tool=1; fi
done
if [ "$missing_tool" -eq 1 ]; then
  printf 'SKIP - case (t8): could not assemble a roborev-free PATH (a base tool is missing)\n'
else
  CASE_N=$((CASE_N + 1))
  OUT="$tmp/out-$CASE_N.txt"
  INVOKED="$tmp/invoked-$CASE_N.txt"
  : >"$INVOKED"
  STUB_INVOKED="$INVOKED" PATH="$nobin" "$nobin/bash" "$WRAPPER" --repo "$work" \
    --agent codex --model gpt-5.6-sol --log "$tmp/transcript-$CASE_N.txt" >"$OUT" 2>&1
  RC=$?
  assert_verdict 'case (t8)' FAIL 1
  assert_says 'case (t8) names the absent binary' "'roborev' is not on PATH"
  assert_never_enqueued 'case (t8)'
fi

printf '== case (t9): an abort BEFORE a verdict still emits a block (the EXIT trap) ==\n'
reset_stub
# Nothing exercised the on_exit trap. Pre-creating <log>.facts as a DIRECTORY makes
# the facts-file truncation fail under `set -e` after the review is enqueued.
work=$(make_fixture case_t9 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
mkdir -p "$tmp/trapcase.log.facts"
CASE_N=$((CASE_N + 1))
OUT="$tmp/out-$CASE_N.txt"
INVOKED="$tmp/invoked-$CASE_N.txt"
: >"$INVOKED"
STUB_INVOKED="$INVOKED" PATH="$stubbin:$PATH" bash "$WRAPPER" --repo "$work" \
  --agent codex --model gpt-5.6-sol --log "$tmp/trapcase.log" >"$OUT" 2>&1
RC=$?
assert_verdict 'case (t9)' FAIL 1
assert_says 'case (t9) the abort is reported, not silent' 'terminated unexpectedly'
assert_one_block 'case (t9)'

printf '== case (u1): token accounting present but UNPARSEABLE is drift, and FAILs ==\n'
if [ "$HAVE_PYTHON3" -eq 1 ]; then
reset_stub
# B3: any JSON shape change used to degrade the tier to a NON-FAILING UNAVAILABLE
# while the real counts were the vacuous baseline, and the run PASSED.
work=$(make_fixture case_u1 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_TOKEN_USAGE='{\"input_tokens\":null,\"cached_input_tokens\":\"n/a\",\"total_output_tokens\":53}'
run_wrapper "$work"
assert_verdict 'case (u1)' FAIL 1
assert_says 'case (u1) drift is named under the tier-2 key' '^vacuity-tier2: FAIL \(token accounting present but unparseable — drift\)$'
assert_says 'case (u1) points at the alias lists' 'INPUT/CACHED/OUTPUT_TOKEN_KEYS'
assert_says 'case (u1) refuses a waiver' 'do not waive it'

printf '== case (u2): RENAMED token fields resolve via the alias sets ==\n'
reset_stub
# The vacuous baseline hiding behind renamed fields: aliases must resolve it so the
# vacuity check still fires instead of degrading to UNAVAILABLE + PASS.
work=$(make_fixture case_u2 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_TOKEN_USAGE='{\"prompt_tokens\":18700,\"cache_read_tokens\":0,\"completion_tokens\":53}'
run_wrapper "$work"
assert_verdict 'case (u2)' FAIL 1
assert_says 'case (u2) the renamed counts were read' '^tokens: input=18700 cached=0 output=53$'
assert_says 'case (u2) the vacuous signature fires' '^vacuity-tier2: FAIL \(vacuous token signature\)$'

printf '== case (u3): has_token_data=false beside real counts is a drift NOTICE, not a bypass ==\n'
reset_stub
work=$(make_fixture case_u3 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_TOKEN_USAGE="$TOKENS_VACUOUS"
STUB_HAS_TOKEN_DATA=false
run_wrapper "$work"
STUB_HAS_TOKEN_DATA=''
assert_verdict 'case (u3)' FAIL 1
assert_says 'case (u3) the counts were still used' '^tokens: input=18700 cached=0 output=53$'
assert_says 'case (u3) the inconsistency is recorded' 'has_token_data=false yet readable counts are present'
assert_says 'case (u3) the vacuous signature still fires' '^vacuity-tier2: FAIL \(vacuous token signature\)$'
fi  # HAVE_PYTHON3

printf '== case (v): option-value and option-name validation ==\n'
reset_stub
work=$(make_fixture case_v pushed)
for bad in "--repo" "--base" "--log"; do
  CASE_N=$((CASE_N + 1))
  OUT="$tmp/out-$CASE_N.txt"
  INVOKED="$tmp/invoked-$CASE_N.txt"
  : >"$INVOKED"
  STUB_INVOKED="$INVOKED" PATH="$stubbin:$PATH" bash "$WRAPPER" \
    --agent codex --model gpt-5.6-sol "$bad" '' >"$OUT" 2>&1
  RC=$?
  if [ "$RC" -eq 2 ]; then ok "usage: an empty '$bad' value exits 2"; else bad "usage: an empty '$bad' value exited $RC (want 2)"; fi
  assert_says "usage: an empty '$bad' value is named" "$bad was given an empty value"
done
for bad_invocation in "--nonsense" "--repo:$tmp/definitely-not-a-directory" "--repo:$tmp"; do
  CASE_N=$((CASE_N + 1))
  OUT="$tmp/out-$CASE_N.txt"
  INVOKED="$tmp/invoked-$CASE_N.txt"
  : >"$INVOKED"
  case "$bad_invocation" in
    --repo:*) set -- --repo "${bad_invocation#--repo:}" ;;
    *) set -- "$bad_invocation" ;;
  esac
  STUB_INVOKED="$INVOKED" PATH="$stubbin:$PATH" bash "$WRAPPER" \
    --agent codex --model gpt-5.6-sol "$@" >"$OUT" 2>&1
  RC=$?
  if [ "$RC" -eq 2 ]; then ok "usage: '$bad_invocation' exits 2"; else bad "usage: '$bad_invocation' exited $RC (want 2)"; fi
  assert_never_enqueued "usage: '$bad_invocation'"
done

printf '== case (x1): a PARTIAL review (only the last commit) FAILs ==\n'
reset_stub
# DEFECT 1, the case that fired on the real probe: `roborev review <sha>` reviews ONE
# COMMIT, so on a multi-commit branch "roborev clean" meant "the last commit was
# clean". The wrapper now reviews the census RANGE, and prompt-content is the assert
# that the whole range actually reached the reviewer.
work=$(make_fixture case_x1 two-code-commits)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
STUB_PROMPT='Review this diff: diff --git a/beta.rs b/beta.rs +fn beta() {}'
run_wrapper "$work"
assert_verdict 'case (x1)' FAIL 1
assert_says 'case (x1) prompt-content names the uncovered code path' '^prompt-content: FAIL \(1/2 code census paths absent from the prompt\)$'
assert_says 'case (x1) lists the missing file' '^  alpha\.rs$'
assert_says 'case (x1) says the reviewer never received the diffs' 'the reviewer never received their diffs'
assert_says 'case (x1) the range itself verified fine' '^sha-assert: PASS$'

printf '== case (x3): a range anchored at the EMPTY TREE FAILs (two-positional form) ==\n'
reset_stub
# The measured signature of the two-positional commit-range form: it anchors the range
# at git's empty tree, so the "diff" is the whole file set and only a fraction of the
# real change reaches the reviewer. Asserting BOTH range endpoints catches it.
work=$(make_fixture case_x3 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
STUB_GIT_REF="4b825dc642cb6eb9a060e54bf8d69288fbee4904..$(git -C "$work" rev-parse HEAD)"
run_wrapper "$work"
assert_verdict 'case (x3)' FAIL 1
assert_says 'case (x3) the wrong range BASE is named' 'the range BASE is .4b825dc642cb6eb9a060e54bf8d69288fbee4904.'
assert_says 'case (x3) the empty-tree signature is called out' 'empty-tree base \(4b825dc6\.\.\.\) is the signature of the non-sanctioned two-positional'

printf '== case (x2): the full range in the prompt PASSes ==\n'
reset_stub
work=$(make_fixture case_x2 two-code-commits)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
STUB_PROMPT='Review this diff: diff --git a/alpha.rs b/alpha.rs diff --git a/beta.rs b/beta.rs diff --git a/main.rs b/main.rs'
run_wrapper "$work"
assert_verdict 'case (x2)' PASS 0
assert_says 'case (x2) every code census path was covered' '^prompt-content: PASS \(2/2 code census paths present\)$'

if [ "$HAVE_PYTHON3" -eq 1 ]; then
printf '== case (y1): the ASYNCHRONOUSLY written job record is POLLED, not assumed ==\n'
reset_stub
# DEFECT 2: the instant `--wait` returned, the record had no git_ref/status/model/
# token_usage; moments later it had all four. Unpolled, FOUR asserts silently
# degraded at once on a normal run.
work=$(make_fixture case_y1 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
STUB_RECORD_BLANK_FOR=2
run_wrapper "$work"
STUB_RECORD_BLANK_FOR=0
assert_verdict 'case (y1)' PASS 0
assert_says 'case (y1) the record is reported complete' '^job-record: PASS$'
assert_says 'case (y1) the polling is disclosed' 'became complete only after [0-9]+ poll'
assert_says 'case (y1) the STRONG sha oracle was used' '^sha-assert: PASS$'
assert_says 'case (y1) tier 2 evaluated rather than degrading' '^vacuity-tier2: PASS$'
assert_says 'case (y1) the model was confirmed from the record' '^model: gpt-5\.6-sol$'

printf '== case (y2): a permanently absent record FAILs, it does not fall back ==\n'
reset_stub
work=$(make_fixture case_y2 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
STUB_SHOW_JSON=none
STUB_GIT_REF=none
run_wrapper "$work"
assert_verdict 'case (y2)' FAIL 1
assert_says 'case (y2) the record is DEGRADED, explicitly' '^job-record: DEGRADED'
assert_says 'case (y2) sha-assert refuses to guess' '^sha-assert: FAIL \(job record unavailable — reviewed range unverifiable\)$'
assert_says 'case (y2) explains why prose cannot substitute' 'prose cannot establish that branch HEAD was reviewed'

printf '== case (z1): exit 0 + a QUOTED severity marker cannot exempt tier 1 ==\n'
reset_stub
# DEFECT 3 (codex): findings used to come from a regex over the WHOLE transcript, so
# incidental "[Low]" text set findings: PRESENT and exempted a vacuous verdict from
# the authoritative tier-1 failure. The marker scan is now confined to the findings
# BLOCK, and the structured verdict wins where it exists.
work=$(make_fixture case_z1 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
STUB_VERDICT=$'No issues found.\nThe severity ladder here is [Critical] > [High] > [Medium] > [Low].\nSummary: the diff contains no code changes to review.'
run_wrapper "$work"
assert_verdict 'case (z1)' FAIL 1
assert_says 'case (z1) the quoted markers did not become findings' '^findings: NONE$'
assert_says 'case (z1) tier 1 still FAILs authoritatively' '^vacuity-tier1: FAIL \(vacuous verdict vs non-empty census\)$'

printf '== case (z2): a structured verdict of F exempts the phrase (no false-FAIL) ==\n'
reset_stub
work=$(make_fixture case_z2 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
STUB_VERDICT_FIELD=F
STUB_REVIEW_RC=1
STUB_VERDICT=$'## Findings\n[Medium] the guard matches no code changes too broadly\n## Summary: 1 finding; it says no code changes.'
run_wrapper "$work"
STUB_REVIEW_RC=0
assert_says 'case (z2) findings come from the structured verdict' '^findings: PRESENT \(1\)$'
assert_says 'case (z2) tier 1 is a NOTICE, not a FAIL' '^vacuity-tier1: NOTICE \(phrase present in a findings-bearing review\)$'
assert_lacks 'case (z2) tier 1 does not FAIL' '^vacuity-tier1: FAIL'

printf '== case (z3): verdict says clean while the findings block has markers = INCONSISTENT ==\n'
reset_stub
work=$(make_fixture case_z3 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
STUB_VERDICT_FIELD=P
STUB_VERDICT=$'## Findings\n[High] a real finding\n## Summary: clean.'
run_wrapper "$work"
assert_verdict 'case (z3)' FAIL 1
assert_says 'case (z3) the contradiction is named' '^findings: INCONSISTENT \(verdict clean, 1 findings marker\(s\)\)$'
assert_says 'case (z3) it fails closed' 'One of the two is wrong'

printf '== case (z4): exit 0 with markers INSIDE the findings block = INCONSISTENT ==\n'
reset_stub
work=$(make_fixture case_z4 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
STUB_VERDICT=$'## Findings\n[High] a real finding the exit code contradicts\n## Summary: 1 finding.'
run_wrapper "$work"
assert_verdict 'case (z4)' FAIL 1
assert_says 'case (z4) the exit-code contradiction is named' '^findings: INCONSISTENT \(exit 0, 1 findings marker\(s\)\)$'
assert_says 'case (z4) it cannot exempt tier 1' 'cannot exempt the tier-1 vacuity check'
fi  # HAVE_PYTHON3

printf '== case (x4): a SINGLE-commit record FAILs even when it equals HEAD ==\n'
reset_stub
# codex, round 5: a single-commit review covers ONE commit while prompt-content matches
# PATHS, so when several commits touch the same file a review of only the last one
# passes every path check. The sanctioned form always records a range.
work=$(make_fixture case_x4 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
STUB_GIT_REF=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (x4)' FAIL 1
assert_says 'case (x4) the single-commit record is refused' '^sha-assert: FAIL \(single-commit record, not the census range\)$'
assert_says 'case (x4) explains the same-file blind spot' 'when several commits touch the same file'

printf '== case (x5): a MENTIONED path without a diff header does NOT count as covered ==\n'
reset_stub
# The substring form of this check was satisfied by any incidental mention — including
# this wrapper quoting a path in a comment. Only a real `diff --git a/P b/P` header is
# evidence the file's diff was sent.
work=$(make_fixture case_x5 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
STUB_PROMPT='Review the branch. The wrapper mentions main.rs in prose but no diff is attached.'
run_wrapper "$work"
assert_verdict 'case (x5)' FAIL 1
assert_says 'case (x5) a bare mention is not coverage' "^prompt-content: FAIL \(1/1 code census paths absent from the prompt\)\$"
assert_says 'case (x5) the diff-header requirement is named' "have NO 'diff --git' header in the prompt"

printf '== case (x6): the REAL codex review shape counts as a completed review ==\n'
reset_stub
# The previous allow-list was INVENTED and rejected a genuine codex review (measured on
# the live probe): "## Review Findings" / "- **Severity**: Medium" / "## Summary".
work=$(make_fixture case_x6 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
STUB_VERDICT_FIELD=F
STUB_REVIEW_RC=1
STUB_VERDICT=$'## Review Findings\n\n- **Severity**: Medium\n- **Location**: `scripts/flow/roborev-review.sh`\n- **Problem**: something\n- **Fix**: something else\n\n## Summary\n\nOne finding.'
run_wrapper "$work"
STUB_REVIEW_RC=0
assert_says 'case (x6) the real shape is a completed review' '^review-completed: PASS$'
assert_says 'case (x6) the **Severity** line is counted as a finding' '^findings: PRESENT \(1\)$'
assert_says 'case (x6) it is reported as FINDINGS, not a malfunction' '^roborev-exit: FINDINGS \(exit 1\)$'

if [ "$HAVE_PYTHON3" -eq 1 ]; then
printf '== case (x7): a LIST payload whose job id differs is NOT used as a fallback ==\n'
reset_stub
# codex, round 5: the ID-less fallback used to return the first nested object carrying
# git_ref, so a PREVIOUS review of the same range could falsely certify the new job.
work=$(make_fixture case_x7 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
STUB_PAYLOAD_JOB=999
STUB_SHOW_JSON=none
run_wrapper "$work"
assert_verdict 'case (x7)' FAIL 1
assert_says 'case (x7) the mismatched record was refused' '^job-record: DEGRADED'
assert_says 'case (x7) sha-assert refuses to certify' '^sha-assert: FAIL \(job record unavailable — reviewed range unverifiable\)$'
fi  # HAVE_PYTHON3

printf '== case (w): a MISSING oracles file FAILs closed, never silently no-ops ==\n'
reset_stub
# roborev-review.sh sources scripts/flow/roborev-review-oracles.sh for the push assert
# and the census. If that file vanished and the wrapper carried on, both checks would
# become no-ops while every key still read PASS — the worst regression available here.
work=$(make_fixture case_w pushed)
lonely="$tmp/lonely"
mkdir -p "$lonely"
cp "$WRAPPER" "$lonely/roborev-review.sh"
cp "$SCRIPT_DIR/../flow/roborev-job-facts.py" "$lonely/" 2>/dev/null || true
CASE_N=$((CASE_N + 1))
OUT="$tmp/out-$CASE_N.txt"
INVOKED="$tmp/invoked-$CASE_N.txt"
: >"$INVOKED"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_INVOKED="$INVOKED" PATH="$stubbin:$PATH" bash "$lonely/roborev-review.sh" --repo "$work" \
  --agent codex --model gpt-5.6-sol --log "$tmp/transcript-$CASE_N.txt" >"$OUT" 2>&1
RC=$?
assert_verdict 'case (w)' FAIL 1
assert_says 'case (w) names the missing oracles file' 'oracles file .* is missing'
assert_says 'case (w) refuses to run with the checks disabled' 'Failing closed rather than proceeding'
assert_never_enqueued 'case (w)'
assert_says 'case (w) push-assert never claims a pass' '^push-assert: SKIP$'

printf '== case (w2): a TRUNCATED oracles file FAILs closed too ==\n'
reset_stub
work=$(make_fixture case_w2 pushed)
truncated="$tmp/truncated"
mkdir -p "$truncated"
cp "$WRAPPER" "$truncated/roborev-review.sh"
printf '# corrupt: defines nothing\n' >"$truncated/roborev-review-oracles.sh"
CASE_N=$((CASE_N + 1))
OUT="$tmp/out-$CASE_N.txt"
INVOKED="$tmp/invoked-$CASE_N.txt"
: >"$INVOKED"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_INVOKED="$INVOKED" PATH="$stubbin:$PATH" bash "$truncated/roborev-review.sh" --repo "$work" \
  --agent codex --model gpt-5.6-sol --log "$tmp/transcript-$CASE_N.txt" >"$OUT" 2>&1
RC=$?
assert_verdict 'case (w2)' FAIL 1
assert_says 'case (w2) names the undefined functions' 'did not define roborev_push_assert and roborev_census'
assert_never_enqueued 'case (w2)'

printf '== usage errors: --agent and --model are BOTH required ==\n'
reset_stub
work=$(make_fixture case_usage pushed)
for pair in "--agent codex" "--model gpt-5.6-sol"; do
  CASE_N=$((CASE_N + 1))
  OUT="$tmp/out-$CASE_N.txt"
  INVOKED="$tmp/invoked-$CASE_N.txt"
  : >"$INVOKED"
  # shellcheck disable=SC2086 # deliberate split of the single-option pair
  STUB_INVOKED="$INVOKED" PATH="$stubbin:$PATH" bash "$WRAPPER" --repo "$work" $pair >"$OUT" 2>&1
  RC=$?
  if [ "$RC" -eq 2 ]; then
    ok "usage: '$pair' alone exits 2"
  else
    bad "usage: '$pair' alone exited $RC (want 2)"
  fi
  missing=--model; case "$pair" in "--model"*) missing=--agent ;; esac
  assert_says "usage: '$pair' names the missing option $missing" "missing required option $missing"
  assert_never_enqueued "usage: '$pair'"
done

printf '== the summary header is distinct from every agent-gate header ==\n'
reset_stub
work=$(make_fixture case_hdr pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_VERDICT=$'No issues found.\nSummary: reviewed the diff; no issues found.'
STUB_TOKEN_USAGE="$TOKENS_GENUINE_LARGE"
run_wrapper "$work"
assert_says 'header: roborev block present' '^==== ROBOREV REVIEW SUMMARY ====$'
assert_lacks 'header: no AGENT-GATE SUMMARY header' '==== AGENT-GATE SUMMARY ===='
assert_lacks 'header: no AGENT-GATE LITE SUMMARY header' '==== AGENT-GATE LITE SUMMARY ===='
assert_lacks 'header: no AGENT-GATE DELTA SUMMARY header' '==== AGENT-GATE DELTA SUMMARY ===='

printf '== --help documents the exit codes and the live worktree probe ==\n'
reset_stub
CASE_N=$((CASE_N + 1))
OUT="$tmp/out-$CASE_N.txt"
INVOKED="$tmp/invoked-$CASE_N.txt"
: >"$INVOKED"
STUB_INVOKED="$INVOKED" PATH="$stubbin:$PATH" bash "$WRAPPER" --help >"$OUT" 2>&1
RC=$?
if [ "$RC" -eq 0 ]; then ok '--help exits 0'; else bad "--help exited $RC (want 0)"; fi
assert_says '--help states the exit-code contract' '0=PASS, 1=FAIL, 3=NOTHING-TO-REVIEW'
assert_says '--help names the sanctioned range invocation' 'Sanctioned invocation'
assert_says '--help marks --branch-without-repo non-sanctioned' "WITHOUT an explicit --repo"
assert_says '--help carries the live worktree probe' 'LIVE WORKTREE PROBE'
assert_says '--help states the probe expectation' 'reviewed-sha == head-sha'
assert_says '--help requires both agent and model' 'Both are required'
assert_never_enqueued '--help'

printf '== hermeticity: the wrapper never reaches a real roborev ==\n'
reset_stub
if grep -qE '^\s*roborev (review|show|list)' "$WRAPPER"; then
  ok 'wrapper invokes roborev only through PATH resolution (stubbable)'
else
  # shellcheck disable=SC2016 # the backticks are prose in the failure message
  bad 'wrapper does not invoke `roborev` by bare name — the stub cannot intercept it'
fi

# The tally line deliberately does NOT start with `RESULT:` — that token belongs to
# the agent gate's summary contract and to the wrapper's own block, and a bare
# `RESULT:` here could be mistaken for either by a grep-based reader.
printf '\n==== ROBOREV REVIEW GUARD TEST TALLY ====\n'
printf 'passed: %d  failed: %d\n' "$PASS" "$FAIL"
if [ "$FAIL" -ne 0 ]; then
  printf 'GUARD-TEST RESULT: FAIL\n'
  exit 1
fi
printf 'GUARD-TEST RESULT: PASS\n'
