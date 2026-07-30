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

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/roborev-guard-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

# ---------------------------------------------------------------------------
# The stub reviewer: first on PATH, driven entirely by STUB_* env vars, replaying
# the recorded shapes of the real binary's output.
#   STUB_ANNOUNCE_SHA  sha to announce; empty => emit NO enqueue announcement
#   STUB_JOB           job id in the announcement / show payload
#   STUB_VERDICT       verdict text written to the transcript
#   STUB_TOKENS        "<input> <cached> <output>", or NONE for no token data
#   STUB_REVIEW_RC     exit code of `roborev review`
#   STUB_INVOKED       file the stub appends its argv to (empty file => never run)
# ---------------------------------------------------------------------------
stubbin="$tmp/bin"
mkdir -p "$stubbin"
cat >"$stubbin/roborev" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
cmd="${1:-}"
shift || true
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
    [ "${STUB_TOKENS:-NONE}" != "NONE" ] || { printf 'null\n'; exit 0; }
    # shellcheck disable=SC2086 # deliberate word-split of the "in cached out" triple
    set -- ${STUB_TOKENS}
    printf '{"id":%s,"status":"done","usage":{"input_tokens":%s,"cached_input_tokens":%s,"output_tokens":%s}}\n' \
      "${STUB_JOB:-4600}" "$1" "$2" "$3"
    exit 0
    ;;
  list) printf 'null\n'; exit 0 ;;
  *) printf 'stub: unsupported roborev subcommand: %s\n' "$cmd" >&2; exit 64 ;;
esac
STUB
chmod +x "$stubbin/roborev"

# ---------------------------------------------------------------------------
# Fixture: a work repo with a local bare origin. `git push` updates the
# remote-tracking ref, which is exactly what the wrapper's push assert reads.
# ---------------------------------------------------------------------------
git_q() { git -C "$1" -c user.email=t@example.invalid -c user.name=Tester -c commit.gpgsign=false "${@:2}"; }

# Modes:
#   pushed          wide refspec, feature pushed (mirror ref present -> fast path)
#   unpushed        wide refspec, feature never pushed
#   empty           wide refspec, feature == main, pushed
#   docs-only       wide refspec, markdown-only change, pushed
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
  if grep -qE "$2" "$OUT"; then
    ok "$1"
  else
    bad "$1: pattern '$2' not found in output"
    printf -- '------- captured -------\n'; cat "$OUT"; printf -- '------------------------\n'
  fi
}

assert_lacks() { # assert_lacks <label> <extended-regex>
  if grep -qE "$2" "$OUT"; then
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

export STUB_JOB=4656
export STUB_VERDICT='No issues found'
export STUB_TOKENS='505625 387328 6332'
export STUB_REVIEW_RC=0
export STUB_ANNOUNCE_SHA=''

printf '== case (a): enqueued sha == base ref ==\n'
work=$(make_fixture case_a pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
run_wrapper "$work"
assert_verdict 'case (a)' FAIL 1
assert_says 'case (a) names the base ref' "EQUALS the base ref 'origin/main'"
assert_says 'case (a) sha-assert FAIL' '^sha-assert: FAIL'
assert_one_block 'case (a)'

printf '== case (b): enqueued sha is neither endpoint ==\n'
work=$(make_fixture case_b pushed)
STUB_ANNOUNCE_SHA='0000000000000000000000000000000000000abc'
run_wrapper "$work"
assert_verdict 'case (b)' FAIL 1
assert_says 'case (b) names neither-endpoint' 'matches NEITHER endpoint'
assert_says 'case (b) prints announced beside expected head' "announced reviewed-sha '0000000000000000000000000000000000000abc'"

printf '== case (c): vacuous verdict vs non-empty census ==\n'
work=$(make_fixture case_c pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_VERDICT='No issues found. Summary: the diff contains no code changes to review.'
run_wrapper "$work"
assert_verdict 'case (c)' FAIL 1
assert_says 'case (c) tier1 FAIL' '^vacuity-tier1: FAIL'
assert_says 'case (c) names the contradiction' 'NO CODE CHANGES to review, but the locally computed census is NON-EMPTY'
assert_says 'case (c) prints the census' '^census: [0-9]+ files, \+[0-9]+/-[0-9]+$'
assert_says 'case (c) sha-assert still PASS' '^sha-assert: PASS$'

printf '== case (c2): a code-free (docs-only) census is attributed to the code-free condition ==\n'
work=$(make_fixture case_c2 docs-only)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_VERDICT='No issues found. Summary: the diff contains no code changes to review.'
run_wrapper "$work"
assert_verdict 'case (c2)' FAIL 1
assert_says 'case (c2) names the code-free condition' 'CODE-FREE-DIFF condition'
assert_says 'case (c2) points at primary-source verification' 'primary-source verification recorded in the PR'

printf '== case (d): vacuous token signature vs non-empty census ==\n'
work=$(make_fixture case_d pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_VERDICT='No issues found'
STUB_TOKENS='18700 0 53'
run_wrapper "$work"
assert_verdict 'case (d)' FAIL 1
assert_says 'case (d) tier2 FAIL' '^vacuity-tier2: FAIL'
assert_says 'case (d) tier1 unaffected' '^vacuity-tier1: PASS$'
assert_says 'case (d) prints observed input vs named constant' 'observed input=18700 < ROBOREV_VACUITY_MIN_INPUT_TOKENS=50000'
assert_says 'case (d) prints observed cached vs the zero clause' 'observed cached=0 == 0'
assert_says 'case (d) prints observed output vs named constant' 'observed output=53 < ROBOREV_VACUITY_MIN_OUTPUT_TOKENS=200'
assert_says 'case (d) tokens line carries the triple' '^tokens: input=18700 cached=0 output=53$'

printf '== case (e): unpushed branch ==\n'
work=$(make_fixture case_e unpushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_TOKENS='505625 387328 6332'
run_wrapper "$work"
assert_verdict 'case (e)' FAIL 1
assert_says 'case (e) push-assert FAIL' '^push-assert: FAIL'
assert_says 'case (e) names the missing remote branch authoritatively' "the remote 'origin' has no branch 'feature' \(authoritative: git ls-remote\)"
assert_never_enqueued 'case (e)'

printf '== case (e2): remote behind HEAD names the unpushed commits ==\n'
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
# THE regression case (#2964 follow-up blocker): CQLite clones set
# remote.origin.fetch = +refs/heads/main:refs/remotes/origin/main, so
# refs/remotes/origin/<feature> is NEVER created however often the branch is
# pushed. A mirror-ref push assert false-FAILs here 100% of the time, which would
# make the only sanctioned roborev invocation unusable fleet-wide and push agents
# back to the bare --branch form. The remote (git ls-remote) is the authority.
work=$(make_fixture case_k narrow)
assert_no_mirror_ref 'case (k)' "$work" origin
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_VERDICT='No issues found'
STUB_TOKENS='505625 387328 6332'
run_wrapper "$work"
assert_verdict 'case (k)' PASS 0
assert_says 'case (k) push-assert PASS despite the absent mirror ref' '^push-assert: PASS$'
assert_says 'case (k) the run proceeded to a real review' '^sha-assert: PASS$'

printf '== case (k2): narrow refspec, pushed but behind HEAD ==\n'
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
work=$(make_fixture case_k3 narrow-upstream)
assert_no_mirror_ref 'case (k3)' "$work" upstream
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work" --base upstream/main
assert_verdict 'case (k3)' PASS 0
assert_says 'case (k3) push-assert PASS against the configured upstream' '^push-assert: PASS$'

printf '== case (k4): ls-remote failure is an infra/auth FAIL, not "never pushed" ==\n'
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
work=$(make_fixture case_k5 no-base)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (k5)' FAIL 1
assert_says 'case (k5) census-check FAIL names the unresolvable base' "^census-check: FAIL \(base 'origin/main' unresolvable\)$"
assert_says 'case (k5) push-assert still PASS' '^push-assert: PASS$'
assert_says 'case (k5) says it is not NOTHING-TO-REVIEW' "explicitly NOT a NOTHING-TO-REVIEW"
assert_lacks 'case (k5) is not NOTHING-TO-REVIEW' '^RESULT: NOTHING-TO-REVIEW$'
assert_never_enqueued 'case (k5)'

printf '== case (f): genuine review with matching sha + healthy accounting ==\n'
work=$(make_fixture case_f pushed)
head_sha=$(git -C "$work" rev-parse HEAD)
STUB_ANNOUNCE_SHA="$head_sha"
STUB_VERDICT='No issues found'
STUB_TOKENS='505625 387328 6332'
run_wrapper "$work"
assert_verdict 'case (f)' PASS 0
for line in 'push-assert: PASS' 'census-check: PASS' 'sha-assert: PASS' 'vacuity-tier1: PASS' 'vacuity-tier2: PASS'; do
  assert_says "case (f) $line" "^$line\$"
done
assert_says 'case (f) reviewed-sha printed' "^reviewed-sha: $head_sha\$"
assert_says 'case (f) job printed' '^job: 4656$'
assert_says 'case (f) log path named' '^log: .+transcript-'
assert_one_block 'case (f)'
# The sanctioned invocation form: explicit HEAD sha, explicit ABSOLUTE --repo,
# --wait, both --agent and --model — and never --branch, never two positionals.
if grep -qF -- "review $head_sha --repo $work --agent codex --model gpt-5.6-sol --wait" "$INVOKED"; then
  ok 'case (f): invoked by explicit sha + explicit absolute --repo + --wait'
else
  bad "case (f): unexpected invocation form: $(cat "$INVOKED")"
fi
if grep -qF -- '--branch' "$INVOKED"; then
  bad 'case (f): the non-sanctioned --branch form was used'
else
  ok 'case (f): no --branch in the invocation'
fi

printf '== case (g): genuinely empty census ==\n'
work=$(make_fixture case_g empty)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (g)' NOTHING-TO-REVIEW 3
assert_says 'case (g) census is zero files' '^census: 0 files, \+0/-0$'
assert_says 'case (g) says it is not a pass' 'NOT a pass'
assert_lacks 'case (g) is not a PASS' '^RESULT: PASS$'
assert_never_enqueued 'case (g)'

printf '== case (h): missing / unparseable enqueue announcement ==\n'
work=$(make_fixture case_h pushed)
STUB_ANNOUNCE_SHA=''
STUB_VERDICT='Review complete. (no enqueue line printed)'
run_wrapper "$work"
assert_verdict 'case (h)' FAIL 1
assert_says 'case (h) sha-assert FAIL (unverifiable)' '^sha-assert: FAIL \(no parseable enqueue announcement\)$'
assert_says 'case (h) reviewed-sha unknown' '^reviewed-sha: -$'

printf '== case (i): unavailable token accounting degrades visibly, tier 1 still governs ==\n'
work=$(make_fixture case_i pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_VERDICT='No issues found. Summary: the diff contains no code changes to review.'
STUB_TOKENS='NONE'
run_wrapper "$work"
assert_verdict 'case (i)' FAIL 1
assert_says 'case (i) tokens UNAVAILABLE' '^tokens: UNAVAILABLE$'
assert_says 'case (i) tier2 UNAVAILABLE' '^vacuity-tier2: UNAVAILABLE$'
assert_says 'case (i) degraded-signal notice, not a skip' 'DEGRADED-SIGNAL notice, never a silent skip'
assert_says 'case (i) tier1 still FAILs' '^vacuity-tier1: FAIL'
assert_lacks 'case (i) UNAVAILABLE never upgrades to PASS' '^RESULT: PASS$'

printf '== case (i2): unavailable accounting alone does not fail an otherwise clean review ==\n'
work=$(make_fixture case_i2 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_VERDICT='No issues found'
STUB_TOKENS='NONE'
run_wrapper "$work"
assert_verdict 'case (i2)' PASS 0
assert_says 'case (i2) tier2 UNAVAILABLE' '^vacuity-tier2: UNAVAILABLE$'

printf '== case (j): a non-zero roborev exit FAILs under its own greppable key ==\n'
work=$(make_fixture case_j pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_VERDICT='No issues found'
STUB_TOKENS='505625 387328 6332'
STUB_REVIEW_RC=1
run_wrapper "$work"
STUB_REVIEW_RC=0
assert_verdict 'case (j)' FAIL 1
assert_says 'case (j) roborev-exit FAIL names the observed code' '^roborev-exit: FAIL \(exit 1\)$'
# Every OTHER per-check key passes here — that is the point of the key: without it a
# reader retaining only the block would see all-PASS beside RESULT: FAIL and be
# unable to attribute the failure.
for line in 'push-assert: PASS' 'census-check: PASS' 'sha-assert: PASS' 'vacuity-tier1: PASS' 'vacuity-tier2: PASS'; do
  assert_says "case (j) $line" "^$line\$"
done
assert_says 'case (j) names the reviewer exit in prose too' "'roborev review' exited 1"
assert_lacks 'case (j) is not a PASS' '^RESULT: PASS$'

printf '== case (j2): a zero roborev exit records roborev-exit: PASS ==\n'
work=$(make_fixture case_j2 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (j2)' PASS 0
assert_says 'case (j2) roborev-exit PASS' '^roborev-exit: PASS$'
# Key ORDER is part of the contract: roborev-exit sits between vacuity-tier2 and log.
if [ "$(grep -nE '^(vacuity-tier2|roborev-exit|log):' "$OUT" | cut -d: -f2 | paste -sd,)" = "vacuity-tier2,roborev-exit,log" ]; then
  ok 'case (j2): roborev-exit is positioned between vacuity-tier2 and log'
else
  bad "case (j2): unexpected key order: $(grep -nE '^(vacuity-tier2|roborev-exit|log):' "$OUT" | cut -d: -f2 | paste -sd,)"
fi

printf '== case (j3): a pre-invocation failure leaves roborev-exit: SKIP ==\n'
work=$(make_fixture case_j3 unpushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (j3)' FAIL 1
assert_says 'case (j3) roborev-exit SKIP (the process never ran)' '^roborev-exit: SKIP$'
assert_never_enqueued 'case (j3)'

printf '== usage errors: --agent and --model are BOTH required ==\n'
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
work=$(make_fixture case_hdr pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_VERDICT='No issues found'
STUB_TOKENS='505625 387328 6332'
run_wrapper "$work"
assert_says 'header: roborev block present' '^==== ROBOREV REVIEW SUMMARY ====$'
assert_lacks 'header: no AGENT-GATE SUMMARY header' '==== AGENT-GATE SUMMARY ===='
assert_lacks 'header: no AGENT-GATE LITE SUMMARY header' '==== AGENT-GATE LITE SUMMARY ===='
assert_lacks 'header: no AGENT-GATE DELTA SUMMARY header' '==== AGENT-GATE DELTA SUMMARY ===='

printf '== --help documents the exit codes and the live worktree probe ==\n'
CASE_N=$((CASE_N + 1))
OUT="$tmp/out-$CASE_N.txt"
INVOKED="$tmp/invoked-$CASE_N.txt"
: >"$INVOKED"
STUB_INVOKED="$INVOKED" PATH="$stubbin:$PATH" bash "$WRAPPER" --help >"$OUT" 2>&1
RC=$?
if [ "$RC" -eq 0 ]; then ok '--help exits 0'; else bad "--help exited $RC (want 0)"; fi
assert_says '--help states the exit-code contract' '0=PASS, 1=FAIL, 3=NOTHING-TO-REVIEW'
assert_says '--help marks bare --branch non-sanctioned' 'Non-sanctioned forms'
assert_says '--help carries the live worktree probe' 'LIVE WORKTREE PROBE'
assert_says '--help states the probe expectation' 'reviewed-sha == head-sha'
assert_says '--help requires both agent and model' 'Both are required'
assert_never_enqueued '--help'

printf '== hermeticity: the wrapper never reaches a real roborev ==\n'
if grep -qE '^\s*roborev (review|show|list)' "$WRAPPER"; then
  ok 'wrapper invokes roborev only through PATH resolution (stubbable)'
else
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
