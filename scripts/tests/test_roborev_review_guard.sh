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
# FILE SIZE (campsite rule, issue #1135): this file is over the ~1500-line test target
# and grew further in #3229 (the `(cx*)` census-exclusion family). It is a single
# responsibility — the sanctioned wrapper's guard contract — driven end to end through
# ONE public surface (the wrapper's own summary block) by ONE stub and ONE fixture
# helper, so splitting it would duplicate that scaffolding across files and make a
# missing case harder to see, not easier. The gate's file-size ratchet covers `.rs`
# only, so this is recorded rather than suppressed; revisit under #1135 if the fixture
# helper and the stub ever want separate homes.
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
#                        wrapper's bounded read RETRY can be exercised (a transient
#                        read failure — there is no asynchronous write to wait out;
#                        counter kept in $STUB_INVOKED.reads)
#   STUB_HAS_TOKEN_DATA  emit a has_token_data field with this value (true/false)
#   STUB_LIST_JSON       `none` => `list --json` returns null, so `show` is the only
#                        record source
#   STUB_SHOW_JSON       `none` => `show --json` returns null; `nested` => the MEASURED
#                        shape (review row nesting the job row under "job"); `review-row` => it
#                        returns the REAL review-row shape (id/prompt, no git_ref or
#                        status), forcing the richer `list --json` source; otherwise the
#                        `list --json` fallback path
#   STUB_CONFIG_PATTERNS what `roborev config get exclude_patterns` prints; EMPTY =>
#                        the subcommand is unsupported (exit 64), i.e. the corroboration
#                        state the check must report UNAVAILABLE rather than fail on;
#                        `none` => the binary ANSWERS with an EMPTY list (exit 0), the
#                        only state that can CORROBORATE an empty parse
#   STUB_INVOKED         file the stub appends its argv to (empty => never run)
# ---------------------------------------------------------------------------
stubbin="$tmp/bin"
mkdir -p "$stubbin"
cat >"$stubbin/roborev" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
cmd="${1:-}"
shift || true

# json_prompt: STUB_PROMPT with its double quotes ESCAPED, so the record stays VALID
# JSON. The real binary emits a JSON string; embedding a prompt verbatim broke the
# record for exactly the cases whose PROMPT carries a quote (a C-quoted `diff --git
# "a/..." "b/..."` header, or a path with a literal quote) — which then degraded the
# whole record and false-FAILed `sha-assert:`, hiding what those cases exist to pin.
json_prompt() {
  local p="${STUB_PROMPT:-}"
  printf '%s' "${p//\"/\\\"}"
}

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
    "$(json_prompt)" \
    "$usage$extra"
}

# record_read_blank: true while the first STUB_RECORD_BLANK_FOR record reads should come
# back empty, simulating a TRANSIENT read failure. (It does NOT model an asynchronous
# record write: the job row is present from enqueue — that diagnosis was retracted.)
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
      *" --prompt "*) printf '%b\n' "${STUB_PROMPT:-}"; exit 0 ;;
    esac
    record_read_blank && { printf 'null\n'; exit 0; }
    [ "${STUB_SHOW_JSON:-object}" != none ] || { printf 'null\n'; exit 0; }
    if [ "${STUB_SHOW_JSON:-object}" = nested ]; then
      # The MEASURED `roborev show <id> --json` shape: a REVIEW row carrying its own
      # `id` (equal to the job id) that NESTS the job row under a "job" key.
      printf '{"id":%s,"job_id":%s,"agent":"codex","verdict_bool":0,"prompt":"%s","job":' \
        "${STUB_JOB:-4600}" "${STUB_JOB:-4600}" "$(json_prompt)"
      emit_job_object
      printf '}\n'
      exit 0
    fi
    if [ "${STUB_SHOW_JSON:-object}" = review-row ]; then
      # The REAL `show --json` shape: the REVIEW row — id/agent/prompt, and no
      # git_ref / status / verdict / token_usage.
      printf '{"id":%s,"job_id":%s,"agent":"codex","prompt":"%s"}\n' \
        "${STUB_JOB:-4600}" "${STUB_JOB:-4600}" "$(json_prompt)"
      exit 0
    fi
    emit_job_object; printf '\n'
    exit 0
    ;;
  list)
    record_read_blank && { printf 'null\n'; exit 0; }
    [ "${STUB_LIST_JSON:-array}" != none ] || { printf 'null\n'; exit 0; }
    printf '['; emit_job_object; printf ']\n'
    exit 0
    ;;
  config)
    # `roborev config get exclude_patterns` — the corroboration source (#3229). Three
    # DISTINGUISHABLE states, because conflating any two of them is how the guard
    # silently self-disabled:
    #   unset  => a build that does not answer `config get` at all (exit 64). This is
    #             the only state corroboration may report UNAVAILABLE for.
    #   'none' => the binary ANSWERS, with an EMPTY list. That is real evidence that
    #             nothing is configured, and is what CORROBORATES an empty parse.
    #   else   => the comma-joined patterns the binary reports.
    if [ "${STUB_CONFIG_PATTERNS:-}" = none ]; then
      printf '\n'
      exit 0
    fi
    if [ -z "${STUB_CONFIG_PATTERNS:-}" ]; then
      printf 'stub: config get not supported by this build\n' >&2
      exit 64
    fi
    printf '%s\n' "$STUB_CONFIG_PATTERNS"
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
#   renamed         main.rs renamed to renamed.rs (census sees both paths)
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
#   --- the #3229 exclusion family -------------------------------------------------
#   docs-executables  .sh/.py/.bt under docs/reports/x-artifacts/harness/ — the PR
#                   #3222 shape: 100% under docs/, 100% executable CODE
#   docs-prose      markdown only, UNDER docs/ (distinct from docs-only, which puts
#                   its markdown at the repo root)
#   docs-artifacts-mixed  a docs-scoped artifact the config does NOT exclude, beside a
#                   .rs file — the declared residual (noise, never a swallow)
#   docs-artifacts  markdown + declared docs-scoped artifacts (.txt/.json/.log/.err/
#                   .jsonl) under docs/reports/x-artifacts/ — still code-free
#   nested-docs-json  website/src/content/docs/c.json + a .rs file: a NESTED `docs`
#                   directory, which a ROOT-ANCHORED `docs/**/*.json` must NOT match
#   docs-odd-name   a .sh under docs/ whose filename carries SPACES and a literal
#                   double quote — the NUL-safety regression (`git diff --numstat`
#                   C-QUOTES it, `-z` does not)
#   depth-sh        tool.sh at the root AND sub/tool.sh — pins leading-`/` anchoring
#                   against its slash-less twin
#   build-subtree   build/gen.rs — pins the `<p>/**` sibling pathspec (a bare
#                   directory name excludes its whole subtree only through it)
#   pre-change-mix  .rs + website/src/content/docs/c.json + a docs/ harness .sh +
#                   a nested .md: the faithfulness fixture for `['docs/**','*.md']`
#   worktree-docs-exec  the FLEET's real 1:1:1:1 layout: the ROOT checkout is left on
#                   `main` and the census lives in a LINKED WORKTREE. `make_fixture`
#                   returns the WORKTREE path, so `$REPO` is the worktree and
#                   `$REPO/.roborev.toml` is NOT the file roborev's daemon reads
#   cargo-lock      Cargo.lock beside a .rs file — a path roborev ALWAYS excludes
#                   through a hard-coded built-in, with no configuration involved
#   cargo-lock-only Cargo.lock beside PROSE only: the census's whole CODE half is eaten
#                   by the built-in, so the reviewer would get an EMPTY prompt — the
#                   TOTAL swallow, which must FAIL rather than NOTICE
#   docs-space-dir  docs/storage engine/probe.sh + a .rs file — a CODE census path whose
#                   directory carries a SPACE (the repo tracks 40 such paths), the header
#                   shape `diff --git a/a b.txt b/a b.txt` no regex can split
#   docs-nonascii-name  docs/reports/x-artifacts/é.sh + a .rs file — the C-QUOTED header
#                   shape `diff --git "a/\303\251.sh" "b/..."` git emits for non-ASCII
#   docs-nonascii-prose  docs/é notes.md + a .rs file — a NON-ASCII PROSE path. The only
#                   pre-existing non-ASCII fixture is a `.sh`, i.e. CODE **by accident**,
#                   which is precisely why nothing covered the census CLASSIFYING a quoted
#                   path by its QUOTED spelling (ext `md"`, prefix `"docs/…`)
#   docs-nonascii-artifact  docs/reports/x-artifacts/é.json + a .rs file — the same
#                   misclassification for a docs-scoped ARTIFACT (ext `json"`)
#   rename-space    a rename where BOTH names carry a space (`docs/storage engine/old
#                   probe.sh` → `new probe.sh`): the header `diff --git a/<sp> b/<sp>`
#                   is unsplittable by any `[^ ]+` regex AND is not a same-path header
#   rename-mixed    a rename where only ONE side needs quoting (probe.sh → `é probe.sh`),
#                   producing the MIXED header `diff --git a/… "b/…"`. Mixed headers occur
#                   ONLY on renames, so they were unreachable by a both-sides-quoted parse
#   newline-name    a file literally named `a` beside one named `a<LF>b.rs` — a newline in
#                   a path, which a newline-DELIMITED prompt path set splits into two
#                   records so `grep -Fxq` treats them as ALTERNATIVES (a false PASS)
#   cargo-lock-and-docs-exec  the same lockfile PLUS a docs/ harness .sh, so a
#                   CONFIGURED swallow and a BUILT-IN one occur in one run
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
  # PRE-BRANCH setup: a RENAME is only expressible in `origin/main...HEAD` when the OLD
  # path exists at the BASE. Adding it on the feature branch instead would make the range
  # a plain addition and the rename header unreachable — the fixture would then pin
  # nothing, which is the failure mode this whole family exists to prevent.
  case "$mode" in
    rename-space)
      mkdir -p "$work/docs/storage engine"
      printf '#!/bin/sh\nexit 0\n' >"$work/docs/storage engine/old probe.sh"
      git_q "$work" add "docs/storage engine/old probe.sh"
      git_q "$work" commit -q -m 'base: a space-bearing harness script'
      ;;
    rename-mixed)
      mkdir -p "$work/docs/reports/x-artifacts"
      printf '#!/bin/sh\nexit 0\n' >"$work/docs/reports/x-artifacts/probe.sh"
      git_q "$work" add docs
      git_q "$work" commit -q -m 'base: an ASCII harness script'
      ;;
  esac
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
    renamed)
      # A rename: with `--no-renames` the census sees TWO paths (old deleted, new added)
      # while the reviewer's diff may carry ONE `diff --git a/old b/new` header.
      git_q "$work" mv main.rs renamed.rs
      git_q "$work" commit -q -m 'rename main.rs'
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
    docs-executables)
      mkdir -p "$work/docs/reports/x-artifacts/harness"
      printf '#!/bin/sh\nexit 0\n' >"$work/docs/reports/x-artifacts/harness/run.sh"
      printf 'print("classify")\n' >"$work/docs/reports/x-artifacts/harness/classify.py"
      printf 'BEGIN { exit(); }\n' >"$work/docs/reports/x-artifacts/harness/offcpu.bt"
      git_q "$work" add docs
      git_q "$work" commit -q -m 'harness executables under docs/'
      ;;
    docs-prose)
      mkdir -p "$work/docs/reports"
      printf '# report\n' >"$work/docs/reports/x-report.md"
      printf '# notes\n' >"$work/docs/notes.md"
      git_q "$work" add docs
      git_q "$work" commit -q -m 'prose under docs/'
      ;;
    docs-artifacts-mixed)
      # A docs-scoped ARTIFACT (non-code by classification) that the configuration does
      # NOT exclude, beside a real code file: the DECLARED RESIDUAL direction — noise
      # delivered to the reviewer, never a failure.
      mkdir -p "$work/docs/reports/x-artifacts"
      printf '{"k":1}\n' >"$work/docs/reports/x-artifacts/b.json"
      printf 'fn helper() {}\n' >>"$work/main.rs"
      git_q "$work" add docs main.rs
      git_q "$work" commit -q -m 'an un-excluded docs artifact beside code'
      ;;
    docs-artifacts)
      mkdir -p "$work/docs/reports/x-artifacts"
      printf '# report\n' >"$work/docs/reports/x-report.md"
      printf 'raw\n' >"$work/docs/reports/x-artifacts/a.txt"
      printf '{"k":1}\n' >"$work/docs/reports/x-artifacts/b.json"
      printf 'log line\n' >"$work/docs/reports/x-artifacts/c.log"
      printf 'stderr line\n' >"$work/docs/reports/x-artifacts/d.err"
      printf '{"k":2}\n' >"$work/docs/reports/x-artifacts/e.jsonl"
      git_q "$work" add docs
      git_q "$work" commit -q -m 'docs artifacts only'
      ;;
    nested-docs-json)
      mkdir -p "$work/website/src/content/docs"
      printf '{"nested":true}\n' >"$work/website/src/content/docs/c.json"
      printf 'fn helper() {}\n' >>"$work/main.rs"
      git_q "$work" add website main.rs
      git_q "$work" commit -q -m 'nested docs json plus code'
      ;;
    docs-odd-name)
      mkdir -p "$work/docs/reports/x-artifacts/harness"
      printf '#!/bin/sh\nexit 0\n' >"$work/docs/reports/x-artifacts/harness/odd \"q\" name.sh"
      printf 'fn helper() {}\n' >>"$work/main.rs"
      git_q "$work" add docs main.rs
      git_q "$work" commit -q -m 'a docs harness path with spaces and a quote'
      ;;
    depth-sh)
      mkdir -p "$work/sub"
      printf '#!/bin/sh\nexit 0\n' >"$work/tool.sh"
      printf '#!/bin/sh\nexit 1\n' >"$work/sub/tool.sh"
      git_q "$work" add tool.sh sub/tool.sh
      git_q "$work" commit -q -m 'tool.sh at two depths'
      ;;
    build-subtree)
      mkdir -p "$work/build"
      printf 'fn generated() {}\n' >"$work/build/gen.rs"
      git_q "$work" add build/gen.rs
      git_q "$work" commit -q -m 'a code file inside build/'
      ;;
    worktree-docs-exec)
      mkdir -p "$work/docs/reports/x-artifacts/harness"
      printf '#!/bin/sh\nexit 0\n' >"$work/docs/reports/x-artifacts/harness/run.sh"
      printf 'print("classify")\n' >"$work/docs/reports/x-artifacts/harness/classify.py"
      printf 'BEGIN { exit(); }\n' >"$work/docs/reports/x-artifacts/harness/offcpu.bt"
      git_q "$work" add docs
      git_q "$work" commit -q -m 'harness executables under docs/'
      ;;
    cargo-lock)
      printf '[[package]]\nname = "x"\nversion = "0.1.0"\n' >"$work/Cargo.lock"
      printf 'fn helper() {}\n' >>"$work/main.rs"
      git_q "$work" add Cargo.lock main.rs
      git_q "$work" commit -q -m 'a lockfile beside code'
      ;;
    cargo-lock-only)
      # THE TOTAL BUILT-IN SWALLOW (#3229 round 3, blocker F1). The census's ONLY
      # non-prose file is a lockfile, so `code-free:` PASSes (a `.lock` extension
      # classifies as CODE) — yet the built-in `**/Cargo.lock` drops it, leaving the
      # reviewer an EMPTY prompt. Any dependency-bump branch (`Cargo.lock` / `go.sum` /
      # `pnpm-lock.yaml`) is this shape.
      printf '[[package]]\nname = "x"\nversion = "0.1.0"\n' >"$work/Cargo.lock"
      printf 'doc line\n' >>"$work/README.md"
      git_q "$work" add Cargo.lock README.md
      git_q "$work" commit -q -m 'a lockfile bump beside prose'
      ;;
    docs-space-dir)
      # A CODE census path whose DIRECTORY carries a space — the real repo already tracks
      # `docs/storage engine/`. git does NOT quote a space-only path, so the header it
      # emits is `diff --git a/a b.txt b/a b.txt`, which no `a/<x> b/<y>` regex can split.
      mkdir -p "$work/docs/storage engine"
      printf '#!/bin/sh\nexit 0\n' >"$work/docs/storage engine/probe.sh"
      printf 'fn helper() {}\n' >>"$work/main.rs"
      git_q "$work" add docs main.rs
      git_q "$work" commit -q -m 'a code path under a space-bearing docs directory'
      ;;
    docs-nonascii-name)
      # A NON-ASCII CODE census path: `git diff --numstat` C-QUOTES it
      # (`"docs/reports/x-artifacts/\303\251.sh"`) and so does the `diff --git` header
      # roborev's diff carries (`diff --git "a/..." "b/..."`). Both sides must normalise.
      mkdir -p "$work/docs/reports/x-artifacts"
      printf '#!/bin/sh\nexit 0\n' >"$work/docs/reports/x-artifacts/é.sh"
      printf 'fn helper() {}\n' >>"$work/main.rs"
      git_q "$work" add docs main.rs
      git_q "$work" commit -q -m 'a code path with a non-ASCII name'
      ;;
    docs-nonascii-prose)
      # A NON-ASCII PROSE path. `git diff --numstat` (no `-z`) renders it C-QUOTED, so a
      # census that classifies the QUOTED spelling reads the extension as `md"` and the
      # prefix as `"docs/…` — and calls a MARKDOWN FILE CODE. The configuration then
      # (correctly) excludes it via `*.md`, and `census-exclusion:` reports a CONFIGURED
      # swallow ⇒ FAIL, pre-enqueue, on an ordinary docs+code branch.
      mkdir -p "$work/docs"
      printf '# notes\n' >"$work/docs/é notes.md"
      printf 'fn helper() {}\n' >>"$work/main.rs"
      git_q "$work" add docs main.rs
      git_q "$work" commit -q -m 'a non-ASCII prose path beside code'
      ;;
    docs-nonascii-artifact)
      # The same misclassification for a docs-scoped ARTIFACT: quoted, the extension reads
      # `json"`, so the artifact classifies CODE while `docs/**/*.json` excludes it.
      mkdir -p "$work/docs/reports/x-artifacts"
      printf '{"k":1}\n' >"$work/docs/reports/x-artifacts/é.json"
      printf 'fn helper() {}\n' >>"$work/main.rs"
      git_q "$work" add docs main.rs
      git_q "$work" commit -q -m 'a non-ASCII docs artifact beside code'
      ;;
    rename-space)
      git_q "$work" mv "docs/storage engine/old probe.sh" "docs/storage engine/new probe.sh"
      git_q "$work" commit -q -m 'rename a space-bearing harness script'
      ;;
    rename-mixed)
      git_q "$work" mv "docs/reports/x-artifacts/probe.sh" "docs/reports/x-artifacts/é probe.sh"
      git_q "$work" commit -q -m 'rename an ASCII script to a non-ASCII name'
      ;;
    newline-name)
      # A path with a LITERAL NEWLINE, beside a path equal to its FIRST LINE. That pairing
      # is the whole point: a newline-delimited prompt path set + `grep -Fxq` turns the
      # two-line pattern into two ALTERNATIVES, so the presence of `a` "proves" the
      # presence of `a<LF>b.rs` — a false PASS on a file the reviewer never received.
      printf 'plain\n' >"$work/a"
      printf 'fn odd() {}\n' >"$work/$(printf 'a\nb.rs')"
      git_q "$work" add -A
      git_q "$work" commit -q -m 'a newline-bearing path beside its first line'
      ;;
    cargo-lock-and-docs-exec)
      # BOTH causes at once: a built-in eats Cargo.lock, a configured `docs/**` eats the
      # harness script. The FAIL must win and both must be named.
      mkdir -p "$work/docs/reports/x-artifacts/harness"
      printf '#!/bin/sh\nexit 0\n' >"$work/docs/reports/x-artifacts/harness/run.sh"
      printf '[[package]]\nname = "x"\nversion = "0.1.0"\n' >"$work/Cargo.lock"
      printf 'fn helper() {}\n' >>"$work/main.rs"
      git_q "$work" add docs Cargo.lock main.rs
      git_q "$work" commit -q -m 'a lockfile and a docs harness script beside code'
      ;;
    pre-change-mix)
      mkdir -p "$work/docs/reports/x-artifacts/harness" "$work/website/src/content/docs" "$work/nested/deep"
      printf '#!/bin/sh\nexit 0\n' >"$work/docs/reports/x-artifacts/harness/run.sh"
      printf '{"nested":true}\n' >"$work/website/src/content/docs/c.json"
      printf '# deep notes\n' >"$work/nested/deep/notes.md"
      printf 'fn helper() {}\n' >>"$work/main.rs"
      git_q "$work" add docs website nested main.rs
      git_q "$work" commit -q -m 'the pre-change replication mix'
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
    worktree-docs-exec)
      # Split the checkout: the ROOT stays on `main` (as the fleet's shared checkout
      # does) and `feature` moves into a LINKED WORKTREE, which is what gets returned.
      # `$REPO` is then the worktree while roborev's daemon binds `$root/work`.
      git_q "$work" checkout -q main
      git_q "$work" worktree add -q "$root/wt" feature
      printf '%s' "$root/wt"
      return 0
      ;;
  esac
  printf '%s' "$work"
}

# ---------------------------------------------------------------------------
# The fixture's OWN roborev configuration (issue #3229). Without this capability a
# CONFIGURATION regression is not expressible at all — which is exactly why the
# `docs/**` blind spot could ship behind a fully green suite.
#
# The file is written UNTRACKED at the work-tree root, so it never enters the census
# it is meant to be evaluated against. Every generated file also carries a `[ci]`
# table below the real key holding a DECOY `exclude_patterns = ['**']`: TOML table
# scoping means that decoy is NOT the top-level key, so if the parser ever stopped
# respecting scoping it would read `['**']`, swallow every census path, and FAIL every
# case in this family at once.
# ---------------------------------------------------------------------------
write_roborev_config() { # write_roborev_config <work> <single-line array literal>
  cat >"$1/.roborev.toml" <<EOF
agent = 'codex'
# Filenames or glob patterns to exclude from review diffs for this repo.
exclude_patterns = $2
snapshot_dir = ''

[ci]
# A DECOY: table-scoped, therefore NOT the top-level exclude_patterns key.
exclude_patterns = ['**']
EOF
}

write_roborev_config_raw() { # write_roborev_config_raw <work> <verbatim body>
  printf '%s\n' "$2" >"$1/.roborev.toml"
}

# The narrowed set this repo actually ships (a subset is enough for the fixtures).
NARROWED_PATTERNS="['*.md', 'docs/**/*.txt', 'docs/**/*.json', 'docs/**/*.jsonl', 'docs/**/*.log', 'docs/**/*.err', 'docs/**/*.csv', 'docs/**/*.svg']"
# The PRE-change value this issue exists to retire.
BLANKET_PATTERNS="['docs/**', '*.md']"

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
# A throwaway HOME for every wrapper run (see run_wrapper).
FIXTURE_HOME="$tmp/home"
mkdir -p "$FIXTURE_HOME"

# ---------------------------------------------------------------------------
# A stub `roborev` carrying PLANTED `:(exclude,glob)` literals (#3229 ⑥).
#
# `census-exclusion:` observes the LIVE built-in deny-list by reading the roborev
# EXECUTABLE (fixed-string presence per pinned pattern + a count of `:(exclude,glob)`
# literals). Without a way to VARY those literals a divergence is not expressible at all —
# the same gap that made the `docs/**` configuration regression untestable until
# `write_roborev_config` existed. The default stub carries ZERO such literals, which is
# the `UNAVAILABLE` state (a wrapper/shim, not the Go binary).
#
# The literals live in a shell COMMENT, so the stub's behaviour is untouched.
# ---------------------------------------------------------------------------
# The pinned v0.61.2 set, mirrored here on purpose rather than imported: a test that read
# the constant it is checking could not catch the constant being wrong.
#
# AN ARRAY, for the reason the production constant is one — and this mirroring is exactly
# how the bug hid. Both sides were space-separated strings, so BOTH glob-expanded
# `**/package-lock.json` to the repo-relative `website/package-lock.json`; the planted
# literals and the presence check made the SAME mistake, agreed with each other, and
# `built-in-set: OK` passed while the real binary (which carries the pattern verbatim)
# FAILed. Two defects that cancel are undetectable by a symmetric test BY CONSTRUCTION —
# the #3042 lesson, reproduced here in shell.
GUARD_PINNED_BUILTINS=(
  '**/.beads/**' '**/.cache/**' '**/.gocache/**' '**/.kata.local.toml'
  '**/Cargo.lock' '**/cargo.lock' '**/Gemfile.lock' '**/Package.resolved'
  '**/Pipfile.lock' '**/Podfile.lock' '**/bun.lock' '**/bun.lockb'
  '**/composer.lock' '**/flake.lock' '**/go.sum' '**/mix.lock'
  '**/package-lock.json' '**/packages.lock.json' '**/pdm.lock' '**/pnpm-lock.yaml'
  '**/poetry.lock' '**/pubspec.lock' '**/uv.lock' '**/yarn.lock'
)

make_builtin_stub() { # make_builtin_stub <pinned|added|removed> -> prints a bin dir
  # SEPARATE statements, for the reason `make_fixture` documents: `local` is a builtin, so
  # ALL of its arguments are expanded before any assignment takes effect — `local mode="$1"
  # dir="$tmp/x-$mode"` would read an UNSET `mode` and abort under `set -u`.
  local mode dir p
  mode="$1"
  dir="$tmp/stubbin-$mode"
  mkdir -p "$dir"
  cp "$stubbin/roborev" "$dir/roborev"
  {
    printf '# planted built-in deny-list literals (mode: %s)\n' "$mode"
    for p in "${GUARD_PINNED_BUILTINS[@]}"; do
      # `removed`: drop exactly one pinned pattern, so the presence check names it.
      if [ "$mode" = removed ] && [ "$p" = '**/Cargo.lock' ]; then continue; fi
      printf '# :(exclude,glob)%s\n' "$p"
    done
    # The TWO bare PREFIX CONSTANTS the real binary carries, so the pinned count matches.
    printf '# :(exclude,glob)\n# :(exclude,glob)**/\n'
    # `added`: one EXTRA built-in — the scenario that matters most, an upgrade that starts
    # excluding source. Detected by the count, since a blind re-extraction is unreliable.
    if [ "$mode" = added ]; then printf '# :(exclude,glob)**/*.rs\n'; fi
  } >>"$dir/roborev"
  chmod +x "$dir/roborev"
  printf '%s' "$dir"
}

# Which stub dir `run_wrapper` puts on PATH. Reset by `reset_stub`.
STUBBIN_OVERRIDE=""

run_wrapper() { # run_wrapper <work-dir> [extra wrapper args...]
  local work="$1"; shift
  # Fail loudly on a broken fixture rather than letting the wrapper fall back to
  # $PWD (which would silently run every assert against the REAL repo). `-e`, not `-d`:
  # a LINKED WORKTREE's `.git` is a FILE holding `gitdir: ...`, and the worktree
  # fixtures are the ones that pin the root-checkout config source.
  if [ -z "$work" ] || [ ! -e "$work/.git" ]; then
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
  # HOME is redirected to a throwaway directory (#3229): the exclusion check UNIONs the
  # repo `.roborev.toml` with the GLOBAL `$HOME/.roborev/config.toml`, so a real global
  # config on the host would make these cases machine-dependent. `$FIXTURE_HOME` is
  # empty unless a case deliberately plants a global config in it.
  STUB_INVOKED="$INVOKED" PATH="${STUBBIN_OVERRIDE:-$stubbin}:$PATH" HOME="$FIXTURE_HOME" \
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
# `\n` stays escaped: it is embedded in a JSON string in the stub payload and rendered
# by `printf %b` on the --prompt path, so the wrapper sees real line-anchored headers
# exactly as the measured prompt does.
PROMPT_WITH_PATHS='Review the following change.\ndiff --git a/main.rs b/main.rs\n@@ fn helper() {} @@\ndiff --git a/README.md b/README.md\ndiff --git a/NOTES.md b/NOTES.md'
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
export STUB_LIST_JSON=array
export STUB_REVIEW_RC=0
export STUB_ANNOUNCE_SHA=''
export STUB_CONFIG_PATTERNS=''

reset_stub() {
  STUBBIN_OVERRIDE=""
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
  STUB_LIST_JSON=array
  STUB_CONFIG_PATTERNS=''
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
STUB_PROMPT='Review this diff:\ndiff --git a/.github/workflows/ci.yml b/.github/workflows/ci.yml'
run_wrapper "$work"
assert_verdict 'case (c2d)' PASS 0
assert_says 'case (c2d) a .yml file is not classified documentation' '^code-free: PASS$'

# ===========================================================================
# The (cx*) family: census-exclusion — the census reconciled against the
# EFFECTIVE roborev exclusion set (issue #3229).
#
# What these pin, and why it could not be pinned before: `exclude_patterns` is
# roborev's own configuration, and the wrapper used to ASSERT in a comment that
# roborev filtered out non-code paths by its own judgement. It does not — it drops
# exactly what its configured pathspecs match — and under `docs/**` that discarded 33 EXECUTABLE
# harness files on PR #3222 while every fixture here stayed green, because no
# fixture could supply a configuration at all. `write_roborev_config` closes that.
# ===========================================================================

printf '== case (cx1): executables under docs/ are CODE, survive the narrowed config, and ARE enqueued ==\n'
reset_stub
# The PR #3222 shape: 100% of the diff under docs/, 100% of it executable.
work=$(make_fixture case_cx1 docs-executables)
write_roborev_config "$work" "$NARROWED_PATTERNS"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/docs/reports/x-artifacts/harness/run.sh b/docs/reports/x-artifacts/harness/run.sh\ndiff --git a/docs/reports/x-artifacts/harness/classify.py b/docs/reports/x-artifacts/harness/classify.py\ndiff --git a/docs/reports/x-artifacts/harness/offcpu.bt b/docs/reports/x-artifacts/harness/offcpu.bt'
run_wrapper "$work"
assert_verdict 'case (cx1)' PASS 0
assert_says 'case (cx1) a docs/ path prefix does not make code documentation' '^code-free: PASS$'
assert_says 'case (cx1) the narrowed config swallows nothing' '^census-exclusion: PASS \(3/3 code census paths survive the effective exclusion set; corroboration: UNAVAILABLE; built-in-set: UNAVAILABLE\)$'
assert_says 'case (cx1) the reviewer got all three executables' '^prompt-content: PASS \(3/3 code census paths present\)$'
if [ -s "$INVOKED" ]; then
  ok 'case (cx1): the review WAS enqueued (the blind spot is closed)'
else
  bad 'case (cx1): no review was enqueued — executables under docs/ are still unreviewable'
fi

printf '== case (cx2): PROSE-only under docs/ is still code-free and still never enqueued ==\n'
reset_stub
# The guard must not have been INVERTED by the narrowing: prose is still uncertifiable.
work=$(make_fixture case_cx2 docs-prose)
write_roborev_config "$work" "$NARROWED_PATTERNS"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx2)' FAIL 1
assert_says 'case (cx2) code-free still FAILs on prose under docs/' '^code-free: FAIL \(code-free census: 2/2 files are documentation/specification text\)$'
assert_never_enqueued 'case (cx2)'
assert_says 'case (cx2) the exclusion check is unreached, and says so' '^census-exclusion: SKIP \(not reached\)$'

printf '== case (cx3): docs-scoped ARTIFACTS with no executables are still code-free ==\n'
reset_stub
# The narrowing must not have traded the old blind spot for a VACUOUS review of a diff
# roborev would empty: .txt/.json/.log/.err/.jsonl under docs/ are declared artifacts.
work=$(make_fixture case_cx3 docs-artifacts)
write_roborev_config "$work" "$NARROWED_PATTERNS"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx3)' FAIL 1
assert_says 'case (cx3) artifacts under docs/ classify non-code' '^code-free: FAIL \(code-free census: 6/6 files are documentation/specification text\)$'
assert_never_enqueued 'case (cx3)'

printf '== case (cx4): a RESTORED docs/** FAILs pre-enqueue, naming the swallowed paths ==\n'
reset_stub
# The regression this issue exists to prevent, now mechanized: the exact pre-change
# value, against a census of executables under docs/.
work=$(make_fixture case_cx4 docs-executables)
write_roborev_config "$work" "$BLANKET_PATTERNS"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx4)' FAIL 1
assert_says 'case (cx4) census-exclusion FAILs with the count' '^census-exclusion: FAIL \(3/3 code census paths excluded:'
assert_says 'case (cx4) names a swallowed path AND the pattern that ate it' "docs/reports/x-artifacts/harness/run\.sh by 'docs/\*\*'"
assert_says 'case (cx4) names the second swallowed path' "classify\.py by 'docs/\*\*'"
# The SOURCE tag is load-bearing now that three config files are evaluated: "excluded by
# 'docs/**'" alone does not tell an operator which file to edit.
assert_says 'case (cx4) names WHICH config file the pattern came from' "by 'docs/\*\*' \[repo-config\]"
assert_says 'case (cx4) enumerates every config source it read' "config sources read, ALL of them"
assert_says 'case (cx4) attributes the swallow to the configuration, not the reviewer' 'excluded by YOUR CONFIGURATION'
assert_says 'case (cx4) attributes the defect to configuration, not the reviewer' 'this is a CONFIGURATION defect \(or a roborev built-in\), not a reviewer one'
assert_says 'case (cx4) does not send the reader to prompt-content' 'do NOT go looking at prompt-content'
assert_never_enqueued 'case (cx4)'
assert_says 'case (cx4) prompt-content is never consulted' '^prompt-content: SKIP$'
assert_one_block 'case (cx4)'

printf '== case (cx5): an UNPARSEABLE exclusion set FAILs closed ==\n'
reset_stub
work=$(make_fixture case_cx5 docs-executables)
write_roborev_config_raw "$work" "agent = 'codex'
exclude_patterns = docs/**"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx5)' FAIL 1
assert_says 'case (cx5) unreadable is its own value form' '^census-exclusion: FAIL \(exclusion set unreadable: '
assert_says 'case (cx5) refuses to alias "cannot tell" to "nothing excluded"' "never 'nothing is excluded'"
assert_lacks 'case (cx5) never reports the absent-config PASS' 'no exclusion patterns configured'
assert_never_enqueued 'case (cx5)'

printf '== case (cx5b): a genuinely ABSENT key PASSes, textually distinct from unreadable ==\n'
reset_stub
work=$(make_fixture case_cx5b docs-executables)
write_roborev_config_raw "$work" "agent = 'codex'
model = 'gpt-5.6-sol'"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
# `none` — the binary ANSWERS with an empty list, CORROBORATING the empty parse. The
# previous revision left this unset (exit 64 => UNAVAILABLE), which locked in an
# UN-CORROBORATED "no exclusion patterns configured" PASS: exactly the state the guard
# reaches when it fails to recognise a key roborev honours, so a green case here blessed
# a guard that had silently self-disabled.
STUB_CONFIG_PATTERNS=none
STUB_PROMPT='Review this diff:\ndiff --git a/docs/reports/x-artifacts/harness/run.sh b/docs/reports/x-artifacts/harness/run.sh\ndiff --git a/docs/reports/x-artifacts/harness/classify.py b/docs/reports/x-artifacts/harness/classify.py\ndiff --git a/docs/reports/x-artifacts/harness/offcpu.bt b/docs/reports/x-artifacts/harness/offcpu.bt'
run_wrapper "$work"
assert_verdict 'case (cx5b)' PASS 0
assert_says 'case (cx5b) absent key is an explicit PASS' '^census-exclusion: PASS \(no exclusion patterns configured; '
assert_says 'case (cx5b) the PASS is CORROBORATED by the binary, not merely parsed' 'corroboration: OK; built-in-set: UNAVAILABLE\)$'
assert_says 'case (cx5b) the built-in excludes are still evaluated and counted' 'roborev v0\.61\.2 built-in exclude\(s\)'
assert_lacks 'case (cx5b) is not reported as unreadable' 'exclusion set unreadable'

printf '== case (cx5c): a TABLE-SCOPED exclude_patterns is NOT the top-level key ==\n'
reset_stub
# `exclude_patterns` under `[ci]` is a different key. Reading it as the effective set
# would make the check both wrong and (with `['**']`) permanently red.
work=$(make_fixture case_cx5c docs-executables)
write_roborev_config_raw "$work" "agent = 'codex'

[ci]
exclude_patterns = ['**']"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_CONFIG_PATTERNS=none
STUB_PROMPT='Review this diff:\ndiff --git a/docs/reports/x-artifacts/harness/run.sh b/docs/reports/x-artifacts/harness/run.sh\ndiff --git a/docs/reports/x-artifacts/harness/classify.py b/docs/reports/x-artifacts/harness/classify.py\ndiff --git a/docs/reports/x-artifacts/harness/offcpu.bt b/docs/reports/x-artifacts/harness/offcpu.bt'
run_wrapper "$work"
assert_verdict 'case (cx5c)' PASS 0
assert_says 'case (cx5c) table scoping is respected' '^census-exclusion: PASS \(no exclusion patterns configured; '
assert_says 'case (cx5c) and the empty parse is corroborated, never assumed' 'corroboration: OK; built-in-set: UNAVAILABLE\)$'

printf '== case (cx5d): parse sees NOTHING while the binary reports a pattern => DRIFT FAIL ==\n'
reset_stub
# BLOCKER B, mechanized. The old code returned `PASS (no exclusion patterns configured)`
# and `return 0` BEFORE corroboration ever ran, so "our parser recognised no key" was
# aliased to "nothing is configured". Measured against roborev v0.61.2: a QUOTED key is
# valid TOML and IS honoured, so this state is reachable in the real world — and it
# enqueues a review from which every docs/ executable is silently dropped, i.e. #3229
# reintroduced under the key meant to prevent it. Here the config file carries a key the
# parser deliberately cannot see (an alien spelling) while the binary reports `docs/**`.
work=$(make_fixture case_cx5d docs-executables)
write_roborev_config_raw "$work" "agent = 'codex'
exclude__patterns = ['docs/**']"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_CONFIG_PATTERNS="docs/**"
run_wrapper "$work"
assert_verdict 'case (cx5d)' FAIL 1
assert_says 'case (cx5d) an unparsed-but-honoured pattern is DRIFT, not a PASS' "^census-exclusion: FAIL \(exclusion set drift: 'docs/\*\*' reported by roborev config get is absent from the parsed set\)$"
assert_says 'case (cx5d) the empty-parse case is called out explicitly' 'the parse found NO configured pattern at all while the binary reports at least one'
assert_says 'case (cx5d) names the issue it would reintroduce' 'issue #3229 reintroduced under the key meant to prevent it'
assert_lacks 'case (cx5d) never reports the un-corroborated absent-config PASS' '^census-exclusion: PASS'
assert_never_enqueued 'case (cx5d)'

printf '== case (cx5e): a QUOTED key spelling is recognised, not silently skipped ==\n'
reset_stub
# The concrete v0.61.2 measurement behind cx5d: `"exclude_patterns"` is the SAME key, is
# honoured by the binary, and the bare-key match used to skip the line entirely. With the
# quoted spelling recognised the swallow is caught by the primary path (a named swallowed
# path), not merely by the drift backstop.
work=$(make_fixture case_cx5e docs-executables)
write_roborev_config_raw "$work" "agent = 'codex'
\"exclude_patterns\" = ['docs/**', '*.md']"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx5e)' FAIL 1
assert_says 'case (cx5e) the quoted key is parsed, so the swallow is named directly' '^census-exclusion: FAIL \(3/3 code census paths excluded:'
assert_says 'case (cx5e) with the pattern and its source file' "run\.sh by 'docs/\*\*' \[repo-config\]"
assert_lacks 'case (cx5e) never reports the absent-config PASS' 'no exclusion patterns configured'
assert_never_enqueued 'case (cx5e)'

printf "== case (cx5f): a SINGLE-quoted key spelling is recognised too ==\n"
reset_stub
work=$(make_fixture case_cx5f docs-executables)
write_roborev_config_raw "$work" "agent = 'codex'
'exclude_patterns' = ['docs/**']"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx5f)' FAIL 1
assert_says 'case (cx5f) the single-quoted key is parsed' '^census-exclusion: FAIL \(3/3 code census paths excluded:'
assert_never_enqueued 'case (cx5f)'

printf '== case (cx5g): an UNKNOWN TOML escape is refused, never silently swallowed ==\n'
reset_stub
# `"a\tb"` used to yield the 3-byte `atb` — a pattern SILENTLY DIFFERENT from the one
# roborev applies, which is the whole failure mode this check exists to prevent. An
# untranslated escape is "we could not tell", so it fails closed.
work=$(make_fixture case_cx5g docs-executables)
write_roborev_config_raw "$work" 'agent = '"'"'codex'"'"'
exclude_patterns = ["docs\q**"]'
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx5g)' FAIL 1
assert_says 'case (cx5g) the unknown escape is named' "^census-exclusion: FAIL \(exclusion set unreadable: unknown escape "
assert_says 'case (cx5g) it explains why swallowing the backslash is wrong' 'is a<TAB>b, not atb'
assert_lacks 'case (cx5g) never reports the absent-config PASS' 'no exclusion patterns configured'
assert_never_enqueued 'case (cx5g)'

printf '== case (cx6): a census path with SPACES and a literal quote compares correctly ==\n'
reset_stub
# NUL-safety. `git diff --numstat` C-QUOTES this path while `-z` output does not, so an
# un-normalised comparison would report a SURVIVING path as swallowed (a false FAIL) —
# the direction that gets a guard bypassed.
work=$(make_fixture case_cx6 docs-odd-name)
write_roborev_config "$work" "$NARROWED_PATTERNS"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/main.rs b/main.rs\ndiff --git a/docs/reports/x-artifacts/harness/odd "q" name.sh b/docs/reports/x-artifacts/harness/odd "q" name.sh'
run_wrapper "$work"
# ASSERT THE VERDICT, not just one key (#3229 round 3, blocker F3). This case used to
# assert `census-exclusion:` alone and therefore reported `ok` twice while the SAME
# hostile path false-FAILed `prompt-content:` (MEASURED: `census-exclusion: PASS (2/2
# survive)` beside `prompt-content: FAIL (1/2 absent)`, `RESULT: FAIL`) — a case that
# passes while the behaviour it names is broken is worse than no case at all.
assert_verdict 'case (cx6)' PASS 0
assert_says 'case (cx6) the odd-named .sh is NOT reported swallowed' '^census-exclusion: PASS \(2/2 code census paths survive'
assert_lacks 'case (cx6) no false FAIL from a quoting artefact' '^census-exclusion: FAIL'
assert_says 'case (cx6) prompt-content compares the quoted census path against the prompt NORMALISED' '^prompt-content: PASS \(2/2 code census paths present\)$'
assert_lacks 'case (cx6) prompt-content does not false-FAIL on a quoting artefact' '^prompt-content: FAIL'

printf '== case (cx6c): a CODE path under a SPACE-bearing directory does not false-FAIL prompt-content ==\n'
reset_stub
# `docs/storage engine/` is a real tracked directory in this repo (40 space-bearing paths
# under docs/), and this change promotes docs/-scoped executables to CODE census paths.
# git does not quote a space-only path, so the header is `diff --git a/a b.sh b/a b.sh`,
# which `a/[^ ]+ b/[^ ]+` cannot split — matched instead by probing the LITERAL header.
work=$(make_fixture case_cx6c docs-space-dir)
write_roborev_config "$work" "$NARROWED_PATTERNS"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/main.rs b/main.rs\ndiff --git a/docs/storage engine/probe.sh b/docs/storage engine/probe.sh'
run_wrapper "$work"
assert_verdict 'case (cx6c)' PASS 0
assert_says 'case (cx6c) the space-bearing path is a surviving CODE census path' '^census-exclusion: PASS \(2/2 code census paths survive'
assert_says 'case (cx6c) prompt-content recognises the space-bearing diff header' '^prompt-content: PASS \(2/2 code census paths present\)$'
assert_lacks 'case (cx6c) prompt-content does not false-FAIL on a space' '^prompt-content: FAIL'

printf '== case (cx6d): a NON-ASCII CODE path is compared through the C-QUOTED header shape ==\n'
reset_stub
# `git diff --numstat` renders the census path as `"docs/.../\303\251.sh"` and the diff
# header roborev carries is `diff --git "a/docs/.../\303\251.sh" "b/..."`. BOTH sides go
# through `roborev_unquote_path`, so the two spellings compare EQUAL. (The stub emits the
# prompt through `printf %b`, hence the doubled backslashes here: they render as the
# single-backslash octal escapes git actually writes.)
work=$(make_fixture case_cx6d docs-nonascii-name)
write_roborev_config "$work" "$NARROWED_PATTERNS"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/main.rs b/main.rs\ndiff --git "a/docs/reports/x-artifacts/\\303\\251.sh" "b/docs/reports/x-artifacts/\\303\\251.sh"'
run_wrapper "$work"
assert_verdict 'case (cx6d)' PASS 0
assert_says 'case (cx6d) the non-ASCII path is a surviving CODE census path' '^census-exclusion: PASS \(2/2 code census paths survive'
assert_says 'case (cx6d) prompt-content recognises the C-quoted diff header' '^prompt-content: PASS \(2/2 code census paths present\)$'
assert_lacks 'case (cx6d) prompt-content does not false-FAIL on an octal-escaped path' '^prompt-content: FAIL'

printf '== case (cx6e): a NON-ASCII PROSE path is classified by its RAW bytes, not its quoted spelling ==\n'
reset_stub
# #3229 round 4, BLOCKER F1. The census read `git diff --numstat` WITHOUT `-z`, so this
# path arrived C-QUOTED and was classified by that spelling: extension `md"` (not `md`),
# prefix `"docs/…` (not `docs/`) ⇒ a MARKDOWN FILE counted as CODE. `*.md` then legitimately
# excludes it and `census-exclusion:` reported a CONFIGURED swallow ⇒ FAIL, pre-enqueue, on
# an ORDINARY docs+code branch. REPRODUCED against the repo's own tracked
# `docs/research/CQLite Writes (M5) — Analysis & Recommended Paths.md`.
# The pre-existing non-ASCII fixture is a `.sh` — CODE by accident — which is why no case
# covered this. This one is deliberately PROSE.
work=$(make_fixture case_cx6e docs-nonascii-prose)
write_roborev_config "$work" "$NARROWED_PATTERNS"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/main.rs b/main.rs'
run_wrapper "$work"
assert_verdict 'case (cx6e)' PASS 0
assert_says 'case (cx6e) the non-ASCII .md is classified NON-code, so only main.rs is code' '^census-exclusion: PASS \(1/1 code census paths survive'
assert_lacks 'case (cx6e) no false configured-swallow FAIL on a quoted prose path' '^census-exclusion: FAIL'
assert_says 'case (cx6e) prompt-content has exactly the one code path to look for' '^prompt-content: PASS \(1/1 code census paths present\)$'

printf '== case (cx6f): a NON-ASCII docs ARTIFACT is classified by its RAW bytes too ==\n'
reset_stub
# The same defect for the docs-scoped ARTIFACT half of the classification: quoted, the
# extension reads `json"`, so a report artifact counted as CODE while `docs/**/*.json`
# excludes it ⇒ the same false pre-enqueue FAIL on any report PR carrying a non-ASCII name.
work=$(make_fixture case_cx6f docs-nonascii-artifact)
write_roborev_config "$work" "$NARROWED_PATTERNS"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/main.rs b/main.rs'
run_wrapper "$work"
assert_verdict 'case (cx6f)' PASS 0
assert_says 'case (cx6f) the non-ASCII .json under docs/ is a NON-code census path' '^census-exclusion: PASS \(1/1 code census paths survive'
assert_lacks 'case (cx6f) no false configured-swallow FAIL on a quoted artifact path' '^census-exclusion: FAIL'

printf '== case (cx6g): a RENAME whose BOTH names carry a space is reachable ==\n'
reset_stub
# #3229 round 4, BLOCKER F2. The census runs `--no-renames` (two paths) while the
# reviewer's diff has rename detection ON (confirmed: `--no-renames` is absent from the
# roborev binary's strings), emitting ONE header `diff --git a/<old> b/<new>`. With a space
# in each name, step (a)'s `[^ ]+` regex cannot split it, step (b) requires BOTH sides
# quoted, and the literal fallback only probed the SAME-path header — so BOTH census sides
# were reported absent and `prompt-content:` FAILed a correct review.
work=$(make_fixture case_cx6g rename-space)
write_roborev_config "$work" "$NARROWED_PATTERNS"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/docs/storage engine/old probe.sh b/docs/storage engine/new probe.sh\nsimilarity index 100%'
run_wrapper "$work"
assert_verdict 'case (cx6g)' PASS 0
assert_says 'case (cx6g) both space-bearing rename sides survive the exclusion set' '^census-exclusion: PASS \(2/2 code census paths survive'
assert_says 'case (cx6g) one rename header covers both space-bearing census sides' '^prompt-content: PASS \(2/2 code census paths present\)$'
assert_lacks 'case (cx6g) prompt-content does not false-FAIL on a space-bearing rename' '^prompt-content: FAIL'

printf '== case (cx6h): a MIXED-QUOTED rename header (only one side quoted) is reachable ==\n'
reset_stub
# BROADER than reported (round 4): when only ONE side needs quoting git emits
# `diff --git a/<ascii> "b/<quoted>"`, which neither the unquoted regex nor the
# both-sides-quoted parse can read. Mixed headers occur ONLY on renames, so this shape was
# structurally unreachable. (Doubled backslashes: the stub renders the prompt with
# `printf %b`, so `\\303` becomes the single-backslash octal escape git writes.)
work=$(make_fixture case_cx6h rename-mixed)
write_roborev_config "$work" "$NARROWED_PATTERNS"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/docs/reports/x-artifacts/probe.sh "b/docs/reports/x-artifacts/\\303\\251 probe.sh"\nsimilarity index 100%'
run_wrapper "$work"
assert_verdict 'case (cx6h)' PASS 0
assert_says 'case (cx6h) both sides of the mixed-quoted rename survive' '^census-exclusion: PASS \(2/2 code census paths survive'
assert_says 'case (cx6h) the mixed-quoted rename header covers both census sides' '^prompt-content: PASS \(2/2 code census paths present\)$'
assert_lacks 'case (cx6h) prompt-content does not false-FAIL on a mixed-quoted header' '^prompt-content: FAIL'

printf '== case (cx6i): a NEWLINE-bearing census path cannot be "proved present" by its first line ==\n'
reset_stub
# #3229 round 4, F3 (nit, fixed inside F2's edit). The prompt path set was
# NEWLINE-DELIMITED and membership was `grep -Fxq`, so the census path `a<LF>b.rs` became
# the two-alternative pattern {`a`, `b.rs`} and the presence of `a` alone reported
# `prompt-content: PASS (2/2 present)` — a genuine FALSE PASS on a file the reviewer never
# received. Membership is now judged PER HEADER in bash, with no delimiter at all.
work=$(make_fixture case_cx6i newline-name)
write_roborev_config "$work" "$NARROWED_PATTERNS"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/a b/a'
run_wrapper "$work"
assert_verdict 'case (cx6i)' FAIL 1
assert_says 'case (cx6i) the newline path is reported ABSENT, not implied by its first line' '^prompt-content: FAIL \(1/2 code census paths absent from the prompt\)$'

printf '== case (cx6j): the same newline-bearing path IS matched when its header is present ==\n'
reset_stub
# The other direction, so (cx6i) cannot be satisfied by a blanket "newline ⇒ absent" rule:
# with the C-quoted header git really emits for such a path, it must count as PRESENT.
work=$(make_fixture case_cx6j newline-name)
write_roborev_config "$work" "$NARROWED_PATTERNS"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/a b/a\ndiff --git "a/a\\nb.rs" "b/a\\nb.rs"'
run_wrapper "$work"
assert_verdict 'case (cx6j)' PASS 0
assert_says 'case (cx6j) the quoted newline header counts as present' '^prompt-content: PASS \(2/2 code census paths present\)$'

printf '== case (cx6b): the same odd-named path is named RAW when a blanket glob eats it ==\n'
reset_stub
work=$(make_fixture case_cx6b docs-odd-name)
write_roborev_config "$work" "$BLANKET_PATTERNS"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx6b)' FAIL 1
assert_says 'case (cx6b) only the docs/ path is swallowed, main.rs survives' '^census-exclusion: FAIL \(1/2 code census paths excluded:'
assert_says 'case (cx6b) the swallowed path is named with its real bytes' 'odd "q" name\.sh'
assert_never_enqueued 'case (cx6b)'

printf '== case (cx7): corroboration DRIFT — a pattern the binary reports that the parse lacks ==\n'
reset_stub
work=$(make_fixture case_cx7 docs-executables)
write_roborev_config "$work" "$NARROWED_PATTERNS"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_CONFIG_PATTERNS="*.md, docs/**/*.txt, docs/**"
run_wrapper "$work"
assert_verdict 'case (cx7)' FAIL 1
assert_says 'case (cx7) drift is its own value form' "^census-exclusion: FAIL \(exclusion set drift: 'docs/\*\*' reported by roborev config get is absent from the parsed set\)$"
assert_says 'case (cx7) explains why that direction fails' 'could be excluding census code invisibly'
assert_never_enqueued 'case (cx7)'

printf '== case (cx7b): corroboration OK when the binary agrees ==\n'
reset_stub
work=$(make_fixture case_cx7b docs-executables)
write_roborev_config "$work" "['*.md', 'docs/**/*.txt']"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_CONFIG_PATTERNS="*.md, docs/**/*.txt"
STUB_PROMPT='Review this diff:\ndiff --git a/docs/reports/x-artifacts/harness/run.sh b/docs/reports/x-artifacts/harness/run.sh\ndiff --git a/docs/reports/x-artifacts/harness/classify.py b/docs/reports/x-artifacts/harness/classify.py\ndiff --git a/docs/reports/x-artifacts/harness/offcpu.bt b/docs/reports/x-artifacts/harness/offcpu.bt'
run_wrapper "$work"
assert_verdict 'case (cx7b)' PASS 0
assert_says 'case (cx7b) corroboration is reported OK' 'corroboration: OK; built-in-set: UNAVAILABLE\)$'

printf '== case (cx8): a slash-containing pattern is ROOT-ANCHORED, so a nested docs path SURVIVES ==\n'
reset_stub
# R1 from the disassembly. Evaluating BOTH a verbatim and a `**/`-prefixed reading and
# failing on either would emit a false FAIL here — on a legitimate report PR.
work=$(make_fixture case_cx8 nested-docs-json)
write_roborev_config "$work" "['*.md', 'docs/**/*.json']"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/main.rs b/main.rs\ndiff --git a/website/src/content/docs/c.json b/website/src/content/docs/c.json'
run_wrapper "$work"
assert_verdict 'case (cx8)' PASS 0
assert_says 'case (cx8) the nested docs/ path is a CODE census path that SURVIVES' '^census-exclusion: PASS \(2/2 code census paths survive'
assert_lacks 'case (cx8) no false FAIL from a both-readings interpretation' '^census-exclusion: FAIL'

printf '== case (cx9): a bare directory name excludes its whole subtree via the /** sibling ==\n'
reset_stub
# R2. `**/build` alone does NOT match `build/gen.rs`; only the `<p>/**` sibling does, so
# a port that emitted one pathspec per pattern would MISS this swallow.
work=$(make_fixture case_cx9 build-subtree)
write_roborev_config "$work" "['build']"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx9)' FAIL 1
assert_says 'case (cx9) the subtree file is swallowed by the bare directory name' "^census-exclusion: FAIL \(1/1 code census paths excluded: build/gen\.rs by 'build' \[repo-config\]\)$"
assert_says 'case (cx9) the emitted pathspecs include the /** sibling' ':\(exclude,glob\)\*\*/build/\*\*'
assert_never_enqueued 'case (cx9)'

printf '== case (cx10): a LEADING slash root-anchors an otherwise-recursive slash-less name ==\n'
reset_stub
# R4. `/tool.sh` => `:(exclude,glob)tool.sh` (root only).
work=$(make_fixture case_cx10 depth-sh)
write_roborev_config "$work" "['/tool.sh']"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx10)' FAIL 1
assert_says 'case (cx10) only the ROOT tool.sh is excluded' "^census-exclusion: FAIL \(1/2 code census paths excluded: tool\.sh by '/tool\.sh' \[repo-config\]\)$"
assert_says 'case (cx10) the pathspec is emitted root-anchored, without the **/ prefix' ':\(exclude,glob\)tool\.sh$'
assert_lacks 'case (cx10) sub/tool.sh is NOT reported swallowed' 'sub/tool\.sh by'

printf '== case (cx10b): its slash-less twin excludes at ANY depth ==\n'
reset_stub
work=$(make_fixture case_cx10b depth-sh)
write_roborev_config "$work" "['tool.sh']"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx10b)' FAIL 1
assert_says 'case (cx10b) BOTH depths are excluded' '^census-exclusion: FAIL \(2/2 code census paths excluded:'
assert_says 'case (cx10b) the nested twin is named' "sub/tool\.sh by 'tool\.sh'"
assert_says 'case (cx10b) the pathspec is **/-prefixed (RECURSIVE)' ':\(exclude,glob\)\*\*/tool\.sh$'

printf '== case (cx11): a TRAILING-slash pattern FAILs naming the inversion, diff-independently ==\n'
reset_stub
# R3, and the decision that it is a FAIL rather than a NOTICE: this census has NO path
# under docs/ at all, so the FAIL can only come from the CONFIGURATION.
work=$(make_fixture case_cx11 pushed)
write_roborev_config "$work" "['*.md', 'docs/']"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx11)' FAIL 1
assert_says 'case (cx11) the trailing-slash inversion is named in the value, WITH its source file' "^census-exclusion: FAIL \(trailing-slash pattern 'docs/' from repo-config resolves RECURSIVE \(\*\*/docs\), opposite to 'docs/\*\*' — drop the trailing slash deliberately or write 'docs/\*\*'\)$"
assert_says 'case (cx11) the detail explains the trim-before-anchoring order' "trims a trailing '/' BEFORE deciding whether the pattern is root-anchored"
assert_says 'case (cx11) the FAIL is explicitly diff-independent' 'independent of whether the pattern currently swallows a census path'
assert_never_enqueued 'case (cx11)'

printf '== case (cx12): a WHITESPACE-ONLY pattern is skipped, never a match-everything ==\n'
reset_stub
# The algorithm skips an empty-after-trim pattern. Treating it as a match-everything
# would swallow the whole census and make the check permanently red.
work=$(make_fixture case_cx12 pushed)
write_roborev_config "$work" "['   ', '*.md']"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx12)' PASS 0
assert_says 'case (cx12) the whitespace-only pattern excluded nothing' '^census-exclusion: PASS \(1/1 code census paths survive'

printf '== case (cx13): the port reproduces the PRE-change configuration faithfully ==\n'
reset_stub
# `['docs/**', '*.md']` resolves to a ROOT-ANCHORED `docs/**` plus a RECURSIVE
# `**/*.md` — which is exactly what the 21-review replay measured (only `.md` was ever
# dropped, at any depth, repo-wide, and never a non-`.md`). The census here carries a
# docs/ harness .sh (must be swallowed), a NESTED docs/ .json (must SURVIVE, proving
# root anchoring) and a .rs (must survive).
work=$(make_fixture case_cx13 pre-change-mix)
write_roborev_config "$work" "$BLANKET_PATTERNS"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx13)' FAIL 1
assert_says 'case (cx13) exactly ONE of the three code paths is swallowed' '^census-exclusion: FAIL \(1/3 code census paths excluded:'
assert_says 'case (cx13) the swallowed one is the docs/ harness script' "docs/reports/x-artifacts/harness/run\.sh by 'docs/\*\*'"
assert_lacks 'case (cx13) the NESTED docs json SURVIVES (docs/** is root-anchored)' 'website/src/content/docs/c\.json by'
assert_says 'case (cx13) docs/** is emitted VERBATIM, root-anchored' ':\(exclude,glob\)docs/\*\*$'
assert_says 'case (cx13) *.md is emitted **/-prefixed, RECURSIVE and repo-wide' ':\(exclude,glob\)\*\*/\*\.md$'

printf '== case (cx14): census-exclusion sits EXACTLY ONCE, immediately after code-free ==\n'
reset_stub
work=$(make_fixture case_cx14 mixed)
write_roborev_config "$work" "$NARROWED_PATTERNS"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx14)' PASS 0
n_excl=$(grep -cE '^census-exclusion: ' "$OUT" || true)
if [ "$n_excl" -eq 1 ]; then
  ok 'case (cx14): census-exclusion: appears exactly once'
else
  bad "case (cx14): census-exclusion: appears $n_excl time(s) (want 1)"
fi
order=$(grep -nE '^(code-free|census-exclusion|job-record):' "$OUT" | cut -d: -f2 | paste -sd,)
if [ "$order" = 'code-free,census-exclusion,job-record' ]; then
  ok 'case (cx14): key order is code-free -> census-exclusion -> job-record'
else
  bad "case (cx14): unexpected key order: $order"
fi

printf '== case (cx15): an unreached exclusion check reads SKIP, never blank ==\n'
reset_stub
work=$(make_fixture case_cx15 unpushed)
write_roborev_config "$work" "$BLANKET_PATTERNS"
run_wrapper "$work"
assert_verdict 'case (cx15)' FAIL 1
assert_says 'case (cx15) push-assert is the failing key' '^push-assert: FAIL'
assert_says 'case (cx15) census-exclusion carries an explicit SKIP cause' '^census-exclusion: SKIP \(not reached\)$'
assert_lacks 'case (cx15) census-exclusion is never blank' '^census-exclusion: *$'

printf '== case (cx16): the GLOBAL config is UNIONed with the repo one ==\n'
reset_stub
# config.ResolveExcludePatterns merges the two; a repo-only read would miss a swallow
# configured globally.
work=$(make_fixture case_cx16 docs-executables)
write_roborev_config "$work" "['*.md']"
mkdir -p "$FIXTURE_HOME/.roborev"
printf "agent = 'codex'\nexclude_patterns = ['docs/**']\n" >"$FIXTURE_HOME/.roborev/config.toml"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
rm -rf "$FIXTURE_HOME/.roborev"
assert_verdict 'case (cx16)' FAIL 1
assert_says 'case (cx16) a GLOBALLY configured pattern is still caught' "^census-exclusion: FAIL \(3/3 code census paths excluded:.*by 'docs/\*\*'"
assert_never_enqueued 'case (cx16)'


printf '== case (cx18): from a LINKED WORKTREE, the ROOT checkout config is what roborev reads ==\n'
reset_stub
# BLOCKER A, mechanized — and it is the shape 1:1:1:1 puts EVERY issue in. The worktree
# carries the NARROWED set, the root checkout still carries the blanket one; roborev's
# daemon binds the repo by its `repos.root_path` (the ROOT checkout) and applies THAT
# file. A repo-config-only read reported "3/3 survive" while the real review delivered an
# emptied prompt. Both files are now evaluated and a swallow in EITHER fails.
work=$(make_fixture case_cx18 worktree-docs-exec)
root_work="${work%/wt}/work"
write_roborev_config "$work" "$NARROWED_PATTERNS"
write_roborev_config "$root_work" "$BLANKET_PATTERNS"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx18)' FAIL 1
assert_says 'case (cx18) the ROOT checkout config is caught despite a narrowed worktree config' '^census-exclusion: FAIL \(3/3 code census paths excluded:'
assert_says 'case (cx18) the swallow is attributed to the ROOT config, not the worktree one' "run\.sh by 'docs/\*\*' \[root-config\]"
assert_says 'case (cx18) both repo config sources are named' 'worktree-config=.* UNION root-config='
assert_says 'case (cx18) it explains that the worktree config does not override' 'A narrowed worktree config does NOT override it'
assert_says 'case (cx18) it names repos.root_path as the binding mechanism' "binds the repository by its 'repos.root_path'"
assert_never_enqueued 'case (cx18)'

printf '== case (cx18b): the same worktree layout PASSes once the ROOT config is narrowed ==\n'
reset_stub
# The complement, so cx18 is not merely "worktrees always fail": with BOTH files narrowed
# the three docs/ executables survive and the review IS enqueued. This also pins that a
# worktree does not double-report its own patterns.
work=$(make_fixture case_cx18b worktree-docs-exec)
root_work="${work%/wt}/work"
write_roborev_config "$work" "$NARROWED_PATTERNS"
write_roborev_config "$root_work" "$NARROWED_PATTERNS"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/docs/reports/x-artifacts/harness/run.sh b/docs/reports/x-artifacts/harness/run.sh\ndiff --git a/docs/reports/x-artifacts/harness/classify.py b/docs/reports/x-artifacts/harness/classify.py\ndiff --git a/docs/reports/x-artifacts/harness/offcpu.bt b/docs/reports/x-artifacts/harness/offcpu.bt'
run_wrapper "$work"
assert_verdict 'case (cx18b)' PASS 0
assert_says 'case (cx18b) all three executables survive both config sources' '^census-exclusion: PASS \(3/3 code census paths survive the effective exclusion set;'
if [ -s "$INVOKED" ]; then
  ok 'case (cx18b): the review WAS enqueued'
else
  bad 'case (cx18b): no review was enqueued — the two-source read must not be a blanket FAIL'
fi

printf "== case (cx19): a roborev BUILT-IN swallow is a NON-FAILING NOTICE that still names the path ==\n"
reset_stub
# `exclude_patterns` is not the whole exclusion set: the v0.61.2 binary ALWAYS appends a
# hard-coded lockfile/cache deny-list. `Cargo.lock` has a `lock` extension, so the census
# classifies it CODE — yet roborev silently drops it, and a check that modelled only the
# configured half reported it SURVIVING. Same false-PASS class as blocker A.
#
# BUT IT IS A NOTICE, NOT A FAIL, and the asymmetry is about the available REMEDY: a
# configured pattern is a one-token edit away, a built-in is compiled into the binary with
# no opt-out. Cargo.lock churn is routine here, so FAILing would permanently red a
# legitimate change class its author cannot fix — and a guard that fires with no available
# fix is the guard that gets disabled, which is how #3229 happened. So: named loudly in
# the value line, review still enqueued, RESULT not FAIL on this account.
work=$(make_fixture case_cx19 cargo-lock)
write_roborev_config "$work" "$NARROWED_PATTERNS"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
# The prompt carries main.rs only — Cargo.lock is exactly what roborev drops.
STUB_PROMPT='Review this diff:\ndiff --git a/main.rs b/main.rs'
run_wrapper "$work"
assert_verdict 'case (cx19)' PASS 0
assert_says 'case (cx19) the built-in swallow is a NOTICE naming the path and the built-in' "^census-exclusion: NOTICE \(1/2 code census paths survive the effective exclusion set; 1 code census path\(s\) excluded by a roborev built-in: Cargo\.lock by '\*\*/Cargo\.lock' \[roborev-builtin\]; corroboration: UNAVAILABLE; built-in-set: UNAVAILABLE\)\$"
assert_lacks 'case (cx19) it is NOT a FAIL' '^census-exclusion: FAIL'
assert_lacks 'case (cx19) and RESULT does not FAIL on its account' '^RESULT: FAIL'
assert_says 'case (cx19) the NOTICE says there is nothing to fix in any config file' 'NOTHING TO FIX'
assert_says 'case (cx19) it states the remedy-based rationale for not failing' 'a check that fires on a legitimate change .* with no remedy available is a check that gets disabled'
assert_says 'case (cx19) the built-in set is version-pinned' 'pinned to v0\.61\.2'
assert_says 'case (cx19) a clean verdict is explicitly declared not to cover the path' 'does NOT cover those path\(s\)'
assert_lacks 'case (cx19) it is NOT blamed on the operator config' 'excluded by YOUR CONFIGURATION'
assert_lacks 'case (cx19) main.rs is not reported swallowed' 'main\.rs by'
# The point of the ruling: the round still happens.
if [ -s "$INVOKED" ]; then
  ok 'case (cx19): the review WAS enqueued (a routine Cargo.lock touch is still reviewable)'
else
  bad 'case (cx19): no review was enqueued — a built-in swallow must not block the round'
fi
# ...and prompt-content must not re-report the SAME known absence as a discovery, which
# would move the unfixable red one key down instead of removing it.
assert_says 'case (cx19) prompt-content does not expect the built-in-excluded path' '^prompt-content: PASS \(1/1 code census paths present\) \(\+1 not expected: excluded by a roborev built-in — see census-exclusion:\)$'
assert_lacks 'case (cx19) prompt-content does not FAIL on the known absence' '^prompt-content: FAIL'
# The default stub carries NO `:(exclude,glob)` literals, so the live built-in set cannot
# be observed. That must read UNAVAILABLE in the VALUE LINE — "never silence" — and be
# neither a failure nor a blessing.
assert_says 'case (cx19) an unobservable built-in set says so in the value line' 'built-in-set: UNAVAILABLE\)$'
assert_says 'case (cx19) UNAVAILABLE is explicitly neither a failure nor a blessing' 'deliberately NEITHER a failure NOR a blessing'

printf "== case (cx19d): a live built-in set MATCHING the pin reads OK, and is corroborated ==\n"
reset_stub
# The complement of cx19: with the pinned literals actually present in the binary, the
# NOTICE carries `built-in-set: OK` — so the pin is corroborated rather than assumed, and
# the OK state is reachable (a state no test could reach would be dead code).
work=$(make_fixture case_cx19d cargo-lock)
write_roborev_config "$work" "$NARROWED_PATTERNS"
STUBBIN_OVERRIDE=$(make_builtin_stub pinned)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/main.rs b/main.rs'
run_wrapper "$work"
assert_verdict 'case (cx19d)' PASS 0
assert_says 'case (cx19d) the built-in set is observed to MATCH the pin' 'built-in-set: OK\)$'
assert_says 'case (cx19d) and the match is stated as corroborated, not assumed' 'The pin is corroborated, not assumed'
assert_says 'case (cx19d) the pinned-built-in swallow is still only a NOTICE' '^census-exclusion: NOTICE \('
assert_lacks 'case (cx19d) an agreeing built-in set never FAILs' '^census-exclusion: FAIL'

printf "== case (cx19e): an ADDED built-in (the upgrade that starts eating source) FAILs ==\n"
reset_stub
# THE case a bare NOTICE would have absorbed silently. The stub carries the pinned 26
# literals PLUS `**/*.rs`: if an upgrade started excluding source, a NOTICE would report
# it as normal operation and we would be blind again — the exact class #3229 exists to
# close. Divergence HAS a remedy (re-extract, update the pin, judge the new built-in), so
# by the rule it FAILs. The fixture touches NO lockfile, so this FAIL is provably about
# the MECHANISM and not about the diff.
work=$(make_fixture case_cx19e pushed)
write_roborev_config "$work" "$NARROWED_PATTERNS"
STUBBIN_OVERRIDE=$(make_builtin_stub added)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx19e)' FAIL 1
assert_says 'case (cx19e) divergence is its own FAIL form, naming the pinned version' '^census-exclusion: FAIL \(roborev built-in exclude set DIVERGED from the pinned v0\.61\.2 set: '
assert_says 'case (cx19e) the delta is quantified against the pinned literal count' "observed 27 ':\(exclude,glob\)' literal\(s\), pinned 26"
assert_says 'case (cx19e) it explains the count is 24 patterns + 2 prefix constants' '= 24 built-in patterns \+ 2 prefix constants'
assert_says 'case (cx19e) it names the remedy that makes this a FAIL, not a NOTICE' 'it HAS a remedy, which is why it FAILs rather than reporting a NOTICE'
assert_says 'case (cx19e) it names the silent-absorption risk in concrete terms' "started excluding '\*\.rs' or 'scripts/\*\*'"
assert_says 'case (cx19e) the FAIL is explicitly diff-independent' 'about the MECHANISM having moved under us, not about this diff'
assert_never_enqueued 'case (cx19e)'

printf "== case (cx19f): a REMOVED pinned built-in FAILs too, naming the missing pattern ==\n"
reset_stub
# Divergence in the OTHER direction. It matters just as much: a pattern that disappeared
# means the model over-excludes, so `census-exclusion:` would report a swallow that no
# longer happens — a FALSE FAIL, the direction that gets a guard bypassed.
work=$(make_fixture case_cx19f pushed)
write_roborev_config "$work" "$NARROWED_PATTERNS"
STUBBIN_OVERRIDE=$(make_builtin_stub removed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx19f)' FAIL 1
assert_says 'case (cx19f) the missing pinned pattern is named exactly' 'pinned pattern\(s\) no longer present in the binary: \*\*/Cargo\.lock'
assert_says 'case (cx19f) the count delta is reported alongside it' "observed 25 ':\(exclude,glob\)' literal\(s\), pinned 26"
assert_says 'case (cx19f) it points at the re-extract-and-repin obligation' 'update ROBOREV_BUILTIN_EXCLUDES and ROBOREV_BUILTIN_PATHSPEC_LITERALS'
assert_never_enqueued 'case (cx19f)'

printf "== case (cx19g): a CONFIGURED swallow AND a built-in divergence name BOTH causes ==\n"
reset_stub
# Precedence: both FAIL causes coexist; the message must not report only the winner.
work=$(make_fixture case_cx19g docs-executables)
write_roborev_config "$work" "$BLANKET_PATTERNS"
STUBBIN_OVERRIDE=$(make_builtin_stub added)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx19g)' FAIL 1
assert_says 'case (cx19g) the configured swallow is named first' '^census-exclusion: FAIL \(3/3 code census paths excluded:'
assert_says 'case (cx19g) and the divergence is named in the SAME value line' 'ALSO roborev built-in exclude set DIVERGED from the pinned v0\.61\.2 set:'
assert_says 'case (cx19g) the configured cause keeps its own remedy line' 'excluded by YOUR CONFIGURATION'
assert_never_enqueued 'case (cx19g)'

printf "== case (cx19c): a CONFIGURED swallow beside a built-in one still FAILs, naming BOTH ==\n"
reset_stub
# The ruling's boundary: NOTICE must not become a way for an actionable config defect to
# ride along unfixed. Here `docs/**` (configured) eats the harness .sh AND the built-in
# eats Cargo.lock.
work=$(make_fixture case_cx19c cargo-lock-and-docs-exec)
write_roborev_config "$work" "$BLANKET_PATTERNS"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx19c)' FAIL 1
assert_says 'case (cx19c) the FAIL wins over the NOTICE' '^census-exclusion: FAIL \(2/3 code census paths excluded:'
assert_says 'case (cx19c) the configured cause is named' "run\.sh by 'docs/\*\*' \[repo-config\]"
assert_says 'case (cx19c) the built-in cause is named too' "Cargo\.lock by '\*\*/Cargo\.lock' \[roborev-builtin\]"
assert_says 'case (cx19c) the actionable half is called out' 'excluded by YOUR CONFIGURATION'
assert_says 'case (cx19c) and the unactionable half does not soften the FAIL' 'does NOT soften this FAIL'
assert_never_enqueued 'case (cx19c)'

printf "== case (cx19b): the built-in set is a CONSTANT, extracted from the pinned binary ==\n"
# Structural, so an upgrade that drops a pattern from the constant cannot pass unnoticed:
# the lock family the v0.61.2 binary carries must all be present, and the maintenance
# obligation must be stated beside them.
_oracles_src="$SCRIPT_DIR/../flow/roborev-review-oracles.sh"
for _bi in '\*\*/Cargo\.lock' '\*\*/go\.sum' '\*\*/package-lock\.json' '\*\*/pnpm-lock\.yaml' \
  '\*\*/poetry\.lock' '\*\*/uv\.lock' '\*\*/composer\.lock' '\*\*/Podfile\.lock' \
  '\*\*/Package\.resolved' '\*\*/\.kata\.local\.toml' '\*\*/yarn\.lock' '\*\*/bun\.lockb' \
  '\*\*/flake\.lock' '\*\*/\.beads/\*\*' '\*\*/\.cache/\*\*'; do
  if grep -qE "$_bi" "$_oracles_src"; then
    ok "structural: the roborev built-in '$_bi' is modelled"
  else
    bad "structural: the roborev built-in '$_bi' is absent from ROBOREV_BUILTIN_EXCLUDES"
  fi
done
if grep -qE 'RE-EXTRACTING this list' "$_oracles_src"; then
  ok 'structural: the built-in set states the re-extract-on-upgrade obligation'
else
  bad 'structural: the built-in set does not state a re-extract-on-upgrade obligation'
fi
# GLOB-SAFETY, asserted structurally. Declared as a space-separated STRING and iterated
# unquoted, `**/package-lock.json` PATHNAME-EXPANDS to the repo-relative
# `website/package-lock.json` — which then reads as "a pinned pattern is no longer present
# in the binary" and FAILs every run. An array removes the hazard structurally; this assert
# stops it being reverted to a string.
if grep -qE '^ROBOREV_BUILTIN_EXCLUDES=\(' "$_oracles_src"; then
  ok 'structural: the built-in set is a bash ARRAY (no word-splitting, no pathname expansion)'
else
  bad 'structural: ROBOREV_BUILTIN_EXCLUDES is not an array — an unquoted iteration glob-expands **/package-lock.json against the repo'
fi
if grep -qE 'for [A-Za-z_]+ in \$ROBOREV_BUILTIN_EXCLUDES' "$_oracles_src"; then
  bad 'structural: ROBOREV_BUILTIN_EXCLUDES is iterated UNQUOTED somewhere — that pathname-expands the patterns'
else
  ok 'structural: ROBOREV_BUILTIN_EXCLUDES is never iterated unquoted'
fi
if grep -qE '^ROBOREV_BUILTIN_PATHSPEC_LITERALS=[0-9]+' "$_oracles_src"; then
  ok 'structural: the :(exclude,glob) literal count is pinned, so an ADDED built-in is observable'
else
  bad 'structural: no pinned :(exclude,glob) literal count — an added built-in could not be detected'
fi

printf "== case (cx20): a TOTAL built-in swallow FAILs pre-enqueue — an empty prompt certifies nothing ==\n"
reset_stub
# THE WORST DEFECT CLASS THIS WRAPPER CAN HAVE (#3229 round 3, blocker F1): a vacuous
# PASS textually identical to a genuine one. A lockfile-only bump PASSes `code-free:` (a
# `.lock` extension classifies as CODE) and its single CODE path is then eaten by the
# built-in `**/Cargo.lock`, so the reviewer receives an EMPTY prompt. Left as a NOTICE
# (the partial-swallow ruling) the block read `census-exclusion: NOTICE (0/1 survive)`,
# `prompt-content: PASS (0/0 ...)`, `RESULT: PASS`, exit 0 — and flow-closer would arm
# `--auto` on an unreviewed diff. This is NOT an exception to the NOTICE ruling: it is the
# same rule ("FAIL where the author can act; NOTICE where only the information is
# actionable; never silence") reaching the case that ruling does not cover, and the remedy
# is the one `code-free:` already prescribes.
work=$(make_fixture case_cx20 cargo-lock-only)
write_roborev_config "$work" "$NARROWED_PATTERNS"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
run_wrapper "$work"
assert_verdict 'case (cx20)' FAIL 1
assert_says 'case (cx20) the lockfile still classifies as CODE, so code-free does not catch it' '^code-free: PASS$'
assert_says 'case (cx20) the total swallow is a FAIL naming the empty diff' "^census-exclusion: FAIL \(0/1 code census paths survive the effective exclusion set; ALL 1 code census path\(s\) excluded by a roborev built-in, so the reviewer would receive an EMPTY diff: Cargo\.lock by '\*\*/Cargo\.lock' \[roborev-builtin\]; corroboration: UNAVAILABLE; built-in-set: UNAVAILABLE\)\$"
assert_lacks 'case (cx20) it is NOT downgraded to a NOTICE' '^census-exclusion: NOTICE'
assert_says 'case (cx20) it states the diff cannot be roborev-certified at all' 'CANNOT be roborev-certified at all'
assert_says 'case (cx20) it prescribes the code-free remedy' 'primary-source verification recorded in the PR'
assert_says 'case (cx20) it explains that a PARTIAL swallow still stays a NOTICE' 'a PARTIAL built-in swallow stays a NOTICE'
assert_says 'case (cx20) it names the unifying rule rather than claiming an exception' 'applied consistently, not an exception to it'
assert_never_enqueued 'case (cx20)'
assert_says 'case (cx20) prompt-content is never consulted' '^prompt-content: SKIP$'
assert_lacks 'case (cx20) a 0/0 PASS is never printed' 'prompt-content: PASS \(0/0'
assert_one_block 'case (cx20)'

printf "== case (cx20b): the SAME lockfile beside real code stays the NOTICE (the ruling is intact) ==\n"
reset_stub
# The complement, so cx20 is not read as "a built-in swallow is a FAIL again". One code
# path survives, so the review IS enqueued and census-exclusion is a NOTICE — the exact
# cx19 outcome. The boundary is TOTAL vs PARTIAL, nothing else.
work=$(make_fixture case_cx20b cargo-lock)
write_roborev_config "$work" "$NARROWED_PATTERNS"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/main.rs b/main.rs'
run_wrapper "$work"
assert_verdict 'case (cx20b)' PASS 0
assert_says 'case (cx20b) a PARTIAL swallow is still the NOTICE' '^census-exclusion: NOTICE \(1/2 code census paths survive'
assert_lacks 'case (cx20b) the total-swallow FAIL does not fire on a partial swallow' 'EMPTY diff'

printf "== case (cx21): prompt-content: can NEVER emit a 0/0 PASS (direct unit probe) ==\n"
# Belt-and-braces behind cx20, exercised DIRECTLY because the wrapper now refuses the
# condition upstream: with every code census path built-in-excluded there is no subject
# left, and `PASS (0/0 code census paths present)` would be indistinguishable from a
# genuine pass. Driven through the real function in the real files, so a future change
# that removes the census-exclusion FAIL cannot silently restore the vacuous PASS.
CHECKS_SRC="$SCRIPT_DIR/../flow/roborev-review-checks.sh"
ORACLES_SRC="$SCRIPT_DIR/../flow/roborev-review-oracles.sh"
cx21_probe="$tmp/cx21-probe.sh"
cx21_out="$tmp/cx21-out.txt"
cat >"$cx21_probe" <<'CX21'
set -uo pipefail
. "$1"   # oracles (roborev_unquote_path)
. "$2"   # checks  (roborev_check_prompt_content)
LOG="$3/cx21.log"
PROMPT_FILE="$LOG.prompt"
printf 'Review this diff:\ndiff --git a/main.rs b/main.rs\n' >"$PROMPT_FILE"
CENSUS='2 files, +2/-0'
BASE='origin/main'
JOB=4600
census_code_paths=("Cargo.lock")
CENSUS_BUILTIN_EXCLUDED=("Cargo.lock")
DETAILS=()
PROMPT_CONTENT=""
roborev_check_prompt_content
printf 'prompt-content: %s\n' "$PROMPT_CONTENT"
CX21
if bash "$cx21_probe" "$ORACLES_SRC" "$CHECKS_SRC" "$tmp" >"$cx21_out" 2>&1; then
  if grep -qE '^prompt-content: FAIL \(no code census path was checkable — a 0/0 is never a pass\)$' "$cx21_out"; then
    ok 'case (cx21): a zero-subject prompt-content is a FAIL, with the reason in the value line'
  else
    bad "case (cx21): expected the 0/0 refusal, got '$(cat "$cx21_out")'"
  fi
  if grep -qE '^prompt-content: PASS \(0/0' "$cx21_out"; then
    bad 'case (cx21): prompt-content emitted the vacuous PASS (0/0 ...) form'
  else
    ok 'case (cx21): prompt-content never emits PASS (0/0 ...)'
  fi
else
  bad "case (cx21): the unit probe did not run: $(cat "$cx21_out")"
fi

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

printf '== case (n2): an unretrievable prompt FAILs — a pass would rest on nothing ==\n'
reset_stub
work=$(make_fixture case_n2 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT=''
run_wrapper "$work"
assert_verdict 'case (n2)' FAIL 1
assert_says 'case (n2) prompt-content FAILs on an unretrievable prompt' '^prompt-content: FAIL \(prompt unretrievable — no evidence any diff was delivered\)$'
assert_says 'case (n2) says a pass would rest on nothing' 'a pass here would rest on nothing'

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
# HOME is redirected for the same reason `run_wrapper` does it: `census-exclusion`
# UNIONs the GLOBAL `$HOME/.roborev/config.toml` into the exclusion set, so on a box
# whose real global config carries a pattern this case would fail on THAT key and its
# own assertion would never be reached. This hand-rolled invocation must not skip it.
STUB_INVOKED="$INVOKED" PATH="$stubbin:$PATH" HOME="$FIXTURE_HOME" \
  bash "$WRAPPER" --repo "$tmp/case_t7/work" \
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
# HOME redirected (see case t7): t9 now runs THROUGH `census-exclusion`, which reads the
# global config, so a host global with a non-empty pattern would fail this case on the
# wrong key and the EXIT-trap assertion below would never fire.
STUB_INVOKED="$INVOKED" PATH="$stubbin:$PATH" HOME="$FIXTURE_HOME" \
  bash "$WRAPPER" --repo "$work" \
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
STUB_PROMPT='Review this diff:\ndiff --git a/beta.rs b/beta.rs\n+fn beta() {}'
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
STUB_PROMPT='Review this diff:\ndiff --git a/alpha.rs b/alpha.rs\ndiff --git a/beta.rs b/beta.rs\ndiff --git a/main.rs b/main.rs'
run_wrapper "$work"
assert_verdict 'case (x2)' PASS 0
assert_says 'case (x2) every code census path was covered' '^prompt-content: PASS \(2/2 code census paths present\)$'

if [ "$HAVE_PYTHON3" -eq 1 ]; then
printf '== case (y1): a transient failed record read is RETRIED, not accepted ==\n'
reset_stub
# An unreadable-on-first-attempt record must not be accepted as empty: doing so silently
# degraded FOUR asserts at once (sha-assert, review-completed, tier 2, model). NOTE the
# retry covers a TRANSIENT READ, not an asynchronous write — the round-5 "written
# asynchronously" diagnosis was wrong and is retracted; the real cause was the two
# payload shapes (see case (x10)).
work=$(make_fixture case_y1 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
STUB_RECORD_BLANK_FOR=2
run_wrapper "$work"
STUB_RECORD_BLANK_FOR=0
assert_verdict 'case (y1)' PASS 0
assert_says 'case (y1) the record is reported complete' '^job-record: PASS$'
assert_says 'case (y1) the retry is disclosed' 'read complete only on retry [0-9]+ of [0-9]+'
assert_lacks 'case (y1) does not repeat the retracted async claim' 'written asynchronously'
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
assert_says 'case (x5) the diff-header requirement is named' "appear on NEITHER side of any 'diff --git' header"

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

if [ "$HAVE_PYTHON3" -eq 1 ]; then
printf '== case (x9): a Summary word inside a finding body does not truncate the count ==\n'
reset_stub
# Observed on the live probe: the findings count read 3 where the review had 4, because
# a finding's own prose contained "summary:" and closed the block early. The terminator
# must be a LINE-INITIAL Summary label.
work=$(make_fixture case_x9 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
STUB_VERDICT_FIELD=F
STUB_REVIEW_RC=1
STUB_VERDICT=$'## Review Findings\n\n- **Severity**: Medium\n- **Problem**: the code prints a summary: line that is not a heading\n\n- **Severity**: Low\n- **Problem**: second finding\n\n## Summary\n\nTwo findings.'
run_wrapper "$work"
STUB_REVIEW_RC=0
assert_says 'case (x9) both findings are counted' '^findings: PRESENT \(2\)$'

printf '== case (x8): show --json returns the REVIEW row; list --json must be used ==\n'
reset_stub
# MEASURED on the live probe: `roborev show <job> --json` returns the REVIEW row —
# parseable, with id/agent/prompt but NO git_ref, status, verdict or token_usage. The
# wrapper used to accept the first payload that merely PARSED, so the richer `list
# --json` source was never consulted and the record looked permanently incomplete,
# silently downgrading sha-assert, tier 2 and model on every real run.
work=$(make_fixture case_x8 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
STUB_SHOW_JSON=review-row
run_wrapper "$work"
assert_verdict 'case (x8)' PASS 0
assert_says 'case (x8) the richer source was used' '^job-record: PASS$'
assert_says 'case (x8) the range oracle worked' '^sha-assert: PASS$'
assert_says 'case (x8) tokens came from the job row' '^vacuity-tier2: PASS$'
assert_says 'case (x8) the model was confirmed' '^model: gpt-5\.6-sol$'
fi  # HAVE_PYTHON3

printf '== case (v1): a multiline "## Summary" heading form is scanned by tier 1 ==\n'
reset_stub
# codex, round 6 (BLOCKER): the region used to be the LINES containing "Summary:", so the
# real heading form — "## Summary", blank line, then the prose — was missed entirely and a
# vacuous clean review whose "no code changes" sentence sits under the heading PASSED.
work=$(make_fixture case_v1 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
STUB_VERDICT_FIELD=P
STUB_VERDICT=$'No issues found.\n\n## Summary\n\nThe diff contains no code changes to review.'
run_wrapper "$work"
assert_verdict 'case (v1)' FAIL 1
assert_says 'case (v1) tier1 FAILs on the heading form' '^vacuity-tier1: FAIL \(vacuous verdict vs non-empty census\)$'
assert_says 'case (v1) findings are NONE, so the gate does not exempt it' '^findings: NONE$'

printf '== case (v2): a RENAME header covers both census paths ==\n'
reset_stub
# codex, round 6 (BLOCKER): the census runs with --no-renames (two paths) while the
# reviewer's diff may have rename detection ON (one a/old b/new header). Requiring
# same-path headers falsely rejected every review containing a detected rename.
work=$(make_fixture case_v2 renamed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
STUB_PROMPT='Review this diff:\ndiff --git a/main.rs b/renamed.rs\nsimilarity index 100%'
run_wrapper "$work"
assert_verdict 'case (v2)' PASS 0
assert_says 'case (v2) both rename sides count as covered' '^prompt-content: PASS \(2/2 code census paths present\)$'

printf '== case (w3): a MISSING checks file FAILs closed ==\n'
reset_stub
# Same bar as the oracles split: an absent checks file would turn review-completed,
# prompt-content, findings and both vacuity tiers into no-ops while the block read PASS.
work=$(make_fixture case_w3 pushed)
lonely_checks="$tmp/lonely-checks"
mkdir -p "$lonely_checks"
cp "$WRAPPER" "$lonely_checks/roborev-review.sh"
cp "$SCRIPT_DIR/../flow/roborev-review-oracles.sh" "$lonely_checks/"
cp "$SCRIPT_DIR/../flow/roborev-job-facts.py" "$lonely_checks/" 2>/dev/null || true
CASE_N=$((CASE_N + 1))
OUT="$tmp/out-$CASE_N.txt"
INVOKED="$tmp/invoked-$CASE_N.txt"
: >"$INVOKED"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
STUB_INVOKED="$INVOKED" PATH="$stubbin:$PATH" bash "$lonely_checks/roborev-review.sh" --repo "$work" \
  --agent codex --model gpt-5.6-sol --log "$tmp/transcript-$CASE_N.txt" >"$OUT" 2>&1
RC=$?
assert_verdict 'case (w3)' FAIL 1
assert_says 'case (w3) names the missing checks file' 'checks file .* is missing'
assert_says 'case (w3) refuses to run with the checks disabled' 'Failing closed rather than proceeding'
assert_says 'case (w3) the checks never claim a pass' '^review-completed: SKIP$'
assert_never_enqueued 'case (w3)'

printf '== case (w4): a TRUNCATED checks file FAILs closed ==\n'
reset_stub
work=$(make_fixture case_w4 pushed)
trunc_checks="$tmp/trunc-checks"
mkdir -p "$trunc_checks"
cp "$WRAPPER" "$trunc_checks/roborev-review.sh"
cp "$SCRIPT_DIR/../flow/roborev-review-oracles.sh" "$trunc_checks/"
cp "$SCRIPT_DIR/../flow/roborev-job-facts.py" "$trunc_checks/" 2>/dev/null || true
printf '# corrupt: defines nothing\n' >"$trunc_checks/roborev-review-checks.sh"
CASE_N=$((CASE_N + 1))
OUT="$tmp/out-$CASE_N.txt"
INVOKED="$tmp/invoked-$CASE_N.txt"
: >"$INVOKED"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
STUB_INVOKED="$INVOKED" PATH="$stubbin:$PATH" bash "$trunc_checks/roborev-review.sh" --repo "$work" \
  --agent codex --model gpt-5.6-sol --log "$tmp/transcript-$CASE_N.txt" >"$OUT" 2>&1
RC=$?
assert_verdict 'case (w4)' FAIL 1
assert_says 'case (w4) names the undefined check function' 'did not define roborev_check_review_completed'
assert_never_enqueued 'case (w4)'

if [ "$HAVE_PYTHON3" -eq 1 ]; then
printf '== case (x10): the NESTED job row in show --json is read as a first-class source ==\n'
reset_stub
# MEASURED (round 6): `roborev show <id> --json` returns a REVIEW row whose own `id`
# equals the job id and which NESTS the job row — git_ref, status, model,
# requested_model, token_usage, verdict — under a "job" key. Returning the FIRST id
# match handed back the outer row, which has none of those fields; that looked like an
# async durability problem and silently downgraded sha-assert, tier 2 and model.
# `list --json` is disabled here so `show` is the ONLY source.
work=$(make_fixture case_x10 pushed)
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse origin/main)
STUB_SHOW_JSON=nested
STUB_LIST_JSON=none
run_wrapper "$work"
assert_verdict 'case (x10)' PASS 0
assert_says 'case (x10) the nested job row was used' '^job-record: PASS$'
assert_says 'case (x10) the range oracle worked from the nested row' '^sha-assert: PASS$'
assert_says 'case (x10) tokens came from the nested row' '^vacuity-tier2: PASS$'
assert_says 'case (x10) the model was confirmed from the nested row' '^model: gpt-5\.6-sol$'
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
# The probe expectation is stated in RANGE terms: reviewed-sha is '<base>..<head>', so
# 'reviewed-sha == head-sha' could never hold and the old pinned wording defended an
# instruction that was guaranteed to fail.
assert_says '--help states the probe expectation in range terms' 'reviewed-sha is a RANGE'
assert_says '--help says the range must end in the worktree HEAD' 'ENDS IN that same head-sha'
assert_lacks '--help no longer asks for reviewed-sha == head-sha' 'reviewed-sha == head-sha'
assert_says '--help requires both agent and model' 'Both are required'
# #3229: the new key is documented — its meaning, its position, and its value grammar.
assert_says '--help documents the census-exclusion key' '^census-exclusion: \(PRE-ENQUEUE, immediately after code-free:'
assert_says '--help states the CORRECTED mechanism, not the falsified one' 'roborev drops exactly what its'
assert_lacks '--help never restates the falsified claim' '[Ee]xcludes non-code paths'
assert_says '--help pins the ported version' 'git\.FormatExcludeArgs'
assert_says '--help carries the swallowed-paths FAIL grammar' 'FAIL \(<m>/<n> code census paths excluded'
assert_says '--help distinguishes unreadable from absent' 'DISTINCT from'
assert_says '--help names the trailing-slash FAIL' 'FAIL \(trailing-slash pattern'
assert_says '--help names the drift FAIL' 'FAIL \(exclusion set drift'
assert_says '--help sends a FAIL to the config, not the reviewer' 'is a CONFIGURATION defect'
assert_says '--help defines docs-only as a code-free CENSUS, not a path prefix' 'CENSUS as code-free: classifies it, NEVER a .docs/. path prefix'
assert_says '--help names the harness convention as reviewed code' 'docs/reports/\*-artifacts/ are executable code'
# The four-source union and the built-in half are the two facts an operator reading a
# census-exclusion FAIL most needs, so --help states them rather than leaving them in the
# oracles file's comments.
assert_says '--help enumerates the four exclusion sources' 'THE EFFECTIVE SET IS A UNION OF FOUR THINGS'
assert_says '--help names the ROOT-checkout config source' '\[root-config\]'
assert_says '--help explains why the root checkout is read' 'binds a repo by its repos\.root_path'
assert_says '--help says a worktree config does not override the root one' 'does NOT override'
assert_says '--help names the non-configurable built-in source' '\[roborev-builtin\]  <-- NOT configurable'
assert_says '--help gives a concrete built-in consequence' 'A Cargo\.lock in your diff IS dropped'
assert_says '--help says every value line names the pattern source' 'names the SOURCE of the pattern responsible'
assert_says '--help says an empty parse must be corroborated' "CORROBORATED that nothing is configured"
assert_says '--help distinguishes a builtin NOTICE from a config FAIL' 'A NOTICE naming \[roborev-builtin\] is not'
# THE UNIFYING RULE must be stated verbatim in every rule-stating surface, so a future
# call of this shape is decided by the rule rather than re-litigated ad hoc.
assert_says '--help states the unifying rule verbatim' 'FAIL where the author can act; NOTICE where only the information is actionable;'
assert_says '--help states the unifying rule verbatim (2nd line)' 'never silence\.'
assert_says '--help says it is one rule, not three ad-hoc calls' 'not three ad-hoc calls but one rule applied three times'
assert_says '--help says a built-in has no fix because it is compiled in' 'deny-list is compiled in,? with no opt-out'
assert_says '--help warns that an unfixable guard gets DISABLED' 'check that gets DISABLED'
assert_says '--help makes built-in DIVERGENCE a FAIL because it has a remedy' 'DIVERGING from the pinned 24 => FAIL'
assert_says '--help names the silent-absorption risk concretely' "began excluding '\*\.rs' or 'scripts/\*\*'"
assert_says '--help says every value ends with the built-in-set state' "built-in-set: OK\|DIVERGED\|UNAVAILABLE"
assert_says '--help says never silence is load-bearing' 'never an unstated assumption'
assert_says '--help says both FAIL causes outrank the NOTICE and all are named' 'outrank the NOTICE, and EVERY cause present is named'
assert_says '--help records that NOTICE is outside the verdict scan' 'outside the verdict scan'
assert_says '--help gives the re-extraction command for a diverged pin' "grep -a -o ':\(exclude,glob\)"
assert_says '--help extends the re-verify obligation to the built-in list' 'Re-verify BOTH the port and the built-in list'
assert_never_enqueued '--help'


printf '== case (cx17): the declared RESIDUAL direction is noise, never a failure ==\n'
reset_stub
# A census path the wrapper classifies NON-CODE that the configuration does NOT exclude
# is delivered to the reviewer as bounded noise. It must fail NO key: the only failing
# direction is the opposite one (config excludes what the census calls CODE).
work=$(make_fixture case_cx17 docs-artifacts-mixed)
write_roborev_config "$work" "['*.md']"
STUB_ANNOUNCE_SHA=$(git -C "$work" rev-parse HEAD)
STUB_PROMPT='Review this diff:\ndiff --git a/main.rs b/main.rs'
run_wrapper "$work"
assert_verdict 'case (cx17)' PASS 0
assert_says 'case (cx17) the un-excluded artifact costs nothing but noise' '^census-exclusion: PASS \(1/1 code census paths survive'
assert_says 'case (cx17) the artifact is classified non-code, so prompt-content ignores it' '^prompt-content: PASS \(1/1 code census paths present\)$'

printf '== structural: the exclusion view is GIT-matched, VERSION-PINNED, and not a hand-rolled matcher ==\n'
reset_stub
ORACLES="$SCRIPT_DIR/../flow/roborev-review-oracles.sh"
if [ -f "$ORACLES" ]; then
  if grep -qF ':(exclude,glob)' "$ORACLES"; then
    ok 'structural: the check constructs :(exclude,glob) git pathspecs'
  else
    bad 'structural: no :(exclude,glob) pathspec construction found — the matcher may have been re-implemented'
  fi
  if grep -qE 'git -C "\$REPO" diff --name-only -z --no-renames' "$ORACLES"; then
    ok 'structural: survivors come from git diff --name-only -z --no-renames (NUL-safe, census-comparable)'
  else
    bad 'structural: the survivor query is not the NUL-safe git diff the census is comparable with'
  fi
  # A second, independent wildmatch implementation is the class of error this check exists
  # to catch, so its absence is asserted rather than assumed.
  if grep -qE 'roborev v0\.61\.2' "$ORACLES"; then
    ok 'structural: the ported construction names the pinned roborev version'
  else
    bad 'structural: the port does not name the roborev version it was derived from'
  fi
  if grep -qiE 're-?verif' "$ORACLES"; then
    ok 'structural: the port states the re-verify-on-upgrade maintenance obligation'
  else
    bad 'structural: the port does not state the re-verify-on-upgrade obligation'
  fi
else
  bad "structural: oracles file not found at $ORACLES"
fi

printf '== structural: NOTICE is OUTSIDE the failing-capable verdict scan ==\n'
# The mirror of the decorative-key bug: a value that reads NOTICE while RESULT: goes FAIL
# would make the built-in ruling a lie. Asserted against the SCAN ITSELF, not just against
# a case's observed exit code, so a future edit that adds NOTICE* to the failing set is
# caught here rather than by whichever case happens to exercise it.
_scan_line=$(grep -nE 'case "\$verdict" in' "$WRAPPER" | head -1 || printf '')
if [ -z "$_scan_line" ]; then
  bad 'structural: could not locate the wrapper verdict scan to inspect'
else
  ok "structural: the verdict scan is a single case over the per-check keys (${_scan_line%%:*})"
  if grep -qE 'case "\$verdict" in FAIL\*\|FINDINGS\*\|ERROR\*\|INCONSISTENT\*\)' "$WRAPPER"; then
    ok 'structural: the failing-capable set is exactly FAIL*|FINDINGS*|ERROR*|INCONSISTENT*'
  else
    bad 'structural: the failing-capable verdict set is not the expected FAIL*|FINDINGS*|ERROR*|INCONSISTENT*'
  fi
  if grep -E 'case "\$verdict" in' "$WRAPPER" | grep -q 'NOTICE'; then
    bad 'structural: NOTICE* appears in the failing-capable verdict scan — a census-exclusion NOTICE would red RESULT:'
  else
    ok 'structural: NOTICE* is absent from the failing-capable verdict scan'
  fi
  if grep -qE '^\s*printf .census-exclusion' "$WRAPPER"; then
    ok 'structural: census-exclusion is still emitted as a block key (not decorative)'
  else
    bad 'structural: census-exclusion is not emitted in the summary block'
  fi
  if grep -qE '"\$CENSUS_EXCLUSION"' "$WRAPPER"; then
    ok 'structural: census-exclusion still participates in the verdict scan (a FAIL there still reds RESULT:)'
  else
    bad 'structural: census-exclusion is absent from the verdict scan — a configured swallow would not red RESULT:'
  fi
fi

printf '== structural: the AC2 live probe is NOT a gate component ==\n'
GATE="$SCRIPT_DIR/../agent-gate.sh"
PROBE_REL='docs/reports/3229-artifacts/live-probe-procedure.md'
if [ -f "$GATE" ]; then
  if grep -qF "$PROBE_REL" "$GATE"; then
    bad "structural: $PROBE_REL is referenced by the agent gate — the live probe needs network + a live reviewer and must never be gate-run"
  else
    ok 'structural: the live probe is absent from the agent gate (documented + recorded, never gate-run)'
  fi
else
  printf 'SKIP - agent-gate.sh not found; the live-probe/gate separation could not be checked\n'
fi
# The probe must NOT be a committed executable under root `docs/` (#3229 ⑤a): roborev
# resolves exclude_patterns from the repo ROOT path and snapshots it at daemon start, so
# such a file makes census-exclusion FAIL correctly and permanently until this change
# merges — a deadlock, not a test. Asserted structurally so it cannot be reintroduced.
_probe_exec_count=$(find "$SCRIPT_DIR/../../docs/reports/3229-artifacts" -maxdepth 1 \
  \( -name '*.sh' -o -name '*.py' -o -name '*.bt' \) 2>/dev/null | wc -l | tr -d '[:space:]')
if [ "${_probe_exec_count:-0}" -eq 0 ]; then
  ok 'structural: the #3229 artifacts dir carries NO executable (a pre-merge self-demonstration is a deadlock)'
else
  bad "structural: $_probe_exec_count executable(s) under docs/reports/3229-artifacts/ — an executable under root docs/ makes census-exclusion FAIL until this change merges"
fi
if [ -f "$SCRIPT_DIR/../../docs/reports/3229-artifacts/live-probe-procedure.md" ]; then
  ok 'structural: the probe procedure is kept as committed prose'
  for _phrase in 'cannot certify itself' 'snapshots config at start' 'In Review' \
    'filed as a tracked issue' 'strictly better'; do
    if grep -qF -- "$_phrase" "$SCRIPT_DIR/../../docs/reports/3229-artifacts/live-probe-procedure.md"; then
      ok "structural: the probe procedure records '$_phrase'"
    else
      bad "structural: the probe procedure does not record '$_phrase'"
    fi
  done
else
  bad 'structural: docs/reports/3229-artifacts/live-probe-procedure.md is missing — the AC2 requirement was dropped rather than rescheduled'
fi

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
